//! Integration tests for Segment-Level Replication & Snapshot Streaming.
//!
//! Test matrix:
//! - Follower from empty node (full segment catch-up)
//! - Follower N segments behind (partial catch-up)
//! - Segment boundary switch during streaming
//! - Leader crash simulation (error recovery)
//! - Snapshot manifest creation and follower needs
//! - Backpressure under high segment count
//! - Multi-follower concurrent catch-up
//! - Tail streaming progressive catch-up

use falcon_cluster::segment_streaming::*;
use falcon_storage::structured_lsn::{SegmentHeader, StructuredLsn, SEGMENT_HEADER_SIZE};

fn make_segment_data(segment_id: u64, payload_size: usize) -> Vec<u8> {
    let hdr = SegmentHeader::new(
        segment_id,
        256 * 1024 * 1024,
        StructuredLsn::new(segment_id, 0),
    );
    let mut hdr_mod = hdr;
    hdr_mod.last_valid_offset = SEGMENT_HEADER_SIZE + payload_size as u64;
    hdr_mod.checksum = hdr_mod.compute_checksum();
    let mut data = hdr_mod.to_bytes();
    // Fill payload with segment-specific pattern
    for i in 0..payload_size {
        data.push(((segment_id as usize * 37 + i) % 256) as u8);
    }
    data
}

// ═══════════════════════════════════════════════════════════════════════════
// Empty follower: full segment catch-up from zero
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_empty_follower_full_catchup() {
    let coord = SegmentReplicationCoordinator::new(
        SegmentReplicationConfig {
            chunk_size: 2048,
            ..Default::default()
        },
        5, 10000,
    );

    // Leader has 5 sealed segments
    for seg in 0..5 {
        coord.register_segment_data(seg, make_segment_data(seg, 8000));
    }

    // Empty follower
    let follower = FollowerState::new_empty(1);
    let hello = ReplicationHello::from_follower(&follower);
    let path = coord.handle_hello(hello);

    // Should need all 5 segments
    match &path {
        ReplicationPath::SegmentStreaming { segments_to_send } => {
            assert_eq!(segments_to_send.len(), 5);
            assert_eq!(*segments_to_send, vec![0, 1, 2, 3, 4]);
        }
        _ => panic!("expected SegmentStreaming for empty follower, got {}", path),
    }

    // Stream all segments
    for seg in 0..5u64 {
        let iter = coord.stream_segment(seg).expect("segment should exist");
        let chunks: Vec<SegmentChunk> = iter.collect();
        assert!(!chunks.is_empty(), "segment {} should have chunks", seg);

        for chunk in &chunks {
            assert!(chunk.verify(), "chunk checksum should be valid");
            let complete = coord.follower_receive_chunk(1, chunk).unwrap();
            if chunk.is_last {
                assert!(complete);
            }
        }
    }

    // Verify follower state
    let state = coord.get_follower_state(1).unwrap();
    assert_eq!(state.sealed_segments.len(), 5);
    for seg in 0..5 {
        assert!(state.sealed_segments.contains(&seg));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Follower N segments behind (partial catch-up)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_follower_n_segments_behind() {
    let coord = SegmentReplicationCoordinator::new(
        SegmentReplicationConfig {
            chunk_size: 4096,
            ..Default::default()
        },
        10, 5000,
    );

    // Leader has segments 0-9
    for seg in 0..10 {
        coord.register_segment_data(seg, make_segment_data(seg, 4000));
    }

    // Follower already has segments 0-6
    let mut follower = FollowerState::new_empty(1);
    for seg in 0..7 {
        follower.seal_segment(seg);
    }

    let hello = ReplicationHello::from_follower(&follower);
    let path = coord.handle_hello(hello);

    match &path {
        ReplicationPath::SegmentStreaming { segments_to_send } => {
            assert_eq!(*segments_to_send, vec![7, 8, 9]);
        }
        _ => panic!("expected SegmentStreaming, got {}", path),
    }

    // Stream only missing segments
    for seg in [7, 8, 9] {
        let iter = coord.stream_segment(seg).unwrap();
        for chunk in iter {
            coord.follower_receive_chunk(1, &chunk).unwrap();
        }
    }

    let state = coord.get_follower_state(1).unwrap();
    assert_eq!(state.sealed_segments.len(), 10);
}

// ═══════════════════════════════════════════════════════════════════════════
// Segment boundary switch: follower catches up then transitions to tail
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_segment_to_tail_transition() {
    let coord = SegmentReplicationCoordinator::new(
        SegmentReplicationConfig {
            chunk_size: 4096,
            tail_batch_size: 512,
            ..Default::default()
        },
        3, 20000,
    );

    // Sealed segments 0-2
    for seg in 0..3 {
        coord.register_segment_data(seg, make_segment_data(seg, 5000));
    }
    // Active segment 3
    let mut active = vec![0u8; 20000];
    for i in 0..20000 { active[i] = (i % 256) as u8; }
    coord.register_active_segment_data(3, active);

    // Follower has segment 0 only
    let mut follower = FollowerState::new_empty(1);
    follower.seal_segment(0);

    // Phase 1: Segment streaming for segments 1, 2
    let hello = ReplicationHello::from_follower(&follower);
    let path = coord.handle_hello(hello);
    match &path {
        ReplicationPath::SegmentStreaming { segments_to_send } => {
            assert_eq!(*segments_to_send, vec![1, 2]);
        }
        _ => panic!("expected SegmentStreaming"),
    }

    for seg in [1, 2] {
        let iter = coord.stream_segment(seg).unwrap();
        for chunk in iter {
            coord.follower_receive_chunk(1, &chunk).unwrap();
        }
    }

    // Phase 2: Re-handshake — should now be tail streaming
    let state = coord.get_follower_state(1).unwrap();
    let hello2 = ReplicationHello::from_follower(&state);
    let path2 = coord.handle_hello(hello2);
    match path2 {
        ReplicationPath::TailStreaming { segment_id, from_offset } => {
            assert_eq!(segment_id, 3);
            assert_eq!(from_offset, 0);
        }
        _ => panic!("expected TailStreaming after segment catch-up, got {}", path2),
    }

    // Stream tail
    let batch = coord.get_tail_batch(3, 0).unwrap();
    assert!(batch.verify());
    coord.follower_receive_tail(1, &batch).unwrap();

    let final_state = coord.get_follower_state(1).unwrap();
    assert!(final_state.local_segment_offset > 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// Leader crash simulation: error recovery
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_leader_crash_error_recovery() {
    let coord = SegmentReplicationCoordinator::new(Default::default(), 5, 0);

    // Follower with progress
    let mut follower = FollowerState::new_empty(1);
    follower.seal_segment(0);
    follower.seal_segment(1);
    follower.seal_segment(2);

    let hello = ReplicationHello::from_follower(&follower);
    coord.handle_hello(hello);

    // Simulate error (leader crash / network interrupt)
    coord.follower_handle_error(1);
    let state = coord.get_follower_state(1).unwrap();
    assert_eq!(state.phase, FollowerPhase::Error);

    // Follower still has sealed segments
    assert_eq!(state.sealed_segments.len(), 3);
    // Position rolled back to after last sealed
    assert_eq!(state.local_segment_id, 3);
    assert_eq!(state.local_segment_offset, 0);

    // Re-handshake should work from the recovered state
    let hello2 = ReplicationHello::from_follower(&state);
    assert_eq!(hello2.last_segment_id, 3);
    assert_eq!(hello2.sealed_segment_ids.len(), 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// Checksum failure mid-stream
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_checksum_failure_mid_stream() {
    let coord = SegmentReplicationCoordinator::new(
        SegmentReplicationConfig { chunk_size: 1024, ..Default::default() },
        2, 0,
    );

    coord.register_segment_data(0, make_segment_data(0, 3000));

    let follower = FollowerState::new_empty(1);
    let hello = ReplicationHello::from_follower(&follower);
    coord.handle_hello(hello);

    // Get real chunks
    let iter = coord.stream_segment(0).unwrap();
    let chunks: Vec<SegmentChunk> = iter.collect();

    // Apply first chunk normally
    coord.follower_receive_chunk(1, &chunks[0]).unwrap();

    // Corrupt second chunk
    let mut bad = chunks[1].clone();
    bad.checksum = 0xDEAD;
    let err = coord.follower_receive_chunk(1, &bad).unwrap_err();

    match err {
        StreamingError::ChecksumMismatch { segment_id, .. } => {
            assert_eq!(segment_id, 0);
        }
        _ => panic!("expected ChecksumMismatch"),
    }

    // Recovery action should be retry chunk
    assert_eq!(decide_recovery(&err), ErrorRecoveryAction::RetryChunk);

    // Retry with good chunk succeeds
    coord.follower_receive_chunk(1, &chunks[1]).unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════
// Snapshot: create and apply to empty follower
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_snapshot_full_lifecycle() {
    let coord = SegmentReplicationCoordinator::new(
        SegmentReplicationConfig { chunk_size: 2048, ..Default::default() },
        5, 30000,
    );

    for seg in 0..5 {
        coord.register_segment_data(seg, make_segment_data(seg, 4000));
    }

    // Create snapshot
    let snap = coord.create_snapshot(1, b"catalog_v1".to_vec());
    assert_eq!(snap.snapshot_lsn, StructuredLsn::new(5, 0));
    assert_eq!(snap.sealed_segments.len(), 5);

    // Empty follower needs full snapshot
    let empty = FollowerState::new_empty(42);
    assert!(snap.follower_needs_full_snapshot(&empty));
    let needed = snap.segments_for_follower(&empty);
    assert_eq!(needed, vec![0, 1, 2, 3, 4]);

    // Partial follower needs only missing segments
    let mut partial = FollowerState::new_empty(43);
    partial.seal_segment(0);
    partial.seal_segment(1);
    assert!(!snap.follower_needs_full_snapshot(&partial));
    let needed = snap.segments_for_follower(&partial);
    assert_eq!(needed, vec![2, 3, 4]);
}

// ═══════════════════════════════════════════════════════════════════════════
// Backpressure under high segment count
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_backpressure_high_segment_count() {
    let bp = BackpressureSender::new(4);

    // Queue 20 segments
    bp.enqueue_segments(&(0..20).collect::<Vec<u64>>());
    assert_eq!(bp.queue_len(), 20);

    let mut sent = 0;
    let mut acked = 0;

    // Simulate send/ack loop
    while bp.queue_len() > 0 || bp.inflight_count() > 0 {
        // Send as many as backpressure allows
        while let Some(_seg) = bp.next_segment() {
            bp.mark_sent();
            sent += 1;
        }

        // Ack all inflight
        let to_ack = bp.inflight_count();
        for _ in 0..to_ack {
            bp.ack();
            acked += 1;
        }
    }

    assert_eq!(sent, 20);
    assert_eq!(acked, 20);
    assert_eq!(bp.inflight_count(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// Multi-follower concurrent catch-up
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_multi_follower_concurrent() {
    use std::sync::Arc;

    let coord = Arc::new(SegmentReplicationCoordinator::new(
        SegmentReplicationConfig { chunk_size: 2048, ..Default::default() },
        5, 0,
    ));

    for seg in 0..5 {
        coord.register_segment_data(seg, make_segment_data(seg, 4000));
    }

    let mut handles = Vec::new();
    for follower_id in 1..=4u64 {
        let c = Arc::clone(&coord);
        handles.push(std::thread::spawn(move || {
            let follower = FollowerState::new_empty(follower_id);
            let hello = ReplicationHello::from_follower(&follower);
            let path = c.handle_hello(hello);

            let segments = match path {
                ReplicationPath::SegmentStreaming { segments_to_send } => segments_to_send,
                _ => panic!("expected SegmentStreaming for follower {}", follower_id),
            };

            for seg in &segments {
                let iter = c.stream_segment(*seg).unwrap();
                for chunk in iter {
                    c.follower_receive_chunk(follower_id, &chunk).unwrap();
                }
            }

            let state = c.get_follower_state(follower_id).unwrap();
            assert_eq!(state.sealed_segments.len(), 5);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // All 4 followers should be registered
    for fid in 1..=4 {
        let state = coord.get_follower_state(fid).unwrap();
        assert_eq!(state.sealed_segments.len(), 5);
    }

    // Metrics should reflect all handshakes
    assert_eq!(
        coord.metrics.handshakes_total.load(std::sync::atomic::Ordering::Relaxed),
        4
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Tail streaming: progressive catch-up
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_tail_progressive_catchup() {
    let coord = SegmentReplicationCoordinator::new(
        SegmentReplicationConfig {
            chunk_size: 4096,
            tail_batch_size: 500,
            ..Default::default()
        },
        0, 5000,
    );

    let mut active = vec![0u8; 5000];
    for i in 0..5000 { active[i] = (i % 256) as u8; }
    coord.register_active_segment_data(0, active);

    // Follower at offset 0 on the same segment
    let mut follower = FollowerState::new_empty(1);
    follower.local_segment_id = 0;
    follower.local_segment_offset = 0;
    follower.local_last_lsn = StructuredLsn::new(0, 0);

    let hello = ReplicationHello::from_follower(&follower);
    let path = coord.handle_hello(hello);
    assert!(matches!(path, ReplicationPath::TailStreaming { .. }));

    // Progressively fetch tail batches
    let mut offset = 0u64;
    let mut batches = 0;
    while offset < 5000 {
        if let Some(batch) = coord.get_tail_batch(0, offset) {
            assert!(batch.verify());
            coord.follower_receive_tail(1, &batch).unwrap();
            offset = batch.from_offset + batch.data.len() as u64;
            batches += 1;
        } else {
            break;
        }
    }

    assert!(batches >= 10, "expected ≥10 batches of 500 bytes, got {}", batches);
    let state = coord.get_follower_state(1).unwrap();
    assert_eq!(state.local_segment_offset, 5000);
}

// ═══════════════════════════════════════════════════════════════════════════
// Metrics accuracy
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_metrics_comprehensive() {
    let coord = SegmentReplicationCoordinator::new(
        SegmentReplicationConfig { chunk_size: 4096, ..Default::default() },
        3, 0,
    );

    for seg in 0..3 {
        coord.register_segment_data(seg, make_segment_data(seg, 4000));
    }

    // Handshake
    let follower = FollowerState::new_empty(1);
    let hello = ReplicationHello::from_follower(&follower);
    coord.handle_hello(hello);

    // Stream segments
    for seg in 0..3 {
        let iter = coord.stream_segment(seg).unwrap();
        for chunk in iter {
            coord.follower_receive_chunk(1, &chunk).unwrap();
        }
    }

    // Inject a checksum failure
    let bad = SegmentChunk {
        segment_id: 0, offset: 0, data: vec![1], checksum: 0, is_last: false,
    };
    let _ = coord.follower_receive_chunk(1, &bad);

    let snap = coord.metrics.snapshot();
    assert_eq!(snap.handshakes_total, 1);
    assert_eq!(snap.segments_streamed_total, 3);
    assert!(snap.segment_bytes_total > 0);
    assert_eq!(snap.checksum_failures, 1);
}
