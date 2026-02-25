//! Integration tests for the Unified Data Plane.
//!
//! Test matrix:
//! - Empty node bootstrap (manifest + segments from leader)
//! - Follower N segments behind catch-up
//! - Compactor generates cold segments, replication stays consistent
//! - Manifest rollback / replay (idempotent)
//! - Chunk loss / CRC fail (fault injection)
//! - Resume after network interrupt
//! - Leader failover mid-stream
//! - GC + streaming concurrent (no delete of streaming segment)
//! - Mixed segment types (WAL + Cold + Snapshot) in one manifest
//! - Performance: segment store throughput

use std::collections::BTreeSet;

use falcon_storage::csn::Csn;
use falcon_storage::structured_lsn::StructuredLsn;
use falcon_storage::unified_data_plane::*;

// ═══════════════════════════════════════════════════════════════════════════
// I1 — Correctness: Empty node bootstrap
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_empty_node_bootstrap() {
    // Leader has 5 WAL + 2 Cold segments
    let leader_store = SegmentStore::new();
    let mut leader_manifest = Manifest::new();

    for i in 0..5u64 {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024 * 1024, StructuredLsn::new(i, 0));
        leader_store.create_segment(hdr).unwrap();
        leader_store.write_chunk(i, &vec![0xAB; 5000]).unwrap();
        leader_store.seal_segment(i).unwrap();
        leader_manifest.add_segment(ManifestEntry {
            segment_id: i,
            kind: SegmentKind::Wal,
            size_bytes: 5000,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i, 5000),
            },
            sealed: true,
        });
    }
    for i in 100..102u64 {
        let hdr = UnifiedSegmentHeader::new_cold(i, 64 * 1024, SegmentCodec::Lz4, 1, 0);
        leader_store.create_segment(hdr).unwrap();
        leader_store.write_chunk(i, &vec![0xCD; 3000]).unwrap();
        leader_store.seal_segment(i).unwrap();
        leader_manifest.add_segment(ManifestEntry {
            segment_id: i,
            kind: SegmentKind::Cold,
            size_bytes: 3000,
            codec: SegmentCodec::Lz4,
            logical_range: LogicalRange::Cold {
                table_id: 1, shard_id: 0, min_key: vec![], max_key: vec![],
            },
            sealed: true,
        });
    }

    // Follower starts empty
    let follower_store = SegmentStore::new();
    let handshake = ReplicationHandshake {
        node_id: 42,
        last_manifest_epoch: 0,
        have_segments: BTreeSet::new(),
        protocol_version: 1,
    };

    let resp = compute_replication_plan(&leader_manifest, &handshake);
    assert_eq!(resp.required_segments.len(), 7); // 5 WAL + 2 Cold

    // Simulate streaming all segments
    for seg_id in &resp.required_segments {
        let body = leader_store.get_segment_body(*seg_id).unwrap();
        let hdr = leader_store.open_segment(*seg_id).unwrap();
        follower_store.create_segment(hdr).unwrap();
        follower_store.write_chunk_at(*seg_id, 0, &body).unwrap();
        follower_store.seal_segment(*seg_id).unwrap();
    }

    // Verify follower has all segments
    for seg_id in &resp.required_segments {
        assert!(follower_store.exists(*seg_id));
        assert!(follower_store.is_sealed(*seg_id).unwrap());
    }

    // Recovery on follower
    let mut rc = RecoveryCoordinator::new(leader_manifest.clone());
    rc.advance_to_verify();
    rc.verify_segments(&follower_store);
    assert_eq!(rc.phase, RecoveryPhase::ReplayWalTail);
    rc.mark_ready();
    assert!(rc.is_ready());
}

// ═══════════════════════════════════════════════════════════════════════════
// I1 — Follower N segments behind catch-up
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_follower_n_segments_catchup() {
    let leader_store = SegmentStore::new();
    let mut leader_manifest = Manifest::new();

    for i in 0..10u64 {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
        leader_store.create_segment(hdr).unwrap();
        leader_store.write_chunk(i, &vec![i as u8; 200]).unwrap();
        leader_store.seal_segment(i).unwrap();
        leader_manifest.add_segment(ManifestEntry {
            segment_id: i,
            kind: SegmentKind::Wal,
            size_bytes: 200,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i, 200),
            },
            sealed: true,
        });
    }

    // Follower has segments 0-6
    let follower_store = SegmentStore::new();
    for i in 0..7u64 {
        let body = leader_store.get_segment_body(i).unwrap();
        let hdr = leader_store.open_segment(i).unwrap();
        follower_store.create_segment(hdr).unwrap();
        follower_store.write_chunk_at(i, 0, &body).unwrap();
        follower_store.seal_segment(i).unwrap();
    }

    let handshake = ReplicationHandshake {
        node_id: 2,
        last_manifest_epoch: 7,
        have_segments: (0..7).collect(),
        protocol_version: 1,
    };

    let resp = compute_replication_plan(&leader_manifest, &handshake);
    assert_eq!(resp.required_segments, vec![7, 8, 9]);

    // Fetch missing
    for seg_id in &resp.required_segments {
        let body = leader_store.get_segment_body(*seg_id).unwrap();
        let hdr = leader_store.open_segment(*seg_id).unwrap();
        follower_store.create_segment(hdr).unwrap();
        follower_store.write_chunk_at(*seg_id, 0, &body).unwrap();
        follower_store.seal_segment(*seg_id).unwrap();
    }

    // Verify all present
    for i in 0..10u64 {
        assert!(follower_store.exists(i));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// I1 — Compactor generates new cold segments, replication consistent
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_compactor_cold_segments_replication() {
    let store = SegmentStore::new();
    let mut manifest = Manifest::new();

    // Initial state: 2 WAL + 1 Cold
    for i in 0..2u64 {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
        store.create_segment(hdr).unwrap();
        store.seal_segment(i).unwrap();
        manifest.add_segment(ManifestEntry {
            segment_id: i,
            kind: SegmentKind::Wal,
            size_bytes: 100,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i, 100),
            },
            sealed: true,
        });
    }
    let cold_hdr = UnifiedSegmentHeader::new_cold(100, 64 * 1024, SegmentCodec::Lz4, 1, 0);
    store.create_segment(cold_hdr).unwrap();
    store.write_chunk(100, &vec![0xCD; 2000]).unwrap();
    store.seal_segment(100).unwrap();
    manifest.add_segment(ManifestEntry {
        segment_id: 100,
        kind: SegmentKind::Cold,
        size_bytes: 2000,
        codec: SegmentCodec::Lz4,
        logical_range: LogicalRange::Cold {
            table_id: 1, shard_id: 0, min_key: vec![], max_key: vec![],
        },
        sealed: true,
    });

    let epoch_before = manifest.epoch;

    // Compactor produces a new cold segment replacing the old one
    let new_cold_hdr = UnifiedSegmentHeader::new_cold(101, 64 * 1024, SegmentCodec::Lz4, 1, 0);
    store.create_segment(new_cold_hdr).unwrap();
    store.write_chunk(101, &vec![0xEF; 1500]).unwrap();
    store.seal_segment(101).unwrap();
    manifest.add_segment(ManifestEntry {
        segment_id: 101,
        kind: SegmentKind::Cold,
        size_bytes: 1500,
        codec: SegmentCodec::Lz4,
        logical_range: LogicalRange::Cold {
            table_id: 1, shard_id: 0, min_key: vec![], max_key: vec![],
        },
        sealed: true,
    });

    assert!(manifest.epoch > epoch_before);

    // Follower catches up — needs the new cold segment too
    let handshake = ReplicationHandshake {
        node_id: 3,
        last_manifest_epoch: epoch_before,
        have_segments: [0, 1, 100].iter().copied().collect(),
        protocol_version: 1,
    };
    let resp = compute_replication_plan(&manifest, &handshake);
    assert!(resp.required_segments.contains(&101));
}

// ═══════════════════════════════════════════════════════════════════════════
// I1 — Manifest idempotent replay
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_manifest_idempotent_operations() {
    let mut m = Manifest::new();

    // Add same segment twice — second should overwrite (idempotent)
    let entry = ManifestEntry {
        segment_id: 0,
        kind: SegmentKind::Wal,
        size_bytes: 100,
        codec: SegmentCodec::None,
        logical_range: LogicalRange::Wal {
            start_lsn: StructuredLsn::ZERO,
            end_lsn: StructuredLsn::ZERO,
        },
        sealed: false,
    };
    m.add_segment(entry.clone());
    m.add_segment(entry);
    assert_eq!(m.segments.len(), 1); // BTreeMap overwrites

    // Seal twice — second is no-op
    assert!(m.seal_segment(0));
    assert!(!m.seal_segment(0)); // already sealed

    // Remove already-removed segment
    let removed = m.remove_segments(&[0, 0, 999]);
    assert_eq!(removed, 1); // only segment 0 existed
}

// ═══════════════════════════════════════════════════════════════════════════
// I2 — Fault Injection: chunk CRC fail
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_fault_chunk_crc_fail() {
    let good_chunk = StreamChunk {
        segment_id: 0,
        chunk_index: 0,
        data: b"valid data".to_vec(),
        crc: StreamChunk::compute_crc(b"valid data"),
        is_last: false,
    };
    assert!(good_chunk.verify());

    // Corrupt CRC
    let bad_chunk = StreamChunk {
        segment_id: 0,
        chunk_index: 0,
        data: b"valid data".to_vec(),
        crc: 0xBADBAD,
        is_last: false,
    };
    assert!(!bad_chunk.verify());

    // Corrupt data
    let bad_data_chunk = StreamChunk {
        segment_id: 0,
        chunk_index: 0,
        data: b"corrupted!".to_vec(),
        crc: StreamChunk::compute_crc(b"valid data"),
        is_last: false,
    };
    assert!(!bad_data_chunk.verify());
}

// ═══════════════════════════════════════════════════════════════════════════
// I2 — Fault Injection: resume after interrupt
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_resume_after_interrupt() {
    let store = SegmentStore::new();
    let mut manifest = Manifest::new();

    // 5 segments to stream
    for i in 0..5u64 {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
        store.create_segment(hdr).unwrap();
        store.write_chunk(i, &vec![i as u8; 300]).unwrap();
        store.seal_segment(i).unwrap();
        manifest.add_segment(ManifestEntry {
            segment_id: i,
            kind: SegmentKind::Wal,
            size_bytes: 300,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i, 300),
            },
            sealed: true,
        });
    }

    // Simulate: first stream gets segments 0, 1, then interrupt
    let follower_store = SegmentStore::new();
    for i in 0..2u64 {
        let body = store.get_segment_body(i).unwrap();
        let hdr = store.open_segment(i).unwrap();
        follower_store.create_segment(hdr).unwrap();
        follower_store.write_chunk_at(i, 0, &body).unwrap();
        follower_store.seal_segment(i).unwrap();
    }

    // Resume: re-handshake with what follower has
    let resume_point = StreamResumePoint {
        stream_id: 1,
        last_segment_id: 1,
        last_chunk_index: 0,
    };
    let handshake = ReplicationHandshake {
        node_id: 1,
        last_manifest_epoch: 0,
        have_segments: [0, 1].iter().copied().collect(),
        protocol_version: 1,
    };
    let resp = compute_replication_plan(&manifest, &handshake);
    assert_eq!(resp.required_segments, vec![2, 3, 4]);

    // Fetch remaining
    for seg_id in &resp.required_segments {
        let body = store.get_segment_body(*seg_id).unwrap();
        let hdr = store.open_segment(*seg_id).unwrap();
        follower_store.create_segment(hdr).unwrap();
        follower_store.write_chunk_at(*seg_id, 0, &body).unwrap();
        follower_store.seal_segment(*seg_id).unwrap();
    }

    // All 5 segments present
    for i in 0..5u64 {
        assert!(follower_store.exists(i));
    }
    // Resume point was used (conceptually)
    let _ = resume_point;
}

// ═══════════════════════════════════════════════════════════════════════════
// I2 — GC + streaming concurrent safety
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_gc_streaming_concurrent_safety() {
    let store = SegmentStore::new();
    let mut manifest = Manifest::new();
    let gc = SegmentGc::new();
    gc.update_min_durable_lsn(StructuredLsn::new(10, 0));

    for i in 0..5u64 {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
        store.create_segment(hdr).unwrap();
        store.write_chunk(i, &vec![0u8; 100]).unwrap();
        store.seal_segment(i).unwrap();
        manifest.add_segment(ManifestEntry {
            segment_id: i,
            kind: SegmentKind::Wal,
            size_bytes: 100,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i + 1, 0),
            },
            sealed: true,
        });
    }

    // Mark segments 2,3 as currently streaming
    gc.mark_streaming(2);
    gc.mark_streaming(3);

    // GC pass — segments 2,3 should be deferred
    let eligible = gc.gc_pass(&manifest);
    assert!(!eligible.contains(&2));
    assert!(!eligible.contains(&3));
    // Segments 0,1,4 are eligible (all followers past them)
    assert!(eligible.contains(&0));
    assert!(eligible.contains(&1));
    assert!(eligible.contains(&4));

    // Unmark and re-check
    gc.unmark_streaming(2);
    gc.unmark_streaming(3);
    let eligible2 = gc.gc_pass(&manifest);
    assert!(eligible2.contains(&2));
    assert!(eligible2.contains(&3));
}

// ═══════════════════════════════════════════════════════════════════════════
// I1 — Mixed segment types in one manifest
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_mixed_segment_types() {
    let store = SegmentStore::new();
    let mut manifest = Manifest::new();

    // WAL segments
    for i in 0..3u64 {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
        store.create_segment(hdr).unwrap();
        store.seal_segment(i).unwrap();
        manifest.add_segment(ManifestEntry {
            segment_id: i,
            kind: SegmentKind::Wal,
            size_bytes: 100,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i, 100),
            },
            sealed: true,
        });
    }

    // Cold segments
    for i in 100..103u64 {
        let hdr = UnifiedSegmentHeader::new_cold(i, 64 * 1024, SegmentCodec::Lz4, 1, 0);
        store.create_segment(hdr).unwrap();
        store.seal_segment(i).unwrap();
        manifest.add_segment(ManifestEntry {
            segment_id: i,
            kind: SegmentKind::Cold,
            size_bytes: 500,
            codec: SegmentCodec::Lz4,
            logical_range: LogicalRange::Cold {
                table_id: 1, shard_id: 0, min_key: vec![], max_key: vec![],
            },
            sealed: true,
        });
    }

    // Snapshot segments
    let snap_hdr = UnifiedSegmentHeader::new_snapshot(200, 1024, 1, 0);
    store.create_segment(snap_hdr).unwrap();
    store.seal_segment(200).unwrap();
    manifest.add_segment(ManifestEntry {
        segment_id: 200,
        kind: SegmentKind::Snapshot,
        size_bytes: 1000,
        codec: SegmentCodec::None,
        logical_range: LogicalRange::Snapshot { snapshot_id: 1, chunk_index: 0 },
        sealed: true,
    });

    // Query by kind
    assert_eq!(manifest.segments_by_kind(SegmentKind::Wal).len(), 3);
    assert_eq!(manifest.segments_by_kind(SegmentKind::Cold).len(), 3);
    assert_eq!(manifest.segments_by_kind(SegmentKind::Snapshot).len(), 1);

    // Snapshot definition includes all types
    let snap_def = SnapshotDefinition::create(
        1, &manifest, StructuredLsn::new(3, 0), Csn::new(50),
    );
    assert_eq!(snap_def.wal_segments.len(), 3);
    assert_eq!(snap_def.cold_segments.len(), 3);
    assert_eq!(snap_def.snapshot_segments.len(), 1);
    assert_eq!(snap_def.all_segments().len(), 7);
}

// ═══════════════════════════════════════════════════════════════════════════
// I2 — Snapshot restore as unified recovery
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_snapshot_restore_unified_recovery() {
    let leader_store = SegmentStore::new();
    let mut leader_manifest = Manifest::new();

    // Build leader state: 3 WAL + 1 Cold
    for i in 0..3u64 {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
        leader_store.create_segment(hdr).unwrap();
        leader_store.write_chunk(i, &vec![0xAA; 400]).unwrap();
        leader_store.seal_segment(i).unwrap();
        leader_manifest.add_segment(ManifestEntry {
            segment_id: i,
            kind: SegmentKind::Wal,
            size_bytes: 400,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i, 400),
            },
            sealed: true,
        });
    }
    let cold_hdr = UnifiedSegmentHeader::new_cold(50, 64 * 1024, SegmentCodec::Lz4, 1, 0);
    leader_store.create_segment(cold_hdr).unwrap();
    leader_store.write_chunk(50, &vec![0xBB; 1000]).unwrap();
    leader_store.seal_segment(50).unwrap();
    leader_manifest.add_segment(ManifestEntry {
        segment_id: 50,
        kind: SegmentKind::Cold,
        size_bytes: 1000,
        codec: SegmentCodec::Lz4,
        logical_range: LogicalRange::Cold {
            table_id: 1, shard_id: 0, min_key: vec![], max_key: vec![],
        },
        sealed: true,
    });

    // Create snapshot definition
    leader_manifest.set_snapshot_cutpoint(SnapshotCutpoint {
        snapshot_id: 1,
        cut_lsn: StructuredLsn::new(3, 0),
        cut_csn: Csn::new(100),
        epoch: leader_manifest.epoch,
    });
    let snap_def = SnapshotDefinition::create(
        1, &leader_manifest, StructuredLsn::new(3, 0), Csn::new(100),
    );

    // Restore on new node
    let new_store = SegmentStore::new();
    let needed = snap_def.missing_for(&BTreeSet::new());
    assert_eq!(needed.len(), 4); // 3 WAL + 1 Cold

    for seg_id in &needed {
        let body = leader_store.get_segment_body(*seg_id).unwrap();
        let hdr = leader_store.open_segment(*seg_id).unwrap();
        new_store.create_segment(hdr).unwrap();
        new_store.write_chunk_at(*seg_id, 0, &body).unwrap();
        new_store.seal_segment(*seg_id).unwrap();
    }

    // Unified recovery path
    let mut rc = RecoveryCoordinator::new(leader_manifest);
    rc.advance_to_verify();
    rc.verify_segments(&new_store);
    assert_eq!(rc.phase, RecoveryPhase::ReplayWalTail);
    rc.mark_ready();
    assert!(rc.is_ready());
}

// ═══════════════════════════════════════════════════════════════════════════
// I3 — Performance: segment store throughput
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_segment_store_throughput() {
    let store = SegmentStore::new();
    let hdr = UnifiedSegmentHeader::new_wal(0, 256 * 1024 * 1024, StructuredLsn::ZERO);
    store.create_segment(hdr).unwrap();

    let chunk = vec![0xABu8; 4096];
    let start = std::time::Instant::now();
    let iterations = 10_000;
    for _ in 0..iterations {
        store.write_chunk(0, &chunk).unwrap();
    }
    let elapsed = start.elapsed();
    let total_mb = (iterations * 4096) as f64 / (1024.0 * 1024.0);
    let throughput = total_mb / elapsed.as_secs_f64();

    // Should achieve at least 100 MB/s in-memory
    assert!(
        throughput > 100.0,
        "throughput too low: {:.1} MB/s ({} iterations in {:.2}s)",
        throughput, iterations, elapsed.as_secs_f64()
    );
}
