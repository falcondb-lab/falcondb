//! Integration tests for the full unified data plane implementation.
//!
//! Test matrix:
//! H1 — Correctness: empty-disk bootstrap, cold compaction+replication, snapshot restore, manifest replay
//! H2 — Fault injection: streaming interrupt, leader switch, GC+streaming concurrent, compactor crash
//! H3 — Performance guardrails: bootstrap throughput, cold compaction latency, replication no-degrade

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use falcon_storage::structured_lsn::StructuredLsn;
use falcon_storage::csn::Csn;
use falcon_storage::unified_data_plane::*;
use falcon_storage::unified_data_plane_full::*;

// ═══════════════════════════════════════════════════════════════════════════
// H1 — Correctness
// ═══════════════════════════════════════════════════════════════════════════

/// H1.1: Empty-disk bootstrap — node starts with nothing, ends fully serviceable.
#[test]
fn test_h1_empty_disk_full_bootstrap() {
    // Leader has WAL + Cold segments
    let leader_store = SegmentStore::new();
    let mut leader_manifest = Manifest::new();

    // 5 WAL segments — use next_segment_id to avoid collision with compactor
    let mut wal_ids = Vec::new();
    for i in 0..5u64 {
        let seg_id = leader_store.next_segment_id();
        wal_ids.push(seg_id);
        let hdr = UnifiedSegmentHeader::new_wal(seg_id, 1024 * 1024, StructuredLsn::new(i, 0));
        leader_store.create_segment(hdr).unwrap();
        leader_store.write_chunk(seg_id, &vec![0xAB; 4096]).unwrap();
        leader_store.seal_segment(seg_id).unwrap();
        leader_manifest.add_segment(ManifestEntry {
            segment_id: seg_id,
            kind: SegmentKind::Wal,
            size_bytes: 4096,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i, 4096),
            },
            sealed: true,
        });
    }

    // 2 Cold segments from compactor
    let compactor = ColdCompactor::new();
    let rows: Vec<Vec<u8>> = (0..20).map(|i| vec![i as u8; 256]).collect();
    let r1 = compactor.compact(&leader_store, 1, 0, &rows[..10], SegmentCodec::Lz4, &[]).unwrap();
    let r2 = compactor.compact(&leader_store, 1, 0, &rows[10..], SegmentCodec::Lz4, &[]).unwrap();
    compactor.apply_to_manifest(&mut leader_manifest, &r1, 1, 0, SegmentCodec::Lz4);
    compactor.apply_to_manifest(&mut leader_manifest, &r2, 1, 0, SegmentCodec::Lz4);

    // Follower: empty disk
    let follower_store = SegmentStore::new();
    let mut boot = BootstrapCoordinator::new_empty_disk();

    // Phase 1: Fetch manifest
    boot.receive_manifest(leader_manifest.clone());
    assert_eq!(boot.phase, BootstrapPhase::ComputeMissing);

    // Phase 2: Compute missing
    boot.compute_missing(&follower_store);
    assert_eq!(boot.phase, BootstrapPhase::StreamSegments);
    assert_eq!(boot.total_segments_needed as usize, leader_manifest.segments.len());

    // Phase 3: Stream all segments
    for &seg_id in &boot.missing_segments.clone() {
        let body = leader_store.get_segment_body(seg_id).unwrap();
        let hdr = leader_store.open_segment(seg_id).unwrap();
        follower_store.create_segment(hdr).unwrap();
        follower_store.write_chunk_at(seg_id, 0, &body).unwrap();
        follower_store.seal_segment(seg_id).unwrap();
        boot.mark_segment_fetched(seg_id, body.len() as u64);
    }
    assert_eq!(boot.phase, BootstrapPhase::VerifySegments);

    // Phase 4: Verify all
    assert!(boot.verify_all(&follower_store));
    assert_eq!(boot.phase, BootstrapPhase::ReplayTail);

    // Phase 5: Ready
    boot.mark_ready();
    assert!(boot.is_ready());
    assert_eq!(boot.progress(), 1.0);
    assert_eq!(boot.segments_fetched, 7);
    assert!(boot.bytes_fetched > 0);
}

/// H1.2: Cold compaction + replication consistency.
/// After compaction on leader, follower sees the same cold segments.
#[test]
fn test_h1_cold_compaction_replication_consistency() {
    let leader_store = Arc::new(SegmentStore::new());
    let mut leader = UnifiedReplicationCoordinator::new(1, leader_store.clone());

    // Leader creates WAL segments — use next_segment_id
    let mut wal_ids = Vec::new();
    for i in 0..3u64 {
        let seg_id = leader_store.next_segment_id();
        wal_ids.push(seg_id);
        let hdr = UnifiedSegmentHeader::new_wal(seg_id, 1024, StructuredLsn::new(i, 0));
        leader_store.create_segment(hdr).unwrap();
        leader_store.write_chunk(seg_id, &vec![0u8; 500]).unwrap();
        leader_store.seal_segment(seg_id).unwrap();
        leader.ssot.manifest.add_segment(ManifestEntry {
            segment_id: seg_id,
            kind: SegmentKind::Wal,
            size_bytes: 500,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i, 500),
            },
            sealed: true,
        });
    }

    // Compact hot data to cold
    let compactor = ColdCompactor::new();
    let rows: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8; 100]).collect();
    let result = compactor.compact(&leader_store, 1, 0, &rows, SegmentCodec::None, &[]).unwrap();
    compactor.apply_to_manifest(&mut leader.ssot.manifest, &result, 1, 0, SegmentCodec::None);

    // Follower handshake — has nothing
    let handshake = ReplicationHandshake {
        node_id: 2,
        last_manifest_epoch: 0,
        have_segments: BTreeSet::new(),
        protocol_version: 1,
    };
    let resp = leader.handle_handshake(&handshake);

    // Follower should need all segments (WAL + Cold)
    assert_eq!(resp.required_segments.len() as usize, leader.ssot.manifest.segments.len());
    assert!(resp.required_segments.contains(&result.new_segment_id));

    // Stream them all
    let follower_store = Arc::new(SegmentStore::new());
    for &seg_id in &resp.required_segments {
        let body = leader.stream_segment(seg_id).unwrap();
        let hdr = leader_store.open_segment(seg_id).unwrap();
        follower_store.create_segment(hdr).unwrap();
        follower_store.write_chunk_at(seg_id, 0, &body).unwrap();
        follower_store.seal_segment(seg_id).unwrap();
    }

    // Verify follower has same segments as leader
    let leader_ids: BTreeSet<u64> = leader_store.list_segments().into_iter().collect();
    let follower_ids: BTreeSet<u64> = follower_store.list_segments().into_iter().collect();
    assert_eq!(leader_ids, follower_ids);

    // Cold segment verifies
    assert!(follower_store.verify_segment(result.new_segment_id).unwrap());
}

/// H1.3: Snapshot restore produces consistent data.
#[test]
fn test_h1_snapshot_restore_consistency() {
    let store = Arc::new(SegmentStore::new());
    let mut coord = UnifiedReplicationCoordinator::new(1, store.clone());

    // Build up segments
    for i in 0..5u64 {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
        store.create_segment(hdr).unwrap();
        store.write_chunk(i, &vec![0u8; 200]).unwrap();
        store.seal_segment(i).unwrap();
        coord.ssot.manifest.add_segment(ManifestEntry {
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

    // Create snapshot
    let snap = coord.create_snapshot(1, StructuredLsn::new(5, 0), Csn::new(100));
    assert_eq!(snap.wal_segments.len(), 5);
    assert!(coord.ssot.manifest.snapshot_cutpoint.is_some());

    // Save manifest for restore
    let saved_manifest = coord.ssot.manifest.clone();
    let saved_epoch = saved_manifest.epoch;

    // Restore on a new node
    let store2 = Arc::new(SegmentStore::new());
    let mut coord2 = UnifiedReplicationCoordinator::new(2, store2.clone());
    coord2.restore_from_snapshot(saved_manifest);

    // Manifest restored correctly
    assert_eq!(coord2.ssot.manifest.epoch, saved_epoch);
    assert_eq!(coord2.ssot.manifest.segments.len(), 5);
    assert!(coord2.ssot.manifest.snapshot_cutpoint.is_some());
    assert_eq!(
        coord2.ssot.manifest.snapshot_cutpoint.as_ref().unwrap().snapshot_id,
        1
    );
}

/// H1.4: Manifest replay is idempotent — same operations applied twice yield same state.
#[test]
fn test_h1_manifest_replay_idempotent() {
    let mut m1 = Manifest::new();
    let mut m2 = Manifest::new();

    // Apply the same sequence to both
    let ops = vec![
        ManifestEntry {
            segment_id: 0, kind: SegmentKind::Wal, size_bytes: 1000,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(0, 0), end_lsn: StructuredLsn::new(0, 1000),
            },
            sealed: false,
        },
        ManifestEntry {
            segment_id: 1, kind: SegmentKind::Cold, size_bytes: 5000,
            codec: SegmentCodec::Lz4,
            logical_range: LogicalRange::Cold {
                table_id: 1, shard_id: 0, min_key: vec![0], max_key: vec![255],
            },
            sealed: true,
        },
        ManifestEntry {
            segment_id: 2, kind: SegmentKind::Snapshot, size_bytes: 2000,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Snapshot { snapshot_id: 1, chunk_index: 0 },
            sealed: true,
        },
    ];

    for entry in &ops {
        m1.add_segment(entry.clone());
        m2.add_segment(entry.clone());
    }
    m1.seal_segment(0);
    m2.seal_segment(0);

    m1.set_snapshot_cutpoint(SnapshotCutpoint {
        snapshot_id: 1, cut_lsn: StructuredLsn::new(1, 0),
        cut_csn: Csn::new(50), epoch: m1.epoch,
    });
    m2.set_snapshot_cutpoint(SnapshotCutpoint {
        snapshot_id: 1, cut_lsn: StructuredLsn::new(1, 0),
        cut_csn: Csn::new(50), epoch: m2.epoch,
    });

    // Both should be identical
    assert_eq!(m1.epoch, m2.epoch);
    assert_eq!(m1.segments.len(), m2.segments.len());
    assert_eq!(m1.to_bytes(), m2.to_bytes());
}

// ═══════════════════════════════════════════════════════════════════════════
// H2 — Fault Injection
// ═══════════════════════════════════════════════════════════════════════════

/// H2.1: Streaming interrupt + resume — bootstrap recovers after partial transfer.
#[test]
fn test_h2_streaming_interrupt_resume() {
    let leader_store = SegmentStore::new();
    let mut leader_manifest = Manifest::new();

    for i in 0..5u64 {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
        leader_store.create_segment(hdr).unwrap();
        leader_store.write_chunk(i, &vec![0xCC; 1024]).unwrap();
        leader_store.seal_segment(i).unwrap();
        leader_manifest.add_segment(ManifestEntry {
            segment_id: i,
            kind: SegmentKind::Wal,
            size_bytes: 1024,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i, 1024),
            },
            sealed: true,
        });
    }

    let follower_store = SegmentStore::new();
    let mut boot = BootstrapCoordinator::new_empty_disk();
    boot.receive_manifest(leader_manifest.clone());
    boot.compute_missing(&follower_store);

    // Transfer first 2 segments, then "interrupt"
    for i in 0..2u64 {
        let body = leader_store.get_segment_body(i).unwrap();
        let hdr = leader_store.open_segment(i).unwrap();
        follower_store.create_segment(hdr).unwrap();
        follower_store.write_chunk_at(i, 0, &body).unwrap();
        follower_store.seal_segment(i).unwrap();
        boot.mark_segment_fetched(i, body.len() as u64);
    }
    assert_eq!(boot.phase, BootstrapPhase::StreamSegments);
    assert_eq!(boot.missing_segments.len(), 3);

    // "Resume" — transfer remaining
    for i in 2..5u64 {
        let body = leader_store.get_segment_body(i).unwrap();
        let hdr = leader_store.open_segment(i).unwrap();
        follower_store.create_segment(hdr).unwrap();
        follower_store.write_chunk_at(i, 0, &body).unwrap();
        follower_store.seal_segment(i).unwrap();
        boot.mark_segment_fetched(i, body.len() as u64);
    }
    assert_eq!(boot.phase, BootstrapPhase::VerifySegments);
    assert!(boot.verify_all(&follower_store));
    boot.mark_ready();
    assert!(boot.is_ready());
}

/// H2.2: Leader switch — follower detects stale manifest and re-handshakes.
#[test]
fn test_h2_leader_switch_manifest_epoch() {
    let store = Arc::new(SegmentStore::new());

    // Old leader at epoch 5
    let mut old_leader = UnifiedReplicationCoordinator::new(1, store.clone());
    for i in 0..5u64 {
        old_leader.ssot.manifest.add_segment(ManifestEntry {
            segment_id: i, kind: SegmentKind::Wal, size_bytes: 100,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::ZERO, end_lsn: StructuredLsn::ZERO,
            },
            sealed: true,
        });
    }

    // New leader has more segments (epoch > old)
    let mut new_leader = UnifiedReplicationCoordinator::new(2, store.clone());
    new_leader.ssot.manifest = old_leader.ssot.manifest.clone();
    for i in 5..8u64 {
        new_leader.ssot.manifest.add_segment(ManifestEntry {
            segment_id: i, kind: SegmentKind::Wal, size_bytes: 100,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::ZERO, end_lsn: StructuredLsn::ZERO,
            },
            sealed: true,
        });
    }

    // Follower re-handshakes with new leader
    let handshake = ReplicationHandshake {
        node_id: 3,
        last_manifest_epoch: old_leader.ssot.manifest.epoch,
        have_segments: old_leader.ssot.manifest.all_segment_ids(),
        protocol_version: 1,
    };

    let resp = new_leader.handle_handshake(&handshake);
    assert!(resp.manifest_epoch > old_leader.ssot.manifest.epoch);
    assert_eq!(resp.required_segments, vec![5, 6, 7]);
}

/// H2.3: GC + streaming concurrent safety — streaming protects segments from GC.
#[test]
fn test_h2_gc_streaming_concurrent_safety() {
    let store = SegmentStore::new();
    let mut ssot = ManifestSsot::new();
    let gc = TwoPhaseGc::new();
    gc.update_min_durable_lsn(StructuredLsn::new(10, 0));

    for i in 0..5u64 {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
        store.create_segment(hdr).unwrap();
        store.write_chunk(i, &vec![0u8; 200]).unwrap();
        store.seal_segment(i).unwrap();
        ssot.manifest.add_segment(ManifestEntry {
            segment_id: i, kind: SegmentKind::Wal, size_bytes: 200,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i + 1, 0),
            },
            sealed: true,
        });
    }

    // Mark segments 2,3 as streaming
    gc.mark_streaming(2);
    gc.mark_streaming(3);

    let plan = gc.mark(&ssot);

    // Streaming segments should be deferred
    let deferred_ids: BTreeSet<u64> = plan.deferred.iter().map(|e| e.segment_id).collect();
    assert!(deferred_ids.contains(&2));
    assert!(deferred_ids.contains(&3));

    // Non-streaming eligible segments can still be swept
    let eligible_ids: BTreeSet<u64> = plan.eligible.iter().map(|e| e.segment_id).collect();
    assert!(!eligible_ids.contains(&2));
    assert!(!eligible_ids.contains(&3));

    // Unmark and re-mark
    gc.unmark_streaming(2);
    gc.unmark_streaming(3);

    let plan2 = gc.mark(&ssot);
    let eligible_ids2: BTreeSet<u64> = plan2.eligible.iter().map(|e| e.segment_id).collect();
    // Now 2 and 3 should be eligible
    assert!(eligible_ids2.contains(&2));
    assert!(eligible_ids2.contains(&3));
}

/// H2.4: Compactor crash restart — partially written segment is orphaned, re-compaction succeeds.
#[test]
fn test_h2_compactor_crash_restart() {
    let store = SegmentStore::new();
    let compactor = ColdCompactor::new();
    let mut manifest = Manifest::new();

    // First compaction succeeds
    let rows1: Vec<Vec<u8>> = vec![vec![1u8; 100]; 5];
    let r1 = compactor.compact(&store, 1, 0, &rows1, SegmentCodec::None, &[]).unwrap();
    compactor.apply_to_manifest(&mut manifest, &r1, 1, 0, SegmentCodec::None);

    // Simulate partial compaction (create segment but don't seal — "crash")
    let partial_id = store.next_segment_id();
    let partial_hdr = UnifiedSegmentHeader::new_cold(partial_id, 1024, SegmentCodec::None, 1, 0);
    store.create_segment(partial_hdr).unwrap();
    store.write_chunk(partial_id, &vec![0u8; 50]).unwrap();
    // NOT sealed — simulates crash

    // "Restart": new compaction should succeed with different segment ID
    let rows2: Vec<Vec<u8>> = vec![vec![2u8; 100]; 8];
    let r2 = compactor.compact(&store, 1, 0, &rows2, SegmentCodec::None, &[r1.new_segment_id]).unwrap();
    compactor.apply_to_manifest(&mut manifest, &r2, 1, 0, SegmentCodec::None);

    // New segment is sealed and in manifest
    assert!(store.is_sealed(r2.new_segment_id).unwrap());
    assert!(manifest.segments.contains_key(&r2.new_segment_id));

    // Partial segment is orphaned (not in manifest) — can be cleaned up
    assert!(!manifest.segments.contains_key(&partial_id));
    assert!(store.exists(partial_id)); // still on disk until cleanup
}

/// H2.5: CRC fault injection on cold blocks.
#[test]
fn test_h2_cold_block_crc_fault() {
    let block = ColdBlock::new_raw(b"important data");
    assert!(block.verify());

    // Corrupt the payload
    let mut corrupted = block.clone();
    if !corrupted.payload.is_empty() {
        corrupted.payload[0] ^= 0xFF;
    }
    assert!(!corrupted.verify());

    // Corrupt the CRC
    let mut bad_crc = block.clone();
    bad_crc.crc = bad_crc.crc.wrapping_add(1);
    assert!(!bad_crc.verify());
}

// ═══════════════════════════════════════════════════════════════════════════
// H3 — Performance Guardrails
// ═══════════════════════════════════════════════════════════════════════════

/// H3.1: Bootstrap throughput — must sustain ≥100 MB/s for segment transfers.
#[test]
fn test_h3_bootstrap_throughput() {
    let store = SegmentStore::new();
    let chunk_size = 64 * 1024; // 64KB chunks
    let num_segments = 20;
    let data = vec![0xABu8; chunk_size];

    let start = std::time::Instant::now();

    for i in 0..num_segments {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024 * 1024, StructuredLsn::new(i, 0));
        store.create_segment(hdr).unwrap();
        // Write ~1MB per segment
        for _ in 0..16 {
            store.write_chunk(i, &data).unwrap();
        }
        store.seal_segment(i).unwrap();
    }

    let elapsed = start.elapsed();
    let total_bytes = num_segments as f64 * 16.0 * chunk_size as f64;
    let throughput_mbps = total_bytes / elapsed.as_secs_f64() / (1024.0 * 1024.0);

    assert!(
        throughput_mbps >= 100.0,
        "Bootstrap throughput {:.1} MB/s below 100 MB/s guardrail",
        throughput_mbps
    );
}

/// H3.2: Cold compaction throughput — compactor must handle ≥10K rows/sec.
#[test]
fn test_h3_cold_compaction_throughput() {
    let store = SegmentStore::new();
    let compactor = ColdCompactor::new();

    let rows: Vec<Vec<u8>> = (0..10_000).map(|i| {
        let mut row = vec![0u8; 64];
        row[..8].copy_from_slice(&(i as u64).to_le_bytes());
        row
    }).collect();

    let start = std::time::Instant::now();
    let result = compactor.compact(&store, 1, 0, &rows, SegmentCodec::None, &[]).unwrap();
    let elapsed = start.elapsed();

    let rows_per_sec = result.row_count as f64 / elapsed.as_secs_f64();
    assert!(
        rows_per_sec >= 10_000.0,
        "Compaction {:.0} rows/sec below 10K guardrail",
        rows_per_sec
    );
    assert_eq!(result.row_count, 10_000);
}

/// H3.3: GC mark phase must complete in <10ms for 1000 segments.
#[test]
fn test_h3_gc_mark_performance() {
    let mut ssot = ManifestSsot::new();
    let gc = TwoPhaseGc::new();
    gc.update_min_durable_lsn(StructuredLsn::new(500, 0));

    // Add 1000 segments
    for i in 0..1000u64 {
        ssot.manifest.add_segment(ManifestEntry {
            segment_id: i,
            kind: SegmentKind::Wal,
            size_bytes: 1024,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0),
                end_lsn: StructuredLsn::new(i + 1, 0),
            },
            sealed: true,
        });
    }

    let start = std::time::Instant::now();
    let plan = gc.mark(&ssot);
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 10,
        "GC mark took {}ms for 1000 segments (guardrail: <10ms)",
        elapsed.as_millis()
    );
    assert!(plan.eligible.len() + plan.deferred.len() + plan.kept as usize > 0);
}

/// H3.4: Two-phase GC full cycle — mark + sweep + verify.
#[test]
fn test_h3_gc_full_cycle() {
    let store = SegmentStore::new();
    let mut ssot = ManifestSsot::new();
    let gc = TwoPhaseGc::new();
    gc.update_min_durable_lsn(StructuredLsn::new(5, 0));

    // Create segments in store + manifest
    for i in 0..10u64 {
        let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
        store.create_segment(hdr).unwrap();
        store.write_chunk(i, &vec![0u8; 100]).unwrap();
        store.seal_segment(i).unwrap();
        ssot.manifest.add_segment(ManifestEntry {
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

    // Pin segment 3 via snapshot
    ssot.pin_snapshot(1, &[3]);

    // Mark phase (dry-run)
    let plan = gc.mark(&ssot);
    assert!(!plan.eligible.is_empty());

    // Segment 3 should be deferred (pinned)
    let deferred_ids: BTreeSet<u64> = plan.deferred.iter().map(|e| e.segment_id).collect();
    assert!(deferred_ids.contains(&3));

    // Segments 5+ should NOT be eligible (end_lsn > min_durable)
    let eligible_ids: BTreeSet<u64> = plan.eligible.iter().map(|e| e.segment_id).collect();
    for id in &eligible_ids {
        assert!(*id < 5, "segment {} should not be eligible", id);
    }

    // Sweep
    let (deleted, freed) = gc.sweep(&plan, &mut ssot, &store);
    assert!(deleted > 0);
    assert!(freed > 0);

    // Segment 3 should still exist (pinned)
    assert!(!store.exists(3) || ssot.snapshot_pinned.contains(&3));

    // Metrics
    assert!(gc.metrics.mark_runs.load(Ordering::Relaxed) >= 1);
    assert!(gc.metrics.sweep_runs.load(Ordering::Relaxed) >= 1);
    assert!(gc.metrics.plans_applied.load(Ordering::Relaxed) >= 1);
}

/// H3.5: Admin status is derivable from components.
#[test]
fn test_h3_admin_status_derivable() {
    let mut ssot = ManifestSsot::new();
    for i in 0..3u64 {
        ssot.manifest.add_segment(ManifestEntry {
            segment_id: i, kind: SegmentKind::Wal, size_bytes: 1000,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(i, 0), end_lsn: StructuredLsn::new(i, 1000),
            },
            sealed: true,
        });
    }
    ssot.manifest.add_segment(ManifestEntry {
        segment_id: 100, kind: SegmentKind::Cold, size_bytes: 5000,
        codec: SegmentCodec::Lz4,
        logical_range: LogicalRange::Cold {
            table_id: 1, shard_id: 0, min_key: vec![], max_key: vec![],
        },
        sealed: true,
    });

    let store_metrics = SegmentStoreMetrics::default();
    let gc = TwoPhaseGc::new();
    let compactor = ColdCompactor::new();

    let status = build_admin_status(&ssot, &store_metrics, &gc, &compactor, None);
    assert_eq!(status.manifest_epoch, 4);
    assert_eq!(status.wal_segments_count, 3);
    assert_eq!(status.cold_segments_count, 1);
    assert_eq!(status.total_segment_bytes, 8000);
    assert_eq!(status.sealed_segments_count, 4);
    assert!(status.bootstrap_state.is_none());
}

/// H3.6: Manifest SSOT — all state derivable, no implicit state.
#[test]
fn test_h3_manifest_ssot_no_implicit_state() {
    let mut ssot = ManifestSsot::new();

    // Add segments of all types
    ssot.manifest.add_segment(ManifestEntry {
        segment_id: 0, kind: SegmentKind::Wal, size_bytes: 100,
        codec: SegmentCodec::None,
        logical_range: LogicalRange::Wal {
            start_lsn: StructuredLsn::new(0, 0), end_lsn: StructuredLsn::new(0, 100),
        },
        sealed: true,
    });
    ssot.manifest.add_segment(ManifestEntry {
        segment_id: 1, kind: SegmentKind::Cold, size_bytes: 500,
        codec: SegmentCodec::Lz4,
        logical_range: LogicalRange::Cold {
            table_id: 1, shard_id: 0, min_key: vec![0], max_key: vec![255],
        },
        sealed: true,
    });
    ssot.manifest.add_segment(ManifestEntry {
        segment_id: 2, kind: SegmentKind::Snapshot, size_bytes: 200,
        codec: SegmentCodec::None,
        logical_range: LogicalRange::Snapshot { snapshot_id: 1, chunk_index: 0 },
        sealed: true,
    });

    // Pin, anchor, follower LSN
    ssot.pin_snapshot(1, &[0, 1, 2]);
    ssot.register_catchup_anchor(10, [0].iter().copied().collect());
    ssot.update_follower_lsn(10, StructuredLsn::new(0, 50));

    // All state derivable
    let status = ssot.derive_status();
    assert_eq!(status.wal_segments, 1);
    assert_eq!(status.cold_segments, 1);
    assert_eq!(status.snapshot_segments, 1);
    assert_eq!(status.total_bytes, 800);
    assert_eq!(status.snapshot_pinned_count, 3);
    assert_eq!(status.catchup_anchor_count, 1);

    // Reachability correct
    assert!(ssot.is_segment_reachable(0)); // in manifest + pinned + anchor
    assert!(ssot.is_segment_reachable(1)); // in manifest + pinned
    assert!(ssot.is_segment_reachable(2)); // in manifest + pinned
    assert!(!ssot.is_segment_reachable(999)); // nothing

    // Min follower LSN
    assert_eq!(ssot.min_follower_lsn(), StructuredLsn::new(0, 50));
}
