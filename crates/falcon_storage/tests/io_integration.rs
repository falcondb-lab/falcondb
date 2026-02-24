//! Integration tests for the async I/O layer.
//!
//! Tests cover:
//! - AsyncFile write/read/flush/sync roundtrip on current platform
//! - AsyncWalWriter group commit coalescing
//! - AsyncWalWriter crash simulation (durable vs buffered)
//! - Snapshot chunk streaming roundtrip with checksums
//! - WAL + snapshot combined workflow
//! - Metrics accumulation under concurrent load

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use falcon_storage::io::async_file::{AsyncFileConfig, FlushPolicy, FlushReason, SnapshotStreamConfig};
use falcon_storage::io::async_wal_writer::{AsyncWalConfig, AsyncWalWriter};
use falcon_storage::io::snapshot_stream::{SnapshotChunk, SnapshotReader, SnapshotWriter};
use falcon_storage::io::{create_async_file, IoErrorKind};
use falcon_storage::wal::{WalReader, WalRecord};

fn temp_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("falcon_io_integ_{}", name))
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

// ═══════════════════════════════════════════════════════════════════════════
// §1 — AsyncFile platform integration
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_platform_async_file_write_sync_read() {
    let dir = temp_dir("platform_file");
    cleanup(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let path = dir.join("test.dat");
    let file = create_async_file(path.clone(), AsyncFileConfig::default()).unwrap();

    // Write
    let test_data = b"platform async file test";
    file.write_at(0, test_data).unwrap();

    // Sync — durability barrier
    file.sync().unwrap();

    // Read back
    let data = file.read_at(0, test_data.len()).unwrap();
    assert_eq!(&data[..], test_data);

    // Verify file size
    assert_eq!(file.size().unwrap(), test_data.len() as u64);

    cleanup(&dir);
}

#[test]
fn test_platform_async_file_large_sequential_write() {
    let dir = temp_dir("platform_large");
    cleanup(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let path = dir.join("large.dat");
    let cfg = AsyncFileConfig {
        sequential_scan: true,
        ..Default::default()
    };
    let file = create_async_file(path.clone(), cfg).unwrap();

    // Write 1MB in 4KB chunks (simulates WAL append pattern)
    let chunk = vec![0xABu8; 4096];
    let mut offset = 0u64;
    for _ in 0..256 {
        file.write_at(offset, &chunk).unwrap();
        offset += 4096;
    }
    file.sync().unwrap();

    assert_eq!(file.size().unwrap(), 1024 * 1024);

    // Verify first and last chunk
    let first = file.read_at(0, 4096).unwrap();
    assert!(first.iter().all(|&b| b == 0xAB));
    let last = file.read_at((256 - 1) * 4096, 4096).unwrap();
    assert!(last.iter().all(|&b| b == 0xAB));

    cleanup(&dir);
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — AsyncWalWriter integration
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_async_wal_write_and_recover() {
    let dir = temp_dir("wal_recover");
    cleanup(&dir);

    // Write 10 records durably
    {
        let config = AsyncWalConfig {
            flush_policy: FlushPolicy::Explicit,
            max_segment_size: 64 * 1024 * 1024,
            wal_dir: dir.clone(),
            file_config: AsyncFileConfig::default(),
        };
        let writer = AsyncWalWriter::open(config).unwrap();

        for i in 0..10u32 {
            let record = WalRecord::BeginTxn {
                txn_id: falcon_common::types::TxnId(i as u64),
            };
            let data = bincode::serialize(&record).unwrap();
            writer.append_durable(&data).unwrap();
        }
    }

    // Recover: read WAL segments using the existing WalReader
    let reader = WalReader::new(&dir);
    let records = reader.read_all().unwrap();
    assert_eq!(
        records.len(),
        10,
        "all 10 durable records should survive recovery"
    );

    // Verify record types
    for (i, record) in records.iter().enumerate() {
        match record {
            WalRecord::BeginTxn { txn_id } => {
                assert_eq!(txn_id.0, i as u64);
            }
            _ => panic!("unexpected record type at index {}", i),
        }
    }

    cleanup(&dir);
}

#[test]
fn test_async_wal_group_commit_reduces_flushes() {
    let dir = temp_dir("wal_gc_reduce");
    cleanup(&dir);

    let config = AsyncWalConfig {
        flush_policy: FlushPolicy::GroupCommit {
            max_wait_us: 500,
            max_batch_bytes: 4 * 1024 * 1024,
        },
        max_segment_size: 64 * 1024 * 1024,
        wal_dir: dir.clone(),
        file_config: AsyncFileConfig::default(),
    };
    let writer = Arc::new(AsyncWalWriter::open(config).unwrap());
    let handle = writer.start_syncer().unwrap();

    // 100 rapid-fire appends
    for i in 0..100u32 {
        let record = WalRecord::BeginTxn {
            txn_id: falcon_common::types::TxnId(i as u64),
        };
        let data = bincode::serialize(&record).unwrap();
        writer.append_buffered(&data).unwrap();
    }

    // Wait for syncer
    std::thread::sleep(Duration::from_millis(100));

    let m = writer.wal_metrics().snapshot();
    assert_eq!(m.records_written, 100);
    assert!(
        m.sync_count < 100,
        "group commit should coalesce: {} syncs for 100 records",
        m.sync_count
    );
    assert!(
        m.group_commit_batches >= 1,
        "at least one batch expected"
    );
    assert!(
        m.avg_records_per_batch() > 1.0,
        "batch should contain multiple records: avg={}",
        m.avg_records_per_batch()
    );

    writer.shutdown();
    handle.join().unwrap();

    // Verify recovery
    let reader = WalReader::new(&dir);
    let records = reader.read_all().unwrap();
    assert_eq!(records.len(), 100, "all records should be recoverable");

    cleanup(&dir);
}

#[test]
fn test_async_wal_crash_only_durable_survives() {
    let dir = temp_dir("wal_crash_durable");
    cleanup(&dir);

    {
        let config = AsyncWalConfig {
            flush_policy: FlushPolicy::Explicit,
            max_segment_size: 64 * 1024 * 1024,
            wal_dir: dir.clone(),
            file_config: AsyncFileConfig::default(),
        };
        let writer = AsyncWalWriter::open(config).unwrap();

        // Write 5 durable records
        for i in 0..5u32 {
            let record = WalRecord::BeginTxn {
                txn_id: falcon_common::types::TxnId(i as u64),
            };
            let data = bincode::serialize(&record).unwrap();
            writer.append_durable(&data).unwrap();
        }

        // Write 5 more WITHOUT flushing (simulating crash before sync)
        for i in 5..10u32 {
            let record = WalRecord::BeginTxn {
                txn_id: falcon_common::types::TxnId(i as u64),
            };
            let data = bincode::serialize(&record).unwrap();
            writer.append_buffered(&data).unwrap();
        }
        // Drop writer without flushing → simulates crash
    }

    // Recovery should find at least the 5 durable records
    let reader = WalReader::new(&dir);
    let records = reader.read_all().unwrap();
    assert!(
        records.len() >= 5,
        "at least 5 durable records should survive, got {}",
        records.len()
    );
    // The buffered records may or may not survive depending on OS buffering —
    // but we guarantee at least the explicitly synced ones.
    // The key invariant: we never CLAIM durability for unsynced records.

    cleanup(&dir);
}

#[test]
fn test_async_wal_concurrent_writers_durability() {
    let dir = temp_dir("wal_concurrent");
    cleanup(&dir);

    let config = AsyncWalConfig {
        flush_policy: FlushPolicy::GroupCommit {
            max_wait_us: 1000,
            max_batch_bytes: 4 * 1024 * 1024,
        },
        max_segment_size: 64 * 1024 * 1024,
        wal_dir: dir.clone(),
        file_config: AsyncFileConfig::default(),
    };
    let writer = Arc::new(AsyncWalWriter::open(config).unwrap());
    let syncer = writer.start_syncer().unwrap();

    // 8 threads × 50 records = 400 total
    let mut handles = Vec::new();
    for t in 0..8u64 {
        let w = Arc::clone(&writer);
        handles.push(std::thread::spawn(move || {
            for i in 0..50u32 {
                let record = WalRecord::BeginTxn {
                    txn_id: falcon_common::types::TxnId(t * 1000 + i as u64),
                };
                let data = bincode::serialize(&record).unwrap();
                w.append_buffered(&data).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Ensure all flushed
    writer.flush_durable(FlushReason::Explicit).unwrap();

    let m = writer.wal_metrics().snapshot();
    assert_eq!(m.records_written, 400);
    assert!(
        m.sync_count < 400,
        "coalescing expected: {} syncs for 400 records",
        m.sync_count
    );

    writer.shutdown();
    syncer.join().unwrap();

    // Verify recovery
    let reader = WalReader::new(&dir);
    let records = reader.read_all().unwrap();
    assert_eq!(records.len(), 400);

    cleanup(&dir);
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Snapshot chunk streaming integration
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_snapshot_stream_large_file() {
    let dir = temp_dir("snap_large");
    cleanup(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let src_path = dir.join("snapshot_src.dat");
    let dst_path = dir.join("snapshot_dst.dat");

    // Create a 500KB source file with known pattern
    let pattern: Vec<u8> = (0..500 * 1024).map(|i| (i % 256) as u8).collect();
    std::fs::write(&src_path, &pattern).unwrap();

    // Stream read → write with 64KB chunks
    let config = SnapshotStreamConfig {
        chunk_size_bytes: 64 * 1024,
        max_inflight_chunks: 4,
        read_ahead: 2,
    };

    let mut reader = SnapshotReader::open(src_path.clone(), "snap-large".into(), config.clone()).unwrap();
    let mut writer = SnapshotWriter::create(dst_path.clone(), config).unwrap();

    let mut chunk_count = 0;
    while let Some(chunk) = reader.read_next_chunk().unwrap() {
        assert!(chunk.verify_checksum(), "chunk {} checksum failed", chunk.chunk_index);
        writer.write_chunk(&chunk).unwrap();
        chunk_count += 1;
    }
    writer.finalize().unwrap();

    // 500KB / 64KB = 8 chunks (7 full + 1 partial)
    assert_eq!(chunk_count, 8);
    assert_eq!(reader.bytes_read(), 500 * 1024);
    assert_eq!(writer.bytes_written(), 500 * 1024);

    // Verify destination matches source
    let dst_data = std::fs::read(&dst_path).unwrap();
    assert_eq!(dst_data, pattern);

    cleanup(&dir);
}

#[test]
fn test_snapshot_stream_rejects_corrupt_chunk() {
    let dir = temp_dir("snap_corrupt");
    cleanup(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let dst_path = dir.join("snapshot_corrupt.dat");
    let config = SnapshotStreamConfig::default();
    let mut writer = SnapshotWriter::create(dst_path.clone(), config).unwrap();

    // Good chunk
    let good = SnapshotChunk {
        snapshot_id: "snap-c".into(),
        offset: 0,
        data: b"good data".to_vec(),
        chunk_index: 0,
        is_last: false,
        checksum: crc32fast::hash(b"good data"),
    };
    writer.write_chunk(&good).unwrap();

    // Corrupt chunk (bad checksum)
    let corrupt = SnapshotChunk {
        snapshot_id: "snap-c".into(),
        offset: 9,
        data: b"bad data".to_vec(),
        chunk_index: 1,
        is_last: true,
        checksum: 0xDEADBEEF,
    };
    let result = writer.write_chunk(&corrupt);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind, IoErrorKind::Corrupt);

    cleanup(&dir);
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — WAL + Snapshot combined workflow
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_wal_then_snapshot_workflow() {
    let dir = temp_dir("wal_snap_combined");
    cleanup(&dir);

    let wal_dir = dir.join("wal");
    let snap_src = dir.join("snapshot.dat");

    // Phase 1: Write WAL records
    {
        let config = AsyncWalConfig {
            flush_policy: FlushPolicy::Explicit,
            wal_dir: wal_dir.clone(),
            ..Default::default()
        };
        let writer = AsyncWalWriter::open(config).unwrap();

        for i in 0..20u32 {
            let record = WalRecord::BeginTxn {
                txn_id: falcon_common::types::TxnId(i as u64),
            };
            let data = bincode::serialize(&record).unwrap();
            writer.append_buffered(&data).unwrap();
        }
        writer.flush_durable(FlushReason::Explicit).unwrap();
    }

    // Phase 2: Create a snapshot from the WAL data
    std::fs::create_dir_all(&dir).unwrap();
    let snap_data = vec![0x55u8; 256 * 1024]; // 256KB simulated snapshot
    std::fs::write(&snap_src, &snap_data).unwrap();

    // Phase 3: Stream the snapshot
    let snap_dst = dir.join("snapshot_replica.dat");
    let config = SnapshotStreamConfig {
        chunk_size_bytes: 64 * 1024,
        ..Default::default()
    };
    let mut reader = SnapshotReader::open(snap_src.clone(), "snap-combined".into(), config.clone()).unwrap();
    let mut writer = SnapshotWriter::create(snap_dst.clone(), config).unwrap();

    while let Some(chunk) = reader.read_next_chunk().unwrap() {
        writer.write_chunk(&chunk).unwrap();
    }
    writer.finalize().unwrap();

    // Phase 4: Verify both WAL and snapshot
    let wal_reader = WalReader::new(&wal_dir);
    let records = wal_reader.read_all().unwrap();
    assert_eq!(records.len(), 20);

    let snap_result = std::fs::read(&snap_dst).unwrap();
    assert_eq!(snap_result, snap_data);

    cleanup(&dir);
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Latency micro-benchmark (evidence collection)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_wal_latency_evidence() {
    // This test collects latency evidence for the benchmark report.
    // It runs both sync and async WAL paths and prints metrics.
    let dir = temp_dir("wal_latency");
    cleanup(&dir);

    let n = 1000u32;

    // ── Baseline: explicit flush per record (worst case) ──
    let baseline_dir = dir.join("baseline");
    {
        let config = AsyncWalConfig {
            flush_policy: FlushPolicy::Explicit,
            wal_dir: baseline_dir.clone(),
            ..Default::default()
        };
        let writer = AsyncWalWriter::open(config).unwrap();
        let start = Instant::now();
        for i in 0..n {
            let record = WalRecord::BeginTxn {
                txn_id: falcon_common::types::TxnId(i as u64),
            };
            let data = bincode::serialize(&record).unwrap();
            writer.append_durable(&data).unwrap();
        }
        let baseline_elapsed = start.elapsed();
        let m = writer.wal_metrics().snapshot();
        eprintln!(
            "BASELINE: {} records in {:?} | syncs={} | avg_sync={}us | max_sync={}us",
            n,
            baseline_elapsed,
            m.sync_count,
            m.avg_sync_latency_us(),
            m.max_sync_latency_us,
        );
    }

    // ── IOCP/Async: group commit (production mode) ──
    let iocp_dir = dir.join("iocp");
    {
        let config = AsyncWalConfig {
            flush_policy: FlushPolicy::GroupCommit {
                max_wait_us: 1000,
                max_batch_bytes: 4 * 1024 * 1024,
            },
            wal_dir: iocp_dir.clone(),
            file_config: AsyncFileConfig {
                async_io_enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let writer = Arc::new(AsyncWalWriter::open(config).unwrap());
        let syncer = writer.start_syncer().unwrap();

        let start = Instant::now();
        for i in 0..n {
            let record = WalRecord::BeginTxn {
                txn_id: falcon_common::types::TxnId(10000 + i as u64),
            };
            let data = bincode::serialize(&record).unwrap();
            writer.append_buffered(&data).unwrap();
        }
        writer.flush_durable(FlushReason::Explicit).unwrap();
        let iocp_elapsed = start.elapsed();

        let m = writer.wal_metrics().snapshot();
        eprintln!(
            "IOCP:     {} records in {:?} | syncs={} | avg_sync={}us | max_sync={}us | batches={} | avg_batch={}",
            n,
            iocp_elapsed,
            m.sync_count,
            m.avg_sync_latency_us(),
            m.max_sync_latency_us,
            m.group_commit_batches,
            m.avg_records_per_batch(),
        );

        assert!(
            m.sync_count < n as u64,
            "IOCP mode should have fewer syncs ({}) than records ({})",
            m.sync_count,
            n
        );

        writer.shutdown();
        syncer.join().unwrap();
    }

    // Both should recover correctly
    let baseline_records = WalReader::new(&baseline_dir).read_all().unwrap();
    assert_eq!(baseline_records.len(), n as usize, "baseline recovery");
    let iocp_records = WalReader::new(&iocp_dir).read_all().unwrap();
    assert!(
        iocp_records.len() >= n as usize,
        "iocp recovery: expected >= {}, got {}",
        n,
        iocp_records.len()
    );

    cleanup(&dir);
}
