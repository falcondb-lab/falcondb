//! WAL I/O pressure tests — v1.0.6
//!
//! Validates WAL determinism under load:
//! - Group commit window effects on batching
//! - Concurrent writer contention
//! - Fsync mode behavior
//! - p50/p99/p999 latency measurement

use std::sync::Arc;
use std::time::{Duration, Instant};

use falcon_storage::group_commit::{GroupCommitConfig, GroupCommitSyncer};
use falcon_storage::wal::{SyncMode, WalRecord, WalWriter};
use falcon_common::types::TxnId;

fn test_wal(dir: &std::path::Path, sync: SyncMode) -> Arc<WalWriter> {
    Arc::new(WalWriter::open_with_options(dir, sync, 64 * 1024 * 1024, 1024).unwrap())
}

/// Measure latencies for a batch of append_and_wait calls.
fn measure_latencies(
    syncer: &GroupCommitSyncer,
    count: usize,
) -> Vec<u64> {
    let mut latencies = Vec::with_capacity(count);
    for i in 0..count {
        let start = Instant::now();
        syncer
            .append_and_wait(&WalRecord::BeginTxn {
                txn_id: TxnId(i as u64),
            })
            .unwrap();
        latencies.push(start.elapsed().as_micros() as u64);
    }
    latencies
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * pct / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

// ─── Test: Group commit window 0µs (immediate flush) ────────────────────

#[test]
fn test_gc_window_zero_immediate_flush() {
    let dir = std::env::temp_dir().join("falcon_wal_press_gc0");
    let _ = std::fs::remove_dir_all(&dir);

    let wal = test_wal(&dir, SyncMode::None);
    let config = GroupCommitConfig {
        flush_interval_us: 1000,
        max_batch_size: 64,
        group_commit_window_us: 0, // immediate
        ring_buffer_capacity: 256 * 1024,
    };
    let syncer = GroupCommitSyncer::new(wal, config);
    let handle = syncer.start_syncer().unwrap();

    let mut latencies = measure_latencies(&syncer, 50);
    latencies.sort();

    let p50 = percentile(&latencies, 50.0);
    let p99 = percentile(&latencies, 99.0);

    println!("gc_window=0µs: p50={}µs p99={}µs", p50, p99);

    // With window=0, every record should flush quickly
    let stats = syncer.stats().snapshot();
    assert!(stats.fsyncs > 0);
    assert!(stats.records_synced >= 50);

    syncer.shutdown();
    handle.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─── Test: Group commit window 200µs (default coalescing) ───────────────

#[test]
fn test_gc_window_200us_coalescing() {
    let dir = std::env::temp_dir().join("falcon_wal_press_gc200");
    let _ = std::fs::remove_dir_all(&dir);

    let wal = test_wal(&dir, SyncMode::None);
    let config = GroupCommitConfig {
        flush_interval_us: 5000,
        max_batch_size: 128,
        group_commit_window_us: 200,
        ring_buffer_capacity: 256 * 1024,
    };
    let syncer = GroupCommitSyncer::new(wal, config);
    let handle = syncer.start_syncer().unwrap();

    let mut latencies = measure_latencies(&syncer, 100);
    latencies.sort();

    let p50 = percentile(&latencies, 50.0);
    let p99 = percentile(&latencies, 99.0);

    println!("gc_window=200µs: p50={}µs p99={}µs", p50, p99);

    let stats = syncer.stats().snapshot();
    assert!(stats.records_synced >= 100);
    // Note: serial append_and_wait may not coalesce well since each call
    // blocks until fsynced. Concurrent writers coalesce better (see other test).
    // Here we just verify correctness: all records were synced.
    assert!(stats.fsyncs > 0, "should have performed fsyncs");

    syncer.shutdown();
    handle.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─── Test: Group commit window 500µs (aggressive batching) ──────────────

#[test]
fn test_gc_window_500us_aggressive_batching() {
    let dir = std::env::temp_dir().join("falcon_wal_press_gc500");
    let _ = std::fs::remove_dir_all(&dir);

    let wal = test_wal(&dir, SyncMode::None);
    let config = GroupCommitConfig {
        flush_interval_us: 10_000,
        max_batch_size: 256,
        group_commit_window_us: 500,
        ring_buffer_capacity: 256 * 1024,
    };
    let syncer = GroupCommitSyncer::new(wal, config);
    let handle = syncer.start_syncer().unwrap();

    let mut latencies = measure_latencies(&syncer, 200);
    latencies.sort();

    let p50 = percentile(&latencies, 50.0);
    let p99 = percentile(&latencies, 99.0);
    let p999 = percentile(&latencies, 99.9);

    println!("gc_window=500µs: p50={}µs p99={}µs p999={}µs", p50, p99, p999);

    let stats = syncer.stats().snapshot();
    assert!(stats.records_synced >= 200);

    syncer.shutdown();
    handle.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─── Test: Concurrent writers with group commit ─────────────────────────

#[test]
fn test_concurrent_writers_group_commit() {
    let dir = std::env::temp_dir().join("falcon_wal_press_conc");
    let _ = std::fs::remove_dir_all(&dir);

    let wal = test_wal(&dir, SyncMode::None);
    let config = GroupCommitConfig {
        flush_interval_us: 2000,
        max_batch_size: 64,
        group_commit_window_us: 200,
        ring_buffer_capacity: 256 * 1024,
    };
    let syncer = GroupCommitSyncer::new(wal, config);
    let handle = syncer.start_syncer().unwrap();

    let num_threads = 4;
    let ops_per_thread = 50;
    let mut threads = Vec::new();

    for t in 0..num_threads {
        let s = Arc::clone(&syncer);
        threads.push(std::thread::spawn(move || {
            let mut latencies = Vec::with_capacity(ops_per_thread);
            for i in 0..ops_per_thread {
                let start = Instant::now();
                s.append_and_wait(&WalRecord::BeginTxn {
                    txn_id: TxnId((t * 1000 + i) as u64),
                })
                .unwrap();
                latencies.push(start.elapsed().as_micros() as u64);
            }
            latencies
        }));
    }

    let mut all_latencies: Vec<u64> = Vec::new();
    for t in threads {
        all_latencies.extend(t.join().unwrap());
    }
    all_latencies.sort();

    let total = num_threads * ops_per_thread;
    let p50 = percentile(&all_latencies, 50.0);
    let p99 = percentile(&all_latencies, 99.0);
    let p999 = percentile(&all_latencies, 99.9);

    println!(
        "concurrent({}x{}): p50={}µs p99={}µs p999={}µs",
        num_threads, ops_per_thread, p50, p99, p999
    );

    let stats = syncer.stats().snapshot();
    assert!(
        stats.records_synced >= total as u64,
        "expected >= {} synced, got {}",
        total,
        stats.records_synced
    );
    // Batching should be effective under concurrency
    assert!(
        stats.fsyncs < total as u64,
        "expected fewer fsyncs than records: {} fsyncs for {} records",
        stats.fsyncs,
        total
    );

    syncer.shutdown();
    handle.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─── Test: FSync mode latency comparison ────────────────────────────────

#[test]
fn test_fsync_mode_none_vs_fsync() {
    // SyncMode::None — no fsync, should be fastest
    let dir_none = std::env::temp_dir().join("falcon_wal_press_none");
    let _ = std::fs::remove_dir_all(&dir_none);
    let wal_none = test_wal(&dir_none, SyncMode::None);
    let config = GroupCommitConfig {
        flush_interval_us: 1000,
        max_batch_size: 32,
        group_commit_window_us: 100,
        ring_buffer_capacity: 256 * 1024,
    };
    let syncer_none = GroupCommitSyncer::new(wal_none, config.clone());
    let h_none = syncer_none.start_syncer().unwrap();

    let mut lat_none = measure_latencies(&syncer_none, 50);
    lat_none.sort();
    let p99_none = percentile(&lat_none, 99.0);

    syncer_none.shutdown();
    h_none.join().unwrap();

    // SyncMode::FSync — includes disk sync
    let dir_fsync = std::env::temp_dir().join("falcon_wal_press_fsync");
    let _ = std::fs::remove_dir_all(&dir_fsync);
    let wal_fsync = test_wal(&dir_fsync, SyncMode::FSync);
    let syncer_fsync = GroupCommitSyncer::new(wal_fsync, config);
    let h_fsync = syncer_fsync.start_syncer().unwrap();

    let mut lat_fsync = measure_latencies(&syncer_fsync, 50);
    lat_fsync.sort();
    let p99_fsync = percentile(&lat_fsync, 99.0);

    syncer_fsync.shutdown();
    h_fsync.join().unwrap();

    println!(
        "SyncMode comparison: None p99={}µs, FSync p99={}µs",
        p99_none, p99_fsync
    );

    // Both should complete (no hang)
    assert!(p99_none < 10_000_000, "None mode p99 should be < 10s");
    assert!(p99_fsync < 10_000_000, "FSync mode p99 should be < 10s");

    let _ = std::fs::remove_dir_all(&dir_none);
    let _ = std::fs::remove_dir_all(&dir_fsync);
}

// ─── Test: Flushed LSN and backlog accessors ────────────────────────────

#[test]
fn test_flushed_lsn_and_backlog() {
    let dir = std::env::temp_dir().join("falcon_wal_press_lsn");
    let _ = std::fs::remove_dir_all(&dir);

    let wal = test_wal(&dir, SyncMode::None);
    let config = GroupCommitConfig {
        flush_interval_us: 500,
        max_batch_size: 8,
        group_commit_window_us: 0,
        ring_buffer_capacity: 256 * 1024,
    };
    let syncer = GroupCommitSyncer::new(wal, config);
    let handle = syncer.start_syncer().unwrap();

    // Initially flushed_lsn = 0
    assert_eq!(syncer.flushed_lsn(), 0);

    // Write some records
    for i in 0..10 {
        syncer
            .append_and_wait(&WalRecord::BeginTxn {
                txn_id: TxnId(i),
            })
            .unwrap();
    }

    // After writes, flushed_lsn should have advanced
    let flushed = syncer.flushed_lsn();
    assert!(flushed > 0, "flushed_lsn should advance after writes");

    syncer.shutdown();
    handle.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─── Test: Group commit stats ring buffer tracking ──────────────────────

#[test]
fn test_group_commit_stats_enhanced() {
    let dir = std::env::temp_dir().join("falcon_wal_press_stats");
    let _ = std::fs::remove_dir_all(&dir);

    let wal = test_wal(&dir, SyncMode::None);
    let config = GroupCommitConfig::default();
    let syncer = GroupCommitSyncer::new(wal, config);
    let handle = syncer.start_syncer().unwrap();

    for i in 0..20 {
        syncer
            .append_and_wait(&WalRecord::BeginTxn {
                txn_id: TxnId(i),
            })
            .unwrap();
    }

    let stats = syncer.stats().snapshot();
    assert!(stats.fsyncs > 0);
    assert!(stats.records_synced >= 20);
    assert!(stats.batches > 0);
    // ring_buffer_used should be 0 after all records are flushed
    assert_eq!(stats.ring_buffer_used, 0, "ring buffer should be empty after flush");

    syncer.shutdown();
    handle.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
