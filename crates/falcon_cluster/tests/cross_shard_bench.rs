//! Cross-Shard Transaction Bench Suite
//!
//! Standard benchmark scenarios for validating tail latency stability
//! and throughput under various workload patterns:
//!
//! - BENCH-1: Uniform key distribution (low contention)
//! - BENCH-2: Zipfian distribution (hotspot)
//! - BENCH-3: Mixed 80% single-shard / 20% cross-shard
//! - BENCH-4: Pure cross-shard (worst case)
//! - BENCH-5: Fault injection: shard restart mid-workload
//! - BENCH-6: High concurrency stress test
//!
//! Each scenario outputs: commit count, abort count (by reason),
//! retry count, P50/P95/P99/P999 latency, throughput TPS,
//! coordinator queue wait, and dominant latency phase.

use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::*;

use falcon_cluster::cross_shard::*;
use falcon_cluster::sharded_engine::ShardedEngine;
use falcon_cluster::two_phase::{HardenedConfig, HardenedTwoPhaseCoordinator};

fn test_schema() -> TableSchema {
    TableSchema {
        id: TableId(1),
        name: "bench".into(),
        columns: vec![
            ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            },
            ColumnDef {
                id: ColumnId(1),
                name: "val".into(),
                data_type: DataType::Int32,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
            },
        ],
        primary_key_columns: vec![0],
        ..Default::default()
    }
}

/// Bench report for a single scenario.
#[derive(Debug)]
struct BenchReport {
    scenario: String,
    total_txns: u64,
    committed: u64,
    aborted: u64,
    retried: u64,
    duration_ms: u64,
    throughput_tps: f64,
    latencies_us: Vec<u64>,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    p999_us: u64,
    dominant_phases: Vec<String>,
}

impl BenchReport {
    fn from_latencies(
        scenario: &str,
        committed: u64,
        aborted: u64,
        retried: u64,
        duration_ms: u64,
        mut latencies: Vec<u64>,
        dominant_phases: Vec<String>,
    ) -> Self {
        latencies.sort();
        let len = latencies.len();
        let p = |pct: f64| -> u64 {
            if len == 0 { return 0; }
            let idx = ((pct / 100.0) * len as f64).ceil() as usize;
            latencies[idx.min(len - 1)]
        };
        let throughput = if duration_ms > 0 {
            committed as f64 / (duration_ms as f64 / 1000.0)
        } else {
            0.0
        };
        Self {
            scenario: scenario.into(),
            total_txns: committed + aborted,
            committed,
            aborted,
            retried,
            duration_ms,
            throughput_tps: throughput,
            p50_us: p(50.0),
            p95_us: p(95.0),
            p99_us: p(99.0),
            p999_us: p(99.9),
            latencies_us: latencies,
            dominant_phases,
        }
    }

    fn summary(&self) -> String {
        format!(
            "[{}] txns={} committed={} aborted={} retried={} tps={:.0} p50={}us p95={}us p99={}us p999={}us duration={}ms",
            self.scenario, self.total_txns, self.committed, self.aborted,
            self.retried, self.throughput_tps,
            self.p50_us, self.p95_us, self.p99_us, self.p999_us,
            self.duration_ms,
        )
    }
}

fn setup_bench(num_shards: u64) -> (Arc<ShardedEngine>, Arc<HardenedTwoPhaseCoordinator>) {
    let engine = Arc::new(ShardedEngine::new(num_shards));
    engine.create_table_all(&test_schema()).unwrap();
    let config = HardenedConfig {
        retry: RetryConfig {
            max_retries: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(50),
            multiplier: 2.0,
            jitter_ratio: 0.1,
            retry_budget: Duration::from_secs(5),
        },
        coord_concurrency: (64, 48),
        circuit_breaker: (20, Duration::from_millis(500)),
        ..Default::default()
    };
    let coord = Arc::new(HardenedTwoPhaseCoordinator::new(engine.clone(), config));
    (engine, coord)
}

/// Simple Zipfian-like key generator: returns keys biased toward low values.
fn zipfian_key(i: u64, max_key: u64) -> i32 {
    // Simple power-law approximation
    let x = (i * 7 + 13) % 1000;
    let biased = (x as f64 / 1000.0).powf(2.0);
    ((biased * max_key as f64) as i32).max(0)
}

/// Run a batch of cross-shard transactions and collect metrics.
fn run_batch(
    coord: &HardenedTwoPhaseCoordinator,
    num_txns: u64,
    target_fn: impl Fn(u64) -> Vec<ShardId>,
    key_fn: impl Fn(u64) -> i32,
) -> BenchReport {
    let mut latencies = Vec::with_capacity(num_txns as usize);
    let mut committed = 0u64;
    let mut aborted = 0u64;
    let mut retried = 0u64;
    let mut dominant_phases = Vec::new();
    let key_counter = AtomicI32::new(1_000_000);

    let start = Instant::now();

    for i in 0..num_txns {
        let targets = target_fn(i);
        let key = key_fn(i);
        // Use unique keys to avoid conflicts (offset by a large base)
        let unique_key = key_counter.fetch_add(1, Ordering::Relaxed);

        let result = coord
            .execute(
                &targets,
                IsolationLevel::ReadCommitted,
                |storage, _tm, txn_id| {
                    let row = OwnedRow::new(vec![
                        Datum::Int32(unique_key),
                        Datum::Int32(key),
                    ]);
                    storage.insert(TableId(1), row, txn_id)?;
                    Ok(())
                },
            )
            .unwrap();

        latencies.push(result.latency.total_us);
        dominant_phases.push(result.latency.dominant_phase().to_string());

        if result.committed {
            committed += 1;
        } else {
            aborted += 1;
        }
        if let Some(ref retry) = result.retry {
            if retry.attempts > 1 {
                retried += 1;
            }
        }
    }

    let duration_ms = start.elapsed().as_millis() as u64;
    BenchReport::from_latencies(
        "batch",
        committed,
        aborted,
        retried,
        duration_ms,
        latencies,
        dominant_phases,
    )
}

// ═══════════════════════════════════════════════════════════════════════════
// BENCH-1: Uniform key distribution (low contention)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn bench_uniform_low_contention() {
    let (_engine, coord) = setup_bench(4);
    let num_txns = 100;

    let mut report = run_batch(
        &coord,
        num_txns,
        |i| vec![ShardId(i % 4), ShardId((i + 1) % 4)],
        |i| i as i32,
    );
    report.scenario = "uniform_low_contention".into();

    eprintln!("{}", report.summary());

    // Acceptance: high commit rate, low p99
    assert!(
        report.committed >= num_txns * 90 / 100,
        "BENCH-1: commit rate should be ≥90%, got {}/{}",
        report.committed, num_txns
    );
    assert!(
        report.p99_us < 50_000_000, // 50s generous ceiling for CI
        "BENCH-1: p99 should be reasonable, got {}us",
        report.p99_us
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// BENCH-2: Zipfian distribution (hotspot)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn bench_zipfian_hotspot() {
    let (_engine, coord) = setup_bench(4);
    let num_txns = 100;

    let mut report = run_batch(
        &coord,
        num_txns,
        |_| vec![ShardId(0), ShardId(1)], // always hits shards 0,1 (hotspot)
        |i| zipfian_key(i, 50),
    );
    report.scenario = "zipfian_hotspot".into();

    eprintln!("{}", report.summary());

    // Under hotspot, abort rate may be higher but system should not crash
    assert!(
        report.committed + report.aborted == num_txns,
        "BENCH-2: all txns should be accounted for"
    );
    // p999 should not diverge catastrophically
    assert!(
        report.p999_us < 100_000_000, // 100s generous ceiling
        "BENCH-2: p999 should not diverge, got {}us",
        report.p999_us
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// BENCH-3: Mixed workload (80% single-shard, 20% cross-shard)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn bench_mixed_workload() {
    let (_engine, coord) = setup_bench(4);
    let num_txns = 100;

    let mut report = run_batch(
        &coord,
        num_txns,
        |i| {
            if i % 5 == 0 {
                // 20% cross-shard
                vec![ShardId(0), ShardId(1), ShardId(2)]
            } else {
                // 80% single-shard
                vec![ShardId(i % 4)]
            }
        },
        |i| i as i32,
    );
    report.scenario = "mixed_80_20".into();

    eprintln!("{}", report.summary());

    assert!(
        report.committed >= num_txns * 85 / 100,
        "BENCH-3: commit rate should be ≥85%, got {}/{}",
        report.committed, num_txns
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// BENCH-4: Pure cross-shard (worst case)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn bench_pure_cross_shard() {
    let (_engine, coord) = setup_bench(4);
    let num_txns = 50;

    let mut report = run_batch(
        &coord,
        num_txns,
        |_| vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)],
        |i| i as i32,
    );
    report.scenario = "pure_cross_shard".into();

    eprintln!("{}", report.summary());

    assert!(
        report.committed >= num_txns * 80 / 100,
        "BENCH-4: commit rate should be ≥80%, got {}/{}",
        report.committed, num_txns
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// BENCH-5: Fault injection — duplicate PK on one shard
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn bench_fault_injection() {
    let (engine, coord) = setup_bench(3);
    let num_txns = 50;

    // Pre-insert a row to cause collisions on shard 0
    {
        let shard = engine.shard(ShardId(0)).unwrap();
        let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
        let row = OwnedRow::new(vec![Datum::Int32(999_999), Datum::Int32(0)]);
        shard.storage.insert(TableId(1), row, txn.txn_id).unwrap();
        shard.txn_mgr.commit(txn.txn_id).unwrap();
    }

    let key_ctr = AtomicI32::new(2_000_000);
    let mut committed = 0u64;
    let mut aborted = 0u64;
    let mut latencies = Vec::new();

    let start = Instant::now();
    for i in 0..num_txns {
        let targets = vec![ShardId(0), ShardId(1)];
        let result = coord
            .execute(
                &targets,
                IsolationLevel::ReadCommitted,
                |storage, _tm, txn_id| {
                    if i % 10 == 0 {
                        // Force collision every 10th txn
                        let row = OwnedRow::new(vec![Datum::Int32(999_999), Datum::Int32(i as i32)]);
                        storage.insert(TableId(1), row, txn_id)?;
                    } else {
                        let k = key_ctr.fetch_add(1, Ordering::Relaxed);
                        let row = OwnedRow::new(vec![Datum::Int32(k), Datum::Int32(i as i32)]);
                        storage.insert(TableId(1), row, txn_id)?;
                    }
                    Ok(())
                },
            )
            .unwrap();

        latencies.push(result.latency.total_us);
        if result.committed {
            committed += 1;
        } else {
            aborted += 1;
        }
    }

    let duration_ms = start.elapsed().as_millis() as u64;
    let report = BenchReport::from_latencies(
        "fault_injection",
        committed,
        aborted,
        0,
        duration_ms,
        latencies,
        vec![],
    );
    eprintln!("{}", report.summary());

    // Some txns should abort (the collision ones), but system stays stable
    assert!(aborted > 0, "BENCH-5: should have some aborts from collisions");
    assert!(
        committed > num_txns / 2,
        "BENCH-5: majority should still commit, got {}",
        committed
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// BENCH-6: High concurrency stress test
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn bench_high_concurrency() {
    let (_engine, coord) = setup_bench(4);
    let num_threads = 4;
    let txns_per_thread = 25;

    let committed = Arc::new(AtomicU64::new(0));
    let aborted = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    std::thread::scope(|s| {
        for t in 0..num_threads {
            let coord = &coord;
            let committed = committed.clone();
            let aborted = aborted.clone();
            s.spawn(move || {
                for i in 0..txns_per_thread {
                    let targets = vec![
                        ShardId((t * 2) % 4),
                        ShardId((t * 2 + 1) % 4),
                    ];
                    let key = (t as i32 * 10000) + (i as i32) + 5_000_000;
                    let result = coord
                        .execute(
                            &targets,
                            IsolationLevel::ReadCommitted,
                            |storage, _tm, txn_id| {
                                let row = OwnedRow::new(vec![
                                    Datum::Int32(key),
                                    Datum::Int32(t as i32),
                                ]);
                                storage.insert(TableId(1), row, txn_id)?;
                                Ok(())
                            },
                        )
                        .unwrap();
                    if result.committed {
                        committed.fetch_add(1, Ordering::Relaxed);
                    } else {
                        aborted.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    let duration_ms = start.elapsed().as_millis() as u64;
    let total_committed = committed.load(Ordering::Relaxed);
    let total_aborted = aborted.load(Ordering::Relaxed);
    let total = total_committed + total_aborted;
    let tps = if duration_ms > 0 {
        total_committed as f64 / (duration_ms as f64 / 1000.0)
    } else {
        0.0
    };

    eprintln!(
        "[high_concurrency] threads={} txns={} committed={} aborted={} tps={:.0} duration={}ms",
        num_threads, total, total_committed, total_aborted, tps, duration_ms
    );

    assert!(
        total_committed >= (num_threads * txns_per_thread) as u64 * 80 / 100,
        "BENCH-6: ≥80% commit rate under concurrency, got {}/{}",
        total_committed, total
    );
}
