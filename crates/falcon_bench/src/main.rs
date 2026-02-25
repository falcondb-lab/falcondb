#![allow(clippy::format_push_string)] // bench CLI output formatting
//! Falcon YCSB-style benchmark harness.
//!
//! Runs configurable workloads against the in-memory storage engine and
//! transaction manager, measuring commit latency and throughput partitioned
//! by Fast-Path (LocalTxn) vs Slow-Path (GlobalTxn).
//!
//! Usage:
//!   cargo run -p falcon_bench -- --ops 10000 --read-pct 50 --local-pct 80 --shards 4
//!   cargo run -p falcon_bench -- --export csv

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;

use falcon_cluster::distributed_exec::{AggMerge, DistributedExecutor, GatherStrategy, SubPlan};
use falcon_cluster::sharded_engine::ShardedEngine;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::{ColumnId, DataType, IsolationLevel, ShardId, TableId};
use falcon_storage::engine::StorageEngine;
use falcon_txn::manager::{SlowPathMode, TxnClassification, TxnManager};

/// Falcon YCSB-style benchmark.
#[derive(Parser, Debug)]
#[command(name = "falcon-bench", about = "YCSB-style benchmark for Falcon")]
struct Args {
    /// Total number of operations.
    #[arg(long, default_value_t = 10000)]
    ops: u64,

    /// Percentage of read operations (0-100). Remainder are writes.
    #[arg(long, default_value_t = 50)]
    read_pct: u8,

    /// Percentage of transactions that are LocalTxn (single-shard).
    #[arg(long, default_value_t = 80)]
    local_pct: u8,

    /// Number of logical shards.
    #[arg(long, default_value_t = 4)]
    shards: u64,

    /// Number of pre-loaded rows.
    #[arg(long, default_value_t = 1000)]
    record_count: u64,

    /// Isolation level: rc | si
    #[arg(long, default_value = "rc")]
    isolation: String,

    /// Export format: text | csv | json
    #[arg(long, default_value = "text")]
    export: String,

    /// If set, run a Fast-Path ON vs OFF comparison.
    #[arg(long, default_value_t = false)]
    compare: bool,

    /// If set, run scatter/gather scale-out benchmark (1/2/4/8 shards).
    #[arg(long, default_value_t = false)]
    scaleout: bool,

    /// If set, run failover benchmark (before/after latency + data integrity).
    #[arg(long, default_value_t = false)]
    failover: bool,

    /// If set, run TPC-B (pgbench) style benchmark.
    #[arg(long, default_value_t = false)]
    tpcb: bool,

    /// If set, run LSM disk-backed KV benchmark.
    #[arg(long, default_value_t = false)]
    lsm: bool,

    /// Concurrency level for multi-threaded benchmarks.
    #[arg(long, default_value_t = 1)]
    threads: u32,

    /// Deterministic random seed for reproducibility.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Run bulk-insert benchmark from a SQL file (requires a running FalconDB server).
    #[arg(long, default_value_t = false)]
    bulk: bool,

    /// SQL file to execute in bulk mode.
    #[arg(long, default_value = "bulk_insert.sql")]
    bulk_file: String,

    /// Server host for bulk mode.
    #[arg(long, default_value = "localhost")]
    bulk_host: String,

    /// Server port for bulk mode.
    #[arg(long, default_value_t = 5433u16)]
    bulk_port: u16,

    /// Database user for bulk mode.
    #[arg(long, default_value = "falcon")]
    bulk_user: String,

    /// Database password for bulk mode (or set PGPASSWORD env var).
    #[arg(long, default_value = "")]
    bulk_password: String,

    /// Database name for bulk mode.
    #[arg(long, default_value = "falcon")]
    bulk_dbname: String,

    /// Number of statements per progress report in bulk mode.
    #[arg(long, default_value_t = 10usize)]
    bulk_progress_interval: usize,

    /// SSL mode for bulk mode connection: disable | prefer | require.
    /// PG18 clients default to 'prefer'; set 'disable' for local FalconDB.
    #[arg(long, default_value = "disable")]
    bulk_sslmode: String,

    /// Connection timeout in seconds for bulk mode (0 = no timeout).
    #[arg(long, default_value_t = 30u64)]
    bulk_connect_timeout: u64,

    /// Run long-run stress test mode.
    #[arg(long, default_value_t = false)]
    long_run: bool,

    /// Duration for long-run mode (e.g. "60s", "10m", "1h", "24h").
    #[arg(long, default_value = "60s")]
    duration: String,

    /// Save current results as the performance baseline.
    #[arg(long, default_value_t = false)]
    baseline_save: bool,

    /// Check current results against saved baseline (fail on regression).
    #[arg(long, default_value_t = false)]
    baseline_check: bool,

    /// Path to baseline file.
    #[arg(long, default_value = "perf_baseline.json")]
    baseline_path: String,

    /// TPS regression threshold percentage (fail if TPS drops more than this).
    #[arg(long, default_value_t = 10)]
    tps_threshold_pct: u32,

    /// P99 regression threshold percentage (fail if P99 rises more than this).
    #[arg(long, default_value_t = 20)]
    p99_threshold_pct: u32,

    /// Sampling interval in seconds for long-run metrics collection.
    #[arg(long, default_value_t = 5)]
    sample_interval_secs: u64,
}

fn bench_schema() -> TableSchema {
    TableSchema {
        id: TableId(1),
        name: "ycsb".into(),
        columns: vec![
            ColumnDef {
                id: ColumnId(0),
                name: "ycsb_key".into(),
                data_type: DataType::Int64,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            },
            ColumnDef {
                id: ColumnId(1),
                name: "field0".into(),
                data_type: DataType::Text,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
            },
        ],
        primary_key_columns: vec![0],
        next_serial_values: std::collections::HashMap::new(),
        check_constraints: vec![],
        unique_constraints: vec![],
        foreign_keys: vec![],
        ..Default::default()
    }
}

fn isolation_from_str(s: &str) -> IsolationLevel {
    match s {
        "si" => IsolationLevel::SnapshotIsolation,
        _ => IsolationLevel::ReadCommitted,
    }
}

/// Simple deterministic pseudo-random (xorshift64).
struct Rng(u64);

impl Rng {
    const fn new(seed: u64) -> Self {
        Self(if seed == 0 { 1 } else { seed })
    }
    const fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    const fn next_pct(&mut self) -> u8 {
        (self.next_u64() % 100) as u8
    }
}

struct BenchResult {
    label: String,
    ops: u64,
    elapsed_ms: u64,
    tps: f64,
    stats: falcon_txn::TxnStatsSnapshot,
    local_txn_count: u64,
    global_txn_count: u64,
    seed: u64,
}

fn run_workload(args: &Args, force_all_global: bool, label: &str) -> BenchResult {
    let storage = Arc::new(StorageEngine::new_in_memory());
    storage.create_table(bench_schema()).unwrap();

    let mgr = Arc::new(TxnManager::new(storage.clone()));
    let isolation = isolation_from_str(&args.isolation);
    let table_id = TableId(1);

    // Phase 1: Load initial data
    for i in 0..args.record_count {
        let txn = mgr.begin(isolation);
        let row = OwnedRow::new(vec![
            Datum::Int64(i as i64),
            Datum::Text(format!("value_{i}")),
        ]);
        storage.insert(table_id, row, txn.txn_id).unwrap();
        mgr.commit(txn.txn_id).unwrap();
    }

    // Reset latency after load phase
    mgr.reset_latency();

    let mut rng = Rng::new(args.seed);
    let mut local_txn_count = 0u64;
    let mut global_txn_count = 0u64;

    // Phase 2: Run workload
    let start = Instant::now();

    for _ in 0..args.ops {
        let is_read = rng.next_pct() < args.read_pct;
        let is_local = !force_all_global && rng.next_pct() < args.local_pct;

        let classification = if is_local {
            local_txn_count += 1;
            let shard = ShardId(rng.next_u64() % args.shards);
            TxnClassification::local(shard)
        } else {
            global_txn_count += 1;
            let s1 = ShardId(0);
            let s2 = ShardId(1u64.min(args.shards - 1));
            TxnClassification::global(vec![s1, s2], SlowPathMode::Xa2Pc)
        };

        let txn = mgr.begin_with_classification(isolation, classification);
        let key = (rng.next_u64() % args.record_count) as i64;

        if is_read {
            // Read operation
            let read_ts = txn.read_ts(mgr.current_ts());
            let _ = storage.scan(table_id, txn.txn_id, read_ts);
            mgr.commit(txn.txn_id).unwrap();
        } else {
            // Write (update) operation
            let read_ts = txn.read_ts(mgr.current_ts());
            let rows = storage.scan(table_id, txn.txn_id, read_ts).unwrap();
            // Find the target key and update it
            for (pk, row) in &rows {
                if let Some(Datum::Int64(k)) = row.values.first() {
                    if *k == key {
                        let new_row = OwnedRow::new(vec![
                            Datum::Int64(key),
                            Datum::Text(format!("updated_{}", rng.next_u64())),
                        ]);
                        let _ = storage.update(table_id, pk, new_row, txn.txn_id);
                        break;
                    }
                }
            }
            if mgr.commit(txn.txn_id).is_err() {
                // OCC or constraint failure — expected under contention
            }
        }
    }

    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis() as u64;
    let tps = if elapsed_ms > 0 {
        args.ops as f64 / (elapsed_ms as f64 / 1000.0)
    } else {
        0.0
    };

    let stats = mgr.stats_snapshot();

    BenchResult {
        label: label.to_owned(),
        ops: args.ops,
        elapsed_ms,
        tps,
        stats,
        local_txn_count,
        global_txn_count,
        seed: args.seed,
    }
}

fn print_result_text(r: &BenchResult) {
    let total_txn = r.local_txn_count + r.global_txn_count;
    let local_pct = if total_txn > 0 {
        r.local_txn_count as f64 / total_txn as f64 * 100.0
    } else {
        0.0
    };
    let global_pct = if total_txn > 0 {
        r.global_txn_count as f64 / total_txn as f64 * 100.0
    } else {
        0.0
    };

    println!("═══════════════════════════════════════════════");
    println!("  {} ", r.label);
    println!("═══════════════════════════════════════════════");
    println!("  Seed:              {}", r.seed);
    println!("  Operations:        {}", r.ops);
    println!("  Elapsed:           {} ms", r.elapsed_ms);
    println!("  TPS:               {:.1}", r.tps);
    println!("  ─── Txn Mix ───");
    println!(
        "  Local (single-shard):  {} ({:.1}%)",
        r.local_txn_count, local_pct
    );
    println!(
        "  Global (cross-shard):  {} ({:.1}%)",
        r.global_txn_count, global_pct
    );
    println!("  ─── Commits ───");
    println!("  Total committed:   {}", r.stats.total_committed);
    println!("  Fast-path commits: {}", r.stats.fast_path_commits);
    println!("  Slow-path commits: {}", r.stats.slow_path_commits);
    println!("  Total aborted:     {}", r.stats.total_aborted);
    println!("  OCC conflicts:     {}", r.stats.occ_conflicts);
    println!("  Constraint viols:  {}", r.stats.constraint_violations);
    println!("  Degraded→Global:   {}", r.stats.degraded_to_global);
    println!("  ─── Latency (commit, µs) ───");
    println!(
        "  Fast-path  p50={:>6}  p95={:>6}  p99={:>6}  (n={})",
        r.stats.latency.fast_path.p50_us,
        r.stats.latency.fast_path.p95_us,
        r.stats.latency.fast_path.p99_us,
        r.stats.latency.fast_path.count,
    );
    println!(
        "  Slow-path  p50={:>6}  p95={:>6}  p99={:>6}  (n={})",
        r.stats.latency.slow_path.p50_us,
        r.stats.latency.slow_path.p95_us,
        r.stats.latency.slow_path.p99_us,
        r.stats.latency.slow_path.count,
    );
    println!(
        "  All        p50={:>6}  p95={:>6}  p99={:>6}  (n={})",
        r.stats.latency.all.p50_us,
        r.stats.latency.all.p95_us,
        r.stats.latency.all.p99_us,
        r.stats.latency.all.count,
    );
    println!();
}

fn print_result_csv(r: &BenchResult) {
    println!("label,ops,elapsed_ms,tps,committed,fast,slow,aborted,occ,constraint,fast_p50,fast_p95,fast_p99,slow_p50,slow_p95,slow_p99,all_p50,all_p95,all_p99");
    println!(
        "{},{},{},{:.1},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        r.label,
        r.ops,
        r.elapsed_ms,
        r.tps,
        r.stats.total_committed,
        r.stats.fast_path_commits,
        r.stats.slow_path_commits,
        r.stats.total_aborted,
        r.stats.occ_conflicts,
        r.stats.constraint_violations,
        r.stats.latency.fast_path.p50_us,
        r.stats.latency.fast_path.p95_us,
        r.stats.latency.fast_path.p99_us,
        r.stats.latency.slow_path.p50_us,
        r.stats.latency.slow_path.p95_us,
        r.stats.latency.slow_path.p99_us,
        r.stats.latency.all.p50_us,
        r.stats.latency.all.p95_us,
        r.stats.latency.all.p99_us,
    );
}

fn print_result_json(r: &BenchResult) {
    let total_txn = r.local_txn_count + r.global_txn_count;
    let obj = serde_json::json!({
        "label": r.label,
        "seed": r.seed,
        "ops": r.ops,
        "elapsed_ms": r.elapsed_ms,
        "tps": r.tps,
        "txn_mix": {
            "local_count": r.local_txn_count,
            "global_count": r.global_txn_count,
            "local_pct": if total_txn > 0 { r.local_txn_count as f64 / total_txn as f64 * 100.0 } else { 0.0 },
            "global_pct": if total_txn > 0 { r.global_txn_count as f64 / total_txn as f64 * 100.0 } else { 0.0 },
        },
        "committed": r.stats.total_committed,
        "fast_path_commits": r.stats.fast_path_commits,
        "slow_path_commits": r.stats.slow_path_commits,
        "aborted": r.stats.total_aborted,
        "occ_conflicts": r.stats.occ_conflicts,
        "constraint_violations": r.stats.constraint_violations,
        "latency": {
            "fast_path": {
                "count": r.stats.latency.fast_path.count,
                "p50_us": r.stats.latency.fast_path.p50_us,
                "p95_us": r.stats.latency.fast_path.p95_us,
                "p99_us": r.stats.latency.fast_path.p99_us,
            },
            "slow_path": {
                "count": r.stats.latency.slow_path.count,
                "p50_us": r.stats.latency.slow_path.p50_us,
                "p95_us": r.stats.latency.slow_path.p95_us,
                "p99_us": r.stats.latency.slow_path.p99_us,
            },
            "all": {
                "count": r.stats.latency.all.count,
                "p50_us": r.stats.latency.all.p50_us,
                "p95_us": r.stats.latency.all.p95_us,
                "p99_us": r.stats.latency.all.p99_us,
            },
        }
    });
    println!("{}", serde_json::to_string_pretty(&obj).unwrap());
}

fn print_result(r: &BenchResult, format: &str) {
    match format {
        "csv" => print_result_csv(r),
        "json" => print_result_json(r),
        _ => print_result_text(r),
    }
}

fn run_scaleout(args: &Args) {
    let shard_counts = [1u64, 2, 4, 8];
    let ops_per_round = args.ops;
    let record_count = args.record_count;
    let isolation = isolation_from_str(&args.isolation);

    println!("Running scatter/gather scale-out benchmark...");
    println!(
        "  ops/round: {ops_per_round}  records: {record_count}\n"
    );

    if args.export == "csv" {
        println!("shards,ops,elapsed_ms,tps,scatter_gather_total_us,max_subplan_us,gather_merge_us,rows_total");
    }

    let mut results: Vec<serde_json::Value> = Vec::new();
    let mut tps_by_shards: Vec<(u64, f64)> = Vec::new();

    for &n_shards in &shard_counts {
        let engine = Arc::new(ShardedEngine::new(n_shards));
        engine.create_table_all(&bench_schema()).unwrap();

        // Load data round-robin across shards
        let mut rng = Rng::new(args.seed);
        for i in 0..record_count {
            let shard_idx = i % n_shards;
            let shard = engine.shard(ShardId(shard_idx)).unwrap();
            let txn = shard.txn_mgr.begin(isolation);
            let row = OwnedRow::new(vec![
                Datum::Int64(i as i64),
                Datum::Text(format!("value_{i}")),
            ]);
            shard.storage.insert(TableId(1), row, txn.txn_id).unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }

        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(30));
        let all_shards = engine.shard_ids();

        // Benchmark: scatter/gather scan + filter + COUNT agg
        let start = Instant::now();
        let mut total_rows = 0usize;
        let mut total_sg_us = 0u64;
        let mut max_sub_us = 0u64;
        let mut gather_us = 0u64;

        for op in 0..ops_per_round {
            let threshold = (rng.next_u64() % 100) as i64;

            // Mix of operations: scan/filter (50%), COUNT agg (30%), SUM agg (20%)
            let op_type = op % 10;
            if op_type < 5 {
                // Scan + filter
                let subplan = SubPlan::new("scan+filter", move |storage, txn_mgr| {
                    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
                    let read_ts = txn.read_ts(txn_mgr.current_ts());
                    let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
                    txn_mgr.commit(txn.txn_id)?;
                    let filtered: Vec<OwnedRow> = rows
                        .into_iter()
                        .filter(|(_, r)| {
                            matches!(r.values.first(), Some(Datum::Int64(k)) if *k % 100 >= threshold)
                        })
                        .map(|(_, r)| r)
                        .collect();
                    Ok((
                        vec![
                            ("ycsb_key".into(), DataType::Int64),
                            ("field0".into(), DataType::Text),
                        ],
                        filtered,
                    ))
                });
                let ((_, rows), metrics) = exec
                    .scatter_gather(
                        &subplan,
                        &all_shards,
                        &GatherStrategy::Union {
                            distinct: false,
                            limit: None,
                            offset: None,
                        },
                    )
                    .unwrap();
                total_rows += rows.len();
                total_sg_us += metrics.total_latency_us;
                if metrics.max_subplan_latency_us > max_sub_us {
                    max_sub_us = metrics.max_subplan_latency_us;
                }
                gather_us += metrics.gather_merge_latency_us;
            } else if op_type < 8 {
                // COUNT(*) two-phase agg
                let subplan = SubPlan::new("partial_count", |storage, txn_mgr| {
                    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
                    let read_ts = txn.read_ts(txn_mgr.current_ts());
                    let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
                    txn_mgr.commit(txn.txn_id)?;
                    Ok((
                        vec![("count".into(), DataType::Int64)],
                        vec![OwnedRow::new(vec![Datum::Int64(rows.len() as i64)])],
                    ))
                });
                let ((_, rows), metrics) = exec
                    .scatter_gather(
                        &subplan,
                        &all_shards,
                        &GatherStrategy::TwoPhaseAgg {
                            group_by_indices: vec![],
                            agg_merges: vec![AggMerge::Count(0)],
                            avg_fixups: vec![],
                            visible_columns: None,
                            having: None,
                            order_by: vec![],
                            limit: None,
                            offset: None,
                        },
                    )
                    .unwrap();
                total_rows += rows.len();
                total_sg_us += metrics.total_latency_us;
                gather_us += metrics.gather_merge_latency_us;
            } else {
                // SUM(ycsb_key) two-phase agg
                let subplan = SubPlan::new("partial_sum", |storage, txn_mgr| {
                    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
                    let read_ts = txn.read_ts(txn_mgr.current_ts());
                    let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
                    txn_mgr.commit(txn.txn_id)?;
                    let mut sum: i64 = 0;
                    for (_, r) in &rows {
                        if let Some(Datum::Int64(k)) = r.values.first() {
                            sum += k;
                        }
                    }
                    Ok((
                        vec![("sum_key".into(), DataType::Int64)],
                        vec![OwnedRow::new(vec![Datum::Int64(sum)])],
                    ))
                });
                let ((_, rows), metrics) = exec
                    .scatter_gather(
                        &subplan,
                        &all_shards,
                        &GatherStrategy::TwoPhaseAgg {
                            group_by_indices: vec![],
                            agg_merges: vec![AggMerge::Sum(0)],
                            avg_fixups: vec![],
                            visible_columns: None,
                            having: None,
                            order_by: vec![],
                            limit: None,
                            offset: None,
                        },
                    )
                    .unwrap();
                total_rows += rows.len();
                total_sg_us += metrics.total_latency_us;
                gather_us += metrics.gather_merge_latency_us;
            }
        }

        let elapsed = start.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;
        let tps = if elapsed_ms > 0 {
            ops_per_round as f64 / (elapsed_ms as f64 / 1000.0)
        } else {
            0.0
        };
        tps_by_shards.push((n_shards, tps));

        match args.export.as_str() {
            "csv" => {
                println!(
                    "{n_shards},{ops_per_round},{elapsed_ms},{tps:.1},{total_sg_us},{max_sub_us},{gather_us},{total_rows}",
                );
            }
            "json" => {
                results.push(serde_json::json!({
                    "shards": n_shards,
                    "ops": ops_per_round,
                    "elapsed_ms": elapsed_ms,
                    "tps": tps,
                    "scatter_gather_total_us": total_sg_us,
                    "max_subplan_us": max_sub_us,
                    "gather_merge_us": gather_us,
                    "rows_total": total_rows,
                }));
            }
            _ => {
                println!(
                    "  {n_shards:>2} shards │ {ops_per_round:>8} ops │ {elapsed_ms:>6} ms │ {tps:>10.1} TPS │ sg_total={total_sg_us:>8}µs max_sub={max_sub_us:>6}µs gather={gather_us:>6}µs",
                );
            }
        }
    }

    if args.export == "json" {
        println!("{}", serde_json::to_string_pretty(&results).unwrap());
    } else if args.export != "csv" {
        println!();
    }

    // P3-1: TPS monotonicity assertion — TPS must not decrease as shards increase
    if tps_by_shards.len() >= 2 {
        let mut monotonic = true;
        for i in 1..tps_by_shards.len() {
            let (prev_shards, prev_tps) = tps_by_shards[i - 1];
            let (cur_shards, cur_tps) = tps_by_shards[i];
            if cur_tps < prev_tps * 0.95 {
                // Allow 5% tolerance for noise
                eprintln!(
                    "SCALE-OUT WARNING: TPS dropped from {prev_tps:.1} ({prev_shards} shards) to {cur_tps:.1} ({cur_shards} shards)"
                );
                monotonic = false;
            }
        }
        if monotonic {
            println!("SCALE-OUT OK: TPS monotonically non-decreasing across shard counts");
        } else {
            eprintln!("SCALE-OUT INVARIANT: TPS decreased with more shards (sub-linear OK, regression NOT OK)");
        }
    }
}

fn run_failover_bench(args: &Args) {
    use falcon_cluster::replication::ShardReplicaGroup;
    use falcon_common::types::{Timestamp, TxnId, TxnType};
    use falcon_storage::wal::WalRecord;

    println!("Running failover benchmark...\n");

    let schema = bench_schema();
    let mut group = ShardReplicaGroup::new(ShardId(0), &[schema]).unwrap();

    let table_id = TableId(1);
    let record_count = args.record_count;

    // Load initial data on primary + replicate
    for i in 0..record_count {
        let row = OwnedRow::new(vec![
            Datum::Int64(i as i64),
            Datum::Text(format!("value_{i}")),
        ]);
        let txn_id = TxnId(i + 1);
        let ts = Timestamp((i + 1) * 10);
        group
            .primary
            .storage
            .insert(table_id, row.clone(), txn_id)
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(txn_id, ts, TxnType::Local)
            .unwrap();
        group.ship_wal_record(WalRecord::Insert {
            txn_id,
            table_id,
            row,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal {
            txn_id,
            commit_ts: ts,
        });
    }
    group.catch_up_replica(0).unwrap();

    // Phase 1: Measure throughput BEFORE failover
    let ops_half = args.ops / 2;
    let mut rng = Rng::new(42);
    let mut latencies_before: Vec<u64> = Vec::with_capacity(ops_half as usize);

    let start_before = Instant::now();
    for _ in 0..ops_half {
        let _key = (rng.next_u64() % record_count) as i64;
        let txn_id = TxnId(rng.next_u64());
        let ts = Timestamp(rng.next_u64());

        let op_start = Instant::now();
        let _ = group.primary.storage.scan(table_id, txn_id, ts);
        latencies_before.push(op_start.elapsed().as_nanos() as u64);
    }
    let elapsed_before_ms = start_before.elapsed().as_millis() as u64;
    let tps_before = if elapsed_before_ms > 0 {
        ops_half as f64 / (elapsed_before_ms as f64 / 1000.0)
    } else {
        0.0
    };

    // Phase 2: Failover
    let failover_start = Instant::now();
    group.promote(0).unwrap();
    let failover_ms = failover_start.elapsed().as_millis() as u64;
    let metrics = group.metrics.snapshot();

    // Phase 3: Measure throughput AFTER failover
    let mut latencies_after: Vec<u64> = Vec::with_capacity(ops_half as usize);
    let start_after = Instant::now();
    for _ in 0..ops_half {
        let _key = (rng.next_u64() % record_count) as i64;
        let txn_id = TxnId(rng.next_u64());
        let ts = Timestamp(rng.next_u64());

        let op_start = Instant::now();
        let _ = group.primary.storage.scan(table_id, txn_id, ts);
        latencies_after.push(op_start.elapsed().as_nanos() as u64);
    }
    let elapsed_after_ms = start_after.elapsed().as_millis() as u64;
    let tps_after = if elapsed_after_ms > 0 {
        ops_half as f64 / (elapsed_after_ms as f64 / 1000.0)
    } else {
        0.0
    };

    // Compute percentiles
    fn percentiles(v: &mut [u64]) -> (u64, u64, u64) {
        v.sort();
        let len = v.len();
        if len == 0 {
            return (0, 0, 0);
        }
        let p50 = v[len * 50 / 100];
        let p95 = v[len * 95 / 100];
        let p99 = v[len.saturating_sub(1).min(len * 99 / 100)];
        (p50, p95, p99)
    }

    let (bp50, bp95, bp99) = percentiles(&mut latencies_before);
    let (ap50, ap95, ap99) = percentiles(&mut latencies_after);

    // Verify data integrity
    let rows = group
        .primary
        .storage
        .scan(table_id, TxnId(999999), Timestamp(u64::MAX - 2))
        .unwrap();
    let data_intact = rows.len() == record_count as usize;

    match args.export.as_str() {
        "csv" => {
            println!("phase,ops,elapsed_ms,tps,p50_ns,p95_ns,p99_ns,failover_ms,data_intact");
            println!(
                "before,{ops_half},{elapsed_before_ms},{tps_before:.1},{bp50},{bp95},{bp99},0,true"
            );
            println!(
                "failover,0,{failover_ms},0,0,0,0,{failover_ms},{data_intact}"
            );
            println!(
                "after,{ops_half},{elapsed_after_ms},{tps_after:.1},{ap50},{ap95},{ap99},0,{data_intact}"
            );
        }
        "json" => {
            let obj = serde_json::json!({
                "before": { "ops": ops_half, "elapsed_ms": elapsed_before_ms, "tps": tps_before, "p50_ns": bp50, "p95_ns": bp95, "p99_ns": bp99 },
                "failover": { "duration_ms": failover_ms, "promote_count": metrics.promote_count },
                "after": { "ops": ops_half, "elapsed_ms": elapsed_after_ms, "tps": tps_after, "p50_ns": ap50, "p95_ns": ap95, "p99_ns": ap99 },
                "data_intact": data_intact,
                "rows_after_failover": rows.len(),
            });
            println!("{}", serde_json::to_string_pretty(&obj).unwrap());
        }
        _ => {
            println!("═══════════════════════════════════════════════");
            println!("  FAILOVER BENCHMARK");
            println!("═══════════════════════════════════════════════");
            println!("  ─── Before Failover ───");
            println!("  Ops:     {ops_half}  TPS: {tps_before:.1}");
            println!("  Latency: p50={bp50}ns  p95={bp95}ns  p99={bp99}ns");
            println!("  ─── Failover ───");
            println!("  Duration:      {failover_ms} ms");
            println!("  Promote count: {}", metrics.promote_count);
            println!("  ─── After Failover ───");
            println!("  Ops:     {ops_half}  TPS: {tps_after:.1}");
            println!("  Latency: p50={ap50}ns  p95={ap95}ns  p99={ap99}ns");
            println!("  ─── Data Integrity ───");
            println!(
                "  Rows after failover: {} (expected {})",
                rows.len(),
                record_count
            );
            println!("  Data intact:         {data_intact}");
            println!();
        }
    }
}

// ── TPC-B (pgbench) workload ────────────────────────────────────────────

fn tpcb_schema() -> Vec<TableSchema> {
    vec![
        TableSchema {
            id: TableId(10),
            name: "pgbench_accounts".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "aid".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "bid".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "abalance".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        },
        TableSchema {
            id: TableId(11),
            name: "pgbench_tellers".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "tid".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "bid".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "tbalance".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        },
        TableSchema {
            id: TableId(12),
            name: "pgbench_branches".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "bid".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "bbalance".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        },
    ]
}

fn run_tpcb(args: &Args) {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let schemas = tpcb_schema();
    for s in &schemas {
        storage.create_table(s.clone()).unwrap();
    }
    let mgr = Arc::new(TxnManager::new(storage.clone()));
    let isolation = isolation_from_str(&args.isolation);

    let scale = (args.record_count / 100).max(1);
    let n_branches = scale;
    let n_tellers = scale * 10;
    let n_accounts = scale * 100;

    // Load: branches
    for i in 0..n_branches {
        let txn = mgr.begin(isolation);
        storage
            .insert(
                TableId(12),
                OwnedRow::new(vec![Datum::Int64(i as i64), Datum::Int64(0)]),
                txn.txn_id,
            )
            .unwrap();
        mgr.commit(txn.txn_id).unwrap();
    }
    // Load: tellers
    for i in 0..n_tellers {
        let txn = mgr.begin(isolation);
        storage
            .insert(
                TableId(11),
                OwnedRow::new(vec![
                    Datum::Int64(i as i64),
                    Datum::Int64((i % n_branches) as i64),
                    Datum::Int64(0),
                ]),
                txn.txn_id,
            )
            .unwrap();
        mgr.commit(txn.txn_id).unwrap();
    }
    // Load: accounts
    for i in 0..n_accounts {
        let txn = mgr.begin(isolation);
        storage
            .insert(
                TableId(10),
                OwnedRow::new(vec![
                    Datum::Int64(i as i64),
                    Datum::Int64((i % n_branches) as i64),
                    Datum::Int64(0),
                ]),
                txn.txn_id,
            )
            .unwrap();
        mgr.commit(txn.txn_id).unwrap();
    }

    mgr.reset_latency();
    let mut rng = Rng::new(42);
    let mut latencies_us: Vec<u64> = Vec::with_capacity(args.ops as usize);

    let start = Instant::now();
    let mut committed = 0u64;
    let mut aborted = 0u64;

    for _ in 0..args.ops {
        let aid = (rng.next_u64() % n_accounts) as i64;
        let _tid = (rng.next_u64() % n_tellers) as i64;
        let bid = (rng.next_u64() % n_branches) as i64;
        let delta = ((rng.next_u64() % 10001) as i64) - 5000;

        let op_start = Instant::now();
        let txn = mgr.begin(isolation);
        let read_ts = txn.read_ts(mgr.current_ts());

        // UPDATE accounts SET abalance = abalance + delta WHERE aid = ?
        let rows = storage.scan(TableId(10), txn.txn_id, read_ts).unwrap();
        for (pk, row) in &rows {
            if let Some(Datum::Int64(k)) = row.values.first() {
                if *k == aid {
                    let old_bal = match row.values.get(2) {
                        Some(Datum::Int64(b)) => *b,
                        _ => 0,
                    };
                    let new_row = OwnedRow::new(vec![
                        Datum::Int64(aid),
                        Datum::Int64(bid),
                        Datum::Int64(old_bal + delta),
                    ]);
                    let _ = storage.update(TableId(10), pk, new_row, txn.txn_id);
                    break;
                }
            }
        }

        match mgr.commit(txn.txn_id) {
            Ok(_) => committed += 1,
            Err(_) => aborted += 1,
        }
        latencies_us.push(op_start.elapsed().as_micros() as u64);
    }

    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis() as u64;
    let tps = if elapsed_ms > 0 {
        args.ops as f64 / (elapsed_ms as f64 / 1000.0)
    } else {
        0.0
    };

    latencies_us.sort();
    let len = latencies_us.len();
    let p50 = if len > 0 {
        latencies_us[len * 50 / 100]
    } else {
        0
    };
    let p95 = if len > 0 {
        latencies_us[len * 95 / 100]
    } else {
        0
    };
    let p99 = if len > 0 {
        latencies_us[len.saturating_sub(1).min(len * 99 / 100)]
    } else {
        0
    };
    let max = latencies_us.last().copied().unwrap_or(0);

    let stats = mgr.stats_snapshot();

    match args.export.as_str() {
        "json" => {
            let obj = serde_json::json!({
                "workload": "tpcb",
                "scale": scale,
                "ops": args.ops,
                "elapsed_ms": elapsed_ms,
                "tps": tps,
                "committed": committed,
                "aborted": aborted,
                "latency_us": { "p50": p50, "p95": p95, "p99": p99, "max": max },
                "backpressure_rejections": stats.admission_rejections,
            });
            println!("{}", serde_json::to_string_pretty(&obj).unwrap());
        }
        "csv" => {
            println!("workload,scale,ops,elapsed_ms,tps,committed,aborted,p50_us,p95_us,p99_us,max_us,backpressure");
            println!(
                "tpcb,{},{},{},{:.1},{},{},{},{},{},{},{}",
                scale,
                args.ops,
                elapsed_ms,
                tps,
                committed,
                aborted,
                p50,
                p95,
                p99,
                max,
                stats.admission_rejections
            );
        }
        _ => {
            println!("═══════════════════════════════════════════════");
            println!("  TPC-B (pgbench) Benchmark");
            println!("═══════════════════════════════════════════════");
            println!("  Scale factor:      {scale}");
            println!("  Accounts:          {n_accounts}");
            println!("  Operations:        {}", args.ops);
            println!("  Elapsed:           {elapsed_ms} ms");
            println!("  TPS:               {tps:.1}");
            println!("  Committed:         {committed}");
            println!("  Aborted:           {aborted}");
            println!("  ─── Latency (µs) ───");
            println!("  P50:    {p50:>8}");
            println!("  P95:    {p95:>8}");
            println!("  P99:    {p99:>8}");
            println!("  Max:    {max:>8}");
            println!("  ─── Backpressure ───");
            println!("  Rejections:        {}", stats.admission_rejections);
            println!();
        }
    }
}

// ── LSM disk-backed KV benchmark (requires `lsm` feature) ──────────────

#[cfg(feature = "lsm")]
fn run_lsm_bench(args: &Args) {
    use falcon_storage::lsm::compaction::CompactionConfig;
    use falcon_storage::lsm::{LsmConfig, LsmEngine};

    let dir = std::env::temp_dir().join(format!("falcon_lsm_bench_{}", std::process::id()));
    let config = LsmConfig {
        memtable_budget_bytes: 16 * 1024 * 1024,
        block_cache_bytes: 64 * 1024 * 1024,
        compaction: CompactionConfig {
            l0_compaction_trigger: 4,
            l0_stall_trigger: 20,
            ..Default::default()
        },
        sync_writes: false,
    };
    let engine = LsmEngine::open(&dir, config).unwrap();

    let n = args.record_count;
    let ops = args.ops;
    let read_pct = args.read_pct;
    let mut rng = Rng::new(42);

    // Load phase
    for i in 0..n {
        let key = format!("key_{:012}", i);
        let val = format!("val_{:012}_{}", i, "x".repeat(100));
        engine.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Benchmark phase
    let mut latencies_us: Vec<u64> = Vec::with_capacity(ops as usize);
    let mut reads = 0u64;
    let mut writes = 0u64;
    let mut stalls = 0u64;

    let start = Instant::now();
    for _ in 0..ops {
        let is_read = rng.next_pct() < read_pct;
        let key_id = rng.next_u64() % n;
        let key = format!("key_{:012}", key_id);

        let op_start = Instant::now();
        if is_read {
            let _ = engine.get(key.as_bytes());
            reads += 1;
        } else {
            let val = format!("upd_{:012}_{}", rng.next_u64(), "y".repeat(100));
            match engine.put(key.as_bytes(), val.as_bytes()) {
                Ok(_) => writes += 1,
                Err(_) => stalls += 1,
            }
        }
        latencies_us.push(op_start.elapsed().as_micros() as u64);
    }
    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis() as u64;
    let tps = if elapsed_ms > 0 {
        ops as f64 / (elapsed_ms as f64 / 1000.0)
    } else {
        0.0
    };

    latencies_us.sort();
    let len = latencies_us.len();
    let p50 = if len > 0 {
        latencies_us[len * 50 / 100]
    } else {
        0
    };
    let p95 = if len > 0 {
        latencies_us[len * 95 / 100]
    } else {
        0
    };
    let p99 = if len > 0 {
        latencies_us[len.saturating_sub(1).min(len * 99 / 100)]
    } else {
        0
    };
    let max = latencies_us.last().copied().unwrap_or(0);

    let stats = engine.stats();

    // Cleanup
    let _ = std::fs::remove_dir_all(&dir);

    match args.export.as_str() {
        "json" => {
            let obj = serde_json::json!({
                "workload": "lsm_kv",
                "record_count": n,
                "ops": ops,
                "elapsed_ms": elapsed_ms,
                "tps": tps,
                "reads": reads,
                "writes": writes,
                "stalls": stalls,
                "latency_us": { "p50": p50, "p95": p95, "p99": p99, "max": max },
                "lsm": {
                    "flushes": stats.flushes_completed,
                    "l0_files": stats.l0_file_count,
                    "total_sst_files": stats.total_sst_files,
                    "total_sst_bytes": stats.total_sst_bytes,
                    "compaction_runs": stats.compaction.runs_completed,
                    "block_cache_hit_rate": stats.block_cache.hit_rate,
                },
            });
            println!("{}", serde_json::to_string_pretty(&obj).unwrap());
        }
        "csv" => {
            println!("workload,records,ops,elapsed_ms,tps,reads,writes,stalls,p50_us,p95_us,p99_us,max_us,flushes,compactions,cache_hit_rate");
            println!(
                "lsm_kv,{},{},{},{:.1},{},{},{},{},{},{},{},{},{},{:.4}",
                n,
                ops,
                elapsed_ms,
                tps,
                reads,
                writes,
                stalls,
                p50,
                p95,
                p99,
                max,
                stats.flushes_completed,
                stats.compaction.runs_completed,
                stats.block_cache.hit_rate
            );
        }
        _ => {
            println!("═══════════════════════════════════════════════");
            println!("  LSM Disk-Backed KV Benchmark");
            println!("═══════════════════════════════════════════════");
            println!("  Records:           {}", n);
            println!("  Operations:        {}", ops);
            println!("  Elapsed:           {} ms", elapsed_ms);
            println!("  TPS:               {:.1}", tps);
            println!("  Reads:             {}", reads);
            println!("  Writes:            {}", writes);
            println!("  Write stalls:      {}", stalls);
            println!("  ─── Latency (µs) ───");
            println!("  P50:    {:>8}", p50);
            println!("  P95:    {:>8}", p95);
            println!("  P99:    {:>8}", p99);
            println!("  Max:    {:>8}", max);
            println!("  ─── LSM Stats ───");
            println!("  Flushes:           {}", stats.flushes_completed);
            println!("  L0 files:          {}", stats.l0_file_count);
            println!("  Total SST files:   {}", stats.total_sst_files);
            println!("  Total SST bytes:   {}", stats.total_sst_bytes);
            println!("  Compaction runs:   {}", stats.compaction.runs_completed);
            println!(
                "  Block cache hit:   {:.2}%",
                stats.block_cache.hit_rate * 100.0
            );
            println!();
        }
    }
}

// ── Duration parser ──────────────────────────────────────────────────────

fn parse_duration(s: &str) -> Duration {
    let s = s.trim();
    if let Some(rest) = s.strip_suffix('h') {
        Duration::from_secs(rest.parse::<u64>().unwrap_or(1) * 3600)
    } else if let Some(rest) = s.strip_suffix('m') {
        Duration::from_secs(rest.parse::<u64>().unwrap_or(1) * 60)
    } else if let Some(rest) = s.strip_suffix('s') {
        Duration::from_secs(rest.parse::<u64>().unwrap_or(60))
    } else {
        Duration::from_secs(s.parse::<u64>().unwrap_or(60))
    }
}

// ── P0-1 / P6-1: Performance Baseline Save & Check ─────────────────────

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct PerfBaseline {
    version: String,
    timestamp: String,
    seed: u64,
    ops: u64,
    record_count: u64,
    shards: u64,
    read_pct: u8,
    local_pct: u8,
    tps: f64,
    fast_path_p99_us: u64,
    slow_path_p99_us: u64,
    all_p99_us: u64,
    fast_path_p50_us: u64,
    slow_path_p50_us: u64,
    all_p50_us: u64,
}

fn save_baseline(r: &BenchResult, args: &Args) {
    let baseline = PerfBaseline {
        version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: format!(
            "{:?}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        ),
        seed: args.seed,
        ops: args.ops,
        record_count: args.record_count,
        shards: args.shards,
        read_pct: args.read_pct,
        local_pct: args.local_pct,
        tps: r.tps,
        fast_path_p99_us: r.stats.latency.fast_path.p99_us,
        slow_path_p99_us: r.stats.latency.slow_path.p99_us,
        all_p99_us: r.stats.latency.all.p99_us,
        fast_path_p50_us: r.stats.latency.fast_path.p50_us,
        slow_path_p50_us: r.stats.latency.slow_path.p50_us,
        all_p50_us: r.stats.latency.all.p50_us,
    };
    let json = serde_json::to_string_pretty(&baseline).expect("serialize baseline");
    std::fs::write(&args.baseline_path, json).expect("write baseline file");
    println!("Baseline saved to {}", args.baseline_path);
}

fn check_baseline(r: &BenchResult, args: &Args) -> bool {
    let data = match std::fs::read_to_string(&args.baseline_path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!(
                "ERROR: Cannot read baseline file '{}': {}",
                args.baseline_path, e
            );
            return false;
        }
    };
    let baseline: PerfBaseline = match serde_json::from_str(&data) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("ERROR: Cannot parse baseline file: {e}");
            return false;
        }
    };

    let mut passed = true;

    // TPS regression check
    let tps_floor = baseline.tps * (1.0 - f64::from(args.tps_threshold_pct) / 100.0);
    if r.tps < tps_floor {
        eprintln!(
            "PERF REGRESSION: TPS {:.1} < baseline floor {:.1} (baseline={:.1}, threshold=-{}%)",
            r.tps, tps_floor, baseline.tps, args.tps_threshold_pct
        );
        passed = false;
    } else {
        println!(
            "TPS OK: {:.1} >= {:.1} (baseline={:.1})",
            r.tps, tps_floor, baseline.tps
        );
    }

    // P99 regression check
    let p99_ceiling = baseline.all_p99_us as f64 * (1.0 + f64::from(args.p99_threshold_pct) / 100.0);
    let current_p99 = r.stats.latency.all.p99_us;
    if current_p99 as f64 > p99_ceiling {
        eprintln!(
            "PERF REGRESSION: P99 {}µs > baseline ceiling {:.0}µs (baseline={}µs, threshold=+{}%)",
            current_p99, p99_ceiling, baseline.all_p99_us, args.p99_threshold_pct
        );
        passed = false;
    } else {
        println!(
            "P99 OK: {}µs <= {:.0}µs (baseline={}µs)",
            current_p99, p99_ceiling, baseline.all_p99_us
        );
    }

    // Fast-path must be better than slow-path (invariant)
    let fp99 = r.stats.latency.fast_path.p99_us;
    let sp99 = r.stats.latency.slow_path.p99_us;
    if fp99 > 0 && sp99 > 0 && fp99 > sp99 {
        eprintln!(
            "INVARIANT VIOLATION: fast-path P99 ({fp99}µs) > slow-path P99 ({sp99}µs)"
        );
        passed = false;
    } else if fp99 > 0 && sp99 > 0 {
        println!(
            "INVARIANT OK: fast-path P99 ({fp99}µs) <= slow-path P99 ({sp99}µs)"
        );
    }

    if passed {
        println!("\n✓ All performance invariants passed");
    } else {
        eprintln!("\n✗ Performance regression detected — FAIL");
    }
    passed
}

// ── P4-1: Long-Run Stress Test ──────────────────────────────────────────

#[derive(serde::Serialize, Debug, Clone)]
struct LongRunSample {
    elapsed_secs: u64,
    interval_ops: u64,
    interval_tps: f64,
    cumulative_ops: u64,
    cumulative_tps: f64,
    fast_path_p99_us: u64,
    slow_path_p99_us: u64,
    all_p99_us: u64,
    committed: u64,
    aborted: u64,
}

#[derive(serde::Serialize, Debug)]
struct LongRunReport {
    duration_secs: u64,
    total_ops: u64,
    avg_tps: f64,
    min_interval_tps: f64,
    max_interval_tps: f64,
    final_p99_us: u64,
    max_p99_us: u64,
    total_committed: u64,
    total_aborted: u64,
    samples: Vec<LongRunSample>,
    stable: bool,
}

fn run_long_run(args: &Args) {
    let target_duration = parse_duration(&args.duration);
    let sample_interval = Duration::from_secs(args.sample_interval_secs);

    println!("═══════════════════════════════════════════════");
    println!("  LONG-RUN STRESS TEST");
    println!("═══════════════════════════════════════════════");
    println!("  Target duration:   {target_duration:?}");
    println!("  Sample interval:   {sample_interval:?}");
    println!("  Seed:              {}", args.seed);
    println!("  Shards:            {}", args.shards);
    println!("  Record count:      {}", args.record_count);
    println!();

    let storage = Arc::new(StorageEngine::new_in_memory());
    storage.create_table(bench_schema()).unwrap();
    let mgr = Arc::new(TxnManager::new(storage.clone()));
    let isolation = isolation_from_str(&args.isolation);
    let table_id = TableId(1);

    // Load initial data
    for i in 0..args.record_count {
        let txn = mgr.begin(isolation);
        let row = OwnedRow::new(vec![
            Datum::Int64(i as i64),
            Datum::Text(format!("value_{i}")),
        ]);
        storage.insert(table_id, row, txn.txn_id).unwrap();
        mgr.commit(txn.txn_id).unwrap();
    }
    mgr.reset_latency();

    let mut rng = Rng::new(args.seed);
    let mut samples: Vec<LongRunSample> = Vec::new();
    let mut total_ops = 0u64;
    let mut interval_ops = 0u64;
    let mut min_tps = f64::MAX;
    let mut max_tps = 0.0f64;
    let mut max_p99 = 0u64;

    let global_start = Instant::now();
    let mut interval_start = Instant::now();

    loop {
        if global_start.elapsed() >= target_duration {
            break;
        }

        // Execute one operation
        let is_read = rng.next_pct() < args.read_pct;
        let is_local = rng.next_pct() < args.local_pct;

        let classification = if is_local {
            let shard = ShardId(rng.next_u64() % args.shards);
            TxnClassification::local(shard)
        } else {
            let s1 = ShardId(0);
            let s2 = ShardId(1u64.min(args.shards - 1));
            TxnClassification::global(vec![s1, s2], SlowPathMode::Xa2Pc)
        };

        let txn = mgr.begin_with_classification(isolation, classification);
        let key = (rng.next_u64() % args.record_count) as i64;

        if is_read {
            let read_ts = txn.read_ts(mgr.current_ts());
            let _ = storage.scan(table_id, txn.txn_id, read_ts);
            let _ = mgr.commit(txn.txn_id);
        } else {
            let read_ts = txn.read_ts(mgr.current_ts());
            let rows = storage.scan(table_id, txn.txn_id, read_ts).unwrap();
            for (pk, row) in &rows {
                if let Some(Datum::Int64(k)) = row.values.first() {
                    if *k == key {
                        let new_row = OwnedRow::new(vec![
                            Datum::Int64(key),
                            Datum::Text(format!("updated_{}", rng.next_u64())),
                        ]);
                        let _ = storage.update(table_id, pk, new_row, txn.txn_id);
                        break;
                    }
                }
            }
            let _ = mgr.commit(txn.txn_id);
        }

        total_ops += 1;
        interval_ops += 1;

        // Sample at interval
        if interval_start.elapsed() >= sample_interval {
            let interval_elapsed = interval_start.elapsed();
            let interval_tps = interval_ops as f64 / interval_elapsed.as_secs_f64();
            let cumulative_tps = total_ops as f64 / global_start.elapsed().as_secs_f64();
            let stats = mgr.stats_snapshot();

            if interval_tps < min_tps {
                min_tps = interval_tps;
            }
            if interval_tps > max_tps {
                max_tps = interval_tps;
            }
            if stats.latency.all.p99_us > max_p99 {
                max_p99 = stats.latency.all.p99_us;
            }

            let sample = LongRunSample {
                elapsed_secs: global_start.elapsed().as_secs(),
                interval_ops,
                interval_tps,
                cumulative_ops: total_ops,
                cumulative_tps,
                fast_path_p99_us: stats.latency.fast_path.p99_us,
                slow_path_p99_us: stats.latency.slow_path.p99_us,
                all_p99_us: stats.latency.all.p99_us,
                committed: stats.total_committed,
                aborted: stats.total_aborted,
            };

            if args.export != "json" {
                println!(
                    "  [{:>6}s] ops={:<8} tps={:<10.1} cum_tps={:<10.1} p99={}µs",
                    sample.elapsed_secs,
                    sample.interval_ops,
                    sample.interval_tps,
                    sample.cumulative_tps,
                    sample.all_p99_us,
                );
            }

            samples.push(sample);
            interval_ops = 0;
            interval_start = Instant::now();
        }
    }

    let total_elapsed = global_start.elapsed();
    let avg_tps = total_ops as f64 / total_elapsed.as_secs_f64();
    let final_stats = mgr.stats_snapshot();

    // Stability check: TPS should not drop more than 50% from peak
    let stable = min_tps >= max_tps * 0.5 || samples.len() < 3;

    let report = LongRunReport {
        duration_secs: total_elapsed.as_secs(),
        total_ops,
        avg_tps,
        min_interval_tps: if min_tps == f64::MAX { 0.0 } else { min_tps },
        max_interval_tps: max_tps,
        final_p99_us: final_stats.latency.all.p99_us,
        max_p99_us: max_p99,
        total_committed: final_stats.total_committed,
        total_aborted: final_stats.total_aborted,
        samples,
        stable,
    };

    match args.export.as_str() {
        "json" => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serialize report")
            );
        }
        "csv" => {
            println!("elapsed_secs,interval_ops,interval_tps,cumulative_ops,cumulative_tps,all_p99_us,committed,aborted");
            for s in &report.samples {
                println!(
                    "{},{},{:.1},{},{:.1},{},{},{}",
                    s.elapsed_secs,
                    s.interval_ops,
                    s.interval_tps,
                    s.cumulative_ops,
                    s.cumulative_tps,
                    s.all_p99_us,
                    s.committed,
                    s.aborted,
                );
            }
        }
        _ => {
            println!();
            println!("═══════════════════════════════════════════════");
            println!("  LONG-RUN SUMMARY");
            println!("═══════════════════════════════════════════════");
            println!("  Duration:          {} s", report.duration_secs);
            println!("  Total ops:         {}", report.total_ops);
            println!("  Avg TPS:           {:.1}", report.avg_tps);
            println!("  Min interval TPS:  {:.1}", report.min_interval_tps);
            println!("  Max interval TPS:  {:.1}", report.max_interval_tps);
            println!("  Final P99:         {} µs", report.final_p99_us);
            println!("  Max P99:           {} µs", report.max_p99_us);
            println!("  Total committed:   {}", report.total_committed);
            println!("  Total aborted:     {}", report.total_aborted);
            println!(
                "  Stable:            {}",
                if report.stable {
                    "YES"
                } else {
                    "NO — TPS dropped >50% from peak"
                }
            );
            println!();
        }
    }

    if !report.stable {
        eprintln!("WARNING: Long-run stability check FAILED — TPS variance too high");
        std::process::exit(1);
    }
}

// ── Bulk Insert Benchmark (via PG wire protocol) ─────────────────────────

/// Split a SQL string into individual statements on `;`, respecting
/// single-quoted strings, double-quoted identifiers, and `--` / `/* */` comments.
fn split_sql(input: &str) -> Vec<String> {
    // Strip UTF-8 BOM if present
    let input = input.strip_prefix('\u{FEFF}').unwrap_or(input);
    let mut stmts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;
    while i < len {
        let ch = chars[i];
        // -- line comment
        if ch == '-' && i + 1 < len && chars[i + 1] == '-' {
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        // /* block comment */
        if ch == '/' && i + 1 < len && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < len && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            i += 2;
            continue;
        }
        // single-quoted string
        if ch == '\'' {
            cur.push(ch);
            i += 1;
            while i < len {
                let c = chars[i];
                cur.push(c);
                i += 1;
                if c == '\'' {
                    if i < len && chars[i] == '\'' {
                        cur.push(chars[i]);
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        // double-quoted identifier
        if ch == '"' {
            cur.push(ch);
            i += 1;
            while i < len {
                let c = chars[i];
                cur.push(c);
                i += 1;
                if c == '"' {
                    if i < len && chars[i] == '"' {
                        cur.push(chars[i]);
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        // statement terminator
        if ch == ';' {
            let s = cur.trim().to_owned();
            if !s.is_empty() {
                stmts.push(s);
            }
            cur.clear();
            i += 1;
            continue;
        }
        cur.push(ch);
        i += 1;
    }
    let s = cur.trim().to_owned();
    if !s.is_empty() {
        stmts.push(s);
    }
    stmts
}

fn run_bulk_bench(args: &Args) {
    let password = if args.bulk_password.is_empty() {
        std::env::var("PGPASSWORD").unwrap_or_default()
    } else {
        args.bulk_password.clone()
    };

    let sql = match std::fs::read_to_string(&args.bulk_file) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("ERROR: Cannot read file '{}': {}", args.bulk_file, e);
            std::process::exit(1);
        }
    };

    let stmts = split_sql(&sql);
    let total_stmts = stmts.len();
    if total_stmts == 0 {
        eprintln!("ERROR: No SQL statements found in '{}'", args.bulk_file);
        std::process::exit(1);
    }

    // Normalize and validate sslmode: 'enable' is not a libpq value, map it to 'require'.
    let sslmode = match args.bulk_sslmode.to_ascii_lowercase().as_str() {
        "disable" | "allow" | "prefer" | "require" | "verify-ca" | "verify-full" => {
            args.bulk_sslmode.to_ascii_lowercase()
        }
        "enable" | "on" | "yes" | "true" | "1" => {
            eprintln!(
                "WARN: --bulk-sslmode '{}' is not a valid libpq value; using 'require' instead.",
                args.bulk_sslmode
            );
            "require".to_owned()
        }
        other => {
            eprintln!(
                "ERROR: Invalid --bulk-sslmode '{other}'. Valid values: disable | allow | prefer | require | verify-ca | verify-full"
            );
            std::process::exit(1);
        }
    };

    let connect_str = {
        let mut s = format!(
            "host={} port={} user={} dbname={} sslmode={}",
            args.bulk_host, args.bulk_port, args.bulk_user, args.bulk_dbname, sslmode
        );
        if !password.is_empty() {
            s.push_str(&format!(" password={password}"));
        }
        if args.bulk_connect_timeout > 0 {
            s.push_str(&format!(" connect_timeout={}", args.bulk_connect_timeout));
        }
        s
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let (client, connection) = match tokio_postgres::connect(&connect_str, tokio_postgres::NoTls).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "ERROR: Connection failed: {}\n  Connection: {}:{}  user={} dbname={} sslmode={}\n  Hint: use --bulk-sslmode disable for local FalconDB (no TLS)",
                    e, args.bulk_host, args.bulk_port, args.bulk_user, args.bulk_dbname, sslmode
                );
                std::process::exit(1);
            }
        };

        // Drive the connection in the background
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {e}");
            }
        });

        println!("═══════════════════════════════════════════════════════");
        println!("  Falcon Bulk Insert Benchmark");
        println!("═══════════════════════════════════════════════════════");
        println!("  File:              {}", args.bulk_file);
        println!("  Server:            {}:{}", args.bulk_host, args.bulk_port);
        println!("  Total statements:  {total_stmts}");
        println!("  Progress interval: every {} statements", args.bulk_progress_interval);
        println!("───────────────────────────────────────────────────────");

        let mut latencies_us: Vec<u64> = Vec::with_capacity(total_stmts);
        let mut errors = 0u64;
        let mut rows_affected: u64 = 0;
        let mut ddl_count = 0u64;
        let mut dml_count = 0u64;

        let bench_start = std::time::Instant::now();

        for (idx, stmt) in stmts.iter().enumerate() {
            let op_start = std::time::Instant::now();

            match client.simple_query(stmt).await {
                Ok(results) => {
                    for msg in &results {
                        if let tokio_postgres::SimpleQueryMessage::CommandComplete(n) = msg {
                            rows_affected += n;
                        }
                    }
                    let upper = stmt.trim_start().to_uppercase();
                    if upper.starts_with("INSERT") || upper.starts_with("UPDATE") || upper.starts_with("DELETE") {
                        dml_count += 1;
                    } else {
                        ddl_count += 1;
                    }
                }
                Err(e) => {
                    errors += 1;
                    eprintln!("  [stmt {}] ERROR: {}", idx + 1, e);
                }
            }

            latencies_us.push(op_start.elapsed().as_micros() as u64);

            // Progress report
            if args.bulk_progress_interval > 0 && (idx + 1) % args.bulk_progress_interval == 0 {
                let elapsed_so_far = bench_start.elapsed().as_millis() as u64;
                let tps_so_far = if elapsed_so_far > 0 {
                    (idx + 1) as f64 / (elapsed_so_far as f64 / 1000.0)
                } else {
                    0.0
                };
                println!(
                    "  [{:>6}/{:>6}] elapsed: {:>6} ms  stmt/s: {:>8.1}  rows: {:>10}  errors: {}",
                    idx + 1,
                    total_stmts,
                    elapsed_so_far,
                    tps_so_far,
                    rows_affected,
                    errors
                );
            }
        }

        let elapsed_ms = bench_start.elapsed().as_millis() as u64;
        let tps = if elapsed_ms > 0 {
            total_stmts as f64 / (elapsed_ms as f64 / 1000.0)
        } else {
            0.0
        };
        let rows_per_sec = if elapsed_ms > 0 {
            rows_affected as f64 / (elapsed_ms as f64 / 1000.0)
        } else {
            0.0
        };

        // Compute latency percentiles
        latencies_us.sort_unstable();
        let len = latencies_us.len();
        let p50 = if len > 0 { latencies_us[len * 50 / 100] } else { 0 };
        let p95 = if len > 0 { latencies_us[len * 95 / 100] } else { 0 };
        let p99 = if len > 0 { latencies_us[(len * 99 / 100).min(len - 1)] } else { 0 };
        let max = latencies_us.last().copied().unwrap_or(0);
        let avg = if len > 0 {
            latencies_us.iter().sum::<u64>() / len as u64
        } else {
            0
        };

        match args.export.as_str() {
            "json" => {
                let obj = serde_json::json!({
                    "workload": "bulk_insert",
                    "file": args.bulk_file,
                    "total_statements": total_stmts,
                    "dml_statements": dml_count,
                    "ddl_statements": ddl_count,
                    "rows_affected": rows_affected,
                    "errors": errors,
                    "elapsed_ms": elapsed_ms,
                    "stmt_per_sec": tps,
                    "rows_per_sec": rows_per_sec,
                    "latency_us": {
                        "avg": avg,
                        "p50": p50,
                        "p95": p95,
                        "p99": p99,
                        "max": max
                    }
                });
                println!("{}", serde_json::to_string_pretty(&obj).unwrap());
            }
            "csv" => {
                println!("workload,file,total_stmts,dml,ddl,rows_affected,errors,elapsed_ms,stmt_per_sec,rows_per_sec,avg_us,p50_us,p95_us,p99_us,max_us");
                println!(
                    "bulk_insert,{},{},{},{},{},{},{},{:.1},{:.1},{},{},{},{},{}",
                    args.bulk_file,
                    total_stmts,
                    dml_count,
                    ddl_count,
                    rows_affected,
                    errors,
                    elapsed_ms,
                    tps,
                    rows_per_sec,
                    avg,
                    p50,
                    p95,
                    p99,
                    max
                );
            }
            _ => {
                println!("───────────────────────────────────────────────────────");
                println!("  RESULTS");
                println!("───────────────────────────────────────────────────────");
                println!("  Total statements:  {total_stmts}");
                println!("  DML statements:    {dml_count}");
                println!("  DDL statements:    {ddl_count}");
                println!("  Rows affected:     {rows_affected}");
                println!("  Errors:            {errors}");
                println!("  Elapsed:           {elapsed_ms} ms");
                println!("  Stmt/s:            {tps:.1}");
                println!("  Rows/s:            {rows_per_sec:.1}");
                println!("  ─── Per-Statement Latency (µs) ───");
                println!("  Avg:    {avg:>8}");
                println!("  P50:    {p50:>8}");
                println!("  P95:    {p95:>8}");
                println!("  P99:    {p99:>8}");
                println!("  Max:    {max:>8}");
                println!("═══════════════════════════════════════════════════════");
                if errors > 0 {
                    println!("  WARNING: {errors} statement(s) failed.");
                } else {
                    println!("  All statements executed successfully.");
                }
                println!("═══════════════════════════════════════════════════════");
            }
        }

        if errors > 0 {
            std::process::exit(2);
        }
    });
}

// ── Main ─────────────────────────────────────────────────────────────────

fn main() {
    let args = Args::parse();

    if args.bulk {
        run_bulk_bench(&args);
    } else if args.long_run {
        run_long_run(&args);
    } else if args.tpcb {
        run_tpcb(&args);
    } else if args.lsm {
        #[cfg(feature = "lsm")]
        run_lsm_bench(&args);
        #[cfg(not(feature = "lsm"))]
        {
            eprintln!("ERROR: LSM benchmark requires the `lsm` feature. Build with: cargo build -p falcon_bench --features lsm");
            std::process::exit(1);
        }
    } else if args.failover {
        run_failover_bench(&args);
    } else if args.scaleout {
        run_scaleout(&args);
    } else if args.compare {
        // Run comparison: Fast-Path ON (mixed local/global) vs OFF (all global)
        println!("Running Fast-Path comparison benchmark...\n");

        let on = run_workload(&args, false, "Fast-Path ON");
        let off = run_workload(&args, true, "Fast-Path OFF (all GlobalTxn)");

        print_result(&on, &args.export);
        print_result(&off, &args.export);

        if args.export == "text" {
            let speedup = if off.stats.latency.all.p50_us > 0 {
                on.stats.latency.all.p50_us as f64 / off.stats.latency.all.p50_us as f64
            } else {
                0.0
            };
            let tps_ratio = if off.tps > 0.0 { on.tps / off.tps } else { 0.0 };
            println!("═══════════════════════════════════════════════");
            println!("  COMPARISON SUMMARY");
            println!("═══════════════════════════════════════════════");
            println!("  TPS ratio (ON/OFF):     {tps_ratio:.2}x");
            println!("  p50 latency ratio:      {speedup:.2}x");
            println!("  Fast-path commits (ON): {}", on.stats.fast_path_commits);
            println!("  Slow-path commits (ON): {}", on.stats.slow_path_commits);
        }

        // P2-1: Assert fast-path P99 < slow-path P99
        if on.stats.latency.fast_path.p99_us > 0
            && on.stats.latency.slow_path.p99_us > 0
            && on.stats.latency.fast_path.p99_us > on.stats.latency.slow_path.p99_us
        {
            eprintln!(
                "INVARIANT VIOLATION: fast-path P99 ({}µs) > slow-path P99 ({}µs)",
                on.stats.latency.fast_path.p99_us, on.stats.latency.slow_path.p99_us
            );
            std::process::exit(1);
        }
    } else {
        let result = run_workload(&args, false, "YCSB Workload A");
        print_result(&result, &args.export);

        // Baseline save/check
        if args.baseline_save {
            save_baseline(&result, &args);
        }
        if args.baseline_check && !check_baseline(&result, &args) {
            std::process::exit(1);
        }
    }
}
