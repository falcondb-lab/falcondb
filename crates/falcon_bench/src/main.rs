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
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 1 } else { seed })
    }
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn next_pct(&mut self) -> u8 {
        (self.next_u64() % 100) as u8
    }
}

struct BenchResult {
    label: String,
    ops: u64,
    elapsed_ms: u64,
    tps: f64,
    stats: falcon_txn::TxnStatsSnapshot,
}

fn run_workload(
    args: &Args,
    force_all_global: bool,
    label: &str,
) -> BenchResult {
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
            Datum::Text(format!("value_{}", i)),
        ]);
        storage.insert(table_id, row, txn.txn_id).unwrap();
        mgr.commit(txn.txn_id).unwrap();
    }

    // Reset latency after load phase
    mgr.reset_latency();

    let mut rng = Rng::new(42);

    // Phase 2: Run workload
    let start = Instant::now();

    for _ in 0..args.ops {
        let is_read = rng.next_pct() < args.read_pct;
        let is_local = !force_all_global && rng.next_pct() < args.local_pct;

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
            match mgr.commit(txn.txn_id) {
                Ok(_) => {}
                Err(_) => {
                    // OCC or constraint failure — expected under contention
                }
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
        label: label.to_string(),
        ops: args.ops,
        elapsed_ms,
        tps,
        stats,
    }
}

fn print_result_text(r: &BenchResult) {
    println!("═══════════════════════════════════════════════");
    println!("  {} ", r.label);
    println!("═══════════════════════════════════════════════");
    println!("  Operations:        {}", r.ops);
    println!("  Elapsed:           {} ms", r.elapsed_ms);
    println!("  TPS:               {:.1}", r.tps);
    println!("  ─── Commits ───");
    println!("  Total committed:   {}", r.stats.total_committed);
    println!("  Fast-path commits: {}", r.stats.fast_path_commits);
    println!("  Slow-path commits: {}", r.stats.slow_path_commits);
    println!("  Total aborted:     {}", r.stats.total_aborted);
    println!("  OCC conflicts:     {}", r.stats.occ_conflicts);
    println!("  Constraint viols:  {}", r.stats.constraint_violations);
    println!("  Degraded→Global:   {}", r.stats.degraded_to_global);
    println!("  ─── Latency (commit, µs) ───");
    println!("  Fast-path  p50={:>6}  p95={:>6}  p99={:>6}  (n={})",
        r.stats.latency.fast_path.p50_us,
        r.stats.latency.fast_path.p95_us,
        r.stats.latency.fast_path.p99_us,
        r.stats.latency.fast_path.count,
    );
    println!("  Slow-path  p50={:>6}  p95={:>6}  p99={:>6}  (n={})",
        r.stats.latency.slow_path.p50_us,
        r.stats.latency.slow_path.p95_us,
        r.stats.latency.slow_path.p99_us,
        r.stats.latency.slow_path.count,
    );
    println!("  All        p50={:>6}  p95={:>6}  p99={:>6}  (n={})",
        r.stats.latency.all.p50_us,
        r.stats.latency.all.p95_us,
        r.stats.latency.all.p99_us,
        r.stats.latency.all.count,
    );
    println!();
}

fn print_result_csv(r: &BenchResult) {
    println!("label,ops,elapsed_ms,tps,committed,fast,slow,aborted,occ,constraint,fast_p50,fast_p95,fast_p99,slow_p50,slow_p95,slow_p99,all_p50,all_p95,all_p99");
    println!("{},{},{},{:.1},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        r.label, r.ops, r.elapsed_ms, r.tps,
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
    let obj = serde_json::json!({
        "label": r.label,
        "ops": r.ops,
        "elapsed_ms": r.elapsed_ms,
        "tps": r.tps,
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
    println!("  ops/round: {}  records: {}\n", ops_per_round, record_count);

    if args.export == "csv" {
        println!("shards,ops,elapsed_ms,tps,scatter_gather_total_us,max_subplan_us,gather_merge_us,rows_total");
    }

    let mut results: Vec<serde_json::Value> = Vec::new();

    for &n_shards in &shard_counts {
        let engine = Arc::new(ShardedEngine::new(n_shards));
        engine.create_table_all(&bench_schema()).unwrap();

        // Load data round-robin across shards
        let mut rng = Rng::new(42);
        for i in 0..record_count {
            let shard_idx = i % n_shards;
            let shard = engine.shard(ShardId(shard_idx)).unwrap();
            let txn = shard.txn_mgr.begin(isolation);
            let row = OwnedRow::new(vec![
                Datum::Int64(i as i64),
                Datum::Text(format!("value_{}", i)),
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
                        vec![("ycsb_key".into(), DataType::Int64), ("field0".into(), DataType::Text)],
                        filtered,
                    ))
                });
                let ((_, rows), metrics) = exec
                    .scatter_gather(&subplan, &all_shards, &GatherStrategy::Union { distinct: false, limit: None, offset: None })
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

        match args.export.as_str() {
            "csv" => {
                println!(
                    "{},{},{},{:.1},{},{},{},{}",
                    n_shards, ops_per_round, elapsed_ms, tps,
                    total_sg_us, max_sub_us, gather_us, total_rows,
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
                    "  {:>2} shards │ {:>8} ops │ {:>6} ms │ {:>10.1} TPS │ sg_total={:>8}µs max_sub={:>6}µs gather={:>6}µs",
                    n_shards, ops_per_round, elapsed_ms, tps,
                    total_sg_us, max_sub_us, gather_us,
                );
            }
        }
    }

    if args.export == "json" {
        println!("{}", serde_json::to_string_pretty(&results).unwrap());
    } else if args.export != "csv" {
        println!();
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
            Datum::Text(format!("value_{}", i)),
        ]);
        let txn_id = TxnId(i + 1);
        let ts = Timestamp((i + 1) * 10);
        group.primary.storage.insert(table_id, row.clone(), txn_id).unwrap();
        group.primary.storage
            .commit_txn(txn_id, ts, TxnType::Local)
            .unwrap();
        group.ship_wal_record(WalRecord::Insert { txn_id, table_id, row });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id, commit_ts: ts });
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
    } else { 0.0 };

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
    } else { 0.0 };

    // Compute percentiles
    fn percentiles(v: &mut [u64]) -> (u64, u64, u64) {
        v.sort();
        let len = v.len();
        if len == 0 { return (0, 0, 0); }
        let p50 = v[len * 50 / 100];
        let p95 = v[len * 95 / 100];
        let p99 = v[len.saturating_sub(1).min(len * 99 / 100)];
        (p50, p95, p99)
    }

    let (bp50, bp95, bp99) = percentiles(&mut latencies_before);
    let (ap50, ap95, ap99) = percentiles(&mut latencies_after);

    // Verify data integrity
    let rows = group.primary.storage.scan(table_id, TxnId(999999), Timestamp(u64::MAX - 2)).unwrap();
    let data_intact = rows.len() == record_count as usize;

    match args.export.as_str() {
        "csv" => {
            println!("phase,ops,elapsed_ms,tps,p50_ns,p95_ns,p99_ns,failover_ms,data_intact");
            println!("before,{},{},{:.1},{},{},{},0,true", ops_half, elapsed_before_ms, tps_before, bp50, bp95, bp99);
            println!("failover,0,{},0,0,0,0,{},{}", failover_ms, failover_ms, data_intact);
            println!("after,{},{},{:.1},{},{},{},0,{}", ops_half, elapsed_after_ms, tps_after, ap50, ap95, ap99, data_intact);
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
            println!("  Ops:     {}  TPS: {:.1}", ops_half, tps_before);
            println!("  Latency: p50={}ns  p95={}ns  p99={}ns", bp50, bp95, bp99);
            println!("  ─── Failover ───");
            println!("  Duration:      {} ms", failover_ms);
            println!("  Promote count: {}", metrics.promote_count);
            println!("  ─── After Failover ───");
            println!("  Ops:     {}  TPS: {:.1}", ops_half, tps_after);
            println!("  Latency: p50={}ns  p95={}ns  p99={}ns", ap50, ap95, ap99);
            println!("  ─── Data Integrity ───");
            println!("  Rows after failover: {} (expected {})", rows.len(), record_count);
            println!("  Data intact:         {}", data_intact);
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
                ColumnDef { id: ColumnId(0), name: "aid".into(), data_type: DataType::Int64, nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "bid".into(), data_type: DataType::Int64, nullable: false, is_primary_key: false, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(2), name: "abalance".into(), data_type: DataType::Int64, nullable: false, is_primary_key: false, default_value: None, is_serial: false },
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        },
        TableSchema {
            id: TableId(11),
            name: "pgbench_tellers".into(),
            columns: vec![
                ColumnDef { id: ColumnId(0), name: "tid".into(), data_type: DataType::Int64, nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "bid".into(), data_type: DataType::Int64, nullable: false, is_primary_key: false, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(2), name: "tbalance".into(), data_type: DataType::Int64, nullable: false, is_primary_key: false, default_value: None, is_serial: false },
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        },
        TableSchema {
            id: TableId(12),
            name: "pgbench_branches".into(),
            columns: vec![
                ColumnDef { id: ColumnId(0), name: "bid".into(), data_type: DataType::Int64, nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "bbalance".into(), data_type: DataType::Int64, nullable: false, is_primary_key: false, default_value: None, is_serial: false },
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
        storage.insert(TableId(12), OwnedRow::new(vec![Datum::Int64(i as i64), Datum::Int64(0)]), txn.txn_id).unwrap();
        mgr.commit(txn.txn_id).unwrap();
    }
    // Load: tellers
    for i in 0..n_tellers {
        let txn = mgr.begin(isolation);
        storage.insert(TableId(11), OwnedRow::new(vec![Datum::Int64(i as i64), Datum::Int64((i % n_branches) as i64), Datum::Int64(0)]), txn.txn_id).unwrap();
        mgr.commit(txn.txn_id).unwrap();
    }
    // Load: accounts
    for i in 0..n_accounts {
        let txn = mgr.begin(isolation);
        storage.insert(TableId(10), OwnedRow::new(vec![Datum::Int64(i as i64), Datum::Int64((i % n_branches) as i64), Datum::Int64(0)]), txn.txn_id).unwrap();
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
                    let old_bal = match row.values.get(2) { Some(Datum::Int64(b)) => *b, _ => 0 };
                    let new_row = OwnedRow::new(vec![Datum::Int64(aid), Datum::Int64(bid), Datum::Int64(old_bal + delta)]);
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
    let tps = if elapsed_ms > 0 { args.ops as f64 / (elapsed_ms as f64 / 1000.0) } else { 0.0 };

    latencies_us.sort();
    let len = latencies_us.len();
    let p50 = if len > 0 { latencies_us[len * 50 / 100] } else { 0 };
    let p95 = if len > 0 { latencies_us[len * 95 / 100] } else { 0 };
    let p99 = if len > 0 { latencies_us[len.saturating_sub(1).min(len * 99 / 100)] } else { 0 };
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
            println!("tpcb,{},{},{},{:.1},{},{},{},{},{},{},{}",
                scale, args.ops, elapsed_ms, tps, committed, aborted, p50, p95, p99, max, stats.admission_rejections);
        }
        _ => {
            println!("═══════════════════════════════════════════════");
            println!("  TPC-B (pgbench) Benchmark");
            println!("═══════════════════════════════════════════════");
            println!("  Scale factor:      {}", scale);
            println!("  Accounts:          {}", n_accounts);
            println!("  Operations:        {}", args.ops);
            println!("  Elapsed:           {} ms", elapsed_ms);
            println!("  TPS:               {:.1}", tps);
            println!("  Committed:         {}", committed);
            println!("  Aborted:           {}", aborted);
            println!("  ─── Latency (µs) ───");
            println!("  P50:    {:>8}", p50);
            println!("  P95:    {:>8}", p95);
            println!("  P99:    {:>8}", p99);
            println!("  Max:    {:>8}", max);
            println!("  ─── Backpressure ───");
            println!("  Rejections:        {}", stats.admission_rejections);
            println!();
        }
    }
}

// ── LSM disk-backed KV benchmark ────────────────────────────────────────

fn run_lsm_bench(args: &Args) {
    use falcon_storage::lsm::{LsmEngine, LsmConfig};
    use falcon_storage::lsm::compaction::CompactionConfig;

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
    let tps = if elapsed_ms > 0 { ops as f64 / (elapsed_ms as f64 / 1000.0) } else { 0.0 };

    latencies_us.sort();
    let len = latencies_us.len();
    let p50 = if len > 0 { latencies_us[len * 50 / 100] } else { 0 };
    let p95 = if len > 0 { latencies_us[len * 95 / 100] } else { 0 };
    let p99 = if len > 0 { latencies_us[len.saturating_sub(1).min(len * 99 / 100)] } else { 0 };
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
            println!("lsm_kv,{},{},{},{:.1},{},{},{},{},{},{},{},{},{},{:.4}",
                n, ops, elapsed_ms, tps, reads, writes, stalls, p50, p95, p99, max,
                stats.flushes_completed, stats.compaction.runs_completed, stats.block_cache.hit_rate);
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
            println!("  Block cache hit:   {:.2}%", stats.block_cache.hit_rate * 100.0);
            println!();
        }
    }
}

fn main() {
    let args = Args::parse();

    if args.tpcb {
        run_tpcb(&args);
    } else if args.lsm {
        run_lsm_bench(&args);
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
            println!("  TPS ratio (ON/OFF):     {:.2}x", tps_ratio);
            println!("  p50 latency ratio:      {:.2}x", speedup);
            println!("  Fast-path commits (ON): {}", on.stats.fast_path_commits);
            println!("  Slow-path commits (ON): {}", on.stats.slow_path_commits);
        }
    } else {
        let result = run_workload(&args, false, "YCSB Workload A");
        print_result(&result, &args.export);
    }
}
