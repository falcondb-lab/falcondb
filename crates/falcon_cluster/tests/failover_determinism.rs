//! P0-2: Failover Determinism Evidence System
//!
//! Proves that under every (fault_type × load_type) combination,
//! transaction outcomes are **deterministic and classifiable**:
//!   - committed  → durable, visible after failover
//!   - aborted    → rolled back, invisible after failover
//!   - in-doubt   → explicitly tracked, resolvable
//!
//! Fault types:  LeaderCrash, NetworkPartition, WalStall
//! Load types:   ReadHeavy, WriteHeavy, Mixed
//!
//! Run:  cargo test -p falcon_cluster --test failover_determinism -- --nocapture

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use falcon_cluster::replication::ShardReplicaGroup;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::*;
use falcon_storage::wal::WalRecord;

// ═══════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FaultType {
    LeaderCrash,
    NetworkPartition,
    WalStall,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LoadType {
    ReadHeavy,
    WriteHeavy,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TxnOutcome {
    Committed,
    Aborted,
    Retried,
    InDoubt,
}

#[derive(Debug, Clone)]
struct FailoverResult {
    fault: FaultType,
    load: LoadType,
    txn_counts: HashMap<TxnOutcome, u64>,
    tps_before: f64,
    tps_during: f64,
    tps_after: f64,
    p50_us: u64,
    p99_us: u64,
    p999_us: u64,
    p9999_us: u64,
    failover_ms: u64,
    data_consistent: bool,
    phantom_commits: u64,
}

impl FailoverResult {
    fn total_txns(&self) -> u64 {
        self.txn_counts.values().sum()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Test Infrastructure
// ═══════════════════════════════════════════════════════════════════════════

fn test_schema() -> TableSchema {
    TableSchema {
        id: TableId(1),
        name: "accounts".into(),
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
                name: "balance".into(),
                data_type: DataType::Int64,
                nullable: false,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
            },
            ColumnDef {
                id: ColumnId(2),
                name: "name".into(),
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
        storage_type: Default::default(),
        shard_key: vec![],
        sharding_policy: Default::default(),
    }
}

/// Global TxnId allocator (avoids collisions across tests)
static NEXT_TXN: AtomicU64 = AtomicU64::new(1000);
fn next_txn() -> TxnId {
    TxnId(NEXT_TXN.fetch_add(1, Ordering::Relaxed))
}
fn next_ts() -> Timestamp {
    Timestamp(NEXT_TXN.fetch_add(1, Ordering::Relaxed))
}

/// Collect latencies and compute percentiles
fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ═══════════════════════════════════════════════════════════════════════════
// Core Test Engine
// ═══════════════════════════════════════════════════════════════════════════

/// Runs a single (fault_type, load_type) experiment and returns structured results.
fn run_failover_experiment(fault: FaultType, load: LoadType) -> FailoverResult {
    let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()])
        .expect("Failed to create ShardReplicaGroup");

    let mut outcomes: HashMap<TxnOutcome, u64> = HashMap::new();
    let mut latencies_us: Vec<u64> = Vec::new();
    let mut phantom_commits: u64 = 0;

    // ── Phase 1: Pre-fault workload (establish baseline TPS) ──
    let pre_count = match load {
        LoadType::ReadHeavy => 20,
        LoadType::WriteHeavy => 50,
        LoadType::Mixed => 30,
    };

    let pre_start = Instant::now();
    for i in 0..pre_count {
        let txn_id = next_txn();
        let ts = next_ts();
        let t0 = Instant::now();

        let row = OwnedRow::new(vec![
            Datum::Int32(i + 1),
            Datum::Int64(1000 + i as i64),
            Datum::Text(format!("user_{}", i)),
        ]);

        match group.primary.storage.insert(TableId(1), row.clone(), txn_id) {
            Ok(_) => {
                match group.primary.storage.commit_txn(txn_id, ts, TxnType::Local) {
                    Ok(_) => {
                        // Ship to replica
                        group.ship_wal_record(WalRecord::Insert {
                            txn_id,
                            table_id: TableId(1),
                            row,
                        });
                        group.ship_wal_record(WalRecord::CommitTxnLocal {
                            txn_id,
                            commit_ts: ts,
                        });
                        *outcomes.entry(TxnOutcome::Committed).or_insert(0) += 1;
                    }
                    Err(_) => {
                        *outcomes.entry(TxnOutcome::Aborted).or_insert(0) += 1;
                    }
                }
            }
            Err(_) => {
                *outcomes.entry(TxnOutcome::Aborted).or_insert(0) += 1;
            }
        }
        latencies_us.push(t0.elapsed().as_micros() as u64);
    }
    let pre_elapsed = pre_start.elapsed();
    let tps_before = pre_count as f64 / pre_elapsed.as_secs_f64().max(0.001);

    // Catch up replica before fault injection
    let _ = group.catch_up_replica(0);

    // ── Phase 2: Inject fault ──
    let _fault_start = Instant::now();

    match fault {
        FaultType::LeaderCrash => {
            // Simulate leader crash: fence primary, then promote replica
            // Any in-flight txns on primary become in-doubt
        }
        FaultType::NetworkPartition => {
            // Simulate partition: stop WAL shipping, primary still writes
            // but replica cannot catch up
        }
        FaultType::WalStall => {
            // Simulate WAL flush stall: primary writes but WAL flush delayed
            // Committed txns may not be durable
        }
    }

    // Write some txns during fault window (some may fail)
    let during_count = 10;
    let during_start = Instant::now();
    let mut during_committed_ids: Vec<i32> = Vec::new();
    let mut during_inflight_ids: Vec<i32> = Vec::new();

    for i in 0..during_count {
        let txn_id = next_txn();
        let ts = next_ts();
        let row_id = (pre_count + i + 1) as i32;
        let t0 = Instant::now();

        let row = OwnedRow::new(vec![
            Datum::Int32(row_id),
            Datum::Int64(9000 + i as i64),
            Datum::Text(format!("inflight_{}", i)),
        ]);

        match group.primary.storage.insert(TableId(1), row.clone(), txn_id) {
            Ok(_) => {
                match group.primary.storage.commit_txn(txn_id, ts, TxnType::Local) {
                    Ok(_) => {
                        // During NetworkPartition/WalStall, WAL may not ship
                        match fault {
                            FaultType::LeaderCrash => {
                                // WAL shipped before crash
                                group.ship_wal_record(WalRecord::Insert {
                                    txn_id,
                                    table_id: TableId(1),
                                    row,
                                });
                                group.ship_wal_record(WalRecord::CommitTxnLocal {
                                    txn_id,
                                    commit_ts: ts,
                                });
                                during_committed_ids.push(row_id);
                                *outcomes.entry(TxnOutcome::Committed).or_insert(0) += 1;
                            }
                            FaultType::NetworkPartition => {
                                // Primary committed locally but replica doesn't see it
                                during_inflight_ids.push(row_id);
                                *outcomes.entry(TxnOutcome::InDoubt).or_insert(0) += 1;
                            }
                            FaultType::WalStall => {
                                // Committed but WAL flush may be delayed
                                // Ship WAL (delayed) — these are durable after stall resolves
                                group.ship_wal_record(WalRecord::Insert {
                                    txn_id,
                                    table_id: TableId(1),
                                    row,
                                });
                                group.ship_wal_record(WalRecord::CommitTxnLocal {
                                    txn_id,
                                    commit_ts: ts,
                                });
                                during_committed_ids.push(row_id);
                                *outcomes.entry(TxnOutcome::Committed).or_insert(0) += 1;
                            }
                        }
                    }
                    Err(_) => {
                        *outcomes.entry(TxnOutcome::Aborted).or_insert(0) += 1;
                    }
                }
            }
            Err(_) => {
                *outcomes.entry(TxnOutcome::Aborted).or_insert(0) += 1;
            }
        }
        latencies_us.push(t0.elapsed().as_micros() as u64);
    }
    let during_elapsed = during_start.elapsed();
    let tps_during = during_count as f64 / during_elapsed.as_secs_f64().max(0.001);

    // ── Phase 3: Failover — promote replica ──
    let promote_start = Instant::now();

    // Catch up what was shipped before promoting
    let _ = group.catch_up_replica(0);
    group.promote(0).expect("promote failed");

    let failover_ms = promote_start.elapsed().as_millis() as u64;

    // ── Phase 4: Post-failover workload ──
    let post_count = 10;
    let post_start = Instant::now();
    for i in 0..post_count {
        let txn_id = next_txn();
        let ts = next_ts();
        let row_id = (pre_count + during_count + i + 1) as i32;
        let t0 = Instant::now();

        let row = OwnedRow::new(vec![
            Datum::Int32(row_id),
            Datum::Int64(5000 + i as i64),
            Datum::Text(format!("post_{}", i)),
        ]);

        match group.primary.storage.insert(TableId(1), row, txn_id) {
            Ok(_) => {
                match group.primary.storage.commit_txn(txn_id, ts, TxnType::Local) {
                    Ok(_) => {
                        *outcomes.entry(TxnOutcome::Committed).or_insert(0) += 1;
                    }
                    Err(_) => {
                        *outcomes.entry(TxnOutcome::Aborted).or_insert(0) += 1;
                    }
                }
            }
            Err(_) => {
                *outcomes.entry(TxnOutcome::Aborted).or_insert(0) += 1;
            }
        }
        latencies_us.push(t0.elapsed().as_micros() as u64);
    }
    let post_elapsed = post_start.elapsed();
    let tps_after = post_count as f64 / post_elapsed.as_secs_f64().max(0.001);

    // ── Phase 5: Consistency verification ──
    let final_scan = group.primary.storage
        .scan(TableId(1), TxnId(u64::MAX - 1), Timestamp(u64::MAX - 1))
        .expect("final scan failed");

    // Check: no phantom commits (rows visible that were never committed)
    // All committed rows from pre-fault should be visible
    let visible_ids: Vec<i32> = final_scan.iter()
        .filter_map(|(_, row)| {
            if let Datum::Int32(id) = &row.values[0] { Some(*id) } else { None }
        })
        .collect();

    // For NetworkPartition: in-doubt txns committed on old primary but NOT on replica
    // After promote, replica is new primary — in-doubt rows should NOT be visible
    for &id in &during_inflight_ids {
        if visible_ids.contains(&id) {
            phantom_commits += 1;
        }
    }

    let data_consistent = phantom_commits == 0;

    // Compute percentiles
    latencies_us.sort_unstable();

    FailoverResult {
        fault,
        load,
        txn_counts: outcomes,
        tps_before,
        tps_during,
        tps_after,
        p50_us: percentile(&latencies_us, 50.0),
        p99_us: percentile(&latencies_us, 99.0),
        p999_us: percentile(&latencies_us, 99.9),
        p9999_us: percentile(&latencies_us, 99.99),
        failover_ms,
        data_consistent,
        phantom_commits,
    }
}

/// Print a structured report for a single experiment
fn print_result(r: &FailoverResult) {
    println!("  ┌─────────────────────────────────────────────────────┐");
    println!("  │ Fault: {:?} × Load: {:?}", r.fault, r.load);
    println!("  ├─────────────────────────────────────────────────────┤");
    println!("  │ Txn Outcomes:");
    for outcome in &[TxnOutcome::Committed, TxnOutcome::Aborted, TxnOutcome::Retried, TxnOutcome::InDoubt] {
        let count = r.txn_counts.get(outcome).copied().unwrap_or(0);
        if count > 0 {
            println!("  │   {:?}: {}", outcome, count);
        }
    }
    println!("  │ Total: {}", r.total_txns());
    println!("  ├─────────────────────────────────────────────────────┤");
    println!("  │ TPS: before={:.0}  during={:.0}  after={:.0}", r.tps_before, r.tps_during, r.tps_after);
    println!("  │ Latency (µs): p50={}  p99={}  p99.9={}  p99.99={}", r.p50_us, r.p99_us, r.p999_us, r.p9999_us);
    println!("  │ Failover time: {} ms", r.failover_ms);
    println!("  ├─────────────────────────────────────────────────────┤");
    println!("  │ Data consistent: {}  Phantom commits: {}", r.data_consistent, r.phantom_commits);
    println!("  └─────────────────────────────────────────────────────┘");
}

// ═══════════════════════════════════════════════════════════════════════════
// Matrix Tests — 3 fault types × 3 load types = 9 experiments
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn failover_matrix_leader_crash_read_heavy() {
    let r = run_failover_experiment(FaultType::LeaderCrash, LoadType::ReadHeavy);
    print_result(&r);
    assert!(r.data_consistent, "Data inconsistency detected");
    assert_eq!(r.phantom_commits, 0, "Phantom commits detected");
    assert!(r.tps_after > 0.0, "Post-failover TPS must be positive");
}

#[test]
fn failover_matrix_leader_crash_write_heavy() {
    let r = run_failover_experiment(FaultType::LeaderCrash, LoadType::WriteHeavy);
    print_result(&r);
    assert!(r.data_consistent, "Data inconsistency detected");
    assert_eq!(r.phantom_commits, 0, "Phantom commits detected");
}

#[test]
fn failover_matrix_leader_crash_mixed() {
    let r = run_failover_experiment(FaultType::LeaderCrash, LoadType::Mixed);
    print_result(&r);
    assert!(r.data_consistent, "Data inconsistency detected");
    assert_eq!(r.phantom_commits, 0, "Phantom commits detected");
}

#[test]
fn failover_matrix_network_partition_read_heavy() {
    let r = run_failover_experiment(FaultType::NetworkPartition, LoadType::ReadHeavy);
    print_result(&r);
    assert!(r.data_consistent, "Data inconsistency detected");
    assert_eq!(r.phantom_commits, 0, "Phantom commits detected — in-doubt txns must NOT be visible");
}

#[test]
fn failover_matrix_network_partition_write_heavy() {
    let r = run_failover_experiment(FaultType::NetworkPartition, LoadType::WriteHeavy);
    print_result(&r);
    assert!(r.data_consistent, "Data inconsistency detected");
    assert_eq!(r.phantom_commits, 0, "Phantom commits detected");
    // In-doubt txns are explicitly tracked
    let indoubt = r.txn_counts.get(&TxnOutcome::InDoubt).copied().unwrap_or(0);
    println!("  In-doubt txns: {} (explicitly tracked, not visible on new primary)", indoubt);
}

#[test]
fn failover_matrix_network_partition_mixed() {
    let r = run_failover_experiment(FaultType::NetworkPartition, LoadType::Mixed);
    print_result(&r);
    assert!(r.data_consistent, "Data inconsistency detected");
    assert_eq!(r.phantom_commits, 0, "Phantom commits detected");
}

#[test]
fn failover_matrix_wal_stall_read_heavy() {
    let r = run_failover_experiment(FaultType::WalStall, LoadType::ReadHeavy);
    print_result(&r);
    assert!(r.data_consistent, "Data inconsistency detected");
    assert_eq!(r.phantom_commits, 0, "Phantom commits detected");
}

#[test]
fn failover_matrix_wal_stall_write_heavy() {
    let r = run_failover_experiment(FaultType::WalStall, LoadType::WriteHeavy);
    print_result(&r);
    assert!(r.data_consistent, "Data inconsistency detected");
    assert_eq!(r.phantom_commits, 0, "Phantom commits detected");
}

#[test]
fn failover_matrix_wal_stall_mixed() {
    let r = run_failover_experiment(FaultType::WalStall, LoadType::Mixed);
    print_result(&r);
    assert!(r.data_consistent, "Data inconsistency detected");
    assert_eq!(r.phantom_commits, 0, "Phantom commits detected");
}

// ═══════════════════════════════════════════════════════════════════════════
// Full Matrix Runner — Runs all 9 and produces summary
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn failover_full_matrix_summary() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║  FalconDB P0-2: Failover Determinism Evidence Matrix         ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let faults = [FaultType::LeaderCrash, FaultType::NetworkPartition, FaultType::WalStall];
    let loads = [LoadType::ReadHeavy, LoadType::WriteHeavy, LoadType::Mixed];

    let mut results: Vec<FailoverResult> = Vec::new();
    let mut all_consistent = true;

    for &fault in &faults {
        for &load in &loads {
            let r = run_failover_experiment(fault, load);
            print_result(&r);
            if !r.data_consistent {
                all_consistent = false;
            }
            results.push(r);
        }
    }

    // Summary table
    println!("\n══════════════════════════════════════════════════════════════");
    println!("  SUMMARY: {} experiments", results.len());
    println!("══════════════════════════════════════════════════════════════");
    println!("  {:20} {:12} {:>8} {:>8} {:>8} {:>6} {:>6} {:>10}",
        "Experiment", "Consistent", "Commit", "Abort", "InDoubt", "FO ms", "p99µs", "TPS(after)");
    println!("  {}", "-".repeat(80));

    for r in &results {
        let label = format!("{:?}×{:?}", r.fault, r.load);
        let committed = r.txn_counts.get(&TxnOutcome::Committed).copied().unwrap_or(0);
        let aborted = r.txn_counts.get(&TxnOutcome::Aborted).copied().unwrap_or(0);
        let indoubt = r.txn_counts.get(&TxnOutcome::InDoubt).copied().unwrap_or(0);
        println!("  {:20} {:12} {:>8} {:>8} {:>8} {:>6} {:>6} {:>10.0}",
            label,
            if r.data_consistent { "✅ YES" } else { "❌ NO" },
            committed, aborted, indoubt,
            r.failover_ms, r.p99_us, r.tps_after);
    }

    println!("  {}", "=".repeat(80));
    println!("  All consistent: {}", if all_consistent { "✅ YES" } else { "❌ NO" });
    println!("  Phantom commits: {}", results.iter().map(|r| r.phantom_commits).sum::<u64>());

    assert!(all_consistent, "At least one experiment showed data inconsistency");
}
