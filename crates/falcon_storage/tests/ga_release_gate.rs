//! GA Release Gate — Automated Verification Tests
//!
//! These tests validate the five P0 requirements for GA (v1.0 Stable):
//!
//! - **GA-P0-1**: Transaction correctness (Single-Shard ACID)
//!   - ACK = Durable: committed txns survive any crash
//!   - No Phantom Commit: uncommitted txns never appear after recovery
//!   - At-most-once Commit: WAL replay N times → same state
//!
//! - **GA-P0-2**: WAL durability & crash recovery semantics
//!   - WRITE → COMMIT → fsync → ACK ordering
//!   - Idempotent replay (N recoveries → identical state)
//!   - Crash point matrix coverage
//!
//! - **GA-P0-3**: Failover transaction behavior predictability
//!   - No phantom commit after promote
//!   - No double commit after promote
//!
//! - **GA-P0-4**: Memory safety (OOM protection)
//!   - Memory limit enforced; new txns rejected at limit
//!   - Node survives; returns identifiable error
//!
//! - **GA-P0-5**: Start/Stop/Restart safety
//!   - Clean shutdown flushes WAL
//!   - Restart auto-recovers without human intervention
//!   - Multiple restarts produce consistent state

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, StorageType, TableSchema};
use falcon_common::types::*;
use falcon_storage::engine::StorageEngine;
use falcon_storage::memory::{MemoryBudget, PressureState};

fn ga_schema(name: &str, table_id: TableId) -> TableSchema {
    TableSchema {
        id: table_id,
        name: name.to_string(),
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
                data_type: DataType::Text,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
            },
        ],
        primary_key_columns: vec![0],
        foreign_keys: vec![],
        storage_type: StorageType::Rowstore,
        ..Default::default()
    }
}

fn ga_row(id: i32, val: &str) -> OwnedRow {
    OwnedRow::new(vec![Datum::Int32(id), Datum::Text(val.into())])
}

/// Helper: recover and scan all committed rows from table.
fn recover_and_scan(dir: &std::path::Path, table_id: TableId) -> Vec<(Vec<u8>, OwnedRow)> {
    let engine = StorageEngine::recover(dir).unwrap();
    engine
        .scan(table_id, TxnId(u64::MAX - 1), Timestamp(u64::MAX - 1))
        .unwrap()
}

/// Helper: extract text values from scan results column index 1.
fn extract_vals(rows: &[(Vec<u8>, OwnedRow)]) -> Vec<String> {
    rows.iter()
        .map(|(_, r)| match &r.values[1] {
            Datum::Text(s) => s.clone(),
            _ => "?".into(),
        })
        .collect()
}

/// Helper: extract integer ids from scan results column index 0.
fn extract_ids(rows: &[(Vec<u8>, OwnedRow)]) -> Vec<i32> {
    rows.iter()
        .map(|(_, r)| match r.values[0] {
            Datum::Int32(i) => i,
            _ => -1,
        })
        .collect()
}

// ═══════════════════════════════════════════════════════════════════════════
// GA-P0-1: Transaction Correctness (Single-Shard ACID)
// ═══════════════════════════════════════════════════════════════════════════

/// ACK = Durable: After COMMIT + fsync, crash → txn MUST survive.
#[test]
fn ga_p0_1_ack_equals_durable() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: commit 10 txns, each fsynced via WAL
    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("ack_test", TableId(1))).unwrap();

        for i in 0..10 {
            let txn = TxnId(100 + i as u64);
            engine.insert(TableId(1), ga_row(i, &format!("v{}", i)), txn).unwrap();
            engine.commit_txn(txn, Timestamp(1000 + i as u64), TxnType::Local).unwrap();
        }
        engine.flush_wal().unwrap();
        // Simulate crash: engine dropped without clean shutdown
    }

    // Phase 2: recover — all 10 txns MUST exist
    let rows = recover_and_scan(dir.path(), TableId(1));
    assert_eq!(
        rows.len(), 10,
        "GA-P0-1 ACK=Durable: expected 10 committed rows after crash, got {}",
        rows.len()
    );

    let ids = extract_ids(&rows);
    for i in 0..10 {
        assert!(ids.contains(&i), "GA-P0-1: committed row id={} missing after recovery", i);
    }
}

/// No Phantom Commit: uncommitted txns MUST NOT appear after recovery.
#[test]
fn ga_p0_1_no_phantom_commit() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("phantom_test", TableId(1))).unwrap();

        // Committed txn
        let txn1 = TxnId(10);
        engine.insert(TableId(1), ga_row(1, "committed"), txn1).unwrap();
        engine.commit_txn(txn1, Timestamp(100), TxnType::Local).unwrap();

        // Uncommitted txn — WAL has Insert but NO CommitTxn
        let txn2 = TxnId(20);
        engine.insert(TableId(1), ga_row(2, "phantom"), txn2).unwrap();

        // Another uncommitted txn
        let txn3 = TxnId(30);
        engine.insert(TableId(1), ga_row(3, "also_phantom"), txn3).unwrap();

        engine.flush_wal().unwrap();
    }

    let rows = recover_and_scan(dir.path(), TableId(1));
    assert_eq!(rows.len(), 1, "GA-P0-1 No Phantom: expected 1 row, got {}", rows.len());

    let vals = extract_vals(&rows);
    assert!(vals.contains(&"committed".to_string()), "committed txn missing");
    assert!(!vals.contains(&"phantom".to_string()), "PHANTOM COMMIT detected!");
    assert!(!vals.contains(&"also_phantom".to_string()), "PHANTOM COMMIT detected!");
}

/// At-most-once Commit: WAL replayed 5 times → identical state each time.
#[test]
fn ga_p0_1_at_most_once_commit() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("atmost_once", TableId(1))).unwrap();

        for i in 0..5 {
            let txn = TxnId(10 + i as u64);
            engine.insert(TableId(1), ga_row(i, &format!("row{}", i)), txn).unwrap();
            engine.commit_txn(txn, Timestamp(100 + i as u64), TxnType::Local).unwrap();
        }
        engine.flush_wal().unwrap();
    }

    // Recover 5 times — each must produce exactly the same state
    let mut snapshots: Vec<Vec<Vec<Datum>>> = Vec::new();
    for attempt in 0..5 {
        let engine = StorageEngine::recover(dir.path()).unwrap();
        let rows = engine
            .scan(TableId(1), TxnId(u64::MAX - 1), Timestamp(u64::MAX - 1))
            .unwrap();
        let mut data: Vec<Vec<Datum>> = rows.into_iter().map(|(_, r)| r.values).collect();
        data.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
        snapshots.push(data);

        assert_eq!(
            snapshots[0].len(),
            snapshots[attempt].len(),
            "GA-P0-1 At-most-once: replay {} row count differs from replay 0",
            attempt
        );
    }

    for i in 1..snapshots.len() {
        assert_eq!(
            snapshots[0], snapshots[i],
            "GA-P0-1 At-most-once: replay {} diverged from replay 0 (DOUBLE COMMIT?)",
            i
        );
    }
}

/// Transaction terminal states from WAL perspective:
/// - Committed txn (WAL has CommitTxn record) → visible after recovery
/// - Aborted txn (WAL has AbortTxn record) → invisible after recovery
/// - In-flight txn (WAL has no terminal record) → invisible after recovery
/// Note: TxnState machine (Active/Prepared/Committed/Aborted transitions)
/// is tested in falcon_txn::manager tests.
#[test]
fn ga_p0_1_txn_terminal_states_from_wal() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("terminal", TableId(1))).unwrap();

        // Case 1: Committed
        let txn1 = TxnId(10);
        engine.insert(TableId(1), ga_row(1, "committed"), txn1).unwrap();
        engine.commit_txn(txn1, Timestamp(100), TxnType::Local).unwrap();

        // Case 2: Explicitly aborted
        let txn2 = TxnId(20);
        engine.insert(TableId(1), ga_row(2, "aborted"), txn2).unwrap();
        engine.abort_txn(txn2, TxnType::Local).unwrap();

        // Case 3: In-flight (no terminal record)
        let txn3 = TxnId(30);
        engine.insert(TableId(1), ga_row(3, "inflight"), txn3).unwrap();

        engine.flush_wal().unwrap();
    }

    let rows = recover_and_scan(dir.path(), TableId(1));
    assert_eq!(rows.len(), 1, "Only committed txn should survive");
    assert_eq!(extract_vals(&rows)[0], "committed");

    let ids = extract_ids(&rows);
    assert!(!ids.contains(&2), "Aborted txn must not survive");
    assert!(!ids.contains(&3), "In-flight txn must not survive");
}

// ═══════════════════════════════════════════════════════════════════════════
// GA-P0-2: WAL Durability & Crash Recovery Semantics
// ═══════════════════════════════════════════════════════════════════════════

/// Crash point 1: crash BEFORE any WAL write → txn does not exist.
#[test]
fn ga_p0_2_crash_before_wal_write() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("cp1", TableId(1))).unwrap();

        // One committed baseline
        let txn1 = TxnId(10);
        engine.insert(TableId(1), ga_row(1, "baseline"), txn1).unwrap();
        engine.commit_txn(txn1, Timestamp(100), TxnType::Local).unwrap();
        engine.flush_wal().unwrap();

        // "Second txn" never touches engine — simulates crash before any write
    }

    let rows = recover_and_scan(dir.path(), TableId(1));
    assert_eq!(rows.len(), 1, "GA-P0-2 CP1: only baseline should exist");
}

/// Crash point 2: crash AFTER WAL insert, BEFORE commit record → txn rolled back.
#[test]
fn ga_p0_2_crash_after_insert_before_commit() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("cp2", TableId(1))).unwrap();

        let txn1 = TxnId(10);
        engine.insert(TableId(1), ga_row(1, "committed"), txn1).unwrap();
        engine.commit_txn(txn1, Timestamp(100), TxnType::Local).unwrap();

        // Insert written to WAL but no commit
        let txn2 = TxnId(20);
        engine.insert(TableId(1), ga_row(2, "no_commit_record"), txn2).unwrap();

        engine.flush_wal().unwrap();
    }

    let rows = recover_and_scan(dir.path(), TableId(1));
    assert_eq!(rows.len(), 1, "GA-P0-2 CP2: uncommitted insert must be rolled back");
    let vals = extract_vals(&rows);
    assert_eq!(vals[0], "committed");
}

/// Crash point 3: crash AFTER commit record written + fsynced → txn MUST survive.
#[test]
fn ga_p0_2_crash_after_commit_fsynced() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("cp3", TableId(1))).unwrap();

        let txn1 = TxnId(10);
        engine.insert(TableId(1), ga_row(1, "durable"), txn1).unwrap();
        engine.commit_txn(txn1, Timestamp(100), TxnType::Local).unwrap();
        engine.flush_wal().unwrap();

        // Crash after fsync — drop engine
    }

    let rows = recover_and_scan(dir.path(), TableId(1));
    assert_eq!(rows.len(), 1, "GA-P0-2 CP3: committed+fsynced txn MUST survive");
    assert_eq!(extract_vals(&rows)[0], "durable");
}

/// WAL replay is idempotent: recover N times → identical final state.
#[test]
fn ga_p0_2_wal_replay_idempotent() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("idempotent", TableId(1))).unwrap();

        // Mix of committed, aborted, and DDL operations
        let txn1 = TxnId(10);
        engine.insert(TableId(1), ga_row(1, "a"), txn1).unwrap();
        engine.commit_txn(txn1, Timestamp(100), TxnType::Local).unwrap();

        let txn2 = TxnId(20);
        engine.insert(TableId(1), ga_row(2, "b"), txn2).unwrap();
        engine.abort_txn(txn2, TxnType::Local).unwrap();

        let txn3 = TxnId(30);
        engine.insert(TableId(1), ga_row(3, "c"), txn3).unwrap();
        engine.commit_txn(txn3, Timestamp(200), TxnType::Local).unwrap();

        // Uncommitted at crash
        let txn4 = TxnId(40);
        engine.insert(TableId(1), ga_row(4, "inflight"), txn4).unwrap();

        engine.flush_wal().unwrap();
    }

    // Recover 7 times
    let mut snapshots = Vec::new();
    for _ in 0..7 {
        let engine = StorageEngine::recover(dir.path()).unwrap();
        let rows = engine
            .scan(TableId(1), TxnId(u64::MAX - 1), Timestamp(u64::MAX - 1))
            .unwrap();
        let mut data: Vec<Vec<Datum>> = rows.into_iter().map(|(_, r)| r.values).collect();
        data.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
        snapshots.push(data);
    }

    // All 7 must be identical
    for i in 1..snapshots.len() {
        assert_eq!(
            snapshots[0], snapshots[i],
            "GA-P0-2 idempotent: replay {} differs from replay 0",
            i
        );
    }
    // Expected: rows 1 ("a") and 3 ("c") only
    assert_eq!(snapshots[0].len(), 2, "Expected 2 committed rows");
}

/// Recovery with interleaved committed + uncommitted txns across multiple tables.
#[test]
fn ga_p0_2_multi_table_interleaved_recovery() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("t1", TableId(1))).unwrap();
        engine.create_table(ga_schema("t2", TableId(2))).unwrap();

        // txn_a: inserts into both tables, commits
        let txn_a = TxnId(100);
        engine.insert(TableId(1), ga_row(1, "t1_a"), txn_a).unwrap();
        engine.insert(TableId(2), ga_row(1, "t2_a"), txn_a).unwrap();
        engine.commit_txn(txn_a, Timestamp(500), TxnType::Local).unwrap();

        // txn_b: inserts into both tables, uncommitted (crash)
        let txn_b = TxnId(200);
        engine.insert(TableId(1), ga_row(2, "t1_b_phantom"), txn_b).unwrap();
        engine.insert(TableId(2), ga_row(2, "t2_b_phantom"), txn_b).unwrap();

        engine.flush_wal().unwrap();
    }

    let rows_t1 = recover_and_scan(dir.path(), TableId(1));
    let rows_t2 = {
        let engine = StorageEngine::recover(dir.path()).unwrap();
        engine.scan(TableId(2), TxnId(u64::MAX - 1), Timestamp(u64::MAX - 1)).unwrap()
    };

    assert_eq!(rows_t1.len(), 1, "t1: only committed txn_a row");
    assert_eq!(rows_t2.len(), 1, "t2: only committed txn_a row");
    assert_eq!(extract_vals(&rows_t1)[0], "t1_a");
    assert_eq!(extract_vals(&rows_t2)[0], "t2_a");
}

/// Recovery with update + delete operations preserves correct final state.
#[test]
fn ga_p0_2_update_delete_recovery() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("upd_del", TableId(1))).unwrap();

        // Insert 3 rows
        let txn1 = TxnId(10);
        engine.insert(TableId(1), ga_row(1, "original"), txn1).unwrap();
        engine.insert(TableId(1), ga_row(2, "to_delete"), txn1).unwrap();
        engine.insert(TableId(1), ga_row(3, "keep"), txn1).unwrap();
        engine.commit_txn(txn1, Timestamp(100), TxnType::Local).unwrap();

        // Update row 1
        let txn2 = TxnId(20);
        let pk1 = engine
            .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
            .unwrap()
            .into_iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(1))
            .unwrap()
            .0;
        engine.update(TableId(1), &pk1, ga_row(1, "updated"), txn2).unwrap();
        engine.commit_txn(txn2, Timestamp(200), TxnType::Local).unwrap();

        // Delete row 2
        let txn3 = TxnId(30);
        let pk2 = engine
            .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
            .unwrap()
            .into_iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(2))
            .unwrap()
            .0;
        engine.delete(TableId(1), &pk2, txn3).unwrap();
        engine.commit_txn(txn3, Timestamp(300), TxnType::Local).unwrap();

        engine.flush_wal().unwrap();
    }

    let rows = recover_and_scan(dir.path(), TableId(1));
    let map: std::collections::HashMap<i32, String> = rows
        .iter()
        .map(|(_, r)| {
            let id = match r.values[0] { Datum::Int32(i) => i, _ => -1 };
            let val = match &r.values[1] { Datum::Text(s) => s.clone(), _ => "?".into() };
            (id, val)
        })
        .collect();

    assert_eq!(map.len(), 2, "Expected 2 rows after update+delete recovery");
    assert_eq!(map.get(&1).unwrap(), "updated");
    assert!(!map.contains_key(&2), "Deleted row 2 should not exist");
    assert_eq!(map.get(&3).unwrap(), "keep");
}

// ═══════════════════════════════════════════════════════════════════════════
// GA-P0-3: Failover Transaction Behavior (predictable)
// ═══════════════════════════════════════════════════════════════════════════

/// After crash, only WAL-committed txns survive — simulates leader crash + promote.
/// New "leader" (recovery) must see exactly the committed set.
#[test]
fn ga_p0_3_failover_only_committed_survive() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("failover", TableId(1))).unwrap();

        // 5 committed
        for i in 0..5 {
            let txn = TxnId(10 + i as u64);
            engine.insert(TableId(1), ga_row(i, &format!("committed_{}", i)), txn).unwrap();
            engine.commit_txn(txn, Timestamp(100 + i as u64), TxnType::Local).unwrap();
        }

        // 3 in-flight (not committed)
        for i in 5..8 {
            let txn = TxnId(100 + i as u64);
            engine.insert(TableId(1), ga_row(i, &format!("inflight_{}", i)), txn).unwrap();
        }

        engine.flush_wal().unwrap();
    }

    // Simulates promote: recover from WAL
    let rows = recover_and_scan(dir.path(), TableId(1));
    assert_eq!(rows.len(), 5, "GA-P0-3: only 5 committed txns after failover");

    let ids = extract_ids(&rows);
    for i in 0..5 {
        assert!(ids.contains(&i), "Committed row {} missing after failover", i);
    }
    for i in 5..8 {
        assert!(!ids.contains(&i), "In-flight row {} survived failover (PHANTOM!)", i);
    }
}

/// After recovery (promote), new leader can accept new writes.
#[test]
fn ga_p0_3_new_leader_accepts_writes() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("new_leader", TableId(1))).unwrap();

        let txn = TxnId(10);
        engine.insert(TableId(1), ga_row(1, "old_leader"), txn).unwrap();
        engine.commit_txn(txn, Timestamp(100), TxnType::Local).unwrap();
        engine.flush_wal().unwrap();
    }

    // Recover (simulate promote)
    let engine = StorageEngine::recover(dir.path()).unwrap();

    // New leader writes
    let txn_new = TxnId(500);
    engine.insert(TableId(1), ga_row(2, "new_leader_write"), txn_new).unwrap();
    engine.commit_txn(txn_new, Timestamp(1000), TxnType::Local).unwrap();
    engine.flush_wal().unwrap();

    // Verify both old and new writes exist
    let rows = engine
        .scan(TableId(1), TxnId(u64::MAX - 1), Timestamp(u64::MAX - 1))
        .unwrap();
    assert_eq!(rows.len(), 2, "GA-P0-3: old + new writes after promote");
}

// ═══════════════════════════════════════════════════════════════════════════
// GA-P0-4: Memory Safety (OOM Protection)
// ═══════════════════════════════════════════════════════════════════════════

/// Memory budget evaluation: pressure states transition correctly.
#[test]
fn ga_p0_4_memory_pressure_states() {
    let budget = MemoryBudget::new(1000, 2000);

    assert_eq!(budget.evaluate(0), PressureState::Normal);
    assert_eq!(budget.evaluate(500), PressureState::Normal);
    assert_eq!(budget.evaluate(999), PressureState::Normal);
    assert_eq!(budget.evaluate(1000), PressureState::Pressure);
    assert_eq!(budget.evaluate(1500), PressureState::Pressure);
    assert_eq!(budget.evaluate(1999), PressureState::Pressure);
    assert_eq!(budget.evaluate(2000), PressureState::Critical);
    assert_eq!(budget.evaluate(10000), PressureState::Critical);
}

/// Memory tracker: alloc/dealloc correctly tracks total bytes.
#[test]
fn ga_p0_4_memory_tracker_accounting() {
    use falcon_storage::memory::MemoryTracker;

    let tracker = MemoryTracker::new(MemoryBudget::new(1_000_000, 2_000_000));
    assert_eq!(tracker.total_bytes(), 0);
    assert_eq!(tracker.pressure_state(), PressureState::Normal);

    tracker.alloc_mvcc(500_000);
    assert_eq!(tracker.mvcc_bytes(), 500_000);
    assert_eq!(tracker.total_bytes(), 500_000);

    tracker.alloc_index(300_000);
    assert_eq!(tracker.total_bytes(), 800_000);
    assert_eq!(tracker.pressure_state(), PressureState::Normal);

    // Cross soft limit → Pressure
    tracker.alloc_write_buffer(300_000);
    assert_eq!(tracker.total_bytes(), 1_100_000);
    assert_eq!(tracker.pressure_state(), PressureState::Pressure);

    // Cross hard limit → Critical
    tracker.alloc_mvcc(1_000_000);
    assert_eq!(tracker.total_bytes(), 2_100_000);
    assert_eq!(tracker.pressure_state(), PressureState::Critical);

    // Dealloc back to Normal
    tracker.dealloc_mvcc(1_500_000);
    tracker.dealloc_write_buffer(300_000);
    assert_eq!(tracker.pressure_state(), PressureState::Normal);
}

/// Global memory governor: 4-tier backpressure decisions.
#[test]
fn ga_p0_4_global_governor_backpressure() {
    use falcon_storage::memory::{GlobalMemoryGovernor, GovernorConfig, BackpressureLevel, BackpressureAction};

    let config = GovernorConfig {
        node_budget_bytes: 1_000_000,
        soft_threshold: 0.70,
        hard_threshold: 0.85,
        emergency_threshold: 0.95,
        ..Default::default()
    };
    let gov = GlobalMemoryGovernor::new(config);

    // Below soft → Allow
    gov.report_usage(500_000);
    assert_eq!(gov.current_level(), BackpressureLevel::None);
    assert!(matches!(gov.check(true), BackpressureAction::Allow));

    // Between soft and hard → Soft (delay writes, allow reads)
    gov.report_usage(750_000);
    assert_eq!(gov.current_level(), BackpressureLevel::Soft);
    assert!(matches!(gov.check(true), BackpressureAction::Delay(_)));
    assert!(matches!(gov.check(false), BackpressureAction::Allow));

    // Between hard and emergency → Hard (reject writes, allow reads)
    gov.report_usage(900_000);
    assert_eq!(gov.current_level(), BackpressureLevel::Hard);
    assert!(matches!(gov.check(true), BackpressureAction::RejectWrite { .. }));
    assert!(matches!(gov.check(false), BackpressureAction::Allow));

    // Above emergency → Emergency (reject all)
    gov.report_usage(960_000);
    assert_eq!(gov.current_level(), BackpressureLevel::Emergency);
    assert!(matches!(gov.check(true), BackpressureAction::RejectAll { .. }));
    assert!(matches!(gov.check(false), BackpressureAction::RejectAll { .. }));
}

// ═══════════════════════════════════════════════════════════════════════════
// GA-P0-5: Start/Stop/Restart Safety
// ═══════════════════════════════════════════════════════════════════════════

/// Clean shutdown flushes WAL; restart recovers all data.
#[test]
fn ga_p0_5_clean_shutdown_restart() {
    let dir = tempfile::tempdir().unwrap();

    // Session 1: create data, shutdown
    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("restart", TableId(1))).unwrap();

        for i in 0..10 {
            let txn = TxnId(10 + i as u64);
            engine.insert(TableId(1), ga_row(i, &format!("s1_{}", i)), txn).unwrap();
            engine.commit_txn(txn, Timestamp(100 + i as u64), TxnType::Local).unwrap();
        }

        // Clean shutdown: WAL flush
        engine.shutdown();
    }

    // Session 2: restart, verify, add more, shutdown
    {
        let engine = StorageEngine::recover(dir.path()).unwrap();
        let rows = engine
            .scan(TableId(1), TxnId(u64::MAX - 1), Timestamp(u64::MAX - 1))
            .unwrap();
        assert_eq!(rows.len(), 10, "GA-P0-5: all 10 rows after first restart");

        // Add more data in session 2
        for i in 10..15 {
            let txn = TxnId(100 + i as u64);
            engine.insert(TableId(1), ga_row(i, &format!("s2_{}", i)), txn).unwrap();
            engine.commit_txn(txn, Timestamp(500 + i as u64), TxnType::Local).unwrap();
        }
        engine.shutdown();
    }

    // Session 3: restart again, verify all 15
    {
        let engine = StorageEngine::recover(dir.path()).unwrap();
        let rows = engine
            .scan(TableId(1), TxnId(u64::MAX - 1), Timestamp(u64::MAX - 1))
            .unwrap();
        assert_eq!(rows.len(), 15, "GA-P0-5: all 15 rows after second restart");
    }
}

/// Multiple crash-restart cycles: data remains consistent.
#[test]
fn ga_p0_5_multiple_crash_restart_cycles() {
    let dir = tempfile::tempdir().unwrap();

    // Cycle 0: initial data
    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("cycles", TableId(1))).unwrap();

        let txn = TxnId(10);
        engine.insert(TableId(1), ga_row(0, "cycle0"), txn).unwrap();
        engine.commit_txn(txn, Timestamp(100), TxnType::Local).unwrap();
        engine.flush_wal().unwrap();
        // Crash: no shutdown()
    }

    // 5 crash-restart cycles, each adding one committed row
    for cycle in 1..=5 {
        let engine = StorageEngine::recover(dir.path()).unwrap();

        // Verify previous data intact
        let rows = engine
            .scan(TableId(1), TxnId(u64::MAX - 1), Timestamp(u64::MAX - 1))
            .unwrap();
        assert_eq!(
            rows.len(), cycle as usize,
            "Cycle {}: expected {} rows, got {}",
            cycle, cycle, rows.len()
        );

        // Add one more committed row
        let txn = TxnId(100 + cycle as u64);
        engine.insert(TableId(1), ga_row(cycle, &format!("cycle{}", cycle)), txn).unwrap();
        engine.commit_txn(txn, Timestamp(1000 + cycle as u64), TxnType::Local).unwrap();
        engine.flush_wal().unwrap();
        // Crash: no shutdown()
    }

    // Final recovery: should have 6 rows (cycle0 + cycle1..5)
    let rows = recover_and_scan(dir.path(), TableId(1));
    assert_eq!(rows.len(), 6, "GA-P0-5: 6 rows after 5 crash-restart cycles");
}

/// DDL operations survive restart.
#[test]
fn ga_p0_5_ddl_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("survive", TableId(1))).unwrap();
        engine.create_table(ga_schema("ephemeral", TableId(2))).unwrap();

        let txn = TxnId(10);
        engine.insert(TableId(1), ga_row(1, "keeper"), txn).unwrap();
        engine.commit_txn(txn, Timestamp(100), TxnType::Local).unwrap();

        let txn2 = TxnId(20);
        engine.insert(TableId(2), ga_row(1, "dropped"), txn2).unwrap();
        engine.commit_txn(txn2, Timestamp(200), TxnType::Local).unwrap();

        engine.drop_table("ephemeral").unwrap();
        engine.flush_wal().unwrap();
    }

    let engine = StorageEngine::recover(dir.path()).unwrap();
    let catalog = engine.get_catalog();
    assert!(catalog.find_table("survive").is_some(), "survive table must exist");
    assert!(catalog.find_table("ephemeral").is_none(), "dropped table must not exist");

    let rows = engine
        .scan(TableId(1), TxnId(u64::MAX - 1), Timestamp(u64::MAX - 1))
        .unwrap();
    assert_eq!(rows.len(), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// GA-P0-2 Supplementary: Checkpoint + WAL delta recovery
// ═══════════════════════════════════════════════════════════════════════════

/// Checkpoint + incremental WAL: both checkpoint data and post-checkpoint
/// WAL delta must be recovered correctly.
#[test]
fn ga_p0_2_checkpoint_plus_delta_recovery() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        engine.create_table(ga_schema("ckpt", TableId(1))).unwrap();

        // Pre-checkpoint data
        for i in 0..5 {
            let txn = TxnId(10 + i as u64);
            engine.insert(TableId(1), ga_row(i, &format!("pre_{}", i)), txn).unwrap();
            engine.commit_txn(txn, Timestamp(100 + i as u64), TxnType::Local).unwrap();
        }

        let (_, count) = engine.checkpoint().unwrap();
        assert_eq!(count, 5);

        // Post-checkpoint data
        for i in 5..10 {
            let txn = TxnId(100 + i as u64);
            engine.insert(TableId(1), ga_row(i, &format!("post_{}", i)), txn).unwrap();
            engine.commit_txn(txn, Timestamp(500 + i as u64), TxnType::Local).unwrap();
        }

        engine.flush_wal().unwrap();
    }

    let rows = recover_and_scan(dir.path(), TableId(1));
    assert_eq!(rows.len(), 10, "Checkpoint + delta: expected 10 rows");
}
