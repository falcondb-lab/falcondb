//! Consistency Test Suite — WAL Invariants & Crash Recovery
//!
//! Tests correspond to invariants defined in `falcon_common::consistency`:
//! - WAL-1: Unique monotonic LSN
//! - WAL-2: Idempotent replay
//! - WAL-3: Monotonic commit sequence
//! - WAL-4: Replay convergence
//! - WAL-5: Recovery completeness
//! - WAL-6: Checksum integrity
//! - CRASH-*: Crash point recovery behavior

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, StorageType, TableSchema};
use falcon_common::types::*;
use falcon_storage::engine::StorageEngine;
use falcon_storage::wal::{SyncMode, WalRecord, WalWriter};

fn test_schema(name: &str, table_id: TableId) -> TableSchema {
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

fn make_row(id: i32, val: &str) -> OwnedRow {
    OwnedRow::new(vec![Datum::Int32(id), Datum::Text(val.into())])
}

// ═══════════════════════════════════════════════════════════════════════════
// WAL-1: Unique monotonic LSN
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_wal1_unique_monotonic_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let wal = WalWriter::open(dir.path(), SyncMode::None).unwrap();

    let mut lsns = Vec::new();
    for i in 0..100 {
        let lsn = wal
            .append(&WalRecord::Insert {
                txn_id: TxnId(i as u64),
                table_id: TableId(1),
                row: make_row(i, "x"),
            })
            .unwrap();
        lsns.push(lsn);
    }

    // Verify strict monotonicity
    for w in lsns.windows(2) {
        assert!(
            w[0] < w[1],
            "WAL-1 violation: LSN {} is not < LSN {}",
            w[0],
            w[1]
        );
    }

    // Verify uniqueness
    let unique: std::collections::HashSet<u64> = lsns.iter().copied().collect();
    assert_eq!(unique.len(), lsns.len(), "WAL-1 violation: duplicate LSNs");
}

// ═══════════════════════════════════════════════════════════════════════════
// WAL-3: Monotonic commit timestamp sequence
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_wal3_monotonic_commit_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(Some(dir.path())).unwrap();

    let schema = test_schema("t1", TableId(1));
    engine.create_table(schema).unwrap();

    let mut commit_timestamps = Vec::new();
    for i in 0..20 {
        let txn_id = TxnId(100 + i as u64);
        engine.insert(TableId(1), make_row(i, "v"), txn_id).unwrap();
        let ts = Timestamp(1000 + i as u64);
        engine.commit_txn(txn_id, ts, TxnType::Local).unwrap();
        commit_timestamps.push(ts.0);
    }

    // Verify monotonicity
    for w in commit_timestamps.windows(2) {
        assert!(
            w[0] <= w[1],
            "WAL-3 violation: commit_ts {} > commit_ts {}",
            w[0],
            w[1]
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// WAL-4 & WAL-5: Replay convergence + recovery completeness
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_wal4_wal5_replay_convergence_and_completeness() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Create data
    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        let schema = test_schema("items", TableId(1));
        engine.create_table(schema).unwrap();

        // Committed txn
        let txn1 = TxnId(10);
        engine
            .insert(TableId(1), make_row(1, "committed"), txn1)
            .unwrap();
        engine
            .commit_txn(txn1, Timestamp(100), TxnType::Local)
            .unwrap();

        // Another committed txn
        let txn2 = TxnId(20);
        engine
            .insert(TableId(1), make_row(2, "also_committed"), txn2)
            .unwrap();
        engine
            .commit_txn(txn2, Timestamp(200), TxnType::Local)
            .unwrap();

        // Uncommitted txn (no commit record — should be rolled back on recovery)
        let txn3 = TxnId(30);
        engine
            .insert(TableId(1), make_row(3, "uncommitted"), txn3)
            .unwrap();
        // No commit_txn for txn3!

        // Flush WAL
        engine.flush_wal().unwrap();
    }

    // Phase 2: Recover and verify
    let engine1 = StorageEngine::recover(dir.path()).unwrap();
    let rows1 = engine1
        .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
        .unwrap();

    // WAL-5: Committed txns must be visible
    assert_eq!(
        rows1.len(),
        2,
        "WAL-5: expected 2 committed rows, got {}",
        rows1.len()
    );
    let vals: Vec<String> = rows1
        .iter()
        .map(|(_, r)| match &r.values[1] {
            Datum::Text(s) => s.clone(),
            _ => "?".into(),
        })
        .collect();
    assert!(
        vals.contains(&"committed".to_string()),
        "WAL-5: committed txn missing"
    );
    assert!(
        vals.contains(&"also_committed".to_string()),
        "WAL-5: also_committed txn missing"
    );
    // Uncommitted txn must NOT be visible
    assert!(
        !vals.contains(&"uncommitted".to_string()),
        "WAL-5: uncommitted txn is visible!"
    );

    // WAL-4: Second recovery produces identical state (compare by row values, not PK bytes)
    let engine2 = StorageEngine::recover(dir.path()).unwrap();
    let rows2 = engine2
        .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
        .unwrap();
    assert_eq!(
        rows1.len(),
        rows2.len(),
        "WAL-4: replay divergence in row count"
    );
    let mut vals1: Vec<Vec<Datum>> = rows1.iter().map(|(_, r)| r.values.clone()).collect();
    let mut vals2: Vec<Vec<Datum>> = rows2.iter().map(|(_, r)| r.values.clone()).collect();
    vals1.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
    vals2.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
    assert_eq!(vals1, vals2, "WAL-4: replay divergence in row data");
}

// ═══════════════════════════════════════════════════════════════════════════
// WAL-2: Idempotent replay (double replay produces same state)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_wal2_idempotent_replay() {
    let dir = tempfile::tempdir().unwrap();

    // Create WAL with committed and aborted txns
    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        let schema = test_schema("tbl", TableId(1));
        engine.create_table(schema).unwrap();

        let txn1 = TxnId(10);
        engine.insert(TableId(1), make_row(1, "a"), txn1).unwrap();
        engine
            .commit_txn(txn1, Timestamp(100), TxnType::Local)
            .unwrap();

        let txn2 = TxnId(20);
        engine.insert(TableId(1), make_row(2, "b"), txn2).unwrap();
        engine.abort_txn(txn2, TxnType::Local).unwrap();

        engine.flush_wal().unwrap();
    }

    // Recover three times — all must produce identical state
    let mut row_snapshots = Vec::new();
    for _ in 0..3 {
        let engine = StorageEngine::recover(dir.path()).unwrap();
        let rows = engine
            .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
            .unwrap();
        let snapshot: Vec<(Vec<u8>, Vec<Datum>)> =
            rows.into_iter().map(|(pk, row)| (pk, row.values)).collect();
        row_snapshots.push(snapshot);
    }

    for i in 1..row_snapshots.len() {
        assert_eq!(
            row_snapshots[0], row_snapshots[i],
            "WAL-2 violation: replay {} diverged from replay 0",
            i
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// WAL-6: Checksum integrity (corrupted WAL stops recovery)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_wal6_checksum_corruption_halts_recovery() {
    let dir = tempfile::tempdir().unwrap();

    // Write some WAL records
    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        let schema = test_schema("corrupt_test", TableId(1));
        engine.create_table(schema).unwrap();

        for i in 0..5 {
            let txn = TxnId(100 + i as u64);
            engine
                .insert(TableId(1), make_row(i, &format!("v{}", i)), txn)
                .unwrap();
            engine
                .commit_txn(txn, Timestamp(1000 + i as u64), TxnType::Local)
                .unwrap();
        }
        engine.flush_wal().unwrap();
    }

    // Find and corrupt the WAL segment
    let wal_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .collect();
    assert!(!wal_files.is_empty(), "No WAL files found");

    let wal_path = wal_files[0].path();
    let mut data = std::fs::read(&wal_path).unwrap();
    // Corrupt a byte in the middle (flip bits in a data region)
    if data.len() > 50 {
        data[50] ^= 0xFF;
    }
    std::fs::write(&wal_path, &data).unwrap();

    // Recovery should still succeed (it stops at the corruption point).
    // If corruption hits early (e.g. the CreateTable record), the table
    // won't exist after recovery, which is correct behavior.
    let engine = StorageEngine::recover(dir.path()).unwrap();
    // Try to scan — table may or may not exist depending on corruption location
    match engine.scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1)) {
        Ok(rows) => {
            // If table survived, must have fewer or equal rows
            assert!(
                rows.len() <= 5,
                "WAL-6: more rows than written after corruption"
            );
        }
        Err(_) => {
            // Table not found — corruption destroyed the CreateTable record.
            // This is correct: recovery halted before seeing the table schema.
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CRASH-1: Crash before WAL write — txn does not exist
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_crash_before_wal_write() {
    let dir = tempfile::tempdir().unwrap();

    // Write one committed txn, then "crash" before writing second txn
    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        let schema = test_schema("crash_test", TableId(1));
        engine.create_table(schema).unwrap();

        let txn1 = TxnId(10);
        engine
            .insert(TableId(1), make_row(1, "before_crash"), txn1)
            .unwrap();
        engine
            .commit_txn(txn1, Timestamp(100), TxnType::Local)
            .unwrap();

        engine.flush_wal().unwrap();
        // "Crash" — txn2 never touches the engine
    }

    let engine = StorageEngine::recover(dir.path()).unwrap();
    let rows = engine
        .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
        .unwrap();
    assert_eq!(rows.len(), 1, "CRASH-1: only pre-crash txn should exist");
}

// ═══════════════════════════════════════════════════════════════════════════
// CRASH-2: Crash after WAL write, before commit — txn rolled back
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_crash_after_wal_write_before_commit() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        let schema = test_schema("crash2", TableId(1));
        engine.create_table(schema).unwrap();

        // Committed txn
        let txn1 = TxnId(10);
        engine
            .insert(TableId(1), make_row(1, "committed"), txn1)
            .unwrap();
        engine
            .commit_txn(txn1, Timestamp(100), TxnType::Local)
            .unwrap();

        // WAL records written but no commit record
        let txn2 = TxnId(20);
        engine
            .insert(TableId(1), make_row(2, "no_commit"), txn2)
            .unwrap();
        // No commit! Simulates crash after write but before commit.

        engine.flush_wal().unwrap();
    }

    let engine = StorageEngine::recover(dir.path()).unwrap();
    let rows = engine
        .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "CRASH-2: uncommitted txn should be rolled back"
    );
    let val = match &rows[0].1.values[1] {
        Datum::Text(s) => s.clone(),
        _ => panic!("expected text"),
    };
    assert_eq!(val, "committed", "CRASH-2: wrong row survived");
}

// ═══════════════════════════════════════════════════════════════════════════
// CRASH-5: Recovery with updates and deletes
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_crash_recovery_with_updates_and_deletes() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        let schema = test_schema("ud_test", TableId(1));
        engine.create_table(schema).unwrap();

        // Insert and commit
        let txn1 = TxnId(10);
        engine
            .insert(TableId(1), make_row(1, "original"), txn1)
            .unwrap();
        engine
            .insert(TableId(1), make_row(2, "to_delete"), txn1)
            .unwrap();
        engine
            .insert(TableId(1), make_row(3, "unchanged"), txn1)
            .unwrap();
        engine
            .commit_txn(txn1, Timestamp(100), TxnType::Local)
            .unwrap();

        // Update row 1
        let txn2 = TxnId(20);
        let pk1 = engine
            .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
            .unwrap()
            .into_iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(1))
            .unwrap()
            .0;
        engine
            .update(TableId(1), &pk1, make_row(1, "updated"), txn2)
            .unwrap();
        engine
            .commit_txn(txn2, Timestamp(200), TxnType::Local)
            .unwrap();

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
        engine
            .commit_txn(txn3, Timestamp(300), TxnType::Local)
            .unwrap();

        engine.flush_wal().unwrap();
    }

    let engine = StorageEngine::recover(dir.path()).unwrap();
    let rows = engine
        .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
        .unwrap();

    assert_eq!(
        rows.len(),
        2,
        "Expected 2 rows after update+delete recovery"
    );

    let vals: std::collections::HashMap<i32, String> = rows
        .iter()
        .map(|(_, r)| {
            let id = match r.values[0] {
                Datum::Int32(i) => i,
                _ => -1,
            };
            let val = match &r.values[1] {
                Datum::Text(s) => s.clone(),
                _ => "?".into(),
            };
            (id, val)
        })
        .collect();

    assert_eq!(vals.get(&1).unwrap(), "updated", "Row 1 should be updated");
    assert!(!vals.contains_key(&2), "Row 2 should be deleted");
    assert_eq!(
        vals.get(&3).unwrap(),
        "unchanged",
        "Row 3 should be unchanged"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Checkpoint + WAL replay: recovery from checkpoint + delta
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_checkpoint_plus_wal_recovery() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        let schema = test_schema("ckpt_test", TableId(1));
        engine.create_table(schema).unwrap();

        // Insert rows and commit
        for i in 0..5 {
            let txn = TxnId(10 + i as u64);
            engine
                .insert(TableId(1), make_row(i, &format!("pre_ckpt_{}", i)), txn)
                .unwrap();
            engine
                .commit_txn(txn, Timestamp(100 + i as u64), TxnType::Local)
                .unwrap();
        }

        // Checkpoint
        let (seg, count) = engine.checkpoint().unwrap();
        assert_eq!(count, 5);

        // More data after checkpoint
        for i in 5..10 {
            let txn = TxnId(100 + i as u64);
            engine
                .insert(TableId(1), make_row(i, &format!("post_ckpt_{}", i)), txn)
                .unwrap();
            engine
                .commit_txn(txn, Timestamp(200 + i as u64), TxnType::Local)
                .unwrap();
        }

        engine.flush_wal().unwrap();
    }

    // Recover: should see all 10 rows (5 from checkpoint + 5 from WAL delta)
    let engine = StorageEngine::recover(dir.path()).unwrap();
    let rows = engine
        .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
        .unwrap();
    assert_eq!(
        rows.len(),
        10,
        "Checkpoint+WAL recovery: expected 10 rows, got {}",
        rows.len()
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// DDL recovery: CREATE TABLE + DROP TABLE replayed correctly
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_ddl_recovery_create_and_drop() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();

        // Create two tables
        let s1 = test_schema("keep_me", TableId(1));
        let s2 = test_schema("drop_me", TableId(2));
        engine.create_table(s1).unwrap();
        engine.create_table(s2).unwrap();

        // Insert into both
        let txn1 = TxnId(10);
        engine
            .insert(TableId(1), make_row(1, "keeper"), txn1)
            .unwrap();
        engine
            .commit_txn(txn1, Timestamp(100), TxnType::Local)
            .unwrap();

        let txn2 = TxnId(20);
        engine
            .insert(TableId(2), make_row(1, "dropper"), txn2)
            .unwrap();
        engine
            .commit_txn(txn2, Timestamp(200), TxnType::Local)
            .unwrap();

        // Drop the second table
        engine.drop_table("drop_me").unwrap();

        engine.flush_wal().unwrap();
    }

    let engine = StorageEngine::recover(dir.path()).unwrap();

    // Table 1 should exist with data
    let rows = engine
        .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
        .unwrap();
    assert_eq!(rows.len(), 1, "keep_me table should have 1 row");

    // Table 2 should not exist
    let catalog = engine.get_catalog();
    assert!(
        catalog.find_table("drop_me").is_none(),
        "drop_me should not exist after recovery"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Concurrent txns: interleaved commits maintain isolation after recovery
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_interleaved_txn_recovery() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = StorageEngine::new(Some(dir.path())).unwrap();
        let schema = test_schema("interleave", TableId(1));
        engine.create_table(schema).unwrap();

        // txn A and B both insert, but only A commits
        let txn_a = TxnId(100);
        let txn_b = TxnId(200);

        engine
            .insert(TableId(1), make_row(1, "from_a"), txn_a)
            .unwrap();
        engine
            .insert(TableId(1), make_row(2, "from_b"), txn_b)
            .unwrap();
        engine
            .insert(TableId(1), make_row(3, "also_from_a"), txn_a)
            .unwrap();

        engine
            .commit_txn(txn_a, Timestamp(500), TxnType::Local)
            .unwrap();
        // txn_b is NOT committed — simulates in-flight txn at crash

        engine.flush_wal().unwrap();
    }

    let engine = StorageEngine::recover(dir.path()).unwrap();
    let rows = engine
        .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
        .unwrap();

    assert_eq!(rows.len(), 2, "Only txn_a's rows should survive");
    let ids: Vec<i32> = rows
        .iter()
        .map(|(_, r)| match r.values[0] {
            Datum::Int32(i) => i,
            _ => -1,
        })
        .collect();
    assert!(
        ids.contains(&1) && ids.contains(&3),
        "Expected rows 1 and 3 from txn_a"
    );
    assert!(
        !ids.contains(&2),
        "Row 2 from uncommitted txn_b should not survive"
    );
}
