use crate::engine::StorageEngine;
use crate::wal::{SyncMode, WalRecord, WalWriter};
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

fn recovery_schema() -> TableSchema {
    TableSchema {
        id: TableId(1),
        name: "rec_test".into(),
        columns: vec![
            ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
                max_length: None,
            },
            ColumnDef {
                id: ColumnId(1),
                name: "val".into(),
                data_type: DataType::Text,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
                max_length: None,
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

#[test]
fn test_recovery_aborts_uncommitted_txns() {
    let dir = std::env::temp_dir().join("falcon_recovery_uncommitted");
    let _ = std::fs::remove_dir_all(&dir);

    // Write WAL with an uncommitted transaction (no Commit record)
    {
        let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.append(&WalRecord::CreateTable {
            schema: recovery_schema(),
        })
        .unwrap();

        // Committed txn
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("committed".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::CommitTxnLocal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(10),
        })
        .unwrap();

        // Uncommitted txn (no commit record  —simulates crash)
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(2), Datum::Text("uncommitted".into())]),
        })
        .unwrap();
        wal.flush().unwrap();
    }

    // Recover  —uncommitted txn should be aborted
    let engine = StorageEngine::recover(&dir).unwrap();
    let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "only committed row should be visible after recovery"
    );
    assert_eq!(rows[0].1.values[1], Datum::Text("committed".into()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_idempotent() {
    let dir = std::env::temp_dir().join("falcon_recovery_idempotent");
    let _ = std::fs::remove_dir_all(&dir);

    // Write WAL
    {
        let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.append(&WalRecord::CreateTable {
            schema: recovery_schema(),
        })
        .unwrap();

        wal.append(&WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::CommitTxnLocal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(10),
        })
        .unwrap();

        // Uncommitted txn
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(2), Datum::Text("crash".into())]),
        })
        .unwrap();
        wal.flush().unwrap();
    }

    // First recovery
    {
        let engine = StorageEngine::recover(&dir).unwrap();
        let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 1);
    }

    // Second recovery (idempotent  —same result)
    {
        let engine = StorageEngine::recover(&dir).unwrap();
        let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "idempotent recovery should produce same result"
        );
        assert_eq!(rows[0].1.values[1], Datum::Text("hello".into()));
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_update_then_crash() {
    let dir = std::env::temp_dir().join("falcon_recovery_update_crash");
    let _ = std::fs::remove_dir_all(&dir);

    // Write WAL: insert + commit, then update without commit (crash)
    {
        let engine = StorageEngine::new(Some(&dir)).unwrap();
        engine.create_table(recovery_schema()).unwrap();

        let txn1 = TxnId(1);
        let pk = engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("original".into())]),
                txn1,
            )
            .unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Update without commit (simulates crash mid-update)
        let txn2 = TxnId(2);
        engine
            .update(
                TableId(1),
                &pk,
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("updated".into())]),
                txn2,
            )
            .unwrap();
        // No commit  —crash!
    }

    // Recover  —should see original value, not the uncommitted update
    let engine = StorageEngine::recover(&dir).unwrap();
    let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.values[1],
        Datum::Text("original".into()),
        "uncommitted update should be rolled back after recovery"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_delete_then_crash() {
    let dir = std::env::temp_dir().join("falcon_recovery_delete_crash");
    let _ = std::fs::remove_dir_all(&dir);

    // Write WAL: insert + commit, then delete without commit (crash)
    {
        let engine = StorageEngine::new(Some(&dir)).unwrap();
        engine.create_table(recovery_schema()).unwrap();

        let txn1 = TxnId(1);
        let pk = engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("keep_me".into())]),
                txn1,
            )
            .unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Delete without commit
        let txn2 = TxnId(2);
        engine.delete(TableId(1), &pk, txn2).unwrap();
        // No commit  —crash!
    }

    // Recover  —row should still be visible
    let engine = StorageEngine::recover(&dir).unwrap();
    let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "uncommitted delete should be rolled back after recovery"
    );
    assert_eq!(rows[0].1.values[1], Datum::Text("keep_me".into()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_explicit_abort_in_wal() {
    let dir = std::env::temp_dir().join("falcon_recovery_explicit_abort");
    let _ = std::fs::remove_dir_all(&dir);

    // Write WAL with explicit abort record
    {
        let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.append(&WalRecord::CreateTable {
            schema: recovery_schema(),
        })
        .unwrap();

        wal.append(&WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("aborted".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::AbortTxnLocal { txn_id: TxnId(1) })
            .unwrap();

        wal.append(&WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(2), Datum::Text("committed".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::CommitTxnLocal {
            txn_id: TxnId(2),
            commit_ts: Timestamp(20),
        })
        .unwrap();
        wal.flush().unwrap();
    }

    let engine = StorageEngine::recover(&dir).unwrap();
    let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "explicitly aborted txn should not be visible"
    );
    assert_eq!(rows[0].1.values[1], Datum::Text("committed".into()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_global_txn_committed() {
    let dir = std::env::temp_dir().join(format!(
        "falcon_recovery_global_commit_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.append(&WalRecord::CreateTable {
            schema: recovery_schema(),
        })
        .unwrap();

        wal.append(&WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: OwnedRow::new(vec![
                Datum::Int32(1),
                Datum::Text("global_committed".into()),
            ]),
        })
        .unwrap();
        wal.append(&WalRecord::PrepareTxn { txn_id: TxnId(1) })
            .unwrap();
        wal.append(&WalRecord::CommitTxnGlobal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(10),
        })
        .unwrap();
        wal.flush().unwrap();
    }

    let engine = StorageEngine::recover(&dir).unwrap();
    let rows = engine.scan(TableId(1), TxnId(99), Timestamp(100)).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "committed global txn should be visible after recovery"
    );
    assert_eq!(rows[0].1.values[1], Datum::Text("global_committed".into()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_global_txn_prepared_not_committed() {
    let dir = std::env::temp_dir().join(format!(
        "falcon_recovery_global_prepared_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.append(&WalRecord::CreateTable {
            schema: recovery_schema(),
        })
        .unwrap();

        // Committed local txn for baseline
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("local_ok".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::CommitTxnLocal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(10),
        })
        .unwrap();

        // Global txn: prepared but NOT committed (simulates coordinator crash)
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(2), Datum::Text("prepared_only".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::PrepareTxn { txn_id: TxnId(2) })
            .unwrap();
        // No commit record  —crash after prepare
        wal.flush().unwrap();
    }

    let engine = StorageEngine::recover(&dir).unwrap();
    let rows = engine.scan(TableId(1), TxnId(99), Timestamp(100)).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "prepared-but-not-committed global txn should be invisible"
    );
    assert_eq!(rows[0].1.values[1], Datum::Text("local_ok".into()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_global_txn_prepared_then_aborted() {
    let dir = std::env::temp_dir().join(format!(
        "falcon_recovery_global_abort_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.append(&WalRecord::CreateTable {
            schema: recovery_schema(),
        })
        .unwrap();

        // Global txn: prepared then explicitly aborted
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("aborted_global".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::PrepareTxn { txn_id: TxnId(1) })
            .unwrap();
        wal.append(&WalRecord::AbortTxnGlobal { txn_id: TxnId(1) })
            .unwrap();

        // Another global txn: committed normally
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(1),
            row: OwnedRow::new(vec![
                Datum::Int32(2),
                Datum::Text("committed_global".into()),
            ]),
        })
        .unwrap();
        wal.append(&WalRecord::PrepareTxn { txn_id: TxnId(2) })
            .unwrap();
        wal.append(&WalRecord::CommitTxnGlobal {
            txn_id: TxnId(2),
            commit_ts: Timestamp(20),
        })
        .unwrap();
        wal.flush().unwrap();
    }

    let engine = StorageEngine::recover(&dir).unwrap();
    let rows = engine.scan(TableId(1), TxnId(99), Timestamp(100)).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "only committed global txn should survive recovery"
    );
    assert_eq!(rows[0].1.values[1], Datum::Text("committed_global".into()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_create_index_durability() {
    let dir = std::env::temp_dir().join(format!(
        "falcon_recovery_create_index_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);

    // Phase 1: create table + create named index, then "crash"
    {
        let engine = StorageEngine::new(Some(&dir)).unwrap();
        engine.create_table(recovery_schema()).unwrap();
        engine
            .create_named_index("idx_val", "rec_test", 1, false)
            .unwrap();
    }

    // Phase 2: recover —index must be present in the registry
    let engine = StorageEngine::recover(&dir).unwrap();
    assert!(
        engine.index_exists("idx_val"),
        "CREATE INDEX must survive WAL recovery"
    );
    let indexed = engine.get_indexed_columns(TableId(1));
    assert!(
        indexed.iter().any(|(col, _)| *col == 1),
        "recovered index must cover column 1"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_drop_index_durability() {
    let dir =
        std::env::temp_dir().join(format!("falcon_recovery_drop_index_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    // Phase 1: create table + index, then drop the index
    {
        let engine = StorageEngine::new(Some(&dir)).unwrap();
        engine.create_table(recovery_schema()).unwrap();
        engine
            .create_named_index("idx_val", "rec_test", 1, false)
            .unwrap();
        engine.drop_index("idx_val").unwrap();
    }

    // Phase 2: recover —index must NOT be present
    let engine = StorageEngine::recover(&dir).unwrap();
    assert!(
        !engine.index_exists("idx_val"),
        "DROP INDEX must survive WAL recovery —index should be gone"
    );
    let indexed = engine.get_indexed_columns(TableId(1));
    assert!(
        !indexed.iter().any(|(col, _)| *col == 1),
        "dropped index must not cover column 1 after recovery"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_multi_table_interleaved() {
    let dir = std::env::temp_dir().join(format!(
        "falcon_recovery_multi_table_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);

    let schema2 = {
        let mut s = recovery_schema();
        s.id = TableId(2);
        s.name = "rec_test2".into();
        s
    };

    {
        let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.append(&WalRecord::CreateTable {
            schema: recovery_schema(),
        })
        .unwrap();
        wal.append(&WalRecord::CreateTable {
            schema: schema2.clone(),
        })
        .unwrap();

        // T1: insert into table 1 + commit
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("t1_ok".into())]),
        })
        .unwrap();
        // T2: insert into table 2 (interleaved, committed later)
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(2),
            row: OwnedRow::new(vec![Datum::Int32(10), Datum::Text("t2_ok".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::CommitTxnLocal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(5),
        })
        .unwrap();
        // T3: insert into table 1, uncommitted (crash)
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(3),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(2), Datum::Text("t1_crash".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::CommitTxnLocal {
            txn_id: TxnId(2),
            commit_ts: Timestamp(10),
        })
        .unwrap();
        wal.flush().unwrap();
    }

    let engine = StorageEngine::recover(&dir).unwrap();
    let rows1 = engine.scan(TableId(1), TxnId(99), Timestamp(100)).unwrap();
    assert_eq!(rows1.len(), 1, "table 1: only committed row");
    assert_eq!(rows1[0].1.values[1], Datum::Text("t1_ok".into()));

    let rows2 = engine.scan(TableId(2), TxnId(99), Timestamp(100)).unwrap();
    assert_eq!(rows2.len(), 1, "table 2: committed row");
    assert_eq!(rows2[0].1.values[1], Datum::Text("t2_ok".into()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_replay_idempotent_three_times() {
    let dir = std::env::temp_dir().join(format!(
        "falcon_recovery_3x_idempotent_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.append(&WalRecord::CreateTable {
            schema: recovery_schema(),
        })
        .unwrap();
        for i in 1..=5i32 {
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(i as u64),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("row{}", i))]),
            })
            .unwrap();
            wal.append(&WalRecord::CommitTxnLocal {
                txn_id: TxnId(i as u64),
                commit_ts: Timestamp(i as u64 * 10),
            })
            .unwrap();
        }
        wal.flush().unwrap();
    }

    // Replay 3 times —each must produce identical result
    for attempt in 1..=3 {
        let engine = StorageEngine::recover(&dir).unwrap();
        let rows = engine.scan(TableId(1), TxnId(99), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 5, "attempt {}: all 5 committed rows", attempt);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_recovery_wal_first_ordering() {
    // Verify that StorageEngine writes to WAL before mutating memtable.
    // We do this by checking that the WAL observer counter fires for every DML.
    let mut engine = StorageEngine::new_in_memory();
    engine.create_table(recovery_schema()).unwrap();

    let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let c = counter.clone();
    engine.set_wal_observer(Box::new(move |_record| {
        c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }));

    // Insert is deferred to txn_wal_buf — observer fires at commit, not here
    let pk = engine
        .insert(
            TableId(1),
            OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
            TxnId(1),
        )
        .unwrap();
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "insert is deferred — observer should not fire yet"
    );

    // Commit T1: flushes deferred insert + commit record = 2 observer fires
    engine
        .commit_txn(TxnId(1), Timestamp(5), falcon_common::types::TxnType::Local)
        .unwrap();
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "commit should flush deferred insert + commit (2 fires)"
    );

    // Update fires immediately
    engine
        .update(
            TableId(1),
            &pk,
            OwnedRow::new(vec![Datum::Int32(1), Datum::Text("b".into())]),
            TxnId(2),
        )
        .unwrap();
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "update fires observer immediately"
    );

    // Commit T2 before T3 can delete
    engine
        .commit_txn(
            TxnId(2),
            Timestamp(10),
            falcon_common::types::TxnType::Local,
        )
        .unwrap();

    // Delete fires immediately
    engine.delete(TableId(1), &pk, TxnId(3)).unwrap();
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::SeqCst),
        5,
        "delete fires observer immediately"
    );
}

#[test]
fn test_recovery_prepared_txn_tracked_as_indoubt() {
    let dir = std::env::temp_dir().join(format!("falcon_recovery_indoubt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.append(&WalRecord::CreateTable {
            schema: recovery_schema(),
        })
        .unwrap();

        // Txn 1: prepared and committed — should NOT be in-doubt
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("committed".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::PrepareTxn { txn_id: TxnId(1) })
            .unwrap();
        wal.append(&WalRecord::CommitTxnGlobal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(10),
        })
        .unwrap();

        // Txn 2: prepared but no commit/abort — should be in-doubt
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(2), Datum::Text("indoubt".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::PrepareTxn { txn_id: TxnId(2) })
            .unwrap();

        // Txn 3: prepared then aborted — should NOT be in-doubt
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(3),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(3), Datum::Text("aborted".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::PrepareTxn { txn_id: TxnId(3) })
            .unwrap();
        wal.append(&WalRecord::AbortTxnGlobal { txn_id: TxnId(3) })
            .unwrap();

        // Txn 4: no prepare, no commit — regular uncommitted, should be aborted
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(4),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(4), Datum::Text("uncommitted".into())]),
        })
        .unwrap();

        wal.flush().unwrap();
    }

    let engine = StorageEngine::recover(&dir).unwrap();

    // Only txn 2 should be in recovered_prepared_txns
    let prepared = engine.recovered_prepared_txns.read();
    assert_eq!(prepared.len(), 1, "only 1 in-doubt prepared txn expected");
    assert!(prepared.contains(&TxnId(2)), "txn 2 should be in-doubt");

    // Txn 1 committed → visible
    let rows = engine.scan(TableId(1), TxnId(99), Timestamp(100)).unwrap();
    assert_eq!(rows.len(), 1, "only committed txn 1 should be visible");
    assert_eq!(rows[0].1.values[1], Datum::Text("committed".into()));

    let _ = std::fs::remove_dir_all(&dir);
}
