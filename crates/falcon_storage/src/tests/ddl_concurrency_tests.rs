use crate::engine::StorageEngine;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId, TxnType};
use std::sync::Arc;

fn schema(id: u64, name: &str) -> TableSchema {
    TableSchema {
        id: TableId(id),
        name: name.into(),
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
fn test_ddl_create_drop_idempotent_error() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(800, "ddl_t1")).unwrap();
    // Duplicate create 鈫?error
    assert!(engine.create_table(schema(800, "ddl_t1")).is_err());
    // Drop
    engine.drop_table("ddl_t1").unwrap();
    // Drop again 鈫?error
    assert!(engine.drop_table("ddl_t1").is_err());
}

#[test]
fn test_ddl_truncate_clears_data() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(801, "ddl_trunc")).unwrap();
    engine
        .insert(
            TableId(801),
            OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
            TxnId(1),
        )
        .unwrap();
    engine
        .commit_txn(TxnId(1), Timestamp(5), TxnType::Local)
        .unwrap();

    let rows = engine.scan(TableId(801), TxnId(99), Timestamp(10)).unwrap();
    assert_eq!(rows.len(), 1);

    engine.truncate_table("ddl_trunc").unwrap();

    let rows = engine.scan(TableId(801), TxnId(99), Timestamp(20)).unwrap();
    assert!(rows.is_empty(), "truncate should clear all data");
}

#[test]
fn test_ddl_concurrent_create_different_tables() {
    let engine = Arc::new(StorageEngine::new_in_memory());
    let mut handles = vec![];
    for i in 0..10u64 {
        let e = engine.clone();
        handles.push(std::thread::spawn(move || {
            e.create_table(schema(850 + i, &format!("conc_t{}", i)))
        }));
    }
    let mut ok_count = 0;
    for h in handles {
        if h.join().unwrap().is_ok() {
            ok_count += 1;
        }
    }
    assert_eq!(
        ok_count, 10,
        "all 10 concurrent creates on different names should succeed"
    );
}

#[test]
fn test_ddl_concurrent_create_same_table() {
    let engine = Arc::new(StorageEngine::new_in_memory());
    let mut handles = vec![];
    for _ in 0..10 {
        let e = engine.clone();
        handles.push(std::thread::spawn(move || {
            e.create_table(schema(860, "conc_same"))
        }));
    }
    let mut ok_count = 0;
    for h in handles {
        if h.join().unwrap().is_ok() {
            ok_count += 1;
        }
    }
    assert_eq!(ok_count, 1, "exactly 1 concurrent create should win");
}

#[test]
fn test_ddl_concurrent_insert_during_create() {
    let engine = Arc::new(StorageEngine::new_in_memory());
    engine.create_table(schema(870, "conc_ins")).unwrap();

    let mut handles = vec![];
    for i in 0..20i32 {
        let e = engine.clone();
        handles.push(std::thread::spawn(move || {
            e.insert(
                TableId(870),
                OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{}", i))]),
                TxnId(100 + i as u64),
            )
        }));
    }
    let mut ok_count = 0;
    for h in handles {
        if h.join().unwrap().is_ok() {
            ok_count += 1;
        }
    }
    assert_eq!(
        ok_count, 20,
        "all concurrent inserts should succeed on unique PKs"
    );
}

#[test]
fn test_ddl_drop_prevents_further_dml() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(880, "ddl_drop_dml")).unwrap();
    engine.drop_table("ddl_drop_dml").unwrap();

    let result = engine.insert(
        TableId(880),
        OwnedRow::new(vec![Datum::Int32(1), Datum::Text("after_drop".into())]),
        TxnId(1),
    );
    assert!(result.is_err(), "insert after DROP should fail");
}

#[test]
fn test_ddl_create_index_and_drop_index() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(890, "idx_test")).unwrap();
    engine
        .create_named_index("idx_val_890", "idx_test", 1, false)
        .unwrap();
    assert!(engine.index_exists("idx_val_890"));

    engine.drop_index("idx_val_890").unwrap();
    assert!(!engine.index_exists("idx_val_890"));
}

#[test]
fn test_ddl_drop_nonexistent_index_returns_error() {
    let engine = StorageEngine::new_in_memory();
    let result = engine.drop_index("nonexistent_idx");
    assert!(
        result.is_err(),
        "dropping nonexistent index should return error"
    );
}

#[test]
fn test_ddl_create_database() {
    let engine = StorageEngine::new_in_memory();
    let oid = engine.create_database("testdb", "admin").unwrap();
    assert!(oid > 0);
    let catalog = engine.get_catalog();
    let db = catalog.find_database("testdb");
    assert!(db.is_some());
    assert_eq!(db.unwrap().name, "testdb");
    assert_eq!(db.unwrap().owner, "admin");
}

#[test]
fn test_ddl_create_database_already_exists() {
    let engine = StorageEngine::new_in_memory();
    engine.create_database("testdb", "admin").unwrap();
    let result = engine.create_database("testdb", "admin");
    assert!(result.is_err(), "creating duplicate database should fail");
}

#[test]
fn test_ddl_create_database_case_insensitive() {
    let engine = StorageEngine::new_in_memory();
    engine.create_database("TestDB", "admin").unwrap();
    let result = engine.create_database("testdb", "admin");
    assert!(result.is_err(), "case-insensitive duplicate should fail");
}

#[test]
fn test_ddl_drop_database() {
    let engine = StorageEngine::new_in_memory();
    engine.create_database("dropme", "admin").unwrap();
    engine.drop_database("dropme").unwrap();
    let catalog = engine.get_catalog();
    assert!(catalog.find_database("dropme").is_none());
}

#[test]
fn test_ddl_drop_database_not_found() {
    let engine = StorageEngine::new_in_memory();
    let result = engine.drop_database("nosuchdb");
    assert!(result.is_err(), "dropping nonexistent database should fail");
}

#[test]
fn test_ddl_drop_default_database_rejected() {
    let engine = StorageEngine::new_in_memory();
    let result = engine.drop_database("falcon");
    assert!(
        result.is_err(),
        "dropping the default 'falcon' database should be rejected"
    );
}

#[test]
fn test_ddl_list_databases_includes_default() {
    let engine = StorageEngine::new_in_memory();
    let catalog = engine.get_catalog();
    let dbs = catalog.list_databases();
    assert!(dbs.iter().any(|db| db.name == "falcon"));
}

#[test]
fn test_ddl_list_databases_after_create() {
    let engine = StorageEngine::new_in_memory();
    engine.create_database("db1", "admin").unwrap();
    engine.create_database("db2", "admin").unwrap();
    let catalog = engine.get_catalog();
    let dbs = catalog.list_databases();
    assert_eq!(dbs.len(), 3); // falcon + db1 + db2
    assert!(dbs.iter().any(|db| db.name == "db1"));
    assert!(dbs.iter().any(|db| db.name == "db2"));
}

// ── ALTER TABLE ADD COLUMN ──────────────────────────────────────

#[test]
fn test_alter_table_add_column_no_default() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(900, "alt_add")).unwrap();

    // Insert a row before adding column
    engine
        .insert(
            TableId(900),
            OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]),
            TxnId(1),
        )
        .unwrap();
    engine
        .commit_txn(TxnId(1), Timestamp(5), TxnType::Local)
        .unwrap();

    // Add column without default
    let new_col = ColumnDef {
        id: ColumnId(0), // will be reassigned
        name: "score".into(),
        data_type: DataType::Int64,
        nullable: true,
        is_primary_key: false,
        default_value: None,
        is_serial: false,
        max_length: None,
    };
    let ddl_id = engine.alter_table_add_column("alt_add", new_col).unwrap();
    assert!(ddl_id > 0);

    // Schema should now have 3 columns
    let cat = engine.get_catalog();
    let tbl = cat.find_table("alt_add").unwrap();
    assert_eq!(tbl.columns.len(), 3);
    assert_eq!(tbl.columns[2].name, "score");

    // Existing row still readable (shorter than schema — OK)
    let rows = engine.scan(TableId(900), TxnId(99), Timestamp(10)).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_alter_table_add_column_with_default_backfills() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(901, "alt_def")).unwrap();

    // Insert 3 rows
    for i in 0..3i32 {
        engine
            .insert(
                TableId(901),
                OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("r{i}"))]),
                TxnId(10 + i as u64),
            )
            .unwrap();
        engine
            .commit_txn(
                TxnId(10 + i as u64),
                Timestamp(10 + i as u64),
                TxnType::Local,
            )
            .unwrap();
    }

    // Add column with default value → triggers backfill
    let new_col = ColumnDef {
        id: ColumnId(0),
        name: "status".into(),
        data_type: DataType::Text,
        nullable: false,
        is_primary_key: false,
        default_value: Some(Datum::Text("active".into())),
        is_serial: false,
        max_length: None,
    };
    let ddl_id = engine.alter_table_add_column("alt_def", new_col).unwrap();
    assert!(engine.wait_for_ddl(ddl_id, 2000), "backfill timed out");

    // All existing rows should now have the default value backfilled
    let rows = engine.scan(TableId(901), TxnId(99), Timestamp(50)).unwrap();
    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert_eq!(
            row.1.values.len(),
            3,
            "row should have 3 columns after ADD COLUMN"
        );
        assert_eq!(row.1.values[2], Datum::Text("active".into()));
    }
}

// ── ALTER TABLE DROP COLUMN ─────────────────────────────────────

#[test]
fn test_alter_table_drop_column() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(902, "alt_drop")).unwrap();

    engine
        .insert(
            TableId(902),
            OwnedRow::new(vec![Datum::Int32(1), Datum::Text("bye".into())]),
            TxnId(1),
        )
        .unwrap();
    engine
        .commit_txn(TxnId(1), Timestamp(5), TxnType::Local)
        .unwrap();

    // Drop the "val" column
    let ddl_id = engine.alter_table_drop_column("alt_drop", "val").unwrap();
    assert!(ddl_id > 0);

    // Schema should now have 1 column
    let cat = engine.get_catalog();
    let tbl = cat.find_table("alt_drop").unwrap();
    assert_eq!(tbl.columns.len(), 1);
    assert_eq!(tbl.columns[0].name, "id");
}

#[test]
fn test_alter_table_drop_pk_column_rejected() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(903, "alt_drop_pk")).unwrap();

    let result = engine.alter_table_drop_column("alt_drop_pk", "id");
    assert!(result.is_err(), "dropping PK column should fail");
}

#[test]
fn test_alter_table_drop_nonexistent_column() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(904, "alt_drop_ne")).unwrap();

    let result = engine.alter_table_drop_column("alt_drop_ne", "nosuch");
    assert!(result.is_err(), "dropping nonexistent column should fail");
}

// ── ALTER TABLE RENAME COLUMN ───────────────────────────────────

#[test]
fn test_alter_table_rename_column() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(905, "alt_ren")).unwrap();

    engine
        .alter_table_rename_column("alt_ren", "val", "description")
        .unwrap();

    let cat = engine.get_catalog();
    let tbl = cat.find_table("alt_ren").unwrap();
    assert_eq!(tbl.columns[1].name, "description");
}

// ── ALTER TABLE RENAME TABLE ────────────────────────────────────

#[test]
fn test_alter_table_rename() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(906, "old_name")).unwrap();

    engine.alter_table_rename("old_name", "new_name").unwrap();

    let cat = engine.get_catalog();
    assert!(cat.find_table("old_name").is_none());
    assert!(cat.find_table("new_name").is_some());
}

#[test]
fn test_alter_table_rename_conflict() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(907, "tbl_a")).unwrap();
    engine.create_table(schema(908, "tbl_b")).unwrap();

    let result = engine.alter_table_rename("tbl_a", "tbl_b");
    assert!(result.is_err(), "rename to existing table should fail");
}

// ── CREATE INDEX CONCURRENTLY ───────────────────────────────────

#[test]
fn test_create_index_concurrently_basic() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(910, "cidx_t")).unwrap();

    // Insert rows first
    for i in 0..100i32 {
        engine
            .insert(
                TableId(910),
                OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{i}"))]),
                TxnId(200 + i as u64),
            )
            .unwrap();
        engine
            .commit_txn(
                TxnId(200 + i as u64),
                Timestamp(200 + i as u64),
                TxnType::Local,
            )
            .unwrap();
    }

    let ddl_id = engine
        .create_index_concurrently("idx_cidx_val", "cidx_t", &[1], false)
        .unwrap();
    assert!(ddl_id > 0);
    assert!(engine.index_exists("idx_cidx_val"));
}

#[test]
fn test_create_unique_index_concurrently_duplicate_rejected() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(911, "uidx_t")).unwrap();

    // Insert duplicate values in column 1
    engine
        .insert(
            TableId(911),
            OwnedRow::new(vec![Datum::Int32(1), Datum::Text("dup".into())]),
            TxnId(1),
        )
        .unwrap();
    engine
        .commit_txn(TxnId(1), Timestamp(1), TxnType::Local)
        .unwrap();
    engine
        .insert(
            TableId(911),
            OwnedRow::new(vec![Datum::Int32(2), Datum::Text("dup".into())]),
            TxnId(2),
        )
        .unwrap();
    engine
        .commit_txn(TxnId(2), Timestamp(2), TxnType::Local)
        .unwrap();

    let result = engine.create_index_concurrently("uidx_dup", "uidx_t", &[1], true);
    assert!(result.is_err(), "unique index with duplicates should fail");
}

// ── Backfill WAL recovery (DdlBackfillUpdate) ───────────────────

#[test]
fn test_add_column_backfill_replayed_by_ddl_backfill_replace() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(912, "bfill_wal")).unwrap();

    engine
        .insert(
            TableId(912),
            OwnedRow::new(vec![Datum::Int32(1), Datum::Text("row1".into())]),
            TxnId(1),
        )
        .unwrap();
    engine
        .commit_txn(TxnId(1), Timestamp(1), TxnType::Local)
        .unwrap();

    // Simulate what ADD COLUMN backfill does: WAL a DdlBackfillUpdate
    // and then call replace_latest. After recovery both should match.
    let new_row = OwnedRow::new(vec![
        Datum::Int32(1),
        Datum::Text("row1".into()),
        Datum::Text("active".into()),
    ]);
    let pk = engine
        .scan(TableId(912), TxnId(99), Timestamp(5))
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .0;

    engine.ddl_backfill_replace(&TableId(912), &pk, new_row.clone());

    // Row should now have 3 columns
    let rows = engine.scan(TableId(912), TxnId(99), Timestamp(5)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.values.len(), 3);
    assert_eq!(rows[0].1.values[2], Datum::Text("active".into()));
}

#[test]
fn test_create_index_concurrently_skips_uncommitted() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(913, "cidx_snap")).unwrap();

    // Committed rows
    for i in 0..5i32 {
        engine
            .insert(
                TableId(913),
                OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{i}"))]),
                TxnId(10 + i as u64),
            )
            .unwrap();
        engine
            .commit_txn(
                TxnId(10 + i as u64),
                Timestamp(10 + i as u64),
                TxnType::Local,
            )
            .unwrap();
    }

    // Uncommitted insert — should NOT appear in the index
    engine
        .insert(
            TableId(913),
            OwnedRow::new(vec![Datum::Int32(99), Datum::Text("uncommitted".into())]),
            TxnId(999),
        )
        .unwrap();
    // Note: TxnId(999) never committed

    let ddl_id = engine
        .create_index_concurrently("idx_snap_val", "cidx_snap", &[1], false)
        .unwrap();
    assert!(ddl_id > 0);
    assert!(engine.index_exists("idx_snap_val"));

    // Full scan should return 5 committed rows only (uncommitted row invisible)
    let results = engine
        .scan(TableId(913), TxnId(99), Timestamp(100))
        .unwrap();
    assert_eq!(results.len(), 5, "scan should return only committed rows");
}

#[test]
fn test_ddl_progress_tracks_add_column() {
    let engine = StorageEngine::new_in_memory();
    engine.create_table(schema(914, "prog_t")).unwrap();

    for i in 0..10i32 {
        engine
            .insert(
                TableId(914),
                OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{i}"))]),
                TxnId(20 + i as u64),
            )
            .unwrap();
        engine
            .commit_txn(
                TxnId(20 + i as u64),
                Timestamp(20 + i as u64),
                TxnType::Local,
            )
            .unwrap();
    }

    let col = ColumnDef {
        id: ColumnId(0),
        name: "tag".into(),
        data_type: DataType::Text,
        nullable: true,
        is_primary_key: false,
        default_value: Some(Datum::Text("x".into())),
        is_serial: false,
        max_length: None,
    };
    let ddl_id = engine.alter_table_add_column("prog_t", col).unwrap();

    let progress = engine.ddl_progress();
    assert!(
        !progress.is_empty(),
        "ddl_progress should return at least one entry"
    );
    let entry = progress.iter().find(|p| p.id == ddl_id);
    assert!(
        entry.is_some(),
        "ddl_progress should contain the new DDL id"
    );
}

// ── ALTER TABLE + DML concurrency ───────────────────────────────

#[test]
fn test_alter_add_column_concurrent_inserts() {
    let engine = Arc::new(StorageEngine::new_in_memory());
    engine.create_table(schema(920, "conc_alt")).unwrap();

    // Pre-populate
    for i in 0..50i32 {
        engine
            .insert(
                TableId(920),
                OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{i}"))]),
                TxnId(300 + i as u64),
            )
            .unwrap();
        engine
            .commit_txn(
                TxnId(300 + i as u64),
                Timestamp(300 + i as u64),
                TxnType::Local,
            )
            .unwrap();
    }

    // Concurrent: ADD COLUMN + inserts
    let e1 = engine.clone();
    let ddl_handle = std::thread::spawn(move || {
        let col = ColumnDef {
            id: ColumnId(0),
            name: "extra".into(),
            data_type: DataType::Int32,
            nullable: true,
            is_primary_key: false,
            default_value: Some(Datum::Int32(42)),
            is_serial: false,
            max_length: None,
        };
        e1.alter_table_add_column("conc_alt", col)
    });

    let e2 = engine.clone();
    let insert_handle = std::thread::spawn(move || {
        let mut ok = 0;
        for i in 50..70i32 {
            if engine
                .insert(
                    TableId(920),
                    OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("new{i}"))]),
                    TxnId(500 + i as u64),
                )
                .is_ok()
            {
                let _ = engine.commit_txn(
                    TxnId(500 + i as u64),
                    Timestamp(500 + i as u64),
                    TxnType::Local,
                );
                ok += 1;
            }
        }
        ok
    });

    let ddl_result = ddl_handle.join().unwrap();
    assert!(ddl_result.is_ok(), "ADD COLUMN should succeed");

    let inserted = insert_handle.join().unwrap();
    assert!(inserted > 0, "some concurrent inserts should succeed");

    // Schema should have 3 columns
    let cat = e2.get_catalog();
    let tbl = cat.find_table("conc_alt").unwrap();
    assert_eq!(tbl.columns.len(), 3);
}
