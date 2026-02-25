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
