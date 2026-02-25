    use crate::engine::StorageEngine;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn test_schema(id: u64) -> TableSchema {
        TableSchema {
            id: TableId(id),
            name: format!("test_{}", id),
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
    fn test_engine_create_and_drop() {
        let engine = StorageEngine::new_in_memory();
        let schema = test_schema(1);
        engine.create_table(schema).unwrap();
        assert!(engine.get_table_schema("test_1").is_some());

        engine.drop_table("test_1").unwrap();
        assert!(engine.get_table_schema("test_1").is_none());
    }

    #[test]
    fn test_engine_duplicate_table() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(test_schema(1)).unwrap();
        assert!(engine.create_table(test_schema(1)).is_err());
    }

    #[test]
    fn test_engine_insert_scan_commit() {
        let engine = StorageEngine::new_in_memory();
        let schema = test_schema(1);
        let table_id = engine.create_table(schema).unwrap();

        let txn = TxnId(1);
        engine
            .insert(
                table_id,
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
                txn,
            )
            .unwrap();
        engine
            .insert(
                table_id,
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("b".into())]),
                txn,
            )
            .unwrap();

        engine.commit_txn_local(txn, Timestamp(10)).unwrap();

        let rows = engine.scan(table_id, TxnId(2), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_engine_recovery() {
        let dir = std::env::temp_dir().join("falcon_engine_recovery_test");
        let _ = std::fs::remove_dir_all(&dir);

        // Phase 1: Write data
        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            let schema = test_schema(1);
            engine.create_table(schema).unwrap();

            let txn = TxnId(1);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(1), Datum::Text("persisted".into())]),
                    txn,
                )
                .unwrap();
            engine.commit_txn_local(txn, Timestamp(10)).unwrap();
        }

        // Phase 2: Recover and verify
        {
            let engine = StorageEngine::recover(&dir).unwrap();
            assert!(engine.get_table_schema("test_1").is_some());

            let rows = engine.scan(TableId(1), TxnId(2), Timestamp(10)).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.values[1], Datum::Text("persisted".into()));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
