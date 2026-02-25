    use crate::engine::StorageEngine;
    use falcon_common::config::NodeRole;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, StorageType, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId, TxnType};

    fn cs_schema() -> TableSchema {
        TableSchema {
            id: TableId(500),
            name: "cs_test".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int64,
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
            storage_type: StorageType::Columnstore,
            ..Default::default()
        }
    }

    fn analytics_engine() -> StorageEngine {
        let mut e = StorageEngine::new_in_memory();
        e.set_node_role(NodeRole::Analytics);
        e
    }

    #[test]
    fn test_columnstore_insert_and_scan_via_engine() {
        let engine = analytics_engine();
        engine.create_table(cs_schema()).unwrap();

        let row1 = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("alpha".into())]);
        let row2 = OwnedRow::new(vec![Datum::Int64(2), Datum::Text("beta".into())]);

        engine.insert(TableId(500), row1, TxnId(1)).unwrap();
        engine.insert(TableId(500), row2, TxnId(1)).unwrap();

        // ColumnStore: read_ts is accepted but not used for versioning
        let rows = engine.scan(TableId(500), TxnId(2), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 2, "both rows must be visible after insert");
    }

    #[test]
    fn test_columnstore_scan_returns_all_rows_regardless_of_read_ts() {
        let engine = analytics_engine();
        engine.create_table(cs_schema()).unwrap();

        for i in 0..5i64 {
            engine
                .insert(
                    TableId(500),
                    OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("v{}", i))]),
                    TxnId(1),
                )
                .unwrap();
        }

        // ColumnStore has no MVCC 鈥?all rows visible at any read_ts
        let rows_past = engine.scan(TableId(500), TxnId(2), Timestamp(0)).unwrap();
        let rows_future = engine
            .scan(TableId(500), TxnId(2), Timestamp(u64::MAX))
            .unwrap();
        assert_eq!(rows_past.len(), 5, "ColumnStore: all rows visible at ts=0");
        assert_eq!(
            rows_future.len(),
            5,
            "ColumnStore: all rows visible at ts=MAX"
        );
    }

    #[test]
    fn test_columnstore_update_not_supported() {
        let engine = analytics_engine();
        engine.create_table(cs_schema()).unwrap();

        let row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("v1".into())]);
        let pk = engine.insert(TableId(500), row.clone(), TxnId(1)).unwrap();

        // UPDATE is not supported on COLUMNSTORE (analytics workload)
        let result = engine.update(TableId(500), &pk, row, TxnId(2));
        assert!(
            result.is_err(),
            "UPDATE on COLUMNSTORE must return an error"
        );
    }

    #[test]
    fn test_columnstore_delete_not_supported() {
        let engine = analytics_engine();
        engine.create_table(cs_schema()).unwrap();

        let row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("v1".into())]);
        let pk = engine.insert(TableId(500), row, TxnId(1)).unwrap();

        // DELETE is not supported on COLUMNSTORE
        let result = engine.delete(TableId(500), &pk, TxnId(2));
        assert!(
            result.is_err(),
            "DELETE on COLUMNSTORE must return an error"
        );
    }

    #[test]
    fn test_columnstore_not_allowed_on_primary() {
        let mut engine = StorageEngine::new_in_memory();
        engine.set_node_role(NodeRole::Primary);

        let result = engine.create_table(cs_schema());
        assert!(
            result.is_err(),
            "COLUMNSTORE must be rejected on Primary nodes"
        );
    }

    #[test]
    fn test_columnstore_commit_does_not_affect_visibility() {
        // ColumnStore rows are immediately visible 鈥?commit is a no-op for visibility
        let engine = analytics_engine();
        engine.create_table(cs_schema()).unwrap();

        engine
            .insert(
                TableId(500),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("x".into())]),
                TxnId(1),
            )
            .unwrap();

        // Visible before commit
        let before = engine
            .scan(TableId(500), TxnId(99), Timestamp(100))
            .unwrap();
        assert_eq!(before.len(), 1);

        engine
            .commit_txn(TxnId(1), Timestamp(10), TxnType::Local)
            .unwrap();

        // Still visible after commit
        let after = engine
            .scan(TableId(500), TxnId(99), Timestamp(100))
            .unwrap();
        assert_eq!(after.len(), 1);
    }
}
