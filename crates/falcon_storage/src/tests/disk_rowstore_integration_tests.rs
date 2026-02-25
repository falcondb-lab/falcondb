    use crate::engine::StorageEngine;
    use falcon_common::config::NodeRole;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, StorageType, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId, TxnType};

    fn disk_schema() -> TableSchema {
        TableSchema {
            id: TableId(600),
            name: "disk_test".into(),
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
            storage_type: StorageType::DiskRowstore,
            ..Default::default()
        }
    }

    fn analytics_engine() -> StorageEngine {
        let mut e = StorageEngine::new_in_memory();
        e.set_node_role(NodeRole::Analytics);
        e
    }

    #[test]
    fn test_disk_rowstore_insert_and_scan_via_engine() {
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("a".into())]),
                TxnId(1),
            )
            .unwrap();
        engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(2), Datum::Text("b".into())]),
                TxnId(1),
            )
            .unwrap();

        let rows = engine.scan(TableId(600), TxnId(2), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_disk_rowstore_update_via_engine() {
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        let pk = engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("old".into())]),
                TxnId(1),
            )
            .unwrap();
        engine
            .update(
                TableId(600),
                &pk,
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("new".into())]),
                TxnId(2),
            )
            .unwrap();

        let rows = engine.scan(TableId(600), TxnId(3), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[1], Datum::Text("new".into()));
    }

    #[test]
    fn test_disk_rowstore_delete_via_engine() {
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        let pk = engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("gone".into())]),
                TxnId(1),
            )
            .unwrap();
        engine.delete(TableId(600), &pk, TxnId(2)).unwrap();

        let rows = engine.scan(TableId(600), TxnId(3), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 0, "deleted row must not appear in scan");
    }

    #[test]
    fn test_disk_rowstore_scan_ignores_read_ts() {
        // DiskRowstore has no MVCC 鈥?read_ts is accepted but not used for versioning
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("x".into())]),
                TxnId(1),
            )
            .unwrap();

        let rows_ts0 = engine.scan(TableId(600), TxnId(2), Timestamp(0)).unwrap();
        let rows_tsmax = engine
            .scan(TableId(600), TxnId(2), Timestamp(u64::MAX))
            .unwrap();
        assert_eq!(rows_ts0.len(), 1, "DiskRowstore: row visible at ts=0");
        assert_eq!(rows_tsmax.len(), 1, "DiskRowstore: row visible at ts=MAX");
    }

    #[test]
    fn test_disk_rowstore_duplicate_key_rejected() {
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        let row = OwnedRow::new(vec![Datum::Int64(42), Datum::Text("dup".into())]);
        engine.insert(TableId(600), row.clone(), TxnId(1)).unwrap();
        let result = engine.insert(TableId(600), row, TxnId(2));
        assert!(
            result.is_err(),
            "duplicate PK must be rejected by DiskRowstore"
        );
    }

    #[test]
    fn test_disk_rowstore_not_allowed_on_primary() {
        let mut engine = StorageEngine::new_in_memory();
        engine.set_node_role(NodeRole::Primary);

        let result = engine.create_table(disk_schema());
        assert!(
            result.is_err(),
            "DISK_ROWSTORE must be rejected on Primary nodes"
        );
    }

    #[test]
    fn test_disk_rowstore_commit_does_not_affect_visibility() {
        // DiskRowstore rows are immediately visible 鈥?commit is a no-op for visibility
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("y".into())]),
                TxnId(1),
            )
            .unwrap();

        let before = engine
            .scan(TableId(600), TxnId(99), Timestamp(100))
            .unwrap();
        assert_eq!(before.len(), 1);

        engine
            .commit_txn(TxnId(1), Timestamp(10), TxnType::Local)
            .unwrap();

        let after = engine
            .scan(TableId(600), TxnId(99), Timestamp(100))
            .unwrap();
        assert_eq!(after.len(), 1);
    }
