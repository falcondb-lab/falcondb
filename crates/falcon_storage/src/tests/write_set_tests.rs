    use crate::engine::StorageEngine;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn ws_schema(id: u64, name: &str) -> TableSchema {
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
    fn test_write_set_multi_table_commit() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(ws_schema(1, "t1")).unwrap();
        engine.create_table(ws_schema(2, "t2")).unwrap();

        let txn1 = TxnId(1);
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
                txn1,
            )
            .unwrap();
        engine
            .insert(
                TableId(2),
                OwnedRow::new(vec![Datum::Int32(10), Datum::Text("x".into())]),
                txn1,
            )
            .unwrap();
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("b".into())]),
                txn1,
            )
            .unwrap();
        engine
            .insert(
                TableId(2),
                OwnedRow::new(vec![Datum::Int32(20), Datum::Text("y".into())]),
                txn1,
            )
            .unwrap();

        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        let t1_rows = engine.scan(TableId(1), TxnId(2), Timestamp(10)).unwrap();
        let t2_rows = engine.scan(TableId(2), TxnId(2), Timestamp(10)).unwrap();
        assert_eq!(t1_rows.len(), 2, "t1 should have 2 rows after commit");
        assert_eq!(t2_rows.len(), 2, "t2 should have 2 rows after commit");
    }

    #[test]
    fn test_write_set_multi_table_abort() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(ws_schema(1, "t1")).unwrap();
        engine.create_table(ws_schema(2, "t2")).unwrap();

        // Commit some baseline data
        let txn1 = TxnId(1);
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("base".into())]),
                txn1,
            )
            .unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Write to both tables, then abort
        let txn2 = TxnId(2);
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("aborted".into())]),
                txn2,
            )
            .unwrap();
        engine
            .insert(
                TableId(2),
                OwnedRow::new(vec![Datum::Int32(10), Datum::Text("aborted".into())]),
                txn2,
            )
            .unwrap();
        engine.abort_txn_local(txn2).unwrap();

        let t1_rows = engine.scan(TableId(1), TxnId(3), Timestamp(20)).unwrap();
        let t2_rows = engine.scan(TableId(2), TxnId(3), Timestamp(20)).unwrap();
        assert_eq!(
            t1_rows.len(),
            1,
            "t1 should only have baseline row after abort"
        );
        assert_eq!(t2_rows.len(), 0, "t2 should be empty after abort");
    }

    #[test]
    fn test_write_set_duplicate_key_in_same_txn() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(ws_schema(1, "t1")).unwrap();

        let txn1 = TxnId(1);
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("first".into())]),
                txn1,
            )
            .unwrap();

        // Update the same key within the same txn
        let pk = crate::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(1), Datum::Text("first".into())]),
            &[0],
        );
        engine
            .update(
                TableId(1),
                &pk,
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("second".into())]),
                txn1,
            )
            .unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        let rows = engine.scan(TableId(1), TxnId(2), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].1.values[1],
            Datum::Text("second".into()),
            "should see the latest update within the same txn"
        );
    }

    #[test]
    fn test_write_set_commit_does_not_affect_other_txns() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(ws_schema(1, "t1")).unwrap();

        // txn1 and txn2 both write
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("txn1".into())]),
                txn1,
            )
            .unwrap();
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("txn2".into())]),
                txn2,
            )
            .unwrap();

        // Commit txn1 only
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2's write should still be uncommitted
        let rows = engine.scan(TableId(1), TxnId(3), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 1, "only txn1's row should be visible");
        assert_eq!(rows[0].1.values[1], Datum::Text("txn1".into()));

        // Now commit txn2
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();
        let rows = engine.scan(TableId(1), TxnId(3), Timestamp(20)).unwrap();
        assert_eq!(
            rows.len(),
            2,
            "both rows should be visible after both commits"
        );
    }
