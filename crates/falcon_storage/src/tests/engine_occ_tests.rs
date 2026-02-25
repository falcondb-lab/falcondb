    use crate::engine::StorageEngine;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn occ_schema() -> TableSchema {
        TableSchema {
            id: TableId(100),
            name: "occ_test".into(),
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
    fn test_read_set_validation_no_conflict() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(occ_schema()).unwrap();

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);
        engine.insert(TableId(100), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 reads  鈥?no concurrent modifications
        let txn2 = TxnId(2);
        let _rows = engine.scan(TableId(100), txn2, Timestamp(15)).unwrap();
        assert!(engine.validate_read_set(txn2, Timestamp(15)).is_ok());
    }

    #[test]
    fn test_read_set_validation_detects_conflict() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(occ_schema()).unwrap();

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v1".into())]);
        let pk = engine.insert(TableId(100), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 reads at start_ts=15
        let txn2 = TxnId(2);
        let _ = engine.get(TableId(100), &pk, txn2, Timestamp(15)).unwrap();

        // txn3 updates same key and commits at ts=20 > txn2's start_ts
        let txn3 = TxnId(3);
        let new_row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v2".into())]);
        engine.update(TableId(100), &pk, new_row, txn3).unwrap();
        engine.commit_txn_local(txn3, Timestamp(20)).unwrap();

        // txn2's validation should FAIL
        assert!(engine.validate_read_set(txn2, Timestamp(15)).is_err());
    }

    #[test]
    fn test_read_set_cleaned_on_commit() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(occ_schema()).unwrap();

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);
        engine.insert(TableId(100), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 reads then commits  鈥?read-set should be cleaned
        let txn2 = TxnId(2);
        let _ = engine.scan(TableId(100), txn2, Timestamp(15)).unwrap();
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();

        // After commit, validate finds no read-set  鈥?passes trivially
        assert!(engine.validate_read_set(txn2, Timestamp(15)).is_ok());
    }

    #[test]
    fn test_scan_records_all_read_keys_for_occ() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(occ_schema()).unwrap();

        let txn1 = TxnId(1);
        for i in 1..=3 {
            let row = OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{}", i))]);
            engine.insert(TableId(100), row, txn1).unwrap();
        }
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 scans all 3 rows
        let txn2 = TxnId(2);
        let rows = engine.scan(TableId(100), txn2, Timestamp(15)).unwrap();
        assert_eq!(rows.len(), 3);

        // txn3 updates row id=2 and commits after txn2's start
        let txn3 = TxnId(3);
        let pk2 = rows
            .iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(2))
            .unwrap()
            .0
            .clone();
        let new_row = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("updated".into())]);
        engine.update(TableId(100), &pk2, new_row, txn3).unwrap();
        engine.commit_txn_local(txn3, Timestamp(20)).unwrap();

        // txn2's read-set should detect the conflict on row id=2
        assert!(engine.validate_read_set(txn2, Timestamp(15)).is_err());
    }
