    use crate::memtable::MemTable;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "test".into(),
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
    fn test_insert_and_get() {
        let table = MemTable::new(test_schema());
        let txn = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);

        let pk = table.insert(row.clone(), txn).unwrap();
        table.commit_txn(txn, Timestamp(10));

        let result = table.get(&pk, TxnId(2), Timestamp(10));
        assert!(result.is_some());
        assert_eq!(result.unwrap().values[1], Datum::Text("hello".into()));
    }

    #[test]
    fn test_scan_visibility() {
        let table = MemTable::new(test_schema());
        let txn1 = TxnId(1);

        table
            .insert(
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
                txn1,
            )
            .unwrap();
        table
            .insert(
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("b".into())]),
                txn1,
            )
            .unwrap();
        table.commit_txn(txn1, Timestamp(10));

        // Committed rows visible
        let rows = table.scan(TxnId(2), Timestamp(10));
        assert_eq!(rows.len(), 2);

        // Uncommitted rows from txn3
        let txn3 = TxnId(3);
        table
            .insert(
                OwnedRow::new(vec![Datum::Int32(3), Datum::Text("c".into())]),
                txn3,
            )
            .unwrap();

        // txn3 sees own write + committed
        let rows = table.scan(txn3, Timestamp(10));
        assert_eq!(rows.len(), 3);

        // Other txns don't see txn3's uncommitted write
        let rows = table.scan(TxnId(4), Timestamp(10));
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_update_and_delete() {
        let table = MemTable::new(test_schema());
        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("old".into())]);
        let pk = table.insert(row, txn1).unwrap();
        table.commit_txn(txn1, Timestamp(10));

        // Update
        let txn2 = TxnId(2);
        let new_row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("new".into())]);
        table.update(&pk, new_row, txn2).unwrap();
        table.commit_txn(txn2, Timestamp(20));

        let result = table.get(&pk, TxnId(3), Timestamp(20));
        assert_eq!(result.unwrap().values[1], Datum::Text("new".into()));

        // Delete
        let txn3 = TxnId(3);
        table.delete(&pk, txn3).unwrap();
        table.commit_txn(txn3, Timestamp(30));

        let result = table.get(&pk, TxnId(4), Timestamp(30));
        assert!(result.is_none());
    }

    #[test]
    fn test_abort_removes_writes() {
        let table = MemTable::new(test_schema());
        let txn1 = TxnId(1);
        table
            .insert(
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("committed".into())]),
                txn1,
            )
            .unwrap();
        table.commit_txn(txn1, Timestamp(10));

        let txn2 = TxnId(2);
        table
            .insert(
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("aborted".into())]),
                txn2,
            )
            .unwrap();
        table.abort_txn(txn2);

        let rows = table.scan(TxnId(3), Timestamp(20));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[1], Datum::Text("committed".into()));
    }
