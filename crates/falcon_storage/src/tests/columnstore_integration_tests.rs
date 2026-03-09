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

    // Uncommitted rows visible to owning txn
    let rows = engine.scan(TableId(500), TxnId(1), Timestamp(100)).unwrap();
    assert_eq!(rows.len(), 2, "both rows must be visible to owning txn");

    // Not visible to other txns until committed
    let rows2 = engine.scan(TableId(500), TxnId(2), Timestamp(100)).unwrap();
    assert_eq!(rows2.len(), 0, "uncommitted rows invisible to other txns");
}

#[test]
fn test_columnstore_scan_respects_mvcc_read_ts() {
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

    // Owning txn sees all 5 rows
    let rows_owner = engine.scan(TableId(500), TxnId(1), Timestamp(0)).unwrap();
    assert_eq!(rows_owner.len(), 5, "owning txn sees its uncommitted rows");

    // Other txn at any read_ts sees 0 (uncommitted)
    let rows_other = engine.scan(TableId(500), TxnId(2), Timestamp(u64::MAX)).unwrap();
    assert_eq!(rows_other.len(), 0, "uncommitted rows invisible to others");
}

#[test]
fn test_columnstore_update_not_supported() {
    let engine = analytics_engine();
    engine.create_table(cs_schema()).unwrap();

    let row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("v1".into())]);
    let pk = engine.insert(TableId(500), row.clone(), TxnId(1)).unwrap();

    let result = engine.update(TableId(500), &pk, row, TxnId(2));
    assert!(
        result.is_err(),
        "UPDATE on COLUMNSTORE must return an error"
    );
}

#[test]
fn test_columnstore_delete_supported() {
    let engine = analytics_engine();
    engine.create_table(cs_schema()).unwrap();

    let row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("v1".into())]);
    let pk = engine.insert(TableId(500), row, TxnId(1)).unwrap();

    // DELETE is now supported on COLUMNSTORE
    let result = engine.delete(TableId(500), &pk, TxnId(2));
    assert!(result.is_ok(), "DELETE on COLUMNSTORE should succeed");
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
fn test_columnstore_mvcc_commit_visibility() {
    let engine = analytics_engine();
    engine.create_table(cs_schema()).unwrap();

    engine
        .insert(
            TableId(500),
            OwnedRow::new(vec![Datum::Int64(1), Datum::Text("x".into())]),
            TxnId(1),
        )
        .unwrap();

    // Visible to owning txn before commit
    let before = engine
        .scan(TableId(500), TxnId(1), Timestamp(100))
        .unwrap();
    assert_eq!(before.len(), 1);

    // Not visible to other txn before commit
    let other_before = engine
        .scan(TableId(500), TxnId(99), Timestamp(100))
        .unwrap();
    assert_eq!(other_before.len(), 0);
}
