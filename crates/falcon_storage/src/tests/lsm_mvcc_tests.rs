use std::sync::Arc;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

use crate::lsm::engine::LsmEngine;
use crate::lsm_table::LsmTable;

fn make_table() -> LsmTable {
    let schema = TableSchema {
        id: TableId(700),
        name: "lsm_test".into(),
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
        ..Default::default()
    };
    let engine = Arc::new(LsmEngine::open_in_memory().unwrap());
    LsmTable::new(schema, engine)
}

fn row(id: i64, val: &str) -> OwnedRow {
    OwnedRow::new(vec![Datum::Int64(id), Datum::Text(val.to_string())])
}

#[test]
fn test_lsm_prepared_invisible_to_other_txn() {
    let t = make_table();
    let r = row(1, "hello");
    t.insert(&r, TxnId(10)).unwrap();

    // Other txn can't see prepared row
    let scan = t.scan(TxnId(99), Timestamp(1000));
    assert!(
        scan.is_empty(),
        "prepared row must be invisible to other txns"
    );
}

#[test]
fn test_lsm_prepared_visible_to_own_txn() {
    let t = make_table();
    let r = row(1, "hello");
    let pk = t.insert(&r, TxnId(10)).unwrap();

    // Own txn sees its prepared write
    let got = t.get(&pk, TxnId(10), Timestamp(0)).unwrap();
    assert!(got.is_some(), "own prepared write must be visible");
    assert_eq!(got.unwrap().values[1], Datum::Text("hello".into()));
}

#[test]
fn test_lsm_committed_visible_after_commit_ts() {
    let t = make_table();
    let r = row(1, "committed");
    let pk = t.insert(&r, TxnId(10)).unwrap();
    t.commit(&pk, TxnId(10), Timestamp(100)).unwrap();

    // read_ts >= commit_ts → visible
    let got = t.get(&pk, TxnId(99), Timestamp(100)).unwrap();
    assert!(got.is_some());

    let got2 = t.get(&pk, TxnId(99), Timestamp(200)).unwrap();
    assert!(got2.is_some());

    // read_ts < commit_ts → invisible
    let got3 = t.get(&pk, TxnId(99), Timestamp(99)).unwrap();
    assert!(
        got3.is_none(),
        "committed row must be invisible before commit_ts"
    );
}

#[test]
fn test_lsm_aborted_invisible() {
    let t = make_table();
    let r = row(1, "aborted");
    let pk = t.insert(&r, TxnId(10)).unwrap();
    t.abort(&pk, TxnId(10)).unwrap();

    let got = t.get(&pk, TxnId(10), Timestamp(u64::MAX)).unwrap();
    assert!(got.is_none(), "aborted row must be invisible");

    let scan = t.scan(TxnId(10), Timestamp(u64::MAX));
    assert!(scan.is_empty());
}

#[test]
fn test_lsm_delete_tombstone() {
    let t = make_table();
    let r = row(1, "alive");
    let pk = t.insert(&r, TxnId(10)).unwrap();
    t.commit(&pk, TxnId(10), Timestamp(100)).unwrap();

    // Verify visible
    assert!(t.get(&pk, TxnId(99), Timestamp(200)).unwrap().is_some());

    // Delete + commit
    t.delete(&pk, TxnId(20)).unwrap();
    t.commit(&pk, TxnId(20), Timestamp(200)).unwrap();

    // Tombstone committed → invisible
    let got = t.get(&pk, TxnId(99), Timestamp(300)).unwrap();
    assert!(got.is_none(), "deleted row must be invisible after commit");
}

#[test]
fn test_lsm_update_replaces_version() {
    let t = make_table();
    let r = row(1, "v1");
    let pk = t.insert(&r, TxnId(10)).unwrap();
    t.commit(&pk, TxnId(10), Timestamp(100)).unwrap();

    // Update
    let r2 = row(1, "v2");
    t.update(&pk, &r2, TxnId(20)).unwrap();
    t.commit(&pk, TxnId(20), Timestamp(200)).unwrap();

    let got = t.get(&pk, TxnId(99), Timestamp(200)).unwrap();
    assert!(got.is_some());
    assert_eq!(got.unwrap().values[1], Datum::Text("v2".into()));
}

#[test]
fn test_lsm_scan_includes_sst_data() {
    let t = make_table();

    // Insert enough rows to trigger a flush to SST
    for i in 0..50 {
        let r = row(i, &format!("val_{}", i));
        let pk = t.insert(&r, TxnId(1)).unwrap();
        t.commit(&pk, TxnId(1), Timestamp(10)).unwrap();
    }

    // Force flush
    t.engine.flush().unwrap();

    // Insert more into memtable (not flushed)
    for i in 50..60 {
        let r = row(i, &format!("val_{}", i));
        let pk = t.insert(&r, TxnId(2)).unwrap();
        t.commit(&pk, TxnId(2), Timestamp(20)).unwrap();
    }

    let scan = t.scan(TxnId(99), Timestamp(100));
    assert_eq!(
        scan.len(),
        60,
        "scan must include both SST and memtable rows"
    );
}

#[test]
fn test_lsm_snapshot_isolation() {
    let t = make_table();

    // txn1 inserts and commits at ts=100
    let r = row(1, "original");
    let pk = t.insert(&r, TxnId(10)).unwrap();
    t.commit(&pk, TxnId(10), Timestamp(100)).unwrap();

    // txn2 updates and commits at ts=200
    let r2 = row(1, "updated");
    t.update(&pk, &r2, TxnId(20)).unwrap();
    t.commit(&pk, TxnId(20), Timestamp(200)).unwrap();

    // Reader at ts=200+ sees new version "updated"
    let got = t.get(&pk, TxnId(99), Timestamp(200)).unwrap();
    assert_eq!(got.unwrap().values[1], Datum::Text("updated".into()));

    // Reader at ts=99 can't see anything (both commits are after ts=99)
    let got2 = t.get(&pk, TxnId(99), Timestamp(99)).unwrap();
    assert!(got2.is_none());
}

#[test]
fn test_lsm_prepared_does_not_hide_committed() {
    let t = make_table();

    // txn1 inserts and commits at ts=100
    let pk = t.insert(&row(1, "v1"), TxnId(10)).unwrap();
    t.commit(&pk, TxnId(10), Timestamp(100)).unwrap();

    // txn2 starts an update (prepared, not yet committed)
    t.update(&pk, &row(1, "v2"), TxnId(20)).unwrap();

    // txn3 reads at ts=150: should see "v1" (committed), NOT nothing
    let got = t.get(&pk, TxnId(30), Timestamp(150)).unwrap();
    assert!(
        got.is_some(),
        "prepared update must not hide committed version"
    );
    assert_eq!(got.unwrap().values[1], Datum::Text("v1".into()));

    // txn2 (writer) sees its own prepared "v2"
    let own = t.get(&pk, TxnId(20), Timestamp(150)).unwrap();
    assert_eq!(own.unwrap().values[1], Datum::Text("v2".into()));
}

#[test]
fn test_lsm_abort_restores_old_version() {
    let t = make_table();

    let pk = t.insert(&row(1, "original"), TxnId(10)).unwrap();
    t.commit(&pk, TxnId(10), Timestamp(100)).unwrap();

    // txn2 updates then aborts
    t.update(&pk, &row(1, "aborted_val"), TxnId(20)).unwrap();
    t.abort(&pk, TxnId(20)).unwrap();

    // Original committed version should still be visible
    let got = t.get(&pk, TxnId(99), Timestamp(200)).unwrap();
    assert!(got.is_some(), "abort must restore old committed version");
    assert_eq!(got.unwrap().values[1], Datum::Text("original".into()));
}
