use std::sync::Arc;

use falcon_common::datum::Datum;
use falcon_executor::{ExecutionResult, Executor};
use falcon_storage::engine::StorageEngine;
use falcon_storage::partition::{PartitionBound, RangeBound};
use falcon_common::types::IsolationLevel;
use falcon_txn::manager::TxnManager;

fn setup() -> (Executor, Arc<StorageEngine>, Arc<TxnManager>) {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    let executor = Executor::new(storage.clone(), txn_mgr.clone());
    (executor, storage, txn_mgr)
}

fn run_sql(executor: &Executor, storage: &StorageEngine, sql: &str) -> ExecutionResult {
    let stmts = falcon_sql_frontend::parser::parse_sql(sql).expect("parse");
    let stmt = stmts.into_iter().next().unwrap();
    let catalog = storage.get_catalog();
    let mut binder = falcon_sql_frontend::binder::Binder::new(catalog);
    let bound = binder.bind(&stmt).expect("bind");
    let plan = falcon_planner::Planner::plan(&bound).expect("plan");
    executor.execute(&plan, None).expect("execute")
}

#[allow(dead_code)]
fn run_txn_sql(
    executor: &Executor,
    storage: &StorageEngine,
    txn_mgr: &TxnManager,
    sql: &str,
) -> ExecutionResult {
    let txn = txn_mgr.begin(IsolationLevel::Serializable);
    let stmts = falcon_sql_frontend::parser::parse_sql(sql).expect("parse");
    let stmt = stmts.into_iter().next().unwrap();
    let catalog = storage.get_catalog();
    let mut binder = falcon_sql_frontend::binder::Binder::new(catalog);
    let bound = binder.bind(&stmt).expect("bind");
    let plan = falcon_planner::Planner::plan(&bound).expect("plan");
    let result = executor.execute(&plan, Some(&txn)).expect("execute");
    txn_mgr.commit(txn.txn_id).unwrap();
    result
}

/// CREATE TABLE ... PARTITION BY RANGE parses and registers the partition spec.
#[test]
fn test_create_partitioned_table_range() {
    let (executor, storage, _txn) = setup();

    let result = run_sql(
        &executor,
        &storage,
        "CREATE TABLE orders (id INT PRIMARY KEY, order_date INT, amount DECIMAL) \
         PARTITION BY RANGE (order_date)",
    );
    assert!(
        matches!(result, ExecutionResult::Ddl { .. }),
        "expected DDL result"
    );

    let schema = storage.get_table_schema("orders").expect("orders table should exist");
    assert_eq!(schema.name, "orders");

    // The partition manager should know this table is partitioned.
    assert!(
        storage.is_partitioned(schema.id),
        "orders should be marked as partitioned"
    );
    assert_eq!(
        storage.partition_key_idx(schema.id),
        Some(1),
        "partition key idx should be column 1 (order_date)"
    );
}

/// CREATE TABLE ... PARTITION BY LIST registers correctly.
#[test]
fn test_create_partitioned_table_list() {
    let (executor, storage, _txn) = setup();

    run_sql(
        &executor,
        &storage,
        "CREATE TABLE regions (id INT PRIMARY KEY, region TEXT) PARTITION BY LIST (region)",
    );

    let schema = storage.get_table_schema("regions").unwrap();
    assert!(storage.is_partitioned(schema.id));
    assert_eq!(storage.partition_key_idx(schema.id), Some(1));
}

/// CREATE TABLE ... PARTITION BY HASH registers correctly.
#[test]
fn test_create_partitioned_table_hash() {
    let (executor, storage, _txn) = setup();

    run_sql(
        &executor,
        &storage,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) PARTITION BY HASH (id)",
    );

    let schema = storage.get_table_schema("users").unwrap();
    assert!(storage.is_partitioned(schema.id));
    assert_eq!(storage.partition_key_idx(schema.id), Some(0));
}

/// Non-partitioned table should NOT be registered in PartitionManager.
#[test]
fn test_regular_table_not_partitioned() {
    let (executor, storage, _txn) = setup();

    run_sql(
        &executor,
        &storage,
        "CREATE TABLE plain (id INT PRIMARY KEY, val TEXT)",
    );

    let schema = storage.get_table_schema("plain").unwrap();
    assert!(
        !storage.is_partitioned(schema.id),
        "plain table should NOT be partitioned"
    );
}

/// INSERT into a partitioned table with a range partition routes to child table.
#[test]
fn test_insert_routes_to_range_partition() {
    let (executor, storage, txn_mgr) = setup();

    run_sql(
        &executor,
        &storage,
        "CREATE TABLE sales (id INT PRIMARY KEY, month INT, amount INT) \
         PARTITION BY RANGE (month)",
    );

    let schema = storage.get_table_schema("sales").unwrap();

    // Manually register two range partitions: [1,7) and [7,13)
    {
        let child1_id = falcon_common::types::TableId(9001);
        let child2_id = falcon_common::types::TableId(9002);

        storage.add_partition_to_table(
            schema.id,
            "sales_h1".into(),
            child1_id,
            PartitionBound::Range(RangeBound::new(
                Some(Datum::Int32(1)),
                Some(Datum::Int32(7)),
            )),
        );
        storage.add_partition_to_table(
            schema.id,
            "sales_h2".into(),
            child2_id,
            PartitionBound::Range(RangeBound::new(
                Some(Datum::Int32(7)),
                Some(Datum::Int32(13)),
            )),
        );

        // Verify routing: month=3 -> child1, month=9 -> child2
        assert_eq!(
            storage.route_partition(schema.id, &Datum::Int32(3)),
            Some(child1_id),
        );
        assert_eq!(
            storage.route_partition(schema.id, &Datum::Int32(9)),
            Some(child2_id),
        );
    }
}
