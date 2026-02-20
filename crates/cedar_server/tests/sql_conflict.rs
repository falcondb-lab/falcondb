#![allow(clippy::approx_constant, clippy::useless_vec)]

mod common;
use common::*;

#[test]
fn test_analyze_table() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor, "CREATE TABLE stats_test (id INT PRIMARY KEY, name TEXT, score INT)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO stats_test VALUES (1, 'alice', 90)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO stats_test VALUES (2, 'bob', 80)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO stats_test VALUES (3, 'alice', 95)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor, "ANALYZE TABLE stats_test", None).unwrap();
    match result {
        ExecutionResult::Query { rows, columns } => {
            assert_eq!(columns[0].0, "table_name");
            assert_eq!(rows[0].values[0], Datum::Text("stats_test".into()));
            assert_eq!(rows[0].values[1], Datum::Int64(3)); // row_count
            assert_eq!(rows[0].values[2], Datum::Int64(3)); // 3 columns analyzed
        }
        _ => panic!("Expected Query result from ANALYZE"),
    }
}

#[test]
fn test_show_table_stats_after_analyze() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor, "CREATE TABLE ts_test (id INT PRIMARY KEY, name TEXT)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO ts_test VALUES (1, 'x')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO ts_test VALUES (2, 'y')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Before ANALYZE, SHOW cedar.table_stats returns no rows
    let result = run_sql(&storage, &txn_mgr, &executor, "SHOW cedar.table_stats", None).unwrap();
    match &result {
        ExecutionResult::Query { rows, .. } => assert!(rows.is_empty(), "no stats before ANALYZE"),
        _ => panic!("Expected Query"),
    }

    // Run ANALYZE
    run_sql(&storage, &txn_mgr, &executor, "ANALYZE TABLE ts_test", None).unwrap();

    // Now SHOW cedar.table_stats should have data
    let result = run_sql(&storage, &txn_mgr, &executor, "SHOW cedar.table_stats", None).unwrap();
    match result {
        ExecutionResult::Query { rows, columns } => {
            assert_eq!(columns[0].0, "table_name");
            assert_eq!(columns[1].0, "column_name");
            assert_eq!(columns[3].0, "distinct_count");
            // Should have 2 rows (one per column: id, name)
            assert_eq!(rows.len(), 2);
            // id column: 2 distinct values
            assert_eq!(rows[0].values[3], Datum::Int64(2));
            // name column: 2 distinct values
            assert_eq!(rows[1].values[3], Datum::Int64(2));
        }
        _ => panic!("Expected Query result from SHOW cedar.table_stats"),
    }
}

#[test]
fn test_analyze_read_only_rejected() {
    let (storage, txn_mgr, executor) = setup();
    // Create table with normal (read-write) executor
    run_sql(&storage, &txn_mgr, &executor, "CREATE TABLE ro_analyze (id INT PRIMARY KEY)", None).unwrap();
    // Now use a read-only executor for ANALYZE
    let ro_executor = Arc::new(Executor::new_read_only(Arc::clone(&storage), Arc::clone(&txn_mgr)));
    let result = run_sql(&storage, &txn_mgr, &ro_executor, "ANALYZE TABLE ro_analyze", None);
    // ANALYZE should succeed even on read-only executor since it's a read operation
    assert!(result.is_ok(), "ANALYZE should work on read-only replicas");
}

// 鈹€鈹€ Cost-based join ordering tests 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

/// Helper: plan with table statistics for cost-based join reordering.
fn run_sql_with_stats(
    storage: &Arc<StorageEngine>,
    _txn_mgr: &Arc<TxnManager>,
    executor: &Arc<Executor>,
    sql: &str,
    txn: Option<&cedar_txn::TxnHandle>,
) -> Result<cedar_executor::ExecutionResult, cedar_common::error::CedarError> {
    let stmts = parse_sql(sql).map_err(cedar_common::error::CedarError::Sql)?;
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(catalog);
    let bound = binder
        .bind(&stmts[0])
        .map_err(cedar_common::error::CedarError::Sql)?;
    // Build row counts from cached stats
    let all_stats = storage.get_all_table_stats();
    let mut row_counts = cedar_planner::TableRowCounts::new();
    for ts in &all_stats {
        row_counts.insert(ts.table_id, ts.row_count);
    }
    let plan = Planner::plan_with_stats(&bound, &row_counts)
        .map_err(cedar_common::error::CedarError::Sql)?;
    executor.execute(&plan, txn)
}

#[test]
fn test_cost_based_join_reorder_correct_results() {
    // Verify that cost-based join reordering produces correct query results.
    // Create a star schema: fact table (large) + two dimension tables (small).
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE products (pid INT PRIMARY KEY, pname TEXT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE regions (rid INT PRIMARY KEY, rname TEXT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE sales (sid INT PRIMARY KEY, product_id INT, region_id INT, amount INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    // Small dimension: 2 products
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO products VALUES (1, 'Widget')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO products VALUES (2, 'Gadget')", Some(&txn)).unwrap();
    // Small dimension: 2 regions
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO regions VALUES (10, 'East')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO regions VALUES (20, 'West')", Some(&txn)).unwrap();
    // Larger fact table: 5 sales
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO sales VALUES (100, 1, 10, 50)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO sales VALUES (101, 2, 20, 75)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO sales VALUES (102, 1, 20, 30)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO sales VALUES (103, 2, 10, 90)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO sales VALUES (104, 1, 10, 60)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // ANALYZE all tables
    run_sql(&storage, &txn_mgr, &executor, "ANALYZE TABLE products", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "ANALYZE TABLE regions", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "ANALYZE TABLE sales", None).unwrap();

    // Query with cost-based planning (stats available 鈫?reordering may occur)
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql_with_stats(&storage, &txn_mgr, &executor,
        "SELECT pname, rname, amount FROM sales \
         INNER JOIN products ON product_id = pid \
         INNER JOIN regions ON region_id = rid \
         ORDER BY amount",
        Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();

    match result {
        ExecutionResult::Query { rows, columns } => {
            assert_eq!(columns.len(), 3);
            assert_eq!(rows.len(), 5, "Should return 5 joined rows");
            // Verify ordering by amount: 30, 50, 60, 75, 90
            let amounts: Vec<&Datum> = rows.iter().map(|r| &r.values[2]).collect();
            assert_eq!(amounts, vec![
                &Datum::Int32(30), &Datum::Int32(50), &Datum::Int32(60),
                &Datum::Int32(75), &Datum::Int32(90)
            ]);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_cost_based_join_without_stats_still_works() {
    // Without ANALYZE, plan_with_stats should fall back to default ordering.
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t_a (id INT PRIMARY KEY, val TEXT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t_b (id INT PRIMARY KEY, a_id INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO t_a VALUES (1, 'hello')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO t_b VALUES (10, 1)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // No ANALYZE 鈥?stats are empty, should still work correctly
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql_with_stats(&storage, &txn_mgr, &executor,
        "SELECT val FROM t_a INNER JOIN t_b ON t_a.id = t_b.a_id",
        Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("hello".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_cost_based_join_left_join_not_reordered() {
    // LEFT JOINs must not be reordered (semantics depend on order).
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE authors2 (aid INT PRIMARY KEY, aname TEXT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE books2 (bid INT PRIMARY KEY, author_fk INT, title TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO authors2 VALUES (1, 'Alice')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO authors2 VALUES (2, 'Bob')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO books2 VALUES (10, 1, 'Book A')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    run_sql(&storage, &txn_mgr, &executor, "ANALYZE TABLE authors2", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "ANALYZE TABLE books2", None).unwrap();

    // LEFT JOIN: Bob should appear with NULL book
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql_with_stats(&storage, &txn_mgr, &executor,
        "SELECT aname, title FROM authors2 LEFT JOIN books2 ON aid = author_fk ORDER BY aname",
        Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Alice".into()));
            assert_eq!(rows[0].values[1], Datum::Text("Book A".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Bob".into()));
            assert!(rows[1].values[1].is_null(), "Bob has no books, should be NULL");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_explain_analyze_select() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE ea_t (id INT PRIMARY KEY, val TEXT)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO ea_t VALUES (1, 'a')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO ea_t VALUES (2, 'b')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO ea_t VALUES (3, 'c')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "EXPLAIN ANALYZE SELECT * FROM ea_t", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();

    match result {
        ExecutionResult::Query { rows, columns } => {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].0, "QUERY PLAN");
            // Should have plan lines + one "Actual:" summary line
            assert!(rows.len() >= 2, "Should have plan + actual metrics");
            let last_line = match &rows.last().unwrap().values[0] {
                Datum::Text(s) => s.clone(),
                _ => panic!("Expected text"),
            };
            assert!(last_line.starts_with("Actual:"), "Last line should start with 'Actual:'");
            assert!(last_line.contains("rows=3"), "Should report 3 actual rows");
            assert!(last_line.contains("time="), "Should include timing");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_explain_analyze_dml() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE ea_dml (id INT PRIMARY KEY, val INT)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO ea_dml VALUES (1, 10)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO ea_dml VALUES (2, 20)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // EXPLAIN ANALYZE on UPDATE should execute the update and report rows affected
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "EXPLAIN ANALYZE UPDATE ea_dml SET val = 99 WHERE id = 1", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            assert!(rows.len() >= 2, "Should have plan + actual metrics");
            let last_line = match &rows.last().unwrap().values[0] {
                Datum::Text(s) => s.clone(),
                _ => panic!("Expected text"),
            };
            assert!(last_line.starts_with("Actual:"), "Last line should start with 'Actual:'");
            assert!(last_line.contains("rows=1"), "Should report 1 affected row");
        }
        _ => panic!("Expected Query result"),
    }

    // Verify the update actually happened
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let verify = run_sql(&storage, &txn_mgr, &executor,
        "SELECT val FROM ea_dml WHERE id = 1", Some(&txn3)).unwrap();
    txn_mgr.commit(txn3.txn_id).unwrap();
    match verify {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int32(99));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_explain_vs_explain_analyze() {
    // EXPLAIN should NOT execute the query; EXPLAIN ANALYZE SHOULD execute it
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE ea_cmp (id INT PRIMARY KEY, val INT)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO ea_cmp VALUES (1, 10)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Plain EXPLAIN: no "Actual:" line
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let explain_result = run_sql(&storage, &txn_mgr, &executor,
        "EXPLAIN SELECT * FROM ea_cmp", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();
    match &explain_result {
        ExecutionResult::Query { rows, .. } => {
            for row in rows {
                if let Datum::Text(s) = &row.values[0] {
                    assert!(!s.starts_with("Actual:"), "Plain EXPLAIN should not have Actual metrics");
                }
            }
        }
        _ => panic!("Expected Query result"),
    }

    // EXPLAIN ANALYZE: has "Actual:" line
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let analyze_result = run_sql(&storage, &txn_mgr, &executor,
        "EXPLAIN ANALYZE SELECT * FROM ea_cmp", Some(&txn3)).unwrap();
    txn_mgr.commit(txn3.txn_id).unwrap();
    match &analyze_result {
        ExecutionResult::Query { rows, .. } => {
            let last_line = match &rows.last().unwrap().values[0] {
                Datum::Text(s) => s.clone(),
                _ => panic!("Expected text"),
            };
            assert!(last_line.starts_with("Actual:"), "EXPLAIN ANALYZE should have Actual metrics");
        }
        _ => panic!("Expected Query result"),
    }
}

// 鈹€鈹€ JSONB Integration Tests 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

#[test]
fn test_jsonb_create_table_and_insert() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE jdocs (id INT PRIMARY KEY, data JSONB)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jdocs VALUES (1, '{\"name\": \"Alice\", \"age\": 30}'::jsonb)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, data FROM jdocs", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(matches!(&rows[0].values[1], Datum::Jsonb(_)));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_jsonb_arrow_operator() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE jtest (id INT PRIMARY KEY, data JSONB)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jtest VALUES (1, '{\"name\": \"Bob\", \"scores\": [10, 20, 30]}'::jsonb)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // -> operator: get field as jsonb
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT data -> 'name' FROM jtest", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0].values[0] {
                Datum::Jsonb(v) => assert_eq!(v, &serde_json::json!("Bob")),
                other => panic!("Expected Jsonb, got {:?}", other),
            }
        }
        _ => panic!("Expected Query result"),
    }

    // ->> operator: get field as text
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT data ->> 'name' FROM jtest", Some(&txn3)).unwrap();
    txn_mgr.commit(txn3.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(&rows[0].values[0], &Datum::Text("Bob".to_string()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_jsonb_contains_operator() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE jcontain (id INT PRIMARY KEY, data JSONB)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jcontain VALUES (1, '{\"a\": 1, \"b\": 2, \"c\": 3}'::jsonb)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jcontain VALUES (2, '{\"x\": 10}'::jsonb)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // @> operator: contains
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id FROM jcontain WHERE data @> '{\"a\": 1}'::jsonb", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(&rows[0].values[0], &Datum::Int32(1));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_jsonb_exists_operator() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE jexist (id INT PRIMARY KEY, data JSONB)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jexist VALUES (1, '{\"name\": \"Alice\"}'::jsonb)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jexist VALUES (2, '{\"age\": 25}'::jsonb)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // ? operator: key exists
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id FROM jexist WHERE data ? 'name'", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(&rows[0].values[0], &Datum::Int32(1));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_jsonb_functions() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE jfunc (id INT PRIMARY KEY)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jfunc VALUES (1)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // jsonb_build_object
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT jsonb_build_object('a', 1, 'b', 'hello') FROM jfunc", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0].values[0] {
                Datum::Jsonb(v) => {
                    assert_eq!(v["a"], serde_json::json!(1));
                    assert_eq!(v["b"], serde_json::json!("hello"));
                }
                other => panic!("Expected Jsonb, got {:?}", other),
            }
        }
        _ => panic!("Expected Query result"),
    }

    // jsonb_build_array
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT jsonb_build_array(1, 2, 3) FROM jfunc", Some(&txn3)).unwrap();
    txn_mgr.commit(txn3.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0].values[0] {
                Datum::Jsonb(v) => assert_eq!(v, &serde_json::json!([1, 2, 3])),
                other => panic!("Expected Jsonb, got {:?}", other),
            }
        }
        _ => panic!("Expected Query result"),
    }

    // jsonb_typeof
    let txn4 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT jsonb_typeof('{\"a\":1}'::jsonb) FROM jfunc", Some(&txn4)).unwrap();
    txn_mgr.commit(txn4.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(&rows[0].values[0], &Datum::Text("object".to_string()));
        }
        _ => panic!("Expected Query result"),
    }

    // jsonb_array_length
    let txn5 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT jsonb_array_length('[1,2,3,4]'::jsonb) FROM jfunc", Some(&txn5)).unwrap();
    txn_mgr.commit(txn5.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(&rows[0].values[0], &Datum::Int32(4));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_jsonb_extract_path() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE jpath (id INT PRIMARY KEY, data JSONB)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jpath VALUES (1, '{\"a\": {\"b\": {\"c\": 42}}}'::jsonb)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT jsonb_extract_path_text(data, 'a', 'b', 'c') FROM jpath", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(&rows[0].values[0], &Datum::Text("42".to_string()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_jsonb_strip_nulls_and_pretty() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE jstrip (id INT PRIMARY KEY)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jstrip VALUES (1)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // jsonb_strip_nulls
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT jsonb_strip_nulls('{\"a\": 1, \"b\": null, \"c\": 3}'::jsonb) FROM jstrip", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0].values[0] {
                Datum::Jsonb(v) => {
                    assert!(v.get("a").is_some());
                    assert!(v.get("b").is_none()); // null stripped
                    assert!(v.get("c").is_some());
                }
                other => panic!("Expected Jsonb, got {:?}", other),
            }
        }
        _ => panic!("Expected Query result"),
    }

    // jsonb_pretty
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT jsonb_pretty('{\"a\":1}'::jsonb) FROM jstrip", Some(&txn3)).unwrap();
    txn_mgr.commit(txn3.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0].values[0] {
                Datum::Text(s) => assert!(s.contains("\"a\""), "Expected pretty-printed JSON"),
                other => panic!("Expected Text, got {:?}", other),
            }
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_jsonb_cast_text_to_jsonb() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE jcast (id INT PRIMARY KEY)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jcast VALUES (1)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CAST('{\"key\": \"value\"}' AS JSONB) FROM jcast", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0].values[0] {
                Datum::Jsonb(v) => assert_eq!(v["key"], serde_json::json!("value")),
                other => panic!("Expected Jsonb, got {:?}", other),
            }
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_jsonb_object_keys() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE jkeys (id INT PRIMARY KEY)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jkeys VALUES (1)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT jsonb_object_keys('{\"a\": 1, \"b\": 2}'::jsonb) FROM jkeys", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0].values[0] {
                Datum::Array(arr) => {
                    assert_eq!(arr.len(), 2);
                }
                other => panic!("Expected Array, got {:?}", other),
            }
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_jsonb_to_jsonb() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE jtoj (id INT PRIMARY KEY, name TEXT)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO jtoj VALUES (1, 'hello')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT to_jsonb(name) FROM jtoj", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0].values[0] {
                Datum::Jsonb(v) => assert_eq!(v, &serde_json::json!("hello")),
                other => panic!("Expected Jsonb, got {:?}", other),
            }
        }
        _ => panic!("Expected Query result"),
    }
}

// 鈹€鈹€鈹€ Correlated Subquery Tests 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

#[test]
fn test_correlated_exists_subquery() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Setup: departments and employees
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT, name TEXT)", Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (1, 1, 'Alice'), (2, 1, 'Bob'), (3, 2, 'Carol')", Some(&txn)).unwrap();

    // Correlated EXISTS: departments that have at least one employee
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM departments WHERE EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id)",
        Some(&txn)).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            let names: Vec<&str> = rows.iter()
                .map(|r| match &r.values[0] { Datum::Text(s) => s.as_str(), _ => "" })
                .collect();
            assert!(names.contains(&"Engineering"), "Engineering should have employees");
            assert!(names.contains(&"Marketing"), "Marketing should have employees");
            assert!(!names.contains(&"Sales"), "Sales should NOT have employees");
            assert_eq!(names.len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_correlated_not_exists_subquery() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT, name TEXT)", Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (1, 1, 'Alice'), (2, 2, 'Carol')", Some(&txn)).unwrap();

    // NOT EXISTS: departments with no employees
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM departments WHERE NOT EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id)",
        Some(&txn)).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("Sales".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_correlated_in_subquery() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50)", Some(&txn)).unwrap();

    // Customers who have at least one order
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders)",
        Some(&txn)).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            let names: Vec<&str> = rows.iter()
                .map(|r| match &r.values[0] { Datum::Text(s) => s.as_str(), _ => "" })
                .collect();
            assert!(names.contains(&"Alice"));
            assert!(names.contains(&"Bob"));
            assert!(!names.contains(&"Carol"));
            assert_eq!(names.len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_correlated_scalar_subquery_in_where() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, category TEXT)", Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO products VALUES (1, 'Widget', 100, 'A'), (2, 'Gadget', 200, 'A'), (3, 'Doohickey', 50, 'B'), (4, 'Thingamajig', 75, 'B')",
        Some(&txn)).unwrap();

    // Products whose price is above the average price in their category
    // This is a classic correlated scalar subquery
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM products p1 WHERE price > (SELECT AVG(price) FROM products p2 WHERE p2.category = p1.category)",
        Some(&txn)).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            let names: Vec<&str> = rows.iter()
                .map(|r| match &r.values[0] { Datum::Text(s) => s.as_str(), _ => "" })
                .collect();
            // Category A: avg=150, Gadget(200) > 150 鉁? Widget(100) < 150 鉁?            // Category B: avg=62.5, Thingamajig(75) > 62.5 鉁? Doohickey(50) < 62.5 鉁?            assert!(names.contains(&"Gadget"), "Gadget (200) > avg(A)=150");
            assert!(names.contains(&"Thingamajig"), "Thingamajig (75) > avg(B)=62.5");
            assert_eq!(names.len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_correlated_scalar_subquery_in_select() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT, name TEXT)", Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Marketing')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (1, 1, 'Alice'), (2, 1, 'Bob'), (3, 2, 'Carol')", Some(&txn)).unwrap();

    // Scalar subquery in SELECT: count of employees per department
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, (SELECT COUNT(*) FROM employees WHERE employees.dept_id = departments.id) FROM departments ORDER BY name",
        Some(&txn)).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            // Engineering has 2 employees, Marketing has 1
            let eng = rows.iter().find(|r| r.values[0] == Datum::Text("Engineering".into())).unwrap();
            let mkt = rows.iter().find(|r| r.values[0] == Datum::Text("Marketing".into())).unwrap();
            assert_eq!(eng.values[1], Datum::Int64(2));
            assert_eq!(mkt.values[1], Datum::Int64(1));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_correlated_exists_empty_result() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT)", Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')", Some(&txn)).unwrap();
    // t2 is empty 鈥?no rows reference t1

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT val FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)",
        Some(&txn)).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 0, "No rows should match when t2 is empty");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_correlated_subquery_with_null_handling() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT)", Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO parent VALUES (1, 'A'), (2, 'B')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO child VALUES (1, 1), (2, NULL)", Some(&txn)).unwrap();

    // Parent with children (child.parent_id = parent.id), NULL parent_id should not match
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM parent WHERE EXISTS (SELECT 1 FROM child WHERE child.parent_id = parent.id)",
        Some(&txn)).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("A".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

// 鈹€鈹€ FK Cascading Action Tests 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

#[test]
fn test_fk_on_delete_cascade() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT REFERENCES departments(id) ON DELETE CASCADE)",
        Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO departments VALUES (1, 'Engineering')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO departments VALUES (2, 'Sales')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO employees VALUES (10, 'Alice', 1)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO employees VALUES (20, 'Bob', 1)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO employees VALUES (30, 'Carol', 2)", Some(&txn)).unwrap();

    // Delete Engineering dept 鈥?should cascade-delete Alice and Bob
    run_sql(&storage, &txn_mgr, &executor, "DELETE FROM departments WHERE id = 1", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor, "SELECT * FROM employees ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1, "Only Carol should remain");
            assert_eq!(rows[0].values[1], Datum::Text("Carol".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_fk_on_delete_set_null() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT REFERENCES departments(id) ON DELETE SET NULL)",
        Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO departments VALUES (1, 'Engineering')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO employees VALUES (10, 'Alice', 1)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO employees VALUES (20, 'Bob', 1)", Some(&txn)).unwrap();

    // Delete dept 鈥?should set dept_id to NULL
    run_sql(&storage, &txn_mgr, &executor, "DELETE FROM departments WHERE id = 1", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor, "SELECT id, name, dept_id FROM employees ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert!(rows[0].values[2].is_null(), "Alice's dept_id should be NULL");
            assert!(rows[1].values[2].is_null(), "Bob's dept_id should be NULL");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_fk_on_delete_restrict() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT REFERENCES departments(id) ON DELETE RESTRICT)",
        Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO departments VALUES (1, 'Engineering')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO employees VALUES (10, 'Alice', 1)", Some(&txn)).unwrap();

    // Delete should fail because children exist
    let result = run_sql(&storage, &txn_mgr, &executor, "DELETE FROM departments WHERE id = 1", Some(&txn));
    assert!(result.is_err(), "DELETE should be rejected when RESTRICT is set");
}

#[test]
fn test_fk_on_update_cascade() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT REFERENCES departments(id) ON UPDATE CASCADE)",
        Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO departments VALUES (1, 'Engineering')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO employees VALUES (10, 'Alice', 1)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO employees VALUES (20, 'Bob', 1)", Some(&txn)).unwrap();

    // Update dept id 鈥?should cascade to employees
    run_sql(&storage, &txn_mgr, &executor, "UPDATE departments SET id = 100 WHERE id = 1", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor, "SELECT id, dept_id FROM employees ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[1], Datum::Int32(100), "Alice's dept_id should be updated to 100");
            assert_eq!(rows[1].values[1], Datum::Int32(100), "Bob's dept_id should be updated to 100");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_fk_on_update_set_null() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT REFERENCES departments(id) ON UPDATE SET NULL)",
        Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO departments VALUES (1, 'Engineering')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO employees VALUES (10, 'Alice', 1)", Some(&txn)).unwrap();

    // Update dept id 鈥?should set employee dept_id to NULL
    run_sql(&storage, &txn_mgr, &executor, "UPDATE departments SET id = 100 WHERE id = 1", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor, "SELECT id, dept_id FROM employees ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(rows[0].values[1].is_null(), "Alice's dept_id should be NULL");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_fk_cascade_multi_level() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Three-level hierarchy: company -> department -> employee
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE companies (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT, company_id INT REFERENCES companies(id) ON DELETE CASCADE)",
        Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT REFERENCES departments(id) ON DELETE CASCADE)",
        Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO companies VALUES (1, 'Acme')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO departments VALUES (10, 'Engineering', 1)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO departments VALUES (20, 'Sales', 1)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO employees VALUES (100, 'Alice', 10)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO employees VALUES (200, 'Bob', 20)", Some(&txn)).unwrap();

    // Delete company 鈥?should cascade to departments AND employees
    run_sql(&storage, &txn_mgr, &executor, "DELETE FROM companies WHERE id = 1", Some(&txn)).unwrap();

    let dept_result = run_sql(&storage, &txn_mgr, &executor, "SELECT * FROM departments", Some(&txn)).unwrap();
    match dept_result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 0, "All departments should be cascade-deleted");
        }
        _ => panic!("Expected Query result"),
    }

    let emp_result = run_sql(&storage, &txn_mgr, &executor, "SELECT * FROM employees", Some(&txn)).unwrap();
    match emp_result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 0, "All employees should be cascade-deleted");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_fk_table_level_constraint_cascade() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT, product TEXT, \
         FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE ON UPDATE CASCADE)",
        Some(&txn)).unwrap();

    // --- Test UPDATE CASCADE via table-level FK ---
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO orders VALUES (1, 'Alice')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO order_items VALUES (10, 1, 'Widget')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO order_items VALUES (20, 1, 'Gadget')", Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor, "UPDATE orders SET id = 99 WHERE id = 1", Some(&txn)).unwrap();
    let result = run_sql(&storage, &txn_mgr, &executor, "SELECT id, order_id FROM order_items ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[1], Datum::Int32(99));
            assert_eq!(rows[1].values[1], Datum::Int32(99));
        }
        _ => panic!("Expected Query result"),
    }

    // --- Test DELETE CASCADE via table-level FK (separate order to avoid PK-update limitation) ---
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO orders VALUES (2, 'Bob')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO order_items VALUES (30, 2, 'Gizmo')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO order_items VALUES (40, 2, 'Doohickey')", Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor, "DELETE FROM orders WHERE id = 2", Some(&txn)).unwrap();
    let result = run_sql(&storage, &txn_mgr, &executor, "SELECT * FROM order_items WHERE order_id = 2", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 0, "Bob's order items should be cascade-deleted");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_fk_no_action_default() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Default FK with no action specified 鈥?should behave like RESTRICT/NO ACTION
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE children (id INT PRIMARY KEY, parent_id INT REFERENCES parents(id))",
        Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO parents VALUES (1, 'Parent')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO children VALUES (10, 1)", Some(&txn)).unwrap();

    // Delete should fail 鈥?default is NO ACTION
    let result = run_sql(&storage, &txn_mgr, &executor, "DELETE FROM parents WHERE id = 1", Some(&txn));
    assert!(result.is_err(), "DELETE should be rejected with default NO ACTION");
}

// 鈹€鈹€鈹€ ALTER COLUMN TYPE tests 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

#[test]
fn test_alter_column_type_int_to_bigint() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t1 (id INT PRIMARY KEY, val INT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t1 VALUES (1, 42)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t1 VALUES (2, 100)", Some(&txn)).unwrap();

    // Change val from INT to BIGINT
    run_sql(&storage, &txn_mgr, &executor,
        "ALTER TABLE t1 ALTER COLUMN val SET DATA TYPE BIGINT", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, val FROM t1 ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[1], Datum::Int64(42));
            assert_eq!(rows[1].values[1], Datum::Int64(100));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_alter_column_type_int_to_text() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t2 (id INT PRIMARY KEY, code INT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t2 VALUES (1, 99)", Some(&txn)).unwrap();

    // Change code from INT to TEXT
    run_sql(&storage, &txn_mgr, &executor,
        "ALTER TABLE t2 ALTER COLUMN code SET DATA TYPE TEXT", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, code FROM t2 ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[1], Datum::Text("99".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_alter_column_type_text_to_int() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t3 (id INT PRIMARY KEY, num TEXT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t3 VALUES (1, '123')", Some(&txn)).unwrap();

    // Change num from TEXT to INT
    run_sql(&storage, &txn_mgr, &executor,
        "ALTER TABLE t3 ALTER COLUMN num SET DATA TYPE INT", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, num FROM t3 ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[1], Datum::Int32(123));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_alter_column_type_preserves_null() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t4 (id INT PRIMARY KEY, val INT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t4 VALUES (1, NULL)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t4 VALUES (2, 5)", Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor,
        "ALTER TABLE t4 ALTER COLUMN val SET DATA TYPE BIGINT", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, val FROM t4 ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert!(rows[0].values[1].is_null(), "NULL should be preserved after type change");
            assert_eq!(rows[1].values[1], Datum::Int64(5));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_alter_column_set_drop_not_null() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t5 (id INT PRIMARY KEY, name TEXT)", Some(&txn)).unwrap();

    // SET NOT NULL
    run_sql(&storage, &txn_mgr, &executor,
        "ALTER TABLE t5 ALTER COLUMN name SET NOT NULL", Some(&txn)).unwrap();

    // Inserting NULL should fail now
    let result = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t5 VALUES (1, NULL)", Some(&txn));
    assert!(result.is_err(), "INSERT NULL should fail after SET NOT NULL");

    // DROP NOT NULL
    run_sql(&storage, &txn_mgr, &executor,
        "ALTER TABLE t5 ALTER COLUMN name DROP NOT NULL", Some(&txn)).unwrap();

    // Inserting NULL should succeed now
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t5 VALUES (1, NULL)", Some(&txn)).unwrap();
}

#[test]
fn test_alter_column_set_drop_default() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t6 (id INT PRIMARY KEY, status TEXT)", Some(&txn)).unwrap();

    // SET DEFAULT
    run_sql(&storage, &txn_mgr, &executor,
        "ALTER TABLE t6 ALTER COLUMN status SET DEFAULT 'active'", Some(&txn)).unwrap();

    // Insert without specifying status 鈥?should get default
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t6 (id) VALUES (1)", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, status FROM t6 WHERE id = 1", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[1], Datum::Text("active".into()));
        }
        _ => panic!("Expected Query result"),
    }

    // DROP DEFAULT
    run_sql(&storage, &txn_mgr, &executor,
        "ALTER TABLE t6 ALTER COLUMN status DROP DEFAULT", Some(&txn)).unwrap();

    // Insert without status 鈥?should get NULL
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t6 (id) VALUES (2)", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, status FROM t6 WHERE id = 2", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(rows[0].values[1].is_null(), "status should be NULL after DROP DEFAULT");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_alter_column_type_int_to_float() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t7 (id INT PRIMARY KEY, price INT)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t7 VALUES (1, 100)", Some(&txn)).unwrap();

    run_sql(&storage, &txn_mgr, &executor,
        "ALTER TABLE t7 ALTER COLUMN price SET DATA TYPE FLOAT", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, price FROM t7 ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[1], Datum::Float64(100.0));
        }
        _ => panic!("Expected Query result"),
    }
}

// 鈹€鈹€鈹€ Recursive CTE tests 鈹€鈹€鈹€

#[test]
fn test_recursive_cte_counting() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Simple counting: 1..5
    let result = run_sql(&storage, &txn_mgr, &executor,
        "WITH RECURSIVE cnt AS (\
           SELECT 1 AS n \
           UNION ALL \
           SELECT n + 1 FROM cnt WHERE n < 5\
         ) SELECT n FROM cnt ORDER BY n",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row.values[0], Datum::Int64((i + 1) as i64));
            }
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_recursive_cte_tree_traversal() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Build an employee hierarchy tree
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, manager_id INT)",
        Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (1, 'CEO', 0)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (2, 'VP1', 1)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (3, 'VP2', 1)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (4, 'Mgr1', 2)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (5, 'Mgr2', 3)", Some(&txn)).unwrap();

    // Traverse from CEO (id=1) down the tree
    let result = run_sql(&storage, &txn_mgr, &executor,
        "WITH RECURSIVE subordinates AS (\
           SELECT id, name, manager_id FROM employees WHERE id = 1 \
           UNION ALL \
           SELECT e.id, e.name, e.manager_id FROM employees e \
             JOIN subordinates s ON e.manager_id = s.id\
         ) SELECT id, name FROM subordinates ORDER BY id",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5, "All 5 employees should be reachable from CEO");
            assert_eq!(rows[0].values[1], Datum::Text("CEO".into()));
            assert_eq!(rows[1].values[1], Datum::Text("VP1".into()));
            assert_eq!(rows[2].values[1], Datum::Text("VP2".into()));
            assert_eq!(rows[3].values[1], Datum::Text("Mgr1".into()));
            assert_eq!(rows[4].values[1], Datum::Text("Mgr2".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_recursive_cte_fibonacci() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Generate first 8 Fibonacci numbers using two-column recursive CTE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "WITH RECURSIVE fib AS (\
           SELECT 1 AS a, 1 AS b \
           UNION ALL \
           SELECT b, a + b FROM fib WHERE b < 20\
         ) SELECT a FROM fib ORDER BY a",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            let vals: Vec<i64> = rows.iter().map(|r| match &r.values[0] {
                Datum::Int64(v) => *v,
                Datum::Int32(v) => *v as i64,
                other => panic!("Unexpected datum: {:?}", other),
            }).collect();
            // Fibonacci: 1, 1, 2, 3, 5, 8, 13
            assert_eq!(vals, vec![1, 1, 2, 3, 5, 8, 13]);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_recursive_cte_empty_base() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE empty_t (id INT PRIMARY KEY, val INT)", Some(&txn)).unwrap();

    // Recursive CTE with empty base case should return 0 rows
    let result = run_sql(&storage, &txn_mgr, &executor,
        "WITH RECURSIVE r AS (\
           SELECT id, val FROM empty_t \
           UNION ALL \
           SELECT id, val + 1 FROM r WHERE val < 10\
         ) SELECT * FROM r",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 0);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_recursive_cte_with_limit() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Use recursive CTE but apply LIMIT on the outer query
    let result = run_sql(&storage, &txn_mgr, &executor,
        "WITH RECURSIVE cnt AS (\
           SELECT 1 AS n \
           UNION ALL \
           SELECT n + 1 FROM cnt WHERE n < 100\
         ) SELECT n FROM cnt ORDER BY n LIMIT 3",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[0], Datum::Int64(1));
            assert_eq!(rows[1].values[0], Datum::Int64(2));
            assert_eq!(rows[2].values[0], Datum::Int64(3));
        }
        _ => panic!("Expected Query result"),
    }
}
