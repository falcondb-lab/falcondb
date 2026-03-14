#![allow(clippy::approx_constant, clippy::useless_vec)]

mod common;
use common::*;

#[test]
fn test_create_table() {
    let (storage, txn_mgr, executor) = setup();
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)",
        None,
    );
    assert!(result.is_ok());
    let schema = storage.get_table_schema("users");
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.primary_key_columns, vec![0]);
}

#[test]
fn test_insert_and_select() {
    let (storage, txn_mgr, executor) = setup();

    // Create table
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)",
        None,
    )
    .unwrap();

    // Begin txn
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Insert
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30)",
        Some(&txn),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 1);
        }
        _ => panic!("Expected DML result"),
    }

    // Insert another
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO users VALUES (2, 'Bob', 25)",
        Some(&txn),
    )
    .unwrap();

    // Commit
    txn_mgr.commit(txn.txn_id).unwrap();

    // Select with a new txn
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT * FROM users ORDER BY id",
        Some(&txn2),
    )
    .unwrap();

    match result {
        falcon_executor::ExecutionResult::Query { columns, rows } => {
            assert_eq!(columns.len(), 3);
            assert_eq!(rows.len(), 2);
            // First row: id=1, name=Alice, age=30
            assert_eq!(rows[0].values[0], falcon_common::datum::Datum::Int32(1));
            assert_eq!(
                rows[0].values[1],
                falcon_common::datum::Datum::Text("Alice".into())
            );
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_update() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE t (id INT PRIMARY KEY, val INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (1, 100)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (2, 200)",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Update
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "UPDATE t SET val = 999 WHERE id = 1",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 1);
        }
        _ => panic!("Expected DML result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();

    // Verify
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT val FROM t WHERE id = 1",
        Some(&txn3),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], falcon_common::datum::Datum::Int64(999));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn3.txn_id).unwrap();
}

#[test]
fn test_delete() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (1, 'a')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (2, 'b')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (3, 'c')",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Delete
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "DELETE FROM t WHERE id = 2",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 1);
        }
        _ => panic!("Expected DML result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();

    // Verify count
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT COUNT(*) FROM t",
        Some(&txn3),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], falcon_common::datum::Datum::Int64(2));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn3.txn_id).unwrap();
}

#[test]
fn test_aggregates() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE scores (id INT PRIMARY KEY, score INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for i in 1..=5 {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!("INSERT INTO scores VALUES ({}, {})", i, i * 10),
            Some(&txn),
        )
        .unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // SUM
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT SUM(score) FROM scores",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            // 10 + 20 + 30 + 40 + 50 = 150
            let sum = rows[0].values[0].as_i64().unwrap();
            assert_eq!(sum, 150);
        }
        _ => panic!("Expected Query result"),
    }

    // MIN / MAX
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT MIN(score), MAX(score) FROM scores",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], falcon_common::datum::Datum::Int32(10));
            assert_eq!(rows[0].values[1], falcon_common::datum::Datum::Int32(50));
        }
        _ => panic!("Expected Query result"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_transaction_rollback() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)",
        None,
    )
    .unwrap();

    // Insert and commit
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (1, 'committed')",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Insert and rollback
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (2, 'rolled_back')",
        Some(&txn2),
    )
    .unwrap();
    txn_mgr.abort(txn2.txn_id).unwrap();

    // Verify only committed row is visible
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT COUNT(*) FROM t",
        Some(&txn3),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], falcon_common::datum::Datum::Int64(1));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn3.txn_id).unwrap();
}

#[test]
fn test_select_with_limit_and_order() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (3, 'c')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (1, 'a')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (2, 'b')",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT * FROM t ORDER BY id LIMIT 2",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], falcon_common::datum::Datum::Int32(1));
            assert_eq!(rows[1].values[0], falcon_common::datum::Datum::Int32(2));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_drop_table() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE to_drop (id INT PRIMARY KEY)",
        None,
    )
    .unwrap();
    assert!(storage.get_table_schema("to_drop").is_some());

    run_sql(&storage, &txn_mgr, &executor, "DROP TABLE to_drop", None).unwrap();
    assert!(storage.get_table_schema("to_drop").is_none());
}

#[test]
fn test_where_filter() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE t (id INT PRIMARY KEY, active BOOLEAN, score INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (1, true, 100)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (2, false, 200)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO t VALUES (3, true, 300)",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Filter by boolean
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT id, score FROM t WHERE active = true ORDER BY id",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], falcon_common::datum::Datum::Int32(1));
            assert_eq!(rows[1].values[0], falcon_common::datum::Datum::Int32(3));
        }
        _ => panic!("Expected Query result"),
    }

    // Filter by comparison
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT id FROM t WHERE score > 150 ORDER BY id",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], falcon_common::datum::Datum::Int32(2));
            assert_eq!(rows[1].values[0], falcon_common::datum::Datum::Int32(3));
        }
        _ => panic!("Expected Query result"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_group_by_sum() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, region, amount) in [
        (1, "east", 100),
        (2, "west", 200),
        (3, "east", 150),
        (4, "west", 300),
        (5, "east", 50),
    ] {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!(
                "INSERT INTO sales VALUES ({}, '{}', {})",
                id, region, amount
            ),
            Some(&txn),
        )
        .unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, columns } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0].values[0],
                falcon_common::datum::Datum::Text("east".into())
            );
            assert_eq!(rows[0].values[1].as_i64().unwrap(), 300); // 100+150+50
            assert_eq!(
                rows[1].values[0],
                falcon_common::datum::Datum::Text("west".into())
            );
            assert_eq!(rows[1].values[1].as_i64().unwrap(), 500); // 200+300
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_group_by_count() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE items (id INT PRIMARY KEY, cat TEXT, price INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, cat, price) in [
        (1, "A", 10),
        (2, "B", 20),
        (3, "A", 30),
        (4, "B", 5),
        (5, "A", 20),
    ] {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!("INSERT INTO items VALUES ({}, '{}', {})", id, cat, price),
            Some(&txn),
        )
        .unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT cat, COUNT(*), MIN(price), MAX(price) FROM items GROUP BY cat ORDER BY cat",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            // A: count=3, min=10, max=30
            assert_eq!(
                rows[0].values[0],
                falcon_common::datum::Datum::Text("A".into())
            );
            assert_eq!(rows[0].values[1], falcon_common::datum::Datum::Int64(3));
            assert_eq!(rows[0].values[2], falcon_common::datum::Datum::Int32(10));
            assert_eq!(rows[0].values[3], falcon_common::datum::Datum::Int32(30));
            // B: count=2, min=5, max=20
            assert_eq!(
                rows[1].values[0],
                falcon_common::datum::Datum::Text("B".into())
            );
            assert_eq!(rows[1].values[1], falcon_common::datum::Datum::Int64(2));
            assert_eq!(rows[1].values[2], falcon_common::datum::Datum::Int32(5));
            assert_eq!(rows[1].values[3], falcon_common::datum::Datum::Int32(20));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_like_filter() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, name, cat) in [
        (1, "Apple Juice", "drinks"),
        (2, "Apple Pie", "food"),
        (3, "Banana Split", "food"),
        (4, "Grape Soda", "drinks"),
        (5, "Pineapple", "fruit"),
    ] {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!(
                "INSERT INTO products VALUES ({}, '{}', '{}')",
                id, name, cat
            ),
            Some(&txn),
        )
        .unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT name FROM products WHERE name LIKE 'Apple%' ORDER BY name",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Apple Juice".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Apple Pie".into()));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_between_filter() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE scores (id INT PRIMARY KEY, name TEXT, score INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, name, score) in [
        (1, "Alice", 85),
        (2, "Bob", 42),
        (3, "Carol", 95),
        (4, "Dave", 70),
        (5, "Eve", 55),
    ] {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!("INSERT INTO scores VALUES ({}, '{}', {})", id, name, score),
            Some(&txn),
        )
        .unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT name FROM scores WHERE score BETWEEN 50 AND 90 ORDER BY name",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[0], Datum::Text("Alice".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Dave".into()));
            assert_eq!(rows[2].values[0], Datum::Text("Eve".into()));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_in_list_filter() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE cities (id INT PRIMARY KEY, name TEXT, country TEXT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, name, country) in [
        (1, "Tokyo", "Japan"),
        (2, "Paris", "France"),
        (3, "London", "UK"),
        (4, "Berlin", "Germany"),
        (5, "Rome", "Italy"),
    ] {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!(
                "INSERT INTO cities VALUES ({}, '{}', '{}')",
                id, name, country
            ),
            Some(&txn),
        )
        .unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT name FROM cities WHERE country IN ('France', 'UK', 'Italy') ORDER BY name",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[0], Datum::Text("London".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Paris".into()));
            assert_eq!(rows[2].values[0], Datum::Text("Rome".into()));
        }
        _ => panic!("Expected Query result"),
    }

    // NOT IN
    let result2 = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT name FROM cities WHERE country NOT IN ('France', 'UK', 'Italy') ORDER BY name",
        Some(&txn2),
    )
    .unwrap();
    match result2 {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Berlin".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Tokyo".into()));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_cast_expression() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE vals (id INT PRIMARY KEY, num INT, label TEXT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO vals VALUES (1, 42, '100')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO vals VALUES (2, 7, '200')",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT CAST(num AS TEXT) FROM vals WHERE id = 1",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("42".into()));
        }
        _ => panic!("Expected Query result"),
    }

    let result2 = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT id FROM vals WHERE CAST(label AS INTEGER) > 150",
        Some(&txn2),
    )
    .unwrap();
    match result2 {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int32(2));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_inner_join() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE departments (dept_id INT PRIMARY KEY, dept_name TEXT)",
        None,
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE employees (emp_id INT PRIMARY KEY, emp_name TEXT, fk_dept INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO departments VALUES (1, 'Engineering')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO departments VALUES (2, 'Sales')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO departments VALUES (3, 'HR')",
        Some(&txn),
    )
    .unwrap();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO employees VALUES (1, 'Alice', 1)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO employees VALUES (2, 'Bob', 2)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO employees VALUES (3, 'Carol', 1)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO employees VALUES (4, 'Dave', 4)",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // INNER JOIN — only matching rows (unambiguous column names)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT emp_name, dept_name FROM employees INNER JOIN departments ON fk_dept = dept_id ORDER BY emp_name",
        Some(&txn2)).unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            // Alice(1=Eng), Bob(2=Sales), Carol(1=Eng) — Dave excluded (dept 4 not found)
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[0], Datum::Text("Alice".into()));
            assert_eq!(rows[0].values[1], Datum::Text("Engineering".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Bob".into()));
            assert_eq!(rows[1].values[1], Datum::Text("Sales".into()));
            assert_eq!(rows[2].values[0], Datum::Text("Carol".into()));
            assert_eq!(rows[2].values[1], Datum::Text("Engineering".into()));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_cross_join() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE colors (id INT PRIMARY KEY, color TEXT)",
        None,
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE sizes (id INT PRIMARY KEY, size TEXT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO colors VALUES (1, 'Red')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO colors VALUES (2, 'Blue')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO sizes VALUES (1, 'S')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO sizes VALUES (2, 'M')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO sizes VALUES (3, 'L')",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT color, size FROM colors CROSS JOIN sizes ORDER BY color, size",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 6); // 2 colors * 3 sizes
                                       // Blue+L, Blue+M, Blue+S, Red+L, Red+M, Red+S
            assert_eq!(rows[0].values[0], Datum::Text("Blue".into()));
            assert_eq!(rows[0].values[1], Datum::Text("L".into()));
            assert_eq!(rows[5].values[0], Datum::Text("Red".into()));
            assert_eq!(rows[5].values[1], Datum::Text("S".into()));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_left_join() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE authors (author_id INT PRIMARY KEY, author_name TEXT)",
        None,
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE books (book_id INT PRIMARY KEY, title TEXT, fk_author INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO authors VALUES (1, 'Tolkien')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO authors VALUES (2, 'Asimov')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO authors VALUES (3, 'Orwell')",
        Some(&txn),
    )
    .unwrap(); // no books

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO books VALUES (1, 'The Hobbit', 1)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO books VALUES (2, 'Foundation', 2)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO books VALUES (3, 'LOTR', 1)",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // LEFT JOIN — all authors, even those without books
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT author_name, title FROM authors LEFT JOIN books ON author_id = fk_author ORDER BY author_name, title",
        Some(&txn2)).unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            // Asimov+Foundation, Orwell+NULL, Tolkien+LOTR, Tolkien+The Hobbit
            assert_eq!(rows.len(), 4);
            assert_eq!(rows[0].values[0], Datum::Text("Asimov".into()));
            assert_eq!(rows[0].values[1], Datum::Text("Foundation".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Orwell".into()));
            assert!(rows[1].values[1].is_null()); // Orwell has no books
            assert_eq!(rows[2].values[0], Datum::Text("Tolkien".into()));
            assert_eq!(rows[2].values[1], Datum::Text("LOTR".into()));
            assert_eq!(rows[3].values[0], Datum::Text("Tolkien".into()));
            assert_eq!(rows[3].values[1], Datum::Text("The Hobbit".into()));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_scalar_subquery() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE scores (id INT PRIMARY KEY, player TEXT, score INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO scores VALUES (1, 'Alice', 90)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO scores VALUES (2, 'Bob', 75)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO scores VALUES (3, 'Carol', 95)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO scores VALUES (4, 'Dave', 80)",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Scalar subquery: find players with max score
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT player FROM scores WHERE score = (SELECT MAX(score) FROM scores)",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("Carol".into()));
        }
        _ => panic!("Expected Query result for scalar subquery"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_in_subquery() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE products (pid INT PRIMARY KEY, pname TEXT)",
        None,
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE orders (oid INT PRIMARY KEY, product_id INT, qty INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO products VALUES (1, 'Widget')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO products VALUES (2, 'Gadget')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO products VALUES (3, 'Doohickey')",
        Some(&txn),
    )
    .unwrap();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO orders VALUES (1, 1, 5)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO orders VALUES (2, 3, 2)",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // IN subquery: products that have orders
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT pname FROM products WHERE pid IN (SELECT product_id FROM orders) ORDER BY pname",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Doohickey".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Widget".into()));
        }
        _ => panic!("Expected Query result for IN subquery"),
    }

    // NOT IN subquery: products without orders
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT pname FROM products WHERE pid NOT IN (SELECT product_id FROM orders)",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("Gadget".into()));
        }
        _ => panic!("Expected Query result for NOT IN subquery"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_exists_subquery() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE depts (did INT PRIMARY KEY, dname TEXT)",
        None,
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE emps (eid INT PRIMARY KEY, ename TEXT, dept INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO depts VALUES (1, 'Engineering')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO depts VALUES (2, 'Marketing')",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO depts VALUES (3, 'Sales')",
        Some(&txn),
    )
    .unwrap();

    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO emps VALUES (1, 'Alice', 1)",
        Some(&txn),
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO emps VALUES (2, 'Bob', 1)",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // EXISTS: all depts returned because emps table is non-empty (uncorrelated)
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT dname FROM depts WHERE EXISTS (SELECT 1 FROM emps) ORDER BY dname",
        Some(&txn2),
    )
    .unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[0], Datum::Text("Engineering".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Marketing".into()));
            assert_eq!(rows[2].values[0], Datum::Text("Sales".into()));
        }
        _ => panic!("Expected Query result for EXISTS subquery"),
    }

    // NOT EXISTS: no emp has dept=999, so all depts returned
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT dname FROM depts WHERE NOT EXISTS (SELECT 1 FROM emps WHERE dept = 999) ORDER BY dname",
        Some(&txn2)).unwrap();
    match result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
        }
        _ => panic!("Expected Query result for NOT EXISTS subquery"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}
