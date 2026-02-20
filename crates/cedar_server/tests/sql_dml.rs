#![allow(clippy::approx_constant, clippy::useless_vec)]

mod common;
use common::*;

#[test]
fn test_select_distinct() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE colors (id INT PRIMARY KEY, color TEXT, size TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (1, 'red', 'S')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (2, 'blue', 'M')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (3, 'red', 'L')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (4, 'blue', 'S')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (5, 'red', 'S')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // DISTINCT on single column
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT DISTINCT color FROM colors ORDER BY color",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("blue".into()));
            assert_eq!(rows[1].values[0], Datum::Text("red".into()));
        }
        _ => panic!("Expected Query result for DISTINCT"),
    }

    // DISTINCT on multiple columns
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT DISTINCT color, size FROM colors ORDER BY color, size",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            // blue+M, blue+S, red+L, red+S (red+S deduped from 2 rows)
            assert_eq!(rows.len(), 4);
            assert_eq!(rows[0].values[0], Datum::Text("blue".into()));
            assert_eq!(rows[0].values[1], Datum::Text("M".into()));
            assert_eq!(rows[3].values[0], Datum::Text("red".into()));
            assert_eq!(rows[3].values[1], Datum::Text("S".into()));
        }
        _ => panic!("Expected Query result for DISTINCT multi-column"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_table_alias() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE staff (id INT PRIMARY KEY, name TEXT, dept_id INT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE departments (id INT PRIMARY KEY, dname TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO staff VALUES (1, 'Alice', 10)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO staff VALUES (2, 'Bob', 20)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO departments VALUES (10, 'Eng')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO departments VALUES (20, 'Sales')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Single-table alias: SELECT s.name FROM staff AS s WHERE s.id = 1
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT s.name FROM staff AS s WHERE s.id = 1",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("Alice".into()));
        }
        _ => panic!("Expected Query result for single-table alias"),
    }

    // JOIN with aliases: SELECT s.name, d.dname FROM staff AS s INNER JOIN departments AS d ON s.dept_id = d.id
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT s.name, d.dname FROM staff AS s INNER JOIN departments AS d ON s.dept_id = d.id ORDER BY s.name",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Alice".into()));
            assert_eq!(rows[0].values[1], Datum::Text("Eng".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Bob".into()));
            assert_eq!(rows[1].values[1], Datum::Text("Sales".into()));
        }
        _ => panic!("Expected Query result for JOIN with aliases"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_case_coalesce_nullif() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, category TEXT, note TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO items VALUES (1, 'Widget', 'A', 'good')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO items VALUES (2, 'Gadget', 'B', NULL)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO items VALUES (3, 'Doohickey', 'A', NULL)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Searched CASE WHEN (ORDER BY name: Doohickey=A, Gadget=B, Widget=A)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, CASE WHEN category = 'A' THEN 'alpha' WHEN category = 'B' THEN 'beta' ELSE 'other' END FROM items ORDER BY name",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[0], Datum::Text("Doohickey".into()));
            assert_eq!(rows[0].values[1], Datum::Text("alpha".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Gadget".into()));
            assert_eq!(rows[1].values[1], Datum::Text("beta".into()));
            assert_eq!(rows[2].values[0], Datum::Text("Widget".into()));
            assert_eq!(rows[2].values[1], Datum::Text("alpha".into()));
        }
        _ => panic!("Expected Query result for CASE WHEN"),
    }

    // COALESCE (ORDER BY name: Doohickey=NULL, Gadget=NULL, Widget=good)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, COALESCE(note, 'N/A') FROM items ORDER BY name",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[1], Datum::Text("N/A".into()));    // Doohickey
            assert_eq!(rows[1].values[1], Datum::Text("N/A".into()));    // Gadget
            assert_eq!(rows[2].values[1], Datum::Text("good".into()));   // Widget
        }
        _ => panic!("Expected Query result for COALESCE"),
    }

    // NULLIF: NULLIF(category, 'A') => NULL when category='A', else category
    // ORDER BY name: Doohickey=A=>NULL, Gadget=B, Widget=A=>NULL
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, NULLIF(category, 'A') FROM items ORDER BY name",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert!(rows[0].values[1].is_null()); // Doohickey: category='A' => NULL
            assert_eq!(rows[1].values[1], Datum::Text("B".into())); // Gadget: 'B' != 'A'
            assert!(rows[2].values[1].is_null()); // Widget: category='A' => NULL
        }
        _ => panic!("Expected Query result for NULLIF"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_string_functions() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE words (id INT PRIMARY KEY, word TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO words VALUES (1, 'Hello')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO words VALUES (2, 'World')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // UPPER / LOWER
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT UPPER(word), LOWER(word) FROM words ORDER BY 1",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("HELLO".into()));
            assert_eq!(rows[0].values[1], Datum::Text("hello".into()));
            assert_eq!(rows[1].values[0], Datum::Text("WORLD".into()));
            assert_eq!(rows[1].values[1], Datum::Text("world".into()));
        }
        _ => panic!("Expected Query result for UPPER/LOWER"),
    }

    // LENGTH
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT word, LENGTH(word) FROM words ORDER BY 1",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[1], Datum::Int32(5));
            assert_eq!(rows[1].values[1], Datum::Int32(5));
        }
        _ => panic!("Expected Query result for LENGTH"),
    }

    // CONCAT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CONCAT(word, '!') FROM words WHERE id = 1",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("Hello!".into()));
        }
        _ => panic!("Expected Query result for CONCAT"),
    }

    // SUBSTRING
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT SUBSTRING(word, 1, 3) FROM words WHERE id = 1",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("Hel".into()));
        }
        _ => panic!("Expected Query result for SUBSTRING"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_insert_select() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE src (id INT PRIMARY KEY, name TEXT, score INT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE dst (id INT PRIMARY KEY, name TEXT, score INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO src VALUES (1, 'Alice', 90)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO src VALUES (2, 'Bob', 80)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO src VALUES (3, 'Carol', 70)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // INSERT ... SELECT all rows
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO dst SELECT id, name, score FROM src",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 3);
        }
        _ => panic!("Expected Dml result for INSERT SELECT"),
    }

    // Verify inserted rows
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, score FROM dst ORDER BY name",
        Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[0], Datum::Text("Alice".into()));
            assert_eq!(rows[0].values[1], Datum::Int32(90));
            assert_eq!(rows[1].values[0], Datum::Text("Bob".into()));
            assert_eq!(rows[2].values[0], Datum::Text("Carol".into()));
        }
        _ => panic!("Expected Query result for SELECT after INSERT SELECT"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();

    // INSERT ... SELECT with WHERE filter
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE top_scorers (id INT PRIMARY KEY, name TEXT, score INT)", None).unwrap();

    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO top_scorers SELECT id, name, score FROM src WHERE score >= 80",
        Some(&txn3)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 2);
        }
        _ => panic!("Expected Dml result for filtered INSERT SELECT"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM top_scorers ORDER BY name",
        Some(&txn3)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Alice".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Bob".into()));
        }
        _ => panic!("Expected Query result for filtered INSERT SELECT verify"),
    }
    txn_mgr.commit(txn3.txn_id).unwrap();
}

#[test]
fn test_limit_offset() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE nums (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for i in 1..=6 {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO nums VALUES ({}, 'v{}')", i, i), Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // LIMIT only
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, val FROM nums ORDER BY id LIMIT 3",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[1], Datum::Text("v1".into()));
            assert_eq!(rows[2].values[1], Datum::Text("v3".into()));
        }
        _ => panic!("Expected Query"),
    }

    // OFFSET only
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, val FROM nums ORDER BY id OFFSET 4",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[1], Datum::Text("v5".into()));
            assert_eq!(rows[1].values[1], Datum::Text("v6".into()));
        }
        _ => panic!("Expected Query"),
    }

    // LIMIT + OFFSET
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, val FROM nums ORDER BY id LIMIT 2 OFFSET 2",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[1], Datum::Text("v3".into()));
            assert_eq!(rows[1].values[1], Datum::Text("v4".into()));
        }
        _ => panic!("Expected Query"),
    }

    // OFFSET past end
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, val FROM nums ORDER BY id OFFSET 100",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 0);
        }
        _ => panic!("Expected Query"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_cte() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (1, 'Alice', 'eng', 100)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (2, 'Bob', 'eng', 120)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (3, 'Carol', 'sales', 90)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (4, 'Dave', 'sales', 80)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Simple CTE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "WITH eng AS (SELECT id, name, salary FROM employees WHERE dept = 'eng') \
         SELECT name, salary FROM eng ORDER BY name",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Alice".into()));
            assert_eq!(rows[0].values[1], Datum::Int32(100));
            assert_eq!(rows[1].values[0], Datum::Text("Bob".into()));
            assert_eq!(rows[1].values[1], Datum::Int32(120));
        }
        _ => panic!("Expected Query result for CTE"),
    }

    // CTE with filter on CTE result
    let result = run_sql(&storage, &txn_mgr, &executor,
        "WITH high_sal AS (SELECT id, name, salary FROM employees WHERE salary >= 100) \
         SELECT name FROM high_sal ORDER BY name",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Alice".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Bob".into()));
        }
        _ => panic!("Expected Query result for CTE with filter"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_alter_table() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO items VALUES (1, 'Widget')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // ADD COLUMN
    run_sql(&storage, &txn_mgr, &executor,
        "ALTER TABLE items ADD COLUMN price INT", None).unwrap();

    // Insert with new column
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO items VALUES (2, 'Gadget', 50)", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();

    // Query new column
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, price FROM items WHERE id = 2",
        Some(&txn3)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("Gadget".into()));
            assert_eq!(rows[0].values[1], Datum::Int32(50));
        }
        _ => panic!("Expected Query result for ALTER TABLE ADD COLUMN"),
    }

    // DROP COLUMN
    run_sql(&storage, &txn_mgr, &executor,
        "ALTER TABLE items DROP COLUMN price", None).unwrap();

    // Verify column is gone 鈥?insert should work with 2 columns again
    let txn4 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO items VALUES (3, 'Doohickey')", Some(&txn4)).unwrap();
    txn_mgr.commit(txn4.txn_id).unwrap();

    let txn5 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM items WHERE id = 3",
        Some(&txn5)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("Doohickey".into()));
        }
        _ => panic!("Expected Query result after DROP COLUMN"),
    }
    txn_mgr.commit(txn5.txn_id).unwrap();
}

#[test]
fn test_select_without_from() {
    let (storage, txn_mgr, executor) = setup();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Simple literal
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT 42", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int32(42));
        }
        _ => panic!("Expected Query"),
    }

    // Arithmetic expression
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT 1 + 2", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int32(3));
        }
        _ => panic!("Expected Query"),
    }

    // String literal
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT 'hello world'", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("hello world".into()));
        }
        _ => panic!("Expected Query"),
    }

    // String function without FROM
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT UPPER('hello')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("HELLO".into()));
        }
        _ => panic!("Expected Query"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_union() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t1 VALUES (1, 'Alice')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t1 VALUES (2, 'Bob')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t2 VALUES (3, 'Carol')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO t2 VALUES (4, 'Alice')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // UNION ALL 鈥?keeps duplicates
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM t1 UNION ALL SELECT name FROM t2",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 4); // Alice, Bob, Carol, Alice
        }
        _ => panic!("Expected Query"),
    }

    // UNION 鈥?deduplicates
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM t1 UNION SELECT name FROM t2",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3); // Alice, Bob, Carol (deduplicated)
        }
        _ => panic!("Expected Query"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_math_and_string_functions() {
    let (storage, txn_mgr, executor) = setup();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ABS
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ABS(-42)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int32(42));
        }
        _ => panic!("Expected Query for ABS"),
    }

    // ROUND, CEIL, FLOOR
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ROUND(3.7)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Float64(4.0));
        }
        _ => panic!("Expected Query for ROUND"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT FLOOR(3.7)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Float64(3.0));
        }
        _ => panic!("Expected Query for FLOOR"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CEIL(3.2)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Float64(4.0));
        }
        _ => panic!("Expected Query for CEIL"),
    }

    // TRIM
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT TRIM('  hello  ')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("hello".into()));
        }
        _ => panic!("Expected Query for TRIM"),
    }

    // REPLACE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT REPLACE('hello world', 'world', 'rust')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("hello rust".into()));
        }
        _ => panic!("Expected Query for REPLACE"),
    }

    // STRPOS (Position)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRPOS('hello world', 'world')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int32(7)); // 1-indexed
        }
        _ => panic!("Expected Query for STRPOS"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_count_distinct_and_if_exists() {
    let (storage, txn_mgr, executor) = setup();

    // CREATE TABLE IF NOT EXISTS 鈥?should not error on duplicate
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE colors (id INT PRIMARY KEY, color TEXT)", None).unwrap();
    let result = run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE IF NOT EXISTS colors (id INT PRIMARY KEY, color TEXT)", None).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Ddl { message } => {
            assert!(message.contains("already exists"));
        }
        _ => panic!("Expected Ddl result"),
    }

    // DROP TABLE IF EXISTS 鈥?should not error on missing table
    let result = run_sql(&storage, &txn_mgr, &executor,
        "DROP TABLE IF EXISTS nonexistent", None).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Ddl { message } => {
            assert!(message.contains("not found"));
        }
        _ => panic!("Expected Ddl result"),
    }

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (1, 'red')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (2, 'blue')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (3, 'red')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (4, 'green')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (5, 'red')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // COUNT(*) = 5
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COUNT(*) FROM colors", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(5));
        }
        _ => panic!("Expected Query for COUNT(*)"),
    }

    // COUNT(DISTINCT color) = 3 (red, blue, green)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COUNT(DISTINCT color) FROM colors", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(3));
        }
        _ => panic!("Expected Query for COUNT(DISTINCT)"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_returning_clause() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // INSERT ... RETURNING *
    let result = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO items VALUES (1, 'apple', 100) RETURNING *", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, columns } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(columns.len(), 3);
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Text("apple".into()));
            assert_eq!(rows[0].values[2], Datum::Int32(100));
        }
        _ => panic!("Expected Query result for INSERT RETURNING"),
    }

    // INSERT ... RETURNING specific columns
    let result = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO items VALUES (2, 'banana', 200) RETURNING id, name", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, columns } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].0, "id");
            assert_eq!(columns[1].0, "name");
            assert_eq!(rows[0].values[0], Datum::Int32(2));
            assert_eq!(rows[0].values[1], Datum::Text("banana".into()));
        }
        _ => panic!("Expected Query result for INSERT RETURNING id, name"),
    }

    // UPDATE ... RETURNING *
    let result = run_sql(&storage, &txn_mgr, &executor,
        "UPDATE items SET price = 150 WHERE id = 1 RETURNING *", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[2], Datum::Int32(150));
        }
        _ => panic!("Expected Query result for UPDATE RETURNING"),
    }

    // DELETE ... RETURNING id
    let result = run_sql(&storage, &txn_mgr, &executor,
        "DELETE FROM items WHERE id = 2 RETURNING id", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, columns } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(columns.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int32(2));
        }
        _ => panic!("Expected Query result for DELETE RETURNING"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_intersect_except() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE set_a (id INT PRIMARY KEY, val TEXT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE set_b (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO set_a VALUES (1, 'x')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO set_a VALUES (2, 'y')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO set_a VALUES (3, 'z')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO set_b VALUES (10, 'y')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO set_b VALUES (20, 'z')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO set_b VALUES (30, 'w')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // INTERSECT: common values (y, z)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT val FROM set_a INTERSECT SELECT val FROM set_b", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            let vals: Vec<&Datum> = rows.iter().map(|r| &r.values[0]).collect();
            assert!(vals.contains(&&Datum::Text("y".into())));
            assert!(vals.contains(&&Datum::Text("z".into())));
        }
        _ => panic!("Expected Query for INTERSECT"),
    }

    // EXCEPT: values in set_a but not set_b (x)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT val FROM set_a EXCEPT SELECT val FROM set_b", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("x".into()));
        }
        _ => panic!("Expected Query for EXCEPT"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_explain() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE expl (id INT PRIMARY KEY, name TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // EXPLAIN SELECT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "EXPLAIN SELECT id, name FROM expl WHERE id = 1", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, columns } => {
            assert_eq!(columns[0].0, "QUERY PLAN");
            assert!(rows.len() >= 2); // at least SeqScan + Output lines
            if let Datum::Text(ref line) = rows[0].values[0] {
                assert!(line.contains("Seq Scan on expl") || line.contains("Index Scan"));
            } else {
                panic!("Expected text plan output");
            }
        }
        _ => panic!("Expected Query for EXPLAIN"),
    }

    // EXPLAIN INSERT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "EXPLAIN INSERT INTO expl VALUES (1, 'a')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Text(ref line) = rows[0].values[0] {
                assert!(line.contains("Insert on expl"));
            } else {
                panic!("Expected text plan output");
            }
        }
        _ => panic!("Expected Query for EXPLAIN INSERT"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_default_values() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE with_defaults (id INT PRIMARY KEY, status TEXT DEFAULT 'active', score INT DEFAULT 0)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Insert with only id 鈥?status and score should get defaults
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO with_defaults (id) VALUES (1)", Some(&txn)).unwrap();

    // Insert with all columns explicitly
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO with_defaults VALUES (2, 'inactive', 99)", Some(&txn)).unwrap();

    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, status, score FROM with_defaults ORDER BY id", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            // Row 1: defaults applied
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Text("active".into()));
            assert_eq!(rows[0].values[2], Datum::Int32(0));
            // Row 2: explicit values
            assert_eq!(rows[1].values[0], Datum::Int32(2));
            assert_eq!(rows[1].values[1], Datum::Text("inactive".into()));
            assert_eq!(rows[1].values[2], Datum::Int32(99));
        }
        _ => panic!("Expected Query for SELECT"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_string_concat_operator() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE people (id INT PRIMARY KEY, first_name TEXT, last_name TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO people VALUES (1, 'John', 'Doe')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // String concatenation with ||
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT first_name || ' ' || last_name FROM people", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("John Doe".into()));
        }
        _ => panic!("Expected Query for string concat"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_expression_projections() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE scores (id INT PRIMARY KEY, math INT, science INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO scores VALUES (1, 80, 90)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO scores VALUES (2, 70, 85)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Expression projection with alias
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, math + science AS total FROM scores ORDER BY id", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, columns } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(columns[1].0, "total");
            assert_eq!(rows[0].values[1], Datum::Int32(170)); // 80+90
            assert_eq!(rows[1].values[1], Datum::Int32(155)); // 70+85
        }
        _ => panic!("Expected Query for expression projection"),
    }

    // Expression with string concat and alias
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT 'Score: ' || math AS label FROM scores WHERE id = 1", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, columns } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(columns[0].0, "label");
            assert_eq!(rows[0].values[0], Datum::Text("Score: 80".into()));
        }
        _ => panic!("Expected Query for string expression projection"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_serial_auto_increment() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE events (id SERIAL PRIMARY KEY, name TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Insert without specifying id 鈥?should auto-increment
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO events (name) VALUES ('first')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO events (name) VALUES ('second')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO events (name) VALUES ('third')", Some(&txn)).unwrap();

    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, name FROM events ORDER BY id", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Text("first".into()));
            assert_eq!(rows[1].values[0], Datum::Int32(2));
            assert_eq!(rows[1].values[1], Datum::Text("second".into()));
            assert_eq!(rows[2].values[0], Datum::Int32(3));
            assert_eq!(rows[2].values[1], Datum::Text("third".into()));
        }
        _ => panic!("Expected Query for SERIAL test"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_truncate_table() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE trunc_test (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO trunc_test VALUES (1, 'a')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO trunc_test VALUES (2, 'b')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Verify rows exist
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COUNT(*) FROM trunc_test", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2));
        }
        _ => panic!("Expected Query"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();

    // TRUNCATE
    run_sql(&storage, &txn_mgr, &executor, "TRUNCATE TABLE trunc_test", None).unwrap();

    // Verify table is empty
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COUNT(*) FROM trunc_test", Some(&txn3)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(0));
        }
        _ => panic!("Expected Query after TRUNCATE"),
    }
    txn_mgr.commit(txn3.txn_id).unwrap();

    // Verify we can still insert after truncate
    let txn4 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO trunc_test VALUES (10, 'new')", Some(&txn4)).unwrap();
    txn_mgr.commit(txn4.txn_id).unwrap();

    let txn5 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, val FROM trunc_test", Some(&txn5)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int32(10));
            assert_eq!(rows[0].values[1], Datum::Text("new".into()));
        }
        _ => panic!("Expected Query after re-insert"),
    }
    txn_mgr.commit(txn5.txn_id).unwrap();
}

#[test]
fn test_nested_function_calls() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE funcs (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO funcs VALUES (1, '  hello world  ')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // UPPER(TRIM(val))
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT UPPER(TRIM(val)) FROM funcs", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("HELLO WORLD".into()));
        }
        _ => panic!("Expected Query for nested functions"),
    }

    // LENGTH(LOWER(REPLACE(val, ' ', '')))
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT LENGTH(REPLACE(TRIM(val), ' ', '')) FROM funcs", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int32(10)); // "helloworld" = 10
        }
        _ => panic!("Expected Query for deeply nested functions"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_check_constraints() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE products (id INT PRIMARY KEY, price INT, CHECK (price > 0))", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Valid insert 鈥?price > 0
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO products VALUES (1, 100)", Some(&txn)).unwrap();

    // Invalid insert 鈥?price = 0 violates CHECK
    let err = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO products VALUES (2, 0)", Some(&txn));
    assert!(err.is_err(), "Expected CHECK constraint violation for price=0");

    // Invalid insert 鈥?price = -5 violates CHECK
    let err = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO products VALUES (3, -5)", Some(&txn));
    assert!(err.is_err(), "Expected CHECK constraint violation for price=-5");

    txn_mgr.commit(txn.txn_id).unwrap();

    // Verify only the valid row exists
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COUNT(*) FROM products", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(1));
        }
        _ => panic!("Expected Query for CHECK test"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_insert_on_conflict_do_nothing() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE kv (key INT PRIMARY KEY, val TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO kv VALUES (1, 'first')", Some(&txn)).unwrap();

    // ON CONFLICT DO NOTHING 鈥?duplicate key should be silently skipped
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO kv VALUES (1, 'duplicate') ON CONFLICT DO NOTHING", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO kv VALUES (2, 'second') ON CONFLICT DO NOTHING", Some(&txn)).unwrap();

    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT key, val FROM kv ORDER BY key", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Text("first".into())); // not "duplicate"
            assert_eq!(rows[1].values[0], Datum::Int32(2));
            assert_eq!(rows[1].values[1], Datum::Text("second".into()));
        }
        _ => panic!("Expected Query for ON CONFLICT DO NOTHING"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_insert_on_conflict_do_update() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE counters (id INT PRIMARY KEY, count INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO counters VALUES (1, 10)", Some(&txn)).unwrap();

    // ON CONFLICT DO UPDATE 鈥?update the existing row
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO counters VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET count = 99", Some(&txn)).unwrap();

    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, count FROM counters", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Int32(99)); // updated
        }
        _ => panic!("Expected Query for ON CONFLICT DO UPDATE"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_unique_constraint() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO users VALUES (1, 'alice@test.com', 'Alice')", Some(&txn)).unwrap();

    // Duplicate email should fail
    let err = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO users VALUES (2, 'alice@test.com', 'Bob')", Some(&txn));
    assert!(err.is_err(), "Expected UNIQUE constraint violation");

    // Different email should succeed
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO users VALUES (2, 'bob@test.com', 'Bob')", Some(&txn)).unwrap();

    // NULL email should be allowed (NULLs are always unique)
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO users (id, name) VALUES (3, 'Charlie')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO users (id, name) VALUES (4, 'Dave')", Some(&txn)).unwrap();

    txn_mgr.commit(txn.txn_id).unwrap();

    // Verify all valid rows exist
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COUNT(*) FROM users", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(4));
        }
        _ => panic!("Expected Query for UNIQUE test"),
    }

    // UPDATE to duplicate email should fail
    let err = run_sql(&storage, &txn_mgr, &executor,
        "UPDATE users SET email = 'alice@test.com' WHERE id = 2", Some(&txn2));
    assert!(err.is_err(), "Expected UNIQUE constraint violation on UPDATE");

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_create_index() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE indexed (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO indexed VALUES (1, 'alpha')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO indexed VALUES (2, 'beta')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Create index on val column
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE INDEX idx_val ON indexed (val)", None).unwrap();

    // Verify data still accessible after index creation
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, val FROM indexed ORDER BY id", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Text("alpha".into()));
            assert_eq!(rows[1].values[0], Datum::Int32(2));
            assert_eq!(rows[1].values[1], Datum::Text("beta".into()));
        }
        _ => panic!("Expected Query for CREATE INDEX test"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_foreign_key_constraint() {
    let (storage, txn_mgr, executor) = setup();

    // Create parent table
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)", None).unwrap();

    // Create child table with FK reference
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT REFERENCES departments(id), name TEXT)", None).unwrap();

    // Insert parent row
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO departments VALUES (1, 'Engineering')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Insert child with valid FK
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (100, 1, 'Alice')", Some(&txn2)).unwrap();

    // Insert child with invalid FK 鈥?dept_id=99 doesn't exist
    let err = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees VALUES (101, 99, 'Bob')", Some(&txn2));
    assert!(err.is_err(), "Expected FOREIGN KEY constraint violation");

    // Insert child with NULL FK 鈥?should be allowed
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO employees (id, name) VALUES (102, 'Charlie')", Some(&txn2)).unwrap();

    txn_mgr.commit(txn2.txn_id).unwrap();

    // Verify correct rows exist
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COUNT(*) FROM employees", Some(&txn3)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2)); // Alice + Charlie
        }
        _ => panic!("Expected Query for FK test"),
    }
    txn_mgr.commit(txn3.txn_id).unwrap();
}

#[test]
fn test_window_functions() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE scores (id INT PRIMARY KEY, dept TEXT, score INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO scores VALUES (1, 'A', 90)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO scores VALUES (2, 'A', 80)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO scores VALUES (3, 'B', 95)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO scores VALUES (4, 'B', 95)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO scores VALUES (5, 'B', 70)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Test ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC)
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, dept, score, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) FROM scores ORDER BY id",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            // id=1, dept=A, score=90 -> row_number=1 in A partition
            assert_eq!(rows[0].values[3], Datum::Int64(1));
            // id=2, dept=A, score=80 -> row_number=2 in A partition
            assert_eq!(rows[1].values[3], Datum::Int64(2));
            // id=3, dept=B, score=95 -> row_number=1 or 2 (tied with id=4)
            // id=5, dept=B, score=70 -> row_number=3
            assert_eq!(rows[4].values[3], Datum::Int64(3));
        }
        _ => panic!("Expected Query for window function test"),
    }

    // Test RANK() OVER (ORDER BY score DESC) 鈥?no partition
    let result2 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, score, RANK() OVER (ORDER BY score DESC) FROM scores ORDER BY id",
        Some(&txn2)).unwrap();
    match &result2 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            // score=90 -> rank=3 (two 95s ahead)
            assert_eq!(rows[0].values[2], Datum::Int64(3));
            // score=80 -> rank=4
            assert_eq!(rows[1].values[2], Datum::Int64(4));
            // score=95 -> rank=1
            assert_eq!(rows[2].values[2], Datum::Int64(1));
            // score=95 -> rank=1
            assert_eq!(rows[3].values[2], Datum::Int64(1));
            // score=70 -> rank=5
            assert_eq!(rows[4].values[2], Datum::Int64(5));
        }
        _ => panic!("Expected Query for RANK test"),
    }

    // Test DENSE_RANK() OVER (ORDER BY score DESC)
    let result3 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, score, DENSE_RANK() OVER (ORDER BY score DESC) FROM scores ORDER BY id",
        Some(&txn2)).unwrap();
    match &result3 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            // score=90 -> dense_rank=2
            assert_eq!(rows[0].values[2], Datum::Int64(2));
            // score=80 -> dense_rank=3
            assert_eq!(rows[1].values[2], Datum::Int64(3));
            // score=95 -> dense_rank=1
            assert_eq!(rows[2].values[2], Datum::Int64(1));
            // score=95 -> dense_rank=1
            assert_eq!(rows[3].values[2], Datum::Int64(1));
            // score=70 -> dense_rank=4
            assert_eq!(rows[4].values[2], Datum::Int64(4));
        }
        _ => panic!("Expected Query for DENSE_RANK test"),
    }

    // Test SUM() OVER (PARTITION BY dept)
    let result4 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, dept, SUM(score) OVER (PARTITION BY dept) FROM scores ORDER BY id",
        Some(&txn2)).unwrap();
    match &result4 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            // dept A: 90+80=170
            assert_eq!(rows[0].values[2], Datum::Int64(170));
            assert_eq!(rows[1].values[2], Datum::Int64(170));
            // dept B: 95+95+70=260
            assert_eq!(rows[2].values[2], Datum::Int64(260));
            assert_eq!(rows[3].values[2], Datum::Int64(260));
            assert_eq!(rows[4].values[2], Datum::Int64(260));
        }
        _ => panic!("Expected Query for SUM OVER test"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_datetime_functions() {
    let (storage, txn_mgr, executor) = setup();

    // Test NOW() returns a timestamp
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT NOW()", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(matches!(rows[0].values[0], Datum::Timestamp(_)));
        }
        _ => panic!("Expected Query for NOW() test"),
    }

    // Test EXTRACT with a known timestamp
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE events (id INT PRIMARY KEY, ts TIMESTAMP)", None).unwrap();
    // Insert timestamp for 2024-06-15 14:30:00 UTC = 1718461800000000 microseconds
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO events VALUES (1, 1718461800000000)", Some(&txn)).unwrap();

    let result2 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT EXTRACT(YEAR FROM ts) FROM events WHERE id = 1", Some(&txn)).unwrap();
    match &result2 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2024));
        }
        _ => panic!("Expected Query for EXTRACT YEAR test"),
    }

    let result3 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT EXTRACT(MONTH FROM ts) FROM events WHERE id = 1", Some(&txn)).unwrap();
    match &result3 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(6));
        }
        _ => panic!("Expected Query for EXTRACT MONTH test"),
    }

    let result4 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT EXTRACT(DAY FROM ts) FROM events WHERE id = 1", Some(&txn)).unwrap();
    match &result4 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(15));
        }
        _ => panic!("Expected Query for EXTRACT DAY test"),
    }

    // Test DATE_TRUNC to month
    let result5 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT DATE_TRUNC('month', ts) FROM events WHERE id = 1", Some(&txn)).unwrap();
    match &result5 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            // 2024-06-01 00:00:00 UTC = 1717200000000000 microseconds
            assert_eq!(rows[0].values[0], Datum::Timestamp(1717200000000000));
        }
        _ => panic!("Expected Query for DATE_TRUNC test"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_derived_tables() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO products VALUES (1, 'Apple', 10)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO products VALUES (2, 'Banana', 20)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO products VALUES (3, 'Cherry', 30)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Simple derived table
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT sub.name, sub.price FROM (SELECT name, price FROM products WHERE price > 10) AS sub ORDER BY sub.price",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Banana".into()));
            assert_eq!(rows[0].values[1], Datum::Int32(20));
            assert_eq!(rows[1].values[0], Datum::Text("Cherry".into()));
            assert_eq!(rows[1].values[1], Datum::Int32(30));
        }
        _ => panic!("Expected Query for derived table test"),
    }

    // Derived table with aggregate
    let result2 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT t.total FROM (SELECT SUM(price) AS total FROM products) AS t",
        Some(&txn2)).unwrap();
    match &result2 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int64(60));
        }
        _ => panic!("Expected Query for derived table aggregate test"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_cast_timestamp_and_coercion() {
    let (storage, txn_mgr, executor) = setup();

    // Test CAST string to timestamp
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CAST('2024-06-15 14:30:00' AS TIMESTAMP)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            // 2024-06-15 14:30:00 UTC = 1718461800000000 us
            assert_eq!(rows[0].values[0], Datum::Timestamp(1718461800000000));
        }
        _ => panic!("Expected Query for CAST timestamp test"),
    }

    // Test EXTRACT from CAST timestamp
    let result2 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT EXTRACT(YEAR FROM CAST('2024-06-15 14:30:00' AS TIMESTAMP))", Some(&txn)).unwrap();
    match &result2 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2024));
        }
        _ => panic!("Expected Query for EXTRACT from CAST test"),
    }

    // Test cross-type comparison: Int32 vs Float64
    let result3 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT 1 < 2.5", Some(&txn)).unwrap();
    match &result3 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Boolean(true));
        }
        _ => panic!("Expected Query for coercion test"),
    }

    // Test CAST date-only string to timestamp
    let result4 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CAST('2024-01-01' AS TIMESTAMP)", Some(&txn)).unwrap();
    match &result4 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            // 2024-01-01 00:00:00 UTC = 1704067200000000 us
            assert_eq!(rows[0].values[0], Datum::Timestamp(1704067200000000));
        }
        _ => panic!("Expected Query for CAST date test"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_implicit_cross_join() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE colors (id INT PRIMARY KEY, name TEXT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE sizes (id INT PRIMARY KEY, label TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (1, 'Red')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO colors VALUES (2, 'Blue')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO sizes VALUES (1, 'S')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO sizes VALUES (2, 'M')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Implicit cross join: FROM colors, sizes
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT colors.name, sizes.label FROM colors, sizes ORDER BY colors.name, sizes.label",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 4); // 2 x 2 = 4 cross product
            assert_eq!(rows[0].values[0], Datum::Text("Blue".into()));
            assert_eq!(rows[0].values[1], Datum::Text("M".into()));
            assert_eq!(rows[3].values[0], Datum::Text("Red".into()));
            assert_eq!(rows[3].values[1], Datum::Text("S".into()));
        }
        _ => panic!("Expected Query for implicit cross join test"),
    }

    // Implicit join with WHERE filter (like traditional join syntax)
    let result2 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT colors.name, sizes.label FROM colors, sizes WHERE colors.id = sizes.id ORDER BY colors.id",
        Some(&txn2)).unwrap();
    match &result2 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Red".into()));
            assert_eq!(rows[0].values[1], Datum::Text("S".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Blue".into()));
            assert_eq!(rows[1].values[1], Datum::Text("M".into()));
        }
        _ => panic!("Expected Query for implicit join with WHERE test"),
    }

    // JOIN USING clause
    let result3 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT colors.name, sizes.label FROM colors JOIN sizes USING (id) ORDER BY colors.id",
        Some(&txn2)).unwrap();
    match &result3 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Red".into()));
            assert_eq!(rows[0].values[1], Datum::Text("S".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Blue".into()));
            assert_eq!(rows[1].values[1], Datum::Text("M".into()));
        }
        _ => panic!("Expected Query for JOIN USING test"),
    }

    // NATURAL JOIN
    let result4 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT colors.name, sizes.label FROM colors NATURAL JOIN sizes ORDER BY colors.id",
        Some(&txn2)).unwrap();
    match &result4 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Red".into()));
            assert_eq!(rows[0].values[1], Datum::Text("S".into()));
        }
        _ => panic!("Expected Query for NATURAL JOIN test"),
    }

    // GREATEST / LEAST functions
    let result5 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT GREATEST(1, 3, 2), LEAST(10, 5, 8)", Some(&txn2)).unwrap();
    match &result5 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(3));
            assert_eq!(rows[0].values[1], Datum::Int64(5));
        }
        _ => panic!("Expected Query for GREATEST/LEAST test"),
    }

    // Typed string literal: TIMESTAMP '...'
    let result6 = run_sql(&storage, &txn_mgr, &executor,
        "SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-06-15 14:30:00')", Some(&txn2)).unwrap();
    match &result6 {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2024));
        }
        _ => panic!("Expected Query for typed string literal test"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_string_functions_extended() {
    let (storage, txn_mgr, executor) = setup();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // LPAD with custom fill
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT LPAD('hi', 5, 'xy')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("xyxhi".into()));
        }
        _ => panic!("Expected Query for LPAD"),
    }

    // LPAD with default fill (space)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT LPAD('hi', 5)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("   hi".into()));
        }
        _ => panic!("Expected Query for LPAD default"),
    }

    // RPAD
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT RPAD('hi', 5, 'xy')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("hixyx".into()));
        }
        _ => panic!("Expected Query for RPAD"),
    }

    // LEFT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT LEFT('hello world', 5)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("hello".into()));
        }
        _ => panic!("Expected Query for LEFT"),
    }

    // RIGHT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT RIGHT('hello world', 5)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("world".into()));
        }
        _ => panic!("Expected Query for RIGHT"),
    }

    // REPEAT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT REPEAT('ab', 3)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("ababab".into()));
        }
        _ => panic!("Expected Query for REPEAT"),
    }

    // REVERSE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT REVERSE('hello')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("olleh".into()));
        }
        _ => panic!("Expected Query for REVERSE"),
    }

    // INITCAP
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT INITCAP('hello world')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("Hello World".into()));
        }
        _ => panic!("Expected Query for INITCAP"),
    }

    // NOT LIKE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT 'hello' NOT LIKE '%xyz%'", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Boolean(true));
        }
        _ => panic!("Expected Query for NOT LIKE"),
    }

    // NOT BETWEEN
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT 10 NOT BETWEEN 1 AND 5", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Boolean(true));
        }
        _ => panic!("Expected Query for NOT BETWEEN"),
    }

    // GREATEST with text
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT GREATEST('apple', 'banana', 'cherry')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("cherry".into()));
        }
        _ => panic!("Expected Query for GREATEST text"),
    }

    // LEAST with text
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT LEAST('apple', 'banana', 'cherry')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("apple".into()));
        }
        _ => panic!("Expected Query for LEAST text"),
    }

    // ILIKE (case-insensitive LIKE)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT 'Hello World' ILIKE '%hello%'", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Boolean(true));
        }
        _ => panic!("Expected Query for ILIKE"),
    }

    // NOT ILIKE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT 'Hello World' NOT ILIKE '%xyz%'", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Boolean(true));
        }
        _ => panic!("Expected Query for NOT ILIKE"),
    }

    // POWER
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT POWER(2, 10)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Float64(1024.0));
        }
        _ => panic!("Expected Query for POWER"),
    }

    // SQRT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT SQRT(144)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Float64(12.0));
        }
        _ => panic!("Expected Query for SQRT"),
    }

    // SIGN
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT SIGN(-42)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(-1));
        }
        _ => panic!("Expected Query for SIGN"),
    }

    // TRUNC
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT TRUNC(3.7)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Float64(3.0));
        }
        _ => panic!("Expected Query for TRUNC"),
    }

    // CHR / ASCII
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CHR(65), ASCII('A')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("A".into()));
            assert_eq!(rows[0].values[1], Datum::Int32(65));
        }
        _ => panic!("Expected Query for CHR/ASCII"),
    }

    // || auto-cast
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT 'value=' || 42", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("value=42".into()));
        }
        _ => panic!("Expected Query for || auto-cast"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}
