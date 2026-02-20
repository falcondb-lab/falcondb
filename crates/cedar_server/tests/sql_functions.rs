#![allow(clippy::approx_constant, clippy::useless_vec)]

mod common;
use common::*;

#[test]
fn test_string_agg() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE tags (id INT PRIMARY KEY, category TEXT, tag TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, cat, tag) in [(1,"fruit","apple"),(2,"fruit","banana"),(3,"veg","carrot"),(4,"fruit","cherry"),(5,"veg","daikon")] {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO tags VALUES ({}, '{}', '{}')", id, cat, tag), Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // STRING_AGG with GROUP BY
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT category, STRING_AGG(tag, ', ') FROM tags GROUP BY category ORDER BY category",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("fruit".into()));
            // STRING_AGG order depends on scan order; check all parts present
            if let Datum::Text(ref s) = rows[0].values[1] {
                let mut parts: Vec<&str> = s.split(", ").collect();
                parts.sort();
                assert_eq!(parts, vec!["apple", "banana", "cherry"]);
            } else { panic!("Expected Text for STRING_AGG fruit"); }
            assert_eq!(rows[1].values[0], Datum::Text("veg".into()));
            if let Datum::Text(ref s) = rows[1].values[1] {
                let mut parts: Vec<&str> = s.split(", ").collect();
                parts.sort();
                assert_eq!(parts, vec!["carrot", "daikon"]);
            } else { panic!("Expected Text for STRING_AGG veg"); }
        }
        _ => panic!("Expected Query for STRING_AGG"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_aggregate_expressions() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE orders (id INT PRIMARY KEY, product TEXT, price INT, qty INT, shipped BOOL)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, product, price, qty, shipped) in [
        (1, "Widget", 10, 5, true),
        (2, "Widget", 10, 3, false),
        (3, "Gadget", 20, 2, true),
        (4, "Gadget", 20, 4, true),
        (5, "Gadget", 20, 1, false),
    ] {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO orders VALUES ({}, '{}', {}, {}, {})", id, product, price, qty, shipped),
            Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // SUM(price * qty) 鈥?aggregate on expression
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT product, SUM(price * qty) FROM orders GROUP BY product ORDER BY product",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Gadget".into()));
            assert_eq!(rows[0].values[1], Datum::Int64(140)); // 20*2 + 20*4 + 20*1
            assert_eq!(rows[1].values[0], Datum::Text("Widget".into()));
            assert_eq!(rows[1].values[1], Datum::Int64(80)); // 10*5 + 10*3
        }
        _ => panic!("Expected Query for SUM(price*qty)"),
    }

    // BOOL_AND / BOOL_OR
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT product, BOOL_AND(shipped), BOOL_OR(shipped) FROM orders GROUP BY product ORDER BY product",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            // Gadget: true, true, false -> AND=false, OR=true
            assert_eq!(rows[0].values[1], Datum::Boolean(false));
            assert_eq!(rows[0].values[2], Datum::Boolean(true));
            // Widget: true, false -> AND=false, OR=true
            assert_eq!(rows[1].values[1], Datum::Boolean(false));
            assert_eq!(rows[1].values[2], Datum::Boolean(true));
        }
        _ => panic!("Expected Query for BOOL_AND/BOOL_OR"),
    }

    // AVG(price * qty) 鈥?expression in AVG
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT AVG(price * qty) FROM orders",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            // (50+30+40+80+20)/5 = 44.0
            assert_eq!(rows[0].values[0], Datum::Float64(44.0));
        }
        _ => panic!("Expected Query for AVG(price*qty)"),
    }

    // TO_CHAR
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT TO_CHAR(TIMESTAMP '2024-06-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS')",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("2024-06-15 14:30:00".into()));
        }
        _ => panic!("Expected Query for TO_CHAR"),
    }

    // CONCAT_WS
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CONCAT_WS('-', 'a', 'b', 'c')", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a-b-c".into()));
        }
        _ => panic!("Expected Query for CONCAT_WS"),
    }

    // PI
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT PI()", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Float64(std::f64::consts::PI));
        }
        _ => panic!("Expected Query for PI"),
    }

    // MIN/MAX on expression
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT MIN(price * qty), MAX(price * qty) FROM orders",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(20));  // min: 20*1
            assert_eq!(rows[0].values[1], Datum::Int64(80));  // max: 20*4
        }
        _ => panic!("Expected Query for MIN/MAX expr"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_window_functions_extended() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE wf_test (id INT PRIMARY KEY, dept TEXT, salary INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, dept, salary) in [
        (1, "A", 100), (2, "A", 200), (3, "A", 300),
        (4, "B", 150), (5, "B", 250),
    ] {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO wf_test VALUES ({}, '{}', {})", id, dept, salary),
            Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // LAG
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, salary, LAG(salary, 1) OVER (PARTITION BY dept ORDER BY id) FROM wf_test ORDER BY id",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            // dept A: id=1 lag=NULL, id=2 lag=100, id=3 lag=200
            assert!(matches!(rows[0].values[2], Datum::Null));
            assert_eq!(rows[1].values[2], Datum::Int64(100));
            assert_eq!(rows[2].values[2], Datum::Int64(200));
            // dept B: id=4 lag=NULL, id=5 lag=150
            assert!(matches!(rows[3].values[2], Datum::Null));
            assert_eq!(rows[4].values[2], Datum::Int64(150));
        }
        _ => panic!("Expected Query for LAG"),
    }

    // LEAD
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, salary, LEAD(salary, 1) OVER (PARTITION BY dept ORDER BY id) FROM wf_test ORDER BY id",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            // dept A: id=1 lead=200, id=2 lead=300, id=3 lead=NULL
            assert_eq!(rows[0].values[2], Datum::Int64(200));
            assert_eq!(rows[1].values[2], Datum::Int64(300));
            assert!(matches!(rows[2].values[2], Datum::Null));
            // dept B: id=4 lead=250, id=5 lead=NULL
            assert_eq!(rows[3].values[2], Datum::Int64(250));
            assert!(matches!(rows[4].values[2], Datum::Null));
        }
        _ => panic!("Expected Query for LEAD"),
    }

    // FIRST_VALUE / LAST_VALUE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY id), LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY id) FROM wf_test ORDER BY id",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            // dept A: first=100, last=300
            assert_eq!(rows[0].values[1], Datum::Int64(100));
            assert_eq!(rows[0].values[2], Datum::Int64(300));
            // dept B: first=150, last=250
            assert_eq!(rows[3].values[1], Datum::Int64(150));
            assert_eq!(rows[3].values[2], Datum::Int64(250));
        }
        _ => panic!("Expected Query for FIRST_VALUE/LAST_VALUE"),
    }

    // NTILE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, NTILE(2) OVER (ORDER BY id) FROM wf_test ORDER BY id",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            // 5 rows, 2 buckets: [1,1,1,2,2] or similar
            let buckets: Vec<i64> = rows.iter().map(|r| {
                if let Datum::Int64(v) = &r.values[1] { *v } else { 0 }
            }).collect();
            // All should be 1 or 2, and bucket 1 should come before bucket 2
            assert!(buckets.iter().all(|&b| b == 1 || b == 2));
            assert!(buckets[0] <= buckets[4]);
        }
        _ => panic!("Expected Query for NTILE"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_having_with_aggregates() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, region, amount) in [
        (1, "East", 100), (2, "East", 200), (3, "East", 50),
        (4, "West", 300), (5, "West", 400),
        (6, "North", 10),
    ] {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO sales VALUES ({}, '{}', {})", id, region, amount),
            Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // HAVING COUNT(*) > 1
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 1 ORDER BY region",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2); // East(3), West(2) 鈥?North(1) excluded
            assert_eq!(rows[0].values[0], Datum::Text("East".into()));
            assert_eq!(rows[0].values[1], Datum::Int64(3));
            assert_eq!(rows[1].values[0], Datum::Text("West".into()));
            assert_eq!(rows[1].values[1], Datum::Int64(2));
        }
        _ => panic!("Expected Query for HAVING COUNT(*)"),
    }

    // HAVING SUM(amount) > 200
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 200 ORDER BY region",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2); // East(350), West(700) 鈥?North(10) excluded
            assert_eq!(rows[0].values[0], Datum::Text("East".into()));
            assert_eq!(rows[0].values[1], Datum::Int64(350));
            assert_eq!(rows[1].values[0], Datum::Text("West".into()));
            assert_eq!(rows[1].values[1], Datum::Int64(700));
        }
        _ => panic!("Expected Query for HAVING SUM(amount)"),
    }

    // HAVING with compound condition: COUNT(*) >= 2 AND AVG(amount) > 100
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT region FROM sales GROUP BY region HAVING COUNT(*) >= 2 AND AVG(amount) > 100 ORDER BY region",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            // East: count=3, avg=116.67 -> yes; West: count=2, avg=350 -> yes; North excluded
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("East".into()));
            assert_eq!(rows[1].values[0], Datum::Text("West".into()));
        }
        _ => panic!("Expected Query for HAVING compound"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_order_by_expression() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE items (id INT PRIMARY KEY, price INT, qty INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, price, qty) in [(1, 10, 3), (2, 5, 8), (3, 20, 1)] {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO items VALUES ({}, {}, {})", id, price, qty),
            Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ORDER BY expression: price * qty DESC
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, price, qty FROM items ORDER BY price * qty DESC",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            // 2: 5*8=40, 1: 10*3=30, 3: 20*1=20
            assert_eq!(rows[0].values[0], Datum::Int64(2));
            assert_eq!(rows[1].values[0], Datum::Int64(1));
            assert_eq!(rows[2].values[0], Datum::Int64(3));
        }
        _ => panic!("Expected Query for ORDER BY expression"),
    }

    // ORDER BY function expression
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, price FROM items ORDER BY ABS(price - 12) ASC",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            // id=1: |10-12|=2, id=2: |5-12|=7, id=3: |20-12|=8 鈫?sorted: 1,2,3
            assert_eq!(rows[0].values[0], Datum::Int64(1));
            assert_eq!(rows[1].values[0], Datum::Int64(2));
            assert_eq!(rows[2].values[0], Datum::Int64(3));
        }
        _ => panic!("Expected Query for ORDER BY function expr"),
    }

    // OFFSET + LIMIT in aggregate query
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id FROM items ORDER BY id LIMIT 2 OFFSET 1",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Int64(2));
            assert_eq!(rows[1].values[0], Datum::Int64(3));
        }
        _ => panic!("Expected Query for OFFSET+LIMIT"),
    }

    // PERCENT_RANK
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, PERCENT_RANK() OVER (ORDER BY price) FROM items ORDER BY id",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            // Sorted by price: id=2(5), id=1(10), id=3(20) 鈫?ranks 0.0, 0.5, 1.0
            assert_eq!(rows[0].values[1], Datum::Float64(0.5));  // id=1, price=10
            assert_eq!(rows[1].values[1], Datum::Float64(0.0));  // id=2, price=5
            assert_eq!(rows[2].values[1], Datum::Float64(1.0));  // id=3, price=20
        }
        _ => panic!("Expected Query for PERCENT_RANK"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_update_with_expressions() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO products VALUES (1, 'Widget', 100)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO products VALUES (2, 'Gadget', 200)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO products VALUES (3, 'Doohickey', 50)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // UPDATE with expression: price = price * 2
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "UPDATE products SET price = price * 2 WHERE price < 150",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(*rows_affected, 2); // Widget(100) and Doohickey(50)
        }
        _ => panic!("Expected Dml for UPDATE"),
    }

    // Verify
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, price FROM products ORDER BY id",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[1], Datum::Int64(200)); // 100*2
            assert_eq!(rows[1].values[1], Datum::Int64(200)); // unchanged
            assert_eq!(rows[2].values[1], Datum::Int64(100)); // 50*2
        }
        _ => panic!("Expected Query"),
    }

    // LIKE with pattern
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM products WHERE name LIKE 'G%' ORDER BY name",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("Gadget".into()));
        }
        _ => panic!("Expected Query for LIKE"),
    }

    // CONCAT_WS with mixed types
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CONCAT_WS(' - ', name, price) FROM products WHERE id = 1",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("Widget - 200".into()));
        }
        _ => panic!("Expected Query for CONCAT_WS"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_string_functions_translate_split() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE words (id INT PRIMARY KEY, word TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO words VALUES (1, 'hello world')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO words VALUES (2, 'foo-bar-baz')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO words VALUES (3, '  spaces  ')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // TRANSLATE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT TRANSLATE(word, 'helo', 'HELO') FROM words WHERE id = 1",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("HELLO wOrLd".into()));
        }
        _ => panic!("Expected Query for TRANSLATE"),
    }

    // SPLIT_PART
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT SPLIT_PART(word, '-', 2) FROM words WHERE id = 2",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("bar".into()));
        }
        _ => panic!("Expected Query for SPLIT_PART"),
    }

    // LTRIM / RTRIM
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT LTRIM(word), RTRIM(word) FROM words WHERE id = 3",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("spaces  ".into()));
            assert_eq!(rows[0].values[1], Datum::Text("  spaces".into()));
        }
        _ => panic!("Expected Query for LTRIM/RTRIM"),
    }

    // BTRIM with custom chars
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT BTRIM('xxhelloxx', 'x')",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("hello".into()));
        }
        _ => panic!("Expected Query for BTRIM custom"),
    }

    // NTH_VALUE window function
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, NTH_VALUE(word, 2) OVER (ORDER BY id) FROM words ORDER BY id",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[1], Datum::Text("foo-bar-baz".into()));
            assert_eq!(rows[1].values[1], Datum::Text("foo-bar-baz".into()));
            assert_eq!(rows[2].values[1], Datum::Text("foo-bar-baz".into()));
        }
        _ => panic!("Expected Query for NTH_VALUE window"),
    }

    // CUME_DIST
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, CUME_DIST() OVER (ORDER BY id) FROM words ORDER BY id",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            let cd1 = if let Datum::Float64(f) = &rows[0].values[1] { *f } else { panic!("expected f64") };
            let cd2 = if let Datum::Float64(f) = &rows[1].values[1] { *f } else { panic!("expected f64") };
            let cd3 = if let Datum::Float64(f) = &rows[2].values[1] { *f } else { panic!("expected f64") };
            assert!((cd1 - 1.0/3.0).abs() < 0.001);
            assert!((cd2 - 2.0/3.0).abs() < 0.001);
            assert!((cd3 - 1.0).abs() < 0.001);
        }
        _ => panic!("Expected Query for CUME_DIST window"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_math_functions_extended() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE nums (id INT PRIMARY KEY, val INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO nums VALUES (1, 10)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "INSERT INTO nums VALUES (2, 7)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // MOD
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT MOD(val, 3) FROM nums WHERE id = 1", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(1)); // 10 % 3 = 1
        }
        _ => panic!("Expected Query for MOD"),
    }

    // DEGREES and RADIANS round-trip
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT DEGREES(PI()), RADIANS(180.0)", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            let deg = if let Datum::Float64(f) = &rows[0].values[0] { *f } else { panic!("f64") };
            let rad = if let Datum::Float64(f) = &rows[0].values[1] { *f } else { panic!("f64") };
            assert!((deg - 180.0).abs() < 0.0001);
            assert!((rad - std::f64::consts::PI).abs() < 0.0001);
        }
        _ => panic!("Expected Query for DEGREES/RADIANS"),
    }

    // HAVING with SUM expression + ORDER BY expression combined
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT val, COUNT(*) FROM nums GROUP BY val HAVING COUNT(*) >= 1 ORDER BY val DESC",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Int64(10));
            assert_eq!(rows[1].values[0], Datum::Int64(7));
        }
        _ => panic!("Expected Query for HAVING+ORDER BY"),
    }

    // DELETE with expression WHERE + RETURNING
    let result = run_sql(&storage, &txn_mgr, &executor,
        "DELETE FROM nums WHERE val * 2 > 15 RETURNING id, val",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1); // val=10 鈫?10*2=20 > 15
            assert_eq!(rows[0].values[0], Datum::Int64(1));
            assert_eq!(rows[0].values[1], Datum::Int64(10));
        }
        _ => panic!("Expected Query for DELETE RETURNING"),
    }

    // Verify only val=7 remains
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COUNT(*) FROM nums", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(1));
        }
        _ => panic!("Expected Query for COUNT after DELETE"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_array_agg() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE team (id INT PRIMARY KEY, dept TEXT, name TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, dept, name) in [
        (1, "Eng", "Alice"), (2, "Eng", "Bob"), (3, "Sales", "Carol"),
        (4, "Sales", "Dave"), (5, "Eng", "Eve"),
    ] {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO team VALUES ({}, '{}', '{}')", id, dept, name),
            Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_AGG basic
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT dept, ARRAY_AGG(name) FROM team GROUP BY dept ORDER BY dept",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Eng".into()));
            // Eng: Alice, Bob, Eve
            if let Datum::Array(arr) = &rows[0].values[1] {
                assert_eq!(arr.len(), 3);
            } else {
                panic!("Expected Array for ARRAY_AGG");
            }
            assert_eq!(rows[1].values[0], Datum::Text("Sales".into()));
            if let Datum::Array(arr) = &rows[1].values[1] {
                assert_eq!(arr.len(), 2);
            } else {
                panic!("Expected Array for ARRAY_AGG");
            }
        }
        _ => panic!("Expected Query for ARRAY_AGG"),
    }

    // ARRAY_AGG with DISTINCT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_AGG(DISTINCT dept) FROM team",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 2); // Eng, Sales
            } else {
                panic!("Expected Array for ARRAY_AGG DISTINCT");
            }
        }
        _ => panic!("Expected Query for ARRAY_AGG DISTINCT"),
    }

    // OFFSET in aggregate query
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT dept, COUNT(*) FROM team GROUP BY dept ORDER BY dept LIMIT 1 OFFSET 1",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("Sales".into()));
            assert_eq!(rows[0].values[1], Datum::Int64(2));
        }
        _ => panic!("Expected Query for OFFSET in aggregate"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_array_literals_and_functions() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE scores (id INT PRIMARY KEY, student TEXT, score INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, student, score) in [
        (1, "Alice", 90), (2, "Bob", 85), (3, "Alice", 95), (4, "Bob", 70),
    ] {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO scores VALUES ({}, '{}', {})", id, student, score),
            Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY literal in SELECT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY[1, 2, 3]", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Datum::Int64(1));
                assert_eq!(arr[1], Datum::Int64(2));
                assert_eq!(arr[2], Datum::Int64(3));
            } else {
                panic!("Expected Array literal result");
            }
        }
        _ => panic!("Expected Query for ARRAY literal"),
    }

    // ARRAY_AGG ordered by student name
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT student, ARRAY_AGG(score) FROM scores GROUP BY student ORDER BY student",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Alice".into()));
            if let Datum::Array(arr) = &rows[0].values[1] {
                assert_eq!(arr.len(), 2); // 90, 95
            } else {
                panic!("Expected Array for Alice scores");
            }
            assert_eq!(rows[1].values[0], Datum::Text("Bob".into()));
            if let Datum::Array(arr) = &rows[1].values[1] {
                assert_eq!(arr.len(), 2); // 85, 70
            } else {
                panic!("Expected Array for Bob scores");
            }
        }
        _ => panic!("Expected Query for ARRAY_AGG grouped"),
    }

    // Test PG text format for arrays
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY[10, 20, 30]", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                // Verify PG text format
                let pg = rows[0].values[0].to_pg_text().unwrap();
                assert_eq!(pg, "{10,20,30}");
                assert_eq!(arr.len(), 3);
            } else {
                panic!("Expected Array");
            }
        }
        _ => panic!("Expected Query for ARRAY PG format"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_array_scalar_functions() {
    let (storage, txn_mgr, executor) = setup();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_LENGTH
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_LENGTH(ARRAY[10, 20, 30], 1)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(3));
        }
        _ => panic!("Expected Query for ARRAY_LENGTH"),
    }

    // CARDINALITY
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CARDINALITY(ARRAY[1, 2, 3, 4, 5])", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(5));
        }
        _ => panic!("Expected Query for CARDINALITY"),
    }

    // ARRAY_APPEND
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_APPEND(ARRAY[1, 2], 3)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[2], Datum::Int64(3));
            } else {
                panic!("Expected Array for ARRAY_APPEND");
            }
        }
        _ => panic!("Expected Query for ARRAY_APPEND"),
    }

    // ARRAY_PREPEND
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_PREPEND(0, ARRAY[1, 2])", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Datum::Int64(0));
                assert_eq!(arr[1], Datum::Int64(1));
            } else {
                panic!("Expected Array for ARRAY_PREPEND");
            }
        }
        _ => panic!("Expected Query for ARRAY_PREPEND"),
    }

    // ARRAY_POSITION found
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_POSITION(ARRAY[10, 20, 30], 20)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2));
        }
        _ => panic!("Expected Query for ARRAY_POSITION found"),
    }

    // ARRAY_POSITION not found 鈫?NULL
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_POSITION(ARRAY[10, 20, 30], 99)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert!(matches!(rows[0].values[0], Datum::Null));
        }
        _ => panic!("Expected Query for ARRAY_POSITION not found"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_ilike_and_cast_extended() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE people (id INT PRIMARY KEY, name TEXT, age INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, name, age) in [
        (1, "Alice Smith", 30), (2, "bob jones", 25), (3, "CAROL KING", 40),
    ] {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO people VALUES ({}, '{}', {})", id, name, age),
            Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ILIKE case-insensitive matching
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM people WHERE name ILIKE '%smith%' ORDER BY id",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("Alice Smith".into()));
        }
        _ => panic!("Expected Query for ILIKE"),
    }

    // ILIKE with uppercase pattern
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM people WHERE name ILIKE 'BOB%'",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("bob jones".into()));
        }
        _ => panic!("Expected Query for ILIKE uppercase"),
    }

    // NOT ILIKE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COUNT(*) FROM people WHERE name NOT ILIKE '%king%'",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2));
        }
        _ => panic!("Expected Query for NOT ILIKE"),
    }

    // CAST to NUMERIC
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CAST(age AS NUMERIC) / 7 FROM people WHERE id = 1",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(f) = &rows[0].values[0] {
                assert!((*f - 30.0/7.0).abs() < 0.001);
            } else {
                panic!("Expected Float64 for CAST to NUMERIC");
            }
        }
        _ => panic!("Expected Query for CAST NUMERIC"),
    }

    // CAST text to int
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CAST('42' AS INT) + 8",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(50));
        }
        _ => panic!("Expected Query for CAST TEXT to INT"),
    }

    // CAST bool to int
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CAST(true AS INT), CAST(false AS INT)",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Int32(0));
        }
        _ => panic!("Expected Query for CAST BOOL to INT"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_generate_series() {
    let (storage, txn_mgr, executor) = setup();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Basic GENERATE_SERIES
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT * FROM generate_series(1, 5)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row.values[0], Datum::Int64((i + 1) as i64));
            }
        }
        _ => panic!("Expected Query for GENERATE_SERIES"),
    }

    // GENERATE_SERIES with step
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT * FROM generate_series(0, 10, 3)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 4); // 0, 3, 6, 9
            assert_eq!(rows[0].values[0], Datum::Int64(0));
            assert_eq!(rows[1].values[0], Datum::Int64(3));
            assert_eq!(rows[2].values[0], Datum::Int64(6));
            assert_eq!(rows[3].values[0], Datum::Int64(9));
        }
        _ => panic!("Expected Query for GENERATE_SERIES with step"),
    }

    // GENERATE_SERIES with WHERE filter
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT generate_series FROM generate_series(1, 10) WHERE generate_series > 7",
        Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3); // 8, 9, 10
            assert_eq!(rows[0].values[0], Datum::Int64(8));
            assert_eq!(rows[1].values[0], Datum::Int64(9));
            assert_eq!(rows[2].values[0], Datum::Int64(10));
        }
        _ => panic!("Expected Query for GENERATE_SERIES with WHERE"),
    }

    // GENERATE_SERIES with aggregate
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT SUM(generate_series) FROM generate_series(1, 100)",
        Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(5050)); // n*(n+1)/2
        }
        _ => panic!("Expected Query for GENERATE_SERIES SUM"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_comprehensive_recent_features() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price NUMERIC, qty SMALLINT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, name, price, qty) in [
        (1, "Pen", "1.50", 100), (2, "Book", "12.99", 50),
        (3, "Eraser", "0.75", 200), (4, "Ruler", "3.25", 75),
    ] {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO items VALUES ({}, '{}', {}, {})", id, name, price, qty),
            Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // GENERATE_SERIES with LIMIT/OFFSET
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT * FROM generate_series(1, 20) LIMIT 5 OFFSET 10",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            assert_eq!(rows[0].values[0], Datum::Int64(11));
            assert_eq!(rows[4].values[0], Datum::Int64(15));
        }
        _ => panic!("Expected Query for GENERATE_SERIES LIMIT/OFFSET"),
    }

    // HAVING with COUNT(*) + ORDER BY expression
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, price * qty FROM items GROUP BY name, price, qty HAVING price * qty > 100 ORDER BY price * qty DESC",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert!(rows.len() >= 2); // Book (649.5), Ruler (243.75), Pen (150), Eraser (150)
        }
        _ => panic!("Expected Query for HAVING with expression"),
    }

    // ARRAY_AGG collects all names
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_AGG(name) FROM items",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 4);
            } else {
                panic!("Expected Array from ARRAY_AGG");
            }
        }
        _ => panic!("Expected Query for ARRAY_AGG"),
    }

    // CAST NUMERIC column to TEXT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CAST(price AS TEXT) FROM items WHERE id = 2",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("12.99".into()));
        }
        _ => panic!("Expected Query for CAST to TEXT"),
    }

    // BETWEEN with expressions
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COUNT(*) FROM items WHERE price * qty BETWEEN 100 AND 300",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            // Pen=150, Eraser=150, Ruler=243.75 鈫?3 items
            assert_eq!(rows[0].values[0], Datum::Int64(3));
        }
        _ => panic!("Expected Query for BETWEEN with expressions"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_array_subscript_and_concat() {
    let (storage, txn_mgr, executor) = setup();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // Array subscript basic (1-indexed)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT (ARRAY[10, 20, 30])[2]", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(20));
        }
        _ => panic!("Expected Query for array subscript"),
    }

    // Array subscript first element
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT (ARRAY['a', 'b', 'c'])[1]", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a".into()));
        }
        _ => panic!("Expected Query for array subscript text"),
    }

    // Array subscript out of bounds 鈫?NULL
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT (ARRAY[1, 2, 3])[5]", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert!(matches!(rows[0].values[0], Datum::Null));
        }
        _ => panic!("Expected Query for array subscript out of bounds"),
    }

    // ARRAY_APPEND then subscript
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT (ARRAY_APPEND(ARRAY[1, 2], 3))[3]", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(3));
        }
        _ => panic!("Expected Query for ARRAY_APPEND then subscript"),
    }

    // String || concatenation still works
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT 'hello' || ' ' || 'world'", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("hello world".into()));
        }
        _ => panic!("Expected Query for string concat"),
    }

    // Array || Array concatenation
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY[1, 2] || ARRAY[3, 4]", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 4);
                assert_eq!(arr[0], Datum::Int64(1));
                assert_eq!(arr[3], Datum::Int64(4));
            } else {
                panic!("Expected Array for array concat");
            }
        }
        _ => panic!("Expected Query for array || array"),
    }

    // ARRAY_REMOVE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_REMOVE(ARRAY[1, 2, 3, 2, 1], 2)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 3); // [1, 3, 1]
                assert_eq!(arr[0], Datum::Int64(1));
                assert_eq!(arr[1], Datum::Int64(3));
                assert_eq!(arr[2], Datum::Int64(1));
            } else {
                panic!("Expected Array for ARRAY_REMOVE");
            }
        }
        _ => panic!("Expected Query for ARRAY_REMOVE"),
    }

    // ARRAY_REPLACE
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_REPLACE(ARRAY[1, 2, 3, 2], 2, 99)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 4);
                assert_eq!(arr[1], Datum::Int64(99));
                assert_eq!(arr[3], Datum::Int64(99));
            } else {
                panic!("Expected Array for ARRAY_REPLACE");
            }
        }
        _ => panic!("Expected Query for ARRAY_REPLACE"),
    }

    // ARRAY_CAT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_CAT(ARRAY[1, 2], ARRAY[3, 4, 5])", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 5);
            } else {
                panic!("Expected Array for ARRAY_CAT");
            }
        }
        _ => panic!("Expected Query for ARRAY_CAT"),
    }

    // ARRAY_TO_STRING
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_TO_STRING(ARRAY[1, 2, 3], ',')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("1,2,3".into()));
        }
        _ => panic!("Expected Query for ARRAY_TO_STRING"),
    }

    // STRING_TO_ARRAY
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_TO_ARRAY('a,b,c', ',')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Datum::Text("a".into()));
                assert_eq!(arr[1], Datum::Text("b".into()));
                assert_eq!(arr[2], Datum::Text("c".into()));
            } else {
                panic!("Expected Array for STRING_TO_ARRAY");
            }
        }
        _ => panic!("Expected Query for STRING_TO_ARRAY"),
    }

    // ARRAY_TO_STRING with delimiter
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_TO_STRING(ARRAY['hello', 'world'], ' ')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("hello world".into()));
        }
        _ => panic!("Expected Query for ARRAY_TO_STRING space"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_array_comprehensive_end_to_end() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE tags (id INT PRIMARY KEY, name TEXT, scores TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, name, scores) in [
        (1, "Alice", "90,85,92"), (2, "Bob", "78,88,95"), (3, "Carol", "100,100,100"),
    ] {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO tags VALUES ({}, '{}', '{}')", id, name, scores),
            Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // STRING_TO_ARRAY on column data + ARRAY_LENGTH
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_LENGTH(STRING_TO_ARRAY(scores, ','), 1) FROM tags WHERE id = 1",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(3));
        }
        _ => panic!("Expected Query for STRING_TO_ARRAY + ARRAY_LENGTH"),
    }

    // ARRAY_UPPER / ARRAY_LOWER
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_LOWER(ARRAY[10, 20, 30], 1), ARRAY_UPPER(ARRAY[10, 20, 30], 1)",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(1));
            assert_eq!(rows[0].values[1], Datum::Int64(3));
        }
        _ => panic!("Expected Query for ARRAY_LOWER/ARRAY_UPPER"),
    }

    // GENERATE_SERIES + ARRAY_AGG 鈫?build array from series
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_AGG(generate_series) FROM generate_series(1, 5)",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 5);
                assert_eq!(arr[0], Datum::Int64(1));
                assert_eq!(arr[4], Datum::Int64(5));
            } else {
                panic!("Expected Array from ARRAY_AGG(generate_series)");
            }
        }
        _ => panic!("Expected Query for GENERATE_SERIES + ARRAY_AGG"),
    }

    // Nested array operations: ARRAY_TO_STRING(ARRAY_APPEND(...), ',')
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_TO_STRING(ARRAY_APPEND(ARRAY['x', 'y'], 'z'), '-')",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("x-y-z".into()));
        }
        _ => panic!("Expected Query for nested array ops"),
    }

    // Array subscript with ARRAY_PREPEND
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT (ARRAY_PREPEND(0, ARRAY[1, 2, 3]))[1]",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(0));
        }
        _ => panic!("Expected Query for ARRAY_PREPEND subscript"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_array_column_type() {
    let (storage, txn_mgr, executor) = setup();

    // CREATE TABLE with ARRAY column type
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE with_arrays (id INT PRIMARY KEY, labels TEXT[], scores INT[])", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO with_arrays VALUES (1, ARRAY['a', 'b', 'c'], ARRAY[10, 20, 30])",
        Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO with_arrays VALUES (2, ARRAY['x', 'y'], ARRAY[100, 200])",
        Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // SELECT array column
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT labels FROM with_arrays WHERE id = 1",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Datum::Text("a".into()));
            } else {
                panic!("Expected Array from labels column");
            }
        }
        _ => panic!("Expected Query for array column select"),
    }

    // Array subscript on column
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT (scores)[2] FROM with_arrays WHERE id = 1",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(20));
        }
        _ => panic!("Expected Query for array column subscript"),
    }

    // CARDINALITY on column
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CARDINALITY(labels) FROM with_arrays ORDER BY id",
        Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Int64(3));
            assert_eq!(rows[1].values[0], Datum::Int64(2));
        }
        _ => panic!("Expected Query for CARDINALITY on array column"),
    }

    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_edge_cases_and_null_handling() {
    let (storage, txn_mgr, executor) = setup();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // NULL in COALESCE chain
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COALESCE(NULL, NULL, 42, 99)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(42));
        }
        _ => panic!("Expected Query for COALESCE"),
    }

    // NULLIF returns NULL when equal
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT NULLIF(5, 5), NULLIF(5, 3)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert!(matches!(rows[0].values[0], Datum::Null));
            assert_eq!(rows[0].values[1], Datum::Int64(5));
        }
        _ => panic!("Expected Query for NULLIF"),
    }

    // CASE WHEN with multiple branches
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CASE WHEN 1 > 2 THEN 'a' WHEN 2 > 1 THEN 'b' ELSE 'c' END",
        Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("b".into()));
        }
        _ => panic!("Expected Query for CASE WHEN"),
    }

    // Empty ARRAY
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CARDINALITY(ARRAY[]::INT[])", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(0));
        }
        _ => panic!("Expected Query for empty array CARDINALITY"),
    }

    // Nested CASE + CAST
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CAST(CASE WHEN true THEN '123' ELSE '0' END AS INT)",
        Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int32(123));
        }
        _ => panic!("Expected Query for nested CASE+CAST"),
    }

    // GREATEST / LEAST with multiple args
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT GREATEST(3, 7, 1, 9, 2), LEAST(3, 7, 1, 9, 2)",
        Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(9));
            assert_eq!(rows[0].values[1], Datum::Int64(1));
        }
        _ => panic!("Expected Query for GREATEST/LEAST"),
    }

    // GENERATE_SERIES with negative step
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT * FROM generate_series(5, 1, -1)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5); // 5, 4, 3, 2, 1
            assert_eq!(rows[0].values[0], Datum::Int64(5));
            assert_eq!(rows[4].values[0], Datum::Int64(1));
        }
        _ => panic!("Expected Query for GENERATE_SERIES negative step"),
    }

    // String functions chain
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT UPPER(REVERSE(TRIM('  hello  ')))", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("OLLEH".into()));
        }
        _ => panic!("Expected Query for chained string functions"),
    }

    // Math expression chain
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ABS(FLOOR(-3.7))", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Float64(4.0));
        }
        _ => panic!("Expected Query for ABS(FLOOR)"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_overlay_and_regexp_replace() {
    let (storage, txn_mgr, executor) = setup();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // OVERLAY basic: replace 5 chars starting at position 7
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT OVERLAY('hello world' PLACING 'RUST' FROM 7 FOR 5)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("hello RUST".into()));
        }
        _ => panic!("Expected Query for OVERLAY"),
    }

    // OVERLAY without FOR (defaults to replacement length)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT OVERLAY('abcdef' PLACING 'XY' FROM 3)", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("abXYef".into()));
        }
        _ => panic!("Expected Query for OVERLAY no count"),
    }

    // REGEXP_REPLACE first match only
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT REGEXP_REPLACE('foo bar foo', 'foo', 'baz')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("baz bar foo".into()));
        }
        _ => panic!("Expected Query for REGEXP_REPLACE first"),
    }

    // REGEXP_REPLACE global flag
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT REGEXP_REPLACE('foo bar foo', 'foo', 'baz', 'g')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("baz bar baz".into()));
        }
        _ => panic!("Expected Query for REGEXP_REPLACE global"),
    }

    // REGEXP_REPLACE with regex pattern
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT REGEXP_REPLACE('abc123def456', '[0-9]+', '#', 'g')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("abc#def#".into()));
        }
        _ => panic!("Expected Query for REGEXP_REPLACE regex"),
    }

    // STARTS_WITH
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STARTS_WITH('hello world', 'hello'), STARTS_WITH('hello', 'world')",
        Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Boolean(true));
            assert_eq!(rows[0].values[1], Datum::Boolean(false));
        }
        _ => panic!("Expected Query for STARTS_WITH"),
    }

    // ENDS_WITH
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ENDS_WITH('hello world', 'world'), ENDS_WITH('hello', 'xyz')",
        Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Boolean(true));
            assert_eq!(rows[0].values[1], Datum::Boolean(false));
        }
        _ => panic!("Expected Query for ENDS_WITH"),
    }

    // REGEXP_MATCH with capture groups
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT REGEXP_MATCH('abc123def', '([a-z]+)([0-9]+)')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Array(arr) = &rows[0].values[0] {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], Datum::Text("abc".into()));
                assert_eq!(arr[1], Datum::Text("123".into()));
            } else {
                panic!("Expected Array from REGEXP_MATCH");
            }
        }
        _ => panic!("Expected Query for REGEXP_MATCH"),
    }

    // REGEXP_MATCH no match 鈫?NULL
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT REGEXP_MATCH('hello', '[0-9]+')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert!(matches!(rows[0].values[0], Datum::Null));
        }
        _ => panic!("Expected Query for REGEXP_MATCH no match"),
    }

    // MD5 hash
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT MD5('hello')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("5d41402abc4b2a76b9719d911017c592".into()));
        }
        _ => panic!("Expected Query for MD5"),
    }

    // REGEXP_COUNT
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT REGEXP_COUNT('hello world hello', 'hello')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2));
        }
        _ => panic!("Expected Query for REGEXP_COUNT"),
    }

    // REGEXP_COUNT with regex pattern
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT REGEXP_COUNT('abc123def456ghi', '[0-9]+')", Some(&txn)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2));
        }
        _ => panic!("Expected Query for REGEXP_COUNT regex"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}
