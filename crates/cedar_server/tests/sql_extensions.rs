#![allow(clippy::approx_constant, clippy::useless_vec)]

mod common;
use common::*;

// ── M7.1: Sequence tests ────────────────────────────────────────────

#[test]
fn test_sequence_create_and_nextval() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // CREATE SEQUENCE
    let result = run_sql(&storage, &txn_mgr, &executor, "CREATE SEQUENCE my_seq", None).unwrap();
    match result {
        ExecutionResult::Ddl { message } => assert!(message.contains("CREATE SEQUENCE")),
        _ => panic!("Expected Ddl result"),
    }

    // nextval returns 1, 2, 3
    for expected in 1..=3i64 {
        let result = run_sql(&storage, &txn_mgr, &executor,
            "SELECT nextval('my_seq')", Some(&txn)).unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values[0], Datum::Int64(expected));
            }
            _ => panic!("Expected Query result"),
        }
    }
}

#[test]
fn test_sequence_currval() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor, "CREATE SEQUENCE counter_seq", None).unwrap();

    // Advance to 1
    run_sql(&storage, &txn_mgr, &executor, "SELECT nextval('counter_seq')", Some(&txn)).unwrap();
    // Advance to 2
    run_sql(&storage, &txn_mgr, &executor, "SELECT nextval('counter_seq')", Some(&txn)).unwrap();

    // currval should return 2
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT currval('counter_seq')", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_sequence_setval() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor, "CREATE SEQUENCE pos_seq", None).unwrap();

    // setval to 42
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT setval('pos_seq', 42)", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(42));
        }
        _ => panic!("Expected Query result"),
    }

    // nextval should return 43
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT nextval('pos_seq')", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(43));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_sequence_drop() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor, "CREATE SEQUENCE temp_seq", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "SELECT nextval('temp_seq')", Some(&txn)).unwrap();

    // DROP SEQUENCE
    let result = run_sql(&storage, &txn_mgr, &executor, "DROP SEQUENCE temp_seq", None).unwrap();
    match result {
        ExecutionResult::Ddl { message } => assert!(message.contains("DROP SEQUENCE")),
        _ => panic!("Expected Ddl result"),
    }

    // nextval on dropped sequence should fail
    let err = run_sql(&storage, &txn_mgr, &executor, "SELECT nextval('temp_seq')", Some(&txn));
    assert!(err.is_err());
}

#[test]
fn test_sequence_drop_if_exists() {
    let (storage, txn_mgr, executor) = setup();

    // DROP IF EXISTS on non-existent sequence should succeed silently
    let result = run_sql(&storage, &txn_mgr, &executor,
        "DROP SEQUENCE IF EXISTS nonexistent_seq", None).unwrap();
    match result {
        ExecutionResult::Ddl { message } => assert!(message.contains("DROP SEQUENCE")),
        _ => panic!("Expected Ddl result"),
    }
}

#[test]
fn test_sequence_show() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor, "CREATE SEQUENCE alpha_seq", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "CREATE SEQUENCE beta_seq", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "SELECT nextval('alpha_seq')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "SELECT nextval('alpha_seq')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor, "SELECT nextval('beta_seq')", Some(&txn)).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SHOW cedar_sequences", None).unwrap();
    match result {
        ExecutionResult::Query { rows, columns } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].0, "sequence_name");
            assert_eq!(columns[1].0, "current_value");
            assert_eq!(rows.len(), 2);
            // Sort by name for deterministic check
            let mut names: Vec<String> = rows.iter()
                .map(|r| match &r.values[0] { Datum::Text(s) => s.clone(), _ => panic!() })
                .collect();
            names.sort();
            assert_eq!(names, vec!["alpha_seq", "beta_seq"]);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_sequence_duplicate_create_fails() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor, "CREATE SEQUENCE dup_seq", None).unwrap();
    let err = run_sql(&storage, &txn_mgr, &executor, "CREATE SEQUENCE dup_seq", None);
    assert!(err.is_err());
}

// 鈹€鈹€ M7.2: Window function tests 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

fn setup_window_test_data() -> (Arc<StorageEngine>, Arc<TxnManager>, Arc<Executor>) {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (id, name, dept, salary) in [
        (1, "Alice",   "eng",   100),
        (2, "Bob",     "eng",   120),
        (3, "Carol",   "eng",   100),
        (4, "Dave",    "sales", 90),
        (5, "Eve",     "sales", 110),
        (6, "Frank",   "hr",    80),
    ] {
        run_sql(&storage, &txn_mgr, &executor,
            &format!("INSERT INTO employees VALUES ({}, '{}', '{}', {})", id, name, dept, salary),
            Some(&txn)).unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();
    (storage, txn_mgr, executor)
}

#[test]
fn test_window_row_number() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, ROW_NUMBER() OVER (ORDER BY salary, name) FROM employees ORDER BY salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, columns } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(rows.len(), 6);
            // salary order: Frank(80), Dave(90), Alice(100), Carol(100), Eve(110), Bob(120)
            // ROW_NUMBER is 1..6
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row.values[1], Datum::Int64((i + 1) as i64));
            }
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_rank() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, RANK() OVER (ORDER BY salary) FROM employees ORDER BY salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // Frank(80)鈫?, Dave(90)鈫?, Alice(100)鈫?, Carol(100)鈫?, Eve(110)鈫?, Bob(120)鈫?
            let ranks: Vec<i64> = rows.iter().map(|r| match &r.values[1] { Datum::Int64(v) => *v, _ => panic!() }).collect();
            assert_eq!(ranks, vec![1, 2, 3, 3, 5, 6]);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_dense_rank() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, DENSE_RANK() OVER (ORDER BY salary) FROM employees ORDER BY salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // Frank(80)鈫?, Dave(90)鈫?, Alice(100)鈫?, Carol(100)鈫?, Eve(110)鈫?, Bob(120)鈫?
            let ranks: Vec<i64> = rows.iter().map(|r| match &r.values[1] { Datum::Int64(v) => *v, _ => panic!() }).collect();
            assert_eq!(ranks, vec![1, 2, 3, 3, 4, 5]);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_partition_by() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary, name) FROM employees ORDER BY dept, salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 6);
            // eng: Alice(100)鈫?, Carol(100)鈫?, Bob(120)鈫?
            // hr: Frank(80)鈫?
            // sales: Dave(90)鈫?, Eve(110)鈫?
            let vals: Vec<(String, i64)> = rows.iter().map(|r| {
                let name = match &r.values[0] { Datum::Text(s) => s.clone(), _ => panic!() };
                let rn = match &r.values[2] { Datum::Int64(v) => *v, _ => panic!() };
                (name, rn)
            }).collect();
            // Check per-partition numbering restarts
            assert_eq!(vals[0].1, 1); // Alice eng
            assert_eq!(vals[2].1, 3); // Bob eng
            assert_eq!(vals[3].1, 1); // Frank hr
            assert_eq!(vals[4].1, 1); // Dave sales
            assert_eq!(vals[5].1, 2); // Eve sales
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_lag_lead() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    // LAG
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, salary, LAG(salary) OVER (ORDER BY salary) FROM employees ORDER BY salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // First row LAG should be NULL
            assert!(matches!(rows[0].values[2], Datum::Null));
            // Second row LAG should be first row's salary (80)
            assert_eq!(rows[1].values[2], Datum::Int64(80));
        }
        _ => panic!("Expected Query result"),
    }

    // LEAD
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, salary, LEAD(salary) OVER (ORDER BY salary) FROM employees ORDER BY salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // Last row LEAD should be NULL
            assert!(matches!(rows[5].values[2], Datum::Null));
            // First row LEAD should be second row's salary (90)
            assert_eq!(rows[0].values[2], Datum::Int64(90));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_ntile() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, NTILE(3) OVER (ORDER BY salary) FROM employees ORDER BY salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // 6 rows / 3 buckets = 2 per bucket
            let buckets: Vec<i64> = rows.iter().map(|r| match &r.values[1] { Datum::Int64(v) => *v, _ => panic!() }).collect();
            assert_eq!(buckets, vec![1, 1, 2, 2, 3, 3]);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_first_last_value() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary) FROM employees ORDER BY dept, salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // eng partition: first salary = 100 (Alice)
            assert_eq!(rows[0].values[1], Datum::Int64(100));
            assert_eq!(rows[1].values[1], Datum::Int64(100));
            assert_eq!(rows[2].values[1], Datum::Int64(100));
            // hr partition: first = 80
            assert_eq!(rows[3].values[1], Datum::Int64(80));
            // sales partition: first = 90
            assert_eq!(rows[4].values[1], Datum::Int64(90));
        }
        _ => panic!("Expected Query result"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary) FROM employees ORDER BY dept, salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // eng partition: last salary = 120 (Bob)
            assert_eq!(rows[0].values[1], Datum::Int64(120));
            assert_eq!(rows[2].values[1], Datum::Int64(120));
            // sales partition: last = 110
            assert_eq!(rows[4].values[1], Datum::Int64(110));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_sum_over() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT dept, SUM(salary) OVER (PARTITION BY dept) FROM employees ORDER BY dept, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // eng: 100+120+100 = 320
            assert_eq!(rows[0].values[1], Datum::Int64(320));
            assert_eq!(rows[1].values[1], Datum::Int64(320));
            assert_eq!(rows[2].values[1], Datum::Int64(320));
            // hr: 80
            assert_eq!(rows[3].values[1], Datum::Int64(80));
            // sales: 90+110 = 200
            assert_eq!(rows[4].values[1], Datum::Int64(200));
            assert_eq!(rows[5].values[1], Datum::Int64(200));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_count_over() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT dept, COUNT(*) OVER (PARTITION BY dept) FROM employees ORDER BY dept, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // eng: 3
            assert_eq!(rows[0].values[1], Datum::Int64(3));
            // hr: 1
            assert_eq!(rows[3].values[1], Datum::Int64(1));
            // sales: 2
            assert_eq!(rows[4].values[1], Datum::Int64(2));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_percent_rank() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, PERCENT_RANK() OVER (ORDER BY salary) FROM employees ORDER BY salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // Frank(80)鈫?.0, Dave(90)鈫?.2, Alice(100)鈫?.4, Carol(100)鈫?.4, Eve(110)鈫?.8, Bob(120)鈫?.0
            let prs: Vec<f64> = rows.iter().map(|r| match &r.values[1] { Datum::Float64(v) => *v, _ => panic!() }).collect();
            assert!((prs[0] - 0.0).abs() < 0.001);
            assert!((prs[1] - 0.2).abs() < 0.001);
            assert!((prs[2] - 0.4).abs() < 0.001);
            assert!((prs[3] - 0.4).abs() < 0.001);
            assert!((prs[4] - 0.8).abs() < 0.001);
            assert!((prs[5] - 1.0).abs() < 0.001);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_cume_dist() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, CUME_DIST() OVER (ORDER BY salary) FROM employees ORDER BY salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // Frank(80)鈫?/6, Dave(90)鈫?/6, Alice(100)鈫?/6, Carol(100)鈫?/6, Eve(110)鈫?/6, Bob(120)鈫?/6
            let cds: Vec<f64> = rows.iter().map(|r| match &r.values[1] { Datum::Float64(v) => *v, _ => panic!() }).collect();
            assert!((cds[0] - 1.0/6.0).abs() < 0.001);
            assert!((cds[1] - 2.0/6.0).abs() < 0.001);
            assert!((cds[2] - 4.0/6.0).abs() < 0.001); // tie at salary=100
            assert!((cds[3] - 4.0/6.0).abs() < 0.001);
            assert!((cds[4] - 5.0/6.0).abs() < 0.001);
            assert!((cds[5] - 1.0).abs() < 0.001);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_nth_value() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, NTH_VALUE(salary, 2) OVER (PARTITION BY dept ORDER BY salary) FROM employees ORDER BY dept, salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // eng partition sorted by salary: Alice(100), Carol(100), Bob(120) 鈫?2nd = 100
            assert_eq!(rows[0].values[1], Datum::Int64(100));
            // hr partition: only Frank(80) 鈫?2nd = NULL
            assert!(matches!(rows[3].values[1], Datum::Null));
            // sales partition: Dave(90), Eve(110) 鈫?2nd = 110
            assert_eq!(rows[4].values[1], Datum::Int64(110));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_window_multiple_windows() {
    let (storage, txn_mgr, executor) = setup_window_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name, ROW_NUMBER() OVER (ORDER BY salary, name), RANK() OVER (ORDER BY salary, name) FROM employees ORDER BY salary, name",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 6);
            // ROW_NUMBER: 1,2,3,4,5,6  (deterministic with salary, name)
            // RANK: 1,2,3,4,5,6  (no ties when ordering by salary, name)
            let rns: Vec<i64> = rows.iter().map(|r| match &r.values[1] { Datum::Int64(v) => *v, _ => panic!() }).collect();
            let ranks: Vec<i64> = rows.iter().map(|r| match &r.values[2] { Datum::Int64(v) => *v, _ => panic!() }).collect();
            assert_eq!(rns, vec![1, 2, 3, 4, 5, 6]);
            assert_eq!(ranks, vec![1, 2, 3, 4, 5, 6]);
        }
        _ => panic!("Expected Query result"),
    }
}

// ========== DATE TYPE TESTS ==========

fn setup_date_test_data() -> (
    std::sync::Arc<cedar_storage::engine::StorageEngine>,
    std::sync::Arc<cedar_txn::TxnManager>,
    std::sync::Arc<cedar_executor::Executor>,
) {
    let storage = std::sync::Arc::new(cedar_storage::engine::StorageEngine::new_in_memory());
    let txn_mgr = std::sync::Arc::new(cedar_txn::TxnManager::new(storage.clone()));
    let executor = std::sync::Arc::new(cedar_executor::Executor::new(storage.clone(), txn_mgr.clone()));
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE events (id INT PRIMARY KEY, name TEXT, event_date DATE)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO events VALUES (1, 'launch', '2024-01-15')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO events VALUES (2, 'release', '2024-06-30')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO events VALUES (3, 'conference', '2023-11-20')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO events VALUES (4, 'holiday', '2024-12-25')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();
    (storage, txn_mgr, executor)
}

#[test]
fn test_date_create_table_and_insert() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, name, event_date FROM events ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, columns } => {
            assert_eq!(columns.len(), 3);
            assert_eq!(rows.len(), 4);
            // Verify date values render as YYYY-MM-DD
            assert_eq!(rows[0].values[2].to_pg_text().unwrap(), "2024-01-15");
            assert_eq!(rows[1].values[2].to_pg_text().unwrap(), "2024-06-30");
            assert_eq!(rows[2].values[2].to_pg_text().unwrap(), "2023-11-20");
            assert_eq!(rows[3].values[2].to_pg_text().unwrap(), "2024-12-25");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_date_order_by() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM events ORDER BY event_date", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            let names: Vec<&str> = rows.iter().map(|r| match &r.values[0] {
                Datum::Text(s) => s.as_str(), _ => panic!()
            }).collect();
            assert_eq!(names, vec!["conference", "launch", "release", "holiday"]);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_date_where_filter() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM events WHERE event_date = '2024-06-30'", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("release".to_string()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_date_cast_text_to_date() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CAST('2025-03-15' AS DATE)", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert!(matches!(rows[0].values[0], Datum::Date(_)));
            assert_eq!(rows[0].values[0].to_pg_text().unwrap(), "2025-03-15");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_date_cast_to_timestamp() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    // Cast date to timestamp 鈥?should be midnight
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CAST(event_date AS TIMESTAMP) FROM events WHERE id = 1", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert!(matches!(rows[0].values[0], Datum::Timestamp(_)));
            assert_eq!(rows[0].values[0].to_pg_text().unwrap(), "2024-01-15 00:00:00");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_date_make_date() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT MAKE_DATE(2024, 7, 4)", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert!(matches!(rows[0].values[0], Datum::Date(_)));
            assert_eq!(rows[0].values[0].to_pg_text().unwrap(), "2024-07-04");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_date_to_date() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT TO_DATE('2023-12-25', 'YYYY-MM-DD')", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert!(matches!(rows[0].values[0], Datum::Date(_)));
            assert_eq!(rows[0].values[0].to_pg_text().unwrap(), "2023-12-25");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_date_current_date() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT CURRENT_DATE", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            // Should be a Date value, not text
            assert!(matches!(rows[0].values[0], Datum::Date(_)));
            let text = rows[0].values[0].to_pg_text().unwrap();
            // Basic sanity: format is YYYY-MM-DD
            assert_eq!(text.len(), 10);
            assert_eq!(&text[4..5], "-");
            assert_eq!(&text[7..8], "-");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_date_extract() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    // EXTRACT YEAR from a date column
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT EXTRACT(YEAR FROM event_date) FROM events WHERE id = 1", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2024));
        }
        _ => panic!("Expected Query result"),
    }
    // EXTRACT MONTH
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT EXTRACT(MONTH FROM event_date) FROM events WHERE id = 2", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(6));
        }
        _ => panic!("Expected Query result"),
    }
    // EXTRACT DAY
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT EXTRACT(DAY FROM event_date) FROM events WHERE id = 4", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(25));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_date_date_add_subtract() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    // DATE_ADD on date column
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT DATE_ADD(event_date, 10) FROM events WHERE id = 1", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert!(matches!(rows[0].values[0], Datum::Date(_)));
            assert_eq!(rows[0].values[0].to_pg_text().unwrap(), "2024-01-25");
        }
        _ => panic!("Expected Query result"),
    }
    // DATE_SUBTRACT on date column
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT DATE_SUBTRACT(event_date, 15) FROM events WHERE id = 1", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert!(matches!(rows[0].values[0], Datum::Date(_)));
            assert_eq!(rows[0].values[0].to_pg_text().unwrap(), "2023-12-31");
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_date_pg_typeof() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT PG_TYPEOF(event_date) FROM events WHERE id = 1", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("date".to_string()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_date_comparison() {
    let (storage, txn_mgr, executor) = setup_date_test_data();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    // Dates after 2024-03-01
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM events WHERE event_date > '2024-03-01' ORDER BY event_date",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            let names: Vec<&str> = rows.iter().map(|r| match &r.values[0] {
                Datum::Text(s) => s.as_str(), _ => panic!()
            }).collect();
            assert_eq!(names, vec!["release", "holiday"]);
        }
        _ => panic!("Expected Query result"),
    }
}

// ========== COPY Tests ==========

fn setup_copy_test() -> (Arc<StorageEngine>, Arc<TxnManager>, Arc<Executor>) {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE copy_test (id INT PRIMARY KEY, name TEXT, score FLOAT, active BOOLEAN)",
        None).unwrap();

    (storage, txn_mgr, executor)
}

#[test]
fn test_copy_from_stdin_text_format() {
    let (storage, txn_mgr, executor) = setup_copy_test();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    let schema = storage.get_table_schema("copy_test").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let data = b"1\tAlice\t95.5\tt\n2\tBob\t87.3\tf\n3\tCharlie\t92.1\tt\n";

    let result = executor.exec_copy_from_data(
        table_id, &schema, &columns, data,
        false, '\t', false, "\\N", '"', '"', &txn,
    ).unwrap();

    match result {
        ExecutionResult::Dml { rows_affected, tag } => {
            assert_eq!(rows_affected, 3);
            assert_eq!(tag, "COPY");
        }
        _ => panic!("Expected Dml result from COPY FROM"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, name, score, active FROM copy_test ORDER BY id",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Text("Alice".into()));
            assert_eq!(rows[1].values[0], Datum::Int32(2));
            assert_eq!(rows[1].values[1], Datum::Text("Bob".into()));
            assert_eq!(rows[2].values[0], Datum::Int32(3));
            assert_eq!(rows[2].values[1], Datum::Text("Charlie".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_copy_from_stdin_csv_format() {
    let (storage, txn_mgr, executor) = setup_copy_test();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    let schema = storage.get_table_schema("copy_test").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let data = b"id,name,score,active\n1,Alice,95.5,true\n2,Bob,87.3,false\n";

    let result = executor.exec_copy_from_data(
        table_id, &schema, &columns, data,
        true, ',', true, "\\N", '"', '"', &txn,
    ).unwrap();

    match result {
        ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 2);
        }
        _ => panic!("Expected Dml result"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM copy_test ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Text("Alice".into()));
            assert_eq!(rows[1].values[0], Datum::Text("Bob".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_copy_from_stdin_csv_with_quoted_fields() {
    let (storage, txn_mgr, executor) = setup_copy_test();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    let schema = storage.get_table_schema("copy_test").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let data = b"1,\"Smith, Alice\",95.5,t\n2,\"O\"\"Brien\",87.3,f\n";

    let result = executor.exec_copy_from_data(
        table_id, &schema, &columns, data,
        true, ',', false, "\\N", '"', '"', &txn,
    ).unwrap();

    match result {
        ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 2);
        }
        _ => panic!("Expected Dml result"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT name FROM copy_test ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("Smith, Alice".into()));
            assert_eq!(rows[1].values[0], Datum::Text("O\"Brien".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_copy_from_stdin_with_null_values() {
    let (storage, txn_mgr, executor) = setup_copy_test();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    let schema = storage.get_table_schema("copy_test").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let data = b"1\tAlice\t\\N\tt\n";

    let result = executor.exec_copy_from_data(
        table_id, &schema, &columns, data,
        false, '\t', false, "\\N", '"', '"', &txn,
    ).unwrap();

    match result {
        ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 1);
        }
        _ => panic!("Expected Dml result"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT score FROM copy_test WHERE id = 1", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert!(rows[0].values[0].is_null(), "Expected Null but got {:?}", rows[0].values[0]);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_copy_from_stdin_column_count_mismatch() {
    let (storage, txn_mgr, executor) = setup_copy_test();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    let schema = storage.get_table_schema("copy_test").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let data = b"1\tAlice\t95.5\n";

    let result = executor.exec_copy_from_data(
        table_id, &schema, &columns, data,
        false, '\t', false, "\\N", '"', '"', &txn,
    );

    assert!(result.is_err(), "Should fail with column count mismatch");
}

#[test]
fn test_copy_to_stdout_text_format() {
    let (storage, txn_mgr, executor) = setup_copy_test();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO copy_test VALUES (1, 'Alice', 95.5, true)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO copy_test VALUES (2, 'Bob', 87.3, false)", Some(&txn)).unwrap();

    let schema = storage.get_table_schema("copy_test").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let result = executor.exec_copy_to(
        table_id, &schema, &columns,
        false, '\t', false, "\\N", '"', '"', &txn,
    ).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            let lines: Vec<String> = rows.iter().map(|r| match &r.values[0] {
                Datum::Text(s) => s.clone(),
                _ => panic!("Expected text"),
            }).collect();
            assert!(lines.iter().all(|l| l.contains('\t')), "Should be tab-delimited");
            assert!(lines.iter().any(|l| l.contains("Alice")), "Should contain Alice in some row");
            assert!(lines.iter().any(|l| l.contains("Bob")), "Should contain Bob in some row");
        }
        _ => panic!("Expected Query result from COPY TO"),
    }
}

#[test]
fn test_copy_to_stdout_csv_format() {
    let (storage, txn_mgr, executor) = setup_copy_test();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO copy_test VALUES (1, 'Alice', 95.5, true)", Some(&txn)).unwrap();

    let schema = storage.get_table_schema("copy_test").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let result = executor.exec_copy_to(
        table_id, &schema, &columns,
        true, ',', false, "\\N", '"', '"', &txn,
    ).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            let line = match &rows[0].values[0] {
                Datum::Text(s) => s.clone(),
                _ => panic!("Expected text"),
            };
            assert!(line.contains(','), "Should be comma-delimited: {}", line);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_copy_to_stdout_with_header() {
    let (storage, txn_mgr, executor) = setup_copy_test();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO copy_test VALUES (1, 'Alice', 95.5, true)", Some(&txn)).unwrap();

    let schema = storage.get_table_schema("copy_test").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let result = executor.exec_copy_to(
        table_id, &schema, &columns,
        true, ',', true, "\\N", '"', '"', &txn,
    ).unwrap();

    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            let header = match &rows[0].values[0] {
                Datum::Text(s) => s.clone(),
                _ => panic!("Expected text"),
            };
            assert!(header.contains("id"), "Header should contain column names: {}", header);
            assert!(header.contains("name"), "Header should contain column names: {}", header);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_copy_roundtrip() {
    let (storage, txn_mgr, executor) = setup_copy_test();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO copy_test VALUES (1, 'Alice', 95.5, true)", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO copy_test VALUES (2, 'Bob', 87.3, false)", Some(&txn)).unwrap();

    let schema = storage.get_table_schema("copy_test").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let export_result = executor.exec_copy_to(
        table_id, &schema, &columns,
        false, '\t', false, "\\N", '"', '"', &txn,
    ).unwrap();

    let exported_data = match export_result {
        ExecutionResult::Query { rows, .. } => {
            rows.iter().map(|r| match &r.values[0] {
                Datum::Text(s) => s.clone(),
                _ => panic!("Expected text"),
            }).collect::<Vec<_>>().join("")
        }
        _ => panic!("Expected Query result"),
    };

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE copy_test2 (id INT PRIMARY KEY, name TEXT, score FLOAT, active BOOLEAN)",
        None).unwrap();

    let schema2 = storage.get_table_schema("copy_test2").unwrap();
    let table_id2 = schema2.id;
    let columns2: Vec<usize> = (0..schema2.columns.len()).collect();

    let import_result = executor.exec_copy_from_data(
        table_id2, &schema2, &columns2, exported_data.as_bytes(),
        false, '\t', false, "\\N", '"', '"', &txn,
    ).unwrap();

    match import_result {
        ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 2);
        }
        _ => panic!("Expected Dml result"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, name FROM copy_test2 ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Text("Alice".into()));
            assert_eq!(rows[1].values[0], Datum::Int32(2));
            assert_eq!(rows[1].values[1], Datum::Text("Bob".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_copy_from_with_date_column() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE copy_dates (id INT PRIMARY KEY, event_date DATE, description TEXT)",
        None).unwrap();

    let schema = storage.get_table_schema("copy_dates").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let data = b"1\t2024-01-15\tNew Year Event\n2\t2024-06-30\tMid Year\n";

    let result = executor.exec_copy_from_data(
        table_id, &schema, &columns, data,
        false, '\t', false, "\\N", '"', '"', &txn,
    ).unwrap();

    match result {
        ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 2);
        }
        _ => panic!("Expected Dml result"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT event_date FROM copy_dates WHERE id = 1", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            match &rows[0].values[0] {
                Datum::Date(days) => {
                    assert!(*days > 0, "Date should be positive days since epoch");
                }
                other => panic!("Expected Date, got {:?}", other),
            }
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_copy_from_with_timestamp_column() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE copy_ts (id INT PRIMARY KEY, created_at TIMESTAMP)",
        None).unwrap();

    let schema = storage.get_table_schema("copy_ts").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let data = b"1\t2024-01-15 10:30:00\n2\t2024-06-30 23:59:59\n";

    let result = executor.exec_copy_from_data(
        table_id, &schema, &columns, data,
        false, '\t', false, "\\N", '"', '"', &txn,
    ).unwrap();

    match result {
        ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 2);
        }
        _ => panic!("Expected Dml result"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT created_at FROM copy_ts WHERE id = 1", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            match &rows[0].values[0] {
                Datum::Timestamp(us) => {
                    assert!(*us > 0, "Timestamp should be positive microseconds");
                }
                other => panic!("Expected Timestamp, got {:?}", other),
            }
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_copy_binder_parses_copy_statement() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE binder_copy_test (id INT PRIMARY KEY, name TEXT)",
        None).unwrap();

    let stmts = parse_sql("COPY binder_copy_test FROM STDIN;").unwrap();
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(&stmts[0]).unwrap();
    match bound {
        cedar_sql_frontend::types::BoundStatement::CopyFrom { columns, csv, delimiter, .. } => {
            assert_eq!(columns, vec![0, 1]);
            assert!(!csv);
            assert_eq!(delimiter, '\t');
        }
        other => panic!("Expected CopyFrom, got {:?}", other),
    }

    let stmts = parse_sql("COPY binder_copy_test TO STDOUT WITH (FORMAT csv, HEADER true);").unwrap();
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(&stmts[0]).unwrap();
    match bound {
        cedar_sql_frontend::types::BoundStatement::CopyTo { csv, delimiter, header, .. } => {
            assert!(csv);
            assert_eq!(delimiter, ',');
            assert!(header);
        }
        other => panic!("Expected CopyTo, got {:?}", other),
    }
}

#[test]
fn test_copy_binder_rejects_file_target() {
    let (storage, txn_mgr, executor) = setup();

    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE file_test (id INT PRIMARY KEY)",
        None).unwrap();

    let stmts = parse_sql("COPY file_test FROM '/tmp/data.csv';").unwrap();
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(catalog);
    let result = binder.bind(&stmts[0]);
    assert!(result.is_err(), "COPY FROM FILE should be unsupported");
}

#[test]
fn test_copy_from_specific_columns() {
    let (storage, txn_mgr, executor) = setup_copy_test();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    let schema = storage.get_table_schema("copy_test").unwrap();
    let table_id = schema.id;
    let columns = vec![0, 1];

    let data = b"1\tAlice\n2\tBob\n";

    let result = executor.exec_copy_from_data(
        table_id, &schema, &columns, data,
        false, '\t', false, "\\N", '"', '"', &txn,
    ).unwrap();

    match result {
        ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 2);
        }
        _ => panic!("Expected Dml result"),
    }

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, name, score FROM copy_test ORDER BY id", Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Text("Alice".into()));
            assert!(rows[0].values[2].is_null(), "Expected Null but got {:?}", rows[0].values[2]);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_copy_from_empty_data() {
    let (storage, txn_mgr, executor) = setup_copy_test();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    let schema = storage.get_table_schema("copy_test").unwrap();
    let table_id = schema.id;
    let columns: Vec<usize> = (0..schema.columns.len()).collect();

    let data = b"";

    let result = executor.exec_copy_from_data(
        table_id, &schema, &columns, data,
        false, '\t', false, "\\N", '"', '"', &txn,
    ).unwrap();

    match result {
        ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 0);
        }
        _ => panic!("Expected Dml result"),
    }
}

// ===========================================================================
// RETURNING clause tests
// ===========================================================================

#[test]
fn test_insert_returning_star() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE ret_test (id INT PRIMARY KEY, name TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO ret_test VALUES (1, 'alice') RETURNING *", Some(&txn)).unwrap();

    match result {
        ExecutionResult::Query { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].0, "id");
            assert_eq!(columns[1].0, "name");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Text("alice".into()));
        }
        _ => panic!("Expected Query result from INSERT RETURNING"),
    }
    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_insert_returning_specific_columns() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE ret_test2 (id INT PRIMARY KEY, name TEXT, age INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO ret_test2 VALUES (1, 'bob', 30) RETURNING id, name", Some(&txn)).unwrap();

    match result {
        ExecutionResult::Query { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].0, "id");
            assert_eq!(columns[1].0, "name");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Text("bob".into()));
        }
        _ => panic!("Expected Query result from INSERT RETURNING"),
    }
    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_insert_returning_multi_row() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE ret_test3 (id INT PRIMARY KEY, val INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO ret_test3 VALUES (1, 10), (2, 20), (3, 30) RETURNING id", Some(&txn)).unwrap();

    match result {
        ExecutionResult::Query { columns, rows } => {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].0, "id");
            assert_eq!(rows.len(), 3);
        }
        _ => panic!("Expected Query result from INSERT RETURNING"),
    }
    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_update_returning() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE ret_upd (id INT PRIMARY KEY, score INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO ret_upd VALUES (1, 100), (2, 200)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "UPDATE ret_upd SET score = score + 10 WHERE id = 1 RETURNING id, score", Some(&txn2)).unwrap();

    match result {
        ExecutionResult::Query { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int32(1));
            assert_eq!(rows[0].values[1], Datum::Int32(110));
        }
        _ => panic!("Expected Query result from UPDATE RETURNING"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_delete_returning() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE ret_del (id INT PRIMARY KEY, name TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO ret_del VALUES (1, 'x'), (2, 'y'), (3, 'z')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "DELETE FROM ret_del WHERE id >= 2 RETURNING *", Some(&txn2)).unwrap();

    match result {
        ExecutionResult::Query { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(rows.len(), 2, "Should return 2 deleted rows");
        }
        _ => panic!("Expected Query result from DELETE RETURNING"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_insert_without_returning_gives_dml_count() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE ret_nodml (id INT PRIMARY KEY)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO ret_nodml VALUES (1), (2)", Some(&txn)).unwrap();

    match result {
        ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 2);
        }
        _ => panic!("Expected DML result without RETURNING"),
    }
    txn_mgr.commit(txn.txn_id).unwrap();
}

// 鈹€鈹€ = ANY(array) / = ALL(array) integration tests 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

#[test]
fn test_any_eq_array_literal() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE any_test (id INT PRIMARY KEY, val INT)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO any_test VALUES (1, 10), (2, 20), (3, 30)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id FROM any_test WHERE val = ANY(ARRAY[10, 30]) ORDER BY id",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get(0), Some(&Datum::Int32(1)));
            assert_eq!(rows[1].get(0), Some(&Datum::Int32(3)));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_all_gt_array_literal() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE all_test (id INT PRIMARY KEY, val INT)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO all_test VALUES (1, 5), (2, 50), (3, 100)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // val > ALL(ARRAY[1,2,3]) means val > 3 for all elements 鈫?val > 3
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id FROM all_test WHERE val > ALL(ARRAY[1, 2, 3]) ORDER BY id",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3); // 5>3, 50>3, 100>3 all true
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_any_neq() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE any_neq (id INT PRIMARY KEY, val INT)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO any_neq VALUES (1, 1), (2, 2)", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // val <> ANY(ARRAY[1]) 鈫?val != 1 for any element
    // row 1: 1 <> 1 = false 鈫?no match; row 2: 2 <> 1 = true 鈫?match
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id FROM any_neq WHERE val <> ANY(ARRAY[1])",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&Datum::Int32(2)));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn.txn_id).unwrap();
}

// 鈹€鈹€ Array slice integration tests 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

#[test]
fn test_array_slice_select() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT (ARRAY[10,20,30,40,50])[2:4]",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0),
                Some(&Datum::Array(vec![Datum::Int32(20), Datum::Int32(30), Datum::Int32(40)])));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_array_slice_open_bounds() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    // [:2] should give first two elements
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT (ARRAY[10,20,30])[:2]",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0),
                Some(&Datum::Array(vec![Datum::Int32(10), Datum::Int32(20)])));
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn.txn_id).unwrap();
}
