#![allow(clippy::approx_constant, clippy::useless_vec)]

mod common;
use common::*;

// ── Phase 1: Statistical Aggregates ──

#[test]
fn test_stddev_pop() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE stats(id SERIAL PRIMARY KEY, val INT)",
        None,
    )
    .unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (i, v) in [2, 4, 4, 4, 5, 5, 7, 9].iter().enumerate() {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!("INSERT INTO stats VALUES ({}, {})", i + 1, v),
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
        "SELECT STDDEV_POP(val) FROM stats",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            let v = rows[0].values[0].as_f64().unwrap();
            assert!(
                (v - 2.0).abs() < 0.01,
                "STDDEV_POP should be ~2.0, got {}",
                v
            );
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_stddev_samp() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE stats2(id SERIAL PRIMARY KEY, val INT)",
        None,
    )
    .unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (i, v) in [2, 4, 4, 4, 5, 5, 7, 9].iter().enumerate() {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!("INSERT INTO stats2 VALUES ({}, {})", i + 1, v),
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
        "SELECT STDDEV(val) FROM stats2",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            let v = rows[0].values[0].as_f64().unwrap();
            // STDDEV_SAMP of [2,4,4,4,5,5,7,9] = ~2.138
            assert!(
                v > 2.0 && v < 2.3,
                "STDDEV_SAMP should be ~2.138, got {}",
                v
            );
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_variance() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE var_t(id SERIAL PRIMARY KEY, x INT)",
        None,
    )
    .unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (i, v) in [10, 20, 30].iter().enumerate() {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!("INSERT INTO var_t VALUES ({}, {})", i + 1, v),
            Some(&txn),
        )
        .unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    // VAR_POP([10,20,30]) = ((10-20)^2 + 0 + (30-20)^2) / 3 = 200/3 ≈ 66.67
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT VAR_POP(x) FROM var_t",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            let v = rows[0].values[0].as_f64().unwrap();
            assert!(
                (v - 66.67).abs() < 0.1,
                "VAR_POP should be ~66.67, got {}",
                v
            );
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_mode_aggregate() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE mode_t(id SERIAL PRIMARY KEY, x INT)",
        None,
    )
    .unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (i, v) in [1, 2, 2, 3, 3, 3, 4].iter().enumerate() {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!("INSERT INTO mode_t VALUES ({}, {})", i + 1, v),
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
        "SELECT MODE(x) FROM mode_t",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            let v = rows[0].values[0]
                .as_i64()
                .unwrap_or(rows[0].values[0].as_f64().map(|f| f as i64).unwrap_or(0));
            assert_eq!(v, 3, "MODE should be 3");
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_bit_aggregates() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE bits(id SERIAL PRIMARY KEY, x INT)",
        None,
    )
    .unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (i, v) in [0b1100i32, 0b1010, 0b1110].iter().enumerate() {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!("INSERT INTO bits VALUES ({}, {})", i + 1, v),
            Some(&txn),
        )
        .unwrap();
    }
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    // BIT_AND(0b1100, 0b1010, 0b1110) = 0b1000 = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT BIT_AND(x) FROM bits",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(8));
        }
        _ => panic!("Expected Query"),
    }

    // BIT_OR(0b1100, 0b1010, 0b1110) = 0b1110 = 14
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT BIT_OR(x) FROM bits",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(14));
        }
        _ => panic!("Expected Query"),
    }

    // BIT_XOR(0b1100, 0b1010, 0b1110) = 0b1100 ^ 0b1010 ^ 0b1110 = 0b1000 = 8... wait:
    // 0b1100 ^ 0b1010 = 0b0110; 0b0110 ^ 0b1110 = 0b1000 = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT BIT_XOR(x) FROM bits",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(8));
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_stddev_empty_and_single() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE empty_t(id SERIAL PRIMARY KEY, x INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    // Empty table: STDDEV should be NULL
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STDDEV(x) FROM empty_t",
        Some(&txn),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert!(
                rows[0].values[0].is_null(),
                "STDDEV of empty should be NULL"
            );
        }
        _ => panic!("Expected Query"),
    }

    // Single row: STDDEV_SAMP should be NULL (n < 2)
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO empty_t VALUES (1, 42)",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STDDEV(x) FROM empty_t",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert!(
                rows[0].values[0].is_null(),
                "STDDEV_SAMP of single row should be NULL"
            );
        }
        _ => panic!("Expected Query"),
    }

    // Single row: STDDEV_POP should be 0
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STDDEV_POP(x) FROM empty_t",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            let v = rows[0].values[0].as_f64().unwrap();
            assert!(
                (v - 0.0).abs() < 0.001,
                "STDDEV_POP of single row should be 0, got {}",
                v
            );
        }
        _ => panic!("Expected Query"),
    }
}

// ── Phase 3: Information Schema ──

#[test]
fn test_information_schema_tables() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE alpha(id INT PRIMARY KEY)",
        None,
    )
    .unwrap();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE beta(name TEXT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT table_name, table_type FROM information_schema.tables",
        Some(&txn),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, columns } => {
            assert!(rows.len() >= 2, "Should have at least 2 tables");
            // Check column names
            assert_eq!(columns[0].0, "table_name");
            assert_eq!(columns[1].0, "table_type");
            // Check that our tables are present
            let names: Vec<String> = rows.iter().map(|r| format!("{}", r.values[0])).collect();
            assert!(names.contains(&"alpha".to_string()), "Should contain alpha");
            assert!(names.contains(&"beta".to_string()), "Should contain beta");
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_information_schema_columns() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE col_test(id INT PRIMARY KEY, name TEXT, age INT)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT column_name, ordinal_position, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'col_test'",
        Some(&txn)).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3, "Should have 3 columns");
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_pg_catalog_pg_tables() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE pg_test(id INT PRIMARY KEY)",
        None,
    )
    .unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT tablename, hasindexes FROM pg_catalog.pg_tables",
        Some(&txn),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, columns } => {
            assert!(rows.len() >= 1, "Should have at least 1 table");
            assert_eq!(columns[0].0, "tablename");
            let names: Vec<String> = rows.iter().map(|r| format!("{}", r.values[0])).collect();
            assert!(
                names.contains(&"pg_test".to_string()),
                "Should contain pg_test"
            );
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_pg_catalog_pg_type() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT typname FROM pg_catalog.pg_type",
        Some(&txn),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert!(rows.len() >= 10, "Should have at least 10 types");
            let names: Vec<String> = rows.iter().map(|r| format!("{}", r.values[0])).collect();
            assert!(names.contains(&"int4".to_string()));
            assert!(names.contains(&"text".to_string()));
            assert!(names.contains(&"bool".to_string()));
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_information_schema_schemata() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT schema_name FROM information_schema.schemata",
        Some(&txn),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            let names: Vec<String> = rows.iter().map(|r| format!("{}", r.values[0])).collect();
            assert!(names.contains(&"public".to_string()));
            assert!(names.contains(&"information_schema".to_string()));
            assert!(names.contains(&"pg_catalog".to_string()));
        }
        _ => panic!("Expected Query"),
    }
}

// ── Phase 6: Additional Type Support ──

#[test]
fn test_uuid_type_column() {
    let (storage, txn_mgr, executor) = setup();
    // UUID type should be accepted (mapped to Text)
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE uuid_t(id UUID, name TEXT)",
        None,
    )
    .unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO uuid_t VALUES ('550e8400-e29b-41d4-a716-446655440000', 'test')",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT id FROM uuid_t",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].values[0],
                Datum::Text("550e8400-e29b-41d4-a716-446655440000".into())
            );
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_bytea_type_column() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE bytea_t(data BYTEA)",
        None,
    )
    .unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO bytea_t VALUES ('\\xDEADBEEF')",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT data FROM bytea_t",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_char_type_column() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE char_t(code CHAR(5), label VARCHAR(100))",
        None,
    )
    .unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "INSERT INTO char_t VALUES ('ABC', 'test label')",
        Some(&txn),
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT code, label FROM char_t",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("ABC".into()));
            assert_eq!(rows[0].values[1], Datum::Text("test label".into()));
        }
        _ => panic!("Expected Query"),
    }
}

// ── Statistical aggregates with GROUP BY ──

#[test]
fn test_stddev_with_group_by() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "CREATE TABLE grp_stats(id SERIAL PRIMARY KEY, grp TEXT, val INT)",
        None,
    )
    .unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    for (i, (g, v)) in [("A", 10), ("A", 20), ("A", 30), ("B", 100), ("B", 200)]
        .iter()
        .enumerate()
    {
        run_sql(
            &storage,
            &txn_mgr,
            &executor,
            &format!("INSERT INTO grp_stats VALUES ({}, '{}', {})", i + 1, g, v),
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
        "SELECT grp, STDDEV_POP(val) FROM grp_stats GROUP BY grp ORDER BY grp",
        Some(&txn2),
    )
    .unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            // Group A: stddev_pop([10,20,30]) = sqrt(200/3) ≈ 8.165
            let a_stddev = rows[0].values[1].as_f64().unwrap();
            assert!(
                (a_stddev - 8.165).abs() < 0.01,
                "Group A STDDEV_POP should be ~8.165, got {}",
                a_stddev
            );
        }
        _ => panic!("Expected Query"),
    }
}
