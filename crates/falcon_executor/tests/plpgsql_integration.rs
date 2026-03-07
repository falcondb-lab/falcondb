use std::sync::Arc;

use falcon_common::datum::Datum;
use falcon_executor::{ExecutionResult, Executor};
use falcon_storage::engine::StorageEngine;
use falcon_txn::manager::TxnManager;

fn setup() -> (Executor, Arc<StorageEngine>, Arc<TxnManager>) {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    let executor = Executor::new(storage.clone(), txn_mgr.clone());
    (executor, storage, txn_mgr)
}

fn run_sql(
    executor: &Executor,
    storage: &StorageEngine,
    sql: &str,
) -> Result<ExecutionResult, String> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts =
        sqlparser::parser::Parser::parse_sql(&dialect, sql).map_err(|e| format!("parse: {e}"))?;
    let stmt = stmts.into_iter().next().ok_or("empty SQL")?;
    let catalog = storage.get_catalog();
    let mut binder = falcon_sql_frontend::binder::Binder::new(catalog);
    let bound = binder.bind(&stmt).map_err(|e| format!("bind: {e}"))?;
    let plan = falcon_planner::Planner::plan(&bound).map_err(|e| format!("plan: {e}"))?;
    executor
        .execute(&plan, None)
        .map_err(|e| format!("exec: {e}"))
}

#[allow(dead_code)]
fn query_first_datum(executor: &Executor, storage: &StorageEngine, sql: &str) -> Datum {
    match run_sql(executor, storage, sql).unwrap() {
        ExecutionResult::Query { rows, .. } => rows
            .into_iter()
            .next()
            .and_then(|r| r.values.into_iter().next())
            .unwrap_or(Datum::Null),
        other => panic!("expected Query, got {:?}", other),
    }
}

#[test]
fn test_create_and_drop_sql_function() {
    let (executor, storage, _txn) = setup();

    let result = run_sql(&executor, &storage,
        "CREATE FUNCTION add_two(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE SQL AS 'SELECT $1 + $2'"
    ).unwrap();
    assert!(matches!(result, ExecutionResult::Ddl { .. }));

    // Verify it's in the catalog
    let catalog = storage.get_catalog();
    assert!(catalog.find_function("add_two").is_some());

    // Drop it
    let result = run_sql(&executor, &storage, "DROP FUNCTION add_two").unwrap();
    assert!(matches!(result, ExecutionResult::Ddl { .. }));

    let catalog = storage.get_catalog();
    assert!(catalog.find_function("add_two").is_none());
}

#[test]
fn test_drop_function_if_exists() {
    let (executor, storage, _txn) = setup();

    // DROP IF EXISTS on nonexistent function should succeed
    let result = run_sql(&executor, &storage, "DROP FUNCTION IF EXISTS nonexistent").unwrap();
    assert!(matches!(result, ExecutionResult::Ddl { .. }));
}

#[test]
fn test_create_or_replace_function() {
    let (executor, storage, _txn) = setup();

    run_sql(
        &executor,
        &storage,
        "CREATE FUNCTION myfunc(x INTEGER) RETURNS INTEGER LANGUAGE SQL AS 'SELECT $1'",
    )
    .unwrap();

    // OR REPLACE should succeed
    run_sql(&executor, &storage,
        "CREATE OR REPLACE FUNCTION myfunc(x INTEGER) RETURNS INTEGER LANGUAGE SQL AS 'SELECT $1 + 1'"
    ).unwrap();

    let catalog = storage.get_catalog();
    let func = catalog.find_function("myfunc").unwrap();
    assert!(func.body.contains("+ 1"));
}

#[test]
fn test_create_plpgsql_function() {
    let (executor, storage, _txn) = setup();

    let result = run_sql(&executor, &storage,
        "CREATE FUNCTION greet(name TEXT) RETURNS TEXT LANGUAGE plpgsql AS 'BEGIN RETURN name; END'"
    ).unwrap();
    assert!(matches!(result, ExecutionResult::Ddl { .. }));

    let catalog = storage.get_catalog();
    let func = catalog.find_function("greet").unwrap();
    assert_eq!(
        func.language,
        falcon_common::schema::FunctionLanguage::PlPgSql
    );
}

#[test]
fn test_create_function_volatility() {
    let (executor, storage, _txn) = setup();

    run_sql(&executor, &storage,
        "CREATE FUNCTION pure_add(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS 'SELECT $1 + $2'"
    ).unwrap();

    let catalog = storage.get_catalog();
    let func = catalog.find_function("pure_add").unwrap();
    assert_eq!(
        func.volatility,
        falcon_common::schema::FunctionVolatility::Immutable
    );
}

#[test]
fn test_create_function_strict() {
    let (executor, storage, _txn) = setup();

    run_sql(&executor, &storage,
        "CREATE FUNCTION strict_fn(a INTEGER) RETURNS INTEGER LANGUAGE SQL RETURNS NULL ON NULL INPUT AS 'SELECT $1'"
    ).unwrap();

    let catalog = storage.get_catalog();
    let func = catalog.find_function("strict_fn").unwrap();
    assert!(func.is_strict);
}
