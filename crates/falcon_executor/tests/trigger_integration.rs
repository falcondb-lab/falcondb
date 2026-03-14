use std::sync::Arc;

use falcon_common::types::IsolationLevel;
use falcon_executor::{ExecutionResult, Executor};
use falcon_storage::engine::StorageEngine;
use falcon_txn::manager::TxnManager;

fn setup() -> (Executor, Arc<StorageEngine>, Arc<TxnManager>) {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    let executor = Executor::new(storage.clone(), txn_mgr.clone());
    (executor, storage, txn_mgr)
}

fn run_sql(executor: &Executor, storage: &StorageEngine, sql: &str) -> Result<ExecutionResult, String> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).map_err(|e| format!("parse: {e}"))?;
    let stmt = stmts.into_iter().next().ok_or("empty SQL")?;
    let catalog = storage.get_catalog();
    let mut binder = falcon_sql_frontend::binder::Binder::new(catalog);
    let bound = binder.bind(&stmt).map_err(|e| format!("bind: {e}"))?;
    let plan = falcon_planner::Planner::plan(&bound).map_err(|e| format!("plan: {e}"))?;
    executor.execute(&plan, None).map_err(|e| format!("exec: {e}"))
}

fn run_dml(executor: &Executor, storage: &StorageEngine, txn_mgr: &TxnManager, sql: &str) -> Result<ExecutionResult, String> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).map_err(|e| format!("parse: {e}"))?;
    let stmt = stmts.into_iter().next().ok_or("empty SQL")?;
    let catalog = storage.get_catalog();
    let mut binder = falcon_sql_frontend::binder::Binder::new(catalog);
    let bound = binder.bind(&stmt).map_err(|e| format!("bind: {e}"))?;
    let plan = falcon_planner::Planner::plan(&bound).map_err(|e| format!("plan: {e}"))?;
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = executor.execute(&plan, Some(&txn)).map_err(|e| format!("exec: {e}"))?;
    txn_mgr.commit(txn.txn_id).map_err(|e| format!("commit: {e:?}"))?;
    Ok(result)
}

#[test]
fn test_create_and_drop_trigger() {
    let (executor, storage, _) = setup();

    run_sql(&executor, &storage,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    run_sql(&executor, &storage,
        "CREATE FUNCTION trg_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$").unwrap();

    run_sql(&executor, &storage,
        "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION trg_fn()").unwrap();

    let catalog = storage.get_catalog();
    assert!(catalog.find_trigger("t", "trg").is_some());

    run_sql(&executor, &storage, "DROP TRIGGER trg ON t").unwrap();

    let catalog = storage.get_catalog();
    assert!(catalog.find_trigger("t", "trg").is_none());
}

#[test]
fn test_drop_trigger_if_exists() {
    let (executor, storage, _) = setup();

    run_sql(&executor, &storage,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY)").unwrap();

    run_sql(&executor, &storage,
        "DROP TRIGGER IF EXISTS nonexistent ON t2").unwrap();
}

#[test]
fn test_duplicate_trigger_error() {
    let (executor, storage, _) = setup();

    run_sql(&executor, &storage,
        "CREATE TABLE t3 (id INTEGER PRIMARY KEY)").unwrap();
    run_sql(&executor, &storage,
        "CREATE FUNCTION trg_fn3() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$").unwrap();
    run_sql(&executor, &storage,
        "CREATE TRIGGER trg3 AFTER INSERT ON t3 FOR EACH ROW EXECUTE FUNCTION trg_fn3()").unwrap();

    let err = run_sql(&executor, &storage,
        "CREATE TRIGGER trg3 AFTER INSERT ON t3 FOR EACH ROW EXECUTE FUNCTION trg_fn3()");
    assert!(err.is_err(), "duplicate trigger should fail");
    assert!(err.unwrap_err().contains("already exists"));
}

#[test]
fn test_insert_with_trigger_fires() {
    let (executor, storage, txn_mgr) = setup();

    run_sql(&executor, &storage,
        "CREATE TABLE audit (id SERIAL PRIMARY KEY, msg TEXT)").unwrap();
    run_sql(&executor, &storage,
        "CREATE TABLE target (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    run_sql(&executor, &storage,
        "CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit(msg) VALUES('inserted'); RETURN NEW; END $$").unwrap();

    run_sql(&executor, &storage,
        "CREATE TRIGGER audit_trg AFTER INSERT ON target FOR EACH STATEMENT EXECUTE FUNCTION audit_fn()").unwrap();

    run_dml(&executor, &storage, &txn_mgr,
        "INSERT INTO target VALUES (1, 'hello')").unwrap();

    let result = run_dml(&executor, &storage, &txn_mgr, "SELECT count(*) FROM audit").unwrap();
    if let ExecutionResult::Query { rows, .. } = result {
        let count = rows.into_iter().next()
            .and_then(|r| r.values.into_iter().next())
            .unwrap_or(falcon_common::datum::Datum::Null);
        assert!(
            matches!(count, falcon_common::datum::Datum::Int64(1)),
            "expected 1 audit row, got {:?}", count
        );
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn test_row_trigger_new_variable() {
    let (executor, storage, txn_mgr) = setup();

    run_sql(&executor, &storage,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    run_sql(&executor, &storage,
        "CREATE TABLE log (id SERIAL PRIMARY KEY, captured INTEGER)").unwrap();

    // Row-level trigger that reads NEW.val and inserts it into log
    run_sql(&executor, &storage,
        "CREATE FUNCTION row_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN INSERT INTO log(captured) VALUES(new_val); RETURN NEW; END $$").unwrap();

    run_sql(&executor, &storage,
        "CREATE TRIGGER row_trg AFTER INSERT ON items FOR EACH ROW EXECUTE FUNCTION row_fn()").unwrap();

    run_dml(&executor, &storage, &txn_mgr,
        "INSERT INTO items VALUES (1, 42)").unwrap();
    run_dml(&executor, &storage, &txn_mgr,
        "INSERT INTO items VALUES (2, 99)").unwrap();

    // Should have 2 log rows (one per inserted item)
    let result = run_dml(&executor, &storage, &txn_mgr, "SELECT count(*) FROM log").unwrap();
    if let ExecutionResult::Query { rows, .. } = result {
        let count = rows.into_iter().next()
            .and_then(|r| r.values.into_iter().next())
            .unwrap_or(falcon_common::datum::Datum::Null);
        assert!(
            matches!(count, falcon_common::datum::Datum::Int64(2)),
            "expected 2 log rows, got {:?}", count
        );
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn test_row_trigger_before_delete_old_variable() {
    let (executor, storage, txn_mgr) = setup();

    run_sql(&executor, &storage,
        "CREATE TABLE data (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    run_sql(&executor, &storage,
        "CREATE TABLE deleted_log (id SERIAL PRIMARY KEY, deleted_id INTEGER)").unwrap();

    run_sql(&executor, &storage,
        "CREATE FUNCTION del_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN INSERT INTO deleted_log(deleted_id) VALUES(old_id); RETURN OLD; END $$").unwrap();

    run_sql(&executor, &storage,
        "CREATE TRIGGER del_trg BEFORE DELETE ON data FOR EACH ROW EXECUTE FUNCTION del_fn()").unwrap();

    run_dml(&executor, &storage, &txn_mgr,
        "INSERT INTO data VALUES (10, 'alice'), (20, 'bob')").unwrap();
    run_dml(&executor, &storage, &txn_mgr,
        "DELETE FROM data WHERE id = 10").unwrap();

    let result = run_dml(&executor, &storage, &txn_mgr, "SELECT count(*) FROM deleted_log").unwrap();
    if let ExecutionResult::Query { rows, .. } = result {
        let count = rows.into_iter().next()
            .and_then(|r| r.values.into_iter().next())
            .unwrap_or(falcon_common::datum::Datum::Null);
        assert!(
            matches!(count, falcon_common::datum::Datum::Int64(1)),
            "expected 1 deleted_log row, got {:?}", count
        );
    } else {
        panic!("expected Query result");
    }
}
