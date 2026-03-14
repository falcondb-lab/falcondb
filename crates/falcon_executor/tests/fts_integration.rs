use std::sync::Arc;

use falcon_common::datum::Datum;
use falcon_common::types::IsolationLevel;
use falcon_executor::{ExecutionResult, Executor};
use falcon_storage::engine::StorageEngine;
use falcon_txn::manager::{TxnHandle, TxnManager};

fn setup() -> (Executor, Arc<StorageEngine>, Arc<TxnManager>) {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    let executor = Executor::new(storage.clone(), txn_mgr.clone());
    (executor, storage, txn_mgr)
}

fn run_sql_txn(
    executor: &Executor,
    storage: &StorageEngine,
    txn: Option<&TxnHandle>,
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
        .execute(&plan, txn)
        .map_err(|e| format!("exec: {e}"))
}

fn run_ddl(executor: &Executor, storage: &StorageEngine, sql: &str) {
    run_sql_txn(executor, storage, None, sql)
        .unwrap_or_else(|e| panic!("DDL failed: {e}\n  sql: {sql}"));
}

fn query_rows(
    executor: &Executor,
    storage: &StorageEngine,
    txn: &TxnHandle,
    sql: &str,
) -> Vec<Vec<Datum>> {
    match run_sql_txn(executor, storage, Some(txn), sql)
        .unwrap_or_else(|e| panic!("SQL failed: {e}\n  query: {sql}"))
    {
        ExecutionResult::Query { rows, .. } => rows.into_iter().map(|r| r.values.into_vec()).collect(),
        other => panic!("expected Query, got {:?}", other),
    }
}

fn query_scalar(executor: &Executor, storage: &StorageEngine, txn: &TxnHandle, sql: &str) -> Datum {
    let rows = query_rows(executor, storage, txn, sql);
    rows.into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
        .unwrap_or(Datum::Null)
}

// ── to_tsvector / to_tsquery ──

#[test]
fn test_to_tsvector_returns_tsvector() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT to_tsvector('the quick brown fox')",
    );
    assert!(matches!(d, Datum::TsVector(_)));
    if let Datum::TsVector(v) = &d {
        let words: Vec<&str> = v.iter().map(|(w, _)| w.as_str()).collect();
        assert!(words.contains(&"quick"));
        assert!(words.contains(&"brown"));
        assert!(words.contains(&"fox"));
        assert!(!words.contains(&"the")); // stop word
    }
}

#[test]
fn test_to_tsquery_returns_tsquery() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(&exec, &store, &txn, "SELECT to_tsquery('fox & dog')");
    assert!(matches!(d, Datum::TsQuery(_)));
}

#[test]
fn test_plainto_tsquery() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT plainto_tsquery('quick brown fox')",
    );
    assert!(matches!(d, Datum::TsQuery(_)));
}

// ── @@ operator ──

#[test]
fn test_ts_match_inline() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT to_tsvector('the quick brown fox') @@ to_tsquery('fox')",
    );
    assert_eq!(d, Datum::Boolean(true));
}

#[test]
fn test_ts_match_negative() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT to_tsvector('the quick brown fox') @@ to_tsquery('cat')",
    );
    assert_eq!(d, Datum::Boolean(false));
}

#[test]
fn test_ts_match_and() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT to_tsvector('the quick brown fox') @@ to_tsquery('quick & fox')",
    );
    assert_eq!(d, Datum::Boolean(true));
}

#[test]
fn test_ts_match_or() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT to_tsvector('the quick brown fox') @@ to_tsquery('cat | fox')",
    );
    assert_eq!(d, Datum::Boolean(true));
}

#[test]
fn test_ts_match_not() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT to_tsvector('the quick brown fox') @@ to_tsquery('fox & !cat')",
    );
    assert_eq!(d, Datum::Boolean(true));
}

// ── ts_rank ──

#[test]
fn test_ts_rank() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT ts_rank(to_tsvector('the quick brown fox'), to_tsquery('fox'))",
    );
    if let Datum::Float64(v) = d {
        assert!(v > 0.0, "rank should be positive");
    } else {
        panic!("expected Float64, got {:?}", d);
    }
}

// ── ts_headline ──

#[test]
fn test_ts_headline() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT ts_headline('the quick brown fox jumped', to_tsquery('fox'))",
    );
    if let Datum::Text(s) = d {
        assert!(s.contains("<b>fox</b>"), "headline should bold 'fox': {s}");
    } else {
        panic!("expected Text, got {:?}", d);
    }
}

// ── numnode / querytree ──

#[test]
fn test_numnode() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT numnode(to_tsquery('fox & dog'))",
    );
    if let Datum::Int32(n) = d {
        assert!(n >= 3, "fox & dog should have at least 3 nodes");
    } else {
        panic!("expected Int32, got {:?}", d);
    }
}

#[test]
fn test_querytree() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT querytree(to_tsquery('fox & dog'))",
    );
    if let Datum::Text(s) = d {
        assert!(s.contains("fox"), "querytree should contain 'fox': {s}");
        assert!(s.contains("dog"), "querytree should contain 'dog': {s}");
    } else {
        panic!("expected Text, got {:?}", d);
    }
}

// ── strip / tsvector_length ──

#[test]
fn test_strip() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT strip(to_tsvector('hello world'))",
    );
    if let Datum::TsVector(v) = d {
        assert!(
            v.iter().all(|(_, p)| p.is_empty()),
            "strip should remove positions"
        );
    } else {
        panic!("expected TsVector, got {:?}", d);
    }
}

#[test]
fn test_tsvector_length() {
    let (exec, store, txn_mgr) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let d = query_scalar(
        &exec,
        &store,
        &txn,
        "SELECT tsvector_length(to_tsvector('hello world hello'))",
    );
    assert_eq!(d, Datum::Int32(2)); // "hello" and "world"
}

// ── Table with tsvector column ──

#[test]
fn test_create_table_with_tsvector() {
    let (exec, store, txn_mgr) = setup();
    run_ddl(
        &exec,
        &store,
        "CREATE TABLE docs (id INT PRIMARY KEY, title TEXT, body TEXT, tsv TSVECTOR)",
    );

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql_txn(&exec, &store, Some(&txn),
        "INSERT INTO docs VALUES (1, 'PostgreSQL Full Text Search', 'PostgreSQL provides full text search', to_tsvector('PostgreSQL provides full text search'))").unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql_txn(&exec, &store, Some(&txn),
        "INSERT INTO docs VALUES (2, 'SQLite FTS', 'SQLite has FTS5 extension', to_tsvector('SQLite has FTS5 extension'))").unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Query with @@
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let rows = query_rows(
        &exec,
        &store,
        &txn,
        "SELECT id, title FROM docs WHERE tsv @@ to_tsquery('postgresql')",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::Int32(1));

    // Query that matches nothing
    let rows = query_rows(
        &exec,
        &store,
        &txn,
        "SELECT id FROM docs WHERE tsv @@ to_tsquery('mysql')",
    );
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_fts_with_ranking() {
    let (exec, store, txn_mgr) = setup();
    run_ddl(
        &exec,
        &store,
        "CREATE TABLE articles (id INT PRIMARY KEY, content TEXT, tsv TSVECTOR)",
    );

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql_txn(
        &exec,
        &store,
        Some(&txn),
        "INSERT INTO articles VALUES (1, 'fox fox fox', to_tsvector('fox fox fox'))",
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql_txn(
        &exec,
        &store,
        Some(&txn),
        "INSERT INTO articles VALUES (2, 'quick brown fox', to_tsvector('quick brown fox'))",
    )
    .unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Both should match 'fox'
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let rows = query_rows(
        &exec,
        &store,
        &txn,
        "SELECT id FROM articles WHERE tsv @@ to_tsquery('fox')",
    );
    assert_eq!(rows.len(), 2);
}
