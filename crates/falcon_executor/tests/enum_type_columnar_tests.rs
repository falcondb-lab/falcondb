use std::sync::Arc;

#[cfg(feature = "columnstore")]
use falcon_common::datum::Datum;
use falcon_common::types::IsolationLevel;
use falcon_executor::{ExecutionResult, Executor};
use falcon_sql_frontend::binder::Binder;
use falcon_storage::engine::StorageEngine;
use falcon_txn::manager::TxnManager;

fn setup() -> (Executor, Arc<StorageEngine>, Arc<TxnManager>) {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    let executor = Executor::new(storage.clone(), txn_mgr.clone());
    (executor, storage, txn_mgr)
}

fn run_str(executor: &Executor, storage: &StorageEngine, sql: &str) -> Result<ExecutionResult, String> {
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(catalog);
    let bound = binder
        .bind_sql_str(sql)
        .unwrap_or_else(|| {
            let dialect = sqlparser::dialect::PostgreSqlDialect {};
            let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql)
                .map_err(|e| falcon_common::error::SqlError::Parse(e.to_string()));
            match stmts {
                Ok(mut v) => {
                    let stmt = v.remove(0);
                    binder.bind(&stmt).map_err(|e| falcon_common::error::SqlError::from(e))
                }
                Err(e) => Err(e),
            }
        })
        .map_err(|e| format!("bind: {e}"))?;
    let plan = falcon_planner::Planner::plan(&bound).map_err(|e| format!("plan: {e}"))?;
    executor.execute(&plan, None).map_err(|e| format!("exec: {e}"))
}

fn run_dml(executor: &Executor, storage: &StorageEngine, txn_mgr: &TxnManager, sql: &str) -> Result<ExecutionResult, String> {
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(catalog);
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).map_err(|e| format!("parse: {e}"))?;
    let stmt = stmts.into_iter().next().ok_or("empty SQL")?;
    let bound = binder.bind(&stmt).map_err(|e| format!("bind: {e}"))?;
    let plan = falcon_planner::Planner::plan(&bound).map_err(|e| format!("plan: {e}"))?;
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = executor.execute(&plan, Some(&txn)).map_err(|e| format!("exec: {e}"))?;
    txn_mgr.commit(txn.txn_id).map_err(|e| format!("commit: {e:?}"))?;
    Ok(result)
}

// ── CREATE TYPE ENUM tests ─────────────────────────────────────────────────

#[test]
fn test_create_type_enum_basic() {
    let (executor, storage, _) = setup();

    let result = run_str(
        &executor,
        &storage,
        "CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')",
    )
    .unwrap();
    assert!(matches!(result, ExecutionResult::Ddl { .. }));

    let catalog = storage.get_catalog();
    let def = catalog.find_enum_type("mood").expect("enum type not found");
    assert_eq!(def.name, "mood");
    assert_eq!(def.labels, vec!["happy", "sad", "neutral"]);
}

#[test]
fn test_create_type_enum_case_insensitive_lookup() {
    let (executor, storage, _) = setup();

    run_str(&executor, &storage, "CREATE TYPE Status AS ENUM ('active', 'inactive')").unwrap();

    let catalog = storage.get_catalog();
    assert!(catalog.find_enum_type("status").is_some());
    assert!(catalog.find_enum_type("STATUS").is_some());
    assert!(catalog.find_enum_type("Status").is_some());
}

#[test]
fn test_create_type_enum_duplicate_fails() {
    let (executor, storage, _) = setup();

    run_str(&executor, &storage, "CREATE TYPE color AS ENUM ('red', 'green', 'blue')").unwrap();
    let err = run_str(&executor, &storage, "CREATE TYPE color AS ENUM ('red', 'green', 'blue')");
    assert!(err.is_err(), "duplicate CREATE TYPE should fail");
}

#[test]
fn test_drop_type_basic() {
    let (executor, storage, _) = setup();

    run_str(&executor, &storage, "CREATE TYPE direction AS ENUM ('north', 'south', 'east', 'west')").unwrap();
    run_str(&executor, &storage, "DROP TYPE direction").unwrap();

    let catalog = storage.get_catalog();
    assert!(catalog.find_enum_type("direction").is_none());
}

#[test]
fn test_drop_type_if_exists() {
    let (executor, storage, _) = setup();

    // Should not error even though type doesn't exist
    run_str(&executor, &storage, "DROP TYPE IF EXISTS nonexistent_type").unwrap();
}

#[test]
fn test_drop_type_nonexistent_fails() {
    let (executor, storage, _) = setup();
    let err = run_str(&executor, &storage, "DROP TYPE nonexistent_type");
    assert!(err.is_err(), "DROP TYPE on nonexistent type should fail");
}

// ── ColumnStore columnar SELECT tests ─────────────────────────────────────

#[cfg(feature = "columnstore")]
use falcon_common::config::NodeRole;
#[cfg(feature = "columnstore")]
use falcon_common::datum::OwnedRow;
#[cfg(feature = "columnstore")]
use falcon_common::schema::{ColumnDef, StorageType, TableSchema};
#[cfg(feature = "columnstore")]
use falcon_common::types::{ColumnId, TableId, Timestamp, TxnId, TxnType};

#[cfg(feature = "columnstore")]
fn make_cs_schema(name: &str, table_id: u64) -> TableSchema {
    TableSchema {
        id: TableId(table_id),
        name: name.to_string(),
        columns: vec![
            ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: falcon_common::types::DataType::Int64,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
                max_length: None,
            },
            ColumnDef {
                id: ColumnId(1),
                name: "val".into(),
                data_type: falcon_common::types::DataType::Int64,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
                max_length: None,
            },
        ],
        primary_key_columns: vec![0],
        storage_type: StorageType::Columnstore,
        ..Default::default()
    }
}

#[cfg(feature = "columnstore")]
fn setup_analytics() -> (Executor, Arc<StorageEngine>, Arc<TxnManager>) {
    let mut engine = StorageEngine::new_in_memory();
    engine.set_node_role(NodeRole::Analytics);
    let storage = Arc::new(engine);
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    let executor = Executor::new(storage.clone(), txn_mgr.clone());
    (executor, storage, txn_mgr)
}

#[test]
#[cfg(feature = "columnstore")]
fn test_columnstore_select_basic() {
    let (executor, storage, txn_mgr) = setup_analytics();

    let schema = make_cs_schema("cs_test", 100);
    storage.create_table(schema).unwrap();

    for i in 1i64..=5 {
        let row = OwnedRow::new(vec![Datum::Int64(i), Datum::Int64(i * 10)]);
        let txn_id = TxnId(i as u64 + 100);
        storage.insert(TableId(100), row, txn_id).unwrap();
        storage.commit_txn(txn_id, Timestamp(i as u64 + 100), TxnType::Local).unwrap();
    }

    let result = run_dml(&executor, &storage, &txn_mgr, "SELECT id, val FROM cs_test").unwrap();
    if let ExecutionResult::Query { rows, .. } = result {
        assert_eq!(rows.len(), 5, "expected 5 rows, got {}", rows.len());
        let mut ids: Vec<i64> = rows
            .iter()
            .filter_map(|r| match r.get(0) {
                Some(Datum::Int64(v)) => Some(*v),
                _ => None,
            })
            .collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    } else {
        panic!("expected Query result");
    }
}

#[test]
#[cfg(feature = "columnstore")]
fn test_columnstore_select_with_filter() {
    let (executor, storage, txn_mgr) = setup_analytics();

    let schema = make_cs_schema("cs_filter", 101);
    storage.create_table(schema).unwrap();

    for i in 1i64..=10 {
        let row = OwnedRow::new(vec![Datum::Int64(i), Datum::Int64(i * 5)]);
        let txn_id = TxnId(i as u64 + 200);
        storage.insert(TableId(101), row, txn_id).unwrap();
        storage.commit_txn(txn_id, Timestamp(i as u64 + 200), TxnType::Local).unwrap();
    }

    let result = run_dml(
        &executor,
        &storage,
        &txn_mgr,
        "SELECT id, val FROM cs_filter WHERE val > 30",
    )
    .unwrap();
    if let ExecutionResult::Query { rows, .. } = result {
        // val > 30 means i*5 > 30 → i > 6 → rows 7,8,9,10
        assert_eq!(rows.len(), 4, "expected 4 rows with val > 30, got {}", rows.len());
        for row in &rows {
            let val = match row.get(1) {
                Some(Datum::Int64(v)) => *v,
                other => panic!("unexpected datum: {other:?}"),
            };
            assert!(val > 30, "val {val} should be > 30");
        }
    } else {
        panic!("expected Query result");
    }
}
