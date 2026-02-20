#![allow(dead_code, unused_imports)]

pub use std::sync::Arc;

pub use cedar_common::datum::Datum;
pub use cedar_common::types::IsolationLevel;
pub use cedar_executor::{Executor, ExecutionResult};
pub use cedar_planner::Planner;
pub use cedar_sql_frontend::binder::Binder;
pub use cedar_sql_frontend::parser::parse_sql;
pub use cedar_storage::engine::StorageEngine;
pub use cedar_txn::TxnManager;

pub fn setup() -> (Arc<StorageEngine>, Arc<TxnManager>, Arc<Executor>) {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
    (storage, txn_mgr, executor)
}

pub fn run_sql(
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
    let plan = Planner::plan(&bound).map_err(cedar_common::error::CedarError::Sql)?;
    executor.execute(&plan, txn)
}
