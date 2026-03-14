#![allow(dead_code, unused_imports)]

pub use std::sync::Arc;

pub use falcon_common::datum::Datum;
pub use falcon_common::types::IsolationLevel;
pub use falcon_executor::{ExecutionResult, Executor};
pub use falcon_planner::Planner;
pub use falcon_sql_frontend::binder::Binder;
pub use falcon_sql_frontend::parser::parse_sql;
pub use falcon_storage::engine::StorageEngine;
pub use falcon_txn::TxnManager;

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
    txn: Option<&falcon_txn::TxnHandle>,
) -> Result<falcon_executor::ExecutionResult, falcon_common::error::FalconError> {
    let stmts = parse_sql(sql).map_err(falcon_common::error::FalconError::Sql)?;
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(catalog);
    let bound = binder
        .bind(&stmts[0])
        .map_err(falcon_common::error::FalconError::Sql)?;
    let plan = Planner::plan(&bound).map_err(falcon_common::error::FalconError::Sql)?;
    executor.execute(&plan, txn)
}
