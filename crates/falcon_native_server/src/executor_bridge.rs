//! Bridge between native protocol messages and the FalconDB SQL execution engine.
//!
//! Converts `QueryRequest` → parse → bind → plan → execute → `QueryResponse`.

use std::sync::Arc;

use falcon_common::types::{IsolationLevel, ShardId};
use falcon_executor::{ExecutionResult, Executor};
use falcon_planner::Planner;
use falcon_protocol_native::codec::{datatype_to_type_id, datum_to_encoded};
use falcon_protocol_native::types::*;
use falcon_sql_frontend::binder::Binder;
use falcon_sql_frontend::parser::parse_sql;
use falcon_storage::engine::StorageEngine;
use falcon_txn::manager::{TxnClassification, TxnManager};

use crate::config::NativeServerConfig;

/// Execution bridge that processes native protocol requests.
pub struct ExecutorBridge {
    pub storage: Arc<StorageEngine>,
    pub txn_mgr: Arc<TxnManager>,
    pub executor: Executor,
    pub config: NativeServerConfig,
}

impl ExecutorBridge {
    pub fn new(
        storage: Arc<StorageEngine>,
        txn_mgr: Arc<TxnManager>,
        config: NativeServerConfig,
    ) -> Self {
        let executor = Executor::new(storage.clone(), txn_mgr.clone());
        Self {
            storage,
            txn_mgr,
            executor,
            config,
        }
    }

    /// Execute a QueryRequest and return a QueryResponse or ErrorResponse.
    pub fn execute_query(&self, req: &QueryRequest) -> Result<QueryResponse, ErrorResponse> {
        // Epoch fencing check
        if self.config.epoch_fencing_enabled && req.epoch != 0 && req.epoch != self.config.epoch {
            return Err(ErrorResponse {
                request_id: req.request_id,
                error_code: ERR_FENCED_EPOCH,
                sqlstate: *b"F0001",
                retryable: true,
                server_epoch: self.config.epoch,
                message: format!(
                    "epoch mismatch: client={}, server={}",
                    req.epoch, self.config.epoch
                ),
            });
        }

        // Parse SQL
        let stmts = match parse_sql(&req.sql) {
            Ok(s) => s,
            Err(e) => {
                return Err(self.make_error(
                    req.request_id,
                    ERR_SYNTAX_ERROR,
                    *b"42601",
                    false,
                    &format!("{}", e),
                ));
            }
        };

        if stmts.is_empty() {
            return Ok(QueryResponse {
                request_id: req.request_id,
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Bind
        let catalog = self.storage.get_catalog();
        let mut binder = Binder::new(catalog);
        let bound = match binder.bind(&stmts[0]) {
            Ok(b) => b,
            Err(e) => {
                return Err(self.make_error(
                    req.request_id,
                    ERR_SYNTAX_ERROR,
                    *b"42000",
                    false,
                    &format!("{}", e),
                ));
            }
        };

        // Plan
        let plan = match Planner::plan(&bound) {
            Ok(p) => p,
            Err(e) => {
                return Err(self.make_error(
                    req.request_id,
                    ERR_INTERNAL_ERROR,
                    *b"XX000",
                    false,
                    &format!("{}", e),
                ));
            }
        };

        // Auto-transaction for the query
        let is_autocommit = req.session_flags & SESSION_AUTOCOMMIT != 0;
        let txn = if is_autocommit {
            Some(self.txn_mgr.begin_with_classification(
                IsolationLevel::ReadCommitted,
                TxnClassification::local(ShardId(0)),
            ))
        } else {
            None
        };

        // Execute
        let result = self.executor.execute(&plan, txn.as_ref());

        // Auto-commit
        if let Some(ref t) = txn {
            if result.is_ok() {
                let _ = self.txn_mgr.commit(t.txn_id);
            } else {
                let _ = self.txn_mgr.abort(t.txn_id);
            }
        }

        match result {
            Ok(exec_result) => Ok(self.execution_result_to_response(req.request_id, exec_result)),
            Err(e) => {
                let (code, sqlstate, retryable) = classify_execution_error(&e);
                Err(self.make_error(req.request_id, code, sqlstate, retryable, &format!("{}", e)))
            }
        }
    }

    /// Execute a BatchRequest and return a BatchResponse.
    pub fn execute_batch(&self, req: &BatchRequest) -> BatchResponse {
        // Epoch fencing check
        if self.config.epoch_fencing_enabled && req.epoch != 0 && req.epoch != self.config.epoch {
            return BatchResponse {
                request_id: req.request_id,
                counts: vec![],
                error: Some(ErrorResponse {
                    request_id: req.request_id,
                    error_code: ERR_FENCED_EPOCH,
                    sqlstate: *b"F0001",
                    retryable: true,
                    server_epoch: self.config.epoch,
                    message: format!(
                        "epoch mismatch: client={}, server={}",
                        req.epoch, self.config.epoch
                    ),
                }),
            };
        }

        let continue_on_error = req.options & 1 != 0;
        let mut counts = Vec::with_capacity(req.rows.len());
        let mut first_error: Option<ErrorResponse> = None;

        for (i, _row) in req.rows.iter().enumerate() {
            // For each row in the batch, execute the SQL template
            // In a real implementation, this would substitute params from the row
            // For now, execute the SQL once per row as a simplified batch
            let txn = self.txn_mgr.begin_with_classification(
                IsolationLevel::ReadCommitted,
                TxnClassification::local(ShardId(0)),
            );

            let stmts = match parse_sql(&req.sql) {
                Ok(s) => s,
                Err(e) => {
                    let _ = self.txn_mgr.abort(txn.txn_id);
                    counts.push(-1);
                    if first_error.is_none() {
                        first_error = Some(self.make_error(
                            req.request_id,
                            ERR_SYNTAX_ERROR,
                            *b"42601",
                            false,
                            &format!("row {}: {}", i, e),
                        ));
                    }
                    if !continue_on_error {
                        break;
                    }
                    continue;
                }
            };

            if stmts.is_empty() {
                let _ = self.txn_mgr.commit(txn.txn_id);
                counts.push(0);
                continue;
            }

            let catalog = self.storage.get_catalog();
            let mut binder = Binder::new(catalog);
            let bound = match binder.bind(&stmts[0]) {
                Ok(b) => b,
                Err(e) => {
                    let _ = self.txn_mgr.abort(txn.txn_id);
                    counts.push(-1);
                    if first_error.is_none() {
                        first_error = Some(self.make_error(
                            req.request_id,
                            ERR_SYNTAX_ERROR,
                            *b"42000",
                            false,
                            &format!("row {}: {}", i, e),
                        ));
                    }
                    if !continue_on_error {
                        break;
                    }
                    continue;
                }
            };

            let plan = match Planner::plan(&bound) {
                Ok(p) => p,
                Err(e) => {
                    let _ = self.txn_mgr.abort(txn.txn_id);
                    counts.push(-1);
                    if first_error.is_none() {
                        first_error = Some(self.make_error(
                            req.request_id,
                            ERR_INTERNAL_ERROR,
                            *b"XX000",
                            false,
                            &format!("row {}: {}", i, e),
                        ));
                    }
                    if !continue_on_error {
                        break;
                    }
                    continue;
                }
            };

            match self.executor.execute(&plan, Some(&txn)) {
                Ok(exec_result) => {
                    let _ = self.txn_mgr.commit(txn.txn_id);
                    match exec_result {
                        ExecutionResult::Dml { rows_affected, .. } => {
                            counts.push(rows_affected as i64)
                        }
                        _ => counts.push(1),
                    }
                }
                Err(e) => {
                    let _ = self.txn_mgr.abort(txn.txn_id);
                    counts.push(-1);
                    if first_error.is_none() {
                        let (code, sqlstate, retryable) = classify_execution_error(&e);
                        first_error = Some(self.make_error(
                            req.request_id,
                            code,
                            sqlstate,
                            retryable,
                            &format!("row {}: {}", i, e),
                        ));
                    }
                    if !continue_on_error {
                        break;
                    }
                }
            }
        }

        BatchResponse {
            request_id: req.request_id,
            counts,
            error: first_error,
        }
    }

    /// Verify authentication credentials.
    pub fn authenticate(&self, user: &str, credential: &[u8]) -> bool {
        // Minimal auth: accept any non-empty password for now.
        // In production, this would check against a user catalog.
        !user.is_empty() && !credential.is_empty()
    }

    fn execution_result_to_response(
        &self,
        request_id: u64,
        result: ExecutionResult,
    ) -> QueryResponse {
        match result {
            ExecutionResult::Query { columns, rows } => {
                let col_meta: Vec<ColumnMeta> = columns
                    .iter()
                    .map(|(name, dt)| ColumnMeta {
                        name: name.clone(),
                        type_id: datatype_to_type_id(dt),
                        nullable: true,
                        precision: 0,
                        scale: 0,
                    })
                    .collect();

                let encoded_rows: Vec<EncodedRow> = rows
                    .iter()
                    .map(|row| EncodedRow {
                        values: row.values.iter().map(datum_to_encoded).collect(),
                    })
                    .collect();

                QueryResponse {
                    request_id,
                    columns: col_meta,
                    rows: encoded_rows,
                    rows_affected: 0,
                }
            }
            ExecutionResult::Dml { rows_affected, .. } => QueryResponse {
                request_id,
                columns: vec![],
                rows: vec![],
                rows_affected,
            },
            ExecutionResult::Ddl { message } => QueryResponse {
                request_id,
                columns: vec![ColumnMeta {
                    name: "result".into(),
                    type_id: TYPE_TEXT,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                }],
                rows: vec![EncodedRow {
                    values: vec![EncodedValue::Text(message)],
                }],
                rows_affected: 0,
            },
            ExecutionResult::TxnControl { action } => QueryResponse {
                request_id,
                columns: vec![ColumnMeta {
                    name: "result".into(),
                    type_id: TYPE_TEXT,
                    nullable: false,
                    precision: 0,
                    scale: 0,
                }],
                rows: vec![EncodedRow {
                    values: vec![EncodedValue::Text(action)],
                }],
                rows_affected: 0,
            },
        }
    }

    fn make_error(
        &self,
        request_id: u64,
        error_code: u32,
        sqlstate: [u8; 5],
        retryable: bool,
        message: &str,
    ) -> ErrorResponse {
        ErrorResponse {
            request_id,
            error_code,
            sqlstate,
            retryable,
            server_epoch: self.config.epoch,
            message: message.to_string(),
        }
    }
}

fn classify_execution_error(e: &falcon_common::error::FalconError) -> (u32, [u8; 5], bool) {
    use falcon_common::error::FalconError;
    match e {
        FalconError::Sql(_) => (ERR_SYNTAX_ERROR, *b"42000", false),
        FalconError::Txn(_) => (ERR_SERIALIZATION_CONFLICT, *b"40001", true),
        FalconError::ReadOnly(_) => (ERR_READ_ONLY, *b"25006", true),
        FalconError::Execution(_) => (ERR_INTERNAL_ERROR, *b"XX000", false),
        FalconError::Retryable { .. } => (ERR_NOT_LEADER, *b"F0002", true),
        _ => (ERR_INTERNAL_ERROR, *b"XX000", false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bridge() -> ExecutorBridge {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        ExecutorBridge::new(storage, txn_mgr, NativeServerConfig::default())
    }

    #[test]
    fn test_select_1() {
        let bridge = make_bridge();
        let req = QueryRequest {
            request_id: 1,
            epoch: 0,
            sql: "SELECT 1".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        };
        let resp = bridge.execute_query(&req).unwrap();
        assert_eq!(resp.request_id, 1);
        assert_eq!(resp.columns.len(), 1);
        assert_eq!(resp.rows.len(), 1);
    }

    #[test]
    fn test_select_multiple_columns() {
        let bridge = make_bridge();
        let req = QueryRequest {
            request_id: 2,
            epoch: 0,
            sql: "SELECT 1 AS a, 'hello' AS b, true AS c".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        };
        let resp = bridge.execute_query(&req).unwrap();
        assert_eq!(resp.request_id, 2);
        assert_eq!(resp.columns.len(), 3);
        assert_eq!(resp.rows.len(), 1);
    }

    #[test]
    fn test_syntax_error() {
        let bridge = make_bridge();
        let req = QueryRequest {
            request_id: 3,
            epoch: 0,
            sql: "SELECTT 1".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        };
        let err = bridge.execute_query(&req).unwrap_err();
        assert_eq!(err.error_code, ERR_SYNTAX_ERROR);
        assert!(!err.retryable);
    }

    #[test]
    fn test_epoch_fencing_reject() {
        let bridge = make_bridge();
        let req = QueryRequest {
            request_id: 4,
            epoch: 999, // wrong epoch
            sql: "SELECT 1".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        };
        let err = bridge.execute_query(&req).unwrap_err();
        assert_eq!(err.error_code, ERR_FENCED_EPOCH);
        assert!(err.retryable);
        assert_eq!(err.server_epoch, 1); // default config epoch
    }

    #[test]
    fn test_epoch_zero_bypasses_fencing() {
        let bridge = make_bridge();
        let req = QueryRequest {
            request_id: 5,
            epoch: 0, // epoch=0 means fencing disabled on client
            sql: "SELECT 1".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        };
        let resp = bridge.execute_query(&req).unwrap();
        assert_eq!(resp.request_id, 5);
    }

    #[test]
    fn test_create_and_query_table() {
        let bridge = make_bridge();

        // Create table
        let create = QueryRequest {
            request_id: 10,
            epoch: 0,
            sql: "CREATE TABLE test (id INT PRIMARY KEY, name TEXT)".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        };
        let resp = bridge.execute_query(&create).unwrap();
        assert_eq!(resp.request_id, 10);

        // Insert
        let insert = QueryRequest {
            request_id: 11,
            epoch: 0,
            sql: "INSERT INTO test VALUES (1, 'alice')".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        };
        let resp = bridge.execute_query(&insert).unwrap();
        assert_eq!(resp.rows_affected, 1);

        // Select
        let select = QueryRequest {
            request_id: 12,
            epoch: 0,
            sql: "SELECT * FROM test".into(),
            params: vec![],
            session_flags: SESSION_AUTOCOMMIT,
        };
        let resp = bridge.execute_query(&select).unwrap();
        assert_eq!(resp.rows.len(), 1);
        assert_eq!(resp.columns.len(), 2);
    }

    #[test]
    fn test_authenticate() {
        let bridge = make_bridge();
        assert!(bridge.authenticate("admin", b"password123"));
        assert!(!bridge.authenticate("", b"password123"));
        assert!(!bridge.authenticate("admin", b""));
    }

    #[test]
    fn test_batch_epoch_fencing() {
        let bridge = make_bridge();
        let req = BatchRequest {
            request_id: 20,
            epoch: 999,
            sql: "INSERT INTO t VALUES (1)".into(),
            column_types: vec![],
            rows: vec![],
            options: 0,
        };
        let resp = bridge.execute_batch(&req);
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().error_code, ERR_FENCED_EPOCH);
    }
}
