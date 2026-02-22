use thiserror::Error;

use crate::types::{TableId, TxnId};

/// Convenience alias for `Result<T, FalconError>`.
pub type FalconResult<T> = Result<T, FalconError>;

/// Error classification for retry/escalation decisions.
///
/// - `UserError`   — bad input, wrong SQL, permission denied (4xx equivalent)
/// - `Retryable`   — write conflict, leader change, epoch mismatch; client SHOULD retry
/// - `Transient`   — timeout, resource exhaustion, backpressure; client MAY retry after back-off
/// - `InternalBug` — should never happen; triggers alert + diagnostic dump
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    UserError,
    Retryable,
    Transient,
    InternalBug,
}

/// Top-level error type that all crate-specific errors convert into.
#[derive(Error, Debug)]
pub enum FalconError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Transaction error: {0}")]
    Txn(#[from] TxnError),

    #[error("SQL error: {0}")]
    Sql(#[from] SqlError),

    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),

    #[error("Cluster error: {0}")]
    Cluster(#[from] ClusterError),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Read-only: {0}")]
    ReadOnly(String),

    /// Retryable error with routing hint for client/proxy.
    /// Carries optional `leader_hint` (node address), `epoch`, `shard_id`, `retry_after_ms`.
    #[error("Retryable: {reason} (shard={shard_id}, epoch={epoch})")]
    Retryable {
        reason: String,
        shard_id: u64,
        epoch: u64,
        leader_hint: Option<String>,
        retry_after_ms: u64,
    },

    /// Transient resource/backpressure error.
    #[error("Transient: {reason} (retry after {retry_after_ms}ms)")]
    Transient { reason: String, retry_after_ms: u64 },

    /// Internal bug — should never occur in production.
    /// Always carries a unique `error_code` and `debug_context` for post-mortem.
    #[error("InternalBug [{error_code}]: {message} | context: {debug_context}")]
    InternalBug {
        error_code: &'static str,
        message: String,
        debug_context: String,
    },
}

/// Storage layer errors.
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Table not found: {0}")]
    TableNotFound(TableId),

    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Duplicate key")]
    DuplicateKey,

    #[error("Unique constraint violation on column {column_idx}: conflicting key in index")]
    UniqueViolation {
        column_idx: usize,
        /// Hex-encoded index key that conflicted.
        index_key_hex: String,
    },

    #[error("Serialization failure: concurrent modification detected")]
    SerializationFailure,

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Memory pressure: shard under pressure, write rejected ({used_bytes} / {limit_bytes} bytes)")]
    MemoryPressure { used_bytes: u64, limit_bytes: u64 },

    #[error("Memory limit exceeded: shard at hard limit, all transactions rejected ({used_bytes} / {limit_bytes} bytes)")]
    MemoryLimitExceeded { used_bytes: u64, limit_bytes: u64 },
}

/// Transaction layer errors.
#[derive(Error, Debug)]
pub enum TxnError {
    #[error("Transaction {0} conflict: write-write on same key")]
    WriteConflict(TxnId),

    #[error("Transaction {0} serialization conflict: read-set invalidated")]
    SerializationConflict(TxnId),

    #[error("Transaction {0} constraint violation: {1}")]
    ConstraintViolation(TxnId, String),

    #[error("Transaction {0} invariant violation: {1}")]
    InvariantViolation(TxnId, String),

    #[error("Transaction {0} aborted")]
    Aborted(TxnId),

    #[error("Transaction {0} not found")]
    NotFound(TxnId),

    #[error("Transaction {0} already committed")]
    AlreadyCommitted(TxnId),

    #[error("Transaction timeout")]
    Timeout,

    #[error("Transaction {0} rejected: shard memory pressure")]
    MemoryPressure(TxnId),

    #[error("Transaction {0} rejected: shard memory limit exceeded")]
    MemoryLimitExceeded(TxnId),

    #[error("Transaction {0} rejected: WAL backlog exceeds admission threshold")]
    WalBacklogExceeded(TxnId),

    #[error("Transaction {0} rejected: replication lag exceeds admission threshold")]
    ReplicationLagExceeded(TxnId),

    #[error("Transaction {0} invalid state transition: {1} → {2}")]
    InvalidTransition(TxnId, String, String),
}

/// SQL frontend errors.
#[derive(Error, Debug)]
pub enum SqlError {
    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Unknown table: {0}")]
    UnknownTable(String),

    #[error("Unknown column: {0}")]
    UnknownColumn(String),

    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("Unsupported feature: {0}")]
    Unsupported(String),

    #[error("Ambiguous column: {0}")]
    AmbiguousColumn(String),

    #[error("Invalid expression: {0}")]
    InvalidExpression(String),

    #[error("Cannot determine type of parameter ${0}: no context")]
    ParamTypeRequired(usize),

    #[error("Parameter ${index} type conflict: expected {expected}, got {got}")]
    ParamTypeConflict {
        index: usize,
        expected: String,
        got: String,
    },

    #[error("Internal invariant violation: {0}")]
    InternalInvariant(String),
}

/// Protocol layer errors.
#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Authentication failed")]
    AuthFailed,

    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Connection closed")]
    ConnectionClosed,
}

/// Execution engine errors.
#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Division by zero")]
    DivisionByZero,

    #[error("Type error: {0}")]
    TypeError(String),

    #[error("Column index out of bounds: {0}")]
    ColumnOutOfBounds(usize),

    #[error("Internal: {0}")]
    Internal(String),

    #[error("Parameter ${0} not provided")]
    ParamMissing(usize),

    #[error("Parameter ${index} type mismatch: expected {expected}, got {got}")]
    ParamTypeMismatch {
        index: usize,
        expected: String,
        got: String,
    },

    #[error("CHECK constraint violated: {0}")]
    CheckConstraintViolation(String),

    #[error("Insufficient privilege: {0}")]
    InsufficientPrivilege(String),

    #[error("Query governor abort: {0}")]
    GovernorAbort(String),
}

/// Cluster / metadata errors.
#[derive(Error, Debug)]
pub enum ClusterError {
    #[error("Shard not found: {0}")]
    ShardNotFound(u64),

    #[error("Node not found: {0}")]
    NodeNotFound(u64),

    #[error("Not leader")]
    NotLeader,

    #[error("Consensus error: {0}")]
    Consensus(String),
}

// ── FalconError classification & helpers ─────────────────────────────────────

impl FalconError {
    /// Classify this error for retry/escalation decisions.
    pub fn kind(&self) -> ErrorKind {
        match self {
            // User-facing errors (bad input, SQL errors, permission)
            FalconError::Sql(_) => ErrorKind::UserError,
            FalconError::ReadOnly(_) => ErrorKind::UserError,
            FalconError::Protocol(ProtocolError::AuthFailed) => ErrorKind::UserError,
            FalconError::Protocol(ProtocolError::InvalidMessage(_)) => ErrorKind::UserError,
            FalconError::Execution(ExecutionError::TypeError(_)) => ErrorKind::UserError,
            FalconError::Execution(ExecutionError::DivisionByZero) => ErrorKind::UserError,
            FalconError::Execution(ExecutionError::ParamMissing(_)) => ErrorKind::UserError,
            FalconError::Execution(ExecutionError::ParamTypeMismatch { .. }) => {
                ErrorKind::UserError
            }
            FalconError::Execution(ExecutionError::InsufficientPrivilege(_)) => {
                ErrorKind::UserError
            }
            FalconError::Execution(ExecutionError::GovernorAbort(_)) => ErrorKind::UserError,
            FalconError::Execution(ExecutionError::CheckConstraintViolation(_)) => {
                ErrorKind::UserError
            }
            FalconError::Storage(StorageError::TableNotFound(_)) => ErrorKind::UserError,
            FalconError::Storage(StorageError::TableAlreadyExists(_)) => ErrorKind::UserError,
            FalconError::Storage(StorageError::DuplicateKey) => ErrorKind::UserError,
            FalconError::Storage(StorageError::UniqueViolation { .. }) => ErrorKind::UserError,
            FalconError::Txn(TxnError::ConstraintViolation(_, _)) => ErrorKind::UserError,

            // Retryable errors (write conflict, leader change, epoch mismatch)
            FalconError::Retryable { .. } => ErrorKind::Retryable,
            FalconError::Txn(TxnError::WriteConflict(_)) => ErrorKind::Retryable,
            FalconError::Txn(TxnError::SerializationConflict(_)) => ErrorKind::Retryable,
            FalconError::Txn(TxnError::Aborted(_)) => ErrorKind::Retryable,
            FalconError::Storage(StorageError::SerializationFailure) => ErrorKind::Retryable,
            FalconError::Cluster(ClusterError::NotLeader) => ErrorKind::Retryable,

            // Transient errors (timeout, resource exhaustion, backpressure)
            FalconError::Transient { .. } => ErrorKind::Transient,
            FalconError::Txn(TxnError::Timeout) => ErrorKind::Transient,
            FalconError::Txn(TxnError::MemoryPressure(_)) => ErrorKind::Transient,
            FalconError::Txn(TxnError::MemoryLimitExceeded(_)) => ErrorKind::Transient,
            FalconError::Txn(TxnError::WalBacklogExceeded(_)) => ErrorKind::Transient,
            FalconError::Txn(TxnError::ReplicationLagExceeded(_)) => ErrorKind::Transient,
            FalconError::Storage(StorageError::MemoryPressure { .. }) => ErrorKind::Transient,
            FalconError::Storage(StorageError::MemoryLimitExceeded { .. }) => ErrorKind::Transient,
            FalconError::Protocol(ProtocolError::Io(_)) => ErrorKind::Transient,
            FalconError::Protocol(ProtocolError::ConnectionClosed) => ErrorKind::Transient,

            // Everything else is an internal bug
            FalconError::InternalBug { .. } => ErrorKind::InternalBug,
            _ => ErrorKind::InternalBug,
        }
    }

    /// Returns true if the client should retry this operation.
    pub fn is_retryable(&self) -> bool {
        matches!(self.kind(), ErrorKind::Retryable)
    }

    /// Returns true if this is a user/input error (4xx equivalent).
    pub fn is_user_error(&self) -> bool {
        matches!(self.kind(), ErrorKind::UserError)
    }

    /// Returns true if this is a transient resource/backpressure error.
    pub fn is_transient(&self) -> bool {
        matches!(self.kind(), ErrorKind::Transient)
    }

    /// Returns true if this is an internal bug that should never occur.
    pub fn is_internal_bug(&self) -> bool {
        matches!(self.kind(), ErrorKind::InternalBug)
    }

    /// Suggested retry delay in milliseconds (0 = retry immediately).
    pub fn retry_after_ms(&self) -> u64 {
        match self {
            FalconError::Retryable { retry_after_ms, .. } => *retry_after_ms,
            FalconError::Transient { retry_after_ms, .. } => *retry_after_ms,
            FalconError::Txn(TxnError::Timeout) => 100,
            FalconError::Txn(TxnError::MemoryPressure(_)) => 50,
            FalconError::Txn(TxnError::WalBacklogExceeded(_)) => 200,
            FalconError::Txn(TxnError::ReplicationLagExceeded(_)) => 500,
            _ => 0,
        }
    }

    /// Map to a PostgreSQL SQLSTATE code.
    pub fn pg_sqlstate(&self) -> &'static str {
        match self {
            FalconError::Sql(SqlError::Parse(_)) => "42601", // syntax_error
            FalconError::Sql(SqlError::UnknownTable(_)) => "42P01", // undefined_table
            FalconError::Sql(SqlError::UnknownColumn(_)) => "42703", // undefined_column
            FalconError::Sql(SqlError::TypeMismatch { .. }) => "42804", // datatype_mismatch
            FalconError::Sql(SqlError::Unsupported(_)) => "0A000", // feature_not_supported
            FalconError::Sql(SqlError::AmbiguousColumn(_)) => "42702", // ambiguous_column
            FalconError::Sql(_) => "42000", // syntax_error_or_access_rule_violation
            FalconError::Storage(StorageError::TableNotFound(_)) => "42P01",
            FalconError::Storage(StorageError::TableAlreadyExists(_)) => "42P07", // duplicate_table
            FalconError::Storage(StorageError::DuplicateKey) => "23505", // unique_violation
            FalconError::Storage(StorageError::UniqueViolation { .. }) => "23505",
            FalconError::Storage(StorageError::SerializationFailure) => "40001", // serialization_failure
            FalconError::Storage(StorageError::MemoryPressure { .. }) => "53200", // out_of_memory
            FalconError::Storage(StorageError::MemoryLimitExceeded { .. }) => "53200",
            FalconError::Txn(TxnError::WriteConflict(_)) => "40001",
            FalconError::Txn(TxnError::SerializationConflict(_)) => "40001",
            FalconError::Txn(TxnError::ConstraintViolation(_, _)) => "23000", // integrity_constraint_violation
            FalconError::Txn(TxnError::Timeout) => "57014",                   // query_canceled
            FalconError::Txn(TxnError::MemoryPressure(_)) => "53200",
            FalconError::Txn(TxnError::MemoryLimitExceeded(_)) => "53200",
            FalconError::Txn(TxnError::WalBacklogExceeded(_)) => "53300", // too_many_connections (reuse)
            FalconError::Txn(TxnError::ReplicationLagExceeded(_)) => "57P03", // cannot_connect_now
            FalconError::ReadOnly(_) => "25006", // read_only_sql_transaction
            FalconError::Protocol(ProtocolError::AuthFailed) => "28P01", // invalid_password
            FalconError::Protocol(ProtocolError::InvalidMessage(_)) => "08P01", // protocol_violation
            FalconError::Protocol(ProtocolError::ConnectionClosed) => "08006", // connection_failure
            FalconError::Execution(ExecutionError::DivisionByZero) => "22012", // division_by_zero
            FalconError::Execution(ExecutionError::TypeError(_)) => "22000",   // data_exception
            FalconError::Execution(ExecutionError::InsufficientPrivilege(_)) => "42501", // insufficient_privilege
            FalconError::Execution(ExecutionError::GovernorAbort(_)) => "57014", // query_canceled
            FalconError::Execution(ExecutionError::CheckConstraintViolation(_)) => "23514", // check_violation
            FalconError::Retryable { .. } => "40001",
            FalconError::Transient { .. } => "53000", // insufficient_resources
            FalconError::InternalBug { .. } => "XX000", // internal_error
            FalconError::Internal(_) => "XX000",
            _ => "XX000",
        }
    }

    /// Map to a PostgreSQL severity string.
    pub fn pg_severity(&self) -> &'static str {
        match self.kind() {
            ErrorKind::UserError => "ERROR",
            ErrorKind::Retryable => "ERROR",
            ErrorKind::Transient => "ERROR",
            ErrorKind::InternalBug => "FATAL",
        }
    }

    /// Construct a retryable error with routing context.
    pub fn retryable(
        reason: impl Into<String>,
        shard_id: u64,
        epoch: u64,
        leader_hint: Option<String>,
        retry_after_ms: u64,
    ) -> Self {
        FalconError::Retryable {
            reason: reason.into(),
            shard_id,
            epoch,
            leader_hint,
            retry_after_ms,
        }
    }

    /// Construct a transient backpressure error.
    pub fn transient(reason: impl Into<String>, retry_after_ms: u64) -> Self {
        FalconError::Transient {
            reason: reason.into(),
            retry_after_ms,
        }
    }

    /// Construct an internal bug error with error code and context.
    pub fn internal_bug(
        error_code: &'static str,
        message: impl Into<String>,
        debug_context: impl Into<String>,
    ) -> Self {
        FalconError::InternalBug {
            error_code,
            message: message.into(),
            debug_context: debug_context.into(),
        }
    }

    /// Add context string to an error, **preserving error classification**.
    ///
    /// For structured variants (`Retryable`, `Transient`, `InternalBug`), the
    /// context is prepended to the `reason`/`message` field. For `Internal`,
    /// it is prepended to the string. For all other variants, the error is
    /// wrapped as `Internal` with context prefix (last resort).
    pub fn with_context(self, ctx: impl Into<String>) -> Self {
        let ctx = ctx.into();
        match self {
            FalconError::Internal(msg) => FalconError::Internal(format!("{ctx}: {msg}")),
            FalconError::Retryable {
                reason,
                shard_id,
                epoch,
                leader_hint,
                retry_after_ms,
            } => FalconError::Retryable {
                reason: format!("{ctx}: {reason}"),
                shard_id,
                epoch,
                leader_hint,
                retry_after_ms,
            },
            FalconError::Transient {
                reason,
                retry_after_ms,
            } => FalconError::Transient {
                reason: format!("{ctx}: {reason}"),
                retry_after_ms,
            },
            FalconError::InternalBug {
                error_code,
                message,
                debug_context,
            } => FalconError::InternalBug {
                error_code,
                message: format!("{ctx}: {message}"),
                debug_context,
            },
            other => FalconError::Internal(format!("{ctx}: {other}")),
        }
    }

    /// Enrich this error with `RequestContext` fields for structured diagnostics.
    ///
    /// Appends `request_id`, `session_id`, `query_id`, `txn_id`, `shard_id`
    /// to the error's context/debug fields so that every error response can
    /// be correlated back to a specific request in logs.
    pub fn with_request_context(self, rctx: &crate::request_context::RequestContext) -> Self {
        let tag = format!(
            "req={} sess={} qry={} txn={} shard={}",
            rctx.request_id, rctx.session_id, rctx.query_id, rctx.txn_id, rctx.shard_id
        );
        match self {
            FalconError::InternalBug {
                error_code,
                message,
                debug_context,
            } => {
                let dc = if debug_context.is_empty() {
                    tag
                } else {
                    format!("{debug_context} | {tag}")
                };
                FalconError::InternalBug {
                    error_code,
                    message,
                    debug_context: dc,
                }
            }
            FalconError::Retryable {
                reason,
                shard_id,
                epoch,
                leader_hint,
                retry_after_ms,
            } => FalconError::Retryable {
                reason: format!("{reason} [{tag}]"),
                shard_id,
                epoch,
                leader_hint,
                retry_after_ms,
            },
            FalconError::Transient {
                reason,
                retry_after_ms,
            } => FalconError::Transient {
                reason: format!("{reason} [{tag}]"),
                retry_after_ms,
            },
            FalconError::Internal(msg) => FalconError::Internal(format!("{msg} [{tag}]")),
            other => other.with_context(tag),
        }
    }

    /// Set `leader_hint` on a `Retryable` error. No-op for other variants.
    pub fn with_leader_hint(self, hint: impl Into<String>) -> Self {
        match self {
            FalconError::Retryable {
                reason,
                shard_id,
                epoch,
                retry_after_ms,
                ..
            } => FalconError::Retryable {
                reason,
                shard_id,
                epoch,
                leader_hint: Some(hint.into()),
                retry_after_ms,
            },
            other => other,
        }
    }

    /// Emit a structured log entry for Fatal/InternalBug errors.
    /// Must be called for every Fatal error before returning to the client.
    /// Log format is stable across patch versions.
    pub fn log_if_fatal(&self) {
        if let FalconError::InternalBug {
            error_code,
            message,
            debug_context,
        } = self
        {
            tracing::error!(
                error_code = error_code,
                error_category = "Fatal",
                component = Self::affected_component(self),
                sqlstate = self.pg_sqlstate(),
                debug_context = debug_context.as_str(),
                "FATAL [{}]: {} | context: {}",
                error_code,
                message,
                debug_context
            );
        }
    }

    /// Identify the affected component for structured logging.
    fn affected_component(&self) -> &'static str {
        match self {
            FalconError::Storage(_) => "storage",
            FalconError::Txn(_) => "txn",
            FalconError::Sql(_) => "sql",
            FalconError::Protocol(_) => "protocol",
            FalconError::Execution(_) => "executor",
            FalconError::Cluster(_) => "cluster",
            FalconError::Retryable { .. } => "cluster",
            FalconError::Transient { .. } => "resource",
            FalconError::InternalBug { .. } => "internal",
            FalconError::Internal(_) => "internal",
            FalconError::ReadOnly(_) => "txn",
        }
    }

    /// Set `debug_context` on an `InternalBug` error. No-op for other variants.
    pub fn with_debug_context(self, ctx: impl Into<String>) -> Self {
        match self {
            FalconError::InternalBug {
                error_code,
                message,
                debug_context,
            } => {
                let dc = ctx.into();
                let dc = if debug_context.is_empty() {
                    dc
                } else {
                    format!("{debug_context} | {dc}")
                };
                FalconError::InternalBug {
                    error_code,
                    message,
                    debug_context: dc,
                }
            }
            other => other,
        }
    }
}

/// Bail with a user-facing error (SQL/input/permission).
/// Usage: `bail_user!("42601", "syntax error near {}", token)`
#[macro_export]
macro_rules! bail_user {
    ($code:expr, $msg:expr) => {
        return Err($crate::error::FalconError::Sql(
            $crate::error::SqlError::InvalidExpression(format!("[{}] {}", $code, $msg))
        ))
    };
    ($code:expr, $fmt:expr, $($arg:tt)*) => {
        return Err($crate::error::FalconError::Sql(
            $crate::error::SqlError::InvalidExpression(format!("[{}] {}", $code, format!($fmt, $($arg)*)))
        ))
    };
}

/// Bail with a retryable error.
/// Usage: `bail_retryable!(shard_id, epoch, "leader changed")`
#[macro_export]
macro_rules! bail_retryable {
    ($shard_id:expr, $epoch:expr, $msg:expr) => {
        return Err($crate::error::FalconError::retryable($msg, $shard_id as u64, $epoch as u64, None, 50))
    };
    ($shard_id:expr, $epoch:expr, $fmt:expr, $($arg:tt)*) => {
        return Err($crate::error::FalconError::retryable(
            format!($fmt, $($arg)*), $shard_id as u64, $epoch as u64, None, 50
        ))
    };
}

/// Bail with a transient backpressure error.
/// Usage: `bail_transient!(200, "WAL backlog full")`
#[macro_export]
macro_rules! bail_transient {
    ($retry_ms:expr, $msg:expr) => {
        return Err($crate::error::FalconError::transient($msg, $retry_ms))
    };
    ($retry_ms:expr, $fmt:expr, $($arg:tt)*) => {
        return Err($crate::error::FalconError::transient(format!($fmt, $($arg)*), $retry_ms))
    };
}

/// Add context to a Result, preserving error classification.
/// Usage: `some_result.ctx("stage=commit, txn=42")?`
pub trait ErrorContext<T> {
    fn ctx(self, context: &str) -> Result<T, FalconError>;
    fn ctx_with(self, f: impl FnOnce() -> String) -> Result<T, FalconError>;
    /// Enrich the error with `RequestContext` fields (req/sess/qry/txn/shard IDs).
    fn ctx_request(self, rctx: &crate::request_context::RequestContext) -> Result<T, FalconError>;
}

impl<T, E: Into<FalconError>> ErrorContext<T> for Result<T, E> {
    fn ctx(self, context: &str) -> Result<T, FalconError> {
        self.map_err(|e| e.into().with_context(context))
    }
    fn ctx_with(self, f: impl FnOnce() -> String) -> Result<T, FalconError> {
        self.map_err(|e| e.into().with_context(f()))
    }
    fn ctx_request(self, rctx: &crate::request_context::RequestContext) -> Result<T, FalconError> {
        self.map_err(|e| e.into().with_request_context(rctx))
    }
}

#[cfg(test)]
mod error_classification {
    use super::*;
    use crate::types::TxnId;

    // ── ErrorKind classification ──────────────────────────────────────────────

    #[test]
    fn test_sql_errors_are_user_errors() {
        let e = FalconError::Sql(SqlError::Parse("bad syntax".into()));
        assert_eq!(e.kind(), ErrorKind::UserError);
        assert!(e.is_user_error());
        assert!(!e.is_retryable());
        assert!(!e.is_transient());
        assert!(!e.is_internal_bug());
    }

    #[test]
    fn test_unknown_table_is_user_error() {
        let e = FalconError::Sql(SqlError::UnknownTable("foo".into()));
        assert_eq!(e.kind(), ErrorKind::UserError);
    }

    #[test]
    fn test_read_only_is_user_error() {
        let e = FalconError::ReadOnly("write on replica".into());
        assert_eq!(e.kind(), ErrorKind::UserError);
        assert_eq!(e.pg_sqlstate(), "25006");
    }

    #[test]
    fn test_write_conflict_is_retryable() {
        let e = FalconError::Txn(TxnError::WriteConflict(TxnId(42)));
        assert_eq!(e.kind(), ErrorKind::Retryable);
        assert!(e.is_retryable());
        assert_eq!(e.pg_sqlstate(), "40001");
    }

    #[test]
    fn test_serialization_conflict_is_retryable() {
        let e = FalconError::Txn(TxnError::SerializationConflict(TxnId(1)));
        assert_eq!(e.kind(), ErrorKind::Retryable);
        assert_eq!(e.pg_sqlstate(), "40001");
    }

    #[test]
    fn test_not_leader_is_retryable() {
        let e = FalconError::Cluster(ClusterError::NotLeader);
        assert_eq!(e.kind(), ErrorKind::Retryable);
        assert!(e.is_retryable());
    }

    #[test]
    fn test_retryable_variant_is_retryable() {
        let e = FalconError::retryable("leader changed", 1, 5, Some("node2:5432".into()), 50);
        assert_eq!(e.kind(), ErrorKind::Retryable);
        assert_eq!(e.retry_after_ms(), 50);
        assert_eq!(e.pg_sqlstate(), "40001");
    }

    #[test]
    fn test_txn_timeout_is_transient() {
        let e = FalconError::Txn(TxnError::Timeout);
        assert_eq!(e.kind(), ErrorKind::Transient);
        assert!(e.is_transient());
        assert_eq!(e.pg_sqlstate(), "57014");
        assert_eq!(e.retry_after_ms(), 100);
    }

    #[test]
    fn test_memory_pressure_is_transient() {
        let e = FalconError::Txn(TxnError::MemoryPressure(TxnId(1)));
        assert_eq!(e.kind(), ErrorKind::Transient);
        assert_eq!(e.retry_after_ms(), 50);
    }

    #[test]
    fn test_wal_backlog_is_transient() {
        let e = FalconError::Txn(TxnError::WalBacklogExceeded(TxnId(1)));
        assert_eq!(e.kind(), ErrorKind::Transient);
        assert_eq!(e.retry_after_ms(), 200);
    }

    #[test]
    fn test_replication_lag_is_transient() {
        let e = FalconError::Txn(TxnError::ReplicationLagExceeded(TxnId(1)));
        assert_eq!(e.kind(), ErrorKind::Transient);
        assert_eq!(e.retry_after_ms(), 500);
    }

    #[test]
    fn test_transient_variant_is_transient() {
        let e = FalconError::transient("WAL backlog full", 200);
        assert_eq!(e.kind(), ErrorKind::Transient);
        assert_eq!(e.retry_after_ms(), 200);
        assert_eq!(e.pg_sqlstate(), "53000");
    }

    #[test]
    fn test_internal_bug_variant() {
        let e = FalconError::internal_bug(
            "E001",
            "unexpected None in shard map",
            "stage=route, shard=3, epoch=7",
        );
        assert_eq!(e.kind(), ErrorKind::InternalBug);
        assert!(e.is_internal_bug());
        assert_eq!(e.pg_sqlstate(), "XX000");
        assert_eq!(e.pg_severity(), "FATAL");
    }

    #[test]
    fn test_internal_string_is_internal_bug() {
        let e = FalconError::Internal("something went wrong".into());
        assert_eq!(e.kind(), ErrorKind::InternalBug);
        assert_eq!(e.pg_sqlstate(), "XX000");
    }

    // ── SQLSTATE mapping ─────────────────────────────────────────────────────

    #[test]
    fn test_sqlstate_parse_error() {
        let e = FalconError::Sql(SqlError::Parse("x".into()));
        assert_eq!(e.pg_sqlstate(), "42601");
    }

    #[test]
    fn test_sqlstate_unique_violation() {
        let e = FalconError::Storage(StorageError::DuplicateKey);
        assert_eq!(e.pg_sqlstate(), "23505");
    }

    #[test]
    fn test_sqlstate_table_not_found() {
        use crate::types::TableId;
        let e = FalconError::Storage(StorageError::TableNotFound(TableId(99)));
        assert_eq!(e.pg_sqlstate(), "42P01");
    }

    #[test]
    fn test_sqlstate_auth_failed() {
        let e = FalconError::Protocol(ProtocolError::AuthFailed);
        assert_eq!(e.pg_sqlstate(), "28P01");
        assert_eq!(e.kind(), ErrorKind::UserError);
    }

    #[test]
    fn test_sqlstate_division_by_zero() {
        let e = FalconError::Execution(ExecutionError::DivisionByZero);
        assert_eq!(e.pg_sqlstate(), "22012");
        assert_eq!(e.kind(), ErrorKind::UserError);
    }

    // ── pg_severity ──────────────────────────────────────────────────────────

    #[test]
    fn test_severity_user_error_is_error() {
        let e = FalconError::Sql(SqlError::Parse("x".into()));
        assert_eq!(e.pg_severity(), "ERROR");
    }

    #[test]
    fn test_severity_retryable_is_error() {
        let e = FalconError::Txn(TxnError::WriteConflict(TxnId(1)));
        assert_eq!(e.pg_severity(), "ERROR");
    }

    #[test]
    fn test_severity_internal_bug_is_fatal() {
        let e = FalconError::internal_bug("E001", "bug", "ctx");
        assert_eq!(e.pg_severity(), "FATAL");
    }

    // ── with_context ─────────────────────────────────────────────────────────

    #[test]
    fn test_with_context_wraps_message() {
        let e = FalconError::Internal("original".into());
        let e2 = e.with_context("stage=commit, txn=42");
        assert!(e2.to_string().contains("stage=commit"));
        assert!(e2.to_string().contains("original"));
    }

    #[test]
    fn test_with_context_on_storage_error() {
        let e = FalconError::Storage(StorageError::KeyNotFound);
        let e2 = e.with_context("stage=lookup, shard=1");
        assert!(e2.to_string().contains("stage=lookup"));
    }

    // ── ErrorContext trait ────────────────────────────────────────────────────

    #[test]
    fn test_error_context_trait_ctx() {
        use super::ErrorContext;
        let result: Result<(), StorageError> = Err(StorageError::KeyNotFound);
        let result2: Result<(), FalconError> = result.ctx("stage=scan, shard=0");
        let err = result2.unwrap_err();
        assert!(err.to_string().contains("stage=scan"));
    }

    #[test]
    fn test_error_context_trait_ctx_with() {
        use super::ErrorContext;
        let txn_id = 42u64;
        let result: Result<(), StorageError> = Err(StorageError::DuplicateKey);
        let result2: Result<(), FalconError> = result.ctx_with(|| format!("txn={}", txn_id));
        let err = result2.unwrap_err();
        assert!(err.to_string().contains("txn=42"));
    }

    #[test]
    fn test_error_context_ok_passthrough() {
        use super::ErrorContext;
        let result: Result<i32, StorageError> = Ok(42);
        let result2: Result<i32, FalconError> = result.ctx("should not appear");
        assert_eq!(result2.unwrap(), 42);
    }

    // ── Constructor helpers ───────────────────────────────────────────────────

    #[test]
    fn test_retryable_constructor() {
        let e = FalconError::retryable("epoch mismatch", 3, 7, None, 100);
        match e {
            FalconError::Retryable {
                reason,
                shard_id,
                epoch,
                leader_hint,
                retry_after_ms,
            } => {
                assert_eq!(reason, "epoch mismatch");
                assert_eq!(shard_id, 3);
                assert_eq!(epoch, 7);
                assert!(leader_hint.is_none());
                assert_eq!(retry_after_ms, 100);
            }
            _ => panic!("expected Retryable variant"),
        }
    }

    #[test]
    fn test_transient_constructor() {
        let e = FalconError::transient("memory pressure", 50);
        match e {
            FalconError::Transient {
                reason,
                retry_after_ms,
            } => {
                assert_eq!(reason, "memory pressure");
                assert_eq!(retry_after_ms, 50);
            }
            _ => panic!("expected Transient variant"),
        }
    }

    #[test]
    fn test_internal_bug_constructor() {
        let e = FalconError::internal_bug("E001", "null shard", "shard=3");
        match e {
            FalconError::InternalBug {
                error_code,
                message,
                debug_context,
            } => {
                assert_eq!(error_code, "E001");
                assert_eq!(message, "null shard");
                assert_eq!(debug_context, "shard=3");
            }
            _ => panic!("expected InternalBug variant"),
        }
    }

    // ── with_context preserves classification ─────────────────────────────

    #[test]
    fn test_with_context_preserves_retryable() {
        let e = FalconError::retryable("epoch mismatch", 3, 7, None, 100);
        let e2 = e.with_context("stage=prepare");
        assert_eq!(e2.kind(), ErrorKind::Retryable);
        assert!(e2.to_string().contains("stage=prepare"));
        assert!(e2.to_string().contains("epoch mismatch"));
    }

    #[test]
    fn test_with_context_preserves_transient() {
        let e = FalconError::transient("memory pressure", 50);
        let e2 = e.with_context("shard=2");
        assert_eq!(e2.kind(), ErrorKind::Transient);
        assert!(e2.to_string().contains("shard=2"));
    }

    #[test]
    fn test_with_context_preserves_internal_bug() {
        let e = FalconError::internal_bug("E-CRASH-001", "null shard", "");
        let e2 = e.with_context("handler");
        assert_eq!(e2.kind(), ErrorKind::InternalBug);
        assert!(e2.to_string().contains("handler"));
    }

    // ── with_request_context ────────────────────────────────────────────────

    #[test]
    fn test_with_request_context_on_internal() {
        use crate::request_context::RequestContext;
        let rctx = RequestContext::with_ids(100, 200, 300);
        let e = FalconError::Internal("something broke".into());
        let e2 = e.with_request_context(&rctx);
        let s = e2.to_string();
        assert!(s.contains("req=100"));
        assert!(s.contains("sess=200"));
        assert!(s.contains("qry=300"));
    }

    #[test]
    fn test_with_request_context_on_internal_bug() {
        use crate::request_context::RequestContext;
        let rctx = RequestContext::with_ids(10, 20, 30);
        let e = FalconError::internal_bug("E-TEST-001", "test bug", "");
        let e2 = e.with_request_context(&rctx);
        match &e2 {
            FalconError::InternalBug { debug_context, .. } => {
                assert!(debug_context.contains("req=10"));
                assert!(debug_context.contains("sess=20"));
            }
            _ => panic!("expected InternalBug"),
        }
        assert_eq!(e2.kind(), ErrorKind::InternalBug);
    }

    #[test]
    fn test_with_request_context_on_retryable() {
        use crate::request_context::RequestContext;
        let rctx = RequestContext::with_ids(1, 2, 3);
        let e = FalconError::retryable("conflict", 0, 1, None, 50);
        let e2 = e.with_request_context(&rctx);
        assert_eq!(e2.kind(), ErrorKind::Retryable);
        assert!(e2.to_string().contains("req=1"));
    }

    // ── with_leader_hint ────────────────────────────────────────────────────

    #[test]
    fn test_with_leader_hint_on_retryable() {
        let e = FalconError::retryable("not leader", 1, 5, None, 50);
        let e2 = e.with_leader_hint("node-3:5432");
        match &e2 {
            FalconError::Retryable { leader_hint, .. } => {
                assert_eq!(leader_hint.as_deref(), Some("node-3:5432"));
            }
            _ => panic!("expected Retryable"),
        }
    }

    #[test]
    fn test_with_leader_hint_noop_on_internal() {
        let e = FalconError::Internal("test".into());
        let e2 = e.with_leader_hint("node-1");
        assert!(matches!(e2, FalconError::Internal(_)));
    }

    // ── with_debug_context ──────────────────────────────────────────────────

    #[test]
    fn test_with_debug_context_on_internal_bug() {
        let e = FalconError::internal_bug("E-001", "msg", "");
        let e2 = e.with_debug_context("shard=3, txn=42");
        match &e2 {
            FalconError::InternalBug { debug_context, .. } => {
                assert_eq!(debug_context, "shard=3, txn=42");
            }
            _ => panic!("expected InternalBug"),
        }
    }

    #[test]
    fn test_with_debug_context_appends() {
        let e = FalconError::internal_bug("E-001", "msg", "existing");
        let e2 = e.with_debug_context("extra");
        match &e2 {
            FalconError::InternalBug { debug_context, .. } => {
                assert!(debug_context.contains("existing"));
                assert!(debug_context.contains("extra"));
            }
            _ => panic!("expected InternalBug"),
        }
    }

    // ── From conversions ─────────────────────────────────────────────────────

    #[test]
    fn test_from_storage_error() {
        let e: FalconError = StorageError::DuplicateKey.into();
        assert_eq!(e.kind(), ErrorKind::UserError);
    }

    #[test]
    fn test_from_txn_error() {
        let e: FalconError = TxnError::WriteConflict(TxnId(1)).into();
        assert_eq!(e.kind(), ErrorKind::Retryable);
    }

    #[test]
    fn test_from_sql_error() {
        let e: FalconError = SqlError::Parse("x".into()).into();
        assert_eq!(e.kind(), ErrorKind::UserError);
    }
}
