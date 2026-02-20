use thiserror::Error;

use crate::types::{TableId, TxnId};

/// Top-level error type that all crate-specific errors convert into.
#[derive(Error, Debug)]
pub enum CedarError {
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
    MemoryPressure {
        used_bytes: u64,
        limit_bytes: u64,
    },

    #[error("Memory limit exceeded: shard at hard limit, all transactions rejected ({used_bytes} / {limit_bytes} bytes)")]
    MemoryLimitExceeded {
        used_bytes: u64,
        limit_bytes: u64,
    },
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
    ParamTypeConflict { index: usize, expected: String, got: String },
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
    ParamTypeMismatch { index: usize, expected: String, got: String },
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

impl CedarError {
    /// Map to PostgreSQL SQLSTATE code.
    pub fn pg_sqlstate(&self) -> &'static str {
        match self {
            CedarError::Storage(StorageError::TableNotFound(_)) => "42P01",
            CedarError::Storage(StorageError::TableAlreadyExists(_)) => "42P07",
            CedarError::Storage(StorageError::DuplicateKey) => "23505",
            CedarError::Storage(StorageError::UniqueViolation { .. }) => "23505",
            CedarError::Storage(StorageError::SerializationFailure) => "40001",
            CedarError::Txn(TxnError::WriteConflict(_)) => "40001",
            CedarError::Txn(TxnError::SerializationConflict(_)) => "40001",
            CedarError::Txn(TxnError::ConstraintViolation(_, _)) => "23000",
            CedarError::Txn(TxnError::InvariantViolation(_, _)) => "XX001",
            CedarError::Txn(TxnError::Aborted(_)) => "40000",
            CedarError::Sql(SqlError::Parse(_)) => "42601",
            CedarError::Sql(SqlError::UnknownTable(_)) => "42P01",
            CedarError::Sql(SqlError::UnknownColumn(_)) => "42703",
            CedarError::Sql(SqlError::TypeMismatch { .. }) => "42804",
            CedarError::Sql(SqlError::Unsupported(_)) => "0A000",
            CedarError::Sql(SqlError::ParamTypeRequired(_)) => "42P18",
            CedarError::Sql(SqlError::ParamTypeConflict { .. }) => "42P18",
            CedarError::Execution(ExecutionError::ParamMissing(_)) => "08P01",
            CedarError::Execution(ExecutionError::ParamTypeMismatch { .. }) => "42804",
            CedarError::Execution(ExecutionError::DivisionByZero) => "22012",
            CedarError::Storage(StorageError::MemoryPressure { .. }) => "53000",
            CedarError::Storage(StorageError::MemoryLimitExceeded { .. }) => "53200",
            CedarError::Txn(TxnError::MemoryPressure(_)) => "53000",
            CedarError::Txn(TxnError::MemoryLimitExceeded(_)) => "53200",
            CedarError::ReadOnly(_) => "25006",
            _ => "XX000",
        }
    }

    /// PG severity string.
    pub fn pg_severity(&self) -> &'static str {
        match self {
            CedarError::Internal(_) => "FATAL",
            _ => "ERROR",
        }
    }
}
