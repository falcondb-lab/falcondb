use serde::{Deserialize, Serialize};
use std::fmt;

/// Unique identifier for a table within the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u64);

/// Unique identifier for a column within a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnId(pub u32);

/// Unique identifier for a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardId(pub u64);

/// Unique identifier for a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// Transaction identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TxnId(pub u64);

/// Logical timestamp for MVCC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub const MIN: Timestamp = Timestamp(0);
    pub const MAX: Timestamp = Timestamp(u64::MAX);

    pub fn next(self) -> Timestamp {
        Timestamp(self.0 + 1)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ts:{}", self.0)
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn:{}", self.0)
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tbl:{}", self.0)
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "shard:{}", self.0)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node:{}", self.0)
    }
}

/// SQL data types supported by CedarDB.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int32,
    Int64,
    Float64,
    Text,
    Timestamp,
    Date,
    Array(Box<DataType>),
    Jsonb,
}

impl DataType {
    /// Return the PG OID for this type.
    pub fn pg_oid(&self) -> i32 {
        match self {
            DataType::Boolean => 16,
            DataType::Int32 => 23,
            DataType::Int64 => 20,
            DataType::Float64 => 701,
            DataType::Text => 25,
            DataType::Timestamp => 1114,
            DataType::Date => 1082,
            DataType::Array(_) => 2277, // anyarray OID
            DataType::Jsonb => 3802, // jsonb OID
        }
    }

    /// Byte size hint (-1 for variable length).
    pub fn type_len(&self) -> i16 {
        match self {
            DataType::Boolean => 1,
            DataType::Int32 => 4,
            DataType::Int64 => 8,
            DataType::Float64 => 8,
            DataType::Text => -1,
            DataType::Timestamp => 8,
            DataType::Date => 4,
            DataType::Array(_) => -1,
            DataType::Jsonb => -1,
        }
    }

    /// PG type OID for pg_catalog.pg_type compatibility.
    pub fn pg_type_oid(&self) -> i32 {
        match self {
            DataType::Boolean => 16,
            DataType::Int32 => 23,
            DataType::Int64 => 20,
            DataType::Float64 => 701,
            DataType::Text => 25,
            DataType::Timestamp => 1114,
            DataType::Date => 1082,
            DataType::Array(inner) => match inner.as_ref() {
                DataType::Int32 => 1007,
                DataType::Int64 => 1016,
                DataType::Text => 1009,
                DataType::Float64 => 1022,
                DataType::Boolean => 1000,
                _ => 2277, // anyarray
            },
            DataType::Jsonb => 3802,
        }
    }

    /// PG internal short type name (udt_name) for information_schema.columns.
    pub fn pg_udt_name(&self) -> &'static str {
        match self {
            DataType::Boolean => "bool",
            DataType::Int32 => "int4",
            DataType::Int64 => "int8",
            DataType::Float64 => "float8",
            DataType::Text => "text",
            DataType::Timestamp => "timestamp",
            DataType::Date => "date",
            DataType::Array(inner) => match inner.as_ref() {
                DataType::Int32 => "_int4",
                DataType::Int64 => "_int8",
                DataType::Text => "_text",
                DataType::Float64 => "_float8",
                DataType::Boolean => "_bool",
                _ => "anyarray",
            },
            DataType::Jsonb => "jsonb",
        }
    }

    /// Numeric precision for information_schema.columns (None if not numeric).
    pub fn numeric_precision(&self) -> Option<i32> {
        match self {
            DataType::Int32 => Some(32),
            DataType::Int64 => Some(64),
            DataType::Float64 => Some(53),
            _ => None,
        }
    }

    /// Numeric scale for information_schema.columns (None if not numeric).
    pub fn numeric_scale(&self) -> Option<i32> {
        match self {
            DataType::Int32 | DataType::Int64 => Some(0),
            _ => None,
        }
    }

    /// Datetime precision for information_schema.columns (None if not temporal).
    pub fn datetime_precision(&self) -> Option<i32> {
        match self {
            DataType::Timestamp => Some(6),
            DataType::Date => Some(0),
            _ => None,
        }
    }

    /// PG-compatible type name for information_schema.columns.data_type.
    pub fn pg_type_name(&self) -> &'static str {
        match self {
            DataType::Boolean => "boolean",
            DataType::Int32 => "integer",
            DataType::Int64 => "bigint",
            DataType::Float64 => "double precision",
            DataType::Text => "text",
            DataType::Timestamp => "timestamp without time zone",
            DataType::Date => "date",
            DataType::Array(_) => "ARRAY",
            DataType::Jsonb => "jsonb",
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Int32 => write!(f, "INT"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::Float64 => write!(f, "FLOAT8"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Date => write!(f, "DATE"),
            DataType::Array(inner) => write!(f, "{}[]", inner),
            DataType::Jsonb => write!(f, "JSONB"),
        }
    }
}

/// Transaction kind: single-shard (Local) vs cross-shard (Global).
/// Immutable once determined at transaction begin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TxnType {
    Local,
    Global,
}

/// Actual execution path taken by a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TxnPath {
    Fast,
    Slow,
}

/// Isolation levels supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum IsolationLevel {
    #[default]
    ReadCommitted,
    SnapshotIsolation,
    Serializable,
}

/// Lightweight transaction context for enforcement across layers.
///
/// Derived from `TxnHandle`; carried through executor → cluster → storage.
/// All commit/abort paths MUST carry a TxnContext so invariants can be
/// validated at every layer boundary.
#[derive(Debug, Clone)]
pub struct TxnContext {
    pub txn_id: TxnId,
    pub txn_type: TxnType,
    pub txn_path: TxnPath,
    pub involved_shards: Vec<ShardId>,
    pub start_ts: Timestamp,
}

impl TxnContext {
    /// Build a minimal local-txn context (single shard, fast path).
    pub fn local(txn_id: TxnId, shard: ShardId, start_ts: Timestamp) -> Self {
        Self {
            txn_id,
            txn_type: TxnType::Local,
            txn_path: TxnPath::Fast,
            involved_shards: vec![shard],
            start_ts,
        }
    }

    /// Build a global-txn context (multiple shards, slow path).
    pub fn global(txn_id: TxnId, shards: Vec<ShardId>, start_ts: Timestamp) -> Self {
        Self {
            txn_id,
            txn_type: TxnType::Global,
            txn_path: TxnPath::Slow,
            involved_shards: shards,
            start_ts,
        }
    }

    /// Validate commit-time invariants. Returns Err(description) on violation.
    ///
    /// Invariants:
    /// 1. LocalTxn → involved_shards.len() == 1
    /// 2. LocalTxn must NOT be on slow-path
    /// 3. GlobalTxn must NOT be on fast-path
    /// 4. txn_path must be consistent with txn_type
    pub fn validate_commit_invariants(&self) -> Result<(), String> {
        if self.txn_type == TxnType::Local && self.involved_shards.len() != 1 {
            return Err(format!(
                "LocalTxn {} has {} involved shards (must be exactly 1)",
                self.txn_id,
                self.involved_shards.len()
            ));
        }
        if self.txn_type == TxnType::Local && self.txn_path == TxnPath::Slow {
            return Err(format!(
                "LocalTxn {} is on slow path (invariant: must be fast)",
                self.txn_id
            ));
        }
        if self.txn_type == TxnType::Global && self.txn_path == TxnPath::Fast {
            return Err(format!(
                "GlobalTxn {} is on fast path (invariant: must be slow)",
                self.txn_id
            ));
        }
        Ok(())
    }
}

