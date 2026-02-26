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
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub const MIN: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
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

/// SQL data types supported by FalconDB.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    /// SMALLINT / INT2: 2-byte signed integer.
    Int16,
    Int32,
    Int64,
    /// REAL / FLOAT4: single-precision IEEE 754 float.
    Float32,
    Float64,
    Text,
    Timestamp,
    Date,
    Array(Box<Self>),
    Jsonb,
    /// Fixed-point decimal: Decimal(precision, scale).
    /// precision = total significant digits (max 38), scale = digits after decimal point.
    Decimal(u8, u8),
    /// TIME without time zone: microseconds since midnight (0..86_400_000_000).
    Time,
    /// INTERVAL: months + days + microseconds (PG-compatible triple).
    Interval,
    /// UUID: 128-bit universally unique identifier.
    Uuid,
    /// BYTEA: arbitrary binary data.
    Bytea,
}

impl DataType {
    /// Return the PG OID for this type.
    pub const fn pg_oid(&self) -> i32 {
        match self {
            Self::Boolean => 16,
            Self::Int16 => 21,
            Self::Int32 => 23,
            Self::Int64 => 20,
            Self::Float32 => 700,
            Self::Float64 => 701,
            Self::Text => 25,
            Self::Timestamp => 1114,
            Self::Date => 1082,
            Self::Array(_) => 2277,      // anyarray OID
            Self::Jsonb => 3802,         // jsonb OID
            Self::Decimal(_, _) => 1700, // numeric OID
            Self::Time => 1083,
            Self::Interval => 1186,
            Self::Uuid => 2950,
            Self::Bytea => 17, // bytea OID
        }
    }

    /// Byte size hint (-1 for variable length).
    pub const fn type_len(&self) -> i16 {
        match self {
            Self::Boolean => 1,
            Self::Int16 => 2,
            Self::Int32 | Self::Float32 | Self::Date => 4,
            Self::Int64 | Self::Float64 | Self::Timestamp | Self::Time => 8,
            Self::Interval | Self::Uuid => 16,
            Self::Text | Self::Array(_) | Self::Jsonb
            | Self::Decimal(_, _) | Self::Bytea => -1, // variable length
        }
    }

    /// PG type OID for pg_catalog.pg_type compatibility.
    pub fn pg_type_oid(&self) -> i32 {
        match self {
            Self::Boolean => 16,
            Self::Int16 => 21,
            Self::Int32 => 23,
            Self::Int64 => 20,
            Self::Float32 => 700,
            Self::Float64 => 701,
            Self::Text => 25,
            Self::Timestamp => 1114,
            Self::Date => 1082,
            Self::Array(inner) => match inner.as_ref() {
                Self::Int16 => 1005,
                Self::Int32 => 1007,
                Self::Int64 => 1016,
                Self::Text => 1009,
                Self::Float32 => 1021,
                Self::Float64 => 1022,
                Self::Boolean => 1000,
                _ => 2277, // anyarray
            },
            Self::Jsonb => 3802,
            Self::Decimal(_, _) => 1700, // numeric
            Self::Time => 1083,
            Self::Interval => 1186,
            Self::Uuid => 2950,
            Self::Bytea => 17,
        }
    }

    /// PG internal short type name (udt_name) for information_schema.columns.
    pub fn pg_udt_name(&self) -> &'static str {
        match self {
            Self::Boolean => "bool",
            Self::Int16 => "int2",
            Self::Int32 => "int4",
            Self::Int64 => "int8",
            Self::Float32 => "float4",
            Self::Float64 => "float8",
            Self::Text => "text",
            Self::Timestamp => "timestamp",
            Self::Date => "date",
            Self::Array(inner) => match inner.as_ref() {
                Self::Int16 => "_int2",
                Self::Int32 => "_int4",
                Self::Int64 => "_int8",
                Self::Text => "_text",
                Self::Float32 => "_float4",
                Self::Float64 => "_float8",
                Self::Boolean => "_bool",
                _ => "anyarray",
            },
            Self::Jsonb => "jsonb",
            Self::Decimal(_, _) => "numeric",
            Self::Time => "time",
            Self::Interval => "interval",
            Self::Uuid => "uuid",
            Self::Bytea => "bytea",
        }
    }

    /// Numeric precision for information_schema.columns (None if not numeric).
    pub const fn numeric_precision(&self) -> Option<i32> {
        match self {
            Self::Int16 => Some(16),
            Self::Int32 => Some(32),
            Self::Int64 => Some(64),
            Self::Float32 => Some(24),
            Self::Float64 => Some(53),
            Self::Decimal(p, _) => Some(*p as i32),
            _ => None,
        }
    }

    /// Numeric scale for information_schema.columns (None if not numeric).
    pub const fn numeric_scale(&self) -> Option<i32> {
        match self {
            Self::Int16 | Self::Int32 | Self::Int64 => Some(0),
            Self::Decimal(_, s) => Some(*s as i32),
            _ => None,
        }
    }

    /// Datetime precision for information_schema.columns (None if not temporal).
    pub const fn datetime_precision(&self) -> Option<i32> {
        match self {
            Self::Timestamp | Self::Time | Self::Interval => Some(6),
            Self::Date => Some(0),
            _ => None,
        }
    }

    /// PG-compatible type name for information_schema.columns.data_type.
    pub const fn pg_type_name(&self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int16 => "smallint",
            Self::Int32 => "integer",
            Self::Int64 => "bigint",
            Self::Float32 => "real",
            Self::Float64 => "double precision",
            Self::Text => "text",
            Self::Timestamp => "timestamp without time zone",
            Self::Date => "date",
            Self::Array(_) => "ARRAY",
            Self::Jsonb => "jsonb",
            Self::Decimal(_, _) => "numeric",
            Self::Time => "time without time zone",
            Self::Interval => "interval",
            Self::Uuid => "uuid",
            Self::Bytea => "bytea",
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean => write!(f, "BOOLEAN"),
            Self::Int16 => write!(f, "SMALLINT"),
            Self::Int32 => write!(f, "INT"),
            Self::Int64 => write!(f, "BIGINT"),
            Self::Float32 => write!(f, "REAL"),
            Self::Float64 => write!(f, "FLOAT8"),
            Self::Text => write!(f, "TEXT"),
            Self::Timestamp => write!(f, "TIMESTAMP"),
            Self::Date => write!(f, "DATE"),
            Self::Array(inner) => write!(f, "{inner}[]"),
            Self::Jsonb => write!(f, "JSONB"),
            Self::Decimal(p, s) => write!(f, "DECIMAL({p},{s})"),
            Self::Time => write!(f, "TIME"),
            Self::Interval => write!(f, "INTERVAL"),
            Self::Uuid => write!(f, "UUID"),
            Self::Bytea => write!(f, "BYTEA"),
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
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
    pub const fn global(txn_id: TxnId, shards: Vec<ShardId>, start_ts: Timestamp) -> Self {
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
