use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::types::DataType;
use serde_json::Value as JsonValue;

/// A single scalar value. This is the fundamental unit of data in CedarDB.
/// Designed for in-memory efficiency: small enum, no heap alloc for fixed-size types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Datum {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Text(String),
    Timestamp(i64), // microseconds since Unix epoch
    Date(i32),        // days since Unix epoch (1970-01-01)
    Array(Vec<Datum>), // PostgreSQL-style array
    Jsonb(JsonValue), // JSONB stored as serde_json::Value
}

impl Datum {
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Datum::Null => None,
            Datum::Boolean(_) => Some(DataType::Boolean),
            Datum::Int32(_) => Some(DataType::Int32),
            Datum::Int64(_) => Some(DataType::Int64),
            Datum::Float64(_) => Some(DataType::Float64),
            Datum::Text(_) => Some(DataType::Text),
            Datum::Timestamp(_) => Some(DataType::Timestamp),
            Datum::Date(_) => Some(DataType::Date),
            Datum::Array(elems) => {
                // Infer element type from the first non-null element.
                let elem_type = elems.iter()
                    .find_map(|d| d.data_type())
                    .unwrap_or(DataType::Text); // default to Text for empty/all-null arrays
                Some(DataType::Array(Box::new(elem_type)))
            }
            Datum::Jsonb(_) => Some(DataType::Jsonb)
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    /// Coerce to boolean for WHERE clause evaluation.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Datum::Boolean(b) => Some(*b),
            Datum::Null => None,
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Datum::Int32(v) => Some(*v as i64),
            Datum::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Datum::Int32(v) => Some(*v as f64),
            Datum::Int64(v) => Some(*v as f64),
            Datum::Float64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Datum::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Encode to PG text format.
    pub fn to_pg_text(&self) -> Option<String> {
        match self {
            Datum::Null => None,
            Datum::Boolean(b) => Some(if *b { "t".into() } else { "f".into() }),
            Datum::Int32(v) => Some(v.to_string()),
            Datum::Int64(v) => Some(v.to_string()),
            Datum::Float64(v) => Some(v.to_string()),
            Datum::Text(s) => Some(s.clone()),
            Datum::Timestamp(us) => {
                let secs = us / 1_000_000;
                let nsecs = ((us % 1_000_000) * 1000) as u32;
                if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                    Some(dt.format("%Y-%m-%d %H:%M:%S").to_string())
                } else {
                    Some(us.to_string())
                }
            }
            Datum::Date(days) => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                if let Some(d) = epoch.checked_add_signed(chrono::Duration::days(*days as i64)) {
                    Some(d.format("%Y-%m-%d").to_string())
                } else {
                    Some(days.to_string())
                }
            }
            Datum::Array(elems) => {
                let inner: Vec<String> = elems.iter().map(|d| match d {
                    Datum::Text(s) => format!("\"{}\"" , s),
                    Datum::Null => "NULL".to_string(),
                    other => format!("{}", other),
                }).collect();
                Some(format!("{{{}}}", inner.join(",")))
            }
            Datum::Jsonb(v) => Some(v.to_string()),
        }
    }

    /// Try to add two datums (for SUM aggregation).
    pub fn add(&self, other: &Datum) -> Option<Datum> {
        match (self, other) {
            (Datum::Int32(a), Datum::Int32(b)) => Some(Datum::Int64(*a as i64 + *b as i64)),
            (Datum::Int64(a), Datum::Int64(b)) => Some(Datum::Int64(a + b)),
            (Datum::Int64(a), Datum::Int32(b)) => Some(Datum::Int64(a + *b as i64)),
            (Datum::Int32(a), Datum::Int64(b)) => Some(Datum::Int64(*a as i64 + b)),
            (Datum::Float64(a), Datum::Float64(b)) => Some(Datum::Float64(a + b)),
            (Datum::Float64(a), Datum::Int64(b)) => Some(Datum::Float64(a + *b as f64)),
            (Datum::Float64(a), Datum::Int32(b)) => Some(Datum::Float64(a + *b as f64)),
            _ => None,
        }
    }
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Datum::Null => write!(f, "NULL"),
            Datum::Boolean(b) => write!(f, "{}", b),
            Datum::Int32(v) => write!(f, "{}", v),
            Datum::Int64(v) => write!(f, "{}", v),
            Datum::Float64(v) => write!(f, "{}", v),
            Datum::Text(s) => write!(f, "{}", s),
            Datum::Timestamp(us) => write!(f, "{}", us),
            Datum::Date(days) => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                if let Some(d) = epoch.checked_add_signed(chrono::Duration::days(*days as i64)) {
                    write!(f, "{}", d.format("%Y-%m-%d"))
                } else {
                    write!(f, "{}", days)
                }
            }
            Datum::Array(elems) => {
                write!(f, "{{")?;
                for (i, d) in elems.iter().enumerate() {
                    if i > 0 { write!(f, ",")?; }
                    write!(f, "{}", d)?;
                }
                write!(f, "}}")
            }
            Datum::Jsonb(v) => write!(f, "{}", v),
        }
    }
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Datum::Null, Datum::Null) => false, // NULL != NULL in SQL
            (Datum::Boolean(a), Datum::Boolean(b)) => a == b,
            (Datum::Int32(a), Datum::Int32(b)) => a == b,
            (Datum::Int64(a), Datum::Int64(b)) => a == b,
            (Datum::Int32(a), Datum::Int64(b)) => (*a as i64) == *b,
            (Datum::Int64(a), Datum::Int32(b)) => *a == (*b as i64),
            (Datum::Float64(a), Datum::Float64(b)) => a == b,
            (Datum::Float64(a), Datum::Int32(b)) => *a == (*b as f64),
            (Datum::Float64(a), Datum::Int64(b)) => *a == (*b as f64),
            (Datum::Int32(a), Datum::Float64(b)) => (*a as f64) == *b,
            (Datum::Int64(a), Datum::Float64(b)) => (*a as f64) == *b,
            (Datum::Text(a), Datum::Text(b)) => a == b,
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a == b,
            (Datum::Date(a), Datum::Date(b)) => a == b,
            (Datum::Array(a), Datum::Array(b)) => a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y),
            (Datum::Jsonb(a), Datum::Jsonb(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Datum {}

impl Hash for Datum {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use explicit type tags (NOT mem::discriminant) to ensure cross-type
        // equality consistency: Int32(x) == Int64(x) must produce the same hash.
        match self {
            Datum::Null => { 0u8.hash(state); }
            Datum::Boolean(b) => { 1u8.hash(state); b.hash(state); }
            // Int32 and Int64 share tag 2, both hash as i64
            Datum::Int32(v) => { 2u8.hash(state); (*v as i64).hash(state); }
            Datum::Int64(v) => { 2u8.hash(state); v.hash(state); }
            Datum::Float64(v) => { 3u8.hash(state); v.to_bits().hash(state); }
            Datum::Text(s) => { 4u8.hash(state); s.hash(state); }
            Datum::Timestamp(us) => { 5u8.hash(state); us.hash(state); }
            Datum::Date(days) => { 8u8.hash(state); days.hash(state); }
            Datum::Array(elems) => {
                6u8.hash(state);
                elems.len().hash(state);
                for e in elems { e.hash(state); }
            }
            Datum::Jsonb(v) => {
                7u8.hash(state);
                v.to_string().hash(state);
            }
        }
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Datum::Null, _) | (_, Datum::Null) => None,
            (Datum::Boolean(a), Datum::Boolean(b)) => a.partial_cmp(b),
            (Datum::Int32(a), Datum::Int32(b)) => a.partial_cmp(b),
            (Datum::Int64(a), Datum::Int64(b)) => a.partial_cmp(b),
            (Datum::Int32(a), Datum::Int64(b)) => (*a as i64).partial_cmp(b),
            (Datum::Int64(a), Datum::Int32(b)) => a.partial_cmp(&(*b as i64)),
            (Datum::Float64(a), Datum::Float64(b)) => a.partial_cmp(b),
            (Datum::Float64(a), Datum::Int32(b)) => a.partial_cmp(&(*b as f64)),
            (Datum::Float64(a), Datum::Int64(b)) => a.partial_cmp(&(*b as f64)),
            (Datum::Int32(a), Datum::Float64(b)) => (*a as f64).partial_cmp(b),
            (Datum::Int64(a), Datum::Float64(b)) => (*a as f64).partial_cmp(b),
            (Datum::Text(a), Datum::Text(b)) => a.partial_cmp(b),
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a.partial_cmp(b),
            (Datum::Date(a), Datum::Date(b)) => a.partial_cmp(b),
            (Datum::Array(_), Datum::Array(_)) => None, // arrays not orderable
            _ => None,
        }
    }
}

impl Ord for Datum {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

/// A row is an ordered list of datums.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OwnedRow {
    pub values: Vec<Datum>,
}

impl OwnedRow {
    pub fn new(values: Vec<Datum>) -> Self {
        Self { values }
    }

    pub fn get(&self, idx: usize) -> Option<&Datum> {
        self.values.get(idx)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl fmt::Display for OwnedRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, v) in self.values.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", v)?;
        }
        write!(f, ")")
    }
}
