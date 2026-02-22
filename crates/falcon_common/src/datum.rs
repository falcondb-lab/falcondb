use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::types::DataType;
use serde_json::Value as JsonValue;

/// A single scalar value. This is the fundamental unit of data in FalconDB.
/// Designed for in-memory efficiency: small enum, no heap alloc for fixed-size types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Datum {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Text(String),
    Timestamp(i64),    // microseconds since Unix epoch
    Date(i32),         // days since Unix epoch (1970-01-01)
    Array(Vec<Datum>), // PostgreSQL-style array
    Jsonb(JsonValue),  // JSONB stored as serde_json::Value
    /// Fixed-point decimal for financial precision: mantissa × 10^(-scale).
    /// e.g. Decimal(12345, 2) = 123.45
    /// Supports up to 38 significant digits (i128 range).
    Decimal(i128, u8),
    /// TIME without time zone: microseconds since midnight (0..86_400_000_000).
    Time(i64),
    /// INTERVAL: (months, days, microseconds) — PG-compatible triple.
    Interval(i32, i32, i64),
    /// UUID: stored as 128-bit value.
    Uuid(u128),
    /// BYTEA: arbitrary binary data.
    Bytea(Vec<u8>),
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
                let elem_type = elems
                    .iter()
                    .find_map(|d| d.data_type())
                    .unwrap_or(DataType::Text); // default to Text for empty/all-null arrays
                Some(DataType::Array(Box::new(elem_type)))
            }
            Datum::Jsonb(_) => Some(DataType::Jsonb),
            Datum::Decimal(_, scale) => Some(DataType::Decimal(38, *scale)),
            Datum::Time(_) => Some(DataType::Time),
            Datum::Interval(_, _, _) => Some(DataType::Interval),
            Datum::Uuid(_) => Some(DataType::Uuid),
            Datum::Bytea(_) => Some(DataType::Bytea),
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
            Datum::Decimal(m, s) => Some(*m as f64 / 10f64.powi(*s as i32)),
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
                // SAFETY: 1970-01-01 is always a valid date — unwrap_or fallback is unreachable.
                let epoch =
                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or(chrono::NaiveDate::MIN);
                if let Some(d) = epoch.checked_add_signed(chrono::Duration::days(*days as i64)) {
                    Some(d.format("%Y-%m-%d").to_string())
                } else {
                    Some(days.to_string())
                }
            }
            Datum::Array(elems) => {
                let inner: Vec<String> = elems
                    .iter()
                    .map(|d| match d {
                        Datum::Text(s) => format!("\"{}\"", s),
                        Datum::Null => "NULL".to_string(),
                        other => format!("{}", other),
                    })
                    .collect();
                Some(format!("{{{}}}", inner.join(",")))
            }
            Datum::Jsonb(v) => Some(v.to_string()),
            Datum::Decimal(m, s) => Some(decimal_to_string(*m, *s)),
            Datum::Time(_) | Datum::Interval(_, _, _) | Datum::Uuid(_) => Some(format!("{}", self)),
            Datum::Bytea(bytes) => {
                // PG hex format: \x followed by hex-encoded bytes
                let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
                Some(format!("\\x{}", hex))
            }
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
            (Datum::Decimal(a, sa), Datum::Decimal(b, sb)) => Some(decimal_add(*a, *sa, *b, *sb)),
            (Datum::Decimal(a, sa), Datum::Int64(b)) => Some(decimal_add(
                *a,
                *sa,
                *b as i128 * 10i128.pow(*sa as u32),
                *sa,
            )),
            (Datum::Int64(a), Datum::Decimal(b, sb)) => Some(decimal_add(
                *a as i128 * 10i128.pow(*sb as u32),
                *sb,
                *b,
                *sb,
            )),
            _ => None,
        }
    }

    /// Create a Decimal from a string like "123.45" or "-0.001".
    pub fn parse_decimal(s: &str) -> Option<Datum> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        let (int_part, frac_part) = if let Some(dot_pos) = s.find('.') {
            (&s[..dot_pos], &s[dot_pos + 1..])
        } else {
            (s, "")
        };
        let scale = frac_part.len() as u8;
        let combined = format!("{}{}", int_part, frac_part);
        let mantissa: i128 = combined.parse().ok()?;
        Some(Datum::Decimal(mantissa, scale))
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
                let epoch =
                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or(chrono::NaiveDate::MIN);
                if let Some(d) = epoch.checked_add_signed(chrono::Duration::days(*days as i64)) {
                    write!(f, "{}", d.format("%Y-%m-%d"))
                } else {
                    write!(f, "{}", days)
                }
            }
            Datum::Array(elems) => {
                write!(f, "{{")?;
                for (i, d) in elems.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{}", d)?;
                }
                write!(f, "}}")
            }
            Datum::Jsonb(v) => write!(f, "{}", v),
            Datum::Decimal(m, s) => write!(f, "{}", decimal_to_string(*m, *s)),
            Datum::Time(us) => {
                let total_secs = *us / 1_000_000;
                let h = total_secs / 3600;
                let m = (total_secs % 3600) / 60;
                let s = total_secs % 60;
                let frac = *us % 1_000_000;
                if frac == 0 {
                    write!(f, "{:02}:{:02}:{:02}", h, m, s)
                } else {
                    write!(f, "{:02}:{:02}:{:02}.{:06}", h, m, s, frac)
                }
            }
            Datum::Interval(months, days, us) => {
                let mut parts = Vec::new();
                if *months != 0 {
                    let y = *months / 12;
                    let mo = *months % 12;
                    if y != 0 {
                        parts.push(format!("{} year{}", y, if y.abs() != 1 { "s" } else { "" }));
                    }
                    if mo != 0 {
                        parts.push(format!(
                            "{} mon{}",
                            mo,
                            if mo.abs() != 1 { "s" } else { "" }
                        ));
                    }
                }
                if *days != 0 {
                    parts.push(format!(
                        "{} day{}",
                        days,
                        if days.abs() != 1 { "s" } else { "" }
                    ));
                }
                if *us != 0 || parts.is_empty() {
                    let total_secs = us.abs() / 1_000_000;
                    let h = total_secs / 3600;
                    let m = (total_secs % 3600) / 60;
                    let s = total_secs % 60;
                    let sign = if *us < 0 { "-" } else { "" };
                    parts.push(format!("{}{:02}:{:02}:{:02}", sign, h, m, s));
                }
                write!(f, "{}", parts.join(" "))
            }
            Datum::Uuid(v) => {
                let bytes = v.to_be_bytes();
                write!(f, "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                    bytes[8], bytes[9], bytes[10], bytes[11],
                    bytes[12], bytes[13], bytes[14], bytes[15])
            }
            Datum::Bytea(bytes) => {
                write!(f, "\\x")?;
                for b in bytes {
                    write!(f, "{:02x}", b)?;
                }
                Ok(())
            }
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
            (Datum::Array(a), Datum::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y)
            }
            (Datum::Jsonb(a), Datum::Jsonb(b)) => a == b,
            (Datum::Decimal(a, sa), Datum::Decimal(b, sb)) => {
                if sa == sb {
                    a == b
                } else {
                    let (na, nb) = decimal_normalize(*a, *sa, *b, *sb);
                    na == nb
                }
            }
            (Datum::Decimal(a, sa), Datum::Int64(b)) => {
                let bm = *b as i128 * 10i128.pow(*sa as u32);
                *a == bm
            }
            (Datum::Int64(a), Datum::Decimal(b, sb)) => {
                let am = *a as i128 * 10i128.pow(*sb as u32);
                am == *b
            }
            (Datum::Time(a), Datum::Time(b)) => a == b,
            (Datum::Interval(am, ad, aus), Datum::Interval(bm, bd, bus)) => {
                am == bm && ad == bd && aus == bus
            }
            (Datum::Uuid(a), Datum::Uuid(b)) => a == b,
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
            Datum::Null => {
                0u8.hash(state);
            }
            Datum::Boolean(b) => {
                1u8.hash(state);
                b.hash(state);
            }
            // Int32 and Int64 share tag 2, both hash as i64
            Datum::Int32(v) => {
                2u8.hash(state);
                (*v as i64).hash(state);
            }
            Datum::Int64(v) => {
                2u8.hash(state);
                v.hash(state);
            }
            Datum::Float64(v) => {
                3u8.hash(state);
                v.to_bits().hash(state);
            }
            Datum::Text(s) => {
                4u8.hash(state);
                s.hash(state);
            }
            Datum::Timestamp(us) => {
                5u8.hash(state);
                us.hash(state);
            }
            Datum::Date(days) => {
                8u8.hash(state);
                days.hash(state);
            }
            Datum::Array(elems) => {
                6u8.hash(state);
                elems.len().hash(state);
                for e in elems {
                    e.hash(state);
                }
            }
            Datum::Jsonb(v) => {
                7u8.hash(state);
                v.to_string().hash(state);
            }
            Datum::Decimal(m, s) => {
                9u8.hash(state);
                // Normalize: remove trailing zeros for consistent hashing
                let (nm, ns) = decimal_trim(*m, *s);
                nm.hash(state);
                ns.hash(state);
            }
            Datum::Time(us) => {
                10u8.hash(state);
                us.hash(state);
            }
            Datum::Interval(months, days, us) => {
                11u8.hash(state);
                months.hash(state);
                days.hash(state);
                us.hash(state);
            }
            Datum::Uuid(v) => {
                12u8.hash(state);
                v.hash(state);
            }
            Datum::Bytea(bytes) => {
                13u8.hash(state);
                bytes.hash(state);
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
            (Datum::Decimal(a, sa), Datum::Decimal(b, sb)) => {
                let (na, nb) = decimal_normalize(*a, *sa, *b, *sb);
                na.partial_cmp(&nb)
            }
            (Datum::Decimal(a, sa), Datum::Int64(b)) => {
                let bm = *b as i128 * 10i128.pow(*sa as u32);
                a.partial_cmp(&bm)
            }
            (Datum::Int64(a), Datum::Decimal(b, sb)) => {
                let am = *a as i128 * 10i128.pow(*sb as u32);
                am.partial_cmp(b)
            }
            (Datum::Decimal(a, sa), Datum::Float64(b)) => {
                let af = *a as f64 / 10f64.powi(*sa as i32);
                af.partial_cmp(b)
            }
            (Datum::Float64(a), Datum::Decimal(b, sb)) => {
                let bf = *b as f64 / 10f64.powi(*sb as i32);
                a.partial_cmp(&bf)
            }
            (Datum::Time(a), Datum::Time(b)) => a.partial_cmp(b),
            (Datum::Interval(am, ad, aus), Datum::Interval(bm, bd, bus)) => {
                // Compare by total: months first, then days, then microseconds
                match am.cmp(bm) {
                    Ordering::Equal => match ad.cmp(bd) {
                        Ordering::Equal => aus.partial_cmp(bus),
                        o => Some(o),
                    },
                    o => Some(o),
                }
            }
            (Datum::Uuid(a), Datum::Uuid(b)) => a.partial_cmp(b),
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

// ── Decimal helper functions ────────────────────────────────────────────

/// Convert a (mantissa, scale) decimal to its string representation.
/// e.g. (12345, 2) → "123.45", (-1, 3) → "-0.001", (100, 0) → "100"
pub fn decimal_to_string(mantissa: i128, scale: u8) -> String {
    if scale == 0 {
        return mantissa.to_string();
    }
    let negative = mantissa < 0;
    let abs = mantissa.unsigned_abs();
    let s = abs.to_string();
    let scale = scale as usize;
    let result = if s.len() <= scale {
        // Need leading zeros: e.g. 1 with scale 3 → "0.001"
        let zeros = scale - s.len();
        format!("0.{}{}", "0".repeat(zeros), s)
    } else {
        let (int_part, frac_part) = s.split_at(s.len() - scale);
        format!("{}.{}", int_part, frac_part)
    };
    if negative {
        format!("-{}", result)
    } else {
        result
    }
}

/// Normalize two decimals to the same scale, returning (a_normalized, b_normalized).
fn decimal_normalize(a: i128, sa: u8, b: i128, sb: u8) -> (i128, i128) {
    if sa == sb {
        (a, b)
    } else if sa > sb {
        let diff = (sa - sb) as u32;
        (a, b * 10i128.pow(diff))
    } else {
        let diff = (sb - sa) as u32;
        (a * 10i128.pow(diff), b)
    }
}

/// Add two decimals, returning a Datum::Decimal with the larger scale.
fn decimal_add(a: i128, sa: u8, b: i128, sb: u8) -> Datum {
    let max_scale = sa.max(sb);
    let (na, nb) = decimal_normalize(a, sa, b, sb);
    Datum::Decimal(na + nb, max_scale)
}

/// Remove trailing zeros from a decimal for canonical form.
fn decimal_trim(mut mantissa: i128, mut scale: u8) -> (i128, u8) {
    if mantissa == 0 {
        return (0, 0);
    }
    while scale > 0 && mantissa % 10 == 0 {
        mantissa /= 10;
        scale -= 1;
    }
    (mantissa, scale)
}

/// Subtract two decimals.
pub fn decimal_sub(a: i128, sa: u8, b: i128, sb: u8) -> Datum {
    let max_scale = sa.max(sb);
    let (na, nb) = decimal_normalize(a, sa, b, sb);
    Datum::Decimal(na - nb, max_scale)
}

/// Multiply two decimals.
pub fn decimal_mul(a: i128, sa: u8, b: i128, sb: u8) -> Datum {
    Datum::Decimal(a * b, sa + sb)
}

/// Divide two decimals with a target result scale.
pub fn decimal_div(a: i128, sa: u8, b: i128, sb: u8, result_scale: u8) -> Option<Datum> {
    if b == 0 {
        return None;
    }
    // Scale up numerator to get desired precision
    let target_scale = result_scale.max(sa).max(sb);
    let extra = (target_scale as u32) + (sb as u32) - (sa as u32);
    let scaled_a = a * 10i128.pow(extra);
    Some(Datum::Decimal(scaled_a / b, target_scale))
}

#[cfg(test)]
mod decimal_tests {
    use super::*;

    #[test]
    fn test_decimal_to_string() {
        assert_eq!(decimal_to_string(12345, 2), "123.45");
        assert_eq!(decimal_to_string(-12345, 2), "-123.45");
        assert_eq!(decimal_to_string(1, 3), "0.001");
        assert_eq!(decimal_to_string(-1, 3), "-0.001");
        assert_eq!(decimal_to_string(100, 0), "100");
        assert_eq!(decimal_to_string(0, 2), "0.00");
    }

    #[test]
    fn test_decimal_parse() {
        assert_eq!(
            Datum::parse_decimal("123.45"),
            Some(Datum::Decimal(12345, 2))
        );
        assert_eq!(Datum::parse_decimal("-0.001"), Some(Datum::Decimal(-1, 3)));
        assert_eq!(Datum::parse_decimal("100"), Some(Datum::Decimal(100, 0)));
        assert_eq!(Datum::parse_decimal(""), None);
    }

    #[test]
    fn test_decimal_add() {
        let a = Datum::Decimal(12345, 2); // 123.45
        let b = Datum::Decimal(6789, 2); // 67.89
        assert_eq!(a.add(&b), Some(Datum::Decimal(19134, 2))); // 191.34
    }

    #[test]
    fn test_decimal_add_different_scales() {
        let a = Datum::Decimal(100, 1); // 10.0
        let b = Datum::Decimal(5, 2); // 0.05
        assert_eq!(a.add(&b), Some(Datum::Decimal(1005, 2))); // 10.05
    }

    #[test]
    fn test_decimal_add_int() {
        let a = Datum::Decimal(12345, 2); // 123.45
        let b = Datum::Int64(10);
        assert_eq!(a.add(&b), Some(Datum::Decimal(13345, 2))); // 133.45
    }

    #[test]
    fn test_decimal_eq() {
        assert_eq!(Datum::Decimal(100, 1), Datum::Decimal(1000, 2)); // 10.0 == 10.00
        assert_eq!(Datum::Decimal(1000, 2), Datum::Int64(10)); // 10.00 == 10
    }

    #[test]
    fn test_decimal_ord() {
        assert!(Datum::Decimal(12345, 2) > Datum::Decimal(12344, 2));
        assert!(Datum::Decimal(100, 1) > Datum::Int64(9));
        assert!(Datum::Decimal(100, 1) < Datum::Int64(11));
    }

    #[test]
    fn test_decimal_display() {
        assert_eq!(format!("{}", Datum::Decimal(12345, 2)), "123.45");
        assert_eq!(format!("{}", Datum::Decimal(-1, 3)), "-0.001");
    }

    #[test]
    fn test_decimal_pg_text() {
        assert_eq!(
            Datum::Decimal(12345, 2).to_pg_text(),
            Some("123.45".to_string())
        );
    }

    #[test]
    fn test_decimal_sub_mul_div() {
        assert_eq!(decimal_sub(12345, 2, 6789, 2), Datum::Decimal(5556, 2)); // 55.56
        assert_eq!(decimal_mul(100, 2, 200, 2), Datum::Decimal(20000, 4)); // 0.2000
        assert_eq!(
            decimal_div(100, 2, 3, 0, 6),
            Some(Datum::Decimal(333333, 6))
        );
        assert_eq!(decimal_div(100, 2, 0, 0, 6), None); // div by zero
    }

    #[test]
    fn test_decimal_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        fn hash_datum(d: &Datum) -> u64 {
            let mut h = DefaultHasher::new();
            d.hash(&mut h);
            h.finish()
        }
        // 10.0 and 10.00 should hash the same (after normalization)
        assert_eq!(
            hash_datum(&Datum::Decimal(100, 1)),
            hash_datum(&Datum::Decimal(1000, 2))
        );
    }
}
