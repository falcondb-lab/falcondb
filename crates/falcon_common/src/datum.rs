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
    Array(Vec<Self>), // PostgreSQL-style array
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
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Int32(_) => Some(DataType::Int32),
            Self::Int64(_) => Some(DataType::Int64),
            Self::Float64(_) => Some(DataType::Float64),
            Self::Text(_) => Some(DataType::Text),
            Self::Timestamp(_) => Some(DataType::Timestamp),
            Self::Date(_) => Some(DataType::Date),
            Self::Array(elems) => {
                // Infer element type from the first non-null element.
                let elem_type = elems
                    .iter()
                    .find_map(Self::data_type)
                    .unwrap_or(DataType::Text); // default to Text for empty/all-null arrays
                Some(DataType::Array(Box::new(elem_type)))
            }
            Self::Jsonb(_) => Some(DataType::Jsonb),
            Self::Decimal(_, scale) => Some(DataType::Decimal(38, *scale)),
            Self::Time(_) => Some(DataType::Time),
            Self::Interval(_, _, _) => Some(DataType::Interval),
            Self::Uuid(_) => Some(DataType::Uuid),
            Self::Bytea(_) => Some(DataType::Bytea),
        }
    }

    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Coerce to boolean for WHERE clause evaluation.
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int32(v) => Some(*v as i64),
            Self::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int32(v) => Some(f64::from(*v)),
            Self::Int64(v) => Some(*v as f64),
            Self::Float64(v) => Some(*v),
            Self::Decimal(m, s) => Some(*m as f64 / 10f64.powi(i32::from(*s))),
            _ => None,
        }
    }

    pub const fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Encode to PG text format.
    pub fn to_pg_text(&self) -> Option<String> {
        match self {
            Self::Null => None,
            Self::Boolean(b) => Some(if *b { "t".into() } else { "f".into() }),
            Self::Int32(v) => Some(v.to_string()),
            Self::Int64(v) => Some(v.to_string()),
            Self::Float64(v) => Some(v.to_string()),
            Self::Text(s) => Some(s.clone()),
            Self::Timestamp(us) => {
                let secs = us / 1_000_000;
                let nsecs = ((us % 1_000_000) * 1000) as u32;
                Some(chrono::DateTime::from_timestamp(secs, nsecs).map_or_else(
                    || us.to_string(),
                    |dt| dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                ))
            }
            Self::Date(days) => {
                // SAFETY: 1970-01-01 is always a valid date — unwrap_or fallback is unreachable.
                let epoch =
                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or(chrono::NaiveDate::MIN);
                Some(epoch.checked_add_signed(chrono::Duration::days(i64::from(*days))).map_or_else(
                    || days.to_string(),
                    |d| d.format("%Y-%m-%d").to_string(),
                ))
            }
            Self::Array(elems) => {
                let inner: Vec<String> = elems
                    .iter()
                    .map(|d| match d {
                        Self::Text(s) => format!("\"{s}\""),
                        Self::Null => "NULL".to_owned(),
                        other => format!("{other}"),
                    })
                    .collect();
                Some(format!("{{{}}}", inner.join(",")))
            }
            Self::Jsonb(v) => Some(v.to_string()),
            Self::Decimal(m, s) => Some(decimal_to_string(*m, *s)),
            Self::Time(_) | Self::Interval(_, _, _) | Self::Uuid(_) => Some(format!("{self}")),
            Self::Bytea(bytes) => {
                // PG hex format: \x followed by hex-encoded bytes
                let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
                Some(format!("\\x{hex}"))
            }
        }
    }

    /// Try to add two datums (for SUM aggregation).
    pub fn add(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (Self::Int32(a), Self::Int32(b)) => Some(Self::Int64(i64::from(*a) + i64::from(*b))),
            (Self::Int64(a), Self::Int64(b)) => Some(Self::Int64(a + b)),
            (Self::Int64(a), Self::Int32(b)) => Some(Self::Int64(a + i64::from(*b))),
            (Self::Int32(a), Self::Int64(b)) => Some(Self::Int64(i64::from(*a) + b)),
            (Self::Float64(a), Self::Float64(b)) => Some(Self::Float64(a + b)),
            (Self::Float64(a), Self::Int64(b)) => Some(Self::Float64(a + *b as f64)),
            (Self::Float64(a), Self::Int32(b)) => Some(Self::Float64(a + f64::from(*b))),
            (Self::Decimal(a, sa), Self::Decimal(b, sb)) => Some(decimal_add(*a, *sa, *b, *sb)),
            (Self::Decimal(a, sa), Self::Int64(b)) => Some(decimal_add(
                *a,
                *sa,
                i128::from(*b) * 10i128.pow(u32::from(*sa)),
                *sa,
            )),
            (Self::Int64(a), Self::Decimal(b, sb)) => Some(decimal_add(
                i128::from(*a) * 10i128.pow(u32::from(*sb)),
                *sb,
                *b,
                *sb,
            )),
            _ => None,
        }
    }

    /// Create a Decimal from a string like "123.45" or "-0.001".
    pub fn parse_decimal(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        let (int_part, frac_part) = s.find('.').map_or((s, ""), |dot_pos| {
            (&s[..dot_pos], &s[dot_pos + 1..])
        });
        let scale = frac_part.len() as u8;
        let combined = format!("{int_part}{frac_part}");
        let mantissa: i128 = combined.parse().ok()?;
        Some(Self::Decimal(mantissa, scale))
    }
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Boolean(b) => write!(f, "{b}"),
            Self::Int32(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Text(s) => write!(f, "{s}"),
            Self::Timestamp(us) => write!(f, "{us}"),
            Self::Date(days) => {
                let epoch =
                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or(chrono::NaiveDate::MIN);
                if let Some(d) = epoch.checked_add_signed(chrono::Duration::days(i64::from(*days))) {
                    write!(f, "{}", d.format("%Y-%m-%d"))
                } else {
                    write!(f, "{days}")
                }
            }
            Self::Array(elems) => {
                write!(f, "{{")?;
                for (i, d) in elems.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{d}")?;
                }
                write!(f, "}}")
            }
            Self::Jsonb(v) => write!(f, "{v}"),
            Self::Decimal(m, s) => write!(f, "{}", decimal_to_string(*m, *s)),
            Self::Time(us) => {
                let total_secs = *us / 1_000_000;
                let h = total_secs / 3600;
                let m = (total_secs % 3600) / 60;
                let s = total_secs % 60;
                let frac = *us % 1_000_000;
                if frac == 0 {
                    write!(f, "{h:02}:{m:02}:{s:02}")
                } else {
                    write!(f, "{h:02}:{m:02}:{s:02}.{frac:06}")
                }
            }
            Self::Interval(months, days, us) => {
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
                    parts.push(format!("{sign}{h:02}:{m:02}:{s:02}"));
                }
                write!(f, "{}", parts.join(" "))
            }
            Self::Uuid(v) => {
                let bytes = v.to_be_bytes();
                write!(f, "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                    bytes[8], bytes[9], bytes[10], bytes[11],
                    bytes[12], bytes[13], bytes[14], bytes[15])
            }
            Self::Bytea(bytes) => {
                write!(f, "\\x")?;
                for b in bytes {
                    write!(f, "{b:02x}")?;
                }
                Ok(())
            }
        }
    }
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => false, // NULL != NULL in SQL
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            (Self::Int32(a), Self::Int32(b)) => a == b,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Int32(a), Self::Int64(b)) => i64::from(*a) == *b,
            (Self::Int64(a), Self::Int32(b)) => *a == i64::from(*b),
            (Self::Float64(a), Self::Float64(b)) => a == b,
            (Self::Float64(a), Self::Int32(b)) => *a == f64::from(*b),
            (Self::Float64(a), Self::Int64(b)) => *a == (*b as f64),
            (Self::Int32(a), Self::Float64(b)) => f64::from(*a) == *b,
            (Self::Int64(a), Self::Float64(b)) => (*a as f64) == *b,
            (Self::Text(a), Self::Text(b)) => a == b,
            (Self::Timestamp(a), Self::Timestamp(b)) => a == b,
            (Self::Date(a), Self::Date(b)) => a == b,
            (Self::Array(a), Self::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y)
            }
            (Self::Jsonb(a), Self::Jsonb(b)) => a == b,
            (Self::Decimal(a, sa), Self::Decimal(b, sb)) => {
                if sa == sb {
                    a == b
                } else {
                    let (na, nb) = decimal_normalize(*a, *sa, *b, *sb);
                    na == nb
                }
            }
            (Self::Decimal(a, sa), Self::Int64(b)) => {
                let bm = i128::from(*b) * 10i128.pow(u32::from(*sa));
                *a == bm
            }
            (Self::Int64(a), Self::Decimal(b, sb)) => {
                let am = i128::from(*a) * 10i128.pow(u32::from(*sb));
                am == *b
            }
            (Self::Time(a), Self::Time(b)) => a == b,
            (Self::Interval(am, ad, aus), Self::Interval(bm, bd, bus)) => {
                am == bm && ad == bd && aus == bus
            }
            (Self::Uuid(a), Self::Uuid(b)) => a == b,
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
            Self::Null => {
                0u8.hash(state);
            }
            Self::Boolean(b) => {
                1u8.hash(state);
                b.hash(state);
            }
            // Int32 and Int64 share tag 2, both hash as i64
            Self::Int32(v) => {
                2u8.hash(state);
                i64::from(*v).hash(state);
            }
            Self::Int64(v) => {
                2u8.hash(state);
                v.hash(state);
            }
            Self::Float64(v) => {
                3u8.hash(state);
                v.to_bits().hash(state);
            }
            Self::Text(s) => {
                4u8.hash(state);
                s.hash(state);
            }
            Self::Timestamp(us) => {
                5u8.hash(state);
                us.hash(state);
            }
            Self::Date(days) => {
                8u8.hash(state);
                days.hash(state);
            }
            Self::Array(elems) => {
                6u8.hash(state);
                elems.len().hash(state);
                for e in elems {
                    e.hash(state);
                }
            }
            Self::Jsonb(v) => {
                7u8.hash(state);
                v.to_string().hash(state);
            }
            Self::Decimal(m, s) => {
                9u8.hash(state);
                // Normalize: remove trailing zeros for consistent hashing
                let (nm, ns) = decimal_trim(*m, *s);
                nm.hash(state);
                ns.hash(state);
            }
            Self::Time(us) => {
                10u8.hash(state);
                us.hash(state);
            }
            Self::Interval(months, days, us) => {
                11u8.hash(state);
                months.hash(state);
                days.hash(state);
                us.hash(state);
            }
            Self::Uuid(v) => {
                12u8.hash(state);
                v.hash(state);
            }
            Self::Bytea(bytes) => {
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
            (Self::Null, _) | (_, Self::Null) => None,
            (Self::Boolean(a), Self::Boolean(b)) => a.partial_cmp(b),
            (Self::Int32(a), Self::Int32(b)) => a.partial_cmp(b),
            (Self::Int64(a), Self::Int64(b)) => a.partial_cmp(b),
            (Self::Int32(a), Self::Int64(b)) => i64::from(*a).partial_cmp(b),
            (Self::Int64(a), Self::Int32(b)) => a.partial_cmp(&i64::from(*b)),
            (Self::Float64(a), Self::Float64(b)) => a.partial_cmp(b),
            (Self::Float64(a), Self::Int32(b)) => a.partial_cmp(&f64::from(*b)),
            (Self::Float64(a), Self::Int64(b)) => a.partial_cmp(&(*b as f64)),
            (Self::Int32(a), Self::Float64(b)) => f64::from(*a).partial_cmp(b),
            (Self::Int64(a), Self::Float64(b)) => (*a as f64).partial_cmp(b),
            (Self::Text(a), Self::Text(b)) => a.partial_cmp(b),
            (Self::Timestamp(a), Self::Timestamp(b)) => a.partial_cmp(b),
            (Self::Date(a), Self::Date(b)) => a.partial_cmp(b),
            (Self::Decimal(a, sa), Self::Decimal(b, sb)) => {
                let (na, nb) = decimal_normalize(*a, *sa, *b, *sb);
                na.partial_cmp(&nb)
            }
            (Self::Decimal(a, sa), Self::Int64(b)) => {
                let bm = i128::from(*b) * 10i128.pow(u32::from(*sa));
                a.partial_cmp(&bm)
            }
            (Self::Int64(a), Self::Decimal(b, sb)) => {
                let am = i128::from(*a) * 10i128.pow(u32::from(*sb));
                am.partial_cmp(b)
            }
            (Self::Decimal(a, sa), Self::Float64(b)) => {
                let af = *a as f64 / 10f64.powi(i32::from(*sa));
                af.partial_cmp(b)
            }
            (Self::Float64(a), Self::Decimal(b, sb)) => {
                let bf = *b as f64 / 10f64.powi(i32::from(*sb));
                a.partial_cmp(&bf)
            }
            (Self::Time(a), Self::Time(b)) => a.partial_cmp(b),
            (Self::Interval(am, ad, aus), Self::Interval(bm, bd, bus)) => {
                // Compare by total: months first, then days, then microseconds
                match am.cmp(bm) {
                    Ordering::Equal => match ad.cmp(bd) {
                        Ordering::Equal => aus.partial_cmp(bus),
                        o => Some(o),
                    },
                    o => Some(o),
                }
            }
            (Self::Uuid(a), Self::Uuid(b)) => a.partial_cmp(b),
            (Self::Array(_), Self::Array(_)) => None, // arrays not orderable
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedRow {
    pub values: Vec<Datum>,
}

impl OwnedRow {
    pub const fn new(values: Vec<Datum>) -> Self {
        Self { values }
    }

    pub fn get(&self, idx: usize) -> Option<&Datum> {
        self.values.get(idx)
    }

    pub const fn len(&self) -> usize {
        self.values.len()
    }

    pub const fn is_empty(&self) -> bool {
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
            write!(f, "{v}")?;
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
        format!("{int_part}.{frac_part}")
    };
    if negative {
        format!("-{result}")
    } else {
        result
    }
}

/// Normalize two decimals to the same scale, returning (a_normalized, b_normalized).
const fn decimal_normalize(a: i128, sa: u8, b: i128, sb: u8) -> (i128, i128) {
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
const fn decimal_trim(mut mantissa: i128, mut scale: u8) -> (i128, u8) {
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
pub const fn decimal_mul(a: i128, sa: u8, b: i128, sb: u8) -> Datum {
    Datum::Decimal(a * b, sa + sb)
}

/// Divide two decimals with a target result scale.
pub fn decimal_div(a: i128, sa: u8, b: i128, sb: u8, result_scale: u8) -> Option<Datum> {
    if b == 0 {
        return None;
    }
    // Scale up numerator to get desired precision
    let target_scale = result_scale.max(sa).max(sb);
    let extra = u32::from(target_scale) + u32::from(sb) - u32::from(sa);
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
