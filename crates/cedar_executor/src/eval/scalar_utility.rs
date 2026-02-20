use cedar_common::datum::Datum;
use cedar_common::error::ExecutionError;
use cedar_sql_frontend::types::ScalarFunc;

/// Dispatch a utility-domain scalar function.
pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::PgTypeof => {
            let type_name = match args.first() {
                Some(Datum::Int32(_)) => "integer",
                Some(Datum::Int64(_)) => "bigint",
                Some(Datum::Float64(_)) => "double precision",
                Some(Datum::Text(_)) => "text",
                Some(Datum::Boolean(_)) => "boolean",
                Some(Datum::Timestamp(_)) => "timestamp without time zone",
                Some(Datum::Date(_)) => "date",
                Some(Datum::Array(_)) => "array",
                Some(Datum::Jsonb(_)) => "jsonb",
                Some(Datum::Null) => "unknown",
                None => "unknown",
            };
            Ok(Datum::Text(type_name.to_string()))
        }
        ScalarFunc::ToNumber => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                Some(Datum::Int32(n)) => return Ok(Datum::Float64(*n as f64)),
                Some(Datum::Int64(n)) => return Ok(Datum::Float64(*n as f64)),
                Some(Datum::Float64(_)) => return Ok(args[0].clone()),
                _ => return Err(ExecutionError::TypeError("TO_NUMBER requires text".into())),
            };
            let cleaned: String = s.chars().filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-' || *c == '+').collect();
            cleaned.parse::<f64>()
                .map(Datum::Float64)
                .map_err(|_| ExecutionError::TypeError(format!("Cannot convert '{}' to number", s)))
        }
        ScalarFunc::Random => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            std::time::SystemTime::now().hash(&mut hasher);
            std::thread::current().id().hash(&mut hasher);
            let hash = hasher.finish();
            let val = (hash as f64) / (u64::MAX as f64);
            Ok(Datum::Float64(val))
        }
        ScalarFunc::GenRandomUuid => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            std::time::SystemTime::now().hash(&mut hasher);
            std::thread::current().id().hash(&mut hasher);
            let h1 = hasher.finish();
            let mut hasher2 = DefaultHasher::new();
            h1.hash(&mut hasher2);
            42u64.hash(&mut hasher2);
            let h2 = hasher2.finish();
            let uuid = format!(
                "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
                (h1 >> 32) as u32,
                ((h1 >> 16) as u16),
                h1 as u16 & 0x0FFF,
                (h2 >> 48) as u16 & 0x3FFF | 0x8000,
                h2 & 0xFFFFFFFFFFFF
            );
            Ok(Datum::Text(uuid))
        }
        ScalarFunc::NumNonnulls => {
            let count = args.iter().filter(|d| !d.is_null()).count();
            Ok(Datum::Int64(count as i64))
        }
        ScalarFunc::NumNulls => {
            let count = args.iter().filter(|d| d.is_null()).count();
            Ok(Datum::Int64(count as i64))
        }
        _ => Err(ExecutionError::TypeError(format!("Not a utility function: {:?}", func))),
    }
}
