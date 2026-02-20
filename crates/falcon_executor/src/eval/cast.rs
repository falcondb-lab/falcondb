use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;

pub(crate) fn eval_cast(val: Datum, target: &str) -> Result<Datum, ExecutionError> {
    if val.is_null() {
        return Ok(Datum::Null);
    }
    let target_lower = target.to_lowercase();
    match target_lower.as_str() {
        "smallint" | "int2" => match &val {
            Datum::Int32(_) => Ok(val),
            Datum::Int64(v) => Ok(Datum::Int32(*v as i32)),
            Datum::Float64(v) => Ok(Datum::Int32(*v as i32)),
            Datum::Text(s) => s
                .parse::<i32>()
                .map(Datum::Int32)
                .map_err(|_| ExecutionError::TypeError(format!("Cannot cast '{}' to smallint", s))),
            Datum::Boolean(b) => Ok(Datum::Int32(if *b { 1 } else { 0 })),
            _ => Err(ExecutionError::TypeError(format!("Cannot cast {:?} to smallint", val))),
        },
        "int" | "integer" | "int4" => match &val {
            Datum::Int32(_) => Ok(val),
            Datum::Int64(v) => Ok(Datum::Int32(*v as i32)),
            Datum::Float64(v) => Ok(Datum::Int32(*v as i32)),
            Datum::Text(s) => s
                .parse::<i32>()
                .map(Datum::Int32)
                .map_err(|_| ExecutionError::TypeError(format!("Cannot cast '{}' to int", s))),
            Datum::Boolean(b) => Ok(Datum::Int32(if *b { 1 } else { 0 })),
            _ => Err(ExecutionError::TypeError(format!("Cannot cast {:?} to int", val))),
        },
        "bigint" | "int8" => match &val {
            Datum::Int64(_) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Int64(*v as i64)),
            Datum::Float64(v) => Ok(Datum::Int64(*v as i64)),
            Datum::Text(s) => s
                .parse::<i64>()
                .map(Datum::Int64)
                .map_err(|_| ExecutionError::TypeError(format!("Cannot cast '{}' to bigint", s))),
            _ => Err(ExecutionError::TypeError(format!("Cannot cast {:?} to bigint", val))),
        },
        "float" | "double" | "float8" | "real" | "float4" | "numeric" | "decimal" => {
            match &val {
                Datum::Float64(_) => Ok(val),
                Datum::Int32(v) => Ok(Datum::Float64(*v as f64)),
                Datum::Int64(v) => Ok(Datum::Float64(*v as f64)),
                Datum::Text(s) => s
                    .parse::<f64>()
                    .map(Datum::Float64)
                    .map_err(|_| ExecutionError::TypeError(format!("Cannot cast '{}' to float", s))),
                _ => Err(ExecutionError::TypeError(format!("Cannot cast {:?} to float", val))),
            }
        }
        "text" | "varchar" | "char" => Ok(Datum::Text(format!("{}", val))),
        "boolean" | "bool" => match &val {
            Datum::Boolean(_) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Boolean(*v != 0)),
            Datum::Int64(v) => Ok(Datum::Boolean(*v != 0)),
            Datum::Text(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" => Ok(Datum::Boolean(true)),
                "false" | "f" | "0" | "no" => Ok(Datum::Boolean(false)),
                _ => Err(ExecutionError::TypeError(format!("Cannot cast '{}' to boolean", s))),
            },
            _ => Err(ExecutionError::TypeError(format!("Cannot cast {:?} to boolean", val))),
        },
        "timestamp" | "timestamp without time zone" => match &val {
            Datum::Timestamp(_) => Ok(val),
            Datum::Date(days) => {
                // Convert date (days since epoch) to timestamp (microseconds since epoch) at midnight
                let us = *days as i64 * 86400 * 1_000_000;
                Ok(Datum::Timestamp(us))
            }
            Datum::Int64(us) => Ok(Datum::Timestamp(*us)),
            Datum::Int32(us) => Ok(Datum::Timestamp(*us as i64)),
            Datum::Text(s) => {
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    Ok(Datum::Timestamp(dt.and_utc().timestamp_micros()))
                } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                    Ok(Datum::Timestamp(dt.and_utc().timestamp_micros()))
                } else if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let dt = d.and_hms_opt(0, 0, 0).unwrap();
                    Ok(Datum::Timestamp(dt.and_utc().timestamp_micros()))
                } else {
                    Err(ExecutionError::TypeError(format!("Cannot cast '{}' to timestamp", s)))
                }
            }
            _ => Err(ExecutionError::TypeError(format!("Cannot cast {:?} to timestamp", val))),
        },
        "date" => match &val {
            Datum::Date(_) => Ok(val),
            Datum::Timestamp(us) => {
                // Convert timestamp (microseconds since epoch) to date (days since epoch)
                let days = (*us / (86400 * 1_000_000)) as i32;
                Ok(Datum::Date(days))
            }
            Datum::Text(s) => {
                use chrono::NaiveDate;
                let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .or_else(|_| NaiveDate::parse_from_str(s, "%m/%d/%Y"))
                    .or_else(|_| NaiveDate::parse_from_str(s, "%Y%m%d"))
                    .map_err(|e| ExecutionError::TypeError(format!("Cannot cast '{}' to date: {}", s, e)))?;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = (date - epoch).num_days() as i32;
                Ok(Datum::Date(days))
            }
            Datum::Int32(d) => Ok(Datum::Date(*d)),
            Datum::Int64(d) => Ok(Datum::Date(*d as i32)),
            _ => Err(ExecutionError::TypeError(format!("Cannot cast {:?} to date", val))),
        },
        "jsonb" | "json" => match &val {
            Datum::Jsonb(_) => Ok(val),
            Datum::Text(s) => serde_json::from_str(s)
                .map(Datum::Jsonb)
                .map_err(|e| ExecutionError::TypeError(format!("Cannot cast '{}' to jsonb: {}", s, e))),
            _ => Err(ExecutionError::TypeError(format!("Cannot cast {:?} to jsonb", val))),
        },
        t if t.ends_with("[]") => match val {
            Datum::Array(_) => Ok(val),
            _ => Err(ExecutionError::TypeError(format!("Cannot cast {:?} to {}", val, target))),
        },
        _ => Err(ExecutionError::TypeError(format!("Unknown target type: {}", target))),
    }
}
