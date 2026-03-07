use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;

/// Safely convert f64 to i32, rejecting NaN, infinities, and out-of-range values.
fn float64_to_i32(v: f64) -> Result<i32, ExecutionError> {
    if v.is_nan() || v.is_infinite() || v < (i32::MIN as f64) || v > (i32::MAX as f64) {
        Err(ExecutionError::NumericOverflow)
    } else {
        Ok(v as i32)
    }
}

/// Safely convert f64 to i64, rejecting NaN, infinities, and out-of-range values.
fn float64_to_i64(v: f64) -> Result<i64, ExecutionError> {
    // i64::MAX as f64 rounds up to 2^63 — use >= instead of > on upper bound
    if v.is_nan() || v.is_infinite() || v < (i64::MIN as f64) || v >= (i64::MAX as f64) {
        Err(ExecutionError::NumericOverflow)
    } else {
        Ok(v as i64)
    }
}

pub fn eval_cast(val: Datum, target: &str) -> Result<Datum, ExecutionError> {
    if val.is_null() {
        return Ok(Datum::Null);
    }
    let target_lower = target.to_lowercase();
    match target_lower.as_str() {
        "smallint" | "int2" => match &val {
            Datum::Int32(_) => Ok(val),
            Datum::Int64(v) => i32::try_from(*v)
                .map(Datum::Int32)
                .map_err(|_| ExecutionError::NumericOverflow),
            Datum::Float64(v) => float64_to_i32(*v).map(Datum::Int32),
            Datum::Text(s) => s
                .parse::<i32>()
                .map(Datum::Int32)
                .map_err(|_| ExecutionError::TypeError(format!("Cannot cast '{s}' to smallint"))),
            Datum::Boolean(b) => Ok(Datum::Int32(if *b { 1 } else { 0 })),
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to smallint"
            ))),
        },
        "int" | "integer" | "int4" => match &val {
            Datum::Int32(_) => Ok(val),
            Datum::Int64(v) => i32::try_from(*v)
                .map(Datum::Int32)
                .map_err(|_| ExecutionError::NumericOverflow),
            Datum::Float64(v) => float64_to_i32(*v).map(Datum::Int32),
            Datum::Text(s) => s
                .parse::<i32>()
                .map(Datum::Int32)
                .map_err(|_| ExecutionError::TypeError(format!("Cannot cast '{s}' to int"))),
            Datum::Boolean(b) => Ok(Datum::Int32(if *b { 1 } else { 0 })),
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to int"
            ))),
        },
        "bigint" | "int8" => match &val {
            Datum::Int64(_) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Int64(i64::from(*v))),
            Datum::Float64(v) => float64_to_i64(*v).map(Datum::Int64),
            Datum::Text(s) => s
                .parse::<i64>()
                .map(Datum::Int64)
                .map_err(|_| ExecutionError::TypeError(format!("Cannot cast '{s}' to bigint"))),
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to bigint"
            ))),
        },
        "float" | "double" | "float8" | "real" | "float4" => match &val {
            Datum::Float64(_) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Float64(f64::from(*v))),
            Datum::Int64(v) => Ok(Datum::Float64(*v as f64)),
            Datum::Decimal(m, s) => Ok(Datum::Float64(*m as f64 / 10f64.powi(i32::from(*s)))),
            Datum::Text(s) => s
                .parse::<f64>()
                .map(Datum::Float64)
                .map_err(|_| ExecutionError::TypeError(format!("Cannot cast '{s}' to float"))),
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to float"
            ))),
        },
        "numeric" | "decimal" => match &val {
            Datum::Decimal(_, _) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Decimal(i128::from(*v), 0)),
            Datum::Int64(v) => Ok(Datum::Decimal(i128::from(*v), 0)),
            Datum::Float64(v) => {
                // Convert float to decimal with reasonable scale
                let s = format!("{v}");
                Datum::parse_decimal(&s)
                    .ok_or_else(|| ExecutionError::TypeError(format!("Cannot cast {v} to numeric")))
            }
            Datum::Text(s) => Datum::parse_decimal(s)
                .ok_or_else(|| ExecutionError::TypeError(format!("Cannot cast '{s}' to numeric"))),
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to numeric"
            ))),
        },
        "text" | "varchar" | "char" => Ok(Datum::Text(format!("{val}"))),
        "boolean" | "bool" => match &val {
            Datum::Boolean(_) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Boolean(*v != 0)),
            Datum::Int64(v) => Ok(Datum::Boolean(*v != 0)),
            Datum::Text(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" => Ok(Datum::Boolean(true)),
                "false" | "f" | "0" | "no" => Ok(Datum::Boolean(false)),
                _ => Err(ExecutionError::TypeError(format!(
                    "Cannot cast '{s}' to boolean"
                ))),
            },
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to boolean"
            ))),
        },
        "timestamp" | "timestamp without time zone" => match &val {
            Datum::Timestamp(_) => Ok(val),
            Datum::Date(days) => {
                // Convert date (days since epoch) to timestamp (microseconds since epoch) at midnight
                let us = i64::from(*days)
                    .checked_mul(86_400_000_000)
                    .ok_or(ExecutionError::NumericOverflow)?;
                Ok(Datum::Timestamp(us))
            }
            Datum::Int64(us) => Ok(Datum::Timestamp(*us)),
            Datum::Int32(us) => Ok(Datum::Timestamp(i64::from(*us))),
            Datum::Text(s) => {
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    Ok(Datum::Timestamp(dt.and_utc().timestamp_micros()))
                } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                {
                    Ok(Datum::Timestamp(dt.and_utc().timestamp_micros()))
                } else if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let dt = match d.and_hms_opt(0, 0, 0) {
                        Some(dt) => dt,
                        None => {
                            return Err(ExecutionError::TypeError(format!(
                                "Cannot cast '{s}' to timestamp"
                            )))
                        }
                    };
                    Ok(Datum::Timestamp(dt.and_utc().timestamp_micros()))
                } else {
                    Err(ExecutionError::TypeError(format!(
                        "Cannot cast '{s}' to timestamp"
                    )))
                }
            }
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to timestamp"
            ))),
        },
        "date" => match &val {
            Datum::Date(_) => Ok(val),
            Datum::Timestamp(us) => {
                // Convert timestamp (microseconds since epoch) to date (days since epoch)
                let days = i32::try_from(*us / 86_400_000_000)
                    .map_err(|_| ExecutionError::NumericOverflow)?;
                Ok(Datum::Date(days))
            }
            Datum::Text(s) => {
                use chrono::NaiveDate;
                let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .or_else(|_| NaiveDate::parse_from_str(s, "%m/%d/%Y"))
                    .or_else(|_| NaiveDate::parse_from_str(s, "%Y%m%d"))
                    .map_err(|e| {
                        ExecutionError::TypeError(format!("Cannot cast '{s}' to date: {e}"))
                    })?;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_else(|| {
                    NaiveDate::from_ymd_opt(2000, 1, 1).unwrap_or(NaiveDate::MIN)
                });
                let days = i32::try_from((date - epoch).num_days())
                    .map_err(|_| ExecutionError::NumericOverflow)?;
                Ok(Datum::Date(days))
            }
            Datum::Int32(d) => Ok(Datum::Date(*d)),
            Datum::Int64(d) => i32::try_from(*d)
                .map(Datum::Date)
                .map_err(|_| ExecutionError::NumericOverflow),
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to date"
            ))),
        },
        "jsonb" | "json" => match &val {
            Datum::Jsonb(_) => Ok(val),
            Datum::Text(s) => serde_json::from_str(s)
                .map(Datum::Jsonb)
                .map_err(|e| ExecutionError::TypeError(format!("Cannot cast '{s}' to jsonb: {e}"))),
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to jsonb"
            ))),
        },
        "uuid" => match &val {
            Datum::Uuid(_) => Ok(val),
            Datum::Text(s) => {
                let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
                if hex.len() != 32 {
                    return Err(ExecutionError::TypeError(format!(
                        "Cannot cast '{s}' to uuid"
                    )));
                }
                let v = u128::from_str_radix(&hex, 16)
                    .map_err(|_| ExecutionError::TypeError(format!("Cannot cast '{s}' to uuid")))?;
                Ok(Datum::Uuid(v))
            }
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to uuid"
            ))),
        },
        "bytea" => match &val {
            Datum::Bytea(_) => Ok(val),
            Datum::Text(s) => {
                let hex_str = s.strip_prefix("\\x").unwrap_or(s);
                let bytes = (0..hex_str.len())
                    .step_by(2)
                    .map(|i| {
                        let end = (i + 2).min(hex_str.len());
                        u8::from_str_radix(&hex_str[i..end], 16)
                    })
                    .collect::<Result<Vec<u8>, _>>()
                    .map_err(|_| {
                        ExecutionError::TypeError(format!("Cannot cast '{s}' to bytea"))
                    })?;
                Ok(Datum::Bytea(bytes))
            }
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to bytea"
            ))),
        },
        "interval" => match &val {
            Datum::Interval(_, _, _) => Ok(val),
            Datum::Text(s) => {
                // Simplified interval parsing: "N days", "HH:MM:SS", or "N hours"
                // Full PG interval parsing is complex; handle common patterns
                let s_lower = s.trim().to_lowercase();
                if let Some(rest) = s_lower
                    .strip_suffix("days")
                    .or_else(|| s_lower.strip_suffix("day"))
                {
                    let d: i32 = rest.trim().parse().map_err(|_| {
                        ExecutionError::TypeError(format!("Cannot cast '{s}' to interval"))
                    })?;
                    Ok(Datum::Interval(0, d, 0))
                } else if s_lower.contains(':') {
                    // HH:MM:SS
                    let parts: Vec<&str> = s_lower.split(':').collect();
                    let cast_err =
                        || ExecutionError::TypeError(format!("Cannot cast '{s}' to interval"));
                    if parts.len() >= 2 {
                        let h: i64 = parts[0].trim().parse().map_err(|_| cast_err())?;
                        let m: i64 = parts[1].trim().parse().map_err(|_| cast_err())?;
                        let sec: i64 = if parts.len() >= 3 {
                            parts[2]
                                .trim()
                                .split('.')
                                .next()
                                .unwrap_or("0")
                                .parse()
                                .map_err(|_| cast_err())?
                        } else {
                            0
                        };
                        let us = h
                            .checked_mul(3_600_000_000)
                            .and_then(|v| v.checked_add(m.checked_mul(60_000_000)?))
                            .and_then(|v| v.checked_add(sec.checked_mul(1_000_000)?))
                            .ok_or(ExecutionError::NumericOverflow)?;
                        Ok(Datum::Interval(0, 0, us))
                    } else {
                        Err(ExecutionError::TypeError(format!(
                            "Cannot cast '{s}' to interval"
                        )))
                    }
                } else {
                    Err(ExecutionError::TypeError(format!(
                        "Cannot cast '{s}' to interval"
                    )))
                }
            }
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to interval"
            ))),
        },
        "time" | "time without time zone" => match &val {
            Datum::Time(_) => Ok(val),
            Datum::Text(s) => {
                let parts: Vec<&str> = s.split(':').collect();
                if parts.len() < 2 {
                    return Err(ExecutionError::TypeError(format!(
                        "Cannot cast '{s}' to time"
                    )));
                }
                let h: i64 = parts[0]
                    .parse()
                    .map_err(|_| ExecutionError::TypeError(format!("Cannot cast '{s}' to time")))?;
                let m: i64 = parts[1]
                    .parse()
                    .map_err(|_| ExecutionError::TypeError(format!("Cannot cast '{s}' to time")))?;
                let cast_err = || ExecutionError::TypeError(format!("Cannot cast '{s}' to time"));
                let (sec, frac) = if parts.len() >= 3 {
                    let sec_parts: Vec<&str> = parts[2].split('.').collect();
                    let s_val: i64 = sec_parts[0].parse().map_err(|_| cast_err())?;
                    let f_val: i64 = if sec_parts.len() > 1 {
                        let f = sec_parts[1];
                        let padded = format!("{:0<6}", &f[..f.len().min(6)]);
                        padded.parse().map_err(|_| cast_err())?
                    } else {
                        0
                    };
                    (s_val, f_val)
                } else {
                    (0, 0)
                };
                let time_us = h
                    .checked_mul(3_600_000_000)
                    .and_then(|v| v.checked_add(m.checked_mul(60_000_000)?))
                    .and_then(|v| v.checked_add(sec.checked_mul(1_000_000)?))
                    .and_then(|v| v.checked_add(frac))
                    .ok_or(ExecutionError::NumericOverflow)?;
                Ok(Datum::Time(time_us))
            }
            Datum::Timestamp(us) => {
                // Extract time-of-day from timestamp
                let day_us = us.rem_euclid(86_400_000_000);
                Ok(Datum::Time(day_us))
            }
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to time"
            ))),
        },
        t if t.ends_with("[]") => match val {
            Datum::Array(_) => Ok(val),
            _ => Err(ExecutionError::TypeError(format!(
                "Cannot cast {val:?} to {target}"
            ))),
        },
        _ => Err(ExecutionError::TypeError(format!(
            "Unknown target type: {target}"
        ))),
    }
}
