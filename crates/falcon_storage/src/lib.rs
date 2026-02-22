pub mod audit;
pub mod backup;
pub mod cdc;
pub mod columnstore;
pub mod disk_rowstore;
pub mod encryption;
pub mod engine;
mod engine_ddl;
mod engine_dml;
pub mod gc;
pub mod group_commit;
pub mod health;
pub mod hotspot;
pub mod index;
pub mod lsm;
pub mod lsm_table;
pub mod memory;
pub mod memtable;
pub mod metering;
pub mod mvcc;
pub mod online_ddl;
pub mod partition;
pub mod pitr;
pub mod resource_isolation;
pub mod role_catalog;
pub mod security_manager;
pub mod stats;
pub mod tenant_registry;
pub mod upgrade;
pub mod verification;
pub mod wal;

#[cfg(test)]
mod tests;

/// Convert (year, month, day) to days since Unix epoch (1970-01-01).
/// Returns None if the date is invalid.
fn date_to_days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Days in each month (non-leap)
    let days_in_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let max_day = if month == 2 && is_leap {
        29
    } else {
        days_in_month[month as usize]
    };
    if day > max_day {
        return None;
    }
    // Calculate days from year 0 to (year, month, day) using civil calendar
    // Then subtract days from year 0 to 1970-01-01
    let y = if month <= 2 { year - 1 } else { year } as i64;
    let m = if month <= 2 { month + 9 } else { month - 3 } as i64;
    let era_days = 365 * y + y / 4 - y / 100 + y / 400 + (153 * m + 2) / 5 + day as i64 - 1;
    // Epoch (1970-01-01) in the same formula
    let epoch_days = {
        let ey = 1969i64; // month 1 <= 2, so y = year - 1 = 1969
        let em = 10i64; // month 1 => m = 1 + 9 = 10
        365 * ey + ey / 4 - ey / 100 + ey / 400 + (153 * em + 2) / 5 + 1 - 1
    };
    Some((era_days - epoch_days) as i32)
}

/// Lightweight datum cast for DDL ALTER COLUMN TYPE.
/// Mirrors falcon_executor::eval::cast::eval_cast but avoids the dependency.
pub(crate) fn eval_cast_datum(
    val: falcon_common::datum::Datum,
    target: &str,
) -> Result<falcon_common::datum::Datum, String> {
    use falcon_common::datum::Datum;
    if val.is_null() {
        return Ok(Datum::Null);
    }
    match target {
        "int" => match &val {
            Datum::Int32(_) => Ok(val),
            Datum::Int64(v) => Ok(Datum::Int32(*v as i32)),
            Datum::Float64(v) => Ok(Datum::Int32(*v as i32)),
            Datum::Text(s) => s
                .parse::<i32>()
                .map(Datum::Int32)
                .map_err(|e| e.to_string()),
            Datum::Boolean(b) => Ok(Datum::Int32(if *b { 1 } else { 0 })),
            _ => Err(format!("Cannot cast {:?} to int", val)),
        },
        "bigint" => match &val {
            Datum::Int64(_) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Int64(*v as i64)),
            Datum::Float64(v) => Ok(Datum::Int64(*v as i64)),
            Datum::Text(s) => s
                .parse::<i64>()
                .map(Datum::Int64)
                .map_err(|e| e.to_string()),
            _ => Err(format!("Cannot cast {:?} to bigint", val)),
        },
        "float8" => match &val {
            Datum::Float64(_) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Float64(*v as f64)),
            Datum::Int64(v) => Ok(Datum::Float64(*v as f64)),
            Datum::Text(s) => s
                .parse::<f64>()
                .map(Datum::Float64)
                .map_err(|e| e.to_string()),
            _ => Err(format!("Cannot cast {:?} to float8", val)),
        },
        "text" => Ok(Datum::Text(format!("{}", val))),
        "boolean" => match &val {
            Datum::Boolean(_) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Boolean(*v != 0)),
            Datum::Int64(v) => Ok(Datum::Boolean(*v != 0)),
            Datum::Text(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" => Ok(Datum::Boolean(true)),
                "false" | "f" | "0" | "no" => Ok(Datum::Boolean(false)),
                _ => Err(format!("Cannot cast '{}' to boolean", s)),
            },
            _ => Err(format!("Cannot cast {:?} to boolean", val)),
        },
        "timestamp" => match &val {
            Datum::Timestamp(_) => Ok(val),
            Datum::Date(days) => {
                let us = *days as i64 * 86400 * 1_000_000;
                Ok(Datum::Timestamp(us))
            }
            Datum::Int64(us) => Ok(Datum::Timestamp(*us)),
            Datum::Int32(us) => Ok(Datum::Timestamp(*us as i64)),
            _ => Err(format!("Cannot cast {:?} to timestamp", val)),
        },
        "date" => match &val {
            Datum::Date(_) => Ok(val),
            Datum::Timestamp(us) => {
                let days = (*us / (86400 * 1_000_000)) as i32;
                Ok(Datum::Date(days))
            }
            Datum::Text(s) => {
                // Parse YYYY-MM-DD manually without chrono
                let parts: Vec<&str> = s.split('-').collect();
                if parts.len() == 3 {
                    let y: i32 = parts[0]
                        .parse()
                        .map_err(|_| format!("Cannot cast '{}' to date", s))?;
                    let m: u32 = parts[1]
                        .parse()
                        .map_err(|_| format!("Cannot cast '{}' to date", s))?;
                    let d: u32 = parts[2]
                        .parse()
                        .map_err(|_| format!("Cannot cast '{}' to date", s))?;
                    let days = date_to_days_since_epoch(y, m, d)
                        .ok_or_else(|| format!("Cannot cast '{}' to date: invalid date", s))?;
                    Ok(Datum::Date(days))
                } else {
                    Err(format!("Cannot cast '{}' to date", s))
                }
            }
            Datum::Int32(d) => Ok(Datum::Date(*d)),
            Datum::Int64(d) => Ok(Datum::Date(*d as i32)),
            _ => Err(format!("Cannot cast {:?} to date", val)),
        },
        "jsonb" => match &val {
            Datum::Jsonb(_) => Ok(val),
            Datum::Text(s) => serde_json::from_str(s)
                .map(Datum::Jsonb)
                .map_err(|e| format!("Cannot cast to jsonb: {}", e)),
            _ => Err(format!("Cannot cast {:?} to jsonb", val)),
        },
        _ => Err(format!("Unsupported cast target: {}", target)),
    }
}
