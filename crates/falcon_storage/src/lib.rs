// ── PRODUCTION modules (always compiled) ──
pub mod audit;
pub mod backup;
pub mod cold_store;
#[allow(dead_code)]
pub mod csn;
pub mod delta_lsn;
pub mod engine;
mod engine_ddl;
mod engine_dml;
pub mod gc;
pub mod gc_budget;
pub mod group_commit;
pub mod health;
pub mod hotspot;
pub mod index;
pub mod io;
pub mod job_scheduler;
pub mod logical_backup;
pub mod memory;
pub mod memtable;
pub mod metering;
pub mod mvcc;
pub mod partition;
pub mod role_catalog;
pub mod scan_optimizer;
pub mod security_manager;
pub mod stats;
pub mod storage_trait;
#[allow(dead_code)]
pub mod structured_lsn;
pub mod table_handle;
pub mod unified_data_plane;
pub mod unified_data_plane_full;
pub mod upgrade;
pub mod ustm;
pub mod verification;
pub mod wal;
pub mod wal_win_async;
pub mod zstd_dict;
pub mod zstd_recompress;
pub mod zstd_segment;
pub mod zstd_streaming;

// ── Enterprise stubs (always compiled for handler compat, disabled at runtime) ──
pub mod cdc;
pub mod cdc_schema_registry;
pub mod cdc_wal_bridge;
pub mod encryption;
pub mod object_store_backend;
#[cfg(feature = "online_ddl_full")]
pub mod online_ddl;
#[cfg(not(feature = "online_ddl_full"))]
#[path = "online_ddl_stub.rs"]
pub mod online_ddl;
pub mod pitr;
pub mod resource_isolation;
pub mod tenant_registry;

// ── EXPERIMENTAL / non-v1.0 storage engines (feature-gated, default OFF) ──
#[cfg(feature = "columnstore")]
pub mod columnstore;
#[cfg(feature = "disk_rowstore")]
pub mod disk_rowstore;
#[cfg(feature = "lsm")]
pub mod lsm;
#[cfg(feature = "lsm")]
pub mod lsm_table;
#[cfg(feature = "redb")]
pub mod redb_table;
#[cfg(feature = "rocksdb")]
pub mod rocksdb_table;
#[cfg(feature = "rocksdb")]
pub mod rocksdb_engine;

// ── Stub for columnstore so engine.rs compiles without the feature ──
#[cfg(not(feature = "columnstore"))]
#[path = "columnstore_stub.rs"]
pub mod columnstore;

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
    let y = i64::from(if month <= 2 { year - 1 } else { year });
    let m = i64::from(if month <= 2 { month + 9 } else { month - 3 });
    let era_days = 365 * y + y / 4 - y / 100 + y / 400 + (153 * m + 2) / 5 + i64::from(day) - 1;
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
            Datum::Int64(v) => i32::try_from(*v)
                .map(Datum::Int32)
                .map_err(|_| "Numeric value out of range for int".to_string()),
            Datum::Float64(v) => {
                if v.is_nan() || v.is_infinite() || *v < (i32::MIN as f64) || *v > (i32::MAX as f64)
                {
                    Err("Numeric value out of range for int".to_string())
                } else {
                    Ok(Datum::Int32(*v as i32))
                }
            }
            Datum::Text(s) => s
                .parse::<i32>()
                .map(Datum::Int32)
                .map_err(|e| e.to_string()),
            Datum::Boolean(b) => Ok(Datum::Int32(if *b { 1 } else { 0 })),
            _ => Err(format!("Cannot cast {val:?} to int")),
        },
        "bigint" => match &val {
            Datum::Int64(_) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Int64(i64::from(*v))),
            Datum::Float64(v) => {
                if v.is_nan() || v.is_infinite() || *v < (i64::MIN as f64) || *v > (i64::MAX as f64)
                {
                    Err("Numeric value out of range for bigint".to_string())
                } else {
                    Ok(Datum::Int64(*v as i64))
                }
            }
            Datum::Text(s) => s
                .parse::<i64>()
                .map(Datum::Int64)
                .map_err(|e| e.to_string()),
            _ => Err(format!("Cannot cast {val:?} to bigint")),
        },
        "float8" => match &val {
            Datum::Float64(_) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Float64(f64::from(*v))),
            Datum::Int64(v) => Ok(Datum::Float64(*v as f64)),
            Datum::Text(s) => s
                .parse::<f64>()
                .map(Datum::Float64)
                .map_err(|e| e.to_string()),
            _ => Err(format!("Cannot cast {val:?} to float8")),
        },
        "text" => Ok(Datum::Text(format!("{val}"))),
        "boolean" => match &val {
            Datum::Boolean(_) => Ok(val),
            Datum::Int32(v) => Ok(Datum::Boolean(*v != 0)),
            Datum::Int64(v) => Ok(Datum::Boolean(*v != 0)),
            Datum::Text(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" => Ok(Datum::Boolean(true)),
                "false" | "f" | "0" | "no" => Ok(Datum::Boolean(false)),
                _ => Err(format!("Cannot cast '{s}' to boolean")),
            },
            _ => Err(format!("Cannot cast {val:?} to boolean")),
        },
        "timestamp" => match &val {
            Datum::Timestamp(_) => Ok(val),
            Datum::Date(days) => {
                let us = i64::from(*days)
                    .checked_mul(86_400_000_000)
                    .ok_or_else(|| "Date value out of range for timestamp".to_string())?;
                Ok(Datum::Timestamp(us))
            }
            Datum::Int64(us) => Ok(Datum::Timestamp(*us)),
            Datum::Int32(us) => Ok(Datum::Timestamp(i64::from(*us))),
            _ => Err(format!("Cannot cast {val:?} to timestamp")),
        },
        "date" => match &val {
            Datum::Date(_) => Ok(val),
            Datum::Timestamp(us) => {
                let days = i32::try_from(*us / 86_400_000_000)
                    .map_err(|_| "Timestamp value out of range for date".to_string())?;
                Ok(Datum::Date(days))
            }
            Datum::Text(s) => {
                // Parse YYYY-MM-DD manually without chrono
                let parts: Vec<&str> = s.split('-').collect();
                if parts.len() == 3 {
                    let y: i32 = parts[0]
                        .parse()
                        .map_err(|_| format!("Cannot cast '{s}' to date"))?;
                    let m: u32 = parts[1]
                        .parse()
                        .map_err(|_| format!("Cannot cast '{s}' to date"))?;
                    let d: u32 = parts[2]
                        .parse()
                        .map_err(|_| format!("Cannot cast '{s}' to date"))?;
                    let days = date_to_days_since_epoch(y, m, d)
                        .ok_or_else(|| format!("Cannot cast '{s}' to date: invalid date"))?;
                    Ok(Datum::Date(days))
                } else {
                    Err(format!("Cannot cast '{s}' to date"))
                }
            }
            Datum::Int32(d) => Ok(Datum::Date(*d)),
            Datum::Int64(d) => i32::try_from(*d)
                .map(Datum::Date)
                .map_err(|_| "Numeric value out of range for date".to_string()),
            _ => Err(format!("Cannot cast {val:?} to date")),
        },
        "jsonb" => match &val {
            Datum::Jsonb(_) => Ok(val),
            Datum::Text(s) => serde_json::from_str(s)
                .map(Datum::Jsonb)
                .map_err(|e| format!("Cannot cast to jsonb: {e}")),
            _ => Err(format!("Cannot cast {val:?} to jsonb")),
        },
        _ => Err(format!("Unsupported cast target: {target}")),
    }
}
