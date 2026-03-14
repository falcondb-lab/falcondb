use chrono::{Datelike, Timelike};
use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::ScalarFunc;

use std::cell::Cell;

thread_local! {
    static STMT_TS_US: Cell<i64> = const { Cell::new(0) };
}

#[allow(dead_code)]
pub fn set_statement_ts(us: i64) {
    STMT_TS_US.with(|c| c.set(us));
}

pub fn reset_statement_ts() {
    STMT_TS_US.with(|c| c.set(0));
}

pub fn statement_ts() -> i64 {
    STMT_TS_US.with(|c| {
        let v = c.get();
        if v != 0 {
            v
        } else {
            let now = chrono::Utc::now().timestamp_micros();
            c.set(now);
            now
        }
    })
}

/// Dispatch a time/date-domain scalar function.
pub fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::Now => Ok(Datum::Timestamp(statement_ts())),
        ScalarFunc::CurrentDate => {
            let us = statement_ts();
            let secs = us / 1_000_000;
            let dt =
                chrono::DateTime::from_timestamp(secs, 0).ok_or(ExecutionError::NumericOverflow)?;
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_else(|| {
                chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap_or(chrono::NaiveDate::MIN)
            });
            let days = i32::try_from((dt.date_naive() - epoch).num_days())
                .map_err(|_| ExecutionError::NumericOverflow)?;
            Ok(Datum::Date(days))
        }
        ScalarFunc::CurrentTime => {
            let us = statement_ts();
            let secs = us / 1_000_000;
            let dt =
                chrono::DateTime::from_timestamp(secs, 0).ok_or(ExecutionError::NumericOverflow)?;
            Ok(Datum::Text(dt.format("%H:%M:%S").to_string()))
        }
        ScalarFunc::Extract => {
            // EXTRACT(field FROM timestamp) — args: [field_text, timestamp]
            let field = match args.first() {
                Some(Datum::Text(s)) => s.to_uppercase(),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "EXTRACT requires field name".into(),
                    ))
                }
            };
            let ts_us = match args.get(1) {
                Some(Datum::Timestamp(us)) | Some(Datum::Int64(us)) => *us,
                Some(Datum::Date(days)) => i64::from(*days)
                    .checked_mul(86_400_000_000)
                    .ok_or(ExecutionError::NumericOverflow)?,
                Some(Datum::Int32(us)) => i64::from(*us),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "EXTRACT requires timestamp or date".into(),
                    ))
                }
            };
            let secs = ts_us / 1_000_000;
            let nsecs = ((ts_us % 1_000_000).abs() * 1000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                .ok_or_else(|| ExecutionError::TypeError("Invalid timestamp".into()))?;
            let val = match field.as_str() {
                "YEAR" => i64::from(dt.year()),
                "MONTH" => i64::from(dt.month()),
                "DAY" => i64::from(dt.day()),
                "HOUR" => i64::from(dt.hour()),
                "MINUTE" => i64::from(dt.minute()),
                "SECOND" => i64::from(dt.second()),
                "DOW" | "DAYOFWEEK" => i64::from(dt.weekday().num_days_from_sunday()),
                "DOY" | "DAYOFYEAR" => i64::from(dt.ordinal()),
                "EPOCH" => ts_us / 1_000_000,
                _ => {
                    return Err(ExecutionError::TypeError(format!(
                        "Unknown EXTRACT field: {field}"
                    )))
                }
            };
            Ok(Datum::Int64(val))
        }
        ScalarFunc::DateTrunc => {
            // DATE_TRUNC(field, timestamp) — args: [field_text, timestamp]
            let field = match args.first() {
                Some(Datum::Text(s)) => s.to_lowercase(),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "DATE_TRUNC requires field name".into(),
                    ))
                }
            };
            let ts_us = match args.get(1) {
                Some(Datum::Timestamp(us)) | Some(Datum::Int64(us)) => *us,
                Some(Datum::Date(days)) => i64::from(*days)
                    .checked_mul(86_400_000_000)
                    .ok_or(ExecutionError::NumericOverflow)?,
                Some(Datum::Int32(us)) => i64::from(*us),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "DATE_TRUNC requires timestamp or date".into(),
                    ))
                }
            };
            let secs = ts_us / 1_000_000;
            let nsecs = ((ts_us % 1_000_000).abs() * 1000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                .ok_or_else(|| ExecutionError::TypeError("Invalid timestamp".into()))?;
            let truncated = match field.as_str() {
                "year" => dt
                    .with_month(1)
                    .and_then(|d| d.with_day(1))
                    .and_then(|d| d.with_hour(0))
                    .and_then(|d| d.with_minute(0))
                    .and_then(|d| d.with_second(0))
                    .and_then(|d| d.with_nanosecond(0)),
                "month" => dt
                    .with_day(1)
                    .and_then(|d| d.with_hour(0))
                    .and_then(|d| d.with_minute(0))
                    .and_then(|d| d.with_second(0))
                    .and_then(|d| d.with_nanosecond(0)),
                "day" => dt
                    .with_hour(0)
                    .and_then(|d| d.with_minute(0))
                    .and_then(|d| d.with_second(0))
                    .and_then(|d| d.with_nanosecond(0)),
                "hour" => dt
                    .with_minute(0)
                    .and_then(|d| d.with_second(0))
                    .and_then(|d| d.with_nanosecond(0)),
                "minute" => dt.with_second(0).and_then(|d| d.with_nanosecond(0)),
                "second" => dt.with_nanosecond(0),
                _ => {
                    return Err(ExecutionError::TypeError(format!(
                        "Unknown DATE_TRUNC field: {field}"
                    )))
                }
            };
            truncated.map_or_else(
                || Err(ExecutionError::TypeError("DATE_TRUNC failed".into())),
                |t| Ok(Datum::Timestamp(t.timestamp_micros())),
            )
        }
        ScalarFunc::ToChar => {
            // TO_CHAR(timestamp, format) — format timestamp as string
            let ts_us = match args.first() {
                Some(Datum::Timestamp(us)) | Some(Datum::Int64(us)) => *us,
                Some(Datum::Date(days)) => i64::from(*days)
                    .checked_mul(86_400_000_000)
                    .ok_or(ExecutionError::NumericOverflow)?,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "TO_CHAR requires timestamp or date".into(),
                    ))
                }
            };
            let fmt = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "TO_CHAR requires format string".into(),
                    ))
                }
            };
            let secs = ts_us / 1_000_000;
            let nsecs = ((ts_us % 1_000_000).abs() * 1000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                .ok_or_else(|| ExecutionError::TypeError("Invalid timestamp".into()))?;
            // Convert PG format to chrono format
            let chrono_fmt = fmt
                .replace("YYYY", "%Y")
                .replace("YY", "%y")
                .replace("MM", "%m")
                .replace("DD", "%d")
                .replace("HH24", "%H")
                .replace("HH12", "%I")
                .replace("HH", "%H")
                .replace("MI", "%M")
                .replace("SS", "%S")
                .replace("AM", "%p")
                .replace("PM", "%p")
                .replace("Month", "%B")
                .replace("MONTH", "%B")
                .replace("Mon", "%b")
                .replace("MON", "%b")
                .replace("Day", "%A")
                .replace("DAY", "%A")
                .replace("Dy", "%a")
                .replace("DY", "%a");
            Ok(Datum::Text(dt.format(&chrono_fmt).to_string()))
        }
        _ => Err(ExecutionError::TypeError(format!(
            "Not a time function: {func:?}"
        ))),
    }
}
