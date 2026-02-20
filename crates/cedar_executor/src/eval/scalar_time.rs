use cedar_common::datum::Datum;
use cedar_common::error::ExecutionError;
use cedar_sql_frontend::types::ScalarFunc;

/// Dispatch a time/date-domain scalar function.
pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::Now => {
            let now = chrono::Utc::now();
            let us = now.timestamp_micros();
            Ok(Datum::Timestamp(us))
        }
        ScalarFunc::CurrentDate => {
            let now = chrono::Utc::now();
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = (now.date_naive() - epoch).num_days() as i32;
            Ok(Datum::Date(days))
        }
        ScalarFunc::CurrentTime => {
            let now = chrono::Utc::now();
            let time_str = now.format("%H:%M:%S").to_string();
            Ok(Datum::Text(time_str))
        }
        ScalarFunc::Extract => {
            // EXTRACT(field FROM timestamp) — args: [field_text, timestamp]
            let field = match args.first() {
                Some(Datum::Text(s)) => s.to_uppercase(),
                _ => return Err(ExecutionError::TypeError("EXTRACT requires field name".into())),
            };
            let ts_us = match args.get(1) {
                Some(Datum::Timestamp(us)) => *us,
                Some(Datum::Date(days)) => *days as i64 * 86400 * 1_000_000,
                Some(Datum::Int64(us)) => *us,
                Some(Datum::Int32(us)) => *us as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("EXTRACT requires timestamp or date".into())),
            };
            let secs = ts_us / 1_000_000;
            let nsecs = ((ts_us % 1_000_000).abs() * 1000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                .ok_or_else(|| ExecutionError::TypeError("Invalid timestamp".into()))?;
            let val = match field.as_str() {
                "YEAR" => dt.format("%Y").to_string().parse::<i64>().unwrap_or(0),
                "MONTH" => dt.format("%m").to_string().parse::<i64>().unwrap_or(0),
                "DAY" => dt.format("%d").to_string().parse::<i64>().unwrap_or(0),
                "HOUR" => dt.format("%H").to_string().parse::<i64>().unwrap_or(0),
                "MINUTE" => dt.format("%M").to_string().parse::<i64>().unwrap_or(0),
                "SECOND" => dt.format("%S").to_string().parse::<i64>().unwrap_or(0),
                "DOW" | "DAYOFWEEK" => dt.format("%w").to_string().parse::<i64>().unwrap_or(0),
                "DOY" | "DAYOFYEAR" => dt.format("%j").to_string().parse::<i64>().unwrap_or(0),
                "EPOCH" => ts_us / 1_000_000,
                _ => return Err(ExecutionError::TypeError(format!("Unknown EXTRACT field: {}", field))),
            };
            Ok(Datum::Int64(val))
        }
        ScalarFunc::DateTrunc => {
            // DATE_TRUNC(field, timestamp) — args: [field_text, timestamp]
            let field = match args.first() {
                Some(Datum::Text(s)) => s.to_lowercase(),
                _ => return Err(ExecutionError::TypeError("DATE_TRUNC requires field name".into())),
            };
            let ts_us = match args.get(1) {
                Some(Datum::Timestamp(us)) => *us,
                Some(Datum::Date(days)) => *days as i64 * 86400 * 1_000_000,
                Some(Datum::Int64(us)) => *us,
                Some(Datum::Int32(us)) => *us as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DATE_TRUNC requires timestamp or date".into())),
            };
            let secs = ts_us / 1_000_000;
            let nsecs = ((ts_us % 1_000_000).abs() * 1000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                .ok_or_else(|| ExecutionError::TypeError("Invalid timestamp".into()))?;
            use chrono::{Datelike, Timelike};
            let truncated = match field.as_str() {
                "year" => dt.with_month(1).and_then(|d| d.with_day(1))
                    .and_then(|d| d.with_hour(0)).and_then(|d| d.with_minute(0))
                    .and_then(|d| d.with_second(0)).and_then(|d| d.with_nanosecond(0)),
                "month" => dt.with_day(1)
                    .and_then(|d| d.with_hour(0)).and_then(|d| d.with_minute(0))
                    .and_then(|d| d.with_second(0)).and_then(|d| d.with_nanosecond(0)),
                "day" => dt.with_hour(0).and_then(|d| d.with_minute(0))
                    .and_then(|d| d.with_second(0)).and_then(|d| d.with_nanosecond(0)),
                "hour" => dt.with_minute(0)
                    .and_then(|d| d.with_second(0)).and_then(|d| d.with_nanosecond(0)),
                "minute" => dt.with_second(0).and_then(|d| d.with_nanosecond(0)),
                "second" => dt.with_nanosecond(0),
                _ => return Err(ExecutionError::TypeError(format!("Unknown DATE_TRUNC field: {}", field))),
            };
            match truncated {
                Some(t) => Ok(Datum::Timestamp(t.timestamp_micros())),
                None => Err(ExecutionError::TypeError("DATE_TRUNC failed".into())),
            }
        }
        ScalarFunc::ToChar => {
            // TO_CHAR(timestamp, format) — format timestamp as string
            let ts_us = match args.first() {
                Some(Datum::Timestamp(us)) => *us,
                Some(Datum::Date(days)) => *days as i64 * 86400 * 1_000_000,
                Some(Datum::Int64(us)) => *us,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TO_CHAR requires timestamp or date".into())),
            };
            let fmt = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TO_CHAR requires format string".into())),
            };
            let secs = ts_us / 1_000_000;
            let nsecs = ((ts_us % 1_000_000).abs() * 1000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                .ok_or_else(|| ExecutionError::TypeError("Invalid timestamp".into()))?;
            // Convert PG format to chrono format
            let chrono_fmt = fmt
                .replace("YYYY", "%Y").replace("YY", "%y")
                .replace("MM", "%m").replace("DD", "%d")
                .replace("HH24", "%H").replace("HH12", "%I").replace("HH", "%H")
                .replace("MI", "%M").replace("SS", "%S")
                .replace("AM", "%p").replace("PM", "%p")
                .replace("Month", "%B").replace("MONTH", "%B")
                .replace("Mon", "%b").replace("MON", "%b")
                .replace("Day", "%A").replace("DAY", "%A")
                .replace("Dy", "%a").replace("DY", "%a");
            Ok(Datum::Text(dt.format(&chrono_fmt).to_string()))
        }
        _ => Err(ExecutionError::TypeError(format!("Not a time function: {:?}", func))),
    }
}
