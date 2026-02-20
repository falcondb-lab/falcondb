use cedar_common::datum::Datum;
use cedar_common::error::ExecutionError;
use cedar_sql_frontend::types::ScalarFunc;

pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Option<Result<Datum, ExecutionError>> {
    match func {
        ScalarFunc::MakeInterval |
        ScalarFunc::DateDiff |
        ScalarFunc::EpochFromTimestamp |
        ScalarFunc::ToDate |
        ScalarFunc::DateAdd |
        ScalarFunc::DateSubtract |
        ScalarFunc::ClockTimestamp |
        ScalarFunc::StatementTimestamp |
        ScalarFunc::Timeofday |
        ScalarFunc::MakeDate |
        ScalarFunc::MakeTimestamp |
        ScalarFunc::ToTimestamp |
        ScalarFunc::Age => Some(dispatch_inner(func, args)),
        _ => None,
    }
}

fn dispatch_inner(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::MakeInterval => {
            // MAKE_INTERVAL(years, months, days, hours, mins, secs) 鈫?text interval
            let years = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) | None => 0,
                _ => return Err(ExecutionError::TypeError("MAKE_INTERVAL requires integer".into())),
            };
            let months = match args.get(1) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) | None => 0,
                _ => 0,
            };
            let days = match args.get(2) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) | None => 0,
                _ => 0,
            };
            let hours = match args.get(3) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) | None => 0,
                _ => 0,
            };
            let mins = match args.get(4) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) | None => 0,
                _ => 0,
            };
            let secs = match args.get(5) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Float64(n)) => *n as i64,
                Some(Datum::Null) | None => 0,
                _ => 0,
            };
            let mut parts = Vec::new();
            if years != 0 { parts.push(format!("{} year{}", years, if years.abs() != 1 { "s" } else { "" })); }
            if months != 0 { parts.push(format!("{} mon{}", months, if months.abs() != 1 { "s" } else { "" })); }
            if days != 0 { parts.push(format!("{} day{}", days, if days.abs() != 1 { "s" } else { "" })); }
            parts.push(format!("{:02}:{:02}:{:02}", hours, mins, secs));
            Ok(Datum::Text(parts.join(" ")))
        }
        ScalarFunc::DateDiff => {
            // DATE_DIFF(timestamp1, timestamp2) 鈫?difference in days
            let ts1 = match args.first() {
                Some(Datum::Timestamp(t)) => *t,
                Some(Datum::Date(d)) => *d as i64 * 86400 * 1_000_000,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DATE_DIFF requires timestamp or date".into())),
            };
            let ts2 = match args.get(1) {
                Some(Datum::Timestamp(t)) => *t,
                Some(Datum::Date(d)) => *d as i64 * 86400 * 1_000_000,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DATE_DIFF requires timestamp or date".into())),
            };
            let diff_days = (ts1 - ts2) / (86400 * 1_000_000);
            Ok(Datum::Int64(diff_days))
        }
        ScalarFunc::EpochFromTimestamp => {
            // EPOCH_FROM_TIMESTAMP(timestamp) 鈫?seconds since epoch as float
            let ts = match args.first() {
                Some(Datum::Timestamp(t)) => *t,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("EPOCH_FROM_TIMESTAMP requires timestamp".into())),
            };
            Ok(Datum::Float64(ts as f64 / 1_000_000.0))
        }
        ScalarFunc::ToDate => {
            // TO_DATE(text, format) 鈫?date
            // Simplified: parse common date formats
            let text = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TO_DATE requires text".into())),
            };
            let _fmt = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => "YYYY-MM-DD".to_string(),
            };
            // Try common formats
            use chrono::NaiveDate;
            let date = NaiveDate::parse_from_str(&text, "%Y-%m-%d")
                .or_else(|_| NaiveDate::parse_from_str(&text, "%m/%d/%Y"))
                .or_else(|_| NaiveDate::parse_from_str(&text, "%d-%m-%Y"))
                .or_else(|_| NaiveDate::parse_from_str(&text, "%Y%m%d"))
                .map_err(|e| ExecutionError::TypeError(format!("TO_DATE parse error: {}", e)))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = (date - epoch).num_days() as i32;
            Ok(Datum::Date(days))
        }
        ScalarFunc::DateAdd => {
            // DATE_ADD(timestamp/date, days) 鈫?same type + days
            let days_to_add = match args.get(1) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Float64(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DATE_ADD requires days".into())),
            };
            match args.first() {
                Some(Datum::Timestamp(t)) => Ok(Datum::Timestamp(*t + days_to_add * 86400 * 1_000_000)),
                Some(Datum::Date(d)) => Ok(Datum::Date(*d + days_to_add as i32)),
                Some(Datum::Null) => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("DATE_ADD requires timestamp or date".into())),
            }
        }
        ScalarFunc::DateSubtract => {
            // DATE_SUBTRACT(timestamp/date, days) 鈫?same type - days
            let days_to_sub = match args.get(1) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Float64(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DATE_SUBTRACT requires days".into())),
            };
            match args.first() {
                Some(Datum::Timestamp(t)) => Ok(Datum::Timestamp(*t - days_to_sub * 86400 * 1_000_000)),
                Some(Datum::Date(d)) => Ok(Datum::Date(*d - days_to_sub as i32)),
                Some(Datum::Null) => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("DATE_SUBTRACT requires timestamp or date".into())),
            }
        }
        ScalarFunc::ClockTimestamp => {
            // CLOCK_TIMESTAMP() 鈫?current timestamp (real-time clock)
            let now = chrono::Utc::now();
            let us = now.timestamp() * 1_000_000 + now.timestamp_subsec_micros() as i64;
            Ok(Datum::Timestamp(us))
        }
        ScalarFunc::StatementTimestamp => {
            // STATEMENT_TIMESTAMP() 鈫?same as NOW() for us (statement start time)
            let now = chrono::Utc::now();
            let us = now.timestamp() * 1_000_000 + now.timestamp_subsec_micros() as i64;
            Ok(Datum::Timestamp(us))
        }
        ScalarFunc::Timeofday => {
            // TIMEOFDAY() 鈫?current date/time as text string
            let now = chrono::Local::now();
            Ok(Datum::Text(now.format("%a %b %d %H:%M:%S%.6f %Y %Z").to_string()))
        }
        ScalarFunc::MakeDate => {
            // MAKE_DATE(year, month, day) 鈫?date (days since epoch)
            let year = match args.first() {
                Some(Datum::Int32(n)) => *n,
                Some(Datum::Int64(n)) => *n as i32,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MAKE_DATE requires year".into())),
            };
            let month = match args.get(1) {
                Some(Datum::Int32(n)) => *n as u32,
                Some(Datum::Int64(n)) => *n as u32,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MAKE_DATE requires month".into())),
            };
            let day = match args.get(2) {
                Some(Datum::Int32(n)) => *n as u32,
                Some(Datum::Int64(n)) => *n as u32,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MAKE_DATE requires day".into())),
            };
            use chrono::NaiveDate;
            let date = NaiveDate::from_ymd_opt(year, month, day)
                .ok_or_else(|| ExecutionError::TypeError(format!("Invalid date: {}-{}-{}", year, month, day)))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = (date - epoch).num_days() as i32;
            Ok(Datum::Date(days))
        }
        ScalarFunc::MakeTimestamp => {
            // MAKE_TIMESTAMP(year, month, day, hour, min, sec) 鈫?timestamp
            let year = match args.first() {
                Some(Datum::Int32(n)) => *n,
                Some(Datum::Int64(n)) => *n as i32,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MAKE_TIMESTAMP requires year".into())),
            };
            let month = match args.get(1) {
                Some(Datum::Int32(n)) => *n as u32,
                Some(Datum::Int64(n)) => *n as u32,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MAKE_TIMESTAMP requires month".into())),
            };
            let day = match args.get(2) {
                Some(Datum::Int32(n)) => *n as u32,
                Some(Datum::Int64(n)) => *n as u32,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MAKE_TIMESTAMP requires day".into())),
            };
            let hour = match args.get(3) {
                Some(Datum::Int32(n)) => *n as u32,
                Some(Datum::Int64(n)) => *n as u32,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MAKE_TIMESTAMP requires hour".into())),
            };
            let min = match args.get(4) {
                Some(Datum::Int32(n)) => *n as u32,
                Some(Datum::Int64(n)) => *n as u32,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MAKE_TIMESTAMP requires min".into())),
            };
            let sec = match args.get(5) {
                Some(Datum::Int32(n)) => *n as u32,
                Some(Datum::Int64(n)) => *n as u32,
                Some(Datum::Float64(n)) => *n as u32,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MAKE_TIMESTAMP requires sec".into())),
            };
            use chrono::NaiveDate;
            let date = NaiveDate::from_ymd_opt(year, month, day)
                .ok_or_else(|| ExecutionError::TypeError("Invalid date".into()))?;
            let dt = date.and_hms_opt(hour, min, sec)
                .ok_or_else(|| ExecutionError::TypeError("Invalid time".into()))?;
            let us = dt.and_utc().timestamp() * 1_000_000;
            Ok(Datum::Timestamp(us))
        }
        ScalarFunc::ToTimestamp => {
            // TO_TIMESTAMP(epoch_seconds) 鈫?timestamp
            let epoch = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TO_TIMESTAMP requires numeric".into())),
            };
            let us = (epoch * 1_000_000.0) as i64;
            Ok(Datum::Timestamp(us))
        }
        ScalarFunc::Age => {
            // AGE(timestamp1) 鈫?interval from timestamp to now
            // AGE(timestamp1, timestamp2) 鈫?interval ts1 - ts2 (like PG: first - second)
            let ts1_us = match args.first() {
                Some(Datum::Timestamp(t)) => *t,
                Some(Datum::Date(d)) => *d as i64 * 86400 * 1_000_000,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("AGE requires timestamp or date".into())),
            };
            let ts2_us: i64 = if args.len() > 1 {
                match args.get(1) {
                    Some(Datum::Timestamp(t)) => *t,
                    Some(Datum::Date(d)) => *d as i64 * 86400 * 1_000_000,
                    Some(Datum::Null) => return Ok(Datum::Null),
                    _ => return Err(ExecutionError::TypeError("AGE requires timestamp or date".into())),
                }
            } else {
                // now in microseconds since epoch
                let now = chrono::Utc::now();
                now.timestamp() * 1_000_000 + now.timestamp_subsec_micros() as i64
            };
            let diff_us = ts1_us - ts2_us;
            let total_secs = diff_us.abs() / 1_000_000;
            let days = total_secs / 86400;
            let years = days / 365;
            let rem_days = days % 365;
            let months = rem_days / 30;
            let d = rem_days % 30;
            let hours = (total_secs % 86400) / 3600;
            let mins = (total_secs % 3600) / 60;
            let secs = total_secs % 60;
            let sign = if diff_us < 0 { "-" } else { "" };
            let mut parts = Vec::new();
            if years > 0 { parts.push(format!("{} year{}", years, if years != 1 { "s" } else { "" })); }
            if months > 0 { parts.push(format!("{} mon{}", months, if months != 1 { "s" } else { "" })); }
            if d > 0 { parts.push(format!("{} day{}", d, if d != 1 { "s" } else { "" })); }
            parts.push(format!("{:02}:{:02}:{:02}", hours, mins, secs));
            Ok(Datum::Text(format!("{}{}", sign, parts.join(" "))))
        }
        _ => unreachable!("dispatch_inner called with unhandled variant"),
    }
}

