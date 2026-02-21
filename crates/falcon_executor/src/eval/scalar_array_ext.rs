#![allow(clippy::needless_range_loop)]

use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::ScalarFunc;

pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Option<Result<Datum, ExecutionError>> {
    match func {
        ScalarFunc::ArrayEvery |
        ScalarFunc::ArraySome |
        ScalarFunc::ArrayMin |
        ScalarFunc::ArrayMax |
        ScalarFunc::ArraySum |
        ScalarFunc::ArrayAvg |
        ScalarFunc::ArrayToSet |
        ScalarFunc::ArrayRepeat |
        ScalarFunc::ArrayRotate |
        ScalarFunc::ArraySample |
        ScalarFunc::ArrayShuffle |
        ScalarFunc::ArrayGenerate |
        ScalarFunc::ArrayToJson |
        ScalarFunc::ArrayPositions |
        ScalarFunc::ArrayZip |
        ScalarFunc::ArrayNdims |
        ScalarFunc::ArrayCompact |
        ScalarFunc::ArrayFlatten |
        ScalarFunc::ArrayIntersect |
        ScalarFunc::ArrayExcept |
        ScalarFunc::ArraySlice |
        ScalarFunc::ArrayFill => Some(dispatch_inner(func, args)),
        _ => None,
    }
}

fn dispatch_inner(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::ArrayEvery => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_EVERY requires array".into())),
            };
            for d in &arr {
                match d {
                    Datum::Boolean(false) => return Ok(Datum::Boolean(false)),
                    Datum::Null => return Ok(Datum::Null),
                    _ => {}
                }
            }
            Ok(Datum::Boolean(true))
        }
        ScalarFunc::ArraySome => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_SOME requires array".into())),
            };
            let mut has_null = false;
            for d in &arr {
                match d {
                    Datum::Boolean(true) => return Ok(Datum::Boolean(true)),
                    Datum::Null => has_null = true,
                    _ => {}
                }
            }
            if has_null { Ok(Datum::Null) } else { Ok(Datum::Boolean(false)) }
        }
        ScalarFunc::ArrayMin => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_MIN requires array".into())),
            };
            let mut min: Option<f64> = None;
            for d in &arr {
                let v = match d {
                    Datum::Int64(n) => *n as f64,
                    Datum::Int32(n) => *n as f64,
                    Datum::Float64(n) => *n,
                    _ => continue,
                };
                min = Some(min.map_or(v, |m: f64| m.min(v)));
            }
            match min {
                Some(v) => Ok(Datum::Float64(v)),
                None => Ok(Datum::Null),
            }
        }
        ScalarFunc::ArrayMax => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_MAX requires array".into())),
            };
            let mut max: Option<f64> = None;
            for d in &arr {
                let v = match d {
                    Datum::Int64(n) => *n as f64,
                    Datum::Int32(n) => *n as f64,
                    Datum::Float64(n) => *n,
                    _ => continue,
                };
                max = Some(max.map_or(v, |m: f64| m.max(v)));
            }
            match max {
                Some(v) => Ok(Datum::Float64(v)),
                None => Ok(Datum::Null),
            }
        }
        ScalarFunc::ArraySum => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_SUM requires array".into())),
            };
            let mut sum = 0.0f64;
            for d in &arr {
                match d {
                    Datum::Int64(n) => sum += *n as f64,
                    Datum::Int32(n) => sum += *n as f64,
                    Datum::Float64(n) => sum += n,
                    _ => {}
                }
            }
            Ok(Datum::Float64(sum))
        }
        ScalarFunc::ArrayAvg => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_AVG requires array".into())),
            };
            let mut sum = 0.0f64;
            let mut count = 0usize;
            for d in &arr {
                match d {
                    Datum::Int64(n) => { sum += *n as f64; count += 1; }
                    Datum::Int32(n) => { sum += *n as f64; count += 1; }
                    Datum::Float64(n) => { sum += n; count += 1; }
                    _ => {}
                }
            }
            if count == 0 { Ok(Datum::Null) } else { Ok(Datum::Float64(sum / count as f64)) }
        }
        ScalarFunc::ArrayToSet => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_TO_SET requires array".into())),
            };
            let mut seen = std::collections::HashSet::new();
            let mut result = Vec::new();
            for d in &arr {
                let key = format!("{}", d);
                if seen.insert(key) {
                    result.push(d.clone());
                }
            }
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArrayRepeat => {
            let elem = match args.first() {
                Some(d) => d.clone(),
                None => return Err(ExecutionError::TypeError("ARRAY_REPEAT requires element".into())),
            };
            if matches!(elem, Datum::Null) { return Ok(Datum::Null); }
            let count = match args.get(1) {
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_REPEAT requires count".into())),
            };
            Ok(Datum::Array(vec![elem; count]))
        }
        ScalarFunc::ArrayRotate => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_ROTATE requires array".into())),
            };
            if arr.is_empty() { return Ok(Datum::Array(arr)); }
            let n = match args.get(1) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_ROTATE requires integer".into())),
            };
            let len = arr.len() as i64;
            let rot = ((n % len) + len) % len;
            let mut result = arr[rot as usize..].to_vec();
            result.extend_from_slice(&arr[..rot as usize]);
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArraySample => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_SAMPLE requires array".into())),
            };
            let n = match args.get(1) {
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_SAMPLE requires count".into())),
            };
            let n = n.min(arr.len());
            use std::time::SystemTime;
            let seed = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default().as_nanos() as u64;
            let mut indices: Vec<usize> = (0..arr.len()).collect();
            let mut rng = seed;
            for i in 0..n {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let j = i + (rng as usize % (arr.len() - i));
                indices.swap(i, j);
            }
            let result: Vec<Datum> = indices[..n].iter().map(|&i| arr[i].clone()).collect();
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArrayShuffle => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_SHUFFLE requires array".into())),
            };
            use std::time::SystemTime;
            let seed = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default().as_nanos() as u64;
            let mut result = arr;
            let mut rng = seed;
            for i in (1..result.len()).rev() {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let j = rng as usize % (i + 1);
                result.swap(i, j);
            }
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArrayGenerate => {
            let start = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_GENERATE requires integer".into())),
            };
            let stop = match args.get(1) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_GENERATE requires integer".into())),
            };
            let step = match args.get(2) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                None => if start <= stop { 1 } else { -1 },
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_GENERATE step must be integer".into())),
            };
            if step == 0 {
                return Err(ExecutionError::TypeError("ARRAY_GENERATE step cannot be zero".into()));
            }
            let mut result = Vec::new();
            let mut i = start;
            if step > 0 {
                while i <= stop {
                    result.push(Datum::Int64(i));
                    i += step;
                }
            } else {
                while i >= stop {
                    result.push(Datum::Int64(i));
                    i += step;
                }
            }
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArrayToJson => {
            fn datum_to_json(d: &Datum) -> String {
                match d {
                    Datum::Null => "null".to_string(),
                    Datum::Boolean(b) => if *b { "true".to_string() } else { "false".to_string() },
                    Datum::Int32(n) => n.to_string(),
                    Datum::Int64(n) => n.to_string(),
                    Datum::Float64(n) => format!("{}", n),
                    Datum::Text(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
                    Datum::Timestamp(us) => {
                        let secs = us / 1_000_000;
                        let nsecs = ((us % 1_000_000) * 1000) as u32;
                        if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                            format!("\"{}\"", dt.format("%Y-%m-%dT%H:%M:%S"))
                        } else {
                            format!("{}", us)
                        }
                    }
                    Datum::Array(arr) => {
                        let elems: Vec<String> = arr.iter().map(datum_to_json).collect();
                        format!("[{}]", elems.join(","))
                    }
                    Datum::Date(days) => {
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        if let Some(d) = epoch.checked_add_signed(chrono::Duration::days(*days as i64)) {
                            format!("\"{}\"", d.format("%Y-%m-%d"))
                        } else {
                            format!("{}", days)
                        }
                    }
                    Datum::Jsonb(v) => v.to_string(),
                    Datum::Decimal(m, s) => falcon_common::datum::decimal_to_string(*m, *s),
                }
            }
            match args.first() {
                Some(Datum::Array(arr)) => {
                    let elems: Vec<String> = arr.iter().map(datum_to_json).collect();
                    Ok(Datum::Text(format!("[{}]", elems.join(","))))
                }
                Some(Datum::Null) => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("ARRAY_TO_JSON requires array".into())),
            }
        }
        ScalarFunc::ArrayPositions => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_POSITIONS requires array".into())),
            };
            let elem = match args.get(1) {
                Some(d) => d.clone(),
                None => return Err(ExecutionError::TypeError("ARRAY_POSITIONS requires element".into())),
            };
            let target = format!("{}", elem);
            let positions: Vec<Datum> = arr.iter().enumerate()
                .filter(|(_, d)| format!("{}", d) == target)
                .map(|(i, _)| Datum::Int64((i + 1) as i64))
                .collect();
            Ok(Datum::Array(positions))
        }
        ScalarFunc::ArrayZip => {
            let arr1 = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_ZIP requires array".into())),
            };
            let arr2 = match args.get(1) {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_ZIP requires array".into())),
            };
            let len = arr1.len().max(arr2.len());
            let mut result = Vec::new();
            for i in 0..len {
                let v1 = arr1.get(i).cloned().unwrap_or(Datum::Null);
                let v2 = arr2.get(i).cloned().unwrap_or(Datum::Null);
                result.push(Datum::Array(vec![v1, v2]));
            }
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArrayNdims => {
            fn count_dims(d: &Datum) -> i64 {
                match d {
                    Datum::Array(a) => {
                        if a.is_empty() { 1 } else { 1 + count_dims(&a[0]) }
                    }
                    _ => 0,
                }
            }
            match args.first() {
                Some(Datum::Array(_)) => Ok(Datum::Int64(count_dims(&args[0]))),
                Some(Datum::Null) => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("ARRAY_NDIMS requires array".into())),
            }
        }
        ScalarFunc::ArrayCompact => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_COMPACT requires array".into())),
            };
            let result: Vec<Datum> = arr.into_iter().filter(|d| !d.is_null()).collect();
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArrayFlatten => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_FLATTEN requires array".into())),
            };
            let mut result = Vec::new();
            for item in arr {
                match item {
                    Datum::Array(inner) => result.extend(inner),
                    other => result.push(other),
                }
            }
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArrayIntersect => {
            let arr1 = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_INTERSECT requires array".into())),
            };
            let arr2 = match args.get(1) {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_INTERSECT requires array".into())),
            };
            let arr2_keys: Vec<String> = arr2.iter().map(|d| format!("{:?}", d)).collect();
            let result: Vec<Datum> = arr1.into_iter()
                .filter(|item| arr2_keys.contains(&format!("{:?}", item)))
                .collect();
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArrayExcept => {
            let arr1 = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_EXCEPT requires array".into())),
            };
            let arr2 = match args.get(1) {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_EXCEPT requires array".into())),
            };
            let arr2_keys: Vec<String> = arr2.iter().map(|d| format!("{:?}", d)).collect();
            let result: Vec<Datum> = arr1.into_iter()
                .filter(|item| !arr2_keys.contains(&format!("{:?}", item)))
                .collect();
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArraySlice => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_SLICE requires array".into())),
            };
            let start = match args.get(1) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_SLICE requires start".into())),
            };
            let end = match args.get(2) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_SLICE requires end".into())),
            };
            let start_idx = start.saturating_sub(1);
            let end_idx = end.min(arr.len());
            if start_idx >= arr.len() || start_idx >= end_idx {
                Ok(Datum::Array(vec![]))
            } else {
                Ok(Datum::Array(arr[start_idx..end_idx].to_vec()))
            }
        }
        ScalarFunc::ArrayFill => {
            let value = match args.first() {
                Some(v) => v.clone(),
                None => return Err(ExecutionError::TypeError("ARRAY_FILL requires value".into())),
            };
            let count = match args.get(1) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_FILL requires count".into())),
            };
            Ok(Datum::Array(vec![value; count]))
        }
        _ => unreachable!("dispatch_inner called with unhandled variant"),
    }
}
