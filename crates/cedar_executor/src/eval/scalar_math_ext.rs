use cedar_common::datum::Datum;
use cedar_common::error::ExecutionError;
use cedar_sql_frontend::types::ScalarFunc;

pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Option<Result<Datum, ExecutionError>> {
    match func {
        ScalarFunc::Cbrt |
        ScalarFunc::Factorial |
        ScalarFunc::Gcd |
        ScalarFunc::Lcm |
        ScalarFunc::Sin |
        ScalarFunc::Cos |
        ScalarFunc::Tan |
        ScalarFunc::Asin |
        ScalarFunc::Acos |
        ScalarFunc::Atan |
        ScalarFunc::Atan2 |
        ScalarFunc::RegexpEscape |
        ScalarFunc::Clamp |
        ScalarFunc::OverlayArray |
        ScalarFunc::BitCount |
        ScalarFunc::BitNot |
        ScalarFunc::Asinh |
        ScalarFunc::Acosh |
        ScalarFunc::Atanh |
        ScalarFunc::BitXor |
        ScalarFunc::BitAnd |
        ScalarFunc::BitOr |
        ScalarFunc::Hashtext |
        ScalarFunc::Crc32 |
        ScalarFunc::TruncPrecision |
        ScalarFunc::RoundPrecision |
        ScalarFunc::RegexpLike |
        ScalarFunc::TrimScale |
        ScalarFunc::MinScale |
        ScalarFunc::Isfinite |
        ScalarFunc::Isinf |
        ScalarFunc::Div |
        ScalarFunc::Scale |
        ScalarFunc::Unnest |
        ScalarFunc::RegexpInstr |
        ScalarFunc::WidthBucket |
        ScalarFunc::Log10 |
        ScalarFunc::Log2 |
        ScalarFunc::Cot |
        ScalarFunc::Sinh |
        ScalarFunc::Cosh |
        ScalarFunc::Tanh |
        ScalarFunc::Isnan |
        ScalarFunc::StringToTable |
        ScalarFunc::Format |
        ScalarFunc::SubstrCount |
        ScalarFunc::Normalize |
        ScalarFunc::ParseIdent |
        ScalarFunc::Levenshtein |
        ScalarFunc::Soundex |
        ScalarFunc::Difference |
        ScalarFunc::Similarity |
        ScalarFunc::HammingDistance => Some(dispatch_inner(func, args)),
        _ => None,
    }
}

fn dispatch_inner(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::Cbrt => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("CBRT requires numeric".into())),
            };
            Ok(Datum::Float64(n.cbrt()))
        }
        ScalarFunc::Factorial => {
            let n = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("FACTORIAL requires integer".into())),
            };
            if n < 0 {
                return Err(ExecutionError::TypeError("FACTORIAL of negative number".into()));
            }
            let mut result: i64 = 1;
            for i in 2..=n {
                result = result.saturating_mul(i);
            }
            Ok(Datum::Int64(result))
        }
        ScalarFunc::Gcd => {
            let a = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("GCD requires integer".into())),
            };
            let b = match args.get(1) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("GCD requires integer".into())),
            };
            let (mut x, mut y) = (a.abs(), b.abs());
            while y != 0 {
                let t = y;
                y = x % y;
                x = t;
            }
            Ok(Datum::Int64(x))
        }
        ScalarFunc::Lcm => {
            let a = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LCM requires integer".into())),
            };
            let b = match args.get(1) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LCM requires integer".into())),
            };
            if a == 0 || b == 0 {
                return Ok(Datum::Int64(0));
            }
            let (mut x, mut y) = (a.abs(), b.abs());
            let product = x * y;
            while y != 0 {
                let t = y;
                y = x % y;
                x = t;
            }
            Ok(Datum::Int64((product / x).abs()))
        }
        ScalarFunc::Sin => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SIN requires numeric".into())),
            };
            Ok(Datum::Float64(n.sin()))
        }
        ScalarFunc::Cos => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("COS requires numeric".into())),
            };
            Ok(Datum::Float64(n.cos()))
        }
        ScalarFunc::Tan => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TAN requires numeric".into())),
            };
            Ok(Datum::Float64(n.tan()))
        }
        ScalarFunc::Asin => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ASIN requires numeric".into())),
            };
            Ok(Datum::Float64(n.asin()))
        }
        ScalarFunc::Acos => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ACOS requires numeric".into())),
            };
            Ok(Datum::Float64(n.acos()))
        }
        ScalarFunc::Atan => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ATAN requires numeric".into())),
            };
            Ok(Datum::Float64(n.atan()))
        }
        ScalarFunc::Atan2 => {
            let y = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ATAN2 requires numeric".into())),
            };
            let x = match args.get(1) {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ATAN2 requires numeric".into())),
            };
            Ok(Datum::Float64(y.atan2(x)))
        }
        ScalarFunc::RegexpEscape => {
            // REGEXP_ESCAPE(text) 鈫?text with regex special chars escaped
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_ESCAPE requires text".into())),
            };
            let escaped = regex::escape(&s);
            Ok(Datum::Text(escaped))
        }
        ScalarFunc::Clamp => {
            // CLAMP(value, min, max) 鈫?value clamped to [min, max]
            let val = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("CLAMP requires numeric".into())),
            };
            let min_val = match args.get(1) {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("CLAMP requires numeric min".into())),
            };
            let max_val = match args.get(2) {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("CLAMP requires numeric max".into())),
            };
            Ok(Datum::Float64(val.max(min_val).min(max_val)))
        }
        ScalarFunc::OverlayArray => {
            // OVERLAY_ARRAY(array, replacement, start [, count]) 鈫?splice array
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("OVERLAY_ARRAY requires array".into())),
            };
            let replacement = match args.get(1) {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("OVERLAY_ARRAY requires replacement array".into())),
            };
            let start = match args.get(2) {
                Some(Datum::Int64(n)) => (*n - 1).max(0) as usize,
                Some(Datum::Int32(n)) => (*n as i64 - 1).max(0) as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("OVERLAY_ARRAY requires start position".into())),
            };
            let count = match args.get(3) {
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Int32(n)) => *n as usize,
                _ => replacement.len(),
            };
            let mut result = Vec::new();
            result.extend_from_slice(&arr[..start.min(arr.len())]);
            result.extend(replacement);
            let skip = (start + count).min(arr.len());
            result.extend_from_slice(&arr[skip..]);
            Ok(Datum::Array(result))
        }
        ScalarFunc::BitCount => {
            // BIT_COUNT(integer) 鈫?number of set bits (popcount)
            let n = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("BIT_COUNT requires integer".into())),
            };
            Ok(Datum::Int64(n.count_ones() as i64))
        }
        ScalarFunc::BitNot => {
            // BIT_NOT(integer) 鈫?bitwise NOT
            let n = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("BIT_NOT requires integer".into())),
            };
            Ok(Datum::Int64(!n))
        }
        ScalarFunc::Asinh => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ASINH requires numeric".into())),
            };
            Ok(Datum::Float64(n.asinh()))
        }
        ScalarFunc::Acosh => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ACOSH requires numeric".into())),
            };
            if n < 1.0 {
                return Err(ExecutionError::TypeError("ACOSH requires value >= 1".into()));
            }
            Ok(Datum::Float64(n.acosh()))
        }
        ScalarFunc::Atanh => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ATANH requires numeric".into())),
            };
            if n.abs() >= 1.0 {
                return Err(ExecutionError::TypeError("ATANH requires -1 < value < 1".into()));
            }
            Ok(Datum::Float64(n.atanh()))
        }
        ScalarFunc::BitXor => {
            // BIT_XOR(a, b) 鈫?bitwise XOR
            let a = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("BIT_XOR requires integer".into())),
            };
            let b = match args.get(1) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("BIT_XOR requires integer".into())),
            };
            Ok(Datum::Int64(a ^ b))
        }
        ScalarFunc::BitAnd => {
            // BIT_AND_FUNC(a, b) 鈫?bitwise AND
            let a = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("BIT_AND requires integer".into())),
            };
            let b = match args.get(1) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("BIT_AND requires integer".into())),
            };
            Ok(Datum::Int64(a & b))
        }
        ScalarFunc::BitOr => {
            // BIT_OR_FUNC(a, b) 鈫?bitwise OR
            let a = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("BIT_OR requires integer".into())),
            };
            let b = match args.get(1) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("BIT_OR requires integer".into())),
            };
            Ok(Datum::Int64(a | b))
        }
        ScalarFunc::Hashtext => {
            // HASHTEXT(text) 鈫?integer hash of text
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("HASHTEXT requires text".into())),
            };
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            s.hash(&mut hasher);
            Ok(Datum::Int64(hasher.finish() as i64))
        }
        ScalarFunc::Crc32 => {
            // CRC32(text) 鈫?CRC32 checksum as integer
            let s = match args.first() {
                Some(Datum::Text(s)) => s.as_bytes().to_vec(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("CRC32 requires text".into())),
            };
            // Simple CRC32 implementation
            let mut crc: u32 = 0xFFFFFFFF;
            for byte in &s {
                crc ^= *byte as u32;
                for _ in 0..8 {
                    if crc & 1 != 0 {
                        crc = (crc >> 1) ^ 0xEDB88320;
                    } else {
                        crc >>= 1;
                    }
                }
            }
            Ok(Datum::Int64((crc ^ 0xFFFFFFFF) as i64))
        }
        ScalarFunc::TruncPrecision => {
            // TRUNC_PRECISION(number, places) 鈫?truncate to given decimal places
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TRUNC_PRECISION requires numeric".into())),
            };
            let places = match args.get(1) {
                Some(Datum::Int64(p)) => *p,
                Some(Datum::Int32(p)) => *p as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => 0,
            };
            let factor = 10f64.powi(places as i32);
            Ok(Datum::Float64((n * factor).trunc() / factor))
        }
        ScalarFunc::RoundPrecision => {
            // ROUND_PRECISION(number, places) 鈫?round to given decimal places
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ROUND_PRECISION requires numeric".into())),
            };
            let places = match args.get(1) {
                Some(Datum::Int64(p)) => *p,
                Some(Datum::Int32(p)) => *p as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => 0,
            };
            let factor = 10f64.powi(places as i32);
            Ok(Datum::Float64((n * factor).round() / factor))
        }
        ScalarFunc::RegexpLike => {
            // REGEXP_LIKE(string, pattern) 鈫?boolean
            let source = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_LIKE requires text".into())),
            };
            let pattern = match args.get(1) {
                Some(Datum::Text(p)) => p.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_LIKE requires pattern".into())),
            };
            let re = regex::Regex::new(&pattern)
                .map_err(|e| ExecutionError::TypeError(format!("Invalid regex: {}", e)))?;
            Ok(Datum::Boolean(re.is_match(&source)))
        }
        ScalarFunc::TrimScale => {
            // TRIM_SCALE(numeric) 鈫?remove trailing zeros from decimal
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => return Ok(Datum::Float64(*n as f64)),
                Some(Datum::Int32(n)) => return Ok(Datum::Float64(*n as f64)),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TRIM_SCALE requires numeric".into())),
            };
            // Format and remove trailing zeros
            let s = format!("{}", n);
            let trimmed = if s.contains('.') {
                s.trim_end_matches('0').trim_end_matches('.')
            } else {
                &s
            };
            Ok(Datum::Float64(trimmed.parse::<f64>().unwrap_or(n)))
        }
        ScalarFunc::MinScale => {
            // MIN_SCALE(numeric) 鈫?minimum scale needed to represent value
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(_)) | Some(Datum::Int32(_)) => return Ok(Datum::Int64(0)),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MIN_SCALE requires numeric".into())),
            };
            let s = format!("{}", n);
            let scale = if let Some(pos) = s.find('.') {
                let frac = s[pos+1..].trim_end_matches('0');
                frac.len()
            } else {
                0
            };
            Ok(Datum::Int64(scale as i64))
        }
        ScalarFunc::Isfinite => {
            // ISFINITE(numeric) 鈫?boolean
            let val = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(_)) | Some(Datum::Int32(_)) => return Ok(Datum::Boolean(true)),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ISFINITE requires numeric".into())),
            };
            Ok(Datum::Boolean(val.is_finite()))
        }
        ScalarFunc::Isinf => {
            // ISINF(numeric) 鈫?boolean
            let val = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(_)) | Some(Datum::Int32(_)) => return Ok(Datum::Boolean(false)),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ISINF requires numeric".into())),
            };
            Ok(Datum::Boolean(val.is_infinite()))
        }
        ScalarFunc::Div => {
            // DIV(a, b) 鈫?integer division (truncated toward zero)
            let a = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Float64(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DIV requires numeric".into())),
            };
            let b = match args.get(1) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Float64(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DIV requires numeric".into())),
            };
            if b == 0 {
                return Err(ExecutionError::TypeError("division by zero".into()));
            }
            Ok(Datum::Int64(a / b))
        }
        ScalarFunc::Scale => {
            // SCALE(numeric) 鈫?number of decimal digits in fractional part
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(_)) | Some(Datum::Int32(_)) => return Ok(Datum::Int64(0)),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SCALE requires numeric".into())),
            };
            let s = format!("{}", n);
            let scale = if let Some(pos) = s.find('.') {
                s.len() - pos - 1
            } else {
                0
            };
            Ok(Datum::Int64(scale as i64))
        }
        ScalarFunc::Unnest => {
            // UNNEST(array) 鈫?text representation of array elements (scalar approximation)
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("UNNEST requires array".into())),
            };
            let text: Vec<String> = arr.iter().map(|d| format!("{}", d)).collect();
            Ok(Datum::Text(text.join("\n")))
        }
        ScalarFunc::RegexpInstr => {
            // REGEXP_INSTR(string, pattern) 鈫?position of first match (1-indexed, 0 if none)
            let source = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_INSTR requires text".into())),
            };
            let pattern = match args.get(1) {
                Some(Datum::Text(p)) => p.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_INSTR requires pattern".into())),
            };
            let re = regex::Regex::new(&pattern)
                .map_err(|e| ExecutionError::TypeError(format!("Invalid regex: {}", e)))?;
            let pos = re.find(&source).map(|m| m.start() + 1).unwrap_or(0);
            Ok(Datum::Int64(pos as i64))
        }
        ScalarFunc::WidthBucket => {
            // WIDTH_BUCKET(value, low, high, count) 鈫?bucket number (0..count+1)
            let val = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("WIDTH_BUCKET requires numeric".into())),
            };
            let low = match args.get(1) {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("WIDTH_BUCKET requires low".into())),
            };
            let high = match args.get(2) {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("WIDTH_BUCKET requires high".into())),
            };
            let count = match args.get(3) {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("WIDTH_BUCKET requires count".into())),
            };
            let bucket = if val < low {
                0
            } else if val >= high {
                count + 1
            } else {
                let width = (high - low) / count as f64;
                ((val - low) / width).floor() as i64 + 1
            };
            Ok(Datum::Int64(bucket))
        }
        ScalarFunc::Log10 => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LOG10 requires numeric".into())),
            };
            Ok(Datum::Float64(n.log10()))
        }
        ScalarFunc::Log2 => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LOG2 requires numeric".into())),
            };
            Ok(Datum::Float64(n.log2()))
        }
        ScalarFunc::Cot => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("COT requires numeric".into())),
            };
            Ok(Datum::Float64(1.0 / n.tan()))
        }
        ScalarFunc::Sinh => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SINH requires numeric".into())),
            };
            Ok(Datum::Float64(n.sinh()))
        }
        ScalarFunc::Cosh => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("COSH requires numeric".into())),
            };
            Ok(Datum::Float64(n.cosh()))
        }
        ScalarFunc::Tanh => {
            let n = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TANH requires numeric".into())),
            };
            Ok(Datum::Float64(n.tanh()))
        }
        ScalarFunc::Isnan => {
            let val = match args.first() {
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Int64(_)) | Some(Datum::Int32(_)) => return Ok(Datum::Boolean(false)),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ISNAN requires numeric".into())),
            };
            Ok(Datum::Boolean(val.is_nan()))
        }
        ScalarFunc::StringToTable => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("STRING_TO_TABLE requires text".into())),
            };
            let delim = match args.get(1) {
                Some(Datum::Text(d)) => d.clone(),
                Some(Datum::Null) => {
                    let result: Vec<Datum> = s.chars().map(|c| Datum::Text(c.to_string())).collect();
                    return Ok(Datum::Array(result));
                }
                _ => return Err(ExecutionError::TypeError("STRING_TO_TABLE requires delimiter".into())),
            };
            let parts: Vec<Datum> = s.split(&delim).map(|p| Datum::Text(p.to_string())).collect();
            Ok(Datum::Array(parts))
        }
        ScalarFunc::Format => {
            if args.is_empty() {
                return Err(ExecutionError::TypeError("FORMAT requires at least one argument".into()));
            }
            let fmt = match &args[0] {
                Datum::Text(s) => s.clone(),
                Datum::Null => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("FORMAT requires text format string".into())),
            };
            let mut result = fmt.clone();
            for (i, arg) in args[1..].iter().enumerate() {
                let placeholder = format!("%{}", i + 1);
                let val_str = format!("{}", arg);
                result = result.replace(&placeholder, &val_str);
            }
            result = result.replace("%s", &args.get(1).map_or(String::new(), |a| format!("{}", a)));
            Ok(Datum::Text(result))
        }
        ScalarFunc::SubstrCount => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SUBSTR_COUNT requires text".into())),
            };
            let sub = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SUBSTR_COUNT requires text".into())),
            };
            if sub.is_empty() {
                return Ok(Datum::Int64(0));
            }
            Ok(Datum::Int64(s.matches(&sub).count() as i64))
        }
        ScalarFunc::Normalize => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("NORMALIZE requires text".into())),
            };
            Ok(Datum::Text(s))
        }
        ScalarFunc::ParseIdent => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("PARSE_IDENT requires text".into())),
            };
            let parts: Vec<Datum> = s.split('.').map(|p| {
                let trimmed = p.trim();
                let unquoted = if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
                    &trimmed[1..trimmed.len()-1]
                } else {
                    trimmed
                };
                Datum::Text(unquoted.to_string())
            }).collect();
            Ok(Datum::Array(parts))
        }
        ScalarFunc::Levenshtein => {
            let s1 = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LEVENSHTEIN requires text".into())),
            };
            let s2 = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LEVENSHTEIN requires text".into())),
            };
            let a: Vec<char> = s1.chars().collect();
            let b: Vec<char> = s2.chars().collect();
            let (m, n) = (a.len(), b.len());
            let mut dp = vec![vec![0usize; n + 1]; m + 1];
            for i in 0..=m { dp[i][0] = i; }
            for j in 0..=n { dp[0][j] = j; }
            for i in 1..=m {
                for j in 1..=n {
                    let cost = if a[i-1] == b[j-1] { 0 } else { 1 };
                    dp[i][j] = (dp[i-1][j] + 1).min(dp[i][j-1] + 1).min(dp[i-1][j-1] + cost);
                }
            }
            Ok(Datum::Int64(dp[m][n] as i64))
        }
        ScalarFunc::Soundex => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SOUNDEX requires text".into())),
            };
            if s.is_empty() { return Ok(Datum::Text(String::new())); }
            let upper = s.to_uppercase();
            let chars: Vec<char> = upper.chars().filter(|c| c.is_ascii_alphabetic()).collect();
            if chars.is_empty() { return Ok(Datum::Text(String::new())); }
            let code = |c: char| -> char {
                match c {
                    'B' | 'F' | 'P' | 'V' => '1',
                    'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
                    'D' | 'T' => '3',
                    'L' => '4',
                    'M' | 'N' => '5',
                    'R' => '6',
                    _ => '0',
                }
            };
            let mut result = String::new();
            result.push(chars[0]);
            let mut last_code = code(chars[0]);
            for &ch in &chars[1..] {
                let c = code(ch);
                if c != '0' && c != last_code {
                    result.push(c);
                    if result.len() == 4 { break; }
                }
                last_code = c;
            }
            while result.len() < 4 { result.push('0'); }
            Ok(Datum::Text(result))
        }
        ScalarFunc::Difference => {
            fn soundex_code(s: &str) -> String {
                if s.is_empty() { return String::new(); }
                let upper = s.to_uppercase();
                let chars: Vec<char> = upper.chars().filter(|c| c.is_ascii_alphabetic()).collect();
                if chars.is_empty() { return String::new(); }
                let code = |c: char| -> char {
                    match c {
                        'B' | 'F' | 'P' | 'V' => '1',
                        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
                        'D' | 'T' => '3', 'L' => '4', 'M' | 'N' => '5', 'R' => '6', _ => '0',
                    }
                };
                let mut result = String::new();
                result.push(chars[0]);
                let mut last_code = code(chars[0]);
                for &ch in &chars[1..] {
                    let c = code(ch);
                    if c != '0' && c != last_code { result.push(c); if result.len() == 4 { break; } }
                    last_code = c;
                }
                while result.len() < 4 { result.push('0'); }
                result
            }
            let s1 = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DIFFERENCE requires text".into())),
            };
            let s2 = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DIFFERENCE requires text".into())),
            };
            let sx1 = soundex_code(&s1);
            let sx2 = soundex_code(&s2);
            let similarity = sx1.chars().zip(sx2.chars()).filter(|(a, b)| a == b).count();
            Ok(Datum::Int64(similarity as i64))
        }
        ScalarFunc::Similarity => {
            let s1 = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SIMILARITY requires text".into())),
            };
            let s2 = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SIMILARITY requires text".into())),
            };
            fn trigrams(s: &str) -> std::collections::HashSet<String> {
                let padded = format!("  {} ", s.to_lowercase());
                let chars: Vec<char> = padded.chars().collect();
                let mut set = std::collections::HashSet::new();
                for w in chars.windows(3) {
                    set.insert(w.iter().collect::<String>());
                }
                set
            }
            let t1 = trigrams(&s1);
            let t2 = trigrams(&s2);
            let intersection = t1.intersection(&t2).count() as f64;
            let union = t1.union(&t2).count() as f64;
            if union == 0.0 { Ok(Datum::Float64(0.0)) } else { Ok(Datum::Float64(intersection / union)) }
        }
        ScalarFunc::HammingDistance => {
            let s1 = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("HAMMING_DISTANCE requires text".into())),
            };
            let s2 = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("HAMMING_DISTANCE requires text".into())),
            };
            let c1: Vec<char> = s1.chars().collect();
            let c2: Vec<char> = s2.chars().collect();
            let max_len = c1.len().max(c2.len());
            let mut dist = 0i64;
            for i in 0..max_len {
                let a = c1.get(i);
                let b = c2.get(i);
                if a != b { dist += 1; }
            }
            Ok(Datum::Int64(dist))
        }
        _ => unreachable!("dispatch_inner called with unhandled variant"),
    }
}
