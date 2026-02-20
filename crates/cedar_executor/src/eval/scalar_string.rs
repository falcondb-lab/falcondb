use cedar_common::datum::Datum;
use cedar_common::error::ExecutionError;
use cedar_sql_frontend::types::ScalarFunc;

/// Dispatch a string-domain scalar function.
pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::Upper => {
            match args.first() {
                Some(Datum::Text(s)) => Ok(Datum::Text(s.to_uppercase())),
                Some(Datum::Null) | None => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("UPPER requires text argument".into())),
            }
        }
        ScalarFunc::Lower => {
            match args.first() {
                Some(Datum::Text(s)) => Ok(Datum::Text(s.to_lowercase())),
                Some(Datum::Null) | None => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("LOWER requires text argument".into())),
            }
        }
        ScalarFunc::Length => {
            match args.first() {
                Some(Datum::Text(s)) => Ok(Datum::Int32(s.len() as i32)),
                Some(Datum::Null) | None => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("LENGTH requires text argument".into())),
            }
        }
        ScalarFunc::Substring => {
            // SUBSTRING(str, start[, length]) — 1-indexed
            let s = match args.first() {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SUBSTRING requires text first arg".into())),
            };
            let start = match args.get(1) {
                Some(Datum::Int32(n)) => (*n as usize).saturating_sub(1),
                Some(Datum::Int64(n)) => (*n as usize).saturating_sub(1),
                _ => 0,
            };
            let chars: Vec<char> = s.chars().collect();
            if start >= chars.len() {
                return Ok(Datum::Text(String::new()));
            }
            let len = match args.get(2) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                _ => chars.len() - start,
            };
            let result: String = chars[start..].iter().take(len).collect();
            Ok(Datum::Text(result))
        }
        ScalarFunc::Concat => {
            let mut result = String::new();
            for arg in args {
                match arg {
                    Datum::Text(s) => result.push_str(s),
                    Datum::Int32(n) => result.push_str(&n.to_string()),
                    Datum::Int64(n) => result.push_str(&n.to_string()),
                    Datum::Float64(f) => result.push_str(&f.to_string()),
                    Datum::Boolean(b) => result.push_str(&b.to_string()),
                    Datum::Null => {} // NULL is skipped in CONCAT
                    _ => result.push_str(&format!("{}", arg)),
                }
            }
            Ok(Datum::Text(result))
        }
        ScalarFunc::ConcatWs => {
            // CONCAT_WS(separator, val1, val2, ...) — concat with separator, skipping NULLs
            let sep = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("CONCAT_WS requires text separator".into())),
            };
            let parts: Vec<String> = args[1..].iter()
                .filter(|d| !d.is_null())
                .map(|d| format!("{}", d))
                .collect();
            Ok(Datum::Text(parts.join(&sep)))
        }
        ScalarFunc::Trim => {
            match args.first() {
                Some(Datum::Text(s)) => Ok(Datum::Text(s.trim().to_string())),
                Some(Datum::Null) | None => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("TRIM requires text argument".into())),
            }
        }
        ScalarFunc::Btrim => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("BTRIM requires text".into())),
            };
            let chars: Vec<char> = match args.get(1) {
                Some(Datum::Text(c)) => c.chars().collect(),
                _ => vec![' '],
            };
            let result = s.trim_matches(|c: char| chars.contains(&c)).to_string();
            Ok(Datum::Text(result))
        }
        ScalarFunc::Ltrim => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LTRIM requires text".into())),
            };
            let chars: Vec<char> = match args.get(1) {
                Some(Datum::Text(c)) => c.chars().collect(),
                _ => vec![' '],
            };
            let result = s.trim_start_matches(|c: char| chars.contains(&c)).to_string();
            Ok(Datum::Text(result))
        }
        ScalarFunc::Rtrim => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("RTRIM requires text".into())),
            };
            let chars: Vec<char> = match args.get(1) {
                Some(Datum::Text(c)) => c.chars().collect(),
                _ => vec![' '],
            };
            let result = s.trim_end_matches(|c: char| chars.contains(&c)).to_string();
            Ok(Datum::Text(result))
        }
        ScalarFunc::Replace => {
            // REPLACE(str, from, to)
            let s = match args.first() {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REPLACE requires text first arg".into())),
            };
            let from = match args.get(1) {
                Some(Datum::Text(s)) => s.as_str(),
                _ => return Err(ExecutionError::TypeError("REPLACE requires text second arg".into())),
            };
            let to = match args.get(2) {
                Some(Datum::Text(s)) => s.as_str(),
                _ => return Err(ExecutionError::TypeError("REPLACE requires text third arg".into())),
            };
            Ok(Datum::Text(s.replace(from, to)))
        }
        ScalarFunc::Position => {
            // POSITION(substr IN str) or STRPOS(str, substr) — returns 1-indexed position
            let haystack = match args.first() {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("POSITION requires text first arg".into())),
            };
            let needle = match args.get(1) {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("POSITION requires text second arg".into())),
            };
            match haystack.find(needle) {
                Some(pos) => Ok(Datum::Int32((pos + 1) as i32)),
                None => Ok(Datum::Int32(0)),
            }
        }
        ScalarFunc::Lpad => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LPAD requires text first arg".into())),
            };
            let len = match args.get(1) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LPAD requires integer length".into())),
            };
            let fill = match args.get(2) {
                Some(Datum::Text(f)) => f.clone(),
                None => " ".to_string(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LPAD fill must be text".into())),
            };
            let char_len = s.chars().count();
            if char_len >= len {
                Ok(Datum::Text(s.chars().take(len).collect()))
            } else {
                let needed = len - char_len;
                let fill_chars: Vec<char> = fill.chars().collect();
                let mut pad = String::new();
                for i in 0..needed {
                    pad.push(fill_chars[i % fill_chars.len()]);
                }
                Ok(Datum::Text(format!("{}{}", pad, s)))
            }
        }
        ScalarFunc::Rpad => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("RPAD requires text first arg".into())),
            };
            let len = match args.get(1) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("RPAD requires integer length".into())),
            };
            let fill = match args.get(2) {
                Some(Datum::Text(f)) => f.clone(),
                None => " ".to_string(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("RPAD fill must be text".into())),
            };
            let char_len = s.chars().count();
            if char_len >= len {
                Ok(Datum::Text(s.chars().take(len).collect()))
            } else {
                let needed = len - char_len;
                let fill_chars: Vec<char> = fill.chars().collect();
                let mut result = s;
                for i in 0..needed {
                    result.push(fill_chars[i % fill_chars.len()]);
                }
                Ok(Datum::Text(result))
            }
        }
        ScalarFunc::Left => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LEFT requires text".into())),
            };
            let n = match args.get(1) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LEFT requires integer".into())),
            };
            Ok(Datum::Text(s.chars().take(n).collect()))
        }
        ScalarFunc::Right => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("RIGHT requires text".into())),
            };
            let n = match args.get(1) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("RIGHT requires integer".into())),
            };
            let chars: Vec<char> = s.chars().collect();
            let start = if n >= chars.len() { 0 } else { chars.len() - n };
            Ok(Datum::Text(chars[start..].iter().collect()))
        }
        ScalarFunc::Repeat => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REPEAT requires text".into())),
            };
            let n = match args.get(1) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REPEAT requires integer".into())),
            };
            Ok(Datum::Text(s.repeat(n)))
        }
        ScalarFunc::Reverse => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REVERSE requires text".into())),
            };
            Ok(Datum::Text(s.chars().rev().collect()))
        }
        ScalarFunc::Initcap => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("INITCAP requires text".into())),
            };
            let mut result = String::new();
            let mut capitalize_next = true;
            for c in s.chars() {
                if c.is_alphanumeric() {
                    if capitalize_next {
                        result.extend(c.to_uppercase());
                        capitalize_next = false;
                    } else {
                        result.extend(c.to_lowercase());
                    }
                } else {
                    result.push(c);
                    capitalize_next = true;
                }
            }
            Ok(Datum::Text(result))
        }
        ScalarFunc::Translate => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TRANSLATE requires text".into())),
            };
            let from = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TRANSLATE requires text args".into())),
            };
            let to = match args.get(2) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TRANSLATE requires text args".into())),
            };
            let from_chars: Vec<char> = from.chars().collect();
            let to_chars: Vec<char> = to.chars().collect();
            let result: String = s.chars().filter_map(|c| {
                if let Some(pos) = from_chars.iter().position(|&fc| fc == c) {
                    if pos < to_chars.len() { Some(to_chars[pos]) } else { None }
                } else {
                    Some(c)
                }
            }).collect();
            Ok(Datum::Text(result))
        }
        ScalarFunc::Split => {
            // SPLIT_PART(string, delimiter, field) — 1-indexed
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SPLIT_PART requires text".into())),
            };
            let delim = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SPLIT_PART requires text delimiter".into())),
            };
            let field = match args.get(2) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SPLIT_PART requires integer field".into())),
            };
            let parts: Vec<&str> = s.split(&delim).collect();
            if field == 0 || field > parts.len() {
                Ok(Datum::Text(String::new()))
            } else {
                Ok(Datum::Text(parts[field - 1].to_string()))
            }
        }
        ScalarFunc::Overlay => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("OVERLAY requires text".into())),
            };
            let replacement = match args.get(1) {
                Some(Datum::Text(r)) => r.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("OVERLAY requires replacement text".into())),
            };
            let start = match args.get(2) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("OVERLAY requires start position".into())),
            };
            let count = match args.get(3) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                None => replacement.len(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("OVERLAY count must be integer".into())),
            };
            let start_idx = start.saturating_sub(1);
            let end_idx = (start_idx + count).min(s.len());
            let result = format!("{}{}{}", &s[..start_idx.min(s.len())], replacement, &s[end_idx..]);
            Ok(Datum::Text(result))
        }
        ScalarFunc::StartsWith => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("STARTS_WITH requires text".into())),
            };
            let prefix = match args.get(1) {
                Some(Datum::Text(p)) => p,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("STARTS_WITH requires prefix".into())),
            };
            Ok(Datum::Boolean(s.starts_with(prefix.as_str())))
        }
        ScalarFunc::EndsWith => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ENDS_WITH requires text".into())),
            };
            let suffix = match args.get(1) {
                Some(Datum::Text(p)) => p,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ENDS_WITH requires suffix".into())),
            };
            Ok(Datum::Boolean(s.ends_with(suffix.as_str())))
        }
        ScalarFunc::Chr => {
            let code = match args.first() {
                Some(Datum::Int32(n)) => *n as u32,
                Some(Datum::Int64(n)) => *n as u32,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("CHR requires integer".into())),
            };
            match char::from_u32(code) {
                Some(c) => Ok(Datum::Text(c.to_string())),
                None => Err(ExecutionError::TypeError(format!("CHR: invalid code point {}", code))),
            }
        }
        ScalarFunc::Ascii => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ASCII requires text".into())),
            };
            match s.chars().next() {
                Some(c) => Ok(Datum::Int32(c as i32)),
                None => Ok(Datum::Int32(0)),
            }
        }
        ScalarFunc::QuoteLiteral => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                other => format!("{}", other.unwrap()),
            };
            Ok(Datum::Text(format!("'{}'", s.replace('\'', "''"))))
        }
        ScalarFunc::QuoteIdent => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("QUOTE_IDENT requires text".into())),
            };
            Ok(Datum::Text(format!("\"{}\"", s.replace('"', "\"\""))))
        }
        ScalarFunc::QuoteNullable => {
            match args.first() {
                Some(Datum::Null) => Ok(Datum::Text("NULL".to_string())),
                Some(Datum::Text(s)) => Ok(Datum::Text(format!("'{}'", s.replace('\'', "''")))),
                Some(other) => Ok(Datum::Text(format!("'{}'", other))),
                None => Ok(Datum::Text("NULL".to_string())),
            }
        }
        ScalarFunc::BitLength => {
            match args.first() {
                Some(Datum::Text(s)) => Ok(Datum::Int64((s.len() * 8) as i64)),
                Some(Datum::Null) | None => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("BIT_LENGTH requires text".into())),
            }
        }
        ScalarFunc::OctetLength => {
            match args.first() {
                Some(Datum::Text(s)) => Ok(Datum::Int64(s.len() as i64)),
                Some(Datum::Null) | None => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("OCTET_LENGTH requires text".into())),
            }
        }
        _ => Err(ExecutionError::TypeError(format!("Not a string function: {:?}", func))),
    }
}
