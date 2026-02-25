use std::fmt::Write as _;

use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::ScalarFunc;

/// Dispatch a string-domain scalar function.
#[inline]
pub fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::Upper => match args.first() {
            Some(Datum::Text(s)) => Ok(Datum::Text(s.to_uppercase())),
            Some(Datum::Null) | None => Ok(Datum::Null),
            _ => Err(ExecutionError::TypeError(
                "UPPER requires text argument".into(),
            )),
        },
        ScalarFunc::Lower => match args.first() {
            Some(Datum::Text(s)) => Ok(Datum::Text(s.to_lowercase())),
            Some(Datum::Null) | None => Ok(Datum::Null),
            _ => Err(ExecutionError::TypeError(
                "LOWER requires text argument".into(),
            )),
        },
        ScalarFunc::Length => match args.first() {
            Some(Datum::Text(s)) => Ok(Datum::Int32(s.len() as i32)),
            Some(Datum::Null) | None => Ok(Datum::Null),
            _ => Err(ExecutionError::TypeError(
                "LENGTH requires text argument".into(),
            )),
        },
        ScalarFunc::Substring => {
            // SUBSTRING(str, start[, length]) — 1-indexed
            let s = match args.first() {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "SUBSTRING requires text first arg".into(),
                    ))
                }
            };
            let start = match args.get(1) {
                Some(Datum::Int32(n)) => (*n as usize).saturating_sub(1),
                Some(Datum::Int64(n)) => (*n as usize).saturating_sub(1),
                _ => 0,
            };
            // Avoid Vec<char>: find byte offset of char at `start` via char_indices.
            let byte_start = s.char_indices().nth(start).map(|(i, _)| i).unwrap_or(s.len());
            if byte_start >= s.len() {
                return Ok(Datum::Text(String::new()));
            }
            let tail = &s[byte_start..];
            let result: String = match args.get(2) {
                Some(Datum::Int32(n)) => tail.chars().take(*n as usize).collect(),
                Some(Datum::Int64(n)) => tail.chars().take(*n as usize).collect(),
                _ => tail.to_owned(),
            };
            Ok(Datum::Text(result))
        }
        ScalarFunc::Concat => {
            let mut result = String::new();
            for arg in args {
                match arg {
                    Datum::Text(s) => result.push_str(s),
                    Datum::Int32(n) => { let _ = write!(result, "{n}"); }
                    Datum::Int64(n) => { let _ = write!(result, "{n}"); }
                    Datum::Float64(f) => { let _ = write!(result, "{f}"); }
                    Datum::Boolean(b) => { let _ = write!(result, "{b}"); }
                    Datum::Null => {} // NULL is skipped in CONCAT
                    _ => { let _ = write!(result, "{arg}"); }
                }
            }
            Ok(Datum::Text(result))
        }
        ScalarFunc::ConcatWs => {
            // CONCAT_WS(separator, val1, val2, ...) — concat with separator, skipping NULLs
            let sep = match args.first() {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "CONCAT_WS requires text separator".into(),
                    ))
                }
            };
            // Avoid intermediate Vec<String>: write directly into result buffer.
            let mut result = String::new();
            let mut first = true;
            for arg in &args[1..] {
                if arg.is_null() { continue; }
                if !first { result.push_str(sep); }
                first = false;
                match arg {
                    Datum::Text(s) => result.push_str(s),
                    _ => { let _ = write!(result, "{arg}"); }
                }
            }
            Ok(Datum::Text(result))
        }
        ScalarFunc::Trim => match args.first() {
            Some(Datum::Text(s)) => Ok(Datum::Text(s.trim().to_owned())),
            Some(Datum::Null) | None => Ok(Datum::Null),
            _ => Err(ExecutionError::TypeError(
                "TRIM requires text argument".into(),
            )),
        },
        ScalarFunc::Btrim => {
            // Borrow &str directly — avoid clone before trim.
            let s = match args.first() {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("BTRIM requires text".into())),
            };
            let result = match args.get(1) {
                Some(Datum::Text(c)) => {
                    let chars: Vec<char> = c.chars().collect();
                    s.trim_matches(|ch: char| chars.contains(&ch)).to_owned()
                }
                _ => s.trim().to_owned(),
            };
            Ok(Datum::Text(result))
        }
        ScalarFunc::Ltrim => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LTRIM requires text".into())),
            };
            let result = match args.get(1) {
                Some(Datum::Text(c)) => {
                    let chars: Vec<char> = c.chars().collect();
                    s.trim_start_matches(|ch: char| chars.contains(&ch)).to_owned()
                }
                _ => s.trim_start().to_owned(),
            };
            Ok(Datum::Text(result))
        }
        ScalarFunc::Rtrim => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("RTRIM requires text".into())),
            };
            let result = match args.get(1) {
                Some(Datum::Text(c)) => {
                    let chars: Vec<char> = c.chars().collect();
                    s.trim_end_matches(|ch: char| chars.contains(&ch)).to_owned()
                }
                _ => s.trim_end().to_owned(),
            };
            Ok(Datum::Text(result))
        }
        ScalarFunc::Replace => {
            // REPLACE(str, from, to)
            let s = match args.first() {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "REPLACE requires text first arg".into(),
                    ))
                }
            };
            let from = match args.get(1) {
                Some(Datum::Text(s)) => s.as_str(),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "REPLACE requires text second arg".into(),
                    ))
                }
            };
            let to = match args.get(2) {
                Some(Datum::Text(s)) => s.as_str(),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "REPLACE requires text third arg".into(),
                    ))
                }
            };
            Ok(Datum::Text(s.replace(from, to)))
        }
        ScalarFunc::Position => {
            // POSITION(substr IN str) or STRPOS(str, substr) — returns 1-indexed position
            let haystack = match args.first() {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "POSITION requires text first arg".into(),
                    ))
                }
            };
            let needle = match args.get(1) {
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "POSITION requires text second arg".into(),
                    ))
                }
            };
            Ok(Datum::Int32(haystack.find(needle).map_or(0, |pos| (pos + 1) as i32)))
        }
        ScalarFunc::Lpad => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "LPAD requires text first arg".into(),
                    ))
                }
            };
            let len = match args.get(1) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "LPAD requires integer length".into(),
                    ))
                }
            };
            let fill = match args.get(2) {
                Some(Datum::Text(f)) => f.clone(),
                None => " ".to_owned(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LPAD fill must be text".into())),
            };
            let char_len = s.chars().count();
            if char_len >= len {
                Ok(Datum::Text(s.chars().take(len).collect()))
            } else {
                let needed = len - char_len;
                // Use cycle() iterator — avoids allocating fill_chars Vec.
                let fill_iter = fill.chars().cycle().take(needed);
                let pad: String = fill_iter.collect();
                let mut result = String::with_capacity(pad.len() + s.len());
                result.push_str(&pad);
                result.push_str(&s);
                Ok(Datum::Text(result))
            }
        }
        ScalarFunc::Rpad => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "RPAD requires text first arg".into(),
                    ))
                }
            };
            let len = match args.get(1) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "RPAD requires integer length".into(),
                    ))
                }
            };
            let fill = match args.get(2) {
                Some(Datum::Text(f)) => f.clone(),
                None => " ".to_owned(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("RPAD fill must be text".into())),
            };
            let char_len = s.chars().count();
            if char_len >= len {
                Ok(Datum::Text(s.chars().take(len).collect()))
            } else {
                let needed = len - char_len;
                let mut result = s;
                result.extend(fill.chars().cycle().take(needed));
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
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("RIGHT requires text".into())),
            };
            let n = match args.get(1) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("RIGHT requires integer".into())),
            };
            // Avoid Vec<char>: count chars, find byte offset of skip point.
            let char_count = s.chars().count();
            let skip = char_count.saturating_sub(n);
            let byte_start = s.char_indices().nth(skip).map(|(i, _)| i).unwrap_or(0);
            Ok(Datum::Text(s[byte_start..].to_owned()))
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
                Some(Datum::Text(s)) => s.as_str(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("INITCAP requires text".into())),
            };
            let mut result = String::with_capacity(s.len());
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
                _ => {
                    return Err(ExecutionError::TypeError(
                        "TRANSLATE requires text args".into(),
                    ))
                }
            };
            let to = match args.get(2) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "TRANSLATE requires text args".into(),
                    ))
                }
            };
            let from_chars: Vec<char> = from.chars().collect();
            let to_chars: Vec<char> = to.chars().collect();
            let result: String = s
                .chars()
                .filter_map(|c| {
                    from_chars.iter().position(|&fc| fc == c).map_or(
                        Some(c),
                        |pos| if pos < to_chars.len() { Some(to_chars[pos]) } else { None },
                    )
                })
                .collect();
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
                _ => {
                    return Err(ExecutionError::TypeError(
                        "SPLIT_PART requires text delimiter".into(),
                    ))
                }
            };
            let field = match args.get(2) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "SPLIT_PART requires integer field".into(),
                    ))
                }
            };
            let parts: Vec<&str> = s.split(&delim).collect();
            if field == 0 || field > parts.len() {
                Ok(Datum::Text(String::new()))
            } else {
                Ok(Datum::Text(parts[field - 1].to_owned()))
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
                _ => {
                    return Err(ExecutionError::TypeError(
                        "OVERLAY requires replacement text".into(),
                    ))
                }
            };
            let start = match args.get(2) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "OVERLAY requires start position".into(),
                    ))
                }
            };
            let count = match args.get(3) {
                Some(Datum::Int32(n)) => *n as usize,
                Some(Datum::Int64(n)) => *n as usize,
                None => replacement.len(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "OVERLAY count must be integer".into(),
                    ))
                }
            };
            let start_idx = start.saturating_sub(1);
            let end_idx = (start_idx + count).min(s.len());
            let result = format!(
                "{}{}{}",
                &s[..start_idx.min(s.len())],
                replacement,
                &s[end_idx..]
            );
            Ok(Datum::Text(result))
        }
        ScalarFunc::StartsWith => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "STARTS_WITH requires text".into(),
                    ))
                }
            };
            let prefix = match args.get(1) {
                Some(Datum::Text(p)) => p,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "STARTS_WITH requires prefix".into(),
                    ))
                }
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
                _ => {
                    return Err(ExecutionError::TypeError(
                        "ENDS_WITH requires suffix".into(),
                    ))
                }
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
                None => Err(ExecutionError::TypeError(format!(
                    "CHR: invalid code point {code}"
                ))),
            }
        }
        ScalarFunc::Ascii => match args.first() {
            Some(Datum::Text(s)) => Ok(Datum::Int32(
                s.chars().next().map_or(0, |c| c as i32),
            )),
            Some(Datum::Null) => Ok(Datum::Null),
            _ => Err(ExecutionError::TypeError("ASCII requires text".into())),
        },
        ScalarFunc::QuoteLiteral => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) | None => return Ok(Datum::Null),
                Some(other) => format!("{other}"),
            };
            Ok(Datum::Text(format!("'{}'", s.replace('\'', "''"))))
        }
        ScalarFunc::QuoteIdent => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => {
                    return Err(ExecutionError::TypeError(
                        "QUOTE_IDENT requires text".into(),
                    ))
                }
            };
            Ok(Datum::Text(format!("\"{}\"", s.replace('"', "\"\""))))
        }
        ScalarFunc::QuoteNullable => match args.first() {
            Some(Datum::Null) | None => Ok(Datum::Text("NULL".to_owned())),
            Some(Datum::Text(s)) => Ok(Datum::Text(format!("'{}'", s.replace('\'', "''")))),
            Some(other) => Ok(Datum::Text(format!("'{other}'"))),
        },
        ScalarFunc::BitLength => match args.first() {
            Some(Datum::Text(s)) => Ok(Datum::Int64((s.len() * 8) as i64)),
            Some(Datum::Null) | None => Ok(Datum::Null),
            _ => Err(ExecutionError::TypeError("BIT_LENGTH requires text".into())),
        },
        ScalarFunc::OctetLength => match args.first() {
            Some(Datum::Text(s)) => Ok(Datum::Int64(s.len() as i64)),
            Some(Datum::Null) | None => Ok(Datum::Null),
            _ => Err(ExecutionError::TypeError(
                "OCTET_LENGTH requires text".into(),
            )),
        },
        _ => Err(ExecutionError::TypeError(format!(
            "Not a string function: {func:?}"
        ))),
    }
}
