use falcon_common::datum::Datum;
use falcon_common::types::ShardId;
use falcon_planner::PlannedTxnType;
use falcon_txn::{SlowPathMode, TxnClassification};

pub(crate) fn classification_from_routing_hint(hint: &falcon_planner::TxnRoutingHint) -> TxnClassification {
    match hint.planned_txn_type() {
        PlannedTxnType::Local => {
            let shard = hint.involved_shards.first().copied().unwrap_or(ShardId(0));
            TxnClassification::local(shard)
        }
        PlannedTxnType::Global => {
            let shards = if hint.involved_shards.is_empty() {
                vec![ShardId(0)]
            } else {
                hint.involved_shards.clone()
            };
            TxnClassification::global(shards, SlowPathMode::Xa2Pc)
        }
    }
}

/// Extract a simple `WHERE col = 'value'` from a lowercased SQL string.
/// Returns the value if found, None otherwise.
pub(crate) fn extract_where_eq(sql: &str, column: &str) -> Option<String> {
    // Look for patterns like: column = 'value' or column='value'
    let pattern = format!("{column} = '");
    let pattern2 = format!("{column}='");
    let start = sql
        .find(&pattern)
        .map(|i| i + pattern.len())
        .or_else(|| sql.find(&pattern2).map(|i| i + pattern2.len()))?;
    let rest = &sql[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

/// Parse `SET log_min_duration_statement = <ms>` or `SET log_min_duration_statement TO <ms>`.
/// Also accepts `-1` or `default` to disable (returns 0).
/// Input `sql` must already be lowercased.
pub(crate) fn parse_set_log_min_duration(sql: &str) -> Option<u64> {
    let rest = sql.strip_prefix("set")?;
    let rest = rest.trim();
    let rest = rest.strip_prefix("log_min_duration_statement")?;
    let rest = rest.trim();
    let rest = if let Some(r) = rest.strip_prefix('=') {
        r.trim()
    } else if let Some(r) = rest.strip_prefix("to") {
        r.trim()
    } else {
        return None;
    };
    let value = rest
        .trim_end_matches(';')
        .trim()
        .trim_matches('\'')
        .trim_matches('"');
    if value == "default" || value == "-1" || value == "0" {
        return Some(0);
    }
    value.parse::<u64>().ok()
}

/// Parse `SET <var> = <value>` or `SET <var> TO <value>`.
/// Input `sql` must already be lowercased.
/// Returns (var_name, value) if successfully parsed.
pub(crate) fn parse_set_command(sql: &str) -> Option<(String, String)> {
    let rest = sql.strip_prefix("set")?.trim();
    // Skip LOCAL/SESSION qualifiers
    let rest = rest.strip_prefix("local ").unwrap_or(rest);
    let rest = rest.strip_prefix("session ").unwrap_or(rest);
    // Find the variable name (everything before = or TO)
    let (name, rest) = if let Some(eq_pos) = rest.find('=') {
        (rest[..eq_pos].trim(), rest[eq_pos + 1..].trim())
    } else if let Some(to_pos) = rest.find(" to ") {
        (rest[..to_pos].trim(), rest[to_pos + 4..].trim())
    } else {
        return None;
    };
    if name.is_empty() {
        return None;
    }
    let value = rest
        .trim_end_matches(';')
        .trim()
        .trim_matches('\'')
        .trim_matches('"');
    Some((name.to_owned(), value.to_owned()))
}

/// Parse `PREPARE name [(type, ...)] AS query`.
/// Returns (name, query) if successfully parsed.
pub(crate) fn parse_prepare_statement(sql: &str) -> Option<(String, String)> {
    let lower = sql.to_lowercase();
    let rest = lower.strip_prefix("prepare")?.trim();
    // Find AS keyword
    let as_pos = rest.find(" as ")?;
    let before_as = rest[..as_pos].trim();
    let query = sql[sql.to_lowercase().find(" as ")? + 4..]
        .trim()
        .trim_end_matches(';')
        .trim();
    // before_as is "name" or "name(type, ...)"
    let name = before_as.find('(').map_or_else(|| before_as.trim(), |paren| before_as[..paren].trim());
    if name.is_empty() || query.is_empty() {
        return None;
    }
    Some((name.to_owned(), query.to_owned()))
}

/// Parse `EXECUTE name [(param, ...)]`.
/// Returns (name, params) where params are converted to Option<Vec<u8>> for bind_params.
pub(crate) fn parse_execute_statement(sql: &str) -> Option<(String, Vec<Option<Vec<u8>>>)> {
    let rest = sql
        .trim()
        .strip_prefix("EXECUTE")
        .or_else(|| sql.trim().strip_prefix("execute"))?
        .trim();
    let rest = rest.trim_end_matches(';').trim();
    // Split name from optional (params)
    let (name, params_str) = rest.find('(').map_or_else(
        || (rest.trim(), None),
        |paren_pos| {
            let name = rest[..paren_pos].trim();
            let params_raw = rest[paren_pos + 1..].trim_end_matches(')').trim();
            (name, Some(params_raw))
        },
    );
    if name.is_empty() {
        return None;
    }
    let params = if let Some(ps) = params_str {
        if ps.is_empty() {
            vec![]
        } else {
            // Simple CSV split, respecting single-quoted strings
            split_params(ps)
                .into_iter()
                .map(|p| {
                    let trimmed = p.trim();
                    if trimmed.eq_ignore_ascii_case("null") {
                        None
                    } else {
                        let unquoted = trimmed.trim_matches('\'');
                        Some(unquoted.as_bytes().to_vec())
                    }
                })
                .collect()
        }
    } else {
        vec![]
    };
    Some((name.to_lowercase(), params))
}

/// Split a comma-separated parameter list, respecting single-quoted strings.
pub(crate) fn split_params(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for ch in s.chars() {
        match ch {
            '\'' => {
                in_quote = !in_quote;
                current.push(ch);
            }
            ',' if !in_quote => {
                parts.push(std::mem::take(&mut current));
            }
            _ => {
                current.push(ch);
            }
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

/// Substitute `$1`, `$2`, ... placeholders with parameter values (for SQL-level EXECUTE).
pub(crate) fn bind_params(sql: &str, param_values: &[Option<Vec<u8>>]) -> String {
    if param_values.is_empty() {
        return sql.to_owned();
    }
    let mut result = String::with_capacity(sql.len() + param_values.len() * 8);
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start {
                if let Ok(idx) = sql[start..end].parse::<usize>() {
                    if idx >= 1 && idx <= param_values.len() {
                        match &param_values[idx - 1] {
                            Some(val) => {
                                let s = String::from_utf8_lossy(val);
                                result.push('\'');
                                for ch in s.chars() {
                                    if ch == '\'' {
                                        result.push('\'');
                                    }
                                    result.push(ch);
                                }
                                result.push('\'');
                            }
                            None => result.push_str("NULL"),
                        }
                        i = end;
                        continue;
                    }
                }
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

/// Convert SQL-level EXECUTE text parameters to typed `Datum` values.
///
/// Uses inferred type hints from the prepared statement to parse text
/// values into the correct Datum variant, matching the behavior of the
/// extended query protocol's `decode_param_value`.
pub(crate) fn text_params_to_datum(
    params: &[Option<Vec<u8>>],
    type_hints: &[Option<falcon_common::types::DataType>],
) -> Vec<Datum> {
    use falcon_common::types::DataType;

    params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let hint = type_hints.get(i).and_then(|t| t.as_ref());
            let bytes = match p {
                Some(b) => b,
                None => return Datum::Null,
            };
            let s = String::from_utf8_lossy(bytes);
            match hint {
                Some(DataType::Int16) => s
                    .parse::<i16>()
                    .map_or_else(|_| Datum::Text(s.into_owned()), |v| Datum::Int32(v as i32)),
                Some(DataType::Int32) => s
                    .parse::<i32>()
                    .map_or_else(|_| Datum::Text(s.into_owned()), Datum::Int32),
                Some(DataType::Int64) => s
                    .parse::<i64>()
                    .map_or_else(|_| Datum::Text(s.into_owned()), Datum::Int64),
                Some(DataType::Float32) => s
                    .parse::<f32>()
                    .map_or_else(|_| Datum::Text(s.into_owned()), |v| Datum::Float64(v as f64)),
                Some(DataType::Float64) => s
                    .parse::<f64>()
                    .map_or_else(|_| Datum::Text(s.into_owned()), Datum::Float64),
                Some(DataType::Boolean) => match s.to_lowercase().as_str() {
                    "t" | "true" | "1" | "yes" | "on" => Datum::Boolean(true),
                    "f" | "false" | "0" | "no" | "off" => Datum::Boolean(false),
                    _ => Datum::Text(s.into_owned()),
                },
                Some(DataType::Decimal(_, _)) => {
                    Datum::parse_decimal(&s).unwrap_or_else(|| Datum::Text(s.into_owned()))
                }
                Some(DataType::Uuid) => {
                    let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
                    if hex.len() == 32 {
                        u128::from_str_radix(&hex, 16)
                            .map_or_else(|_| Datum::Text(s.into_owned()), Datum::Uuid)
                    } else {
                        Datum::Text(s.into_owned())
                    }
                }
                Some(DataType::Bytea) => {
                    let raw = s.strip_prefix("\\x").unwrap_or(&s);
                    let bytes: Vec<u8> = (0..raw.len())
                        .step_by(2)
                        .filter_map(|i| raw.get(i..i + 2).and_then(|h| u8::from_str_radix(h, 16).ok()))
                        .collect();
                    Datum::Bytea(bytes)
                }
                Some(DataType::Time) | Some(DataType::Interval)
                | Some(DataType::Timestamp) | Some(DataType::Date)
                | Some(DataType::Text) | Some(DataType::Array(_))
                | Some(DataType::Jsonb) => Datum::Text(s.into_owned()),
                _ => {
                    // No hint or text-like types: try integer, float, text
                    if let Ok(i) = s.parse::<i64>() {
                        Datum::Int64(i)
                    } else if let Ok(f) = s.parse::<f64>() {
                        Datum::Float64(f)
                    } else {
                        Datum::Text(s.into_owned())
                    }
                }
            }
        })
        .collect()
}
