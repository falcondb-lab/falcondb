use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::BinOp;
use serde_json::Value as JsonValue;

pub fn eval_binary_op(
    left: &Datum,
    op: BinOp,
    right: &Datum,
) -> Result<Datum, ExecutionError> {
    if left.is_null() || right.is_null() {
        return match op {
            BinOp::And => eval_and_null(left, right),
            BinOp::Or => eval_or_null(left, right),
            _ => Ok(Datum::Null),
        };
    }

    // Fast path: skip coercion when both sides are the same Datum variant.
    // This avoids 2 Datum clones for the common same-type case.
    // Only invoke coercion for cross-type pairs (Date vs Text, Int32 vs Int64, etc.).
    if std::mem::discriminant(left) != std::mem::discriminant(right) {
        let (cl, cr) = coerce_for_comparison(left, right);
        // Use eval_binary_op_same to avoid re-entering coercion (prevents infinite recursion
        // when coercion cannot unify the types).
        return eval_binary_op_same(&cl, op, &cr);
    }

    match op {
        BinOp::Eq => Ok(Datum::Boolean(left == right)),
        BinOp::NotEq => Ok(Datum::Boolean(left != right)),
        BinOp::Lt => Ok(Datum::Boolean(left < right)),
        BinOp::LtEq => Ok(Datum::Boolean(left <= right)),
        BinOp::Gt => Ok(Datum::Boolean(left > right)),
        BinOp::GtEq => Ok(Datum::Boolean(left >= right)),
        BinOp::And => {
            let lb = left
                .as_bool()
                .ok_or_else(|| ExecutionError::TypeError("AND requires boolean".into()))?;
            let rb = right
                .as_bool()
                .ok_or_else(|| ExecutionError::TypeError("AND requires boolean".into()))?;
            Ok(Datum::Boolean(lb && rb))
        }
        BinOp::Or => {
            let lb = left
                .as_bool()
                .ok_or_else(|| ExecutionError::TypeError("OR requires boolean".into()))?;
            let rb = right
                .as_bool()
                .ok_or_else(|| ExecutionError::TypeError("OR requires boolean".into()))?;
            Ok(Datum::Boolean(lb || rb))
        }
        BinOp::Plus => eval_arithmetic(left, right, |a, b| a + b, |a, b| a + b),
        BinOp::Minus => eval_arithmetic(left, right, |a, b| a - b, |a, b| a - b),
        BinOp::Multiply => eval_arithmetic(left, right, |a, b| a * b, |a, b| a * b),
        BinOp::Divide => {
            match right {
                Datum::Int32(0) | Datum::Int64(0) => {
                    return Err(ExecutionError::DivisionByZero);
                }
                Datum::Float64(f) if *f == 0.0 => {
                    return Err(ExecutionError::DivisionByZero);
                }
                _ => {}
            }
            eval_arithmetic(left, right, |a, b| a / b, |a, b| a / b)
        }
        BinOp::Modulo => {
            match right {
                Datum::Int32(0) | Datum::Int64(0) => {
                    return Err(ExecutionError::DivisionByZero);
                }
                _ => {}
            }
            eval_arithmetic(left, right, |a, b| a % b, |a, b| a % b)
        }
        // JSONB operators
        BinOp::JsonArrow => eval_json_arrow(left, right, false),
        BinOp::JsonArrowText => eval_json_arrow(left, right, true),
        BinOp::JsonHashArrow => eval_json_path(left, right, false),
        BinOp::JsonHashArrowText => eval_json_path(left, right, true),
        BinOp::JsonContains => eval_json_contains(left, right),
        BinOp::JsonContainedBy => eval_json_contains(right, left),
        BinOp::JsonExists => eval_json_exists(left, right),
        BinOp::StringConcat => match (left, right) {
            (Datum::Array(a), Datum::Array(b)) => {
                let mut result = a.clone();
                result.extend(b.iter().cloned());
                Ok(Datum::Array(result))
            }
            (Datum::Array(a), elem) if !elem.is_null() => {
                let mut result = a.clone();
                result.push(elem.clone());
                Ok(Datum::Array(result))
            }
            (elem, Datum::Array(b)) if !elem.is_null() => {
                let mut result = vec![elem.clone()];
                result.extend(b.iter().cloned());
                Ok(Datum::Array(result))
            }
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            (l, r) => {
                let ls = match l {
                    Datum::Text(s) => s.clone(),
                    other => format!("{other}"),
                };
                let rs = match r {
                    Datum::Text(s) => s.clone(),
                    other => format!("{other}"),
                };
                Ok(Datum::Text(format!("{ls}{rs}")))
            }
        },
    }
}

/// Inner binary op evaluation — called after coercion. Does NOT re-enter
/// coercion, preventing infinite recursion when types remain mismatched.
fn eval_binary_op_same(
    left: &Datum,
    op: BinOp,
    right: &Datum,
) -> Result<Datum, ExecutionError> {
    if left.is_null() || right.is_null() {
        return match op {
            BinOp::And => eval_and_null(left, right),
            BinOp::Or => eval_or_null(left, right),
            _ => Ok(Datum::Null),
        };
    }
    match op {
        BinOp::Eq => Ok(Datum::Boolean(left == right)),
        BinOp::NotEq => Ok(Datum::Boolean(left != right)),
        BinOp::Lt => Ok(Datum::Boolean(left < right)),
        BinOp::LtEq => Ok(Datum::Boolean(left <= right)),
        BinOp::Gt => Ok(Datum::Boolean(left > right)),
        BinOp::GtEq => Ok(Datum::Boolean(left >= right)),
        BinOp::And => {
            let lb = left
                .as_bool()
                .ok_or_else(|| ExecutionError::TypeError("AND requires boolean".into()))?;
            let rb = right
                .as_bool()
                .ok_or_else(|| ExecutionError::TypeError("AND requires boolean".into()))?;
            Ok(Datum::Boolean(lb && rb))
        }
        BinOp::Or => {
            let lb = left
                .as_bool()
                .ok_or_else(|| ExecutionError::TypeError("OR requires boolean".into()))?;
            let rb = right
                .as_bool()
                .ok_or_else(|| ExecutionError::TypeError("OR requires boolean".into()))?;
            Ok(Datum::Boolean(lb || rb))
        }
        BinOp::Plus => eval_arithmetic(left, right, |a, b| a + b, |a, b| a + b),
        BinOp::Minus => eval_arithmetic(left, right, |a, b| a - b, |a, b| a - b),
        BinOp::Multiply => eval_arithmetic(left, right, |a, b| a * b, |a, b| a * b),
        BinOp::Divide => {
            match right {
                Datum::Int32(0) | Datum::Int64(0) => {
                    return Err(ExecutionError::DivisionByZero);
                }
                Datum::Float64(f) if *f == 0.0 => {
                    return Err(ExecutionError::DivisionByZero);
                }
                _ => {}
            }
            eval_arithmetic(left, right, |a, b| a / b, |a, b| a / b)
        }
        BinOp::Modulo => {
            match right {
                Datum::Int32(0) | Datum::Int64(0) => {
                    return Err(ExecutionError::DivisionByZero);
                }
                _ => {}
            }
            eval_arithmetic(left, right, |a, b| a % b, |a, b| a % b)
        }
        BinOp::JsonArrow => eval_json_arrow(left, right, false),
        BinOp::JsonArrowText => eval_json_arrow(left, right, true),
        BinOp::JsonHashArrow => eval_json_path(left, right, false),
        BinOp::JsonHashArrowText => eval_json_path(left, right, true),
        BinOp::JsonContains => eval_json_contains(left, right),
        BinOp::JsonContainedBy => eval_json_contains(right, left),
        BinOp::JsonExists => eval_json_exists(left, right),
        BinOp::StringConcat => match (left, right) {
            (Datum::Array(a), Datum::Array(b)) => {
                let mut result = a.clone();
                result.extend(b.iter().cloned());
                Ok(Datum::Array(result))
            }
            (Datum::Array(a), elem) if !elem.is_null() => {
                let mut result = a.clone();
                result.push(elem.clone());
                Ok(Datum::Array(result))
            }
            (elem, Datum::Array(b)) if !elem.is_null() => {
                let mut result = vec![elem.clone()];
                result.extend(b.iter().cloned());
                Ok(Datum::Array(result))
            }
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            (l, r) => {
                let ls = match l {
                    Datum::Text(s) => s.clone(),
                    other => format!("{other}"),
                };
                let rs = match r {
                    Datum::Text(s) => s.clone(),
                    other => format!("{other}"),
                };
                Ok(Datum::Text(format!("{ls}{rs}")))
            }
        },
    }
}

fn eval_arithmetic(
    left: &Datum,
    right: &Datum,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Result<Datum, ExecutionError> {
    match (left, right) {
        (Datum::Int32(a), Datum::Int32(b)) => Ok(Datum::Int64(int_op(i64::from(*a), i64::from(*b)))),
        (Datum::Int64(a), Datum::Int64(b)) => Ok(Datum::Int64(int_op(*a, *b))),
        (Datum::Int32(a), Datum::Int64(b)) => Ok(Datum::Int64(int_op(i64::from(*a), *b))),
        (Datum::Int64(a), Datum::Int32(b)) => Ok(Datum::Int64(int_op(*a, i64::from(*b)))),
        (Datum::Float64(a), Datum::Float64(b)) => Ok(Datum::Float64(float_op(*a, *b))),
        (Datum::Float64(a), Datum::Int32(b)) => Ok(Datum::Float64(float_op(*a, f64::from(*b)))),
        (Datum::Float64(a), Datum::Int64(b)) => Ok(Datum::Float64(float_op(*a, *b as f64))),
        (Datum::Int32(a), Datum::Float64(b)) => Ok(Datum::Float64(float_op(f64::from(*a), *b))),
        (Datum::Int64(a), Datum::Float64(b)) => Ok(Datum::Float64(float_op(*a as f64, *b))),
        _ => Err(ExecutionError::TypeError(format!(
            "Cannot perform arithmetic on {left:?} and {right:?}"
        ))),
    }
}

const fn eval_and_null(left: &Datum, right: &Datum) -> Result<Datum, ExecutionError> {
    if matches!(left.as_bool(), Some(false)) {
        return Ok(Datum::Boolean(false));
    }
    if matches!(right.as_bool(), Some(false)) {
        return Ok(Datum::Boolean(false));
    }
    Ok(Datum::Null)
}

const fn eval_or_null(left: &Datum, right: &Datum) -> Result<Datum, ExecutionError> {
    if matches!(left.as_bool(), Some(true)) {
        return Ok(Datum::Boolean(true));
    }
    if matches!(right.as_bool(), Some(true)) {
        return Ok(Datum::Boolean(true));
    }
    Ok(Datum::Null)
}

// ── JSONB operator helpers ──────────────────────────────────────────────

/// Convert a Datum to a serde_json::Value for JSONB operations.
fn datum_to_json(d: &Datum) -> Result<JsonValue, ExecutionError> {
    match d {
        Datum::Jsonb(v) => Ok(v.clone()),
        Datum::Text(s) => serde_json::from_str(s)
            .map_err(|e| ExecutionError::TypeError(format!("invalid JSON: {e}"))),
        _ => Err(ExecutionError::TypeError(format!(
            "cannot use {d:?} as JSONB"
        ))),
    }
}

/// Convert a serde_json::Value back to Datum.
fn json_to_datum(v: &JsonValue) -> Datum {
    Datum::Jsonb(v.clone())
}

/// Convert a serde_json::Value to a text Datum.
fn json_to_text_datum(v: &JsonValue) -> Datum {
    match v {
        JsonValue::Null => Datum::Null,
        JsonValue::String(s) => Datum::Text(s.clone()),
        other => Datum::Text(other.to_string()),
    }
}

/// `->` (as_text=false) or `->>` (as_text=true): index by key or array index.
fn eval_json_arrow(left: &Datum, right: &Datum, as_text: bool) -> Result<Datum, ExecutionError> {
    let json = datum_to_json(left)?;
    let result = match right {
        Datum::Text(key) => json.get(key.as_str()).cloned(),
        Datum::Int32(idx) => json.get(*idx as usize).cloned(),
        Datum::Int64(idx) => json.get(*idx as usize).cloned(),
        _ => {
            return Err(ExecutionError::TypeError(
                "JSONB -> operator requires text key or integer index".into(),
            ))
        }
    };
    Ok(result.map_or(Datum::Null, |v| {
        if as_text { json_to_text_datum(&v) } else { json_to_datum(&v) }
    }))
}

/// `#>` or `#>>`: extract nested path from a text array.
fn eval_json_path(left: &Datum, right: &Datum, as_text: bool) -> Result<Datum, ExecutionError> {
    let mut json = datum_to_json(left)?;
    let path_elems = match right {
        Datum::Array(arr) => arr
            .iter()
            .map(|d| match d {
                Datum::Text(s) => Ok(s.clone()),
                other => Ok(format!("{other}")),
            })
            .collect::<Result<Vec<_>, ExecutionError>>()?,
        Datum::Text(s) => {
            // Accept '{a,b,c}' PG-style path syntax
            let trimmed = s.trim_start_matches('{').trim_end_matches('}');
            trimmed.split(',').map(|p| p.trim().to_owned()).collect()
        }
        _ => {
            return Err(ExecutionError::TypeError(
                "JSONB #> operator requires text array path".into(),
            ))
        }
    };

    for elem in &path_elems {
        json = match json {
            JsonValue::Object(ref map) => match map.get(elem.as_str()) {
                Some(v) => v.clone(),
                None => return Ok(Datum::Null),
            },
            JsonValue::Array(ref arr) => match elem.parse::<usize>() {
                Ok(idx) => match arr.get(idx) {
                    Some(v) => v.clone(),
                    None => return Ok(Datum::Null),
                },
                Err(_) => return Ok(Datum::Null),
            },
            _ => return Ok(Datum::Null),
        };
    }

    Ok(if as_text {
        json_to_text_datum(&json)
    } else {
        json_to_datum(&json)
    })
}

/// `@>`: left contains right (deep containment).
fn eval_json_contains(left: &Datum, right: &Datum) -> Result<Datum, ExecutionError> {
    let lhs = datum_to_json(left)?;
    let rhs = datum_to_json(right)?;
    Ok(Datum::Boolean(json_contains(&lhs, &rhs)))
}

/// Recursive JSON containment check (PG @> semantics).
fn json_contains(container: &JsonValue, containee: &JsonValue) -> bool {
    match (container, containee) {
        (JsonValue::Object(lm), JsonValue::Object(rm)) => rm
            .iter()
            .all(|(k, rv)| lm.get(k).is_some_and(|lv| json_contains(lv, rv))),
        (JsonValue::Array(la), JsonValue::Array(ra)) => ra
            .iter()
            .all(|rv| la.iter().any(|lv| json_contains(lv, rv))),
        (a, b) => a == b,
    }
}

/// Coerce mismatched types for comparison operators.
/// When one side is Date/Timestamp and the other is Text, cast the Text to match.
fn coerce_for_comparison(left: &Datum, right: &Datum) -> (Datum, Datum) {
    use super::cast::eval_cast;
    match (left, right) {
        (Datum::Date(_), Datum::Text(_)) => {
            if let Ok(r) = eval_cast(right.clone(), "date") {
                return (left.clone(), r);
            }
        }
        (Datum::Text(_), Datum::Date(_)) => {
            if let Ok(l) = eval_cast(left.clone(), "date") {
                return (l, right.clone());
            }
        }
        (Datum::Timestamp(_), Datum::Text(_)) => {
            if let Ok(r) = eval_cast(right.clone(), "timestamp") {
                return (left.clone(), r);
            }
        }
        (Datum::Text(_), Datum::Timestamp(_)) => {
            if let Ok(l) = eval_cast(left.clone(), "timestamp") {
                return (l, right.clone());
            }
        }
        // Int32 vs Int64: promote Int32 to Int64
        (Datum::Int32(a), Datum::Int64(_)) => {
            return (Datum::Int64(i64::from(*a)), right.clone());
        }
        (Datum::Int64(_), Datum::Int32(b)) => {
            return (left.clone(), Datum::Int64(i64::from(*b)));
        }
        _ => {}
    }
    (left.clone(), right.clone())
}

/// `?`: key/element exists in JSONB object or array.
fn eval_json_exists(left: &Datum, right: &Datum) -> Result<Datum, ExecutionError> {
    let json = datum_to_json(left)?;
    let key = match right {
        Datum::Text(s) => s.clone(),
        other => format!("{other}"),
    };
    let exists = match &json {
        JsonValue::Object(map) => map.contains_key(&key),
        JsonValue::Array(arr) => arr.iter().any(|v| v.as_str() == Some(&key)),
        _ => false,
    };
    Ok(Datum::Boolean(exists))
}
