use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::ScalarFunc;
use serde_json::Value as JsonValue;

/// Dispatch a JSONB-domain scalar function.
pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::JsonbBuildObject => {
            if !args.len().is_multiple_of(2) {
                return Err(ExecutionError::TypeError(
                    "jsonb_build_object requires even number of arguments".into(),
                ));
            }
            let mut map = serde_json::Map::new();
            for chunk in args.chunks(2) {
                let key = match &chunk[0] {
                    Datum::Text(s) => s.clone(),
                    Datum::Null => return Ok(Datum::Null),
                    other => format!("{}", other),
                };
                let val = datum_to_json_value(&chunk[1]);
                map.insert(key, val);
            }
            Ok(Datum::Jsonb(JsonValue::Object(map)))
        }
        ScalarFunc::JsonbBuildArray => {
            let arr: Vec<JsonValue> = args.iter().map(datum_to_json_value).collect();
            Ok(Datum::Jsonb(JsonValue::Array(arr)))
        }
        ScalarFunc::JsonbTypeof => {
            let json = require_jsonb(args, 0, "jsonb_typeof")?;
            let type_name = match &json {
                JsonValue::Null => "null",
                JsonValue::Bool(_) => "boolean",
                JsonValue::Number(_) => "number",
                JsonValue::String(_) => "string",
                JsonValue::Array(_) => "array",
                JsonValue::Object(_) => "object",
            };
            Ok(Datum::Text(type_name.to_string()))
        }
        ScalarFunc::JsonbArrayLength => {
            let json = require_jsonb(args, 0, "jsonb_array_length")?;
            match &json {
                JsonValue::Array(arr) => Ok(Datum::Int32(arr.len() as i32)),
                JsonValue::Null => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError(
                    "jsonb_array_length requires a JSON array".into(),
                )),
            }
        }
        ScalarFunc::JsonbExtractPath | ScalarFunc::JsonbExtractPathText => {
            let as_text = matches!(func, ScalarFunc::JsonbExtractPathText);
            let mut json = require_jsonb(args, 0, "jsonb_extract_path")?;
            for arg in &args[1..] {
                let key = match arg {
                    Datum::Text(s) => s.clone(),
                    Datum::Null => return Ok(Datum::Null),
                    other => format!("{}", other),
                };
                json = match json {
                    JsonValue::Object(ref map) => match map.get(&key) {
                        Some(v) => v.clone(),
                        None => return Ok(Datum::Null),
                    },
                    JsonValue::Array(ref arr) => match key.parse::<usize>() {
                        Ok(idx) => match arr.get(idx) {
                            Some(v) => v.clone(),
                            None => return Ok(Datum::Null),
                        },
                        Err(_) => return Ok(Datum::Null),
                    },
                    _ => return Ok(Datum::Null),
                };
            }
            if as_text {
                Ok(json_to_text(&json))
            } else {
                Ok(Datum::Jsonb(json))
            }
        }
        ScalarFunc::JsonbObjectKeys => {
            let json = require_jsonb(args, 0, "jsonb_object_keys")?;
            match &json {
                JsonValue::Object(map) => {
                    let keys: Vec<Datum> = map.keys().map(|k| Datum::Text(k.clone())).collect();
                    Ok(Datum::Array(keys))
                }
                _ => Err(ExecutionError::TypeError(
                    "jsonb_object_keys requires a JSON object".into(),
                )),
            }
        }
        ScalarFunc::JsonbPretty => {
            let json = require_jsonb(args, 0, "jsonb_pretty")?;
            Ok(Datum::Text(
                serde_json::to_string_pretty(&json).unwrap_or_default(),
            ))
        }
        ScalarFunc::JsonbStripNulls => {
            let json = require_jsonb(args, 0, "jsonb_strip_nulls")?;
            Ok(Datum::Jsonb(strip_nulls(&json)))
        }
        ScalarFunc::ToJsonb => match args.first() {
            Some(Datum::Jsonb(_)) => Ok(args[0].clone()),
            Some(Datum::Null) => Ok(Datum::Jsonb(JsonValue::Null)),
            Some(d) => Ok(Datum::Jsonb(datum_to_json_value(d))),
            None => Ok(Datum::Null),
        },
        ScalarFunc::JsonbConcat => {
            let a = require_jsonb(args, 0, "jsonb_concat")?;
            let b = require_jsonb(args, 1, "jsonb_concat")?;
            match (a, b) {
                (JsonValue::Object(mut am), JsonValue::Object(bm)) => {
                    for (k, v) in bm {
                        am.insert(k, v);
                    }
                    Ok(Datum::Jsonb(JsonValue::Object(am)))
                }
                (JsonValue::Array(mut aa), JsonValue::Array(ba)) => {
                    aa.extend(ba);
                    Ok(Datum::Jsonb(JsonValue::Array(aa)))
                }
                (a, b) => {
                    // Fall back to array concat
                    Ok(Datum::Jsonb(JsonValue::Array(vec![a, b])))
                }
            }
        }
        ScalarFunc::JsonbDeleteKey => {
            let mut json = require_jsonb(args, 0, "jsonb_delete_key")?;
            let key = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(other) => format!("{}", other),
                None => return Ok(Datum::Jsonb(json)),
            };
            match &mut json {
                JsonValue::Object(map) => {
                    map.remove(&key);
                }
                JsonValue::Array(arr) => {
                    arr.retain(|v| v.as_str() != Some(&key));
                }
                _ => {}
            }
            Ok(Datum::Jsonb(json))
        }
        ScalarFunc::JsonbDeletePath => {
            let json = require_jsonb(args, 0, "jsonb_delete_path")?;
            let path = match args.get(1) {
                Some(Datum::Array(arr)) => arr
                    .iter()
                    .map(|d| match d {
                        Datum::Text(s) => s.clone(),
                        other => format!("{}", other),
                    })
                    .collect::<Vec<_>>(),
                _ => return Ok(Datum::Jsonb(json)),
            };
            Ok(Datum::Jsonb(delete_path(json, &path)))
        }
        ScalarFunc::JsonbSetPath => {
            let mut json = require_jsonb(args, 0, "jsonb_set")?;
            let path = match args.get(1) {
                Some(Datum::Array(arr)) => arr
                    .iter()
                    .map(|d| match d {
                        Datum::Text(s) => s.clone(),
                        other => format!("{}", other),
                    })
                    .collect::<Vec<_>>(),
                _ => return Ok(Datum::Jsonb(json)),
            };
            let new_val = match args.get(2) {
                Some(Datum::Jsonb(v)) => v.clone(),
                Some(Datum::Text(s)) => {
                    serde_json::from_str(s).unwrap_or(JsonValue::String(s.clone()))
                }
                Some(d) => datum_to_json_value(d),
                None => JsonValue::Null,
            };
            set_path(&mut json, &path, new_val);
            Ok(Datum::Jsonb(json))
        }
        ScalarFunc::JsonbArrayElements | ScalarFunc::JsonbArrayElementsText => {
            let as_text = matches!(func, ScalarFunc::JsonbArrayElementsText);
            let json = require_jsonb(args, 0, "jsonb_array_elements")?;
            match json {
                JsonValue::Array(arr) => {
                    let elems: Vec<Datum> = arr
                        .iter()
                        .map(|v| {
                            if as_text {
                                json_to_text(v)
                            } else {
                                Datum::Jsonb(v.clone())
                            }
                        })
                        .collect();
                    Ok(Datum::Array(elems))
                }
                _ => Err(ExecutionError::TypeError(
                    "jsonb_array_elements requires a JSON array".into(),
                )),
            }
        }
        ScalarFunc::JsonbEach | ScalarFunc::JsonbEachText => {
            let as_text = matches!(func, ScalarFunc::JsonbEachText);
            let json = require_jsonb(args, 0, "jsonb_each")?;
            match json {
                JsonValue::Object(map) => {
                    let pairs: Vec<Datum> = map
                        .iter()
                        .flat_map(|(k, v)| {
                            let val = if as_text {
                                json_to_text(v)
                            } else {
                                Datum::Jsonb(v.clone())
                            };
                            vec![Datum::Text(k.clone()), val]
                        })
                        .collect();
                    Ok(Datum::Array(pairs))
                }
                _ => Err(ExecutionError::TypeError(
                    "jsonb_each requires a JSON object".into(),
                )),
            }
        }
        ScalarFunc::RowToJson => {
            // Simple: convert the first argument to JSON
            match args.first() {
                Some(d) => Ok(Datum::Jsonb(datum_to_json_value(d))),
                None => Ok(Datum::Null),
            }
        }
        _ => Err(ExecutionError::TypeError(format!(
            "Unknown JSONB function: {:?}",
            func
        ))),
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn require_jsonb(args: &[Datum], idx: usize, func_name: &str) -> Result<JsonValue, ExecutionError> {
    match args.get(idx) {
        Some(Datum::Jsonb(v)) => Ok(v.clone()),
        Some(Datum::Text(s)) => serde_json::from_str(s)
            .map_err(|e| ExecutionError::TypeError(format!("{}: invalid JSON: {}", func_name, e))),
        Some(Datum::Null) => Ok(JsonValue::Null),
        _ => Err(ExecutionError::TypeError(format!(
            "{} requires JSONB argument at position {}",
            func_name, idx
        ))),
    }
}

fn datum_to_json_value(d: &Datum) -> JsonValue {
    match d {
        Datum::Null => JsonValue::Null,
        Datum::Boolean(b) => JsonValue::Bool(*b),
        Datum::Int32(n) => JsonValue::Number((*n).into()),
        Datum::Int64(n) => JsonValue::Number((*n).into()),
        Datum::Float64(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Datum::Text(s) => JsonValue::String(s.clone()),
        Datum::Timestamp(us) => JsonValue::Number((*us).into()),
        Datum::Date(days) => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .expect("unix epoch date is always valid");
            if let Some(d) = epoch.checked_add_signed(chrono::Duration::days(*days as i64)) {
                JsonValue::String(d.format("%Y-%m-%d").to_string())
            } else {
                JsonValue::Number((*days).into())
            }
        }
        Datum::Array(arr) => JsonValue::Array(arr.iter().map(datum_to_json_value).collect()),
        Datum::Jsonb(v) => v.clone(),
        Datum::Decimal(m, s) => {
            let f = *m as f64 / 10f64.powi(*s as i32);
            serde_json::Number::from_f64(f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }
        Datum::Time(_) | Datum::Interval(_, _, _) | Datum::Uuid(_) => {
            JsonValue::String(format!("{}", d))
        }
        Datum::Bytea(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
            JsonValue::String(format!("\\x{}", hex))
        }
    }
}

fn json_to_text(v: &JsonValue) -> Datum {
    match v {
        JsonValue::Null => Datum::Null,
        JsonValue::String(s) => Datum::Text(s.clone()),
        other => Datum::Text(other.to_string()),
    }
}

fn strip_nulls(v: &JsonValue) -> JsonValue {
    match v {
        JsonValue::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (k, val) in map {
                if !val.is_null() {
                    new_map.insert(k.clone(), strip_nulls(val));
                }
            }
            JsonValue::Object(new_map)
        }
        JsonValue::Array(arr) => JsonValue::Array(arr.iter().map(strip_nulls).collect()),
        other => other.clone(),
    }
}

fn delete_path(mut json: JsonValue, path: &[String]) -> JsonValue {
    if path.is_empty() {
        return json;
    }
    if path.len() == 1 {
        match &mut json {
            JsonValue::Object(map) => {
                map.remove(&path[0]);
            }
            JsonValue::Array(arr) => {
                if let Ok(idx) = path[0].parse::<usize>() {
                    if idx < arr.len() {
                        arr.remove(idx);
                    }
                }
            }
            _ => {}
        }
        return json;
    }
    match &mut json {
        JsonValue::Object(map) => {
            if let Some(child) = map.remove(&path[0]) {
                let updated = delete_path(child, &path[1..]);
                map.insert(path[0].clone(), updated);
            }
        }
        JsonValue::Array(arr) => {
            if let Ok(idx) = path[0].parse::<usize>() {
                if idx < arr.len() {
                    let child = arr.remove(idx);
                    let updated = delete_path(child, &path[1..]);
                    arr.insert(idx, updated);
                }
            }
        }
        _ => {}
    }
    json
}

fn set_path(json: &mut JsonValue, path: &[String], val: JsonValue) {
    if path.is_empty() {
        *json = val;
        return;
    }
    if path.len() == 1 {
        match json {
            JsonValue::Object(map) => {
                map.insert(path[0].clone(), val);
            }
            JsonValue::Array(arr) => {
                if let Ok(idx) = path[0].parse::<usize>() {
                    if idx < arr.len() {
                        arr[idx] = val;
                    }
                }
            }
            _ => {}
        }
        return;
    }
    match json {
        JsonValue::Object(map) => {
            let entry = map
                .entry(path[0].clone())
                .or_insert(JsonValue::Object(serde_json::Map::new()));
            set_path(entry, &path[1..], val);
        }
        JsonValue::Array(arr) => {
            if let Ok(idx) = path[0].parse::<usize>() {
                if idx < arr.len() {
                    set_path(&mut arr[idx], &path[1..], val);
                }
            }
        }
        _ => {}
    }
}
