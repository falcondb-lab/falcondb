use cedar_common::datum::Datum;
use cedar_common::error::ExecutionError;
use cedar_sql_frontend::types::ScalarFunc;

/// Dispatch a core array-domain scalar function.
pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::ArrayLength | ScalarFunc::Cardinality => {
            match args.first() {
                Some(Datum::Array(arr)) => Ok(Datum::Int64(arr.len() as i64)),
                Some(Datum::Null) => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("ARRAY_LENGTH requires array".into())),
            }
        }
        ScalarFunc::ArrayPosition => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_POSITION requires array".into())),
            };
            let elem = match args.get(1) {
                Some(d) => d,
                None => return Err(ExecutionError::TypeError("ARRAY_POSITION requires element".into())),
            };
            for (i, v) in arr.iter().enumerate() {
                if v == elem { return Ok(Datum::Int64((i + 1) as i64)); }
            }
            Ok(Datum::Null)
        }
        ScalarFunc::ArrayAppend => {
            let mut arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => Vec::new(),
                _ => return Err(ExecutionError::TypeError("ARRAY_APPEND requires array".into())),
            };
            let elem = args.get(1).cloned().unwrap_or(Datum::Null);
            arr.push(elem);
            Ok(Datum::Array(arr))
        }
        ScalarFunc::ArrayPrepend => {
            let elem = args.first().cloned().unwrap_or(Datum::Null);
            let mut arr = match args.get(1) {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => Vec::new(),
                _ => return Err(ExecutionError::TypeError("ARRAY_PREPEND requires array".into())),
            };
            arr.insert(0, elem);
            Ok(Datum::Array(arr))
        }
        ScalarFunc::ArrayRemove => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_REMOVE requires array".into())),
            };
            let elem = args.get(1).unwrap_or(&Datum::Null);
            let result: Vec<Datum> = arr.iter().filter(|v| v != &elem).cloned().collect();
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArrayReplace => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_REPLACE requires array".into())),
            };
            let from = args.get(1).unwrap_or(&Datum::Null);
            let to = args.get(2).cloned().unwrap_or(Datum::Null);
            let result: Vec<Datum> = arr.iter().map(|v| if v == from { to.clone() } else { v.clone() }).collect();
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArrayCat => {
            let a = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => Vec::new(),
                _ => return Err(ExecutionError::TypeError("ARRAY_CAT requires array".into())),
            };
            let b = match args.get(1) {
                Some(Datum::Array(b)) => b.clone(),
                Some(Datum::Null) => Vec::new(),
                _ => return Err(ExecutionError::TypeError("ARRAY_CAT requires array".into())),
            };
            let mut result = a;
            result.extend(b);
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArrayToString => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_TO_STRING requires array".into())),
            };
            let delim = match args.get(1) {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                Some(other) => format!("{}", other),
                None => return Err(ExecutionError::TypeError("ARRAY_TO_STRING requires delimiter".into())),
            };
            let null_str = args.get(2).and_then(|d| match d {
                Datum::Text(s) => Some(s.clone()),
                Datum::Null => None,
                other => Some(format!("{}", other)),
            });
            let parts: Vec<String> = arr.iter().filter_map(|v| {
                if v.is_null() {
                    null_str.clone()
                } else {
                    Some(format!("{}", v))
                }
            }).collect();
            Ok(Datum::Text(parts.join(&delim)))
        }
        ScalarFunc::StringToArray => {
            let s = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("STRING_TO_ARRAY requires text".into())),
            };
            let delim = match args.get(1) {
                Some(Datum::Text(d)) => d.clone(),
                Some(Datum::Null) => return Ok(Datum::Array(vec![Datum::Text(s)])),
                Some(other) => format!("{}", other),
                None => return Err(ExecutionError::TypeError("STRING_TO_ARRAY requires delimiter".into())),
            };
            let null_str = args.get(2).and_then(|d| match d {
                Datum::Text(s) => Some(s.clone()),
                _ => None,
            });
            let parts: Vec<Datum> = s.split(&delim).map(|part| {
                if let Some(ref ns) = null_str {
                    if part == ns { Datum::Null } else { Datum::Text(part.to_string()) }
                } else {
                    Datum::Text(part.to_string())
                }
            }).collect();
            Ok(Datum::Array(parts))
        }
        ScalarFunc::ArrayUpper => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_UPPER requires array".into())),
            };
            Ok(Datum::Int64(arr.len() as i64))
        }
        ScalarFunc::ArrayLower => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_LOWER requires array".into())),
            };
            if arr.is_empty() {
                Ok(Datum::Null)
            } else {
                Ok(Datum::Int64(1))
            }
        }
        ScalarFunc::ArrayDims => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_DIMS requires array".into())),
            };
            if arr.is_empty() {
                Ok(Datum::Text("[]".to_string()))
            } else {
                Ok(Datum::Text(format!("[1:{}]", arr.len())))
            }
        }
        ScalarFunc::ArrayReverse => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_REVERSE requires array".into())),
            };
            let mut reversed = arr;
            reversed.reverse();
            Ok(Datum::Array(reversed))
        }
        ScalarFunc::ArrayDistinct => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_DISTINCT requires array".into())),
            };
            let mut seen = Vec::new();
            let mut result = Vec::new();
            for item in arr {
                let key = format!("{:?}", item);
                if !seen.contains(&key) {
                    seen.push(key);
                    result.push(item);
                }
            }
            Ok(Datum::Array(result))
        }
        ScalarFunc::ArraySort => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_SORT requires array".into())),
            };
            let mut sorted = arr;
            sorted.sort_by(|a, b| format!("{}", a).cmp(&format!("{}", b)));
            Ok(Datum::Array(sorted))
        }
        ScalarFunc::ArrayContains => {
            let arr = match args.first() {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_CONTAINS requires array".into())),
            };
            let elem = match args.get(1) {
                Some(e) => e,
                None => return Err(ExecutionError::TypeError("ARRAY_CONTAINS requires element".into())),
            };
            if elem.is_null() {
                return Ok(Datum::Null);
            }
            let found = arr.iter().any(|item| {
                format!("{:?}", item) == format!("{:?}", elem)
            });
            Ok(Datum::Boolean(found))
        }
        ScalarFunc::ArrayOverlap => {
            let arr1 = match args.first() {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_OVERLAP requires array".into())),
            };
            let arr2 = match args.get(1) {
                Some(Datum::Array(a)) => a,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ARRAY_OVERLAP requires array".into())),
            };
            let overlap = arr1.iter().any(|item1| {
                let k1 = format!("{:?}", item1);
                arr2.iter().any(|item2| format!("{:?}", item2) == k1)
            });
            Ok(Datum::Boolean(overlap))
        }
        _ => Err(ExecutionError::TypeError(format!("Not a core array function: {:?}", func))),
    }
}
