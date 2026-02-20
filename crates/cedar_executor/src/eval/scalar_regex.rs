use cedar_common::datum::Datum;
use cedar_common::error::ExecutionError;
use cedar_sql_frontend::types::ScalarFunc;

/// Dispatch a regex-domain scalar function.
pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::RegexpReplace => {
            // REGEXP_REPLACE(source, pattern, replacement [, flags])
            let source = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_REPLACE requires text".into())),
            };
            let pattern = match args.get(1) {
                Some(Datum::Text(p)) => p.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_REPLACE requires pattern".into())),
            };
            let replacement = match args.get(2) {
                Some(Datum::Text(r)) => r.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_REPLACE requires replacement".into())),
            };
            let flags = match args.get(3) {
                Some(Datum::Text(f)) => f.clone(),
                _ => String::new(),
            };
            let re = regex::Regex::new(&pattern)
                .map_err(|e| ExecutionError::TypeError(format!("Invalid regex: {}", e)))?;
            let result = if flags.contains('g') {
                re.replace_all(&source, replacement.as_str()).to_string()
            } else {
                re.replace(&source, replacement.as_str()).to_string()
            };
            Ok(Datum::Text(result))
        }
        ScalarFunc::RegexpMatch => {
            // REGEXP_MATCH(string, pattern) → returns array of capture groups or NULL
            let source = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_MATCH requires text".into())),
            };
            let pattern = match args.get(1) {
                Some(Datum::Text(p)) => p.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_MATCH requires pattern".into())),
            };
            let re = regex::Regex::new(&pattern)
                .map_err(|e| ExecutionError::TypeError(format!("Invalid regex: {}", e)))?;
            match re.captures(&source) {
                Some(caps) => {
                    let matches: Vec<Datum> = caps.iter().skip(1).map(|m| {
                        match m {
                            Some(mat) => Datum::Text(mat.as_str().to_string()),
                            None => Datum::Null,
                        }
                    }).collect();
                    if matches.is_empty() {
                        // No capture groups; return full match
                        Ok(Datum::Array(vec![Datum::Text(caps.get(0).unwrap().as_str().to_string())]))
                    } else {
                        Ok(Datum::Array(matches))
                    }
                }
                None => Ok(Datum::Null),
            }
        }
        ScalarFunc::RegexpCount => {
            // REGEXP_COUNT(string, pattern [, flags])
            let source = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_COUNT requires text".into())),
            };
            let pattern = match args.get(1) {
                Some(Datum::Text(p)) => p.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_COUNT requires pattern".into())),
            };
            let re = regex::Regex::new(&pattern)
                .map_err(|e| ExecutionError::TypeError(format!("Invalid regex: {}", e)))?;
            Ok(Datum::Int64(re.find_iter(&source).count() as i64))
        }
        ScalarFunc::RegexpSubstr => {
            // REGEXP_SUBSTR(string, pattern) → first matching substring or NULL
            let source = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_SUBSTR requires text".into())),
            };
            let pattern = match args.get(1) {
                Some(Datum::Text(p)) => p.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_SUBSTR requires pattern".into())),
            };
            let re = regex::Regex::new(&pattern)
                .map_err(|e| ExecutionError::TypeError(format!("Invalid regex: {}", e)))?;
            match re.find(&source) {
                Some(m) => Ok(Datum::Text(m.as_str().to_string())),
                None => Ok(Datum::Null),
            }
        }
        ScalarFunc::RegexpSplitToArray => {
            // REGEXP_SPLIT_TO_ARRAY(string, pattern) → split string by regex into array
            let source = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_SPLIT_TO_ARRAY requires text".into())),
            };
            let pattern = match args.get(1) {
                Some(Datum::Text(p)) => p.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("REGEXP_SPLIT_TO_ARRAY requires pattern".into())),
            };
            let re = regex::Regex::new(&pattern)
                .map_err(|e| ExecutionError::TypeError(format!("Invalid regex: {}", e)))?;
            let parts: Vec<Datum> = re.split(&source).map(|s| Datum::Text(s.to_string())).collect();
            Ok(Datum::Array(parts))
        }
        _ => Err(ExecutionError::TypeError(format!("Not a regex function: {:?}", func))),
    }
}
