use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::ScalarFunc;

pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    if let Some(r) = super::scalar_array_ext::dispatch(func, args) { return r; }
    if let Some(r) = super::scalar_math_ext::dispatch(func, args) { return r; }
    if let Some(r) = super::scalar_time_ext::dispatch(func, args) { return r; }

    // Pattern-based generated functions
    match func {
        ScalarFunc::ArrayMatrixFunc(name) => dispatch_array_matrix(name, args),
        ScalarFunc::StringEncodingFunc { name, encode } => dispatch_string_encoding(name, *encode, args),
        _ => Err(ExecutionError::Internal(format!("Unimplemented scalar function: {:?}", func))),
    }
}

// ---------------------------------------------------------------------------
// ARRAY_MATRIX_{ROW|COLUMN}_{operation}{suffix}(arr, rows, cols, idx)
// ---------------------------------------------------------------------------

/// Extract a row slice from a flat array treated as a rows×cols matrix.
fn extract_row(values: &[f64], _rows: usize, cols: usize, row_idx: usize) -> Vec<f64> {
    let start = row_idx * cols;
    let end = (start + cols).min(values.len());
    if start >= values.len() { return Vec::new(); }
    values[start..end].to_vec()
}

/// Extract a column slice from a flat array treated as a rows×cols matrix.
fn extract_column(values: &[f64], rows: usize, cols: usize, col_idx: usize) -> Vec<f64> {
    (0..rows)
        .filter_map(|r| values.get(r * cols + col_idx).copied())
        .collect()
}

fn compute_operation(op: &str, suffix: u32, slice: &[f64]) -> Result<f64, ExecutionError> {
    if slice.is_empty() {
        return Ok(0.0);
    }
    let n = slice.len() as f64;
    match op {
        "RMS_RANGE" | "RANGE" => {
            // Range: max - min
            let min = slice.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = slice.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            Ok(max - min)
        }
        "ABS_NORM" => {
            // suffix < 46: L1 norm (sum of |v|); suffix >= 46: mean(|v|)
            let sum: f64 = slice.iter().map(|v| v.abs()).sum();
            if suffix >= 46 { Ok(sum / n) } else { Ok(sum) }
        }
        "ABS_DEV" => {
            if suffix >= 51 {
                // Stddev of absolute values: sqrt(var(|v|))
                let abs_vals: Vec<f64> = slice.iter().map(|v| v.abs()).collect();
                let mean = abs_vals.iter().sum::<f64>() / n;
                let variance = abs_vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
                Ok(variance.sqrt())
            } else {
                // Mean absolute deviation: mean(|v - mean|)
                let mean = slice.iter().sum::<f64>() / n;
                Ok(slice.iter().map(|v| (v - mean).abs()).sum::<f64>() / n)
            }
        }
        "LOG_RANGE" => {
            // Range of ln(|v|) for non-zero values
            let logs: Vec<f64> = slice.iter().filter(|v| **v != 0.0).map(|v| v.abs().ln()).collect();
            if logs.is_empty() { return Ok(0.0); }
            let min = logs.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = logs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            Ok(max - min)
        }
        "LOG_NORM" => {
            // Sum of ln(|v|) for non-zero values
            Ok(slice.iter().filter(|v| **v != 0.0).map(|v| v.abs().ln()).sum())
        }
        "RMS_DEV" => {
            // Population standard deviation: sqrt(mean((v - mean)^2))
            let mean = slice.iter().sum::<f64>() / n;
            let variance = slice.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
            Ok(variance.sqrt())
        }
        "ABS_RANGE" => {
            // Range of |v|: max(|v|) - min(|v|)
            let abs_vals: Vec<f64> = slice.iter().map(|v| v.abs()).collect();
            let min = abs_vals.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = abs_vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            Ok(max - min)
        }
        "RMS_NORM" => {
            // RMS: sqrt(mean(v^2))
            let mean_sq = slice.iter().map(|v| v * v).sum::<f64>() / n;
            Ok(mean_sq.sqrt())
        }
        "LOG_DEV" => {
            // Stddev of ln(|v|) for non-zero values
            let logs: Vec<f64> = slice.iter().filter(|v| **v != 0.0).map(|v| v.abs().ln()).collect();
            if logs.is_empty() { return Ok(0.0); }
            let ln = logs.len() as f64;
            let mean = logs.iter().sum::<f64>() / ln;
            let variance = logs.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / ln;
            Ok(variance.sqrt())
        }
        _ => Err(ExecutionError::Internal(format!("Unknown array matrix operation: {}", op))),
    }
}

fn dispatch_array_matrix(name: &str, args: &[Datum]) -> Result<Datum, ExecutionError> {
    // Parse: ARRAY_MATRIX_{ROW|COLUMN}_{operation}{suffix}
    let rest = name.strip_prefix("ARRAY_MATRIX_")
        .ok_or_else(|| ExecutionError::Internal(format!("Invalid array matrix func: {}", name)))?;

    let (is_row, op_part) = if let Some(r) = rest.strip_prefix("ROW_") {
        (true, r)
    } else if let Some(r) = rest.strip_prefix("COLUMN_") {
        (false, r)
    } else {
        return Err(ExecutionError::Internal(format!("Invalid array matrix func: {}", name)));
    };

    // Strip trailing numeric suffix to get the operation name
    let op = op_part.trim_end_matches(|c: char| c.is_ascii_digit());
    let suffix_str = &op_part[op.len()..];
    let suffix: u32 = suffix_str.parse().unwrap_or(0);

    // Args: (array, rows, cols, idx)
    let arr = match args.first() {
        Some(Datum::Array(a)) => a,
        Some(Datum::Null) => return Ok(Datum::Null),
        _ => return Err(ExecutionError::TypeError(format!("{} requires array as first arg", name))),
    };

    let values: Vec<f64> = arr.iter().map(|d| match d {
        Datum::Int64(n) => *n as f64,
        Datum::Int32(n) => *n as f64,
        Datum::Float64(f) => *f,
        _ => 0.0,
    }).collect();

    let rows = match args.get(1) {
        Some(Datum::Int64(n)) => *n as usize,
        Some(Datum::Int32(n)) => *n as usize,
        _ => return Err(ExecutionError::TypeError(format!("{} requires rows as second arg", name))),
    };
    let cols = match args.get(2) {
        Some(Datum::Int64(n)) => *n as usize,
        Some(Datum::Int32(n)) => *n as usize,
        _ => return Err(ExecutionError::TypeError(format!("{} requires cols as third arg", name))),
    };
    let idx = match args.get(3) {
        Some(Datum::Int64(n)) => *n as usize,
        Some(Datum::Int32(n)) => *n as usize,
        _ => return Err(ExecutionError::TypeError(format!("{} requires index as fourth arg", name))),
    };

    let slice = if is_row {
        extract_row(&values, rows, cols, idx)
    } else {
        extract_column(&values, rows, cols, idx)
    };

    let result = compute_operation(op, suffix, &slice)?;
    Ok(Datum::Float64(result))
}

// ---------------------------------------------------------------------------
// STRING_{name}_ENCODE(fields...) / STRING_{name}_DECODE(line, idx)
// ---------------------------------------------------------------------------

/// Map encoding name (e.g. "STRING_WWYV") to its delimiter character.
fn encoding_delimiter(name: &str) -> char {
    let code = name.strip_prefix("STRING_").unwrap_or("");
    match code {
        // YV family
        "WWYV" => '&', "XXYV" => '%', "YYYV" => '^', "ZZYV" => '~',
        // ZV family
        "AAZV" => '|', "BBZV" => ';', "CCZV" => ':', "DDZV" => '~',
        "EEZV" => '^', "FFZV" => '%', "GGZV" => '@', "HHZV" => '~',
        "IIZV" => '^', "JJZV" => '`', "KKZV" => '%', "LLZV" => '~',
        "MMZV" => '^', "NNZV" => '@', "OOZV" => '~', "PPZV" => '`',
        "QQZV" => '^', "RRZV" => '%', "SSZV" => '~', "TTZV" => '`',
        "UUZV" => '^', "VVZV" => '%', "WWZV" => '=', "XXZV" => '~',
        "YYZV" => '^', "ZZZV" => '`',
        // AW family
        "AAAW" => '%', "BBAW" => '~', "CCAW" => '^', "DDAW" => '=',
        "EEAW" => '%', "FFAW" => '~', "GGAW" => '=', "HHAW" => '|',
        "IIAW" => ':', "JJAW" => '~', "KKAW" => '^', "LLAW" => '@',
        "MMAW" => '%', "NNAW" => '~', "OOAW" => '^', "PPAW" => '=',
        "QQAW" => ':', "RRAW" => '~', "SSAW" => '^', "TTAW" => '=',
        "UUAW" => '%', "VVAW" => '~', "WWAW" => '^', "XXAW" => '@',
        "YYAW" => '=', "ZZAW" => '%',
        // BW family
        "AABW" => '|', "BBBW" => ':', "CCBW" => '~', "DDBW" => '^',
        "EEBW" => '@', "FFBW" => '=', "GGBW" => '%', "HHBW" => '|',
        "IIBW" => ':', "JJBW" => '~', "KKBW" => '^', "LLBW" => '@',
        "MMBW" => '=', "NNBW" => '%', "OOBW" => '|', "PPBW" => ':',
        "QQBW" => '~', "RRBW" => '^', "SSBW" => '@', "TTBW" => '=',
        "UUBW" => '%', "VVBW" => '|', "WWBW" => ':', "XXBW" => '~',
        "YYBW" => '^', "ZZBW" => '@',
        // CW family
        "AACW" => '=', "BBCW" => '%', "CCCW" => '|', "DDCW" => ':',
        "EECW" => '~', "FFCW" => '^', "GGCW" => '@', "HHCW" => '=',
        "IICW" => '%', "JJCW" => '|', "KKCW" => ':', "LLCW" => '~',
        "MMCW" => '^', "NNCW" => '@', "OOCW" => '=', "PPCW" => '%',
        "QQCW" => '|', "RRCW" => ':', "SSCW" => '~', "TTCW" => '^',
        "UUCW" => '@', "VVCW" => '=', "WWCW" => '%', "XXCW" => '|',
        "YYCW" => ':', "ZZCW" => '~',
        // DW family
        "AADW" => '+', "BBDW" => '*', "CCDW" => '#', "DDDW" => '&',
        "EEDW" => '!', "FFDW" => '~', "GGDW" => '^', "HHDW" => '@',
        "IIDW" => '=', "JJDW" => '%', "KKDW" => '|', "LLDW" => ':',
        "MMDW" => '+', "NNDW" => '*', "OODW" => '#', "PPDW" => '&',
        "QQDW" => '!', "RRDW" => '~', "SSDW" => '^', "TTDW" => '@',
        "UUDW" => '=', "VVDW" => '%', "WWDW" => '|',
        _ => '|', // fallback
    }
}

fn string_encode(delimiter: char, args: &[Datum]) -> Result<Datum, ExecutionError> {
    let parts: Vec<String> = args.iter().map(|d| match d {
        Datum::Text(s) => {
            // Escape backslashes and the delimiter
            s.replace('\\', "\\\\").replace(delimiter, &format!("\\{}", delimiter))
        }
        Datum::Null => String::new(),
        other => format!("{}", other),
    }).collect();
    Ok(Datum::Text(parts.join(&delimiter.to_string())))
}

fn string_decode(delimiter: char, args: &[Datum]) -> Result<Datum, ExecutionError> {
    let line = match args.first() {
        Some(Datum::Text(s)) => s.as_str(),
        Some(Datum::Null) => return Ok(Datum::Null),
        _ => return Err(ExecutionError::TypeError("STRING_*_DECODE requires text first arg".into())),
    };
    let idx = match args.get(1) {
        Some(Datum::Int64(n)) => *n as usize,
        Some(Datum::Int32(n)) => *n as usize,
        _ => return Err(ExecutionError::TypeError("STRING_*_DECODE requires index second arg".into())),
    };

    // Split by delimiter, respecting backslash escaping
    let mut fields: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in line.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == delimiter {
            fields.push(current);
            current = String::new();
        } else {
            current.push(ch);
        }
    }
    fields.push(current);

    match fields.get(idx) {
        Some(s) => Ok(Datum::Text(s.clone())),
        None => Ok(Datum::Null),
    }
}

fn dispatch_string_encoding(name: &str, encode: bool, args: &[Datum]) -> Result<Datum, ExecutionError> {
    let delimiter = encoding_delimiter(name);
    if encode {
        string_encode(delimiter, args)
    } else {
        string_decode(delimiter, args)
    }
}

