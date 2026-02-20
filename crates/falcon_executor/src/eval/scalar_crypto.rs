use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::ScalarFunc;
use sha2::{Digest, Sha256};

use super::common::expect_text_arg;

pub(crate) fn eval_md5(args: &[Datum]) -> Result<Datum, ExecutionError> {
    let s = match expect_text_arg(args, 0, "MD5")? {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };

    use md5::Digest as _;
    let hash = md5::Md5::digest(s.as_bytes());
    Ok(Datum::Text(format!("{:x}", hash)))
}

pub(crate) fn eval_sha256(args: &[Datum]) -> Result<Datum, ExecutionError> {
    let s = match expect_text_arg(args, 0, "SHA256")? {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };

    let hash = Sha256::digest(s.as_bytes());
    Ok(Datum::Text(format!("{:x}", hash)))
}

/// Dispatch a crypto/encoding-domain scalar function.
pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::Md5 => eval_md5(args),
        ScalarFunc::Sha256 => eval_sha256(args),
        ScalarFunc::Encode => {
            // ENCODE(data_text, format) — encode text to hex/base64
            let data = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ENCODE requires text".into())),
            };
            let fmt = match args.get(1) {
                Some(Datum::Text(f)) => f.to_lowercase(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("ENCODE requires format".into())),
            };
            match fmt.as_str() {
                "hex" => {
                    let hex: String = data.as_bytes().iter().map(|b| format!("{:02x}", b)).collect();
                    Ok(Datum::Text(hex))
                }
                "base64" => {
                    use base64::Engine;
                    Ok(Datum::Text(base64::engine::general_purpose::STANDARD.encode(data.as_bytes())))
                }
                _ => Err(ExecutionError::TypeError(format!("Unknown encoding: {}", fmt))),
            }
        }
        ScalarFunc::Decode => {
            // DECODE(encoded_text, format) — decode hex/base64 to text
            let data = match args.first() {
                Some(Datum::Text(s)) => s.clone(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DECODE requires text".into())),
            };
            let fmt = match args.get(1) {
                Some(Datum::Text(f)) => f.to_lowercase(),
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DECODE requires format".into())),
            };
            match fmt.as_str() {
                "hex" => {
                    let bytes: Result<Vec<u8>, _> = (0..data.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&data[i..i+2], 16))
                        .collect();
                    let bytes = bytes.map_err(|_| ExecutionError::TypeError("Invalid hex".into()))?;
                    Ok(Datum::Text(String::from_utf8_lossy(&bytes).to_string()))
                }
                "base64" => {
                    use base64::Engine;
                    let bytes = base64::engine::general_purpose::STANDARD.decode(data.as_bytes())
                        .map_err(|_| ExecutionError::TypeError("Invalid base64".into()))?;
                    Ok(Datum::Text(String::from_utf8_lossy(&bytes).to_string()))
                }
                _ => Err(ExecutionError::TypeError(format!("Unknown encoding: {}", fmt))),
            }
        }
        ScalarFunc::ToHex => {
            let n = match args.first() {
                Some(Datum::Int64(n)) => *n,
                Some(Datum::Int32(n)) => *n as i64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TO_HEX requires integer".into())),
            };
            Ok(Datum::Text(format!("{:x}", n)))
        }
        _ => Err(ExecutionError::TypeError(format!("Not a crypto function: {:?}", func))),
    }
}
