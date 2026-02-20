use cedar_common::datum::Datum;
use cedar_common::error::ExecutionError;
use cedar_sql_frontend::types::ScalarFunc;

/// Dispatch a math-domain scalar function.
pub(crate) fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::Abs => {
            match args.first() {
                Some(Datum::Int32(n)) => Ok(Datum::Int32(n.abs())),
                Some(Datum::Int64(n)) => Ok(Datum::Int64(n.abs())),
                Some(Datum::Float64(f)) => Ok(Datum::Float64(f.abs())),
                Some(Datum::Null) | None => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("ABS requires numeric argument".into())),
            }
        }
        ScalarFunc::Round => {
            match args.first() {
                Some(Datum::Float64(f)) => Ok(Datum::Float64(f.round())),
                Some(Datum::Int32(n)) => Ok(Datum::Int32(*n)),
                Some(Datum::Int64(n)) => Ok(Datum::Int64(*n)),
                Some(Datum::Null) | None => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("ROUND requires numeric argument".into())),
            }
        }
        ScalarFunc::Ceil | ScalarFunc::Ceiling => {
            match args.first() {
                Some(Datum::Float64(f)) => Ok(Datum::Float64(f.ceil())),
                Some(Datum::Int32(n)) => Ok(Datum::Int32(*n)),
                Some(Datum::Int64(n)) => Ok(Datum::Int64(*n)),
                Some(Datum::Null) | None => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("CEIL requires numeric argument".into())),
            }
        }
        ScalarFunc::Floor => {
            match args.first() {
                Some(Datum::Float64(f)) => Ok(Datum::Float64(f.floor())),
                Some(Datum::Int32(n)) => Ok(Datum::Int32(*n)),
                Some(Datum::Int64(n)) => Ok(Datum::Int64(*n)),
                Some(Datum::Null) | None => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("FLOOR requires numeric argument".into())),
            }
        }
        ScalarFunc::Power => {
            let base = match args.first() {
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("POWER requires numeric".into())),
            };
            let exp = match args.get(1) {
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("POWER requires numeric exponent".into())),
            };
            Ok(Datum::Float64(base.powf(exp)))
        }
        ScalarFunc::Sqrt => {
            let val = match args.first() {
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("SQRT requires numeric".into())),
            };
            Ok(Datum::Float64(val.sqrt()))
        }
        ScalarFunc::Sign => {
            match args.first() {
                Some(Datum::Int32(n)) => Ok(Datum::Int32(n.signum())),
                Some(Datum::Int64(n)) => Ok(Datum::Int64(n.signum())),
                Some(Datum::Float64(n)) => Ok(Datum::Float64(if *n > 0.0 { 1.0 } else if *n < 0.0 { -1.0 } else { 0.0 })),
                Some(Datum::Null) => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("SIGN requires numeric".into())),
            }
        }
        ScalarFunc::Trunc => {
            let val = match args.first() {
                Some(Datum::Int32(n)) => return Ok(Datum::Int32(*n)),
                Some(Datum::Int64(n)) => return Ok(Datum::Int64(*n)),
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("TRUNC requires numeric".into())),
            };
            let precision = match args.get(1) {
                Some(Datum::Int32(n)) => *n,
                Some(Datum::Int64(n)) => *n as i32,
                None => 0,
                _ => 0,
            };
            let factor = 10f64.powi(precision);
            Ok(Datum::Float64((val * factor).trunc() / factor))
        }
        ScalarFunc::Ln => {
            let val = match args.first() {
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("LN requires numeric".into())),
            };
            Ok(Datum::Float64(val.ln()))
        }
        ScalarFunc::Log => {
            let (base, val) = if args.len() >= 2 {
                let b = match args.first() {
                    Some(Datum::Int32(n)) => *n as f64,
                    Some(Datum::Int64(n)) => *n as f64,
                    Some(Datum::Float64(n)) => *n,
                    Some(Datum::Null) => return Ok(Datum::Null),
                    _ => return Err(ExecutionError::TypeError("LOG requires numeric".into())),
                };
                let v = match args.get(1) {
                    Some(Datum::Int32(n)) => *n as f64,
                    Some(Datum::Int64(n)) => *n as f64,
                    Some(Datum::Float64(n)) => *n,
                    Some(Datum::Null) => return Ok(Datum::Null),
                    _ => return Err(ExecutionError::TypeError("LOG requires numeric".into())),
                };
                (b, v)
            } else {
                let v = match args.first() {
                    Some(Datum::Int32(n)) => *n as f64,
                    Some(Datum::Int64(n)) => *n as f64,
                    Some(Datum::Float64(n)) => *n,
                    Some(Datum::Null) => return Ok(Datum::Null),
                    _ => return Err(ExecutionError::TypeError("LOG requires numeric".into())),
                };
                (10.0, v)
            };
            Ok(Datum::Float64(val.log(base)))
        }
        ScalarFunc::Exp => {
            let val = match args.first() {
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("EXP requires numeric".into())),
            };
            Ok(Datum::Float64(val.exp()))
        }
        ScalarFunc::Pi => {
            Ok(Datum::Float64(std::f64::consts::PI))
        }
        ScalarFunc::Mod => {
            let a = match args.first() {
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MOD requires numeric".into())),
            };
            let b = match args.get(1) {
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Float64(n)) => *n,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("MOD requires numeric".into())),
            };
            if b == 0.0 { return Err(ExecutionError::TypeError("division by zero".into())); }
            if matches!(args[0], Datum::Int64(_) | Datum::Int32(_)) && matches!(args[1], Datum::Int64(_) | Datum::Int32(_)) {
                Ok(Datum::Int64(a as i64 % b as i64))
            } else {
                Ok(Datum::Float64(a % b))
            }
        }
        ScalarFunc::Degrees => {
            let v = match args.first() {
                Some(Datum::Float64(f)) => *f,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("DEGREES requires numeric".into())),
            };
            Ok(Datum::Float64(v.to_degrees()))
        }
        ScalarFunc::Radians => {
            let v = match args.first() {
                Some(Datum::Float64(f)) => *f,
                Some(Datum::Int64(n)) => *n as f64,
                Some(Datum::Int32(n)) => *n as f64,
                Some(Datum::Null) => return Ok(Datum::Null),
                _ => return Err(ExecutionError::TypeError("RADIANS requires numeric".into())),
            };
            Ok(Datum::Float64(v.to_radians()))
        }
        ScalarFunc::Greatest => {
            let mut best: Option<Datum> = None;
            for a in args.iter() {
                if a.is_null() { continue; }
                match &best {
                    None => best = Some(a.clone()),
                    Some(current) => {
                        if a > current { best = Some(a.clone()); }
                    }
                }
            }
            Ok(best.unwrap_or(Datum::Null))
        }
        ScalarFunc::Least => {
            let mut best: Option<Datum> = None;
            for a in args.iter() {
                if a.is_null() { continue; }
                match &best {
                    None => best = Some(a.clone()),
                    Some(current) => {
                        if a < current { best = Some(a.clone()); }
                    }
                }
            }
            Ok(best.unwrap_or(Datum::Null))
        }
        _ => Err(ExecutionError::TypeError(format!("Not a math function: {:?}", func))),
    }
}
