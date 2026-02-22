use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::{BoundExpr, ScalarFunc};

use super::binary_ops::eval_binary_op;
use super::cast::eval_cast;

// ── Domain dispatch router ──────────────────────────────────────
// Core scalar functions are routed to domain-specific modules.
// Remaining extended functions fall through to the legacy mega-match below.
fn try_dispatch_domain(func: &ScalarFunc, args: &[Datum]) -> Option<Result<Datum, ExecutionError>> {
    match func {
        // String domain
        ScalarFunc::Upper
        | ScalarFunc::Lower
        | ScalarFunc::Length
        | ScalarFunc::Substring
        | ScalarFunc::Concat
        | ScalarFunc::ConcatWs
        | ScalarFunc::Trim
        | ScalarFunc::Btrim
        | ScalarFunc::Ltrim
        | ScalarFunc::Rtrim
        | ScalarFunc::Replace
        | ScalarFunc::Position
        | ScalarFunc::Lpad
        | ScalarFunc::Rpad
        | ScalarFunc::Left
        | ScalarFunc::Right
        | ScalarFunc::Repeat
        | ScalarFunc::Reverse
        | ScalarFunc::Initcap
        | ScalarFunc::Translate
        | ScalarFunc::Split
        | ScalarFunc::Overlay
        | ScalarFunc::StartsWith
        | ScalarFunc::EndsWith
        | ScalarFunc::Chr
        | ScalarFunc::Ascii
        | ScalarFunc::QuoteLiteral
        | ScalarFunc::QuoteIdent
        | ScalarFunc::QuoteNullable
        | ScalarFunc::BitLength
        | ScalarFunc::OctetLength => Some(super::scalar_string::dispatch(func, args)),

        // Math domain
        ScalarFunc::Abs
        | ScalarFunc::Round
        | ScalarFunc::Ceil
        | ScalarFunc::Ceiling
        | ScalarFunc::Floor
        | ScalarFunc::Power
        | ScalarFunc::Sqrt
        | ScalarFunc::Sign
        | ScalarFunc::Trunc
        | ScalarFunc::Ln
        | ScalarFunc::Log
        | ScalarFunc::Exp
        | ScalarFunc::Pi
        | ScalarFunc::Mod
        | ScalarFunc::Degrees
        | ScalarFunc::Radians
        | ScalarFunc::Greatest
        | ScalarFunc::Least => Some(super::scalar_math::dispatch(func, args)),

        // Time domain
        ScalarFunc::Now
        | ScalarFunc::CurrentDate
        | ScalarFunc::CurrentTime
        | ScalarFunc::Extract
        | ScalarFunc::DateTrunc
        | ScalarFunc::ToChar => Some(super::scalar_time::dispatch(func, args)),

        // Array domain (core)
        ScalarFunc::ArrayLength
        | ScalarFunc::Cardinality
        | ScalarFunc::ArrayPosition
        | ScalarFunc::ArrayAppend
        | ScalarFunc::ArrayPrepend
        | ScalarFunc::ArrayRemove
        | ScalarFunc::ArrayReplace
        | ScalarFunc::ArrayCat
        | ScalarFunc::ArrayToString
        | ScalarFunc::StringToArray
        | ScalarFunc::ArrayUpper
        | ScalarFunc::ArrayLower
        | ScalarFunc::ArrayDims
        | ScalarFunc::ArrayReverse
        | ScalarFunc::ArrayDistinct
        | ScalarFunc::ArraySort
        | ScalarFunc::ArrayContains
        | ScalarFunc::ArrayOverlap => Some(super::scalar_array::dispatch(func, args)),

        // Regex domain
        ScalarFunc::RegexpReplace
        | ScalarFunc::RegexpMatch
        | ScalarFunc::RegexpCount
        | ScalarFunc::RegexpSubstr
        | ScalarFunc::RegexpSplitToArray => Some(super::scalar_regex::dispatch(func, args)),

        // Crypto/encoding domain
        ScalarFunc::Md5
        | ScalarFunc::Sha256
        | ScalarFunc::Encode
        | ScalarFunc::Decode
        | ScalarFunc::ToHex => Some(super::scalar_crypto::dispatch(func, args)),

        // Utility domain
        ScalarFunc::PgTypeof
        | ScalarFunc::ToNumber
        | ScalarFunc::Random
        | ScalarFunc::GenRandomUuid
        | ScalarFunc::NumNonnulls
        | ScalarFunc::NumNulls => Some(super::scalar_utility::dispatch(func, args)),

        // JSONB domain
        ScalarFunc::JsonbBuildObject
        | ScalarFunc::JsonbBuildArray
        | ScalarFunc::JsonbTypeof
        | ScalarFunc::JsonbArrayLength
        | ScalarFunc::JsonbExtractPath
        | ScalarFunc::JsonbExtractPathText
        | ScalarFunc::JsonbObjectKeys
        | ScalarFunc::JsonbPretty
        | ScalarFunc::JsonbStripNulls
        | ScalarFunc::JsonbSetPath
        | ScalarFunc::ToJsonb
        | ScalarFunc::JsonbAgg
        | ScalarFunc::JsonbConcat
        | ScalarFunc::JsonbDeleteKey
        | ScalarFunc::JsonbDeletePath
        | ScalarFunc::JsonbPopulateRecord
        | ScalarFunc::JsonbArrayElements
        | ScalarFunc::JsonbArrayElementsText
        | ScalarFunc::JsonbEach
        | ScalarFunc::JsonbEachText
        | ScalarFunc::RowToJson => Some(super::scalar_jsonb::dispatch(func, args)),

        _ => None,
    }
}

/// Substitute parameter placeholders with concrete Datum values.
/// Rewrites the expression tree, replacing `BoundExpr::Parameter(idx)` with
/// `BoundExpr::Literal(params[idx-1])`. Returns an error if a param is missing.
pub fn substitute_params_expr(
    expr: &BoundExpr,
    params: &[Datum],
) -> Result<BoundExpr, ExecutionError> {
    match expr {
        BoundExpr::Parameter(idx) => {
            let i = idx.checked_sub(1).ok_or(ExecutionError::ParamMissing(0))?;
            let val = params.get(i).ok_or(ExecutionError::ParamMissing(*idx))?;
            Ok(BoundExpr::Literal(val.clone()))
        }
        BoundExpr::Literal(_)
        | BoundExpr::ColumnRef(_)
        | BoundExpr::OuterColumnRef(_)
        | BoundExpr::SequenceNextval(_)
        | BoundExpr::SequenceCurrval(_)
        | BoundExpr::SequenceSetval(_, _)
        | BoundExpr::Grouping(_) => Ok(expr.clone()),
        BoundExpr::BinaryOp { left, op, right } => Ok(BoundExpr::BinaryOp {
            left: Box::new(substitute_params_expr(left, params)?),
            op: *op,
            right: Box::new(substitute_params_expr(right, params)?),
        }),
        BoundExpr::Not(inner) => Ok(BoundExpr::Not(Box::new(substitute_params_expr(
            inner, params,
        )?))),
        BoundExpr::IsNull(inner) => Ok(BoundExpr::IsNull(Box::new(substitute_params_expr(
            inner, params,
        )?))),
        BoundExpr::IsNotNull(inner) => Ok(BoundExpr::IsNotNull(Box::new(substitute_params_expr(
            inner, params,
        )?))),
        BoundExpr::IsNotDistinctFrom { left, right } => Ok(BoundExpr::IsNotDistinctFrom {
            left: Box::new(substitute_params_expr(left, params)?),
            right: Box::new(substitute_params_expr(right, params)?),
        }),
        BoundExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => Ok(BoundExpr::Like {
            expr: Box::new(substitute_params_expr(expr, params)?),
            pattern: Box::new(substitute_params_expr(pattern, params)?),
            negated: *negated,
            case_insensitive: *case_insensitive,
        }),
        BoundExpr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(BoundExpr::Between {
            expr: Box::new(substitute_params_expr(expr, params)?),
            low: Box::new(substitute_params_expr(low, params)?),
            high: Box::new(substitute_params_expr(high, params)?),
            negated: *negated,
        }),
        BoundExpr::InList {
            expr,
            list,
            negated,
        } => Ok(BoundExpr::InList {
            expr: Box::new(substitute_params_expr(expr, params)?),
            list: list
                .iter()
                .map(|e| substitute_params_expr(e, params))
                .collect::<Result<_, _>>()?,
            negated: *negated,
        }),
        BoundExpr::Cast { expr, target_type } => Ok(BoundExpr::Cast {
            expr: Box::new(substitute_params_expr(expr, params)?),
            target_type: target_type.clone(),
        }),
        BoundExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Ok(BoundExpr::Case {
            operand: operand
                .as_ref()
                .map(|e| substitute_params_expr(e, params).map(Box::new))
                .transpose()?,
            conditions: conditions
                .iter()
                .map(|e| substitute_params_expr(e, params))
                .collect::<Result<_, _>>()?,
            results: results
                .iter()
                .map(|e| substitute_params_expr(e, params))
                .collect::<Result<_, _>>()?,
            else_result: else_result
                .as_ref()
                .map(|e| substitute_params_expr(e, params).map(Box::new))
                .transpose()?,
        }),
        BoundExpr::Coalesce(args) => Ok(BoundExpr::Coalesce(
            args.iter()
                .map(|e| substitute_params_expr(e, params))
                .collect::<Result<_, _>>()?,
        )),
        BoundExpr::Function { func, args } => Ok(BoundExpr::Function {
            func: func.clone(),
            args: args
                .iter()
                .map(|e| substitute_params_expr(e, params))
                .collect::<Result<_, _>>()?,
        }),
        BoundExpr::AggregateExpr {
            func,
            arg,
            distinct,
        } => Ok(BoundExpr::AggregateExpr {
            func: func.clone(),
            arg: arg
                .as_ref()
                .map(|e| substitute_params_expr(e, params).map(Box::new))
                .transpose()?,
            distinct: *distinct,
        }),
        BoundExpr::ArrayLiteral(elems) => Ok(BoundExpr::ArrayLiteral(
            elems
                .iter()
                .map(|e| substitute_params_expr(e, params))
                .collect::<Result<_, _>>()?,
        )),
        BoundExpr::ArrayIndex { array, index } => Ok(BoundExpr::ArrayIndex {
            array: Box::new(substitute_params_expr(array, params)?),
            index: Box::new(substitute_params_expr(index, params)?),
        }),
        BoundExpr::AnyOp {
            left,
            compare_op,
            right,
        } => Ok(BoundExpr::AnyOp {
            left: Box::new(substitute_params_expr(left, params)?),
            compare_op: *compare_op,
            right: Box::new(substitute_params_expr(right, params)?),
        }),
        BoundExpr::AllOp {
            left,
            compare_op,
            right,
        } => Ok(BoundExpr::AllOp {
            left: Box::new(substitute_params_expr(left, params)?),
            compare_op: *compare_op,
            right: Box::new(substitute_params_expr(right, params)?),
        }),
        BoundExpr::ArraySlice {
            array,
            lower,
            upper,
        } => Ok(BoundExpr::ArraySlice {
            array: Box::new(substitute_params_expr(array, params)?),
            lower: lower
                .as_ref()
                .map(|e| substitute_params_expr(e, params).map(Box::new))
                .transpose()?,
            upper: upper
                .as_ref()
                .map(|e| substitute_params_expr(e, params).map(Box::new))
                .transpose()?,
        }),
        BoundExpr::ScalarSubquery(_) | BoundExpr::InSubquery { .. } | BoundExpr::Exists { .. } => {
            // Subqueries are materialized before eval; params inside them were
            // already handled during binding. Clone as-is.
            Ok(expr.clone())
        }
    }
}

/// Evaluate a bound expression against a row, producing a Datum.
/// Convenience wrapper that calls `eval_expr_with_params` with no parameters.
pub fn eval_expr(expr: &BoundExpr, row: &OwnedRow) -> Result<Datum, ExecutionError> {
    eval_expr_with_params(expr, row, &[])
}

/// Evaluate a bound expression against a row with parameter values.
/// Parameters are 1-indexed: `$1` maps to `params[0]`.
pub fn eval_expr_with_params(
    expr: &BoundExpr,
    row: &OwnedRow,
    params: &[Datum],
) -> Result<Datum, ExecutionError> {
    match expr {
        BoundExpr::Literal(datum) => Ok(datum.clone()),
        BoundExpr::ColumnRef(idx) => row
            .get(*idx)
            .cloned()
            .ok_or(ExecutionError::ColumnOutOfBounds(*idx)),
        BoundExpr::BinaryOp { left, op, right } => {
            let lval = eval_expr_with_params(left, row, params)?;
            let rval = eval_expr_with_params(right, row, params)?;
            eval_binary_op(&lval, *op, &rval)
        }
        BoundExpr::Not(inner) => {
            let val = eval_expr_with_params(inner, row, params)?;
            match val {
                Datum::Boolean(b) => Ok(Datum::Boolean(!b)),
                Datum::Null => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("NOT requires boolean".into())),
            }
        }
        BoundExpr::IsNull(inner) => {
            let val = eval_expr_with_params(inner, row, params)?;
            Ok(Datum::Boolean(val.is_null()))
        }
        BoundExpr::IsNotNull(inner) => {
            let val = eval_expr_with_params(inner, row, params)?;
            Ok(Datum::Boolean(!val.is_null()))
        }
        BoundExpr::IsNotDistinctFrom { left, right } => {
            let lv = eval_expr_with_params(left, row, params)?;
            let rv = eval_expr_with_params(right, row, params)?;
            // NULL-safe equality: NULL IS NOT DISTINCT FROM NULL = true
            let result = match (&lv, &rv) {
                (Datum::Null, Datum::Null) => true,
                (Datum::Null, _) | (_, Datum::Null) => false,
                _ => lv == rv,
            };
            Ok(Datum::Boolean(result))
        }
        BoundExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let val = eval_expr_with_params(expr, row, params)?;
            let pat_val = eval_expr_with_params(pattern, row, params)?;
            match (&val, &pat_val) {
                (Datum::Text(s), Datum::Text(p)) => {
                    let matched = if *case_insensitive {
                        like_match(&s.to_lowercase(), &p.to_lowercase())
                    } else {
                        like_match(s, p)
                    };
                    Ok(Datum::Boolean(if *negated { !matched } else { matched }))
                }
                (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError("LIKE requires text".into())),
            }
        }
        BoundExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let val = eval_expr_with_params(expr, row, params)?;
            let low_val = eval_expr_with_params(low, row, params)?;
            let high_val = eval_expr_with_params(high, row, params)?;
            if val.is_null() || low_val.is_null() || high_val.is_null() {
                return Ok(Datum::Null);
            }
            let in_range = val >= low_val && val <= high_val;
            Ok(Datum::Boolean(if *negated { !in_range } else { in_range }))
        }
        BoundExpr::InList {
            expr,
            list,
            negated,
        } => {
            let val = eval_expr_with_params(expr, row, params)?;
            if val.is_null() {
                return Ok(Datum::Null);
            }
            let mut found = false;
            for item in list {
                let item_val = eval_expr_with_params(item, row, params)?;
                if item_val.is_null() {
                    continue;
                }
                if val == item_val {
                    found = true;
                    break;
                }
            }
            Ok(Datum::Boolean(if *negated { !found } else { found }))
        }
        BoundExpr::Cast { expr, target_type } => {
            let val = eval_expr_with_params(expr, row, params)?;
            eval_cast(val, target_type)
        }
        BoundExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            match operand {
                Some(op_expr) => {
                    // Simple CASE: CASE expr WHEN val1 THEN res1 ...
                    let op_val = eval_expr_with_params(op_expr, row, params)?;
                    for (cond, res) in conditions.iter().zip(results.iter()) {
                        let cond_val = eval_expr_with_params(cond, row, params)?;
                        if op_val == cond_val {
                            return eval_expr_with_params(res, row, params);
                        }
                    }
                }
                None => {
                    // Searched CASE: CASE WHEN cond1 THEN res1 ...
                    for (cond, res) in conditions.iter().zip(results.iter()) {
                        let cond_val = eval_expr_with_params(cond, row, params)?;
                        if cond_val == Datum::Boolean(true) {
                            return eval_expr_with_params(res, row, params);
                        }
                    }
                }
            }
            match else_result {
                Some(e) => eval_expr_with_params(e, row, params),
                None => Ok(Datum::Null),
            }
        }
        BoundExpr::Coalesce(args) => {
            for arg in args {
                let val = eval_expr_with_params(arg, row, params)?;
                if !matches!(val, Datum::Null) {
                    return Ok(val);
                }
            }
            Ok(Datum::Null)
        }
        BoundExpr::Function { func, args } => {
            let vals: Vec<Datum> = args
                .iter()
                .map(|a| eval_expr_with_params(a, row, params))
                .collect::<Result<_, _>>()?;
            eval_scalar_func(func, &vals)
        }
        BoundExpr::ArrayLiteral(elems) => {
            let vals: Vec<Datum> = elems
                .iter()
                .map(|e| eval_expr_with_params(e, row, params))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Datum::Array(vals))
        }
        BoundExpr::ArrayIndex { array, index } => {
            let arr_val = eval_expr_with_params(array, row, params)?;
            let idx_val = eval_expr_with_params(index, row, params)?;
            match (&arr_val, &idx_val) {
                (Datum::Array(arr), Datum::Int32(i)) => {
                    let idx = *i as usize;
                    if idx >= 1 && idx <= arr.len() {
                        Ok(arr[idx - 1].clone())
                    } else {
                        Ok(Datum::Null)
                    }
                }
                (Datum::Array(arr), Datum::Int64(i)) => {
                    let idx = *i as usize;
                    if idx >= 1 && idx <= arr.len() {
                        Ok(arr[idx - 1].clone())
                    } else {
                        Ok(Datum::Null)
                    }
                }
                (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError(
                    "Array subscript requires array and integer index".into(),
                )),
            }
        }
        BoundExpr::AggregateExpr { .. } => Err(ExecutionError::TypeError(
            "Aggregate in expression must be evaluated via eval_having_expr".into(),
        )),
        BoundExpr::ScalarSubquery(_) | BoundExpr::InSubquery { .. } | BoundExpr::Exists { .. } => {
            Err(ExecutionError::TypeError(
                "Subquery should be materialized before eval".into(),
            ))
        }
        BoundExpr::OuterColumnRef(_) => Err(ExecutionError::TypeError(
            "OuterColumnRef should be substituted before eval".into(),
        )),
        BoundExpr::SequenceNextval(_)
        | BoundExpr::SequenceCurrval(_)
        | BoundExpr::SequenceSetval(_, _) => Err(ExecutionError::TypeError(
            "Sequence functions must be evaluated via the executor".into(),
        )),
        BoundExpr::Parameter(idx) => {
            // Parameters are 1-indexed: $1 -> params[0]
            if params.is_empty() {
                return Err(ExecutionError::TypeError(format!(
                    "Parameter ${} must be bound before execution",
                    idx
                )));
            }
            let i = idx.checked_sub(1).ok_or(ExecutionError::ParamMissing(0))?;
            params
                .get(i)
                .cloned()
                .ok_or(ExecutionError::ParamMissing(*idx))
        }
        BoundExpr::AnyOp {
            left,
            compare_op,
            right,
        } => {
            let lval = eval_expr_with_params(left, row, params)?;
            let rval = eval_expr_with_params(right, row, params)?;
            if lval.is_null() {
                return Ok(Datum::Null);
            }
            match rval {
                Datum::Array(ref elems) => {
                    let mut has_null = false;
                    for elem in elems {
                        if elem.is_null() {
                            has_null = true;
                            continue;
                        }
                        let cmp = eval_binary_op(&lval, *compare_op, elem)?;
                        if cmp == Datum::Boolean(true) {
                            return Ok(Datum::Boolean(true));
                        }
                    }
                    // SQL semantics: if any element was NULL and none matched, result is NULL
                    Ok(if has_null {
                        Datum::Null
                    } else {
                        Datum::Boolean(false)
                    })
                }
                Datum::Null => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError(
                    "ANY requires array operand".into(),
                )),
            }
        }
        BoundExpr::AllOp {
            left,
            compare_op,
            right,
        } => {
            let lval = eval_expr_with_params(left, row, params)?;
            let rval = eval_expr_with_params(right, row, params)?;
            if lval.is_null() {
                return Ok(Datum::Null);
            }
            match rval {
                Datum::Array(ref elems) => {
                    if elems.is_empty() {
                        // ALL over empty set is vacuously true
                        return Ok(Datum::Boolean(true));
                    }
                    let mut has_null = false;
                    for elem in elems {
                        if elem.is_null() {
                            has_null = true;
                            continue;
                        }
                        let cmp = eval_binary_op(&lval, *compare_op, elem)?;
                        if cmp == Datum::Boolean(false) {
                            return Ok(Datum::Boolean(false));
                        }
                    }
                    Ok(if has_null {
                        Datum::Null
                    } else {
                        Datum::Boolean(true)
                    })
                }
                Datum::Null => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError(
                    "ALL requires array operand".into(),
                )),
            }
        }
        BoundExpr::ArraySlice {
            array,
            lower,
            upper,
        } => {
            let arr_val = eval_expr_with_params(array, row, params)?;
            match arr_val {
                Datum::Array(ref elems) => {
                    let len = elems.len() as i64;
                    // PostgreSQL: 1-indexed, inclusive bounds; default lower=1, upper=len
                    let lo = match lower {
                        Some(e) => {
                            let v = eval_expr_with_params(e, row, params)?;
                            match v {
                                Datum::Int32(i) => i as i64,
                                Datum::Int64(i) => i,
                                Datum::Null => return Ok(Datum::Null),
                                _ => {
                                    return Err(ExecutionError::TypeError(
                                        "Array slice bound must be integer".into(),
                                    ))
                                }
                            }
                        }
                        None => 1,
                    };
                    let hi = match upper {
                        Some(e) => {
                            let v = eval_expr_with_params(e, row, params)?;
                            match v {
                                Datum::Int32(i) => i as i64,
                                Datum::Int64(i) => i,
                                Datum::Null => return Ok(Datum::Null),
                                _ => {
                                    return Err(ExecutionError::TypeError(
                                        "Array slice bound must be integer".into(),
                                    ))
                                }
                            }
                        }
                        None => len,
                    };
                    // Convert 1-indexed inclusive [lo, hi] to 0-indexed range
                    let start = ((lo - 1).max(0) as usize).min(elems.len());
                    let end = (hi.max(0) as usize).min(elems.len());
                    if start >= end {
                        Ok(Datum::Array(vec![]))
                    } else {
                        Ok(Datum::Array(elems[start..end].to_vec()))
                    }
                }
                Datum::Null => Ok(Datum::Null),
                _ => Err(ExecutionError::TypeError(
                    "Array slice requires array operand".into(),
                )),
            }
        }
        BoundExpr::Grouping(_) => {
            // Default: returns 0 (all columns are real group-by values).
            // The executor substitutes the actual bitmask per active grouping set
            // before reaching here, so this is just a fallback for non-grouping-set queries.
            Ok(Datum::Int32(0))
        }
    }
}

fn eval_scalar_func(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    // Fast-path: dispatch core functions to domain-specific modules
    if let Some(result) = try_dispatch_domain(func, args) {
        return result;
    }

    // Dispatch extended functions to the scalar_ext module.
    super::scalar_ext::dispatch(func, args)
}

/// SQL LIKE pattern matching: % matches any sequence, _ matches single char.
fn like_match(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_match_inner(&s_chars, &p_chars)
}

fn like_match_inner(s: &[char], p: &[char]) -> bool {
    if p.is_empty() {
        return s.is_empty();
    }
    if p[0] == '%' {
        // % matches zero or more characters
        for i in 0..=s.len() {
            if like_match_inner(&s[i..], &p[1..]) {
                return true;
            }
        }
        false
    } else if p[0] == '_' {
        // _ matches exactly one character
        !s.is_empty() && like_match_inner(&s[1..], &p[1..])
    } else {
        !s.is_empty() && s[0] == p[0] && like_match_inner(&s[1..], &p[1..])
    }
}

/// Evaluate a filter expression and return true if the row passes.
pub fn eval_filter(expr: &BoundExpr, row: &OwnedRow) -> Result<bool, ExecutionError> {
    let result = eval_expr(expr, row)?;
    Ok(result.as_bool().unwrap_or(false))
}

/// Evaluate a filter expression with parameter values.
pub fn eval_filter_with_params(
    expr: &BoundExpr,
    row: &OwnedRow,
    params: &[Datum],
) -> Result<bool, ExecutionError> {
    let result = eval_expr_with_params(expr, row, params)?;
    Ok(result.as_bool().unwrap_or(false))
}

/// Evaluate a HAVING expression that may contain inline AggregateExpr nodes.
/// For each AggregateExpr, compute the aggregate over group_rows.
/// For other nodes, evaluate against the first row of the group (representative).
pub fn eval_having_expr(
    expr: &BoundExpr,
    group_rows: &[&OwnedRow],
) -> Result<Datum, ExecutionError> {
    match expr {
        BoundExpr::AggregateExpr {
            func,
            arg,
            distinct,
        } => {
            // Evaluate aggregate over group rows
            let eval_all = |e: &BoundExpr| -> Result<Vec<Datum>, ExecutionError> {
                let mut vals = Vec::new();
                for row in group_rows {
                    let v = eval_expr(e, row)?;
                    if !v.is_null() {
                        vals.push(v);
                    }
                }
                Ok(vals)
            };
            let distinct_vals = |e: &BoundExpr| -> Result<Vec<Datum>, ExecutionError> {
                let mut seen = std::collections::HashSet::new();
                let mut vals = Vec::new();
                for row in group_rows {
                    let v = eval_expr(e, row)?;
                    if v.is_null() {
                        continue;
                    }
                    let key = format!("{}", v);
                    if seen.insert(key) {
                        vals.push(v);
                    }
                }
                Ok(vals)
            };
            use falcon_sql_frontend::types::AggFunc;
            match func {
                AggFunc::Count => {
                    if let Some(e) = arg {
                        if *distinct {
                            Ok(Datum::Int64(distinct_vals(e)?.len() as i64))
                        } else {
                            Ok(Datum::Int64(eval_all(e)?.len() as i64))
                        }
                    } else {
                        Ok(Datum::Int64(group_rows.len() as i64))
                    }
                }
                AggFunc::Sum => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("SUM requires arg".into()))?;
                    let vals = if *distinct {
                        distinct_vals(e)?
                    } else {
                        eval_all(e)?
                    };
                    let mut acc = Datum::Null;
                    for v in vals {
                        acc = if acc.is_null() {
                            v
                        } else {
                            acc.add(&v).unwrap_or(acc)
                        };
                    }
                    Ok(acc)
                }
                AggFunc::Avg => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("AVG requires arg".into()))?;
                    let vals = if *distinct {
                        distinct_vals(e)?
                    } else {
                        eval_all(e)?
                    };
                    let mut sum = 0.0f64;
                    let mut count = 0i64;
                    for v in &vals {
                        if let Some(f) = v.as_f64() {
                            sum += f;
                            count += 1;
                        }
                    }
                    if count == 0 {
                        Ok(Datum::Null)
                    } else {
                        Ok(Datum::Float64(sum / count as f64))
                    }
                }
                AggFunc::Min => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("MIN requires arg".into()))?;
                    let vals = eval_all(e)?;
                    Ok(vals.into_iter().min().unwrap_or(Datum::Null))
                }
                AggFunc::Max => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("MAX requires arg".into()))?;
                    let vals = eval_all(e)?;
                    Ok(vals.into_iter().max().unwrap_or(Datum::Null))
                }
                AggFunc::BoolAnd => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("BOOL_AND requires arg".into()))?;
                    let vals = eval_all(e)?;
                    if vals.is_empty() {
                        Ok(Datum::Null)
                    } else {
                        Ok(Datum::Boolean(
                            vals.iter().all(|v| v.as_bool().unwrap_or(true)),
                        ))
                    }
                }
                AggFunc::BoolOr => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("BOOL_OR requires arg".into()))?;
                    let vals = eval_all(e)?;
                    if vals.is_empty() {
                        Ok(Datum::Null)
                    } else {
                        Ok(Datum::Boolean(
                            vals.iter().any(|v| v.as_bool().unwrap_or(false)),
                        ))
                    }
                }
                AggFunc::StringAgg(sep) => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("STRING_AGG requires arg".into()))?;
                    let vals = eval_all(e)?;
                    if vals.is_empty() {
                        Ok(Datum::Null)
                    } else {
                        Ok(Datum::Text(
                            vals.iter()
                                .map(|v| format!("{}", v))
                                .collect::<Vec<_>>()
                                .join(sep),
                        ))
                    }
                }
                AggFunc::ArrayAgg => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("ARRAY_AGG requires arg".into()))?;
                    let vals = if *distinct {
                        distinct_vals(e)?
                    } else {
                        eval_all(e)?
                    };
                    if vals.is_empty() {
                        Ok(Datum::Null)
                    } else {
                        Ok(Datum::Array(vals))
                    }
                }
                // Statistical aggregates in HAVING — compute inline
                AggFunc::VarPop | AggFunc::VarSamp | AggFunc::StddevPop | AggFunc::StddevSamp => {
                    let e = arg.as_ref().ok_or(ExecutionError::TypeError(
                        "Statistical aggregate requires arg".into(),
                    ))?;
                    let vals = if *distinct {
                        distinct_vals(e)?
                    } else {
                        eval_all(e)?
                    };
                    let floats: Vec<f64> = vals.iter().filter_map(|v| v.as_f64()).collect();
                    let n = floats.len();
                    if n == 0 {
                        return Ok(Datum::Null);
                    }
                    let mean = floats.iter().sum::<f64>() / n as f64;
                    let sum_sq: f64 = floats.iter().map(|x| (x - mean).powi(2)).sum();
                    let variance = match func {
                        AggFunc::VarPop | AggFunc::StddevPop => sum_sq / n as f64,
                        _ => {
                            if n < 2 {
                                return Ok(Datum::Null);
                            }
                            sum_sq / (n - 1) as f64
                        }
                    };
                    match func {
                        AggFunc::StddevPop | AggFunc::StddevSamp => {
                            Ok(Datum::Float64(variance.sqrt()))
                        }
                        _ => Ok(Datum::Float64(variance)),
                    }
                }
                AggFunc::Mode => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("MODE requires arg".into()))?;
                    let vals = eval_all(e)?;
                    if vals.is_empty() {
                        return Ok(Datum::Null);
                    }
                    let mut freq: Vec<(String, usize, Datum)> = Vec::new();
                    for v in vals {
                        let key = format!("{}", v);
                        if let Some(entry) = freq.iter_mut().find(|(k, _, _)| *k == key) {
                            entry.1 += 1;
                        } else {
                            freq.push((key, 1, v));
                        }
                    }
                    freq.sort_by(|a, b| b.1.cmp(&a.1));
                    Ok(freq
                        .into_iter()
                        .next()
                        .map(|(_, _, v)| v)
                        .unwrap_or(Datum::Null))
                }
                AggFunc::BitAndAgg => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("BIT_AND requires arg".into()))?;
                    let vals = eval_all(e)?;
                    if vals.is_empty() {
                        return Ok(Datum::Null);
                    }
                    let mut r: Option<i64> = None;
                    for v in &vals {
                        if let Some(i) = v.as_i64() {
                            r = Some(r.map_or(i, |acc| acc & i));
                        }
                    }
                    Ok(r.map(Datum::Int64).unwrap_or(Datum::Null))
                }
                AggFunc::BitOrAgg => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("BIT_OR requires arg".into()))?;
                    let vals = eval_all(e)?;
                    if vals.is_empty() {
                        return Ok(Datum::Null);
                    }
                    let mut r: i64 = 0;
                    for v in &vals {
                        if let Some(i) = v.as_i64() {
                            r |= i;
                        }
                    }
                    Ok(Datum::Int64(r))
                }
                AggFunc::BitXorAgg => {
                    let e = arg
                        .as_ref()
                        .ok_or(ExecutionError::TypeError("BIT_XOR requires arg".into()))?;
                    let vals = eval_all(e)?;
                    if vals.is_empty() {
                        return Ok(Datum::Null);
                    }
                    let mut r: i64 = 0;
                    for v in &vals {
                        if let Some(i) = v.as_i64() {
                            r ^= i;
                        }
                    }
                    Ok(Datum::Int64(r))
                }
                // Two-argument and ordered-set aggregates: return NULL in HAVING context
                _ => Ok(Datum::Null),
            }
        }
        BoundExpr::BinaryOp { left, op, right } => {
            let lval = eval_having_expr(left, group_rows)?;
            let rval = eval_having_expr(right, group_rows)?;
            eval_binary_op(&lval, *op, &rval)
        }
        BoundExpr::Not(inner) => {
            let val = eval_having_expr(inner, group_rows)?;
            match val.as_bool() {
                Some(b) => Ok(Datum::Boolean(!b)),
                None => Ok(Datum::Null),
            }
        }
        // For non-aggregate expressions, evaluate against first row
        _ => {
            if let Some(row) = group_rows.first() {
                eval_expr(expr, row)
            } else {
                Ok(Datum::Null)
            }
        }
    }
}

/// Evaluate a HAVING filter and return true if the group passes.
pub fn eval_having_filter(
    expr: &BoundExpr,
    group_rows: &[&OwnedRow],
) -> Result<bool, ExecutionError> {
    let result = eval_having_expr(expr, group_rows)?;
    Ok(result.as_bool().unwrap_or(false))
}
