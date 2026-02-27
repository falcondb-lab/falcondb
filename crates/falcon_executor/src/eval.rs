use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::{AggFunc, BinOp, BoundExpr, ScalarFunc};

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

/// Quick check: does the expression tree contain any Parameter nodes?
fn expr_has_params(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::Parameter(_) => true,
        BoundExpr::Literal(_)
        | BoundExpr::ColumnRef(_)
        | BoundExpr::OuterColumnRef(_)
        | BoundExpr::SequenceNextval(_)
        | BoundExpr::SequenceCurrval(_)
        | BoundExpr::SequenceSetval(_, _)
        | BoundExpr::Grouping(_) => false,
        BoundExpr::BinaryOp { left, right, .. } => expr_has_params(left) || expr_has_params(right),
        BoundExpr::Not(inner)
        | BoundExpr::IsNull(inner)
        | BoundExpr::IsNotNull(inner) => expr_has_params(inner),
        BoundExpr::IsNotDistinctFrom { left, right } => expr_has_params(left) || expr_has_params(right),
        BoundExpr::Like { expr: e, pattern, .. } => expr_has_params(e) || expr_has_params(pattern),
        BoundExpr::Between { expr: e, low, high, .. } => {
            expr_has_params(e) || expr_has_params(low) || expr_has_params(high)
        }
        BoundExpr::InList { expr: e, list, .. } => {
            expr_has_params(e) || list.iter().any(expr_has_params)
        }
        BoundExpr::Cast { expr: e, .. } => expr_has_params(e),
        BoundExpr::Case { operand, conditions, results, else_result } => {
            operand.as_ref().is_some_and(|e| expr_has_params(e))
                || conditions.iter().any(expr_has_params)
                || results.iter().any(expr_has_params)
                || else_result.as_ref().is_some_and(|e| expr_has_params(e))
        }
        BoundExpr::Coalesce(args) | BoundExpr::ArrayLiteral(args) => args.iter().any(expr_has_params),
        BoundExpr::Function { args, .. } => args.iter().any(expr_has_params),
        BoundExpr::AggregateExpr { arg, .. } => arg.as_ref().is_some_and(|e| expr_has_params(e)),
        BoundExpr::ArrayIndex { array, index } => expr_has_params(array) || expr_has_params(index),
        BoundExpr::AnyOp { left, right, .. } | BoundExpr::AllOp { left, right, .. } => {
            expr_has_params(left) || expr_has_params(right)
        }
        BoundExpr::ArraySlice { array, lower, upper } => {
            expr_has_params(array)
                || lower.as_ref().is_some_and(|e| expr_has_params(e))
                || upper.as_ref().is_some_and(|e| expr_has_params(e))
        }
        _ => true, // conservative: assume params for unknown variants
    }
}

/// Substitute parameter placeholders with concrete Datum values.
///
/// Rewrites the expression tree, replacing `BoundExpr::Parameter(idx)` with
/// `BoundExpr::Literal(params[idx-1])`. Returns an error if a param is missing.
pub fn substitute_params_expr(
    expr: &BoundExpr,
    params: &[Datum],
) -> Result<BoundExpr, ExecutionError> {
    // Fast path: no parameters to substitute — return a clone without walking the tree
    if params.is_empty() || !expr_has_params(expr) {
        return Ok(expr.clone());
    }
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
            // Fast path: literal pattern — avoid re-evaluating + re-lowering per row
            let pat_val = match pattern.as_ref() {
                BoundExpr::Literal(d) => std::borrow::Cow::Borrowed(d),
                _ => std::borrow::Cow::Owned(eval_expr_with_params(pattern, row, params)?),
            };
            match (&val, pat_val.as_ref()) {
                (Datum::Text(s), Datum::Text(p)) => {
                    let matched = if *case_insensitive {
                        // For literal patterns the lowered pattern is constant across rows,
                        // but we still need to lower it once here. The real saving is
                        // avoiding re-evaluating the pattern expression each row (handled
                        // by the Cow::Borrowed above). Lower only the input string per row.
                        let lower_p = p.to_lowercase();
                        like_match(&s.to_lowercase(), &lower_p)
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
            // Fast path: if all list items are literals, skip per-item eval.
            // Use direct comparison with early exit — faster than rebuilding
            // a HashSet on every row for typical list sizes.
            let all_literals = list.iter().all(|e| matches!(e, BoundExpr::Literal(_)));
            let found = if all_literals {
                list.iter().any(|e| match e {
                    BoundExpr::Literal(d) if !d.is_null() => *d == val,
                    _ => false,
                })
            } else {
                // General case: evaluate each expression
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
                found
            };
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
            else_result.as_ref().map_or(Ok(Datum::Null), |e| eval_expr_with_params(e, row, params))
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
            // Stack-inline evaluation for ≤4 args (covers ~95% of scalar functions:
            // UPPER, LENGTH, ROUND, COALESCE, etc.) — avoids heap Vec allocation.
            if args.len() <= 4 {
                let mut inline = [Datum::Null, Datum::Null, Datum::Null, Datum::Null];
                for (i, a) in args.iter().enumerate() {
                    inline[i] = eval_expr_with_params(a, row, params)?;
                }
                eval_scalar_func(func, &inline[..args.len()])
            } else {
                let vals: Vec<Datum> = args
                    .iter()
                    .map(|a| eval_expr_with_params(a, row, params))
                    .collect::<Result<_, _>>()?;
                eval_scalar_func(func, &vals)
            }
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
                    "Parameter ${idx} must be bound before execution"
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
                                Datum::Int32(i) => i64::from(i),
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
                                Datum::Int32(i) => i64::from(i),
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

/// Byte-encode a single Datum into `buf` for dedup/hashing purposes.
/// Uses tag-byte + fixed-width payload — no String allocation.
pub(crate) fn encode_datum_key(buf: &mut Vec<u8>, datum: &Datum) {
    match datum {
        Datum::Null => buf.push(0),
        Datum::Boolean(b) => { buf.push(1); buf.push(u8::from(*b)); }
        Datum::Int32(v) => { buf.push(2); buf.extend_from_slice(&v.to_le_bytes()); }
        Datum::Int64(v) => { buf.push(3); buf.extend_from_slice(&v.to_le_bytes()); }
        Datum::Float64(v) => { buf.push(4); buf.extend_from_slice(&v.to_le_bytes()); }
        Datum::Text(s) => { buf.push(5); buf.extend_from_slice(s.as_bytes()); buf.push(0); }
        Datum::Timestamp(v) => { buf.push(6); buf.extend_from_slice(&v.to_le_bytes()); }
        Datum::Date(v) => { buf.push(7); buf.extend_from_slice(&v.to_le_bytes()); }
        Datum::Time(v) => { buf.push(8); buf.extend_from_slice(&v.to_le_bytes()); }
        Datum::Decimal(m, s) => { buf.push(9); buf.extend_from_slice(&m.to_le_bytes()); buf.push(*s); }
        Datum::Uuid(v) => { buf.push(10); buf.extend_from_slice(&v.to_le_bytes()); }
        Datum::Bytea(b) => { buf.push(11); buf.extend_from_slice(&(b.len() as u32).to_le_bytes()); buf.extend_from_slice(b); }
        Datum::Interval(mo, d, us) => { buf.push(12); buf.extend_from_slice(&mo.to_le_bytes()); buf.extend_from_slice(&d.to_le_bytes()); buf.extend_from_slice(&us.to_le_bytes()); }
        other => { buf.push(255); buf.extend_from_slice(format!("{other}").as_bytes()); buf.push(0); }
    }
}

/// SQL LIKE pattern matching: % matches any sequence, _ matches single char.
/// Works directly on `&str` slices — no `Vec<char>` allocation.
/// Uses an iterative two-pointer approach with backtracking for `%`,
/// which is O(n*m) worst case but avoids exponential recursion.
fn like_match(s: &str, pattern: &str) -> bool {
    let sb = s.as_bytes();
    let pb = pattern.as_bytes();
    let slen = sb.len();
    let plen = pb.len();

    // Fast path: pure ASCII (no multi-byte chars) — work on bytes directly
    if s.is_ascii() && pattern.is_ascii() {
        let mut si = 0usize;
        let mut pi = 0usize;
        let mut star_p = usize::MAX; // position after last '%' in pattern
        let mut star_s = 0usize; // position in s when last '%' was seen

        while si < slen {
            if pi < plen && (pb[pi] == b'_' || pb[pi] == sb[si]) {
                si += 1;
                pi += 1;
            } else if pi < plen && pb[pi] == b'%' {
                star_p = pi + 1;
                star_s = si;
                pi += 1;
            } else if star_p != usize::MAX {
                // Backtrack: advance the match start for the last '%'
                star_s += 1;
                si = star_s;
                pi = star_p;
            } else {
                return false;
            }
        }
        // Consume trailing '%' in pattern
        while pi < plen && pb[pi] == b'%' {
            pi += 1;
        }
        return pi == plen;
    }

    // Unicode fallback: collect chars (rare path)
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_match_chars(&s_chars, &p_chars)
}

/// Unicode-aware LIKE matching on char slices (fallback for non-ASCII).
fn like_match_chars(s: &[char], p: &[char]) -> bool {
    let slen = s.len();
    let plen = p.len();
    let mut si = 0usize;
    let mut pi = 0usize;
    let mut star_p = usize::MAX;
    let mut star_s = 0usize;

    while si < slen {
        if pi < plen && (p[pi] == '_' || p[pi] == s[si]) {
            si += 1;
            pi += 1;
        } else if pi < plen && p[pi] == '%' {
            star_p = pi + 1;
            star_s = si;
            pi += 1;
        } else if star_p != usize::MAX {
            star_s += 1;
            si = star_s;
            pi = star_p;
        } else {
            return false;
        }
    }
    while pi < plen && p[pi] == '%' {
        pi += 1;
    }
    pi == plen
}

/// Evaluate a filter expression and return true if the row passes.
///
/// Uses a specialized fast path for common patterns (`col op literal`,
/// `AND`, `OR`, `NOT`, `IsNull`, `IsNotNull`) that borrows directly from
/// the expression tree and row, avoiding per-row `Datum::clone()`.
/// Falls back to the general `eval_expr` path for complex expressions.
pub fn eval_filter(expr: &BoundExpr, row: &OwnedRow) -> Result<bool, ExecutionError> {
    match expr {
        // AND: short-circuit without allocating Datum
        BoundExpr::BinaryOp { left, op: BinOp::And, right } => {
            Ok(eval_filter(left, row)? && eval_filter(right, row)?)
        }
        // OR: short-circuit without allocating Datum
        BoundExpr::BinaryOp { left, op: BinOp::Or, right } => {
            Ok(eval_filter(left, row)? || eval_filter(right, row)?)
        }
        // col <cmp> literal / literal <cmp> col — borrow both sides, zero clone
        BoundExpr::BinaryOp { left, op, right }
            if matches!(op, BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq) =>
        {
            let (lref, rref) = match (left.as_ref(), right.as_ref()) {
                (BoundExpr::ColumnRef(li), BoundExpr::Literal(rd)) => {
                    let ld = row.get(*li).unwrap_or(&Datum::Null);
                    (ld, rd)
                }
                (BoundExpr::Literal(ld), BoundExpr::ColumnRef(ri)) => {
                    let rd = row.get(*ri).unwrap_or(&Datum::Null);
                    (ld, rd)
                }
                (BoundExpr::ColumnRef(li), BoundExpr::ColumnRef(ri)) => {
                    let ld = row.get(*li).unwrap_or(&Datum::Null);
                    let rd = row.get(*ri).unwrap_or(&Datum::Null);
                    (ld, rd)
                }
                _ => {
                    // Complex operands — fall back to general path
                    let result = eval_expr(expr, row)?;
                    return Ok(result.as_bool().unwrap_or(false));
                }
            };
            if lref.is_null() || rref.is_null() {
                return Ok(false); // NULL comparisons are false in filter context
            }
            // Only use direct comparison when both sides are the same type.
            // Mixed types (e.g. Date vs Text) need the coercion in eval_binary_op.
            if std::mem::discriminant(lref) != std::mem::discriminant(rref) {
                let result = eval_binary_op(lref, *op, rref)?;
                return Ok(result.as_bool().unwrap_or(false));
            }
            Ok(match op {
                BinOp::Eq => lref == rref,
                BinOp::NotEq => lref != rref,
                BinOp::Lt => lref < rref,
                BinOp::LtEq => lref <= rref,
                BinOp::Gt => lref > rref,
                BinOp::GtEq => lref >= rref,
                _ => unreachable!(),
            })
        }
        // NOT
        BoundExpr::Not(inner) => Ok(!eval_filter(inner, row)?),
        // IS NULL / IS NOT NULL — borrow, no clone
        BoundExpr::IsNull(inner) => {
            if let BoundExpr::ColumnRef(idx) = inner.as_ref() {
                Ok(row.get(*idx).map_or(true, Datum::is_null))
            } else {
                let val = eval_expr(inner, row)?;
                Ok(val.is_null())
            }
        }
        BoundExpr::IsNotNull(inner) => {
            if let BoundExpr::ColumnRef(idx) = inner.as_ref() {
                Ok(row.get(*idx).map_or(false, |d| !d.is_null()))
            } else {
                let val = eval_expr(inner, row)?;
                Ok(!val.is_null())
            }
        }
        // IN list: col IN (lit, lit, ...) — borrow column value, compare against literal refs
        BoundExpr::InList { expr, list, negated }
            if matches!(expr.as_ref(), BoundExpr::ColumnRef(_))
                && list.iter().all(|e| matches!(e, BoundExpr::Literal(_))) =>
        {
            let col_idx = match expr.as_ref() {
                BoundExpr::ColumnRef(i) => *i,
                _ => unreachable!(),
            };
            let val = row.get(col_idx).unwrap_or(&Datum::Null);
            if val.is_null() {
                return Ok(false);
            }
            let found = list.iter().any(|e| match e {
                BoundExpr::Literal(d) if !d.is_null() => d == val,
                _ => false,
            });
            Ok(if *negated { !found } else { found })
        }
        // BETWEEN: col BETWEEN lit AND lit — borrow column value, compare directly
        BoundExpr::Between { expr: inner, low, high, negated }
            if matches!(inner.as_ref(), BoundExpr::ColumnRef(_))
                && matches!(low.as_ref(), BoundExpr::Literal(_))
                && matches!(high.as_ref(), BoundExpr::Literal(_)) =>
        {
            let col_idx = match inner.as_ref() {
                BoundExpr::ColumnRef(i) => *i,
                _ => unreachable!(),
            };
            let val = row.get(col_idx).unwrap_or(&Datum::Null);
            if val.is_null() {
                return Ok(false);
            }
            let lo = match low.as_ref() {
                BoundExpr::Literal(d) => d,
                _ => unreachable!(),
            };
            let hi = match high.as_ref() {
                BoundExpr::Literal(d) => d,
                _ => unreachable!(),
            };
            let in_range = val >= lo && val <= hi;
            Ok(if *negated { !in_range } else { in_range })
        }
        // LIKE / ILIKE: col LIKE 'pattern' — borrow column value, pre-lower literal pattern
        BoundExpr::Like { expr: inner, pattern, negated, case_insensitive }
            if matches!(inner.as_ref(), BoundExpr::ColumnRef(_))
                && matches!(pattern.as_ref(), BoundExpr::Literal(Datum::Text(_))) =>
        {
            let col_idx = match inner.as_ref() {
                BoundExpr::ColumnRef(i) => *i,
                _ => unreachable!(),
            };
            let pat_str = match pattern.as_ref() {
                BoundExpr::Literal(Datum::Text(s)) => s,
                _ => unreachable!(),
            };
            let val = row.get(col_idx).unwrap_or(&Datum::Null);
            match val {
                Datum::Text(s) => {
                    let matched = if *case_insensitive {
                        // Pattern is a literal — lower it once here (per filter call
                        // from eval_filter, but the caller's loop calls us per row;
                        // however the pattern lowering is cheap compared to eval_expr).
                        like_match(&s.to_lowercase(), &pat_str.to_lowercase())
                    } else {
                        like_match(s, pat_str)
                    };
                    Ok(if *negated { !matched } else { matched })
                }
                Datum::Null => Ok(false),
                _ => Ok(false),
            }
        }
        // Literal boolean (e.g. constant-folded TRUE/FALSE)
        BoundExpr::Literal(Datum::Boolean(b)) => Ok(*b),
        // General fallback
        _ => {
            let result = eval_expr(expr, row)?;
            Ok(result.as_bool().unwrap_or(false))
        }
    }
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
///
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
                let mut seen: std::collections::HashSet<Vec<u8>> =
                    std::collections::HashSet::new();
                let mut vals = Vec::new();
                let mut key_buf = Vec::with_capacity(32);
                for row in group_rows {
                    let v = eval_expr(e, row)?;
                    if v.is_null() {
                        continue;
                    }
                    // Byte-encode the datum for dedup — avoids format!() String allocation
                    key_buf.clear();
                    encode_datum_key(&mut key_buf, &v);
                    if seen.insert(key_buf.clone()) {
                        vals.push(v);
                    }
                }
                Ok(vals)
            };
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
                        .ok_or_else(|| ExecutionError::TypeError("SUM requires arg".into()))?;
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
                        .ok_or_else(|| ExecutionError::TypeError("AVG requires arg".into()))?;
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
                        .ok_or_else(|| ExecutionError::TypeError("MIN requires arg".into()))?;
                    let vals = eval_all(e)?;
                    Ok(vals.into_iter().min().unwrap_or(Datum::Null))
                }
                AggFunc::Max => {
                    let e = arg
                        .as_ref()
                        .ok_or_else(|| ExecutionError::TypeError("MAX requires arg".into()))?;
                    let vals = eval_all(e)?;
                    Ok(vals.into_iter().max().unwrap_or(Datum::Null))
                }
                AggFunc::BoolAnd => {
                    let e = arg
                        .as_ref()
                        .ok_or_else(|| ExecutionError::TypeError("BOOL_AND requires arg".into()))?;
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
                        .ok_or_else(|| ExecutionError::TypeError("BOOL_OR requires arg".into()))?;
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
                        .ok_or_else(|| ExecutionError::TypeError("STRING_AGG requires arg".into()))?;
                    let vals = eval_all(e)?;
                    if vals.is_empty() {
                        Ok(Datum::Null)
                    } else {
                        Ok(Datum::Text(
                            vals.iter()
                                .map(|v| format!("{v}"))
                                .collect::<Vec<_>>()
                                .join(sep),
                        ))
                    }
                }
                AggFunc::ArrayAgg => {
                    let e = arg
                        .as_ref()
                        .ok_or_else(|| ExecutionError::TypeError("ARRAY_AGG requires arg".into()))?;
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
                    let e = arg.as_ref().ok_or_else(|| ExecutionError::TypeError(
                        "Statistical aggregate requires arg".into(),
                    ))?;
                    let vals = if *distinct {
                        distinct_vals(e)?
                    } else {
                        eval_all(e)?
                    };
                    let floats: Vec<f64> = vals.iter().filter_map(falcon_common::datum::Datum::as_f64).collect();
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
                        .ok_or_else(|| ExecutionError::TypeError("MODE requires arg".into()))?;
                    let vals = eval_all(e)?;
                    if vals.is_empty() {
                        return Ok(Datum::Null);
                    }
                    let mut freq: Vec<(String, usize, Datum)> = Vec::new();
                    for v in vals {
                        let key = format!("{v}");
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
                        .map_or(Datum::Null, |(_, _, v)| v))
                }
                AggFunc::BitAndAgg => {
                    let e = arg
                        .as_ref()
                        .ok_or_else(|| ExecutionError::TypeError("BIT_AND requires arg".into()))?;
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
                    Ok(r.map_or(Datum::Null, Datum::Int64))
                }
                AggFunc::BitOrAgg => {
                    let e = arg
                        .as_ref()
                        .ok_or_else(|| ExecutionError::TypeError("BIT_OR requires arg".into()))?;
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
                        .ok_or_else(|| ExecutionError::TypeError("BIT_XOR requires arg".into()))?;
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
            Ok(val.as_bool().map_or(Datum::Null, |b| Datum::Boolean(!b)))
        }
        // For non-aggregate expressions, evaluate against first row
        _ => {
            group_rows.first().map_or(Ok(Datum::Null), |row| eval_expr(expr, row))
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
