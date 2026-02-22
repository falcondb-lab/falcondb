//! Predicate normalization for query optimization.
//!
//! Transforms:
//! - BETWEEN x AND y  →  expr >= x AND expr <= y
//! - NOT BETWEEN      →  expr < x OR expr > y
//! - IN (a, b, c)     →  equality set { a, b, c } (for shard key inference)
//! - Constant folding  →  evaluate pure-literal expressions at bind time
//! - CNF conversion    →  flatten nested ANDs into a conjunction list
//!
//! Also provides:
//! - Parameter type inference from schema context
//! - Volatile function detection for shard key safety

use std::collections::HashMap;

use falcon_common::datum::Datum;
use falcon_common::schema::TableSchema;
use falcon_common::types::DataType;

use crate::types::{BinOp, BoundExpr, ScalarFunc};

/// Normalize a bound expression tree.
/// Applies: BETWEEN expansion, constant folding, CNF flattening.
pub fn normalize_expr(expr: &BoundExpr) -> BoundExpr {
    let expanded = expand_between(expr);
    fold_constants(&expanded)
}

/// Expand BETWEEN into >= AND <= (or < OR > for NOT BETWEEN).
fn expand_between(expr: &BoundExpr) -> BoundExpr {
    match expr {
        BoundExpr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => {
            let inner_n = expand_between(inner);
            let low_n = expand_between(low);
            let high_n = expand_between(high);
            if *negated {
                // NOT BETWEEN → expr < low OR expr > high
                BoundExpr::BinaryOp {
                    left: Box::new(BoundExpr::BinaryOp {
                        left: Box::new(inner_n.clone()),
                        op: BinOp::Lt,
                        right: Box::new(low_n),
                    }),
                    op: BinOp::Or,
                    right: Box::new(BoundExpr::BinaryOp {
                        left: Box::new(inner_n),
                        op: BinOp::Gt,
                        right: Box::new(high_n),
                    }),
                }
            } else {
                // BETWEEN → expr >= low AND expr <= high
                BoundExpr::BinaryOp {
                    left: Box::new(BoundExpr::BinaryOp {
                        left: Box::new(inner_n.clone()),
                        op: BinOp::GtEq,
                        right: Box::new(low_n),
                    }),
                    op: BinOp::And,
                    right: Box::new(BoundExpr::BinaryOp {
                        left: Box::new(inner_n),
                        op: BinOp::LtEq,
                        right: Box::new(high_n),
                    }),
                }
            }
        }
        // Recurse into subexpressions
        BoundExpr::BinaryOp { left, op, right } => BoundExpr::BinaryOp {
            left: Box::new(expand_between(left)),
            op: *op,
            right: Box::new(expand_between(right)),
        },
        BoundExpr::Not(inner) => BoundExpr::Not(Box::new(expand_between(inner))),
        BoundExpr::IsNull(inner) => BoundExpr::IsNull(Box::new(expand_between(inner))),
        BoundExpr::IsNotNull(inner) => BoundExpr::IsNotNull(Box::new(expand_between(inner))),
        BoundExpr::Like {
            expr: inner,
            pattern,
            negated,
            case_insensitive,
        } => BoundExpr::Like {
            expr: Box::new(expand_between(inner)),
            pattern: Box::new(expand_between(pattern)),
            negated: *negated,
            case_insensitive: *case_insensitive,
        },
        BoundExpr::InList {
            expr: inner,
            list,
            negated,
        } => BoundExpr::InList {
            expr: Box::new(expand_between(inner)),
            list: list.iter().map(expand_between).collect(),
            negated: *negated,
        },
        BoundExpr::Cast {
            expr: inner,
            target_type,
        } => BoundExpr::Cast {
            expr: Box::new(expand_between(inner)),
            target_type: target_type.clone(),
        },
        BoundExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => BoundExpr::Case {
            operand: operand.as_ref().map(|e| Box::new(expand_between(e))),
            conditions: conditions.iter().map(expand_between).collect(),
            results: results.iter().map(expand_between).collect(),
            else_result: else_result.as_ref().map(|e| Box::new(expand_between(e))),
        },
        BoundExpr::Coalesce(args) => BoundExpr::Coalesce(args.iter().map(expand_between).collect()),
        BoundExpr::Function { func, args } => BoundExpr::Function {
            func: func.clone(),
            args: args.iter().map(expand_between).collect(),
        },
        BoundExpr::ArrayLiteral(elems) => {
            BoundExpr::ArrayLiteral(elems.iter().map(expand_between).collect())
        }
        BoundExpr::ArrayIndex { array, index } => BoundExpr::ArrayIndex {
            array: Box::new(expand_between(array)),
            index: Box::new(expand_between(index)),
        },
        BoundExpr::AnyOp {
            left,
            compare_op,
            right,
        } => BoundExpr::AnyOp {
            left: Box::new(expand_between(left)),
            compare_op: *compare_op,
            right: Box::new(expand_between(right)),
        },
        BoundExpr::AllOp {
            left,
            compare_op,
            right,
        } => BoundExpr::AllOp {
            left: Box::new(expand_between(left)),
            compare_op: *compare_op,
            right: Box::new(expand_between(right)),
        },
        BoundExpr::ArraySlice {
            array,
            lower,
            upper,
        } => BoundExpr::ArraySlice {
            array: Box::new(expand_between(array)),
            lower: lower.as_ref().map(|l| Box::new(expand_between(l))),
            upper: upper.as_ref().map(|u| Box::new(expand_between(u))),
        },
        // Leaf nodes and subqueries — return as-is
        _ => expr.clone(),
    }
}

/// Constant folding: evaluate pure-literal binary expressions at bind time.
fn fold_constants(expr: &BoundExpr) -> BoundExpr {
    match expr {
        BoundExpr::BinaryOp { left, op, right } => {
            let l = fold_constants(left);
            let r = fold_constants(right);
            // If both sides are literals, try to evaluate
            if let (BoundExpr::Literal(lv), BoundExpr::Literal(rv)) = (&l, &r) {
                if let Some(result) = eval_constant_binop(lv, *op, rv) {
                    return BoundExpr::Literal(result);
                }
            }
            BoundExpr::BinaryOp {
                left: Box::new(l),
                op: *op,
                right: Box::new(r),
            }
        }
        BoundExpr::Not(inner) => {
            let folded = fold_constants(inner);
            if let BoundExpr::Literal(Datum::Boolean(b)) = &folded {
                return BoundExpr::Literal(Datum::Boolean(!b));
            }
            BoundExpr::Not(Box::new(folded))
        }
        BoundExpr::IsNull(inner) => {
            let folded = fold_constants(inner);
            if let BoundExpr::Literal(ref d) = folded {
                return BoundExpr::Literal(Datum::Boolean(d.is_null()));
            }
            BoundExpr::IsNull(Box::new(folded))
        }
        BoundExpr::IsNotNull(inner) => {
            let folded = fold_constants(inner);
            if let BoundExpr::Literal(ref d) = folded {
                return BoundExpr::Literal(Datum::Boolean(!d.is_null()));
            }
            BoundExpr::IsNotNull(Box::new(folded))
        }
        BoundExpr::IsNotDistinctFrom { left, right } => {
            let l = fold_constants(left);
            let r = fold_constants(right);
            if let (BoundExpr::Literal(ref lv), BoundExpr::Literal(ref rv)) = (&l, &r) {
                let result = match (lv, rv) {
                    (Datum::Null, Datum::Null) => true,
                    (Datum::Null, _) | (_, Datum::Null) => false,
                    _ => lv == rv,
                };
                return BoundExpr::Literal(Datum::Boolean(result));
            }
            BoundExpr::IsNotDistinctFrom {
                left: Box::new(l),
                right: Box::new(r),
            }
        }
        BoundExpr::Cast {
            expr: inner,
            target_type,
        } => BoundExpr::Cast {
            expr: Box::new(fold_constants(inner)),
            target_type: target_type.clone(),
        },
        BoundExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => BoundExpr::Case {
            operand: operand.as_ref().map(|e| Box::new(fold_constants(e))),
            conditions: conditions.iter().map(fold_constants).collect(),
            results: results.iter().map(fold_constants).collect(),
            else_result: else_result.as_ref().map(|e| Box::new(fold_constants(e))),
        },
        BoundExpr::Coalesce(args) => BoundExpr::Coalesce(args.iter().map(fold_constants).collect()),
        BoundExpr::Function { func, args } => BoundExpr::Function {
            func: func.clone(),
            args: args.iter().map(fold_constants).collect(),
        },
        BoundExpr::Like {
            expr: inner,
            pattern,
            negated,
            case_insensitive,
        } => BoundExpr::Like {
            expr: Box::new(fold_constants(inner)),
            pattern: Box::new(fold_constants(pattern)),
            negated: *negated,
            case_insensitive: *case_insensitive,
        },
        BoundExpr::InList {
            expr: inner,
            list,
            negated,
        } => BoundExpr::InList {
            expr: Box::new(fold_constants(inner)),
            list: list.iter().map(fold_constants).collect(),
            negated: *negated,
        },
        BoundExpr::ArrayLiteral(elems) => {
            BoundExpr::ArrayLiteral(elems.iter().map(fold_constants).collect())
        }
        BoundExpr::ArrayIndex { array, index } => BoundExpr::ArrayIndex {
            array: Box::new(fold_constants(array)),
            index: Box::new(fold_constants(index)),
        },
        BoundExpr::AnyOp {
            left,
            compare_op,
            right,
        } => BoundExpr::AnyOp {
            left: Box::new(fold_constants(left)),
            compare_op: *compare_op,
            right: Box::new(fold_constants(right)),
        },
        BoundExpr::AllOp {
            left,
            compare_op,
            right,
        } => BoundExpr::AllOp {
            left: Box::new(fold_constants(left)),
            compare_op: *compare_op,
            right: Box::new(fold_constants(right)),
        },
        BoundExpr::ArraySlice {
            array,
            lower,
            upper,
        } => BoundExpr::ArraySlice {
            array: Box::new(fold_constants(array)),
            lower: lower.as_ref().map(|l| Box::new(fold_constants(l))),
            upper: upper.as_ref().map(|u| Box::new(fold_constants(u))),
        },
        // Leaf nodes — return as-is
        _ => expr.clone(),
    }
}

/// Evaluate a binary operation on two literal Datum values.
/// Returns None if the operation cannot be folded (e.g., non-numeric types for arithmetic).
fn eval_constant_binop(left: &Datum, op: BinOp, right: &Datum) -> Option<Datum> {
    match op {
        BinOp::Plus => eval_arith(left, right, |a, b| a + b, |a, b| a + b, |a, b| a + b),
        BinOp::Minus => eval_arith(left, right, |a, b| a - b, |a, b| a - b, |a, b| a - b),
        BinOp::Multiply => eval_arith(left, right, |a, b| a * b, |a, b| a * b, |a, b| a * b),
        BinOp::Divide => {
            // Avoid division by zero
            match right {
                Datum::Int32(0) | Datum::Int64(0) => None,
                Datum::Float64(f) if *f == 0.0 => None,
                _ => eval_arith(left, right, |a, b| a / b, |a, b| a / b, |a, b| a / b),
            }
        }
        BinOp::And => {
            if let (Datum::Boolean(a), Datum::Boolean(b)) = (left, right) {
                Some(Datum::Boolean(*a && *b))
            } else {
                None
            }
        }
        BinOp::Or => {
            if let (Datum::Boolean(a), Datum::Boolean(b)) = (left, right) {
                Some(Datum::Boolean(*a || *b))
            } else {
                None
            }
        }
        BinOp::StringConcat => {
            if let (Datum::Text(a), Datum::Text(b)) = (left, right) {
                Some(Datum::Text(format!("{}{}", a, b)))
            } else {
                None
            }
        }
        // Comparison ops on literals — not folded to keep things simple
        // (would need PartialOrd which has Null semantics issues)
        _ => None,
    }
}

fn eval_arith(
    left: &Datum,
    right: &Datum,
    i32_op: fn(i32, i32) -> i32,
    i64_op: fn(i64, i64) -> i64,
    f64_op: fn(f64, f64) -> f64,
) -> Option<Datum> {
    match (left, right) {
        (Datum::Int32(a), Datum::Int32(b)) => Some(Datum::Int32(i32_op(*a, *b))),
        (Datum::Int64(a), Datum::Int64(b)) => Some(Datum::Int64(i64_op(*a, *b))),
        (Datum::Float64(a), Datum::Float64(b)) => Some(Datum::Float64(f64_op(*a, *b))),
        (Datum::Int32(a), Datum::Int64(b)) => Some(Datum::Int64(i64_op(*a as i64, *b))),
        (Datum::Int64(a), Datum::Int32(b)) => Some(Datum::Int64(i64_op(*a, *b as i64))),
        (Datum::Int32(a), Datum::Float64(b)) => Some(Datum::Float64(f64_op(*a as f64, *b))),
        (Datum::Float64(a), Datum::Int32(b)) => Some(Datum::Float64(f64_op(*a, *b as f64))),
        (Datum::Int64(a), Datum::Float64(b)) => Some(Datum::Float64(f64_op(*a as f64, *b))),
        (Datum::Float64(a), Datum::Int64(b)) => Some(Datum::Float64(f64_op(*a, *b as f64))),
        _ => None,
    }
}

/// Extract a flat CNF (Conjunctive Normal Form) from an expression.
/// Flattens nested ANDs into a list of conjuncts.
/// This is useful for predicate pushdown and shard key inference.
pub fn to_cnf_conjuncts(expr: &BoundExpr) -> Vec<BoundExpr> {
    let mut conjuncts = Vec::new();
    collect_and_conjuncts(expr, &mut conjuncts);
    conjuncts
}

fn collect_and_conjuncts(expr: &BoundExpr, out: &mut Vec<BoundExpr>) {
    match expr {
        BoundExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_and_conjuncts(left, out);
            collect_and_conjuncts(right, out);
        }
        _ => out.push(expr.clone()),
    }
}

/// Reconstruct an AND-chain from a list of conjuncts.
pub fn from_cnf_conjuncts(conjuncts: &[BoundExpr]) -> Option<BoundExpr> {
    if conjuncts.is_empty() {
        return None;
    }
    let mut result = conjuncts[0].clone();
    for c in &conjuncts[1..] {
        result = BoundExpr::BinaryOp {
            left: Box::new(result),
            op: BinOp::And,
            right: Box::new(c.clone()),
        };
    }
    Some(result)
}

// ---------------------------------------------------------------------------
// 4.2: Parameter type inference
// ---------------------------------------------------------------------------

/// Inferred parameter types from schema context.
/// Maps parameter index (1-indexed) → inferred DataType.
pub type ParamTypes = HashMap<usize, DataType>;

/// Infer parameter types from a bound expression tree using the schema.
///
/// Walks the expression tree looking for patterns like:
/// - `ColumnRef(i) = Parameter(n)` → param n has the type of column i
/// - `ColumnRef(i) > Parameter(n)` → same for comparison ops
/// - `InList { expr: ColumnRef(i), list: [.., Parameter(n), ..] }` → same
///
/// Returns a map from parameter index to inferred DataType.
pub fn infer_param_types(expr: &BoundExpr, schema: &TableSchema) -> ParamTypes {
    let mut types = ParamTypes::new();
    collect_param_types(expr, schema, &mut types);
    types
}

fn collect_param_types(expr: &BoundExpr, schema: &TableSchema, out: &mut ParamTypes) {
    match expr {
        BoundExpr::BinaryOp { left, op, right } => {
            // Pattern: ColumnRef op Parameter → infer type from column
            // Works for both comparison and arithmetic ops
            if is_comparison(*op) || is_arithmetic(*op) {
                infer_from_col_param(left, right, schema, out);
                infer_from_col_param(right, left, schema, out);
            }
            // Arithmetic: Parameter + Literal(Int) → infer Int64
            if is_arithmetic(*op) {
                infer_from_literal_param(left, right, out);
                infer_from_literal_param(right, left, out);
            }
            collect_param_types(left, schema, out);
            collect_param_types(right, schema, out);
        }
        BoundExpr::InList {
            expr: inner, list, ..
        } => {
            // If inner is ColumnRef and list contains Parameters, infer from column
            if let BoundExpr::ColumnRef(col_idx) = inner.as_ref() {
                if let Some(col) = schema.columns.get(*col_idx) {
                    for item in list {
                        if let BoundExpr::Parameter(param_idx) = item {
                            out.entry(*param_idx)
                                .or_insert_with(|| col.data_type.clone());
                        }
                    }
                }
            }
            collect_param_types(inner, schema, out);
            for item in list {
                collect_param_types(item, schema, out);
            }
        }
        BoundExpr::Not(inner) | BoundExpr::IsNull(inner) | BoundExpr::IsNotNull(inner) => {
            collect_param_types(inner, schema, out);
        }
        BoundExpr::Cast {
            expr: inner,
            target_type,
        } => {
            // CAST($1 AS int) → param has the target type
            if let BoundExpr::Parameter(param_idx) = inner.as_ref() {
                if let Some(dt) = parse_cast_target(target_type) {
                    out.entry(*param_idx).or_insert(dt);
                }
            }
            collect_param_types(inner, schema, out);
        }
        BoundExpr::Like {
            expr: inner,
            pattern,
            ..
        } => {
            // LIKE pattern is always Text
            if let BoundExpr::Parameter(param_idx) = pattern.as_ref() {
                out.entry(*param_idx).or_insert(DataType::Text);
            }
            // If inner is a parameter compared via LIKE, it's text
            if let BoundExpr::Parameter(param_idx) = inner.as_ref() {
                out.entry(*param_idx).or_insert(DataType::Text);
            }
            collect_param_types(inner, schema, out);
            collect_param_types(pattern, schema, out);
        }
        BoundExpr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            // ColumnRef BETWEEN Parameter AND Parameter
            if let BoundExpr::ColumnRef(col_idx) = inner.as_ref() {
                if let Some(col) = schema.columns.get(*col_idx) {
                    if let BoundExpr::Parameter(p) = low.as_ref() {
                        out.entry(*p).or_insert_with(|| col.data_type.clone());
                    }
                    if let BoundExpr::Parameter(p) = high.as_ref() {
                        out.entry(*p).or_insert_with(|| col.data_type.clone());
                    }
                }
            }
            collect_param_types(inner, schema, out);
            collect_param_types(low, schema, out);
            collect_param_types(high, schema, out);
        }
        BoundExpr::IsNotDistinctFrom { left, right } => {
            // IS NOT DISTINCT FROM behaves like equality for type inference
            infer_from_col_param(left, right, schema, out);
            infer_from_col_param(right, left, schema, out);
            collect_param_types(left, schema, out);
            collect_param_types(right, schema, out);
        }
        BoundExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            // CASE operand WHEN Parameter → operand type
            if let Some(op) = operand {
                if let BoundExpr::ColumnRef(col_idx) = op.as_ref() {
                    if let Some(col) = schema.columns.get(*col_idx) {
                        for c in conditions {
                            if let BoundExpr::Parameter(p) = c {
                                out.entry(*p).or_insert_with(|| col.data_type.clone());
                            }
                        }
                    }
                }
                collect_param_types(op, schema, out);
            }
            for c in conditions {
                collect_param_types(c, schema, out);
            }
            for r in results {
                collect_param_types(r, schema, out);
            }
            if let Some(e) = else_result {
                collect_param_types(e, schema, out);
            }
        }
        BoundExpr::Coalesce(args) => {
            // COALESCE: if any sibling is a ColumnRef, infer type for Parameter siblings
            let col_type = args.iter().find_map(|a| {
                if let BoundExpr::ColumnRef(col_idx) = a {
                    schema.columns.get(*col_idx).map(|c| c.data_type.clone())
                } else {
                    None
                }
            });
            if let Some(ref dt) = col_type {
                for a in args {
                    if let BoundExpr::Parameter(p) = a {
                        out.entry(*p).or_insert_with(|| dt.clone());
                    }
                }
            }
            for a in args {
                collect_param_types(a, schema, out);
            }
        }
        BoundExpr::ArrayLiteral(args) => {
            for a in args {
                collect_param_types(a, schema, out);
            }
        }
        BoundExpr::Function { func, args } => {
            // Infer parameter types from well-known function signatures
            infer_function_param_types(func, args, out);
            for a in args {
                collect_param_types(a, schema, out);
            }
        }
        BoundExpr::ArrayIndex { array, index } => {
            // Array index is always an integer
            if let BoundExpr::Parameter(p) = index.as_ref() {
                out.entry(*p).or_insert(DataType::Int32);
            }
            collect_param_types(array, schema, out);
            collect_param_types(index, schema, out);
        }
        BoundExpr::AnyOp { left, right, .. } | BoundExpr::AllOp { left, right, .. } => {
            infer_from_col_param(left, right, schema, out);
            infer_from_col_param(right, left, schema, out);
            collect_param_types(left, schema, out);
            collect_param_types(right, schema, out);
        }
        BoundExpr::ArraySlice {
            array,
            lower,
            upper,
        } => {
            // Slice bounds are integers
            if let Some(l) = lower {
                if let BoundExpr::Parameter(p) = l.as_ref() {
                    out.entry(*p).or_insert(DataType::Int32);
                }
                collect_param_types(l, schema, out);
            }
            if let Some(u) = upper {
                if let BoundExpr::Parameter(p) = u.as_ref() {
                    out.entry(*p).or_insert(DataType::Int32);
                }
                collect_param_types(u, schema, out);
            }
            collect_param_types(array, schema, out);
        }
        // Leaf nodes — nothing to infer
        _ => {}
    }
}

/// If `col_side` is ColumnRef and `param_side` is Parameter, infer the param type
/// from the column's data type.
fn infer_from_col_param(
    col_side: &BoundExpr,
    param_side: &BoundExpr,
    schema: &TableSchema,
    out: &mut ParamTypes,
) {
    if let (BoundExpr::ColumnRef(col_idx), BoundExpr::Parameter(param_idx)) = (col_side, param_side)
    {
        if let Some(col) = schema.columns.get(*col_idx) {
            out.entry(*param_idx)
                .or_insert_with(|| col.data_type.clone());
        }
    }
}

/// If `lit_side` is a Literal and `param_side` is Parameter, infer from the literal type.
fn infer_from_literal_param(lit_side: &BoundExpr, param_side: &BoundExpr, out: &mut ParamTypes) {
    if let (BoundExpr::Literal(datum), BoundExpr::Parameter(param_idx)) = (lit_side, param_side) {
        let dt = match datum {
            Datum::Int32(_) => DataType::Int32,
            Datum::Int64(_) => DataType::Int64,
            Datum::Float64(_) => DataType::Float64,
            Datum::Boolean(_) => DataType::Boolean,
            Datum::Text(_) => DataType::Text,
            _ => return,
        };
        out.entry(*param_idx).or_insert(dt);
    }
}

/// Parse a CAST target type string into a DataType.
fn parse_cast_target(target: &str) -> Option<DataType> {
    let t = target.to_lowercase();
    match t.as_str() {
        "int" | "int4" | "integer" => Some(DataType::Int32),
        "bigint" | "int8" => Some(DataType::Int64),
        "float" | "float8" | "double precision" | "double" | "real" | "numeric" | "decimal" => {
            Some(DataType::Float64)
        }
        "text" | "varchar" | "char" | "character varying" | "character" | "bpchar" => {
            Some(DataType::Text)
        }
        "boolean" | "bool" => Some(DataType::Boolean),
        "timestamp" | "timestamp without time zone" | "timestamptz" => Some(DataType::Timestamp),
        "date" => Some(DataType::Date),
        "jsonb" | "json" => Some(DataType::Jsonb),
        _ => None,
    }
}

/// Infer parameter types from well-known scalar function signatures.
fn infer_function_param_types(func: &ScalarFunc, args: &[BoundExpr], out: &mut ParamTypes) {
    use ScalarFunc::*;
    // Functions whose arguments have known types
    match func {
        // String functions: all args are text
        Upper | Lower | Trim | Ltrim | Rtrim | Length | OctetLength | Concat | ConcatWs
        | Replace | Repeat | Reverse | Left | Right | Lpad | Rpad | Md5 | Sha256 | Initcap
        | Translate | Encode | Decode | StartsWith => {
            for a in args {
                if let BoundExpr::Parameter(p) = a {
                    out.entry(*p).or_insert(DataType::Text);
                }
            }
        }
        // Substring: (text, int, int)
        Substring => {
            if let Some(BoundExpr::Parameter(p)) = args.first() {
                out.entry(*p).or_insert(DataType::Text);
            }
            for a in args.iter().skip(1) {
                if let BoundExpr::Parameter(p) = a {
                    out.entry(*p).or_insert(DataType::Int32);
                }
            }
        }
        // Position: (text, text) → int
        Position => {
            for a in args {
                if let BoundExpr::Parameter(p) = a {
                    out.entry(*p).or_insert(DataType::Text);
                }
            }
        }
        // Math functions: numeric args
        Abs | Ceil | Floor | Round | Trunc | Sqrt | Power | Log | Ln | Exp | Sign | Mod => {
            for a in args {
                if let BoundExpr::Parameter(p) = a {
                    out.entry(*p).or_insert(DataType::Float64);
                }
            }
        }
        // Any other function — don't infer
        _ => {}
    }
}

fn is_arithmetic(op: BinOp) -> bool {
    matches!(
        op,
        BinOp::Plus | BinOp::Minus | BinOp::Multiply | BinOp::Divide | BinOp::Modulo
    )
}

fn is_comparison(op: BinOp) -> bool {
    matches!(
        op,
        BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq
    )
}

// ---------------------------------------------------------------------------
// 4.3 supplement: IN → equality set
// ---------------------------------------------------------------------------

/// An equality set extracted from an IN-list predicate.
/// Represents: `column_idx IN (values...)`.
#[derive(Debug, Clone)]
pub struct EqualitySet {
    /// Column index the IN-list applies to.
    pub column_idx: usize,
    /// The literal values in the IN-list (parameters are excluded).
    pub values: Vec<Datum>,
    /// Whether the IN was negated (NOT IN).
    pub negated: bool,
}

/// Extract equality sets from a normalized expression's CNF conjuncts.
///
/// For each conjunct of the form `ColumnRef(i) IN (lit1, lit2, ...)`,
/// produces an `EqualitySet`.
pub fn extract_equality_sets(expr: &BoundExpr) -> Vec<EqualitySet> {
    let conjuncts = to_cnf_conjuncts(expr);
    let mut sets = Vec::new();
    for c in &conjuncts {
        if let BoundExpr::InList {
            expr: inner,
            list,
            negated,
        } = c
        {
            if let BoundExpr::ColumnRef(col_idx) = inner.as_ref() {
                let values: Vec<Datum> = list
                    .iter()
                    .filter_map(|e| {
                        if let BoundExpr::Literal(d) = e {
                            Some(d.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if !values.is_empty() {
                    sets.push(EqualitySet {
                        column_idx: *col_idx,
                        values,
                        negated: *negated,
                    });
                }
            }
        }
        // Also detect `ColumnRef = Literal` as a single-value equality set
        if let BoundExpr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = c
        {
            if let (BoundExpr::ColumnRef(col_idx), BoundExpr::Literal(val)) =
                (left.as_ref(), right.as_ref())
            {
                sets.push(EqualitySet {
                    column_idx: *col_idx,
                    values: vec![val.clone()],
                    negated: false,
                });
            }
            if let (BoundExpr::Literal(val), BoundExpr::ColumnRef(col_idx)) =
                (left.as_ref(), right.as_ref())
            {
                sets.push(EqualitySet {
                    column_idx: *col_idx,
                    values: vec![val.clone()],
                    negated: false,
                });
            }
        }
        // IS NOT DISTINCT FROM is NULL-safe equality — treat as equality for routing
        if let BoundExpr::IsNotDistinctFrom { left, right } = c {
            if let (BoundExpr::ColumnRef(col_idx), BoundExpr::Literal(val)) =
                (left.as_ref(), right.as_ref())
            {
                sets.push(EqualitySet {
                    column_idx: *col_idx,
                    values: vec![val.clone()],
                    negated: false,
                });
            }
            if let (BoundExpr::Literal(val), BoundExpr::ColumnRef(col_idx)) =
                (left.as_ref(), right.as_ref())
            {
                sets.push(EqualitySet {
                    column_idx: *col_idx,
                    values: vec![val.clone()],
                    negated: false,
                });
            }
        }
    }
    sets
}

// ---------------------------------------------------------------------------
// 4.4 supplement: Volatile function detection
// ---------------------------------------------------------------------------

/// Check if a scalar function is volatile (non-deterministic).
///
/// Volatile functions include:
/// - RANDOM, GEN_RANDOM_UUID
/// - NOW, CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME
/// - CLOCK_TIMESTAMP, STATEMENT_TIMESTAMP, TIMEOFDAY
///
/// These must NOT participate in shard key inference because their
/// results are non-deterministic and differ between nodes.
pub fn is_volatile(func: &ScalarFunc) -> bool {
    matches!(
        func,
        ScalarFunc::Random
            | ScalarFunc::GenRandomUuid
            | ScalarFunc::Now
            | ScalarFunc::CurrentDate
            | ScalarFunc::CurrentTime
            | ScalarFunc::ClockTimestamp
            | ScalarFunc::StatementTimestamp
            | ScalarFunc::Timeofday
    )
}

/// Check if a bound expression contains any volatile function call.
///
/// Used by shard key inference to reject predicates that contain
/// non-deterministic expressions.
pub fn expr_has_volatile(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::Function { func, args } => {
            is_volatile(func) || args.iter().any(expr_has_volatile)
        }
        BoundExpr::BinaryOp { left, right, .. } => {
            expr_has_volatile(left) || expr_has_volatile(right)
        }
        BoundExpr::Not(inner)
        | BoundExpr::IsNull(inner)
        | BoundExpr::IsNotNull(inner)
        | BoundExpr::Cast { expr: inner, .. } => expr_has_volatile(inner),
        BoundExpr::Like {
            expr: inner,
            pattern,
            ..
        } => expr_has_volatile(inner) || expr_has_volatile(pattern),
        BoundExpr::Between {
            expr: inner,
            low,
            high,
            ..
        } => expr_has_volatile(inner) || expr_has_volatile(low) || expr_has_volatile(high),
        BoundExpr::InList {
            expr: inner, list, ..
        } => expr_has_volatile(inner) || list.iter().any(expr_has_volatile),
        BoundExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_deref().is_some_and(expr_has_volatile)
                || conditions.iter().any(expr_has_volatile)
                || results.iter().any(expr_has_volatile)
                || else_result.as_deref().is_some_and(expr_has_volatile)
        }
        BoundExpr::Coalesce(args) | BoundExpr::ArrayLiteral(args) => {
            args.iter().any(expr_has_volatile)
        }
        BoundExpr::ArrayIndex { array, index } => {
            expr_has_volatile(array) || expr_has_volatile(index)
        }
        BoundExpr::AnyOp { left, right, .. } | BoundExpr::AllOp { left, right, .. } => {
            expr_has_volatile(left) || expr_has_volatile(right)
        }
        BoundExpr::ArraySlice {
            array,
            lower,
            upper,
        } => {
            expr_has_volatile(array)
                || lower.as_deref().is_some_and(expr_has_volatile)
                || upper.as_deref().is_some_and(expr_has_volatile)
        }
        // Leaf nodes: Literal, ColumnRef, Parameter, etc. — not volatile
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests for extended parameter type inference
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "t".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "name".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "score".into(),
                    data_type: DataType::Float64,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        }
    }

    #[test]
    fn test_infer_comparison_col_eq_param() {
        let schema = test_schema();
        // id = $1
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Parameter(1)),
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Int64));
    }

    #[test]
    fn test_infer_comparison_param_eq_col() {
        let schema = test_schema();
        // $1 = name
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Parameter(1)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::ColumnRef(1)),
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Text));
    }

    #[test]
    fn test_infer_arithmetic_col_plus_param() {
        let schema = test_schema();
        // score + $1
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(2)),
            op: BinOp::Plus,
            right: Box::new(BoundExpr::Parameter(1)),
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Float64));
    }

    #[test]
    fn test_infer_arithmetic_literal_plus_param() {
        let schema = test_schema();
        // 42 + $1
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Literal(Datum::Int64(42))),
            op: BinOp::Plus,
            right: Box::new(BoundExpr::Parameter(1)),
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Int64));
    }

    #[test]
    fn test_infer_cast_param_as_int() {
        let schema = test_schema();
        // CAST($1 AS integer)
        let expr = BoundExpr::Cast {
            expr: Box::new(BoundExpr::Parameter(1)),
            target_type: "integer".into(),
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Int32));
    }

    #[test]
    fn test_infer_cast_param_as_text() {
        let schema = test_schema();
        let expr = BoundExpr::Cast {
            expr: Box::new(BoundExpr::Parameter(1)),
            target_type: "varchar".into(),
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Text));
    }

    #[test]
    fn test_infer_cast_param_as_boolean() {
        let schema = test_schema();
        let expr = BoundExpr::Cast {
            expr: Box::new(BoundExpr::Parameter(1)),
            target_type: "bool".into(),
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Boolean));
    }

    #[test]
    fn test_infer_like_pattern_is_text() {
        let schema = test_schema();
        // name LIKE $1
        let expr = BoundExpr::Like {
            expr: Box::new(BoundExpr::ColumnRef(1)),
            pattern: Box::new(BoundExpr::Parameter(1)),
            negated: false,
            case_insensitive: false,
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Text));
    }

    #[test]
    fn test_infer_is_not_distinct_from() {
        let schema = test_schema();
        // id IS NOT DISTINCT FROM $1
        let expr = BoundExpr::IsNotDistinctFrom {
            left: Box::new(BoundExpr::ColumnRef(0)),
            right: Box::new(BoundExpr::Parameter(1)),
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Int64));
    }

    #[test]
    fn test_infer_coalesce_with_column() {
        let schema = test_schema();
        // COALESCE(name, $1)
        let expr = BoundExpr::Coalesce(vec![BoundExpr::ColumnRef(1), BoundExpr::Parameter(1)]);
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Text));
    }

    #[test]
    fn test_infer_function_upper_param_is_text() {
        let schema = test_schema();
        // UPPER($1)
        let expr = BoundExpr::Function {
            func: ScalarFunc::Upper,
            args: vec![BoundExpr::Parameter(1)],
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Text));
    }

    #[test]
    fn test_infer_function_abs_param_is_float() {
        let schema = test_schema();
        // ABS($1)
        let expr = BoundExpr::Function {
            func: ScalarFunc::Abs,
            args: vec![BoundExpr::Parameter(1)],
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Float64));
    }

    #[test]
    fn test_infer_function_substring_mixed() {
        let schema = test_schema();
        // SUBSTRING($1, $2, $3)
        let expr = BoundExpr::Function {
            func: ScalarFunc::Substring,
            args: vec![
                BoundExpr::Parameter(1),
                BoundExpr::Parameter(2),
                BoundExpr::Parameter(3),
            ],
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Text));
        assert_eq!(types.get(&2), Some(&DataType::Int32));
        assert_eq!(types.get(&3), Some(&DataType::Int32));
    }

    #[test]
    fn test_infer_array_index_is_int() {
        let schema = test_schema();
        // arr[$1]
        let expr = BoundExpr::ArrayIndex {
            array: Box::new(BoundExpr::ColumnRef(0)),
            index: Box::new(BoundExpr::Parameter(1)),
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Int32));
    }

    #[test]
    fn test_infer_array_slice_bounds_are_int() {
        let schema = test_schema();
        // arr[$1:$2]
        let expr = BoundExpr::ArraySlice {
            array: Box::new(BoundExpr::ColumnRef(0)),
            lower: Some(Box::new(BoundExpr::Parameter(1))),
            upper: Some(Box::new(BoundExpr::Parameter(2))),
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Int32));
        assert_eq!(types.get(&2), Some(&DataType::Int32));
    }

    #[test]
    fn test_infer_between_params() {
        let schema = test_schema();
        // id BETWEEN $1 AND $2
        let expr = BoundExpr::Between {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            low: Box::new(BoundExpr::Parameter(1)),
            high: Box::new(BoundExpr::Parameter(2)),
            negated: false,
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Int64));
        assert_eq!(types.get(&2), Some(&DataType::Int64));
    }

    #[test]
    fn test_infer_in_list_params() {
        let schema = test_schema();
        // id IN ($1, $2)
        let expr = BoundExpr::InList {
            expr: Box::new(BoundExpr::ColumnRef(0)),
            list: vec![BoundExpr::Parameter(1), BoundExpr::Parameter(2)],
            negated: false,
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Int64));
        assert_eq!(types.get(&2), Some(&DataType::Int64));
    }

    #[test]
    fn test_infer_multiple_params_in_and() {
        let schema = test_schema();
        // id = $1 AND name = $2
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(0)),
                op: BinOp::Eq,
                right: Box::new(BoundExpr::Parameter(1)),
            }),
            op: BinOp::And,
            right: Box::new(BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(1)),
                op: BinOp::Eq,
                right: Box::new(BoundExpr::Parameter(2)),
            }),
        };
        let types = infer_param_types(&expr, &schema);
        assert_eq!(types.get(&1), Some(&DataType::Int64));
        assert_eq!(types.get(&2), Some(&DataType::Text));
    }

    #[test]
    fn test_parse_cast_target_types() {
        assert_eq!(parse_cast_target("int"), Some(DataType::Int32));
        assert_eq!(parse_cast_target("integer"), Some(DataType::Int32));
        assert_eq!(parse_cast_target("bigint"), Some(DataType::Int64));
        assert_eq!(parse_cast_target("float8"), Some(DataType::Float64));
        assert_eq!(parse_cast_target("text"), Some(DataType::Text));
        assert_eq!(parse_cast_target("varchar"), Some(DataType::Text));
        assert_eq!(parse_cast_target("boolean"), Some(DataType::Boolean));
        assert_eq!(parse_cast_target("timestamp"), Some(DataType::Timestamp));
        assert_eq!(parse_cast_target("date"), Some(DataType::Date));
        assert_eq!(parse_cast_target("jsonb"), Some(DataType::Jsonb));
        assert_eq!(parse_cast_target("unknown_type"), None);
    }
}
