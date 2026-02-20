#![allow(clippy::too_many_arguments)]

use cedar_common::datum::{Datum, OwnedRow};
use cedar_common::error::{CedarError, ExecutionError};
use cedar_common::schema::TableSchema;
use cedar_common::types::DataType;
use cedar_sql_frontend::types::*;

use crate::expr_engine::ExprEngine;
use crate::executor::{ExecutionResult, Executor};

impl Executor {
    pub(crate) fn exec_aggregate(
        &self,
        raw_rows: &[(Vec<u8>, OwnedRow)],
        schema: &TableSchema,
        projections: &[BoundProjection],
        filter: Option<&BoundExpr>,
        group_by: &[usize],
        grouping_sets: &[Vec<usize>],
        having: Option<&BoundExpr>,
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: &DistinctMode,
    ) -> Result<ExecutionResult, CedarError> {
        // Filter first (WHERE)
        let mut filtered: Vec<&OwnedRow> = Vec::new();
        for (_pk, row) in raw_rows {
            if let Some(f) = filter {
                if !ExprEngine::eval_filter(f, row).map_err(CedarError::Execution)? {
                    continue;
                }
            }
            filtered.push(row);
        }

        let columns = self.resolve_output_columns(projections, schema);

        // GROUPING SETS: run aggregation once per set, UNION ALL results.
        if !grouping_sets.is_empty() {
            let mut result_rows: Vec<OwnedRow> = Vec::new();
            // Collect the union of all columns referenced across all sets
            // so we know which columns are "nulled out" per set.
            for gset in grouping_sets {
                let set_rows = self.aggregate_one_grouping_set(
                    projections, &filtered, gset, group_by, having,
                )?;
                result_rows.extend(set_rows);
            }
            return self.finalize_aggregate(result_rows, order_by, distinct, offset, limit, columns);
        }

        if group_by.is_empty() {
            // No GROUP BY — single group over all filtered rows
            let result_values = self.compute_group_row(projections, &filtered)?;
            return Ok(ExecutionResult::Query {
                columns,
                rows: vec![OwnedRow::new(result_values)],
            });
        }

        // GROUP BY: build groups using a Vec-based approach (key = group column values)
        let mut groups: Vec<(Vec<Datum>, Vec<usize>)> = Vec::new(); // (key_datums, row_indices)
        for (i, row) in filtered.iter().enumerate() {
            let key: Vec<Datum> = group_by
                .iter()
                .map(|&col_idx| row.get(col_idx).cloned().unwrap_or(Datum::Null))
                .collect();

            if let Some(pos) = groups.iter().position(|(k, _)| {
                k.len() == key.len()
                    && k.iter()
                        .zip(key.iter())
                        .all(|(a, b)| format!("{}", a) == format!("{}", b))
            }) {
                groups[pos].1.push(i);
            } else {
                groups.push((key, vec![i]));
            }
        }

        // Compute aggregates per group
        let mut result_rows: Vec<OwnedRow> = Vec::new();
        for (_key, indices) in &groups {
            let group_rows: Vec<&OwnedRow> = indices.iter().map(|&i| filtered[i]).collect();
            let row_values = self.compute_group_row(projections, &group_rows)?;
            let result_row = OwnedRow::new(row_values);

            // Apply HAVING filter — evaluates aggregate expressions over the group
            if let Some(h) = having {
                if !ExprEngine::eval_having_filter(h, &group_rows).map_err(CedarError::Execution)? {
                    continue;
                }
            }

            result_rows.push(result_row);
        }

        self.finalize_aggregate(result_rows, order_by, distinct, offset, limit, columns)
    }

    /// Post-aggregation: ORDER BY, DISTINCT, OFFSET, LIMIT.
    fn finalize_aggregate(
        &self,
        mut result_rows: Vec<OwnedRow>,
        order_by: &[BoundOrderBy],
        distinct: &DistinctMode,
        offset: Option<usize>,
        limit: Option<usize>,
        columns: Vec<(String, DataType)>,
    ) -> Result<ExecutionResult, CedarError> {
        // Order by
        crate::external_sort::sort_rows(&mut result_rows, order_by, self.external_sorter.as_ref())?;

        // DISTINCT
        if !matches!(distinct, DistinctMode::None) {
            let mut seen = std::collections::HashSet::new();
            result_rows.retain(|row| {
                let key = row
                    .values
                    .iter()
                    .map(|d| format!("{}", d))
                    .collect::<Vec<_>>()
                    .join("\0");
                seen.insert(key)
            });
        }

        // Offset
        if let Some(off) = offset {
            if off < result_rows.len() {
                result_rows = result_rows.split_off(off);
            } else {
                result_rows.clear();
            }
        }

        // Limit
        if let Some(lim) = limit {
            result_rows.truncate(lim);
        }

        Ok(ExecutionResult::Query {
            columns,
            rows: result_rows,
        })
    }

    /// Run aggregation for one grouping set. Columns in `all_group_cols` but NOT
    /// in `active_set` are output as NULL (PostgreSQL semantics).
    fn aggregate_one_grouping_set(
        &self,
        projections: &[BoundProjection],
        filtered: &[&OwnedRow],
        active_set: &[usize],
        all_group_cols: &[usize],
        having: Option<&BoundExpr>,
    ) -> Result<Vec<OwnedRow>, CedarError> {
        if active_set.is_empty() {
            // Grand total: single group over all rows
            let row_values = self.compute_group_row_with_nulls(
                projections, filtered, active_set, all_group_cols,
            )?;
            if let Some(h) = having {
                if !ExprEngine::eval_having_filter(h, filtered).map_err(CedarError::Execution)? {
                    return Ok(vec![]);
                }
            }
            return Ok(vec![OwnedRow::new(row_values)]);
        }

        // Build groups by active_set columns only
        let mut groups: Vec<(Vec<Datum>, Vec<usize>)> = Vec::new();
        for (i, row) in filtered.iter().enumerate() {
            let key: Vec<Datum> = active_set
                .iter()
                .map(|&col_idx| row.get(col_idx).cloned().unwrap_or(Datum::Null))
                .collect();

            if let Some(pos) = groups.iter().position(|(k, _)| {
                k.len() == key.len()
                    && k.iter()
                        .zip(key.iter())
                        .all(|(a, b)| format!("{}", a) == format!("{}", b))
            }) {
                groups[pos].1.push(i);
            } else {
                groups.push((key, vec![i]));
            }
        }

        let mut result_rows = Vec::new();
        for (_key, indices) in &groups {
            let group_rows: Vec<&OwnedRow> = indices.iter().map(|&i| filtered[i]).collect();
            let row_values = self.compute_group_row_with_nulls(
                projections, &group_rows, active_set, all_group_cols,
            )?;

            if let Some(h) = having {
                if !ExprEngine::eval_having_filter(h, &group_rows).map_err(CedarError::Execution)? {
                    continue;
                }
            }
            result_rows.push(OwnedRow::new(row_values));
        }
        Ok(result_rows)
    }

    /// Like compute_group_row but NULLs out columns that are in all_group_cols
    /// but NOT in active_set (GROUPING SETS semantics).
    fn compute_group_row_with_nulls(
        &self,
        projections: &[BoundProjection],
        group_rows: &[&OwnedRow],
        active_set: &[usize],
        all_group_cols: &[usize],
    ) -> Result<Vec<Datum>, CedarError> {
        let mut values = Vec::new();
        for proj in projections {
            match proj {
                BoundProjection::Aggregate(func, agg_expr, _, agg_distinct, agg_filter) => {
                    let filtered_rows = Self::apply_agg_filter(group_rows, agg_filter.as_deref(), active_set, all_group_cols);
                    let val = self.compute_aggregate(func, agg_expr.as_ref(), *agg_distinct, &filtered_rows)?;
                    values.push(val);
                }
                BoundProjection::Column(idx, _) => {
                    // If this column is a group-by column but not in the active set, output NULL
                    if all_group_cols.contains(idx) && !active_set.contains(idx) {
                        values.push(Datum::Null);
                    } else {
                        let val = group_rows
                            .first()
                            .and_then(|r| r.get(*idx).cloned())
                            .unwrap_or(Datum::Null);
                        values.push(val);
                    }
                }
                BoundProjection::Expr(expr, _) => {
                    // Substitute GROUPING() nodes with computed bitmask before eval
                    let substituted = Self::substitute_grouping(expr, active_set, all_group_cols);
                    let empty = OwnedRow::new(vec![]);
                    let dummy = group_rows.first().copied().unwrap_or(&empty);
                    let val = ExprEngine::eval_row(&substituted, dummy).map_err(CedarError::Execution)?;
                    values.push(val);
                }
                BoundProjection::Window(..) => {
                    values.push(Datum::Null);
                }
            }
        }
        Ok(values)
    }

    /// Recursively substitute BoundExpr::Grouping nodes with literal bitmask values.
    /// For GROUPING(col1, col2, ...), bit i is 1 if col_i is NOT in the active_set
    /// (super-aggregate NULL), 0 if it IS in the active_set (real group-by value).
    fn substitute_grouping(
        expr: &BoundExpr,
        active_set: &[usize],
        all_group_cols: &[usize],
    ) -> BoundExpr {
        match expr {
            BoundExpr::Grouping(cols) => {
                let mut bitmask: i32 = 0;
                for (i, col_idx) in cols.iter().enumerate() {
                    if all_group_cols.contains(col_idx) && !active_set.contains(col_idx) {
                        bitmask |= 1 << (cols.len() - 1 - i);
                    }
                }
                BoundExpr::Literal(Datum::Int32(bitmask))
            }
            BoundExpr::BinaryOp { left, op, right } => BoundExpr::BinaryOp {
                left: Box::new(Self::substitute_grouping(left, active_set, all_group_cols)),
                op: *op,
                right: Box::new(Self::substitute_grouping(right, active_set, all_group_cols)),
            },
            BoundExpr::Not(inner) => BoundExpr::Not(
                Box::new(Self::substitute_grouping(inner, active_set, all_group_cols)),
            ),
            BoundExpr::Case { operand, conditions, results, else_result } => BoundExpr::Case {
                operand: operand.as_ref().map(|e| Box::new(Self::substitute_grouping(e, active_set, all_group_cols))),
                conditions: conditions.iter().map(|c| Self::substitute_grouping(c, active_set, all_group_cols)).collect(),
                results: results.iter().map(|r| Self::substitute_grouping(r, active_set, all_group_cols)).collect(),
                else_result: else_result.as_ref().map(|e| Box::new(Self::substitute_grouping(e, active_set, all_group_cols))),
            },
            BoundExpr::Cast { expr: inner, target_type } => BoundExpr::Cast {
                expr: Box::new(Self::substitute_grouping(inner, active_set, all_group_cols)),
                target_type: target_type.clone(),
            },
            BoundExpr::Coalesce(args) => BoundExpr::Coalesce(
                args.iter().map(|a| Self::substitute_grouping(a, active_set, all_group_cols)).collect(),
            ),
            BoundExpr::Function { func, args } => BoundExpr::Function {
                func: func.clone(),
                args: args.iter().map(|a| Self::substitute_grouping(a, active_set, all_group_cols)).collect(),
            },
            // Leaf nodes and others: return as-is
            other => other.clone(),
        }
    }

    /// Filter group rows by an aggregate FILTER clause.
    /// If no filter, returns the original rows unchanged (as owned copies).
    /// When active_set/all_group_cols are non-empty (GROUPING SETS context),
    /// GROUPING() nodes in the filter are substituted before evaluation.
    fn apply_agg_filter<'a>(
        rows: &[&'a OwnedRow],
        filter: Option<&BoundExpr>,
        active_set: &[usize],
        all_group_cols: &[usize],
    ) -> Vec<&'a OwnedRow> {
        match filter {
            None => rows.to_vec(),
            Some(expr) => {
                let substituted = if !all_group_cols.is_empty() {
                    Self::substitute_grouping(expr, active_set, all_group_cols)
                } else {
                    expr.clone()
                };
                rows.iter()
                    .copied()
                    .filter(|row| {
                        matches!(ExprEngine::eval_row(&substituted, row), Ok(Datum::Boolean(true)))
                    })
                    .collect()
            }
        }
    }

    /// Compute one output row for a group of source rows.
    pub(crate) fn compute_group_row(
        &self,
        projections: &[BoundProjection],
        group_rows: &[&OwnedRow],
    ) -> Result<Vec<Datum>, CedarError> {
        let mut values = Vec::new();
        for proj in projections {
            match proj {
                BoundProjection::Aggregate(func, agg_expr, _, agg_distinct, agg_filter) => {
                    let filtered_rows = Self::apply_agg_filter(group_rows, agg_filter.as_deref(), &[], &[]);
                    let val = self.compute_aggregate(func, agg_expr.as_ref(), *agg_distinct, &filtered_rows)?;
                    values.push(val);
                }
                BoundProjection::Column(idx, _) => {
                    let val = group_rows
                        .first()
                        .and_then(|r| r.get(*idx).cloned())
                        .unwrap_or(Datum::Null);
                    values.push(val);
                }
                BoundProjection::Expr(expr, _) => {
                    let empty = OwnedRow::new(vec![]);
                    let dummy = group_rows.first().copied().unwrap_or(&empty);
                    let val = ExprEngine::eval_row(expr, dummy).map_err(CedarError::Execution)?;
                    values.push(val);
                }
                BoundProjection::Window(..) => {
                    values.push(Datum::Null);
                }
            }
        }
        Ok(values)
    }

    pub(crate) fn compute_aggregate(
        &self,
        func: &AggFunc,
        agg_expr: Option<&BoundExpr>,
        distinct: bool,
        rows: &[&OwnedRow],
    ) -> Result<Datum, CedarError> {
        // Helper: evaluate expression for each row, collecting non-null values
        let eval_all = |expr: &BoundExpr| -> Result<Vec<Datum>, CedarError> {
            let mut vals = Vec::new();
            for row in rows {
                let v = ExprEngine::eval_row(expr, row).map_err(CedarError::Execution)?;
                if !v.is_null() {
                    vals.push(v);
                }
            }
            Ok(vals)
        };

        // Helper: collect distinct non-null values
        let distinct_vals = |expr: &BoundExpr| -> Result<Vec<Datum>, CedarError> {
            let mut seen = std::collections::HashSet::new();
            let mut vals = Vec::new();
            for row in rows {
                let v = ExprEngine::eval_row(expr, row).map_err(CedarError::Execution)?;
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

        match func {
            AggFunc::Count => {
                if let Some(expr) = agg_expr {
                    if distinct {
                        Ok(Datum::Int64(distinct_vals(expr)?.len() as i64))
                    } else {
                        Ok(Datum::Int64(eval_all(expr)?.len() as i64))
                    }
                } else {
                    Ok(Datum::Int64(rows.len() as i64))
                }
            }
            AggFunc::Sum => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "SUM requires an argument".into(),
                )))?;
                let vals = if distinct { distinct_vals(expr)? } else { eval_all(expr)? };
                let mut acc = Datum::Null;
                for val in vals {
                    acc = if acc.is_null() {
                        val
                    } else {
                        acc.add(&val).unwrap_or(acc)
                    };
                }
                Ok(acc)
            }
            AggFunc::Avg => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "AVG requires an argument".into(),
                )))?;
                let vals = if distinct { distinct_vals(expr)? } else { eval_all(expr)? };
                let mut sum = 0.0f64;
                let mut count = 0i64;
                for val in &vals {
                    if let Some(f) = val.as_f64() {
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
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "MIN requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                let mut min_val: Option<Datum> = None;
                for val in vals {
                    min_val = Some(match min_val {
                        None => val,
                        Some(cur) => {
                            if val < cur {
                                val
                            } else {
                                cur
                            }
                        }
                    });
                }
                Ok(min_val.unwrap_or(Datum::Null))
            }
            AggFunc::Max => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "MAX requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                let mut max_val: Option<Datum> = None;
                for val in vals {
                    max_val = Some(match max_val {
                        None => val,
                        Some(cur) => {
                            if val > cur {
                                val
                            } else {
                                cur
                            }
                        }
                    });
                }
                Ok(max_val.unwrap_or(Datum::Null))
            }
            AggFunc::BoolAnd => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "BOOL_AND requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                if vals.is_empty() {
                    return Ok(Datum::Null);
                }
                let mut result = true;
                for val in &vals {
                    if let Some(b) = val.as_bool() {
                        if !b {
                            result = false;
                        }
                    }
                }
                Ok(Datum::Boolean(result))
            }
            AggFunc::BoolOr => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "BOOL_OR requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                if vals.is_empty() {
                    return Ok(Datum::Null);
                }
                let mut result = false;
                for val in &vals {
                    if let Some(b) = val.as_bool() {
                        if b {
                            result = true;
                        }
                    }
                }
                Ok(Datum::Boolean(result))
            }
            AggFunc::StringAgg(sep) => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "STRING_AGG requires an argument".into(),
                )))?;
                let mut parts: Vec<String> = Vec::new();
                if distinct {
                    let vals = distinct_vals(expr)?;
                    for val in vals {
                        parts.push(format!("{}", val));
                    }
                } else {
                    let vals = eval_all(expr)?;
                    for val in vals {
                        parts.push(format!("{}", val));
                    }
                }
                if parts.is_empty() {
                    Ok(Datum::Null)
                } else {
                    Ok(Datum::Text(parts.join(sep)))
                }
            }
            AggFunc::ArrayAgg => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "ARRAY_AGG requires an argument".into(),
                )))?;
                let vals = if distinct { distinct_vals(expr)? } else { eval_all(expr)? };
                if vals.is_empty() {
                    Ok(Datum::Null)
                } else {
                    Ok(Datum::Array(vals))
                }
            }
            // ── Statistical aggregates (single-argument) ──
            AggFunc::VarPop | AggFunc::VarSamp | AggFunc::StddevPop | AggFunc::StddevSamp => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "Statistical aggregate requires an argument".into(),
                )))?;
                let vals = if distinct { distinct_vals(expr)? } else { eval_all(expr)? };
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
                        if n < 2 { return Ok(Datum::Null); }
                        sum_sq / (n - 1) as f64
                    }
                };
                match func {
                    AggFunc::StddevPop | AggFunc::StddevSamp => Ok(Datum::Float64(variance.sqrt())),
                    _ => Ok(Datum::Float64(variance)),
                }
            }
            // ── Two-argument statistical aggregates ──
            AggFunc::Corr | AggFunc::CovarPop | AggFunc::CovarSamp |
            AggFunc::RegrSlope | AggFunc::RegrIntercept | AggFunc::RegrR2 |
            AggFunc::RegrCount | AggFunc::RegrAvgX | AggFunc::RegrAvgY |
            AggFunc::RegrSXX | AggFunc::RegrSYY | AggFunc::RegrSXY => {
                // Two-argument aggregates: we use the first expression argument.
                // The second argument is expected as a second column in the row.
                // For simplicity, we evaluate the expression and treat pairs:
                // regr(y, x) expects 2 args but our AST only captures the first.
                // We'll use a convention: if the expression evaluates to a pair-like
                // value, split it. Otherwise, use column indices 0 and 1 from the row.
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "Two-argument aggregate requires an argument".into(),
                )))?;
                // Collect (y, x) pairs from consecutive column refs or the first two columns
                let mut pairs: Vec<(f64, f64)> = Vec::new();
                for row in rows {
                    let val = ExprEngine::eval_row(expr, row).map_err(CedarError::Execution)?;
                    if val.is_null() { continue; }
                    // Try to get y from expr, x from next column
                    let y = val.as_f64();
                    // For two-arg aggregates, the second arg is typically the next column
                    // We use columns 0=y, 1=x as a reasonable default
                    let x = row.values.get(1).and_then(|d| d.as_f64())
                        .or_else(|| row.values.first().and_then(|d| d.as_f64()));
                    if let (Some(yv), Some(xv)) = (y, x) {
                        pairs.push((yv, xv));
                    }
                }
                let n = pairs.len();
                if n == 0 { return Ok(Datum::Null); }

                match func {
                    AggFunc::RegrCount => Ok(Datum::Int64(n as i64)),
                    AggFunc::RegrAvgX => {
                        let avg_x = pairs.iter().map(|(_, x)| x).sum::<f64>() / n as f64;
                        Ok(Datum::Float64(avg_x))
                    }
                    AggFunc::RegrAvgY => {
                        let avg_y = pairs.iter().map(|(y, _)| y).sum::<f64>() / n as f64;
                        Ok(Datum::Float64(avg_y))
                    }
                    _ => {
                        let avg_x = pairs.iter().map(|(_, x)| x).sum::<f64>() / n as f64;
                        let avg_y = pairs.iter().map(|(y, _)| y).sum::<f64>() / n as f64;
                        let sxx: f64 = pairs.iter().map(|(_, x)| (x - avg_x).powi(2)).sum();
                        let syy: f64 = pairs.iter().map(|(y, _)| (y - avg_y).powi(2)).sum();
                        let sxy: f64 = pairs.iter().map(|(y, x)| (x - avg_x) * (y - avg_y)).sum();

                        match func {
                            AggFunc::RegrSXX => Ok(Datum::Float64(sxx)),
                            AggFunc::RegrSYY => Ok(Datum::Float64(syy)),
                            AggFunc::RegrSXY => Ok(Datum::Float64(sxy)),
                            AggFunc::CovarPop => Ok(Datum::Float64(sxy / n as f64)),
                            AggFunc::CovarSamp => {
                                if n < 2 { return Ok(Datum::Null); }
                                Ok(Datum::Float64(sxy / (n - 1) as f64))
                            }
                            AggFunc::Corr => {
                                let denom = (sxx * syy).sqrt();
                                if denom == 0.0 { Ok(Datum::Null) } else { Ok(Datum::Float64(sxy / denom)) }
                            }
                            AggFunc::RegrSlope => {
                                if sxx == 0.0 { Ok(Datum::Null) } else { Ok(Datum::Float64(sxy / sxx)) }
                            }
                            AggFunc::RegrIntercept => {
                                if sxx == 0.0 { Ok(Datum::Null) } else { Ok(Datum::Float64(avg_y - (sxy / sxx) * avg_x)) }
                            }
                            AggFunc::RegrR2 => {
                                if syy == 0.0 { Ok(Datum::Null) } else { Ok(Datum::Float64((sxy * sxy) / (sxx * syy))) }
                            }
                            _ => Ok(Datum::Null),
                        }
                    }
                }
            }
            // ── Ordered-set aggregates ──
            AggFunc::Mode => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "MODE requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                if vals.is_empty() { return Ok(Datum::Null); }
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
                Ok(freq.into_iter().next().map(|(_, _, v)| v).unwrap_or(Datum::Null))
            }
            AggFunc::PercentileCont(frac) => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "PERCENTILE_CONT requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                let mut floats: Vec<f64> = vals.iter().filter_map(|v| v.as_f64()).collect();
                if floats.is_empty() { return Ok(Datum::Null); }
                floats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = floats.len();
                let idx = frac * (n - 1) as f64;
                let lo = idx.floor() as usize;
                let hi = idx.ceil() as usize;
                let result = if lo == hi {
                    floats[lo]
                } else {
                    floats[lo] + (floats[hi] - floats[lo]) * (idx - lo as f64)
                };
                Ok(Datum::Float64(result))
            }
            AggFunc::PercentileDisc(frac) => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "PERCENTILE_DISC requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                let mut floats: Vec<f64> = vals.iter().filter_map(|v| v.as_f64()).collect();
                if floats.is_empty() { return Ok(Datum::Null); }
                floats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = (frac * floats.len() as f64).ceil() as usize;
                let idx = idx.min(floats.len()).saturating_sub(1);
                Ok(Datum::Float64(floats[idx]))
            }
            // ── Bit aggregates ──
            AggFunc::BitAndAgg => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "BIT_AND requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                if vals.is_empty() { return Ok(Datum::Null); }
                let mut result: Option<i64> = None;
                for v in &vals {
                    if let Some(i) = v.as_i64() {
                        result = Some(result.map_or(i, |r| r & i));
                    }
                }
                Ok(result.map(Datum::Int64).unwrap_or(Datum::Null))
            }
            AggFunc::BitOrAgg => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "BIT_OR requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                if vals.is_empty() { return Ok(Datum::Null); }
                let mut result: i64 = 0;
                for v in &vals {
                    if let Some(i) = v.as_i64() {
                        result |= i;
                    }
                }
                Ok(Datum::Int64(result))
            }
            AggFunc::BitXorAgg => {
                let expr = agg_expr.ok_or(CedarError::Execution(ExecutionError::TypeError(
                    "BIT_XOR requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                if vals.is_empty() { return Ok(Datum::Null); }
                let mut result: i64 = 0;
                for v in &vals {
                    if let Some(i) = v.as_i64() {
                        result ^= i;
                    }
                }
                Ok(Datum::Int64(result))
            }
        }
    }
}
