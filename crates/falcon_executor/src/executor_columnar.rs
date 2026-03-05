//! Vectorized columnar aggregate execution for ColumnStore tables.
//!
//! When a query is a pure aggregate (COUNT/SUM/AVG/MIN/MAX) over a ColumnStore
//! table with no GROUP BY, no correlated subqueries, and no HAVING clause,
//! this path bypasses row-at-a-time OwnedRow deserialization entirely.
//!
//! Instead it uses `StorageEngine::scan_columnar()` which returns typed
//! `Vec<Datum>` per column directly from the columnar segment layout, then
//! runs `vectorized_aggregate` over those vectors.
//!
//! Speedup vs row-at-a-time: eliminates per-row allocation + type dispatch.

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_common::schema::TableSchema;
use falcon_common::types::DataType;
use falcon_sql_frontend::types::*;

use crate::executor::{ExecutionResult, Executor};
use crate::vectorized::{
    is_vectorizable, vectorized_aggregate, vectorized_filter, AggDescriptor, RecordBatch,
    vectorized_hash_agg,
};

impl Executor {
    /// Execute a pure aggregate query over pre-scanned columnar data.
    ///
    /// `col_vecs`: one `Vec<Datum>` per column, returned by `scan_columnar`.
    /// Applies an optional filter (vectorized), then computes each aggregate
    /// projection over the surviving rows.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn exec_columnar_aggregate(
        &self,
        col_vecs: Vec<Vec<Datum>>,
        schema: &TableSchema,
        projections: &[BoundProjection],
        filter: Option<&BoundExpr>,
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: &DistinctMode,
    ) -> Result<ExecutionResult, FalconError> {
        // Build a RecordBatch from the columnar vectors.
        let mut batch = RecordBatch::from_columns(col_vecs);

        // Apply vectorized filter if present.
        if let Some(f) = filter {
            if is_vectorizable(projections, Some(f)) {
                vectorized_filter(&mut batch, f);
            } else {
                // Fallback: materialise rows and filter row-at-a-time.
                let rows = batch.to_rows();
                let mut kept = Vec::new();
                for row in &rows {
                    if crate::expr_engine::ExprEngine::eval_filter(f, row)
                        .map_err(FalconError::Execution)?
                    {
                        kept.push(row.clone());
                    }
                }
                // Rebuild batch from filtered rows.
                let num_cols = schema.columns.len();
                batch = RecordBatch::from_rows(&kept, num_cols);
            }
        }

        let active = batch.active_indices();
        let num_active = active.len();

        // Compute each projection.
        let mut result_values: Vec<Datum> = Vec::with_capacity(projections.len());
        let mut columns: Vec<(String, DataType)> = Vec::with_capacity(projections.len());

        for proj in projections {
            match proj {
                BoundProjection::Aggregate(func, arg_expr, alias, _distinct, _filter) => {
                    let col_name = alias.clone();
                    let data_type = match func {
                        AggFunc::Count => DataType::Int64,
                        AggFunc::Sum
                        | AggFunc::Avg
                        | AggFunc::StddevPop
                        | AggFunc::StddevSamp
                        | AggFunc::VarPop
                        | AggFunc::VarSamp => DataType::Float64,
                        _ => DataType::Text,
                    };
                    columns.push((col_name, data_type));

                    let _ = _distinct;
                    let _ = _filter;
                    // COUNT(*) shortcut
                    if matches!(func, AggFunc::Count) && arg_expr.is_none() {
                        result_values.push(Datum::Int64(num_active as i64));
                        continue;
                    }

                    // Resolve the column index from the argument expression.
                    let col_idx = match arg_expr.as_ref() {
                        Some(BoundExpr::ColumnRef(idx)) => *idx,
                        Some(BoundExpr::Literal(d)) => {
                            // COUNT(literal) — treat as COUNT(*)
                            if matches!(func, AggFunc::Count) {
                                result_values.push(Datum::Int64(num_active as i64));
                                continue;
                            }
                            result_values.push(d.clone());
                            continue;
                        }
                        _ => {
                            // Complex expression: fall back to Null for now.
                            result_values.push(Datum::Null);
                            continue;
                        }
                    };

                    if col_idx < batch.columns.len() {
                        let val = vectorized_aggregate(&batch.columns[col_idx], &active, func)
                            .map_err(FalconError::Execution)?;
                        result_values.push(val);
                    } else {
                        result_values.push(Datum::Null);
                    }
                }

                BoundProjection::Column(col_idx, alias) => {
                    let col_name = if alias.is_empty() {
                        schema
                            .columns
                            .get(*col_idx).map_or_else(|| format!("col{col_idx}"), |c| c.name.clone())
                    } else {
                        alias.clone()
                    };
                    let data_type = schema
                        .columns
                        .get(*col_idx)
                        .map_or(DataType::Text, |c| c.data_type.clone());
                    columns.push((col_name, data_type));

                    // For non-aggregate projections in a pure-agg context, return first value.
                    if *col_idx < batch.columns.len() && !active.is_empty() {
                        result_values.push(batch.columns[*col_idx].get_datum(active[0]));
                    } else {
                        result_values.push(Datum::Null);
                    }
                }

                BoundProjection::Expr(expr, alias) => {
                    let col_name = if alias.is_empty() {
                        "?column?".to_owned()
                    } else {
                        alias.clone()
                    };
                    columns.push((col_name, DataType::Text));
                    let dummy = OwnedRow::new(vec![]);
                    let val = crate::expr_engine::ExprEngine::eval_row(expr, &dummy)
                        .map_err(FalconError::Execution)?;
                    result_values.push(val);
                }

                _ => {
                    columns.push(("?column?".into(), DataType::Text));
                    result_values.push(Datum::Null);
                }
            }
        }

        let mut rows = vec![OwnedRow::new(result_values)];

        // Apply ORDER BY / LIMIT / OFFSET (rare for pure aggregates but supported).
        crate::external_sort::sort_rows(&mut rows, order_by, self.external_sorter.as_ref())?;
        self.apply_distinct(distinct, &mut rows);

        if let Some(off) = offset {
            if off < rows.len() {
                rows.drain(..off);
            } else {
                rows.clear();
            }
        }
        if let Some(lim) = limit {
            rows.truncate(lim);
        }

        Ok(ExecutionResult::Query { columns, rows })
    }

    /// Execute a GROUP BY aggregate query over pre-scanned columnar data.
    ///
    /// Uses `vectorized_hash_agg` to hash-group across column vectors without
    /// materialising intermediate `OwnedRow` objects.
    ///
    /// Eligibility (checked by caller): no HAVING, no correlated filter,
    /// no GROUPING SETS, no window functions, simple AGG projections only.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn exec_columnar_group_agg(
        &self,
        col_vecs: Vec<Vec<Datum>>,
        schema: &TableSchema,
        projections: &[BoundProjection],
        filter: Option<&BoundExpr>,
        group_by: &[usize],
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: &DistinctMode,
    ) -> Result<ExecutionResult, FalconError> {
        let mut batch = RecordBatch::from_columns(col_vecs);

        // Apply vectorized filter if present.
        if let Some(f) = filter {
            if is_vectorizable(projections, Some(f)) {
                vectorized_filter(&mut batch, f);
            } else {
                let rows = batch.to_rows();
                let mut kept = Vec::new();
                for row in &rows {
                    if crate::expr_engine::ExprEngine::eval_filter(f, row)
                        .map_err(FalconError::Execution)?
                    {
                        kept.push(row.clone());
                    }
                }
                let num_cols = schema.columns.len();
                batch = RecordBatch::from_rows(&kept, num_cols);
            }
        }

        // Build AggDescriptor list from projections (only Aggregate projections).
        // Non-aggregate projections (Column refs) are handled as group-key pass-through.
        let mut agg_descs: Vec<AggDescriptor> = Vec::new();
        for proj in projections {
            if let BoundProjection::Aggregate(func, arg_expr, _, _, _) = proj {
                let col_idx = match arg_expr {
                    Some(BoundExpr::ColumnRef(idx)) => Some(*idx),
                    _ => None, // COUNT(*) or complex expr → treat as COUNT(*)
                };
                agg_descs.push(AggDescriptor { func: func.clone(), col_idx });
            }
        }

        let hash_result = vectorized_hash_agg(&batch, group_by, &agg_descs);

        // `vectorized_hash_agg` returns rows as [key_cols..., agg_cols...].
        // Remap to match projection order.
        let columns = self.resolve_output_columns(projections, schema);
        let num_key_cols = group_by.len();
        let mut result_rows: Vec<OwnedRow> = Vec::with_capacity(hash_result.rows.len());

        for hash_row in hash_result.rows {
            let mut values = Vec::with_capacity(projections.len());
            let mut agg_offset = 0usize;
            for proj in projections {
                match proj {
                    BoundProjection::Column(col_idx, _) => {
                        // Find this col_idx in the group_by list to get position in key cols.
                        let key_pos = group_by.iter().position(|&k| k == *col_idx);
                        let val = key_pos
                            .and_then(|p| hash_row.values.get(p))
                            .cloned()
                            .unwrap_or(Datum::Null);
                        values.push(val);
                    }
                    BoundProjection::Aggregate(..) => {
                        let val = hash_row.values
                            .get(num_key_cols + agg_offset)
                            .cloned()
                            .unwrap_or(Datum::Null);
                        values.push(val);
                        agg_offset += 1;
                    }
                    BoundProjection::Expr(expr, _) => {
                        let dummy = OwnedRow::new(vec![]);
                        let val = crate::expr_engine::ExprEngine::eval_row(expr, &dummy)
                            .map_err(FalconError::Execution)?;
                        values.push(val);
                    }
                    _ => {
                        values.push(Datum::Null);
                    }
                }
            }
            result_rows.push(OwnedRow::new(values));
        }

        crate::external_sort::sort_rows(&mut result_rows, order_by, self.external_sorter.as_ref())?;
        self.apply_distinct(distinct, &mut result_rows);

        if let Some(off) = offset {
            if off < result_rows.len() {
                result_rows.drain(..off);
            } else {
                result_rows.clear();
            }
        }
        if let Some(lim) = limit {
            result_rows.truncate(lim);
        }

        Ok(ExecutionResult::Query { columns, rows: result_rows })
    }
}

#[cfg(test)]
mod columnar_group_agg_tests {
    use falcon_common::datum::Datum;

    use crate::vectorized::{AggDescriptor, RecordBatch, vectorized_hash_agg};
    use falcon_sql_frontend::types::AggFunc;

    fn make_batch(col_a: Vec<i64>, col_b: Vec<i64>) -> RecordBatch {
        let datums_a: Vec<Datum> = col_a.into_iter().map(Datum::Int64).collect();
        let datums_b: Vec<Datum> = col_b.into_iter().map(Datum::Int64).collect();
        RecordBatch::from_columns(vec![datums_a, datums_b])
    }

    #[test]
    fn test_group_by_sum() {
        // col0 = category (group key), col1 = value (to SUM)
        // rows: (1,10),(1,20),(2,5),(2,15),(3,100)
        let batch = make_batch(vec![1, 1, 2, 2, 3], vec![10, 20, 5, 15, 100]);
        let aggs = vec![AggDescriptor { func: AggFunc::Sum, col_idx: Some(1) }];
        let result = vectorized_hash_agg(&batch, &[0], &aggs);

        // 3 groups
        assert_eq!(result.rows.len(), 3);

        let mut sums: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
        for row in &result.rows {
            if let (Datum::Int64(k), Datum::Int64(v)) = (&row.values[0], &row.values[1]) {
                sums.insert(*k, *v);
            }
        }
        assert_eq!(sums[&1], 30);
        assert_eq!(sums[&2], 20);
        assert_eq!(sums[&3], 100);
    }

    #[test]
    fn test_group_by_count() {
        let batch = make_batch(vec![1, 1, 2, 2, 2], vec![0, 0, 0, 0, 0]);
        let aggs = vec![AggDescriptor { func: AggFunc::Count, col_idx: None }];
        let result = vectorized_hash_agg(&batch, &[0], &aggs);

        assert_eq!(result.rows.len(), 2);
        let mut counts: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
        for row in &result.rows {
            if let (Datum::Int64(k), Datum::Int64(c)) = (&row.values[0], &row.values[1]) {
                counts.insert(*k, *c);
            }
        }
        assert_eq!(counts[&1], 2);
        assert_eq!(counts[&2], 3);
    }

    #[test]
    fn test_group_by_min_max() {
        // col0 = group (A=1, B=2), col1 = values
        let batch = make_batch(vec![1, 1, 1, 2, 2], vec![3, 1, 5, 8, 2]);
        let aggs = vec![
            AggDescriptor { func: AggFunc::Min, col_idx: Some(1) },
            AggDescriptor { func: AggFunc::Max, col_idx: Some(1) },
        ];
        let result = vectorized_hash_agg(&batch, &[0], &aggs);
        assert_eq!(result.rows.len(), 2);

        let mut minmax: std::collections::HashMap<i64, (i64, i64)> = std::collections::HashMap::new();
        for row in &result.rows {
            if let (Datum::Int64(k), Datum::Int64(mn), Datum::Int64(mx)) =
                (&row.values[0], &row.values[1], &row.values[2])
            {
                minmax.insert(*k, (*mn, *mx));
            }
        }
        assert_eq!(minmax[&1], (1, 5));
        assert_eq!(minmax[&2], (2, 8));
    }

    #[test]
    fn test_single_group_all_rows() {
        // All rows belong to the same group key
        let batch = make_batch(vec![7, 7, 7], vec![10, 20, 30]);
        let aggs = vec![AggDescriptor { func: AggFunc::Sum, col_idx: Some(1) }];
        let result = vectorized_hash_agg(&batch, &[0], &aggs);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[1], Datum::Int64(60));
    }

    #[test]
    fn test_empty_batch() {
        let batch = RecordBatch::from_columns(vec![vec![], vec![]]);
        let aggs = vec![AggDescriptor { func: AggFunc::Count, col_idx: None }];
        let result = vectorized_hash_agg(&batch, &[0], &aggs);
        assert_eq!(result.rows.len(), 0);
    }
}
