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
use crate::vectorized::{is_vectorizable, vectorized_aggregate, vectorized_filter, RecordBatch};

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
                            .get(*col_idx)
                            .map(|c| c.name.clone())
                            .unwrap_or_else(|| format!("col{}", col_idx))
                    } else {
                        alias.clone()
                    };
                    let data_type = schema
                        .columns
                        .get(*col_idx)
                        .map(|c| c.data_type.clone())
                        .unwrap_or(DataType::Text);
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
                        "?column?".to_string()
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
                rows = rows.split_off(off);
            } else {
                rows.clear();
            }
        }
        if let Some(lim) = limit {
            rows.truncate(lim);
        }

        Ok(ExecutionResult::Query { columns, rows })
    }
}
