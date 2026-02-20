#![allow(clippy::too_many_arguments)]

use cedar_common::datum::{Datum, OwnedRow};
use cedar_common::error::CedarError;
use cedar_common::schema::TableSchema;
use cedar_common::types::TableId;
use cedar_sql_frontend::types::*;
use cedar_storage::memtable::{encode_column_value, encode_pk_from_datums};
use cedar_txn::TxnHandle;

use crate::expr_engine::ExprEngine;
use crate::executor::{Executor, CteData, ExecutionResult};
use crate::vectorized::{RecordBatch, vectorized_filter, is_vectorizable};
use crate::parallel::parallel_filter;

impl Executor {
    /// Try to detect a PK point lookup: `WHERE pk_col = literal` for single-column PKs.
    /// Returns (encoded_pk, remaining_filter) if successful.
    pub(crate) fn try_pk_point_lookup(
        &self,
        filter: Option<&BoundExpr>,
        schema: &TableSchema,
    ) -> Option<(Vec<u8>, Option<BoundExpr>)> {
        let f = filter?;
        let pk_cols = &schema.primary_key_columns;
        if pk_cols.len() != 1 {
            return None; // only single-column PKs for now
        }
        let pk_col = pk_cols[0];

        match f {
            BoundExpr::BinaryOp { left, op: BinOp::Eq, right } => {
                let (col_idx, datum) = match (left.as_ref(), right.as_ref()) {
                    (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d)) => (*idx, d),
                    (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => (*idx, d),
                    _ => return None,
                };
                if col_idx == pk_col {
                    let pk = encode_pk_from_datums(&[datum]);
                    Some((pk, None))
                } else {
                    None
                }
            }
            BoundExpr::BinaryOp { left, op: BinOp::And, right } => {
                // Try left side
                if let BoundExpr::BinaryOp { left: ll, op: BinOp::Eq, right: lr } = left.as_ref() {
                    let extracted = match (ll.as_ref(), lr.as_ref()) {
                        (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d)) => Some((*idx, d)),
                        (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => Some((*idx, d)),
                        _ => None,
                    };
                    if let Some((col_idx, datum)) = extracted {
                        if col_idx == pk_col {
                            let pk = encode_pk_from_datums(&[datum]);
                            return Some((pk, Some(*right.clone())));
                        }
                    }
                }
                // Try right side
                if let BoundExpr::BinaryOp { left: rl, op: BinOp::Eq, right: rr } = right.as_ref() {
                    let extracted = match (rl.as_ref(), rr.as_ref()) {
                        (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d)) => Some((*idx, d)),
                        (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => Some((*idx, d)),
                        _ => None,
                    };
                    if let Some((col_idx, datum)) = extracted {
                        if col_idx == pk_col {
                            let pk = encode_pk_from_datums(&[datum]);
                            return Some((pk, Some(*left.clone())));
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Try to extract a simple equality predicate `col = literal` from a filter
    /// that matches an indexed column. Returns (column_idx, encoded_key, remaining_filter).
    /// `remaining_filter` is the part of the filter that still needs post-filtering
    /// (None if the entire filter was consumed by the index lookup).
    pub(crate) fn try_index_scan_predicate(
        &self,
        filter: Option<&BoundExpr>,
        table_id: TableId,
    ) -> Option<(usize, Vec<u8>, Option<BoundExpr>)> {
        let f = filter?;
        let indexed_cols = self.storage.get_indexed_columns(table_id);
        if indexed_cols.is_empty() {
            return None;
        }

        // Pattern: BinaryOp { ColumnRef(idx), Eq, Literal(val) }
        // or BinaryOp { Literal(val), Eq, ColumnRef(idx) }
        match f {
            BoundExpr::BinaryOp { left, op: BinOp::Eq, right } => {
                let (col_idx, datum) = match (left.as_ref(), right.as_ref()) {
                    (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d)) => (*idx, d),
                    (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => (*idx, d),
                    _ => return None,
                };
                if indexed_cols.iter().any(|(c, _)| *c == col_idx) {
                    let key = encode_column_value(datum);
                    Some((col_idx, key, None)) // entire filter consumed
                } else {
                    None
                }
            }
            // Pattern: AND(col = literal, rest) — extract the indexed predicate
            BoundExpr::BinaryOp { left, op: BinOp::And, right } => {
                // Try left side
                if let BoundExpr::BinaryOp { left: ll, op: BinOp::Eq, right: lr } = left.as_ref() {
                    let extracted = match (ll.as_ref(), lr.as_ref()) {
                        (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d)) => Some((*idx, d)),
                        (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => Some((*idx, d)),
                        _ => None,
                    };
                    if let Some((col_idx, datum)) = extracted {
                        if indexed_cols.iter().any(|(c, _)| *c == col_idx) {
                            let key = encode_column_value(datum);
                            return Some((col_idx, key, Some(*right.clone())));
                        }
                    }
                }
                // Try right side
                if let BoundExpr::BinaryOp { left: rl, op: BinOp::Eq, right: rr } = right.as_ref() {
                    let extracted = match (rl.as_ref(), rr.as_ref()) {
                        (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d)) => Some((*idx, d)),
                        (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => Some((*idx, d)),
                        _ => None,
                    };
                    if let Some((col_idx, datum)) = extracted {
                        if indexed_cols.iter().any(|(c, _)| *c == col_idx) {
                            let key = encode_column_value(datum);
                            return Some((col_idx, key, Some(*left.clone())));
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Execute an index scan: look up rows via secondary index, then apply
    /// residual filter + project + group + sort + limit (same pipeline as seq scan).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn exec_index_scan(
        &self,
        table_id: TableId,
        schema: &TableSchema,
        index_col: usize,
        index_value: &BoundExpr,
        projections: &[BoundProjection],
        visible_projection_count: usize,
        filter: Option<&BoundExpr>,
        group_by: &[usize],
        grouping_sets: &[Vec<usize>],
        having: Option<&BoundExpr>,
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: &DistinctMode,
        txn: &TxnHandle,
        _cte_data: &CteData,
        _virtual_rows: &[OwnedRow],
    ) -> Result<ExecutionResult, CedarError> {
        // Encode the index lookup key from the literal value
        let dummy_row = OwnedRow::new(vec![]);
        let datum = crate::expr_engine::ExprEngine::eval_row(index_value, &dummy_row)
            .map_err(CedarError::Execution)?;
        let key = encode_column_value(&datum);

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let raw_rows = self.storage.index_scan(table_id, index_col, &key, txn.txn_id, read_ts)?;

        // From here, same pipeline as exec_seq_scan with the residual filter
        let mat_filter = self.materialize_filter(filter, txn)?;
        let mat_having = self.materialize_filter(having, txn)?;

        let filter_has_correlated_sub = mat_filter.as_ref()
            .is_some_and(Self::expr_has_outer_ref);

        let has_window = projections.iter().any(|p| matches!(p, BoundProjection::Window(..)));
        let has_agg = projections.iter().any(|p| matches!(p, BoundProjection::Aggregate(..)));

        if (has_agg || !group_by.is_empty() || !grouping_sets.is_empty()) && !has_window {
            return self.exec_aggregate(&raw_rows, schema, projections, mat_filter.as_ref(), group_by, grouping_sets, mat_having.as_ref(), order_by, limit, offset, distinct);
        }

        let mut filtered: Vec<&OwnedRow> = Vec::new();
        for (_pk, row) in &raw_rows {
            if let Some(ref f) = mat_filter {
                if filter_has_correlated_sub {
                    let row_filter = self.materialize_correlated(f, row, txn)?;
                    if !crate::expr_engine::ExprEngine::eval_filter(&row_filter, row).map_err(CedarError::Execution)? {
                        continue;
                    }
                } else if !crate::expr_engine::ExprEngine::eval_filter(f, row).map_err(CedarError::Execution)? {
                    continue;
                }
            }
            filtered.push(row);
        }

        let projs_have_correlated = projections.iter().any(|p| match p {
            BoundProjection::Expr(e, _) => Self::expr_has_outer_ref(e),
            _ => false,
        });

        let mut columns = self.resolve_output_columns(projections, schema);
        let mut result_rows: Vec<OwnedRow> = Vec::new();
        for row in &filtered {
            if projs_have_correlated {
                result_rows.push(self.project_row_correlated(row, projections, txn)?);
            } else {
                result_rows.push(self.project_row(row, projections)?);
            }
        }

        if has_window {
            self.compute_window_functions(&filtered, projections, &mut result_rows)?;
        }

        crate::external_sort::sort_rows(&mut result_rows, order_by, self.external_sorter.as_ref())?;
        self.apply_distinct(distinct, &mut result_rows);

        if let Some(off) = offset {
            if off < result_rows.len() {
                result_rows = result_rows.split_off(off);
            } else {
                result_rows.clear();
            }
        }
        if let Some(lim) = limit {
            result_rows.truncate(lim);
        }

        if visible_projection_count < projections.len() {
            for row in &mut result_rows {
                row.values.truncate(visible_projection_count);
            }
            columns.truncate(visible_projection_count);
        }

        Ok(ExecutionResult::Query {
            columns,
            rows: result_rows,
        })
    }

    pub(crate) fn exec_seq_scan(
        &self,
        table_id: cedar_common::types::TableId,
        schema: &TableSchema,
        projections: &[BoundProjection],
        visible_projection_count: usize,
        filter: Option<&BoundExpr>,
        group_by: &[usize],
        grouping_sets: &[Vec<usize>],
        having: Option<&BoundExpr>,
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: &DistinctMode,
        txn: &TxnHandle,
        cte_data: &CteData,
        virtual_rows: &[OwnedRow],
    ) -> Result<ExecutionResult, CedarError> {
        // Check if this is a dual (no-FROM) or CTE table, otherwise scan storage
        let (raw_rows, effective_filter): (Vec<(Vec<u8>, OwnedRow)>, Option<BoundExpr>) =
            if !virtual_rows.is_empty() {
                // VALUES clause or GENERATE_SERIES — use inline data directly
                (virtual_rows.iter().map(|r| (vec![], r.clone())).collect(), filter.cloned())
            } else if table_id == TableId(0) {
                // Virtual dual table — single empty row for SELECT without FROM
                (vec![(vec![], OwnedRow::new(vec![]))], filter.cloned())
            } else if let Some(cte_rows) = cte_data.get(&table_id) {
                (cte_rows.iter().map(|r| (vec![], r.clone())).collect(), filter.cloned())
            } else if let Some((pk, remaining)) = self.try_pk_point_lookup(filter, schema) {
                // PK point lookup: O(1) hash lookup instead of full scan
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                match self.storage.get(table_id, &pk, txn.txn_id, read_ts)? {
                    Some(row) => (vec![(pk, row)], remaining),
                    None => (vec![], remaining),
                }
            } else if let Some((col_idx, key, remaining)) = self.try_index_scan_predicate(filter, table_id) {
                // Index scan: use secondary index instead of full table scan
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                let rows = self.storage.index_scan(table_id, col_idx, &key, txn.txn_id, read_ts)?;
                (rows, remaining)
            } else {
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                (self.storage.scan(table_id, txn.txn_id, read_ts)?, filter.cloned())
            };

        // Materialize any subqueries in filter/having.
        // materialize_subqueries now leaves correlated subqueries (containing OuterColumnRef)
        // in place — they will be handled per-row below.
        let mat_filter = self.materialize_filter(effective_filter.as_ref(), txn)?;
        let mat_having = self.materialize_filter(having, txn)?;

        // Check if the materialized filter still contains correlated subqueries.
        // If so, we need per-row re-materialization during filtering.
        let filter_has_correlated_sub = mat_filter.as_ref()
            .is_some_and(Self::expr_has_outer_ref);

        // Check if this is a window function query
        let has_window = projections.iter().any(|p| matches!(p, BoundProjection::Window(..)));

        // Check if this is an aggregate query
        let has_agg = projections.iter().any(|p| matches!(p, BoundProjection::Aggregate(..)));

        if (has_agg || !group_by.is_empty() || !grouping_sets.is_empty()) && !has_window {
            return self.exec_aggregate(&raw_rows, schema, projections, mat_filter.as_ref(), group_by, grouping_sets, mat_having.as_ref(), order_by, limit, offset, distinct);
        }

        // Filter — choose execution strategy based on data size and query shape.
        let mut filtered: Vec<&OwnedRow> = Vec::new();
        if let Some(ref f) = mat_filter {
            if filter_has_correlated_sub {
                // Correlated subquery: must re-materialise per row (no parallel/vectorized)
                for (_pk, row) in &raw_rows {
                    let row_filter = self.materialize_correlated(f, row, txn)?;
                    if ExprEngine::eval_filter(&row_filter, row).map_err(CedarError::Execution)? {
                        filtered.push(row);
                    }
                }
            } else if self.parallel_config.should_parallelize(raw_rows.len())
                      && is_vectorizable(projections, mat_filter.as_ref())
            {
                // ── Parallel filter path ──
                let matched = parallel_filter(&raw_rows, f, &self.parallel_config);
                for idx in matched {
                    filtered.push(&raw_rows[idx].1);
                }
            } else if raw_rows.len() >= 256 && is_vectorizable(projections, mat_filter.as_ref()) {
                // ── Vectorized filter path (single-threaded, batched) ──
                let num_cols = schema.columns.len();
                let rows_only: Vec<OwnedRow> = raw_rows.iter().map(|(_, r)| r.clone()).collect();
                let mut batch = RecordBatch::from_rows(&rows_only, num_cols);
                vectorized_filter(&mut batch, f);
                for idx in batch.active_indices() {
                    filtered.push(&raw_rows[idx].1);
                }
            } else {
                // ── Row-at-a-time filter (small data) ──
                for (_pk, row) in &raw_rows {
                    if ExprEngine::eval_filter(f, row).map_err(CedarError::Execution)? {
                        filtered.push(row);
                    }
                }
            }
        } else {
            // No filter — all rows pass
            for (_pk, row) in &raw_rows {
                filtered.push(row);
            }
        }

        // Project — handle correlated subqueries in projections
        let projs_have_correlated = projections.iter().any(|p| match p {
            BoundProjection::Expr(e, _) => Self::expr_has_outer_ref(e),
            _ => false,
        });

        let mut columns = self.resolve_output_columns(projections, schema);
        let mut result_rows: Vec<OwnedRow> = Vec::new();
        for row in &filtered {
            if projs_have_correlated {
                let projected = self.project_row_correlated(row, projections, txn)?;
                result_rows.push(projected);
            } else {
                let projected = self.project_row(row, projections)?;
                result_rows.push(projected);
            }
        }

        // Compute window functions and inject values
        if has_window {
            self.compute_window_functions(&filtered, projections, &mut result_rows)?;
        }

        // Order by (must happen before DISTINCT ON so we keep the right row per group)
        crate::external_sort::sort_rows(&mut result_rows, order_by, self.external_sorter.as_ref())?;

        // Distinct (after ORDER BY so DISTINCT ON picks the first row per group)
        self.apply_distinct(distinct, &mut result_rows);

        // Offset + Limit
        if let Some(off) = offset {
            if off < result_rows.len() {
                result_rows = result_rows.split_off(off);
            } else {
                result_rows.clear();
            }
        }
        if let Some(lim) = limit {
            result_rows.truncate(lim);
        }

        // Strip hidden ORDER BY columns
        if visible_projection_count < projections.len() {
            for row in &mut result_rows {
                row.values.truncate(visible_projection_count);
            }
            columns.truncate(visible_projection_count);
        }

        Ok(ExecutionResult::Query {
            columns,
            rows: result_rows,
        })
    }

    pub(crate) fn merge_rows(&self, left: &OwnedRow, right: &OwnedRow) -> OwnedRow {
        let mut values = left.values.clone();
        values.extend(right.values.iter().cloned());
        OwnedRow::new(values)
    }

    /// Remove duplicate rows in-place using string-based comparison.
    pub(crate) fn dedup_rows(&self, rows: &mut Vec<OwnedRow>) {
        let mut seen: Vec<Vec<String>> = Vec::new();
        rows.retain(|row| {
            let key: Vec<String> = row.values.iter().map(|d| format!("{}", d)).collect();
            if seen.contains(&key) {
                false
            } else {
                seen.push(key);
                true
            }
        });
    }

    /// Apply DISTINCT / DISTINCT ON deduplication.
    pub(crate) fn apply_distinct(&self, mode: &DistinctMode, rows: &mut Vec<OwnedRow>) {
        match mode {
            DistinctMode::None => {}
            DistinctMode::All => self.dedup_rows(rows),
            DistinctMode::On(indices) => {
                let mut seen: Vec<Vec<String>> = Vec::new();
                rows.retain(|row| {
                    let key: Vec<String> = indices.iter().map(|&i| {
                        row.get(i).map(|d| format!("{}", d)).unwrap_or_default()
                    }).collect();
                    if seen.contains(&key) {
                        false
                    } else {
                        seen.push(key);
                        true
                    }
                });
            }
        }
    }

    /// Project a row with correlated subquery support.
    /// For expression projections containing OuterColumnRef, substitute with
    /// current row values and materialize subqueries per-row.
    pub(crate) fn project_row_correlated(
        &self,
        row: &OwnedRow,
        projections: &[BoundProjection],
        txn: &TxnHandle,
    ) -> Result<OwnedRow, CedarError> {
        let mut values = Vec::with_capacity(projections.len());
        for proj in projections {
            match proj {
                BoundProjection::Column(idx, _) => {
                    values.push(row.get(*idx).cloned().unwrap_or(Datum::Null));
                }
                BoundProjection::Expr(expr, _) => {
                    if Self::expr_has_outer_ref(expr) {
                        let mat = self.materialize_correlated(expr, row, txn)?;
                        let val = ExprEngine::eval_row(&mat, row).map_err(CedarError::Execution)?;
                        values.push(val);
                    } else {
                        let val = ExprEngine::eval_row(expr, row).map_err(CedarError::Execution)?;
                        values.push(val);
                    }
                }
                BoundProjection::Aggregate(..) => {
                    values.push(Datum::Null);
                }
                BoundProjection::Window(..) => {
                    values.push(Datum::Null);
                }
            }
        }
        Ok(OwnedRow::new(values))
    }

    pub(crate) fn project_row(
        &self,
        row: &OwnedRow,
        projections: &[BoundProjection],
    ) -> Result<OwnedRow, CedarError> {
        let mut values = Vec::with_capacity(projections.len());
        for proj in projections {
            match proj {
                BoundProjection::Column(idx, _) => {
                    values.push(row.get(*idx).cloned().unwrap_or(Datum::Null));
                }
                BoundProjection::Expr(expr, _) => {
                    let val = self.eval_expr_with_sequences(expr, row)?;
                    values.push(val);
                }
                BoundProjection::Aggregate(..) => {
                    // Should not reach here in non-aggregate path
                    values.push(Datum::Null);
                }
                BoundProjection::Window(..) => {
                    // Window values are injected separately; placeholder here
                    values.push(Datum::Null);
                }
            }
        }
        Ok(OwnedRow::new(values))
    }

    /// Evaluate an expression, handling sequence functions via storage.
    pub(crate) fn eval_expr_with_sequences(
        &self,
        expr: &BoundExpr,
        row: &OwnedRow,
    ) -> Result<Datum, CedarError> {
        match expr {
            BoundExpr::SequenceNextval(name) => {
                let val = self.storage.sequence_nextval(name)
                    .map_err(CedarError::Storage)?;
                Ok(Datum::Int64(val))
            }
            BoundExpr::SequenceCurrval(name) => {
                let val = self.storage.sequence_currval(name)
                    .map_err(CedarError::Storage)?;
                Ok(Datum::Int64(val))
            }
            BoundExpr::SequenceSetval(name, value) => {
                let val = self.storage.sequence_setval(name, *value)
                    .map_err(CedarError::Storage)?;
                Ok(Datum::Int64(val))
            }
            _ => ExprEngine::eval_row(expr, row).map_err(CedarError::Execution),
        }
    }
}

