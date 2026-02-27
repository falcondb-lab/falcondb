#![allow(clippy::too_many_arguments)]

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_common::schema::TableSchema;
use falcon_common::types::TableId;
use falcon_sql_frontend::types::*;
use falcon_storage::memtable::{encode_column_value, encode_pk_from_datums, SimpleAggOp};
use falcon_txn::TxnHandle;

use crate::executor::{CteData, ExecutionResult, Executor};
use crate::expr_engine::ExprEngine;
use crate::parallel::parallel_filter;
use crate::vectorized::{is_vectorizable, vectorized_filter, RecordBatch};

/// Range bound extracted from a comparison predicate on an indexed column.
enum RangeBound {
    Gt(Datum),
    Gte(Datum),
    Lt(Datum),
    Lte(Datum),
    Between(Datum, Datum),
}

impl Executor {
    /// Try to detect a PK point lookup from the filter expression.
    /// Supports both single-column and composite (multi-column) PKs.
    ///
    /// Flattens AND conjuncts and extracts `pk_col = literal` for each PK column.
    /// If all PK columns are matched, returns the encoded composite key and any
    /// remaining non-PK conjuncts as a residual filter.
    pub(crate) fn try_pk_point_lookup(
        &self,
        filter: Option<&BoundExpr>,
        schema: &TableSchema,
    ) -> Option<(Vec<u8>, Option<BoundExpr>)> {
        let f = filter?;
        let pk_cols = &schema.primary_key_columns;
        if pk_cols.is_empty() {
            return None;
        }

        // Flatten AND tree into a list of conjuncts.
        let mut conjuncts: Vec<&BoundExpr> = Vec::new();
        Self::flatten_and(f, &mut conjuncts);

        // For each PK column, find a matching `col = literal` conjunct.
        // pk_datums[i] = datum for pk_cols[i], matched_indices = which conjuncts were consumed.
        let mut pk_datums: Vec<Option<&Datum>> = vec![None; pk_cols.len()];
        let mut matched_indices: Vec<bool> = vec![false; conjuncts.len()];

        for (ci, conj) in conjuncts.iter().enumerate() {
            if let Some((col_idx, datum)) = Self::try_extract_eq_literal(conj) {
                // Check if this column is one of the PK columns
                if let Some(pk_pos) = pk_cols.iter().position(|&c| c == col_idx) {
                    if pk_datums[pk_pos].is_none() {
                        pk_datums[pk_pos] = Some(datum);
                        matched_indices[ci] = true;
                    }
                }
            }
        }

        // All PK columns must be matched
        if pk_datums.iter().any(|d| d.is_none()) {
            return None;
        }

        // Encode composite PK in column order
        let datum_refs: Vec<&Datum> = pk_datums.into_iter().map(|d| d.unwrap()).collect();
        let pk = encode_pk_from_datums(&datum_refs);

        // Build residual filter from unmatched conjuncts
        let remaining: Vec<BoundExpr> = conjuncts
            .iter()
            .zip(matched_indices.iter())
            .filter(|(_, &matched)| !matched)
            .map(|(&conj, _)| conj.clone())
            .collect();

        let residual = Self::conjoin(remaining);
        Some((pk, residual))
    }

    /// Flatten a tree of AND expressions into a flat list of conjuncts.
    fn flatten_and<'a>(expr: &'a BoundExpr, out: &mut Vec<&'a BoundExpr>) {
        match expr {
            BoundExpr::BinaryOp { left, op: BinOp::And, right } => {
                Self::flatten_and(left, out);
                Self::flatten_and(right, out);
            }
            other => out.push(other),
        }
    }

    /// Try to extract `col_idx = literal` from a simple equality expression.
    fn try_extract_eq_literal(expr: &BoundExpr) -> Option<(usize, &Datum)> {
        if let BoundExpr::BinaryOp { left, op: BinOp::Eq, right } = expr {
            match (left.as_ref(), right.as_ref()) {
                (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d))
                | (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => Some((*idx, d)),
                _ => None,
            }
        } else {
            None
        }
    }

    /// Re-join a list of conjuncts into a single AND expression.
    /// Returns None for empty lists.
    fn conjoin(mut exprs: Vec<BoundExpr>) -> Option<BoundExpr> {
        match exprs.len() {
            0 => None,
            1 => Some(exprs.pop().unwrap()),
            _ => {
                let mut acc = exprs.pop().unwrap();
                while let Some(e) = exprs.pop() {
                    acc = BoundExpr::BinaryOp {
                        left: Box::new(e),
                        op: BinOp::And,
                        right: Box::new(acc),
                    };
                }
                Some(acc)
            }
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
            BoundExpr::BinaryOp {
                left,
                op: BinOp::Eq,
                right,
            } => {
                let (col_idx, datum) = match (left.as_ref(), right.as_ref()) {
                    (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d))
                    | (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => (*idx, d),
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
            BoundExpr::BinaryOp {
                left,
                op: BinOp::And,
                right,
            } => {
                // Try left side
                if let BoundExpr::BinaryOp {
                    left: ll,
                    op: BinOp::Eq,
                    right: lr,
                } = left.as_ref()
                {
                    let extracted = match (ll.as_ref(), lr.as_ref()) {
                        (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d))
                        | (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => Some((*idx, d)),
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
                if let BoundExpr::BinaryOp {
                    left: rl,
                    op: BinOp::Eq,
                    right: rr,
                } = right.as_ref()
                {
                    let extracted = match (rl.as_ref(), rr.as_ref()) {
                        (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d))
                        | (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => Some((*idx, d)),
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

    /// Try to extract range predicates on an indexed column from the filter.
    ///
    /// Detects patterns:
    /// - `col > literal`, `col >= literal`, `col < literal`, `col <= literal`
    /// - `col BETWEEN low AND high`
    /// - AND combinations that form a range on the same indexed column
    ///
    /// Returns `(column_idx, lower_bound, upper_bound, remaining_filter)`.
    /// Each bound is `Option<(encoded_key, inclusive)>`.
    pub(crate) fn try_index_range_scan_predicate(
        &self,
        filter: Option<&BoundExpr>,
        table_id: TableId,
    ) -> Option<(
        usize,
        Option<(Vec<u8>, bool)>,
        Option<(Vec<u8>, bool)>,
        Option<BoundExpr>,
    )> {
        let f = filter?;
        let indexed_cols = self.storage.get_indexed_columns(table_id);
        if indexed_cols.is_empty() {
            return None;
        }

        // Flatten AND tree into conjuncts
        let mut conjuncts: Vec<&BoundExpr> = Vec::new();
        Self::flatten_and(f, &mut conjuncts);

        // For each indexed column, try to collect range bounds from conjuncts
        for &(col_idx, _unique) in &indexed_cols {
            let mut lower: Option<(Vec<u8>, bool)> = None; // (key, inclusive)
            let mut upper: Option<(Vec<u8>, bool)> = None;
            let mut matched: Vec<bool> = vec![false; conjuncts.len()];

            for (ci, conj) in conjuncts.iter().enumerate() {
                if let Some((cidx, bound)) = Self::try_extract_range_bound(conj) {
                    if cidx == col_idx {
                        match bound {
                            RangeBound::Gt(d) => {
                                let key = encode_column_value(&d);
                                lower = Some((key, false));
                                matched[ci] = true;
                            }
                            RangeBound::Gte(d) => {
                                let key = encode_column_value(&d);
                                lower = Some((key, true));
                                matched[ci] = true;
                            }
                            RangeBound::Lt(d) => {
                                let key = encode_column_value(&d);
                                upper = Some((key, false));
                                matched[ci] = true;
                            }
                            RangeBound::Lte(d) => {
                                let key = encode_column_value(&d);
                                upper = Some((key, true));
                                matched[ci] = true;
                            }
                            RangeBound::Between(lo, hi) => {
                                lower = Some((encode_column_value(&lo), true));
                                upper = Some((encode_column_value(&hi), true));
                                matched[ci] = true;
                            }
                        }
                    }
                }
            }

            if lower.is_some() || upper.is_some() {
                let remaining: Vec<BoundExpr> = conjuncts
                    .iter()
                    .zip(matched.iter())
                    .filter(|(_, &m)| !m)
                    .map(|(&conj, _)| conj.clone())
                    .collect();
                let residual = Self::conjoin(remaining);
                return Some((col_idx, lower, upper, residual));
            }
        }
        None
    }

    /// Extract a range bound from a comparison expression on a column.
    fn try_extract_range_bound(expr: &BoundExpr) -> Option<(usize, RangeBound)> {
        match expr {
            BoundExpr::BinaryOp { left, op, right } => {
                let (col_idx, datum, flipped) = match (left.as_ref(), right.as_ref()) {
                    (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d)) => (*idx, d.clone(), false),
                    (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => (*idx, d.clone(), true),
                    _ => return None,
                };
                // When the literal is on the left (flipped), reverse the operator direction
                let effective_op = if flipped {
                    match op {
                        BinOp::Gt => BinOp::Lt,
                        BinOp::GtEq => BinOp::LtEq,
                        BinOp::Lt => BinOp::Gt,
                        BinOp::LtEq => BinOp::GtEq,
                        other => *other,
                    }
                } else {
                    *op
                };
                match effective_op {
                    BinOp::Gt => Some((col_idx, RangeBound::Gt(datum))),
                    BinOp::GtEq => Some((col_idx, RangeBound::Gte(datum))),
                    BinOp::Lt => Some((col_idx, RangeBound::Lt(datum))),
                    BinOp::LtEq => Some((col_idx, RangeBound::Lte(datum))),
                    _ => None,
                }
            }
            BoundExpr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => {
                if *negated {
                    return None;
                }
                if let BoundExpr::ColumnRef(idx) = inner.as_ref() {
                    if let (BoundExpr::Literal(lo), BoundExpr::Literal(hi)) =
                        (low.as_ref(), high.as_ref())
                    {
                        return Some((*idx, RangeBound::Between(lo.clone(), hi.clone())));
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Try streaming aggregates: detect simple COUNT/SUM/AVG/MIN/MAX on column refs
    /// and compute them in one pass without materializing any rows.
    fn try_streaming_aggs(
        &self,
        projections: &[BoundProjection],
        schema: &TableSchema,
        table_id: TableId,
        txn: &TxnHandle,
    ) -> Option<ExecutionResult> {
        // Build aggregate specs. For AVG, decompose into SUM + COUNT.
        // Track: (SimpleAggOp, Option<col_idx>) and how to map back to projections.
        struct AggMapping {
            sum_idx: Option<usize>,   // index in specs for SUM (used by AVG)
            count_idx: Option<usize>, // index in specs for COUNT (used by AVG)
            direct_idx: Option<usize>, // index in specs for direct agg
            is_avg: bool,
        }

        let mut specs: Vec<(SimpleAggOp, Option<usize>)> = Vec::new();
        let mut mappings: Vec<AggMapping> = Vec::new();

        for proj in projections {
            match proj {
                BoundProjection::Aggregate(func, arg, _, distinct, filter) => {
                    if *distinct || filter.is_some() {
                        return None; // can't handle DISTINCT or filtered aggs
                    }
                    let col_idx = match arg {
                        None => None, // COUNT(*)
                        Some(BoundExpr::ColumnRef(idx)) => Some(*idx),
                        _ => return None, // complex expression — bail
                    };
                    match func {
                        AggFunc::Count => {
                            let idx = specs.len();
                            specs.push((
                                if col_idx.is_none() { SimpleAggOp::CountStar } else { SimpleAggOp::Count },
                                col_idx,
                            ));
                            mappings.push(AggMapping {
                                sum_idx: None, count_idx: None, direct_idx: Some(idx), is_avg: false,
                            });
                        }
                        AggFunc::Sum => {
                            let idx = specs.len();
                            specs.push((SimpleAggOp::Sum, col_idx));
                            mappings.push(AggMapping {
                                sum_idx: None, count_idx: None, direct_idx: Some(idx), is_avg: false,
                            });
                        }
                        AggFunc::Avg => {
                            let si = specs.len();
                            specs.push((SimpleAggOp::Sum, col_idx));
                            let ci = specs.len();
                            specs.push((SimpleAggOp::Count, col_idx));
                            mappings.push(AggMapping {
                                sum_idx: Some(si), count_idx: Some(ci), direct_idx: None, is_avg: true,
                            });
                        }
                        AggFunc::Min => {
                            let idx = specs.len();
                            specs.push((SimpleAggOp::Min, col_idx));
                            mappings.push(AggMapping {
                                sum_idx: None, count_idx: None, direct_idx: Some(idx), is_avg: false,
                            });
                        }
                        AggFunc::Max => {
                            let idx = specs.len();
                            specs.push((SimpleAggOp::Max, col_idx));
                            mappings.push(AggMapping {
                                sum_idx: None, count_idx: None, direct_idx: Some(idx), is_avg: false,
                            });
                        }
                        _ => return None, // unsupported aggregate
                    }
                }
                _ => return None, // non-aggregate projection
            }
        }

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let raw_results = self
            .storage
            .compute_simple_aggs(table_id, txn.txn_id, read_ts, &specs)
            .ok()?;

        // Map raw results back to projection order
        let mut values = Vec::with_capacity(projections.len());
        for m in &mappings {
            if m.is_avg {
                let sum = &raw_results[m.sum_idx.unwrap()];
                let count = &raw_results[m.count_idx.unwrap()];
                match (sum.as_f64(), count.as_f64()) {
                    (Some(s), Some(c)) if c > 0.0 => values.push(Datum::Float64(s / c)),
                    _ => values.push(Datum::Null),
                }
            } else {
                values.push(raw_results[m.direct_idx.unwrap()].clone());
            }
        }

        let columns = self.resolve_output_columns(projections, schema);
        Some(ExecutionResult::Query {
            columns,
            rows: vec![OwnedRow::new(values)],
        })
    }

    /// Detect ORDER BY <pk_col> [ASC|DESC] LIMIT K pattern for scan_top_k_by_pk.
    /// Returns Some((k, ascending)) if all conditions are met.
    #[allow(clippy::too_many_arguments)]
    fn try_pk_ordered_limit(
        &self,
        schema: &TableSchema,
        filter: Option<&BoundExpr>,
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
        group_by: &[usize],
        distinct: &DistinctMode,
        has_agg: &bool,
        has_window: &bool,
        virtual_rows: &[OwnedRow],
        table_id: TableId,
        cte_data: &CteData,
    ) -> Option<(usize, bool)> {
        // Must have: LIMIT, single ORDER BY column, no filter/agg/group/offset/distinct/window
        let k = limit?;
        if k == 0
            || offset.is_some()
            || filter.is_some()
            || *has_agg
            || *has_window
            || !group_by.is_empty()
            || !matches!(distinct, DistinctMode::None)
            || !virtual_rows.is_empty()
            || table_id == TableId(0)
            || cte_data.contains_key(&table_id)
            || order_by.len() != 1
        {
            return None;
        }
        // Must be single-column PK
        let pk_cols = &schema.primary_key_columns;
        if pk_cols.len() != 1 {
            return None;
        }
        let pk_col_idx = pk_cols[0];
        let ob = &order_by[0];
        // Check if the ORDER BY projection maps to the PK column
        // (ob.column_idx is index into projection list)
        // We accept both Column(col_idx, _) and Expr(ColumnRef(col_idx), _)
        // Since this is checked before raw_rows is computed, we don't have
        // the projections here. Use a simple heuristic: for SELECT *, the
        // order_by column_idx directly maps to the table column index.
        if ob.column_idx == pk_col_idx {
            Some((k, ob.asc))
        } else {
            None
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
    ) -> Result<ExecutionResult, FalconError> {
        // Encode the index lookup key from the literal value
        let dummy_row = OwnedRow::new(vec![]);
        let datum = crate::expr_engine::ExprEngine::eval_row(index_value, &dummy_row)
            .map_err(FalconError::Execution)?;
        let key = encode_column_value(&datum);

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let raw_rows = self
            .storage
            .index_scan(table_id, index_col, &key, txn.txn_id, read_ts)?;

        // From here, same pipeline as exec_seq_scan with the residual filter
        let mat_filter = self.materialize_filter(filter, txn)?;
        let mat_having = self.materialize_filter(having, txn)?;

        let filter_has_correlated_sub = mat_filter.as_ref().is_some_and(Self::expr_has_outer_ref);

        let has_window = projections
            .iter()
            .any(|p| matches!(p, BoundProjection::Window(..)));
        let has_agg = projections
            .iter()
            .any(|p| matches!(p, BoundProjection::Aggregate(..)));

        if (has_agg || !group_by.is_empty() || !grouping_sets.is_empty()) && !has_window {
            return self.exec_aggregate(
                &raw_rows,
                schema,
                projections,
                mat_filter.as_ref(),
                group_by,
                grouping_sets,
                mat_having.as_ref(),
                order_by,
                limit,
                offset,
                distinct,
            );
        }

        let projs_have_correlated = projections.iter().any(|p| match p {
            BoundProjection::Expr(e, _) => Self::expr_has_outer_ref(e),
            _ => false,
        });

        // Early-limit capacity hint: if no ORDER BY/DISTINCT/window, we know the max output size.
        let capacity_hint = match (order_by.is_empty() && matches!(distinct, DistinctMode::None) && !has_window, limit, offset) {
            (true, Some(lim), Some(off)) => lim + off,
            (true, Some(lim), None) => lim,
            _ => raw_rows.len(),
        };

        let mut columns = self.resolve_output_columns(projections, schema);
        // For window functions we still need the filtered row references.
        let mut filtered: Vec<&OwnedRow> = Vec::new();
        let mut result_rows: Vec<OwnedRow> = Vec::with_capacity(capacity_hint.min(raw_rows.len()));

        // Effective row limit for early cutoff (only when no sort/distinct/window).
        let early_limit = if order_by.is_empty() && matches!(distinct, DistinctMode::None) && !has_window {
            limit.map(|l| l + offset.unwrap_or(0))
        } else {
            None
        };

        for (_pk, row) in &raw_rows {
            // Early cutoff: stop scanning once we have enough rows.
            if let Some(max) = early_limit {
                if result_rows.len() >= max {
                    break;
                }
            }

            if let Some(ref f) = mat_filter {
                if filter_has_correlated_sub {
                    let row_filter = self.materialize_correlated(f, row, txn)?;
                    if !crate::expr_engine::ExprEngine::eval_filter(&row_filter, row)
                        .map_err(FalconError::Execution)?
                    {
                        continue;
                    }
                } else if !crate::expr_engine::ExprEngine::eval_filter(f, row)
                    .map_err(FalconError::Execution)?
                {
                    continue;
                }
            }

            if has_window {
                filtered.push(row);
            }

            // Project immediately (single-pass filter+project).
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
                result_rows.drain(..off);
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

    /// Execute an index range scan: evaluate range bounds, look up PKs via
    /// `StorageEngine::index_range_scan`, then apply the standard
    /// filter → project → sort → limit pipeline.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn exec_index_range_scan(
        &self,
        table_id: TableId,
        schema: &TableSchema,
        index_col: usize,
        lower_bound: Option<&(BoundExpr, bool)>,
        upper_bound: Option<&(BoundExpr, bool)>,
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
    ) -> Result<ExecutionResult, FalconError> {
        let dummy_row = OwnedRow::new(vec![]);

        let lo = if let Some((expr, inclusive)) = lower_bound {
            let d = crate::expr_engine::ExprEngine::eval_row(expr, &dummy_row)
                .map_err(FalconError::Execution)?;
            Some((encode_column_value(&d), *inclusive))
        } else {
            None
        };
        let hi = if let Some((expr, inclusive)) = upper_bound {
            let d = crate::expr_engine::ExprEngine::eval_row(expr, &dummy_row)
                .map_err(FalconError::Execution)?;
            Some((encode_column_value(&d), *inclusive))
        } else {
            None
        };

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let lo_ref = lo.as_ref().map(|(k, inc)| (k.as_slice(), *inc));
        let hi_ref = hi.as_ref().map(|(k, inc)| (k.as_slice(), *inc));
        let raw_rows = self.storage.index_range_scan(
            table_id, index_col, lo_ref, hi_ref, txn.txn_id, read_ts,
        )?;

        // Same pipeline as exec_index_scan
        let mat_filter = self.materialize_filter(filter, txn)?;
        let mat_having = self.materialize_filter(having, txn)?;
        let filter_has_correlated_sub = mat_filter.as_ref().is_some_and(Self::expr_has_outer_ref);

        let has_window = projections.iter().any(|p| matches!(p, BoundProjection::Window(..)));
        let has_agg = projections.iter().any(|p| matches!(p, BoundProjection::Aggregate(..)));

        if (has_agg || !group_by.is_empty() || !grouping_sets.is_empty()) && !has_window {
            return self.exec_aggregate(
                &raw_rows, schema, projections, mat_filter.as_ref(),
                group_by, grouping_sets, mat_having.as_ref(),
                order_by, limit, offset, distinct,
            );
        }

        let projs_have_correlated = projections.iter().any(|p| match p {
            BoundProjection::Expr(e, _) => Self::expr_has_outer_ref(e),
            _ => false,
        });

        let capacity_hint = match (
            order_by.is_empty() && matches!(distinct, DistinctMode::None) && !has_window,
            limit, offset,
        ) {
            (true, Some(lim), Some(off)) => lim + off,
            (true, Some(lim), None) => lim,
            _ => raw_rows.len(),
        };

        let mut columns = self.resolve_output_columns(projections, schema);
        let mut filtered: Vec<&OwnedRow> = Vec::new();
        let mut result_rows: Vec<OwnedRow> = Vec::with_capacity(capacity_hint.min(raw_rows.len()));

        let early_limit = if order_by.is_empty() && matches!(distinct, DistinctMode::None) && !has_window {
            limit.map(|l| l + offset.unwrap_or(0))
        } else {
            None
        };

        for (_pk, row) in &raw_rows {
            if let Some(max) = early_limit {
                if result_rows.len() >= max { break; }
            }
            if let Some(ref f) = mat_filter {
                if filter_has_correlated_sub {
                    let row_filter = self.materialize_correlated(f, row, txn)?;
                    if !crate::expr_engine::ExprEngine::eval_filter(&row_filter, row)
                        .map_err(FalconError::Execution)? { continue; }
                } else if !crate::expr_engine::ExprEngine::eval_filter(f, row)
                    .map_err(FalconError::Execution)? { continue; }
            }
            if has_window { filtered.push(row); }
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
            if off < result_rows.len() { result_rows.drain(..off); } else { result_rows.clear(); }
        }
        if let Some(lim) = limit { result_rows.truncate(lim); }

        if visible_projection_count < projections.len() {
            for row in &mut result_rows { row.values.truncate(visible_projection_count); }
            columns.truncate(visible_projection_count);
        }

        Ok(ExecutionResult::Query { columns, rows: result_rows })
    }

    pub(crate) fn exec_seq_scan(
        &self,
        table_id: falcon_common::types::TableId,
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
    ) -> Result<ExecutionResult, FalconError> {
        // Pre-compute aggregate/window flags (needed for scan strategy selection)
        let has_window = projections
            .iter()
            .any(|p| matches!(p, BoundProjection::Window(..)));
        let has_agg = projections
            .iter()
            .any(|p| matches!(p, BoundProjection::Aggregate(..)));

        // ── COUNT(*) fast path: early exit before scanning ──
        // When ALL projections are COUNT(*) with no filter/GROUP BY/HAVING on a real table,
        // just count visible rows via MVCC visibility checks — zero row cloning.
        if has_agg
            && !has_window
            && group_by.is_empty()
            && grouping_sets.is_empty()
            && filter.is_none()
            && having.is_none()
            && virtual_rows.is_empty()
            && table_id != TableId(0)
            && cte_data.get(&table_id).is_none()
        {
            let is_pure_count_star = projections.iter().all(|p| {
                matches!(
                    p,
                    BoundProjection::Aggregate(AggFunc::Count, None, _, false, None)
                )
            });
            if is_pure_count_star {
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                if let Ok(count) = self.storage.count_visible(table_id, txn.txn_id, read_ts) {
                    let columns = self.resolve_output_columns(projections, schema);
                    let row = OwnedRow::new(
                        projections
                            .iter()
                            .map(|_| Datum::Int64(count as i64))
                            .collect(),
                    );
                    return Ok(ExecutionResult::Query {
                        columns,
                        rows: vec![row],
                    });
                }
            }

            // ── Streaming aggregate fast path ──
            // For mixed simple aggregates (COUNT/SUM/AVG/MIN/MAX) on column refs,
            // compute in a single pass without materializing any rows.
            if let Some(result) = self.try_streaming_aggs(
                projections, schema, table_id, txn,
            ) {
                return Ok(result);
            }
        }

        // ── Fused streaming aggregate path ──
        // For aggregate queries (with or without GROUP BY / WHERE filter) on a real
        // rowstore table, compute results in a single pass through MVCC chains
        // without cloning any rows. This avoids O(N) row + PK allocation.
        if (has_agg || !group_by.is_empty())
            && !has_window
            && grouping_sets.is_empty()
            && having.is_none()
            && virtual_rows.is_empty()
            && table_id != TableId(0)
            && cte_data.get(&table_id).is_none()
            && !filter.is_some_and(Self::expr_has_outer_ref)
            && Self::is_fused_eligible(projections, grouping_sets, having)
        {
            return self.exec_fused_aggregate(
                table_id, schema, projections, filter, group_by,
                order_by, limit, offset, distinct, txn,
            );
        }

        // Check if this is a dual (no-FROM) or CTE table, otherwise scan storage
        let (raw_rows, effective_filter): (Vec<(Vec<u8>, OwnedRow)>, Option<BoundExpr>) =
            if !virtual_rows.is_empty() {
                // VALUES clause or GENERATE_SERIES — use inline data directly
                (
                    virtual_rows.iter().map(|r| (Vec::new(), r.clone())).collect(),
                    filter.cloned(),
                )
            } else if table_id == TableId(0) {
                // Virtual dual table — single empty row for SELECT without FROM
                (vec![(Vec::new(), OwnedRow::new(Vec::new()))], filter.cloned())
            } else if let Some(cte_rows) = cte_data.get(&table_id) {
                (
                    cte_rows.iter().map(|r| (Vec::new(), r.clone())).collect(),
                    filter.cloned(),
                )
            } else if let Some((pk, remaining)) = self.try_pk_point_lookup(filter, schema) {
                // PK point lookup: O(1) hash lookup instead of full scan
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                match self.storage.get(table_id, &pk, txn.txn_id, read_ts)? {
                    Some(row) => (vec![(pk, row)], remaining),
                    None => (vec![], remaining),
                }
            } else if let Some((col_idx, key, remaining)) =
                self.try_index_scan_predicate(filter, table_id)
            {
                // Index scan: use secondary index instead of full table scan
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                let rows = self
                    .storage
                    .index_scan(table_id, col_idx, &key, txn.txn_id, read_ts)?;
                (rows, remaining)
            } else if let Some((col_idx, lo, hi, remaining)) =
                self.try_index_range_scan_predicate(filter, table_id)
            {
                // Index range scan: use secondary index for range queries
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                let lo_ref = lo.as_ref().map(|(k, inc)| (k.as_slice(), *inc));
                let hi_ref = hi.as_ref().map(|(k, inc)| (k.as_slice(), *inc));
                let rows = self.storage.index_range_scan(
                    table_id, col_idx, lo_ref, hi_ref, txn.txn_id, read_ts,
                )?;
                (rows, remaining)
            } else if let Some((k, ascending)) = self.try_pk_ordered_limit(
                schema, filter, order_by, limit, offset, group_by, distinct,
                &has_agg, &has_window, virtual_rows, table_id, cte_data,
            ) {
                // ORDER BY <pk> LIMIT K: only materialize K rows
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                let rows = self.storage.scan_top_k_by_pk(
                    table_id, txn.txn_id, read_ts, k, ascending,
                )?;
                // Rows are already sorted and limited — clear order_by/limit
                // to skip redundant sort+limit later. We do this via a wrapper
                // that returns the rows and signals "already sorted".
                (rows, filter.cloned())
            } else {
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                (
                    self.storage.scan(table_id, txn.txn_id, read_ts)?,
                    filter.cloned(),
                )
            };

        // Materialize any subqueries in filter/having.
        // materialize_subqueries now leaves correlated subqueries (containing OuterColumnRef)
        // in place — they will be handled per-row below.
        let mat_filter = self.materialize_filter(effective_filter.as_ref(), txn)?;
        let mat_having = self.materialize_filter(having, txn)?;

        // Check if the materialized filter still contains correlated subqueries.
        // If so, we need per-row re-materialization during filtering.
        let filter_has_correlated_sub = mat_filter.as_ref().is_some_and(Self::expr_has_outer_ref);

        // (has_window and has_agg already computed above)

        if (has_agg || !group_by.is_empty() || !grouping_sets.is_empty()) && !has_window {
            // ── Vectorized columnar aggregate path (ColumnStore tables only) ──
            // When: pure aggregate (no GROUP BY, no correlated filter, no HAVING subquery),
            // and the table is a ColumnStore — use scan_columnar to get typed column vectors
            // directly, bypassing row-at-a-time OwnedRow deserialization.
            let is_simple_agg = group_by.is_empty()
                && grouping_sets.is_empty()
                && having.is_none()
                && !filter_has_correlated_sub
                && virtual_rows.is_empty()
                && table_id != TableId(0)
                && cte_data.get(&table_id).is_none();

            if is_simple_agg {
                // ── COUNT(*) fast path (rowstore) ──
                // When ALL projections are COUNT(*) and there's no filter, skip full scan
                // and just count visible rows via MVCC visibility checks (no row cloning).
                let is_pure_count_star = mat_filter.is_none()
                    && projections.iter().all(|p| matches!(
                        p,
                        BoundProjection::Aggregate(AggFunc::Count, None, _, false, None)
                    ));
                if is_pure_count_star {
                    let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                    if let Ok(count) = self.storage.count_visible(table_id, txn.txn_id, read_ts) {
                        let columns = self.resolve_output_columns(projections, schema);
                        let row = OwnedRow::new(
                            projections.iter().map(|_| Datum::Int64(count as i64)).collect(),
                        );
                        return Ok(ExecutionResult::Query {
                            columns,
                            rows: vec![row],
                        });
                    }
                }

                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                if let Some(col_vecs) = self.storage.scan_columnar(table_id, txn.txn_id, read_ts) {
                    return self.exec_columnar_aggregate(
                        col_vecs,
                        schema,
                        projections,
                        mat_filter.as_ref(),
                        order_by,
                        limit,
                        offset,
                        distinct,
                    );
                }
            }

            return self.exec_aggregate(
                &raw_rows,
                schema,
                projections,
                mat_filter.as_ref(),
                group_by,
                grouping_sets,
                mat_having.as_ref(),
                order_by,
                limit,
                offset,
                distinct,
            );
        }

        // Project — handle correlated subqueries in projections
        let projs_have_correlated = projections.iter().any(|p| match p {
            BoundProjection::Expr(e, _) => Self::expr_has_outer_ref(e),
            _ => false,
        });

        let mut columns = self.resolve_output_columns(projections, schema);

        // Early-limit capacity hint: if no ORDER BY/DISTINCT/window, we know the max output size.
        let capacity_hint = match (
            order_by.is_empty() && matches!(distinct, DistinctMode::None) && !has_window,
            limit,
            offset,
        ) {
            (true, Some(lim), Some(off)) => (lim + off).min(raw_rows.len()),
            (true, Some(lim), None) => lim.min(raw_rows.len()),
            _ => raw_rows.len(),
        };

        // Effective row limit for early cutoff (only when no sort/distinct/window).
        let early_limit = if order_by.is_empty() && matches!(distinct, DistinctMode::None) && !has_window {
            limit.map(|l| l + offset.unwrap_or(0))
        } else {
            None
        };

        // ── Single-pass filter + project ──
        // For the common non-window path we avoid materialising a separate `filtered` Vec:
        // filter and project are fused in one loop, eliminating the intermediate allocation.
        // Window functions still need a `filtered` reference Vec for frame computation.
        let mut filtered: Vec<&OwnedRow> = Vec::new();
        let mut result_rows: Vec<OwnedRow> = Vec::with_capacity(capacity_hint);

        // Detect column-only projections: when all projections are Column(idx),
        // we can gather values by index without going through eval_expr at all.
        let col_only_indices: Option<Vec<usize>> = if !projs_have_correlated {
            let indices: Vec<usize> = projections
                .iter()
                .filter_map(|p| match p {
                    BoundProjection::Column(idx, _) => Some(*idx),
                    _ => None,
                })
                .collect();
            if indices.len() == projections.len() {
                Some(indices)
            } else {
                None
            }
        } else {
            None
        };

        // Inline helper: project a row using the fastest available path.
        macro_rules! do_project {
            ($row:expr) => {
                if projs_have_correlated {
                    self.project_row_correlated($row, projections, txn)?
                } else if let Some(ref idx) = col_only_indices {
                    // Fast path: column-only projection — gather by index, no eval
                    let vals: Vec<Datum> = idx
                        .iter()
                        .map(|&i| $row.get(i).cloned().unwrap_or(Datum::Null))
                        .collect();
                    OwnedRow::new(vals)
                } else {
                    self.project_row($row, projections)?
                }
            };
        }

        if let Some(ref f) = mat_filter {
            if filter_has_correlated_sub {
                // Correlated subquery: must re-materialise per row (no parallel/vectorized)
                for (_pk, row) in &raw_rows {
                    if let Some(max) = early_limit {
                        if result_rows.len() >= max { break; }
                    }
                    let row_filter = self.materialize_correlated(f, row, txn)?;
                    if ExprEngine::eval_filter(&row_filter, row).map_err(FalconError::Execution)? {
                        if has_window { filtered.push(row); }
                        result_rows.push(do_project!(row));
                    }
                }
            } else if self.parallel_config.should_parallelize(raw_rows.len())
                && is_vectorizable(projections, mat_filter.as_ref())
            {
                // ── Parallel filter path — project after gather ──
                let matched = parallel_filter(&raw_rows, f, &self.parallel_config);
                for idx in matched {
                    if let Some(max) = early_limit {
                        if result_rows.len() >= max { break; }
                    }
                    let row = &raw_rows[idx].1;
                    if has_window { filtered.push(row); }
                    result_rows.push(do_project!(row));
                }
            } else if raw_rows.len() >= 256 && is_vectorizable(projections, mat_filter.as_ref()) {
                // ── Vectorized filter path (single-threaded, batched) ──
                let num_cols = schema.columns.len();
                let mut batch = RecordBatch::from_row_pairs(&raw_rows, num_cols);
                vectorized_filter(&mut batch, f);
                for &idx in &*batch.active_indices() {
                    if let Some(max) = early_limit {
                        if result_rows.len() >= max { break; }
                    }
                    let row = &raw_rows[idx].1;
                    if has_window { filtered.push(row); }
                    result_rows.push(do_project!(row));
                }
            } else {
                // ── Row-at-a-time filter+project (small data, fused single pass) ──
                for (_pk, row) in &raw_rows {
                    if let Some(max) = early_limit {
                        if result_rows.len() >= max { break; }
                    }
                    if ExprEngine::eval_filter(f, row).map_err(FalconError::Execution)? {
                        if has_window { filtered.push(row); }
                        result_rows.push(do_project!(row));
                    }
                }
            }
        } else {
            // No filter — project all rows
            for (_pk, row) in &raw_rows {
                if let Some(max) = early_limit {
                    if result_rows.len() >= max { break; }
                }
                if has_window { filtered.push(row); }
                result_rows.push(do_project!(row));
            }
        }

        // Compute window functions and inject values
        if has_window {
            self.compute_window_functions(&filtered, projections, &mut result_rows)?
        }

        // Order by (must happen before DISTINCT ON so we keep the right row per group)
        crate::external_sort::sort_rows(&mut result_rows, order_by, self.external_sorter.as_ref())?;

        // Distinct (after ORDER BY so DISTINCT ON picks the first row per group)
        self.apply_distinct(distinct, &mut result_rows);

        // Offset + Limit
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
        let mut values = Vec::with_capacity(left.values.len() + right.values.len());
        values.extend_from_slice(&left.values);
        values.extend_from_slice(&right.values);
        OwnedRow::new(values)
    }

    /// Fill `buf` with the merged row values (left ++ right), reusing the buffer
    /// across calls to avoid per-match Vec allocation. Returns an OwnedRow that
    /// takes ownership of `buf`'s contents; caller must reclaim via `row.values`.
    #[inline]
    pub(crate) fn merge_rows_into(
        &self,
        left: &OwnedRow,
        right: &OwnedRow,
        buf: &mut Vec<Datum>,
    ) -> OwnedRow {
        buf.clear();
        buf.extend_from_slice(&left.values);
        buf.extend_from_slice(&right.values);
        OwnedRow::new(std::mem::take(buf))
    }

    /// Remove duplicate rows in-place using a HashSet with byte-encoded keys.
    /// Uses compact binary encoding instead of `format!("{d}")` string keys,
    /// eliminating all per-row String allocations.
    pub(crate) fn dedup_rows(&self, rows: &mut Vec<OwnedRow>) {
        let mut seen: std::collections::HashSet<Box<[u8]>> =
            std::collections::HashSet::with_capacity(rows.len());
        let mut buf = Vec::with_capacity(64);
        rows.retain(|row| {
            crate::executor_aggregate::encode_group_key_all(&mut buf, row);
            // Two-step: only clone+box for genuinely new keys
            if seen.contains(buf.as_slice()) {
                false
            } else {
                seen.insert(buf.clone().into_boxed_slice());
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
                let mut seen: std::collections::HashSet<Box<[u8]>> =
                    std::collections::HashSet::with_capacity(rows.len());
                let mut buf = Vec::with_capacity(64);
                rows.retain(|row| {
                    crate::executor_aggregate::encode_group_key(&mut buf, row, indices);
                    if seen.contains(buf.as_slice()) {
                        false
                    } else {
                        seen.insert(buf.clone().into_boxed_slice());
                        true
                    }
                });
            }
        }
    }

    /// Project a row with correlated subquery support.
    /// For expression projections containing OuterColumnRef, substitute with
    /// current row values and materialize subqueries per-row.
    #[inline]
    pub(crate) fn project_row_correlated(
        &self,
        row: &OwnedRow,
        projections: &[BoundProjection],
        txn: &TxnHandle,
    ) -> Result<OwnedRow, FalconError> {
        let mut values = Vec::with_capacity(projections.len());
        for proj in projections {
            match proj {
                BoundProjection::Column(idx, _) => {
                    values.push(row.get(*idx).cloned().unwrap_or(Datum::Null));
                }
                BoundProjection::Expr(expr, _) => {
                    if Self::expr_has_outer_ref(expr) {
                        let mat = self.materialize_correlated(expr, row, txn)?;
                        let val =
                            ExprEngine::eval_row(&mat, row).map_err(FalconError::Execution)?;
                        values.push(val);
                    } else {
                        let val =
                            ExprEngine::eval_row(expr, row).map_err(FalconError::Execution)?;
                        values.push(val);
                    }
                }
                BoundProjection::Aggregate(..)
                | BoundProjection::Window(..) => {
                    values.push(Datum::Null);
                }
            }
        }
        Ok(OwnedRow::new(values))
    }

    #[inline]
    pub(crate) fn project_row(
        &self,
        row: &OwnedRow,
        projections: &[BoundProjection],
    ) -> Result<OwnedRow, FalconError> {
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

    /// Buffer-reusing projection: fills `buf`, produces an OwnedRow, caller
    /// reclaims the Vec from the returned row for reuse on the next iteration.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn project_row_into(
        &self,
        row: &OwnedRow,
        projections: &[BoundProjection],
        buf: &mut Vec<Datum>,
    ) -> Result<OwnedRow, FalconError> {
        buf.clear();
        for proj in projections {
            match proj {
                BoundProjection::Column(idx, _) => {
                    buf.push(row.get(*idx).cloned().unwrap_or(Datum::Null));
                }
                BoundProjection::Expr(expr, _) => {
                    let val = self.eval_expr_with_sequences(expr, row)?;
                    buf.push(val);
                }
                BoundProjection::Aggregate(..) => buf.push(Datum::Null),
                BoundProjection::Window(..) => buf.push(Datum::Null),
            }
        }
        Ok(OwnedRow::new(std::mem::take(buf)))
    }

    /// Evaluate an expression, handling sequence functions via storage.
    #[inline]
    pub(crate) fn eval_expr_with_sequences(
        &self,
        expr: &BoundExpr,
        row: &OwnedRow,
    ) -> Result<Datum, FalconError> {
        match expr {
            BoundExpr::SequenceNextval(name) => {
                let val = self
                    .storage
                    .sequence_nextval(name)
                    .map_err(FalconError::Storage)?;
                Ok(Datum::Int64(val))
            }
            BoundExpr::SequenceCurrval(name) => {
                let val = self
                    .storage
                    .sequence_currval(name)
                    .map_err(FalconError::Storage)?;
                Ok(Datum::Int64(val))
            }
            BoundExpr::SequenceSetval(name, value) => {
                let val = self
                    .storage
                    .sequence_setval(name, *value)
                    .map_err(FalconError::Storage)?;
                Ok(Datum::Int64(val))
            }
            _ => ExprEngine::eval_row(expr, row).map_err(FalconError::Execution),
        }
    }
}
