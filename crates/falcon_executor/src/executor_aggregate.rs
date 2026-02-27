#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::{ExecutionError, FalconError};
use falcon_common::schema::TableSchema;
use falcon_common::types::{DataType, TableId};
use falcon_sql_frontend::types::*;
use falcon_txn::TxnHandle;

use crate::executor::{ExecutionResult, Executor};
use crate::expr_engine::ExprEngine;

// ── Streaming aggregate accumulator ─────────────────────────────────────────

/// Per-projection accumulator state for fused streaming aggregation.
enum ProjAccum {
    /// COUNT(*) — no expression
    CountStar(i64),
    /// COUNT(expr) / SUM / AVG / MIN / MAX
    Agg {
        func: AggFunc,
        count: i64,
        sum_i: i64,
        sum_f: f64,
        has_int: bool,
        has_float: bool,
        min_val: Option<Datum>,
        max_val: Option<Datum>,
    },
    /// Column or Expr projection — stores first row's value
    FirstVal(Datum),
}

impl ProjAccum {
    const fn new_count_star() -> Self {
        Self::CountStar(0)
    }

    fn new_agg(func: &AggFunc) -> Self {
        Self::Agg {
            func: func.clone(),
            count: 0,
            sum_i: 0,
            sum_f: 0.0,
            has_int: false,
            has_float: false,
            min_val: None,
            max_val: None,
        }
    }

    const fn feed_count_star(&mut self) {
        if let Self::CountStar(n) = self {
            *n += 1;
        }
    }

    fn feed_value(&mut self, val: &Datum) {
        match self {
            Self::CountStar(n) => *n += 1,
            Self::Agg {
                func,
                count,
                sum_i,
                sum_f,
                has_int,
                has_float,
                min_val,
                max_val,
            } => {
                if val.is_null() {
                    return;
                }
                match func {
                    AggFunc::Count => {
                        *count += 1;
                    }
                    AggFunc::Sum => {
                        match val {
                            Datum::Int32(v) => {
                                *sum_i += i64::from(*v);
                                *has_int = true;
                            }
                            Datum::Int64(v) => {
                                *sum_i += v;
                                *has_int = true;
                            }
                            Datum::Float64(v) => {
                                *sum_f += v;
                                *has_float = true;
                            }
                            _ => {
                                if let Some(f) = val.as_f64() {
                                    *sum_f += f;
                                    *has_float = true;
                                }
                            }
                        }
                        *count += 1;
                    }
                    AggFunc::Avg => {
                        if let Some(f) = val.as_f64() {
                            *sum_f += f;
                            *count += 1;
                        }
                    }
                    AggFunc::Min => {
                        *min_val = Some(match min_val.take() {
                            None => val.clone(),
                            Some(cur) => {
                                if val < &cur {
                                    val.clone()
                                } else {
                                    cur
                                }
                            }
                        });
                    }
                    AggFunc::Max => {
                        *max_val = Some(match max_val.take() {
                            None => val.clone(),
                            Some(cur) => {
                                if val > &cur {
                                    val.clone()
                                } else {
                                    cur
                                }
                            }
                        });
                    }
                    _ => {}
                }
            }
            Self::FirstVal(_) => {}
        }
    }

    /// Merge two partial accumulators (used by parallel map-reduce aggregation).
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::CountStar(a), Self::CountStar(b)) => Self::CountStar(a + b),
            (
                Self::Agg {
                    func,
                    count: c1,
                    sum_i: s1,
                    sum_f: sf1,
                    has_int: hi1,
                    has_float: hf1,
                    min_val: mn1,
                    max_val: mx1,
                },
                Self::Agg {
                    count: c2,
                    sum_i: s2,
                    sum_f: sf2,
                    has_int: hi2,
                    has_float: hf2,
                    min_val: mn2,
                    max_val: mx2,
                    ..
                },
            ) => Self::Agg {
                func,
                count: c1 + c2,
                sum_i: s1 + s2,
                sum_f: sf1 + sf2,
                has_int: hi1 || hi2,
                has_float: hf1 || hf2,
                min_val: match (mn1, mn2) {
                    (None, b) => b,
                    (a, None) => a,
                    (Some(a), Some(b)) => Some(if a < b { a } else { b }),
                },
                max_val: match (mx1, mx2) {
                    (None, b) => b,
                    (a, None) => a,
                    (Some(a), Some(b)) => Some(if a > b { a } else { b }),
                },
            },
            (a, _) => a,
        }
    }

    fn result(&self) -> Datum {
        match self {
            Self::CountStar(n) => Datum::Int64(*n),
            Self::Agg {
                func,
                count,
                sum_i,
                sum_f,
                has_int,
                has_float,
                min_val,
                max_val,
                ..
            } => match func {
                AggFunc::Count => Datum::Int64(*count),
                AggFunc::Sum => {
                    if *count == 0 {
                        Datum::Null
                    } else if *has_float {
                        Datum::Float64(*sum_f + *sum_i as f64)
                    } else if *has_int {
                        Datum::Int64(*sum_i)
                    } else {
                        Datum::Null
                    }
                }
                AggFunc::Avg => {
                    if *count == 0 {
                        Datum::Null
                    } else {
                        Datum::Float64(*sum_f / *count as f64)
                    }
                }
                AggFunc::Min => min_val.clone().unwrap_or(Datum::Null),
                AggFunc::Max => max_val.clone().unwrap_or(Datum::Null),
                _ => Datum::Null,
            },
            Self::FirstVal(d) => d.clone(),
        }
    }
}

/// Encode ALL columns of a row into a reusable buffer — avoids the need to
/// build a `Vec<usize>` of `(0..ncols)` indices just for dedup / DISTINCT.
#[inline]
pub(crate) fn encode_group_key_all(buf: &mut Vec<u8>, row: &OwnedRow) {
    buf.clear();
    for datum in &row.values {
        encode_datum_into(buf, datum);
    }
}

/// Encode a single Datum into the buffer (shared by both key encoders).
#[inline]
fn encode_datum_into(buf: &mut Vec<u8>, datum: &Datum) {
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

/// Encode group key from row into a reusable buffer (avoids per-row allocation).
pub(crate) fn encode_group_key(buf: &mut Vec<u8>, row: &OwnedRow, group_cols: &[usize]) {
    buf.clear();
    for &col_idx in group_cols {
        let datum = row.get(col_idx).unwrap_or(&Datum::Null);
        encode_datum_into(buf, datum);
    }
}

impl Executor {
    /// Check if a fused streaming aggregate is possible (no row cloning).
    /// Returns None if the query shape is not eligible.
    pub(crate) fn is_fused_eligible(
        projections: &[BoundProjection],
        grouping_sets: &[Vec<usize>],
        having: Option<&BoundExpr>,
    ) -> bool {
        if !grouping_sets.is_empty() || having.is_some() {
            return false;
        }
        for proj in projections {
            match proj {
                BoundProjection::Aggregate(func, _expr, _, distinct, agg_filter) => {
                    if *distinct || agg_filter.is_some() {
                        return false;
                    }
                    match func {
                        AggFunc::Count | AggFunc::Sum | AggFunc::Avg
                        | AggFunc::Min | AggFunc::Max => {}
                        _ => return false,
                    }
                }
                BoundProjection::Column(..) | BoundProjection::Expr(..) => {}
                BoundProjection::Window(..) => return false,
            }
        }
        true
    }

    /// Create fresh accumulators for each projection.
    fn create_accums(projections: &[BoundProjection]) -> Vec<ProjAccum> {
        projections
            .iter()
            .map(|p| match p {
                BoundProjection::Aggregate(_func, None, _, _, _) => ProjAccum::new_count_star(),
                BoundProjection::Aggregate(func, Some(_), _, _, _) => ProjAccum::new_agg(func),
                _ => ProjAccum::FirstVal(Datum::Null),
            })
            .collect()
    }

    /// Feed one row into a group's accumulators.
    fn feed_row_to_accums(
        accums: &mut [ProjAccum],
        projections: &[BoundProjection],
        row: &OwnedRow,
        is_first: bool,
    ) -> Result<(), FalconError> {
        for (i, proj) in projections.iter().enumerate() {
            match proj {
                BoundProjection::Aggregate(_func, None, _, _, _) => {
                    accums[i].feed_count_star();
                }
                BoundProjection::Aggregate(_func, Some(expr), _, _, _) => {
                    let val = ExprEngine::eval_row(expr, row)
                        .map_err(FalconError::Execution)?;
                    accums[i].feed_value(&val);
                }
                BoundProjection::Column(idx, _) => {
                    if is_first {
                        accums[i] = ProjAccum::FirstVal(
                            row.get(*idx).cloned().unwrap_or(Datum::Null),
                        );
                    }
                }
                BoundProjection::Expr(expr, _) => {
                    if is_first {
                        let val = ExprEngine::eval_row(expr, row)
                            .map_err(FalconError::Execution)?;
                        accums[i] = ProjAccum::FirstVal(val);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Fused streaming aggregate: single pass through MVCC chains, zero row cloning.
    /// Handles WHERE filter + GROUP BY + simple aggregates (COUNT/SUM/AVG/MIN/MAX).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn exec_fused_aggregate(
        &self,
        table_id: TableId,
        schema: &TableSchema,
        projections: &[BoundProjection],
        filter: Option<&BoundExpr>,
        group_by: &[usize],
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: &DistinctMode,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let mat_filter = self.materialize_filter(filter, txn)?;
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());

        let columns = self.resolve_output_columns(projections, schema);

        if group_by.is_empty() {
            // ── No GROUP BY: single global group (parallel map-reduce) ──
            struct PartialNoGroup {
                accums: Vec<ProjAccum>,
                first: bool,
                error: Option<FalconError>,
            }
            let mat_filter_ref = &mat_filter;
            let result = self.storage.map_reduce_visible(
                table_id,
                txn.txn_id,
                read_ts,
                || PartialNoGroup {
                    accums: Self::create_accums(projections),
                    first: true,
                    error: None,
                },
                |state, row| {
                    if state.error.is_some() {
                        return;
                    }
                    if let Some(ref f) = *mat_filter_ref {
                        match ExprEngine::eval_filter(f, row) {
                            Ok(true) => {}
                            Ok(false) => return,
                            Err(e) => {
                                state.error = Some(FalconError::Execution(e));
                                return;
                            }
                        }
                    }
                    if let Err(e) = Self::feed_row_to_accums(
                        &mut state.accums, projections, row, state.first,
                    ) {
                        state.error = Some(e);
                        return;
                    }
                    state.first = false;
                },
                |a, b| {
                    if a.error.is_some() {
                        return a;
                    }
                    if b.error.is_some() {
                        return b;
                    }
                    PartialNoGroup {
                        accums: a
                            .accums
                            .into_iter()
                            .zip(b.accums)
                            .map(|(x, y)| x.merge(y))
                            .collect(),
                        first: a.first && b.first,
                        error: None,
                    }
                },
            )?;
            if let Some(e) = result.error {
                return Err(e);
            }

            let values: Vec<Datum> = result.accums.iter().map(ProjAccum::result).collect();
            return Ok(ExecutionResult::Query {
                columns,
                rows: vec![OwnedRow::new(values)],
            });
        }

        // ── GROUP BY: hash-aggregate (parallel map-reduce) ──
        struct PartialGroupBy {
            groups: HashMap<Vec<u8>, (Vec<ProjAccum>, bool)>,
            key_buf: Vec<u8>,
            error: Option<FalconError>,
            group_limit: usize,
        }
        let mat_filter_ref = &mat_filter;
        let group_limit = self.hash_agg_group_limit;
        let result = self.storage.map_reduce_visible(
            table_id,
            txn.txn_id,
            read_ts,
            || PartialGroupBy {
                groups: HashMap::new(),
                key_buf: Vec::with_capacity(32),
                error: None,
                group_limit,
            },
            |state, row| {
                if state.error.is_some() {
                    return;
                }
                if let Some(ref f) = *mat_filter_ref {
                    match ExprEngine::eval_filter(f, row) {
                        Ok(true) => {}
                        Ok(false) => return,
                        Err(e) => {
                            state.error = Some(FalconError::Execution(e));
                            return;
                        }
                    }
                }
                encode_group_key(&mut state.key_buf, row, group_by);

                let (accums, is_first) =
                    if let Some(entry) = state.groups.get_mut(state.key_buf.as_slice()) {
                        entry
                    } else {
                        // Check group count limit before inserting a new group
                        if state.group_limit > 0 && state.groups.len() >= state.group_limit {
                            state.error = Some(FalconError::Execution(
                                ExecutionError::ResourceExhausted(format!(
                                    "hash aggregation group limit exceeded: {} groups (limit: {}). \
                                     Reduce cardinality or increase falcon.spill.hash_agg_group_limit",
                                    state.groups.len(),
                                    state.group_limit,
                                )),
                            ));
                            return;
                        }
                        let a = Self::create_accums(projections);
                        state.groups.insert(state.key_buf.clone(), (a, true));
                        state.groups.get_mut(state.key_buf.as_slice()).unwrap()
                    };

                let first = *is_first;
                *is_first = false;

                if let Err(e) = Self::feed_row_to_accums(
                    accums, projections, row, first,
                ) {
                    state.error = Some(e);
                }
            },
            |mut a, b| {
                if a.error.is_some() {
                    return a;
                }
                if b.error.is_some() {
                    return b;
                }
                // Merge b's groups into a — in-place to avoid Vec allocation per group
                for (key, (b_accums, b_first)) in b.groups {
                    if let Some((a_accums, a_first)) = a.groups.get_mut(&key) {
                        // Merge in-place: drain b_accums and merge into a_accums
                        for (a_acc, b_acc) in a_accums.iter_mut().zip(b_accums) {
                            let old = std::mem::replace(a_acc, ProjAccum::CountStar(0));
                            *a_acc = old.merge(b_acc);
                        }
                        *a_first = *a_first && b_first;
                    } else {
                        a.groups.insert(key, (b_accums, b_first));
                    }
                }
                a
            },
        )?;
        if let Some(e) = result.error {
            return Err(e);
        }
        let groups = result.groups;

        // Convert groups to result rows
        let mut result_rows: Vec<OwnedRow> = Vec::with_capacity(groups.len());
        for (accums, _) in groups.values() {
            let values: Vec<Datum> = accums.iter().map(ProjAccum::result).collect();
            result_rows.push(OwnedRow::new(values));
        }

        // ORDER BY
        crate::external_sort::sort_rows(
            &mut result_rows,
            order_by,
            self.external_sorter.as_ref(),
        )?;

        // DISTINCT
        self.apply_distinct(distinct, &mut result_rows);

        // OFFSET + LIMIT
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

        Ok(ExecutionResult::Query {
            columns,
            rows: result_rows,
        })
    }

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
    ) -> Result<ExecutionResult, FalconError> {
        // Filter first (WHERE)
        let mut filtered: Vec<&OwnedRow> = Vec::new();
        for (_pk, row) in raw_rows {
            if let Some(f) = filter {
                if !ExprEngine::eval_filter(f, row).map_err(FalconError::Execution)? {
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
                    projections,
                    &filtered,
                    gset,
                    group_by,
                    having,
                )?;
                result_rows.extend(set_rows);
            }
            return self.finalize_aggregate(
                result_rows,
                order_by,
                distinct,
                offset,
                limit,
                columns,
            );
        }

        if group_by.is_empty() {
            // No GROUP BY — single group over all filtered rows
            let result_values = self.compute_group_row(projections, &filtered)?;
            return Ok(ExecutionResult::Query {
                columns,
                rows: vec![OwnedRow::new(result_values)],
            });
        }

        // GROUP BY: build groups using HashMap with byte-encoded keys — O(n).
        // group_map maps key → index into group_indices (insertion-ordered).
        // Only one key_buf.clone() per distinct group (stored in group_map key).
        let mut group_map: HashMap<Vec<u8>, usize> =
            HashMap::with_capacity(filtered.len().min(1024));
        let mut group_indices: Vec<Vec<usize>> = Vec::new();
        let mut key_buf = Vec::with_capacity(64);
        for (i, row) in filtered.iter().enumerate() {
            encode_group_key(&mut key_buf, row, group_by);
            if let Some(&gi) = group_map.get(key_buf.as_slice()) {
                group_indices[gi].push(i);
            } else {
                let gi = group_indices.len();
                group_map.insert(key_buf.clone(), gi);
                group_indices.push(vec![i]);
            }
        }

        // Compute aggregates per group — reuse a single Vec buffer across groups
        let mut result_rows: Vec<OwnedRow> = Vec::with_capacity(group_indices.len());
        let mut group_rows: Vec<&OwnedRow> = Vec::new();
        for indices in &group_indices {
            group_rows.clear();
            group_rows.extend(indices.iter().map(|&i| filtered[i]));
            let row_values = self.compute_group_row(projections, &group_rows)?;
            let result_row = OwnedRow::new(row_values);

            // Apply HAVING filter — evaluates aggregate expressions over the group
            if let Some(h) = having {
                if !ExprEngine::eval_having_filter(h, &group_rows)
                    .map_err(FalconError::Execution)?
                {
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
    ) -> Result<ExecutionResult, FalconError> {
        // Order by
        crate::external_sort::sort_rows(&mut result_rows, order_by, self.external_sorter.as_ref())?;

        // DISTINCT
        if !matches!(distinct, DistinctMode::None) {
            let mut seen: std::collections::HashSet<Box<[u8]>> =
                std::collections::HashSet::with_capacity(result_rows.len());
            let mut buf = Vec::with_capacity(64);
            result_rows.retain(|row| {
                encode_group_key_all(&mut buf, row);
                seen.insert(buf.clone().into_boxed_slice())
            });
        }

        // Offset
        if let Some(off) = offset {
            if off < result_rows.len() {
                result_rows.drain(..off);
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
    ) -> Result<Vec<OwnedRow>, FalconError> {
        if active_set.is_empty() {
            // Grand total: single group over all rows
            let row_values = self.compute_group_row_with_nulls(
                projections,
                filtered,
                active_set,
                all_group_cols,
            )?;
            if let Some(h) = having {
                if !ExprEngine::eval_having_filter(h, filtered).map_err(FalconError::Execution)? {
                    return Ok(vec![]);
                }
            }
            return Ok(vec![OwnedRow::new(row_values)]);
        }

        // Build groups by active_set columns using HashMap — O(n) amortized.
        // Single clone per distinct key (stored in group_map).
        let mut group_map: HashMap<Vec<u8>, usize> =
            HashMap::with_capacity(filtered.len().min(256));
        let mut group_indices: Vec<Vec<usize>> = Vec::new();
        let mut key_buf = Vec::with_capacity(64);
        for (i, row) in filtered.iter().enumerate() {
            encode_group_key(&mut key_buf, row, active_set);
            if let Some(&gi) = group_map.get(key_buf.as_slice()) {
                group_indices[gi].push(i);
            } else {
                let gi = group_indices.len();
                group_map.insert(key_buf.clone(), gi);
                group_indices.push(vec![i]);
            }
        }

        let mut result_rows = Vec::new();
        let mut group_rows_buf: Vec<&OwnedRow> = Vec::new();
        for indices in &group_indices {
            group_rows_buf.clear();
            group_rows_buf.extend(indices.iter().map(|&i| filtered[i]));
            let group_rows = &group_rows_buf;
            let row_values = self.compute_group_row_with_nulls(
                projections,
                &group_rows,
                active_set,
                all_group_cols,
            )?;

            if let Some(h) = having {
                if !ExprEngine::eval_having_filter(h, &group_rows)
                    .map_err(FalconError::Execution)?
                {
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
    ) -> Result<Vec<Datum>, FalconError> {
        let mut values = Vec::with_capacity(projections.len());
        let empty_row = OwnedRow::new(vec![]);
        for proj in projections {
            match proj {
                BoundProjection::Aggregate(func, agg_expr, _, agg_distinct, agg_filter) => {
                    let filtered_rows = Self::apply_agg_filter(
                        group_rows,
                        agg_filter.as_deref(),
                        active_set,
                        all_group_cols,
                    );
                    let val = self.compute_aggregate(
                        func,
                        agg_expr.as_ref(),
                        *agg_distinct,
                        &filtered_rows,
                    )?;
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
                    let dummy = group_rows.first().copied().unwrap_or(&empty_row);
                    let val = ExprEngine::eval_row(&substituted, dummy)
                        .map_err(FalconError::Execution)?;
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
            BoundExpr::Not(inner) => BoundExpr::Not(Box::new(Self::substitute_grouping(
                inner,
                active_set,
                all_group_cols,
            ))),
            BoundExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => BoundExpr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| Box::new(Self::substitute_grouping(e, active_set, all_group_cols))),
                conditions: conditions
                    .iter()
                    .map(|c| Self::substitute_grouping(c, active_set, all_group_cols))
                    .collect(),
                results: results
                    .iter()
                    .map(|r| Self::substitute_grouping(r, active_set, all_group_cols))
                    .collect(),
                else_result: else_result
                    .as_ref()
                    .map(|e| Box::new(Self::substitute_grouping(e, active_set, all_group_cols))),
            },
            BoundExpr::Cast {
                expr: inner,
                target_type,
            } => BoundExpr::Cast {
                expr: Box::new(Self::substitute_grouping(inner, active_set, all_group_cols)),
                target_type: target_type.clone(),
            },
            BoundExpr::Coalesce(args) => BoundExpr::Coalesce(
                args.iter()
                    .map(|a| Self::substitute_grouping(a, active_set, all_group_cols))
                    .collect(),
            ),
            BoundExpr::Function { func, args } => BoundExpr::Function {
                func: func.clone(),
                args: args
                    .iter()
                    .map(|a| Self::substitute_grouping(a, active_set, all_group_cols))
                    .collect(),
            },
            // Leaf nodes and others: return as-is
            other => other.clone(),
        }
    }

    /// Filter group rows by an aggregate FILTER clause.
    /// Returns `Cow::Borrowed` when no filter — avoids cloning the entire slice.
    /// When active_set/all_group_cols are non-empty (GROUPING SETS context),
    /// GROUPING() nodes in the filter are substituted before evaluation.
    fn apply_agg_filter<'a>(
        rows: &'a [&'a OwnedRow],
        filter: Option<&BoundExpr>,
        active_set: &[usize],
        all_group_cols: &[usize],
    ) -> std::borrow::Cow<'a, [&'a OwnedRow]> {
        match filter {
            None => std::borrow::Cow::Borrowed(rows),
            Some(expr) => {
                let substituted = if !all_group_cols.is_empty() {
                    Self::substitute_grouping(expr, active_set, all_group_cols)
                } else {
                    expr.clone()
                };
                std::borrow::Cow::Owned(
                    rows.iter()
                        .copied()
                        .filter(|row| {
                            matches!(
                                ExprEngine::eval_row(&substituted, row),
                                Ok(Datum::Boolean(true))
                            )
                        })
                        .collect(),
                )
            }
        }
    }

    /// Compute one output row for a group of source rows.
    pub(crate) fn compute_group_row(
        &self,
        projections: &[BoundProjection],
        group_rows: &[&OwnedRow],
    ) -> Result<Vec<Datum>, FalconError> {
        let mut values = Vec::with_capacity(projections.len());
        // Hoist empty row once — used as fallback for Expr projections on empty groups.
        let empty_row = OwnedRow::new(vec![]);
        for proj in projections {
            match proj {
                BoundProjection::Aggregate(func, agg_expr, _, agg_distinct, agg_filter) => {
                    let filtered_rows =
                        Self::apply_agg_filter(group_rows, agg_filter.as_deref(), &[], &[]);
                    let val = self.compute_aggregate(
                        func,
                        agg_expr.as_ref(),
                        *agg_distinct,
                        &filtered_rows,
                    )?;
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
                    let dummy = group_rows.first().copied().unwrap_or(&empty_row);
                    let val = ExprEngine::eval_row(expr, dummy).map_err(FalconError::Execution)?;
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
    ) -> Result<Datum, FalconError> {
        // Helper: evaluate expression for each row, collecting non-null values
        let eval_all = |expr: &BoundExpr| -> Result<Vec<Datum>, FalconError> {
            let mut vals = Vec::new();
            for row in rows {
                let v = ExprEngine::eval_row(expr, row).map_err(FalconError::Execution)?;
                if !v.is_null() {
                    vals.push(v);
                }
            }
            Ok(vals)
        };

        // Helper: collect distinct non-null values
        let distinct_vals = |expr: &BoundExpr| -> Result<Vec<Datum>, FalconError> {
            let mut seen = std::collections::HashSet::new();
            let mut vals = Vec::new();
            for row in rows {
                let v = ExprEngine::eval_row(expr, row).map_err(FalconError::Execution)?;
                if v.is_null() {
                    continue;
                }
                let key = format!("{v}");
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
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "SUM requires an argument".into(),
                )))?;
                let vals = if distinct {
                    distinct_vals(expr)?
                } else {
                    eval_all(expr)?
                };
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
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "AVG requires an argument".into(),
                )))?;
                let vals = if distinct {
                    distinct_vals(expr)?
                } else {
                    eval_all(expr)?
                };
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
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
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
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
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
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
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
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
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
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "STRING_AGG requires an argument".into(),
                )))?;
                let vals = if distinct {
                    distinct_vals(expr)?
                } else {
                    eval_all(expr)?
                };
                if vals.is_empty() {
                    return Ok(Datum::Null);
                }
                // Build result string directly — avoid format!() for Text values
                let mut result = String::new();
                let mut first = true;
                for val in &vals {
                    if val.is_null() {
                        continue;
                    }
                    if !first {
                        result.push_str(sep);
                    }
                    first = false;
                    match val {
                        Datum::Text(s) => result.push_str(s),
                        other => {
                            use std::fmt::Write;
                            let _ = write!(result, "{other}");
                        }
                    }
                }
                if first {
                    // All values were NULL
                    Ok(Datum::Null)
                } else {
                    Ok(Datum::Text(result))
                }
            }
            AggFunc::ArrayAgg => {
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "ARRAY_AGG requires an argument".into(),
                )))?;
                let vals = if distinct {
                    distinct_vals(expr)?
                } else {
                    eval_all(expr)?
                };
                if vals.is_empty() {
                    Ok(Datum::Null)
                } else {
                    Ok(Datum::Array(vals))
                }
            }
            // ── Statistical aggregates (single-argument) ──
            AggFunc::VarPop | AggFunc::VarSamp | AggFunc::StddevPop | AggFunc::StddevSamp => {
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "Statistical aggregate requires an argument".into(),
                )))?;
                let vals = if distinct {
                    distinct_vals(expr)?
                } else {
                    eval_all(expr)?
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
                    AggFunc::StddevPop | AggFunc::StddevSamp => Ok(Datum::Float64(variance.sqrt())),
                    _ => Ok(Datum::Float64(variance)),
                }
            }
            // ── Two-argument statistical aggregates ──
            AggFunc::Corr
            | AggFunc::CovarPop
            | AggFunc::CovarSamp
            | AggFunc::RegrSlope
            | AggFunc::RegrIntercept
            | AggFunc::RegrR2
            | AggFunc::RegrCount
            | AggFunc::RegrAvgX
            | AggFunc::RegrAvgY
            | AggFunc::RegrSXX
            | AggFunc::RegrSYY
            | AggFunc::RegrSXY => {
                // Two-argument aggregates: we use the first expression argument.
                // The second argument is expected as a second column in the row.
                // For simplicity, we evaluate the expression and treat pairs:
                // regr(y, x) expects 2 args but our AST only captures the first.
                // We'll use a convention: if the expression evaluates to a pair-like
                // value, split it. Otherwise, use column indices 0 and 1 from the row.
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "Two-argument aggregate requires an argument".into(),
                )))?;
                // Collect (y, x) pairs from consecutive column refs or the first two columns
                let mut pairs: Vec<(f64, f64)> = Vec::new();
                for row in rows {
                    let val = ExprEngine::eval_row(expr, row).map_err(FalconError::Execution)?;
                    if val.is_null() {
                        continue;
                    }
                    // Try to get y from expr, x from next column
                    let y = val.as_f64();
                    // For two-arg aggregates, the second arg is typically the next column
                    // We use columns 0=y, 1=x as a reasonable default
                    let x = row
                        .values
                        .get(1)
                        .and_then(falcon_common::datum::Datum::as_f64)
                        .or_else(|| row.values.first().and_then(falcon_common::datum::Datum::as_f64));
                    if let (Some(yv), Some(xv)) = (y, x) {
                        pairs.push((yv, xv));
                    }
                }
                let n = pairs.len();
                if n == 0 {
                    return Ok(Datum::Null);
                }

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
                                if n < 2 {
                                    return Ok(Datum::Null);
                                }
                                Ok(Datum::Float64(sxy / (n - 1) as f64))
                            }
                            AggFunc::Corr => {
                                let denom = (sxx * syy).sqrt();
                                if denom == 0.0 {
                                    Ok(Datum::Null)
                                } else {
                                    Ok(Datum::Float64(sxy / denom))
                                }
                            }
                            AggFunc::RegrSlope => {
                                if sxx == 0.0 {
                                    Ok(Datum::Null)
                                } else {
                                    Ok(Datum::Float64(sxy / sxx))
                                }
                            }
                            AggFunc::RegrIntercept => {
                                if sxx == 0.0 {
                                    Ok(Datum::Null)
                                } else {
                                    Ok(Datum::Float64((sxy / sxx).mul_add(-avg_x, avg_y)))
                                }
                            }
                            AggFunc::RegrR2 => {
                                if syy == 0.0 {
                                    Ok(Datum::Null)
                                } else {
                                    Ok(Datum::Float64((sxy * sxy) / (sxx * syy)))
                                }
                            }
                            _ => Ok(Datum::Null),
                        }
                    }
                }
            }
            // ── Ordered-set aggregates ──
            AggFunc::Mode => {
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "MODE requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
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
            AggFunc::PercentileCont(frac) => {
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "PERCENTILE_CONT requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                let mut floats: Vec<f64> = vals.iter().filter_map(falcon_common::datum::Datum::as_f64).collect();
                if floats.is_empty() {
                    return Ok(Datum::Null);
                }
                floats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = floats.len();
                let idx = frac * (n - 1) as f64;
                let lo = idx.floor() as usize;
                let hi = idx.ceil() as usize;
                let result = if lo == hi {
                    floats[lo]
                } else {
                    (floats[hi] - floats[lo]).mul_add(idx - lo as f64, floats[lo])
                };
                Ok(Datum::Float64(result))
            }
            AggFunc::PercentileDisc(frac) => {
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "PERCENTILE_DISC requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                let mut floats: Vec<f64> = vals.iter().filter_map(falcon_common::datum::Datum::as_f64).collect();
                if floats.is_empty() {
                    return Ok(Datum::Null);
                }
                floats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = (frac * floats.len() as f64).ceil() as usize;
                let idx = idx.min(floats.len()).saturating_sub(1);
                Ok(Datum::Float64(floats[idx]))
            }
            // ── Bit aggregates ──
            AggFunc::BitAndAgg => {
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "BIT_AND requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                if vals.is_empty() {
                    return Ok(Datum::Null);
                }
                let mut result: Option<i64> = None;
                for v in &vals {
                    if let Some(i) = v.as_i64() {
                        result = Some(result.map_or(i, |r| r & i));
                    }
                }
                Ok(result.map_or(Datum::Null, Datum::Int64))
            }
            AggFunc::BitOrAgg => {
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "BIT_OR requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                if vals.is_empty() {
                    return Ok(Datum::Null);
                }
                let mut result: i64 = 0;
                for v in &vals {
                    if let Some(i) = v.as_i64() {
                        result |= i;
                    }
                }
                Ok(Datum::Int64(result))
            }
            AggFunc::BitXorAgg => {
                let expr = agg_expr.ok_or_else(|| FalconError::Execution(ExecutionError::TypeError(
                    "BIT_XOR requires an argument".into(),
                )))?;
                let vals = eval_all(expr)?;
                if vals.is_empty() {
                    return Ok(Datum::Null);
                }
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
