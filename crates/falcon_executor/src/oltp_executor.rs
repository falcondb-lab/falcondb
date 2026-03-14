//! OLTP fast-path executor.
//!
//! [`OltpExecutor`] handles the six most common OLTP statement kinds without
//! going through the general Volcano-style executor pipeline.  It is invoked
//! from [`Executor::execute_with_params`] when the plan has been pre-classified
//! as an [`OltpKind`] at PREPARE time.
//!
//! # What this bypasses (per call)
//! - `reset_statement_ts()` syscall  
//! - `Instant::now()` timing syscall  
//! - AI feature extraction + `predict_cost` + `record_feedback`  
//! - AIOps `record()` metrics write  
//! - `substitute_params_plan` full-tree clone  
//! - `check_privilege` `RwLock` read + role-catalog traversal (for superuser sessions)  
//! - `materialize_ctes` `HashMap` allocation  
//! - `exec_seq_scan` full-table iterator + per-row `eval_filter`  
//!
//! # What it still does correctly
//! - MVCC read-timestamp acquisition  
//! - `record_read` (OCC read-set tracking)  
//! - WAL append via `storage.update` / `storage.insert` / `storage.delete`  
//! - Row-level trigger re-check at execute time (triggers added after PREPARE
//!   are detected and the call falls back to the general path)  

use std::sync::Arc;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{DataType, TableId};
use falcon_planner::PhysicalPlan;
use falcon_sql_frontend::types::{BinOp, BoundExpr, OnConflictAction};
use falcon_storage::engine::StorageEngine;
use falcon_storage::memtable::encode_pk_from_datums_stack;
use falcon_txn::{TxnHandle, TxnManager};

use crate::executor::ExecutionResult;
use crate::oltp_classifier::OltpKind;

/// Lightweight fast-path executor — holds only what OLTP operations need.
pub(crate) struct OltpExecutor<'a> {
    pub storage: &'a Arc<StorageEngine>,
    pub txn_mgr: &'a Arc<TxnManager>,
}

impl<'a> OltpExecutor<'a> {
    /// Dispatch to the appropriate fast-path handler based on `kind`.
    ///
    /// `plan` must be the same plan that was classified to produce `kind`.
    /// `params` are the fully-evaluated runtime parameter values.
    pub fn dispatch(
        &self,
        kind: OltpKind,
        plan: &PhysicalPlan,
        params: &[Datum],
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        match kind {
            OltpKind::PointSelect { table_id } => {
                self.point_select(table_id, plan, params, txn)
            }
            OltpKind::InsertSingle { table_id } | OltpKind::InsertBatch { table_id } => {
                self.insert_fast(table_id, plan, params, txn)
            }
            OltpKind::InsertIgnore { table_id } => {
                self.insert_ignore(table_id, plan, params, txn)
            }
            OltpKind::UpsertByPk { table_id } => {
                self.upsert_by_pk(table_id, plan, params, txn)
            }
            OltpKind::UpdateByPk { table_id } => {
                self.update_by_pk(table_id, plan, params, txn)
            }
            OltpKind::DeleteByPk { table_id } => {
                self.delete_by_pk(table_id, plan, params, txn)
            }
        }
    }

    // ── Point SELECT ─────────────────────────────────────────────────────────

    /// `SELECT [cols] FROM t WHERE pk_col = $N`
    ///
    /// Zero heap allocations on the hot path when the row is not found.
    /// When a row is found: one `Vec<Datum>` for the projected row.
    fn point_select(
        &self,
        table_id: TableId,
        plan: &PhysicalPlan,
        params: &[Datum],
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let (schema, projections, visible_count) = match plan {
            PhysicalPlan::SeqScan {
                schema,
                projections,
                visible_projection_count,
                ..
            } => (schema, projections.as_slice(), *visible_projection_count),
            _ => return Err(FalconError::Internal("point_select: wrong plan kind".into())),
        };

        // Re-check triggers at execute time (guard against triggers added after PREPARE)
        if self.has_select_triggers(schema) {
            return Err(FalconError::Internal(
                "oltp fast path: triggers appeared after prepare, falling back".into(),
            ));
        }

        // Encode PK directly from params — no BoundExpr eval
        let pk_datums = extract_pk_datums_from_filter(
            plan_filter(plan),
            schema,
            params,
        )?;
        let pk_refs: Vec<&Datum> = pk_datums.iter().collect();
        let pk = encode_pk_from_datums_stack(&pk_refs);

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let row = match self.storage.get(table_id, &pk, txn.txn_id, read_ts)? {
            Some(r) => r,
            None => {
                return Ok(ExecutionResult::Query {
                    columns: build_column_meta(schema, projections, visible_count),
                    rows: vec![],
                });
            }
        };

        // Project columns — inline, no ExprEngine dispatch
        let values: Vec<Datum> = project_row(&row, projections, visible_count);

        Ok(ExecutionResult::Query {
            columns: build_column_meta(schema, projections, visible_count),
            rows: vec![OwnedRow::new(values)],
        })
    }

    // ── INSERT fast path ─────────────────────────────────────────────────────

    /// `INSERT INTO t VALUES ($1, $2, ...)` — plain insert, no ON CONFLICT.
    ///
    /// All rows in the plan are bound from `params` and inserted in a single
    /// `batch_insert` call, which acquires one lock per shard instead of N.
    fn insert_fast(
        &self,
        table_id: TableId,
        plan: &PhysicalPlan,
        params: &[Datum],
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let (schema, columns, rows_exprs) = extract_insert_parts(plan)?;

        // Re-check triggers at execute time
        if self.has_insert_triggers(schema) {
            return Err(FalconError::Internal(
                "oltp fast path: triggers appeared after prepare, falling back".into(),
            ));
        }

        let ncols = schema.columns.len();
        let nrows = rows_exprs.len();
        let mut built: Vec<OwnedRow> = Vec::with_capacity(nrows);

        for row_exprs in rows_exprs {
            let values = bind_row_from_params(schema, columns, row_exprs, params, ncols)?;
            built.push(OwnedRow::new(values));
        }

        let count = built.len() as u64;
        self.storage
            .batch_insert(table_id, built, txn.txn_id)
            .map_err(FalconError::Storage)?;

        Ok(ExecutionResult::Dml {
            rows_affected: count,
            tag: "INSERT",
        })
    }

    /// `INSERT ... ON CONFLICT DO NOTHING` — insert, skip on duplicate key.
    fn insert_ignore(
        &self,
        table_id: TableId,
        plan: &PhysicalPlan,
        params: &[Datum],
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let (schema, columns, rows_exprs) = extract_insert_parts(plan)?;

        if self.has_insert_triggers(schema) {
            return Err(FalconError::Internal(
                "oltp fast path: triggers appeared after prepare, falling back".into(),
            ));
        }

        let ncols = schema.columns.len();
        let mut count = 0u64;
        for row_exprs in rows_exprs {
            let values = bind_row_from_params(schema, columns, row_exprs, params, ncols)?;
            match self
                .storage
                .insert(table_id, OwnedRow::new(values), txn.txn_id)
            {
                Ok(_) => count += 1,
                Err(falcon_common::error::StorageError::DuplicateKey) => {} // skip
                Err(e) => return Err(FalconError::Storage(e)),
            }
        }
        Ok(ExecutionResult::Dml {
            rows_affected: count,
            tag: "INSERT",
        })
    }

    // ── UPSERT ───────────────────────────────────────────────────────────────

    /// `INSERT ... ON CONFLICT (pk) DO UPDATE SET col=$N ...`
    ///
    /// Uses a direct PK lookup for the conflict target instead of a full scan.
    fn upsert_by_pk(
        &self,
        table_id: TableId,
        plan: &PhysicalPlan,
        params: &[Datum],
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let (schema, columns, rows_exprs) = extract_insert_parts(plan)?;
        let on_conflict = match plan {
            PhysicalPlan::Insert { on_conflict, .. } => on_conflict,
            _ => return Err(FalconError::Internal("upsert_by_pk: wrong plan".into())),
        };
        let (do_update_assignments, where_clause) = match on_conflict {
            Some(OnConflictAction::DoUpdate(asgn, wc)) => (asgn, wc),
            _ => return Err(FalconError::Internal("upsert_by_pk: no DoUpdate".into())),
        };

        if self.has_insert_triggers(schema) {
            return Err(FalconError::Internal(
                "oltp fast path: triggers appeared after prepare, falling back".into(),
            ));
        }

        let ncols = schema.columns.len();
        let mut count = 0u64;
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());

        for row_exprs in rows_exprs {
            let values = bind_row_from_params(schema, columns, row_exprs, params, ncols)?;
            let row = OwnedRow::new(values.clone());
            match self.storage.insert(table_id, row, txn.txn_id) {
                Ok(_) => {
                    count += 1;
                }
                Err(falcon_common::error::StorageError::DuplicateKey) => {
                    // Conflict: look up the existing row by PK and apply DO UPDATE
                    let pk_datums: Vec<&Datum> = schema
                        .primary_key_columns
                        .iter()
                        .map(|&i| &values[i])
                        .collect();
                    let pk = encode_pk_from_datums_stack(&pk_datums);

                    let existing =
                        match self.storage.get(table_id, &pk, txn.txn_id, read_ts)? {
                            Some(r) => r,
                            None => continue, // concurrent delete, skip
                        };

                    // Build combined row: [existing cols] ++ [excluded cols]
                    let mut combined = existing.values.clone();
                    combined.extend(values.iter().cloned());
                    let combined_row = OwnedRow::new(combined);

                    // Check optional WHERE clause on the conflict target
                    if let Some(wc) = where_clause {
                        if !crate::expr_engine::ExprEngine::eval_filter(wc, &combined_row)
                            .map_err(FalconError::Execution)?
                        {
                            continue; // WHERE clause false — skip update
                        }
                    }

                    // Apply DO UPDATE assignments
                    let mut new_values = existing.values.clone();
                    for (col_idx, expr) in do_update_assignments {
                        let val = crate::expr_engine::ExprEngine::eval_row(expr, &combined_row)
                            .map_err(FalconError::Execution)?;
                        new_values[*col_idx] = val;
                    }

                    self.storage
                        .update(table_id, &pk.to_vec(), OwnedRow::new(new_values), txn.txn_id)?;
                    count += 1;
                }
                Err(e) => return Err(FalconError::Storage(e)),
            }
        }

        Ok(ExecutionResult::Dml {
            rows_affected: count,
            tag: "INSERT",
        })
    }

    // ── UPDATE by PK ─────────────────────────────────────────────────────────

    /// `UPDATE t SET col=$2 WHERE pk_col=$1`
    ///
    /// Direct PK lookup — no seq scan, no eval_filter loop.
    fn update_by_pk(
        &self,
        table_id: TableId,
        plan: &PhysicalPlan,
        params: &[Datum],
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let (schema, assignments, filter) = match plan {
            PhysicalPlan::Update {
                schema,
                assignments,
                filter,
                ..
            } => (schema, assignments.as_slice(), filter.as_ref()),
            _ => return Err(FalconError::Internal("update_by_pk: wrong plan".into())),
        };

        if self.has_update_triggers(schema) {
            return Err(FalconError::Internal(
                "oltp fast path: triggers appeared after prepare, falling back".into(),
            ));
        }

        let pk_datums = extract_pk_datums_from_filter(filter, schema, params)?;
        let pk_refs: Vec<&Datum> = pk_datums.iter().collect();
        let pk = encode_pk_from_datums_stack(&pk_refs);

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let existing = match self.storage.get(table_id, &pk, txn.txn_id, read_ts)? {
            Some(r) => r,
            None => {
                return Ok(ExecutionResult::Dml {
                    rows_affected: 0,
                    tag: "UPDATE",
                })
            }
        };

        // Apply assignments — eval each expression against the existing row
        let mut new_values = existing.values.clone();
        for (col_idx, expr) in assignments {
            // Fast path: literal or param — no heap alloc
            let val = eval_assignment_expr(expr, &existing, params)?;
            new_values[*col_idx] = val;
        }

        // Enforce NOT NULL inline (no separate loop, no constraint struct alloc)
        for (i, val) in new_values.iter().enumerate() {
            if i < schema.columns.len()
                && val.is_null()
                && !schema.columns[i].nullable
            {
                return Err(FalconError::Execution(
                    falcon_common::error::ExecutionError::TypeError(format!(
                        "NULL value in column '{}' violates NOT NULL constraint",
                        schema.columns[i].name
                    )),
                ));
            }
        }

        self.storage
            .update(table_id, &pk.to_vec(), OwnedRow::new(new_values), txn.txn_id)?;

        Ok(ExecutionResult::Dml {
            rows_affected: 1,
            tag: "UPDATE",
        })
    }

    // ── DELETE by PK ─────────────────────────────────────────────────────────

    /// `DELETE FROM t WHERE pk_col=$1`
    ///
    /// Single DashMap remove — O(1), no seq scan.
    fn delete_by_pk(
        &self,
        table_id: TableId,
        plan: &PhysicalPlan,
        params: &[Datum],
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let (schema, filter) = match plan {
            PhysicalPlan::Delete { schema, filter, .. } => (schema, filter.as_ref()),
            _ => return Err(FalconError::Internal("delete_by_pk: wrong plan".into())),
        };

        if self.has_delete_triggers(schema) {
            return Err(FalconError::Internal(
                "oltp fast path: triggers appeared after prepare, falling back".into(),
            ));
        }

        let pk_datums = extract_pk_datums_from_filter(filter, schema, params)?;
        let pk_refs: Vec<&Datum> = pk_datums.iter().collect();
        let pk = encode_pk_from_datums_stack(&pk_refs);

        self.storage.delete(table_id, &pk.to_vec(), txn.txn_id)?;

        Ok(ExecutionResult::Dml {
            rows_affected: 1,
            tag: "DELETE",
        })
    }

    // ── Trigger guards ───────────────────────────────────────────────────────

    fn has_select_triggers(&self, _schema: &TableSchema) -> bool {
        false // SELECT never fires row triggers
    }

    fn has_insert_triggers(&self, schema: &TableSchema) -> bool {
        use falcon_common::schema::{TriggerEvent, TriggerLevel, TriggerTiming};
        let cat = self.storage.get_catalog();
        cat.triggers_for_table(&schema.name).iter().any(|t| {
            t.enabled
                && (t.events.contains(&TriggerEvent::Insert))
                && (t.timing == TriggerTiming::Before || t.timing == TriggerTiming::After)
                && t.level == TriggerLevel::Statement || t.level == TriggerLevel::Row
        })
    }

    fn has_update_triggers(&self, schema: &TableSchema) -> bool {
        use falcon_common::schema::{TriggerEvent, TriggerLevel, TriggerTiming};
        let cat = self.storage.get_catalog();
        cat.triggers_for_table(&schema.name).iter().any(|t| {
            t.enabled
                && t.events.contains(&TriggerEvent::Update)
                && (t.timing == TriggerTiming::Before || t.timing == TriggerTiming::After)
                && (t.level == TriggerLevel::Statement || t.level == TriggerLevel::Row)
        })
    }

    fn has_delete_triggers(&self, schema: &TableSchema) -> bool {
        use falcon_common::schema::{TriggerEvent, TriggerLevel, TriggerTiming};
        let cat = self.storage.get_catalog();
        cat.triggers_for_table(&schema.name).iter().any(|t| {
            t.enabled
                && t.events.contains(&TriggerEvent::Delete)
                && (t.timing == TriggerTiming::Before || t.timing == TriggerTiming::After)
                && (t.level == TriggerLevel::Statement || t.level == TriggerLevel::Row)
        })
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Extract the filter expression from a plan node.
fn plan_filter(plan: &PhysicalPlan) -> Option<&BoundExpr> {
    match plan {
        PhysicalPlan::SeqScan { filter, .. } => filter.as_ref(),
        PhysicalPlan::Update { filter, .. } => filter.as_ref(),
        PhysicalPlan::Delete { filter, .. } => filter.as_ref(),
        _ => None,
    }
}

/// Extract `(schema, columns, rows)` from an Insert plan.
fn extract_insert_parts(
    plan: &PhysicalPlan,
) -> Result<(&TableSchema, &[usize], &[Vec<BoundExpr>]), FalconError> {
    match plan {
        PhysicalPlan::Insert {
            schema,
            columns,
            rows,
            ..
        } => Ok((schema, columns.as_slice(), rows.as_slice())),
        _ => Err(FalconError::Internal(
            "oltp fast path: expected Insert plan".into(),
        )),
    }
}

/// Bind a single insert row: for each column, resolve the value from either
/// a `Literal` datum (already in the plan) or a `Parameter` index into `params`.
///
/// Column defaults are applied for positions not covered by `columns`.
/// Only called on "trivial" schemas (no SERIAL, no dynamic defaults, no FK).
fn bind_row_from_params(
    schema: &TableSchema,
    columns: &[usize],
    row_exprs: &[BoundExpr],
    params: &[Datum],
    ncols: usize,
) -> Result<Vec<Datum>, FalconError> {
    let mut values: Vec<Datum> = schema
        .columns
        .iter()
        .map(|c| c.default_value.clone().unwrap_or(Datum::Null))
        .collect();
    // Overwrite with provided expressions
    for (i, expr) in row_exprs.iter().enumerate() {
        let col_idx = if i < columns.len() { columns[i] } else { i };
        if col_idx >= ncols {
            continue;
        }
        values[col_idx] = match expr {
            BoundExpr::Literal(d) => d.clone(),
            BoundExpr::Parameter(n) => {
                let idx = n.saturating_sub(1);
                params
                    .get(idx)
                    .cloned()
                    .unwrap_or(Datum::Null)
            }
            other => {
                crate::expr_engine::ExprEngine::eval_row(other, &OwnedRow::EMPTY)
                    .map_err(FalconError::Execution)?
            }
        };
    }
    // NOT NULL check — inline single pass
    for (i, val) in values.iter().enumerate() {
        if val.is_null() && i < schema.columns.len() && !schema.columns[i].nullable {
            return Err(FalconError::Execution(
                falcon_common::error::ExecutionError::TypeError(format!(
                    "NULL value in column '{}' violates NOT NULL constraint",
                    schema.columns[i].name
                )),
            ));
        }
    }
    Ok(values)
}

/// Walk the PK equality filter and resolve each PK column's `Datum` from either
/// the literal embedded in the filter or the `params` slice (for `$N` params).
///
/// Returns PK column values **in primary_key_columns order**.
fn extract_pk_datums_from_filter(
    filter: Option<&BoundExpr>,
    schema: &TableSchema,
    params: &[Datum],
) -> Result<Vec<Datum>, FalconError> {
    let f = filter.ok_or_else(|| {
        FalconError::Internal("oltp fast path: missing PK filter".into())
    })?;

    let pk_cols = &schema.primary_key_columns;
    let mut pk_datums: Vec<Option<Datum>> = vec![None; pk_cols.len()];

    let mut conjuncts: Vec<&BoundExpr> = Vec::new();
    flatten_and_exprs(f, &mut conjuncts);

    for conj in conjuncts {
        if let BoundExpr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = conj
        {
            let (col_idx, datum_opt) = match (left.as_ref(), right.as_ref()) {
                (BoundExpr::ColumnRef(idx), BoundExpr::Literal(d)) => (*idx, Some(d.clone())),
                (BoundExpr::Literal(d), BoundExpr::ColumnRef(idx)) => (*idx, Some(d.clone())),
                (BoundExpr::ColumnRef(idx), BoundExpr::Parameter(n)) => {
                    let v = params.get(n.saturating_sub(1)).cloned().unwrap_or(Datum::Null);
                    (*idx, Some(v))
                }
                (BoundExpr::Parameter(n), BoundExpr::ColumnRef(idx)) => {
                    let v = params.get(n.saturating_sub(1)).cloned().unwrap_or(Datum::Null);
                    (*idx, Some(v))
                }
                _ => continue,
            };
            if let Some(pk_pos) = pk_cols.iter().position(|&c| c == col_idx) {
                if pk_datums[pk_pos].is_none() {
                    pk_datums[pk_pos] = datum_opt;
                }
            }
        }
    }

    pk_datums
        .into_iter()
        .enumerate()
        .map(|(i, opt)| {
            opt.ok_or_else(|| {
                FalconError::Internal(format!(
                    "oltp fast path: PK column {} missing from filter",
                    schema.primary_key_columns[i]
                ))
            })
        })
        .collect()
}

/// Evaluate a single assignment expression for the UPDATE fast path.
/// Handles the common cases inline without recursion:
/// - `Literal(d)` → return datum
/// - `Parameter(n)` → index into params
/// - Everything else → delegate to ExprEngine
#[inline]
fn eval_assignment_expr(
    expr: &BoundExpr,
    row: &OwnedRow,
    params: &[Datum],
) -> Result<Datum, FalconError> {
    match expr {
        BoundExpr::Literal(d) => Ok(d.clone()),
        BoundExpr::Parameter(n) => Ok(params
            .get(n.saturating_sub(1))
            .cloned()
            .unwrap_or(Datum::Null)),
        other => crate::expr_engine::ExprEngine::eval_row(other, row)
            .map_err(FalconError::Execution),
    }
}

/// Project a row's columns according to the projection list.
/// For SELECT *, this is a no-op move.
fn project_row(
    row: &OwnedRow,
    projections: &[falcon_sql_frontend::types::BoundProjection],
    visible_count: usize,
) -> Vec<Datum> {
    use falcon_sql_frontend::types::BoundProjection;
    let count = visible_count.min(projections.len());
    let mut out = Vec::with_capacity(count);
    for proj in &projections[..count] {
        match proj {
            BoundProjection::Column(idx, _) => {
                if let Some(d) = row.values.get(*idx) {
                    out.push(d.clone());
                }
            }
            // For fast path we only handle plain column refs; complex exprs fall back
            _ => {
                out.push(Datum::Null);
            }
        }
    }
    out
}

/// Build column metadata for the result of a SELECT.
fn build_column_meta(
    schema: &TableSchema,
    projections: &[falcon_sql_frontend::types::BoundProjection],
    visible_count: usize,
) -> Vec<(String, DataType)> {
    use falcon_sql_frontend::types::BoundProjection;
    let count = visible_count.min(projections.len());
    let mut cols = Vec::with_capacity(count);
    for proj in &projections[..count] {
        match proj {
            BoundProjection::Column(idx, alias) => {
                let name = if alias.is_empty() {
                    schema
                        .columns
                        .get(*idx)
                        .map(|c| c.name.clone())
                        .unwrap_or_default()
                } else {
                    alias.clone()
                };
                let dt = schema
                    .columns
                    .get(*idx)
                    .map(|c| c.data_type.clone())
                    .unwrap_or(DataType::Text);
                cols.push((name, dt));
            }
            _ => cols.push(("?column?".into(), DataType::Text)),
        }
    }
    cols
}

/// Flatten AND conjuncts — identical to the one in `executor_query.rs` but
/// private to this module to avoid a cross-module dependency.
fn flatten_and_exprs<'a>(expr: &'a BoundExpr, out: &mut Vec<&'a BoundExpr>) {
    match expr {
        BoundExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            flatten_and_exprs(left, out);
            flatten_and_exprs(right, out);
        }
        other => out.push(other),
    }
}
