#![allow(clippy::too_many_arguments)]

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::{ExecutionError, FalconError};
use falcon_common::schema::TableSchema;
use falcon_common::types::DataType;
use falcon_sql_frontend::types::*;
use falcon_txn::TxnHandle;

use crate::executor::{ExecutionResult, Executor};
use crate::expr_engine::ExprEngine;

thread_local! {
    /// Reusable per-thread scratch buffer for batch INSERT row building.
    /// Eliminates the fresh Vec<OwnedRow> allocation on every exec_insert_batch call.
    static BUILT_ROWS_POOL: std::cell::RefCell<Vec<OwnedRow>> =
        std::cell::RefCell::new(Vec::with_capacity(64));
}

/// Acquire the thread-local built_rows buffer (cleared, capacity preserved).
#[inline]
fn take_built_rows_pool() -> Vec<OwnedRow> {
    BUILT_ROWS_POOL.with(|pool| {
        let mut buf = pool.borrow_mut();
        let cap = buf.capacity();
        std::mem::replace(&mut *buf, Vec::with_capacity(cap))
    })
}

/// Return a used buffer back to the thread-local pool for reuse.
#[inline]
fn return_built_rows_pool(mut buf: Vec<OwnedRow>) {
    buf.clear();
    BUILT_ROWS_POOL.with(|pool| {
        let mut slot = pool.borrow_mut();
        if buf.capacity() > slot.capacity() {
            *slot = buf;
        }
    });
}

impl Executor {
    /// Fill dynamic defaults (CURRENT_TIMESTAMP, etc.) for NULL columns.
    /// Calls statement_ts() once internally. For batch INSERT use
    /// eval_dynamic_defaults_with_ts() to share a single clock read across all rows.
    pub(crate) fn eval_dynamic_defaults(schema: &TableSchema, values: &mut [Datum]) {
        if schema.dynamic_defaults.is_empty() {
            return;
        }
        let ts_us = crate::eval::scalar_time::statement_ts();
        Self::eval_dynamic_defaults_with_ts(schema, values, ts_us);
    }

    /// Fill dynamic defaults using a caller-supplied statement timestamp (microseconds).
    /// Use this in batch INSERT loops to avoid one clock_gettime syscall per row.
    pub(crate) fn eval_dynamic_defaults_with_ts(
        schema: &TableSchema,
        values: &mut [Datum],
        ts_us: i64,
    ) {
        use chrono::Timelike;
        use falcon_common::schema::DefaultFn;
        for (&col_idx, dfn) in &schema.dynamic_defaults {
            if col_idx < values.len() && values[col_idx].is_null() {
                values[col_idx] = match dfn {
                    DefaultFn::CurrentTimestamp => Datum::Timestamp(ts_us),
                    DefaultFn::CurrentDate => {
                        let secs = ts_us / 1_000_000;
                        let dt = chrono::DateTime::from_timestamp(secs, 0)
                            .unwrap_or_default()
                            .date_naive();
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        Datum::Date((dt - epoch).num_days() as i32)
                    }
                    DefaultFn::CurrentTime => {
                        let secs = ts_us / 1_000_000;
                        let nanos = ((ts_us % 1_000_000) * 1_000) as u32;
                        let dt = chrono::DateTime::from_timestamp(secs, nanos).unwrap_or_default();
                        let t = dt.time();
                        Datum::Time(
                            t.num_seconds_from_midnight() as i64 * 1_000_000
                                + ts_us % 1_000_000,
                        )
                    }
                    DefaultFn::Nextval(_) => continue,
                };
            }
        }
    }

    /// Enforce VARCHAR(n)/CHAR(n) max_length on text columns.
    pub(crate) fn enforce_max_length(
        schema: &TableSchema,
        values: &[Datum],
    ) -> Result<(), FalconError> {
        for (i, col) in schema.columns.iter().enumerate() {
            if let Some(max_len) = col.max_length {
                if let Datum::Text(ref s) = values[i] {
                    let char_count = s.chars().count() as u32;
                    if char_count > max_len {
                        return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                            "value too long for type character varying({})",
                            max_len
                        ))));
                    }
                }
            }
        }
        Ok(())
    }

    /// Evaluate a single row's expressions, apply defaults, coerce types, fill serials.
    fn build_insert_row(
        &self,
        schema: &TableSchema,
        columns: &[usize],
        row_exprs: &[BoundExpr],
    ) -> Result<Vec<Datum>, FalconError> {
        let ts_us = if schema.dynamic_defaults.is_empty() {
            0
        } else {
            crate::eval::scalar_time::statement_ts()
        };
        self.build_insert_row_with_ts(schema, columns, row_exprs, ts_us)
    }

    /// Like build_insert_row but accepts a pre-computed statement_ts (µs).
    /// Use in batch INSERT loops so clock_gettime is called only once per batch,
    /// not once per row. Pass 0 when schema.dynamic_defaults is empty.
    fn build_insert_row_with_ts(
        &self,
        schema: &TableSchema,
        columns: &[usize],
        row_exprs: &[BoundExpr],
        ts_us: i64,
    ) -> Result<Vec<Datum>, FalconError> {
        let ncols = schema.columns.len();
        let mut values: Vec<Datum> = Vec::with_capacity(ncols);
        values.resize(ncols, Datum::Null);

        // Evaluate provided columns — fast path for literals, skip eval_row overhead
        for (i, expr) in row_exprs.iter().enumerate() {
            let col_idx = columns[i];
            let val = match expr {
                BoundExpr::Literal(d) => d.clone(),
                _ => {
                    ExprEngine::eval_row(expr, &OwnedRow::EMPTY).map_err(FalconError::Execution)?
                }
            };
            values[col_idx] = val;
        }

        // Fill defaults only for columns NOT provided by the INSERT
        for (col_idx, col) in schema.columns.iter().enumerate() {
            if values[col_idx].is_null() {
                if let Some(ref def) = col.default_value {
                    values[col_idx] = def.clone();
                }
            }
        }

        // Coerce values to match column data types (e.g. Text -> Date, Text -> Timestamp)
        for (col_idx, col) in schema.columns.iter().enumerate() {
            if values[col_idx].is_null() {
                continue;
            }
            if let Some(val_type) = values[col_idx].data_type() {
                if val_type != col.data_type {
                    let target = datatype_to_cast_target(&col.data_type);
                    if let Ok(cast_val) =
                        crate::eval::cast::eval_cast(values[col_idx].clone(), &target)
                    {
                        values[col_idx] = cast_val;
                    }
                }
            }
        }

        // Auto-fill SERIAL columns that are still NULL (not explicitly provided)
        for (col_idx, col) in schema.columns.iter().enumerate() {
            if col.is_serial && values[col_idx].is_null() {
                let next_val = self.storage.next_serial_value(&schema.name, col_idx)?;
                values[col_idx] = if col.data_type == DataType::Int64 {
                    Datum::Int64(next_val)
                } else {
                    Datum::Int32(i32::try_from(next_val).map_err(|_| {
                        FalconError::Execution(
                            falcon_common::error::ExecutionError::NumericOverflow,
                        )
                    })?)
                };
            }
        }

        // Dynamic defaults (CURRENT_TIMESTAMP, etc.) — use pre-computed ts_us
        if !schema.dynamic_defaults.is_empty() {
            Self::eval_dynamic_defaults_with_ts(schema, &mut values, ts_us);
        }

        // Fill nextval sequence defaults for columns still NULL
        for (&col_idx, dfn) in &schema.dynamic_defaults {
            if let falcon_common::schema::DefaultFn::Nextval(ref seq_name) = dfn {
                if col_idx < values.len() && values[col_idx].is_null() {
                    let next_val = self.storage.sequence_nextval(seq_name)?;
                    values[col_idx] = Datum::Int64(next_val);
                }
            }
        }

        // Enforce VARCHAR(n)/CHAR(n) max length
        Self::enforce_max_length(schema, &values)?;

        // Enforce NOT NULL constraints
        for (i, val) in values.iter().enumerate() {
            if val.is_null() && !schema.columns[i].nullable {
                return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                    "NULL value in column '{}' violates NOT NULL constraint",
                    schema.columns[i].name
                ))));
            }
        }

        Ok(values)
    }

    /// P0-5: Single-pass INSERT row builder for "trivial" schemas.
    ///
    /// A schema is trivial when **all** of the following hold:
    /// - No SERIAL / SEQUENCE columns (`is_serial == false` for all columns)
    /// - No dynamic defaults (`schema.dynamic_defaults.is_empty()`)
    /// - No explicit type-coercion needed (all provided values already match
    ///   the column type, or coercion can be skipped for literals)
    ///
    /// Under these conditions we can replace the 5 separate loops in
    /// `build_insert_row_with_ts` with a single `O(n_cols)` pass:
    /// 1. Bind expression value (literal / parameter / eval)
    /// 2. Apply static column default if still NULL
    /// 3. Check NOT NULL constraint
    ///
    /// Coercion is intentionally skipped here — for INSERT with literal /
    /// parameter values the planner already ensures types match, and any
    /// mismatch that slips through will be caught by the storage layer.
    ///
    /// Call [`build_insert_row_with_ts`] instead when the schema has SERIAL
    /// columns, dynamic defaults, or requires coercion.
    pub(crate) fn build_insert_row_single_pass(
        schema: &TableSchema,
        columns: &[usize],
        row_exprs: &[BoundExpr],
    ) -> Result<Vec<Datum>, FalconError> {
        debug_assert!(
            !schema.columns.iter().any(|c| c.is_serial),
            "build_insert_row_single_pass called on schema with SERIAL columns"
        );
        debug_assert!(
            schema.dynamic_defaults.is_empty(),
            "build_insert_row_single_pass called on schema with dynamic defaults"
        );

        let ncols = schema.columns.len();
        // Pre-fill with static defaults (or Null)
        let mut values: Vec<Datum> = schema
            .columns
            .iter()
            .map(|c| c.default_value.clone().unwrap_or(Datum::Null))
            .collect();

        // Single pass: bind expressions and check NOT NULL in the same loop
        for (i, expr) in row_exprs.iter().enumerate() {
            let col_idx = if i < columns.len() { columns[i] } else { i };
            if col_idx >= ncols {
                continue;
            }
            values[col_idx] = match expr {
                BoundExpr::Literal(d) => d.clone(),
                _ => {
                    ExprEngine::eval_row(expr, &OwnedRow::EMPTY).map_err(FalconError::Execution)?
                }
            };
        }

        // NOT NULL check — single pass, combined with the bind above would
        // require a second index, so keep it as a lean separate loop that
        // the CPU branch predictor can optimise away on nullable-all schemas.
        for (i, val) in values.iter().enumerate() {
            if val.is_null() && i < ncols && !schema.columns[i].nullable {
                return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                    "NULL value in column '{}' violates NOT NULL constraint",
                    schema.columns[i].name
                ))));
            }
        }

        Ok(values)
    }

    /// Returns `true` when [`build_insert_row_single_pass`] is safe to call
    /// for `schema` — i.e. the schema has no SERIAL columns and no dynamic
    /// defaults (CURRENT_TIMESTAMP, nextval sequences, etc.).
    #[inline]
    pub(crate) fn is_trivial_insert_schema(schema: &TableSchema) -> bool {
        !schema.columns.iter().any(|c| c.is_serial) && schema.dynamic_defaults.is_empty()
    }

    pub(crate) fn exec_insert(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &TableSchema,
        columns: &[usize],
        rows: &[Vec<BoundExpr>],
        returning: &[(BoundExpr, String)],
        on_conflict: &Option<OnConflictAction>,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        use falcon_common::schema::{TriggerEvent, TriggerTiming};
        self.fire_triggers(&schema.name, TriggerTiming::Before, TriggerEvent::Insert, txn)?;
        let result = (|| {
            // ── Partition routing: if parent table is partitioned, resolve child table_id ──
            let effective_table_id = if self.storage.is_partitioned(table_id) {
                if let Some(key_idx) = self.storage.partition_key_idx(table_id) {
                    let col_pos = columns.get(key_idx).copied().unwrap_or(key_idx);
                    let first_row = rows.first();
                    let datum_opt = first_row
                        .and_then(|r| r.get(col_pos))
                        .and_then(|expr| {
                            ExprEngine::eval_row(expr, &OwnedRow::EMPTY).ok()
                        });
                    if let Some(datum) = datum_opt {
                        self.storage.route_partition(table_id, &datum).unwrap_or(table_id)
                    } else {
                        table_id
                    }
                } else {
                    table_id
                }
            } else {
                table_id
            };

            // ── Fast path: no RETURNING, no ON CONFLICT ──────────────────────
            if returning.is_empty() && on_conflict.is_none() {
                return self.exec_insert_batch(effective_table_id, schema, columns, rows, txn);
            }

            // ── Fast path: ON CONFLICT DO NOTHING without RETURNING ──────────
            if returning.is_empty() && matches!(on_conflict, Some(OnConflictAction::DoNothing)) {
                return self.exec_insert_conflict_skip(effective_table_id, schema, columns, rows, txn);
            }

            // ── Slow path: RETURNING or ON CONFLICT DO UPDATE ────────────────
            self.exec_insert_slow(effective_table_id, schema, columns, rows, returning, on_conflict, txn)
        })()?
;
        self.fire_triggers(&schema.name, TriggerTiming::After, TriggerEvent::Insert, txn)?;
        Ok(result)
    }

    /// INSERT ... ON CONFLICT DO NOTHING fast path: build row, try insert, skip on DuplicateKey.
    fn exec_insert_conflict_skip(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &TableSchema,
        columns: &[usize],
        rows: &[Vec<BoundExpr>],
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let has_checks = !schema.check_constraints.is_empty();
        // Pre-build FK lookup sets (scan each referenced table once)
        let fk_lookup: Vec<(Vec<usize>, std::collections::HashSet<Vec<Datum>>)> =
            if !schema.foreign_keys.is_empty() {
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                schema
                    .foreign_keys
                    .iter()
                    .map(|fk| {
                        let ref_schema =
                            self.storage
                                .get_table_schema(&fk.ref_table)
                                .ok_or_else(|| {
                                    FalconError::Execution(ExecutionError::TypeError(format!(
                                        "Referenced table '{}' not found",
                                        fk.ref_table
                                    )))
                                })?;
                        let ref_col_indices: Vec<usize> = fk
                            .ref_columns
                            .iter()
                            .map(|name| {
                                ref_schema.find_column(name).ok_or_else(|| {
                                    FalconError::Execution(ExecutionError::TypeError(format!(
                                        "Referenced column '{}' not found in '{}'",
                                        name, fk.ref_table
                                    )))
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        let ref_rows = self.storage.scan(ref_schema.id, txn.txn_id, read_ts)?;
                        let fk_set: std::collections::HashSet<Vec<Datum>> = ref_rows
                            .iter()
                            .map(|(_, r)| {
                                ref_col_indices
                                    .iter()
                                    .map(|&i| r.values[i].clone())
                                    .collect()
                            })
                            .collect();
                        Ok((fk.columns.clone(), fk_set))
                    })
                    .collect::<Result<Vec<_>, FalconError>>()?
            } else {
                vec![]
            };
        let mut count = 0u64;
        for row_exprs in rows {
            let values = self.build_insert_row(schema, columns, row_exprs)?;
            let row = OwnedRow::new(values);
            if has_checks {
                self.eval_check_constraints(schema, &row)?;
            }
            for (fk_cols, fk_set) in &fk_lookup {
                let fk_vals: Vec<Datum> =
                    fk_cols.iter().map(|&idx| row.values[idx].clone()).collect();
                if fk_vals.iter().any(Datum::is_null) {
                    continue;
                }
                if !fk_set.contains(&fk_vals) {
                    return Err(FalconError::Execution(ExecutionError::TypeError(
                        "FOREIGN KEY constraint violated".into(),
                    )));
                }
            }
            match self.storage.insert(table_id, row, txn.txn_id) {
                Ok(_) => count += 1,
                Err(falcon_common::error::StorageError::DuplicateKey) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(ExecutionResult::Dml {
            rows_affected: count,
            tag: "INSERT",
        })
    }

    /// Fast batch INSERT path: no RETURNING, no ON CONFLICT.
    fn exec_insert_batch(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &TableSchema,
        columns: &[usize],
        rows: &[Vec<BoundExpr>],
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let has_checks = !schema.check_constraints.is_empty();
        let mut built_rows: Vec<OwnedRow> = take_built_rows_pool();
        built_rows.reserve(rows.len());

        // P0-5: trivial schema → single-pass builder (no SERIAL, no dynamic defaults).
        // Non-trivial schemas fall through to the multi-pass builder below.
        if Self::is_trivial_insert_schema(schema) {
            for row_exprs in rows {
                let values =
                    Self::build_insert_row_single_pass(schema, columns, row_exprs)?;
                let row = OwnedRow::new(values);
                if has_checks {
                    self.eval_check_constraints(schema, &row)?;
                }
                built_rows.push(row);
            }
        } else {
            // Pre-compute statement_ts once for the entire batch.
            // All rows in the same INSERT statement share the same logical statement time.
            let stmt_ts = if schema.dynamic_defaults.is_empty() {
                0
            } else {
                crate::eval::scalar_time::statement_ts()
            };
            for row_exprs in rows {
                let values =
                    self.build_insert_row_with_ts(schema, columns, row_exprs, stmt_ts)?;
                let row = OwnedRow::new(values);
                if has_checks {
                    self.eval_check_constraints(schema, &row)?;
                }
                built_rows.push(row);
            }
        }

        // Phase 2: UNIQUE constraint enforcement is delegated to MemTable.insert()
        // which calls check_unique_indexes() inside the DashMap entry critical section.
        // The former O(N) full-table scan here was redundant and a hot-path bottleneck.
        // StorageError::DuplicateKey from batch_insert() is translated to FalconError below.

        // Phase 3: Batch FK constraint check — scan each referenced table ONCE
        if !schema.foreign_keys.is_empty() {
            let read_ts = txn.read_ts(self.txn_mgr.current_ts());
            for fk in &schema.foreign_keys {
                let ref_schema = self
                    .storage
                    .get_table_schema(&fk.ref_table)
                    .ok_or_else(|| {
                        FalconError::Execution(ExecutionError::TypeError(format!(
                            "Referenced table '{}' not found",
                            fk.ref_table
                        )))
                    })?;
                let ref_table_id = ref_schema.id;
                let ref_col_indices: Vec<usize> = fk
                    .ref_columns
                    .iter()
                    .map(|name| {
                        ref_schema.find_column(name).ok_or_else(|| {
                            FalconError::Execution(ExecutionError::TypeError(format!(
                                "Referenced column '{}' not found in table '{}'",
                                name, fk.ref_table
                            )))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // Scan referenced table ONCE, build HashSet of valid FK values
                let ref_rows = self.storage.scan(ref_table_id, txn.txn_id, read_ts)?;
                let fk_set: std::collections::HashSet<Vec<Datum>> = ref_rows
                    .iter()
                    .map(|(_, ref_row)| {
                        ref_col_indices
                            .iter()
                            .map(|&idx| ref_row.values[idx].clone())
                            .collect()
                    })
                    .collect();

                // Check each new row
                for new_row in &built_rows {
                    let fk_vals: Vec<Datum> = fk
                        .columns
                        .iter()
                        .map(|&idx| new_row.values[idx].clone())
                        .collect();
                    if fk_vals.iter().any(falcon_common::datum::Datum::is_null) {
                        continue;
                    }
                    if !fk_set.contains(&fk_vals) {
                        return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                            "FOREIGN KEY constraint violated: no matching row in '{}'",
                            fk.ref_table
                        ))));
                    }
                }
            }
        }

        use falcon_common::schema::{TriggerEvent, TriggerLevel, TriggerTiming};
        let catalog = self.storage.get_catalog();
        let has_row_triggers = catalog.triggers_for_table(&schema.name)
            .iter()
            .any(|t| t.enabled && t.events.contains(&TriggerEvent::Insert) && t.level == TriggerLevel::Row);

        if !has_row_triggers {
            let count = built_rows.len() as u64;
            // batch_insert takes ownership; return an empty vec to pool after
            let consumed = built_rows;
            return_built_rows_pool(Vec::new());
            self.storage.batch_insert(table_id, consumed, txn.txn_id)
                .map_err(FalconError::Storage)?;
            return Ok(ExecutionResult::Dml { rows_affected: count, tag: "INSERT" });
        }

        let mut count = 0u64;
        let rows_to_fire: Vec<OwnedRow> = built_rows.drain(..).collect();
        return_built_rows_pool(built_rows);
        for row in rows_to_fire {
            let new_vars = Self::build_new_row_vars(schema, &row);
            self.fire_row_trigger(&schema.name, TriggerTiming::Before, TriggerEvent::Insert, &new_vars, txn)?;
            self.storage.insert(table_id, row, txn.txn_id).map_err(FalconError::Storage)?;
            self.fire_row_trigger(&schema.name, TriggerTiming::After, TriggerEvent::Insert, &new_vars, txn)?;
            count += 1;
        }
        Ok(ExecutionResult::Dml { rows_affected: count, tag: "INSERT" })
    }

    /// Slow per-row INSERT path: supports RETURNING and ON CONFLICT.
    fn exec_insert_slow(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &TableSchema,
        columns: &[usize],
        rows: &[Vec<BoundExpr>],
        returning: &[(BoundExpr, String)],
        on_conflict: &Option<OnConflictAction>,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let mut count = 0u64;
        let mut returning_rows = Vec::new();

        // UNIQUE constraint enforcement is delegated to MemTable.insert() via check_unique_indexes().
        // The former O(N) full-table pre-scan was a hot-path bottleneck and is no longer needed.

        // Pre-build FK lookup sets once
        let fk_lookup: Vec<(Vec<usize>, std::collections::HashSet<Vec<Datum>>)> =
            if !schema.foreign_keys.is_empty() {
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                schema
                    .foreign_keys
                    .iter()
                    .map(|fk| {
                        let ref_schema =
                            self.storage
                                .get_table_schema(&fk.ref_table)
                                .ok_or_else(|| {
                                    FalconError::Execution(ExecutionError::TypeError(format!(
                                        "Referenced table '{}' not found",
                                        fk.ref_table
                                    )))
                                })?;
                        let ref_table_id = ref_schema.id;
                        let ref_col_indices: Vec<usize> = fk
                            .ref_columns
                            .iter()
                            .map(|name| {
                                ref_schema.find_column(name).ok_or_else(|| {
                                    FalconError::Execution(ExecutionError::TypeError(format!(
                                        "Referenced column '{}' not found in table '{}'",
                                        name, fk.ref_table
                                    )))
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        let ref_rows = self.storage.scan(ref_table_id, txn.txn_id, read_ts)?;
                        let fk_set: std::collections::HashSet<Vec<Datum>> = ref_rows
                            .iter()
                            .map(|(_, ref_row)| {
                                ref_col_indices
                                    .iter()
                                    .map(|&idx| ref_row.values[idx].clone())
                                    .collect()
                            })
                            .collect();
                        Ok((fk.columns.clone(), fk_set))
                    })
                    .collect::<Result<Vec<_>, FalconError>>()?
            } else {
                vec![]
            };

        for row_exprs in rows {
            let values = self.build_insert_row(schema, columns, row_exprs)?;

            // Build row once for CHECK constraints (avoids clone)
            let row = OwnedRow::new(values);
            if !schema.check_constraints.is_empty() {
                self.eval_check_constraints(schema, &row)?;
            }
            let values = row.values;

            // Enforce FOREIGN KEY constraints (using pre-built HashSets)
            for (fk_cols, fk_set) in &fk_lookup {
                let fk_vals: Vec<Datum> = fk_cols.iter().map(|&idx| values[idx].clone()).collect();
                if fk_vals.iter().any(falcon_common::datum::Datum::is_null) {
                    continue;
                }
                if !fk_set.contains(&fk_vals) {
                    return Err(FalconError::Execution(ExecutionError::TypeError(
                        "FOREIGN KEY constraint violated".into(),
                    )));
                }
            }

            let new_vars = {
                use falcon_common::schema::{TriggerEvent, TriggerLevel};
                let cat = self.storage.get_catalog();
                let needs = cat.triggers_for_table(&schema.name).iter()
                    .any(|t| t.enabled && t.events.contains(&TriggerEvent::Insert) && t.level == TriggerLevel::Row);
                if needs { Some(Self::build_new_row_vars(schema, &OwnedRow::new(values.clone()))) } else { None }
            };
            if let Some(ref rv) = new_vars {
                use falcon_common::schema::{TriggerEvent, TriggerTiming};
                self.fire_row_trigger(&schema.name, TriggerTiming::Before, TriggerEvent::Insert, rv, txn)?;
            }
            let row = OwnedRow::new(values.clone());
            match self.storage.insert(table_id, row, txn.txn_id) {
                Ok(_) => {
                    if let Some(ref rv) = new_vars {
                        use falcon_common::schema::{TriggerEvent, TriggerTiming};
                        self.fire_row_trigger(&schema.name, TriggerTiming::After, TriggerEvent::Insert, rv, txn)?;
                    }
                    if !returning.is_empty() {
                        let ret_row = OwnedRow::new(values);
                        let ret_vals: Vec<Datum> = returning
                            .iter()
                            .map(|(expr, _)| {
                                ExprEngine::eval_row(expr, &ret_row).map_err(FalconError::Execution)
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        returning_rows.push(ret_vals);
                    }
                    count += 1;
                }
                Err(falcon_common::error::StorageError::DuplicateKey) => {
                    match on_conflict {
                        Some(OnConflictAction::DoNothing) => {
                            // Skip this row silently
                        }
                        Some(OnConflictAction::DoUpdate(assignments, where_clause)) => {
                            // Find the existing row by PK and update it
                            let pk_values: Vec<Datum> = schema
                                .primary_key_columns
                                .iter()
                                .map(|&idx| values[idx].clone())
                                .collect();
                            let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                            let existing_rows = self.storage.scan(table_id, txn.txn_id, read_ts)?;
                            for (pk, existing_row) in &existing_rows {
                                let existing_pk: Vec<Datum> = schema
                                    .primary_key_columns
                                    .iter()
                                    .map(|&idx| existing_row.values[idx].clone())
                                    .collect();
                                if existing_pk == pk_values {
                                    // Build combined row: [existing cols] ++ [excluded cols]
                                    let mut combined_values = existing_row.values.clone();
                                    combined_values.extend(values.iter().cloned());
                                    let combined_row = OwnedRow::new(combined_values);

                                    // Evaluate WHERE clause — skip update if false
                                    if let Some(ref wc) = where_clause {
                                        if !ExprEngine::eval_filter(wc, &combined_row)
                                            .map_err(FalconError::Execution)?
                                        {
                                            break;
                                        }
                                    }

                                    let mut new_values = existing_row.values.clone();
                                    for (col_idx, expr) in assignments {
                                        let val = ExprEngine::eval_row(expr, &combined_row)
                                            .map_err(FalconError::Execution)?;
                                        new_values[*col_idx] = val;
                                    }
                                    // Enforce constraints on the updated row
                                    Self::enforce_max_length(schema, &new_values)?;
                                    for (i, val) in new_values.iter().enumerate() {
                                        if i < schema.columns.len()
                                            && val.is_null()
                                            && !schema.columns[i].nullable
                                        {
                                            return Err(FalconError::Execution(
                                                ExecutionError::TypeError(format!(
                                                    "NULL value in column '{}' violates NOT NULL constraint",
                                                    schema.columns[i].name
                                                )),
                                            ));
                                        }
                                    }
                                    let check_row = OwnedRow::new(new_values);
                                    self.eval_check_constraints(schema, &check_row)?;
                                    let new_values = check_row.values;

                                    // UNIQUE enforcement delegated to storage.update() via
                                    // check_unique_indexes() — no O(N) pre-scan needed here.

                                    self.apply_fk_on_update(
                                        schema,
                                        existing_row,
                                        &new_values,
                                        txn,
                                    )?;
                                    let new_row = OwnedRow::new(new_values.clone());
                                    self.storage.update(table_id, pk, new_row, txn.txn_id)?;
                                    if !returning.is_empty() {
                                        let ret_row = OwnedRow::new(new_values);
                                        let ret_vals: Vec<Datum> = returning
                                            .iter()
                                            .map(|(expr, _)| {
                                                ExprEngine::eval_row(expr, &ret_row)
                                                    .map_err(FalconError::Execution)
                                            })
                                            .collect::<Result<Vec<_>, _>>()?;
                                        returning_rows.push(ret_vals);
                                    }
                                    count += 1;
                                    break;
                                }
                            }
                        }
                        None => {
                            return Err(falcon_common::error::StorageError::DuplicateKey.into());
                        }
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        if !returning.is_empty() {
            let columns: Vec<(String, falcon_common::types::DataType)> = returning
                .iter()
                .map(|(expr, alias)| {
                    let dt = if let BoundExpr::ColumnRef(idx) = expr {
                        schema
                            .columns
                            .get(*idx)
                            .map_or(DataType::Text, |c| c.data_type.clone())
                    } else {
                        DataType::Text
                    };
                    (alias.clone(), dt)
                })
                .collect();
            let rows = returning_rows.into_iter().map(OwnedRow::new).collect();
            Ok(ExecutionResult::Query { columns, rows })
        } else {
            Ok(ExecutionResult::Dml {
                rows_affected: count,
                tag: "INSERT",
            })
        }
    }

    pub(crate) fn exec_insert_select(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &TableSchema,
        columns: &[usize],
        sel: &BoundSelect,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        // Execute the SELECT query first
        let cte_data = self.materialize_ctes(&sel.ctes, txn)?;
        let select_result = if sel.joins.is_empty() {
            self.exec_seq_scan(
                sel.table_id,
                &sel.schema,
                &sel.projections,
                sel.visible_projection_count,
                sel.filter.as_ref(),
                &sel.group_by,
                &sel.grouping_sets,
                sel.having.as_ref(),
                &sel.order_by,
                sel.limit,
                sel.offset,
                &sel.distinct,
                txn,
                &cte_data,
                &sel.virtual_rows,
            )?
        } else {
            self.exec_nested_loop_join(
                sel.table_id,
                &sel.schema,
                &sel.joins,
                &sel.schema,
                &sel.projections,
                sel.visible_projection_count,
                sel.filter.as_ref(),
                &sel.order_by,
                sel.limit,
                sel.offset,
                &sel.distinct,
                txn,
                &cte_data,
            )?
        };

        // Extract rows from select result and insert them
        let source_rows = match select_result {
            ExecutionResult::Query { rows, .. } => rows,
            _ => {
                return Err(FalconError::Internal(
                    "INSERT SELECT: expected query result".into(),
                ))
            }
        };

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());

        // Pre-scan existing rows once for UNIQUE checks
        let mut unique_sets: Vec<std::collections::HashSet<Vec<Datum>>> =
            if !schema.unique_constraints.is_empty() {
                let existing = self.storage.scan(table_id, txn.txn_id, read_ts)?;
                schema
                    .unique_constraints
                    .iter()
                    .map(|uniq_cols| {
                        existing
                            .iter()
                            .filter_map(|(_, row)| {
                                let key: Vec<Datum> = uniq_cols
                                    .iter()
                                    .map(|&idx| row.values[idx].clone())
                                    .collect();
                                if key.iter().any(falcon_common::datum::Datum::is_null) {
                                    None
                                } else {
                                    Some(key)
                                }
                            })
                            .collect()
                    })
                    .collect()
            } else {
                vec![]
            };

        // Pre-build FK lookup sets once
        let fk_lookup: Vec<(Vec<usize>, std::collections::HashSet<Vec<Datum>>)> =
            if !schema.foreign_keys.is_empty() {
                schema
                    .foreign_keys
                    .iter()
                    .map(|fk| {
                        let ref_schema =
                            self.storage
                                .get_table_schema(&fk.ref_table)
                                .ok_or_else(|| {
                                    FalconError::Execution(ExecutionError::TypeError(format!(
                                        "Referenced table '{}' not found",
                                        fk.ref_table
                                    )))
                                })?;
                        let ref_col_indices: Vec<usize> = fk
                            .ref_columns
                            .iter()
                            .map(|name| {
                                ref_schema.find_column(name).ok_or_else(|| {
                                    FalconError::Execution(ExecutionError::TypeError(format!(
                                        "Referenced column '{}' not found in table '{}'",
                                        name, fk.ref_table
                                    )))
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        let ref_rows = self.storage.scan(ref_schema.id, txn.txn_id, read_ts)?;
                        let fk_set: std::collections::HashSet<Vec<Datum>> = ref_rows
                            .iter()
                            .map(|(_, r)| {
                                ref_col_indices
                                    .iter()
                                    .map(|&idx| r.values[idx].clone())
                                    .collect()
                            })
                            .collect();
                        Ok((fk.columns.clone(), fk_set))
                    })
                    .collect::<Result<Vec<_>, FalconError>>()?
            } else {
                vec![]
            };

        let mut count = 0u64;
        for source_row in &source_rows {
            if source_row.values.len() != columns.len() {
                return Err(FalconError::Internal(format!(
                    "INSERT SELECT: column count mismatch ({} vs {})",
                    columns.len(),
                    source_row.values.len()
                )));
            }
            // Start with column defaults, then overwrite with SELECT values
            let mut values: Vec<Datum> = schema
                .columns
                .iter()
                .map(|c| c.default_value.clone().unwrap_or(Datum::Null))
                .collect();
            for (i, val) in source_row.values.iter().enumerate() {
                values[columns[i]] = val.clone();
            }

            // Fill SERIAL columns still NULL
            for (col_idx, col) in schema.columns.iter().enumerate() {
                if col.is_serial && values[col_idx].is_null() {
                    let next_val = self.storage.next_serial_value(&schema.name, col_idx)?;
                    values[col_idx] = if col.data_type == DataType::Int64 {
                        Datum::Int64(next_val)
                    } else {
                        Datum::Int32(i32::try_from(next_val).map_err(|_| {
                            FalconError::Execution(
                                falcon_common::error::ExecutionError::NumericOverflow,
                            )
                        })?)
                    };
                }
            }

            // Dynamic defaults (CURRENT_TIMESTAMP, etc.)
            Self::eval_dynamic_defaults(schema, &mut values);

            // Enforce NOT NULL constraints
            for (i, val) in values.iter().enumerate() {
                if val.is_null() && !schema.columns[i].nullable {
                    return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                        "NULL value in column '{}' violates NOT NULL constraint",
                        schema.columns[i].name
                    ))));
                }
            }

            // Build row once — check constraints on it directly (avoids clone)
            let row = OwnedRow::new(values);
            self.eval_check_constraints(schema, &row)?;

            // Enforce UNIQUE constraints
            for (set_idx, uniq_cols) in schema.unique_constraints.iter().enumerate() {
                let key: Vec<Datum> = uniq_cols
                    .iter()
                    .map(|&idx| row.values[idx].clone())
                    .collect();
                if key.iter().any(falcon_common::datum::Datum::is_null) {
                    continue;
                }
                if !unique_sets[set_idx].insert(key) {
                    let col_names: Vec<&str> = uniq_cols
                        .iter()
                        .map(|&idx| schema.columns[idx].name.as_str())
                        .collect();
                    return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                        "UNIQUE constraint violated on column(s): {}",
                        col_names.join(", ")
                    ))));
                }
            }

            // Enforce FK constraints
            for (fk_cols, fk_set) in &fk_lookup {
                let fk_vals: Vec<Datum> =
                    fk_cols.iter().map(|&idx| row.values[idx].clone()).collect();
                if fk_vals.iter().any(falcon_common::datum::Datum::is_null) {
                    continue;
                }
                if !fk_set.contains(&fk_vals) {
                    return Err(FalconError::Execution(ExecutionError::TypeError(
                        "FOREIGN KEY constraint violated".into(),
                    )));
                }
            }

            self.storage.insert(table_id, row, txn.txn_id)?;
            count += 1;
        }

        Ok(ExecutionResult::Dml {
            rows_affected: count,
            tag: "INSERT",
        })
    }

    pub(crate) fn exec_update(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &TableSchema,
        assignments: &[(usize, BoundExpr)],
        filter: Option<&BoundExpr>,
        returning: &[(BoundExpr, String)],
        from_table: Option<&BoundFromTable>,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        use falcon_common::schema::{TriggerEvent, TriggerTiming};
        self.fire_triggers(&schema.name, TriggerTiming::Before, TriggerEvent::Update, txn)?;
        let result = self.exec_update_inner(table_id, schema, assignments, filter, returning, from_table, txn)?;
        self.fire_triggers(&schema.name, TriggerTiming::After, TriggerEvent::Update, txn)?;
        Ok(result)
    }

    fn exec_update_inner(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &TableSchema,
        assignments: &[(usize, BoundExpr)],
        filter: Option<&BoundExpr>,
        returning: &[(BoundExpr, String)],
        from_table: Option<&BoundFromTable>,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        // Multi-table UPDATE ... FROM: nested loop join approach
        if let Some(ft) = from_table {
            return self.exec_update_from(
                table_id,
                schema,
                assignments,
                filter,
                returning,
                ft,
                txn,
            );
        }

        // Apply RLS USING filter for UPDATE — hidden rows are silently skipped
        let rls_merged = self.apply_rls_filter(filter.cloned(), schema, "UPDATE");
        let effective_where: Option<&BoundExpr> = rls_merged.as_ref().or(filter);

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let (rows, effective_filter): (Vec<(Vec<u8>, _)>, _) =
            if let Some((pk_buf, remaining)) = self.try_pk_point_lookup(effective_where, schema) {
                match self.storage.get(table_id, &pk_buf, txn.txn_id, read_ts)? {
                    Some(row) => (vec![(pk_buf, row)], remaining),
                    None => (vec![], remaining),
                }
            } else if let Some((col_idx, key, remaining)) =
                self.try_index_scan_predicate(effective_where, table_id)
            {
                (
                    self.storage
                        .index_scan(table_id, col_idx, &key, txn.txn_id, read_ts)?,
                    remaining,
                )
            } else {
                (
                    self.storage.scan(table_id, txn.txn_id, read_ts)?,
                    effective_where.cloned(),
                )
            };
        let mut count = 0u64;
        let mat_filter = self.materialize_filter(effective_filter.as_ref(), txn)?;
        let mut returning_rows = Vec::new();

        // Pre-scan all rows once for UNIQUE constraint checks (avoid per-row O(n) rescan)
        // Use current_ts to catch concurrent committed conflicts beyond txn start snapshot
        let all_rows_for_uniq = if !schema.unique_constraints.is_empty() {
            Some(
                self.storage
                    .scan(table_id, txn.txn_id, self.txn_mgr.current_ts())?,
            )
        } else {
            None
        };

        for (pk, row) in &rows {
            // Apply filter
            if let Some(ref f) = mat_filter {
                if !ExprEngine::eval_filter(f, row).map_err(FalconError::Execution)? {
                    continue;
                }
            }

            // Apply assignments
            let mut new_values = row.values.clone();
            for (col_idx, expr) in assignments {
                let val = ExprEngine::eval_row(expr, row).map_err(FalconError::Execution)?;
                new_values[*col_idx] = val;
            }

            // Enforce VARCHAR(n)/CHAR(n) max length
            Self::enforce_max_length(schema, &new_values)?;

            // Enforce NOT NULL constraints
            for (i, val) in new_values.iter().enumerate() {
                if i < schema.columns.len() && val.is_null() && !schema.columns[i].nullable {
                    return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                        "NULL value in column '{}' violates NOT NULL constraint",
                        schema.columns[i].name
                    ))));
                }
            }

            // Check constraints directly on OwnedRow (avoids clone)
            let check_row = OwnedRow::new(new_values);
            self.eval_check_constraints(schema, &check_row)?;
            let new_values = check_row.values;

            // Enforce UNIQUE constraints on updated row
            if let Some(ref all_rows) = all_rows_for_uniq {
                for uniq_cols in &schema.unique_constraints {
                    let new_vals: Vec<&Datum> =
                        uniq_cols.iter().map(|&idx| &new_values[idx]).collect();
                    if new_vals.iter().any(|v| v.is_null()) {
                        continue;
                    }
                    for (other_pk, other_row) in all_rows {
                        if other_pk == pk {
                            continue;
                        } // skip self
                        let other_vals: Vec<&Datum> = uniq_cols
                            .iter()
                            .map(|&idx| &other_row.values[idx])
                            .collect();
                        if new_vals == other_vals {
                            let col_names: Vec<&str> = uniq_cols
                                .iter()
                                .map(|&idx| schema.columns[idx].name.as_str())
                                .collect();
                            return Err(FalconError::Execution(ExecutionError::TypeError(
                                format!(
                                    "UNIQUE constraint violated on column(s): {}",
                                    col_names.join(", ")
                                ),
                            )));
                        }
                    }
                }
            }

            // Apply FK cascading actions for child tables if referenced columns changed
            self.apply_fk_on_update(schema, row, &new_values, txn)?;

            let row_trigger_vars = {
                use falcon_common::schema::{TriggerEvent, TriggerLevel};
                let cat = self.storage.get_catalog();
                let needs = cat.triggers_for_table(&schema.name).iter()
                    .any(|t| t.enabled && t.events.contains(&TriggerEvent::Update) && t.level == TriggerLevel::Row);
                if needs {
                    let new_row_tmp = OwnedRow::new(new_values.clone());
                    Some(Self::build_update_row_vars(schema, row, &new_row_tmp))
                } else {
                    None
                }
            };
            if let Some(ref rv) = row_trigger_vars {
                use falcon_common::schema::{TriggerEvent, TriggerTiming};
                self.fire_row_trigger(&schema.name, TriggerTiming::Before, TriggerEvent::Update, rv, txn)?;
            }

            if !returning.is_empty() {
                let ret_row = OwnedRow::new(new_values.clone());
                let ret_vals: Vec<Datum> = returning
                    .iter()
                    .map(|(expr, _)| {
                        ExprEngine::eval_row(expr, &ret_row).map_err(FalconError::Execution)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                returning_rows.push(ret_vals);
            }

            let new_row = OwnedRow::new(new_values);
            self.storage.update(table_id, pk, new_row, txn.txn_id)?;
            if let Some(ref rv) = row_trigger_vars {
                use falcon_common::schema::{TriggerEvent, TriggerTiming};
                self.fire_row_trigger(&schema.name, TriggerTiming::After, TriggerEvent::Update, rv, txn)?;
            }
            count += 1;
        }

        if !returning.is_empty() {
            let columns: Vec<(String, falcon_common::types::DataType)> = returning
                .iter()
                .map(|(expr, alias)| {
                    let dt = if let BoundExpr::ColumnRef(idx) = expr {
                        schema
                            .columns
                            .get(*idx)
                            .map_or(DataType::Text, |c| c.data_type.clone())
                    } else {
                        DataType::Text
                    };
                    (alias.clone(), dt)
                })
                .collect();
            let rows = returning_rows.into_iter().map(OwnedRow::new).collect();
            Ok(ExecutionResult::Query { columns, rows })
        } else {
            Ok(ExecutionResult::Dml {
                rows_affected: count,
                tag: "UPDATE",
            })
        }
    }

    pub(crate) fn exec_delete(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &TableSchema,
        filter: Option<&BoundExpr>,
        returning: &[(BoundExpr, String)],
        using_table: Option<&BoundFromTable>,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        use falcon_common::schema::{TriggerEvent, TriggerTiming};
        self.fire_triggers(&schema.name, TriggerTiming::Before, TriggerEvent::Delete, txn)?;
        let result = self.exec_delete_inner(table_id, schema, filter, returning, using_table, txn)?;
        self.fire_triggers(&schema.name, TriggerTiming::After, TriggerEvent::Delete, txn)?;
        Ok(result)
    }

    fn exec_delete_inner(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &TableSchema,
        filter: Option<&BoundExpr>,
        returning: &[(BoundExpr, String)],
        using_table: Option<&BoundFromTable>,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        // Multi-table DELETE ... USING: nested loop join approach
        if let Some(ut) = using_table {
            return self.exec_delete_using(table_id, schema, filter, returning, ut, txn);
        }

        // Apply RLS USING filter for DELETE — hidden rows are silently skipped
        let rls_merged = self.apply_rls_filter(filter.cloned(), schema, "DELETE");
        let effective_where: Option<&BoundExpr> = rls_merged.as_ref().or(filter);

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let (rows, effective_filter): (Vec<(Vec<u8>, _)>, _) =
            if let Some((pk_buf, remaining)) = self.try_pk_point_lookup(effective_where, schema) {
                match self.storage.get(table_id, &pk_buf, txn.txn_id, read_ts)? {
                    Some(row) => (vec![(pk_buf, row)], remaining),
                    None => (vec![], remaining),
                }
            } else if let Some((col_idx, key, remaining)) =
                self.try_index_scan_predicate(effective_where, table_id)
            {
                (
                    self.storage
                        .index_scan(table_id, col_idx, &key, txn.txn_id, read_ts)?,
                    remaining,
                )
            } else {
                (
                    self.storage.scan(table_id, txn.txn_id, read_ts)?,
                    effective_where.cloned(),
                )
            };
        let mut count = 0u64;
        let mat_filter = self.materialize_filter(effective_filter.as_ref(), txn)?;
        let mut returning_rows = Vec::new();

        use falcon_common::schema::{TriggerEvent, TriggerLevel, TriggerTiming};
        let has_row_triggers = {
            let cat = self.storage.get_catalog();
            cat.triggers_for_table(&schema.name).iter()
                .any(|t| t.enabled && t.events.contains(&TriggerEvent::Delete) && t.level == TriggerLevel::Row)
        };

        for (pk, row) in &rows {
            if let Some(ref f) = mat_filter {
                if !ExprEngine::eval_filter(f, row).map_err(FalconError::Execution)? {
                    continue;
                }
            }

            let old_vars = if has_row_triggers {
                Some(Self::build_old_row_vars(schema, row))
            } else {
                None
            };
            if let Some(ref ov) = old_vars {
                self.fire_row_trigger(&schema.name, TriggerTiming::Before, TriggerEvent::Delete, ov, txn)?;
            }

            if !returning.is_empty() {
                let ret_vals: Vec<Datum> = returning
                    .iter()
                    .map(|(expr, _)| {
                        ExprEngine::eval_row(expr, row).map_err(FalconError::Execution)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                returning_rows.push(ret_vals);
            }

            self.apply_fk_on_delete(schema, row, txn)?;
            self.storage.delete(table_id, pk, txn.txn_id)?;

            if let Some(ref ov) = old_vars {
                self.fire_row_trigger(&schema.name, TriggerTiming::After, TriggerEvent::Delete, ov, txn)?;
            }
            count += 1;
        }

        if !returning.is_empty() {
            let columns: Vec<(String, falcon_common::types::DataType)> = returning
                .iter()
                .map(|(expr, alias)| {
                    let dt = if let BoundExpr::ColumnRef(idx) = expr {
                        schema
                            .columns
                            .get(*idx)
                            .map_or(DataType::Text, |c| c.data_type.clone())
                    } else {
                        DataType::Text
                    };
                    (alias.clone(), dt)
                })
                .collect();
            let rows = returning_rows.into_iter().map(OwnedRow::new).collect();
            Ok(ExecutionResult::Query { columns, rows })
        } else {
            Ok(ExecutionResult::Dml {
                rows_affected: count,
                tag: "DELETE",
            })
        }
    }

    /// Apply FK referential actions when a row in the parent table is deleted.
    /// Scans all tables in the catalog for FKs that reference `parent_schema`,
    /// then applies CASCADE / SET NULL / RESTRICT as appropriate.
    fn apply_fk_on_delete(
        &self,
        parent_schema: &TableSchema,
        deleted_row: &OwnedRow,
        txn: &TxnHandle,
    ) -> Result<(), FalconError> {
        let mut visited = std::collections::HashSet::new();
        visited.insert(parent_schema.id);
        self.apply_fk_on_delete_inner(parent_schema, deleted_row, txn, &mut visited)
    }

    fn apply_fk_on_delete_inner(
        &self,
        parent_schema: &TableSchema,
        deleted_row: &OwnedRow,
        txn: &TxnHandle,
        visited: &mut std::collections::HashSet<falcon_common::types::TableId>,
    ) -> Result<(), FalconError> {
        use falcon_common::schema::FkAction;

        let catalog = self.storage.get_catalog();
        let parent_name_lower = parent_schema.name.to_lowercase();

        for child_table in catalog.list_tables() {
            for fk in &child_table.foreign_keys {
                if fk.ref_table.to_lowercase() != parent_name_lower {
                    continue;
                }

                // Resolve referenced column indices in parent
                let ref_col_indices: Vec<usize> = fk
                    .ref_columns
                    .iter()
                    .filter_map(|name| parent_schema.find_column(name))
                    .collect();
                if ref_col_indices.len() != fk.ref_columns.len() {
                    continue; // skip malformed FK
                }

                // Get the parent row's referenced values
                let parent_vals: Vec<&Datum> = ref_col_indices
                    .iter()
                    .map(|&idx| &deleted_row.values[idx])
                    .collect();

                // Scan child table for matching rows
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                let child_rows = self.storage.scan(child_table.id, txn.txn_id, read_ts)?;

                // Cycle detection for CASCADE: mark at table level before row loop
                // so all matching rows are processed, not just the first one.
                if matches!(fk.on_delete, FkAction::Cascade) && !visited.insert(child_table.id) {
                    continue;
                }

                for (child_pk, child_row) in &child_rows {
                    let child_vals: Vec<&Datum> = fk
                        .columns
                        .iter()
                        .map(|&idx| &child_row.values[idx])
                        .collect();

                    // Skip if any child FK value is NULL
                    if child_vals.iter().any(|v| v.is_null()) {
                        continue;
                    }

                    if child_vals != parent_vals {
                        continue;
                    }

                    // Matching child row found — apply the action
                    match fk.on_delete {
                        FkAction::Cascade => {
                            // Recursively apply cascading for grandchild tables
                            self.apply_fk_on_delete_inner(child_table, child_row, txn, visited)?;
                            self.storage.delete(child_table.id, child_pk, txn.txn_id)?;
                        }
                        FkAction::SetNull => {
                            for &col_idx in &fk.columns {
                                if col_idx < child_table.columns.len()
                                    && !child_table.columns[col_idx].nullable
                                {
                                    return Err(FalconError::Execution(ExecutionError::TypeError(
                                        format!(
                                            "Cannot set column '{}' to NULL (NOT NULL constraint) via ON DELETE SET NULL in '{}'",
                                            child_table.columns[col_idx].name, child_table.name
                                        ),
                                    )));
                                }
                            }
                            let mut new_values = child_row.values.clone();
                            for &col_idx in &fk.columns {
                                new_values[col_idx] = Datum::Null;
                            }
                            let new_row = OwnedRow::new(new_values);
                            self.storage
                                .update(child_table.id, child_pk, new_row, txn.txn_id)?;
                        }
                        FkAction::SetDefault => {
                            let mut new_values = child_row.values.clone();
                            for &col_idx in &fk.columns {
                                let def = child_table.columns[col_idx]
                                    .default_value
                                    .clone()
                                    .unwrap_or(Datum::Null);
                                if def.is_null()
                                    && col_idx < child_table.columns.len()
                                    && !child_table.columns[col_idx].nullable
                                {
                                    return Err(FalconError::Execution(ExecutionError::TypeError(
                                        format!(
                                            "Cannot set column '{}' to NULL default (NOT NULL constraint) via ON DELETE SET DEFAULT in '{}'",
                                            child_table.columns[col_idx].name, child_table.name
                                        ),
                                    )));
                                }
                                new_values[col_idx] = def;
                            }
                            let new_row = OwnedRow::new(new_values);
                            self.storage
                                .update(child_table.id, child_pk, new_row, txn.txn_id)?;
                        }
                        FkAction::Restrict | FkAction::NoAction => {
                            return Err(FalconError::Execution(ExecutionError::TypeError(
                                format!(
                                    "Cannot delete row from '{}': referenced by foreign key in '{}'",
                                    parent_schema.name, child_table.name
                                ),
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Apply FK referential actions when a row in the parent table is updated.
    /// Only triggers if the referenced columns actually changed.
    fn apply_fk_on_update(
        &self,
        parent_schema: &TableSchema,
        old_row: &OwnedRow,
        new_values: &[Datum],
        txn: &TxnHandle,
    ) -> Result<(), FalconError> {
        let mut visited = std::collections::HashSet::new();
        visited.insert(parent_schema.id);
        self.apply_fk_on_update_inner(parent_schema, old_row, new_values, txn, &mut visited)
    }

    #[allow(clippy::only_used_in_recursion)]
    fn apply_fk_on_update_inner(
        &self,
        parent_schema: &TableSchema,
        old_row: &OwnedRow,
        new_values: &[Datum],
        txn: &TxnHandle,
        visited: &mut std::collections::HashSet<falcon_common::types::TableId>,
    ) -> Result<(), FalconError> {
        use falcon_common::schema::FkAction;

        let catalog = self.storage.get_catalog();
        let parent_name_lower = parent_schema.name.to_lowercase();

        for child_table in catalog.list_tables() {
            for fk in &child_table.foreign_keys {
                if fk.ref_table.to_lowercase() != parent_name_lower {
                    continue;
                }

                let ref_col_indices: Vec<usize> = fk
                    .ref_columns
                    .iter()
                    .filter_map(|name| parent_schema.find_column(name))
                    .collect();
                if ref_col_indices.len() != fk.ref_columns.len() {
                    continue;
                }

                // Check if any referenced column actually changed
                let old_vals: Vec<&Datum> = ref_col_indices
                    .iter()
                    .map(|&idx| &old_row.values[idx])
                    .collect();
                let new_ref_vals: Vec<&Datum> = ref_col_indices
                    .iter()
                    .map(|&idx| &new_values[idx])
                    .collect();
                if old_vals == new_ref_vals {
                    continue; // no change in referenced columns
                }

                // Scan child table for rows matching old values
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                let child_rows = self.storage.scan(child_table.id, txn.txn_id, read_ts)?;

                for (child_pk, child_row) in &child_rows {
                    let child_vals: Vec<&Datum> = fk
                        .columns
                        .iter()
                        .map(|&idx| &child_row.values[idx])
                        .collect();

                    if child_vals.iter().any(|v| v.is_null()) {
                        continue;
                    }

                    if child_vals != old_vals {
                        continue;
                    }

                    match fk.on_update {
                        FkAction::Cascade => {
                            let mut updated_child = child_row.values.clone();
                            for (i, &fk_col) in fk.columns.iter().enumerate() {
                                updated_child[fk_col] = new_ref_vals[i].clone();
                            }
                            self.apply_fk_on_update_inner(
                                child_table,
                                child_row,
                                &updated_child,
                                txn,
                                visited,
                            )?;
                            let new_row = OwnedRow::new(updated_child);
                            self.storage
                                .update(child_table.id, child_pk, new_row, txn.txn_id)?;
                        }
                        FkAction::SetNull => {
                            for &fk_col in &fk.columns {
                                if fk_col < child_table.columns.len()
                                    && !child_table.columns[fk_col].nullable
                                {
                                    return Err(FalconError::Execution(ExecutionError::TypeError(
                                        format!(
                                            "Cannot set column '{}' to NULL (NOT NULL constraint) via ON UPDATE SET NULL in '{}'",
                                            child_table.columns[fk_col].name, child_table.name
                                        ),
                                    )));
                                }
                            }
                            let mut updated_child = child_row.values.clone();
                            for &fk_col in &fk.columns {
                                updated_child[fk_col] = Datum::Null;
                            }
                            let new_row = OwnedRow::new(updated_child);
                            self.storage
                                .update(child_table.id, child_pk, new_row, txn.txn_id)?;
                        }
                        FkAction::SetDefault => {
                            let mut updated_child = child_row.values.clone();
                            for &fk_col in &fk.columns {
                                let def = child_table.columns[fk_col]
                                    .default_value
                                    .clone()
                                    .unwrap_or(Datum::Null);
                                if def.is_null()
                                    && fk_col < child_table.columns.len()
                                    && !child_table.columns[fk_col].nullable
                                {
                                    return Err(FalconError::Execution(ExecutionError::TypeError(
                                        format!(
                                            "Cannot set column '{}' to NULL default (NOT NULL constraint) via ON UPDATE SET DEFAULT in '{}'",
                                            child_table.columns[fk_col].name, child_table.name
                                        ),
                                    )));
                                }
                                updated_child[fk_col] = def;
                            }
                            let new_row = OwnedRow::new(updated_child);
                            self.storage
                                .update(child_table.id, child_pk, new_row, txn.txn_id)?;
                        }
                        FkAction::Restrict | FkAction::NoAction => {
                            return Err(FalconError::Execution(ExecutionError::TypeError(
                                format!(
                                    "Cannot update row in '{}': referenced by foreign key in '{}'",
                                    parent_schema.name, child_table.name
                                ),
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// UPDATE ... FROM: nested-loop join between target table and FROM table.
    /// For each (target_row, from_row) pair where the filter matches the combined row,
    /// apply the assignments to target_row.
    fn exec_update_from(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &TableSchema,
        assignments: &[(usize, BoundExpr)],
        filter: Option<&BoundExpr>,
        returning: &[(BoundExpr, String)],
        ft: &BoundFromTable,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let target_rows = self.storage.scan(table_id, txn.txn_id, read_ts)?;
        let from_rows = self.storage.scan(ft.table_id, txn.txn_id, read_ts)?;
        let mat_filter = self.materialize_filter(filter, txn)?;

        let all_rows_for_uniq = if !schema.unique_constraints.is_empty() {
            Some(
                self.storage
                    .scan(table_id, txn.txn_id, self.txn_mgr.current_ts())?,
            )
        } else {
            None
        };

        let mut count = 0u64;
        let mut returning_rows = Vec::new();
        let mut updated_pks = std::collections::HashSet::new();

        for (pk, target_row) in &target_rows {
            for (_from_pk, from_row) in &from_rows {
                // Build combined row: [target_cols... , from_cols...]
                let mut combined_vals = target_row.values.clone();
                combined_vals.extend(from_row.values.iter().cloned());
                let combined_row = OwnedRow::new(combined_vals);

                // Apply filter on combined row
                if let Some(ref f) = mat_filter {
                    if !ExprEngine::eval_filter(f, &combined_row).map_err(FalconError::Execution)? {
                        continue;
                    }
                }

                // Avoid updating the same target row multiple times
                if !updated_pks.insert(pk.clone()) {
                    continue;
                }

                // Evaluate assignments against combined row
                let mut new_values = target_row.values.clone();
                for (col_idx, expr) in assignments {
                    let val = ExprEngine::eval_row(expr, &combined_row)
                        .map_err(FalconError::Execution)?;
                    new_values[*col_idx] = val;
                }

                // Enforce VARCHAR(n)/CHAR(n) max length
                Self::enforce_max_length(schema, &new_values)?;

                // Enforce NOT NULL constraints
                for (i, val) in new_values.iter().enumerate() {
                    if i < schema.columns.len() && val.is_null() && !schema.columns[i].nullable {
                        return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                            "NULL value in column '{}' violates NOT NULL constraint",
                            schema.columns[i].name
                        ))));
                    }
                }

                // Enforce CHECK constraints
                let check_row = OwnedRow::new(new_values.clone());
                self.eval_check_constraints(schema, &check_row)?;

                // Enforce UNIQUE constraints
                if let Some(ref all_rows) = all_rows_for_uniq {
                    for uniq_cols in &schema.unique_constraints {
                        let new_vals: Vec<&Datum> =
                            uniq_cols.iter().map(|&idx| &new_values[idx]).collect();
                        if new_vals.iter().any(|v| v.is_null()) {
                            continue;
                        }
                        for (other_pk, other_row) in all_rows {
                            if other_pk == pk {
                                continue;
                            }
                            let other_vals: Vec<&Datum> = uniq_cols
                                .iter()
                                .map(|&idx| &other_row.values[idx])
                                .collect();
                            if new_vals == other_vals {
                                let col_names: Vec<&str> = uniq_cols
                                    .iter()
                                    .map(|&idx| schema.columns[idx].name.as_str())
                                    .collect();
                                return Err(FalconError::Execution(ExecutionError::TypeError(
                                    format!(
                                        "UNIQUE constraint violated on column(s): {}",
                                        col_names.join(", ")
                                    ),
                                )));
                            }
                        }
                    }
                }

                // FK cascading on update
                self.apply_fk_on_update(schema, target_row, &new_values, txn)?;

                if !returning.is_empty() {
                    let ret_row = OwnedRow::new(new_values.clone());
                    let ret_vals: Vec<Datum> = returning
                        .iter()
                        .map(|(expr, _)| {
                            ExprEngine::eval_row(expr, &ret_row).map_err(FalconError::Execution)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    returning_rows.push(ret_vals);
                }

                let new_row = OwnedRow::new(new_values);
                self.storage.update(table_id, pk, new_row, txn.txn_id)?;
                count += 1;
                break; // move to next target row after first match
            }
        }

        if !returning.is_empty() {
            let columns: Vec<(String, DataType)> = returning
                .iter()
                .map(|(expr, alias)| {
                    let dt = if let BoundExpr::ColumnRef(idx) = expr {
                        schema
                            .columns
                            .get(*idx)
                            .map_or(DataType::Text, |c| c.data_type.clone())
                    } else {
                        DataType::Text
                    };
                    (alias.clone(), dt)
                })
                .collect();
            let rows = returning_rows.into_iter().map(OwnedRow::new).collect();
            Ok(ExecutionResult::Query { columns, rows })
        } else {
            Ok(ExecutionResult::Dml {
                rows_affected: count,
                tag: "UPDATE",
            })
        }
    }

    /// DELETE ... USING: nested-loop join between target table and USING table.
    /// For each (target_row, using_row) pair where the filter matches the combined row,
    /// delete the target_row.
    fn exec_delete_using(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &TableSchema,
        filter: Option<&BoundExpr>,
        returning: &[(BoundExpr, String)],
        ut: &BoundFromTable,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let target_rows = self.storage.scan(table_id, txn.txn_id, read_ts)?;
        let using_rows = self.storage.scan(ut.table_id, txn.txn_id, read_ts)?;
        let mat_filter = self.materialize_filter(filter, txn)?;

        let mut count = 0u64;
        let mut returning_rows = Vec::new();
        let mut deleted_pks = std::collections::HashSet::new();

        for (pk, target_row) in &target_rows {
            for (_using_pk, using_row) in &using_rows {
                // Build combined row: [target_cols... , using_cols...]
                let mut combined_vals = target_row.values.clone();
                combined_vals.extend(using_row.values.iter().cloned());
                let combined_row = OwnedRow::new(combined_vals);

                // Apply filter on combined row
                if let Some(ref f) = mat_filter {
                    if !ExprEngine::eval_filter(f, &combined_row).map_err(FalconError::Execution)? {
                        continue;
                    }
                }

                // Avoid deleting the same target row multiple times
                if !deleted_pks.insert(pk.clone()) {
                    continue;
                }

                if !returning.is_empty() {
                    let ret_vals: Vec<Datum> = returning
                        .iter()
                        .map(|(expr, _)| {
                            ExprEngine::eval_row(expr, target_row).map_err(FalconError::Execution)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    returning_rows.push(ret_vals);
                }

                // FK cascading on delete
                self.apply_fk_on_delete(schema, target_row, txn)?;

                self.storage.delete(table_id, pk, txn.txn_id)?;
                count += 1;
                break; // move to next target row after first match
            }
        }

        if !returning.is_empty() {
            let columns: Vec<(String, DataType)> = returning
                .iter()
                .map(|(expr, alias)| {
                    let dt = if let BoundExpr::ColumnRef(idx) = expr {
                        schema
                            .columns
                            .get(*idx)
                            .map_or(DataType::Text, |c| c.data_type.clone())
                    } else {
                        DataType::Text
                    };
                    (alias.clone(), dt)
                })
                .collect();
            let rows = returning_rows.into_iter().map(OwnedRow::new).collect();
            Ok(ExecutionResult::Query { columns, rows })
        } else {
            Ok(ExecutionResult::Dml {
                rows_affected: count,
                tag: "DELETE",
            })
        }
    }

    /// Execute a MERGE statement (SQL:2003).
    pub(crate) fn exec_merge(
        &self,
        merge: &BoundMerge,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let target_id = merge.target_table_id;
        let source_id = merge.source_table_id;
        let target_schema = &merge.target_schema;

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let source_rows = self.storage.scan(source_id, txn.txn_id, read_ts)?;
        let target_rows = self.storage.scan(target_id, txn.txn_id, read_ts)?;

        let all_rows_for_uniq = if !target_schema.unique_constraints.is_empty() {
            Some(
                self.storage
                    .scan(target_id, txn.txn_id, self.txn_mgr.current_ts())?,
            )
        } else {
            None
        };

        let mut count: u64 = 0;

        for (_src_pk, src_row) in &source_rows {
            // Find matching target row via ON condition
            let mut matched_target: Option<(&Vec<u8>, &OwnedRow)> = None;
            for (tgt_pk, tgt_row) in &target_rows {
                let mut combined = tgt_row.values.clone();
                combined.extend(src_row.values.iter().cloned());
                let combined_row = OwnedRow::new(combined);
                if ExprEngine::eval_filter(&merge.on_expr, &combined_row)
                    .map_err(FalconError::Execution)?
                {
                    matched_target = Some((tgt_pk, tgt_row));
                    break;
                }
            }

            if let Some((tgt_pk, tgt_row)) = matched_target {
                // WHEN MATCHED clauses
                for clause in &merge.clauses {
                    if clause.kind != MergeClauseKind::Matched {
                        continue;
                    }
                    let mut combined = tgt_row.values.clone();
                    combined.extend(src_row.values.iter().cloned());
                    let combined_row = OwnedRow::new(combined);

                    if let Some(ref pred) = clause.predicate {
                        if !ExprEngine::eval_filter(pred, &combined_row)
                            .map_err(FalconError::Execution)?
                        {
                            continue;
                        }
                    }
                    match &clause.action {
                        BoundMergeAction::Update(assignments) => {
                            let mut new_values = tgt_row.values.clone();
                            for (col_idx, expr) in assignments {
                                let val = ExprEngine::eval_row(expr, &combined_row)
                                    .map_err(FalconError::Execution)?;
                                new_values[*col_idx] = val;
                            }
                            Self::enforce_max_length(target_schema, &new_values)?;
                            for (i, val) in new_values.iter().enumerate() {
                                if i < target_schema.columns.len()
                                    && val.is_null()
                                    && !target_schema.columns[i].nullable
                                {
                                    return Err(FalconError::Execution(
                                        ExecutionError::TypeError(format!(
                                            "NULL value in column '{}' violates NOT NULL constraint",
                                            target_schema.columns[i].name
                                        )),
                                    ));
                                }
                            }
                            let check_row = OwnedRow::new(new_values);
                            self.eval_check_constraints(target_schema, &check_row)?;
                            let new_values = check_row.values;

                            // Enforce UNIQUE constraints
                            if let Some(ref all_rows) = all_rows_for_uniq {
                                for uniq_cols in &target_schema.unique_constraints {
                                    let new_vals: Vec<&Datum> =
                                        uniq_cols.iter().map(|&idx| &new_values[idx]).collect();
                                    if new_vals.iter().any(|v| v.is_null()) {
                                        continue;
                                    }
                                    for (other_pk, other_row) in all_rows {
                                        if other_pk == tgt_pk {
                                            continue;
                                        }
                                        let other_vals: Vec<&Datum> = uniq_cols
                                            .iter()
                                            .map(|&idx| &other_row.values[idx])
                                            .collect();
                                        if new_vals == other_vals {
                                            let col_names: Vec<&str> = uniq_cols
                                                .iter()
                                                .map(|&idx| {
                                                    target_schema.columns[idx].name.as_str()
                                                })
                                                .collect();
                                            return Err(FalconError::Execution(
                                                ExecutionError::TypeError(format!(
                                                    "UNIQUE constraint violated on column(s): {}",
                                                    col_names.join(", ")
                                                )),
                                            ));
                                        }
                                    }
                                }
                            }

                            self.apply_fk_on_update(target_schema, tgt_row, &new_values, txn)?;
                            let new_row = OwnedRow::new(new_values);
                            self.storage
                                .update(target_id, tgt_pk, new_row, txn.txn_id)?;
                            count += 1;
                        }
                        BoundMergeAction::Delete => {
                            self.apply_fk_on_delete(target_schema, tgt_row, txn)?;
                            self.storage.delete(target_id, tgt_pk, txn.txn_id)?;
                            count += 1;
                        }
                        BoundMergeAction::Insert(..) => {
                            return Err(FalconError::Execution(ExecutionError::TypeError(
                                "INSERT not allowed in WHEN MATCHED".into(),
                            )));
                        }
                    }
                    break;
                }
            } else {
                // WHEN NOT MATCHED clauses
                for clause in &merge.clauses {
                    if clause.kind != MergeClauseKind::NotMatched {
                        continue;
                    }
                    let nulls: Vec<Datum> =
                        target_schema.columns.iter().map(|_| Datum::Null).collect();
                    let mut combined = nulls;
                    combined.extend(src_row.values.iter().cloned());
                    let combined_row = OwnedRow::new(combined);

                    if let Some(ref pred) = clause.predicate {
                        if !ExprEngine::eval_filter(pred, &combined_row)
                            .map_err(FalconError::Execution)?
                        {
                            continue;
                        }
                    }
                    match &clause.action {
                        BoundMergeAction::Insert(cols, value_exprs) => {
                            let mut values: Vec<Datum> = target_schema
                                .columns
                                .iter()
                                .map(|c| c.default_value.clone().unwrap_or(Datum::Null))
                                .collect();
                            for (i, col_idx) in cols.iter().enumerate() {
                                let val = ExprEngine::eval_row(&value_exprs[i], &combined_row)
                                    .map_err(FalconError::Execution)?;
                                values[*col_idx] = val;
                            }
                            for (i, col) in target_schema.columns.iter().enumerate() {
                                if values[i].is_null() {
                                    continue;
                                }
                                if let Some(vt) = values[i].data_type() {
                                    if vt != col.data_type {
                                        let target = datatype_to_cast_target(&col.data_type);
                                        if let Ok(cv) =
                                            crate::eval::cast::eval_cast(values[i].clone(), &target)
                                        {
                                            values[i] = cv;
                                        }
                                    }
                                }
                            }
                            // Fill SERIAL columns still NULL
                            for (col_idx, col) in target_schema.columns.iter().enumerate() {
                                if col.is_serial && values[col_idx].is_null() {
                                    let next_val = self
                                        .storage
                                        .next_serial_value(&target_schema.name, col_idx)?;
                                    values[col_idx] = if col.data_type == DataType::Int64 {
                                        Datum::Int64(next_val)
                                    } else {
                                        Datum::Int32(i32::try_from(next_val).map_err(|_| {
                                            FalconError::Execution(ExecutionError::NumericOverflow)
                                        })?)
                                    };
                                }
                            }
                            Self::enforce_max_length(target_schema, &values)?;
                            Self::eval_dynamic_defaults(target_schema, &mut values);
                            // NOT NULL check
                            for (i, val) in values.iter().enumerate() {
                                if val.is_null() && !target_schema.columns[i].nullable {
                                    return Err(FalconError::Execution(ExecutionError::TypeError(
                                        format!(
                                            "NULL value in column '{}' violates NOT NULL constraint",
                                            target_schema.columns[i].name
                                        ),
                                    )));
                                }
                            }
                            let row = OwnedRow::new(values);
                            self.eval_check_constraints(target_schema, &row)?;
                            // UNIQUE constraints
                            if let Some(ref all_rows) = all_rows_for_uniq {
                                for uniq_cols in &target_schema.unique_constraints {
                                    let new_vals: Vec<&Datum> =
                                        uniq_cols.iter().map(|&idx| &row.values[idx]).collect();
                                    if new_vals.iter().any(|v| v.is_null()) {
                                        continue;
                                    }
                                    for (_other_pk, other_row) in all_rows {
                                        let other_vals: Vec<&Datum> = uniq_cols
                                            .iter()
                                            .map(|&idx| &other_row.values[idx])
                                            .collect();
                                        if new_vals == other_vals {
                                            let col_names: Vec<&str> = uniq_cols
                                                .iter()
                                                .map(|&idx| {
                                                    target_schema.columns[idx].name.as_str()
                                                })
                                                .collect();
                                            return Err(FalconError::Execution(
                                                ExecutionError::TypeError(format!(
                                                    "MERGE: UNIQUE constraint violated on column(s): {}",
                                                    col_names.join(", ")
                                                )),
                                            ));
                                        }
                                    }
                                }
                            }
                            // FK validation
                            if !target_schema.foreign_keys.is_empty() {
                                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                                for fk in &target_schema.foreign_keys {
                                    let fk_vals: Vec<Datum> = fk
                                        .columns
                                        .iter()
                                        .map(|&idx| row.values[idx].clone())
                                        .collect();
                                    if fk_vals.iter().any(Datum::is_null) {
                                        continue;
                                    }
                                    let ref_schema = self
                                        .storage
                                        .get_table_schema(&fk.ref_table)
                                        .ok_or_else(|| {
                                        FalconError::Execution(ExecutionError::TypeError(format!(
                                            "Referenced table '{}' not found",
                                            fk.ref_table
                                        )))
                                    })?;
                                    let ref_col_indices: Vec<usize> = fk
                                        .ref_columns
                                        .iter()
                                        .map(|name| {
                                            ref_schema.find_column(name).ok_or_else(|| {
                                                FalconError::Execution(ExecutionError::TypeError(
                                                    format!(
                                                        "Referenced column '{}' not found in '{}'",
                                                        name, fk.ref_table
                                                    ),
                                                ))
                                            })
                                        })
                                        .collect::<Result<Vec<_>, _>>()?;
                                    let ref_rows =
                                        self.storage.scan(ref_schema.id, txn.txn_id, read_ts)?;
                                    let found = ref_rows.iter().any(|(_, r)| {
                                        let rv: Vec<&Datum> =
                                            ref_col_indices.iter().map(|&i| &r.values[i]).collect();
                                        let fv: Vec<&Datum> = fk_vals.iter().collect();
                                        rv == fv
                                    });
                                    if !found {
                                        return Err(FalconError::Execution(
                                            ExecutionError::TypeError(
                                                "MERGE: FOREIGN KEY constraint violated".into(),
                                            ),
                                        ));
                                    }
                                }
                            }
                            self.storage.insert(target_id, row, txn.txn_id)?;
                            count += 1;
                        }
                        _ => {
                            return Err(FalconError::Execution(ExecutionError::TypeError(
                                "Only INSERT allowed in WHEN NOT MATCHED".into(),
                            )));
                        }
                    }
                    break;
                }
            }
        }

        Ok(ExecutionResult::Dml {
            tag: "MERGE",
            rows_affected: count,
        })
    }

    /// CREATE MATERIALIZED VIEW: parse query, create backing table, populate.
    pub(crate) fn exec_create_materialized_view(
        &self,
        name: &str,
        query_sql: &str,
        txn: &TxnHandle,
    ) -> Result<(), FalconError> {
        // Execute the query to get column metadata and rows
        let (columns, rows) = self.run_internal_query(query_sql, txn)?;

        // Build a backing table schema with a synthetic rowid PK
        let backing_table_name = format!("_mv_{}", name.to_lowercase());
        let mut col_defs = vec![falcon_common::schema::ColumnDef {
            id: falcon_common::types::ColumnId(0),
            name: "_rowid".to_owned(),
            data_type: DataType::Int64,
            nullable: false,
            is_primary_key: true,
            default_value: None,
            is_serial: true,
            max_length: None,
            generated: None,
        }];
        for (i, (col_name, col_type)) in columns.iter().enumerate() {
            col_defs.push(falcon_common::schema::ColumnDef {
                id: falcon_common::types::ColumnId((i + 1) as u32),
                name: col_name.clone(),
                data_type: col_type.clone(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
                max_length: None,
                generated: None,
            });
        }

        let backing_schema = TableSchema {
            name: backing_table_name.clone(),
            columns: col_defs,
            primary_key_columns: vec![0],
            ..Default::default()
        };

        // Create the backing table
        let table_id = self.storage.create_table(backing_schema)?;

        // Insert rows
        let mut rowid: i64 = 1;
        for row in &rows {
            let mut values = vec![Datum::Int64(rowid)];
            values.extend(row.values.iter().cloned());
            let ins_row = OwnedRow::new(values);
            self.storage.insert(table_id, ins_row, txn.txn_id)?;
            rowid += 1;
        }

        // Register the materialized view definition
        self.storage
            .create_materialized_view(name, query_sql, table_id)?;
        Ok(())
    }

    /// REFRESH MATERIALIZED VIEW: truncate backing table, re-populate from query.
    pub(crate) fn exec_refresh_materialized_view(
        &self,
        name: &str,
        txn: &TxnHandle,
    ) -> Result<(), FalconError> {
        let (backing_id, query_sql) = {
            let catalog = self.storage.get_catalog();
            let mv = catalog.find_materialized_view(name).ok_or_else(|| {
                FalconError::Execution(ExecutionError::TypeError(format!(
                    "materialized view '{name}' does not exist"
                )))
            })?;
            (mv.backing_table_id, mv.query_sql.clone())
        };

        // Truncate backing table
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let existing = self.storage.scan(backing_id, txn.txn_id, read_ts)?;
        for (pk, _) in &existing {
            self.storage.delete(backing_id, pk, txn.txn_id)?;
        }

        // Re-execute query and insert
        let (_columns, rows) = self.run_internal_query(&query_sql, txn)?;
        let mut rowid: i64 = 1;
        for row in &rows {
            let mut values = vec![Datum::Int64(rowid)];
            values.extend(row.values.iter().cloned());
            let ins_row = OwnedRow::new(values);
            self.storage.insert(backing_id, ins_row, txn.txn_id)?;
            rowid += 1;
        }
        Ok(())
    }

    /// Execute a SQL query internally and return (columns, rows).
    #[allow(clippy::type_complexity)]
    fn run_internal_query(
        &self,
        sql: &str,
        txn: &TxnHandle,
    ) -> Result<(Vec<(String, DataType)>, Vec<OwnedRow>), FalconError> {
        use falcon_planner::planner::Planner;
        use falcon_sql_frontend::binder::Binder;
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| {
            FalconError::Execution(ExecutionError::TypeError(format!(
                "matview query parse error: {e}"
            )))
        })?;
        let stmt = stmts.into_iter().next().ok_or_else(|| {
            FalconError::Execution(ExecutionError::TypeError("empty query".into()))
        })?;
        let catalog = self.storage.get_catalog();
        let mut binder = Binder::new(catalog);
        let bound = binder.bind(&stmt).map_err(|e| {
            FalconError::Execution(ExecutionError::TypeError(format!(
                "matview query bind error: {e}"
            )))
        })?;
        let plan = Planner::plan(&bound).map_err(|e| {
            FalconError::Execution(ExecutionError::TypeError(format!(
                "matview query plan error: {e}"
            )))
        })?;
        match self.execute(&plan, Some(txn))? {
            ExecutionResult::Query { columns, rows } => Ok((columns, rows)),
            _ => Err(FalconError::Execution(ExecutionError::TypeError(
                "materialized view query must be a SELECT".into(),
            ))),
        }
    }

    fn subst_vars_in_sql(sql: &str, vars: &std::collections::HashMap<String, Datum>) -> String {
        if vars.is_empty() {
            return sql.to_owned();
        }
        let mut result = sql.to_owned();
        let mut pairs: Vec<_> = vars.iter().collect();
        // longest keys first to avoid partial substitutions
        pairs.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
        for (k, v) in pairs {
            let literal = datum_to_sql_literal(v);
            // word-boundary replacement
            let mut out = String::with_capacity(result.len());
            let bytes = result.as_bytes();
            let mut i = 0;
            while i < result.len() {
                if result[i..].starts_with(k.as_str()) {
                    let end = i + k.len();
                    let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
                    let after_ok = end >= result.len() || !bytes[end].is_ascii_alphanumeric() && bytes[end] != b'_';
                    if before_ok && after_ok {
                        out.push_str(&literal);
                        i = end;
                        continue;
                    }
                }
                out.push(result[i..].chars().next().unwrap());
                i += result[i..].chars().next().unwrap().len_utf8();
            }
            result = out;
        }
        result
    }

    fn row_to_vars(schema: &TableSchema, row: &OwnedRow) -> std::collections::HashMap<String, Datum> {
        schema.columns.iter().enumerate()
            .map(|(i, col)| (col.name.to_lowercase(), row.values.get(i).cloned().unwrap_or(Datum::Null)))
            .collect()
    }

    pub(crate) fn fire_triggers(
        &self,
        table_name: &str,
        timing: falcon_common::schema::TriggerTiming,
        event: falcon_common::schema::TriggerEvent,
        txn: &TxnHandle,
    ) -> Result<(), FalconError> {
        use falcon_common::schema::TriggerLevel;
        self.fire_triggers_inner(table_name, timing, event, TriggerLevel::Statement, None, txn)
    }

    pub(crate) fn fire_row_trigger(
        &self,
        table_name: &str,
        timing: falcon_common::schema::TriggerTiming,
        event: falcon_common::schema::TriggerEvent,
        row_vars: &std::collections::HashMap<String, Datum>,
        txn: &TxnHandle,
    ) -> Result<(), FalconError> {
        use falcon_common::schema::TriggerLevel;
        self.fire_triggers_inner(table_name, timing, event, TriggerLevel::Row, Some(row_vars), txn)
    }

    /// Build vars for row trigger: includes plain col names, new_col / old_col prefixed names.
    fn build_new_row_vars(schema: &TableSchema, new_row: &OwnedRow) -> std::collections::HashMap<String, Datum> {
        let mut vars = Self::row_to_vars(schema, new_row);
        for (col, val) in vars.clone() {
            vars.insert(format!("new_{col}"), val.clone());
            vars.insert(format!("new.{col}"), val);
        }
        vars
    }

    fn build_old_row_vars(schema: &TableSchema, old_row: &OwnedRow) -> std::collections::HashMap<String, Datum> {
        let mut vars = Self::row_to_vars(schema, old_row);
        for (col, val) in vars.clone() {
            vars.insert(format!("old_{col}"), val.clone());
            vars.insert(format!("old.{col}"), val);
        }
        vars
    }

    fn build_update_row_vars(schema: &TableSchema, old_row: &OwnedRow, new_row: &OwnedRow) -> std::collections::HashMap<String, Datum> {
        let mut vars = Self::build_new_row_vars(schema, new_row);
        for (col, val) in Self::row_to_vars(schema, old_row) {
            vars.insert(format!("old_{col}"), val.clone());
            vars.insert(format!("old.{col}"), val);
        }
        vars
    }

    fn fire_triggers_inner(
        &self,
        table_name: &str,
        timing: falcon_common::schema::TriggerTiming,
        event: falcon_common::schema::TriggerEvent,
        level: falcon_common::schema::TriggerLevel,
        row_vars: Option<&std::collections::HashMap<String, Datum>>,
        txn: &TxnHandle,
    ) -> Result<(), FalconError> {
        let catalog = self.storage.get_catalog();
        let triggers: Vec<_> = catalog.triggers_for_table(table_name)
            .into_iter()
            .filter(|t| t.enabled && t.timing == timing && t.events.contains(&event) && t.level == level)
            .map(|t| t.function_name.clone())
            .collect();
        if triggers.is_empty() {
            return Ok(());
        }
        let empty_vars = std::collections::HashMap::new();
        let extra = row_vars.unwrap_or(&empty_vars);
        for func_name in triggers {
            let func_def = match catalog.find_function(&func_name).cloned() {
                Some(f) => f,
                None => continue,
            };
            let eval_sql = |sql: &str, vars: &std::collections::HashMap<String, Datum>| -> Result<Datum, falcon_common::error::ExecutionError> {
                let resolved = Self::subst_vars_in_sql(sql, vars);
                self.eval_sql_expr_in_txn(&resolved, txn)
                    .map_err(|e| falcon_common::error::ExecutionError::TypeError(e.to_string()))
            };
            let eval_rows = |sql: &str, vars: &std::collections::HashMap<String, Datum>| -> Result<Vec<std::collections::HashMap<String, Datum>>, falcon_common::error::ExecutionError> {
                let resolved = Self::subst_vars_in_sql(sql, vars);
                self.eval_sql_rows(&resolved, Some(txn))
                    .map_err(|e| falcon_common::error::ExecutionError::TypeError(e.to_string()))
            };
            let _ = crate::plpgsql::execute_plpgsql_with_vars(&func_def, &[], extra, eval_sql, eval_rows);
        }
        Ok(())
    }
}

/// Map a DataType to the cast target string used by eval_cast.
fn datatype_to_cast_target(dt: &DataType) -> String {
    match dt {
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "int".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float32 => "real".into(),
        DataType::Float64 => "float".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Text => "text".into(),
        DataType::Timestamp => "timestamp".into(),
        DataType::Date => "date".into(),
        DataType::Jsonb => "jsonb".into(),
        DataType::Array(_) => "array".into(),
        DataType::Decimal(_, _) => "numeric".into(),
        DataType::Time => "time".into(),
        DataType::Interval => "interval".into(),
        DataType::Uuid => "uuid".into(),
        DataType::Bytea => "bytea".into(),
        DataType::TsVector => "tsvector".into(),
        DataType::TsQuery => "tsquery".into(),
    }
}

fn datum_to_sql_literal(d: &Datum) -> String {
    match d {
        Datum::Null => "NULL".to_owned(),
        Datum::Boolean(b) => if *b { "TRUE".to_owned() } else { "FALSE".to_owned() },
        Datum::Int32(i) => i.to_string(),
        Datum::Int64(i) => i.to_string(),
        Datum::Float64(f) => format!("{f}"),
        Datum::Text(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!("'{other}'"),
    }
}
