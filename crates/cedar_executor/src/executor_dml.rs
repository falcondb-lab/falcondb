#![allow(clippy::too_many_arguments)]

use cedar_common::datum::{Datum, OwnedRow};
use cedar_common::error::{CedarError, ExecutionError};
use cedar_common::schema::TableSchema;
use cedar_common::types::DataType;
use cedar_sql_frontend::types::*;
use cedar_txn::TxnHandle;

use crate::expr_engine::ExprEngine;
use crate::executor::{Executor, CteData, ExecutionResult};

impl Executor {
    pub(crate) fn exec_insert(
        &self,
        table_id: cedar_common::types::TableId,
        schema: &TableSchema,
        columns: &[usize],
        rows: &[Vec<BoundExpr>],
        returning: &[(BoundExpr, String)],
        on_conflict: &Option<OnConflictAction>,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, CedarError> {
        let mut count = 0u64;
        let dummy_row = OwnedRow::new(vec![]);
        let mut returning_rows = Vec::new();

        for row_exprs in rows {
            // Build a full row with defaults (or NULLs) for missing columns
            let mut values: Vec<Datum> = schema.columns.iter()
                .map(|c| c.default_value.clone().unwrap_or(Datum::Null))
                .collect();
            for (i, expr) in row_exprs.iter().enumerate() {
                let col_idx = columns[i];
                let val = ExprEngine::eval_row(expr, &dummy_row)
                    .map_err(CedarError::Execution)?;
                values[col_idx] = val;
            }

            // Coerce values to match column data types (e.g. Text -> Date, Text -> Timestamp)
            for (col_idx, col) in schema.columns.iter().enumerate() {
                if values[col_idx].is_null() {
                    continue;
                }
                if let Some(val_type) = values[col_idx].data_type() {
                    if val_type != col.data_type {
                        let target = datatype_to_cast_target(&col.data_type);
                        if let Ok(cast_val) = crate::eval::cast::eval_cast(values[col_idx].clone(), &target) {
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
                        Datum::Int32(next_val as i32)
                    };
                }
            }

            // Enforce NOT NULL constraints
            for (i, val) in values.iter().enumerate() {
                if val.is_null() && !schema.columns[i].nullable {
                    return Err(CedarError::Execution(ExecutionError::TypeError(
                        format!("NULL value in column '{}' violates NOT NULL constraint", schema.columns[i].name),
                    )));
                }
            }

            // Enforce CHECK constraints
            let check_row = OwnedRow::new(values.clone());
            self.eval_check_constraints(schema, &check_row)?;

            // Enforce UNIQUE constraints
            if !schema.unique_constraints.is_empty() {
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                let existing_rows = self.storage.scan(table_id, txn.txn_id, read_ts)?;
                for uniq_cols in &schema.unique_constraints {
                    let new_vals: Vec<&Datum> = uniq_cols.iter().map(|&idx| &values[idx]).collect();
                    // Skip if any value is NULL (NULLs are always unique per SQL standard)
                    if new_vals.iter().any(|v| v.is_null()) {
                        continue;
                    }
                    for (_, existing_row) in &existing_rows {
                        let existing_vals: Vec<&Datum> = uniq_cols.iter().map(|&idx| &existing_row.values[idx]).collect();
                        if new_vals == existing_vals {
                            let col_names: Vec<&str> = uniq_cols.iter().map(|&idx| schema.columns[idx].name.as_str()).collect();
                            return Err(CedarError::Execution(ExecutionError::TypeError(
                                format!("UNIQUE constraint violated on column(s): {}", col_names.join(", ")),
                            )));
                        }
                    }
                }
            }

            // Enforce FOREIGN KEY constraints
            for fk in &schema.foreign_keys {
                let fk_vals: Vec<&Datum> = fk.columns.iter().map(|&idx| &values[idx]).collect();
                // Skip if any FK value is NULL
                if fk_vals.iter().any(|v| v.is_null()) {
                    continue;
                }
                let ref_schema = self.storage.get_table_schema(&fk.ref_table)
                    .ok_or_else(|| CedarError::Execution(ExecutionError::TypeError(
                        format!("Referenced table '{}' not found", fk.ref_table),
                    )))?;
                let ref_table_id = ref_schema.id;
                let ref_col_indices: Vec<usize> = fk.ref_columns.iter()
                    .map(|name| ref_schema.find_column(name).unwrap_or(0))
                    .collect();
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                let ref_rows = self.storage.scan(ref_table_id, txn.txn_id, read_ts)?;
                let found = ref_rows.iter().any(|(_, ref_row)| {
                    let ref_vals: Vec<&Datum> = ref_col_indices.iter().map(|&idx| &ref_row.values[idx]).collect();
                    ref_vals == fk_vals
                });
                if !found {
                    return Err(CedarError::Execution(ExecutionError::TypeError(
                        format!("FOREIGN KEY constraint violated: no matching row in '{}'", fk.ref_table),
                    )));
                }
            }

            let row = OwnedRow::new(values.clone());
            match self.storage.insert(table_id, row, txn.txn_id) {
                Ok(_) => {
                    if !returning.is_empty() {
                        let ret_row = OwnedRow::new(values.clone());
                        let ret_vals: Vec<Datum> = returning.iter().map(|(expr, _)| {
                            ExprEngine::eval_row(expr, &ret_row).unwrap_or(Datum::Null)
                        }).collect();
                        returning_rows.push(ret_vals);
                    }
                    count += 1;
                }
                Err(cedar_common::error::StorageError::DuplicateKey) => {
                    match on_conflict {
                        Some(OnConflictAction::DoNothing) => {
                            // Skip this row silently
                        }
                        Some(OnConflictAction::DoUpdate(assignments)) => {
                            // Find the existing row by PK and update it
                            let pk_values: Vec<Datum> = schema.primary_key_columns.iter()
                                .map(|&idx| values[idx].clone())
                                .collect();
                            let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                            let existing_rows = self.storage.scan(table_id, txn.txn_id, read_ts)?;
                            for (pk, existing_row) in &existing_rows {
                                let existing_pk: Vec<Datum> = schema.primary_key_columns.iter()
                                    .map(|&idx| existing_row.values[idx].clone())
                                    .collect();
                                if existing_pk == pk_values {
                                    // Build combined row: [existing cols] ++ [excluded cols]
                                    // so excluded.col references (ColumnRef(num_cols + idx)) resolve correctly
                                    let mut combined_values = existing_row.values.clone();
                                    combined_values.extend(values.iter().cloned());
                                    let combined_row = OwnedRow::new(combined_values);

                                    let mut new_values = existing_row.values.clone();
                                    for (col_idx, expr) in assignments {
                                        let val = ExprEngine::eval_row(expr, &combined_row)
                                            .map_err(CedarError::Execution)?;
                                        new_values[*col_idx] = val;
                                    }
                                    let new_row = OwnedRow::new(new_values.clone());
                                    self.storage.update(table_id, pk, new_row, txn.txn_id)?;
                                    if !returning.is_empty() {
                                        let ret_row = OwnedRow::new(new_values.clone());
                                        let ret_vals: Vec<Datum> = returning.iter().map(|(expr, _)| {
                                            ExprEngine::eval_row(expr, &ret_row).unwrap_or(Datum::Null)
                                        }).collect();
                                        returning_rows.push(ret_vals);
                                    }
                                    count += 1;
                                    break;
                                }
                            }
                        }
                        None => {
                            return Err(cedar_common::error::StorageError::DuplicateKey.into());
                        }
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        if !returning.is_empty() {
            let columns: Vec<(String, cedar_common::types::DataType)> = returning.iter().map(|(expr, alias)| {
                let dt = if let BoundExpr::ColumnRef(idx) = expr {
                    schema.columns.get(*idx).map(|c| c.data_type.clone()).unwrap_or(DataType::Text)
                } else {
                    DataType::Text
                };
                (alias.clone(), dt)
            }).collect();
            let rows = returning_rows.into_iter().map(OwnedRow::new).collect();
            Ok(ExecutionResult::Query { columns, rows })
        } else {
            Ok(ExecutionResult::Dml {
                rows_affected: count,
                tag: "INSERT".into(),
            })
        }
    }

    pub(crate) fn exec_insert_select(
        &self,
        table_id: cedar_common::types::TableId,
        schema: &TableSchema,
        columns: &[usize],
        sel: &BoundSelect,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, CedarError> {
        // Execute the SELECT query first
        let select_result = if sel.joins.is_empty() {
            self.exec_seq_scan(
                sel.table_id, &sel.schema, &sel.projections,
                sel.visible_projection_count,
                sel.filter.as_ref(), &sel.group_by, &sel.grouping_sets,
                sel.having.as_ref(),
                &sel.order_by, sel.limit, sel.offset, &sel.distinct, txn,
                &CteData::new(),
                &sel.virtual_rows,
            )?
        } else {
            self.exec_nested_loop_join(
                sel.table_id, &sel.schema, &sel.joins, &sel.schema,
                &sel.projections, sel.visible_projection_count,
                sel.filter.as_ref(),
                &sel.order_by, sel.limit, sel.offset, &sel.distinct, txn,
                &CteData::new(),
            )?
        };

        // Extract rows from select result and insert them
        let source_rows = match select_result {
            ExecutionResult::Query { rows, .. } => rows,
            _ => return Err(CedarError::Internal("INSERT SELECT: expected query result".into())),
        };

        let mut count = 0u64;
        for source_row in &source_rows {
            if source_row.values.len() != columns.len() {
                return Err(CedarError::Internal(format!(
                    "INSERT SELECT: column count mismatch ({} vs {})",
                    columns.len(), source_row.values.len()
                )));
            }
            let mut values = vec![Datum::Null; schema.num_columns()];
            for (i, val) in source_row.values.iter().enumerate() {
                values[columns[i]] = val.clone();
            }
            let row = OwnedRow::new(values);
            self.storage.insert(table_id, row, txn.txn_id)?;
            count += 1;
        }

        Ok(ExecutionResult::Dml {
            rows_affected: count,
            tag: "INSERT".into(),
        })
    }

    pub(crate) fn exec_update(
        &self,
        table_id: cedar_common::types::TableId,
        schema: &TableSchema,
        assignments: &[(usize, BoundExpr)],
        filter: Option<&BoundExpr>,
        returning: &[(BoundExpr, String)],
        from_table: Option<&BoundFromTable>,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, CedarError> {
        // Multi-table UPDATE ... FROM: nested loop join approach
        if let Some(ft) = from_table {
            return self.exec_update_from(table_id, schema, assignments, filter, returning, ft, txn);
        }

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let (rows, effective_filter) = if let Some((pk, remaining)) = self.try_pk_point_lookup(filter, schema) {
            match self.storage.get(table_id, &pk, txn.txn_id, read_ts)? {
                Some(row) => (vec![(pk, row)], remaining),
                None => (vec![], remaining),
            }
        } else if let Some((col_idx, key, remaining)) = self.try_index_scan_predicate(filter, table_id) {
            (self.storage.index_scan(table_id, col_idx, &key, txn.txn_id, read_ts)?, remaining)
        } else {
            (self.storage.scan(table_id, txn.txn_id, read_ts)?, filter.cloned())
        };
        let mut count = 0u64;
        let mat_filter = self.materialize_filter(effective_filter.as_ref(), txn)?;
        let mut returning_rows = Vec::new();

        for (pk, row) in &rows {
            // Apply filter
            if let Some(ref f) = mat_filter {
                if !ExprEngine::eval_filter(f, row).map_err(CedarError::Execution)? {
                    continue;
                }
            }

            // Apply assignments
            let mut new_values = row.values.clone();
            for (col_idx, expr) in assignments {
                let val = ExprEngine::eval_row(expr, row).map_err(CedarError::Execution)?;
                new_values[*col_idx] = val;
            }

            // Enforce NOT NULL constraints
            for (i, val) in new_values.iter().enumerate() {
                if i < schema.columns.len() && val.is_null() && !schema.columns[i].nullable {
                    return Err(CedarError::Execution(ExecutionError::TypeError(
                        format!("NULL value in column '{}' violates NOT NULL constraint", schema.columns[i].name),
                    )));
                }
            }

            // Enforce CHECK constraints
            let check_row = OwnedRow::new(new_values.clone());
            self.eval_check_constraints(schema, &check_row)?;

            // Enforce UNIQUE constraints on updated row
            if !schema.unique_constraints.is_empty() {
                // Need all rows for uniqueness check (not just the narrowed scan result)
                let all_rows = self.storage.scan(table_id, txn.txn_id, read_ts)?;
                for uniq_cols in &schema.unique_constraints {
                    let new_vals: Vec<&Datum> = uniq_cols.iter().map(|&idx| &new_values[idx]).collect();
                    if new_vals.iter().any(|v| v.is_null()) {
                        continue;
                    }
                    for (other_pk, other_row) in &all_rows {
                        if other_pk == pk { continue; } // skip self
                        let other_vals: Vec<&Datum> = uniq_cols.iter().map(|&idx| &other_row.values[idx]).collect();
                        if new_vals == other_vals {
                            let col_names: Vec<&str> = uniq_cols.iter().map(|&idx| schema.columns[idx].name.as_str()).collect();
                            return Err(CedarError::Execution(ExecutionError::TypeError(
                                format!("UNIQUE constraint violated on column(s): {}", col_names.join(", ")),
                            )));
                        }
                    }
                }
            }

            // Apply FK cascading actions for child tables if referenced columns changed
            self.apply_fk_on_update(schema, row, &new_values, txn)?;

            if !returning.is_empty() {
                let ret_row = OwnedRow::new(new_values.clone());
                let ret_vals: Vec<Datum> = returning.iter().map(|(expr, _)| {
                    ExprEngine::eval_row(expr, &ret_row).unwrap_or(Datum::Null)
                }).collect();
                returning_rows.push(ret_vals);
            }

            let new_row = OwnedRow::new(new_values);
            self.storage.update(table_id, pk, new_row, txn.txn_id)?;
            count += 1;
        }

        if !returning.is_empty() {
            let columns: Vec<(String, cedar_common::types::DataType)> = returning.iter().map(|(expr, alias)| {
                let dt = if let BoundExpr::ColumnRef(idx) = expr {
                    schema.columns.get(*idx).map(|c| c.data_type.clone()).unwrap_or(DataType::Text)
                } else {
                    DataType::Text
                };
                (alias.clone(), dt)
            }).collect();
            let rows = returning_rows.into_iter().map(OwnedRow::new).collect();
            Ok(ExecutionResult::Query { columns, rows })
        } else {
            Ok(ExecutionResult::Dml {
                rows_affected: count,
                tag: "UPDATE".into(),
            })
        }
    }

    pub(crate) fn exec_delete(
        &self,
        table_id: cedar_common::types::TableId,
        schema: &TableSchema,
        filter: Option<&BoundExpr>,
        returning: &[(BoundExpr, String)],
        using_table: Option<&BoundFromTable>,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, CedarError> {
        // Multi-table DELETE ... USING: nested loop join approach
        if let Some(ut) = using_table {
            return self.exec_delete_using(table_id, schema, filter, returning, ut, txn);
        }

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let (rows, effective_filter) = if let Some((pk, remaining)) = self.try_pk_point_lookup(filter, schema) {
            match self.storage.get(table_id, &pk, txn.txn_id, read_ts)? {
                Some(row) => (vec![(pk, row)], remaining),
                None => (vec![], remaining),
            }
        } else if let Some((col_idx, key, remaining)) = self.try_index_scan_predicate(filter, table_id) {
            (self.storage.index_scan(table_id, col_idx, &key, txn.txn_id, read_ts)?, remaining)
        } else {
            (self.storage.scan(table_id, txn.txn_id, read_ts)?, filter.cloned())
        };
        let mut count = 0u64;
        let mat_filter = self.materialize_filter(effective_filter.as_ref(), txn)?;
        let mut returning_rows = Vec::new();

        for (pk, row) in &rows {
            if let Some(ref f) = mat_filter {
                if !ExprEngine::eval_filter(f, row).map_err(CedarError::Execution)? {
                    continue;
                }
            }

            if !returning.is_empty() {
                let ret_vals: Vec<Datum> = returning.iter().map(|(expr, _)| {
                    ExprEngine::eval_row(expr, row).unwrap_or(Datum::Null)
                }).collect();
                returning_rows.push(ret_vals);
            }

            // Apply FK cascading actions for child tables referencing this row
            self.apply_fk_on_delete(schema, row, txn)?;

            self.storage.delete(table_id, pk, txn.txn_id)?;
            count += 1;
        }

        if !returning.is_empty() {
            let columns: Vec<(String, cedar_common::types::DataType)> = returning.iter().map(|(expr, alias)| {
                let dt = if let BoundExpr::ColumnRef(idx) = expr {
                    schema.columns.get(*idx).map(|c| c.data_type.clone()).unwrap_or(DataType::Text)
                } else {
                    DataType::Text
                };
                (alias.clone(), dt)
            }).collect();
            let rows = returning_rows.into_iter().map(OwnedRow::new).collect();
            Ok(ExecutionResult::Query { columns, rows })
        } else {
            Ok(ExecutionResult::Dml {
                rows_affected: count,
                tag: "DELETE".into(),
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
    ) -> Result<(), CedarError> {
        use cedar_common::schema::FkAction;

        let catalog = self.storage.get_catalog();
        let parent_name_lower = parent_schema.name.to_lowercase();

        for child_table in catalog.list_tables() {
            for fk in &child_table.foreign_keys {
                if fk.ref_table.to_lowercase() != parent_name_lower {
                    continue;
                }

                // Resolve referenced column indices in parent
                let ref_col_indices: Vec<usize> = fk.ref_columns.iter()
                    .filter_map(|name| parent_schema.find_column(name))
                    .collect();
                if ref_col_indices.len() != fk.ref_columns.len() {
                    continue; // skip malformed FK
                }

                // Get the parent row's referenced values
                let parent_vals: Vec<&Datum> = ref_col_indices.iter()
                    .map(|&idx| &deleted_row.values[idx])
                    .collect();

                // Scan child table for matching rows
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                let child_rows = self.storage.scan(child_table.id, txn.txn_id, read_ts)?;

                for (child_pk, child_row) in &child_rows {
                    let child_vals: Vec<&Datum> = fk.columns.iter()
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
                            self.apply_fk_on_delete(child_table, child_row, txn)?;
                            self.storage.delete(child_table.id, child_pk, txn.txn_id)?;
                        }
                        FkAction::SetNull => {
                            let mut new_values = child_row.values.clone();
                            for &col_idx in &fk.columns {
                                new_values[col_idx] = Datum::Null;
                            }
                            let new_row = OwnedRow::new(new_values);
                            self.storage.update(child_table.id, child_pk, new_row, txn.txn_id)?;
                        }
                        FkAction::SetDefault => {
                            let mut new_values = child_row.values.clone();
                            for &col_idx in &fk.columns {
                                new_values[col_idx] = child_table.columns[col_idx]
                                    .default_value.clone().unwrap_or(Datum::Null);
                            }
                            let new_row = OwnedRow::new(new_values);
                            self.storage.update(child_table.id, child_pk, new_row, txn.txn_id)?;
                        }
                        FkAction::Restrict | FkAction::NoAction => {
                            return Err(CedarError::Execution(ExecutionError::TypeError(
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
    ) -> Result<(), CedarError> {
        use cedar_common::schema::FkAction;

        let catalog = self.storage.get_catalog();
        let parent_name_lower = parent_schema.name.to_lowercase();

        for child_table in catalog.list_tables() {
            for fk in &child_table.foreign_keys {
                if fk.ref_table.to_lowercase() != parent_name_lower {
                    continue;
                }

                let ref_col_indices: Vec<usize> = fk.ref_columns.iter()
                    .filter_map(|name| parent_schema.find_column(name))
                    .collect();
                if ref_col_indices.len() != fk.ref_columns.len() {
                    continue;
                }

                // Check if any referenced column actually changed
                let old_vals: Vec<&Datum> = ref_col_indices.iter()
                    .map(|&idx| &old_row.values[idx])
                    .collect();
                let new_ref_vals: Vec<&Datum> = ref_col_indices.iter()
                    .map(|&idx| &new_values[idx])
                    .collect();
                if old_vals == new_ref_vals {
                    continue; // no change in referenced columns
                }

                // Scan child table for rows matching old values
                let read_ts = txn.read_ts(self.txn_mgr.current_ts());
                let child_rows = self.storage.scan(child_table.id, txn.txn_id, read_ts)?;

                for (child_pk, child_row) in &child_rows {
                    let child_vals: Vec<&Datum> = fk.columns.iter()
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
                            let new_row = OwnedRow::new(updated_child);
                            self.storage.update(child_table.id, child_pk, new_row, txn.txn_id)?;
                        }
                        FkAction::SetNull => {
                            let mut updated_child = child_row.values.clone();
                            for &fk_col in &fk.columns {
                                updated_child[fk_col] = Datum::Null;
                            }
                            let new_row = OwnedRow::new(updated_child);
                            self.storage.update(child_table.id, child_pk, new_row, txn.txn_id)?;
                        }
                        FkAction::SetDefault => {
                            let mut updated_child = child_row.values.clone();
                            for &fk_col in &fk.columns {
                                updated_child[fk_col] = child_table.columns[fk_col]
                                    .default_value.clone().unwrap_or(Datum::Null);
                            }
                            let new_row = OwnedRow::new(updated_child);
                            self.storage.update(child_table.id, child_pk, new_row, txn.txn_id)?;
                        }
                        FkAction::Restrict | FkAction::NoAction => {
                            return Err(CedarError::Execution(ExecutionError::TypeError(
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
        table_id: cedar_common::types::TableId,
        schema: &TableSchema,
        assignments: &[(usize, BoundExpr)],
        filter: Option<&BoundExpr>,
        returning: &[(BoundExpr, String)],
        ft: &BoundFromTable,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, CedarError> {
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let target_rows = self.storage.scan(table_id, txn.txn_id, read_ts)?;
        let from_rows = self.storage.scan(ft.table_id, txn.txn_id, read_ts)?;
        let mat_filter = self.materialize_filter(filter, txn)?;

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
                    if !ExprEngine::eval_filter(f, &combined_row).map_err(CedarError::Execution)? {
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
                    let val = ExprEngine::eval_row(expr, &combined_row).map_err(CedarError::Execution)?;
                    new_values[*col_idx] = val;
                }

                // Enforce NOT NULL constraints
                for (i, val) in new_values.iter().enumerate() {
                    if i < schema.columns.len() && val.is_null() && !schema.columns[i].nullable {
                        return Err(CedarError::Execution(ExecutionError::TypeError(
                            format!("NULL value in column '{}' violates NOT NULL constraint", schema.columns[i].name),
                        )));
                    }
                }

                // Enforce CHECK constraints
                let check_row = OwnedRow::new(new_values.clone());
                self.eval_check_constraints(schema, &check_row)?;

                // FK cascading on update
                self.apply_fk_on_update(schema, target_row, &new_values, txn)?;

                if !returning.is_empty() {
                    let ret_row = OwnedRow::new(new_values.clone());
                    let ret_vals: Vec<Datum> = returning.iter().map(|(expr, _)| {
                        ExprEngine::eval_row(expr, &ret_row).unwrap_or(Datum::Null)
                    }).collect();
                    returning_rows.push(ret_vals);
                }

                let new_row = OwnedRow::new(new_values);
                self.storage.update(table_id, pk, new_row, txn.txn_id)?;
                count += 1;
                break; // move to next target row after first match
            }
        }

        if !returning.is_empty() {
            let columns: Vec<(String, DataType)> = returning.iter().map(|(expr, alias)| {
                let dt = if let BoundExpr::ColumnRef(idx) = expr {
                    schema.columns.get(*idx).map(|c| c.data_type.clone()).unwrap_or(DataType::Text)
                } else {
                    DataType::Text
                };
                (alias.clone(), dt)
            }).collect();
            let rows = returning_rows.into_iter().map(OwnedRow::new).collect();
            Ok(ExecutionResult::Query { columns, rows })
        } else {
            Ok(ExecutionResult::Dml {
                rows_affected: count,
                tag: "UPDATE".into(),
            })
        }
    }

    /// DELETE ... USING: nested-loop join between target table and USING table.
    /// For each (target_row, using_row) pair where the filter matches the combined row,
    /// delete the target_row.
    fn exec_delete_using(
        &self,
        table_id: cedar_common::types::TableId,
        schema: &TableSchema,
        filter: Option<&BoundExpr>,
        returning: &[(BoundExpr, String)],
        ut: &BoundFromTable,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, CedarError> {
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
                    if !ExprEngine::eval_filter(f, &combined_row).map_err(CedarError::Execution)? {
                        continue;
                    }
                }

                // Avoid deleting the same target row multiple times
                if !deleted_pks.insert(pk.clone()) {
                    continue;
                }

                if !returning.is_empty() {
                    let ret_vals: Vec<Datum> = returning.iter().map(|(expr, _)| {
                        ExprEngine::eval_row(expr, target_row).unwrap_or(Datum::Null)
                    }).collect();
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
            let columns: Vec<(String, DataType)> = returning.iter().map(|(expr, alias)| {
                let dt = if let BoundExpr::ColumnRef(idx) = expr {
                    schema.columns.get(*idx).map(|c| c.data_type.clone()).unwrap_or(DataType::Text)
                } else {
                    DataType::Text
                };
                (alias.clone(), dt)
            }).collect();
            let rows = returning_rows.into_iter().map(OwnedRow::new).collect();
            Ok(ExecutionResult::Query { columns, rows })
        } else {
            Ok(ExecutionResult::Dml {
                rows_affected: count,
                tag: "DELETE".into(),
            })
        }
    }

}

/// Map a DataType to the cast target string used by eval_cast.
fn datatype_to_cast_target(dt: &DataType) -> String {
    match dt {
        DataType::Int32 => "int".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float64 => "float".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Text => "text".into(),
        DataType::Timestamp => "timestamp".into(),
        DataType::Date => "date".into(),
        DataType::Jsonb => "jsonb".into(),
        DataType::Array(_) => "array".into(),
    }
}
