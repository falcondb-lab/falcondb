#![allow(clippy::too_many_arguments)]

use cedar_common::error::CedarError;
use cedar_sql_frontend::types::*;
use cedar_txn::TxnHandle;

use crate::executor::{CteData, ExecutionResult, Executor};

impl Executor {
    /// Execute UNION [ALL] by running each additional query and concatenating results.
    pub(crate) fn exec_union(
        &self,
        mut base_result: ExecutionResult,
        unions: &[(BoundSelect, SetOpKind, bool)],
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, CedarError> {
        let (columns, base_rows) = match &mut base_result {
            ExecutionResult::Query { columns, rows } => {
                (std::mem::take(columns), std::mem::take(rows))
            }
            _ => return Err(CedarError::Internal("Set operation base must be query result".into())),
        };

        let mut all_rows = base_rows;

        for (union_sel, set_op, is_all) in unions {
            let union_result = if union_sel.joins.is_empty() {
                self.exec_seq_scan(
                    union_sel.table_id,
                    &union_sel.schema,
                    &union_sel.projections,
                    union_sel.visible_projection_count,
                    union_sel.filter.as_ref(),
                    &union_sel.group_by,
                    &union_sel.grouping_sets,
                    union_sel.having.as_ref(),
                    &union_sel.order_by,
                    union_sel.limit,
                    union_sel.offset,
                    &union_sel.distinct,
                    txn,
                    &CteData::new(),
                    &union_sel.virtual_rows,
                )?
            } else {
                self.exec_nested_loop_join(
                    union_sel.table_id,
                    &union_sel.schema,
                    &union_sel.joins,
                    &union_sel.schema,
                    &union_sel.projections,
                    union_sel.visible_projection_count,
                    union_sel.filter.as_ref(),
                    &union_sel.order_by,
                    union_sel.limit,
                    union_sel.offset,
                    &union_sel.distinct,
                    txn,
                    &CteData::new(),
                )?
            };

            let right_rows = match union_result {
                ExecutionResult::Query { rows, .. } => rows,
                _ => Vec::new(),
            };

            match set_op {
                SetOpKind::Union => {
                    all_rows.extend(right_rows);
                    if !is_all {
                        self.dedup_rows(&mut all_rows);
                    }
                }
                SetOpKind::Intersect => {
                    // Keep only rows present in both sides
                    let right_set: std::collections::HashSet<String> =
                        right_rows.iter().map(|r| format!("{:?}", r.values)).collect();
                    all_rows.retain(|r| right_set.contains(&format!("{:?}", r.values)));
                    if !is_all {
                        self.dedup_rows(&mut all_rows);
                    }
                }
                SetOpKind::Except => {
                    // Remove rows that appear in the right side
                    let right_set: std::collections::HashSet<String> =
                        right_rows.iter().map(|r| format!("{:?}", r.values)).collect();
                    all_rows.retain(|r| !right_set.contains(&format!("{:?}", r.values)));
                    if !is_all {
                        self.dedup_rows(&mut all_rows);
                    }
                }
            }
        }

        Ok(ExecutionResult::Query {
            columns,
            rows: all_rows,
        })
    }

    /// Materialize CTE queries into row data.
    pub(crate) fn materialize_ctes(
        &self,
        ctes: &[BoundCte],
        txn: &TxnHandle,
    ) -> Result<CteData, CedarError> {
        let mut cte_data = CteData::new();
        for cte in ctes {
            // Recursive CTE: iterative fixpoint execution
            if let Some(ref recursive_sel) = cte.recursive_select {
                let base_rows = self.exec_cte_select(&cte.select, txn, &cte_data)?;
                let mut all_rows = base_rows.clone();
                let mut working_rows = base_rows;

                const MAX_ITERATIONS: usize = 1000;
                for _ in 0..MAX_ITERATIONS {
                    if working_rows.is_empty() {
                        break;
                    }
                    // Feed current working rows as CTE data for recursive reference
                    let mut recursive_cte_data = cte_data.clone();
                    recursive_cte_data.insert(cte.table_id, working_rows);

                    let new_rows = self.exec_cte_select(recursive_sel, txn, &recursive_cte_data)?;
                    if new_rows.is_empty() {
                        break;
                    }
                    all_rows.extend(new_rows.clone());
                    working_rows = new_rows;
                }
                cte_data.insert(cte.table_id, all_rows);
                continue;
            }

            let sel = &cte.select;
            // Virtual rows (GENERATE_SERIES etc.) — use inline data directly
            if !sel.virtual_rows.is_empty() {
                cte_data.insert(cte.table_id, sel.virtual_rows.clone());
                continue;
            }
            let rows = self.exec_cte_select(sel, txn, &CteData::new())?;
            cte_data.insert(cte.table_id, rows);
        }
        Ok(cte_data)
    }

    /// Execute a single CTE SELECT and return its rows.
    fn exec_cte_select(
        &self,
        sel: &BoundSelect,
        txn: &TxnHandle,
        cte_data: &CteData,
    ) -> Result<Vec<cedar_common::datum::OwnedRow>, CedarError> {
        let result = if sel.joins.is_empty() {
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
                cte_data,
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
                cte_data,
            )?
        };
        match result {
            ExecutionResult::Query { rows, .. } => Ok(rows),
            _ => Err(CedarError::Internal("CTE must produce query result".into())),
        }
    }
}
