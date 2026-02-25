//! Coordinator-side execution: join gathering, subquery materialization, plan walking.

use std::sync::Arc;

use falcon_common::datum::OwnedRow;
use falcon_common::error::FalconError;
use falcon_common::types::IsolationLevel;
use falcon_executor::executor::{ExecutionResult, Executor};
use falcon_planner::plan::PhysicalPlan;
use falcon_sql_frontend::types::BoundExpr;
use falcon_storage::engine::StorageEngine;
use falcon_txn::{TxnHandle, TxnManager};

impl super::DistributedQueryEngine {
    /// Coordinator-side join: gather all table data from all shards into a
    /// temporary in-memory StorageEngine, then execute the join locally.
    /// This ensures cross-shard correctness — no missing matches when
    /// left and right rows reside on different shards.
    pub(crate) fn exec_coordinator_join(
        &self,
        plan: &PhysicalPlan,
        _txn: Option<&TxnHandle>,
    ) -> Result<ExecutionResult, FalconError> {
        let (left_table_id, joins) = match plan {
            PhysicalPlan::NestedLoopJoin {
                left_table_id,
                joins,
                ..
            }
            | PhysicalPlan::HashJoin {
                left_table_id,
                joins,
                ..
            } => (*left_table_id, joins),
            _ => {
                return Err(FalconError::internal_bug(
                    "E-QE-007",
                    "exec_coordinator_join called on non-join plan",
                    "plan type mismatch",
                ))
            }
        };
        let mut table_ids = vec![left_table_id];
        for join in joins {
            table_ids.push(join.right_table_id);
        }
        self.gather_and_execute_locally(&table_ids, plan)
    }

    /// Coordinator-side execution for SeqScan with subqueries/CTEs/UNIONs.
    /// Gathers all referenced table data from all shards into a temp engine,
    /// then executes locally. This ensures subqueries see all data across shards.
    pub(crate) fn exec_coordinator_subquery(
        &self,
        plan: &PhysicalPlan,
        _txn: Option<&TxnHandle>,
    ) -> Result<ExecutionResult, FalconError> {
        let table_ids = Self::extract_table_ids(plan);
        self.gather_and_execute_locally(&table_ids, plan)
    }

    /// Shared coordinator-side execution: gather data for given tables from
    /// all shards into a temp in-memory StorageEngine, then execute the plan.
    pub(crate) fn gather_and_execute_locally(
        &self,
        table_ids: &[falcon_common::types::TableId],
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        let temp_storage = Arc::new(StorageEngine::new_in_memory());
        let temp_txn_mgr = Arc::new(TxnManager::new(temp_storage.clone()));
        self.populate_temp_storage(table_ids, &temp_storage, &temp_txn_mgr)?;
        let temp_exec = Executor::new(temp_storage, temp_txn_mgr.clone());
        let temp_txn = temp_txn_mgr.begin(IsolationLevel::ReadCommitted);
        temp_exec.execute(plan, Some(&temp_txn))
    }

    /// Gather rows for the given tables from all shards into `temp_storage`.
    /// Shared by `gather_and_execute_locally` and `materialize_dml_filter`.
    fn populate_temp_storage(
        &self,
        table_ids: &[falcon_common::types::TableId],
        temp_storage: &Arc<StorageEngine>,
        temp_txn_mgr: &Arc<TxnManager>,
    ) -> Result<(), FalconError> {
        let shard_ids = self.engine.shard_ids();
        for &table_id in table_ids {
            let schema = shard_ids
                .iter()
                .find_map(|sid| {
                    let s = self.engine.shard(*sid)?;
                    s.storage.get_table_schema_by_id(table_id)
                })
                .ok_or_else(|| {
                    FalconError::Internal(format!("Table {table_id:?} not found on any shard"))
                })?;

            temp_storage.create_table(schema.clone()).map_err(|e| {
                FalconError::Internal(format!("Failed to create temp table: {e:?}"))
            })?;

            // Parallel gather from all shards.
            let mut per_shard: Vec<Vec<OwnedRow>> = Vec::with_capacity(shard_ids.len());
            std::thread::scope(|scope| {
                let handles: Vec<_> = shard_ids
                    .iter()
                    .filter_map(|sid| {
                        let shard = self.engine.shard(*sid)?;
                        Some(scope.spawn(move || {
                            let shard_txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
                            let read_ts = shard_txn.read_ts(shard.txn_mgr.current_ts());
                            let rows = shard
                                .storage
                                .scan(table_id, shard_txn.txn_id, read_ts)
                                .unwrap_or_default();
                            let _ = shard.txn_mgr.commit(shard_txn.txn_id);
                            rows.into_iter().map(|(_, r)| r).collect::<Vec<_>>()
                        }))
                    })
                    .collect();
                for h in handles {
                    if let Ok(rows) = h.join() {
                        per_shard.push(rows);
                    }
                }
            });

            let temp_txn = temp_txn_mgr.begin(IsolationLevel::ReadCommitted);
            for row in per_shard.into_iter().flatten() {
                temp_storage
                    .insert(table_id, row, temp_txn.txn_id)
                    .map_err(|e| {
                        FalconError::Internal(format!("Failed to insert into temp storage: {e:?}"))
                    })?;
            }
            temp_txn_mgr.commit(temp_txn.txn_id).map_err(|e| {
                FalconError::Internal(format!("Failed to commit temp txn: {e:?}"))
            })?;
        }
        Ok(())
    }

    /// Materialize subqueries in a DML filter at coordinator level.
    /// Gathers all subquery-referenced table data from all shards into a temp engine,
    /// then uses the Executor's materialize_filter to replace subquery nodes with
    /// concrete values. Returns the materialized filter expression.
    pub(crate) fn materialize_dml_filter(&self, filter: &BoundExpr) -> Result<BoundExpr, FalconError> {
        // Collect table IDs referenced by subqueries in the filter
        let mut ids = std::collections::HashSet::new();
        Self::collect_table_ids_from_expr(filter, &mut ids);

        let temp_storage = Arc::new(StorageEngine::new_in_memory());
        let temp_txn_mgr = Arc::new(TxnManager::new(temp_storage.clone()));
        let ids_vec: Vec<_> = ids.into_iter().collect();
        self.populate_temp_storage(&ids_vec, &temp_storage, &temp_txn_mgr)?;
        // Use a temp Executor to materialize subqueries with full cross-shard data
        let temp_exec = Executor::new(temp_storage, temp_txn_mgr.clone());
        let temp_txn = temp_txn_mgr.begin(IsolationLevel::ReadCommitted);
        temp_exec.materialize_subqueries(filter, &temp_txn)
    }

    // ── Plan walking: extract TableIds and detect subqueries ──────────────

    /// Extract all TableIds referenced in a plan (main table + subquery/CTE/UNION tables).
    #[inline]
    pub(crate) fn extract_table_ids(plan: &PhysicalPlan) -> Vec<falcon_common::types::TableId> {
        use std::collections::HashSet;

        let mut ids = HashSet::new();
        Self::collect_table_ids_from_plan(plan, &mut ids);
        ids.into_iter().collect()
    }

    fn collect_table_ids_from_plan(
        plan: &PhysicalPlan,
        ids: &mut std::collections::HashSet<falcon_common::types::TableId>,
    ) {
        match plan {
            PhysicalPlan::SeqScan {
                table_id,
                filter,
                projections,
                having,
                ctes,
                unions,
                ..
            } => {
                ids.insert(*table_id);
                if let Some(f) = filter {
                    Self::collect_table_ids_from_expr(f, ids);
                }
                if let Some(h) = having {
                    Self::collect_table_ids_from_expr(h, ids);
                }
                for proj in projections {
                    match proj {
                        falcon_sql_frontend::types::BoundProjection::Expr(e, _)
                        | falcon_sql_frontend::types::BoundProjection::Aggregate(
                            _,
                            Some(e),
                            _,
                            _,
                            _,
                        ) => {
                            Self::collect_table_ids_from_expr(e, ids);
                        }
                        _ => {}
                    }
                }
                for cte in ctes {
                    ids.insert(cte.table_id);
                    Self::collect_table_ids_from_select(&cte.select, ids);
                }
                for (union_sel, _, _) in unions {
                    Self::collect_table_ids_from_select(union_sel, ids);
                }
            }
            PhysicalPlan::NestedLoopJoin {
                left_table_id,
                joins,
                ..
            }
            | PhysicalPlan::HashJoin {
                left_table_id,
                joins,
                ..
            } => {
                ids.insert(*left_table_id);
                for join in joins {
                    ids.insert(join.right_table_id);
                }
            }
            _ => {}
        }
    }

    fn collect_table_ids_from_select(
        sel: &falcon_sql_frontend::types::BoundSelect,
        ids: &mut std::collections::HashSet<falcon_common::types::TableId>,
    ) {
        ids.insert(sel.table_id);
        if let Some(f) = &sel.filter {
            Self::collect_table_ids_from_expr(f, ids);
        }
        for join in &sel.joins {
            ids.insert(join.right_table_id);
        }
        for cte in &sel.ctes {
            ids.insert(cte.table_id);
            Self::collect_table_ids_from_select(&cte.select, ids);
        }
        for (union_sel, _, _) in &sel.unions {
            Self::collect_table_ids_from_select(union_sel, ids);
        }
    }

    pub(crate) fn collect_table_ids_from_expr(
        expr: &BoundExpr,
        ids: &mut std::collections::HashSet<falcon_common::types::TableId>,
    ) {
        match expr {
            BoundExpr::ScalarSubquery(sel) => {
                Self::collect_table_ids_from_select(sel, ids);
            }
            BoundExpr::InSubquery { expr, subquery, .. } => {
                Self::collect_table_ids_from_expr(expr, ids);
                Self::collect_table_ids_from_select(subquery, ids);
            }
            BoundExpr::Exists { subquery, .. } => {
                Self::collect_table_ids_from_select(subquery, ids);
            }
            BoundExpr::BinaryOp { left, right, .. }
            | BoundExpr::IsNotDistinctFrom { left, right }
            | BoundExpr::AnyOp { left, right, .. }
            | BoundExpr::AllOp { left, right, .. } => {
                Self::collect_table_ids_from_expr(left, ids);
                Self::collect_table_ids_from_expr(right, ids);
            }
            BoundExpr::Not(inner)
            | BoundExpr::IsNull(inner)
            | BoundExpr::IsNotNull(inner)
            | BoundExpr::Cast { expr: inner, .. } => {
                Self::collect_table_ids_from_expr(inner, ids);
            }
            BoundExpr::Between {
                expr, low, high, ..
            } => {
                Self::collect_table_ids_from_expr(expr, ids);
                Self::collect_table_ids_from_expr(low, ids);
                Self::collect_table_ids_from_expr(high, ids);
            }
            BoundExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(op) = operand {
                    Self::collect_table_ids_from_expr(op, ids);
                }
                for c in conditions {
                    Self::collect_table_ids_from_expr(c, ids);
                }
                for r in results {
                    Self::collect_table_ids_from_expr(r, ids);
                }
                if let Some(el) = else_result {
                    Self::collect_table_ids_from_expr(el, ids);
                }
            }
            BoundExpr::InList { expr, list, .. } => {
                Self::collect_table_ids_from_expr(expr, ids);
                for e in list {
                    Self::collect_table_ids_from_expr(e, ids);
                }
            }
            BoundExpr::Function { args: exprs, .. }
            | BoundExpr::Coalesce(exprs)
            | BoundExpr::ArrayLiteral(exprs) => {
                for e in exprs {
                    Self::collect_table_ids_from_expr(e, ids);
                }
            }
            BoundExpr::Like { expr, pattern, .. } => {
                Self::collect_table_ids_from_expr(expr, ids);
                Self::collect_table_ids_from_expr(pattern, ids);
            }
            BoundExpr::ArrayIndex { array, index } => {
                Self::collect_table_ids_from_expr(array, ids);
                Self::collect_table_ids_from_expr(index, ids);
            }
            BoundExpr::ArraySlice {
                array,
                lower,
                upper,
            } => {
                Self::collect_table_ids_from_expr(array, ids);
                if let Some(e) = lower {
                    Self::collect_table_ids_from_expr(e, ids);
                }
                if let Some(e) = upper {
                    Self::collect_table_ids_from_expr(e, ids);
                }
            }
            BoundExpr::AggregateExpr {
                arg: Some(inner), ..
            } => {
                Self::collect_table_ids_from_expr(inner, ids);
            }
            BoundExpr::ColumnRef(_)
            | BoundExpr::Literal(_)
            | BoundExpr::OuterColumnRef(_)
            | BoundExpr::AggregateExpr { arg: None, .. }
            | BoundExpr::SequenceNextval(_)
            | BoundExpr::SequenceCurrval(_)
            | BoundExpr::SequenceSetval(_, _)
            | BoundExpr::Parameter(_)
            | BoundExpr::Grouping(_) => {}
        }
    }

    /// Check if a BoundExpr contains any subquery node (InSubquery, Exists, ScalarSubquery).
    #[inline]
    pub(crate) fn expr_has_subquery(expr: &BoundExpr) -> bool {
        match expr {
            BoundExpr::ScalarSubquery(_)
            | BoundExpr::InSubquery { .. }
            | BoundExpr::Exists { .. } => true,
            BoundExpr::BinaryOp { left, right, .. }
            | BoundExpr::IsNotDistinctFrom { left, right }
            | BoundExpr::AnyOp { left, right, .. }
            | BoundExpr::AllOp { left, right, .. } => {
                Self::expr_has_subquery(left) || Self::expr_has_subquery(right)
            }
            BoundExpr::Not(inner)
            | BoundExpr::IsNull(inner)
            | BoundExpr::IsNotNull(inner)
            | BoundExpr::Cast { expr: inner, .. } => Self::expr_has_subquery(inner),
            BoundExpr::Like { expr, pattern, .. } => {
                Self::expr_has_subquery(expr) || Self::expr_has_subquery(pattern)
            }
            BoundExpr::Between {
                expr, low, high, ..
            } => {
                Self::expr_has_subquery(expr)
                    || Self::expr_has_subquery(low)
                    || Self::expr_has_subquery(high)
            }
            BoundExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                operand.as_deref().is_some_and(Self::expr_has_subquery)
                    || conditions.iter().any(Self::expr_has_subquery)
                    || results.iter().any(Self::expr_has_subquery)
                    || else_result.as_deref().is_some_and(Self::expr_has_subquery)
            }
            BoundExpr::InList { expr, list, .. } => {
                Self::expr_has_subquery(expr) || list.iter().any(Self::expr_has_subquery)
            }
            BoundExpr::Function { args: exprs, .. }
            | BoundExpr::Coalesce(exprs)
            | BoundExpr::ArrayLiteral(exprs) => exprs.iter().any(Self::expr_has_subquery),
            BoundExpr::ArrayIndex { array, index } => {
                Self::expr_has_subquery(array) || Self::expr_has_subquery(index)
            }
            BoundExpr::ArraySlice {
                array,
                lower,
                upper,
            } => {
                Self::expr_has_subquery(array)
                    || lower.as_deref().is_some_and(Self::expr_has_subquery)
                    || upper.as_deref().is_some_and(Self::expr_has_subquery)
            }
            BoundExpr::AggregateExpr {
                arg: Some(inner), ..
            } => Self::expr_has_subquery(inner),
            BoundExpr::ColumnRef(_)
            | BoundExpr::Literal(_)
            | BoundExpr::OuterColumnRef(_)
            | BoundExpr::AggregateExpr { arg: None, .. }
            | BoundExpr::SequenceNextval(_)
            | BoundExpr::SequenceCurrval(_)
            | BoundExpr::SequenceSetval(_, _)
            | BoundExpr::Parameter(_)
            | BoundExpr::Grouping(_) => false,
        }
    }
}
