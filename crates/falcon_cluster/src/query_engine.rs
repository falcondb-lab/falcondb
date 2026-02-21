//! Distributed query engine: dispatches PhysicalPlan execution.
//!
//! For local plans (SeqScan, Insert, etc.), delegates to the per-shard Executor.
//! For DistPlan, uses parallel scatter/gather across shards, where each shard
//! runs its own Executor with its own transaction.
//! For multi-shard writes, uses TwoPhaseCoordinator for atomic commits.

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use std::collections::{HashMap, HashSet};

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_common::types::{IsolationLevel, ShardId};
use falcon_executor::executor::{ExecutionResult, Executor};
use falcon_planner::plan::{DistAggMerge, DistGather, PhysicalPlan};
use falcon_sql_frontend::types::BoundExpr;
use falcon_storage::engine::StorageEngine;
use falcon_txn::{TxnHandle, TxnManager};

use crate::distributed_exec::{
    AggMerge, DistributedExecutor, ShardResult,
};
use crate::sharded_engine::ShardedEngine;
use crate::two_phase::TwoPhaseCoordinator;

/// Last scatter/gather execution metrics for observability.
#[derive(Debug, Clone, Default)]
pub struct ScatterStats {
    pub shards_participated: usize,
    pub total_rows_gathered: usize,
    pub per_shard_latency_us: Vec<(u64, u64)>,
    pub per_shard_row_count: Vec<(u64, usize)>,
    pub gather_strategy: String,
    pub total_latency_us: u64,
    pub failed_shards: Vec<u64>,
    /// Merge type labels for TwoPhaseAgg (e.g. "COUNT(col1)", "SUM(DISTINCT col2) [collect-dedup]")
    pub merge_labels: Vec<String>,
    /// Set when PK shard pruning routes a DistPlan to a single shard.
    pub pruned_to_shard: Option<u64>,
}

/// Distributed query engine that coordinates execution across shards.
///
/// - **Local plans**: dispatched to the appropriate shard's `Executor`.
/// - **DistPlan**: parallel scatter of subplan to each shard's Executor + gather/merge.
/// - **Multi-shard writes**: coordinated via `TwoPhaseCoordinator`.
pub struct DistributedQueryEngine {
    engine: Arc<ShardedEngine>,
    dist_exec: DistributedExecutor,
    two_pc: TwoPhaseCoordinator,
    timeout: Duration,
    last_scatter_stats: Mutex<ScatterStats>,
}

impl DistributedQueryEngine {
    pub fn new(engine: Arc<ShardedEngine>, timeout: Duration) -> Self {
        let dist_exec = DistributedExecutor::new(engine.clone(), timeout);
        let two_pc = TwoPhaseCoordinator::new(engine.clone(), timeout);
        Self {
            engine,
            dist_exec,
            two_pc,
            timeout,
            last_scatter_stats: Mutex::new(ScatterStats::default()),
        }
    }

    /// Get the last scatter/gather stats for observability.
    pub fn last_scatter_stats(&self) -> ScatterStats {
        self.last_scatter_stats.lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Health check: probe each shard with a trivial read transaction.
    /// Returns per-shard status: (shard_id, healthy, latency_us).
    pub fn shard_health_check(&self) -> Vec<(ShardId, bool, u64)> {
        let shard_ids: Vec<ShardId> = self.engine.all_shards().iter().map(|s| s.shard_id).collect();
        std::thread::scope(|s| {
            let handles: Vec<_> = shard_ids
                .iter()
                .map(|&sid| {
                    let engine = &self.engine;
                    s.spawn(move || {
                        let start = Instant::now();
                        match engine.shard(sid) {
                            Some(shard) => {
                                let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
                                let _ = shard.txn_mgr.commit(txn.txn_id);
                                (sid, true, start.elapsed().as_micros() as u64)
                            }
                            None => (sid, false, start.elapsed().as_micros() as u64),
                        }
                    })
                })
                .collect();
            handles.into_iter().filter_map(|h| h.join().ok()).collect()
        })
    }

    /// Execute a physical plan. Handles both local and distributed plans.
    ///
    /// Routing logic:
    /// - **DistPlan**: parallel scatter/gather across shards
    /// - **DDL** (CreateTable, DropTable, AlterTable, Truncate, CreateIndex, DropIndex):
    ///   propagated to ALL shards
    /// - **INSERT**: routed to the correct shard by PK hash (if extractable)
    /// - **Other**: executed on shard 0 (default)
    pub fn execute(
        &self,
        plan: &PhysicalPlan,
        txn: Option<&TxnHandle>,
    ) -> Result<ExecutionResult, FalconError> {
        match plan {
            // EXPLAIN wrapping a DistPlan: show plan + per-shard stats
            PhysicalPlan::Explain(inner) if matches!(**inner, PhysicalPlan::DistPlan { .. }) => {
                self.exec_explain_dist(inner)
            }

            // EXPLAIN wrapping a NestedLoopJoin/HashJoin: show coordinator-side join plan
            PhysicalPlan::Explain(inner) if matches!(**inner, PhysicalPlan::NestedLoopJoin { .. } | PhysicalPlan::HashJoin { .. }) => {
                self.exec_explain_coordinator_join(inner)
            }

            // EXPLAIN wrapping a bare SeqScan (subquery/CTE/UNION): show coordinator-side plan
            PhysicalPlan::Explain(inner) if matches!(**inner, PhysicalPlan::SeqScan { .. }) && self.engine.shard_ids().len() > 1 => {
                self.exec_explain_coordinator_subquery(inner)
            }

            // EXPLAIN ANALYZE: execute on shard 0 (local executor handles timing)
            PhysicalPlan::ExplainAnalyze(_) => {
                let shard = self.engine.shard(ShardId(0)).ok_or_else(|| {
                    FalconError::internal_bug("E-QE-001", "No shard 0 available", "ExplainAnalyze dispatch")
                })?;
                let local_exec = Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                local_exec.execute(plan, txn)
            }

            PhysicalPlan::DistPlan {
                subplan,
                target_shards,
                gather,
            } => self.exec_dist_plan(subplan, target_shards, gather),

            // DDL: propagate to all shards
            PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::AlterTable { .. }
            | PhysicalPlan::Truncate { .. }
            | PhysicalPlan::CreateIndex { .. }
            | PhysicalPlan::DropIndex { .. } => self.exec_ddl_all_shards(plan),

            // INSERT: split rows by target shard for correct placement.
            // Single-row inserts route directly; multi-row inserts are split
            // so each shard only receives its rows.
            PhysicalPlan::Insert { .. } => self.exec_insert_split(plan),

            // UPDATE: try to route by PK filter, otherwise broadcast to all shards.
            // If the filter contains subqueries, materialize them at coordinator level
            // first so each shard sees correct cross-shard subquery results.
            PhysicalPlan::Update { table_id, schema, assignments, filter, returning, from_table } => {
                if let Some(target_shard) = self.resolve_filter_shard(schema, filter.as_ref()) {
                    self.execute_on_shard_auto_txn(target_shard, plan)
                } else if let Some(f) = filter.as_ref().filter(|f| Self::expr_has_subquery(f)) {
                    let mat_filter = self.materialize_dml_filter(f)?;
                    let mat_plan = PhysicalPlan::Update {
                        table_id: *table_id,
                        schema: schema.clone(),
                        assignments: assignments.clone(),
                        filter: Some(mat_filter),
                        returning: returning.clone(),
                        from_table: from_table.clone(),
                    };
                    self.exec_dml_all_shards(&mat_plan)
                } else if let Some(pruned) = self.resolve_in_list_shards(schema, filter.as_ref()) {
                    self.exec_dml_on_shards(plan, &pruned)
                } else {
                    self.exec_dml_all_shards(plan)
                }
            }

            // DELETE: try to route by PK filter, otherwise broadcast to all shards.
            // If the filter contains subqueries, materialize them at coordinator level
            // first so each shard sees correct cross-shard subquery results.
            PhysicalPlan::Delete { table_id, schema, filter, returning, using_table } => {
                if let Some(target_shard) = self.resolve_filter_shard(schema, filter.as_ref()) {
                    self.execute_on_shard_auto_txn(target_shard, plan)
                } else if let Some(f) = filter.as_ref().filter(|f| Self::expr_has_subquery(f)) {
                    let mat_filter = self.materialize_dml_filter(f)?;
                    let mat_plan = PhysicalPlan::Delete {
                        table_id: *table_id,
                        schema: schema.clone(),
                        filter: Some(mat_filter),
                        returning: returning.clone(),
                        using_table: using_table.clone(),
                    };
                    self.exec_dml_all_shards(&mat_plan)
                } else if let Some(pruned) = self.resolve_in_list_shards(schema, filter.as_ref()) {
                    self.exec_dml_on_shards(plan, &pruned)
                } else {
                    self.exec_dml_all_shards(plan)
                }
            }

            // NestedLoopJoin: coordinator-side join — gather all table data
            // from all shards into a temp engine, then execute the join locally.
            // This ensures cross-shard correctness (no missing matches).
            PhysicalPlan::NestedLoopJoin { .. } | PhysicalPlan::HashJoin { .. } => self.exec_coordinator_join(plan, txn),

            // SeqScan that wasn't wrapped in DistPlan (has subqueries/CTEs/UNIONs):
            // needs coordinator-side execution to gather all referenced table data.
            PhysicalPlan::SeqScan { .. } if self.engine.shard_ids().len() > 1 => {
                self.exec_coordinator_subquery(plan, txn)
            }

            // RunGc: propagate to all shards
            PhysicalPlan::RunGc => self.exec_gc_all_shards(),

            // Everything else: execute on shard 0
            other => {
                let shard = self.engine.shard(ShardId(0)).ok_or_else(|| {
                    FalconError::internal_bug("E-QE-002", "No shard 0 available", "default plan dispatch")
                })?;
                let local_exec = Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                local_exec.execute(other, txn)
            }
        }
    }

    // ── Cross-region / lag-aware replica routing ─────────────────────────────

    /// Lag-aware replica routing: given a set of replica metrics, select the
    /// replica with the lowest `lag_lsn`. Returns `None` if no replicas are
    /// available (caller falls back to primary).
    ///
    /// Used for cross-region read routing: analytics/replica nodes that are
    /// caught up (lag_lsn == 0) are preferred; among lagging replicas the one
    /// with the smallest lag is chosen.
    pub fn select_least_lagging_replica(
        replicas: &[crate::replication::runner::ReplicaRunnerMetricsSnapshot],
    ) -> Option<&crate::replication::runner::ReplicaRunnerMetricsSnapshot> {
        replicas
            .iter()
            .filter(|r| r.connected)
            .min_by_key(|r| r.lag_lsn)
    }

    /// Route a read-only query to the least-lagging available replica shard,
    /// falling back to shard 0 (primary) if no replicas are available or all
    /// replicas exceed `max_lag_lsn`.
    ///
    /// `replica_metrics`: per-shard replica metrics (shard_id → metrics).
    /// `max_lag_lsn`: maximum acceptable lag in LSN units (0 = require fully caught up).
    pub fn route_read_to_replica(
        &self,
        plan: &PhysicalPlan,
        txn: Option<&TxnHandle>,
        replica_metrics: &[(ShardId, crate::replication::runner::ReplicaRunnerMetricsSnapshot)],
        max_lag_lsn: u64,
    ) -> Result<ExecutionResult, FalconError> {
        // Find the shard whose replica has the lowest lag within the threshold.
        let best = replica_metrics
            .iter()
            .filter(|(_, m)| m.connected && m.lag_lsn <= max_lag_lsn)
            .min_by_key(|(_, m)| m.lag_lsn)
            .map(|(sid, _)| *sid);

        let target_shard = best.unwrap_or(ShardId(0));

        tracing::debug!(
            shard_id = target_shard.0,
            "lag-aware routing: selected shard (max_lag_lsn={})",
            max_lag_lsn,
        );

        self.execute_on_shard(target_shard, plan, txn)
    }

    /// Execute a plan on a specific shard.
    pub fn execute_on_shard(
        &self,
        shard_id: ShardId,
        plan: &PhysicalPlan,
        txn: Option<&TxnHandle>,
    ) -> Result<ExecutionResult, FalconError> {
        let shard = self.engine.shard(shard_id).ok_or_else(|| {
            FalconError::internal_bug(
                "E-QE-003",
                format!("Shard {:?} not found", shard_id),
                "execute_on_shard dispatch",
            )
        })?;
        let local_exec = Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
        local_exec.execute(plan, txn)
    }

    /// Aggregate transaction statistics across all shards.
    pub fn aggregate_txn_stats(&self) -> falcon_txn::TxnStatsSnapshot {
        let mut agg = falcon_txn::TxnStatsSnapshot::default();
        for shard in self.engine.all_shards() {
            let snap = shard.txn_mgr.stats_snapshot();
            agg.total_committed += snap.total_committed;
            agg.fast_path_commits += snap.fast_path_commits;
            agg.slow_path_commits += snap.slow_path_commits;
            agg.total_aborted += snap.total_aborted;
            agg.occ_conflicts += snap.occ_conflicts;
            agg.degraded_to_global += snap.degraded_to_global;
            agg.constraint_violations += snap.constraint_violations;
            agg.active_count += snap.active_count;
        }
        agg
    }

    /// Per-shard statistics for observability (SHOW falcon.shard_stats).
    /// Returns: Vec<(shard_id, table_count, committed, aborted, active)>
    pub fn per_shard_stats(&self) -> Vec<(u64, usize, u64, u64, usize)> {
        self.engine
            .all_shards()
            .iter()
            .enumerate()
            .map(|(i, shard)| {
                let snap = shard.txn_mgr.stats_snapshot();
                let table_count = shard.storage.get_catalog().table_count();
                (i as u64, table_count, snap.total_committed, snap.total_aborted, snap.active_count)
            })
            .collect()
    }

    /// Per-shard detailed load with per-table breakdown (SHOW falcon.shards).
    pub fn shard_load_detailed(&self) -> Vec<crate::rebalancer::ShardLoadDetailed> {
        crate::rebalancer::ShardLoadDetailed::collect_all(&self.engine)
    }

    /// Get the rebalancer status (SHOW falcon.rebalance_status).
    /// Returns a default status if no rebalancer is attached.
    pub fn rebalance_status(&self) -> crate::rebalancer::RebalancerStatus {
        // The DistributedQueryEngine doesn't own the rebalancer directly,
        // so return a default status. The actual rebalancer status is
        // exposed via the RebalanceRunnerHandle or ShardRebalancer.
        crate::rebalancer::RebalancerStatus {
            last_run: None,
            last_snapshot: None,
            last_plan: None,
            migration_statuses: Vec::new(),
            runs_completed: 0,
            total_rows_migrated: 0,
        }
    }

    /// Collect a shard load snapshot for rebalance planning.
    pub fn shard_load_snapshot(&self) -> crate::rebalancer::ShardLoadSnapshot {
        crate::rebalancer::ShardLoadSnapshot::collect(&self.engine)
    }

    /// Generate a rebalance plan (dry-run) from current shard loads.
    pub fn rebalance_plan(
        &self,
        planner: &crate::rebalancer::RebalancePlanner,
    ) -> crate::rebalancer::MigrationPlan {
        let snapshot = self.shard_load_snapshot();
        planner.plan(&snapshot, &self.engine)
    }

    /// Execute a rebalance plan. Returns (tasks_completed, tasks_failed, rows_migrated).
    pub fn rebalance_execute(
        &self,
        plan: &crate::rebalancer::MigrationPlan,
    ) -> (usize, usize, u64) {
        let migrator = crate::rebalancer::ShardMigrator::new(
            crate::rebalancer::RebalancerConfig::default(),
        );
        let mut completed = 0usize;
        let mut failed = 0usize;
        let mut rows_migrated = 0u64;

        for task in &plan.tasks {
            let status = migrator.execute_task(task, &self.engine);
            match status.phase {
                crate::rebalancer::MigrationPhase::Completed => {
                    completed += 1;
                    rows_migrated += status.rows_migrated;
                }
                _ => {
                    failed += 1;
                    if let Some(ref err) = status.error {
                        tracing::warn!(
                            "Rebalance task failed: {:?} → {:?} ({}): {}",
                            task.source_shard, task.target_shard, task.table_name, err
                        );
                    }
                }
            }
        }

        (completed, failed, rows_migrated)
    }

    /// Access the underlying two-phase coordinator for multi-shard writes.
    pub fn two_phase_coordinator(&self) -> &TwoPhaseCoordinator {
        &self.two_pc
    }

    /// Access the underlying distributed executor for closure-based scatter/gather.
    pub fn distributed_executor(&self) -> &DistributedExecutor {
        &self.dist_exec
    }

    /// Access the underlying sharded engine.
    pub fn engine(&self) -> &ShardedEngine {
        &self.engine
    }

    /// EXPLAIN for coordinator-side join: show the join strategy and tables involved.
    fn exec_explain_coordinator_join(
        &self,
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        use falcon_common::types::DataType;

        let mut lines: Vec<OwnedRow> = Vec::new();
        let is_hash = matches!(plan, PhysicalPlan::HashJoin { .. });
        let strategy = if is_hash { "HashJoin" } else { "NestedLoopJoin" };
        lines.push(OwnedRow::new(vec![Datum::Text(
            format!("CoordinatorJoin ({}, gather-all-then-join)", strategy),
        )]));
        let (left_table_id, joins, filter, order_by, limit, offset) = match plan {
            PhysicalPlan::NestedLoopJoin { left_table_id, joins, filter, order_by, limit, offset, .. }
            | PhysicalPlan::HashJoin { left_table_id, joins, filter, order_by, limit, offset, .. }
                => (left_table_id, joins, filter, order_by, limit, offset),
            _ => return Err(FalconError::internal_bug("E-QE-006", "exec_explain_coordinator_join: not a join plan", "plan type mismatch")),
        };
        {
            // Show tables involved
            let mut table_ids = vec![*left_table_id];
            for join in joins {
                table_ids.push(join.right_table_id);
            }
            lines.push(OwnedRow::new(vec![Datum::Text(
                format!("  Tables: {:?} (gathered from {} shards each)", table_ids, self.engine.shard_ids().len()),
            )]));

            // Show join types
            for (i, join) in joins.iter().enumerate() {
                lines.push(OwnedRow::new(vec![Datum::Text(
                    format!("  Join {}: {:?} on table {:?}", i, join.join_type, join.right_table_id),
                )]));
            }

            if filter.is_some() {
                lines.push(OwnedRow::new(vec![Datum::Text("  Filter: yes".into())]));
            }
            if !order_by.is_empty() {
                lines.push(OwnedRow::new(vec![Datum::Text(
                    format!("  Order by: {} columns", order_by.len()),
                )]));
            }
            if let Some(l) = limit {
                lines.push(OwnedRow::new(vec![Datum::Text(format!("  Limit: {}", l))]));
            }
            if let Some(o) = offset {
                lines.push(OwnedRow::new(vec![Datum::Text(format!("  Offset: {}", o))]));
            }

            // Execute and show row counts
            let result = self.exec_coordinator_join(plan, None);
            match &result {
                Ok(ExecutionResult::Query { rows, .. }) => {
                    lines.push(OwnedRow::new(vec![Datum::Text(
                        format!("  Result rows: {}", rows.len()),
                    )]));
                }
                Ok(_) => {}
                Err(e) => {
                    lines.push(OwnedRow::new(vec![Datum::Text(
                        format!("  Error: {}", e),
                    )]));
                }
            }
        }

        Ok(ExecutionResult::Query {
            columns: vec![("QUERY PLAN".into(), DataType::Text)],
            rows: lines,
        })
    }

    /// EXPLAIN for coordinator-side subquery execution.
    fn exec_explain_coordinator_subquery(
        &self,
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        use falcon_common::types::DataType;

        let mut lines: Vec<OwnedRow> = Vec::new();
        lines.push(OwnedRow::new(vec![Datum::Text(
            "CoordinatorSubquery (gather-all-then-execute)".into(),
        )]));

        let table_ids = Self::extract_table_ids(plan);
        lines.push(OwnedRow::new(vec![Datum::Text(
            format!("  Tables: {:?} (gathered from {} shards each)", table_ids, self.engine.shard_ids().len()),
        )]));

        // Execute and show row counts
        let result = self.exec_coordinator_subquery(plan, None);
        match &result {
            Ok(ExecutionResult::Query { rows, .. }) => {
                lines.push(OwnedRow::new(vec![Datum::Text(
                    format!("  Result rows: {}", rows.len()),
                )]));
            }
            Ok(_) => {}
            Err(e) => {
                lines.push(OwnedRow::new(vec![Datum::Text(
                    format!("  Error: {}", e),
                )]));
            }
        }

        Ok(ExecutionResult::Query {
            columns: vec![("QUERY PLAN".into(), DataType::Text)],
            rows: lines,
        })
    }

    /// Coordinator-side execution for SeqScan with subqueries/CTEs/UNIONs.
    /// Gathers all referenced table data from all shards into a temp engine,
    /// then executes locally. This ensures subqueries see all data across shards.
    fn exec_coordinator_subquery(
        &self,
        plan: &PhysicalPlan,
        _txn: Option<&TxnHandle>,
    ) -> Result<ExecutionResult, FalconError> {
        let table_ids = Self::extract_table_ids(plan);
        self.gather_and_execute_locally(&table_ids, plan)
    }

    /// Extract all TableIds referenced in a plan (main table + subquery/CTE/UNION tables).
    fn extract_table_ids(plan: &PhysicalPlan) -> Vec<falcon_common::types::TableId> {
        use std::collections::HashSet;

        let mut ids = HashSet::new();
        Self::collect_table_ids_from_plan(plan, &mut ids);
        ids.into_iter().collect()
    }

    fn collect_table_ids_from_plan(plan: &PhysicalPlan, ids: &mut std::collections::HashSet<falcon_common::types::TableId>) {
        match plan {
            PhysicalPlan::SeqScan { table_id, filter, projections, having, ctes, unions, .. } => {
                ids.insert(*table_id);
                if let Some(f) = filter { Self::collect_table_ids_from_expr(f, ids); }
                if let Some(h) = having { Self::collect_table_ids_from_expr(h, ids); }
                for proj in projections {
                    match proj {
                        falcon_sql_frontend::types::BoundProjection::Expr(e, _)
                        | falcon_sql_frontend::types::BoundProjection::Aggregate(_, Some(e), _, _, _) => {
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
            PhysicalPlan::NestedLoopJoin { left_table_id, joins, .. }
            | PhysicalPlan::HashJoin { left_table_id, joins, .. } => {
                ids.insert(*left_table_id);
                for join in joins { ids.insert(join.right_table_id); }
            }
            _ => {}
        }
    }

    fn collect_table_ids_from_select(sel: &falcon_sql_frontend::types::BoundSelect, ids: &mut std::collections::HashSet<falcon_common::types::TableId>) {
        ids.insert(sel.table_id);
        if let Some(f) = &sel.filter { Self::collect_table_ids_from_expr(f, ids); }
        for join in &sel.joins { ids.insert(join.right_table_id); }
        for cte in &sel.ctes {
            ids.insert(cte.table_id);
            Self::collect_table_ids_from_select(&cte.select, ids);
        }
        for (union_sel, _, _) in &sel.unions {
            Self::collect_table_ids_from_select(union_sel, ids);
        }
    }

    fn collect_table_ids_from_expr(expr: &BoundExpr, ids: &mut std::collections::HashSet<falcon_common::types::TableId>) {
        match expr {
            BoundExpr::ScalarSubquery(sel) => { Self::collect_table_ids_from_select(sel, ids); }
            BoundExpr::InSubquery { expr, subquery, .. } => {
                Self::collect_table_ids_from_expr(expr, ids);
                Self::collect_table_ids_from_select(subquery, ids);
            }
            BoundExpr::Exists { subquery, .. } => { Self::collect_table_ids_from_select(subquery, ids); }
            BoundExpr::BinaryOp { left, right, .. } => {
                Self::collect_table_ids_from_expr(left, ids);
                Self::collect_table_ids_from_expr(right, ids);
            }
            BoundExpr::Not(inner) | BoundExpr::IsNull(inner) | BoundExpr::IsNotNull(inner) | BoundExpr::Cast { expr: inner, .. } => {
                Self::collect_table_ids_from_expr(inner, ids);
            }
            BoundExpr::Between { expr, low, high, .. } => {
                Self::collect_table_ids_from_expr(expr, ids);
                Self::collect_table_ids_from_expr(low, ids);
                Self::collect_table_ids_from_expr(high, ids);
            }
            BoundExpr::Case { operand, conditions, results, else_result } => {
                if let Some(op) = operand { Self::collect_table_ids_from_expr(op, ids); }
                for c in conditions { Self::collect_table_ids_from_expr(c, ids); }
                for r in results { Self::collect_table_ids_from_expr(r, ids); }
                if let Some(el) = else_result { Self::collect_table_ids_from_expr(el, ids); }
            }
            BoundExpr::InList { expr, list, .. } => {
                Self::collect_table_ids_from_expr(expr, ids);
                for e in list { Self::collect_table_ids_from_expr(e, ids); }
            }
            BoundExpr::Function { args, .. } => { for a in args { Self::collect_table_ids_from_expr(a, ids); } }
            BoundExpr::Coalesce(exprs) | BoundExpr::ArrayLiteral(exprs) => {
                for e in exprs { Self::collect_table_ids_from_expr(e, ids); }
            }
            BoundExpr::Like { expr, pattern, .. } => {
                Self::collect_table_ids_from_expr(expr, ids);
                Self::collect_table_ids_from_expr(pattern, ids);
            }
            BoundExpr::ArrayIndex { array, index } => {
                Self::collect_table_ids_from_expr(array, ids);
                Self::collect_table_ids_from_expr(index, ids);
            }
            BoundExpr::IsNotDistinctFrom { left, right } => {
                Self::collect_table_ids_from_expr(left, ids);
                Self::collect_table_ids_from_expr(right, ids);
            }
            BoundExpr::AnyOp { left, right, .. } | BoundExpr::AllOp { left, right, .. } => {
                Self::collect_table_ids_from_expr(left, ids);
                Self::collect_table_ids_from_expr(right, ids);
            }
            BoundExpr::ArraySlice { array, lower, upper } => {
                Self::collect_table_ids_from_expr(array, ids);
                if let Some(e) = lower { Self::collect_table_ids_from_expr(e, ids); }
                if let Some(e) = upper { Self::collect_table_ids_from_expr(e, ids); }
            }
            BoundExpr::AggregateExpr { arg: Some(inner), .. } => { Self::collect_table_ids_from_expr(inner, ids); }
            BoundExpr::ColumnRef(_) | BoundExpr::Literal(_) | BoundExpr::OuterColumnRef(_) | BoundExpr::AggregateExpr { arg: None, .. }
            | BoundExpr::SequenceNextval(_) | BoundExpr::SequenceCurrval(_) | BoundExpr::SequenceSetval(_, _)
            | BoundExpr::Parameter(_) | BoundExpr::Grouping(_) => {}
        }
    }

    /// Check if a BoundExpr contains any subquery node (InSubquery, Exists, ScalarSubquery).
    fn expr_has_subquery(expr: &BoundExpr) -> bool {
        match expr {
            BoundExpr::ScalarSubquery(_) | BoundExpr::InSubquery { .. } | BoundExpr::Exists { .. } => true,
            BoundExpr::BinaryOp { left, right, .. } => {
                Self::expr_has_subquery(left) || Self::expr_has_subquery(right)
            }
            BoundExpr::Not(inner) | BoundExpr::IsNull(inner) | BoundExpr::IsNotNull(inner)
            | BoundExpr::Cast { expr: inner, .. } => Self::expr_has_subquery(inner),
            BoundExpr::Like { expr, pattern, .. } => {
                Self::expr_has_subquery(expr) || Self::expr_has_subquery(pattern)
            }
            BoundExpr::Between { expr, low, high, .. } => {
                Self::expr_has_subquery(expr) || Self::expr_has_subquery(low) || Self::expr_has_subquery(high)
            }
            BoundExpr::Case { operand, conditions, results, else_result } => {
                operand.as_deref().is_some_and(Self::expr_has_subquery)
                    || conditions.iter().any(Self::expr_has_subquery)
                    || results.iter().any(Self::expr_has_subquery)
                    || else_result.as_deref().is_some_and(Self::expr_has_subquery)
            }
            BoundExpr::InList { expr, list, .. } => {
                Self::expr_has_subquery(expr) || list.iter().any(Self::expr_has_subquery)
            }
            BoundExpr::Function { args, .. } => args.iter().any(Self::expr_has_subquery),
            BoundExpr::Coalesce(exprs) | BoundExpr::ArrayLiteral(exprs) => exprs.iter().any(Self::expr_has_subquery),
            BoundExpr::ArrayIndex { array, index } => {
                Self::expr_has_subquery(array) || Self::expr_has_subquery(index)
            }
            BoundExpr::IsNotDistinctFrom { left, right } => {
                Self::expr_has_subquery(left) || Self::expr_has_subquery(right)
            }
            BoundExpr::AnyOp { left, right, .. } | BoundExpr::AllOp { left, right, .. } => {
                Self::expr_has_subquery(left) || Self::expr_has_subquery(right)
            }
            BoundExpr::ArraySlice { array, lower, upper } => {
                Self::expr_has_subquery(array)
                    || lower.as_deref().is_some_and(Self::expr_has_subquery)
                    || upper.as_deref().is_some_and(Self::expr_has_subquery)
            }
            BoundExpr::AggregateExpr { arg: Some(inner), .. } => Self::expr_has_subquery(inner),
            BoundExpr::ColumnRef(_) | BoundExpr::Literal(_) | BoundExpr::OuterColumnRef(_) | BoundExpr::AggregateExpr { arg: None, .. }
            | BoundExpr::SequenceNextval(_) | BoundExpr::SequenceCurrval(_) | BoundExpr::SequenceSetval(_, _)
            | BoundExpr::Parameter(_) | BoundExpr::Grouping(_) => false,
        }
    }

    /// Materialize subqueries in a DML filter at coordinator level.
    /// Gathers all subquery-referenced table data from all shards into a temp engine,
    /// then uses the Executor's materialize_filter to replace subquery nodes with
    /// concrete values. Returns the materialized filter expression.
    fn materialize_dml_filter(
        &self,
        filter: &BoundExpr,
    ) -> Result<BoundExpr, FalconError> {
        // Collect table IDs referenced by subqueries in the filter
        let mut ids = std::collections::HashSet::new();
        Self::collect_table_ids_from_expr(filter, &mut ids);

        let temp_storage = Arc::new(StorageEngine::new_in_memory());
        let temp_txn_mgr = Arc::new(TxnManager::new(temp_storage.clone()));

        // Gather subquery tables from all shards
        let shard_ids = self.engine.shard_ids();
        for &table_id in &ids {
            let schema = shard_ids.iter()
                .find_map(|sid| {
                    let s = self.engine.shard(*sid)?;
                    s.storage.get_table_schema_by_id(table_id)
                })
                .ok_or_else(|| FalconError::Internal(
                    format!("Table {:?} not found on any shard", table_id),
                ))?;

            temp_storage.create_table(schema.clone()).map_err(|e| {
                FalconError::Internal(format!("Failed to create temp table: {:?}", e))
            })?;

            let gathered_rows: Vec<falcon_common::datum::OwnedRow> = {
                let mut per_shard: Vec<Vec<falcon_common::datum::OwnedRow>> = Vec::new();
                std::thread::scope(|s| {
                    let handles: Vec<_> = shard_ids.iter().filter_map(|sid| {
                        let shard = self.engine.shard(*sid)?;
                        Some(s.spawn(move || {
                            let shard_txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
                            let read_ts = shard_txn.read_ts(shard.txn_mgr.current_ts());
                            let rows = shard.storage.scan(table_id, shard_txn.txn_id, read_ts)
                                .unwrap_or_default();
                            let _ = shard.txn_mgr.commit(shard_txn.txn_id);
                            rows.into_iter().map(|(_, r)| r).collect::<Vec<_>>()
                        }))
                    }).collect();
                    for h in handles {
                        if let Ok(rows) = h.join() {
                            per_shard.push(rows);
                        }
                    }
                });
                per_shard.into_iter().flatten().collect()
            };

            let temp_txn = temp_txn_mgr.begin(IsolationLevel::ReadCommitted);
            for row in gathered_rows {
                temp_storage.insert(table_id, row, temp_txn.txn_id).map_err(|e| {
                    FalconError::Internal(format!("Failed to insert into temp storage: {:?}", e))
                })?;
            }
            temp_txn_mgr.commit(temp_txn.txn_id).map_err(|e| {
                FalconError::Internal(format!("Failed to commit temp txn: {:?}", e))
            })?;
        }

        // Use a temp Executor to materialize subqueries with full cross-shard data
        let temp_exec = Executor::new(temp_storage, temp_txn_mgr.clone());
        let temp_txn = temp_txn_mgr.begin(IsolationLevel::ReadCommitted);
        temp_exec.materialize_subqueries(filter, &temp_txn)
    }

    /// Coordinator-side join: gather all table data from all shards into a
    /// temporary in-memory StorageEngine, then execute the join locally.
    /// This ensures cross-shard correctness — no missing matches when
    /// left and right rows reside on different shards.
    fn exec_coordinator_join(
        &self,
        plan: &PhysicalPlan,
        _txn: Option<&TxnHandle>,
    ) -> Result<ExecutionResult, FalconError> {
        let (left_table_id, joins) = match plan {
            PhysicalPlan::NestedLoopJoin { left_table_id, joins, .. }
            | PhysicalPlan::HashJoin { left_table_id, joins, .. } => (*left_table_id, joins),
            _ => return Err(FalconError::internal_bug("E-QE-007", "exec_coordinator_join called on non-join plan", "plan type mismatch")),
        };
        let mut table_ids = vec![left_table_id];
        for join in joins { table_ids.push(join.right_table_id); }
        self.gather_and_execute_locally(&table_ids, plan)
    }

    /// Shared coordinator-side execution: gather data for given tables from
    /// all shards into a temp in-memory StorageEngine, then execute the plan.
    fn gather_and_execute_locally(
        &self,
        table_ids: &[falcon_common::types::TableId],
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        let temp_storage = Arc::new(StorageEngine::new_in_memory());
        let temp_txn_mgr = Arc::new(TxnManager::new(temp_storage.clone()));

        let shard_ids = self.engine.shard_ids();
        for &table_id in table_ids {
            let schema = shard_ids.iter()
                .find_map(|sid| {
                    let s = self.engine.shard(*sid)?;
                    s.storage.get_table_schema_by_id(table_id)
                })
                .ok_or_else(|| FalconError::Internal(
                    format!("Table {:?} not found on any shard", table_id),
                ))?;

            temp_storage.create_table(schema.clone()).map_err(|e| {
                FalconError::Internal(format!("Failed to create temp table: {:?}", e))
            })?;

            // Parallel gather from all shards
            let gathered_rows: Vec<OwnedRow> = {
                let mut per_shard: Vec<Vec<OwnedRow>> = Vec::new();
                std::thread::scope(|s| {
                    let handles: Vec<_> = shard_ids.iter().filter_map(|sid| {
                        let shard = self.engine.shard(*sid)?;
                        Some(s.spawn(move || {
                            let shard_txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
                            let read_ts = shard_txn.read_ts(shard.txn_mgr.current_ts());
                            let rows = shard.storage.scan(table_id, shard_txn.txn_id, read_ts)
                                .unwrap_or_default();
                            let _ = shard.txn_mgr.commit(shard_txn.txn_id);
                            rows.into_iter().map(|(_, r)| r).collect::<Vec<_>>()
                        }))
                    }).collect();
                    for h in handles {
                        if let Ok(rows) = h.join() {
                            per_shard.push(rows);
                        }
                    }
                });
                per_shard.into_iter().flatten().collect()
            };

            let temp_txn = temp_txn_mgr.begin(IsolationLevel::ReadCommitted);
            for row in gathered_rows {
                temp_storage.insert(table_id, row, temp_txn.txn_id).map_err(|e| {
                    FalconError::Internal(format!("Failed to insert into temp storage: {:?}", e))
                })?;
            }
            temp_txn_mgr.commit(temp_txn.txn_id).map_err(|e| {
                FalconError::Internal(format!("Failed to commit temp txn: {:?}", e))
            })?;
        }

        let temp_exec = Executor::new(temp_storage, temp_txn_mgr.clone());
        let temp_txn = temp_txn_mgr.begin(IsolationLevel::ReadCommitted);
        temp_exec.execute(plan, Some(&temp_txn))
    }

    /// EXPLAIN ANALYZE for a DistPlan: execute the plan and show per-shard stats.
    fn exec_explain_dist(
        &self,
        inner: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        use falcon_common::datum::OwnedRow;
        use falcon_common::types::DataType;

        // Execute the inner DistPlan to populate scatter stats.
        let result = self.execute(inner, None);
        let stats = self.last_scatter_stats();

        // Build EXPLAIN output with plan structure + execution stats.
        let shard0 = self.engine.shard(ShardId(0)).ok_or_else(|| {
            FalconError::internal_bug("E-QE-008", "No shard 0 available", "exec_explain_analyze_coordinator")
        })?;
        let local_exec = Executor::new(shard0.storage.clone(), shard0.txn_mgr.clone());
        let plan_lines = local_exec.format_plan(inner, 0);

        let mut lines: Vec<OwnedRow> = plan_lines
            .into_iter()
            .map(|l| OwnedRow::new(vec![Datum::Text(l)]))
            .collect();

        // Append gather strategy details
        if let PhysicalPlan::DistPlan { gather, target_shards, .. } = inner {
            lines.push(OwnedRow::new(vec![Datum::Text(String::new())]));
            lines.push(OwnedRow::new(vec![Datum::Text(
                format!("Distributed Plan ({} shards)", target_shards.len()),
            )]));
            match gather {
                DistGather::Union { distinct, limit, offset } => {
                    lines.push(OwnedRow::new(vec![Datum::Text(
                        format!("  Gather: Union (distinct={}, limit={:?}, offset={:?})", distinct, limit, offset),
                    )]));
                }
                DistGather::MergeSortLimit { sort_columns, limit, offset } => {
                    let cols: Vec<String> = sort_columns.iter()
                        .map(|(idx, asc)| format!("col{}:{}", idx, if *asc { "ASC" } else { "DESC" }))
                        .collect();
                    lines.push(OwnedRow::new(vec![Datum::Text(
                        format!("  Gather: MergeSortLimit (sort=[{}], limit={:?}, offset={:?})",
                            cols.join(", "), limit, offset),
                    )]));
                }
                DistGather::TwoPhaseAgg { group_by_indices, agg_merges, having, avg_fixups, visible_columns, order_by, limit, offset } => {
                    let merges: Vec<String> = agg_merges.iter().map(|m| m.to_string()).collect();
                    lines.push(OwnedRow::new(vec![Datum::Text(
                        format!("  Gather: TwoPhaseAgg (group_by={:?})", group_by_indices),
                    )]));
                    for merge_label in &merges {
                        lines.push(OwnedRow::new(vec![Datum::Text(
                            format!("    merge: {}", merge_label),
                        )]));
                    }
                    if !avg_fixups.is_empty() {
                        lines.push(OwnedRow::new(vec![Datum::Text(
                            format!("    AVG decomposition: {:?} (visible_columns={})", avg_fixups, visible_columns),
                        )]));
                    }
                    if having.is_some() {
                        lines.push(OwnedRow::new(vec![Datum::Text(
                            "    HAVING: applied post-merge".into(),
                        )]));
                    }
                    if !order_by.is_empty() {
                        let cols: Vec<String> = order_by.iter()
                            .map(|(idx, asc)| format!("col{}:{}", idx, if *asc { "ASC" } else { "DESC" }))
                            .collect();
                        lines.push(OwnedRow::new(vec![Datum::Text(
                            format!("    ORDER BY: [{}], LIMIT: {:?}, OFFSET: {:?}", cols.join(", "), limit, offset),
                        )]));
                    }
                }
            }
        }

        // Append execution stats
        lines.push(OwnedRow::new(vec![Datum::Text(String::new())]));
        lines.push(OwnedRow::new(vec![Datum::Text(
            format!("Execution Stats ({})", if result.is_ok() { "OK" } else { "ERROR" }),
        )]));
        lines.push(OwnedRow::new(vec![Datum::Text(
            format!("  Shards participated: {}", stats.shards_participated),
        )]));
        lines.push(OwnedRow::new(vec![Datum::Text(
            format!("  Total rows gathered: {}", stats.total_rows_gathered),
        )]));
        lines.push(OwnedRow::new(vec![Datum::Text(
            format!("  Gather strategy: {}", stats.gather_strategy),
        )]));
        if let Some(pruned) = stats.pruned_to_shard {
            lines.push(OwnedRow::new(vec![Datum::Text(
                format!("  Shard pruning: routed to shard {} (PK point lookup)", pruned),
            )]));
        }
        lines.push(OwnedRow::new(vec![Datum::Text(
            format!("  Total latency: {}us", stats.total_latency_us),
        )]));
        for (sid, lat) in &stats.per_shard_latency_us {
            let row_count = stats.per_shard_row_count.iter()
                .find(|(s, _)| s == sid)
                .map(|(_, c)| *c)
                .unwrap_or(0);
            lines.push(OwnedRow::new(vec![Datum::Text(
                format!("  Shard {} latency: {}us, rows: {}", sid, lat, row_count),
            )]));
        }
        if !stats.failed_shards.is_empty() {
            lines.push(OwnedRow::new(vec![Datum::Text(
                format!("  Failed/timed-out shards: {:?}", stats.failed_shards),
            )]));
        }

        Ok(ExecutionResult::Query {
            columns: vec![("QUERY PLAN".into(), DataType::Text)],
            rows: lines,
        })
    }

    /// Execute DDL on ALL shards in parallel. Returns the result from the first shard.
    fn exec_ddl_all_shards(
        &self,
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        let results: Vec<Result<ExecutionResult, FalconError>> =
            std::thread::scope(|s| {
                let handles: Vec<_> = self.engine.all_shards()
                    .iter()
                    .map(|shard| {
                        let plan_ref = plan;
                        s.spawn(move || {
                            let local_exec = Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                            local_exec.execute(plan_ref, None)
                        })
                    })
                    .collect();
                handles.into_iter()
                    .map(|h| h.join().unwrap_or_else(|_| Err(FalconError::internal_bug(
                        "E-QE-001", "shard thread panicked", "exec_ddl_all_shards"
                    ))))
                    .collect()
            });

        let mut first_result = None;
        for r in results {
            let result = r?;
            if first_result.is_none() {
                first_result = Some(result);
            }
        }
        first_result.ok_or_else(|| FalconError::internal_bug("E-QE-009", "No shards available", "exec_gc_all_shards"))
    }

    /// Execute a DML plan on a specific shard with an auto-commit transaction.
    fn execute_on_shard_auto_txn(
        &self,
        shard_id: ShardId,
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        let shard = self.engine.shard(shard_id).ok_or_else(|| {
            FalconError::internal_bug("E-QE-010", format!("Shard {:?} not found", shard_id), "exec_dml_autocommit")
        })?;
        let local_exec = Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
        let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
        let result = local_exec.execute(plan, Some(&txn));
        match &result {
            Ok(_) => { let _ = shard.txn_mgr.commit(txn.txn_id); }
            Err(_) => { let _ = shard.txn_mgr.abort(txn.txn_id); }
        }
        result
    }

    /// Split a multi-row INSERT by target shard: group rows by PK hash,
    /// build a per-shard INSERT plan, and execute each on its target shard.
    /// Falls back to shard 0 for rows whose PK cannot be extracted.
    fn exec_insert_split(
        &self,
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        let (table_id, schema, columns, rows, source_select, returning, on_conflict) = match plan {
            PhysicalPlan::Insert {
                table_id, schema, columns, rows, source_select, returning, on_conflict,
            } => (*table_id, schema, columns, rows, source_select, returning, on_conflict),
            _ => return Err(FalconError::internal_bug("E-QE-011", "exec_insert_split called with non-Insert", "plan type mismatch")),
        };

        // INSERT ... SELECT — execute the SELECT at coordinator level to see all
        // shard data, then convert results to VALUES and split by PK hash.
        if let Some(sel) = source_select {
            return self.exec_insert_select(table_id, schema, columns, sel, returning, on_conflict);
        }

        if rows.is_empty() {
            return self.execute_on_shard_auto_txn(ShardId(0), plan);
        }

        // Group rows by target shard
        let pk_col_idx = schema.primary_key_columns.first().copied();
        let mut shard_rows: HashMap<ShardId, Vec<Vec<BoundExpr>>> = HashMap::new();

        for row in rows {
            let target = pk_col_idx
                .and_then(|idx| row.get(idx))
                .and_then(|expr| self.extract_int_key(expr))
                .map(|key| self.engine.shard_for_key(key))
                .unwrap_or(ShardId(0));
            shard_rows.entry(target).or_default().push(row.clone());
        }

        // Build per-shard INSERT plans
        let shard_plans: Vec<(ShardId, PhysicalPlan)> = shard_rows
            .into_iter()
            .map(|(shard_id, shard_row_batch)| {
                let shard_plan = PhysicalPlan::Insert {
                    table_id,
                    schema: schema.clone(),
                    columns: columns.to_vec(),
                    rows: shard_row_batch,
                    source_select: None,
                    returning: returning.to_vec(),
                    on_conflict: on_conflict.clone(),
                };
                (shard_id, shard_plan)
            })
            .collect();

        // Single shard → skip thread overhead
        if shard_plans.len() == 1 {
            let (sid, sp) = &shard_plans[0];
            return self.execute_on_shard_auto_txn(*sid, sp);
        }

        // Execute per-shard INSERTs in parallel
        let shard_count = shard_plans.len();
        let results: Vec<(ShardId, Result<ExecutionResult, FalconError>)> =
            std::thread::scope(|s| {
                let handles: Vec<_> = shard_plans
                    .iter()
                    .map(|(shard_id, shard_plan)| {
                        let engine = &self.engine;
                        let sid = *shard_id;
                        s.spawn(move || {
                            let shard = engine.shard(sid).ok_or_else(|| {
                                FalconError::internal_bug("E-QE-012", format!("Shard {:?} not found", sid), "exec_insert_split parallel")
                            })?;
                            let local_exec =
                                Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
                            let result = local_exec.execute(shard_plan, Some(&txn));
                            match &result {
                                Ok(_) => { let _ = shard.txn_mgr.commit(txn.txn_id); }
                                Err(_) => { let _ = shard.txn_mgr.abort(txn.txn_id); }
                            }
                            result
                        })
                    })
                    .collect();
                shard_plans.iter().map(|(sid, _)| *sid)
                    .zip(handles.into_iter().map(|h| h.join().unwrap_or_else(|_| Err(FalconError::internal_bug(
                        "E-QE-002", "shard thread panicked", "exec_dist_plan scatter"
                    )))))
                    .collect()
            });

        let mut total_rows_affected = 0u64;
        let mut tag = String::new();
        let mut errors: Vec<String> = Vec::new();
        let mut succeeded = 0usize;

        for (sid, result) in results {
            match result {
                Ok(ExecutionResult::Dml { rows_affected, tag: t }) => {
                    total_rows_affected += rows_affected;
                    if tag.is_empty() { tag = t; }
                    succeeded += 1;
                }
                Ok(other) => return Ok(other),
                Err(e) => errors.push(format!("shard {:?}: {}", sid, e)),
            }
        }

        if !errors.is_empty() {
            return Err(FalconError::Transient {
                reason: format!(
                    "INSERT failed on {}/{} shards ({} succeeded): {}",
                    errors.len(), shard_count, succeeded, errors.join("; ")
                ),
                retry_after_ms: 100,
            });
        }

        Ok(ExecutionResult::Dml {
            rows_affected: total_rows_affected,
            tag,
        })
    }

    /// Execute INSERT ... SELECT at coordinator level: run the SELECT across
    /// all shards (gathering all source data), then convert results to VALUES
    /// rows and split-insert by PK hash for correct shard placement.
    fn exec_insert_select(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &falcon_common::schema::TableSchema,
        columns: &[usize],
        source_select: &falcon_sql_frontend::types::BoundSelect,
        returning: &[(BoundExpr, String)],
        on_conflict: &Option<falcon_sql_frontend::types::OnConflictAction>,
    ) -> Result<ExecutionResult, FalconError> {
        // Plan the SELECT and execute at coordinator level to see all shard data
        let select_plan = falcon_planner::Planner::plan(
            &falcon_sql_frontend::types::BoundStatement::Select(source_select.clone()),
        )?;
        let select_table_ids = Self::extract_table_ids(&select_plan);
        let select_result = self.gather_and_execute_locally(&select_table_ids, &select_plan)?;

        let result_rows = match select_result {
            ExecutionResult::Query { rows, .. } => rows,
            _ => return Ok(ExecutionResult::Dml { rows_affected: 0, tag: "INSERT".into() }),
        };

        if result_rows.is_empty() {
            return Ok(ExecutionResult::Dml { rows_affected: 0, tag: "INSERT".into() });
        }

        // Convert OwnedRows to BoundExpr::Literal VALUES rows
        let value_rows: Vec<Vec<BoundExpr>> = result_rows
            .iter()
            .map(|row| row.values.iter().map(|d| BoundExpr::Literal(d.clone())).collect())
            .collect();

        // Build an INSERT with VALUES (no source_select) and use the existing split logic
        let values_plan = PhysicalPlan::Insert {
            table_id,
            schema: schema.clone(),
            columns: columns.to_vec(),
            rows: value_rows,
            source_select: None,
            returning: returning.to_vec(),
            on_conflict: on_conflict.clone(),
        };
        self.exec_insert_split(&values_plan)
    }

    /// Execute RunGc on ALL shards in parallel, aggregating results.
    fn exec_gc_all_shards(&self) -> Result<ExecutionResult, FalconError> {
        let gc_results: Vec<Result<ExecutionResult, FalconError>> =
            std::thread::scope(|s| {
                let handles: Vec<_> = self.engine.all_shards()
                    .iter()
                    .map(|shard| {
                        s.spawn(move || {
                            let local_exec = Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                            local_exec.execute(&PhysicalPlan::RunGc, None)
                        })
                    })
                    .collect();
                handles.into_iter()
                    .map(|h| h.join().unwrap_or_else(|_| Err(FalconError::internal_bug(
                        "E-QE-003", "shard thread panicked", "exec_gc_all_shards"
                    ))))
                    .collect()
            });

        let mut total_chains = 0i64;
        let mut watermark = 0i64;
        for r in gc_results {
            if let ExecutionResult::Query { rows, .. } = r? {
                for row in &rows {
                    if row.values.len() >= 2 {
                        if let Datum::Text(ref key) = row.values[0] {
                            if let Datum::Int64(val) = row.values[1] {
                                match key.as_str() {
                                    "chains_processed" => total_chains += val,
                                    "watermark_ts" => { if val > watermark { watermark = val; } }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
        use falcon_common::types::DataType;
        use falcon_common::datum::OwnedRow;
        let columns = vec![
            ("metric".to_string(), DataType::Text),
            ("value".to_string(), DataType::Int64),
        ];
        let rows = vec![
            OwnedRow::new(vec![Datum::Text("watermark_ts".into()), Datum::Int64(watermark)]),
            OwnedRow::new(vec![Datum::Text("chains_processed".into()), Datum::Int64(total_chains)]),
            OwnedRow::new(vec![Datum::Text("shards_processed".into()), Datum::Int64(self.engine.all_shards().len() as i64)]),
        ];
        Ok(ExecutionResult::Query { columns, rows })
    }

    /// Execute a DML plan on a specific subset of shards (IN-list pruning).
    fn exec_dml_on_shards(
        &self,
        plan: &PhysicalPlan,
        shards: &[ShardId],
    ) -> Result<ExecutionResult, FalconError> {
        let shard_count = shards.len();
        let results: Vec<(u64, Result<ExecutionResult, FalconError>)> =
            std::thread::scope(|s| {
                let handles: Vec<_> = shards
                    .iter()
                    .map(|sid| {
                        let shard = self.engine.shard(*sid);
                        let plan_ref = plan;
                        let shard_id = sid.0;
                        s.spawn(move || {
                            let shard = match shard {
                                Some(s) => s,
                                None => return (shard_id, Err(FalconError::internal_bug(
                                    "E-QE-004",
                                    format!("DML dispatch: shard {} not found", shard_id),
                                    "execute_dml_on_shards",
                                ))),
                            };
                            let local_exec = Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
                            let result = local_exec.execute(plan_ref, Some(&txn));
                            match &result {
                                Ok(_) => { let _ = shard.txn_mgr.commit(txn.txn_id); }
                                Err(_) => { let _ = shard.txn_mgr.abort(txn.txn_id); }
                            }
                            (shard_id, result)
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| {
                    h.join().unwrap_or_else(|_| (0, Err(FalconError::internal_bug(
                        "E-QE-005",
                        "DML dispatch: shard thread panicked",
                        "std::thread::JoinHandle returned Err",
                    ))))
                }).collect()
            });

        Self::merge_dml_results(results, shard_count)
    }

    /// Merge DML results from multiple shards, handling both Dml and Query (RETURNING) results.
    fn merge_dml_results(
        results: Vec<(impl std::fmt::Display, Result<ExecutionResult, FalconError>)>,
        shard_count: usize,
    ) -> Result<ExecutionResult, FalconError> {
        let mut total_rows_affected = 0u64;
        let mut tag = String::new();
        let mut errors: Vec<String> = Vec::new();
        let mut succeeded = 0usize;
        // For RETURNING: collect query rows from all shards
        let mut returning_columns: Option<Vec<(String, falcon_common::types::DataType)>> = None;
        let mut returning_rows: Vec<falcon_common::datum::OwnedRow> = Vec::new();

        for (shard_id, result) in results {
            match result {
                Ok(ExecutionResult::Dml { rows_affected, tag: t }) => {
                    total_rows_affected += rows_affected;
                    if tag.is_empty() { tag = t; }
                    succeeded += 1;
                }
                Ok(ExecutionResult::Query { columns, rows }) => {
                    // RETURNING clause produces Query results
                    if returning_columns.is_none() {
                        returning_columns = Some(columns);
                    }
                    returning_rows.extend(rows);
                    succeeded += 1;
                }
                Ok(_) => { succeeded += 1; }
                Err(e) => errors.push(format!("shard {}: {}", shard_id, e)),
            }
        }

        if !errors.is_empty() {
            return Err(FalconError::Transient {
                reason: format!(
                    "DML failed on {}/{} shards ({} succeeded): {}",
                    errors.len(), shard_count, succeeded, errors.join("; ")
                ),
                retry_after_ms: 100,
            });
        }

        // If any shard returned RETURNING rows, return merged Query result
        if let Some(columns) = returning_columns {
            return Ok(ExecutionResult::Query { columns, rows: returning_rows });
        }

        Ok(ExecutionResult::Dml {
            rows_affected: total_rows_affected,
            tag,
        })
    }

    /// Execute a DML plan (UPDATE/DELETE) on ALL shards in parallel, summing rows_affected.
    /// Collects partial results even when some shards fail, reporting all errors.
    fn exec_dml_all_shards(
        &self,
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        let shard_count = self.engine.all_shards().len();
        let results: Vec<(usize, Result<ExecutionResult, FalconError>)> =
            std::thread::scope(|s| {
                let handles: Vec<_> = self.engine.all_shards()
                    .iter()
                    .enumerate()
                    .map(|(idx, shard)| {
                        let plan_ref = plan;
                        s.spawn(move || {
                            let local_exec = Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
                            let result = local_exec.execute(plan_ref, Some(&txn));
                            match &result {
                                Ok(_) => { let _ = shard.txn_mgr.commit(txn.txn_id); }
                                Err(_) => { let _ = shard.txn_mgr.abort(txn.txn_id); }
                            }
                            (idx, result)
                        })
                    })
                    .collect();
                handles.into_iter()
                    .map(|h| h.join().unwrap_or_else(|_| (usize::MAX, Err(FalconError::internal_bug(
                        "E-QE-004", "shard thread panicked", "exec_dml_all_shards"
                    )))))
                    .collect()
            });

        Self::merge_dml_results(results, shard_count)
    }

    /// Resolve the target shard for a query by extracting a PK equality from the
    /// filter expression. Handles bare `pk = literal`, AND chains containing
    /// `pk = literal`, and returns None otherwise.
    fn resolve_filter_shard(
        &self,
        schema: &falcon_common::schema::TableSchema,
        filter: Option<&falcon_sql_frontend::types::BoundExpr>,
    ) -> Option<ShardId> {
        self.extract_pk_key(schema, filter?)
            .map(|key| self.engine.shard_for_key(key))
    }

    /// Extract a PK integer key from a filter expression. Handles:
    /// - `pk = literal`
    /// - `pk = literal AND other_conditions` (recursive AND chains)
    fn extract_pk_key(
        &self,
        schema: &falcon_common::schema::TableSchema,
        filter: &falcon_sql_frontend::types::BoundExpr,
    ) -> Option<i64> {
        use falcon_sql_frontend::types::{BinOp, BoundExpr};

        if schema.primary_key_columns.is_empty() {
            return None;
        }
        let pk_col_idx = schema.primary_key_columns[0];

        match filter {
            BoundExpr::BinaryOp { op: BinOp::Eq, left, right } => {
                if let Some(key) = self.match_pk_eq(pk_col_idx, left, right) {
                    return Some(key);
                }
                if let Some(key) = self.match_pk_eq(pk_col_idx, right, left) {
                    return Some(key);
                }
                None
            }
            // AND chain: recurse into both sides
            BoundExpr::BinaryOp { op: BinOp::And, left, right } => {
                self.extract_pk_key(schema, left)
                    .or_else(|| self.extract_pk_key(schema, right))
            }
            _ => None,
        }
    }

    /// Resolve target shards for an IN-list filter: `pk IN (1, 5, 9)`.
    /// Returns a deduplicated set of shards, or None if not an IN-list on PK.
    fn resolve_in_list_shards(
        &self,
        schema: &falcon_common::schema::TableSchema,
        filter: Option<&falcon_sql_frontend::types::BoundExpr>,
    ) -> Option<Vec<ShardId>> {
        if schema.primary_key_columns.is_empty() {
            return None;
        }
        let pk_col_idx = schema.primary_key_columns[0];
        let filter = filter?;

        // Try bare InList or AND chain containing InList
        self.extract_in_list_keys(pk_col_idx, filter)
            .map(|keys| {
                let mut shards: Vec<ShardId> = keys.iter()
                    .map(|k| self.engine.shard_for_key(*k))
                    .collect();
                shards.sort_by_key(|s| s.0);
                shards.dedup();
                shards
            })
    }

    /// Extract PK keys from an IN-list expression or AND chain containing one.
    fn extract_in_list_keys(
        &self,
        pk_col_idx: usize,
        filter: &falcon_sql_frontend::types::BoundExpr,
    ) -> Option<Vec<i64>> {
        use falcon_sql_frontend::types::{BinOp, BoundExpr};

        match filter {
            BoundExpr::InList { expr, list, negated } if !negated => {
                if let BoundExpr::ColumnRef(idx) = expr.as_ref() {
                    if *idx == pk_col_idx {
                        let keys: Vec<i64> = list.iter()
                            .filter_map(|e| self.extract_int_key(e))
                            .collect();
                        if keys.len() == list.len() {
                            return Some(keys);
                        }
                    }
                }
                None
            }
            BoundExpr::BinaryOp { op: BinOp::And, left, right } => {
                self.extract_in_list_keys(pk_col_idx, left)
                    .or_else(|| self.extract_in_list_keys(pk_col_idx, right))
            }
            _ => None,
        }
    }

    /// Check if (col_expr, val_expr) is (ColumnRef(pk_col_idx), Literal(int)).
    fn match_pk_eq(
        &self,
        pk_col_idx: usize,
        col_expr: &falcon_sql_frontend::types::BoundExpr,
        val_expr: &falcon_sql_frontend::types::BoundExpr,
    ) -> Option<i64> {
        use falcon_sql_frontend::types::BoundExpr;
        match col_expr {
            BoundExpr::ColumnRef(idx) if *idx == pk_col_idx => {
                self.extract_int_key(val_expr)
            }
            _ => None,
        }
    }

    /// Check if a subplan is a SeqScan with a PK equality filter that can be
    /// routed to a single shard. Returns the target shard if so.
    fn try_single_shard_scan(&self, subplan: &PhysicalPlan) -> Option<ShardId> {
        match subplan {
            PhysicalPlan::SeqScan { schema, filter, .. } => {
                self.resolve_filter_shard(schema, filter.as_ref())
            }
            _ => None,
        }
    }

    /// Check if a subplan is a SeqScan with a PK IN-list filter that can be
    /// pruned to a subset of shards. Returns the pruned shard list if so.
    fn try_in_list_shard_prune(&self, subplan: &PhysicalPlan) -> Option<Vec<ShardId>> {
        match subplan {
            PhysicalPlan::SeqScan { schema, filter, .. } => {
                let shards = self.resolve_in_list_shards(schema, filter.as_ref())?;
                // Only useful if it actually prunes (fewer shards than total)
                if shards.len() < self.engine.shard_ids().len() {
                    Some(shards)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Extract an integer key from a bound expression (literal only).
    fn extract_int_key(&self, expr: &BoundExpr) -> Option<i64> {
        match expr {
            BoundExpr::Literal(Datum::Int32(v)) => Some(*v as i64),
            BoundExpr::Literal(Datum::Int64(v)) => Some(*v),
            _ => None,
        }
    }

    /// Execute a DistPlan: scatter the subplan to each shard's Executor in parallel,
    /// then gather/merge results according to the DistGather strategy.
    ///
    /// **Optimization**: if the subplan is a SeqScan with a PK equality filter,
    /// route to a single shard instead of scattering to all shards.
    fn exec_dist_plan(
        &self,
        subplan: &PhysicalPlan,
        target_shards: &[ShardId],
        gather: &DistGather,
    ) -> Result<ExecutionResult, FalconError> {
        // ── Single-shard shortcut: PK point lookup ──
        if let Some(single_shard) = self.try_single_shard_scan(subplan) {
            // Record shard pruning in stats for observability
            let start = Instant::now();
            let result = self.execute_on_shard_auto_txn(single_shard, subplan);
            let elapsed = start.elapsed().as_micros() as u64;
            let row_count = match &result {
                Ok(ExecutionResult::Query { rows, .. }) => rows.len(),
                _ => 0,
            };
            *self.last_scatter_stats.lock().unwrap_or_else(|p| p.into_inner()) = ScatterStats {
                shards_participated: 1,
                total_rows_gathered: row_count,
                per_shard_latency_us: vec![(single_shard.0, elapsed)],
                per_shard_row_count: vec![(single_shard.0, row_count)],
                gather_strategy: "ShardPruned".into(),
                total_latency_us: elapsed,
                pruned_to_shard: Some(single_shard.0),
                ..Default::default()
            };
            return result;
        }

        // ── IN-list shard pruning: scatter only to shards that own the keys ──
        // Only attempt if target_shards hasn't already been pruned (avoid infinite recursion).
        if target_shards.len() == self.engine.shard_ids().len() {
            if let Some(pruned_shards) = self.try_in_list_shard_prune(subplan) {
                return self.exec_dist_plan(subplan, &pruned_shards, gather);
            }
        }

        let total_start = Instant::now();
        let timeout = self.timeout;

        // Validate shard IDs
        for &sid in target_shards {
            if self.engine.shard(sid).is_none() {
                return Err(FalconError::internal_bug(
                    "E-QE-013", format!("Shard {:?} not found", sid), "exec_dist_plan pre-validation",
                ));
            }
        }

        // Shared cancellation flag: set when any shard exceeds timeout.
        let cancelled = AtomicBool::new(false);

        // ── Scatter: execute subplan on each shard in parallel ──
        let shard_results: Vec<Result<ShardResult, FalconError>> =
            std::thread::scope(|s| {
                let handles: Vec<_> = target_shards
                    .iter()
                    .map(|&shard_id| {
                        let engine = &self.engine;
                        let subplan_ref = subplan;
                        let cancelled_ref = &cancelled;
                        s.spawn(move || {
                            // Check if already cancelled before starting
                            if cancelled_ref.load(Ordering::Relaxed) {
                                return Err(FalconError::Transient {
                                    reason: format!(
                                        "DistPlan cancelled before shard {:?} started", shard_id,
                                    ),
                                    retry_after_ms: 50,
                                });
                            }

                            let shard = match engine.shard(shard_id) {
                                Some(s) => s,
                                None => return Err(FalconError::Cluster(
                                    falcon_common::error::ClusterError::ShardNotFound(shard_id.0)
                                )),
                            };

                            // Retry once on transient failure.
                            let max_attempts = 2u8;
                            let mut last_err = None;
                            for attempt in 0..max_attempts {
                                if attempt > 0 {
                                    std::thread::sleep(std::time::Duration::from_millis(5));
                                    if cancelled_ref.load(Ordering::Relaxed) {
                                        break;
                                    }
                                }

                                let executor =
                                    Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                                let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
                                let start = Instant::now();
                                let result = executor.execute(subplan_ref, Some(&txn));
                                let latency_us = start.elapsed().as_micros() as u64;
                                let _ = shard.txn_mgr.commit(txn.txn_id);

                                // Check timeout and signal cancellation
                                if total_start.elapsed() > timeout {
                                    cancelled_ref.store(true, Ordering::Relaxed);
                                    return Err(FalconError::Transient {
                                        reason: format!(
                                            "DistPlan timeout after {}ms (shard {:?}, latency={}us)",
                                            timeout.as_millis(), shard_id, latency_us,
                                        ),
                                        retry_after_ms: 100,
                                    });
                                }

                                match result {
                                    Ok(ExecutionResult::Query { columns, rows }) => {
                                        return Ok(ShardResult {
                                            shard_id,
                                            columns,
                                            rows,
                                            latency_us,
                                        });
                                    }
                                    Ok(_) => {
                                        return Err(FalconError::internal_bug(
                                            "E-QE-014",
                                            "DistPlan subplan must return Query result",
                                            format!("shard {:?}", shard_id),
                                        ));
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "Shard {:?} attempt {}/{} failed: {}",
                                            shard_id, attempt + 1, max_attempts, e,
                                        );
                                        last_err = Some(e);
                                    }
                                }
                            }

                            Err(last_err.unwrap_or_else(|| FalconError::Internal(
                                format!("Shard {:?} failed after {} attempts", shard_id, max_attempts),
                            )))
                        })
                    })
                    .collect();

                handles.into_iter()
                    .map(|h| h.join().unwrap_or_else(|_| Err(FalconError::internal_bug(
                        "E-QE-005", "shard thread panicked", "exec_dist_plan_scatter"
                    ))))
                    .collect()
            });

        // Collect results — partial failure resilience: log errors, continue with successful shards.
        let mut collected: Vec<ShardResult> = Vec::with_capacity(target_shards.len());
        let mut failed_shards: Vec<(ShardId, String)> = Vec::new();
        for (i, r) in shard_results.into_iter().enumerate() {
            match r {
                Ok(sr) => collected.push(sr),
                Err(e) => {
                    let sid = target_shards.get(i).copied().unwrap_or(ShardId(i as u64));
                    tracing::warn!("Shard {:?} failed during scatter: {}", sid, e);
                    failed_shards.push((sid, format!("{}", e)));
                }
            }
        }
        // If ALL shards failed, propagate the error.
        if collected.is_empty() {
            let msgs: Vec<String> = failed_shards.iter()
                .map(|(sid, msg)| format!("shard_{}: {}", sid.0, msg))
                .collect();
            return Err(FalconError::Transient {
                reason: format!(
                    "All {} shards failed: {}", target_shards.len(), msgs.join("; "),
                ),
                retry_after_ms: 200,
            });
        }

        // ── Gather: merge using the same logic as DistributedExecutor ──
        let columns = collected.first().map(|r| r.columns.clone()).unwrap_or_default();
        let total_rows: usize = collected.iter().map(|r| r.rows.len()).sum();

        let merged_rows = match gather {
            DistGather::Union { distinct, limit, offset } => {
                let mut all = Vec::with_capacity(total_rows);
                for sr in &collected {
                    all.extend(sr.rows.iter().cloned());
                }
                if *distinct {
                    // O(N) distinct dedup while preserving first-seen order.
                    let mut seen: HashSet<Vec<Datum>> = HashSet::with_capacity(all.len());
                    all.retain(|row| seen.insert(row.values.clone()));
                }
                // Apply post-gather OFFSET
                if let Some(off) = offset {
                    if *off < all.len() {
                        all = all.split_off(*off);
                    } else {
                        all.clear();
                    }
                }
                if let Some(lim) = limit {
                    all.truncate(*lim);
                }
                all
            }
            DistGather::MergeSortLimit { sort_columns, limit, offset } => {
                // Fast path: if each shard output is already sorted by `sort_columns`,
                // do k-way merge (O(N log k)) with early stop at OFFSET+LIMIT.
                let is_pre_sorted = collected.iter().all(|sr| {
                    sr.rows.windows(2).all(|w| {
                        let a = &w[0];
                        let b = &w[1];
                        for &(col_idx, ascending) in sort_columns {
                            let va = a.values.get(col_idx);
                            let vb = b.values.get(col_idx);
                            let ord = crate::distributed_exec::compare_datums(va, vb);
                            let ord = if ascending { ord } else { ord.reverse() };
                            if ord != std::cmp::Ordering::Equal {
                                return ord != std::cmp::Ordering::Greater;
                            }
                        }
                        true
                    })
                });

                if is_pre_sorted {
                    use std::cmp::Reverse;
                    use std::collections::BinaryHeap;

                    let need = match (offset, limit) {
                        (Some(o), Some(l)) => Some(o + l),
                        _ => None, // no early termination
                    };

                    // (Reverse for min-heap) entries: (sort_key, shard_idx, row_idx)
                    struct MergeEntry {
                        row: OwnedRow,
                        shard_idx: usize,
                        row_idx: usize,
                        sort_columns: Arc<Vec<(usize, bool)>>,
                    }
                    impl PartialEq for MergeEntry {
                        fn eq(&self, other: &Self) -> bool {
                            self.cmp(other) == std::cmp::Ordering::Equal
                        }
                    }
                    impl Eq for MergeEntry {}
                    impl PartialOrd for MergeEntry {
                        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                            Some(self.cmp(other))
                        }
                    }
                    impl Ord for MergeEntry {
                        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                            for &(col_idx, ascending) in self.sort_columns.iter() {
                                let va = self.row.values.get(col_idx);
                                let vb = other.row.values.get(col_idx);
                                let ord = crate::distributed_exec::compare_datums(va, vb);
                                let ord = if ascending { ord } else { ord.reverse() };
                                if ord != std::cmp::Ordering::Equal {
                                    return ord;
                                }
                            }
                            std::cmp::Ordering::Equal
                        }
                    }

                    // Build shard row slices
                    let shard_rows: Vec<&[OwnedRow]> = collected.iter()
                        .map(|sr| sr.rows.as_slice())
                        .collect();

                    // Initialize min-heap with first row from each non-empty shard
                    let sort_cols = Arc::new(sort_columns.clone());
                    let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();
                    for (si, rows) in shard_rows.iter().enumerate() {
                        if !rows.is_empty() {
                            heap.push(Reverse(MergeEntry {
                                row: rows[0].clone(),
                                shard_idx: si,
                                row_idx: 0,
                                sort_columns: sort_cols.clone(),
                            }));
                        }
                    }

                    let mut merged = Vec::with_capacity(need.unwrap_or(total_rows));
                    while let Some(Reverse(entry)) = heap.pop() {
                        merged.push(entry.row);
                        // Early termination: stop once we have enough for offset+limit
                        if let Some(n) = need {
                            if merged.len() >= n {
                                break;
                            }
                        }
                        // Push next row from the same shard
                        let next_idx = entry.row_idx + 1;
                        if next_idx < shard_rows[entry.shard_idx].len() {
                            heap.push(Reverse(MergeEntry {
                                row: shard_rows[entry.shard_idx][next_idx].clone(),
                                shard_idx: entry.shard_idx,
                                row_idx: next_idx,
                                sort_columns: sort_cols.clone(),
                            }));
                        }
                    }

                    // Apply offset
                    if let Some(off) = offset {
                        if *off < merged.len() {
                            merged = merged.split_off(*off);
                        } else {
                            merged.clear();
                        }
                    }
                    // Apply limit
                    if let Some(lim) = limit {
                        merged.truncate(*lim);
                    }
                    merged
                } else {
                    // Fallback: unsorted per-shard results, gather-all and sort.
                    let mut all = Vec::with_capacity(total_rows);
                    for sr in &collected {
                        all.extend(sr.rows.iter().cloned());
                    }
                    all.sort_by(|a, b| {
                        for &(col_idx, ascending) in sort_columns {
                            let va = a.values.get(col_idx);
                            let vb = b.values.get(col_idx);
                            let ord = crate::distributed_exec::compare_datums(va, vb);
                            let ord = if ascending { ord } else { ord.reverse() };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    if let Some(off) = offset {
                        if *off < all.len() {
                            all = all.split_off(*off);
                        } else {
                            all.clear();
                        }
                    }
                    if let Some(lim) = limit {
                        all.truncate(*lim);
                    }
                    all
                }
            }
            DistGather::TwoPhaseAgg {
                group_by_indices,
                agg_merges,
                having,
                avg_fixups,
                visible_columns,
                order_by: post_sort,
                limit: post_limit,
                offset: post_offset,
            } => {
                let cluster_merges: Vec<AggMerge> = agg_merges
                    .iter()
                    .map(|m| match m {
                        DistAggMerge::Sum(i) => AggMerge::Sum(*i),
                        DistAggMerge::Count(i) => AggMerge::Count(*i),
                        DistAggMerge::Min(i) => AggMerge::Min(*i),
                        DistAggMerge::Max(i) => AggMerge::Max(*i),
                        DistAggMerge::BoolAnd(i) => AggMerge::BoolAnd(*i),
                        DistAggMerge::BoolOr(i) => AggMerge::BoolOr(*i),
                        DistAggMerge::StringAgg(i, ref sep) => AggMerge::StringAgg(*i, sep.clone()),
                        DistAggMerge::ArrayAgg(i) => AggMerge::ArrayAgg(*i),
                        DistAggMerge::CountDistinct(i) => AggMerge::CountDistinct(*i),
                        DistAggMerge::SumDistinct(i) => AggMerge::SumDistinct(*i),
                        DistAggMerge::AvgDistinct(i) => AggMerge::AvgDistinct(*i),
                        DistAggMerge::StringAggDistinct(i, ref sep) => AggMerge::StringAggDistinct(*i, sep.clone()),
                        DistAggMerge::ArrayAggDistinct(i) => AggMerge::ArrayAggDistinct(*i),
                    })
                    .collect();
                let mut merged = crate::distributed_exec::merge_two_phase_agg(
                    &collected,
                    group_by_indices,
                    &cluster_merges,
                );
                // Apply AVG fixups: row[sum_idx] = row[sum_idx] / row[count_idx]
                for row in &mut merged {
                    for &(sum_idx, count_idx) in avg_fixups {
                        let sum_val = row.values.get(sum_idx).cloned().unwrap_or(Datum::Null);
                        let count_val = row.values.get(count_idx).cloned().unwrap_or(Datum::Null);
                        let avg = match (&sum_val, &count_val) {
                            (_, Datum::Int64(0)) | (_, Datum::Null) => Datum::Null,
                            (Datum::Int64(s), Datum::Int64(c)) => Datum::Float64(*s as f64 / *c as f64),
                            (Datum::Int32(s), Datum::Int64(c)) => Datum::Float64(*s as f64 / *c as f64),
                            (Datum::Float64(s), Datum::Int64(c)) => Datum::Float64(s / *c as f64),
                            _ => Datum::Null,
                        };
                        if let Some(v) = row.values.get_mut(sum_idx) {
                            *v = avg;
                        }
                    }
                    // Truncate hidden COUNT columns
                    row.values.truncate(*visible_columns);
                }
                // Apply HAVING filter post-merge (was stripped from subplan)
                if let Some(having_expr) = having {
                    merged.retain(|row| {
                        falcon_executor::expr_engine::ExprEngine::eval_filter(having_expr, row).unwrap_or(false)
                    });
                }
                // Apply post-merge ORDER BY
                if !post_sort.is_empty() {
                    merged.sort_by(|a, b| {
                        for &(col_idx, ascending) in post_sort {
                            let va = a.values.get(col_idx);
                            let vb = b.values.get(col_idx);
                            let ord = crate::distributed_exec::compare_datums(va, vb);
                            let ord = if ascending { ord } else { ord.reverse() };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                // Apply post-merge OFFSET
                if let Some(off) = post_offset {
                    if *off < merged.len() {
                        merged = merged.split_off(*off);
                    } else {
                        merged.clear();
                    }
                }
                // Apply post-merge LIMIT
                if let Some(lim) = post_limit {
                    merged.truncate(*lim);
                }
                merged
            }
        };

        // Record scatter stats for observability
        let gather_strategy = match gather {
            DistGather::Union { .. } => "Union",
            DistGather::MergeSortLimit { .. } => "MergeSortLimit",
            DistGather::TwoPhaseAgg { .. } => "TwoPhaseAgg",
        };
        let per_shard_latency: Vec<(u64, u64)> = collected
            .iter()
            .map(|sr| (sr.shard_id.0, sr.latency_us))
            .collect();
        let per_shard_rows: Vec<(u64, usize)> = collected
            .iter()
            .map(|sr| (sr.shard_id.0, sr.rows.len()))
            .collect();
        // Check gather-phase timeout
        if total_start.elapsed() > timeout {
            return Err(FalconError::Internal(format!(
                "DistPlan gather-phase timeout after {}ms",
                timeout.as_millis(),
            )));
        }

        let failed_shard_ids: Vec<u64> = failed_shards.iter().map(|(sid, _)| sid.0).collect();
        let merge_labels = if let DistGather::TwoPhaseAgg { agg_merges, .. } = gather {
            agg_merges.iter().map(|m| m.to_string()).collect()
        } else {
            vec![]
        };
        if let Ok(mut stats) = self.last_scatter_stats.lock() {
            *stats = ScatterStats {
                shards_participated: collected.len(),
                total_rows_gathered: merged_rows.len(),
                per_shard_latency_us: per_shard_latency,
                per_shard_row_count: per_shard_rows,
                gather_strategy: gather_strategy.into(),
                total_latency_us: total_start.elapsed().as_micros() as u64,
                failed_shards: failed_shard_ids,
                merge_labels,
                pruned_to_shard: None,
            };
        }

        Ok(ExecutionResult::Query {
            columns,
            rows: merged_rows,
        })
    }
}
