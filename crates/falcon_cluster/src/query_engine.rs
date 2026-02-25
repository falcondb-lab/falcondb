//! Distributed query engine: dispatches PhysicalPlan execution.
//!
//! For local plans (SeqScan, Insert, etc.), delegates to the per-shard Executor.
//! For DistPlan, uses parallel scatter/gather across shards, where each shard
//! runs its own Executor with its own transaction.
//! For multi-shard writes, uses TwoPhaseCoordinator for atomic commits.
//!
//! Split into submodules for maintainability:
//! - `routing` — PK-based shard routing, IN-list pruning, replica selection
//! - `coordinator` — coordinator-side join/subquery, plan walking, subquery materialization
//! - `explain` — EXPLAIN output formatting
//! - `dml` — DDL propagation, INSERT splitting, UPDATE/DELETE broadcast, GC
//! - `scatter` — scatter/gather execution for distributed plans

mod coordinator;
mod dml;
mod explain;
mod routing;
mod scatter;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use falcon_common::error::FalconError;
use falcon_common::types::{IsolationLevel, ShardId};
use falcon_executor::executor::{ExecutionResult, Executor};
use falcon_planner::plan::PhysicalPlan;
use falcon_txn::TxnHandle;

use crate::distributed_exec::DistributedExecutor;
use crate::sharded_engine::ShardedEngine;
use crate::two_phase::TwoPhaseCoordinator;

// Re-export gateway types so `pub use query_engine::{ .. }` in lib.rs still works.
pub use crate::gateway::{
    GatewayAdmissionConfig, GatewayAdmissionControl, GatewayDisposition, GatewayMetrics,
    GatewayMetricsSnapshot, ScatterStats,
};

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
    gateway_metrics: GatewayMetrics,
    admission: GatewayAdmissionControl,
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
            gateway_metrics: GatewayMetrics::new(),
            admission: GatewayAdmissionControl::default(),
        }
    }

    /// Create with explicit admission control config.
    pub fn new_with_admission(
        engine: Arc<ShardedEngine>,
        timeout: Duration,
        admission_config: GatewayAdmissionConfig,
    ) -> Self {
        let dist_exec = DistributedExecutor::new(engine.clone(), timeout);
        let two_pc = TwoPhaseCoordinator::new(engine.clone(), timeout);
        Self {
            engine,
            dist_exec,
            two_pc,
            timeout,
            last_scatter_stats: Mutex::new(ScatterStats::default()),
            gateway_metrics: GatewayMetrics::new(),
            admission: GatewayAdmissionControl::new(admission_config),
        }
    }

    /// Get the last scatter/gather stats for observability.
    pub fn last_scatter_stats(&self) -> ScatterStats {
        self.last_scatter_stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Get a snapshot of gateway forwarding metrics.
    pub fn gateway_metrics_snapshot(&self) -> GatewayMetricsSnapshot {
        self.gateway_metrics.snapshot()
    }

    /// Get current admission control counters.
    pub fn admission_snapshot(&self) -> (u64, u64) {
        (self.admission.inflight(), self.admission.forwarded())
    }

    /// Health check: probe each shard with a trivial read transaction.
    /// Returns per-shard status: (shard_id, healthy, latency_us).
    pub fn shard_health_check(&self) -> Vec<(ShardId, bool, u64)> {
        let shard_ids: Vec<ShardId> = self
            .engine
            .all_shards()
            .iter()
            .map(|s| s.shard_id)
            .collect();
        std::thread::scope(|s| {
            let handles: Vec<_> = shard_ids
                .iter()
                .map(|&sid| {
                    let engine = &self.engine;
                    s.spawn(move || {
                        let start = Instant::now();
                        engine.shard(sid).map_or(
                            (sid, false, start.elapsed().as_micros() as u64),
                            |shard| {
                                let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
                                let _ = shard.txn_mgr.commit(txn.txn_id);
                                (sid, true, start.elapsed().as_micros() as u64)
                            },
                        )
                    })
                })
                .collect();
            handles.into_iter().filter_map(|h| h.join().ok()).collect()
        })
    }

    /// Execute a parameterized physical plan across shards.
    ///
    /// Substitutes `BoundExpr::Parameter(idx)` nodes with concrete `Datum` values
    /// from `params`, then delegates to `execute()` for full distributed routing.
    ///
    /// This is the distributed counterpart to `Executor::execute_with_params`.
    /// Without this, parameterized queries would bypass the distributed engine
    /// and silently execute on shard 0 only.
    pub fn execute_with_params(
        &self,
        plan: &PhysicalPlan,
        txn: Option<&TxnHandle>,
        params: &[falcon_common::datum::Datum],
    ) -> Result<ExecutionResult, FalconError> {
        if params.is_empty() {
            return self.execute(plan, txn);
        }
        let substituted =
            falcon_executor::param_subst::substitute_params_plan(plan, params)
                .map_err(FalconError::Execution)?;
        self.execute(&substituted, txn)
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
            PhysicalPlan::Explain(inner)
                if matches!(
                    **inner,
                    PhysicalPlan::NestedLoopJoin { .. } | PhysicalPlan::HashJoin { .. }
                ) =>
            {
                self.exec_explain_coordinator_join(inner)
            }

            // EXPLAIN wrapping a bare SeqScan (subquery/CTE/UNION): show coordinator-side plan
            PhysicalPlan::Explain(inner)
                if matches!(**inner, PhysicalPlan::SeqScan { .. })
                    && self.engine.shard_ids().len() > 1 =>
            {
                self.exec_explain_coordinator_subquery(inner)
            }

            // EXPLAIN ANALYZE: execute on shard 0 (local executor handles timing)
            PhysicalPlan::ExplainAnalyze(_) => {
                let shard = self.engine.shard(ShardId(0)).ok_or_else(|| {
                    FalconError::internal_bug(
                        "E-QE-001",
                        "No shard 0 available",
                        "ExplainAnalyze dispatch",
                    )
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
            PhysicalPlan::Update {
                table_id,
                schema,
                assignments,
                filter,
                returning,
                from_table,
            } => {
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
            PhysicalPlan::Delete {
                table_id,
                schema,
                filter,
                returning,
                using_table,
            } => {
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
            PhysicalPlan::NestedLoopJoin { .. } | PhysicalPlan::HashJoin { .. } => {
                self.exec_coordinator_join(plan, txn)
            }

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
                    FalconError::internal_bug(
                        "E-QE-002",
                        "No shard 0 available",
                        "default plan dispatch",
                    )
                })?;
                let local_exec = Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                local_exec.execute(other, txn)
            }
        }
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
                (
                    i as u64,
                    table_count,
                    snap.total_committed,
                    snap.total_aborted,
                    snap.active_count,
                )
            })
            .collect()
    }

    /// Per-shard detailed load with per-table breakdown (SHOW falcon.shards).
    pub fn shard_load_detailed(&self) -> Vec<crate::rebalancer::ShardLoadDetailed> {
        crate::rebalancer::ShardLoadDetailed::collect_all(&self.engine)
    }

    /// Get the rebalancer status (SHOW falcon.rebalance_status).
    /// Returns a default status if no rebalancer is attached.
    pub const fn rebalance_status(&self) -> crate::rebalancer::RebalancerStatus {
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
        let migrator =
            crate::rebalancer::ShardMigrator::new(crate::rebalancer::RebalancerConfig::default());
        let mut completed = 0usize;
        let mut failed = 0usize;
        let mut rows_migrated = 0u64;

        for task in &plan.tasks {
            let status = migrator.execute_task(task, &self.engine);
            if status.phase == crate::rebalancer::MigrationPhase::Completed {
                completed += 1;
                rows_migrated += status.rows_migrated;
            } else {
                failed += 1;
                if let Some(ref err) = status.error {
                    tracing::warn!(
                        "Rebalance task failed: {:?} → {:?} ({}): {}",
                        task.source_shard,
                        task.target_shard,
                        task.table_name,
                        err
                    );
                }
            }
        }

        (completed, failed, rows_migrated)
    }

    /// Access the underlying two-phase coordinator for multi-shard writes.
    pub const fn two_phase_coordinator(&self) -> &TwoPhaseCoordinator {
        &self.two_pc
    }

    /// Access the underlying distributed executor for closure-based scatter/gather.
    pub const fn distributed_executor(&self) -> &DistributedExecutor {
        &self.dist_exec
    }

    /// Access the underlying sharded engine.
    pub fn engine(&self) -> &ShardedEngine {
        &self.engine
    }
}