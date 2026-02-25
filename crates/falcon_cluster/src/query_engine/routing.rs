//! PK-based shard routing and replica selection for DistributedQueryEngine.

use falcon_common::datum::Datum;
use falcon_common::error::FalconError;
use falcon_common::types::ShardId;
use falcon_executor::executor::{ExecutionResult, Executor};
use falcon_planner::plan::PhysicalPlan;
use falcon_sql_frontend::types::BoundExpr;
use falcon_txn::TxnHandle;

impl super::DistributedQueryEngine {
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
        replica_metrics: &[(
            ShardId,
            crate::replication::runner::ReplicaRunnerMetricsSnapshot,
        )],
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
                format!("Shard {shard_id:?} not found"),
                "execute_on_shard dispatch",
            )
        })?;
        let local_exec = Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
        local_exec.execute(plan, txn)
    }

    /// Resolve the target shard for a query by extracting a PK equality from the
    /// filter expression. Handles bare `pk = literal`, AND chains containing
    /// `pk = literal`, and returns None otherwise.
    pub(crate) fn resolve_filter_shard(
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
            BoundExpr::BinaryOp {
                op: BinOp::Eq,
                left,
                right,
            } => {
                if let Some(key) = self.match_pk_eq(pk_col_idx, left, right) {
                    return Some(key);
                }
                if let Some(key) = self.match_pk_eq(pk_col_idx, right, left) {
                    return Some(key);
                }
                None
            }
            // AND chain: recurse into both sides
            BoundExpr::BinaryOp {
                op: BinOp::And,
                left,
                right,
            } => self
                .extract_pk_key(schema, left)
                .or_else(|| self.extract_pk_key(schema, right)),
            _ => None,
        }
    }

    /// Resolve target shards for an IN-list filter: `pk IN (1, 5, 9)`.
    /// Returns a deduplicated set of shards, or None if not an IN-list on PK.
    pub(crate) fn resolve_in_list_shards(
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
        self.extract_in_list_keys(pk_col_idx, filter).map(|keys| {
            let mut shards: Vec<ShardId> =
                keys.iter().map(|k| self.engine.shard_for_key(*k)).collect();
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
            BoundExpr::InList {
                expr,
                list,
                negated,
            } if !negated => {
                if let BoundExpr::ColumnRef(idx) = expr.as_ref() {
                    if *idx == pk_col_idx {
                        let keys: Vec<i64> = list
                            .iter()
                            .filter_map(|e| self.extract_int_key(e))
                            .collect();
                        if keys.len() == list.len() {
                            return Some(keys);
                        }
                    }
                }
                None
            }
            BoundExpr::BinaryOp {
                op: BinOp::And,
                left,
                right,
            } => self
                .extract_in_list_keys(pk_col_idx, left)
                .or_else(|| self.extract_in_list_keys(pk_col_idx, right)),
            _ => None,
        }
    }

    /// Check if (col_expr, val_expr) is (ColumnRef(pk_col_idx), Literal(int)).
    const fn match_pk_eq(
        &self,
        pk_col_idx: usize,
        col_expr: &falcon_sql_frontend::types::BoundExpr,
        val_expr: &falcon_sql_frontend::types::BoundExpr,
    ) -> Option<i64> {
        use falcon_sql_frontend::types::BoundExpr;
        match col_expr {
            BoundExpr::ColumnRef(idx) if *idx == pk_col_idx => self.extract_int_key(val_expr),
            _ => None,
        }
    }

    /// Check if a subplan is a SeqScan with a PK equality filter that can be
    /// routed to a single shard. Returns the target shard if so.
    pub(crate) fn try_single_shard_scan(&self, subplan: &PhysicalPlan) -> Option<ShardId> {
        match subplan {
            PhysicalPlan::SeqScan { schema, filter, .. } => {
                self.resolve_filter_shard(schema, filter.as_ref())
            }
            _ => None,
        }
    }

    /// Check if a subplan is a SeqScan with a PK IN-list filter that can be
    /// pruned to a subset of shards. Returns the pruned shard list if so.
    pub(crate) fn try_in_list_shard_prune(&self, subplan: &PhysicalPlan) -> Option<Vec<ShardId>> {
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
    pub(crate) const fn extract_int_key(&self, expr: &BoundExpr) -> Option<i64> {
        match expr {
            BoundExpr::Literal(Datum::Int32(v)) => Some(*v as i64),
            BoundExpr::Literal(Datum::Int64(v)) => Some(*v),
            _ => None,
        }
    }
}
