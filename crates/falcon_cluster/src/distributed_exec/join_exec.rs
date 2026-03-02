//! Distributed JOIN execution orchestration.
//!
//! Wires `BroadcastJoinPlan` and `HashPartitionJoinPlan` into the scatter/gather
//! execution pipeline via `ShardExecBackend`. The coordinator:
//!
//! - **Broadcast join**: gathers the small table, creates `BroadcastJoin` subplans,
//!   scatters them to all shards for local hash-join with the partitioned table.
//! - **Hash-partition join**: gathers both tables, repartitions by join-key hash,
//!   creates per-shard `HashPartitionJoin` subplans, scatters for local join.

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_common::types::{DataType, ShardId, TableId};

use super::backend::ShardExecBackend;
use super::scatter::DistributedExecutor;
use super::serializable_plan::SerializableSubPlan;
use super::{GatherStrategy, ScatterGatherMetrics, SubPlanResult};

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Join Execution Context
// ═══════════════════════════════════════════════════════════════════════════

/// All metadata needed to execute a distributed join.
///
/// Constructed by the query planner after strategy selection; passed to
/// `execute_broadcast_join` or `execute_hash_partition_join`.
#[derive(Debug, Clone)]
pub struct JoinExecContext {
    pub left_table_id: TableId,
    pub right_table_id: TableId,
    pub left_columns: Vec<(String, DataType)>,
    pub right_columns: Vec<(String, DataType)>,
    pub left_key_col: usize,
    pub right_key_col: usize,
    /// Shards to execute the join on.
    pub target_shards: Vec<ShardId>,
}

impl JoinExecContext {
    /// Compute the combined output column metadata (left ++ right).
    pub fn output_columns(&self) -> Vec<(String, DataType)> {
        let mut out = self.left_columns.clone();
        out.extend(self.right_columns.iter().cloned());
        out
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Broadcast Join Execution
// ═══════════════════════════════════════════════════════════════════════════

/// Execute a broadcast join via the scatter/gather pipeline.
///
/// Steps:
/// 1. Scatter a `Scan` on `broadcast_table` to all shards → gather all rows
/// 2. Build a `BroadcastJoin` subplan embedding the gathered rows
/// 3. Scatter the join plan to all shards via `backend`
/// 4. Each shard joins broadcast data with its local partition
/// 5. Union all shard results
///
/// The `broadcast_table` is the **smaller** table (determined by the cost model).
pub fn execute_broadcast_join(
    executor: &DistributedExecutor,
    ctx: &JoinExecContext,
    broadcast_table: TableId,
    backend: &dyn ShardExecBackend,
) -> Result<(SubPlanResult, ScatterGatherMetrics), FalconError> {
    // Determine which side is broadcast vs local.
    let (bc_table, bc_cols, bc_key, local_table, local_cols, local_key) =
        if broadcast_table == ctx.left_table_id {
            (
                ctx.left_table_id,
                &ctx.left_columns,
                ctx.left_key_col,
                ctx.right_table_id,
                &ctx.right_columns,
                ctx.right_key_col,
            )
        } else {
            (
                ctx.right_table_id,
                &ctx.right_columns,
                ctx.right_key_col,
                ctx.left_table_id,
                &ctx.left_columns,
                ctx.left_key_col,
            )
        };

    // Step 1: Gather all rows of the broadcast (small) table.
    let scan_plan = SerializableSubPlan::Scan {
        table_id: bc_table.0,
        projection: vec![],
        columns: bc_cols.clone(),
    };

    let gather_strategy = GatherStrategy::Union {
        distinct: false,
        limit: None,
        offset: None,
    };

    let ((_, broadcast_rows), _scan_metrics) = executor.scatter_gather_serializable(
        &scan_plan,
        &ctx.target_shards,
        &gather_strategy,
        backend,
    )?;

    tracing::debug!(
        broadcast_table = bc_table.0,
        broadcast_rows = broadcast_rows.len(),
        "Broadcast join: gathered small table rows"
    );

    // Step 2–4: Create BroadcastJoin plan and scatter to all shards.
    let output_columns = ctx.output_columns();
    let join_plan = SerializableSubPlan::BroadcastJoin {
        broadcast_rows,
        broadcast_columns: bc_cols.clone(),
        local_table_id: local_table.0,
        local_columns: local_cols.clone(),
        broadcast_key_col: bc_key,
        local_key_col: local_key,
        output_columns: output_columns.clone(),
    };

    let join_gather = GatherStrategy::Union {
        distinct: false,
        limit: None,
        offset: None,
    };

    executor.scatter_gather_serializable(
        &join_plan,
        &ctx.target_shards,
        &join_gather,
        backend,
    )
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Hash-Partition Join Execution
// ═══════════════════════════════════════════════════════════════════════════

/// Execute a hash-partition join via the scatter/gather pipeline.
///
/// Steps:
/// 1. Scatter `Scan` on both tables to all shards → gather all rows
/// 2. Hash-partition both sides by join key into N buckets (one per shard)
/// 3. Create per-shard `HashPartitionJoin` subplans with co-located data
/// 4. Scatter per-shard plans via `scatter_gather_per_shard`
/// 5. Union all shard results
pub fn execute_hash_partition_join(
    executor: &DistributedExecutor,
    ctx: &JoinExecContext,
    backend: &dyn ShardExecBackend,
) -> Result<(SubPlanResult, ScatterGatherMetrics), FalconError> {
    let gather_union = GatherStrategy::Union {
        distinct: false,
        limit: None,
        offset: None,
    };

    // Step 1a: Gather all left-table rows.
    let left_scan = SerializableSubPlan::Scan {
        table_id: ctx.left_table_id.0,
        projection: vec![],
        columns: ctx.left_columns.clone(),
    };
    let ((_, left_rows), _) = executor.scatter_gather_serializable(
        &left_scan,
        &ctx.target_shards,
        &gather_union,
        backend,
    )?;

    // Step 1b: Gather all right-table rows.
    let right_scan = SerializableSubPlan::Scan {
        table_id: ctx.right_table_id.0,
        projection: vec![],
        columns: ctx.right_columns.clone(),
    };
    let ((_, right_rows), _) = executor.scatter_gather_serializable(
        &right_scan,
        &ctx.target_shards,
        &gather_union,
        backend,
    )?;

    tracing::debug!(
        left_rows = left_rows.len(),
        right_rows = right_rows.len(),
        num_partitions = ctx.target_shards.len(),
        "Hash-partition join: gathered both tables, partitioning"
    );

    // Step 2: Hash-partition both sides into N buckets.
    let num_partitions = ctx.target_shards.len();
    let mut left_buckets: Vec<Vec<OwnedRow>> = vec![Vec::new(); num_partitions];
    let mut right_buckets: Vec<Vec<OwnedRow>> = vec![Vec::new(); num_partitions];

    for row in &left_rows {
        let bucket = hash_partition_bucket(
            row.values.get(ctx.left_key_col),
            num_partitions,
        );
        left_buckets[bucket].push(row.clone());
    }
    for row in &right_rows {
        let bucket = hash_partition_bucket(
            row.values.get(ctx.right_key_col),
            num_partitions,
        );
        right_buckets[bucket].push(row.clone());
    }

    // Step 3: Create per-shard HashPartitionJoin plans.
    let output_columns = ctx.output_columns();
    let per_shard_plans: Vec<(ShardId, SerializableSubPlan)> = ctx
        .target_shards
        .iter()
        .enumerate()
        .map(|(i, &shard_id)| {
            let plan = SerializableSubPlan::HashPartitionJoin {
                left_rows: std::mem::take(&mut left_buckets[i]),
                left_columns: ctx.left_columns.clone(),
                left_key_col: ctx.left_key_col,
                right_rows: std::mem::take(&mut right_buckets[i]),
                right_columns: ctx.right_columns.clone(),
                right_key_col: ctx.right_key_col,
                output_columns: output_columns.clone(),
            };
            (shard_id, plan)
        })
        .collect();

    // Step 4–5: Scatter per-shard plans and union results.
    let join_gather = GatherStrategy::Union {
        distinct: false,
        limit: None,
        offset: None,
    };

    executor.scatter_gather_per_shard(
        &per_shard_plans,
        &join_gather,
        backend,
    )
}

/// Hash a datum value to a partition bucket index.
/// Uses a simple hash to distribute rows across N partitions.
fn hash_partition_bucket(datum: Option<&Datum>, num_partitions: usize) -> usize {
    if num_partitions == 0 {
        return 0;
    }
    let hash = match datum {
        None | Some(Datum::Null) => 0u64,
        Some(Datum::Int32(v)) => *v as u64,
        Some(Datum::Int64(v)) => *v as u64,
        Some(Datum::Text(s)) => {
            // FNV-1a hash for strings
            let mut h: u64 = 0xcbf29ce484222325;
            for b in s.bytes() {
                h ^= b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
            h
        }
        Some(Datum::Float64(v)) => v.to_bits(),
        Some(Datum::Boolean(b)) => *b as u64,
        _ => {
            // Fallback: serialize and hash the bytes
            let bytes = bincode::serialize(&datum).unwrap_or_default();
            let mut h: u64 = 0xcbf29ce484222325;
            for b in bytes {
                h ^= b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
            h
        }
    };
    (hash % num_partitions as u64) as usize
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_partition_bucket_distribution() {
        let num = 4;
        // Different int values should distribute across buckets
        let b0 = hash_partition_bucket(Some(&Datum::Int64(0)), num);
        let b1 = hash_partition_bucket(Some(&Datum::Int64(1)), num);
        let b4 = hash_partition_bucket(Some(&Datum::Int64(4)), num);
        assert!(b0 < num);
        assert!(b1 < num);
        assert_eq!(b0, b4); // 0 % 4 == 4 % 4

        // Null goes to bucket 0
        assert_eq!(hash_partition_bucket(None, num), 0);
        assert_eq!(hash_partition_bucket(Some(&Datum::Null), num), 0);
    }

    #[test]
    fn test_hash_partition_bucket_strings() {
        let num = 8;
        let b_a = hash_partition_bucket(Some(&Datum::Text("alice".into())), num);
        let b_b = hash_partition_bucket(Some(&Datum::Text("bob".into())), num);
        assert!(b_a < num);
        assert!(b_b < num);
        // Same string should always map to same bucket
        let b_a2 = hash_partition_bucket(Some(&Datum::Text("alice".into())), num);
        assert_eq!(b_a, b_a2);
    }

    #[test]
    fn test_join_exec_context_output_columns() {
        let ctx = JoinExecContext {
            left_table_id: TableId(1),
            right_table_id: TableId(2),
            left_columns: vec![("a".into(), DataType::Int64)],
            right_columns: vec![("b".into(), DataType::Text)],
            left_key_col: 0,
            right_key_col: 0,
            target_shards: vec![ShardId(0)],
        };
        let out = ctx.output_columns();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, "a");
        assert_eq!(out[1].0, "b");
    }
}
