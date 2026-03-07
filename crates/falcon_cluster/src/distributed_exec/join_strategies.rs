//! Cross-shard JOIN optimization strategies.
//!
//! Provides three strategies for distributed joins, selected by estimated cost:
//!
//! - **GatherAll** (status quo): pull all rows to coordinator, join locally.
//!   Best for tiny tables or when both sides are small.
//! - **BroadcastJoin**: replicate the smaller table to every shard, execute
//!   the join on each shard in parallel, union the results.
//!   Best when one side is much smaller than the other.
//! - **HashPartitionJoin**: repartition both sides by join key hash, then
//!   join co-located partitions in parallel.
//!   Best when both sides are large and the join key is selective.
//!
//! # Cost Model
//!
//! Strategy selection uses a simple row-count heuristic:
//! - If both tables have ≤ `GATHER_ALL_THRESHOLD` rows → GatherAll
//! - If the smaller table has ≤ `BROADCAST_THRESHOLD` rows → BroadcastJoin
//! - Otherwise → HashPartitionJoin

use std::fmt;

use falcon_common::types::{ShardId, TableId};

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Strategy Enum
// ═══════════════════════════════════════════════════════════════════════════

/// Selected join distribution strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinStrategy {
    /// Pull all rows to coordinator and join locally (current default).
    GatherAll,
    /// Broadcast the smaller table to all shards.
    Broadcast {
        /// Table to broadcast (the smaller one).
        broadcast_table: TableId,
        /// Table that stays partitioned on its shards.
        partitioned_table: TableId,
    },
    /// Repartition both sides by join key hash, then join co-located partitions.
    HashPartition {
        left_table: TableId,
        right_table: TableId,
        /// Column index in the left table used as the join/partition key.
        left_key_col: usize,
        /// Column index in the right table used as the join/partition key.
        right_key_col: usize,
    },
}

impl fmt::Display for JoinStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GatherAll => write!(f, "GatherAll"),
            Self::Broadcast {
                broadcast_table, ..
            } => write!(f, "Broadcast(small={:?})", broadcast_table),
            Self::HashPartition {
                left_key_col,
                right_key_col,
                ..
            } => write!(
                f,
                "HashPartition(left_key={}, right_key={})",
                left_key_col, right_key_col
            ),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Cost Model & Strategy Selection
// ═══════════════════════════════════════════════════════════════════════════

/// If both tables have at most this many rows, use GatherAll (simple, low overhead).
const GATHER_ALL_THRESHOLD: u64 = 10_000;

/// If the smaller table has at most this many rows, broadcast it.
const BROADCAST_THRESHOLD: u64 = 50_000;

/// Estimated table statistics for cost-based strategy selection.
#[derive(Debug, Clone)]
pub struct TableStats {
    pub table_id: TableId,
    pub estimated_rows: u64,
    /// Number of shards this table is distributed across.
    pub shard_count: u64,
}

/// Input for join strategy selection.
#[derive(Debug, Clone)]
pub struct JoinCostInput {
    pub left: TableStats,
    pub right: TableStats,
    /// Column index in left table for the equi-join key, if known.
    pub left_key_col: Option<usize>,
    /// Column index in right table for the equi-join key, if known.
    pub right_key_col: Option<usize>,
    /// Total number of shards in the cluster.
    pub num_shards: u64,
}

/// Select the optimal join strategy based on estimated table sizes.
///
/// This is a heuristic cost model — not a full optimizer. It uses simple
/// thresholds to choose between the three strategies.
pub fn select_join_strategy(input: &JoinCostInput) -> JoinStrategy {
    let left_rows = input.left.estimated_rows;
    let right_rows = input.right.estimated_rows;

    // Case 1: Both small → GatherAll is cheapest (no repartitioning overhead)
    if left_rows <= GATHER_ALL_THRESHOLD && right_rows <= GATHER_ALL_THRESHOLD {
        tracing::debug!(
            left_rows,
            right_rows,
            "JoinStrategy: GatherAll (both tables small)"
        );
        return JoinStrategy::GatherAll;
    }

    let (smaller_table, larger_table, smaller_rows) = if left_rows <= right_rows {
        (input.left.table_id, input.right.table_id, left_rows)
    } else {
        (input.right.table_id, input.left.table_id, right_rows)
    };

    // Case 2: One side is small → Broadcast
    if smaller_rows <= BROADCAST_THRESHOLD {
        tracing::debug!(
            broadcast_table = ?smaller_table,
            broadcast_rows = smaller_rows,
            partitioned_rows = left_rows.max(right_rows),
            "JoinStrategy: Broadcast (small table replicated to all shards)"
        );
        return JoinStrategy::Broadcast {
            broadcast_table: smaller_table,
            partitioned_table: larger_table,
        };
    }

    // Case 3: Both large → HashPartition (if equi-join keys are known)
    if let (Some(lk), Some(rk)) = (input.left_key_col, input.right_key_col) {
        tracing::debug!(
            left_rows,
            right_rows,
            left_key_col = lk,
            right_key_col = rk,
            "JoinStrategy: HashPartition (both tables large, equi-join)"
        );
        return JoinStrategy::HashPartition {
            left_table: input.left.table_id,
            right_table: input.right.table_id,
            left_key_col: lk,
            right_key_col: rk,
        };
    }

    // Fallback: no equi-join key → GatherAll (can't hash-partition non-equi joins)
    tracing::debug!(
        left_rows,
        right_rows,
        "JoinStrategy: GatherAll (fallback, no equi-join key for hash partition)"
    );
    JoinStrategy::GatherAll
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Broadcast Join Execution Plan
// ═══════════════════════════════════════════════════════════════════════════

/// Plan for executing a broadcast join across shards.
///
/// The coordinator:
/// 1. Gathers all rows of the broadcast table from all shards
/// 2. Sends (broadcasts) these rows to every shard
/// 3. Each shard joins its local partition of the large table with the broadcast data
/// 4. Coordinator unions all per-shard join results
#[derive(Debug, Clone)]
pub struct BroadcastJoinPlan {
    pub broadcast_table: TableId,
    pub partitioned_table: TableId,
    /// Shards to execute the join on (all shards that have data).
    pub target_shards: Vec<ShardId>,
    /// Estimated rows in the broadcast table.
    pub broadcast_rows: u64,
}

impl BroadcastJoinPlan {
    /// Estimate the network transfer cost (bytes) for this plan.
    /// Cost = broadcast_rows × avg_row_size × num_shards (broadcast) +
    ///        result_rows × avg_row_size (gather results)
    pub fn estimated_network_cost(&self, avg_row_bytes: u64) -> u64 {
        // Result gathering is hard to estimate without selectivity — use broadcast cost as proxy
        self.broadcast_rows * avg_row_bytes * self.target_shards.len() as u64
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Hash Partition Join Execution Plan
// ═══════════════════════════════════════════════════════════════════════════

/// Plan for executing a hash-partition join across shards.
///
/// The coordinator:
/// 1. Gathers all rows of both tables from all shards
/// 2. Hashes each row by its join key column
/// 3. Assigns rows to target shards based on hash
/// 4. Sends repartitioned data to each shard
/// 5. Each shard joins its partition of left with its partition of right
/// 6. Coordinator unions all per-shard join results
#[derive(Debug, Clone)]
pub struct HashPartitionJoinPlan {
    pub left_table: TableId,
    pub right_table: TableId,
    /// Column index in the left table used as the partition key.
    pub left_key_col: usize,
    /// Column index in the right table used as the partition key.
    pub right_key_col: usize,
    /// Number of partitions (typically = number of shards).
    pub num_partitions: u64,
    /// Estimated total rows from both sides.
    pub total_rows: u64,
}

impl HashPartitionJoinPlan {
    /// Estimate the network transfer cost.
    /// Cost = total_rows × avg_row_size × 2 (repartition + gather results)
    pub fn estimated_network_cost(&self, avg_row_bytes: u64) -> u64 {
        self.total_rows * avg_row_bytes * 2
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_input(left_rows: u64, right_rows: u64, has_equi_key: bool) -> JoinCostInput {
        JoinCostInput {
            left: TableStats {
                table_id: TableId(1),
                estimated_rows: left_rows,
                shard_count: 4,
            },
            right: TableStats {
                table_id: TableId(2),
                estimated_rows: right_rows,
                shard_count: 4,
            },
            left_key_col: if has_equi_key { Some(0) } else { None },
            right_key_col: if has_equi_key { Some(1) } else { None },
            num_shards: 4,
        }
    }

    #[test]
    fn test_both_small_tables_use_gather_all() {
        let input = make_input(100, 500, true);
        assert_eq!(select_join_strategy(&input), JoinStrategy::GatherAll);
    }

    #[test]
    fn test_one_small_one_large_uses_broadcast() {
        let input = make_input(1_000, 1_000_000, true);
        let strategy = select_join_strategy(&input);
        assert!(
            matches!(strategy, JoinStrategy::Broadcast { broadcast_table, .. } if broadcast_table == TableId(1)),
            "Expected Broadcast with table 1, got {strategy:?}"
        );
    }

    #[test]
    fn test_both_large_with_equi_key_uses_hash_partition() {
        let input = make_input(500_000, 1_000_000, true);
        let strategy = select_join_strategy(&input);
        assert!(
            matches!(strategy, JoinStrategy::HashPartition { .. }),
            "Expected HashPartition, got {strategy:?}"
        );
    }

    #[test]
    fn test_both_large_without_equi_key_falls_back_to_gather_all() {
        let input = make_input(500_000, 1_000_000, false);
        assert_eq!(select_join_strategy(&input), JoinStrategy::GatherAll);
    }

    #[test]
    fn test_symmetric_small_tables() {
        let input = make_input(GATHER_ALL_THRESHOLD, GATHER_ALL_THRESHOLD, true);
        assert_eq!(select_join_strategy(&input), JoinStrategy::GatherAll);
    }

    #[test]
    fn test_broadcast_selects_smaller_table() {
        // Right table is smaller
        let input = make_input(1_000_000, 5_000, true);
        let strategy = select_join_strategy(&input);
        assert!(
            matches!(strategy, JoinStrategy::Broadcast { broadcast_table, .. } if broadcast_table == TableId(2)),
            "Expected Broadcast with table 2, got {strategy:?}"
        );
    }

    #[test]
    fn test_broadcast_join_plan_cost() {
        let plan = BroadcastJoinPlan {
            broadcast_table: TableId(1),
            partitioned_table: TableId(2),
            target_shards: vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)],
            broadcast_rows: 1000,
        };
        // 1000 rows × 100 bytes × 4 shards = 400,000 bytes
        assert_eq!(plan.estimated_network_cost(100), 400_000);
    }

    #[test]
    fn test_hash_partition_plan_cost() {
        let plan = HashPartitionJoinPlan {
            left_table: TableId(1),
            right_table: TableId(2),
            left_key_col: 0,
            right_key_col: 0,
            num_partitions: 4,
            total_rows: 100_000,
        };
        // 100,000 rows × 100 bytes × 2 = 20,000,000 bytes
        assert_eq!(plan.estimated_network_cost(100), 20_000_000);
    }

    #[test]
    fn test_strategy_display() {
        assert_eq!(JoinStrategy::GatherAll.to_string(), "GatherAll");
        assert_eq!(
            JoinStrategy::Broadcast {
                broadcast_table: TableId(1),
                partitioned_table: TableId(2),
            }
            .to_string(),
            "Broadcast(small=TableId(1))"
        );
        assert_eq!(
            JoinStrategy::HashPartition {
                left_table: TableId(1),
                right_table: TableId(2),
                left_key_col: 0,
                right_key_col: 1,
            }
            .to_string(),
            "HashPartition(left_key=0, right_key=1)"
        );
    }
}
