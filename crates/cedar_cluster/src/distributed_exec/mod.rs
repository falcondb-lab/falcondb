//! Distributed execution engine: scatter/gather across shards.
//!
//! Executes subplans on each shard in parallel, collects partial results,
//! and merges them into a final result set. Features:
//! - **Union**: gather all rows, optional DISTINCT dedup, OFFSET, LIMIT
//! - **OrderByLimit**: global merge-sort with OFFSET + LIMIT
//! - **TwoPhaseAgg**: partial agg per shard + final merge (9 agg funcs)
//! - **Partial failure resilience**: returns partial results if some shards fail
//! - **Retry**: 1 retry with 5ms backoff on transient failure
//! - **Timeout**: per-scatter timeout check after each shard completes

pub mod gather;
pub mod scatter;

// Re-export all public types at the module level for backward compatibility.
pub use gather::{compare_datums, merge_two_phase_agg};
pub use scatter::DistributedExecutor;

use std::sync::Arc;

use cedar_common::datum::OwnedRow;
use cedar_common::error::CedarError;
use cedar_common::types::{DataType, ShardId};

/// Result type for subplan execution: (columns, rows).
pub type SubPlanResult = (Vec<(String, DataType)>, Vec<OwnedRow>);

/// Type alias for the subplan execution closure.
type SubPlanFn = Box<
    dyn Fn(
            &cedar_storage::engine::StorageEngine,
            &cedar_txn::manager::TxnManager,
        ) -> Result<SubPlanResult, CedarError>
        + Send
        + Sync,
>;

/// Result from executing a subplan on a single shard.
#[derive(Debug, Clone)]
pub struct ShardResult {
    pub shard_id: ShardId,
    pub columns: Vec<(String, DataType)>,
    pub rows: Vec<OwnedRow>,
    pub latency_us: u64,
}

fn compare_rows_by_columns(
    a: &OwnedRow,
    b: &OwnedRow,
    sort_columns: &[(usize, bool)],
) -> std::cmp::Ordering {
    for &(col_idx, ascending) in sort_columns {
        let va = a.values.get(col_idx);
        let vb = b.values.get(col_idx);
        let ord = compare_datums(va, vb);
        let ord = if ascending { ord } else { ord.reverse() };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Metrics collected during a scatter/gather execution.
#[derive(Debug, Clone, Default)]
pub struct ScatterGatherMetrics {
    pub shards_participated: usize,
    pub total_rows_gathered: usize,
    pub max_subplan_latency_us: u64,
    pub gather_merge_latency_us: u64,
    pub total_latency_us: u64,
    pub per_shard_latency_us: Vec<(u64, u64)>,
    pub per_shard_row_count: Vec<(u64, usize)>,
    pub failed_shards: Vec<u64>,
}

/// A subplan to execute on a shard. For MVP, this is a closure-based interface
/// that operates directly on the shard's (StorageEngine, TxnManager).
/// In production, this would be a serializable plan sent over RPC.
pub struct SubPlan {
    /// Human-readable description for observability.
    pub description: String,
    /// The function to execute on each shard.
    /// Takes (storage, txn_mgr) and returns column metadata + rows.
    execute_fn: SubPlanFn,
}

impl SubPlan {
    pub fn new<F>(description: &str, f: F) -> Self
    where
        F: Fn(
                &cedar_storage::engine::StorageEngine,
                &cedar_txn::manager::TxnManager,
            ) -> Result<SubPlanResult, CedarError>
            + Send
            + Sync
            + 'static,
    {
        Self {
            description: description.to_string(),
            execute_fn: Box::new(f),
        }
    }

    pub(crate) fn execute(
        &self,
        storage: &cedar_storage::engine::StorageEngine,
        txn_mgr: &cedar_txn::manager::TxnManager,
    ) -> Result<SubPlanResult, CedarError> {
        (self.execute_fn)(storage, txn_mgr)
    }
}

/// How to merge shard results in the gather phase.
#[derive(Clone)]
pub enum GatherStrategy {
    /// Simple union of all rows. Optional distinct dedup, offset, and limit.
    Union {
        distinct: bool,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    /// Sort by column indices, then apply optional offset + limit.
    OrderByLimit {
        sort_columns: Vec<(usize, bool)>, // (col_idx, ascending)
        limit: Option<usize>,
        offset: Option<usize>,
    },
    /// Two-phase aggregation: merge partial aggregates into final result.
    TwoPhaseAgg {
        /// Group-by column indices in the partial result.
        group_by_indices: Vec<usize>,
        /// Aggregate column indices and their merge functions.
        agg_merges: Vec<AggMerge>,
        /// AVG decomposition fixups: (sum_output_idx, count_hidden_idx).
        /// Post-merge: row[sum_idx] = row[sum_idx] / row[count_idx].
        avg_fixups: Vec<(usize, usize)>,
        /// Number of visible output columns (before hidden COUNT cols for AVG).
        visible_columns: Option<usize>,
        /// Post-merge filter predicate (HAVING). Takes a row, returns true to keep.
        #[allow(clippy::type_complexity)]
        having: Option<Arc<dyn Fn(&OwnedRow) -> bool + Send + Sync>>,
        /// Post-merge sort: (column_idx, ascending).
        order_by: Vec<(usize, bool)>,
        /// Post-merge limit.
        limit: Option<usize>,
        /// Post-merge offset.
        offset: Option<usize>,
    },
}

impl std::fmt::Debug for GatherStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GatherStrategy::Union { distinct, limit, offset } => {
                f.debug_struct("Union")
                    .field("distinct", distinct)
                    .field("limit", limit)
                    .field("offset", offset)
                    .finish()
            }
            GatherStrategy::OrderByLimit { sort_columns, limit, offset } => {
                f.debug_struct("OrderByLimit")
                    .field("sort_columns", sort_columns)
                    .field("limit", limit)
                    .field("offset", offset)
                    .finish()
            }
            GatherStrategy::TwoPhaseAgg { group_by_indices, agg_merges, avg_fixups, visible_columns, having, order_by, limit, offset } => {
                f.debug_struct("TwoPhaseAgg")
                    .field("group_by_indices", group_by_indices)
                    .field("agg_merges", agg_merges)
                    .field("avg_fixups", avg_fixups)
                    .field("visible_columns", visible_columns)
                    .field("having", &having.as_ref().map(|_| "<fn>"))
                    .field("order_by", order_by)
                    .field("limit", limit)
                    .field("offset", offset)
                    .finish()
            }
        }
    }
}

/// How to merge a single aggregate column across shards.
#[derive(Debug, Clone)]
pub enum AggMerge {
    /// SUM: add all partial sums.
    Sum(usize),
    /// COUNT: add all partial counts.
    Count(usize),
    /// MIN: take the minimum across shards.
    Min(usize),
    /// MAX: take the maximum across shards.
    Max(usize),
    /// BOOL_AND: logical AND across all shards.
    BoolAnd(usize),
    /// BOOL_OR: logical OR across all shards.
    BoolOr(usize),
    /// STRING_AGG: concatenate partial strings with separator.
    StringAgg(usize, String),
    /// ARRAY_AGG: concatenate partial arrays across shards.
    ArrayAgg(usize),
    /// COUNT(DISTINCT): each shard sends ARRAY_AGG(DISTINCT col),
    /// gather concatenates, deduplicates, produces Int64 count.
    CountDistinct(usize),
    /// SUM(DISTINCT): each shard sends ARRAY_AGG(DISTINCT col),
    /// gather concatenates, deduplicates, sums unique values.
    SumDistinct(usize),
    /// AVG(DISTINCT): each shard sends ARRAY_AGG(DISTINCT col),
    /// gather concatenates, deduplicates, computes sum/count as Float64.
    AvgDistinct(usize),
    /// STRING_AGG(DISTINCT): each shard sends ARRAY_AGG(DISTINCT col),
    /// gather concatenates, deduplicates, joins unique values with separator.
    StringAggDistinct(usize, String),
    /// ARRAY_AGG(DISTINCT): each shard sends ARRAY_AGG(DISTINCT col),
    /// gather concatenates, deduplicates, returns unique array.
    ArrayAggDistinct(usize),
}

/// Failure policy for scatter/gather.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailurePolicy {
    /// Any shard failure causes the entire query to fail. (Default, recommended for correctness.)
    Strict,
    /// Allow partial results from available shards; failed shards return warnings.
    BestEffort,
}

/// Resource limits for gather-phase buffering.
#[derive(Debug, Clone)]
pub struct GatherLimits {
    /// Maximum number of rows buffered in memory during gather.
    /// Exceeding this limit causes the query to fail with a clear error.
    pub max_rows_buffered: usize,
}

impl Default for GatherLimits {
    fn default() -> Self {
        Self {
            max_rows_buffered: 1_000_000, // 1M rows
        }
    }
}
