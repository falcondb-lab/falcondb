//! Intra-query parallel execution engine.
//!
//! Provides:
//!   - `ParallelConfig`: thread pool sizing and batch configuration
//!   - `parallel_scan`: partition a scan across worker threads
//!   - `parallel_filter`: apply a filter in parallel across row partitions
//!   - `parallel_aggregate`: partitioned hash aggregation with merge
//!
//! The parallelism model is partition-parallel: input rows are split into
//! equal-sized chunks and each chunk is processed independently by a worker
//! thread.  Results are merged on the coordinator thread.

use std::collections::HashMap;
use std::sync::Arc;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_sql_frontend::types::*;

use crate::expr_engine::ExprEngine;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Parallel execution configuration.
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Maximum number of worker threads for intra-query parallelism.
    /// 0 = disabled (single-threaded), default = num_cpus.
    pub max_threads: usize,
    /// Minimum number of rows to justify parallel execution.
    pub min_rows_for_parallel: usize,
    /// Batch size for vectorized processing within each partition.
    pub batch_size: usize,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            max_threads: cpus,
            min_rows_for_parallel: 10_000,
            batch_size: 1024,
        }
    }
}

impl ParallelConfig {
    /// Create a config with parallelism disabled.
    pub fn single_threaded() -> Self {
        Self {
            max_threads: 0,
            min_rows_for_parallel: usize::MAX,
            batch_size: 1024,
        }
    }

    /// Should we use parallel execution for this many rows?
    pub fn should_parallelize(&self, num_rows: usize) -> bool {
        self.max_threads > 1 && num_rows >= self.min_rows_for_parallel
    }

    /// Effective number of worker threads (capped by data size).
    pub fn effective_threads(&self, num_rows: usize) -> usize {
        if !self.should_parallelize(num_rows) {
            return 1;
        }
        let rows_per_thread = self.min_rows_for_parallel;
        let needed = num_rows.div_ceil(rows_per_thread);
        needed.min(self.max_threads)
    }
}

// ---------------------------------------------------------------------------
// Parallel filter
// ---------------------------------------------------------------------------

/// Apply a filter to rows in parallel.  Partitions the input across threads,
/// each thread evaluates the filter on its partition, and results are merged
/// preserving the original order.
pub fn parallel_filter(
    rows: &[(Vec<u8>, OwnedRow)],
    filter: &BoundExpr,
    config: &ParallelConfig,
) -> Vec<usize> {
    let n = rows.len();
    let num_threads = config.effective_threads(n);

    if num_threads <= 1 {
        // Single-threaded fallback
        return (0..n)
            .filter(|&i| ExprEngine::eval_filter(filter, &rows[i].1).unwrap_or(false))
            .collect();
    }

    let chunk_size = n.div_ceil(num_threads);
    let filter_arc = Arc::new(filter.clone());
    let rows_arc = Arc::new(rows.to_vec());

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let f = filter_arc.clone();
            let r = rows_arc.clone();
            let start = t * chunk_size;
            let end = (start + chunk_size).min(n);
            std::thread::spawn(move || {
                let mut matched = Vec::new();
                for i in start..end {
                    if ExprEngine::eval_filter(&f, &r[i].1).unwrap_or(false) {
                        matched.push(i);
                    }
                }
                matched
            })
        })
        .collect();

    let mut result = Vec::new();
    for h in handles {
        if let Ok(indices) = h.join() {
            result.extend(indices);
        }
    }
    result.sort_unstable();
    result
}

// ---------------------------------------------------------------------------
// Parallel aggregate
// ---------------------------------------------------------------------------

/// Group key: a vector of string representations of group-by columns.
type GroupKey = Vec<String>;

/// Partial aggregate state for a single group.
#[derive(Debug, Clone)]
pub struct PartialAggState {
    /// Per-aggregate accumulated values (count, sum, etc.)
    pub count: i64,
    pub sum: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    /// Sum of squared deviations from the running mean (Welford's algorithm)
    pub m2: f64,
    pub mean: f64,
    /// First row of the group (for non-aggregate column access)
    pub first_row: Option<OwnedRow>,
}

impl Default for PartialAggState {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
            m2: 0.0,
            mean: 0.0,
            first_row: None,
        }
    }
}

impl PartialAggState {
    /// Update with a new value using Welford's online algorithm.
    pub fn update(&mut self, val: Option<f64>, row: &OwnedRow) {
        if self.first_row.is_none() {
            self.first_row = Some(row.clone());
        }
        if let Some(v) = val {
            self.count += 1;
            self.sum += v;
            if self.min.is_none_or(|m| v < m) {
                self.min = Some(v);
            }
            if self.max.is_none_or(|m| v > m) {
                self.max = Some(v);
            }
            // Welford's online algorithm for variance
            let delta = v - self.mean;
            self.mean += delta / self.count as f64;
            let delta2 = v - self.mean;
            self.m2 += delta * delta2;
        }
    }

    /// Merge another partial state into this one.
    pub fn merge(&mut self, other: &PartialAggState) {
        if other.count == 0 {
            return;
        }
        if self.first_row.is_none() {
            self.first_row = other.first_row.clone();
        }
        let combined_count = self.count + other.count;
        let combined_sum = self.sum + other.sum;
        // Parallel Welford merge
        if combined_count > 0 {
            let delta = other.mean - self.mean;
            let combined_mean = combined_sum / combined_count as f64;
            let combined_m2 = self.m2
                + other.m2
                + delta * delta * (self.count as f64 * other.count as f64 / combined_count as f64);
            self.m2 = combined_m2;
            self.mean = combined_mean;
        }
        self.count = combined_count;
        self.sum = combined_sum;
        if let Some(om) = other.min {
            self.min = Some(self.min.map_or(om, |m: f64| m.min(om)));
        }
        if let Some(om) = other.max {
            self.max = Some(self.max.map_or(om, |m: f64| m.max(om)));
        }
    }

    /// Finalize an aggregate function from this state.
    pub fn finalize(&self, func: &AggFunc) -> Datum {
        match func {
            AggFunc::Count => Datum::Int64(self.count),
            AggFunc::Sum => {
                if self.count == 0 {
                    Datum::Null
                } else {
                    Datum::Float64(self.sum)
                }
            }
            AggFunc::Avg => {
                if self.count == 0 {
                    Datum::Null
                } else {
                    Datum::Float64(self.sum / self.count as f64)
                }
            }
            AggFunc::Min => self.min.map(Datum::Float64).unwrap_or(Datum::Null),
            AggFunc::Max => self.max.map(Datum::Float64).unwrap_or(Datum::Null),
            AggFunc::VarPop => {
                if self.count == 0 {
                    Datum::Null
                } else {
                    Datum::Float64(self.m2 / self.count as f64)
                }
            }
            AggFunc::VarSamp => {
                if self.count < 2 {
                    Datum::Null
                } else {
                    Datum::Float64(self.m2 / (self.count - 1) as f64)
                }
            }
            AggFunc::StddevPop => {
                if self.count == 0 {
                    Datum::Null
                } else {
                    Datum::Float64((self.m2 / self.count as f64).sqrt())
                }
            }
            AggFunc::StddevSamp => {
                if self.count < 2 {
                    Datum::Null
                } else {
                    Datum::Float64((self.m2 / (self.count - 1) as f64).sqrt())
                }
            }
            _ => Datum::Null, // unsupported aggregate
        }
    }
}

/// Execute a grouped aggregate in parallel.
///
/// 1. Partition input rows across threads.
/// 2. Each thread builds a local HashMap<GroupKey, PartialAggState>.
/// 3. Merge all partial hash maps on the coordinator.
/// 4. Finalize aggregate values.
pub fn parallel_grouped_aggregate(
    rows: &[(Vec<u8>, OwnedRow)],
    group_by: &[usize],
    agg_col_idx: usize,
    agg_func: &AggFunc,
    config: &ParallelConfig,
) -> HashMap<GroupKey, PartialAggState> {
    let n = rows.len();
    let num_threads = config.effective_threads(n);

    if num_threads <= 1 {
        return single_thread_aggregate(rows, group_by, agg_col_idx, agg_func);
    }

    let chunk_size = n.div_ceil(num_threads);
    let rows_arc = Arc::new(rows.to_vec());
    let group_by_arc = Arc::new(group_by.to_vec());

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let r = rows_arc.clone();
            let gb = group_by_arc.clone();
            let start = t * chunk_size;
            let end = (start + chunk_size).min(n);
            std::thread::spawn(move || {
                let mut local_map: HashMap<GroupKey, PartialAggState> = HashMap::new();
                for i in start..end {
                    let row = &r[i].1;
                    let key: GroupKey = gb
                        .iter()
                        .map(|&idx| {
                            row.values
                                .get(idx)
                                .map(|d| format!("{}", d))
                                .unwrap_or_default()
                        })
                        .collect();
                    let val = row.values.get(agg_col_idx).and_then(|d| d.as_f64());
                    let state = local_map.entry(key).or_default();
                    state.update(val, row);
                }
                local_map
            })
        })
        .collect();

    // Merge phase
    let mut global_map: HashMap<GroupKey, PartialAggState> = HashMap::new();
    for h in handles {
        if let Ok(local_map) = h.join() {
            for (key, partial) in local_map {
                global_map.entry(key).or_default().merge(&partial);
            }
        }
    }
    global_map
}

fn single_thread_aggregate(
    rows: &[(Vec<u8>, OwnedRow)],
    group_by: &[usize],
    agg_col_idx: usize,
    _agg_func: &AggFunc,
) -> HashMap<GroupKey, PartialAggState> {
    let mut map: HashMap<GroupKey, PartialAggState> = HashMap::new();
    for (_pk, row) in rows {
        let key: GroupKey = group_by
            .iter()
            .map(|&idx| {
                row.values
                    .get(idx)
                    .map(|d| format!("{}", d))
                    .unwrap_or_default()
            })
            .collect();
        let val = row.values.get(agg_col_idx).and_then(|d| d.as_f64());
        map.entry(key).or_default().update(val, row);
    }
    map
}

// ---------------------------------------------------------------------------
// Parallel scan helper
// ---------------------------------------------------------------------------

/// Split rows into partitions for parallel processing.
pub fn partition_rows<T: Clone>(rows: &[T], num_partitions: usize) -> Vec<Vec<T>> {
    if num_partitions <= 1 {
        return vec![rows.to_vec()];
    }
    let chunk_size = rows.len().div_ceil(num_partitions);
    rows.chunks(chunk_size).map(|c| c.to_vec()).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_config_defaults() {
        let cfg = ParallelConfig::default();
        assert!(cfg.max_threads >= 1);
        assert!(cfg.min_rows_for_parallel > 0);
    }

    #[test]
    fn test_parallel_config_single_threaded() {
        let cfg = ParallelConfig::single_threaded();
        assert_eq!(cfg.max_threads, 0);
        assert!(!cfg.should_parallelize(1_000_000));
    }

    #[test]
    fn test_partial_agg_state_welford() {
        let mut state = PartialAggState::default();
        let dummy = OwnedRow::new(vec![]);
        for v in [10.0, 20.0, 30.0] {
            state.update(Some(v), &dummy);
        }
        assert_eq!(state.count, 3);
        assert!((state.sum - 60.0).abs() < 0.01);
        assert!((state.mean - 20.0).abs() < 0.01);
        // VAR_POP = m2 / n = 200/3 ≈ 66.67
        let var_pop = state.m2 / state.count as f64;
        assert!((var_pop - 66.67).abs() < 0.1);
    }

    #[test]
    fn test_partial_agg_merge() {
        let dummy = OwnedRow::new(vec![]);
        let mut s1 = PartialAggState::default();
        for v in [10.0, 20.0] {
            s1.update(Some(v), &dummy);
        }

        let mut s2 = PartialAggState::default();
        for v in [30.0] {
            s2.update(Some(v), &dummy);
        }

        s1.merge(&s2);
        assert_eq!(s1.count, 3);
        assert!((s1.sum - 60.0).abs() < 0.01);
        // After merge, variance should still be correct
        let var_pop = s1.m2 / s1.count as f64;
        assert!((var_pop - 66.67).abs() < 0.1, "var_pop={}", var_pop);
    }

    #[test]
    fn test_partition_rows() {
        let rows: Vec<i32> = (0..10).collect();
        let parts = partition_rows(&rows, 3);
        assert_eq!(parts.len(), 3);
        let total: usize = parts.iter().map(|p| p.len()).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_parallel_filter_small() {
        let rows: Vec<(Vec<u8>, OwnedRow)> = (0..5)
            .map(|i| (vec![], OwnedRow::new(vec![Datum::Int64(i)])))
            .collect();
        let filter = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Gt,
            right: Box::new(BoundExpr::Literal(Datum::Int64(2))),
        };
        let cfg = ParallelConfig::single_threaded();
        let result = parallel_filter(&rows, &filter, &cfg);
        assert_eq!(result, vec![3, 4]);
    }
}
