//! Scatter phase: execute subplans on shards in parallel with retry and timeout.
//!
//! Provides three entry points:
//! - `scatter_gather`: closure-based subplan (legacy, in-process only)
//! - `scatter_gather_serializable`: same `SerializableSubPlan` to all shards via backend
//! - `scatter_gather_per_shard`: different `SerializableSubPlan` per shard via backend

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use falcon_common::datum::Datum;
use falcon_common::error::FalconError;
use falcon_common::types::{DataType, ShardId};

use falcon_common::datum::OwnedRow;

use crate::sharded_engine::ShardedEngine;

use super::backend::ShardExecBackend;
use super::gather::{compare_datums, merge_two_phase_agg};
use super::serializable_plan::SerializableSubPlan;
use super::streaming_merge::StreamingMergeSort;
use super::{
    compare_rows_by_columns, FailurePolicy, GatherLimits, GatherStrategy, ScatterGatherMetrics,
    ShardResult, SubPlan, SubPlanResult,
};

/// The distributed executor.
pub struct DistributedExecutor {
    engine: Arc<ShardedEngine>,
    timeout: Duration,
    failure_policy: FailurePolicy,
    gather_limits: GatherLimits,
}

impl DistributedExecutor {
    pub fn new(engine: Arc<ShardedEngine>, timeout: Duration) -> Self {
        Self {
            engine,
            timeout,
            failure_policy: FailurePolicy::Strict,
            gather_limits: GatherLimits::default(),
        }
    }

    /// Set the failure policy (Strict or BestEffort).
    pub const fn with_failure_policy(mut self, policy: FailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    /// Set gather-phase resource limits.
    pub const fn with_gather_limits(mut self, limits: GatherLimits) -> Self {
        self.gather_limits = limits;
        self
    }

    /// Execute a subplan on all shards (scatter), then merge results (gather).
    ///
    /// Scatter phase runs subplans **in parallel** using `std::thread::scope`.
    /// Each shard executes independently; results are collected after all finish.
    pub fn scatter_gather(
        &self,
        subplan: &SubPlan,
        target_shards: &[ShardId],
        strategy: &GatherStrategy,
    ) -> Result<(SubPlanResult, ScatterGatherMetrics), FalconError> {
        let total_start = Instant::now();
        let timeout = self.timeout;

        // ── Scatter: execute subplan on each target shard IN PARALLEL ──
        // Validate shard IDs first (before spawning threads).
        for &sid in target_shards {
            if self.engine.shard(sid).is_none() {
                return Err(FalconError::internal_bug(
                    "E-SCATTER-001",
                    format!("Shard {sid:?} not found during pre-validation"),
                    format!("target_shards={target_shards:?}"),
                ));
            }
        }

        let shard_results: Vec<Result<ShardResult, FalconError>> = std::thread::scope(|s| {
            // collect() is required: ScopedJoinHandle borrows `s` and must be stored before joining.
            #[allow(clippy::needless_collect)]
            let handles: Vec<_> = target_shards
                .iter()
                .map(|&shard_id| {
                    let engine = &self.engine;
                    let subplan_ref = &subplan;
                    s.spawn(move || {
                        let shard = engine.shard(shard_id).ok_or_else(|| {
                            FalconError::internal_bug(
                                "E-SCATTER-002",
                                format!("Shard {shard_id:?} disappeared during execution"),
                                "shard was validated before spawn but missing at execution time",
                            )
                        })?;

                        // Retry once on transient failure.
                        let max_attempts = 2u8;
                        let mut last_err = None;
                        for attempt in 0..max_attempts {
                            if attempt > 0 {
                                // Interruptible retry delay (condvar instead of bare sleep)
                                {
                                    let pair = std::sync::Mutex::new(false);
                                    let cvar = std::sync::Condvar::new();
                                    let guard = pair
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                                    let _ = cvar.wait_timeout(guard, Duration::from_millis(5));
                                }
                            }

                            let start = Instant::now();
                            let result = subplan_ref.execute(&shard.storage, &shard.txn_mgr);
                            let latency_us = start.elapsed().as_micros() as u64;

                            // Check timeout
                            if total_start.elapsed() > timeout {
                                return Err(FalconError::Transient {
                                    reason: format!(
                                        "Scatter/gather timeout after {}ms (shard {:?})",
                                        timeout.as_millis(),
                                        shard_id,
                                    ),
                                    retry_after_ms: 100,
                                });
                            }

                            match result {
                                Ok((columns, rows)) => {
                                    return Ok(ShardResult {
                                        shard_id,
                                        columns,
                                        rows,
                                        latency_us,
                                    });
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Shard {:?} attempt {}/{} failed: {}",
                                        shard_id,
                                        attempt + 1,
                                        max_attempts,
                                        e,
                                    );
                                    last_err = Some(e);
                                }
                            }
                        }

                        Err(last_err.unwrap_or_else(|| {
                            FalconError::Internal(format!(
                                "Shard {shard_id:?} failed after {max_attempts} attempts"
                            ))
                        }))
                    })
                })
                .collect();

            handles
                .into_iter()
                .map(|h| {
                    h.join().unwrap_or_else(|_| {
                        Err(FalconError::internal_bug(
                        "E-SCATTER-003",
                        "Shard thread panicked during scatter execution",
                        "std::thread::JoinHandle returned Err — indicates panic in spawned thread",
                    ))
                    })
                })
                .collect()
        });

        // Collect results — apply failure policy.
        let mut collected: Vec<ShardResult> = Vec::with_capacity(target_shards.len());
        let mut failed_shards: Vec<(ShardId, String)> = Vec::new();
        let mut max_subplan_latency_us = 0u64;
        for (i, r) in shard_results.into_iter().enumerate() {
            match r {
                Ok(sr) => {
                    if sr.latency_us > max_subplan_latency_us {
                        max_subplan_latency_us = sr.latency_us;
                    }
                    collected.push(sr);
                }
                Err(e) => {
                    let sid = target_shards.get(i).copied().unwrap_or(ShardId(i as u64));
                    tracing::warn!("Shard {:?} failed during scatter: {}", sid, e);
                    failed_shards.push((sid, format!("{e}")));
                }
            }
        }

        // Strict policy: any shard failure → fail the whole query.
        if self.failure_policy == FailurePolicy::Strict && !failed_shards.is_empty() {
            let msgs: Vec<String> = failed_shards
                .iter()
                .map(|(sid, msg)| format!("shard_{}: {}", sid.0, msg))
                .collect();
            return Err(FalconError::Transient {
                reason: format!(
                    "Scatter failed ({} of {} shards): {}",
                    failed_shards.len(),
                    target_shards.len(),
                    msgs.join("; "),
                ),
                retry_after_ms: 100,
            });
        }

        // BestEffort: at least one shard must succeed.
        if collected.is_empty() {
            let msgs: Vec<String> = failed_shards
                .iter()
                .map(|(sid, msg)| format!("shard_{}: {}", sid.0, msg))
                .collect();
            return Err(FalconError::Transient {
                reason: format!(
                    "All {} shards failed: {}",
                    target_shards.len(),
                    msgs.join("; "),
                ),
                retry_after_ms: 200,
            });
        }
        let shard_results = collected;

        // ── Gather: merge results ──
        let gather_start = Instant::now();

        let columns = if let Some(first) = shard_results.first() {
            first.columns.clone()
        } else {
            vec![]
        };

        let total_rows_gathered: usize = shard_results.iter().map(|r| r.rows.len()).sum();

        // Memory guard: reject if total rows exceed configured limit.
        if total_rows_gathered > self.gather_limits.max_rows_buffered {
            return Err(FalconError::Transient {
                reason: format!(
                    "Gather phase aborted: {} rows exceeds max_rows_buffered limit of {}",
                    total_rows_gathered, self.gather_limits.max_rows_buffered,
                ),
                retry_after_ms: 0,
            });
        }

        let merged_rows = match strategy {
            GatherStrategy::Union {
                distinct,
                limit,
                offset,
            } => {
                let mut all_rows = Vec::with_capacity(total_rows_gathered);
                for sr in &shard_results {
                    all_rows.extend(sr.rows.iter().cloned());
                }
                if *distinct {
                    // O(N) distinct dedup while preserving first-seen order.
                    let mut seen: HashSet<Vec<Datum>> = HashSet::with_capacity(all_rows.len());
                    all_rows.retain(|row| seen.insert(row.values.clone()));
                }
                if let Some(off) = offset {
                    if *off < all_rows.len() {
                        all_rows = all_rows.split_off(*off);
                    } else {
                        all_rows.clear();
                    }
                }
                if let Some(lim) = limit {
                    all_rows.truncate(*lim);
                }
                all_rows
            }
            GatherStrategy::OrderByLimit {
                sort_columns,
                limit,
                offset,
            } => {
                // Fast path: if each shard output is already sorted by `sort_columns`,
                // do k-way merge (O(N log k)) with early stop at OFFSET+LIMIT.
                let is_pre_sorted = shard_results.iter().all(|sr| {
                    sr.rows.windows(2).all(|w| {
                        compare_rows_by_columns(&w[0], &w[1], sort_columns)
                            != std::cmp::Ordering::Greater
                    })
                });

                if is_pre_sorted {
                    // Use streaming k-way merge: O(N log K) with early
                    // termination at OFFSET + LIMIT.
                    let merger = StreamingMergeSort::from_shard_results(
                        &shard_results,
                        sort_columns.clone(),
                    );
                    merger.collect_with_offset_limit(*offset, *limit)
                } else {
                    // Fallback: generic path for unsorted per-shard outputs.
                    let mut all_rows = Vec::with_capacity(total_rows_gathered);
                    for sr in &shard_results {
                        all_rows.extend(sr.rows.iter().cloned());
                    }
                    all_rows.sort_by(|a, b| compare_rows_by_columns(a, b, sort_columns));
                    if let Some(off) = offset {
                        if *off < all_rows.len() {
                            all_rows = all_rows.split_off(*off);
                        } else {
                            all_rows.clear();
                        }
                    }
                    if let Some(lim) = limit {
                        all_rows.truncate(*lim);
                    }
                    all_rows
                }
            }
            GatherStrategy::TwoPhaseAgg {
                group_by_indices,
                agg_merges,
                avg_fixups,
                visible_columns,
                having,
                order_by,
                limit,
                offset,
            } => {
                let mut merged = merge_two_phase_agg(&shard_results, group_by_indices, agg_merges);
                // Apply AVG fixups: row[sum_idx] = row[sum_idx] / row[count_idx]
                for row in &mut merged {
                    for &(sum_idx, count_idx) in avg_fixups {
                        let sum_val = row.values.get(sum_idx).cloned().unwrap_or(Datum::Null);
                        let count_val = row.values.get(count_idx).cloned().unwrap_or(Datum::Null);
                        let avg = match (&sum_val, &count_val) {
                            (_, Datum::Int64(0)) | (_, Datum::Null) => Datum::Null,
                            (Datum::Int64(s), Datum::Int64(c)) => {
                                Datum::Float64(*s as f64 / *c as f64)
                            }
                            (Datum::Int32(s), Datum::Int64(c)) => {
                                Datum::Float64(f64::from(*s) / *c as f64)
                            }
                            (Datum::Float64(s), Datum::Int64(c)) => Datum::Float64(s / *c as f64),
                            _ => Datum::Null,
                        };
                        if let Some(v) = row.values.get_mut(sum_idx) {
                            *v = avg;
                        }
                    }
                    // Truncate hidden COUNT columns
                    if let Some(vc) = visible_columns {
                        row.values.truncate(*vc);
                    }
                }
                // Apply HAVING filter post-merge
                if let Some(pred) = having {
                    merged.retain(|row| pred(row));
                }
                // Apply post-merge ORDER BY
                if !order_by.is_empty() {
                    merged.sort_by(|a, b| {
                        for &(col_idx, ascending) in order_by {
                            let va = a.values.get(col_idx);
                            let vb = b.values.get(col_idx);
                            let ord = compare_datums(va, vb);
                            let ord = if ascending { ord } else { ord.reverse() };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                // Apply post-merge OFFSET
                if let Some(off) = offset {
                    if *off < merged.len() {
                        merged = merged.split_off(*off);
                    } else {
                        merged.clear();
                    }
                }
                // Apply post-merge LIMIT
                if let Some(lim) = limit {
                    merged.truncate(*lim);
                }
                merged
            }
        };

        let gather_merge_latency_us = gather_start.elapsed().as_micros() as u64;
        let total_latency_us = total_start.elapsed().as_micros() as u64;

        let per_shard_latency: Vec<(u64, u64)> = shard_results
            .iter()
            .map(|sr| (sr.shard_id.0, sr.latency_us))
            .collect();
        let per_shard_row_count: Vec<(u64, usize)> = shard_results
            .iter()
            .map(|sr| (sr.shard_id.0, sr.rows.len()))
            .collect();
        let failed_shard_ids: Vec<u64> = failed_shards.iter().map(|(sid, _)| sid.0).collect();

        let metrics = ScatterGatherMetrics {
            shards_participated: shard_results.len(),
            total_rows_gathered,
            max_subplan_latency_us,
            gather_merge_latency_us,
            total_latency_us,
            per_shard_latency_us: per_shard_latency,
            per_shard_row_count,
            failed_shards: failed_shard_ids,
        };

        Ok(((columns, merged_rows), metrics))
    }

    /// Execute a `SerializableSubPlan` on all target shards via backend, then merge.
    ///
    /// This is the **preferred** entry point for distributed execution:
    /// - With `LocalShardBackend`: in-process (same as `scatter_gather` but plan-driven)
    /// - With `GrpcShardBackend`: transparent gRPC dispatch to remote shard leaders
    ///
    /// The same plan is sent to every target shard. For per-shard plans, use
    /// `scatter_gather_per_shard`.
    pub fn scatter_gather_serializable(
        &self,
        plan: &SerializableSubPlan,
        target_shards: &[ShardId],
        strategy: &GatherStrategy,
        backend: &dyn ShardExecBackend,
    ) -> Result<(SubPlanResult, ScatterGatherMetrics), FalconError> {
        let plans: Vec<(ShardId, &SerializableSubPlan)> =
            target_shards.iter().map(|&sid| (sid, plan)).collect();
        self.scatter_gather_per_shard_inner(&plans, strategy, backend)
    }

    /// Execute a **different** `SerializableSubPlan` on each target shard via backend.
    ///
    /// Used when each shard receives a distinct plan (e.g. hash-partition join
    /// where each shard gets its own partition of the data).
    pub fn scatter_gather_per_shard(
        &self,
        plans: &[(ShardId, SerializableSubPlan)],
        strategy: &GatherStrategy,
        backend: &dyn ShardExecBackend,
    ) -> Result<(SubPlanResult, ScatterGatherMetrics), FalconError> {
        let refs: Vec<(ShardId, &SerializableSubPlan)> =
            plans.iter().map(|(sid, p)| (*sid, p)).collect();
        self.scatter_gather_per_shard_inner(&refs, strategy, backend)
    }

    /// Internal: scatter via backend + gather merge.
    fn scatter_gather_per_shard_inner(
        &self,
        plans: &[(ShardId, &SerializableSubPlan)],
        strategy: &GatherStrategy,
        backend: &dyn ShardExecBackend,
    ) -> Result<(SubPlanResult, ScatterGatherMetrics), FalconError> {
        let total_start = Instant::now();
        let timeout = self.timeout;

        // ── Scatter: execute plans on each shard IN PARALLEL via backend ──
        let shard_results: Vec<Result<ShardResult, FalconError>> = std::thread::scope(|s| {
            #[allow(clippy::needless_collect)]
            let handles: Vec<_> = plans
                .iter()
                .map(|&(shard_id, plan_ref)| {
                    s.spawn(move || {
                        // Retry once on transient failure.
                        let max_attempts = 2u8;
                        let mut last_err = None;
                        for attempt in 0..max_attempts {
                            if attempt > 0 {
                                let pair = std::sync::Mutex::new(false);
                                let cvar = std::sync::Condvar::new();
                                let guard = pair
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                let _ = cvar.wait_timeout(guard, Duration::from_millis(5));
                            }

                            // Check timeout
                            if total_start.elapsed() > timeout {
                                return Err(FalconError::Transient {
                                    reason: format!(
                                        "Scatter/gather timeout after {}ms (shard {:?})",
                                        timeout.as_millis(),
                                        shard_id,
                                    ),
                                    retry_after_ms: 100,
                                });
                            }

                            match backend.execute_on_shard(shard_id, plan_ref) {
                                Ok(sr) => return Ok(sr),
                                Err(e) => {
                                    tracing::warn!(
                                        "Shard {:?} attempt {}/{} failed: {}",
                                        shard_id,
                                        attempt + 1,
                                        max_attempts,
                                        e,
                                    );
                                    last_err = Some(e);
                                }
                            }
                        }

                        Err(last_err.unwrap_or_else(|| {
                            FalconError::Internal(format!(
                                "Shard {shard_id:?} failed after {max_attempts} attempts"
                            ))
                        }))
                    })
                })
                .collect();

            handles
                .into_iter()
                .map(|h| {
                    h.join().unwrap_or_else(|_| {
                        Err(FalconError::internal_bug(
                            "E-SCATTER-004",
                            "Shard thread panicked during backend scatter execution",
                            "std::thread::JoinHandle returned Err",
                        ))
                    })
                })
                .collect()
        });

        // Collect results — apply failure policy.
        let target_shard_ids: Vec<ShardId> = plans.iter().map(|(sid, _)| *sid).collect();
        let mut collected: Vec<ShardResult> = Vec::with_capacity(plans.len());
        let mut failed_shards: Vec<(ShardId, String)> = Vec::new();
        let mut max_subplan_latency_us = 0u64;
        for (i, r) in shard_results.into_iter().enumerate() {
            match r {
                Ok(sr) => {
                    if sr.latency_us > max_subplan_latency_us {
                        max_subplan_latency_us = sr.latency_us;
                    }
                    collected.push(sr);
                }
                Err(e) => {
                    let sid = target_shard_ids
                        .get(i)
                        .copied()
                        .unwrap_or(ShardId(i as u64));
                    tracing::warn!("Shard {:?} failed during scatter: {}", sid, e);
                    failed_shards.push((sid, format!("{e}")));
                }
            }
        }

        if self.failure_policy == FailurePolicy::Strict && !failed_shards.is_empty() {
            let msgs: Vec<String> = failed_shards
                .iter()
                .map(|(sid, msg)| format!("shard_{}: {}", sid.0, msg))
                .collect();
            return Err(FalconError::Transient {
                reason: format!(
                    "Scatter failed ({} of {} shards): {}",
                    failed_shards.len(),
                    plans.len(),
                    msgs.join("; "),
                ),
                retry_after_ms: 100,
            });
        }

        if collected.is_empty() {
            let msgs: Vec<String> = failed_shards
                .iter()
                .map(|(sid, msg)| format!("shard_{}: {}", sid.0, msg))
                .collect();
            return Err(FalconError::Transient {
                reason: format!("All {} shards failed: {}", plans.len(), msgs.join("; "),),
                retry_after_ms: 200,
            });
        }

        // ── Gather: merge results ──
        let go = gather_merge(&collected, strategy, &self.gather_limits)?;

        let total_latency_us = total_start.elapsed().as_micros() as u64;
        let per_shard_latency: Vec<(u64, u64)> = collected
            .iter()
            .map(|sr| (sr.shard_id.0, sr.latency_us))
            .collect();
        let per_shard_row_count: Vec<(u64, usize)> = collected
            .iter()
            .map(|sr| (sr.shard_id.0, sr.rows.len()))
            .collect();
        let failed_shard_ids: Vec<u64> = failed_shards.iter().map(|(sid, _)| sid.0).collect();

        let metrics = ScatterGatherMetrics {
            shards_participated: collected.len(),
            total_rows_gathered: go.total_rows_gathered,
            max_subplan_latency_us,
            gather_merge_latency_us: go.gather_merge_latency_us,
            total_latency_us,
            per_shard_latency_us: per_shard_latency,
            per_shard_row_count,
            failed_shards: failed_shard_ids,
        };

        Ok(((go.columns, go.merged_rows), metrics))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Gather Phase (shared by all scatter entry points)
// ═══════════════════════════════════════════════════════════════════════════

/// Output of the gather merge phase.
struct GatherOutput {
    columns: Vec<(String, DataType)>,
    merged_rows: Vec<OwnedRow>,
    total_rows_gathered: usize,
    gather_merge_latency_us: u64,
}

/// Merge shard results according to the gather strategy.
///
/// This is a pure function extracted from `DistributedExecutor::scatter_gather`
/// so that all scatter entry points (closure-based, serializable, per-shard)
/// share the same gather logic.
fn gather_merge(
    shard_results: &[ShardResult],
    strategy: &GatherStrategy,
    gather_limits: &GatherLimits,
) -> Result<GatherOutput, FalconError> {
    let gather_start = Instant::now();

    let columns = if let Some(first) = shard_results.first() {
        first.columns.clone()
    } else {
        vec![]
    };

    let total_rows_gathered: usize = shard_results.iter().map(|r| r.rows.len()).sum();

    if total_rows_gathered > gather_limits.max_rows_buffered {
        return Err(FalconError::Transient {
            reason: format!(
                "Gather phase aborted: {} rows exceeds max_rows_buffered limit of {}",
                total_rows_gathered, gather_limits.max_rows_buffered,
            ),
            retry_after_ms: 0,
        });
    }

    let merged_rows = match strategy {
        GatherStrategy::Union {
            distinct,
            limit,
            offset,
        } => {
            let mut all_rows = Vec::with_capacity(total_rows_gathered);
            for sr in shard_results {
                all_rows.extend(sr.rows.iter().cloned());
            }
            if *distinct {
                let mut seen: HashSet<Vec<Datum>> = HashSet::with_capacity(all_rows.len());
                all_rows.retain(|row| seen.insert(row.values.clone()));
            }
            if let Some(off) = offset {
                if *off < all_rows.len() {
                    all_rows = all_rows.split_off(*off);
                } else {
                    all_rows.clear();
                }
            }
            if let Some(lim) = limit {
                all_rows.truncate(*lim);
            }
            all_rows
        }
        GatherStrategy::OrderByLimit {
            sort_columns,
            limit,
            offset,
        } => {
            let is_pre_sorted = shard_results.iter().all(|sr| {
                sr.rows.windows(2).all(|w| {
                    compare_rows_by_columns(&w[0], &w[1], sort_columns)
                        != std::cmp::Ordering::Greater
                })
            });

            if is_pre_sorted {
                let merger =
                    StreamingMergeSort::from_shard_results(shard_results, sort_columns.clone());
                merger.collect_with_offset_limit(*offset, *limit)
            } else {
                let mut all_rows = Vec::with_capacity(total_rows_gathered);
                for sr in shard_results {
                    all_rows.extend(sr.rows.iter().cloned());
                }
                all_rows.sort_by(|a, b| compare_rows_by_columns(a, b, sort_columns));
                if let Some(off) = offset {
                    if *off < all_rows.len() {
                        all_rows = all_rows.split_off(*off);
                    } else {
                        all_rows.clear();
                    }
                }
                if let Some(lim) = limit {
                    all_rows.truncate(*lim);
                }
                all_rows
            }
        }
        GatherStrategy::TwoPhaseAgg {
            group_by_indices,
            agg_merges,
            avg_fixups,
            visible_columns,
            having,
            order_by,
            limit,
            offset,
        } => {
            let mut merged = merge_two_phase_agg(shard_results, group_by_indices, agg_merges);
            for row in &mut merged {
                for &(sum_idx, count_idx) in avg_fixups {
                    let sum_val = row.values.get(sum_idx).cloned().unwrap_or(Datum::Null);
                    let count_val = row.values.get(count_idx).cloned().unwrap_or(Datum::Null);
                    let avg = match (&sum_val, &count_val) {
                        (_, Datum::Int64(0)) | (_, Datum::Null) => Datum::Null,
                        (Datum::Int64(s), Datum::Int64(c)) => Datum::Float64(*s as f64 / *c as f64),
                        (Datum::Int32(s), Datum::Int64(c)) => {
                            Datum::Float64(f64::from(*s) / *c as f64)
                        }
                        (Datum::Float64(s), Datum::Int64(c)) => Datum::Float64(s / *c as f64),
                        _ => Datum::Null,
                    };
                    if let Some(v) = row.values.get_mut(sum_idx) {
                        *v = avg;
                    }
                }
                if let Some(vc) = visible_columns {
                    row.values.truncate(*vc);
                }
            }
            if let Some(pred) = having {
                merged.retain(|row| pred(row));
            }
            if !order_by.is_empty() {
                merged.sort_by(|a, b| {
                    for &(col_idx, ascending) in order_by {
                        let va = a.values.get(col_idx);
                        let vb = b.values.get(col_idx);
                        let ord = compare_datums(va, vb);
                        let ord = if ascending { ord } else { ord.reverse() };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
            if let Some(off) = offset {
                if *off < merged.len() {
                    merged = merged.split_off(*off);
                } else {
                    merged.clear();
                }
            }
            if let Some(lim) = limit {
                merged.truncate(*lim);
            }
            merged
        }
    };

    let gather_merge_latency_us = gather_start.elapsed().as_micros() as u64;

    Ok(GatherOutput {
        columns,
        merged_rows,
        total_rows_gathered,
        gather_merge_latency_us,
    })
}
