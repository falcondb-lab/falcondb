//! Scatter/gather execution for distributed plans.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_common::types::{IsolationLevel, ShardId};
use falcon_executor::executor::{ExecutionResult, Executor};
use falcon_planner::plan::{DistAggMerge, DistGather, PhysicalPlan};

use crate::distributed_exec::{AggMerge, ShardResult};
use crate::gateway::ScatterStats;

impl super::DistributedQueryEngine {
    /// Execute a DistPlan: scatter the subplan to each shard's Executor in parallel,
    /// then gather/merge results according to the DistGather strategy.
    ///
    /// **Optimization**: if the subplan is a SeqScan with a PK equality filter,
    /// route to a single shard instead of scattering to all shards.
    pub(crate) fn exec_dist_plan(
        &self,
        subplan: &PhysicalPlan,
        target_shards: &[ShardId],
        gather: &DistGather,
    ) -> Result<ExecutionResult, FalconError> {
        // Track gateway forwarding
        self.gateway_metrics.forward_total.fetch_add(1, Ordering::Relaxed);
        let gw_start = Instant::now();

        // ── Single-shard shortcut: PK point lookup ──
        if let Some(single_shard) = self.try_single_shard_scan(subplan) {
            // Record shard pruning in stats for observability
            let start = Instant::now();
            let result = self.execute_on_shard_auto_txn(single_shard, subplan);
            let elapsed = start.elapsed().as_micros() as u64;
            self.gateway_metrics.forward_latency_us.fetch_add(
                gw_start.elapsed().as_micros() as u64, Ordering::Relaxed);
            if result.is_err() {
                self.gateway_metrics.forward_failed_total.fetch_add(1, Ordering::Relaxed);
            }
            let row_count = match &result {
                Ok(ExecutionResult::Query { rows, .. }) => rows.len(),
                _ => 0,
            };
            *self
                .last_scatter_stats
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = ScatterStats {
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
                    "E-QE-013",
                    format!("Shard {sid:?} not found"),
                    "exec_dist_plan pre-validation",
                ));
            }
        }

        // Shared cancellation flag: set when any shard exceeds timeout.
        let cancelled = AtomicBool::new(false);

        // ── Scatter: execute subplan on each shard in parallel ──
        let shard_results: Vec<Result<ShardResult, FalconError>> = std::thread::scope(|s| {
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
                                    "DistPlan cancelled before shard {shard_id:?} started",
                                ),
                                retry_after_ms: 50,
                            });
                        }

                        let shard = match engine.shard(shard_id) {
                            Some(s) => s,
                            None => {
                                return Err(FalconError::Cluster(
                                    falcon_common::error::ClusterError::ShardNotFound(shard_id.0),
                                ))
                            }
                        };

                        // Retry once on transient failure.
                        let max_attempts = 2u8;
                        let mut last_err = None;
                        for attempt in 0..max_attempts {
                            if attempt > 0 {
                                // Interruptible retry delay (condvar instead of bare sleep)
                                {
                                    let pair = std::sync::Mutex::new(false);
                                    let cvar = std::sync::Condvar::new();
                                    let guard = pair.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                                    let _ = cvar
                                        .wait_timeout(guard, std::time::Duration::from_millis(5));
                                }
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
                                        timeout.as_millis(),
                                        shard_id,
                                        latency_us,
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
                                        format!("shard {shard_id:?}"),
                                    ));
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
                            "E-QE-005",
                            "shard thread panicked",
                            "exec_dist_plan_scatter",
                        ))
                    })
                })
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
                    failed_shards.push((sid, format!("{e}")));
                }
            }
        }
        // If ALL shards failed, propagate the error.
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

        // ── Gather: merge using the same logic as DistributedExecutor ──
        let columns = collected
            .first()
            .map(|r| r.columns.clone())
            .unwrap_or_default();
        let total_rows: usize = collected.iter().map(|r| r.rows.len()).sum();

        let merged_rows = match gather {
            DistGather::Union {
                distinct,
                limit,
                offset,
            } => {
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
            DistGather::MergeSortLimit {
                sort_columns,
                limit,
                offset,
            } => {
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
                    let shard_rows: Vec<&[OwnedRow]> =
                        collected.iter().map(|sr| sr.rows.as_slice()).collect();

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
                        DistAggMerge::StringAggDistinct(i, ref sep) => {
                            AggMerge::StringAggDistinct(*i, sep.clone())
                        }
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
                    row.values.truncate(*visible_columns);
                }
                // Apply HAVING filter post-merge (was stripped from subplan)
                if let Some(having_expr) = having {
                    merged.retain(|row| {
                        falcon_executor::expr_engine::ExprEngine::eval_filter(having_expr, row)
                            .unwrap_or(false)
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
            agg_merges.iter().map(std::string::ToString::to_string).collect()
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
                failed_shards: failed_shard_ids.clone(),
                merge_labels,
                pruned_to_shard: None,
            };
        }

        // Record gateway forwarding latency and failures for the full scatter/gather path
        self.gateway_metrics.forward_latency_us.fetch_add(
            gw_start.elapsed().as_micros() as u64, Ordering::Relaxed);
        if !failed_shard_ids.is_empty() {
            self.gateway_metrics.forward_failed_total.fetch_add(
                failed_shard_ids.len() as u64, Ordering::Relaxed);
        }

        Ok(ExecutionResult::Query {
            columns,
            rows: merged_rows,
        })
    }
}
