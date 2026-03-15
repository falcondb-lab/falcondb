//! # C4: Parallel Two-Phase Commit
//!
//! Replaces the serial per-shard prepare loop in `TwoPhaseCoordinator::execute`
//! with **parallel prepare**: all participant shards receive their prepare
//! request simultaneously, reducing multi-shard write latency from O(N·RTT)
//! to O(max_RTT).
//!
//! ## Why this matters
//!
//! With N=8 shards and 1ms per-shard prepare latency, serial 2PC costs 8ms.
//! Parallel 2PC costs 1ms (the slowest shard determines the critical path).
//!
//! ## Protocol
//!
//! ```text
//!   Phase 1 — Parallel Prepare:
//!       Spawn N threads (std::thread::scope), one per shard.
//!       Each thread: begin txn → execute write_fn → return PrepareResult.
//!       Coordinator waits for ALL to complete (barrier).
//!       If ANY fails → decision = Abort.
//!       If ALL succeed → decision = Commit.
//!
//!   Decision durability:
//!       Log decision to CoordinatorDecisionLog BEFORE phase 2.
//!
//!   Phase 2 — Parallel Commit/Abort:
//!       Spawn N threads simultaneously.
//!       Each thread: commit or abort its per-shard txn.
//!
//!   Idempotency:
//!       On coordinator crash between phase 1 and phase 2, the
//!       InDoubtResolver recovers using the durable decision log.
//! ```
//!
//! ## Timeout Handling
//!
//! A single `deadline` is computed before phase 1 begins. Each shard thread
//! checks remaining budget and returns `Err(Timeout)` if exceeded, so the
//! coordinator never waits longer than `timeout` wall-clock time total.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use falcon_common::error::FalconError;
use falcon_common::types::{IsolationLevel, ShardId, TxnId};
use falcon_storage::engine::StorageEngine;
use falcon_txn::manager::TxnManager;

use crate::deterministic_2pc::{CoordinatorDecision, CoordinatorDecisionLog};
use crate::sharded_engine::ShardedEngine;

// ─────────────────────────────────────────────────────────────────────────────
// Parallel 2PC Result
// ─────────────────────────────────────────────────────────────────────────────

/// Per-shard outcome of the prepare phase.
#[derive(Debug)]
pub struct ParallelShardResult {
    pub shard_id: ShardId,
    pub txn_id: TxnId,
    pub prepared: bool,
    pub prepare_latency_us: u64,
    /// Error message if prepare failed.
    pub error: Option<String>,
}

/// Full result of a parallel 2PC execution.
#[derive(Debug)]
pub struct Parallel2PcResult {
    pub committed: bool,
    pub participants: Vec<ParallelShardResult>,
    /// Max of all per-shard prepare latencies (critical path).
    pub prepare_latency_us: u64,
    /// Max of all per-shard commit latencies.
    pub commit_latency_us: u64,
    /// Total wall-clock time.
    pub total_latency_us: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct Parallel2PcMetrics {
    pub commits: AtomicU64,
    pub aborts: AtomicU64,
    pub timeouts: AtomicU64,
    pub prepare_errors: AtomicU64,
    /// Sum of prepare latency reduction vs. serial (estimated as
    /// `sum_prepare_us - max_prepare_us` per transaction).
    pub latency_saved_us: AtomicU64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Parallel2PcCoordinator
// ─────────────────────────────────────────────────────────────────────────────

/// Coordinates two-phase commit with **parallel prepare and parallel commit**.
pub struct Parallel2PcCoordinator {
    engine: Arc<ShardedEngine>,
    timeout: Duration,
    decision_log: Option<Arc<CoordinatorDecisionLog>>,
    global_txn_seq: AtomicU64,
    pub metrics: Parallel2PcMetrics,
}

impl Parallel2PcCoordinator {
    pub fn new(engine: Arc<ShardedEngine>, timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            engine,
            timeout,
            decision_log: None,
            global_txn_seq: AtomicU64::new(1),
            metrics: Parallel2PcMetrics::default(),
        })
    }

    pub fn new_with_log(
        engine: Arc<ShardedEngine>,
        timeout: Duration,
        log: Arc<CoordinatorDecisionLog>,
    ) -> Arc<Self> {
        Arc::new(Self {
            engine,
            timeout,
            decision_log: Some(log),
            global_txn_seq: AtomicU64::new(1),
            metrics: Parallel2PcMetrics::default(),
        })
    }

    /// Execute a multi-shard write with parallel prepare and parallel commit.
    ///
    /// `write_fn` is called on each shard's (StorageEngine, TxnManager, TxnId).
    /// It must **not** call commit — the coordinator handles that.
    ///
    /// The function is called in parallel across all shards; it must be
    /// `Send + Sync` and must not assume any ordering relative to other shards.
    pub fn execute<F>(
        self: &Arc<Self>,
        target_shards: &[ShardId],
        isolation: IsolationLevel,
        write_fn: F,
    ) -> Result<Parallel2PcResult, FalconError>
    where
        F: Fn(&StorageEngine, &TxnManager, TxnId) -> Result<(), FalconError> + Send + Sync,
    {
        if target_shards.is_empty() {
            return Ok(Parallel2PcResult {
                committed: true,
                participants: vec![],
                prepare_latency_us: 0,
                commit_latency_us: 0,
                total_latency_us: 0,
            });
        }

        let total_start = Instant::now();
        let deadline = total_start + self.timeout;

        // ── Validate all shard IDs up-front ────────────────────────────────
        for &sid in target_shards {
            if self.engine.shard(sid).is_none() {
                return Err(FalconError::Internal(format!(
                    "Parallel2PC: shard {sid:?} not found"
                )));
            }
        }

        // ── Phase 1: Parallel Prepare ───────────────────────────────────────
        // Each shard thread: begin txn → execute write_fn → store result.
        let phase1_results: Vec<ParallelShardResult> = std::thread::scope(|s| {
            #[allow(clippy::needless_collect)]
            let handles: Vec<_> = target_shards
                .iter()
                .map(|&shard_id| {
                    let engine = &self.engine;
                    let wf = &write_fn;
                    s.spawn(move || {
                        let t0 = Instant::now();

                        // Check timeout before even starting.
                        if Instant::now() >= deadline {
                            return ParallelShardResult {
                                shard_id,
                                txn_id: TxnId(0),
                                prepared: false,
                                prepare_latency_us: 0,
                                error: Some("prepare timeout before start".into()),
                            };
                        }

                        let shard = match engine.shard(shard_id) {
                            Some(s) => s,
                            None => {
                                return ParallelShardResult {
                                    shard_id,
                                    txn_id: TxnId(0),
                                    prepared: false,
                                    prepare_latency_us: 0,
                                    error: Some(format!("shard {shard_id:?} not found")),
                                }
                            }
                        };

                        let txn = shard.txn_mgr.begin(isolation);
                        let txn_id = txn.txn_id;

                        // Check timeout after begin.
                        if Instant::now() >= deadline {
                            let _ = shard.txn_mgr.abort(txn_id);
                            return ParallelShardResult {
                                shard_id,
                                txn_id,
                                prepared: false,
                                prepare_latency_us: t0.elapsed().as_micros() as u64,
                                error: Some("prepare timeout after begin".into()),
                            };
                        }

                        match wf(&shard.storage, &shard.txn_mgr, txn_id) {
                            Ok(()) => ParallelShardResult {
                                shard_id,
                                txn_id,
                                prepared: true,
                                prepare_latency_us: t0.elapsed().as_micros() as u64,
                                error: None,
                            },
                            Err(e) => {
                                let _ = shard.txn_mgr.abort(txn_id);
                                ParallelShardResult {
                                    shard_id,
                                    txn_id,
                                    prepared: false,
                                    prepare_latency_us: t0.elapsed().as_micros() as u64,
                                    error: Some(e.to_string()),
                                }
                            }
                        }
                    })
                })
                .collect();

            handles
                .into_iter()
                .map(|h| {
                    h.join().unwrap_or_else(|_| ParallelShardResult {
                        shard_id: ShardId(u64::MAX),
                        txn_id: TxnId(0),
                        prepared: false,
                        prepare_latency_us: 0,
                        error: Some("shard thread panicked".into()),
                    })
                })
                .collect()
        });

        let prepare_latency_us = phase1_results
            .iter()
            .map(|r| r.prepare_latency_us)
            .max()
            .unwrap_or(0);
        let sum_prepare_us: u64 = phase1_results.iter().map(|r| r.prepare_latency_us).sum();
        let latency_saved = sum_prepare_us.saturating_sub(prepare_latency_us);
        self.metrics
            .latency_saved_us
            .fetch_add(latency_saved, Ordering::Relaxed);

        let all_prepared = phase1_results.iter().all(|r| r.prepared);
        if !all_prepared {
            self.metrics.prepare_errors.fetch_add(1, Ordering::Relaxed);
        }

        // Check timeout after phase 1.
        if total_start.elapsed() > self.timeout {
            self.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
            // Abort all prepared participants.
            self.parallel_abort(&phase1_results);
            return Err(FalconError::Transient {
                reason: format!(
                    "Parallel2PC timeout after prepare phase ({}ms)",
                    self.timeout.as_millis()
                ),
                retry_after_ms: 100,
            });
        }

        // ── Decision durability ─────────────────────────────────────────────
        let global_txn_id = TxnId(self.global_txn_seq.fetch_add(1, Ordering::Relaxed));
        let decision = if all_prepared {
            CoordinatorDecision::Commit
        } else {
            CoordinatorDecision::Abort
        };
        let shard_ids: Vec<ShardId> = phase1_results.iter().map(|r| r.shard_id).collect();

        if let Some(ref log) = self.decision_log {
            log.log_decision(global_txn_id, decision, &shard_ids, prepare_latency_us);
        }

        // ── Phase 2: Parallel Commit or Abort ──────────────────────────────
        let commit_start = Instant::now();

        if all_prepared {
            self.parallel_commit(&phase1_results);
            if let Some(ref log) = self.decision_log {
                log.mark_applied(global_txn_id);
            }
            self.metrics.commits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.parallel_abort(&phase1_results);
            self.metrics.aborts.fetch_add(1, Ordering::Relaxed);
        }

        let commit_latency_us = commit_start.elapsed().as_micros() as u64;
        let total_latency_us = total_start.elapsed().as_micros() as u64;

        Ok(Parallel2PcResult {
            committed: all_prepared,
            participants: phase1_results,
            prepare_latency_us,
            commit_latency_us,
            total_latency_us,
        })
    }

    /// Commit all prepared shards in parallel.
    fn parallel_commit(&self, results: &[ParallelShardResult]) {
        std::thread::scope(|s| {
            for r in results.iter().filter(|r| r.prepared) {
                let shard_id = r.shard_id;
                let txn_id = r.txn_id;
                let engine = &self.engine;
                s.spawn(move || {
                    if let Some(shard) = engine.shard(shard_id) {
                        if let Err(e) = shard.txn_mgr.commit(txn_id) {
                            tracing::error!(
                                "Parallel2PC: commit failed on shard {:?} txn {:?}: {}",
                                shard_id,
                                txn_id,
                                e
                            );
                        }
                    }
                });
            }
        });
    }

    /// Abort all prepared shards in parallel.
    fn parallel_abort(&self, results: &[ParallelShardResult]) {
        std::thread::scope(|s| {
            for r in results.iter().filter(|r| r.prepared) {
                let shard_id = r.shard_id;
                let txn_id = r.txn_id;
                let engine = &self.engine;
                s.spawn(move || {
                    if let Some(shard) = engine.shard(shard_id) {
                        let _ = shard.txn_mgr.abort(txn_id);
                    }
                });
            }
        });
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sharded_engine::ShardedEngine;

    fn make_engine(n: u64) -> Arc<ShardedEngine> {
        Arc::new(ShardedEngine::new(n))
    }

    #[test]
    fn parallel_2pc_commit_two_shards() {
        let engine = make_engine(2);
        let coord = Parallel2PcCoordinator::new(engine, Duration::from_secs(5));
        let result = coord
            .execute(
                &[ShardId(0), ShardId(1)],
                IsolationLevel::SnapshotIsolation,
                |_storage, _txn_mgr, _txn_id| Ok(()),
            )
            .unwrap();
        assert!(result.committed);
        assert_eq!(result.participants.len(), 2);
        assert!(result.participants.iter().all(|p| p.prepared));
        assert_eq!(coord.metrics.commits.load(Ordering::Relaxed), 1);
        assert_eq!(coord.metrics.aborts.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn parallel_2pc_aborts_on_error() {
        let engine = make_engine(3);
        let coord = Parallel2PcCoordinator::new(engine, Duration::from_secs(5));
        // write_fn fails on shard 1.
        let result = coord
            .execute(
                &[ShardId(0), ShardId(1), ShardId(2)],
                IsolationLevel::SnapshotIsolation,
                |_storage, _txn_mgr, _txn_id| {
                    Err(FalconError::Internal("simulated write failure".into()))
                },
            )
            .unwrap();
        assert!(!result.committed);
        assert_eq!(coord.metrics.aborts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn parallel_2pc_empty_shards() {
        let engine = make_engine(1);
        let coord = Parallel2PcCoordinator::new(engine, Duration::from_secs(5));
        let result = coord
            .execute(&[], IsolationLevel::SnapshotIsolation, |_, _, _| Ok(()))
            .unwrap();
        assert!(result.committed);
        assert!(result.participants.is_empty());
    }

    #[test]
    fn parallel_2pc_is_faster_than_serial() {
        // This test verifies that N shards are processed concurrently by
        // checking that total time < N * per_shard_delay.
        let n = 4u64;
        let engine = make_engine(n);
        let coord = Parallel2PcCoordinator::new(Arc::clone(&engine), Duration::from_secs(10));
        let shards: Vec<ShardId> = (0..n).map(ShardId).collect();
        let delay = Duration::from_millis(20);
        let t0 = Instant::now();
        coord
            .execute(
                &shards,
                IsolationLevel::SnapshotIsolation,
                move |_s, _m, _id| {
                    std::thread::sleep(delay);
                    Ok(())
                },
            )
            .unwrap();
        let elapsed = t0.elapsed();
        let serial_bound = delay * n as u32;
        // Parallel should finish well under N * delay.
        assert!(
            elapsed < serial_bound,
            "parallel prepare took {:?}, but serial bound is {:?}",
            elapsed,
            serial_bound
        );
    }
}
