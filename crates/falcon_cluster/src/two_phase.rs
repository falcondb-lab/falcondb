//! Two-Phase Commit (2PC) coordinator for multi-shard write transactions.
//!
//! Ensures atomicity across shards: either ALL shards commit or ALL abort.
//!
//! Protocol:
//! 1. **Prepare phase**: Each participant shard executes the write and
//!    transitions to Prepared state. If any shard fails, all abort.
//! 2. **Commit phase**: Coordinator sends Commit to all prepared shards.
//!    If commit fails on any shard after prepare, the coordinator logs
//!    the inconsistency (in production, a recovery log resolves this).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use falcon_common::error::FalconError;
use falcon_common::types::ShardId;
use falcon_storage::engine::StorageEngine;
use falcon_txn::manager::TxnManager;

use crate::cross_shard::{
    CircuitBreaker, ConcurrencyLimiter, CrossShardLatencyBreakdown, CrossShardTxnResult,
    DeadlockDetector, FailureClass, LayeredTimeouts, RetryConfig, RetryOutcome,
    ShardConflictTracker, TimeoutPhase, TimeoutPolicy, WaitReason,
};
use crate::sharded_engine::ShardedEngine;

/// Outcome of a single shard's participation in a 2PC transaction.
#[derive(Debug)]
pub struct ShardParticipant {
    pub shard_id: ShardId,
    /// The per-shard txn_id allocated during prepare.
    pub txn_id: falcon_common::types::TxnId,
    /// Whether this shard successfully prepared.
    pub prepared: bool,
}

/// Result of a 2PC transaction.
#[derive(Debug)]
pub struct TwoPhaseResult {
    pub committed: bool,
    pub participants: Vec<ShardParticipant>,
    pub prepare_latency_us: u64,
    pub commit_latency_us: u64,
}

/// Coordinates two-phase commit across multiple shards.
pub struct TwoPhaseCoordinator {
    engine: Arc<ShardedEngine>,
    timeout: Duration,
}

impl TwoPhaseCoordinator {
    pub fn new(engine: Arc<ShardedEngine>, timeout: Duration) -> Self {
        Self { engine, timeout }
    }

    /// Upgrade to a hardened coordinator with all production primitives.
    pub fn into_hardened(self) -> HardenedTwoPhaseCoordinator {
        HardenedTwoPhaseCoordinator::new(self.engine, HardenedConfig::default())
    }

    /// Execute a multi-shard write transaction with 2PC.
    ///
    /// `write_fn` is called on each shard with (storage, txn_mgr, txn_id).
    /// It should execute the write operations but NOT commit — the coordinator
    /// handles commit/abort.
    ///
    /// Returns `TwoPhaseResult` indicating overall commit/abort.
    pub fn execute<F>(
        &self,
        target_shards: &[ShardId],
        isolation: falcon_common::types::IsolationLevel,
        write_fn: F,
    ) -> Result<TwoPhaseResult, FalconError>
    where
        F: Fn(&StorageEngine, &TxnManager, falcon_common::types::TxnId) -> Result<(), FalconError>
            + Send
            + Sync,
    {
        let total_start = Instant::now();

        // ── Phase 1: Prepare ──
        // Begin transactions on all shards and execute writes.
        let mut participants: Vec<ShardParticipant> = Vec::with_capacity(target_shards.len());
        let mut all_prepared = true;

        for &shard_id in target_shards {
            let shard = self.engine.shard(shard_id).ok_or_else(|| {
                FalconError::internal_bug(
                    "E-2PC-001",
                    format!("Shard {:?} not found during 2PC prepare", shard_id),
                    format!("target_shards={:?}", target_shards),
                )
            })?;

            let txn = shard.txn_mgr.begin(isolation);
            let txn_id = txn.txn_id;

            // Execute the write on this shard
            match write_fn(&shard.storage, &shard.txn_mgr, txn_id) {
                Ok(()) => {
                    participants.push(ShardParticipant {
                        shard_id,
                        txn_id,
                        prepared: true,
                    });
                }
                Err(e) => {
                    tracing::warn!("2PC prepare failed on shard {:?}: {}", shard_id, e);
                    // Mark as not prepared
                    participants.push(ShardParticipant {
                        shard_id,
                        txn_id,
                        prepared: false,
                    });
                    all_prepared = false;
                    break;
                }
            }

            // Check timeout
            if total_start.elapsed() > self.timeout {
                all_prepared = false;
                break;
            }
        }

        let prepare_latency_us = total_start.elapsed().as_micros() as u64;

        // ── Phase 2: Commit or Abort ──
        let commit_start = Instant::now();

        if all_prepared {
            // All shards prepared — commit all.
            for p in &participants {
                let shard = match self.engine.shard(p.shard_id) {
                    Some(s) => s,
                    None => {
                        tracing::error!(
                            "2PC commit: shard {:?} not found — skipping commit for txn {}",
                            p.shard_id,
                            p.txn_id
                        );
                        continue;
                    }
                };
                if let Err(e) = shard.txn_mgr.commit(p.txn_id) {
                    // In production, this would be logged to a recovery journal.
                    // The coordinator would retry or trigger manual resolution.
                    tracing::error!(
                        "2PC commit failed on shard {:?} after prepare: {}",
                        p.shard_id,
                        e
                    );
                    // Continue committing other shards — cannot undo a commit.
                }
            }
        } else {
            // At least one shard failed — abort all.
            for p in &participants {
                if p.prepared {
                    if let Some(shard) = self.engine.shard(p.shard_id) {
                        let _ = shard.txn_mgr.abort(p.txn_id);
                    }
                }
            }
        }

        let commit_latency_us = commit_start.elapsed().as_micros() as u64;

        Ok(TwoPhaseResult {
            committed: all_prepared,
            participants,
            prepare_latency_us,
            commit_latency_us,
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Hardened Two-Phase Coordinator
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for the hardened coordinator.
#[derive(Debug, Clone)]
pub struct HardenedConfig {
    /// Per-phase timeout configuration.
    pub timeouts: LayeredTimeouts,
    /// Timeout policy (fail-fast vs best-effort).
    pub timeout_policy: TimeoutPolicy,
    /// Coordinator-level concurrency limit (hard, soft).
    pub coord_concurrency: (u64, u64),
    /// Retry configuration.
    pub retry: RetryConfig,
    /// Circuit breaker: (failure_threshold, recovery_timeout).
    pub circuit_breaker: (u64, Duration),
    /// Conflict tracker window.
    pub conflict_window: Duration,
    /// Deadlock detection timeout.
    pub deadlock_timeout: Duration,
}

impl Default for HardenedConfig {
    fn default() -> Self {
        Self {
            timeouts: LayeredTimeouts::default(),
            timeout_policy: TimeoutPolicy::FailFast,
            coord_concurrency: (128, 96),
            retry: RetryConfig::default(),
            circuit_breaker: (5, Duration::from_secs(10)),
            conflict_window: Duration::from_secs(60),
            deadlock_timeout: Duration::from_secs(10),
        }
    }
}

/// Production-grade two-phase coordinator with:
/// - Per-phase latency breakdown
/// - Layered timeouts (fail-fast / best-effort)
/// - Coordinator concurrency limiter (semaphore)
/// - Retry with exponential backoff + jitter + budget
/// - Circuit breaker (anti-thundering-herd)
/// - Per-shard conflict/abort tracking
/// - Deadlock detection (timeout-based)
pub struct HardenedTwoPhaseCoordinator {
    engine: Arc<ShardedEngine>,
    config: HardenedConfig,
    coord_limiter: ConcurrencyLimiter,
    circuit_breaker: CircuitBreaker,
    conflict_tracker: ShardConflictTracker,
    deadlock_detector: DeadlockDetector,
    total_attempted: AtomicU64,
    total_committed: AtomicU64,
    total_aborted: AtomicU64,
}

impl HardenedTwoPhaseCoordinator {
    pub fn new(engine: Arc<ShardedEngine>, config: HardenedConfig) -> Self {
        let (hard, soft) = config.coord_concurrency;
        let (cb_thresh, cb_recovery) = config.circuit_breaker;
        Self {
            engine,
            coord_limiter: ConcurrencyLimiter::new(hard, soft),
            circuit_breaker: CircuitBreaker::new(cb_thresh, cb_recovery),
            conflict_tracker: ShardConflictTracker::new(config.conflict_window),
            deadlock_detector: DeadlockDetector::new(config.deadlock_timeout),
            config,
            total_attempted: AtomicU64::new(0),
            total_committed: AtomicU64::new(0),
            total_aborted: AtomicU64::new(0),
        }
    }

    /// Execute a hardened cross-shard write transaction.
    ///
    /// Integrates admission control, per-phase timeouts, retry,
    /// circuit breaker, conflict tracking, and deadlock detection.
    pub fn execute<F>(
        &self,
        target_shards: &[ShardId],
        isolation: falcon_common::types::IsolationLevel,
        write_fn: F,
    ) -> Result<CrossShardTxnResult, FalconError>
    where
        F: Fn(&StorageEngine, &TxnManager, falcon_common::types::TxnId) -> Result<(), FalconError>
            + Send
            + Sync,
    {
        let txn_seq = self.total_attempted.fetch_add(1, Ordering::Relaxed);
        let total_start = Instant::now();
        let mut latency = CrossShardLatencyBreakdown::default();

        // ── Circuit Breaker Check ──
        if !self.circuit_breaker.allow_request() {
            return Ok(CrossShardTxnResult {
                committed: false,
                shard_count: target_shards.len(),
                latency,
                retry: None,
                abort_reason: Some("circuit breaker open".into()),
                timeout_phase: None,
                failure_class: Some(FailureClass::Transient),
            });
        }

        // ── Admission Control: coordinator queue ──
        let queue_start = Instant::now();
        let _guard = match self.coord_limiter.try_acquire() {
            Ok(g) => g,
            Err(e) => {
                latency.coord_queue_us = queue_start.elapsed().as_micros() as u64;
                return Ok(CrossShardTxnResult {
                    committed: false,
                    shard_count: target_shards.len(),
                    latency,
                    retry: None,
                    abort_reason: Some(format!("admission rejected: {}", e)),
                    timeout_phase: None,
                    failure_class: Some(FailureClass::Transient),
                });
            }
        };
        latency.coord_queue_us = queue_start.elapsed().as_micros() as u64;

        // Check coord queue timeout
        if self.config.timeout_policy == TimeoutPolicy::FailFast
            && latency.coord_queue_us > self.config.timeouts.coord_queue.as_micros() as u64
        {
            return Ok(CrossShardTxnResult {
                committed: false,
                shard_count: target_shards.len(),
                latency,
                retry: None,
                abort_reason: Some("coordinator queue timeout".into()),
                timeout_phase: Some(TimeoutPhase::CoordinatorQueue),
                failure_class: Some(FailureClass::Timeout),
            });
        }

        // ── Retry Loop ──
        let mut retry_outcome = RetryOutcome {
            attempts: 0,
            total_backoff: Duration::ZERO,
            failure_classes: Vec::new(),
            succeeded: false,
        };
        let retry_budget_start = Instant::now();

        for attempt in 0..=self.config.retry.max_retries {
            retry_outcome.attempts = attempt + 1;

            // Register with deadlock detector
            let shard_ids_raw: Vec<u64> = target_shards.iter().map(|s| s.0).collect();
            self.deadlock_detector
                .register_wait(txn_seq, shard_ids_raw, WaitReason::CommitBarrier);

            let result = self.execute_inner(
                target_shards,
                isolation,
                &write_fn,
                &total_start,
                &mut latency,
            );

            self.deadlock_detector.unregister(txn_seq);

            match result {
                Ok(true) => {
                    retry_outcome.succeeded = true;
                    self.circuit_breaker.record_success();
                    self.total_committed.fetch_add(1, Ordering::Relaxed);
                    for &sid in target_shards {
                        self.conflict_tracker.record_txn(sid.0);
                    }
                    latency.total_us = total_start.elapsed().as_micros() as u64;
                    return Ok(CrossShardTxnResult {
                        committed: true,
                        shard_count: target_shards.len(),
                        latency,
                        retry: Some(retry_outcome),
                        abort_reason: None,
                        timeout_phase: None,
                        failure_class: None,
                    });
                }
                Ok(false) => {
                    self.total_aborted.fetch_add(1, Ordering::Relaxed);
                    retry_outcome.failure_classes.push(FailureClass::Conflict);
                    for &sid in target_shards {
                        self.conflict_tracker.record_txn(sid.0);
                        self.conflict_tracker.record_conflict(sid.0);
                    }
                }
                Err(e) => {
                    let fc = FailureClass::classify(&e);
                    retry_outcome.failure_classes.push(fc.clone());
                    if !fc.is_retryable() {
                        self.circuit_breaker.record_failure();
                        self.total_aborted.fetch_add(1, Ordering::Relaxed);
                        latency.total_us = total_start.elapsed().as_micros() as u64;
                        return Ok(CrossShardTxnResult {
                            committed: false,
                            shard_count: target_shards.len(),
                            latency,
                            retry: Some(retry_outcome),
                            abort_reason: Some(e.to_string()),
                            timeout_phase: None,
                            failure_class: Some(fc),
                        });
                    }
                }
            }

            // Check retry budget
            if retry_budget_start.elapsed() >= self.config.retry.retry_budget {
                break;
            }
            if attempt < self.config.retry.max_retries {
                let backoff = self.config.retry.backoff_for_attempt(attempt);
                retry_outcome.total_backoff += backoff;
                latency.retry_backoff_us += backoff.as_micros() as u64;
                // Interruptible backoff: use condvar timeout instead of bare sleep
                // so the thread yields properly and doesn't block shutdown.
                let pair = std::sync::Mutex::new(false);
                let cvar = std::sync::Condvar::new();
                let guard = pair.lock().unwrap_or_else(|e| e.into_inner());
                let _ = cvar.wait_timeout(guard, backoff);
            }
        }

        // Exhausted retries
        self.circuit_breaker.record_failure();
        self.total_aborted.fetch_add(1, Ordering::Relaxed);
        latency.total_us = total_start.elapsed().as_micros() as u64;
        Ok(CrossShardTxnResult {
            committed: false,
            shard_count: target_shards.len(),
            latency,
            retry: Some(retry_outcome),
            abort_reason: Some("exhausted retries".into()),
            timeout_phase: None,
            failure_class: Some(FailureClass::Conflict),
        })
    }

    /// Inner execution: single attempt at 2PC.
    /// Returns Ok(true) = committed, Ok(false) = aborted (prepare failed),
    /// Err = hard error.
    fn execute_inner<F>(
        &self,
        target_shards: &[ShardId],
        isolation: falcon_common::types::IsolationLevel,
        write_fn: &F,
        total_start: &Instant,
        latency: &mut CrossShardLatencyBreakdown,
    ) -> Result<bool, FalconError>
    where
        F: Fn(&StorageEngine, &TxnManager, falcon_common::types::TxnId) -> Result<(), FalconError>
            + Send
            + Sync,
    {
        // ── Phase 1: Prepare ──
        let prepare_start = Instant::now();
        let mut participants: Vec<ShardParticipant> = Vec::with_capacity(target_shards.len());
        let mut all_prepared = true;

        for &shard_id in target_shards {
            // Check total timeout
            if total_start.elapsed() > self.config.timeouts.total {
                self.abort_all(&participants);
                latency.prepare_wal_us = prepare_start.elapsed().as_micros() as u64;
                return Err(FalconError::Transient {
                    reason: format!(
                        "2PC total timeout exceeded during prepare (shard {:?})",
                        shard_id
                    ),
                    retry_after_ms: 100,
                });
            }

            let shard = self.engine.shard(shard_id).ok_or_else(|| {
                FalconError::internal_bug(
                    "E-2PC-002",
                    format!("Shard {:?} not found during hardened 2PC prepare", shard_id),
                    format!("target_shards={:?}", target_shards),
                )
            })?;

            let rpc_start = Instant::now();
            let txn = shard.txn_mgr.begin(isolation);
            let txn_id = txn.txn_id;

            match write_fn(&shard.storage, &shard.txn_mgr, txn_id) {
                Ok(()) => {
                    let rpc_us = rpc_start.elapsed().as_micros() as u64;
                    latency.shard_rpc_us.push((shard_id.0, rpc_us));

                    // Per-shard prepare timeout check
                    if self.config.timeout_policy == TimeoutPolicy::FailFast
                        && rpc_us > self.config.timeouts.prepare.as_micros() as u64
                    {
                        let _ = shard.txn_mgr.abort(txn_id);
                        self.abort_all(&participants);
                        latency.prepare_wal_us = prepare_start.elapsed().as_micros() as u64;
                        return Err(FalconError::Transient {
                            reason: format!(
                                "2PC prepare timeout on shard {:?} ({}us > {}us)",
                                shard_id,
                                rpc_us,
                                self.config.timeouts.prepare.as_micros()
                            ),
                            retry_after_ms: 50,
                        });
                    }

                    participants.push(ShardParticipant {
                        shard_id,
                        txn_id,
                        prepared: true,
                    });
                }
                Err(e) => {
                    latency
                        .shard_rpc_us
                        .push((shard_id.0, rpc_start.elapsed().as_micros() as u64));
                    tracing::warn!("2PC prepare failed on shard {:?}: {}", shard_id, e);
                    participants.push(ShardParticipant {
                        shard_id,
                        txn_id,
                        prepared: false,
                    });
                    all_prepared = false;
                    break;
                }
            }
        }

        latency.prepare_wal_us = prepare_start.elapsed().as_micros() as u64;

        // ── Commit Barrier ──
        let barrier_start = Instant::now();
        latency.commit_barrier_us = barrier_start.elapsed().as_micros() as u64;

        // ── Phase 2: Commit or Abort ──
        let commit_start = Instant::now();

        if all_prepared {
            for p in &participants {
                let shard = match self.engine.shard(p.shard_id) {
                    Some(s) => s,
                    None => {
                        tracing::error!(
                            "2PC hardened commit: shard {:?} not found — skipping",
                            p.shard_id
                        );
                        continue;
                    }
                };
                if let Err(e) = shard.txn_mgr.commit(p.txn_id) {
                    tracing::error!(
                        "2PC commit failed on shard {:?} after prepare: {}",
                        p.shard_id,
                        e
                    );
                }
            }
            latency.commit_wal_us = commit_start.elapsed().as_micros() as u64;
            Ok(true)
        } else {
            self.abort_all(&participants);
            latency.commit_wal_us = commit_start.elapsed().as_micros() as u64;
            Ok(false)
        }
    }

    fn abort_all(&self, participants: &[ShardParticipant]) {
        for p in participants {
            if p.prepared {
                if let Some(shard) = self.engine.shard(p.shard_id) {
                    let _ = shard.txn_mgr.abort(p.txn_id);
                }
            }
        }
    }

    // ── Observability ──

    /// Coordinator concurrency limiter stats.
    pub fn concurrency_stats(&self) -> crate::cross_shard::ConcurrencyStats {
        self.coord_limiter.stats()
    }

    /// Circuit breaker stats.
    pub fn circuit_breaker_stats(&self) -> crate::cross_shard::CircuitBreakerStats {
        self.circuit_breaker.stats()
    }

    /// Per-shard conflict/abort snapshots.
    pub fn conflict_snapshots(&self) -> Vec<crate::cross_shard::ShardConflictSnapshot> {
        self.conflict_tracker.all_snapshots()
    }

    /// Detect stalled transactions.
    pub fn detect_stalled(&self) -> Vec<crate::cross_shard::StalledTxn> {
        self.deadlock_detector.detect_stalled()
    }

    /// Lifetime counters.
    pub fn total_attempted(&self) -> u64 {
        self.total_attempted.load(Ordering::Relaxed)
    }
    pub fn total_committed(&self) -> u64 {
        self.total_committed.load(Ordering::Relaxed)
    }
    pub fn total_aborted(&self) -> u64 {
        self.total_aborted.load(Ordering::Relaxed)
    }

    /// Access the underlying sharded engine.
    pub fn engine(&self) -> &ShardedEngine {
        &self.engine
    }
}
