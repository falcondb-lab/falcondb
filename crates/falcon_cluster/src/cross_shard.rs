//! Cross-Shard Transaction Hardening Module
//!
//! Production-grade infrastructure for cross-shard transactions:
//! - **§2.1 Critical Path Breakdown**: Per-phase latency tracking for every 2PC txn
//! - **§2.2 Layered Timeouts**: Per-phase timeout budgets (queue, RPC, prepare, commit, lock, retry)
//! - **§3 Admission Control**: Semaphore-based coordinator + shard concurrency limiting
//! - **§3.2 Backpressure**: Queue policies (FIFO/priority), load shedding
//! - **§5 Retry Framework**: Error classification, exponential backoff+jitter, retry budget, circuit breaker
//! - **§4 Conflict Tracking**: Per-shard abort/conflict rate tracking, adaptive concurrency
//! - **§7 Deadlock Detection**: Timeout-based deadlock breaker with explainable abort reason

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use falcon_common::types::ShardId;

// ═══════════════════════════════════════════════════════════════════════════
// §2.1: Cross-Shard Transaction Latency Breakdown
// ═══════════════════════════════════════════════════════════════════════════

/// Per-phase latency breakdown for a single cross-shard transaction.
/// Every field is in **microseconds** (0 = phase not entered).
///
/// This enables single-txn tracing to pinpoint exactly which phase
/// caused tail latency.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CrossShardLatencyBreakdown {
    /// Coordinator queue wait time (admission control).
    pub coord_queue_us: u64,
    /// Route/planning: determine target shards + txn classification.
    pub route_plan_us: u64,
    /// Per-shard RPC round-trip times (shard_id → latency_us).
    pub shard_rpc_us: Vec<(u64, u64)>,
    /// Read phase time (across all shards).
    pub read_phase_us: u64,
    /// Lock/validation time (OCC validate or 2PL lock acquire).
    pub lock_validate_us: u64,
    /// Prepare phase: write WAL on each shard.
    pub prepare_wal_us: u64,
    /// Commit barrier: coordinator decides commit/abort.
    pub commit_barrier_us: u64,
    /// Commit phase: write commit WAL on each shard.
    pub commit_wal_us: u64,
    /// Replication ack time (if under ack-based commit policy).
    pub replication_ack_us: u64,
    /// GC/vacuum impact (if GC ran during this txn).
    pub gc_impact_us: u64,
    /// Retry/backoff time (cumulative across retries).
    pub retry_backoff_us: u64,
    /// Total end-to-end latency.
    pub total_us: u64,
}

impl CrossShardLatencyBreakdown {
    /// Compute total from phases (for validation).
    pub fn computed_total(&self) -> u64 {
        self.coord_queue_us
            + self.route_plan_us
            + self.read_phase_us
            + self.lock_validate_us
            + self.prepare_wal_us
            + self.commit_barrier_us
            + self.commit_wal_us
            + self.replication_ack_us
            + self.gc_impact_us
            + self.retry_backoff_us
    }

    /// Max per-shard RPC latency (critical path for parallel scatter).
    pub fn max_shard_rpc_us(&self) -> u64 {
        self.shard_rpc_us
            .iter()
            .map(|(_, us)| *us)
            .max()
            .unwrap_or(0)
    }

    /// Dominant phase name (largest contributor).
    pub fn dominant_phase(&self) -> &'static str {
        let phases: [(&str, u64); 10] = [
            ("coord_queue", self.coord_queue_us),
            ("route_plan", self.route_plan_us),
            ("shard_rpc", self.max_shard_rpc_us()),
            ("read_phase", self.read_phase_us),
            ("lock_validate", self.lock_validate_us),
            ("prepare_wal", self.prepare_wal_us),
            ("commit_barrier", self.commit_barrier_us),
            ("commit_wal", self.commit_wal_us),
            ("replication_ack", self.replication_ack_us),
            ("retry_backoff", self.retry_backoff_us),
        ];
        phases
            .iter()
            .max_by_key(|p| p.1)
            .map(|p| p.0)
            .unwrap_or("unknown")
    }

    /// Compact waterfall string for tracing.
    pub fn waterfall(&self) -> String {
        format!(
            "queue={}|route={}|rpc_max={}|read={}|lock={}|prep={}|barrier={}|commit={}|repl={}|retry={}|total={}",
            self.coord_queue_us, self.route_plan_us, self.max_shard_rpc_us(),
            self.read_phase_us, self.lock_validate_us, self.prepare_wal_us,
            self.commit_barrier_us, self.commit_wal_us, self.replication_ack_us,
            self.retry_backoff_us, self.total_us,
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2.2: Layered Timeout Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// Per-phase timeout configuration for cross-shard transactions.
/// Each phase has an independent timeout; exceeding any phase timeout
/// triggers an abort with an explainable timeout reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayeredTimeouts {
    /// Coordinator queue admission timeout.
    pub coord_queue: Duration,
    /// Per-shard RPC round-trip timeout.
    pub shard_rpc: Duration,
    /// Prepare phase timeout (per shard).
    pub prepare: Duration,
    /// Commit phase timeout (per shard).
    pub commit: Duration,
    /// Lock/validation timeout.
    pub lock_validate: Duration,
    /// Total end-to-end timeout (hard ceiling).
    pub total: Duration,
    /// Maximum cumulative retry time.
    pub retry_ceiling: Duration,
}

impl Default for LayeredTimeouts {
    fn default() -> Self {
        Self {
            coord_queue: Duration::from_millis(500),
            shard_rpc: Duration::from_secs(2),
            prepare: Duration::from_secs(5),
            commit: Duration::from_secs(5),
            lock_validate: Duration::from_secs(1),
            total: Duration::from_secs(30),
            retry_ceiling: Duration::from_secs(10),
        }
    }
}

/// Timeout policy: what to do when a phase timeout is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TimeoutPolicy {
    /// Abort immediately on any phase timeout (prefer stability).
    #[default]
    FailFast,
    /// Allow overshooting phase timeouts as long as total timeout holds
    /// (prefer success rate).
    BestEffort,
}

/// Which phase timed out (for explainable errors).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeoutPhase {
    CoordinatorQueue,
    ShardRpc(ShardId),
    Prepare(ShardId),
    Commit(ShardId),
    LockValidate,
    Total,
    RetryCeiling,
}

impl std::fmt::Display for TimeoutPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CoordinatorQueue => write!(f, "coordinator_queue"),
            Self::ShardRpc(s) => write!(f, "shard_rpc({})", s.0),
            Self::Prepare(s) => write!(f, "prepare(shard_{})", s.0),
            Self::Commit(s) => write!(f, "commit(shard_{})", s.0),
            Self::LockValidate => write!(f, "lock_validate"),
            Self::Total => write!(f, "total"),
            Self::RetryCeiling => write!(f, "retry_ceiling"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3: Concurrency Limiter (Semaphore-based admission control)
// ═══════════════════════════════════════════════════════════════════════════

/// Semaphore-based concurrency limiter for coordinator and per-shard limits.
///
/// Supports both hard limits (reject when full) and soft limits (queue when
/// approaching capacity, reject at hard limit).
pub struct ConcurrencyLimiter {
    /// Current in-flight count.
    in_flight: AtomicU64,
    /// Hard limit: reject above this.
    hard_limit: u64,
    /// Soft limit: start queueing/degrading above this.
    soft_limit: u64,
    /// Total admitted (lifetime counter).
    total_admitted: AtomicU64,
    /// Total rejected (lifetime counter).
    total_rejected: AtomicU64,
    /// Total queued (lifetime counter).
    total_queued: AtomicU64,
}

impl ConcurrencyLimiter {
    pub fn new(hard_limit: u64, soft_limit: u64) -> Self {
        assert!(soft_limit <= hard_limit, "soft_limit must be ≤ hard_limit");
        Self {
            in_flight: AtomicU64::new(0),
            hard_limit,
            soft_limit,
            total_admitted: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            total_queued: AtomicU64::new(0),
        }
    }

    /// Try to acquire a permit. Returns Ok(guard) or Err with reason.
    pub fn try_acquire(&self) -> Result<ConcurrencyGuard<'_>, ConcurrencyReject> {
        let current = self.in_flight.fetch_add(1, Ordering::AcqRel);
        if current >= self.hard_limit {
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(ConcurrencyReject::HardLimitReached {
                current: current + 1,
                limit: self.hard_limit,
            });
        }
        self.total_admitted.fetch_add(1, Ordering::Relaxed);
        if current >= self.soft_limit {
            self.total_queued.fetch_add(1, Ordering::Relaxed);
        }
        Ok(ConcurrencyGuard { limiter: self })
    }

    /// Current in-flight count.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Whether the limiter is above the soft limit (approaching capacity).
    pub fn is_pressured(&self) -> bool {
        self.in_flight.load(Ordering::Relaxed) >= self.soft_limit
    }

    /// Snapshot of limiter stats for observability.
    pub fn stats(&self) -> ConcurrencyStats {
        ConcurrencyStats {
            in_flight: self.in_flight.load(Ordering::Relaxed),
            hard_limit: self.hard_limit,
            soft_limit: self.soft_limit,
            total_admitted: self.total_admitted.load(Ordering::Relaxed),
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
            total_queued: self.total_queued.load(Ordering::Relaxed),
        }
    }

    fn release(&self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// RAII guard: releases the concurrency permit on drop.
pub struct ConcurrencyGuard<'a> {
    limiter: &'a ConcurrencyLimiter,
}

impl<'a> Drop for ConcurrencyGuard<'a> {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

/// Reason for concurrency rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcurrencyReject {
    HardLimitReached { current: u64, limit: u64 },
}

impl std::fmt::Display for ConcurrencyReject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HardLimitReached { current, limit } => {
                write!(f, "concurrency hard limit reached ({}/{})", current, limit)
            }
        }
    }
}

/// Observable concurrency limiter stats.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConcurrencyStats {
    pub in_flight: u64,
    pub hard_limit: u64,
    pub soft_limit: u64,
    pub total_admitted: u64,
    pub total_rejected: u64,
    pub total_queued: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §5: Retry Framework
// ═══════════════════════════════════════════════════════════════════════════

/// Classification of a cross-shard transaction failure for retry decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureClass {
    /// Transient failure: safe to retry immediately.
    /// Examples: network blip, temporary unavailability.
    Transient,
    /// Conflict: safe to retry with backoff.
    /// Examples: OCC validation failure, write-write conflict.
    Conflict,
    /// Timeout: outcome may be indeterminate. Must check status before retry.
    /// Examples: coordinator timeout, commit phase timeout.
    Timeout,
    /// Permanent: do NOT retry.
    /// Examples: constraint violation, schema error, auth failure.
    Permanent,
}

impl FailureClass {
    /// Classify a FalconError into a FailureClass.
    pub fn classify(err: &falcon_common::error::FalconError) -> Self {
        use falcon_common::error::*;
        match err {
            // Conflicts → retry with backoff
            FalconError::Txn(TxnError::WriteConflict(_))
            | FalconError::Txn(TxnError::SerializationConflict(_))
            | FalconError::Storage(StorageError::SerializationFailure) => Self::Conflict,

            // Timeouts → indeterminate
            FalconError::Txn(TxnError::Timeout) => Self::Timeout,

            // Transient → retry immediately
            FalconError::Txn(TxnError::MemoryPressure(_))
            | FalconError::Txn(TxnError::WalBacklogExceeded(_))
            | FalconError::Txn(TxnError::ReplicationLagExceeded(_))
            | FalconError::Storage(StorageError::MemoryPressure { .. }) => Self::Transient,

            // Permanent → do not retry
            FalconError::Txn(TxnError::ConstraintViolation(_, _))
            | FalconError::Txn(TxnError::InvariantViolation(_, _))
            | FalconError::Storage(StorageError::DuplicateKey)
            | FalconError::Storage(StorageError::UniqueViolation { .. })
            | FalconError::Sql(_)
            | FalconError::ReadOnly(_) => Self::Permanent,

            // Default: treat as transient (conservative)
            _ => Self::Transient,
        }
    }

    /// Whether this failure class is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient | Self::Conflict)
    }
}

/// Retry configuration with exponential backoff + jitter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retries (0 = no retry).
    pub max_retries: u32,
    /// Initial backoff duration.
    pub initial_backoff: Duration,
    /// Maximum backoff duration (cap).
    pub max_backoff: Duration,
    /// Backoff multiplier (e.g. 2.0 for doubling).
    pub multiplier: f64,
    /// Jitter ratio (0.0–1.0): randomized fraction of backoff to add.
    pub jitter_ratio: f64,
    /// Maximum cumulative retry time budget.
    pub retry_budget: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(2),
            multiplier: 2.0,
            jitter_ratio: 0.25,
            retry_budget: Duration::from_secs(10),
        }
    }
}

impl RetryConfig {
    /// Compute the backoff duration for the nth retry attempt (0-indexed).
    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        let base = self.initial_backoff.as_micros() as f64 * self.multiplier.powi(attempt as i32);
        let base_us = base.min(self.max_backoff.as_micros() as f64) as u64;

        // Add jitter: uniform random in [0, jitter_ratio * base_us]
        let jitter_us = if self.jitter_ratio > 0.0 {
            // Deterministic pseudo-random based on attempt number for reproducibility
            let seed = attempt as u64 * 6364136223846793005 + 1442695040888963407;
            let frac = (seed % 10000) as f64 / 10000.0;
            (frac * self.jitter_ratio * base_us as f64) as u64
        } else {
            0
        };

        Duration::from_micros(base_us + jitter_us)
    }
}

/// Outcome of a retry attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryOutcome {
    /// Total attempts made (1 = no retries, original attempt only).
    pub attempts: u32,
    /// Total time spent in backoff.
    pub total_backoff: Duration,
    /// Classification of each failed attempt.
    pub failure_classes: Vec<FailureClass>,
    /// Whether the final attempt succeeded.
    pub succeeded: bool,
}

// ═══════════════════════════════════════════════════════════════════════════
// §5.2: Circuit Breaker (anti-thundering-herd)
// ═══════════════════════════════════════════════════════════════════════════

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitState {
    /// Normal operation: requests flow through.
    Closed,
    /// Tripped: all requests are rejected immediately.
    Open,
    /// Half-open: allow a probe request to test recovery.
    HalfOpen,
}

/// Per-shard or per-coordinator circuit breaker.
///
/// Trips (opens) after `failure_threshold` consecutive failures.
/// After `recovery_timeout`, transitions to HalfOpen and allows one probe.
/// If the probe succeeds, transitions back to Closed.
pub struct CircuitBreaker {
    state: Mutex<CircuitState>,
    consecutive_failures: AtomicU64,
    failure_threshold: u64,
    recovery_timeout: Duration,
    last_failure_time: Mutex<Option<Instant>>,
    /// Lifetime counters.
    total_trips: AtomicU64,
    total_rejects: AtomicU64,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u64, recovery_timeout: Duration) -> Self {
        Self {
            state: Mutex::new(CircuitState::Closed),
            consecutive_failures: AtomicU64::new(0),
            failure_threshold,
            recovery_timeout,
            last_failure_time: Mutex::new(None),
            total_trips: AtomicU64::new(0),
            total_rejects: AtomicU64::new(0),
        }
    }

    /// Check if a request should be allowed through.
    pub fn allow_request(&self) -> bool {
        let mut state = self.state.lock();
        match *state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if recovery timeout has elapsed
                let last = self.last_failure_time.lock();
                if let Some(t) = *last {
                    if t.elapsed() >= self.recovery_timeout {
                        *state = CircuitState::HalfOpen;
                        return true; // allow probe
                    }
                }
                self.total_rejects.fetch_add(1, Ordering::Relaxed);
                false
            }
            CircuitState::HalfOpen => {
                // Already allowing a probe; reject additional requests
                self.total_rejects.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Record a successful request.
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        let mut state = self.state.lock();
        if *state == CircuitState::HalfOpen {
            *state = CircuitState::Closed;
        }
    }

    /// Record a failed request.
    pub fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
        if failures >= self.failure_threshold {
            let mut state = self.state.lock();
            if *state == CircuitState::Closed || *state == CircuitState::HalfOpen {
                *state = CircuitState::Open;
                *self.last_failure_time.lock() = Some(Instant::now());
                self.total_trips.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Current state.
    pub fn state(&self) -> CircuitState {
        *self.state.lock()
    }

    /// Observable stats.
    pub fn stats(&self) -> CircuitBreakerStats {
        CircuitBreakerStats {
            state: self.state(),
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            failure_threshold: self.failure_threshold,
            total_trips: self.total_trips.load(Ordering::Relaxed),
            total_rejects: self.total_rejects.load(Ordering::Relaxed),
        }
    }
}

/// Observable circuit breaker stats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerStats {
    pub state: CircuitState,
    pub consecutive_failures: u64,
    pub failure_threshold: u64,
    pub total_trips: u64,
    pub total_rejects: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §4: Conflict / Abort Rate Tracker (per-shard)
// ═══════════════════════════════════════════════════════════════════════════

/// Tracks per-shard conflict and abort rates for adaptive concurrency control.
pub struct ShardConflictTracker {
    /// Per-shard counters: (shard_id → ShardConflictCounters).
    shards: RwLock<HashMap<u64, ShardConflictCounters>>,
    /// Window size for rate calculation.
    #[allow(dead_code)]
    window: Duration,
}

struct ShardConflictCounters {
    total_txns: AtomicU64,
    conflict_aborts: AtomicU64,
    timeout_aborts: AtomicU64,
    #[allow(dead_code)]
    other_aborts: AtomicU64,
    total_lock_wait_us: AtomicU64,
    #[allow(dead_code)]
    window_start: Mutex<Instant>,
}

impl ShardConflictCounters {
    fn new() -> Self {
        Self {
            total_txns: AtomicU64::new(0),
            conflict_aborts: AtomicU64::new(0),
            timeout_aborts: AtomicU64::new(0),
            other_aborts: AtomicU64::new(0),
            total_lock_wait_us: AtomicU64::new(0),
            window_start: Mutex::new(Instant::now()),
        }
    }
}

impl ShardConflictTracker {
    pub fn new(window: Duration) -> Self {
        Self {
            shards: RwLock::new(HashMap::new()),
            window,
        }
    }

    fn ensure_shard(&self, shard_id: u64) {
        let read = self.shards.read();
        if read.contains_key(&shard_id) {
            return;
        }
        drop(read);
        self.shards
            .write()
            .entry(shard_id)
            .or_insert_with(ShardConflictCounters::new);
    }

    /// Record a completed transaction on a shard.
    pub fn record_txn(&self, shard_id: u64) {
        self.ensure_shard(shard_id);
        let read = self.shards.read();
        if let Some(c) = read.get(&shard_id) {
            c.total_txns.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a conflict abort on a shard.
    pub fn record_conflict(&self, shard_id: u64) {
        self.ensure_shard(shard_id);
        let read = self.shards.read();
        if let Some(c) = read.get(&shard_id) {
            c.conflict_aborts.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a timeout abort on a shard.
    pub fn record_timeout(&self, shard_id: u64) {
        self.ensure_shard(shard_id);
        let read = self.shards.read();
        if let Some(c) = read.get(&shard_id) {
            c.timeout_aborts.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record lock wait time on a shard.
    pub fn record_lock_wait(&self, shard_id: u64, wait_us: u64) {
        self.ensure_shard(shard_id);
        let read = self.shards.read();
        if let Some(c) = read.get(&shard_id) {
            c.total_lock_wait_us.fetch_add(wait_us, Ordering::Relaxed);
        }
    }

    /// Get conflict rate snapshot for a shard.
    pub fn shard_snapshot(&self, shard_id: u64) -> ShardConflictSnapshot {
        let read = self.shards.read();
        match read.get(&shard_id) {
            Some(c) => {
                let total = c.total_txns.load(Ordering::Relaxed);
                let conflicts = c.conflict_aborts.load(Ordering::Relaxed);
                let timeouts = c.timeout_aborts.load(Ordering::Relaxed);
                ShardConflictSnapshot {
                    shard_id,
                    total_txns: total,
                    conflict_aborts: conflicts,
                    timeout_aborts: timeouts,
                    conflict_rate: if total > 0 {
                        conflicts as f64 / total as f64
                    } else {
                        0.0
                    },
                    abort_rate: if total > 0 {
                        (conflicts + timeouts) as f64 / total as f64
                    } else {
                        0.0
                    },
                    avg_lock_wait_us: if total > 0 {
                        c.total_lock_wait_us.load(Ordering::Relaxed) / total
                    } else {
                        0
                    },
                }
            }
            None => ShardConflictSnapshot {
                shard_id,
                ..Default::default()
            },
        }
    }

    /// Get snapshots for all tracked shards.
    pub fn all_snapshots(&self) -> Vec<ShardConflictSnapshot> {
        let shard_ids: Vec<u64> = self.shards.read().keys().copied().collect();
        shard_ids
            .iter()
            .map(|&sid| self.shard_snapshot(sid))
            .collect()
    }
}

/// Snapshot of per-shard conflict/abort rates.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShardConflictSnapshot {
    pub shard_id: u64,
    pub total_txns: u64,
    pub conflict_aborts: u64,
    pub timeout_aborts: u64,
    pub conflict_rate: f64,
    pub abort_rate: f64,
    pub avg_lock_wait_us: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §4.2: Adaptive Concurrency Controller
// ═══════════════════════════════════════════════════════════════════════════

/// Adjusts per-shard concurrency limits based on observed conflict rates.
///
/// - If conflict_rate > `scale_down_threshold`: reduce concurrency limit
/// - If conflict_rate < `scale_up_threshold`: increase concurrency limit
/// - Clamps between `min_concurrency` and `max_concurrency`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveConcurrencyConfig {
    /// Conflict rate above which we reduce concurrency.
    pub scale_down_threshold: f64,
    /// Conflict rate below which we increase concurrency.
    pub scale_up_threshold: f64,
    /// Minimum concurrency limit per shard.
    pub min_concurrency: u64,
    /// Maximum concurrency limit per shard.
    pub max_concurrency: u64,
    /// How many permits to adjust per step.
    pub step_size: u64,
    /// How often to re-evaluate (transactions between evaluations).
    pub eval_interval_txns: u64,
}

impl Default for AdaptiveConcurrencyConfig {
    fn default() -> Self {
        Self {
            scale_down_threshold: 0.15, // >15% conflict rate → reduce
            scale_up_threshold: 0.05,   // <5% conflict rate → increase
            min_concurrency: 4,
            max_concurrency: 256,
            step_size: 4,
            eval_interval_txns: 100,
        }
    }
}

impl AdaptiveConcurrencyConfig {
    /// Compute the adjusted concurrency limit given current limit and conflict rate.
    pub fn adjust(&self, current_limit: u64, conflict_rate: f64) -> u64 {
        if conflict_rate > self.scale_down_threshold {
            // Scale down
            current_limit
                .saturating_sub(self.step_size)
                .max(self.min_concurrency)
        } else if conflict_rate < self.scale_up_threshold {
            // Scale up
            (current_limit + self.step_size).min(self.max_concurrency)
        } else {
            // Stable
            current_limit
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §7: Deadlock Detection (timeout-based)
// ═══════════════════════════════════════════════════════════════════════════

/// A stalled transaction entry in the deadlock detector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StalledTxn {
    /// Transaction ID.
    pub txn_id: u64,
    /// Shards involved in this transaction.
    pub involved_shards: Vec<u64>,
    /// Where the txn is stuck.
    pub wait_reason: WaitReason,
    /// How long the txn has been waiting (microseconds).
    pub wait_duration_us: u64,
}

/// Reason why a cross-shard transaction is stalled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitReason {
    /// Waiting in coordinator queue for admission.
    CoordinatorQueue,
    /// Waiting for a shard lock.
    ShardLockWait(u64),
    /// Waiting for OCC validation.
    Validate,
    /// Waiting for WAL write.
    WalWrite,
    /// Waiting for replication ack.
    ReplicationAck,
    /// Waiting for commit barrier (other shards to prepare).
    CommitBarrier,
}

impl std::fmt::Display for WaitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CoordinatorQueue => write!(f, "coordinator_queue"),
            Self::ShardLockWait(s) => write!(f, "shard_lock_wait(shard_{})", s),
            Self::Validate => write!(f, "validate"),
            Self::WalWrite => write!(f, "wal_write"),
            Self::ReplicationAck => write!(f, "replication_ack"),
            Self::CommitBarrier => write!(f, "commit_barrier"),
        }
    }
}

/// Timeout-based deadlock detector.
///
/// Any cross-shard transaction that has been waiting longer than
/// `deadlock_timeout` is considered potentially deadlocked and will
/// be aborted with an explainable reason.
pub struct DeadlockDetector {
    /// Registered in-flight transactions.
    active: Mutex<HashMap<u64, ActiveTxnEntry>>,
    /// Timeout threshold: transactions waiting longer than this are aborted.
    deadlock_timeout: Duration,
    /// Total deadlocks detected (lifetime counter).
    total_deadlocks: AtomicU64,
}

struct ActiveTxnEntry {
    txn_id: u64,
    involved_shards: Vec<u64>,
    wait_reason: WaitReason,
    started_waiting: Instant,
}

impl DeadlockDetector {
    pub fn new(deadlock_timeout: Duration) -> Self {
        Self {
            active: Mutex::new(HashMap::new()),
            deadlock_timeout,
            total_deadlocks: AtomicU64::new(0),
        }
    }

    /// Register a transaction as waiting on a resource.
    pub fn register_wait(&self, txn_id: u64, involved_shards: Vec<u64>, reason: WaitReason) {
        let mut active = self.active.lock();
        active.insert(
            txn_id,
            ActiveTxnEntry {
                txn_id,
                involved_shards,
                wait_reason: reason,
                started_waiting: Instant::now(),
            },
        );
    }

    /// Unregister a transaction (it is no longer waiting).
    pub fn unregister(&self, txn_id: u64) {
        self.active.lock().remove(&txn_id);
    }

    /// Scan for stalled transactions that exceed the deadlock timeout.
    /// Returns the list of stalled transactions that should be aborted.
    pub fn detect_stalled(&self) -> Vec<StalledTxn> {
        let active = self.active.lock();
        let mut stalled = Vec::new();
        for entry in active.values() {
            let elapsed = entry.started_waiting.elapsed();
            if elapsed >= self.deadlock_timeout {
                self.total_deadlocks.fetch_add(1, Ordering::Relaxed);
                stalled.push(StalledTxn {
                    txn_id: entry.txn_id,
                    involved_shards: entry.involved_shards.clone(),
                    wait_reason: entry.wait_reason.clone(),
                    wait_duration_us: elapsed.as_micros() as u64,
                });
            }
        }
        stalled
    }

    /// Total deadlocks detected.
    pub fn total_deadlocks(&self) -> u64 {
        self.total_deadlocks.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3.2: Queue Policy
// ═══════════════════════════════════════════════════════════════════════════

/// Queue ordering policy for the coordinator admission queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum QueuePolicy {
    /// First-in, first-out.
    #[default]
    Fifo,
    /// Short transactions (fewer shards) get priority.
    ShortTxnPriority,
}

/// Action to take when the queue exceeds a configurable length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum QueueOverflowAction {
    /// Reject new requests.
    #[default]
    Reject,
    /// Shed load: drop lowest-priority requests.
    ShedLoad,
    /// Degrade: allow but with reduced guarantees (e.g. skip replication ack).
    Degrade,
}

// ═══════════════════════════════════════════════════════════════════════════
// Aggregated Cross-Shard Metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Aggregated cross-shard transaction metrics for SLO/SLA reporting.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CrossShardMetrics {
    /// Total cross-shard transactions attempted.
    pub total_attempted: u64,
    /// Total committed.
    pub total_committed: u64,
    /// Total aborted.
    pub total_aborted: u64,
    /// Abort rate breakdown by cause.
    pub aborts_by_conflict: u64,
    pub aborts_by_timeout: u64,
    pub aborts_by_admission: u64,
    pub aborts_by_permanent: u64,
    /// Retry stats.
    pub total_retries: u64,
    pub retry_successes: u64,
    /// Circuit breaker trips.
    pub circuit_breaker_trips: u64,
    /// Latency percentiles (microseconds).
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    /// Coordinator queue wait percentiles (microseconds).
    pub queue_p50_us: u64,
    pub queue_p99_us: u64,
    /// Throughput (txns/sec in the last window).
    pub throughput_tps: f64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §1.2: Enhanced 2PC Result
// ═══════════════════════════════════════════════════════════════════════════

/// Enhanced result from a hardened cross-shard 2PC transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossShardTxnResult {
    /// Whether the transaction committed.
    pub committed: bool,
    /// Number of shards that participated.
    pub shard_count: usize,
    /// Full latency breakdown.
    pub latency: CrossShardLatencyBreakdown,
    /// Retry outcome (if retries were attempted).
    pub retry: Option<RetryOutcome>,
    /// If aborted: why.
    pub abort_reason: Option<String>,
    /// If timed out: which phase.
    pub timeout_phase: Option<TimeoutPhase>,
    /// Failure classification.
    pub failure_class: Option<FailureClass>,
}

// ═══════════════════════════════════════════════════════════════════════════
// §8: Per-Shard Slow-Path Rate Limiter
// ═══════════════════════════════════════════════════════════════════════════

/// Per-shard inflight limiter for cross-shard (slow-path) transactions.
///
/// Each shard gets its own concurrency limit. When a shard's inflight count
/// reaches the limit, new cross-shard transactions targeting that shard are
/// rejected until inflight drops. This prevents a hot shard from starving others.
pub struct PerShardSlowPathLimiter {
    /// Per-shard inflight counters and limits.
    shards: RwLock<HashMap<u64, ShardSlowPathState>>,
    /// Default per-shard limit for newly seen shards.
    default_limit: u64,
    /// Lifetime stats.
    total_admitted: AtomicU64,
    total_rejected: AtomicU64,
    total_throttled: AtomicU64,
}

struct ShardSlowPathState {
    inflight: AtomicU64,
    limit: AtomicU64,
    total_txns: AtomicU64,
    total_rejected: AtomicU64,
}

impl ShardSlowPathState {
    fn new(limit: u64) -> Self {
        Self {
            inflight: AtomicU64::new(0),
            limit: AtomicU64::new(limit),
            total_txns: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
        }
    }
}

/// RAII guard for a per-shard slow-path permit.
pub struct ShardSlowPathGuard<'a> {
    limiter: &'a PerShardSlowPathLimiter,
    shard_id: u64,
}

impl<'a> std::fmt::Debug for ShardSlowPathGuard<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardSlowPathGuard")
            .field("shard_id", &self.shard_id)
            .finish()
    }
}

impl<'a> Drop for ShardSlowPathGuard<'a> {
    fn drop(&mut self) {
        self.limiter.release(self.shard_id);
    }
}

/// Per-shard slow-path stats snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShardSlowPathSnapshot {
    pub shard_id: u64,
    pub inflight: u64,
    pub limit: u64,
    pub total_txns: u64,
    pub total_rejected: u64,
}

/// Aggregate slow-path stats.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SlowPathStats {
    pub total_admitted: u64,
    pub total_rejected: u64,
    pub total_throttled: u64,
    pub per_shard: Vec<ShardSlowPathSnapshot>,
}

impl PerShardSlowPathLimiter {
    pub fn new(default_limit: u64) -> Self {
        Self {
            shards: RwLock::new(HashMap::new()),
            default_limit,
            total_admitted: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            total_throttled: AtomicU64::new(0),
        }
    }

    fn ensure_shard(&self, shard_id: u64) {
        let read = self.shards.read();
        if read.contains_key(&shard_id) {
            return;
        }
        drop(read);
        self.shards
            .write()
            .entry(shard_id)
            .or_insert_with(|| ShardSlowPathState::new(self.default_limit));
    }

    /// Try to acquire a slow-path permit for a set of target shards.
    /// Returns Ok(guards) if ALL shards admit, Err with the first rejected shard.
    pub fn try_acquire_all<'a>(
        &'a self,
        shard_ids: &[u64],
    ) -> Result<Vec<ShardSlowPathGuard<'a>>, SlowPathReject> {
        let mut guards = Vec::with_capacity(shard_ids.len());

        for &sid in shard_ids {
            match self.try_acquire_one(sid) {
                Ok(g) => guards.push(g),
                Err(reject) => {
                    // Release already acquired guards (they drop automatically)
                    drop(guards);
                    self.total_rejected.fetch_add(1, Ordering::Relaxed);
                    return Err(reject);
                }
            }
        }

        self.total_admitted.fetch_add(1, Ordering::Relaxed);
        Ok(guards)
    }

    fn try_acquire_one(&self, shard_id: u64) -> Result<ShardSlowPathGuard<'_>, SlowPathReject> {
        self.ensure_shard(shard_id);
        let read = self.shards.read();
        if let Some(state) = read.get(&shard_id) {
            let limit = state.limit.load(Ordering::Relaxed);
            let current = state.inflight.fetch_add(1, Ordering::AcqRel);
            if current >= limit {
                state.inflight.fetch_sub(1, Ordering::AcqRel);
                state.total_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(SlowPathReject {
                    shard_id,
                    inflight: current,
                    limit,
                });
            }
            state.total_txns.fetch_add(1, Ordering::Relaxed);
            Ok(ShardSlowPathGuard {
                limiter: self,
                shard_id,
            })
        } else {
            Err(SlowPathReject {
                shard_id,
                inflight: 0,
                limit: 0,
            })
        }
    }

    fn release(&self, shard_id: u64) {
        let read = self.shards.read();
        if let Some(state) = read.get(&shard_id) {
            state.inflight.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Dynamically adjust a shard's limit (e.g. from adaptive concurrency controller).
    pub fn set_shard_limit(&self, shard_id: u64, new_limit: u64) {
        self.ensure_shard(shard_id);
        let read = self.shards.read();
        if let Some(state) = read.get(&shard_id) {
            state.limit.store(new_limit, Ordering::Relaxed);
        }
    }

    /// Snapshot for a single shard.
    pub fn shard_snapshot(&self, shard_id: u64) -> ShardSlowPathSnapshot {
        let read = self.shards.read();
        match read.get(&shard_id) {
            Some(state) => ShardSlowPathSnapshot {
                shard_id,
                inflight: state.inflight.load(Ordering::Relaxed),
                limit: state.limit.load(Ordering::Relaxed),
                total_txns: state.total_txns.load(Ordering::Relaxed),
                total_rejected: state.total_rejected.load(Ordering::Relaxed),
            },
            None => ShardSlowPathSnapshot {
                shard_id,
                ..Default::default()
            },
        }
    }

    /// Aggregate stats across all shards.
    pub fn stats(&self) -> SlowPathStats {
        let read = self.shards.read();
        let per_shard: Vec<_> = read
            .keys()
            .map(|&sid| {
                let state = &read[&sid];
                ShardSlowPathSnapshot {
                    shard_id: sid,
                    inflight: state.inflight.load(Ordering::Relaxed),
                    limit: state.limit.load(Ordering::Relaxed),
                    total_txns: state.total_txns.load(Ordering::Relaxed),
                    total_rejected: state.total_rejected.load(Ordering::Relaxed),
                }
            })
            .collect();
        SlowPathStats {
            total_admitted: self.total_admitted.load(Ordering::Relaxed),
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
            total_throttled: self.total_throttled.load(Ordering::Relaxed),
            per_shard,
        }
    }
}

/// Rejection reason from the per-shard slow-path limiter.
#[derive(Debug, Clone)]
pub struct SlowPathReject {
    pub shard_id: u64,
    pub inflight: u64,
    pub limit: u64,
}

impl std::fmt::Display for SlowPathReject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shard {} slow-path limit reached ({}/{})",
            self.shard_id, self.inflight, self.limit
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §9: Latency Histogram (P50/P95/P99/P999 tracking)
// ═══════════════════════════════════════════════════════════════════════════

/// Fixed-bucket latency histogram for fast percentile queries.
///
/// Buckets are in microseconds: [0, 100), [100, 200), ..., [9900, 10000),
/// [10ms, 20ms), ..., [990ms, 1000ms), [1s+).
/// Total: 100 fine-grained buckets (0-10ms) + 99 coarse buckets (10ms-1s) + 1 overflow.
pub struct LatencyHistogram {
    /// Fine-grained buckets: 0-10ms in 100µs steps (100 buckets).
    fine: [AtomicU64; 100],
    /// Coarse buckets: 10ms-1s in 10ms steps (99 buckets).
    coarse: [AtomicU64; 99],
    /// Overflow: ≥1s.
    overflow: AtomicU64,
    /// Total observations.
    count: AtomicU64,
    /// Sum of all observations (µs).
    sum_us: AtomicU64,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self {
            fine: std::array::from_fn(|_| AtomicU64::new(0)),
            coarse: std::array::from_fn(|_| AtomicU64::new(0)),
            overflow: AtomicU64::new(0),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
        }
    }

    /// Record a latency observation in microseconds.
    pub fn observe(&self, latency_us: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(latency_us, Ordering::Relaxed);

        if latency_us < 10_000 {
            // Fine bucket: 0-10ms in 100µs steps
            let bucket = (latency_us / 100) as usize;
            let bucket = bucket.min(99);
            self.fine[bucket].fetch_add(1, Ordering::Relaxed);
        } else if latency_us < 1_000_000 {
            // Coarse bucket: 10ms-1s in 10ms steps
            let bucket = ((latency_us - 10_000) / 10_000) as usize;
            let bucket = bucket.min(98);
            self.coarse[bucket].fetch_add(1, Ordering::Relaxed);
        } else {
            self.overflow.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Compute percentile (0.0-1.0) in microseconds.
    pub fn percentile(&self, p: f64) -> u64 {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return 0;
        }
        let target = (total as f64 * p).ceil() as u64;
        let mut cumulative = 0u64;

        // Scan fine buckets
        for (i, bucket) in self.fine.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            if cumulative >= target {
                return (i as u64 + 1) * 100; // upper bound of bucket
            }
        }

        // Scan coarse buckets
        for (i, bucket) in self.coarse.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            if cumulative >= target {
                return 10_000 + (i as u64 + 1) * 10_000; // upper bound
            }
        }

        // Overflow
        1_000_000 // 1 second
    }

    /// Get P50/P95/P99/P999 in one call.
    pub fn percentiles(&self) -> LatencyPercentiles {
        LatencyPercentiles {
            p50_us: self.percentile(0.50),
            p95_us: self.percentile(0.95),
            p99_us: self.percentile(0.99),
            p999_us: self.percentile(0.999),
            count: self.count.load(Ordering::Relaxed),
            avg_us: {
                let c = self.count.load(Ordering::Relaxed);
                if c > 0 {
                    self.sum_us.load(Ordering::Relaxed) / c
                } else {
                    0
                }
            },
        }
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Percentile summary snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub count: u64,
    pub avg_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Latency Breakdown ──

    #[test]
    fn test_breakdown_dominant_phase() {
        let b = CrossShardLatencyBreakdown {
            prepare_wal_us: 5000,
            commit_wal_us: 1000,
            lock_validate_us: 200,
            ..Default::default()
        };
        assert_eq!(b.dominant_phase(), "prepare_wal");
    }

    #[test]
    fn test_breakdown_waterfall_format() {
        let b = CrossShardLatencyBreakdown {
            coord_queue_us: 100,
            route_plan_us: 50,
            total_us: 500,
            ..Default::default()
        };
        let w = b.waterfall();
        assert!(w.contains("queue=100"));
        assert!(w.contains("route=50"));
        assert!(w.contains("total=500"));
    }

    #[test]
    fn test_breakdown_max_shard_rpc() {
        let b = CrossShardLatencyBreakdown {
            shard_rpc_us: vec![(0, 100), (1, 500), (2, 200)],
            ..Default::default()
        };
        assert_eq!(b.max_shard_rpc_us(), 500);
    }

    // ── Layered Timeouts ──

    #[test]
    fn test_timeout_phase_display() {
        assert_eq!(
            TimeoutPhase::CoordinatorQueue.to_string(),
            "coordinator_queue"
        );
        assert_eq!(
            TimeoutPhase::ShardRpc(ShardId(3)).to_string(),
            "shard_rpc(3)"
        );
        assert_eq!(
            TimeoutPhase::Prepare(ShardId(1)).to_string(),
            "prepare(shard_1)"
        );
        assert_eq!(TimeoutPhase::Total.to_string(), "total");
    }

    // ── Concurrency Limiter ──

    #[test]
    fn test_concurrency_limiter_admit() {
        let limiter = ConcurrencyLimiter::new(10, 5);
        let _g1 = limiter.try_acquire().unwrap();
        let _g2 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.in_flight(), 2);
        assert!(!limiter.is_pressured());
    }

    #[test]
    fn test_concurrency_limiter_soft_limit() {
        let limiter = ConcurrencyLimiter::new(10, 2);
        let _g1 = limiter.try_acquire().unwrap(); // previous=0, in_flight→1
        let _g2 = limiter.try_acquire().unwrap(); // previous=1, in_flight→2
        assert!(limiter.is_pressured()); // in_flight(2) >= soft(2)
        let _g3 = limiter.try_acquire().unwrap(); // previous=2 >= soft(2) → queued
        let stats = limiter.stats();
        assert_eq!(stats.total_queued, 1); // g3 was at/above soft limit
    }

    #[test]
    fn test_concurrency_limiter_hard_limit() {
        let limiter = ConcurrencyLimiter::new(2, 1);
        let _g1 = limiter.try_acquire().unwrap();
        let _g2 = limiter.try_acquire().unwrap();
        let result = limiter.try_acquire();
        assert!(result.is_err());
        let stats = limiter.stats();
        assert_eq!(stats.total_rejected, 1);
    }

    #[test]
    fn test_concurrency_limiter_release() {
        let limiter = ConcurrencyLimiter::new(2, 1);
        {
            let _g1 = limiter.try_acquire().unwrap();
            let _g2 = limiter.try_acquire().unwrap();
            assert_eq!(limiter.in_flight(), 2);
        }
        // Guards dropped
        assert_eq!(limiter.in_flight(), 0);
        // Can acquire again
        let _g3 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.in_flight(), 1);
    }

    // ── Retry Framework ──

    #[test]
    fn test_failure_classification() {
        use falcon_common::error::*;
        use falcon_common::types::TxnId;

        let conflict = FalconError::Txn(TxnError::WriteConflict(TxnId(1)));
        assert_eq!(FailureClass::classify(&conflict), FailureClass::Conflict);
        assert!(FailureClass::Conflict.is_retryable());

        let timeout = FalconError::Txn(TxnError::Timeout);
        assert_eq!(FailureClass::classify(&timeout), FailureClass::Timeout);
        assert!(!FailureClass::Timeout.is_retryable());

        let permanent = FalconError::Storage(StorageError::DuplicateKey);
        assert_eq!(FailureClass::classify(&permanent), FailureClass::Permanent);
        assert!(!FailureClass::Permanent.is_retryable());

        let transient = FalconError::Txn(TxnError::WalBacklogExceeded(TxnId(2)));
        assert_eq!(FailureClass::classify(&transient), FailureClass::Transient);
        assert!(FailureClass::Transient.is_retryable());
    }

    #[test]
    fn test_retry_backoff_exponential() {
        let cfg = RetryConfig {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(2),
            multiplier: 2.0,
            jitter_ratio: 0.0, // no jitter for deterministic test
            ..Default::default()
        };
        let b0 = cfg.backoff_for_attempt(0);
        let b1 = cfg.backoff_for_attempt(1);
        let b2 = cfg.backoff_for_attempt(2);
        assert_eq!(b0, Duration::from_millis(10));
        assert_eq!(b1, Duration::from_millis(20));
        assert_eq!(b2, Duration::from_millis(40));
    }

    #[test]
    fn test_retry_backoff_capped() {
        let cfg = RetryConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(500),
            multiplier: 10.0,
            jitter_ratio: 0.0,
            ..Default::default()
        };
        let b0 = cfg.backoff_for_attempt(0);
        let b1 = cfg.backoff_for_attempt(1);
        let b2 = cfg.backoff_for_attempt(2);
        assert_eq!(b0, Duration::from_millis(100));
        assert_eq!(b1, Duration::from_millis(500)); // capped
        assert_eq!(b2, Duration::from_millis(500)); // still capped
    }

    #[test]
    fn test_retry_backoff_with_jitter() {
        let cfg = RetryConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
            multiplier: 2.0,
            jitter_ratio: 0.25,
            ..Default::default()
        };
        let b0 = cfg.backoff_for_attempt(0);
        // With jitter, backoff should be >= base and <= base * (1 + jitter_ratio)
        assert!(b0 >= Duration::from_millis(100));
        assert!(b0 <= Duration::from_millis(125));
    }

    // ── Circuit Breaker ──

    #[test]
    fn test_circuit_breaker_closed_by_default() {
        let cb = CircuitBreaker::new(3, Duration::from_millis(100));
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_trips_after_threshold() {
        let cb = CircuitBreaker::new(3, Duration::from_millis(100));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure(); // 3rd failure → trip
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());
        assert_eq!(cb.stats().total_trips, 1);
    }

    #[test]
    fn test_circuit_breaker_recovery() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(10));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait for recovery timeout
        std::thread::sleep(Duration::from_millis(15));
        assert!(cb.allow_request()); // transitions to HalfOpen
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        cb.record_success(); // probe succeeded → Closed
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_success_resets_failures() {
        let cb = CircuitBreaker::new(3, Duration::from_millis(100));
        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // resets counter
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed); // still closed (only 2 consecutive)
    }

    // ── Conflict Tracker ──

    #[test]
    fn test_conflict_tracker_rates() {
        let tracker = ShardConflictTracker::new(Duration::from_secs(60));
        for _ in 0..10 {
            tracker.record_txn(0);
        }
        for _ in 0..3 {
            tracker.record_conflict(0);
        }
        tracker.record_timeout(0);

        let snap = tracker.shard_snapshot(0);
        assert_eq!(snap.total_txns, 10);
        assert_eq!(snap.conflict_aborts, 3);
        assert_eq!(snap.timeout_aborts, 1);
        assert!((snap.conflict_rate - 0.3).abs() < 0.001);
        assert!((snap.abort_rate - 0.4).abs() < 0.001);
    }

    // ── Adaptive Concurrency ──

    #[test]
    fn test_adaptive_concurrency_scale_down() {
        let cfg = AdaptiveConcurrencyConfig::default();
        // High conflict rate → scale down
        let new = cfg.adjust(64, 0.25);
        assert_eq!(new, 60);
    }

    #[test]
    fn test_adaptive_concurrency_scale_up() {
        let cfg = AdaptiveConcurrencyConfig::default();
        // Low conflict rate → scale up
        let new = cfg.adjust(64, 0.02);
        assert_eq!(new, 68);
    }

    #[test]
    fn test_adaptive_concurrency_stable() {
        let cfg = AdaptiveConcurrencyConfig::default();
        // Moderate conflict rate → no change
        let new = cfg.adjust(64, 0.10);
        assert_eq!(new, 64);
    }

    #[test]
    fn test_adaptive_concurrency_clamp_min() {
        let cfg = AdaptiveConcurrencyConfig {
            min_concurrency: 4,
            step_size: 10,
            ..Default::default()
        };
        let new = cfg.adjust(8, 0.50);
        assert_eq!(new, 4); // clamped to min
    }

    #[test]
    fn test_adaptive_concurrency_clamp_max() {
        let cfg = AdaptiveConcurrencyConfig {
            max_concurrency: 100,
            step_size: 10,
            ..Default::default()
        };
        let new = cfg.adjust(98, 0.01);
        assert_eq!(new, 100); // clamped to max
    }

    // ── Deadlock Detector ──

    #[test]
    fn test_deadlock_detector_no_stall() {
        let dd = DeadlockDetector::new(Duration::from_millis(50));
        dd.register_wait(1, vec![0, 1], WaitReason::ShardLockWait(0));
        let stalled = dd.detect_stalled();
        assert!(stalled.is_empty(), "should not detect stall immediately");
        dd.unregister(1);
    }

    #[test]
    fn test_deadlock_detector_detects_stall() {
        let dd = DeadlockDetector::new(Duration::from_millis(10));
        dd.register_wait(42, vec![0, 1, 2], WaitReason::CommitBarrier);
        std::thread::sleep(Duration::from_millis(15));
        let stalled = dd.detect_stalled();
        assert_eq!(stalled.len(), 1);
        assert_eq!(stalled[0].txn_id, 42);
        assert_eq!(stalled[0].involved_shards, vec![0, 1, 2]);
        assert_eq!(stalled[0].wait_reason, WaitReason::CommitBarrier);
        assert!(stalled[0].wait_duration_us >= 10_000);
        assert_eq!(dd.total_deadlocks(), 1);
    }

    #[test]
    fn test_deadlock_detector_unregister_clears() {
        let dd = DeadlockDetector::new(Duration::from_millis(10));
        dd.register_wait(1, vec![0], WaitReason::WalWrite);
        dd.unregister(1);
        std::thread::sleep(Duration::from_millis(15));
        let stalled = dd.detect_stalled();
        assert!(stalled.is_empty());
    }

    // ── Queue Policy ──

    #[test]
    fn test_queue_policy_defaults() {
        assert_eq!(QueuePolicy::default(), QueuePolicy::Fifo);
        assert_eq!(QueueOverflowAction::default(), QueueOverflowAction::Reject);
    }

    // ── Wait Reason Display ──

    #[test]
    fn test_wait_reason_display() {
        assert_eq!(
            WaitReason::CoordinatorQueue.to_string(),
            "coordinator_queue"
        );
        assert_eq!(
            WaitReason::ShardLockWait(3).to_string(),
            "shard_lock_wait(shard_3)"
        );
        assert_eq!(WaitReason::CommitBarrier.to_string(), "commit_barrier");
    }

    // ── Per-Shard Slow-Path Limiter ──

    #[test]
    fn test_slow_path_limiter_admit() {
        let limiter = PerShardSlowPathLimiter::new(4);
        let guards = limiter.try_acquire_all(&[0, 1, 2]).unwrap();
        assert_eq!(guards.len(), 3);
        let snap0 = limiter.shard_snapshot(0);
        assert_eq!(snap0.inflight, 1);
        assert_eq!(snap0.limit, 4);
        assert_eq!(snap0.total_txns, 1);
    }

    #[test]
    fn test_slow_path_limiter_reject_at_limit() {
        let limiter = PerShardSlowPathLimiter::new(2);
        let _g1 = limiter.try_acquire_all(&[0]).unwrap();
        let _g2 = limiter.try_acquire_all(&[0]).unwrap();
        // Shard 0 is now at limit (2 inflight)
        let result = limiter.try_acquire_all(&[0]);
        assert!(result.is_err());
        let reject = result.unwrap_err();
        assert_eq!(reject.shard_id, 0);
        assert_eq!(reject.limit, 2);
    }

    #[test]
    fn test_slow_path_limiter_release_on_drop() {
        let limiter = PerShardSlowPathLimiter::new(1);
        {
            let _g = limiter.try_acquire_all(&[5]).unwrap();
            assert_eq!(limiter.shard_snapshot(5).inflight, 1);
        }
        // Guard dropped — inflight should be 0
        assert_eq!(limiter.shard_snapshot(5).inflight, 0);
        // Can acquire again
        let _g2 = limiter.try_acquire_all(&[5]).unwrap();
    }

    #[test]
    fn test_slow_path_limiter_multi_shard_rollback() {
        let limiter = PerShardSlowPathLimiter::new(1);
        let _g1 = limiter.try_acquire_all(&[10]).unwrap();
        // Shard 10 is full. Acquiring [20, 10] should fail on shard 10
        // and release the guard for shard 20.
        let result = limiter.try_acquire_all(&[20, 10]);
        assert!(result.is_err());
        // Shard 20 should have 0 inflight (guard was dropped on rollback)
        assert_eq!(limiter.shard_snapshot(20).inflight, 0);
    }

    #[test]
    fn test_slow_path_limiter_dynamic_limit() {
        let limiter = PerShardSlowPathLimiter::new(1);
        let _g = limiter.try_acquire_all(&[0]).unwrap();
        // At limit, should reject
        assert!(limiter.try_acquire_all(&[0]).is_err());
        // Raise limit
        limiter.set_shard_limit(0, 5);
        let _g2 = limiter.try_acquire_all(&[0]).unwrap();
        assert_eq!(limiter.shard_snapshot(0).inflight, 2);
    }

    #[test]
    fn test_slow_path_limiter_stats() {
        let limiter = PerShardSlowPathLimiter::new(2);
        let _g = limiter.try_acquire_all(&[0, 1]).unwrap();
        let _g2 = limiter.try_acquire_all(&[0]).unwrap();
        let _ = limiter.try_acquire_all(&[0]); // rejected
        let stats = limiter.stats();
        assert_eq!(stats.total_admitted, 2);
        assert_eq!(stats.total_rejected, 1);
    }

    // ── Latency Histogram ──

    #[test]
    fn test_histogram_empty() {
        let h = LatencyHistogram::new();
        let p = h.percentiles();
        assert_eq!(p.count, 0);
        assert_eq!(p.p50_us, 0);
        assert_eq!(p.p99_us, 0);
    }

    #[test]
    fn test_histogram_single_observation() {
        let h = LatencyHistogram::new();
        h.observe(500); // 500µs
        let p = h.percentiles();
        assert_eq!(p.count, 1);
        assert_eq!(p.avg_us, 500);
        // Single observation: all percentiles should be the same bucket
        assert!(p.p50_us >= 500);
        assert!(p.p99_us >= 500);
    }

    #[test]
    fn test_histogram_percentiles_ordering() {
        let h = LatencyHistogram::new();
        // Uniform distribution from 100µs to 10000µs
        for i in 1..=100 {
            h.observe(i * 100);
        }
        let p = h.percentiles();
        assert_eq!(p.count, 100);
        assert!(
            p.p50_us <= p.p95_us,
            "p50 ({}) <= p95 ({})",
            p.p50_us,
            p.p95_us
        );
        assert!(
            p.p95_us <= p.p99_us,
            "p95 ({}) <= p99 ({})",
            p.p95_us,
            p.p99_us
        );
        assert!(
            p.p99_us <= p.p999_us,
            "p99 ({}) <= p999 ({})",
            p.p99_us,
            p.p999_us
        );
    }

    #[test]
    fn test_histogram_coarse_buckets() {
        let h = LatencyHistogram::new();
        h.observe(50_000); // 50ms — coarse bucket
        h.observe(500_000); // 500ms — coarse bucket
        let p = h.percentiles();
        assert_eq!(p.count, 2);
        assert!(p.p50_us >= 50_000);
    }

    #[test]
    fn test_histogram_overflow() {
        let h = LatencyHistogram::new();
        h.observe(2_000_000); // 2 seconds — overflow
        let p = h.percentiles();
        assert_eq!(p.count, 1);
        assert_eq!(p.p50_us, 1_000_000); // overflow returns 1s
    }

    #[test]
    fn test_slow_path_reject_display() {
        let r = SlowPathReject {
            shard_id: 3,
            inflight: 10,
            limit: 10,
        };
        assert!(r.to_string().contains("shard 3"));
        assert!(r.to_string().contains("10/10"));
    }
}
