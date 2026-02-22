//! Shard circuit breaker — prevents cascading failures when a shard is unhealthy.
//!
//! State machine: Closed → Open → HalfOpen → Closed
//!
//! - **Closed**: normal operation; failures are counted.
//! - **Open**: shard is considered down; all requests return `Retryable` immediately.
//! - **HalfOpen**: after `recovery_timeout`, one probe request is allowed through.
//!   - Success → Closed (reset counters)
//!   - Failure → Open (restart timer)
//!
//! Each shard gets its own `ShardCircuitBreaker`. A `ClusterCircuitBreaker`
//! manages one per shard and is the primary entry point.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use falcon_common::error::{FalconError, FalconResult};
use falcon_common::types::ShardId;

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    /// Normal operation.
    Closed,
    /// Shard is considered down; requests are rejected immediately.
    Open,
    /// One probe request is allowed through to test recovery.
    HalfOpen,
}

impl std::fmt::Display for BreakerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BreakerState::Closed => write!(f, "closed"),
            BreakerState::Open => write!(f, "open"),
            BreakerState::HalfOpen => write!(f, "half_open"),
        }
    }
}

/// Configuration for a circuit breaker.
#[derive(Debug, Clone)]
pub struct BreakerConfig {
    /// Number of consecutive failures before opening.
    pub failure_threshold: u32,
    /// Number of consecutive successes in HalfOpen before closing.
    pub success_threshold: u32,
    /// How long to stay Open before transitioning to HalfOpen.
    pub recovery_timeout: Duration,
    /// Minimum number of requests in a window before evaluating failure rate.
    pub min_requests: u32,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            recovery_timeout: Duration::from_secs(10),
            min_requests: 3,
        }
    }
}

/// Metrics snapshot for a single circuit breaker.
#[derive(Debug, Clone, Default)]
pub struct BreakerMetrics {
    pub state: String,
    pub consecutive_failures: u32,
    pub consecutive_successes: u32,
    pub total_failures: u64,
    pub total_successes: u64,
    pub total_rejected: u64,
    pub state_changes: u64,
    pub last_failure_ms_ago: Option<u64>,
}

struct BreakerInner {
    state: BreakerState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    opened_at: Option<Instant>,
    last_failure_at: Option<Instant>,
    state_changes: u64,
}

/// Per-shard circuit breaker.
pub struct ShardCircuitBreaker {
    shard_id: ShardId,
    config: BreakerConfig,
    inner: Mutex<BreakerInner>,
    total_failures: AtomicU64,
    total_successes: AtomicU64,
    total_rejected: AtomicU64,
}

impl ShardCircuitBreaker {
    pub fn new(shard_id: ShardId, config: BreakerConfig) -> Arc<Self> {
        Arc::new(Self {
            shard_id,
            config,
            inner: Mutex::new(BreakerInner {
                state: BreakerState::Closed,
                consecutive_failures: 0,
                consecutive_successes: 0,
                opened_at: None,
                last_failure_at: None,
                state_changes: 0,
            }),
            total_failures: AtomicU64::new(0),
            total_successes: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
        })
    }

    /// Check whether a request is allowed through.
    /// Returns `Ok(())` if allowed, `Err(Retryable)` if the breaker is open.
    pub fn check(&self) -> FalconResult<()> {
        let mut inner = self.inner.lock();
        match inner.state {
            BreakerState::Closed => Ok(()),
            BreakerState::Open => {
                // Check if recovery timeout has elapsed → transition to HalfOpen
                if let Some(opened_at) = inner.opened_at {
                    if opened_at.elapsed() >= self.config.recovery_timeout {
                        inner.state = BreakerState::HalfOpen;
                        inner.consecutive_successes = 0;
                        inner.state_changes += 1;
                        tracing::info!(
                            shard_id = self.shard_id.0,
                            "circuit breaker: Open → HalfOpen (probe allowed)"
                        );
                        return Ok(());
                    }
                }
                self.total_rejected.fetch_add(1, Ordering::Relaxed);
                Err(FalconError::Retryable {
                    reason: format!(
                        "shard {} circuit breaker is open (too many failures)",
                        self.shard_id.0
                    ),
                    shard_id: self.shard_id.0,
                    epoch: 0,
                    leader_hint: None,
                    retry_after_ms: self.config.recovery_timeout.as_millis().min(5000) as u64,
                })
            }
            BreakerState::HalfOpen => {
                // Only one probe at a time; subsequent requests are rejected
                self.total_rejected.fetch_add(1, Ordering::Relaxed);
                Err(FalconError::Retryable {
                    reason: format!(
                        "shard {} circuit breaker is half-open (probe in progress)",
                        self.shard_id.0
                    ),
                    shard_id: self.shard_id.0,
                    epoch: 0,
                    leader_hint: None,
                    retry_after_ms: 500,
                })
            }
        }
    }

    /// Record a successful request outcome.
    pub fn record_success(&self) {
        self.total_successes.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.inner.lock();
        inner.consecutive_failures = 0;
        match inner.state {
            BreakerState::HalfOpen => {
                inner.consecutive_successes += 1;
                if inner.consecutive_successes >= self.config.success_threshold {
                    inner.state = BreakerState::Closed;
                    inner.opened_at = None;
                    inner.state_changes += 1;
                    tracing::info!(
                        shard_id = self.shard_id.0,
                        "circuit breaker: HalfOpen → Closed (recovered)"
                    );
                }
            }
            BreakerState::Closed => {
                inner.consecutive_successes += 1;
            }
            BreakerState::Open => {}
        }
    }

    /// Record a failed request outcome.
    pub fn record_failure(&self) {
        self.total_failures.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.inner.lock();
        inner.consecutive_failures += 1;
        inner.consecutive_successes = 0;
        inner.last_failure_at = Some(Instant::now());

        match inner.state {
            BreakerState::Closed => {
                if inner.consecutive_failures >= self.config.failure_threshold {
                    inner.state = BreakerState::Open;
                    inner.opened_at = Some(Instant::now());
                    inner.state_changes += 1;
                    tracing::warn!(
                        shard_id = self.shard_id.0,
                        consecutive_failures = inner.consecutive_failures,
                        "circuit breaker: Closed → Open (failure threshold reached)"
                    );
                }
            }
            BreakerState::HalfOpen => {
                // Probe failed → back to Open
                inner.state = BreakerState::Open;
                inner.opened_at = Some(Instant::now());
                inner.consecutive_successes = 0;
                inner.state_changes += 1;
                tracing::warn!(
                    shard_id = self.shard_id.0,
                    "circuit breaker: HalfOpen → Open (probe failed)"
                );
            }
            BreakerState::Open => {}
        }
    }

    /// Force-reset the breaker to Closed (for manual recovery / testing).
    pub fn force_close(&self) {
        let mut inner = self.inner.lock();
        inner.state = BreakerState::Closed;
        inner.consecutive_failures = 0;
        inner.consecutive_successes = 0;
        inner.opened_at = None;
        inner.state_changes += 1;
        tracing::info!(shard_id = self.shard_id.0, "circuit breaker: force-closed");
    }

    /// Current state.
    pub fn state(&self) -> BreakerState {
        self.inner.lock().state
    }

    /// Metrics snapshot.
    pub fn metrics(&self) -> BreakerMetrics {
        let inner = self.inner.lock();
        BreakerMetrics {
            state: inner.state.to_string(),
            consecutive_failures: inner.consecutive_failures,
            consecutive_successes: inner.consecutive_successes,
            total_failures: self.total_failures.load(Ordering::Relaxed),
            total_successes: self.total_successes.load(Ordering::Relaxed),
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
            state_changes: inner.state_changes,
            last_failure_ms_ago: inner
                .last_failure_at
                .map(|t| t.elapsed().as_millis() as u64),
        }
    }
}

/// Cluster-wide circuit breaker registry — one `ShardCircuitBreaker` per shard.
pub struct ClusterCircuitBreaker {
    breakers: Mutex<HashMap<ShardId, Arc<ShardCircuitBreaker>>>,
    config: BreakerConfig,
}

impl ClusterCircuitBreaker {
    pub fn new(config: BreakerConfig) -> Arc<Self> {
        Arc::new(Self {
            breakers: Mutex::new(HashMap::new()),
            config,
        })
    }

    pub fn new_default() -> Arc<Self> {
        Self::new(BreakerConfig::default())
    }

    /// Get or create the breaker for a shard.
    pub fn for_shard(&self, shard_id: ShardId) -> Arc<ShardCircuitBreaker> {
        let mut breakers = self.breakers.lock();
        breakers
            .entry(shard_id)
            .or_insert_with(|| ShardCircuitBreaker::new(shard_id, self.config.clone()))
            .clone()
    }

    /// Check whether a request to a shard is allowed.
    pub fn check(&self, shard_id: ShardId) -> FalconResult<()> {
        self.for_shard(shard_id).check()
    }

    /// Record success for a shard.
    pub fn record_success(&self, shard_id: ShardId) {
        self.for_shard(shard_id).record_success();
    }

    /// Record failure for a shard.
    pub fn record_failure(&self, shard_id: ShardId) {
        self.for_shard(shard_id).record_failure();
    }

    /// Metrics for all shards.
    pub fn all_metrics(&self) -> HashMap<u64, BreakerMetrics> {
        self.breakers
            .lock()
            .iter()
            .map(|(sid, b)| (sid.0, b.metrics()))
            .collect()
    }

    /// Number of shards currently in Open state.
    pub fn open_shard_count(&self) -> usize {
        self.breakers
            .lock()
            .values()
            .filter(|b| b.state() == BreakerState::Open)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn fast_config() -> BreakerConfig {
        BreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            recovery_timeout: Duration::from_millis(50),
            min_requests: 1,
        }
    }

    fn make_breaker() -> Arc<ShardCircuitBreaker> {
        ShardCircuitBreaker::new(ShardId(1), fast_config())
    }

    #[test]
    fn test_closed_allows_requests() {
        let b = make_breaker();
        assert_eq!(b.state(), BreakerState::Closed);
        assert!(b.check().is_ok());
    }

    #[test]
    fn test_opens_after_threshold_failures() {
        let b = make_breaker();
        b.record_failure();
        b.record_failure();
        assert_eq!(b.state(), BreakerState::Closed);
        b.record_failure(); // threshold = 3
        assert_eq!(b.state(), BreakerState::Open);
    }

    #[test]
    fn test_open_rejects_requests() {
        let b = make_breaker();
        for _ in 0..3 {
            b.record_failure();
        }
        let err = b.check().unwrap_err();
        assert!(err.is_retryable());
        assert!(err.to_string().contains("circuit breaker is open"));
        assert_eq!(b.metrics().total_rejected, 1);
    }

    #[test]
    fn test_open_to_halfopen_after_timeout() {
        let b = make_breaker();
        for _ in 0..3 {
            b.record_failure();
        }
        assert_eq!(b.state(), BreakerState::Open);
        thread::sleep(Duration::from_millis(60));
        assert!(b.check().is_ok()); // probe allowed
        assert_eq!(b.state(), BreakerState::HalfOpen);
    }

    #[test]
    fn test_halfopen_success_closes() {
        let b = make_breaker();
        for _ in 0..3 {
            b.record_failure();
        }
        thread::sleep(Duration::from_millis(60));
        b.check().unwrap(); // probe
        b.record_success();
        b.record_success(); // success_threshold = 2
        assert_eq!(b.state(), BreakerState::Closed);
    }

    #[test]
    fn test_halfopen_failure_reopens() {
        let b = make_breaker();
        for _ in 0..3 {
            b.record_failure();
        }
        thread::sleep(Duration::from_millis(60));
        b.check().unwrap(); // probe
        b.record_failure();
        assert_eq!(b.state(), BreakerState::Open);
    }

    #[test]
    fn test_success_resets_failure_counter() {
        let b = make_breaker();
        b.record_failure();
        b.record_failure();
        b.record_success(); // resets consecutive_failures
        b.record_failure();
        b.record_failure();
        // Only 2 consecutive failures after reset, not 3 → still Closed
        assert_eq!(b.state(), BreakerState::Closed);
    }

    #[test]
    fn test_force_close() {
        let b = make_breaker();
        for _ in 0..3 {
            b.record_failure();
        }
        assert_eq!(b.state(), BreakerState::Open);
        b.force_close();
        assert_eq!(b.state(), BreakerState::Closed);
        assert!(b.check().is_ok());
    }

    #[test]
    fn test_metrics_snapshot() {
        let b = make_breaker();
        b.record_success();
        b.record_failure();
        b.record_failure();
        let m = b.metrics();
        assert_eq!(m.total_successes, 1);
        assert_eq!(m.total_failures, 2);
        assert_eq!(m.consecutive_failures, 2);
        assert_eq!(m.state, "closed");
    }

    #[test]
    fn test_cluster_circuit_breaker_per_shard() {
        let cluster = ClusterCircuitBreaker::new_default();
        let s0 = ShardId(0);
        let s1 = ShardId(1);
        assert!(cluster.check(s0).is_ok());
        assert!(cluster.check(s1).is_ok());
        // Fail shard 0 to threshold
        for _ in 0..5 {
            cluster.record_failure(s0);
        }
        assert!(cluster.check(s0).is_err());
        assert!(cluster.check(s1).is_ok()); // shard 1 unaffected
        assert_eq!(cluster.open_shard_count(), 1);
    }

    #[test]
    fn test_cluster_metrics_all_shards() {
        let cluster = ClusterCircuitBreaker::new_default();
        cluster.record_success(ShardId(0));
        cluster.record_failure(ShardId(1));
        let m = cluster.all_metrics();
        assert!(m.contains_key(&0));
        assert!(m.contains_key(&1));
    }

    #[test]
    fn test_state_changes_counted() {
        let b = make_breaker();
        for _ in 0..3 {
            b.record_failure();
        } // Closed → Open
        thread::sleep(Duration::from_millis(60));
        b.check().unwrap(); // Open → HalfOpen
        b.record_success();
        b.record_success(); // HalfOpen → Closed
        let m = b.metrics();
        assert_eq!(m.state_changes, 3);
    }
}
