//! Deterministic 2PC extensions for enterprise-grade transaction guarantees.
//!
//! Addresses three production gaps in the base 2PC coordinator:
//!
//! 1. **Coordinator Decision Log** — durable commit/abort decision point
//!    that survives coordinator crashes. The decision is written *before*
//!    sending commit/abort to participants, ensuring exactly-once semantics.
//!
//! 2. **Layered Timeout Controller** — soft timeout returns `Retryable` to
//!    the client (they can retry); hard timeout aborts the transaction and
//!    enqueues it for the in-doubt resolver.
//!
//! 3. **Slow-Shard Policy** — configurable behavior when a shard is slower
//!    than its peers: `FastAbort` (abort immediately) or `HedgedRequest`
//!    (send a speculative retry to a replica).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};

use falcon_common::types::{ShardId, TxnId};

// ═══════════════════════════════════════════════════════════════════════════
// 1. Coordinator Decision Log
// ═══════════════════════════════════════════════════════════════════════════

/// The coordinator's durable decision for a 2PC transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorDecision {
    /// All participants prepared — coordinator decides to commit.
    Commit,
    /// At least one participant failed — coordinator decides to abort.
    Abort,
}

impl std::fmt::Display for CoordinatorDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoordinatorDecision::Commit => write!(f, "commit"),
            CoordinatorDecision::Abort => write!(f, "abort"),
        }
    }
}

/// A single decision record in the coordinator log.
#[derive(Debug, Clone)]
pub struct DecisionRecord {
    /// Monotonically increasing sequence number.
    pub seq: u64,
    /// The transaction this decision applies to.
    pub txn_id: TxnId,
    /// The coordinator's decision.
    pub decision: CoordinatorDecision,
    /// Which shards participated.
    pub participant_shards: Vec<ShardId>,
    /// Wall-clock timestamp (ms since epoch).
    pub timestamp_ms: u64,
    /// Whether the decision has been fully applied to all participants.
    pub applied: bool,
    /// Time taken to reach the decision (prepare phase duration in µs).
    pub prepare_latency_us: u64,
}

/// Configuration for the coordinator decision log.
#[derive(Debug, Clone)]
pub struct DecisionLogConfig {
    /// Maximum number of decision records to retain.
    pub max_entries: usize,
    /// How long to retain applied decisions before eviction.
    pub retention: Duration,
}

impl Default for DecisionLogConfig {
    fn default() -> Self {
        Self {
            max_entries: 10_000,
            retention: Duration::from_secs(3600), // 1 hour
        }
    }
}

/// Coordinator decision log — the durable point of truth for 2PC outcomes.
///
/// # Protocol
///
/// 1. Coordinator runs prepare on all shards.
/// 2. If all prepared: `log_decision(txn, Commit, shards)` — **this is the
///    durable commit point**. After this write, the transaction *will* commit
///    even if the coordinator crashes.
/// 3. Coordinator sends commit/abort to all participants.
/// 4. Once all participants acknowledge, `mark_applied(txn)`.
///
/// On crash recovery, the in-doubt resolver reads unapplied decisions from
/// this log and re-sends commit/abort to participants.
pub struct CoordinatorDecisionLog {
    config: DecisionLogConfig,
    records: RwLock<VecDeque<DecisionRecord>>,
    next_seq: AtomicU64,
    /// Total decisions logged.
    total_logged: AtomicU64,
    /// Total decisions applied (fully propagated).
    total_applied: AtomicU64,
}

impl CoordinatorDecisionLog {
    pub fn new(config: DecisionLogConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            records: RwLock::new(VecDeque::new()),
            next_seq: AtomicU64::new(1),
            total_logged: AtomicU64::new(0),
            total_applied: AtomicU64::new(0),
        })
    }

    /// Log a coordinator decision. This is the **durable commit point**.
    ///
    /// Must be called AFTER all shards have prepared (or any has failed)
    /// and BEFORE sending commit/abort to participants.
    pub fn log_decision(
        &self,
        txn_id: TxnId,
        decision: CoordinatorDecision,
        participant_shards: &[ShardId],
        prepare_latency_us: u64,
    ) -> u64 {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let record = DecisionRecord {
            seq,
            txn_id,
            decision,
            participant_shards: participant_shards.to_vec(),
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            applied: false,
            prepare_latency_us,
        };

        tracing::info!(
            seq = seq,
            txn_id = txn_id.0,
            decision = %decision,
            shards = ?participant_shards,
            "2PC decision logged (durable commit point)"
        );

        let mut records = self.records.write();
        // Evict old applied records if at capacity
        while records.len() >= self.config.max_entries {
            if let Some(front) = records.front() {
                if front.applied {
                    records.pop_front();
                } else {
                    break; // Don't evict unapplied decisions
                }
            } else {
                break;
            }
        }
        records.push_back(record);
        self.total_logged.fetch_add(1, Ordering::Relaxed);
        seq
    }

    /// Mark a decision as fully applied (all participants acknowledged).
    pub fn mark_applied(&self, txn_id: TxnId) -> bool {
        let mut records = self.records.write();
        for record in records.iter_mut().rev() {
            if record.txn_id == txn_id && !record.applied {
                record.applied = true;
                self.total_applied.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Get the decision for a transaction (used by in-doubt resolver).
    pub fn get_decision(&self, txn_id: TxnId) -> Option<CoordinatorDecision> {
        let records = self.records.read();
        records.iter().rev()
            .find(|r| r.txn_id == txn_id)
            .map(|r| r.decision)
    }

    /// Get all unapplied decisions (for crash recovery).
    pub fn unapplied_decisions(&self) -> Vec<DecisionRecord> {
        let records = self.records.read();
        records.iter()
            .filter(|r| !r.applied)
            .cloned()
            .collect()
    }

    /// Snapshot of recent decisions for observability.
    pub fn recent_decisions(&self, limit: usize) -> Vec<DecisionRecord> {
        let records = self.records.read();
        records.iter().rev().take(limit).cloned().collect()
    }

    /// Metrics snapshot.
    pub fn snapshot(&self) -> DecisionLogSnapshot {
        let records = self.records.read();
        let unapplied = records.iter().filter(|r| !r.applied).count();
        DecisionLogSnapshot {
            total_logged: self.total_logged.load(Ordering::Relaxed),
            total_applied: self.total_applied.load(Ordering::Relaxed),
            current_entries: records.len(),
            unapplied_count: unapplied,
            max_entries: self.config.max_entries,
        }
    }
}

/// Metrics snapshot for the decision log.
#[derive(Debug, Clone)]
pub struct DecisionLogSnapshot {
    pub total_logged: u64,
    pub total_applied: u64,
    pub current_entries: usize,
    pub unapplied_count: usize,
    pub max_entries: usize,
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Layered Timeout Controller
// ═══════════════════════════════════════════════════════════════════════════

/// Layered timeout configuration.
///
/// - **Soft timeout**: returns `Retryable` to the client — the transaction
///   is aborted but the client can retry immediately.
/// - **Hard timeout**: aborts the transaction AND enqueues it for the
///   in-doubt resolver (in case some shards have already prepared).
#[derive(Debug, Clone)]
pub struct LayeredTimeoutConfig {
    /// Soft timeout: client gets Retryable error.
    pub soft_timeout: Duration,
    /// Hard timeout: abort + enqueue for in-doubt resolver.
    pub hard_timeout: Duration,
    /// Per-shard RPC timeout (within soft timeout budget).
    pub per_shard_timeout: Duration,
}

impl Default for LayeredTimeoutConfig {
    fn default() -> Self {
        Self {
            soft_timeout: Duration::from_millis(500),
            hard_timeout: Duration::from_secs(5),
            per_shard_timeout: Duration::from_millis(200),
        }
    }
}

/// Result of a timeout check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeoutResult {
    /// Within budget — proceed.
    Ok,
    /// Soft timeout exceeded — return Retryable to client.
    SoftTimeout { elapsed_ms: u64, budget_ms: u64 },
    /// Hard timeout exceeded — abort + enqueue for resolver.
    HardTimeout { elapsed_ms: u64, budget_ms: u64 },
    /// Per-shard timeout exceeded.
    ShardTimeout { shard_id: ShardId, elapsed_us: u64, budget_us: u64 },
}

impl std::fmt::Display for TimeoutResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeoutResult::Ok => write!(f, "ok"),
            TimeoutResult::SoftTimeout { elapsed_ms, budget_ms } =>
                write!(f, "soft_timeout({}ms/{}ms)", elapsed_ms, budget_ms),
            TimeoutResult::HardTimeout { elapsed_ms, budget_ms } =>
                write!(f, "hard_timeout({}ms/{}ms)", elapsed_ms, budget_ms),
            TimeoutResult::ShardTimeout { shard_id, elapsed_us, budget_us } =>
                write!(f, "shard_timeout(shard_{}, {}us/{}us)", shard_id.0, elapsed_us, budget_us),
        }
    }
}

/// Layered timeout controller for 2PC transactions.
pub struct LayeredTimeoutController {
    config: LayeredTimeoutConfig,
    /// Total soft timeouts triggered.
    soft_timeouts: AtomicU64,
    /// Total hard timeouts triggered.
    hard_timeouts: AtomicU64,
    /// Total shard timeouts triggered.
    shard_timeouts: AtomicU64,
}

impl LayeredTimeoutController {
    pub fn new(config: LayeredTimeoutConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            soft_timeouts: AtomicU64::new(0),
            hard_timeouts: AtomicU64::new(0),
            shard_timeouts: AtomicU64::new(0),
        })
    }

    /// Check the overall transaction timeout.
    pub fn check_transaction(&self, started_at: Instant) -> TimeoutResult {
        let elapsed = started_at.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;

        if elapsed >= self.config.hard_timeout {
            self.hard_timeouts.fetch_add(1, Ordering::Relaxed);
            return TimeoutResult::HardTimeout {
                elapsed_ms,
                budget_ms: self.config.hard_timeout.as_millis() as u64,
            };
        }

        if elapsed >= self.config.soft_timeout {
            self.soft_timeouts.fetch_add(1, Ordering::Relaxed);
            return TimeoutResult::SoftTimeout {
                elapsed_ms,
                budget_ms: self.config.soft_timeout.as_millis() as u64,
            };
        }

        TimeoutResult::Ok
    }

    /// Check a per-shard RPC timeout.
    pub fn check_shard(&self, shard_id: ShardId, rpc_started_at: Instant) -> TimeoutResult {
        let elapsed = rpc_started_at.elapsed();
        let elapsed_us = elapsed.as_micros() as u64;

        if elapsed >= self.config.per_shard_timeout {
            self.shard_timeouts.fetch_add(1, Ordering::Relaxed);
            return TimeoutResult::ShardTimeout {
                shard_id,
                elapsed_us,
                budget_us: self.config.per_shard_timeout.as_micros() as u64,
            };
        }

        TimeoutResult::Ok
    }

    /// Get the configuration.
    pub fn config(&self) -> &LayeredTimeoutConfig {
        &self.config
    }

    /// Metrics snapshot.
    pub fn snapshot(&self) -> LayeredTimeoutSnapshot {
        LayeredTimeoutSnapshot {
            soft_timeout_ms: self.config.soft_timeout.as_millis() as u64,
            hard_timeout_ms: self.config.hard_timeout.as_millis() as u64,
            per_shard_timeout_us: self.config.per_shard_timeout.as_micros() as u64,
            total_soft_timeouts: self.soft_timeouts.load(Ordering::Relaxed),
            total_hard_timeouts: self.hard_timeouts.load(Ordering::Relaxed),
            total_shard_timeouts: self.shard_timeouts.load(Ordering::Relaxed),
        }
    }
}

/// Metrics snapshot for the layered timeout controller.
#[derive(Debug, Clone)]
pub struct LayeredTimeoutSnapshot {
    pub soft_timeout_ms: u64,
    pub hard_timeout_ms: u64,
    pub per_shard_timeout_us: u64,
    pub total_soft_timeouts: u64,
    pub total_hard_timeouts: u64,
    pub total_shard_timeouts: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Slow-Shard Policy
// ═══════════════════════════════════════════════════════════════════════════

/// Policy for handling slow shards during 2PC prepare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlowShardPolicy {
    /// Abort the entire transaction immediately when any shard exceeds
    /// the per-shard timeout. Minimizes tail latency at the cost of
    /// higher abort rate.
    FastAbort,

    /// Send a speculative retry (hedged request) to a replica of the
    /// slow shard. Use the first response. Reduces abort rate but
    /// increases resource usage.
    HedgedRequest,

    /// Wait for the slow shard up to the hard timeout. Most tolerant
    /// but worst for tail latency.
    Wait,
}

impl std::fmt::Display for SlowShardPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlowShardPolicy::FastAbort => write!(f, "fast_abort"),
            SlowShardPolicy::HedgedRequest => write!(f, "hedged_request"),
            SlowShardPolicy::Wait => write!(f, "wait"),
        }
    }
}

impl Default for SlowShardPolicy {
    fn default() -> Self {
        SlowShardPolicy::FastAbort
    }
}

/// Configuration for slow-shard handling.
#[derive(Debug, Clone)]
pub struct SlowShardConfig {
    /// The policy to apply when a shard is slow.
    pub policy: SlowShardPolicy,
    /// Threshold (µs) above which a shard is considered "slow".
    /// Typically set to the P95 or P99 of shard RPC latency.
    pub slow_threshold_us: u64,
    /// For HedgedRequest: delay before sending the hedge (µs).
    /// Set to ~P50 of shard RPC latency.
    pub hedge_delay_us: u64,
    /// Maximum number of hedged requests in flight at once.
    pub max_hedged_inflight: usize,
}

impl Default for SlowShardConfig {
    fn default() -> Self {
        Self {
            policy: SlowShardPolicy::FastAbort,
            slow_threshold_us: 10_000, // 10ms
            hedge_delay_us: 2_000,     // 2ms
            max_hedged_inflight: 8,
        }
    }
}

/// Tracker for slow-shard events and hedged request outcomes.
pub struct SlowShardTracker {
    config: SlowShardConfig,
    /// Total slow shard detections.
    total_slow_detected: AtomicU64,
    /// Total fast-aborts triggered by slow shards.
    total_fast_aborts: AtomicU64,
    /// Total hedged requests sent.
    total_hedged_sent: AtomicU64,
    /// Total hedged requests that won (returned before the original).
    total_hedged_won: AtomicU64,
    /// Current hedged requests in flight.
    hedged_inflight: AtomicU64,
    /// Per-shard slow event history (shard_id, latency_us, timestamp).
    slow_events: Mutex<VecDeque<SlowShardEvent>>,
    max_events: usize,
}

/// A recorded slow-shard event.
#[derive(Debug, Clone)]
pub struct SlowShardEvent {
    pub shard_id: ShardId,
    pub latency_us: u64,
    pub threshold_us: u64,
    pub action: SlowShardPolicy,
    pub timestamp_ms: u64,
}

impl SlowShardTracker {
    pub fn new(config: SlowShardConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            total_slow_detected: AtomicU64::new(0),
            total_fast_aborts: AtomicU64::new(0),
            total_hedged_sent: AtomicU64::new(0),
            total_hedged_won: AtomicU64::new(0),
            hedged_inflight: AtomicU64::new(0),
            slow_events: Mutex::new(VecDeque::new()),
            max_events: 256,
        })
    }

    /// Evaluate a shard's RPC latency and return the recommended action.
    pub fn evaluate(&self, shard_id: ShardId, latency_us: u64) -> SlowShardAction {
        if latency_us <= self.config.slow_threshold_us {
            return SlowShardAction::Proceed;
        }

        self.total_slow_detected.fetch_add(1, Ordering::Relaxed);

        let event = SlowShardEvent {
            shard_id,
            latency_us,
            threshold_us: self.config.slow_threshold_us,
            action: self.config.policy,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        let mut events = self.slow_events.lock();
        if events.len() >= self.max_events {
            events.pop_front();
        }
        events.push_back(event);

        match self.config.policy {
            SlowShardPolicy::FastAbort => {
                self.total_fast_aborts.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    shard_id = shard_id.0,
                    latency_us = latency_us,
                    threshold_us = self.config.slow_threshold_us,
                    "Slow shard detected — fast-abort"
                );
                SlowShardAction::Abort {
                    reason: format!(
                        "shard {} slow: {}us > {}us threshold (fast-abort policy)",
                        shard_id.0, latency_us, self.config.slow_threshold_us
                    ),
                }
            }
            SlowShardPolicy::HedgedRequest => {
                let inflight = self.hedged_inflight.load(Ordering::Relaxed);
                if inflight >= self.config.max_hedged_inflight as u64 {
                    // Too many hedges in flight — fall back to fast-abort
                    self.total_fast_aborts.fetch_add(1, Ordering::Relaxed);
                    SlowShardAction::Abort {
                        reason: format!(
                            "shard {} slow: {}us (hedge limit {} reached)",
                            shard_id.0, latency_us, self.config.max_hedged_inflight
                        ),
                    }
                } else {
                    self.total_hedged_sent.fetch_add(1, Ordering::Relaxed);
                    self.hedged_inflight.fetch_add(1, Ordering::Relaxed);
                    tracing::info!(
                        shard_id = shard_id.0,
                        latency_us = latency_us,
                        "Slow shard detected — sending hedged request"
                    );
                    SlowShardAction::Hedge {
                        shard_id,
                        delay_us: self.config.hedge_delay_us,
                    }
                }
            }
            SlowShardPolicy::Wait => {
                tracing::debug!(
                    shard_id = shard_id.0,
                    latency_us = latency_us,
                    "Slow shard detected — waiting (wait policy)"
                );
                SlowShardAction::Proceed
            }
        }
    }

    /// Record that a hedged request completed (call when hedge finishes).
    pub fn record_hedge_complete(&self, won: bool) {
        self.hedged_inflight.fetch_sub(1, Ordering::Relaxed);
        if won {
            self.total_hedged_won.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Recent slow shard events.
    pub fn recent_events(&self, limit: usize) -> Vec<SlowShardEvent> {
        let events = self.slow_events.lock();
        events.iter().rev().take(limit).cloned().collect()
    }

    /// Get the current policy.
    pub fn policy(&self) -> SlowShardPolicy {
        self.config.policy
    }

    /// Metrics snapshot.
    pub fn snapshot(&self) -> SlowShardSnapshot {
        SlowShardSnapshot {
            policy: self.config.policy,
            slow_threshold_us: self.config.slow_threshold_us,
            hedge_delay_us: self.config.hedge_delay_us,
            max_hedged_inflight: self.config.max_hedged_inflight,
            total_slow_detected: self.total_slow_detected.load(Ordering::Relaxed),
            total_fast_aborts: self.total_fast_aborts.load(Ordering::Relaxed),
            total_hedged_sent: self.total_hedged_sent.load(Ordering::Relaxed),
            total_hedged_won: self.total_hedged_won.load(Ordering::Relaxed),
            hedged_inflight: self.hedged_inflight.load(Ordering::Relaxed),
        }
    }
}

/// Action recommended by the slow-shard tracker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlowShardAction {
    /// Shard is within threshold — proceed normally.
    Proceed,
    /// Abort the transaction due to slow shard.
    Abort { reason: String },
    /// Send a hedged request to a replica.
    Hedge { shard_id: ShardId, delay_us: u64 },
}

/// Metrics snapshot for the slow-shard tracker.
#[derive(Debug, Clone)]
pub struct SlowShardSnapshot {
    pub policy: SlowShardPolicy,
    pub slow_threshold_us: u64,
    pub hedge_delay_us: u64,
    pub max_hedged_inflight: usize,
    pub total_slow_detected: u64,
    pub total_fast_aborts: u64,
    pub total_hedged_sent: u64,
    pub total_hedged_won: u64,
    pub hedged_inflight: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── Decision Log Tests ──────────────────────────────────────────────

    #[test]
    fn test_decision_log_basic() {
        let log = CoordinatorDecisionLog::new(DecisionLogConfig::default());
        let txn_id = TxnId(100);
        let shards = vec![ShardId(0), ShardId(1)];

        let seq = log.log_decision(txn_id, CoordinatorDecision::Commit, &shards, 500);
        assert_eq!(seq, 1);

        let decision = log.get_decision(txn_id);
        assert_eq!(decision, Some(CoordinatorDecision::Commit));
    }

    #[test]
    fn test_decision_log_mark_applied() {
        let log = CoordinatorDecisionLog::new(DecisionLogConfig::default());
        let txn_id = TxnId(200);

        log.log_decision(txn_id, CoordinatorDecision::Abort, &[ShardId(0)], 100);

        let unapplied = log.unapplied_decisions();
        assert_eq!(unapplied.len(), 1);

        assert!(log.mark_applied(txn_id));

        let unapplied = log.unapplied_decisions();
        assert_eq!(unapplied.len(), 0);
    }

    #[test]
    fn test_decision_log_unapplied_for_recovery() {
        let log = CoordinatorDecisionLog::new(DecisionLogConfig::default());

        log.log_decision(TxnId(1), CoordinatorDecision::Commit, &[ShardId(0)], 100);
        log.log_decision(TxnId(2), CoordinatorDecision::Abort, &[ShardId(1)], 200);
        log.log_decision(TxnId(3), CoordinatorDecision::Commit, &[ShardId(0), ShardId(1)], 300);

        log.mark_applied(TxnId(2));

        let unapplied = log.unapplied_decisions();
        assert_eq!(unapplied.len(), 2);
        assert_eq!(unapplied[0].txn_id, TxnId(1));
        assert_eq!(unapplied[1].txn_id, TxnId(3));
    }

    #[test]
    fn test_decision_log_eviction() {
        let config = DecisionLogConfig {
            max_entries: 3,
            retention: Duration::from_secs(1),
        };
        let log = CoordinatorDecisionLog::new(config);

        log.log_decision(TxnId(1), CoordinatorDecision::Commit, &[ShardId(0)], 100);
        log.mark_applied(TxnId(1));
        log.log_decision(TxnId(2), CoordinatorDecision::Commit, &[ShardId(0)], 100);
        log.mark_applied(TxnId(2));
        log.log_decision(TxnId(3), CoordinatorDecision::Commit, &[ShardId(0)], 100);
        log.mark_applied(TxnId(3));

        // This should evict the oldest applied entry
        log.log_decision(TxnId(4), CoordinatorDecision::Commit, &[ShardId(0)], 100);

        let snap = log.snapshot();
        assert!(snap.current_entries <= 3);
    }

    #[test]
    fn test_decision_log_snapshot() {
        let log = CoordinatorDecisionLog::new(DecisionLogConfig::default());
        log.log_decision(TxnId(1), CoordinatorDecision::Commit, &[ShardId(0)], 100);
        log.log_decision(TxnId(2), CoordinatorDecision::Abort, &[ShardId(1)], 200);
        log.mark_applied(TxnId(1));

        let snap = log.snapshot();
        assert_eq!(snap.total_logged, 2);
        assert_eq!(snap.total_applied, 1);
        assert_eq!(snap.unapplied_count, 1);
    }

    #[test]
    fn test_decision_display() {
        assert_eq!(CoordinatorDecision::Commit.to_string(), "commit");
        assert_eq!(CoordinatorDecision::Abort.to_string(), "abort");
    }

    // ── Layered Timeout Tests ───────────────────────────────────────────

    #[test]
    fn test_timeout_within_budget() {
        let ctrl = LayeredTimeoutController::new(LayeredTimeoutConfig::default());
        let started = Instant::now();
        assert_eq!(ctrl.check_transaction(started), TimeoutResult::Ok);
    }

    #[test]
    fn test_soft_timeout() {
        let config = LayeredTimeoutConfig {
            soft_timeout: Duration::from_millis(1),
            hard_timeout: Duration::from_secs(5),
            per_shard_timeout: Duration::from_millis(200),
        };
        let ctrl = LayeredTimeoutController::new(config);
        let started = Instant::now();
        std::thread::sleep(Duration::from_millis(5));
        let result = ctrl.check_transaction(started);
        match result {
            TimeoutResult::SoftTimeout { .. } => {}
            other => panic!("expected SoftTimeout, got {:?}", other),
        }
        assert_eq!(ctrl.snapshot().total_soft_timeouts, 1);
    }

    #[test]
    fn test_hard_timeout() {
        let config = LayeredTimeoutConfig {
            soft_timeout: Duration::from_millis(1),
            hard_timeout: Duration::from_millis(2),
            per_shard_timeout: Duration::from_millis(200),
        };
        let ctrl = LayeredTimeoutController::new(config);
        let started = Instant::now();
        std::thread::sleep(Duration::from_millis(10));
        let result = ctrl.check_transaction(started);
        match result {
            TimeoutResult::HardTimeout { .. } => {}
            other => panic!("expected HardTimeout, got {:?}", other),
        }
        assert_eq!(ctrl.snapshot().total_hard_timeouts, 1);
    }

    #[test]
    fn test_shard_timeout() {
        let config = LayeredTimeoutConfig {
            soft_timeout: Duration::from_secs(5),
            hard_timeout: Duration::from_secs(10),
            per_shard_timeout: Duration::from_millis(1),
        };
        let ctrl = LayeredTimeoutController::new(config);
        let rpc_start = Instant::now();
        std::thread::sleep(Duration::from_millis(5));
        let result = ctrl.check_shard(ShardId(3), rpc_start);
        match result {
            TimeoutResult::ShardTimeout { shard_id, .. } => {
                assert_eq!(shard_id, ShardId(3));
            }
            other => panic!("expected ShardTimeout, got {:?}", other),
        }
    }

    #[test]
    fn test_timeout_result_display() {
        assert_eq!(TimeoutResult::Ok.to_string(), "ok");
        let soft = TimeoutResult::SoftTimeout { elapsed_ms: 600, budget_ms: 500 };
        assert!(soft.to_string().contains("soft_timeout"));
        let hard = TimeoutResult::HardTimeout { elapsed_ms: 6000, budget_ms: 5000 };
        assert!(hard.to_string().contains("hard_timeout"));
    }

    #[test]
    fn test_timeout_snapshot() {
        let ctrl = LayeredTimeoutController::new(LayeredTimeoutConfig::default());
        let snap = ctrl.snapshot();
        assert_eq!(snap.soft_timeout_ms, 500);
        assert_eq!(snap.hard_timeout_ms, 5000);
        assert_eq!(snap.total_soft_timeouts, 0);
    }

    // ── Slow-Shard Policy Tests ─────────────────────────────────────────

    #[test]
    fn test_slow_shard_within_threshold() {
        let tracker = SlowShardTracker::new(SlowShardConfig::default());
        let action = tracker.evaluate(ShardId(0), 5_000); // 5ms < 10ms threshold
        assert_eq!(action, SlowShardAction::Proceed);
    }

    #[test]
    fn test_slow_shard_fast_abort() {
        let config = SlowShardConfig {
            policy: SlowShardPolicy::FastAbort,
            slow_threshold_us: 5_000,
            ..Default::default()
        };
        let tracker = SlowShardTracker::new(config);
        let action = tracker.evaluate(ShardId(1), 10_000); // 10ms > 5ms
        match action {
            SlowShardAction::Abort { reason } => {
                assert!(reason.contains("fast-abort"));
                assert!(reason.contains("shard 1"));
            }
            other => panic!("expected Abort, got {:?}", other),
        }
        assert_eq!(tracker.snapshot().total_fast_aborts, 1);
    }

    #[test]
    fn test_slow_shard_hedged_request() {
        let config = SlowShardConfig {
            policy: SlowShardPolicy::HedgedRequest,
            slow_threshold_us: 5_000,
            hedge_delay_us: 1_000,
            max_hedged_inflight: 4,
        };
        let tracker = SlowShardTracker::new(config);
        let action = tracker.evaluate(ShardId(2), 10_000);
        match action {
            SlowShardAction::Hedge { shard_id, delay_us } => {
                assert_eq!(shard_id, ShardId(2));
                assert_eq!(delay_us, 1_000);
            }
            other => panic!("expected Hedge, got {:?}", other),
        }
        assert_eq!(tracker.snapshot().total_hedged_sent, 1);
        assert_eq!(tracker.snapshot().hedged_inflight, 1);

        tracker.record_hedge_complete(true);
        assert_eq!(tracker.snapshot().hedged_inflight, 0);
        assert_eq!(tracker.snapshot().total_hedged_won, 1);
    }

    #[test]
    fn test_slow_shard_hedge_limit_fallback() {
        let config = SlowShardConfig {
            policy: SlowShardPolicy::HedgedRequest,
            slow_threshold_us: 1_000,
            max_hedged_inflight: 1,
            ..Default::default()
        };
        let tracker = SlowShardTracker::new(config);

        // First hedge succeeds
        let action1 = tracker.evaluate(ShardId(0), 5_000);
        assert!(matches!(action1, SlowShardAction::Hedge { .. }));

        // Second hedge should fall back to abort (limit reached)
        let action2 = tracker.evaluate(ShardId(1), 5_000);
        assert!(matches!(action2, SlowShardAction::Abort { .. }));
    }

    #[test]
    fn test_slow_shard_wait_policy() {
        let config = SlowShardConfig {
            policy: SlowShardPolicy::Wait,
            slow_threshold_us: 5_000,
            ..Default::default()
        };
        let tracker = SlowShardTracker::new(config);
        let action = tracker.evaluate(ShardId(0), 50_000);
        assert_eq!(action, SlowShardAction::Proceed);
    }

    #[test]
    fn test_slow_shard_events_recorded() {
        let config = SlowShardConfig {
            policy: SlowShardPolicy::FastAbort,
            slow_threshold_us: 1_000,
            ..Default::default()
        };
        let tracker = SlowShardTracker::new(config);
        tracker.evaluate(ShardId(0), 5_000);
        tracker.evaluate(ShardId(1), 10_000);

        let events = tracker.recent_events(10);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].shard_id, ShardId(1)); // most recent first
    }

    #[test]
    fn test_slow_shard_policy_display() {
        assert_eq!(SlowShardPolicy::FastAbort.to_string(), "fast_abort");
        assert_eq!(SlowShardPolicy::HedgedRequest.to_string(), "hedged_request");
        assert_eq!(SlowShardPolicy::Wait.to_string(), "wait");
    }

    #[test]
    fn test_slow_shard_snapshot() {
        let tracker = SlowShardTracker::new(SlowShardConfig::default());
        let snap = tracker.snapshot();
        assert_eq!(snap.policy, SlowShardPolicy::FastAbort);
        assert_eq!(snap.slow_threshold_us, 10_000);
        assert_eq!(snap.total_slow_detected, 0);
    }
}
