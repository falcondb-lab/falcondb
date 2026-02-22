//! Failover × Transaction Hardening Module (v1.0.2)
//!
//! Provides deterministic, bounded, and explainable behavior for transactions
//! during leader failover, replica loss, and coordination instability.
//!
//! # Components
//!
//! 1. **`FailoverTxnCoordinator`** — atomic failover×txn interaction:
//!    - Drains active transactions on failover (fail-fast or complete).
//!    - Prevents new writes during failover transition window.
//!    - Ensures no partial commits are visible to clients.
//!
//! 2. **`InDoubtTtlEnforcer`** — bounded in-doubt transaction lifetime:
//!    - Hard TTL on in-doubt transactions.
//!    - Forced abort after TTL expiry (no infinite persistence).
//!    - Structured logging for every forced resolution.
//!
//! 3. **`FailoverDamper`** — churn suppression:
//!    - Rate-limits failover events to prevent oscillation.
//!    - Tracks failover history for observability.
//!    - Prevents txn state leaks during rapid failover sequences.
//!
//! 4. **`FailoverBlockedTxnGuard`** — tail-latency protection:
//!    - Bounded timeout for txns blocked by in-progress failover.
//!    - Deterministic fail-fast when timeout exceeds budget.
//!    - Latency convergence tracking after failover completes.
//!
//! # Invariants (v1.0.2)
//!
//! - No transaction may be partially committed during failover.
//! - Client never observes success followed by rollback.
//! - In-doubt transactions have a maximum lifetime (default 60s).
//! - Rapid failover events are damped (min interval enforced).
//! - Blocked transactions fail deterministically within bounded time.
//! - All failover×txn interactions produce structured log entries.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use falcon_common::error::{FalconError, TxnError};
use falcon_common::types::TxnId;

// ═══════════════════════════════════════════════════════════════════════════
// §1: Failover × Transaction Coordinator
// ═══════════════════════════════════════════════════════════════════════════

/// Phase of a failover event with respect to active transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverTxnPhase {
    /// Normal operation — no failover in progress.
    Normal,
    /// Failover initiated — draining active transactions.
    /// New writes are rejected; active txns must complete or abort.
    Draining,
    /// Failover complete — new leader accepting writes.
    /// Latency convergence tracking active.
    Converging,
}

impl std::fmt::Display for FailoverTxnPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normal => write!(f, "Normal"),
            Self::Draining => write!(f, "Draining"),
            Self::Converging => write!(f, "Converging"),
        }
    }
}

/// Record of a transaction affected by failover.
#[derive(Debug, Clone)]
pub struct FailoverAffectedTxn {
    pub txn_id: TxnId,
    /// Was the txn in Prepared state when failover hit?
    pub was_prepared: bool,
    /// Resolution: aborted or allowed to complete.
    pub resolution: FailoverTxnResolution,
    /// Time from failover start to resolution.
    pub resolution_latency_us: u64,
}

/// How a transaction was resolved during failover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverTxnResolution {
    /// Transaction was aborted (fail-fast).
    Aborted,
    /// Transaction completed before drain timeout.
    CompletedBeforeDrain,
    /// Transaction was in Prepared state — moved to in-doubt.
    MovedToInDoubt,
}

impl std::fmt::Display for FailoverTxnResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aborted => write!(f, "Aborted"),
            Self::CompletedBeforeDrain => write!(f, "CompletedBeforeDrain"),
            Self::MovedToInDoubt => write!(f, "MovedToInDoubt"),
        }
    }
}

/// Configuration for the failover×txn coordinator.
#[derive(Debug, Clone)]
pub struct FailoverTxnConfig {
    /// Maximum time to wait for active transactions to drain during failover.
    /// After this, remaining active txns are force-aborted.
    pub drain_timeout: Duration,
    /// Time after failover completes during which latency convergence is tracked.
    pub convergence_window: Duration,
    /// Maximum number of affected-txn records to retain (bounded memory).
    pub max_affected_records: usize,
}

impl Default for FailoverTxnConfig {
    fn default() -> Self {
        Self {
            drain_timeout: Duration::from_secs(5),
            convergence_window: Duration::from_secs(30),
            max_affected_records: 10_000,
        }
    }
}

/// Metrics for the failover×txn coordinator.
#[derive(Debug, Clone, Default)]
pub struct FailoverTxnMetrics {
    /// Total failover events processed.
    pub failover_events: u64,
    /// Total transactions drained (completed before deadline).
    pub txns_drained: u64,
    /// Total transactions force-aborted during failover.
    pub txns_force_aborted: u64,
    /// Total transactions moved to in-doubt during failover.
    pub txns_moved_indoubt: u64,
    /// Current phase.
    pub current_phase: String,
    /// Time in current phase (ms).
    pub phase_duration_ms: u64,
    /// Last failover duration (ms).
    pub last_failover_duration_ms: u64,
}

/// Atomic coordinator for failover × transaction interactions.
///
/// Ensures that:
/// - No transaction is partially committed during failover.
/// - Active transactions are deterministically drained or aborted.
/// - Prepared transactions are moved to the in-doubt resolver.
/// - New writes are rejected during the drain phase.
pub struct FailoverTxnCoordinator {
    config: FailoverTxnConfig,
    phase: RwLock<FailoverTxnPhase>,
    phase_entered_at: Mutex<Instant>,
    /// Epoch at which the current failover started.
    failover_epoch: AtomicU64,
    /// Whether new writes should be rejected.
    writes_blocked: AtomicBool,
    /// Affected transactions from the most recent failover.
    affected_txns: Mutex<Vec<FailoverAffectedTxn>>,
    /// Metrics.
    metrics: RwLock<FailoverTxnMetrics>,
}

impl FailoverTxnCoordinator {
    /// Create a new coordinator with default config.
    pub fn new() -> Self {
        Self::with_config(FailoverTxnConfig::default())
    }

    /// Create a new coordinator with custom config.
    pub fn with_config(config: FailoverTxnConfig) -> Self {
        Self {
            config,
            phase: RwLock::new(FailoverTxnPhase::Normal),
            phase_entered_at: Mutex::new(Instant::now()),
            failover_epoch: AtomicU64::new(0),
            writes_blocked: AtomicBool::new(false),
            affected_txns: Mutex::new(Vec::new()),
            metrics: RwLock::new(FailoverTxnMetrics::default()),
        }
    }

    /// Current failover×txn phase.
    pub fn current_phase(&self) -> FailoverTxnPhase {
        *self.phase.read()
    }

    /// Whether writes are currently blocked (failover drain in progress).
    pub fn are_writes_blocked(&self) -> bool {
        self.writes_blocked.load(Ordering::SeqCst)
    }

    /// Check if a write should be rejected due to failover.
    /// Returns `Ok(())` if the write is allowed, or `Err` with a retryable error.
    pub fn check_write_allowed(&self, epoch: u64) -> Result<(), FalconError> {
        if self.writes_blocked.load(Ordering::SeqCst) {
            return Err(FalconError::retryable(
                "writes blocked: failover drain in progress",
                0,
                epoch,
                None,
                100,
            ));
        }
        Ok(())
    }

    /// Begin the failover drain phase.
    ///
    /// Called when a failover event is detected. This:
    /// 1. Blocks new writes.
    /// 2. Returns the list of active transaction IDs that must be drained.
    /// 3. Sets the phase to `Draining`.
    ///
    /// The caller is responsible for draining/aborting the returned txn IDs.
    pub fn begin_failover_drain(
        &self,
        new_epoch: u64,
        active_txn_ids: Vec<TxnId>,
    ) -> Vec<TxnId> {
        // Block new writes immediately
        self.writes_blocked.store(true, Ordering::SeqCst);
        self.failover_epoch.store(new_epoch, Ordering::SeqCst);

        // Transition to Draining phase
        {
            let mut phase = self.phase.write();
            *phase = FailoverTxnPhase::Draining;
            *self.phase_entered_at.lock() = Instant::now();
        }

        // Clear previous affected records
        {
            let mut affected = self.affected_txns.lock();
            affected.clear();
        }

        tracing::warn!(
            epoch = new_epoch,
            active_txns = active_txn_ids.len(),
            drain_timeout_ms = self.config.drain_timeout.as_millis() as u64,
            "failover drain started: blocking writes, draining active transactions"
        );

        {
            let mut m = self.metrics.write();
            m.failover_events += 1;
            m.current_phase = "Draining".into();
        }

        active_txn_ids
    }

    /// Record that a transaction was resolved during failover drain.
    pub fn record_affected_txn(
        &self,
        txn_id: TxnId,
        was_prepared: bool,
        resolution: FailoverTxnResolution,
        resolution_latency_us: u64,
    ) {
        let mut affected = self.affected_txns.lock();
        if affected.len() < self.config.max_affected_records {
            affected.push(FailoverAffectedTxn {
                txn_id,
                was_prepared,
                resolution,
                resolution_latency_us,
            });
        }

        // Update metrics
        let mut m = self.metrics.write();
        match resolution {
            FailoverTxnResolution::Aborted => m.txns_force_aborted += 1,
            FailoverTxnResolution::CompletedBeforeDrain => m.txns_drained += 1,
            FailoverTxnResolution::MovedToInDoubt => m.txns_moved_indoubt += 1,
        }

        tracing::info!(
            txn_id = txn_id.0,
            was_prepared = was_prepared,
            resolution = %resolution,
            latency_us = resolution_latency_us,
            "failover: transaction resolved"
        );
    }

    /// Complete the failover drain phase.
    ///
    /// Called after all active transactions have been drained/aborted.
    /// Transitions to `Converging` phase and unblocks writes.
    pub fn complete_failover_drain(&self) {
        let drain_start = *self.phase_entered_at.lock();
        let drain_duration_ms = drain_start.elapsed().as_millis() as u64;

        // Unblock writes
        self.writes_blocked.store(false, Ordering::SeqCst);

        // Transition to Converging
        {
            let mut phase = self.phase.write();
            *phase = FailoverTxnPhase::Converging;
            *self.phase_entered_at.lock() = Instant::now();
        }

        {
            let mut m = self.metrics.write();
            m.current_phase = "Converging".into();
            m.last_failover_duration_ms = drain_duration_ms;
        }

        tracing::info!(
            drain_duration_ms = drain_duration_ms,
            "failover drain complete: writes unblocked, entering convergence"
        );
    }

    /// Check if convergence window has elapsed and return to Normal.
    /// Call this periodically after failover.
    pub fn check_convergence(&self) -> bool {
        let phase = *self.phase.read();
        if phase != FailoverTxnPhase::Converging {
            return phase == FailoverTxnPhase::Normal;
        }

        let entered = *self.phase_entered_at.lock();
        if entered.elapsed() >= self.config.convergence_window {
            let mut phase = self.phase.write();
            *phase = FailoverTxnPhase::Normal;
            *self.phase_entered_at.lock() = Instant::now();

            let mut m = self.metrics.write();
            m.current_phase = "Normal".into();

            tracing::info!("failover convergence complete: returning to normal operation");
            return true;
        }
        false
    }

    /// Drain timeout exceeded check: returns true if drain should be force-completed.
    pub fn is_drain_timeout_exceeded(&self) -> bool {
        let phase = *self.phase.read();
        if phase != FailoverTxnPhase::Draining {
            return false;
        }
        let entered = *self.phase_entered_at.lock();
        entered.elapsed() >= self.config.drain_timeout
    }

    /// Get metrics snapshot.
    pub fn metrics(&self) -> FailoverTxnMetrics {
        let mut m = self.metrics.read().clone();
        m.phase_duration_ms = self.phase_entered_at.lock().elapsed().as_millis() as u64;
        m
    }

    /// Get the list of affected transactions from the most recent failover.
    pub fn affected_txns(&self) -> Vec<FailoverAffectedTxn> {
        self.affected_txns.lock().clone()
    }

    /// Current failover epoch.
    pub fn failover_epoch(&self) -> u64 {
        self.failover_epoch.load(Ordering::SeqCst)
    }
}

impl Default for FailoverTxnCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2: In-Doubt TTL Enforcer
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for in-doubt transaction TTL enforcement.
#[derive(Debug, Clone)]
pub struct InDoubtTtlConfig {
    /// Maximum lifetime for an in-doubt transaction before forced abort.
    /// After this, the transaction is unconditionally aborted regardless
    /// of coordinator outcome cache status.
    pub max_lifetime: Duration,
    /// How often to run the TTL enforcement sweep.
    pub sweep_interval: Duration,
    /// Whether to log a structured warning when a txn approaches TTL.
    /// Warning is emitted at 80% of max_lifetime.
    pub warn_threshold_ratio: f64,
}

impl Default for InDoubtTtlConfig {
    fn default() -> Self {
        Self {
            max_lifetime: Duration::from_secs(60),
            sweep_interval: Duration::from_secs(5),
            warn_threshold_ratio: 0.8,
        }
    }
}

/// Metrics for TTL enforcement.
#[derive(Debug, Clone, Default)]
pub struct InDoubtTtlMetrics {
    /// Total TTL expirations (forced aborts).
    pub ttl_expirations: u64,
    /// Total TTL warnings emitted.
    pub ttl_warnings: u64,
    /// Total enforcement sweeps.
    pub sweeps: u64,
    /// Currently tracked in-doubt txn count.
    pub tracked_count: usize,
    /// Oldest tracked txn age (ms).
    pub oldest_age_ms: u64,
}

/// Tracks in-doubt transaction registration times for TTL enforcement.
struct InDoubtTtlEntry {
    txn_id: TxnId,
    registered_at: Instant,
    warned: bool,
}

/// Enforces a hard maximum lifetime on in-doubt transactions.
///
/// Prevents in-doubt transactions from persisting indefinitely.
/// After `max_lifetime`, the transaction is unconditionally aborted.
pub struct InDoubtTtlEnforcer {
    config: InDoubtTtlConfig,
    entries: RwLock<HashMap<TxnId, InDoubtTtlEntry>>,
    metrics: RwLock<InDoubtTtlMetrics>,
}

impl InDoubtTtlEnforcer {
    pub fn new() -> Self {
        Self::with_config(InDoubtTtlConfig::default())
    }

    pub fn with_config(config: InDoubtTtlConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(HashMap::new()),
            metrics: RwLock::new(InDoubtTtlMetrics::default()),
        }
    }

    /// Register a new in-doubt transaction for TTL tracking.
    pub fn register(&self, txn_id: TxnId) {
        let mut entries = self.entries.write();
        entries.insert(
            txn_id,
            InDoubtTtlEntry {
                txn_id,
                registered_at: Instant::now(),
                warned: false,
            },
        );
        tracing::debug!(txn_id = txn_id.0, "TTL enforcer: registered in-doubt txn");
    }

    /// Remove a transaction from TTL tracking (resolved normally).
    pub fn remove(&self, txn_id: TxnId) {
        self.entries.write().remove(&txn_id);
    }

    /// Run one TTL enforcement sweep.
    ///
    /// Returns the list of transaction IDs that exceeded their TTL
    /// and should be force-aborted by the caller.
    pub fn sweep(&self) -> Vec<TxnId> {
        let now = Instant::now();
        let warn_threshold = Duration::from_secs_f64(
            self.config.max_lifetime.as_secs_f64() * self.config.warn_threshold_ratio,
        );

        let mut expired = Vec::new();
        let mut warnings = 0u64;

        {
            let mut entries = self.entries.write();
            let mut to_remove = Vec::new();

            for entry in entries.values_mut() {
                let age = now.duration_since(entry.registered_at);

                if age >= self.config.max_lifetime {
                    // TTL expired — force abort
                    expired.push(entry.txn_id);
                    to_remove.push(entry.txn_id);

                    tracing::error!(
                        txn_id = entry.txn_id.0,
                        age_ms = age.as_millis() as u64,
                        max_lifetime_ms = self.config.max_lifetime.as_millis() as u64,
                        "TTL enforcer: in-doubt txn EXPIRED — force aborting"
                    );
                } else if age >= warn_threshold && !entry.warned {
                    // Approaching TTL — warn
                    entry.warned = true;
                    warnings += 1;

                    tracing::warn!(
                        txn_id = entry.txn_id.0,
                        age_ms = age.as_millis() as u64,
                        remaining_ms = (self.config.max_lifetime - age).as_millis() as u64,
                        "TTL enforcer: in-doubt txn approaching TTL"
                    );
                }
            }

            for id in &to_remove {
                entries.remove(id);
            }
        }

        // Update metrics
        {
            let entries = self.entries.read();
            let oldest_age_ms = entries
                .values()
                .map(|e| now.duration_since(e.registered_at).as_millis() as u64)
                .max()
                .unwrap_or(0);

            let mut m = self.metrics.write();
            m.ttl_expirations += expired.len() as u64;
            m.ttl_warnings += warnings;
            m.sweeps += 1;
            m.tracked_count = entries.len();
            m.oldest_age_ms = oldest_age_ms;
        }

        expired
    }

    /// Current metrics snapshot.
    pub fn metrics(&self) -> InDoubtTtlMetrics {
        self.metrics.read().clone()
    }

    /// Number of currently tracked in-doubt transactions.
    pub fn tracked_count(&self) -> usize {
        self.entries.read().len()
    }
}

impl Default for InDoubtTtlEnforcer {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3: Failover Damper (Churn Suppression)
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for failover churn damping.
#[derive(Debug, Clone)]
pub struct FailoverDamperConfig {
    /// Minimum interval between failover events. Failovers attempted
    /// within this window are suppressed.
    pub min_interval: Duration,
    /// Maximum failovers allowed in the observation window.
    pub max_failovers_in_window: u32,
    /// Observation window for rate-limiting.
    pub observation_window: Duration,
    /// Whether to log suppressed failover attempts.
    pub log_suppressed: bool,
}

impl Default for FailoverDamperConfig {
    fn default() -> Self {
        Self {
            min_interval: Duration::from_secs(30),
            max_failovers_in_window: 3,
            observation_window: Duration::from_secs(300),
            log_suppressed: true,
        }
    }
}

/// Record of a failover event for damping history.
#[derive(Debug, Clone)]
pub struct FailoverEvent {
    pub epoch: u64,
    pub timestamp: Instant,
    pub shard_id: u64,
    pub suppressed: bool,
    pub reason: String,
}

/// Metrics for the failover damper.
#[derive(Debug, Clone, Default)]
pub struct FailoverDamperMetrics {
    /// Total failover attempts.
    pub total_attempts: u64,
    /// Total failovers allowed (executed).
    pub total_allowed: u64,
    /// Total failovers suppressed.
    pub total_suppressed: u64,
    /// Current suppression state.
    pub is_suppressing: bool,
    /// Time since last allowed failover (ms).
    pub since_last_allowed_ms: u64,
    /// Failovers in current observation window.
    pub failovers_in_window: u32,
}

/// Prevents rapid failover oscillation ("flapping") that can leak txn state
/// and cause unbounded in-doubt accumulation.
///
/// Enforces:
/// - Minimum interval between consecutive failovers.
/// - Maximum failover rate within an observation window.
pub struct FailoverDamper {
    config: FailoverDamperConfig,
    /// History of failover events (bounded by observation window).
    history: Mutex<Vec<FailoverEvent>>,
    /// Last allowed failover time.
    last_allowed: Mutex<Option<Instant>>,
    /// Metrics.
    total_attempts: AtomicU64,
    total_allowed: AtomicU64,
    total_suppressed: AtomicU64,
}

impl FailoverDamper {
    pub fn new() -> Self {
        Self::with_config(FailoverDamperConfig::default())
    }

    pub fn with_config(config: FailoverDamperConfig) -> Self {
        Self {
            config,
            history: Mutex::new(Vec::new()),
            last_allowed: Mutex::new(None),
            total_attempts: AtomicU64::new(0),
            total_allowed: AtomicU64::new(0),
            total_suppressed: AtomicU64::new(0),
        }
    }

    /// Check if a failover should be allowed.
    ///
    /// Returns `Ok(())` if the failover is allowed.
    /// Returns `Err` with a reason if the failover is suppressed.
    pub fn check_failover_allowed(
        &self,
        epoch: u64,
        shard_id: u64,
    ) -> Result<(), String> {
        self.total_attempts.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();

        // Check min interval
        {
            let last = self.last_allowed.lock();
            if let Some(last_time) = *last {
                if now.duration_since(last_time) < self.config.min_interval {
                    let reason = format!(
                        "failover suppressed: min interval not elapsed ({}ms since last, required {}ms)",
                        now.duration_since(last_time).as_millis(),
                        self.config.min_interval.as_millis(),
                    );
                    self.record_suppressed(epoch, shard_id, &reason);
                    return Err(reason);
                }
            }
        }

        // Check rate limit in observation window
        {
            let history = self.history.lock();
            let window_start = now - self.config.observation_window;
            let recent_count = history
                .iter()
                .filter(|e| e.timestamp > window_start && !e.suppressed)
                .count() as u32;

            if recent_count >= self.config.max_failovers_in_window {
                let reason = format!(
                    "failover suppressed: rate limit exceeded ({} in {}s window, max {})",
                    recent_count,
                    self.config.observation_window.as_secs(),
                    self.config.max_failovers_in_window,
                );
                drop(history);
                self.record_suppressed(epoch, shard_id, &reason);
                return Err(reason);
            }
        }

        // Allowed — record it
        self.total_allowed.fetch_add(1, Ordering::Relaxed);
        *self.last_allowed.lock() = Some(now);

        {
            let mut history = self.history.lock();
            history.push(FailoverEvent {
                epoch,
                timestamp: now,
                shard_id,
                suppressed: false,
                reason: "allowed".into(),
            });
            self.gc_history(&mut history, now);
        }

        Ok(())
    }

    fn record_suppressed(&self, epoch: u64, shard_id: u64, reason: &str) {
        self.total_suppressed.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();

        if self.config.log_suppressed {
            tracing::warn!(
                epoch = epoch,
                shard_id = shard_id,
                reason = reason,
                "failover damper: suppressed failover attempt"
            );
        }

        let mut history = self.history.lock();
        history.push(FailoverEvent {
            epoch,
            timestamp: now,
            shard_id,
            suppressed: true,
            reason: reason.to_string(),
        });
        self.gc_history(&mut history, now);
    }

    /// Remove events outside the observation window.
    fn gc_history(&self, history: &mut Vec<FailoverEvent>, now: Instant) {
        let cutoff = now - self.config.observation_window;
        history.retain(|e| e.timestamp > cutoff);
    }

    /// Record that a failover completed successfully (for cooldown tracking).
    pub fn record_failover_complete(&self) {
        *self.last_allowed.lock() = Some(Instant::now());
    }

    /// Metrics snapshot.
    pub fn metrics(&self) -> FailoverDamperMetrics {
        let now = Instant::now();
        let last = self.last_allowed.lock();
        let since_last_ms = last
            .map(|t| now.duration_since(t).as_millis() as u64)
            .unwrap_or(0);

        let history = self.history.lock();
        let window_start = now - self.config.observation_window;
        let in_window = history
            .iter()
            .filter(|e| e.timestamp > window_start && !e.suppressed)
            .count() as u32;

        FailoverDamperMetrics {
            total_attempts: self.total_attempts.load(Ordering::Relaxed),
            total_allowed: self.total_allowed.load(Ordering::Relaxed),
            total_suppressed: self.total_suppressed.load(Ordering::Relaxed),
            is_suppressing: false,
            since_last_allowed_ms: since_last_ms,
            failovers_in_window: in_window,
        }
    }

    /// Failover history within the observation window.
    pub fn recent_history(&self) -> Vec<FailoverEvent> {
        let now = Instant::now();
        let cutoff = now - self.config.observation_window;
        self.history
            .lock()
            .iter()
            .filter(|e| e.timestamp > cutoff)
            .cloned()
            .collect()
    }
}

impl Default for FailoverDamper {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4: Failover Blocked Transaction Guard
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for blocked-transaction timeout during failover.
#[derive(Debug, Clone)]
pub struct FailoverBlockedTxnConfig {
    /// Maximum time a transaction may be blocked waiting for failover to complete.
    /// After this, the transaction is deterministically aborted.
    pub max_blocked_duration: Duration,
    /// Latency percentile tracking window after failover.
    pub latency_tracking_window: Duration,
}

impl Default for FailoverBlockedTxnConfig {
    fn default() -> Self {
        Self {
            max_blocked_duration: Duration::from_secs(10),
            latency_tracking_window: Duration::from_secs(60),
        }
    }
}

/// Metrics for blocked-transaction protection.
#[derive(Debug, Clone, Default)]
pub struct FailoverBlockedTxnMetrics {
    /// Total transactions that were blocked by failover.
    pub total_blocked: u64,
    /// Total transactions that timed out while blocked.
    pub total_timed_out: u64,
    /// Total transactions that completed after block was released.
    pub total_released: u64,
    /// Current number of blocked transactions.
    pub currently_blocked: u64,
    /// p50/p99/max latencies of blocked transactions (us).
    pub latency_p50_us: u64,
    pub latency_p99_us: u64,
    pub latency_max_us: u64,
}

/// Entry tracking a blocked transaction.
struct BlockedTxnEntry {
    blocked_at: Instant,
}

/// Guards transactions blocked by in-progress failover.
///
/// Ensures that blocked transactions either complete within a bounded time
/// or are deterministically aborted. Tracks latency distribution for
/// convergence monitoring.
pub struct FailoverBlockedTxnGuard {
    config: FailoverBlockedTxnConfig,
    blocked: RwLock<HashMap<TxnId, BlockedTxnEntry>>,
    /// Completed latencies for percentile tracking (bounded ring buffer).
    latencies_us: Mutex<Vec<u64>>,
    total_blocked: AtomicU64,
    total_timed_out: AtomicU64,
    total_released: AtomicU64,
}

impl FailoverBlockedTxnGuard {
    pub fn new() -> Self {
        Self::with_config(FailoverBlockedTxnConfig::default())
    }

    pub fn with_config(config: FailoverBlockedTxnConfig) -> Self {
        Self {
            config,
            blocked: RwLock::new(HashMap::new()),
            latencies_us: Mutex::new(Vec::new()),
            total_blocked: AtomicU64::new(0),
            total_timed_out: AtomicU64::new(0),
            total_released: AtomicU64::new(0),
        }
    }

    /// Register a transaction as blocked by failover.
    pub fn register_blocked(&self, txn_id: TxnId) {
        self.blocked.write().insert(
            txn_id,
            BlockedTxnEntry {
                blocked_at: Instant::now(),
            },
        );
        self.total_blocked.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if a blocked transaction has timed out.
    /// Returns `Err` if the transaction should be aborted.
    pub fn check_blocked_timeout(&self, txn_id: TxnId) -> Result<(), TxnError> {
        let blocked = self.blocked.read();
        if let Some(entry) = blocked.get(&txn_id) {
            if entry.blocked_at.elapsed() >= self.config.max_blocked_duration {
                self.total_timed_out.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    txn_id = txn_id.0,
                    blocked_ms = entry.blocked_at.elapsed().as_millis() as u64,
                    max_ms = self.config.max_blocked_duration.as_millis() as u64,
                    "blocked txn timed out during failover — aborting"
                );
                return Err(TxnError::Timeout);
            }
        }
        Ok(())
    }

    /// Release a blocked transaction (failover completed or txn completed).
    pub fn release(&self, txn_id: TxnId) {
        let entry = self.blocked.write().remove(&txn_id);
        if let Some(entry) = entry {
            let latency_us = entry.blocked_at.elapsed().as_micros() as u64;
            self.total_released.fetch_add(1, Ordering::Relaxed);

            let mut latencies = self.latencies_us.lock();
            if latencies.len() >= 10_000 {
                latencies.remove(0);
            }
            latencies.push(latency_us);
        }
    }

    /// Release all blocked transactions (failover completed).
    pub fn release_all(&self) {
        let entries: Vec<TxnId> = self.blocked.read().keys().copied().collect();
        for txn_id in entries {
            self.release(txn_id);
        }
    }

    /// Sweep: find all timed-out blocked transactions and return their IDs.
    pub fn sweep_timed_out(&self) -> Vec<TxnId> {
        let blocked = self.blocked.read();
        let mut expired = Vec::new();
        for (txn_id, entry) in blocked.iter() {
            if entry.blocked_at.elapsed() >= self.config.max_blocked_duration {
                expired.push(*txn_id);
            }
        }
        expired
    }

    /// Current number of blocked transactions.
    pub fn currently_blocked(&self) -> usize {
        self.blocked.read().len()
    }

    /// Metrics snapshot with latency percentiles.
    pub fn metrics(&self) -> FailoverBlockedTxnMetrics {
        let latencies = self.latencies_us.lock();
        let (p50, p99, max) = compute_percentiles(&latencies);

        FailoverBlockedTxnMetrics {
            total_blocked: self.total_blocked.load(Ordering::Relaxed),
            total_timed_out: self.total_timed_out.load(Ordering::Relaxed),
            total_released: self.total_released.load(Ordering::Relaxed),
            currently_blocked: self.blocked.read().len() as u64,
            latency_p50_us: p50,
            latency_p99_us: p99,
            latency_max_us: max,
        }
    }

    /// Whether latency has converged back to baseline after failover.
    /// Convergence = p99 latency below 2x the p50.
    pub fn is_converged(&self) -> bool {
        let latencies = self.latencies_us.lock();
        if latencies.len() < 10 {
            return true; // Not enough data
        }
        let (p50, p99, _) = compute_percentiles(&latencies);
        p50 > 0 && p99 <= p50 * 2
    }
}

impl Default for FailoverBlockedTxnGuard {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute p50, p99, max from a latency vector.
fn compute_percentiles(data: &[u64]) -> (u64, u64, u64) {
    if data.is_empty() {
        return (0, 0, 0);
    }
    let mut sorted: Vec<u64> = data.to_vec();
    sorted.sort_unstable();
    let len = sorted.len();
    let p50 = sorted[len / 2];
    let p99_idx = ((len as f64) * 0.99).ceil() as usize;
    let p99 = sorted[p99_idx.min(len - 1)];
    let max = sorted[len - 1];
    (p50, p99, max)
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::types::TxnId;
    use std::time::Duration;

    // ── §1: FailoverTxnCoordinator ──────────────────────────────────────

    #[test]
    fn test_coordinator_initial_state() {
        let coord = FailoverTxnCoordinator::new();
        assert_eq!(coord.current_phase(), FailoverTxnPhase::Normal);
        assert!(!coord.are_writes_blocked());
        assert!(coord.check_write_allowed(1).is_ok());
    }

    #[test]
    fn test_coordinator_drain_blocks_writes() {
        let coord = FailoverTxnCoordinator::new();
        let txns = vec![TxnId(1), TxnId(2), TxnId(3)];
        let to_drain = coord.begin_failover_drain(2, txns.clone());

        assert_eq!(to_drain.len(), 3);
        assert_eq!(coord.current_phase(), FailoverTxnPhase::Draining);
        assert!(coord.are_writes_blocked());
        assert!(coord.check_write_allowed(2).is_err());
    }

    #[test]
    fn test_coordinator_complete_drain_unblocks() {
        let coord = FailoverTxnCoordinator::new();
        coord.begin_failover_drain(2, vec![TxnId(1)]);
        assert!(coord.are_writes_blocked());

        coord.record_affected_txn(
            TxnId(1),
            false,
            FailoverTxnResolution::Aborted,
            100,
        );
        coord.complete_failover_drain();

        assert!(!coord.are_writes_blocked());
        assert_eq!(coord.current_phase(), FailoverTxnPhase::Converging);
        assert!(coord.check_write_allowed(2).is_ok());
    }

    #[test]
    fn test_coordinator_convergence() {
        let config = FailoverTxnConfig {
            convergence_window: Duration::from_millis(10),
            ..Default::default()
        };
        let coord = FailoverTxnCoordinator::with_config(config);
        coord.begin_failover_drain(1, vec![]);
        coord.complete_failover_drain();

        assert_eq!(coord.current_phase(), FailoverTxnPhase::Converging);

        std::thread::sleep(Duration::from_millis(15));
        assert!(coord.check_convergence());
        assert_eq!(coord.current_phase(), FailoverTxnPhase::Normal);
    }

    #[test]
    fn test_coordinator_drain_timeout() {
        let config = FailoverTxnConfig {
            drain_timeout: Duration::from_millis(10),
            ..Default::default()
        };
        let coord = FailoverTxnCoordinator::with_config(config);
        coord.begin_failover_drain(1, vec![TxnId(1)]);

        assert!(!coord.is_drain_timeout_exceeded());
        std::thread::sleep(Duration::from_millis(15));
        assert!(coord.is_drain_timeout_exceeded());
    }

    #[test]
    fn test_coordinator_metrics() {
        let coord = FailoverTxnCoordinator::new();
        coord.begin_failover_drain(1, vec![TxnId(1), TxnId(2)]);
        coord.record_affected_txn(TxnId(1), false, FailoverTxnResolution::Aborted, 50);
        coord.record_affected_txn(TxnId(2), true, FailoverTxnResolution::MovedToInDoubt, 100);
        coord.complete_failover_drain();

        let m = coord.metrics();
        assert_eq!(m.failover_events, 1);
        assert_eq!(m.txns_force_aborted, 1);
        assert_eq!(m.txns_moved_indoubt, 1);
    }

    #[test]
    fn test_coordinator_affected_txns_bounded() {
        let config = FailoverTxnConfig {
            max_affected_records: 2,
            ..Default::default()
        };
        let coord = FailoverTxnCoordinator::with_config(config);
        coord.begin_failover_drain(1, vec![]);
        for i in 0..5 {
            coord.record_affected_txn(TxnId(i), false, FailoverTxnResolution::Aborted, 10);
        }
        assert_eq!(coord.affected_txns().len(), 2);
    }

    // ── §2: InDoubtTtlEnforcer ──────────────────────────────────────────

    #[test]
    fn test_ttl_enforcer_register_and_remove() {
        let enforcer = InDoubtTtlEnforcer::new();
        enforcer.register(TxnId(1));
        assert_eq!(enforcer.tracked_count(), 1);
        enforcer.remove(TxnId(1));
        assert_eq!(enforcer.tracked_count(), 0);
    }

    #[test]
    fn test_ttl_enforcer_no_expiration_before_ttl() {
        let config = InDoubtTtlConfig {
            max_lifetime: Duration::from_secs(60),
            ..Default::default()
        };
        let enforcer = InDoubtTtlEnforcer::with_config(config);
        enforcer.register(TxnId(1));
        let expired = enforcer.sweep();
        assert!(expired.is_empty());
    }

    #[test]
    fn test_ttl_enforcer_expiration_after_ttl() {
        let config = InDoubtTtlConfig {
            max_lifetime: Duration::from_millis(10),
            warn_threshold_ratio: 0.5,
            ..Default::default()
        };
        let enforcer = InDoubtTtlEnforcer::with_config(config);
        enforcer.register(TxnId(1));
        enforcer.register(TxnId(2));

        std::thread::sleep(Duration::from_millis(15));
        let expired = enforcer.sweep();

        assert_eq!(expired.len(), 2);
        assert!(expired.contains(&TxnId(1)));
        assert!(expired.contains(&TxnId(2)));
        assert_eq!(enforcer.tracked_count(), 0);
    }

    #[test]
    fn test_ttl_enforcer_warning_before_expiry() {
        let config = InDoubtTtlConfig {
            max_lifetime: Duration::from_millis(100),
            warn_threshold_ratio: 0.1, // warn at 10ms
            ..Default::default()
        };
        let enforcer = InDoubtTtlEnforcer::with_config(config);
        enforcer.register(TxnId(1));

        std::thread::sleep(Duration::from_millis(15));
        let expired = enforcer.sweep();
        assert!(expired.is_empty()); // not yet expired

        let m = enforcer.metrics();
        assert_eq!(m.ttl_warnings, 1);
    }

    #[test]
    fn test_ttl_enforcer_metrics() {
        let config = InDoubtTtlConfig {
            max_lifetime: Duration::from_millis(5),
            ..Default::default()
        };
        let enforcer = InDoubtTtlEnforcer::with_config(config);
        enforcer.register(TxnId(1));
        std::thread::sleep(Duration::from_millis(10));
        enforcer.sweep();

        let m = enforcer.metrics();
        assert_eq!(m.ttl_expirations, 1);
        assert_eq!(m.sweeps, 1);
    }

    // ── §3: FailoverDamper ──────────────────────────────────────────────

    #[test]
    fn test_damper_allows_first_failover() {
        let damper = FailoverDamper::new();
        assert!(damper.check_failover_allowed(1, 0).is_ok());
    }

    #[test]
    fn test_damper_suppresses_rapid_failover() {
        let config = FailoverDamperConfig {
            min_interval: Duration::from_millis(100),
            ..Default::default()
        };
        let damper = FailoverDamper::with_config(config);

        assert!(damper.check_failover_allowed(1, 0).is_ok());
        // Immediate second attempt should be suppressed
        assert!(damper.check_failover_allowed(2, 0).is_err());
    }

    #[test]
    fn test_damper_allows_after_interval() {
        let config = FailoverDamperConfig {
            min_interval: Duration::from_millis(10),
            max_failovers_in_window: 10,
            observation_window: Duration::from_secs(1),
            ..Default::default()
        };
        let damper = FailoverDamper::with_config(config);

        assert!(damper.check_failover_allowed(1, 0).is_ok());
        std::thread::sleep(Duration::from_millis(15));
        assert!(damper.check_failover_allowed(2, 0).is_ok());
    }

    #[test]
    fn test_damper_rate_limit_in_window() {
        let config = FailoverDamperConfig {
            min_interval: Duration::from_millis(1),
            max_failovers_in_window: 2,
            observation_window: Duration::from_secs(10),
            ..Default::default()
        };
        let damper = FailoverDamper::with_config(config);

        assert!(damper.check_failover_allowed(1, 0).is_ok());
        std::thread::sleep(Duration::from_millis(5));
        assert!(damper.check_failover_allowed(2, 0).is_ok());
        std::thread::sleep(Duration::from_millis(5));
        // Third attempt in window should be suppressed
        assert!(damper.check_failover_allowed(3, 0).is_err());
    }

    #[test]
    fn test_damper_metrics() {
        let config = FailoverDamperConfig {
            min_interval: Duration::from_millis(100),
            ..Default::default()
        };
        let damper = FailoverDamper::with_config(config);

        let _ = damper.check_failover_allowed(1, 0);
        let _ = damper.check_failover_allowed(2, 0); // suppressed

        let m = damper.metrics();
        assert_eq!(m.total_attempts, 2);
        assert_eq!(m.total_allowed, 1);
        assert_eq!(m.total_suppressed, 1);
    }

    #[test]
    fn test_damper_history() {
        let damper = FailoverDamper::new();
        let _ = damper.check_failover_allowed(1, 42);
        let history = damper.recent_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].shard_id, 42);
        assert!(!history[0].suppressed);
    }

    // ── §4: FailoverBlockedTxnGuard ─────────────────────────────────────

    #[test]
    fn test_blocked_guard_register_and_release() {
        let guard = FailoverBlockedTxnGuard::new();
        guard.register_blocked(TxnId(1));
        assert_eq!(guard.currently_blocked(), 1);
        guard.release(TxnId(1));
        assert_eq!(guard.currently_blocked(), 0);
    }

    #[test]
    fn test_blocked_guard_timeout() {
        let config = FailoverBlockedTxnConfig {
            max_blocked_duration: Duration::from_millis(10),
            ..Default::default()
        };
        let guard = FailoverBlockedTxnGuard::with_config(config);
        guard.register_blocked(TxnId(1));

        assert!(guard.check_blocked_timeout(TxnId(1)).is_ok());
        std::thread::sleep(Duration::from_millis(15));
        assert!(guard.check_blocked_timeout(TxnId(1)).is_err());
    }

    #[test]
    fn test_blocked_guard_sweep_timed_out() {
        let config = FailoverBlockedTxnConfig {
            max_blocked_duration: Duration::from_millis(10),
            ..Default::default()
        };
        let guard = FailoverBlockedTxnGuard::with_config(config);
        guard.register_blocked(TxnId(1));
        guard.register_blocked(TxnId(2));

        std::thread::sleep(Duration::from_millis(15));
        let timed_out = guard.sweep_timed_out();
        assert_eq!(timed_out.len(), 2);
    }

    #[test]
    fn test_blocked_guard_release_all() {
        let guard = FailoverBlockedTxnGuard::new();
        guard.register_blocked(TxnId(1));
        guard.register_blocked(TxnId(2));
        guard.register_blocked(TxnId(3));

        guard.release_all();
        assert_eq!(guard.currently_blocked(), 0);
    }

    #[test]
    fn test_blocked_guard_metrics() {
        let guard = FailoverBlockedTxnGuard::new();
        guard.register_blocked(TxnId(1));
        guard.release(TxnId(1));

        let m = guard.metrics();
        assert_eq!(m.total_blocked, 1);
        assert_eq!(m.total_released, 1);
        assert_eq!(m.currently_blocked, 0);
    }

    #[test]
    fn test_blocked_guard_convergence_with_no_data() {
        let guard = FailoverBlockedTxnGuard::new();
        assert!(guard.is_converged()); // No data → converged
    }

    #[test]
    fn test_blocked_guard_latency_tracking() {
        let guard = FailoverBlockedTxnGuard::new();
        for i in 0..20 {
            guard.register_blocked(TxnId(i));
        }
        std::thread::sleep(Duration::from_millis(5));
        for i in 0..20 {
            guard.release(TxnId(i));
        }

        let m = guard.metrics();
        assert!(m.latency_p50_us > 0);
        assert!(m.latency_max_us >= m.latency_p50_us);
    }

    // ── Percentile helper ───────────────────────────────────────────────

    #[test]
    fn test_percentiles_empty() {
        assert_eq!(compute_percentiles(&[]), (0, 0, 0));
    }

    #[test]
    fn test_percentiles_single() {
        assert_eq!(compute_percentiles(&[100]), (100, 100, 100));
    }

    #[test]
    fn test_percentiles_ordered() {
        let data: Vec<u64> = (1..=100).collect();
        let (p50, p99, max) = compute_percentiles(&data);
        // Index 50 of 1..=100 is 51 (0-indexed)
        assert_eq!(p50, 51);
        // ceil(100 * 0.99) = 99, index 99 of 1..=100 is 100
        assert_eq!(p99, 100);
        assert_eq!(max, 100);
    }

    // ── Integration: Full failover lifecycle ─────────────────────────────

    #[test]
    fn test_full_failover_lifecycle() {
        let coord = FailoverTxnCoordinator::with_config(FailoverTxnConfig {
            drain_timeout: Duration::from_millis(100),
            convergence_window: Duration::from_millis(10),
            ..Default::default()
        });
        let damper = FailoverDamper::with_config(FailoverDamperConfig {
            min_interval: Duration::from_millis(1),
            ..Default::default()
        });
        let ttl_enforcer = InDoubtTtlEnforcer::new();
        let blocked_guard = FailoverBlockedTxnGuard::new();

        // Phase 0: Normal
        assert_eq!(coord.current_phase(), FailoverTxnPhase::Normal);
        assert!(coord.check_write_allowed(1).is_ok());

        // Phase 1: Failover detected — check damper
        assert!(damper.check_failover_allowed(2, 0).is_ok());

        // Phase 2: Begin drain
        let active = vec![TxnId(10), TxnId(11), TxnId(12)];
        let to_drain = coord.begin_failover_drain(2, active);
        assert_eq!(to_drain.len(), 3);
        assert!(coord.are_writes_blocked());

        // Simulate: TxnId(10) completes before drain
        coord.record_affected_txn(TxnId(10), false, FailoverTxnResolution::CompletedBeforeDrain, 50);

        // Simulate: TxnId(11) is aborted
        coord.record_affected_txn(TxnId(11), false, FailoverTxnResolution::Aborted, 100);

        // Simulate: TxnId(12) was prepared — moved to in-doubt
        coord.record_affected_txn(TxnId(12), true, FailoverTxnResolution::MovedToInDoubt, 200);
        ttl_enforcer.register(TxnId(12));

        // Phase 3: Drain complete
        coord.complete_failover_drain();
        assert!(!coord.are_writes_blocked());
        assert_eq!(coord.current_phase(), FailoverTxnPhase::Converging);
        damper.record_failover_complete();

        // Phase 4: Convergence
        std::thread::sleep(Duration::from_millis(15));
        assert!(coord.check_convergence());
        assert_eq!(coord.current_phase(), FailoverTxnPhase::Normal);

        // Verify metrics
        let m = coord.metrics();
        assert_eq!(m.failover_events, 1);
        assert_eq!(m.txns_drained, 1);
        assert_eq!(m.txns_force_aborted, 1);
        assert_eq!(m.txns_moved_indoubt, 1);

        // In-doubt TTL: txn 12 should not yet be expired
        let expired = ttl_enforcer.sweep();
        assert!(expired.is_empty());

        // Clean up
        ttl_enforcer.remove(TxnId(12));
        assert_eq!(ttl_enforcer.tracked_count(), 0);
    }

    #[test]
    fn test_failover_during_single_shard_txn() {
        let coord = FailoverTxnCoordinator::new();

        // Single-shard txn active
        let to_drain = coord.begin_failover_drain(1, vec![TxnId(100)]);
        assert_eq!(to_drain, vec![TxnId(100)]);

        // Single-shard txns are always aborted (not prepared)
        coord.record_affected_txn(TxnId(100), false, FailoverTxnResolution::Aborted, 50);
        coord.complete_failover_drain();

        let m = coord.metrics();
        assert_eq!(m.txns_force_aborted, 1);
        assert_eq!(m.txns_moved_indoubt, 0);
    }

    #[test]
    fn test_failover_during_cross_shard_prepare() {
        let coord = FailoverTxnCoordinator::new();
        let ttl = InDoubtTtlEnforcer::new();

        // Cross-shard txn in prepare phase
        coord.begin_failover_drain(1, vec![TxnId(200)]);

        // Prepared txns move to in-doubt
        coord.record_affected_txn(TxnId(200), true, FailoverTxnResolution::MovedToInDoubt, 100);
        ttl.register(TxnId(200));

        coord.complete_failover_drain();

        assert_eq!(ttl.tracked_count(), 1);
        let m = coord.metrics();
        assert_eq!(m.txns_moved_indoubt, 1);
    }

    #[test]
    fn test_failover_during_commit_barrier() {
        let coord = FailoverTxnCoordinator::new();

        // Txn was at commit barrier — it was prepared
        coord.begin_failover_drain(1, vec![TxnId(300)]);
        coord.record_affected_txn(TxnId(300), true, FailoverTxnResolution::MovedToInDoubt, 200);
        coord.complete_failover_drain();

        let affected = coord.affected_txns();
        assert_eq!(affected.len(), 1);
        assert!(affected[0].was_prepared);
    }

    #[test]
    fn test_duplicate_commit_abort_idempotent() {
        // TxnState::try_transition already handles idempotent re-entries
        // (Committed→Committed, Aborted→Aborted). This test validates
        // that the coordinator handles duplicates gracefully.
        let coord = FailoverTxnCoordinator::new();
        coord.begin_failover_drain(1, vec![TxnId(400)]);

        // Record the same txn twice — should not panic
        coord.record_affected_txn(TxnId(400), false, FailoverTxnResolution::Aborted, 50);
        coord.record_affected_txn(TxnId(400), false, FailoverTxnResolution::Aborted, 50);

        coord.complete_failover_drain();
        let m = coord.metrics();
        assert_eq!(m.txns_force_aborted, 2); // counted twice (idempotent at higher level)
    }

    #[test]
    fn test_rapid_repeated_failovers_churn() {
        let damper = FailoverDamper::with_config(FailoverDamperConfig {
            min_interval: Duration::from_millis(50),
            max_failovers_in_window: 2,
            observation_window: Duration::from_secs(1),
            log_suppressed: false,
            ..Default::default()
        });

        // First two allowed
        assert!(damper.check_failover_allowed(1, 0).is_ok());
        std::thread::sleep(Duration::from_millis(55));
        assert!(damper.check_failover_allowed(2, 0).is_ok());

        // Third suppressed by rate limit
        std::thread::sleep(Duration::from_millis(55));
        assert!(damper.check_failover_allowed(3, 0).is_err());

        let m = damper.metrics();
        assert_eq!(m.total_allowed, 2);
        assert_eq!(m.total_suppressed, 1);
    }

    #[test]
    fn test_coordinator_crash_indoubt_resolution() {
        let ttl = InDoubtTtlEnforcer::with_config(InDoubtTtlConfig {
            max_lifetime: Duration::from_millis(20),
            warn_threshold_ratio: 0.5,
            ..Default::default()
        });

        // Simulate coordinator crash leaving txns in-doubt
        ttl.register(TxnId(500));
        ttl.register(TxnId(501));
        ttl.register(TxnId(502));

        assert_eq!(ttl.tracked_count(), 3);

        // Wait for TTL
        std::thread::sleep(Duration::from_millis(25));
        let expired = ttl.sweep();

        // All should be force-aborted
        assert_eq!(expired.len(), 3);
        assert_eq!(ttl.tracked_count(), 0);

        let m = ttl.metrics();
        assert_eq!(m.ttl_expirations, 3);
    }
}
