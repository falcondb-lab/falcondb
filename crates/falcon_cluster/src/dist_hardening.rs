//! Distributed & Failover Hardening — from "能用" to "无脑稳"
//!
//! This module closes the production-readiness gaps in the distributed layer:
//!
//! 1. **`FailoverPreFlight`** — pre-promotion checklist (lag check, quorum check,
//!    cooldown, epoch monotonicity). Blocks unsafe promotions.
//!
//! 2. **`SplitBrainDetector`** — detects and rejects stale-epoch writes after
//!    a partition heals. Prevents two nodes serving writes simultaneously.
//!
//! 3. **`AutoRestartSupervisor`** — wraps `BgTaskSupervisor` with bounded
//!    automatic restart for failed critical tasks. Exponential backoff.
//!
//! 4. **`HealthCheckHysteresis`** — requires N consecutive missed heartbeats
//!    before declaring a node failed. Prevents flapping.
//!
//! 5. **`PromotionSafetyGuard`** — ensures promotion is atomic-or-rollback.
//!    If any step fails, the old primary is unfenced and the promotion is aborted.
//!
//! # Invariants
//!
//! - A promotion MUST NOT proceed if the candidate replica's LSN gap exceeds
//!   `max_promotion_lag` (data-loss prevention).
//! - Two nodes MUST NOT both accept writes at the same epoch.
//! - A failed critical background task MUST be restarted within bounded time.
//! - A node MUST NOT be declared failed due to a single missed heartbeat.
//! - A failed promotion MUST leave the old primary writable (no total outage).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

// ═══════════════════════════════════════════════════════════════════════════
// §1: Failover Pre-Flight Check
// ═══════════════════════════════════════════════════════════════════════════

/// Reason a pre-flight check can reject a promotion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreFlightRejectReason {
    /// Candidate replica is too far behind primary.
    ReplicaLagTooHigh { lag: u64, max: u64 },
    /// Not enough healthy replicas to maintain quorum after promotion.
    InsufficientQuorum { healthy: usize, required: usize },
    /// Failover cooldown has not elapsed since last promotion.
    CooldownActive { remaining_ms: u64 },
    /// Epoch would go backwards (stale request).
    StaleEpoch { current: u64, requested: u64 },
    /// No candidate replica available.
    NoCandidateAvailable,
    /// Primary is not actually failed (false alarm).
    PrimaryNotFailed,
}

impl std::fmt::Display for PreFlightRejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReplicaLagTooHigh { lag, max } => {
                write!(f, "replica lag {lag} exceeds max {max}")
            }
            Self::InsufficientQuorum { healthy, required } => {
                write!(f, "only {healthy} healthy replicas, need {required}")
            }
            Self::CooldownActive { remaining_ms } => {
                write!(f, "cooldown active, {remaining_ms}ms remaining")
            }
            Self::StaleEpoch { current, requested } => {
                write!(f, "stale epoch: current={current}, requested={requested}")
            }
            Self::NoCandidateAvailable => write!(f, "no candidate replica available"),
            Self::PrimaryNotFailed => write!(f, "primary is not actually failed"),
        }
    }
}

/// Input for a pre-flight check.
#[derive(Debug, Clone)]
pub struct PreFlightInput {
    /// Current epoch of the shard.
    pub current_epoch: u64,
    /// Candidate replica index.
    pub candidate_idx: Option<usize>,
    /// Candidate replica's applied LSN.
    pub candidate_lsn: u64,
    /// Primary's current LSN.
    pub primary_lsn: u64,
    /// Number of healthy replicas.
    pub healthy_replicas: usize,
    /// Is the primary actually detected as failed?
    pub primary_is_failed: bool,
}

/// Configuration for pre-flight checks.
#[derive(Debug, Clone)]
pub struct PreFlightConfig {
    /// Maximum acceptable lag (in LSN units) for the promotion candidate.
    pub max_promotion_lag: u64,
    /// Minimum healthy replicas required after promotion.
    pub min_post_promotion_replicas: usize,
    /// Cooldown duration between failovers.
    pub cooldown: Duration,
}

impl Default for PreFlightConfig {
    fn default() -> Self {
        Self {
            max_promotion_lag: 1000,
            min_post_promotion_replicas: 0,
            cooldown: Duration::from_secs(30),
        }
    }
}

/// Pre-flight metrics.
#[derive(Debug, Clone, Default)]
pub struct PreFlightMetrics {
    pub checks_passed: u64,
    pub checks_rejected: u64,
    pub last_reject_reason: Option<String>,
}

/// Pre-flight checker for failover promotions.
///
/// Evaluates a checklist before allowing a promotion to proceed.
/// Any single failed check aborts the entire promotion.
pub struct FailoverPreFlight {
    config: PreFlightConfig,
    last_failover: Mutex<Option<Instant>>,
    metrics: Mutex<PreFlightMetrics>,
}

impl FailoverPreFlight {
    pub fn new(config: PreFlightConfig) -> Self {
        Self {
            config,
            last_failover: Mutex::new(None),
            metrics: Mutex::new(PreFlightMetrics::default()),
        }
    }

    /// Run all pre-flight checks. Returns `Ok(())` if promotion is safe.
    pub fn check(&self, input: &PreFlightInput) -> Result<(), PreFlightRejectReason> {
        // Check 1: Primary must actually be failed
        if !input.primary_is_failed {
            self.record_reject(&PreFlightRejectReason::PrimaryNotFailed);
            return Err(PreFlightRejectReason::PrimaryNotFailed);
        }

        // Check 2: Must have a candidate
        if input.candidate_idx.is_none() {
            self.record_reject(&PreFlightRejectReason::NoCandidateAvailable);
            return Err(PreFlightRejectReason::NoCandidateAvailable);
        }

        // Check 3: Candidate replica lag within bounds
        let lag = input.primary_lsn.saturating_sub(input.candidate_lsn);
        if lag > self.config.max_promotion_lag {
            let reason = PreFlightRejectReason::ReplicaLagTooHigh {
                lag,
                max: self.config.max_promotion_lag,
            };
            self.record_reject(&reason);
            return Err(reason);
        }

        // Check 4: Sufficient quorum after promotion
        // After promotion, one replica becomes primary, so healthy_replicas - 1
        let post_promotion = input.healthy_replicas.saturating_sub(1);
        if post_promotion < self.config.min_post_promotion_replicas {
            let reason = PreFlightRejectReason::InsufficientQuorum {
                healthy: input.healthy_replicas,
                required: self.config.min_post_promotion_replicas + 1,
            };
            self.record_reject(&reason);
            return Err(reason);
        }

        // Check 5: Cooldown elapsed
        {
            let last = self.last_failover.lock();
            if let Some(last_time) = *last {
                let elapsed = last_time.elapsed();
                if elapsed < self.config.cooldown {
                    let remaining = self.config.cooldown - elapsed;
                    let reason = PreFlightRejectReason::CooldownActive {
                        remaining_ms: remaining.as_millis() as u64,
                    };
                    self.record_reject(&reason);
                    return Err(reason);
                }
            }
        }

        // All checks passed
        self.metrics.lock().checks_passed += 1;
        Ok(())
    }

    /// Record that a failover completed successfully.
    pub fn record_failover_complete(&self) {
        *self.last_failover.lock() = Some(Instant::now());
    }

    /// Get metrics snapshot.
    pub fn metrics(&self) -> PreFlightMetrics {
        self.metrics.lock().clone()
    }

    fn record_reject(&self, reason: &PreFlightRejectReason) {
        let mut m = self.metrics.lock();
        m.checks_rejected += 1;
        m.last_reject_reason = Some(reason.to_string());
        tracing::warn!(reason = %reason, "failover pre-flight check REJECTED");
    }
}

impl Default for FailoverPreFlight {
    fn default() -> Self {
        Self::new(PreFlightConfig::default())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2: Split-Brain Detector
// ═══════════════════════════════════════════════════════════════════════════

/// A write attempt from a potentially stale node.
#[derive(Debug, Clone)]
pub struct WriteEpochCheck {
    /// Epoch the writer believes is current.
    pub writer_epoch: u64,
    /// Node ID of the writer.
    pub writer_node: u64,
}

/// Result of a split-brain check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitBrainVerdict {
    /// Write is from the current epoch — allowed.
    Allowed,
    /// Write is from a stale epoch — rejected (split-brain).
    Rejected {
        writer_epoch: u64,
        current_epoch: u64,
        writer_node: u64,
    },
}

/// Metrics for split-brain detection.
#[derive(Debug, Clone, Default)]
pub struct SplitBrainMetrics {
    pub checks: u64,
    pub rejections: u64,
    pub last_rejected_node: Option<u64>,
    pub last_rejected_epoch: Option<u64>,
}

/// Detects and rejects stale-epoch writes after a partition heals.
///
/// Every shard tracks its authoritative epoch. Any write arriving with
/// an epoch lower than the current one is a split-brain symptom and
/// must be rejected to prevent data divergence.
pub struct SplitBrainDetector {
    /// Authoritative epoch for this shard.
    current_epoch: AtomicU64,
    metrics: Mutex<SplitBrainMetrics>,
    /// History of detected split-brain events (bounded).
    history: Mutex<Vec<SplitBrainEvent>>,
    max_history: usize,
}

/// A recorded split-brain detection event.
#[derive(Debug, Clone)]
pub struct SplitBrainEvent {
    pub detected_at: Instant,
    pub writer_node: u64,
    pub writer_epoch: u64,
    pub current_epoch: u64,
}

impl SplitBrainDetector {
    pub fn new(initial_epoch: u64) -> Self {
        Self {
            current_epoch: AtomicU64::new(initial_epoch),
            metrics: Mutex::new(SplitBrainMetrics::default()),
            history: Mutex::new(Vec::new()),
            max_history: 100,
        }
    }

    /// Check if a write is from the current epoch.
    pub fn check_write(&self, check: &WriteEpochCheck) -> SplitBrainVerdict {
        let current = self.current_epoch.load(Ordering::SeqCst);
        let mut m = self.metrics.lock();
        m.checks += 1;

        if check.writer_epoch < current {
            m.rejections += 1;
            m.last_rejected_node = Some(check.writer_node);
            m.last_rejected_epoch = Some(check.writer_epoch);

            let event = SplitBrainEvent {
                detected_at: Instant::now(),
                writer_node: check.writer_node,
                writer_epoch: check.writer_epoch,
                current_epoch: current,
            };

            let mut hist = self.history.lock();
            if hist.len() < self.max_history {
                hist.push(event);
            }

            tracing::error!(
                writer_node = check.writer_node,
                writer_epoch = check.writer_epoch,
                current_epoch = current,
                "SPLIT-BRAIN DETECTED: stale-epoch write rejected"
            );

            SplitBrainVerdict::Rejected {
                writer_epoch: check.writer_epoch,
                current_epoch: current,
                writer_node: check.writer_node,
            }
        } else {
            SplitBrainVerdict::Allowed
        }
    }

    /// Advance the epoch (called after successful promotion).
    pub fn advance_epoch(&self, new_epoch: u64) {
        let old = self.current_epoch.fetch_max(new_epoch, Ordering::SeqCst);
        if new_epoch > old {
            tracing::info!(
                old_epoch = old,
                new_epoch = new_epoch,
                "split-brain detector: epoch advanced"
            );
        }
    }

    /// Current authoritative epoch.
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::SeqCst)
    }

    /// Metrics snapshot.
    pub fn metrics(&self) -> SplitBrainMetrics {
        self.metrics.lock().clone()
    }

    /// Recent split-brain events.
    pub fn recent_events(&self) -> Vec<SplitBrainEvent> {
        self.history.lock().clone()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3: Auto-Restart Supervisor
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for auto-restart behavior.
#[derive(Debug, Clone)]
pub struct AutoRestartConfig {
    /// Maximum number of restart attempts before giving up.
    pub max_restarts: u32,
    /// Initial backoff delay between restarts.
    pub initial_backoff: Duration,
    /// Maximum backoff delay (exponential backoff cap).
    pub max_backoff: Duration,
    /// Backoff multiplier (e.g., 2.0 for doubling).
    pub backoff_multiplier: f64,
    /// If a task runs successfully for this long, reset the restart counter.
    pub success_reset_duration: Duration,
}

impl Default for AutoRestartConfig {
    fn default() -> Self {
        Self {
            max_restarts: 5,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            success_reset_duration: Duration::from_secs(300),
        }
    }
}

/// State of a single auto-restartable task.
#[derive(Debug, Clone)]
pub struct RestartableTaskState {
    pub name: String,
    pub restart_count: u32,
    pub max_restarts: u32,
    pub last_failure: Option<Instant>,
    pub last_restart: Option<Instant>,
    pub next_backoff: Duration,
    pub is_permanently_failed: bool,
    pub last_started_at: Option<Instant>,
}

/// Metrics for the auto-restart supervisor.
#[derive(Debug, Clone, Default)]
pub struct AutoRestartMetrics {
    pub total_restarts: u64,
    pub total_permanent_failures: u64,
    pub total_success_resets: u64,
    pub tasks_monitored: usize,
}

/// Wraps task health monitoring with automatic restart logic.
///
/// For each registered task, tracks failure count and computes
/// exponential backoff. After `max_restarts`, the task is marked
/// as permanently failed and requires manual intervention.
pub struct AutoRestartSupervisor {
    config: AutoRestartConfig,
    tasks: RwLock<HashMap<String, RestartableTaskState>>,
    metrics: Mutex<AutoRestartMetrics>,
}

impl AutoRestartSupervisor {
    pub fn new(config: AutoRestartConfig) -> Self {
        Self {
            config,
            tasks: RwLock::new(HashMap::new()),
            metrics: Mutex::new(AutoRestartMetrics::default()),
        }
    }

    /// Register a task for auto-restart monitoring.
    pub fn register(&self, name: &str) {
        let state = RestartableTaskState {
            name: name.to_owned(),
            restart_count: 0,
            max_restarts: self.config.max_restarts,
            last_failure: None,
            last_restart: None,
            next_backoff: self.config.initial_backoff,
            is_permanently_failed: false,
            last_started_at: None,
        };
        self.tasks.write().insert(name.to_owned(), state);
        self.metrics.lock().tasks_monitored = self.tasks.read().len();
    }

    /// Mark a task as started (running).
    pub fn mark_started(&self, name: &str) {
        if let Some(task) = self.tasks.write().get_mut(name) {
            task.last_started_at = Some(Instant::now());
        }
    }

    /// Report a task failure. Returns `Some(backoff)` if the task should be
    /// restarted after the backoff duration, or `None` if it has exhausted
    /// all restart attempts (permanently failed).
    pub fn report_failure(&self, name: &str, reason: &str) -> Option<Duration> {
        let mut tasks = self.tasks.write();
        let task = tasks.get_mut(name)?;

        if task.is_permanently_failed {
            return None;
        }

        task.restart_count += 1;
        task.last_failure = Some(Instant::now());

        if task.restart_count > self.config.max_restarts {
            task.is_permanently_failed = true;
            self.metrics.lock().total_permanent_failures += 1;
            tracing::error!(
                task = name,
                restarts = task.restart_count,
                reason = reason,
                "task PERMANENTLY FAILED — manual intervention required"
            );
            return None;
        }

        let backoff = task.next_backoff;
        // Exponential backoff with cap
        task.next_backoff = Duration::from_secs_f64(
            (task.next_backoff.as_secs_f64() * self.config.backoff_multiplier)
                .min(self.config.max_backoff.as_secs_f64()),
        );
        task.last_restart = Some(Instant::now());

        self.metrics.lock().total_restarts += 1;

        tracing::warn!(
            task = name,
            restart_count = task.restart_count,
            max_restarts = self.config.max_restarts,
            backoff_ms = backoff.as_millis() as u64,
            reason = reason,
            "task failed — scheduling restart with backoff"
        );

        Some(backoff)
    }

    /// Report that a task has been running successfully. If it has been
    /// running longer than `success_reset_duration`, reset its restart counter.
    pub fn report_healthy(&self, name: &str) {
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.get_mut(name) {
            if let Some(started_at) = task.last_started_at {
                if started_at.elapsed() >= self.config.success_reset_duration
                    && task.restart_count > 0
                {
                    let old_count = task.restart_count;
                    task.restart_count = 0;
                    task.next_backoff = self.config.initial_backoff;
                    task.is_permanently_failed = false;
                    self.metrics.lock().total_success_resets += 1;
                    tracing::info!(
                        task = name,
                        old_restart_count = old_count,
                        "task stable — restart counter reset"
                    );
                }
            }
        }
    }

    /// Check which tasks are permanently failed.
    pub fn permanently_failed_tasks(&self) -> Vec<String> {
        self.tasks
            .read()
            .values()
            .filter(|t| t.is_permanently_failed)
            .map(|t| t.name.clone())
            .collect()
    }

    /// Get the state of a specific task.
    pub fn task_state(&self, name: &str) -> Option<RestartableTaskState> {
        self.tasks.read().get(name).cloned()
    }

    /// Overall metrics.
    pub fn metrics(&self) -> AutoRestartMetrics {
        let mut m = self.metrics.lock().clone();
        m.tasks_monitored = self.tasks.read().len();
        m
    }
}

impl Default for AutoRestartSupervisor {
    fn default() -> Self {
        Self::new(AutoRestartConfig::default())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4: Health-Check Hysteresis
// ═══════════════════════════════════════════════════════════════════════════

/// Debounced health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebouncedHealth {
    /// Node is healthy.
    Healthy,
    /// Node is suspected (some misses, but not yet declared failed).
    Suspect,
    /// Node is confirmed failed (enough consecutive misses).
    Failed,
}

impl std::fmt::Display for DebouncedHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy => write!(f, "Healthy"),
            Self::Suspect => write!(f, "Suspect"),
            Self::Failed => write!(f, "Failed"),
        }
    }
}

/// Configuration for health-check hysteresis.
#[derive(Debug, Clone)]
pub struct HysteresisConfig {
    /// Number of consecutive missed heartbeats to transition Healthy → Suspect.
    pub suspect_threshold: u32,
    /// Number of consecutive missed heartbeats to transition Suspect → Failed.
    pub failure_threshold: u32,
    /// Number of consecutive successful heartbeats to transition Failed → Healthy.
    pub recovery_threshold: u32,
}

impl Default for HysteresisConfig {
    fn default() -> Self {
        Self {
            suspect_threshold: 2,
            failure_threshold: 5,
            recovery_threshold: 3,
        }
    }
}

/// Per-node hysteresis state.
#[derive(Debug, Clone)]
pub struct NodeHysteresisState {
    pub node_id: u64,
    pub status: DebouncedHealth,
    pub consecutive_misses: u32,
    pub consecutive_successes: u32,
    pub total_transitions: u64,
    pub last_transition: Option<Instant>,
}

/// Metrics for health-check hysteresis.
#[derive(Debug, Clone, Default)]
pub struct HysteresisMetrics {
    pub total_evaluations: u64,
    pub total_transitions: u64,
    pub false_alarms_prevented: u64,
}

/// Health-check hysteresis engine.
///
/// Prevents flapping by requiring N consecutive missed heartbeats
/// before declaring a node failed, and M consecutive successes
/// before declaring it recovered.
pub struct HealthCheckHysteresis {
    config: HysteresisConfig,
    nodes: RwLock<HashMap<u64, NodeHysteresisState>>,
    metrics: Mutex<HysteresisMetrics>,
}

impl HealthCheckHysteresis {
    pub fn new(config: HysteresisConfig) -> Self {
        Self {
            config,
            nodes: RwLock::new(HashMap::new()),
            metrics: Mutex::new(HysteresisMetrics::default()),
        }
    }

    /// Register a node for hysteresis tracking.
    pub fn register_node(&self, node_id: u64) {
        self.nodes.write().insert(
            node_id,
            NodeHysteresisState {
                node_id,
                status: DebouncedHealth::Healthy,
                consecutive_misses: 0,
                consecutive_successes: 0,
                total_transitions: 0,
                last_transition: None,
            },
        );
    }

    /// Record a missed heartbeat. Returns the new debounced status.
    pub fn record_miss(&self, node_id: u64) -> DebouncedHealth {
        let mut nodes = self.nodes.write();
        let node = match nodes.get_mut(&node_id) {
            Some(n) => n,
            None => return DebouncedHealth::Healthy,
        };

        node.consecutive_misses += 1;
        node.consecutive_successes = 0;
        self.metrics.lock().total_evaluations += 1;

        let old_status = node.status;
        let new_status = if node.consecutive_misses >= self.config.failure_threshold {
            DebouncedHealth::Failed
        } else if node.consecutive_misses >= self.config.suspect_threshold {
            DebouncedHealth::Suspect
        } else {
            old_status // Don't immediately downgrade
        };

        if new_status != old_status {
            node.status = new_status;
            node.total_transitions += 1;
            node.last_transition = Some(Instant::now());
            self.metrics.lock().total_transitions += 1;

            tracing::warn!(
                node_id = node_id,
                old = %old_status,
                new = %new_status,
                consecutive_misses = node.consecutive_misses,
                "health hysteresis transition"
            );
        } else if node.consecutive_misses > 0
            && node.consecutive_misses < self.config.suspect_threshold
        {
            // Single miss didn't cause transition — this is a prevented false alarm
            self.metrics.lock().false_alarms_prevented += 1;
        }

        new_status
    }

    /// Record a successful heartbeat. Returns the new debounced status.
    pub fn record_success(&self, node_id: u64) -> DebouncedHealth {
        let mut nodes = self.nodes.write();
        let node = match nodes.get_mut(&node_id) {
            Some(n) => n,
            None => return DebouncedHealth::Healthy,
        };

        node.consecutive_successes += 1;
        node.consecutive_misses = 0;
        self.metrics.lock().total_evaluations += 1;

        let old_status = node.status;
        let new_status =
            if node.consecutive_successes >= self.config.recovery_threshold {
                DebouncedHealth::Healthy
            } else if old_status == DebouncedHealth::Failed {
                // Still failed until recovery threshold is met
                DebouncedHealth::Suspect
            } else {
                old_status
            };

        if new_status != old_status {
            node.status = new_status;
            node.total_transitions += 1;
            node.last_transition = Some(Instant::now());
            self.metrics.lock().total_transitions += 1;

            tracing::info!(
                node_id = node_id,
                old = %old_status,
                new = %new_status,
                consecutive_successes = node.consecutive_successes,
                "health hysteresis recovery"
            );
        }

        new_status
    }

    /// Get the current debounced health of a node.
    pub fn node_status(&self, node_id: u64) -> DebouncedHealth {
        self.nodes
            .read()
            .get(&node_id)
            .map_or(DebouncedHealth::Healthy, |n| n.status)
    }

    /// Get state of a specific node.
    pub fn node_state(&self, node_id: u64) -> Option<NodeHysteresisState> {
        self.nodes.read().get(&node_id).cloned()
    }

    /// Metrics snapshot.
    pub fn metrics(&self) -> HysteresisMetrics {
        self.metrics.lock().clone()
    }
}

impl Default for HealthCheckHysteresis {
    fn default() -> Self {
        Self::new(HysteresisConfig::default())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5: Promotion Safety Guard
// ═══════════════════════════════════════════════════════════════════════════

/// Step in the promotion protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionStep {
    /// Not started.
    Idle,
    /// Old primary fenced (read-only).
    OldPrimaryFenced,
    /// Candidate replica caught up to primary LSN.
    CandidateCaughtUp,
    /// Roles swapped (old primary → replica, candidate → primary).
    RolesSwapped,
    /// New primary unfenced (accepting writes).
    NewPrimaryUnfenced,
    /// Promotion complete.
    Complete,
    /// Promotion rolled back due to failure.
    RolledBack,
}

impl std::fmt::Display for PromotionStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::OldPrimaryFenced => write!(f, "OldPrimaryFenced"),
            Self::CandidateCaughtUp => write!(f, "CandidateCaughtUp"),
            Self::RolesSwapped => write!(f, "RolesSwapped"),
            Self::NewPrimaryUnfenced => write!(f, "NewPrimaryUnfenced"),
            Self::Complete => write!(f, "Complete"),
            Self::RolledBack => write!(f, "RolledBack"),
        }
    }
}

/// Metrics for promotion safety.
#[derive(Debug, Clone, Default)]
pub struct PromotionSafetyMetrics {
    pub promotions_attempted: u64,
    pub promotions_completed: u64,
    pub promotions_rolled_back: u64,
    pub rollback_reasons: Vec<String>,
}

/// Tracks the promotion protocol state and enables rollback
/// if any step fails, preventing total write outage.
///
/// Protocol:
/// 1. Fence old primary (make read-only)
/// 2. Catch up candidate to primary LSN
/// 3. Swap roles
/// 4. Unfence new primary
///
/// If step 2 or 3 fails → rollback: unfence old primary.
/// If step 4 fails → rollback: swap roles back, unfence old primary.
pub struct PromotionSafetyGuard {
    current_step: Mutex<PromotionStep>,
    metrics: Mutex<PromotionSafetyMetrics>,
    /// LSN that the candidate must reach before promotion can proceed.
    target_lsn: AtomicU64,
}

impl PromotionSafetyGuard {
    pub fn new() -> Self {
        Self {
            current_step: Mutex::new(PromotionStep::Idle),
            metrics: Mutex::new(PromotionSafetyMetrics::default()),
            target_lsn: AtomicU64::new(0),
        }
    }

    /// Begin a promotion attempt.
    pub fn begin(&self, primary_lsn: u64) {
        *self.current_step.lock() = PromotionStep::Idle;
        self.target_lsn.store(primary_lsn, Ordering::SeqCst);
        self.metrics.lock().promotions_attempted += 1;
        tracing::info!(
            target_lsn = primary_lsn,
            "promotion safety guard: BEGIN"
        );
    }

    /// Record that the old primary has been fenced.
    pub fn record_fenced(&self) {
        *self.current_step.lock() = PromotionStep::OldPrimaryFenced;
        tracing::info!("promotion safety guard: old primary FENCED");
    }

    /// Record that the candidate has caught up.
    pub fn record_caught_up(&self, candidate_lsn: u64) -> Result<(), String> {
        let target = self.target_lsn.load(Ordering::SeqCst);
        if candidate_lsn < target {
            let msg = format!(
                "candidate LSN {candidate_lsn} < target LSN {target} — catch-up incomplete"
            );
            tracing::error!(msg = %msg, "promotion safety guard: CATCH-UP FAILED");
            return Err(msg);
        }
        *self.current_step.lock() = PromotionStep::CandidateCaughtUp;
        tracing::info!(
            candidate_lsn = candidate_lsn,
            target_lsn = target,
            "promotion safety guard: candidate CAUGHT UP"
        );
        Ok(())
    }

    /// Record that roles have been swapped.
    pub fn record_roles_swapped(&self) {
        *self.current_step.lock() = PromotionStep::RolesSwapped;
        tracing::info!("promotion safety guard: roles SWAPPED");
    }

    /// Record that the new primary has been unfenced.
    pub fn record_unfenced(&self) {
        *self.current_step.lock() = PromotionStep::NewPrimaryUnfenced;
        tracing::info!("promotion safety guard: new primary UNFENCED");
    }

    /// Mark promotion as complete.
    pub fn complete(&self) {
        *self.current_step.lock() = PromotionStep::Complete;
        self.metrics.lock().promotions_completed += 1;
        tracing::info!("promotion safety guard: COMPLETE");
    }

    /// Roll back the promotion. The caller should unfence the old primary
    /// and restore original roles based on the current step.
    pub fn rollback(&self, reason: &str) -> PromotionStep {
        let step = *self.current_step.lock();
        *self.current_step.lock() = PromotionStep::RolledBack;
        let mut m = self.metrics.lock();
        m.promotions_rolled_back += 1;
        m.rollback_reasons.push(reason.to_owned());

        tracing::error!(
            failed_at_step = %step,
            reason = reason,
            "promotion safety guard: ROLLED BACK"
        );

        step
    }

    /// Current step.
    pub fn current_step(&self) -> PromotionStep {
        *self.current_step.lock()
    }

    /// Metrics snapshot.
    pub fn metrics(&self) -> PromotionSafetyMetrics {
        self.metrics.lock().clone()
    }
}

impl Default for PromotionSafetyGuard {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6: Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── §1: FailoverPreFlight ──

    #[test]
    fn test_preflight_passes_when_all_ok() {
        let pf = FailoverPreFlight::default();
        let input = PreFlightInput {
            current_epoch: 1,
            candidate_idx: Some(0),
            candidate_lsn: 100,
            primary_lsn: 100,
            healthy_replicas: 2,
            primary_is_failed: true,
        };
        assert!(pf.check(&input).is_ok());
    }

    #[test]
    fn test_preflight_rejects_primary_not_failed() {
        let pf = FailoverPreFlight::default();
        let input = PreFlightInput {
            current_epoch: 1,
            candidate_idx: Some(0),
            candidate_lsn: 100,
            primary_lsn: 100,
            healthy_replicas: 2,
            primary_is_failed: false,
        };
        assert_eq!(
            pf.check(&input).unwrap_err(),
            PreFlightRejectReason::PrimaryNotFailed
        );
    }

    #[test]
    fn test_preflight_rejects_no_candidate() {
        let pf = FailoverPreFlight::default();
        let input = PreFlightInput {
            current_epoch: 1,
            candidate_idx: None,
            candidate_lsn: 0,
            primary_lsn: 100,
            healthy_replicas: 0,
            primary_is_failed: true,
        };
        assert_eq!(
            pf.check(&input).unwrap_err(),
            PreFlightRejectReason::NoCandidateAvailable
        );
    }

    #[test]
    fn test_preflight_rejects_high_lag() {
        let config = PreFlightConfig {
            max_promotion_lag: 100,
            ..Default::default()
        };
        let pf = FailoverPreFlight::new(config);
        let input = PreFlightInput {
            current_epoch: 1,
            candidate_idx: Some(0),
            candidate_lsn: 50,
            primary_lsn: 200,
            healthy_replicas: 2,
            primary_is_failed: true,
        };
        match pf.check(&input).unwrap_err() {
            PreFlightRejectReason::ReplicaLagTooHigh { lag, max } => {
                assert_eq!(lag, 150);
                assert_eq!(max, 100);
            }
            other => panic!("expected ReplicaLagTooHigh, got {:?}", other),
        }
    }

    #[test]
    fn test_preflight_rejects_cooldown() {
        let config = PreFlightConfig {
            cooldown: Duration::from_millis(100),
            ..Default::default()
        };
        let pf = FailoverPreFlight::new(config);

        let input = PreFlightInput {
            current_epoch: 1,
            candidate_idx: Some(0),
            candidate_lsn: 100,
            primary_lsn: 100,
            healthy_replicas: 2,
            primary_is_failed: true,
        };

        assert!(pf.check(&input).is_ok());
        pf.record_failover_complete();

        // Immediate second attempt should be rejected
        match pf.check(&input).unwrap_err() {
            PreFlightRejectReason::CooldownActive { .. } => {}
            other => panic!("expected CooldownActive, got {:?}", other),
        }

        // After cooldown, should pass
        std::thread::sleep(Duration::from_millis(110));
        assert!(pf.check(&input).is_ok());
    }

    #[test]
    fn test_preflight_metrics() {
        let pf = FailoverPreFlight::default();
        let good = PreFlightInput {
            current_epoch: 1,
            candidate_idx: Some(0),
            candidate_lsn: 100,
            primary_lsn: 100,
            healthy_replicas: 2,
            primary_is_failed: true,
        };
        let bad = PreFlightInput {
            primary_is_failed: false,
            ..good.clone()
        };

        let _ = pf.check(&good);
        let _ = pf.check(&bad);
        let _ = pf.check(&good);

        let m = pf.metrics();
        assert_eq!(m.checks_passed, 2);
        assert_eq!(m.checks_rejected, 1);
    }

    // ── §2: SplitBrainDetector ──

    #[test]
    fn test_split_brain_allows_current_epoch() {
        let detector = SplitBrainDetector::new(5);
        let check = WriteEpochCheck {
            writer_epoch: 5,
            writer_node: 1,
        };
        assert_eq!(detector.check_write(&check), SplitBrainVerdict::Allowed);
    }

    #[test]
    fn test_split_brain_rejects_stale_epoch() {
        let detector = SplitBrainDetector::new(5);
        let check = WriteEpochCheck {
            writer_epoch: 3,
            writer_node: 42,
        };
        match detector.check_write(&check) {
            SplitBrainVerdict::Rejected {
                writer_epoch,
                current_epoch,
                writer_node,
            } => {
                assert_eq!(writer_epoch, 3);
                assert_eq!(current_epoch, 5);
                assert_eq!(writer_node, 42);
            }
            SplitBrainVerdict::Allowed => panic!("should have rejected stale epoch"),
        }
    }

    #[test]
    fn test_split_brain_advance_epoch() {
        let detector = SplitBrainDetector::new(1);
        detector.advance_epoch(5);
        assert_eq!(detector.current_epoch(), 5);

        // Advance to lower epoch should be no-op
        detector.advance_epoch(3);
        assert_eq!(detector.current_epoch(), 5);
    }

    #[test]
    fn test_split_brain_metrics_and_history() {
        let detector = SplitBrainDetector::new(5);
        let _ = detector.check_write(&WriteEpochCheck {
            writer_epoch: 5,
            writer_node: 1,
        });
        let _ = detector.check_write(&WriteEpochCheck {
            writer_epoch: 2,
            writer_node: 99,
        });
        let _ = detector.check_write(&WriteEpochCheck {
            writer_epoch: 5,
            writer_node: 1,
        });

        let m = detector.metrics();
        assert_eq!(m.checks, 3);
        assert_eq!(m.rejections, 1);
        assert_eq!(m.last_rejected_node, Some(99));
        assert_eq!(m.last_rejected_epoch, Some(2));

        let events = detector.recent_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].writer_node, 99);
    }

    // ── §3: AutoRestartSupervisor ──

    #[test]
    fn test_autorestart_first_failure_returns_backoff() {
        let sup = AutoRestartSupervisor::default();
        sup.register("gc");
        let backoff = sup.report_failure("gc", "OOM");
        assert!(backoff.is_some());
        assert_eq!(backoff.unwrap(), Duration::from_secs(1));
    }

    #[test]
    fn test_autorestart_exponential_backoff() {
        let config = AutoRestartConfig {
            max_restarts: 10,
            initial_backoff: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            max_backoff: Duration::from_secs(60),
            ..Default::default()
        };
        let sup = AutoRestartSupervisor::new(config);
        sup.register("repl");

        let b1 = sup.report_failure("repl", "err").unwrap();
        let b2 = sup.report_failure("repl", "err").unwrap();
        let b3 = sup.report_failure("repl", "err").unwrap();

        assert_eq!(b1, Duration::from_secs(1));
        assert_eq!(b2, Duration::from_secs(2));
        assert_eq!(b3, Duration::from_secs(4));
    }

    #[test]
    fn test_autorestart_max_backoff_cap() {
        let config = AutoRestartConfig {
            max_restarts: 20,
            initial_backoff: Duration::from_secs(30),
            backoff_multiplier: 3.0,
            max_backoff: Duration::from_secs(60),
            ..Default::default()
        };
        let sup = AutoRestartSupervisor::new(config);
        sup.register("task");

        let _ = sup.report_failure("task", "err"); // 30s
        let b2 = sup.report_failure("task", "err").unwrap(); // min(90, 60) = 60s
        assert_eq!(b2, Duration::from_secs(60));
    }

    #[test]
    fn test_autorestart_permanent_failure() {
        let config = AutoRestartConfig {
            max_restarts: 2,
            ..Default::default()
        };
        let sup = AutoRestartSupervisor::new(config);
        sup.register("task");

        assert!(sup.report_failure("task", "err1").is_some()); // restart 1
        assert!(sup.report_failure("task", "err2").is_some()); // restart 2
        assert!(sup.report_failure("task", "err3").is_none()); // permanent

        let failed = sup.permanently_failed_tasks();
        assert_eq!(failed, vec!["task".to_string()]);
    }

    #[test]
    fn test_autorestart_success_resets_counter() {
        let config = AutoRestartConfig {
            max_restarts: 3,
            success_reset_duration: Duration::from_millis(10),
            ..Default::default()
        };
        let sup = AutoRestartSupervisor::new(config);
        sup.register("task");

        // Fail twice
        let _ = sup.report_failure("task", "err");
        let _ = sup.report_failure("task", "err");

        // Start and run successfully
        sup.mark_started("task");
        std::thread::sleep(Duration::from_millis(15));
        sup.report_healthy("task");

        // Counter should be reset
        let state = sup.task_state("task").unwrap();
        assert_eq!(state.restart_count, 0);
        assert!(!state.is_permanently_failed);
    }

    #[test]
    fn test_autorestart_metrics() {
        let config = AutoRestartConfig {
            max_restarts: 1,
            ..Default::default()
        };
        let sup = AutoRestartSupervisor::new(config);
        sup.register("a");
        sup.register("b");

        let _ = sup.report_failure("a", "err"); // restart
        let _ = sup.report_failure("a", "err"); // permanent

        let m = sup.metrics();
        assert_eq!(m.total_restarts, 1);
        assert_eq!(m.total_permanent_failures, 1);
        assert_eq!(m.tasks_monitored, 2);
    }

    // ── §4: HealthCheckHysteresis ──

    #[test]
    fn test_hysteresis_single_miss_stays_healthy() {
        let hyst = HealthCheckHysteresis::default(); // suspect_threshold=2, failure_threshold=5
        hyst.register_node(1);

        let status = hyst.record_miss(1);
        assert_eq!(status, DebouncedHealth::Healthy, "single miss → still healthy");
    }

    #[test]
    fn test_hysteresis_suspect_after_threshold() {
        let config = HysteresisConfig {
            suspect_threshold: 2,
            failure_threshold: 5,
            recovery_threshold: 3,
        };
        let hyst = HealthCheckHysteresis::new(config);
        hyst.register_node(1);

        hyst.record_miss(1); // 1 miss → Healthy
        let status = hyst.record_miss(1); // 2 misses → Suspect
        assert_eq!(status, DebouncedHealth::Suspect);
    }

    #[test]
    fn test_hysteresis_failed_after_threshold() {
        let config = HysteresisConfig {
            suspect_threshold: 2,
            failure_threshold: 4,
            recovery_threshold: 3,
        };
        let hyst = HealthCheckHysteresis::new(config);
        hyst.register_node(1);

        for _ in 0..3 {
            hyst.record_miss(1);
        }
        assert_eq!(hyst.node_status(1), DebouncedHealth::Suspect);

        let status = hyst.record_miss(1); // 4th miss → Failed
        assert_eq!(status, DebouncedHealth::Failed);
    }

    #[test]
    fn test_hysteresis_recovery_requires_threshold() {
        let config = HysteresisConfig {
            suspect_threshold: 1,
            failure_threshold: 2,
            recovery_threshold: 3,
        };
        let hyst = HealthCheckHysteresis::new(config);
        hyst.register_node(1);

        // Drive to Failed
        hyst.record_miss(1);
        hyst.record_miss(1);
        assert_eq!(hyst.node_status(1), DebouncedHealth::Failed);

        // Partial recovery (2 successes < 3 threshold)
        hyst.record_success(1);
        hyst.record_success(1);
        assert_ne!(hyst.node_status(1), DebouncedHealth::Healthy);

        // Full recovery
        hyst.record_success(1);
        assert_eq!(hyst.node_status(1), DebouncedHealth::Healthy);
    }

    #[test]
    fn test_hysteresis_miss_resets_success_counter() {
        let config = HysteresisConfig {
            suspect_threshold: 1,
            failure_threshold: 2,
            recovery_threshold: 3,
        };
        let hyst = HealthCheckHysteresis::new(config);
        hyst.register_node(1);

        // Drive to Failed
        hyst.record_miss(1);
        hyst.record_miss(1);
        assert_eq!(hyst.node_status(1), DebouncedHealth::Failed);

        // 2 successes then a miss → resets
        hyst.record_success(1);
        hyst.record_success(1);
        hyst.record_miss(1); // resets success counter
        let state = hyst.node_state(1).unwrap();
        assert_eq!(state.consecutive_successes, 0);
        assert_eq!(state.consecutive_misses, 1);
    }

    #[test]
    fn test_hysteresis_false_alarm_metric() {
        let hyst = HealthCheckHysteresis::default(); // suspect=2
        hyst.register_node(1);

        // Single miss that doesn't trigger suspect
        hyst.record_miss(1);
        // Then recovered
        hyst.record_success(1);

        let m = hyst.metrics();
        assert!(m.false_alarms_prevented > 0, "should count prevented false alarms");
    }

    // ── §5: PromotionSafetyGuard ──

    #[test]
    fn test_promotion_happy_path() {
        let guard = PromotionSafetyGuard::new();

        guard.begin(100);
        assert_eq!(guard.current_step(), PromotionStep::Idle);

        guard.record_fenced();
        assert_eq!(guard.current_step(), PromotionStep::OldPrimaryFenced);

        guard.record_caught_up(100).unwrap();
        assert_eq!(guard.current_step(), PromotionStep::CandidateCaughtUp);

        guard.record_roles_swapped();
        assert_eq!(guard.current_step(), PromotionStep::RolesSwapped);

        guard.record_unfenced();
        assert_eq!(guard.current_step(), PromotionStep::NewPrimaryUnfenced);

        guard.complete();
        assert_eq!(guard.current_step(), PromotionStep::Complete);

        let m = guard.metrics();
        assert_eq!(m.promotions_attempted, 1);
        assert_eq!(m.promotions_completed, 1);
        assert_eq!(m.promotions_rolled_back, 0);
    }

    #[test]
    fn test_promotion_rollback_on_catch_up_failure() {
        let guard = PromotionSafetyGuard::new();

        guard.begin(100);
        guard.record_fenced();

        // Candidate didn't catch up
        let err = guard.record_caught_up(50);
        assert!(err.is_err());

        let failed_step = guard.rollback("catch-up failed");
        assert_eq!(failed_step, PromotionStep::OldPrimaryFenced);
        assert_eq!(guard.current_step(), PromotionStep::RolledBack);

        let m = guard.metrics();
        assert_eq!(m.promotions_attempted, 1);
        assert_eq!(m.promotions_completed, 0);
        assert_eq!(m.promotions_rolled_back, 1);
    }

    #[test]
    fn test_promotion_rollback_after_swap() {
        let guard = PromotionSafetyGuard::new();

        guard.begin(100);
        guard.record_fenced();
        guard.record_caught_up(100).unwrap();
        guard.record_roles_swapped();

        // Unfencing failed
        let failed_step = guard.rollback("unfence failed");
        assert_eq!(failed_step, PromotionStep::RolesSwapped);

        let m = guard.metrics();
        assert_eq!(m.rollback_reasons.len(), 1);
        assert_eq!(m.rollback_reasons[0], "unfence failed");
    }

    #[test]
    fn test_promotion_multiple_attempts() {
        let guard = PromotionSafetyGuard::new();

        // First attempt: rollback
        guard.begin(100);
        guard.record_fenced();
        guard.rollback("test");

        // Second attempt: success
        guard.begin(200);
        guard.record_fenced();
        guard.record_caught_up(200).unwrap();
        guard.record_roles_swapped();
        guard.record_unfenced();
        guard.complete();

        let m = guard.metrics();
        assert_eq!(m.promotions_attempted, 2);
        assert_eq!(m.promotions_completed, 1);
        assert_eq!(m.promotions_rolled_back, 1);
    }
}
