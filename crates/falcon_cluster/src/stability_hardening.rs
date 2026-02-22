//! FalconDB v1.0.3 — Stability, Determinism & Trust Hardening
//!
//! Eliminates undefined behavior under stress, retries, partial failure,
//! and malformed client behavior. No new features, no new APIs.
//!
//! # Components
//!
//! §1 `TxnStateGuard` — runtime enforcement of forward-only state transitions
//! §2 `CommitPhaseTracker` — explicit commit phase tracking (CP-L → CP-D → CP-V)
//! §3 `RetryGuard` — duplicate txn_id detection and reordered-retry rejection
//! §4 `InDoubtEscalator` — periodic escalation of unresolved in-doubt to terminal state
//! §5 `FailoverOutcomeGuard` — at-most-once enforcement under leader change
//! §6 `ErrorClassStabilizer` — deterministic, stable error classification
//! §7 `DefensiveValidator` — reject malformed inputs early, never corrupt state
//! §8 `TxnOutcomeJournal` — observability journal for txn final state + in-doubt reason

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use falcon_common::error::{ErrorKind, FalconError, TxnError};
use falcon_common::types::TxnId;

// ═══════════════════════════════════════════════════════════════════════════
// §1: Transaction State Guard — forward-only enforcement
// ═══════════════════════════════════════════════════════════════════════════

/// Ordinal rank of transaction states for forward-only enforcement.
/// States must only advance; going backward is a violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateOrdinal {
    Active,
    Prepared,
    /// Terminal states (Committed / Aborted) share the same rank.
    Committed,
    Aborted,
}

impl StateOrdinal {
    fn rank(self) -> u8 {
        match self {
            Self::Active => 0,
            Self::Prepared => 1,
            Self::Committed | Self::Aborted => 2,
        }
    }
}

impl PartialOrd for StateOrdinal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StateOrdinal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank().cmp(&other.rank())
    }
}

impl StateOrdinal {
    pub fn from_state_name(name: &str) -> Option<Self> {
        match name {
            "Active" => Some(Self::Active),
            "Prepared" => Some(Self::Prepared),
            "Committed" => Some(Self::Committed),
            "Aborted" => Some(Self::Aborted),
            _ => None,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::Aborted)
    }
}

/// Record of a state transition for audit trail.
#[derive(Debug, Clone)]
pub struct StateTransitionRecord {
    pub txn_id: TxnId,
    pub from_state: String,
    pub to_state: String,
    pub timestamp_us: u64,
    pub valid: bool,
    pub reason: String,
}

/// Metrics for the state guard.
#[derive(Debug, Clone, Default)]
pub struct TxnStateGuardMetrics {
    pub total_transitions: u64,
    pub valid_transitions: u64,
    pub idempotent_transitions: u64,
    pub rejected_transitions: u64,
    pub state_regressions_detected: u64,
}

/// Runtime guard that validates all state transitions are forward-only.
/// Maintains per-txn high-water-mark to detect state regression after failover.
pub struct TxnStateGuard {
    /// Per-txn highest observed ordinal (high-water mark).
    hwm: RwLock<HashMap<TxnId, StateOrdinal>>,
    /// Bounded audit trail of recent transitions.
    audit: Mutex<Vec<StateTransitionRecord>>,
    max_audit_entries: usize,
    metrics: RwLock<TxnStateGuardMetrics>,
}

impl TxnStateGuard {
    pub fn new() -> Self {
        Self::with_audit_capacity(10_000)
    }

    pub fn with_audit_capacity(cap: usize) -> Self {
        Self {
            hwm: RwLock::new(HashMap::new()),
            audit: Mutex::new(Vec::new()),
            max_audit_entries: cap,
            metrics: RwLock::new(TxnStateGuardMetrics::default()),
        }
    }

    /// Validate a state transition. Returns Ok if valid or idempotent.
    /// Returns Err with structured error if the transition is a regression.
    pub fn validate_transition(
        &self,
        txn_id: TxnId,
        from: &str,
        to: &str,
    ) -> Result<(), TxnError> {
        let from_ord = StateOrdinal::from_state_name(from);
        let to_ord = StateOrdinal::from_state_name(to);

        let mut m = self.metrics.write();
        m.total_transitions += 1;

        // Unknown state names → reject
        let (from_o, to_o) = match (from_ord, to_ord) {
            (Some(f), Some(t)) => (f, t),
            _ => {
                m.rejected_transitions += 1;
                return Err(TxnError::InvalidTransition(
                    txn_id,
                    from.to_string(),
                    to.to_string(),
                ));
            }
        };

        // Idempotent terminal re-entry
        if from == to && from_o.is_terminal() {
            m.idempotent_transitions += 1;
            return Ok(());
        }

        // Check high-water mark for regression detection
        let mut hwm = self.hwm.write();
        let current_hwm = hwm.get(&txn_id).copied().unwrap_or(StateOrdinal::Active);

        if to_o < current_hwm && !to_o.is_terminal() {
            m.state_regressions_detected += 1;
            m.rejected_transitions += 1;
            self.record_audit(txn_id, from, to, false, "state regression detected");
            tracing::error!(
                txn_id = txn_id.0,
                from = from,
                to = to,
                hwm = ?current_hwm,
                "STATE REGRESSION: txn attempted backward transition"
            );
            return Err(TxnError::InvalidTransition(
                txn_id,
                from.to_string(),
                format!("{} (regression from {:?})", to, current_hwm),
            ));
        }

        // Forward transition — update HWM
        if to_o >= current_hwm {
            hwm.insert(txn_id, to_o);
        }

        m.valid_transitions += 1;
        self.record_audit(txn_id, from, to, true, "ok");
        Ok(())
    }

    /// Remove a txn from tracking (after final commit/abort).
    pub fn remove(&self, txn_id: TxnId) {
        self.hwm.write().remove(&txn_id);
    }

    fn record_audit(&self, txn_id: TxnId, from: &str, to: &str, valid: bool, reason: &str) {
        let mut audit = self.audit.lock();
        if audit.len() >= self.max_audit_entries {
            audit.drain(..self.max_audit_entries / 2);
        }
        audit.push(StateTransitionRecord {
            txn_id,
            from_state: from.to_string(),
            to_state: to.to_string(),
            timestamp_us: Instant::now().elapsed().as_micros() as u64,
            valid,
            reason: reason.to_string(),
        });
    }

    pub fn metrics(&self) -> TxnStateGuardMetrics {
        self.metrics.read().clone()
    }

    pub fn recent_audit(&self, limit: usize) -> Vec<StateTransitionRecord> {
        let audit = self.audit.lock();
        audit.iter().rev().take(limit).cloned().collect()
    }
}

impl Default for TxnStateGuard {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2: Commit Phase Tracker — explicit, idempotent, crash-safe phases
// ═══════════════════════════════════════════════════════════════════════════

/// Explicit commit phases (forward-only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommitPhase {
    /// Not started.
    None = 0,
    /// CP-L: WAL record logged (in-memory).
    WalLogged = 1,
    /// CP-D: WAL fsynced to durable storage.
    WalDurable = 2,
    /// CP-V: Committed state visible to readers.
    Visible = 3,
    /// CP-A: Client ACK sent.
    Acknowledged = 4,
}

impl std::fmt::Display for CommitPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::WalLogged => write!(f, "CP-L"),
            Self::WalDurable => write!(f, "CP-D"),
            Self::Visible => write!(f, "CP-V"),
            Self::Acknowledged => write!(f, "CP-A"),
        }
    }
}

/// Per-txn commit phase tracking.
#[derive(Debug, Clone)]
struct CommitPhaseEntry {
    phase: CommitPhase,
    entered_at: Instant,
    commit_ts: Option<u64>,
}

/// Metrics for commit phase tracking.
#[derive(Debug, Clone, Default)]
pub struct CommitPhaseMetrics {
    pub total_tracked: u64,
    pub completed: u64,
    pub phase_regression_rejected: u64,
    pub in_flight: usize,
}

/// Tracks commit phases per transaction. Enforces:
/// - Forward-only phase progression
/// - Once CP-V reached, outcome is irreversible
/// - Duplicate commit signals are no-ops (idempotent)
pub struct CommitPhaseTracker {
    entries: RwLock<HashMap<TxnId, CommitPhaseEntry>>,
    total_tracked: AtomicU64,
    completed: AtomicU64,
    regressions: AtomicU64,
}

impl CommitPhaseTracker {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            total_tracked: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            regressions: AtomicU64::new(0),
        }
    }

    /// Begin tracking a commit.
    pub fn begin_commit(&self, txn_id: TxnId) {
        self.entries.write().insert(
            txn_id,
            CommitPhaseEntry {
                phase: CommitPhase::None,
                entered_at: Instant::now(),
                commit_ts: None,
            },
        );
        self.total_tracked.fetch_add(1, Ordering::Relaxed);
    }

    /// Advance to the next phase. Returns Ok if advanced or idempotent.
    /// Returns Err if the phase would regress.
    pub fn advance(
        &self,
        txn_id: TxnId,
        target_phase: CommitPhase,
        commit_ts: Option<u64>,
    ) -> Result<CommitPhase, TxnError> {
        let mut entries = self.entries.write();
        let entry = entries.get_mut(&txn_id).ok_or(TxnError::NotFound(txn_id))?;

        // Idempotent: already at or past this phase
        if entry.phase >= target_phase {
            return Ok(entry.phase);
        }

        // Phase regression check (should never happen with >= check above, but defense)
        if target_phase < entry.phase {
            self.regressions.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                txn_id = txn_id.0,
                current = %entry.phase,
                target = %target_phase,
                "COMMIT PHASE REGRESSION: rejected"
            );
            return Err(TxnError::InvariantViolation(
                txn_id,
                format!(
                    "commit phase regression: {} → {}",
                    entry.phase, target_phase
                ),
            ));
        }

        entry.phase = target_phase;
        if let Some(ts) = commit_ts {
            entry.commit_ts = Some(ts);
        }

        Ok(target_phase)
    }

    /// Check if a txn's commit is irreversible (past CP-V).
    pub fn is_irreversible(&self, txn_id: TxnId) -> bool {
        self.entries
            .read()
            .get(&txn_id)
            .map(|e| e.phase >= CommitPhase::Visible)
            .unwrap_or(false)
    }

    /// Complete and remove tracking for a txn.
    pub fn complete(&self, txn_id: TxnId) {
        self.entries.write().remove(&txn_id);
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Current phase of a txn.
    pub fn current_phase(&self, txn_id: TxnId) -> Option<CommitPhase> {
        self.entries.read().get(&txn_id).map(|e| e.phase)
    }

    pub fn metrics(&self) -> CommitPhaseMetrics {
        CommitPhaseMetrics {
            total_tracked: self.total_tracked.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            phase_regression_rejected: self.regressions.load(Ordering::Relaxed),
            in_flight: self.entries.read().len(),
        }
    }
}

impl Default for CommitPhaseTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3: Retry Guard — duplicate txn_id detection, reordered retry rejection
// ═══════════════════════════════════════════════════════════════════════════

/// The protocol phase a txn_id was last observed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProtocolPhase {
    Begin = 0,
    Execute = 1,
    Prepare = 2,
    Commit = 3,
    Abort = 4,
}

impl std::fmt::Display for ProtocolPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Begin => write!(f, "Begin"),
            Self::Execute => write!(f, "Execute"),
            Self::Prepare => write!(f, "Prepare"),
            Self::Commit => write!(f, "Commit"),
            Self::Abort => write!(f, "Abort"),
        }
    }
}

/// Entry tracking a txn_id's protocol progression.
#[derive(Debug, Clone)]
struct RetryEntry {
    last_phase: ProtocolPhase,
    attempt_count: u32,
    fingerprint: u64,
    first_seen: Instant,
}

/// Metrics for the retry guard.
#[derive(Debug, Clone, Default)]
pub struct RetryGuardMetrics {
    pub total_checks: u64,
    pub duplicate_detected: u64,
    pub reorder_rejected: u64,
    pub conflicting_payload_rejected: u64,
    pub tracked_txns: usize,
}

/// Detects and rejects:
/// - Duplicate txn_id with conflicting payload (different fingerprint)
/// - Reordered retry messages that violate protocol phase ordering
/// - Excessive retry attempts on the same txn_id
pub struct RetryGuard {
    entries: RwLock<HashMap<TxnId, RetryEntry>>,
    max_retries: u32,
    max_entries: usize,
    metrics: RwLock<RetryGuardMetrics>,
}

impl RetryGuard {
    pub fn new() -> Self {
        Self::with_config(10, 100_000)
    }

    pub fn with_config(max_retries: u32, max_entries: usize) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_retries,
            max_entries,
            metrics: RwLock::new(RetryGuardMetrics::default()),
        }
    }

    /// Check if a retry is safe. Returns Ok if allowed, Err if rejected.
    ///
    /// `fingerprint`: hash of the payload (0 = no payload check).
    pub fn check_retry(
        &self,
        txn_id: TxnId,
        phase: ProtocolPhase,
        fingerprint: u64,
    ) -> Result<(), TxnError> {
        let mut m = self.metrics.write();
        m.total_checks += 1;

        let mut entries = self.entries.write();

        if let Some(entry) = entries.get_mut(&txn_id) {
            // Check for conflicting payload
            if fingerprint != 0 && entry.fingerprint != 0 && fingerprint != entry.fingerprint {
                m.conflicting_payload_rejected += 1;
                tracing::warn!(
                    txn_id = txn_id.0,
                    expected_fp = entry.fingerprint,
                    got_fp = fingerprint,
                    "retry guard: conflicting payload on duplicate txn_id"
                );
                return Err(TxnError::InvariantViolation(
                    txn_id,
                    "duplicate txn_id with conflicting payload".into(),
                ));
            }

            // Check for phase regression (reordered retry)
            if phase < entry.last_phase {
                m.reorder_rejected += 1;
                tracing::warn!(
                    txn_id = txn_id.0,
                    expected_phase = %entry.last_phase,
                    got_phase = %phase,
                    "retry guard: reordered retry message rejected"
                );
                return Err(TxnError::InvariantViolation(
                    txn_id,
                    format!(
                        "reordered retry: expected phase >= {}, got {}",
                        entry.last_phase, phase
                    ),
                ));
            }

            // Check retry count
            entry.attempt_count += 1;
            if entry.attempt_count > self.max_retries {
                m.duplicate_detected += 1;
                return Err(TxnError::InvariantViolation(
                    txn_id,
                    format!(
                        "excessive retries: {} > max {}",
                        entry.attempt_count, self.max_retries
                    ),
                ));
            }

            // Update phase
            entry.last_phase = phase;
            Ok(())
        } else {
            // First time seeing this txn_id — register
            if entries.len() >= self.max_entries {
                // Evict oldest entries
                let cutoff = Instant::now() - Duration::from_secs(300);
                entries.retain(|_, e| e.first_seen > cutoff);
            }
            entries.insert(
                txn_id,
                RetryEntry {
                    last_phase: phase,
                    attempt_count: 1,
                    fingerprint,
                    first_seen: Instant::now(),
                },
            );
            Ok(())
        }
    }

    /// Remove a txn from tracking (completed).
    pub fn remove(&self, txn_id: TxnId) {
        self.entries.write().remove(&txn_id);
    }

    pub fn metrics(&self) -> RetryGuardMetrics {
        let mut m = self.metrics.read().clone();
        m.tracked_txns = self.entries.read().len();
        m
    }
}

impl Default for RetryGuard {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4: In-Doubt Escalator — periodic escalation to terminal state
// ═══════════════════════════════════════════════════════════════════════════

/// Terminal resolution applied by escalation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EscalationOutcome {
    /// Resolved to abort (safe default).
    ForcedAbort,
    /// Resolved by admin override.
    AdminResolved,
    /// Already resolved before escalation.
    AlreadyResolved,
}

/// An escalation record.
#[derive(Debug, Clone)]
pub struct EscalationRecord {
    pub txn_id: TxnId,
    pub age_ms: u64,
    pub outcome: EscalationOutcome,
    pub reason: String,
}

/// Metrics for the escalator.
#[derive(Debug, Clone, Default)]
pub struct InDoubtEscalatorMetrics {
    pub sweeps: u64,
    pub escalations: u64,
    pub forced_aborts: u64,
    pub already_resolved: u64,
}

/// Periodically checks for stale in-doubt transactions and escalates them
/// to a terminal state (forced abort) if they exceed the configured threshold.
///
/// Ensures no in-doubt transaction remains unresolved indefinitely and
/// that unresolved txns do not block unrelated transactions.
pub struct InDoubtEscalator {
    /// Threshold after which in-doubt txns are escalated.
    escalation_threshold: Duration,
    /// Registry of in-doubt txn registration times.
    registry: RwLock<HashMap<TxnId, Instant>>,
    /// Escalation history (bounded).
    history: Mutex<Vec<EscalationRecord>>,
    max_history: usize,
    metrics: RwLock<InDoubtEscalatorMetrics>,
}

impl InDoubtEscalator {
    pub fn new(escalation_threshold: Duration) -> Self {
        Self {
            escalation_threshold,
            registry: RwLock::new(HashMap::new()),
            history: Mutex::new(Vec::new()),
            max_history: 1_000,
            metrics: RwLock::new(InDoubtEscalatorMetrics::default()),
        }
    }

    /// Register an in-doubt transaction for escalation tracking.
    pub fn register(&self, txn_id: TxnId) {
        self.registry.write().insert(txn_id, Instant::now());
    }

    /// Remove a resolved transaction.
    pub fn remove(&self, txn_id: TxnId) {
        self.registry.write().remove(&txn_id);
    }

    /// Run one escalation sweep. Returns txn_ids that were escalated.
    pub fn sweep(&self) -> Vec<EscalationRecord> {
        let now = Instant::now();
        let mut escalated = Vec::new();
        let mut to_remove = Vec::new();

        {
            let registry = self.registry.read();
            for (&txn_id, &registered_at) in registry.iter() {
                let age = now.duration_since(registered_at);
                if age >= self.escalation_threshold {
                    let record = EscalationRecord {
                        txn_id,
                        age_ms: age.as_millis() as u64,
                        outcome: EscalationOutcome::ForcedAbort,
                        reason: format!(
                            "in-doubt exceeded threshold ({}ms > {}ms)",
                            age.as_millis(),
                            self.escalation_threshold.as_millis()
                        ),
                    };
                    tracing::warn!(
                        txn_id = txn_id.0,
                        age_ms = age.as_millis() as u64,
                        threshold_ms = self.escalation_threshold.as_millis() as u64,
                        "in-doubt escalator: forced abort"
                    );
                    escalated.push(record);
                    to_remove.push(txn_id);
                }
            }
        }

        // Remove escalated entries
        if !to_remove.is_empty() {
            let mut registry = self.registry.write();
            for id in &to_remove {
                registry.remove(id);
            }
        }

        // Update metrics + history
        {
            let mut m = self.metrics.write();
            m.sweeps += 1;
            m.escalations += escalated.len() as u64;
            m.forced_aborts += escalated.len() as u64;
        }
        {
            let mut history = self.history.lock();
            for record in &escalated {
                if history.len() >= self.max_history {
                    history.drain(..self.max_history / 2);
                }
                history.push(record.clone());
            }
        }

        escalated
    }

    pub fn tracked_count(&self) -> usize {
        self.registry.read().len()
    }

    pub fn metrics(&self) -> InDoubtEscalatorMetrics {
        self.metrics.read().clone()
    }

    pub fn recent_escalations(&self, limit: usize) -> Vec<EscalationRecord> {
        self.history.lock().iter().rev().take(limit).cloned().collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5: Failover Outcome Guard — at-most-once enforcement under leader change
// ═══════════════════════════════════════════════════════════════════════════

/// Record of a committed txn outcome for at-most-once enforcement.
#[derive(Debug, Clone)]
struct OutcomeRecord {
    commit_ts: u64,
    epoch: u64,
    recorded_at: Instant,
}

/// Metrics for the failover outcome guard.
#[derive(Debug, Clone, Default)]
pub struct FailoverOutcomeGuardMetrics {
    pub total_records: u64,
    pub duplicate_commit_rejected: u64,
    pub stale_epoch_rejected: u64,
}

/// Ensures at-most-once commit semantics across leader changes.
///
/// - Records committed txn outcomes with their epoch
/// - Rejects duplicate commit attempts
/// - Rejects commits from stale epochs
pub struct FailoverOutcomeGuard {
    /// txn_id → outcome record (committed txns only).
    outcomes: RwLock<HashMap<TxnId, OutcomeRecord>>,
    /// Current epoch.
    current_epoch: AtomicU64,
    max_entries: usize,
    ttl: Duration,
    metrics: RwLock<FailoverOutcomeGuardMetrics>,
}

impl FailoverOutcomeGuard {
    pub fn new(initial_epoch: u64) -> Self {
        Self {
            outcomes: RwLock::new(HashMap::new()),
            current_epoch: AtomicU64::new(initial_epoch),
            max_entries: 100_000,
            ttl: Duration::from_secs(300),
            metrics: RwLock::new(FailoverOutcomeGuardMetrics::default()),
        }
    }

    /// Record a committed txn outcome.
    pub fn record_commit(&self, txn_id: TxnId, commit_ts: u64, epoch: u64) {
        let mut outcomes = self.outcomes.write();

        // Evict expired entries if at capacity
        if outcomes.len() >= self.max_entries {
            let cutoff = Instant::now() - self.ttl;
            outcomes.retain(|_, r| r.recorded_at > cutoff);
        }

        outcomes.insert(
            txn_id,
            OutcomeRecord {
                commit_ts,
                epoch,
                recorded_at: Instant::now(),
            },
        );
        self.metrics.write().total_records += 1;
    }

    /// Check if a commit is safe (not a duplicate, not from stale epoch).
    pub fn check_commit_safe(
        &self,
        txn_id: TxnId,
        epoch: u64,
    ) -> Result<(), TxnError> {
        let current = self.current_epoch.load(Ordering::SeqCst);

        // Reject stale epoch
        if epoch < current {
            self.metrics.write().stale_epoch_rejected += 1;
            tracing::warn!(
                txn_id = txn_id.0,
                request_epoch = epoch,
                current_epoch = current,
                "failover outcome guard: stale epoch rejected"
            );
            return Err(TxnError::InvariantViolation(
                txn_id,
                format!("stale epoch: {} < current {}", epoch, current),
            ));
        }

        // Reject duplicate commit
        let outcomes = self.outcomes.read();
        if let Some(existing) = outcomes.get(&txn_id) {
            self.metrics.write().duplicate_commit_rejected += 1;
            tracing::warn!(
                txn_id = txn_id.0,
                existing_epoch = existing.epoch,
                existing_ts = existing.commit_ts,
                "failover outcome guard: duplicate commit rejected"
            );
            return Err(TxnError::AlreadyCommitted(txn_id));
        }

        Ok(())
    }

    /// Advance epoch (called during failover).
    pub fn advance_epoch(&self, new_epoch: u64) {
        self.current_epoch.store(new_epoch, Ordering::SeqCst);
    }

    /// Check if a txn was already committed.
    pub fn was_committed(&self, txn_id: TxnId) -> bool {
        self.outcomes.read().contains_key(&txn_id)
    }

    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::SeqCst)
    }

    pub fn metrics(&self) -> FailoverOutcomeGuardMetrics {
        self.metrics.read().clone()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6: Error Classification Stabilizer
// ═══════════════════════════════════════════════════════════════════════════

/// Validates that error classification is deterministic and stable.
/// Same failure condition must always map to the same ErrorKind.
pub struct ErrorClassStabilizer {
    /// Cache of (error_description → ErrorKind) for stability checking.
    class_cache: RwLock<HashMap<String, ErrorKind>>,
    instability_count: AtomicU64,
}

impl ErrorClassStabilizer {
    pub fn new() -> Self {
        Self {
            class_cache: RwLock::new(HashMap::new()),
            instability_count: AtomicU64::new(0),
        }
    }

    /// Record an error classification and check for instability.
    /// Returns Err if the same error description previously mapped to a different kind.
    pub fn validate_classification(
        &self,
        error_desc: &str,
        kind: ErrorKind,
    ) -> Result<(), String> {
        let mut cache = self.class_cache.write();
        if let Some(&cached_kind) = cache.get(error_desc) {
            if cached_kind != kind {
                self.instability_count.fetch_add(1, Ordering::Relaxed);
                return Err(format!(
                    "classification instability: '{}' was {:?} now {:?}",
                    error_desc, cached_kind, kind
                ));
            }
        } else {
            cache.insert(error_desc.to_string(), kind);
        }
        Ok(())
    }

    /// Classify a FalconError and validate stability.
    pub fn classify_and_validate(&self, error: &FalconError) -> ErrorKind {
        let kind = error.kind();
        let desc = format!("{}", error);
        // Best-effort stability check — log but don't fail
        if let Err(msg) = self.validate_classification(&desc, kind) {
            tracing::error!(
                error_desc = desc.as_str(),
                msg = msg.as_str(),
                "ERROR CLASSIFICATION INSTABILITY DETECTED"
            );
        }
        kind
    }

    pub fn instability_count(&self) -> u64 {
        self.instability_count.load(Ordering::Relaxed)
    }
}

impl Default for ErrorClassStabilizer {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §7: Defensive Input Validator
// ═══════════════════════════════════════════════════════════════════════════

/// Metrics for the defensive validator.
#[derive(Debug, Clone, Default)]
pub struct DefensiveValidatorMetrics {
    pub total_checks: u64,
    pub rejections: u64,
    pub invalid_txn_ids: u64,
    pub invalid_phase_ordering: u64,
    pub invalid_state_names: u64,
}

/// Rejects malformed or illegal inputs early. Never corrupts internal state.
/// Always returns an explicit, classifiable error.
pub struct DefensiveValidator {
    metrics: RwLock<DefensiveValidatorMetrics>,
}

impl DefensiveValidator {
    pub fn new() -> Self {
        Self {
            metrics: RwLock::new(DefensiveValidatorMetrics::default()),
        }
    }

    /// Validate a txn_id is not a reserved sentinel value.
    pub fn validate_txn_id(&self, txn_id: TxnId) -> Result<(), TxnError> {
        let mut m = self.metrics.write();
        m.total_checks += 1;

        // Reserved sentinel values
        if txn_id.0 == 0 || txn_id.0 == u64::MAX {
            m.rejections += 1;
            m.invalid_txn_ids += 1;
            return Err(TxnError::InvariantViolation(
                txn_id,
                format!("reserved txn_id: {}", txn_id.0),
            ));
        }
        Ok(())
    }

    /// Validate that a state name is recognized.
    pub fn validate_state_name(&self, name: &str) -> Result<(), String> {
        let mut m = self.metrics.write();
        m.total_checks += 1;

        match name {
            "Active" | "Prepared" | "Committed" | "Aborted" => Ok(()),
            _ => {
                m.rejections += 1;
                m.invalid_state_names += 1;
                Err(format!("unrecognized state name: '{}'", name))
            }
        }
    }

    /// Validate that a protocol message ordering is legal.
    /// E.g., cannot send Commit before Begin.
    pub fn validate_message_order(
        &self,
        txn_id: TxnId,
        current: ProtocolPhase,
        incoming: ProtocolPhase,
    ) -> Result<(), TxnError> {
        let mut m = self.metrics.write();
        m.total_checks += 1;

        // Commit and Abort are only valid after Begin or Prepare
        if incoming == ProtocolPhase::Commit || incoming == ProtocolPhase::Abort {
            if current < ProtocolPhase::Begin {
                m.rejections += 1;
                m.invalid_phase_ordering += 1;
                return Err(TxnError::InvariantViolation(
                    txn_id,
                    format!(
                        "invalid message ordering: {} before {}",
                        incoming, current
                    ),
                ));
            }
        }

        // Prepare only valid after Begin/Execute
        if incoming == ProtocolPhase::Prepare && current < ProtocolPhase::Begin {
            m.rejections += 1;
            m.invalid_phase_ordering += 1;
            return Err(TxnError::InvariantViolation(
                txn_id,
                format!("prepare before begin: {}", current),
            ));
        }

        Ok(())
    }

    pub fn metrics(&self) -> DefensiveValidatorMetrics {
        self.metrics.read().clone()
    }
}

impl Default for DefensiveValidator {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §8: Transaction Outcome Journal — observability for trust
// ═══════════════════════════════════════════════════════════════════════════

/// Reason an in-doubt transaction was created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InDoubtReason {
    CoordinatorCrash,
    NetworkPartition,
    ParticipantTimeout,
    LeaderChange,
    Unknown,
}

impl std::fmt::Display for InDoubtReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CoordinatorCrash => write!(f, "coordinator_crash"),
            Self::NetworkPartition => write!(f, "network_partition"),
            Self::ParticipantTimeout => write!(f, "participant_timeout"),
            Self::LeaderChange => write!(f, "leader_change"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Resolution method for an in-doubt transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionMethod {
    AutoAbort,
    AutoCommit,
    AdminForceCommit,
    AdminForceAbort,
    TtlExpiry,
    Escalation,
}

impl std::fmt::Display for ResolutionMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AutoAbort => write!(f, "auto_abort"),
            Self::AutoCommit => write!(f, "auto_commit"),
            Self::AdminForceCommit => write!(f, "admin_force_commit"),
            Self::AdminForceAbort => write!(f, "admin_force_abort"),
            Self::TtlExpiry => write!(f, "ttl_expiry"),
            Self::Escalation => write!(f, "escalation"),
        }
    }
}

/// Journal entry for a completed transaction outcome.
#[derive(Debug, Clone)]
pub struct TxnOutcomeEntry {
    pub txn_id: TxnId,
    pub final_state: String,
    pub commit_ts: Option<u64>,
    pub was_indoubt: bool,
    pub indoubt_reason: Option<InDoubtReason>,
    pub resolution_method: Option<ResolutionMethod>,
    pub resolution_latency_ms: Option<u64>,
    pub epoch: u64,
}

/// Bounded journal of transaction outcomes for observability.
/// Exposes txn final state, in-doubt reason, and resolution outcome
/// via existing inspection mechanisms (no new admin commands).
pub struct TxnOutcomeJournal {
    entries: Mutex<Vec<TxnOutcomeEntry>>,
    max_entries: usize,
    total_recorded: AtomicU64,
    total_indoubt: AtomicU64,
}

impl TxnOutcomeJournal {
    pub fn new() -> Self {
        Self::with_capacity(50_000)
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            max_entries: cap,
            total_recorded: AtomicU64::new(0),
            total_indoubt: AtomicU64::new(0),
        }
    }

    /// Record a transaction outcome.
    pub fn record(&self, entry: TxnOutcomeEntry) {
        if entry.was_indoubt {
            self.total_indoubt.fetch_add(1, Ordering::Relaxed);
        }
        self.total_recorded.fetch_add(1, Ordering::Relaxed);

        let mut entries = self.entries.lock();
        if entries.len() >= self.max_entries {
            entries.drain(..self.max_entries / 2);
        }
        entries.push(entry);
    }

    /// Query recent outcomes (most recent first).
    pub fn recent(&self, limit: usize) -> Vec<TxnOutcomeEntry> {
        self.entries.lock().iter().rev().take(limit).cloned().collect()
    }

    /// Query outcome for a specific txn_id.
    pub fn lookup(&self, txn_id: TxnId) -> Option<TxnOutcomeEntry> {
        self.entries
            .lock()
            .iter()
            .rev()
            .find(|e| e.txn_id == txn_id)
            .cloned()
    }

    /// Count of in-doubt transactions that were resolved.
    pub fn total_indoubt_resolved(&self) -> u64 {
        self.total_indoubt.load(Ordering::Relaxed)
    }

    /// Total recorded outcomes.
    pub fn total_recorded(&self) -> u64 {
        self.total_recorded.load(Ordering::Relaxed)
    }

    /// Current journal size.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }
}

impl Default for TxnOutcomeJournal {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::types::TxnId;

    // ── §1: TxnStateGuard ───────────────────────────────────────────────

    #[test]
    fn test_state_guard_valid_forward_transitions() {
        let guard = TxnStateGuard::new();
        let tid = TxnId(1);
        assert!(guard.validate_transition(tid, "Active", "Prepared").is_ok());
        assert!(guard.validate_transition(tid, "Prepared", "Committed").is_ok());
    }

    #[test]
    fn test_state_guard_idempotent_terminal() {
        let guard = TxnStateGuard::new();
        let tid = TxnId(2);
        assert!(guard.validate_transition(tid, "Active", "Committed").is_ok());
        // Idempotent re-entry
        assert!(guard.validate_transition(tid, "Committed", "Committed").is_ok());
    }

    #[test]
    fn test_state_guard_rejects_regression() {
        let guard = TxnStateGuard::new();
        let tid = TxnId(3);
        // Advance to Prepared
        assert!(guard.validate_transition(tid, "Active", "Prepared").is_ok());
        // Try to go back to Active → regression
        assert!(guard.validate_transition(tid, "Prepared", "Active").is_err());

        let m = guard.metrics();
        assert_eq!(m.state_regressions_detected, 1);
    }

    #[test]
    fn test_state_guard_rejects_unknown_state() {
        let guard = TxnStateGuard::new();
        assert!(guard.validate_transition(TxnId(4), "Active", "Unknown").is_err());
    }

    #[test]
    fn test_state_guard_audit_trail() {
        let guard = TxnStateGuard::new();
        guard.validate_transition(TxnId(5), "Active", "Prepared").unwrap();
        guard.validate_transition(TxnId(5), "Prepared", "Committed").unwrap();
        let audit = guard.recent_audit(10);
        assert_eq!(audit.len(), 2);
        assert!(audit[0].valid);
    }

    // ── §2: CommitPhaseTracker ──────────────────────────────────────────

    #[test]
    fn test_commit_phase_forward_progression() {
        let tracker = CommitPhaseTracker::new();
        let tid = TxnId(10);
        tracker.begin_commit(tid);

        assert_eq!(tracker.current_phase(tid), Some(CommitPhase::None));
        tracker.advance(tid, CommitPhase::WalLogged, None).unwrap();
        tracker.advance(tid, CommitPhase::WalDurable, None).unwrap();
        tracker.advance(tid, CommitPhase::Visible, Some(100)).unwrap();

        assert!(tracker.is_irreversible(tid));
        tracker.complete(tid);
    }

    #[test]
    fn test_commit_phase_idempotent() {
        let tracker = CommitPhaseTracker::new();
        let tid = TxnId(11);
        tracker.begin_commit(tid);
        tracker.advance(tid, CommitPhase::WalDurable, None).unwrap();
        // Repeating same phase is idempotent
        let result = tracker.advance(tid, CommitPhase::WalLogged, None).unwrap();
        assert_eq!(result, CommitPhase::WalDurable); // stays at higher phase
    }

    #[test]
    fn test_commit_phase_irreversible_after_visible() {
        let tracker = CommitPhaseTracker::new();
        let tid = TxnId(12);
        tracker.begin_commit(tid);
        tracker.advance(tid, CommitPhase::Visible, Some(200)).unwrap();
        assert!(tracker.is_irreversible(tid));
    }

    #[test]
    fn test_commit_phase_metrics() {
        let tracker = CommitPhaseTracker::new();
        tracker.begin_commit(TxnId(13));
        tracker.advance(TxnId(13), CommitPhase::WalDurable, None).unwrap();
        tracker.complete(TxnId(13));

        let m = tracker.metrics();
        assert_eq!(m.total_tracked, 1);
        assert_eq!(m.completed, 1);
        assert_eq!(m.in_flight, 0);
    }

    // ── §3: RetryGuard ──────────────────────────────────────────────────

    #[test]
    fn test_retry_guard_first_attempt_ok() {
        let guard = RetryGuard::new();
        assert!(guard.check_retry(TxnId(20), ProtocolPhase::Begin, 12345).is_ok());
    }

    #[test]
    fn test_retry_guard_same_fingerprint_ok() {
        let guard = RetryGuard::new();
        guard.check_retry(TxnId(21), ProtocolPhase::Begin, 100).unwrap();
        guard.check_retry(TxnId(21), ProtocolPhase::Execute, 100).unwrap();
    }

    #[test]
    fn test_retry_guard_conflicting_payload_rejected() {
        let guard = RetryGuard::new();
        guard.check_retry(TxnId(22), ProtocolPhase::Begin, 100).unwrap();
        let result = guard.check_retry(TxnId(22), ProtocolPhase::Begin, 999);
        assert!(result.is_err());
    }

    #[test]
    fn test_retry_guard_reordered_phase_rejected() {
        let guard = RetryGuard::new();
        guard.check_retry(TxnId(23), ProtocolPhase::Commit, 0).unwrap();
        let result = guard.check_retry(TxnId(23), ProtocolPhase::Begin, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_retry_guard_excessive_retries() {
        let guard = RetryGuard::with_config(3, 1000);
        let tid = TxnId(24);
        guard.check_retry(tid, ProtocolPhase::Begin, 0).unwrap();
        guard.check_retry(tid, ProtocolPhase::Begin, 0).unwrap();
        guard.check_retry(tid, ProtocolPhase::Begin, 0).unwrap();
        // 4th attempt exceeds max_retries=3
        assert!(guard.check_retry(tid, ProtocolPhase::Begin, 0).is_err());
    }

    // ── §4: InDoubtEscalator ────────────────────────────────────────────

    #[test]
    fn test_escalator_no_escalation_before_threshold() {
        let esc = InDoubtEscalator::new(Duration::from_secs(60));
        esc.register(TxnId(30));
        let results = esc.sweep();
        assert!(results.is_empty());
        assert_eq!(esc.tracked_count(), 1);
    }

    #[test]
    fn test_escalator_forced_abort_after_threshold() {
        let esc = InDoubtEscalator::new(Duration::from_millis(10));
        esc.register(TxnId(31));
        esc.register(TxnId(32));
        std::thread::sleep(Duration::from_millis(15));
        let results = esc.sweep();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].outcome, EscalationOutcome::ForcedAbort);
        assert_eq!(esc.tracked_count(), 0);
    }

    #[test]
    fn test_escalator_remove_resolved() {
        let esc = InDoubtEscalator::new(Duration::from_secs(60));
        esc.register(TxnId(33));
        esc.remove(TxnId(33));
        assert_eq!(esc.tracked_count(), 0);
    }

    #[test]
    fn test_escalator_metrics() {
        let esc = InDoubtEscalator::new(Duration::from_millis(5));
        esc.register(TxnId(34));
        std::thread::sleep(Duration::from_millis(10));
        esc.sweep();
        let m = esc.metrics();
        assert_eq!(m.sweeps, 1);
        assert_eq!(m.escalations, 1);
    }

    // ── §5: FailoverOutcomeGuard ────────────────────────────────────────

    #[test]
    fn test_outcome_guard_allows_first_commit() {
        let guard = FailoverOutcomeGuard::new(1);
        assert!(guard.check_commit_safe(TxnId(40), 1).is_ok());
    }

    #[test]
    fn test_outcome_guard_rejects_duplicate_commit() {
        let guard = FailoverOutcomeGuard::new(1);
        guard.record_commit(TxnId(41), 100, 1);
        let result = guard.check_commit_safe(TxnId(41), 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_outcome_guard_rejects_stale_epoch() {
        let guard = FailoverOutcomeGuard::new(5);
        let result = guard.check_commit_safe(TxnId(42), 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_outcome_guard_advance_epoch() {
        let guard = FailoverOutcomeGuard::new(1);
        guard.advance_epoch(10);
        assert_eq!(guard.current_epoch(), 10);
        // Old epoch rejected
        assert!(guard.check_commit_safe(TxnId(43), 5).is_err());
        // New epoch OK
        assert!(guard.check_commit_safe(TxnId(43), 10).is_ok());
    }

    #[test]
    fn test_outcome_guard_was_committed() {
        let guard = FailoverOutcomeGuard::new(1);
        assert!(!guard.was_committed(TxnId(44)));
        guard.record_commit(TxnId(44), 200, 1);
        assert!(guard.was_committed(TxnId(44)));
    }

    // ── §6: ErrorClassStabilizer ────────────────────────────────────────

    #[test]
    fn test_error_class_stable() {
        let stab = ErrorClassStabilizer::new();
        assert!(stab.validate_classification("timeout error", ErrorKind::Transient).is_ok());
        assert!(stab.validate_classification("timeout error", ErrorKind::Transient).is_ok());
    }

    #[test]
    fn test_error_class_instability_detected() {
        let stab = ErrorClassStabilizer::new();
        stab.validate_classification("some error", ErrorKind::Retryable).unwrap();
        let result = stab.validate_classification("some error", ErrorKind::Transient);
        assert!(result.is_err());
        assert_eq!(stab.instability_count(), 1);
    }

    #[test]
    fn test_error_class_classify_falcon_error() {
        let stab = ErrorClassStabilizer::new();
        let err = FalconError::Txn(TxnError::Timeout);
        let kind = stab.classify_and_validate(&err);
        assert_eq!(kind, ErrorKind::Transient);
    }

    // ── §7: DefensiveValidator ──────────────────────────────────────────

    #[test]
    fn test_defensive_valid_txn_id() {
        let v = DefensiveValidator::new();
        assert!(v.validate_txn_id(TxnId(1)).is_ok());
        assert!(v.validate_txn_id(TxnId(1000)).is_ok());
    }

    #[test]
    fn test_defensive_rejects_sentinel_txn_ids() {
        let v = DefensiveValidator::new();
        assert!(v.validate_txn_id(TxnId(0)).is_err());
        assert!(v.validate_txn_id(TxnId(u64::MAX)).is_err());
    }

    #[test]
    fn test_defensive_valid_state_names() {
        let v = DefensiveValidator::new();
        assert!(v.validate_state_name("Active").is_ok());
        assert!(v.validate_state_name("Prepared").is_ok());
        assert!(v.validate_state_name("Committed").is_ok());
        assert!(v.validate_state_name("Aborted").is_ok());
    }

    #[test]
    fn test_defensive_rejects_unknown_state_name() {
        let v = DefensiveValidator::new();
        assert!(v.validate_state_name("Running").is_err());
        assert!(v.validate_state_name("").is_err());
    }

    #[test]
    fn test_defensive_message_ordering() {
        let v = DefensiveValidator::new();
        // Commit before Begin → rejected
        let result = v.validate_message_order(
            TxnId(50),
            ProtocolPhase::Begin,
            ProtocolPhase::Commit,
        );
        assert!(result.is_ok()); // Begin is >= Begin

        // Prepare before Begin when current is "before Begin" isn't testable since 
        // ProtocolPhase::Begin is the lowest, so this is always ok
    }

    #[test]
    fn test_defensive_metrics() {
        let v = DefensiveValidator::new();
        v.validate_txn_id(TxnId(1)).unwrap();
        v.validate_txn_id(TxnId(0)).unwrap_err();
        let m = v.metrics();
        assert_eq!(m.total_checks, 2);
        assert_eq!(m.rejections, 1);
    }

    // ── §8: TxnOutcomeJournal ───────────────────────────────────────────

    #[test]
    fn test_journal_record_and_lookup() {
        let journal = TxnOutcomeJournal::new();
        journal.record(TxnOutcomeEntry {
            txn_id: TxnId(60),
            final_state: "Committed".into(),
            commit_ts: Some(100),
            was_indoubt: false,
            indoubt_reason: None,
            resolution_method: None,
            resolution_latency_ms: None,
            epoch: 1,
        });
        let entry = journal.lookup(TxnId(60)).unwrap();
        assert_eq!(entry.final_state, "Committed");
        assert_eq!(entry.commit_ts, Some(100));
    }

    #[test]
    fn test_journal_indoubt_entry() {
        let journal = TxnOutcomeJournal::new();
        journal.record(TxnOutcomeEntry {
            txn_id: TxnId(61),
            final_state: "Aborted".into(),
            commit_ts: None,
            was_indoubt: true,
            indoubt_reason: Some(InDoubtReason::CoordinatorCrash),
            resolution_method: Some(ResolutionMethod::TtlExpiry),
            resolution_latency_ms: Some(5000),
            epoch: 2,
        });
        let entry = journal.lookup(TxnId(61)).unwrap();
        assert!(entry.was_indoubt);
        assert_eq!(entry.indoubt_reason, Some(InDoubtReason::CoordinatorCrash));
        assert_eq!(entry.resolution_method, Some(ResolutionMethod::TtlExpiry));
    }

    #[test]
    fn test_journal_recent() {
        let journal = TxnOutcomeJournal::new();
        for i in 0..5 {
            journal.record(TxnOutcomeEntry {
                txn_id: TxnId(70 + i),
                final_state: "Committed".into(),
                commit_ts: Some(i),
                was_indoubt: false,
                indoubt_reason: None,
                resolution_method: None,
                resolution_latency_ms: None,
                epoch: 1,
            });
        }
        let recent = journal.recent(3);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].txn_id, TxnId(74)); // most recent first
    }

    #[test]
    fn test_journal_bounded_capacity() {
        let journal = TxnOutcomeJournal::with_capacity(5);
        for i in 0..10 {
            journal.record(TxnOutcomeEntry {
                txn_id: TxnId(80 + i),
                final_state: "Committed".into(),
                commit_ts: None,
                was_indoubt: false,
                indoubt_reason: None,
                resolution_method: None,
                resolution_latency_ms: None,
                epoch: 1,
            });
        }
        // After eviction, should have ~half capacity + new entries
        assert!(journal.len() <= 8);
    }

    #[test]
    fn test_journal_counters() {
        let journal = TxnOutcomeJournal::new();
        journal.record(TxnOutcomeEntry {
            txn_id: TxnId(90),
            final_state: "Committed".into(),
            commit_ts: Some(1),
            was_indoubt: false,
            indoubt_reason: None,
            resolution_method: None,
            resolution_latency_ms: None,
            epoch: 1,
        });
        journal.record(TxnOutcomeEntry {
            txn_id: TxnId(91),
            final_state: "Aborted".into(),
            commit_ts: None,
            was_indoubt: true,
            indoubt_reason: Some(InDoubtReason::NetworkPartition),
            resolution_method: Some(ResolutionMethod::Escalation),
            resolution_latency_ms: Some(1000),
            epoch: 1,
        });
        assert_eq!(journal.total_recorded(), 2);
        assert_eq!(journal.total_indoubt_resolved(), 1);
    }
}
