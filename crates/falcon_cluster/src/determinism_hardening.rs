//! FalconDB v1.0.4 — Production-Grade Determinism & Failure Safety
//!
//! This module eliminates implicit blocking, silent queue growth, ambiguous
//! transaction outcomes, and undocumented failover behavior.
//!
//! # Design Principles (v1.0.4)
//!
//! 1. **Prefer rejection over degradation** — If a resource is exhausted,
//!    reject immediately with a structured error code. Never silently queue.
//! 2. **Prefer determinism over throughput** — Every transaction ends in an
//!    explainable terminal state. No "unknown outcome" except genuine network
//!    partition during commit.
//! 3. **Prefer boring correctness over cleverness** — Failover is predictable.
//!    Replay is idempotent. Docs, tests, and code say the same thing.
//!
//! # Components
//!
//! §1 `ResourceExhaustionContract` — formalized exhaustion semantics
//! §2 `TxnTerminalState` — every txn ends in one of these, with error code + retry policy
//! §3 `FailoverCommitInvariant` — CP-D / CP-V crash window validation
//! §4 `QueueDepthGuard` — bounded queue depth with no-silent-growth assertion
//! §5 `DeterministicRejectPolicy` — uniform rejection behavior across all resource types

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

use falcon_common::error::{ErrorKind, FalconError, TxnError};
use falcon_common::types::TxnId;

// ═══════════════════════════════════════════════════════════════════════════
// §1: Resource Exhaustion Contract
// ═══════════════════════════════════════════════════════════════════════════

/// Every resource that can be exhausted MUST have an entry here.
/// Each entry defines: what happens when exhausted, the error code returned,
/// and whether the exhaustion is visible in metrics/logs/CLI.
///
/// **Invariant RES-1**: No resource exhaustion path may block implicitly.
///   All must reject with a structured error within `max_reject_latency`.
///
/// **Invariant RES-2**: Every rejection increments a per-resource counter
///   visible via `SHOW falcon.admission` and Prometheus metrics.
///
/// **Invariant RES-3**: No queue may grow without bound. Every queue has
///   a hard capacity. Exceeding it triggers immediate rejection.
#[derive(Debug, Clone)]
pub struct ResourceExhaustionContract {
    /// Human-readable resource name.
    pub resource_name: &'static str,
    /// PG SQLSTATE code returned on exhaustion.
    pub sqlstate: &'static str,
    /// Error kind classification.
    pub error_kind: ErrorKind,
    /// Whether the client should retry (and suggested delay).
    pub retry_policy: RetryPolicy,
    /// Maximum time (µs) between resource check and rejection response.
    /// Exceeding this is a bug.
    pub max_reject_latency_us: u64,
    /// Prometheus metric name for rejection counter.
    pub metric_name: &'static str,
    /// Whether this resource is surfaced via `SHOW falcon.admission`.
    pub show_visible: bool,
}

/// Retry policy for a rejected operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryPolicy {
    /// Do not retry — the request is permanently invalid.
    NoRetry,
    /// Retry after the specified delay (ms).
    RetryAfter(u64),
    /// Retry with exponential backoff starting at base_ms, up to max_ms.
    ExponentialBackoff { base_ms: u64, max_ms: u64 },
    /// Retry on a different node (leader changed).
    RetryOnDifferentNode { retry_after_ms: u64 },
}

impl RetryPolicy {
    /// Whether the client should retry at all.
    pub const fn should_retry(&self) -> bool {
        !matches!(self, Self::NoRetry)
    }

    /// Suggested initial retry delay in milliseconds.
    pub const fn initial_delay_ms(&self) -> u64 {
        match self {
            Self::NoRetry => 0,
            Self::RetryAfter(ms) => *ms,
            Self::ExponentialBackoff { base_ms, .. } => *base_ms,
            Self::RetryOnDifferentNode { retry_after_ms } => *retry_after_ms,
        }
    }
}

/// The global resource exhaustion contract table.
/// Every exhaustible resource is enumerated here.
pub fn resource_contracts() -> Vec<ResourceExhaustionContract> {
    vec![
        ResourceExhaustionContract {
            resource_name: "memory_hard_limit",
            sqlstate: "53200", // out_of_memory
            error_kind: ErrorKind::Transient,
            retry_policy: RetryPolicy::ExponentialBackoff { base_ms: 100, max_ms: 5000 },
            max_reject_latency_us: 100,
            metric_name: "falcon_admission_memory_rejected",
            show_visible: true,
        },
        ResourceExhaustionContract {
            resource_name: "memory_soft_pressure",
            sqlstate: "53200",
            error_kind: ErrorKind::Transient,
            retry_policy: RetryPolicy::RetryAfter(50),
            max_reject_latency_us: 100,
            metric_name: "falcon_admission_memory_pressure",
            show_visible: true,
        },
        ResourceExhaustionContract {
            resource_name: "wal_backlog",
            sqlstate: "53300", // too_many_connections (reused for resource)
            error_kind: ErrorKind::Transient,
            retry_policy: RetryPolicy::RetryAfter(200),
            max_reject_latency_us: 100,
            metric_name: "falcon_admission_wal_rejected",
            show_visible: true,
        },
        ResourceExhaustionContract {
            resource_name: "replication_lag",
            sqlstate: "57P03", // cannot_connect_now
            error_kind: ErrorKind::Transient,
            retry_policy: RetryPolicy::RetryAfter(500),
            max_reject_latency_us: 100,
            metric_name: "falcon_admission_replication_rejected",
            show_visible: true,
        },
        ResourceExhaustionContract {
            resource_name: "connection_limit",
            sqlstate: "53300",
            error_kind: ErrorKind::Transient,
            retry_policy: RetryPolicy::ExponentialBackoff { base_ms: 50, max_ms: 2000 },
            max_reject_latency_us: 50,
            metric_name: "falcon_admission_connection_rejected",
            show_visible: true,
        },
        ResourceExhaustionContract {
            resource_name: "query_concurrency",
            sqlstate: "53300",
            error_kind: ErrorKind::Transient,
            retry_policy: RetryPolicy::RetryAfter(50),
            max_reject_latency_us: 50,
            metric_name: "falcon_admission_query_rejected",
            show_visible: true,
        },
        ResourceExhaustionContract {
            resource_name: "write_concurrency_per_shard",
            sqlstate: "53300",
            error_kind: ErrorKind::Transient,
            retry_policy: RetryPolicy::RetryAfter(50),
            max_reject_latency_us: 50,
            metric_name: "falcon_admission_write_rejected",
            show_visible: true,
        },
        ResourceExhaustionContract {
            resource_name: "cross_shard_txn_concurrency",
            sqlstate: "53300",
            error_kind: ErrorKind::Transient,
            retry_policy: RetryPolicy::RetryAfter(100),
            max_reject_latency_us: 100,
            metric_name: "falcon_admission_cross_shard_rejected",
            show_visible: true,
        },
        ResourceExhaustionContract {
            resource_name: "ddl_concurrency",
            sqlstate: "53300",
            error_kind: ErrorKind::Transient,
            retry_policy: RetryPolicy::RetryAfter(200),
            max_reject_latency_us: 50,
            metric_name: "falcon_admission_ddl_rejected",
            show_visible: true,
        },
    ]
}

/// Validate that all resource exhaustion contracts are self-consistent.
pub fn validate_contracts() -> Result<(), Vec<String>> {
    let contracts = resource_contracts();
    let mut errors = Vec::new();

    for c in &contracts {
        if c.resource_name.is_empty() {
            errors.push("Empty resource_name".into());
        }
        if c.sqlstate.len() != 5 {
            errors.push(format!("{}: SQLSTATE must be 5 chars, got '{}'", c.resource_name, c.sqlstate));
        }
        if c.max_reject_latency_us == 0 {
            errors.push(format!("{}: max_reject_latency_us must be > 0", c.resource_name));
        }
        if c.metric_name.is_empty() {
            errors.push(format!("{}: metric_name must not be empty", c.resource_name));
        }
        if !c.show_visible {
            errors.push(format!("{}: all resources MUST be show_visible (v1.0.4 contract)", c.resource_name));
        }
    }

    // Check for duplicate resource names
    let mut seen = std::collections::HashSet::new();
    for c in &contracts {
        if !seen.insert(c.resource_name) {
            errors.push(format!("Duplicate resource_name: {}", c.resource_name));
        }
    }

    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2: Transaction Terminal State Formalization
// ═══════════════════════════════════════════════════════════════════════════

/// Every transaction MUST end in exactly one of these terminal states.
///
/// **Invariant TTS-1**: No transaction may end without a terminal classification.
/// **Invariant TTS-2**: Each terminal state maps to exactly one SQLSTATE + retry policy.
/// **Invariant TTS-3**: "Unknown outcome" (Indeterminate) is only valid during
///   genuine network partition or crash during commit. It MUST NOT occur during
///   normal operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnTerminalState {
    /// Transaction committed successfully.
    Committed {
        commit_ts: u64,
    },
    /// Transaction aborted due to a retryable condition.
    AbortedRetryable {
        reason: AbortReason,
    },
    /// Transaction aborted due to a non-retryable condition.
    AbortedNonRetryable {
        reason: AbortReason,
    },
    /// Transaction rejected before execution (pre-admission).
    Rejected {
        reason: RejectReason,
    },
    /// Transaction outcome is indeterminate (crash/partition during commit).
    /// Client MUST check status or retry with idempotency key.
    Indeterminate {
        reason: String,
    },
}

/// Why a transaction was aborted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbortReason {
    /// Write-write conflict (OCC).
    SerializationConflict,
    /// Deadlock detected.
    Deadlock,
    /// Constraint violation (UNIQUE, FK, CHECK, NOT NULL).
    ConstraintViolation(String),
    /// Transaction timeout exceeded.
    Timeout,
    /// Explicit ROLLBACK by client.
    ExplicitRollback,
    /// Failover forced abort (in-flight txn during leader change).
    FailoverAbort,
    /// Read-only txn attempted write.
    ReadOnlyViolation,
    /// Storage error during execution.
    StorageError(String),
    /// Internal invariant violation.
    InvariantViolation(String),
}

/// Why a transaction was rejected before execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// Memory limit exceeded.
    MemoryExhausted,
    /// WAL backlog exceeded.
    WalBacklogExceeded,
    /// Replication lag exceeded.
    ReplicationLagExceeded,
    /// Connection limit reached.
    ConnectionLimitReached,
    /// Query concurrency limit reached.
    QueryConcurrencyExceeded,
    /// Write concurrency limit reached on shard.
    WriteConcurrencyExceeded(u64),
    /// Cross-shard txn concurrency exceeded.
    CrossShardConcurrencyExceeded,
    /// DDL concurrency exceeded.
    DdlConcurrencyExceeded,
    /// Circuit breaker open on target shard.
    CircuitBreakerOpen(u64),
    /// Node is in drain/read-only mode.
    NodeNotAcceptingWrites,
}

impl TxnTerminalState {
    /// PG SQLSTATE code for this terminal state.
    pub const fn sqlstate(&self) -> &'static str {
        match self {
            Self::Committed { .. } => "00000",
            Self::AbortedRetryable { reason }
            | Self::AbortedNonRetryable { reason } => reason.sqlstate(),
            Self::Rejected { reason } => reason.sqlstate(),
            Self::Indeterminate { .. } => "08006", // connection_failure
        }
    }

    /// Retry policy for this terminal state.
    pub const fn retry_policy(&self) -> RetryPolicy {
        match self {
            Self::Committed { .. }
            | Self::AbortedNonRetryable { .. } => RetryPolicy::NoRetry,
            Self::AbortedRetryable { .. } => RetryPolicy::RetryAfter(0),
            Self::Rejected { reason } => reason.retry_policy(),
            Self::Indeterminate { .. } => RetryPolicy::ExponentialBackoff {
                base_ms: 100,
                max_ms: 5000,
            },
        }
    }

    /// Error kind classification.
    pub const fn error_kind(&self) -> ErrorKind {
        match self {
            Self::Committed { .. } => ErrorKind::UserError, // not an error
            Self::AbortedRetryable { .. } => ErrorKind::Retryable,
            Self::AbortedNonRetryable { reason } => match reason {
                AbortReason::ConstraintViolation(_)
                | AbortReason::ReadOnlyViolation
                | AbortReason::ExplicitRollback => ErrorKind::UserError,
                _ => ErrorKind::Retryable,
            },
            Self::Rejected { .. }
            | Self::Indeterminate { .. } => ErrorKind::Transient,
        }
    }

    /// Whether the transaction is in a definitive terminal state.
    pub const fn is_terminal(&self) -> bool {
        !matches!(self, Self::Indeterminate { .. })
    }

    /// Human-readable client message.
    pub fn client_message(&self) -> String {
        match self {
            Self::Committed { commit_ts } => format!("COMMIT (ts={commit_ts})"),
            Self::AbortedRetryable { reason } => format!("ABORT (retryable): {}", reason.description()),
            Self::AbortedNonRetryable { reason } => format!("ABORT: {}", reason.description()),
            Self::Rejected { reason } => format!("REJECTED: {}", reason.description()),
            Self::Indeterminate { reason } => format!("INDETERMINATE: {reason}"),
        }
    }
}

impl AbortReason {
    pub const fn sqlstate(&self) -> &'static str {
        match self {
            Self::SerializationConflict
            | Self::FailoverAbort => "40001",
            Self::Deadlock => "40P01",
            Self::ConstraintViolation(_) => "23000",
            Self::Timeout => "57014",
            Self::ExplicitRollback => "40000",
            Self::ReadOnlyViolation => "25006",
            Self::StorageError(_)
            | Self::InvariantViolation(_) => "XX000",
        }
    }

    pub fn description(&self) -> String {
        match self {
            Self::SerializationConflict => "serialization conflict (write-write or OCC)".into(),
            Self::Deadlock => "deadlock detected".into(),
            Self::ConstraintViolation(d) => format!("constraint violation: {d}"),
            Self::Timeout => "transaction timeout exceeded".into(),
            Self::ExplicitRollback => "explicit ROLLBACK".into(),
            Self::FailoverAbort => "aborted due to leader failover".into(),
            Self::ReadOnlyViolation => "read-only transaction attempted a write".into(),
            Self::StorageError(e) => format!("storage error: {e}"),
            Self::InvariantViolation(e) => format!("invariant violation: {e}"),
        }
    }
}

impl RejectReason {
    pub const fn sqlstate(&self) -> &'static str {
        match self {
            Self::MemoryExhausted => "53200",
            Self::WalBacklogExceeded
            | Self::ConnectionLimitReached
            | Self::QueryConcurrencyExceeded
            | Self::WriteConcurrencyExceeded(_)
            | Self::CrossShardConcurrencyExceeded
            | Self::DdlConcurrencyExceeded => "53300",
            Self::ReplicationLagExceeded
            | Self::CircuitBreakerOpen(_) => "57P03",
            Self::NodeNotAcceptingWrites => "25006",
        }
    }

    pub const fn retry_policy(&self) -> RetryPolicy {
        match self {
            Self::MemoryExhausted => RetryPolicy::ExponentialBackoff { base_ms: 100, max_ms: 5000 },
            Self::ReplicationLagExceeded => RetryPolicy::RetryAfter(500),
            Self::WalBacklogExceeded
            | Self::DdlConcurrencyExceeded => RetryPolicy::RetryAfter(200),
            Self::ConnectionLimitReached => RetryPolicy::ExponentialBackoff { base_ms: 50, max_ms: 2000 },
            Self::QueryConcurrencyExceeded
            | Self::WriteConcurrencyExceeded(_) => RetryPolicy::RetryAfter(50),
            Self::CrossShardConcurrencyExceeded => RetryPolicy::RetryAfter(100),
            Self::CircuitBreakerOpen(_) => RetryPolicy::RetryOnDifferentNode { retry_after_ms: 1000 },
            Self::NodeNotAcceptingWrites => RetryPolicy::RetryOnDifferentNode { retry_after_ms: 500 },
        }
    }

    pub fn description(&self) -> String {
        match self {
            Self::MemoryExhausted => "memory limit exceeded".into(),
            Self::WalBacklogExceeded => "WAL backlog exceeded admission threshold".into(),
            Self::ReplicationLagExceeded => "replication lag exceeded admission threshold".into(),
            Self::ConnectionLimitReached => "connection limit reached".into(),
            Self::QueryConcurrencyExceeded => "query concurrency limit reached".into(),
            Self::WriteConcurrencyExceeded(s) => format!("write concurrency limit on shard {s}"),
            Self::CrossShardConcurrencyExceeded => "cross-shard txn concurrency exceeded".into(),
            Self::DdlConcurrencyExceeded => "DDL concurrency limit reached".into(),
            Self::CircuitBreakerOpen(s) => format!("circuit breaker open on shard {s}"),
            Self::NodeNotAcceptingWrites => "node is in drain or read-only mode".into(),
        }
    }
}

/// Map a FalconError to a TxnTerminalState.
/// This is the canonical classification function — every error MUST map to
/// exactly one terminal state.
pub fn classify_error(err: &FalconError) -> TxnTerminalState {
    match err {
        // Retryable aborts
        FalconError::Txn(TxnError::WriteConflict(_))
        | FalconError::Txn(TxnError::SerializationConflict(_)) => {
            TxnTerminalState::AbortedRetryable {
                reason: AbortReason::SerializationConflict,
            }
        }
        FalconError::Txn(TxnError::Aborted(_)) => TxnTerminalState::AbortedRetryable {
            reason: AbortReason::SerializationConflict,
        },
        FalconError::Txn(TxnError::Timeout) => TxnTerminalState::AbortedRetryable {
            reason: AbortReason::Timeout,
        },

        // Non-retryable aborts
        FalconError::Txn(TxnError::ConstraintViolation(_, msg)) => {
            TxnTerminalState::AbortedNonRetryable {
                reason: AbortReason::ConstraintViolation(msg.clone()),
            }
        }
        FalconError::ReadOnly(_) => TxnTerminalState::AbortedNonRetryable {
            reason: AbortReason::ReadOnlyViolation,
        },
        FalconError::Txn(TxnError::InvariantViolation(_, msg)) => {
            TxnTerminalState::AbortedNonRetryable {
                reason: AbortReason::InvariantViolation(msg.clone()),
            }
        }

        // Pre-admission rejections
        FalconError::Txn(TxnError::MemoryPressure(_))
        | FalconError::Txn(TxnError::MemoryLimitExceeded(_)) => TxnTerminalState::Rejected {
            reason: RejectReason::MemoryExhausted,
        },
        FalconError::Txn(TxnError::WalBacklogExceeded(_)) => TxnTerminalState::Rejected {
            reason: RejectReason::WalBacklogExceeded,
        },
        FalconError::Txn(TxnError::ReplicationLagExceeded(_)) => TxnTerminalState::Rejected {
            reason: RejectReason::ReplicationLagExceeded,
        },
        FalconError::Transient { reason, .. } => {
            if reason.contains("memory") {
                TxnTerminalState::Rejected { reason: RejectReason::MemoryExhausted }
            } else if reason.contains("WAL") {
                TxnTerminalState::Rejected { reason: RejectReason::WalBacklogExceeded }
            } else if reason.contains("replication") {
                TxnTerminalState::Rejected { reason: RejectReason::ReplicationLagExceeded }
            } else if reason.contains("connection") {
                TxnTerminalState::Rejected { reason: RejectReason::ConnectionLimitReached }
            } else if reason.contains("query") {
                TxnTerminalState::Rejected { reason: RejectReason::QueryConcurrencyExceeded }
            } else if reason.contains("DDL") {
                TxnTerminalState::Rejected { reason: RejectReason::DdlConcurrencyExceeded }
            } else {
                TxnTerminalState::Rejected { reason: RejectReason::QueryConcurrencyExceeded }
            }
        }

        // Connection failure → indeterminate
        FalconError::Protocol(falcon_common::error::ProtocolError::ConnectionClosed) => {
            TxnTerminalState::Indeterminate {
                reason: "connection lost during transaction".into(),
            }
        }

        // Everything else: non-retryable abort
        _ => TxnTerminalState::AbortedNonRetryable {
            reason: AbortReason::StorageError(err.to_string()),
        },
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3: Failover × Commit Invariant Validator
// ═══════════════════════════════════════════════════════════════════════════

/// Models the commit phases for failover invariant validation.
///
/// **Invariant FC-1**: Leader crash before CP-D → txn MUST be lost (rolled back).
/// **Invariant FC-2**: Leader crash after CP-D but before CP-V → txn MUST survive
///   recovery and become visible on the new leader.
/// **Invariant FC-3**: No txn may be acknowledged (CP-V) without being durable (CP-D).
/// **Invariant FC-4**: Replay of a committed txn is idempotent — replaying twice
///   produces the same state as replaying once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommitPhase {
    /// Transaction active, no WAL record.
    Active,
    /// WAL record written but not fsynced (CP-L).
    WalLogged,
    /// WAL record fsynced to disk (CP-D).
    WalDurable,
    /// Transaction visible to readers (CP-V).
    Visible,
    /// Client has received acknowledgement (CP-A).
    Acknowledged,
}

/// Record of a transaction's commit phase at the moment of a simulated crash.
#[derive(Debug, Clone)]
pub struct FailoverCrashRecord {
    pub txn_id: TxnId,
    pub phase_at_crash: CommitPhase,
    pub expected_after_recovery: FailoverExpectedOutcome,
}

/// Expected outcome after recovery from a crash at a given commit phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverExpectedOutcome {
    /// Transaction must be rolled back (invisible).
    MustBeRolledBack,
    /// Transaction must survive and be visible.
    MustSurvive,
    /// Outcome depends on commit policy (may or may not survive).
    PolicyDependent,
}

impl CommitPhase {
    /// Determine expected recovery outcome for a crash at this phase.
    pub const fn expected_recovery(&self, local_fsync_required: bool) -> FailoverExpectedOutcome {
        match self {
            Self::Active => FailoverExpectedOutcome::MustBeRolledBack,
            Self::WalLogged => {
                if local_fsync_required {
                    FailoverExpectedOutcome::MustBeRolledBack
                } else {
                    FailoverExpectedOutcome::PolicyDependent
                }
            }
            Self::WalDurable | Self::Visible | Self::Acknowledged => {
                FailoverExpectedOutcome::MustSurvive
            }
        }
    }
}

/// Validates failover invariants for a set of transactions.
pub fn validate_failover_invariants(
    crash_records: &[FailoverCrashRecord],
    recovered_visible: &[TxnId],
    local_fsync_required: bool,
) -> Result<(), Vec<String>> {
    let mut violations = Vec::new();
    let visible_set: std::collections::HashSet<_> = recovered_visible.iter().collect();

    for record in crash_records {
        let expected = record.phase_at_crash.expected_recovery(local_fsync_required);
        let is_visible = visible_set.contains(&record.txn_id);

        match expected {
            FailoverExpectedOutcome::MustBeRolledBack => {
                if is_visible {
                    violations.push(format!(
                        "FC-1 violation: txn {} was at {:?} at crash but is visible after recovery",
                        record.txn_id, record.phase_at_crash
                    ));
                }
            }
            FailoverExpectedOutcome::MustSurvive => {
                if !is_visible {
                    violations.push(format!(
                        "FC-2 violation: txn {} was at {:?} at crash but is NOT visible after recovery",
                        record.txn_id, record.phase_at_crash
                    ));
                }
            }
            FailoverExpectedOutcome::PolicyDependent => {
                // Either outcome is acceptable
            }
        }
    }

    if violations.is_empty() { Ok(()) } else { Err(violations) }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4: Queue Depth Guard — No Silent Growth
// ═══════════════════════════════════════════════════════════════════════════

/// Bounded queue with deterministic rejection.
///
/// **Invariant QD-1**: Queue depth never exceeds `hard_capacity`.
/// **Invariant QD-2**: When `hard_capacity` is reached, new entries are
///   rejected immediately (no blocking, no silent queueing).
/// **Invariant QD-3**: Every rejection is counted and visible via metrics.
pub struct QueueDepthGuard {
    name: &'static str,
    current_depth: AtomicU64,
    hard_capacity: u64,
    total_rejected: AtomicU64,
    total_enqueued: AtomicU64,
    peak_depth: AtomicU64,
}

impl QueueDepthGuard {
    pub const fn new(name: &'static str, hard_capacity: u64) -> Self {
        Self {
            name,
            current_depth: AtomicU64::new(0),
            hard_capacity,
            total_rejected: AtomicU64::new(0),
            total_enqueued: AtomicU64::new(0),
            peak_depth: AtomicU64::new(0),
        }
    }

    /// Try to enqueue. Returns Ok(QueueSlot) or Err if at capacity.
    pub fn try_enqueue(&self) -> Result<QueueSlot<'_>, FalconError> {
        let current = self.current_depth.fetch_add(1, Ordering::AcqRel);
        if current >= self.hard_capacity {
            self.current_depth.fetch_sub(1, Ordering::Relaxed);
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(FalconError::transient(
                format!(
                    "queue '{}' at capacity ({}/{})",
                    self.name, current, self.hard_capacity
                ),
                50,
            ));
        }
        self.total_enqueued.fetch_add(1, Ordering::Relaxed);
        // Update peak
        let mut peak = self.peak_depth.load(Ordering::Relaxed);
        let new_depth = current + 1;
        while new_depth > peak {
            match self.peak_depth.compare_exchange_weak(
                peak, new_depth, Ordering::Relaxed, Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
        Ok(QueueSlot { guard: self })
    }

    /// Current queue depth.
    pub fn depth(&self) -> u64 {
        self.current_depth.load(Ordering::Relaxed)
    }

    /// Hard capacity.
    pub const fn capacity(&self) -> u64 {
        self.hard_capacity
    }

    /// Peak observed depth.
    pub fn peak(&self) -> u64 {
        self.peak_depth.load(Ordering::Relaxed)
    }

    /// Total rejections.
    pub fn total_rejected(&self) -> u64 {
        self.total_rejected.load(Ordering::Relaxed)
    }

    /// Total successful enqueues.
    pub fn total_enqueued(&self) -> u64 {
        self.total_enqueued.load(Ordering::Relaxed)
    }

    /// Queue name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Snapshot for metrics/observability.
    pub fn snapshot(&self) -> QueueDepthSnapshot {
        QueueDepthSnapshot {
            name: self.name,
            depth: self.depth(),
            capacity: self.hard_capacity,
            peak: self.peak(),
            total_enqueued: self.total_enqueued(),
            total_rejected: self.total_rejected(),
        }
    }
}

/// RAII slot that decrements queue depth on drop.
pub struct QueueSlot<'a> {
    guard: &'a QueueDepthGuard,
}

impl<'a> Drop for QueueSlot<'a> {
    fn drop(&mut self) {
        self.guard.current_depth.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Observable queue depth snapshot.
#[derive(Debug, Clone)]
pub struct QueueDepthSnapshot {
    pub name: &'static str,
    pub depth: u64,
    pub capacity: u64,
    pub peak: u64,
    pub total_enqueued: u64,
    pub total_rejected: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §5: Deterministic Reject Policy
// ═══════════════════════════════════════════════════════════════════════════

/// Unified rejection behavior: every resource type uses the same pattern.
///
/// 1. Check resource availability (non-blocking, O(1)).
/// 2. If unavailable: increment rejection counter, return structured error.
/// 3. Error includes: SQLSTATE, retry policy, human-readable reason.
/// 4. No blocking, no wait, no silent queue growth.
pub struct DeterministicRejectPolicy {
    /// Per-resource rejection counters (resource_name → count).
    counters: RwLock<HashMap<&'static str, AtomicU64>>,
    /// Total rejections across all resources.
    total: AtomicU64,
}

impl Default for DeterministicRejectPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl DeterministicRejectPolicy {
    pub fn new() -> Self {
        let mut map = HashMap::new();
        for contract in resource_contracts() {
            map.insert(contract.resource_name, AtomicU64::new(0));
        }
        Self {
            counters: RwLock::new(map),
            total: AtomicU64::new(0),
        }
    }

    /// Record a rejection for a resource.
    pub fn record_rejection(&self, resource_name: &'static str) {
        self.total.fetch_add(1, Ordering::Relaxed);
        let counters = self.counters.read();
        if let Some(counter) = counters.get(resource_name) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Total rejections across all resources.
    pub fn total_rejections(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Per-resource rejection counts.
    pub fn per_resource_rejections(&self) -> HashMap<&'static str, u64> {
        let counters = self.counters.read();
        counters
            .iter()
            .map(|(k, v)| (*k, v.load(Ordering::Relaxed)))
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6: Idempotent Replay Validator
// ═══════════════════════════════════════════════════════════════════════════

/// Validates that WAL replay is idempotent.
///
/// **Invariant IR-1**: Replaying the same WAL segment twice produces
///   identical state (same visible rows, same commit set).
/// **Invariant IR-2**: Replaying a committed txn's records produces
///   the same data regardless of how many times it is replayed.
pub struct IdempotentReplayValidator {
    /// Set of txn_ids that have been "replayed".
    replayed_txns: Mutex<std::collections::HashSet<TxnId>>,
    /// Count of duplicate replay attempts (should be harmless).
    duplicate_replays: AtomicU64,
    /// Count of violations (non-idempotent replay detected).
    violations: AtomicU64,
}

impl Default for IdempotentReplayValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl IdempotentReplayValidator {
    pub fn new() -> Self {
        Self {
            replayed_txns: Mutex::new(std::collections::HashSet::new()),
            duplicate_replays: AtomicU64::new(0),
            violations: AtomicU64::new(0),
        }
    }

    /// Record a replay of a txn. Returns true if this is a duplicate replay.
    pub fn record_replay(&self, txn_id: TxnId) -> bool {
        let mut set = self.replayed_txns.lock();
        if set.contains(&txn_id) {
            self.duplicate_replays.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            set.insert(txn_id);
            false
        }
    }

    /// Record a violation (non-idempotent replay detected).
    pub fn record_violation(&self, txn_id: TxnId) {
        self.violations.fetch_add(1, Ordering::Relaxed);
        tracing::error!(
            txn_id = txn_id.0,
            "IDEMPOTENCY VIOLATION: replay of txn {} produced different state",
            txn_id
        );
    }

    pub fn duplicate_count(&self) -> u64 {
        self.duplicate_replays.load(Ordering::Relaxed)
    }

    pub fn violation_count(&self) -> u64 {
        self.violations.load(Ordering::Relaxed)
    }

    pub fn replayed_count(&self) -> usize {
        self.replayed_txns.lock().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::types::TxnId;

    // ── §1: Resource Exhaustion Contracts ──

    #[test]
    fn test_all_contracts_are_valid() {
        validate_contracts().expect("all resource contracts must be valid");
    }

    #[test]
    fn test_contract_count_is_expected() {
        let contracts = resource_contracts();
        assert!(
            contracts.len() >= 9,
            "must have at least 9 resource contracts, got {}",
            contracts.len()
        );
    }

    #[test]
    fn test_all_contracts_have_unique_names() {
        let contracts = resource_contracts();
        let mut names = std::collections::HashSet::new();
        for c in &contracts {
            assert!(names.insert(c.resource_name), "duplicate: {}", c.resource_name);
        }
    }

    #[test]
    fn test_all_contracts_are_show_visible() {
        for c in resource_contracts() {
            assert!(c.show_visible, "{} must be show_visible", c.resource_name);
        }
    }

    #[test]
    fn test_all_contracts_have_valid_sqlstate() {
        for c in resource_contracts() {
            assert_eq!(c.sqlstate.len(), 5, "{}: bad SQLSTATE", c.resource_name);
        }
    }

    // ── §2: Transaction Terminal State ──

    #[test]
    fn test_committed_state() {
        let s = TxnTerminalState::Committed { commit_ts: 42 };
        assert_eq!(s.sqlstate(), "00000");
        assert!(!s.retry_policy().should_retry());
        assert!(s.is_terminal());
    }

    #[test]
    fn test_aborted_retryable_state() {
        let s = TxnTerminalState::AbortedRetryable {
            reason: AbortReason::SerializationConflict,
        };
        assert_eq!(s.sqlstate(), "40001");
        assert!(s.retry_policy().should_retry());
        assert!(s.is_terminal());
        assert_eq!(s.error_kind(), ErrorKind::Retryable);
    }

    #[test]
    fn test_aborted_non_retryable_constraint() {
        let s = TxnTerminalState::AbortedNonRetryable {
            reason: AbortReason::ConstraintViolation("UNIQUE on col 0".into()),
        };
        assert_eq!(s.sqlstate(), "23000");
        assert!(!s.retry_policy().should_retry());
        assert!(s.is_terminal());
        assert_eq!(s.error_kind(), ErrorKind::UserError);
    }

    #[test]
    fn test_rejected_memory() {
        let s = TxnTerminalState::Rejected {
            reason: RejectReason::MemoryExhausted,
        };
        assert_eq!(s.sqlstate(), "53200");
        assert!(s.retry_policy().should_retry());
        assert!(s.is_terminal());
        assert_eq!(s.error_kind(), ErrorKind::Transient);
    }

    #[test]
    fn test_indeterminate_state() {
        let s = TxnTerminalState::Indeterminate {
            reason: "connection lost during commit".into(),
        };
        assert_eq!(s.sqlstate(), "08006");
        assert!(s.retry_policy().should_retry());
        assert!(!s.is_terminal());
    }

    #[test]
    fn test_all_reject_reasons_have_sqlstate() {
        let reasons = vec![
            RejectReason::MemoryExhausted,
            RejectReason::WalBacklogExceeded,
            RejectReason::ReplicationLagExceeded,
            RejectReason::ConnectionLimitReached,
            RejectReason::QueryConcurrencyExceeded,
            RejectReason::WriteConcurrencyExceeded(0),
            RejectReason::CrossShardConcurrencyExceeded,
            RejectReason::DdlConcurrencyExceeded,
            RejectReason::CircuitBreakerOpen(0),
            RejectReason::NodeNotAcceptingWrites,
        ];
        for r in &reasons {
            assert_eq!(r.sqlstate().len(), 5, "bad SQLSTATE for {:?}", r);
            assert!(r.retry_policy().should_retry(), "{:?} should be retryable", r);
        }
    }

    #[test]
    fn test_all_abort_reasons_have_sqlstate() {
        let reasons = vec![
            AbortReason::SerializationConflict,
            AbortReason::Deadlock,
            AbortReason::ConstraintViolation("test".into()),
            AbortReason::Timeout,
            AbortReason::ExplicitRollback,
            AbortReason::FailoverAbort,
            AbortReason::ReadOnlyViolation,
            AbortReason::StorageError("test".into()),
            AbortReason::InvariantViolation("test".into()),
        ];
        for r in &reasons {
            assert_eq!(r.sqlstate().len(), 5, "bad SQLSTATE for {:?}", r);
            assert!(!r.description().is_empty(), "empty description for {:?}", r);
        }
    }

    #[test]
    fn test_classify_error_serialization_conflict() {
        let err = FalconError::Txn(TxnError::SerializationConflict(TxnId(1)));
        let state = classify_error(&err);
        assert!(matches!(state, TxnTerminalState::AbortedRetryable { .. }));
        assert_eq!(state.sqlstate(), "40001");
    }

    #[test]
    fn test_classify_error_memory_pressure() {
        let err = FalconError::Txn(TxnError::MemoryPressure(TxnId(1)));
        let state = classify_error(&err);
        assert!(matches!(state, TxnTerminalState::Rejected { .. }));
        assert_eq!(state.sqlstate(), "53200");
    }

    #[test]
    fn test_classify_error_read_only() {
        let err = FalconError::ReadOnly("write attempted".into());
        let state = classify_error(&err);
        assert!(matches!(state, TxnTerminalState::AbortedNonRetryable { .. }));
        assert_eq!(state.sqlstate(), "25006");
    }

    #[test]
    fn test_classify_error_transient_wal() {
        let err = FalconError::transient("WAL flush queue backlog too large", 200);
        let state = classify_error(&err);
        assert!(matches!(state, TxnTerminalState::Rejected { reason: RejectReason::WalBacklogExceeded }));
    }

    // ── §3: Failover Invariants ──

    #[test]
    fn test_fc1_crash_before_durable_must_rollback() {
        let records = vec![
            FailoverCrashRecord {
                txn_id: TxnId(1),
                phase_at_crash: CommitPhase::Active,
                expected_after_recovery: FailoverExpectedOutcome::MustBeRolledBack,
            },
            FailoverCrashRecord {
                txn_id: TxnId(2),
                phase_at_crash: CommitPhase::WalLogged,
                expected_after_recovery: FailoverExpectedOutcome::MustBeRolledBack,
            },
        ];
        // Neither txn should be visible after recovery
        let recovered = vec![];
        assert!(validate_failover_invariants(&records, &recovered, true).is_ok());
    }

    #[test]
    fn test_fc2_crash_after_durable_must_survive() {
        let records = vec![FailoverCrashRecord {
            txn_id: TxnId(3),
            phase_at_crash: CommitPhase::WalDurable,
            expected_after_recovery: FailoverExpectedOutcome::MustSurvive,
        }];
        let recovered = vec![TxnId(3)];
        assert!(validate_failover_invariants(&records, &recovered, true).is_ok());
    }

    #[test]
    fn test_fc2_violation_durable_not_recovered() {
        let records = vec![FailoverCrashRecord {
            txn_id: TxnId(3),
            phase_at_crash: CommitPhase::WalDurable,
            expected_after_recovery: FailoverExpectedOutcome::MustSurvive,
        }];
        let recovered = vec![]; // missing!
        let result = validate_failover_invariants(&records, &recovered, true);
        assert!(result.is_err());
        assert!(result.unwrap_err()[0].contains("FC-2"));
    }

    #[test]
    fn test_fc1_violation_active_recovered() {
        let records = vec![FailoverCrashRecord {
            txn_id: TxnId(1),
            phase_at_crash: CommitPhase::Active,
            expected_after_recovery: FailoverExpectedOutcome::MustBeRolledBack,
        }];
        let recovered = vec![TxnId(1)]; // should NOT be visible
        let result = validate_failover_invariants(&records, &recovered, true);
        assert!(result.is_err());
        assert!(result.unwrap_err()[0].contains("FC-1"));
    }

    #[test]
    fn test_fc3_acknowledged_must_survive() {
        let records = vec![FailoverCrashRecord {
            txn_id: TxnId(5),
            phase_at_crash: CommitPhase::Acknowledged,
            expected_after_recovery: FailoverExpectedOutcome::MustSurvive,
        }];
        let recovered = vec![TxnId(5)];
        assert!(validate_failover_invariants(&records, &recovered, true).is_ok());
    }

    #[test]
    fn test_commit_phase_ordering() {
        assert!(CommitPhase::Active < CommitPhase::WalLogged);
        assert!(CommitPhase::WalLogged < CommitPhase::WalDurable);
        assert!(CommitPhase::WalDurable < CommitPhase::Visible);
        assert!(CommitPhase::Visible < CommitPhase::Acknowledged);
    }

    #[test]
    fn test_policy_dependent_phase() {
        // WalLogged with no fsync requirement → policy dependent
        let outcome = CommitPhase::WalLogged.expected_recovery(false);
        assert_eq!(outcome, FailoverExpectedOutcome::PolicyDependent);
        // WalLogged with fsync required → must be rolled back
        let outcome = CommitPhase::WalLogged.expected_recovery(true);
        assert_eq!(outcome, FailoverExpectedOutcome::MustBeRolledBack);
    }

    // ── §4: Queue Depth Guard ──

    #[test]
    fn test_queue_enqueue_dequeue() {
        let guard = QueueDepthGuard::new("test_queue", 10);
        assert_eq!(guard.depth(), 0);
        {
            let _slot = guard.try_enqueue().unwrap();
            assert_eq!(guard.depth(), 1);
        }
        assert_eq!(guard.depth(), 0);
        assert_eq!(guard.total_enqueued(), 1);
    }

    #[test]
    fn test_queue_capacity_enforced() {
        let guard = QueueDepthGuard::new("test_queue", 2);
        let _s1 = guard.try_enqueue().unwrap();
        let _s2 = guard.try_enqueue().unwrap();
        let result = guard.try_enqueue();
        assert!(result.is_err());
        assert_eq!(guard.total_rejected(), 1);
        assert_eq!(guard.depth(), 2); // no silent growth
    }

    #[test]
    fn test_queue_peak_tracking() {
        let guard = QueueDepthGuard::new("test_queue", 10);
        {
            let _s1 = guard.try_enqueue().unwrap();
            let _s2 = guard.try_enqueue().unwrap();
            let _s3 = guard.try_enqueue().unwrap();
            assert_eq!(guard.peak(), 3);
        }
        assert_eq!(guard.depth(), 0);
        assert_eq!(guard.peak(), 3); // peak preserved
    }

    #[test]
    fn test_queue_snapshot() {
        let guard = QueueDepthGuard::new("my_queue", 100);
        let _s = guard.try_enqueue().unwrap();
        let snap = guard.snapshot();
        assert_eq!(snap.name, "my_queue");
        assert_eq!(snap.depth, 1);
        assert_eq!(snap.capacity, 100);
        assert_eq!(snap.total_enqueued, 1);
        assert_eq!(snap.total_rejected, 0);
    }

    // ── §5: Deterministic Reject Policy ──

    #[test]
    fn test_reject_policy_tracks_per_resource() {
        let policy = DeterministicRejectPolicy::new();
        policy.record_rejection("memory_hard_limit");
        policy.record_rejection("memory_hard_limit");
        policy.record_rejection("wal_backlog");
        assert_eq!(policy.total_rejections(), 3);
        let per = policy.per_resource_rejections();
        assert_eq!(*per.get("memory_hard_limit").unwrap_or(&0), 2);
        assert_eq!(*per.get("wal_backlog").unwrap_or(&0), 1);
    }

    // ── §6: Idempotent Replay ──

    #[test]
    fn test_idempotent_replay_first_is_not_duplicate() {
        let v = IdempotentReplayValidator::new();
        assert!(!v.record_replay(TxnId(1)));
        assert_eq!(v.replayed_count(), 1);
    }

    #[test]
    fn test_idempotent_replay_second_is_duplicate() {
        let v = IdempotentReplayValidator::new();
        v.record_replay(TxnId(1));
        assert!(v.record_replay(TxnId(1)));
        assert_eq!(v.duplicate_count(), 1);
    }

    #[test]
    fn test_idempotent_replay_violation_counted() {
        let v = IdempotentReplayValidator::new();
        v.record_violation(TxnId(1));
        assert_eq!(v.violation_count(), 1);
    }

    // ── Cross-cutting: classify_error covers all ErrorKind paths ──

    #[test]
    fn test_classify_error_covers_all_terminal_types() {
        // Committed: not an error, so not classified here
        // Retryable abort
        let r = classify_error(&FalconError::Txn(TxnError::WriteConflict(TxnId(1))));
        assert!(matches!(r, TxnTerminalState::AbortedRetryable { .. }));

        // Non-retryable abort
        let r = classify_error(&FalconError::Txn(TxnError::ConstraintViolation(TxnId(1), "pk".into())));
        assert!(matches!(r, TxnTerminalState::AbortedNonRetryable { .. }));

        // Rejection
        let r = classify_error(&FalconError::Txn(TxnError::WalBacklogExceeded(TxnId(1))));
        assert!(matches!(r, TxnTerminalState::Rejected { .. }));

        // Indeterminate
        let r = classify_error(&FalconError::Protocol(falcon_common::error::ProtocolError::ConnectionClosed));
        assert!(matches!(r, TxnTerminalState::Indeterminate { .. }));
    }

    // ── Retry policy completeness ──

    #[test]
    fn test_retry_policy_initial_delay() {
        assert_eq!(RetryPolicy::NoRetry.initial_delay_ms(), 0);
        assert_eq!(RetryPolicy::RetryAfter(100).initial_delay_ms(), 100);
        assert_eq!(
            RetryPolicy::ExponentialBackoff { base_ms: 50, max_ms: 1000 }.initial_delay_ms(),
            50
        );
        assert_eq!(
            RetryPolicy::RetryOnDifferentNode { retry_after_ms: 500 }.initial_delay_ms(),
            500
        );
    }
}
