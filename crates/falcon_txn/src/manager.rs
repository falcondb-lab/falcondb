//! # Module Status: PRODUCTION
//! Transaction Manager — single source of truth for transaction lifecycle.
//!
//! ## Golden Path (OLTP Write)
//! ```text
//! SQL → Planner → Executor → TxnManager.begin()
//!   → MVCC/OCC read/write on MemTable (in-memory row store)
//!   → TxnManager.commit()
//!     → StorageEngine.commit_txn()  [OCC validation + WAL append]
//!     → WAL fsync (per CommitPolicy)
//!     → Replication stream (if configured)
//!     → Client ACK
//! ```
//!
//! ## Path Classification (deterministic, no runtime guessing)
//! - **Fast-Path**: single-shard → local OCC commit, no 2PC
//! - **Slow-Path**: multi-shard → XA-style 2PC via CoordinatorDecisionLog
//!
//! ## Invariants
//! - Every write MUST go through TxnManager (no direct MemTable mutation)
//! - Every commit MUST append to WAL before client ACK
//! - Path classification is fixed at `begin()` — no implicit upgrade

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use parking_lot::Mutex;

use falcon_common::error::{StorageError, TxnError};
use falcon_common::kernel::TxnLatencyBreakdown;
use falcon_common::security::TxnPriority;
use falcon_common::tenant::{TenantId, SYSTEM_TENANT_ID};
use falcon_common::types::{IsolationLevel, ShardId, Timestamp, TxnContext, TxnId};
pub use falcon_common::types::{TxnPath, TxnType};
use falcon_storage::engine::StorageEngine;

use crate::deadlock::{SsiLockManager, SsiWriteIntent};

/// Slow-path mode for cross-shard transactions.
/// - `Xa2Pc`: XA-style two-phase commit (default, fully implemented).
/// - `AutocommitSplit`: split a multi-shard write into per-shard autocommit txns
///   (weaker consistency, used for bulk-load scenarios).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlowPathMode {
    Xa2Pc,
    AutocommitSplit,
}

/// Classification captured at transaction begin.
#[derive(Debug, Clone)]
pub struct TxnClassification {
    pub txn_type: TxnType,
    pub involved_shards: Vec<ShardId>,
    pub slow_path_mode: SlowPathMode,
}

impl TxnClassification {
    pub fn local(shard: ShardId) -> Self {
        Self {
            txn_type: TxnType::Local,
            involved_shards: vec![shard],
            slow_path_mode: SlowPathMode::Xa2Pc,
        }
    }

    pub const fn global(shards: Vec<ShardId>, mode: SlowPathMode) -> Self {
        Self {
            txn_type: TxnType::Global,
            involved_shards: shards,
            slow_path_mode: mode,
        }
    }
}

/// Transaction state.
///
/// ## Valid state transitions (B1 — explicit state machine)
/// ```text
///  Active ──► Prepared ──► Committed
///    │            │
///    │            └──► Aborted
///    │
///    ├──► Committed  (fast-path local)
///    └──► Aborted
/// ```
///
/// Idempotent re-entries: Committed→Committed, Aborted→Aborted (no-ops).
/// All other transitions are invalid and return `TxnError::InvalidTransition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Prepared,
    Committed,
    Aborted,
}

impl std::fmt::Display for TxnState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "Active"),
            Self::Prepared => write!(f, "Prepared"),
            Self::Committed => write!(f, "Committed"),
            Self::Aborted => write!(f, "Aborted"),
        }
    }
}

/// Result of a state transition attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionResult {
    /// Transition applied successfully.
    Applied,
    /// No-op: target state was the same as current (idempotent).
    Idempotent,
}

impl TxnState {
    /// Validate and apply a state transition. Returns:
    /// - `Ok(Applied)` if the transition is valid and was applied.
    /// - `Ok(Idempotent)` if already in the target state (no-op).
    /// - `Err(InvalidTransition)` if the transition is illegal.
    pub fn try_transition(
        &mut self,
        target: Self,
        txn_id: TxnId,
    ) -> Result<TransitionResult, TxnError> {
        use TxnState::*;
        match (*self, target) {
            // Idempotent re-entries
            (Committed, Committed) | (Aborted, Aborted) => Ok(TransitionResult::Idempotent),
            // Valid forward transitions
            (Active, Prepared)
            | (Active, Committed)
            | (Active, Aborted)
            | (Prepared, Committed)
            | (Prepared, Aborted) => {
                *self = target;
                Ok(TransitionResult::Applied)
            }
            // Everything else is invalid
            _ => Err(TxnError::InvalidTransition(
                txn_id,
                self.to_string(),
                target.to_string(),
            )),
        }
    }
}

/// Handle to an active transaction. Held by the session.
#[derive(Debug, Clone)]
pub struct TxnHandle {
    pub txn_id: TxnId,
    pub start_ts: Timestamp,
    pub isolation: IsolationLevel,
    pub txn_type: TxnType,
    pub path: TxnPath,
    pub slow_path_mode: SlowPathMode,
    pub involved_shards: Vec<ShardId>,
    pub degraded: bool,
    pub state: TxnState,
    /// Wall-clock instant when the transaction began (for latency measurement).
    pub begin_instant: Option<Instant>,
    /// P1-4: Unique trace ID for distributed tracing / correlation.
    pub trace_id: u64,
    /// P1-4: Number of OCC conflict retries this transaction has undergone.
    pub occ_retry_count: u32,
    /// P2-1: Tenant this transaction belongs to.
    pub tenant_id: TenantId,
    /// P2-3: Transaction priority for SLA scheduling.
    pub priority: TxnPriority,
    /// DK-1: Per-phase latency breakdown.
    pub latency_breakdown: TxnLatencyBreakdown,
    /// v1.2: Transaction access mode (READ ONLY / READ WRITE).
    /// When true, any DML (INSERT/UPDATE/DELETE) is rejected.
    pub read_only: bool,
    /// v1.2: Per-transaction timeout in milliseconds (0 = no timeout).
    pub timeout_ms: u64,
    /// v1.2: Execution summary counters.
    pub exec_summary: TxnExecSummary,
}

/// Execution summary for a transaction (v1.2).
/// Tracks statement counts, rows affected, and timing.
#[derive(Debug, Clone, Default)]
pub struct TxnExecSummary {
    /// Number of statements executed in this transaction.
    pub statement_count: u64,
    /// Total rows read across all statements.
    pub rows_read: u64,
    /// Total rows written (inserted + updated + deleted).
    pub rows_written: u64,
    /// Total rows inserted.
    pub rows_inserted: u64,
    /// Total rows updated.
    pub rows_updated: u64,
    /// Total rows deleted.
    pub rows_deleted: u64,
}

impl TxnExecSummary {
    pub const fn record_read(&mut self, count: u64) {
        self.rows_read += count;
    }
    pub const fn record_insert(&mut self, count: u64) {
        self.rows_inserted += count;
        self.rows_written += count;
    }
    pub const fn record_update(&mut self, count: u64) {
        self.rows_updated += count;
        self.rows_written += count;
    }
    pub const fn record_delete(&mut self, count: u64) {
        self.rows_deleted += count;
        self.rows_written += count;
    }
    pub const fn record_statement(&mut self) {
        self.statement_count += 1;
    }
}

/// A completed transaction record for the history ring buffer.
#[derive(Debug, Clone)]
pub struct TxnRecord {
    pub txn_id: TxnId,
    pub txn_type: TxnType,
    pub txn_path: TxnPath,
    pub shard_count: usize,
    pub start_ts: Timestamp,
    pub commit_ts: Option<Timestamp>,
    pub commit_latency_us: u64,
    pub outcome: TxnOutcome,
    pub degraded: bool,
    /// P1-4: Trace ID for distributed tracing correlation.
    pub trace_id: u64,
    /// P1-4: Number of OCC retries this transaction underwent.
    pub occ_retry_count: u32,
    /// P2-1: Tenant this transaction belonged to.
    pub tenant_id: TenantId,
    /// P2-3: Transaction priority.
    pub priority: TxnPriority,
    /// DK-1: Per-phase latency breakdown.
    pub latency_breakdown: TxnLatencyBreakdown,
}

/// Outcome of a completed transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnOutcome {
    Committed,
    Aborted(String),
}

impl TxnHandle {
    /// Read timestamp for this transaction.
    /// Under Read Committed, each statement gets a fresh read_ts (latest committed).
    /// Under Snapshot Isolation, read_ts is fixed at start_ts.
    pub const fn read_ts(&self, current_ts: Timestamp) -> Timestamp {
        match self.isolation {
            IsolationLevel::ReadCommitted => current_ts,
            IsolationLevel::SnapshotIsolation | IsolationLevel::Serializable => self.start_ts,
        }
    }

    /// Check if this transaction has exceeded its timeout.
    /// Returns true if the transaction should be aborted due to timeout.
    pub fn is_timed_out(&self) -> bool {
        if self.timeout_ms == 0 {
            return false;
        }
        self.begin_instant
            .is_some_and(|begin| begin.elapsed().as_millis() as u64 >= self.timeout_ms)
    }

    /// Elapsed time since transaction began, in milliseconds.
    pub fn elapsed_ms(&self) -> u64 {
        self.begin_instant
            .map_or(0, |b| b.elapsed().as_millis() as u64)
    }

    /// Derive a lightweight TxnContext for cross-layer enforcement.
    pub fn to_context(&self) -> TxnContext {
        TxnContext {
            txn_id: self.txn_id,
            txn_type: self.txn_type,
            txn_path: self.path,
            involved_shards: self.involved_shards.clone(),
            start_ts: self.start_ts,
        }
    }
}

/// Aggregate transaction statistics for observability.
#[derive(Debug, Clone, Default)]
pub struct TxnStatsSnapshot {
    pub total_committed: u64,
    pub fast_path_commits: u64,
    pub slow_path_commits: u64,
    pub total_aborted: u64,
    pub occ_conflicts: u64,
    pub degraded_to_global: u64,
    pub constraint_violations: u64,
    pub admission_rejections: u64,
    pub active_count: usize,
    /// Commit latency percentiles (microseconds), partitioned.
    pub latency: LatencyStats,
    /// Ratio of fast-path commits to total commits (0.0..1.0). NaN if no commits.
    pub fast_path_ratio: f64,
    /// Number of times an implicit fast→slow path upgrade was blocked.
    pub implicit_upgrade_blocked: u64,
    /// P2-3: Per-priority latency statistics.
    pub priority_latency: PriorityLatencyStats,
    /// P2-3: SLA violation counters.
    pub sla_violations: SlaViolationStats,
}

/// Latency percentile stats for each partition.
#[derive(Debug, Clone, Default)]
pub struct LatencyStats {
    pub fast_path: PercentileSet,
    pub slow_path: PercentileSet,
    pub all: PercentileSet,
}

/// p50 / p95 / p99 / p999 in microseconds.
#[derive(Debug, Clone, Default)]
pub struct PercentileSet {
    pub count: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
}

/// P2-3: Per-priority latency breakdown.
#[derive(Debug, Clone, Default)]
pub struct PriorityLatencyStats {
    pub background: PercentileSet,
    pub normal: PercentileSet,
    pub high: PercentileSet,
    pub system: PercentileSet,
}

/// P2-3: SLA violation counters — tracks how many txns exceeded their priority's latency target.
#[derive(Debug, Clone, Default)]
pub struct SlaViolationStats {
    /// Number of high-priority txns that exceeded the high-priority latency target.
    pub high_priority_violations: u64,
    /// Number of normal-priority txns that exceeded the normal-priority latency target.
    pub normal_priority_violations: u64,
    /// Total violations across all priorities.
    pub total_violations: u64,
}

/// Diagnostic info about the GC safepoint and long-running transactions.
#[derive(Debug, Clone)]
pub struct GcSafepointInfo {
    /// Minimum start_ts among all active transactions.
    pub min_active_start_ts: Timestamp,
    /// Current (latest) allocated timestamp.
    pub current_ts: Timestamp,
    /// Number of currently active transactions.
    pub active_txn_count: usize,
    /// Number of currently Prepared (in-doubt / undecided) transactions.
    /// P0-3: These pin the GC safepoint and must not be reclaimed.
    pub prepared_txn_count: usize,
    /// Age of the longest-running active transaction (microseconds).
    pub longest_txn_age_us: u64,
    /// True if the GC safepoint is stalled (held back by a long-running txn).
    pub stalled: bool,
}

/// Ring buffer of recent completed transactions.
struct TxnHistory {
    buf: VecDeque<TxnRecord>,
    capacity: usize,
}

impl TxnHistory {
    fn new(capacity: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn push(&mut self, record: TxnRecord) {
        if self.buf.len() >= self.capacity {
            self.buf.pop_front();
        }
        self.buf.push_back(record);
    }

    fn snapshot(&self) -> Vec<TxnRecord> {
        self.buf.iter().cloned().collect()
    }
}

/// SLA latency targets per priority (microseconds). Transactions exceeding these are violations.
const SLA_TARGET_HIGH_US: u64 = 10_000; // 10ms for high-priority
const SLA_TARGET_NORMAL_US: u64 = 100_000; // 100ms for normal
const SLA_TARGET_BACKGROUND_US: u64 = 1_000_000; // 1s for background

/// Maximum latency samples kept per bucket (ring buffer size).
const LATENCY_SAMPLES_CAP: usize = 100_000;

/// Ring-buffer backed latency sample bucket.
struct LatencyBucket {
    samples: Vec<u64>,
    write_pos: usize,
    full: bool,
}

impl LatencyBucket {
    const fn new() -> Self {
        Self {
            samples: Vec::new(),
            write_pos: 0,
            full: false,
        }
    }

    fn push(&mut self, value: u64) {
        if self.samples.len() < LATENCY_SAMPLES_CAP {
            self.samples.push(value);
        } else {
            self.samples[self.write_pos] = value;
            self.full = true;
        }
        self.write_pos = (self.write_pos + 1) % LATENCY_SAMPLES_CAP;
    }

    fn snapshot(&self) -> Vec<u64> {
        self.samples.clone()
    }
}

/// Latency samples for percentile computation using ring buffers.
struct LatencyRecorder {
    fast_path: LatencyBucket,
    slow_path: LatencyBucket,
    /// P2-3: Per-priority latency samples.
    priority_background: LatencyBucket,
    priority_normal: LatencyBucket,
    priority_high: LatencyBucket,
    priority_system: LatencyBucket,
    /// P2-3: SLA violation counters.
    sla_high_violations: u64,
    sla_normal_violations: u64,
    sla_total_violations: u64,
}

impl LatencyRecorder {
    const fn new() -> Self {
        Self {
            fast_path: LatencyBucket::new(),
            slow_path: LatencyBucket::new(),
            priority_background: LatencyBucket::new(),
            priority_normal: LatencyBucket::new(),
            priority_high: LatencyBucket::new(),
            priority_system: LatencyBucket::new(),
            sla_high_violations: 0,
            sla_normal_violations: 0,
            sla_total_violations: 0,
        }
    }

    fn record(&mut self, path: TxnPath, latency_us: u64) {
        match path {
            TxnPath::Fast => self.fast_path.push(latency_us),
            TxnPath::Slow => self.slow_path.push(latency_us),
        }
    }

    /// P2-3: Record latency by priority and check SLA violations.
    fn record_priority(&mut self, priority: TxnPriority, latency_us: u64) {
        match priority {
            TxnPriority::Background => {
                self.priority_background.push(latency_us);
                if latency_us > SLA_TARGET_BACKGROUND_US {
                    self.sla_total_violations += 1;
                }
            }
            TxnPriority::Normal => {
                self.priority_normal.push(latency_us);
                if latency_us > SLA_TARGET_NORMAL_US {
                    self.sla_normal_violations += 1;
                    self.sla_total_violations += 1;
                }
            }
            TxnPriority::High => {
                self.priority_high.push(latency_us);
                if latency_us > SLA_TARGET_HIGH_US {
                    self.sla_high_violations += 1;
                    self.sla_total_violations += 1;
                }
            }
            TxnPriority::System => {
                self.priority_system.push(latency_us);
            }
        }
    }

    fn compute_stats(&self) -> LatencyStats {
        let fast_snap = self.fast_path.snapshot();
        let slow_snap = self.slow_path.snapshot();
        let mut all: Vec<u64> = fast_snap.iter().chain(slow_snap.iter()).copied().collect();
        LatencyStats {
            fast_path: percentile_set(&fast_snap),
            slow_path: percentile_set(&slow_snap),
            all: {
                all.sort_unstable();
                compute_percentiles(&all)
            },
        }
    }

    fn compute_priority_stats(&self) -> PriorityLatencyStats {
        PriorityLatencyStats {
            background: percentile_set(&self.priority_background.snapshot()),
            normal: percentile_set(&self.priority_normal.snapshot()),
            high: percentile_set(&self.priority_high.snapshot()),
            system: percentile_set(&self.priority_system.snapshot()),
        }
    }

    const fn compute_sla_violations(&self) -> SlaViolationStats {
        SlaViolationStats {
            high_priority_violations: self.sla_high_violations,
            normal_priority_violations: self.sla_normal_violations,
            total_violations: self.sla_total_violations,
        }
    }

    fn reset(&mut self) {
        self.fast_path = LatencyBucket::new();
        self.slow_path = LatencyBucket::new();
        self.priority_background = LatencyBucket::new();
        self.priority_normal = LatencyBucket::new();
        self.priority_high = LatencyBucket::new();
        self.priority_system = LatencyBucket::new();
        self.sla_high_violations = 0;
        self.sla_normal_violations = 0;
        self.sla_total_violations = 0;
    }
}

fn percentile_set(samples: &[u64]) -> PercentileSet {
    let mut sorted: Vec<u64> = samples.to_vec();
    sorted.sort_unstable();
    compute_percentiles(&sorted)
}

fn compute_percentiles(sorted: &[u64]) -> PercentileSet {
    if sorted.is_empty() {
        return PercentileSet::default();
    }
    let n = sorted.len();
    // Nearest-rank method: index = ceil(n * p / 100) - 1, clamped to [0, n-1].
    let pct = |p_num: usize, p_den: usize| -> usize {
        let rank = (n * p_num).div_ceil(p_den); // ceiling division
        std::cmp::min(rank.saturating_sub(1), n - 1)
    };
    PercentileSet {
        count: n as u64,
        p50_us: sorted[pct(50, 100)],
        p95_us: sorted[pct(95, 100)],
        p99_us: sorted[pct(99, 100)],
        p999_us: sorted[pct(999, 1000)],
    }
}

/// Atomic counters for transaction statistics.
struct TxnStatsCollector {
    total_committed: AtomicU64,
    fast_path_commits: AtomicU64,
    slow_path_commits: AtomicU64,
    total_aborted: AtomicU64,
    occ_conflicts: AtomicU64,
    degraded_to_global: AtomicU64,
    constraint_violations: AtomicU64,
    admission_rejections: AtomicU64,
    /// Number of times an implicit fast→slow path upgrade was blocked.
    implicit_upgrade_blocked: AtomicU64,
}

impl TxnStatsCollector {
    const fn new() -> Self {
        Self {
            total_committed: AtomicU64::new(0),
            fast_path_commits: AtomicU64::new(0),
            slow_path_commits: AtomicU64::new(0),
            total_aborted: AtomicU64::new(0),
            occ_conflicts: AtomicU64::new(0),
            degraded_to_global: AtomicU64::new(0),
            constraint_violations: AtomicU64::new(0),
            admission_rejections: AtomicU64::new(0),
            implicit_upgrade_blocked: AtomicU64::new(0),
        }
    }

    fn record_fast_commit(&self) {
        self.total_committed.fetch_add(1, Ordering::Relaxed);
        self.fast_path_commits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_slow_commit(&self) {
        self.total_committed.fetch_add(1, Ordering::Relaxed);
        self.slow_path_commits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_abort(&self) {
        self.total_aborted.fetch_add(1, Ordering::Relaxed);
    }

    fn record_occ_conflict(&self) {
        self.occ_conflicts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_degradation(&self) {
        self.degraded_to_global.fetch_add(1, Ordering::Relaxed);
    }

    fn record_constraint_violation(&self) {
        self.constraint_violations.fetch_add(1, Ordering::Relaxed);
    }

    fn record_admission_rejection(&self) {
        self.admission_rejections.fetch_add(1, Ordering::Relaxed);
    }

    fn record_implicit_upgrade_blocked(&self) {
        self.implicit_upgrade_blocked
            .fetch_add(1, Ordering::Relaxed);
    }

    fn base_snapshot(&self, active_count: usize) -> TxnStatsSnapshot {
        let fast = self.fast_path_commits.load(Ordering::Relaxed);
        let total = self.total_committed.load(Ordering::Relaxed);
        let ratio = if total > 0 {
            fast as f64 / total as f64
        } else {
            f64::NAN
        };
        TxnStatsSnapshot {
            total_committed: total,
            fast_path_commits: fast,
            slow_path_commits: self.slow_path_commits.load(Ordering::Relaxed),
            total_aborted: self.total_aborted.load(Ordering::Relaxed),
            occ_conflicts: self.occ_conflicts.load(Ordering::Relaxed),
            degraded_to_global: self.degraded_to_global.load(Ordering::Relaxed),
            constraint_violations: self.constraint_violations.load(Ordering::Relaxed),
            admission_rejections: self.admission_rejections.load(Ordering::Relaxed),
            active_count,
            latency: LatencyStats::default(),
            fast_path_ratio: ratio,
            implicit_upgrade_blocked: self.implicit_upgrade_blocked.load(Ordering::Relaxed),
            priority_latency: PriorityLatencyStats::default(),
            sla_violations: SlaViolationStats::default(),
        }
    }
}

/// Maximum number of completed transaction records kept in the history ring buffer.
const TXN_HISTORY_CAPACITY: usize = 4096;

/// P1-4: Configurable threshold for slow transaction logging (microseconds).
/// Transactions exceeding this latency are recorded in the slow_txn_log.
const DEFAULT_SLOW_TXN_THRESHOLD_US: u64 = 100_000; // 100ms

/// P1-4: Maximum entries in the slow transaction log ring buffer.
const SLOW_TXN_LOG_CAPACITY: usize = 256;

/// Manages transaction lifecycle: begin, commit, abort, timestamp allocation.
pub struct TxnManager {
    /// Monotonic timestamp counter.
    ts_counter: AtomicU64,
    /// Shard-local fast-path timestamp counter.
    local_ts_counter: AtomicU64,
    /// Monotonic txn id counter.
    txn_counter: AtomicU64,
    /// Active transactions.
    active_txns: DashMap<TxnId, TxnHandle>,
    /// Reference to the storage engine for commit/abort propagation.
    storage: Arc<StorageEngine>,
    /// Transaction statistics collector.
    stats: TxnStatsCollector,
    /// Ring buffer of recently completed transactions.
    history: Mutex<TxnHistory>,
    /// Latency samples for percentile computation.
    latency: Mutex<LatencyRecorder>,
    /// P1-4: Slow transaction log — ring buffer of transactions exceeding the threshold.
    slow_txn_log: Mutex<TxnHistory>,
    /// P1-4: Slow transaction threshold in microseconds.
    slow_txn_threshold_us: AtomicU64,
    /// Admission gate: WAL backlog threshold in bytes (0 = disabled).
    /// When WAL backlog_bytes >= this, new write transactions are rejected.
    wal_backlog_threshold_bytes: AtomicU64,
    /// Admission gate: replication lag threshold (0 = disabled).
    /// When slowest replica lag >= this, new write transactions are rejected.
    replication_lag_threshold_ms: AtomicU64,
    /// Max age for idle-in-transaction sessions (ms, 0 = disabled).
    /// Transactions exceeding this are auto-aborted by the GC reaper.
    idle_in_txn_timeout_ms: AtomicU64,
    /// Optional HLC for cross-node causal ordering.
    /// When set, alloc_ts() delegates to HLC instead of the monotonic counter.
    hlc: Option<std::sync::Arc<falcon_common::hlc::HybridClock>>,
    /// Sampling divisor for latency/history recording (1 = record all, 64 = 1-in-64).
    latency_sample_divisor: u64,
    /// Named prepared transactions: GID → TxnId (for PREPARE TRANSACTION / COMMIT PREPARED).
    prepared_gids: DashMap<String, TxnId>,
    /// SSI predicate lock manager for Serializable isolation anti-dependency detection.
    ssi: SsiLockManager,
}

impl TxnManager {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            ts_counter: AtomicU64::new(1),
            local_ts_counter: AtomicU64::new(1),
            txn_counter: AtomicU64::new(1),
            active_txns: DashMap::new(),
            storage,
            stats: TxnStatsCollector::new(),
            history: Mutex::new(TxnHistory::new(TXN_HISTORY_CAPACITY)),
            latency: Mutex::new(LatencyRecorder::new()),
            slow_txn_log: Mutex::new(TxnHistory::new(SLOW_TXN_LOG_CAPACITY)),
            slow_txn_threshold_us: AtomicU64::new(DEFAULT_SLOW_TXN_THRESHOLD_US),
            wal_backlog_threshold_bytes: AtomicU64::new(0),
            replication_lag_threshold_ms: AtomicU64::new(0),
            idle_in_txn_timeout_ms: AtomicU64::new(0),
            hlc: None,
            latency_sample_divisor: 64,
            prepared_gids: DashMap::new(),
            ssi: SsiLockManager::new(),
        }
    }

    /// Create a TxnManager with HLC-based timestamp allocation.
    pub fn new_with_hlc(
        storage: Arc<StorageEngine>,
        hlc: std::sync::Arc<falcon_common::hlc::HybridClock>,
    ) -> Self {
        Self {
            ts_counter: AtomicU64::new(1),
            local_ts_counter: AtomicU64::new(1),
            txn_counter: AtomicU64::new(1),
            active_txns: DashMap::new(),
            storage,
            stats: TxnStatsCollector::new(),
            history: Mutex::new(TxnHistory::new(TXN_HISTORY_CAPACITY)),
            latency: Mutex::new(LatencyRecorder::new()),
            slow_txn_log: Mutex::new(TxnHistory::new(SLOW_TXN_LOG_CAPACITY)),
            slow_txn_threshold_us: AtomicU64::new(DEFAULT_SLOW_TXN_THRESHOLD_US),
            wal_backlog_threshold_bytes: AtomicU64::new(0),
            replication_lag_threshold_ms: AtomicU64::new(0),
            idle_in_txn_timeout_ms: AtomicU64::new(0),
            hlc: Some(hlc),
            latency_sample_divisor: 64,
            prepared_gids: DashMap::new(),
            ssi: SsiLockManager::new(),
        }
    }

    /// P1-4: Set the slow transaction threshold (microseconds).
    pub fn set_slow_txn_threshold_us(&self, us: u64) {
        self.slow_txn_threshold_us.store(us, Ordering::Relaxed);
    }

    /// P1-4: Get current slow transaction threshold (microseconds).
    pub fn slow_txn_threshold_us(&self) -> u64 {
        self.slow_txn_threshold_us.load(Ordering::Relaxed)
    }

    /// P1-4: Snapshot of slow transactions for SHOW falcon.slow_txns.
    pub fn slow_txn_snapshot(&self) -> Vec<TxnRecord> {
        self.slow_txn_log.lock().snapshot()
    }

    /// Allocate a new timestamp.
    /// Uses HLC when available (cluster mode), monotonic counter otherwise.
    pub fn alloc_ts(&self) -> Timestamp {
        if let Some(ref hlc) = self.hlc {
            let ts = hlc.now();
            // Keep monotonic counter in sync so current_ts() stays valid
            self.ts_counter.fetch_max(ts.0 + 1, Ordering::Relaxed);
            ts
        } else {
            Timestamp(self.ts_counter.fetch_add(1, Ordering::Relaxed))
        }
    }

    #[allow(dead_code)]
    fn alloc_local_ts(&self) -> Timestamp {
        Timestamp(self.local_ts_counter.fetch_add(1, Ordering::Relaxed))
    }

    /// Current (latest) timestamp without advancing.
    pub fn current_ts(&self) -> Timestamp {
        Timestamp(self.ts_counter.load(Ordering::Relaxed))
    }

    /// Seed prepared_gids from recovered WAL data after crash recovery.
    pub fn seed_prepared_gids(&self, gids: std::collections::HashMap<String, TxnId>) {
        for (gid, txn_id) in gids {
            self.prepared_gids.insert(gid, txn_id);
        }
    }

    /// Advance counters past recovered WAL values so new transactions
    /// get timestamps/ids higher than anything replayed during recovery.
    pub fn advance_counters_past(&self, max_ts: u64, max_txn_id: u64) {
        if max_ts > 0 {
            self.ts_counter.fetch_max(max_ts + 1, Ordering::Relaxed);
            self.local_ts_counter
                .fetch_max(max_ts + 1, Ordering::Relaxed);
            if let Some(ref hlc) = self.hlc {
                hlc.advance_past(Timestamp(max_ts));
            }
        }
        if max_txn_id > 0 {
            self.txn_counter
                .fetch_max(max_txn_id + 1, Ordering::Relaxed);
        }
    }

    /// Begin a new transaction.
    pub fn begin(&self, isolation: IsolationLevel) -> TxnHandle {
        self.begin_with_classification(isolation, TxnClassification::local(ShardId(0)))
    }

    /// Begin a transaction with admission control.
    /// Under PRESSURE state, write transactions are rejected (or delayed per policy).
    /// Under CRITICAL state, ALL new transactions are rejected.
    pub fn try_begin(&self, isolation: IsolationLevel) -> Result<TxnHandle, TxnError> {
        self.try_begin_with_classification(isolation, TxnClassification::local(ShardId(0)))
    }

    /// Begin a transaction with admission control and classification.
    ///
    /// Admission gates (checked in order, first failure rejects):
    /// 1. Memory pressure (Critical → reject all; Pressure → reject writes).
    /// 2. WAL backlog: if `wal_stats.backlog_bytes` exceeds the configured threshold,
    ///    new write transactions are rejected to prevent unbounded WAL growth.
    /// 3. Replication lag: if the slowest replica is lagging beyond the configured
    ///    threshold, new write transactions are rejected to prevent data loss on failover.
    pub fn try_begin_with_classification(
        &self,
        isolation: IsolationLevel,
        classification: TxnClassification,
    ) -> Result<TxnHandle, TxnError> {
        use falcon_storage::memory::PressureState;

        let dummy_txn_id = TxnId(0); // placeholder for error reporting

        // ── Gate 1: Memory pressure ──────────────────────────────────────
        let pressure = self.storage.pressure_state();
        match pressure {
            PressureState::Critical => {
                self.stats.record_admission_rejection();
                tracing::warn!("Admission control: rejecting txn (CRITICAL memory pressure)");
                return Err(TxnError::MemoryLimitExceeded(dummy_txn_id));
            }
            PressureState::Pressure => {
                // Under PRESSURE, only reject write-path (Global) transactions.
                // Local transactions may be read-only and should not be blocked,
                // because the transaction access mode is not yet known at begin time.
                if classification.txn_type == TxnType::Global {
                    self.stats.record_admission_rejection();
                    tracing::warn!(
                        "Admission control: rejecting global txn (PRESSURE memory state)"
                    );
                    return Err(TxnError::MemoryPressure(dummy_txn_id));
                }
                tracing::debug!("Admission control: allowing local txn under PRESSURE");
            }
            PressureState::Normal => {}
        }

        // ── Gate 2: WAL backlog ───────────────────────────────────────────
        // Only applies to write transactions (Global or Local with write intent).
        // We check the WAL stats backlog_bytes against the configured threshold.
        let wal_threshold = self.wal_backlog_threshold_bytes.load(Ordering::Relaxed);
        if wal_threshold > 0 {
            let wal_snap = self.storage.wal_stats_snapshot();
            if wal_snap.backlog_bytes >= wal_threshold {
                self.stats.record_admission_rejection();
                tracing::warn!(
                    "Admission control: rejecting txn (WAL backlog {}B >= threshold {}B)",
                    wal_snap.backlog_bytes,
                    wal_threshold,
                );
                return Err(TxnError::WalBacklogExceeded(dummy_txn_id));
            }
        }

        // ── Gate 3: Replication lag ───────────────────────────────────────
        let lag_threshold_ms = self.replication_lag_threshold_ms.load(Ordering::Relaxed);
        if lag_threshold_ms > 0 {
            let min_replica_ts = self.storage.replica_ack_tracker().min_replica_safe_ts();
            let current_ts = self.current_ts();
            // Lag in timestamp units (each unit ≈ 1 logical step; we treat it as ms for threshold).
            if min_replica_ts.0 < current_ts.0 {
                let lag = current_ts.0.saturating_sub(min_replica_ts.0);
                if lag >= lag_threshold_ms {
                    self.stats.record_admission_rejection();
                    tracing::warn!(
                        "Admission control: rejecting txn (replication lag {} >= threshold {})",
                        lag,
                        lag_threshold_ms,
                    );
                    return Err(TxnError::ReplicationLagExceeded(dummy_txn_id));
                }
            }
        }

        Ok(self.begin_with_classification(isolation, classification))
    }

    /// Set the WAL backlog admission threshold in bytes (0 = disabled).
    pub fn set_wal_backlog_threshold_bytes(&self, bytes: u64) {
        self.wal_backlog_threshold_bytes
            .store(bytes, Ordering::Relaxed);
    }

    /// Set the replication lag admission threshold (0 = disabled).
    pub fn set_replication_lag_threshold(&self, lag: u64) {
        self.replication_lag_threshold_ms
            .store(lag, Ordering::Relaxed);
    }

    /// Set idle-in-transaction timeout (ms, 0 = disabled).
    /// Transactions exceeding this age are auto-aborted by reap_long_transactions().
    pub fn set_idle_in_txn_timeout_ms(&self, ms: u64) {
        self.idle_in_txn_timeout_ms.store(ms, Ordering::Relaxed);
    }

    pub fn idle_in_txn_timeout_ms(&self) -> u64 {
        self.idle_in_txn_timeout_ms.load(Ordering::Relaxed)
    }

    /// Auto-abort transactions that have been active longer than idle_in_txn_timeout_ms.
    /// Skips Prepared (in-doubt) transactions — those must be resolved by the 2PC coordinator.
    /// Returns number of transactions reaped.
    pub fn reap_long_transactions(&self) -> usize {
        let timeout_ms = self.idle_in_txn_timeout_ms.load(Ordering::Relaxed);
        if timeout_ms == 0 {
            return 0;
        }
        let victims: Vec<TxnId> = self
            .active_txns
            .iter()
            .filter(|e| {
                let h = e.value();
                h.state == TxnState::Active && h.elapsed_ms() > timeout_ms
            })
            .map(|e| *e.key())
            .collect();

        let mut reaped = 0;
        for txn_id in victims {
            if let Ok(()) = self.abort_with_reason(txn_id, "idle_in_transaction_timeout") {
                tracing::warn!(%txn_id, timeout_ms, "reaped long-running transaction");
                reaped += 1;
            }
        }
        reaped
    }

    pub fn begin_with_classification(
        &self,
        isolation: IsolationLevel,
        classification: TxnClassification,
    ) -> TxnHandle {
        let txn_id = TxnId(self.txn_counter.fetch_add(1, Ordering::Relaxed));
        let start_ts = self.alloc_ts();

        let handle = TxnHandle {
            txn_id,
            start_ts,
            isolation,
            txn_type: classification.txn_type,
            path: if classification.txn_type == TxnType::Local {
                TxnPath::Fast
            } else {
                TxnPath::Slow
            },
            slow_path_mode: classification.slow_path_mode,
            involved_shards: classification.involved_shards,
            degraded: false,
            state: TxnState::Active,
            begin_instant: Some(Instant::now()),
            trace_id: txn_id.0,
            occ_retry_count: 0,
            tenant_id: SYSTEM_TENANT_ID,
            priority: TxnPriority::Normal,
            latency_breakdown: TxnLatencyBreakdown::default(),
            read_only: false,
            timeout_ms: 0,
            exec_summary: TxnExecSummary::default(),
        };

        self.active_txns.insert(txn_id, handle.clone());
        handle
    }

    /// Begin a read-only transaction pinned to an externally supplied snapshot timestamp.
    /// Used for consistent cross-shard reads: all shards receive the same `snapshot_ts`
    /// so they observe the same committed state.
    pub fn begin_at_snapshot(
        &self,
        snapshot_ts: Timestamp,
        isolation: IsolationLevel,
    ) -> TxnHandle {
        self.begin_at_snapshot_on_shard(snapshot_ts, isolation, ShardId(0))
    }

    /// Begin a transaction pinned to an external snapshot, assigned to a specific shard.
    pub fn begin_at_snapshot_on_shard(
        &self,
        snapshot_ts: Timestamp,
        isolation: IsolationLevel,
        shard_id: ShardId,
    ) -> TxnHandle {
        let txn_id = TxnId(self.txn_counter.fetch_add(1, Ordering::Relaxed));
        let handle = TxnHandle {
            txn_id,
            start_ts: snapshot_ts,
            isolation,
            txn_type: TxnType::Local,
            path: TxnPath::Fast,
            slow_path_mode: SlowPathMode::Xa2Pc,
            involved_shards: vec![shard_id],
            degraded: false,
            state: TxnState::Active,
            begin_instant: Some(Instant::now()),
            trace_id: txn_id.0,
            occ_retry_count: 0,
            tenant_id: SYSTEM_TENANT_ID,
            priority: TxnPriority::Normal,
            latency_breakdown: TxnLatencyBreakdown::default(),
            read_only: false,
            timeout_ms: 0,
            exec_summary: TxnExecSummary::default(),
        };
        self.active_txns.insert(txn_id, handle.clone());
        handle
    }

    /// Lightweight begin for autocommit queries. Skips DashMap insert,
    /// Instant::now(), and admission control. Caller must use commit_autocommit.
    pub fn begin_autocommit(&self, isolation: IsolationLevel) -> TxnHandle {
        let txn_id = TxnId(self.txn_counter.fetch_add(1, Ordering::Relaxed));
        let start_ts = self.alloc_ts();
        TxnHandle {
            txn_id,
            start_ts,
            isolation,
            txn_type: TxnType::Local,
            path: TxnPath::Fast,
            slow_path_mode: SlowPathMode::Xa2Pc,
            involved_shards: Vec::new(),
            degraded: false,
            state: TxnState::Active,
            begin_instant: None,
            trace_id: txn_id.0,
            occ_retry_count: 0,
            tenant_id: SYSTEM_TENANT_ID,
            priority: TxnPriority::Normal,
            latency_breakdown: TxnLatencyBreakdown::default(),
            read_only: false,
            timeout_ms: 0,
            exec_summary: TxnExecSummary::default(),
        }
    }

    /// Lightweight commit for autocommit write queries.
    pub fn commit_autocommit(&self, txn_id: TxnId) -> Result<Timestamp, TxnError> {
        let commit_ts = self.alloc_ts();
        if let Err(e) = self.storage.commit_txn(txn_id, commit_ts, TxnType::Local) {
            if matches!(e, StorageError::UniqueViolation { .. }) {
                self.stats.record_constraint_violation();
            }
            return Err(Self::storage_err_to_txn_err(txn_id, e));
        }
        self.stats.record_fast_commit();
        Ok(commit_ts)
    }

    /// Ultra-lightweight commit for read-only autocommit. Skips storage entirely.
    pub fn commit_autocommit_readonly(&self) {
        self.stats.record_fast_commit();
    }

    /// Lightweight abort for autocommit queries.
    pub fn abort_autocommit(&self, txn_id: TxnId) {
        let _ = self.storage.abort_txn(txn_id, TxnType::Local);
        self.stats.record_abort();
    }

    pub fn observe_involved_shards(
        &self,
        txn_id: TxnId,
        shards: &[ShardId],
    ) -> Result<(), TxnError> {
        let mut entry = self
            .active_txns
            .get_mut(&txn_id)
            .ok_or(TxnError::NotFound(txn_id))?;

        if entry.state != TxnState::Active {
            return Ok(());
        }

        for shard in shards {
            if !entry.involved_shards.contains(shard) {
                entry.involved_shards.push(*shard);
            }
        }

        if entry.involved_shards.len() > 1 && entry.txn_type == TxnType::Local {
            // P0-2: Track implicit fast→slow path upgrade.
            // This is an observable degradation — the txn started as single-shard
            // but touched additional shards at runtime.
            entry.txn_type = TxnType::Global;
            entry.path = TxnPath::Slow;
            entry.degraded = true;
            self.stats.record_degradation();
            self.stats.record_implicit_upgrade_blocked();
            tracing::warn!(
                "TXN {} implicitly upgraded from fast-path to slow-path (shards: {:?})",
                txn_id,
                entry.involved_shards
            );
        }

        Ok(())
    }

    pub fn force_global(&self, txn_id: TxnId, mode: SlowPathMode) -> Result<(), TxnError> {
        let mut entry = self
            .active_txns
            .get_mut(&txn_id)
            .ok_or(TxnError::NotFound(txn_id))?;

        if entry.state == TxnState::Active {
            entry.txn_type = TxnType::Global;
            entry.path = TxnPath::Slow;
            entry.slow_path_mode = mode;
            entry.degraded = true;
            self.stats.record_degradation();
        }

        Ok(())
    }

    pub fn get_txn(&self, txn_id: TxnId) -> Option<TxnHandle> {
        self.active_txns
            .get(&txn_id)
            .map(|entry| entry.value().clone())
    }

    /// Explicitly prepare a transaction for 2PC.
    /// **Invariant**: Only GlobalTxn may enter Prepared state.
    /// Calling this on a LocalTxn returns an InvariantViolation error.
    /// **Idempotent**: calling prepare on an already-Prepared txn is a no-op.
    pub fn prepare(&self, txn_id: TxnId) -> Result<(), TxnError> {
        let entry = self
            .active_txns
            .get(&txn_id)
            .ok_or(TxnError::NotFound(txn_id))?;

        if entry.txn_type == TxnType::Local {
            return Err(TxnError::InvariantViolation(
                txn_id,
                "LocalTxn cannot enter Prepared state".into(),
            ));
        }

        // P0-3: Idempotent — already Prepared is a successful no-op.
        if entry.state == TxnState::Prepared {
            return Ok(());
        }
        if entry.state != TxnState::Active {
            return Err(TxnError::AlreadyCommitted(txn_id));
        }

        drop(entry);

        // Delegate to storage
        self.storage
            .prepare_txn(txn_id)
            .map_err(|_| TxnError::Aborted(txn_id))?;

        if let Some(mut e) = self.active_txns.get_mut(&txn_id) {
            e.state = TxnState::Prepared;
        }

        Ok(())
    }

    /// PREPARE TRANSACTION 'gid': prepare the current txn and associate it with a global ID.
    /// The session detaches from the transaction; it can later be resolved via
    /// COMMIT PREPARED or ROLLBACK PREPARED from any session.
    pub fn prepare_named(&self, txn_id: TxnId, gid: String) -> Result<(), TxnError> {
        if self.prepared_gids.contains_key(&gid) {
            return Err(TxnError::InvariantViolation(
                txn_id,
                format!("transaction identifier \"{gid}\" is already in use"),
            ));
        }
        let entry = self
            .active_txns
            .get(&txn_id)
            .ok_or(TxnError::NotFound(txn_id))?;
        if entry.state == TxnState::Prepared {
            // Already prepared — just register the GID
            drop(entry);
            self.prepared_gids.insert(gid, txn_id);
            return Ok(());
        }
        if entry.state != TxnState::Active {
            return Err(TxnError::AlreadyCommitted(txn_id));
        }
        drop(entry);

        // Persist the prepare with GID to WAL for crash recovery
        self.storage
            .prepare_txn_named(txn_id, gid.clone())
            .map_err(|_| TxnError::Aborted(txn_id))?;

        if let Some(mut e) = self.active_txns.get_mut(&txn_id) {
            e.state = TxnState::Prepared;
        }
        self.prepared_gids.insert(gid, txn_id);
        Ok(())
    }

    /// COMMIT PREPARED 'gid': resolve a previously prepared transaction.
    pub fn commit_prepared(&self, gid: &str) -> Result<Timestamp, TxnError> {
        let txn_id = self
            .prepared_gids
            .remove(gid)
            .map(|(_, id)| id)
            .ok_or_else(|| {
                TxnError::InvariantViolation(
                    TxnId(0),
                    format!("prepared transaction \"{gid}\" does not exist"),
                )
            })?;
        self.commit(txn_id)
    }

    /// ROLLBACK PREPARED 'gid': abort a previously prepared transaction.
    pub fn rollback_prepared(&self, gid: &str) -> Result<(), TxnError> {
        let txn_id = self
            .prepared_gids
            .remove(gid)
            .map(|(_, id)| id)
            .ok_or_else(|| {
                TxnError::InvariantViolation(
                    TxnId(0),
                    format!("prepared transaction \"{gid}\" does not exist"),
                )
            })?;
        self.abort(txn_id)
    }

    /// Commit a transaction.
    ///
    /// For LocalTxn under SI/Serializable: validates the read-set (OCC) before
    /// committing. If a concurrent committed write modified any key in the
    /// read-set after `start_ts`, the transaction is aborted.
    ///
    /// LocalTxn: fast-path (no 2PC, no global coordination).
    /// GlobalTxn: slow-path (PREPARE → COMMIT with 2PC if configured).
    pub fn commit(&self, txn_id: TxnId) -> Result<Timestamp, TxnError> {
        let mut entry = self
            .active_txns
            .get_mut(&txn_id)
            .ok_or(TxnError::NotFound(txn_id))?;

        // P0-3: Idempotent — if already committed, return a synthetic ts.
        if entry.state == TxnState::Committed {
            return Ok(entry.start_ts);
        }
        if entry.state == TxnState::Aborted {
            return Err(TxnError::Aborted(txn_id));
        }
        // For Prepared txns entering commit via slow-path, we allow it below.
        if entry.state != TxnState::Active && entry.state != TxnState::Prepared {
            return Err(TxnError::AlreadyCommitted(txn_id));
        }

        // ── Hard invariant validation (M1: not bypassable) ──
        // Inlined from TxnContext::validate_commit_invariants to avoid Vec clone
        if entry.txn_type == TxnType::Local && entry.involved_shards.len() != 1 {
            let msg = format!(
                "LocalTxn {} has {} involved shards (must be exactly 1)",
                txn_id,
                entry.involved_shards.len()
            );
            tracing::error!("TXN invariant violation at commit: {}", msg);
            return Err(TxnError::InvariantViolation(txn_id, msg));
        }
        if entry.txn_type == TxnType::Local && entry.path == TxnPath::Slow {
            let msg = format!(
                "LocalTxn {} is on slow path (invariant: must be fast)",
                txn_id
            );
            tracing::error!("TXN invariant violation at commit: {}", msg);
            return Err(TxnError::InvariantViolation(txn_id, msg));
        }
        if entry.txn_type == TxnType::Global && entry.path == TxnPath::Fast {
            let msg = format!(
                "GlobalTxn {} is on fast path (invariant: must be slow)",
                txn_id
            );
            tracing::error!("TXN invariant violation at commit: {}", msg);
            return Err(TxnError::InvariantViolation(txn_id, msg));
        }

        let local_fast_path = entry.txn_type == TxnType::Local && entry.involved_shards.len() <= 1;
        let slow_mode = entry.slow_path_mode;
        let isolation = entry.isolation;
        let start_ts = entry.start_ts;
        let entry_begin_instant = entry.begin_instant;
        let shard_count = entry.involved_shards.len();
        let was_degraded = entry.degraded;
        let trace_id = entry.trace_id;
        let occ_retry_count = entry.occ_retry_count;
        let tenant_id = entry.tenant_id;
        let priority = entry.priority;
        let latency_breakdown = entry.latency_breakdown;

        if local_fast_path {
            // ── OCC validation under SI / Serializable ──
            // Allocate commit_ts FIRST, then validate. This closes the TOCTOU
            // window: any concurrent commit that obtains a higher ts cannot
            // have been visible at our start_ts, and any commit with ts in
            // (start_ts, commit_ts) is caught by validate_read_set.
            // A wasted ts on abort is harmless.
            let commit_ts = self.alloc_ts();
            if matches!(
                isolation,
                IsolationLevel::SnapshotIsolation | IsolationLevel::Serializable
            ) {
                drop(entry);
                if self.storage.validate_read_set(txn_id, start_ts).is_err() {
                    self.stats.record_occ_conflict();
                    self.ssi.remove_txn(txn_id);
                    self.abort(txn_id)?;
                    return Err(TxnError::SerializationConflict(txn_id));
                }

                // SSI: check rw-antidependency for Serializable transactions.
                // Register write intents, then detect if any committed txn's read
                // predicate overlaps our writes (T_committed →rw→ T_self).
                if isolation == IsolationLevel::Serializable {
                    let write_keys = self.storage.write_set_keys(txn_id);
                    if !write_keys.is_empty() {
                        for (table_id, key) in &write_keys {
                            self.ssi.add_write_intent(txn_id, SsiWriteIntent {
                                table_id: *table_id,
                                key: key.clone(),
                            });
                        }
                        let conflicts = self.ssi.check_rw_conflicts(txn_id);
                        if !conflicts.is_empty() {
                            self.stats.record_occ_conflict();
                            self.ssi.remove_txn(txn_id);
                            self.abort(txn_id)?;
                            return Err(TxnError::SerializationConflict(txn_id));
                        }
                    }
                }

                entry = self
                    .active_txns
                    .get_mut(&txn_id)
                    .ok_or(TxnError::NotFound(txn_id))?;
            }

            entry.path = TxnPath::Fast;
            // Note: state is set to Committed only AFTER storage confirms.
            drop(entry);

            if let Err(e) = self.storage.commit_txn(txn_id, commit_ts, TxnType::Local) {
                let latency_us = entry_begin_instant.map_or(0, |i| i.elapsed().as_micros() as u64);
                self.record_completed(TxnRecord {
                    txn_id,
                    txn_type: TxnType::Local,
                    txn_path: TxnPath::Fast,
                    shard_count,
                    start_ts,
                    commit_ts: None,
                    commit_latency_us: latency_us,
                    outcome: TxnOutcome::Aborted(format!("{e}")),
                    degraded: was_degraded,
                    trace_id,
                    occ_retry_count,
                    tenant_id,
                    priority,
                    latency_breakdown,
                });
                if matches!(e, StorageError::UniqueViolation { .. }) {
                    self.stats.record_constraint_violation();
                }
                self.ssi.remove_txn(txn_id);
                self.active_txns.remove(&txn_id);
                return Err(Self::storage_err_to_txn_err(txn_id, e));
            }

            // Skip redundant get_mut for state update — we remove immediately
            let latency_us = entry_begin_instant.map_or(0, |i| i.elapsed().as_micros() as u64);
            if isolation == IsolationLevel::Serializable {
                self.ssi.remove_txn(txn_id);
            }
            self.active_txns.remove(&txn_id);
            self.record_completed(TxnRecord {
                txn_id,
                txn_type: TxnType::Local,
                txn_path: TxnPath::Fast,
                shard_count,
                start_ts,
                commit_ts: Some(commit_ts),
                commit_latency_us: latency_us,
                outcome: TxnOutcome::Committed,
                degraded: was_degraded,
                trace_id,
                occ_retry_count,
                tenant_id,
                priority,
                latency_breakdown,
            });
            self.stats.record_fast_commit();
            return Ok(commit_ts);
        }

        // ── Slow-path global commit ──
        // State machine: only GlobalTxn may enter Prepared state.
        entry.txn_type = TxnType::Global;
        entry.path = TxnPath::Slow;
        entry.state = TxnState::Prepared;
        drop(entry);

        if matches!(slow_mode, SlowPathMode::Xa2Pc) {
            self.storage
                .prepare_txn(txn_id)
                .map_err(|_| TxnError::Aborted(txn_id))?;
        }
        // CP-L: prepare WAL record logged for global txn.
        tracing::debug!(
            txn_id = txn_id.0,
            cp = "CP-L",
            path = "slow",
            "consistency commit point: prepare WAL logged"
        );

        // Allocate commit_ts BEFORE OCC validation to close the TOCTOU window,
        // matching the local fast-path ordering. Any concurrent commit that
        // obtains a higher ts after this point cannot have been visible at
        // start_ts, and any commit with ts in (start_ts, commit_ts) is
        // caught by validate_read_set.
        let commit_ts = self.alloc_ts();

        // OCC validation for global path under SI/Serializable.
        if matches!(
            isolation,
            IsolationLevel::SnapshotIsolation | IsolationLevel::Serializable
        ) && self.storage.validate_read_set(txn_id, start_ts).is_err()
        {
            self.stats.record_occ_conflict();
            self.abort(txn_id)?;
            return Err(TxnError::SerializationConflict(txn_id));
        }

        if let Err(e) = self.storage.commit_txn(txn_id, commit_ts, TxnType::Global) {
            let latency_us = entry_begin_instant.map_or(0, |i| i.elapsed().as_micros() as u64);
            self.record_completed(TxnRecord {
                txn_id,
                txn_type: TxnType::Global,
                txn_path: TxnPath::Slow,
                shard_count,
                start_ts,
                commit_ts: None,
                commit_latency_us: latency_us,
                outcome: TxnOutcome::Aborted(format!("{e}")),
                degraded: was_degraded,
                trace_id,
                occ_retry_count,
                tenant_id,
                priority,
                latency_breakdown,
            });
            if matches!(e, StorageError::UniqueViolation { .. }) {
                self.stats.record_constraint_violation();
            }
            self.active_txns.remove(&txn_id);
            return Err(Self::storage_err_to_txn_err(txn_id, e));
        }

        let latency_us = entry_begin_instant.map_or(0, |i| i.elapsed().as_micros() as u64);
        self.record_completed(TxnRecord {
            txn_id,
            txn_type: TxnType::Global,
            txn_path: TxnPath::Slow,
            shard_count,
            start_ts,
            commit_ts: Some(commit_ts),
            commit_latency_us: latency_us,
            outcome: TxnOutcome::Committed,
            degraded: was_degraded,
            trace_id,
            occ_retry_count,
            tenant_id,
            priority,
            latency_breakdown,
        });
        // CP-D: storage confirmed — WAL durable for global txn.
        tracing::debug!(
            txn_id = txn_id.0,
            commit_ts = commit_ts.0,
            cp = "CP-D",
            path = "slow",
            "consistency commit point: WAL durable"
        );

        // CP-V: visible to readers — remove from active set directly
        self.active_txns.remove(&txn_id);
        tracing::debug!(
            txn_id = txn_id.0,
            commit_ts = commit_ts.0,
            cp = "CP-V",
            path = "slow",
            "consistency commit point: visible to readers"
        );
        self.stats.record_slow_commit();
        tracing::debug!(
            "TXN slow-commit(global): {} at {} latency={}us",
            txn_id,
            commit_ts,
            latency_us
        );
        Ok(commit_ts)
    }

    /// Abort a transaction.
    pub fn abort(&self, txn_id: TxnId) -> Result<(), TxnError> {
        self.abort_with_reason(txn_id, "explicit")
    }

    /// Abort a transaction with a specific reason string for observability.
    /// **Idempotent**: calling abort on an already-Aborted txn is a no-op.
    pub fn abort_with_reason(&self, txn_id: TxnId, reason: &str) -> Result<(), TxnError> {
        let mut entry = self
            .active_txns
            .get_mut(&txn_id)
            .ok_or(TxnError::NotFound(txn_id))?;

        // P0-3: Idempotent — already Aborted is a successful no-op.
        if entry.state == TxnState::Aborted {
            return Ok(());
        }
        if entry.state == TxnState::Committed {
            return Err(TxnError::AlreadyCommitted(txn_id));
        }

        let was_global = entry.txn_type == TxnType::Global;
        let txn_type_snap = entry.txn_type;
        let path_snap = entry.path;
        let shard_count = entry.involved_shards.len();
        let start_ts = entry.start_ts;
        let begin_instant = entry.begin_instant;
        let degraded = entry.degraded;
        let trace_id = entry.trace_id;
        let occ_retry_count = entry.occ_retry_count;
        let tenant_id = entry.tenant_id;
        let priority = entry.priority;
        let latency_breakdown = entry.latency_breakdown;
        entry.state = TxnState::Aborted;
        drop(entry);

        // Propagate abort to storage via unified API
        let txn_type = if was_global {
            TxnType::Global
        } else {
            TxnType::Local
        };
        let _ = self.storage.abort_txn(txn_id, txn_type);

        let latency_us = begin_instant.map_or(0, |i| i.elapsed().as_micros() as u64);
        self.record_completed(TxnRecord {
            txn_id,
            txn_type: txn_type_snap,
            txn_path: path_snap,
            shard_count,
            start_ts,
            commit_ts: None,
            commit_latency_us: latency_us,
            outcome: TxnOutcome::Aborted(reason.to_owned()),
            degraded,
            trace_id,
            occ_retry_count,
            tenant_id,
            priority,
            latency_breakdown,
        });

        self.ssi.remove_txn(txn_id);
        self.active_txns.remove(&txn_id);
        self.stats.record_abort();
        tracing::debug!("TXN abort: {} reason={}", txn_id, reason);
        Ok(())
    }

    /// Register a table-level read predicate for SSI anti-dependency tracking.
    /// Only has effect for Serializable transactions; no-op otherwise.
    pub fn register_ssi_read(&self, txn: &TxnHandle, table_id: u64) {
        if txn.isolation != IsolationLevel::Serializable {
            return;
        }
        self.ssi.add_predicate(txn.txn_id, crate::deadlock::SsiPredicate {
            table_id,
            range_start: None,
            range_end: None,
        });
    }

    /// Number of currently active transactions.
    pub fn active_count(&self) -> usize {
        self.active_txns.len()
    }

    /// Minimum active start timestamp (for GC watermark).
    pub fn min_active_ts(&self) -> Timestamp {
        self.active_txns
            .iter()
            .map(|e| e.value().start_ts)
            .min()
            .unwrap_or_else(|| self.current_ts())
    }

    /// Age of the longest-running active transaction in microseconds.
    /// Returns 0 if no active transactions.
    pub fn longest_txn_age_us(&self) -> u64 {
        self.active_txns
            .iter()
            .filter_map(|e| {
                e.value()
                    .begin_instant
                    .map(|i| i.elapsed().as_micros() as u64)
            })
            .max()
            .unwrap_or(0)
    }

    /// Number of currently Prepared (in-doubt) transactions.
    /// P0-3: These MUST NOT be GC'd — their resources are held until resolved.
    pub fn prepared_count(&self) -> usize {
        self.active_txns
            .iter()
            .filter(|e| e.value().state == TxnState::Prepared)
            .count()
    }

    /// GC safepoint diagnostic info.
    /// Returns (min_active_start_ts, current_ts, is_stalled).
    /// `is_stalled` is true when the safepoint hasn't moved because a long-running
    /// transaction is holding back GC (min_active_ts < current_ts - 1).
    /// P0-3: Prepared (undecided) txns are included — their start_ts pins the safepoint.
    pub fn gc_safepoint_info(&self) -> GcSafepointInfo {
        let current = self.current_ts();
        let min_active = self.min_active_ts();
        let active = self.active_txns.len();
        let prepared = self.prepared_count();
        // Stalled = there are active txns and safepoint is behind current by at least 2
        let stalled = active > 0 && min_active.0 + 1 < current.0;
        GcSafepointInfo {
            min_active_start_ts: min_active,
            current_ts: current,
            active_txn_count: active,
            prepared_txn_count: prepared,
            longest_txn_age_us: self.longest_txn_age_us(),
            stalled,
        }
    }

    /// Take a snapshot of transaction statistics for observability.
    /// Includes latency percentiles computed from the latency recorder.
    pub fn stats_snapshot(&self) -> TxnStatsSnapshot {
        let mut snap = self.stats.base_snapshot(self.active_txns.len());
        let lat = self.latency.lock();
        snap.latency = lat.compute_stats();
        snap.priority_latency = lat.compute_priority_stats();
        snap.sla_violations = lat.compute_sla_violations();
        snap
    }

    /// Get a snapshot of recent completed transaction records.
    pub fn txn_history_snapshot(&self) -> Vec<TxnRecord> {
        self.history.lock().snapshot()
    }

    /// Reset latency samples (e.g. between benchmark runs).
    pub fn reset_latency(&self) {
        self.latency.lock().reset();
    }

    /// Set sampling divisor for latency/history (1 = record every txn).
    pub fn set_latency_sample_divisor(&mut self, d: u64) {
        self.latency_sample_divisor = d.max(1);
    }

    fn record_completed(&self, record: TxnRecord) {
        let threshold = self.slow_txn_threshold_us.load(Ordering::Relaxed);
        let is_slow = threshold > 0 && record.commit_latency_us >= threshold;
        let sample = self.latency_sample_divisor <= 1
            || record.txn_id.0.is_multiple_of(self.latency_sample_divisor);

        if sample {
            if record.outcome == TxnOutcome::Committed {
                let mut lat = self.latency.lock();
                lat.record(record.txn_path, record.commit_latency_us);
                lat.record_priority(record.priority, record.commit_latency_us);
            }
            if is_slow {
                // Both history and slow_txn_log need it — clone once
                self.history.lock().push(record.clone());
                tracing::warn!(
                    txn_id = %record.txn_id,
                    latency_us = record.commit_latency_us,
                    "Slow transaction detected (threshold={}us)", threshold,
                );
                self.slow_txn_log.lock().push(record);
            } else {
                // Only history — move without clone
                self.history.lock().push(record);
            }
        } else if is_slow {
            tracing::warn!(
                txn_id = %record.txn_id,
                latency_us = record.commit_latency_us,
                "Slow transaction detected (threshold={}us)", threshold,
            );
            self.slow_txn_log.lock().push(record);
        }
        // Common case: !sample && !is_slow → record is dropped, zero allocation
    }

    /// B7: List transactions running longer than `threshold_us` microseconds.
    /// Returns (txn_id, age_us, state, txn_type) for each long-running txn.
    pub fn long_running_txns(&self, threshold_us: u64) -> Vec<(TxnId, u64, TxnState, TxnType)> {
        self.active_txns
            .iter()
            .filter_map(|e| {
                let age = e
                    .value()
                    .begin_instant
                    .map_or(0, |i| i.elapsed().as_micros() as u64);
                if age >= threshold_us {
                    Some((e.value().txn_id, age, e.value().state, e.value().txn_type))
                } else {
                    None
                }
            })
            .collect()
    }

    /// B7: Kill (force-abort) a long-running transaction.
    /// Returns Ok(()) if killed, Err if not found or already committed.
    pub fn kill_txn(&self, txn_id: TxnId) -> Result<(), TxnError> {
        self.abort_with_reason(txn_id, "killed: long-running")
    }

    /// B7: Kill all transactions running longer than `threshold_us`.
    /// Returns the number of transactions killed.
    pub fn kill_long_running(&self, threshold_us: u64) -> usize {
        let victims: Vec<TxnId> = self
            .long_running_txns(threshold_us)
            .into_iter()
            .map(|(id, _, _, _)| id)
            .collect();
        let mut killed = 0;
        for txn_id in victims {
            if self.kill_txn(txn_id).is_ok() {
                killed += 1;
            }
        }
        killed
    }

    /// Access the underlying storage engine (for wiring GcRunner etc.).
    pub const fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    /// Convert a StorageError from commit into the appropriate TxnError.
    /// UniqueViolation → ConstraintViolation (not retryable).
    /// Everything else → Aborted (generic).
    fn storage_err_to_txn_err(txn_id: TxnId, err: StorageError) -> TxnError {
        match err {
            StorageError::UniqueViolation {
                column_idx,
                ref index_key_hex,
            } => {
                tracing::warn!(
                    "TXN {} constraint violation: unique index on column {} key={}",
                    txn_id,
                    column_idx,
                    index_key_hex,
                );
                TxnError::ConstraintViolation(
                    txn_id,
                    format!(
                        "unique constraint on column {column_idx}: duplicate key {index_key_hex}",
                    ),
                )
            }
            _ => TxnError::Aborted(txn_id),
        }
    }
}

/// Implement SafepointProvider for TxnManager so GcRunner can query it directly.
/// - `min_active_ts`: from active transaction tracking
/// - `replica_safe_ts`: dynamically from the engine's ReplicaAckTracker
impl falcon_storage::gc::SafepointProvider for TxnManager {
    fn min_active_ts(&self) -> Timestamp {
        self.min_active_ts()
    }

    fn replica_safe_ts(&self) -> Timestamp {
        self.storage.replica_ack_tracker().min_replica_safe_ts()
    }

    fn reap_long_transactions(&self) -> usize {
        self.reap_long_transactions()
    }
}
