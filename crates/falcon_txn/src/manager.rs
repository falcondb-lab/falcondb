use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use parking_lot::Mutex;

use falcon_common::error::{StorageError, TxnError};
pub use falcon_common::types::{TxnPath, TxnType};
use falcon_common::types::{IsolationLevel, ShardId, Timestamp, TxnContext, TxnId};
use falcon_common::tenant::{TenantId, SYSTEM_TENANT_ID};
use falcon_common::security::TxnPriority;
use falcon_common::kernel::TxnLatencyBreakdown;
use falcon_storage::engine::StorageEngine;

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

    pub fn global(shards: Vec<ShardId>, mode: SlowPathMode) -> Self {
        Self {
            txn_type: TxnType::Global,
            involved_shards: shards,
            slow_path_mode: mode,
        }
    }
}

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Prepared,
    Committed,
    Aborted,
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
    pub fn record_read(&mut self, count: u64) {
        self.rows_read += count;
    }
    pub fn record_insert(&mut self, count: u64) {
        self.rows_inserted += count;
        self.rows_written += count;
    }
    pub fn record_update(&mut self, count: u64) {
        self.rows_updated += count;
        self.rows_written += count;
    }
    pub fn record_delete(&mut self, count: u64) {
        self.rows_deleted += count;
        self.rows_written += count;
    }
    pub fn record_statement(&mut self) {
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
    pub fn read_ts(&self, current_ts: Timestamp) -> Timestamp {
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
        if let Some(begin) = self.begin_instant {
            begin.elapsed().as_millis() as u64 >= self.timeout_ms
        } else {
            false
        }
    }

    /// Elapsed time since transaction began, in milliseconds.
    pub fn elapsed_ms(&self) -> u64 {
        self.begin_instant
            .map(|b| b.elapsed().as_millis() as u64)
            .unwrap_or(0)
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
const SLA_TARGET_HIGH_US: u64 = 10_000;      // 10ms for high-priority
const SLA_TARGET_NORMAL_US: u64 = 100_000;    // 100ms for normal
const SLA_TARGET_BACKGROUND_US: u64 = 1_000_000; // 1s for background

/// Sorted latency samples for percentile computation.
struct LatencyRecorder {
    fast_path: Vec<u64>,
    slow_path: Vec<u64>,
    /// P2-3: Per-priority latency samples.
    priority_background: Vec<u64>,
    priority_normal: Vec<u64>,
    priority_high: Vec<u64>,
    priority_system: Vec<u64>,
    /// P2-3: SLA violation counters.
    sla_high_violations: u64,
    sla_normal_violations: u64,
    sla_total_violations: u64,
}

impl LatencyRecorder {
    fn new() -> Self {
        Self {
            fast_path: Vec::new(),
            slow_path: Vec::new(),
            priority_background: Vec::new(),
            priority_normal: Vec::new(),
            priority_high: Vec::new(),
            priority_system: Vec::new(),
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
        let mut all: Vec<u64> = self.fast_path.iter().chain(self.slow_path.iter()).copied().collect();
        LatencyStats {
            fast_path: percentile_set(&self.fast_path),
            slow_path: percentile_set(&self.slow_path),
            all: {
                all.sort_unstable();
                compute_percentiles(&all)
            },
        }
    }

    fn compute_priority_stats(&self) -> PriorityLatencyStats {
        PriorityLatencyStats {
            background: percentile_set(&self.priority_background),
            normal: percentile_set(&self.priority_normal),
            high: percentile_set(&self.priority_high),
            system: percentile_set(&self.priority_system),
        }
    }

    fn compute_sla_violations(&self) -> SlaViolationStats {
        SlaViolationStats {
            high_priority_violations: self.sla_high_violations,
            normal_priority_violations: self.sla_normal_violations,
            total_violations: self.sla_total_violations,
        }
    }

    fn reset(&mut self) {
        self.fast_path.clear();
        self.slow_path.clear();
        self.priority_background.clear();
        self.priority_normal.clear();
        self.priority_high.clear();
        self.priority_system.clear();
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
    PercentileSet {
        count: n as u64,
        p50_us: sorted[n * 50 / 100],
        p95_us: sorted[std::cmp::min(n * 95 / 100, n - 1)],
        p99_us: sorted[std::cmp::min(n * 99 / 100, n - 1)],
        p999_us: sorted[std::cmp::min(n * 999 / 1000, n - 1)],
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
    fn new() -> Self {
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
        self.implicit_upgrade_blocked.fetch_add(1, Ordering::Relaxed);
    }

    fn base_snapshot(&self, active_count: usize) -> TxnStatsSnapshot {
        let fast = self.fast_path_commits.load(Ordering::Relaxed);
        let total = self.total_committed.load(Ordering::Relaxed);
        let ratio = if total > 0 { fast as f64 / total as f64 } else { f64::NAN };
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
    pub fn alloc_ts(&self) -> Timestamp {
        Timestamp(self.ts_counter.fetch_add(1, Ordering::SeqCst))
    }

    fn alloc_local_ts(&self) -> Timestamp {
        Timestamp(self.local_ts_counter.fetch_add(1, Ordering::SeqCst))
    }

    /// Current (latest) timestamp without advancing.
    pub fn current_ts(&self) -> Timestamp {
        Timestamp(self.ts_counter.load(Ordering::SeqCst))
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
                self.stats.record_admission_rejection();
                tracing::warn!("Admission control: rejecting txn (PRESSURE memory state)");
                return Err(TxnError::MemoryPressure(dummy_txn_id));
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
                    wal_snap.backlog_bytes, wal_threshold,
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
                        lag, lag_threshold_ms,
                    );
                    return Err(TxnError::ReplicationLagExceeded(dummy_txn_id));
                }
            }
        }

        Ok(self.begin_with_classification(isolation, classification))
    }

    /// Set the WAL backlog admission threshold in bytes (0 = disabled).
    pub fn set_wal_backlog_threshold_bytes(&self, bytes: u64) {
        self.wal_backlog_threshold_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Set the replication lag admission threshold (0 = disabled).
    pub fn set_replication_lag_threshold(&self, lag: u64) {
        self.replication_lag_threshold_ms.store(lag, Ordering::Relaxed);
    }

    pub fn begin_with_classification(
        &self,
        isolation: IsolationLevel,
        classification: TxnClassification,
    ) -> TxnHandle {
        let txn_id = TxnId(self.txn_counter.fetch_add(1, Ordering::SeqCst));
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
        tracing::debug!(
            "TXN begin: {} at {} type={:?} shards={:?}",
            txn_id,
            start_ts,
            handle.txn_type,
            handle.involved_shards
        );
        handle
    }

    pub fn observe_involved_shards(&self, txn_id: TxnId, shards: &[ShardId]) -> Result<(), TxnError> {
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
                txn_id, entry.involved_shards
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
        self.active_txns.get(&txn_id).map(|entry| entry.value().clone())
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
        let ctx = entry.to_context();
        if let Err(msg) = ctx.validate_commit_invariants() {
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
        let latency_breakdown = entry.latency_breakdown.clone();

        if local_fast_path {
            // ── Invariant: LocalTxn never enters Prepared ──
            if entry.state == TxnState::Prepared {
                return Err(TxnError::InvariantViolation(
                    txn_id,
                    "LocalTxn must never enter Prepared state".into(),
                ));
            }

            // ── OCC validation under SI / Serializable ──
            // Must validate BEFORE allocating commit_ts to avoid phantom commits.
            if matches!(isolation, IsolationLevel::SnapshotIsolation | IsolationLevel::Serializable) {
                // Drop the DashMap ref before calling into storage to avoid deadlocks.
                drop(entry);
                if self.storage.validate_read_set(txn_id, start_ts).is_err() {
                    // Validation failed — abort the transaction.
                    self.stats.record_occ_conflict();
                    self.abort(txn_id)?;
                    return Err(TxnError::SerializationConflict(txn_id));
                }
                // Re-acquire the entry after validation.
                entry = self
                    .active_txns
                    .get_mut(&txn_id)
                    .ok_or(TxnError::NotFound(txn_id))?;
            }

            let local_seq = self.alloc_local_ts();
            let commit_ts = self.alloc_ts();
            entry.path = TxnPath::Fast;
            entry.state = TxnState::Committed;
            drop(entry);

            // Fast-path local commit: no prepare/global coordination.
            if let Err(e) = self.storage.commit_txn(txn_id, commit_ts, TxnType::Local) {
                let latency_us = entry_begin_instant.map(|i| i.elapsed().as_micros() as u64).unwrap_or(0);
                self.record_completed(TxnRecord {
                    txn_id, txn_type: TxnType::Local, txn_path: TxnPath::Fast,
                    shard_count, start_ts,
                    commit_ts: None, commit_latency_us: latency_us,
                    outcome: TxnOutcome::Aborted(format!("{}", e)),
                    degraded: was_degraded,
                    trace_id, occ_retry_count, tenant_id, priority,
                    latency_breakdown: latency_breakdown.clone(),
                });
                if matches!(e, StorageError::UniqueViolation { .. }) {
                    self.stats.record_constraint_violation();
                }
                self.active_txns.remove(&txn_id);
                return Err(Self::storage_err_to_txn_err(txn_id, e));
            }

            let latency_us = entry_begin_instant.map(|i| i.elapsed().as_micros() as u64).unwrap_or(0);
            self.record_completed(TxnRecord {
                txn_id, txn_type: TxnType::Local, txn_path: TxnPath::Fast,
                shard_count, start_ts,
                commit_ts: Some(commit_ts), commit_latency_us: latency_us,
                outcome: TxnOutcome::Committed,
                degraded: was_degraded,
                trace_id, occ_retry_count, tenant_id, priority,
                latency_breakdown: latency_breakdown.clone(),
            });
            self.active_txns.remove(&txn_id);
            self.stats.record_fast_commit();
            tracing::debug!(
                "TXN fast-commit(local): {} local_seq={} commit_ts={} latency={}us",
                txn_id,
                local_seq,
                commit_ts,
                latency_us
            );
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

        // OCC validation for global path under SI/Serializable.
        if matches!(isolation, IsolationLevel::SnapshotIsolation | IsolationLevel::Serializable)
            && self.storage.validate_read_set(txn_id, start_ts).is_err()
        {
            self.stats.record_occ_conflict();
            self.abort(txn_id)?;
            return Err(TxnError::SerializationConflict(txn_id));
        }

        let commit_ts = self.alloc_ts();
        if let Some(mut prepared) = self.active_txns.get_mut(&txn_id) {
            prepared.state = TxnState::Committed;
        }

        if let Err(e) = self.storage.commit_txn(txn_id, commit_ts, TxnType::Global) {
            let latency_us = entry_begin_instant.map(|i| i.elapsed().as_micros() as u64).unwrap_or(0);
            self.record_completed(TxnRecord {
                txn_id, txn_type: TxnType::Global, txn_path: TxnPath::Slow,
                shard_count, start_ts,
                commit_ts: None, commit_latency_us: latency_us,
                outcome: TxnOutcome::Aborted(format!("{}", e)),
                degraded: was_degraded,
                trace_id, occ_retry_count, tenant_id, priority,
                latency_breakdown: latency_breakdown.clone(),
            });
            if matches!(e, StorageError::UniqueViolation { .. }) {
                self.stats.record_constraint_violation();
            }
            self.active_txns.remove(&txn_id);
            return Err(Self::storage_err_to_txn_err(txn_id, e));
        }

        let latency_us = entry_begin_instant.map(|i| i.elapsed().as_micros() as u64).unwrap_or(0);
        self.record_completed(TxnRecord {
            txn_id, txn_type: TxnType::Global, txn_path: TxnPath::Slow,
            shard_count, start_ts,
            commit_ts: Some(commit_ts), commit_latency_us: latency_us,
            outcome: TxnOutcome::Committed,
            degraded: was_degraded,
            trace_id, occ_retry_count, tenant_id, priority,
            latency_breakdown: latency_breakdown.clone(),
        });
        self.active_txns.remove(&txn_id);
        self.stats.record_slow_commit();
        tracing::debug!("TXN slow-commit(global): {} at {} latency={}us", txn_id, commit_ts, latency_us);
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
        let latency_breakdown = entry.latency_breakdown.clone();
        entry.state = TxnState::Aborted;
        drop(entry);

        // Propagate abort to storage via unified API
        let txn_type = if was_global { TxnType::Global } else { TxnType::Local };
        let _ = self.storage.abort_txn(txn_id, txn_type);

        let latency_us = begin_instant.map(|i| i.elapsed().as_micros() as u64).unwrap_or(0);
        self.record_completed(TxnRecord {
            txn_id, txn_type: txn_type_snap, txn_path: path_snap,
            shard_count, start_ts,
            commit_ts: None, commit_latency_us: latency_us,
            outcome: TxnOutcome::Aborted(reason.to_string()),
            degraded,
            trace_id, occ_retry_count, tenant_id, priority,
            latency_breakdown,
        });

        self.active_txns.remove(&txn_id);
        self.stats.record_abort();
        tracing::debug!("TXN abort: {} reason={}", txn_id, reason);
        Ok(())
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
            .unwrap_or(self.current_ts())
    }

    /// Age of the longest-running active transaction in microseconds.
    /// Returns 0 if no active transactions.
    pub fn longest_txn_age_us(&self) -> u64 {
        self.active_txns
            .iter()
            .filter_map(|e| e.value().begin_instant.map(|i| i.elapsed().as_micros() as u64))
            .max()
            .unwrap_or(0)
    }

    /// Number of currently Prepared (in-doubt) transactions.
    /// P0-3: These MUST NOT be GC'd — their resources are held until resolved.
    pub fn prepared_count(&self) -> usize {
        self.active_txns.iter().filter(|e| e.value().state == TxnState::Prepared).count()
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

    /// Record a completed transaction into history + latency.
    /// P1-4: Also checks slow txn threshold and logs to slow_txn_log.
    fn record_completed(&self, record: TxnRecord) {
        if record.outcome == TxnOutcome::Committed {
            let mut lat = self.latency.lock();
            lat.record(record.txn_path, record.commit_latency_us);
            lat.record_priority(record.priority, record.commit_latency_us);
        }
        // P1-4: Slow transaction detection
        let threshold = self.slow_txn_threshold_us.load(Ordering::Relaxed);
        if record.commit_latency_us >= threshold && threshold > 0 {
            tracing::warn!(
                txn_id = %record.txn_id,
                trace_id = record.trace_id,
                latency_us = record.commit_latency_us,
                path = ?record.txn_path,
                shards = record.shard_count,
                occ_retries = record.occ_retry_count,
                "Slow transaction detected (threshold={}us)", threshold,
            );
            self.slow_txn_log.lock().push(record.clone());
        }
        self.history.lock().push(record);
    }

    /// Access the underlying storage engine (for wiring GcRunner etc.).
    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    /// Convert a StorageError from commit into the appropriate TxnError.
    /// UniqueViolation → ConstraintViolation (not retryable).
    /// Everything else → Aborted (generic).
    fn storage_err_to_txn_err(txn_id: TxnId, err: StorageError) -> TxnError {
        match err {
            StorageError::UniqueViolation { column_idx, ref index_key_hex } => {
                tracing::warn!(
                    "TXN {} constraint violation: unique index on column {} key={}",
                    txn_id, column_idx, index_key_hex,
                );
                TxnError::ConstraintViolation(
                    txn_id,
                    format!(
                        "unique constraint on column {}: duplicate key {}",
                        column_idx, index_key_hex,
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
}
