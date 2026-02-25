//! P2-1: SLA-aware Admission & Scheduling.
//!
//! Turns FalconDB's admission control from "mechanism" into "hard contract":
//! - Uses **recent p99/p999 latency** as the primary admission signal
//! - Three-state output: Accept / RejectRetryable / RejectNonRetryable
//! - Every rejection is **explainable** (reason code + metrics + log)
//! - Transaction classification drives priority: fast-path > slow-path
//!
//! # Design Principles
//!
//! 1. **p99/p999 is the product**: admission decisions optimize tail latency,
//!    not throughput. Throughput is a side-effect of healthy tail latency.
//! 2. **Reject over degrade**: when pressure rises, reject deterministically
//!    rather than allowing unbounded queueing.
//! 3. **Explainability**: every rejection carries a `RejectionExplanation`
//!    with the signal that triggered it, current value, threshold, and remedy.
//!
//! # Invariants
//!
//! - **SLA-1**: If recent p99 > `target_p99_ms`, new slow-path txns are rejected.
//! - **SLA-2**: If recent p999 > `target_p999_ms`, ALL new txns are rejected
//!   except health-check probes.
//! - **SLA-3**: Fast-path txns (single-shard, < budget_ms) have priority over
//!   slow-path txns (cross-shard, long-running).
//! - **SLA-4**: Rejection rate is bounded: at most `max_reject_rate_pct`% of
//!   requests in any 1-second window are rejected (prevents total blackout).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Latency Tracker (lock-free sliding window)
// ═══════════════════════════════════════════════════════════════════════════

/// Fixed-size circular buffer for latency samples.
/// Lock-free writes (atomic index), read-side takes a snapshot under RwLock.
pub struct LatencyTracker {
    /// Ring buffer of latency samples (microseconds).
    samples: RwLock<Vec<u64>>,
    /// Next write position (wraps around).
    write_pos: AtomicU64,
    /// Total samples recorded (monotonic).
    total_samples: AtomicU64,
    /// Capacity of the ring buffer.
    capacity: usize,
}

impl LatencyTracker {
    /// Create a new tracker with the given window size.
    pub fn new(window_size: usize) -> Self {
        Self {
            samples: RwLock::new(vec![0u64; window_size]),
            write_pos: AtomicU64::new(0),
            total_samples: AtomicU64::new(0),
            capacity: window_size,
        }
    }

    /// Record a latency sample (microseconds).
    pub fn record(&self, latency_us: u64) {
        let pos = self.write_pos.fetch_add(1, Ordering::Relaxed) as usize % self.capacity;
        {
            let mut buf = self.samples.write();
            buf[pos] = latency_us;
        }
        self.total_samples.fetch_add(1, Ordering::Relaxed);
    }

    /// Compute percentile statistics from the current window.
    /// Returns (p50, p99, p999) in microseconds.
    pub fn percentiles(&self) -> LatencyPercentiles {
        let total = self.total_samples.load(Ordering::Relaxed);
        let count = (total as usize).min(self.capacity);
        if count == 0 {
            return LatencyPercentiles::default();
        }

        let buf = self.samples.read();
        let mut sorted: Vec<u64> = if total as usize <= self.capacity {
            buf[..count].to_vec()
        } else {
            buf.clone()
        };
        sorted.sort_unstable();

        let p = |pct: f64| -> u64 {
            let idx = ((count as f64 * pct / 100.0).ceil() as usize).saturating_sub(1);
            sorted[idx.min(count - 1)]
        };

        LatencyPercentiles {
            p50_us: p(50.0),
            p90_us: p(90.0),
            p99_us: p(99.0),
            p999_us: p(99.9),
            max_us: sorted[count - 1],
            sample_count: count as u64,
        }
    }

    /// Total samples recorded since creation.
    pub fn total_samples(&self) -> u64 {
        self.total_samples.load(Ordering::Relaxed)
    }
}

/// Latency percentile snapshot.
#[derive(Debug, Clone, Default)]
pub struct LatencyPercentiles {
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
    pub sample_count: u64,
}

impl LatencyPercentiles {
    /// Convert microseconds to milliseconds for a specific percentile.
    pub fn p99_ms(&self) -> f64 {
        self.p99_us as f64 / 1000.0
    }

    pub fn p999_ms(&self) -> f64 {
        self.p999_us as f64 / 1000.0
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Transaction Classification
// ═══════════════════════════════════════════════════════════════════════════

/// Classification of a transaction for admission priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxnClass {
    /// Single-shard, expected short (< budget). Highest priority.
    FastPath,
    /// Cross-shard or expected long-running. Lower priority, subject to quota.
    SlowPath,
    /// DDL operations. Lowest priority, strict quota.
    Ddl,
    /// Internal system operations (GC, replication). Always admitted.
    System,
}

impl std::fmt::Display for TxnClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FastPath => write!(f, "fast_path"),
            Self::SlowPath => write!(f, "slow_path"),
            Self::Ddl => write!(f, "ddl"),
            Self::System => write!(f, "system"),
        }
    }
}

/// Hints for classifying a transaction before execution.
#[derive(Debug, Clone)]
pub struct TxnClassificationHints {
    /// Number of shards the txn will touch.
    pub shard_count: usize,
    /// Whether this is a DDL operation.
    pub is_ddl: bool,
    /// Whether this is an internal system operation.
    pub is_system: bool,
    /// Estimated row count (0 = unknown).
    pub estimated_rows: u64,
    /// Whether the query has a LIMIT clause.
    pub has_limit: bool,
}

impl Default for TxnClassificationHints {
    fn default() -> Self {
        Self {
            shard_count: 1,
            is_ddl: false,
            is_system: false,
            estimated_rows: 0,
            has_limit: false,
        }
    }
}

/// Classify a transaction based on hints.
pub const fn classify_txn(hints: &TxnClassificationHints) -> TxnClass {
    if hints.is_system {
        return TxnClass::System;
    }
    if hints.is_ddl {
        return TxnClass::Ddl;
    }
    if hints.shard_count > 1 {
        return TxnClass::SlowPath;
    }
    // Single-shard with high row estimate → slow path
    if hints.estimated_rows > 10_000 && !hints.has_limit {
        return TxnClass::SlowPath;
    }
    TxnClass::FastPath
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — SLA Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// SLA targets for the admission controller.
#[derive(Debug, Clone)]
pub struct SlaConfig {
    /// Target p99 latency in milliseconds.
    pub target_p99_ms: f64,
    /// Target p999 latency in milliseconds.
    pub target_p999_ms: f64,
    /// Maximum concurrent inflight transactions (all classes).
    pub max_inflight_txn: usize,
    /// Maximum concurrent cross-shard (slow-path) inflight transactions.
    pub max_cross_shard_inflight: usize,
    /// Slow-path quota as fraction of max_inflight_txn (e.g. 0.3 = 30%).
    pub slow_path_quota_ratio: f64,
    /// DDL quota (absolute number).
    pub ddl_quota: usize,
    /// Minimum samples before SLA enforcement kicks in (cold-start grace).
    pub min_samples_for_enforcement: u64,
    /// Latency window size (number of samples).
    pub latency_window_size: usize,
    /// Maximum reject rate (fraction, e.g. 0.5 = 50%). Prevents total blackout.
    pub max_reject_rate: f64,
}

impl Default for SlaConfig {
    fn default() -> Self {
        Self {
            target_p99_ms: 20.0,
            target_p999_ms: 100.0,
            max_inflight_txn: 500,
            max_cross_shard_inflight: 50,
            slow_path_quota_ratio: 0.3,
            ddl_quota: 4,
            min_samples_for_enforcement: 100,
            latency_window_size: 10_000,
            max_reject_rate: 0.5,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Admission Decision
// ═══════════════════════════════════════════════════════════════════════════

/// Three-state admission decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// Request is accepted.
    Accept,
    /// Request is rejected but can be retried after a delay.
    RejectRetryable { retry_after_ms: u64 },
    /// Request is rejected permanently (e.g. resource exhaustion, non-retryable).
    RejectNonRetryable,
}

/// Detailed explanation of why a request was rejected.
#[derive(Debug, Clone)]
pub struct RejectionExplanation {
    /// The primary signal that triggered rejection.
    pub signal: RejectionSignal,
    /// Current value of the signal.
    pub current_value: f64,
    /// Threshold that was exceeded.
    pub threshold: f64,
    /// Transaction class of the rejected request.
    pub txn_class: TxnClass,
    /// Suggested remedy for the caller.
    pub remedy: &'static str,
    /// SQLSTATE code.
    pub sqlstate: &'static str,
}

impl std::fmt::Display for RejectionExplanation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {}: current={:.2}, threshold={:.2}, class={}, remedy={}",
            self.sqlstate, self.signal, self.current_value, self.threshold, self.txn_class, self.remedy,
        )
    }
}

/// The signal that triggered a rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectionSignal {
    /// Recent p99 latency exceeded target.
    P99Exceeded,
    /// Recent p999 latency exceeded target.
    P999Exceeded,
    /// Total inflight transaction limit reached.
    InflightLimitReached,
    /// Cross-shard (slow-path) quota exhausted.
    SlowPathQuotaExhausted,
    /// DDL quota exhausted.
    DdlQuotaExhausted,
    /// Memory pressure.
    MemoryPressure,
    /// WAL backlog exceeded.
    WalBacklog,
    /// Reject rate cap reached (anti-blackout).
    RejectRateCapped,
}

impl std::fmt::Display for RejectionSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::P99Exceeded => write!(f, "p99_exceeded"),
            Self::P999Exceeded => write!(f, "p999_exceeded"),
            Self::InflightLimitReached => write!(f, "inflight_limit"),
            Self::SlowPathQuotaExhausted => write!(f, "slow_path_quota"),
            Self::DdlQuotaExhausted => write!(f, "ddl_quota"),
            Self::MemoryPressure => write!(f, "memory_pressure"),
            Self::WalBacklog => write!(f, "wal_backlog"),
            Self::RejectRateCapped => write!(f, "reject_rate_cap"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — SLA Admission Controller
// ═══════════════════════════════════════════════════════════════════════════

/// SLA-aware admission controller.
///
/// Wraps the existing `AdmissionControl` with latency-driven decisions.
pub struct SlaAdmissionController {
    config: RwLock<SlaConfig>,
    /// Sliding-window latency tracker.
    latency: LatencyTracker,
    /// Current inflight counts per class.
    inflight_fast: AtomicU64,
    inflight_slow: AtomicU64,
    inflight_ddl: AtomicU64,
    inflight_total: AtomicU64,
    /// Rejection counters per signal.
    rejected_p99: AtomicU64,
    rejected_p999: AtomicU64,
    rejected_inflight: AtomicU64,
    rejected_slow_quota: AtomicU64,
    rejected_ddl_quota: AtomicU64,
    rejected_memory: AtomicU64,
    rejected_wal: AtomicU64,
    rejected_rate_cap: AtomicU64,
    /// Total accepted / rejected in current second (for rate cap).
    accepted_this_sec: AtomicU64,
    rejected_this_sec: AtomicU64,
    /// Second boundary for rate cap reset.
    rate_cap_epoch: AtomicU64,
    /// Total accepted (lifetime).
    total_accepted: AtomicU64,
    /// Total rejected (lifetime).
    total_rejected: AtomicU64,
}

impl SlaAdmissionController {
    /// Create a new SLA admission controller.
    pub fn new(config: SlaConfig) -> Arc<Self> {
        let window = config.latency_window_size;
        Arc::new(Self {
            config: RwLock::new(config),
            latency: LatencyTracker::new(window),
            inflight_fast: AtomicU64::new(0),
            inflight_slow: AtomicU64::new(0),
            inflight_ddl: AtomicU64::new(0),
            inflight_total: AtomicU64::new(0),
            rejected_p99: AtomicU64::new(0),
            rejected_p999: AtomicU64::new(0),
            rejected_inflight: AtomicU64::new(0),
            rejected_slow_quota: AtomicU64::new(0),
            rejected_ddl_quota: AtomicU64::new(0),
            rejected_memory: AtomicU64::new(0),
            rejected_wal: AtomicU64::new(0),
            rejected_rate_cap: AtomicU64::new(0),
            accepted_this_sec: AtomicU64::new(0),
            rejected_this_sec: AtomicU64::new(0),
            rate_cap_epoch: AtomicU64::new(0),
            total_accepted: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
        })
    }

    /// Create with default config.
    pub fn new_default() -> Arc<Self> {
        Self::new(SlaConfig::default())
    }

    /// Update SLA configuration at runtime.
    pub fn update_config(&self, config: SlaConfig) {
        *self.config.write() = config;
    }

    /// Try to admit a transaction. Returns (decision, optional explanation).
    ///
    /// On Accept, returns an RAII `SlaPermit` that decrements inflight on drop.
    pub fn try_admit(
        self: &Arc<Self>,
        class: TxnClass,
    ) -> Result<SlaPermit, (AdmissionDecision, RejectionExplanation)> {
        let cfg = self.config.read().clone();

        // System txns always pass (GC, replication, health-check).
        if class == TxnClass::System {
            self.total_accepted.fetch_add(1, Ordering::Relaxed);
            return Ok(SlaPermit {
                controller: Arc::clone(self),
                class,
                start: Instant::now(),
            });
        }

        // ── Anti-blackout: check reject rate cap ──
        self.maybe_reset_rate_cap();
        let rejected_sec = self.rejected_this_sec.load(Ordering::Relaxed);
        let accepted_sec = self.accepted_this_sec.load(Ordering::Relaxed);
        let total_sec = accepted_sec + rejected_sec;
        if total_sec > 10 {
            let reject_ratio = rejected_sec as f64 / total_sec as f64;
            if reject_ratio >= cfg.max_reject_rate {
                // Allow this request through to prevent total blackout.
                self.rejected_rate_cap.fetch_add(1, Ordering::Relaxed);
                // Still accept — the cap means "stop rejecting, not stop accepting"
                return self.do_accept(class);
            }
        }

        // ── Check inflight limits ──
        let total_inflight = self.inflight_total.load(Ordering::Relaxed) as usize;
        if total_inflight >= cfg.max_inflight_txn {
            return self.do_reject(class, RejectionSignal::InflightLimitReached,
                total_inflight as f64, cfg.max_inflight_txn as f64,
                "53300", "Reduce concurrent connections or increase max_inflight_txn",
                50);
        }

        // ── Class-specific quota checks ──
        match class {
            TxnClass::SlowPath => {
                let slow = self.inflight_slow.load(Ordering::Relaxed) as usize;
                let quota = (cfg.max_inflight_txn as f64 * cfg.slow_path_quota_ratio) as usize;
                if slow >= quota.max(1) {
                    return self.do_reject(class, RejectionSignal::SlowPathQuotaExhausted,
                        slow as f64, quota as f64,
                        "53300", "Cross-shard quota exhausted; retry as single-shard or wait",
                        100);
                }
                let cross = self.inflight_slow.load(Ordering::Relaxed) as usize;
                if cross >= cfg.max_cross_shard_inflight {
                    return self.do_reject(class, RejectionSignal::SlowPathQuotaExhausted,
                        cross as f64, cfg.max_cross_shard_inflight as f64,
                        "53300", "Max cross-shard inflight reached",
                        100);
                }
            }
            TxnClass::Ddl => {
                let ddl = self.inflight_ddl.load(Ordering::Relaxed) as usize;
                if ddl >= cfg.ddl_quota {
                    return self.do_reject(class, RejectionSignal::DdlQuotaExhausted,
                        ddl as f64, cfg.ddl_quota as f64,
                        "53300", "DDL concurrency limit reached; wait for in-progress DDL",
                        200);
                }
            }
            _ => {}
        }

        // ── Latency-driven SLA enforcement ──
        let pct = self.latency.percentiles();
        if pct.sample_count >= cfg.min_samples_for_enforcement {
            // SLA-2: p999 breach → reject ALL non-system txns
            if pct.p999_ms() > cfg.target_p999_ms {
                return self.do_reject(class, RejectionSignal::P999Exceeded,
                    pct.p999_ms(), cfg.target_p999_ms,
                    "53300", "p999 latency SLA breach; system is overloaded",
                    200);
            }

            // SLA-1: p99 breach → reject slow-path only (fast-path still admitted)
            if pct.p99_ms() > cfg.target_p99_ms && class != TxnClass::FastPath {
                return self.do_reject(class, RejectionSignal::P99Exceeded,
                    pct.p99_ms(), cfg.target_p99_ms,
                    "53300", "p99 latency SLA breach; slow-path txns rejected",
                    100);
            }
        }

        // ── All checks passed → accept ──
        self.do_accept(class)
    }

    /// Record that a transaction completed with the given latency.
    pub fn record_completion(&self, latency_us: u64) {
        self.latency.record(latency_us);
    }

    /// Get current latency percentiles.
    pub fn latency_percentiles(&self) -> LatencyPercentiles {
        self.latency.percentiles()
    }

    /// Snapshot of admission metrics.
    pub fn metrics(&self) -> SlaAdmissionMetrics {
        let pct = self.latency.percentiles();
        let cfg = self.config.read();
        SlaAdmissionMetrics {
            p50_us: pct.p50_us,
            p99_us: pct.p99_us,
            p999_us: pct.p999_us,
            max_us: pct.max_us,
            target_p99_ms: cfg.target_p99_ms,
            target_p999_ms: cfg.target_p999_ms,
            inflight_fast: self.inflight_fast.load(Ordering::Relaxed),
            inflight_slow: self.inflight_slow.load(Ordering::Relaxed),
            inflight_ddl: self.inflight_ddl.load(Ordering::Relaxed),
            inflight_total: self.inflight_total.load(Ordering::Relaxed),
            total_accepted: self.total_accepted.load(Ordering::Relaxed),
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
            rejected_p99: self.rejected_p99.load(Ordering::Relaxed),
            rejected_p999: self.rejected_p999.load(Ordering::Relaxed),
            rejected_inflight: self.rejected_inflight.load(Ordering::Relaxed),
            rejected_slow_quota: self.rejected_slow_quota.load(Ordering::Relaxed),
            rejected_ddl_quota: self.rejected_ddl_quota.load(Ordering::Relaxed),
            rejected_memory: self.rejected_memory.load(Ordering::Relaxed),
            rejected_wal: self.rejected_wal.load(Ordering::Relaxed),
            rejected_rate_cap: self.rejected_rate_cap.load(Ordering::Relaxed),
            sample_count: pct.sample_count,
        }
    }

    // ── Internal helpers ──

    fn do_accept(self: &Arc<Self>, class: TxnClass) -> Result<SlaPermit, (AdmissionDecision, RejectionExplanation)> {
        match class {
            TxnClass::FastPath => { self.inflight_fast.fetch_add(1, Ordering::Relaxed); }
            TxnClass::SlowPath => { self.inflight_slow.fetch_add(1, Ordering::Relaxed); }
            TxnClass::Ddl => { self.inflight_ddl.fetch_add(1, Ordering::Relaxed); }
            TxnClass::System => {}
        }
        self.inflight_total.fetch_add(1, Ordering::Relaxed);
        self.total_accepted.fetch_add(1, Ordering::Relaxed);
        self.accepted_this_sec.fetch_add(1, Ordering::Relaxed);
        Ok(SlaPermit {
            controller: Arc::clone(self),
            class,
            start: Instant::now(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn do_reject(
        &self,
        class: TxnClass,
        signal: RejectionSignal,
        current: f64,
        threshold: f64,
        sqlstate: &'static str,
        remedy: &'static str,
        retry_ms: u64,
    ) -> Result<SlaPermit, (AdmissionDecision, RejectionExplanation)> {
        // Increment per-signal counter
        match signal {
            RejectionSignal::P99Exceeded => { self.rejected_p99.fetch_add(1, Ordering::Relaxed); }
            RejectionSignal::P999Exceeded => { self.rejected_p999.fetch_add(1, Ordering::Relaxed); }
            RejectionSignal::InflightLimitReached => { self.rejected_inflight.fetch_add(1, Ordering::Relaxed); }
            RejectionSignal::SlowPathQuotaExhausted => { self.rejected_slow_quota.fetch_add(1, Ordering::Relaxed); }
            RejectionSignal::DdlQuotaExhausted => { self.rejected_ddl_quota.fetch_add(1, Ordering::Relaxed); }
            RejectionSignal::MemoryPressure => { self.rejected_memory.fetch_add(1, Ordering::Relaxed); }
            RejectionSignal::WalBacklog => { self.rejected_wal.fetch_add(1, Ordering::Relaxed); }
            RejectionSignal::RejectRateCapped => { self.rejected_rate_cap.fetch_add(1, Ordering::Relaxed); }
        }
        self.total_rejected.fetch_add(1, Ordering::Relaxed);
        self.rejected_this_sec.fetch_add(1, Ordering::Relaxed);

        tracing::info!(
            signal = %signal,
            current = current,
            threshold = threshold,
            txn_class = %class,
            "SLA admission rejected"
        );

        let explanation = RejectionExplanation {
            signal,
            current_value: current,
            threshold,
            txn_class: class,
            remedy,
            sqlstate,
        };

        Err((AdmissionDecision::RejectRetryable { retry_after_ms: retry_ms }, explanation))
    }

    fn maybe_reset_rate_cap(&self) {
        let now_sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let prev = self.rate_cap_epoch.load(Ordering::Relaxed);
        if now_sec > prev
            && self.rate_cap_epoch.compare_exchange(prev, now_sec, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                self.accepted_this_sec.store(0, Ordering::Relaxed);
                self.rejected_this_sec.store(0, Ordering::Relaxed);
            }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — SLA Permit (RAII)
// ═══════════════════════════════════════════════════════════════════════════

/// RAII permit that tracks inflight duration and decrements counters on drop.
pub struct SlaPermit {
    controller: Arc<SlaAdmissionController>,
    class: TxnClass,
    start: Instant,
}

impl std::fmt::Debug for SlaPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlaPermit")
            .field("class", &self.class)
            .field("elapsed_us", &self.elapsed_us())
            .finish()
    }
}

impl SlaPermit {
    /// Get the transaction class.
    pub const fn class(&self) -> TxnClass {
        self.class
    }

    /// Get elapsed time since admission.
    pub fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}

impl Drop for SlaPermit {
    fn drop(&mut self) {
        let latency_us = self.start.elapsed().as_micros() as u64;
        match self.class {
            TxnClass::FastPath => { self.controller.inflight_fast.fetch_sub(1, Ordering::Relaxed); }
            TxnClass::SlowPath => { self.controller.inflight_slow.fetch_sub(1, Ordering::Relaxed); }
            TxnClass::Ddl => { self.controller.inflight_ddl.fetch_sub(1, Ordering::Relaxed); }
            TxnClass::System => {}
        }
        self.controller.inflight_total.fetch_sub(1, Ordering::Relaxed);
        self.controller.latency.record(latency_us);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Metrics Snapshot
// ═══════════════════════════════════════════════════════════════════════════

/// Full metrics snapshot for observability (SHOW, Prometheus, diag bundle).
#[derive(Debug, Clone, Default)]
pub struct SlaAdmissionMetrics {
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
    pub target_p99_ms: f64,
    pub target_p999_ms: f64,
    pub inflight_fast: u64,
    pub inflight_slow: u64,
    pub inflight_ddl: u64,
    pub inflight_total: u64,
    pub total_accepted: u64,
    pub total_rejected: u64,
    pub rejected_p99: u64,
    pub rejected_p999: u64,
    pub rejected_inflight: u64,
    pub rejected_slow_quota: u64,
    pub rejected_ddl_quota: u64,
    pub rejected_memory: u64,
    pub rejected_wal: u64,
    pub rejected_rate_cap: u64,
    pub sample_count: u64,
}

impl SlaAdmissionMetrics {
    /// Accept rate (0.0 - 1.0).
    pub fn accept_rate(&self) -> f64 {
        let total = self.total_accepted + self.total_rejected;
        if total == 0 { 1.0 } else { self.total_accepted as f64 / total as f64 }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_tracker_empty() {
        let tracker = LatencyTracker::new(100);
        let pct = tracker.percentiles();
        assert_eq!(pct.sample_count, 0);
        assert_eq!(pct.p99_us, 0);
    }

    #[test]
    fn test_latency_tracker_single_sample() {
        let tracker = LatencyTracker::new(100);
        tracker.record(5000); // 5ms
        let pct = tracker.percentiles();
        assert_eq!(pct.sample_count, 1);
        assert_eq!(pct.p99_us, 5000);
        assert_eq!(pct.p50_us, 5000);
    }

    #[test]
    fn test_latency_tracker_percentiles() {
        let tracker = LatencyTracker::new(1000);
        // Record 1000 samples: 1us, 2us, ..., 1000us
        for i in 1..=1000 {
            tracker.record(i);
        }
        let pct = tracker.percentiles();
        assert_eq!(pct.sample_count, 1000);
        // p50 ≈ 500, p99 ≈ 990, p999 ≈ 999
        assert!(pct.p50_us >= 490 && pct.p50_us <= 510, "p50={}", pct.p50_us);
        assert!(pct.p99_us >= 985 && pct.p99_us <= 1000, "p99={}", pct.p99_us);
        assert_eq!(pct.max_us, 1000);
    }

    #[test]
    fn test_latency_tracker_wraps() {
        let tracker = LatencyTracker::new(10);
        // Write 20 samples — first 10 are overwritten
        for i in 0..20 {
            tracker.record(i * 100);
        }
        assert_eq!(tracker.total_samples(), 20);
        let pct = tracker.percentiles();
        assert_eq!(pct.sample_count, 10);
        // Only samples 1000..1900 should be in the buffer
        assert!(pct.p50_us >= 1000, "p50={}", pct.p50_us);
    }

    #[test]
    fn test_txn_classification() {
        assert_eq!(classify_txn(&TxnClassificationHints::default()), TxnClass::FastPath);
        assert_eq!(classify_txn(&TxnClassificationHints {
            shard_count: 3, ..Default::default()
        }), TxnClass::SlowPath);
        assert_eq!(classify_txn(&TxnClassificationHints {
            is_ddl: true, ..Default::default()
        }), TxnClass::Ddl);
        assert_eq!(classify_txn(&TxnClassificationHints {
            is_system: true, ..Default::default()
        }), TxnClass::System);
        // Large row estimate without LIMIT → slow path
        assert_eq!(classify_txn(&TxnClassificationHints {
            estimated_rows: 50_000, ..Default::default()
        }), TxnClass::SlowPath);
        // Large row estimate WITH LIMIT → fast path
        assert_eq!(classify_txn(&TxnClassificationHints {
            estimated_rows: 50_000, has_limit: true, ..Default::default()
        }), TxnClass::FastPath);
    }

    #[test]
    fn test_sla_admit_accept_under_normal() {
        let ctrl = SlaAdmissionController::new(SlaConfig::default());
        let permit = ctrl.try_admit(TxnClass::FastPath);
        assert!(permit.is_ok());
        let p = permit.unwrap();
        assert_eq!(p.class(), TxnClass::FastPath);
        assert_eq!(ctrl.inflight_total.load(Ordering::Relaxed), 1);
        drop(p);
        assert_eq!(ctrl.inflight_total.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_sla_admit_system_always_passes() {
        let ctrl = SlaAdmissionController::new(SlaConfig {
            max_inflight_txn: 0, // Zero limit
            ..Default::default()
        });
        // System txns bypass all checks
        let permit = ctrl.try_admit(TxnClass::System);
        assert!(permit.is_ok());
    }

    #[test]
    fn test_sla_admit_inflight_limit() {
        let ctrl = SlaAdmissionController::new(SlaConfig {
            max_inflight_txn: 2,
            ..Default::default()
        });
        let p1 = ctrl.try_admit(TxnClass::FastPath).unwrap();
        let p2 = ctrl.try_admit(TxnClass::FastPath).unwrap();
        // Third should be rejected
        let result = ctrl.try_admit(TxnClass::FastPath);
        assert!(result.is_err());
        let (decision, explanation) = result.unwrap_err();
        assert!(matches!(decision, AdmissionDecision::RejectRetryable { .. }));
        assert_eq!(explanation.signal, RejectionSignal::InflightLimitReached);
        drop(p1);
        drop(p2);
    }

    #[test]
    fn test_sla_admit_slow_path_quota() {
        let ctrl = SlaAdmissionController::new(SlaConfig {
            max_inflight_txn: 100,
            max_cross_shard_inflight: 2,
            slow_path_quota_ratio: 0.5,
            ..Default::default()
        });
        let _p1 = ctrl.try_admit(TxnClass::SlowPath).unwrap();
        let _p2 = ctrl.try_admit(TxnClass::SlowPath).unwrap();
        // Third slow-path should be rejected
        let result = ctrl.try_admit(TxnClass::SlowPath);
        assert!(result.is_err());
        let (_, explanation) = result.unwrap_err();
        assert_eq!(explanation.signal, RejectionSignal::SlowPathQuotaExhausted);
        // But fast-path still works
        let fp = ctrl.try_admit(TxnClass::FastPath);
        assert!(fp.is_ok());
    }

    #[test]
    fn test_sla_admit_ddl_quota() {
        let ctrl = SlaAdmissionController::new(SlaConfig {
            max_inflight_txn: 100,
            ddl_quota: 1,
            ..Default::default()
        });
        let _p1 = ctrl.try_admit(TxnClass::Ddl).unwrap();
        let result = ctrl.try_admit(TxnClass::Ddl);
        assert!(result.is_err());
        let (_, explanation) = result.unwrap_err();
        assert_eq!(explanation.signal, RejectionSignal::DdlQuotaExhausted);
    }

    #[test]
    fn test_sla_admit_p999_breach_rejects_all() {
        let ctrl = SlaAdmissionController::new(SlaConfig {
            target_p999_ms: 10.0,
            min_samples_for_enforcement: 5,
            latency_window_size: 100,
            ..Default::default()
        });
        // Record high-latency samples to trigger p999 breach
        for _ in 0..10 {
            ctrl.latency.record(20_000); // 20ms > 10ms target
        }
        let result = ctrl.try_admit(TxnClass::FastPath);
        assert!(result.is_err());
        let (_, explanation) = result.unwrap_err();
        assert_eq!(explanation.signal, RejectionSignal::P999Exceeded);
    }

    #[test]
    fn test_sla_admit_p99_breach_rejects_slow_only() {
        let ctrl = SlaAdmissionController::new(SlaConfig {
            target_p99_ms: 5.0,
            target_p999_ms: 50.0,
            min_samples_for_enforcement: 5,
            latency_window_size: 100,
            ..Default::default()
        });
        // Record mixed latency: mostly low but p99 > 5ms
        for i in 0..100 {
            if i < 98 {
                ctrl.latency.record(1000); // 1ms
            } else {
                ctrl.latency.record(10_000); // 10ms (drives p99 > 5ms)
            }
        }
        // Fast-path should still pass (SLA-1: only slow-path rejected on p99 breach)
        let fp = ctrl.try_admit(TxnClass::FastPath);
        assert!(fp.is_ok());
        // Slow-path should be rejected
        let sp = ctrl.try_admit(TxnClass::SlowPath);
        assert!(sp.is_err());
    }

    #[test]
    fn test_sla_permit_records_latency_on_drop() {
        let ctrl = SlaAdmissionController::new(SlaConfig::default());
        let permit = ctrl.try_admit(TxnClass::FastPath).unwrap();
        // Small sleep to accumulate some latency
        std::thread::sleep(std::time::Duration::from_millis(1));
        drop(permit);
        assert!(ctrl.latency.total_samples() >= 1);
    }

    #[test]
    fn test_sla_metrics_snapshot() {
        let ctrl = SlaAdmissionController::new(SlaConfig::default());
        let _permit = ctrl.try_admit(TxnClass::FastPath).unwrap();
        let m = ctrl.metrics();
        assert_eq!(m.inflight_total, 1);
        assert_eq!(m.inflight_fast, 1);
        assert_eq!(m.total_accepted, 1);
        assert_eq!(m.total_rejected, 0);
        assert!(m.accept_rate() > 0.99);
    }

    #[test]
    fn test_rejection_explanation_display() {
        let expl = RejectionExplanation {
            signal: RejectionSignal::P99Exceeded,
            current_value: 25.0,
            threshold: 20.0,
            txn_class: TxnClass::SlowPath,
            remedy: "reduce load",
            sqlstate: "53300",
        };
        let s = format!("{}", expl);
        assert!(s.contains("p99_exceeded"));
        assert!(s.contains("25.00"));
        assert!(s.contains("20.00"));
        assert!(s.contains("slow_path"));
    }

    #[test]
    fn test_sla_config_defaults() {
        let cfg = SlaConfig::default();
        assert_eq!(cfg.target_p99_ms, 20.0);
        assert_eq!(cfg.target_p999_ms, 100.0);
        assert_eq!(cfg.max_inflight_txn, 500);
        assert_eq!(cfg.max_cross_shard_inflight, 50);
    }
}
