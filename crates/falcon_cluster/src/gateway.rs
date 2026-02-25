//! Gateway types: disposition classification, admission control, and forwarding metrics.
//!
//! Extracted from `query_engine.rs` for modularity. These types are used by
//! `DistributedQueryEngine` and exposed via the crate's public API.

use std::sync::atomic::{AtomicU64, Ordering};

/// Last scatter/gather execution metrics for observability.
#[derive(Debug, Clone, Default)]
pub struct ScatterStats {
    pub shards_participated: usize,
    pub total_rows_gathered: usize,
    pub per_shard_latency_us: Vec<(u64, u64)>,
    pub per_shard_row_count: Vec<(u64, usize)>,
    pub gather_strategy: String,
    pub total_latency_us: u64,
    pub failed_shards: Vec<u64>,
    /// Merge type labels for TwoPhaseAgg (e.g. "COUNT(col1)", "SUM(DISTINCT col2) [collect-dedup]")
    pub merge_labels: Vec<String>,
    /// Set when PK shard pruning routes a DistPlan to a single shard.
    pub pruned_to_shard: Option<u64>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Gateway Disposition — semantic classification for every gateway request
// ═══════════════════════════════════════════════════════════════════════════

/// How the gateway classified and handled a request.
/// Every request that enters the gateway MUST be assigned exactly one disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GatewayDisposition {
    /// Executed locally on this node (no forwarding needed).
    LocalExec,
    /// Forwarded to the shard leader on another node.
    ForwardedToLeader,
    /// Rejected: no leader is available for the target shard.
    RejectNoLeader,
    /// Rejected: gateway is overloaded (inflight or forwarded limit exceeded).
    RejectOverloaded,
    /// Rejected: request timed out before execution could begin.
    RejectTimeout,
}

impl GatewayDisposition {
    /// Whether this disposition represents a successful execution.
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::LocalExec | Self::ForwardedToLeader)
    }

    /// Whether the client should retry this request.
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::RejectOverloaded | Self::RejectTimeout)
    }

    /// Label for metrics/logging.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::LocalExec => "LOCAL_EXEC",
            Self::ForwardedToLeader => "FORWARDED_TO_LEADER",
            Self::RejectNoLeader => "REJECT_NO_LEADER",
            Self::RejectOverloaded => "REJECT_OVERLOADED",
            Self::RejectTimeout => "REJECT_TIMEOUT",
        }
    }
}

impl std::fmt::Display for GatewayDisposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Gateway Admission Control
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for gateway admission control.
#[derive(Debug, Clone)]
#[derive(Default)]
pub struct GatewayAdmissionConfig {
    /// Maximum concurrent inflight requests (local + forwarded).
    /// 0 = unlimited.
    pub max_inflight: u64,
    /// Maximum concurrent forwarded requests.
    /// 0 = unlimited.
    pub max_forwarded: u64,
}


/// Gateway admission control — fast-reject when overloaded.
///
/// Uses atomic counters for lock-free tracking. When limits are exceeded,
/// requests are immediately rejected with `RejectOverloaded` rather than
/// queuing (which would cause p99 explosion).
#[derive(Debug)]
pub struct GatewayAdmissionControl {
    config: GatewayAdmissionConfig,
    /// Current inflight request count.
    inflight: AtomicU64,
    /// Current forwarded request count.
    forwarded: AtomicU64,
}

impl GatewayAdmissionControl {
    pub const fn new(config: GatewayAdmissionConfig) -> Self {
        Self {
            config,
            inflight: AtomicU64::new(0),
            forwarded: AtomicU64::new(0),
        }
    }

    /// Try to acquire an inflight slot. Returns false if overloaded.
    pub fn try_acquire_inflight(&self) -> bool {
        if self.config.max_inflight == 0 {
            self.inflight.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        // CAS loop to atomically check-and-increment
        loop {
            let current = self.inflight.load(Ordering::Relaxed);
            if current >= self.config.max_inflight {
                return false;
            }
            if self.inflight.compare_exchange_weak(
                current, current + 1, Ordering::Relaxed, Ordering::Relaxed
            ).is_ok() {
                return true;
            }
        }
    }

    /// Release an inflight slot.
    pub fn release_inflight(&self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Try to acquire a forwarded slot. Returns false if overloaded.
    pub fn try_acquire_forwarded(&self) -> bool {
        if self.config.max_forwarded == 0 {
            self.forwarded.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        loop {
            let current = self.forwarded.load(Ordering::Relaxed);
            if current >= self.config.max_forwarded {
                return false;
            }
            if self.forwarded.compare_exchange_weak(
                current, current + 1, Ordering::Relaxed, Ordering::Relaxed
            ).is_ok() {
                return true;
            }
        }
    }

    /// Release a forwarded slot.
    pub fn release_forwarded(&self) {
        self.forwarded.fetch_sub(1, Ordering::Relaxed);
    }

    /// Current inflight count.
    pub fn inflight(&self) -> u64 {
        self.inflight.load(Ordering::Relaxed)
    }

    /// Current forwarded count.
    pub fn forwarded(&self) -> u64 {
        self.forwarded.load(Ordering::Relaxed)
    }
}

impl Default for GatewayAdmissionControl {
    fn default() -> Self {
        Self::new(GatewayAdmissionConfig::default())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Gateway Metrics (v1.0.6 — enhanced with disposition + admission)
// ═══════════════════════════════════════════════════════════════════════════

/// Gateway forwarding metrics — lock-free atomic counters.
///
/// Tracks how many queries were forwarded to non-local shards,
/// how many forwarding attempts failed, and cumulative latency.
#[derive(Debug)]
pub struct GatewayMetrics {
    /// Total queries forwarded (dispatched to non-local shard or scatter/gather).
    pub forward_total: AtomicU64,
    /// Total forwarding failures (shard unreachable, timeout, etc.).
    pub forward_failed_total: AtomicU64,
    /// Cumulative forwarding latency in microseconds.
    pub forward_latency_us: AtomicU64,
    // v1.0.6: disposition-based counters
    /// Total requests classified as LOCAL_EXEC.
    pub local_exec_total: AtomicU64,
    /// Total requests rejected (all reasons).
    pub reject_total: AtomicU64,
    /// Rejections due to no leader.
    pub reject_no_leader: AtomicU64,
    /// Rejections due to overload.
    pub reject_overloaded: AtomicU64,
    /// Rejections due to timeout.
    pub reject_timeout: AtomicU64,
}

impl GatewayMetrics {
    pub const fn new() -> Self {
        Self {
            forward_total: AtomicU64::new(0),
            forward_failed_total: AtomicU64::new(0),
            forward_latency_us: AtomicU64::new(0),
            local_exec_total: AtomicU64::new(0),
            reject_total: AtomicU64::new(0),
            reject_no_leader: AtomicU64::new(0),
            reject_overloaded: AtomicU64::new(0),
            reject_timeout: AtomicU64::new(0),
        }
    }

    /// Record a gateway disposition.
    pub fn record_disposition(&self, disp: GatewayDisposition) {
        match disp {
            GatewayDisposition::LocalExec => {
                self.local_exec_total.fetch_add(1, Ordering::Relaxed);
            }
            GatewayDisposition::ForwardedToLeader => {
                self.forward_total.fetch_add(1, Ordering::Relaxed);
            }
            GatewayDisposition::RejectNoLeader => {
                self.reject_total.fetch_add(1, Ordering::Relaxed);
                self.reject_no_leader.fetch_add(1, Ordering::Relaxed);
            }
            GatewayDisposition::RejectOverloaded => {
                self.reject_total.fetch_add(1, Ordering::Relaxed);
                self.reject_overloaded.fetch_add(1, Ordering::Relaxed);
            }
            GatewayDisposition::RejectTimeout => {
                self.reject_total.fetch_add(1, Ordering::Relaxed);
                self.reject_timeout.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> GatewayMetricsSnapshot {
        GatewayMetricsSnapshot {
            forward_total: self.forward_total.load(Ordering::Relaxed),
            forward_failed_total: self.forward_failed_total.load(Ordering::Relaxed),
            forward_latency_us: self.forward_latency_us.load(Ordering::Relaxed),
            local_exec_total: self.local_exec_total.load(Ordering::Relaxed),
            reject_total: self.reject_total.load(Ordering::Relaxed),
            reject_no_leader: self.reject_no_leader.load(Ordering::Relaxed),
            reject_overloaded: self.reject_overloaded.load(Ordering::Relaxed),
            reject_timeout: self.reject_timeout.load(Ordering::Relaxed),
        }
    }
}

impl Default for GatewayMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of gateway metrics.
#[derive(Debug, Clone, Default)]
pub struct GatewayMetricsSnapshot {
    pub forward_total: u64,
    pub forward_failed_total: u64,
    pub forward_latency_us: u64,
    pub local_exec_total: u64,
    pub reject_total: u64,
    pub reject_no_leader: u64,
    pub reject_overloaded: u64,
    pub reject_timeout: u64,
}
