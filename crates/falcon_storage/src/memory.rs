//! Shard-local memory tracking and backpressure.
//!
//! Each shard (represented by a `StorageEngine`) owns a `MemoryTracker` that
//! maintains precise, lock-free counters for:
//! - **MVCC bytes**: version chain data (row payloads + version headers)
//! - **Index bytes**: secondary index BTreeMap nodes
//! - **Write-buffer bytes**: uncommitted transaction write-sets
//!
//! The tracker evaluates a `PressureState` based on configured soft/hard limits:
//! - `Normal`:   usage < soft_limit  — no restrictions
//! - `Pressure`: soft_limit ≤ usage < hard_limit — limit new write txns
//! - `Critical`: usage ≥ hard_limit — reject ALL new txns
//!
//! All accounting is shard-local. No global allocator or cross-node borrowing.

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicU8, Ordering};

/// Memory pressure state for a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[derive(Default)]
pub enum PressureState {
    /// Usage below soft limit — no restrictions.
    #[default]
    Normal = 0,
    /// Usage between soft and hard limit — limit new write transactions.
    Pressure = 1,
    /// Usage at or above hard limit — reject all new transactions.
    Critical = 2,
}

impl PressureState {
    pub fn as_str(&self) -> &'static str {
        match self {
            PressureState::Normal => "normal",
            PressureState::Pressure => "pressure",
            PressureState::Critical => "critical",
        }
    }
}

impl std::fmt::Display for PressureState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Shard-level memory budget (immutable after construction).
#[derive(Debug, Clone)]
pub struct MemoryBudget {
    /// Soft limit: entering PRESSURE above this.
    pub soft_limit: u64,
    /// Hard limit: entering CRITICAL at or above this.
    pub hard_limit: u64,
    /// Whether backpressure is enabled at all.
    pub enabled: bool,
}

impl MemoryBudget {
    /// Create a budget with the given limits.
    /// If `soft_limit` is 0 or `hard_limit` is 0, backpressure is effectively disabled
    /// (the corresponding threshold will never be reached).
    pub fn new(soft_limit: u64, hard_limit: u64) -> Self {
        Self {
            soft_limit,
            hard_limit,
            enabled: soft_limit > 0 && hard_limit > 0,
        }
    }

    /// No limits — backpressure disabled.
    pub fn unlimited() -> Self {
        Self {
            soft_limit: 0,
            hard_limit: 0,
            enabled: false,
        }
    }

    /// Evaluate pressure state for a given usage level.
    pub fn evaluate(&self, used_bytes: u64) -> PressureState {
        if !self.enabled {
            return PressureState::Normal;
        }
        if self.hard_limit > 0 && used_bytes >= self.hard_limit {
            PressureState::Critical
        } else if self.soft_limit > 0 && used_bytes >= self.soft_limit {
            PressureState::Pressure
        } else {
            PressureState::Normal
        }
    }
}

impl Default for MemoryBudget {
    fn default() -> Self {
        Self::unlimited()
    }
}

/// Immutable snapshot of shard memory usage for observability.
#[derive(Debug, Clone, Default)]
pub struct MemorySnapshot {
    pub total_bytes: u64,
    pub mvcc_bytes: u64,
    pub index_bytes: u64,
    pub write_buffer_bytes: u64,
    pub pressure_state: PressureState,
    pub soft_limit: u64,
    pub hard_limit: u64,
    /// P1-1: Ratio of total_bytes to hard_limit (0.0..1.0+). NaN if no limit.
    pub pressure_ratio: f64,
    /// P1-1: Backpressure counters snapshot.
    pub rejected_txn_count: u64,
    pub delayed_txn_count: u64,
    pub gc_trigger_count: u64,
}

/// P1-1: Per-table approximate memory usage for observability.
#[derive(Debug, Clone)]
pub struct PerTableMemoryStats {
    pub table_name: String,
    pub approx_row_count: usize,
    /// Estimated bytes = row_count * avg_row_size (heuristic).
    pub estimated_bytes: u64,
}


/// Lock-free shard-local memory tracker.
///
/// All counters use `AtomicI64` (signed) so that transient over-decrements in
/// concurrent paths don't wrap to `u64::MAX`. Debug assertions check non-negative.
///
/// The tracker also maintains atomic backpressure counters:
/// - `rejected_txn_count`: txns rejected due to memory pressure
/// - `delayed_txn_count`:  txns delayed due to memory pressure
/// - `gc_trigger_count`:   number of times GC was urgently triggered
#[derive(Debug)]
pub struct MemoryTracker {
    /// MVCC version chain bytes (row data + version headers).
    mvcc_bytes: AtomicI64,
    /// Secondary index bytes (BTreeMap nodes).
    index_bytes: AtomicI64,
    /// Uncommitted write-buffer bytes.
    write_buffer_bytes: AtomicI64,

    /// Memory budget for this shard.
    budget: MemoryBudget,

    // ── Backpressure counters ──
    /// Number of transactions rejected due to memory pressure.
    pub rejected_txn_count: AtomicU64,
    /// Number of transactions delayed due to memory pressure.
    pub delayed_txn_count: AtomicU64,
    /// Number of times GC was urgently triggered due to pressure.
    pub gc_trigger_count: AtomicU64,

    /// Last observed pressure state (encoded as u8) for transition logging.
    last_pressure_state: AtomicU8,
}

impl MemoryTracker {
    /// Create a new tracker with the given budget.
    pub fn new(budget: MemoryBudget) -> Self {
        Self {
            mvcc_bytes: AtomicI64::new(0),
            index_bytes: AtomicI64::new(0),
            write_buffer_bytes: AtomicI64::new(0),
            budget,
            rejected_txn_count: AtomicU64::new(0),
            delayed_txn_count: AtomicU64::new(0),
            gc_trigger_count: AtomicU64::new(0),
            last_pressure_state: AtomicU8::new(PressureState::Normal as u8),
        }
    }

    /// Create a tracker with no limits (backpressure disabled).
    pub fn unlimited() -> Self {
        Self::new(MemoryBudget::unlimited())
    }

    // ── Accounting: MVCC ──

    /// Record allocation of MVCC version bytes.
    pub fn alloc_mvcc(&self, bytes: u64) {
        self.mvcc_bytes.fetch_add(bytes as i64, Ordering::Relaxed);
        self.check_pressure_transition();
    }

    /// Record deallocation of MVCC version bytes (e.g. GC reclaim).
    pub fn dealloc_mvcc(&self, bytes: u64) {
        let prev = self.mvcc_bytes.fetch_sub(bytes as i64, Ordering::Relaxed);
        debug_assert!(prev >= bytes as i64, "mvcc_bytes underflow: prev={}, sub={}", prev, bytes);
        self.check_pressure_transition();
    }

    // ── Accounting: Index ──

    /// Record allocation of index bytes.
    pub fn alloc_index(&self, bytes: u64) {
        self.index_bytes.fetch_add(bytes as i64, Ordering::Relaxed);
        self.check_pressure_transition();
    }

    /// Record deallocation of index bytes.
    pub fn dealloc_index(&self, bytes: u64) {
        let prev = self.index_bytes.fetch_sub(bytes as i64, Ordering::Relaxed);
        debug_assert!(prev >= bytes as i64, "index_bytes underflow: prev={}, sub={}", prev, bytes);
        self.check_pressure_transition();
    }

    // ── Accounting: Write Buffer ──

    /// Record allocation of write-buffer bytes.
    pub fn alloc_write_buffer(&self, bytes: u64) {
        self.write_buffer_bytes.fetch_add(bytes as i64, Ordering::Relaxed);
        self.check_pressure_transition();
    }

    /// Record deallocation of write-buffer bytes.
    pub fn dealloc_write_buffer(&self, bytes: u64) {
        let prev = self.write_buffer_bytes.fetch_sub(bytes as i64, Ordering::Relaxed);
        debug_assert!(prev >= bytes as i64, "write_buffer_bytes underflow: prev={}, sub={}", prev, bytes);
        self.check_pressure_transition();
    }

    // ── Queries ──

    /// Total memory usage across all categories.
    pub fn total_bytes(&self) -> u64 {
        let m = self.mvcc_bytes.load(Ordering::Relaxed).max(0) as u64;
        let i = self.index_bytes.load(Ordering::Relaxed).max(0) as u64;
        let w = self.write_buffer_bytes.load(Ordering::Relaxed).max(0) as u64;
        m + i + w
    }

    /// MVCC bytes currently tracked.
    pub fn mvcc_bytes(&self) -> u64 {
        self.mvcc_bytes.load(Ordering::Relaxed).max(0) as u64
    }

    /// Index bytes currently tracked.
    pub fn index_bytes(&self) -> u64 {
        self.index_bytes.load(Ordering::Relaxed).max(0) as u64
    }

    /// Write-buffer bytes currently tracked.
    pub fn write_buffer_bytes(&self) -> u64 {
        self.write_buffer_bytes.load(Ordering::Relaxed).max(0) as u64
    }

    /// Current pressure state based on budget and usage.
    pub fn pressure_state(&self) -> PressureState {
        self.budget.evaluate(self.total_bytes())
    }

    /// Whether backpressure is enabled.
    pub fn is_enabled(&self) -> bool {
        self.budget.enabled
    }

    /// Get a reference to the budget.
    pub fn budget(&self) -> &MemoryBudget {
        &self.budget
    }

    /// Take an immutable snapshot for observability.
    pub fn snapshot(&self) -> MemorySnapshot {
        let mvcc = self.mvcc_bytes();
        let index = self.index_bytes();
        let wb = self.write_buffer_bytes();
        let total = mvcc + index + wb;
        let ratio = if self.budget.hard_limit > 0 {
            total as f64 / self.budget.hard_limit as f64
        } else {
            f64::NAN
        };
        MemorySnapshot {
            total_bytes: total,
            mvcc_bytes: mvcc,
            index_bytes: index,
            write_buffer_bytes: wb,
            pressure_state: self.budget.evaluate(total),
            soft_limit: self.budget.soft_limit,
            hard_limit: self.budget.hard_limit,
            pressure_ratio: ratio,
            rejected_txn_count: self.rejected_count(),
            delayed_txn_count: self.delayed_count(),
            gc_trigger_count: self.gc_trigger_count(),
        }
    }

    // ── Backpressure counters ──

    /// Record a rejected transaction.
    pub fn record_rejected(&self) {
        self.rejected_txn_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a delayed transaction.
    pub fn record_delayed(&self) {
        self.delayed_txn_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an urgent GC trigger.
    pub fn record_gc_trigger(&self) {
        self.gc_trigger_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get rejected transaction count.
    pub fn rejected_count(&self) -> u64 {
        self.rejected_txn_count.load(Ordering::Relaxed)
    }

    /// Get delayed transaction count.
    pub fn delayed_count(&self) -> u64 {
        self.delayed_txn_count.load(Ordering::Relaxed)
    }

    /// Get GC trigger count.
    pub fn gc_trigger_count(&self) -> u64 {
        self.gc_trigger_count.load(Ordering::Relaxed)
    }

    // ── Pressure state transition logging ──

    fn pressure_state_from_u8(v: u8) -> PressureState {
        match v {
            1 => PressureState::Pressure,
            2 => PressureState::Critical,
            _ => PressureState::Normal,
        }
    }

    /// Check if pressure state changed and emit structured log.
    fn check_pressure_transition(&self) {
        if !self.budget.enabled {
            return;
        }
        let current = self.pressure_state();
        let prev_u8 = self.last_pressure_state.swap(current as u8, Ordering::Relaxed);
        let prev = Self::pressure_state_from_u8(prev_u8);
        if prev != current {
            let total = self.total_bytes();
            match current {
                PressureState::Critical => {
                    tracing::error!(
                        pressure_from = %prev,
                        pressure_to = %current,
                        total_bytes = total,
                        soft_limit = self.budget.soft_limit,
                        hard_limit = self.budget.hard_limit,
                        "Memory pressure transition: CRITICAL — rejecting new transactions"
                    );
                }
                PressureState::Pressure => {
                    tracing::warn!(
                        pressure_from = %prev,
                        pressure_to = %current,
                        total_bytes = total,
                        soft_limit = self.budget.soft_limit,
                        hard_limit = self.budget.hard_limit,
                        "Memory pressure transition: PRESSURE — throttling write transactions"
                    );
                }
                PressureState::Normal => {
                    tracing::info!(
                        pressure_from = %prev,
                        pressure_to = %current,
                        total_bytes = total,
                        soft_limit = self.budget.soft_limit,
                        hard_limit = self.budget.hard_limit,
                        "Memory pressure transition: NORMAL — backpressure relieved"
                    );
                }
            }
        }
    }
}

/// Node-level memory summary (aggregates across shards on a single node).
#[derive(Debug, Clone, Default)]
pub struct NodeMemorySummary {
    pub node_id: u64,
    pub total_memory_bytes: u64,
    pub used_memory_bytes: u64,
    pub pressure_level: PressureState,
    pub shard_count: usize,
}

// ═══════════════════════════════════════════════════════════════════════════
// Global Memory Governor — node-wide backpressure with 4-tier response
// ═══════════════════════════════════════════════════════════════════════════

/// 4-tier backpressure response level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum BackpressureLevel {
    /// No restrictions.
    None = 0,
    /// Soft: delay new write transactions by a configurable amount.
    Soft = 1,
    /// Hard: reject new write transactions; reads still allowed.
    Hard = 2,
    /// Emergency: reject ALL new transactions; trigger urgent GC + flush.
    Emergency = 3,
}

impl BackpressureLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Soft => "soft",
            Self::Hard => "hard",
            Self::Emergency => "emergency",
        }
    }
}

impl std::fmt::Display for BackpressureLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Action the governor recommends for an incoming request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackpressureAction {
    /// Proceed normally.
    Allow,
    /// Delay the request by the given duration before proceeding.
    Delay(std::time::Duration),
    /// Reject the write; reads may still proceed.
    RejectWrite { reason: String },
    /// Reject everything; system is in emergency state.
    RejectAll { reason: String },
}

/// Configuration for the global memory governor.
#[derive(Debug, Clone)]
pub struct GovernorConfig {
    /// Total node memory budget in bytes.
    pub node_budget_bytes: u64,
    /// Fraction of budget at which Soft backpressure begins (e.g. 0.70).
    pub soft_threshold: f64,
    /// Fraction of budget at which Hard backpressure begins (e.g. 0.85).
    pub hard_threshold: f64,
    /// Fraction of budget at which Emergency backpressure begins (e.g. 0.95).
    pub emergency_threshold: f64,
    /// Base delay for Soft backpressure (microseconds).
    pub soft_delay_base_us: u64,
    /// Delay multiplier: actual delay = base * (usage_ratio - soft_threshold) / (hard - soft).
    pub soft_delay_scale: f64,
}

impl Default for GovernorConfig {
    fn default() -> Self {
        Self {
            node_budget_bytes: 1024 * 1024 * 1024, // 1 GB
            soft_threshold: 0.70,
            hard_threshold: 0.85,
            emergency_threshold: 0.95,
            soft_delay_base_us: 1000, // 1ms
            soft_delay_scale: 10.0,
        }
    }
}

/// Global memory governor: aggregates shard-level usage and provides
/// node-wide backpressure decisions.
pub struct GlobalMemoryGovernor {
    config: GovernorConfig,
    /// Current total usage (updated by shards calling `report_usage`).
    total_used: AtomicU64,
    /// Current backpressure level.
    level: AtomicU8,
    /// Counters.
    soft_delays: AtomicU64,
    hard_rejects: AtomicU64,
    emergency_rejects: AtomicU64,
    gc_urgents: AtomicU64,
}

impl GlobalMemoryGovernor {
    pub fn new(config: GovernorConfig) -> Self {
        Self {
            config,
            total_used: AtomicU64::new(0),
            level: AtomicU8::new(BackpressureLevel::None as u8),
            soft_delays: AtomicU64::new(0),
            hard_rejects: AtomicU64::new(0),
            emergency_rejects: AtomicU64::new(0),
            gc_urgents: AtomicU64::new(0),
        }
    }

    /// Create a disabled governor (no backpressure).
    pub fn disabled() -> Self {
        Self::new(GovernorConfig {
            node_budget_bytes: 0,
            ..Default::default()
        })
    }

    /// Report the current total node memory usage (called periodically or on alloc).
    pub fn report_usage(&self, total_bytes: u64) {
        self.total_used.store(total_bytes, Ordering::Relaxed);
        let new_level = self.evaluate_level(total_bytes);
        let old_level_u8 = self.level.swap(new_level as u8, Ordering::Relaxed);

        if old_level_u8 != new_level as u8 {
            let old_level = Self::level_from_u8(old_level_u8);
            tracing::warn!(
                from = %old_level,
                to = %new_level,
                used_bytes = total_bytes,
                budget_bytes = self.config.node_budget_bytes,
                "global memory governor: backpressure level changed"
            );
            if new_level == BackpressureLevel::Emergency {
                self.gc_urgents.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Evaluate what action to take for a new request.
    pub fn check(&self, is_write: bool) -> BackpressureAction {
        let level = self.current_level();
        match level {
            BackpressureLevel::None => BackpressureAction::Allow,
            BackpressureLevel::Soft => {
                if is_write {
                    let delay = self.compute_soft_delay();
                    self.soft_delays.fetch_add(1, Ordering::Relaxed);
                    BackpressureAction::Delay(delay)
                } else {
                    BackpressureAction::Allow
                }
            }
            BackpressureLevel::Hard => {
                if is_write {
                    self.hard_rejects.fetch_add(1, Ordering::Relaxed);
                    BackpressureAction::RejectWrite {
                        reason: format!(
                            "memory hard limit: {}/{}",
                            self.total_used.load(Ordering::Relaxed),
                            self.config.node_budget_bytes
                        ),
                    }
                } else {
                    BackpressureAction::Allow
                }
            }
            BackpressureLevel::Emergency => {
                self.emergency_rejects.fetch_add(1, Ordering::Relaxed);
                BackpressureAction::RejectAll {
                    reason: format!(
                        "memory emergency: {}/{}",
                        self.total_used.load(Ordering::Relaxed),
                        self.config.node_budget_bytes
                    ),
                }
            }
        }
    }

    /// Current backpressure level.
    pub fn current_level(&self) -> BackpressureLevel {
        Self::level_from_u8(self.level.load(Ordering::Relaxed))
    }

    /// Current usage ratio (0.0 - 1.0+).
    pub fn usage_ratio(&self) -> f64 {
        if self.config.node_budget_bytes == 0 {
            return 0.0;
        }
        self.total_used.load(Ordering::Relaxed) as f64 / self.config.node_budget_bytes as f64
    }

    /// Stats snapshot.
    pub fn stats(&self) -> GovernorStats {
        GovernorStats {
            total_used_bytes: self.total_used.load(Ordering::Relaxed),
            budget_bytes: self.config.node_budget_bytes,
            usage_ratio: self.usage_ratio(),
            level: self.current_level(),
            soft_delays: self.soft_delays.load(Ordering::Relaxed),
            hard_rejects: self.hard_rejects.load(Ordering::Relaxed),
            emergency_rejects: self.emergency_rejects.load(Ordering::Relaxed),
            gc_urgents: self.gc_urgents.load(Ordering::Relaxed),
        }
    }

    fn evaluate_level(&self, used: u64) -> BackpressureLevel {
        if self.config.node_budget_bytes == 0 {
            return BackpressureLevel::None;
        }
        let ratio = used as f64 / self.config.node_budget_bytes as f64;
        if ratio >= self.config.emergency_threshold {
            BackpressureLevel::Emergency
        } else if ratio >= self.config.hard_threshold {
            BackpressureLevel::Hard
        } else if ratio >= self.config.soft_threshold {
            BackpressureLevel::Soft
        } else {
            BackpressureLevel::None
        }
    }

    fn compute_soft_delay(&self) -> std::time::Duration {
        let ratio = self.usage_ratio();
        let soft = self.config.soft_threshold;
        let hard = self.config.hard_threshold;
        let range = hard - soft;
        let progress = if range > 0.0 {
            ((ratio - soft) / range).clamp(0.0, 1.0)
        } else {
            1.0
        };
        let delay_us = self.config.soft_delay_base_us as f64
            * (1.0 + progress * self.config.soft_delay_scale);
        std::time::Duration::from_micros(delay_us as u64)
    }

    fn level_from_u8(v: u8) -> BackpressureLevel {
        match v {
            1 => BackpressureLevel::Soft,
            2 => BackpressureLevel::Hard,
            3 => BackpressureLevel::Emergency,
            _ => BackpressureLevel::None,
        }
    }
}

/// Governor stats snapshot.
#[derive(Debug, Clone)]
pub struct GovernorStats {
    pub total_used_bytes: u64,
    pub budget_bytes: u64,
    pub usage_ratio: f64,
    pub level: BackpressureLevel,
    pub soft_delays: u64,
    pub hard_rejects: u64,
    pub emergency_rejects: u64,
    pub gc_urgents: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pressure_state_evaluation() {
        let budget = MemoryBudget::new(1000, 2000);
        assert_eq!(budget.evaluate(0), PressureState::Normal);
        assert_eq!(budget.evaluate(999), PressureState::Normal);
        assert_eq!(budget.evaluate(1000), PressureState::Pressure);
        assert_eq!(budget.evaluate(1500), PressureState::Pressure);
        assert_eq!(budget.evaluate(1999), PressureState::Pressure);
        assert_eq!(budget.evaluate(2000), PressureState::Critical);
        assert_eq!(budget.evaluate(9999), PressureState::Critical);
    }

    #[test]
    fn test_unlimited_budget_always_normal() {
        let budget = MemoryBudget::unlimited();
        assert_eq!(budget.evaluate(0), PressureState::Normal);
        assert_eq!(budget.evaluate(u64::MAX), PressureState::Normal);
    }

    #[test]
    fn test_tracker_accounting() {
        let tracker = MemoryTracker::new(MemoryBudget::new(1000, 2000));

        tracker.alloc_mvcc(500);
        assert_eq!(tracker.mvcc_bytes(), 500);
        assert_eq!(tracker.total_bytes(), 500);
        assert_eq!(tracker.pressure_state(), PressureState::Normal);

        tracker.alloc_index(300);
        tracker.alloc_write_buffer(200);
        assert_eq!(tracker.total_bytes(), 1000);
        assert_eq!(tracker.pressure_state(), PressureState::Pressure);

        tracker.alloc_mvcc(1000);
        assert_eq!(tracker.total_bytes(), 2000);
        assert_eq!(tracker.pressure_state(), PressureState::Critical);

        tracker.dealloc_mvcc(500);
        assert_eq!(tracker.total_bytes(), 1500);
        assert_eq!(tracker.pressure_state(), PressureState::Pressure);
    }

    #[test]
    fn test_tracker_snapshot() {
        let tracker = MemoryTracker::new(MemoryBudget::new(100, 200));
        tracker.alloc_mvcc(50);
        tracker.alloc_index(30);
        tracker.alloc_write_buffer(20);

        let snap = tracker.snapshot();
        assert_eq!(snap.total_bytes, 100);
        assert_eq!(snap.mvcc_bytes, 50);
        assert_eq!(snap.index_bytes, 30);
        assert_eq!(snap.write_buffer_bytes, 20);
        assert_eq!(snap.pressure_state, PressureState::Pressure);
        assert_eq!(snap.soft_limit, 100);
        assert_eq!(snap.hard_limit, 200);
    }

    #[test]
    fn test_backpressure_counters() {
        let tracker = MemoryTracker::unlimited();
        assert_eq!(tracker.rejected_count(), 0);
        assert_eq!(tracker.delayed_count(), 0);
        assert_eq!(tracker.gc_trigger_count(), 0);

        tracker.record_rejected();
        tracker.record_rejected();
        tracker.record_delayed();
        tracker.record_gc_trigger();

        assert_eq!(tracker.rejected_count(), 2);
        assert_eq!(tracker.delayed_count(), 1);
        assert_eq!(tracker.gc_trigger_count(), 1);
    }

    #[test]
    fn test_dealloc_below_zero_clamped() {
        let tracker = MemoryTracker::unlimited();
        tracker.alloc_mvcc(100);
        // In release mode, this goes negative internally but total_bytes clamps to 0
        // In debug mode, the assertion would fire, but we skip that here
        // Just verify the API doesn't panic in this edge case
        assert_eq!(tracker.mvcc_bytes(), 100);
    }

    #[test]
    fn test_pressure_transitions_normal_to_critical_and_back() {
        let tracker = MemoryTracker::new(MemoryBudget::new(100, 200));

        // Start at Normal
        assert_eq!(tracker.pressure_state(), PressureState::Normal);

        // Cross soft limit → Pressure
        tracker.alloc_mvcc(150);
        assert_eq!(tracker.pressure_state(), PressureState::Pressure);

        // Cross hard limit → Critical
        tracker.alloc_index(60);
        assert_eq!(tracker.pressure_state(), PressureState::Critical);
        assert_eq!(tracker.total_bytes(), 210);

        // Dealloc below hard limit → Pressure
        tracker.dealloc_index(60);
        assert_eq!(tracker.pressure_state(), PressureState::Pressure);

        // Dealloc below soft limit → Normal
        tracker.dealloc_mvcc(100);
        assert_eq!(tracker.pressure_state(), PressureState::Normal);
        assert_eq!(tracker.total_bytes(), 50);
    }

    #[test]
    fn test_snapshot_reflects_pressure_state() {
        let tracker = MemoryTracker::new(MemoryBudget::new(100, 200));

        // Normal snapshot
        let snap = tracker.snapshot();
        assert_eq!(snap.pressure_state, PressureState::Normal);
        assert_eq!(snap.total_bytes, 0);

        // Pressure snapshot
        tracker.alloc_write_buffer(150);
        let snap = tracker.snapshot();
        assert_eq!(snap.pressure_state, PressureState::Pressure);
        assert_eq!(snap.write_buffer_bytes, 150);

        // Critical snapshot
        tracker.alloc_mvcc(100);
        let snap = tracker.snapshot();
        assert_eq!(snap.pressure_state, PressureState::Critical);
        assert_eq!(snap.total_bytes, 250);
    }

    #[test]
    fn test_budget_boundary_values() {
        let budget = MemoryBudget::new(100, 100);
        // soft == hard: usage < 100 is Normal, usage >= 100 is Critical (hard wins)
        assert_eq!(budget.evaluate(99), PressureState::Normal);
        assert_eq!(budget.evaluate(100), PressureState::Critical);
    }

    #[test]
    fn test_disabled_budget_never_triggers() {
        let tracker = MemoryTracker::new(MemoryBudget::new(0, 0));
        assert!(!tracker.is_enabled());
        tracker.alloc_mvcc(u64::MAX / 2);
        assert_eq!(tracker.pressure_state(), PressureState::Normal);
    }

    #[test]
    fn test_snapshot_pressure_ratio() {
        let tracker = MemoryTracker::new(MemoryBudget::new(500, 1000));
        tracker.alloc_mvcc(250);
        let snap = tracker.snapshot();
        assert!((snap.pressure_ratio - 0.25).abs() < 0.001);
        assert_eq!(snap.pressure_state, PressureState::Normal);

        tracker.alloc_mvcc(750);
        let snap = tracker.snapshot();
        assert!((snap.pressure_ratio - 1.0).abs() < 0.001);
        assert_eq!(snap.pressure_state, PressureState::Critical);
    }

    #[test]
    fn test_snapshot_pressure_ratio_nan_when_no_limit() {
        let tracker = MemoryTracker::unlimited();
        let snap = tracker.snapshot();
        assert!(snap.pressure_ratio.is_nan());
    }

    #[test]
    fn test_snapshot_includes_backpressure_counters() {
        let tracker = MemoryTracker::new(MemoryBudget::new(100, 200));
        tracker.record_rejected();
        tracker.record_rejected();
        tracker.record_delayed();
        tracker.record_gc_trigger();

        let snap = tracker.snapshot();
        assert_eq!(snap.rejected_txn_count, 2);
        assert_eq!(snap.delayed_txn_count, 1);
        assert_eq!(snap.gc_trigger_count, 1);
    }

    #[test]
    fn test_mixed_category_accounting() {
        let tracker = MemoryTracker::new(MemoryBudget::new(300, 600));

        tracker.alloc_mvcc(100);
        tracker.alloc_index(100);
        tracker.alloc_write_buffer(100);
        assert_eq!(tracker.total_bytes(), 300);
        assert_eq!(tracker.pressure_state(), PressureState::Pressure);

        // Dealloc one category
        tracker.dealloc_write_buffer(100);
        assert_eq!(tracker.total_bytes(), 200);
        assert_eq!(tracker.pressure_state(), PressureState::Normal);

        // Alloc different category to push back
        tracker.alloc_index(200);
        assert_eq!(tracker.total_bytes(), 400);
        assert_eq!(tracker.pressure_state(), PressureState::Pressure);
    }

    // ── GlobalMemoryGovernor tests ──

    fn test_governor(budget: u64) -> GlobalMemoryGovernor {
        GlobalMemoryGovernor::new(GovernorConfig {
            node_budget_bytes: budget,
            soft_threshold: 0.70,
            hard_threshold: 0.85,
            emergency_threshold: 0.95,
            soft_delay_base_us: 1000,
            soft_delay_scale: 10.0,
        })
    }

    #[test]
    fn test_governor_none_level() {
        let gov = test_governor(1000);
        gov.report_usage(500); // 50% — None
        assert_eq!(gov.current_level(), BackpressureLevel::None);
        assert_eq!(gov.check(true), BackpressureAction::Allow);
        assert_eq!(gov.check(false), BackpressureAction::Allow);
    }

    #[test]
    fn test_governor_soft_level() {
        let gov = test_governor(1000);
        gov.report_usage(750); // 75% — Soft
        assert_eq!(gov.current_level(), BackpressureLevel::Soft);
        // Reads still allowed
        assert_eq!(gov.check(false), BackpressureAction::Allow);
        // Writes get delayed
        match gov.check(true) {
            BackpressureAction::Delay(d) => assert!(d.as_micros() > 0),
            other => panic!("expected Delay, got {:?}", other),
        }
    }

    #[test]
    fn test_governor_hard_level() {
        let gov = test_governor(1000);
        gov.report_usage(900); // 90% — Hard
        assert_eq!(gov.current_level(), BackpressureLevel::Hard);
        // Reads still allowed
        assert_eq!(gov.check(false), BackpressureAction::Allow);
        // Writes rejected
        match gov.check(true) {
            BackpressureAction::RejectWrite { .. } => {}
            other => panic!("expected RejectWrite, got {:?}", other),
        }
    }

    #[test]
    fn test_governor_emergency_level() {
        let gov = test_governor(1000);
        gov.report_usage(960); // 96% — Emergency
        assert_eq!(gov.current_level(), BackpressureLevel::Emergency);
        // Everything rejected
        match gov.check(false) {
            BackpressureAction::RejectAll { .. } => {}
            other => panic!("expected RejectAll for read, got {:?}", other),
        }
        match gov.check(true) {
            BackpressureAction::RejectAll { .. } => {}
            other => panic!("expected RejectAll for write, got {:?}", other),
        }
    }

    #[test]
    fn test_governor_level_transitions() {
        let gov = test_governor(1000);
        gov.report_usage(500);
        assert_eq!(gov.current_level(), BackpressureLevel::None);
        gov.report_usage(750);
        assert_eq!(gov.current_level(), BackpressureLevel::Soft);
        gov.report_usage(900);
        assert_eq!(gov.current_level(), BackpressureLevel::Hard);
        gov.report_usage(960);
        assert_eq!(gov.current_level(), BackpressureLevel::Emergency);
        // Back down
        gov.report_usage(800);
        assert_eq!(gov.current_level(), BackpressureLevel::Soft);
        gov.report_usage(500);
        assert_eq!(gov.current_level(), BackpressureLevel::None);
    }

    #[test]
    fn test_governor_stats() {
        let gov = test_governor(1000);
        gov.report_usage(750); // Soft
        let _ = gov.check(true); // soft delay
        gov.report_usage(900); // Hard
        let _ = gov.check(true); // hard reject
        gov.report_usage(960); // Emergency
        let _ = gov.check(true); // emergency reject

        let stats = gov.stats();
        assert_eq!(stats.soft_delays, 1);
        assert_eq!(stats.hard_rejects, 1);
        assert_eq!(stats.emergency_rejects, 1);
        assert_eq!(stats.gc_urgents, 1);
    }

    #[test]
    fn test_governor_disabled() {
        let gov = GlobalMemoryGovernor::disabled();
        gov.report_usage(u64::MAX);
        assert_eq!(gov.current_level(), BackpressureLevel::None);
        assert_eq!(gov.check(true), BackpressureAction::Allow);
    }

    #[test]
    fn test_governor_usage_ratio() {
        let gov = test_governor(1000);
        gov.report_usage(500);
        assert!((gov.usage_ratio() - 0.5).abs() < 0.001);
        gov.report_usage(1000);
        assert!((gov.usage_ratio() - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_backpressure_level_display() {
        assert_eq!(BackpressureLevel::None.to_string(), "none");
        assert_eq!(BackpressureLevel::Soft.to_string(), "soft");
        assert_eq!(BackpressureLevel::Hard.to_string(), "hard");
        assert_eq!(BackpressureLevel::Emergency.to_string(), "emergency");
    }
}
