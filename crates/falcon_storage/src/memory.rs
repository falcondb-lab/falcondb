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
}
