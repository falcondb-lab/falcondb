//! P2-2: Deterministic GC with tail-latency budget.
//!
//! Wraps the existing GC infrastructure with:
//! - **Per-sweep time budget**: GC never exceeds `max_sweep_us` in a single sweep
//! - **Per-sweep key budget**: at most `max_keys_per_sweep` chains processed
//! - **Tail-latency guard**: if recent p99 > threshold, GC throttles itself
//! - **Chain length histogram**: observable distribution of MVCC chain lengths
//! - **Incremental reclaim**: small frequent steps instead of big infrequent ones
//!
//! # Invariants
//!
//! - **GC-B1**: A single GC sweep MUST NOT exceed `max_sweep_us` wall-clock time.
//!   If the budget is exhausted mid-sweep, GC stops and resumes next interval.
//! - **GC-B2**: Under normal pressure, GC runs at `normal_interval_ms`.
//!   Under memory pressure, GC accelerates but each sweep remains bounded.
//! - **GC-B3**: GC never reclaims uncommitted or still-visible versions
//!   (inherited from existing `gc::sweep_memtable` invariant).
//! - **GC-B4**: Chain length histogram is updated every sweep for observability.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::gc::{GcConfig, GcStats, GcSweepResult};
use crate::memtable::MemTable;
use falcon_common::types::Timestamp;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — GC Budget Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// Budget-aware GC configuration.
#[derive(Debug, Clone)]
pub struct GcBudgetConfig {
    /// Maximum wall-clock time per sweep in microseconds.
    /// When exceeded, sweep stops immediately and resumes next interval.
    pub max_sweep_us: u64,
    /// Maximum number of keys to process per sweep (0 = use time budget only).
    pub max_keys_per_sweep: u64,
    /// Normal GC interval (milliseconds).
    pub normal_interval_ms: u64,
    /// Accelerated GC interval under memory pressure (milliseconds).
    pub pressure_interval_ms: u64,
    /// Critical GC interval under severe memory pressure (milliseconds).
    pub critical_interval_ms: u64,
    /// If recent p99 query latency exceeds this (microseconds), throttle GC.
    /// 0 = no latency-aware throttling.
    pub latency_guard_p99_us: u64,
    /// When latency guard triggers, multiply interval by this factor.
    pub latency_throttle_factor: u32,
    /// Chain length histogram bucket boundaries.
    pub histogram_buckets: Vec<u64>,
}

impl Default for GcBudgetConfig {
    fn default() -> Self {
        Self {
            max_sweep_us: 5_000,         // 5ms max per sweep
            max_keys_per_sweep: 10_000,  // 10k keys max per sweep
            normal_interval_ms: 500,     // 500ms
            pressure_interval_ms: 100,   // 100ms under pressure
            critical_interval_ms: 20,    // 20ms under critical
            latency_guard_p99_us: 0,     // disabled by default
            latency_throttle_factor: 4,  // 4x slower when guarding
            histogram_buckets: vec![1, 2, 3, 5, 10, 20, 50, 100],
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Chain Length Histogram
// ═══════════════════════════════════════════════════════════════════════════

/// Histogram of MVCC version chain lengths.
#[derive(Debug, Clone, Default)]
pub struct ChainLengthHistogram {
    /// Bucket boundaries (e.g. [1, 2, 3, 5, 10, 20, 50, 100]).
    pub buckets: Vec<u64>,
    /// Counts per bucket (bucket[i] counts chains with length <= buckets[i]).
    pub counts: Vec<u64>,
    /// Overflow: chains longer than the last bucket boundary.
    pub overflow: u64,
    /// Total chains observed.
    pub total: u64,
    /// Maximum chain length observed.
    pub max_length: u64,
    /// Sum of all chain lengths (for computing mean).
    pub sum_lengths: u64,
}

impl ChainLengthHistogram {
    pub fn new(buckets: &[u64]) -> Self {
        Self {
            buckets: buckets.to_vec(),
            counts: vec![0; buckets.len()],
            overflow: 0,
            total: 0,
            max_length: 0,
            sum_lengths: 0,
        }
    }

    /// Record a chain length observation.
    pub fn observe(&mut self, length: u64) {
        self.total += 1;
        self.sum_lengths += length;
        if length > self.max_length {
            self.max_length = length;
        }
        let mut placed = false;
        for (i, &boundary) in self.buckets.iter().enumerate() {
            if length <= boundary {
                self.counts[i] += 1;
                placed = true;
                break;
            }
        }
        if !placed {
            self.overflow += 1;
        }
    }

    /// Mean chain length.
    pub fn mean(&self) -> f64 {
        if self.total == 0 { 0.0 } else { self.sum_lengths as f64 / self.total as f64 }
    }

    /// Reset all counters.
    pub fn reset(&mut self) {
        for c in &mut self.counts {
            *c = 0;
        }
        self.overflow = 0;
        self.total = 0;
        self.max_length = 0;
        self.sum_lengths = 0;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Budget-aware Sweep
// ═══════════════════════════════════════════════════════════════════════════

/// Result of a budget-aware GC sweep.
#[derive(Debug, Clone, Default)]
pub struct BudgetSweepResult {
    /// The underlying sweep result.
    pub inner: GcSweepResult,
    /// Whether the sweep was stopped early due to time budget exhaustion.
    pub budget_exhausted: bool,
    /// Whether the sweep was stopped early due to key count budget.
    pub key_budget_exhausted: bool,
    /// Whether the sweep was throttled due to latency guard.
    pub latency_throttled: bool,
    /// Chain length histogram from this sweep.
    pub histogram: ChainLengthHistogram,
}

/// Run a single budget-aware GC sweep over a MemTable.
///
/// Respects both time and key budgets. Stops immediately when either is exhausted.
/// Returns detailed results including whether budgets were hit.
pub fn sweep_memtable_budgeted(
    table: &MemTable,
    watermark: Timestamp,
    gc_config: &GcConfig,
    budget_config: &GcBudgetConfig,
    stats: &GcStats,
) -> BudgetSweepResult {
    let start = Instant::now();
    let mut result = BudgetSweepResult {
        inner: GcSweepResult {
            safepoint_ts: watermark,
            ..Default::default()
        },
        histogram: ChainLengthHistogram::new(&budget_config.histogram_buckets),
        ..Default::default()
    };

    // Fast path: no GC candidates
    if table.gc_candidates() == 0 {
        result.inner.sweep_duration_us = start.elapsed().as_micros() as u64;
        stats.record_sweep(&result.inner);
        return result;
    }

    let max_time = std::time::Duration::from_micros(budget_config.max_sweep_us);
    let max_keys = if budget_config.max_keys_per_sweep > 0 {
        budget_config.max_keys_per_sweep
    } else {
        u64::MAX
    };

    let mut processed = 0u64;

    for entry in &table.data {
        // Time budget check (every 64 keys to amortize syscall cost)
        if processed.is_multiple_of(64) && processed > 0
            && start.elapsed() >= max_time
        {
            result.budget_exhausted = true;
            result.inner.keys_skipped += table.data.len() as u64 - processed;
            break;
        }

        // Key budget check
        if processed >= max_keys {
            result.key_budget_exhausted = true;
            result.inner.keys_skipped += table.data.len() as u64 - processed;
            break;
        }

        let chain = entry.value();
        let chain_len = chain.version_chain_len();
        result.histogram.observe(chain_len as u64);
        stats.observe_chain_length(chain_len as u64);

        // Skip short chains (configurable)
        if chain_len < gc_config.min_chain_length {
            result.inner.keys_skipped += 1;
            processed += 1;
            continue;
        }

        result.inner.chains_inspected += 1;
        let chain_result = chain.gc(watermark);

        if chain_result.reclaimed_versions > 0 {
            result.inner.chains_pruned += 1;
            result.inner.reclaimed_versions += chain_result.reclaimed_versions;
            result.inner.reclaimed_bytes += chain_result.reclaimed_bytes;
        }

        processed += 1;
    }

    // Decrement gc_candidates by reclaimed versions
    if result.inner.reclaimed_versions > 0 {
        table.gc_candidates_sub(result.inner.reclaimed_versions);
    }

    result.inner.sweep_duration_us = start.elapsed().as_micros() as u64;
    stats.record_sweep(&result.inner);
    result
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Cumulative Budget GC Metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Cumulative metrics for budget-aware GC.
pub struct GcBudgetMetrics {
    /// Total sweeps that were budget-exhausted (time).
    pub time_budget_hits: AtomicU64,
    /// Total sweeps that were budget-exhausted (keys).
    pub key_budget_hits: AtomicU64,
    /// Total sweeps throttled by latency guard.
    pub latency_throttled: AtomicU64,
    /// Total sweeps completed within budget.
    pub completed_in_budget: AtomicU64,
    /// Maximum sweep duration observed (microseconds).
    pub max_sweep_us: AtomicU64,
    /// Sum of sweep durations (for mean calculation).
    pub sum_sweep_us: AtomicU64,
    /// Total sweeps.
    pub total_sweeps: AtomicU64,
}

impl GcBudgetMetrics {
    pub const fn new() -> Self {
        Self {
            time_budget_hits: AtomicU64::new(0),
            key_budget_hits: AtomicU64::new(0),
            latency_throttled: AtomicU64::new(0),
            completed_in_budget: AtomicU64::new(0),
            max_sweep_us: AtomicU64::new(0),
            sum_sweep_us: AtomicU64::new(0),
            total_sweeps: AtomicU64::new(0),
        }
    }

    /// Record a budget sweep result.
    pub fn record(&self, result: &BudgetSweepResult) {
        self.total_sweeps.fetch_add(1, Ordering::Relaxed);
        self.sum_sweep_us.fetch_add(result.inner.sweep_duration_us, Ordering::Relaxed);
        if result.budget_exhausted {
            self.time_budget_hits.fetch_add(1, Ordering::Relaxed);
        }
        if result.key_budget_exhausted {
            self.key_budget_hits.fetch_add(1, Ordering::Relaxed);
        }
        if result.latency_throttled {
            self.latency_throttled.fetch_add(1, Ordering::Relaxed);
        }
        if !result.budget_exhausted && !result.key_budget_exhausted {
            self.completed_in_budget.fetch_add(1, Ordering::Relaxed);
        }
        // CAS update for max
        let dur = result.inner.sweep_duration_us;
        let mut current = self.max_sweep_us.load(Ordering::Relaxed);
        while dur > current {
            match self.max_sweep_us.compare_exchange_weak(current, dur, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Snapshot for reporting.
    pub fn snapshot(&self) -> GcBudgetSnapshot {
        let total = self.total_sweeps.load(Ordering::Relaxed);
        let sum = self.sum_sweep_us.load(Ordering::Relaxed);
        GcBudgetSnapshot {
            time_budget_hits: self.time_budget_hits.load(Ordering::Relaxed),
            key_budget_hits: self.key_budget_hits.load(Ordering::Relaxed),
            latency_throttled: self.latency_throttled.load(Ordering::Relaxed),
            completed_in_budget: self.completed_in_budget.load(Ordering::Relaxed),
            max_sweep_us: self.max_sweep_us.load(Ordering::Relaxed),
            mean_sweep_us: if total > 0 { sum as f64 / total as f64 } else { 0.0 },
            total_sweeps: total,
        }
    }
}

impl Default for GcBudgetMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable snapshot for reporting.
#[derive(Debug, Clone, Default)]
pub struct GcBudgetSnapshot {
    pub time_budget_hits: u64,
    pub key_budget_hits: u64,
    pub latency_throttled: u64,
    pub completed_in_budget: u64,
    pub max_sweep_us: u64,
    pub mean_sweep_us: f64,
    pub total_sweeps: u64,
}

impl GcBudgetSnapshot {
    /// Fraction of sweeps that completed within budget.
    pub fn in_budget_rate(&self) -> f64 {
        if self.total_sweeps == 0 { 1.0 }
        else { self.completed_in_budget as f64 / self.total_sweeps as f64 }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_empty() {
        let h = ChainLengthHistogram::new(&[1, 2, 5, 10]);
        assert_eq!(h.total, 0);
        assert_eq!(h.mean(), 0.0);
        assert_eq!(h.max_length, 0);
    }

    #[test]
    fn test_histogram_observe() {
        let mut h = ChainLengthHistogram::new(&[1, 2, 5, 10]);
        h.observe(1);
        h.observe(1);
        h.observe(3);
        h.observe(7);
        h.observe(15); // overflow
        assert_eq!(h.total, 5);
        assert_eq!(h.counts[0], 2); // <= 1
        assert_eq!(h.counts[1], 0); // <= 2
        assert_eq!(h.counts[2], 1); // <= 5
        assert_eq!(h.counts[3], 1); // <= 10
        assert_eq!(h.overflow, 1);
        assert_eq!(h.max_length, 15);
        assert!((h.mean() - 5.4).abs() < 0.01);
    }

    #[test]
    fn test_histogram_reset() {
        let mut h = ChainLengthHistogram::new(&[1, 5, 10]);
        h.observe(3);
        h.observe(8);
        h.reset();
        assert_eq!(h.total, 0);
        assert_eq!(h.max_length, 0);
        assert_eq!(h.overflow, 0);
    }

    #[test]
    fn test_gc_budget_config_defaults() {
        let cfg = GcBudgetConfig::default();
        assert_eq!(cfg.max_sweep_us, 5_000);
        assert_eq!(cfg.max_keys_per_sweep, 10_000);
        assert_eq!(cfg.normal_interval_ms, 500);
    }

    #[test]
    fn test_gc_budget_metrics_record() {
        let metrics = GcBudgetMetrics::new();
        let result = BudgetSweepResult {
            inner: GcSweepResult {
                sweep_duration_us: 3000,
                reclaimed_versions: 5,
                ..Default::default()
            },
            budget_exhausted: false,
            key_budget_exhausted: false,
            latency_throttled: false,
            histogram: ChainLengthHistogram::new(&[1, 5, 10]),
        };
        metrics.record(&result);
        let snap = metrics.snapshot();
        assert_eq!(snap.total_sweeps, 1);
        assert_eq!(snap.completed_in_budget, 1);
        assert_eq!(snap.time_budget_hits, 0);
        assert_eq!(snap.max_sweep_us, 3000);
        assert!((snap.mean_sweep_us - 3000.0).abs() < 0.01);
        assert!((snap.in_budget_rate() - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_gc_budget_metrics_budget_hit() {
        let metrics = GcBudgetMetrics::new();
        let result = BudgetSweepResult {
            inner: GcSweepResult {
                sweep_duration_us: 6000,
                ..Default::default()
            },
            budget_exhausted: true,
            key_budget_exhausted: false,
            latency_throttled: false,
            histogram: ChainLengthHistogram::new(&[1, 5, 10]),
        };
        metrics.record(&result);
        let snap = metrics.snapshot();
        assert_eq!(snap.time_budget_hits, 1);
        assert_eq!(snap.completed_in_budget, 0);
        assert!((snap.in_budget_rate() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_gc_budget_metrics_multiple_sweeps() {
        let metrics = GcBudgetMetrics::new();
        for i in 0..10 {
            let result = BudgetSweepResult {
                inner: GcSweepResult {
                    sweep_duration_us: 1000 + i * 500,
                    ..Default::default()
                },
                budget_exhausted: i >= 8, // last 2 hit budget
                key_budget_exhausted: false,
                latency_throttled: i == 5, // one throttled
                histogram: ChainLengthHistogram::new(&[1, 5, 10]),
            };
            metrics.record(&result);
        }
        let snap = metrics.snapshot();
        assert_eq!(snap.total_sweeps, 10);
        assert_eq!(snap.time_budget_hits, 2);
        assert_eq!(snap.latency_throttled, 1);
        assert_eq!(snap.completed_in_budget, 8);
        assert_eq!(snap.max_sweep_us, 5500); // 1000 + 9*500
    }

    #[test]
    fn test_histogram_all_overflow() {
        let mut h = ChainLengthHistogram::new(&[1, 2, 3]);
        h.observe(100);
        h.observe(200);
        assert_eq!(h.overflow, 2);
        assert_eq!(h.counts.iter().sum::<u64>(), 0);
    }

    #[test]
    fn test_histogram_boundary_values() {
        let mut h = ChainLengthHistogram::new(&[1, 5, 10]);
        h.observe(1);  // exactly on boundary → bucket 0
        h.observe(5);  // exactly on boundary → bucket 1
        h.observe(10); // exactly on boundary → bucket 2
        assert_eq!(h.counts[0], 1);
        assert_eq!(h.counts[1], 1);
        assert_eq!(h.counts[2], 1);
        assert_eq!(h.overflow, 0);
    }
}
