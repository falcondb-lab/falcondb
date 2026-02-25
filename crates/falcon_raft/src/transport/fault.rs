//! Fault injection transport wrapper for testing.
//!
//! `FaultyTransport` wraps any transport and injects configurable faults:
//! - **Drop**: randomly drop messages (configurable drop rate)
//! - **Delay**: add latency to messages (configurable distribution)
//! - **Reorder**: buffer messages and deliver out of order
//! - **Partition**: block messages between specific node pairs
//!
//! Used in integration tests to validate Raft safety under network failures.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Fault Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// Fault injection configuration.
#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// Drop rate: fraction of messages to drop (0.0 = none, 1.0 = all).
    pub drop_rate: f64,
    /// Minimum added delay (milliseconds).
    pub delay_min_ms: u64,
    /// Maximum added delay (milliseconds).
    pub delay_max_ms: u64,
    /// Reorder window: messages within this window may be delivered out of order.
    /// 0 = no reordering.
    pub reorder_window: usize,
    /// Set of partitioned node pairs: (from, to). Messages between these pairs are dropped.
    pub partitions: HashSet<(u64, u64)>,
    /// Whether faults are currently active.
    pub enabled: bool,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            drop_rate: 0.0,
            delay_min_ms: 0,
            delay_max_ms: 0,
            reorder_window: 0,
            partitions: HashSet::new(),
            enabled: false,
        }
    }
}

impl FaultConfig {
    /// Create a config that drops a fraction of messages.
    pub fn with_drop_rate(rate: f64) -> Self {
        Self {
            drop_rate: rate,
            enabled: true,
            ..Default::default()
        }
    }

    /// Create a config that adds delay to all messages.
    pub fn with_delay(min_ms: u64, max_ms: u64) -> Self {
        Self {
            delay_min_ms: min_ms,
            delay_max_ms: max_ms,
            enabled: true,
            ..Default::default()
        }
    }

    /// Create a config that partitions specific node pairs.
    pub fn with_partition(pairs: Vec<(u64, u64)>) -> Self {
        Self {
            partitions: pairs.into_iter().collect(),
            enabled: true,
            ..Default::default()
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Fault Injector
// ═══════════════════════════════════════════════════════════════════════════

/// Fault injector that can be shared across transport instances.
/// Thread-safe: config can be updated at runtime.
pub struct FaultInjector {
    config: RwLock<FaultConfig>,
    /// Monotonic counter for deterministic pseudo-random decisions.
    counter: AtomicU64,
    /// Metrics.
    messages_dropped: AtomicU64,
    messages_delayed: AtomicU64,
    messages_partitioned: AtomicU64,
    messages_passed: AtomicU64,
}

impl FaultInjector {
    pub const fn new(config: FaultConfig) -> Self {
        Self {
            config: RwLock::new(config),
            counter: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
            messages_delayed: AtomicU64::new(0),
            messages_partitioned: AtomicU64::new(0),
            messages_passed: AtomicU64::new(0),
        }
    }

    /// Create a disabled (passthrough) injector.
    pub fn disabled() -> Self {
        Self::new(FaultConfig::default())
    }

    /// Update the fault configuration at runtime.
    pub fn update_config(&self, config: FaultConfig) {
        *self.config.write() = config;
    }

    /// Enable fault injection.
    pub fn enable(&self) {
        self.config.write().enabled = true;
    }

    /// Disable fault injection (passthrough mode).
    pub fn disable(&self) {
        self.config.write().enabled = false;
    }

    /// Add a partition between two nodes (bidirectional).
    pub fn add_partition(&self, a: u64, b: u64) {
        let mut cfg = self.config.write();
        cfg.partitions.insert((a, b));
        cfg.partitions.insert((b, a));
        cfg.enabled = true;
    }

    /// Remove a partition between two nodes (bidirectional).
    pub fn remove_partition(&self, a: u64, b: u64) {
        let mut cfg = self.config.write();
        cfg.partitions.remove(&(a, b));
        cfg.partitions.remove(&(b, a));
    }

    /// Clear all partitions.
    pub fn clear_partitions(&self) {
        self.config.write().partitions.clear();
    }

    /// Set the drop rate.
    pub fn set_drop_rate(&self, rate: f64) {
        let mut cfg = self.config.write();
        cfg.drop_rate = rate;
        if rate > 0.0 {
            cfg.enabled = true;
        }
    }

    /// Set the delay range.
    pub fn set_delay(&self, min_ms: u64, max_ms: u64) {
        let mut cfg = self.config.write();
        cfg.delay_min_ms = min_ms;
        cfg.delay_max_ms = max_ms;
        if min_ms > 0 || max_ms > 0 {
            cfg.enabled = true;
        }
    }

    /// Decide what fault (if any) to apply to a message from `from` to `to`.
    /// Returns the fault decision.
    pub fn decide(&self, from: u64, to: u64) -> FaultDecision {
        let cfg = self.config.read();
        if !cfg.enabled {
            self.messages_passed.fetch_add(1, Ordering::Relaxed);
            return FaultDecision::Pass;
        }

        // Check partition
        if cfg.partitions.contains(&(from, to)) {
            self.messages_partitioned.fetch_add(1, Ordering::Relaxed);
            return FaultDecision::Drop;
        }

        // Check drop rate
        if cfg.drop_rate > 0.0 {
            let n = self.counter.fetch_add(1, Ordering::Relaxed);
            // Deterministic pseudo-random based on counter
            let hash = simple_hash(n);
            let threshold = (cfg.drop_rate * u64::MAX as f64) as u64;
            if hash < threshold {
                self.messages_dropped.fetch_add(1, Ordering::Relaxed);
                return FaultDecision::Drop;
            }
        }

        // Check delay
        if cfg.delay_min_ms > 0 || cfg.delay_max_ms > 0 {
            let n = self.counter.fetch_add(1, Ordering::Relaxed);
            let range = cfg.delay_max_ms.saturating_sub(cfg.delay_min_ms);
            let delay = if range > 0 {
                cfg.delay_min_ms + (simple_hash(n) % range)
            } else {
                cfg.delay_min_ms
            };
            self.messages_delayed.fetch_add(1, Ordering::Relaxed);
            return FaultDecision::Delay(Duration::from_millis(delay));
        }

        self.messages_passed.fetch_add(1, Ordering::Relaxed);
        FaultDecision::Pass
    }

    /// Get fault injection metrics snapshot.
    pub fn metrics(&self) -> FaultMetrics {
        FaultMetrics {
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
            messages_delayed: self.messages_delayed.load(Ordering::Relaxed),
            messages_partitioned: self.messages_partitioned.load(Ordering::Relaxed),
            messages_passed: self.messages_passed.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters.
    pub fn reset_metrics(&self) {
        self.messages_dropped.store(0, Ordering::Relaxed);
        self.messages_delayed.store(0, Ordering::Relaxed);
        self.messages_partitioned.store(0, Ordering::Relaxed);
        self.messages_passed.store(0, Ordering::Relaxed);
    }
}

/// Fault decision for a single message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultDecision {
    /// Pass the message through unchanged.
    Pass,
    /// Drop the message entirely.
    Drop,
    /// Delay the message by the given duration.
    Delay(Duration),
}

/// Fault injection metrics.
#[derive(Debug, Clone, Default)]
pub struct FaultMetrics {
    pub messages_dropped: u64,
    pub messages_delayed: u64,
    pub messages_partitioned: u64,
    pub messages_passed: u64,
}

impl FaultMetrics {
    pub const fn total(&self) -> u64 {
        self.messages_dropped + self.messages_delayed + self.messages_partitioned + self.messages_passed
    }

    pub fn drop_rate(&self) -> f64 {
        let total = self.total();
        if total == 0 {
            0.0
        } else {
            (self.messages_dropped + self.messages_partitioned) as f64 / total as f64
        }
    }
}

/// Simple non-cryptographic hash for deterministic pseudo-random decisions.
const fn simple_hash(n: u64) -> u64 {
    let mut x = n;
    x = x.wrapping_mul(0x517cc1b727220a95);
    x = x.rotate_left(32);
    x = x.wrapping_mul(0x6c62272e07bb0142);
    x ^= x >> 33;
    x
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fault_injector_disabled() {
        let fi = FaultInjector::disabled();
        for _ in 0..100 {
            assert_eq!(fi.decide(1, 2), FaultDecision::Pass);
        }
        assert_eq!(fi.metrics().messages_passed, 100);
    }

    #[test]
    fn test_fault_injector_partition() {
        let fi = FaultInjector::new(FaultConfig::with_partition(vec![(1, 2)]));
        assert_eq!(fi.decide(1, 2), FaultDecision::Drop);
        assert_eq!(fi.decide(2, 1), FaultDecision::Pass); // unidirectional
        assert_eq!(fi.decide(1, 3), FaultDecision::Pass); // different pair
        assert_eq!(fi.metrics().messages_partitioned, 1);
    }

    #[test]
    fn test_fault_injector_bidirectional_partition() {
        let fi = FaultInjector::disabled();
        fi.add_partition(1, 2);
        assert_eq!(fi.decide(1, 2), FaultDecision::Drop);
        assert_eq!(fi.decide(2, 1), FaultDecision::Drop);
        assert_eq!(fi.decide(1, 3), FaultDecision::Pass);
    }

    #[test]
    fn test_fault_injector_remove_partition() {
        let fi = FaultInjector::disabled();
        fi.add_partition(1, 2);
        assert_eq!(fi.decide(1, 2), FaultDecision::Drop);
        fi.remove_partition(1, 2);
        assert_eq!(fi.decide(1, 2), FaultDecision::Pass);
    }

    #[test]
    fn test_fault_injector_drop_rate() {
        let fi = FaultInjector::new(FaultConfig::with_drop_rate(0.5));
        let mut dropped = 0;
        let total = 1000;
        for _ in 0..total {
            if fi.decide(1, 2) == FaultDecision::Drop {
                dropped += 1;
            }
        }
        // With 50% drop rate, expect roughly 500 drops (allow wide margin for hash distribution)
        assert!(
            dropped > 200 && dropped < 800,
            "expected ~500 drops, got {}",
            dropped
        );
    }

    #[test]
    fn test_fault_injector_delay() {
        let fi = FaultInjector::new(FaultConfig::with_delay(100, 200));
        let decision = fi.decide(1, 2);
        match decision {
            FaultDecision::Delay(d) => {
                assert!(d >= Duration::from_millis(100));
                assert!(d <= Duration::from_millis(200));
            }
            other => panic!("expected Delay, got {:?}", other),
        }
    }

    #[test]
    fn test_fault_injector_enable_disable() {
        let fi = FaultInjector::new(FaultConfig::with_drop_rate(1.0));
        assert_eq!(fi.decide(1, 2), FaultDecision::Drop);
        fi.disable();
        assert_eq!(fi.decide(1, 2), FaultDecision::Pass);
        fi.enable();
        assert_eq!(fi.decide(1, 2), FaultDecision::Drop);
    }

    #[test]
    fn test_fault_injector_metrics() {
        let fi = FaultInjector::disabled();
        fi.add_partition(1, 2);
        fi.decide(1, 2); // partitioned
        fi.decide(1, 3); // passed
        fi.decide(1, 3); // passed
        let m = fi.metrics();
        assert_eq!(m.messages_partitioned, 1);
        assert_eq!(m.messages_passed, 2);
        assert_eq!(m.total(), 3);
    }

    #[test]
    fn test_fault_injector_reset_metrics() {
        let fi = FaultInjector::disabled();
        fi.decide(1, 2);
        fi.decide(1, 2);
        assert_eq!(fi.metrics().messages_passed, 2);
        fi.reset_metrics();
        assert_eq!(fi.metrics().messages_passed, 0);
    }

    #[test]
    fn test_fault_config_default() {
        let cfg = FaultConfig::default();
        assert_eq!(cfg.drop_rate, 0.0);
        assert_eq!(cfg.delay_min_ms, 0);
        assert!(!cfg.enabled);
        assert!(cfg.partitions.is_empty());
    }

    #[test]
    fn test_fault_metrics_drop_rate() {
        let m = FaultMetrics {
            messages_dropped: 20,
            messages_partitioned: 10,
            messages_delayed: 0,
            messages_passed: 70,
        };
        assert!((m.drop_rate() - 0.3).abs() < 0.01);
    }

    #[test]
    fn test_fault_decision_deterministic() {
        // Same injector, same counter → same decisions
        let fi1 = FaultInjector::new(FaultConfig::with_drop_rate(0.5));
        let fi2 = FaultInjector::new(FaultConfig::with_drop_rate(0.5));
        let decisions1: Vec<_> = (0..100).map(|_| fi1.decide(1, 2)).collect();
        let decisions2: Vec<_> = (0..100).map(|_| fi2.decide(1, 2)).collect();
        assert_eq!(decisions1, decisions2, "fault injection should be deterministic");
    }
}
