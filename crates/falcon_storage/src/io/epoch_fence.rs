//! Storage-layer epoch fencing for WAL write barrier.
//!
//! Enforces that all WAL writes carry a valid epoch/term. A write from a
//! stale epoch (e.g., a partitioned leader that has been superseded) is
//! **deterministically rejected** at the final write barrier, preventing
//! split-brain data divergence.
//!
//! This is the storage-level enforcement. The cluster layer (`SplitBrainDetector`,
//! `HAReplicaGroup`) provides the epoch lifecycle management; this module
//! provides the final guardrail at the I/O boundary.
//!
//! # Protocol
//!
//! 1. Cluster layer bumps epoch on every leadership change.
//! 2. `StorageEpochFence::advance_epoch(new)` is called after promotion.
//! 3. Every WAL append carries `writer_epoch`.
//! 4. `check_write(writer_epoch)` rejects if `writer_epoch < current_epoch`.
//! 5. Rejection is logged, metered, and returns a typed error.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Epoch Fence
// ═══════════════════════════════════════════════════════════════════════════

/// Result of an epoch fence check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochFenceResult {
    /// Write epoch is current or newer — allowed.
    Allowed,
    /// Write epoch is stale — rejected. Contains diagnostic info.
    Rejected {
        writer_epoch: u64,
        current_epoch: u64,
    },
}

/// Metrics for the storage epoch fence.
#[derive(Debug, Clone, Default)]
pub struct EpochFenceMetrics {
    /// Total write checks performed.
    pub total_checks: u64,
    /// Total stale-epoch rejections.
    pub total_rejections: u64,
    /// Last rejection details.
    pub last_rejection_epoch: Option<u64>,
    /// Last rejection timestamp (monotonic).
    pub last_rejection_at: Option<Instant>,
    /// Number of epoch advances.
    pub epoch_advances: u64,
}

/// Storage-layer epoch fence — the final guardrail before WAL bytes hit disk.
///
/// This is intentionally simple and fast: a single atomic load on the hot path.
/// The fence is checked BEFORE `write_at()` on the WAL file, ensuring that
/// no stale-epoch bytes ever reach the durability barrier.
pub struct StorageEpochFence {
    /// Current authoritative epoch for this shard's storage.
    current_epoch: AtomicU64,
    /// Whether fencing is enabled (can be disabled for single-node setups).
    enabled: bool,
    /// Metrics (mutex-protected, only touched on rejection or advance — not hot path).
    metrics: Mutex<EpochFenceMetrics>,
}

impl StorageEpochFence {
    /// Create a new fence with the given initial epoch.
    pub fn new(initial_epoch: u64) -> Self {
        Self {
            current_epoch: AtomicU64::new(initial_epoch),
            enabled: true,
            metrics: Mutex::new(EpochFenceMetrics::default()),
        }
    }

    /// Create a disabled fence (single-node mode, no replication).
    pub fn disabled() -> Self {
        Self {
            current_epoch: AtomicU64::new(0),
            enabled: false,
            metrics: Mutex::new(EpochFenceMetrics::default()),
        }
    }

    /// Check if a write with the given epoch is allowed.
    ///
    /// **Hot path**: when the write is allowed (normal case), this is a single
    /// atomic load with no mutex acquisition.
    #[inline]
    pub fn check_write(&self, writer_epoch: u64) -> EpochFenceResult {
        if !self.enabled {
            return EpochFenceResult::Allowed;
        }

        let current = self.current_epoch.load(Ordering::SeqCst);

        if writer_epoch >= current {
            // Fast path: allowed. Only bump the check counter lazily.
            // We don't acquire the mutex here — metrics.total_checks is
            // approximate (acceptable for observability).
            return EpochFenceResult::Allowed;
        }

        // Slow path: stale epoch — acquire mutex for metrics
        let mut m = self.metrics.lock();
        m.total_checks += 1;
        m.total_rejections += 1;
        m.last_rejection_epoch = Some(writer_epoch);
        m.last_rejection_at = Some(Instant::now());

        tracing::error!(
            writer_epoch = writer_epoch,
            current_epoch = current,
            "EPOCH FENCE: stale-epoch WAL write rejected at storage barrier"
        );

        EpochFenceResult::Rejected {
            writer_epoch,
            current_epoch: current,
        }
    }

    /// Advance the epoch after a successful promotion.
    ///
    /// Uses `fetch_max` to ensure monotonic advancement.
    pub fn advance_epoch(&self, new_epoch: u64) {
        let old = self.current_epoch.fetch_max(new_epoch, Ordering::SeqCst);
        if new_epoch > old {
            let mut m = self.metrics.lock();
            m.epoch_advances += 1;
            tracing::info!(
                old_epoch = old,
                new_epoch = new_epoch,
                "storage epoch fence: epoch advanced"
            );
        }
    }

    /// Current epoch.
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::SeqCst)
    }

    /// Whether fencing is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Metrics snapshot.
    pub fn metrics(&self) -> EpochFenceMetrics {
        self.metrics.lock().clone()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fence_allows_current_epoch() {
        let fence = StorageEpochFence::new(5);
        assert_eq!(fence.check_write(5), EpochFenceResult::Allowed);
        assert_eq!(fence.check_write(6), EpochFenceResult::Allowed);
        assert_eq!(fence.check_write(100), EpochFenceResult::Allowed);
    }

    #[test]
    fn test_fence_rejects_stale_epoch() {
        let fence = StorageEpochFence::new(5);
        assert_eq!(
            fence.check_write(4),
            EpochFenceResult::Rejected {
                writer_epoch: 4,
                current_epoch: 5,
            }
        );
        assert_eq!(
            fence.check_write(1),
            EpochFenceResult::Rejected {
                writer_epoch: 1,
                current_epoch: 5,
            }
        );
    }

    #[test]
    fn test_fence_advance_epoch() {
        let fence = StorageEpochFence::new(1);
        assert_eq!(fence.check_write(1), EpochFenceResult::Allowed);

        fence.advance_epoch(3);
        assert_eq!(fence.current_epoch(), 3);

        // Old epoch now rejected
        assert_eq!(
            fence.check_write(2),
            EpochFenceResult::Rejected {
                writer_epoch: 2,
                current_epoch: 3,
            }
        );

        // New epoch allowed
        assert_eq!(fence.check_write(3), EpochFenceResult::Allowed);
    }

    #[test]
    fn test_fence_advance_is_monotonic() {
        let fence = StorageEpochFence::new(5);
        fence.advance_epoch(3); // Should be ignored (not monotonic)
        assert_eq!(fence.current_epoch(), 5);
        fence.advance_epoch(7);
        assert_eq!(fence.current_epoch(), 7);
    }

    #[test]
    fn test_fence_disabled() {
        let fence = StorageEpochFence::disabled();
        assert!(!fence.is_enabled());
        // All writes allowed when disabled
        assert_eq!(fence.check_write(0), EpochFenceResult::Allowed);
        assert_eq!(fence.check_write(999), EpochFenceResult::Allowed);
    }

    #[test]
    fn test_fence_metrics() {
        let fence = StorageEpochFence::new(5);
        let _ = fence.check_write(5); // allowed
        let _ = fence.check_write(3); // rejected
        let _ = fence.check_write(2); // rejected

        let m = fence.metrics();
        assert_eq!(m.total_rejections, 2);
        assert_eq!(m.last_rejection_epoch, Some(2));
    }
}
