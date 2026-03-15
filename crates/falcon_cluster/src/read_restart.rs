//! # C6: Read Restart Protocol (Uncertainty Interval Retry)
//!
//! Detects and resolves **timestamp uncertainty conflicts** during reads,
//! analogous to CockroachDB's `ReadWithinUncertaintyIntervalError` and
//! the associated read-restart retry loop.
//!
//! ## The Problem
//!
//! In a distributed system, two nodes' clocks are never perfectly synchronized.
//! Suppose node A commits a transaction at wall-clock time T, but node B's
//! clock believes the current time is T-5ms (B is 5ms behind A).
//!
//! A read started on node B at timestamp T_read = T-5ms could miss A's commit
//! even though A's commit was causally before B's read.
//!
//! ## CockroachDB's Solution
//!
//! CockroachDB defines an **uncertainty window** `[T_read, T_read + max_clock_skew]`.
//! If any committed value falls within this window, the read must be restarted
//! at a higher timestamp to ensure it observes that value.
//!
//! ## FalconDB Implementation
//!
//! `ReadRestartGuard` wraps a read operation and:
//! 1. Executes the read at `start_ts`.
//! 2. Checks if any row version has `commit_ts` in `(start_ts, start_ts + uncertainty_ns)`.
//! 3. If yes → returns `ReadRestartNeeded { restart_ts }` where
//!    `restart_ts = max_uncertain_commit_ts + 1`.
//! 4. The caller retries the read at `restart_ts`.
//! 5. After `max_retries` the read is escalated to the leader path.
//!
//! ## Integration
//!
//! ```ignore
//! let guard = ReadRestartGuard::new(uncertainty_ms, max_retries);
//! let result = guard.execute(|read_ts| {
//!     storage.scan(table_id, txn_id, read_ts)
//! })?;
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use falcon_common::error::FalconError;
use falcon_common::types::Timestamp;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for the read-restart protocol.
#[derive(Debug, Clone)]
pub struct ReadRestartConfig {
    /// Maximum assumed clock skew between nodes.
    /// Rows with commit_ts in (read_ts, read_ts + uncertainty_window) trigger a restart.
    /// Default: 500ms expressed in HLC units (physical_ms << 16).
    pub uncertainty_window: u64,
    /// Maximum number of restart attempts before escalating to leader.
    /// Default: 3.
    pub max_retries: u32,
    /// Whether to enable uncertainty-interval checking at all.
    /// Can be disabled for single-node deployments where clock skew is zero.
    /// Default: true.
    pub enabled: bool,
}

impl Default for ReadRestartConfig {
    fn default() -> Self {
        // 500ms in HLC Timestamp units: physical_ms is in bits [63..16].
        let uncertainty_ms: u64 = 500;
        Self {
            uncertainty_window: uncertainty_ms << 16,
            max_retries: 3,
            enabled: true,
        }
    }
}

impl ReadRestartConfig {
    pub fn with_uncertainty_ms(uncertainty_ms: u64) -> Self {
        Self {
            uncertainty_window: uncertainty_ms << 16,
            ..Default::default()
        }
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Read Restart Outcome
// ─────────────────────────────────────────────────────────────────────────────

/// Outcome of a single read attempt.
#[derive(Debug, Clone)]
pub enum ReadAttemptOutcome<T> {
    /// Read succeeded — no uncertainty conflicts.
    Ok(T),
    /// Uncertainty interval conflict — retry at `restart_ts`.
    RestartNeeded {
        /// The timestamp at which the next attempt should read.
        restart_ts: Timestamp,
        /// The commit_ts of the conflicting row version.
        conflicting_ts: Timestamp,
    },
}

/// Final error when all retries are exhausted.
#[derive(Debug, Clone)]
pub struct ReadRestartExhausted {
    pub attempts: u32,
    pub last_read_ts: Timestamp,
    pub last_conflicting_ts: Timestamp,
}

impl std::fmt::Display for ReadRestartExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReadRestart exhausted after {} attempts (last_ts={}, conflicting_ts={})",
            self.attempts, self.last_read_ts.0, self.last_conflicting_ts.0
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Uncertainty Checker
// ─────────────────────────────────────────────────────────────────────────────

/// Checks whether a set of row commit timestamps falls within the uncertainty window.
///
/// Used by storage/executor code to detect whether a restart is needed.
pub struct UncertaintyChecker {
    config: ReadRestartConfig,
}

impl UncertaintyChecker {
    pub fn new(config: ReadRestartConfig) -> Self {
        Self { config }
    }

    /// Check if any of the given commit timestamps falls in the uncertainty window
    /// `(read_ts, read_ts + uncertainty_window)`.
    ///
    /// Returns the maximum such commit_ts if a conflict exists, or `None` if clean.
    pub fn check_uncertainty(
        &self,
        read_ts: Timestamp,
        commit_timestamps: &[Timestamp],
    ) -> Option<Timestamp> {
        if !self.config.enabled {
            return None;
        }
        let window_end = read_ts.0.saturating_add(self.config.uncertainty_window);
        commit_timestamps
            .iter()
            .filter(|ts| ts.0 > read_ts.0 && ts.0 <= window_end)
            .max_by_key(|ts| ts.0)
            .copied()
    }

    /// Compute the restart timestamp given a conflicting commit_ts.
    ///
    /// The restart_ts is set to `conflicting_ts + 1` so the next read
    /// observes the conflicting version.
    pub fn restart_ts(conflicting_ts: Timestamp) -> Timestamp {
        Timestamp(conflicting_ts.0 + 1)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ReadRestartGuard
// ─────────────────────────────────────────────────────────────────────────────

/// Metrics for the read restart subsystem.
#[derive(Debug, Default)]
pub struct ReadRestartMetrics {
    /// Total reads attempted (all attempts across all guard instances).
    pub total_reads: AtomicU64,
    /// Reads that completed on the first attempt (no restart needed).
    pub clean_reads: AtomicU64,
    /// Total restart events (each restart increments this).
    pub restarts: AtomicU64,
    /// Reads that exhausted all retries (escalated to leader).
    pub exhausted: AtomicU64,
}

impl ReadRestartMetrics {
    pub fn snapshot(&self) -> ReadRestartMetricsSnapshot {
        ReadRestartMetricsSnapshot {
            total_reads: self.total_reads.load(Ordering::Relaxed),
            clean_reads: self.clean_reads.load(Ordering::Relaxed),
            restarts: self.restarts.load(Ordering::Relaxed),
            exhausted: self.exhausted.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadRestartMetricsSnapshot {
    pub total_reads: u64,
    pub clean_reads: u64,
    pub restarts: u64,
    pub exhausted: u64,
}

/// Executes a read with automatic uncertainty-interval restart.
///
/// # Type Parameters
/// - `T`: the read result type (e.g. `Vec<OwnedRow>`)
///
/// # Usage
/// ```ignore
/// let guard = ReadRestartGuard::new(config, metrics);
/// let rows = guard.execute(start_ts, |ts| {
///     // Perform the actual read at `ts` and return (result, commit_timestamps).
///     let rows = storage.scan(table_id, txn_id, ts)?;
///     let cts: Vec<Timestamp> = rows.iter().map(|r| r.commit_ts).collect();
///     Ok((rows, cts))
/// })?;
/// ```
pub struct ReadRestartGuard {
    checker: UncertaintyChecker,
    metrics: Arc<ReadRestartMetrics>,
}

impl ReadRestartGuard {
    pub fn new(config: ReadRestartConfig, metrics: Arc<ReadRestartMetrics>) -> Self {
        Self {
            checker: UncertaintyChecker::new(config),
            metrics,
        }
    }

    pub fn new_default() -> Self {
        Self::new(
            ReadRestartConfig::default(),
            Arc::new(ReadRestartMetrics::default()),
        )
    }

    /// Execute a read closure with automatic restart on uncertainty conflicts.
    ///
    /// The closure receives the current `read_ts` and returns:
    /// - `Ok((result, commit_timestamps))` on success
    /// - `Err(e)` on storage error
    ///
    /// `commit_timestamps` are the commit timestamps of rows visible to this read.
    /// The guard checks them for uncertainty conflicts and retries if needed.
    pub fn execute<T, F>(
        &self,
        start_ts: Timestamp,
        mut f: F,
    ) -> Result<T, ReadRestartError>
    where
        F: FnMut(Timestamp) -> Result<(T, Vec<Timestamp>), FalconError>,
    {
        let max_retries = self.checker.config.max_retries;
        let mut read_ts = start_ts;
        let mut last_conflicting = Timestamp(0);

        self.metrics.total_reads.fetch_add(1, Ordering::Relaxed);

        for attempt in 0..=max_retries {
            let (result, commit_ts_vec) = f(read_ts).map_err(ReadRestartError::Storage)?;

            match self.checker.check_uncertainty(read_ts, &commit_ts_vec) {
                None => {
                    // Clean read — no uncertainty conflict.
                    if attempt == 0 {
                        self.metrics.clean_reads.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(result);
                }
                Some(conflicting_ts) => {
                    last_conflicting = conflicting_ts;
                    if attempt < max_retries {
                        self.metrics.restarts.fetch_add(1, Ordering::Relaxed);
                        read_ts = UncertaintyChecker::restart_ts(conflicting_ts);
                        tracing::debug!(
                            "ReadRestart attempt {}/{}: ts {} → {} (conflict at {})",
                            attempt + 1,
                            max_retries,
                            read_ts.0 - 1,
                            read_ts.0,
                            conflicting_ts.0,
                        );
                    } else {
                        // All retries exhausted.
                        self.metrics.exhausted.fetch_add(1, Ordering::Relaxed);
                        return Err(ReadRestartError::Exhausted(ReadRestartExhausted {
                            attempts: attempt + 1,
                            last_read_ts: read_ts,
                            last_conflicting_ts: last_conflicting,
                        }));
                    }
                }
            }
        }

        // Unreachable — loop always returns.
        Err(ReadRestartError::Exhausted(ReadRestartExhausted {
            attempts: max_retries + 1,
            last_read_ts: read_ts,
            last_conflicting_ts: last_conflicting,
        }))
    }
}

/// Error from `ReadRestartGuard::execute`.
#[derive(Debug)]
pub enum ReadRestartError {
    /// Underlying storage error.
    Storage(FalconError),
    /// All retry attempts exhausted — caller should escalate to leader path.
    Exhausted(ReadRestartExhausted),
}

impl std::fmt::Display for ReadRestartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(e) => write!(f, "ReadRestart storage error: {e}"),
            Self::Exhausted(e) => write!(f, "{e}"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(v: u64) -> Timestamp {
        Timestamp(v)
    }

    #[test]
    fn no_uncertainty_clean_read() {
        let cfg = ReadRestartConfig::with_uncertainty_ms(500);
        let checker = UncertaintyChecker::new(cfg);
        // Commit timestamps are all <= read_ts (no uncertainty).
        let conflict = checker.check_uncertainty(ts(1000 << 16), &[ts(500 << 16), ts(800 << 16)]);
        assert!(conflict.is_none());
    }

    #[test]
    fn uncertainty_detected_in_window() {
        let cfg = ReadRestartConfig::with_uncertainty_ms(500);
        let checker = UncertaintyChecker::new(cfg);
        let read_ts = ts(1000 << 16);
        // commit_ts = 1200ms << 16 is inside (1000, 1500) window
        let commit_ts = ts(1200 << 16);
        let conflict = checker.check_uncertainty(read_ts, &[commit_ts]);
        assert!(conflict.is_some());
        assert_eq!(conflict.unwrap().0, 1200 << 16);
    }

    #[test]
    fn uncertainty_outside_window_no_conflict() {
        let cfg = ReadRestartConfig::with_uncertainty_ms(100);
        let checker = UncertaintyChecker::new(cfg);
        let read_ts = ts(1000 << 16);
        // commit_ts = 1200ms is outside [1000, 1100] window
        let commit_ts = ts(1200 << 16);
        let conflict = checker.check_uncertainty(read_ts, &[commit_ts]);
        assert!(conflict.is_none());
    }

    #[test]
    fn restart_guard_clean_read() {
        let guard = ReadRestartGuard::new_default();
        let result = guard.execute(ts(1000), |_ts| Ok((42u32, vec![ts(500), ts(800)])));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn restart_guard_retries_on_conflict() {
        let config = ReadRestartConfig {
            uncertainty_window: 200,
            max_retries: 2,
            enabled: true,
        };
        let metrics = Arc::new(ReadRestartMetrics::default());
        let guard = ReadRestartGuard::new(config, Arc::clone(&metrics));

        let call_count = std::sync::atomic::AtomicU32::new(0);
        let result = guard.execute(ts(1000), |read_ts| {
            let n = call_count.fetch_add(1, Ordering::Relaxed);
            if n == 0 {
                // First call: return a commit_ts in the uncertainty window.
                Ok((99u32, vec![ts(read_ts.0 + 100)]))
            } else {
                // Second call: clean.
                Ok((99u32, vec![ts(500)]))
            }
        });
        assert!(result.is_ok());
        assert_eq!(metrics.restarts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn restart_guard_exhausted() {
        let config = ReadRestartConfig {
            uncertainty_window: 1000,
            max_retries: 2,
            enabled: true,
        };
        let metrics = Arc::new(ReadRestartMetrics::default());
        let guard = ReadRestartGuard::new(config, Arc::clone(&metrics));

        // Always return a conflict.
        let result = guard.execute(ts(1000), |read_ts| {
            // Return a commit_ts always within the window.
            Ok((0u32, vec![Timestamp(read_ts.0 + 100)]))
        });
        assert!(matches!(result, Err(ReadRestartError::Exhausted(_))));
        assert_eq!(metrics.exhausted.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn disabled_checker_never_conflicts() {
        let cfg = ReadRestartConfig::disabled();
        let checker = UncertaintyChecker::new(cfg);
        // Even with ts inside window, disabled checker returns None.
        let conflict =
            checker.check_uncertainty(ts(1000 << 16), &[ts(1100 << 16), ts(1200 << 16)]);
        assert!(conflict.is_none());
    }
}
