//! P1-3 / P1-6: Fault injection helpers for chaos and stability testing.
//!
//! These helpers are test-only utilities that simulate production failure modes:
//! - Leader crash (abrupt stop of primary)
//! - Replica delay (artificial latency on WAL apply)
//! - WAL corruption (flip bytes in a WAL segment)
//! - Disk latency (configurable sleep before I/O)
//!
//! All helpers are designed to be composable — a chaos test can combine
//! multiple fault types in a single scenario.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Global fault injection state. Thread-safe, lock-free.
///
/// Wire this into the subsystem under test (e.g. replica runner, WAL writer)
/// via `Arc<FaultInjector>`. When a fault is armed, the subsystem should
/// check the injector before critical operations.
#[derive(Debug)]
pub struct FaultInjector {
    /// If true, the node should act as if the leader crashed (reject writes, stop replication).
    leader_killed: AtomicBool,
    /// Artificial delay injected before each WAL apply on replicas (microseconds).
    replica_delay_us: AtomicU64,
    /// If true, the next WAL read should return corrupted data.
    wal_corruption_armed: AtomicBool,
    /// Artificial delay injected before each disk I/O (microseconds).
    disk_delay_us: AtomicU64,
    /// Count of faults that have fired (for observability in tests).
    faults_fired: AtomicU64,
}

impl Default for FaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

impl FaultInjector {
    pub fn new() -> Self {
        Self {
            leader_killed: AtomicBool::new(false),
            replica_delay_us: AtomicU64::new(0),
            wal_corruption_armed: AtomicBool::new(false),
            disk_delay_us: AtomicU64::new(0),
            faults_fired: AtomicU64::new(0),
        }
    }

    // ── Leader crash ──

    /// Simulate killing the leader. After this, `is_leader_killed()` returns true.
    pub fn kill_leader(&self) {
        self.leader_killed.store(true, Ordering::SeqCst);
        self.faults_fired.fetch_add(1, Ordering::Relaxed);
    }

    /// Revive the leader (e.g. after failover completes).
    pub fn revive_leader(&self) {
        self.leader_killed.store(false, Ordering::SeqCst);
    }

    /// Check whether the leader is currently "killed".
    pub fn is_leader_killed(&self) -> bool {
        self.leader_killed.load(Ordering::SeqCst)
    }

    // ── Replica delay ──

    /// Set artificial delay for replica WAL apply (microseconds). 0 = no delay.
    pub fn set_replica_delay(&self, us: u64) {
        self.replica_delay_us.store(us, Ordering::Relaxed);
    }

    /// Get the configured replica delay. Returns Duration::ZERO if no delay.
    pub fn replica_delay(&self) -> Duration {
        Duration::from_micros(self.replica_delay_us.load(Ordering::Relaxed))
    }

    /// Apply the replica delay (blocking sleep). Returns true if delay was applied.
    pub fn maybe_delay_replica(&self) -> bool {
        let us = self.replica_delay_us.load(Ordering::Relaxed);
        if us > 0 {
            std::thread::sleep(Duration::from_micros(us));
            self.faults_fired.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    // ── WAL corruption ──

    /// Arm a WAL corruption fault. The next WAL read should flip some bytes.
    pub fn arm_wal_corruption(&self) {
        self.wal_corruption_armed.store(true, Ordering::SeqCst);
    }

    /// Check and consume the WAL corruption fault. Returns true once (one-shot).
    pub fn take_wal_corruption(&self) -> bool {
        let was_armed = self.wal_corruption_armed.swap(false, Ordering::SeqCst);
        if was_armed {
            self.faults_fired.fetch_add(1, Ordering::Relaxed);
        }
        was_armed
    }

    // ── Disk delay ──

    /// Set artificial delay before disk I/O (microseconds). 0 = no delay.
    pub fn set_disk_delay(&self, us: u64) {
        self.disk_delay_us.store(us, Ordering::Relaxed);
    }

    /// Apply the disk delay (blocking sleep). Returns true if delay was applied.
    pub fn maybe_delay_disk(&self) -> bool {
        let us = self.disk_delay_us.load(Ordering::Relaxed);
        if us > 0 {
            std::thread::sleep(Duration::from_micros(us));
            self.faults_fired.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    // ── Observability ──

    /// Total number of faults that have fired.
    pub fn faults_fired(&self) -> u64 {
        self.faults_fired.load(Ordering::Relaxed)
    }

    /// Reset all faults and counters.
    pub fn reset(&self) {
        self.leader_killed.store(false, Ordering::SeqCst);
        self.replica_delay_us.store(0, Ordering::Relaxed);
        self.wal_corruption_armed.store(false, Ordering::SeqCst);
        self.disk_delay_us.store(0, Ordering::Relaxed);
        self.faults_fired.store(0, Ordering::Relaxed);
    }
}

/// Convenience constructor for `Arc<FaultInjector>`.
pub fn new_injector() -> Arc<FaultInjector> {
    Arc::new(FaultInjector::new())
}

// ── ChaosRunner ──────────────────────────────────────────────────────────────

/// A chaos scenario to execute.
#[derive(Debug, Clone)]
pub enum ChaosScenario {
    /// Kill the leader for the given duration, then revive.
    KillLeader { duration_ms: u64 },
    /// Inject replica delay for the given duration.
    ReplicaDelay { delay_us: u64, duration_ms: u64 },
    /// Arm WAL corruption (one-shot).
    WalCorruption,
    /// Inject disk latency for the given duration.
    DiskLatency { delay_us: u64, duration_ms: u64 },
}

/// Result of a single chaos scenario run.
#[derive(Debug, Clone)]
pub struct ChaosResult {
    pub scenario: String,
    pub duration_ms: u64,
    pub faults_fired: u64,
    pub data_consistent: bool,
    pub error: Option<String>,
}

/// Stability report produced after a chaos test run.
#[derive(Debug, Clone)]
pub struct StabilityReport {
    /// Total scenarios executed.
    pub total_scenarios: usize,
    /// Scenarios that completed without data inconsistency.
    pub passed: usize,
    /// Scenarios that detected data inconsistency or errors.
    pub failed: usize,
    /// Per-scenario results.
    pub results: Vec<ChaosResult>,
    /// Whether all scenarios passed (no data inconsistency detected).
    pub all_consistent: bool,
    /// Wall-clock duration of the full chaos run (milliseconds).
    pub total_duration_ms: u64,
}

impl StabilityReport {
    /// Human-readable summary.
    pub fn summary(&self) -> String {
        format!(
            "ChaosRun: total={} passed={} failed={} consistent={} duration={}ms",
            self.total_scenarios, self.passed, self.failed,
            self.all_consistent, self.total_duration_ms,
        )
    }
}

/// Automated chaos runner — executes a sequence of fault scenarios against
/// a `FaultInjector` and produces a `StabilityReport`.
///
/// The runner is intentionally synchronous (blocking) so it can be used in
/// both unit tests and integration test harnesses without requiring a Tokio runtime.
pub struct ChaosRunner {
    injector: Arc<FaultInjector>,
}

impl ChaosRunner {
    pub fn new(injector: Arc<FaultInjector>) -> Self {
        Self { injector }
    }

    /// Run a sequence of chaos scenarios and return a stability report.
    ///
    /// `consistency_check` is a closure called after each scenario to verify
    /// data consistency. It should return `Ok(())` if consistent, `Err(msg)` otherwise.
    pub fn run<F>(&self, scenarios: Vec<ChaosScenario>, mut consistency_check: F) -> StabilityReport
    where
        F: FnMut() -> Result<(), String>,
    {
        let run_start = std::time::Instant::now();
        let mut results = Vec::with_capacity(scenarios.len());

        for scenario in &scenarios {
            let scenario_start = std::time::Instant::now();
            self.injector.reset();

            let scenario_name = match scenario {
                ChaosScenario::KillLeader { duration_ms } => {
                    self.injector.kill_leader();
                    std::thread::sleep(Duration::from_millis(*duration_ms));
                    self.injector.revive_leader();
                    format!("KillLeader({}ms)", duration_ms)
                }
                ChaosScenario::ReplicaDelay { delay_us, duration_ms } => {
                    self.injector.set_replica_delay(*delay_us);
                    std::thread::sleep(Duration::from_millis(*duration_ms));
                    self.injector.set_replica_delay(0);
                    format!("ReplicaDelay({}us, {}ms)", delay_us, duration_ms)
                }
                ChaosScenario::WalCorruption => {
                    self.injector.arm_wal_corruption();
                    format!("WalCorruption")
                }
                ChaosScenario::DiskLatency { delay_us, duration_ms } => {
                    self.injector.set_disk_delay(*delay_us);
                    std::thread::sleep(Duration::from_millis(*duration_ms));
                    self.injector.set_disk_delay(0);
                    format!("DiskLatency({}us, {}ms)", delay_us, duration_ms)
                }
            };

            let faults_fired = self.injector.faults_fired();
            let (data_consistent, error) = match consistency_check() {
                Ok(()) => (true, None),
                Err(msg) => (false, Some(msg)),
            };

            results.push(ChaosResult {
                scenario: scenario_name,
                duration_ms: scenario_start.elapsed().as_millis() as u64,
                faults_fired,
                data_consistent,
                error,
            });
        }

        let passed = results.iter().filter(|r| r.data_consistent).count();
        let failed = results.len() - passed;
        StabilityReport {
            total_scenarios: results.len(),
            passed,
            failed,
            all_consistent: failed == 0,
            results,
            total_duration_ms: run_start.elapsed().as_millis() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── ChaosRunner tests ──

    #[test]
    fn test_chaos_runner_all_pass() {
        let injector = new_injector();
        let runner = ChaosRunner::new(injector);
        let scenarios = vec![
            ChaosScenario::KillLeader { duration_ms: 1 },
            ChaosScenario::WalCorruption,
        ];
        let report = runner.run(scenarios, || Ok(()));
        assert_eq!(report.total_scenarios, 2);
        assert_eq!(report.passed, 2);
        assert_eq!(report.failed, 0);
        assert!(report.all_consistent);
        assert!(!report.summary().is_empty());
    }

    #[test]
    fn test_chaos_runner_detects_inconsistency() {
        let injector = new_injector();
        let runner = ChaosRunner::new(injector);
        let scenarios = vec![
            ChaosScenario::KillLeader { duration_ms: 1 },
            ChaosScenario::ReplicaDelay { delay_us: 100, duration_ms: 1 },
        ];
        let mut call_count = 0u32;
        let report = runner.run(scenarios, move || {
            call_count += 1;
            if call_count == 2 {
                Err("replica diverged".into())
            } else {
                Ok(())
            }
        });
        assert_eq!(report.total_scenarios, 2);
        assert_eq!(report.passed, 1);
        assert_eq!(report.failed, 1);
        assert!(!report.all_consistent);
        assert!(report.results[1].error.as_deref() == Some("replica diverged"));
    }

    #[test]
    fn test_chaos_runner_disk_latency() {
        let injector = new_injector();
        let runner = ChaosRunner::new(injector);
        let scenarios = vec![
            ChaosScenario::DiskLatency { delay_us: 100, duration_ms: 1 },
        ];
        let report = runner.run(scenarios, || Ok(()));
        assert!(report.all_consistent);
        assert_eq!(report.total_scenarios, 1);
    }

    #[test]
    fn test_stability_report_summary() {
        let report = StabilityReport {
            total_scenarios: 5,
            passed: 4,
            failed: 1,
            results: vec![],
            all_consistent: false,
            total_duration_ms: 1234,
        };
        let s = report.summary();
        assert!(s.contains("total=5"));
        assert!(s.contains("passed=4"));
        assert!(s.contains("failed=1"));
        assert!(s.contains("consistent=false"));
    }

    #[test]
    fn test_leader_kill_and_revive() {
        let fi = FaultInjector::new();
        assert!(!fi.is_leader_killed());

        fi.kill_leader();
        assert!(fi.is_leader_killed());
        assert_eq!(fi.faults_fired(), 1);

        fi.revive_leader();
        assert!(!fi.is_leader_killed());
    }

    #[test]
    fn test_replica_delay() {
        let fi = FaultInjector::new();
        assert_eq!(fi.replica_delay(), Duration::ZERO);

        fi.set_replica_delay(1000);
        assert_eq!(fi.replica_delay(), Duration::from_micros(1000));

        // Test maybe_delay_replica fires
        let start = std::time::Instant::now();
        assert!(fi.maybe_delay_replica());
        assert!(start.elapsed() >= Duration::from_micros(500)); // some tolerance
        assert_eq!(fi.faults_fired(), 1);
    }

    #[test]
    fn test_wal_corruption_one_shot() {
        let fi = FaultInjector::new();
        assert!(!fi.take_wal_corruption());

        fi.arm_wal_corruption();
        assert!(fi.take_wal_corruption()); // first take consumes it
        assert!(!fi.take_wal_corruption()); // second take returns false
        assert_eq!(fi.faults_fired(), 1);
    }

    #[test]
    fn test_disk_delay() {
        let fi = FaultInjector::new();
        assert!(!fi.maybe_delay_disk()); // no delay configured

        fi.set_disk_delay(500);
        let start = std::time::Instant::now();
        assert!(fi.maybe_delay_disk());
        assert!(start.elapsed() >= Duration::from_micros(250));
    }

    #[test]
    fn test_reset_clears_all() {
        let fi = FaultInjector::new();
        fi.kill_leader();
        fi.set_replica_delay(5000);
        fi.arm_wal_corruption();
        fi.set_disk_delay(1000);

        fi.reset();
        assert!(!fi.is_leader_killed());
        assert_eq!(fi.replica_delay(), Duration::ZERO);
        assert!(!fi.take_wal_corruption());
        assert!(!fi.maybe_delay_disk());
        assert_eq!(fi.faults_fired(), 0);
    }

    #[test]
    fn test_new_injector_arc() {
        let fi = new_injector();
        fi.kill_leader();
        assert!(fi.is_leader_killed());
    }
}
