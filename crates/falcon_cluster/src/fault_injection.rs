//! P1-3 / P1-6: Fault injection helpers for chaos and stability testing.
//!
//! These helpers are test-only utilities that simulate production failure modes:
//! - Leader crash (abrupt stop of primary)
//! - Replica delay (artificial latency on WAL apply)
//! - WAL corruption (flip bytes in a WAL segment)
//! - Disk latency (configurable sleep before I/O)
//! - **Network partition** (split-brain simulation between node groups)
//! - **CPU / IO jitter** (random latency spikes with configurable distribution)
//!
//! All helpers are designed to be composable — a chaos test can combine
//! multiple fault types in a single scenario.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

/// Global fault injection state. Thread-safe, lock-free.
///
/// Wire this into the subsystem under test (e.g. replica runner, WAL writer)
/// via `Arc<FaultInjector>`. When a fault is armed, the subsystem should
/// check the injector before critical operations.
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
    /// Network partition state.
    partition: Mutex<PartitionState>,
    /// CPU/IO jitter configuration.
    jitter: Mutex<JitterConfig>,
    /// Total partition events fired.
    partition_events: AtomicU64,
    /// Total jitter events fired.
    jitter_events: AtomicU64,
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
            partition: Mutex::new(PartitionState::new()),
            jitter: Mutex::new(JitterConfig::disabled()),
            partition_events: AtomicU64::new(0),
            jitter_events: AtomicU64::new(0),
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
        *self.partition.lock() = PartitionState::new();
        *self.jitter.lock() = JitterConfig::disabled();
        self.partition_events.store(0, Ordering::Relaxed);
        self.jitter_events.store(0, Ordering::Relaxed);
    }

    // ── Network partition (split-brain) ──

    /// Create a network partition between two groups of nodes.
    ///
    /// Nodes in `group_a` cannot communicate with nodes in `group_b` and
    /// vice versa. This simulates a network split / split-brain scenario.
    pub fn partition_nodes(&self, group_a: Vec<u64>, group_b: Vec<u64>) {
        let mut state = self.partition.lock();
        state.active = true;
        state.group_a = group_a.into_iter().collect();
        state.group_b = group_b.into_iter().collect();
        state.partition_count += 1;
        self.faults_fired.fetch_add(1, Ordering::Relaxed);
        self.partition_events.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            group_a = ?state.group_a,
            group_b = ?state.group_b,
            "Network partition injected (split-brain)"
        );
    }

    /// Heal the network partition (restore connectivity).
    pub fn heal_partition(&self) {
        let mut state = self.partition.lock();
        if state.active {
            state.active = false;
            state.heal_count += 1;
            tracing::info!("Network partition healed");
        }
    }

    /// Check if a network partition is currently active.
    pub fn is_partitioned(&self) -> bool {
        self.partition.lock().active
    }

    /// Check if two nodes can communicate (not separated by partition).
    ///
    /// Returns `true` if communication is allowed, `false` if blocked.
    pub fn can_communicate(&self, node_a: u64, node_b: u64) -> bool {
        let state = self.partition.lock();
        if !state.active {
            return true;
        }
        // Blocked if one is in group_a and the other in group_b
        let a_in_a = state.group_a.contains(&node_a);
        let a_in_b = state.group_b.contains(&node_a);
        let b_in_a = state.group_a.contains(&node_b);
        let b_in_b = state.group_b.contains(&node_b);

        if (a_in_a && b_in_b) || (a_in_b && b_in_a) {
            self.partition_events.fetch_add(1, Ordering::Relaxed);
            false
        } else {
            true
        }
    }

    /// Get the partition state snapshot.
    pub fn partition_snapshot(&self) -> PartitionSnapshot {
        let state = self.partition.lock();
        PartitionSnapshot {
            active: state.active,
            group_a: state.group_a.iter().copied().collect(),
            group_b: state.group_b.iter().copied().collect(),
            partition_count: state.partition_count,
            heal_count: state.heal_count,
            total_events: self.partition_events.load(Ordering::Relaxed),
        }
    }

    // ── CPU / IO jitter ──

    /// Configure CPU/IO jitter injection.
    ///
    /// When enabled, `maybe_jitter()` will inject random delays drawn from
    /// a uniform distribution in `[base_us, base_us + amplitude_us]`.
    pub fn set_jitter(&self, config: JitterConfig) {
        *self.jitter.lock() = config;
    }

    /// Apply jitter if configured. Returns the actual delay applied (0 if none).
    ///
    /// Call this at I/O or CPU-intensive checkpoints to simulate
    /// real-world latency spikes.
    pub fn maybe_jitter(&self) -> u64 {
        let config = self.jitter.lock().clone();
        if !config.enabled {
            return 0;
        }

        // Simple deterministic jitter using a counter-based approach
        // (avoids requiring `rand` crate dependency)
        let counter = self.jitter_events.fetch_add(1, Ordering::Relaxed);
        let jitter_us = if config.amplitude_us > 0 {
            config.base_us + (counter.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407) % config.amplitude_us)
        } else {
            config.base_us
        };

        if jitter_us > 0 {
            std::thread::sleep(Duration::from_micros(jitter_us));
            self.faults_fired.fetch_add(1, Ordering::Relaxed);
        }
        jitter_us
    }

    /// Check if jitter is currently enabled.
    pub fn is_jitter_enabled(&self) -> bool {
        self.jitter.lock().enabled
    }

    /// Get jitter metrics.
    pub fn jitter_snapshot(&self) -> JitterSnapshot {
        let config = self.jitter.lock().clone();
        JitterSnapshot {
            enabled: config.enabled,
            base_us: config.base_us,
            amplitude_us: config.amplitude_us,
            mode: config.mode,
            total_events: self.jitter_events.load(Ordering::Relaxed),
        }
    }

    /// Combined snapshot of all fault injection state.
    pub fn full_snapshot(&self) -> FaultInjectorSnapshot {
        FaultInjectorSnapshot {
            leader_killed: self.is_leader_killed(),
            replica_delay_us: self.replica_delay_us.load(Ordering::Relaxed),
            wal_corruption_armed: self.wal_corruption_armed.load(Ordering::Relaxed),
            disk_delay_us: self.disk_delay_us.load(Ordering::Relaxed),
            faults_fired: self.faults_fired(),
            partition: self.partition_snapshot(),
            jitter: self.jitter_snapshot(),
        }
    }
}

impl std::fmt::Debug for FaultInjector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultInjector")
            .field("leader_killed", &self.is_leader_killed())
            .field("faults_fired", &self.faults_fired())
            .field("partitioned", &self.is_partitioned())
            .field("jitter_enabled", &self.is_jitter_enabled())
            .finish()
    }
}

/// Convenience constructor for `Arc<FaultInjector>`.
pub fn new_injector() -> Arc<FaultInjector> {
    Arc::new(FaultInjector::new())
}

// ═══════════════════════════════════════════════════════════════════════════
// Network Partition Types
// ═══════════════════════════════════════════════════════════════════════════

/// Internal state for network partition simulation.
struct PartitionState {
    active: bool,
    group_a: HashSet<u64>,
    group_b: HashSet<u64>,
    partition_count: u64,
    heal_count: u64,
}

impl PartitionState {
    fn new() -> Self {
        Self {
            active: false,
            group_a: HashSet::new(),
            group_b: HashSet::new(),
            partition_count: 0,
            heal_count: 0,
        }
    }
}

/// Observable snapshot of partition state.
#[derive(Debug, Clone)]
pub struct PartitionSnapshot {
    pub active: bool,
    pub group_a: Vec<u64>,
    pub group_b: Vec<u64>,
    pub partition_count: u64,
    pub heal_count: u64,
    pub total_events: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// CPU / IO Jitter Types
// ═══════════════════════════════════════════════════════════════════════════

/// Jitter injection mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum JitterMode {
    /// Jitter applied to CPU-bound operations (e.g. query execution).
    Cpu,
    /// Jitter applied to IO-bound operations (e.g. WAL write, disk read).
    Io,
    /// Jitter applied to both CPU and IO operations.
    #[default]
    Both,
}

impl std::fmt::Display for JitterMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JitterMode::Cpu => write!(f, "cpu"),
            JitterMode::Io => write!(f, "io"),
            JitterMode::Both => write!(f, "both"),
        }
    }
}


/// Configuration for CPU/IO jitter injection.
#[derive(Debug, Clone)]
pub struct JitterConfig {
    /// Whether jitter is enabled.
    pub enabled: bool,
    /// Base delay in microseconds (minimum jitter).
    pub base_us: u64,
    /// Amplitude in microseconds (jitter range = [base, base + amplitude]).
    pub amplitude_us: u64,
    /// Which operations to apply jitter to.
    pub mode: JitterMode,
}

impl JitterConfig {
    /// Disabled jitter (no-op).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            base_us: 0,
            amplitude_us: 0,
            mode: JitterMode::Both,
        }
    }

    /// Light jitter: 100-500µs spikes (simulates normal variance).
    pub fn light() -> Self {
        Self {
            enabled: true,
            base_us: 100,
            amplitude_us: 400,
            mode: JitterMode::Both,
        }
    }

    /// Heavy jitter: 1-10ms spikes (simulates GC pauses, IO stalls).
    pub fn heavy() -> Self {
        Self {
            enabled: true,
            base_us: 1_000,
            amplitude_us: 9_000,
            mode: JitterMode::Both,
        }
    }

    /// CPU-only jitter (simulates CPU contention).
    pub fn cpu_only(base_us: u64, amplitude_us: u64) -> Self {
        Self {
            enabled: true,
            base_us,
            amplitude_us,
            mode: JitterMode::Cpu,
        }
    }

    /// IO-only jitter (simulates disk latency spikes).
    pub fn io_only(base_us: u64, amplitude_us: u64) -> Self {
        Self {
            enabled: true,
            base_us,
            amplitude_us,
            mode: JitterMode::Io,
        }
    }
}

/// Observable snapshot of jitter state.
#[derive(Debug, Clone)]
pub struct JitterSnapshot {
    pub enabled: bool,
    pub base_us: u64,
    pub amplitude_us: u64,
    pub mode: JitterMode,
    pub total_events: u64,
}

/// Combined snapshot of all fault injection state.
#[derive(Debug, Clone)]
pub struct FaultInjectorSnapshot {
    pub leader_killed: bool,
    pub replica_delay_us: u64,
    pub wal_corruption_armed: bool,
    pub disk_delay_us: u64,
    pub faults_fired: u64,
    pub partition: PartitionSnapshot,
    pub jitter: JitterSnapshot,
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
    /// Simulate a network partition (split-brain) for the given duration.
    NetworkPartition { group_a: Vec<u64>, group_b: Vec<u64>, duration_ms: u64 },
    /// Inject CPU/IO jitter for the given duration.
    CpuIoJitter { config: JitterConfig, duration_ms: u64 },
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
                    "WalCorruption".to_string()
                }
                ChaosScenario::DiskLatency { delay_us, duration_ms } => {
                    self.injector.set_disk_delay(*delay_us);
                    std::thread::sleep(Duration::from_millis(*duration_ms));
                    self.injector.set_disk_delay(0);
                    format!("DiskLatency({}us, {}ms)", delay_us, duration_ms)
                }
                ChaosScenario::NetworkPartition { group_a, group_b, duration_ms } => {
                    self.injector.partition_nodes(group_a.clone(), group_b.clone());
                    std::thread::sleep(Duration::from_millis(*duration_ms));
                    self.injector.heal_partition();
                    format!("NetworkPartition(a={:?}, b={:?}, {}ms)", group_a, group_b, duration_ms)
                }
                ChaosScenario::CpuIoJitter { config, duration_ms } => {
                    let mode = config.mode;
                    self.injector.set_jitter(config.clone());
                    std::thread::sleep(Duration::from_millis(*duration_ms));
                    self.injector.set_jitter(JitterConfig::disabled());
                    format!("CpuIoJitter({}, {}ms)", mode, duration_ms)
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

    // ── Network partition tests ──

    #[test]
    fn test_partition_basic() {
        let fi = FaultInjector::new();
        assert!(!fi.is_partitioned());

        fi.partition_nodes(vec![1, 2], vec![3, 4]);
        assert!(fi.is_partitioned());

        // Same group can communicate
        assert!(fi.can_communicate(1, 2));
        assert!(fi.can_communicate(3, 4));

        // Cross-group blocked
        assert!(!fi.can_communicate(1, 3));
        assert!(!fi.can_communicate(2, 4));
        assert!(!fi.can_communicate(3, 1));
    }

    #[test]
    fn test_partition_heal() {
        let fi = FaultInjector::new();
        fi.partition_nodes(vec![1], vec![2]);
        assert!(!fi.can_communicate(1, 2));

        fi.heal_partition();
        assert!(!fi.is_partitioned());
        assert!(fi.can_communicate(1, 2));
    }

    #[test]
    fn test_partition_snapshot() {
        let fi = FaultInjector::new();
        fi.partition_nodes(vec![1, 2], vec![3]);
        fi.can_communicate(1, 3); // triggers event

        let snap = fi.partition_snapshot();
        assert!(snap.active);
        assert_eq!(snap.partition_count, 1);
        assert_eq!(snap.heal_count, 0);
        assert!(snap.total_events >= 1);

        fi.heal_partition();
        let snap2 = fi.partition_snapshot();
        assert!(!snap2.active);
        assert_eq!(snap2.heal_count, 1);
    }

    #[test]
    fn test_partition_unknown_nodes_pass() {
        let fi = FaultInjector::new();
        fi.partition_nodes(vec![1], vec![2]);
        // Node 99 is not in either group — should be able to communicate
        assert!(fi.can_communicate(99, 1));
        assert!(fi.can_communicate(99, 2));
    }

    #[test]
    fn test_partition_multiple_cycles() {
        let fi = FaultInjector::new();
        fi.partition_nodes(vec![1], vec![2]);
        fi.heal_partition();
        fi.partition_nodes(vec![3], vec![4]);
        fi.heal_partition();

        let snap = fi.partition_snapshot();
        assert_eq!(snap.partition_count, 2);
        assert_eq!(snap.heal_count, 2);
    }

    #[test]
    fn test_chaos_runner_network_partition() {
        let injector = new_injector();
        let runner = ChaosRunner::new(injector);
        let scenarios = vec![
            ChaosScenario::NetworkPartition {
                group_a: vec![1, 2],
                group_b: vec![3],
                duration_ms: 1,
            },
        ];
        let report = runner.run(scenarios, || Ok(()));
        assert!(report.all_consistent);
        assert!(report.results[0].scenario.contains("NetworkPartition"));
    }

    // ── CPU / IO jitter tests ──

    #[test]
    fn test_jitter_disabled_by_default() {
        let fi = FaultInjector::new();
        assert!(!fi.is_jitter_enabled());
        assert_eq!(fi.maybe_jitter(), 0);
    }

    #[test]
    fn test_jitter_enabled() {
        let fi = FaultInjector::new();
        fi.set_jitter(JitterConfig {
            enabled: true,
            base_us: 100,
            amplitude_us: 0,
            mode: JitterMode::Both,
        });
        assert!(fi.is_jitter_enabled());
        let delay = fi.maybe_jitter();
        assert_eq!(delay, 100);
        assert!(fi.faults_fired() >= 1);
    }

    #[test]
    fn test_jitter_with_amplitude() {
        let fi = FaultInjector::new();
        fi.set_jitter(JitterConfig {
            enabled: true,
            base_us: 100,
            amplitude_us: 200,
            mode: JitterMode::Cpu,
        });
        let delay = fi.maybe_jitter();
        assert!(delay >= 100);
        assert!(delay < 300);
    }

    #[test]
    fn test_jitter_snapshot() {
        let fi = FaultInjector::new();
        fi.set_jitter(JitterConfig::light());
        fi.maybe_jitter();
        fi.maybe_jitter();

        let snap = fi.jitter_snapshot();
        assert!(snap.enabled);
        assert_eq!(snap.base_us, 100);
        assert_eq!(snap.amplitude_us, 400);
        assert_eq!(snap.total_events, 2);
    }

    #[test]
    fn test_jitter_presets() {
        let light = JitterConfig::light();
        assert!(light.enabled);
        assert_eq!(light.base_us, 100);

        let heavy = JitterConfig::heavy();
        assert!(heavy.base_us > light.base_us);

        let cpu = JitterConfig::cpu_only(50, 100);
        assert_eq!(cpu.mode, JitterMode::Cpu);

        let io = JitterConfig::io_only(50, 100);
        assert_eq!(io.mode, JitterMode::Io);

        let disabled = JitterConfig::disabled();
        assert!(!disabled.enabled);
    }

    #[test]
    fn test_jitter_mode_display() {
        assert_eq!(JitterMode::Cpu.to_string(), "cpu");
        assert_eq!(JitterMode::Io.to_string(), "io");
        assert_eq!(JitterMode::Both.to_string(), "both");
    }

    #[test]
    fn test_chaos_runner_jitter() {
        let injector = new_injector();
        let runner = ChaosRunner::new(injector);
        let scenarios = vec![
            ChaosScenario::CpuIoJitter {
                config: JitterConfig::light(),
                duration_ms: 1,
            },
        ];
        let report = runner.run(scenarios, || Ok(()));
        assert!(report.all_consistent);
        assert!(report.results[0].scenario.contains("CpuIoJitter"));
    }

    // ── Full snapshot test ──

    #[test]
    fn test_full_snapshot() {
        let fi = FaultInjector::new();
        fi.kill_leader();
        fi.partition_nodes(vec![1], vec![2]);
        fi.set_jitter(JitterConfig::light());

        let snap = fi.full_snapshot();
        assert!(snap.leader_killed);
        assert!(snap.partition.active);
        assert!(snap.jitter.enabled);
        assert!(snap.faults_fired >= 2);
    }

    #[test]
    fn test_reset_clears_partition_and_jitter() {
        let fi = FaultInjector::new();
        fi.partition_nodes(vec![1], vec![2]);
        fi.set_jitter(JitterConfig::heavy());

        fi.reset();
        assert!(!fi.is_partitioned());
        assert!(!fi.is_jitter_enabled());
        assert_eq!(fi.partition_snapshot().partition_count, 0);
        assert_eq!(fi.jitter_snapshot().total_events, 0);
    }
}
