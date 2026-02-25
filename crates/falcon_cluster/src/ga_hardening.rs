//! v1.1.1 §1–5: GA Hardening — Crash/Restart, Config Rollback,
//! Resource Leak Detection, Tail Latency Guardrails, Background Task Isolation.
//!
//! **One-line**: All unpredictable behavior must be eliminated.
//! The system must behave the same at 3 a.m. on day 300 as it did on day 1.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;

use parking_lot::{Mutex, RwLock};

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Crash / Restart Hardening
// ═══════════════════════════════════════════════════════════════════════════

/// Component type that participates in the startup/shutdown protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComponentType {
    DataNode,
    Gateway,
    Controller,
    Compactor,
    ReplicationWorker,
    BackupWorker,
    AuditDrain,
    SnapshotWorker,
}

impl fmt::Display for ComponentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DataNode => write!(f, "DATA_NODE"),
            Self::Gateway => write!(f, "GATEWAY"),
            Self::Controller => write!(f, "CONTROLLER"),
            Self::Compactor => write!(f, "COMPACTOR"),
            Self::ReplicationWorker => write!(f, "REPLICATION"),
            Self::BackupWorker => write!(f, "BACKUP"),
            Self::AuditDrain => write!(f, "AUDIT_DRAIN"),
            Self::SnapshotWorker => write!(f, "SNAPSHOT"),
        }
    }
}

/// Component lifecycle state — explicit, no "unknown" allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentState {
    /// Not yet started.
    Init,
    /// Starting: loading state, replaying WAL, etc.
    Starting,
    /// Fully operational.
    Running,
    /// Graceful shutdown in progress.
    ShuttingDown,
    /// Stopped cleanly.
    Stopped,
    /// Crashed or failed — requires investigation.
    Failed,
}

impl fmt::Display for ComponentState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init => write!(f, "INIT"),
            Self::Starting => write!(f, "STARTING"),
            Self::Running => write!(f, "RUNNING"),
            Self::ShuttingDown => write!(f, "SHUTTING_DOWN"),
            Self::Stopped => write!(f, "STOPPED"),
            Self::Failed => write!(f, "FAILED"),
        }
    }
}

/// Recovery action taken on restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    WalReplay { records_replayed: u64, from_lsn: u64, to_lsn: u64 },
    ColdSegmentRebuild { segments_rebuilt: u64 },
    ReplicationResume { from_lsn: u64 },
    CheckpointRestore { checkpoint_lsn: u64 },
    InDoubtTxnResolution { committed: u64, aborted: u64 },
    IndexRebuild { indexes_rebuilt: u64 },
    None,
}

impl fmt::Display for RecoveryAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WalReplay { records_replayed, from_lsn, to_lsn } =>
                write!(f, "WAL_REPLAY(records={records_replayed}, lsn={from_lsn}..{to_lsn})"),
            Self::ColdSegmentRebuild { segments_rebuilt } =>
                write!(f, "COLD_REBUILD(segments={segments_rebuilt})"),
            Self::ReplicationResume { from_lsn } =>
                write!(f, "REPL_RESUME(from_lsn={from_lsn})"),
            Self::CheckpointRestore { checkpoint_lsn } =>
                write!(f, "CHECKPOINT(lsn={checkpoint_lsn})"),
            Self::InDoubtTxnResolution { committed, aborted } =>
                write!(f, "INDOUBT(committed={committed}, aborted={aborted})"),
            Self::IndexRebuild { indexes_rebuilt } =>
                write!(f, "INDEX_REBUILD(count={indexes_rebuilt})"),
            Self::None => write!(f, "NONE"),
        }
    }
}

/// Record of a component's startup/recovery.
#[derive(Debug, Clone)]
pub struct StartupRecord {
    pub component: ComponentType,
    pub started_at: u64,
    pub ready_at: Option<u64>,
    pub recovery_actions: Vec<RecoveryAction>,
    pub state: ComponentState,
    pub previous_shutdown: ShutdownType,
    pub duration_ms: Option<u64>,
}

/// How the previous instance terminated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownType {
    Clean,
    Crash,
    Unknown,
}

impl fmt::Display for ShutdownType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clean => write!(f, "CLEAN"),
            Self::Crash => write!(f, "CRASH"),
            Self::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// Startup/shutdown order specification.
#[derive(Debug, Clone)]
pub struct LifecycleOrder {
    pub startup_order: Vec<ComponentType>,
    pub shutdown_order: Vec<ComponentType>,
}

impl Default for LifecycleOrder {
    fn default() -> Self {
        Self {
            startup_order: vec![
                ComponentType::Controller,
                ComponentType::DataNode,
                ComponentType::ReplicationWorker,
                ComponentType::Gateway,
                ComponentType::Compactor,
                ComponentType::SnapshotWorker,
                ComponentType::BackupWorker,
                ComponentType::AuditDrain,
            ],
            shutdown_order: vec![
                ComponentType::AuditDrain,
                ComponentType::BackupWorker,
                ComponentType::SnapshotWorker,
                ComponentType::Compactor,
                ComponentType::Gateway,
                ComponentType::ReplicationWorker,
                ComponentType::DataNode,
                ComponentType::Controller,
            ],
        }
    }
}

/// Crash/restart hardening coordinator.
/// Tracks every component's lifecycle and ensures deterministic startup/shutdown.
pub struct CrashHardeningCoordinator {
    lifecycle_order: LifecycleOrder,
    components: RwLock<HashMap<ComponentType, ComponentState>>,
    startup_log: Mutex<Vec<StartupRecord>>,
    shutdown_sentinel_path: Option<String>,
    pub metrics: CrashHardeningMetrics,
}

#[derive(Debug, Default)]
pub struct CrashHardeningMetrics {
    pub clean_shutdowns: AtomicU64,
    pub crash_recoveries: AtomicU64,
    pub wal_replays: AtomicU64,
    pub cold_rebuilds: AtomicU64,
    pub indoubt_resolutions: AtomicU64,
    pub startup_total_ms: AtomicU64,
}

impl CrashHardeningCoordinator {
    pub fn new(lifecycle_order: LifecycleOrder, sentinel_path: Option<String>) -> Self {
        Self {
            lifecycle_order,
            components: RwLock::new(HashMap::new()),
            startup_log: Mutex::new(Vec::new()),
            shutdown_sentinel_path: sentinel_path,
            metrics: CrashHardeningMetrics::default(),
        }
    }

    /// Detect how the previous instance terminated.
    pub fn detect_previous_shutdown(&self) -> ShutdownType {
        self.shutdown_sentinel_path.as_ref().map_or(ShutdownType::Unknown, |path| {
            if std::path::Path::new(path).exists() {
                ShutdownType::Clean
            } else {
                ShutdownType::Crash
            }
        })
    }

    /// Write the shutdown sentinel (called during graceful shutdown).
    pub fn write_shutdown_sentinel(&self) -> bool {
        self.shutdown_sentinel_path.as_ref().is_some_and(|path| {
            std::fs::write(path, b"CLEAN_SHUTDOWN").is_ok()
        })
    }

    /// Remove the sentinel on startup (so crash on next run is detectable).
    pub fn clear_shutdown_sentinel(&self) -> bool {
        self.shutdown_sentinel_path.as_ref().is_none_or(|path| {
            if std::path::Path::new(path).exists() {
                std::fs::remove_file(path).is_ok()
            } else {
                true
            }
        })
    }

    /// Register a component in INIT state.
    pub fn register(&self, component: ComponentType) {
        self.components.write().insert(component, ComponentState::Init);
    }

    /// Transition a component state.
    pub fn transition(&self, component: ComponentType, new_state: ComponentState) {
        self.components.write().insert(component, new_state);
    }

    /// Record a startup completion.
    pub fn record_startup(
        &self,
        component: ComponentType,
        recovery_actions: Vec<RecoveryAction>,
        previous_shutdown: ShutdownType,
        duration_ms: u64,
    ) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let record = StartupRecord {
            component,
            started_at: now.saturating_sub(duration_ms / 1000),
            ready_at: Some(now),
            recovery_actions: recovery_actions.clone(),
            state: ComponentState::Running,
            previous_shutdown,
            duration_ms: Some(duration_ms),
        };
        for action in &recovery_actions {
            match action {
                RecoveryAction::WalReplay { .. } => {
                    self.metrics.wal_replays.fetch_add(1, Ordering::Relaxed);
                }
                RecoveryAction::ColdSegmentRebuild { .. } => {
                    self.metrics.cold_rebuilds.fetch_add(1, Ordering::Relaxed);
                }
                RecoveryAction::InDoubtTxnResolution { .. } => {
                    self.metrics.indoubt_resolutions.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
        if previous_shutdown == ShutdownType::Crash {
            self.metrics.crash_recoveries.fetch_add(1, Ordering::Relaxed);
        }
        self.metrics.startup_total_ms.fetch_add(duration_ms, Ordering::Relaxed);
        self.transition(component, ComponentState::Running);
        self.startup_log.lock().push(record);
    }

    /// Get the startup order.
    pub fn startup_order(&self) -> &[ComponentType] {
        &self.lifecycle_order.startup_order
    }

    /// Get the shutdown order.
    pub fn shutdown_order(&self) -> &[ComponentType] {
        &self.lifecycle_order.shutdown_order
    }

    /// Check if all components are in Running state.
    pub fn all_running(&self) -> bool {
        let comps = self.components.read();
        !comps.is_empty() && comps.values().all(|s| *s == ComponentState::Running)
    }

    /// Check if any component is in Failed state.
    pub fn any_failed(&self) -> bool {
        self.components.read().values().any(|s| *s == ComponentState::Failed)
    }

    /// Get component states.
    pub fn component_states(&self) -> HashMap<ComponentType, ComponentState> {
        self.components.read().clone()
    }

    /// Get startup log.
    pub fn startup_log(&self) -> Vec<StartupRecord> {
        self.startup_log.lock().clone()
    }

    /// Execute graceful shutdown sequence.
    pub fn begin_shutdown(&self) {
        for component in &self.lifecycle_order.shutdown_order {
            self.transition(*component, ComponentState::ShuttingDown);
        }
    }

    /// Mark shutdown complete.
    pub fn complete_shutdown(&self) {
        for component in &self.lifecycle_order.shutdown_order {
            self.transition(*component, ComponentState::Stopped);
        }
        self.write_shutdown_sentinel();
        self.metrics.clean_shutdowns.fetch_add(1, Ordering::Relaxed);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Config Change Consistency & Rollback
// ═══════════════════════════════════════════════════════════════════════════

/// A versioned, checksummed configuration entry.
#[derive(Debug, Clone)]
pub struct VersionedConfig {
    pub version: u64,
    pub key: String,
    pub value: String,
    pub checksum: u64,
    pub applied_at: u64,
    pub applied_by: String,
    pub rollout_state: RolloutState,
}

/// Staged rollout state for a config change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RolloutState {
    /// Proposed but not yet applied.
    Staged,
    /// Applied to canary nodes.
    Canary,
    /// Rolling out to all nodes.
    RollingOut,
    /// Fully applied.
    Applied,
    /// Rolled back.
    RolledBack,
}

impl fmt::Display for RolloutState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Staged => write!(f, "STAGED"),
            Self::Canary => write!(f, "CANARY"),
            Self::RollingOut => write!(f, "ROLLING_OUT"),
            Self::Applied => write!(f, "APPLIED"),
            Self::RolledBack => write!(f, "ROLLED_BACK"),
        }
    }
}

/// Config rollback manager — ensures configs are versioned, checksummed,
/// and can be rolled back within SLA windows.
pub struct ConfigRollbackManager {
    /// Current config values (key → latest VersionedConfig).
    current: RwLock<HashMap<String, VersionedConfig>>,
    /// History: key → Vec<VersionedConfig> (newest last).
    history: RwLock<HashMap<String, Vec<VersionedConfig>>>,
    next_version: AtomicU64,
    max_history: usize,
    pub metrics: ConfigRollbackMetrics,
}

#[derive(Debug, Default)]
pub struct ConfigRollbackMetrics {
    pub changes_applied: AtomicU64,
    pub rollbacks_executed: AtomicU64,
    pub checksum_mismatches: AtomicU64,
    pub staged_count: AtomicU64,
}

impl ConfigRollbackManager {
    pub fn new(max_history: usize) -> Self {
        Self {
            current: RwLock::new(HashMap::new()),
            history: RwLock::new(HashMap::new()),
            next_version: AtomicU64::new(1),
            max_history,
            metrics: ConfigRollbackMetrics::default(),
        }
    }

    /// Compute a simple checksum for a config value.
    fn compute_checksum(key: &str, value: &str) -> u64 {
        let mut h: u64 = 5381;
        for b in key.bytes().chain(value.bytes()) {
            h = h.wrapping_mul(33).wrapping_add(u64::from(b));
        }
        h
    }

    /// Stage a config change (does not apply yet).
    pub fn stage_change(&self, key: &str, value: &str, applied_by: &str) -> u64 {
        let version = self.next_version.fetch_add(1, Ordering::Relaxed);
        let checksum = Self::compute_checksum(key, value);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entry = VersionedConfig {
            version,
            key: key.to_owned(),
            value: value.to_owned(),
            checksum,
            applied_at: now,
            applied_by: applied_by.to_owned(),
            rollout_state: RolloutState::Staged,
        };
        let mut hist = self.history.write();
        let h = hist.entry(key.to_owned()).or_default();
        if h.len() >= self.max_history {
            h.remove(0);
        }
        h.push(entry);
        self.metrics.staged_count.fetch_add(1, Ordering::Relaxed);
        version
    }

    /// Apply a staged change (transition from Staged to Applied).
    pub fn apply_change(&self, key: &str, version: u64) -> bool {
        let mut hist = self.history.write();
        if let Some(entries) = hist.get_mut(key) {
            if let Some(entry) = entries.iter_mut().find(|e| e.version == version) {
                if entry.rollout_state != RolloutState::Staged
                    && entry.rollout_state != RolloutState::Canary
                    && entry.rollout_state != RolloutState::RollingOut
                {
                    return false;
                }
                entry.rollout_state = RolloutState::Applied;
                self.current.write().insert(key.to_owned(), entry.clone());
                self.metrics.changes_applied.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Direct set (stage + apply in one step).
    pub fn set(&self, key: &str, value: &str, applied_by: &str) -> u64 {
        let version = self.stage_change(key, value, applied_by);
        self.apply_change(key, version);
        version
    }

    /// Rollback a key to a specific version.
    pub fn rollback_to_version(&self, key: &str, target_version: u64) -> bool {
        let hist = self.history.read();
        if let Some(entries) = hist.get(key) {
            if let Some(target) = entries.iter().find(|e| e.version == target_version) {
                let mut restored = target.clone();
                restored.rollout_state = RolloutState::RolledBack;
                drop(hist);
                self.current.write().insert(key.to_owned(), restored);
                self.metrics.rollbacks_executed.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Rollback a key to the previous version.
    pub fn rollback(&self, key: &str) -> bool {
        let hist = self.history.read();
        if let Some(entries) = hist.get(key) {
            if entries.len() >= 2 {
                let prev = entries[entries.len() - 2].version;
                drop(hist);
                return self.rollback_to_version(key, prev);
            }
        }
        false
    }

    /// Get current value for a key.
    pub fn get(&self, key: &str) -> Option<VersionedConfig> {
        self.current.read().get(key).cloned()
    }

    /// Verify checksum of a config entry.
    pub fn verify_checksum(&self, key: &str) -> bool {
        if let Some(entry) = self.current.read().get(key) {
            let expected = Self::compute_checksum(&entry.key, &entry.value);
            if expected != entry.checksum {
                self.metrics.checksum_mismatches.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            true
        } else {
            true // missing key is not a mismatch
        }
    }

    /// Get history for a key.
    pub fn key_history(&self, key: &str) -> Vec<VersionedConfig> {
        self.history.read().get(key).cloned().unwrap_or_default()
    }

    /// Get all current config keys.
    pub fn all_keys(&self) -> Vec<String> {
        self.current.read().keys().cloned().collect()
    }

    /// Advance rollout state for staged rollout.
    pub fn advance_rollout(&self, key: &str, version: u64, new_state: RolloutState) -> bool {
        let mut hist = self.history.write();
        if let Some(entries) = hist.get_mut(key) {
            if let Some(entry) = entries.iter_mut().find(|e| e.version == version) {
                entry.rollout_state = new_state;
                if new_state == RolloutState::Applied {
                    self.current.write().insert(key.to_owned(), entry.clone());
                    self.metrics.changes_applied.fetch_add(1, Ordering::Relaxed);
                }
                return true;
            }
        }
        false
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Resource Leak Zero-Tolerance
// ═══════════════════════════════════════════════════════════════════════════

/// Resource type being tracked for leaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LeakResourceType {
    FileDescriptor,
    TcpConnection,
    Thread,
    HotMemoryBytes,
    ColdMemoryBytes,
    CacheBytes,
}

impl fmt::Display for LeakResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FileDescriptor => write!(f, "FD"),
            Self::TcpConnection => write!(f, "TCP"),
            Self::Thread => write!(f, "THREAD"),
            Self::HotMemoryBytes => write!(f, "HOT_MEM"),
            Self::ColdMemoryBytes => write!(f, "COLD_MEM"),
            Self::CacheBytes => write!(f, "CACHE"),
        }
    }
}

/// A snapshot of resource usage at a point in time.
#[derive(Debug, Clone)]
pub struct ResourceSnapshot {
    pub timestamp: u64,
    pub values: HashMap<LeakResourceType, i64>,
}

/// Leak detection result.
#[derive(Debug, Clone)]
pub struct LeakDetectionResult {
    pub resource: LeakResourceType,
    pub is_leaking: bool,
    pub growth_rate_per_hour: f64,
    pub current_value: i64,
    pub baseline_value: i64,
    pub samples: usize,
    pub confidence: f64,
}

/// Resource leak detector — samples resource counts periodically
/// and flags monotonic growth.
pub struct ResourceLeakDetector {
    snapshots: Mutex<VecDeque<ResourceSnapshot>>,
    max_snapshots: usize,
    /// Threshold: growth rate per hour that triggers a leak alarm.
    alarm_thresholds: RwLock<HashMap<LeakResourceType, f64>>,
    pub metrics: LeakDetectorMetrics,
}

#[derive(Debug, Default)]
pub struct LeakDetectorMetrics {
    pub samples_taken: AtomicU64,
    pub leaks_detected: AtomicU64,
    pub false_positives_cleared: AtomicU64,
}

impl ResourceLeakDetector {
    pub fn new(max_snapshots: usize) -> Self {
        Self {
            snapshots: Mutex::new(VecDeque::with_capacity(max_snapshots)),
            max_snapshots,
            alarm_thresholds: RwLock::new(HashMap::new()),
            metrics: LeakDetectorMetrics::default(),
        }
    }

    /// Set alarm threshold for a resource type (growth per hour).
    pub fn set_threshold(&self, resource: LeakResourceType, growth_per_hour: f64) {
        self.alarm_thresholds.write().insert(resource, growth_per_hour);
    }

    /// Record a resource snapshot.
    pub fn record_snapshot(&self, values: HashMap<LeakResourceType, i64>) {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let snapshot = ResourceSnapshot { timestamp: ts, values };
        let mut snaps = self.snapshots.lock();
        if snaps.len() >= self.max_snapshots {
            snaps.pop_front();
        }
        snaps.push_back(snapshot);
        self.metrics.samples_taken.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a snapshot with explicit timestamp.
    pub fn record_snapshot_at(&self, values: HashMap<LeakResourceType, i64>, timestamp: u64) {
        let snapshot = ResourceSnapshot { timestamp, values };
        let mut snaps = self.snapshots.lock();
        if snaps.len() >= self.max_snapshots {
            snaps.pop_front();
        }
        snaps.push_back(snapshot);
        self.metrics.samples_taken.fetch_add(1, Ordering::Relaxed);
    }

    /// Analyze a specific resource for leaks using linear regression.
    pub fn analyze(&self, resource: LeakResourceType) -> LeakDetectionResult {
        let snaps = self.snapshots.lock();
        let mut points: Vec<(f64, f64)> = Vec::new();
        let first_ts = snaps.front().map_or(0, |s| s.timestamp) as f64;

        for snap in snaps.iter() {
            if let Some(&val) = snap.values.get(&resource) {
                let x = (snap.timestamp as f64 - first_ts) / 3600.0; // hours
                points.push((x, val as f64));
            }
        }

        if points.len() < 2 {
            return LeakDetectionResult {
                resource,
                is_leaking: false,
                growth_rate_per_hour: 0.0,
                current_value: points.last().map_or(0, |p| p.1 as i64),
                baseline_value: points.first().map_or(0, |p| p.1 as i64),
                samples: points.len(),
                confidence: 0.0,
            };
        }

        let n = points.len() as f64;
        let (mut sx, mut sy, mut sxy, mut sxx) = (0.0, 0.0, 0.0, 0.0);
        for &(x, y) in &points {
            sx += x; sy += y; sxy += x * y; sxx += x * x;
        }
        let denom = n.mul_add(sxx, -(sx * sx));
        let slope = if denom.abs() < 1e-10 { 0.0 } else { n.mul_add(sxy, -(sx * sy)) / denom };

        // R² for confidence
        let y_mean = sy / n;
        let ss_tot: f64 = points.iter().map(|(_, y)| (y - y_mean).powi(2)).sum();
        let intercept = (sy - slope * sx) / n;
        let ss_res: f64 = points.iter().map(|(x, y)| {
            let predicted = intercept + slope * x;
            (y - predicted).powi(2)
        }).sum();
        let r_squared = if ss_tot > 0.0 { 1.0 - ss_res / ss_tot } else { 0.0 };

        let threshold = self.alarm_thresholds.read().get(&resource).copied().unwrap_or(1.0);
        let is_leaking = slope > threshold && r_squared > 0.7;

        if is_leaking {
            self.metrics.leaks_detected.fetch_add(1, Ordering::Relaxed);
        }

        LeakDetectionResult {
            resource,
            is_leaking,
            growth_rate_per_hour: slope,
            current_value: points.last().map_or(0, |p| p.1 as i64),
            baseline_value: points.first().map_or(0, |p| p.1 as i64),
            samples: points.len(),
            confidence: r_squared,
        }
    }

    /// Analyze all tracked resources.
    pub fn analyze_all(&self) -> Vec<LeakDetectionResult> {
        let resources: Vec<LeakResourceType> = {
            let snaps = self.snapshots.lock();
            let mut set = std::collections::HashSet::new();
            for snap in snaps.iter() {
                for key in snap.values.keys() {
                    set.insert(*key);
                }
            }
            set.into_iter().collect()
        };
        resources.iter().map(|r| self.analyze(*r)).collect()
    }

    /// Get the latest snapshot.
    pub fn latest_snapshot(&self) -> Option<ResourceSnapshot> {
        self.snapshots.lock().back().cloned()
    }

    /// Get snapshot count.
    pub fn snapshot_count(&self) -> usize {
        self.snapshots.lock().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Tail Latency Guardrail v2
// ═══════════════════════════════════════════════════════════════════════════

/// Hot path being guarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GuardedPath {
    WalCommit,
    GatewayForward,
    ColdDecompress,
    ReplicationApply,
    IndexLookup,
    TxnCommit,
}

impl fmt::Display for GuardedPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WalCommit => write!(f, "WAL_COMMIT"),
            Self::GatewayForward => write!(f, "GW_FORWARD"),
            Self::ColdDecompress => write!(f, "COLD_DECOMPRESS"),
            Self::ReplicationApply => write!(f, "REPL_APPLY"),
            Self::IndexLookup => write!(f, "INDEX_LOOKUP"),
            Self::TxnCommit => write!(f, "TXN_COMMIT"),
        }
    }
}

/// Guardrail configuration for a path.
#[derive(Debug, Clone)]
pub struct PathGuardrail {
    pub path: GuardedPath,
    /// p99 threshold in microseconds.
    pub p99_threshold_us: u64,
    /// p999 threshold in microseconds.
    pub p999_threshold_us: u64,
    /// Absolute max: any single op exceeding this triggers immediate event.
    pub absolute_max_us: u64,
    /// Whether to trigger backpressure on breach.
    pub trigger_backpressure: bool,
}

/// Guardrail breach event.
#[derive(Debug, Clone)]
pub struct GuardrailBreach {
    pub id: u64,
    pub path: GuardedPath,
    pub breach_type: BreachType,
    pub observed_us: u64,
    pub threshold_us: u64,
    pub timestamp: u64,
    pub backpressure_triggered: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreachType {
    P99Exceeded,
    P999Exceeded,
    AbsoluteMaxExceeded,
}

impl fmt::Display for BreachType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::P99Exceeded => write!(f, "P99_EXCEEDED"),
            Self::P999Exceeded => write!(f, "P999_EXCEEDED"),
            Self::AbsoluteMaxExceeded => write!(f, "ABS_MAX_EXCEEDED"),
        }
    }
}

/// Tail latency guardrail engine.
pub struct LatencyGuardrailEngine {
    guardrails: RwLock<HashMap<GuardedPath, PathGuardrail>>,
    /// Rolling window of latency samples per path.
    samples: RwLock<HashMap<GuardedPath, VecDeque<u64>>>,
    max_samples: usize,
    breaches: Mutex<VecDeque<GuardrailBreach>>,
    next_breach_id: AtomicU64,
    max_breaches: usize,
    backpressure_active: AtomicBool,
    pub metrics: GuardrailMetrics,
}

#[derive(Debug, Default)]
pub struct GuardrailMetrics {
    pub samples_recorded: AtomicU64,
    pub p99_breaches: AtomicU64,
    pub p999_breaches: AtomicU64,
    pub absolute_breaches: AtomicU64,
    pub backpressure_activations: AtomicU64,
}

impl LatencyGuardrailEngine {
    pub fn new(max_samples: usize, max_breaches: usize) -> Self {
        Self {
            guardrails: RwLock::new(HashMap::new()),
            samples: RwLock::new(HashMap::new()),
            max_samples,
            breaches: Mutex::new(VecDeque::with_capacity(max_breaches)),
            next_breach_id: AtomicU64::new(1),
            max_breaches,
            backpressure_active: AtomicBool::new(false),
            metrics: GuardrailMetrics::default(),
        }
    }

    /// Define a guardrail for a path.
    pub fn define_guardrail(&self, guardrail: PathGuardrail) {
        self.guardrails.write().insert(guardrail.path, guardrail);
    }

    /// Record a latency sample and check guardrails. Returns breach if any.
    pub fn record(&self, path: GuardedPath, latency_us: u64) -> Option<GuardrailBreach> {
        self.metrics.samples_recorded.fetch_add(1, Ordering::Relaxed);

        // Store sample
        {
            let mut samples = self.samples.write();
            let q = samples.entry(path).or_insert_with(|| VecDeque::with_capacity(self.max_samples));
            if q.len() >= self.max_samples {
                q.pop_front();
            }
            q.push_back(latency_us);
        }

        // Check absolute max first
        let guardrails = self.guardrails.read();
        let guardrail = guardrails.get(&path)?;

        if latency_us > guardrail.absolute_max_us {
            self.metrics.absolute_breaches.fetch_add(1, Ordering::Relaxed);
            let breach = self.create_breach(path, BreachType::AbsoluteMaxExceeded, latency_us, guardrail.absolute_max_us, guardrail.trigger_backpressure);
            return Some(breach);
        }

        // Check percentiles (only periodically, every 100 samples)
        let sample_count = self.metrics.samples_recorded.load(Ordering::Relaxed);
        if sample_count.is_multiple_of(100) {
            let samples = self.samples.read();
            if let Some(q) = samples.get(&path) {
                if q.len() >= 10 {
                    let mut sorted: Vec<u64> = q.iter().copied().collect();
                    sorted.sort_unstable();
                    let p99_idx = ((sorted.len() as f64 * 0.99).ceil() as usize).min(sorted.len()) - 1;
                    let p999_idx = ((sorted.len() as f64 * 0.999).ceil() as usize).min(sorted.len()) - 1;
                    let p99 = sorted[p99_idx];
                    let p999 = sorted[p999_idx];

                    if p999 > guardrail.p999_threshold_us {
                        self.metrics.p999_breaches.fetch_add(1, Ordering::Relaxed);
                        let breach = self.create_breach(path, BreachType::P999Exceeded, p999, guardrail.p999_threshold_us, guardrail.trigger_backpressure);
                        return Some(breach);
                    }
                    if p99 > guardrail.p99_threshold_us {
                        self.metrics.p99_breaches.fetch_add(1, Ordering::Relaxed);
                        let breach = self.create_breach(path, BreachType::P99Exceeded, p99, guardrail.p99_threshold_us, guardrail.trigger_backpressure);
                        return Some(breach);
                    }
                }
            }
        }
        None
    }

    fn create_breach(
        &self,
        path: GuardedPath,
        breach_type: BreachType,
        observed: u64,
        threshold: u64,
        trigger_bp: bool,
    ) -> GuardrailBreach {
        let id = self.next_breach_id.fetch_add(1, Ordering::Relaxed);
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if trigger_bp {
            self.backpressure_active.store(true, Ordering::Relaxed);
            self.metrics.backpressure_activations.fetch_add(1, Ordering::Relaxed);
        }
        let breach = GuardrailBreach {
            id, path, breach_type, observed_us: observed, threshold_us: threshold,
            timestamp: ts, backpressure_triggered: trigger_bp,
        };
        let mut breaches = self.breaches.lock();
        if breaches.len() >= self.max_breaches { breaches.pop_front(); }
        breaches.push_back(breach.clone());
        breach
    }

    /// Check if backpressure is active.
    pub fn is_backpressure_active(&self) -> bool {
        self.backpressure_active.load(Ordering::Relaxed)
    }

    /// Clear backpressure.
    pub fn clear_backpressure(&self) {
        self.backpressure_active.store(false, Ordering::Relaxed);
    }

    /// Get recent breaches.
    pub fn recent_breaches(&self, count: usize) -> Vec<GuardrailBreach> {
        self.breaches.lock().iter().rev().take(count).cloned().collect()
    }

    /// Get percentiles for a path.
    pub fn percentiles(&self, path: GuardedPath) -> Option<(u64, u64, u64)> {
        let samples = self.samples.read();
        let q = samples.get(&path)?;
        if q.len() < 2 { return None; }
        let mut sorted: Vec<u64> = q.iter().copied().collect();
        sorted.sort_unstable();
        let p50 = sorted[sorted.len() / 2];
        let p99 = sorted[((sorted.len() as f64 * 0.99).ceil() as usize).min(sorted.len()) - 1];
        let p999 = sorted[((sorted.len() as f64 * 0.999).ceil() as usize).min(sorted.len()) - 1];
        Some((p50, p99, p999))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Background Task Resource Isolation
// ═══════════════════════════════════════════════════════════════════════════

/// Background task type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BgTaskType {
    Compaction,
    ColdMigration,
    Snapshot,
    Rebalance,
    Backup,
    IndexBuild,
    GarbageCollection,
}

impl fmt::Display for BgTaskType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compaction => write!(f, "COMPACTION"),
            Self::ColdMigration => write!(f, "COLD_MIGRATION"),
            Self::Snapshot => write!(f, "SNAPSHOT"),
            Self::Rebalance => write!(f, "REBALANCE"),
            Self::Backup => write!(f, "BACKUP"),
            Self::IndexBuild => write!(f, "INDEX_BUILD"),
            Self::GarbageCollection => write!(f, "GC"),
        }
    }
}

/// Resource quota for a background task.
#[derive(Debug, Clone)]
pub struct BgTaskQuota {
    pub task_type: BgTaskType,
    /// Max CPU percentage (0.0–1.0).
    pub cpu_quota: f64,
    /// Max IO bytes per second.
    pub io_bytes_per_sec: u64,
    /// Max memory bytes.
    pub memory_bytes: u64,
    /// Max concurrent operations.
    pub max_concurrent: u32,
    /// Whether to dynamically reduce under foreground load.
    pub dynamic_throttle: bool,
}

/// Current resource usage for a background task.
#[derive(Debug, Clone, Default)]
pub struct BgTaskUsage {
    pub cpu_fraction: f64,
    pub io_bytes_used: u64,
    pub memory_bytes_used: u64,
    pub active_ops: u32,
}

/// Throttle decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThrottleDecision {
    /// Proceed at full speed.
    Allow,
    /// Slow down: sleep for the given duration.
    Throttle(u64), // sleep_ms
    /// Reject: quota exceeded.
    Reject,
}

impl fmt::Display for ThrottleDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow => write!(f, "ALLOW"),
            Self::Throttle(ms) => write!(f, "THROTTLE({ms}ms)"),
            Self::Reject => write!(f, "REJECT"),
        }
    }
}

/// Background task resource isolator.
pub struct BgTaskIsolator {
    quotas: RwLock<HashMap<BgTaskType, BgTaskQuota>>,
    usage: RwLock<HashMap<BgTaskType, BgTaskUsage>>,
    /// Current foreground load (0.0 = idle, 1.0 = saturated).
    foreground_load: std::sync::atomic::AtomicU64, // stored as f64 bits
    pub metrics: BgIsolatorMetrics,
}

#[derive(Debug, Default)]
pub struct BgIsolatorMetrics {
    pub requests_allowed: AtomicU64,
    pub requests_throttled: AtomicU64,
    pub requests_rejected: AtomicU64,
    pub dynamic_reductions: AtomicU64,
}

impl Default for BgTaskIsolator {
    fn default() -> Self {
        Self::new()
    }
}

impl BgTaskIsolator {
    pub fn new() -> Self {
        Self {
            quotas: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
            foreground_load: AtomicU64::new(0),
            metrics: BgIsolatorMetrics::default(),
        }
    }

    /// Define quota for a task type.
    pub fn set_quota(&self, quota: BgTaskQuota) {
        self.quotas.write().insert(quota.task_type, quota);
    }

    /// Update foreground load factor (0.0–1.0).
    pub fn set_foreground_load(&self, load: f64) {
        self.foreground_load.store(load.to_bits(), Ordering::Relaxed);
    }

    /// Get foreground load factor.
    pub fn foreground_load(&self) -> f64 {
        f64::from_bits(self.foreground_load.load(Ordering::Relaxed))
    }

    /// Report current usage for a task type.
    pub fn report_usage(&self, task_type: BgTaskType, usage: BgTaskUsage) {
        self.usage.write().insert(task_type, usage);
    }

    /// Request permission to perform an operation.
    pub fn request(&self, task_type: BgTaskType, io_bytes: u64) -> ThrottleDecision {
        let quotas = self.quotas.read();
        let quota = if let Some(q) = quotas.get(&task_type) { q } else {
            self.metrics.requests_allowed.fetch_add(1, Ordering::Relaxed);
            return ThrottleDecision::Allow;
        };

        let usage = self.usage.read();
        let current = usage.get(&task_type).cloned().unwrap_or_default();

        // Check concurrent ops
        if current.active_ops >= quota.max_concurrent {
            self.metrics.requests_rejected.fetch_add(1, Ordering::Relaxed);
            return ThrottleDecision::Reject;
        }

        // Check memory
        if current.memory_bytes_used >= quota.memory_bytes {
            self.metrics.requests_rejected.fetch_add(1, Ordering::Relaxed);
            return ThrottleDecision::Reject;
        }

        // Dynamic throttle under foreground load
        let fg_load = self.foreground_load();
        if quota.dynamic_throttle && fg_load > 0.5 {
            let effective_io_limit = (quota.io_bytes_per_sec as f64 * (1.0 - fg_load)) as u64;
            if effective_io_limit > 0 && io_bytes > 0 {
                let sleep_ms = (io_bytes as f64 / effective_io_limit as f64 * 1000.0) as u64;
                if sleep_ms > 0 {
                    self.metrics.requests_throttled.fetch_add(1, Ordering::Relaxed);
                    self.metrics.dynamic_reductions.fetch_add(1, Ordering::Relaxed);
                    return ThrottleDecision::Throttle(sleep_ms.min(5000));
                }
            }
        }

        // Check IO rate
        if quota.io_bytes_per_sec > 0 && io_bytes > 0 {
            let sleep_ms = (io_bytes as f64 / quota.io_bytes_per_sec as f64 * 1000.0) as u64;
            if sleep_ms > 10 {
                self.metrics.requests_throttled.fetch_add(1, Ordering::Relaxed);
                return ThrottleDecision::Throttle(sleep_ms.min(5000));
            }
        }

        self.metrics.requests_allowed.fetch_add(1, Ordering::Relaxed);
        ThrottleDecision::Allow
    }

    /// Get all quotas.
    pub fn all_quotas(&self) -> HashMap<BgTaskType, BgTaskQuota> {
        self.quotas.read().clone()
    }

    /// Get current usage snapshot.
    pub fn usage_snapshot(&self) -> HashMap<BgTaskType, BgTaskUsage> {
        self.usage.read().clone()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // -- Crash Hardening Tests --

    #[test]
    fn test_lifecycle_order_default() {
        let order = LifecycleOrder::default();
        assert_eq!(order.startup_order[0], ComponentType::Controller);
        assert_eq!(*order.shutdown_order.last().unwrap(), ComponentType::Controller);
    }

    #[test]
    fn test_component_lifecycle() {
        let coord = CrashHardeningCoordinator::new(LifecycleOrder::default(), None);
        coord.register(ComponentType::DataNode);
        coord.register(ComponentType::Gateway);
        assert!(!coord.all_running());
        coord.transition(ComponentType::DataNode, ComponentState::Running);
        coord.transition(ComponentType::Gateway, ComponentState::Running);
        assert!(coord.all_running());
        assert!(!coord.any_failed());
        coord.transition(ComponentType::Gateway, ComponentState::Failed);
        assert!(coord.any_failed());
        assert!(!coord.all_running());
    }

    #[test]
    fn test_startup_record() {
        let coord = CrashHardeningCoordinator::new(LifecycleOrder::default(), None);
        coord.register(ComponentType::DataNode);
        coord.record_startup(
            ComponentType::DataNode,
            vec![
                RecoveryAction::WalReplay { records_replayed: 100, from_lsn: 0, to_lsn: 100 },
                RecoveryAction::IndexRebuild { indexes_rebuilt: 3 },
            ],
            ShutdownType::Crash,
            500,
        );
        assert_eq!(coord.metrics.crash_recoveries.load(Ordering::Relaxed), 1);
        assert_eq!(coord.metrics.wal_replays.load(Ordering::Relaxed), 1);
        assert_eq!(coord.startup_log().len(), 1);
    }

    #[test]
    fn test_shutdown_sequence() {
        let coord = CrashHardeningCoordinator::new(LifecycleOrder::default(), None);
        for c in coord.startup_order() {
            coord.register(*c);
            coord.transition(*c, ComponentState::Running);
        }
        assert!(coord.all_running());
        coord.begin_shutdown();
        for (_, state) in coord.component_states() {
            assert_eq!(state, ComponentState::ShuttingDown);
        }
        coord.complete_shutdown();
        for (_, state) in coord.component_states() {
            assert_eq!(state, ComponentState::Stopped);
        }
        assert_eq!(coord.metrics.clean_shutdowns.load(Ordering::Relaxed), 1);
    }

    // -- Config Rollback Tests --

    #[test]
    fn test_config_set_and_get() {
        let mgr = ConfigRollbackManager::new(100);
        let v = mgr.set("max_connections", "100", "admin");
        let entry = mgr.get("max_connections").unwrap();
        assert_eq!(entry.value, "100");
        assert_eq!(entry.version, v);
        assert_eq!(entry.rollout_state, RolloutState::Applied);
    }

    #[test]
    fn test_config_rollback() {
        let mgr = ConfigRollbackManager::new(100);
        let v1 = mgr.set("pool_size", "10", "admin");
        let _v2 = mgr.set("pool_size", "20", "admin");
        assert_eq!(mgr.get("pool_size").unwrap().value, "20");
        assert!(mgr.rollback("pool_size"));
        assert_eq!(mgr.get("pool_size").unwrap().rollout_state, RolloutState::RolledBack);
        // Value is restored from v1
        let current = mgr.get("pool_size").unwrap();
        assert_eq!(current.value, "10");
        assert_eq!(mgr.metrics.rollbacks_executed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_config_staged_rollout() {
        let mgr = ConfigRollbackManager::new(100);
        let v = mgr.stage_change("repl_factor", "3", "admin");
        let entry = mgr.key_history("repl_factor");
        assert_eq!(entry.len(), 1);
        assert_eq!(entry[0].rollout_state, RolloutState::Staged);
        // Advance through stages
        mgr.advance_rollout("repl_factor", v, RolloutState::Canary);
        mgr.advance_rollout("repl_factor", v, RolloutState::RollingOut);
        mgr.advance_rollout("repl_factor", v, RolloutState::Applied);
        assert_eq!(mgr.get("repl_factor").unwrap().value, "3");
    }

    #[test]
    fn test_config_checksum_verify() {
        let mgr = ConfigRollbackManager::new(100);
        mgr.set("key1", "val1", "admin");
        assert!(mgr.verify_checksum("key1"));
        // Corrupt the checksum
        {
            let mut current = mgr.current.write();
            if let Some(entry) = current.get_mut("key1") {
                entry.checksum = 12345;
            }
        }
        assert!(!mgr.verify_checksum("key1"));
        assert_eq!(mgr.metrics.checksum_mismatches.load(Ordering::Relaxed), 1);
    }

    // -- Resource Leak Detector Tests --

    #[test]
    fn test_leak_detector_stable() {
        let detector = ResourceLeakDetector::new(1000);
        detector.set_threshold(LeakResourceType::FileDescriptor, 1.0);
        // Stable FD count over time
        for hour in 0..10u64 {
            let mut vals = HashMap::new();
            vals.insert(LeakResourceType::FileDescriptor, 50i64);
            detector.record_snapshot_at(vals, 1000 + hour * 3600);
        }
        let result = detector.analyze(LeakResourceType::FileDescriptor);
        assert!(!result.is_leaking);
        assert!(result.growth_rate_per_hour.abs() < 0.1);
    }

    #[test]
    fn test_leak_detector_growing() {
        let detector = ResourceLeakDetector::new(1000);
        detector.set_threshold(LeakResourceType::FileDescriptor, 1.0);
        // FD count growing at 5/hour
        for hour in 0..10u64 {
            let mut vals = HashMap::new();
            vals.insert(LeakResourceType::FileDescriptor, 50 + (hour as i64) * 5);
            detector.record_snapshot_at(vals, 1000 + hour * 3600);
        }
        let result = detector.analyze(LeakResourceType::FileDescriptor);
        assert!(result.is_leaking);
        assert!(result.growth_rate_per_hour > 4.0);
        assert!(result.confidence > 0.9);
    }

    #[test]
    fn test_leak_detector_analyze_all() {
        let detector = ResourceLeakDetector::new(1000);
        for hour in 0..5u64 {
            let mut vals = HashMap::new();
            vals.insert(LeakResourceType::FileDescriptor, 50);
            vals.insert(LeakResourceType::Thread, 10 + hour as i64);
            detector.record_snapshot_at(vals, 1000 + hour * 3600);
        }
        let results = detector.analyze_all();
        assert_eq!(results.len(), 2);
    }

    // -- Latency Guardrail Tests --

    #[test]
    fn test_guardrail_no_breach() {
        let engine = LatencyGuardrailEngine::new(10000, 100);
        engine.define_guardrail(PathGuardrail {
            path: GuardedPath::WalCommit,
            p99_threshold_us: 10_000,
            p999_threshold_us: 50_000,
            absolute_max_us: 100_000,
            trigger_backpressure: true,
        });
        for _ in 0..100 {
            let breach = engine.record(GuardedPath::WalCommit, 1_000);
            assert!(breach.is_none());
        }
        assert!(!engine.is_backpressure_active());
    }

    #[test]
    fn test_guardrail_absolute_breach() {
        let engine = LatencyGuardrailEngine::new(10000, 100);
        engine.define_guardrail(PathGuardrail {
            path: GuardedPath::WalCommit,
            p99_threshold_us: 10_000,
            p999_threshold_us: 50_000,
            absolute_max_us: 100_000,
            trigger_backpressure: true,
        });
        let breach = engine.record(GuardedPath::WalCommit, 200_000);
        assert!(breach.is_some());
        let b = breach.unwrap();
        assert_eq!(b.breach_type, BreachType::AbsoluteMaxExceeded);
        assert!(b.backpressure_triggered);
        assert!(engine.is_backpressure_active());
        assert_eq!(engine.metrics.absolute_breaches.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_guardrail_backpressure_clear() {
        let engine = LatencyGuardrailEngine::new(10000, 100);
        engine.define_guardrail(PathGuardrail {
            path: GuardedPath::GatewayForward,
            p99_threshold_us: 5_000,
            p999_threshold_us: 20_000,
            absolute_max_us: 50_000,
            trigger_backpressure: true,
        });
        engine.record(GuardedPath::GatewayForward, 60_000);
        assert!(engine.is_backpressure_active());
        engine.clear_backpressure();
        assert!(!engine.is_backpressure_active());
    }

    #[test]
    fn test_guardrail_percentiles() {
        let engine = LatencyGuardrailEngine::new(10000, 100);
        engine.define_guardrail(PathGuardrail {
            path: GuardedPath::TxnCommit,
            p99_threshold_us: 10_000,
            p999_threshold_us: 50_000,
            absolute_max_us: 100_000,
            trigger_backpressure: false,
        });
        for i in 0..1000u64 {
            engine.record(GuardedPath::TxnCommit, 100 + i);
        }
        let (p50, p99, p999) = engine.percentiles(GuardedPath::TxnCommit).unwrap();
        assert!(p50 > 0);
        assert!(p99 > p50);
        assert!(p999 >= p99);
    }

    // -- Background Task Isolator Tests --

    #[test]
    fn test_bg_isolator_allow() {
        let isolator = BgTaskIsolator::new();
        isolator.set_quota(BgTaskQuota {
            task_type: BgTaskType::Compaction,
            cpu_quota: 0.2,
            io_bytes_per_sec: 50_000_000,
            memory_bytes: 500_000_000,
            max_concurrent: 2,
            dynamic_throttle: true,
        });
        let decision = isolator.request(BgTaskType::Compaction, 100_000);
        assert_eq!(decision, ThrottleDecision::Allow);
    }

    #[test]
    fn test_bg_isolator_reject_concurrent() {
        let isolator = BgTaskIsolator::new();
        isolator.set_quota(BgTaskQuota {
            task_type: BgTaskType::Backup,
            cpu_quota: 0.1,
            io_bytes_per_sec: 10_000_000,
            memory_bytes: 100_000_000,
            max_concurrent: 1,
            dynamic_throttle: false,
        });
        isolator.report_usage(BgTaskType::Backup, BgTaskUsage {
            cpu_fraction: 0.05,
            io_bytes_used: 5_000_000,
            memory_bytes_used: 50_000_000,
            active_ops: 1, // already at max
        });
        let decision = isolator.request(BgTaskType::Backup, 1_000_000);
        assert_eq!(decision, ThrottleDecision::Reject);
    }

    #[test]
    fn test_bg_isolator_dynamic_throttle() {
        let isolator = BgTaskIsolator::new();
        isolator.set_quota(BgTaskQuota {
            task_type: BgTaskType::Compaction,
            cpu_quota: 0.2,
            io_bytes_per_sec: 50_000_000,
            memory_bytes: 500_000_000,
            max_concurrent: 2,
            dynamic_throttle: true,
        });
        isolator.set_foreground_load(0.8); // high foreground load
        let decision = isolator.request(BgTaskType::Compaction, 10_000_000);
        match decision {
            ThrottleDecision::Throttle(ms) => assert!(ms > 0),
            other => panic!("expected Throttle, got {:?}", other),
        }
        assert!(isolator.metrics.dynamic_reductions.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_bg_isolator_no_quota_allows() {
        let isolator = BgTaskIsolator::new();
        // No quota defined for GC
        let decision = isolator.request(BgTaskType::GarbageCollection, 1_000_000);
        assert_eq!(decision, ThrottleDecision::Allow);
    }

    // -- Concurrent Tests --

    #[test]
    fn test_guardrail_concurrent() {
        use std::sync::Arc;
        let engine = Arc::new(LatencyGuardrailEngine::new(100_000, 1000));
        engine.define_guardrail(PathGuardrail {
            path: GuardedPath::WalCommit,
            p99_threshold_us: 10_000,
            p999_threshold_us: 50_000,
            absolute_max_us: 100_000,
            trigger_backpressure: false,
        });
        let mut handles = Vec::new();
        for _ in 0..4 {
            let e = Arc::clone(&engine);
            handles.push(std::thread::spawn(move || {
                for i in 0..250u64 {
                    e.record(GuardedPath::WalCommit, 100 + i);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(engine.metrics.samples_recorded.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn test_config_rollback_concurrent() {
        use std::sync::Arc;
        let mgr = Arc::new(ConfigRollbackManager::new(1000));
        let mut handles = Vec::new();
        for t in 0..4 {
            let m = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    m.set(&format!("key-{}", t), &format!("val-{}", i), "test");
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(mgr.all_keys().len(), 4);
    }
}
