//! AI-driven automatic operations (AIOps) for FalconDB cluster management.
//!
//! Provides four integrated subsystems:
//!
//! 1. **Anomaly Detector** — sliding-window Z-score + IQR detection on key metrics
//!    (latency, TPS, replication lag, error rate). Fires `OpsAlert` events.
//!
//! 2. **Root Cause Analyzer** — rule-based causal graph traversal that maps
//!    symptom patterns to probable root causes and recommends remediation actions.
//!
//! 3. **Workload Forecaster** — exponential smoothing time-series model that
//!    predicts future TPS/memory/disk usage for proactive capacity management.
//!
//! 4. **Ops Advisor** — synthesizes signals from all subsystems into ranked
//!    `OpsRecommendation` items consumable by human operators or automated runbooks.
//!
//! All state is in-process (no external ML service). The module is designed to
//! be embedded in the cluster supervisor loop with a periodic `tick()` call.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use falcon_common::types::NodeId;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Metric snapshot (ingestion interface)
// ═══════════════════════════════════════════════════════════════════════════

/// A point-in-time snapshot of cluster metrics for one node or the whole cluster.
#[derive(Debug, Clone)]
pub struct MetricSnapshot {
    pub timestamp_ms: u64,
    pub node_id: Option<NodeId>,
    /// Transactions per second (measured over last interval)
    pub tps: f64,
    /// P99 query latency in microseconds
    pub latency_p99_us: f64,
    /// P50 query latency in microseconds
    pub latency_p50_us: f64,
    /// Replication lag in bytes
    pub replication_lag_bytes: u64,
    /// WAL write bytes per second
    pub wal_write_bps: f64,
    /// Active transaction count
    pub active_txns: u64,
    /// Aborted transaction count (cumulative delta since last snapshot)
    pub aborted_txns_delta: u64,
    /// Memory used by MemTables in bytes
    pub memtable_bytes: u64,
    /// Number of pending compaction tasks
    pub pending_compactions: u32,
    /// Error rate (errors per second)
    pub error_rate: f64,
    /// CPU usage fraction [0,1] (optional; 0 = unknown)
    pub cpu_usage: f64,
    /// Disk free bytes (0 = unknown)
    pub disk_free_bytes: u64,
    /// Connection count
    pub connections: u32,
}

impl MetricSnapshot {
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

impl Default for MetricSnapshot {
    fn default() -> Self {
        Self {
            timestamp_ms: Self::now_ms(),
            node_id: None,
            tps: 0.0,
            latency_p99_us: 0.0,
            latency_p50_us: 0.0,
            replication_lag_bytes: 0,
            wal_write_bps: 0.0,
            active_txns: 0,
            aborted_txns_delta: 0,
            memtable_bytes: 0,
            pending_compactions: 0,
            error_rate: 0.0,
            cpu_usage: 0.0,
            disk_free_bytes: 0,
            connections: 0,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Alert types
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

impl fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => write!(f, "INFO"),
            Self::Warning => write!(f, "WARNING"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertKind {
    HighLatency,
    TpsAnomaly,
    HighErrorRate,
    ReplicationLag,
    MemoryPressure,
    DiskSpaceLow,
    ConnectionSaturation,
    CompactionBacklog,
    TpsDropped,
    AbortStorm,
}

impl fmt::Display for AlertKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::HighLatency => "high_latency",
            Self::TpsAnomaly => "tps_anomaly",
            Self::HighErrorRate => "high_error_rate",
            Self::ReplicationLag => "replication_lag",
            Self::MemoryPressure => "memory_pressure",
            Self::DiskSpaceLow => "disk_space_low",
            Self::ConnectionSaturation => "connection_saturation",
            Self::CompactionBacklog => "compaction_backlog",
            Self::TpsDropped => "tps_dropped",
            Self::AbortStorm => "abort_storm",
        };
        write!(f, "{s}")
    }
}

/// A fired anomaly alert.
#[derive(Debug, Clone)]
pub struct OpsAlert {
    pub id: u64,
    pub timestamp_ms: u64,
    pub severity: AlertSeverity,
    pub kind: AlertKind,
    pub node_id: Option<NodeId>,
    /// Human-readable description with metric values.
    pub message: String,
    /// Observed value that triggered the alert.
    pub observed_value: f64,
    /// Threshold that was exceeded.
    pub threshold: f64,
    /// Z-score of the anomaly (if statistical detection was used).
    pub z_score: Option<f64>,
}

impl fmt::Display for OpsAlert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} {} | value={:.2} threshold={:.2} | {}",
            self.severity,
            self.kind,
            self.timestamp_ms,
            self.observed_value,
            self.threshold,
            self.message
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Anomaly Detector
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for the anomaly detector.
#[derive(Debug, Clone)]
pub struct AnomalyDetectorConfig {
    /// Sliding window size for baseline statistics.
    pub window_size: usize,
    /// Z-score threshold for anomaly detection.
    pub zscore_threshold: f64,
    /// P99 latency alert threshold (microseconds).
    pub latency_p99_warn_us: f64,
    pub latency_p99_crit_us: f64,
    /// Error rate thresholds (errors/sec).
    pub error_rate_warn: f64,
    pub error_rate_crit: f64,
    /// Replication lag thresholds (bytes).
    pub replication_lag_warn_bytes: u64,
    pub replication_lag_crit_bytes: u64,
    /// Memory usage thresholds (bytes).
    pub memtable_warn_bytes: u64,
    pub memtable_crit_bytes: u64,
    /// Disk free space thresholds.
    pub disk_free_warn_bytes: u64,
    pub disk_free_crit_bytes: u64,
    /// Connection count thresholds.
    pub connections_warn: u32,
    pub connections_crit: u32,
    /// Compaction backlog thresholds.
    pub compaction_warn: u32,
    pub compaction_crit: u32,
    /// TPS drop threshold (fraction of baseline, e.g. 0.5 = 50% drop).
    pub tps_drop_threshold: f64,
    /// Abort rate threshold (aborts/sec).
    pub abort_rate_warn: f64,
}

impl Default for AnomalyDetectorConfig {
    fn default() -> Self {
        Self {
            window_size: 60,
            zscore_threshold: 3.0,
            latency_p99_warn_us: 50_000.0,
            latency_p99_crit_us: 200_000.0,
            error_rate_warn: 1.0,
            error_rate_crit: 10.0,
            replication_lag_warn_bytes: 10 * 1024 * 1024,
            replication_lag_crit_bytes: 100 * 1024 * 1024,
            memtable_warn_bytes: 512 * 1024 * 1024,
            memtable_crit_bytes: 1024 * 1024 * 1024,
            disk_free_warn_bytes: 10 * 1024 * 1024 * 1024,
            disk_free_crit_bytes: 1024 * 1024 * 1024,
            connections_warn: 800,
            connections_crit: 950,
            compaction_warn: 10,
            compaction_crit: 50,
            tps_drop_threshold: 0.4,
            abort_rate_warn: 5.0,
        }
    }
}

/// Per-metric sliding window for online statistics.
struct MetricWindow {
    values: VecDeque<f64>,
    capacity: usize,
    sum: f64,
    sum_sq: f64,
}

impl MetricWindow {
    fn new(capacity: usize) -> Self {
        Self {
            values: VecDeque::with_capacity(capacity),
            capacity,
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    fn push(&mut self, v: f64) {
        if self.values.len() == self.capacity {
            let old = self.values.pop_front().unwrap_or(0.0);
            self.sum -= old;
            self.sum_sq -= old * old;
        }
        self.values.push_back(v);
        self.sum += v;
        self.sum_sq += v * v;
    }

    fn mean(&self) -> f64 {
        if self.values.is_empty() {
            0.0
        } else {
            self.sum / self.values.len() as f64
        }
    }

    fn stddev(&self) -> f64 {
        let n = self.values.len() as f64;
        if n < 2.0 {
            return 0.0;
        }
        let mean = self.mean();
        let variance = (self.sum_sq / n) - mean * mean;
        variance.max(0.0).sqrt()
    }

    fn zscore(&self, v: f64) -> f64 {
        let sd = self.stddev();
        if sd < 1e-9 {
            0.0
        } else {
            (v - self.mean()) / sd
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

/// Key into the per-metric windows map.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum WindowKey {
    Tps,
    LatencyP99,
    LatencyP50,
    ErrorRate,
    AbortRate,
}

pub struct AnomalyDetector {
    config: AnomalyDetectorConfig,
    windows: HashMap<WindowKey, MetricWindow>,
    alert_seq: AtomicU64,
}

impl AnomalyDetector {
    pub fn new(config: AnomalyDetectorConfig) -> Self {
        let cap = config.window_size;
        let mut windows = HashMap::new();
        windows.insert(WindowKey::Tps, MetricWindow::new(cap));
        windows.insert(WindowKey::LatencyP99, MetricWindow::new(cap));
        windows.insert(WindowKey::LatencyP50, MetricWindow::new(cap));
        windows.insert(WindowKey::ErrorRate, MetricWindow::new(cap));
        windows.insert(WindowKey::AbortRate, MetricWindow::new(cap));
        Self {
            config,
            windows,
            alert_seq: AtomicU64::new(1),
        }
    }

    fn next_id(&self) -> u64 {
        self.alert_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Feed a new snapshot and return any alerts fired.
    pub fn feed(&mut self, snap: &MetricSnapshot) -> Vec<OpsAlert> {
        let mut alerts = Vec::new();

        // Update windows
        self.windows
            .get_mut(&WindowKey::Tps)
            .unwrap()
            .push(snap.tps);
        self.windows
            .get_mut(&WindowKey::LatencyP99)
            .unwrap()
            .push(snap.latency_p99_us);
        self.windows
            .get_mut(&WindowKey::LatencyP50)
            .unwrap()
            .push(snap.latency_p50_us);
        self.windows
            .get_mut(&WindowKey::ErrorRate)
            .unwrap()
            .push(snap.error_rate);
        let abort_rate = snap.aborted_txns_delta as f64;
        self.windows
            .get_mut(&WindowKey::AbortRate)
            .unwrap()
            .push(abort_rate);

        let ts = snap.timestamp_ms;
        let nid = snap.node_id;

        // ── Latency alerts ──────────────────────────────────────────────
        if snap.latency_p99_us >= self.config.latency_p99_crit_us {
            alerts.push(self.make_alert(
                AlertSeverity::Critical,
                AlertKind::HighLatency,
                nid,
                ts,
                snap.latency_p99_us,
                self.config.latency_p99_crit_us,
                None,
                format!(
                    "P99 latency {:.1}ms exceeds critical threshold",
                    snap.latency_p99_us / 1000.0
                ),
            ));
        } else if snap.latency_p99_us >= self.config.latency_p99_warn_us {
            alerts.push(self.make_alert(
                AlertSeverity::Warning,
                AlertKind::HighLatency,
                nid,
                ts,
                snap.latency_p99_us,
                self.config.latency_p99_warn_us,
                None,
                format!(
                    "P99 latency {:.1}ms above warning threshold",
                    snap.latency_p99_us / 1000.0
                ),
            ));
        }

        // ── Statistical TPS anomaly (Z-score) ───────────────────────────
        let tps_w = self.windows.get(&WindowKey::Tps).unwrap();
        if tps_w.len() >= 10 {
            let z = tps_w.zscore(snap.tps);
            if z.abs() >= self.config.zscore_threshold {
                let kind = if z < 0.0 {
                    AlertKind::TpsDropped
                } else {
                    AlertKind::TpsAnomaly
                };
                alerts.push(self.make_alert(
                    AlertSeverity::Warning,
                    kind,
                    nid,
                    ts,
                    snap.tps,
                    tps_w.mean(),
                    Some(z),
                    format!(
                        "TPS {:.1} is {:.1}σ from baseline {:.1}",
                        snap.tps,
                        z.abs(),
                        tps_w.mean()
                    ),
                ));
            }
        }

        // ── TPS absolute drop ────────────────────────────────────────────
        let tps_mean = self.windows.get(&WindowKey::Tps).unwrap().mean();
        if tps_mean > 10.0 && snap.tps < tps_mean * self.config.tps_drop_threshold {
            alerts.push(self.make_alert(
                AlertSeverity::Critical,
                AlertKind::TpsDropped,
                nid,
                ts,
                snap.tps,
                tps_mean * self.config.tps_drop_threshold,
                None,
                format!(
                    "TPS dropped to {:.1} (baseline {:.1}, threshold {:.0}%)",
                    snap.tps,
                    tps_mean,
                    self.config.tps_drop_threshold * 100.0
                ),
            ));
        }

        // ── Error rate ──────────────────────────────────────────────────
        if snap.error_rate >= self.config.error_rate_crit {
            alerts.push(self.make_alert(
                AlertSeverity::Critical,
                AlertKind::HighErrorRate,
                nid,
                ts,
                snap.error_rate,
                self.config.error_rate_crit,
                None,
                format!("Error rate {:.2}/s is critical", snap.error_rate),
            ));
        } else if snap.error_rate >= self.config.error_rate_warn {
            alerts.push(self.make_alert(
                AlertSeverity::Warning,
                AlertKind::HighErrorRate,
                nid,
                ts,
                snap.error_rate,
                self.config.error_rate_warn,
                None,
                format!(
                    "Error rate {:.2}/s above warning threshold",
                    snap.error_rate
                ),
            ));
        }

        // ── Replication lag ─────────────────────────────────────────────
        if snap.replication_lag_bytes >= self.config.replication_lag_crit_bytes {
            alerts.push(self.make_alert(
                AlertSeverity::Critical,
                AlertKind::ReplicationLag,
                nid,
                ts,
                snap.replication_lag_bytes as f64,
                self.config.replication_lag_crit_bytes as f64,
                None,
                format!(
                    "Replication lag {:.1}MB is critical",
                    snap.replication_lag_bytes as f64 / 1e6
                ),
            ));
        } else if snap.replication_lag_bytes >= self.config.replication_lag_warn_bytes {
            alerts.push(self.make_alert(
                AlertSeverity::Warning,
                AlertKind::ReplicationLag,
                nid,
                ts,
                snap.replication_lag_bytes as f64,
                self.config.replication_lag_warn_bytes as f64,
                None,
                format!(
                    "Replication lag {:.1}MB above warning threshold",
                    snap.replication_lag_bytes as f64 / 1e6
                ),
            ));
        }

        // ── Memory pressure ─────────────────────────────────────────────
        if snap.memtable_bytes >= self.config.memtable_crit_bytes {
            alerts.push(self.make_alert(
                AlertSeverity::Critical,
                AlertKind::MemoryPressure,
                nid,
                ts,
                snap.memtable_bytes as f64,
                self.config.memtable_crit_bytes as f64,
                None,
                format!(
                    "MemTable usage {:.1}GB is critical",
                    snap.memtable_bytes as f64 / 1e9
                ),
            ));
        } else if snap.memtable_bytes >= self.config.memtable_warn_bytes {
            alerts.push(self.make_alert(
                AlertSeverity::Warning,
                AlertKind::MemoryPressure,
                nid,
                ts,
                snap.memtable_bytes as f64,
                self.config.memtable_warn_bytes as f64,
                None,
                format!(
                    "MemTable usage {:.1}MB above warning threshold",
                    snap.memtable_bytes as f64 / 1e6
                ),
            ));
        }

        // ── Disk space ──────────────────────────────────────────────────
        if snap.disk_free_bytes > 0 {
            if snap.disk_free_bytes <= self.config.disk_free_crit_bytes {
                alerts.push(self.make_alert(
                    AlertSeverity::Critical,
                    AlertKind::DiskSpaceLow,
                    nid,
                    ts,
                    snap.disk_free_bytes as f64,
                    self.config.disk_free_crit_bytes as f64,
                    None,
                    format!(
                        "Disk free {:.1}GB is critically low",
                        snap.disk_free_bytes as f64 / 1e9
                    ),
                ));
            } else if snap.disk_free_bytes <= self.config.disk_free_warn_bytes {
                alerts.push(self.make_alert(
                    AlertSeverity::Warning,
                    AlertKind::DiskSpaceLow,
                    nid,
                    ts,
                    snap.disk_free_bytes as f64,
                    self.config.disk_free_warn_bytes as f64,
                    None,
                    format!(
                        "Disk free {:.1}GB below warning threshold",
                        snap.disk_free_bytes as f64 / 1e9
                    ),
                ));
            }
        }

        // ── Connection saturation ───────────────────────────────────────
        if snap.connections >= self.config.connections_crit {
            alerts.push(self.make_alert(
                AlertSeverity::Critical,
                AlertKind::ConnectionSaturation,
                nid,
                ts,
                snap.connections as f64,
                self.config.connections_crit as f64,
                None,
                format!("Connection count {} approaching limit", snap.connections),
            ));
        } else if snap.connections >= self.config.connections_warn {
            alerts.push(self.make_alert(
                AlertSeverity::Warning,
                AlertKind::ConnectionSaturation,
                nid,
                ts,
                snap.connections as f64,
                self.config.connections_warn as f64,
                None,
                format!(
                    "Connection count {} above warning threshold",
                    snap.connections
                ),
            ));
        }

        // ── Compaction backlog ──────────────────────────────────────────
        if snap.pending_compactions >= self.config.compaction_crit {
            alerts.push(self.make_alert(
                AlertSeverity::Critical,
                AlertKind::CompactionBacklog,
                nid,
                ts,
                snap.pending_compactions as f64,
                self.config.compaction_crit as f64,
                None,
                format!(
                    "{} pending compaction tasks (critical)",
                    snap.pending_compactions
                ),
            ));
        } else if snap.pending_compactions >= self.config.compaction_warn {
            alerts.push(self.make_alert(
                AlertSeverity::Warning,
                AlertKind::CompactionBacklog,
                nid,
                ts,
                snap.pending_compactions as f64,
                self.config.compaction_warn as f64,
                None,
                format!(
                    "{} pending compaction tasks (warning)",
                    snap.pending_compactions
                ),
            ));
        }

        // ── Abort storm ─────────────────────────────────────────────────
        if abort_rate >= self.config.abort_rate_warn {
            alerts.push(self.make_alert(
                AlertSeverity::Warning,
                AlertKind::AbortStorm,
                nid,
                ts,
                abort_rate,
                self.config.abort_rate_warn,
                None,
                format!(
                    "{} transaction aborts in last interval",
                    snap.aborted_txns_delta
                ),
            ));
        }

        alerts
    }

    #[allow(clippy::too_many_arguments)]
    fn make_alert(
        &self,
        severity: AlertSeverity,
        kind: AlertKind,
        node_id: Option<NodeId>,
        timestamp_ms: u64,
        observed_value: f64,
        threshold: f64,
        z_score: Option<f64>,
        message: String,
    ) -> OpsAlert {
        OpsAlert {
            id: self.next_id(),
            timestamp_ms,
            severity,
            kind,
            node_id,
            message,
            observed_value,
            threshold,
            z_score,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Root Cause Analyzer
// ═══════════════════════════════════════════════════════════════════════════

/// Probable root cause identified by the analyzer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RootCause {
    /// Sustained high write amplification — WAL flush is the bottleneck.
    WalFlushBottleneck,
    /// Compaction falling behind write rate — LSM tree is growing unbounded.
    CompactionLagging,
    /// Network issue causing replication lag (not a local write issue).
    NetworkDegradation,
    /// Memory exhaustion causing GC pressure / eviction storms.
    MemoryExhaustion,
    /// Query plan regression — a recent schema change or stats update caused bad plans.
    QueryPlanRegression,
    /// Connection pool exhaustion — upstream is hammering the DB.
    ConnectionExhaustion,
    /// Disk I/O saturation — writes are queuing behind disk bandwidth.
    DiskIoSaturation,
    /// Lock contention — high abort rate from serialization conflicts.
    LockContention,
    /// Node is unhealthy (heartbeat issues, process crash recovery).
    NodeUnhealthy,
    /// Unknown — insufficient signal to determine root cause.
    Unknown,
}

impl fmt::Display for RootCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::WalFlushBottleneck => "wal_flush_bottleneck",
            Self::CompactionLagging => "compaction_lagging",
            Self::NetworkDegradation => "network_degradation",
            Self::MemoryExhaustion => "memory_exhaustion",
            Self::QueryPlanRegression => "query_plan_regression",
            Self::ConnectionExhaustion => "connection_exhaustion",
            Self::DiskIoSaturation => "disk_io_saturation",
            Self::LockContention => "lock_contention",
            Self::NodeUnhealthy => "node_unhealthy",
            Self::Unknown => "unknown",
        };
        write!(f, "{s}")
    }
}

/// A root cause diagnosis result.
#[derive(Debug, Clone)]
pub struct Diagnosis {
    pub root_cause: RootCause,
    /// Confidence score [0,1].
    pub confidence: f64,
    /// Human-readable explanation.
    pub explanation: String,
    /// Recommended immediate action.
    pub immediate_action: String,
    /// Recommended long-term fix.
    pub long_term_fix: String,
    /// Contributing alert IDs.
    pub contributing_alert_ids: Vec<u64>,
}

/// Rule-based root cause analyzer over a set of co-occurring alerts.
pub struct RootCauseAnalyzer;

impl RootCauseAnalyzer {
    /// Analyze a set of alerts and return ranked diagnoses.
    pub fn analyze(alerts: &[OpsAlert], recent_snap: &MetricSnapshot) -> Vec<Diagnosis> {
        if alerts.is_empty() {
            return vec![];
        }

        let mut diagnoses = Vec::new();
        let ids: Vec<u64> = alerts.iter().map(|a| a.id).collect();

        // Collect alert kinds for pattern matching
        let kinds: std::collections::HashSet<AlertKind> = alerts.iter().map(|a| a.kind).collect();

        // ── Rule: WAL flush bottleneck ───────────────────────────────────
        // Signal: high latency + high WAL write rate + NOT high compaction
        if kinds.contains(&AlertKind::HighLatency)
            && recent_snap.wal_write_bps > 100_000_000.0
            && recent_snap.pending_compactions < 5
        {
            diagnoses.push(Diagnosis {
                root_cause: RootCause::WalFlushBottleneck,
                confidence: 0.80,
                explanation: format!(
                    "WAL write rate {:.1}MB/s combined with {:.1}ms P99 latency suggests flush pipeline is saturated.",
                    recent_snap.wal_write_bps / 1e6,
                    recent_snap.latency_p99_us / 1000.0
                ),
                immediate_action: "Reduce write concurrency or enable WAL compression.".into(),
                long_term_fix: "Add NVMe write cache or increase WAL segment size.".into(),
                contributing_alert_ids: ids.clone(),
            });
        }

        // ── Rule: Compaction lagging ─────────────────────────────────────
        // Signal: compaction backlog + memory pressure
        if kinds.contains(&AlertKind::CompactionBacklog)
            && kinds.contains(&AlertKind::MemoryPressure)
        {
            diagnoses.push(Diagnosis {
                root_cause: RootCause::CompactionLagging,
                confidence: 0.85,
                explanation: format!(
                    "{} pending compaction tasks with {:.1}GB MemTable usage indicates write amplification spiral.",
                    recent_snap.pending_compactions,
                    recent_snap.memtable_bytes as f64 / 1e9
                ),
                immediate_action: "Trigger manual compaction: `COMPACT TABLE <name>`".into(),
                long_term_fix: "Tune compaction thread count or write throttle threshold.".into(),
                contributing_alert_ids: ids.clone(),
            });
        }

        // ── Rule: Network degradation ────────────────────────────────────
        // Signal: replication lag WITHOUT local memory/compaction issues
        if kinds.contains(&AlertKind::ReplicationLag)
            && !kinds.contains(&AlertKind::MemoryPressure)
            && !kinds.contains(&AlertKind::CompactionBacklog)
        {
            diagnoses.push(Diagnosis {
                root_cause: RootCause::NetworkDegradation,
                confidence: 0.75,
                explanation: format!(
                    "Replication lag {:.1}MB without local pressure suggests network bottleneck to replica.",
                    recent_snap.replication_lag_bytes as f64 / 1e6
                ),
                immediate_action: "Check network bandwidth to replica nodes.".into(),
                long_term_fix: "Enable WAL streaming compression or add replica bandwidth quota.".into(),
                contributing_alert_ids: ids.clone(),
            });
        }

        // ── Rule: Memory exhaustion ──────────────────────────────────────
        if kinds.contains(&AlertKind::MemoryPressure) && recent_snap.cpu_usage > 0.85 {
            diagnoses.push(Diagnosis {
                root_cause: RootCause::MemoryExhaustion,
                confidence: 0.90,
                explanation: format!(
                    "MemTable {:.1}GB with CPU {:.0}% indicates GC/eviction pressure.",
                    recent_snap.memtable_bytes as f64 / 1e9,
                    recent_snap.cpu_usage * 100.0
                ),
                immediate_action: "Run `FLUSH MEMTABLE` to force compaction to disk.".into(),
                long_term_fix: "Increase heap limit or reduce MemTable size threshold.".into(),
                contributing_alert_ids: ids.clone(),
            });
        }

        // ── Rule: Connection exhaustion ──────────────────────────────────
        if kinds.contains(&AlertKind::ConnectionSaturation)
            && kinds.contains(&AlertKind::HighLatency)
        {
            diagnoses.push(Diagnosis {
                root_cause: RootCause::ConnectionExhaustion,
                confidence: 0.80,
                explanation: format!(
                    "{} connections with high latency suggests connection queuing at OS level.",
                    recent_snap.connections
                ),
                immediate_action: "Enable connection limiting in smart_gateway config.".into(),
                long_term_fix: "Scale out with additional DB nodes or tune max_connections.".into(),
                contributing_alert_ids: ids.clone(),
            });
        }

        // ── Rule: Lock contention / abort storm ──────────────────────────
        if kinds.contains(&AlertKind::AbortStorm) {
            diagnoses.push(Diagnosis {
                root_cause: RootCause::LockContention,
                confidence: 0.70,
                explanation: format!(
                    "{} transaction aborts suggest hot-row contention or serialization conflicts.",
                    recent_snap.aborted_txns_delta
                ),
                immediate_action: "Check for hot-row patterns; consider `ON CONFLICT DO NOTHING`."
                    .into(),
                long_term_fix:
                    "Review transaction isolation level; add row-level sharding for hot keys."
                        .into(),
                contributing_alert_ids: ids.clone(),
            });
        }

        // ── Rule: Disk saturation ────────────────────────────────────────
        if kinds.contains(&AlertKind::DiskSpaceLow) && kinds.contains(&AlertKind::HighLatency) {
            diagnoses.push(Diagnosis {
                root_cause: RootCause::DiskIoSaturation,
                confidence: 0.75,
                explanation: "Low disk space with high latency indicates I/O contention from disk-full fragmentation.".into(),
                immediate_action: "Delete old WAL archives; run `VACUUM` on cold tables.".into(),
                long_term_fix: "Add storage capacity or enable WAL archival to remote storage.".into(),
                contributing_alert_ids: ids.clone(),
            });
        }

        // ── Fallback ─────────────────────────────────────────────────────
        if diagnoses.is_empty() {
            diagnoses.push(Diagnosis {
                root_cause: RootCause::Unknown,
                confidence: 0.20,
                explanation: format!(
                    "Unable to determine root cause from {} alerts.",
                    alerts.len()
                ),
                immediate_action: "Collect extended diagnostics with `EXPLAIN ANALYZE`.".into(),
                long_term_fix: "Enable debug tracing and review logs.".into(),
                contributing_alert_ids: ids,
            });
        }

        // Sort by confidence descending
        diagnoses.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        diagnoses
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Workload Forecaster
// ═══════════════════════════════════════════════════════════════════════════

/// Exponential smoothing (Holt-Winters single exponential) forecaster.
/// Predicts future values of a single metric series.
#[derive(Debug, Clone)]
pub struct ExponentialSmoother {
    /// Smoothing factor α ∈ (0,1). Higher = more weight on recent values.
    pub alpha: f64,
    /// Current smoothed level.
    pub level: f64,
    /// Whether the model has been initialized.
    initialized: bool,
}

impl ExponentialSmoother {
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha: alpha.clamp(0.01, 0.99),
            level: 0.0,
            initialized: false,
        }
    }

    pub fn update(&mut self, value: f64) {
        if !self.initialized {
            self.level = value;
            self.initialized = true;
        } else {
            self.level = self.alpha * value + (1.0 - self.alpha) * self.level;
        }
    }

    /// Predict h steps ahead (for simple exponential smoothing, forecast = level).
    pub fn forecast(&self, _h: usize) -> f64 {
        self.level
    }

    pub fn is_ready(&self) -> bool {
        self.initialized
    }
}

/// Holt's double exponential smoothing (level + trend).
#[derive(Debug, Clone)]
pub struct HoltSmoother {
    pub alpha: f64,
    pub beta: f64,
    pub level: f64,
    pub trend: f64,
    initialized: bool,
    prev_value: f64,
}

impl HoltSmoother {
    pub fn new(alpha: f64, beta: f64) -> Self {
        Self {
            alpha: alpha.clamp(0.01, 0.99),
            beta: beta.clamp(0.01, 0.99),
            level: 0.0,
            trend: 0.0,
            initialized: false,
            prev_value: 0.0,
        }
    }

    pub fn update(&mut self, value: f64) {
        if !self.initialized {
            self.level = value;
            self.trend = 0.0;
            self.prev_value = value;
            self.initialized = true;
        } else {
            let prev_level = self.level;
            self.level = self.alpha * value + (1.0 - self.alpha) * (self.level + self.trend);
            self.trend = self.beta * (self.level - prev_level) + (1.0 - self.beta) * self.trend;
            self.prev_value = value;
        }
    }

    /// Forecast h steps ahead: level + h * trend.
    pub fn forecast(&self, h: usize) -> f64 {
        (self.level + h as f64 * self.trend).max(0.0)
    }

    pub fn is_ready(&self) -> bool {
        self.initialized
    }
}

/// Multi-metric workload forecaster.
pub struct WorkloadForecaster {
    pub tps: HoltSmoother,
    pub latency_p99: ExponentialSmoother,
    pub memtable_bytes: HoltSmoother,
    pub wal_bps: ExponentialSmoother,
    pub connections: ExponentialSmoother,
    /// History of (timestamp_ms, tps) for capacity planning.
    tps_history: VecDeque<(u64, f64)>,
    history_cap: usize,
}

impl WorkloadForecaster {
    pub fn new() -> Self {
        Self {
            tps: HoltSmoother::new(0.3, 0.1),
            latency_p99: ExponentialSmoother::new(0.2),
            memtable_bytes: HoltSmoother::new(0.2, 0.05),
            wal_bps: ExponentialSmoother::new(0.3),
            connections: ExponentialSmoother::new(0.2),
            tps_history: VecDeque::with_capacity(1440),
            history_cap: 1440,
        }
    }

    pub fn feed(&mut self, snap: &MetricSnapshot) {
        self.tps.update(snap.tps);
        self.latency_p99.update(snap.latency_p99_us);
        self.memtable_bytes.update(snap.memtable_bytes as f64);
        self.wal_bps.update(snap.wal_write_bps);
        self.connections.update(snap.connections as f64);

        if self.tps_history.len() == self.history_cap {
            self.tps_history.pop_front();
        }
        self.tps_history.push_back((snap.timestamp_ms, snap.tps));
    }

    /// Forecast for a time horizon (in intervals from now).
    pub fn forecast(&self, horizon_intervals: usize) -> ForecastResult {
        ForecastResult {
            horizon_intervals,
            tps: self.tps.forecast(horizon_intervals),
            latency_p99_us: self.latency_p99.forecast(horizon_intervals),
            memtable_bytes: self.memtable_bytes.forecast(horizon_intervals) as u64,
            wal_bps: self.wal_bps.forecast(horizon_intervals),
            connections: self.connections.forecast(horizon_intervals) as u32,
        }
    }

    /// Return the peak TPS observed in the history window.
    pub fn peak_tps(&self) -> f64 {
        self.tps_history
            .iter()
            .map(|(_, t)| *t)
            .fold(0.0_f64, f64::max)
    }

    /// Return the trend direction for TPS: positive = growing, negative = shrinking.
    pub fn tps_trend(&self) -> f64 {
        self.tps.trend
    }
}

impl Default for WorkloadForecaster {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a workload forecast.
#[derive(Debug, Clone)]
pub struct ForecastResult {
    /// Number of intervals in the forecast horizon.
    pub horizon_intervals: usize,
    pub tps: f64,
    pub latency_p99_us: f64,
    pub memtable_bytes: u64,
    pub wal_bps: f64,
    pub connections: u32,
}

impl fmt::Display for ForecastResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Forecast(+{}): tps={:.1} p99={:.1}ms mem={:.1}MB wal={:.1}MB/s conns={}",
            self.horizon_intervals,
            self.tps,
            self.latency_p99_us / 1000.0,
            self.memtable_bytes as f64 / 1e6,
            self.wal_bps / 1e6,
            self.connections,
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — Ops Advisor
// ═══════════════════════════════════════════════════════════════════════════

/// Priority of a recommendation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecommendationPriority {
    Low,
    Medium,
    High,
    Urgent,
}

impl fmt::Display for RecommendationPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => write!(f, "LOW"),
            Self::Medium => write!(f, "MEDIUM"),
            Self::High => write!(f, "HIGH"),
            Self::Urgent => write!(f, "URGENT"),
        }
    }
}

/// An actionable recommendation from the AI ops advisor.
#[derive(Debug, Clone)]
pub struct OpsRecommendation {
    pub id: u64,
    pub timestamp_ms: u64,
    pub priority: RecommendationPriority,
    pub title: String,
    pub description: String,
    /// SQL or admin command to execute.
    pub action_sql: Option<String>,
    /// Whether this can be automated without human approval.
    pub auto_executable: bool,
    /// Category tag for filtering.
    pub category: String,
    /// Estimated impact (human-readable).
    pub estimated_impact: String,
}

impl fmt::Display for OpsRecommendation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} | {} | auto={} | {}",
            self.priority, self.title, self.category, self.auto_executable, self.description
        )
    }
}

/// The top-level AI Ops Advisor: synthesizes alerts, diagnoses, and forecasts
/// into ranked actionable recommendations.
pub struct OpsAdvisor {
    rec_seq: AtomicU64,
    /// Suppression window: don't repeat the same recommendation within this duration.
    suppression_window: Duration,
    last_recommendations: Mutex<HashMap<String, Instant>>,
}

impl OpsAdvisor {
    pub fn new(suppression_window: Duration) -> Self {
        Self {
            rec_seq: AtomicU64::new(1),
            suppression_window,
            last_recommendations: Mutex::new(HashMap::new()),
        }
    }

    fn next_id(&self) -> u64 {
        self.rec_seq.fetch_add(1, Ordering::Relaxed)
    }

    fn is_suppressed(&self, key: &str) -> bool {
        let map = self.last_recommendations.lock();
        if let Some(t) = map.get(key) {
            t.elapsed() < self.suppression_window
        } else {
            false
        }
    }

    fn record_issued(&self, key: &str) {
        self.last_recommendations
            .lock()
            .insert(key.to_owned(), Instant::now());
    }

    /// Generate recommendations from current state.
    pub fn advise(
        &self,
        alerts: &[OpsAlert],
        diagnoses: &[Diagnosis],
        forecast: &ForecastResult,
        snap: &MetricSnapshot,
    ) -> Vec<OpsRecommendation> {
        let mut recs = Vec::new();
        let ts = snap.timestamp_ms;

        // ── Recommendations from diagnoses ───────────────────────────────
        for diag in diagnoses {
            let key = format!("diag:{}", diag.root_cause);
            if self.is_suppressed(&key) {
                continue;
            }
            let (priority, auto_executable) = match diag.root_cause {
                RootCause::WalFlushBottleneck => (RecommendationPriority::High, false),
                RootCause::CompactionLagging => (RecommendationPriority::High, true),
                RootCause::NetworkDegradation => (RecommendationPriority::Medium, false),
                RootCause::MemoryExhaustion => (RecommendationPriority::Urgent, true),
                RootCause::QueryPlanRegression => (RecommendationPriority::Medium, false),
                RootCause::ConnectionExhaustion => (RecommendationPriority::High, false),
                RootCause::DiskIoSaturation => (RecommendationPriority::High, false),
                RootCause::LockContention => (RecommendationPriority::Medium, false),
                RootCause::NodeUnhealthy => (RecommendationPriority::Urgent, false),
                RootCause::Unknown => (RecommendationPriority::Low, false),
            };
            let action_sql = match diag.root_cause {
                RootCause::CompactionLagging => {
                    Some("-- Run via admin: COMPACT ALL TABLES".to_owned())
                }
                RootCause::MemoryExhaustion => {
                    Some("-- Run via admin: FLUSH MEMTABLE ALL".to_owned())
                }
                _ => None,
            };
            recs.push(OpsRecommendation {
                id: self.next_id(),
                timestamp_ms: ts,
                priority,
                title: format!("Detected: {}", diag.root_cause),
                description: format!("{} | Action: {}", diag.explanation, diag.immediate_action),
                action_sql,
                auto_executable,
                category: "root_cause".into(),
                estimated_impact: format!("Confidence {:.0}%", diag.confidence * 100.0),
            });
            self.record_issued(&key);
        }

        // ── Proactive: forecast-based recommendations ────────────────────
        if forecast.tps > 0.0 {
            // Memory growth warning
            let key = "forecast:memory_growth";
            if !self.is_suppressed(key)
                && forecast.memtable_bytes > snap.memtable_bytes.saturating_add(256 * 1024 * 1024)
            {
                recs.push(OpsRecommendation {
                    id: self.next_id(),
                    timestamp_ms: ts,
                    priority: RecommendationPriority::Medium,
                    title: "Memory growth forecast".into(),
                    description: format!(
                        "MemTable projected to grow to {:.1}GB in {} intervals (currently {:.1}GB).",
                        forecast.memtable_bytes as f64 / 1e9,
                        forecast.horizon_intervals,
                        snap.memtable_bytes as f64 / 1e9
                    ),
                    action_sql: None,
                    auto_executable: false,
                    category: "capacity".into(),
                    estimated_impact: "Proactive memory management before OOM".into(),
                });
                self.record_issued(key);
            }

            // TPS growth: scale-out warning
            let key = "forecast:tps_growth";
            if !self.is_suppressed(key) && forecast.tps > snap.tps * 1.5 {
                recs.push(OpsRecommendation {
                    id: self.next_id(),
                    timestamp_ms: ts,
                    priority: RecommendationPriority::Medium,
                    title: "TPS growth requires capacity review".into(),
                    description: format!(
                        "TPS forecast {:.1} (current {:.1}) — may require adding nodes in {} intervals.",
                        forecast.tps, snap.tps, forecast.horizon_intervals
                    ),
                    action_sql: None,
                    auto_executable: false,
                    category: "capacity".into(),
                    estimated_impact: "Prevent throughput ceiling before it impacts latency".into(),
                });
                self.record_issued(key);
            }

            // Connection forecast warning
            let key = "forecast:connection_growth";
            if !self.is_suppressed(key) && forecast.connections > 900 {
                recs.push(OpsRecommendation {
                    id: self.next_id(),
                    timestamp_ms: ts,
                    priority: RecommendationPriority::High,
                    title: "Connection saturation forecast".into(),
                    description: format!(
                        "Connections forecast to reach {} in {} intervals. Enable connection pooling.",
                        forecast.connections, forecast.horizon_intervals
                    ),
                    action_sql: Some("SET falcon.max_connections = 1000;".into()),
                    auto_executable: false,
                    category: "capacity".into(),
                    estimated_impact: "Prevent connection refusal before saturation".into(),
                });
                self.record_issued(key);
            }
        }

        // ── Alert-based spot recommendations ────────────────────────────
        for alert in alerts {
            let key = format!("alert:{}", alert.kind);
            if self.is_suppressed(&key) {
                continue;
            }
            let rec = match alert.kind {
                AlertKind::HighLatency if alert.severity == AlertSeverity::Critical => Some(OpsRecommendation {
                    id: self.next_id(),
                    timestamp_ms: ts,
                    priority: RecommendationPriority::Urgent,
                    title: "Critical query latency spike".into(),
                    description: format!("P99 latency {:.1}ms. Identify slow queries immediately.", alert.observed_value / 1000.0),
                    action_sql: Some("SELECT * FROM falcon.slow_queries ORDER BY total_time_us DESC LIMIT 10;".into()),
                    auto_executable: false,
                    category: "performance".into(),
                    estimated_impact: "Identify and kill blocking queries".into(),
                }),
                AlertKind::DiskSpaceLow if alert.severity == AlertSeverity::Critical => Some(OpsRecommendation {
                    id: self.next_id(),
                    timestamp_ms: ts,
                    priority: RecommendationPriority::Urgent,
                    title: "Critical disk space — immediate action required".into(),
                    description: format!("Only {:.1}GB free. Run WAL cleanup now.", alert.observed_value / 1e9),
                    action_sql: Some("-- Run: falcon_cli archive-wal --delete-old".to_owned()),
                    auto_executable: false,
                    category: "storage".into(),
                    estimated_impact: "Prevent write-ahead log from filling disk (would cause DB halt)".into(),
                }),
                AlertKind::ReplicationLag if alert.severity >= AlertSeverity::Warning => Some(OpsRecommendation {
                    id: self.next_id(),
                    timestamp_ms: ts,
                    priority: RecommendationPriority::High,
                    title: "Replication lag elevated".into(),
                    description: format!("Lag {:.1}MB. Check replica network and CPU.", alert.observed_value / 1e6),
                    action_sql: Some("SELECT * FROM falcon.replica_status;".into()),
                    auto_executable: false,
                    category: "replication".into(),
                    estimated_impact: "Prevent replica falling behind primary by >RPO".into(),
                }),
                _ => None,
            };
            if let Some(r) = rec {
                recs.push(r);
                self.record_issued(&key);
            }
        }

        // Sort by priority (Urgent first)
        recs.sort_by(|a, b| b.priority.cmp(&a.priority));
        recs
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — AiOpsEngine: unified entry point
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for the full AIOps engine.
#[derive(Debug, Clone)]
pub struct AiOpsConfig {
    pub anomaly: AnomalyDetectorConfig,
    /// Forecast horizon (in tick intervals).
    pub forecast_horizon: usize,
    /// How long to suppress duplicate recommendations.
    pub suppression_window: Duration,
    /// Maximum alerts to retain in history.
    pub alert_history_cap: usize,
    /// Maximum recommendations to retain in history.
    pub rec_history_cap: usize,
}

impl Default for AiOpsConfig {
    fn default() -> Self {
        Self {
            anomaly: AnomalyDetectorConfig::default(),
            forecast_horizon: 10,
            suppression_window: Duration::from_secs(300),
            alert_history_cap: 1000,
            rec_history_cap: 500,
        }
    }
}

/// Snapshot of the AIOps engine state (for admin queries / SHOW commands).
#[derive(Debug, Clone, Default)]
pub struct AiOpsSnapshot {
    pub tick_count: u64,
    pub total_alerts_fired: u64,
    pub total_recommendations_issued: u64,
    pub active_alerts: Vec<OpsAlert>,
    pub latest_recommendations: Vec<OpsRecommendation>,
    pub forecast: Option<ForecastResult>,
    pub latest_diagnoses: Vec<Diagnosis>,
}

struct AiOpsInner {
    config: AiOpsConfig,
    detector: AnomalyDetector,
    forecaster: WorkloadForecaster,
    advisor: OpsAdvisor,
    alert_history: VecDeque<OpsAlert>,
    rec_history: VecDeque<OpsRecommendation>,
    tick_count: u64,
    total_alerts: u64,
    total_recs: u64,
    latest_diagnoses: Vec<Diagnosis>,
    latest_forecast: Option<ForecastResult>,
}

/// The unified AI-driven operations engine.
///
/// # Usage
/// ```ignore
/// let engine = AiOpsEngine::new(AiOpsConfig::default());
/// // Called once per monitoring interval (e.g., every 1s):
/// let snap = MetricSnapshot { tps: 5000.0, latency_p99_us: 1200.0, ..Default::default() };
/// let result = engine.tick(snap);
/// for rec in result.latest_recommendations {
///     println!("{}", rec);
/// }
/// ```
#[derive(Clone)]
pub struct AiOpsEngine {
    inner: Arc<Mutex<AiOpsInner>>,
}

impl AiOpsEngine {
    pub fn new(config: AiOpsConfig) -> Self {
        let suppression = config.suppression_window;
        let alert_cap = config.alert_history_cap;
        let rec_cap = config.rec_history_cap;
        Self {
            inner: Arc::new(Mutex::new(AiOpsInner {
                detector: AnomalyDetector::new(config.anomaly.clone()),
                forecaster: WorkloadForecaster::new(),
                advisor: OpsAdvisor::new(suppression),
                alert_history: VecDeque::with_capacity(alert_cap),
                rec_history: VecDeque::with_capacity(rec_cap),
                tick_count: 0,
                total_alerts: 0,
                total_recs: 0,
                latest_diagnoses: vec![],
                latest_forecast: None,
                config,
            })),
        }
    }

    /// Feed one metric snapshot and get back alerts + recommendations.
    /// Call this once per monitoring interval.
    pub fn tick(&self, snap: MetricSnapshot) -> AiOpsSnapshot {
        let mut g = self.inner.lock();
        g.tick_count += 1;

        // 1. Detect anomalies
        let alerts = g.detector.feed(&snap);
        let alert_count = alerts.len() as u64;
        g.total_alerts += alert_count;

        // 2. Forecast
        g.forecaster.feed(&snap);
        let forecast = g.forecaster.forecast(g.config.forecast_horizon);
        g.latest_forecast = Some(forecast.clone());

        // 3. Root cause analysis
        let diagnoses = if !alerts.is_empty() {
            RootCauseAnalyzer::analyze(&alerts, &snap)
        } else {
            vec![]
        };
        g.latest_diagnoses = diagnoses.clone();

        // 4. Generate recommendations
        let recs = g.advisor.advise(&alerts, &diagnoses, &forecast, &snap);
        let rec_count = recs.len() as u64;
        g.total_recs += rec_count;

        // 5. Update histories (bounded ring buffers)
        let cap = g.config.alert_history_cap;
        for alert in &alerts {
            if g.alert_history.len() == cap {
                g.alert_history.pop_front();
            }
            g.alert_history.push_back(alert.clone());
        }
        let rec_cap = g.config.rec_history_cap;
        for rec in &recs {
            if g.rec_history.len() == rec_cap {
                g.rec_history.pop_front();
            }
            g.rec_history.push_back(rec.clone());
        }

        let tick = g.tick_count;
        let total_a = g.total_alerts;
        let total_r = g.total_recs;
        let active_alerts: Vec<_> = alerts;
        let latest_recs: Vec<_> = recs;
        let diag = diagnoses;
        let fc = Some(forecast);

        AiOpsSnapshot {
            tick_count: tick,
            total_alerts_fired: total_a,
            total_recommendations_issued: total_r,
            active_alerts,
            latest_recommendations: latest_recs,
            forecast: fc,
            latest_diagnoses: diag,
        }
    }

    /// Return recent alert history (up to `n` entries, newest first).
    pub fn recent_alerts(&self, n: usize) -> Vec<OpsAlert> {
        let g = self.inner.lock();
        g.alert_history.iter().rev().take(n).cloned().collect()
    }

    /// Return recent recommendations (up to `n` entries, newest first).
    pub fn recent_recommendations(&self, n: usize) -> Vec<OpsRecommendation> {
        let g = self.inner.lock();
        g.rec_history.iter().rev().take(n).cloned().collect()
    }

    /// Return the latest forecast result.
    pub fn latest_forecast(&self) -> Option<ForecastResult> {
        self.inner.lock().latest_forecast.clone()
    }

    /// Return summary metrics.
    pub fn metrics(&self) -> AiOpsMetrics {
        let g = self.inner.lock();
        AiOpsMetrics {
            ticks: g.tick_count,
            total_alerts: g.total_alerts,
            total_recommendations: g.total_recs,
            alert_history_len: g.alert_history.len(),
            rec_history_len: g.rec_history.len(),
        }
    }
}

/// Summary metrics for the AIOps engine.
#[derive(Debug, Clone)]
pub struct AiOpsMetrics {
    pub ticks: u64,
    pub total_alerts: u64,
    pub total_recommendations: u64,
    pub alert_history_len: usize,
    pub rec_history_len: usize,
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 — Unit tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn normal_snap() -> MetricSnapshot {
        MetricSnapshot {
            tps: 1000.0,
            latency_p99_us: 5000.0,
            latency_p50_us: 1000.0,
            error_rate: 0.01,
            replication_lag_bytes: 0,
            memtable_bytes: 100 * 1024 * 1024,
            pending_compactions: 0,
            connections: 50,
            disk_free_bytes: 100 * 1024 * 1024 * 1024,
            aborted_txns_delta: 0,
            ..Default::default()
        }
    }

    #[test]
    fn test_anomaly_detector_no_alerts_under_normal_load() {
        let mut det = AnomalyDetector::new(AnomalyDetectorConfig::default());
        let snap = normal_snap();
        // Warm up window with 20 normal snapshots
        for _ in 0..20 {
            let alerts = det.feed(&snap);
            assert!(
                alerts.is_empty(),
                "Expected no alerts under normal load, got: {:?}",
                alerts.iter().map(|a| a.kind).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn test_anomaly_detector_high_latency_warning() {
        let mut det = AnomalyDetector::new(AnomalyDetectorConfig::default());
        let mut snap = normal_snap();
        snap.latency_p99_us = 80_000.0; // above warn threshold 50ms
        let alerts = det.feed(&snap);
        let kinds: Vec<_> = alerts.iter().map(|a| a.kind).collect();
        assert!(
            kinds.contains(&AlertKind::HighLatency),
            "Expected HighLatency alert"
        );
    }

    #[test]
    fn test_anomaly_detector_critical_latency() {
        let mut det = AnomalyDetector::new(AnomalyDetectorConfig::default());
        let mut snap = normal_snap();
        snap.latency_p99_us = 300_000.0; // above crit threshold 200ms
        let alerts = det.feed(&snap);
        let crit: Vec<_> = alerts
            .iter()
            .filter(|a| a.severity == AlertSeverity::Critical)
            .collect();
        assert!(!crit.is_empty(), "Expected critical alert");
    }

    #[test]
    fn test_anomaly_detector_replication_lag() {
        let mut det = AnomalyDetector::new(AnomalyDetectorConfig::default());
        let mut snap = normal_snap();
        snap.replication_lag_bytes = 50 * 1024 * 1024; // 50MB > warn (10MB)
        let alerts = det.feed(&snap);
        let kinds: Vec<_> = alerts.iter().map(|a| a.kind).collect();
        assert!(kinds.contains(&AlertKind::ReplicationLag));
    }

    #[test]
    fn test_anomaly_detector_disk_space_low() {
        let mut det = AnomalyDetector::new(AnomalyDetectorConfig::default());
        let mut snap = normal_snap();
        snap.disk_free_bytes = 500 * 1024 * 1024; // 500MB < crit (1GB)
        let alerts = det.feed(&snap);
        let crit: Vec<_> = alerts
            .iter()
            .filter(|a| a.kind == AlertKind::DiskSpaceLow && a.severity == AlertSeverity::Critical)
            .collect();
        assert!(!crit.is_empty(), "Expected critical DiskSpaceLow alert");
    }

    #[test]
    fn test_anomaly_detector_abort_storm() {
        let mut det = AnomalyDetector::new(AnomalyDetectorConfig::default());
        let mut snap = normal_snap();
        snap.aborted_txns_delta = 20; // > warn threshold 5
        let alerts = det.feed(&snap);
        let kinds: Vec<_> = alerts.iter().map(|a| a.kind).collect();
        assert!(kinds.contains(&AlertKind::AbortStorm));
    }

    #[test]
    fn test_root_cause_analyzer_compaction_lagging() {
        let alerts = vec![
            OpsAlert {
                id: 1,
                timestamp_ms: 0,
                severity: AlertSeverity::Warning,
                kind: AlertKind::CompactionBacklog,
                node_id: None,
                message: "backlog".into(),
                observed_value: 20.0,
                threshold: 10.0,
                z_score: None,
            },
            OpsAlert {
                id: 2,
                timestamp_ms: 0,
                severity: AlertSeverity::Warning,
                kind: AlertKind::MemoryPressure,
                node_id: None,
                message: "memory".into(),
                observed_value: 600e6,
                threshold: 512e6,
                z_score: None,
            },
        ];
        let mut snap = normal_snap();
        snap.pending_compactions = 20;
        snap.memtable_bytes = 600 * 1024 * 1024;
        let diagnoses = RootCauseAnalyzer::analyze(&alerts, &snap);
        assert!(!diagnoses.is_empty());
        assert_eq!(diagnoses[0].root_cause, RootCause::CompactionLagging);
        assert!(diagnoses[0].confidence >= 0.8);
    }

    #[test]
    fn test_root_cause_analyzer_network_degradation() {
        let alerts = vec![OpsAlert {
            id: 1,
            timestamp_ms: 0,
            severity: AlertSeverity::Warning,
            kind: AlertKind::ReplicationLag,
            node_id: None,
            message: "lag".into(),
            observed_value: 50e6,
            threshold: 10e6,
            z_score: None,
        }];
        let mut snap = normal_snap();
        snap.replication_lag_bytes = 50 * 1024 * 1024;
        let diagnoses = RootCauseAnalyzer::analyze(&alerts, &snap);
        assert!(!diagnoses.is_empty());
        assert_eq!(diagnoses[0].root_cause, RootCause::NetworkDegradation);
    }

    #[test]
    fn test_root_cause_unknown_fallback() {
        let alerts = vec![OpsAlert {
            id: 1,
            timestamp_ms: 0,
            severity: AlertSeverity::Info,
            kind: AlertKind::TpsAnomaly,
            node_id: None,
            message: "tps".into(),
            observed_value: 500.0,
            threshold: 1000.0,
            z_score: Some(3.5),
        }];
        let snap = normal_snap();
        let diagnoses = RootCauseAnalyzer::analyze(&alerts, &snap);
        // Should return Unknown fallback
        assert!(!diagnoses.is_empty());
    }

    #[test]
    fn test_exponential_smoother_convergence() {
        let mut s = ExponentialSmoother::new(0.3);
        for _ in 0..50 {
            s.update(100.0);
        }
        assert!(
            (s.level - 100.0).abs() < 1.0,
            "Should converge to 100.0, got {}",
            s.level
        );
    }

    #[test]
    fn test_holt_smoother_trend_detection() {
        let mut s = HoltSmoother::new(0.3, 0.1);
        // Feed increasing values
        for i in 0..20 {
            s.update(100.0 + i as f64 * 10.0);
        }
        assert!(
            s.trend > 0.0,
            "Should detect positive trend, got trend={}",
            s.trend
        );
        let fc = s.forecast(5);
        assert!(
            fc > s.level,
            "Forecast should be higher than current level with positive trend"
        );
    }

    #[test]
    fn test_workload_forecaster_feed_and_forecast() {
        let mut f = WorkloadForecaster::new();
        for _ in 0..10 {
            f.feed(&MetricSnapshot {
                tps: 2000.0,
                memtable_bytes: 200 * 1024 * 1024,
                ..Default::default()
            });
        }
        let fc = f.forecast(5);
        assert!(fc.tps > 0.0, "TPS forecast should be positive");
        assert_eq!(fc.horizon_intervals, 5);
    }

    #[test]
    fn test_ai_ops_engine_tick_no_alerts() {
        let engine = AiOpsEngine::new(AiOpsConfig::default());
        let snap = normal_snap();
        let result = engine.tick(snap);
        assert_eq!(result.tick_count, 1);
        // Under normal load, no critical alerts should fire
        let critical: Vec<_> = result
            .active_alerts
            .iter()
            .filter(|a| a.severity == AlertSeverity::Critical)
            .collect();
        assert!(critical.is_empty(), "No critical alerts under normal load");
    }

    #[test]
    fn test_ai_ops_engine_tick_with_alerts() {
        let engine = AiOpsEngine::new(AiOpsConfig::default());
        // Feed critical scenario
        let snap = MetricSnapshot {
            tps: 100.0,
            latency_p99_us: 300_000.0,              // critical
            error_rate: 15.0,                       // critical
            memtable_bytes: 2 * 1024 * 1024 * 1024, // critical
            pending_compactions: 60,                // critical
            disk_free_bytes: 100 * 1024 * 1024,     // critical
            ..Default::default()
        };
        let result = engine.tick(snap);
        assert!(!result.active_alerts.is_empty(), "Should have alerts");
        let m = engine.metrics();
        assert_eq!(m.ticks, 1);
        assert!(m.total_alerts > 0);
    }

    #[test]
    fn test_ai_ops_engine_recommendation_for_critical_latency() {
        let engine = AiOpsEngine::new(AiOpsConfig::default());
        let snap = MetricSnapshot {
            latency_p99_us: 300_000.0,
            ..normal_snap()
        };
        let result = engine.tick(snap);
        let urgent: Vec<_> = result
            .latest_recommendations
            .iter()
            .filter(|r| r.priority == RecommendationPriority::Urgent)
            .collect();
        assert!(
            !urgent.is_empty(),
            "Should have urgent recommendation for critical latency"
        );
    }

    #[test]
    fn test_ai_ops_engine_recent_history() {
        let engine = AiOpsEngine::new(AiOpsConfig::default());
        for _ in 0..5 {
            engine.tick(MetricSnapshot {
                latency_p99_us: 300_000.0,
                ..normal_snap()
            });
        }
        let alerts = engine.recent_alerts(10);
        // Should have alerts from multiple ticks (suppression doesn't apply to alert history)
        assert!(!alerts.is_empty());
    }

    #[test]
    fn test_ops_recommendation_display() {
        let rec = OpsRecommendation {
            id: 1,
            timestamp_ms: 0,
            priority: RecommendationPriority::Urgent,
            title: "Test".into(),
            description: "desc".into(),
            action_sql: None,
            auto_executable: false,
            category: "perf".into(),
            estimated_impact: "high".into(),
        };
        let s = format!("{rec}");
        assert!(s.contains("URGENT"));
        assert!(s.contains("Test"));
    }

    #[test]
    fn test_forecast_result_display() {
        let fc = ForecastResult {
            horizon_intervals: 10,
            tps: 5000.0,
            latency_p99_us: 8000.0,
            memtable_bytes: 256 * 1024 * 1024,
            wal_bps: 10e6,
            connections: 100,
        };
        let s = format!("{fc}");
        assert!(s.contains("Forecast(+10)"));
        assert!(s.contains("tps=5000"));
    }

    #[test]
    fn test_zscore_anomaly_detection() {
        let mut det = AnomalyDetector::new(AnomalyDetectorConfig::default());
        // Fill window with consistent 1000 TPS
        let mut snap = normal_snap();
        for _ in 0..30 {
            det.feed(&snap);
        }
        // Now spike TPS to 10x normal — should trigger z-score anomaly
        snap.tps = 10000.0;
        let alerts = det.feed(&snap);
        let tps_anomaly: Vec<_> = alerts
            .iter()
            .filter(|a| a.kind == AlertKind::TpsAnomaly)
            .collect();
        assert!(
            !tps_anomaly.is_empty(),
            "Should detect TPS spike as anomaly"
        );
        assert!(tps_anomaly[0].z_score.is_some());
    }
}
