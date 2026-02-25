//! v1.1.1 §6–11: Cost Visibility, Capacity Guard v2, Audit Hardening,
//! Postmortem Ready, Admin Console v2, Change Impact Preview.
//!
//! **One-line**: Enterprise knows where money goes, can replay any incident,
//! and previews impact before every change.

use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use parking_lot::{Mutex, RwLock};

use falcon_common::types::{NodeId, TableId};

// ═══════════════════════════════════════════════════════════════════════════
// §6 — Cost Visibility
// ═══════════════════════════════════════════════════════════════════════════

/// Per-table cost breakdown.
#[derive(Debug, Clone)]
pub struct TableCost {
    pub table_id: TableId,
    pub table_name: String,
    /// Hot memory (active MemTable) in bytes.
    pub hot_memory_bytes: u64,
    /// Cold/compressed storage in bytes.
    pub cold_memory_bytes: u64,
    /// WAL IO bytes written since last reset.
    pub wal_io_bytes: u64,
    /// Replication traffic bytes sent.
    pub replication_bytes: u64,
    /// Row count.
    pub row_count: u64,
    /// Compression ratio (uncompressed / compressed). 1.0 = no compression.
    pub compression_ratio: f64,
    /// Compression savings in bytes.
    pub compression_savings_bytes: u64,
}

/// Per-shard cost breakdown.
#[derive(Debug, Clone)]
pub struct ShardCost {
    pub shard_id: u64,
    pub node_id: NodeId,
    pub hot_memory_bytes: u64,
    pub cold_memory_bytes: u64,
    pub wal_io_bytes: u64,
    pub replication_bytes: u64,
    pub table_count: u32,
    pub total_rows: u64,
}

/// Cluster-wide cost summary.
#[derive(Debug, Clone)]
pub struct ClusterCostSummary {
    pub timestamp: u64,
    pub total_hot_memory_bytes: u64,
    pub total_cold_memory_bytes: u64,
    pub total_wal_io_bytes: u64,
    pub total_replication_bytes: u64,
    pub total_rows: u64,
    pub avg_compression_ratio: f64,
    pub total_compression_savings: u64,
    pub table_costs: Vec<TableCost>,
    pub shard_costs: Vec<ShardCost>,
}

/// Cost tracker — aggregates per-table and per-shard cost data.
pub struct CostTracker {
    table_costs: RwLock<HashMap<TableId, TableCost>>,
    shard_costs: RwLock<HashMap<u64, ShardCost>>,
    /// Historical snapshots for trend analysis.
    history: Mutex<VecDeque<ClusterCostSummary>>,
    max_history: usize,
    pub metrics: CostTrackerMetrics,
}

#[derive(Debug, Default)]
pub struct CostTrackerMetrics {
    pub snapshots_taken: AtomicU64,
    pub tables_tracked: AtomicU64,
    pub shards_tracked: AtomicU64,
}

impl CostTracker {
    pub fn new(max_history: usize) -> Self {
        Self {
            table_costs: RwLock::new(HashMap::new()),
            shard_costs: RwLock::new(HashMap::new()),
            history: Mutex::new(VecDeque::with_capacity(max_history)),
            max_history,
            metrics: CostTrackerMetrics::default(),
        }
    }

    /// Update cost data for a table.
    pub fn update_table(&self, cost: TableCost) {
        self.table_costs.write().insert(cost.table_id, cost);
    }

    /// Update cost data for a shard.
    pub fn update_shard(&self, cost: ShardCost) {
        self.shard_costs.write().insert(cost.shard_id, cost);
    }

    /// Record WAL IO for a table.
    pub fn record_wal_io(&self, table_id: TableId, bytes: u64) {
        let mut costs = self.table_costs.write();
        if let Some(tc) = costs.get_mut(&table_id) {
            tc.wal_io_bytes += bytes;
        }
    }

    /// Record replication traffic for a shard.
    pub fn record_replication(&self, shard_id: u64, bytes: u64) {
        let mut costs = self.shard_costs.write();
        if let Some(sc) = costs.get_mut(&shard_id) {
            sc.replication_bytes += bytes;
        }
    }

    /// Generate a cluster cost summary.
    pub fn snapshot(&self) -> ClusterCostSummary {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let tables = self.table_costs.read();
        let shards = self.shard_costs.read();

        let table_costs: Vec<TableCost> = tables.values().cloned().collect();
        let shard_costs: Vec<ShardCost> = shards.values().cloned().collect();

        let total_hot: u64 = table_costs.iter().map(|t| t.hot_memory_bytes).sum();
        let total_cold: u64 = table_costs.iter().map(|t| t.cold_memory_bytes).sum();
        let total_wal: u64 = table_costs.iter().map(|t| t.wal_io_bytes).sum();
        let total_repl: u64 = shard_costs.iter().map(|s| s.replication_bytes).sum();
        let total_rows: u64 = table_costs.iter().map(|t| t.row_count).sum();
        let total_savings: u64 = table_costs.iter().map(|t| t.compression_savings_bytes).sum();

        let ratios: Vec<f64> = table_costs.iter()
            .filter(|t| t.compression_ratio > 0.0)
            .map(|t| t.compression_ratio)
            .collect();
        let avg_ratio = if ratios.is_empty() { 1.0 } else {
            ratios.iter().sum::<f64>() / ratios.len() as f64
        };

        let summary = ClusterCostSummary {
            timestamp: ts,
            total_hot_memory_bytes: total_hot,
            total_cold_memory_bytes: total_cold,
            total_wal_io_bytes: total_wal,
            total_replication_bytes: total_repl,
            total_rows,
            avg_compression_ratio: avg_ratio,
            total_compression_savings: total_savings,
            table_costs,
            shard_costs,
        };

        let mut hist = self.history.lock();
        if hist.len() >= self.max_history { hist.pop_front(); }
        hist.push_back(summary.clone());
        self.metrics.snapshots_taken.fetch_add(1, Ordering::Relaxed);

        summary
    }

    /// Get cost history.
    pub fn cost_history(&self, limit: usize) -> Vec<ClusterCostSummary> {
        self.history.lock().iter().rev().take(limit).cloned().collect()
    }

    /// Get top N tables by a cost metric.
    pub fn top_tables_by_hot_memory(&self, n: usize) -> Vec<TableCost> {
        let mut tables: Vec<TableCost> = self.table_costs.read().values().cloned().collect();
        tables.sort_by(|a, b| b.hot_memory_bytes.cmp(&a.hot_memory_bytes));
        tables.truncate(n);
        tables
    }

    /// Get table count.
    pub fn table_count(&self) -> usize {
        self.table_costs.read().len()
    }

    /// Get shard count.
    pub fn shard_count(&self) -> usize {
        self.shard_costs.read().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Capacity Guard & Forecast v2
// ═══════════════════════════════════════════════════════════════════════════

/// Pressure type detected by the capacity guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PressureType {
    MemoryApproaching,
    WalGrowthTooFast,
    ColdSegmentBloat,
    ShardImbalance,
    ConnectionSaturation,
}

impl fmt::Display for PressureType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MemoryApproaching => write!(f, "MEMORY_APPROACHING"),
            Self::WalGrowthTooFast => write!(f, "WAL_GROWTH_FAST"),
            Self::ColdSegmentBloat => write!(f, "COLD_BLOAT"),
            Self::ShardImbalance => write!(f, "SHARD_IMBALANCE"),
            Self::ConnectionSaturation => write!(f, "CONN_SATURATION"),
        }
    }
}

/// Recommendation type from the capacity guard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recommendation {
    ScaleOut { add_nodes: u32, reason: String },
    IncreaseCompression { current_profile: String, suggested_profile: String },
    WalGc { suggested_gc_threshold_mb: u64 },
    ColdCompaction { estimated_savings_mb: u64 },
    ShardRebalance { overloaded_nodes: Vec<NodeId> },
    ConnectionPoolResize { current: u32, suggested: u32 },
}

impl fmt::Display for Recommendation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ScaleOut { add_nodes, reason } =>
                write!(f, "SCALE_OUT(+{add_nodes} nodes: {reason})"),
            Self::IncreaseCompression { suggested_profile, .. } =>
                write!(f, "COMPRESS(→{suggested_profile})"),
            Self::WalGc { suggested_gc_threshold_mb } =>
                write!(f, "WAL_GC(threshold={suggested_gc_threshold_mb}MB)"),
            Self::ColdCompaction { estimated_savings_mb } =>
                write!(f, "COLD_COMPACT(save ~{estimated_savings_mb}MB)"),
            Self::ShardRebalance { overloaded_nodes } =>
                write!(f, "REBALANCE(overloaded={overloaded_nodes:?})"),
            Self::ConnectionPoolResize { suggested, .. } =>
                write!(f, "RESIZE_POOL(→{suggested})"),
        }
    }
}

/// Capacity guard alert.
#[derive(Debug, Clone)]
pub struct CapacityGuardAlert {
    pub id: u64,
    pub pressure: PressureType,
    pub severity: CapacityGuardSeverity,
    pub recommendation: Recommendation,
    pub message: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CapacityGuardSeverity {
    Advisory,
    Warning,
    Urgent,
}

impl fmt::Display for CapacityGuardSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Advisory => write!(f, "ADVISORY"),
            Self::Warning => write!(f, "WARNING"),
            Self::Urgent => write!(f, "URGENT"),
        }
    }
}

/// Capacity guard v2 — detects pressure and generates recommendations.
pub struct CapacityGuardV2 {
    /// Thresholds: resource → (warning_pct, urgent_pct)
    thresholds: RwLock<HashMap<PressureType, (f64, f64)>>,
    alerts: Mutex<VecDeque<CapacityGuardAlert>>,
    next_alert_id: AtomicU64,
    max_alerts: usize,
    pub metrics: CapacityGuardMetrics,
}

#[derive(Debug, Default)]
pub struct CapacityGuardMetrics {
    pub evaluations: AtomicU64,
    pub alerts_generated: AtomicU64,
    pub recommendations_generated: AtomicU64,
}

impl CapacityGuardV2 {
    pub fn new(max_alerts: usize) -> Self {
        let mut thresholds = HashMap::new();
        thresholds.insert(PressureType::MemoryApproaching, (0.75, 0.90));
        thresholds.insert(PressureType::WalGrowthTooFast, (0.60, 0.85));
        thresholds.insert(PressureType::ColdSegmentBloat, (0.70, 0.90));
        thresholds.insert(PressureType::ConnectionSaturation, (0.80, 0.95));

        Self {
            thresholds: RwLock::new(thresholds),
            alerts: Mutex::new(VecDeque::with_capacity(max_alerts)),
            next_alert_id: AtomicU64::new(1),
            max_alerts,
            metrics: CapacityGuardMetrics::default(),
        }
    }

    /// Set threshold for a pressure type.
    pub fn set_threshold(&self, pressure: PressureType, warning_pct: f64, urgent_pct: f64) {
        self.thresholds.write().insert(pressure, (warning_pct, urgent_pct));
    }

    /// Evaluate memory pressure.
    pub fn evaluate_memory(&self, used_bytes: u64, total_bytes: u64) -> Option<CapacityGuardAlert> {
        self.metrics.evaluations.fetch_add(1, Ordering::Relaxed);
        if total_bytes == 0 { return None; }
        let ratio = used_bytes as f64 / total_bytes as f64;
        let thresholds = self.thresholds.read();
        let (warn, urgent) = thresholds.get(&PressureType::MemoryApproaching).copied().unwrap_or((0.75, 0.90));

        if ratio >= urgent {
            Some(self.create_alert(
                PressureType::MemoryApproaching,
                CapacityGuardSeverity::Urgent,
                Recommendation::ScaleOut {
                    add_nodes: 1,
                    reason: format!("Memory at {:.1}%", ratio * 100.0),
                },
                format!("Memory usage {:.1}% — urgent", ratio * 100.0),
            ))
        } else if ratio >= warn {
            Some(self.create_alert(
                PressureType::MemoryApproaching,
                CapacityGuardSeverity::Warning,
                Recommendation::IncreaseCompression {
                    current_profile: "balanced".into(),
                    suggested_profile: "aggressive".into(),
                },
                format!("Memory usage {:.1}% — warning", ratio * 100.0),
            ))
        } else {
            None
        }
    }

    /// Evaluate WAL growth rate.
    pub fn evaluate_wal_growth(&self, growth_bytes_per_hour: u64, max_wal_bytes: u64) -> Option<CapacityGuardAlert> {
        self.metrics.evaluations.fetch_add(1, Ordering::Relaxed);
        if max_wal_bytes == 0 { return None; }
        let hours_to_fill = if growth_bytes_per_hour > 0 {
            max_wal_bytes as f64 / growth_bytes_per_hour as f64
        } else {
            f64::INFINITY
        };

        if hours_to_fill < 6.0 {
            Some(self.create_alert(
                PressureType::WalGrowthTooFast,
                CapacityGuardSeverity::Urgent,
                Recommendation::WalGc { suggested_gc_threshold_mb: max_wal_bytes / 2 / (1024 * 1024) },
                format!("WAL fills in {hours_to_fill:.1}h at current rate"),
            ))
        } else if hours_to_fill < 24.0 {
            Some(self.create_alert(
                PressureType::WalGrowthTooFast,
                CapacityGuardSeverity::Warning,
                Recommendation::WalGc { suggested_gc_threshold_mb: max_wal_bytes / 3 / (1024 * 1024) },
                format!("WAL fills in {hours_to_fill:.1}h at current rate"),
            ))
        } else {
            None
        }
    }

    /// Evaluate cold segment bloat.
    pub fn evaluate_cold_bloat(&self, cold_bytes: u64, uncompressed_bytes: u64) -> Option<CapacityGuardAlert> {
        self.metrics.evaluations.fetch_add(1, Ordering::Relaxed);
        if uncompressed_bytes == 0 { return None; }
        let ratio = cold_bytes as f64 / uncompressed_bytes as f64;
        // If compression ratio is poor (> 0.8 = less than 20% savings)
        if ratio > 0.8 {
            let _savings = uncompressed_bytes.saturating_sub(cold_bytes) / (1024 * 1024);
            Some(self.create_alert(
                PressureType::ColdSegmentBloat,
                CapacityGuardSeverity::Advisory,
                Recommendation::ColdCompaction { estimated_savings_mb: (uncompressed_bytes / 5) / (1024 * 1024) },
                format!("Cold compression ratio {ratio:.2} — consider recompaction"),
            ))
        } else {
            None
        }
    }

    fn create_alert(
        &self,
        pressure: PressureType,
        severity: CapacityGuardSeverity,
        recommendation: Recommendation,
        message: String,
    ) -> CapacityGuardAlert {
        let id = self.next_alert_id.fetch_add(1, Ordering::Relaxed);
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.metrics.alerts_generated.fetch_add(1, Ordering::Relaxed);
        self.metrics.recommendations_generated.fetch_add(1, Ordering::Relaxed);
        let alert = CapacityGuardAlert { id, pressure, severity, recommendation, message, timestamp: ts };
        let mut alerts = self.alerts.lock();
        if alerts.len() >= self.max_alerts { alerts.pop_front(); }
        alerts.push_back(alert.clone());
        alert
    }

    /// Get recent alerts.
    pub fn recent_alerts(&self, count: usize) -> Vec<CapacityGuardAlert> {
        self.alerts.lock().iter().rev().take(count).cloned().collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 — Full-Stack Audit Log Hardening
// ═══════════════════════════════════════════════════════════════════════════

/// Unified audit event with trace ID.
#[derive(Debug, Clone)]
pub struct UnifiedAuditEvent {
    pub id: u64,
    pub timestamp: u64,
    pub trace_id: String,
    pub span_id: String,
    pub category: String,
    pub severity: String,
    pub actor: String,
    pub source_ip: String,
    pub action: String,
    pub resource: String,
    pub outcome: String,
    pub details: String,
    pub component: String,
    pub node_id: Option<NodeId>,
    pub shard_id: Option<u64>,
    pub duration_us: Option<u64>,
}

/// Hardened audit log with trace correlation.
pub struct HardenedAuditLog {
    events: Mutex<VecDeque<UnifiedAuditEvent>>,
    capacity: usize,
    next_id: AtomicU64,
    pub metrics: HardenedAuditMetrics,
}

#[derive(Debug, Default)]
pub struct HardenedAuditMetrics {
    pub events_recorded: AtomicU64,
    pub events_exported: AtomicU64,
    pub trace_correlations: AtomicU64,
}

impl HardenedAuditLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            events: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            next_id: AtomicU64::new(1),
            metrics: HardenedAuditMetrics::default(),
        }
    }

    /// Record an audit event with full trace context.
    pub fn record(&self, event: UnifiedAuditEvent) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut events = self.events.lock();
        let mut ev = event;
        ev.id = id;
        if ev.timestamp == 0 {
            ev.timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }
        if events.len() >= self.capacity { events.pop_front(); }
        events.push_back(ev);
        self.metrics.events_recorded.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Query events by trace ID.
    pub fn query_by_trace(&self, trace_id: &str) -> Vec<UnifiedAuditEvent> {
        self.metrics.trace_correlations.fetch_add(1, Ordering::Relaxed);
        self.events.lock().iter()
            .filter(|e| e.trace_id == trace_id)
            .cloned()
            .collect()
    }

    /// Export events as SIEM-compatible JSON lines.
    pub fn export_jsonl(&self, since_id: u64) -> Vec<String> {
        let events = self.events.lock();
        let mut lines = Vec::new();
        for e in events.iter().filter(|e| e.id >= since_id) {
            lines.push(format!(
                "{{\"id\":{},\"ts\":{},\"trace\":\"{}\",\"span\":\"{}\",\"cat\":\"{}\",\"sev\":\"{}\",\"actor\":\"{}\",\"ip\":\"{}\",\"action\":\"{}\",\"resource\":\"{}\",\"outcome\":\"{}\",\"component\":\"{}\",\"duration_us\":{}}}",
                e.id, e.timestamp, e.trace_id, e.span_id, e.category, e.severity,
                e.actor, e.source_ip, e.action, e.resource, e.outcome, e.component,
                e.duration_us.unwrap_or(0)
            ));
            self.metrics.events_exported.fetch_add(1, Ordering::Relaxed);
        }
        lines
    }

    /// Get total event count.
    pub fn total(&self) -> u64 {
        self.metrics.events_recorded.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §9 — Postmortem Ready
// ═══════════════════════════════════════════════════════════════════════════

/// A postmortem-ready decision point — records why a system action was taken.
#[derive(Debug, Clone)]
pub struct DecisionPoint {
    pub id: u64,
    pub timestamp: u64,
    pub decision: String,
    pub reason: String,
    pub trigger_metric: String,
    pub trigger_value: f64,
    pub threshold: f64,
    pub action_taken: String,
    pub outcome: String,
}

/// Key metric change during an incident window.
#[derive(Debug, Clone)]
pub struct MetricChange {
    pub metric_name: String,
    pub before: f64,
    pub after: f64,
    pub delta_pct: f64,
    pub timestamp: u64,
}

/// Postmortem report — auto-generated for incident replay.
#[derive(Debug, Clone)]
pub struct PostmortemReport {
    pub incident_id: u64,
    pub title: String,
    pub generated_at: u64,
    pub window_start: u64,
    pub window_end: u64,
    pub timeline: Vec<PostmortemTimelineEntry>,
    pub metric_changes: Vec<MetricChange>,
    pub decision_points: Vec<DecisionPoint>,
    pub root_cause: Option<String>,
    pub impact_summary: Option<String>,
    pub recommendations: Vec<String>,
}

/// A single entry in the postmortem timeline.
#[derive(Debug, Clone)]
pub struct PostmortemTimelineEntry {
    pub timestamp: u64,
    pub event_type: String,
    pub description: String,
    pub severity: String,
}

/// Postmortem generator — collects events, metrics, and decisions.
pub struct PostmortemGenerator {
    decision_points: Mutex<VecDeque<DecisionPoint>>,
    metric_snapshots: RwLock<HashMap<String, VecDeque<(u64, f64)>>>,
    next_decision_id: AtomicU64,
    max_decisions: usize,
    max_metric_samples: usize,
    pub metrics: PostmortemMetrics,
}

#[derive(Debug, Default)]
pub struct PostmortemMetrics {
    pub decisions_recorded: AtomicU64,
    pub reports_generated: AtomicU64,
    pub metric_samples: AtomicU64,
}

impl PostmortemGenerator {
    pub fn new(max_decisions: usize, max_metric_samples: usize) -> Self {
        Self {
            decision_points: Mutex::new(VecDeque::with_capacity(max_decisions)),
            metric_snapshots: RwLock::new(HashMap::new()),
            next_decision_id: AtomicU64::new(1),
            max_decisions,
            max_metric_samples,
            metrics: PostmortemMetrics::default(),
        }
    }

    /// Record a decision point.
    pub fn record_decision(
        &self,
        decision: &str,
        reason: &str,
        trigger_metric: &str,
        trigger_value: f64,
        threshold: f64,
        action_taken: &str,
    ) -> u64 {
        let id = self.next_decision_id.fetch_add(1, Ordering::Relaxed);
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let dp = DecisionPoint {
            id, timestamp: ts,
            decision: decision.to_owned(),
            reason: reason.to_owned(),
            trigger_metric: trigger_metric.to_owned(),
            trigger_value, threshold,
            action_taken: action_taken.to_owned(),
            outcome: String::new(),
        };
        let mut dps = self.decision_points.lock();
        if dps.len() >= self.max_decisions { dps.pop_front(); }
        dps.push_back(dp);
        self.metrics.decisions_recorded.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Record a decision point with explicit timestamp.
    #[allow(clippy::too_many_arguments)]
    pub fn record_decision_at(
        &self,
        decision: &str,
        reason: &str,
        trigger_metric: &str,
        trigger_value: f64,
        threshold: f64,
        action_taken: &str,
        timestamp: u64,
    ) -> u64 {
        let id = self.next_decision_id.fetch_add(1, Ordering::Relaxed);
        let dp = DecisionPoint {
            id, timestamp,
            decision: decision.to_owned(),
            reason: reason.to_owned(),
            trigger_metric: trigger_metric.to_owned(),
            trigger_value, threshold,
            action_taken: action_taken.to_owned(),
            outcome: String::new(),
        };
        let mut dps = self.decision_points.lock();
        if dps.len() >= self.max_decisions { dps.pop_front(); }
        dps.push_back(dp);
        self.metrics.decisions_recorded.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Update a decision's outcome.
    pub fn update_outcome(&self, decision_id: u64, outcome: &str) {
        let mut dps = self.decision_points.lock();
        if let Some(dp) = dps.iter_mut().find(|d| d.id == decision_id) {
            dp.outcome = outcome.to_owned();
        }
    }

    /// Record a metric sample.
    pub fn record_metric(&self, name: &str, value: f64) {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.record_metric_at(name, value, ts);
    }

    /// Record a metric sample with explicit timestamp.
    pub fn record_metric_at(&self, name: &str, value: f64, timestamp: u64) {
        let mut snaps = self.metric_snapshots.write();
        let q = snaps.entry(name.to_owned())
            .or_insert_with(|| VecDeque::with_capacity(self.max_metric_samples));
        if q.len() >= self.max_metric_samples { q.pop_front(); }
        q.push_back((timestamp, value));
        self.metrics.metric_samples.fetch_add(1, Ordering::Relaxed);
    }

    /// Generate a postmortem report for a time window.
    pub fn generate_report(
        &self,
        incident_id: u64,
        title: &str,
        window_start: u64,
        window_end: u64,
        timeline_entries: Vec<PostmortemTimelineEntry>,
    ) -> PostmortemReport {
        self.metrics.reports_generated.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Collect decision points in window
        let decisions: Vec<DecisionPoint> = self.decision_points.lock().iter()
            .filter(|d| d.timestamp >= window_start && d.timestamp <= window_end)
            .cloned()
            .collect();

        // Compute metric changes
        let snaps = self.metric_snapshots.read();
        let mut metric_changes = Vec::new();
        for (name, samples) in snaps.iter() {
            let before_samples: Vec<f64> = samples.iter()
                .filter(|(ts, _)| *ts < window_start)
                .map(|(_, v)| *v)
                .collect();
            let after_samples: Vec<f64> = samples.iter()
                .filter(|(ts, _)| *ts >= window_start && *ts <= window_end)
                .map(|(_, v)| *v)
                .collect();
            if !before_samples.is_empty() && !after_samples.is_empty() {
                let before = before_samples.iter().sum::<f64>() / before_samples.len() as f64;
                let after = after_samples.iter().sum::<f64>() / after_samples.len() as f64;
                let delta = if before.abs() > 1e-10 { (after - before) / before * 100.0 } else { 0.0 };
                metric_changes.push(MetricChange {
                    metric_name: name.clone(),
                    before, after, delta_pct: delta,
                    timestamp: window_start,
                });
            }
        }

        PostmortemReport {
            incident_id,
            title: title.to_owned(),
            generated_at: now,
            window_start,
            window_end,
            timeline: timeline_entries,
            metric_changes,
            decision_points: decisions,
            root_cause: None,
            impact_summary: None,
            recommendations: Vec::new(),
        }
    }

    /// Export a report as formatted text (for `falconctl postmortem export`).
    pub fn export_text(report: &PostmortemReport) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "=== POSTMORTEM: {} ===", report.title);
        let _ = writeln!(out, "Incident ID: {}", report.incident_id);
        let _ = writeln!(out, "Window: {} — {}", report.window_start, report.window_end);
        let _ = writeln!(out, "Generated: {}\n", report.generated_at);

        out.push_str("--- TIMELINE ---\n");
        for entry in &report.timeline {
            let _ = writeln!(out, "[{}] [{}] {} — {}",
                entry.timestamp, entry.severity, entry.event_type, entry.description);
        }

        out.push_str("\n--- METRIC CHANGES ---\n");
        for mc in &report.metric_changes {
            let _ = writeln!(out, "{}: {:.2} → {:.2} ({:+.1}%)",
                mc.metric_name, mc.before, mc.after, mc.delta_pct);
        }

        out.push_str("\n--- DECISION POINTS ---\n");
        for dp in &report.decision_points {
            let _ = writeln!(out, "[{}] {}: {} (trigger: {}={:.2} > {:.2}) → {}",
                dp.timestamp, dp.decision, dp.reason,
                dp.trigger_metric, dp.trigger_value, dp.threshold,
                dp.action_taken);
            if !dp.outcome.is_empty() {
                let _ = writeln!(out, "  Outcome: {}", dp.outcome);
            }
        }

        if let Some(ref rc) = report.root_cause {
            let _ = write!(out, "\n--- ROOT CAUSE ---\n{rc}\n");
        }
        if let Some(ref impact) = report.impact_summary {
            let _ = write!(out, "\n--- IMPACT ---\n{impact}\n");
        }
        out
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §10 — Admin Console v2
// ═══════════════════════════════════════════════════════════════════════════

/// Admin Console v2 endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminV2Endpoint {
    /// Real-time SLO dashboard.
    SloDashboard,
    /// Cost breakdown view.
    CostView,
    /// Capacity / forecast view.
    CapacityView,
    /// Compression effectiveness.
    CompressionView,
    /// Event timeline.
    EventTimeline,
    /// Latency guardrail status.
    GuardrailStatus,
    /// Resource leak status.
    LeakStatus,
    /// Background task isolation.
    BgTaskStatus,
    /// Postmortem reports.
    PostmortemList,
    /// Change impact preview.
    ChangeImpact,
}

impl fmt::Display for AdminV2Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SloDashboard => write!(f, "/admin/v2/slo"),
            Self::CostView => write!(f, "/admin/v2/cost"),
            Self::CapacityView => write!(f, "/admin/v2/capacity"),
            Self::CompressionView => write!(f, "/admin/v2/compression"),
            Self::EventTimeline => write!(f, "/admin/v2/timeline"),
            Self::GuardrailStatus => write!(f, "/admin/v2/guardrails"),
            Self::LeakStatus => write!(f, "/admin/v2/leaks"),
            Self::BgTaskStatus => write!(f, "/admin/v2/bgtasks"),
            Self::PostmortemList => write!(f, "/admin/v2/postmortem"),
            Self::ChangeImpact => write!(f, "/admin/v2/impact"),
        }
    }
}

/// Admin Console v2 router.
pub struct AdminConsoleV2 {
    endpoints: Vec<AdminV2Endpoint>,
}

impl Default for AdminConsoleV2 {
    fn default() -> Self {
        Self::new()
    }
}

impl AdminConsoleV2 {
    pub fn new() -> Self {
        Self {
            endpoints: vec![
                AdminV2Endpoint::SloDashboard,
                AdminV2Endpoint::CostView,
                AdminV2Endpoint::CapacityView,
                AdminV2Endpoint::CompressionView,
                AdminV2Endpoint::EventTimeline,
                AdminV2Endpoint::GuardrailStatus,
                AdminV2Endpoint::LeakStatus,
                AdminV2Endpoint::BgTaskStatus,
                AdminV2Endpoint::PostmortemList,
                AdminV2Endpoint::ChangeImpact,
            ],
        }
    }

    pub fn endpoints(&self) -> &[AdminV2Endpoint] {
        &self.endpoints
    }

    pub const fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §11 — Change Impact Preview
// ═══════════════════════════════════════════════════════════════════════════

/// A proposed change to the system.
#[derive(Debug, Clone)]
pub enum ProposedChange {
    IncreaseCompression { from: String, to: String },
    AddNode { node_count: u32 },
    RemoveNode { node_id: NodeId },
    ChangeReplicationFactor { from: u32, to: u32 },
    ResizeConnectionPool { from: u32, to: u32 },
    AdjustWalGc { new_threshold_mb: u64 },
    EnableColdCompaction,
}

impl fmt::Display for ProposedChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncreaseCompression { from, to } =>
                write!(f, "compression: {from} → {to}"),
            Self::AddNode { node_count } =>
                write!(f, "add {node_count} node(s)"),
            Self::RemoveNode { node_id } =>
                write!(f, "remove node {node_id:?}"),
            Self::ChangeReplicationFactor { from, to } =>
                write!(f, "replication: {from} → {to}"),
            Self::ResizeConnectionPool { from, to } =>
                write!(f, "pool: {from} → {to}"),
            Self::AdjustWalGc { new_threshold_mb } =>
                write!(f, "WAL GC threshold → {new_threshold_mb}MB"),
            Self::EnableColdCompaction =>
                write!(f, "enable cold compaction"),
        }
    }
}

/// Impact estimate for a proposed change.
#[derive(Debug, Clone)]
pub struct ImpactEstimate {
    pub change: String,
    /// CPU impact as a delta percentage (e.g., +5.0 means +5% CPU).
    pub cpu_delta_pct: f64,
    /// Memory impact in bytes (positive = increase).
    pub memory_delta_bytes: i64,
    /// Storage impact in bytes.
    pub storage_delta_bytes: i64,
    /// Latency p99 impact in microseconds (positive = slower).
    pub latency_p99_delta_us: i64,
    /// Whether guardrails remain satisfied.
    pub guardrail_ok: bool,
    /// Human-readable summary.
    pub summary: String,
    /// Risk level.
    pub risk: ImpactRisk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImpactRisk {
    Low,
    Medium,
    High,
}

impl fmt::Display for ImpactRisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => write!(f, "LOW"),
            Self::Medium => write!(f, "MEDIUM"),
            Self::High => write!(f, "HIGH"),
        }
    }
}

/// Change impact previewer.
pub struct ChangeImpactPreview {
    pub metrics: ChangeImpactMetrics,
}

#[derive(Debug, Default)]
pub struct ChangeImpactMetrics {
    pub previews_computed: AtomicU64,
}

impl Default for ChangeImpactPreview {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangeImpactPreview {
    pub fn new() -> Self {
        Self { metrics: ChangeImpactMetrics::default() }
    }

    /// Estimate the impact of a proposed change.
    pub fn preview(&self, change: &ProposedChange) -> ImpactEstimate {
        self.metrics.previews_computed.fetch_add(1, Ordering::Relaxed);
        match change {
            ProposedChange::IncreaseCompression { from, to } => {
                ImpactEstimate {
                    change: change.to_string(),
                    cpu_delta_pct: 5.0,
                    memory_delta_bytes: -i64::from(1024 * 1024 * 100), // -100MB cold
                    storage_delta_bytes: -i64::from(1024 * 1024 * 500),
                    latency_p99_delta_us: 200, // slight increase on read
                    guardrail_ok: true,
                    summary: format!(
                        "Compression {from} → {to}: +5% CPU (compactor), -100MB cold memory, -500MB storage, p99 +0.2ms (guardrail OK)"
                    ),
                    risk: ImpactRisk::Low,
                }
            }
            ProposedChange::AddNode { node_count } => {
                ImpactEstimate {
                    change: change.to_string(),
                    cpu_delta_pct: 0.0,
                    memory_delta_bytes: 0,
                    storage_delta_bytes: 0,
                    latency_p99_delta_us: -500, // improvement from lower per-node load
                    guardrail_ok: true,
                    summary: format!(
                        "Add {node_count} node(s): triggers shard rebalance, ~10min migration, p99 improves ~0.5ms"
                    ),
                    risk: ImpactRisk::Medium,
                }
            }
            ProposedChange::RemoveNode { node_id } => {
                ImpactEstimate {
                    change: change.to_string(),
                    cpu_delta_pct: 10.0,
                    memory_delta_bytes: 0,
                    storage_delta_bytes: 0,
                    latency_p99_delta_us: 1000,
                    guardrail_ok: false, // removing node may breach guardrails
                    summary: format!(
                        "Remove {node_id:?}: +10% CPU on remaining, p99 +1ms, guardrail may breach during migration"
                    ),
                    risk: ImpactRisk::High,
                }
            }
            ProposedChange::ChangeReplicationFactor { from, to } => {
                let repl_delta = i64::from(*to) - i64::from(*from);
                ImpactEstimate {
                    change: change.to_string(),
                    cpu_delta_pct: repl_delta as f64 * 3.0,
                    memory_delta_bytes: repl_delta * 50 * 1024 * 1024,
                    storage_delta_bytes: repl_delta * 200 * 1024 * 1024,
                    latency_p99_delta_us: if repl_delta > 0 { 300 } else { -200 },
                    guardrail_ok: *to <= 5,
                    summary: format!(
                        "Replication {} → {}: {}% CPU, {}MB memory, p99 {}us",
                        from, to,
                        repl_delta * 3,
                        repl_delta * 50,
                        if repl_delta > 0 { "+300" } else { "-200" }
                    ),
                    risk: if repl_delta.abs() > 1 { ImpactRisk::High } else { ImpactRisk::Medium },
                }
            }
            ProposedChange::ResizeConnectionPool { from, to } => {
                ImpactEstimate {
                    change: change.to_string(),
                    cpu_delta_pct: 0.0,
                    memory_delta_bytes: (i64::from(*to) - i64::from(*from)) * 2 * 1024 * 1024,
                    storage_delta_bytes: 0,
                    latency_p99_delta_us: 0,
                    guardrail_ok: true,
                    summary: format!("Pool {} → {}: {}MB memory delta", from, to,
                        (i64::from(*to) - i64::from(*from)) * 2),
                    risk: ImpactRisk::Low,
                }
            }
            ProposedChange::AdjustWalGc { new_threshold_mb } => {
                ImpactEstimate {
                    change: change.to_string(),
                    cpu_delta_pct: 1.0,
                    memory_delta_bytes: 0,
                    storage_delta_bytes: -(*new_threshold_mb as i64 * 1024 * 1024 / 2),
                    latency_p99_delta_us: 0,
                    guardrail_ok: true,
                    summary: format!("WAL GC at {}MB: +1% CPU (GC), recovers ~{}MB", new_threshold_mb, new_threshold_mb / 2),
                    risk: ImpactRisk::Low,
                }
            }
            ProposedChange::EnableColdCompaction => {
                ImpactEstimate {
                    change: change.to_string(),
                    cpu_delta_pct: 3.0,
                    memory_delta_bytes: -(50 * 1024 * 1024),
                    storage_delta_bytes: -(200 * 1024 * 1024),
                    latency_p99_delta_us: 100,
                    guardrail_ok: true,
                    summary: "Cold compaction: +3% CPU, -50MB memory, -200MB storage, p99 +0.1ms".into(),
                    risk: ImpactRisk::Low,
                }
            }
        }
    }

    /// Format impact for CLI display.
    pub fn format_impact(estimate: &ImpactEstimate) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "Change: {}", estimate.change);
        out.push_str("Impact:\n");
        let _ = writeln!(out, "  CPU:     {:+.1}%", estimate.cpu_delta_pct);
        let _ = writeln!(out, "  Memory:  {:+}B", estimate.memory_delta_bytes);
        let _ = writeln!(out, "  Storage: {:+}B", estimate.storage_delta_bytes);
        let _ = writeln!(out, "  p99:     {:+}us", estimate.latency_p99_delta_us);
        let _ = writeln!(out, "  Guardrail: {}", if estimate.guardrail_ok { "OK" } else { "AT RISK" });
        let _ = writeln!(out, "  Risk:    {}", estimate.risk);
        let _ = writeln!(out, "  Summary: {}", estimate.summary);
        out
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §12 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // -- Cost Tracker Tests --

    #[test]
    fn test_cost_tracker_basic() {
        let tracker = CostTracker::new(100);
        tracker.update_table(TableCost {
            table_id: TableId(1),
            table_name: "users".into(),
            hot_memory_bytes: 10_000_000,
            cold_memory_bytes: 50_000_000,
            wal_io_bytes: 5_000_000,
            replication_bytes: 0,
            row_count: 100_000,
            compression_ratio: 3.0,
            compression_savings_bytes: 100_000_000,
        });
        tracker.update_table(TableCost {
            table_id: TableId(2),
            table_name: "orders".into(),
            hot_memory_bytes: 20_000_000,
            cold_memory_bytes: 100_000_000,
            wal_io_bytes: 10_000_000,
            replication_bytes: 0,
            row_count: 500_000,
            compression_ratio: 2.5,
            compression_savings_bytes: 150_000_000,
        });
        let summary = tracker.snapshot();
        assert_eq!(summary.table_costs.len(), 2);
        assert_eq!(summary.total_hot_memory_bytes, 30_000_000);
        assert_eq!(summary.total_rows, 600_000);
        assert!(summary.avg_compression_ratio > 2.0);
    }

    #[test]
    fn test_cost_top_tables() {
        let tracker = CostTracker::new(100);
        for i in 0..10u64 {
            tracker.update_table(TableCost {
                table_id: TableId(i),
                table_name: format!("table_{}", i),
                hot_memory_bytes: (i + 1) * 1_000_000,
                cold_memory_bytes: 0, wal_io_bytes: 0, replication_bytes: 0,
                row_count: 0, compression_ratio: 1.0, compression_savings_bytes: 0,
            });
        }
        let top = tracker.top_tables_by_hot_memory(3);
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].table_id, TableId(9)); // highest
    }

    // -- Capacity Guard v2 Tests --

    #[test]
    fn test_capacity_guard_memory_warning() {
        let guard = CapacityGuardV2::new(100);
        let alert = guard.evaluate_memory(800_000_000, 1_000_000_000); // 80%
        assert!(alert.is_some());
        let a = alert.unwrap();
        assert_eq!(a.severity, CapacityGuardSeverity::Warning);
        assert_eq!(a.pressure, PressureType::MemoryApproaching);
    }

    #[test]
    fn test_capacity_guard_memory_urgent() {
        let guard = CapacityGuardV2::new(100);
        let alert = guard.evaluate_memory(950_000_000, 1_000_000_000); // 95%
        assert!(alert.is_some());
        assert_eq!(alert.unwrap().severity, CapacityGuardSeverity::Urgent);
    }

    #[test]
    fn test_capacity_guard_memory_ok() {
        let guard = CapacityGuardV2::new(100);
        let alert = guard.evaluate_memory(500_000_000, 1_000_000_000); // 50%
        assert!(alert.is_none());
    }

    #[test]
    fn test_capacity_guard_wal_growth() {
        let guard = CapacityGuardV2::new(100);
        // 1GB WAL, growing at 500MB/hr → fills in 2h
        let alert = guard.evaluate_wal_growth(500_000_000, 1_000_000_000);
        assert!(alert.is_some());
        assert_eq!(alert.unwrap().severity, CapacityGuardSeverity::Urgent);
    }

    // -- Hardened Audit Tests --

    #[test]
    fn test_hardened_audit_trace_correlation() {
        let log = HardenedAuditLog::new(1000);
        log.record(UnifiedAuditEvent {
            id: 0, timestamp: 1000, trace_id: "trace-abc".into(), span_id: "span-1".into(),
            category: "AUTH".into(), severity: "INFO".into(), actor: "alice".into(),
            source_ip: "10.0.0.1".into(), action: "LOGIN".into(), resource: "system".into(),
            outcome: "OK".into(), details: "".into(), component: "gateway".into(),
            node_id: None, shard_id: None, duration_us: Some(500),
        });
        log.record(UnifiedAuditEvent {
            id: 0, timestamp: 1001, trace_id: "trace-abc".into(), span_id: "span-2".into(),
            category: "DATA".into(), severity: "INFO".into(), actor: "alice".into(),
            source_ip: "10.0.0.1".into(), action: "SELECT".into(), resource: "users".into(),
            outcome: "OK".into(), details: "".into(), component: "executor".into(),
            node_id: None, shard_id: Some(0), duration_us: Some(1200),
        });
        let correlated = log.query_by_trace("trace-abc");
        assert_eq!(correlated.len(), 2);
    }

    #[test]
    fn test_hardened_audit_export() {
        let log = HardenedAuditLog::new(1000);
        log.record(UnifiedAuditEvent {
            id: 0, timestamp: 1000, trace_id: "t1".into(), span_id: "s1".into(),
            category: "OPS".into(), severity: "WARN".into(), actor: "admin".into(),
            source_ip: "10.0.0.1".into(), action: "DRAIN".into(), resource: "node-3".into(),
            outcome: "OK".into(), details: "".into(), component: "controller".into(),
            node_id: Some(NodeId(3)), shard_id: None, duration_us: None,
        });
        let lines = log.export_jsonl(1);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("\"trace\":\"t1\""));
        assert!(lines[0].contains("\"component\":\"controller\""));
    }

    // -- Postmortem Tests --

    #[test]
    fn test_postmortem_decision_recording() {
        let gen = PostmortemGenerator::new(1000, 10000);
        let d1 = gen.record_decision(
            "LEADER_ELECTION",
            "Node 2 heartbeat missed",
            "heartbeat_gap_ms",
            5000.0,
            3000.0,
            "elected node 3 as leader for shard 0",
        );
        gen.update_outcome(d1, "Leader elected in 200ms, 0 transactions lost");
        assert_eq!(gen.metrics.decisions_recorded.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_postmortem_report_generation() {
        let gen = PostmortemGenerator::new(1000, 10000);

        // Record metrics before and during incident
        for i in 0..10u64 {
            gen.record_metric_at("latency_p99_ms", 5.0 + i as f64 * 0.1, 900 + i);
        }
        for i in 0..10u64 {
            gen.record_metric_at("latency_p99_ms", 50.0 + i as f64, 1000 + i);
        }

        gen.record_decision_at(
            "BACKPRESSURE",
            "p99 exceeded guardrail",
            "latency_p99_ms",
            55.0, 20.0,
            "activated backpressure",
            1005,
        );

        let timeline = vec![
            PostmortemTimelineEntry {
                timestamp: 1000, event_type: "NODE_FAILURE".into(),
                description: "Node 2 crashed".into(), severity: "CRITICAL".into(),
            },
            PostmortemTimelineEntry {
                timestamp: 1002, event_type: "LEADER_CHANGE".into(),
                description: "Shard 0 leader → node 3".into(), severity: "HIGH".into(),
            },
        ];

        let report = gen.generate_report(42, "Node 2 Crash Incident", 1000, 1010, timeline);
        assert_eq!(report.incident_id, 42);
        assert_eq!(report.timeline.len(), 2);
        assert!(!report.decision_points.is_empty());

        let text = PostmortemGenerator::export_text(&report);
        assert!(text.contains("POSTMORTEM"));
        assert!(text.contains("BACKPRESSURE"));
        assert!(text.contains("TIMELINE"));
    }

    // -- Admin Console v2 Tests --

    #[test]
    fn test_admin_v2_endpoints() {
        let console = AdminConsoleV2::new();
        assert_eq!(console.endpoint_count(), 10);
        assert!(console.endpoints().contains(&AdminV2Endpoint::SloDashboard));
        assert!(console.endpoints().contains(&AdminV2Endpoint::ChangeImpact));
    }

    // -- Change Impact Preview Tests --

    #[test]
    fn test_change_impact_compression() {
        let preview = ChangeImpactPreview::new();
        let est = preview.preview(&ProposedChange::IncreaseCompression {
            from: "balanced".into(), to: "aggressive".into(),
        });
        assert!(est.cpu_delta_pct > 0.0);
        assert!(est.memory_delta_bytes < 0);
        assert!(est.guardrail_ok);
        assert_eq!(est.risk, ImpactRisk::Low);
    }

    #[test]
    fn test_change_impact_remove_node() {
        let preview = ChangeImpactPreview::new();
        let est = preview.preview(&ProposedChange::RemoveNode { node_id: NodeId(3) });
        assert!(!est.guardrail_ok);
        assert_eq!(est.risk, ImpactRisk::High);
    }

    #[test]
    fn test_change_impact_format() {
        let preview = ChangeImpactPreview::new();
        let est = preview.preview(&ProposedChange::EnableColdCompaction);
        let text = ChangeImpactPreview::format_impact(&est);
        assert!(text.contains("CPU:"));
        assert!(text.contains("Memory:"));
        assert!(text.contains("Guardrail:"));
    }

    // -- Concurrent Tests --

    #[test]
    fn test_cost_tracker_concurrent() {
        use std::sync::Arc;
        let tracker = Arc::new(CostTracker::new(100));
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let tr = Arc::clone(&tracker);
            handles.push(std::thread::spawn(move || {
                for i in 0..50u64 {
                    tr.update_table(TableCost {
                        table_id: TableId(t * 100 + i),
                        table_name: format!("t_{}_{}", t, i),
                        hot_memory_bytes: 1000,
                        cold_memory_bytes: 0, wal_io_bytes: 0, replication_bytes: 0,
                        row_count: 0, compression_ratio: 1.0, compression_savings_bytes: 0,
                    });
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(tracker.table_count(), 200);
    }

    #[test]
    fn test_postmortem_concurrent() {
        use std::sync::Arc;
        let gen = Arc::new(PostmortemGenerator::new(5000, 50000));
        let mut handles = Vec::new();
        for t in 0..4 {
            let g = Arc::clone(&gen);
            handles.push(std::thread::spawn(move || {
                for i in 0..250u64 {
                    g.record_metric_at(&format!("metric_{}", t), i as f64, 1000 + i);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(gen.metrics.metric_samples.load(Ordering::Relaxed), 1000);
    }
}
