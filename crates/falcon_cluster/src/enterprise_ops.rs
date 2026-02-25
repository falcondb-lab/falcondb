//! v1.1.0 §7–12: Enterprise Ops — Auto Rebalance, Capacity Planning,
//! SLO Engine, Incident Timeline, Admin Console API.
//!
//! This module delivers the operational automation layer:
//! - **Auto Rebalance**: rate-limited shard migration on node join/leave
//! - **Capacity Planning**: resource growth prediction and alerts
//! - **SLO Engine**: define/compute/export SLOs with rolling windows
//! - **Incident Timeline**: auto-correlate failures, changes, metrics
//! - **Admin Console API**: structured endpoints for web UI

use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime};

use parking_lot::{Mutex, RwLock};

use falcon_common::types::NodeId;

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Auto Rebalance / Scale v1
// ═══════════════════════════════════════════════════════════════════════════

/// Rebalance trigger — why rebalancing was initiated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceTrigger {
    NodeJoined(NodeId),
    NodeLeft(NodeId),
    Manual,
    LoadImbalance,
    Scheduled,
}

impl fmt::Display for RebalanceTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NodeJoined(id) => write!(f, "NODE_JOINED({id:?})"),
            Self::NodeLeft(id) => write!(f, "NODE_LEFT({id:?})"),
            Self::Manual => write!(f, "MANUAL"),
            Self::LoadImbalance => write!(f, "LOAD_IMBALANCE"),
            Self::Scheduled => write!(f, "SCHEDULED"),
        }
    }
}

/// A single shard migration task.
#[derive(Debug, Clone)]
pub struct MigrationTask {
    pub task_id: u64,
    pub shard_id: u64,
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub state: MigrationState,
    pub bytes_migrated: u64,
    pub total_bytes: u64,
    pub started_at: Option<Instant>,
    pub completed_at: Option<Instant>,
}

/// Migration state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationState {
    Pending,
    Preparing,
    Copying,
    CatchingUp,
    Cutover,
    Completed,
    Failed,
    Cancelled,
}

impl fmt::Display for MigrationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Preparing => write!(f, "PREPARING"),
            Self::Copying => write!(f, "COPYING"),
            Self::CatchingUp => write!(f, "CATCHING_UP"),
            Self::Cutover => write!(f, "CUTOVER"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

/// Rebalance configuration.
#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    /// Maximum concurrent migrations.
    pub max_concurrent: usize,
    /// Bytes per second rate limit per migration (0 = unlimited).
    pub rate_limit_bytes_per_sec: u64,
    /// Maximum write latency increase allowed during rebalance (ms).
    pub max_sla_impact_ms: u64,
    /// Whether to auto-trigger on node join/leave.
    pub auto_trigger: bool,
    /// Minimum imbalance ratio to trigger rebalance (e.g., 0.2 = 20%).
    pub imbalance_threshold: f64,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 2,
            rate_limit_bytes_per_sec: 50_000_000, // 50 MB/s
            max_sla_impact_ms: 10,
            auto_trigger: true,
            imbalance_threshold: 0.2,
        }
    }
}

/// Auto-rebalancer — orchestrates shard migrations.
pub struct AutoRebalancer {
    config: RebalanceConfig,
    tasks: RwLock<Vec<MigrationTask>>,
    next_task_id: AtomicU64,
    active_count: AtomicU64,
    paused: std::sync::atomic::AtomicBool,
    pub metrics: RebalanceMetrics,
}

#[derive(Debug, Default)]
pub struct RebalanceMetrics {
    pub migrations_started: AtomicU64,
    pub migrations_completed: AtomicU64,
    pub migrations_failed: AtomicU64,
    pub bytes_migrated: AtomicU64,
    pub sla_pauses: AtomicU64,
}

impl AutoRebalancer {
    pub fn new(config: RebalanceConfig) -> Self {
        Self {
            config,
            tasks: RwLock::new(Vec::new()),
            next_task_id: AtomicU64::new(1),
            active_count: AtomicU64::new(0),
            paused: std::sync::atomic::AtomicBool::new(false),
            metrics: RebalanceMetrics::default(),
        }
    }

    /// Compute a rebalance plan given current node→shard assignments.
    /// Returns list of (shard_id, from, to) migrations.
    pub fn compute_plan(
        &self,
        assignments: &HashMap<NodeId, Vec<u64>>,
        available_nodes: &[NodeId],
    ) -> Vec<(u64, NodeId, NodeId)> {
        if available_nodes.is_empty() {
            return Vec::new();
        }
        let total_shards: usize = assignments.values().map(std::vec::Vec::len).sum();
        let ideal = (total_shards as f64 / available_nodes.len() as f64).ceil() as usize;

        let mut overloaded: Vec<(NodeId, Vec<u64>)> = Vec::new();
        let mut underloaded: Vec<(NodeId, usize)> = Vec::new();

        for node in available_nodes {
            let shards = assignments.get(node).cloned().unwrap_or_default();
            if shards.len() > ideal {
                overloaded.push((*node, shards));
            } else if shards.len() < ideal {
                underloaded.push((*node, ideal - shards.len()));
            }
        }

        let mut plan = Vec::new();
        for (from_node, shards) in &mut overloaded {
            while shards.len() > ideal {
                if let Some((to_node, remaining)) = underloaded.iter_mut().find(|(_, r)| *r > 0) {
                    let shard_id = shards.pop().unwrap();
                    plan.push((shard_id, *from_node, *to_node));
                    *remaining -= 1;
                } else {
                    break;
                }
            }
        }
        plan
    }

    /// Schedule a migration task.
    pub fn schedule_migration(&self, shard_id: u64, from: NodeId, to: NodeId) -> Option<u64> {
        if self.paused.load(Ordering::Relaxed) {
            return None;
        }
        if self.active_count.load(Ordering::Relaxed) as usize >= self.config.max_concurrent {
            return None;
        }
        let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        let task = MigrationTask {
            task_id,
            shard_id,
            from_node: from,
            to_node: to,
            state: MigrationState::Pending,
            bytes_migrated: 0,
            total_bytes: 0,
            started_at: None,
            completed_at: None,
        };
        self.tasks.write().push(task);
        self.metrics.migrations_started.fetch_add(1, Ordering::Relaxed);
        Some(task_id)
    }

    /// Advance a migration to the next state.
    pub fn advance(&self, task_id: u64, new_state: MigrationState) -> bool {
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.iter_mut().find(|t| t.task_id == task_id) {
            let old = task.state;
            task.state = new_state;
            match new_state {
                MigrationState::Preparing => {
                    task.started_at = Some(Instant::now());
                    self.active_count.fetch_add(1, Ordering::Relaxed);
                }
                MigrationState::Completed => {
                    task.completed_at = Some(Instant::now());
                    self.active_count.fetch_sub(1, Ordering::Relaxed);
                    self.metrics.migrations_completed.fetch_add(1, Ordering::Relaxed);
                    self.metrics.bytes_migrated.fetch_add(task.bytes_migrated, Ordering::Relaxed);
                }
                MigrationState::Failed | MigrationState::Cancelled => {
                    task.completed_at = Some(Instant::now());
                    if old != MigrationState::Pending {
                        self.active_count.fetch_sub(1, Ordering::Relaxed);
                    }
                    if new_state == MigrationState::Failed {
                        self.metrics.migrations_failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                _ => {}
            }
            true
        } else {
            false
        }
    }

    /// Update migration progress.
    pub fn update_progress(&self, task_id: u64, bytes_migrated: u64, total_bytes: u64) {
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.iter_mut().find(|t| t.task_id == task_id) {
            task.bytes_migrated = bytes_migrated;
            task.total_bytes = total_bytes;
        }
    }

    /// Pause rebalancing (SLA impact detected).
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
        self.metrics.sla_pauses.fetch_add(1, Ordering::Relaxed);
    }

    /// Resume rebalancing.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Get active migration count.
    pub fn active_migrations(&self) -> u64 {
        self.active_count.load(Ordering::Relaxed)
    }

    /// Get all tasks.
    pub fn all_tasks(&self) -> Vec<MigrationTask> {
        self.tasks.read().clone()
    }

    /// Get pending tasks.
    pub fn pending_tasks(&self) -> Vec<MigrationTask> {
        self.tasks.read().iter()
            .filter(|t| t.state == MigrationState::Pending)
            .cloned()
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 — Capacity Planning & Forecast
// ═══════════════════════════════════════════════════════════════════════════

/// Resource type being tracked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceType {
    WalSize,
    MemoryUsage,
    ColdStorageSize,
    ShardCount,
    ConnectionCount,
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WalSize => write!(f, "WAL_SIZE"),
            Self::MemoryUsage => write!(f, "MEMORY"),
            Self::ColdStorageSize => write!(f, "COLD_STORAGE"),
            Self::ShardCount => write!(f, "SHARD_COUNT"),
            Self::ConnectionCount => write!(f, "CONNECTIONS"),
        }
    }
}

/// A data point in a resource time series.
#[derive(Debug, Clone, Copy)]
pub struct ResourceSample {
    pub timestamp: u64,
    pub value: f64,
}

/// Resource forecast result.
#[derive(Debug, Clone)]
pub struct ForecastResult {
    pub resource: ResourceType,
    pub current_value: f64,
    pub capacity: f64,
    /// Predicted value at forecast horizon.
    pub predicted_value: f64,
    /// Growth rate per hour.
    pub growth_rate_per_hour: f64,
    /// Estimated time until capacity is reached (hours). None if declining or stable.
    pub time_to_exhaustion_hours: Option<f64>,
    /// Utilization ratio (0.0–1.0).
    pub utilization: f64,
}

/// Capacity alert severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CapacityAlertLevel {
    Ok,
    Watch,
    Warning,
    Critical,
}

impl fmt::Display for CapacityAlertLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => write!(f, "OK"),
            Self::Watch => write!(f, "WATCH"),
            Self::Warning => write!(f, "WARNING"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Capacity alert.
#[derive(Debug, Clone)]
pub struct CapacityAlert {
    pub resource: ResourceType,
    pub level: CapacityAlertLevel,
    pub message: String,
    pub forecast: ForecastResult,
    pub timestamp: u64,
}

/// Capacity planner — tracks resource usage and forecasts growth.
pub struct CapacityPlanner {
    series: RwLock<HashMap<ResourceType, VecDeque<ResourceSample>>>,
    capacities: RwLock<HashMap<ResourceType, f64>>,
    max_samples: usize,
    alerts: Mutex<VecDeque<CapacityAlert>>,
    pub metrics: CapacityMetrics,
}

#[derive(Debug, Default)]
pub struct CapacityMetrics {
    pub samples_recorded: AtomicU64,
    pub forecasts_computed: AtomicU64,
    pub alerts_generated: AtomicU64,
}

impl CapacityPlanner {
    pub fn new(max_samples: usize) -> Self {
        Self {
            series: RwLock::new(HashMap::new()),
            capacities: RwLock::new(HashMap::new()),
            max_samples,
            alerts: Mutex::new(VecDeque::with_capacity(100)),
            metrics: CapacityMetrics::default(),
        }
    }

    /// Set the capacity limit for a resource.
    pub fn set_capacity(&self, resource: ResourceType, capacity: f64) {
        self.capacities.write().insert(resource, capacity);
    }

    /// Record a sample.
    pub fn record(&self, resource: ResourceType, value: f64) {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let sample = ResourceSample { timestamp: ts, value };
        let mut series = self.series.write();
        let q = series.entry(resource).or_insert_with(|| VecDeque::with_capacity(self.max_samples));
        if q.len() >= self.max_samples {
            q.pop_front();
        }
        q.push_back(sample);
        self.metrics.samples_recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a sample with explicit timestamp.
    pub fn record_at(&self, resource: ResourceType, value: f64, timestamp: u64) {
        let sample = ResourceSample { timestamp, value };
        let mut series = self.series.write();
        let q = series.entry(resource).or_insert_with(|| VecDeque::with_capacity(self.max_samples));
        if q.len() >= self.max_samples {
            q.pop_front();
        }
        q.push_back(sample);
        self.metrics.samples_recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// Forecast a resource. Uses linear regression on the time series.
    pub fn forecast(&self, resource: ResourceType, horizon_hours: f64) -> Option<ForecastResult> {
        self.metrics.forecasts_computed.fetch_add(1, Ordering::Relaxed);
        let series = self.series.read();
        let samples = series.get(&resource)?;
        if samples.len() < 2 {
            return None;
        }

        let capacity = self.capacities.read().get(&resource).copied().unwrap_or(f64::MAX);

        // Linear regression: y = a + b*x (x = hours since first sample)
        let first_ts = samples.front().unwrap().timestamp as f64;
        let n = samples.len() as f64;
        let mut sum_x = 0.0f64;
        let mut sum_y = 0.0f64;
        let mut sum_xy = 0.0f64;
        let mut sum_xx = 0.0f64;

        for s in samples {
            let x = (s.timestamp as f64 - first_ts) / 3600.0; // hours
            let y = s.value;
            sum_x += x;
            sum_y += y;
            sum_xy += x * y;
            sum_xx += x * x;
        }

        let denom = n.mul_add(sum_xx, -(sum_x * sum_x));
        let (a, b) = if denom.abs() < 1e-10 {
            (sum_y / n, 0.0)
        } else {
            let b = n.mul_add(sum_xy, -(sum_x * sum_y)) / denom;
            let a = b.mul_add(-sum_x, sum_y) / n;
            (a, b)
        };

        let last = samples.back().unwrap();
        let current_x = (last.timestamp as f64 - first_ts) / 3600.0;
        let predicted_value = b.mul_add(current_x + horizon_hours, a);
        let time_to_exhaustion = if b > 1e-10 {
            let remaining = capacity - last.value;
            if remaining > 0.0 { Some(remaining / b) } else { Some(0.0) }
        } else {
            None
        };

        Some(ForecastResult {
            resource,
            current_value: last.value,
            capacity,
            predicted_value,
            growth_rate_per_hour: b,
            time_to_exhaustion_hours: time_to_exhaustion,
            utilization: if capacity > 0.0 { last.value / capacity } else { 0.0 },
        })
    }

    /// Evaluate all resources and generate alerts.
    pub fn evaluate_alerts(&self) -> Vec<CapacityAlert> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let resources: Vec<ResourceType> = self.series.read().keys().copied().collect();
        let mut alerts = Vec::new();

        for resource in resources {
            if let Some(forecast) = self.forecast(resource, 24.0) {
                let level = if forecast.utilization > 0.95 {
                    CapacityAlertLevel::Critical
                } else if forecast.utilization > 0.85
                    || forecast.time_to_exhaustion_hours.is_some_and(|t| t < 24.0)
                {
                    CapacityAlertLevel::Warning
                } else if forecast.utilization > 0.7
                    || forecast.time_to_exhaustion_hours.is_some_and(|t| t < 72.0)
                {
                    CapacityAlertLevel::Watch
                } else {
                    CapacityAlertLevel::Ok
                };

                if level > CapacityAlertLevel::Ok {
                    let msg = format!(
                        "{}: {:.1}% used, growth {:.1}/hr, exhaustion in {:.1}h",
                        resource,
                        forecast.utilization * 100.0,
                        forecast.growth_rate_per_hour,
                        forecast.time_to_exhaustion_hours.unwrap_or(f64::INFINITY),
                    );
                    let alert = CapacityAlert {
                        resource,
                        level,
                        message: msg,
                        forecast,
                        timestamp: now,
                    };
                    self.metrics.alerts_generated.fetch_add(1, Ordering::Relaxed);
                    let mut hist = self.alerts.lock();
                    if hist.len() >= 100 { hist.pop_front(); }
                    hist.push_back(alert.clone());
                    alerts.push(alert);
                }
            }
        }
        alerts
    }

    /// Get alert history.
    pub fn alert_history(&self, limit: usize) -> Vec<CapacityAlert> {
        self.alerts.lock().iter().rev().take(limit).cloned().collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §9 — SLO Engine v1
// ═══════════════════════════════════════════════════════════════════════════

/// SLO metric type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SloMetricType {
    Availability,
    LatencyP99,
    LatencyP999,
    Durability,
    ReplicationLag,
    ErrorRate,
}

impl fmt::Display for SloMetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Availability => write!(f, "availability"),
            Self::LatencyP99 => write!(f, "latency_p99"),
            Self::LatencyP999 => write!(f, "latency_p999"),
            Self::Durability => write!(f, "durability"),
            Self::ReplicationLag => write!(f, "replication_lag"),
            Self::ErrorRate => write!(f, "error_rate"),
        }
    }
}

/// SLO definition.
#[derive(Debug, Clone)]
pub struct SloDefinition {
    pub id: String,
    pub metric: SloMetricType,
    pub target: f64,
    /// Rolling window in seconds.
    pub window_secs: u64,
    pub description: String,
}

/// SLO evaluation result.
#[derive(Debug, Clone)]
pub struct SloEvaluation {
    pub slo_id: String,
    pub metric: SloMetricType,
    pub target: f64,
    pub actual: f64,
    pub met: bool,
    pub error_budget_remaining: f64,
    pub window_secs: u64,
    pub evaluated_at: u64,
}

/// SLO engine — define, compute, and export SLOs.
pub struct SloEngine {
    definitions: RwLock<Vec<SloDefinition>>,
    evaluations: RwLock<Vec<SloEvaluation>>,
    /// Metric data source (rolling window samples).
    samples: RwLock<HashMap<SloMetricType, VecDeque<(u64, f64)>>>,
    max_samples: usize,
    pub metrics: SloEngineMetrics,
}

#[derive(Debug, Default)]
pub struct SloEngineMetrics {
    pub evaluations_run: AtomicU64,
    pub slos_met: AtomicU64,
    pub slos_breached: AtomicU64,
}

impl SloEngine {
    pub fn new(max_samples: usize) -> Self {
        Self {
            definitions: RwLock::new(Vec::new()),
            evaluations: RwLock::new(Vec::new()),
            samples: RwLock::new(HashMap::new()),
            max_samples,
            metrics: SloEngineMetrics::default(),
        }
    }

    /// Define an SLO.
    pub fn define(&self, slo: SloDefinition) {
        self.definitions.write().push(slo);
    }

    /// Record a metric sample.
    pub fn record_sample(&self, metric: SloMetricType, value: f64) {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.record_sample_at(metric, value, ts);
    }

    /// Record a metric sample with explicit timestamp.
    pub fn record_sample_at(&self, metric: SloMetricType, value: f64, timestamp: u64) {
        let mut samples = self.samples.write();
        let q = samples.entry(metric)
            .or_insert_with(|| VecDeque::with_capacity(self.max_samples));
        if q.len() >= self.max_samples {
            q.pop_front();
        }
        q.push_back((timestamp, value));
    }

    /// Evaluate all defined SLOs against current data.
    pub fn evaluate_all(&self) -> Vec<SloEvaluation> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let definitions = self.definitions.read();
        let samples = self.samples.read();
        let mut results = Vec::new();

        for slo in definitions.iter() {
            self.metrics.evaluations_run.fetch_add(1, Ordering::Relaxed);
            let actual = self.compute_metric(&samples, &slo.metric, slo.window_secs, now);
            let met = match slo.metric {
                SloMetricType::Availability | SloMetricType::Durability => actual >= slo.target,
                SloMetricType::LatencyP99 | SloMetricType::LatencyP999 | SloMetricType::ReplicationLag => {
                    actual <= slo.target
                }
                SloMetricType::ErrorRate => actual <= slo.target,
            };
            let error_budget = match slo.metric {
                SloMetricType::Availability | SloMetricType::Durability => {
                    let budget = 1.0 - slo.target; // e.g., 0.001 for 99.9%
                    let used = 1.0 - actual;
                    if budget > 0.0 { 1.0 - (used / budget) } else { 0.0 }
                }
                _ => if met { 1.0 } else { 0.0 },
            };

            if met {
                self.metrics.slos_met.fetch_add(1, Ordering::Relaxed);
            } else {
                self.metrics.slos_breached.fetch_add(1, Ordering::Relaxed);
            }

            results.push(SloEvaluation {
                slo_id: slo.id.clone(),
                metric: slo.metric.clone(),
                target: slo.target,
                actual,
                met,
                error_budget_remaining: error_budget.clamp(0.0, 1.0),
                window_secs: slo.window_secs,
                evaluated_at: now,
            });
        }

        *self.evaluations.write() = results.clone();
        results
    }

    fn compute_metric(
        &self,
        samples: &HashMap<SloMetricType, VecDeque<(u64, f64)>>,
        metric: &SloMetricType,
        window_secs: u64,
        now: u64,
    ) -> f64 {
        let q = match samples.get(metric) {
            Some(q) if !q.is_empty() => q,
            _ => return 0.0,
        };
        let cutoff = now.saturating_sub(window_secs);
        let window_samples: Vec<f64> = q.iter()
            .filter(|(ts, _)| *ts >= cutoff)
            .map(|(_, v)| *v)
            .collect();
        if window_samples.is_empty() {
            return 0.0;
        }

        match metric {
            SloMetricType::Availability | SloMetricType::Durability => {
                // Samples are 1.0 (success) or 0.0 (failure)
                let sum: f64 = window_samples.iter().sum();
                sum / window_samples.len() as f64
            }
            SloMetricType::LatencyP99 | SloMetricType::LatencyP999 => {
                let mut sorted = window_samples;
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let pct = if *metric == SloMetricType::LatencyP99 { 0.99 } else { 0.999 };
                let idx = ((sorted.len() as f64 * pct).ceil() as usize).min(sorted.len()) - 1;
                sorted[idx]
            }
            SloMetricType::ReplicationLag => {
                // Use max of window
                window_samples.iter().copied().fold(0.0_f64, f64::max)
            }
            SloMetricType::ErrorRate => {
                let errors: f64 = window_samples.iter().filter(|v| **v > 0.0).count() as f64;
                errors / window_samples.len() as f64
            }
        }
    }

    /// Get latest evaluations.
    pub fn latest_evaluations(&self) -> Vec<SloEvaluation> {
        self.evaluations.read().clone()
    }

    /// Export evaluations as Prometheus metrics text.
    pub fn export_prometheus(&self) -> String {
        let evals = self.evaluations.read();
        let mut out = String::new();
        for e in evals.iter() {
            let _ = writeln!(out, "falcon_slo_actual{{slo_id=\"{}\",metric=\"{}\"}} {:.6}",
                e.slo_id, e.metric, e.actual);
            let _ = writeln!(out, "falcon_slo_target{{slo_id=\"{}\",metric=\"{}\"}} {:.6}",
                e.slo_id, e.metric, e.target);
            let _ = writeln!(out, "falcon_slo_met{{slo_id=\"{}\",metric=\"{}\"}} {}",
                e.slo_id, e.metric, i32::from(e.met));
            let _ = writeln!(out, "falcon_slo_error_budget{{slo_id=\"{}\",metric=\"{}\"}} {:.6}",
                e.slo_id, e.metric, e.error_budget_remaining);
        }
        out
    }

    /// Get definition count.
    pub fn definition_count(&self) -> usize {
        self.definitions.read().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §10 — Incident & Event Timeline
// ═══════════════════════════════════════════════════════════════════════════

/// Incident severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IncidentSeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl fmt::Display for IncidentSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => write!(f, "LOW"),
            Self::Medium => write!(f, "MEDIUM"),
            Self::High => write!(f, "HIGH"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Timeline event type.
#[derive(Debug, Clone, PartialEq)]
pub enum TimelineEventType {
    NodeFailure(NodeId),
    LeaderChange { shard_id: u64, old: Option<NodeId>, new: NodeId },
    OpsAction(String),
    MetricAnomaly { metric: String, value: f64, threshold: f64 },
    SloBreached(String),
    BackupCompleted(u64),
    ConfigChange { key: String },
    RebalanceStarted,
    RebalanceCompleted,
}

impl fmt::Display for TimelineEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NodeFailure(id) => write!(f, "NODE_FAILURE({id:?})"),
            Self::LeaderChange { shard_id, new, .. } =>
                write!(f, "LEADER_CHANGE(shard={shard_id}, new={new:?})"),
            Self::OpsAction(a) => write!(f, "OPS({a})"),
            Self::MetricAnomaly { metric, .. } => write!(f, "ANOMALY({metric})"),
            Self::SloBreached(id) => write!(f, "SLO_BREACH({id})"),
            Self::BackupCompleted(id) => write!(f, "BACKUP_OK({id})"),
            Self::ConfigChange { key } => write!(f, "CONFIG({key})"),
            Self::RebalanceStarted => write!(f, "REBALANCE_START"),
            Self::RebalanceCompleted => write!(f, "REBALANCE_DONE"),
        }
    }
}

/// Timeline event.
#[derive(Debug, Clone)]
pub struct TimelineEvent {
    pub id: u64,
    pub timestamp: u64,
    pub event_type: TimelineEventType,
    pub severity: IncidentSeverity,
    pub description: String,
    /// Correlated incident ID, if part of an incident.
    pub incident_id: Option<u64>,
}

/// An incident — a group of correlated timeline events.
#[derive(Debug, Clone)]
pub struct Incident {
    pub id: u64,
    pub title: String,
    pub severity: IncidentSeverity,
    pub started_at: u64,
    pub resolved_at: Option<u64>,
    pub event_ids: Vec<u64>,
    pub root_cause: Option<String>,
    pub impact: Option<String>,
}

/// Incident timeline — auto-correlate events into incidents.
pub struct IncidentTimeline {
    events: Mutex<VecDeque<TimelineEvent>>,
    incidents: RwLock<Vec<Incident>>,
    next_event_id: AtomicU64,
    next_incident_id: AtomicU64,
    event_capacity: usize,
    /// Correlation window: events within this window may be grouped.
    correlation_window_secs: u64,
    pub metrics: TimelineMetrics,
}

#[derive(Debug, Default)]
pub struct TimelineMetrics {
    pub events_recorded: AtomicU64,
    pub incidents_created: AtomicU64,
    pub incidents_resolved: AtomicU64,
}

impl IncidentTimeline {
    pub fn new(event_capacity: usize, correlation_window_secs: u64) -> Self {
        Self {
            events: Mutex::new(VecDeque::with_capacity(event_capacity)),
            incidents: RwLock::new(Vec::new()),
            next_event_id: AtomicU64::new(1),
            next_incident_id: AtomicU64::new(1),
            event_capacity,
            correlation_window_secs,
            metrics: TimelineMetrics::default(),
        }
    }

    /// Record a timeline event. Returns the event ID.
    pub fn record_event(
        &self,
        event_type: TimelineEventType,
        severity: IncidentSeverity,
        description: &str,
    ) -> u64 {
        let id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let event = TimelineEvent {
            id,
            timestamp: ts,
            event_type,
            severity,
            description: description.to_owned(),
            incident_id: None,
        };
        let mut events = self.events.lock();
        if events.len() >= self.event_capacity {
            events.pop_front();
        }
        events.push_back(event);
        self.metrics.events_recorded.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Create an incident from recent events.
    pub fn create_incident(
        &self,
        title: &str,
        severity: IncidentSeverity,
        event_ids: Vec<u64>,
    ) -> u64 {
        let id = self.next_incident_id.fetch_add(1, Ordering::Relaxed);
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let incident = Incident {
            id,
            title: title.to_owned(),
            severity,
            started_at: ts,
            resolved_at: None,
            event_ids: event_ids.clone(),
            root_cause: None,
            impact: None,
        };
        self.incidents.write().push(incident);

        // Correlate events
        let mut events = self.events.lock();
        for eid in &event_ids {
            if let Some(e) = events.iter_mut().find(|e| e.id == *eid) {
                e.incident_id = Some(id);
            }
        }
        self.metrics.incidents_created.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Resolve an incident.
    pub fn resolve_incident(&self, incident_id: u64, root_cause: &str, impact: &str) -> bool {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut incidents = self.incidents.write();
        if let Some(inc) = incidents.iter_mut().find(|i| i.id == incident_id) {
            inc.resolved_at = Some(ts);
            inc.root_cause = Some(root_cause.to_owned());
            inc.impact = Some(impact.to_owned());
            self.metrics.incidents_resolved.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Auto-correlate: find recent high-severity events and group them.
    pub fn auto_correlate(&self) -> Option<u64> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let cutoff = now.saturating_sub(self.correlation_window_secs);
        let events = self.events.lock();
        let uncorrelated: Vec<u64> = events.iter()
            .filter(|e| {
                e.timestamp >= cutoff
                    && e.incident_id.is_none()
                    && e.severity >= IncidentSeverity::High
            })
            .map(|e| e.id)
            .collect();
        drop(events);

        if uncorrelated.len() >= 2 {
            let incident_id = self.create_incident(
                "Auto-correlated incident",
                IncidentSeverity::High,
                uncorrelated,
            );
            Some(incident_id)
        } else {
            None
        }
    }

    /// Get recent events.
    pub fn recent_events(&self, count: usize) -> Vec<TimelineEvent> {
        self.events.lock().iter().rev().take(count).cloned().collect()
    }

    /// Get all incidents.
    pub fn all_incidents(&self) -> Vec<Incident> {
        self.incidents.read().clone()
    }

    /// Get open (unresolved) incidents.
    pub fn open_incidents(&self) -> Vec<Incident> {
        self.incidents.read().iter()
            .filter(|i| i.resolved_at.is_none())
            .cloned()
            .collect()
    }

    /// Get events for an incident.
    pub fn events_for_incident(&self, incident_id: u64) -> Vec<TimelineEvent> {
        self.events.lock().iter()
            .filter(|e| e.incident_id == Some(incident_id))
            .cloned()
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §11 — Admin Console API Model
// ═══════════════════════════════════════════════════════════════════════════

/// Admin API endpoint type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminEndpoint {
    ClusterOverview,
    NodeList,
    NodeDetail(NodeId),
    ShardMap,
    ShardDetail(u64),
    RebalanceStatus,
    BackupList,
    BackupDetail(u64),
    RestoreList,
    SloStatus,
    AuditLog,
    IncidentList,
    IncidentDetail(u64),
    CapacityForecast,
    ConfigList,
}

impl fmt::Display for AdminEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClusterOverview => write!(f, "/admin/cluster"),
            Self::NodeList => write!(f, "/admin/nodes"),
            Self::NodeDetail(id) => write!(f, "/admin/nodes/{id:?}"),
            Self::ShardMap => write!(f, "/admin/shards"),
            Self::ShardDetail(id) => write!(f, "/admin/shards/{id}"),
            Self::RebalanceStatus => write!(f, "/admin/rebalance"),
            Self::BackupList => write!(f, "/admin/backups"),
            Self::BackupDetail(id) => write!(f, "/admin/backups/{id}"),
            Self::RestoreList => write!(f, "/admin/restores"),
            Self::SloStatus => write!(f, "/admin/slo"),
            Self::AuditLog => write!(f, "/admin/audit"),
            Self::IncidentList => write!(f, "/admin/incidents"),
            Self::IncidentDetail(id) => write!(f, "/admin/incidents/{id}"),
            Self::CapacityForecast => write!(f, "/admin/capacity"),
            Self::ConfigList => write!(f, "/admin/config"),
        }
    }
}

/// Cluster overview response.
#[derive(Debug, Clone)]
pub struct ClusterOverviewResponse {
    pub cluster_name: String,
    pub version: String,
    pub nodes_total: usize,
    pub nodes_online: usize,
    pub nodes_suspect: usize,
    pub nodes_offline: usize,
    pub shards_total: usize,
    pub shards_healthy: usize,
    pub shards_under_replicated: usize,
    pub active_migrations: u64,
    pub slo_all_met: bool,
    pub open_incidents: usize,
    pub last_backup_at: Option<u64>,
}

/// Node detail response.
#[derive(Debug, Clone)]
pub struct NodeDetailResponse {
    pub node_id: NodeId,
    pub address: String,
    pub state: String,
    pub version: String,
    pub shards: Vec<u64>,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub wal_size: u64,
    pub uptime_secs: u64,
}

/// Shard detail response.
#[derive(Debug, Clone)]
pub struct ShardDetailResponse {
    pub shard_id: u64,
    pub leader: Option<NodeId>,
    pub replicas: Vec<NodeId>,
    pub state: String,
    pub replication_lag: u64,
    pub row_count: u64,
    pub size_bytes: u64,
}

/// Admin API router — maps endpoints to handlers (model only).
pub struct AdminApiRouter {
    registered_endpoints: Vec<AdminEndpoint>,
}

impl Default for AdminApiRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl AdminApiRouter {
    pub fn new() -> Self {
        Self {
            registered_endpoints: vec![
                AdminEndpoint::ClusterOverview,
                AdminEndpoint::NodeList,
                AdminEndpoint::ShardMap,
                AdminEndpoint::RebalanceStatus,
                AdminEndpoint::BackupList,
                AdminEndpoint::RestoreList,
                AdminEndpoint::SloStatus,
                AdminEndpoint::AuditLog,
                AdminEndpoint::IncidentList,
                AdminEndpoint::CapacityForecast,
                AdminEndpoint::ConfigList,
            ],
        }
    }

    pub fn endpoints(&self) -> &[AdminEndpoint] {
        &self.registered_endpoints
    }

    pub const fn endpoint_count(&self) -> usize {
        self.registered_endpoints.len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §12 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // -- Auto Rebalancer Tests --

    #[test]
    fn test_compute_rebalance_plan() {
        let rb = AutoRebalancer::new(RebalanceConfig::default());
        let mut assignments = HashMap::new();
        assignments.insert(NodeId(1), vec![0, 1, 2, 3]); // 4 shards
        assignments.insert(NodeId(2), vec![4]);            // 1 shard
        assignments.insert(NodeId(3), vec![]);             // 0 shards
        let nodes = vec![NodeId(1), NodeId(2), NodeId(3)];
        let plan = rb.compute_plan(&assignments, &nodes);
        // Ideal = ceil(5/3) = 2 per node
        // Node 1 has 4 (excess 2), Node 2 has 1 (needs 1), Node 3 has 0 (needs 2)
        assert!(!plan.is_empty());
        assert!(plan.len() >= 2); // at least 2 migrations
    }

    #[test]
    fn test_migration_lifecycle() {
        let rb = AutoRebalancer::new(RebalanceConfig::default());
        let tid = rb.schedule_migration(0, NodeId(1), NodeId(3)).unwrap();
        assert_eq!(rb.pending_tasks().len(), 1);
        rb.advance(tid, MigrationState::Preparing);
        assert_eq!(rb.active_migrations(), 1);
        rb.update_progress(tid, 500_000, 1_000_000);
        rb.advance(tid, MigrationState::Copying);
        rb.advance(tid, MigrationState::CatchingUp);
        rb.advance(tid, MigrationState::Cutover);
        rb.advance(tid, MigrationState::Completed);
        assert_eq!(rb.active_migrations(), 0);
        assert_eq!(rb.metrics.migrations_completed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_migration_max_concurrent() {
        let config = RebalanceConfig { max_concurrent: 1, ..Default::default() };
        let rb = AutoRebalancer::new(config);
        let t1 = rb.schedule_migration(0, NodeId(1), NodeId(2));
        assert!(t1.is_some());
        rb.advance(t1.unwrap(), MigrationState::Preparing);
        // Second migration should be rejected (max=1)
        let t2 = rb.schedule_migration(1, NodeId(1), NodeId(3));
        assert!(t2.is_none());
    }

    #[test]
    fn test_rebalance_pause_resume() {
        let rb = AutoRebalancer::new(RebalanceConfig::default());
        rb.pause();
        assert!(rb.is_paused());
        assert!(rb.schedule_migration(0, NodeId(1), NodeId(2)).is_none());
        rb.resume();
        assert!(!rb.is_paused());
        assert!(rb.schedule_migration(0, NodeId(1), NodeId(2)).is_some());
    }

    // -- Capacity Planner Tests --

    #[test]
    fn test_capacity_forecast_growth() {
        let planner = CapacityPlanner::new(1000);
        planner.set_capacity(ResourceType::WalSize, 1_000_000_000.0); // 1GB

        // Simulate linear growth: 100MB/hour
        for hour in 0..10 {
            let value = 100_000_000.0 * hour as f64; // growing by 100MB/hr
            planner.record_at(ResourceType::WalSize, value, 1000 + hour * 3600);
        }

        let forecast = planner.forecast(ResourceType::WalSize, 24.0).unwrap();
        assert!(forecast.growth_rate_per_hour > 90_000_000.0); // ~100MB/hr
        assert!(forecast.time_to_exhaustion_hours.is_some());
        assert!(forecast.utilization > 0.0);
    }

    #[test]
    fn test_capacity_alerts() {
        let planner = CapacityPlanner::new(1000);
        planner.set_capacity(ResourceType::MemoryUsage, 100.0);

        // At 90% usage with growth
        for hour in 0..5u64 {
            planner.record_at(ResourceType::MemoryUsage, 80.0 + hour as f64 * 2.0, 1000 + hour * 3600);
        }

        let alerts = planner.evaluate_alerts();
        // Should generate at least a warning (>85% utilization)
        assert!(!alerts.is_empty());
        assert!(alerts[0].level >= CapacityAlertLevel::Warning);
    }

    // -- SLO Engine Tests --

    #[test]
    fn test_slo_availability() {
        let engine = SloEngine::new(10000);
        engine.define(SloDefinition {
            id: "write-avail".into(),
            metric: SloMetricType::Availability,
            target: 0.999,
            window_secs: 3600,
            description: "Write availability SLO".into(),
        });

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // 999 successes, 1 failure
        for i in 0..1000u64 {
            let val = if i == 500 { 0.0 } else { 1.0 };
            engine.record_sample_at(SloMetricType::Availability, val, now - 1800 + i);
        }

        let evals = engine.evaluate_all();
        assert_eq!(evals.len(), 1);
        assert!(evals[0].actual >= 0.998); // 999/1000
        assert!(evals[0].met);
    }

    #[test]
    fn test_slo_latency() {
        let engine = SloEngine::new(10000);
        engine.define(SloDefinition {
            id: "read-p99".into(),
            metric: SloMetricType::LatencyP99,
            target: 50.0, // 50ms
            window_secs: 3600,
            description: "Read p99 latency".into(),
        });

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Most reads under 10ms, a few at 30ms
        for i in 0..100u64 {
            let val = if i >= 95 { 30.0 } else { 5.0 + (i as f64) * 0.1 };
            engine.record_sample_at(SloMetricType::LatencyP99, val, now - 1800 + i);
        }

        let evals = engine.evaluate_all();
        assert_eq!(evals.len(), 1);
        assert!(evals[0].met); // p99 should be ~30ms < 50ms target
    }

    #[test]
    fn test_slo_breach() {
        let engine = SloEngine::new(10000);
        engine.define(SloDefinition {
            id: "write-avail".into(),
            metric: SloMetricType::Availability,
            target: 0.999,
            window_secs: 3600,
            description: "".into(),
        });

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // 90% availability — breaches 99.9% SLO
        for i in 0..100u64 {
            let val = if i % 10 == 0 { 0.0 } else { 1.0 };
            engine.record_sample_at(SloMetricType::Availability, val, now - 1800 + i);
        }

        let evals = engine.evaluate_all();
        assert!(!evals[0].met);
        assert_eq!(engine.metrics.slos_breached.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_slo_prometheus_export() {
        let engine = SloEngine::new(10000);
        engine.define(SloDefinition {
            id: "test-slo".into(),
            metric: SloMetricType::Availability,
            target: 0.99,
            window_secs: 60,
            description: "".into(),
        });

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        for i in 0..10u64 {
            engine.record_sample_at(SloMetricType::Availability, 1.0, now - 30 + i);
        }
        engine.evaluate_all();
        let prom = engine.export_prometheus();
        assert!(prom.contains("falcon_slo_actual"));
        assert!(prom.contains("falcon_slo_met"));
        assert!(prom.contains("test-slo"));
    }

    // -- Incident Timeline Tests --

    #[test]
    fn test_incident_lifecycle() {
        let timeline = IncidentTimeline::new(1000, 300);
        let e1 = timeline.record_event(
            TimelineEventType::NodeFailure(NodeId(1)),
            IncidentSeverity::High,
            "Node 1 failed",
        );
        let e2 = timeline.record_event(
            TimelineEventType::LeaderChange { shard_id: 0, old: Some(NodeId(1)), new: NodeId(2) },
            IncidentSeverity::High,
            "Leader changed for shard 0",
        );
        let inc_id = timeline.create_incident("Node 1 failure incident", IncidentSeverity::High, vec![e1, e2]);
        assert!(timeline.open_incidents().len() == 1);

        timeline.resolve_incident(inc_id, "Node 1 hardware failure", "30s write unavailability on shard 0");
        assert!(timeline.open_incidents().is_empty());

        let incidents = timeline.all_incidents();
        assert_eq!(incidents.len(), 1);
        assert!(incidents[0].resolved_at.is_some());
        assert_eq!(incidents[0].root_cause.as_deref(), Some("Node 1 hardware failure"));
    }

    #[test]
    fn test_auto_correlate() {
        let timeline = IncidentTimeline::new(1000, 300);
        // Record multiple high-severity events
        timeline.record_event(
            TimelineEventType::NodeFailure(NodeId(1)),
            IncidentSeverity::High,
            "Node 1 down",
        );
        timeline.record_event(
            TimelineEventType::MetricAnomaly {
                metric: "cpu".into(),
                value: 0.99,
                threshold: 0.85,
            },
            IncidentSeverity::High,
            "CPU spike",
        );
        timeline.record_event(
            TimelineEventType::SloBreached("write-avail".into()),
            IncidentSeverity::Critical,
            "SLO breached",
        );

        let inc = timeline.auto_correlate();
        assert!(inc.is_some());
        let events = timeline.events_for_incident(inc.unwrap());
        assert!(events.len() >= 2);
    }

    #[test]
    fn test_timeline_no_autocorrelate_low_severity() {
        let timeline = IncidentTimeline::new(1000, 300);
        timeline.record_event(
            TimelineEventType::ConfigChange { key: "a".into() },
            IncidentSeverity::Low,
            "Config changed",
        );
        let inc = timeline.auto_correlate();
        assert!(inc.is_none());
    }

    // -- Admin API Router Tests --

    #[test]
    fn test_admin_api_endpoints() {
        let router = AdminApiRouter::new();
        assert_eq!(router.endpoint_count(), 11);
        assert!(router.endpoints().contains(&AdminEndpoint::ClusterOverview));
        assert!(router.endpoints().contains(&AdminEndpoint::SloStatus));
    }

    // -- Concurrent Tests --

    #[test]
    fn test_slo_engine_concurrent() {
        use std::sync::Arc;
        let engine = Arc::new(SloEngine::new(100_000));
        engine.define(SloDefinition {
            id: "avail".into(),
            metric: SloMetricType::Availability,
            target: 0.99,
            window_secs: 3600,
            description: "".into(),
        });

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut handles = Vec::new();
        for t in 0..4 {
            let e = Arc::clone(&engine);
            handles.push(std::thread::spawn(move || {
                for i in 0..250u64 {
                    e.record_sample_at(
                        SloMetricType::Availability,
                        if (t * 250 + i) % 100 == 0 { 0.0 } else { 1.0 },
                        now - 1800 + t * 250 + i,
                    );
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        let evals = engine.evaluate_all();
        assert_eq!(evals.len(), 1);
        assert!(evals[0].actual > 0.95); // ~99% availability
    }

    #[test]
    fn test_timeline_concurrent() {
        use std::sync::Arc;
        let tl = Arc::new(IncidentTimeline::new(5000, 300));
        let mut handles = Vec::new();
        for t in 0..4 {
            let tl_c = Arc::clone(&tl);
            handles.push(std::thread::spawn(move || {
                for i in 0..250u64 {
                    tl_c.record_event(
                        TimelineEventType::OpsAction(format!("t{}-op{}", t, i)),
                        IncidentSeverity::Low,
                        "test",
                    );
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(tl.metrics.events_recorded.load(Ordering::Relaxed), 1000);
    }
}
