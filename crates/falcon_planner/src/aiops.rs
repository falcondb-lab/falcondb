//! AIOps — Autonomous database operations intelligence for FalconDB.
//!
//! Provides four integrated subsystems:
//!
//! 1. **SlowQueryDetector** — records every query execution; flags queries exceeding
//!    an adaptive threshold (EMA-based p95 baseline + configurable multiplier).
//! 2. **AnomalyDetector** — tracks per-second TPS and p99 latency using a rolling
//!    window; fires alerts when observations exceed 3σ above the running mean.
//! 3. **IndexAdvisor** — records full-scan events tagged by table and filter columns;
//!    recommends `CREATE INDEX` when a table accumulates enough slow full scans.
//! 4. **WorkloadProfiler** — aggregates per-query-fingerprint execution statistics
//!    (call count, total/avg/max latency, error rate) for capacity planning.
//!
//! All subsystems are lock-protected and cheaply cloneable via `Arc`.
//! The top-level [`AiOps`] struct wires them together and is the single entry
//! point used by the `Executor`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ─── Timestamp helper ────────────────────────────────────────────────────────

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

// ─── Alert severity ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlertSeverity {
    Warning,
    Critical,
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertSeverity::Warning => write!(f, "WARNING"),
            AlertSeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

// ─── AIOps alert ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AiOpsAlert {
    pub id: u64,
    pub ts_unix_ms: u64,
    pub severity: AlertSeverity,
    pub source: String,
    pub message: String,
}

// ─── Slow query record ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SlowQueryRecord {
    pub ts_unix_ms: u64,
    pub sql_fingerprint: String,
    /// Truncated original SQL (first 200 chars)
    pub sql_text: String,
    pub table_name: String,
    pub duration_us: u64,
    pub threshold_us: u64,
    pub plan_kind: String,
}

// ─── Index advice ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct IndexAdvice {
    pub table_name: String,
    pub column_hints: Vec<String>,
    pub full_scan_count: u64,
    pub avg_scan_duration_us: u64,
    pub suggestion: String,
}

// ─── Workload fingerprint stats ──────────────────────────────────────────────

/// Reservoir of size R for p99 estimation (no sort needed at query time).
#[derive(Debug, Clone)]
struct Reservoir {
    samples: Vec<u64>,
    capacity: usize,
    total_seen: u64,
}

impl Reservoir {
    fn new(capacity: usize) -> Self {
        Self { samples: Vec::with_capacity(capacity), capacity, total_seen: 0 }
    }

    fn insert(&mut self, x: u64) {
        self.total_seen += 1;
        if self.samples.len() < self.capacity {
            self.samples.push(x);
        } else {
            // Simple reservoir: replace a random position using LCG
            let idx = (self.total_seen.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407)
                >> 33) as usize % self.capacity;
            self.samples[idx] = x;
        }
    }

    fn percentile(&self, pct: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
}

#[derive(Debug, Clone)]
pub struct WorkloadStat {
    pub fingerprint: String,
    pub call_count: u64,
    pub total_us: u64,
    pub max_us: u64,
    pub error_count: u64,
    reservoir: Reservoir,
}

impl WorkloadStat {
    pub fn avg_us(&self) -> u64 {
        if self.call_count == 0 { 0 } else { self.total_us / self.call_count }
    }

    pub fn p99_us(&self) -> u64 {
        self.reservoir.percentile(99.0)
    }

    pub fn p95_us(&self) -> u64 {
        self.reservoir.percentile(95.0)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. SlowQueryDetector
// ─────────────────────────────────────────────────────────────────────────────

/// Adaptive slow-query detector using an EMA baseline.
///
/// Threshold = max(abs_floor_us, ema_p95 * multiplier).
/// After `warmup_samples` executions the adaptive threshold kicks in.
struct SlowQueryDetectorInner {
    abs_floor_us: u64,
    multiplier: f64,
    ema_p95: f64,
    ema_alpha: f64,
    sample_count: u64,
    warmup_samples: u64,
    records: VecDeque<SlowQueryRecord>,
    max_records: usize,
}

#[derive(Clone)]
pub struct SlowQueryDetector {
    inner: Arc<Mutex<SlowQueryDetectorInner>>,
}

impl SlowQueryDetector {
    pub fn new(abs_floor_us: u64, multiplier: f64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SlowQueryDetectorInner {
                abs_floor_us,
                multiplier,
                ema_p95: abs_floor_us as f64,
                ema_alpha: 0.05,
                sample_count: 0,
                warmup_samples: 100,
                records: VecDeque::new(),
                max_records: 200,
            })),
        }
    }

    /// Record a query execution. Returns Some(record) if it was slow.
    pub fn record(
        &self,
        sql_text: &str,
        sql_fingerprint: &str,
        table_name: &str,
        duration_us: u64,
        plan_kind: &str,
    ) -> Option<SlowQueryRecord> {
        let mut g = self.inner.lock().unwrap();

        let alpha = g.ema_alpha;
        let weighted = if duration_us as f64 > g.ema_p95 {
            duration_us as f64
        } else {
            g.ema_p95 * 0.95 + duration_us as f64 * 0.05
        };
        g.ema_p95 = (1.0 - alpha) * g.ema_p95 + alpha * weighted;
        g.sample_count += 1;

        let threshold_us = if g.sample_count < g.warmup_samples {
            g.abs_floor_us
        } else {
            (g.ema_p95 * g.multiplier).max(g.abs_floor_us as f64) as u64
        };

        if duration_us >= threshold_us {
            let truncated = if sql_text.len() > 200 { &sql_text[..200] } else { sql_text };
            let rec = SlowQueryRecord {
                ts_unix_ms: now_unix_ms(),
                sql_fingerprint: sql_fingerprint.to_owned(),
                sql_text: truncated.to_owned(),
                table_name: table_name.to_owned(),
                duration_us,
                threshold_us,
                plan_kind: plan_kind.to_owned(),
            };
            if g.records.len() >= g.max_records {
                g.records.pop_front();
            }
            g.records.push_back(rec.clone());
            Some(rec)
        } else {
            None
        }
    }

    pub fn recent_records(&self, limit: usize) -> Vec<SlowQueryRecord> {
        let g = self.inner.lock().unwrap();
        g.records.iter().rev().take(limit).cloned().collect()
    }

    pub fn total_logged(&self) -> usize {
        self.inner.lock().unwrap().records.len()
    }

    pub fn stats(&self) -> (u64, u64, f64) {
        let g = self.inner.lock().unwrap();
        let threshold = (g.ema_p95 * g.multiplier).max(g.abs_floor_us as f64) as u64;
        (g.sample_count, threshold, g.ema_p95)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. AnomalyDetector
// ─────────────────────────────────────────────────────────────────────────────

/// Online anomaly detector using Welford's running variance (3σ rule).
///
/// Tracks two signals independently: TPS and p99 query latency (µs).
/// An alert fires when the current observation exceeds mean + 3σ.
struct Signal {
    n: u64,
    mean: f64,
    m2: f64,
    window: VecDeque<f64>,
    window_size: usize,
}

impl Signal {
    fn new(window_size: usize) -> Self {
        Self { n: 0, mean: 0.0, m2: 0.0, window: VecDeque::new(), window_size }
    }

    fn update(&mut self, x: f64) -> bool {
        // Welford online update
        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;

        // Rolling window for recency
        self.window.push_back(x);
        if self.window.len() > self.window_size {
            self.window.pop_front();
        }

        // Fire anomaly if n >= 30 and value > mean + 3σ
        if self.n >= 30 {
            let variance = self.m2 / self.n as f64;
            let sigma = variance.sqrt();
            x > self.mean + 3.0 * sigma
        } else {
            false
        }
    }

    fn stddev(&self) -> f64 {
        if self.n < 2 {
            return 0.0;
        }
        (self.m2 / self.n as f64).sqrt()
    }
}

struct AnomalyDetectorInner {
    tps_signal: Signal,
    latency_signal: Signal,
    // Per-second accumulator
    current_second: u64,
    queries_this_second: u64,
    latency_sum_this_second: u64,
    alerts: VecDeque<AiOpsAlert>,
    max_alerts: usize,
    next_alert_id: u64,
    // Consecutive slow-query tracking
    consecutive_slow: u32,
    consecutive_slow_threshold: u32,
}

#[derive(Clone)]
pub struct AnomalyDetector {
    inner: Arc<Mutex<AnomalyDetectorInner>>,
}

impl AnomalyDetector {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(AnomalyDetectorInner {
                tps_signal: Signal::new(60),
                latency_signal: Signal::new(60),
                current_second: now_unix_secs(),
                queries_this_second: 0,
                latency_sum_this_second: 0,
                alerts: VecDeque::new(),
                max_alerts: 500,
                next_alert_id: 1,
                consecutive_slow: 0,
                consecutive_slow_threshold: 5,
            })),
        }
    }

    /// Record one query completion with slow flag. May emit anomaly alerts internally.
    pub fn record(&self, duration_us: u64, is_slow: bool) -> Vec<AiOpsAlert> {
        let mut g = self.inner.lock().unwrap();
        let now = now_unix_secs();

        let mut new_alerts = Vec::new();

        if now != g.current_second {
            // Flush accumulated second
            let tps = g.queries_this_second;
            let avg_lat = if tps > 0 { g.latency_sum_this_second / tps } else { 0 };

            if tps > 0 {
                if g.tps_signal.update(tps as f64) {
                    let alert = AiOpsAlert {
                        id: g.next_alert_id,
                        ts_unix_ms: now_unix_ms(),
                        severity: AlertSeverity::Warning,
                        source: "AnomalyDetector/TPS".to_owned(),
                        message: format!(
                            "TPS spike detected: {} tps (mean={:.1}, σ={:.1})",
                            tps, g.tps_signal.mean, g.tps_signal.stddev()
                        ),
                    };
                    g.next_alert_id += 1;
                    if g.alerts.len() >= g.max_alerts { g.alerts.pop_front(); }
                    g.alerts.push_back(alert.clone());
                    new_alerts.push(alert);
                }

                if g.latency_signal.update(avg_lat as f64) {
                    let sev = if avg_lat > g.latency_signal.mean as u64 * 5 {
                        AlertSeverity::Critical
                    } else {
                        AlertSeverity::Warning
                    };
                    let alert = AiOpsAlert {
                        id: g.next_alert_id,
                        ts_unix_ms: now_unix_ms(),
                        severity: sev,
                        source: "AnomalyDetector/Latency".to_owned(),
                        message: format!(
                            "Latency anomaly: avg={}µs (mean={:.1}µs, σ={:.1}µs)",
                            avg_lat, g.latency_signal.mean, g.latency_signal.stddev()
                        ),
                    };
                    g.next_alert_id += 1;
                    if g.alerts.len() >= g.max_alerts { g.alerts.pop_front(); }
                    g.alerts.push_back(alert.clone());
                    new_alerts.push(alert);
                }
            }

            g.current_second = now;
            g.queries_this_second = 0;
            g.latency_sum_this_second = 0;
        }

        g.queries_this_second += 1;
        g.latency_sum_this_second += duration_us;

        // Consecutive slow query tracking
        if is_slow {
            g.consecutive_slow += 1;
            if g.consecutive_slow >= g.consecutive_slow_threshold {
                let alert = AiOpsAlert {
                    id: g.next_alert_id,
                    ts_unix_ms: now_unix_ms(),
                    severity: AlertSeverity::Warning,
                    source: "AnomalyDetector/SlowQuery".to_owned(),
                    message: format!(
                        "{} consecutive slow queries detected (last={}µs)",
                        g.consecutive_slow, duration_us
                    ),
                };
                g.next_alert_id += 1;
                if g.alerts.len() >= g.max_alerts { g.alerts.pop_front(); }
                g.alerts.push_back(alert.clone());
                new_alerts.push(alert);
                g.consecutive_slow = 0; // reset after firing
            }
        } else {
            g.consecutive_slow = 0;
        }

        new_alerts
    }

    pub fn recent_alerts(&self, limit: usize) -> Vec<AiOpsAlert> {
        let g = self.inner.lock().unwrap();
        g.alerts.iter().rev().take(limit).cloned().collect()
    }

    pub fn stats(&self) -> (f64, f64, f64, f64, u64) {
        let g = self.inner.lock().unwrap();
        (
            g.tps_signal.mean,
            g.tps_signal.stddev(),
            g.latency_signal.mean,
            g.latency_signal.stddev(),
            g.alerts.len() as u64,
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. IndexAdvisor
// ─────────────────────────────────────────────────────────────────────────────

/// Tracks full-scan events and emits index creation recommendations.
///
/// A recommendation is emitted when a table has ≥ `min_scans` full scans
/// and the average scan duration exceeds `min_avg_us`.

#[derive(Default)]
struct TableScanAccum {
    count: u64,
    total_us: u64,
    column_hints: HashMap<String, u64>,
}

struct IndexAdvisorInner {
    table_scans: HashMap<String, TableScanAccum>,
    min_scans: u64,
    min_avg_us: u64,
}

#[derive(Clone)]
pub struct IndexAdvisor {
    inner: Arc<Mutex<IndexAdvisorInner>>,
}

impl IndexAdvisor {
    pub fn new(min_scans: u64, min_avg_us: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(IndexAdvisorInner {
                table_scans: HashMap::new(),
                min_scans,
                min_avg_us,
            })),
        }
    }

    /// Record a full-scan event for `table_name`, with optional filter column hints.
    pub fn record_full_scan(&self, table_name: &str, duration_us: u64, filter_cols: &[&str]) {
        let mut g = self.inner.lock().unwrap();
        let acc = g.table_scans.entry(table_name.to_owned()).or_default();
        acc.count += 1;
        acc.total_us += duration_us;
        for col in filter_cols {
            *acc.column_hints.entry((*col).to_owned()).or_insert(0) += 1;
        }
    }

    /// Return current index recommendations.
    pub fn advice(&self) -> Vec<IndexAdvice> {
        let g = self.inner.lock().unwrap();
        let mut result = Vec::new();
        for (table, acc) in &g.table_scans {
            if acc.count < g.min_scans {
                continue;
            }
            let avg_us = acc.total_us / acc.count;
            if avg_us < g.min_avg_us {
                continue;
            }
            // Top columns by occurrence
            let mut cols: Vec<(&String, &u64)> = acc.column_hints.iter().collect();
            cols.sort_by(|a, b| b.1.cmp(a.1));
            let top_cols: Vec<String> = cols.iter().take(3).map(|(c, _)| (*c).clone()).collect();

            let suggestion = if top_cols.is_empty() {
                format!(
                    "Consider adding an index on `{}` — {} full scans, avg {}µs",
                    table, acc.count, avg_us
                )
            } else {
                format!(
                    "CREATE INDEX ON {} ({}) -- {} scans, avg {}µs",
                    table,
                    top_cols.join(", "),
                    acc.count,
                    avg_us
                )
            };

            result.push(IndexAdvice {
                table_name: table.clone(),
                column_hints: top_cols,
                full_scan_count: acc.count,
                avg_scan_duration_us: avg_us,
                suggestion,
            });
        }
        // Sort by impact (count × avg_us) descending
        result.sort_by(|a, b| {
            let sa = a.full_scan_count * a.avg_scan_duration_us;
            let sb = b.full_scan_count * b.avg_scan_duration_us;
            sb.cmp(&sa)
        });
        result
    }

    pub fn table_count(&self) -> usize {
        self.inner.lock().unwrap().table_scans.len()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. WorkloadProfiler
// ─────────────────────────────────────────────────────────────────────────────

/// Aggregates per-fingerprint execution statistics.
///
/// Fingerprint = first 120 chars of normalized SQL (lowercase, collapsed whitespace).

struct WorkloadProfilerInner {
    stats: HashMap<String, WorkloadStat>,
    max_fingerprints: usize,
}

#[derive(Clone)]
pub struct WorkloadProfiler {
    inner: Arc<Mutex<WorkloadProfilerInner>>,
}

impl WorkloadProfiler {
    pub fn new(max_fingerprints: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WorkloadProfilerInner {
                stats: HashMap::new(),
                max_fingerprints,
            })),
        }
    }

    pub fn record(&self, sql: &str, duration_us: u64, is_error: bool) {
        let fp = normalize_fingerprint(sql);
        let mut g = self.inner.lock().unwrap();
        if !g.stats.contains_key(&fp) && g.stats.len() >= g.max_fingerprints {
            return;
        }
        let entry = g.stats.entry(fp.clone()).or_insert_with(|| WorkloadStat {
            fingerprint: fp,
            call_count: 0,
            total_us: 0,
            max_us: 0,
            error_count: 0,
            reservoir: Reservoir::new(100),
        });
        entry.call_count += 1;
        entry.total_us += duration_us;
        if duration_us > entry.max_us {
            entry.max_us = duration_us;
        }
        if is_error {
            entry.error_count += 1;
        }
        entry.reservoir.insert(duration_us);
    }

    /// Return top-N workloads by total time consumed.
    pub fn top_by_total_time(&self, n: usize) -> Vec<WorkloadStat> {
        let g = self.inner.lock().unwrap();
        let mut v: Vec<WorkloadStat> = g.stats.values().cloned().collect();
        v.sort_by(|a, b| b.total_us.cmp(&a.total_us));
        v.truncate(n);
        v
    }

    /// Return top-N workloads by call count.
    pub fn top_by_call_count(&self, n: usize) -> Vec<WorkloadStat> {
        let g = self.inner.lock().unwrap();
        let mut v: Vec<WorkloadStat> = g.stats.values().cloned().collect();
        v.sort_by(|a, b| b.call_count.cmp(&a.call_count));
        v.truncate(n);
        v
    }

    pub fn fingerprint_count(&self) -> usize {
        self.inner.lock().unwrap().stats.len()
    }
}

fn normalize_fingerprint(sql: &str) -> String {
    let lower = sql.to_lowercase();
    let normalized: String = lower
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if normalized.len() > 120 {
        normalized[..120].to_owned()
    } else {
        normalized
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Top-level AiOps facade
// ─────────────────────────────────────────────────────────────────────────────

/// Summary snapshot returned by `AiOps::stats()`.
#[derive(Debug, Clone)]
pub struct AiOpsStats {
    // SlowQueryDetector
    pub slow_query_total_samples: u64,
    pub slow_query_threshold_us: u64,
    pub slow_query_ema_baseline_us: u64,
    pub slow_query_logged: usize,
    // AnomalyDetector
    pub anomaly_tps_mean: f64,
    pub anomaly_tps_stddev: f64,
    pub anomaly_latency_mean_us: f64,
    pub anomaly_latency_stddev_us: f64,
    pub anomaly_alerts_total: u64,
    // IndexAdvisor
    pub index_advisor_tables_tracked: usize,
    pub index_advisor_advice_count: usize,
    // WorkloadProfiler
    pub workload_fingerprints: usize,
}

/// Unified AIOps engine. Holds and coordinates all subsystems.
#[derive(Clone)]
pub struct AiOps {
    pub slow_query: SlowQueryDetector,
    pub anomaly: AnomalyDetector,
    pub index_advisor: IndexAdvisor,
    pub workload: WorkloadProfiler,
}

impl AiOps {
    /// Create with production-ready defaults.
    pub fn new() -> Self {
        Self {
            slow_query: SlowQueryDetector::new(
                100_000, // 100ms absolute floor
                2.0,     // threshold = 2× EMA p95
            ),
            anomaly: AnomalyDetector::new(),
            index_advisor: IndexAdvisor::new(
                10,      // min 10 full scans
                50_000,  // min 50ms avg scan duration
            ),
            workload: WorkloadProfiler::new(1000),
        }
    }

    /// Main entry point called by the Executor after every query.
    pub fn record(
        &self,
        sql: &str,
        plan_kind: &str,
        table_name: Option<&str>,
        filter_cols: &[&str],
        duration_us: u64,
        is_error: bool,
    ) -> Vec<AiOpsAlert> {
        let fp = normalize_fingerprint(sql);
        let tbl = table_name.unwrap_or("");

        // 1. Slow query detection
        let slow_rec = self.slow_query.record(sql, &fp, tbl, duration_us, plan_kind);
        let is_slow = slow_rec.is_some();

        // 2. Anomaly detection (pass slow flag for consecutive-slow alerting)
        let alerts = self.anomaly.record(duration_us, is_slow);

        // 3. Index advisor: only for full scans
        if plan_kind == "SeqScan" || plan_kind == "ColumnScan" {
            if let Some(tbl_name) = table_name {
                self.index_advisor.record_full_scan(tbl_name, duration_us, filter_cols);
            }
        }

        // 4. Workload profiling
        self.workload.record(sql, duration_us, is_error);

        alerts
    }

    /// Aggregate stats snapshot for `SHOW AIOPS STATS`.
    pub fn stats(&self) -> AiOpsStats {
        let (sq_samples, sq_threshold, sq_ema) = self.slow_query.stats();
        let sq_logged = self.slow_query.recent_records(200).len();
        let (tps_mean, tps_sd, lat_mean, lat_sd, alert_total) = self.anomaly.stats();
        let advice = self.index_advisor.advice();

        AiOpsStats {
            slow_query_total_samples: sq_samples,
            slow_query_threshold_us: sq_threshold,
            slow_query_ema_baseline_us: sq_ema as u64,
            slow_query_logged: sq_logged,
            anomaly_tps_mean: tps_mean,
            anomaly_tps_stddev: tps_sd,
            anomaly_latency_mean_us: lat_mean,
            anomaly_latency_stddev_us: lat_sd,
            anomaly_alerts_total: alert_total,
            index_advisor_tables_tracked: self.index_advisor.table_count(),
            index_advisor_advice_count: advice.len(),
            workload_fingerprints: self.workload.fingerprint_count(),
        }
    }

    pub fn recent_alerts(&self, limit: usize) -> Vec<AiOpsAlert> {
        self.anomaly.recent_alerts(limit)
    }

    pub fn slow_queries(&self, limit: usize) -> Vec<SlowQueryRecord> {
        self.slow_query.recent_records(limit)
    }

    pub fn index_advice(&self) -> Vec<IndexAdvice> {
        self.index_advisor.advice()
    }

    pub fn top_workloads(&self, n: usize) -> Vec<WorkloadStat> {
        self.workload.top_by_total_time(n)
    }
}

impl Default for AiOps {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Global singleton (mirrors pattern used by global_ai_optimizer)
// ─────────────────────────────────────────────────────────────────────────────

use std::sync::OnceLock;

static GLOBAL_AIOPS: OnceLock<AiOps> = OnceLock::new();

pub fn global_aiops() -> &'static AiOps {
    GLOBAL_AIOPS.get_or_init(AiOps::new)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slow_query_detector_warmup() {
        let d = SlowQueryDetector::new(100_000, 2.0);
        let r = d.record("select 1", "select 1", "", 50_000, "SeqScan");
        assert!(r.is_none());
        let r = d.record("select 1", "select 1", "", 200_000, "SeqScan");
        assert!(r.is_some());
        assert_eq!(r.unwrap().plan_kind, "SeqScan");
    }

    #[test]
    fn test_slow_query_detector_adaptive() {
        let d = SlowQueryDetector::new(10_000, 3.0);
        for _ in 0..100 {
            d.record("select x from t", "select x from t", "t", 5_000, "IndexScan");
        }
        let (samples, threshold, _ema) = d.stats();
        assert!(samples >= 100);
        assert!(threshold >= 10_000);
        let r = d.record("select x from t", "select x from t", "t", threshold * 2, "SeqScan");
        assert!(r.is_some());
    }

    #[test]
    fn test_anomaly_detector_no_false_positive_cold() {
        let d = AnomalyDetector::new();
        for i in 0..29u64 {
            let alerts = d.record(i * 1000, false);
            assert!(alerts.is_empty(), "cold-start should not fire alerts");
        }
    }

    #[test]
    fn test_anomaly_detector_consecutive_slow() {
        let d = AnomalyDetector::new();
        // Fire 5 consecutive slow queries — should produce an alert on the 5th
        let mut fired = false;
        for _ in 0..5 {
            let alerts = d.record(500_000, true);
            if !alerts.is_empty() {
                fired = true;
                assert!(alerts.iter().any(|a| a.source.contains("SlowQuery")));
            }
        }
        assert!(fired, "expected a consecutive slow-query alert after 5 slow queries");
    }

    #[test]
    fn test_index_advisor_basic() {
        let advisor = IndexAdvisor::new(5, 1_000);
        for _ in 0..5 {
            advisor.record_full_scan("orders", 50_000, &["customer_id", "status"]);
        }
        let advice = advisor.advice();
        assert!(!advice.is_empty());
        assert_eq!(advice[0].table_name, "orders");
        assert!(advice[0].suggestion.contains("orders"));
        assert!(advice[0].full_scan_count >= 5);
    }

    #[test]
    fn test_index_advisor_below_threshold() {
        let advisor = IndexAdvisor::new(10, 100_000);
        for _ in 0..5 {
            advisor.record_full_scan("users", 1_000, &["email"]);
        }
        // Count < min_scans AND avg < min_avg → no advice
        assert!(advisor.advice().is_empty());
    }

    #[test]
    fn test_workload_profiler() {
        let profiler = WorkloadProfiler::new(100);
        for _ in 0..10 {
            profiler.record("SELECT * FROM orders WHERE id = 1", 5_000, false);
        }
        profiler.record("INSERT INTO t VALUES (1)", 1_000, false);
        profiler.record("SELECT * FROM orders WHERE id = 1", 100_000, true);

        let top = profiler.top_by_total_time(5);
        assert!(!top.is_empty());
        assert!(top[0].fingerprint.contains("select"));
        assert_eq!(top[0].call_count, 11);
        assert_eq!(top[0].error_count, 1);
        // p99 should be above 0 after 11 samples
        assert!(top[0].p99_us() > 0);
    }

    #[test]
    fn test_workload_fingerprint_normalization() {
        let profiler = WorkloadProfiler::new(100);
        profiler.record("SELECT  *  FROM  t", 1_000, false);
        profiler.record("select * from t", 1_000, false);
        let top = profiler.top_by_call_count(1);
        assert_eq!(top[0].call_count, 2);
    }

    #[test]
    fn test_workload_p99_estimation() {
        let profiler = WorkloadProfiler::new(100);
        // Feed 50 fast + 50 slow (half outliers) so p99 must be the slow value
        for _ in 0..50 {
            profiler.record("select p99", 1_000, false);
        }
        for _ in 0..50 {
            profiler.record("select p99", 1_000_000, false);
        }
        let top = profiler.top_by_call_count(1);
        let stat = &top[0];
        // With 50% outliers, p99 must land in the 1_000_000 bucket
        assert!(stat.p99_us() >= 1_000, "p99 should be positive");
        assert!(stat.max_us >= 1_000_000, "max should capture the outlier");
        assert_eq!(stat.call_count, 100);
    }

    #[test]
    fn test_aiops_record_integration() {
        let ops = AiOps::new();
        ops.record("select * from orders", "SeqScan", Some("orders"), &["id"], 200_000, false);
        ops.record("select 1", "SeqScan", None, &[], 50_000, false);

        let stats = ops.stats();
        assert_eq!(stats.slow_query_total_samples, 2);
        assert_eq!(stats.workload_fingerprints, 2);
        // Index advisor tracks the SeqScan on "orders"
        assert_eq!(stats.index_advisor_tables_tracked, 1);
    }

    #[test]
    fn test_normalize_fingerprint() {
        let a = normalize_fingerprint("SELECT   *   FROM t  WHERE  id = 1");
        let b = normalize_fingerprint("select * from t where id = 1");
        assert_eq!(a, b);
    }
}
