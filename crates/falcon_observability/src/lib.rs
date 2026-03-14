//! Observability setup: structured logging, metrics (Prometheus), tracing.
#![allow(clippy::too_many_arguments)]

pub mod stmt_stats;

use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

/// Guard that must be kept alive for the process lifetime to flush async log writers.
pub type LogGuard = tracing_appender::non_blocking::WorkerGuard;

/// Custom timer: local time with millisecond precision.
struct LocalTimer;

impl tracing_subscriber::fmt::time::FormatTime for LocalTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = chrono::Local::now();
        write!(w, "{}", now.format("%Y-%m-%dT%H:%M:%S%.3f%:z"))
    }
}

/// Initialize tracing from a `LoggingConfig`. Returns guards that flush pending writes on drop.
///
/// Supports:
/// - `level`: trace/debug/info/warn/error (overridden by RUST_LOG env var)
/// - `stderr`: write to stderr
/// - `file`: optional log file path; rotation controlled by `rotation` + `max_size_mb`
/// - `format`: "text" or "json"
/// - `max_files`: prune old rotated files after init
pub fn init_tracing_from_config(cfg: &falcon_common::config::LoggingConfig) -> Vec<LogGuard> {
    use tracing_appender::rolling::{RollingFileAppender, Rotation};
    use tracing_subscriber::Registry;

    let mut guards = Vec::new();

    // EnvFilter: RUST_LOG overrides config level
    let filter_str = std::env::var("RUST_LOG").unwrap_or_else(|_| cfg.level.clone());
    let env_filter = EnvFilter::new(&filter_str);
    let use_json = cfg.format.eq_ignore_ascii_case("json");

    let registry = Registry::default().with(env_filter);

    // Build the file appender (optional)
    let file_writer: Option<tracing_appender::non_blocking::NonBlocking> = if !cfg.file.is_empty() {
        let path = std::path::Path::new(&cfg.file);
        let log_dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let filename = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "falcondb.log".to_owned());

        let _ = std::fs::create_dir_all(log_dir);

        if cfg.max_files > 0 {
            prune_old_log_files(log_dir, &filename, cfg.max_files);
        }

        let rotation = match cfg.rotation.to_lowercase().as_str() {
            "hourly" => Rotation::HOURLY,
            "never" | "size" => Rotation::NEVER,
            _ => Rotation::DAILY,
        };

        let appender = RollingFileAppender::new(rotation, log_dir, &filename);
        let (nb, guard) = tracing_appender::non_blocking(appender);
        guards.push(guard);
        Some(nb)
    } else {
        None
    };

    // Compose layers using Option<Layer> — tracing_subscriber implements Layer for Option<L>.
    // stderr layer (text or json, mutually exclusive)
    let stderr_text = if cfg.stderr && !use_json {
        Some(
            fmt::layer()
                .with_timer(LocalTimer)
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_ansi(false)
                .with_writer(std::io::stderr),
        )
    } else {
        None
    };

    let stderr_json = if cfg.stderr && use_json {
        Some(
            fmt::layer()
                .json()
                .with_timer(LocalTimer)
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_writer(std::io::stderr),
        )
    } else {
        None
    };

    // file layer — build inside if/else so file_writer is consumed by exactly one branch
    if use_json {
        let file_json = file_writer.map(|w| {
            fmt::layer()
                .json()
                .with_timer(LocalTimer)
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_writer(w)
        });
        registry.with(stderr_json).with(file_json).init();
    } else {
        let file_text = file_writer.map(|w| {
            fmt::layer()
                .with_timer(LocalTimer)
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_ansi(false)
                .with_writer(w)
        });
        registry.with(stderr_text).with(file_text).init();
    }

    guards
}

/// Initialize tracing with defaults (stderr, info level). Used before config is loaded.
pub fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = fmt::layer()
        .with_timer(LocalTimer)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_ansi(false);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}

/// Remove old rotated log files, keeping only the most recent `keep` files.
fn prune_old_log_files(dir: &std::path::Path, prefix: &str, keep: usize) {
    let base = prefix.split('.').next().unwrap_or(prefix);
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    let mut files: Vec<_> = entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with(base) && e.path().is_file()
        })
        .collect();

    // Sort by modified time descending (newest first)
    files.sort_by(|a, b| {
        let ta = a.metadata().and_then(|m| m.modified()).ok();
        let tb = b.metadata().and_then(|m| m.modified()).ok();
        tb.cmp(&ta)
    });

    for old in files.iter().skip(keep) {
        let _ = std::fs::remove_file(old.path());
    }
}

/// Initialize Prometheus metrics exporter.
/// Returns the listen address for the metrics endpoint.
pub fn init_metrics(listen_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let addr: std::net::SocketAddr = listen_addr.parse()?;
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()?;
    tracing::info!("Prometheus metrics endpoint on http://{}/metrics", addr);
    Ok(())
}

/// Record common database metrics.
pub fn record_query_metrics(duration_us: u64, query_type: &str, success: bool) {
    let success_str = if success { "true" } else { "false" };
    metrics::counter!("falcon_queries_total", "type" => query_type.to_owned(), "success" => success_str.to_owned()).increment(1);
    metrics::histogram!("falcon_query_duration_us", "type" => query_type.to_owned())
        .record(duration_us as f64);
}

pub fn record_txn_metrics(action: &str) {
    match action {
        "committed" => metrics::counter!("falcon_txn_committed_total").increment(1),
        "aborted" => metrics::counter!("falcon_txn_aborted_total").increment(1),
        _ => {}
    }
}

pub fn record_txn_active_count(count: u64) {
    metrics::gauge!("falcon_txn_active_count").set(count as f64);
}

pub fn init_txn_counters() {
    metrics::counter!("falcon_txn_committed_total").absolute(0);
    metrics::counter!("falcon_txn_aborted_total").absolute(0);
}

pub fn record_active_connections(count: usize) {
    metrics::gauge!("falcon_active_connections").set(count as f64);
}

pub fn record_storage_metrics(table_count: usize, total_rows_approx: usize) {
    metrics::gauge!("falcon_table_count").set(table_count as f64);
    metrics::gauge!("falcon_total_rows_approx").set(total_rows_approx as f64);
}

/// Record memory pressure and usage metrics from a MemorySnapshot.
pub fn record_memory_metrics(
    mvcc_bytes: u64,
    index_bytes: u64,
    write_buffer_bytes: u64,
    total_bytes: u64,
    soft_limit: u64,
    hard_limit: u64,
    pressure_state: &str,
    admission_rejections: u64,
) {
    metrics::gauge!("falcon_memory_mvcc_bytes").set(mvcc_bytes as f64);
    metrics::gauge!("falcon_memory_index_bytes").set(index_bytes as f64);
    metrics::gauge!("falcon_memory_write_buffer_bytes").set(write_buffer_bytes as f64);
    metrics::gauge!("falcon_memory_total_bytes").set(total_bytes as f64);
    metrics::gauge!("falcon_memory_soft_limit_bytes").set(soft_limit as f64);
    metrics::gauge!("falcon_memory_hard_limit_bytes").set(hard_limit as f64);
    // Encode pressure state as numeric: 0=normal, 1=pressure, 2=critical
    let pressure_num = match pressure_state {
        "pressure" => 1.0,
        "critical" => 2.0,
        _ => 0.0,
    };
    metrics::gauge!("falcon_memory_pressure_state").set(pressure_num);
    metrics::gauge!("falcon_memory_admission_rejections").set(admission_rejections as f64);
}

// ---------------------------------------------------------------------------
// Prepared Statement / Extended Query Protocol metrics
// ---------------------------------------------------------------------------

/// Record an extended-query-protocol operation.
///
/// `op` should be one of: "parse", "bind", "execute", "describe", "close".
/// `path` indicates execution path: "plan" (plan-based) or "legacy" (text-substitution).
pub fn record_prepared_stmt_op(op: &str, path: &str) {
    metrics::counter!(
        "falcon_prepared_stmt_ops_total",
        "op" => op.to_owned(),
        "path" => path.to_owned()
    )
    .increment(1);
}

/// Record a SQL-level prepared statement command (PREPARE, EXECUTE, DEALLOCATE).
/// `cmd` should be one of: "prepare", "execute", "deallocate".
pub fn record_prepared_stmt_sql_cmd(cmd: &str) {
    metrics::counter!(
        "falcon_prepared_stmt_sql_cmds_total",
        "cmd" => cmd.to_owned()
    )
    .increment(1);
}

/// Record the duration of a Parse operation (prepare_statement) in microseconds.
pub fn record_prepared_stmt_parse_duration_us(duration_us: u64, success: bool) {
    metrics::histogram!(
        "falcon_prepared_stmt_parse_duration_us",
        "success" => success.to_string()
    )
    .record(duration_us as f64);
}

/// Record the duration of a Bind operation in microseconds.
pub fn record_prepared_stmt_bind_duration_us(duration_us: u64) {
    metrics::histogram!("falcon_prepared_stmt_bind_duration_us").record(duration_us as f64);
}

/// Record the duration of an Execute (extended query) operation in microseconds.
pub fn record_prepared_stmt_execute_duration_us(duration_us: u64, success: bool) {
    metrics::histogram!(
        "falcon_prepared_stmt_execute_duration_us",
        "success" => success.to_string()
    )
    .record(duration_us as f64);
}

/// Record the number of parameters bound in a single Bind operation.
pub fn record_prepared_stmt_param_count(count: usize) {
    metrics::histogram!("falcon_prepared_stmt_param_count").record(count as f64);
}

/// Record the current number of active prepared statements in a session.
pub fn record_prepared_stmt_active(count: usize) {
    metrics::gauge!("falcon_prepared_stmt_active").set(count as f64);
}

/// Record the current number of active portals in a session.
pub fn record_prepared_stmt_portals_active(count: usize) {
    metrics::gauge!("falcon_prepared_stmt_portals_active").set(count as f64);
}

// ---------------------------------------------------------------------------
// P2-6: Enterprise metrics (tenants, WAL stability, replication, backup, audit)
// ---------------------------------------------------------------------------

/// Record per-tenant metrics.
pub fn record_tenant_metrics(
    tenant_id: u64,
    tenant_name: &str,
    active_txns: u32,
    memory_bytes: u64,
    current_qps: f64,
    txns_committed: u64,
    txns_aborted: u64,
    quota_exceeded_count: u64,
) {
    let tid = tenant_id.to_string();
    metrics::gauge!("falcon_tenant_active_txns", "tenant_id" => tid.clone(), "tenant_name" => tenant_name.to_owned()).set(f64::from(active_txns));
    metrics::gauge!("falcon_tenant_memory_bytes", "tenant_id" => tid.clone(), "tenant_name" => tenant_name.to_owned()).set(memory_bytes as f64);
    metrics::gauge!("falcon_tenant_qps", "tenant_id" => tid.clone(), "tenant_name" => tenant_name.to_owned()).set(current_qps);
    metrics::gauge!("falcon_tenant_txns_committed", "tenant_id" => tid.clone(), "tenant_name" => tenant_name.to_owned()).set(txns_committed as f64);
    metrics::gauge!("falcon_tenant_txns_aborted", "tenant_id" => tid.clone(), "tenant_name" => tenant_name.to_owned()).set(txns_aborted as f64);
    metrics::gauge!("falcon_tenant_quota_exceeded", "tenant_id" => tid, "tenant_name" => tenant_name.to_owned()).set(quota_exceeded_count as f64);
}

/// Record WAL stability metrics (P1-2 / P2-6).
pub fn record_wal_stability_metrics(
    fsync_total_us: u64,
    fsync_max_us: u64,
    fsync_avg_us: u64,
    group_commit_avg_size: f64,
    backlog_bytes: u64,
) {
    metrics::gauge!("falcon_wal_fsync_total_us").set(fsync_total_us as f64);
    metrics::gauge!("falcon_wal_fsync_max_us").set(fsync_max_us as f64);
    metrics::gauge!("falcon_wal_fsync_avg_us").set(fsync_avg_us as f64);
    metrics::gauge!("falcon_wal_group_commit_avg_size").set(group_commit_avg_size);
    metrics::gauge!("falcon_wal_backlog_bytes").set(backlog_bytes as f64);
}

/// Record replication / HA metrics (P1-3 / P2-6).
pub fn record_replication_metrics(
    leader_changes: u64,
    replication_lag_us: u64,
    max_replication_lag_us: u64,
    promote_count: u64,
) {
    metrics::gauge!("falcon_replication_leader_changes").set(leader_changes as f64);
    metrics::gauge!("falcon_replication_lag_us").set(replication_lag_us as f64);
    metrics::gauge!("falcon_replication_max_lag_us").set(max_replication_lag_us as f64);
    metrics::gauge!("falcon_replication_promote_count").set(promote_count as f64);
}

/// Record backup metrics (P2-4 / P2-6).
pub fn record_backup_metrics(total_completed: u64, total_failed: u64, total_bytes: u64) {
    metrics::gauge!("falcon_backup_completed_total").set(total_completed as f64);
    metrics::gauge!("falcon_backup_failed_total").set(total_failed as f64);
    metrics::gauge!("falcon_backup_bytes_total").set(total_bytes as f64);
}

/// Record audit event count.
pub fn record_audit_metrics(total_events: u64, buffered: u64) {
    metrics::gauge!("falcon_audit_events_total").set(total_events as f64);
    metrics::gauge!("falcon_audit_events_buffered").set(buffered as f64);
}

// ---------------------------------------------------------------------------
// P3: Commercialization & cloud metrics
// ---------------------------------------------------------------------------

/// Record per-tenant resource metering metrics (P3-3).
pub fn record_metering_metrics(
    tenant_id: u64,
    cpu_us: u64,
    memory_bytes: u64,
    storage_bytes: u64,
    query_count: u64,
    qps: f64,
    throttled: bool,
) {
    let tid = tenant_id.to_string();
    metrics::gauge!("falcon_metering_cpu_us", "tenant_id" => tid.clone()).set(cpu_us as f64);
    metrics::gauge!("falcon_metering_memory_bytes", "tenant_id" => tid.clone())
        .set(memory_bytes as f64);
    metrics::gauge!("falcon_metering_storage_bytes", "tenant_id" => tid.clone())
        .set(storage_bytes as f64);
    metrics::gauge!("falcon_metering_query_count", "tenant_id" => tid.clone())
        .set(query_count as f64);
    metrics::gauge!("falcon_metering_qps", "tenant_id" => tid.clone()).set(qps);
    metrics::gauge!("falcon_metering_throttled", "tenant_id" => tid).set(if throttled {
        1.0
    } else {
        0.0
    });
}

/// Record security posture metrics (P3-6).
pub fn record_security_metrics(
    encryption_enabled: bool,
    tls_enabled: bool,
    ip_allowlist_enabled: bool,
    blocked_by_ip: u64,
    total_connection_attempts: u64,
) {
    metrics::gauge!("falcon_security_encryption_enabled").set(if encryption_enabled {
        1.0
    } else {
        0.0
    });
    metrics::gauge!("falcon_security_tls_enabled").set(if tls_enabled { 1.0 } else { 0.0 });
    metrics::gauge!("falcon_security_ip_allowlist_enabled").set(if ip_allowlist_enabled {
        1.0
    } else {
        0.0
    });
    metrics::gauge!("falcon_security_blocked_by_ip").set(blocked_by_ip as f64);
    metrics::gauge!("falcon_security_connection_attempts").set(total_connection_attempts as f64);
}

/// Record health score metrics (P3-7).
pub fn record_health_metrics(
    overall_score: u32,
    component_count: usize,
    recommendation_count: usize,
) {
    metrics::gauge!("falcon_health_overall_score").set(f64::from(overall_score));
    metrics::gauge!("falcon_health_component_count").set(component_count as f64);
    metrics::gauge!("falcon_health_recommendation_count").set(recommendation_count as f64);
}

/// Record SLA priority latency metrics (P2-3 / P3).
pub fn record_sla_metrics(
    high_p99_us: u64,
    normal_p99_us: u64,
    bg_p99_us: u64,
    sla_violations_total: u64,
    sla_high_violations: u64,
    sla_normal_violations: u64,
) {
    metrics::gauge!("falcon_sla_high_p99_us").set(high_p99_us as f64);
    metrics::gauge!("falcon_sla_normal_p99_us").set(normal_p99_us as f64);
    metrics::gauge!("falcon_sla_bg_p99_us").set(bg_p99_us as f64);
    metrics::gauge!("falcon_sla_violations_total").set(sla_violations_total as f64);
    metrics::gauge!("falcon_sla_high_violations").set(sla_high_violations as f64);
    metrics::gauge!("falcon_sla_normal_violations").set(sla_normal_violations as f64);
}

// ---------------------------------------------------------------------------
// Crash domain metrics
// ---------------------------------------------------------------------------

/// Record crash domain panic statistics for Prometheus.
pub fn record_crash_domain_metrics(
    panic_count: u64,
    throttle_window_count: u64,
    throttle_triggered_count: u64,
    recent_panic_count: usize,
) {
    metrics::gauge!("falcon_crash_domain_panic_count").set(panic_count as f64);
    metrics::gauge!("falcon_crash_domain_throttle_window_count").set(throttle_window_count as f64);
    metrics::gauge!("falcon_crash_domain_throttle_triggered_count")
        .set(throttle_triggered_count as f64);
    metrics::gauge!("falcon_crash_domain_recent_panic_count").set(recent_panic_count as f64);
}

// ---------------------------------------------------------------------------
// Priority scheduler metrics
// ---------------------------------------------------------------------------

/// Record priority scheduler lane metrics for Prometheus.
pub fn record_priority_scheduler_metrics(
    high_active: usize,
    high_admitted: u64,
    high_rejected: u64,
    normal_active: usize,
    normal_max: usize,
    normal_admitted: u64,
    normal_rejected: u64,
    normal_wait_us: u64,
    low_active: usize,
    low_max: usize,
    low_admitted: u64,
    low_rejected: u64,
    low_wait_us: u64,
) {
    metrics::gauge!("falcon_scheduler_high_active").set(high_active as f64);
    metrics::gauge!("falcon_scheduler_high_admitted").set(high_admitted as f64);
    metrics::gauge!("falcon_scheduler_high_rejected").set(high_rejected as f64);
    metrics::gauge!("falcon_scheduler_normal_active").set(normal_active as f64);
    metrics::gauge!("falcon_scheduler_normal_max").set(normal_max as f64);
    metrics::gauge!("falcon_scheduler_normal_admitted").set(normal_admitted as f64);
    metrics::gauge!("falcon_scheduler_normal_rejected").set(normal_rejected as f64);
    metrics::gauge!("falcon_scheduler_normal_wait_us").set(normal_wait_us as f64);
    metrics::gauge!("falcon_scheduler_low_active").set(low_active as f64);
    metrics::gauge!("falcon_scheduler_low_max").set(low_max as f64);
    metrics::gauge!("falcon_scheduler_low_admitted").set(low_admitted as f64);
    metrics::gauge!("falcon_scheduler_low_rejected").set(low_rejected as f64);
    metrics::gauge!("falcon_scheduler_low_wait_us").set(low_wait_us as f64);
}

// ---------------------------------------------------------------------------
// Token bucket metrics
// ---------------------------------------------------------------------------

/// Record token bucket rate limiter metrics for Prometheus.
pub fn record_token_bucket_metrics(
    name: &str,
    rate_per_sec: u64,
    burst: u64,
    available_tokens: u64,
    paused: bool,
    total_consumed: u64,
    total_acquired: u64,
    total_rejected: u64,
    total_wait_us: u64,
) {
    let n = name.to_owned();
    metrics::gauge!("falcon_token_bucket_rate_per_sec", "bucket" => n.clone())
        .set(rate_per_sec as f64);
    metrics::gauge!("falcon_token_bucket_burst", "bucket" => n.clone()).set(burst as f64);
    metrics::gauge!("falcon_token_bucket_available", "bucket" => n.clone())
        .set(available_tokens as f64);
    metrics::gauge!("falcon_token_bucket_paused", "bucket" => n.clone()).set(if paused {
        1.0
    } else {
        0.0
    });
    metrics::gauge!("falcon_token_bucket_consumed", "bucket" => n.clone())
        .set(total_consumed as f64);
    metrics::gauge!("falcon_token_bucket_acquired", "bucket" => n.clone())
        .set(total_acquired as f64);
    metrics::gauge!("falcon_token_bucket_rejected", "bucket" => n.clone())
        .set(total_rejected as f64);
    metrics::gauge!("falcon_token_bucket_wait_us", "bucket" => n).set(total_wait_us as f64);
}

// ---------------------------------------------------------------------------
// Deterministic 2PC metrics
// ---------------------------------------------------------------------------

/// Record coordinator decision log metrics for Prometheus.
pub fn record_decision_log_metrics(
    total_logged: u64,
    total_applied: u64,
    current_entries: usize,
    unapplied_count: usize,
) {
    metrics::gauge!("falcon_2pc_decision_log_total_logged").set(total_logged as f64);
    metrics::gauge!("falcon_2pc_decision_log_total_applied").set(total_applied as f64);
    metrics::gauge!("falcon_2pc_decision_log_entries").set(current_entries as f64);
    metrics::gauge!("falcon_2pc_decision_log_unapplied").set(unapplied_count as f64);
}

/// Record layered timeout controller metrics for Prometheus.
pub fn record_layered_timeout_metrics(
    total_soft_timeouts: u64,
    total_hard_timeouts: u64,
    total_shard_timeouts: u64,
) {
    metrics::gauge!("falcon_2pc_soft_timeouts").set(total_soft_timeouts as f64);
    metrics::gauge!("falcon_2pc_hard_timeouts").set(total_hard_timeouts as f64);
    metrics::gauge!("falcon_2pc_shard_timeouts").set(total_shard_timeouts as f64);
}

/// Record slow-shard tracker metrics for Prometheus.
pub fn record_slow_shard_metrics(
    total_slow_detected: u64,
    total_fast_aborts: u64,
    total_hedged_sent: u64,
    total_hedged_won: u64,
    hedged_inflight: u64,
) {
    metrics::gauge!("falcon_2pc_slow_shard_detected").set(total_slow_detected as f64);
    metrics::gauge!("falcon_2pc_slow_shard_fast_aborts").set(total_fast_aborts as f64);
    metrics::gauge!("falcon_2pc_slow_shard_hedged_sent").set(total_hedged_sent as f64);
    metrics::gauge!("falcon_2pc_slow_shard_hedged_won").set(total_hedged_won as f64);
    metrics::gauge!("falcon_2pc_slow_shard_hedged_inflight").set(hedged_inflight as f64);
}

// ---------------------------------------------------------------------------
// Fault injection / chaos metrics
// ---------------------------------------------------------------------------

/// Record fault injection metrics for Prometheus.
pub fn record_fault_injection_metrics(
    faults_fired: u64,
    partition_active: bool,
    partition_count: u64,
    partition_heal_count: u64,
    partition_events: u64,
    jitter_enabled: bool,
    jitter_events: u64,
) {
    metrics::gauge!("falcon_chaos_faults_fired").set(faults_fired as f64);
    metrics::gauge!("falcon_chaos_partition_active").set(if partition_active { 1.0 } else { 0.0 });
    metrics::gauge!("falcon_chaos_partition_count").set(partition_count as f64);
    metrics::gauge!("falcon_chaos_partition_heal_count").set(partition_heal_count as f64);
    metrics::gauge!("falcon_chaos_partition_events").set(partition_events as f64);
    metrics::gauge!("falcon_chaos_jitter_enabled").set(if jitter_enabled { 1.0 } else { 0.0 });
    metrics::gauge!("falcon_chaos_jitter_events").set(jitter_events as f64);
}

// ---------------------------------------------------------------------------
// Security hardening metrics
// ---------------------------------------------------------------------------

/// Record security hardening metrics for Prometheus.
pub fn record_security_hardening_metrics(
    auth_total_checks: u64,
    auth_total_lockouts: u64,
    auth_total_failures: u64,
    auth_active_lockouts: usize,
    pw_total_checks: u64,
    pw_total_rejections: u64,
    fw_total_checks: u64,
    fw_total_blocked: u64,
    fw_injection_detected: u64,
    fw_dangerous_blocked: u64,
    fw_stacking_blocked: u64,
) {
    metrics::gauge!("falcon_security_auth_checks").set(auth_total_checks as f64);
    metrics::gauge!("falcon_security_auth_lockouts").set(auth_total_lockouts as f64);
    metrics::gauge!("falcon_security_auth_failures").set(auth_total_failures as f64);
    metrics::gauge!("falcon_security_auth_active_lockouts").set(auth_active_lockouts as f64);
    metrics::gauge!("falcon_security_password_checks").set(pw_total_checks as f64);
    metrics::gauge!("falcon_security_password_rejections").set(pw_total_rejections as f64);
    metrics::gauge!("falcon_security_firewall_checks").set(fw_total_checks as f64);
    metrics::gauge!("falcon_security_firewall_blocked").set(fw_total_blocked as f64);
    metrics::gauge!("falcon_security_firewall_injection").set(fw_injection_detected as f64);
    metrics::gauge!("falcon_security_firewall_dangerous").set(fw_dangerous_blocked as f64);
    metrics::gauge!("falcon_security_firewall_stacking").set(fw_stacking_blocked as f64);
}

// ---------------------------------------------------------------------------
// Version / compatibility metrics
// ---------------------------------------------------------------------------

/// Record version and compatibility metrics for Prometheus.
pub fn record_compat_metrics(wal_format_version: u32, snapshot_format_version: u32) {
    metrics::gauge!("falcon_compat_wal_format_version").set(f64::from(wal_format_version));
    metrics::gauge!("falcon_compat_snapshot_format_version")
        .set(f64::from(snapshot_format_version));
}

/// Record transaction fast-path / slow-path statistics from a TxnStatsSnapshot.
pub fn record_txn_stats(
    total_committed: u64,
    fast_path_commits: u64,
    slow_path_commits: u64,
    total_aborted: u64,
    occ_conflicts: u64,
    degraded_to_global: u64,
    active_count: usize,
) {
    metrics::gauge!("falcon_txn_committed_total").set(total_committed as f64);
    metrics::gauge!("falcon_txn_fast_path_commits").set(fast_path_commits as f64);
    metrics::gauge!("falcon_txn_slow_path_commits").set(slow_path_commits as f64);
    metrics::gauge!("falcon_txn_aborted_total").set(total_aborted as f64);
    metrics::gauge!("falcon_txn_occ_conflicts").set(occ_conflicts as f64);
    metrics::gauge!("falcon_txn_degraded_to_global").set(degraded_to_global as f64);
    metrics::gauge!("falcon_txn_active_count").set(active_count as f64);
}

// ---------------------------------------------------------------------------
// Background task supervisor metrics (D4-1)
// ---------------------------------------------------------------------------

/// Record a background task failure event for Prometheus.
pub fn record_bg_task_failed(task_name: &str) {
    metrics::counter!(
        "falcon_bg_task_failed_total",
        "task" => task_name.to_owned()
    )
    .increment(1);
}

/// Record background task supervisor summary metrics.
pub fn record_bg_supervisor_metrics(
    total_tasks: usize,
    running: usize,
    failed: usize,
    node_health_degraded: bool,
) {
    metrics::gauge!("falcon_bg_tasks_total").set(total_tasks as f64);
    metrics::gauge!("falcon_bg_tasks_running").set(running as f64);
    metrics::gauge!("falcon_bg_tasks_failed").set(failed as f64);
    metrics::gauge!("falcon_bg_node_degraded").set(if node_health_degraded { 1.0 } else { 0.0 });
}

// ---------------------------------------------------------------------------
// Lock contention metrics (D6-1)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// v1.0.4: Determinism & failure safety metrics
// ---------------------------------------------------------------------------

/// Record transaction terminal state metrics (v1.0.4 §2).
/// Every transaction MUST end with exactly one call to this function.
pub fn record_txn_terminal_state(
    terminal_type: &str, // "committed", "aborted_retryable", "aborted_non_retryable", "rejected", "indeterminate"
    reason: &str,        // abort/reject reason or "" for committed
    sqlstate: &str,
) {
    metrics::counter!(
        "falcon_txn_terminal_total",
        "type" => terminal_type.to_owned(),
        "reason" => reason.to_owned(),
        "sqlstate" => sqlstate.to_owned()
    )
    .increment(1);
}

/// Record per-resource admission rejection (v1.0.4 §1).
pub fn record_admission_rejection(resource_name: &str, sqlstate: &str) {
    metrics::counter!(
        "falcon_admission_rejection_total",
        "resource" => resource_name.to_owned(),
        "sqlstate" => sqlstate.to_owned()
    )
    .increment(1);
}

/// Record failover recovery duration (v1.0.4 §3).
pub fn record_failover_recovery_duration_ms(duration_ms: u64, outcome: &str) {
    metrics::histogram!(
        "falcon_failover_recovery_duration_ms",
        "outcome" => outcome.to_owned()
    )
    .record(duration_ms as f64);
    metrics::counter!(
        "falcon_failover_recovery_total",
        "outcome" => outcome.to_owned()
    )
    .increment(1);
}

/// Record queue depth metrics (v1.0.4 §4).
pub fn record_queue_depth_metrics(
    queue_name: &str,
    depth: u64,
    capacity: u64,
    peak: u64,
    total_enqueued: u64,
    total_rejected: u64,
) {
    let n = queue_name.to_owned();
    metrics::gauge!("falcon_queue_depth", "queue" => n.clone()).set(depth as f64);
    metrics::gauge!("falcon_queue_capacity", "queue" => n.clone()).set(capacity as f64);
    metrics::gauge!("falcon_queue_peak", "queue" => n.clone()).set(peak as f64);
    metrics::gauge!("falcon_queue_enqueued_total", "queue" => n.clone()).set(total_enqueued as f64);
    metrics::gauge!("falcon_queue_rejected_total", "queue" => n).set(total_rejected as f64);
}

/// Record idempotent replay metrics (v1.0.4 §6).
pub fn record_replay_metrics(replayed_count: u64, duplicate_count: u64, violation_count: u64) {
    metrics::gauge!("falcon_replay_replayed_total").set(replayed_count as f64);
    metrics::gauge!("falcon_replay_duplicate_total").set(duplicate_count as f64);
    metrics::gauge!("falcon_replay_violation_total").set(violation_count as f64);
}

// ── P2: SLA Admission Metrics ──────────────────────────────────────────

/// Record SLA admission controller metrics (P2-1).
pub fn record_sla_admission_metrics(
    p99_us: u64,
    p999_us: u64,
    target_p99_ms: f64,
    target_p999_ms: f64,
    inflight_fast: u64,
    inflight_slow: u64,
    inflight_ddl: u64,
    total_accepted: u64,
    total_rejected: u64,
) {
    metrics::gauge!("falcon_sla_p99_us").set(p99_us as f64);
    metrics::gauge!("falcon_sla_p999_us").set(p999_us as f64);
    metrics::gauge!("falcon_sla_target_p99_ms").set(target_p99_ms);
    metrics::gauge!("falcon_sla_target_p999_ms").set(target_p999_ms);
    metrics::gauge!("falcon_sla_inflight_fast").set(inflight_fast as f64);
    metrics::gauge!("falcon_sla_inflight_slow").set(inflight_slow as f64);
    metrics::gauge!("falcon_sla_inflight_ddl").set(inflight_ddl as f64);
    metrics::gauge!("falcon_sla_total_accepted").set(total_accepted as f64);
    metrics::gauge!("falcon_sla_total_rejected").set(total_rejected as f64);
}

/// Record SLA admission rejection by signal (P2-1).
pub fn record_sla_rejection(signal: &str, txn_class: &str) {
    metrics::counter!(
        "falcon_sla_rejection_total",
        "signal" => signal.to_owned(),
        "txn_class" => txn_class.to_owned()
    )
    .increment(1);
}

// ── P2: GC Budget Metrics ─────────────────────────────────────────────

/// Record GC budget metrics (P2-2).
pub fn record_gc_budget_metrics(
    sweep_duration_us: u64,
    budget_exhausted: bool,
    key_budget_exhausted: bool,
    latency_throttled: bool,
    reclaimed_versions: u64,
    reclaimed_bytes: u64,
    max_chain_length: u64,
    mean_chain_length: f64,
) {
    metrics::histogram!("falcon_gc_sweep_duration_us").record(sweep_duration_us as f64);
    metrics::gauge!("falcon_gc_reclaimed_versions").set(reclaimed_versions as f64);
    metrics::gauge!("falcon_gc_reclaimed_bytes").set(reclaimed_bytes as f64);
    metrics::gauge!("falcon_gc_max_chain_length").set(max_chain_length as f64);
    metrics::gauge!("falcon_gc_mean_chain_length").set(mean_chain_length);
    if budget_exhausted {
        metrics::counter!("falcon_gc_budget_exhausted_total").increment(1);
    }
    if key_budget_exhausted {
        metrics::counter!("falcon_gc_key_budget_exhausted_total").increment(1);
    }
    if latency_throttled {
        metrics::counter!("falcon_gc_latency_throttled_total").increment(1);
    }
}

// ── P2: Network Transfer Metrics ──────────────────────────────────────

/// Record distributed query network transfer metrics (P2-3).
pub fn record_dist_query_network_metrics(
    bytes_in: u64,
    bytes_out: u64,
    rows_in: u64,
    rows_out: u64,
    limit_pushed: bool,
    partial_agg: bool,
    filter_pushed: bool,
) {
    metrics::counter!("falcon_dist_query_bytes_in_total").increment(bytes_in);
    metrics::counter!("falcon_dist_query_bytes_out_total").increment(bytes_out);
    metrics::counter!("falcon_dist_query_rows_in_total").increment(rows_in);
    metrics::counter!("falcon_dist_query_rows_out_total").increment(rows_out);
    metrics::counter!("falcon_dist_query_total").increment(1);
    if limit_pushed {
        metrics::counter!("falcon_dist_query_limit_pushdown_total").increment(1);
    }
    if partial_agg {
        metrics::counter!("falcon_dist_query_partial_agg_total").increment(1);
    }
    if filter_pushed {
        metrics::counter!("falcon_dist_query_filter_pushdown_total").increment(1);
    }
    if bytes_in > bytes_out {
        metrics::counter!("falcon_dist_query_bytes_saved_total").increment(bytes_in - bytes_out);
    }
}

// ---------------------------------------------------------------------------
// Distributed cluster metrics (P2 Observability)
// ---------------------------------------------------------------------------

/// Record smart gateway routing metrics.
pub fn record_gateway_routing_metrics(
    local_exec: u64,
    forward_to_leader: u64,
    reject_overloaded: u64,
    reject_no_route: u64,
    inflight: u64,
    forwarded: u64,
    forward_failed: u64,
    client_connects: u64,
    client_failovers: u64,
) {
    metrics::gauge!("falcon_gateway_local_exec_total").set(local_exec as f64);
    metrics::gauge!("falcon_gateway_forward_total").set(forward_to_leader as f64);
    metrics::gauge!("falcon_gateway_reject_overloaded_total").set(reject_overloaded as f64);
    metrics::gauge!("falcon_gateway_reject_no_route_total").set(reject_no_route as f64);
    metrics::gauge!("falcon_gateway_inflight").set(inflight as f64);
    metrics::gauge!("falcon_gateway_forwarded").set(forwarded as f64);
    metrics::gauge!("falcon_gateway_forward_failed_total").set(forward_failed as f64);
    metrics::gauge!("falcon_gateway_client_connects_total").set(client_connects as f64);
    metrics::gauge!("falcon_gateway_client_failovers_total").set(client_failovers as f64);
}

/// Record topology cache metrics.
pub fn record_topology_cache_metrics(
    cache_hits: u64,
    cache_misses: u64,
    epoch_bumps: u64,
    invalidations: u64,
    leader_changes: u64,
    current_epoch: u64,
) {
    metrics::gauge!("falcon_topology_cache_hits").set(cache_hits as f64);
    metrics::gauge!("falcon_topology_cache_misses").set(cache_misses as f64);
    metrics::gauge!("falcon_topology_epoch_bumps").set(epoch_bumps as f64);
    metrics::gauge!("falcon_topology_invalidations").set(invalidations as f64);
    metrics::gauge!("falcon_topology_leader_changes").set(leader_changes as f64);
    metrics::gauge!("falcon_topology_current_epoch").set(current_epoch as f64);
}

/// Record client discovery subscription metrics.
pub fn record_discovery_subscription_metrics(
    active_subscriptions: u64,
    total_subscriptions: u64,
    events_published: u64,
    events_delivered: u64,
    events_dropped: u64,
    evictions: u64,
) {
    metrics::gauge!("falcon_discovery_active_subscriptions").set(active_subscriptions as f64);
    metrics::gauge!("falcon_discovery_total_subscriptions").set(total_subscriptions as f64);
    metrics::gauge!("falcon_discovery_events_published").set(events_published as f64);
    metrics::gauge!("falcon_discovery_events_delivered").set(events_delivered as f64);
    metrics::gauge!("falcon_discovery_events_dropped").set(events_dropped as f64);
    metrics::gauge!("falcon_discovery_evictions").set(evictions as f64);
}

/// Record client connection manager metrics.
pub fn record_client_connection_metrics(
    failovers: u64,
    total_connects: u64,
    total_connect_failures: u64,
    health_checks: u64,
    health_check_failures: u64,
) {
    metrics::gauge!("falcon_client_conn_failovers_total").set(failovers as f64);
    metrics::gauge!("falcon_client_conn_connects_total").set(total_connects as f64);
    metrics::gauge!("falcon_client_conn_failures_total").set(total_connect_failures as f64);
    metrics::gauge!("falcon_client_conn_health_checks_total").set(health_checks as f64);
    metrics::gauge!("falcon_client_conn_health_failures_total").set(health_check_failures as f64);
}

/// Record NOT_LEADER redirector metrics.
pub fn record_redirector_metrics(
    redirect_attempts: u64,
    redirect_successes: u64,
    budget_exhaustions: u64,
    leader_hints_applied: u64,
) {
    metrics::gauge!("falcon_redirector_attempts_total").set(redirect_attempts as f64);
    metrics::gauge!("falcon_redirector_successes_total").set(redirect_successes as f64);
    metrics::gauge!("falcon_redirector_budget_exhausted_total").set(budget_exhaustions as f64);
    metrics::gauge!("falcon_redirector_hints_applied_total").set(leader_hints_applied as f64);
}

/// Record epoch fencing metrics at storage layer.
pub fn record_epoch_fence_metrics(
    current_epoch: u64,
    total_checks: u64,
    total_rejections: u64,
    total_accepted: u64,
) {
    metrics::gauge!("falcon_epoch_fence_current").set(current_epoch as f64);
    metrics::gauge!("falcon_epoch_fence_checks_total").set(total_checks as f64);
    metrics::gauge!("falcon_epoch_fence_rejections_total").set(total_rejections as f64);
    metrics::gauge!("falcon_epoch_fence_accepted_total").set(total_accepted as f64);
}

/// Record shard migration metrics.
pub fn record_shard_migration_metrics(
    active_migrations: u64,
    completed_migrations: u64,
    failed_migrations: u64,
    rolled_back: u64,
    bytes_transferred: u64,
) {
    metrics::gauge!("falcon_shard_migration_active").set(active_migrations as f64);
    metrics::gauge!("falcon_shard_migration_completed_total").set(completed_migrations as f64);
    metrics::gauge!("falcon_shard_migration_failed_total").set(failed_migrations as f64);
    metrics::gauge!("falcon_shard_migration_rolled_back_total").set(rolled_back as f64);
    metrics::gauge!("falcon_shard_migration_bytes_total").set(bytes_transferred as f64);
}

/// Record cluster health summary metrics.
pub fn record_cluster_health_metrics(
    health_status: &str,
    total_nodes: u64,
    alive_nodes: u64,
    total_shards: u64,
    shards_with_leader: u64,
    replication_lag_max_ms: u64,
) {
    let status_num = match health_status {
        "healthy" => 0.0,
        "degraded" => 1.0,
        "critical" => 2.0,
        _ => 3.0,
    };
    metrics::gauge!("falcon_cluster_health_status").set(status_num);
    metrics::gauge!("falcon_cluster_total_nodes").set(total_nodes as f64);
    metrics::gauge!("falcon_cluster_alive_nodes").set(alive_nodes as f64);
    metrics::gauge!("falcon_cluster_total_shards").set(total_shards as f64);
    metrics::gauge!("falcon_cluster_shards_with_leader").set(shards_with_leader as f64);
    metrics::gauge!("falcon_cluster_replication_lag_max_ms").set(replication_lag_max_ms as f64);
}

/// Record segment-level replication streaming metrics.
pub fn record_segment_streaming_metrics(
    handshakes_total: u64,
    segments_streamed_total: u64,
    segment_bytes_total: u64,
    tail_bytes_total: u64,
    checksum_failures: u64,
    error_rollbacks: u64,
    snapshots_created: u64,
) {
    metrics::gauge!("falcon_repl_handshakes_total").set(handshakes_total as f64);
    metrics::gauge!("falcon_repl_segments_streamed_total").set(segments_streamed_total as f64);
    metrics::gauge!("falcon_repl_segment_bytes_total").set(segment_bytes_total as f64);
    metrics::gauge!("falcon_repl_tail_bytes_total").set(tail_bytes_total as f64);
    metrics::gauge!("falcon_repl_checksum_failures_total").set(checksum_failures as f64);
    metrics::gauge!("falcon_repl_error_rollbacks_total").set(error_rollbacks as f64);
    metrics::gauge!("falcon_repl_snapshots_created_total").set(snapshots_created as f64);
}

/// Record automatic shard rebalancer metrics.
pub fn record_rebalancer_metrics(
    runs_completed: u64,
    total_rows_migrated: u64,
    is_running: bool,
    is_paused: bool,
    last_imbalance_ratio: f64,
    last_completed_tasks: u64,
    last_failed_tasks: u64,
    last_duration_ms: u64,
    shard_move_rate: f64,
) {
    metrics::gauge!("falcon_rebalancer_runs_total").set(runs_completed as f64);
    metrics::gauge!("falcon_rebalancer_rows_migrated_total").set(total_rows_migrated as f64);
    metrics::gauge!("falcon_rebalancer_running").set(if is_running { 1.0 } else { 0.0 });
    metrics::gauge!("falcon_rebalancer_paused").set(if is_paused { 1.0 } else { 0.0 });
    metrics::gauge!("falcon_rebalancer_imbalance_ratio").set(last_imbalance_ratio);
    metrics::gauge!("falcon_rebalancer_completed_tasks").set(last_completed_tasks as f64);
    metrics::gauge!("falcon_rebalancer_failed_tasks").set(last_failed_tasks as f64);
    metrics::gauge!("falcon_rebalancer_last_duration_ms").set(last_duration_ms as f64);
    metrics::gauge!("falcon_rebalancer_move_rate_rows_per_sec").set(shard_move_rate);
}

/// Record lock contention event for a named lock/structure.
pub fn record_lock_contention(lock_name: &str, wait_us: u64) {
    metrics::counter!(
        "falcon_lock_contention_total",
        "lock" => lock_name.to_owned()
    )
    .increment(1);
    metrics::histogram!(
        "falcon_lock_wait_us",
        "lock" => lock_name.to_owned()
    )
    .record(wait_us as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── LocalTimer ──────────────────────────────────────────────────────

    #[test]
    fn test_local_timer_format() {
        use tracing_subscriber::fmt::time::FormatTime;
        let timer = LocalTimer;
        let mut buf = String::new();
        let mut writer = tracing_subscriber::fmt::format::Writer::new(&mut buf);
        timer
            .format_time(&mut writer)
            .expect("format_time should not fail");
        // Output should be an ISO-8601-ish timestamp: YYYY-MM-DDTHH:MM:SS.mmm+HH:MM
        assert!(buf.len() >= 23, "timestamp too short: {buf}");
        assert!(buf.contains('T'), "missing 'T' separator in: {buf}");
    }

    // ── Pressure state encoding ─────────────────────────────────────────

    #[test]
    fn test_memory_pressure_encoding() {
        // These should not panic even without a metrics recorder installed.
        // The `metrics` crate uses a no-op recorder by default.
        record_memory_metrics(1000, 500, 200, 1700, 2000, 4000, "normal", 0);
        record_memory_metrics(1000, 500, 200, 1700, 2000, 4000, "pressure", 5);
        record_memory_metrics(1000, 500, 200, 1700, 2000, 4000, "critical", 10);
        record_memory_metrics(1000, 500, 200, 1700, 2000, 4000, "unknown_state", 0);
    }

    // ── Cluster health status encoding ──────────────────────────────────

    #[test]
    fn test_cluster_health_status_encoding() {
        record_cluster_health_metrics("healthy", 3, 3, 16, 16, 0);
        record_cluster_health_metrics("degraded", 3, 2, 16, 14, 500);
        record_cluster_health_metrics("critical", 3, 1, 16, 8, 5000);
        record_cluster_health_metrics("unknown", 3, 0, 16, 0, 99999);
    }

    // ── Query metrics smoke ─────────────────────────────────────────────

    #[test]
    fn test_record_query_metrics_does_not_panic() {
        record_query_metrics(150, "select", true);
        record_query_metrics(0, "insert", false);
        record_query_metrics(u64::MAX, "update", true);
    }

    #[test]
    fn test_record_txn_metrics_does_not_panic() {
        record_txn_metrics("begin");
        record_txn_metrics("commit");
        record_txn_metrics("abort");
    }

    #[test]
    fn test_record_active_connections_does_not_panic() {
        record_active_connections(0);
        record_active_connections(1000);
    }

    #[test]
    fn test_record_storage_metrics_does_not_panic() {
        record_storage_metrics(10, 500_000);
        record_storage_metrics(0, 0);
    }

    // ── Prepared statement metrics smoke ─────────────────────────────────

    #[test]
    fn test_prepared_stmt_metrics_do_not_panic() {
        record_prepared_stmt_op("parse", "plan");
        record_prepared_stmt_op("bind", "legacy");
        record_prepared_stmt_op("execute", "plan");
        record_prepared_stmt_sql_cmd("prepare");
        record_prepared_stmt_sql_cmd("execute");
        record_prepared_stmt_sql_cmd("deallocate");
        record_prepared_stmt_parse_duration_us(100, true);
        record_prepared_stmt_parse_duration_us(5000, false);
        record_prepared_stmt_bind_duration_us(50);
        record_prepared_stmt_execute_duration_us(200, true);
        record_prepared_stmt_param_count(0);
        record_prepared_stmt_param_count(10);
        record_prepared_stmt_active(5);
        record_prepared_stmt_portals_active(3);
    }

    // ── Enterprise / tenant metrics smoke ────────────────────────────────

    #[test]
    fn test_tenant_metrics_do_not_panic() {
        record_tenant_metrics(1, "acme", 5, 1024 * 1024, 150.0, 1000, 10, 0);
        record_tenant_metrics(0, "", 0, 0, 0.0, 0, 0, 0);
    }

    #[test]
    fn test_wal_stability_metrics_do_not_panic() {
        record_wal_stability_metrics(10000, 500, 100, 3.5, 8192);
    }

    #[test]
    fn test_replication_metrics_do_not_panic() {
        record_replication_metrics(2, 500, 1000, 1);
    }

    #[test]
    fn test_backup_metrics_do_not_panic() {
        record_backup_metrics(10, 1, 1024 * 1024 * 100);
    }

    #[test]
    fn test_audit_metrics_do_not_panic() {
        record_audit_metrics(5000, 50);
    }

    // ── Security metrics smoke ──────────────────────────────────────────

    #[test]
    fn test_security_metrics_do_not_panic() {
        record_security_metrics(true, true, true, 5, 1000);
        record_security_metrics(false, false, false, 0, 0);
    }

    #[test]
    fn test_security_hardening_metrics_do_not_panic() {
        record_security_hardening_metrics(100, 5, 20, 2, 50, 3, 200, 10, 1, 0, 0);
    }

    // ── Health / SLA metrics smoke ──────────────────────────────────────

    #[test]
    fn test_health_metrics_do_not_panic() {
        record_health_metrics(95, 10, 2);
        record_health_metrics(0, 0, 0);
    }

    #[test]
    fn test_sla_metrics_do_not_panic() {
        record_sla_metrics(500, 1000, 5000, 3, 1, 2);
    }

    #[test]
    fn test_sla_admission_metrics_do_not_panic() {
        record_sla_admission_metrics(500, 1000, 10.0, 50.0, 5, 2, 0, 100, 3);
        record_sla_rejection("latency", "fast");
    }

    // ── 2PC / distributed metrics smoke ─────────────────────────────────

    #[test]
    fn test_decision_log_metrics_do_not_panic() {
        record_decision_log_metrics(100, 99, 10, 1);
    }

    #[test]
    fn test_layered_timeout_metrics_do_not_panic() {
        record_layered_timeout_metrics(5, 2, 1);
    }

    #[test]
    fn test_slow_shard_metrics_do_not_panic() {
        record_slow_shard_metrics(3, 1, 2, 1, 0);
    }

    // ── Chaos / fault injection metrics smoke ───────────────────────────

    #[test]
    fn test_fault_injection_metrics_do_not_panic() {
        record_fault_injection_metrics(10, true, 3, 2, 5, false, 0);
        record_fault_injection_metrics(0, false, 0, 0, 0, true, 100);
    }

    // ── Scheduler / token bucket metrics smoke ──────────────────────────

    #[test]
    fn test_priority_scheduler_metrics_do_not_panic() {
        record_priority_scheduler_metrics(5, 100, 2, 10, 20, 500, 10, 150, 3, 5, 200, 50, 300);
    }

    #[test]
    fn test_token_bucket_metrics_do_not_panic() {
        record_token_bucket_metrics("write_limiter", 1000, 100, 50, false, 500, 600, 10, 200);
        record_token_bucket_metrics("read_limiter", 5000, 500, 0, true, 0, 0, 0, 0);
    }

    // ── Crash domain metrics smoke ──────────────────────────────────────

    #[test]
    fn test_crash_domain_metrics_do_not_panic() {
        record_crash_domain_metrics(0, 0, 0, 0);
        record_crash_domain_metrics(5, 3, 1, 2);
    }

    // ── Background task metrics smoke ───────────────────────────────────

    #[test]
    fn test_bg_task_metrics_do_not_panic() {
        record_bg_task_failed("gc_runner");
        record_bg_supervisor_metrics(5, 4, 1, true);
        record_bg_supervisor_metrics(3, 3, 0, false);
    }

    // ── Transaction stats metrics smoke ─────────────────────────────────

    #[test]
    fn test_txn_stats_do_not_panic() {
        record_txn_stats(1000, 900, 100, 50, 10, 5, 3);
    }

    #[test]
    fn test_txn_terminal_state_do_not_panic() {
        record_txn_terminal_state("committed", "", "00000");
        record_txn_terminal_state("aborted_retryable", "write_conflict", "40001");
        record_txn_terminal_state("rejected", "admission_control", "53300");
    }

    // ── GC budget metrics smoke ─────────────────────────────────────────

    #[test]
    fn test_gc_budget_metrics_do_not_panic() {
        record_gc_budget_metrics(500, false, false, false, 100, 8192, 5, 1.5);
        record_gc_budget_metrics(2000, true, true, true, 500, 65536, 20, 3.7);
    }

    // ── Network / distributed query metrics smoke ───────────────────────

    #[test]
    fn test_dist_query_network_metrics_do_not_panic() {
        record_dist_query_network_metrics(10000, 5000, 100, 50, true, true, true);
        record_dist_query_network_metrics(0, 0, 0, 0, false, false, false);
        // bytes_in <= bytes_out (no savings path)
        record_dist_query_network_metrics(100, 200, 10, 20, false, false, false);
    }

    // ── Cluster observability metrics smoke ──────────────────────────────

    #[test]
    fn test_gateway_routing_metrics_do_not_panic() {
        record_gateway_routing_metrics(100, 50, 5, 2, 10, 60, 3, 200, 5);
    }

    #[test]
    fn test_topology_cache_metrics_do_not_panic() {
        record_topology_cache_metrics(500, 50, 10, 5, 3, 42);
    }

    #[test]
    fn test_discovery_subscription_metrics_do_not_panic() {
        record_discovery_subscription_metrics(10, 100, 500, 490, 10, 2);
    }

    #[test]
    fn test_client_connection_metrics_do_not_panic() {
        record_client_connection_metrics(3, 100, 5, 50, 2);
    }

    #[test]
    fn test_redirector_metrics_do_not_panic() {
        record_redirector_metrics(20, 18, 2, 15);
    }

    #[test]
    fn test_epoch_fence_metrics_do_not_panic() {
        record_epoch_fence_metrics(42, 1000, 5, 995);
    }

    #[test]
    fn test_shard_migration_metrics_do_not_panic() {
        record_shard_migration_metrics(1, 10, 2, 1, 1024 * 1024);
    }

    #[test]
    fn test_segment_streaming_metrics_do_not_panic() {
        record_segment_streaming_metrics(5, 100, 1024 * 1024, 512, 0, 1, 3);
    }

    #[test]
    fn test_rebalancer_metrics_do_not_panic() {
        record_rebalancer_metrics(5, 10000, true, false, 0.15, 3, 0, 5000, 200.0);
        record_rebalancer_metrics(0, 0, false, true, 0.0, 0, 0, 0, 0.0);
    }

    #[test]
    fn test_lock_contention_does_not_panic() {
        record_lock_contention("shard_map_rwlock", 150);
        record_lock_contention("wal_buffer", 0);
    }

    // ── Compat metrics smoke ────────────────────────────────────────────

    #[test]
    fn test_compat_metrics_do_not_panic() {
        record_compat_metrics(1, 1);
        record_compat_metrics(u32::MAX, u32::MAX);
    }

    // ── Metering metrics smoke ──────────────────────────────────────────

    #[test]
    fn test_metering_metrics_do_not_panic() {
        record_metering_metrics(1, 5000, 1024 * 1024, 1024 * 1024 * 10, 500, 100.0, false);
        record_metering_metrics(0, 0, 0, 0, 0, 0.0, true);
    }

    // ── Queue depth metrics smoke ───────────────────────────────────────

    #[test]
    fn test_queue_depth_metrics_do_not_panic() {
        record_queue_depth_metrics("wal_writer", 10, 100, 50, 1000, 5);
    }

    // ── Replay metrics smoke ────────────────────────────────────────────

    #[test]
    fn test_replay_metrics_do_not_panic() {
        record_replay_metrics(100, 5, 0);
    }

    // ── Admission rejection smoke ───────────────────────────────────────

    #[test]
    fn test_admission_rejection_do_not_panic() {
        record_admission_rejection("connections", "53300");
        record_admission_rejection("memory", "53200");
    }

    // ── Failover recovery smoke ─────────────────────────────────────────

    #[test]
    fn test_failover_recovery_do_not_panic() {
        record_failover_recovery_duration_ms(1500, "success");
        record_failover_recovery_duration_ms(30000, "timeout");
    }
}
