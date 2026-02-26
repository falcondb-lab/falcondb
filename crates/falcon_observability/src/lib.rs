//! Observability setup: structured logging, metrics (Prometheus), tracing.
#![allow(clippy::too_many_arguments)]

use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

/// Custom timer that formats timestamps in local time using chrono.
struct LocalTimer;

impl tracing_subscriber::fmt::time::FormatTime for LocalTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = chrono::Local::now();
        write!(w, "{}", now.format("%Y-%m-%dT%H:%M:%S%.3f%:z"))
    }
}

/// Initialize the global tracing subscriber with structured logging.
pub fn init_tracing() {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

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
    metrics::counter!("falcon_queries_total", "type" => query_type.to_owned(), "success" => success.to_string()).increment(1);
    metrics::histogram!("falcon_query_duration_us", "type" => query_type.to_owned())
        .record(duration_us as f64);
}

pub fn record_txn_metrics(action: &str) {
    metrics::counter!("falcon_txn_total", "action" => action.to_owned()).increment(1);
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
    metrics::gauge!("falcon_compat_snapshot_format_version").set(f64::from(snapshot_format_version));
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
    metrics::gauge!("falcon_queue_enqueued_total", "queue" => n.clone())
        .set(total_enqueued as f64);
    metrics::gauge!("falcon_queue_rejected_total", "queue" => n).set(total_rejected as f64);
}

/// Record idempotent replay metrics (v1.0.4 §6).
pub fn record_replay_metrics(
    replayed_count: u64,
    duplicate_count: u64,
    violation_count: u64,
) {
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
