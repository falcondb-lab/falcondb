//! Observability setup: structured logging, metrics (Prometheus), tracing.
#![allow(clippy::too_many_arguments)]

use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

/// Initialize the global tracing subscriber with structured logging.
pub fn init_tracing() {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,falcon=debug"));

    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true);

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
    metrics::counter!("falcon_queries_total", "type" => query_type.to_string(), "success" => success.to_string()).increment(1);
    metrics::histogram!("falcon_query_duration_us", "type" => query_type.to_string())
        .record(duration_us as f64);
}

pub fn record_txn_metrics(action: &str) {
    metrics::counter!("falcon_txn_total", "action" => action.to_string()).increment(1);
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

/// Record an extended-query-protocol operation (Parse, Bind, Execute, Describe, Close).
/// `op` should be one of: "parse", "bind", "execute", "describe", "close".
/// `path` indicates execution path: "plan" (plan-based) or "legacy" (text-substitution).
pub fn record_prepared_stmt_op(op: &str, path: &str) {
    metrics::counter!(
        "falcon_prepared_stmt_ops_total",
        "op" => op.to_string(),
        "path" => path.to_string()
    )
    .increment(1);
}

/// Record a SQL-level prepared statement command (PREPARE, EXECUTE, DEALLOCATE).
/// `cmd` should be one of: "prepare", "execute", "deallocate".
pub fn record_prepared_stmt_sql_cmd(cmd: &str) {
    metrics::counter!(
        "falcon_prepared_stmt_sql_cmds_total",
        "cmd" => cmd.to_string()
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
    metrics::gauge!("falcon_tenant_active_txns", "tenant_id" => tid.clone(), "tenant_name" => tenant_name.to_string()).set(active_txns as f64);
    metrics::gauge!("falcon_tenant_memory_bytes", "tenant_id" => tid.clone(), "tenant_name" => tenant_name.to_string()).set(memory_bytes as f64);
    metrics::gauge!("falcon_tenant_qps", "tenant_id" => tid.clone(), "tenant_name" => tenant_name.to_string()).set(current_qps);
    metrics::gauge!("falcon_tenant_txns_committed", "tenant_id" => tid.clone(), "tenant_name" => tenant_name.to_string()).set(txns_committed as f64);
    metrics::gauge!("falcon_tenant_txns_aborted", "tenant_id" => tid.clone(), "tenant_name" => tenant_name.to_string()).set(txns_aborted as f64);
    metrics::gauge!("falcon_tenant_quota_exceeded", "tenant_id" => tid.clone(), "tenant_name" => tenant_name.to_string()).set(quota_exceeded_count as f64);
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
    metrics::gauge!("falcon_health_overall_score").set(overall_score as f64);
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
    let n = name.to_string();
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
    metrics::gauge!("falcon_compat_wal_format_version").set(wal_format_version as f64);
    metrics::gauge!("falcon_compat_snapshot_format_version").set(snapshot_format_version as f64);
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
        "task" => task_name.to_string()
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

/// Record lock contention event for a named lock/structure.
pub fn record_lock_contention(lock_name: &str, wait_us: u64) {
    metrics::counter!(
        "falcon_lock_contention_total",
        "lock" => lock_name.to_string()
    )
    .increment(1);
    metrics::histogram!(
        "falcon_lock_wait_us",
        "lock" => lock_name.to_string()
    )
    .record(wait_us as f64);
}
