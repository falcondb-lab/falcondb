//! Observability setup: structured logging, metrics (Prometheus), tracing.

use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

/// Initialize the global tracing subscriber with structured logging.
pub fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,falcon=debug"));

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
    metrics::histogram!("falcon_query_duration_us", "type" => query_type.to_string()).record(duration_us as f64);
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
pub fn record_backup_metrics(
    total_completed: u64,
    total_failed: u64,
    total_bytes: u64,
) {
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
    metrics::gauge!("falcon_metering_memory_bytes", "tenant_id" => tid.clone()).set(memory_bytes as f64);
    metrics::gauge!("falcon_metering_storage_bytes", "tenant_id" => tid.clone()).set(storage_bytes as f64);
    metrics::gauge!("falcon_metering_query_count", "tenant_id" => tid.clone()).set(query_count as f64);
    metrics::gauge!("falcon_metering_qps", "tenant_id" => tid.clone()).set(qps);
    metrics::gauge!("falcon_metering_throttled", "tenant_id" => tid).set(if throttled { 1.0 } else { 0.0 });
}

/// Record security posture metrics (P3-6).
pub fn record_security_metrics(
    encryption_enabled: bool,
    tls_enabled: bool,
    ip_allowlist_enabled: bool,
    blocked_by_ip: u64,
    total_connection_attempts: u64,
) {
    metrics::gauge!("falcon_security_encryption_enabled").set(if encryption_enabled { 1.0 } else { 0.0 });
    metrics::gauge!("falcon_security_tls_enabled").set(if tls_enabled { 1.0 } else { 0.0 });
    metrics::gauge!("falcon_security_ip_allowlist_enabled").set(if ip_allowlist_enabled { 1.0 } else { 0.0 });
    metrics::gauge!("falcon_security_blocked_by_ip").set(blocked_by_ip as f64);
    metrics::gauge!("falcon_security_connection_attempts").set(total_connection_attempts as f64);
}

/// Record health score metrics (P3-7).
pub fn record_health_metrics(overall_score: u32, component_count: usize, recommendation_count: usize) {
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
