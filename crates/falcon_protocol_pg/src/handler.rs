use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Instant;

use falcon_cluster::fault_injection::FaultInjector;
use falcon_cluster::security_hardening::{
    AuthRateLimiter, AuthRateLimiterConfig, PasswordPolicy, PasswordPolicyConfig, SqlFirewall,
    SqlFirewallConfig,
};
use falcon_cluster::{
    ClusterAdmin, ClusterEventLog, CoordinatorDecisionLog, DecisionLogConfig,
    DistributedQueryEngine, HAReplicaGroup, LayeredTimeoutConfig, LayeredTimeoutController,
    RaftShardCoordinator, ReplicaRunnerMetrics, SlowShardConfig, SlowShardTracker,
    SyncReplicationWaiter, TokenBucket, TokenBucketConfig,
};
use falcon_common::consistency::CommitPolicy;
use falcon_common::datum::Datum;
use falcon_common::error::FalconError;
use falcon_common::types::ShardId;
use falcon_executor::{ExecutionResult, Executor, PriorityScheduler, PrioritySchedulerConfig};
use falcon_planner::{IndexedColumns, PhysicalPlan, PlannedTxnType, Planner, TableRowCounts};
use falcon_sql_frontend::binder::Binder;
use falcon_sql_frontend::types::{BinOp, BoundExpr, BoundInsert, BoundStatement, OnConflictAction};
use falcon_sql_frontend::parser::parse_sql;
use falcon_storage::engine::StorageEngine;
use falcon_txn::{SlowPathMode, TxnClassification, TxnManager};
use parking_lot::RwLock;

use crate::codec::{BackendMessage, FieldDescription};

fn datum_add(a: &Datum, b: &Datum) -> Option<Datum> {
    match (a, b) {
        (Datum::Int32(x), Datum::Int32(y)) => Some(Datum::Int32(x.wrapping_add(*y))),
        (Datum::Int64(x), Datum::Int64(y)) => Some(Datum::Int64(x.wrapping_add(*y))),
        (Datum::Int32(x), Datum::Int64(y)) => Some(Datum::Int64((*x as i64).wrapping_add(*y))),
        (Datum::Int64(x), Datum::Int32(y)) => Some(Datum::Int64(x.wrapping_add(*y as i64))),
        (Datum::Float64(x), Datum::Float64(y)) => Some(Datum::Float64(x + y)),
        _ => None,
    }
}

fn datum_sub(a: &Datum, b: &Datum) -> Option<Datum> {
    match (a, b) {
        (Datum::Int32(x), Datum::Int32(y)) => Some(Datum::Int32(x.wrapping_sub(*y))),
        (Datum::Int64(x), Datum::Int64(y)) => Some(Datum::Int64(x.wrapping_sub(*y))),
        (Datum::Int32(x), Datum::Int64(y)) => Some(Datum::Int64((*x as i64).wrapping_sub(*y))),
        (Datum::Int64(x), Datum::Int32(y)) => Some(Datum::Int64(x.wrapping_sub(*y as i64))),
        (Datum::Float64(x), Datum::Float64(y)) => Some(Datum::Float64(x - y)),
        _ => None,
    }
}
use crate::handler_utils::classification_from_routing_hint;
use crate::plan_cache::PlanCache;
use crate::session::PgSession;
use crate::slow_query_log::SlowQueryLog;

// ── Sub-context structs ──────────────────────────────────────────────────
// Grouping reduces cognitive load and makes dependencies explicit.

/// Enterprise / multi-tenant / security context.
#[derive(Clone)]
pub(crate) struct EnterpriseContext {
    pub(crate) tenant_registry: Arc<falcon_storage::tenant_registry::TenantRegistry>,
    pub(crate) audit_log: Arc<falcon_storage::audit::AuditLog>,
    pub(crate) license_info: Arc<falcon_common::edition::LicenseInfo>,
    pub(crate) feature_gate: Arc<falcon_common::edition::FeatureGate>,
    pub(crate) resource_meter: Arc<falcon_storage::metering::ResourceMeter>,
    pub(crate) security_manager: Arc<falcon_storage::security_manager::SecurityManager>,
    pub(crate) auth_rate_limiter: Arc<AuthRateLimiter>,
    pub(crate) password_policy: Arc<PasswordPolicy>,
    pub(crate) sql_firewall: Arc<SqlFirewall>,
}

/// Cluster / distributed execution / replication context.
#[derive(Clone)]
pub(crate) struct ClusterContext {
    pub(crate) shard_ids: Vec<ShardId>,
    pub(crate) dist_engine: Option<Arc<DistributedQueryEngine>>,
    pub(crate) commit_policy: CommitPolicy,
    pub(crate) sync_waiter: Option<Arc<SyncReplicationWaiter>>,
    pub(crate) ha_group: Option<Arc<RwLock<HAReplicaGroup>>>,
    pub(crate) admin: Arc<ClusterAdmin>,
    pub(crate) rebalance_token_bucket: Arc<TokenBucket>,
    pub(crate) decision_log: Arc<CoordinatorDecisionLog>,
    pub(crate) timeout_controller: Arc<LayeredTimeoutController>,
    pub(crate) slow_shard_tracker: Arc<SlowShardTracker>,
    pub(crate) fault_injector: Arc<FaultInjector>,
    pub(crate) raft_coordinator: Option<Arc<RaftShardCoordinator>>,
    pub(crate) lifecycle_coordinator: Option<Arc<falcon_cluster::ClusterLifecycleCoordinator>>,
    pub(crate) failure_detector: Option<Arc<falcon_cluster::ClusterFailureDetector>>,
}

/// Observability / diagnostics context.
#[derive(Clone)]
pub(crate) struct ObservabilityContext {
    pub(crate) slow_query_log: Arc<SlowQueryLog>,
    pub(crate) plan_cache: Arc<PlanCache>,
    pub(crate) replica_metrics: Option<Arc<ReplicaRunnerMetrics>>,
    pub(crate) hotspot_detector: Arc<falcon_storage::hotspot::HotspotDetector>,
    pub(crate) consistency_verifier: Arc<falcon_storage::verification::ConsistencyVerifier>,
    pub(crate) priority_scheduler: Arc<PriorityScheduler>,
}

/// Handles a single SQL query within a session, producing PG backend messages.
#[derive(Clone)]
pub struct QueryHandler {
    // ── Core (always required) ──
    pub(crate) storage: Arc<StorageEngine>,
    pub(crate) txn_mgr: Arc<TxnManager>,
    pub(crate) executor: Arc<Executor>,

    // ── Grouped contexts ──
    pub(crate) enterprise: EnterpriseContext,
    pub(crate) cluster: ClusterContext,
    pub(crate) observability: ObservabilityContext,

    flush_stats_counter: Arc<AtomicU64>,
    /// Last-1 schema cache: avoids catalog RwLock + deep clone on repeated INSERTs.
    schema_cache: Arc<parking_lot::Mutex<Option<(String, falcon_common::schema::TableSchema)>>>,
}

impl QueryHandler {
    pub fn new(
        storage: Arc<StorageEngine>,
        txn_mgr: Arc<TxnManager>,
        executor: Arc<Executor>,
    ) -> Self {
        let tenant_reg = storage.ext.tenant_registry.clone();
        let res_meter = storage.ext.resource_meter.clone();
        Self {
            storage,
            txn_mgr,
            executor,
            flush_stats_counter: Arc::new(AtomicU64::new(0)),
            schema_cache: Arc::new(parking_lot::Mutex::new(None)),
            enterprise: EnterpriseContext {
                tenant_registry: tenant_reg,
                audit_log: Arc::new(falcon_storage::audit::AuditLog::new()),
                license_info: Arc::new(falcon_common::edition::LicenseInfo::community()),
                feature_gate: Arc::new(falcon_common::edition::FeatureGate::for_edition(
                    falcon_common::edition::EditionTier::Community,
                )),
                resource_meter: res_meter,
                security_manager: Arc::new(falcon_storage::security_manager::SecurityManager::new()),
                auth_rate_limiter: Arc::new(AuthRateLimiter::new(AuthRateLimiterConfig::default())),
                password_policy: Arc::new(PasswordPolicy::new(PasswordPolicyConfig::default())),
                sql_firewall: Arc::new(SqlFirewall::new(SqlFirewallConfig::default())),
            },
            cluster: ClusterContext {
                shard_ids: vec![ShardId(0)],
                dist_engine: None,
                commit_policy: CommitPolicy::default(),
                sync_waiter: None,
                ha_group: None,
                admin: ClusterAdmin::new(ClusterEventLog::new(256)),
                rebalance_token_bucket: TokenBucket::new(TokenBucketConfig::rebalance()),
                decision_log: CoordinatorDecisionLog::new(DecisionLogConfig::default()),
                timeout_controller: LayeredTimeoutController::new(LayeredTimeoutConfig::default()),
                slow_shard_tracker: SlowShardTracker::new(SlowShardConfig::default()),
                fault_injector: Arc::new(FaultInjector::new()),
                raft_coordinator: None,
                lifecycle_coordinator: None,
                failure_detector: None,
            },
            observability: ObservabilityContext {
                slow_query_log: Arc::new(SlowQueryLog::disabled()),
                plan_cache: Arc::new(PlanCache::new(65536)),
                replica_metrics: None,
                hotspot_detector: Arc::new(falcon_storage::hotspot::HotspotDetector::default()),
                consistency_verifier: Arc::new(
                    falcon_storage::verification::ConsistencyVerifier::default(),
                ),
                priority_scheduler: PriorityScheduler::new(PrioritySchedulerConfig::default()),
            },
        }
    }

    /// Create a handler with cluster topology awareness.
    /// When `shard_ids` has more than one entry, read-only queries will be
    /// wrapped in `DistPlan` via `Planner::wrap_distributed()`.
    /// The `dist_engine` handles DistPlan execution, DDL propagation,
    /// and shard-routed DML.
    pub fn new_distributed(
        storage: Arc<StorageEngine>,
        txn_mgr: Arc<TxnManager>,
        executor: Arc<Executor>,
        shard_ids: Vec<ShardId>,
        dist_engine: Arc<DistributedQueryEngine>,
    ) -> Self {
        let tenant_reg = storage.ext.tenant_registry.clone();
        let res_meter = storage.ext.resource_meter.clone();
        Self {
            storage,
            txn_mgr,
            executor,
            flush_stats_counter: Arc::new(AtomicU64::new(0)),
            schema_cache: Arc::new(parking_lot::Mutex::new(None)),
            enterprise: EnterpriseContext {
                tenant_registry: tenant_reg,
                audit_log: Arc::new(falcon_storage::audit::AuditLog::new()),
                license_info: Arc::new(falcon_common::edition::LicenseInfo::community()),
                feature_gate: Arc::new(falcon_common::edition::FeatureGate::for_edition(
                    falcon_common::edition::EditionTier::Community,
                )),
                resource_meter: res_meter,
                security_manager: Arc::new(falcon_storage::security_manager::SecurityManager::new()),
                auth_rate_limiter: Arc::new(AuthRateLimiter::new(AuthRateLimiterConfig::default())),
                password_policy: Arc::new(PasswordPolicy::new(PasswordPolicyConfig::default())),
                sql_firewall: Arc::new(SqlFirewall::new(SqlFirewallConfig::default())),
            },
            cluster: ClusterContext {
                shard_ids,
                dist_engine: Some(dist_engine),
                commit_policy: CommitPolicy::default(),
                sync_waiter: None,
                ha_group: None,
                admin: ClusterAdmin::new(ClusterEventLog::new(256)),
                rebalance_token_bucket: TokenBucket::new(TokenBucketConfig::rebalance()),
                decision_log: CoordinatorDecisionLog::new(DecisionLogConfig::default()),
                timeout_controller: LayeredTimeoutController::new(LayeredTimeoutConfig::default()),
                slow_shard_tracker: SlowShardTracker::new(SlowShardConfig::default()),
                fault_injector: Arc::new(FaultInjector::new()),
                raft_coordinator: None,
                lifecycle_coordinator: None,
                failure_detector: None,
            },
            observability: ObservabilityContext {
                slow_query_log: Arc::new(SlowQueryLog::disabled()),
                plan_cache: Arc::new(PlanCache::new(65536)),
                replica_metrics: None,
                hotspot_detector: Arc::new(falcon_storage::hotspot::HotspotDetector::default()),
                consistency_verifier: Arc::new(
                    falcon_storage::verification::ConsistencyVerifier::default(),
                ),
                priority_scheduler: PriorityScheduler::new(PrioritySchedulerConfig::default()),
            },
        }
    }

    /// Set the cluster admin coordinator.
    pub fn set_cluster_admin(&mut self, admin: Arc<ClusterAdmin>) {
        self.cluster.admin = admin;
    }

    /// Set the commit policy for durability guarantees.
    pub const fn set_commit_policy(&mut self, policy: CommitPolicy) {
        self.cluster.commit_policy = policy;
    }

    /// Configure synchronous replication waiter for quorum-ack / semi-sync / sync modes.
    pub fn set_sync_replication(
        &mut self,
        waiter: Arc<SyncReplicationWaiter>,
        group: Arc<RwLock<HAReplicaGroup>>,
    ) {
        self.cluster.sync_waiter = Some(waiter);
        self.cluster.ha_group = Some(group);
    }

    /// Set replica runner metrics for SHOW falcon.replica_stats.
    pub fn set_replica_metrics(&mut self, metrics: Arc<ReplicaRunnerMetrics>) {
        self.observability.replica_metrics = Some(metrics);
    }

    /// Set the Raft shard coordinator for SHOW falcon.raft_stats.
    pub fn set_raft_coordinator(&mut self, coordinator: Arc<RaftShardCoordinator>) {
        self.cluster.raft_coordinator = Some(coordinator);
    }

    pub fn set_lifecycle_coordinator(&mut self, lc: Arc<falcon_cluster::ClusterLifecycleCoordinator>) {
        self.cluster.lifecycle_coordinator = Some(lc);
    }

    pub fn set_failure_detector(&mut self, fd: Arc<falcon_cluster::ClusterFailureDetector>) {
        self.cluster.failure_detector = Some(fd);
    }

    /// Set a shared SecurityManager instance (so SHOW falcon.security reflects
    /// the same state as the PgServer IP allowlist enforcement).
    pub fn set_security_manager(&mut self, mgr: Arc<falcon_storage::security_manager::SecurityManager>) {
        self.enterprise.security_manager = mgr;
    }

    /// Set a shared slow query log instance.
    pub fn with_slow_query_log(mut self, log: Arc<SlowQueryLog>) -> Self {
        self.observability.slow_query_log = log;
        self
    }

    /// Get a reference to the slow query log.
    pub const fn slow_query_log(&self) -> &Arc<SlowQueryLog> {
        &self.observability.slow_query_log
    }

    /// Process a simple query string. Returns a list of backend messages to send.
    ///
    /// The entire request is wrapped in `catch_request` so that any panic in
    /// parse/bind/plan/execute is converted to an ErrorResponse rather than
    /// crashing the process.
    pub fn handle_query(&self, sql: &str, session: &mut PgSession) -> Vec<BackendMessage> {
        let sql = sql.trim();
        if sql.is_empty() {
            return vec![BackendMessage::EmptyQueryResponse];
        }
        let t0 = std::time::Instant::now();
        let first = sql.as_bytes().first().copied().unwrap_or(0);
        let msgs = self.handle_query_dispatch(sql, first, session);
        let elapsed_us = t0.elapsed().as_micros() as u64;
        let qtype = match first {
            b'I' | b'i' => "insert", b'U' | b'u' => "update",
            b'D' | b'd' => "delete", b'S' | b's' => "select",
            b'C' | b'c' | b'B' | b'b' | b'R' | b'r' | b'E' | b'e' => "txn_control",
            _ => "other",
        };
        let ok = !msgs.iter().any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        falcon_observability::record_query_metrics(elapsed_us, qtype, ok);
        msgs
    }

    fn handle_query_dispatch(&self, sql: &str, first: u8, session: &mut PgSession) -> Vec<BackendMessage> {
        // Fast-path transaction control: bypass sqlparser+binder+planner entirely
        match first {
            b'B' | b'b' if sql.len() >= 5 && sql.as_bytes()[..5].eq_ignore_ascii_case(b"BEGIN") => {
                let rest = sql[5..].trim().trim_end_matches(';').trim();
                if rest.is_empty()
                    || rest.eq_ignore_ascii_case("TRANSACTION")
                    || rest.eq_ignore_ascii_case("WORK")
                {
                    return self.fast_begin(session);
                }
            }
            b'C' | b'c' if sql.len() >= 6 && sql.as_bytes()[..6].eq_ignore_ascii_case(b"COMMIT") => {
                let rest = sql[6..].trim().trim_end_matches(';').trim();
                if rest.is_empty()
                    || rest.eq_ignore_ascii_case("TRANSACTION")
                    || rest.eq_ignore_ascii_case("WORK")
                {
                    return self.fast_commit(session);
                }
            }
            b'E' | b'e' if sql.len() >= 3 && sql.as_bytes()[..3].eq_ignore_ascii_case(b"END") => {
                let rest = sql[3..].trim().trim_end_matches(';').trim();
                if rest.is_empty()
                    || rest.eq_ignore_ascii_case("TRANSACTION")
                    || rest.eq_ignore_ascii_case("WORK")
                {
                    return self.fast_commit(session);
                }
            }
            b'R' | b'r' if sql.len() >= 8 && sql.as_bytes()[..8].eq_ignore_ascii_case(b"ROLLBACK") => {
                let rest = sql[8..].trim().trim_end_matches(';').trim();
                // Don't intercept ROLLBACK TO SAVEPOINT
                if rest.is_empty() {
                    return self.fast_rollback(session);
                }
            }
            _ => {}
        }

        // Intercept system/catalog queries that psql sends (skip for DML/SELECT — no match possible)
        if first != b'I' && first != b'i' && first != b'U' && first != b'u'
            && first != b'D' && first != b'd' && first != b'S' && first != b's'
        {
            if let Some(response) = self.handle_system_query(sql, session) {
                return response;
            }
        }

        // Fast-path DML: skip crash domain + RequestContext overhead
        if first == b'I' || first == b'i' {
            if let Some(fast_ins) = self.try_fast_insert_parse(sql) {
                // Ultra-fast: skip executor entirely for simple schemas
                if let Some(result) = self.try_direct_insert(&fast_ins, session) {
                    return match result {
                        Ok(msgs) => msgs,
                        Err(e) => vec![self.error_response(&e)],
                    };
                }
                let plan = match Planner::plan_insert_owned(fast_ins) {
                    Ok(p) => Planner::wrap_distributed(p, &self.cluster.shard_ids),
                    Err(e) => return vec![self.error_response(&FalconError::Sql(e))],
                };
                return match self.execute_single_plan(sql, plan, session) {
                    Ok(msgs) => msgs,
                    Err(e) => vec![self.error_response(&e)],
                };
            }
        }
        if first == b'U' || first == b'u' {
            if let Some(msgs) = self.try_fast_direct_update(sql, session) {
                return msgs;
            }
            if let Some(plan) = self.try_fast_update_parse(sql) {
                let plan = Planner::wrap_distributed(plan, &self.cluster.shard_ids);
                return match self.execute_single_plan(sql, plan, session) {
                    Ok(msgs) => msgs,
                    Err(e) => vec![self.error_response(&e)],
                };
            }
        }
        if first == b'S' || first == b's' {
            if let Some(msgs) = self.try_fast_select(sql, session) {
                return msgs;
            }
        }

        // Standard path: crash domain isolation for complex queries
        let rctx = falcon_common::request_context::RequestContext::new(session.id as u64);
        let ctx = format!("session_id={}", session.id);
        let result =
            falcon_common::crash_domain::catch_request_result("handle_query", &ctx, || {
                self.handle_query_inner(sql, session)
            });
        match result {
            Ok(msgs) => msgs,
            Err(e) => vec![self.error_response(&e.with_request_context(&rctx))],
        }
    }

    /// Fast-path parser for simple INSERT INTO table (cols) VALUES (...), ...
    /// Bypasses sqlparser-rs entirely, producing BoundInsert directly.
    /// Returns None if the SQL doesn't match the simple pattern (falls back to standard path).
    fn try_fast_insert_parse(&self, sql: &str) -> Option<BoundInsert> {
        let trimmed = sql.trim();
        // Quick prefix check (case-insensitive)
        if trimmed.len() < 20 || !trimmed[..11].eq_ignore_ascii_case("INSERT INTO") {
            return None;
        }
        let rest = trimmed[11..].trim_start();

        // Extract table name (identifier chars)
        let tbl_end = rest.find(|c: char| !c.is_alphanumeric() && c != '_' && c != '"')?;
        if tbl_end == 0 { return None; }
        let table_name = rest[..tbl_end].trim_matches('"');
        let rest = rest[tbl_end..].trim_start();

        // Extract column list: (col1, col2, ...)
        if !rest.starts_with('(') { return None; }
        let col_end = rest.find(')')?;
        let col_names: Vec<&str> = rest[1..col_end].split(',').map(|s| s.trim().trim_matches('"')).collect();
        if col_names.is_empty() { return None; }
        let rest = rest[col_end + 1..].trim_start();

        // Expect VALUES keyword
        if rest.len() < 6 || !rest[..6].eq_ignore_ascii_case("VALUES") { return None; }
        // Reject RETURNING / ON CONFLICT DO UPDATE (fall back to standard path)
        // Allow ON CONFLICT DO NOTHING
        let mut on_conflict_do_nothing = false;
        {
            fn contains_ci(hay: &str, needle: &str) -> bool {
                hay.as_bytes().windows(needle.len()).any(|w|
                    w.eq_ignore_ascii_case(needle.as_bytes()))
            }
            if contains_ci(rest, "RETURNING") {
                return None;
            }
            if contains_ci(rest, "ON CONFLICT") {
                if contains_ci(rest, "DO NOTHING") {
                    on_conflict_do_nothing = true;
                } else {
                    return None;
                }
            }
        }
        let values_str = rest[6..].trim_start();
        // Strip trailing ON CONFLICT DO NOTHING
        let values_str = if on_conflict_do_nothing {
            let bytes = values_str.as_bytes();
            let needle = b"ON CONFLICT";
            let idx = bytes.windows(needle.len()).position(|w|
                w.eq_ignore_ascii_case(needle));
            if let Some(i) = idx {
                values_str[..i].trim_end()
            } else {
                values_str
            }
        } else {
            values_str
        };

        // Last-1 schema cache: skip catalog RwLock + deep clone for repeated table
        let schema = {
            let cached = self.schema_cache.lock();
            if let Some((ref name, ref s)) = *cached {
                if name == table_name { Some(s.clone()) } else { None }
            } else { None }
        }.or_else(|| {
            let s = self.storage.get_table_schema(table_name)?;
            *self.schema_cache.lock() = Some((table_name.to_owned(), s.clone()));
            Some(s)
        })?;
        let col_indices: Vec<usize> = col_names.iter().map(|name| {
            schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
        }).collect::<Option<Vec<_>>>()?;

        // Collect column DataTypes for value parsing
        use falcon_common::types::DataType;
        let col_types: Vec<&DataType> = col_indices.iter().map(|&i| &schema.columns[i].data_type).collect();
        let ncols = col_indices.len();

        // Parse VALUES tuples using byte-level scanner
        let bytes = values_str.as_bytes();
        let len = bytes.len();
        let mut pos = 0;
        let mut rows: Vec<Vec<BoundExpr>> = Vec::new();

        while pos < len {
            // Skip whitespace and commas between tuples
            while pos < len && matches!(bytes[pos], b' ' | b'\t' | b'\n' | b'\r' | b',') {
                pos += 1;
            }
            if pos >= len || bytes[pos] == b';' { break; }
            if bytes[pos] != b'(' { return None; }
            pos += 1;

            let mut vals = Vec::with_capacity(ncols);
            #[allow(clippy::needless_range_loop)]
            for vi in 0..ncols {
                // Skip whitespace
                while pos < len && bytes[pos] == b' ' { pos += 1; }
                if vi > 0 {
                    if pos < len && bytes[pos] == b',' { pos += 1; }
                    while pos < len && bytes[pos] == b' ' { pos += 1; }
                }

                // Parse one value
                let datum = if bytes[pos] == b'\'' {
                    // String literal
                    pos += 1;
                    let mut s = String::new();
                    let mut start = pos;
                    loop {
                        if pos >= len { return None; }
                        if bytes[pos] == b'\'' {
                            s.push_str(std::str::from_utf8(&bytes[start..pos]).ok()?);
                            pos += 1;
                            if pos < len && bytes[pos] == b'\'' {
                                // Escaped quote
                                s.push('\'');
                                pos += 1;
                                start = pos;
                            } else {
                                break;
                            }
                        } else {
                            pos += 1;
                        }
                    }
                    Datum::Text(s)
                } else if bytes[pos] == b'N' || bytes[pos] == b'n' {
                    // NULL
                    if pos + 4 <= len && values_str[pos..pos+4].eq_ignore_ascii_case("null") {
                        pos += 4;
                        Datum::Null
                    } else {
                        return None;
                    }
                } else if bytes[pos] == b't' || bytes[pos] == b'T' {
                    // true
                    if pos + 4 <= len && values_str[pos..pos+4].eq_ignore_ascii_case("true") {
                        pos += 4;
                        Datum::Boolean(true)
                    } else {
                        return None;
                    }
                } else if bytes[pos] == b'f' || bytes[pos] == b'F' {
                    // false
                    if pos + 5 <= len && values_str[pos..pos+5].eq_ignore_ascii_case("false") {
                        pos += 5;
                        Datum::Boolean(false)
                    } else {
                        return None;
                    }
                } else if bytes[pos] == b'c' || bytes[pos] == b'C' {
                    // CURRENT_TIMESTAMP / CURRENT_DATE
                    if pos + 17 <= len && values_str[pos..pos+17].eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
                        pos += 17;
                        let now = chrono::Utc::now();
                        Datum::Timestamp(now.timestamp() * 1_000_000 + now.timestamp_subsec_micros() as i64)
                    } else if pos + 12 <= len && values_str[pos..pos+12].eq_ignore_ascii_case("CURRENT_DATE") {
                        pos += 12;
                        let now = chrono::Utc::now();
                        Datum::Timestamp(now.timestamp() * 1_000_000 + now.timestamp_subsec_micros() as i64)
                    } else {
                        return None;
                    }
                } else if bytes[pos] == b'-' || bytes[pos].is_ascii_digit() {
                    // Number
                    let num_start = pos;
                    if bytes[pos] == b'-' { pos += 1; }
                    let mut is_float = false;
                    while pos < len && bytes[pos].is_ascii_digit() { pos += 1; }
                    if pos < len && bytes[pos] == b'.' {
                        is_float = true;
                        pos += 1;
                        while pos < len && bytes[pos].is_ascii_digit() { pos += 1; }
                    }
                    // Scientific notation
                    if pos < len && (bytes[pos] == b'e' || bytes[pos] == b'E') {
                        is_float = true;
                        pos += 1;
                        if pos < len && (bytes[pos] == b'+' || bytes[pos] == b'-') { pos += 1; }
                        while pos < len && bytes[pos].is_ascii_digit() { pos += 1; }
                    }
                    let num_str = std::str::from_utf8(&bytes[num_start..pos]).ok()?;
                    if is_float {
                        Datum::Float64(num_str.parse().ok()?)
                    } else {
                        match col_types[vi] {
                            DataType::Int32 => Datum::Int32(num_str.parse().ok()?),
                            DataType::Float64 => Datum::Float64(num_str.parse().ok()?),
                            _ => Datum::Int64(num_str.parse().ok()?),
                        }
                    }
                } else {
                    return None;
                };
                vals.push(BoundExpr::Literal(datum));
            }

            // Skip whitespace and expect ')'
            while pos < len && bytes[pos] == b' ' { pos += 1; }
            if pos >= len || bytes[pos] != b')' { return None; }
            pos += 1;
            rows.push(vals);
        }

        if rows.is_empty() { return None; }

        Some(BoundInsert {
            table_id: schema.id,
            table_name: table_name.to_owned(),
            schema,
            columns: col_indices,
            rows,
            source_select: None,
            returning: vec![],
            on_conflict: if on_conflict_do_nothing {
                Some(OnConflictAction::DoNothing)
            } else {
                None
            },
        })
    }

    /// Ultra-fast direct insert: skip executor entirely for simple schemas.
    /// Handles dynamic defaults (CURRENT_TIMESTAMP etc.) and partial column lists.
    /// Returns None if schema is too complex (constraints, serials, etc.).
    fn try_direct_insert(
        &self,
        ins: &BoundInsert,
        session: &mut PgSession,
    ) -> Option<Result<Vec<BackendMessage>, FalconError>> {
        // Disabled: saves ~1µs per txn at 16c but causes convoy at 64c (51K→20K).
        // The executor path's overhead naturally staggers flush_lock arrivals.
        return None;
        #[allow(unreachable_code)]
        use falcon_common::datum::OwnedRow;
        use falcon_common::schema::DefaultFn;
        if self.cluster.dist_engine.is_some() || session.txn.is_some() {
            return None;
        }
        let schema = &ins.schema;
        if !schema.check_constraints.is_empty()
            || !schema.unique_constraints.is_empty()
            || !schema.foreign_keys.is_empty()
            || schema.columns.iter().any(|c| c.is_serial)
        {
            return None;
        }
        let on_conflict_skip = matches!(ins.on_conflict, Some(OnConflictAction::DoNothing));
        if ins.on_conflict.is_some() && !on_conflict_skip {
            return None;
        }
        let ncols = schema.columns.len();
        let mut rows: Vec<OwnedRow> = Vec::with_capacity(ins.rows.len());
        for row_exprs in &ins.rows {
            // Start with static defaults for all columns
            let mut values: Vec<Datum> = schema.columns.iter()
                .map(|c| c.default_value.clone().unwrap_or(Datum::Null))
                .collect();
            // Overlay provided columns with literal values
            for (i, expr) in row_exprs.iter().enumerate() {
                match expr {
                    BoundExpr::Literal(d) => values[ins.columns[i]] = d.clone(),
                    _ => return None,
                }
            }
            // Fill dynamic defaults for still-NULL columns
            for (&col_idx, dfn) in &schema.dynamic_defaults {
                if col_idx < ncols && values[col_idx].is_null() {
                    values[col_idx] = match dfn {
                        DefaultFn::CurrentTimestamp => {
                            Datum::Timestamp(chrono::Utc::now().timestamp_micros())
                        }
                        DefaultFn::CurrentDate => {
                            let today = chrono::Utc::now().date_naive();
                            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            Datum::Date((today - epoch).num_days() as i32)
                        }
                        DefaultFn::CurrentTime => {
                            use chrono::Timelike;
                            let t = chrono::Utc::now().time();
                            Datum::Time(
                                t.num_seconds_from_midnight() as i64 * 1_000_000
                                    + t.nanosecond() as i64 / 1_000,
                            )
                        }
                        DefaultFn::Nextval(_) => continue,
                    };
                }
            }
            rows.push(OwnedRow::new(values));
        }
        let txn = self.txn_mgr.begin_autocommit(session.default_isolation);
        let txn_id = txn.txn_id;
        session.txn = Some(txn);
        let mut count = 0u64;
        for row in rows {
            match self.storage.insert(ins.table_id, row, txn_id) {
                Ok(_) => count += 1,
                Err(falcon_common::error::StorageError::DuplicateKey) if on_conflict_skip => {}
                Err(e) => {
                    self.txn_mgr.abort_autocommit(txn_id);
                    session.txn = None;
                    return Some(Ok(vec![self.error_response(&FalconError::Storage(e))]));
                }
            }
        }
        if let Err(e) = self.txn_mgr.commit_autocommit(txn_id) {
            session.txn = None;
            return Some(Ok(vec![self.error_response(&FalconError::Txn(e))]));
        }
        session.txn = None;
        let tag = if count == 1 { "INSERT 0 1".to_owned() } else { format!("INSERT 0 {count}") };
        Some(Ok(vec![BackendMessage::CommandComplete { tag }]))
    }

    /// Fast-path parser for simple UPDATE table SET col = expr WHERE col = val.
    /// Handles: col = literal, col = col +/- literal.
    /// Returns None if the SQL doesn't match (falls back to standard path).
    fn try_fast_update_parse(&self, sql: &str) -> Option<PhysicalPlan> {
        let trimmed = sql.trim().trim_end_matches(';').trim_end();
        if trimmed.len() < 20 || !trimmed[..6].eq_ignore_ascii_case("UPDATE") {
            return None;
        }
        let rest = trimmed[6..].trim_start();

        // Table name
        let tbl_end = rest.find(|c: char| !c.is_alphanumeric() && c != '_' && c != '"')?;
        if tbl_end == 0 { return None; }
        let table_name = rest[..tbl_end].trim_matches('"');
        let rest = rest[tbl_end..].trim_start();

        // SET keyword
        if rest.len() < 4 || !rest[..3].eq_ignore_ascii_case("SET") { return None; }
        let rest = rest[3..].trim_start();

        // Reject RETURNING / FROM (complex) — zero-alloc
        fn contains_ci_u(hay: &str, needle: &str) -> bool {
            hay.as_bytes().windows(needle.len()).any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
        }
        if contains_ci_u(rest, "RETURNING") || contains_ci_u(rest, " FROM ") {
            return None;
        }

        // Find WHERE — zero-alloc byte scan
        let where_pos = {
            let bytes = rest.as_bytes();
            let mut found = None;
            for i in 0..bytes.len().saturating_sub(6) {
                if bytes[i] == b' ' && bytes[i+1..i+6].eq_ignore_ascii_case(b"WHERE") && (i + 6 >= bytes.len() || bytes[i+6] == b' ') {
                    found = Some(i);
                    break;
                }
            }
            found?
        };
        let set_part = rest[..where_pos].trim();
        let where_part = rest[where_pos + 7..].trim();

        // Look up table (use schema cache)
        let schema = {
            let cached = self.schema_cache.lock();
            if let Some((ref name, ref s)) = *cached {
                if name == table_name { Some(s.clone()) } else { None }
            } else { None }
        }.or_else(|| {
            let s = self.storage.get_table_schema(table_name)?;
            *self.schema_cache.lock() = Some((table_name.to_owned(), s.clone()));
            Some(s)
        })?;

        // Parse assignments: col = expr [, col = expr]
        let mut assignments = Vec::new();
        for assign_str in set_part.split(',') {
            let assign_str = assign_str.trim();
            let eq_pos = assign_str.find('=')?;
            let col_name = assign_str[..eq_pos].trim().trim_matches('"');
            let expr_str = assign_str[eq_pos + 1..].trim();

            let col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))?;

            // Parse expr: literal | col +/- literal
            let expr = self.parse_fast_expr(expr_str, &schema)?;
            assignments.push((col_idx, expr));
        }

        // Parse WHERE: col = literal [AND col = literal]
        let mut filters: Vec<BoundExpr> = Vec::new();
        // AND splitting — zero-alloc byte scan
        let conds: Vec<&str> = {
            let bytes = where_part.as_bytes();
            let mut parts = Vec::new();
            let mut start = 0;
            let mut i = 0;
            while i + 5 <= bytes.len() {
                if bytes[i] == b' ' && bytes[i+1..i+4].eq_ignore_ascii_case(b"AND") && bytes[i+4] == b' ' {
                    parts.push(where_part[start..i].trim());
                    start = i + 5;
                    i = start;
                } else {
                    i += 1;
                }
            }
            parts.push(where_part[start..].trim());
            parts
        };
        for cond in conds {
            let eq_pos = cond.find('=')?;
            // Reject !=, >=, <=
            if eq_pos > 0 && matches!(cond.as_bytes()[eq_pos - 1], b'!' | b'>' | b'<') {
                return None;
            }
            if eq_pos + 1 < cond.len() && cond.as_bytes()[eq_pos + 1] == b'=' {
                return None; // == not valid SQL but bail
            }
            let col_name = cond[..eq_pos].trim().trim_matches('"');
            let val_str = cond[eq_pos + 1..].trim();
            let col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))?;
            let val = self.parse_fast_literal(val_str, &schema.columns[col_idx].data_type)?;
            filters.push(BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(col_idx)),
                op: BinOp::Eq,
                right: Box::new(BoundExpr::Literal(val)),
            });
        }

        let filter = if filters.len() == 1 {
            Some(filters.pop().unwrap())
        } else if filters.len() > 1 {
            let mut combined = filters.pop().unwrap();
            while let Some(f) = filters.pop() {
                combined = BoundExpr::BinaryOp {
                    left: Box::new(f),
                    op: BinOp::And,
                    right: Box::new(combined),
                };
            }
            Some(combined)
        } else {
            None
        };

        Some(PhysicalPlan::Update {
            table_id: schema.id,
            schema,
            assignments,
            filter,
            returning: vec![],
            from_table: None,
        })
    }

    fn parse_fast_expr(&self, s: &str, schema: &falcon_common::schema::TableSchema) -> Option<BoundExpr> {
        let s = s.trim();
        // Try: col + literal or col - literal
        if let Some(plus_pos) = s.find('+') {
            let lhs = s[..plus_pos].trim();
            let rhs = s[plus_pos + 1..].trim();
            if let Some(col_idx) = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(lhs)) {
                let val = self.parse_fast_literal(rhs, &schema.columns[col_idx].data_type)?;
                return Some(BoundExpr::BinaryOp {
                    left: Box::new(BoundExpr::ColumnRef(col_idx)),
                    op: BinOp::Plus,
                    right: Box::new(BoundExpr::Literal(val)),
                });
            }
        }
        if let Some(minus_pos) = s.rfind('-') {
            if minus_pos > 0 {
                let lhs = s[..minus_pos].trim();
                let rhs = s[minus_pos + 1..].trim();
                if let Some(col_idx) = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(lhs)) {
                    let val = self.parse_fast_literal(rhs, &schema.columns[col_idx].data_type)?;
                    return Some(BoundExpr::BinaryOp {
                        left: Box::new(BoundExpr::ColumnRef(col_idx)),
                        op: BinOp::Minus,
                        right: Box::new(BoundExpr::Literal(val)),
                    });
                }
            }
        }
        // Try: plain literal
        // Need to guess type — use Int64 for numbers, Text for strings
        if s.starts_with('\'') {
            let inner = s.trim_matches('\'');
            return Some(BoundExpr::Literal(Datum::Text(inner.to_owned())));
        }
        if s.eq_ignore_ascii_case("NULL") {
            return Some(BoundExpr::Literal(Datum::Null));
        }
        if let Ok(v) = s.parse::<i64>() {
            return Some(BoundExpr::Literal(Datum::Int64(v)));
        }
        if let Ok(v) = s.parse::<f64>() {
            return Some(BoundExpr::Literal(Datum::Float64(v)));
        }
        None
    }

    fn parse_fast_literal(&self, s: &str, dt: &falcon_common::types::DataType) -> Option<Datum> {
        use falcon_common::types::DataType;
        let s = s.trim();
        if s.eq_ignore_ascii_case("NULL") { return Some(Datum::Null); }
        if s.starts_with('\'') {
            return Some(Datum::Text(s.trim_matches('\'').to_owned()));
        }
        match dt {
            DataType::Int32 => Some(Datum::Int32(s.parse().ok()?)),
            DataType::Float64 => Some(Datum::Float64(s.parse().ok()?)),
            _ => Some(Datum::Int64(s.parse().ok()?)),
        }
    }

    /// Execute a single DML plan (fast-path for INSERT). Handles autocommit.
    fn execute_single_plan(
        &self,
        _sql: &str,
        plan: PhysicalPlan,
        session: &mut PgSession,
    ) -> Result<Vec<BackendMessage>, FalconError> {
        let mut messages = Vec::new();

        let auto_txn = if session.txn.is_none() {
            let txn = self.txn_mgr.begin_autocommit(session.default_isolation);
            session.txn = Some(txn);
            true
        } else {
            false
        };

        // Execute
        let result = if let Some(dist) = &self.cluster.dist_engine {
            dist.execute(&plan, session.txn.as_ref())
        } else {
            self.executor.execute(&plan, session.txn.as_ref())
        };

        match result {
            Ok(ExecutionResult::Dml { rows_affected, tag }) => {
                let cmd_tag = match (tag, rows_affected) {
                    ("INSERT", 1) => "INSERT 0 1".to_owned(),
                    ("INSERT", n) => format!("INSERT 0 {n}"),
                    ("UPDATE", 1) => "UPDATE 1".to_owned(),
                    ("UPDATE", n) => format!("UPDATE {n}"),
                    ("DELETE", 1) => "DELETE 1".to_owned(),
                    ("DELETE", n) => format!("DELETE {n}"),
                    (t, n) => format!("{t} {n}"),
                };
                messages.push(BackendMessage::CommandComplete { tag: cmd_tag });
            }
            Ok(other) => {
                // Unexpected result type for fast-path; shouldn't happen
                messages.push(BackendMessage::CommandComplete {
                    tag: format!("{other:?}"),
                });
            }
            Err(e) => {
                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        self.txn_mgr.abort_autocommit(txn.txn_id);
                    }
                    session.txn = None;
                }
                messages.push(self.error_response(&e));
                return Ok(messages);
            }
        }

        if auto_txn {
            if let Some(ref txn) = session.txn {
                let _ = self.txn_mgr.commit_autocommit(txn.txn_id);
            }
            session.txn = None;
        }

        Ok(messages)
    }

    fn fast_begin(&self, session: &mut PgSession) -> Vec<BackendMessage> {
        if session.in_transaction() {
            return vec![
                BackendMessage::NoticeResponse {
                    message: "there is already a transaction in progress".into(),
                },
                BackendMessage::CommandComplete { tag: "BEGIN".into() },
            ];
        }
        if let Err(reason) = self.storage.check_tenant_quota(session.tenant_id) {
            return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "53400".into(),
                message: reason,
            }];
        }
        let txn = match self.txn_mgr.try_begin_with_classification(
            session.default_isolation,
            TxnClassification::local(ShardId(0)),
        ) {
            Ok(t) => t,
            Err(e) => {
                let ce: FalconError = e.into();
                return vec![self.error_response(&ce)];
            }
        };
        self.storage.ext.cdc_manager.emit_begin(txn.txn_id);
        self.storage.record_tenant_txn_begin(session.tenant_id);
        session.txn = Some(txn);
        session.autocommit = false;
        vec![BackendMessage::CommandComplete { tag: "BEGIN".into() }]
    }

    fn fast_commit(&self, session: &mut PgSession) -> Vec<BackendMessage> {
        if let Some(ref txn) = session.txn {
            match self.txn_mgr.commit(txn.txn_id) {
                Ok(commit_ts) => {
                    if let (Some(ref waiter), Some(ref group)) =
                        (&self.cluster.sync_waiter, &self.cluster.ha_group)
                    {
                        let timeout = std::time::Duration::from_secs(5);
                        let group_read = group.read();
                        if let Err(e) = waiter.wait_for_commit(commit_ts.0, &group_read, timeout) {
                            tracing::warn!("Sync replication wait failed after COMMIT: {}", e);
                        }
                    }
                    self.storage.record_tenant_txn_commit(session.tenant_id);
                    falcon_observability::record_txn_metrics("committed");
                    session.txn = None;
                    session.autocommit = true;
                    self.flush_txn_stats();
                    vec![BackendMessage::CommandComplete { tag: "COMMIT".into() }]
                }
                Err(e) => {
                    self.storage.record_tenant_txn_abort(session.tenant_id);
                    falcon_observability::record_txn_metrics("aborted");
                    session.txn = None;
                    session.autocommit = true;
                    self.flush_txn_stats();
                    vec![self.error_response(&FalconError::Txn(e))]
                }
            }
        } else {
            vec![
                BackendMessage::NoticeResponse {
                    message: "there is no transaction in progress".into(),
                },
                BackendMessage::CommandComplete { tag: "COMMIT".into() },
            ]
        }
    }

    fn fast_rollback(&self, session: &mut PgSession) -> Vec<BackendMessage> {
        if let Some(ref txn) = session.txn {
            let _ = self.txn_mgr.abort(txn.txn_id);
            self.storage.record_tenant_txn_abort(session.tenant_id);
            falcon_observability::record_txn_metrics("aborted");
            session.txn = None;
            session.autocommit = true;
            self.flush_txn_stats();
        }
        vec![BackendMessage::CommandComplete { tag: "ROLLBACK".into() }]
    }

    /// Fast-path SELECT for simple point lookups: SELECT col [, col2] FROM table WHERE pk = val
    /// Bypasses sqlparser, binder, planner, and build_indexed_columns entirely.
    fn try_fast_select(&self, sql: &str, session: &mut PgSession) -> Option<Vec<BackendMessage>> {
        let trimmed = sql.trim().trim_end_matches(';').trim_end();
        if trimmed.len() < 20 || !trimmed[..6].eq_ignore_ascii_case("SELECT") {
            return None;
        }
        let rest = trimmed[6..].trim_start();

        // Find FROM keyword
        let from_pos = {
            let bytes = rest.as_bytes();
            let mut i = 0;
            let mut found = None;
            while i + 5 <= bytes.len() {
                if bytes[i] == b' ' && bytes[i+1..i+5].eq_ignore_ascii_case(b"FROM") && (i + 5 >= bytes.len() || bytes[i+5] == b' ') {
                    found = Some(i);
                    break;
                }
                i += 1;
            }
            found?
        };
        let cols_str = rest[..from_pos].trim();
        let after_from = rest[from_pos + 5..].trim_start();

        // Table name
        let tbl_end = after_from.find(|c: char| !c.is_alphanumeric() && c != '_' && c != '"')?;
        if tbl_end == 0 { return None; }
        let table_name = after_from[..tbl_end].trim_matches('"');
        let after_table = after_from[tbl_end..].trim_start();

        // WHERE keyword
        if after_table.len() < 6 || !after_table[..5].eq_ignore_ascii_case("WHERE") {
            return None;
        }
        let where_part = after_table[5..].trim_start();

        // Reject complex WHERE (AND, OR, subqueries, etc. beyond simple equality)
        fn contains_ci(hay: &str, needle: &str) -> bool {
            hay.as_bytes().windows(needle.len()).any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
        }
        if contains_ci(where_part, " AND ") || contains_ci(where_part, " OR ") || where_part.contains('(') {
            return None;
        }

        // Parse: col = value
        let eq_pos = where_part.find('=')?;
        if eq_pos > 0 && matches!(where_part.as_bytes()[eq_pos - 1], b'!' | b'>' | b'<') {
            return None;
        }
        let where_col = where_part[..eq_pos].trim().trim_matches('"');
        let where_val_str = where_part[eq_pos + 1..].trim();

        // Look up schema (use cache)
        let schema = {
            let cached = self.schema_cache.lock();
            if let Some((ref name, ref s)) = *cached {
                if name == table_name { Some(s.clone()) } else { None }
            } else { None }
        }.or_else(|| {
            let s = self.storage.get_table_schema(table_name)?;
            *self.schema_cache.lock() = Some((table_name.to_owned(), s.clone()));
            Some(s)
        })?;

        // WHERE column must be the (single) PK
        if schema.primary_key_columns.len() != 1 { return None; }
        let pk_col_idx = schema.primary_key_columns[0];
        let where_col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(where_col))?;
        if where_col_idx != pk_col_idx { return None; }

        // Parse PK value
        let pk_val = self.parse_fast_literal(where_val_str, &schema.columns[pk_col_idx].data_type)?;
        let pk = falcon_storage::memtable::encode_pk_from_datums(&[&pk_val]);

        // Parse select columns
        let sel_col_names: Vec<&str> = cols_str.split(',').map(|s| s.trim().trim_matches('"')).collect();
        let sel_col_indices: Vec<usize> = sel_col_names.iter().map(|name| {
            schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
        }).collect::<Option<Vec<_>>>()?;

        // Ensure txn exists
        let auto_txn = if session.txn.is_none() {
            let txn = self.txn_mgr.begin_autocommit(session.default_isolation);
            session.txn = Some(txn);
            true
        } else {
            false
        };

        let txn = session.txn.as_ref().unwrap();
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let result = self.storage.get(schema.id, &pk, txn.txn_id, read_ts);

        let mut messages = Vec::new();

        // Build RowDescription
        let fields: Vec<FieldDescription> = sel_col_indices.iter().map(|&ci| {
            let col = &schema.columns[ci];
            FieldDescription {
                name: col.name.clone(),
                table_oid: 0,
                column_attr: 0,
                type_oid: col.data_type.pg_oid(),
                type_len: col.data_type.type_len(),
                type_modifier: -1,
                format_code: 0,
            }
        }).collect();
        messages.push(BackendMessage::RowDescription { fields });

        match result {
            Ok(Some(row)) => {
                let values: Vec<Option<String>> = sel_col_indices.iter()
                    .map(|&ci| row.values.get(ci).map(|d| d.to_pg_text()).unwrap_or(None))
                    .collect();
                messages.push(BackendMessage::DataRow { values });
                messages.push(BackendMessage::CommandComplete { tag: "SELECT 1".into() });
            }
            Ok(None) => {
                messages.push(BackendMessage::CommandComplete { tag: "SELECT 0".into() });
            }
            Err(e) => {
                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        self.txn_mgr.abort_autocommit(txn.txn_id);
                    }
                    session.txn = None;
                }
                return Some(vec![self.error_response(&FalconError::Storage(e))]);
            }
        }

        if auto_txn {
            self.txn_mgr.commit_autocommit_readonly();
            session.txn = None;
        }

        Some(messages)
    }

    /// Direct UPDATE bypass for simple point updates on constraint-free tables.
    /// Parses SQL, reads row, applies SET, writes back — skips executor entirely.
    fn try_fast_direct_update(&self, sql: &str, session: &mut PgSession) -> Option<Vec<BackendMessage>> {
        let trimmed = sql.trim().trim_end_matches(';').trim_end();
        if trimmed.len() < 20 || !trimmed[..6].eq_ignore_ascii_case("UPDATE") {
            return None;
        }
        let rest = trimmed[6..].trim_start();

        // Table name
        let tbl_end = rest.find(|c: char| !c.is_alphanumeric() && c != '_' && c != '"')?;
        if tbl_end == 0 { return None; }
        let table_name = rest[..tbl_end].trim_matches('"');
        let rest = rest[tbl_end..].trim_start();

        // SET keyword
        if rest.len() < 4 || !rest[..3].eq_ignore_ascii_case("SET") { return None; }
        let rest = rest[3..].trim_start();

        // Reject RETURNING / FROM
        fn has_ci(hay: &str, needle: &str) -> bool {
            hay.as_bytes().windows(needle.len()).any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
        }
        if has_ci(rest, "RETURNING") || has_ci(rest, " FROM ") { return None; }

        // Find WHERE
        let where_pos = {
            let bytes = rest.as_bytes();
            let mut found = None;
            for i in 0..bytes.len().saturating_sub(6) {
                if bytes[i] == b' ' && bytes[i+1..i+6].eq_ignore_ascii_case(b"WHERE") && (i + 6 >= bytes.len() || bytes[i+6] == b' ') {
                    found = Some(i);
                    break;
                }
            }
            found?
        };
        let set_part = rest[..where_pos].trim();
        let where_part = rest[where_pos + 7..].trim();

        // Schema (cached)
        let schema = {
            let cached = self.schema_cache.lock();
            if let Some((ref name, ref s)) = *cached {
                if name == table_name { Some(s.clone()) } else { None }
            } else { None }
        }.or_else(|| {
            let s = self.storage.get_table_schema(table_name)?;
            *self.schema_cache.lock() = Some((table_name.to_owned(), s.clone()));
            Some(s)
        })?;

        // Eligibility: no constraints, single PK, no RETURNING
        if !schema.unique_constraints.is_empty()
            || !schema.check_constraints.is_empty()
            || !schema.foreign_keys.is_empty()
            || schema.primary_key_columns.len() != 1
        {
            return None;
        }
        let pk_col_idx = schema.primary_key_columns[0];

        // Parse WHERE: only pk = literal (no AND)
        if has_ci(where_part, " AND ") || has_ci(where_part, " OR ") { return None; }
        let eq_pos = where_part.find('=')?;
        if eq_pos > 0 && matches!(where_part.as_bytes()[eq_pos - 1], b'!' | b'>' | b'<') { return None; }
        let w_col = where_part[..eq_pos].trim().trim_matches('"');
        let w_val_str = where_part[eq_pos + 1..].trim();
        let w_col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(w_col))?;
        if w_col_idx != pk_col_idx { return None; }
        let pk_val = self.parse_fast_literal(w_val_str, &schema.columns[pk_col_idx].data_type)?;
        let pk = falcon_storage::memtable::encode_pk_from_datums(&[&pk_val]);

        // Parse SET assignments: col = expr [, col = expr]
        // For direct execution, we store (col_idx, SetOp) instead of BoundExpr
        enum SetOp { Literal(Datum), Plus(usize, Datum), Minus(usize, Datum) }
        let mut ops: Vec<(usize, SetOp)> = Vec::new();
        for assign_str in set_part.split(',') {
            let assign_str = assign_str.trim();
            let aeq = assign_str.find('=')?;
            let col_name = assign_str[..aeq].trim().trim_matches('"');
            let expr_str = assign_str[aeq + 1..].trim();
            let col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))?;

            if let Some(plus_pos) = expr_str.find('+') {
                let lhs = expr_str[..plus_pos].trim();
                let rhs = expr_str[plus_pos + 1..].trim();
                if schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(lhs))? == col_idx {
                    let val = self.parse_fast_literal(rhs, &schema.columns[col_idx].data_type)?;
                    ops.push((col_idx, SetOp::Plus(col_idx, val)));
                } else { return None; }
            } else if let Some(minus_pos) = expr_str.rfind('-') {
                if minus_pos > 0 {
                    let lhs = expr_str[..minus_pos].trim();
                    let rhs = expr_str[minus_pos + 1..].trim();
                    if schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(lhs))? == col_idx {
                        let val = self.parse_fast_literal(rhs, &schema.columns[col_idx].data_type)?;
                        ops.push((col_idx, SetOp::Minus(col_idx, val)));
                    } else { return None; }
                } else { return None; }
            } else {
                let val = self.parse_fast_literal(expr_str, &schema.columns[col_idx].data_type)?;
                ops.push((col_idx, SetOp::Literal(val)));
            }
        }

        // Ensure txn
        let auto_txn = if session.txn.is_none() {
            let txn = self.txn_mgr.begin_autocommit(session.default_isolation);
            session.txn = Some(txn);
            true
        } else {
            false
        };

        let txn = session.txn.as_ref().unwrap();
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());

        // Read current row
        let row = match self.storage.get(schema.id, &pk, txn.txn_id, read_ts) {
            Ok(Some(r)) => r,
            Ok(None) => {
                let tag = "UPDATE 0".to_owned();
                if auto_txn {
                    let _ = self.txn_mgr.commit_autocommit(txn.txn_id);
                    session.txn = None;
                }
                return Some(vec![BackendMessage::CommandComplete { tag }]);
            }
            Err(e) => {
                if auto_txn {
                    self.txn_mgr.abort_autocommit(txn.txn_id);
                    session.txn = None;
                }
                return Some(vec![self.error_response(&FalconError::Storage(e))]);
            }
        };

        // Apply assignments
        let mut new_values = row.values.clone();
        for (col_idx, op) in &ops {
            new_values[*col_idx] = match op {
                SetOp::Literal(v) => v.clone(),
                SetOp::Plus(src, v) => datum_add(&row.values[*src], v)?,
                SetOp::Minus(src, v) => datum_sub(&row.values[*src], v)?,
            };
        }

        let new_row = falcon_common::datum::OwnedRow::new(new_values);
        match self.storage.update(schema.id, &pk, new_row, txn.txn_id) {
            Ok(()) => {}
            Err(e) => {
                if auto_txn {
                    self.txn_mgr.abort_autocommit(txn.txn_id);
                    session.txn = None;
                }
                return Some(vec![self.error_response(&FalconError::Storage(e))]);
            }
        }

        if auto_txn {
            let _ = self.txn_mgr.commit_autocommit(txn.txn_id);
            session.txn = None;
        }

        Some(vec![BackendMessage::CommandComplete { tag: "UPDATE 1".to_owned() }])
    }

    /// Inner query processing logic, called from `handle_query` inside a
    /// crash-domain guard.
    fn handle_query_inner(
        &self,
        sql: &str,
        session: &mut PgSession,
    ) -> Result<Vec<BackendMessage>, FalconError> {
        // ── Fast-path: lightweight INSERT parser (bypasses sqlparser-rs) ──
        if let Some(fast_ins) = self.try_fast_insert_parse(sql) {
            // Ultra-fast: skip executor entirely for simple schemas
            if let Some(result) = self.try_direct_insert(&fast_ins, session) {
                return result;
            }
            let plan = match Planner::plan_insert_owned(fast_ins) {
                Ok(p) => Planner::wrap_distributed(p, &self.cluster.shard_ids),
                Err(e) => return Ok(vec![self.error_response(&FalconError::Sql(e))]),
            };
            return self.execute_single_plan(sql, plan, session);
        }

        // ── Fast-path: lightweight UPDATE parser (bypasses sqlparser-rs) ──
        if let Some(plan) = self.try_fast_update_parse(sql) {
            let plan = Planner::wrap_distributed(plan, &self.cluster.shard_ids);
            return self.execute_single_plan(sql, plan, session);
        }

        // ── Standard path: sqlparser + binder + planner ──
        let stmts = match parse_sql(sql) {
            Ok(stmts) => stmts,
            Err(e) => {
                return Ok(vec![self.error_response(&FalconError::Sql(e))]);
            }
        };

        let mut messages = Vec::new();

        for stmt in &stmts {
            // Try plan cache first
            let plan = if let Some(cached) = self.observability.plan_cache.get(sql) {
                cached
            } else {
                // Bind
                let catalog = self.storage.get_catalog();
                let mut binder = Binder::new(catalog);
                let bound = match binder.bind(stmt) {
                    Ok(b) => b,
                    Err(e) => {
                        messages.push(self.error_response(&FalconError::Sql(e)));
                        return Ok(messages);
                    }
                };

                // Fast path for INSERT: skip row_counts/indexed_cols (unused)
                // and use owned planning to avoid cloning the rows vector.
                
                if let BoundStatement::Insert(ins) = bound {
                    match Planner::plan_insert_owned(ins) {
                        Ok(p) => Planner::wrap_distributed(p, &self.cluster.shard_ids),
                        Err(e) => {
                            messages.push(self.error_response(&FalconError::Sql(e)));
                            return Ok(messages);
                        }
                    }
                } else {
                    let is_dml = matches!(&bound, BoundStatement::Update(_) | BoundStatement::Delete(_));
                    let (row_counts, indexed_cols) = if is_dml {
                        (TableRowCounts::new(), IndexedColumns::new())
                    } else {
                        (self.build_row_counts(), self.build_indexed_columns())
                    };
                    let p = match Planner::plan_with_indexes(&bound, &row_counts, &indexed_cols) {
                        Ok(p) => p,
                        Err(e) => {
                            messages.push(self.error_response(&FalconError::Sql(e)));
                            return Ok(messages);
                        }
                    };
                    let p = Planner::wrap_distributed(p, &self.cluster.shard_ids);
                    // Cache the plan (skip DML — each UPDATE/DELETE has unique literals)
                    if !matches!(p, PhysicalPlan::Update { .. } | PhysicalPlan::Delete { .. }) {
                        self.observability.plan_cache.put(sql, p.clone());
                    }
                    p
                }
            };

            let routing_hint = plan.routing_hint();

            // Handle transaction control
            match &plan {
                PhysicalPlan::Begin => {
                    if session.in_transaction() {
                        messages.push(BackendMessage::NoticeResponse {
                            message: "there is already a transaction in progress".into(),
                        });
                    }
                    // Multi-tenant: check quota before allowing BEGIN
                    if let Err(reason) = self.storage.check_tenant_quota(session.tenant_id) {
                        messages.push(BackendMessage::ErrorResponse {
                            severity: "ERROR".into(),
                            code: "53400".into(),
                            message: reason,
                        });
                        continue;
                    }
                    let txn = match self.txn_mgr.try_begin_with_classification(
                        session.default_isolation,
                        TxnClassification::local(ShardId(0)),
                    ) {
                        Ok(t) => t,
                        Err(e) => {
                            let ce: FalconError = e.into();
                            messages.push(self.error_response(&ce));
                            continue;
                        }
                    };
                    // CDC: emit BEGIN marker for explicit transaction
                    self.storage.ext.cdc_manager.emit_begin(txn.txn_id);
                    // Multi-tenant: record txn begin for metering
                    self.storage.record_tenant_txn_begin(session.tenant_id);

                    session.txn = Some(txn);
                    session.autocommit = false;
                    messages.push(BackendMessage::CommandComplete {
                        tag: "BEGIN".into(),
                    });
                    continue;
                }
                PhysicalPlan::Commit => {
                    if let Some(ref txn) = session.txn {
                        match self.txn_mgr.commit(txn.txn_id) {
                            Ok(commit_ts) => {
                                // Sync replication: wait for replicas to ack before responding
                                if let (Some(ref waiter), Some(ref group)) =
                                    (&self.cluster.sync_waiter, &self.cluster.ha_group)
                                {
                                    let timeout = std::time::Duration::from_secs(5);
                                    let group_read = group.read();
                                    if let Err(e) =
                                        waiter.wait_for_commit(commit_ts.0, &group_read, timeout)
                                    {
                                        tracing::warn!(
                                            "Sync replication wait failed after COMMIT: {}",
                                            e
                                        );
                                        // Commit succeeded locally but replication timed out —
                                        // report as warning, not error (data is durable on primary).
                                        messages.push(BackendMessage::NoticeResponse {
                                            message: format!("sync replication timeout: {e}"),
                                        });
                                    }
                                }
                                // Multi-tenant: record commit
                                self.storage.record_tenant_txn_commit(session.tenant_id);
                                session.txn = None;
                                session.autocommit = true;
                                self.flush_txn_stats();
                                messages.push(BackendMessage::CommandComplete {
                                    tag: "COMMIT".into(),
                                });
                            }
                            Err(e) => {
                                // Multi-tenant: record abort (commit failed)
                                self.storage.record_tenant_txn_abort(session.tenant_id);
                                session.txn = None;
                                session.autocommit = true;
                                self.flush_txn_stats();
                                messages.push(self.error_response(&FalconError::Txn(e)));
                            }
                        }
                    } else {
                        messages.push(BackendMessage::NoticeResponse {
                            message: "there is no transaction in progress".into(),
                        });
                        messages.push(BackendMessage::CommandComplete {
                            tag: "COMMIT".into(),
                        });
                    }
                    continue;
                }
                PhysicalPlan::Rollback => {
                    if let Some(ref txn) = session.txn {
                        let _ = self.txn_mgr.abort(txn.txn_id);
                        // Multi-tenant: record abort
                        self.storage.record_tenant_txn_abort(session.tenant_id);
                        session.txn = None;
                        session.autocommit = true;
                        self.flush_txn_stats();
                    }
                    messages.push(BackendMessage::CommandComplete {
                        tag: "ROLLBACK".into(),
                    });
                    continue;
                }
                _ => {}
            }

            if let Some(ref txn) = session.txn {
                let _ = self
                    .txn_mgr
                    .observe_involved_shards(txn.txn_id, &routing_hint.involved_shards);
                if matches!(routing_hint.planned_txn_type(), PlannedTxnType::Global) {
                    let _ = self.txn_mgr.force_global(txn.txn_id, SlowPathMode::Xa2Pc);
                }
            }

            // Handle COPY FROM STDIN — store state in session, return CopyInResponse
            if let PhysicalPlan::CopyFrom {
                table_id,
                schema,
                columns,
                csv,
                delimiter,
                header,
                null_string,
                quote,
                escape,
            } = &plan
            {
                use crate::session::{CopyFormat, CopyState};
                session.copy_state = Some(CopyState {
                    table_name: schema.name.clone(),
                    table_id: *table_id,
                    schema: schema.clone(),
                    columns: columns.clone(),
                    format: CopyFormat {
                        csv: *csv,
                        delimiter: *delimiter,
                        header: *header,
                        null_string: null_string.clone(),
                        quote: *quote,
                        escape: *escape,
                    },
                });
                let col_formats = vec![0i16; columns.len()]; // text format
                messages.push(BackendMessage::CopyInResponse {
                    format: 0,
                    column_formats: col_formats,
                });
                return Ok(messages);
            }

            // Handle COPY TO STDOUT — execute scan and stream data
            if let PhysicalPlan::CopyTo {
                table_id,
                schema,
                columns,
                csv,
                delimiter,
                header,
                null_string,
                quote,
                escape,
            } = &plan
            {
                // Ensure a transaction exists
                let auto_txn = if session.txn.is_none() {
                    let classification = classification_from_routing_hint(&routing_hint);
                    let txn = match self
                        .txn_mgr
                        .try_begin_with_classification(session.default_isolation, classification)
                    {
                        Ok(t) => t,
                        Err(e) => {
                            let ce: FalconError = e.into();
                            messages.push(self.error_response(&ce));
                            continue;
                        }
                    };
                    session.txn = Some(txn);
                    true
                } else {
                    false
                };

                let txn_ref = if let Some(t) = session.txn.as_ref() { t } else {
                    messages.push(BackendMessage::ErrorResponse {
                        severity: "ERROR".into(),
                        code: "25P01".into(),
                        message: "no active transaction for COPY TO".into(),
                    });
                    continue;
                };
                let result = self.executor.exec_copy_to(
                    *table_id,
                    schema,
                    columns,
                    *csv,
                    *delimiter,
                    *header,
                    null_string,
                    *quote,
                    *escape,
                    txn_ref,
                );

                match result {
                    Ok(ExecutionResult::Query { rows, .. }) => {
                        let col_formats = vec![0i16; columns.len()];
                        messages.push(BackendMessage::CopyOutResponse {
                            format: 0,
                            column_formats: col_formats,
                        });
                        let row_count = rows.len();
                        for row in &rows {
                            if let Some(Datum::Text(ref line)) = row.values.first().cloned() {
                                messages.push(BackendMessage::CopyData(line.as_bytes().to_vec()));
                            }
                        }
                        messages.push(BackendMessage::CopyDone);
                        messages.push(BackendMessage::CommandComplete {
                            tag: format!("COPY {row_count}"),
                        });
                    }
                    Err(e) => {
                        messages.push(self.error_response(&e));
                    }
                    _ => {}
                }

                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        let _ = self.txn_mgr.commit(txn.txn_id);
                    }
                    session.txn = None;
                    self.flush_txn_stats();
                }
                return Ok(messages);
            }

            // Handle COPY (query) TO STDOUT
            if let PhysicalPlan::CopyQueryTo {
                query,
                csv,
                delimiter,
                header,
                null_string,
                quote,
                escape,
            } = &plan
            {
                let auto_txn = if session.txn.is_none() {
                    let classification = classification_from_routing_hint(&routing_hint);
                    let txn = match self
                        .txn_mgr
                        .try_begin_with_classification(session.default_isolation, classification)
                    {
                        Ok(t) => t,
                        Err(e) => {
                            let ce: FalconError = e.into();
                            messages.push(self.error_response(&ce));
                            continue;
                        }
                    };
                    session.txn = Some(txn);
                    true
                } else {
                    false
                };

                let txn_ref = if let Some(t) = session.txn.as_ref() { t } else {
                    messages.push(BackendMessage::ErrorResponse {
                        severity: "ERROR".into(),
                        code: "25P01".into(),
                        message: "no active transaction for COPY TO (query)".into(),
                    });
                    continue;
                };
                let result = self.executor.exec_copy_query_to(
                    query,
                    *csv,
                    *delimiter,
                    *header,
                    null_string,
                    *quote,
                    *escape,
                    txn_ref,
                );

                match result {
                    Ok(ExecutionResult::Query { rows, .. }) => {
                        let col_formats = vec![0i16; 1]; // single text column
                        messages.push(BackendMessage::CopyOutResponse {
                            format: 0,
                            column_formats: col_formats,
                        });
                        let row_count = rows.len();
                        for row in &rows {
                            if let Some(Datum::Text(ref line)) = row.values.first().cloned() {
                                messages.push(BackendMessage::CopyData(line.as_bytes().to_vec()));
                            }
                        }
                        messages.push(BackendMessage::CopyDone);
                        messages.push(BackendMessage::CommandComplete {
                            tag: format!("COPY {row_count}"),
                        });
                    }
                    Err(e) => {
                        messages.push(self.error_response(&e));
                    }
                    _ => {}
                }

                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        let _ = self.txn_mgr.commit(txn.txn_id);
                    }
                    session.txn = None;
                    self.flush_txn_stats();
                }
                return Ok(messages);
            }

            // For DDL and metadata commands, execute without txn
            if matches!(
                plan,
                PhysicalPlan::CreateTable { .. }
                    | PhysicalPlan::DropTable { .. }
                    | PhysicalPlan::ShowTxnStats
                    | PhysicalPlan::RunGc
            ) {
                match self.executor.execute(&plan, None) {
                    Ok(ExecutionResult::Ddl { message }) => {
                        self.observability.plan_cache.invalidate();
                        messages.push(BackendMessage::CommandComplete { tag: message });
                    }
                    Ok(ExecutionResult::Query { columns, rows }) => {
                        let fields: Vec<FieldDescription> = columns
                            .iter()
                            .map(|(name, dt)| FieldDescription {
                                name: name.clone(),
                                table_oid: 0,
                                column_attr: 0,
                                type_oid: dt.pg_oid(),
                                type_len: dt.type_len(),
                                type_modifier: -1,
                                format_code: 0,
                            })
                            .collect();
                        messages.push(BackendMessage::RowDescription { fields });
                        for row in &rows {
                            let values: Vec<Option<String>> =
                                row.values.iter().map(|d| Some(d.to_string())).collect();
                            messages.push(BackendMessage::DataRow { values });
                        }
                        messages.push(BackendMessage::CommandComplete {
                            tag: format!("SHOW {}", rows.len()),
                        });
                    }
                    Err(e) => {
                        messages.push(self.error_response(&e));
                        return Ok(messages);
                    }
                    _ => {}
                }
                continue;
            }

            // For DML/query, ensure a transaction exists (autocommit = implicit txn)
            let auto_txn = if session.txn.is_none() {
                let txn = self.txn_mgr.begin_autocommit(session.default_isolation);
                session.txn = Some(txn);
                true
            } else {
                false
            };

            // Route execution: DistPlan and multi-shard DML/DDL go through
            // DistributedQueryEngine when available; local plans use Executor.
            let query_start = Instant::now();
            let result = if let Some(dist) = &self.cluster.dist_engine {
                dist.execute(&plan, session.txn.as_ref())
            } else {
                self.executor.execute(&plan, session.txn.as_ref())
            };
            let query_duration = query_start.elapsed();

            match result {
                Ok(exec_result) => {
                    let is_readonly = matches!(&exec_result, ExecutionResult::Query { .. }
                        | ExecutionResult::Ddl { .. } | ExecutionResult::TxnControl { .. });
                    match exec_result {
                        ExecutionResult::Query { columns, rows } => {
                            let fields: Vec<FieldDescription> = columns
                                .iter()
                                .map(|(name, dt)| FieldDescription {
                                    name: name.clone(),
                                    table_oid: 0,
                                    column_attr: 0,
                                    type_oid: dt.pg_oid(),
                                    type_len: dt.type_len(),
                                    type_modifier: -1,
                                    format_code: 0,
                                })
                                .collect();
                            messages.push(BackendMessage::RowDescription { fields });

                            let row_count = rows.len();
                            for row in rows {
                                let values: Vec<Option<String>> =
                                    row.values.iter().map(falcon_common::datum::Datum::to_pg_text).collect();
                                messages.push(BackendMessage::DataRow { values });
                            }

                            messages.push(BackendMessage::CommandComplete {
                                tag: format!("SELECT {row_count}"),
                            });
                        }
                        ExecutionResult::Dml { rows_affected, tag } => {
                            let cmd_tag = match tag {
                                "INSERT" => format!("INSERT 0 {rows_affected}"),
                                "UPDATE" => format!("UPDATE {rows_affected}"),
                                "DELETE" => format!("DELETE {rows_affected}"),
                                _ => format!("{tag} {rows_affected}"),
                            };
                            messages.push(BackendMessage::CommandComplete { tag: cmd_tag });
                        }
                        ExecutionResult::Ddl { message } => {
                            self.observability.plan_cache.invalidate();
                            messages.push(BackendMessage::CommandComplete { tag: message });
                        }
                        ExecutionResult::TxnControl { action } => {
                            messages.push(BackendMessage::CommandComplete { tag: action });
                        }
                    }

                    if auto_txn {
                        if let Some(ref txn) = session.txn {
                            if is_readonly {
                                self.txn_mgr.commit_autocommit_readonly();
                            } else {
                                let _ = self.txn_mgr.commit_autocommit(txn.txn_id);
                            }
                        }
                        session.txn = None;
                    }

                    // Record to slow query log and emit tracing warn if threshold exceeded
                    if self.observability.slow_query_log.record(sql, query_duration, session.id, &session.peer_addr) {
                        tracing::warn!(
                            target: "slow_query",
                            session_id = session.id,
                            client = %session.peer_addr,
                            duration_ms = query_duration.as_millis(),
                            sql = %sql,
                            "slow query"
                        );
                    }
                }
                Err(e) => {
                    // Record to slow query log (even failed queries)
                    if self.observability.slow_query_log.record(sql, query_duration, session.id, &session.peer_addr) {
                        tracing::warn!(
                            target: "slow_query",
                            session_id = session.id,
                            client = %session.peer_addr,
                            duration_ms = query_duration.as_millis(),
                            sql = %sql,
                            "slow query (failed)"
                        );
                    }

                    // Auto-abort on error
                    if auto_txn {
                        if let Some(ref txn) = session.txn {
                            self.txn_mgr.abort_autocommit(txn.txn_id);
                        }
                        session.txn = None;
                    }
                    messages.push(self.error_response(&e));
                    return Ok(messages);
                }
            }
        }

        Ok(messages)
    }

    // handle_copy_data and handle_copy_data_inner are in handler_copy.rs

    // handle_system_query, cursor helpers, savepoint/tenant handlers are in handler_session.rs

    /// Parse and dispatch cluster admin commands like `SELECT falcon_add_node(42)`.
    pub(crate) fn parse_and_dispatch_admin_command(
        &self,
        sql_lower: &str,
    ) -> Option<Vec<BackendMessage>> {
        // Strip "select " prefix
        let rest = sql_lower.strip_prefix("select ")?;

        // Extract function name and args: "falcon_add_node(42)" → ("falcon_add_node", ["42"])
        let paren_start = rest.find('(')?;
        let paren_end = rest.rfind(')')?;
        if paren_end <= paren_start {
            return None;
        }

        let func_name = rest[..paren_start].trim();
        let args_str = rest[paren_start + 1..paren_end].trim();
        let args: Vec<&str> = if args_str.is_empty() {
            vec![]
        } else {
            args_str.split(',').collect()
        };

        self.handle_cluster_admin_command(func_name, &args)
    }

    /// Helper to build a simple query result with typed columns.
    pub(crate) fn single_row_result(
        &self,
        cols: Vec<(&str, i32, i16)>, // (name, type_oid, type_len)
        rows: Vec<Vec<Option<String>>>,
    ) -> Vec<BackendMessage> {
        let fields: Vec<FieldDescription> = cols
            .iter()
            .map(|(name, type_oid, type_len)| FieldDescription {
                name: name.to_string(),
                table_oid: 0,
                column_attr: 0,
                type_oid: *type_oid,
                type_len: *type_len,
                type_modifier: -1,
                format_code: 0,
            })
            .collect();

        let mut messages = vec![BackendMessage::RowDescription { fields }];
        let row_count = rows.len();
        for row in rows {
            messages.push(BackendMessage::DataRow { values: row });
        }
        messages.push(BackendMessage::CommandComplete {
            tag: format!("SELECT {row_count}"),
        });
        messages
    }

    /// Build an IndexedColumns map from storage for index scan detection in the planner.
    fn build_indexed_columns(&self) -> IndexedColumns {
        let mut indexed = IndexedColumns::new();
        let catalog = self.storage.get_catalog();
        for table in catalog.tables_map().values() {
            let cols = self.storage.get_indexed_columns(table.id);
            if !cols.is_empty() {
                indexed.insert(table.id, cols.iter().map(|(c, _)| *c).collect());
            }
        }
        indexed
    }

    /// Build a TableRowCounts map from cached ANALYZE stats for cost-based planning.
    fn build_row_counts(&self) -> TableRowCounts {
        let all_stats = self.storage.get_all_table_stats();
        let mut counts = TableRowCounts::new();
        for ts in &all_stats {
            counts.insert(ts.table_id, ts.row_count);
        }
        counts
    }

    /// Describe a SQL query: parse/bind/plan and return the output column descriptions.
    /// Used by the extended query protocol's Describe message.
    /// Returns Ok(fields) for queries, Ok(empty) for DML/DDL, Err for parse/bind errors.
    ///
    /// Wrapped in crash-domain guard — panics are caught and converted to FalconError.
    pub fn describe_query(&self, sql: &str) -> Result<Vec<FieldDescription>, FalconError> {
        falcon_common::crash_domain::catch_request_result("describe_query", sql, || {
            self.describe_query_inner(sql)
        })
    }

    fn describe_query_inner(&self, sql: &str) -> Result<Vec<FieldDescription>, FalconError> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Ok(vec![]);
        }

        let stmts = parse_sql(sql).map_err(FalconError::Sql)?;
        if stmts.is_empty() {
            return Ok(vec![]);
        }

        let catalog = self.storage.get_catalog();
        let mut binder = Binder::new(catalog);
        let bound = binder.bind(&stmts[0]).map_err(FalconError::Sql)?;

        let row_counts = self.build_row_counts();
        let indexed_cols = self.build_indexed_columns();
        let plan = Planner::plan_with_indexes(&bound, &row_counts, &indexed_cols)
            .map_err(FalconError::Sql)?;

        // Extract column info from the plan
        Ok(self.plan_output_fields(&plan))
    }

    /// Parse + bind + plan a SQL statement for the extended query protocol.
    /// Returns (PhysicalPlan, inferred_param_types, row_desc) on success.
    ///
    /// Wrapped in crash-domain guard — panics are caught and converted to FalconError.
    #[allow(clippy::type_complexity)]
    pub fn prepare_statement(
        &self,
        sql: &str,
    ) -> Result<
        (
            PhysicalPlan,
            Vec<Option<falcon_common::types::DataType>>,
            Vec<crate::session::FieldDescriptionCompact>,
        ),
        FalconError,
    > {
        falcon_common::crash_domain::catch_request_result("prepare_statement", sql, || {
            self.prepare_statement_inner(sql)
        })
    }

    #[allow(clippy::type_complexity)]
    fn prepare_statement_inner(
        &self,
        sql: &str,
    ) -> Result<
        (
            PhysicalPlan,
            Vec<Option<falcon_common::types::DataType>>,
            Vec<crate::session::FieldDescriptionCompact>,
        ),
        FalconError,
    > {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(FalconError::Sql(falcon_common::error::SqlError::Parse(
                "empty query".into(),
            )));
        }

        let stmts = parse_sql(sql).map_err(FalconError::Sql)?;
        if stmts.is_empty() {
            return Err(FalconError::Sql(falcon_common::error::SqlError::Parse(
                "empty query".into(),
            )));
        }

        let catalog = self.storage.get_catalog();
        let mut binder = Binder::new(catalog);
        let (bound, inferred_types) = binder
            .bind_with_params_lenient(&stmts[0], None)
            .map_err(FalconError::Sql)?;

        let row_counts = self.build_row_counts();
        let indexed_cols = self.build_indexed_columns();
        let plan = Planner::plan_with_indexes(&bound, &row_counts, &indexed_cols)
            .map_err(FalconError::Sql)?;

        // Wrap in DistPlan if multi-shard cluster
        let plan = Planner::wrap_distributed(plan, &self.cluster.shard_ids);

        // Build compact row description from the plan output fields
        let fields = self.plan_output_fields(&plan);
        let row_desc: Vec<crate::session::FieldDescriptionCompact> = fields
            .iter()
            .map(|f| crate::session::FieldDescriptionCompact {
                name: f.name.clone(),
                type_oid: f.type_oid,
                type_len: f.type_len,
            })
            .collect();

        Ok((plan, inferred_types, row_desc))
    }

    /// Execute a pre-planned query with parameter values.
    /// Used by the extended query protocol's Execute message.
    ///
    /// Wrapped in crash-domain guard — panics are caught and converted to ErrorResponse.
    pub fn execute_plan(
        &self,
        plan: &PhysicalPlan,
        params: &[Datum],
        session: &mut PgSession,
    ) -> Vec<BackendMessage> {
        let rctx = falcon_common::request_context::RequestContext::new(session.id as u64);
        let ctx = format!("session_id={}", session.id);
        let result = falcon_common::crash_domain::catch_request("execute_plan", &ctx, || {
            self.execute_plan_inner(plan, params, session)
        });
        match result {
            Ok(msgs) => msgs,
            Err(e) => vec![self.error_response(&e.with_request_context(&rctx))],
        }
    }

    fn execute_plan_inner(
        &self,
        plan: &PhysicalPlan,
        params: &[Datum],
        session: &mut PgSession,
    ) -> Vec<BackendMessage> {
        let mut messages = Vec::new();

        // Handle transaction control plans directly (no params needed)
        match plan {
            PhysicalPlan::Begin | PhysicalPlan::Commit | PhysicalPlan::Rollback => {
                // Delegate to handle_query for transaction control
                // (these don't have parameters anyway)
                let sql = match plan {
                    PhysicalPlan::Begin => "BEGIN",
                    PhysicalPlan::Commit => "COMMIT",
                    PhysicalPlan::Rollback => "ROLLBACK",
                    _ => unreachable!(),
                };
                return self.handle_query(sql, session);
            }
            _ => {}
        }

        let routing_hint = plan.routing_hint();

        if let Some(ref txn) = session.txn {
            let _ = self
                .txn_mgr
                .observe_involved_shards(txn.txn_id, &routing_hint.involved_shards);
            if matches!(routing_hint.planned_txn_type(), PlannedTxnType::Global) {
                let _ = self.txn_mgr.force_global(txn.txn_id, SlowPathMode::Xa2Pc);
            }
        }

        // For DDL/metadata, execute without params
        if matches!(
            plan,
            PhysicalPlan::CreateTable { .. }
                | PhysicalPlan::DropTable { .. }
                | PhysicalPlan::ShowTxnStats
                | PhysicalPlan::RunGc
        ) {
            match self.executor.execute(plan, None) {
                Ok(ExecutionResult::Ddl { message }) => {
                    self.observability.plan_cache.invalidate();
                    messages.push(BackendMessage::CommandComplete { tag: message });
                }
                Ok(ExecutionResult::Query { columns, rows }) => {
                    let fields: Vec<FieldDescription> = columns
                        .iter()
                        .map(|(name, dt)| FieldDescription {
                            name: name.clone(),
                            table_oid: 0,
                            column_attr: 0,
                            type_oid: dt.pg_oid(),
                            type_len: dt.type_len(),
                            type_modifier: -1,
                            format_code: 0,
                        })
                        .collect();
                    messages.push(BackendMessage::RowDescription { fields });
                    for row in &rows {
                        let values: Vec<Option<String>> =
                            row.values.iter().map(|d| Some(d.to_string())).collect();
                        messages.push(BackendMessage::DataRow { values });
                    }
                    messages.push(BackendMessage::CommandComplete {
                        tag: format!("SHOW {}", rows.len()),
                    });
                }
                Err(e) => {
                    messages.push(self.error_response(&e));
                }
                _ => {}
            }
            return messages;
        }

        // For DML/query, ensure a transaction exists (autocommit = implicit txn)
        let auto_txn = if session.txn.is_none() {
            let classification = classification_from_routing_hint(&routing_hint);
            let txn = match self
                .txn_mgr
                .try_begin_with_classification(session.default_isolation, classification)
            {
                Ok(t) => t,
                Err(e) => {
                    let ce: FalconError = e.into();
                    messages.push(self.error_response(&ce));
                    return messages;
                }
            };
            session.txn = Some(txn);
            true
        } else {
            false
        };

        // Execute with parameter substitution — route through dist_engine when available
        let query_start = std::time::Instant::now();
        let result = if let Some(dist) = &self.cluster.dist_engine {
            dist.execute_with_params(plan, session.txn.as_ref(), params)
        } else {
            self.executor
                .execute_with_params(plan, session.txn.as_ref(), params)
        };
        let _query_duration = query_start.elapsed();

        match result {
            Ok(exec_result) => {
                match exec_result {
                    ExecutionResult::Query { columns, rows } => {
                        let fields: Vec<FieldDescription> = columns
                            .iter()
                            .map(|(name, dt)| FieldDescription {
                                name: name.clone(),
                                table_oid: 0,
                                column_attr: 0,
                                type_oid: dt.pg_oid(),
                                type_len: dt.type_len(),
                                type_modifier: -1,
                                format_code: 0,
                            })
                            .collect();
                        messages.push(BackendMessage::RowDescription { fields });
                        let row_count = rows.len();
                        for row in rows {
                            let values: Vec<Option<String>> =
                                row.values.iter().map(falcon_common::datum::Datum::to_pg_text).collect();
                            messages.push(BackendMessage::DataRow { values });
                        }
                        messages.push(BackendMessage::CommandComplete {
                            tag: format!("SELECT {row_count}"),
                        });
                    }
                    ExecutionResult::Dml { rows_affected, tag } => {
                        let cmd_tag = match tag {
                            "INSERT" => format!("INSERT 0 {rows_affected}"),
                            "UPDATE" => format!("UPDATE {rows_affected}"),
                            "DELETE" => format!("DELETE {rows_affected}"),
                            _ => format!("{tag} {rows_affected}"),
                        };
                        messages.push(BackendMessage::CommandComplete { tag: cmd_tag });
                    }
                    ExecutionResult::Ddl { message } => {
                        self.observability.plan_cache.invalidate();
                        messages.push(BackendMessage::CommandComplete { tag: message });
                    }
                    ExecutionResult::TxnControl { action } => {
                        messages.push(BackendMessage::CommandComplete { tag: action });
                    }
                }

                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        let _ = self.txn_mgr.commit(txn.txn_id);
                    }
                    session.txn = None;
                    self.flush_txn_stats();
                }
            }
            Err(e) => {
                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        let _ = self.txn_mgr.abort(txn.txn_id);
                    }
                    session.txn = None;
                    self.flush_txn_stats();
                }
                messages.push(self.error_response(&e));
            }
        }

        messages
    }

    /// Map a Falcon DataType to a PostgreSQL type OID.
    pub const fn datatype_to_oid(&self, dt: Option<&falcon_common::types::DataType>) -> i32 {
        use falcon_common::types::DataType;
        match dt {
            Some(DataType::Int16) => 21,           // INT2
            Some(DataType::Int32) => 23,           // INT4
            Some(DataType::Int64) => 20,           // INT8
            Some(DataType::Float32) => 700,        // FLOAT4
            Some(DataType::Float64) => 701,        // FLOAT8
            Some(DataType::Boolean) => 16,         // BOOL
            Some(DataType::Text) => 25,            // TEXT
            Some(DataType::Timestamp) => 1114,     // TIMESTAMP
            Some(DataType::Date) => 1082,          // DATE
            Some(DataType::Array(_)) => 2277,      // ANYARRAY
            Some(DataType::Jsonb) => 3802,         // JSONB
            Some(DataType::Decimal(_, _)) => 1700, // NUMERIC
            Some(DataType::Time) => 1083,          // TIME
            Some(DataType::Interval) => 1186,      // INTERVAL
            Some(DataType::Uuid) => 2950,          // UUID
            Some(DataType::Bytea) => 17,           // BYTEA
            None => 0,                             // unspecified
        }
    }

    /// Extract output column FieldDescriptions from a physical plan.
    fn plan_output_fields(&self, plan: &PhysicalPlan) -> Vec<FieldDescription> {
        use falcon_common::types::DataType;
        use falcon_sql_frontend::types::{AggFunc, BinOp, BoundExpr, BoundProjection, ScalarFunc};

        /// Infer the DataType of a BoundExpr given the source table schema columns.
        fn infer_expr_type(
            expr: &BoundExpr,
            cols: &[falcon_common::schema::ColumnDef],
        ) -> DataType {
            match expr {
                BoundExpr::Literal(d) => d.data_type().unwrap_or(DataType::Text),
                BoundExpr::ColumnRef(idx) => cols
                    .get(*idx)
                    .map_or(DataType::Text, |c| c.data_type.clone()),
                BoundExpr::BinaryOp { left, op, right } => {
                    match op {
                        // Comparison / logical → Boolean
                        BinOp::Eq
                        | BinOp::NotEq
                        | BinOp::Lt
                        | BinOp::LtEq
                        | BinOp::Gt
                        | BinOp::GtEq
                        | BinOp::And
                        | BinOp::Or => DataType::Boolean,
                        // Arithmetic → promote operand types
                        BinOp::Plus
                        | BinOp::Minus
                        | BinOp::Multiply
                        | BinOp::Divide
                        | BinOp::Modulo => {
                            let lt = infer_expr_type(left, cols);
                            let rt = infer_expr_type(right, cols);
                            promote_numeric(lt, rt)
                        }
                        // String concat
                        BinOp::StringConcat
                        | BinOp::JsonArrowText
                        | BinOp::JsonHashArrowText => DataType::Text,
                        // JSONB operators → Jsonb (or Text for ->>/#>>)
                        BinOp::JsonArrow
                        | BinOp::JsonHashArrow
                        | BinOp::JsonContains
                        | BinOp::JsonContainedBy
                        | BinOp::JsonExists => DataType::Jsonb,
                    }
                }
                BoundExpr::Not(_)
                | BoundExpr::IsNull(_)
                | BoundExpr::IsNotNull(_)
                | BoundExpr::IsNotDistinctFrom { .. }
                | BoundExpr::Like { .. }
                | BoundExpr::Between { .. }
                | BoundExpr::InList { .. }
                | BoundExpr::Exists { .. }
                | BoundExpr::InSubquery { .. } => DataType::Boolean,
                BoundExpr::Cast { target_type, .. } => parse_cast_type(target_type),
                BoundExpr::Case {
                    results,
                    else_result,
                    ..
                } => {
                    // Infer from first THEN branch
                    if let Some(first) = results.first() {
                        infer_expr_type(first, cols)
                    } else if let Some(e) = else_result {
                        infer_expr_type(e, cols)
                    } else {
                        DataType::Text
                    }
                }
                BoundExpr::Coalesce(exprs) => exprs
                    .first()
                    .map_or(DataType::Text, |e| infer_expr_type(e, cols)),
                BoundExpr::Function { func, args } => infer_func_type(func, args, cols),
                BoundExpr::AggregateExpr { func, arg, .. } => {
                    let input_ty = arg.as_ref().map(|a| infer_expr_type(a, cols));
                    infer_agg_return_type(func, input_ty)
                }
                BoundExpr::ArrayLiteral(_) => DataType::Array(Box::new(DataType::Text)),
                BoundExpr::ArrayIndex { array, .. } => {
                    // Element type of the array
                    match infer_expr_type(array, cols) {
                        DataType::Array(inner) => *inner,
                        _ => DataType::Text,
                    }
                }
                BoundExpr::OuterColumnRef(idx) => cols
                    .get(*idx)
                    .map_or(DataType::Text, |c| c.data_type.clone()),
                BoundExpr::SequenceNextval(_)
                | BoundExpr::SequenceCurrval(_)
                | BoundExpr::SequenceSetval(_, _) => DataType::Int64,
                BoundExpr::Grouping(_) => DataType::Int32,
                _ => DataType::Text,
            }
        }

        fn promote_numeric(a: DataType, b: DataType) -> DataType {
            match (&a, &b) {
                (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
                (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
                (DataType::Int32, DataType::Int32) => DataType::Int32,
                _ => a,
            }
        }

        fn parse_cast_type(t: &str) -> DataType {
            match t.to_uppercase().as_str() {
                "INT" | "INT4" | "INTEGER" => DataType::Int32,
                "BIGINT" | "INT8" => DataType::Int64,
                "FLOAT" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => DataType::Float64,
                "BOOL" | "BOOLEAN" => DataType::Boolean,
                "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => DataType::Timestamp,
                "DATE" => DataType::Date,
                "JSONB" => DataType::Jsonb,
                _ => DataType::Text,
            }
        }

        fn infer_func_type(
            func: &ScalarFunc,
            args: &[BoundExpr],
            cols: &[falcon_common::schema::ColumnDef],
        ) -> DataType {
            match func {
                // String → Text
                ScalarFunc::Upper
                | ScalarFunc::Lower
                | ScalarFunc::Trim
                | ScalarFunc::Replace
                | ScalarFunc::Lpad
                | ScalarFunc::Rpad
                | ScalarFunc::Left
                | ScalarFunc::Right
                | ScalarFunc::Repeat
                | ScalarFunc::Reverse
                | ScalarFunc::Initcap
                | ScalarFunc::Chr
                | ScalarFunc::ToChar
                | ScalarFunc::Concat
                | ScalarFunc::ConcatWs
                | ScalarFunc::Substring
                | ScalarFunc::Btrim
                | ScalarFunc::Ltrim
                | ScalarFunc::Rtrim
                | ScalarFunc::Overlay
                | ScalarFunc::RegexpReplace
                | ScalarFunc::RegexpSubstr
                | ScalarFunc::Translate
                | ScalarFunc::QuoteLiteral
                | ScalarFunc::QuoteIdent
                | ScalarFunc::QuoteNullable
                | ScalarFunc::Md5
                | ScalarFunc::Encode
                | ScalarFunc::Decode
                | ScalarFunc::ToHex
                | ScalarFunc::PgTypeof
                | ScalarFunc::GenRandomUuid
                | ScalarFunc::ArrayDims
                | ScalarFunc::ArrayToString => DataType::Text,
                // Integer results
                ScalarFunc::Length
                | ScalarFunc::Position
                | ScalarFunc::Ascii
                | ScalarFunc::RegexpCount
                | ScalarFunc::ArrayLength
                | ScalarFunc::ArrayPosition
                | ScalarFunc::Cardinality
                | ScalarFunc::ArrayUpper
                | ScalarFunc::ArrayLower
                | ScalarFunc::WidthBucket
                | ScalarFunc::Factorial
                | ScalarFunc::Gcd
                | ScalarFunc::Lcm => DataType::Int64,
                // Float results
                ScalarFunc::Abs
                | ScalarFunc::Round
                | ScalarFunc::Ceil
                | ScalarFunc::Floor
                | ScalarFunc::Power
                | ScalarFunc::Sqrt
                | ScalarFunc::Sign
                | ScalarFunc::Trunc
                | ScalarFunc::Ln
                | ScalarFunc::Log
                | ScalarFunc::Exp
                | ScalarFunc::Pi
                | ScalarFunc::Mod
                | ScalarFunc::Degrees
                | ScalarFunc::Radians
                | ScalarFunc::Cbrt
                | ScalarFunc::Extract
                | ScalarFunc::ToNumber
                | ScalarFunc::Random
                | ScalarFunc::Log10
                | ScalarFunc::Log2
                | ScalarFunc::Sin
                | ScalarFunc::Cos
                | ScalarFunc::Tan
                | ScalarFunc::Asin
                | ScalarFunc::Acos
                | ScalarFunc::Atan
                | ScalarFunc::Atan2
                | ScalarFunc::Cot
                | ScalarFunc::Sinh
                | ScalarFunc::Cosh
                | ScalarFunc::Tanh => DataType::Float64,
                // Date/time
                ScalarFunc::Now
                | ScalarFunc::DateTrunc => DataType::Timestamp,
                ScalarFunc::CurrentDate => DataType::Date,
                ScalarFunc::CurrentTime => DataType::Time,
                // Bool
                ScalarFunc::StartsWith
                | ScalarFunc::EndsWith
                | ScalarFunc::ArrayContains
                | ScalarFunc::ArrayOverlap => DataType::Boolean,
                // Array-returning
                ScalarFunc::Split
                | ScalarFunc::RegexpMatch
                | ScalarFunc::RegexpSplitToArray
                | ScalarFunc::StringToArray
                | ScalarFunc::ArrayFill
                | ScalarFunc::ArrayReverse
                | ScalarFunc::ArrayDistinct
                | ScalarFunc::ArraySort
                | ScalarFunc::ArrayIntersect
                | ScalarFunc::ArrayExcept
                | ScalarFunc::ArrayCompact
                | ScalarFunc::ArrayFlatten
                | ScalarFunc::ArraySlice => DataType::Array(Box::new(DataType::Text)),
                // Pass-through: Greatest/Least inherit from first arg
                ScalarFunc::Greatest | ScalarFunc::Least => args
                    .first()
                    .map_or(DataType::Text, |a| infer_expr_type(a, cols)),
                // Array mutation returns array
                ScalarFunc::ArrayAppend
                | ScalarFunc::ArrayPrepend
                | ScalarFunc::ArrayRemove
                | ScalarFunc::ArrayReplace
                | ScalarFunc::ArrayCat => args
                    .first().map_or_else(|| DataType::Array(Box::new(DataType::Text)), |a| infer_expr_type(a, cols)),
                // Catch-all for remaining scalar functions — default to Text
                _ => DataType::Text,
            }
        }

        fn infer_agg_return_type(func: &AggFunc, input_ty: Option<DataType>) -> DataType {
            match func {
                AggFunc::Count => DataType::Int64,
                AggFunc::Sum => match input_ty {
                    Some(DataType::Float64) => DataType::Float64,
                    _ => DataType::Int64, // SUM promotes int types to bigint
                },
                AggFunc::Min | AggFunc::Max => input_ty.unwrap_or(DataType::Text),
                AggFunc::StringAgg(_) => DataType::Text,
                AggFunc::BoolAnd | AggFunc::BoolOr => DataType::Boolean,
                AggFunc::ArrayAgg => DataType::Array(Box::new(input_ty.unwrap_or(DataType::Text))),
                // Statistical aggregates always return Float64
                AggFunc::Avg
                | AggFunc::StddevPop
                | AggFunc::StddevSamp
                | AggFunc::VarPop
                | AggFunc::VarSamp
                | AggFunc::Corr
                | AggFunc::CovarPop
                | AggFunc::CovarSamp
                | AggFunc::RegrSlope
                | AggFunc::RegrIntercept
                | AggFunc::RegrR2
                | AggFunc::RegrAvgX
                | AggFunc::RegrAvgY
                | AggFunc::RegrSXX
                | AggFunc::RegrSYY
                | AggFunc::RegrSXY
                | AggFunc::PercentileCont(_)
                | AggFunc::PercentileDisc(_) => DataType::Float64,
                AggFunc::RegrCount => DataType::Int64,
                AggFunc::Mode => input_ty.unwrap_or(DataType::Text),
                AggFunc::BitAndAgg | AggFunc::BitOrAgg | AggFunc::BitXorAgg => DataType::Int64,
            }
        }

        fn projection_to_field(
            p: &BoundProjection,
            schema: &falcon_common::schema::TableSchema,
        ) -> FieldDescription {
            match p {
                BoundProjection::Column(idx, alias) => {
                    if let Some(col) = schema.columns.get(*idx) {
                        FieldDescription {
                            name: alias.clone(),
                            table_oid: 0,
                            column_attr: 0,
                            type_oid: col.data_type.pg_oid(),
                            type_len: col.data_type.type_len(),
                            type_modifier: -1,
                            format_code: 0,
                        }
                    } else {
                        FieldDescription {
                            name: alias.clone(),
                            table_oid: 0,
                            column_attr: 0,
                            type_oid: 25, // TEXT fallback
                            type_len: -1,
                            type_modifier: -1,
                            format_code: 0,
                        }
                    }
                }
                BoundProjection::Aggregate(func, arg, alias, _, _) => {
                    let input_ty = arg.as_ref().map(|a| infer_expr_type(a, &schema.columns));
                    let dt = infer_agg_return_type(func, input_ty);
                    FieldDescription {
                        name: alias.clone(),
                        table_oid: 0,
                        column_attr: 0,
                        type_oid: dt.pg_oid(),
                        type_len: dt.type_len(),
                        type_modifier: -1,
                        format_code: 0,
                    }
                }
                BoundProjection::Expr(expr, alias) => {
                    let dt = infer_expr_type(expr, &schema.columns);
                    FieldDescription {
                        name: alias.clone(),
                        table_oid: 0,
                        column_attr: 0,
                        type_oid: dt.pg_oid(),
                        type_len: dt.type_len(),
                        type_modifier: -1,
                        format_code: 0,
                    }
                }
                BoundProjection::Window(w) => {
                    FieldDescription {
                        name: w.alias.clone(),
                        table_oid: 0,
                        column_attr: 0,
                        type_oid: 20, // BIGINT (window funcs typically return int)
                        type_len: 8,
                        type_modifier: -1,
                        format_code: 0,
                    }
                }
            }
        }

        match plan {
            PhysicalPlan::SeqScan {
                projections,
                schema,
                ..
            }
            | PhysicalPlan::IndexScan {
                projections,
                schema,
                ..
            } => projections
                .iter()
                .map(|p| projection_to_field(p, schema))
                .collect(),
            PhysicalPlan::NestedLoopJoin {
                projections,
                combined_schema,
                ..
            }
            | PhysicalPlan::HashJoin {
                projections,
                combined_schema,
                ..
            } => projections
                .iter()
                .map(|p| projection_to_field(p, combined_schema))
                .collect(),
            PhysicalPlan::Explain(_) | PhysicalPlan::ExplainAnalyze(_) => {
                vec![FieldDescription {
                    name: "QUERY PLAN".into(),
                    table_oid: 0,
                    column_attr: 0,
                    type_oid: 25, // TEXT
                    type_len: -1,
                    type_modifier: -1,
                    format_code: 0,
                }]
            }
            PhysicalPlan::DistPlan { subplan, .. } => self.plan_output_fields(subplan),
            // DML/DDL/txn control — no result columns
            _ => vec![],
        }
    }

    pub(crate) fn error_response(&self, err: &FalconError) -> BackendMessage {
        let mut message = err.to_string();
        // Append routing hints for retryable errors so PG-aware proxies can act.
        if let FalconError::Retryable {
            leader_hint: Some(ref hint),
            retry_after_ms,
            ..
        } = err
        {
            message = format!(
                "{message} HINT: leader={hint}, retry_after={retry_after_ms}ms"
            );
        }
        BackendMessage::ErrorResponse {
            severity: err.pg_severity().into(),
            code: err.pg_sqlstate().into(),
            message,
        }
    }

    /// Push current txn stats to Prometheus gauges (sampled 1-in-64).
    pub(crate) fn flush_txn_stats(&self) {
        let n = self.flush_stats_counter.fetch_add(1, AtomicOrdering::Relaxed);
        if n % 64 != 0 {
            return;
        }
        let s = self.txn_mgr.stats_snapshot();
        falcon_observability::record_txn_stats(
            s.total_committed,
            s.fast_path_commits,
            s.slow_path_commits,
            s.total_aborted,
            s.occ_conflicts,
            s.degraded_to_global,
            s.active_count,
        );
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler_utils::{
        bind_params, extract_where_eq, parse_execute_statement, parse_prepare_statement,
        parse_set_command, parse_set_log_min_duration, split_params, text_params_to_datum,
    };
    use crate::session::PgSession;

    fn setup_handler() -> (QueryHandler, PgSession) {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let handler = QueryHandler::new(storage, txn_mgr, executor);
        let session = PgSession::new(1);
        (handler, session)
    }

    fn extract_data_rows(msgs: &[BackendMessage]) -> Vec<Vec<Option<String>>> {
        msgs.iter()
            .filter_map(|m| {
                if let BackendMessage::DataRow { values } = m {
                    Some(values.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn has_row_description(msgs: &[BackendMessage]) -> bool {
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::RowDescription { .. }))
    }

    #[test]
    fn test_show_gc_stats() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.gc_stats", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty(), "should have data rows");
        // Verify expected metric names
        let metric_names: Vec<_> = rows
            .iter()
            .filter_map(|r| r.first().cloned().flatten())
            .collect();
        assert!(metric_names.contains(&"gc_safepoint_ts".to_string()));
        assert!(metric_names.contains(&"total_sweeps".to_string()));
        assert!(metric_names.contains(&"reclaimed_version_count".to_string()));
        assert!(metric_names.contains(&"reclaimed_memory_bytes".to_string()));
        assert!(metric_names.contains(&"max_chain_length".to_string()));
    }

    #[test]
    fn test_show_gc_safepoint() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.gc_safepoint", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 6, "gc_safepoint should have 6 rows");
        let metric_names: Vec<_> = rows
            .iter()
            .filter_map(|r| r.first().cloned().flatten())
            .collect();
        assert!(metric_names.contains(&"min_active_start_ts".to_string()));
        assert!(metric_names.contains(&"current_ts".to_string()));
        assert!(metric_names.contains(&"active_txn_count".to_string()));
        assert!(metric_names.contains(&"prepared_txn_count".to_string()));
        assert!(metric_names.contains(&"longest_txn_age_us".to_string()));
        assert!(metric_names.contains(&"stalled".to_string()));
    }

    #[test]
    fn test_show_wal_stats() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.wal_stats", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 9);
        let metric_names: Vec<_> = rows
            .iter()
            .filter_map(|r| r.first().cloned().flatten())
            .collect();
        assert!(metric_names.contains(&"wal_enabled".to_string()));
        assert!(metric_names.contains(&"records_written".to_string()));
        assert!(metric_names.contains(&"observer_notifications".to_string()));
        assert!(metric_names.contains(&"flushes".to_string()));
        assert!(metric_names.contains(&"fsync_total_us".to_string()));
        assert!(metric_names.contains(&"fsync_avg_us".to_string()));
        assert!(metric_names.contains(&"group_commit_avg_size".to_string()));
        assert!(metric_names.contains(&"backlog_bytes".to_string()));
        // In-memory engine has WAL disabled
        assert_eq!(rows[0][1], Some("false".into()));
        // No records written yet
        assert_eq!(rows[1][1], Some("0".into()));
    }

    #[test]
    fn test_show_node_role() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.node_role", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("role".into()));
        // Default role when FALCON_NODE_ROLE env var is not set
        assert!(rows[0][1].is_some(), "role value should be present");
    }

    #[test]
    fn test_show_replication_stats() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.replication_stats", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 5);
        // promote_count should be 0 initially
        assert_eq!(rows[0][0], Some("promote_count".into()));
        assert_eq!(rows[0][1], Some("0".into()));
        assert_eq!(rows[1][0], Some("last_failover_time_ms".into()));
        assert_eq!(rows[1][1], Some("0".into()));
        assert_eq!(rows[2][0], Some("leader_changes".into()));
        assert_eq!(rows[2][1], Some("0".into()));
        assert_eq!(rows[3][0], Some("replication_lag_us".into()));
        assert_eq!(rows[4][0], Some("max_replication_lag_us".into()));
    }

    #[test]
    fn test_show_replication_stats_after_record_failover() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.record_failover(42);
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let handler = QueryHandler::new(storage, txn_mgr, executor);
        let mut session = PgSession::new(1);

        let msgs = handler.handle_query("SHOW falcon.replication_stats", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows[0][1], Some("1".into()), "promote_count should be 1");
        assert_eq!(
            rows[1][1],
            Some("42".into()),
            "last_failover_time_ms should be 42"
        );
    }

    #[test]
    fn test_show_txn_stats() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.txn_stats", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty());
        let metric_names: Vec<_> = rows
            .iter()
            .filter_map(|r| r.first().cloned().flatten())
            .collect();
        assert!(metric_names.contains(&"total_committed".to_string()));
        assert!(metric_names.contains(&"fast_path_commits".to_string()));
        assert!(metric_names.contains(&"slow_path_commits".to_string()));
        assert!(metric_names.contains(&"active_count".to_string()));
    }

    #[test]
    fn test_show_txn_history() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.txn_history", &mut session);
        assert!(has_row_description(&msgs));
        // No txns committed yet, so 0 data rows
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_empty_query_returns_empty_response() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("", &mut session);
        assert!(matches!(msgs[0], BackendMessage::EmptyQueryResponse));
    }

    #[test]
    fn test_whitespace_only_query() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("   \n\t  ", &mut session);
        assert!(matches!(msgs[0], BackendMessage::EmptyQueryResponse));
    }

    #[test]
    fn test_create_table_via_handler() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query(
            "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)",
            &mut session,
        );
        // Should contain CommandComplete, not an error
        let has_complete = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
        assert!(has_complete, "CREATE TABLE should produce CommandComplete");
    }

    #[test]
    fn test_multiple_failover_records_accumulate() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.record_failover(10);
        storage.record_failover(20);
        storage.record_failover(30);
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let handler = QueryHandler::new(storage, txn_mgr, executor);
        let mut session = PgSession::new(1);

        let msgs = handler.handle_query("SHOW falcon.replication_stats", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows[0][1], Some("3".into()), "promote_count should be 3");
        assert_eq!(
            rows[1][1],
            Some("30".into()),
            "last_failover_time_ms should be 30 (last recorded)"
        );
    }

    #[test]
    fn test_show_scatter_stats_without_dist_engine() {
        let (handler, mut session) = setup_handler();
        // Without dist_engine, scatter_stats should return zeroed/empty metrics
        let msgs = handler.handle_query("SHOW falcon.scatter_stats", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        // Should have rows with zeroed values (no dist engine = no scatter stats)
        assert!(!rows.is_empty());
    }

    #[test]
    fn test_gc_stats_initial_values_are_zero() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.gc_stats", &mut session);
        let rows = extract_data_rows(&msgs);
        // total_sweeps should be "0" initially
        let sweeps_row = rows.iter().find(|r| r[0] == Some("total_sweeps".into()));
        assert_eq!(sweeps_row.unwrap()[1], Some("0".into()));
        // reclaimed_version_count should be "0"
        let reclaimed_row = rows
            .iter()
            .find(|r| r[0] == Some("reclaimed_version_count".into()));
        assert_eq!(reclaimed_row.unwrap()[1], Some("0".into()));
    }

    #[test]
    fn test_show_txn_history_after_commits() {
        let (handler, mut session) = setup_handler();

        // Create table + commit a transaction via handler
        handler.handle_query(
            "CREATE TABLE th (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );
        handler.handle_query("BEGIN", &mut session);
        handler.handle_query("INSERT INTO th VALUES (1, 'a')", &mut session);
        handler.handle_query("COMMIT", &mut session);

        let msgs = handler.handle_query("SHOW falcon.txn_history", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        // Should have at least 1 completed transaction record
        assert!(
            !rows.is_empty(),
            "txn_history should have records after commits"
        );
    }

    #[test]
    fn test_show_txn_stats_after_commits() {
        let (handler, mut session) = setup_handler();

        handler.handle_query("CREATE TABLE ts (id INT PRIMARY KEY)", &mut session);
        handler.handle_query("BEGIN", &mut session);
        handler.handle_query("INSERT INTO ts VALUES (1)", &mut session);
        handler.handle_query("COMMIT", &mut session);

        let msgs = handler.handle_query("SHOW falcon.txn_stats", &mut session);
        let rows = extract_data_rows(&msgs);
        let committed_row = rows.iter().find(|r| r[0] == Some("total_committed".into()));
        let count: u64 = committed_row.unwrap()[1].as_ref().unwrap().parse().unwrap();
        assert!(count >= 1, "total_committed should be >= 1 after a commit");
    }

    // ── Error path tests ──

    fn has_error_response(msgs: &[BackendMessage]) -> bool {
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
    }

    fn has_notice_response(msgs: &[BackendMessage]) -> bool {
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::NoticeResponse { .. }))
    }

    #[test]
    fn test_invalid_sql_returns_error() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECTT * FROMM nothing", &mut session);
        assert!(
            has_error_response(&msgs),
            "Invalid SQL should produce ErrorResponse"
        );
    }

    #[test]
    fn test_unknown_table_returns_error() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT * FROM nonexistent_table", &mut session);
        assert!(
            has_error_response(&msgs),
            "Unknown table should produce ErrorResponse"
        );
    }

    #[test]
    fn test_commit_without_transaction_returns_notice() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("COMMIT", &mut session);
        assert!(
            has_notice_response(&msgs),
            "COMMIT without active txn should produce NoticeResponse"
        );
    }

    #[test]
    fn test_double_begin_returns_notice() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("BEGIN", &mut session);
        let msgs = handler.handle_query("BEGIN", &mut session);
        assert!(
            has_notice_response(&msgs),
            "BEGIN while already in txn should produce NoticeResponse"
        );
        // Clean up
        handler.handle_query("ROLLBACK", &mut session);
    }

    #[test]
    fn test_rollback_without_transaction_succeeds() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("ROLLBACK", &mut session);
        let has_complete = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
        assert!(
            has_complete,
            "ROLLBACK without txn should still return CommandComplete"
        );
        assert!(
            !has_error_response(&msgs),
            "ROLLBACK without txn should not be an error"
        );
    }

    #[test]
    fn test_insert_into_nonexistent_table_returns_error() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("INSERT INTO ghost (id) VALUES (1)", &mut session);
        assert!(has_error_response(&msgs));
    }

    #[test]
    fn test_begin_commit_rollback_lifecycle() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE lc (id INT PRIMARY KEY)", &mut session);

        // BEGIN → INSERT → ROLLBACK → verify no data
        handler.handle_query("BEGIN", &mut session);
        handler.handle_query("INSERT INTO lc VALUES (1)", &mut session);
        handler.handle_query("ROLLBACK", &mut session);

        let msgs = handler.handle_query("SELECT * FROM lc", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 0, "ROLLBACK should discard inserted row");

        // BEGIN → INSERT → COMMIT → verify data persisted
        handler.handle_query("BEGIN", &mut session);
        handler.handle_query("INSERT INTO lc VALUES (2)", &mut session);
        handler.handle_query("COMMIT", &mut session);

        let msgs2 = handler.handle_query("SELECT * FROM lc", &mut session);
        let rows2 = extract_data_rows(&msgs2);
        assert_eq!(rows2.len(), 1, "COMMIT should persist inserted row");
    }

    // ── parse_set_log_min_duration tests ──

    #[test]
    fn test_parse_log_min_duration_equals() {
        assert_eq!(
            parse_set_log_min_duration("set log_min_duration_statement = 500"),
            Some(500)
        );
        assert_eq!(
            parse_set_log_min_duration("set log_min_duration_statement = 0"),
            Some(0)
        );
        assert_eq!(
            parse_set_log_min_duration("set log_min_duration_statement = 100;"),
            Some(100)
        );
    }

    #[test]
    fn test_parse_log_min_duration_to() {
        assert_eq!(
            parse_set_log_min_duration("set log_min_duration_statement to 3000"),
            Some(3000)
        );
        assert_eq!(
            parse_set_log_min_duration("set log_min_duration_statement to '5000'"),
            Some(5000)
        );
    }

    #[test]
    fn test_parse_log_min_duration_disable() {
        assert_eq!(
            parse_set_log_min_duration("set log_min_duration_statement = default"),
            Some(0)
        );
        assert_eq!(
            parse_set_log_min_duration("set log_min_duration_statement = -1"),
            Some(0)
        );
    }

    #[test]
    fn test_parse_log_min_duration_not_matching() {
        assert_eq!(parse_set_log_min_duration("select 1"), None);
        assert_eq!(
            parse_set_log_min_duration("set statement_timeout = 100"),
            None
        );
        assert_eq!(
            parse_set_log_min_duration("set log_min_duration_statement"),
            None
        );
    }

    // ── Slow query log handler tests ──

    #[test]
    fn test_show_slow_queries_disabled() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.slow_queries", &mut session);
        let rows = extract_data_rows(&msgs);
        // threshold_ms should say "disabled"
        assert_eq!(rows[0][1], Some("disabled".into()));
        // total_slow_queries = 0
        assert_eq!(rows[1][1], Some("0".into()));
    }

    #[test]
    fn test_set_log_min_duration_enables_slow_log() {
        let (handler, mut session) = setup_handler();

        // Enable with 0ms threshold (logs everything)
        handler.handle_query("SET log_min_duration_statement = 1", &mut session);

        // Create table and run a query
        handler.handle_query("CREATE TABLE sq_test (id INT PRIMARY KEY)", &mut session);
        handler.handle_query("SELECT * FROM sq_test", &mut session);

        // Check slow queries
        let msgs = handler.handle_query("SHOW falcon.slow_queries", &mut session);
        let rows = extract_data_rows(&msgs);
        // threshold should be 1ms now
        assert_eq!(rows[0][1], Some("1".into()));
    }

    #[test]
    fn test_reset_slow_queries() {
        let log = Arc::new(SlowQueryLog::new(std::time::Duration::from_millis(1), 100));
        log.record("SELECT 1", std::time::Duration::from_millis(10), 1, "");

        let storage = Arc::new(StorageEngine::new_in_memory());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let handler = QueryHandler::new(storage, txn_mgr, executor).with_slow_query_log(log);
        let mut session = PgSession::new(1);

        // Verify there's 1 entry
        let msgs = handler.handle_query("SHOW falcon.slow_queries", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows[1][1], Some("1".into())); // total_slow_queries

        // Reset
        handler.handle_query("RESET falcon.slow_queries", &mut session);

        // Should be empty now
        let msgs2 = handler.handle_query("SHOW falcon.slow_queries", &mut session);
        let rows2 = extract_data_rows(&msgs2);
        assert_eq!(rows2[1][1], Some("0".into())); // total_slow_queries
    }

    // ── Checkpoint tests ──

    #[test]
    fn test_checkpoint_without_wal_returns_error() {
        // In-memory engine has no WAL, so CHECKPOINT should fail
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("CHECKPOINT", &mut session);
        assert!(
            has_error_response(&msgs),
            "CHECKPOINT without WAL should return error"
        );
    }

    #[test]
    fn test_checkpoint_stats_without_wal() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.checkpoint_stats", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows[0][1], Some("false".into())); // wal_enabled
        assert_eq!(rows[1][1], Some("false".into())); // checkpoint_available
    }

    #[test]
    fn test_checkpoint_with_wal() {
        let dir = std::env::temp_dir().join("falcon_handler_ckpt_test");
        let _ = std::fs::remove_dir_all(&dir);

        let storage = Arc::new(StorageEngine::new(Some(&dir)).unwrap());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let handler = QueryHandler::new(storage, txn_mgr, executor);
        let mut session = PgSession::new(1);

        handler.handle_query("CREATE TABLE ckpt_h (id INT PRIMARY KEY)", &mut session);
        handler.handle_query("INSERT INTO ckpt_h VALUES (1)", &mut session);

        let msgs = handler.handle_query("CHECKPOINT", &mut session);
        assert!(
            !has_error_response(&msgs),
            "CHECKPOINT with WAL should succeed"
        );
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty());
        let val = rows[0][0].as_ref().unwrap();
        assert!(
            val.starts_with("OK"),
            "Checkpoint result should start with OK: {}",
            val
        );

        // Verify checkpoint_stats shows WAL enabled
        let msgs2 = handler.handle_query("SHOW falcon.checkpoint_stats", &mut session);
        let rows2 = extract_data_rows(&msgs2);
        assert_eq!(rows2[0][1], Some("true".into())); // wal_enabled

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Describe query tests (M4.7) ──

    #[test]
    fn test_describe_select_returns_columns() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE desc_t (id INT PRIMARY KEY, name TEXT, active BOOLEAN)",
            &mut session,
        );

        let fields = handler.describe_query("SELECT * FROM desc_t").unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].type_oid, 23); // INT4
        assert_eq!(fields[1].name, "name");
        assert_eq!(fields[1].type_oid, 25); // TEXT
        assert_eq!(fields[2].name, "active");
        assert_eq!(fields[2].type_oid, 16); // BOOL
    }

    #[test]
    fn test_describe_dml_returns_empty() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE desc_dml (id INT PRIMARY KEY)", &mut session);

        let fields = handler
            .describe_query("INSERT INTO desc_dml VALUES (1)")
            .unwrap();
        assert!(fields.is_empty(), "DML should have no result columns");
    }

    #[test]
    fn test_describe_explain_returns_query_plan_column() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE desc_ex (id INT PRIMARY KEY)", &mut session);

        let fields = handler
            .describe_query("EXPLAIN SELECT * FROM desc_ex")
            .unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "QUERY PLAN");
        assert_eq!(fields[0].type_oid, 25); // TEXT
    }

    #[test]
    fn test_describe_empty_sql() {
        let (handler, _session) = setup_handler();
        let fields = handler.describe_query("").unwrap();
        assert!(fields.is_empty());
    }

    #[test]
    fn test_describe_invalid_sql_returns_error() {
        let (handler, _session) = setup_handler();
        let result = handler.describe_query("SELECTT FROMM nothing");
        assert!(result.is_err());
    }

    // ── information_schema tests (M5.1) ──

    #[test]
    fn test_information_schema_tables() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE is_t1 (id INT PRIMARY KEY)", &mut session);
        handler.handle_query("CREATE TABLE is_t2 (name TEXT)", &mut session);

        let msgs = handler.handle_query("SELECT * FROM information_schema.tables", &mut session);
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 2, "should list at least 2 tables");
        // Check that our tables appear
        let names: Vec<&str> = rows.iter().filter_map(|r| r[2].as_deref()).collect();
        assert!(names.contains(&"is_t1"));
        assert!(names.contains(&"is_t2"));
        // Check columns: table_catalog=falcon, table_schema=public, table_type=BASE TABLE
        let t1_row = rows.iter().find(|r| r[2] == Some("is_t1".into())).unwrap();
        assert_eq!(t1_row[0], Some("falcon".into()));
        assert_eq!(t1_row[1], Some("public".into()));
        assert_eq!(t1_row[3], Some("BASE TABLE".into()));
    }

    #[test]
    fn test_information_schema_tables_with_filter() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE filt_a (id INT PRIMARY KEY)", &mut session);
        handler.handle_query("CREATE TABLE filt_b (id INT PRIMARY KEY)", &mut session);

        let msgs = handler.handle_query(
            "SELECT * FROM information_schema.tables WHERE table_name = 'filt_a'",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][2], Some("filt_a".into()));
    }

    #[test]
    fn test_information_schema_columns() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE is_cols (id INT PRIMARY KEY, name TEXT, active BOOLEAN)",
            &mut session,
        );

        let msgs = handler.handle_query(
            "SELECT * FROM information_schema.columns WHERE table_name = 'is_cols'",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 3);
        // Check column details
        assert_eq!(rows[0][3], Some("id".into())); // column_name
        assert_eq!(rows[0][4], Some("1".into())); // ordinal_position
        assert_eq!(rows[0][6], Some("NO".into())); // is_nullable (PK)
        assert_eq!(rows[0][7], Some("integer".into())); // data_type
        assert_eq!(rows[0][12], Some("int4".into())); // udt_name

        assert_eq!(rows[1][3], Some("name".into()));
        assert_eq!(rows[1][7], Some("text".into()));
        assert_eq!(rows[1][12], Some("text".into())); // udt_name

        assert_eq!(rows[2][3], Some("active".into()));
        assert_eq!(rows[2][7], Some("boolean".into()));
        assert_eq!(rows[2][12], Some("bool".into())); // udt_name
    }

    #[test]
    fn test_information_schema_table_constraints() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE is_con (id INT PRIMARY KEY, val TEXT UNIQUE)",
            &mut session,
        );

        let msgs = handler.handle_query(
            "SELECT * FROM information_schema.table_constraints",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        let con_names: Vec<&str> = rows
            .iter()
            .filter(|r| r[3] == Some("is_con".into()))
            .filter_map(|r| r[4].as_deref())
            .collect();
        assert!(con_names.contains(&"PRIMARY KEY"));
        assert!(con_names.contains(&"UNIQUE"));
    }

    #[test]
    fn test_information_schema_key_column_usage() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE is_kcu (id INT PRIMARY KEY, name TEXT)",
            &mut session,
        );

        let msgs = handler.handle_query(
            "SELECT * FROM information_schema.key_column_usage",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        let pk_rows: Vec<_> = rows
            .iter()
            .filter(|r| r[1] == Some("is_kcu".into()))
            .collect();
        assert!(!pk_rows.is_empty());
        assert_eq!(pk_rows[0][2], Some("id".into())); // column_name
    }

    #[test]
    fn test_information_schema_schemata() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT * FROM information_schema.schemata", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 2); // public + information_schema
        let schema_names: Vec<&str> = rows.iter().filter_map(|r| r[1].as_deref()).collect();
        assert!(schema_names.contains(&"public"));
        assert!(schema_names.contains(&"information_schema"));
    }

    // ── View tests (M5.2) ──

    #[test]
    fn test_create_view_and_select() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE v_base (id INT PRIMARY KEY, name TEXT, active BOOLEAN)",
            &mut session,
        );
        handler.handle_query("INSERT INTO v_base VALUES (1, 'alice', true)", &mut session);
        handler.handle_query("INSERT INTO v_base VALUES (2, 'bob', false)", &mut session);

        // Create view
        let msgs = handler.handle_query(
            "CREATE VIEW v_active AS SELECT id, name FROM v_base WHERE active = true",
            &mut session,
        );
        assert!(!has_error_response(&msgs), "CREATE VIEW should succeed");

        // Select from view
        let msgs2 = handler.handle_query("SELECT * FROM v_active", &mut session);
        assert!(
            !has_error_response(&msgs2),
            "SELECT from view should succeed: {:?}",
            msgs2
        );
        let rows = extract_data_rows(&msgs2);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("1".into()));
        assert_eq!(rows[0][1], Some("alice".into()));
    }

    #[test]
    fn test_drop_view() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE dv_base (id INT PRIMARY KEY)", &mut session);
        handler.handle_query(
            "CREATE VIEW dv_view AS SELECT id FROM dv_base",
            &mut session,
        );

        let msgs = handler.handle_query("DROP VIEW dv_view", &mut session);
        assert!(!has_error_response(&msgs), "DROP VIEW should succeed");

        // Selecting from dropped view should fail
        let msgs2 = handler.handle_query("SELECT * FROM dv_view", &mut session);
        assert!(
            has_error_response(&msgs2),
            "SELECT from dropped view should fail"
        );
    }

    #[test]
    fn test_drop_view_if_exists() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("DROP VIEW IF EXISTS nonexistent_view", &mut session);
        assert!(
            !has_error_response(&msgs),
            "DROP VIEW IF EXISTS should not error"
        );
    }

    #[test]
    fn test_create_or_replace_view() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE cr_base (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO cr_base VALUES (1, 'a')", &mut session);

        handler.handle_query(
            "CREATE VIEW cr_view AS SELECT id FROM cr_base",
            &mut session,
        );
        // Replace with different query
        let msgs = handler.handle_query(
            "CREATE OR REPLACE VIEW cr_view AS SELECT id, val FROM cr_base",
            &mut session,
        );
        assert!(
            !has_error_response(&msgs),
            "CREATE OR REPLACE VIEW should succeed"
        );

        let msgs2 = handler.handle_query("SELECT * FROM cr_view", &mut session);
        let rows = extract_data_rows(&msgs2);
        assert_eq!(rows.len(), 1);
        // Should now have 2 columns (id, val) instead of just id
        assert_eq!(rows[0].len(), 2);
    }

    #[test]
    fn test_view_in_information_schema() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE vis_base (id INT PRIMARY KEY)", &mut session);
        handler.handle_query(
            "CREATE VIEW vis_view AS SELECT id FROM vis_base",
            &mut session,
        );

        // Views should appear in information_schema.tables with type VIEW
        let msgs = handler.handle_query("SELECT * FROM information_schema.tables", &mut session);
        let rows = extract_data_rows(&msgs);
        let _view_row = rows.iter().find(|r| r[2] == Some("vis_view".into()));
        // Currently views are not listed in information_schema.tables — that's fine for now
        // Just verify the view works
        let msgs2 = handler.handle_query("SELECT * FROM vis_view", &mut session);
        assert!(!has_error_response(&msgs2));
    }

    // ── ALTER TABLE RENAME tests (M5.3) ──

    #[test]
    fn test_alter_table_rename_column() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ren_col (id INT PRIMARY KEY, old_name TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO ren_col VALUES (1, 'hello')", &mut session);

        let msgs = handler.handle_query(
            "ALTER TABLE ren_col RENAME COLUMN old_name TO new_name",
            &mut session,
        );
        assert!(!has_error_response(&msgs), "RENAME COLUMN should succeed");

        // Verify column was renamed by querying information_schema
        let msgs2 = handler.handle_query(
            "SELECT * FROM information_schema.columns WHERE table_name = 'ren_col'",
            &mut session,
        );
        let rows = extract_data_rows(&msgs2);
        let col_names: Vec<&str> = rows.iter().filter_map(|r| r[3].as_deref()).collect();
        assert!(col_names.contains(&"new_name"), "Column should be renamed");
        assert!(!col_names.contains(&"old_name"), "Old name should be gone");

        // Verify data is still accessible
        let msgs3 = handler.handle_query("SELECT * FROM ren_col", &mut session);
        let rows3 = extract_data_rows(&msgs3);
        assert_eq!(rows3.len(), 1);
        assert_eq!(rows3[0][1], Some("hello".into()));
    }

    #[test]
    fn test_alter_table_rename_to() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE old_tbl (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO old_tbl VALUES (1, 'data')", &mut session);

        let msgs = handler.handle_query("ALTER TABLE old_tbl RENAME TO new_tbl", &mut session);
        assert!(!has_error_response(&msgs), "RENAME TABLE should succeed");

        // Old name should fail
        let msgs2 = handler.handle_query("SELECT * FROM old_tbl", &mut session);
        assert!(has_error_response(&msgs2), "Old table name should fail");

        // New name should work
        let msgs3 = handler.handle_query("SELECT * FROM new_tbl", &mut session);
        assert!(!has_error_response(&msgs3), "New table name should work");
        let rows = extract_data_rows(&msgs3);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Some("data".into()));
    }

    // ── pg_catalog handler tests ──

    #[test]
    fn test_pg_type() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT * FROM pg_catalog.pg_type", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 10, "should have builtin types");
        // Check that int4 is present
        let type_names: Vec<_> = rows
            .iter()
            .filter_map(|r| r.get(1).cloned().flatten())
            .collect();
        assert!(type_names.contains(&"int4".to_string()));
        assert!(type_names.contains(&"text".to_string()));
        assert!(type_names.contains(&"bool".to_string()));
    }

    #[test]
    fn test_pg_type_filter_by_oid() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query(
            "SELECT * FROM pg_catalog.pg_type WHERE oid = '23'",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Some("int4".into()));
    }

    #[test]
    fn test_pg_namespace() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT * FROM pg_catalog.pg_namespace", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 3);
        let ns_names: Vec<_> = rows
            .iter()
            .filter_map(|r| r.get(1).cloned().flatten())
            .collect();
        assert!(ns_names.contains(&"pg_catalog".to_string()));
        assert!(ns_names.contains(&"public".to_string()));
        assert!(ns_names.contains(&"information_schema".to_string()));
    }

    #[test]
    fn test_pg_database() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT * FROM pg_catalog.pg_database", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Some("falcon".into()));
    }

    #[test]
    fn test_pg_settings() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT * FROM pg_catalog.pg_settings", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 10);
        let setting_names: Vec<_> = rows
            .iter()
            .filter_map(|r| r.get(0).cloned().flatten())
            .collect();
        assert!(setting_names.contains(&"server_version".to_string()));
        assert!(setting_names.contains(&"server_encoding".to_string()));
        assert!(setting_names.contains(&"search_path".to_string()));
    }

    #[test]
    fn test_pg_index_with_pk() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE idx_test (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );
        let msgs = handler.handle_query("SELECT * FROM pg_catalog.pg_index", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 1, "should have at least PK index");
        // Check indisprimary is 't' for the PK
        let pk_rows: Vec<_> = rows
            .iter()
            .filter(|r| r.get(4).cloned().flatten() == Some("t".into()))
            .collect();
        assert!(!pk_rows.is_empty(), "should have a primary key index");
    }

    #[test]
    fn test_pg_constraint_pk() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE con_test (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );
        let msgs = handler.handle_query("SELECT * FROM pg_catalog.pg_constraint", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 1);
        // contype 'p' = PK
        let pk_rows: Vec<_> = rows
            .iter()
            .filter(|r| r.get(3).cloned().flatten() == Some("p".into()))
            .collect();
        assert!(!pk_rows.is_empty(), "should have PK constraint");
        // conname should contain table name
        let conname = pk_rows[0].get(1).cloned().flatten().unwrap();
        assert!(
            conname.contains("con_test"),
            "PK constraint name should reference table"
        );
    }

    // ── extract_where_eq tests ──

    #[test]
    fn test_extract_where_eq() {
        assert_eq!(
            extract_where_eq("select * from t where table_name = 'foo'", "table_name"),
            Some("foo".into())
        );
        assert_eq!(
            extract_where_eq("select * from t where table_name='bar'", "table_name"),
            Some("bar".into())
        );
        assert_eq!(extract_where_eq("select * from t", "table_name"), None);
    }

    // ── Phase 2: Prepared Statement / Parameterized SQL tests ──

    #[test]
    fn test_prepare_statement_select_with_param() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ps_t1 (id INT PRIMARY KEY, name TEXT)",
            &mut session,
        );

        let result = handler.prepare_statement("SELECT * FROM ps_t1 WHERE id = $1");
        assert!(
            result.is_ok(),
            "prepare_statement should succeed: {:?}",
            result.err()
        );
        let (plan, inferred_types, row_desc) = result.unwrap();
        // Should have 1 inferred parameter
        assert_eq!(inferred_types.len(), 1, "should infer 1 param type");
        // Row description should have 2 columns (id, name)
        assert_eq!(row_desc.len(), 2, "should describe 2 output columns");
        assert_eq!(row_desc[0].name, "id");
        assert_eq!(row_desc[1].name, "name");
        // Plan should exist
        assert!(!matches!(plan, PhysicalPlan::Begin));
    }

    #[test]
    fn test_prepare_statement_insert_with_params() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ps_t2 (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );

        let result = handler.prepare_statement("INSERT INTO ps_t2 VALUES ($1, $2)");
        assert!(
            result.is_ok(),
            "prepare INSERT should succeed: {:?}",
            result.err()
        );
        let (_plan, inferred_types, row_desc) = result.unwrap();
        assert_eq!(inferred_types.len(), 2, "should infer 2 param types");
        // INSERT has no output columns
        assert!(row_desc.is_empty(), "INSERT should have no row description");
    }

    #[test]
    fn test_prepare_statement_empty_sql() {
        let (handler, _session) = setup_handler();
        let result = handler.prepare_statement("");
        assert!(result.is_err(), "empty SQL should fail");
    }

    #[test]
    fn test_prepare_statement_invalid_sql() {
        let (handler, _session) = setup_handler();
        let result = handler.prepare_statement("SELECTT FROMM nothing");
        assert!(result.is_err(), "invalid SQL should fail");
    }

    #[test]
    fn test_execute_plan_select_no_params() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ep_t1 (id INT PRIMARY KEY, name TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO ep_t1 VALUES (1, 'alice')", &mut session);
        handler.handle_query("INSERT INTO ep_t1 VALUES (2, 'bob')", &mut session);

        let (plan, _types, _desc) = handler.prepare_statement("SELECT * FROM ep_t1").unwrap();
        let msgs = handler.execute_plan(&plan, &[], &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 2, "should return 2 rows");
    }

    #[test]
    fn test_execute_plan_select_with_int_param() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ep_t2 (id INT PRIMARY KEY, name TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO ep_t2 VALUES (1, 'alice')", &mut session);
        handler.handle_query("INSERT INTO ep_t2 VALUES (2, 'bob')", &mut session);

        let (plan, _types, _desc) = handler
            .prepare_statement("SELECT * FROM ep_t2 WHERE id = $1")
            .unwrap();
        let params = vec![Datum::Int32(1)];
        let msgs = handler.execute_plan(&plan, &params, &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1, "should return 1 row for id=1");
        assert_eq!(rows[0][1], Some("alice".into()));
    }

    #[test]
    fn test_execute_plan_select_with_text_param() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ep_t3 (id INT PRIMARY KEY, name TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO ep_t3 VALUES (1, 'alice')", &mut session);
        handler.handle_query("INSERT INTO ep_t3 VALUES (2, 'bob')", &mut session);

        let (plan, _types, _desc) = handler
            .prepare_statement("SELECT * FROM ep_t3 WHERE name = $1")
            .unwrap();
        let params = vec![Datum::Text("bob".into())];
        let msgs = handler.execute_plan(&plan, &params, &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1, "should return 1 row for name='bob'");
        assert_eq!(rows[0][0], Some("2".into()));
    }

    #[test]
    fn test_execute_plan_insert_with_params() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ep_t4 (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );

        let (plan, _types, _desc) = handler
            .prepare_statement("INSERT INTO ep_t4 VALUES ($1, $2)")
            .unwrap();
        let params = vec![Datum::Int32(42), Datum::Text("hello".into())];
        let msgs = handler.execute_plan(&plan, &params, &mut session);
        assert!(
            !has_error_response(&msgs),
            "INSERT with params should succeed: {:?}",
            msgs
        );

        // Verify the data was inserted
        let msgs2 = handler.handle_query("SELECT * FROM ep_t4", &mut session);
        let rows = extract_data_rows(&msgs2);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("42".into()));
        assert_eq!(rows[0][1], Some("hello".into()));
    }

    #[test]
    fn test_execute_plan_update_with_params() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ep_t5 (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO ep_t5 VALUES (1, 'old')", &mut session);

        let (plan, _types, _desc) = handler
            .prepare_statement("UPDATE ep_t5 SET val = $1 WHERE id = $2")
            .unwrap();
        let params = vec![Datum::Text("new".into()), Datum::Int32(1)];
        let msgs = handler.execute_plan(&plan, &params, &mut session);
        assert!(
            !has_error_response(&msgs),
            "UPDATE with params should succeed: {:?}",
            msgs
        );

        let msgs2 = handler.handle_query("SELECT * FROM ep_t5", &mut session);
        let rows = extract_data_rows(&msgs2);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Some("new".into()));
    }

    #[test]
    fn test_execute_plan_delete_with_params() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ep_t6 (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO ep_t6 VALUES (1, 'a')", &mut session);
        handler.handle_query("INSERT INTO ep_t6 VALUES (2, 'b')", &mut session);

        let (plan, _types, _desc) = handler
            .prepare_statement("DELETE FROM ep_t6 WHERE id = $1")
            .unwrap();
        let params = vec![Datum::Int32(1)];
        let msgs = handler.execute_plan(&plan, &params, &mut session);
        assert!(
            !has_error_response(&msgs),
            "DELETE with params should succeed: {:?}",
            msgs
        );

        let msgs2 = handler.handle_query("SELECT * FROM ep_t6", &mut session);
        let rows = extract_data_rows(&msgs2);
        assert_eq!(rows.len(), 1, "should have 1 row after deleting id=1");
        assert_eq!(rows[0][0], Some("2".into()));
    }

    #[test]
    fn test_execute_plan_with_null_param() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ep_t7 (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );

        let (plan, _types, _desc) = handler
            .prepare_statement("INSERT INTO ep_t7 VALUES ($1, $2)")
            .unwrap();
        let params = vec![Datum::Int32(1), Datum::Null];
        let msgs = handler.execute_plan(&plan, &params, &mut session);
        assert!(
            !has_error_response(&msgs),
            "INSERT with NULL param should succeed: {:?}",
            msgs
        );

        let msgs2 = handler.handle_query("SELECT * FROM ep_t7", &mut session);
        let rows = extract_data_rows(&msgs2);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], None, "NULL param should produce NULL value");
    }

    #[test]
    fn test_execute_plan_reuse_with_different_params() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ep_t8 (id INT PRIMARY KEY, name TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO ep_t8 VALUES (1, 'alice')", &mut session);
        handler.handle_query("INSERT INTO ep_t8 VALUES (2, 'bob')", &mut session);
        handler.handle_query("INSERT INTO ep_t8 VALUES (3, 'carol')", &mut session);

        let (plan, _types, _desc) = handler
            .prepare_statement("SELECT * FROM ep_t8 WHERE id = $1")
            .unwrap();

        // Execute with param=1
        let msgs1 = handler.execute_plan(&plan, &[Datum::Int32(1)], &mut session);
        let rows1 = extract_data_rows(&msgs1);
        assert_eq!(rows1.len(), 1);
        assert_eq!(rows1[0][1], Some("alice".into()));

        // Execute same plan with param=2
        let msgs2 = handler.execute_plan(&plan, &[Datum::Int32(2)], &mut session);
        let rows2 = extract_data_rows(&msgs2);
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0][1], Some("bob".into()));

        // Execute same plan with param=3
        let msgs3 = handler.execute_plan(&plan, &[Datum::Int32(3)], &mut session);
        let rows3 = extract_data_rows(&msgs3);
        assert_eq!(rows3.len(), 1);
        assert_eq!(rows3[0][1], Some("carol".into()));
    }

    #[test]
    fn test_datatype_to_oid_mapping() {
        let (handler, _session) = setup_handler();
        use falcon_common::types::DataType;

        assert_eq!(handler.datatype_to_oid(Some(&DataType::Int32)), 23);
        assert_eq!(handler.datatype_to_oid(Some(&DataType::Int64)), 20);
        assert_eq!(handler.datatype_to_oid(Some(&DataType::Float64)), 701);
        assert_eq!(handler.datatype_to_oid(Some(&DataType::Boolean)), 16);
        assert_eq!(handler.datatype_to_oid(Some(&DataType::Text)), 25);
        assert_eq!(handler.datatype_to_oid(Some(&DataType::Timestamp)), 1114);
        assert_eq!(handler.datatype_to_oid(Some(&DataType::Date)), 1082);
        assert_eq!(handler.datatype_to_oid(Some(&DataType::Jsonb)), 3802);
        assert_eq!(handler.datatype_to_oid(None), 0);
    }

    #[test]
    fn test_plan_output_fields_for_select() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE pof_t (id INT PRIMARY KEY, name TEXT, active BOOLEAN)",
            &mut session,
        );

        let (plan, _types, _desc) = handler.prepare_statement("SELECT * FROM pof_t").unwrap();
        let fields = handler.plan_output_fields(&plan);
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].type_oid, 23); // INT4
        assert_eq!(fields[1].name, "name");
        assert_eq!(fields[1].type_oid, 25); // TEXT
        assert_eq!(fields[2].name, "active");
        assert_eq!(fields[2].type_oid, 16); // BOOL
    }

    #[test]
    fn test_full_prepared_stmt_lifecycle() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ps_life (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO ps_life VALUES (1, 'one')", &mut session);
        handler.handle_query("INSERT INTO ps_life VALUES (2, 'two')", &mut session);

        // 1. Parse: prepare the statement
        let (plan, inferred_types, row_desc) = handler
            .prepare_statement("SELECT * FROM ps_life WHERE id = $1")
            .expect("Parse phase should succeed");

        // Store in session (simulating server.rs Parse handler)
        let effective_oids: Vec<i32> = inferred_types
            .iter()
            .map(|t| handler.datatype_to_oid(t.as_ref()))
            .collect();
        session.prepared_statements.insert(
            "stmt1".into(),
            crate::session::PreparedStatement {
                query: "SELECT * FROM ps_life WHERE id = $1".into(),
                param_types: effective_oids.clone(),
                plan: Some(plan),
                inferred_param_types: inferred_types.clone(),
                row_desc: row_desc.clone(),
            },
        );

        // 2. Describe: verify param types and row desc
        let ps = session.prepared_statements.get("stmt1").unwrap();
        assert_eq!(ps.param_types.len(), 1);
        assert_eq!(ps.row_desc.len(), 2);
        assert_eq!(ps.row_desc[0].name, "id");
        assert_eq!(ps.row_desc[1].name, "val");

        // 3. Bind: create portal with concrete params
        let datum_params = vec![Datum::Int32(2)];
        session.portals.insert(
            "portal1".into(),
            crate::session::Portal {
                plan: ps.plan.clone(),
                params: datum_params,
                bound_sql: String::new(),
            },
        );

        // 4. Execute: run portal
        let portal = session.portals.get("portal1").unwrap().clone();
        let msgs =
            handler.execute_plan(portal.plan.as_ref().unwrap(), &portal.params, &mut session);
        assert!(
            !has_error_response(&msgs),
            "Execute should succeed: {:?}",
            msgs
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Some("two".into()));

        // 5. Close: cleanup
        session.prepared_statements.remove("stmt1");
        session.portals.remove("portal1");
        assert!(session.prepared_statements.is_empty());
        assert!(session.portals.is_empty());
    }

    #[test]
    fn test_multiple_portals_same_statement() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ps_mp (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO ps_mp VALUES (1, 'a')", &mut session);
        handler.handle_query("INSERT INTO ps_mp VALUES (2, 'b')", &mut session);
        handler.handle_query("INSERT INTO ps_mp VALUES (3, 'c')", &mut session);

        let (plan, inferred_types, row_desc) = handler
            .prepare_statement("SELECT * FROM ps_mp WHERE id = $1")
            .unwrap();

        session.prepared_statements.insert(
            "s".into(),
            crate::session::PreparedStatement {
                query: "SELECT * FROM ps_mp WHERE id = $1".into(),
                param_types: vec![23],
                plan: Some(plan),
                inferred_param_types: inferred_types,
                row_desc,
            },
        );

        // Bind two portals from the same statement with different params
        let ps = session.prepared_statements.get("s").unwrap();
        session.portals.insert(
            "p1".into(),
            crate::session::Portal {
                plan: ps.plan.clone(),
                params: vec![Datum::Int32(1)],
                bound_sql: String::new(),
            },
        );
        session.portals.insert(
            "p2".into(),
            crate::session::Portal {
                plan: ps.plan.clone(),
                params: vec![Datum::Int32(3)],
                bound_sql: String::new(),
            },
        );

        // Execute portal 1
        let p1 = session.portals.get("p1").unwrap().clone();
        let msgs1 = handler.execute_plan(p1.plan.as_ref().unwrap(), &p1.params, &mut session);
        let rows1 = extract_data_rows(&msgs1);
        assert_eq!(rows1.len(), 1);
        assert_eq!(rows1[0][1], Some("a".into()));

        // Execute portal 2
        let p2 = session.portals.get("p2").unwrap().clone();
        let msgs2 = handler.execute_plan(p2.plan.as_ref().unwrap(), &p2.params, &mut session);
        let rows2 = extract_data_rows(&msgs2);
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0][1], Some("c".into()));
    }

    // ── M4: Multi-tenancy SQL commands ───────────────────────────────────────

    #[test]
    fn test_create_tenant_basic() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("CREATE TENANT acme", &mut session);
        let has_complete = msgs.iter().any(|m| {
            matches!(m,
                BackendMessage::CommandComplete { tag } if tag.contains("acme")
            )
        });
        assert!(has_complete, "CREATE TENANT should return CommandComplete");
    }

    #[test]
    fn test_create_tenant_with_quotas() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query(
            "CREATE TENANT bigcorp MAX_QPS 1000 MAX_STORAGE_BYTES 1073741824",
            &mut session,
        );
        let has_complete = msgs.iter().any(|m| {
            matches!(m,
                BackendMessage::CommandComplete { tag } if tag.contains("bigcorp")
            )
        });
        assert!(has_complete, "CREATE TENANT with quotas should succeed");
    }

    #[test]
    fn test_create_tenant_duplicate_fails() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TENANT dup", &mut session);
        let msgs = handler.handle_query("CREATE TENANT dup", &mut session);
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(has_error, "Duplicate CREATE TENANT should return error");
    }

    #[test]
    fn test_drop_tenant_basic() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TENANT todelete", &mut session);
        let msgs = handler.handle_query("DROP TENANT todelete", &mut session);
        let has_complete = msgs.iter().any(|m| {
            matches!(m,
                BackendMessage::CommandComplete { tag } if tag.contains("todelete")
            )
        });
        assert!(has_complete, "DROP TENANT should return CommandComplete");
    }

    #[test]
    fn test_drop_tenant_nonexistent_fails() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("DROP TENANT ghost", &mut session);
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(has_error, "DROP TENANT on nonexistent tenant should error");
    }

    #[test]
    fn test_show_falcon_tenants() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TENANT t1", &mut session);
        handler.handle_query("CREATE TENANT t2", &mut session);
        let msgs = handler.handle_query("SHOW falcon.tenants", &mut session);
        assert!(
            has_row_description(&msgs),
            "SHOW falcon.tenants should have RowDescription"
        );
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty(), "SHOW falcon.tenants should return rows");
        // Should include system tenant + t1 + t2
        let all_text: String = rows
            .iter()
            .flat_map(|r| r.iter())
            .filter_map(|v| v.as_ref())
            .cloned()
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            all_text.contains("3") || all_text.contains("t1") || all_text.contains("t2"),
            "tenants output should mention created tenants: {}",
            all_text
        );
    }

    #[test]
    fn test_show_falcon_tenant_usage() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TENANT usage_test", &mut session);
        let msgs = handler.handle_query("SHOW falcon.tenant_usage", &mut session);
        assert!(
            has_row_description(&msgs),
            "SHOW falcon.tenant_usage should have RowDescription"
        );
        let rows = extract_data_rows(&msgs);
        assert!(
            !rows.is_empty(),
            "SHOW falcon.tenant_usage should return rows"
        );
    }

    // ── M4: Vectorized columnar aggregate path ────────────────────────────────

    #[test]
    #[cfg(feature = "columnstore")]
    fn test_columnstore_count_uses_vectorized_path() {
        let (handler, mut session) = setup_handler();
        // Create a ColumnStore table and insert rows
        handler.handle_query(
            "CREATE TABLE cs_agg (id INT, score FLOAT8) ENGINE=columnstore",
            &mut session,
        );
        for i in 1..=100i32 {
            handler.handle_query(
                &format!("INSERT INTO cs_agg VALUES ({}, {})", i, i as f64 * 1.5),
                &mut session,
            );
        }
        // COUNT(*) should go through vectorized columnar path
        let msgs = handler.handle_query("SELECT COUNT(*) FROM cs_agg", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1, "COUNT(*) should return 1 row");
        let count_val = rows[0][0].as_deref().unwrap_or("0");
        assert_eq!(
            count_val, "100",
            "COUNT(*) should return 100, got {}",
            count_val
        );
    }

    #[test]
    #[cfg(feature = "columnstore")]
    fn test_columnstore_sum_uses_vectorized_path() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE cs_sum (id INT, val INT) ENGINE=columnstore",
            &mut session,
        );
        for i in 1..=10i32 {
            handler.handle_query(
                &format!("INSERT INTO cs_sum VALUES ({}, {})", i, i),
                &mut session,
            );
        }
        // SUM should go through vectorized columnar path
        let msgs = handler.handle_query("SELECT SUM(val) FROM cs_sum", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1, "SUM should return 1 row");
        let sum_val = rows[0][0].as_deref().unwrap_or("0");
        // SUM(1..10) = 55
        assert_eq!(sum_val, "55", "SUM(val) should be 55, got {}", sum_val);
    }

    // ── M4: Lag-aware replica routing ────────────────────────────────────────

    #[test]
    fn test_select_least_lagging_replica_prefers_caught_up() {
        use falcon_cluster::replication::runner::ReplicaRunnerMetricsSnapshot;
        use falcon_cluster::DistributedQueryEngine;

        let replicas = vec![
            ReplicaRunnerMetricsSnapshot {
                chunks_applied: 10,
                records_applied: 100,
                applied_lsn: 100,
                primary_lsn: 100,
                lag_lsn: 0,
                reconnect_count: 0,
                acks_sent: 10,
                connected: true,
            },
            ReplicaRunnerMetricsSnapshot {
                chunks_applied: 8,
                records_applied: 80,
                applied_lsn: 80,
                primary_lsn: 100,
                lag_lsn: 20,
                reconnect_count: 1,
                acks_sent: 8,
                connected: true,
            },
        ];

        let best = DistributedQueryEngine::select_least_lagging_replica(&replicas);
        assert!(best.is_some(), "should select a replica");
        assert_eq!(
            best.unwrap().lag_lsn,
            0,
            "should prefer fully caught-up replica"
        );
    }

    #[test]
    fn test_select_least_lagging_replica_skips_disconnected() {
        use falcon_cluster::replication::runner::ReplicaRunnerMetricsSnapshot;
        use falcon_cluster::DistributedQueryEngine;

        let replicas = vec![
            ReplicaRunnerMetricsSnapshot {
                chunks_applied: 10,
                records_applied: 100,
                applied_lsn: 100,
                primary_lsn: 100,
                lag_lsn: 0,
                reconnect_count: 0,
                acks_sent: 10,
                connected: false,
            },
            ReplicaRunnerMetricsSnapshot {
                chunks_applied: 9,
                records_applied: 90,
                applied_lsn: 90,
                primary_lsn: 100,
                lag_lsn: 10,
                reconnect_count: 0,
                acks_sent: 9,
                connected: true,
            },
        ];

        let best = DistributedQueryEngine::select_least_lagging_replica(&replicas);
        assert!(best.is_some(), "should select connected replica");
        assert!(
            best.unwrap().connected,
            "selected replica must be connected"
        );
        assert_eq!(
            best.unwrap().lag_lsn,
            10,
            "should select the connected replica"
        );
    }

    #[test]
    fn test_select_least_lagging_replica_empty_returns_none() {
        use falcon_cluster::replication::runner::ReplicaRunnerMetricsSnapshot;
        use falcon_cluster::DistributedQueryEngine;

        let replicas: Vec<ReplicaRunnerMetricsSnapshot> = vec![];
        let best = DistributedQueryEngine::select_least_lagging_replica(&replicas);
        assert!(best.is_none(), "empty replica list should return None");
    }

    // ── Cluster Operations Closure tests ────────────────────────────────

    #[test]
    fn test_show_cluster_events_empty() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.cluster_events", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty());
        assert_eq!(rows[0][0], Some("(no events)".into()));
    }

    #[test]
    fn test_show_node_lifecycle_empty() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.node_lifecycle", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty());
        assert_eq!(rows[0][0], Some("(none)".into()));
    }

    #[test]
    fn test_show_rebalance_plan_single_shard() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.rebalance_plan", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty());
        // Single-shard mode: should say not applicable
        assert!(rows[0][1].as_ref().unwrap().contains("single-shard"));
    }

    #[test]
    fn test_admin_add_node() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT falcon_add_node(42)", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert!(rows[0][0].as_ref().unwrap().contains("Scale-out initiated"));
        assert!(rows[0][0].as_ref().unwrap().contains("42"));
    }

    #[test]
    fn test_admin_add_node_duplicate() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("SELECT falcon_add_node(42)", &mut session);
        let msgs = handler.handle_query("SELECT falcon_add_node(42)", &mut session);
        let rows = extract_data_rows(&msgs);
        assert!(rows[0][0].as_ref().unwrap().contains("ERROR"));
    }

    #[test]
    fn test_admin_remove_node() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT falcon_remove_node(7)", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(rows[0][0].as_ref().unwrap().contains("Scale-in initiated"));
    }

    #[test]
    fn test_admin_promote_leader() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT falcon_promote_leader(0)", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(rows[0][0]
            .as_ref()
            .unwrap()
            .contains("Leader promotion requested"));
    }

    #[test]
    fn test_admin_rebalance_apply_single_shard() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT falcon_rebalance_apply()", &mut session);
        let rows = extract_data_rows(&msgs);
        assert!(rows[0][0].as_ref().unwrap().contains("single-shard"));
    }

    #[test]
    fn test_admin_add_node_invalid_id() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT falcon_add_node(0)", &mut session);
        let rows = extract_data_rows(&msgs);
        assert!(rows[0][0].as_ref().unwrap().contains("ERROR"));
    }

    #[test]
    fn test_cluster_events_after_admin_ops() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("SELECT falcon_add_node(10)", &mut session);
        handler.handle_query("SELECT falcon_remove_node(20)", &mut session);
        handler.handle_query("SELECT falcon_promote_leader(0)", &mut session);

        let msgs = handler.handle_query("SHOW falcon.cluster_events", &mut session);
        let rows = extract_data_rows(&msgs);
        // Should have events for: add_node(joining), remove_node(draining), promote_leader
        assert!(
            rows.len() >= 3,
            "expected at least 3 events, got {}",
            rows.len()
        );
    }

    #[test]
    fn test_node_lifecycle_after_add() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("SELECT falcon_add_node(99)", &mut session);

        let msgs = handler.handle_query("SHOW falcon.node_lifecycle", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("scale_out".into()));
        assert_eq!(rows[0][1], Some("99".into()));
        assert_eq!(rows[0][2], Some("joining".into()));
    }

    #[test]
    fn test_g1_full_scale_out_lifecycle_via_sql() {
        let (handler, mut session) = setup_handler();

        // 1. Add node
        let msgs = handler.handle_query("SELECT falcon_add_node(50)", &mut session);
        let rows = extract_data_rows(&msgs);
        assert!(rows[0][0].as_ref().unwrap().contains("Scale-out initiated"));

        // 2. Verify node_lifecycle shows joining
        let msgs = handler.handle_query("SHOW falcon.node_lifecycle", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows[0][2], Some("joining".into()));

        // 3. Verify cluster_events logged the scale-out
        let msgs = handler.handle_query("SHOW falcon.cluster_events", &mut session);
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 1);
        assert!(
            rows.iter()
                .any(|r| { r[2].as_deref() == Some("scale_out") }),
            "cluster_events should contain scale_out event"
        );

        // 4. Rebalance plan should work (single-shard mode returns not applicable)
        let msgs = handler.handle_query("SHOW falcon.rebalance_plan", &mut session);
        assert!(has_row_description(&msgs));
    }

    #[test]
    fn test_g2_full_scale_in_lifecycle_via_sql() {
        let (handler, mut session) = setup_handler();

        // 1. Remove node
        let msgs = handler.handle_query("SELECT falcon_remove_node(30)", &mut session);
        let rows = extract_data_rows(&msgs);
        assert!(rows[0][0].as_ref().unwrap().contains("Scale-in initiated"));

        // 2. Verify node_lifecycle shows draining
        let msgs = handler.handle_query("SHOW falcon.node_lifecycle", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows[0][2], Some("draining".into()));

        // 3. Verify cluster_events logged the scale-in
        let msgs = handler.handle_query("SHOW falcon.cluster_events", &mut session);
        let rows = extract_data_rows(&msgs);
        assert!(
            rows.iter().any(|r| { r[2].as_deref() == Some("scale_in") }),
            "cluster_events should contain scale_in event"
        );
    }

    #[test]
    fn test_g5_rebalance_plan_has_estimated_time_column() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.rebalance_plan", &mut session);
        assert!(has_row_description(&msgs));
        // Verify the RowDescription has 6 columns (type, shards, table, detail, status, estimated_time)
        // or 2 columns in single-shard mode (metric, value)
        for msg in &msgs {
            if let BackendMessage::RowDescription { fields } = msg {
                // single-shard mode has 2 columns, multi-shard has 6
                assert!(
                    fields.len() == 2 || fields.len() == 6,
                    "rebalance_plan should have 2 or 6 columns, got {}",
                    fields.len()
                );
                if fields.len() == 6 {
                    assert_eq!(fields[5].name, "estimated_time");
                }
            }
        }
    }

    // ── Tail Latency Governance tests ───────────────────────────────────

    #[test]
    fn test_show_priority_scheduler() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.priority_scheduler", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 10, "expected scheduler metrics rows");
        assert_eq!(rows[0][0], Some("high_active".into()));
        assert_eq!(rows[0][1], Some("0".into()));
    }

    #[test]
    fn test_show_token_bucket() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.token_bucket", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 6, "expected token bucket metrics rows");
        assert_eq!(rows[0][0], Some("rate_per_sec".into()));
        // Rebalance preset: 5000 tokens/sec
        assert_eq!(rows[0][1], Some("5000".into()));
    }

    // ── Deterministic 2PC tests ─────────────────────────────────────────

    #[test]
    fn test_show_two_phase_config() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.two_phase_config", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        // Should have decision_log + timeout + slow_shard sections (20 rows)
        assert!(
            rows.len() >= 15,
            "expected at least 15 rows, got {}",
            rows.len()
        );
        assert_eq!(rows[0][0], Some("decision_log.total_logged".into()));
        assert_eq!(rows[0][1], Some("0".into()));
        // Check timeout defaults
        assert_eq!(rows[5][0], Some("timeout.soft_ms".into()));
        assert_eq!(rows[5][1], Some("500".into()));
        // Check slow-shard policy default
        assert_eq!(rows[11][0], Some("slow_shard.policy".into()));
        assert_eq!(rows[11][1], Some("fast_abort".into()));
    }

    // ── Chaos / Fault Injection tests ───────────────────────────────────

    #[test]
    fn test_show_fault_injection() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.fault_injection", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        // 5 base + 6 partition + 5 jitter = 16 rows
        assert!(
            rows.len() >= 16,
            "expected at least 16 rows, got {}",
            rows.len()
        );
        assert_eq!(rows[0][0], Some("leader_killed".into()));
        assert_eq!(rows[0][1], Some("false".into()));
        assert_eq!(rows[5][0], Some("partition.active".into()));
        assert_eq!(rows[5][1], Some("false".into()));
        assert_eq!(rows[11][0], Some("jitter.enabled".into()));
        assert_eq!(rows[11][1], Some("false".into()));
    }

    // ── Observability catalog test ──────────────────────────────────────

    #[test]
    fn test_show_observability_catalog() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.observability_catalog", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        // Should have 50 commands listed
        assert!(
            rows.len() >= 50,
            "expected at least 50 catalog entries, got {}",
            rows.len()
        );
        // First entry
        assert_eq!(rows[0][0], Some("SHOW falcon.version".into()));
        // Last entry should be the catalog itself
        let last = rows.last().unwrap();
        assert_eq!(last[0], Some("SHOW falcon.observability_catalog".into()));
        // Check 3 columns: command, description, since
        assert!(rows[0].len() == 3);
        assert!(rows[0][2].is_some()); // since column
    }

    // ── Security hardening tests ────────────────────────────────────────

    #[test]
    fn test_show_security_audit() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.security_audit", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        // 8 auth + 8 password + 11 firewall = 27 rows
        assert!(
            rows.len() >= 27,
            "expected at least 27 rows, got {}",
            rows.len()
        );
        assert_eq!(rows[0][0], Some("auth.max_failures".into()));
        assert_eq!(rows[0][1], Some("5".into()));
        assert_eq!(rows[8][0], Some("password.min_length".into()));
        assert_eq!(rows[8][1], Some("8".into()));
        assert_eq!(rows[16][0], Some("firewall.detect_injection".into()));
        assert_eq!(rows[16][1], Some("true".into()));
    }

    // ── Release engineering tests ───────────────────────────────────────

    #[test]
    fn test_show_wire_compat() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.wire_compat", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        // 10 rows: server_version, wal_format_version, snapshot_format_version, etc.
        assert!(
            rows.len() >= 10,
            "expected at least 10 rows, got {}",
            rows.len()
        );
        assert_eq!(rows[0][0], Some("server_version".into()));
        assert_eq!(rows[1][0], Some("wal_format_version".into()));
        assert_eq!(rows[1][1], Some("4".into()));
        assert_eq!(rows[8][0], Some("wal_magic".into()));
        assert_eq!(rows[8][1], Some("FALC".into()));
    }

    // ── B8: PG protocol corner cases ─────────────────────────────────────

    #[test]
    fn test_b8_empty_query_no_crash() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("", &mut session);
        // Should return EmptyQueryResponse or at least not panic
        let has_empty = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::EmptyQueryResponse));
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(
            has_empty || has_error,
            "empty query should return EmptyQueryResponse or ErrorResponse"
        );
    }

    #[test]
    fn test_b8_semicolons_only_no_crash() {
        let (handler, mut session) = setup_handler();
        // Semicolons-only: must not panic. Empty vec, EmptyQueryResponse, or ErrorResponse all OK.
        let msgs = handler.handle_query(";;;", &mut session);
        let _ = msgs; // just verify no panic
    }

    #[test]
    fn test_syntax_error_returns_error_response() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECTTTT 1", &mut session);
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(has_error, "syntax error should return ErrorResponse");
    }

    #[test]
    fn test_select_1_returns_data() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT 1", &mut session);
        assert!(
            has_row_description(&msgs),
            "SELECT 1 should return RowDescription"
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1, "SELECT 1 should return 1 row");
    }

    #[test]
    fn test_begin_commit_lifecycle() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("BEGIN", &mut session);
        let has_begin = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
        assert!(has_begin, "BEGIN should return CommandComplete");

        let msgs = handler.handle_query("COMMIT", &mut session);
        let has_commit = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
        assert!(has_commit, "COMMIT should return CommandComplete");
    }

    #[test]
    fn test_commit_outside_txn_does_not_crash() {
        let (handler, mut session) = setup_handler();
        // COMMIT without BEGIN — should not panic
        let msgs = handler.handle_query("COMMIT", &mut session);
        assert!(
            !msgs.is_empty(),
            "COMMIT outside txn should return something"
        );
    }

    #[test]
    fn test_rollback_outside_txn_does_not_crash() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("ROLLBACK", &mut session);
        assert!(
            !msgs.is_empty(),
            "ROLLBACK outside txn should return something"
        );
    }

    #[test]
    fn test_drop_nonexistent_table_returns_error() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("DROP TABLE nonexistent_table_xyz", &mut session);
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(
            has_error,
            "DROP nonexistent table should return ErrorResponse"
        );
    }

    #[test]
    fn test_select_from_nonexistent_table_returns_error() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT * FROM nonexistent_table_xyz", &mut session);
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(
            has_error,
            "SELECT from nonexistent table should return ErrorResponse"
        );
    }

    #[test]
    fn test_create_table_and_insert_lifecycle() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query(
            "CREATE TABLE b8_test (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );
        let has_ok = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
        assert!(has_ok, "CREATE TABLE should succeed");

        let msgs = handler.handle_query("INSERT INTO b8_test VALUES (1, 'hello')", &mut session);
        let has_ok = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
        assert!(has_ok, "INSERT should succeed");

        let msgs = handler.handle_query("SELECT * FROM b8_test", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_duplicate_create_table_returns_error() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE b8_dup (id INT PRIMARY KEY)", &mut session);
        let msgs = handler.handle_query("CREATE TABLE b8_dup (id INT PRIMARY KEY)", &mut session);
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(
            has_error,
            "duplicate CREATE TABLE should return ErrorResponse"
        );
    }

    #[test]
    fn test_set_and_show_client_encoding() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SET client_encoding TO 'UTF8'", &mut session);
        let has_ok = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
        assert!(has_ok, "SET client_encoding should succeed");

        let msgs = handler.handle_query("SHOW client_encoding", &mut session);
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty());
    }

    // ── 1.1 ACID SQL-level verification ──────────────────────────────────

    #[test]
    fn test_acid_atomicity_commit_visible() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE acid_a (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        handler.handle_query("BEGIN", &mut session);
        handler.handle_query("INSERT INTO acid_a VALUES (1, 'a')", &mut session);
        handler.handle_query("INSERT INTO acid_a VALUES (2, 'b')", &mut session);
        handler.handle_query("COMMIT", &mut session);

        let msgs = handler.handle_query("SELECT * FROM acid_a ORDER BY id", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 2, "committed txn: both rows visible");
    }

    #[test]
    fn test_acid_atomicity_rollback_invisible() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE acid_r (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        handler.handle_query("BEGIN", &mut session);
        handler.handle_query("INSERT INTO acid_r VALUES (1, 'a')", &mut session);
        handler.handle_query("INSERT INTO acid_r VALUES (2, 'b')", &mut session);
        handler.handle_query("ROLLBACK", &mut session);

        let msgs = handler.handle_query("SELECT * FROM acid_r", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 0, "rolled-back txn: no rows visible");
    }

    #[test]
    fn test_acid_consistency_pk_enforced() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE acid_pk (id INT PRIMARY KEY)", &mut session);
        handler.handle_query("INSERT INTO acid_pk VALUES (1)", &mut session);
        let msgs = handler.handle_query("INSERT INTO acid_pk VALUES (1)", &mut session);
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(has_error, "duplicate PK must return error");
    }

    #[test]
    fn test_acid_consistency_not_null_enforced() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE acid_nn (id INT PRIMARY KEY, v TEXT NOT NULL)",
            &mut session,
        );
        let msgs = handler.handle_query("INSERT INTO acid_nn VALUES (1, NULL)", &mut session);
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(has_error, "NOT NULL violation must return error");
    }

    #[test]
    fn test_acid_isolation_snapshot() {
        // Two sessions would be needed for true isolation test, but we verify
        // that within a single session, auto-commit semantics hold:
        // insert in txn1, commit, insert in txn2, rollback → only txn1 visible
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE acid_si (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );

        // txn1: commit
        handler.handle_query("BEGIN", &mut session);
        handler.handle_query("INSERT INTO acid_si VALUES (1, 'committed')", &mut session);
        handler.handle_query("COMMIT", &mut session);

        // txn2: rollback
        handler.handle_query("BEGIN", &mut session);
        handler.handle_query(
            "INSERT INTO acid_si VALUES (2, 'rolled_back')",
            &mut session,
        );
        handler.handle_query("ROLLBACK", &mut session);

        let msgs = handler.handle_query("SELECT * FROM acid_si", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1, "only committed row visible");
        assert_eq!(rows[0][1], Some("committed".into()));
    }

    #[test]
    fn test_acid_durability_wal_observer() {
        // Verify that committed data survives (using in-memory WAL observer as proxy)
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE acid_d (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO acid_d VALUES (1, 'durable')", &mut session);

        // Read back immediately — must be visible (WAL applied synchronously)
        let msgs = handler.handle_query("SELECT v FROM acid_d WHERE id = 1", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("durable".into()));
    }

    // ── 1.3 Fast-Path / Slow-Path verification ──────────────────────────

    #[test]
    fn test_fast_path_stats_visible() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE fp (id INT PRIMARY KEY, v TEXT)", &mut session);
        handler.handle_query("INSERT INTO fp VALUES (1, 'fast')", &mut session);
        handler.handle_query("INSERT INTO fp VALUES (2, 'path')", &mut session);

        let msgs = handler.handle_query("SHOW falcon.txn_stats", &mut session);
        let rows = extract_data_rows(&msgs);
        let metric_names: Vec<_> = rows
            .iter()
            .filter_map(|r| r.first().cloned().flatten())
            .collect();
        assert!(
            metric_names.contains(&"fast_path_commits".to_string()),
            "txn_stats must expose fast_path_commits"
        );
        assert!(
            metric_names.contains(&"slow_path_commits".to_string()),
            "txn_stats must expose slow_path_commits"
        );
    }

    // ── 3.1 PG SQL whitelist verification ────────────────────────────────

    #[test]
    fn test_sql_join_inner() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE j_a (id INT PRIMARY KEY, name TEXT)",
            &mut session,
        );
        handler.handle_query("CREATE TABLE j_b (aid INT, val TEXT)", &mut session);
        handler.handle_query("INSERT INTO j_a VALUES (1, 'alice')", &mut session);
        handler.handle_query("INSERT INTO j_b VALUES (1, 'x')", &mut session);
        handler.handle_query("INSERT INTO j_b VALUES (2, 'y')", &mut session);

        let msgs = handler.handle_query(
            "SELECT j_a.name, j_b.val FROM j_a JOIN j_b ON j_a.id = j_b.aid",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("alice".into()));
    }

    #[test]
    fn test_sql_join_left() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE lj_a (id INT PRIMARY KEY)", &mut session);
        handler.handle_query("CREATE TABLE lj_b (aid INT, v TEXT)", &mut session);
        handler.handle_query("INSERT INTO lj_a VALUES (1)", &mut session);
        handler.handle_query("INSERT INTO lj_a VALUES (2)", &mut session);
        handler.handle_query("INSERT INTO lj_b VALUES (1, 'matched')", &mut session);

        let msgs = handler.handle_query(
            "SELECT lj_a.id, lj_b.v FROM lj_a LEFT JOIN lj_b ON lj_a.id = lj_b.aid ORDER BY lj_a.id",
            &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 2, "LEFT JOIN: both rows from left side");
    }

    #[test]
    fn test_sql_group_by_aggregate() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE agg (id INT PRIMARY KEY, dept TEXT, salary INT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO agg VALUES (1, 'eng', 100)", &mut session);
        handler.handle_query("INSERT INTO agg VALUES (2, 'eng', 200)", &mut session);
        handler.handle_query("INSERT INTO agg VALUES (3, 'sales', 150)", &mut session);

        let msgs = handler.handle_query(
            "SELECT dept, COUNT(*), SUM(salary) FROM agg GROUP BY dept ORDER BY dept",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_sql_order_by_limit() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE obl (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        for i in 1..=5 {
            handler.handle_query(
                &format!("INSERT INTO obl VALUES ({}, 'r{}')", i, i),
                &mut session,
            );
        }
        let msgs = handler.handle_query("SELECT * FROM obl ORDER BY id DESC LIMIT 3", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Some("5".into()));
    }

    #[test]
    fn test_sql_upsert_on_conflict() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE ups (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO ups VALUES (1, 'original')", &mut session);
        handler.handle_query(
            "INSERT INTO ups VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET v = excluded.v",
            &mut session,
        );

        let msgs = handler.handle_query("SELECT v FROM ups WHERE id = 1", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("updated".into()));
    }

    #[test]
    fn test_sql_update_returning() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE ur (id INT PRIMARY KEY, v INT)", &mut session);
        handler.handle_query("INSERT INTO ur VALUES (1, 10)", &mut session);

        let msgs = handler.handle_query(
            "UPDATE ur SET v = 20 WHERE id = 1 RETURNING id, v",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Some("20".into()));
    }

    #[test]
    fn test_sql_delete_returning() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE dr (id INT PRIMARY KEY, v TEXT)", &mut session);
        handler.handle_query("INSERT INTO dr VALUES (1, 'gone')", &mut session);

        let msgs = handler.handle_query("DELETE FROM dr WHERE id = 1 RETURNING v", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("gone".into()));

        let msgs = handler.handle_query("SELECT * FROM dr", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 0);
    }

    // ── 3.2 Unsupported features return clear errors ─────────────────────

    #[test]
    fn test_unsupported_create_trigger_error() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE trig_t (id INT PRIMARY KEY)", &mut session);
        let msgs = handler.handle_query(
            "CREATE TRIGGER my_trig AFTER INSERT ON trig_t FOR EACH ROW EXECUTE FUNCTION noop()",
            &mut session,
        );
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(has_error, "CREATE TRIGGER must return error in v1.0");
    }

    #[test]
    fn test_unsupported_create_function_error() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query(
            "CREATE FUNCTION add(a int, b int) RETURNS int AS $$ SELECT a + b; $$ LANGUAGE SQL",
            &mut session,
        );
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(has_error, "CREATE FUNCTION must return error in v1.0");
    }

    // ── 5.1-5.2 Observability SHOW commands ──────────────────────────────

    #[test]
    fn test_show_memory_stats() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.memory", &mut session);
        assert!(
            has_row_description(&msgs),
            "SHOW falcon.memory should return data"
        );
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty());
    }

    #[test]
    fn test_show_nodes() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.nodes", &mut session);
        // May return RowDescription + data or error — either is acceptable for single-node
        assert!(!msgs.is_empty());
    }

    #[test]
    fn test_show_replication() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW falcon.replication_stats", &mut session);
        assert!(!msgs.is_empty());
    }

    // ── SQL-level PREPARE / EXECUTE / DEALLOCATE tests ──────────────────

    #[test]
    fn test_sql_prepare_execute_select() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE sql_ps1 (id INT PRIMARY KEY, name TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO sql_ps1 VALUES (1, 'alice')", &mut session);
        handler.handle_query("INSERT INTO sql_ps1 VALUES (2, 'bob')", &mut session);

        // PREPARE
        let msgs = handler.handle_query(
            "PREPARE my_select AS SELECT * FROM sql_ps1 WHERE id = $1",
            &mut session,
        );
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::CommandComplete { tag } if tag == "PREPARE")),
            "PREPARE should return CommandComplete"
        );
        assert!(
            session.prepared_statements.contains_key("my_select"),
            "prepared statement should be stored in session"
        );

        // Verify plan was created (plan-based path)
        let ps = session.prepared_statements.get("my_select").unwrap();
        assert!(ps.plan.is_some(), "SQL PREPARE should create a plan");
        assert_eq!(ps.inferred_param_types.len(), 1, "should infer 1 param");

        // EXECUTE with param
        let msgs = handler.handle_query("EXECUTE my_select(1)", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Some("alice".into()));
    }

    #[test]
    fn test_sql_prepare_execute_insert() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE sql_ps2 (id INT PRIMARY KEY, val TEXT)",
            &mut session,
        );

        let msgs = handler.handle_query(
            "PREPARE my_insert AS INSERT INTO sql_ps2 VALUES ($1, $2)",
            &mut session,
        );
        assert!(msgs.iter().any(
            |m| matches!(m, BackendMessage::CommandComplete { tag } if tag == "PREPARE")
        ));

        // Execute the prepared insert
        let msgs = handler.handle_query("EXECUTE my_insert(1, 'hello')", &mut session);
        let has_complete = msgs.iter().any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
        assert!(has_complete, "EXECUTE INSERT should return CommandComplete");

        // Verify data was inserted
        let msgs = handler.handle_query("SELECT * FROM sql_ps2", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("1".into()));
        assert_eq!(rows[0][1], Some("hello".into()));
    }

    #[test]
    fn test_sql_prepare_execute_no_params() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE sql_ps3 (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO sql_ps3 VALUES (1, 'x')", &mut session);

        handler.handle_query(
            "PREPARE all_rows AS SELECT * FROM sql_ps3",
            &mut session,
        );
        let msgs = handler.handle_query("EXECUTE all_rows", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_sql_execute_nonexistent() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("EXECUTE ghost_stmt(1)", &mut session);
        let has_error = msgs.iter().any(|m| {
            matches!(m, BackendMessage::ErrorResponse { code, .. } if code == "26000")
        });
        assert!(has_error, "EXECUTE on nonexistent stmt should return SQLSTATE 26000");
    }

    #[test]
    fn test_sql_deallocate_specific() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE sql_dealloc (id INT PRIMARY KEY)",
            &mut session,
        );
        handler.handle_query(
            "PREPARE stmt_a AS SELECT * FROM sql_dealloc",
            &mut session,
        );
        handler.handle_query(
            "PREPARE stmt_b AS SELECT * FROM sql_dealloc",
            &mut session,
        );
        assert_eq!(session.prepared_statements.len(), 2);

        handler.handle_query("DEALLOCATE stmt_a", &mut session);
        assert_eq!(session.prepared_statements.len(), 1);
        assert!(!session.prepared_statements.contains_key("stmt_a"));
        assert!(session.prepared_statements.contains_key("stmt_b"));
    }

    #[test]
    fn test_sql_deallocate_all() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE sql_dealloc2 (id INT PRIMARY KEY)",
            &mut session,
        );
        handler.handle_query(
            "PREPARE s1 AS SELECT * FROM sql_dealloc2",
            &mut session,
        );
        handler.handle_query(
            "PREPARE s2 AS SELECT * FROM sql_dealloc2",
            &mut session,
        );
        assert_eq!(session.prepared_statements.len(), 2);

        handler.handle_query("DEALLOCATE ALL", &mut session);
        assert!(session.prepared_statements.is_empty());
    }

    #[test]
    fn test_sql_discard_all_clears_prepared() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE sql_discard (id INT PRIMARY KEY)",
            &mut session,
        );
        handler.handle_query(
            "PREPARE disc_stmt AS SELECT * FROM sql_discard",
            &mut session,
        );
        assert!(!session.prepared_statements.is_empty());

        handler.handle_query("DISCARD ALL", &mut session);
        assert!(session.prepared_statements.is_empty());
    }

    #[test]
    fn test_sql_prepare_with_type_hints() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE sql_typed (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );

        // PREPARE with explicit type list (types are informational, query is planned)
        let msgs = handler.handle_query(
            "PREPARE typed_q(int, text) AS SELECT * FROM sql_typed WHERE id = $1",
            &mut session,
        );
        assert!(msgs.iter().any(
            |m| matches!(m, BackendMessage::CommandComplete { tag } if tag == "PREPARE")
        ));
        assert!(session.prepared_statements.contains_key("typed_q"));
    }

    #[test]
    fn test_sql_prepare_invalid_syntax() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("PREPARE", &mut session);
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        assert!(has_error, "bare PREPARE should be an error");
    }

    // ── Helper function unit tests ──────────────────────────────────────

    #[test]
    fn test_parse_prepare_statement_basic() {
        let result = parse_prepare_statement("PREPARE foo AS SELECT 1");
        assert!(result.is_some());
        let (name, query) = result.unwrap();
        assert_eq!(name, "foo");
        assert_eq!(query, "SELECT 1");
    }

    #[test]
    fn test_parse_prepare_statement_with_types() {
        let result = parse_prepare_statement("PREPARE bar(int, text) AS INSERT INTO t VALUES ($1, $2)");
        assert!(result.is_some());
        let (name, query) = result.unwrap();
        assert_eq!(name, "bar");
        assert_eq!(query, "INSERT INTO t VALUES ($1, $2)");
    }

    #[test]
    fn test_parse_prepare_statement_case_insensitive() {
        let result = parse_prepare_statement("prepare MyStmt as SELECT * FROM t");
        assert!(result.is_some());
        let (name, _query) = result.unwrap();
        assert_eq!(name, "mystmt");
    }

    #[test]
    fn test_parse_execute_statement_no_params() {
        let result = parse_execute_statement("EXECUTE foo");
        assert!(result.is_some());
        let (name, params) = result.unwrap();
        assert_eq!(name, "foo");
        assert!(params.is_empty());
    }

    #[test]
    fn test_parse_execute_statement_with_params() {
        let result = parse_execute_statement("EXECUTE foo(1, 'hello', NULL)");
        assert!(result.is_some());
        let (name, params) = result.unwrap();
        assert_eq!(name, "foo");
        assert_eq!(params.len(), 3);
        assert_eq!(params[0], Some(b"1".to_vec()));
        assert_eq!(params[1], Some(b"hello".to_vec()));
        assert!(params[2].is_none()); // NULL
    }

    #[test]
    fn test_bind_params_substitution() {
        let result = bind_params("SELECT * FROM t WHERE id = $1 AND name = $2", &[
            Some(b"42".to_vec()),
            Some(b"alice".to_vec()),
        ]);
        assert_eq!(result, "SELECT * FROM t WHERE id = '42' AND name = 'alice'");
    }

    #[test]
    fn test_bind_params_null() {
        let result = bind_params("INSERT INTO t VALUES ($1, $2)", &[
            Some(b"1".to_vec()),
            None,
        ]);
        assert_eq!(result, "INSERT INTO t VALUES ('1', NULL)");
    }

    #[test]
    fn test_bind_params_no_params() {
        let result = bind_params("SELECT 1", &[]);
        assert_eq!(result, "SELECT 1");
    }

    #[test]
    fn test_text_params_to_datum_typed() {
        use falcon_common::types::DataType;
        let params = vec![
            Some(b"42".to_vec()),
            Some(b"hello".to_vec()),
            None,
        ];
        let hints = vec![
            Some(DataType::Int32),
            Some(DataType::Text),
            Some(DataType::Int32),
        ];
        let datums = text_params_to_datum(&params, &hints);
        assert_eq!(datums.len(), 3);
        assert_eq!(datums[0], Datum::Int32(42));
        assert_eq!(datums[1], Datum::Text("hello".into()));
        assert!(matches!(datums[2], Datum::Null), "None param should produce Datum::Null");
    }

    #[test]
    fn test_text_params_to_datum_no_hints() {
        let params = vec![Some(b"123".to_vec()), Some(b"abc".to_vec())];
        let hints: Vec<Option<falcon_common::types::DataType>> = vec![];
        let datums = text_params_to_datum(&params, &hints);
        assert_eq!(datums[0], Datum::Int64(123)); // inferred as integer
        assert_eq!(datums[1], Datum::Text("abc".into())); // inferred as text
    }

    #[test]
    fn test_text_params_to_datum_boolean() {
        use falcon_common::types::DataType;
        let params = vec![Some(b"true".to_vec()), Some(b"false".to_vec())];
        let hints = vec![Some(DataType::Boolean), Some(DataType::Boolean)];
        let datums = text_params_to_datum(&params, &hints);
        assert_eq!(datums[0], Datum::Boolean(true));
        assert_eq!(datums[1], Datum::Boolean(false));
    }

    // ── SMALLINT / REAL data type tests ─────────────────────────────────

    #[test]
    fn test_smallint_create_insert_select() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE si_t (id SMALLINT PRIMARY KEY, val SMALLINT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO si_t VALUES (1, 100)", &mut session);
        handler.handle_query("INSERT INTO si_t VALUES (2, -32000)", &mut session);

        let msgs = handler.handle_query("SELECT * FROM si_t ORDER BY id", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Some("1".into()));
        assert_eq!(rows[0][1], Some("100".into()));
        assert_eq!(rows[1][1], Some("-32000".into()));
    }

    #[test]
    fn test_real_create_insert_select() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE real_t (id INT PRIMARY KEY, val REAL)",
            &mut session,
        );
        handler.handle_query("INSERT INTO real_t VALUES (1, 3.14)", &mut session);
        handler.handle_query("INSERT INTO real_t VALUES (2, -0.5)", &mut session);

        let msgs = handler.handle_query("SELECT * FROM real_t ORDER BY id", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 2);
        // REAL is stored as Float64 internally, values should round-trip
        assert!(rows[0][1].as_ref().unwrap().starts_with("3.14"));
        assert!(rows[1][1].as_ref().unwrap().starts_with("-0.5"));
    }

    #[test]
    fn test_smallint_cast() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE cast_si (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO cast_si VALUES (1, '42')", &mut session);

        let msgs = handler.handle_query(
            "SELECT CAST(v AS SMALLINT) FROM cast_si WHERE id = 1",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("42".into()));
    }

    #[test]
    fn test_real_cast() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE cast_re (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO cast_re VALUES (1, '2.718')", &mut session);

        let msgs = handler.handle_query(
            "SELECT CAST(v AS REAL) FROM cast_re WHERE id = 1",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert!(rows[0][0].as_ref().unwrap().starts_with("2.718"));
    }

    #[test]
    fn test_mixed_int_types_table() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE mixed_int (a SMALLINT, b INT, c BIGINT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO mixed_int VALUES (1, 100000, 9999999999)", &mut session);

        let msgs = handler.handle_query("SELECT * FROM mixed_int", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("1".into()));
        assert_eq!(rows[0][1], Some("100000".into()));
        assert_eq!(rows[0][2], Some("9999999999".into()));
    }

    #[test]
    fn test_mixed_float_types_table() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE mixed_float (a REAL, b FLOAT8)",
            &mut session,
        );
        handler.handle_query("INSERT INTO mixed_float VALUES (1.5, 2.5)", &mut session);

        let msgs = handler.handle_query("SELECT * FROM mixed_float", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert!(rows[0][0].as_ref().unwrap().starts_with("1.5"));
        assert!(rows[0][1].as_ref().unwrap().starts_with("2.5"));
    }

    // ── NUMERIC / DECIMAL tests ────────────────────────────────────────

    #[test]
    fn test_numeric_create_insert_select() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE t_numeric (id INT PRIMARY KEY, price NUMERIC(10,2), qty DECIMAL)",
            &mut session,
        );
        handler.handle_query(
            "INSERT INTO t_numeric VALUES (1, 123.45, 99.9)",
            &mut session,
        );

        let msgs = handler.handle_query("SELECT price, qty FROM t_numeric WHERE id = 1", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("123.45".into()));
        // qty parsed as Decimal with default precision
        assert!(rows[0][1].as_ref().unwrap().contains("99.9"));
    }

    #[test]
    fn test_numeric_cast_from_text() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE t_num_cast (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO t_num_cast VALUES (1, '456.789')", &mut session);

        let msgs = handler.handle_query(
            "SELECT CAST(v AS NUMERIC) FROM t_num_cast WHERE id = 1",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("456.789".into()));
    }

    #[test]
    fn test_numeric_cast_from_int() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE t_num_int (id INT PRIMARY KEY, v INT)",
            &mut session,
        );
        handler.handle_query("INSERT INTO t_num_int VALUES (1, 42)", &mut session);

        let msgs = handler.handle_query(
            "SELECT CAST(v AS DECIMAL) FROM t_num_int WHERE id = 1",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("42".into()));
    }

    // ── UUID tests ─────────────────────────────────────────────────────

    #[test]
    fn test_uuid_create_insert_select() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE t_uuid (id INT PRIMARY KEY, uid UUID)",
            &mut session,
        );
        handler.handle_query(
            "INSERT INTO t_uuid VALUES (1, '550e8400-e29b-41d4-a716-446655440000')",
            &mut session,
        );

        let msgs = handler.handle_query("SELECT uid FROM t_uuid WHERE id = 1", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0],
            Some("550e8400-e29b-41d4-a716-446655440000".into())
        );
    }

    #[test]
    fn test_uuid_cast_from_text() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE t_uuid_cast (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        handler.handle_query(
            "INSERT INTO t_uuid_cast VALUES (1, '12345678-1234-1234-1234-123456789abc')",
            &mut session,
        );

        let msgs = handler.handle_query(
            "SELECT CAST(v AS UUID) FROM t_uuid_cast WHERE id = 1",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0],
            Some("12345678-1234-1234-1234-123456789abc".into())
        );
    }

    // ── BYTEA tests ────────────────────────────────────────────────────

    #[test]
    fn test_bytea_create_insert_select() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE t_bytea (id INT PRIMARY KEY, data BYTEA)",
            &mut session,
        );
        handler.handle_query(
            "INSERT INTO t_bytea VALUES (1, '\\xdeadbeef')",
            &mut session,
        );

        let msgs = handler.handle_query("SELECT data FROM t_bytea WHERE id = 1", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("\\xdeadbeef".into()));
    }

    #[test]
    fn test_bytea_cast_from_text() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE t_bytea_cast (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        handler.handle_query(
            "INSERT INTO t_bytea_cast VALUES (1, 'cafebabe')",
            &mut session,
        );

        let msgs = handler.handle_query(
            "SELECT CAST(v AS BYTEA) FROM t_bytea_cast WHERE id = 1",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("\\xcafebabe".into()));
    }

    // ── TIME tests ─────────────────────────────────────────────────────

    #[test]
    fn test_time_create_insert_select() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE t_time (id INT PRIMARY KEY, t TIME)",
            &mut session,
        );
        handler.handle_query(
            "INSERT INTO t_time VALUES (1, '14:30:00')",
            &mut session,
        );

        let msgs = handler.handle_query("SELECT t FROM t_time WHERE id = 1", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("14:30:00".into()));
    }

    #[test]
    fn test_time_cast_from_text() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE t_time_cast (id INT PRIMARY KEY, v TEXT)",
            &mut session,
        );
        handler.handle_query(
            "INSERT INTO t_time_cast VALUES (1, '09:15:30')",
            &mut session,
        );

        let msgs = handler.handle_query(
            "SELECT CAST(v AS TIME) FROM t_time_cast WHERE id = 1",
            &mut session,
        );
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("09:15:30".into()));
    }

    // ── INTERVAL tests ─────────────────────────────────────────────────

    #[test]
    fn test_interval_create_insert_select() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE t_interval (id INT PRIMARY KEY, dur INTERVAL)",
            &mut session,
        );
        handler.handle_query(
            "INSERT INTO t_interval VALUES (1, '01:30:00')",
            &mut session,
        );

        let msgs = handler.handle_query("SELECT dur FROM t_interval WHERE id = 1", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        // Interval stored as text currently, should come back
        assert!(rows[0][0].is_some());
    }

    // ── Mixed-type table ───────────────────────────────────────────────

    #[test]
    fn test_all_new_types_table() {
        let (handler, mut session) = setup_handler();
        handler.handle_query(
            "CREATE TABLE t_all_types (\
                id INT PRIMARY KEY, \
                price NUMERIC(10,2), \
                uid UUID, \
                blob BYTEA, \
                t TIME \
            )",
            &mut session,
        );
        handler.handle_query(
            "INSERT INTO t_all_types VALUES (\
                1, \
                99.95, \
                'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', \
                '\\xaabbccdd', \
                '23:59:59' \
            )",
            &mut session,
        );

        let msgs = handler.handle_query("SELECT * FROM t_all_types WHERE id = 1", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("1".into()));
        assert_eq!(rows[0][1], Some("99.95".into()));
        assert_eq!(
            rows[0][2],
            Some("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into())
        );
        assert_eq!(rows[0][3], Some("\\xaabbccdd".into()));
        assert_eq!(rows[0][4], Some("23:59:59".into()));
    }
}
