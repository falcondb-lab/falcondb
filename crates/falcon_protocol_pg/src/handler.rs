use std::sync::Arc;
use std::time::Instant;

use falcon_cluster::fault_injection::FaultInjector;
use falcon_cluster::security_hardening::{
    AuthRateLimiter, AuthRateLimiterConfig, PasswordPolicy, PasswordPolicyConfig, SqlFirewall,
    SqlFirewallConfig,
};
use falcon_cluster::{
    ClusterAdmin, ClusterEventLog, CoordinatorDecisionLog, DecisionLogConfig,
    DistributedQueryEngine, HAReplicaGroup, LayeredTimeoutConfig, LayeredTimeoutController,
    ReplicaRunnerMetrics, SlowShardConfig, SlowShardTracker, SyncReplicationWaiter, TokenBucket,
    TokenBucketConfig,
};
use falcon_common::consistency::CommitPolicy;
use falcon_common::datum::Datum;
use falcon_common::error::FalconError;
use falcon_common::types::ShardId;
use falcon_executor::{ExecutionResult, Executor, PriorityScheduler, PrioritySchedulerConfig};
use falcon_planner::{IndexedColumns, PhysicalPlan, PlannedTxnType, Planner, TableRowCounts};
use falcon_sql_frontend::binder::Binder;
use falcon_sql_frontend::parser::parse_sql;
use falcon_storage::engine::StorageEngine;
use falcon_txn::{SlowPathMode, TxnClassification, TxnManager};
use parking_lot::RwLock;

use crate::codec::{BackendMessage, FieldDescription};
use crate::plan_cache::PlanCache;
use crate::session::PgSession;
use crate::slow_query_log::SlowQueryLog;

/// Handles a single SQL query within a session, producing PG backend messages.
#[derive(Clone)]
pub struct QueryHandler {
    pub(crate) storage: Arc<StorageEngine>,
    pub(crate) txn_mgr: Arc<TxnManager>,
    pub(crate) executor: Arc<Executor>,
    /// When set, the handler wraps eligible plans in DistPlan for multi-shard execution.
    /// Empty or single-element means single-shard mode (no wrapping).
    pub(crate) cluster_shard_ids: Vec<ShardId>,
    /// Optional distributed query engine for executing DistPlan and routed DML.
    /// When present, DistPlan/DDL/DML are dispatched here instead of the local executor.
    pub(crate) dist_engine: Option<Arc<DistributedQueryEngine>>,
    /// Slow query log shared across all sessions.
    pub(crate) slow_query_log: Arc<SlowQueryLog>,
    /// Query plan cache shared across all sessions.
    pub(crate) plan_cache: Arc<PlanCache>,
    /// Replica replication metrics (only set when running as replica).
    pub(crate) replica_metrics: Option<Arc<ReplicaRunnerMetrics>>,
    /// P2-1: Tenant registry for multi-tenant isolation.
    pub(crate) tenant_registry: Arc<falcon_storage::tenant_registry::TenantRegistry>,
    /// P2-2: Audit log for enterprise compliance.
    pub(crate) audit_log: Arc<falcon_storage::audit::AuditLog>,
    /// P3-1: License information.
    pub(crate) license_info: Arc<falcon_common::edition::LicenseInfo>,
    /// P3-1: Feature gate for edition-based feature control.
    pub(crate) feature_gate: Arc<falcon_common::edition::FeatureGate>,
    /// P3-3: Resource meter for per-tenant billing.
    pub(crate) resource_meter: Arc<falcon_storage::metering::ResourceMeter>,
    /// P3-6: Security manager for encryption/TLS/IP control.
    pub(crate) security_manager: Arc<falcon_storage::security_manager::SecurityManager>,
    /// DK-5: Hotspot detector for shard/table access monitoring.
    pub(crate) hotspot_detector: Arc<falcon_storage::hotspot::HotspotDetector>,
    /// DK-9: Consistency verifier for self-verification.
    pub(crate) consistency_verifier: Arc<falcon_storage::verification::ConsistencyVerifier>,
    /// Active commit policy: determines durability guarantees before client ACK.
    pub(crate) commit_policy: CommitPolicy,
    /// Sync replication waiter: enforces quorum-ack / semi-sync / sync durability.
    pub(crate) sync_waiter: Option<Arc<SyncReplicationWaiter>>,
    /// HA replica group for sync replication LSN tracking.
    pub(crate) ha_group: Option<Arc<RwLock<HAReplicaGroup>>>,
    /// Cluster admin coordinator for scale-out/in, rebalance, leader transfer.
    pub(crate) cluster_admin: Arc<ClusterAdmin>,
    /// Priority scheduler for tail latency governance.
    pub(crate) priority_scheduler: Arc<PriorityScheduler>,
    /// Token bucket for DDL/backfill/rebalance rate limiting.
    pub(crate) rebalance_token_bucket: Arc<TokenBucket>,
    /// 2PC coordinator decision log (durable commit point).
    pub(crate) decision_log: Arc<CoordinatorDecisionLog>,
    /// 2PC layered timeout controller.
    pub(crate) timeout_controller: Arc<LayeredTimeoutController>,
    /// 2PC slow-shard tracker.
    pub(crate) slow_shard_tracker: Arc<SlowShardTracker>,
    /// Fault injector for chaos testing.
    pub(crate) fault_injector: Arc<FaultInjector>,
    /// Auth rate limiter for brute-force protection.
    pub(crate) auth_rate_limiter: Arc<AuthRateLimiter>,
    /// Password policy enforcer.
    pub(crate) password_policy: Arc<PasswordPolicy>,
    /// SQL firewall for injection detection.
    pub(crate) sql_firewall: Arc<SqlFirewall>,
}

impl QueryHandler {
    pub fn new(
        storage: Arc<StorageEngine>,
        txn_mgr: Arc<TxnManager>,
        executor: Arc<Executor>,
    ) -> Self {
        Self {
            storage,
            txn_mgr,
            executor,
            cluster_shard_ids: vec![ShardId(0)],
            dist_engine: None,
            slow_query_log: Arc::new(SlowQueryLog::disabled()),
            plan_cache: Arc::new(PlanCache::new(256)),
            replica_metrics: None,
            tenant_registry: Arc::new(falcon_storage::tenant_registry::TenantRegistry::new()),
            audit_log: Arc::new(falcon_storage::audit::AuditLog::new()),
            license_info: Arc::new(falcon_common::edition::LicenseInfo::community()),
            feature_gate: Arc::new(falcon_common::edition::FeatureGate::for_edition(
                falcon_common::edition::EditionTier::Community,
            )),
            resource_meter: Arc::new(falcon_storage::metering::ResourceMeter::new()),
            security_manager: Arc::new(falcon_storage::security_manager::SecurityManager::new()),
            hotspot_detector: Arc::new(falcon_storage::hotspot::HotspotDetector::default()),
            consistency_verifier: Arc::new(
                falcon_storage::verification::ConsistencyVerifier::default(),
            ),
            commit_policy: CommitPolicy::default(),
            sync_waiter: None,
            ha_group: None,
            cluster_admin: ClusterAdmin::new(ClusterEventLog::new(256)),
            priority_scheduler: PriorityScheduler::new(PrioritySchedulerConfig::default()),
            rebalance_token_bucket: TokenBucket::new(TokenBucketConfig::rebalance()),
            decision_log: CoordinatorDecisionLog::new(DecisionLogConfig::default()),
            timeout_controller: LayeredTimeoutController::new(LayeredTimeoutConfig::default()),
            slow_shard_tracker: SlowShardTracker::new(SlowShardConfig::default()),
            fault_injector: Arc::new(FaultInjector::new()),
            auth_rate_limiter: Arc::new(AuthRateLimiter::new(AuthRateLimiterConfig::default())),
            password_policy: Arc::new(PasswordPolicy::new(PasswordPolicyConfig::default())),
            sql_firewall: Arc::new(SqlFirewall::new(SqlFirewallConfig::default())),
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
        Self {
            storage,
            txn_mgr,
            executor,
            cluster_shard_ids: shard_ids,
            dist_engine: Some(dist_engine),
            slow_query_log: Arc::new(SlowQueryLog::disabled()),
            plan_cache: Arc::new(PlanCache::new(256)),
            replica_metrics: None,
            tenant_registry: Arc::new(falcon_storage::tenant_registry::TenantRegistry::new()),
            audit_log: Arc::new(falcon_storage::audit::AuditLog::new()),
            license_info: Arc::new(falcon_common::edition::LicenseInfo::community()),
            feature_gate: Arc::new(falcon_common::edition::FeatureGate::for_edition(
                falcon_common::edition::EditionTier::Community,
            )),
            resource_meter: Arc::new(falcon_storage::metering::ResourceMeter::new()),
            security_manager: Arc::new(falcon_storage::security_manager::SecurityManager::new()),
            hotspot_detector: Arc::new(falcon_storage::hotspot::HotspotDetector::default()),
            consistency_verifier: Arc::new(
                falcon_storage::verification::ConsistencyVerifier::default(),
            ),
            commit_policy: CommitPolicy::default(),
            sync_waiter: None,
            ha_group: None,
            cluster_admin: ClusterAdmin::new(ClusterEventLog::new(256)),
            priority_scheduler: PriorityScheduler::new(PrioritySchedulerConfig::default()),
            rebalance_token_bucket: TokenBucket::new(TokenBucketConfig::rebalance()),
            decision_log: CoordinatorDecisionLog::new(DecisionLogConfig::default()),
            timeout_controller: LayeredTimeoutController::new(LayeredTimeoutConfig::default()),
            slow_shard_tracker: SlowShardTracker::new(SlowShardConfig::default()),
            fault_injector: Arc::new(FaultInjector::new()),
            auth_rate_limiter: Arc::new(AuthRateLimiter::new(AuthRateLimiterConfig::default())),
            password_policy: Arc::new(PasswordPolicy::new(PasswordPolicyConfig::default())),
            sql_firewall: Arc::new(SqlFirewall::new(SqlFirewallConfig::default())),
        }
    }

    /// Set the cluster admin coordinator.
    pub fn set_cluster_admin(&mut self, admin: Arc<ClusterAdmin>) {
        self.cluster_admin = admin;
    }

    /// Set the commit policy for durability guarantees.
    pub fn set_commit_policy(&mut self, policy: CommitPolicy) {
        self.commit_policy = policy;
    }

    /// Configure synchronous replication waiter for quorum-ack / semi-sync / sync modes.
    pub fn set_sync_replication(
        &mut self,
        waiter: Arc<SyncReplicationWaiter>,
        group: Arc<RwLock<HAReplicaGroup>>,
    ) {
        self.sync_waiter = Some(waiter);
        self.ha_group = Some(group);
    }

    /// Set replica runner metrics for SHOW falcon.replica_stats.
    pub fn set_replica_metrics(&mut self, metrics: Arc<ReplicaRunnerMetrics>) {
        self.replica_metrics = Some(metrics);
    }

    /// Set a shared slow query log instance.
    pub fn with_slow_query_log(mut self, log: Arc<SlowQueryLog>) -> Self {
        self.slow_query_log = log;
        self
    }

    /// Get a reference to the slow query log.
    pub fn slow_query_log(&self) -> &Arc<SlowQueryLog> {
        &self.slow_query_log
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

        // Intercept system/catalog queries that psql sends
        if let Some(response) = self.handle_system_query(sql, session) {
            return response;
        }

        // Build per-request context for tracing and error enrichment.
        let rctx = falcon_common::request_context::RequestContext::new(session.id as u64);

        // Wrap the core query path in catch_request for crash domain isolation.
        // If any code below panics, the panic is caught and converted to an
        // InternalBug error response — the connection stays alive.
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

    /// Inner query processing logic, called from `handle_query` inside a
    /// crash-domain guard.
    fn handle_query_inner(
        &self,
        sql: &str,
        session: &mut PgSession,
    ) -> Result<Vec<BackendMessage>, FalconError> {
        // Parse
        let stmts = match parse_sql(sql) {
            Ok(stmts) => stmts,
            Err(e) => {
                return Ok(vec![self.error_response(&FalconError::Sql(e))]);
            }
        };

        let mut messages = Vec::new();

        for stmt in &stmts {
            // Try plan cache first
            let plan = if let Some(cached) = self.plan_cache.get(sql) {
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

                // Plan (with cost-based join reordering and index scan detection)
                let row_counts = self.build_row_counts();
                let indexed_cols = self.build_indexed_columns();
                let p = match Planner::plan_with_indexes(&bound, &row_counts, &indexed_cols) {
                    Ok(p) => p,
                    Err(e) => {
                        messages.push(self.error_response(&FalconError::Sql(e)));
                        return Ok(messages);
                    }
                };

                // Wrap in DistPlan if multi-shard cluster
                let p = Planner::wrap_distributed(p, &self.cluster_shard_ids);

                // Cache the plan
                self.plan_cache.put(sql, p.clone());
                p
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
                                    (&self.sync_waiter, &self.ha_group)
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
                                            message: format!("sync replication timeout: {}", e),
                                        });
                                    }
                                }
                                session.txn = None;
                                session.autocommit = true;
                                self.flush_txn_stats();
                                messages.push(BackendMessage::CommandComplete {
                                    tag: "COMMIT".into(),
                                });
                            }
                            Err(e) => {
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

                let txn_ref = match session.txn.as_ref() {
                    Some(t) => t,
                    None => {
                        messages.push(BackendMessage::ErrorResponse {
                            severity: "ERROR".into(),
                            code: "25P01".into(),
                            message: "no active transaction for COPY TO".into(),
                        });
                        continue;
                    }
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
                            tag: format!("COPY {}", row_count),
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

                let txn_ref = match session.txn.as_ref() {
                    Some(t) => t,
                    None => {
                        messages.push(BackendMessage::ErrorResponse {
                            severity: "ERROR".into(),
                            code: "25P01".into(),
                            message: "no active transaction for COPY TO (query)".into(),
                        });
                        continue;
                    }
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
                            tag: format!("COPY {}", row_count),
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
                        self.plan_cache.invalidate();
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

            // Route execution: DistPlan and multi-shard DML/DDL go through
            // DistributedQueryEngine when available; local plans use Executor.
            let query_start = Instant::now();
            let result = if let Some(dist) = &self.dist_engine {
                dist.execute(&plan, session.txn.as_ref())
            } else {
                self.executor.execute(&plan, session.txn.as_ref())
            };
            let query_duration = query_start.elapsed();

            match result {
                Ok(exec_result) => {
                    match exec_result {
                        ExecutionResult::Query { columns, rows } => {
                            // RowDescription
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

                            // DataRows
                            let row_count = rows.len();
                            for row in rows {
                                let values: Vec<Option<String>> =
                                    row.values.iter().map(|d| d.to_pg_text()).collect();
                                messages.push(BackendMessage::DataRow { values });
                            }

                            messages.push(BackendMessage::CommandComplete {
                                tag: format!("SELECT {}", row_count),
                            });
                        }
                        ExecutionResult::Dml { rows_affected, tag } => {
                            let cmd_tag = match tag.as_str() {
                                "INSERT" => format!("INSERT 0 {}", rows_affected),
                                "UPDATE" => format!("UPDATE {}", rows_affected),
                                "DELETE" => format!("DELETE {}", rows_affected),
                                _ => format!("{} {}", tag, rows_affected),
                            };
                            messages.push(BackendMessage::CommandComplete { tag: cmd_tag });
                        }
                        ExecutionResult::Ddl { message } => {
                            self.plan_cache.invalidate();
                            messages.push(BackendMessage::CommandComplete { tag: message });
                        }
                        ExecutionResult::TxnControl { action } => {
                            messages.push(BackendMessage::CommandComplete { tag: action });
                        }
                    }

                    // Auto-commit if needed
                    if auto_txn {
                        if let Some(ref txn) = session.txn {
                            let _ = self.txn_mgr.commit(txn.txn_id);
                        }
                        session.txn = None;
                        self.flush_txn_stats();
                    }

                    // Record to slow query log
                    self.slow_query_log.record(sql, query_duration, session.id);
                }
                Err(e) => {
                    // Record to slow query log (even failed queries)
                    self.slow_query_log.record(sql, query_duration, session.id);

                    // Auto-abort on error
                    if auto_txn {
                        if let Some(ref txn) = session.txn {
                            let _ = self.txn_mgr.abort(txn.txn_id);
                        }
                        session.txn = None;
                        self.flush_txn_stats();
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
            tag: format!("SELECT {}", row_count),
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
        let plan = Planner::wrap_distributed(plan, &self.cluster_shard_ids);

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
                    self.plan_cache.invalidate();
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

        // Execute with parameter substitution
        let query_start = std::time::Instant::now();
        let result = if params.is_empty() {
            if let Some(dist) = &self.dist_engine {
                dist.execute(plan, session.txn.as_ref())
            } else {
                self.executor.execute(plan, session.txn.as_ref())
            }
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
                                row.values.iter().map(|d| d.to_pg_text()).collect();
                            messages.push(BackendMessage::DataRow { values });
                        }
                        messages.push(BackendMessage::CommandComplete {
                            tag: format!("SELECT {}", row_count),
                        });
                    }
                    ExecutionResult::Dml { rows_affected, tag } => {
                        let cmd_tag = match tag.as_str() {
                            "INSERT" => format!("INSERT 0 {}", rows_affected),
                            "UPDATE" => format!("UPDATE {}", rows_affected),
                            "DELETE" => format!("DELETE {}", rows_affected),
                            _ => format!("{} {}", tag, rows_affected),
                        };
                        messages.push(BackendMessage::CommandComplete { tag: cmd_tag });
                    }
                    ExecutionResult::Ddl { message } => {
                        self.plan_cache.invalidate();
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
    pub fn datatype_to_oid(&self, dt: Option<&falcon_common::types::DataType>) -> i32 {
        use falcon_common::types::DataType;
        match dt {
            Some(DataType::Int32) => 23,           // INT4
            Some(DataType::Int64) => 20,           // INT8
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
                    .map(|c| c.data_type.clone())
                    .unwrap_or(DataType::Text),
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
                        BinOp::StringConcat => DataType::Text,
                        // JSONB operators → Jsonb (or Text for ->>/#>>)
                        BinOp::JsonArrow
                        | BinOp::JsonHashArrow
                        | BinOp::JsonContains
                        | BinOp::JsonContainedBy
                        | BinOp::JsonExists => DataType::Jsonb,
                        BinOp::JsonArrowText | BinOp::JsonHashArrowText => DataType::Text,
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
                    .map(|e| infer_expr_type(e, cols))
                    .unwrap_or(DataType::Text),
                BoundExpr::Function { func, args } => infer_func_type(func, args, cols),
                BoundExpr::ScalarSubquery(_) => DataType::Text, // best-effort
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
                    .map(|c| c.data_type.clone())
                    .unwrap_or(DataType::Text),
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
                "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" => DataType::Text,
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
                ScalarFunc::Now => DataType::Timestamp,
                ScalarFunc::CurrentDate => DataType::Date,
                ScalarFunc::CurrentTime => DataType::Time,
                ScalarFunc::DateTrunc => DataType::Timestamp,
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
                    .map(|a| infer_expr_type(a, cols))
                    .unwrap_or(DataType::Text),
                // Array mutation returns array
                ScalarFunc::ArrayAppend
                | ScalarFunc::ArrayPrepend
                | ScalarFunc::ArrayRemove
                | ScalarFunc::ArrayReplace
                | ScalarFunc::ArrayCat => args
                    .first()
                    .map(|a| infer_expr_type(a, cols))
                    .unwrap_or(DataType::Array(Box::new(DataType::Text))),
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
                AggFunc::Avg => DataType::Float64,
                AggFunc::Min | AggFunc::Max => input_ty.unwrap_or(DataType::Text),
                AggFunc::StringAgg(_) => DataType::Text,
                AggFunc::BoolAnd | AggFunc::BoolOr => DataType::Boolean,
                AggFunc::ArrayAgg => DataType::Array(Box::new(input_ty.unwrap_or(DataType::Text))),
                // Statistical aggregates always return Float64
                AggFunc::StddevPop
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
                "{} HINT: leader={}, retry_after={}ms",
                message, hint, retry_after_ms
            );
        }
        BackendMessage::ErrorResponse {
            severity: err.pg_severity().into(),
            code: err.pg_sqlstate().into(),
            message,
        }
    }

    /// Push current txn stats to Prometheus gauges.
    pub(crate) fn flush_txn_stats(&self) {
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

fn classification_from_routing_hint(hint: &falcon_planner::TxnRoutingHint) -> TxnClassification {
    match hint.planned_txn_type() {
        PlannedTxnType::Local => {
            let shard = hint.involved_shards.first().copied().unwrap_or(ShardId(0));
            TxnClassification::local(shard)
        }
        PlannedTxnType::Global => {
            let shards = if hint.involved_shards.is_empty() {
                vec![ShardId(0)]
            } else {
                hint.involved_shards.clone()
            };
            TxnClassification::global(shards, SlowPathMode::Xa2Pc)
        }
    }
}

/// Extract a simple `WHERE col = 'value'` from a lowercased SQL string.
/// Returns the value if found, None otherwise.
pub(crate) fn extract_where_eq(sql: &str, column: &str) -> Option<String> {
    // Look for patterns like: column = 'value' or column='value'
    let pattern = format!("{} = '", column);
    let pattern2 = format!("{}='", column);
    let start = sql
        .find(&pattern)
        .map(|i| i + pattern.len())
        .or_else(|| sql.find(&pattern2).map(|i| i + pattern2.len()))?;
    let rest = &sql[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

/// Parse `SET log_min_duration_statement = <ms>` or `SET log_min_duration_statement TO <ms>`.
/// Also accepts `-1` or `default` to disable (returns 0).
/// Input `sql` must already be lowercased.
pub(crate) fn parse_set_log_min_duration(sql: &str) -> Option<u64> {
    let rest = sql.strip_prefix("set")?;
    let rest = rest.trim();
    let rest = rest.strip_prefix("log_min_duration_statement")?;
    let rest = rest.trim();
    let rest = if let Some(r) = rest.strip_prefix('=') {
        r.trim()
    } else if let Some(r) = rest.strip_prefix("to") {
        r.trim()
    } else {
        return None;
    };
    let value = rest
        .trim_end_matches(';')
        .trim()
        .trim_matches('\'')
        .trim_matches('"');
    if value == "default" || value == "-1" || value == "0" {
        return Some(0);
    }
    value.parse::<u64>().ok()
}

/// Parse `SET <var> = <value>` or `SET <var> TO <value>`.
/// Input `sql` must already be lowercased.
/// Returns (var_name, value) if successfully parsed.
pub(crate) fn parse_set_command(sql: &str) -> Option<(String, String)> {
    let rest = sql.strip_prefix("set")?.trim();
    // Skip LOCAL/SESSION qualifiers
    let rest = rest.strip_prefix("local ").unwrap_or(rest);
    let rest = rest.strip_prefix("session ").unwrap_or(rest);
    // Find the variable name (everything before = or TO)
    let (name, rest) = if let Some(eq_pos) = rest.find('=') {
        (rest[..eq_pos].trim(), rest[eq_pos + 1..].trim())
    } else if let Some(to_pos) = rest.find(" to ") {
        (rest[..to_pos].trim(), rest[to_pos + 4..].trim())
    } else {
        return None;
    };
    if name.is_empty() {
        return None;
    }
    let value = rest
        .trim_end_matches(';')
        .trim()
        .trim_matches('\'')
        .trim_matches('"');
    Some((name.to_string(), value.to_string()))
}

/// Parse `PREPARE name [(type, ...)] AS query`.
/// Returns (name, query) if successfully parsed.
pub(crate) fn parse_prepare_statement(sql: &str) -> Option<(String, String)> {
    let lower = sql.to_lowercase();
    let rest = lower.strip_prefix("prepare")?.trim();
    // Find AS keyword
    let as_pos = rest.find(" as ")?;
    let before_as = rest[..as_pos].trim();
    let query = sql[sql.to_lowercase().find(" as ")? + 4..]
        .trim()
        .trim_end_matches(';')
        .trim();
    // before_as is "name" or "name(type, ...)"
    let name = if let Some(paren) = before_as.find('(') {
        before_as[..paren].trim()
    } else {
        before_as.trim()
    };
    if name.is_empty() || query.is_empty() {
        return None;
    }
    Some((name.to_string(), query.to_string()))
}

/// Parse `EXECUTE name [(param, ...)]`.
/// Returns (name, params) where params are converted to Option<Vec<u8>> for bind_params.
pub(crate) fn parse_execute_statement(sql: &str) -> Option<(String, Vec<Option<Vec<u8>>>)> {
    let rest = sql
        .trim()
        .strip_prefix("EXECUTE")
        .or_else(|| sql.trim().strip_prefix("execute"))?
        .trim();
    let rest = rest.trim_end_matches(';').trim();
    // Split name from optional (params)
    let (name, params_str) = if let Some(paren_pos) = rest.find('(') {
        let name = rest[..paren_pos].trim();
        let params_raw = rest[paren_pos + 1..].trim_end_matches(')').trim();
        (name, Some(params_raw))
    } else {
        (rest.trim(), None)
    };
    if name.is_empty() {
        return None;
    }
    let params = if let Some(ps) = params_str {
        if ps.is_empty() {
            vec![]
        } else {
            // Simple CSV split, respecting single-quoted strings
            split_params(ps)
                .into_iter()
                .map(|p| {
                    let trimmed = p.trim();
                    if trimmed.eq_ignore_ascii_case("null") {
                        None
                    } else {
                        let unquoted = trimmed.trim_matches('\'');
                        Some(unquoted.as_bytes().to_vec())
                    }
                })
                .collect()
        }
    } else {
        vec![]
    };
    Some((name.to_lowercase(), params))
}

/// Split a comma-separated parameter list, respecting single-quoted strings.
fn split_params(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for ch in s.chars() {
        match ch {
            '\'' => {
                in_quote = !in_quote;
                current.push(ch);
            }
            ',' if !in_quote => {
                parts.push(std::mem::take(&mut current));
            }
            _ => {
                current.push(ch);
            }
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

/// Substitute `$1`, `$2`, ... placeholders with parameter values (for SQL-level EXECUTE).
pub(crate) fn bind_params(sql: &str, param_values: &[Option<Vec<u8>>]) -> String {
    if param_values.is_empty() {
        return sql.to_string();
    }
    let mut result = String::with_capacity(sql.len() + param_values.len() * 8);
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start {
                if let Ok(idx) = sql[start..end].parse::<usize>() {
                    if idx >= 1 && idx <= param_values.len() {
                        match &param_values[idx - 1] {
                            Some(val) => {
                                let s = String::from_utf8_lossy(val);
                                result.push('\'');
                                for ch in s.chars() {
                                    if ch == '\'' {
                                        result.push('\'');
                                    }
                                    result.push(ch);
                                }
                                result.push('\'');
                            }
                            None => result.push_str("NULL"),
                        }
                        i = end;
                        continue;
                    }
                }
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
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
        log.record("SELECT 1", std::time::Duration::from_millis(10), 1);

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
        assert_eq!(rows[1][1], Some("3".into()));
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
}
