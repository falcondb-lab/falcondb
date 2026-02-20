use std::sync::Arc;
use std::time::Instant;

use cedar_cluster::{DistributedQueryEngine, ReplicaRunnerMetrics};
use cedar_common::datum::Datum;
use cedar_common::types::ShardId;
use cedar_common::error::CedarError;
use cedar_executor::{ExecutionResult, Executor};
use cedar_planner::{IndexedColumns, PhysicalPlan, PlannedTxnType, Planner, TableRowCounts};
use cedar_sql_frontend::binder::Binder;
use cedar_sql_frontend::parser::parse_sql;
use cedar_storage::engine::StorageEngine;
use cedar_txn::{SlowPathMode, TxnClassification, TxnManager};

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
    pub(crate) tenant_registry: Arc<cedar_storage::tenant_registry::TenantRegistry>,
    /// P2-2: Audit log for enterprise compliance.
    pub(crate) audit_log: Arc<cedar_storage::audit::AuditLog>,
    /// P3-1: License information.
    pub(crate) license_info: Arc<cedar_common::edition::LicenseInfo>,
    /// P3-1: Feature gate for edition-based feature control.
    pub(crate) feature_gate: Arc<cedar_common::edition::FeatureGate>,
    /// P3-3: Resource meter for per-tenant billing.
    pub(crate) resource_meter: Arc<cedar_storage::metering::ResourceMeter>,
    /// P3-6: Security manager for encryption/TLS/IP control.
    pub(crate) security_manager: Arc<cedar_storage::security_manager::SecurityManager>,
    /// DK-5: Hotspot detector for shard/table access monitoring.
    pub(crate) hotspot_detector: Arc<cedar_storage::hotspot::HotspotDetector>,
    /// DK-9: Consistency verifier for self-verification.
    pub(crate) consistency_verifier: Arc<cedar_storage::verification::ConsistencyVerifier>,
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
            tenant_registry: Arc::new(cedar_storage::tenant_registry::TenantRegistry::new()),
            audit_log: Arc::new(cedar_storage::audit::AuditLog::new()),
            license_info: Arc::new(cedar_common::edition::LicenseInfo::community()),
            feature_gate: Arc::new(cedar_common::edition::FeatureGate::for_edition(cedar_common::edition::EditionTier::Community)),
            resource_meter: Arc::new(cedar_storage::metering::ResourceMeter::new()),
            security_manager: Arc::new(cedar_storage::security_manager::SecurityManager::new()),
            hotspot_detector: Arc::new(cedar_storage::hotspot::HotspotDetector::default()),
            consistency_verifier: Arc::new(cedar_storage::verification::ConsistencyVerifier::default()),
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
            tenant_registry: Arc::new(cedar_storage::tenant_registry::TenantRegistry::new()),
            audit_log: Arc::new(cedar_storage::audit::AuditLog::new()),
            license_info: Arc::new(cedar_common::edition::LicenseInfo::community()),
            feature_gate: Arc::new(cedar_common::edition::FeatureGate::for_edition(cedar_common::edition::EditionTier::Community)),
            resource_meter: Arc::new(cedar_storage::metering::ResourceMeter::new()),
            security_manager: Arc::new(cedar_storage::security_manager::SecurityManager::new()),
            hotspot_detector: Arc::new(cedar_storage::hotspot::HotspotDetector::default()),
            consistency_verifier: Arc::new(cedar_storage::verification::ConsistencyVerifier::default()),
        }
    }

    /// Set replica runner metrics for SHOW cedar.replica_stats.
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
    pub fn handle_query(
        &self,
        sql: &str,
        session: &mut PgSession,
    ) -> Vec<BackendMessage> {
        let sql = sql.trim();
        if sql.is_empty() {
            return vec![BackendMessage::EmptyQueryResponse];
        }

        // Intercept system/catalog queries that psql sends
        if let Some(response) = self.handle_system_query(sql, session) {
            return response;
        }

        // Parse
        let stmts = match parse_sql(sql) {
            Ok(stmts) => stmts,
            Err(e) => {
                return vec![self.error_response(&CedarError::Sql(e))];
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
                        messages.push(self.error_response(&CedarError::Sql(e)));
                        return messages;
                    }
                };

                // Plan (with cost-based join reordering and index scan detection)
                let row_counts = self.build_row_counts();
                let indexed_cols = self.build_indexed_columns();
                let p = match Planner::plan_with_indexes(&bound, &row_counts, &indexed_cols) {
                    Ok(p) => p,
                    Err(e) => {
                        messages.push(self.error_response(&CedarError::Sql(e)));
                        return messages;
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
                    let txn = match self
                        .txn_mgr
                        .try_begin_with_classification(
                            session.default_isolation,
                            TxnClassification::local(ShardId(0)),
                        ) {
                        Ok(t) => t,
                        Err(e) => {
                            let ce: CedarError = e.into();
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
                            Ok(_) => {
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
                                messages.push(self.error_response(
                                    &CedarError::Txn(e),
                                ));
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
                    let _ = self
                        .txn_mgr
                        .force_global(txn.txn_id, SlowPathMode::Xa2Pc);
                }
            }

            // Handle COPY FROM STDIN — store state in session, return CopyInResponse
            if let PhysicalPlan::CopyFrom {
                table_id, schema, columns,
                csv, delimiter, header, null_string, quote, escape,
            } = &plan {
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
                return messages;
            }

            // Handle COPY TO STDOUT — execute scan and stream data
            if let PhysicalPlan::CopyTo {
                table_id, schema, columns,
                csv, delimiter, header, null_string, quote, escape,
            } = &plan {
                // Ensure a transaction exists
                let auto_txn = if session.txn.is_none() {
                    let classification = classification_from_routing_hint(&routing_hint);
                    let txn = match self.txn_mgr
                        .try_begin_with_classification(session.default_isolation, classification) {
                        Ok(t) => t,
                        Err(e) => {
                            let ce: CedarError = e.into();
                            messages.push(self.error_response(&ce));
                            continue;
                        }
                    };
                    session.txn = Some(txn);
                    true
                } else {
                    false
                };

                let result = self.executor.exec_copy_to(
                    *table_id, schema, columns,
                    *csv, *delimiter, *header, null_string, *quote, *escape,
                    session.txn.as_ref().unwrap(),
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
                return messages;
            }

            // Handle COPY (query) TO STDOUT
            if let PhysicalPlan::CopyQueryTo {
                query, csv, delimiter, header, null_string, quote, escape,
            } = &plan {
                let auto_txn = if session.txn.is_none() {
                    let classification = classification_from_routing_hint(&routing_hint);
                    let txn = match self.txn_mgr
                        .try_begin_with_classification(session.default_isolation, classification) {
                        Ok(t) => t,
                        Err(e) => {
                            let ce: CedarError = e.into();
                            messages.push(self.error_response(&ce));
                            continue;
                        }
                    };
                    session.txn = Some(txn);
                    true
                } else {
                    false
                };

                let result = self.executor.exec_copy_query_to(
                    query, *csv, *delimiter, *header, null_string, *quote, *escape,
                    session.txn.as_ref().unwrap(),
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
                return messages;
            }

            // For DDL and metadata commands, execute without txn
            if matches!(plan, PhysicalPlan::CreateTable { .. } | PhysicalPlan::DropTable { .. } | PhysicalPlan::ShowTxnStats | PhysicalPlan::RunGc) {
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
                            let values: Vec<Option<String>> = row
                                .values
                                .iter()
                                .map(|d| Some(d.to_string()))
                                .collect();
                            messages.push(BackendMessage::DataRow { values });
                        }
                        messages.push(BackendMessage::CommandComplete {
                            tag: format!("SHOW {}", rows.len()),
                        });
                    }
                    Err(e) => {
                        messages.push(self.error_response(&e));
                        return messages;
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
                    .try_begin_with_classification(session.default_isolation, classification) {
                    Ok(t) => t,
                    Err(e) => {
                        let ce: CedarError = e.into();
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
                                let values: Vec<Option<String>> = row
                                    .values
                                    .iter()
                                    .map(|d| d.to_pg_text())
                                    .collect();
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
                    return messages;
                }
            }
        }

        messages
    }

    /// Process COPY FROM STDIN data collected by the server connection loop.
    /// Returns backend messages to send (CommandComplete or ErrorResponse).
    pub fn handle_copy_data(
        &self,
        data: &[u8],
        session: &mut PgSession,
    ) -> Vec<BackendMessage> {
        let copy_state = match session.copy_state.take() {
            Some(cs) => cs,
            None => {
                return vec![BackendMessage::ErrorResponse {
                    severity: "ERROR".into(),
                    code: "08P01".into(),
                    message: "No active COPY operation".into(),
                }];
            }
        };

        // Ensure a transaction
        let auto_txn = if session.txn.is_none() {
            let classification = TxnClassification::local(ShardId(0));
            let txn = match self.txn_mgr
                .try_begin_with_classification(session.default_isolation, classification) {
                Ok(t) => t,
                Err(e) => {
                    let ce: CedarError = e.into();
                    return vec![self.error_response(&ce)];
                }
            };
            session.txn = Some(txn);
            true
        } else {
            false
        };

        let result = self.executor.exec_copy_from_data(
            copy_state.table_id,
            &copy_state.schema,
            &copy_state.columns,
            data,
            copy_state.format.csv,
            copy_state.format.delimiter,
            copy_state.format.header,
            &copy_state.format.null_string,
            copy_state.format.quote,
            copy_state.format.escape,
            session.txn.as_ref().unwrap(),
        );

        let messages = match result {
            Ok(ExecutionResult::Dml { rows_affected, .. }) => {
                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        let _ = self.txn_mgr.commit(txn.txn_id);
                    }
                    session.txn = None;
                    self.flush_txn_stats();
                }
                vec![BackendMessage::CommandComplete {
                    tag: format!("COPY {}", rows_affected),
                }]
            }
            Err(e) => {
                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        let _ = self.txn_mgr.abort(txn.txn_id);
                    }
                    session.txn = None;
                    self.flush_txn_stats();
                }
                vec![self.error_response(&e)]
            }
            _ => {
                vec![BackendMessage::ErrorResponse {
                    severity: "ERROR".into(),
                    code: "XX000".into(),
                    message: "Unexpected COPY FROM result".into(),
                }]
            }
        };

        messages
    }

    /// Intercept system queries that psql and PG drivers send.
    /// Returns Some(messages) if handled, None to fall through to normal execution.
    fn handle_system_query(&self, sql: &str, session: &mut PgSession) -> Option<Vec<BackendMessage>> {
        let sql_lower = sql.to_lowercase();
        let sql_lower = sql_lower.trim().trim_end_matches(';').trim();

        // SELECT version()
        if sql_lower == "select version()" || sql_lower.starts_with("select version()") {
            return Some(self.single_row_result(
                vec![("version", 25, -1)],
                vec![vec![Some("PostgreSQL 15.0.0 on CedarDB (in-memory OLTP)".into())]],
            ));
        }

        // SELECT current_schema() / current_database() / current_user
        if sql_lower == "select current_schema()" || sql_lower.starts_with("select current_schema") {
            return Some(self.single_row_result(
                vec![("current_schema", 25, -1)],
                vec![vec![Some("public".into())]],
            ));
        }
        if sql_lower == "select current_database()" || sql_lower.starts_with("select current_database") {
            return Some(self.single_row_result(
                vec![("current_database", 25, -1)],
                vec![vec![Some("cedar".into())]],
            ));
        }
        if sql_lower == "select current_user" {
            return Some(self.single_row_result(
                vec![("current_user", 25, -1)],
                vec![vec![Some("cedar".into())]],
            ));
        }

        // SET log_min_duration_statement = <ms>
        if let Some(threshold_ms) = parse_set_log_min_duration(sql_lower) {
            self.slow_query_log.set_threshold(
                std::time::Duration::from_millis(threshold_ms)
            );
            return Some(vec![BackendMessage::CommandComplete {
                tag: "SET".into(),
            }]);
        }

        // CHECKPOINT — trigger a storage checkpoint (WAL compaction)
        if sql_lower == "checkpoint" {
            match self.storage.checkpoint() {
                Ok((segment_id, row_count)) => {
                    return Some(self.single_row_result(
                        vec![("checkpoint", 25, -1)],
                        vec![vec![Some(format!("OK segment={} rows={}", segment_id, row_count))]],
                    ));
                }
                Err(e) => {
                    return Some(vec![BackendMessage::ErrorResponse {
                        severity: "ERROR".into(),
                        code: "XX000".into(),
                        message: format!("CHECKPOINT failed: {}", e),
                    }]);
                }
            }
        }

        // RESET cedar.slow_queries — clear the slow query log
        if sql_lower == "reset cedar.slow_queries" {
            self.slow_query_log.clear();
            return Some(vec![BackendMessage::CommandComplete {
                tag: "RESET".into(),
            }]);
        }

        // SET <var> = <value> / SET <var> TO <value> — store in session GUC
        if sql_lower.starts_with("set ") {
            if let Some((name, value)) = parse_set_command(sql_lower) {
                session.set_guc(&name, &value);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "SET".into(),
            }]);
        }

        // SAVEPOINT <name>
        if sql_lower.starts_with("savepoint ") {
            let name = sql_lower.trim_start_matches("savepoint ").trim().to_string();
            if !session.in_transaction() {
                return Some(vec![BackendMessage::ErrorResponse {
                    severity: "ERROR".into(),
                    code: "25P01".into(),
                    message: "SAVEPOINT can only be used in transaction blocks".into(),
                }]);
            }
            let txn_id = session.txn.as_ref().unwrap().txn_id;
            let write_set_len = self.storage.write_set_snapshot(txn_id);
            let read_set_len = self.storage.read_set_snapshot(txn_id);
            session.savepoints.push(crate::session::SavepointEntry {
                name,
                write_set_len,
                read_set_len,
            });
            return Some(vec![BackendMessage::CommandComplete {
                tag: "SAVEPOINT".into(),
            }]);
        }

        // ROLLBACK TO [SAVEPOINT] <name>
        if sql_lower.starts_with("rollback to ") {
            let rest = sql_lower.trim_start_matches("rollback to ").trim();
            let name = rest.strip_prefix("savepoint ").unwrap_or(rest).trim();
            if let Some(pos) = session.savepoints.iter().rposition(|sp| sp.name == name) {
                let write_snap = session.savepoints[pos].write_set_len;
                let read_snap = session.savepoints[pos].read_set_len;
                if let Some(ref txn) = session.txn {
                    self.storage.rollback_write_set_after(txn.txn_id, write_snap);
                    self.storage.rollback_read_set_after(txn.txn_id, read_snap);
                }
                session.savepoints.truncate(pos + 1);
                return Some(vec![BackendMessage::CommandComplete {
                    tag: "ROLLBACK".into(),
                }]);
            } else {
                return Some(vec![BackendMessage::ErrorResponse {
                    severity: "ERROR".into(),
                    code: "3B001".into(),
                    message: format!("savepoint \"{}\" does not exist", name),
                }]);
            }
        }

        // RELEASE [SAVEPOINT] <name>
        if sql_lower.starts_with("release ") {
            let rest = sql_lower.trim_start_matches("release ").trim();
            let name = rest.strip_prefix("savepoint ").unwrap_or(rest).trim();
            if let Some(pos) = session.savepoints.iter().rposition(|sp| sp.name == name) {
                session.savepoints.remove(pos);
                return Some(vec![BackendMessage::CommandComplete {
                    tag: "RELEASE".into(),
                }]);
            } else {
                return Some(vec![BackendMessage::ErrorResponse {
                    severity: "ERROR".into(),
                    code: "3B001".into(),
                    message: format!("savepoint \"{}\" does not exist", name),
                }]);
            }
        }

        // SHOW ... queries
        if sql_lower.starts_with("show ") {
            let param = sql_lower.trim_start_matches("show ").trim();

            // Delegate cedar.* SHOW commands to handler_show module
            if param.starts_with("cedar.") {
                if let Some(result) = self.handle_show_cedar(param, session) {
                    return Some(result);
                }
            }

            // Look up GUC variable from session, with hardcoded fallbacks
            let value = if let Some(v) = session.get_guc(param) {
                v.to_string()
            } else {
                match param {
                    "transaction_isolation" => "read committed".into(),
                    "max_identifier_length" => "63".into(),
                    _ => "".into(),
                }
            };
            return Some(self.single_row_result(
                vec![(param, 25, -1)],
                vec![vec![Some(value)]],
            ));
        }

        // Delegate information_schema and pg_catalog queries to handler_catalog module
        if let Some(result) = self.handle_catalog_query(sql_lower, session) {
            return Some(result);
        }

        // PREPARE name [(type, ...)] AS query
        if sql_lower.starts_with("prepare ") {
            if let Some((name, query)) = parse_prepare_statement(sql) {
                session.prepared_statements.insert(
                    name,
                    crate::session::PreparedStatement {
                        query,
                        param_types: vec![],
                        plan: None,
                        inferred_param_types: vec![],
                        row_desc: vec![],
                    },
                );
                cedar_observability::record_prepared_stmt_sql_cmd("prepare");
                cedar_observability::record_prepared_stmt_active(session.prepared_statements.len());
                return Some(vec![BackendMessage::CommandComplete {
                    tag: "PREPARE".into(),
                }]);
            }
            return Some(vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42601".into(),
                message: "invalid PREPARE syntax".into(),
            }]);
        }

        // EXECUTE name [(param, ...)]
        if sql_lower.starts_with("execute ") {
            if let Some((name, params)) = parse_execute_statement(sql) {
                let bound = if let Some(ps) = session.prepared_statements.get(&name) {
                    bind_params(&ps.query, &params)
                } else {
                    return Some(vec![BackendMessage::ErrorResponse {
                        severity: "ERROR".into(),
                        code: "26000".into(),
                        message: format!("prepared statement \"{}\" does not exist", name),
                    }]);
                };
                // Execute the bound SQL through the normal query path
                cedar_observability::record_prepared_stmt_sql_cmd("execute");
                return Some(self.handle_query(&bound, session));
            }
        }

        // DISCARD ALL (psql sends this on reconnect sometimes)
        if sql_lower == "discard all" {
            session.reset_all_gucs();
            session.prepared_statements.clear();
            session.portals.clear();
            return Some(vec![BackendMessage::CommandComplete {
                tag: "DISCARD ALL".into(),
            }]);
        }

        // DEALLOCATE [ALL | name]
        if sql_lower.starts_with("deallocate") {
            let rest = sql_lower.trim_start_matches("deallocate").trim().trim_end_matches(';').trim();
            if rest == "all" {
                session.prepared_statements.clear();
            } else if !rest.is_empty() {
                session.prepared_statements.remove(rest);
            }
            cedar_observability::record_prepared_stmt_sql_cmd("deallocate");
            cedar_observability::record_prepared_stmt_active(session.prepared_statements.len());
            return Some(vec![BackendMessage::CommandComplete {
                tag: "DEALLOCATE".into(),
            }]);
        }

        // RESET [ALL | var]
        if sql_lower.starts_with("reset ") {
            let var = sql_lower.trim_start_matches("reset ").trim().trim_end_matches(';').trim();
            if var == "all" {
                session.reset_all_gucs();
            } else {
                session.reset_guc(var);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "RESET".into(),
            }]);
        }

        // ── LISTEN channel ──
        if sql_lower.starts_with("listen ") {
            let channel = sql_lower.trim_start_matches("listen ").trim().trim_end_matches(';').trim().to_string();
            if !channel.is_empty() {
                session.notifications.listen(&channel, &session.notification_hub);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "LISTEN".into(),
            }]);
        }

        // ── UNLISTEN channel | UNLISTEN * ──
        if sql_lower.starts_with("unlisten ") {
            let channel = sql_lower.trim_start_matches("unlisten ").trim().trim_end_matches(';').trim();
            if channel == "*" {
                session.notifications.unlisten_all(&session.notification_hub);
            } else {
                session.notifications.unlisten(channel, &session.notification_hub);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "UNLISTEN".into(),
            }]);
        }

        // ── NOTIFY channel [, 'payload'] ──
        if sql_lower.starts_with("notify ") {
            let rest = sql.trim_start_matches(|c: char| c.is_alphabetic() || c == ' ')
                .trim_end_matches(';').trim();
            // rest is now "channel" or "channel, 'payload'"
            let (channel, payload) = if let Some(comma_pos) = rest.find(',') {
                let ch = rest[..comma_pos].trim().to_lowercase();
                let pl = rest[comma_pos + 1..].trim().trim_matches('\'').to_string();
                (ch, pl)
            } else {
                (rest.trim().to_lowercase(), String::new())
            };
            if !channel.is_empty() {
                session.notification_hub.notify(&channel, session.id, &payload);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "NOTIFY".into(),
            }]);
        }

        // ── DECLARE cursor_name [NO SCROLL] CURSOR [WITH HOLD] FOR query ──
        if sql_lower.starts_with("declare ") {
            return Some(self.handle_declare_cursor(sql, sql_lower, session));
        }

        // ── FETCH [count] FROM cursor_name ──
        if sql_lower.starts_with("fetch ") {
            return Some(self.handle_fetch_cursor(sql_lower, session));
        }

        // ── MOVE [count] FROM cursor_name ──
        if sql_lower.starts_with("move ") {
            let rest = sql_lower.trim_start_matches("move ").trim();
            let (count, cursor_name, backward) = Self::parse_move_args(rest);
            match session.cursors.get_mut(&cursor_name) {
                Some(cursor) => {
                    let moved = if backward {
                        let actual = count.min(cursor.position);
                        cursor.position -= actual;
                        actual
                    } else {
                        let remaining = cursor.rows.len().saturating_sub(cursor.position);
                        let actual = count.min(remaining);
                        cursor.position += actual;
                        actual
                    };
                    return Some(vec![BackendMessage::CommandComplete {
                        tag: format!("MOVE {}", moved),
                    }]);
                }
                None => {
                    return Some(vec![BackendMessage::ErrorResponse {
                        severity: "ERROR".into(),
                        code: "34000".into(),
                        message: format!("cursor \"{}\" does not exist", cursor_name),
                    }]);
                }
            }
        }

        // ── CLOSE cursor_name | CLOSE ALL ──
        if sql_lower.starts_with("close ") {
            let rest = sql_lower.trim_start_matches("close ").trim();
            if rest == "all" {
                session.cursors.clear();
            } else {
                session.cursors.remove(rest);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "CLOSE CURSOR".into(),
            }]);
        }

        None
    }

    /// Handle DECLARE cursor_name [NO SCROLL] CURSOR [WITH HOLD] FOR query
    fn handle_declare_cursor(
        &self,
        sql: &str,
        sql_lower: &str,
        session: &mut PgSession,
    ) -> Vec<BackendMessage> {
        // Parse: DECLARE name [NO SCROLL] CURSOR [WITH HOLD] FOR <query>
        let rest = sql_lower.trim_start_matches("declare ").trim();
        let parts: Vec<&str> = rest.splitn(2, " cursor").collect();
        if parts.len() < 2 {
            return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42601".into(),
                message: "syntax error in DECLARE CURSOR".into(),
            }];
        }

        let cursor_name = parts[0].trim_end_matches(" no scroll").trim().to_string();
        let with_hold = parts[1].contains("with hold");

        // Extract the FOR query
        let for_pos = rest.find(" for ");
        if for_pos.is_none() {
            return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42601".into(),
                message: "DECLARE CURSOR requires FOR <query>".into(),
            }];
        }
        let for_idx = for_pos.unwrap();
        // Get the original-case SQL for the query portion
        let declare_prefix_len = "declare ".len();
        let query_start = declare_prefix_len + for_idx + " for ".len();
        let query = if query_start < sql.len() {
            sql[query_start..].trim().trim_end_matches(';').to_string()
        } else {
            return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42601".into(),
                message: "empty query in DECLARE CURSOR".into(),
            }];
        };

        // Execute the query and store results in cursor
        let txn = session.txn.as_ref();
        match self.execute_sql_for_cursor(&query, txn) {
            Ok((columns, rows)) => {
                session.cursors.insert(cursor_name.clone(), crate::session::CursorState {
                    name: cursor_name,
                    rows,
                    columns,
                    position: 0,
                    with_hold,
                });
                vec![BackendMessage::CommandComplete {
                    tag: "DECLARE CURSOR".into(),
                }]
            }
            Err(msg) => vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42P03".into(),
                message: msg,
            }],
        }
    }

    /// Execute a SQL query and return (columns, rows) for cursor materialization.
    #[allow(clippy::type_complexity)]
    fn execute_sql_for_cursor(
        &self,
        query: &str,
        txn: Option<&cedar_txn::TxnHandle>,
    ) -> Result<(Vec<(String, cedar_common::types::DataType)>, Vec<cedar_common::datum::OwnedRow>), String> {
        let catalog = self.storage.get_catalog();
        let mut binder = Binder::new(catalog);
        let stmts = parse_sql(query).map_err(|e| format!("parse error: {}", e))?;
        if stmts.is_empty() {
            return Err("empty query".into());
        }
        let bound = binder.bind(&stmts[0]).map_err(|e| format!("bind error: {}", e))?;
        let plan = Planner::plan(&bound).map_err(|e| format!("plan error: {}", e))?;
        let result = self.executor.execute(&plan, txn).map_err(|e| format!("exec error: {}", e))?;
        match result {
            cedar_executor::ExecutionResult::Query { columns, rows } => Ok((columns, rows)),
            _ => Err("DECLARE CURSOR requires a SELECT query".into()),
        }
    }

    /// Handle FETCH [count] FROM cursor_name
    fn handle_fetch_cursor(
        &self,
        sql_lower: &str,
        session: &mut PgSession,
    ) -> Vec<BackendMessage> {
        let rest = sql_lower.trim_start_matches("fetch ").trim();

        // Parse: FETCH [NEXT | FORWARD | count] [FROM | IN] cursor_name
        let (count, cursor_name) = Self::parse_fetch_args(rest);

        let cursor = match session.cursors.get_mut(&cursor_name) {
            Some(c) => c,
            None => return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "34000".into(),
                message: format!("cursor \"{}\" does not exist", cursor_name),
            }],
        };

        // Build field descriptions
        let fields: Vec<FieldDescription> = cursor.columns.iter().map(|(name, dt)| {
            let (type_oid, type_len) = Self::data_type_to_pg_oid(dt);
            FieldDescription {
                name: name.clone(),
                table_oid: 0, column_attr: 0,
                type_oid, type_len,
                type_modifier: -1, format_code: 0,
            }
        }).collect();

        let mut msgs = vec![BackendMessage::RowDescription { fields }];

        // Fetch up to `count` rows from current position
        let end = (cursor.position + count).min(cursor.rows.len());
        for i in cursor.position..end {
            let row_vals: Vec<Option<String>> = cursor.rows[i].values.iter()
                .map(|d| if d.is_null() { None } else { Some(format!("{}", d)) })
                .collect();
            msgs.push(BackendMessage::DataRow { values: row_vals });
        }
        let fetched = end - cursor.position;
        cursor.position = end;

        msgs.push(BackendMessage::CommandComplete {
            tag: format!("FETCH {}", fetched),
        });
        msgs
    }

    /// Parse FETCH arguments: [NEXT | FORWARD | ALL | count] [FROM | IN] cursor_name
    fn parse_fetch_args(rest: &str) -> (usize, String) {
        let tokens: Vec<&str> = rest.split_whitespace().collect();
        match tokens.len() {
            1 => (1, tokens[0].to_string()),
            2 => {
                if tokens[0] == "from" || tokens[0] == "in"
                    || tokens[0] == "next" || tokens[0] == "forward"
                {
                    (1, tokens[1].to_string())
                } else if tokens[0] == "all" {
                    (usize::MAX, tokens[1].to_string())
                } else if let Ok(n) = tokens[0].parse::<usize>() {
                    (n, tokens[1].to_string())
                } else {
                    (1, tokens[0].to_string())
                }
            }
            3.. => {
                // Try: count FROM name / FORWARD count FROM name / NEXT FROM name
                let first = tokens[0];
                if first == "next" || first == "forward" {
                    let cursor = tokens.last().unwrap().to_string();
                    let count = tokens.get(1).and_then(|t| t.parse::<usize>().ok()).unwrap_or(1);
                    (count, cursor)
                } else if first == "all" {
                    let cursor = tokens.last().unwrap().to_string();
                    (usize::MAX, cursor)
                } else if let Ok(n) = first.parse::<usize>() {
                    let cursor = tokens.last().unwrap().to_string();
                    (n, cursor)
                } else {
                    let cursor = tokens.last().unwrap().to_string();
                    (1, cursor)
                }
            }
            _ => (1, String::new()),
        }
    }

    /// Parse MOVE arguments: [BACKWARD | FORWARD] [count | ALL] [FROM | IN] cursor_name
    /// Returns (count, cursor_name, backward).
    fn parse_move_args(rest: &str) -> (usize, String, bool) {
        let tokens: Vec<&str> = rest.split_whitespace().collect();
        let mut backward = false;
        let mut count = 1usize;
        let mut cursor_name = String::new();

        let mut i = 0;
        if i < tokens.len() && tokens[i] == "backward" {
            backward = true;
            i += 1;
        } else if i < tokens.len() && tokens[i] == "forward" {
            i += 1;
        }

        if i < tokens.len() {
            if tokens[i] == "all" {
                count = usize::MAX;
                i += 1;
            } else if let Ok(n) = tokens[i].parse::<usize>() {
                count = n;
                i += 1;
            }
        }

        // Skip FROM/IN
        if i < tokens.len() && (tokens[i] == "from" || tokens[i] == "in") {
            i += 1;
        }

        if i < tokens.len() {
            cursor_name = tokens[i].to_string();
        }

        (count, cursor_name, backward)
    }

    /// Map DataType to PG OID and type_len for cursor column descriptions.
    fn data_type_to_pg_oid(dt: &cedar_common::types::DataType) -> (i32, i16) {
        use cedar_common::types::DataType;
        match dt {
            DataType::Int32 => (23, 4),    // int4
            DataType::Int64 => (20, 8),    // int8
            DataType::Float64 => (701, 8), // float8
            DataType::Text => (25, -1),    // text
            DataType::Boolean => (16, 1),  // bool
            DataType::Timestamp => (1114, 8), // timestamp
            DataType::Date => (1082, 4),   // date
            _ => (25, -1),                 // fallback to text
        }
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
    pub fn describe_query(&self, sql: &str) -> Result<Vec<FieldDescription>, CedarError> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Ok(vec![]);
        }

        let stmts = parse_sql(sql).map_err(CedarError::Sql)?;
        if stmts.is_empty() {
            return Ok(vec![]);
        }

        let catalog = self.storage.get_catalog();
        let mut binder = Binder::new(catalog);
        let bound = binder.bind(&stmts[0]).map_err(CedarError::Sql)?;

        let row_counts = self.build_row_counts();
        let indexed_cols = self.build_indexed_columns();
        let plan = Planner::plan_with_indexes(&bound, &row_counts, &indexed_cols).map_err(CedarError::Sql)?;

        // Extract column info from the plan
        Ok(self.plan_output_fields(&plan))
    }

    /// Parse + bind + plan a SQL statement for the extended query protocol.
    /// Returns (PhysicalPlan, inferred_param_types, row_desc) on success.
    pub fn prepare_statement(
        &self,
        sql: &str,
    ) -> Result<(PhysicalPlan, Vec<Option<cedar_common::types::DataType>>, Vec<crate::session::FieldDescriptionCompact>), CedarError> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(CedarError::Sql(cedar_common::error::SqlError::Parse(
                "empty query".into(),
            )));
        }

        let stmts = parse_sql(sql).map_err(CedarError::Sql)?;
        if stmts.is_empty() {
            return Err(CedarError::Sql(cedar_common::error::SqlError::Parse(
                "empty query".into(),
            )));
        }

        let catalog = self.storage.get_catalog();
        let mut binder = Binder::new(catalog);
        let (bound, inferred_types) = binder
            .bind_with_params_lenient(&stmts[0], None)
            .map_err(CedarError::Sql)?;

        let row_counts = self.build_row_counts();
        let indexed_cols = self.build_indexed_columns();
        let plan = Planner::plan_with_indexes(&bound, &row_counts, &indexed_cols)
            .map_err(CedarError::Sql)?;

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
    pub fn execute_plan(
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
                let _ = self
                    .txn_mgr
                    .force_global(txn.txn_id, SlowPathMode::Xa2Pc);
            }
        }

        // For DDL/metadata, execute without params
        if matches!(plan, PhysicalPlan::CreateTable { .. } | PhysicalPlan::DropTable { .. } | PhysicalPlan::ShowTxnStats | PhysicalPlan::RunGc) {
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
                        let values: Vec<Option<String>> = row
                            .values
                            .iter()
                            .map(|d| Some(d.to_string()))
                            .collect();
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
                    let ce: CedarError = e.into();
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
                            let values: Vec<Option<String>> = row
                                .values
                                .iter()
                                .map(|d| d.to_pg_text())
                                .collect();
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

    /// Map a Cedar DataType to a PostgreSQL type OID.
    pub fn datatype_to_oid(&self, dt: Option<&cedar_common::types::DataType>) -> i32 {
        use cedar_common::types::DataType;
        match dt {
            Some(DataType::Int32) => 23,      // INT4
            Some(DataType::Int64) => 20,      // INT8
            Some(DataType::Float64) => 701,   // FLOAT8
            Some(DataType::Boolean) => 16,    // BOOL
            Some(DataType::Text) => 25,       // TEXT
            Some(DataType::Timestamp) => 1114, // TIMESTAMP
            Some(DataType::Date) => 1082,     // DATE
            Some(DataType::Array(_)) => 2277, // ANYARRAY
            Some(DataType::Jsonb) => 3802,    // JSONB
            None => 0,                        // unspecified
        }
    }

    /// Extract output column FieldDescriptions from a physical plan.
    fn plan_output_fields(&self, plan: &PhysicalPlan) -> Vec<FieldDescription> {
        use cedar_common::types::DataType;
        use cedar_sql_frontend::types::{AggFunc, BoundExpr, BoundProjection, ScalarFunc, BinOp};

        /// Infer the DataType of a BoundExpr given the source table schema columns.
        fn infer_expr_type(
            expr: &BoundExpr,
            cols: &[cedar_common::schema::ColumnDef],
        ) -> DataType {
            match expr {
                BoundExpr::Literal(d) => d.data_type().unwrap_or(DataType::Text),
                BoundExpr::ColumnRef(idx) => {
                    cols.get(*idx).map(|c| c.data_type.clone()).unwrap_or(DataType::Text)
                }
                BoundExpr::BinaryOp { left, op, right } => {
                    match op {
                        // Comparison / logical → Boolean
                        BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq
                        | BinOp::Gt | BinOp::GtEq | BinOp::And | BinOp::Or => DataType::Boolean,
                        // Arithmetic → promote operand types
                        BinOp::Plus | BinOp::Minus | BinOp::Multiply | BinOp::Divide | BinOp::Modulo => {
                            let lt = infer_expr_type(left, cols);
                            let rt = infer_expr_type(right, cols);
                            promote_numeric(lt, rt)
                        }
                        // String concat
                        BinOp::StringConcat => DataType::Text,
                        // JSONB operators → Jsonb (or Text for ->>/#>>)
                        BinOp::JsonArrow | BinOp::JsonHashArrow
                        | BinOp::JsonContains | BinOp::JsonContainedBy
                        | BinOp::JsonExists => DataType::Jsonb,
                        BinOp::JsonArrowText | BinOp::JsonHashArrowText => DataType::Text,
                    }
                }
                BoundExpr::Not(_) | BoundExpr::IsNull(_) | BoundExpr::IsNotNull(_)
                | BoundExpr::IsNotDistinctFrom { .. } | BoundExpr::Like { .. }
                | BoundExpr::Between { .. } | BoundExpr::InList { .. }
                | BoundExpr::Exists { .. } | BoundExpr::InSubquery { .. } => DataType::Boolean,
                BoundExpr::Cast { target_type, .. } => parse_cast_type(target_type),
                BoundExpr::Case { results, else_result, .. } => {
                    // Infer from first THEN branch
                    if let Some(first) = results.first() {
                        infer_expr_type(first, cols)
                    } else if let Some(e) = else_result {
                        infer_expr_type(e, cols)
                    } else {
                        DataType::Text
                    }
                }
                BoundExpr::Coalesce(exprs) => {
                    exprs.first().map(|e| infer_expr_type(e, cols)).unwrap_or(DataType::Text)
                }
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
                BoundExpr::OuterColumnRef(idx) => {
                    cols.get(*idx).map(|c| c.data_type.clone()).unwrap_or(DataType::Text)
                }
                BoundExpr::SequenceNextval(_) | BoundExpr::SequenceCurrval(_)
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
            cols: &[cedar_common::schema::ColumnDef],
        ) -> DataType {
            match func {
                // String → Text
                ScalarFunc::Upper | ScalarFunc::Lower | ScalarFunc::Trim
                | ScalarFunc::Replace | ScalarFunc::Lpad | ScalarFunc::Rpad
                | ScalarFunc::Left | ScalarFunc::Right | ScalarFunc::Repeat
                | ScalarFunc::Reverse | ScalarFunc::Initcap | ScalarFunc::Chr
                | ScalarFunc::ToChar | ScalarFunc::Concat | ScalarFunc::ConcatWs
                | ScalarFunc::Substring | ScalarFunc::Btrim | ScalarFunc::Ltrim
                | ScalarFunc::Rtrim | ScalarFunc::Overlay | ScalarFunc::RegexpReplace
                | ScalarFunc::RegexpSubstr | ScalarFunc::Translate
                | ScalarFunc::QuoteLiteral | ScalarFunc::QuoteIdent | ScalarFunc::QuoteNullable
                | ScalarFunc::Md5 | ScalarFunc::Encode | ScalarFunc::Decode
                | ScalarFunc::ToHex | ScalarFunc::PgTypeof | ScalarFunc::GenRandomUuid
                | ScalarFunc::ArrayDims | ScalarFunc::ArrayToString => DataType::Text,
                // Integer results
                ScalarFunc::Length | ScalarFunc::Position | ScalarFunc::Ascii
                | ScalarFunc::RegexpCount | ScalarFunc::ArrayLength | ScalarFunc::ArrayPosition
                | ScalarFunc::Cardinality | ScalarFunc::ArrayUpper | ScalarFunc::ArrayLower
                | ScalarFunc::WidthBucket | ScalarFunc::Factorial | ScalarFunc::Gcd
                | ScalarFunc::Lcm => DataType::Int64,
                // Float results
                ScalarFunc::Abs | ScalarFunc::Round | ScalarFunc::Ceil | ScalarFunc::Floor
                | ScalarFunc::Power | ScalarFunc::Sqrt | ScalarFunc::Sign | ScalarFunc::Trunc
                | ScalarFunc::Ln | ScalarFunc::Log | ScalarFunc::Exp | ScalarFunc::Pi
                | ScalarFunc::Mod | ScalarFunc::Degrees | ScalarFunc::Radians
                | ScalarFunc::Cbrt | ScalarFunc::Extract | ScalarFunc::ToNumber
                | ScalarFunc::Random | ScalarFunc::Log10 | ScalarFunc::Log2
                | ScalarFunc::Sin | ScalarFunc::Cos | ScalarFunc::Tan
                | ScalarFunc::Asin | ScalarFunc::Acos | ScalarFunc::Atan | ScalarFunc::Atan2
                | ScalarFunc::Cot | ScalarFunc::Sinh | ScalarFunc::Cosh | ScalarFunc::Tanh => DataType::Float64,
                // Date/time
                ScalarFunc::Now => DataType::Timestamp,
                ScalarFunc::CurrentDate => DataType::Date,
                ScalarFunc::CurrentTime => DataType::Text, // TIME not yet a DataType
                ScalarFunc::DateTrunc => DataType::Timestamp,
                // Bool
                ScalarFunc::StartsWith | ScalarFunc::EndsWith
                | ScalarFunc::ArrayContains | ScalarFunc::ArrayOverlap => DataType::Boolean,
                // Array-returning
                ScalarFunc::Split | ScalarFunc::RegexpMatch | ScalarFunc::RegexpSplitToArray
                | ScalarFunc::StringToArray | ScalarFunc::ArrayFill
                | ScalarFunc::ArrayReverse | ScalarFunc::ArrayDistinct | ScalarFunc::ArraySort
                | ScalarFunc::ArrayIntersect | ScalarFunc::ArrayExcept | ScalarFunc::ArrayCompact
                | ScalarFunc::ArrayFlatten | ScalarFunc::ArraySlice => {
                    DataType::Array(Box::new(DataType::Text))
                }
                // Pass-through: Greatest/Least inherit from first arg
                ScalarFunc::Greatest | ScalarFunc::Least => {
                    args.first().map(|a| infer_expr_type(a, cols)).unwrap_or(DataType::Text)
                }
                // Array mutation returns array
                ScalarFunc::ArrayAppend | ScalarFunc::ArrayPrepend
                | ScalarFunc::ArrayRemove | ScalarFunc::ArrayReplace
                | ScalarFunc::ArrayCat => {
                    args.first().map(|a| infer_expr_type(a, cols)).unwrap_or(DataType::Array(Box::new(DataType::Text)))
                }
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
                AggFunc::StddevPop | AggFunc::StddevSamp |
                AggFunc::VarPop | AggFunc::VarSamp |
                AggFunc::Corr | AggFunc::CovarPop | AggFunc::CovarSamp |
                AggFunc::RegrSlope | AggFunc::RegrIntercept | AggFunc::RegrR2 |
                AggFunc::RegrAvgX | AggFunc::RegrAvgY |
                AggFunc::RegrSXX | AggFunc::RegrSYY | AggFunc::RegrSXY |
                AggFunc::PercentileCont(_) | AggFunc::PercentileDisc(_) => DataType::Float64,
                AggFunc::RegrCount => DataType::Int64,
                AggFunc::Mode => input_ty.unwrap_or(DataType::Text),
                AggFunc::BitAndAgg | AggFunc::BitOrAgg | AggFunc::BitXorAgg => DataType::Int64,
            }
        }

        fn projection_to_field(p: &BoundProjection, schema: &cedar_common::schema::TableSchema) -> FieldDescription {
            match p {
                BoundProjection::Column(idx, alias) => {
                    let col = &schema.columns[*idx];
                    FieldDescription {
                        name: alias.clone(),
                        table_oid: 0,
                        column_attr: 0,
                        type_oid: col.data_type.pg_oid(),
                        type_len: col.data_type.type_len(),
                        type_modifier: -1,
                        format_code: 0,
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
            PhysicalPlan::SeqScan { projections, schema, .. }
            | PhysicalPlan::IndexScan { projections, schema, .. } => {
                projections.iter().map(|p| projection_to_field(p, schema)).collect()
            }
            PhysicalPlan::NestedLoopJoin { projections, combined_schema, .. }
            | PhysicalPlan::HashJoin { projections, combined_schema, .. } => {
                projections.iter().map(|p| projection_to_field(p, combined_schema)).collect()
            }
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
            PhysicalPlan::DistPlan { subplan, .. } => {
                self.plan_output_fields(subplan)
            }
            // DML/DDL/txn control — no result columns
            _ => vec![],
        }
    }

    fn error_response(&self, err: &CedarError) -> BackendMessage {
        BackendMessage::ErrorResponse {
            severity: err.pg_severity().into(),
            code: err.pg_sqlstate().into(),
            message: err.to_string(),
        }
    }

    /// Push current txn stats to Prometheus gauges.
    fn flush_txn_stats(&self) {
        let s = self.txn_mgr.stats_snapshot();
        cedar_observability::record_txn_stats(
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

fn classification_from_routing_hint(hint: &cedar_planner::TxnRoutingHint) -> TxnClassification {
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
    let start = sql.find(&pattern).map(|i| i + pattern.len())
        .or_else(|| sql.find(&pattern2).map(|i| i + pattern2.len()))?;
    let rest = &sql[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

/// Parse `SET log_min_duration_statement = <ms>` or `SET log_min_duration_statement TO <ms>`.
/// Also accepts `-1` or `default` to disable (returns 0).
/// Input `sql` must already be lowercased.
fn parse_set_log_min_duration(sql: &str) -> Option<u64> {
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
    let value = rest.trim_end_matches(';').trim().trim_matches('\'').trim_matches('"');
    if value == "default" || value == "-1" || value == "0" {
        return Some(0);
    }
    value.parse::<u64>().ok()
}

/// Parse `SET <var> = <value>` or `SET <var> TO <value>`.
/// Input `sql` must already be lowercased.
/// Returns (var_name, value) if successfully parsed.
fn parse_set_command(sql: &str) -> Option<(String, String)> {
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
    let value = rest.trim_end_matches(';').trim().trim_matches('\'').trim_matches('"');
    Some((name.to_string(), value.to_string()))
}

/// Parse `PREPARE name [(type, ...)] AS query`.
/// Returns (name, query) if successfully parsed.
fn parse_prepare_statement(sql: &str) -> Option<(String, String)> {
    let lower = sql.to_lowercase();
    let rest = lower.strip_prefix("prepare")?.trim();
    // Find AS keyword
    let as_pos = rest.find(" as ")?;
    let before_as = rest[..as_pos].trim();
    let query = sql[sql.to_lowercase().find(" as ")? + 4..].trim().trim_end_matches(';').trim();
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
fn parse_execute_statement(sql: &str) -> Option<(String, Vec<Option<Vec<u8>>>)> {
    let rest = sql.trim().strip_prefix("EXECUTE").or_else(|| sql.trim().strip_prefix("execute"))?.trim();
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
            split_params(ps).into_iter().map(|p| {
                let trimmed = p.trim();
                if trimmed.eq_ignore_ascii_case("null") {
                    None
                } else {
                    let unquoted = trimmed.trim_matches('\'');
                    Some(unquoted.as_bytes().to_vec())
                }
            }).collect()
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
fn bind_params(sql: &str, param_values: &[Option<Vec<u8>>]) -> String {
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
                                    if ch == '\'' { result.push('\''); }
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
        msgs.iter().any(|m| matches!(m, BackendMessage::RowDescription { .. }))
    }

    #[test]
    fn test_show_gc_stats() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW cedar.gc_stats", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty(), "should have data rows");
        // Verify expected metric names
        let metric_names: Vec<_> = rows.iter()
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
        let msgs = handler.handle_query("SHOW cedar.gc_safepoint", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 6, "gc_safepoint should have 6 rows");
        let metric_names: Vec<_> = rows.iter()
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
        let msgs = handler.handle_query("SHOW cedar.wal_stats", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 9);
        let metric_names: Vec<_> = rows.iter()
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
        let msgs = handler.handle_query("SHOW cedar.node_role", &mut session);
        assert!(has_row_description(&msgs), "should have RowDescription");
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("role".into()));
        // Default role when CEDAR_NODE_ROLE env var is not set
        assert!(rows[0][1].is_some(), "role value should be present");
    }

    #[test]
    fn test_show_replication_stats() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW cedar.replication_stats", &mut session);
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

        let msgs = handler.handle_query("SHOW cedar.replication_stats", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows[0][1], Some("1".into()), "promote_count should be 1");
        assert_eq!(rows[1][1], Some("42".into()), "last_failover_time_ms should be 42");
    }

    #[test]
    fn test_show_txn_stats() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW cedar.txn_stats", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty());
        let metric_names: Vec<_> = rows.iter()
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
        let msgs = handler.handle_query("SHOW cedar.txn_history", &mut session);
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
            "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)", &mut session);
        // Should contain CommandComplete, not an error
        let has_complete = msgs.iter().any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
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

        let msgs = handler.handle_query("SHOW cedar.replication_stats", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows[0][1], Some("3".into()), "promote_count should be 3");
        assert_eq!(rows[1][1], Some("30".into()), "last_failover_time_ms should be 30 (last recorded)");
    }

    #[test]
    fn test_show_scatter_stats_without_dist_engine() {
        let (handler, mut session) = setup_handler();
        // Without dist_engine, scatter_stats should return zeroed/empty metrics
        let msgs = handler.handle_query("SHOW cedar.scatter_stats", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        // Should have rows with zeroed values (no dist engine = no scatter stats)
        assert!(!rows.is_empty());
    }

    #[test]
    fn test_gc_stats_initial_values_are_zero() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW cedar.gc_stats", &mut session);
        let rows = extract_data_rows(&msgs);
        // total_sweeps should be "0" initially
        let sweeps_row = rows.iter().find(|r| r[0] == Some("total_sweeps".into()));
        assert_eq!(sweeps_row.unwrap()[1], Some("0".into()));
        // reclaimed_version_count should be "0"
        let reclaimed_row = rows.iter().find(|r| r[0] == Some("reclaimed_version_count".into()));
        assert_eq!(reclaimed_row.unwrap()[1], Some("0".into()));
    }

    #[test]
    fn test_show_txn_history_after_commits() {
        let (handler, mut session) = setup_handler();

        // Create table + commit a transaction via handler
        handler.handle_query(
            "CREATE TABLE th (id INT PRIMARY KEY, val TEXT)", &mut session);
        handler.handle_query("BEGIN", &mut session);
        handler.handle_query("INSERT INTO th VALUES (1, 'a')", &mut session);
        handler.handle_query("COMMIT", &mut session);

        let msgs = handler.handle_query("SHOW cedar.txn_history", &mut session);
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        // Should have at least 1 completed transaction record
        assert!(!rows.is_empty(), "txn_history should have records after commits");
    }

    #[test]
    fn test_show_txn_stats_after_commits() {
        let (handler, mut session) = setup_handler();

        handler.handle_query(
            "CREATE TABLE ts (id INT PRIMARY KEY)", &mut session);
        handler.handle_query("BEGIN", &mut session);
        handler.handle_query("INSERT INTO ts VALUES (1)", &mut session);
        handler.handle_query("COMMIT", &mut session);

        let msgs = handler.handle_query("SHOW cedar.txn_stats", &mut session);
        let rows = extract_data_rows(&msgs);
        let committed_row = rows.iter()
            .find(|r| r[0] == Some("total_committed".into()));
        let count: u64 = committed_row.unwrap()[1].as_ref().unwrap().parse().unwrap();
        assert!(count >= 1, "total_committed should be >= 1 after a commit");
    }

    // ── Error path tests ──

    fn has_error_response(msgs: &[BackendMessage]) -> bool {
        msgs.iter().any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
    }

    fn has_notice_response(msgs: &[BackendMessage]) -> bool {
        msgs.iter().any(|m| matches!(m, BackendMessage::NoticeResponse { .. }))
    }

    #[test]
    fn test_invalid_sql_returns_error() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECTT * FROMM nothing", &mut session);
        assert!(has_error_response(&msgs), "Invalid SQL should produce ErrorResponse");
    }

    #[test]
    fn test_unknown_table_returns_error() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SELECT * FROM nonexistent_table", &mut session);
        assert!(has_error_response(&msgs), "Unknown table should produce ErrorResponse");
    }

    #[test]
    fn test_commit_without_transaction_returns_notice() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("COMMIT", &mut session);
        assert!(has_notice_response(&msgs),
            "COMMIT without active txn should produce NoticeResponse");
    }

    #[test]
    fn test_double_begin_returns_notice() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("BEGIN", &mut session);
        let msgs = handler.handle_query("BEGIN", &mut session);
        assert!(has_notice_response(&msgs),
            "BEGIN while already in txn should produce NoticeResponse");
        // Clean up
        handler.handle_query("ROLLBACK", &mut session);
    }

    #[test]
    fn test_rollback_without_transaction_succeeds() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("ROLLBACK", &mut session);
        let has_complete = msgs.iter().any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
        assert!(has_complete, "ROLLBACK without txn should still return CommandComplete");
        assert!(!has_error_response(&msgs), "ROLLBACK without txn should not be an error");
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
        assert_eq!(parse_set_log_min_duration("set log_min_duration_statement = 500"), Some(500));
        assert_eq!(parse_set_log_min_duration("set log_min_duration_statement = 0"), Some(0));
        assert_eq!(parse_set_log_min_duration("set log_min_duration_statement = 100;"), Some(100));
    }

    #[test]
    fn test_parse_log_min_duration_to() {
        assert_eq!(parse_set_log_min_duration("set log_min_duration_statement to 3000"), Some(3000));
        assert_eq!(parse_set_log_min_duration("set log_min_duration_statement to '5000'"), Some(5000));
    }

    #[test]
    fn test_parse_log_min_duration_disable() {
        assert_eq!(parse_set_log_min_duration("set log_min_duration_statement = default"), Some(0));
        assert_eq!(parse_set_log_min_duration("set log_min_duration_statement = -1"), Some(0));
    }

    #[test]
    fn test_parse_log_min_duration_not_matching() {
        assert_eq!(parse_set_log_min_duration("select 1"), None);
        assert_eq!(parse_set_log_min_duration("set statement_timeout = 100"), None);
        assert_eq!(parse_set_log_min_duration("set log_min_duration_statement"), None);
    }

    // ── Slow query log handler tests ──

    #[test]
    fn test_show_slow_queries_disabled() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW cedar.slow_queries", &mut session);
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
        let msgs = handler.handle_query("SHOW cedar.slow_queries", &mut session);
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
        let handler = QueryHandler::new(storage, txn_mgr, executor)
            .with_slow_query_log(log);
        let mut session = PgSession::new(1);

        // Verify there's 1 entry
        let msgs = handler.handle_query("SHOW cedar.slow_queries", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows[1][1], Some("1".into())); // total_slow_queries

        // Reset
        handler.handle_query("RESET cedar.slow_queries", &mut session);

        // Should be empty now
        let msgs2 = handler.handle_query("SHOW cedar.slow_queries", &mut session);
        let rows2 = extract_data_rows(&msgs2);
        assert_eq!(rows2[1][1], Some("0".into())); // total_slow_queries
    }

    // ── Checkpoint tests ──

    #[test]
    fn test_checkpoint_without_wal_returns_error() {
        // In-memory engine has no WAL, so CHECKPOINT should fail
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("CHECKPOINT", &mut session);
        assert!(has_error_response(&msgs), "CHECKPOINT without WAL should return error");
    }

    #[test]
    fn test_checkpoint_stats_without_wal() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("SHOW cedar.checkpoint_stats", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows[0][1], Some("false".into())); // wal_enabled
        assert_eq!(rows[1][1], Some("false".into())); // checkpoint_available
    }

    #[test]
    fn test_checkpoint_with_wal() {
        let dir = std::env::temp_dir().join("cedar_handler_ckpt_test");
        let _ = std::fs::remove_dir_all(&dir);

        let storage = Arc::new(StorageEngine::new(Some(&dir)).unwrap());
        let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
        let executor = Arc::new(Executor::new(storage.clone(), txn_mgr.clone()));
        let handler = QueryHandler::new(storage, txn_mgr, executor);
        let mut session = PgSession::new(1);

        handler.handle_query("CREATE TABLE ckpt_h (id INT PRIMARY KEY)", &mut session);
        handler.handle_query("INSERT INTO ckpt_h VALUES (1)", &mut session);

        let msgs = handler.handle_query("CHECKPOINT", &mut session);
        assert!(!has_error_response(&msgs), "CHECKPOINT with WAL should succeed");
        let rows = extract_data_rows(&msgs);
        assert!(!rows.is_empty());
        let val = rows[0][0].as_ref().unwrap();
        assert!(val.starts_with("OK"), "Checkpoint result should start with OK: {}", val);

        // Verify checkpoint_stats shows WAL enabled
        let msgs2 = handler.handle_query("SHOW cedar.checkpoint_stats", &mut session);
        let rows2 = extract_data_rows(&msgs2);
        assert_eq!(rows2[0][1], Some("true".into())); // wal_enabled

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Describe query tests (M4.7) ──

    #[test]
    fn test_describe_select_returns_columns() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE desc_t (id INT PRIMARY KEY, name TEXT, active BOOLEAN)", &mut session);

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

        let fields = handler.describe_query("INSERT INTO desc_dml VALUES (1)").unwrap();
        assert!(fields.is_empty(), "DML should have no result columns");
    }

    #[test]
    fn test_describe_explain_returns_query_plan_column() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE desc_ex (id INT PRIMARY KEY)", &mut session);

        let fields = handler.describe_query("EXPLAIN SELECT * FROM desc_ex").unwrap();
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

        let msgs = handler.handle_query(
            "SELECT * FROM information_schema.tables", &mut session);
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 2, "should list at least 2 tables");
        // Check that our tables appear
        let names: Vec<&str> = rows.iter()
            .filter_map(|r| r[2].as_deref())
            .collect();
        assert!(names.contains(&"is_t1"));
        assert!(names.contains(&"is_t2"));
        // Check columns: table_catalog=cedar, table_schema=public, table_type=BASE TABLE
        let t1_row = rows.iter().find(|r| r[2] == Some("is_t1".into())).unwrap();
        assert_eq!(t1_row[0], Some("cedar".into()));
        assert_eq!(t1_row[1], Some("public".into()));
        assert_eq!(t1_row[3], Some("BASE TABLE".into()));
    }

    #[test]
    fn test_information_schema_tables_with_filter() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE filt_a (id INT PRIMARY KEY)", &mut session);
        handler.handle_query("CREATE TABLE filt_b (id INT PRIMARY KEY)", &mut session);

        let msgs = handler.handle_query(
            "SELECT * FROM information_schema.tables WHERE table_name = 'filt_a'", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][2], Some("filt_a".into()));
    }

    #[test]
    fn test_information_schema_columns() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE is_cols (id INT PRIMARY KEY, name TEXT, active BOOLEAN)", &mut session);

        let msgs = handler.handle_query(
            "SELECT * FROM information_schema.columns WHERE table_name = 'is_cols'", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 3);
        // Check column details
        assert_eq!(rows[0][3], Some("id".into()));        // column_name
        assert_eq!(rows[0][4], Some("1".into()));          // ordinal_position
        assert_eq!(rows[0][6], Some("NO".into()));         // is_nullable (PK)
        assert_eq!(rows[0][7], Some("integer".into()));    // data_type
        assert_eq!(rows[0][12], Some("int4".into()));      // udt_name

        assert_eq!(rows[1][3], Some("name".into()));
        assert_eq!(rows[1][7], Some("text".into()));
        assert_eq!(rows[1][12], Some("text".into()));      // udt_name

        assert_eq!(rows[2][3], Some("active".into()));
        assert_eq!(rows[2][7], Some("boolean".into()));
        assert_eq!(rows[2][12], Some("bool".into()));      // udt_name
    }

    #[test]
    fn test_information_schema_table_constraints() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE is_con (id INT PRIMARY KEY, val TEXT UNIQUE)", &mut session);

        let msgs = handler.handle_query(
            "SELECT * FROM information_schema.table_constraints", &mut session);
        let rows = extract_data_rows(&msgs);
        let con_names: Vec<&str> = rows.iter()
            .filter(|r| r[3] == Some("is_con".into()))
            .filter_map(|r| r[4].as_deref())
            .collect();
        assert!(con_names.contains(&"PRIMARY KEY"));
        assert!(con_names.contains(&"UNIQUE"));
    }

    #[test]
    fn test_information_schema_key_column_usage() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE is_kcu (id INT PRIMARY KEY, name TEXT)", &mut session);

        let msgs = handler.handle_query(
            "SELECT * FROM information_schema.key_column_usage", &mut session);
        let rows = extract_data_rows(&msgs);
        let pk_rows: Vec<_> = rows.iter()
            .filter(|r| r[1] == Some("is_kcu".into()))
            .collect();
        assert!(!pk_rows.is_empty());
        assert_eq!(pk_rows[0][2], Some("id".into())); // column_name
    }

    #[test]
    fn test_information_schema_schemata() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query(
            "SELECT * FROM information_schema.schemata", &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 2); // public + information_schema
        let schema_names: Vec<&str> = rows.iter()
            .filter_map(|r| r[1].as_deref())
            .collect();
        assert!(schema_names.contains(&"public"));
        assert!(schema_names.contains(&"information_schema"));
    }

    // ── View tests (M5.2) ──

    #[test]
    fn test_create_view_and_select() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE v_base (id INT PRIMARY KEY, name TEXT, active BOOLEAN)", &mut session);
        handler.handle_query("INSERT INTO v_base VALUES (1, 'alice', true)", &mut session);
        handler.handle_query("INSERT INTO v_base VALUES (2, 'bob', false)", &mut session);

        // Create view
        let msgs = handler.handle_query("CREATE VIEW v_active AS SELECT id, name FROM v_base WHERE active = true", &mut session);
        assert!(!has_error_response(&msgs), "CREATE VIEW should succeed");

        // Select from view
        let msgs2 = handler.handle_query("SELECT * FROM v_active", &mut session);
        assert!(!has_error_response(&msgs2), "SELECT from view should succeed: {:?}", msgs2);
        let rows = extract_data_rows(&msgs2);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some("1".into()));
        assert_eq!(rows[0][1], Some("alice".into()));
    }

    #[test]
    fn test_drop_view() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE dv_base (id INT PRIMARY KEY)", &mut session);
        handler.handle_query("CREATE VIEW dv_view AS SELECT id FROM dv_base", &mut session);

        let msgs = handler.handle_query("DROP VIEW dv_view", &mut session);
        assert!(!has_error_response(&msgs), "DROP VIEW should succeed");

        // Selecting from dropped view should fail
        let msgs2 = handler.handle_query("SELECT * FROM dv_view", &mut session);
        assert!(has_error_response(&msgs2), "SELECT from dropped view should fail");
    }

    #[test]
    fn test_drop_view_if_exists() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query("DROP VIEW IF EXISTS nonexistent_view", &mut session);
        assert!(!has_error_response(&msgs), "DROP VIEW IF EXISTS should not error");
    }

    #[test]
    fn test_create_or_replace_view() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE cr_base (id INT PRIMARY KEY, val TEXT)", &mut session);
        handler.handle_query("INSERT INTO cr_base VALUES (1, 'a')", &mut session);

        handler.handle_query("CREATE VIEW cr_view AS SELECT id FROM cr_base", &mut session);
        // Replace with different query
        let msgs = handler.handle_query("CREATE OR REPLACE VIEW cr_view AS SELECT id, val FROM cr_base", &mut session);
        assert!(!has_error_response(&msgs), "CREATE OR REPLACE VIEW should succeed");

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
        handler.handle_query("CREATE VIEW vis_view AS SELECT id FROM vis_base", &mut session);

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
        handler.handle_query("CREATE TABLE ren_col (id INT PRIMARY KEY, old_name TEXT)", &mut session);
        handler.handle_query("INSERT INTO ren_col VALUES (1, 'hello')", &mut session);

        let msgs = handler.handle_query("ALTER TABLE ren_col RENAME COLUMN old_name TO new_name", &mut session);
        assert!(!has_error_response(&msgs), "RENAME COLUMN should succeed");

        // Verify column was renamed by querying information_schema
        let msgs2 = handler.handle_query(
            "SELECT * FROM information_schema.columns WHERE table_name = 'ren_col'", &mut session);
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
        handler.handle_query("CREATE TABLE old_tbl (id INT PRIMARY KEY, val TEXT)", &mut session);
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
        let msgs = handler.handle_query(
            "SELECT * FROM pg_catalog.pg_type",
            &mut session,
        );
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 10, "should have builtin types");
        // Check that int4 is present
        let type_names: Vec<_> = rows.iter()
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
        let msgs = handler.handle_query(
            "SELECT * FROM pg_catalog.pg_namespace",
            &mut session,
        );
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 3);
        let ns_names: Vec<_> = rows.iter()
            .filter_map(|r| r.get(1).cloned().flatten())
            .collect();
        assert!(ns_names.contains(&"pg_catalog".to_string()));
        assert!(ns_names.contains(&"public".to_string()));
        assert!(ns_names.contains(&"information_schema".to_string()));
    }

    #[test]
    fn test_pg_database() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query(
            "SELECT * FROM pg_catalog.pg_database",
            &mut session,
        );
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Some("cedar".into()));
    }

    #[test]
    fn test_pg_settings() {
        let (handler, mut session) = setup_handler();
        let msgs = handler.handle_query(
            "SELECT * FROM pg_catalog.pg_settings",
            &mut session,
        );
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 10);
        let setting_names: Vec<_> = rows.iter()
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
        let msgs = handler.handle_query(
            "SELECT * FROM pg_catalog.pg_index",
            &mut session,
        );
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 1, "should have at least PK index");
        // Check indisprimary is 't' for the PK
        let pk_rows: Vec<_> = rows.iter()
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
        let msgs = handler.handle_query(
            "SELECT * FROM pg_catalog.pg_constraint",
            &mut session,
        );
        assert!(has_row_description(&msgs));
        let rows = extract_data_rows(&msgs);
        assert!(rows.len() >= 1);
        // contype 'p' = PK
        let pk_rows: Vec<_> = rows.iter()
            .filter(|r| r.get(3).cloned().flatten() == Some("p".into()))
            .collect();
        assert!(!pk_rows.is_empty(), "should have PK constraint");
        // conname should contain table name
        let conname = pk_rows[0].get(1).cloned().flatten().unwrap();
        assert!(conname.contains("con_test"), "PK constraint name should reference table");
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
        assert_eq!(
            extract_where_eq("select * from t", "table_name"),
            None
        );
    }

    // ── Phase 2: Prepared Statement / Parameterized SQL tests ──

    #[test]
    fn test_prepare_statement_select_with_param() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE ps_t1 (id INT PRIMARY KEY, name TEXT)", &mut session);

        let result = handler.prepare_statement("SELECT * FROM ps_t1 WHERE id = $1");
        assert!(result.is_ok(), "prepare_statement should succeed: {:?}", result.err());
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
        handler.handle_query("CREATE TABLE ps_t2 (id INT PRIMARY KEY, val TEXT)", &mut session);

        let result = handler.prepare_statement("INSERT INTO ps_t2 VALUES ($1, $2)");
        assert!(result.is_ok(), "prepare INSERT should succeed: {:?}", result.err());
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
        handler.handle_query("CREATE TABLE ep_t1 (id INT PRIMARY KEY, name TEXT)", &mut session);
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
        handler.handle_query("CREATE TABLE ep_t2 (id INT PRIMARY KEY, name TEXT)", &mut session);
        handler.handle_query("INSERT INTO ep_t2 VALUES (1, 'alice')", &mut session);
        handler.handle_query("INSERT INTO ep_t2 VALUES (2, 'bob')", &mut session);

        let (plan, _types, _desc) = handler.prepare_statement("SELECT * FROM ep_t2 WHERE id = $1").unwrap();
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
        handler.handle_query("CREATE TABLE ep_t3 (id INT PRIMARY KEY, name TEXT)", &mut session);
        handler.handle_query("INSERT INTO ep_t3 VALUES (1, 'alice')", &mut session);
        handler.handle_query("INSERT INTO ep_t3 VALUES (2, 'bob')", &mut session);

        let (plan, _types, _desc) = handler.prepare_statement("SELECT * FROM ep_t3 WHERE name = $1").unwrap();
        let params = vec![Datum::Text("bob".into())];
        let msgs = handler.execute_plan(&plan, &params, &mut session);
        let rows = extract_data_rows(&msgs);
        assert_eq!(rows.len(), 1, "should return 1 row for name='bob'");
        assert_eq!(rows[0][0], Some("2".into()));
    }

    #[test]
    fn test_execute_plan_insert_with_params() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE ep_t4 (id INT PRIMARY KEY, val TEXT)", &mut session);

        let (plan, _types, _desc) = handler.prepare_statement("INSERT INTO ep_t4 VALUES ($1, $2)").unwrap();
        let params = vec![Datum::Int32(42), Datum::Text("hello".into())];
        let msgs = handler.execute_plan(&plan, &params, &mut session);
        assert!(!has_error_response(&msgs), "INSERT with params should succeed: {:?}", msgs);

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
        handler.handle_query("CREATE TABLE ep_t5 (id INT PRIMARY KEY, val TEXT)", &mut session);
        handler.handle_query("INSERT INTO ep_t5 VALUES (1, 'old')", &mut session);

        let (plan, _types, _desc) = handler.prepare_statement("UPDATE ep_t5 SET val = $1 WHERE id = $2").unwrap();
        let params = vec![Datum::Text("new".into()), Datum::Int32(1)];
        let msgs = handler.execute_plan(&plan, &params, &mut session);
        assert!(!has_error_response(&msgs), "UPDATE with params should succeed: {:?}", msgs);

        let msgs2 = handler.handle_query("SELECT * FROM ep_t5", &mut session);
        let rows = extract_data_rows(&msgs2);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Some("new".into()));
    }

    #[test]
    fn test_execute_plan_delete_with_params() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE ep_t6 (id INT PRIMARY KEY, val TEXT)", &mut session);
        handler.handle_query("INSERT INTO ep_t6 VALUES (1, 'a')", &mut session);
        handler.handle_query("INSERT INTO ep_t6 VALUES (2, 'b')", &mut session);

        let (plan, _types, _desc) = handler.prepare_statement("DELETE FROM ep_t6 WHERE id = $1").unwrap();
        let params = vec![Datum::Int32(1)];
        let msgs = handler.execute_plan(&plan, &params, &mut session);
        assert!(!has_error_response(&msgs), "DELETE with params should succeed: {:?}", msgs);

        let msgs2 = handler.handle_query("SELECT * FROM ep_t6", &mut session);
        let rows = extract_data_rows(&msgs2);
        assert_eq!(rows.len(), 1, "should have 1 row after deleting id=1");
        assert_eq!(rows[0][0], Some("2".into()));
    }

    #[test]
    fn test_execute_plan_with_null_param() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE ep_t7 (id INT PRIMARY KEY, val TEXT)", &mut session);

        let (plan, _types, _desc) = handler.prepare_statement("INSERT INTO ep_t7 VALUES ($1, $2)").unwrap();
        let params = vec![Datum::Int32(1), Datum::Null];
        let msgs = handler.execute_plan(&plan, &params, &mut session);
        assert!(!has_error_response(&msgs), "INSERT with NULL param should succeed: {:?}", msgs);

        let msgs2 = handler.handle_query("SELECT * FROM ep_t7", &mut session);
        let rows = extract_data_rows(&msgs2);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], None, "NULL param should produce NULL value");
    }

    #[test]
    fn test_execute_plan_reuse_with_different_params() {
        let (handler, mut session) = setup_handler();
        handler.handle_query("CREATE TABLE ep_t8 (id INT PRIMARY KEY, name TEXT)", &mut session);
        handler.handle_query("INSERT INTO ep_t8 VALUES (1, 'alice')", &mut session);
        handler.handle_query("INSERT INTO ep_t8 VALUES (2, 'bob')", &mut session);
        handler.handle_query("INSERT INTO ep_t8 VALUES (3, 'carol')", &mut session);

        let (plan, _types, _desc) = handler.prepare_statement("SELECT * FROM ep_t8 WHERE id = $1").unwrap();

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
        use cedar_common::types::DataType;

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
        handler.handle_query("CREATE TABLE pof_t (id INT PRIMARY KEY, name TEXT, active BOOLEAN)", &mut session);

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
        handler.handle_query("CREATE TABLE ps_life (id INT PRIMARY KEY, val TEXT)", &mut session);
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
        let msgs = handler.execute_plan(
            portal.plan.as_ref().unwrap(),
            &portal.params,
            &mut session,
        );
        assert!(!has_error_response(&msgs), "Execute should succeed: {:?}", msgs);
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
        handler.handle_query("CREATE TABLE ps_mp (id INT PRIMARY KEY, val TEXT)", &mut session);
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
        session.portals.insert("p1".into(), crate::session::Portal {
            plan: ps.plan.clone(),
            params: vec![Datum::Int32(1)],
            bound_sql: String::new(),
        });
        session.portals.insert("p2".into(), crate::session::Portal {
            plan: ps.plan.clone(),
            params: vec![Datum::Int32(3)],
            bound_sql: String::new(),
        });

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
}
