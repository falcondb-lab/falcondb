use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use falcon_common::config::SpillConfig;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::{ExecutionError, FalconError};
use falcon_common::schema::TableSchema;
use falcon_common::security::{
    ObjectRef, ObjectType, Privilege, PrivilegeCheckResult, PrivilegeManager, RoleCatalog, RoleId,
    SUPERUSER_ROLE_ID,
};
use falcon_common::types::{DataType, TableId};
use falcon_planner::ai_optimizer::{extract_features_from_physical, AiOptimizer, FeedbackRecord, PlanKind};
use falcon_planner::aiops::{global_aiops, AiOps};
use falcon_planner::PhysicalPlan;
use falcon_sql_frontend::types::*;
use falcon_storage::engine::StorageEngine;
use falcon_txn::{TxnHandle, TxnManager};

use crate::enterprise::{DmlAuditLog, ReplicationSlotManager, RlsRegistry};
use crate::external_sort::ExternalSorter;
use crate::parallel::ParallelConfig;

/// Materialized CTE data: table_id -> rows
pub(crate) type CteData = HashMap<TableId, Vec<OwnedRow>>;

/// Result of executing a physical plan.
#[derive(Debug)]
pub enum ExecutionResult {
    /// DDL success with a message.
    Ddl { message: String },
    /// DML success with affected row count.
    Dml {
        rows_affected: u64,
        tag: &'static str,
    },
    /// Query result with column metadata and rows.
    Query {
        columns: Vec<(String, DataType)>,
        rows: Vec<OwnedRow>,
    },
    /// Transaction control result.
    TxnControl { action: String },
}

/// The executor processes physical plans against storage.
pub struct Executor {
    pub(crate) storage: Arc<StorageEngine>,
    pub(crate) txn_mgr: Arc<TxnManager>,
    /// When true, the executor rejects all DDL and DML writes.
    /// Used on replica nodes to enforce read-only mode.
    read_only: std::sync::atomic::AtomicBool,
    /// Optional handle to active connection counter for SHOW falcon.connections.
    active_connections: Option<Arc<std::sync::atomic::AtomicUsize>>,
    /// Maximum connections setting for SHOW falcon.connections.
    max_connections: usize,
    /// External sorter for spill-to-disk ORDER BY (None = pure in-memory).
    pub(crate) external_sorter: Option<ExternalSorter>,
    /// Maximum groups allowed in hash aggregation (0 = unlimited).
    pub(crate) hash_agg_group_limit: usize,
    /// Maximum total rows accumulated in a recursive CTE (0 = unlimited).
    pub(crate) recursive_cte_max_rows: usize,
    /// Parallel execution configuration.
    pub(crate) parallel_config: ParallelConfig,
    /// RBAC: role catalog for inheritance resolution.
    role_catalog: Option<Arc<std::sync::RwLock<RoleCatalog>>>,
    /// RBAC: privilege manager for GRANT/REVOKE checks.
    privilege_manager: Option<Arc<std::sync::RwLock<PrivilegeManager>>>,
    /// RBAC: current session role.
    current_role: RoleId,
    /// AI query optimizer for plan selection feedback.
    pub(crate) ai_optimizer: Arc<AiOptimizer>,
    /// AIOps engine: slow-query detection, anomaly detection, index advisor, workload profiler.
    pub(crate) aiops: Arc<AiOps>,
    /// Enterprise: Row Level Security policy registry.
    pub(crate) rls_registry: Arc<RlsRegistry>,
    /// Enterprise: Logical replication slot manager.
    pub(crate) repl_slots: Arc<ReplicationSlotManager>,
    /// Enterprise: DML audit log.
    pub(crate) dml_audit: Arc<DmlAuditLog>,
}

impl Executor {
    pub fn new(storage: Arc<StorageEngine>, txn_mgr: Arc<TxnManager>) -> Self {
        Self {
            storage,
            txn_mgr,
            read_only: std::sync::atomic::AtomicBool::new(false),
            active_connections: None,
            max_connections: 0,
            external_sorter: None,
            hash_agg_group_limit: 0,
            ai_optimizer: Arc::new(AiOptimizer::new()),
            aiops: Arc::new(global_aiops().clone()),
            recursive_cte_max_rows: 0,
            parallel_config: ParallelConfig::default(),
            role_catalog: None,
            privilege_manager: None,
            current_role: SUPERUSER_ROLE_ID,
            rls_registry: Arc::new(RlsRegistry::new()),
            repl_slots: Arc::new(ReplicationSlotManager::new()),
            dml_audit: Arc::new(DmlAuditLog::new()),
        }
    }

    /// Create an executor in read-only mode (for replica nodes).
    pub fn new_read_only(storage: Arc<StorageEngine>, txn_mgr: Arc<TxnManager>) -> Self {
        Self {
            storage,
            txn_mgr,
            read_only: std::sync::atomic::AtomicBool::new(true),
            active_connections: None,
            max_connections: 0,
            external_sorter: None,
            hash_agg_group_limit: 0,
            ai_optimizer: Arc::new(AiOptimizer::new()),
            aiops: Arc::new(global_aiops().clone()),
            recursive_cte_max_rows: 0,
            parallel_config: ParallelConfig::default(),
            role_catalog: None,
            privilege_manager: None,
            current_role: SUPERUSER_ROLE_ID,
            rls_registry: Arc::new(RlsRegistry::new()),
            repl_slots: Arc::new(ReplicationSlotManager::new()),
            dml_audit: Arc::new(DmlAuditLog::new()),
        }
    }

    /// Configure RBAC enforcement for this executor.
    pub fn set_rbac(
        &mut self,
        role_catalog: Arc<std::sync::RwLock<RoleCatalog>>,
        privilege_manager: Arc<std::sync::RwLock<PrivilegeManager>>,
        current_role: RoleId,
    ) {
        self.role_catalog = Some(role_catalog);
        self.privilege_manager = Some(privilege_manager);
        self.current_role = current_role;
    }

    /// Set the current session role.
    pub const fn set_current_role(&mut self, role_id: RoleId) {
        self.current_role = role_id;
    }

    /// Configure parallel execution.
    pub const fn set_parallel_config(&mut self, config: ParallelConfig) {
        self.parallel_config = config;
    }

    /// Set the active connections handle for SHOW falcon.connections.
    pub fn set_connection_info(&mut self, active: Arc<std::sync::atomic::AtomicUsize>, max: usize) {
        self.active_connections = Some(active);
        self.max_connections = max;
    }

    /// Whether this executor is in read-only mode.
    pub fn is_read_only(&self) -> bool {
        self.read_only.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Configure spill-to-disk from SpillConfig.
    pub fn set_spill_config(&mut self, config: &SpillConfig) {
        self.external_sorter = ExternalSorter::from_config(config);
        self.hash_agg_group_limit = config.hash_agg_group_limit;
        self.recursive_cte_max_rows = config.recursive_cte_max_rows;
    }

    /// Get the spill metrics snapshot (None if spill is disabled).
    pub fn spill_metrics(&self) -> Option<crate::external_sort::SpillMetricsSnapshot> {
        self.external_sorter.as_ref().map(|s| s.metrics.snapshot())
    }

    /// Set read-only mode (e.g. after failover role change).
    pub fn set_read_only(&self, val: bool) {
        self.read_only
            .store(val, std::sync::atomic::Ordering::Relaxed);
    }

    /// Guard: reject the operation if this executor is read-only.
    fn reject_if_read_only(&self, op: &str) -> Result<(), FalconError> {
        if self.is_read_only() {
            Err(FalconError::ReadOnly(format!(
                "cannot execute {op} on a read-only replica"
            )))
        } else {
            Ok(())
        }
    }

    /// Public RBAC guard for integration testing.
    /// Returns Ok(()) if the current role has the required privilege, Err otherwise.
    pub fn check_privilege_public(
        &self,
        privilege: Privilege,
        object_type: ObjectType,
        object_name: &str,
    ) -> Result<(), FalconError> {
        self.check_privilege(privilege, object_type, object_name)
    }

    /// RBAC guard: check if the current role has the required privilege on the object.
    /// Superusers bypass all checks. If no RBAC is configured, all operations are allowed.
    fn check_privilege(
        &self,
        privilege: Privilege,
        object_type: ObjectType,
        object_name: &str,
    ) -> Result<(), FalconError> {
        let (catalog, manager) = match (&self.role_catalog, &self.privilege_manager) {
            (Some(c), Some(m)) => (c, m),
            _ => return Ok(()), // RBAC not configured — allow all
        };
        let catalog = catalog
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Superuser bypasses all privilege checks
        if let Some(role) = catalog.get_role(self.current_role) {
            if role.is_superuser {
                return Ok(());
            }
        }
        let effective_roles = catalog.effective_roles(self.current_role);
        // Check if any effective role is superuser
        for &rid in &effective_roles {
            if let Some(r) = catalog.get_role(rid) {
                if r.is_superuser {
                    return Ok(());
                }
            }
        }
        // Use a simple hash of the object name as the object_id
        let object_id = {
            let mut h: u64 = 5381;
            for b in object_name.bytes() {
                h = h.wrapping_mul(33).wrapping_add(u64::from(b));
            }
            h
        };
        let object_ref = ObjectRef {
            object_type,
            object_id,
            object_name: object_name.to_owned(),
        };
        let manager = manager
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match manager.check_privilege(&effective_roles, privilege, &object_ref) {
            PrivilegeCheckResult::Allowed => Ok(()),
            PrivilegeCheckResult::Denied {
                role,
                privilege,
                object,
            } => {
                let role_name = catalog.get_role(self.current_role).map_or_else(
                    || format!("role_id_{}", self.current_role.0),
                    |r| r.name.clone(),
                );
                tracing::warn!(
                    role_id = self.current_role.0,
                    role_name = %role_name,
                    privilege = ?privilege,
                    object_type = %object_type,
                    object_name = %object_name,
                    denied_role = %role,
                    "RBAC: permission denied"
                );
                Err(FalconError::Execution(
                    ExecutionError::InsufficientPrivilege(format!(
                        "permission denied for {object_type} on {object}: role {role} lacks {privilege:?}"
                    )),
                ))
            }
        }
    }

    /// Guard: reject DML if the transaction is in READ ONLY mode.
    fn reject_if_txn_read_only(txn: &falcon_txn::TxnHandle, op: &str) -> Result<(), FalconError> {
        if txn.read_only {
            Err(FalconError::ReadOnly(format!(
                "cannot execute {op} in a read-only transaction"
            )))
        } else {
            Ok(())
        }
    }

    /// Guard: abort if the transaction has exceeded its timeout.
    fn check_txn_timeout(txn: &falcon_txn::TxnHandle) -> Result<(), FalconError> {
        if txn.is_timed_out() {
            Err(FalconError::Txn(falcon_common::error::TxnError::Timeout))
        } else {
            Ok(())
        }
    }

    /// Execute a parameterized physical plan within a transaction context.
    /// Substitutes `BoundExpr::Parameter` nodes with concrete values from `params`
    /// before delegating to `execute`.
    pub fn execute_with_params(
        &self,
        plan: &PhysicalPlan,
        txn: Option<&TxnHandle>,
        params: &[Datum],
    ) -> Result<ExecutionResult, FalconError> {
        if params.is_empty() {
            return self.execute(plan, txn);
        }
        // Fast path for simple INSERT: substitute row exprs without cloning schema
        if let PhysicalPlan::Insert {
            table_id,
            schema,
            columns,
            rows,
            source_select,
            returning,
            on_conflict,
        } = plan
        {
            if source_select.is_none() {
                let subst_rows =
                    crate::param_subst::subst_rows(rows, params).map_err(FalconError::Execution)?;
                let subst_returning = if returning.is_empty() {
                    Vec::new()
                } else {
                    crate::param_subst::subst_returning(returning, params)
                        .map_err(FalconError::Execution)?
                };
                let subst_on_conflict = crate::param_subst::subst_on_conflict(on_conflict, params)
                    .map_err(FalconError::Execution)?;
                let txn = txn.ok_or_else(|| {
                    FalconError::Execution(falcon_common::error::ExecutionError::TypeError(
                        "INSERT requires a transaction".into(),
                    ))
                })?;
                return self.exec_insert(
                    *table_id,
                    schema,
                    columns,
                    &subst_rows,
                    &subst_returning,
                    &subst_on_conflict,
                    txn,
                );
            }
        }
        let substituted = crate::param_subst::substitute_params_plan(plan, params)
            .map_err(FalconError::Execution)?;
        self.execute(&substituted, txn)
    }

    /// Extract table name and filter column hints from a scan plan for AIOps.
    fn extract_scan_context(plan: &PhysicalPlan) -> (Option<String>, Vec<String>) {
        match plan {
            PhysicalPlan::SeqScan { schema, .. } => {
                (Some(schema.name.clone()), vec![])
            }
            PhysicalPlan::IndexScan { schema, .. } | PhysicalPlan::IndexRangeScan { schema, .. } => {
                (Some(schema.name.clone()), vec![])
            }
            _ => (None, vec![]),
        }
    }

    /// Whether a plan is a query that benefits from AI feedback.
    fn is_query_plan(plan: &PhysicalPlan) -> bool {
        matches!(
            plan,
            PhysicalPlan::SeqScan { .. }
                | PhysicalPlan::IndexScan { .. }
                | PhysicalPlan::IndexRangeScan { .. }
                | PhysicalPlan::ColumnScan { .. }
                | PhysicalPlan::HashJoin { .. }
                | PhysicalPlan::NestedLoopJoin { .. }
                | PhysicalPlan::MergeSortJoin { .. }
        )
    }

    /// Execute a physical plan within a transaction context.
    /// `txn` is None for DDL and txn control statements.
    pub fn execute(
        &self,
        plan: &PhysicalPlan,
        txn: Option<&TxnHandle>,
    ) -> Result<ExecutionResult, FalconError> {
        self.execute_with_sql(plan, txn, "")
    }

    /// Execute with original SQL text for AIOps telemetry.
    pub fn execute_with_sql(
        &self,
        plan: &PhysicalPlan,
        txn: Option<&TxnHandle>,
        sql_text: &str,
    ) -> Result<ExecutionResult, FalconError> {
        crate::eval::scalar_time::reset_statement_ts();

        // AI feedback + AIOps: time query plans, record feedback, feed AIOps engine.
        if Self::is_query_plan(plan) {
            let t0 = Instant::now();
            let result = self.execute_inner(plan, txn);
            let elapsed_us = t0.elapsed().as_micros() as u64;
            let features = extract_features_from_physical(plan);
            let kind = PlanKind::from_physical(plan);
            let predicted = self.ai_optimizer.predict_cost(&features, kind);
            self.ai_optimizer.record_feedback(FeedbackRecord {
                features,
                plan_kind: kind,
                actual_us: elapsed_us,
                predicted_log2_cost: predicted,
            });
            let plan_kind_str = format!("{:?}", kind);
            let (table_name, filter_cols_owned) = Self::extract_scan_context(plan);
            let filter_cols: Vec<&str> = filter_cols_owned.iter().map(String::as_str).collect();
            let is_error = result.is_err();
            self.aiops.record(
                sql_text,
                &plan_kind_str,
                table_name.as_deref(),
                &filter_cols,
                elapsed_us,
                is_error,
            );
            return result;
        }

        self.execute_inner(plan, txn)
    }

    fn execute_inner(
        &self,
        plan: &PhysicalPlan,
        txn: Option<&TxnHandle>,
    ) -> Result<ExecutionResult, FalconError> {
        match plan {
            PhysicalPlan::CreateDatabase {
                name,
                if_not_exists,
            } => {
                self.reject_if_read_only("CREATE DATABASE")?;
                match self.storage.create_database(name, "falcon") {
                    Ok(_oid) => Ok(ExecutionResult::Ddl {
                        message: "CREATE DATABASE".to_owned(),
                    }),
                    Err(falcon_common::error::StorageError::DatabaseAlreadyExists(_))
                        if *if_not_exists =>
                    {
                        Ok(ExecutionResult::Ddl {
                            message: "CREATE DATABASE".to_owned(),
                        })
                    }
                    Err(e) => Err(e.into()),
                }
            }
            PhysicalPlan::DropDatabase { name, if_exists } => {
                self.reject_if_read_only("DROP DATABASE")?;
                match self.storage.drop_database(name) {
                    Ok(()) => Ok(ExecutionResult::Ddl {
                        message: "DROP DATABASE".to_owned(),
                    }),
                    Err(falcon_common::error::StorageError::DatabaseNotFound(_)) if *if_exists => {
                        Ok(ExecutionResult::Ddl {
                            message: "DROP DATABASE".to_owned(),
                        })
                    }
                    Err(e) => Err(e.into()),
                }
            }
            PhysicalPlan::CreateSchema {
                name,
                if_not_exists,
            } => {
                self.reject_if_read_only("CREATE SCHEMA")?;
                match self.storage.create_schema(name, "falcon") {
                    Ok(()) => Ok(ExecutionResult::Ddl {
                        message: "CREATE SCHEMA".to_owned(),
                    }),
                    Err(falcon_common::error::StorageError::SchemaAlreadyExists(_))
                        if *if_not_exists =>
                    {
                        Ok(ExecutionResult::Ddl {
                            message: "CREATE SCHEMA".to_owned(),
                        })
                    }
                    Err(e) => Err(e.into()),
                }
            }
            PhysicalPlan::DropSchema { name, if_exists } => {
                self.reject_if_read_only("DROP SCHEMA")?;
                match self.storage.drop_schema(name) {
                    Ok(()) => Ok(ExecutionResult::Ddl {
                        message: "DROP SCHEMA".to_owned(),
                    }),
                    Err(falcon_common::error::StorageError::SchemaNotFound(_)) if *if_exists => {
                        Ok(ExecutionResult::Ddl {
                            message: "DROP SCHEMA".to_owned(),
                        })
                    }
                    Err(e) => Err(e.into()),
                }
            }
            PhysicalPlan::CreateRole {
                name,
                can_login,
                is_superuser,
                can_create_db,
                can_create_role,
                password,
            } => {
                self.reject_if_read_only("CREATE ROLE")?;
                match self.storage.create_role(
                    name,
                    *can_login,
                    *is_superuser,
                    *can_create_db,
                    *can_create_role,
                    password.clone(),
                ) {
                    Ok(_id) => Ok(ExecutionResult::Ddl {
                        message: "CREATE ROLE".to_owned(),
                    }),
                    Err(e) => Err(e.into()),
                }
            }
            PhysicalPlan::DropRole { name, if_exists } => {
                self.reject_if_read_only("DROP ROLE")?;
                match self.storage.drop_role(name) {
                    Ok(()) => Ok(ExecutionResult::Ddl {
                        message: "DROP ROLE".to_owned(),
                    }),
                    Err(falcon_common::error::StorageError::RoleNotFound(_)) if *if_exists => {
                        Ok(ExecutionResult::Ddl {
                            message: "DROP ROLE".to_owned(),
                        })
                    }
                    Err(e) => Err(e.into()),
                }
            }
            PhysicalPlan::AlterRole {
                name,
                password,
                can_login,
                is_superuser,
                can_create_db,
                can_create_role,
            } => {
                self.reject_if_read_only("ALTER ROLE")?;
                self.storage.alter_role(
                    name,
                    password.clone(),
                    *can_login,
                    *is_superuser,
                    *can_create_db,
                    *can_create_role,
                )?;
                Ok(ExecutionResult::Ddl {
                    message: "ALTER ROLE".to_owned(),
                })
            }
            PhysicalPlan::Grant {
                privilege,
                object_type,
                object_name,
                grantee,
            } => {
                self.reject_if_read_only("GRANT")?;
                self.storage.grant_privilege(
                    grantee,
                    privilege,
                    object_type,
                    object_name,
                    "falcon",
                )?;
                Ok(ExecutionResult::Ddl {
                    message: "GRANT".to_owned(),
                })
            }
            PhysicalPlan::Revoke {
                privilege,
                object_type,
                object_name,
                grantee,
            } => {
                self.reject_if_read_only("REVOKE")?;
                self.storage
                    .revoke_privilege(grantee, privilege, object_type, object_name)?;
                Ok(ExecutionResult::Ddl {
                    message: "REVOKE".to_owned(),
                })
            }
            PhysicalPlan::ShowRoles => {
                let catalog = self.storage.get_catalog();
                let roles = catalog.list_role_entries();
                let columns = vec![
                    ("role_name".to_owned(), DataType::Text),
                    ("can_login".to_owned(), DataType::Boolean),
                    ("is_superuser".to_owned(), DataType::Boolean),
                    ("can_create_db".to_owned(), DataType::Boolean),
                    ("can_create_role".to_owned(), DataType::Boolean),
                ];
                let mut rows = Vec::new();
                for r in roles {
                    rows.push(OwnedRow::new(vec![
                        Datum::Text(r.name.clone()),
                        Datum::Boolean(r.can_login),
                        Datum::Boolean(r.is_superuser),
                        Datum::Boolean(r.can_create_db),
                        Datum::Boolean(r.can_create_role),
                    ]));
                }
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowSchemas => {
                let catalog = self.storage.get_catalog();
                let schemas = catalog.list_schemas();
                let columns = vec![
                    ("schema_name".to_owned(), DataType::Text),
                    ("owner".to_owned(), DataType::Text),
                ];
                let mut rows = Vec::new();
                for s in schemas {
                    rows.push(OwnedRow::new(vec![
                        Datum::Text(s.name.clone()),
                        Datum::Text(s.owner.clone()),
                    ]));
                }
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowGrants { role_name } => {
                let catalog = self.storage.get_catalog();
                let grants = catalog.list_grants();
                let roles = catalog.list_role_entries();
                let columns = vec![
                    ("grantee".to_owned(), DataType::Text),
                    ("privilege".to_owned(), DataType::Text),
                    ("object_type".to_owned(), DataType::Text),
                    ("object_name".to_owned(), DataType::Text),
                ];
                let mut rows = Vec::new();
                for g in grants {
                    let grantee_name = roles
                        .iter()
                        .find(|r| r.id == g.grantee_id)
                        .map_or_else(|| format!("id_{}", g.grantee_id), |r| r.name.clone());
                    if let Some(ref filter) = role_name {
                        if grantee_name.to_lowercase() != filter.to_lowercase() {
                            continue;
                        }
                    }
                    rows.push(OwnedRow::new(vec![
                        Datum::Text(grantee_name),
                        Datum::Text(g.privilege.clone()),
                        Datum::Text(g.object_type.clone()),
                        Datum::Text(g.object_name.clone()),
                    ]));
                }
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::CreateTable {
                schema,
                if_not_exists,
                partition_spec,
            } => {
                self.reject_if_read_only("CREATE TABLE")?;
                self.check_privilege(Privilege::Create, ObjectType::Schema, "public")?;
                self.exec_create_table(schema, *if_not_exists, partition_spec.as_ref())
            }
            PhysicalPlan::CreateTableAs {
                table_name,
                if_not_exists,
                query,
            } => {
                self.reject_if_read_only("CREATE TABLE AS")?;
                self.check_privilege(Privilege::Create, ObjectType::Schema, "public")?;
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("CREATE TABLE AS requires active transaction".into())
                })?;
                self.exec_create_table_as(table_name, *if_not_exists, query, txn)
            }
            PhysicalPlan::DropTable {
                table_name,
                if_exists,
            } => {
                self.reject_if_read_only("DROP TABLE")?;
                self.check_privilege(Privilege::All, ObjectType::Table, table_name)?;
                self.exec_drop_table(table_name, *if_exists)
            }
            PhysicalPlan::AlterTable { table_name, ops } => {
                self.reject_if_read_only("ALTER TABLE")?;
                self.check_privilege(Privilege::All, ObjectType::Table, table_name)?;
                self.exec_alter_table(table_name, ops)
            }
            PhysicalPlan::Insert {
                table_id,
                schema,
                columns,
                rows,
                source_select,
                returning,
                on_conflict,
            } => {
                self.reject_if_read_only("INSERT")?;
                self.check_privilege(Privilege::Insert, ObjectType::Table, &schema.name)?;
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("INSERT requires active transaction".into())
                })?;
                Self::reject_if_txn_read_only(txn, "INSERT")?;
                Self::check_txn_timeout(txn)?;
                if let Some(sel) = source_select {
                    self.exec_insert_select(*table_id, schema, columns, sel, txn)
                } else {
                    self.exec_insert(
                        *table_id,
                        schema,
                        columns,
                        rows,
                        returning,
                        on_conflict,
                        txn,
                    )
                }
            }
            PhysicalPlan::Update {
                table_id,
                schema,
                assignments,
                filter,
                returning,
                from_table,
            } => {
                self.reject_if_read_only("UPDATE")?;
                self.check_privilege(Privilege::Update, ObjectType::Table, &schema.name)?;
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("UPDATE requires active transaction".into())
                })?;
                Self::reject_if_txn_read_only(txn, "UPDATE")?;
                Self::check_txn_timeout(txn)?;
                self.exec_update(
                    *table_id,
                    schema,
                    assignments,
                    filter.as_ref(),
                    returning,
                    from_table.as_ref(),
                    txn,
                )
            }
            PhysicalPlan::Delete {
                table_id,
                schema,
                filter,
                returning,
                using_table,
            } => {
                self.reject_if_read_only("DELETE")?;
                self.check_privilege(Privilege::Delete, ObjectType::Table, &schema.name)?;
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("DELETE requires active transaction".into())
                })?;
                Self::reject_if_txn_read_only(txn, "DELETE")?;
                Self::check_txn_timeout(txn)?;
                self.exec_delete(
                    *table_id,
                    schema,
                    filter.as_ref(),
                    returning,
                    using_table.as_ref(),
                    txn,
                )
            }
            PhysicalPlan::Merge(merge) => {
                self.reject_if_read_only("MERGE")?;
                self.check_privilege(Privilege::Insert, ObjectType::Table, &merge.target_name)?;
                self.check_privilege(Privilege::Update, ObjectType::Table, &merge.target_name)?;
                self.check_privilege(Privilege::Delete, ObjectType::Table, &merge.target_name)?;
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("MERGE requires active transaction".into())
                })?;
                Self::reject_if_txn_read_only(txn, "MERGE")?;
                Self::check_txn_timeout(txn)?;
                self.exec_merge(merge, txn)
            }
            PhysicalPlan::SeqScan {
                table_id,
                schema,
                projections,
                visible_projection_count,
                filter,
                group_by,
                grouping_sets,
                having,
                order_by,
                limit,
                offset,
                distinct,
                ctes,
                unions,
                virtual_rows,
                for_lock,
            } => {
                self.check_privilege(Privilege::Select, ObjectType::Table, &schema.name)?;
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("SELECT requires active transaction".into())
                })?;
                self.register_row_locks(*table_id, for_lock, txn)?;
                // Materialize CTEs
                let cte_data = self.materialize_ctes(ctes, txn)?;
                let mut result = self.exec_seq_scan(
                    *table_id,
                    schema,
                    projections,
                    *visible_projection_count,
                    filter.as_ref(),
                    group_by,
                    grouping_sets,
                    having.as_ref(),
                    order_by,
                    *limit,
                    *offset,
                    distinct,
                    txn,
                    &cte_data,
                    virtual_rows,
                )?;
                if !unions.is_empty() {
                    result = self.exec_union(result, unions, txn)?;
                }
                Ok(result)
            }
            PhysicalPlan::ColumnScan {
                table_id,
                schema,
                projections,
                visible_projection_count,
                filter,
                group_by,
                grouping_sets,
                having,
                order_by,
                limit,
                offset,
                distinct,
                ctes,
                unions,
                for_lock,
            } => {
                self.check_privilege(Privilege::Select, ObjectType::Table, &schema.name)?;
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("SELECT requires active transaction".into())
                })?;
                self.register_row_locks(*table_id, for_lock, txn)?;

                // All columnar paths (pure AGG, GROUP BY AGG, plain scan) are
                // handled inside exec_seq_scan to avoid calling scan_columnar twice.
                let cte_data = self.materialize_ctes(ctes, txn)?;
                let mut result = self.exec_seq_scan(
                    *table_id,
                    schema,
                    projections,
                    *visible_projection_count,
                    filter.as_ref(),
                    group_by,
                    grouping_sets,
                    having.as_ref(),
                    order_by,
                    *limit,
                    *offset,
                    distinct,
                    txn,
                    &cte_data,
                    &[],
                )?;
                if !unions.is_empty() {
                    result = self.exec_union(result, unions, txn)?;
                }
                Ok(result)
            }
            PhysicalPlan::IndexScan {
                table_id,
                schema,
                index_col,
                index_value,
                projections,
                visible_projection_count,
                filter,
                group_by,
                grouping_sets,
                having,
                order_by,
                limit,
                offset,
                distinct,
                ctes,
                unions,
                virtual_rows,
                for_lock,
            } => {
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("SELECT requires active transaction".into())
                })?;
                self.register_row_locks(*table_id, for_lock, txn)?;
                let cte_data = self.materialize_ctes(ctes, txn)?;
                let mut result = self.exec_index_scan(
                    *table_id,
                    schema,
                    *index_col,
                    index_value,
                    projections,
                    *visible_projection_count,
                    filter.as_ref(),
                    group_by,
                    grouping_sets,
                    having.as_ref(),
                    order_by,
                    *limit,
                    *offset,
                    distinct,
                    txn,
                    &cte_data,
                    virtual_rows,
                )?;
                if !unions.is_empty() {
                    result = self.exec_union(result, unions, txn)?;
                }
                Ok(result)
            }
            PhysicalPlan::IndexRangeScan {
                table_id,
                schema,
                index_col,
                lower_bound,
                upper_bound,
                projections,
                visible_projection_count,
                filter,
                group_by,
                grouping_sets,
                having,
                order_by,
                limit,
                offset,
                distinct,
                ctes,
                unions,
                virtual_rows,
                for_lock,
            } => {
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("SELECT requires active transaction".into())
                })?;
                self.register_row_locks(*table_id, for_lock, txn)?;
                let cte_data = self.materialize_ctes(ctes, txn)?;
                let mut result = self.exec_index_range_scan(
                    *table_id,
                    schema,
                    *index_col,
                    lower_bound.as_ref(),
                    upper_bound.as_ref(),
                    projections,
                    *visible_projection_count,
                    filter.as_ref(),
                    group_by,
                    grouping_sets,
                    having.as_ref(),
                    order_by,
                    *limit,
                    *offset,
                    distinct,
                    txn,
                    &cte_data,
                    virtual_rows,
                )?;
                if !unions.is_empty() {
                    result = self.exec_union(result, unions, txn)?;
                }
                Ok(result)
            }
            PhysicalPlan::NestedLoopJoin {
                left_table_id,
                left_schema,
                joins,
                combined_schema,
                projections,
                visible_projection_count,
                filter,
                order_by,
                limit,
                offset,
                distinct,
                ctes,
                unions,
            } => {
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("SELECT requires active transaction".into())
                })?;
                let cte_data = self.materialize_ctes(ctes, txn)?;
                let mut result = self.exec_nested_loop_join(
                    *left_table_id,
                    left_schema,
                    joins,
                    combined_schema,
                    projections,
                    *visible_projection_count,
                    filter.as_ref(),
                    order_by,
                    *limit,
                    *offset,
                    distinct,
                    txn,
                    &cte_data,
                )?;
                if !unions.is_empty() {
                    result = self.exec_union(result, unions, txn)?;
                }
                Ok(result)
            }
            PhysicalPlan::HashJoin {
                left_table_id,
                left_schema,
                joins,
                combined_schema,
                projections,
                visible_projection_count,
                filter,
                order_by,
                limit,
                offset,
                distinct,
                ctes,
                unions,
            } => {
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("SELECT requires active transaction".into())
                })?;
                let cte_data = self.materialize_ctes(ctes, txn)?;
                let mut result = self.exec_hash_join(
                    *left_table_id,
                    left_schema,
                    joins,
                    combined_schema,
                    projections,
                    *visible_projection_count,
                    filter.as_ref(),
                    order_by,
                    *limit,
                    *offset,
                    distinct,
                    txn,
                    &cte_data,
                )?;
                if !unions.is_empty() {
                    result = self.exec_union(result, unions, txn)?;
                }
                Ok(result)
            }
            PhysicalPlan::MergeSortJoin {
                left_table_id,
                left_schema,
                joins,
                combined_schema,
                projections,
                visible_projection_count,
                filter,
                order_by,
                limit,
                offset,
                distinct,
                ctes,
                unions,
            } => {
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("SELECT requires active transaction".into())
                })?;
                let cte_data = self.materialize_ctes(ctes, txn)?;
                let mut result = self.exec_merge_sort_join(
                    *left_table_id,
                    left_schema,
                    joins,
                    combined_schema,
                    projections,
                    *visible_projection_count,
                    filter.as_ref(),
                    order_by,
                    *limit,
                    *offset,
                    distinct,
                    txn,
                    &cte_data,
                )?;
                if !unions.is_empty() {
                    result = self.exec_union(result, unions, txn)?;
                }
                Ok(result)
            }
            PhysicalPlan::Truncate { table_name } => {
                self.reject_if_read_only("TRUNCATE")?;
                self.storage.truncate_table(table_name)?;
                Ok(ExecutionResult::Dml {
                    rows_affected: 0,
                    tag: "TRUNCATE TABLE",
                })
            }
            PhysicalPlan::Explain(inner) => {
                let plan_text = self.format_plan(inner, 0);
                let rows = plan_text
                    .into_iter()
                    .map(|line| OwnedRow::new(vec![Datum::Text(line)]))
                    .collect();
                Ok(ExecutionResult::Query {
                    columns: vec![("QUERY PLAN".into(), DataType::Text)],
                    rows,
                })
            }
            PhysicalPlan::ExplainAnalyze(inner) => {
                let plan_text = self.format_plan(inner, 0);
                let start = Instant::now();
                let result = self.execute(inner, txn)?;
                let elapsed = start.elapsed();
                let (actual_rows, actual_cols) = match &result {
                    ExecutionResult::Query { rows, columns } => (rows.len(), columns.len()),
                    ExecutionResult::Dml { rows_affected, .. } => (*rows_affected as usize, 0),
                    _ => (0, 0),
                };
                let mut rows = Vec::new();
                for line in &plan_text {
                    rows.push(OwnedRow::new(vec![Datum::Text(line.clone())]));
                }
                rows.push(OwnedRow::new(vec![Datum::Text(format!(
                    "Actual: rows={}, cols={}, time={:.3}ms",
                    actual_rows,
                    actual_cols,
                    elapsed.as_secs_f64() * 1000.0
                ))]));
                Ok(ExecutionResult::Query {
                    columns: vec![("QUERY PLAN".into(), DataType::Text)],
                    rows,
                })
            }
            PhysicalPlan::CreateIndex {
                index_name,
                table_name,
                column_indices,
                unique,
                concurrently,
            } => {
                self.reject_if_read_only("CREATE INDEX")?;
                if *concurrently {
                    let ddl_id = self.storage.create_index_concurrently(
                        index_name,
                        table_name,
                        column_indices,
                        *unique,
                    )?;
                    Ok(ExecutionResult::Ddl {
                        message: format!(
                            "CREATE INDEX CONCURRENTLY {index_name} (ddl_id={ddl_id})"
                        ),
                    })
                } else {
                    for &col_idx in column_indices {
                        self.storage
                            .create_named_index(index_name, table_name, col_idx, *unique)?;
                    }
                    Ok(ExecutionResult::Ddl {
                        message: format!("CREATE INDEX {index_name}"),
                    })
                }
            }
            PhysicalPlan::CreateView {
                name,
                query_sql,
                or_replace,
            } => {
                self.reject_if_read_only("CREATE VIEW")?;
                self.storage.create_view(name, query_sql, *or_replace)?;
                Ok(ExecutionResult::Ddl {
                    message: format!("CREATE VIEW {name}"),
                })
            }
            PhysicalPlan::DropView { name, if_exists } => {
                self.reject_if_read_only("DROP VIEW")?;
                self.storage.drop_view(name, *if_exists)?;
                Ok(ExecutionResult::Ddl {
                    message: format!("DROP VIEW {name}"),
                })
            }
            PhysicalPlan::CreateMaterializedView { name, query_sql } => {
                self.reject_if_read_only("CREATE MATERIALIZED VIEW")?;
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal(
                        "CREATE MATERIALIZED VIEW requires active transaction".into(),
                    )
                })?;
                self.exec_create_materialized_view(name, query_sql, txn)?;
                Ok(ExecutionResult::Ddl {
                    message: format!("CREATE MATERIALIZED VIEW {name}"),
                })
            }
            PhysicalPlan::DropMaterializedView { name, if_exists } => {
                self.reject_if_read_only("DROP MATERIALIZED VIEW")?;
                self.storage.drop_materialized_view(name, *if_exists)?;
                Ok(ExecutionResult::Ddl {
                    message: format!("DROP MATERIALIZED VIEW {name}"),
                })
            }
            PhysicalPlan::RefreshMaterializedView { name } => {
                self.reject_if_read_only("REFRESH MATERIALIZED VIEW")?;
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal(
                        "REFRESH MATERIALIZED VIEW requires active transaction".into(),
                    )
                })?;
                self.exec_refresh_materialized_view(name, txn)?;
                Ok(ExecutionResult::Ddl {
                    message: format!("REFRESH MATERIALIZED VIEW {name}"),
                })
            }
            PhysicalPlan::DropIndex { index_name } => {
                self.reject_if_read_only("DROP INDEX")?;
                self.storage.drop_index(index_name)?;
                Ok(ExecutionResult::Ddl {
                    message: format!("DROP INDEX {index_name}"),
                })
            }
            PhysicalPlan::Begin | PhysicalPlan::Commit | PhysicalPlan::Rollback => {
                // Handled by session layer, not executor
                Ok(ExecutionResult::TxnControl {
                    action: match plan {
                        PhysicalPlan::Begin => "BEGIN".into(),
                        PhysicalPlan::Commit => "COMMIT".into(),
                        PhysicalPlan::Rollback => "ROLLBACK".into(),
                        _ => unreachable!(),
                    },
                })
            }
            PhysicalPlan::ShowTxnStats => {
                let stats = self.txn_mgr.stats_snapshot();
                let columns = vec![
                    ("metric".into(), DataType::Text),
                    ("value".into(), DataType::Int64),
                ];
                let rows = vec![
                    OwnedRow::new(vec![
                        Datum::Text("total_committed".into()),
                        Datum::Int64(stats.total_committed as i64),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("fast_path_commits".into()),
                        Datum::Int64(stats.fast_path_commits as i64),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("slow_path_commits".into()),
                        Datum::Int64(stats.slow_path_commits as i64),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("total_aborted".into()),
                        Datum::Int64(stats.total_aborted as i64),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("occ_conflicts".into()),
                        Datum::Int64(stats.occ_conflicts as i64),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("degraded_to_global".into()),
                        Datum::Int64(stats.degraded_to_global as i64),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("active_count".into()),
                        Datum::Int64(stats.active_count as i64),
                    ]),
                ];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowNodeRole => {
                let role = falcon_common::globals::node_role().to_owned();
                let columns = vec![
                    ("metric".into(), DataType::Text),
                    ("value".into(), DataType::Text),
                ];
                let rows = vec![OwnedRow::new(vec![
                    Datum::Text("role".into()),
                    Datum::Text(role),
                ])];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowWalStats => {
                let ws = self.storage.wal_stats_snapshot();
                let wal_enabled = self.storage.is_wal_enabled();
                let columns = vec![
                    ("metric".into(), DataType::Text),
                    ("value".into(), DataType::Text),
                ];
                let rows = vec![
                    OwnedRow::new(vec![
                        Datum::Text("wal_enabled".into()),
                        Datum::Text(wal_enabled.to_string()),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("records_written".into()),
                        Datum::Text(ws.records_written.to_string()),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("observer_notifications".into()),
                        Datum::Text(ws.observer_notifications.to_string()),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("flushes".into()),
                        Datum::Text(ws.flushes.to_string()),
                    ]),
                ];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowConnections => {
                let active = self
                    .active_connections
                    .as_ref()
                    .map_or(0, |a| a.load(std::sync::atomic::Ordering::Relaxed));
                let columns = vec![
                    ("metric".into(), DataType::Text),
                    ("value".into(), DataType::Text),
                ];
                let rows = vec![
                    OwnedRow::new(vec![
                        Datum::Text("active_connections".into()),
                        Datum::Text(active.to_string()),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("max_connections".into()),
                        Datum::Text(self.max_connections.to_string()),
                    ]),
                ];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::RunGc => {
                let watermark = self.txn_mgr.min_active_ts();
                let chains_processed = self.storage.run_gc(watermark);
                let columns = vec![
                    ("metric".into(), DataType::Text),
                    ("value".into(), DataType::Int64),
                ];
                let rows = vec![
                    OwnedRow::new(vec![
                        Datum::Text("watermark_ts".into()),
                        Datum::Int64(watermark.0 as i64),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("chains_processed".into()),
                        Datum::Int64(chains_processed as i64),
                    ]),
                ];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::Analyze { table_name } => {
                let stats = self.storage.analyze_table(table_name)?;
                let columns = vec![
                    ("table_name".into(), DataType::Text),
                    ("row_count".into(), DataType::Int64),
                    ("columns_analyzed".into(), DataType::Int64),
                ];
                let rows = vec![OwnedRow::new(vec![
                    Datum::Text(stats.table_name),
                    Datum::Int64(stats.row_count as i64),
                    Datum::Int64(stats.column_stats.len() as i64),
                ])];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowTableStats { table_name } => {
                let all_stats = if let Some(ref name) = table_name {
                    let schema = self.storage.get_table_schema(name);
                    if let Some(s) = schema {
                        self.storage.get_table_stats(s.id).into_iter().collect()
                    } else {
                        vec![]
                    }
                } else {
                    self.storage.get_all_table_stats()
                };
                let columns = vec![
                    ("table_name".into(), DataType::Text),
                    ("column_name".into(), DataType::Text),
                    ("row_count".into(), DataType::Int64),
                    ("distinct_count".into(), DataType::Int64),
                    ("null_count".into(), DataType::Int64),
                    ("min_value".into(), DataType::Text),
                    ("max_value".into(), DataType::Text),
                    ("avg_width".into(), DataType::Int64),
                ];
                let mut rows = Vec::new();
                for ts in &all_stats {
                    for cs in &ts.column_stats {
                        rows.push(OwnedRow::new(vec![
                            Datum::Text(ts.table_name.clone()),
                            Datum::Text(cs.column_name.clone()),
                            Datum::Int64(ts.row_count as i64),
                            Datum::Int64(cs.distinct_count as i64),
                            Datum::Int64(cs.null_count as i64),
                            Datum::Text(
                                cs.min_value
                                    .as_ref()
                                    .map(|d| format!("{d}"))
                                    .unwrap_or_default(),
                            ),
                            Datum::Text(
                                cs.max_value
                                    .as_ref()
                                    .map(|d| format!("{d}"))
                                    .unwrap_or_default(),
                            ),
                            Datum::Int64(i64::from(cs.avg_width)),
                        ]));
                    }
                }
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::CreateSequence { name, start } => {
                self.reject_if_read_only("CREATE SEQUENCE")?;
                self.storage
                    .create_sequence(name, *start)
                    .map_err(FalconError::Storage)?;
                Ok(ExecutionResult::Ddl {
                    message: format!("CREATE SEQUENCE {name}"),
                })
            }
            PhysicalPlan::DropSequence { name, if_exists } => {
                self.reject_if_read_only("DROP SEQUENCE")?;
                match self.storage.drop_sequence(name) {
                    Ok(_) => Ok(ExecutionResult::Ddl {
                        message: format!("DROP SEQUENCE {name}"),
                    }),
                    Err(_) if *if_exists => Ok(ExecutionResult::Ddl {
                        message: format!("DROP SEQUENCE IF EXISTS {name}"),
                    }),
                    Err(e) => Err(FalconError::Storage(e)),
                }
            }
            PhysicalPlan::ShowSequences => {
                let seqs = self.storage.list_sequences();
                let columns = vec![
                    ("sequence_name".into(), DataType::Text),
                    ("current_value".into(), DataType::Int64),
                ];
                let rows: Vec<OwnedRow> = seqs
                    .into_iter()
                    .map(|(name, val)| OwnedRow::new(vec![Datum::Text(name), Datum::Int64(val)]))
                    .collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowTenants | PhysicalPlan::ShowTenantUsage => {
                Ok(ExecutionResult::Query {
                    columns: vec![
                        ("metric".into(), DataType::Text),
                        ("value".into(), DataType::Text),
                    ],
                    rows: vec![OwnedRow::new(vec![
                        Datum::Text("tenant_count".into()),
                        Datum::Text("0".into()),
                    ])],
                })
            }
            PhysicalPlan::CreateTenant {
                name,
                max_qps: _,
                max_storage_bytes: _,
            } => Ok(ExecutionResult::Ddl {
                message: format!("CREATE TENANT {name}"),
            }),
            PhysicalPlan::DropTenant { name } => Ok(ExecutionResult::Ddl {
                message: format!("DROP TENANT {name}"),
            }),
            PhysicalPlan::CopyFrom { .. } => {
                // CopyFrom is handled specially by the protocol layer.
                // The executor processes the received data via exec_copy_from_data.
                Err(FalconError::Internal(
                    "CopyFrom must be handled by the protocol layer".into(),
                ))
            }
            PhysicalPlan::CopyTo {
                table_id,
                schema,
                columns,
                csv,
                delimiter,
                header,
                null_string,
                quote,
                escape,
                file_path: _,
            } => {
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("COPY TO requires active transaction".into())
                })?;
                self.exec_copy_to(
                    *table_id,
                    schema,
                    columns,
                    *csv,
                    *delimiter,
                    *header,
                    null_string,
                    *quote,
                    *escape,
                    txn,
                )
            }
            PhysicalPlan::CopyQueryTo {
                query,
                csv,
                delimiter,
                header,
                null_string,
                quote,
                escape,
                file_path: _,
            } => {
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("COPY TO requires active transaction".into())
                })?;
                self.exec_copy_query_to(
                    query,
                    *csv,
                    *delimiter,
                    *header,
                    null_string,
                    *quote,
                    *escape,
                    txn,
                )
            }
            PhysicalPlan::CreateFunction { def } => {
                self.reject_if_read_only("CREATE FUNCTION")?;
                self.storage
                    .create_function(def.clone())
                    .map_err(FalconError::Storage)?;
                Ok(ExecutionResult::Ddl {
                    message: "CREATE FUNCTION".to_owned(),
                })
            }
            PhysicalPlan::DropFunction { name, if_exists } => {
                self.reject_if_read_only("DROP FUNCTION")?;
                match self.storage.drop_function(name) {
                    Ok(()) => Ok(ExecutionResult::Ddl {
                        message: "DROP FUNCTION".to_owned(),
                    }),
                    Err(falcon_common::error::StorageError::FunctionNotFound(_)) if *if_exists => {
                        Ok(ExecutionResult::Ddl {
                            message: "DROP FUNCTION".to_owned(),
                        })
                    }
                    Err(e) => Err(e.into()),
                }
            }
            PhysicalPlan::CallProcedure { name, args } => {
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("CALL requires active transaction".into())
                })?;
                self.exec_call_procedure(name, args, txn)
            }
            PhysicalPlan::DoBlock { body, language } => {
                let txn = txn.ok_or_else(|| {
                    FalconError::Internal("DO requires active transaction".into())
                })?;
                self.exec_do_block(body, language, txn)
            }
            PhysicalPlan::CreateTrigger { trigger } => {
                self.reject_if_read_only("CREATE TRIGGER")?;
                self.storage
                    .create_trigger(trigger.clone())
                    .map_err(FalconError::Storage)?;
                Ok(ExecutionResult::Ddl {
                    message: "CREATE TRIGGER".to_owned(),
                })
            }
            PhysicalPlan::DropTrigger { trigger_name, table_name, if_exists } => {
                self.reject_if_read_only("DROP TRIGGER")?;
                match self.storage.drop_trigger(table_name, trigger_name) {
                    Ok(()) => Ok(ExecutionResult::Ddl {
                        message: "DROP TRIGGER".to_owned(),
                    }),
                    Err(_) if *if_exists => Ok(ExecutionResult::Ddl {
                        message: "DROP TRIGGER".to_owned(),
                    }),
                    Err(e) => Err(FalconError::Storage(e)),
                }
            }
            PhysicalPlan::CreateType { name, labels } => {
                self.reject_if_read_only("CREATE TYPE")?;
                use falcon_common::schema::EnumTypeDef;
                let def = EnumTypeDef { name: name.clone(), labels: labels.clone() };
                self.storage
                    .create_enum_type(def)
                    .map_err(FalconError::Storage)?;
                Ok(ExecutionResult::Ddl { message: "CREATE TYPE".to_owned() })
            }
            PhysicalPlan::DropType { name, if_exists } => {
                self.reject_if_read_only("DROP TYPE")?;
                self.storage
                    .drop_enum_type(name, *if_exists)
                    .map_err(FalconError::Storage)?;
                Ok(ExecutionResult::Ddl { message: "DROP TYPE".to_owned() })
            }
            PhysicalPlan::NoOp => Ok(ExecutionResult::Ddl {
                message: "DO".to_owned(),
            }),
            PhysicalPlan::ShowPgVar { name, value } => {
                use falcon_common::datum::{Datum, OwnedRow};
                use falcon_common::types::DataType;
                Ok(ExecutionResult::Query {
                    columns: vec![(name.clone(), DataType::Text)],
                    rows: vec![OwnedRow::new(vec![Datum::Text(value.clone())])],
                })
            }
            PhysicalPlan::ShowDdlStatus => {
                use falcon_common::datum::{Datum, OwnedRow};
                use falcon_common::types::DataType;
                let progress = self.storage.ddl_progress();
                let columns = vec![
                    ("ddl_id".to_owned(), DataType::Int64),
                    ("description".to_owned(), DataType::Text),
                    ("phase".to_owned(), DataType::Text),
                    ("rows_processed".to_owned(), DataType::Int64),
                    ("rows_total".to_owned(), DataType::Int64),
                    ("pct".to_owned(), DataType::Float64),
                    ("elapsed_ms".to_owned(), DataType::Int64),
                    ("error".to_owned(), DataType::Text),
                ];
                let rows = progress
                    .into_iter()
                    .map(|p| {
                        OwnedRow::new(vec![
                            Datum::Int64(p.id as i64),
                            Datum::Text(p.description),
                            Datum::Text(p.phase.to_string()),
                            Datum::Int64(p.rows_processed as i64),
                            Datum::Int64(p.rows_total as i64),
                            Datum::Float64(p.pct),
                            Datum::Int64(p.elapsed_ms.unwrap_or(0) as i64),
                            p.error.map_or(Datum::Null, Datum::Text),
                        ])
                    })
                    .collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::Backup {
                dest_dir,
                incremental,
                label,
            } => {
                use falcon_common::datum::{Datum, OwnedRow};
                use falcon_common::types::DataType;
                let result = if *incremental {
                    self.storage.backup_incremental(dest_dir, label)
                } else {
                    self.storage.backup_full(dest_dir, label)
                };
                match result {
                    Ok(backup_id) => Ok(ExecutionResult::Query {
                        columns: vec![
                            ("backup_id".to_owned(), DataType::Int64),
                            ("status".to_owned(), DataType::Text),
                        ],
                        rows: vec![OwnedRow::new(vec![
                            Datum::Int64(backup_id as i64),
                            Datum::Text("completed".to_owned()),
                        ])],
                    }),
                    Err(e) => Err(FalconError::Storage(e)),
                }
            }
            PhysicalPlan::Restore { src_dir } => {
                use falcon_common::datum::{Datum, OwnedRow};
                use falcon_common::types::DataType;
                match self.storage.restore_from_backup(src_dir) {
                    Ok(rows) => Ok(ExecutionResult::Query {
                        columns: vec![("rows_restored".to_owned(), DataType::Int64)],
                        rows: vec![OwnedRow::new(vec![Datum::Int64(rows as i64)])],
                    }),
                    Err(e) => Err(FalconError::Storage(e)),
                }
            }
            PhysicalPlan::CreateJob {
                name,
                backup_type,
                dest_dir,
                interval_secs,
            } => {
                use falcon_common::datum::{Datum, OwnedRow};
                use falcon_common::types::DataType;
                use falcon_storage::backup::BackupType;
                let btype = if backup_type.eq_ignore_ascii_case("incremental") {
                    BackupType::Incremental
                } else {
                    BackupType::Full
                };
                let arc_engine: std::sync::Arc<falcon_storage::engine::StorageEngine> =
                    std::sync::Arc::clone(&self.storage);
                let job_id = arc_engine.schedule_recurring_backup(
                    btype,
                    dest_dir.clone(),
                    name.clone(),
                    *interval_secs,
                );
                Ok(ExecutionResult::Query {
                    columns: vec![
                        ("job_id".to_owned(), DataType::Int64),
                        ("name".to_owned(), DataType::Text),
                        ("interval_secs".to_owned(), DataType::Int64),
                    ],
                    rows: vec![OwnedRow::new(vec![
                        Datum::Int64(job_id as i64),
                        Datum::Text(name.clone()),
                        Datum::Int64(*interval_secs as i64),
                    ])],
                })
            }
            PhysicalPlan::DropJob { job_id } => {
                use falcon_common::datum::{Datum, OwnedRow};
                use falcon_common::types::DataType;
                let cancelled = self.storage.cancel_job(*job_id);
                Ok(ExecutionResult::Query {
                    columns: vec![("cancelled".to_owned(), DataType::Boolean)],
                    rows: vec![OwnedRow::new(vec![Datum::Boolean(cancelled)])],
                })
            }
            PhysicalPlan::ShowJobs => {
                use falcon_common::datum::{Datum, OwnedRow};
                use falcon_common::types::DataType;
                let jobs = self.storage.job_statuses();
                let columns = vec![
                    ("job_id".to_owned(), DataType::Int64),
                    ("name".to_owned(), DataType::Text),
                    ("interval_secs".to_owned(), DataType::Int64),
                    ("state".to_owned(), DataType::Text),
                    ("runs".to_owned(), DataType::Int64),
                    ("last_run_ms".to_owned(), DataType::Int64),
                    ("last_error".to_owned(), DataType::Text),
                ];
                let rows = jobs
                    .into_iter()
                    .map(|j| {
                        OwnedRow::new(vec![
                            Datum::Int64(j.id as i64),
                            Datum::Text(j.name),
                            j.interval_secs
                                .map_or(Datum::Null, |s| Datum::Int64(s as i64)),
                            Datum::Text(j.state.to_string()),
                            Datum::Int64(j.runs as i64),
                            Datum::Int64(j.last_run_ms as i64),
                            j.last_error.map_or(Datum::Null, Datum::Text),
                        ])
                    })
                    .collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowBackupStatus => {
                use falcon_common::datum::{Datum, OwnedRow};
                use falcon_common::types::DataType;
                let history = self.storage.backup_history(50);
                let columns = vec![
                    ("backup_id".to_owned(), DataType::Int64),
                    ("backup_type".to_owned(), DataType::Text),
                    ("status".to_owned(), DataType::Text),
                    ("label".to_owned(), DataType::Text),
                    ("start_lsn".to_owned(), DataType::Int64),
                    ("end_lsn".to_owned(), DataType::Int64),
                    ("size_bytes".to_owned(), DataType::Int64),
                    ("table_count".to_owned(), DataType::Int64),
                ];
                let rows = history
                    .into_iter()
                    .map(|b| {
                        OwnedRow::new(vec![
                            Datum::Int64(b.backup_id as i64),
                            Datum::Text(format!("{}", b.backup_type)),
                            Datum::Text(format!("{:?}", b.status)),
                            Datum::Text(b.label),
                            Datum::Int64(b.start_lsn as i64),
                            Datum::Int64(b.end_lsn as i64),
                            Datum::Int64(b.total_bytes as i64),
                            Datum::Int64(b.table_count as i64),
                        ])
                    })
                    .collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowAiStats => {
                use falcon_common::datum::{Datum, OwnedRow};
                use falcon_common::types::DataType;
                let diag = self.ai_optimizer.diagnostics();
                let columns = vec![
                    ("metric".to_owned(), DataType::Text),
                    ("value".to_owned(), DataType::Text),
                ];
                let rows = vec![
                    OwnedRow::new(vec![
                        Datum::Text("enabled".into()),
                        Datum::Text(diag.enabled.to_string()),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("samples_trained".into()),
                        Datum::Text(diag.n_samples.to_string()),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("model_ready".into()),
                        Datum::Text(diag.model_ready.to_string()),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("ema_mae_log2".into()),
                        Datum::Text(format!("{:.4}", diag.ema_mae)),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("query_fingerprints".into()),
                        Datum::Text(diag.n_fingerprints.to_string()),
                    ]),
                ];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowAiopsStats => {
                use falcon_common::types::DataType;
                let s = self.aiops.stats();
                let columns = vec![
                    ("metric".to_owned(), DataType::Text),
                    ("value".to_owned(), DataType::Text),
                ];
                let rows = vec![
                    OwnedRow::new(vec![Datum::Text("slow_query_samples".into()), Datum::Text(s.slow_query_total_samples.to_string())]),
                    OwnedRow::new(vec![Datum::Text("slow_query_threshold_us".into()), Datum::Text(s.slow_query_threshold_us.to_string())]),
                    OwnedRow::new(vec![Datum::Text("slow_query_ema_baseline_us".into()), Datum::Text(s.slow_query_ema_baseline_us.to_string())]),
                    OwnedRow::new(vec![Datum::Text("slow_query_logged".into()), Datum::Text(s.slow_query_logged.to_string())]),
                    OwnedRow::new(vec![Datum::Text("anomaly_tps_mean".into()), Datum::Text(format!("{:.2}", s.anomaly_tps_mean))]),
                    OwnedRow::new(vec![Datum::Text("anomaly_tps_stddev".into()), Datum::Text(format!("{:.2}", s.anomaly_tps_stddev))]),
                    OwnedRow::new(vec![Datum::Text("anomaly_latency_mean_us".into()), Datum::Text(format!("{:.2}", s.anomaly_latency_mean_us))]),
                    OwnedRow::new(vec![Datum::Text("anomaly_latency_stddev_us".into()), Datum::Text(format!("{:.2}", s.anomaly_latency_stddev_us))]),
                    OwnedRow::new(vec![Datum::Text("anomaly_alerts_total".into()), Datum::Text(s.anomaly_alerts_total.to_string())]),
                    OwnedRow::new(vec![Datum::Text("index_advisor_tables_tracked".into()), Datum::Text(s.index_advisor_tables_tracked.to_string())]),
                    OwnedRow::new(vec![Datum::Text("index_advisor_advice_count".into()), Datum::Text(s.index_advisor_advice_count.to_string())]),
                    OwnedRow::new(vec![Datum::Text("workload_fingerprints".into()), Datum::Text(s.workload_fingerprints.to_string())]),
                ];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowAiopsAlerts => {
                use falcon_common::types::DataType;
                let alerts = self.aiops.recent_alerts(50);
                let columns = vec![
                    ("id".to_owned(), DataType::Int64),
                    ("ts_unix_ms".to_owned(), DataType::Int64),
                    ("severity".to_owned(), DataType::Text),
                    ("source".to_owned(), DataType::Text),
                    ("message".to_owned(), DataType::Text),
                ];
                let rows = alerts.iter().map(|a| OwnedRow::new(vec![
                    Datum::Int64(a.id as i64),
                    Datum::Int64(a.ts_unix_ms as i64),
                    Datum::Text(a.severity.to_string()),
                    Datum::Text(a.source.clone()),
                    Datum::Text(a.message.clone()),
                ])).collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowAiopsIndexAdvice => {
                use falcon_common::types::DataType;
                let advice = self.aiops.index_advice();
                let columns = vec![
                    ("table_name".to_owned(), DataType::Text),
                    ("full_scan_count".to_owned(), DataType::Int64),
                    ("avg_scan_duration_us".to_owned(), DataType::Int64),
                    ("column_hints".to_owned(), DataType::Text),
                    ("suggestion".to_owned(), DataType::Text),
                ];
                let rows = advice.iter().map(|a| OwnedRow::new(vec![
                    Datum::Text(a.table_name.clone()),
                    Datum::Int64(a.full_scan_count as i64),
                    Datum::Int64(a.avg_scan_duration_us as i64),
                    Datum::Text(a.column_hints.join(", ")),
                    Datum::Text(a.suggestion.clone()),
                ])).collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowAiopsWorkload => {
                use falcon_common::types::DataType;
                let workloads = self.aiops.top_workloads(20);
                let columns = vec![
                    ("fingerprint".to_owned(), DataType::Text),
                    ("call_count".to_owned(), DataType::Int64),
                    ("avg_us".to_owned(), DataType::Int64),
                    ("p95_us".to_owned(), DataType::Int64),
                    ("p99_us".to_owned(), DataType::Int64),
                    ("max_us".to_owned(), DataType::Int64),
                    ("total_us".to_owned(), DataType::Int64),
                    ("error_count".to_owned(), DataType::Int64),
                ];
                let rows = workloads.iter().map(|w| OwnedRow::new(vec![
                    Datum::Text(w.fingerprint.clone()),
                    Datum::Int64(w.call_count as i64),
                    Datum::Int64(w.avg_us() as i64),
                    Datum::Int64(w.p95_us() as i64),
                    Datum::Int64(w.p99_us() as i64),
                    Datum::Int64(w.max_us as i64),
                    Datum::Int64(w.total_us as i64),
                    Datum::Int64(w.error_count as i64),
                ])).collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowAiopsSlowQueries => {
                use falcon_common::types::DataType;
                let records = self.aiops.slow_queries(100);
                let columns = vec![
                    ("ts_unix_ms".to_owned(), DataType::Int64),
                    ("duration_us".to_owned(), DataType::Int64),
                    ("threshold_us".to_owned(), DataType::Int64),
                    ("plan_kind".to_owned(), DataType::Text),
                    ("table_name".to_owned(), DataType::Text),
                    ("sql_text".to_owned(), DataType::Text),
                ];
                let rows = records.iter().map(|r| OwnedRow::new(vec![
                    Datum::Int64(r.ts_unix_ms as i64),
                    Datum::Int64(r.duration_us as i64),
                    Datum::Int64(r.threshold_us as i64),
                    Datum::Text(r.plan_kind.clone()),
                    Datum::Text(r.table_name.clone()),
                    Datum::Text(r.sql_text.clone()),
                ])).collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            // ── Enterprise: RLS ────────────────────────────────────────────
            PhysicalPlan::AlterTableEnableRls { table_name, enable } => {
                self.rls_registry.set_enabled(table_name, *enable);
                let msg = if *enable {
                    format!("Row level security enabled on \"{}\"", table_name)
                } else {
                    format!("Row level security disabled on \"{}\"", table_name)
                };
                Ok(ExecutionResult::Ddl { message: msg })
            }
            PhysicalPlan::CreatePolicy { policy_name, table_name, command, permissive, using_expr, check_expr } => {
                self.rls_registry.add_policy(
                    table_name,
                    policy_name,
                    command,
                    *permissive,
                    using_expr.clone(),
                    check_expr.clone(),
                );
                Ok(ExecutionResult::Ddl { message: format!("Policy \"{}\" created on \"{}\"", policy_name, table_name) })
            }
            PhysicalPlan::DropPolicy { policy_name, table_name, if_exists } => {
                let dropped = self.rls_registry.drop_policy(table_name, policy_name);
                if !dropped && !if_exists {
                    return Err(FalconError::Sql(falcon_common::error::SqlError::UnknownTable(
                        format!("policy \"{}\" on \"{}\" does not exist", policy_name, table_name),
                    )));
                }
                Ok(ExecutionResult::Ddl { message: format!("Policy \"{}\" dropped", policy_name) })
            }
            PhysicalPlan::ShowPolicies { table_name } => {
                use falcon_common::types::DataType;
                let policies = self.rls_registry.list_policies(table_name.as_deref());
                let columns = vec![
                    ("table_name".to_owned(), DataType::Text),
                    ("policy_name".to_owned(), DataType::Text),
                    ("command".to_owned(), DataType::Text),
                    ("permissive".to_owned(), DataType::Boolean),
                    ("using_expr".to_owned(), DataType::Text),
                    ("check_expr".to_owned(), DataType::Text),
                    ("rls_enabled".to_owned(), DataType::Boolean),
                ];
                let rows = policies.into_iter().map(|p| OwnedRow::new(vec![
                    Datum::Text(p.0),
                    Datum::Text(p.1),
                    Datum::Text(p.2),
                    Datum::Boolean(p.3),
                    p.4.map_or(Datum::Null, Datum::Text),
                    p.5.map_or(Datum::Null, Datum::Text),
                    Datum::Boolean(p.6),
                ])).collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            // ── Enterprise: Replication Slots ─────────────────────────────
            PhysicalPlan::CreateReplicationSlot { slot_name, plugin } => {
                use falcon_common::types::DataType;
                self.repl_slots.create(slot_name, plugin);
                let lsn = self.repl_slots.current_lsn();
                let columns = vec![
                    ("slot_name".to_owned(), DataType::Text),
                    ("plugin".to_owned(), DataType::Text),
                    ("confirmed_flush_lsn".to_owned(), DataType::Int64),
                ];
                let rows = vec![OwnedRow::new(vec![
                    Datum::Text(slot_name.clone()),
                    Datum::Text(plugin.clone()),
                    Datum::Int64(lsn as i64),
                ])];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::DropReplicationSlot { slot_name } => {
                self.repl_slots.remove(slot_name);
                Ok(ExecutionResult::Ddl { message: format!("Replication slot \"{}\" dropped", slot_name) })
            }
            PhysicalPlan::ShowReplicationSlots => {
                use falcon_common::types::DataType;
                let slots = self.repl_slots.list();
                let columns = vec![
                    ("slot_name".to_owned(), DataType::Text),
                    ("plugin".to_owned(), DataType::Text),
                    ("confirmed_flush_lsn".to_owned(), DataType::Int64),
                    ("restart_lsn".to_owned(), DataType::Int64),
                    ("active".to_owned(), DataType::Boolean),
                    ("created_at_unix_ms".to_owned(), DataType::Int64),
                ];
                let rows = slots.into_iter().map(|s| OwnedRow::new(vec![
                    Datum::Text(s.name),
                    Datum::Text(s.plugin),
                    Datum::Int64(s.confirmed_flush_lsn as i64),
                    Datum::Int64(s.restart_lsn as i64),
                    Datum::Boolean(s.active),
                    Datum::Int64(s.created_at_unix_ms as i64),
                ])).collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            // ── Enterprise: Audit Log ─────────────────────────────────────
            PhysicalPlan::ShowAuditLog { limit } => {
                use falcon_common::types::DataType;
                let events = self.dml_audit.snapshot(*limit);
                let columns = vec![
                    ("event_id".to_owned(), DataType::Int64),
                    ("timestamp_ms".to_owned(), DataType::Int64),
                    ("event_type".to_owned(), DataType::Text),
                    ("role_name".to_owned(), DataType::Text),
                    ("detail".to_owned(), DataType::Text),
                    ("sql".to_owned(), DataType::Text),
                    ("success".to_owned(), DataType::Boolean),
                ];
                let rows = events.into_iter().map(|e| OwnedRow::new(vec![
                    Datum::Int64(e.event_id as i64),
                    Datum::Int64(e.timestamp_ms as i64),
                    Datum::Text(e.event_type.clone()),
                    Datum::Text(e.role_name.clone()),
                    Datum::Text(e.detail.clone()),
                    e.sql.clone().map_or(Datum::Null, Datum::Text),
                    Datum::Boolean(e.success),
                ])).collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowAuditLogForTable { table_name, limit } => {
                use falcon_common::types::DataType;
                let events = self.dml_audit.snapshot(*limit);
                let events: Vec<_> = events.into_iter()
                    .filter(|e| e.sql.as_deref().map_or(false, |s| s.to_lowercase().contains(table_name.as_str())))
                    .collect();
                let columns = vec![
                    ("event_id".to_owned(), DataType::Int64),
                    ("timestamp_ms".to_owned(), DataType::Int64),
                    ("event_type".to_owned(), DataType::Text),
                    ("role_name".to_owned(), DataType::Text),
                    ("detail".to_owned(), DataType::Text),
                    ("sql".to_owned(), DataType::Text),
                    ("success".to_owned(), DataType::Boolean),
                ];
                let rows = events.into_iter().map(|e| OwnedRow::new(vec![
                    Datum::Int64(e.event_id as i64),
                    Datum::Int64(e.timestamp_ms as i64),
                    Datum::Text(e.event_type.clone()),
                    Datum::Text(e.role_name.clone()),
                    Datum::Text(e.detail.clone()),
                    e.sql.clone().map_or(Datum::Null, Datum::Text),
                    Datum::Boolean(e.success),
                ])).collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowMemoryProfile => {
                use falcon_common::types::DataType;
                let snap = self.storage.memory_profile_snapshot();
                let columns = vec![
                    ("metric".to_owned(), DataType::Text),
                    ("value".to_owned(), DataType::Text),
                ];
                let rows = vec![
                    OwnedRow::new(vec![Datum::Text("large_mem_enabled".into()), Datum::Text(snap.large_mem_enabled.to_string())]),
                    OwnedRow::new(vec![Datum::Text("dashmap_shards".into()), Datum::Text(snap.dashmap_shards.to_string())]),
                    OwnedRow::new(vec![Datum::Text("capacity_hint".into()), Datum::Text(snap.capacity_hint.to_string())]),
                    OwnedRow::new(vec![Datum::Text("skip_pk_order".into()), Datum::Text(snap.skip_pk_order.to_string())]),
                    OwnedRow::new(vec![Datum::Text("gc_interval_ms".into()), Datum::Text(snap.gc_interval_ms.to_string())]),
                    OwnedRow::new(vec![Datum::Text("gc_batch_size".into()), Datum::Text(snap.gc_batch_size.to_string())]),
                    OwnedRow::new(vec![Datum::Text("memory_soft_limit_bytes".into()), Datum::Text(snap.soft_limit_bytes.to_string())]),
                    OwnedRow::new(vec![Datum::Text("memory_hard_limit_bytes".into()), Datum::Text(snap.hard_limit_bytes.to_string())]),
                    OwnedRow::new(vec![Datum::Text("memory_used_bytes".into()), Datum::Text(snap.used_bytes.to_string())]),
                    OwnedRow::new(vec![Datum::Text("pressure_state".into()), Datum::Text(snap.pressure_state)]),
                    OwnedRow::new(vec![Datum::Text("table_count".into()), Datum::Text(snap.table_count.to_string())]),
                ];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::DistPlan { .. } => Err(FalconError::Internal(
                "DistPlan must be executed via DistributedQueryEngine, not the local Executor"
                    .into(),
            )),
        }
    }

    fn datum_to_sql_literal(d: &Datum) -> String {
        match d {
            Datum::Null => "NULL".to_owned(),
            Datum::Boolean(b) => {
                if *b {
                    "TRUE".to_owned()
                } else {
                    "FALSE".to_owned()
                }
            }
            Datum::Int32(i) => i.to_string(),
            Datum::Int64(i) => i.to_string(),
            Datum::Float64(f) => format!("{}", f),
            Datum::Text(s) => format!("'{}'", s.replace('\'', "''")),
            other => format!("'{}'", other),
        }
    }

    pub(crate) fn eval_user_function(
        &self,
        name: &str,
        args: &[BoundExpr],
        row: &OwnedRow,
    ) -> Result<Datum, FalconError> {
        use falcon_common::schema::FunctionLanguage;

        let catalog = self.storage.get_catalog();
        let func_def = catalog
            .find_function(name)
            .ok_or_else(|| {
                FalconError::Execution(ExecutionError::TypeError(format!(
                    "function \"{}\" does not exist",
                    name
                )))
            })?
            .clone();

        let arg_vals: Vec<Datum> = args
            .iter()
            .map(|a| self.eval_expr_with_sequences(a, row))
            .collect::<Result<_, _>>()?;

        let eval_sql = |sql: &str,
                        vars: &std::collections::HashMap<String, Datum>|
         -> Result<Datum, ExecutionError> {
            let mut resolved = sql.to_owned();
            for (k, v) in vars {
                resolved = resolved.replace(k, &Self::datum_to_sql_literal(v));
            }
            let select_sql = if resolved.to_uppercase().starts_with("SELECT ") {
                resolved
            } else {
                format!("SELECT {}", resolved)
            };
            let dialect = sqlparser::dialect::PostgreSqlDialect {};
            let stmts = sqlparser::parser::Parser::parse_sql(&dialect, &select_sql)
                .map_err(|e| ExecutionError::TypeError(format!("SQL parse: {e}")))?;
            let stmt = stmts
                .into_iter()
                .next()
                .ok_or_else(|| ExecutionError::TypeError("empty SQL".into()))?;
            let cat = self.storage.get_catalog();
            let mut binder = falcon_sql_frontend::binder::Binder::new(cat);
            let bound = binder
                .bind(&stmt)
                .map_err(|e| ExecutionError::TypeError(format!("bind: {e}")))?;
            let plan = falcon_planner::Planner::plan(&bound)
                .map_err(|e| ExecutionError::TypeError(format!("plan: {e}")))?;
            let result = self
                .execute(&plan, None)
                .map_err(|e| ExecutionError::TypeError(format!("exec: {e}")))?;
            match result {
                ExecutionResult::Query { rows, .. } => Ok(rows
                    .into_iter()
                    .next()
                    .and_then(|r| r.values.into_iter().next())
                    .unwrap_or(Datum::Null)),
                _ => Ok(Datum::Null),
            }
        };
        let eval_rows_no_txn = |sql: &str,
                                vars: &std::collections::HashMap<String, Datum>|
         -> Result<Vec<std::collections::HashMap<String, Datum>>, ExecutionError> {
            let mut resolved = sql.to_owned();
            for (k, v) in vars {
                resolved = resolved.replace(k.as_str(), &Self::datum_to_sql_literal(v));
            }
            let select_sql = if resolved.to_uppercase().starts_with("SELECT ") {
                resolved
            } else {
                format!("SELECT {}", resolved)
            };
            match self.eval_sql_rows(&select_sql, None) {
                Ok(rows) => Ok(rows),
                Err(e) => Err(ExecutionError::TypeError(format!("FOR row eval: {e}"))),
            }
        };

        let result = match func_def.language {
            FunctionLanguage::Sql => {
                crate::plpgsql::execute_sql_function(&func_def, &arg_vals, eval_sql)
                    .map_err(FalconError::Execution)?
            }
            FunctionLanguage::PlPgSql => {
                crate::plpgsql::execute_plpgsql(&func_def, &arg_vals, eval_sql, eval_rows_no_txn)
                    .map_err(FalconError::Execution)?
            }
        };
        Ok(result)
    }

    fn exec_do_block(
        &self,
        body: &str,
        language: &str,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        use falcon_common::schema::{FunctionDef, FunctionLanguage, FunctionVolatility};
        use falcon_common::error::ExecutionError;

        let lang = match language.to_lowercase().as_str() {
            "plpgsql" | "plpg_sql" | "" => FunctionLanguage::PlPgSql,
            "sql" => FunctionLanguage::Sql,
            other => {
                return Err(FalconError::Execution(ExecutionError::TypeError(
                    format!("DO: unsupported language '{other}'"),
                )))
            }
        };

        let func_def = FunctionDef {
            name: "<anonymous>".into(),
            params: vec![],
            return_type: None,
            language: lang,
            body: body.to_owned(),
            volatility: FunctionVolatility::Volatile,
            is_strict: false,
            or_replace: false,
        };

        let eval_sql = |sql: &str,
                        vars: &std::collections::HashMap<String, Datum>|
         -> Result<Datum, ExecutionError> {
            let mut resolved = sql.to_owned();
            for (k, v) in vars {
                resolved = resolved.replace(k.as_str(), &Self::datum_to_sql_literal(v));
            }
            let select_sql = if resolved.to_uppercase().starts_with("SELECT ") {
                resolved.clone()
            } else {
                format!("SELECT {}", resolved)
            };
            match self.eval_sql_expr_in_txn(&select_sql, txn) {
                Ok(val) => Ok(val),
                Err(_) => match self.eval_sql_stmt_in_txn(&resolved, txn) {
                    Ok(_) => Ok(Datum::Null),
                    Err(e) => Err(ExecutionError::TypeError(format!("DO eval: {e}"))),
                },
            }
        };

        let txn_ptr = txn as *const TxnHandle;
        let eval_rows = |sql: &str,
                         vars: &std::collections::HashMap<String, Datum>|
         -> Result<Vec<std::collections::HashMap<String, Datum>>, ExecutionError> {
            let mut resolved = sql.to_owned();
            for (k, v) in vars {
                resolved = resolved.replace(k.as_str(), &Self::datum_to_sql_literal(v));
            }
            let select_sql = if resolved.to_uppercase().starts_with("SELECT ") {
                resolved
            } else {
                format!("SELECT {}", resolved)
            };
            let t = unsafe { &*txn_ptr };
            self.eval_sql_rows(&select_sql, Some(t))
                .map_err(|e| ExecutionError::TypeError(format!("DO FOR row eval: {e}")))
        };

        crate::plpgsql::execute_plpgsql(&func_def, &[], eval_sql, eval_rows)
            .map_err(FalconError::Execution)?;

        Ok(ExecutionResult::Dml {
            rows_affected: 0,
            tag: "DO",
        })
    }

    fn exec_call_procedure(
        &self,
        name: &str,
        args: &[BoundExpr],
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        use falcon_common::schema::FunctionLanguage;

        let catalog = self.storage.get_catalog();
        let func_def = catalog
            .find_function(name)
            .ok_or_else(|| {
                FalconError::Execution(ExecutionError::TypeError(format!(
                    "function \"{}\" does not exist",
                    name
                )))
            })?
            .clone();

        let empty_row = OwnedRow::empty();
        let arg_vals: Vec<Datum> = args
            .iter()
            .map(|a| crate::eval::eval_expr(a, &empty_row))
            .collect::<Result<_, _>>()
            .map_err(FalconError::Execution)?;

        let eval_sql = |sql: &str,
                        vars: &std::collections::HashMap<String, Datum>|
         -> Result<Datum, ExecutionError> {
            // Substitute PL/pgSQL variables ($1, $2, named) in the SQL string
            let mut resolved = sql.to_owned();
            for (k, v) in vars {
                resolved = resolved.replace(k.as_str(), &Self::datum_to_sql_literal(v));
            }
            // Try evaluating as a simple SELECT expression
            let select_sql = if resolved.to_uppercase().starts_with("SELECT ") {
                resolved.clone()
            } else {
                format!("SELECT {}", resolved)
            };
            // Parse + bind + plan + execute within current txn
            match self.eval_sql_expr_in_txn(&select_sql, txn) {
                Ok(val) => Ok(val),
                Err(_) => {
                    // If it fails as expression, try as statement
                    match self.eval_sql_stmt_in_txn(&resolved, txn) {
                        Ok(_) => Ok(Datum::Null),
                        Err(e) => Err(ExecutionError::TypeError(format!(
                            "PL/pgSQL eval error: {e}"
                        ))),
                    }
                }
            }
        };

        let txn_ptr = txn as *const TxnHandle;
        let eval_rows_txn = |sql: &str,
                             vars: &std::collections::HashMap<String, Datum>|
         -> Result<Vec<std::collections::HashMap<String, Datum>>, ExecutionError> {
            let mut resolved = sql.to_owned();
            for (k, v) in vars {
                resolved = resolved.replace(k.as_str(), &Self::datum_to_sql_literal(v));
            }
            let select_sql = if resolved.to_uppercase().starts_with("SELECT ") {
                resolved
            } else {
                format!("SELECT {}", resolved)
            };
            // SAFETY: txn lives for the duration of exec_call_procedure
            let t = unsafe { &*txn_ptr };
            self.eval_sql_rows(&select_sql, Some(t))
                .map_err(|e| ExecutionError::TypeError(format!("FOR row eval: {e}")))
        };

        let result = match func_def.language {
            FunctionLanguage::Sql => {
                crate::plpgsql::execute_sql_function(&func_def, &arg_vals, eval_sql)
                    .map_err(FalconError::Execution)?
            }
            FunctionLanguage::PlPgSql => {
                crate::plpgsql::execute_plpgsql(&func_def, &arg_vals, eval_sql, eval_rows_txn)
                    .map_err(FalconError::Execution)?
            }
        };

        Ok(ExecutionResult::Dml {
            rows_affected: 0,
            tag: "CALL",
        })
    }

    pub(crate) fn eval_sql_expr_in_txn(&self, sql: &str, txn: &TxnHandle) -> Result<Datum, FalconError> {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).map_err(|e| {
            FalconError::Execution(ExecutionError::TypeError(format!("SQL parse: {e}")))
        })?;
        let stmt = stmts
            .into_iter()
            .next()
            .ok_or_else(|| FalconError::Execution(ExecutionError::TypeError("empty SQL".into())))?;
        let catalog = self.storage.get_catalog();
        let mut binder = falcon_sql_frontend::binder::Binder::new(catalog);
        let bound = binder
            .bind(&stmt)
            .map_err(|e| FalconError::Execution(ExecutionError::TypeError(format!("bind: {e}"))))?;
        let plan = falcon_planner::Planner::plan(&bound)
            .map_err(|e| FalconError::Execution(ExecutionError::TypeError(format!("plan: {e}"))))?;
        let result = self.execute(&plan, Some(txn))?;
        match result {
            ExecutionResult::Query { rows, .. } => Ok(rows
                .into_iter()
                .next()
                .and_then(|r| r.values.into_iter().next())
                .unwrap_or(Datum::Null)),
            _ => Ok(Datum::Null),
        }
    }

    fn eval_sql_stmt_in_txn(
        &self,
        sql: &str,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).map_err(|e| {
            FalconError::Execution(ExecutionError::TypeError(format!("SQL parse: {e}")))
        })?;
        let stmt = stmts
            .into_iter()
            .next()
            .ok_or_else(|| FalconError::Execution(ExecutionError::TypeError("empty SQL".into())))?;
        let catalog = self.storage.get_catalog();
        let mut binder = falcon_sql_frontend::binder::Binder::new(catalog);
        let bound = binder
            .bind(&stmt)
            .map_err(|e| FalconError::Execution(ExecutionError::TypeError(format!("bind: {e}"))))?;
        let plan = falcon_planner::Planner::plan(&bound)
            .map_err(|e| FalconError::Execution(ExecutionError::TypeError(format!("plan: {e}"))))?;
        self.execute(&plan, Some(txn))
    }

    /// Execute a SELECT and return all rows as column-name → Datum maps.
    /// Used by PL/pgSQL `FOR row IN SELECT ... LOOP`.
    pub(crate) fn eval_sql_rows(
        &self,
        sql: &str,
        txn: Option<&TxnHandle>,
    ) -> Result<Vec<std::collections::HashMap<String, Datum>>, FalconError> {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).map_err(|e| {
            FalconError::Execution(ExecutionError::TypeError(format!("SQL parse: {e}")))
        })?;
        let stmt = stmts
            .into_iter()
            .next()
            .ok_or_else(|| FalconError::Execution(ExecutionError::TypeError("empty SQL".into())))?;
        let catalog = self.storage.get_catalog();
        let mut binder = falcon_sql_frontend::binder::Binder::new(catalog);
        let bound = binder
            .bind(&stmt)
            .map_err(|e| FalconError::Execution(ExecutionError::TypeError(format!("bind: {e}"))))?;
        let plan = falcon_planner::Planner::plan(&bound)
            .map_err(|e| FalconError::Execution(ExecutionError::TypeError(format!("plan: {e}"))))?;
        match self.execute(&plan, txn)? {
            ExecutionResult::Query { columns, rows } => {
                Ok(rows
                    .into_iter()
                    .map(|row| {
                        columns
                            .iter()
                            .zip(row.values.into_iter())
                            .map(|(col, val)| (col.0.to_lowercase(), val))
                            .collect()
                    })
                    .collect())
            }
            _ => Ok(vec![]),
        }
    }

    fn exec_create_table(
        &self,
        schema: &TableSchema,
        if_not_exists: bool,
        partition_spec: Option<&falcon_sql_frontend::types::BoundPartitionSpec>,
    ) -> Result<ExecutionResult, FalconError> {
        match self.storage.create_table(schema.clone()) {
            Ok(table_id) => {
                if let Some(spec) = partition_spec {
                    use falcon_sql_frontend::types::PartitionStrategy;
                    use falcon_storage::partition::{
                        PartitionStrategy as StorageStrategy, PartitionedTableDef,
                    };
                    let key_idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&spec.key_column))
                        .ok_or_else(|| FalconError::Execution(
                            falcon_common::error::ExecutionError::Internal(
                                format!("partition key column '{}' not found", spec.key_column),
                            ),
                        ))?;
                    let storage_strategy = match spec.strategy {
                        PartitionStrategy::Range => StorageStrategy::Range,
                        PartitionStrategy::List  => StorageStrategy::List,
                        PartitionStrategy::Hash  => StorageStrategy::Hash { modulus: 8 },
                    };
                    let pdef = PartitionedTableDef {
                        parent_table_id: table_id,
                        parent_table_name: schema.name.clone(),
                        partition_key_idx: key_idx,
                        partition_key_name: spec.key_column.clone(),
                        strategy: storage_strategy,
                        partitions: Vec::new(),
                        has_default: false,
                    };
                    self.storage.register_partitioned_table(pdef);
                }
                Ok(ExecutionResult::Ddl {
                    message: format!("CREATE TABLE {}", schema.name),
                })
            }
            Err(e) if if_not_exists => Ok(ExecutionResult::Ddl {
                message: format!(
                    "CREATE TABLE IF NOT EXISTS {} (already exists)",
                    schema.name
                ),
            }),
            Err(e) => Err(e.into()),
        }
    }

    fn exec_create_table_as(
        &self,
        table_name: &str,
        if_not_exists: bool,
        query: &BoundSelect,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        use falcon_common::schema::{ColumnDef, TableSchema};
        use falcon_common::types::{ColumnId, TableId};
        use falcon_planner::Planner;

        let bound_sel = falcon_sql_frontend::types::BoundStatement::Select(query.clone());
        let select_plan = Planner::plan(&bound_sel)?;
        let result = self.execute(&select_plan, Some(txn))?;
        let (columns, rows) = match result {
            ExecutionResult::Query { columns, rows } => (columns, rows),
            _ => {
                return Err(FalconError::Internal(
                    "CTAS: inner SELECT did not produce rows".into(),
                ))
            }
        };

        let col_defs: Vec<ColumnDef> = columns
            .iter()
            .enumerate()
            .map(|(i, (name, dt))| {
                ColumnDef::new(
                    ColumnId(i as u32),
                    name.clone(),
                    dt.clone(),
                    true,
                    false,
                    None,
                    false,
                )
            })
            .collect();

        let schema = TableSchema {
            id: TableId(0),
            name: table_name.to_owned(),
            columns: col_defs,
            ..Default::default()
        };

        match self.storage.create_table(schema.clone()) {
            Ok(_) => {}
            Err(_) if if_not_exists => {
                return Ok(ExecutionResult::Ddl {
                    message: format!("CREATE TABLE {table_name}"),
                });
            }
            Err(e) => return Err(e.into()),
        }

        let new_schema = self
            .storage
            .get_catalog()
            .find_table(table_name)
            .ok_or_else(|| {
                FalconError::Internal(format!("CTAS: table {table_name} not found after create"))
            })?
            .clone();

        let table_id = new_schema.id;
        for row in rows {
            self.storage.insert(table_id, row, txn.txn_id)?;
        }

        Ok(ExecutionResult::Ddl {
            message: format!("CREATE TABLE {table_name}"),
        })
    }

    fn exec_drop_table(
        &self,
        table_name: &str,
        if_exists: bool,
    ) -> Result<ExecutionResult, FalconError> {
        match self.storage.drop_table(table_name) {
            Ok(_) => Ok(ExecutionResult::Ddl {
                message: format!("DROP TABLE {table_name}"),
            }),
            Err(e) if if_exists => {
                // IF EXISTS — silently succeed
                Ok(ExecutionResult::Ddl {
                    message: format!("DROP TABLE IF EXISTS {table_name} (not found)"),
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    fn exec_alter_table(
        &self,
        table_name: &str,
        ops: &[AlterTableOp],
    ) -> Result<ExecutionResult, FalconError> {
        let mut messages = Vec::new();
        let mut ddl_ids = Vec::new();
        for op in ops {
            let pair = match op {
                AlterTableOp::AddColumn(col) => {
                    let id = self
                        .storage
                        .alter_table_add_column(table_name, col.clone())?;
                    Some((id, format!("ADD COLUMN {}", col.name)))
                }
                AlterTableOp::AddColumnDynamic {
                    col,
                    dynamic_default,
                } => {
                    let id = self.storage.alter_table_add_column_dynamic(
                        table_name,
                        col.clone(),
                        dynamic_default.clone(),
                    )?;
                    Some((id, format!("ADD COLUMN {} (dynamic default)", col.name)))
                }
                AlterTableOp::AddConstraint(bc) => {
                    use falcon_common::schema::AddConstraintKind;
                    use falcon_sql_frontend::types::ConstraintKind;
                    let storage_kind = match &bc.kind {
                        ConstraintKind::PrimaryKey(cols) => {
                            AddConstraintKind::PrimaryKey(cols.clone())
                        }
                        ConstraintKind::Unique(cols) => AddConstraintKind::Unique(cols.clone()),
                        ConstraintKind::ForeignKey {
                            columns,
                            ref_table,
                            ref_columns,
                        } => AddConstraintKind::ForeignKey {
                            columns: columns.clone(),
                            ref_table: ref_table.clone(),
                            ref_columns: ref_columns.clone(),
                        },
                        ConstraintKind::Check(expr) => {
                            let val = crate::eval::eval_expr(
                                expr,
                                &falcon_common::datum::OwnedRow::new(vec![]),
                            )
                            .map_err(FalconError::Execution)?;
                            AddConstraintKind::Check(format!("{val}"))
                        }
                    };
                    let id = self.storage.alter_table_add_constraint(
                        table_name,
                        bc.name.as_deref(),
                        &storage_kind,
                    )?;
                    let kind_str = match &bc.kind {
                        ConstraintKind::PrimaryKey(_) => "ADD PRIMARY KEY",
                        ConstraintKind::Unique(_) => "ADD UNIQUE",
                        ConstraintKind::ForeignKey { .. } => "ADD FOREIGN KEY",
                        ConstraintKind::Check(_) => "ADD CHECK",
                    };
                    Some((id, kind_str.to_string()))
                }
                AlterTableOp::DropColumn(col_name) => {
                    let id = self.storage.alter_table_drop_column(table_name, col_name)?;
                    Some((id, format!("DROP COLUMN {col_name}")))
                }
                AlterTableOp::RenameColumn { old_name, new_name } => {
                    let id = self
                        .storage
                        .alter_table_rename_column(table_name, old_name, new_name)?;
                    Some((id, format!("RENAME COLUMN {old_name} TO {new_name}")))
                }
                AlterTableOp::RenameTable { new_name } => {
                    let id = self.storage.alter_table_rename(table_name, new_name)?;
                    Some((id, format!("RENAME TO {new_name}")))
                }
                AlterTableOp::AlterColumnType {
                    column_name,
                    new_type,
                } => {
                    let id = self.storage.alter_table_change_column_type(
                        table_name,
                        column_name,
                        new_type.clone(),
                    )?;
                    Some((id, format!("ALTER COLUMN {column_name} TYPE {new_type}")))
                }
                AlterTableOp::AlterColumnSetNotNull { column_name } => {
                    let id = self
                        .storage
                        .alter_table_set_not_null(table_name, column_name)?;
                    Some((id, format!("ALTER COLUMN {column_name} SET NOT NULL")))
                }
                AlterTableOp::AlterColumnDropNotNull { column_name } => {
                    let id = self
                        .storage
                        .alter_table_drop_not_null(table_name, column_name)?;
                    Some((id, format!("ALTER COLUMN {column_name} DROP NOT NULL")))
                }
                AlterTableOp::AlterColumnSetDefault {
                    column_name,
                    default_expr,
                } => {
                    let default_val = crate::eval::eval_expr(
                        default_expr,
                        &falcon_common::datum::OwnedRow::new(vec![]),
                    )
                    .map_err(FalconError::Execution)?;
                    let id = self.storage.alter_table_set_default(
                        table_name,
                        column_name,
                        default_val,
                    )?;
                    Some((id, format!("ALTER COLUMN {column_name} SET DEFAULT")))
                }
                AlterTableOp::AlterColumnDropDefault { column_name } => {
                    let id = self
                        .storage
                        .alter_table_drop_default(table_name, column_name)?;
                    Some((id, format!("ALTER COLUMN {column_name} DROP DEFAULT")))
                }
                AlterTableOp::DropConstraint { name, if_exists } => {
                    let id = self
                        .storage
                        .alter_table_drop_constraint(table_name, name, *if_exists)?;
                    Some((id, format!("DROP CONSTRAINT {name}")))
                }
                AlterTableOp::RenameConstraint { old_name, new_name } => {
                    let id = self
                        .storage
                        .alter_table_rename_constraint(table_name, old_name, new_name)?;
                    Some((id, format!("RENAME CONSTRAINT {old_name} TO {new_name}")))
                }
                AlterTableOp::NoOp => None,
            };
            let (ddl_id, msg) = match pair {
                Some(p) => p,
                None => continue,
            };
            ddl_ids.push(ddl_id);
            messages.push(msg);
        }
        let ids_str = ddl_ids
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        Ok(ExecutionResult::Ddl {
            message: format!(
                "ALTER TABLE {} {} [ddl_ids={}]",
                table_name,
                messages.join(", "),
                ids_str
            ),
        })
    }

    pub(crate) fn resolve_output_columns(
        &self,
        projections: &[BoundProjection],
        schema: &TableSchema,
    ) -> Vec<(String, DataType)> {
        projections
            .iter()
            .map(|p| match p {
                BoundProjection::Column(idx, alias) => {
                    (alias.clone(), schema.columns[*idx].data_type.clone())
                }
                BoundProjection::Aggregate(_, _, alias, _, _) => {
                    (alias.clone(), DataType::Int64) // simplification
                }
                BoundProjection::Expr(_, alias) => {
                    (alias.clone(), DataType::Text) // simplification
                }
                BoundProjection::Window(wf) => (wf.alias.clone(), DataType::Int64),
            })
            .collect()
    }

    /// Format a physical plan as human-readable text lines.
    pub fn format_plan(&self, plan: &PhysicalPlan, indent: usize) -> Vec<String> {
        let pad = "  ".repeat(indent);
        match plan {
            PhysicalPlan::SeqScan {
                table_id,
                schema,
                projections,
                filter,
                group_by,
                order_by,
                limit,
                offset,
                distinct,
                unions,
                ..
            } => {
                // Row estimate from table stats or memtable
                let base_rows = self
                    .storage
                    .get_table_stats(*table_id)
                    .map(|s| s.row_count as f64)
                    .unwrap_or_else(|| {
                        self.storage
                            .get_table(*table_id)
                            .map_or(1000.0, |t| t.row_count_approx() as f64)
                    });
                let mut est_rows = base_rows;
                // Apply filter selectivity
                if filter.is_some() {
                    est_rows *= 0.33;
                }
                if let Some(l) = limit {
                    est_rows = est_rows.min(*l as f64);
                }
                if let Some(o) = offset {
                    est_rows = (est_rows - *o as f64).max(0.0);
                }
                // Cost model: startup_cost..total_cost
                let startup_cost = 0.0_f64;
                let total_cost = base_rows * 1.0 + base_rows * 0.01; // seq_page_cost + cpu_tuple_cost
                let mut lines = vec![format!(
                    "{}Seq Scan on {}  (cost={:.2}..{:.2} rows={} width={})",
                    pad,
                    schema.name,
                    startup_cost,
                    total_cost,
                    est_rows as u64,
                    schema.columns.len() * 8
                )];
                let cols: Vec<String> = projections
                    .iter()
                    .map(|p| match p {
                        BoundProjection::Column(_, a) => a.clone(),
                        BoundProjection::Aggregate(_, _, a, _, _) => a.clone(),
                        BoundProjection::Expr(_, a) => a.clone(),
                        BoundProjection::Window(wf) => wf.alias.clone(),
                    })
                    .collect();
                lines.push(format!("{}  Output: {}", pad, cols.join(", ")));
                if let Some(f) = filter {
                    lines.push(format!("{pad}  Filter: {f:?}"));
                }
                if !group_by.is_empty() {
                    lines.push(format!("{pad}  Group By: {group_by:?}"));
                }
                if !order_by.is_empty() {
                    lines.push(format!("{}  Sort Key: {} column(s)", pad, order_by.len()));
                }
                if let Some(l) = limit {
                    lines.push(format!("{pad}  Limit: {l}"));
                }
                if let Some(o) = offset {
                    lines.push(format!("{pad}  Offset: {o}"));
                }
                if !matches!(distinct, DistinctMode::None) {
                    lines.push(format!("{pad}  Distinct: {distinct:?}"));
                }
                if !unions.is_empty() {
                    lines.push(format!(
                        "{}  Set Ops: {} additional query(ies)",
                        pad,
                        unions.len()
                    ));
                }
                lines
            }
            PhysicalPlan::ColumnScan {
                schema,
                filter,
                order_by,
                limit,
                offset,
                ..
            } => {
                let mut lines = vec![format!("{}Column Scan on {}", pad, schema.name,)];
                if let Some(f) = filter {
                    lines.push(format!("{pad}  Filter: {f:?}"));
                }
                if !order_by.is_empty() {
                    lines.push(format!("{}  Sort Key: {} column(s)", pad, order_by.len()));
                }
                if let Some(l) = limit {
                    lines.push(format!("{pad}  Limit: {l}"));
                }
                if let Some(o) = offset {
                    lines.push(format!("{pad}  Offset: {o}"));
                }
                lines
            }
            PhysicalPlan::IndexScan {
                table_id,
                schema,
                index_col,
                index_value,
                projections,
                filter,
                order_by,
                limit,
                offset,
                distinct,
                unions,
                ..
            } => {
                let base_rows = self
                    .storage
                    .get_table_stats(*table_id)
                    .map(|s| s.row_count as f64)
                    .unwrap_or_else(|| {
                        self.storage
                            .get_table(*table_id)
                            .map_or(1000.0, |t| t.row_count_approx() as f64)
                    });
                let est_rows = (base_rows * 0.01).max(1.0);
                let startup_cost = 0.0_f64;
                let total_cost = est_rows * 0.01 + 1.0; // index lookup cost
                let col_name = schema
                    .columns
                    .get(*index_col)
                    .map_or("?", |c| c.name.as_str());
                let mut lines = vec![format!(
                    "{}Index Scan using {} on {}  (cost={:.2}..{:.2} rows={} width={})",
                    pad,
                    col_name,
                    schema.name,
                    startup_cost,
                    total_cost,
                    est_rows as u64,
                    schema.columns.len() * 8
                )];
                lines.push(format!("{pad}  Index Cond: ({col_name} = {index_value:?})"));
                let cols: Vec<String> = projections
                    .iter()
                    .map(|p| match p {
                        BoundProjection::Column(_, a) => a.clone(),
                        BoundProjection::Aggregate(_, _, a, _, _) => a.clone(),
                        BoundProjection::Expr(_, a) => a.clone(),
                        BoundProjection::Window(wf) => wf.alias.clone(),
                    })
                    .collect();
                lines.push(format!("{}  Output: {}", pad, cols.join(", ")));
                if let Some(f) = filter {
                    lines.push(format!("{pad}  Filter: {f:?}"));
                }
                if !order_by.is_empty() {
                    lines.push(format!("{}  Sort Key: {} column(s)", pad, order_by.len()));
                }
                if let Some(l) = limit {
                    lines.push(format!("{pad}  Limit: {l}"));
                }
                if let Some(o) = offset {
                    lines.push(format!("{pad}  Offset: {o}"));
                }
                if !matches!(distinct, DistinctMode::None) {
                    lines.push(format!("{pad}  Distinct: {distinct:?}"));
                }
                if !unions.is_empty() {
                    lines.push(format!(
                        "{}  Set Ops: {} additional query(ies)",
                        pad,
                        unions.len()
                    ));
                }
                lines
            }
            PhysicalPlan::IndexRangeScan {
                table_id,
                schema,
                index_col,
                lower_bound,
                upper_bound,
                projections,
                filter,
                order_by,
                limit,
                offset,
                distinct,
                unions,
                ..
            } => {
                let base_rows = self
                    .storage
                    .get_table_stats(*table_id)
                    .map(|s| s.row_count as f64)
                    .unwrap_or_else(|| {
                        self.storage
                            .get_table(*table_id)
                            .map_or(1000.0, |t| t.row_count_approx() as f64)
                    });
                let est_rows = (base_rows * 0.10).max(1.0); // range scan ~10% selectivity
                let startup_cost = 0.0_f64;
                let total_cost = est_rows * 0.01 + 1.0;
                let col_name = schema
                    .columns
                    .get(*index_col)
                    .map_or("?", |c| c.name.as_str());
                let mut lines = vec![format!(
                    "{}Index Range Scan using {} on {}  (cost={:.2}..{:.2} rows={} width={})",
                    pad,
                    col_name,
                    schema.name,
                    startup_cost,
                    total_cost,
                    est_rows as u64,
                    schema.columns.len() * 8
                )];
                // Format the range condition
                let lo_str = lower_bound.as_ref().map(|(expr, inc)| {
                    let op = if *inc { ">=" } else { ">" };
                    format!("{col_name} {op} {expr:?}")
                });
                let hi_str = upper_bound.as_ref().map(|(expr, inc)| {
                    let op = if *inc { "<=" } else { "<" };
                    format!("{col_name} {op} {expr:?}")
                });
                let cond = match (lo_str, hi_str) {
                    (Some(l), Some(h)) => format!("{l} AND {h}"),
                    (Some(l), None) => l,
                    (None, Some(h)) => h,
                    (None, None) => "?".into(),
                };
                lines.push(format!("{pad}  Index Cond: ({cond})"));
                let cols: Vec<String> = projections
                    .iter()
                    .map(|p| match p {
                        BoundProjection::Column(_, a) => a.clone(),
                        BoundProjection::Aggregate(_, _, a, _, _) => a.clone(),
                        BoundProjection::Expr(_, a) => a.clone(),
                        BoundProjection::Window(wf) => wf.alias.clone(),
                    })
                    .collect();
                lines.push(format!("{}  Output: {}", pad, cols.join(", ")));
                if let Some(f) = filter {
                    lines.push(format!("{pad}  Filter: {f:?}"));
                }
                if !order_by.is_empty() {
                    lines.push(format!("{}  Sort Key: {} column(s)", pad, order_by.len()));
                }
                if let Some(l) = limit {
                    lines.push(format!("{pad}  Limit: {l}"));
                }
                if let Some(o) = offset {
                    lines.push(format!("{pad}  Offset: {o}"));
                }
                if !matches!(distinct, DistinctMode::None) {
                    lines.push(format!("{pad}  Distinct: {distinct:?}"));
                }
                if !unions.is_empty() {
                    lines.push(format!(
                        "{}  Set Ops: {} additional query(ies)",
                        pad,
                        unions.len()
                    ));
                }
                lines
            }
            PhysicalPlan::NestedLoopJoin {
                left_table_id,
                left_schema,
                joins,
                projections,
                filter,
                order_by,
                limit,
                offset,
                distinct,
                unions,
                ..
            }
            | PhysicalPlan::HashJoin {
                left_table_id,
                left_schema,
                joins,
                projections,
                filter,
                order_by,
                limit,
                offset,
                distinct,
                unions,
                ..
            }
            | PhysicalPlan::MergeSortJoin {
                left_table_id,
                left_schema,
                joins,
                projections,
                filter,
                order_by,
                limit,
                offset,
                distinct,
                unions,
                ..
            } => {
                let is_hash = matches!(plan, PhysicalPlan::HashJoin { .. });
                let is_merge = matches!(plan, PhysicalPlan::MergeSortJoin { .. });
                let strategy = if is_merge {
                    "Merge Sort Join"
                } else if is_hash {
                    "Hash Join"
                } else {
                    "Nested Loop"
                };
                // Estimate left table rows
                let left_rows = self
                    .storage
                    .get_table_stats(*left_table_id)
                    .map(|s| s.row_count as f64)
                    .unwrap_or_else(|| {
                        self.storage
                            .get_table(*left_table_id)
                            .map_or(1000.0, |t| t.row_count_approx() as f64)
                    });
                // Estimate join output rows
                let mut est_rows = left_rows;
                for j in joins {
                    let right_rows = self
                        .storage
                        .get_table_schema(&j.right_table_name)
                        .and_then(|s| {
                            self.storage
                                .get_table(s.id)
                                .map(|t| t.row_count_approx() as f64)
                        })
                        .unwrap_or(1000.0);
                    est_rows = if is_hash {
                        // Hash join: roughly min(left, right) for equi-join
                        (left_rows * right_rows * 0.1).max(1.0)
                    } else {
                        left_rows * right_rows * 0.1
                    };
                }
                if filter.is_some() {
                    est_rows *= 0.33;
                }
                if let Some(l) = limit {
                    est_rows = est_rows.min(*l as f64);
                }
                // Cost model
                let startup_cost = if is_hash { left_rows * 0.01 } else { 0.0 };
                let total_cost = if is_hash {
                    left_rows * 1.01 + est_rows * 0.01 // build + probe
                } else {
                    left_rows * est_rows * 0.01 // nested loop cost
                };
                let width = left_schema.columns.len() * 8 + joins.iter().map(|_| 64).sum::<usize>();
                let mut lines = vec![format!(
                    "{}{} on {}  (cost={:.2}..{:.2} rows={} width={})",
                    pad,
                    strategy,
                    left_schema.name,
                    startup_cost,
                    total_cost,
                    est_rows as u64,
                    width
                )];
                for j in joins {
                    lines.push(format!(
                        "{}  -> {:?} JOIN {}",
                        pad, j.join_type, j.right_table_name
                    ));
                }
                let cols: Vec<String> = projections
                    .iter()
                    .map(|p| match p {
                        BoundProjection::Column(_, a) => a.clone(),
                        BoundProjection::Aggregate(_, _, a, _, _) => a.clone(),
                        BoundProjection::Expr(_, a) => a.clone(),
                        BoundProjection::Window(wf) => wf.alias.clone(),
                    })
                    .collect();
                lines.push(format!("{}  Output: {}", pad, cols.join(", ")));
                if let Some(f) = filter {
                    lines.push(format!("{pad}  Filter: {f:?}"));
                }
                if !order_by.is_empty() {
                    lines.push(format!("{}  Sort Key: {} column(s)", pad, order_by.len()));
                }
                if let Some(l) = limit {
                    lines.push(format!("{pad}  Limit: {l}"));
                }
                if let Some(o) = offset {
                    lines.push(format!("{pad}  Offset: {o}"));
                }
                if !matches!(distinct, DistinctMode::None) {
                    lines.push(format!("{pad}  Distinct: {distinct:?}"));
                }
                if !unions.is_empty() {
                    lines.push(format!(
                        "{}  Set Ops: {} additional query(ies)",
                        pad,
                        unions.len()
                    ));
                }
                lines
            }
            PhysicalPlan::Insert {
                schema,
                columns,
                returning,
                ..
            } => {
                let col_names: Vec<String> = columns
                    .iter()
                    .map(|&i| schema.columns[i].name.clone())
                    .collect();
                let mut lines = vec![format!(
                    "{}Insert on {} ({})",
                    pad,
                    schema.name,
                    col_names.join(", ")
                )];
                if !returning.is_empty() {
                    lines.push(format!("{}  Returning: {} column(s)", pad, returning.len()));
                }
                lines
            }
            PhysicalPlan::Update {
                schema,
                assignments,
                filter,
                returning,
                ..
            } => {
                let cols: Vec<String> = assignments
                    .iter()
                    .map(|(i, _)| schema.columns[*i].name.clone())
                    .collect();
                let mut lines = vec![format!(
                    "{}Update on {} SET {}",
                    pad,
                    schema.name,
                    cols.join(", ")
                )];
                if let Some(f) = filter {
                    lines.push(format!("{pad}  Filter: {f:?}"));
                }
                if !returning.is_empty() {
                    lines.push(format!("{}  Returning: {} column(s)", pad, returning.len()));
                }
                lines
            }
            PhysicalPlan::Delete {
                schema,
                filter,
                returning,
                ..
            } => {
                let mut lines = vec![format!("{}Delete on {}", pad, schema.name)];
                if let Some(f) = filter {
                    lines.push(format!("{pad}  Filter: {f:?}"));
                }
                if !returning.is_empty() {
                    lines.push(format!("{}  Returning: {} column(s)", pad, returning.len()));
                }
                lines
            }
            PhysicalPlan::Merge(m) => {
                vec![format!(
                    "{}Merge on {} using {} ({} clauses)",
                    pad,
                    m.target_name,
                    m.source_name,
                    m.clauses.len()
                )]
            }
            PhysicalPlan::CreateMaterializedView { name, .. } => {
                vec![format!("{}CreateMaterializedView {}", pad, name)]
            }
            PhysicalPlan::DropMaterializedView { name, .. } => {
                vec![format!("{}DropMaterializedView {}", pad, name)]
            }
            PhysicalPlan::RefreshMaterializedView { name } => {
                vec![format!("{}RefreshMaterializedView {}", pad, name)]
            }
            PhysicalPlan::CreateDatabase {
                name,
                if_not_exists,
            } => {
                vec![format!(
                    "{}CreateDatabase {} (if_not_exists={})",
                    pad, name, if_not_exists
                )]
            }
            PhysicalPlan::DropDatabase { name, if_exists } => {
                vec![format!(
                    "{}DropDatabase {} (if_exists={})",
                    pad, name, if_exists
                )]
            }
            PhysicalPlan::CreateTable {
                schema,
                if_not_exists,
                partition_spec,
            } => {
                vec![format!(
                    "{}CreateTable {} (if_not_exists={}, partitioned={})",
                    pad, schema.name, if_not_exists, partition_spec.is_some()
                )]
            }
            PhysicalPlan::CreateTableAs {
                table_name,
                if_not_exists,
                ..
            } => {
                vec![format!(
                    "{}CreateTableAs {} (if_not_exists={})",
                    pad, table_name, if_not_exists
                )]
            }
            PhysicalPlan::DropTable {
                table_name,
                if_exists,
            } => {
                vec![format!(
                    "{}DropTable {} (if_exists={})",
                    pad, table_name, if_exists
                )]
            }
            PhysicalPlan::AlterTable { table_name, ops } => {
                vec![format!("{}AlterTable {} {:?}", pad, table_name, ops)]
            }
            PhysicalPlan::Explain(inner) | PhysicalPlan::ExplainAnalyze(inner) => {
                let mut lines = vec![format!("{}Explain", pad)];
                lines.extend(self.format_plan(inner, indent + 1));
                lines
            }
            PhysicalPlan::Truncate { table_name } => {
                vec![format!("{}Truncate {}", pad, table_name)]
            }
            PhysicalPlan::CreateIndex {
                index_name,
                table_name,
                ..
            } => {
                vec![format!(
                    "{}CreateIndex {} on {}",
                    pad, index_name, table_name
                )]
            }
            PhysicalPlan::DropIndex { index_name } => {
                vec![format!("{}DropIndex {}", pad, index_name)]
            }
            PhysicalPlan::CreateView { name, .. } => {
                vec![format!("{}CreateView {}", pad, name)]
            }
            PhysicalPlan::DropView { name, .. } => {
                vec![format!("{}DropView {}", pad, name)]
            }
            PhysicalPlan::Begin => vec![format!("{}Begin", pad)],
            PhysicalPlan::Commit => vec![format!("{}Commit", pad)],
            PhysicalPlan::Rollback => vec![format!("{}Rollback", pad)],
            PhysicalPlan::ShowTxnStats => vec![format!("{}ShowTxnStats", pad)],
            PhysicalPlan::ShowNodeRole => vec![format!("{}ShowNodeRole", pad)],
            PhysicalPlan::ShowWalStats => vec![format!("{}ShowWalStats", pad)],
            PhysicalPlan::ShowConnections => vec![format!("{}ShowConnections", pad)],
            PhysicalPlan::RunGc => vec![format!("{}RunGc", pad)],
            PhysicalPlan::Analyze { table_name } => vec![format!("{}Analyze {}", pad, table_name)],
            PhysicalPlan::ShowTableStats { table_name } => {
                vec![format!("{}ShowTableStats (table={:?})", pad, table_name)]
            }
            PhysicalPlan::CreateSequence { name, start } => {
                vec![format!("{}CreateSequence {} start={}", pad, name, start)]
            }
            PhysicalPlan::DropSequence { name, .. } => {
                vec![format!("{}DropSequence {}", pad, name)]
            }
            PhysicalPlan::ShowSequences => vec![format!("{}ShowSequences", pad)],
            PhysicalPlan::ShowTenants => vec![format!("{}ShowTenants", pad)],
            PhysicalPlan::ShowTenantUsage => vec![format!("{}ShowTenantUsage", pad)],
            PhysicalPlan::CreateTenant { name, .. } => {
                vec![format!("{}CreateTenant {}", pad, name)]
            }
            PhysicalPlan::DropTenant { name } => vec![format!("{}DropTenant {}", pad, name)],
            PhysicalPlan::CopyFrom { schema, .. } => {
                vec![format!("{}CopyFrom STDIN into {}", pad, schema.name)]
            }
            PhysicalPlan::CopyTo { schema, .. } => {
                vec![format!("{}CopyTo STDOUT from {}", pad, schema.name)]
            }
            PhysicalPlan::CopyQueryTo { query, .. } => {
                let mut lines = vec![format!("{}CopyQueryTo STDOUT", pad)];
                lines.extend(self.format_plan(query, indent + 2));
                lines
            }
            PhysicalPlan::DistPlan {
                subplan,
                target_shards,
                gather,
                ..
            } => {
                let mut lines = vec![format!(
                    "{}DistPlan (scatter to {} shards, gather={:?})",
                    pad,
                    target_shards.len(),
                    gather
                )];
                lines.extend(self.format_plan(subplan, indent + 1));
                lines
            }
            _ => {
                vec![format!("{}DDL/DCL statement (no plan detail)", pad)]
            }
        }
    }
}
