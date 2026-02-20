use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use cedar_common::config::SpillConfig;
use cedar_common::datum::{Datum, OwnedRow};
use cedar_common::error::CedarError;
use cedar_common::schema::TableSchema;
use cedar_common::types::{DataType, TableId};
use cedar_planner::PhysicalPlan;
use cedar_sql_frontend::types::*;
use cedar_storage::engine::StorageEngine;
use cedar_txn::{TxnHandle, TxnManager};

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
    Dml { rows_affected: u64, tag: String },
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
    read_only: bool,
    /// Optional handle to active connection counter for SHOW cedar.connections.
    active_connections: Option<Arc<std::sync::atomic::AtomicUsize>>,
    /// Maximum connections setting for SHOW cedar.connections.
    max_connections: usize,
    /// External sorter for spill-to-disk ORDER BY (None = pure in-memory).
    pub(crate) external_sorter: Option<ExternalSorter>,
    /// Parallel execution configuration.
    pub(crate) parallel_config: ParallelConfig,
}

impl Executor {
    pub fn new(storage: Arc<StorageEngine>, txn_mgr: Arc<TxnManager>) -> Self {
        Self { storage, txn_mgr, read_only: false, active_connections: None, max_connections: 0, external_sorter: None, parallel_config: ParallelConfig::default() }
    }

    /// Create an executor in read-only mode (for replica nodes).
    pub fn new_read_only(storage: Arc<StorageEngine>, txn_mgr: Arc<TxnManager>) -> Self {
        Self { storage, txn_mgr, read_only: true, active_connections: None, max_connections: 0, external_sorter: None, parallel_config: ParallelConfig::default() }
    }

    /// Configure parallel execution.
    pub fn set_parallel_config(&mut self, config: ParallelConfig) {
        self.parallel_config = config;
    }

    /// Set the active connections handle for SHOW cedar.connections.
    pub fn set_connection_info(&mut self, active: Arc<std::sync::atomic::AtomicUsize>, max: usize) {
        self.active_connections = Some(active);
        self.max_connections = max;
    }

    /// Whether this executor is in read-only mode.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Configure spill-to-disk from SpillConfig.
    pub fn set_spill_config(&mut self, config: &SpillConfig) {
        self.external_sorter = ExternalSorter::from_config(config);
    }

    /// Set read-only mode (e.g. after failover role change).
    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    /// Guard: reject the operation if this executor is read-only.
    fn reject_if_read_only(&self, op: &str) -> Result<(), CedarError> {
        if self.read_only {
            Err(CedarError::ReadOnly(format!(
                "cannot execute {} on a read-only replica", op
            )))
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
    ) -> Result<ExecutionResult, CedarError> {
        if params.is_empty() {
            return self.execute(plan, txn);
        }
        let substituted = crate::param_subst::substitute_params_plan(plan, params)
            .map_err(CedarError::Execution)?;
        self.execute(&substituted, txn)
    }

    /// Execute a physical plan within a transaction context.
    /// `txn` is None for DDL and txn control statements.
    pub fn execute(
        &self,
        plan: &PhysicalPlan,
        txn: Option<&TxnHandle>,
    ) -> Result<ExecutionResult, CedarError> {
        match plan {
            PhysicalPlan::CreateTable { schema, if_not_exists } => {
                self.reject_if_read_only("CREATE TABLE")?;
                self.exec_create_table(schema, *if_not_exists)
            }
            PhysicalPlan::DropTable { table_name, if_exists } => {
                self.reject_if_read_only("DROP TABLE")?;
                self.exec_drop_table(table_name, *if_exists)
            }
            PhysicalPlan::AlterTable { table_name, ops } => {
                self.reject_if_read_only("ALTER TABLE")?;
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
                let txn = txn.ok_or(CedarError::Internal(
                    "INSERT requires active transaction".into(),
                ))?;
                if let Some(sel) = source_select {
                    self.exec_insert_select(*table_id, schema, columns, sel, txn)
                } else {
                    self.exec_insert(*table_id, schema, columns, rows, returning, on_conflict, txn)
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
                let txn = txn.ok_or(CedarError::Internal(
                    "UPDATE requires active transaction".into(),
                ))?;
                self.exec_update(*table_id, schema, assignments, filter.as_ref(), returning, from_table.as_ref(), txn)
            }
            PhysicalPlan::Delete {
                table_id,
                schema,
                filter,
                returning,
                using_table,
            } => {
                self.reject_if_read_only("DELETE")?;
                let txn = txn.ok_or(CedarError::Internal(
                    "DELETE requires active transaction".into(),
                ))?;
                self.exec_delete(*table_id, schema, filter.as_ref(), returning, using_table.as_ref(), txn)
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
            } => {
                let txn = txn.ok_or(CedarError::Internal(
                    "SELECT requires active transaction".into(),
                ))?;
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
            } => {
                let txn = txn.ok_or(CedarError::Internal(
                    "SELECT requires active transaction".into(),
                ))?;
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
                let txn = txn.ok_or(CedarError::Internal(
                    "SELECT requires active transaction".into(),
                ))?;
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
                let txn = txn.ok_or(CedarError::Internal(
                    "SELECT requires active transaction".into(),
                ))?;
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
                let txn = txn.ok_or(CedarError::Internal(
                    "SELECT requires active transaction".into(),
                ))?;
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
                    tag: "TRUNCATE TABLE".into(),
                })
            }
            PhysicalPlan::Explain(inner) => {
                let plan_text = self.format_plan(inner, 0);
                let rows = plan_text.into_iter()
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
                    actual_rows, actual_cols,
                    elapsed.as_secs_f64() * 1000.0
                ))]));
                Ok(ExecutionResult::Query {
                    columns: vec![("QUERY PLAN".into(), DataType::Text)],
                    rows,
                })
            }
            PhysicalPlan::CreateIndex { index_name, table_name, column_indices, unique } => {
                self.reject_if_read_only("CREATE INDEX")?;
                for &col_idx in column_indices {
                    self.storage.create_named_index(index_name, table_name, col_idx, *unique)?;
                }
                Ok(ExecutionResult::Ddl {
                    message: format!("CREATE INDEX {}", index_name),
                })
            }
            PhysicalPlan::CreateView { name, query_sql, or_replace } => {
                self.reject_if_read_only("CREATE VIEW")?;
                self.storage.create_view(name, query_sql, *or_replace)?;
                Ok(ExecutionResult::Ddl {
                    message: format!("CREATE VIEW {}", name),
                })
            }
            PhysicalPlan::DropView { name, if_exists } => {
                self.reject_if_read_only("DROP VIEW")?;
                self.storage.drop_view(name, *if_exists)?;
                Ok(ExecutionResult::Ddl {
                    message: format!("DROP VIEW {}", name),
                })
            }
            PhysicalPlan::DropIndex { index_name } => {
                self.reject_if_read_only("DROP INDEX")?;
                self.storage.drop_index(index_name)?;
                Ok(ExecutionResult::Ddl {
                    message: format!("DROP INDEX {}", index_name),
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
                    OwnedRow::new(vec![Datum::Text("total_committed".into()), Datum::Int64(stats.total_committed as i64)]),
                    OwnedRow::new(vec![Datum::Text("fast_path_commits".into()), Datum::Int64(stats.fast_path_commits as i64)]),
                    OwnedRow::new(vec![Datum::Text("slow_path_commits".into()), Datum::Int64(stats.slow_path_commits as i64)]),
                    OwnedRow::new(vec![Datum::Text("total_aborted".into()), Datum::Int64(stats.total_aborted as i64)]),
                    OwnedRow::new(vec![Datum::Text("occ_conflicts".into()), Datum::Int64(stats.occ_conflicts as i64)]),
                    OwnedRow::new(vec![Datum::Text("degraded_to_global".into()), Datum::Int64(stats.degraded_to_global as i64)]),
                    OwnedRow::new(vec![Datum::Text("active_count".into()), Datum::Int64(stats.active_count as i64)]),
                ];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowNodeRole => {
                let role = std::env::var("CEDAR_NODE_ROLE").unwrap_or_else(|_| "standalone".into());
                let columns = vec![
                    ("metric".into(), DataType::Text),
                    ("value".into(), DataType::Text),
                ];
                let rows = vec![
                    OwnedRow::new(vec![Datum::Text("role".into()), Datum::Text(role)]),
                ];
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
                    OwnedRow::new(vec![Datum::Text("wal_enabled".into()), Datum::Text(wal_enabled.to_string())]),
                    OwnedRow::new(vec![Datum::Text("records_written".into()), Datum::Text(ws.records_written.to_string())]),
                    OwnedRow::new(vec![Datum::Text("observer_notifications".into()), Datum::Text(ws.observer_notifications.to_string())]),
                    OwnedRow::new(vec![Datum::Text("flushes".into()), Datum::Text(ws.flushes.to_string())]),
                ];
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::ShowConnections => {
                let active = self.active_connections.as_ref()
                    .map(|a| a.load(std::sync::atomic::Ordering::Relaxed))
                    .unwrap_or(0);
                let columns = vec![
                    ("metric".into(), DataType::Text),
                    ("value".into(), DataType::Text),
                ];
                let rows = vec![
                    OwnedRow::new(vec![Datum::Text("active_connections".into()), Datum::Text(active.to_string())]),
                    OwnedRow::new(vec![Datum::Text("max_connections".into()), Datum::Text(self.max_connections.to_string())]),
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
                    OwnedRow::new(vec![Datum::Text("watermark_ts".into()), Datum::Int64(watermark.0 as i64)]),
                    OwnedRow::new(vec![Datum::Text("chains_processed".into()), Datum::Int64(chains_processed as i64)]),
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
                            Datum::Text(cs.min_value.as_ref().map(|d| format!("{}", d)).unwrap_or_default()),
                            Datum::Text(cs.max_value.as_ref().map(|d| format!("{}", d)).unwrap_or_default()),
                            Datum::Int64(cs.avg_width as i64),
                        ]));
                    }
                }
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::CreateSequence { name, start } => {
                self.reject_if_read_only("CREATE SEQUENCE")?;
                self.storage.create_sequence(name, *start)
                    .map_err(CedarError::Storage)?;
                Ok(ExecutionResult::Ddl {
                    message: format!("CREATE SEQUENCE {}", name),
                })
            }
            PhysicalPlan::DropSequence { name, if_exists } => {
                self.reject_if_read_only("DROP SEQUENCE")?;
                match self.storage.drop_sequence(name) {
                    Ok(_) => Ok(ExecutionResult::Ddl {
                        message: format!("DROP SEQUENCE {}", name),
                    }),
                    Err(_) if *if_exists => Ok(ExecutionResult::Ddl {
                        message: format!("DROP SEQUENCE IF EXISTS {}", name),
                    }),
                    Err(e) => Err(CedarError::Storage(e)),
                }
            }
            PhysicalPlan::ShowSequences => {
                let seqs = self.storage.list_sequences();
                let columns = vec![
                    ("sequence_name".into(), DataType::Text),
                    ("current_value".into(), DataType::Int64),
                ];
                let rows: Vec<OwnedRow> = seqs.into_iter()
                    .map(|(name, val)| OwnedRow::new(vec![
                        Datum::Text(name),
                        Datum::Int64(val),
                    ]))
                    .collect();
                Ok(ExecutionResult::Query { columns, rows })
            }
            PhysicalPlan::CopyFrom { .. } => {
                // CopyFrom is handled specially by the protocol layer.
                // The executor processes the received data via exec_copy_from_data.
                Err(CedarError::Internal(
                    "CopyFrom must be handled by the protocol layer".into(),
                ))
            }
            PhysicalPlan::CopyTo {
                table_id, schema, columns,
                csv, delimiter, header, null_string, quote, escape,
            } => {
                let txn = txn.ok_or(CedarError::Internal(
                    "COPY TO requires active transaction".into(),
                ))?;
                self.exec_copy_to(
                    *table_id, schema, columns,
                    *csv, *delimiter, *header, null_string, *quote, *escape, txn,
                )
            }
            PhysicalPlan::CopyQueryTo {
                query, csv, delimiter, header, null_string, quote, escape,
            } => {
                let txn = txn.ok_or(CedarError::Internal(
                    "COPY TO requires active transaction".into(),
                ))?;
                self.exec_copy_query_to(
                    query, *csv, *delimiter, *header, null_string, *quote, *escape, txn,
                )
            }
            PhysicalPlan::DistPlan { .. } => {
                Err(CedarError::Internal(
                    "DistPlan must be executed via DistributedQueryEngine, not the local Executor".into(),
                ))
            }
        }
    }

    fn exec_create_table(&self, schema: &TableSchema, if_not_exists: bool) -> Result<ExecutionResult, CedarError> {
        match self.storage.create_table(schema.clone()) {
            Ok(_) => Ok(ExecutionResult::Ddl {
                message: format!("CREATE TABLE {}", schema.name),
            }),
            Err(e) if if_not_exists => {
                // IF NOT EXISTS — silently succeed
                Ok(ExecutionResult::Ddl {
                    message: format!("CREATE TABLE IF NOT EXISTS {} (already exists)", schema.name),
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    fn exec_drop_table(&self, table_name: &str, if_exists: bool) -> Result<ExecutionResult, CedarError> {
        match self.storage.drop_table(table_name) {
            Ok(_) => Ok(ExecutionResult::Ddl {
                message: format!("DROP TABLE {}", table_name),
            }),
            Err(e) if if_exists => {
                // IF EXISTS — silently succeed
                Ok(ExecutionResult::Ddl {
                    message: format!("DROP TABLE IF EXISTS {} (not found)", table_name),
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    fn exec_alter_table(
        &self,
        table_name: &str,
        ops: &[AlterTableOp],
    ) -> Result<ExecutionResult, CedarError> {
        let mut messages = Vec::new();
        let mut ddl_ids = Vec::new();
        for op in ops {
            let (ddl_id, msg) = match op {
                AlterTableOp::AddColumn(col) => {
                    let id = self.storage.alter_table_add_column(table_name, col.clone())?;
                    (id, format!("ADD COLUMN {}", col.name))
                }
                AlterTableOp::DropColumn(col_name) => {
                    let id = self.storage.alter_table_drop_column(table_name, col_name)?;
                    (id, format!("DROP COLUMN {}", col_name))
                }
                AlterTableOp::RenameColumn { old_name, new_name } => {
                    let id = self.storage.alter_table_rename_column(table_name, old_name, new_name)?;
                    (id, format!("RENAME COLUMN {} TO {}", old_name, new_name))
                }
                AlterTableOp::RenameTable { new_name } => {
                    let id = self.storage.alter_table_rename(table_name, new_name)?;
                    (id, format!("RENAME TO {}", new_name))
                }
                AlterTableOp::AlterColumnType { column_name, new_type } => {
                    let id = self.storage.alter_table_change_column_type(table_name, column_name, new_type.clone())?;
                    (id, format!("ALTER COLUMN {} TYPE {}", column_name, new_type))
                }
                AlterTableOp::AlterColumnSetNotNull { column_name } => {
                    let id = self.storage.alter_table_set_not_null(table_name, column_name)?;
                    (id, format!("ALTER COLUMN {} SET NOT NULL", column_name))
                }
                AlterTableOp::AlterColumnDropNotNull { column_name } => {
                    let id = self.storage.alter_table_drop_not_null(table_name, column_name)?;
                    (id, format!("ALTER COLUMN {} DROP NOT NULL", column_name))
                }
                AlterTableOp::AlterColumnSetDefault { column_name, default_expr } => {
                    let default_val = crate::eval::eval_expr(default_expr, &cedar_common::datum::OwnedRow::new(vec![]))
                        .map_err(CedarError::Execution)?;
                    let id = self.storage.alter_table_set_default(table_name, column_name, default_val)?;
                    (id, format!("ALTER COLUMN {} SET DEFAULT", column_name))
                }
                AlterTableOp::AlterColumnDropDefault { column_name } => {
                    let id = self.storage.alter_table_drop_default(table_name, column_name)?;
                    (id, format!("ALTER COLUMN {} DROP DEFAULT", column_name))
                }
            };
            ddl_ids.push(ddl_id);
            messages.push(msg);
        }
        let ids_str = ddl_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(",");
        Ok(ExecutionResult::Ddl {
            message: format!("ALTER TABLE {} {} [ddl_ids={}]", table_name, messages.join(", "), ids_str),
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
                BoundProjection::Window(wf) => {
                    (wf.alias.clone(), DataType::Int64)
                }
            })
            .collect()
    }

    /// Format a physical plan as human-readable text lines.
    pub fn format_plan(&self, plan: &PhysicalPlan, indent: usize) -> Vec<String> {
        let pad = "  ".repeat(indent);
        match plan {
            PhysicalPlan::SeqScan { table_id, schema, projections, filter, group_by, order_by, limit, offset, distinct, unions, .. } => {
                // Row estimate from table stats or memtable
                let base_rows = self.storage.get_table_stats(*table_id)
                    .map(|s| s.row_count as f64)
                    .unwrap_or_else(|| {
                        self.storage.get_table(*table_id)
                            .map(|t| t.row_count_approx() as f64)
                            .unwrap_or(1000.0)
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
                    pad, schema.name, startup_cost, total_cost,
                    est_rows as u64, schema.columns.len() * 8
                )];
                let cols: Vec<String> = projections.iter().map(|p| match p {
                    BoundProjection::Column(_, a) => a.clone(),
                    BoundProjection::Aggregate(_, _, a, _, _) => a.clone(),
                    BoundProjection::Expr(_, a) => a.clone(),
                    BoundProjection::Window(wf) => wf.alias.clone(),
                }).collect();
                lines.push(format!("{}  Output: {}", pad, cols.join(", ")));
                if let Some(f) = filter { lines.push(format!("{}  Filter: {:?}", pad, f)); }
                if !group_by.is_empty() { lines.push(format!("{}  Group By: {:?}", pad, group_by)); }
                if !order_by.is_empty() { lines.push(format!("{}  Sort Key: {} column(s)", pad, order_by.len())); }
                if let Some(l) = limit { lines.push(format!("{}  Limit: {}", pad, l)); }
                if let Some(o) = offset { lines.push(format!("{}  Offset: {}", pad, o)); }
                if !matches!(distinct, DistinctMode::None) { lines.push(format!("{}  Distinct: {:?}", pad, distinct)); }
                if !unions.is_empty() { lines.push(format!("{}  Set Ops: {} additional query(ies)", pad, unions.len())); }
                lines
            }
            PhysicalPlan::IndexScan { table_id, schema, index_col, index_value, projections, filter, order_by, limit, offset, distinct, unions, .. } => {
                let base_rows = self.storage.get_table_stats(*table_id)
                    .map(|s| s.row_count as f64)
                    .unwrap_or_else(|| {
                        self.storage.get_table(*table_id)
                            .map(|t| t.row_count_approx() as f64)
                            .unwrap_or(1000.0)
                    });
                let est_rows = if filter.is_some() { (base_rows * 0.01).max(1.0) } else { (base_rows * 0.01).max(1.0) };
                let startup_cost = 0.0_f64;
                let total_cost = est_rows * 0.01 + 1.0; // index lookup cost
                let col_name = schema.columns.get(*index_col).map(|c| c.name.as_str()).unwrap_or("?");
                let mut lines = vec![format!(
                    "{}Index Scan using {} on {}  (cost={:.2}..{:.2} rows={} width={})",
                    pad, col_name, schema.name, startup_cost, total_cost,
                    est_rows as u64, schema.columns.len() * 8
                )];
                lines.push(format!("{}  Index Cond: ({} = {:?})", pad, col_name, index_value));
                let cols: Vec<String> = projections.iter().map(|p| match p {
                    BoundProjection::Column(_, a) => a.clone(),
                    BoundProjection::Aggregate(_, _, a, _, _) => a.clone(),
                    BoundProjection::Expr(_, a) => a.clone(),
                    BoundProjection::Window(wf) => wf.alias.clone(),
                }).collect();
                lines.push(format!("{}  Output: {}", pad, cols.join(", ")));
                if let Some(f) = filter { lines.push(format!("{}  Filter: {:?}", pad, f)); }
                if !order_by.is_empty() { lines.push(format!("{}  Sort Key: {} column(s)", pad, order_by.len())); }
                if let Some(l) = limit { lines.push(format!("{}  Limit: {}", pad, l)); }
                if let Some(o) = offset { lines.push(format!("{}  Offset: {}", pad, o)); }
                if !matches!(distinct, DistinctMode::None) { lines.push(format!("{}  Distinct: {:?}", pad, distinct)); }
                if !unions.is_empty() { lines.push(format!("{}  Set Ops: {} additional query(ies)", pad, unions.len())); }
                lines
            }
            PhysicalPlan::NestedLoopJoin { left_table_id, left_schema, joins, projections, filter, order_by, limit, offset, distinct, unions, .. }
            | PhysicalPlan::HashJoin { left_table_id, left_schema, joins, projections, filter, order_by, limit, offset, distinct, unions, .. }
            | PhysicalPlan::MergeSortJoin { left_table_id, left_schema, joins, projections, filter, order_by, limit, offset, distinct, unions, .. } => {
                let is_hash = matches!(plan, PhysicalPlan::HashJoin { .. });
                let is_merge = matches!(plan, PhysicalPlan::MergeSortJoin { .. });
                let strategy = if is_merge { "Merge Sort Join" } else if is_hash { "Hash Join" } else { "Nested Loop" };
                // Estimate left table rows
                let left_rows = self.storage.get_table_stats(*left_table_id)
                    .map(|s| s.row_count as f64)
                    .unwrap_or_else(|| {
                        self.storage.get_table(*left_table_id)
                            .map(|t| t.row_count_approx() as f64)
                            .unwrap_or(1000.0)
                    });
                // Estimate join output rows
                let mut est_rows = left_rows;
                for j in joins {
                    let right_rows = self.storage.get_table_schema(&j.right_table_name)
                        .and_then(|s| self.storage.get_table(s.id).map(|t| t.row_count_approx() as f64))
                        .unwrap_or(1000.0);
                    est_rows = if is_hash {
                        // Hash join: roughly min(left, right) for equi-join
                        (left_rows * right_rows * 0.1).max(1.0)
                    } else {
                        left_rows * right_rows * 0.1
                    };
                }
                if filter.is_some() { est_rows *= 0.33; }
                if let Some(l) = limit { est_rows = est_rows.min(*l as f64); }
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
                    pad, strategy, left_schema.name, startup_cost, total_cost,
                    est_rows as u64, width
                )];
                for j in joins {
                    lines.push(format!("{}  -> {:?} JOIN {}", pad, j.join_type, j.right_table_name));
                }
                let cols: Vec<String> = projections.iter().map(|p| match p {
                    BoundProjection::Column(_, a) => a.clone(),
                    BoundProjection::Aggregate(_, _, a, _, _) => a.clone(),
                    BoundProjection::Expr(_, a) => a.clone(),
                    BoundProjection::Window(wf) => wf.alias.clone(),
                }).collect();
                lines.push(format!("{}  Output: {}", pad, cols.join(", ")));
                if let Some(f) = filter { lines.push(format!("{}  Filter: {:?}", pad, f)); }
                if !order_by.is_empty() { lines.push(format!("{}  Sort Key: {} column(s)", pad, order_by.len())); }
                if let Some(l) = limit { lines.push(format!("{}  Limit: {}", pad, l)); }
                if let Some(o) = offset { lines.push(format!("{}  Offset: {}", pad, o)); }
                if !matches!(distinct, DistinctMode::None) { lines.push(format!("{}  Distinct: {:?}", pad, distinct)); }
                if !unions.is_empty() { lines.push(format!("{}  Set Ops: {} additional query(ies)", pad, unions.len())); }
                lines
            }
            PhysicalPlan::Insert { schema, columns, returning, .. } => {
                let col_names: Vec<String> = columns.iter().map(|&i| schema.columns[i].name.clone()).collect();
                let mut lines = vec![format!("{}Insert on {} ({})", pad, schema.name, col_names.join(", "))];
                if !returning.is_empty() { lines.push(format!("{}  Returning: {} column(s)", pad, returning.len())); }
                lines
            }
            PhysicalPlan::Update { schema, assignments, filter, returning, .. } => {
                let cols: Vec<String> = assignments.iter().map(|(i, _)| schema.columns[*i].name.clone()).collect();
                let mut lines = vec![format!("{}Update on {} SET {}", pad, schema.name, cols.join(", "))];
                if let Some(f) = filter { lines.push(format!("{}  Filter: {:?}", pad, f)); }
                if !returning.is_empty() { lines.push(format!("{}  Returning: {} column(s)", pad, returning.len())); }
                lines
            }
            PhysicalPlan::Delete { schema, filter, returning, .. } => {
                let mut lines = vec![format!("{}Delete on {}", pad, schema.name)];
                if let Some(f) = filter { lines.push(format!("{}  Filter: {:?}", pad, f)); }
                if !returning.is_empty() { lines.push(format!("{}  Returning: {} column(s)", pad, returning.len())); }
                lines
            }
            PhysicalPlan::CreateTable { schema, if_not_exists } => {
                vec![format!("{}CreateTable {} (if_not_exists={})", pad, schema.name, if_not_exists)]
            }
            PhysicalPlan::DropTable { table_name, if_exists } => {
                vec![format!("{}DropTable {} (if_exists={})", pad, table_name, if_exists)]
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
            PhysicalPlan::CreateIndex { index_name, table_name, .. } => {
                vec![format!("{}CreateIndex {} on {}", pad, index_name, table_name)]
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
            PhysicalPlan::DistPlan { subplan, target_shards, gather, .. } => {
                let mut lines = vec![format!(
                    "{}DistPlan (scatter to {} shards, gather={:?})",
                    pad, target_shards.len(), gather
                )];
                lines.extend(self.format_plan(subplan, indent + 1));
                lines
            }
        }
    }

}

