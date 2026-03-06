//! DML execution: DDL propagation, INSERT splitting, UPDATE/DELETE broadcast, GC.

use std::collections::HashMap;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_common::types::{DataType, IsolationLevel, ShardId, TxnId};
use falcon_executor::executor::{ExecutionResult, Executor};
use falcon_planner::plan::PhysicalPlan;
use falcon_sql_frontend::types::BoundExpr;

impl super::DistributedQueryEngine {
    /// Execute DDL on ALL shards in parallel. Returns the result from the first shard.
    pub(crate) fn exec_ddl_all_shards(&self, plan: &PhysicalPlan) -> Result<ExecutionResult, FalconError> {
        let results: Vec<Result<ExecutionResult, FalconError>> = std::thread::scope(|s| {
            #[allow(clippy::needless_collect)] // scoped JoinHandles must be stored before joining
            let handles: Vec<_> = self
                .engine
                .all_shards()
                .iter()
                .map(|shard| {
                    let plan_ref = plan;
                    s.spawn(move || {
                        let local_exec =
                            Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                        local_exec.execute(plan_ref, None)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join().unwrap_or_else(|_| {
                        Err(FalconError::internal_bug(
                            "E-QE-001",
                            "shard thread panicked",
                            "exec_ddl_all_shards",
                        ))
                    })
                })
                .collect()
        });

        let mut first_result = None;
        for r in results {
            let result = r?;
            if first_result.is_none() {
                first_result = Some(result);
            }
        }
        first_result.ok_or_else(|| {
            FalconError::internal_bug("E-QE-009", "No shards available", "exec_gc_all_shards")
        })
    }

    /// Execute a DML plan on a specific shard with an auto-commit transaction.
    pub(crate) fn execute_on_shard_auto_txn(
        &self,
        shard_id: ShardId,
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        let shard = self.engine.shard(shard_id).ok_or_else(|| {
            FalconError::internal_bug(
                "E-QE-010",
                format!("Shard {shard_id:?} not found"),
                "exec_dml_autocommit",
            )
        })?;
        let local_exec = Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
        let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
        let result = local_exec.execute(plan, Some(&txn));
        match &result {
            Ok(_) => {
                let _ = shard.txn_mgr.commit(txn.txn_id);
            }
            Err(_) => {
                let _ = shard.txn_mgr.abort(txn.txn_id);
            }
        }
        result
    }

    /// Split a multi-row INSERT by target shard: group rows by PK hash,
    /// build a per-shard INSERT plan, and execute each on its target shard.
    /// Falls back to shard 0 for rows whose PK cannot be extracted.
    pub(crate) fn exec_insert_split(&self, plan: &PhysicalPlan) -> Result<ExecutionResult, FalconError> {
        let (table_id, schema, columns, rows, source_select, returning, on_conflict) = match plan {
            PhysicalPlan::Insert {
                table_id,
                schema,
                columns,
                rows,
                source_select,
                returning,
                on_conflict,
            } => (
                *table_id,
                schema,
                columns,
                rows,
                source_select,
                returning,
                on_conflict,
            ),
            _ => {
                return Err(FalconError::internal_bug(
                    "E-QE-011",
                    "exec_insert_split called with non-Insert",
                    "plan type mismatch",
                ))
            }
        };

        // INSERT ... SELECT — execute the SELECT at coordinator level to see all
        // shard data, then convert results to VALUES and split by PK hash.
        if let Some(sel) = source_select {
            return self.exec_insert_select(table_id, schema, columns, sel, returning, on_conflict);
        }

        if rows.is_empty() {
            return self.execute_on_shard_auto_txn(ShardId(0), plan);
        }

        // Group rows by target shard
        let pk_col_idx = schema.primary_key_columns.first().copied();
        let mut shard_rows: HashMap<ShardId, Vec<Vec<BoundExpr>>> = HashMap::new();

        for row in rows {
            let target = pk_col_idx
                .and_then(|idx| row.get(idx))
                .and_then(|expr| self.extract_int_key(expr))
                .map_or(ShardId(0), |key| self.engine.shard_for_key(key));
            shard_rows.entry(target).or_default().push(row.clone());
        }

        // Build per-shard INSERT plans
        let shard_plans: Vec<(ShardId, PhysicalPlan)> = shard_rows
            .into_iter()
            .map(|(shard_id, shard_row_batch)| {
                let shard_plan = PhysicalPlan::Insert {
                    table_id,
                    schema: schema.clone(),
                    columns: columns.clone(),
                    rows: shard_row_batch,
                    source_select: None,
                    returning: returning.clone(),
                    on_conflict: on_conflict.clone(),
                };
                (shard_id, shard_plan)
            })
            .collect();

        // Single shard → skip thread overhead
        if shard_plans.len() == 1 {
            let (sid, sp) = &shard_plans[0];
            return self.execute_on_shard_auto_txn(*sid, sp);
        }

        // Two-phase execution for cross-shard atomicity:
        // Phase 1: execute on all shards without committing
        let shard_count = shard_plans.len();
        let snapshot_ts = self.engine.consistent_snapshot_ts();

        let phase1: Vec<(ShardId, TxnId, Result<ExecutionResult, FalconError>)> =
            std::thread::scope(|s| {
                let handles: Vec<_> = shard_plans
                    .iter()
                    .map(|(shard_id, shard_plan)| {
                        let engine = &self.engine;
                        let sid = *shard_id;
                        s.spawn(move || {
                            let shard = match engine.shard(sid) {
                                Some(s) => s,
                                None => {
                                    return (
                                        sid,
                                        TxnId(0),
                                        Err(FalconError::internal_bug(
                                            "E-QE-012",
                                            format!("Shard {sid:?} not found"),
                                            "exec_insert_split parallel",
                                        )),
                                    )
                                }
                            };
                            let local_exec =
                                Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                            let txn = shard.txn_mgr.begin_at_snapshot_on_shard(snapshot_ts, IsolationLevel::ReadCommitted, sid);
                            let txn_id = txn.txn_id;
                            let result = local_exec.execute(shard_plan, Some(&txn));
                            shard.storage.flush_local_cache();
                            (sid, txn_id, result)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join().unwrap_or_else(|_| {
                            (
                                ShardId(0),
                                TxnId(0),
                                Err(FalconError::internal_bug(
                                    "E-QE-002",
                                    "shard thread panicked",
                                    "exec_insert_split phase1",
                                )),
                            )
                        })
                    })
                    .collect()
            });

        // Phase 2: commit all on success, abort all on any failure
        let mut all_ok = phase1.iter().all(|(_, _, r)| r.is_ok());

        // WAL prepare before decision — if any prepare fails, downgrade to abort
        if all_ok {
            for (sid, txn_id, _) in &phase1 {
                if *txn_id != TxnId(0) {
                    if let Some(shard) = self.engine.shard(*sid) {
                        if let Err(e) = shard.storage.prepare_txn(*txn_id) {
                            tracing::error!(shard = ?sid, txn = txn_id.0, error = %e, "WAL prepare failed — aborting all");
                            all_ok = false;
                            break;
                        }
                    }
                }
            }
        }

        // Log coordinator decision durably BEFORE applying
        let shard_ids: Vec<ShardId> = phase1.iter().map(|(sid, _, _)| *sid).collect();
        let global_txn_id = self.two_pc.alloc_global_txn_id();
        let decision = if all_ok {
            crate::deterministic_2pc::CoordinatorDecision::Commit
        } else {
            crate::deterministic_2pc::CoordinatorDecision::Abort
        };
        if let Some(log) = self.two_pc.decision_log() {
            log.log_decision(global_txn_id, decision, &shard_ids, 0);
        }

        for (sid, txn_id, _) in &phase1 {
            if let Some(shard) = self.engine.shard(*sid) {
                if *txn_id != TxnId(0) {
                    if all_ok {
                        let _ = shard.txn_mgr.commit(*txn_id);
                    } else {
                        let _ = shard.txn_mgr.abort(*txn_id);
                    }
                }
            }
        }

        if let Some(log) = self.two_pc.decision_log() {
            log.mark_applied(global_txn_id);
        }

        let results: Vec<(ShardId, Result<ExecutionResult, FalconError>)> = phase1
            .into_iter()
            .map(|(sid, _, r)| (sid, r))
            .collect();
        Self::merge_dml_results(results, shard_count)
    }

    /// Execute INSERT ... SELECT at coordinator level: run the SELECT across
    /// all shards (gathering all source data), then convert results to VALUES
    /// rows and split-insert by PK hash for correct shard placement.
    fn exec_insert_select(
        &self,
        table_id: falcon_common::types::TableId,
        schema: &falcon_common::schema::TableSchema,
        columns: &[usize],
        source_select: &falcon_sql_frontend::types::BoundSelect,
        returning: &[(BoundExpr, String)],
        on_conflict: &Option<falcon_sql_frontend::types::OnConflictAction>,
    ) -> Result<ExecutionResult, FalconError> {
        // Plan the SELECT and execute at coordinator level to see all shard data
        let select_plan = falcon_planner::Planner::plan(
            &falcon_sql_frontend::types::BoundStatement::Select(source_select.clone()),
        )?;
        let select_table_ids = Self::extract_table_ids(&select_plan);
        let select_result = self.gather_and_execute_locally(&select_table_ids, &select_plan)?;

        let result_rows = match select_result {
            ExecutionResult::Query { rows, .. } => rows,
            _ => {
                return Ok(ExecutionResult::Dml {
                    rows_affected: 0,
                    tag: "INSERT",
                })
            }
        };

        if result_rows.is_empty() {
            return Ok(ExecutionResult::Dml {
                rows_affected: 0,
                tag: "INSERT",
            });
        }

        // Convert OwnedRows to BoundExpr::Literal VALUES rows
        let value_rows: Vec<Vec<BoundExpr>> = result_rows
            .iter()
            .map(|row| {
                row.values
                    .iter()
                    .map(|d| BoundExpr::Literal(d.clone()))
                    .collect()
            })
            .collect();

        // Build an INSERT with VALUES (no source_select) and use the existing split logic
        let values_plan = PhysicalPlan::Insert {
            table_id,
            schema: schema.clone(),
            columns: columns.to_vec(),
            rows: value_rows,
            source_select: None,
            returning: returning.to_vec(),
            on_conflict: on_conflict.clone(),
        };
        self.exec_insert_split(&values_plan)
    }

    /// Execute RunGc on ALL shards in parallel, aggregating results.
    pub(crate) fn exec_gc_all_shards(&self) -> Result<ExecutionResult, FalconError> {
        let gc_results: Vec<Result<ExecutionResult, FalconError>> = std::thread::scope(|s| {
            #[allow(clippy::needless_collect)] // scoped JoinHandles must be stored before joining
            let handles: Vec<_> = self
                .engine
                .all_shards()
                .iter()
                .map(|shard| {
                    s.spawn(move || {
                        let local_exec =
                            Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                        local_exec.execute(&PhysicalPlan::RunGc, None)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join().unwrap_or_else(|_| {
                        Err(FalconError::internal_bug(
                            "E-QE-003",
                            "shard thread panicked",
                            "exec_gc_all_shards",
                        ))
                    })
                })
                .collect()
        });

        let mut total_chains = 0i64;
        let mut watermark = 0i64;
        for r in gc_results {
            if let ExecutionResult::Query { rows, .. } = r? {
                for row in &rows {
                    if row.values.len() >= 2 {
                        if let Datum::Text(ref key) = row.values[0] {
                            if let Datum::Int64(val) = row.values[1] {
                                match key.as_str() {
                                    "chains_processed" => total_chains += val,
                                    "watermark_ts" => {
                                        if val > watermark {
                                            watermark = val;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
        let columns = vec![
            ("metric".to_owned(), DataType::Text),
            ("value".to_owned(), DataType::Int64),
        ];
        let rows = vec![
            OwnedRow::new(vec![
                Datum::Text("watermark_ts".into()),
                Datum::Int64(watermark),
            ]),
            OwnedRow::new(vec![
                Datum::Text("chains_processed".into()),
                Datum::Int64(total_chains),
            ]),
            OwnedRow::new(vec![
                Datum::Text("shards_processed".into()),
                Datum::Int64(self.engine.all_shards().len() as i64),
            ]),
        ];
        Ok(ExecutionResult::Query { columns, rows })
    }

    /// Execute a DML plan on a specific subset of shards (IN-list pruning).
    ///
    /// fix58: All shard transactions start at the same consistent snapshot timestamp.
    /// fix59: Two-phase execution — run DML on all shards first, then commit all on
    ///        success or abort all on any failure, ensuring cross-shard atomicity.
    pub(crate) fn exec_dml_on_shards(
        &self,
        plan: &PhysicalPlan,
        shards: &[ShardId],
    ) -> Result<ExecutionResult, FalconError> {
        let shard_count = shards.len();
        // fix58: consistent snapshot across all shards
        let snapshot_ts = self.engine.consistent_snapshot_ts();

        // Phase 1: execute on all shards, do not commit yet
        let phase1: Vec<(u64, TxnId, Result<ExecutionResult, FalconError>)> = std::thread::scope(|s| {
            let handles: Vec<_> = shards
                .iter()
                .map(|sid| {
                    let shard = self.engine.shard(*sid);
                    let plan_ref = plan;
                    let shard_id = sid.0;
                    s.spawn(move || {
                        let shard = match shard {
                            Some(s) => s,
                            None => {
                                return (
                                    shard_id,
                                    TxnId(0),
                                    Err(FalconError::internal_bug(
                                        "E-QE-004",
                                        format!("DML dispatch: shard {shard_id} not found"),
                                        "exec_dml_on_shards",
                                    )),
                                )
                            }
                        };
                        let local_exec =
                            Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                        let txn = shard.txn_mgr.begin_at_snapshot_on_shard(snapshot_ts, IsolationLevel::ReadCommitted, *sid);
                        let txn_id = txn.txn_id;
                        let result = local_exec.execute(plan_ref, Some(&txn));
                        shard.storage.flush_local_cache();
                        (shard_id, txn_id, result)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join().unwrap_or_else(|_| {
                        (
                            0,
                            TxnId(0),
                            Err(FalconError::internal_bug(
                                "E-QE-005",
                                "DML dispatch: shard thread panicked",
                                "exec_dml_on_shards phase1",
                            )),
                        )
                    })
                })
                .collect()
        });

        // Phase 2: fix59 — commit all on success, abort all on any failure
        let mut all_ok = phase1.iter().all(|(_, _, r)| r.is_ok());

        // WAL prepare before decision — if any prepare fails, downgrade to abort
        if all_ok {
            for (shard_id, txn_id, _) in &phase1 {
                if *txn_id != TxnId(0) {
                    if let Some(shard) = self.engine.shard(ShardId(*shard_id)) {
                        if let Err(e) = shard.storage.prepare_txn(*txn_id) {
                            tracing::error!(shard = shard_id, txn = txn_id.0, error = %e, "WAL prepare failed — aborting all");
                            all_ok = false;
                            break;
                        }
                    }
                }
            }
        }

        // Log coordinator decision durably BEFORE applying
        let dml_shard_ids: Vec<ShardId> = phase1.iter().map(|(sid, _, _)| ShardId(*sid)).collect();
        let global_txn_id = self.two_pc.alloc_global_txn_id();
        let decision = if all_ok {
            crate::deterministic_2pc::CoordinatorDecision::Commit
        } else {
            crate::deterministic_2pc::CoordinatorDecision::Abort
        };
        if let Some(log) = self.two_pc.decision_log() {
            log.log_decision(global_txn_id, decision, &dml_shard_ids, 0);
        }

        for (shard_id, txn_id, _) in &phase1 {
            if let Some(shard) = self.engine.shard(ShardId(*shard_id)) {
                if *txn_id != TxnId(0) {
                    if all_ok {
                        let _ = shard.txn_mgr.commit(*txn_id);
                    } else {
                        let _ = shard.txn_mgr.abort(*txn_id);
                    }
                }
            }
        }

        if let Some(log) = self.two_pc.decision_log() {
            log.mark_applied(global_txn_id);
        }

        let results: Vec<(u64, Result<ExecutionResult, FalconError>)> = phase1
            .into_iter()
            .map(|(sid, _, r)| (sid, r))
            .collect();
        Self::merge_dml_results(results, shard_count)
    }

    /// Merge DML results from multiple shards, handling both Dml and Query (RETURNING) results.
    pub(crate) fn merge_dml_results(
        results: Vec<(impl std::fmt::Display, Result<ExecutionResult, FalconError>)>,
        shard_count: usize,
    ) -> Result<ExecutionResult, FalconError> {
        let mut total_rows_affected = 0u64;
        let mut tag: &'static str = "";
        let mut errors: Vec<String> = Vec::new();
        let mut succeeded = 0usize;
        // For RETURNING: collect query rows from all shards
        let mut returning_columns: Option<Vec<(String, falcon_common::types::DataType)>> = None;
        let mut returning_rows: Vec<falcon_common::datum::OwnedRow> = Vec::new();

        for (shard_id, result) in results {
            match result {
                Ok(ExecutionResult::Dml {
                    rows_affected,
                    tag: t,
                }) => {
                    total_rows_affected += rows_affected;
                    if tag.is_empty() {
                        tag = t;
                    }
                    succeeded += 1;
                }
                Ok(ExecutionResult::Query { columns, rows }) => {
                    // RETURNING clause produces Query results
                    if returning_columns.is_none() {
                        returning_columns = Some(columns);
                    }
                    returning_rows.extend(rows);
                    succeeded += 1;
                }
                Ok(_) => {
                    succeeded += 1;
                }
                Err(e) => errors.push(format!("shard {shard_id}: {e}")),
            }
        }

        if !errors.is_empty() {
            return Err(FalconError::Transient {
                reason: format!(
                    "DML failed on {}/{} shards ({} succeeded): {}",
                    errors.len(),
                    shard_count,
                    succeeded,
                    errors.join("; ")
                ),
                retry_after_ms: 100,
            });
        }

        // If any shard returned RETURNING rows, return merged Query result
        if let Some(columns) = returning_columns {
            return Ok(ExecutionResult::Query {
                columns,
                rows: returning_rows,
            });
        }

        Ok(ExecutionResult::Dml {
            rows_affected: total_rows_affected,
            tag,
        })
    }

    /// Execute a DML plan (UPDATE/DELETE) on ALL shards in parallel, summing rows_affected.
    ///
    /// fix58: consistent snapshot timestamp across all shards.
    /// fix59: Two-phase execution — commit all on success, abort all on failure.
    pub(crate) fn exec_dml_all_shards(&self, plan: &PhysicalPlan) -> Result<ExecutionResult, FalconError> {
        let shard_count = self.engine.all_shards().len();
        // fix58: consistent snapshot
        let snapshot_ts = self.engine.consistent_snapshot_ts();

        // Phase 1: execute DML on all shards without committing
        let phase1: Vec<(usize, TxnId, Result<ExecutionResult, FalconError>)> = std::thread::scope(|s| {
            #[allow(clippy::needless_collect)]
            let handles: Vec<_> = self
                .engine
                .all_shards()
                .iter()
                .enumerate()
                .map(|(idx, shard)| {
                    let plan_ref = plan;
                    s.spawn(move || {
                        let local_exec =
                            Executor::new(shard.storage.clone(), shard.txn_mgr.clone());
                        let txn = shard.txn_mgr.begin_at_snapshot_on_shard(snapshot_ts, IsolationLevel::ReadCommitted, shard.shard_id);
                        let txn_id = txn.txn_id;
                        let result = local_exec.execute(plan_ref, Some(&txn));
                        shard.storage.flush_local_cache();
                        (idx, txn_id, result)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join().unwrap_or_else(|_| {
                        (
                            usize::MAX,
                            TxnId(0),
                            Err(FalconError::internal_bug(
                                "E-QE-004",
                                "shard thread panicked",
                                "exec_dml_all_shards phase1",
                            )),
                        )
                    })
                })
                .collect()
        });

        // Phase 2: fix59 — commit all on success, abort all on any failure
        let mut all_ok = phase1.iter().all(|(_, _, r)| r.is_ok());

        // WAL prepare before decision — if any prepare fails, downgrade to abort
        if all_ok {
            for (idx, txn_id, _) in &phase1 {
                if *txn_id != TxnId(0) {
                    if let Some(shard) = self.engine.shard_by_index(*idx) {
                        if let Err(e) = shard.storage.prepare_txn(*txn_id) {
                            tracing::error!(shard = idx, txn = txn_id.0, error = %e, "WAL prepare failed — aborting all");
                            all_ok = false;
                            break;
                        }
                    }
                }
            }
        }

        // Log coordinator decision durably BEFORE applying
        let all_shard_ids: Vec<ShardId> = self.engine.all_shards().iter().map(|s| s.shard_id).collect();
        let global_txn_id = self.two_pc.alloc_global_txn_id();
        let decision = if all_ok {
            crate::deterministic_2pc::CoordinatorDecision::Commit
        } else {
            crate::deterministic_2pc::CoordinatorDecision::Abort
        };
        if let Some(log) = self.two_pc.decision_log() {
            log.log_decision(global_txn_id, decision, &all_shard_ids, 0);
        }

        for (idx, txn_id, _) in &phase1 {
            if let Some(shard) = self.engine.shard_by_index(*idx) {
                if *txn_id != TxnId(0) {
                    if all_ok {
                        if let Err(e) = shard.txn_mgr.commit(*txn_id) {
                            tracing::error!(shard = idx, txn = txn_id.0, error = %e, "commit failed in exec_dml_all_shards");
                        }
                    } else {
                        let _ = shard.txn_mgr.abort(*txn_id);
                    }
                }
            }
        }

        if let Some(log) = self.two_pc.decision_log() {
            log.mark_applied(global_txn_id);
        }

        let results: Vec<(usize, Result<ExecutionResult, FalconError>)> = phase1
            .into_iter()
            .map(|(idx, _, r)| (idx, r))
            .collect();
        Self::merge_dml_results(results, shard_count)
    }
}
