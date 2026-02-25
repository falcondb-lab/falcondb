//! EXPLAIN output formatting for distributed plans, coordinator joins, and subqueries.

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_common::types::{DataType, ShardId};
use falcon_executor::executor::{ExecutionResult, Executor};
use falcon_planner::plan::{DistGather, PhysicalPlan};

impl super::DistributedQueryEngine {
    /// EXPLAIN ANALYZE for a DistPlan: execute the plan and show per-shard stats.
    pub(crate) fn exec_explain_dist(&self, inner: &PhysicalPlan) -> Result<ExecutionResult, FalconError> {
        // Execute the inner DistPlan to populate scatter stats.
        let result = self.execute(inner, None);
        let stats = self.last_scatter_stats();

        // Build EXPLAIN output with plan structure + execution stats.
        let shard0 = self.engine.shard(ShardId(0)).ok_or_else(|| {
            FalconError::internal_bug(
                "E-QE-008",
                "No shard 0 available",
                "exec_explain_analyze_coordinator",
            )
        })?;
        let local_exec = Executor::new(shard0.storage.clone(), shard0.txn_mgr.clone());
        let plan_lines = local_exec.format_plan(inner, 0);

        let mut lines: Vec<OwnedRow> = plan_lines
            .into_iter()
            .map(|l| OwnedRow::new(vec![Datum::Text(l)]))
            .collect();

        // Append gather strategy details
        if let PhysicalPlan::DistPlan {
            gather,
            target_shards,
            ..
        } = inner
        {
            lines.push(OwnedRow::new(vec![Datum::Text(String::new())]));
            lines.push(OwnedRow::new(vec![Datum::Text(format!(
                "Distributed Plan ({} shards)",
                target_shards.len()
            ))]));
            match gather {
                DistGather::Union {
                    distinct,
                    limit,
                    offset,
                } => {
                    lines.push(OwnedRow::new(vec![Datum::Text(format!(
                        "  Gather: Union (distinct={distinct}, limit={limit:?}, offset={offset:?})"
                    ))]));
                }
                DistGather::MergeSortLimit {
                    sort_columns,
                    limit,
                    offset,
                } => {
                    let cols: Vec<String> = sort_columns
                        .iter()
                        .map(|(idx, asc)| {
                            format!("col{}:{}", idx, if *asc { "ASC" } else { "DESC" })
                        })
                        .collect();
                    lines.push(OwnedRow::new(vec![Datum::Text(format!(
                        "  Gather: MergeSortLimit (sort=[{}], limit={:?}, offset={:?})",
                        cols.join(", "),
                        limit,
                        offset
                    ))]));
                }
                DistGather::TwoPhaseAgg {
                    group_by_indices,
                    agg_merges,
                    having,
                    avg_fixups,
                    visible_columns,
                    order_by,
                    limit,
                    offset,
                } => {
                    let merges: Vec<String> = agg_merges.iter().map(std::string::ToString::to_string).collect();
                    lines.push(OwnedRow::new(vec![Datum::Text(format!(
                        "  Gather: TwoPhaseAgg (group_by={group_by_indices:?})"
                    ))]));
                    for merge_label in &merges {
                        lines.push(OwnedRow::new(vec![Datum::Text(format!(
                            "    merge: {merge_label}"
                        ))]));
                    }
                    if !avg_fixups.is_empty() {
                        lines.push(OwnedRow::new(vec![Datum::Text(format!(
                            "    AVG decomposition: {avg_fixups:?} (visible_columns={visible_columns})"
                        ))]));
                    }
                    if having.is_some() {
                        lines.push(OwnedRow::new(vec![Datum::Text(
                            "    HAVING: applied post-merge".into(),
                        )]));
                    }
                    if !order_by.is_empty() {
                        let cols: Vec<String> = order_by
                            .iter()
                            .map(|(idx, asc)| {
                                format!("col{}:{}", idx, if *asc { "ASC" } else { "DESC" })
                            })
                            .collect();
                        lines.push(OwnedRow::new(vec![Datum::Text(format!(
                            "    ORDER BY: [{}], LIMIT: {:?}, OFFSET: {:?}",
                            cols.join(", "),
                            limit,
                            offset
                        ))]));
                    }
                }
            }
        }

        // Append execution stats
        lines.push(OwnedRow::new(vec![Datum::Text(String::new())]));
        lines.push(OwnedRow::new(vec![Datum::Text(format!(
            "Execution Stats ({})",
            if result.is_ok() { "OK" } else { "ERROR" }
        ))]));
        lines.push(OwnedRow::new(vec![Datum::Text(format!(
            "  Shards participated: {}",
            stats.shards_participated
        ))]));
        lines.push(OwnedRow::new(vec![Datum::Text(format!(
            "  Total rows gathered: {}",
            stats.total_rows_gathered
        ))]));
        lines.push(OwnedRow::new(vec![Datum::Text(format!(
            "  Gather strategy: {}",
            stats.gather_strategy
        ))]));
        if let Some(pruned) = stats.pruned_to_shard {
            lines.push(OwnedRow::new(vec![Datum::Text(format!(
                "  Shard pruning: routed to shard {pruned} (PK point lookup)"
            ))]));
        }
        lines.push(OwnedRow::new(vec![Datum::Text(format!(
            "  Total latency: {}us",
            stats.total_latency_us
        ))]));
        for (sid, lat) in &stats.per_shard_latency_us {
            let row_count = stats
                .per_shard_row_count
                .iter()
                .find(|(s, _)| s == sid)
                .map_or(0, |(_, c)| *c);
            lines.push(OwnedRow::new(vec![Datum::Text(format!(
                "  Shard {sid} latency: {lat}us, rows: {row_count}"
            ))]));
        }
        if !stats.failed_shards.is_empty() {
            lines.push(OwnedRow::new(vec![Datum::Text(format!(
                "  Failed/timed-out shards: {:?}",
                stats.failed_shards
            ))]));
        }

        Ok(ExecutionResult::Query {
            columns: vec![("QUERY PLAN".into(), DataType::Text)],
            rows: lines,
        })
    }

    /// EXPLAIN for coordinator-side join: show the join strategy and tables involved.
    pub(crate) fn exec_explain_coordinator_join(
        &self,
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        let mut lines: Vec<OwnedRow> = Vec::new();
        let is_hash = matches!(plan, PhysicalPlan::HashJoin { .. });
        let strategy = if is_hash {
            "HashJoin"
        } else {
            "NestedLoopJoin"
        };
        lines.push(OwnedRow::new(vec![Datum::Text(format!(
            "CoordinatorJoin ({strategy}, gather-all-then-join)"
        ))]));
        let (left_table_id, joins, filter, order_by, limit, offset) = match plan {
            PhysicalPlan::NestedLoopJoin {
                left_table_id,
                joins,
                filter,
                order_by,
                limit,
                offset,
                ..
            }
            | PhysicalPlan::HashJoin {
                left_table_id,
                joins,
                filter,
                order_by,
                limit,
                offset,
                ..
            } => (left_table_id, joins, filter, order_by, limit, offset),
            _ => {
                return Err(FalconError::internal_bug(
                    "E-QE-006",
                    "exec_explain_coordinator_join: not a join plan",
                    "plan type mismatch",
                ))
            }
        };
        {
            // Show tables involved
            let mut table_ids = vec![*left_table_id];
            for join in joins {
                table_ids.push(join.right_table_id);
            }
            lines.push(OwnedRow::new(vec![Datum::Text(format!(
                "  Tables: {:?} (gathered from {} shards each)",
                table_ids,
                self.engine.shard_ids().len()
            ))]));

            // Show join types
            for (i, join) in joins.iter().enumerate() {
                lines.push(OwnedRow::new(vec![Datum::Text(format!(
                    "  Join {}: {:?} on table {:?}",
                    i, join.join_type, join.right_table_id
                ))]));
            }

            if filter.is_some() {
                lines.push(OwnedRow::new(vec![Datum::Text("  Filter: yes".into())]));
            }
            if !order_by.is_empty() {
                lines.push(OwnedRow::new(vec![Datum::Text(format!(
                    "  Order by: {} columns",
                    order_by.len()
                ))]));
            }
            if let Some(l) = limit {
                lines.push(OwnedRow::new(vec![Datum::Text(format!("  Limit: {l}"))]));
            }
            if let Some(o) = offset {
                lines.push(OwnedRow::new(vec![Datum::Text(format!("  Offset: {o}"))]));
            }

            // Execute and show row counts
            let result = self.exec_coordinator_join(plan, None);
            match &result {
                Ok(ExecutionResult::Query { rows, .. }) => {
                    lines.push(OwnedRow::new(vec![Datum::Text(format!(
                        "  Result rows: {}",
                        rows.len()
                    ))]));
                }
                Ok(_) => {}
                Err(e) => {
                    lines.push(OwnedRow::new(vec![Datum::Text(format!("  Error: {e}"))]));
                }
            }
        }

        Ok(ExecutionResult::Query {
            columns: vec![("QUERY PLAN".into(), DataType::Text)],
            rows: lines,
        })
    }

    /// EXPLAIN for coordinator-side subquery execution.
    pub(crate) fn exec_explain_coordinator_subquery(
        &self,
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, FalconError> {
        let mut lines: Vec<OwnedRow> = Vec::new();
        lines.push(OwnedRow::new(vec![Datum::Text(
            "CoordinatorSubquery (gather-all-then-execute)".into(),
        )]));

        let table_ids = Self::extract_table_ids(plan);
        lines.push(OwnedRow::new(vec![Datum::Text(format!(
            "  Tables: {:?} (gathered from {} shards each)",
            table_ids,
            self.engine.shard_ids().len()
        ))]));

        // Execute and show row counts
        let result = self.exec_coordinator_subquery(plan, None);
        match &result {
            Ok(ExecutionResult::Query { rows, .. }) => {
                lines.push(OwnedRow::new(vec![Datum::Text(format!(
                    "  Result rows: {}",
                    rows.len()
                ))]));
            }
            Ok(_) => {}
            Err(e) => {
                lines.push(OwnedRow::new(vec![Datum::Text(format!("  Error: {e}"))]));
            }
        }

        Ok(ExecutionResult::Query {
            columns: vec![("QUERY PLAN".into(), DataType::Text)],
            rows: lines,
        })
    }
}
