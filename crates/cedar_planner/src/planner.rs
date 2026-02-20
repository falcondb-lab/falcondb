use cedar_common::error::SqlError;
use cedar_common::types::ShardId;
use cedar_sql_frontend::types::*;

use crate::cost::{self, IndexedColumns, TableRowCounts};
use crate::plan::{DistAggMerge, DistGather, PhysicalPlan};

/// Join algorithm strategy selected by the cost-based optimizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    /// Nested loop: O(n*m), best for small right side or non-equi conditions.
    NestedLoop,
    /// Hash join: O(n+m), default for equi-joins with moderate table sizes.
    Hash,
    /// Sort-merge join: O(n*log(n) + m*log(m)), best for very large tables
    /// or when output order matches join key.
    MergeSort,
}

/// MVP planner: direct translation from bound statement to physical plan.
/// With optional cost-based join reordering when table statistics are available.
///
/// The recommended entry point is [`plan_optimized`](Self::plan_optimized), which
/// goes through the full pipeline:
///   `BoundStatement` → `LogicalPlan` → optimize → `PhysicalPlan`
///
/// The legacy [`plan`](Self::plan) method is retained for backward compatibility
/// and performs a direct `BoundStatement` → `PhysicalPlan` translation.
pub struct Planner;

impl Planner {
    pub fn plan(stmt: &BoundStatement) -> Result<PhysicalPlan, SqlError> {
        match stmt {
            BoundStatement::CreateTable(ct) => Ok(PhysicalPlan::CreateTable {
                schema: ct.schema.clone(),
                if_not_exists: ct.if_not_exists,
            }),
            BoundStatement::DropTable(dt) => Ok(PhysicalPlan::DropTable {
                table_name: dt.table_name.clone(),
                if_exists: dt.if_exists,
            }),
            BoundStatement::AlterTable(alt) => Ok(PhysicalPlan::AlterTable {
                table_name: alt.table_name.clone(),
                ops: alt.ops.clone(),
            }),
            BoundStatement::Insert(ins) => Ok(PhysicalPlan::Insert {
                table_id: ins.table_id,
                schema: ins.schema.clone(),
                columns: ins.columns.clone(),
                rows: ins.rows.clone(),
                source_select: ins.source_select.clone(),
                returning: ins.returning.clone(),
                on_conflict: ins.on_conflict.clone(),
            }),
            BoundStatement::Update(upd) => Ok(PhysicalPlan::Update {
                table_id: upd.table_id,
                schema: upd.schema.clone(),
                assignments: upd.assignments.clone(),
                filter: upd.filter.clone(),
                returning: upd.returning.clone(),
                from_table: upd.from_table.clone(),
            }),
            BoundStatement::Delete(del) => Ok(PhysicalPlan::Delete {
                table_id: del.table_id,
                schema: del.schema.clone(),
                filter: del.filter.clone(),
                returning: del.returning.clone(),
                using_table: del.using_table.clone(),
            }),
            BoundStatement::Select(sel) => {
                if sel.joins.is_empty() {
                    Ok(PhysicalPlan::SeqScan {
                        table_id: sel.table_id,
                        schema: sel.schema.clone(),
                        projections: sel.projections.clone(),
                        visible_projection_count: sel.visible_projection_count,
                        filter: sel.filter.clone(),
                        group_by: sel.group_by.clone(),
                        grouping_sets: sel.grouping_sets.clone(),
                        having: sel.having.clone(),
                        order_by: sel.order_by.clone(),
                        limit: sel.limit,
                        offset: sel.offset,
                        distinct: sel.distinct.clone(),
                        ctes: sel.ctes.clone(),
                        unions: sel.unions.clone(),
                        virtual_rows: sel.virtual_rows.clone(),
                    })
                } else {
                    // Use HashJoin for equi-joins (all joins have equality conditions)
                    let use_hash_join = sel.joins.iter().all(|j| Self::is_equi_join_condition(j.condition.as_ref()));
                    if use_hash_join {
                        Ok(PhysicalPlan::HashJoin {
                            left_table_id: sel.table_id,
                            left_schema: sel.schema.clone(),
                            joins: sel.joins.clone(),
                            combined_schema: sel.schema.clone(),
                            projections: sel.projections.clone(),
                            visible_projection_count: sel.visible_projection_count,
                            filter: sel.filter.clone(),
                            order_by: sel.order_by.clone(),
                            limit: sel.limit,
                            offset: sel.offset,
                            distinct: sel.distinct.clone(),
                            ctes: sel.ctes.clone(),
                            unions: sel.unions.clone(),
                        })
                    } else {
                        Ok(PhysicalPlan::NestedLoopJoin {
                            left_table_id: sel.table_id,
                            left_schema: sel.schema.clone(),
                            joins: sel.joins.clone(),
                            combined_schema: sel.schema.clone(),
                            projections: sel.projections.clone(),
                            visible_projection_count: sel.visible_projection_count,
                            filter: sel.filter.clone(),
                            order_by: sel.order_by.clone(),
                            limit: sel.limit,
                            offset: sel.offset,
                            distinct: sel.distinct.clone(),
                            ctes: sel.ctes.clone(),
                            unions: sel.unions.clone(),
                        })
                    }
                }
            }
            BoundStatement::Explain(inner) => {
                let inner_plan = Planner::plan(inner)?;
                Ok(PhysicalPlan::Explain(Box::new(inner_plan)))
            }
            BoundStatement::ExplainAnalyze(inner) => {
                let inner_plan = Planner::plan(inner)?;
                Ok(PhysicalPlan::ExplainAnalyze(Box::new(inner_plan)))
            }
            BoundStatement::Truncate { table_name } => Ok(PhysicalPlan::Truncate {
                table_name: table_name.clone(),
            }),
            BoundStatement::CreateIndex { index_name, table_name, column_indices, unique } => {
                Ok(PhysicalPlan::CreateIndex {
                    index_name: index_name.clone(),
                    table_name: table_name.clone(),
                    column_indices: column_indices.clone(),
                    unique: *unique,
                })
            }
            BoundStatement::DropIndex { index_name } => {
                Ok(PhysicalPlan::DropIndex {
                    index_name: index_name.clone(),
                })
            }
            BoundStatement::CreateView { name, query_sql, or_replace } => {
                Ok(PhysicalPlan::CreateView {
                    name: name.clone(),
                    query_sql: query_sql.clone(),
                    or_replace: *or_replace,
                })
            }
            BoundStatement::DropView { name, if_exists } => {
                Ok(PhysicalPlan::DropView {
                    name: name.clone(),
                    if_exists: *if_exists,
                })
            }
            BoundStatement::Begin => Ok(PhysicalPlan::Begin),
            BoundStatement::Commit => Ok(PhysicalPlan::Commit),
            BoundStatement::Rollback => Ok(PhysicalPlan::Rollback),
            BoundStatement::ShowTxnStats => Ok(PhysicalPlan::ShowTxnStats),
            BoundStatement::ShowNodeRole => Ok(PhysicalPlan::ShowNodeRole),
            BoundStatement::ShowWalStats => Ok(PhysicalPlan::ShowWalStats),
            BoundStatement::ShowConnections => Ok(PhysicalPlan::ShowConnections),
            BoundStatement::RunGc => Ok(PhysicalPlan::RunGc),
            BoundStatement::Analyze { table_name } => Ok(PhysicalPlan::Analyze { table_name: table_name.clone() }),
            BoundStatement::ShowTableStats { table_name } => Ok(PhysicalPlan::ShowTableStats { table_name: table_name.clone() }),
            BoundStatement::CreateSequence { name, start } => Ok(PhysicalPlan::CreateSequence { name: name.clone(), start: *start }),
            BoundStatement::DropSequence { name, if_exists } => Ok(PhysicalPlan::DropSequence { name: name.clone(), if_exists: *if_exists }),
            BoundStatement::ShowSequences => Ok(PhysicalPlan::ShowSequences),
            BoundStatement::CopyFrom { table_id, schema, columns, csv, delimiter, header, null_string, quote, escape } => {
                Ok(PhysicalPlan::CopyFrom {
                    table_id: *table_id, schema: schema.clone(), columns: columns.clone(),
                    csv: *csv, delimiter: *delimiter, header: *header,
                    null_string: null_string.clone(), quote: *quote, escape: *escape,
                })
            }
            BoundStatement::CopyTo { table_id, schema, columns, csv, delimiter, header, null_string, quote, escape } => {
                Ok(PhysicalPlan::CopyTo {
                    table_id: *table_id, schema: schema.clone(), columns: columns.clone(),
                    csv: *csv, delimiter: *delimiter, header: *header,
                    null_string: null_string.clone(), quote: *quote, escape: *escape,
                })
            }
            BoundStatement::CopyQueryTo { query, csv, delimiter, header, null_string, quote, escape } => {
                let inner_plan = Self::plan(&BoundStatement::Select(*query.clone()))?;
                Ok(PhysicalPlan::CopyQueryTo {
                    query: Box::new(inner_plan),
                    csv: *csv, delimiter: *delimiter, header: *header,
                    null_string: null_string.clone(), quote: *quote, escape: *escape,
                })
            }
        }
    }

    /// Plan with cost-based join reordering and index scan detection.
    /// Uses `stats` for join ordering and `indexes` to emit `IndexScan` nodes.
    /// Falls back to `plan()` for non-SELECT statements.
    pub fn plan_with_indexes(
        stmt: &BoundStatement,
        stats: &TableRowCounts,
        indexes: &IndexedColumns,
    ) -> Result<PhysicalPlan, SqlError> {
        // First, try index scan for single-table SELECTs
        if let BoundStatement::Select(sel) = stmt {
            if sel.joins.is_empty() {
                if let Some(plan) = Self::try_index_scan_plan(sel, indexes) {
                    return Ok(plan);
                }
            }
        }
        // EXPLAIN / EXPLAIN ANALYZE: recurse
        if let BoundStatement::Explain(inner) = stmt {
            let inner_plan = Self::plan_with_indexes(inner, stats, indexes)?;
            return Ok(PhysicalPlan::Explain(Box::new(inner_plan)));
        }
        if let BoundStatement::ExplainAnalyze(inner) = stmt {
            let inner_plan = Self::plan_with_indexes(inner, stats, indexes)?;
            return Ok(PhysicalPlan::ExplainAnalyze(Box::new(inner_plan)));
        }
        // Fall through to stats-based planning
        Self::plan_with_stats(stmt, stats)
    }

    /// Plan with optional cost-based join reordering using table row counts.
    /// Falls back to `plan()` for non-SELECT statements or when stats are empty.
    pub fn plan_with_stats(
        stmt: &BoundStatement,
        stats: &TableRowCounts,
    ) -> Result<PhysicalPlan, SqlError> {
        if let BoundStatement::Select(sel) = stmt {
            if !sel.joins.is_empty() && !stats.is_empty() {
                // Reorder joins first using cost estimates
                let left_col_count = sel.joins[0].right_col_offset;
                let reordered = cost::reorder_joins(left_col_count, &sel.joins, stats);
                // Build a temporary sel with reordered joins for strategy selection
                let mut reordered_sel = sel.clone();
                reordered_sel.joins = reordered;
                // Use cost-based join strategy selection
                return Ok(Self::choose_join_strategy(&reordered_sel, stats));
            }
        }
        // EXPLAIN with stats: recurse into the inner statement
        if let BoundStatement::Explain(inner) = stmt {
            let inner_plan = Self::plan_with_stats(inner, stats)?;
            return Ok(PhysicalPlan::Explain(Box::new(inner_plan)));
        }
        if let BoundStatement::ExplainAnalyze(inner) = stmt {
            let inner_plan = Self::plan_with_stats(inner, stats)?;
            return Ok(PhysicalPlan::ExplainAnalyze(Box::new(inner_plan)));
        }
        // Fallback to regular plan for non-join statements
        Self::plan(stmt)
    }

    /// Wrap a local plan in a `DistPlan` if the cluster has multiple shards.
    ///
    /// Call this after `plan()` when the session knows the cluster topology.
    /// - DDL / txn-control / EXPLAIN plans are returned unchanged.
    /// - Read-only queries (SeqScan, NestedLoopJoin) are wrapped with
    ///   `DistGather::Union` (the simplest correct strategy).
    /// - DML plans are returned unchanged — the handler should use
    ///   `TwoPhaseCoordinator` for multi-shard writes instead.
    ///
    /// `all_shards` is the full list of shard IDs in the cluster.
    pub fn wrap_distributed(plan: PhysicalPlan, all_shards: &[ShardId]) -> PhysicalPlan {
        // Single-shard clusters never need distributed execution.
        if all_shards.len() <= 1 {
            return plan;
        }

        // EXPLAIN / EXPLAIN ANALYZE: recursively wrap the inner plan.
        if let PhysicalPlan::Explain(inner) = plan {
            let wrapped_inner = Self::wrap_distributed(*inner, all_shards);
            return PhysicalPlan::Explain(Box::new(wrapped_inner));
        }
        if let PhysicalPlan::ExplainAnalyze(inner) = plan {
            let wrapped_inner = Self::wrap_distributed(*inner, all_shards);
            return PhysicalPlan::ExplainAnalyze(Box::new(wrapped_inner));
        }

        // Extract gather-strategy inputs before consuming plan.
        let gather_and_offsets = match &plan {
            PhysicalPlan::SeqScan {
                projections, group_by, grouping_sets, having, order_by, limit, offset, distinct,
                filter, ctes, unions, ..
            }
            | PhysicalPlan::IndexScan {
                projections, group_by, grouping_sets, having, order_by, limit, offset, distinct,
                filter, ctes, unions, ..
            } => {
                // If the query has cross-table subqueries (IN/EXISTS/scalar subquery),
                // CTEs, UNIONs, or GROUPING SETS, DON'T distribute — requires
                // coordinator-side execution for correctness.
                if Self::has_cross_table_subqueries(filter.as_ref(), projections, having.as_ref())
                    || !ctes.is_empty()
                    || !unions.is_empty()
                    || !grouping_sets.is_empty()
                {
                    None
                } else {
                    Some((
                        Self::choose_gather_strategy(projections, group_by, having.clone(), order_by, *limit, *offset, distinct),
                        *limit, *offset,
                    ))
                }
            }
            // NestedLoopJoin: NOT wrapped in DistPlan — requires coordinator-side join
            // to ensure cross-shard correctness. The query engine handles this specially.
            PhysicalPlan::NestedLoopJoin { .. } | PhysicalPlan::HashJoin { .. } | PhysicalPlan::MergeSortJoin { .. } => None,
            _ => None,
        };

        match gather_and_offsets {
            Some(((gather, rewritten_projs), limit, offset)) => {
                let mut subplan = Self::adjust_subplan_for_offset(plan, limit, offset);
                // Strip HAVING from subplan when using TwoPhaseAgg (applied post-merge)
                if matches!(gather, DistGather::TwoPhaseAgg { .. }) {
                    Self::strip_having(&mut subplan);
                }
                // Apply rewritten projections (e.g. AVG→SUM + hidden COUNT)
                if let Some(new_projs) = rewritten_projs {
                    match &mut subplan {
                        PhysicalPlan::SeqScan { projections, .. }
                        | PhysicalPlan::IndexScan { projections, .. } => {
                            *projections = new_projs;
                        }
                        _ => {}
                    }
                }
                PhysicalPlan::DistPlan {
                    subplan: Box::new(subplan),
                    target_shards: all_shards.to_vec(),
                    gather,
                }
            }
            // DDL, DML, txn control, EXPLAIN, etc. — stay local.
            None => plan,
        }
    }

    /// Choose the optimal gather strategy based on plan properties.
    ///
    /// Priority:
    /// 1. Aggregate-only queries (COUNT/SUM/MIN/MAX) → TwoPhaseAgg
    /// 2. ORDER BY + optional LIMIT → MergeSortLimit
    /// 3. Otherwise → Union
    fn choose_gather_strategy(
        projections: &[BoundProjection],
        group_by: &[usize],
        having: Option<BoundExpr>,
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: &DistinctMode,
    ) -> (DistGather, Option<Vec<BoundProjection>>) {
        // Check for aggregate-only projections (all projections are aggregates or group-by columns)
        if let Some(result) = Self::try_two_phase_agg(projections, group_by, having, order_by, limit, offset) {
            return result;
        }

        let gather = if !order_by.is_empty() {
            let sort_columns: Vec<(usize, bool)> = order_by
                .iter()
                .map(|ob| (ob.column_idx, ob.asc))
                .collect();
            DistGather::MergeSortLimit {
                sort_columns,
                limit,
                offset,
            }
        } else {
            DistGather::Union { distinct: !matches!(distinct, DistinctMode::None), limit, offset }
        };
        (gather, None)
    }

    /// Adjust the subplan for distributed execution: strip OFFSET (applied at
    /// gather), and push `limit + offset` so each shard returns enough rows.
    fn adjust_subplan_for_offset(
        mut plan: PhysicalPlan,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> PhysicalPlan {
        if offset.is_none() {
            return plan;
        }
        let combined = match (limit, offset) {
            (Some(l), Some(o)) => Some(l + o),
            (None, Some(_)) => None, // no limit, keep all rows
            _ => limit,
        };
        match &mut plan {
            PhysicalPlan::SeqScan { limit: lim, offset: off, .. }
            | PhysicalPlan::IndexScan { limit: lim, offset: off, .. } => {
                *off = None;
                *lim = combined;
            }
            PhysicalPlan::NestedLoopJoin { limit: lim, offset: off, .. }
            | PhysicalPlan::HashJoin { limit: lim, offset: off, .. }
            | PhysicalPlan::MergeSortJoin { limit: lim, offset: off, .. } => {
                *off = None;
                *lim = combined;
            }
            _ => {}
        }
        plan
    }

    /// Check if a SeqScan has subqueries in its filter, projections, or HAVING
    /// that reference other tables. Such queries need coordinator-side execution.
    fn has_cross_table_subqueries(
        filter: Option<&BoundExpr>,
        projections: &[BoundProjection],
        having: Option<&BoundExpr>,
    ) -> bool {
        if let Some(f) = filter {
            if Self::expr_has_subquery(f) {
                return true;
            }
        }
        if let Some(h) = having {
            if Self::expr_has_subquery(h) {
                return true;
            }
        }
        for proj in projections {
            match proj {
                BoundProjection::Expr(expr, _) | BoundProjection::Aggregate(_, Some(expr), _, _, _) => {
                    if Self::expr_has_subquery(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    /// Recursively check if a BoundExpr contains any subquery node.
    fn expr_has_subquery(expr: &BoundExpr) -> bool {
        match expr {
            BoundExpr::ScalarSubquery(_) | BoundExpr::InSubquery { .. } | BoundExpr::Exists { .. } => true,
            BoundExpr::BinaryOp { left, right, .. } => {
                Self::expr_has_subquery(left) || Self::expr_has_subquery(right)
            }
            BoundExpr::Not(inner) | BoundExpr::IsNull(inner) | BoundExpr::IsNotNull(inner) => {
                Self::expr_has_subquery(inner)
            }
            BoundExpr::Like { expr, pattern, .. } => {
                Self::expr_has_subquery(expr) || Self::expr_has_subquery(pattern)
            }
            BoundExpr::Between { expr, low, high, .. } => {
                Self::expr_has_subquery(expr) || Self::expr_has_subquery(low) || Self::expr_has_subquery(high)
            }
            BoundExpr::Case { operand, conditions, results, else_result } => {
                if let Some(op) = operand { if Self::expr_has_subquery(op) { return true; } }
                for cond in conditions { if Self::expr_has_subquery(cond) { return true; } }
                for res in results { if Self::expr_has_subquery(res) { return true; } }
                if let Some(el) = else_result { if Self::expr_has_subquery(el) { return true; } }
                false
            }
            BoundExpr::InList { expr, list, .. } => {
                Self::expr_has_subquery(expr) || list.iter().any(Self::expr_has_subquery)
            }
            BoundExpr::Function { args, .. } => args.iter().any(Self::expr_has_subquery),
            BoundExpr::Cast { expr: inner, .. } => Self::expr_has_subquery(inner),
            BoundExpr::Coalesce(exprs) => exprs.iter().any(Self::expr_has_subquery),
            BoundExpr::AggregateExpr { arg: Some(inner), .. } => Self::expr_has_subquery(inner),
            BoundExpr::ArrayLiteral(exprs) => exprs.iter().any(Self::expr_has_subquery),
            BoundExpr::ArrayIndex { array, index } => {
                Self::expr_has_subquery(array) || Self::expr_has_subquery(index)
            }
            BoundExpr::IsNotDistinctFrom { left, right } => {
                Self::expr_has_subquery(left) || Self::expr_has_subquery(right)
            }
            BoundExpr::AnyOp { left, right, .. } | BoundExpr::AllOp { left, right, .. } => {
                Self::expr_has_subquery(left) || Self::expr_has_subquery(right)
            }
            BoundExpr::ArraySlice { array, lower, upper } => {
                Self::expr_has_subquery(array)
                    || lower.as_deref().is_some_and(Self::expr_has_subquery)
                    || upper.as_deref().is_some_and(Self::expr_has_subquery)
            }
            // Leaf nodes: no subqueries
            BoundExpr::ColumnRef(_) | BoundExpr::Literal(_) | BoundExpr::OuterColumnRef(_) | BoundExpr::AggregateExpr { arg: None, .. }
            | BoundExpr::SequenceNextval(_) | BoundExpr::SequenceCurrval(_) | BoundExpr::SequenceSetval(_, _)
            | BoundExpr::Parameter(_) | BoundExpr::Grouping(_) => false,
        }
    }

    /// Strip HAVING from a SeqScan subplan (applied post-merge in TwoPhaseAgg).
    fn strip_having(plan: &mut PhysicalPlan) {
        if let PhysicalPlan::SeqScan { having, .. } = plan {
            *having = None;
        }
    }

    fn try_two_phase_agg(
        projections: &[BoundProjection],
        group_by: &[usize],
        having: Option<BoundExpr>,
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Option<(DistGather, Option<Vec<BoundProjection>>)> {
        if projections.is_empty() {
            return None;
        }

        let mut agg_merges = Vec::new();
        let mut has_agg = false;
        let mut output_idx = group_by.len(); // aggregates come after group-by columns
        let visible_columns = group_by.len() + projections.iter().filter(|p| matches!(p, BoundProjection::Aggregate(..))).count();

        // Track AVG decomposition: (sum_output_idx, hidden_count_idx)
        let mut avg_fixups: Vec<(usize, usize)> = Vec::new();
        // Hidden COUNT projections appended after visible columns for AVG decomposition
        let mut hidden_projections: Vec<BoundProjection> = Vec::new();
        let mut hidden_idx = visible_columns;

        for proj in projections {
            match proj {
                BoundProjection::Aggregate(func, _arg, _alias, _distinct, _filter) => {
                    // DISTINCT aggregate handling for distributed two-phase:
                    // COUNT/SUM/AVG(DISTINCT): rewrite to ARRAY_AGG(DISTINCT), collect-dedup at gather.
                    // MIN/MAX/BOOL_AND/BOOL_OR(DISTINCT): DISTINCT is a no-op, use regular merge.
                    if *_distinct && _arg.is_some() {
                        let merge = match func {
                            AggFunc::Count => DistAggMerge::CountDistinct(output_idx),
                            AggFunc::Sum => DistAggMerge::SumDistinct(output_idx),
                            AggFunc::Avg => DistAggMerge::AvgDistinct(output_idx),
                            // MIN/MAX/BOOL_AND/BOOL_OR(DISTINCT) ≡ non-distinct — no-op
                            AggFunc::Min => DistAggMerge::Min(output_idx),
                            AggFunc::Max => DistAggMerge::Max(output_idx),
                            AggFunc::BoolAnd => DistAggMerge::BoolAnd(output_idx),
                            AggFunc::BoolOr => DistAggMerge::BoolOr(output_idx),
                            AggFunc::StringAgg(ref sep) => DistAggMerge::StringAggDistinct(output_idx, sep.clone()),
                            AggFunc::ArrayAgg => DistAggMerge::ArrayAggDistinct(output_idx),
                            // Statistical/ordered-set/bit aggregates: fall back to non-distributed
                            _ => return None,
                        };
                        agg_merges.push(merge);
                        has_agg = true;
                        output_idx += 1;
                        continue;
                    }
                    // Non-DISTINCT aggregates with distinct=true but no arg (e.g. COUNT(*) DISTINCT)
                    if *_distinct {
                        return None;
                    }
                    let merge = match func {
                        AggFunc::Count => DistAggMerge::Count(output_idx),
                        AggFunc::Sum => DistAggMerge::Sum(output_idx),
                        AggFunc::Min => DistAggMerge::Min(output_idx),
                        AggFunc::Max => DistAggMerge::Max(output_idx),
                        AggFunc::BoolAnd => DistAggMerge::BoolAnd(output_idx),
                        AggFunc::BoolOr => DistAggMerge::BoolOr(output_idx),
                        AggFunc::StringAgg(ref sep) => DistAggMerge::StringAgg(output_idx, sep.clone()),
                        AggFunc::ArrayAgg => DistAggMerge::ArrayAgg(output_idx),
                        AggFunc::Avg => {
                            // Decompose AVG(col) into SUM(col) at this position
                            // + hidden COUNT(col) appended at end
                            agg_merges.push(DistAggMerge::Sum(output_idx));
                            // Add hidden COUNT projection
                            hidden_projections.push(BoundProjection::Aggregate(
                                AggFunc::Count,
                                _arg.clone(),
                                "__avg_count".into(),
                                false,
                                _filter.clone(),
                            ));
                            agg_merges.push(DistAggMerge::Count(hidden_idx));
                            avg_fixups.push((output_idx, hidden_idx));
                            hidden_idx += 1;
                            has_agg = true;
                            output_idx += 1;
                            continue;
                        }
                        // Statistical/ordered-set/bit aggregates: fall back to non-distributed
                        _ => return None,
                    };
                    agg_merges.push(merge);
                    has_agg = true;
                    output_idx += 1;
                }
                BoundProjection::Column(_, _) => {
                    // Column refs are fine alongside aggregates (for GROUP BY)
                }
                _ => {
                    // Expressions or windows — not supported for 2PA
                    return None;
                }
            }
        }

        if !has_agg {
            return None;
        }

        // group_by_indices must be OUTPUT column positions (0..N),
        // not the original table column indices, because the executor
        // places group-by columns first in the output row.
        let group_by_output_indices: Vec<usize> = (0..group_by.len()).collect();

        // Rewrite HAVING: replace AggregateExpr nodes with ColumnRef pointing
        // to the merged output column positions so eval_filter can evaluate them.
        let rewritten_having = having.map(|expr| {
            let mut agg_output_map: Vec<(AggFunc, Option<usize>, usize)> = Vec::new();
            let mut idx = group_by.len();
            for proj in projections {
                if let BoundProjection::Aggregate(func, arg, _, _, _) = proj {
                    let arg_col = arg.as_ref().and_then(|e| {
                        if let BoundExpr::ColumnRef(c) = e { Some(*c) } else { None }
                    });
                    agg_output_map.push((func.clone(), arg_col, idx));
                    idx += 1;
                }
            }
            Self::rewrite_agg_refs(expr, &agg_output_map)
        });

        // Check if any subplan projection rewriting is needed
        let has_distinct_agg = agg_merges.iter().any(|m| matches!(m, DistAggMerge::CountDistinct(_) | DistAggMerge::SumDistinct(_) | DistAggMerge::AvgDistinct(_) | DistAggMerge::StringAggDistinct(_, _) | DistAggMerge::ArrayAggDistinct(_)));
        let needs_rewrite = !avg_fixups.is_empty() || has_distinct_agg;

        let rewritten_projs = if needs_rewrite {
            let mut new_projs: Vec<BoundProjection> = projections.iter().map(|p| {
                match p {
                    BoundProjection::Aggregate(AggFunc::Avg, arg, alias, true, filter) if arg.is_some() => {
                        // Replace AVG(DISTINCT col) with ARRAY_AGG(DISTINCT col)
                        BoundProjection::Aggregate(AggFunc::ArrayAgg, arg.clone(), alias.clone(), true, filter.clone())
                    }
                    BoundProjection::Aggregate(AggFunc::Avg, arg, alias, distinct, filter) => {
                        // Replace AVG with SUM for the subplan (non-distinct decomposition)
                        BoundProjection::Aggregate(AggFunc::Sum, arg.clone(), alias.clone(), *distinct, filter.clone())
                    }
                    BoundProjection::Aggregate(AggFunc::Count, arg, alias, true, filter) if arg.is_some() => {
                        // Replace COUNT(DISTINCT col) with ARRAY_AGG(DISTINCT col)
                        BoundProjection::Aggregate(AggFunc::ArrayAgg, arg.clone(), alias.clone(), true, filter.clone())
                    }
                    BoundProjection::Aggregate(AggFunc::Sum, arg, alias, true, filter) if arg.is_some() => {
                        // Replace SUM(DISTINCT col) with ARRAY_AGG(DISTINCT col)
                        BoundProjection::Aggregate(AggFunc::ArrayAgg, arg.clone(), alias.clone(), true, filter.clone())
                    }
                    BoundProjection::Aggregate(AggFunc::StringAgg(_), arg, alias, true, filter) if arg.is_some() => {
                        // Replace STRING_AGG(DISTINCT col, sep) with ARRAY_AGG(DISTINCT col)
                        BoundProjection::Aggregate(AggFunc::ArrayAgg, arg.clone(), alias.clone(), true, filter.clone())
                    }
                    _ => p.clone(),
                }
            }).collect();
            // Append hidden COUNT projections for AVG decomposition
            new_projs.extend(hidden_projections);
            Some(new_projs)
        } else {
            None
        };

        let sort_columns: Vec<(usize, bool)> = order_by
            .iter()
            .map(|ob| (ob.column_idx, ob.asc))
            .collect();

        Some((DistGather::TwoPhaseAgg {
            group_by_indices: group_by_output_indices,
            agg_merges,
            having: rewritten_having,
            avg_fixups,
            visible_columns,
            order_by: sort_columns,
            limit,
            offset,
        }, rewritten_projs))
    }

    /// Recursively rewrite AggregateExpr nodes in an expression to ColumnRef
    /// pointing to the merged output column positions.
    fn rewrite_agg_refs(
        expr: BoundExpr,
        agg_map: &[(AggFunc, Option<usize>, usize)],
    ) -> BoundExpr {
        match expr {
            BoundExpr::AggregateExpr { ref func, ref arg, .. } => {
                let arg_col = arg.as_ref().and_then(|e| {
                    if let BoundExpr::ColumnRef(c) = e.as_ref() { Some(*c) } else { None }
                });
                for (af, ac, out_idx) in agg_map {
                    if std::mem::discriminant(af) == std::mem::discriminant(func)
                        && *ac == arg_col
                    {
                        return BoundExpr::ColumnRef(*out_idx);
                    }
                }
                expr
            }
            BoundExpr::BinaryOp { left, op, right } => {
                BoundExpr::BinaryOp {
                    left: Box::new(Self::rewrite_agg_refs(*left, agg_map)),
                    op,
                    right: Box::new(Self::rewrite_agg_refs(*right, agg_map)),
                }
            }
            BoundExpr::Not(inner) => {
                BoundExpr::Not(Box::new(Self::rewrite_agg_refs(*inner, agg_map)))
            }
            BoundExpr::IsNull(inner) => {
                BoundExpr::IsNull(Box::new(Self::rewrite_agg_refs(*inner, agg_map)))
            }
            BoundExpr::IsNotNull(inner) => {
                BoundExpr::IsNotNull(Box::new(Self::rewrite_agg_refs(*inner, agg_map)))
            }
            BoundExpr::Cast { expr: inner, target_type } => BoundExpr::Cast {
                expr: Box::new(Self::rewrite_agg_refs(*inner, agg_map)),
                target_type,
            },
            BoundExpr::Function { func, args } => BoundExpr::Function {
                func,
                args: args.into_iter().map(|a| Self::rewrite_agg_refs(a, agg_map)).collect(),
            },
            BoundExpr::Coalesce(args) => BoundExpr::Coalesce(
                args.into_iter().map(|a| Self::rewrite_agg_refs(a, agg_map)).collect(),
            ),
            BoundExpr::Case { operand, conditions, results, else_result } => BoundExpr::Case {
                operand: operand.map(|e| Box::new(Self::rewrite_agg_refs(*e, agg_map))),
                conditions: conditions.into_iter().map(|c| Self::rewrite_agg_refs(c, agg_map)).collect(),
                results: results.into_iter().map(|r| Self::rewrite_agg_refs(r, agg_map)).collect(),
                else_result: else_result.map(|e| Box::new(Self::rewrite_agg_refs(*e, agg_map))),
            },
            BoundExpr::Like { expr: inner, pattern, negated, case_insensitive } => BoundExpr::Like {
                expr: Box::new(Self::rewrite_agg_refs(*inner, agg_map)),
                pattern: Box::new(Self::rewrite_agg_refs(*pattern, agg_map)),
                negated,
                case_insensitive,
            },
            BoundExpr::InList { expr: inner, list, negated } => BoundExpr::InList {
                expr: Box::new(Self::rewrite_agg_refs(*inner, agg_map)),
                list: list.into_iter().map(|l| Self::rewrite_agg_refs(l, agg_map)).collect(),
                negated,
            },
            BoundExpr::Between { expr: inner, low, high, negated } => BoundExpr::Between {
                expr: Box::new(Self::rewrite_agg_refs(*inner, agg_map)),
                low: Box::new(Self::rewrite_agg_refs(*low, agg_map)),
                high: Box::new(Self::rewrite_agg_refs(*high, agg_map)),
                negated,
            },
            BoundExpr::IsNotDistinctFrom { left, right } => BoundExpr::IsNotDistinctFrom {
                left: Box::new(Self::rewrite_agg_refs(*left, agg_map)),
                right: Box::new(Self::rewrite_agg_refs(*right, agg_map)),
            },
            BoundExpr::ArrayLiteral(elems) => BoundExpr::ArrayLiteral(
                elems.into_iter().map(|e| Self::rewrite_agg_refs(e, agg_map)).collect(),
            ),
            BoundExpr::ArrayIndex { array, index } => BoundExpr::ArrayIndex {
                array: Box::new(Self::rewrite_agg_refs(*array, agg_map)),
                index: Box::new(Self::rewrite_agg_refs(*index, agg_map)),
            },
            BoundExpr::AnyOp { left, compare_op, right } => BoundExpr::AnyOp {
                left: Box::new(Self::rewrite_agg_refs(*left, agg_map)),
                compare_op,
                right: Box::new(Self::rewrite_agg_refs(*right, agg_map)),
            },
            BoundExpr::AllOp { left, compare_op, right } => BoundExpr::AllOp {
                left: Box::new(Self::rewrite_agg_refs(*left, agg_map)),
                compare_op,
                right: Box::new(Self::rewrite_agg_refs(*right, agg_map)),
            },
            BoundExpr::ArraySlice { array, lower, upper } => BoundExpr::ArraySlice {
                array: Box::new(Self::rewrite_agg_refs(*array, agg_map)),
                lower: lower.map(|l| Box::new(Self::rewrite_agg_refs(*l, agg_map))),
                upper: upper.map(|u| Box::new(Self::rewrite_agg_refs(*u, agg_map))),
            },
            other => other,
        }
    }

    /// Check if a join condition is an equi-join (simple equality between columns).
    /// Returns true for `col_a = col_b` or `col_a = col_b AND col_c = col_d` patterns.
    fn is_equi_join_condition(condition: Option<&BoundExpr>) -> bool {
        match condition {
            None => false, // CROSS JOIN — no condition, use nested loop
            Some(expr) => Self::is_equi_expr(expr),
        }
    }

    fn is_equi_expr(expr: &BoundExpr) -> bool {
        match expr {
            BoundExpr::BinaryOp { op: BinOp::Eq, left, right } => {
                matches!(left.as_ref(), BoundExpr::ColumnRef(_))
                    && matches!(right.as_ref(), BoundExpr::ColumnRef(_))
            }
            BoundExpr::BinaryOp { op: BinOp::And, left, right } => {
                Self::is_equi_expr(left) && Self::is_equi_expr(right)
            }
            _ => false,
        }
    }

    /// Choose the best join strategy based on table statistics.
    ///
    /// Decision matrix:
    /// - **Nested Loop**: small right side (< 100 rows) OR non-equi condition
    /// - **Hash Join**: equi-join with moderate right side (default choice)
    /// - **Merge Sort Join**: both sides large (> 10K rows) AND equi-join,
    ///   OR when ORDER BY matches join key (avoids post-join sort)
    ///
    /// Cost model (simplified):
    ///   NL cost  = left_rows * right_rows
    ///   Hash cost = left_rows + right_rows + build_cost(min(left, right))
    ///   Sort cost = (left_rows * log(left_rows) + right_rows * log(right_rows)) + merge_cost
    fn choose_join_strategy(
        sel: &BoundSelect,
        stats: &TableRowCounts,
    ) -> PhysicalPlan {
        let left_rows = stats.get(&sel.table_id).copied().unwrap_or(1000);
        let all_equi = sel.joins.iter().all(|j| Self::is_equi_join_condition(j.condition.as_ref()));

        // Estimate total right-side rows
        let right_rows_sum: u64 = sel.joins.iter()
            .map(|j| stats.get(&j.right_table_id).copied().unwrap_or(1000))
            .sum();

        // Check if ORDER BY matches any join key (merge join can avoid sort)
        let order_by_matches_join_key = !sel.order_by.is_empty() && sel.joins.iter().any(|j| {
            if let Some(ref cond) = j.condition {
                Self::order_by_matches_join_condition(&sel.order_by, cond)
            } else {
                false
            }
        });

        let strategy = if !all_equi {
            // Non-equi join → must use NL
            JoinStrategy::NestedLoop
        } else if right_rows_sum < 100 {
            // Small right side → NL is fine (no hash table overhead)
            JoinStrategy::NestedLoop
        } else if left_rows > 10_000 && right_rows_sum > 10_000 && order_by_matches_join_key {
            // Both sides large + ORDER BY matches join key → merge sort avoids post-sort
            JoinStrategy::MergeSort
        } else if left_rows > 50_000 && right_rows_sum > 50_000 {
            // Both sides very large → merge sort to avoid huge hash table
            JoinStrategy::MergeSort
        } else {
            // Default: hash join
            JoinStrategy::Hash
        };

        match strategy {
            JoinStrategy::NestedLoop => PhysicalPlan::NestedLoopJoin {
                left_table_id: sel.table_id,
                left_schema: sel.schema.clone(),
                joins: sel.joins.clone(),
                combined_schema: sel.schema.clone(),
                projections: sel.projections.clone(),
                visible_projection_count: sel.visible_projection_count,
                filter: sel.filter.clone(),
                order_by: sel.order_by.clone(),
                limit: sel.limit,
                offset: sel.offset,
                distinct: sel.distinct.clone(),
                ctes: sel.ctes.clone(),
                unions: sel.unions.clone(),
            },
            JoinStrategy::Hash => PhysicalPlan::HashJoin {
                left_table_id: sel.table_id,
                left_schema: sel.schema.clone(),
                joins: sel.joins.clone(),
                combined_schema: sel.schema.clone(),
                projections: sel.projections.clone(),
                visible_projection_count: sel.visible_projection_count,
                filter: sel.filter.clone(),
                order_by: sel.order_by.clone(),
                limit: sel.limit,
                offset: sel.offset,
                distinct: sel.distinct.clone(),
                ctes: sel.ctes.clone(),
                unions: sel.unions.clone(),
            },
            JoinStrategy::MergeSort => PhysicalPlan::MergeSortJoin {
                left_table_id: sel.table_id,
                left_schema: sel.schema.clone(),
                joins: sel.joins.clone(),
                combined_schema: sel.schema.clone(),
                projections: sel.projections.clone(),
                visible_projection_count: sel.visible_projection_count,
                filter: sel.filter.clone(),
                order_by: sel.order_by.clone(),
                limit: sel.limit,
                offset: sel.offset,
                distinct: sel.distinct.clone(),
                ctes: sel.ctes.clone(),
                unions: sel.unions.clone(),
            },
        }
    }

    /// Check if ORDER BY columns match a join condition's key columns.
    fn order_by_matches_join_condition(order_by: &[BoundOrderBy], condition: &BoundExpr) -> bool {
        // Extract equi-join column pairs from the condition
        let key_cols = Self::extract_equi_columns(condition);
        if key_cols.is_empty() {
            return false;
        }
        // Check if the first ORDER BY column matches any join key column
        if let Some(first_ob) = order_by.first() {
            key_cols.iter().any(|(l, r)| first_ob.column_idx == *l || first_ob.column_idx == *r)
        } else {
            false
        }
    }

    /// Extract (left_col, right_col) pairs from equi-join conditions.
    fn extract_equi_columns(expr: &BoundExpr) -> Vec<(usize, usize)> {
        let mut pairs = Vec::new();
        match expr {
            BoundExpr::BinaryOp { left, op: BinOp::Eq, right } => {
                if let (BoundExpr::ColumnRef(l), BoundExpr::ColumnRef(r)) = (left.as_ref(), right.as_ref()) {
                    pairs.push((*l, *r));
                }
            }
            BoundExpr::BinaryOp { left, op: BinOp::And, right } => {
                pairs.extend(Self::extract_equi_columns(left));
                pairs.extend(Self::extract_equi_columns(right));
            }
            _ => {}
        }
        pairs
    }

    /// Try to produce an IndexScan plan for a single-table SELECT when the
    /// filter contains a `col = literal` predicate on an indexed column.
    fn try_index_scan_plan(
        sel: &BoundSelect,
        indexes: &IndexedColumns,
    ) -> Option<PhysicalPlan> {
        let filter = sel.filter.as_ref()?;
        let indexed_cols = indexes.get(&sel.table_id)?;
        if indexed_cols.is_empty() {
            return None;
        }

        let (col_idx, value, residual) = Self::try_extract_index_predicate(filter, indexed_cols)?;

        Some(PhysicalPlan::IndexScan {
            table_id: sel.table_id,
            schema: sel.schema.clone(),
            index_col: col_idx,
            index_value: value,
            projections: sel.projections.clone(),
            visible_projection_count: sel.visible_projection_count,
            filter: residual,
            group_by: sel.group_by.clone(),
            grouping_sets: sel.grouping_sets.clone(),
            having: sel.having.clone(),
            order_by: sel.order_by.clone(),
            limit: sel.limit,
            offset: sel.offset,
            distinct: sel.distinct.clone(),
            ctes: sel.ctes.clone(),
            unions: sel.unions.clone(),
            virtual_rows: sel.virtual_rows.clone(),
        })
    }

    /// Extract a `col = literal` predicate from a filter expression that matches
    /// one of the indexed columns. Returns (column_idx, literal_expr, residual_filter).
    fn try_extract_index_predicate(
        filter: &BoundExpr,
        indexed_cols: &[usize],
    ) -> Option<(usize, BoundExpr, Option<BoundExpr>)> {
        match filter {
            // Simple: col = literal
            BoundExpr::BinaryOp { left, op: BinOp::Eq, right } => {
                let (col_idx, lit) = match (left.as_ref(), right.as_ref()) {
                    (BoundExpr::ColumnRef(idx), BoundExpr::Literal(_)) => (*idx, right.as_ref()),
                    (BoundExpr::Literal(_), BoundExpr::ColumnRef(idx)) => (*idx, left.as_ref()),
                    _ => return None,
                };
                if indexed_cols.contains(&col_idx) {
                    Some((col_idx, lit.clone(), None))
                } else {
                    None
                }
            }
            // AND: try to extract from either side
            BoundExpr::BinaryOp { left, op: BinOp::And, right } => {
                // Try left side first
                if let BoundExpr::BinaryOp { left: ll, op: BinOp::Eq, right: lr } = left.as_ref() {
                    let extracted = match (ll.as_ref(), lr.as_ref()) {
                        (BoundExpr::ColumnRef(idx), BoundExpr::Literal(_)) => Some((*idx, lr.as_ref())),
                        (BoundExpr::Literal(_), BoundExpr::ColumnRef(idx)) => Some((*idx, ll.as_ref())),
                        _ => None,
                    };
                    if let Some((col_idx, lit)) = extracted {
                        if indexed_cols.contains(&col_idx) {
                            return Some((col_idx, lit.clone(), Some(*right.clone())));
                        }
                    }
                }
                // Try right side
                if let BoundExpr::BinaryOp { left: rl, op: BinOp::Eq, right: rr } = right.as_ref() {
                    let extracted = match (rl.as_ref(), rr.as_ref()) {
                        (BoundExpr::ColumnRef(idx), BoundExpr::Literal(_)) => Some((*idx, rr.as_ref())),
                        (BoundExpr::Literal(_), BoundExpr::ColumnRef(idx)) => Some((*idx, rl.as_ref())),
                        _ => None,
                    };
                    if let Some((col_idx, lit)) = extracted {
                        if indexed_cols.contains(&col_idx) {
                            return Some((col_idx, lit.clone(), Some(*left.clone())));
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }
}
