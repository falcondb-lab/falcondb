//! Rule-based optimizer for `LogicalPlan`.
//!
//! Each rule is a function `LogicalPlan 鈫?LogicalPlan` applied top-down.
//! The optimizer runs a fixed set of rules in order:
//!
//! 1. **Predicate pushdown** 鈥?push Filter below Join / Project.
//! 2. **Projection pruning** 鈥?remove unused columns (placeholder).
//! 3. **Join reordering** 鈥?reorder multi-way joins using cost estimates.
//! 4. **Limit pushdown** 鈥?push Limit into scan when no sort/agg.

use std::collections::HashSet;

use crate::cost::{IndexedColumns, TableRowCounts};
use crate::logical_plan::LogicalPlan;
use falcon_sql_frontend::types::*;

/// Optimizer configuration 鈥?controls which rules are enabled.
#[derive(Debug, Clone)]
pub struct OptimizerConfig {
    pub predicate_pushdown: bool,
    pub projection_pruning: bool,
    pub join_reorder: bool,
    pub limit_pushdown: bool,
    pub subquery_decorrelation: bool,
    pub join_strategy_selection: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            predicate_pushdown: true,
            projection_pruning: true,
            join_reorder: true,
            limit_pushdown: true,
            subquery_decorrelation: true,
            join_strategy_selection: true,
        }
    }
}

/// Context passed to optimizer rules (statistics, indexes, etc.).
pub struct OptimizerContext<'a> {
    pub stats: &'a TableRowCounts,
    pub indexes: &'a IndexedColumns,
    pub config: &'a OptimizerConfig,
}

/// Apply all enabled optimization rules to a `LogicalPlan`.
pub fn optimize(
    plan: LogicalPlan,
    ctx: &OptimizerContext<'_>,
) -> LogicalPlan {
    let mut plan = plan;

    if ctx.config.predicate_pushdown {
        plan = rule_predicate_pushdown(plan);
    }
    if ctx.config.projection_pruning {
        plan = rule_projection_pruning(plan);
    }
    if ctx.config.subquery_decorrelation {
        plan = rule_subquery_decorrelation(plan);
    }
    if ctx.config.join_reorder && !ctx.stats.is_empty() {
        plan = rule_join_reorder(plan, ctx.stats);
    }
    if ctx.config.limit_pushdown {
        plan = rule_limit_pushdown(plan);
    }

    plan
}

// 鈹€鈹€ Rule 1: Predicate Pushdown 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

/// Push `Filter` nodes as close to the data source as possible.
///
/// Transformations applied:
/// - Filter(Project(input)) 鈫?Project(Filter(input))         (if predicate
///   only references columns available in `input`)
/// - Filter(MultiJoin(base, joins)) 鈫?split conjuncts, push
///   single-table predicates into the base scan or individual join filters.
fn rule_predicate_pushdown(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        // Push filter below projection when predicate doesn't reference
        // computed expressions (only ColumnRefs that exist in the input).
        LogicalPlan::Filter {
            input,
            predicate,
        } => {
            let inner = rule_predicate_pushdown(*input);
            match inner {
                LogicalPlan::Project { input: proj_input, projections, visible_count } => {
                    if predicate_uses_only_column_refs(&predicate) {
                        // Safe to push below projection
                        let pushed = LogicalPlan::Filter {
                            input: proj_input,
                            predicate,
                        };
                        LogicalPlan::Project {
                            input: Box::new(rule_predicate_pushdown(pushed)),
                            projections,
                            visible_count,
                        }
                    } else {
                        LogicalPlan::Filter {
                            input: Box::new(LogicalPlan::Project {
                                input: proj_input,
                                projections,
                                visible_count,
                            }),
                            predicate,
                        }
                    }
                }
                // Push single-table predicates into the join's base scan
                LogicalPlan::MultiJoin { base, joins } => {
                    let conjuncts = split_conjuncts(predicate);
                    let mut pushed_to_base = Vec::new();
                    let mut remaining = Vec::new();

                    // Determine max column index in the base scan
                    let base_col_count = joins.first()
                        .map(|j| j.right_col_offset)
                        .unwrap_or(usize::MAX);

                    for conj in conjuncts {
                        if max_column_ref(&conj) < base_col_count {
                            pushed_to_base.push(conj);
                        } else {
                            remaining.push(conj);
                        }
                    }

                    let mut new_base = *base;
                    for pred in pushed_to_base {
                        new_base = LogicalPlan::Filter {
                            input: Box::new(new_base),
                            predicate: pred,
                        };
                    }

                    let join_plan = LogicalPlan::MultiJoin {
                        base: Box::new(rule_predicate_pushdown(new_base)),
                        joins,
                    };

                    if remaining.is_empty() {
                        join_plan
                    } else {
                        LogicalPlan::Filter {
                            input: Box::new(join_plan),
                            predicate: combine_conjuncts(remaining),
                        }
                    }
                }
                other => LogicalPlan::Filter {
                    input: Box::new(other),
                    predicate,
                },
            }
        }
        // Recurse into all other node types
        LogicalPlan::Project { input, projections, visible_count } => LogicalPlan::Project {
            input: Box::new(rule_predicate_pushdown(*input)),
            projections,
            visible_count,
        },
        LogicalPlan::Aggregate { input, group_by, grouping_sets, projections, visible_count, having } => {
            LogicalPlan::Aggregate {
                input: Box::new(rule_predicate_pushdown(*input)),
                group_by,
                grouping_sets,
                projections,
                visible_count,
                having,
            }
        }
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(rule_predicate_pushdown(*input)),
            order_by,
        },
        LogicalPlan::Limit { input, limit, offset } => LogicalPlan::Limit {
            input: Box::new(rule_predicate_pushdown(*input)),
            limit,
            offset,
        },
        LogicalPlan::Distinct { input, mode } => LogicalPlan::Distinct {
            input: Box::new(rule_predicate_pushdown(*input)),
            mode,
        },
        LogicalPlan::MultiJoin { base, joins } => LogicalPlan::MultiJoin {
            base: Box::new(rule_predicate_pushdown(*base)),
            joins,
        },
        LogicalPlan::Join { left, right, join_info } => LogicalPlan::Join {
            left: Box::new(rule_predicate_pushdown(*left)),
            right: Box::new(rule_predicate_pushdown(*right)),
            join_info,
        },
        LogicalPlan::SetOp { left, right, kind, all } => LogicalPlan::SetOp {
            left: Box::new(rule_predicate_pushdown(*left)),
            right: Box::new(rule_predicate_pushdown(*right)),
            kind, all,
        },
        LogicalPlan::WithCtes { ctes, input } => LogicalPlan::WithCtes {
            ctes,
            input: Box::new(rule_predicate_pushdown(*input)),
        },
        LogicalPlan::Explain(inner) => LogicalPlan::Explain(
            Box::new(rule_predicate_pushdown(*inner)),
        ),
        LogicalPlan::ExplainAnalyze(inner) => LogicalPlan::ExplainAnalyze(
            Box::new(rule_predicate_pushdown(*inner)),
        ),
        LogicalPlan::CopyQueryTo { query, csv, delimiter, header, null_string, quote, escape } => {
            LogicalPlan::CopyQueryTo {
                query: Box::new(rule_predicate_pushdown(*query)),
                csv, delimiter, header, null_string, quote, escape,
            }
        }
        // Leaf / DML / DDL / utility 鈥?no children to recurse into
        other => other,
    }
}

// 鈹€鈹€ Rule 2: Join Reordering 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

/// Reorder multi-way inner joins using cost estimates from table row counts.
/// Delegates to `crate::cost::reorder_joins` for the actual reordering logic.
fn rule_join_reorder(plan: LogicalPlan, stats: &TableRowCounts) -> LogicalPlan {
    match plan {
        LogicalPlan::MultiJoin { base, joins } if !joins.is_empty() => {
            let left_col_count = joins[0].right_col_offset;
            let reordered = crate::cost::reorder_joins(left_col_count, &joins, stats);
            LogicalPlan::MultiJoin {
                base: Box::new(rule_join_reorder(*base, stats)),
                joins: reordered,
            }
        }
        // Recurse
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(rule_join_reorder(*input, stats)),
            predicate,
        },
        LogicalPlan::Project { input, projections, visible_count } => LogicalPlan::Project {
            input: Box::new(rule_join_reorder(*input, stats)),
            projections,
            visible_count,
        },
        LogicalPlan::Aggregate { input, group_by, grouping_sets, projections, visible_count, having } => {
            LogicalPlan::Aggregate {
                input: Box::new(rule_join_reorder(*input, stats)),
                group_by, grouping_sets, projections, visible_count, having,
            }
        }
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(rule_join_reorder(*input, stats)),
            order_by,
        },
        LogicalPlan::Limit { input, limit, offset } => LogicalPlan::Limit {
            input: Box::new(rule_join_reorder(*input, stats)),
            limit, offset,
        },
        LogicalPlan::Distinct { input, mode } => LogicalPlan::Distinct {
            input: Box::new(rule_join_reorder(*input, stats)),
            mode,
        },
        LogicalPlan::Join { left, right, join_info } => LogicalPlan::Join {
            left: Box::new(rule_join_reorder(*left, stats)),
            right: Box::new(rule_join_reorder(*right, stats)),
            join_info,
        },
        LogicalPlan::SetOp { left, right, kind, all } => LogicalPlan::SetOp {
            left: Box::new(rule_join_reorder(*left, stats)),
            right: Box::new(rule_join_reorder(*right, stats)),
            kind, all,
        },
        LogicalPlan::WithCtes { ctes, input } => LogicalPlan::WithCtes {
            ctes,
            input: Box::new(rule_join_reorder(*input, stats)),
        },
        LogicalPlan::Explain(inner) => LogicalPlan::Explain(
            Box::new(rule_join_reorder(*inner, stats)),
        ),
        LogicalPlan::ExplainAnalyze(inner) => LogicalPlan::ExplainAnalyze(
            Box::new(rule_join_reorder(*inner, stats)),
        ),
        LogicalPlan::CopyQueryTo { query, csv, delimiter, header, null_string, quote, escape } => {
            LogicalPlan::CopyQueryTo {
                query: Box::new(rule_join_reorder(*query, stats)),
                csv, delimiter, header, null_string, quote, escape,
            }
        }
        other => other,
    }
}

// 鈹€鈹€ Rule 3: Limit Pushdown 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

/// Push `Limit` below `Sort` when safe (the Sort already enforces order,
/// so the physical sort can benefit from a top-N optimization).
///
/// Also pushes `Limit` into a bare `Scan` when there is no sort or
/// aggregate between them (simple LIMIT pushdown).
fn rule_limit_pushdown(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Limit { input, limit, offset } => {
            let inner = rule_limit_pushdown(*input);
            match inner {
                // Limit(Project(Scan)) 鈫?Project(Scan with limit)
                LogicalPlan::Project {
                    input: proj_inner,
                    projections,
                    visible_count,
                } if matches!(*proj_inner, LogicalPlan::Scan { .. }) => {
                    // Only push if no offset (offset must be applied after full scan)
                    if offset.is_none() || offset == Some(0) {
                        let mut scan = *proj_inner;
                        // Wrap scan with limit
                        scan = LogicalPlan::Limit {
                            input: Box::new(scan),
                            limit,
                            offset: None,
                        };
                        LogicalPlan::Project {
                            input: Box::new(scan),
                            projections,
                            visible_count,
                        }
                    } else {
                        LogicalPlan::Limit {
                            input: Box::new(LogicalPlan::Project {
                                input: proj_inner,
                                projections,
                                visible_count,
                            }),
                            limit,
                            offset,
                        }
                    }
                }
                other => LogicalPlan::Limit {
                    input: Box::new(other),
                    limit,
                    offset,
                },
            }
        }
        // Recurse
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(rule_limit_pushdown(*input)),
            predicate,
        },
        LogicalPlan::Project { input, projections, visible_count } => LogicalPlan::Project {
            input: Box::new(rule_limit_pushdown(*input)),
            projections,
            visible_count,
        },
        LogicalPlan::Aggregate { input, group_by, grouping_sets, projections, visible_count, having } => {
            LogicalPlan::Aggregate {
                input: Box::new(rule_limit_pushdown(*input)),
                group_by, grouping_sets, projections, visible_count, having,
            }
        }
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(rule_limit_pushdown(*input)),
            order_by,
        },
        LogicalPlan::Distinct { input, mode } => LogicalPlan::Distinct {
            input: Box::new(rule_limit_pushdown(*input)),
            mode,
        },
        LogicalPlan::MultiJoin { base, joins } => LogicalPlan::MultiJoin {
            base: Box::new(rule_limit_pushdown(*base)),
            joins,
        },
        LogicalPlan::Join { left, right, join_info } => LogicalPlan::Join {
            left: Box::new(rule_limit_pushdown(*left)),
            right: Box::new(rule_limit_pushdown(*right)),
            join_info,
        },
        LogicalPlan::SetOp { left, right, kind, all } => LogicalPlan::SetOp {
            left: Box::new(rule_limit_pushdown(*left)),
            right: Box::new(rule_limit_pushdown(*right)),
            kind, all,
        },
        LogicalPlan::WithCtes { ctes, input } => LogicalPlan::WithCtes {
            ctes,
            input: Box::new(rule_limit_pushdown(*input)),
        },
        LogicalPlan::Explain(inner) => LogicalPlan::Explain(
            Box::new(rule_limit_pushdown(*inner)),
        ),
        LogicalPlan::ExplainAnalyze(inner) => LogicalPlan::ExplainAnalyze(
            Box::new(rule_limit_pushdown(*inner)),
        ),
        LogicalPlan::CopyQueryTo { query, csv, delimiter, header, null_string, quote, escape } => {
            LogicalPlan::CopyQueryTo {
                query: Box::new(rule_limit_pushdown(*query)),
                csv, delimiter, header, null_string, quote, escape,
            }
        }
        other => other,
    }
}

// 鈹€鈹€ Helpers 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

// ── Rule 4: Projection Pruning ──────────────────────────────────────────

/// Remove unused columns from Project nodes.
///
/// Walk the plan top-down collecting "required" column indices.  When we
/// reach a `Project` node whose projections include columns not demanded
/// by any ancestor, mark them for removal (but keep at least the visible
/// ones).  This reduces scan width and intermediate row size.
fn rule_projection_pruning(plan: LogicalPlan) -> LogicalPlan {
    // Collect all columns referenced by the plan *above* each Project node.
    // Then prune unreferenced projections.
    prune_plan(plan, &None)
}

/// Recursively prune unused projections.
/// `required` is Some(set) when an ancestor has told us which columns it needs.
fn prune_plan(plan: LogicalPlan, required: &Option<HashSet<usize>>) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, projections, visible_count } => {
            // Determine which columns are referenced by the projections themselves
            let mut child_required = HashSet::new();
            for proj in &projections {
                collect_projection_refs(proj, &mut child_required);
            }
            // Also collect columns from projections that parents need
            if let Some(req) = required {
                for &idx in req {
                    if idx < projections.len() {
                        collect_projection_refs(&projections[idx], &mut child_required);
                    }
                }
            }

            let pruned_input = prune_plan(*input, &Some(child_required));

            // Prune hidden projections that no ancestor needs
            let mut new_projections = Vec::with_capacity(projections.len());
            let mut new_visible = visible_count;
            for (i, proj) in projections.into_iter().enumerate() {
                if i < visible_count {
                    // Always keep visible projections
                    new_projections.push(proj);
                } else if let Some(req) = required {
                    // Hidden projection: keep only if required
                    if req.contains(&i) {
                        new_projections.push(proj);
                    }
                } else {
                    // No requirement info — keep all
                    new_projections.push(proj);
                }
            }
            if new_visible > new_projections.len() {
                new_visible = new_projections.len();
            }

            LogicalPlan::Project {
                input: Box::new(pruned_input),
                projections: new_projections,
                visible_count: new_visible,
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            // Collect columns the filter needs, forward to child
            let mut filter_refs = HashSet::new();
            collect_expr_refs(&predicate, &mut filter_refs);
            let merged = merge_required(required, &filter_refs);
            LogicalPlan::Filter {
                input: Box::new(prune_plan(*input, &Some(merged))),
                predicate,
            }
        }
        LogicalPlan::Sort { input, order_by } => {
            let mut sort_refs = HashSet::new();
            for ob in &order_by {
                sort_refs.insert(ob.column_idx);
            }
            let merged = merge_required(required, &sort_refs);
            LogicalPlan::Sort {
                input: Box::new(prune_plan(*input, &Some(merged))),
                order_by,
            }
        }
        LogicalPlan::Aggregate { input, group_by, grouping_sets, projections, visible_count, having } => {
            let mut child_refs = HashSet::new();
            for &g in &group_by { child_refs.insert(g); }
            for gs in &grouping_sets { for &g in gs { child_refs.insert(g); } }
            for proj in &projections { collect_projection_refs(proj, &mut child_refs); }
            if let Some(ref h) = having { collect_expr_refs(h, &mut child_refs); }
            LogicalPlan::Aggregate {
                input: Box::new(prune_plan(*input, &Some(child_refs))),
                group_by, grouping_sets, projections, visible_count, having,
            }
        }
        LogicalPlan::Limit { input, limit, offset } => LogicalPlan::Limit {
            input: Box::new(prune_plan(*input, required)),
            limit, offset,
        },
        LogicalPlan::Distinct { input, mode } => LogicalPlan::Distinct {
            input: Box::new(prune_plan(*input, required)),
            mode,
        },
        LogicalPlan::MultiJoin { base, joins } => LogicalPlan::MultiJoin {
            base: Box::new(prune_plan(*base, required)),
            joins,
        },
        LogicalPlan::Join { left, right, join_info } => LogicalPlan::Join {
            left: Box::new(prune_plan(*left, required)),
            right: Box::new(prune_plan(*right, required)),
            join_info,
        },
        LogicalPlan::SetOp { left, right, kind, all } => LogicalPlan::SetOp {
            left: Box::new(prune_plan(*left, required)),
            right: Box::new(prune_plan(*right, required)),
            kind, all,
        },
        LogicalPlan::WithCtes { ctes, input } => LogicalPlan::WithCtes {
            ctes,
            input: Box::new(prune_plan(*input, required)),
        },
        LogicalPlan::Explain(inner) => LogicalPlan::Explain(
            Box::new(prune_plan(*inner, required)),
        ),
        LogicalPlan::ExplainAnalyze(inner) => LogicalPlan::ExplainAnalyze(
            Box::new(prune_plan(*inner, required)),
        ),
        LogicalPlan::CopyQueryTo { query, csv, delimiter, header, null_string, quote, escape } => {
            LogicalPlan::CopyQueryTo {
                query: Box::new(prune_plan(*query, required)),
                csv, delimiter, header, null_string, quote, escape,
            }
        }
        other => other,
    }
}

fn collect_projection_refs(proj: &BoundProjection, out: &mut HashSet<usize>) {
    match proj {
        BoundProjection::Column(idx, _) => { out.insert(*idx); }
        BoundProjection::Expr(expr, _) => collect_expr_refs(expr, out),
        BoundProjection::Aggregate(_, arg, _, _, filter) => {
            if let Some(expr) = arg { collect_expr_refs(expr, out); }
            if let Some(f) = filter { collect_expr_refs(f, out); }
        }
        BoundProjection::Window(wf) => {
            for &col in &wf.partition_by { out.insert(col); }
            for ob in &wf.order_by { out.insert(ob.column_idx); }
        }
    }
}

fn collect_expr_refs(expr: &BoundExpr, out: &mut HashSet<usize>) {
    match expr {
        BoundExpr::ColumnRef(idx) => { out.insert(*idx); }
        BoundExpr::BinaryOp { left, right, .. } => {
            collect_expr_refs(left, out);
            collect_expr_refs(right, out);
        }
        BoundExpr::Not(inner) | BoundExpr::IsNull(inner) | BoundExpr::IsNotNull(inner)
        | BoundExpr::Cast { expr: inner, .. } => collect_expr_refs(inner, out),
        BoundExpr::Between { expr, low, high, .. } => {
            collect_expr_refs(expr, out);
            collect_expr_refs(low, out);
            collect_expr_refs(high, out);
        }
        BoundExpr::InList { expr, list, .. } => {
            collect_expr_refs(expr, out);
            for e in list { collect_expr_refs(e, out); }
        }
        BoundExpr::Like { expr, pattern, .. } => {
            collect_expr_refs(expr, out);
            collect_expr_refs(pattern, out);
        }
        BoundExpr::Function { args, .. } => {
            for a in args { collect_expr_refs(a, out); }
        }
        BoundExpr::Coalesce(args) => {
            for a in args { collect_expr_refs(a, out); }
        }
        BoundExpr::Case { operand, conditions, results, else_result } => {
            if let Some(op) = operand { collect_expr_refs(op, out); }
            for c in conditions { collect_expr_refs(c, out); }
            for r in results { collect_expr_refs(r, out); }
            if let Some(el) = else_result { collect_expr_refs(el, out); }
        }
        BoundExpr::IsNotDistinctFrom { left, right } => {
            collect_expr_refs(left, out);
            collect_expr_refs(right, out);
        }
        BoundExpr::AnyOp { left, right, .. } | BoundExpr::AllOp { left, right, .. } => {
            collect_expr_refs(left, out);
            collect_expr_refs(right, out);
        }
        BoundExpr::ArrayLiteral(exprs) => {
            for e in exprs { collect_expr_refs(e, out); }
        }
        BoundExpr::ArrayIndex { array, index } => {
            collect_expr_refs(array, out);
            collect_expr_refs(index, out);
        }
        BoundExpr::ArraySlice { array, lower, upper } => {
            collect_expr_refs(array, out);
            if let Some(l) = lower { collect_expr_refs(l, out); }
            if let Some(u) = upper { collect_expr_refs(u, out); }
        }
        BoundExpr::AggregateExpr { arg: Some(a), .. } => {
            collect_expr_refs(a, out);
        }
        BoundExpr::AggregateExpr { arg: None, .. } => {}
        _ => {}
    }
}

fn merge_required(parent: &Option<HashSet<usize>>, extra: &HashSet<usize>) -> HashSet<usize> {
    match parent {
        Some(p) => p.union(extra).copied().collect(),
        None => extra.clone(),
    }
}

// ── Rule 5: Subquery Decorrelation ──────────────────────────────────────

/// Convert simple correlated subqueries to semi-joins.
///
/// Pattern detected:
///   `Filter(input, EXISTS(SELECT ... FROM t WHERE t.col = OuterColumnRef(i)))`
///     → `SemiJoin(input, Scan(t), on: input.col_i = t.col)`
///
/// Currently handles:
///   - `EXISTS (SELECT ... WHERE inner.col = outer.col)` → semi-join
///   - `NOT EXISTS (...)` → anti-join (represented as Filter with mark)
///   - `col IN (SELECT col FROM t)` → semi-join with equality
///
/// More complex patterns (multi-level correlation, lateral references)
/// are left as-is for the row-at-a-time executor to handle.
fn rule_subquery_decorrelation(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let inner = rule_subquery_decorrelation(*input);

            // Try to extract a semi-join from EXISTS subquery
            if let Some((subquery_table_id, subquery_schema, correlation_pairs, negated)) =
                try_extract_exists_semijoin(&predicate)
            {
                // Build the semi-join condition: inner.col_i = subquery.col_j
                // The right columns start at offset = schema column count from inner
                let right_col_offset = count_output_columns(&inner);
                let join_condition = build_correlation_condition(&correlation_pairs, right_col_offset);

                let join_type = if negated { JoinType::Left } else { JoinType::Inner };

                let join = LogicalPlan::Join {
                    left: Box::new(inner),
                    right: Box::new(LogicalPlan::Scan {
                        table_id: subquery_table_id,
                        schema: subquery_schema.clone(),
                        virtual_rows: vec![],
                    }),
                    join_info: BoundJoin {
                        join_type,
                        right_table_id: subquery_table_id,
                        right_table_name: subquery_schema.name.clone(),
                        right_schema: subquery_schema,
                        right_col_offset,
                        condition: Some(join_condition),
                    },
                };

                if negated {
                    // Anti-join: wrap with IS NULL check on right side
                    let null_check = BoundExpr::IsNull(
                        Box::new(BoundExpr::ColumnRef(right_col_offset))
                    );
                    LogicalPlan::Filter {
                        input: Box::new(join),
                        predicate: null_check,
                    }
                } else {
                    // Semi-join: just use DISTINCT to eliminate duplicates
                    LogicalPlan::Distinct {
                        input: Box::new(join),
                        mode: DistinctMode::All,
                    }
                }
            } else if let Some((col_idx, subquery_table_id, subquery_schema, inner_col_idx, negated)) =
                try_extract_in_semijoin(&predicate)
            {
                // IN (SELECT col FROM t) → semi-join
                let right_col_offset = count_output_columns(&inner);
                let condition = BoundExpr::BinaryOp {
                    left: Box::new(BoundExpr::ColumnRef(col_idx)),
                    op: BinOp::Eq,
                    right: Box::new(BoundExpr::ColumnRef(right_col_offset + inner_col_idx)),
                };

                let join_type = if negated { JoinType::Left } else { JoinType::Inner };

                let join = LogicalPlan::Join {
                    left: Box::new(inner),
                    right: Box::new(LogicalPlan::Scan {
                        table_id: subquery_table_id,
                        schema: subquery_schema.clone(),
                        virtual_rows: vec![],
                    }),
                    join_info: BoundJoin {
                        join_type,
                        right_table_id: subquery_table_id,
                        right_table_name: subquery_schema.name.clone(),
                        right_schema: subquery_schema,
                        right_col_offset,
                        condition: Some(condition),
                    },
                };

                if negated {
                    let null_check = BoundExpr::IsNull(
                        Box::new(BoundExpr::ColumnRef(right_col_offset + inner_col_idx))
                    );
                    LogicalPlan::Filter {
                        input: Box::new(join),
                        predicate: null_check,
                    }
                } else {
                    LogicalPlan::Distinct {
                        input: Box::new(join),
                        mode: DistinctMode::All,
                    }
                }
            } else {
                // Cannot decorrelate — keep as-is
                LogicalPlan::Filter {
                    input: Box::new(inner),
                    predicate,
                }
            }
        }
        // Recurse into all other node types
        LogicalPlan::Project { input, projections, visible_count } => LogicalPlan::Project {
            input: Box::new(rule_subquery_decorrelation(*input)),
            projections, visible_count,
        },
        LogicalPlan::Aggregate { input, group_by, grouping_sets, projections, visible_count, having } => {
            LogicalPlan::Aggregate {
                input: Box::new(rule_subquery_decorrelation(*input)),
                group_by, grouping_sets, projections, visible_count, having,
            }
        }
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(rule_subquery_decorrelation(*input)),
            order_by,
        },
        LogicalPlan::Limit { input, limit, offset } => LogicalPlan::Limit {
            input: Box::new(rule_subquery_decorrelation(*input)),
            limit, offset,
        },
        LogicalPlan::Distinct { input, mode } => LogicalPlan::Distinct {
            input: Box::new(rule_subquery_decorrelation(*input)),
            mode,
        },
        LogicalPlan::MultiJoin { base, joins } => LogicalPlan::MultiJoin {
            base: Box::new(rule_subquery_decorrelation(*base)),
            joins,
        },
        LogicalPlan::Join { left, right, join_info } => LogicalPlan::Join {
            left: Box::new(rule_subquery_decorrelation(*left)),
            right: Box::new(rule_subquery_decorrelation(*right)),
            join_info,
        },
        LogicalPlan::SetOp { left, right, kind, all } => LogicalPlan::SetOp {
            left: Box::new(rule_subquery_decorrelation(*left)),
            right: Box::new(rule_subquery_decorrelation(*right)),
            kind, all,
        },
        LogicalPlan::WithCtes { ctes, input } => LogicalPlan::WithCtes {
            ctes,
            input: Box::new(rule_subquery_decorrelation(*input)),
        },
        LogicalPlan::Explain(inner) => LogicalPlan::Explain(
            Box::new(rule_subquery_decorrelation(*inner)),
        ),
        LogicalPlan::ExplainAnalyze(inner) => LogicalPlan::ExplainAnalyze(
            Box::new(rule_subquery_decorrelation(*inner)),
        ),
        LogicalPlan::CopyQueryTo { query, csv, delimiter, header, null_string, quote, escape } => {
            LogicalPlan::CopyQueryTo {
                query: Box::new(rule_subquery_decorrelation(*query)),
                csv, delimiter, header, null_string, quote, escape,
            }
        }
        other => other,
    }
}

/// Try to extract an EXISTS/NOT EXISTS correlated subquery into a semi-join.
/// Returns (subquery_table_id, subquery_schema, correlation_pairs, negated).
/// `correlation_pairs` is a list of (outer_col_idx, inner_col_idx) equalities.
#[allow(clippy::type_complexity)]
fn try_extract_exists_semijoin(
    expr: &BoundExpr,
) -> Option<(falcon_common::types::TableId, falcon_common::schema::TableSchema, Vec<(usize, usize)>, bool)> {
    let (subquery, negated) = match expr {
        BoundExpr::Exists { subquery, negated } => (subquery, *negated),
        _ => return None,
    };

    // The subquery must be a simple select from a single table with a
    // correlated WHERE clause
    let sel = subquery;
    if !sel.joins.is_empty() || !sel.ctes.is_empty() || !sel.unions.is_empty() {
        return None;
    }

    // Extract correlation pairs from the filter
    let filter = sel.filter.as_ref()?;
    let pairs = extract_correlation_pairs(filter)?;
    if pairs.is_empty() {
        return None;
    }

    Some((sel.table_id, sel.schema.clone(), pairs, negated))
}

/// Try to extract an IN/NOT IN subquery into a semi-join.
/// Returns (outer_col_idx, subquery_table_id, subquery_schema, inner_col_idx, negated).
fn try_extract_in_semijoin(
    expr: &BoundExpr,
) -> Option<(usize, falcon_common::types::TableId, falcon_common::schema::TableSchema, usize, bool)> {
    let (outer_expr, subquery, negated) = match expr {
        BoundExpr::InSubquery { expr, subquery, negated } => (expr, subquery, *negated),
        _ => return None,
    };

    // The outer expression must be a simple column reference
    let outer_col = match outer_expr.as_ref() {
        BoundExpr::ColumnRef(idx) => *idx,
        _ => return None,
    };

    // The subquery must be a simple single-table select
    let sel = subquery;
    if !sel.joins.is_empty() || !sel.ctes.is_empty() || !sel.unions.is_empty() {
        return None;
    }

    // The first projection must be a column reference
    if sel.projections.is_empty() {
        return None;
    }
    let inner_col = match &sel.projections[0] {
        BoundProjection::Column(idx, _) => *idx,
        _ => return None,
    };

    Some((outer_col, sel.table_id, sel.schema.clone(), inner_col, negated))
}

/// Extract correlation pairs from a filter: (outer_col_idx, inner_col_idx).
/// Pattern: `OuterColumnRef(i) = ColumnRef(j)` or `ColumnRef(j) = OuterColumnRef(i)`.
fn extract_correlation_pairs(expr: &BoundExpr) -> Option<Vec<(usize, usize)>> {
    let mut pairs = Vec::new();
    extract_pairs_recursive(expr, &mut pairs);
    if pairs.is_empty() { None } else { Some(pairs) }
}

fn extract_pairs_recursive(expr: &BoundExpr, pairs: &mut Vec<(usize, usize)>) {
    match expr {
        BoundExpr::BinaryOp { left, op: BinOp::Eq, right } => {
            match (left.as_ref(), right.as_ref()) {
                (BoundExpr::OuterColumnRef(outer), BoundExpr::ColumnRef(inner)) |
                (BoundExpr::ColumnRef(inner), BoundExpr::OuterColumnRef(outer)) => {
                    pairs.push((*outer, *inner));
                }
                _ => {}
            }
        }
        BoundExpr::BinaryOp { left, op: BinOp::And, right } => {
            extract_pairs_recursive(left, pairs);
            extract_pairs_recursive(right, pairs);
        }
        _ => {}
    }
}

/// Count output columns of a logical plan (approximate — based on schema).
fn count_output_columns(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::Scan { schema, .. } => schema.columns.len(),
        LogicalPlan::Project { projections, .. } => projections.len(),
        LogicalPlan::Aggregate { projections, .. } => projections.len(),
        LogicalPlan::Filter { input, .. } |
        LogicalPlan::Sort { input, .. } |
        LogicalPlan::Limit { input, .. } |
        LogicalPlan::Distinct { input, .. } |
        LogicalPlan::WithCtes { input, .. } => count_output_columns(input),
        LogicalPlan::Join { left, right, .. } => {
            count_output_columns(left) + count_output_columns(right)
        }
        LogicalPlan::MultiJoin { base, joins } => {
            let mut cols = count_output_columns(base);
            for j in joins { cols += j.right_schema.columns.len(); }
            cols
        }
        _ => 0,
    }
}

/// Build a join condition from correlation pairs.
fn build_correlation_condition(pairs: &[(usize, usize)], right_offset: usize) -> BoundExpr {
    assert!(!pairs.is_empty());
    let mut conditions: Vec<BoundExpr> = pairs.iter().map(|(outer, inner)| {
        BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(*outer)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::ColumnRef(right_offset + *inner)),
        }
    }).collect();

    let mut result = conditions.pop().unwrap();
    while let Some(cond) = conditions.pop() {
        result = BoundExpr::BinaryOp {
            left: Box::new(cond),
            op: BinOp::And,
            right: Box::new(result),
        };
    }
    result
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Check if an expression only references `ColumnRef` (no computed exprs).
fn predicate_uses_only_column_refs(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::ColumnRef(_) | BoundExpr::Literal(_) | BoundExpr::Parameter(_) => true,
        BoundExpr::BinaryOp { left, right, .. } => {
            predicate_uses_only_column_refs(left) && predicate_uses_only_column_refs(right)
        }
        BoundExpr::Not(inner) | BoundExpr::IsNull(inner) | BoundExpr::IsNotNull(inner) => {
            predicate_uses_only_column_refs(inner)
        }
        BoundExpr::Between { expr, low, high, .. } => {
            predicate_uses_only_column_refs(expr)
                && predicate_uses_only_column_refs(low)
                && predicate_uses_only_column_refs(high)
        }
        BoundExpr::InList { expr, list, .. } => {
            predicate_uses_only_column_refs(expr)
                && list.iter().all(predicate_uses_only_column_refs)
        }
        BoundExpr::Like { expr, pattern, .. } => {
            predicate_uses_only_column_refs(expr) && predicate_uses_only_column_refs(pattern)
        }
        BoundExpr::Cast { expr: inner, .. } => predicate_uses_only_column_refs(inner),
        BoundExpr::IsNotDistinctFrom { left, right } => {
            predicate_uses_only_column_refs(left) && predicate_uses_only_column_refs(right)
        }
        _ => false,
    }
}

/// Split an AND-conjunction into individual predicates.
fn split_conjuncts(expr: BoundExpr) -> Vec<BoundExpr> {
    match expr {
        BoundExpr::BinaryOp { left, op: BinOp::And, right } => {
            let mut result = split_conjuncts(*left);
            result.extend(split_conjuncts(*right));
            result
        }
        other => vec![other],
    }
}

/// Combine a list of predicates into an AND-conjunction.
fn combine_conjuncts(mut preds: Vec<BoundExpr>) -> BoundExpr {
    assert!(!preds.is_empty());
    let mut result = preds.pop().unwrap();
    while let Some(pred) = preds.pop() {
        result = BoundExpr::BinaryOp {
            left: Box::new(pred),
            op: BinOp::And,
            right: Box::new(result),
        };
    }
    result
}

/// Find the maximum column index referenced in an expression.
/// Returns 0 if no ColumnRef is found.
fn max_column_ref(expr: &BoundExpr) -> usize {
    match expr {
        BoundExpr::ColumnRef(idx) => *idx,
        BoundExpr::BinaryOp { left, right, .. } => {
            max_column_ref(left).max(max_column_ref(right))
        }
        BoundExpr::Not(inner) | BoundExpr::IsNull(inner) | BoundExpr::IsNotNull(inner) => {
            max_column_ref(inner)
        }
        BoundExpr::Between { expr, low, high, .. } => {
            max_column_ref(expr).max(max_column_ref(low)).max(max_column_ref(high))
        }
        BoundExpr::InList { expr, list, .. } => {
            let max_list = list.iter().map(max_column_ref).max().unwrap_or(0);
            max_column_ref(expr).max(max_list)
        }
        BoundExpr::Like { expr, pattern, .. } => {
            max_column_ref(expr).max(max_column_ref(pattern))
        }
        BoundExpr::Cast { expr: inner, .. } => max_column_ref(inner),
        BoundExpr::IsNotDistinctFrom { left, right } => {
            max_column_ref(left).max(max_column_ref(right))
        }
        BoundExpr::Function { args, .. } => args.iter().map(max_column_ref).max().unwrap_or(0),
        BoundExpr::Coalesce(args) => args.iter().map(max_column_ref).max().unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::Datum;

    #[test]
    fn test_split_conjuncts_single() {
        let pred = BoundExpr::Literal(Datum::Boolean(true));
        let result = split_conjuncts(pred.clone());
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_split_conjuncts_nested_and() {
        let a = BoundExpr::ColumnRef(0);
        let b = BoundExpr::ColumnRef(1);
        let c = BoundExpr::ColumnRef(2);
        let and_bc = BoundExpr::BinaryOp {
            left: Box::new(b),
            op: BinOp::And,
            right: Box::new(c),
        };
        let and_abc = BoundExpr::BinaryOp {
            left: Box::new(a),
            op: BinOp::And,
            right: Box::new(and_bc),
        };
        let result = split_conjuncts(and_abc);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_combine_conjuncts_roundtrip() {
        let preds = vec![
            BoundExpr::ColumnRef(0),
            BoundExpr::ColumnRef(1),
        ];
        let combined = combine_conjuncts(preds);
        let split = split_conjuncts(combined);
        assert_eq!(split.len(), 2);
    }

    #[test]
    fn test_max_column_ref() {
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(3)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::ColumnRef(7)),
        };
        assert_eq!(max_column_ref(&expr), 7);
    }

    #[test]
    fn test_predicate_pushdown_below_project() {
        use falcon_common::schema::TableSchema;
        use falcon_common::types::TableId;

        let scan = LogicalPlan::Scan {
            table_id: TableId(1),
            schema: TableSchema {
                id: TableId(1),
                name: "t".into(),
                columns: vec![],
                primary_key_columns: vec![],
                next_serial_values: Default::default(),
                check_constraints: vec![],
                unique_constraints: vec![],
                foreign_keys: vec![],
            ..Default::default()
            },
            virtual_rows: vec![],
        };
        let project = LogicalPlan::Project {
            input: Box::new(scan),
            projections: vec![],
            visible_count: 0,
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(project),
            predicate: BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(0)),
                op: BinOp::Eq,
                right: Box::new(BoundExpr::Literal(Datum::Int32(1))),
            },
        };

        let optimized = rule_predicate_pushdown(filter);
        // Filter should have been pushed below Project
        assert!(matches!(optimized, LogicalPlan::Project { .. }));
        if let LogicalPlan::Project { input, .. } = &optimized {
            assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));
        }
    }
}
