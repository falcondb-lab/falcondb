//! Parameter substitution: rewrite a PhysicalPlan so that every
//! `BoundExpr::Parameter(idx)` is replaced with `BoundExpr::Literal(params[idx-1])`.
//!
//! This allows the existing executor to run parameterized plans without
//! threading a `params` slice through every exec_* method.

use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use falcon_planner::PhysicalPlan;
use falcon_sql_frontend::types::*;

use crate::eval::substitute_params_expr;

// ── helpers for nested types ─────────────────────────────────────────

fn subst_opt(expr: &Option<BoundExpr>, p: &[Datum]) -> Result<Option<BoundExpr>, ExecutionError> {
    expr.as_ref()
        .map(|e| substitute_params_expr(e, p))
        .transpose()
}

fn subst_vec(
    exprs: &[(BoundExpr, String)],
    p: &[Datum],
) -> Result<Vec<(BoundExpr, String)>, ExecutionError> {
    exprs
        .iter()
        .map(|(e, s)| Ok((substitute_params_expr(e, p)?, s.clone())))
        .collect()
}

fn subst_assignments(
    a: &[(usize, BoundExpr)],
    p: &[Datum],
) -> Result<Vec<(usize, BoundExpr)>, ExecutionError> {
    a.iter()
        .map(|(idx, e)| Ok((*idx, substitute_params_expr(e, p)?)))
        .collect()
}

fn subst_rows(rows: &[Vec<BoundExpr>], p: &[Datum]) -> Result<Vec<Vec<BoundExpr>>, ExecutionError> {
    rows.iter()
        .map(|row| row.iter().map(|e| substitute_params_expr(e, p)).collect())
        .collect()
}

fn subst_projection(
    proj: &BoundProjection,
    p: &[Datum],
) -> Result<BoundProjection, ExecutionError> {
    match proj {
        BoundProjection::Column(idx, alias) => Ok(BoundProjection::Column(*idx, alias.clone())),
        BoundProjection::Aggregate(func, arg, alias, distinct, filter) => {
            let new_arg = arg
                .as_ref()
                .map(|e| substitute_params_expr(e, p))
                .transpose()?;
            let new_filter = filter
                .as_ref()
                .map(|e| substitute_params_expr(e, p).map(Box::new))
                .transpose()?;
            Ok(BoundProjection::Aggregate(
                func.clone(),
                new_arg,
                alias.clone(),
                *distinct,
                new_filter,
            ))
        }
        BoundProjection::Expr(expr, alias) => Ok(BoundProjection::Expr(
            substitute_params_expr(expr, p)?,
            alias.clone(),
        )),
        BoundProjection::Window(wf) => Ok(BoundProjection::Window(wf.clone())),
    }
}

fn subst_projections(
    projs: &[BoundProjection],
    p: &[Datum],
) -> Result<Vec<BoundProjection>, ExecutionError> {
    projs.iter().map(|proj| subst_projection(proj, p)).collect()
}

fn subst_join(j: &BoundJoin, p: &[Datum]) -> Result<BoundJoin, ExecutionError> {
    Ok(BoundJoin {
        join_type: j.join_type,
        right_table_id: j.right_table_id,
        right_table_name: j.right_table_name.clone(),
        right_schema: j.right_schema.clone(),
        right_col_offset: j.right_col_offset,
        condition: subst_opt(&j.condition, p)?,
    })
}

fn subst_joins(joins: &[BoundJoin], p: &[Datum]) -> Result<Vec<BoundJoin>, ExecutionError> {
    joins.iter().map(|j| subst_join(j, p)).collect()
}

fn subst_on_conflict(
    oc: &Option<OnConflictAction>,
    p: &[Datum],
) -> Result<Option<OnConflictAction>, ExecutionError> {
    match oc {
        None => Ok(None),
        Some(OnConflictAction::DoNothing) => Ok(Some(OnConflictAction::DoNothing)),
        Some(OnConflictAction::DoUpdate(assignments)) => Ok(Some(OnConflictAction::DoUpdate(
            subst_assignments(assignments, p)?,
        ))),
    }
}

fn subst_select(sel: &BoundSelect, p: &[Datum]) -> Result<BoundSelect, ExecutionError> {
    Ok(BoundSelect {
        table_id: sel.table_id,
        table_name: sel.table_name.clone(),
        schema: sel.schema.clone(),
        projections: subst_projections(&sel.projections, p)?,
        visible_projection_count: sel.visible_projection_count,
        filter: subst_opt(&sel.filter, p)?,
        group_by: sel.group_by.clone(),
        grouping_sets: sel.grouping_sets.clone(),
        having: subst_opt(&sel.having, p)?,
        order_by: sel.order_by.clone(),
        limit: sel.limit,
        offset: sel.offset,
        distinct: sel.distinct.clone(),
        joins: subst_joins(&sel.joins, p)?,
        ctes: subst_ctes(&sel.ctes, p)?,
        unions: sel
            .unions
            .iter()
            .map(|(s, k, a)| Ok((subst_select(s, p)?, *k, *a)))
            .collect::<Result<_, ExecutionError>>()?,
        virtual_rows: sel.virtual_rows.clone(),
    })
}

fn subst_ctes(ctes: &[BoundCte], p: &[Datum]) -> Result<Vec<BoundCte>, ExecutionError> {
    ctes.iter()
        .map(|c| {
            Ok(BoundCte {
                name: c.name.clone(),
                table_id: c.table_id,
                select: subst_select(&c.select, p)?,
                recursive_select: c
                    .recursive_select
                    .as_ref()
                    .map(|s| subst_select(s, p).map(Box::new))
                    .transpose()?,
            })
        })
        .collect()
}

fn subst_unions(
    unions: &[(BoundSelect, SetOpKind, bool)],
    p: &[Datum],
) -> Result<Vec<(BoundSelect, SetOpKind, bool)>, ExecutionError> {
    unions
        .iter()
        .map(|(s, k, a)| Ok((subst_select(s, p)?, *k, *a)))
        .collect()
}

// ── Public API ───────────────────────────────────────────────────────

/// Rewrite a PhysicalPlan, replacing all `BoundExpr::Parameter` nodes with
/// the corresponding literal values from `params`.
/// If `params` is empty, the plan is returned as-is (no allocation).
pub fn substitute_params_plan(
    plan: &PhysicalPlan,
    params: &[Datum],
) -> Result<PhysicalPlan, ExecutionError> {
    if params.is_empty() {
        return Ok(plan.clone());
    }
    match plan {
        // ── DDL / utility (no BoundExpr) ──
        PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::AlterTable { .. }
        | PhysicalPlan::Truncate { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::DropIndex { .. }
        | PhysicalPlan::CreateView { .. }
        | PhysicalPlan::DropView { .. }
        | PhysicalPlan::Begin
        | PhysicalPlan::Commit
        | PhysicalPlan::Rollback
        | PhysicalPlan::ShowTxnStats
        | PhysicalPlan::ShowNodeRole
        | PhysicalPlan::ShowWalStats
        | PhysicalPlan::ShowConnections
        | PhysicalPlan::RunGc
        | PhysicalPlan::Analyze { .. }
        | PhysicalPlan::ShowTableStats { .. }
        | PhysicalPlan::CreateSequence { .. }
        | PhysicalPlan::DropSequence { .. }
        | PhysicalPlan::ShowSequences
        | PhysicalPlan::CopyFrom { .. }
        | PhysicalPlan::CopyTo { .. } => Ok(plan.clone()),

        // ── DML ──
        PhysicalPlan::Insert {
            table_id,
            schema,
            columns,
            rows,
            source_select,
            returning,
            on_conflict,
        } => Ok(PhysicalPlan::Insert {
            table_id: *table_id,
            schema: schema.clone(),
            columns: columns.clone(),
            rows: subst_rows(rows, params)?,
            source_select: source_select
                .as_ref()
                .map(|s| subst_select(s, params))
                .transpose()?,
            returning: subst_vec(returning, params)?,
            on_conflict: subst_on_conflict(on_conflict, params)?,
        }),
        PhysicalPlan::Update {
            table_id,
            schema,
            assignments,
            filter,
            returning,
            from_table,
        } => Ok(PhysicalPlan::Update {
            table_id: *table_id,
            schema: schema.clone(),
            assignments: subst_assignments(assignments, params)?,
            filter: subst_opt(filter, params)?,
            returning: subst_vec(returning, params)?,
            from_table: from_table.clone(),
        }),
        PhysicalPlan::Delete {
            table_id,
            schema,
            filter,
            returning,
            using_table,
        } => Ok(PhysicalPlan::Delete {
            table_id: *table_id,
            schema: schema.clone(),
            filter: subst_opt(filter, params)?,
            returning: subst_vec(returning, params)?,
            using_table: using_table.clone(),
        }),

        // ── Scans / joins ──
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
        } => Ok(PhysicalPlan::SeqScan {
            table_id: *table_id,
            schema: schema.clone(),
            projections: subst_projections(projections, params)?,
            visible_projection_count: *visible_projection_count,
            filter: subst_opt(filter, params)?,
            group_by: group_by.clone(),
            grouping_sets: grouping_sets.clone(),
            having: subst_opt(having, params)?,
            order_by: order_by.clone(),
            limit: *limit,
            offset: *offset,
            distinct: distinct.clone(),
            ctes: subst_ctes(ctes, params)?,
            unions: subst_unions(unions, params)?,
            virtual_rows: virtual_rows.clone(),
        }),
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
        } => Ok(PhysicalPlan::IndexScan {
            table_id: *table_id,
            schema: schema.clone(),
            index_col: *index_col,
            index_value: substitute_params_expr(index_value, params)?,
            projections: subst_projections(projections, params)?,
            visible_projection_count: *visible_projection_count,
            filter: subst_opt(filter, params)?,
            group_by: group_by.clone(),
            grouping_sets: grouping_sets.clone(),
            having: subst_opt(having, params)?,
            order_by: order_by.clone(),
            limit: *limit,
            offset: *offset,
            distinct: distinct.clone(),
            ctes: subst_ctes(ctes, params)?,
            unions: subst_unions(unions, params)?,
            virtual_rows: virtual_rows.clone(),
        }),
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
        } => Ok(PhysicalPlan::NestedLoopJoin {
            left_table_id: *left_table_id,
            left_schema: left_schema.clone(),
            joins: subst_joins(joins, params)?,
            combined_schema: combined_schema.clone(),
            projections: subst_projections(projections, params)?,
            visible_projection_count: *visible_projection_count,
            filter: subst_opt(filter, params)?,
            order_by: order_by.clone(),
            limit: *limit,
            offset: *offset,
            distinct: distinct.clone(),
            ctes: subst_ctes(ctes, params)?,
            unions: subst_unions(unions, params)?,
        }),
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
        } => Ok(PhysicalPlan::HashJoin {
            left_table_id: *left_table_id,
            left_schema: left_schema.clone(),
            joins: subst_joins(joins, params)?,
            combined_schema: combined_schema.clone(),
            projections: subst_projections(projections, params)?,
            visible_projection_count: *visible_projection_count,
            filter: subst_opt(filter, params)?,
            order_by: order_by.clone(),
            limit: *limit,
            offset: *offset,
            distinct: distinct.clone(),
            ctes: subst_ctes(ctes, params)?,
            unions: subst_unions(unions, params)?,
        }),

        // ── Wrappers ──
        PhysicalPlan::Explain(inner) => Ok(PhysicalPlan::Explain(Box::new(
            substitute_params_plan(inner, params)?,
        ))),
        PhysicalPlan::ExplainAnalyze(inner) => Ok(PhysicalPlan::ExplainAnalyze(Box::new(
            substitute_params_plan(inner, params)?,
        ))),
        PhysicalPlan::CopyQueryTo {
            query,
            csv,
            delimiter,
            header,
            null_string,
            quote,
            escape,
        } => Ok(PhysicalPlan::CopyQueryTo {
            query: Box::new(substitute_params_plan(query, params)?),
            csv: *csv,
            delimiter: *delimiter,
            header: *header,
            null_string: null_string.clone(),
            quote: *quote,
            escape: *escape,
        }),

        // Catch-all: any remaining variants without BoundExpr
        #[allow(unreachable_patterns)]
        _ => Ok(plan.clone()),
    }
}
