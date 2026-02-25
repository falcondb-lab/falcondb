//! Logical plan representation for query optimization.
//!
//! `LogicalPlan` sits between `BoundStatement` (from the SQL frontend) and
//! `PhysicalPlan` (consumed by the executor). The optimizer applies rule-based
//! transformations on `LogicalPlan` before it is lowered to `PhysicalPlan`.

use falcon_common::datum::OwnedRow;
use falcon_common::schema::TableSchema;
use falcon_common::types::TableId;
use falcon_sql_frontend::types::*;

// ── Relational operators ────────────────────────────────────────────────

/// A logical relational algebra tree. Each variant represents a logical
/// operator that can be optimized independently of its physical implementation.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum LogicalPlan {
    // ── Leaf operators ──────────────────────────────────────────────
    /// Full table scan.
    Scan {
        table_id: TableId,
        schema: TableSchema,
        /// Inline virtual rows (for GENERATE_SERIES etc.)
        virtual_rows: Vec<OwnedRow>,
    },

    // ── Unary relational operators ──────────────────────────────────
    /// Selection (WHERE / HAVING).
    Filter {
        input: Box<Self>,
        predicate: BoundExpr,
    },

    /// Projection (SELECT-list).
    Project {
        input: Box<Self>,
        projections: Vec<BoundProjection>,
        visible_count: usize,
    },

    /// Aggregation (GROUP BY + aggregate functions).
    Aggregate {
        input: Box<Self>,
        group_by: Vec<usize>,
        grouping_sets: Vec<Vec<usize>>,
        projections: Vec<BoundProjection>,
        visible_count: usize,
        having: Option<BoundExpr>,
    },

    /// Sort (ORDER BY).
    Sort {
        input: Box<Self>,
        order_by: Vec<BoundOrderBy>,
    },

    /// Limit + Offset.
    Limit {
        input: Box<Self>,
        limit: Option<usize>,
        offset: Option<usize>,
    },

    /// DISTINCT elimination.
    Distinct {
        input: Box<Self>,
        mode: DistinctMode,
    },

    // ── Binary relational operators ─────────────────────────────────
    /// Join (all join types — physical strategy chosen later).
    Join {
        left: Box<Self>,
        right: Box<Self>,
        join_info: BoundJoin,
    },

    /// Multi-way join (preserves original join list for reordering).
    MultiJoin {
        base: Box<Self>,
        joins: Vec<BoundJoin>,
    },

    // ── Set operations ──────────────────────────────────────────────
    /// UNION / INTERSECT / EXCEPT
    SetOp {
        left: Box<Self>,
        right: Box<Self>,
        kind: SetOpKind,
        /// true = ALL (no dedup)
        all: bool,
    },

    // ── CTEs ────────────────────────────────────────────────────────
    /// WITH (common table expressions) wrapping an inner query.
    WithCtes {
        ctes: Vec<BoundCte>,
        input: Box<Self>,
    },

    // ── DML (pass-through to physical) ──────────────────────────────
    Insert {
        table_id: TableId,
        schema: TableSchema,
        columns: Vec<usize>,
        rows: Vec<Vec<BoundExpr>>,
        source_select: Option<BoundSelect>,
        returning: Vec<(BoundExpr, String)>,
        on_conflict: Option<OnConflictAction>,
    },

    Update {
        table_id: TableId,
        schema: TableSchema,
        assignments: Vec<(usize, BoundExpr)>,
        filter: Option<BoundExpr>,
        returning: Vec<(BoundExpr, String)>,
        from_table: Option<BoundFromTable>,
    },

    Delete {
        table_id: TableId,
        schema: TableSchema,
        filter: Option<BoundExpr>,
        returning: Vec<(BoundExpr, String)>,
        using_table: Option<BoundFromTable>,
    },

    // ── DDL (pass-through) ──────────────────────────────────────────
    CreateTable {
        schema: TableSchema,
        if_not_exists: bool,
    },
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    AlterTable {
        table_name: String,
        ops: Vec<AlterTableOp>,
    },
    Truncate {
        table_name: String,
    },
    CreateIndex {
        index_name: String,
        table_name: String,
        column_indices: Vec<usize>,
        unique: bool,
    },
    DropIndex {
        index_name: String,
    },
    CreateView {
        name: String,
        query_sql: String,
        or_replace: bool,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    CreateSequence {
        name: String,
        start: i64,
    },
    DropSequence {
        name: String,
        if_exists: bool,
    },

    // ── Utility / Session ───────────────────────────────────────────
    Explain(Box<Self>),
    ExplainAnalyze(Box<Self>),
    Begin,
    Commit,
    Rollback,
    ShowTxnStats,
    ShowNodeRole,
    ShowWalStats,
    ShowConnections,
    RunGc,
    Analyze {
        table_name: String,
    },
    ShowTableStats {
        table_name: Option<String>,
    },
    ShowSequences,
    ShowTenants,
    ShowTenantUsage,
    CreateTenant {
        name: String,
        max_qps: u64,
        max_storage_bytes: u64,
    },
    DropTenant {
        name: String,
    },

    // ── COPY ────────────────────────────────────────────────────────
    CopyFrom {
        table_id: TableId,
        schema: TableSchema,
        columns: Vec<usize>,
        csv: bool,
        delimiter: char,
        header: bool,
        null_string: String,
        quote: char,
        escape: char,
    },
    CopyTo {
        table_id: TableId,
        schema: TableSchema,
        columns: Vec<usize>,
        csv: bool,
        delimiter: char,
        header: bool,
        null_string: String,
        quote: char,
        escape: char,
    },
    CopyQueryTo {
        query: Box<Self>,
        csv: bool,
        delimiter: char,
        header: bool,
        null_string: String,
        quote: char,
        escape: char,
    },
}

// ── Conversion: BoundStatement → LogicalPlan ────────────────────────────

impl LogicalPlan {
    /// Lower a `BoundStatement` into a `LogicalPlan` tree.
    ///
    /// This is a straightforward structural translation — no optimizations
    /// are applied here. The optimizer works on the resulting tree.
    pub fn from_bound(stmt: &BoundStatement) -> Result<Self, falcon_common::error::SqlError> {
        match stmt {
            // ── Database DDL (pass-through to legacy planner) ─────
            BoundStatement::CreateDatabase { .. } | BoundStatement::DropDatabase { .. } => {
                Err(falcon_common::error::SqlError::Unsupported(
                    "Database DDL uses legacy planner path".into(),
                ))
            }
            // ── DDL ─────────────────────────────────────────────────
            BoundStatement::CreateTable(ct) => Ok(Self::CreateTable {
                schema: ct.schema.clone(),
                if_not_exists: ct.if_not_exists,
            }),
            BoundStatement::DropTable(dt) => Ok(Self::DropTable {
                table_name: dt.table_name.clone(),
                if_exists: dt.if_exists,
            }),
            BoundStatement::AlterTable(alt) => Ok(Self::AlterTable {
                table_name: alt.table_name.clone(),
                ops: alt.ops.clone(),
            }),
            BoundStatement::Truncate { table_name } => Ok(Self::Truncate {
                table_name: table_name.clone(),
            }),
            BoundStatement::CreateIndex {
                index_name,
                table_name,
                column_indices,
                unique,
            } => Ok(Self::CreateIndex {
                index_name: index_name.clone(),
                table_name: table_name.clone(),
                column_indices: column_indices.clone(),
                unique: *unique,
            }),
            BoundStatement::DropIndex { index_name } => Ok(Self::DropIndex {
                index_name: index_name.clone(),
            }),
            BoundStatement::CreateView {
                name,
                query_sql,
                or_replace,
            } => Ok(Self::CreateView {
                name: name.clone(),
                query_sql: query_sql.clone(),
                or_replace: *or_replace,
            }),
            BoundStatement::DropView { name, if_exists } => Ok(Self::DropView {
                name: name.clone(),
                if_exists: *if_exists,
            }),
            BoundStatement::CreateSequence { name, start } => Ok(Self::CreateSequence {
                name: name.clone(),
                start: *start,
            }),
            BoundStatement::DropSequence { name, if_exists } => Ok(Self::DropSequence {
                name: name.clone(),
                if_exists: *if_exists,
            }),

            // ── DML ─────────────────────────────────────────────────
            BoundStatement::Insert(ins) => Ok(Self::Insert {
                table_id: ins.table_id,
                schema: ins.schema.clone(),
                columns: ins.columns.clone(),
                rows: ins.rows.clone(),
                source_select: ins.source_select.clone(),
                returning: ins.returning.clone(),
                on_conflict: ins.on_conflict.clone(),
            }),
            BoundStatement::Update(upd) => Ok(Self::Update {
                table_id: upd.table_id,
                schema: upd.schema.clone(),
                assignments: upd.assignments.clone(),
                filter: upd.filter.clone(),
                returning: upd.returning.clone(),
                from_table: upd.from_table.clone(),
            }),
            BoundStatement::Delete(del) => Ok(Self::Delete {
                table_id: del.table_id,
                schema: del.schema.clone(),
                filter: del.filter.clone(),
                returning: del.returning.clone(),
                using_table: del.using_table.clone(),
            }),

            // ── SELECT ──────────────────────────────────────────────
            BoundStatement::Select(sel) => Ok(Self::from_bound_select(sel)),

            // ── EXPLAIN ──────────────────────────────────────────────
            BoundStatement::Explain(inner) => {
                let inner_plan = Self::from_bound(inner)?;
                Ok(Self::Explain(Box::new(inner_plan)))
            }
            BoundStatement::ExplainAnalyze(inner) => {
                let inner_plan = Self::from_bound(inner)?;
                Ok(Self::ExplainAnalyze(Box::new(inner_plan)))
            }

            // ── Session / Utility ────────────────────────────────────
            BoundStatement::Begin => Ok(Self::Begin),
            BoundStatement::Commit => Ok(Self::Commit),
            BoundStatement::Rollback => Ok(Self::Rollback),
            BoundStatement::ShowTxnStats => Ok(Self::ShowTxnStats),
            BoundStatement::ShowNodeRole => Ok(Self::ShowNodeRole),
            BoundStatement::ShowWalStats => Ok(Self::ShowWalStats),
            BoundStatement::ShowConnections => Ok(Self::ShowConnections),
            BoundStatement::RunGc => Ok(Self::RunGc),
            BoundStatement::Analyze { table_name } => Ok(Self::Analyze {
                table_name: table_name.clone(),
            }),
            BoundStatement::ShowTableStats { table_name } => Ok(Self::ShowTableStats {
                table_name: table_name.clone(),
            }),
            BoundStatement::ShowSequences => Ok(Self::ShowSequences),
            BoundStatement::ShowTenants => Ok(Self::ShowTenants),
            BoundStatement::ShowTenantUsage => Ok(Self::ShowTenantUsage),
            BoundStatement::CreateTenant {
                name,
                max_qps,
                max_storage_bytes,
            } => Ok(Self::CreateTenant {
                name: name.clone(),
                max_qps: *max_qps,
                max_storage_bytes: *max_storage_bytes,
            }),
            BoundStatement::DropTenant { name } => {
                Ok(Self::DropTenant { name: name.clone() })
            }

            // ── COPY ─────────────────────────────────────────────────
            BoundStatement::CopyFrom {
                table_id,
                schema,
                columns,
                csv,
                delimiter,
                header,
                null_string,
                quote,
                escape,
            } => Ok(Self::CopyFrom {
                table_id: *table_id,
                schema: schema.clone(),
                columns: columns.clone(),
                csv: *csv,
                delimiter: *delimiter,
                header: *header,
                null_string: null_string.clone(),
                quote: *quote,
                escape: *escape,
            }),
            BoundStatement::CopyTo {
                table_id,
                schema,
                columns,
                csv,
                delimiter,
                header,
                null_string,
                quote,
                escape,
            } => Ok(Self::CopyTo {
                table_id: *table_id,
                schema: schema.clone(),
                columns: columns.clone(),
                csv: *csv,
                delimiter: *delimiter,
                header: *header,
                null_string: null_string.clone(),
                quote: *quote,
                escape: *escape,
            }),
            BoundStatement::CopyQueryTo {
                query,
                csv,
                delimiter,
                header,
                null_string,
                quote,
                escape,
            } => {
                let inner = Self::from_bound_select(query);
                Ok(Self::CopyQueryTo {
                    query: Box::new(inner),
                    csv: *csv,
                    delimiter: *delimiter,
                    header: *header,
                    null_string: null_string.clone(),
                    quote: *quote,
                    escape: *escape,
                })
            }
            // ── Schema / Role / Grant DDL (pass-through to legacy planner) ──
            BoundStatement::CreateSchema { .. }
            | BoundStatement::DropSchema { .. }
            | BoundStatement::CreateRole { .. }
            | BoundStatement::DropRole { .. }
            | BoundStatement::AlterRole { .. }
            | BoundStatement::Grant { .. }
            | BoundStatement::Revoke { .. }
            | BoundStatement::ShowRoles
            | BoundStatement::ShowSchemas
            | BoundStatement::ShowGrants { .. } => {
                Err(falcon_common::error::SqlError::Unsupported(
                    "Schema/Role/Grant DDL uses legacy planner path".into(),
                ))
            }
        }
    }

    /// Decompose a `BoundSelect` into a tree of logical operators.
    ///
    /// The decomposition follows relational algebra:
    ///   Scan → [CTEs] → [Join] → [Filter] → [Aggregate] → [Distinct] → [Sort] → [Limit] → Project
    fn from_bound_select(sel: &BoundSelect) -> Self {
        // 1. Base scan
        let mut plan = Self::Scan {
            table_id: sel.table_id,
            schema: sel.schema.clone(),
            virtual_rows: sel.virtual_rows.clone(),
        };

        // Preserve table_name for physical plan lowering
        let _table_name = sel.table_name.clone();

        // 2. CTEs
        if !sel.ctes.is_empty() {
            plan = Self::WithCtes {
                ctes: sel.ctes.clone(),
                input: Box::new(plan),
            };
        }

        // 3. Joins
        if !sel.joins.is_empty() {
            plan = Self::MultiJoin {
                base: Box::new(plan),
                joins: sel.joins.clone(),
            };
        }

        // 4. Filter (WHERE)
        if let Some(ref filter) = sel.filter {
            plan = Self::Filter {
                input: Box::new(plan),
                predicate: filter.clone(),
            };
        }

        // 5. Aggregate (GROUP BY + aggregates)
        let has_agg = !sel.group_by.is_empty()
            || !sel.grouping_sets.is_empty()
            || sel
                .projections
                .iter()
                .any(|p| matches!(p, BoundProjection::Aggregate(..)));
        if has_agg {
            plan = Self::Aggregate {
                input: Box::new(plan),
                group_by: sel.group_by.clone(),
                grouping_sets: sel.grouping_sets.clone(),
                projections: sel.projections.clone(),
                visible_count: sel.visible_projection_count,
                having: sel.having.clone(),
            };
        }

        // 6. DISTINCT
        if !matches!(sel.distinct, DistinctMode::None) {
            plan = Self::Distinct {
                input: Box::new(plan),
                mode: sel.distinct.clone(),
            };
        }

        // 7. Sort (ORDER BY)
        if !sel.order_by.is_empty() {
            plan = Self::Sort {
                input: Box::new(plan),
                order_by: sel.order_by.clone(),
            };
        }

        // 8. Limit / Offset
        if sel.limit.is_some() || sel.offset.is_some() {
            plan = Self::Limit {
                input: Box::new(plan),
                limit: sel.limit,
                offset: sel.offset,
            };
        }

        // 9. Projection (if no aggregate already carries projections)
        if !has_agg {
            plan = Self::Project {
                input: Box::new(plan),
                projections: sel.projections.clone(),
                visible_count: sel.visible_projection_count,
            };
        }

        // 10. UNIONs
        for (union_sel, kind, all) in &sel.unions {
            let right = Self::from_bound_select(union_sel);
            plan = Self::SetOp {
                left: Box::new(plan),
                right: Box::new(right),
                kind: *kind,
                all: *all,
            };
        }

        plan
    }
}
