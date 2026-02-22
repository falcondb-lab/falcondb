use std::collections::HashSet;

use falcon_common::schema::TableSchema;
use falcon_common::types::{ShardId, TableId};
use falcon_sql_frontend::types::SetOpKind;
use falcon_sql_frontend::types::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannedTxnType {
    Local,
    Global,
}

#[derive(Debug, Clone)]
pub struct TxnRoutingHint {
    pub involved_shards: Vec<ShardId>,
    pub single_shard_proven: bool,
    /// Human-readable explanation of why the routing decision was made.
    /// Examples: "single-table DML on table_id=1", "multi-table join".
    pub inference_reason: String,
}

impl TxnRoutingHint {
    pub fn planned_txn_type(&self) -> PlannedTxnType {
        if self.single_shard_proven && self.involved_shards.len() <= 1 {
            PlannedTxnType::Local
        } else {
            PlannedTxnType::Global
        }
    }
}

/// Physical execution plan — the tree of operators to execute.
/// MVP: near 1:1 mapping from BoundStatement (no real optimization).
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum PhysicalPlan {
    /// DDL: create table
    CreateTable {
        schema: TableSchema,
        if_not_exists: bool,
    },
    /// DDL: drop table
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    /// DDL: alter table
    AlterTable {
        table_name: String,
        ops: Vec<AlterTableOp>,
    },
    /// DML: insert rows (VALUES or SELECT source)
    Insert {
        table_id: TableId,
        schema: TableSchema,
        columns: Vec<usize>,
        rows: Vec<Vec<BoundExpr>>,
        source_select: Option<BoundSelect>,
        returning: Vec<(BoundExpr, String)>,
        on_conflict: Option<OnConflictAction>,
    },
    /// DML: sequential scan + filter + update
    Update {
        table_id: TableId,
        schema: TableSchema,
        assignments: Vec<(usize, BoundExpr)>,
        filter: Option<BoundExpr>,
        returning: Vec<(BoundExpr, String)>,
        from_table: Option<BoundFromTable>,
    },
    /// DML: sequential scan + filter + delete
    Delete {
        table_id: TableId,
        schema: TableSchema,
        filter: Option<BoundExpr>,
        returning: Vec<(BoundExpr, String)>,
        using_table: Option<BoundFromTable>,
    },
    /// Query: scan + filter + project + group + sort + limit
    SeqScan {
        table_id: TableId,
        schema: TableSchema,
        projections: Vec<BoundProjection>,
        visible_projection_count: usize,
        filter: Option<BoundExpr>,
        group_by: Vec<usize>,
        grouping_sets: Vec<Vec<usize>>,
        having: Option<BoundExpr>,
        order_by: Vec<BoundOrderBy>,
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: DistinctMode,
        ctes: Vec<BoundCte>,
        unions: Vec<(BoundSelect, SetOpKind, bool)>,
        virtual_rows: Vec<falcon_common::datum::OwnedRow>,
    },
    /// Query: index scan + residual filter + project + group + sort + limit
    /// Used when the planner detects a `col = literal` predicate on an indexed column.
    IndexScan {
        table_id: TableId,
        schema: TableSchema,
        /// Column index used for the index lookup.
        index_col: usize,
        /// The literal value to look up in the index.
        index_value: BoundExpr,
        projections: Vec<BoundProjection>,
        visible_projection_count: usize,
        /// Residual filter after extracting the index predicate.
        filter: Option<BoundExpr>,
        group_by: Vec<usize>,
        grouping_sets: Vec<Vec<usize>>,
        having: Option<BoundExpr>,
        order_by: Vec<BoundOrderBy>,
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: DistinctMode,
        ctes: Vec<BoundCte>,
        unions: Vec<(BoundSelect, SetOpKind, bool)>,
        virtual_rows: Vec<falcon_common::datum::OwnedRow>,
    },
    /// Query with JOINs: nested loop join + filter + project + sort + limit
    NestedLoopJoin {
        left_table_id: TableId,
        left_schema: TableSchema,
        joins: Vec<BoundJoin>,
        combined_schema: TableSchema,
        projections: Vec<BoundProjection>,
        visible_projection_count: usize,
        filter: Option<BoundExpr>,
        order_by: Vec<BoundOrderBy>,
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: DistinctMode,
        ctes: Vec<BoundCte>,
        unions: Vec<(BoundSelect, SetOpKind, bool)>,
    },
    /// Query with equi-join: hash join (build on right, probe with left)
    /// Used when the join condition is a simple equality on columns.
    HashJoin {
        left_table_id: TableId,
        left_schema: TableSchema,
        joins: Vec<BoundJoin>,
        combined_schema: TableSchema,
        projections: Vec<BoundProjection>,
        visible_projection_count: usize,
        filter: Option<BoundExpr>,
        order_by: Vec<BoundOrderBy>,
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: DistinctMode,
        ctes: Vec<BoundCte>,
        unions: Vec<(BoundSelect, SetOpKind, bool)>,
    },
    /// Query with sort-merge join: both sides sorted on join key, then merged.
    /// Chosen when both sides are large (hash table too big) or when the
    /// output needs to be sorted on the join key anyway.
    MergeSortJoin {
        left_table_id: TableId,
        left_schema: TableSchema,
        joins: Vec<BoundJoin>,
        combined_schema: TableSchema,
        projections: Vec<BoundProjection>,
        visible_projection_count: usize,
        filter: Option<BoundExpr>,
        order_by: Vec<BoundOrderBy>,
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: DistinctMode,
        ctes: Vec<BoundCte>,
        unions: Vec<(BoundSelect, SetOpKind, bool)>,
    },
    /// EXPLAIN: wraps another plan to show its structure
    Explain(Box<PhysicalPlan>),
    /// EXPLAIN ANALYZE: executes the inner plan and shows actual metrics
    ExplainAnalyze(Box<PhysicalPlan>),
    /// TRUNCATE TABLE
    Truncate {
        table_name: String,
    },
    /// CREATE INDEX
    CreateIndex {
        index_name: String,
        table_name: String,
        column_indices: Vec<usize>,
        unique: bool,
    },
    /// DROP INDEX
    DropIndex {
        index_name: String,
    },
    /// CREATE VIEW
    CreateView {
        name: String,
        query_sql: String,
        or_replace: bool,
    },
    /// DROP VIEW
    DropView {
        name: String,
        if_exists: bool,
    },
    /// Transaction control
    Begin,
    Commit,
    Rollback,
    /// Observability: SHOW falcon_txn_stats
    ShowTxnStats,
    /// Observability: SHOW falcon.node_role
    ShowNodeRole,
    /// Observability: SHOW falcon.wal_stats
    ShowWalStats,
    /// Observability: SHOW falcon.connections
    ShowConnections,
    /// GC: SHOW falcon_gc
    RunGc,
    /// ANALYZE table_name — collect table statistics
    Analyze {
        table_name: String,
    },
    /// SHOW falcon.table_stats [table_name]
    ShowTableStats {
        table_name: Option<String>,
    },
    /// CREATE SEQUENCE
    CreateSequence {
        name: String,
        start: i64,
    },
    /// DROP SEQUENCE
    DropSequence {
        name: String,
        if_exists: bool,
    },
    /// SHOW falcon.sequences
    ShowSequences,
    /// Multi-tenancy: SHOW falcon.tenants
    ShowTenants,
    /// Multi-tenancy: SHOW falcon.tenant_usage
    ShowTenantUsage,
    /// Multi-tenancy: CREATE TENANT name
    CreateTenant {
        name: String,
        max_qps: u64,
        max_storage_bytes: u64,
    },
    /// Multi-tenancy: DROP TENANT name
    DropTenant {
        name: String,
    },
    /// COPY table FROM STDIN — bulk import
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
    /// COPY table TO STDOUT — bulk export
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
    /// COPY (query) TO STDOUT — export query results
    CopyQueryTo {
        query: Box<PhysicalPlan>,
        csv: bool,
        delimiter: char,
        header: bool,
        null_string: String,
        quote: char,
        escape: char,
    },
    /// Distributed plan: wraps a local subplan for scatter/gather execution
    /// across multiple shards. The executor dispatches this to the
    /// DistributedExecutor for parallel execution + merge.
    DistPlan {
        /// The local subplan to execute on each shard.
        subplan: Box<PhysicalPlan>,
        /// Target shard IDs for scatter.
        target_shards: Vec<ShardId>,
        /// How to merge results from shards.
        gather: DistGather,
    },
}

/// How to gather/merge results from a distributed scatter.
#[derive(Debug, Clone)]
pub enum DistGather {
    /// Simple union of all shard results. Optional post-gather dedup, offset, and limit.
    Union {
        distinct: bool,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    /// Merge-sort by columns, then apply optional offset + limit.
    MergeSortLimit {
        sort_columns: Vec<(usize, bool)>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    /// Two-phase aggregation: partial agg per shard, final merge.
    /// HAVING is stripped from the subplan and applied post-merge.
    /// AVG is decomposed: subplan computes SUM + hidden COUNT; gather merges
    /// both then computes AVG = SUM/COUNT via `avg_fixups`.
    TwoPhaseAgg {
        group_by_indices: Vec<usize>,
        agg_merges: Vec<DistAggMerge>,
        having: Option<BoundExpr>,
        /// (sum_output_idx, count_hidden_idx) — post-merge: row[sum_idx] /= row[count_idx]
        avg_fixups: Vec<(usize, usize)>,
        /// Number of visible output columns (before hidden COUNT cols for AVG).
        visible_columns: usize,
        /// Post-merge sort: (column_idx, ascending). Applied after merge + HAVING.
        order_by: Vec<(usize, bool)>,
        /// Post-merge limit. Applied after sort.
        limit: Option<usize>,
        /// Post-merge offset. Applied after sort, before limit.
        offset: Option<usize>,
    },
}

/// How to merge a single aggregate column across shards (mirrors AggMerge in falcon_cluster).
#[derive(Debug, Clone)]
pub enum DistAggMerge {
    Sum(usize),
    Count(usize),
    Min(usize),
    Max(usize),
    BoolAnd(usize),
    BoolOr(usize),
    StringAgg(usize, String),
    ArrayAgg(usize),
    /// COUNT(DISTINCT col): each shard computes ARRAY_AGG(DISTINCT col),
    /// gather concatenates arrays, deduplicates globally, counts unique values.
    CountDistinct(usize),
    /// SUM(DISTINCT col): each shard computes ARRAY_AGG(DISTINCT col),
    /// gather concatenates arrays, deduplicates globally, sums unique values.
    SumDistinct(usize),
    /// AVG(DISTINCT col): each shard computes ARRAY_AGG(DISTINCT col),
    /// gather concatenates arrays, deduplicates globally, computes sum/count as Float64.
    AvgDistinct(usize),
    /// STRING_AGG(DISTINCT col, sep): each shard computes ARRAY_AGG(DISTINCT col),
    /// gather concatenates arrays, deduplicates globally, joins with separator.
    StringAggDistinct(usize, String),
    /// ARRAY_AGG(DISTINCT col): each shard computes ARRAY_AGG(DISTINCT col),
    /// gather concatenates arrays, deduplicates globally, returns unique array.
    ArrayAggDistinct(usize),
}

impl std::fmt::Display for DistAggMerge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistAggMerge::Sum(i) => write!(f, "SUM(col{})", i),
            DistAggMerge::Count(i) => write!(f, "COUNT(col{})", i),
            DistAggMerge::Min(i) => write!(f, "MIN(col{})", i),
            DistAggMerge::Max(i) => write!(f, "MAX(col{})", i),
            DistAggMerge::BoolAnd(i) => write!(f, "BOOL_AND(col{})", i),
            DistAggMerge::BoolOr(i) => write!(f, "BOOL_OR(col{})", i),
            DistAggMerge::StringAgg(i, sep) => write!(f, "STRING_AGG(col{}, '{}')", i, sep),
            DistAggMerge::ArrayAgg(i) => write!(f, "ARRAY_AGG(col{})", i),
            DistAggMerge::CountDistinct(i) => write!(f, "COUNT(DISTINCT col{}) [collect-dedup]", i),
            DistAggMerge::SumDistinct(i) => write!(f, "SUM(DISTINCT col{}) [collect-dedup]", i),
            DistAggMerge::AvgDistinct(i) => write!(f, "AVG(DISTINCT col{}) [collect-dedup]", i),
            DistAggMerge::StringAggDistinct(i, sep) => write!(
                f,
                "STRING_AGG(DISTINCT col{}, '{}') [collect-dedup]",
                i, sep
            ),
            DistAggMerge::ArrayAggDistinct(i) => {
                write!(f, "ARRAY_AGG(DISTINCT col{}) [collect-dedup]", i)
            }
        }
    }
}

impl PhysicalPlan {
    /// Infer a routing hint for transaction classification.
    ///
    /// Conservative policy:
    /// - Only simple single-table plans are considered "single_shard_proven".
    /// - Any multi-table / join / union / CTE shape is treated as not proven.
    pub fn routing_hint(&self) -> TxnRoutingHint {
        let mut table_ids = HashSet::new();
        let mut single_shard_proven = false;
        let reason;

        match self {
            PhysicalPlan::Insert {
                table_id,
                source_select,
                ..
            } => {
                table_ids.insert(*table_id);
                if let Some(sel) = source_select {
                    collect_select_table_ids(sel, &mut table_ids);
                    single_shard_proven = false;
                    reason = format!("INSERT...SELECT on table_id={}, multi-source", table_id.0);
                } else {
                    single_shard_proven = true;
                    reason = format!("single-table INSERT on table_id={}", table_id.0);
                }
            }
            PhysicalPlan::Update { table_id, .. } => {
                table_ids.insert(*table_id);
                single_shard_proven = true;
                reason = format!("single-table UPDATE on table_id={}", table_id.0);
            }
            PhysicalPlan::Delete { table_id, .. } => {
                table_ids.insert(*table_id);
                single_shard_proven = true;
                reason = format!("single-table DELETE on table_id={}", table_id.0);
            }
            PhysicalPlan::SeqScan {
                table_id,
                unions,
                ctes,
                ..
            } => {
                table_ids.insert(*table_id);
                single_shard_proven = unions.is_empty() && ctes.is_empty();

                for cte in ctes {
                    collect_select_table_ids(&cte.select, &mut table_ids);
                }
                for (sel, _, _) in unions {
                    collect_select_table_ids(sel, &mut table_ids);
                }
                if single_shard_proven {
                    reason = format!("single-table SeqScan on table_id={}", table_id.0);
                } else {
                    reason = format!("SeqScan on table_id={} with unions/CTEs", table_id.0);
                }
            }
            PhysicalPlan::NestedLoopJoin {
                left_table_id,
                joins,
                ctes,
                unions,
                ..
            }
            | PhysicalPlan::HashJoin {
                left_table_id,
                joins,
                ctes,
                unions,
                ..
            }
            | PhysicalPlan::MergeSortJoin {
                left_table_id,
                joins,
                ctes,
                unions,
                ..
            } => {
                table_ids.insert(*left_table_id);
                for join in joins {
                    table_ids.insert(join.right_table_id);
                }
                for cte in ctes {
                    collect_select_table_ids(&cte.select, &mut table_ids);
                }
                for (sel, _, _) in unions {
                    collect_select_table_ids(sel, &mut table_ids);
                }
                single_shard_proven = false;
                reason = format!("multi-table join ({} tables)", table_ids.len());
            }
            PhysicalPlan::Explain(inner) | PhysicalPlan::ExplainAnalyze(inner) => {
                return inner.routing_hint();
            }
            PhysicalPlan::DistPlan { target_shards, .. } => {
                return TxnRoutingHint {
                    involved_shards: target_shards.clone(),
                    single_shard_proven: target_shards.len() <= 1,
                    inference_reason: format!(
                        "DistPlan with {} target shards",
                        target_shards.len()
                    ),
                };
            }
            _ => {
                reason = "DDL or non-DML plan".into();
            }
        }

        let mut involved_shards = HashSet::new();
        for table_id in table_ids {
            involved_shards.insert(shard_for_table(table_id));
        }

        let mut involved_shards: Vec<ShardId> = involved_shards.into_iter().collect();
        involved_shards.sort_by_key(|s| s.0);

        TxnRoutingHint {
            involved_shards,
            single_shard_proven,
            inference_reason: reason,
        }
    }
}

fn collect_select_table_ids(select: &BoundSelect, out: &mut HashSet<TableId>) {
    out.insert(select.table_id);
    for join in &select.joins {
        out.insert(join.right_table_id);
    }
    for cte in &select.ctes {
        collect_select_table_ids(&cte.select, out);
    }
    for (union_sel, _, _) in &select.unions {
        collect_select_table_ids(union_sel, out);
    }
}

fn shard_for_table(table_id: TableId) -> ShardId {
    // Deterministic planner-side affinity hint.
    // Actual distributed routing can replace this with cluster shard-map lookup.
    ShardId(table_id.0 % 1024)
}
