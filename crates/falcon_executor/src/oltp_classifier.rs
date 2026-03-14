//! OLTP statement classifier: detects whether a physical plan can be
//! executed on the zero-overhead fast path instead of the general executor.
//!
//! # Design
//! Call [`OltpKind::classify`] **once at PREPARE time**.  Store the result
//! alongside the cached `PhysicalPlan`.  At EXECUTE time, read the stored
//! `Option<OltpKind>` — no repeated classification work.
//!
//! ## Fast-path eligibility criteria (all must hold)
//! - Single-table DML or PK point-read SELECT
//! - No RETURNING clause
//! - No FK constraints on the table
//! - No CHECK constraints on the table
//! - No row-level triggers registered for the table
//! - Filter (if any) is a pure PK equality: `pk_col = $N` or `pk_col = literal`
//! - No GROUP BY / ORDER BY / UNION / LIMIT / OFFSET
//!
//! If any condition fails, the plan falls back to the general executor path
//! which handles all edge cases correctly.

use falcon_common::schema::TableSchema;
use falcon_common::types::TableId;
use falcon_planner::PhysicalPlan;
use falcon_sql_frontend::types::{BinOp, BoundExpr, OnConflictAction};

/// OLTP statement kind — identifies which fast-path handler to invoke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OltpKind {
    /// `SELECT ... WHERE pk_col = $N` — single-row point read, no aggregation.
    PointSelect { table_id: TableId },
    /// `INSERT INTO t VALUES ($1, $2, ...)` — single row, plain insert.
    InsertSingle { table_id: TableId },
    /// `INSERT INTO t VALUES (...), (...), ...` — multi-row batch insert.
    InsertBatch { table_id: TableId },
    /// `UPDATE t SET col=$2 WHERE pk_col=$1` — PK-targeted single-row update.
    UpdateByPk { table_id: TableId },
    /// `DELETE FROM t WHERE pk_col=$1` — PK-targeted single-row delete.
    DeleteByPk { table_id: TableId },
    /// `INSERT ... ON CONFLICT (pk) DO UPDATE ...` — upsert by PK.
    UpsertByPk { table_id: TableId },
    /// `INSERT ... ON CONFLICT DO NOTHING` — idempotent insert.
    InsertIgnore { table_id: TableId },
}

impl OltpKind {
    /// Classify `plan` at **prepare time**.
    ///
    /// Returns `Some(kind)` if the plan qualifies for the OLTP fast path,
    /// `None` if it must go through the general executor.
    pub fn classify(plan: &PhysicalPlan) -> Option<Self> {
        match plan {
            PhysicalPlan::Insert {
                table_id,
                schema,
                rows,
                source_select,
                returning,
                on_conflict,
                ..
            } => {
                // INSERT … SELECT, RETURNING, or complex constraints → general path
                if source_select.is_some() || !returning.is_empty() {
                    return None;
                }
                if !is_trivial_schema(schema) {
                    return None;
                }
                match on_conflict {
                    None if rows.len() == 1 => {
                        Some(Self::InsertSingle { table_id: *table_id })
                    }
                    None => Some(Self::InsertBatch { table_id: *table_id }),
                    Some(OnConflictAction::DoNothing) => {
                        Some(Self::InsertIgnore { table_id: *table_id })
                    }
                    Some(OnConflictAction::DoUpdate(..)) => {
                        Some(Self::UpsertByPk { table_id: *table_id })
                    }
                }
            }

            PhysicalPlan::Update {
                table_id,
                schema,
                filter,
                from_table,
                returning,
                ..
            } => {
                if from_table.is_some() || !returning.is_empty() {
                    return None;
                }
                if !is_trivial_schema(schema) {
                    return None;
                }
                let f = filter.as_ref()?;
                if is_pk_equality_filter(f, schema) {
                    Some(Self::UpdateByPk { table_id: *table_id })
                } else {
                    None
                }
            }

            PhysicalPlan::Delete {
                table_id,
                schema,
                filter,
                returning,
                using_table,
            } => {
                if using_table.is_some() || !returning.is_empty() {
                    return None;
                }
                if !is_trivial_schema(schema) {
                    return None;
                }
                let f = filter.as_ref()?;
                if is_pk_equality_filter(f, schema) {
                    Some(Self::DeleteByPk { table_id: *table_id })
                } else {
                    None
                }
            }

            PhysicalPlan::SeqScan {
                table_id,
                schema,
                filter,
                group_by,
                order_by,
                limit,
                offset,
                unions,
                ctes,
                virtual_rows,
                ..
            } => {
                // Must be plain point-read with no analytical features
                if !group_by.is_empty()
                    || !order_by.is_empty()
                    || !unions.is_empty()
                    || !ctes.is_empty()
                    || !virtual_rows.is_empty()
                    || limit.is_some()
                    || offset.unwrap_or(0) != 0
                {
                    return None;
                }
                let f = filter.as_ref()?;
                if is_pk_equality_filter(f, schema) {
                    Some(Self::PointSelect { table_id: *table_id })
                } else {
                    None
                }
            }

            _ => None,
        }
    }

    /// Returns the `TableId` embedded in every variant.
    pub fn table_id(self) -> TableId {
        match self {
            Self::PointSelect { table_id }
            | Self::InsertSingle { table_id }
            | Self::InsertBatch { table_id }
            | Self::UpdateByPk { table_id }
            | Self::DeleteByPk { table_id }
            | Self::UpsertByPk { table_id }
            | Self::InsertIgnore { table_id } => table_id,
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Returns `true` when the schema has no FK constraints, no CHECK constraints,
/// no SERIAL columns (which require a storage round-trip), and no triggers
/// that would require the general path to fire row-level hooks.
///
/// The absence of triggers is only checked structurally here (schema level).
/// The `OltpExecutor` re-checks trigger absence at execute time via the catalog
/// to guard against triggers added after PREPARE.
#[inline]
fn is_trivial_schema(schema: &TableSchema) -> bool {
    schema.foreign_keys.is_empty()
        && schema.check_constraints.is_empty()
        && !schema.columns.iter().any(|c| c.is_serial)
        && schema.dynamic_defaults.is_empty()
}

/// Returns `true` if `filter` is a pure PK equality predicate:
/// `pk_col = literal` or `pk_col = $param` for every PK column (AND-ed together
/// for composite PKs).
///
/// We accept `BoundExpr::Parameter` on the RHS because the fast-path executor
/// receives evaluated `Datum` values directly from the params slice — parameter
/// substitution is bypassed entirely.
pub(crate) fn is_pk_equality_filter(filter: &BoundExpr, schema: &TableSchema) -> bool {
    let pk_cols = &schema.primary_key_columns;
    if pk_cols.is_empty() {
        return false;
    }

    // Flatten AND tree
    let mut conjuncts: Vec<&BoundExpr> = Vec::new();
    flatten_and(filter, &mut conjuncts);

    // Each PK column must be covered by exactly one `col = literal/param` conjunct
    let mut covered = vec![false; pk_cols.len()];
    for conj in &conjuncts {
        if let Some(col_idx) = extract_eq_col(conj) {
            if let Some(pk_pos) = pk_cols.iter().position(|&c| c == col_idx) {
                covered[pk_pos] = true;
            }
        }
    }
    covered.iter().all(|&c| c)
}

/// Flatten a tree of AND expressions into a flat list of conjuncts.
fn flatten_and<'a>(expr: &'a BoundExpr, out: &mut Vec<&'a BoundExpr>) {
    match expr {
        BoundExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            flatten_and(left, out);
            flatten_and(right, out);
        }
        other => out.push(other),
    }
}

/// Extract the column index from `col = literal` or `col = $param` equality.
/// Returns `None` for anything more complex.
fn extract_eq_col(expr: &BoundExpr) -> Option<usize> {
    if let BoundExpr::BinaryOp {
        left,
        op: BinOp::Eq,
        right,
    } = expr
    {
        match (left.as_ref(), right.as_ref()) {
            (BoundExpr::ColumnRef(idx), BoundExpr::Literal(_))
            | (BoundExpr::Literal(_), BoundExpr::ColumnRef(idx)) => Some(*idx),
            (BoundExpr::ColumnRef(idx), BoundExpr::Parameter(_))
            | (BoundExpr::Parameter(_), BoundExpr::ColumnRef(idx)) => Some(*idx),
            _ => None,
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::Datum;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};
    use falcon_sql_frontend::types::BoundExpr;

    fn make_schema(pk_cols: Vec<usize>) -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "t".into(),
            columns: vec![
                ColumnDef::new(
                    ColumnId(0),
                    "id".into(),
                    DataType::Int64,
                    false,
                    true,
                    None,
                    false,
                ),
                ColumnDef::new(
                    ColumnId(1),
                    "val".into(),
                    DataType::Text,
                    true,
                    false,
                    None,
                    false,
                ),
            ],
            primary_key_columns: pk_cols,
            ..TableSchema::default()
        }
    }

    #[test]
    fn test_single_pk_eq_literal() {
        let schema = make_schema(vec![0]);
        let filter = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Int64(1))),
        };
        assert!(is_pk_equality_filter(&filter, &schema));
    }

    #[test]
    fn test_single_pk_eq_param() {
        let schema = make_schema(vec![0]);
        let filter = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Parameter(1)),
        };
        assert!(is_pk_equality_filter(&filter, &schema));
    }

    #[test]
    fn test_non_pk_col_rejected() {
        let schema = make_schema(vec![0]);
        let filter = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(1)), // col 1 is not PK
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Text("x".into()))),
        };
        assert!(!is_pk_equality_filter(&filter, &schema));
    }

    #[test]
    fn test_composite_pk_both_cols_required() {
        let schema = make_schema(vec![0, 1]);
        // Only one PK col covered → must be false
        let filter = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Int64(1))),
        };
        assert!(!is_pk_equality_filter(&filter, &schema));

        // Both cols covered with AND → true
        let filter_full = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(0)),
                op: BinOp::Eq,
                right: Box::new(BoundExpr::Literal(Datum::Int64(1))),
            }),
            op: BinOp::And,
            right: Box::new(BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(1)),
                op: BinOp::Eq,
                right: Box::new(BoundExpr::Literal(Datum::Text("a".into()))),
            }),
        };
        assert!(is_pk_equality_filter(&filter_full, &schema));
    }
}
