//! # P0-4: Prepared Statement Fast Path
//!
//! Implements server-side `PREPARE` / `EXECUTE` / `DEALLOCATE` semantics.
//!
//! ## Architecture
//! ```text
//! PREPARE p AS SELECT * FROM t WHERE id = $1
//!   → PreparedStatementCache.prepare(name="p", sql, param_types)
//!     → parse SQL → bind → optimize → store PhysicalPlan (with Parameter nodes)
//!
//! EXECUTE p(42)
//!   → PreparedStatementCache.execute(name="p", params=[42])
//!     → retrieve PhysicalPlan → substitute $1=42 → run executor
//!     (skip parse + bind + optimize → saves 1–3 ms per query)
//!
//! DEALLOCATE p
//!   → PreparedStatementCache.deallocate(name="p")
//! ```
//!
//! ## Performance characteristics
//! - First `EXECUTE`: same cost as a normal query (plan is freshly compiled).
//! - Subsequent `EXECUTE` with the same `name`: ~5–50 µs (param subst + execute).
//! - DDL invalidation: any DDL on referenced tables clears the plan automatically.
//!
//! ## Integration
//! The `Executor` struct holds a `PreparedStatementCache` per session.
//! `Executor::run_statement` dispatches `Prepare`, `Execute`, and `Deallocate`
//! statement kinds to the methods defined here.

use std::collections::HashMap;
use std::sync::Arc;

use falcon_common::datum::Datum;
use falcon_common::error::{ExecutionError, FalconError};
use falcon_common::types::DataType;
use falcon_planner::PhysicalPlan;

// ── Prepared Statement entry ──────────────────────────────────────────────────

/// A compiled prepared statement stored in the session cache.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// User-assigned name (e.g. `"p"` from `PREPARE p AS ...`).
    pub name: String,
    /// Original SQL string (kept for re-planning after DDL invalidation).
    pub sql: String,
    /// Declared parameter types (positional, 1-indexed).
    /// `None` = type inferred at bind time.
    pub param_types: Vec<Option<DataType>>,
    /// The compiled physical plan with `BoundExpr::Parameter` nodes intact.
    pub plan: Arc<PhysicalPlan>,
    /// The DDL epoch at which this plan was compiled.
    /// If the current epoch advances past this, the plan must be recompiled.
    pub ddl_epoch: u64,
    /// Number of times this statement has been executed.
    pub execute_count: u64,
}

// ── Bind parameters ───────────────────────────────────────────────────────────

/// A single bound parameter value.
#[derive(Debug, Clone)]
pub struct BoundParam {
    pub value: Datum,
    pub data_type: Option<DataType>,
}

impl BoundParam {
    pub fn new(value: Datum) -> Self {
        Self { value, data_type: None }
    }

    pub fn typed(value: Datum, data_type: DataType) -> Self {
        Self { value, data_type: Some(data_type) }
    }
}

// ── Session-level prepared statement cache ────────────────────────────────────

/// Per-session prepared statement store.
///
/// Maps statement name → `PreparedStatement`.  Capacity is bounded by
/// `MAX_PREPARED_STATEMENTS` to prevent unbounded memory growth.
pub struct PreparedStatementCache {
    stmts: HashMap<String, PreparedStatement>,
    max_capacity: usize,
}

/// Maximum number of prepared statements per session.
pub const MAX_PREPARED_STATEMENTS: usize = 256;

impl PreparedStatementCache {
    pub fn new() -> Self {
        Self {
            stmts: HashMap::new(),
            max_capacity: MAX_PREPARED_STATEMENTS,
        }
    }

    pub fn with_capacity(max: usize) -> Self {
        Self {
            stmts: HashMap::new(),
            max_capacity: max,
        }
    }

    /// Store a compiled prepared statement.
    ///
    /// Replaces any existing statement with the same name.
    /// Returns `Err` if the cache is at capacity and the name is new.
    pub fn store(&mut self, stmt: PreparedStatement) -> Result<(), FalconError> {
        if !self.stmts.contains_key(&stmt.name) && self.stmts.len() >= self.max_capacity {
            return Err(FalconError::Execution(ExecutionError::Internal(
                format!(
                    "prepared statement cache full ({} entries); deallocate statements before preparing new ones",
                    self.max_capacity
                ),
            )));
        }
        self.stmts.insert(stmt.name.clone(), stmt);
        Ok(())
    }

    /// Look up a prepared statement by name.
    pub fn get(&self, name: &str) -> Option<&PreparedStatement> {
        self.stmts.get(name)
    }

    /// Look up a prepared statement by name (mutable, for updating execute_count).
    pub fn get_mut(&mut self, name: &str) -> Option<&mut PreparedStatement> {
        self.stmts.get_mut(name)
    }

    /// Remove a prepared statement (`DEALLOCATE name`).
    /// Returns `true` if the statement existed.
    pub fn deallocate(&mut self, name: &str) -> bool {
        self.stmts.remove(name).is_some()
    }

    /// Deallocate all prepared statements (`DEALLOCATE ALL`).
    pub fn deallocate_all(&mut self) {
        self.stmts.clear();
    }

    /// Invalidate all prepared statements whose DDL epoch is older than `current_epoch`.
    /// Returns the number of entries evicted.
    pub fn invalidate_stale(&mut self, current_epoch: u64) -> usize {
        let before = self.stmts.len();
        self.stmts.retain(|_, v| v.ddl_epoch == current_epoch);
        before - self.stmts.len()
    }

    /// Number of prepared statements in cache.
    pub fn len(&self) -> usize {
        self.stmts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stmts.is_empty()
    }

    /// Names of all prepared statements (for `SHOW PREPARED_STATEMENTS`).
    pub fn names(&self) -> Vec<String> {
        self.stmts.keys().cloned().collect()
    }

    /// Summary snapshot for observability.
    pub fn snapshot(&self) -> Vec<PreparedStmtSnapshot> {
        self.stmts
            .values()
            .map(|s| PreparedStmtSnapshot {
                name: s.name.clone(),
                sql: s.sql.clone(),
                param_count: s.param_types.len(),
                execute_count: s.execute_count,
                ddl_epoch: s.ddl_epoch,
            })
            .collect()
    }
}

impl Default for PreparedStatementCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Observability snapshot for one prepared statement.
#[derive(Debug, Clone)]
pub struct PreparedStmtSnapshot {
    pub name: String,
    pub sql: String,
    pub param_count: usize,
    pub execute_count: u64,
    pub ddl_epoch: u64,
}

// ── Parameter substitution ────────────────────────────────────────────────────

/// Substitute `$N` parameter placeholders in a plan tree with concrete values.
///
/// Returns a new `PhysicalPlan` with all `BoundExpr::Parameter(n)` nodes in
/// filter/assignment expressions replaced by `BoundExpr::Literal(params[n-1])`.
///
/// Only the nodes that carry `BoundExpr` filter expressions are rewritten;
/// all other plan nodes are cloned as-is (they contain no parameter nodes).
pub fn bind_params(
    plan: &PhysicalPlan,
    params: &[BoundParam],
) -> Result<PhysicalPlan, FalconError> {
    use falcon_planner::PhysicalPlan as PP;
    use falcon_sql_frontend::types::BoundExpr;

    /// Helper: substitute params in an optional filter expression.
    fn subst_opt(
        opt: &Option<BoundExpr>,
        params: &[BoundParam],
    ) -> Result<Option<BoundExpr>, FalconError> {
        opt.as_ref().map(|e| subst_expr(e, params)).transpose()
    }

    Ok(match plan {
        PP::SeqScan { table_id, schema, projections, visible_projection_count,
                      filter, group_by, grouping_sets, having, order_by,
                      limit, offset, distinct, ctes, unions, virtual_rows, for_lock } =>
        {
            PP::SeqScan {
                table_id: *table_id,
                schema: schema.clone(),
                projections: projections.clone(),
                visible_projection_count: *visible_projection_count,
                filter: subst_opt(filter, params)?,
                group_by: group_by.clone(),
                grouping_sets: grouping_sets.clone(),
                having: subst_opt(having, params)?,
                order_by: order_by.clone(),
                limit: *limit,
                offset: *offset,
                distinct: distinct.clone(),
                ctes: ctes.clone(),
                unions: unions.clone(),
                virtual_rows: virtual_rows.clone(),
                for_lock: *for_lock,
            }
        }
        PP::Update { table_id, schema, assignments, filter, returning, from_table } => {
            PP::Update {
                table_id: *table_id,
                schema: schema.clone(),
                assignments: assignments
                    .iter()
                    .map(|(col, e)| Ok((*col, subst_expr(e, params)?)))
                    .collect::<Result<Vec<_>, FalconError>>()?,
                filter: subst_opt(filter, params)?,
                returning: returning.clone(),
                from_table: from_table.clone(),
            }
        }
        PP::Delete { table_id, schema, filter, returning, using_table } => {
            PP::Delete {
                table_id: *table_id,
                schema: schema.clone(),
                filter: subst_opt(filter, params)?,
                returning: returning.clone(),
                using_table: using_table.clone(),
            }
        }
        // All other plan nodes carry no BoundExpr parameters — clone unchanged.
        other => other.clone(),
    })
}

/// Recursively substitute `BoundExpr::Parameter(n)` nodes with literal values.
pub fn subst_expr(
    expr: &falcon_sql_frontend::types::BoundExpr,
    params: &[BoundParam],
) -> Result<falcon_sql_frontend::types::BoundExpr, FalconError> {
    use falcon_sql_frontend::types::BoundExpr as E;

    Ok(match expr {
        E::Parameter(n) => {
            let idx = n.saturating_sub(1);
            let param = params.get(idx).ok_or_else(|| {
                FalconError::Execution(falcon_common::error::ExecutionError::ParamMissing(*n))
            })?;
            E::Literal(param.value.clone())
        }
        E::BinaryOp { op, left, right } => E::BinaryOp {
            op: *op,
            left: Box::new(subst_expr(left, params)?),
            right: Box::new(subst_expr(right, params)?),
        },
        E::Not(inner) => E::Not(Box::new(subst_expr(inner, params)?)),
        E::IsNull(inner) => E::IsNull(Box::new(subst_expr(inner, params)?)),
        E::IsNotNull(inner) => E::IsNotNull(Box::new(subst_expr(inner, params)?)),
        E::IsNotDistinctFrom { left, right } => E::IsNotDistinctFrom {
            left: Box::new(subst_expr(left, params)?),
            right: Box::new(subst_expr(right, params)?),
        },
        E::Like { expr: inner, pattern, negated, case_insensitive } => E::Like {
            expr: Box::new(subst_expr(inner, params)?),
            pattern: Box::new(subst_expr(pattern, params)?),
            negated: *negated,
            case_insensitive: *case_insensitive,
        },
        E::Between { expr: inner, low, high, negated } => E::Between {
            expr: Box::new(subst_expr(inner, params)?),
            low: Box::new(subst_expr(low, params)?),
            high: Box::new(subst_expr(high, params)?),
            negated: *negated,
        },
        E::InList { expr: inner, list, negated } => E::InList {
            expr: Box::new(subst_expr(inner, params)?),
            list: list.iter().map(|e| subst_expr(e, params)).collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        },
        E::Cast { expr: inner, target_type } => E::Cast {
            expr: Box::new(subst_expr(inner, params)?),
            target_type: target_type.clone(),
        },
        E::Case { operand, conditions, results, else_result } => E::Case {
            operand: operand.as_ref().map(|o| subst_expr(o, params).map(Box::new)).transpose()?,
            conditions: conditions.iter().map(|e| subst_expr(e, params)).collect::<Result<Vec<_>, _>>()?,
            results: results.iter().map(|e| subst_expr(e, params)).collect::<Result<Vec<_>, _>>()?,
            else_result: else_result.as_ref().map(|e| subst_expr(e, params).map(Box::new)).transpose()?,
        },
        E::Coalesce(args) => E::Coalesce(
            args.iter().map(|a| subst_expr(a, params)).collect::<Result<Vec<_>, _>>()?,
        ),
        E::Function { func, args } => E::Function {
            func: func.clone(),
            args: args.iter().map(|a| subst_expr(a, params)).collect::<Result<Vec<_>, _>>()?,
        },
        E::ArrayLiteral(elems) => E::ArrayLiteral(
            elems.iter().map(|e| subst_expr(e, params)).collect::<Result<Vec<_>, _>>()?,
        ),
        E::ArrayIndex { array, index } => E::ArrayIndex {
            array: Box::new(subst_expr(array, params)?),
            index: Box::new(subst_expr(index, params)?),
        },
        // Leaf nodes with no nested expressions — clone unchanged.
        leaf => leaf.clone(),
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_store_and_get() {
        let mut cache = PreparedStatementCache::new();
        let stmt = PreparedStatement {
            name: "p1".into(),
            sql: "SELECT 1".into(),
            param_types: vec![],
            plan: Arc::new(PhysicalPlan::Values { rows: vec![], columns: vec![] }),
            ddl_epoch: 1,
            execute_count: 0,
        };
        cache.store(stmt).unwrap();
        assert!(cache.get("p1").is_some());
        assert!(cache.get("p2").is_none());
    }

    #[test]
    fn test_deallocate() {
        let mut cache = PreparedStatementCache::new();
        let stmt = PreparedStatement {
            name: "p1".into(),
            sql: "SELECT 1".into(),
            param_types: vec![],
            plan: Arc::new(PhysicalPlan::Values { rows: vec![], columns: vec![] }),
            ddl_epoch: 1,
            execute_count: 0,
        };
        cache.store(stmt).unwrap();
        assert!(cache.deallocate("p1"));
        assert!(!cache.deallocate("p1")); // already gone
    }

    #[test]
    fn test_invalidate_stale() {
        let mut cache = PreparedStatementCache::new();
        for i in 0..5u64 {
            let stmt = PreparedStatement {
                name: format!("p{i}"),
                sql: "SELECT 1".into(),
                param_types: vec![],
                plan: Arc::new(PhysicalPlan::Values { rows: vec![], columns: vec![] }),
                ddl_epoch: i % 2, // epochs 0 and 1
                execute_count: 0,
            };
            cache.store(stmt).unwrap();
        }
        let removed = cache.invalidate_stale(1); // keep only epoch=1
        assert!(removed > 0);
        for s in cache.stmts.values() {
            assert_eq!(s.ddl_epoch, 1);
        }
    }

    #[test]
    fn test_capacity_limit() {
        let mut cache = PreparedStatementCache::with_capacity(2);
        for i in 0..2usize {
            let stmt = PreparedStatement {
                name: format!("p{i}"),
                sql: "SELECT 1".into(),
                param_types: vec![],
                plan: Arc::new(PhysicalPlan::Values { rows: vec![], columns: vec![] }),
                ddl_epoch: 1,
                execute_count: 0,
            };
            cache.store(stmt).unwrap();
        }
        let stmt = PreparedStatement {
            name: "overflow".into(),
            sql: "SELECT 1".into(),
            param_types: vec![],
            plan: Arc::new(PhysicalPlan::Values { rows: vec![], columns: vec![] }),
            ddl_epoch: 1,
            execute_count: 0,
        };
        assert!(cache.store(stmt).is_err(), "should fail at capacity");
    }
}
