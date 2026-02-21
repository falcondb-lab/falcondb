use std::collections::HashMap;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::{FalconError, ExecutionError};
use falcon_common::schema::{Catalog, TableSchema};
use falcon_sql_frontend::binder::Binder;
use falcon_sql_frontend::types::*;
use falcon_txn::TxnHandle;

use crate::expr_engine::ExprEngine;
use crate::executor::{ExecutionResult, Executor};

impl Executor {
    /// Execute a BoundSelect as an inner subquery, returning the result rows.
    pub(crate) fn exec_subquery(
        &self,
        sub: &BoundSelect,
        txn: &TxnHandle,
    ) -> Result<Vec<OwnedRow>, FalconError> {
        let plan = falcon_planner::Planner::plan(
            &falcon_sql_frontend::types::BoundStatement::Select(sub.clone()),
        )?;
        let result = self.execute(&plan, Some(txn))?;
        match result {
            ExecutionResult::Query { rows, .. } => Ok(rows),
            _ => Ok(vec![]),
        }
    }

    /// Walk an expression tree and replace subquery nodes with their computed results.
    /// This handles uncorrelated subqueries by executing them once and substituting literals.
    pub fn materialize_subqueries(
        &self,
        expr: &BoundExpr,
        txn: &TxnHandle,
    ) -> Result<BoundExpr, FalconError> {
        match expr {
            BoundExpr::ScalarSubquery(sub) => {
                // If the subquery is correlated, leave it for per-row materialization
                if Self::bound_select_has_outer_ref(sub) {
                    return Ok(expr.clone());
                }
                let rows = self.exec_subquery(sub, txn)?;
                let val = rows
                    .first()
                    .and_then(|r| r.get(0).cloned())
                    .unwrap_or(Datum::Null);
                Ok(BoundExpr::Literal(val))
            }
            BoundExpr::InSubquery { expr: inner_expr, subquery, negated } => {
                if Self::bound_select_has_outer_ref(subquery) || Self::expr_has_outer_ref(inner_expr) {
                    return Ok(expr.clone());
                }
                let mat_expr = self.materialize_subqueries(inner_expr, txn)?;
                let rows = self.exec_subquery(subquery, txn)?;
                let list: Vec<BoundExpr> = rows
                    .iter()
                    .filter_map(|r| r.get(0).cloned())
                    .map(BoundExpr::Literal)
                    .collect();
                Ok(BoundExpr::InList {
                    expr: Box::new(mat_expr),
                    list,
                    negated: *negated,
                })
            }
            BoundExpr::Exists { subquery, negated } => {
                if Self::bound_select_has_outer_ref(subquery) {
                    return Ok(expr.clone());
                }
                let rows = self.exec_subquery(subquery, txn)?;
                let exists = !rows.is_empty();
                let result = if *negated { !exists } else { exists };
                Ok(BoundExpr::Literal(Datum::Boolean(result)))
            }
            // Recurse into compound expressions
            BoundExpr::BinaryOp { left, op, right } => {
                let l = self.materialize_subqueries(left, txn)?;
                let r = self.materialize_subqueries(right, txn)?;
                Ok(BoundExpr::BinaryOp {
                    left: Box::new(l),
                    op: *op,
                    right: Box::new(r),
                })
            }
            BoundExpr::Not(inner) => {
                let m = self.materialize_subqueries(inner, txn)?;
                Ok(BoundExpr::Not(Box::new(m)))
            }
            BoundExpr::IsNull(inner) => {
                let m = self.materialize_subqueries(inner, txn)?;
                Ok(BoundExpr::IsNull(Box::new(m)))
            }
            BoundExpr::IsNotNull(inner) => {
                let m = self.materialize_subqueries(inner, txn)?;
                Ok(BoundExpr::IsNotNull(Box::new(m)))
            }
            BoundExpr::Like { expr: inner, pattern, negated, case_insensitive } => {
                let m = self.materialize_subqueries(inner, txn)?;
                let mp = self.materialize_subqueries(pattern, txn)?;
                Ok(BoundExpr::Like {
                    expr: Box::new(m),
                    pattern: Box::new(mp),
                    negated: *negated,
                    case_insensitive: *case_insensitive,
                })
            }
            BoundExpr::Between { expr: inner, low, high, negated } => {
                let me = self.materialize_subqueries(inner, txn)?;
                let ml = self.materialize_subqueries(low, txn)?;
                let mh = self.materialize_subqueries(high, txn)?;
                Ok(BoundExpr::Between {
                    expr: Box::new(me),
                    low: Box::new(ml),
                    high: Box::new(mh),
                    negated: *negated,
                })
            }
            BoundExpr::InList { expr: inner, list, negated } => {
                let me = self.materialize_subqueries(inner, txn)?;
                let ml: Vec<BoundExpr> = list
                    .iter()
                    .map(|e| self.materialize_subqueries(e, txn))
                    .collect::<Result<_, _>>()?;
                Ok(BoundExpr::InList {
                    expr: Box::new(me),
                    list: ml,
                    negated: *negated,
                })
            }
            BoundExpr::Cast { expr: inner, target_type } => {
                let m = self.materialize_subqueries(inner, txn)?;
                Ok(BoundExpr::Cast {
                    expr: Box::new(m),
                    target_type: target_type.clone(),
                })
            }
            BoundExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mo = operand
                    .as_ref()
                    .map(|e| self.materialize_subqueries(e, txn).map(Box::new))
                    .transpose()?;
                let mc: Vec<BoundExpr> = conditions
                    .iter()
                    .map(|e| self.materialize_subqueries(e, txn))
                    .collect::<Result<_, _>>()?;
                let mr: Vec<BoundExpr> = results
                    .iter()
                    .map(|e| self.materialize_subqueries(e, txn))
                    .collect::<Result<_, _>>()?;
                let me = else_result
                    .as_ref()
                    .map(|e| self.materialize_subqueries(e, txn).map(Box::new))
                    .transpose()?;
                Ok(BoundExpr::Case {
                    operand: mo,
                    conditions: mc,
                    results: mr,
                    else_result: me,
                })
            }
            BoundExpr::Coalesce(args) => {
                let ma: Vec<BoundExpr> = args
                    .iter()
                    .map(|e| self.materialize_subqueries(e, txn))
                    .collect::<Result<_, _>>()?;
                Ok(BoundExpr::Coalesce(ma))
            }
            BoundExpr::Function { func, args } => {
                let ma: Vec<BoundExpr> = args
                    .iter()
                    .map(|e| self.materialize_subqueries(e, txn))
                    .collect::<Result<_, _>>()?;
                Ok(BoundExpr::Function {
                    func: func.clone(),
                    args: ma,
                })
            }
            // AggregateExpr — pass through (evaluated later in HAVING)
            BoundExpr::AggregateExpr { func, arg, distinct } => {
                let ma = arg
                    .as_ref()
                    .map(|e| self.materialize_subqueries(e, txn).map(Box::new))
                    .transpose()?;
                Ok(BoundExpr::AggregateExpr {
                    func: func.clone(),
                    arg: ma,
                    distinct: *distinct,
                })
            }
            BoundExpr::ArrayLiteral(elems) => {
                let me: Vec<BoundExpr> = elems
                    .iter()
                    .map(|e| self.materialize_subqueries(e, txn))
                    .collect::<Result<_, _>>()?;
                Ok(BoundExpr::ArrayLiteral(me))
            }
            BoundExpr::ArrayIndex { array, index } => {
                let ma = self.materialize_subqueries(array, txn)?;
                let mi = self.materialize_subqueries(index, txn)?;
                Ok(BoundExpr::ArrayIndex {
                    array: Box::new(ma),
                    index: Box::new(mi),
                })
            }
            BoundExpr::IsNotDistinctFrom { left, right } => {
                let ml = self.materialize_subqueries(left, txn)?;
                let mr = self.materialize_subqueries(right, txn)?;
                Ok(BoundExpr::IsNotDistinctFrom {
                    left: Box::new(ml),
                    right: Box::new(mr),
                })
            }
            BoundExpr::AnyOp { left, compare_op, right } => {
                let ml = self.materialize_subqueries(left, txn)?;
                let mr = self.materialize_subqueries(right, txn)?;
                Ok(BoundExpr::AnyOp {
                    left: Box::new(ml),
                    compare_op: *compare_op,
                    right: Box::new(mr),
                })
            }
            BoundExpr::AllOp { left, compare_op, right } => {
                let ml = self.materialize_subqueries(left, txn)?;
                let mr = self.materialize_subqueries(right, txn)?;
                Ok(BoundExpr::AllOp {
                    left: Box::new(ml),
                    compare_op: *compare_op,
                    right: Box::new(mr),
                })
            }
            BoundExpr::ArraySlice { array, lower, upper } => {
                let ma = self.materialize_subqueries(array, txn)?;
                let ml = lower.as_ref()
                    .map(|e| self.materialize_subqueries(e, txn).map(Box::new))
                    .transpose()?;
                let mu = upper.as_ref()
                    .map(|e| self.materialize_subqueries(e, txn).map(Box::new))
                    .transpose()?;
                Ok(BoundExpr::ArraySlice {
                    array: Box::new(ma),
                    lower: ml,
                    upper: mu,
                })
            }
            // Leaf nodes — no subqueries
            BoundExpr::Literal(_) | BoundExpr::ColumnRef(_) | BoundExpr::OuterColumnRef(_)
            | BoundExpr::SequenceNextval(_) | BoundExpr::SequenceCurrval(_) | BoundExpr::SequenceSetval(_, _)
            | BoundExpr::Parameter(_) | BoundExpr::Grouping(_) => Ok(expr.clone()),
        }
    }

    /// Materialize subqueries in an optional filter expression.
    pub fn materialize_filter(
        &self,
        filter: Option<&BoundExpr>,
        txn: &TxnHandle,
    ) -> Result<Option<BoundExpr>, FalconError> {
        match filter {
            Some(f) => Ok(Some(self.materialize_subqueries(f, txn)?)),
            None => Ok(None),
        }
    }

    /// Check if a BoundExpr tree contains any OuterColumnRef nodes (correlated).
    pub fn expr_has_outer_ref(expr: &BoundExpr) -> bool {
        match expr {
            BoundExpr::OuterColumnRef(_) => true,
            BoundExpr::Literal(_) | BoundExpr::ColumnRef(_) => false,
            BoundExpr::BinaryOp { left, right, .. } => {
                Self::expr_has_outer_ref(left) || Self::expr_has_outer_ref(right)
            }
            BoundExpr::Not(inner) | BoundExpr::IsNull(inner) | BoundExpr::IsNotNull(inner)
            | BoundExpr::Cast { expr: inner, .. } => Self::expr_has_outer_ref(inner),
            BoundExpr::Like { expr, pattern, .. } => {
                Self::expr_has_outer_ref(expr) || Self::expr_has_outer_ref(pattern)
            }
            BoundExpr::Between { expr, low, high, .. } => {
                Self::expr_has_outer_ref(expr) || Self::expr_has_outer_ref(low) || Self::expr_has_outer_ref(high)
            }
            BoundExpr::InList { expr, list, .. } => {
                Self::expr_has_outer_ref(expr) || list.iter().any(Self::expr_has_outer_ref)
            }
            BoundExpr::Case { operand, conditions, results, else_result } => {
                operand.as_deref().is_some_and(Self::expr_has_outer_ref)
                    || conditions.iter().any(Self::expr_has_outer_ref)
                    || results.iter().any(Self::expr_has_outer_ref)
                    || else_result.as_deref().is_some_and(Self::expr_has_outer_ref)
            }
            BoundExpr::Coalesce(args) | BoundExpr::ArrayLiteral(args) => {
                args.iter().any(Self::expr_has_outer_ref)
            }
            BoundExpr::Function { args, .. } => args.iter().any(Self::expr_has_outer_ref),
            BoundExpr::AggregateExpr { arg, .. } => {
                arg.as_deref().is_some_and(Self::expr_has_outer_ref)
            }
            BoundExpr::ArrayIndex { array, index } => {
                Self::expr_has_outer_ref(array) || Self::expr_has_outer_ref(index)
            }
            BoundExpr::IsNotDistinctFrom { left, right } => {
                Self::expr_has_outer_ref(left) || Self::expr_has_outer_ref(right)
            }
            BoundExpr::AnyOp { left, right, .. } | BoundExpr::AllOp { left, right, .. } => {
                Self::expr_has_outer_ref(left) || Self::expr_has_outer_ref(right)
            }
            BoundExpr::ArraySlice { array, lower, upper } => {
                Self::expr_has_outer_ref(array)
                    || lower.as_deref().is_some_and(Self::expr_has_outer_ref)
                    || upper.as_deref().is_some_and(Self::expr_has_outer_ref)
            }
            BoundExpr::ScalarSubquery(sub) => Self::bound_select_has_outer_ref(sub),
            BoundExpr::InSubquery { expr, subquery, .. } => {
                Self::expr_has_outer_ref(expr) || Self::bound_select_has_outer_ref(subquery)
            }
            BoundExpr::Exists { subquery, .. } => Self::bound_select_has_outer_ref(subquery),
            BoundExpr::SequenceNextval(_) | BoundExpr::SequenceCurrval(_) | BoundExpr::SequenceSetval(_, _)
            | BoundExpr::Parameter(_) | BoundExpr::Grouping(_) => false,
        }
    }

    /// Check if a BoundSelect contains any OuterColumnRef in its filter or projections.
    fn bound_select_has_outer_ref(sel: &BoundSelect) -> bool {
        if let Some(ref f) = sel.filter {
            if Self::expr_has_outer_ref(f) { return true; }
        }
        if let Some(ref h) = sel.having {
            if Self::expr_has_outer_ref(h) { return true; }
        }
        for proj in &sel.projections {
            match proj {
                BoundProjection::Expr(e, _) => {
                    if Self::expr_has_outer_ref(e) { return true; }
                }
                BoundProjection::Aggregate(_, Some(e), _, _, _) => {
                    if Self::expr_has_outer_ref(e) { return true; }
                }
                _ => {}
            }
        }
        false
    }

    /// Substitute OuterColumnRef nodes with literal values from the outer row.
    pub fn substitute_outer_refs(expr: &BoundExpr, outer_row: &OwnedRow) -> BoundExpr {
        match expr {
            BoundExpr::OuterColumnRef(idx) => {
                let val = outer_row.get(*idx).cloned().unwrap_or(Datum::Null);
                BoundExpr::Literal(val)
            }
            BoundExpr::Literal(_) | BoundExpr::ColumnRef(_) => expr.clone(),
            BoundExpr::BinaryOp { left, op, right } => BoundExpr::BinaryOp {
                left: Box::new(Self::substitute_outer_refs(left, outer_row)),
                op: *op,
                right: Box::new(Self::substitute_outer_refs(right, outer_row)),
            },
            BoundExpr::Not(inner) => BoundExpr::Not(Box::new(Self::substitute_outer_refs(inner, outer_row))),
            BoundExpr::IsNull(inner) => BoundExpr::IsNull(Box::new(Self::substitute_outer_refs(inner, outer_row))),
            BoundExpr::IsNotNull(inner) => BoundExpr::IsNotNull(Box::new(Self::substitute_outer_refs(inner, outer_row))),
            BoundExpr::Like { expr, pattern, negated, case_insensitive } => BoundExpr::Like {
                expr: Box::new(Self::substitute_outer_refs(expr, outer_row)),
                pattern: Box::new(Self::substitute_outer_refs(pattern, outer_row)),
                negated: *negated, case_insensitive: *case_insensitive,
            },
            BoundExpr::Between { expr, low, high, negated } => BoundExpr::Between {
                expr: Box::new(Self::substitute_outer_refs(expr, outer_row)),
                low: Box::new(Self::substitute_outer_refs(low, outer_row)),
                high: Box::new(Self::substitute_outer_refs(high, outer_row)),
                negated: *negated,
            },
            BoundExpr::InList { expr, list, negated } => BoundExpr::InList {
                expr: Box::new(Self::substitute_outer_refs(expr, outer_row)),
                list: list.iter().map(|e| Self::substitute_outer_refs(e, outer_row)).collect(),
                negated: *negated,
            },
            BoundExpr::Cast { expr, target_type } => BoundExpr::Cast {
                expr: Box::new(Self::substitute_outer_refs(expr, outer_row)),
                target_type: target_type.clone(),
            },
            BoundExpr::Case { operand, conditions, results, else_result } => BoundExpr::Case {
                operand: operand.as_ref().map(|e| Box::new(Self::substitute_outer_refs(e, outer_row))),
                conditions: conditions.iter().map(|e| Self::substitute_outer_refs(e, outer_row)).collect(),
                results: results.iter().map(|e| Self::substitute_outer_refs(e, outer_row)).collect(),
                else_result: else_result.as_ref().map(|e| Box::new(Self::substitute_outer_refs(e, outer_row))),
            },
            BoundExpr::Coalesce(args) => BoundExpr::Coalesce(
                args.iter().map(|e| Self::substitute_outer_refs(e, outer_row)).collect(),
            ),
            BoundExpr::Function { func, args } => BoundExpr::Function {
                func: func.clone(),
                args: args.iter().map(|e| Self::substitute_outer_refs(e, outer_row)).collect(),
            },
            BoundExpr::AggregateExpr { func, arg, distinct } => BoundExpr::AggregateExpr {
                func: func.clone(),
                arg: arg.as_ref().map(|e| Box::new(Self::substitute_outer_refs(e, outer_row))),
                distinct: *distinct,
            },
            BoundExpr::ArrayLiteral(elems) => BoundExpr::ArrayLiteral(
                elems.iter().map(|e| Self::substitute_outer_refs(e, outer_row)).collect(),
            ),
            BoundExpr::ArrayIndex { array, index } => BoundExpr::ArrayIndex {
                array: Box::new(Self::substitute_outer_refs(array, outer_row)),
                index: Box::new(Self::substitute_outer_refs(index, outer_row)),
            },
            BoundExpr::IsNotDistinctFrom { left, right } => BoundExpr::IsNotDistinctFrom {
                left: Box::new(Self::substitute_outer_refs(left, outer_row)),
                right: Box::new(Self::substitute_outer_refs(right, outer_row)),
            },
            BoundExpr::AnyOp { left, compare_op, right } => BoundExpr::AnyOp {
                left: Box::new(Self::substitute_outer_refs(left, outer_row)),
                compare_op: *compare_op,
                right: Box::new(Self::substitute_outer_refs(right, outer_row)),
            },
            BoundExpr::AllOp { left, compare_op, right } => BoundExpr::AllOp {
                left: Box::new(Self::substitute_outer_refs(left, outer_row)),
                compare_op: *compare_op,
                right: Box::new(Self::substitute_outer_refs(right, outer_row)),
            },
            BoundExpr::ArraySlice { array, lower, upper } => BoundExpr::ArraySlice {
                array: Box::new(Self::substitute_outer_refs(array, outer_row)),
                lower: lower.as_ref().map(|e| Box::new(Self::substitute_outer_refs(e, outer_row))),
                upper: upper.as_ref().map(|e| Box::new(Self::substitute_outer_refs(e, outer_row))),
            },
            BoundExpr::ScalarSubquery(sub) => {
                let sub2 = Self::substitute_outer_refs_in_select(sub, outer_row);
                BoundExpr::ScalarSubquery(Box::new(sub2))
            }
            BoundExpr::InSubquery { expr, subquery, negated } => BoundExpr::InSubquery {
                expr: Box::new(Self::substitute_outer_refs(expr, outer_row)),
                subquery: Box::new(Self::substitute_outer_refs_in_select(subquery, outer_row)),
                negated: *negated,
            },
            BoundExpr::Exists { subquery, negated } => BoundExpr::Exists {
                subquery: Box::new(Self::substitute_outer_refs_in_select(subquery, outer_row)),
                negated: *negated,
            },
            BoundExpr::SequenceNextval(_) | BoundExpr::SequenceCurrval(_) | BoundExpr::SequenceSetval(_, _)
            | BoundExpr::Parameter(_) | BoundExpr::Grouping(_) => expr.clone(),
        }
    }

    /// Substitute OuterColumnRef nodes inside a BoundSelect.
    fn substitute_outer_refs_in_select(sel: &BoundSelect, outer_row: &OwnedRow) -> BoundSelect {
        let mut sel2 = sel.clone();
        sel2.filter = sel2.filter.as_ref().map(|f| Self::substitute_outer_refs(f, outer_row));
        sel2.having = sel2.having.as_ref().map(|h| Self::substitute_outer_refs(h, outer_row));
        sel2.projections = sel2.projections.iter().map(|p| match p {
            BoundProjection::Expr(e, a) => {
                BoundProjection::Expr(Self::substitute_outer_refs(e, outer_row), a.clone())
            }
            other => other.clone(),
        }).collect();
        sel2
    }

    /// Materialize a correlated subquery expression for a specific outer row.
    /// Substitutes OuterColumnRef with the outer row's values, then executes.
    pub fn materialize_correlated(
        &self,
        expr: &BoundExpr,
        outer_row: &OwnedRow,
        txn: &TxnHandle,
    ) -> Result<BoundExpr, FalconError> {
        let substituted = Self::substitute_outer_refs(expr, outer_row);
        self.materialize_subqueries(&substituted, txn)
    }

    pub(crate) fn eval_check_constraints(
        &self,
        schema: &TableSchema,
        row: &OwnedRow,
    ) -> Result<(), FalconError> {
        if schema.check_constraints.is_empty() {
            return Ok(());
        }
        // Build a minimal catalog containing just this table for binding
        let mut catalog = Catalog::new();
        catalog.add_table(schema.clone());
        let binder = Binder::new(catalog);

        for check_sql in &schema.check_constraints {
            // Parse the expression
            let parsed = sqlparser::parser::Parser::parse_sql(
                &sqlparser::dialect::PostgreSqlDialect {},
                &format!("SELECT * FROM {} WHERE {}", schema.name, check_sql),
            );
            if let Ok(stmts) = parsed {
                if let Some(sqlparser::ast::Statement::Query(query)) = stmts.first() {
                    if let sqlparser::ast::SetExpr::Select(sel) = query.body.as_ref() {
                        if let Some(ref where_expr) = sel.selection {
                            // Bind the expression
                            let aliases = HashMap::new();
                            if let Ok(bound) =
                                binder.bind_expr_with_aliases(where_expr, schema, &aliases)
                            {
                                // Evaluate against the row
                                match ExprEngine::eval_row(&bound, row) {
                                    Ok(Datum::Boolean(true)) => {} // constraint satisfied
                                    Ok(Datum::Null) => {} // NULL is treated as satisfied (SQL semantics)
                                    _ => {
                                        return Err(FalconError::Execution(
                                            ExecutionError::CheckConstraintViolation(check_sql.clone()),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
