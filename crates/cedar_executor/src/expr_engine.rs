use cedar_common::datum::{Datum, OwnedRow};
use cedar_common::error::ExecutionError;
use cedar_sql_frontend::types::BoundExpr;

pub struct ExprEngine;

impl ExprEngine {
    pub fn eval_row(expr: &BoundExpr, row: &OwnedRow) -> Result<Datum, ExecutionError> {
        crate::eval::eval_expr(expr, row)
    }

    pub fn eval_filter(expr: &BoundExpr, row: &OwnedRow) -> Result<bool, ExecutionError> {
        crate::eval::eval_filter(expr, row)
    }

    pub fn eval_having_filter(
        expr: &BoundExpr,
        group_rows: &[&OwnedRow],
    ) -> Result<bool, ExecutionError> {
        crate::eval::eval_having_filter(expr, group_rows)
    }
}

#[derive(Debug, Clone)]
pub struct CompiledExpr {
    expr: BoundExpr,
}

impl CompiledExpr {
    pub fn compile(expr: &BoundExpr) -> Self {
        Self { expr: expr.clone() }
    }

    pub fn eval_row(&self, row: &OwnedRow) -> Result<Datum, ExecutionError> {
        ExprEngine::eval_row(&self.expr, row)
    }

    pub fn as_expr(&self) -> &BoundExpr {
        &self.expr
    }
}

pub fn compile(expr: &BoundExpr) -> CompiledExpr {
    CompiledExpr::compile(expr)
}
