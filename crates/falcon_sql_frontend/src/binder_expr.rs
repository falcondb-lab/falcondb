use std::collections::HashMap;

use falcon_common::datum::Datum;
use falcon_common::error::SqlError;
use falcon_common::schema::TableSchema;
use falcon_common::types::DataType;
use sqlparser::ast::{self, Expr, Value};

use crate::types::*;
use crate::binder::{Binder, AliasMap};

impl Binder {
    pub fn bind_expr(&self, expr: &Expr, schema: &TableSchema) -> Result<BoundExpr, SqlError> {
        self.bind_expr_with_aliases(expr, schema, &HashMap::new())
    }

    /// Bind an expression with an alias map for CompoundIdentifier resolution.
    pub fn bind_expr_with_aliases(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        aliases: &AliasMap,
    ) -> Result<BoundExpr, SqlError> {
        self.bind_expr_full(expr, schema, aliases, None)
    }

    /// Bind an expression with full context including optional outer schema
    /// for correlated subquery support.
    pub(crate) fn bind_expr_full(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        aliases: &AliasMap,
        outer_schema: Option<&TableSchema>,
    ) -> Result<BoundExpr, SqlError> {
        match expr {
            Expr::Identifier(ident) => {
                if let Some(col_idx) = schema.find_column(&ident.value) {
                    Ok(BoundExpr::ColumnRef(col_idx))
                } else if let Some(outer) = outer_schema {
                    // Try outer schema for correlated subquery
                    let outer_idx = outer.find_column(&ident.value)
                        .ok_or_else(|| SqlError::UnknownColumn(ident.value.clone()))?;
                    Ok(BoundExpr::OuterColumnRef(outer_idx))
                } else {
                    Err(SqlError::UnknownColumn(ident.value.clone()))
                }
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let qualifier = parts[0].value.to_lowercase();
                let col_name = parts[1].value.to_lowercase();

                // First, try alias map to resolve qualifier -> real table name + offset
                let resolved_table = aliases.get(&qualifier).map(|(name, _offset)| name.clone());
                let lookup_name = resolved_table.as_deref().unwrap_or(&qualifier);

                let matching: Vec<usize> = schema.columns.iter().enumerate()
                    .filter(|(_, c)| c.name.to_lowercase() == col_name)
                    .map(|(i, _)| i)
                    .collect();
                if matching.is_empty() {
                    // Try outer schema for correlated subquery
                    if let Some(outer) = outer_schema {
                        let outer_idx = outer.find_column(&col_name)
                            .ok_or_else(|| SqlError::UnknownColumn(format!("{}.{}", parts[0].value, parts[1].value)))?;
                        return Ok(BoundExpr::OuterColumnRef(outer_idx));
                    }
                    return Err(SqlError::UnknownColumn(format!("{}.{}", parts[0].value, parts[1].value)));
                }
                if matching.len() == 1 {
                    // If the qualifier is not recognized in the inner scope,
                    // it might be an outer table reference (correlated subquery).
                    if !aliases.contains_key(&qualifier) {
                        if let Some(outer) = outer_schema {
                            if let Some(outer_idx) = outer.find_column(&col_name) {
                                return Ok(BoundExpr::OuterColumnRef(outer_idx));
                            }
                        }
                    }
                    return Ok(BoundExpr::ColumnRef(matching[0]));
                }
                // Multiple matches — disambiguate using alias map offset
                if let Some((_real_name, col_offset)) = aliases.get(&qualifier) {
                    // Find the column by name starting from the alias offset
                    for &idx in &matching {
                        if idx >= *col_offset
                            && schema.columns[idx].name.to_lowercase() == col_name
                        {
                            return Ok(BoundExpr::ColumnRef(idx));
                        }
                    }
                    // Fallback: catalog lookup for non-CTE tables
                    if let Some(table_schema) = self.catalog.find_table(lookup_name) {
                        if let Some(col_pos) = table_schema.find_column(&col_name) {
                            let target_idx = col_offset + col_pos;
                            if matching.contains(&target_idx) {
                                return Ok(BoundExpr::ColumnRef(target_idx));
                            }
                        }
                    }
                }
                // Fallback: look up in catalog by name
                if let Some(table_schema) = self.catalog.find_table(lookup_name) {
                    if let Some(col_pos) = table_schema.find_column(&col_name) {
                        for &idx in &matching {
                            if idx >= col_pos {
                                let start = idx - col_pos;
                                if start + table_schema.columns.len() <= schema.columns.len() {
                                    let matches_table = table_schema.columns.iter().enumerate().all(|(j, tc)| {
                                        schema.columns[start + j].name.to_lowercase() == tc.name.to_lowercase()
                                    });
                                    if matches_table {
                                        return Ok(BoundExpr::ColumnRef(idx));
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(BoundExpr::ColumnRef(matching[0]))
            }
            Expr::Value(Value::Placeholder(s)) => {
                // $1, $2, ... parameter placeholders (1-indexed)
                let idx = s.trim_start_matches('$')
                    .parse::<usize>()
                    .map_err(|_| SqlError::Parse(format!("Invalid parameter placeholder: {}", s)))?;
                if idx == 0 {
                    return Err(SqlError::Parse("Parameter index must be >= 1".into()));
                }
                // Track parameter in ParamEnv if active
                if let Some(ref mut env) = *self.param_env.borrow_mut() {
                    env.touch_param(idx);
                }
                Ok(BoundExpr::Parameter(idx))
            }
            Expr::Value(val) => {
                let datum = self.value_to_datum(val)?;
                Ok(BoundExpr::Literal(datum))
            }
            Expr::TypedString { data_type, value } => {
                let target_type = format!("{}", data_type).to_lowercase();
                Ok(BoundExpr::Cast {
                    expr: Box::new(BoundExpr::Literal(Datum::Text(value.clone()))),
                    target_type,
                })
            }
            Expr::BinaryOp { left, op, right } => {
                let bound_left = self.bind_expr_full(left, schema, aliases, outer_schema)?;
                let bound_right = self.bind_expr_full(right, schema, aliases, outer_schema)?;
                let bin_op = self.resolve_bin_op(op)?;
                // Infer parameter types from binary op context
                self.infer_param_type_from_binary(&bound_left, &bound_right, schema);
                self.infer_param_type_from_binary(&bound_right, &bound_left, schema);
                Ok(BoundExpr::BinaryOp {
                    left: Box::new(bound_left),
                    op: bin_op,
                    right: Box::new(bound_right),
                })
            }
            Expr::UnaryOp {
                op: ast::UnaryOperator::Not,
                expr,
            } => {
                let bound = self.bind_expr_full(expr, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Not(Box::new(bound)))
            }
            Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr,
            } => {
                let bound = self.bind_expr_full(expr, schema, aliases, outer_schema)?;
                // -expr → 0 - expr
                Ok(BoundExpr::BinaryOp {
                    left: Box::new(BoundExpr::Literal(Datum::Int32(0))),
                    op: BinOp::Minus,
                    right: Box::new(bound),
                })
            }
            Expr::UnaryOp {
                op: ast::UnaryOperator::Plus,
                expr,
            } => {
                // +expr is a no-op
                self.bind_expr_full(expr, schema, aliases, outer_schema)
            }
            Expr::IsNull(expr) => {
                let bound = self.bind_expr_full(expr, schema, aliases, outer_schema)?;
                Ok(BoundExpr::IsNull(Box::new(bound)))
            }
            Expr::IsNotNull(expr) => {
                let bound = self.bind_expr_full(expr, schema, aliases, outer_schema)?;
                Ok(BoundExpr::IsNotNull(Box::new(bound)))
            }
            Expr::Nested(inner) => self.bind_expr_full(inner, schema, aliases, outer_schema),
            Expr::Like { negated, expr: like_expr, pattern, .. } => {
                let bound_expr = self.bind_expr_full(like_expr, schema, aliases, outer_schema)?;
                let bound_pattern = self.bind_expr_full(pattern, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Like {
                    expr: Box::new(bound_expr),
                    pattern: Box::new(bound_pattern),
                    negated: *negated,
                    case_insensitive: false,
                })
            }
            Expr::ILike { negated, expr: like_expr, pattern, .. } => {
                let bound_expr = self.bind_expr_full(like_expr, schema, aliases, outer_schema)?;
                let bound_pattern = self.bind_expr_full(pattern, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Like {
                    expr: Box::new(bound_expr),
                    pattern: Box::new(bound_pattern),
                    negated: *negated,
                    case_insensitive: true,
                })
            }
            Expr::Between { expr: between_expr, negated, low, high } => {
                let bound_expr = self.bind_expr_full(between_expr, schema, aliases, outer_schema)?;
                let bound_low = self.bind_expr_full(low, schema, aliases, outer_schema)?;
                let bound_high = self.bind_expr_full(high, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Between {
                    expr: Box::new(bound_expr),
                    low: Box::new(bound_low),
                    high: Box::new(bound_high),
                    negated: *negated,
                })
            }
            Expr::InList { expr: in_expr, list, negated } => {
                let bound_expr = self.bind_expr_full(in_expr, schema, aliases, outer_schema)?;
                let bound_list: Vec<BoundExpr> = list
                    .iter()
                    .map(|e| self.bind_expr_full(e, schema, aliases, outer_schema))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(BoundExpr::InList {
                    expr: Box::new(bound_expr),
                    list: bound_list,
                    negated: *negated,
                })
            }
            Expr::Cast { expr: cast_expr, data_type, .. } => {
                let bound_expr = self.bind_expr_full(cast_expr, schema, aliases, outer_schema)?;
                let target_type = format!("{}", data_type);
                Ok(BoundExpr::Cast {
                    expr: Box::new(bound_expr),
                    target_type,
                })
            }
            Expr::Subquery(query) => {
                let bound = self.bind_select_query_with_outer(query, Some(schema), None)?;
                Ok(BoundExpr::ScalarSubquery(Box::new(bound)))
            }
            Expr::InSubquery { expr: in_expr, subquery, negated } => {
                let bound_expr = self.bind_expr_full(in_expr, schema, aliases, outer_schema)?;
                let bound_sub = self.bind_select_query_with_outer(subquery, Some(schema), None)?;
                Ok(BoundExpr::InSubquery {
                    expr: Box::new(bound_expr),
                    subquery: Box::new(bound_sub),
                    negated: *negated,
                })
            }
            Expr::Exists { subquery, negated } => {
                let bound_sub = self.bind_select_query_with_outer(subquery, Some(schema), None)?;
                Ok(BoundExpr::Exists {
                    subquery: Box::new(bound_sub),
                    negated: *negated,
                })
            }
            Expr::Case { operand, conditions, results, else_result } => {
                let bound_operand = operand
                    .as_ref()
                    .map(|e| self.bind_expr_full(e, schema, aliases, outer_schema))
                    .transpose()?
                    .map(Box::new);
                let bound_conditions: Vec<BoundExpr> = conditions
                    .iter()
                    .map(|e| self.bind_expr_full(e, schema, aliases, outer_schema))
                    .collect::<Result<_, _>>()?;
                let bound_results: Vec<BoundExpr> = results
                    .iter()
                    .map(|e| self.bind_expr_full(e, schema, aliases, outer_schema))
                    .collect::<Result<_, _>>()?;
                let bound_else = else_result
                    .as_ref()
                    .map(|e| self.bind_expr_full(e, schema, aliases, outer_schema))
                    .transpose()?
                    .map(Box::new);
                Ok(BoundExpr::Case {
                    operand: bound_operand,
                    conditions: bound_conditions,
                    results: bound_results,
                    else_result: bound_else,
                })
            }
            Expr::Function(func) if func.name.to_string().to_uppercase() == "COALESCE" => {
                let args = match &func.args {
                    ast::FunctionArguments::List(args) => {
                        args.args.iter().map(|a| match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                self.bind_expr_full(e, schema, aliases, outer_schema)
                            }
                            _ => Err(SqlError::Unsupported("Complex COALESCE argument".into())),
                        }).collect::<Result<Vec<_>, _>>()?
                    }
                    _ => return Err(SqlError::Unsupported("COALESCE requires arguments".into())),
                };
                Ok(BoundExpr::Coalesce(args))
            }
            Expr::Function(func) if func.name.to_string().to_uppercase() == "NULLIF" => {
                // NULLIF(a, b) => CASE WHEN a = b THEN NULL ELSE a END
                let args: Vec<&Expr> = match &func.args {
                    ast::FunctionArguments::List(args) => {
                        args.args.iter().filter_map(|a| match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                            _ => None,
                        }).collect()
                    }
                    _ => return Err(SqlError::Unsupported("NULLIF requires arguments".into())),
                };
                if args.len() != 2 {
                    return Err(SqlError::Parse("NULLIF requires exactly 2 arguments".into()));
                }
                let a = self.bind_expr_full(args[0], schema, aliases, outer_schema)?;
                let b = self.bind_expr_full(args[1], schema, aliases, outer_schema)?;
                Ok(BoundExpr::Case {
                    operand: None,
                    conditions: vec![BoundExpr::BinaryOp {
                        left: Box::new(a.clone()),
                        op: BinOp::Eq,
                        right: Box::new(b),
                    }],
                    results: vec![BoundExpr::Literal(Datum::Null)],
                    else_result: Some(Box::new(a)),
                })
            }
            Expr::Function(func) if func.name.to_string().to_uppercase() == "GROUPING" => {
                // GROUPING(col1, col2, ...) — resolve each arg to a column index
                let col_exprs: Vec<&Expr> = match &func.args {
                    ast::FunctionArguments::List(args) => {
                        args.args.iter().filter_map(|a| match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                            _ => None,
                        }).collect()
                    }
                    _ => return Err(SqlError::Parse("GROUPING requires at least one argument".into())),
                };
                if col_exprs.is_empty() {
                    return Err(SqlError::Parse("GROUPING requires at least one argument".into()));
                }
                let mut col_indices = Vec::new();
                for e in col_exprs {
                    match e {
                        Expr::Identifier(ident) => {
                            let col_idx = schema.find_column(&ident.value)
                                .ok_or_else(|| SqlError::UnknownColumn(ident.value.clone()))?;
                            col_indices.push(col_idx);
                        }
                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                            let col_name = &parts[1].value;
                            let col_idx = schema.find_column(col_name)
                                .ok_or_else(|| SqlError::UnknownColumn(col_name.clone()))?;
                            col_indices.push(col_idx);
                        }
                        _ => return Err(SqlError::Parse("GROUPING arguments must be column references".into())),
                    }
                }
                Ok(BoundExpr::Grouping(col_indices))
            }
            Expr::Substring { expr: sub_expr, substring_from, substring_for, .. } => {
                let mut args = vec![self.bind_expr_full(sub_expr, schema, aliases, outer_schema)?];
                if let Some(from_expr) = substring_from {
                    args.push(self.bind_expr_full(from_expr, schema, aliases, outer_schema)?);
                }
                if let Some(for_expr) = substring_for {
                    args.push(self.bind_expr_full(for_expr, schema, aliases, outer_schema)?);
                }
                Ok(BoundExpr::Function { func: ScalarFunc::Substring, args })
            }
            Expr::Floor { expr: floor_expr, .. } => {
                let arg = self.bind_expr_full(floor_expr, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Function { func: ScalarFunc::Floor, args: vec![arg] })
            }
            Expr::Ceil { expr: ceil_expr, .. } => {
                let arg = self.bind_expr_full(ceil_expr, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Function { func: ScalarFunc::Ceil, args: vec![arg] })
            }
            Expr::Trim { expr: trim_expr, .. } => {
                let arg = self.bind_expr_full(trim_expr, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Function { func: ScalarFunc::Trim, args: vec![arg] })
            }
            Expr::Position { expr: pos_expr, r#in: in_expr } => {
                let needle = self.bind_expr_full(pos_expr, schema, aliases, outer_schema)?;
                let haystack = self.bind_expr_full(in_expr, schema, aliases, outer_schema)?;
                // POSITION(needle IN haystack) -> args = [haystack, needle] for strpos semantics
                Ok(BoundExpr::Function { func: ScalarFunc::Position, args: vec![haystack, needle] })
            }
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                // Sequence functions: nextval, currval, setval
                match func_name.as_str() {
                    "NEXTVAL" => {
                        let seq_name = self.extract_string_arg(func, 0)?;
                        return Ok(BoundExpr::SequenceNextval(seq_name));
                    }
                    "CURRVAL" => {
                        let seq_name = self.extract_string_arg(func, 0)?;
                        return Ok(BoundExpr::SequenceCurrval(seq_name));
                    }
                    "SETVAL" => {
                        let seq_name = self.extract_string_arg(func, 0)?;
                        let value = self.extract_int_arg_from_func(func, 1)?;
                        return Ok(BoundExpr::SequenceSetval(seq_name, value));
                    }
                    _ => {}
                }
                // Check if this is an aggregate function (for HAVING expressions)
                let agg_func = match func_name.as_str() {
                    "COUNT" => Some(AggFunc::Count),
                    "SUM" => Some(AggFunc::Sum),
                    "AVG" => Some(AggFunc::Avg),
                    "MIN" => Some(AggFunc::Min),
                    "MAX" => Some(AggFunc::Max),
                    "BOOL_AND" => Some(AggFunc::BoolAnd),
                    "BOOL_OR" => Some(AggFunc::BoolOr),
                    "ARRAY_AGG" => Some(AggFunc::ArrayAgg),
                    // Statistical aggregates
                    "STDDEV" | "STDDEV_SAMP" => Some(AggFunc::StddevSamp),
                    "STDDEV_POP" => Some(AggFunc::StddevPop),
                    "VARIANCE" | "VAR_SAMP" => Some(AggFunc::VarSamp),
                    "VAR_POP" => Some(AggFunc::VarPop),
                    // Two-argument statistical aggregates
                    "CORR" => Some(AggFunc::Corr),
                    "COVAR_POP" => Some(AggFunc::CovarPop),
                    "COVAR_SAMP" => Some(AggFunc::CovarSamp),
                    "REGR_SLOPE" => Some(AggFunc::RegrSlope),
                    "REGR_INTERCEPT" => Some(AggFunc::RegrIntercept),
                    "REGR_R2" => Some(AggFunc::RegrR2),
                    "REGR_COUNT" => Some(AggFunc::RegrCount),
                    "REGR_AVGX" => Some(AggFunc::RegrAvgX),
                    "REGR_AVGY" => Some(AggFunc::RegrAvgY),
                    "REGR_SXX" => Some(AggFunc::RegrSXX),
                    "REGR_SYY" => Some(AggFunc::RegrSYY),
                    "REGR_SXY" => Some(AggFunc::RegrSXY),
                    // Ordered-set aggregates
                    "MODE" => Some(AggFunc::Mode),
                    // Bit aggregates
                    "BIT_AND" => Some(AggFunc::BitAndAgg),
                    "BIT_OR" => Some(AggFunc::BitOrAgg),
                    "BIT_XOR" => Some(AggFunc::BitXorAgg),
                    _ => None,
                };
                if let Some(agg) = agg_func {
                    let is_distinct = if let ast::FunctionArguments::List(args) = &func.args {
                        matches!(args.duplicate_treatment, Some(ast::DuplicateTreatment::Distinct))
                    } else { false };
                    let bound_arg = if let ast::FunctionArguments::List(args) = &func.args {
                        if args.args.is_empty() {
                            None
                        } else if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)) = args.args.first() {
                            None
                        } else if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(inner))) = args.args.first() {
                            Some(Box::new(self.bind_expr_full(inner, schema, aliases, outer_schema)?))
                        } else { None }
                    } else { None };
                    return Ok(BoundExpr::AggregateExpr { func: agg, arg: bound_arg, distinct: is_distinct });
                }
                // Special-case functions that need custom arg handling
                match func_name.as_str() {
                    "NOW" | "CURRENT_TIMESTAMP" => {
                        return Ok(BoundExpr::Function { func: ScalarFunc::Now, args: vec![] });
                    }
                    "CURRENT_DATE" => {
                        return Ok(BoundExpr::Function { func: ScalarFunc::CurrentDate, args: vec![] });
                    }
                    "CURRENT_TIME" => {
                        return Ok(BoundExpr::Function { func: ScalarFunc::CurrentTime, args: vec![] });
                    }
                    "DATE_TRUNC" => {
                        let bound_args = self.bind_func_args_full(func, schema, aliases, outer_schema)?;
                        return Ok(BoundExpr::Function { func: ScalarFunc::DateTrunc, args: bound_args });
                    }
                    "DATE_PART" => {
                        let bound_args = self.bind_func_args_full(func, schema, aliases, outer_schema)?;
                        return Ok(BoundExpr::Function { func: ScalarFunc::Extract, args: bound_args });
                    }
                    "EXTRACT" => {
                        let bound_args = self.bind_func_args_full(func, schema, aliases, outer_schema)?;
                        return Ok(BoundExpr::Function { func: ScalarFunc::Extract, args: bound_args });
                    }
                    _ => {}
                }
                // Resolve function name to ScalarFunc variant
                let scalar_func = crate::resolve_function::resolve_scalar_func(&func_name)
                    .ok_or_else(|| SqlError::Unsupported(format!("Function: {}", func_name)))?;
                let bound_args = self.bind_func_args_full(func, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Function { func: scalar_func, args: bound_args })
            }
            Expr::Extract { field, expr: extract_expr, syntax: _ } => {
                let ts_expr = self.bind_expr_full(extract_expr, schema, aliases, outer_schema)?;
                let field_str = format!("{}", field).to_uppercase();
                Ok(BoundExpr::Function {
                    func: ScalarFunc::Extract,
                    args: vec![BoundExpr::Literal(Datum::Text(field_str)), ts_expr],
                })
            }
            Expr::Array(ast::Array { elem, .. }) => {
                let bound_elems: Vec<BoundExpr> = elem.iter()
                    .map(|e| self.bind_expr_full(e, schema, aliases, outer_schema))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(BoundExpr::ArrayLiteral(bound_elems))
            }
            Expr::Overlay { expr: ov_expr, overlay_what, overlay_from, overlay_for } => {
                let bound_str = self.bind_expr_full(ov_expr, schema, aliases, outer_schema)?;
                let bound_what = self.bind_expr_full(overlay_what, schema, aliases, outer_schema)?;
                let bound_from = self.bind_expr_full(overlay_from, schema, aliases, outer_schema)?;
                let mut args = vec![bound_str, bound_what, bound_from];
                if let Some(ov_for) = overlay_for {
                    args.push(self.bind_expr_full(ov_for, schema, aliases, outer_schema)?);
                }
                Ok(BoundExpr::Function { func: ScalarFunc::Overlay, args })
            }
            Expr::Subscript { expr: sub_expr, subscript } => {
                let bound_arr = self.bind_expr_full(sub_expr, schema, aliases, outer_schema)?;
                match subscript.as_ref() {
                    ast::Subscript::Index { index } => {
                        let bound_idx = self.bind_expr_full(index, schema, aliases, outer_schema)?;
                        Ok(BoundExpr::ArrayIndex {
                            array: Box::new(bound_arr),
                            index: Box::new(bound_idx),
                        })
                    }
                    ast::Subscript::Slice { lower_bound, upper_bound, .. } => {
                        let bound_lower = lower_bound.as_ref()
                            .map(|e| self.bind_expr_full(e, schema, aliases, outer_schema))
                            .transpose()?
                            .map(Box::new);
                        let bound_upper = upper_bound.as_ref()
                            .map(|e| self.bind_expr_full(e, schema, aliases, outer_schema))
                            .transpose()?
                            .map(Box::new);
                        Ok(BoundExpr::ArraySlice {
                            array: Box::new(bound_arr),
                            lower: bound_lower,
                            upper: bound_upper,
                        })
                    }
                }
            }
            Expr::IsTrue(inner) => {
                let bound = self.bind_expr_full(inner, schema, aliases, outer_schema)?;
                Ok(BoundExpr::BinaryOp {
                    left: Box::new(bound),
                    op: BinOp::Eq,
                    right: Box::new(BoundExpr::Literal(Datum::Boolean(true))),
                })
            }
            Expr::IsFalse(inner) => {
                let bound = self.bind_expr_full(inner, schema, aliases, outer_schema)?;
                Ok(BoundExpr::BinaryOp {
                    left: Box::new(bound),
                    op: BinOp::Eq,
                    right: Box::new(BoundExpr::Literal(Datum::Boolean(false))),
                })
            }
            Expr::IsNotTrue(inner) => {
                let bound = self.bind_expr_full(inner, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Not(Box::new(BoundExpr::BinaryOp {
                    left: Box::new(bound),
                    op: BinOp::Eq,
                    right: Box::new(BoundExpr::Literal(Datum::Boolean(true))),
                })))
            }
            Expr::IsNotFalse(inner) => {
                let bound = self.bind_expr_full(inner, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Not(Box::new(BoundExpr::BinaryOp {
                    left: Box::new(bound),
                    op: BinOp::Eq,
                    right: Box::new(BoundExpr::Literal(Datum::Boolean(false))),
                })))
            }
            Expr::IsDistinctFrom(left, right) => {
                // a IS DISTINCT FROM b  ≡  NOT (a IS NOT DISTINCT FROM b)
                // ≡  (a <> b) OR (a IS NULL AND b IS NOT NULL) OR (a IS NOT NULL AND b IS NULL)
                // Simplified: NOT ((a = b) OR (a IS NULL AND b IS NULL))
                let bl = self.bind_expr_full(left, schema, aliases, outer_schema)?;
                let br = self.bind_expr_full(right, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Not(Box::new(BoundExpr::IsNotDistinctFrom {
                    left: Box::new(bl),
                    right: Box::new(br),
                })))
            }
            Expr::IsNotDistinctFrom(left, right) => {
                let bl = self.bind_expr_full(left, schema, aliases, outer_schema)?;
                let br = self.bind_expr_full(right, schema, aliases, outer_schema)?;
                Ok(BoundExpr::IsNotDistinctFrom {
                    left: Box::new(bl),
                    right: Box::new(br),
                })
            }
            Expr::Interval(ast::Interval { value, .. }) => {
                // Bind the interval value expression as text, let the executor parse it
                let bound_val = self.bind_expr_full(value, schema, aliases, outer_schema)?;
                Ok(BoundExpr::Cast {
                    expr: Box::new(bound_val),
                    target_type: "interval".to_string(),
                })
            }
            Expr::AnyOp { left, compare_op, right } => {
                let bound_left = self.bind_expr_full(left, schema, aliases, outer_schema)?;
                let bound_right = self.bind_expr_full(right, schema, aliases, outer_schema)?;
                let op = self.resolve_bin_op(compare_op)?;
                Ok(BoundExpr::AnyOp {
                    left: Box::new(bound_left),
                    compare_op: op,
                    right: Box::new(bound_right),
                })
            }
            Expr::AllOp { left, compare_op, right } => {
                let bound_left = self.bind_expr_full(left, schema, aliases, outer_schema)?;
                let bound_right = self.bind_expr_full(right, schema, aliases, outer_schema)?;
                let op = self.resolve_bin_op(compare_op)?;
                Ok(BoundExpr::AllOp {
                    left: Box::new(bound_left),
                    compare_op: op,
                    right: Box::new(bound_right),
                })
            }
            _ => Err(SqlError::Unsupported(format!("Expression: {:?}", expr))),
        }
    }

    /// Bind function arguments to BoundExpr list.
    #[allow(dead_code)]
    pub(crate) fn bind_func_args(
        &self,
        func: &ast::Function,
        schema: &TableSchema,
        aliases: &AliasMap,
    ) -> Result<Vec<BoundExpr>, SqlError> {
        self.bind_func_args_full(func, schema, aliases, None)
    }

    /// Extract a string argument from a function call at the given position.
    fn extract_string_arg(&self, func: &ast::Function, pos: usize) -> Result<String, SqlError> {
        if let ast::FunctionArguments::List(args) = &func.args {
            if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                Expr::Value(Value::SingleQuotedString(s))
            ))) = args.args.get(pos) {
                return Ok(s.to_lowercase());
            }
        }
        Err(SqlError::Parse(format!("Expected string argument at position {}", pos)))
    }

    /// Extract an integer argument from a function call at the given position.
    fn extract_int_arg_from_func(&self, func: &ast::Function, pos: usize) -> Result<i64, SqlError> {
        if let ast::FunctionArguments::List(args) = &func.args {
            if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                Expr::Value(Value::Number(n, _))
            ))) = args.args.get(pos) {
                return n.parse::<i64>().map_err(|_| SqlError::Parse("Invalid integer".into()));
            }
        }
        Err(SqlError::Parse(format!("Expected integer argument at position {}", pos)))
    }

    /// If `expr` is a Parameter and `other` has a known type from the schema,
    /// unify the parameter with that type in ParamEnv.
    pub(crate) fn infer_param_type_from_binary(
        &self,
        expr: &BoundExpr,
        other: &BoundExpr,
        schema: &TableSchema,
    ) {
        if let BoundExpr::Parameter(idx) = expr {
            if let Some(dt) = Self::infer_expr_type(other, schema) {
                if let Some(ref mut env) = *self.param_env.borrow_mut() {
                    let _ = env.unify_param(*idx, &dt);
                }
            }
        }
    }

    /// Try to infer the DataType of a bound expression from the schema context.
    fn infer_expr_type(expr: &BoundExpr, schema: &TableSchema) -> Option<DataType> {
        match expr {
            BoundExpr::ColumnRef(idx) => {
                schema.columns.get(*idx).map(|c| c.data_type.clone())
            }
            BoundExpr::Literal(datum) => datum_to_datatype(datum),
            BoundExpr::Cast { target_type, .. } => parse_target_type(target_type),
            _ => None,
        }
    }

    /// Bind function arguments with optional outer schema for correlated subqueries.
    pub(crate) fn bind_func_args_full(
        &self,
        func: &ast::Function,
        schema: &TableSchema,
        aliases: &AliasMap,
        outer_schema: Option<&TableSchema>,
    ) -> Result<Vec<BoundExpr>, SqlError> {
        match &func.args {
            ast::FunctionArguments::List(args) => {
                args.args.iter().map(|a| match a {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                        self.bind_expr_full(e, schema, aliases, outer_schema)
                    }
                    _ => Err(SqlError::Unsupported("Complex function argument".into())),
                }).collect()
            }
            _ => Err(SqlError::Unsupported("Invalid function arguments".into())),
        }
    }
}

/// Map a Datum value to its corresponding DataType.
fn datum_to_datatype(datum: &Datum) -> Option<DataType> {
    match datum {
        Datum::Int32(_) => Some(DataType::Int32),
        Datum::Int64(_) => Some(DataType::Int64),
        Datum::Float64(_) => Some(DataType::Float64),
        Datum::Boolean(_) => Some(DataType::Boolean),
        Datum::Text(_) => Some(DataType::Text),
        Datum::Timestamp(_) => Some(DataType::Timestamp),
        Datum::Jsonb(_) => Some(DataType::Jsonb),
        Datum::Array(_) => None, // array element type unknown from value alone
        Datum::Null => None,
        _ => None,
    }
}

/// Parse a CAST target type string into a DataType.
fn parse_target_type(target: &str) -> Option<DataType> {
    match target.to_lowercase().as_str() {
        "int" | "integer" | "int4" => Some(DataType::Int32),
        "bigint" | "int8" => Some(DataType::Int64),
        "float" | "double precision" | "float8" => Some(DataType::Float64),
        "real" | "float4" => Some(DataType::Float64),
        "text" | "varchar" | "character varying" => Some(DataType::Text),
        "boolean" | "bool" => Some(DataType::Boolean),
        "timestamp" | "timestamp without time zone" => Some(DataType::Timestamp),
        "jsonb" => Some(DataType::Jsonb),
        _ => None,
    }
}
