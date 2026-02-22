use std::collections::HashMap;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::SqlError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{ColumnId, DataType, TableId};
use sqlparser::ast::{self, Expr, SelectItem, SetExpr, Value};

use crate::binder::{AliasMap, Binder};
use crate::types::*;

impl Binder {
    pub(crate) fn bind_select(&self, query: &ast::Query) -> Result<BoundStatement, SqlError> {
        Ok(BoundStatement::Select(self.bind_select_query(query)?))
    }

    /// Bind a SELECT query into a BoundSelect. Shared by top-level SELECT and subqueries.
    pub(crate) fn bind_select_query(&self, query: &ast::Query) -> Result<BoundSelect, SqlError> {
        self.bind_select_query_with_outer(query, None, None)
    }

    /// Bind a SELECT query with an optional outer schema for correlated subquery support.
    /// When `outer_schema` is Some, unresolved columns fall back to the outer schema
    /// and produce `OuterColumnRef` nodes.
    /// `inherited_ctes` provides pre-registered CTE schemas (used for recursive CTE binding).
    pub(crate) fn bind_select_query_with_outer(
        &self,
        query: &ast::Query,
        outer_schema: Option<&TableSchema>,
        inherited_ctes: Option<&HashMap<String, TableSchema>>,
    ) -> Result<BoundSelect, SqlError> {
        // Handle UNION / UNION ALL
        if let SetExpr::SetOperation {
            left,
            right,
            set_quantifier,
            op,
        } = query.body.as_ref()
        {
            let set_op_kind = match op {
                ast::SetOperator::Union => SetOpKind::Union,
                ast::SetOperator::Intersect => SetOpKind::Intersect,
                ast::SetOperator::Except => SetOpKind::Except,
            };
            let is_all = matches!(set_quantifier, ast::SetQuantifier::All);

            // Bind left and right as separate queries (wrap in dummy Query for recursion)
            let left_query = ast::Query {
                body: left.clone(),
                order_by: None,
                limit: None,
                offset: None,
                with: query.with.clone(),
                fetch: None,
                locks: vec![],
                limit_by: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            };
            let right_query = ast::Query {
                body: right.clone(),
                order_by: None,
                limit: None,
                offset: None,
                with: None,
                fetch: None,
                locks: vec![],
                limit_by: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            };

            let mut bound_left = self.bind_select_query(&left_query)?;
            let bound_right = self.bind_select_query(&right_query)?;

            // Apply top-level ORDER BY and LIMIT to the combined result
            // Parse ORDER BY using left's schema/projections
            let visible_projection_count = bound_left.projections.len();
            let mut order_by = Vec::new();
            let alias_map: AliasMap = HashMap::new();
            for ob in &query.order_by.as_ref().map_or(vec![], |o| o.exprs.clone()) {
                let col_idx = self.resolve_order_by_expr(
                    &ob.expr,
                    &mut bound_left.projections,
                    &bound_left.schema,
                    &alias_map,
                )?;
                let asc = ob.asc.unwrap_or(true);
                order_by.push(BoundOrderBy {
                    column_idx: col_idx,
                    asc,
                });
            }

            let limit = query
                .limit
                .as_ref()
                .map(|expr| Self::eval_limit_offset_expr(expr, "LIMIT"))
                .transpose()?;

            let offset = query
                .offset
                .as_ref()
                .map(|o| Self::eval_limit_offset_expr(&o.value, "OFFSET"))
                .transpose()?;

            bound_left.order_by = order_by;
            bound_left.limit = limit;
            bound_left.offset = offset;
            bound_left.visible_projection_count = visible_projection_count;
            bound_left.unions.push((bound_right, set_op_kind, is_all));
            return Ok(bound_left);
        }

        // Handle VALUES clause: VALUES (1,'a'), (2,'b')
        if let SetExpr::Values(values) = query.body.as_ref() {
            return self.bind_values_clause(values, query);
        }

        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            _ => return Err(SqlError::Unsupported("Non-SELECT query body".into())),
        };

        // Bind CTEs (WITH ... AS)
        let mut cte_schemas: HashMap<String, TableSchema> = HashMap::new();
        let mut bound_ctes: Vec<BoundCte> = Vec::new();
        // Import inherited CTE schemas (for recursive CTE binding)
        if let Some(inherited) = inherited_ctes {
            for (k, v) in inherited {
                cte_schemas.insert(k.clone(), v.clone());
            }
        }
        if let Some(with) = &query.with {
            let is_recursive = with.recursive;
            for (i, cte) in with.cte_tables.iter().enumerate() {
                let cte_name = cte.alias.name.value.to_lowercase();
                let cte_table_id = TableId(1_000_000 + i as u64);

                // Check for recursive CTE: WITH RECURSIVE name AS (base UNION ALL recursive)
                if is_recursive {
                    if let SetExpr::SetOperation {
                        left,
                        right,
                        op: ast::SetOperator::Union,
                        set_quantifier,
                    } = cte.query.body.as_ref()
                    {
                        let is_all = matches!(set_quantifier, ast::SetQuantifier::All);
                        if !is_all {
                            return Err(SqlError::Unsupported(
                                "Recursive CTE requires UNION ALL".into(),
                            ));
                        }

                        // Bind the base (anchor) part — no self-reference allowed
                        let base_query = ast::Query {
                            body: left.clone(),
                            order_by: None,
                            limit: None,
                            offset: None,
                            with: None,
                            fetch: None,
                            locks: vec![],
                            limit_by: vec![],
                            for_clause: None,
                            settings: None,
                            format_clause: None,
                        };
                        let base_select = self.bind_select_query(&base_query)?;

                        // Derive CTE schema from base part
                        let cte_columns = Self::schema_from_projections(
                            &base_select.projections,
                            base_select.visible_projection_count,
                            &base_select.schema,
                        );
                        let cte_schema = TableSchema {
                            id: cte_table_id,
                            name: cte_name.clone(),
                            columns: cte_columns,
                            primary_key_columns: vec![],
                            next_serial_values: std::collections::HashMap::new(),
                            check_constraints: vec![],
                            unique_constraints: vec![],
                            foreign_keys: vec![],
                            ..Default::default()
                        };
                        // Register CTE schema so recursive part can reference it
                        cte_schemas.insert(cte_name.clone(), cte_schema);

                        // Bind the recursive part — can reference the CTE by name
                        let recursive_query = ast::Query {
                            body: right.clone(),
                            order_by: None,
                            limit: None,
                            offset: None,
                            with: None,
                            fetch: None,
                            locks: vec![],
                            limit_by: vec![],
                            for_clause: None,
                            settings: None,
                            format_clause: None,
                        };
                        let recursive_select = self.bind_select_query_with_outer(
                            &recursive_query,
                            None,
                            Some(&cte_schemas),
                        )?;

                        bound_ctes.push(BoundCte {
                            name: cte_name,
                            table_id: cte_table_id,
                            select: base_select,
                            recursive_select: Some(Box::new(recursive_select)),
                        });
                        continue;
                    }
                }

                // Non-recursive CTE
                let cte_select = self.bind_select_query(&cte.query)?;

                // Build schema from CTE output projections
                let cte_columns = Self::schema_from_projections(
                    &cte_select.projections,
                    cte_select.visible_projection_count,
                    &cte_select.schema,
                );
                let cte_schema = TableSchema {
                    id: cte_table_id,
                    name: cte_name.clone(),
                    columns: cte_columns,
                    primary_key_columns: vec![],
                    next_serial_values: std::collections::HashMap::new(),
                    check_constraints: vec![],
                    unique_constraints: vec![],
                    foreign_keys: vec![],
                    ..Default::default()
                };
                cte_schemas.insert(cte_name.clone(), cte_schema);
                bound_ctes.push(BoundCte {
                    name: cte_name,
                    table_id: cte_table_id,
                    select: cte_select,
                    recursive_select: None,
                });
            }
        }

        // Resolve FROM — primary table (check CTEs first, then catalog)
        // Handle SELECT without FROM (e.g. SELECT 1+1, SELECT 'hello')
        let (mut table_name, left_schema);
        let mut alias_map: AliasMap = HashMap::new();

        if select.from.is_empty() {
            // No FROM — virtual single-row "dual" table
            table_name = "__dual__".to_string();
            left_schema = TableSchema {
                id: TableId(0),
                name: "__dual__".to_string(),
                columns: vec![],
                primary_key_columns: vec![],
                next_serial_values: std::collections::HashMap::new(),
                check_constraints: vec![],
                unique_constraints: vec![],
                foreign_keys: vec![],
                ..Default::default()
            };
        } else {
            let from = &select.from[0];
            // Handle derived tables (subqueries in FROM)
            if let ast::TableFactor::Derived {
                subquery, alias, ..
            } = &from.relation
            {
                let derived_alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| format!("__derived_{}", bound_ctes.len()));
                let derived_select = self.bind_select_query(subquery)?;
                let derived_table_id = TableId(2_000_000 + bound_ctes.len() as u64);

                // Build schema from derived table's output projections
                let mut derived_columns = Vec::new();
                for proj in &derived_select.projections[..derived_select.visible_projection_count] {
                    let (col_name, data_type) = match proj {
                        BoundProjection::Column(idx, alias) => {
                            if alias.is_empty() {
                                let col = &derived_select.schema.columns[*idx];
                                (col.name.clone(), col.data_type.clone())
                            } else {
                                let col = &derived_select.schema.columns[*idx];
                                (alias.clone(), col.data_type.clone())
                            }
                        }
                        BoundProjection::Aggregate(_, _, a, _, _) => (a.clone(), DataType::Int64),
                        BoundProjection::Expr(_, a) => (a.clone(), DataType::Text),
                        BoundProjection::Window(wf) => (wf.alias.clone(), DataType::Int64),
                    };
                    derived_columns.push(falcon_common::schema::ColumnDef {
                        id: ColumnId(derived_columns.len() as u32),
                        name: col_name,
                        data_type,
                        nullable: true,
                        is_primary_key: false,
                        default_value: None,
                        is_serial: false,
                    });
                }
                let derived_schema = TableSchema {
                    id: derived_table_id,
                    name: derived_alias.clone(),
                    columns: derived_columns,
                    primary_key_columns: vec![],
                    next_serial_values: std::collections::HashMap::new(),
                    check_constraints: vec![],
                    unique_constraints: vec![],
                    foreign_keys: vec![],
                    ..Default::default()
                };
                cte_schemas.insert(derived_alias.to_lowercase(), derived_schema.clone());
                bound_ctes.push(BoundCte {
                    name: derived_alias.to_lowercase(),
                    table_id: derived_table_id,
                    select: derived_select,
                    recursive_select: None,
                });

                table_name = derived_alias.clone();
                left_schema = derived_schema;
                alias_map.insert(derived_alias.to_lowercase(), (derived_alias.clone(), 0));
            } else if let ast::TableFactor::Table {
                name,
                args: Some(args),
                alias,
                ..
            } = &from.relation
            {
                // Table-valued function: GENERATE_SERIES(start, stop [, step])
                let func_name = name.to_string().to_uppercase();
                if func_name == "GENERATE_SERIES" {
                    let tvf_alias = alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| "generate_series".to_string());
                    let tvf_table_id = TableId(3_000_000 + bound_ctes.len() as u64);

                    // Parse arguments
                    let ast::TableFunctionArgs { args: fn_args, .. } = args;
                    let raw_args: Vec<&Expr> = fn_args
                        .iter()
                        .filter_map(|a| match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                            _ => None,
                        })
                        .collect();
                    if raw_args.len() < 2 {
                        return Err(SqlError::Parse(
                            "GENERATE_SERIES requires at least 2 arguments".into(),
                        ));
                    }

                    // Store args as literal values for executor
                    let start_val = self.eval_const_expr(raw_args[0])?;
                    let stop_val = self.eval_const_expr(raw_args[1])?;
                    let step_val = if raw_args.len() >= 3 {
                        self.eval_const_expr(raw_args[2])?
                    } else {
                        Datum::Int64(1)
                    };

                    // Build virtual schema with single "generate_series" column
                    let tvf_schema = TableSchema {
                        id: tvf_table_id,
                        name: tvf_alias.clone(),
                        columns: vec![falcon_common::schema::ColumnDef {
                            id: ColumnId(0),
                            name: "generate_series".to_string(),
                            data_type: DataType::Int64,
                            nullable: false,
                            is_primary_key: false,
                            default_value: None,
                            is_serial: false,
                        }],
                        primary_key_columns: vec![],
                        next_serial_values: std::collections::HashMap::new(),
                        check_constraints: vec![],
                        unique_constraints: vec![],
                        foreign_keys: vec![],
                        ..Default::default()
                    };

                    // Build inline rows as a CTE
                    let mut series_rows = Vec::new();
                    let (s, e, st) = match (&start_val, &stop_val, &step_val) {
                        (Datum::Int64(s), Datum::Int64(e), Datum::Int64(st)) => (*s, *e, *st),
                        _ => {
                            return Err(SqlError::Parse(
                                "GENERATE_SERIES requires integer arguments".into(),
                            ))
                        }
                    };
                    if st > 0 {
                        let mut v = s;
                        while v <= e {
                            series_rows.push(v);
                            v += st;
                        }
                    } else if st < 0 {
                        let mut v = s;
                        while v >= e {
                            series_rows.push(v);
                            v += st;
                        }
                    }

                    // Build a fake CTE select that produces these rows
                    // We'll use projections with literal values — but simpler to store as
                    // inline virtual table data in the BoundSelect
                    let series_projections = vec![BoundProjection::Column(0, String::new())];
                    let series_select = BoundSelect {
                        table_id: tvf_table_id,
                        table_name: tvf_alias.clone(),
                        schema: tvf_schema.clone(),
                        projections: series_projections,
                        visible_projection_count: 1,
                        filter: None,
                        group_by: vec![],
                        grouping_sets: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        distinct: DistinctMode::None,
                        joins: vec![],
                        ctes: vec![],
                        unions: vec![],
                        virtual_rows: series_rows
                            .iter()
                            .map(|v| OwnedRow::new(vec![Datum::Int64(*v)]))
                            .collect(),
                    };

                    cte_schemas.insert(tvf_alias.to_lowercase(), tvf_schema.clone());
                    bound_ctes.push(BoundCte {
                        name: tvf_alias.to_lowercase(),
                        table_id: tvf_table_id,
                        select: series_select,
                        recursive_select: None,
                    });

                    // Store the actual series data in a special CTE name pattern
                    // We'll store it in the CTE schema metadata
                    table_name = tvf_alias.clone();
                    left_schema = tvf_schema;
                    alias_map.insert(tvf_alias.to_lowercase(), (tvf_alias.clone(), 0));

                    // Store inline data: we'll generate the rows in the executor
                    // by recognizing the table_id range 3_000_000+
                } else if func_name == "UNNEST" {
                    let unnest_alias = alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| "unnest".to_string());
                    let unnest_table_id = TableId(3_000_000 + bound_ctes.len() as u64);

                    let ast::TableFunctionArgs { args: fn_args, .. } = args;
                    let raw_args: Vec<&Expr> = fn_args
                        .iter()
                        .filter_map(|a| match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                            _ => None,
                        })
                        .collect();
                    if raw_args.is_empty() {
                        return Err(SqlError::Parse(
                            "UNNEST requires at least 1 argument".into(),
                        ));
                    }

                    let arr_val = self.eval_const_expr(raw_args[0])?;
                    let elements = match arr_val {
                        Datum::Array(elems) => elems,
                        Datum::Null => vec![],
                        _ => {
                            return Err(SqlError::Parse("UNNEST requires an array argument".into()))
                        }
                    };

                    let elem_type = elements
                        .first()
                        .map(|d| match d {
                            Datum::Int32(_) => DataType::Int32,
                            Datum::Int64(_) => DataType::Int64,
                            Datum::Float64(_) => DataType::Float64,
                            Datum::Boolean(_) => DataType::Boolean,
                            Datum::Text(_) => DataType::Text,
                            _ => DataType::Text,
                        })
                        .unwrap_or(DataType::Text);

                    let unnest_schema = TableSchema {
                        id: unnest_table_id,
                        name: unnest_alias.clone(),
                        columns: vec![falcon_common::schema::ColumnDef {
                            id: ColumnId(0),
                            name: "unnest".to_string(),
                            data_type: elem_type,
                            nullable: true,
                            is_primary_key: false,
                            default_value: None,
                            is_serial: false,
                        }],
                        primary_key_columns: vec![],
                        next_serial_values: std::collections::HashMap::new(),
                        check_constraints: vec![],
                        unique_constraints: vec![],
                        foreign_keys: vec![],
                        ..Default::default()
                    };

                    let unnest_projections = vec![BoundProjection::Column(0, String::new())];
                    let unnest_select = BoundSelect {
                        table_id: unnest_table_id,
                        table_name: unnest_alias.clone(),
                        schema: unnest_schema.clone(),
                        projections: unnest_projections,
                        visible_projection_count: 1,
                        filter: None,
                        group_by: vec![],
                        grouping_sets: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        distinct: DistinctMode::None,
                        joins: vec![],
                        ctes: vec![],
                        unions: vec![],
                        virtual_rows: elements
                            .into_iter()
                            .map(|d| OwnedRow::new(vec![d]))
                            .collect(),
                    };

                    cte_schemas.insert(unnest_alias.to_lowercase(), unnest_schema.clone());
                    bound_ctes.push(BoundCte {
                        name: unnest_alias.to_lowercase(),
                        table_id: unnest_table_id,
                        select: unnest_select,
                        recursive_select: None,
                    });

                    table_name = unnest_alias.clone();
                    left_schema = unnest_schema;
                    alias_map.insert(unnest_alias.to_lowercase(), (unnest_alias.clone(), 0));
                } else {
                    return Err(SqlError::Unsupported(format!(
                        "Table function: {}",
                        func_name
                    )));
                }
            } else if let ast::TableFactor::UNNEST {
                array_exprs, alias, ..
            } = &from.relation
            {
                // Handle FROM UNNEST(ARRAY[...]) syntax (TableFactor::UNNEST variant)
                let unnest_alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "unnest".to_string());
                let unnest_table_id = TableId(3_000_000 + bound_ctes.len() as u64);

                if array_exprs.is_empty() {
                    return Err(SqlError::Parse(
                        "UNNEST requires at least 1 argument".into(),
                    ));
                }

                let arr_val = self.eval_const_expr(&array_exprs[0])?;
                let elements = match arr_val {
                    Datum::Array(elems) => elems,
                    Datum::Null => vec![],
                    _ => return Err(SqlError::Parse("UNNEST requires an array argument".into())),
                };

                let elem_type = elements
                    .first()
                    .map(|d| match d {
                        Datum::Int32(_) => DataType::Int32,
                        Datum::Int64(_) => DataType::Int64,
                        Datum::Float64(_) => DataType::Float64,
                        Datum::Boolean(_) => DataType::Boolean,
                        Datum::Text(_) => DataType::Text,
                        _ => DataType::Text,
                    })
                    .unwrap_or(DataType::Text);

                let unnest_schema = TableSchema {
                    id: unnest_table_id,
                    name: unnest_alias.clone(),
                    columns: vec![falcon_common::schema::ColumnDef {
                        id: ColumnId(0),
                        name: "unnest".to_string(),
                        data_type: elem_type,
                        nullable: true,
                        is_primary_key: false,
                        default_value: None,
                        is_serial: false,
                    }],
                    primary_key_columns: vec![],
                    next_serial_values: std::collections::HashMap::new(),
                    check_constraints: vec![],
                    unique_constraints: vec![],
                    foreign_keys: vec![],
                    ..Default::default()
                };

                let unnest_projections = vec![BoundProjection::Column(0, String::new())];
                let unnest_select = BoundSelect {
                    table_id: unnest_table_id,
                    table_name: unnest_alias.clone(),
                    schema: unnest_schema.clone(),
                    projections: unnest_projections,
                    visible_projection_count: 1,
                    filter: None,
                    group_by: vec![],
                    grouping_sets: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    distinct: DistinctMode::None,
                    joins: vec![],
                    ctes: vec![],
                    unions: vec![],
                    virtual_rows: elements
                        .into_iter()
                        .map(|d| OwnedRow::new(vec![d]))
                        .collect(),
                };

                cte_schemas.insert(unnest_alias.to_lowercase(), unnest_schema.clone());
                bound_ctes.push(BoundCte {
                    name: unnest_alias.to_lowercase(),
                    table_id: unnest_table_id,
                    select: unnest_select,
                    recursive_select: None,
                });

                table_name = unnest_alias.clone();
                left_schema = unnest_schema;
                alias_map.insert(unnest_alias.to_lowercase(), (unnest_alias.clone(), 0));
            } else {
                let (tn, left_alias) = self.extract_table_name(&from.relation)?;
                table_name = tn;

                // ── Information schema / pg_catalog virtual tables ──
                let tn_lower = table_name.to_lowercase();
                if let Some((vt_schema, vt_rows)) =
                    self.try_bind_virtual_catalog_table(&tn_lower, &bound_ctes)
                {
                    let vt_alias = left_alias.as_ref().unwrap_or(&table_name).clone();
                    let vt_id = TableId(4_000_000 + bound_ctes.len() as u64);
                    let vt_table_schema = TableSchema {
                        id: vt_id,
                        name: vt_alias.clone(),
                        columns: vt_schema,
                        primary_key_columns: vec![],
                        next_serial_values: std::collections::HashMap::new(),
                        check_constraints: vec![],
                        unique_constraints: vec![],
                        foreign_keys: vec![],
                        ..Default::default()
                    };
                    let vt_projections: Vec<BoundProjection> = (0..vt_table_schema.columns.len())
                        .map(|i| {
                            BoundProjection::Column(i, vt_table_schema.columns[i].name.clone())
                        })
                        .collect();
                    let vt_select = BoundSelect {
                        table_id: vt_id,
                        table_name: vt_alias.clone(),
                        schema: vt_table_schema.clone(),
                        projections: vt_projections.clone(),
                        visible_projection_count: vt_projections.len(),
                        filter: None,
                        group_by: vec![],
                        grouping_sets: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        distinct: DistinctMode::None,
                        joins: vec![],
                        ctes: vec![],
                        unions: vec![],
                        virtual_rows: vt_rows,
                    };
                    cte_schemas.insert(vt_alias.to_lowercase(), vt_table_schema.clone());
                    bound_ctes.push(BoundCte {
                        name: vt_alias.to_lowercase(),
                        table_id: vt_id,
                        select: vt_select,
                        recursive_select: None,
                    });
                    table_name = vt_alias.clone();
                    left_schema = vt_table_schema;
                    alias_map.insert(vt_alias.to_lowercase(), (vt_alias.clone(), 0));
                } else {
                    left_schema =
                        if let Some(cte_schema) = cte_schemas.get(&table_name.to_lowercase()) {
                            cte_schema.clone()
                        } else if let Some(view) = self.catalog.find_view(&table_name).cloned() {
                            // View expansion: bind view SQL as a CTE
                            self.bind_view_as_cte(
                                &table_name,
                                &view.query_sql,
                                &mut cte_schemas,
                                &mut bound_ctes,
                            )?
                        } else {
                            self.catalog
                                .find_table(&table_name)
                                .ok_or_else(|| SqlError::UnknownTable(table_name.clone()))?
                                .clone()
                        };
                    // Register the left table by its real name
                    alias_map.insert(table_name.to_lowercase(), (table_name.clone(), 0));
                    // Register alias if present
                    if let Some(ref alias) = left_alias {
                        alias_map.insert(alias.to_lowercase(), (table_name.clone(), 0));
                    }
                }
            }
        }

        // Build combined schema for multi-table queries (JOINs)
        let mut combined_columns = left_schema.columns.clone();
        let mut joins: Vec<BoundJoin> = Vec::new();

        // Handle implicit CROSS JOINs for multiple FROM items (e.g. FROM t1, t2)
        for extra_from in select.from.iter().skip(1) {
            let (extra_name, extra_alias) = self.extract_table_name(&extra_from.relation)?;
            let extra_schema = if let Some(cte_schema) = cte_schemas.get(&extra_name.to_lowercase())
            {
                cte_schema.clone()
            } else if let Some(view) = self.catalog.find_view(&extra_name).cloned() {
                self.bind_view_as_cte(
                    &extra_name,
                    &view.query_sql,
                    &mut cte_schemas,
                    &mut bound_ctes,
                )?
            } else {
                self.catalog
                    .find_table(&extra_name)
                    .ok_or_else(|| SqlError::UnknownTable(extra_name.clone()))?
                    .clone()
            };
            let offset = combined_columns.len();
            alias_map.insert(extra_name.to_lowercase(), (extra_name.clone(), offset));
            if let Some(ref alias) = extra_alias {
                alias_map.insert(alias.to_lowercase(), (extra_name.clone(), offset));
            }
            joins.push(BoundJoin {
                right_table_id: extra_schema.id,
                right_table_name: extra_name,
                right_schema: extra_schema.clone(),
                right_col_offset: offset,
                join_type: JoinType::Cross,
                condition: None,
            });
            combined_columns.extend(extra_schema.columns.clone());
        }

        let from_joins = select.from.first().map(|f| &f.joins[..]).unwrap_or(&[]);
        for join_item in from_joins {
            // Handle derived tables in JOIN
            if let ast::TableFactor::Derived {
                subquery, alias, ..
            } = &join_item.relation
            {
                let d_alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| format!("__derived_{}", bound_ctes.len()));
                let d_select = self.bind_select_query(subquery)?;
                let d_table_id = TableId(2_000_000 + bound_ctes.len() as u64);
                let mut d_columns = Vec::new();
                for proj in &d_select.projections[..d_select.visible_projection_count] {
                    let (col_name, data_type) = match proj {
                        BoundProjection::Column(idx, a) => {
                            if a.is_empty() {
                                let col = &d_select.schema.columns[*idx];
                                (col.name.clone(), col.data_type.clone())
                            } else {
                                let col = &d_select.schema.columns[*idx];
                                (a.clone(), col.data_type.clone())
                            }
                        }
                        BoundProjection::Aggregate(_, _, a, _, _) => (a.clone(), DataType::Int64),
                        BoundProjection::Expr(_, a) => (a.clone(), DataType::Text),
                        BoundProjection::Window(wf) => (wf.alias.clone(), DataType::Int64),
                    };
                    d_columns.push(falcon_common::schema::ColumnDef {
                        id: ColumnId(d_columns.len() as u32),
                        name: col_name,
                        data_type,
                        nullable: true,
                        is_primary_key: false,
                        default_value: None,
                        is_serial: false,
                    });
                }
                let d_schema = TableSchema {
                    id: d_table_id,
                    name: d_alias.clone(),
                    columns: d_columns,
                    primary_key_columns: vec![],
                    next_serial_values: std::collections::HashMap::new(),
                    check_constraints: vec![],
                    unique_constraints: vec![],
                    foreign_keys: vec![],
                    ..Default::default()
                };
                cte_schemas.insert(d_alias.to_lowercase(), d_schema.clone());
                bound_ctes.push(BoundCte {
                    name: d_alias.to_lowercase(),
                    table_id: d_table_id,
                    select: d_select,
                    recursive_select: None,
                });
            }

            let (right_table_name, right_alias) = self.extract_table_name(&join_item.relation)?;
            let right_schema =
                if let Some(cte_schema) = cte_schemas.get(&right_table_name.to_lowercase()) {
                    cte_schema.clone()
                } else if let Some(view) = self.catalog.find_view(&right_table_name).cloned() {
                    self.bind_view_as_cte(
                        &right_table_name,
                        &view.query_sql,
                        &mut cte_schemas,
                        &mut bound_ctes,
                    )?
                } else {
                    self.catalog
                        .find_table(&right_table_name)
                        .ok_or_else(|| SqlError::UnknownTable(right_table_name.clone()))?
                        .clone()
                };

            let join_type = match &join_item.join_operator {
                ast::JoinOperator::Inner(_) => JoinType::Inner,
                ast::JoinOperator::LeftOuter(_) => JoinType::Left,
                ast::JoinOperator::RightOuter(_) => JoinType::Right,
                ast::JoinOperator::CrossJoin => JoinType::Cross,
                ast::JoinOperator::FullOuter(_) => JoinType::FullOuter,
                _ => {
                    return Err(SqlError::Unsupported(format!(
                        "Join type: {:?}",
                        std::mem::discriminant(&join_item.join_operator)
                    )))
                }
            };

            let right_col_offset = combined_columns.len();

            // Register right table in alias map
            alias_map.insert(
                right_table_name.to_lowercase(),
                (right_table_name.clone(), right_col_offset),
            );
            if let Some(ref alias) = right_alias {
                alias_map.insert(
                    alias.to_lowercase(),
                    (right_table_name.clone(), right_col_offset),
                );
            }

            // Extract ON condition
            let condition = match &join_item.join_operator {
                ast::JoinOperator::Inner(constraint)
                | ast::JoinOperator::LeftOuter(constraint)
                | ast::JoinOperator::RightOuter(constraint)
                | ast::JoinOperator::FullOuter(constraint, ..) => {
                    match constraint {
                        ast::JoinConstraint::On(expr) => {
                            // Build a temporary combined schema for binding the ON condition
                            let temp_schema = self.build_combined_schema(
                                &left_schema,
                                &combined_columns,
                                &right_schema,
                                right_col_offset,
                            );
                            Some(self.bind_expr_with_aliases(expr, &temp_schema, &alias_map)?)
                        }
                        ast::JoinConstraint::None => None,
                        ast::JoinConstraint::Using(columns) => {
                            // USING(col1, col2) -> build AND chain of left.col = right.col
                            let temp_schema = self.build_combined_schema(
                                &left_schema,
                                &combined_columns,
                                &right_schema,
                                right_col_offset,
                            );
                            let mut condition: Option<BoundExpr> = None;
                            for col_ident in columns {
                                let col_name = col_ident.value.to_lowercase();
                                let left_idx = temp_schema
                                    .columns
                                    .iter()
                                    .position(|c| c.name.to_lowercase() == col_name)
                                    .ok_or_else(|| {
                                        SqlError::UnknownColumn(col_ident.value.clone())
                                    })?;
                                let right_idx =
                                    right_schema.find_column(&col_ident.value).ok_or_else(
                                        || SqlError::UnknownColumn(col_ident.value.clone()),
                                    )?;
                                let eq_expr = BoundExpr::BinaryOp {
                                    left: Box::new(BoundExpr::ColumnRef(left_idx)),
                                    op: BinOp::Eq,
                                    right: Box::new(BoundExpr::ColumnRef(
                                        right_col_offset + right_idx,
                                    )),
                                };
                                condition = Some(match condition {
                                    None => eq_expr,
                                    Some(prev) => BoundExpr::BinaryOp {
                                        left: Box::new(prev),
                                        op: BinOp::And,
                                        right: Box::new(eq_expr),
                                    },
                                });
                            }
                            condition
                        }
                        ast::JoinConstraint::Natural => {
                            // NATURAL JOIN: auto-detect shared column names
                            let temp_schema = self.build_combined_schema(
                                &left_schema,
                                &combined_columns,
                                &right_schema,
                                right_col_offset,
                            );
                            let mut condition: Option<BoundExpr> = None;
                            let left_col_names: Vec<String> = combined_columns
                                .iter()
                                .map(|c| c.name.to_lowercase())
                                .collect();
                            for right_col in &right_schema.columns {
                                let rname = right_col.name.to_lowercase();
                                if let Some(left_idx) =
                                    left_col_names.iter().position(|n| *n == rname)
                                {
                                    let right_idx =
                                        right_schema.find_column(&right_col.name).unwrap();
                                    let eq_expr = BoundExpr::BinaryOp {
                                        left: Box::new(BoundExpr::ColumnRef(left_idx)),
                                        op: BinOp::Eq,
                                        right: Box::new(BoundExpr::ColumnRef(
                                            right_col_offset + right_idx,
                                        )),
                                    };
                                    condition = Some(match condition {
                                        None => eq_expr,
                                        Some(prev) => BoundExpr::BinaryOp {
                                            left: Box::new(prev),
                                            op: BinOp::And,
                                            right: Box::new(eq_expr),
                                        },
                                    });
                                }
                            }
                            let _ = temp_schema; // used for consistency
                            condition
                        }
                    }
                }
                ast::JoinOperator::CrossJoin => None,
                _ => None,
            };

            // Append right columns to combined column list
            for col in &right_schema.columns {
                combined_columns.push(col.clone());
            }

            joins.push(BoundJoin {
                join_type,
                right_table_id: right_schema.id,
                right_table_name,
                right_schema,
                right_col_offset,
                condition,
            });
        }

        // Build combined schema for column resolution in projections/filters
        let schema = if joins.is_empty() {
            left_schema.clone()
        } else {
            TableSchema {
                id: left_schema.id,
                name: left_schema.name.clone(),
                columns: combined_columns,
                primary_key_columns: left_schema.primary_key_columns.clone(),
                next_serial_values: std::collections::HashMap::new(),
                check_constraints: vec![],
                unique_constraints: vec![],
                foreign_keys: vec![],
                ..Default::default()
            }
        };

        // Build named window map from WINDOW clause (e.g. WINDOW w AS (...))
        let named_windows: std::collections::HashMap<String, ast::WindowSpec> = select
            .named_window
            .iter()
            .filter_map(|nwd| {
                let name = nwd.0.value.to_lowercase();
                match &nwd.1 {
                    ast::NamedWindowExpr::WindowSpec(spec) => Some((name, spec.clone())),
                    ast::NamedWindowExpr::NamedWindow(_) => None, // chained refs not supported
                }
            })
            .collect();

        // Resolve projections
        let mut projections = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => {
                    for (i, col) in schema.columns.iter().enumerate() {
                        projections.push(BoundProjection::Column(i, col.name.clone()));
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let bound = self.bind_projection_expr_full(
                        expr,
                        &schema,
                        &alias_map,
                        outer_schema,
                        &named_windows,
                    )?;
                    projections.push(bound);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let mut bound = self.bind_projection_expr_full(
                        expr,
                        &schema,
                        &alias_map,
                        outer_schema,
                        &named_windows,
                    )?;
                    match &mut bound {
                        BoundProjection::Column(_, ref mut a) => *a = alias.value.clone(),
                        BoundProjection::Aggregate(_, _, ref mut a, _, _) => {
                            *a = alias.value.clone()
                        }
                        BoundProjection::Expr(_, ref mut a) => *a = alias.value.clone(),
                        BoundProjection::Window(ref mut wf) => wf.alias = alias.value.clone(),
                    }
                    projections.push(bound);
                }
                SelectItem::QualifiedWildcard(name, _) => {
                    let qualifier = name.to_string().to_lowercase();
                    if let Some((_real_name, col_offset)) = alias_map.get(&qualifier) {
                        // Find which table this qualifier refers to and emit its columns
                        let offset = *col_offset;
                        // Determine the column range for this table
                        let table_col_count = if offset == 0 {
                            // Left (primary) table — columns up to the first join offset
                            if let Some(first_join) = joins.first() {
                                first_join.right_col_offset
                            } else {
                                schema.columns.len()
                            }
                        } else {
                            // Find the join that starts at this offset
                            let j = joins.iter().find(|j| j.right_col_offset == offset);
                            if let Some(j) = j {
                                j.right_schema.columns.len()
                            } else {
                                // CTE or derived table — count from offset to next join or end
                                let next_offset = joins
                                    .iter()
                                    .map(|j| j.right_col_offset)
                                    .filter(|&o| o > offset)
                                    .min()
                                    .unwrap_or(schema.columns.len());
                                next_offset - offset
                            }
                        };
                        for i in offset..offset + table_col_count {
                            if i < schema.columns.len() {
                                projections.push(BoundProjection::Column(
                                    i,
                                    schema.columns[i].name.clone(),
                                ));
                            }
                        }
                    } else {
                        return Err(SqlError::UnknownTable(qualifier));
                    }
                }
            }
        }

        // WHERE
        let filter = select
            .selection
            .as_ref()
            .map(|expr| self.bind_expr_full(expr, &schema, &alias_map, outer_schema))
            .transpose()?;

        // GROUP BY
        let mut group_by = Vec::new();
        let mut grouping_sets: Vec<Vec<usize>> = Vec::new();
        match &select.group_by {
            ast::GroupByExpr::All(_) => {
                // GROUP BY ALL: automatically group by all non-aggregate projections
                for proj in &projections {
                    match proj {
                        BoundProjection::Column(idx, _) => {
                            group_by.push(*idx);
                        }
                        BoundProjection::Expr(_, _) => {
                            // Expression projection — use its position as group key
                            // (it will be evaluated during grouping)
                        }
                        BoundProjection::Aggregate(..) | BoundProjection::Window(..) => {
                            // Skip aggregates and window functions
                        }
                    }
                }
            }
            ast::GroupByExpr::Expressions(exprs, _) => {
                for expr in exprs {
                    match expr {
                        Expr::GroupingSets(sets) => {
                            for set in sets {
                                let mut resolved = Vec::new();
                                for e in set {
                                    let col_idx = self.resolve_group_by_expr(
                                        e,
                                        &schema,
                                        &alias_map,
                                        outer_schema,
                                        &mut projections,
                                    )?;
                                    resolved.push(col_idx);
                                }
                                // Add all columns from this set to group_by (union)
                                for &c in &resolved {
                                    if !group_by.contains(&c) {
                                        group_by.push(c);
                                    }
                                }
                                grouping_sets.push(resolved);
                            }
                        }
                        Expr::Cube(lists) => {
                            let mut cols = Vec::new();
                            for list in lists {
                                for e in list {
                                    let col_idx = self.resolve_group_by_expr(
                                        e,
                                        &schema,
                                        &alias_map,
                                        outer_schema,
                                        &mut projections,
                                    )?;
                                    cols.push(col_idx);
                                }
                            }
                            // Generate all 2^n subsets
                            let n = cols.len();
                            for mask in 0..(1u64 << n) {
                                let mut set = Vec::new();
                                for (i, &col) in cols.iter().enumerate() {
                                    if mask & (1u64 << i) != 0 {
                                        set.push(col);
                                    }
                                }
                                grouping_sets.push(set);
                            }
                            for &c in &cols {
                                if !group_by.contains(&c) {
                                    group_by.push(c);
                                }
                            }
                        }
                        Expr::Rollup(lists) => {
                            let mut cols = Vec::new();
                            for list in lists {
                                for e in list {
                                    let col_idx = self.resolve_group_by_expr(
                                        e,
                                        &schema,
                                        &alias_map,
                                        outer_schema,
                                        &mut projections,
                                    )?;
                                    cols.push(col_idx);
                                }
                            }
                            // Generate progressive prefixes: [all], [all-1], ..., [first], []
                            for i in (0..=cols.len()).rev() {
                                grouping_sets.push(cols[..i].to_vec());
                            }
                            for &c in &cols {
                                if !group_by.contains(&c) {
                                    group_by.push(c);
                                }
                            }
                        }
                        Expr::Identifier(ident) => {
                            let col_idx = schema
                                .find_column(&ident.value)
                                .ok_or_else(|| SqlError::UnknownColumn(ident.value.clone()))?;
                            group_by.push(col_idx);
                        }
                        Expr::Value(Value::Number(n, _)) => {
                            let idx: usize = n
                                .parse()
                                .map_err(|_| SqlError::Parse("Invalid GROUP BY index".into()))?;
                            if idx == 0 || idx > projections.len() {
                                return Err(SqlError::Parse("GROUP BY index out of range".into()));
                            }
                            match &projections[idx - 1] {
                                BoundProjection::Column(col_idx, _) => {
                                    group_by.push(*col_idx);
                                }
                                BoundProjection::Expr(_, _) => {
                                    // Expression projection — group by its index
                                    group_by.push(idx - 1);
                                }
                                _ => {
                                    return Err(SqlError::Unsupported(
                                        "GROUP BY on aggregate or window".into(),
                                    ));
                                }
                            }
                        }
                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                            let col_name = &parts[1].value;
                            let col_idx = schema
                                .find_column(col_name)
                                .ok_or_else(|| SqlError::UnknownColumn(col_name.clone()))?;
                            group_by.push(col_idx);
                        }
                        other => {
                            // Complex expression: bind it and try to resolve to a column
                            let bound =
                                self.bind_expr_full(other, &schema, &alias_map, outer_schema)?;
                            if let BoundExpr::ColumnRef(col_idx) = &bound {
                                group_by.push(*col_idx);
                            } else {
                                // Add as hidden projection and group by its index
                                let idx = projections.len();
                                projections
                                    .push(BoundProjection::Expr(bound, format!("{}", other)));
                                group_by.push(idx);
                            }
                        }
                    }
                }
            }
        }

        // HAVING
        let having = select
            .having
            .as_ref()
            .map(|expr| self.bind_expr_full(expr, &schema, &alias_map, outer_schema))
            .transpose()?;

        // ORDER BY
        let visible_projection_count = projections.len();
        let mut order_by = Vec::new();
        for ob in &query.order_by.as_ref().map_or(vec![], |o| o.exprs.clone()) {
            let col_idx =
                self.resolve_order_by_expr(&ob.expr, &mut projections, &schema, &alias_map)?;
            let asc = ob.asc.unwrap_or(true);
            order_by.push(BoundOrderBy {
                column_idx: col_idx,
                asc,
            });
        }

        // LIMIT
        let limit = query
            .limit
            .as_ref()
            .map(|expr| Self::eval_limit_offset_expr(expr, "LIMIT"))
            .transpose()?;

        // OFFSET
        let offset = query
            .offset
            .as_ref()
            .map(|o| Self::eval_limit_offset_expr(&o.value, "OFFSET"))
            .transpose()?;

        // DISTINCT / DISTINCT ON
        let distinct = match &select.distinct {
            Some(ast::Distinct::Distinct) => DistinctMode::All,
            Some(ast::Distinct::On(exprs)) => {
                let mut on_indices = Vec::new();
                for expr in exprs {
                    // Resolve each DISTINCT ON expression to a projection index
                    let idx =
                        self.resolve_order_by_expr(expr, &mut projections, &schema, &alias_map)?;
                    on_indices.push(idx);
                }
                DistinctMode::On(on_indices)
            }
            None => DistinctMode::None,
        };

        Ok(BoundSelect {
            table_id: left_schema.id,
            table_name,
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
            joins,
            ctes: bound_ctes,
            unions: vec![],
            virtual_rows: vec![],
        })
    }

    /// Build a temporary combined schema for resolving JOIN ON conditions.
    pub(crate) fn build_combined_schema(
        &self,
        left_schema: &TableSchema,
        combined_so_far: &[falcon_common::schema::ColumnDef],
        right_schema: &TableSchema,
        _right_col_offset: usize,
    ) -> TableSchema {
        let mut columns = combined_so_far.to_vec();
        for col in &right_schema.columns {
            columns.push(col.clone());
        }
        TableSchema {
            id: left_schema.id,
            name: left_schema.name.clone(),
            columns,
            primary_key_columns: left_schema.primary_key_columns.clone(),
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        }
    }

    /// Resolve a single GROUP BY element expression to a column index.
    /// Shared by plain GROUP BY, GROUPING SETS, CUBE, and ROLLUP.
    fn resolve_group_by_expr(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        alias_map: &AliasMap,
        outer_schema: Option<&TableSchema>,
        projections: &mut Vec<BoundProjection>,
    ) -> Result<usize, SqlError> {
        match expr {
            Expr::Identifier(ident) => schema
                .find_column(&ident.value)
                .ok_or_else(|| SqlError::UnknownColumn(ident.value.clone())),
            Expr::Value(Value::Number(n, _)) => {
                let idx: usize = n
                    .parse()
                    .map_err(|_| SqlError::Parse("Invalid GROUP BY index".into()))?;
                if idx == 0 || idx > projections.len() {
                    return Err(SqlError::Parse("GROUP BY index out of range".into()));
                }
                match &projections[idx - 1] {
                    BoundProjection::Column(col_idx, _) => Ok(*col_idx),
                    BoundProjection::Expr(_, _) => Ok(idx - 1),
                    _ => Err(SqlError::Unsupported(
                        "GROUP BY on aggregate or window".into(),
                    )),
                }
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let col_name = &parts[1].value;
                schema
                    .find_column(col_name)
                    .ok_or_else(|| SqlError::UnknownColumn(col_name.clone()))
            }
            other => {
                let bound = self.bind_expr_full(other, schema, alias_map, outer_schema)?;
                if let BoundExpr::ColumnRef(col_idx) = &bound {
                    Ok(*col_idx)
                } else {
                    let idx = projections.len();
                    projections.push(BoundProjection::Expr(bound, format!("{}", other)));
                    Ok(idx)
                }
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn bind_projection_expr_with_aliases(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        aliases: &AliasMap,
    ) -> Result<BoundProjection, SqlError> {
        let empty = std::collections::HashMap::new();
        self.bind_projection_expr_full(expr, schema, aliases, None, &empty)
    }

    pub(crate) fn bind_projection_expr_full(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        aliases: &AliasMap,
        outer_schema: Option<&TableSchema>,
        named_windows: &std::collections::HashMap<String, ast::WindowSpec>,
    ) -> Result<BoundProjection, SqlError> {
        match expr {
            Expr::Identifier(ident) => {
                let col_idx = schema
                    .find_column(&ident.value)
                    .ok_or_else(|| SqlError::UnknownColumn(ident.value.clone()))?;
                Ok(BoundProjection::Column(
                    col_idx,
                    schema.columns[col_idx].name.clone(),
                ))
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let bound = self.bind_expr_full(expr, schema, aliases, outer_schema)?;
                if let BoundExpr::ColumnRef(idx) = &bound {
                    Ok(BoundProjection::Column(
                        *idx,
                        schema.columns[*idx].name.clone(),
                    ))
                } else {
                    Ok(BoundProjection::Expr(bound, format!("{}", expr)))
                }
            }
            Expr::Function(func) if func.over.is_some() => {
                // Window function: ROW_NUMBER/RANK/DENSE_RANK/SUM/COUNT/AVG OVER (...)
                let func_name = func.name.to_string().to_uppercase();

                // Parse the window function type
                let col_idx = self.bind_agg_col_idx(func, schema)?;
                let window_func = match func_name.as_str() {
                    "ROW_NUMBER" => WindowFunc::RowNumber,
                    "RANK" => WindowFunc::Rank,
                    "DENSE_RANK" => WindowFunc::DenseRank,
                    "NTILE" => {
                        let n = self.extract_int_arg(func, 0)?.unwrap_or(1);
                        WindowFunc::Ntile(n)
                    }
                    "LAG" => {
                        let idx =
                            col_idx.ok_or(SqlError::Unsupported("LAG requires column".into()))?;
                        let offset = self.extract_int_arg(func, 1)?.unwrap_or(1);
                        WindowFunc::Lag(idx, offset)
                    }
                    "LEAD" => {
                        let idx =
                            col_idx.ok_or(SqlError::Unsupported("LEAD requires column".into()))?;
                        let offset = self.extract_int_arg(func, 1)?.unwrap_or(1);
                        WindowFunc::Lead(idx, offset)
                    }
                    "FIRST_VALUE" => {
                        let idx = col_idx
                            .ok_or(SqlError::Unsupported("FIRST_VALUE requires column".into()))?;
                        WindowFunc::FirstValue(idx)
                    }
                    "LAST_VALUE" => {
                        let idx = col_idx
                            .ok_or(SqlError::Unsupported("LAST_VALUE requires column".into()))?;
                        WindowFunc::LastValue(idx)
                    }
                    "PERCENT_RANK" => WindowFunc::PercentRank,
                    "CUME_DIST" => WindowFunc::CumeDist,
                    "NTH_VALUE" => {
                        let idx = col_idx
                            .ok_or(SqlError::Unsupported("NTH_VALUE requires column".into()))?;
                        let n = self.extract_int_arg(func, 1)?.unwrap_or(1);
                        WindowFunc::NthValue(idx, n)
                    }
                    "COUNT" => WindowFunc::Agg(AggFunc::Count, col_idx),
                    "SUM" => WindowFunc::Agg(AggFunc::Sum, col_idx),
                    "AVG" => WindowFunc::Agg(AggFunc::Avg, col_idx),
                    "MIN" => WindowFunc::Agg(AggFunc::Min, col_idx),
                    "MAX" => WindowFunc::Agg(AggFunc::Max, col_idx),
                    _ => {
                        return Err(SqlError::Unsupported(format!(
                            "Window function: {}",
                            func_name
                        )))
                    }
                };

                // Parse OVER clause — resolve named window references
                let window_spec = match func.over.as_ref().unwrap() {
                    ast::WindowType::WindowSpec(spec) => std::borrow::Cow::Borrowed(spec),
                    ast::WindowType::NamedWindow(ident) => {
                        let name = ident.value.to_lowercase();
                        let spec = named_windows
                            .get(&name)
                            .ok_or_else(|| SqlError::UnknownColumn(format!("window '{}'", name)))?;
                        std::borrow::Cow::Owned(spec.clone())
                    }
                };

                let partition_by: Vec<usize> = window_spec
                    .partition_by
                    .iter()
                    .map(|e| {
                        let bound = self.bind_expr_with_aliases(e, schema, aliases)?;
                        match bound {
                            BoundExpr::ColumnRef(idx) => Ok(idx),
                            // Allow positional reference: PARTITION BY 1
                            BoundExpr::Literal(Datum::Int32(n))
                                if n >= 1 && (n as usize) <= schema.columns.len() =>
                            {
                                Ok((n - 1) as usize)
                            }
                            _ => Err(SqlError::Unsupported(
                                "Non-column PARTITION BY expression".into(),
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let order_by: Vec<BoundOrderBy> = window_spec
                    .order_by
                    .iter()
                    .map(|ob| {
                        let bound = self.bind_expr_with_aliases(&ob.expr, schema, aliases)?;
                        match bound {
                            BoundExpr::ColumnRef(idx) => Ok(BoundOrderBy {
                                column_idx: idx,
                                asc: ob.asc.unwrap_or(true),
                            }),
                            // Allow positional reference: ORDER BY 1
                            BoundExpr::Literal(Datum::Int32(n))
                                if n >= 1 && (n as usize) <= schema.columns.len() =>
                            {
                                Ok(BoundOrderBy {
                                    column_idx: (n - 1) as usize,
                                    asc: ob.asc.unwrap_or(true),
                                })
                            }
                            _ => Err(SqlError::Unsupported(
                                "Non-column ORDER BY in window".into(),
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // Parse window frame
                let frame = if let Some(ref wf) = window_spec.window_frame {
                    let mode = match wf.units {
                        ast::WindowFrameUnits::Rows => WindowFrameMode::Rows,
                        ast::WindowFrameUnits::Range => WindowFrameMode::Range,
                        ast::WindowFrameUnits::Groups => WindowFrameMode::Groups,
                    };
                    let start = Self::bind_window_frame_bound(&wf.start_bound);
                    let end = wf
                        .end_bound
                        .as_ref()
                        .map(Self::bind_window_frame_bound)
                        .unwrap_or(WindowFrameBound::CurrentRow);
                    WindowFrame { mode, start, end }
                } else if !order_by.is_empty() {
                    WindowFrame::default_with_order_by()
                } else {
                    WindowFrame::default()
                };

                let alias = format!("{}", expr);
                Ok(BoundProjection::Window(BoundWindowFunc {
                    func: window_func,
                    partition_by,
                    order_by,
                    frame,
                    alias,
                }))
            }
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                // Handle STRING_AGG specially (two args: column, separator)
                if func_name == "STRING_AGG" {
                    if let ast::FunctionArguments::List(args) = &func.args {
                        if args.args.len() >= 2 {
                            // First arg: expression
                            let agg_expr = if let Some(ast::FunctionArg::Unnamed(
                                ast::FunctionArgExpr::Expr(inner_expr),
                            )) = args.args.first()
                            {
                                Some(self.bind_expr_with_aliases(inner_expr, schema, aliases)?)
                            } else {
                                return Err(SqlError::Unsupported(
                                    "STRING_AGG requires expression argument".into(),
                                ));
                            };
                            // Second arg: separator literal
                            let sep = if let Some(ast::FunctionArg::Unnamed(
                                ast::FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                                    s,
                                ))),
                            )) = args.args.get(1)
                            {
                                s.clone()
                            } else {
                                ",".to_string()
                            };
                            let is_distinct = matches!(
                                args.duplicate_treatment,
                                Some(ast::DuplicateTreatment::Distinct)
                            );
                            let alias = format!("string_agg({})", func);
                            let bound_filter =
                                self.bind_agg_filter(&func.filter, schema, aliases)?;
                            return Ok(BoundProjection::Aggregate(
                                AggFunc::StringAgg(sep),
                                agg_expr,
                                alias,
                                is_distinct,
                                bound_filter,
                            ));
                        }
                    }
                    return Err(SqlError::Unsupported(
                        "STRING_AGG requires (column, separator)".into(),
                    ));
                }

                let agg = match func_name.as_str() {
                    "COUNT" => AggFunc::Count,
                    "SUM" => AggFunc::Sum,
                    "AVG" => AggFunc::Avg,
                    "MIN" => AggFunc::Min,
                    "MAX" => AggFunc::Max,
                    "BOOL_AND" => AggFunc::BoolAnd,
                    "BOOL_OR" => AggFunc::BoolOr,
                    "ARRAY_AGG" => AggFunc::ArrayAgg,
                    // Statistical aggregates
                    "STDDEV" | "STDDEV_SAMP" => AggFunc::StddevSamp,
                    "STDDEV_POP" => AggFunc::StddevPop,
                    "VARIANCE" | "VAR_SAMP" => AggFunc::VarSamp,
                    "VAR_POP" => AggFunc::VarPop,
                    // Two-argument statistical aggregates
                    "CORR" => AggFunc::Corr,
                    "COVAR_POP" => AggFunc::CovarPop,
                    "COVAR_SAMP" => AggFunc::CovarSamp,
                    "REGR_SLOPE" => AggFunc::RegrSlope,
                    "REGR_INTERCEPT" => AggFunc::RegrIntercept,
                    "REGR_R2" => AggFunc::RegrR2,
                    "REGR_COUNT" => AggFunc::RegrCount,
                    "REGR_AVGX" => AggFunc::RegrAvgX,
                    "REGR_AVGY" => AggFunc::RegrAvgY,
                    "REGR_SXX" => AggFunc::RegrSXX,
                    "REGR_SYY" => AggFunc::RegrSYY,
                    "REGR_SXY" => AggFunc::RegrSXY,
                    // Ordered-set aggregates
                    "MODE" => AggFunc::Mode,
                    // Bit aggregates
                    "BIT_AND" => AggFunc::BitAndAgg,
                    "BIT_OR" => AggFunc::BitOrAgg,
                    "BIT_XOR" => AggFunc::BitXorAgg,
                    _ => {
                        // Not an aggregate — delegate to bind_expr (handles COALESCE, NULLIF, etc.)
                        let bound = self.bind_expr_full(expr, schema, aliases, outer_schema)?;
                        return Ok(BoundProjection::Expr(bound, format!("{}", expr)));
                    }
                };

                let (agg_expr, agg_distinct) = match &func.args {
                    ast::FunctionArguments::List(args) => {
                        let is_distinct = matches!(
                            args.duplicate_treatment,
                            Some(ast::DuplicateTreatment::Distinct)
                        );
                        let bound_arg = if args.args.is_empty() {
                            None // COUNT(*)
                        } else if let Some(ast::FunctionArg::Unnamed(
                            ast::FunctionArgExpr::Wildcard,
                        )) = args.args.first()
                        {
                            None // COUNT(*)
                        } else if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                            inner_expr,
                        ))) = args.args.first()
                        {
                            Some(self.bind_expr_with_aliases(inner_expr, schema, aliases)?)
                        } else {
                            return Err(SqlError::Unsupported(
                                "Unsupported aggregate argument form".into(),
                            ));
                        };
                        (bound_arg, is_distinct)
                    }
                    ast::FunctionArguments::None => (None, false),
                    _ => return Err(SqlError::Unsupported("Subquery in aggregate".into())),
                };

                let alias = format!(
                    "{}({}{})",
                    func_name.to_lowercase(),
                    if agg_distinct { "distinct " } else { "" },
                    expr
                );

                let bound_filter = self.bind_agg_filter(&func.filter, schema, aliases)?;
                Ok(BoundProjection::Aggregate(
                    agg,
                    agg_expr,
                    alias,
                    agg_distinct,
                    bound_filter,
                ))
            }
            _ => {
                let bound = self.bind_expr_full(expr, schema, aliases, outer_schema)?;
                Ok(BoundProjection::Expr(bound, format!("{}", expr)))
            }
        }
    }

    /// Bind an aggregate FILTER (WHERE ...) clause from the AST.
    fn bind_agg_filter(
        &self,
        filter: &Option<Box<Expr>>,
        schema: &TableSchema,
        aliases: &AliasMap,
    ) -> Result<Option<Box<BoundExpr>>, SqlError> {
        match filter {
            Some(f) => {
                let bound = self.bind_expr_with_aliases(f, schema, aliases)?;
                Ok(Some(Box::new(bound)))
            }
            None => Ok(None),
        }
    }

    /// Convert a sqlparser WindowFrameBound to our internal WindowFrameBound.
    fn bind_window_frame_bound(bound: &ast::WindowFrameBound) -> WindowFrameBound {
        match bound {
            ast::WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
            ast::WindowFrameBound::Preceding(None) => WindowFrameBound::UnboundedPreceding,
            ast::WindowFrameBound::Preceding(Some(expr)) => {
                if let Expr::Value(Value::Number(n, _)) = expr.as_ref() {
                    WindowFrameBound::Preceding(n.parse::<u64>().unwrap_or(0))
                } else {
                    WindowFrameBound::UnboundedPreceding
                }
            }
            ast::WindowFrameBound::Following(None) => WindowFrameBound::UnboundedFollowing,
            ast::WindowFrameBound::Following(Some(expr)) => {
                if let Expr::Value(Value::Number(n, _)) = expr.as_ref() {
                    WindowFrameBound::Following(n.parse::<u64>().unwrap_or(0))
                } else {
                    WindowFrameBound::UnboundedFollowing
                }
            }
        }
    }

    /// Bind a VALUES clause into a BoundSelect with virtual rows.
    fn bind_values_clause(
        &self,
        values: &ast::Values,
        query: &ast::Query,
    ) -> Result<BoundSelect, SqlError> {
        use falcon_common::datum::OwnedRow;
        use falcon_common::schema::ColumnDef;

        if values.rows.is_empty() {
            return Err(SqlError::Parse(
                "VALUES clause requires at least one row".into(),
            ));
        }

        let num_cols = values.rows[0].len();
        let dummy_table_id = TableId(2_999_999);

        // Build virtual rows by evaluating each value expression
        let mut virtual_rows = Vec::new();
        for row_exprs in &values.rows {
            if row_exprs.len() != num_cols {
                return Err(SqlError::Parse(
                    "All VALUES rows must have the same number of columns".into(),
                ));
            }
            let mut datums = Vec::with_capacity(num_cols);
            for expr in row_exprs {
                let datum = self.bind_value_expr(expr)?;
                datums.push(datum);
            }
            virtual_rows.push(OwnedRow::new(datums));
        }

        // Infer column types from first row
        let columns: Vec<ColumnDef> = (0..num_cols)
            .map(|i| {
                let dt = virtual_rows[0].values[i]
                    .data_type()
                    .unwrap_or(DataType::Text);
                ColumnDef {
                    id: ColumnId(i as u32),
                    name: format!("column{}", i + 1),
                    data_type: dt,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                }
            })
            .collect();

        let schema = TableSchema {
            id: dummy_table_id,
            name: "__values__".to_string(),
            columns,
            primary_key_columns: vec![],
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };

        let projections: Vec<BoundProjection> = (0..num_cols)
            .map(|i| BoundProjection::Column(i, format!("column{}", i + 1)))
            .collect();
        let visible_projection_count = projections.len();

        // Parse LIMIT/OFFSET from the outer query
        let limit = query
            .limit
            .as_ref()
            .map(|expr| match expr {
                Expr::Value(Value::Number(n, _)) => n
                    .parse::<usize>()
                    .map_err(|_| SqlError::Parse("Invalid LIMIT".into())),
                _ => Err(SqlError::Parse("LIMIT must be a number".into())),
            })
            .transpose()?;
        let offset = query
            .offset
            .as_ref()
            .map(|o| match &o.value {
                Expr::Value(Value::Number(n, _)) => n
                    .parse::<usize>()
                    .map_err(|_| SqlError::Parse("Invalid OFFSET".into())),
                _ => Err(SqlError::Parse("OFFSET must be a number".into())),
            })
            .transpose()?;

        Ok(BoundSelect {
            table_id: dummy_table_id,
            table_name: "__values__".to_string(),
            schema,
            projections,
            visible_projection_count,
            filter: None,
            group_by: vec![],
            grouping_sets: vec![],
            having: None,
            order_by: vec![],
            limit,
            offset,
            distinct: DistinctMode::None,
            joins: vec![],
            ctes: vec![],
            unions: vec![],
            virtual_rows,
        })
    }

    /// Evaluate a simple expression to a Datum for VALUES clause.
    fn bind_value_expr(&self, expr: &Expr) -> Result<Datum, SqlError> {
        match expr {
            Expr::Value(v) => Self::bind_value_literal(v),
            Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr: inner,
            } => {
                let val = self.bind_value_expr(inner)?;
                match val {
                    Datum::Int32(n) => Ok(Datum::Int32(-n)),
                    Datum::Int64(n) => Ok(Datum::Int64(-n)),
                    Datum::Float64(n) => Ok(Datum::Float64(-n)),
                    _ => Err(SqlError::Parse("Cannot negate non-numeric value".into())),
                }
            }
            Expr::UnaryOp {
                op: ast::UnaryOperator::Plus,
                expr: inner,
            } => self.bind_value_expr(inner),
            Expr::Nested(inner) => self.bind_value_expr(inner),
            Expr::TypedString { data_type, value } => {
                // e.g. DATE '2024-01-01', TIMESTAMP '...'
                let target = format!("{}", data_type).to_lowercase();
                Self::parse_typed_datum(value, &target)
            }
            Expr::Cast {
                expr: cast_expr,
                data_type,
                ..
            } => {
                let val = self.bind_value_expr(cast_expr)?;
                let target = format!("{}", data_type).to_lowercase();
                match &val {
                    Datum::Text(s) => Self::parse_typed_datum(s, &target),
                    Datum::Int32(n)
                        if target.contains("float")
                            || target.contains("double")
                            || target.contains("real")
                            || target.contains("numeric") =>
                    {
                        Ok(Datum::Float64(*n as f64))
                    }
                    Datum::Int32(n) if target.contains("bigint") || target.contains("int8") => {
                        Ok(Datum::Int64(*n as i64))
                    }
                    Datum::Int64(n)
                        if target.contains("int")
                            && !target.contains("bigint")
                            && !target.contains("int8") =>
                    {
                        Ok(Datum::Int32(*n as i32))
                    }
                    Datum::Float64(f) if target.contains("int") => Ok(Datum::Int64(*f as i64)),
                    _ => Ok(val),
                }
            }
            Expr::Array(ast::Array { elem, .. }) => {
                let elements: Vec<Datum> = elem
                    .iter()
                    .map(|e| self.bind_value_expr(e))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Datum::Array(elements))
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.bind_value_expr(left)?;
                let r = self.bind_value_expr(right)?;
                Self::eval_const_binop(&l, op, &r)
            }
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                Err(SqlError::Unsupported(format!(
                    "Function '{}' in VALUES clause (use INSERT ... SELECT instead)",
                    name
                )))
            }
            _ => Err(SqlError::Unsupported(
                "Complex expression in VALUES clause".into(),
            )),
        }
    }

    /// Evaluate a constant binary operation on two Datum values.
    fn eval_const_binop(
        left: &Datum,
        op: &ast::BinaryOperator,
        right: &Datum,
    ) -> Result<Datum, SqlError> {
        use ast::BinaryOperator::*;
        match (left, op, right) {
            (Datum::Int32(a), Plus, Datum::Int32(b)) => Ok(Datum::Int32(a + b)),
            (Datum::Int32(a), Minus, Datum::Int32(b)) => Ok(Datum::Int32(a - b)),
            (Datum::Int32(a), Multiply, Datum::Int32(b)) => Ok(Datum::Int32(a * b)),
            (Datum::Int32(a), Divide, Datum::Int32(b)) if *b != 0 => Ok(Datum::Int32(a / b)),
            (Datum::Int64(a), Plus, Datum::Int64(b)) => Ok(Datum::Int64(a + b)),
            (Datum::Int64(a), Minus, Datum::Int64(b)) => Ok(Datum::Int64(a - b)),
            (Datum::Int64(a), Multiply, Datum::Int64(b)) => Ok(Datum::Int64(a * b)),
            (Datum::Int64(a), Divide, Datum::Int64(b)) if *b != 0 => Ok(Datum::Int64(a / b)),
            (Datum::Float64(a), Plus, Datum::Float64(b)) => Ok(Datum::Float64(a + b)),
            (Datum::Float64(a), Minus, Datum::Float64(b)) => Ok(Datum::Float64(a - b)),
            (Datum::Float64(a), Multiply, Datum::Float64(b)) => Ok(Datum::Float64(a * b)),
            (Datum::Float64(a), Divide, Datum::Float64(b)) if *b != 0.0 => {
                Ok(Datum::Float64(a / b))
            }
            // Mixed int/float promotion
            (Datum::Int32(a), _, Datum::Float64(_)) => {
                Self::eval_const_binop(&Datum::Float64(*a as f64), op, right)
            }
            (Datum::Float64(_), _, Datum::Int32(b)) => {
                Self::eval_const_binop(left, op, &Datum::Float64(*b as f64))
            }
            (Datum::Int32(a), _, Datum::Int64(_)) => {
                Self::eval_const_binop(&Datum::Int64(*a as i64), op, right)
            }
            (Datum::Int64(_), _, Datum::Int32(b)) => {
                Self::eval_const_binop(left, op, &Datum::Int64(*b as i64))
            }
            // String concat
            (Datum::Text(a), StringConcat, Datum::Text(b)) => {
                Ok(Datum::Text(format!("{}{}", a, b)))
            }
            _ => Err(SqlError::Unsupported(format!(
                "Constant binary op {:?} in VALUES",
                op
            ))),
        }
    }

    /// Parse a text value into a Datum given a SQL type name (e.g. "date", "timestamp", "boolean").
    /// For date/timestamp types, we store as text and let the executor handle the cast at runtime,
    /// since the frontend crate does not depend on chrono.
    fn parse_typed_datum(value: &str, target: &str) -> Result<Datum, SqlError> {
        if target.contains("bool") {
            match value.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" | "on" => Ok(Datum::Boolean(true)),
                "false" | "f" | "0" | "no" | "off" => Ok(Datum::Boolean(false)),
                _ => Err(SqlError::Parse(format!("Invalid BOOLEAN: '{}'", value))),
            }
        } else if target.contains("int") {
            let n: i64 = value
                .parse()
                .map_err(|_| SqlError::Parse(format!("Invalid INT: '{}'", value)))?;
            if target.contains("bigint") || target.contains("int8") {
                Ok(Datum::Int64(n))
            } else if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                Ok(Datum::Int32(n as i32))
            } else {
                Ok(Datum::Int64(n))
            }
        } else if target.contains("float")
            || target.contains("double")
            || target.contains("real")
            || target.contains("numeric")
            || target.contains("decimal")
        {
            let f: f64 = value
                .parse()
                .map_err(|_| SqlError::Parse(format!("Invalid FLOAT: '{}'", value)))?;
            Ok(Datum::Float64(f))
        } else {
            // For date, timestamp, text, and other types: keep as text.
            // The executor's CAST logic will handle the conversion at runtime.
            Ok(Datum::Text(value.to_string()))
        }
    }

    fn bind_value_literal(v: &Value) -> Result<Datum, SqlError> {
        match v {
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        Ok(Datum::Int32(i as i32))
                    } else {
                        Ok(Datum::Int64(i))
                    }
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Datum::Float64(f))
                } else {
                    Err(SqlError::Parse(format!("Cannot parse number: {}", n)))
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(Datum::Text(s.clone()))
            }
            Value::Boolean(b) => Ok(Datum::Boolean(*b)),
            Value::Null => Ok(Datum::Null),
            _ => Err(SqlError::Unsupported(format!(
                "Value type in VALUES: {:?}",
                v
            ))),
        }
    }

    pub(crate) fn resolve_order_by_expr(
        &self,
        expr: &Expr,
        projections: &mut Vec<BoundProjection>,
        schema: &TableSchema,
        aliases: &AliasMap,
    ) -> Result<usize, SqlError> {
        match expr {
            Expr::Identifier(ident) => {
                // Try to match against projection aliases first
                for (i, proj) in projections.iter().enumerate() {
                    let alias = match proj {
                        BoundProjection::Column(_, a) => a,
                        BoundProjection::Aggregate(_, _, a, _, _) => a,
                        BoundProjection::Expr(_, a) => a,
                        BoundProjection::Window(wf) => &wf.alias,
                    };
                    if alias.to_lowercase() == ident.value.to_lowercase() {
                        return Ok(i);
                    }
                }
                // Try column name in existing projections
                for (i, proj) in projections.iter().enumerate() {
                    if let BoundProjection::Column(col_idx, _) = proj {
                        if schema.columns[*col_idx].name.to_lowercase()
                            == ident.value.to_lowercase()
                        {
                            return Ok(i);
                        }
                    }
                }
                // Not in projections — try source schema and add as hidden projection
                if let Some(col_idx) = schema.find_column(&ident.value) {
                    let idx = projections.len();
                    projections.push(BoundProjection::Column(
                        col_idx,
                        schema.columns[col_idx].name.clone(),
                    ));
                    return Ok(idx);
                }
                Err(SqlError::UnknownColumn(ident.value.clone()))
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                // table.column in ORDER BY — resolve via bind_expr then find matching projection
                let col_name = parts[1].value.to_lowercase();
                // Try to find in projections by column name
                for (i, proj) in projections.iter().enumerate() {
                    if let BoundProjection::Column(col_idx, _) = proj {
                        if schema.columns[*col_idx].name.to_lowercase() == col_name {
                            // Verify it's the right table by using CompoundIdentifier resolution
                            let bound = self.bind_expr_with_aliases(expr, schema, aliases)?;
                            if let BoundExpr::ColumnRef(resolved_idx) = bound {
                                if resolved_idx == *col_idx {
                                    return Ok(i);
                                }
                            }
                        }
                    }
                }
                // Not in projections — try alias resolution and add as hidden projection
                let bound = self.bind_expr_with_aliases(expr, schema, aliases)?;
                if let BoundExpr::ColumnRef(resolved_idx) = &bound {
                    let idx = projections.len();
                    projections.push(BoundProjection::Column(
                        *resolved_idx,
                        schema.columns[*resolved_idx].name.clone(),
                    ));
                    return Ok(idx);
                }
                Err(SqlError::UnknownColumn(format!(
                    "{}.{}",
                    parts[0].value, parts[1].value
                )))
            }
            Expr::Value(Value::Number(n, _)) => {
                let idx: usize = n
                    .parse()
                    .map_err(|_| SqlError::Parse("Invalid ORDER BY index".into()))?;
                if idx == 0 || idx > projections.len() {
                    return Err(SqlError::Parse("ORDER BY index out of range".into()));
                }
                Ok(idx - 1) // 1-indexed to 0-indexed
            }
            _ => {
                // Complex expression in ORDER BY — bind it and add as hidden projection
                let bound = self.bind_expr_with_aliases(expr, schema, aliases)?;
                let alias = format!("{}", expr);
                let idx = projections.len();
                projections.push(BoundProjection::Expr(bound, alias));
                Ok(idx)
            }
        }
    }

    /// Evaluate a LIMIT or OFFSET expression at bind time.
    /// Supports number literals, CAST expressions, nested parentheses,
    /// and simple arithmetic on constants.
    fn eval_limit_offset_expr(expr: &Expr, label: &str) -> Result<usize, SqlError> {
        match expr {
            Expr::Value(Value::Number(n, _)) => n
                .parse::<usize>()
                .map_err(|_| SqlError::Parse(format!("Invalid {}", label))),
            Expr::Value(Value::Null) => {
                // LIMIT NULL means no limit (treat as very large)
                Ok(usize::MAX)
            }
            Expr::UnaryOp {
                op: ast::UnaryOperator::Plus,
                expr: inner,
            } => Self::eval_limit_offset_expr(inner, label),
            Expr::Nested(inner) => Self::eval_limit_offset_expr(inner, label),
            Expr::Cast {
                expr: inner,
                data_type,
                ..
            } => {
                // CAST(expr AS INT) — evaluate inner then check it's numeric
                let val = Self::eval_limit_offset_expr(inner, label)?;
                let type_str = format!("{}", data_type).to_uppercase();
                if type_str.contains("INT")
                    || type_str.contains("BIGINT")
                    || type_str.contains("NUMERIC")
                {
                    Ok(val)
                } else {
                    Err(SqlError::Parse(format!(
                        "{} CAST to non-integer type: {}",
                        label, data_type
                    )))
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = Self::eval_limit_offset_expr(left, label)?;
                let r = Self::eval_limit_offset_expr(right, label)?;
                match op {
                    ast::BinaryOperator::Plus => Ok(l.saturating_add(r)),
                    ast::BinaryOperator::Minus => Ok(l.saturating_sub(r)),
                    ast::BinaryOperator::Multiply => Ok(l.saturating_mul(r)),
                    ast::BinaryOperator::Divide => {
                        if r == 0 {
                            Err(SqlError::Parse(format!("Division by zero in {}", label)))
                        } else {
                            Ok(l / r)
                        }
                    }
                    _ => Err(SqlError::Parse(format!(
                        "{} expression contains unsupported operator",
                        label
                    ))),
                }
            }
            _ => Err(SqlError::Parse(format!(
                "{} must be a constant expression",
                label
            ))),
        }
    }

    /// Derive column definitions from bound projections (used for CTE schema derivation).
    fn schema_from_projections(
        projections: &[BoundProjection],
        visible_count: usize,
        source_schema: &TableSchema,
    ) -> Vec<falcon_common::schema::ColumnDef> {
        let mut columns = Vec::new();
        for proj in &projections[..visible_count] {
            let (col_name, data_type) = match proj {
                BoundProjection::Column(idx, alias) => {
                    let col = &source_schema.columns[*idx];
                    let name = if alias.is_empty() {
                        col.name.clone()
                    } else {
                        alias.clone()
                    };
                    (name, col.data_type.clone())
                }
                BoundProjection::Aggregate(_, _, alias, _, _) => (alias.clone(), DataType::Int64),
                BoundProjection::Expr(_, alias) => (alias.clone(), DataType::Text),
                BoundProjection::Window(wf) => (wf.alias.clone(), DataType::Int64),
            };
            columns.push(falcon_common::schema::ColumnDef {
                id: ColumnId(columns.len() as u32),
                name: col_name,
                data_type,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
            });
        }
        columns
    }

    /// Try to bind a virtual catalog table (information_schema / pg_catalog).
    /// Returns (column_defs, virtual_rows) if the table name matches a known virtual table.
    fn try_bind_virtual_catalog_table(
        &self,
        table_name_lower: &str,
        _ctes: &[BoundCte],
    ) -> Option<(Vec<falcon_common::schema::ColumnDef>, Vec<OwnedRow>)> {
        match table_name_lower {
            "information_schema.tables" => {
                let columns = vec![
                    Self::vt_col(0, "table_catalog", DataType::Text),
                    Self::vt_col(1, "table_schema", DataType::Text),
                    Self::vt_col(2, "table_name", DataType::Text),
                    Self::vt_col(3, "table_type", DataType::Text),
                ];
                let mut rows = Vec::new();
                for name in self.catalog.tables_map().keys() {
                    rows.push(OwnedRow::new(vec![
                        Datum::Text("falcon".into()),
                        Datum::Text("public".into()),
                        Datum::Text(name.clone()),
                        Datum::Text("BASE TABLE".into()),
                    ]));
                }
                rows.sort_by(|a, b| {
                    let an = a
                        .values
                        .get(2)
                        .map(|d| format!("{}", d))
                        .unwrap_or_default();
                    let bn = b
                        .values
                        .get(2)
                        .map(|d| format!("{}", d))
                        .unwrap_or_default();
                    an.cmp(&bn)
                });
                Some((columns, rows))
            }
            "information_schema.columns" => {
                let columns = vec![
                    Self::vt_col(0, "table_catalog", DataType::Text),
                    Self::vt_col(1, "table_schema", DataType::Text),
                    Self::vt_col(2, "table_name", DataType::Text),
                    Self::vt_col(3, "column_name", DataType::Text),
                    Self::vt_col(4, "ordinal_position", DataType::Int32),
                    Self::vt_col(5, "data_type", DataType::Text),
                    Self::vt_col(6, "is_nullable", DataType::Text),
                    Self::vt_col(7, "column_default", DataType::Text),
                ];
                let mut rows = Vec::new();
                let mut table_names: Vec<_> = self.catalog.tables_map().keys().cloned().collect();
                table_names.sort();
                for tname in &table_names {
                    if let Some(schema) = self.catalog.find_table(tname) {
                        for (i, col) in schema.columns.iter().enumerate() {
                            rows.push(OwnedRow::new(vec![
                                Datum::Text("falcon".into()),
                                Datum::Text("public".into()),
                                Datum::Text(tname.clone()),
                                Datum::Text(col.name.clone()),
                                Datum::Int32((i + 1) as i32),
                                Datum::Text(format!("{}", col.data_type)),
                                Datum::Text(if col.nullable { "YES" } else { "NO" }.into()),
                                col.default_value
                                    .clone()
                                    .map(|d| Datum::Text(format!("{}", d)))
                                    .unwrap_or(Datum::Null),
                            ]));
                        }
                    }
                }
                Some((columns, rows))
            }
            "pg_catalog.pg_tables" | "pg_tables" => {
                let columns = vec![
                    Self::vt_col(0, "schemaname", DataType::Text),
                    Self::vt_col(1, "tablename", DataType::Text),
                    Self::vt_col(2, "tableowner", DataType::Text),
                    Self::vt_col(3, "hasindexes", DataType::Boolean),
                    Self::vt_col(4, "hasrules", DataType::Boolean),
                    Self::vt_col(5, "hastriggers", DataType::Boolean),
                ];
                let mut rows = Vec::new();
                let mut table_names: Vec<_> = self.catalog.tables_map().keys().cloned().collect();
                table_names.sort();
                for tname in &table_names {
                    rows.push(OwnedRow::new(vec![
                        Datum::Text("public".into()),
                        Datum::Text(tname.clone()),
                        Datum::Text("falcon".into()),
                        Datum::Boolean(
                            !self
                                .catalog
                                .find_table(tname)
                                .is_none_or(|s| s.primary_key_columns.is_empty()),
                        ),
                        Datum::Boolean(false),
                        Datum::Boolean(false),
                    ]));
                }
                Some((columns, rows))
            }
            "pg_catalog.pg_views" | "pg_views" => {
                let columns = vec![
                    Self::vt_col(0, "schemaname", DataType::Text),
                    Self::vt_col(1, "viewname", DataType::Text),
                    Self::vt_col(2, "viewowner", DataType::Text),
                    Self::vt_col(3, "definition", DataType::Text),
                ];
                let rows: Vec<OwnedRow> = self
                    .catalog
                    .views
                    .iter()
                    .map(|v| {
                        OwnedRow::new(vec![
                            Datum::Text("public".into()),
                            Datum::Text(v.name.clone()),
                            Datum::Text("falcon".into()),
                            Datum::Text(v.query_sql.clone()),
                        ])
                    })
                    .collect();
                Some((columns, rows))
            }
            "pg_catalog.pg_indexes" | "pg_indexes" => {
                let columns = vec![
                    Self::vt_col(0, "schemaname", DataType::Text),
                    Self::vt_col(1, "tablename", DataType::Text),
                    Self::vt_col(2, "indexname", DataType::Text),
                    Self::vt_col(3, "indexdef", DataType::Text),
                ];
                Some((columns, vec![]))
            }
            "information_schema.schemata" => {
                let columns = vec![
                    Self::vt_col(0, "catalog_name", DataType::Text),
                    Self::vt_col(1, "schema_name", DataType::Text),
                    Self::vt_col(2, "schema_owner", DataType::Text),
                ];
                let rows = vec![
                    OwnedRow::new(vec![
                        Datum::Text("falcon".into()),
                        Datum::Text("public".into()),
                        Datum::Text("falcon".into()),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("falcon".into()),
                        Datum::Text("information_schema".into()),
                        Datum::Text("falcon".into()),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Text("falcon".into()),
                        Datum::Text("pg_catalog".into()),
                        Datum::Text("falcon".into()),
                    ]),
                ];
                Some((columns, rows))
            }
            "pg_catalog.pg_type" | "pg_type" => {
                let columns = vec![
                    Self::vt_col(0, "oid", DataType::Int32),
                    Self::vt_col(1, "typname", DataType::Text),
                    Self::vt_col(2, "typlen", DataType::Int32),
                    Self::vt_col(3, "typtype", DataType::Text),
                ];
                let type_entries = vec![
                    (16, "bool", 1, "b"),
                    (20, "int8", 8, "b"),
                    (21, "int2", 2, "b"),
                    (23, "int4", 4, "b"),
                    (25, "text", -1, "b"),
                    (700, "float4", 4, "b"),
                    (701, "float8", 8, "b"),
                    (1043, "varchar", -1, "b"),
                    (1082, "date", 4, "b"),
                    (1114, "timestamp", 8, "b"),
                    (3802, "jsonb", -1, "b"),
                    (2277, "anyarray", -1, "p"),
                ];
                let rows = type_entries
                    .iter()
                    .map(|(oid, name, len, tt)| {
                        OwnedRow::new(vec![
                            Datum::Int32(*oid),
                            Datum::Text(name.to_string()),
                            Datum::Int32(*len),
                            Datum::Text(tt.to_string()),
                        ])
                    })
                    .collect();
                Some((columns, rows))
            }
            "pg_catalog.pg_namespace" | "pg_namespace" => {
                let columns = vec![
                    Self::vt_col(0, "oid", DataType::Int32),
                    Self::vt_col(1, "nspname", DataType::Text),
                    Self::vt_col(2, "nspowner", DataType::Int32),
                ];
                let rows = vec![
                    OwnedRow::new(vec![
                        Datum::Int32(11),
                        Datum::Text("pg_catalog".into()),
                        Datum::Int32(10),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Int32(2200),
                        Datum::Text("public".into()),
                        Datum::Int32(10),
                    ]),
                    OwnedRow::new(vec![
                        Datum::Int32(13183),
                        Datum::Text("information_schema".into()),
                        Datum::Int32(10),
                    ]),
                ];
                Some((columns, rows))
            }
            "pg_catalog.pg_database" | "pg_database" => {
                let columns = vec![
                    Self::vt_col(0, "oid", DataType::Int32),
                    Self::vt_col(1, "datname", DataType::Text),
                    Self::vt_col(2, "datdba", DataType::Int32),
                    Self::vt_col(3, "encoding", DataType::Int32),
                ];
                let rows = vec![OwnedRow::new(vec![
                    Datum::Int32(1),
                    Datum::Text("falcon".into()),
                    Datum::Int32(10),
                    Datum::Int32(6),
                ])];
                Some((columns, rows))
            }
            _ => None,
        }
    }

    /// Helper to build a virtual table column definition.
    fn vt_col(idx: u32, name: &str, data_type: DataType) -> falcon_common::schema::ColumnDef {
        falcon_common::schema::ColumnDef {
            id: ColumnId(idx),
            name: name.to_string(),
            data_type,
            nullable: true,
            is_primary_key: false,
            default_value: None,
            is_serial: false,
        }
    }
}
