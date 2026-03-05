use falcon_common::datum::Datum;
use falcon_common::error::FalconError;
use falcon_planner::{
    ColumnStatsInfo, IndexedColumns, PhysicalPlan, PlannedTxnType, Planner, TableRowCounts,
    TableStatsInfo, TableStatsMap,
};
use falcon_sql_frontend::binder::Binder;
use falcon_sql_frontend::parser::parse_sql;
use falcon_sql_frontend::types::{AggFunc, BinOp, BoundExpr, BoundProjection, ScalarFunc};
use falcon_txn::SlowPathMode;

use crate::codec::{BackendMessage, FieldDescription};
use crate::handler::QueryHandler;
use crate::session::PgSession;
use falcon_executor::ExecutionResult;

impl QueryHandler {
    /// Build an IndexedColumns map from storage for index scan detection in the planner.
    pub(crate) fn build_indexed_columns(&self) -> IndexedColumns {
        let mut indexed = IndexedColumns::new();
        let catalog = self.storage.get_catalog();
        for table in catalog.tables_map().values() {
            let cols = self.storage.get_indexed_columns(table.id);
            if !cols.is_empty() {
                indexed.insert(table.id, cols.iter().map(|(c, _)| *c).collect());
            }
        }
        indexed
    }

    /// Build a TableRowCounts map from cached ANALYZE stats for cost-based planning.
    pub(crate) fn build_row_counts(&self) -> TableRowCounts {
        let all_stats = self.storage.get_all_table_stats();
        let mut counts = TableRowCounts::new();
        for ts in &all_stats {
            counts.insert(ts.table_id, ts.row_count);
        }
        counts
    }

    /// Build a full TableStatsMap from cached ANALYZE stats for cost-based optimizer.
    pub(crate) fn build_table_stats(&self) -> TableStatsMap {
        let all_stats = self.storage.get_all_table_stats();
        let mut map = TableStatsMap::new();
        for ts in all_stats {
            let columns = ts.column_stats.iter().map(|cs| {
                let null_fraction = if ts.row_count > 0 {
                    cs.null_count as f64 / ts.row_count as f64
                } else {
                    0.0
                };
                let mcv = cs.mcv.as_ref().map_or_else(Vec::new, |m| m.entries.clone());
                let (histogram_bounds, histogram_rows) = cs.histogram.as_ref().map_or_else(
                    || (vec![], 0),
                    |h| (h.bounds.clone(), h.total_rows),
                );
                ColumnStatsInfo {
                    column_idx: cs.column_idx,
                    distinct_count: cs.distinct_count,
                    null_fraction,
                    min_value: cs.min_value.clone(),
                    max_value: cs.max_value.clone(),
                    mcv,
                    histogram_bounds,
                    histogram_rows,
                }
            }).collect();
            map.insert(ts.table_id, TableStatsInfo {
                table_id: ts.table_id,
                row_count: ts.row_count,
                columns,
            });
        }
        map
    }

    /// Describe a SQL query: parse/bind/plan and return the output column descriptions.
    /// Used by the extended query protocol's Describe message.
    pub fn describe_query(&self, sql: &str) -> Result<Vec<FieldDescription>, FalconError> {
        falcon_common::crash_domain::catch_request_result("describe_query", sql, || {
            self.describe_query_inner(sql)
        })
    }

    fn describe_query_inner(&self, sql: &str) -> Result<Vec<FieldDescription>, FalconError> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Ok(vec![]);
        }

        let stmts = parse_sql(sql).map_err(FalconError::Sql)?;
        if stmts.is_empty() {
            return Ok(vec![]);
        }

        let catalog = self.storage.get_catalog();
        let mut binder = Binder::new(catalog);
        let bound = binder.bind(&stmts[0]).map_err(FalconError::Sql)?;

        let row_counts = self.build_row_counts();
        let indexed_cols = self.build_indexed_columns();
        let plan = Planner::plan_with_indexes(&bound, &row_counts, &indexed_cols)
            .map_err(FalconError::Sql)?;

        // Extract column info from the plan
        Ok(self.plan_output_fields(&plan))
    }

    /// Parse + bind + plan a SQL statement for the extended query protocol.
    #[allow(clippy::type_complexity)]
    pub fn prepare_statement(
        &self,
        sql: &str,
    ) -> Result<
        (
            PhysicalPlan,
            Vec<Option<falcon_common::types::DataType>>,
            Vec<crate::session::FieldDescriptionCompact>,
        ),
        FalconError,
    > {
        falcon_common::crash_domain::catch_request_result("prepare_statement", sql, || {
            self.prepare_statement_inner(sql)
        })
    }

    #[allow(clippy::type_complexity)]
    fn prepare_statement_inner(
        &self,
        sql: &str,
    ) -> Result<
        (
            PhysicalPlan,
            Vec<Option<falcon_common::types::DataType>>,
            Vec<crate::session::FieldDescriptionCompact>,
        ),
        FalconError,
    > {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(FalconError::Sql(falcon_common::error::SqlError::Parse(
                "empty query".into(),
            )));
        }

        let stmts = parse_sql(sql).map_err(FalconError::Sql)?;
        if stmts.is_empty() {
            return Err(FalconError::Sql(falcon_common::error::SqlError::Parse(
                "empty query".into(),
            )));
        }

        let catalog = self.storage.get_catalog();
        let mut binder = Binder::new(catalog);
        let (bound, inferred_types) = binder
            .bind_with_params_lenient(&stmts[0], None)
            .map_err(FalconError::Sql)?;

        let row_counts = self.build_row_counts();
        let indexed_cols = self.build_indexed_columns();
        let plan = Planner::plan_with_indexes(&bound, &row_counts, &indexed_cols)
            .map_err(FalconError::Sql)?;

        // Wrap in DistPlan if multi-shard cluster
        let plan = Planner::wrap_distributed(plan, &self.cluster.shard_ids);

        // Build compact row description from the plan output fields
        let fields = self.plan_output_fields(&plan);
        let row_desc: Vec<crate::session::FieldDescriptionCompact> = fields
            .iter()
            .map(|f| crate::session::FieldDescriptionCompact {
                name: f.name.clone(),
                type_oid: f.type_oid,
                type_len: f.type_len,
            })
            .collect();

        Ok((plan, inferred_types, row_desc))
    }

    /// Execute a pre-planned query with parameter values.
    /// Used by the extended query protocol's Execute message.
    pub fn execute_plan(
        &self,
        plan: &PhysicalPlan,
        params: &[Datum],
        session: &mut PgSession,
    ) -> Vec<BackendMessage> {
        let rctx = falcon_common::request_context::RequestContext::new(session.id as u64);
        let ctx = format!("session_id={}", session.id);
        let result = falcon_common::crash_domain::catch_request("execute_plan", &ctx, || {
            self.execute_plan_inner(plan, params, session)
        });
        match result {
            Ok(msgs) => msgs,
            Err(e) => vec![self.error_response(&e.with_request_context(&rctx))],
        }
    }

    fn execute_plan_inner(
        &self,
        plan: &PhysicalPlan,
        params: &[Datum],
        session: &mut PgSession,
    ) -> Vec<BackendMessage> {
        let mut messages = Vec::with_capacity(2);

        match plan {
            PhysicalPlan::Begin | PhysicalPlan::Commit | PhysicalPlan::Rollback => {
                let sql = match plan {
                    PhysicalPlan::Begin => "BEGIN",
                    PhysicalPlan::Commit => "COMMIT",
                    PhysicalPlan::Rollback => "ROLLBACK",
                    _ => unreachable!(),
                };
                return self.handle_query(sql, session);
            }
            _ => {}
        }

        let routing_hint = plan.routing_hint();

        if let Some(ref txn) = session.txn {
            let _ = self
                .txn_mgr
                .observe_involved_shards(txn.txn_id, &routing_hint.involved_shards);
            if matches!(routing_hint.planned_txn_type(), PlannedTxnType::Global) {
                let _ = self.txn_mgr.force_global(txn.txn_id, SlowPathMode::Xa2Pc);
            }
        }

        // For DDL/metadata, execute without params
        if matches!(
            plan,
            PhysicalPlan::CreateTable { .. }
                | PhysicalPlan::DropTable { .. }
                | PhysicalPlan::ShowTxnStats
                | PhysicalPlan::RunGc
        ) {
            match self.executor.execute(plan, None) {
                Ok(ExecutionResult::Ddl { message }) => {
                    self.observability.plan_cache.invalidate();
                    messages.push(BackendMessage::CommandComplete { tag: message });
                }
                Ok(ExecutionResult::Query { columns, rows }) => {
                    let fields: Vec<FieldDescription> = columns
                        .iter()
                        .map(|(name, dt)| FieldDescription {
                            name: name.clone(),
                            table_oid: 0,
                            column_attr: 0,
                            type_oid: dt.pg_oid(),
                            type_len: dt.type_len(),
                            type_modifier: -1,
                            format_code: 0,
                        })
                        .collect();
                    messages.push(BackendMessage::RowDescription { fields });
                    for row in &rows {
                        let values: Vec<Option<String>> =
                            row.values.iter().map(|d| Some(d.to_string())).collect();
                        messages.push(BackendMessage::DataRow { values });
                    }
                    messages.push(BackendMessage::CommandComplete {
                        tag: format!("SHOW {}", rows.len()),
                    });
                }
                Err(e) => {
                    messages.push(self.error_response(&e));
                }
                _ => {}
            }
            return messages;
        }

        let plan_is_dml = matches!(plan,
            PhysicalPlan::Insert { .. } | PhysicalPlan::Update { .. } | PhysicalPlan::Delete { .. });

        // For DML/query, ensure a transaction exists (autocommit = implicit txn)
        let auto_txn = if session.txn.is_none() {
            let txn = self.txn_mgr.begin_autocommit(session.default_isolation);
            session.txn = Some(txn);
            true
        } else {
            false
        };

        let result = if let Some(dist) = &self.cluster.dist_engine {
            dist.execute_with_params(plan, session.txn.as_ref(), params)
        } else {
            self.executor
                .execute_with_params(plan, session.txn.as_ref(), params)
        };

        match result {
            Ok(exec_result) => {
                match exec_result {
                    ExecutionResult::Query { columns, rows } => {
                        let fields: Vec<FieldDescription> = columns
                            .iter()
                            .map(|(name, dt)| FieldDescription {
                                name: name.clone(),
                                table_oid: 0,
                                column_attr: 0,
                                type_oid: dt.pg_oid(),
                                type_len: dt.type_len(),
                                type_modifier: -1,
                                format_code: 0,
                            })
                            .collect();
                        messages.push(BackendMessage::RowDescription { fields });
                        let row_count = rows.len();
                        for row in rows {
                            let values: Vec<Option<String>> =
                                row.values.iter().map(falcon_common::datum::Datum::to_pg_text).collect();
                            messages.push(BackendMessage::DataRow { values });
                        }
                        messages.push(BackendMessage::CommandComplete {
                            tag: format!("SELECT {row_count}"),
                        });
                    }
                    ExecutionResult::Dml { rows_affected, tag } => {
                        let cmd_tag = if rows_affected == 1 {
                            match tag {
                                "INSERT" => "INSERT 0 1".to_owned(),
                                "UPDATE" => "UPDATE 1".to_owned(),
                                "DELETE" => "DELETE 1".to_owned(),
                                _ => format!("{tag} 1"),
                            }
                        } else {
                            match tag {
                                "INSERT" => format!("INSERT 0 {rows_affected}"),
                                "UPDATE" => format!("UPDATE {rows_affected}"),
                                "DELETE" => format!("DELETE {rows_affected}"),
                                _ => format!("{tag} {rows_affected}"),
                            }
                        };
                        messages.push(BackendMessage::CommandComplete { tag: cmd_tag });
                    }
                    ExecutionResult::Ddl { message } => {
                        self.observability.plan_cache.invalidate();
                        messages.push(BackendMessage::CommandComplete { tag: message });
                    }
                    ExecutionResult::TxnControl { action } => {
                        messages.push(BackendMessage::CommandComplete { tag: action });
                    }
                }

                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        if plan_is_dml {
                            let _ = self.txn_mgr.commit_autocommit(txn.txn_id);
                        } else {
                            self.txn_mgr.commit_autocommit_readonly();
                        }
                    }
                    session.txn = None;
                }
            }
            Err(e) => {
                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        self.txn_mgr.abort_autocommit(txn.txn_id);
                    }
                    session.txn = None;
                }
                messages.push(self.error_response(&e));
            }
        }

        messages
    }

    /// Map a Falcon DataType to a PostgreSQL type OID.
    pub const fn datatype_to_oid(&self, dt: Option<&falcon_common::types::DataType>) -> i32 {
        use falcon_common::types::DataType;
        match dt {
            Some(DataType::Int16) => 21,           // INT2
            Some(DataType::Int32) => 23,           // INT4
            Some(DataType::Int64) => 20,           // INT8
            Some(DataType::Float32) => 700,        // FLOAT4
            Some(DataType::Float64) => 701,        // FLOAT8
            Some(DataType::Boolean) => 16,         // BOOL
            Some(DataType::Text) => 25,            // TEXT
            Some(DataType::Timestamp) => 1114,     // TIMESTAMP
            Some(DataType::Date) => 1082,          // DATE
            Some(DataType::Array(_)) => 2277,      // ANYARRAY
            Some(DataType::Jsonb) => 3802,         // JSONB
            Some(DataType::Decimal(_, _)) => 1700, // NUMERIC
            Some(DataType::Time) => 1083,          // TIME
            Some(DataType::Interval) => 1186,      // INTERVAL
            Some(DataType::Uuid) => 2950,          // UUID
            Some(DataType::Bytea) => 17,           // BYTEA
            None => 0,                             // unspecified
        }
    }

    /// Extract output column FieldDescriptions from a physical plan.
    pub(crate) fn plan_output_fields(&self, plan: &PhysicalPlan) -> Vec<FieldDescription> {
        use falcon_common::types::DataType;

        fn infer_expr_type(
            expr: &BoundExpr,
            cols: &[falcon_common::schema::ColumnDef],
        ) -> DataType {
            match expr {
                BoundExpr::Literal(d) => d.data_type().unwrap_or(DataType::Text),
                BoundExpr::ColumnRef(idx) => cols
                    .get(*idx)
                    .map_or(DataType::Text, |c| c.data_type.clone()),
                BoundExpr::BinaryOp { left, op, right } => {
                    match op {
                        BinOp::Eq
                        | BinOp::NotEq
                        | BinOp::Lt
                        | BinOp::LtEq
                        | BinOp::Gt
                        | BinOp::GtEq
                        | BinOp::And
                        | BinOp::Or => DataType::Boolean,
                        BinOp::Plus
                        | BinOp::Minus
                        | BinOp::Multiply
                        | BinOp::Divide
                        | BinOp::Modulo => {
                            let lt = infer_expr_type(left, cols);
                            let rt = infer_expr_type(right, cols);
                            promote_numeric(lt, rt)
                        }
                        BinOp::StringConcat
                        | BinOp::JsonArrowText
                        | BinOp::JsonHashArrowText => DataType::Text,
                        BinOp::JsonArrow
                        | BinOp::JsonHashArrow
                        | BinOp::JsonContains
                        | BinOp::JsonContainedBy
                        | BinOp::JsonExists => DataType::Jsonb,
                    }
                }
                BoundExpr::Not(_)
                | BoundExpr::IsNull(_)
                | BoundExpr::IsNotNull(_)
                | BoundExpr::IsNotDistinctFrom { .. }
                | BoundExpr::Like { .. }
                | BoundExpr::Between { .. }
                | BoundExpr::InList { .. }
                | BoundExpr::Exists { .. }
                | BoundExpr::InSubquery { .. } => DataType::Boolean,
                BoundExpr::Cast { target_type, .. } => parse_cast_type(target_type),
                BoundExpr::Case {
                    results,
                    else_result,
                    ..
                } => {
                    if let Some(first) = results.first() {
                        infer_expr_type(first, cols)
                    } else if let Some(e) = else_result {
                        infer_expr_type(e, cols)
                    } else {
                        DataType::Text
                    }
                }
                BoundExpr::Coalesce(exprs) => exprs
                    .first()
                    .map_or(DataType::Text, |e| infer_expr_type(e, cols)),
                BoundExpr::Function { func, args } => infer_func_type(func, args, cols),
                BoundExpr::AggregateExpr { func, arg, .. } => {
                    let input_ty = arg.as_ref().map(|a| infer_expr_type(a, cols));
                    infer_agg_return_type(func, input_ty)
                }
                BoundExpr::ArrayLiteral(_) => DataType::Array(Box::new(DataType::Text)),
                BoundExpr::ArrayIndex { array, .. } => {
                    match infer_expr_type(array, cols) {
                        DataType::Array(inner) => *inner,
                        _ => DataType::Text,
                    }
                }
                BoundExpr::OuterColumnRef(idx) => cols
                    .get(*idx)
                    .map_or(DataType::Text, |c| c.data_type.clone()),
                BoundExpr::SequenceNextval(_)
                | BoundExpr::SequenceCurrval(_)
                | BoundExpr::SequenceSetval(_, _) => DataType::Int64,
                BoundExpr::Grouping(_) => DataType::Int32,
                _ => DataType::Text,
            }
        }

        fn promote_numeric(a: DataType, b: DataType) -> DataType {
            match (&a, &b) {
                (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
                (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
                (DataType::Int32, DataType::Int32) => DataType::Int32,
                _ => a,
            }
        }

        fn parse_cast_type(t: &str) -> DataType {
            match t.to_uppercase().as_str() {
                "INT" | "INT4" | "INTEGER" => DataType::Int32,
                "BIGINT" | "INT8" => DataType::Int64,
                "FLOAT" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => DataType::Float64,
                "BOOL" | "BOOLEAN" => DataType::Boolean,
                "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => DataType::Timestamp,
                "DATE" => DataType::Date,
                "JSONB" => DataType::Jsonb,
                _ => DataType::Text,
            }
        }

        fn infer_func_type(
            func: &ScalarFunc,
            args: &[BoundExpr],
            cols: &[falcon_common::schema::ColumnDef],
        ) -> DataType {
            match func {
                ScalarFunc::Upper
                | ScalarFunc::Lower
                | ScalarFunc::Trim
                | ScalarFunc::Replace
                | ScalarFunc::Lpad
                | ScalarFunc::Rpad
                | ScalarFunc::Left
                | ScalarFunc::Right
                | ScalarFunc::Repeat
                | ScalarFunc::Reverse
                | ScalarFunc::Initcap
                | ScalarFunc::Chr
                | ScalarFunc::ToChar
                | ScalarFunc::Concat
                | ScalarFunc::ConcatWs
                | ScalarFunc::Substring
                | ScalarFunc::Btrim
                | ScalarFunc::Ltrim
                | ScalarFunc::Rtrim
                | ScalarFunc::Overlay
                | ScalarFunc::RegexpReplace
                | ScalarFunc::RegexpSubstr
                | ScalarFunc::Translate
                | ScalarFunc::QuoteLiteral
                | ScalarFunc::QuoteIdent
                | ScalarFunc::QuoteNullable
                | ScalarFunc::Md5
                | ScalarFunc::Encode
                | ScalarFunc::Decode
                | ScalarFunc::ToHex
                | ScalarFunc::PgTypeof
                | ScalarFunc::GenRandomUuid
                | ScalarFunc::ArrayDims
                | ScalarFunc::ArrayToString => DataType::Text,
                ScalarFunc::Length
                | ScalarFunc::Position
                | ScalarFunc::Ascii
                | ScalarFunc::RegexpCount
                | ScalarFunc::ArrayLength
                | ScalarFunc::ArrayPosition
                | ScalarFunc::Cardinality
                | ScalarFunc::ArrayUpper
                | ScalarFunc::ArrayLower
                | ScalarFunc::WidthBucket
                | ScalarFunc::Factorial
                | ScalarFunc::Gcd
                | ScalarFunc::Lcm => DataType::Int64,
                ScalarFunc::Abs
                | ScalarFunc::Round
                | ScalarFunc::Ceil
                | ScalarFunc::Floor
                | ScalarFunc::Power
                | ScalarFunc::Sqrt
                | ScalarFunc::Sign
                | ScalarFunc::Trunc
                | ScalarFunc::Ln
                | ScalarFunc::Log
                | ScalarFunc::Exp
                | ScalarFunc::Pi
                | ScalarFunc::Mod
                | ScalarFunc::Degrees
                | ScalarFunc::Radians
                | ScalarFunc::Sin
                | ScalarFunc::Cos
                | ScalarFunc::Tan
                | ScalarFunc::Asin
                | ScalarFunc::Acos
                | ScalarFunc::Atan
                | ScalarFunc::Atan2
                | ScalarFunc::Cbrt
                | ScalarFunc::Random => DataType::Float64,
                ScalarFunc::Now => DataType::Timestamp,
                ScalarFunc::CurrentDate | ScalarFunc::ToDate => DataType::Date,
                ScalarFunc::ToTimestamp => DataType::Timestamp,
                ScalarFunc::Extract | ScalarFunc::EpochFromTimestamp => {
                    DataType::Float64
                }
                ScalarFunc::DateTrunc | ScalarFunc::Age => DataType::Timestamp,
                ScalarFunc::ToNumber => DataType::Float64,
                ScalarFunc::Greatest | ScalarFunc::Least => args
                    .first()
                    .map_or(DataType::Text, |a| infer_expr_type(a, cols)),
                ScalarFunc::RegexpMatch | ScalarFunc::StringToArray => {
                    DataType::Array(Box::new(DataType::Text))
                }
                ScalarFunc::ArrayAppend
                | ScalarFunc::ArrayPrepend
                | ScalarFunc::ArrayRemove
                | ScalarFunc::ArrayCat => args
                    .first().map_or_else(|| DataType::Array(Box::new(DataType::Text)), |a| infer_expr_type(a, cols)),
                _ => DataType::Text,
            }
        }

        fn infer_agg_return_type(func: &AggFunc, input_ty: Option<DataType>) -> DataType {
            match func {
                AggFunc::Count => DataType::Int64,
                AggFunc::Sum => match input_ty {
                    Some(DataType::Float64) => DataType::Float64,
                    _ => DataType::Int64,
                },
                AggFunc::Min | AggFunc::Max => input_ty.unwrap_or(DataType::Text),
                AggFunc::StringAgg(_) => DataType::Text,
                AggFunc::BoolAnd | AggFunc::BoolOr => DataType::Boolean,
                AggFunc::ArrayAgg => DataType::Array(Box::new(input_ty.unwrap_or(DataType::Text))),
                AggFunc::Avg
                | AggFunc::StddevPop
                | AggFunc::StddevSamp
                | AggFunc::VarPop
                | AggFunc::VarSamp
                | AggFunc::Corr
                | AggFunc::CovarPop
                | AggFunc::CovarSamp
                | AggFunc::RegrSlope
                | AggFunc::RegrIntercept
                | AggFunc::RegrR2
                | AggFunc::RegrAvgX
                | AggFunc::RegrAvgY
                | AggFunc::RegrSXX
                | AggFunc::RegrSYY
                | AggFunc::RegrSXY
                | AggFunc::PercentileCont(_)
                | AggFunc::PercentileDisc(_) => DataType::Float64,
                AggFunc::RegrCount => DataType::Int64,
                AggFunc::Mode => input_ty.unwrap_or(DataType::Text),
                AggFunc::BitAndAgg | AggFunc::BitOrAgg | AggFunc::BitXorAgg => DataType::Int64,
            }
        }

        fn projection_to_field(
            p: &BoundProjection,
            schema: &falcon_common::schema::TableSchema,
        ) -> FieldDescription {
            match p {
                BoundProjection::Column(idx, alias) => {
                    if let Some(col) = schema.columns.get(*idx) {
                        FieldDescription {
                            name: alias.clone(),
                            table_oid: 0,
                            column_attr: 0,
                            type_oid: col.data_type.pg_oid(),
                            type_len: col.data_type.type_len(),
                            type_modifier: -1,
                            format_code: 0,
                        }
                    } else {
                        FieldDescription {
                            name: alias.clone(),
                            table_oid: 0,
                            column_attr: 0,
                            type_oid: 25, // TEXT fallback
                            type_len: -1,
                            type_modifier: -1,
                            format_code: 0,
                        }
                    }
                }
                BoundProjection::Aggregate(func, arg, alias, _, _) => {
                    let input_ty = arg.as_ref().map(|a| infer_expr_type(a, &schema.columns));
                    let dt = infer_agg_return_type(func, input_ty);
                    FieldDescription {
                        name: alias.clone(),
                        table_oid: 0,
                        column_attr: 0,
                        type_oid: dt.pg_oid(),
                        type_len: dt.type_len(),
                        type_modifier: -1,
                        format_code: 0,
                    }
                }
                BoundProjection::Expr(expr, alias) => {
                    let dt = infer_expr_type(expr, &schema.columns);
                    FieldDescription {
                        name: alias.clone(),
                        table_oid: 0,
                        column_attr: 0,
                        type_oid: dt.pg_oid(),
                        type_len: dt.type_len(),
                        type_modifier: -1,
                        format_code: 0,
                    }
                }
                BoundProjection::Window(w) => {
                    FieldDescription {
                        name: w.alias.clone(),
                        table_oid: 0,
                        column_attr: 0,
                        type_oid: 20, // BIGINT (window funcs typically return int)
                        type_len: 8,
                        type_modifier: -1,
                        format_code: 0,
                    }
                }
            }
        }

        match plan {
            PhysicalPlan::SeqScan {
                projections,
                schema,
                ..
            }
            | PhysicalPlan::IndexScan {
                projections,
                schema,
                ..
            } => projections
                .iter()
                .map(|p| projection_to_field(p, schema))
                .collect(),
            PhysicalPlan::NestedLoopJoin {
                projections,
                combined_schema,
                ..
            }
            | PhysicalPlan::HashJoin {
                projections,
                combined_schema,
                ..
            } => projections
                .iter()
                .map(|p| projection_to_field(p, combined_schema))
                .collect(),
            PhysicalPlan::Explain(_) | PhysicalPlan::ExplainAnalyze(_) => {
                vec![FieldDescription {
                    name: "QUERY PLAN".into(),
                    table_oid: 0,
                    column_attr: 0,
                    type_oid: 25, // TEXT
                    type_len: -1,
                    type_modifier: -1,
                    format_code: 0,
                }]
            }
            PhysicalPlan::DistPlan { subplan, .. } => self.plan_output_fields(subplan),
            _ => vec![],
        }
    }
}
