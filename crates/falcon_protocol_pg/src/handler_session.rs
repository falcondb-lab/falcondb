use falcon_planner::Planner;
use falcon_sql_frontend::binder::Binder;
use falcon_sql_frontend::parser::parse_sql;

use crate::codec::{BackendMessage, FieldDescription};
use crate::session::PgSession;

use super::handler::{QueryHandler, parse_set_log_min_duration, parse_set_command,
    parse_prepare_statement, parse_execute_statement, bind_params};

impl QueryHandler {
    /// Intercept system queries that psql and PG drivers send.
    /// Returns Some(messages) if handled, None to fall through to normal execution.
    pub(crate) fn handle_system_query(&self, sql: &str, session: &mut PgSession) -> Option<Vec<BackendMessage>> {
        let sql_lower = sql.to_lowercase();
        let sql_lower = sql_lower.trim().trim_end_matches(';').trim();

        // SELECT version()
        if sql_lower == "select version()" || sql_lower.starts_with("select version()") {
            return Some(self.single_row_result(
                vec![("version", 25, -1)],
                vec![vec![Some("PostgreSQL 15.0.0 on FalconDB (in-memory OLTP)".into())]],
            ));
        }

        // SELECT current_schema() / current_database() / current_user
        if sql_lower == "select current_schema()" || sql_lower.starts_with("select current_schema") {
            return Some(self.single_row_result(
                vec![("current_schema", 25, -1)],
                vec![vec![Some("public".into())]],
            ));
        }
        if sql_lower == "select current_database()" || sql_lower.starts_with("select current_database") {
            return Some(self.single_row_result(
                vec![("current_database", 25, -1)],
                vec![vec![Some("falcon".into())]],
            ));
        }
        if sql_lower == "select current_user" {
            return Some(self.single_row_result(
                vec![("current_user", 25, -1)],
                vec![vec![Some("falcon".into())]],
            ));
        }

        // SET log_min_duration_statement = <ms>
        if let Some(threshold_ms) = parse_set_log_min_duration(sql_lower) {
            self.slow_query_log.set_threshold(
                std::time::Duration::from_millis(threshold_ms)
            );
            return Some(vec![BackendMessage::CommandComplete {
                tag: "SET".into(),
            }]);
        }

        // Cluster admin commands: SELECT falcon_add_node(id), falcon_remove_node(id),
        // falcon_promote_leader(shard_id), falcon_rebalance_apply()
        if sql_lower.starts_with("select falcon_") {
            if let Some(result) = self.parse_and_dispatch_admin_command(sql_lower) {
                return Some(result);
            }
        }

        // CHECKPOINT — trigger a storage checkpoint (WAL compaction)
        if sql_lower == "checkpoint" {
            match self.storage.checkpoint() {
                Ok((segment_id, row_count)) => {
                    return Some(self.single_row_result(
                        vec![("checkpoint", 25, -1)],
                        vec![vec![Some(format!("OK segment={} rows={}", segment_id, row_count))]],
                    ));
                }
                Err(e) => {
                    return Some(vec![BackendMessage::ErrorResponse {
                        severity: "ERROR".into(),
                        code: "XX000".into(),
                        message: format!("CHECKPOINT failed: {}", e),
                    }]);
                }
            }
        }

        // RESET falcon.slow_queries — clear the slow query log
        if sql_lower == "reset falcon.slow_queries" {
            self.slow_query_log.clear();
            return Some(vec![BackendMessage::CommandComplete {
                tag: "RESET".into(),
            }]);
        }

        // SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY/WRITE (pgjdbc setReadOnly)
        if sql_lower.contains("session characteristics as transaction") ||
           (sql_lower.starts_with("set transaction") && !sql_lower.contains("isolation")) {
            // Accept silently — store as GUC for SHOW compatibility
            if sql_lower.contains("read only") {
                session.set_guc("default_transaction_read_only", "on");
            } else {
                session.set_guc("default_transaction_read_only", "off");
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "SET".into(),
            }]);
        }

        // SET <var> = <value> / SET <var> TO <value> — store in session GUC
        if sql_lower.starts_with("set ") {
            if let Some((name, value)) = parse_set_command(sql_lower) {
                session.set_guc(&name, &value);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "SET".into(),
            }]);
        }

        // SAVEPOINT <name>
        if sql_lower.starts_with("savepoint ") {
            return Some(self.handle_savepoint(sql_lower, session));
        }

        // ROLLBACK TO [SAVEPOINT] <name>
        if sql_lower.starts_with("rollback to ") {
            return Some(self.handle_rollback_to(sql_lower, session));
        }

        // RELEASE [SAVEPOINT] <name>
        if sql_lower.starts_with("release ") {
            return Some(self.handle_release_savepoint(sql_lower, session));
        }

        // CREATE TENANT name [MAX_QPS n] [MAX_CONNECTIONS n] [MAX_STORAGE_BYTES n]
        if sql_lower.starts_with("create tenant ") {
            return Some(self.handle_create_tenant(sql_lower));
        }

        // DROP TENANT name
        if sql_lower.starts_with("drop tenant ") {
            return Some(self.handle_drop_tenant(sql_lower));
        }

        // SHOW ... queries
        if sql_lower.starts_with("show ") {
            let param = sql_lower.trim_start_matches("show ").trim();

            // Delegate falcon.* SHOW commands to handler_show module
            if param.starts_with("falcon.") {
                if let Some(result) = self.handle_show_falcon(param, session) {
                    return Some(result);
                }
            }

            // Look up GUC variable from session, with hardcoded fallbacks
            let value = if let Some(v) = session.get_guc(param) {
                v.to_string()
            } else {
                match param {
                    "transaction_isolation" => "read committed".into(),
                    "max_identifier_length" => "63".into(),
                    _ => "".into(),
                }
            };
            return Some(self.single_row_result(
                vec![(param, 25, -1)],
                vec![vec![Some(value)]],
            ));
        }

        // Delegate information_schema and pg_catalog queries to handler_catalog module
        if let Some(result) = self.handle_catalog_query(sql_lower, session) {
            return Some(result);
        }

        // PREPARE name [(type, ...)] AS query
        if sql_lower.starts_with("prepare ") {
            if let Some((name, query)) = parse_prepare_statement(sql) {
                session.prepared_statements.insert(
                    name,
                    crate::session::PreparedStatement {
                        query,
                        param_types: vec![],
                        plan: None,
                        inferred_param_types: vec![],
                        row_desc: vec![],
                    },
                );
                falcon_observability::record_prepared_stmt_sql_cmd("prepare");
                falcon_observability::record_prepared_stmt_active(session.prepared_statements.len());
                return Some(vec![BackendMessage::CommandComplete {
                    tag: "PREPARE".into(),
                }]);
            }
            return Some(vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42601".into(),
                message: "invalid PREPARE syntax".into(),
            }]);
        }

        // EXECUTE name [(param, ...)]
        if sql_lower.starts_with("execute ") {
            if let Some((name, params)) = parse_execute_statement(sql) {
                let bound = if let Some(ps) = session.prepared_statements.get(&name) {
                    bind_params(&ps.query, &params)
                } else {
                    return Some(vec![BackendMessage::ErrorResponse {
                        severity: "ERROR".into(),
                        code: "26000".into(),
                        message: format!("prepared statement \"{}\" does not exist", name),
                    }]);
                };
                // Execute the bound SQL through the normal query path
                falcon_observability::record_prepared_stmt_sql_cmd("execute");
                return Some(self.handle_query(&bound, session));
            }
        }

        // DISCARD ALL (psql sends this on reconnect sometimes)
        if sql_lower == "discard all" {
            session.reset_all_gucs();
            session.prepared_statements.clear();
            session.portals.clear();
            return Some(vec![BackendMessage::CommandComplete {
                tag: "DISCARD ALL".into(),
            }]);
        }

        // DEALLOCATE [ALL | name]
        if sql_lower.starts_with("deallocate") {
            let rest = sql_lower.trim_start_matches("deallocate").trim().trim_end_matches(';').trim();
            if rest == "all" {
                session.prepared_statements.clear();
            } else if !rest.is_empty() {
                session.prepared_statements.remove(rest);
            }
            falcon_observability::record_prepared_stmt_sql_cmd("deallocate");
            falcon_observability::record_prepared_stmt_active(session.prepared_statements.len());
            return Some(vec![BackendMessage::CommandComplete {
                tag: "DEALLOCATE".into(),
            }]);
        }

        // RESET [ALL | var]
        if sql_lower.starts_with("reset ") {
            let var = sql_lower.trim_start_matches("reset ").trim().trim_end_matches(';').trim();
            if var == "all" {
                session.reset_all_gucs();
            } else {
                session.reset_guc(var);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "RESET".into(),
            }]);
        }

        // ── LISTEN channel ──
        if sql_lower.starts_with("listen ") {
            let channel = sql_lower.trim_start_matches("listen ").trim().trim_end_matches(';').trim().to_string();
            if !channel.is_empty() {
                session.notifications.listen(&channel, &session.notification_hub);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "LISTEN".into(),
            }]);
        }

        // ── UNLISTEN channel | UNLISTEN * ──
        if sql_lower.starts_with("unlisten ") {
            let channel = sql_lower.trim_start_matches("unlisten ").trim().trim_end_matches(';').trim();
            if channel == "*" {
                session.notifications.unlisten_all(&session.notification_hub);
            } else {
                session.notifications.unlisten(channel, &session.notification_hub);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "UNLISTEN".into(),
            }]);
        }

        // ── NOTIFY channel [, 'payload'] ──
        if sql_lower.starts_with("notify ") {
            let rest = sql.trim_start_matches(|c: char| c.is_alphabetic() || c == ' ')
                .trim_end_matches(';').trim();
            // rest is now "channel" or "channel, 'payload'"
            let (channel, payload) = if let Some(comma_pos) = rest.find(',') {
                let ch = rest[..comma_pos].trim().to_lowercase();
                let pl = rest[comma_pos + 1..].trim().trim_matches('\'').to_string();
                (ch, pl)
            } else {
                (rest.trim().to_lowercase(), String::new())
            };
            if !channel.is_empty() {
                session.notification_hub.notify(&channel, session.id, &payload);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "NOTIFY".into(),
            }]);
        }

        // ── DECLARE cursor_name [NO SCROLL] CURSOR [WITH HOLD] FOR query ──
        if sql_lower.starts_with("declare ") {
            return Some(self.handle_declare_cursor(sql, sql_lower, session));
        }

        // ── FETCH [count] FROM cursor_name ──
        if sql_lower.starts_with("fetch ") {
            return Some(self.handle_fetch_cursor(sql_lower, session));
        }

        // ── MOVE [count] FROM cursor_name ──
        if sql_lower.starts_with("move ") {
            return Some(self.handle_move_cursor(sql_lower, session));
        }

        // ── CLOSE cursor_name | CLOSE ALL ──
        if sql_lower.starts_with("close ") {
            let rest = sql_lower.trim_start_matches("close ").trim();
            if rest == "all" {
                session.cursors.clear();
            } else {
                session.cursors.remove(rest);
            }
            return Some(vec![BackendMessage::CommandComplete {
                tag: "CLOSE CURSOR".into(),
            }]);
        }

        None
    }

    // ── Savepoint helpers ──

    fn handle_savepoint(&self, sql_lower: &str, session: &mut PgSession) -> Vec<BackendMessage> {
        let name = sql_lower.trim_start_matches("savepoint ").trim().to_string();
        if !session.in_transaction() {
            return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "25P01".into(),
                message: "SAVEPOINT can only be used in transaction blocks".into(),
            }];
        }
        let txn_id = match session.txn.as_ref() {
            Some(t) => t.txn_id,
            None => return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "25P01".into(),
                message: "SAVEPOINT requires an active transaction".into(),
            }],
        };
        let write_set_len = self.storage.write_set_snapshot(txn_id);
        let read_set_len = self.storage.read_set_snapshot(txn_id);
        session.savepoints.push(crate::session::SavepointEntry {
            name,
            write_set_len,
            read_set_len,
        });
        vec![BackendMessage::CommandComplete {
            tag: "SAVEPOINT".into(),
        }]
    }

    fn handle_rollback_to(&self, sql_lower: &str, session: &mut PgSession) -> Vec<BackendMessage> {
        let rest = sql_lower.trim_start_matches("rollback to ").trim();
        let name = rest.strip_prefix("savepoint ").unwrap_or(rest).trim();
        if let Some(pos) = session.savepoints.iter().rposition(|sp| sp.name == name) {
            let write_snap = session.savepoints[pos].write_set_len;
            let read_snap = session.savepoints[pos].read_set_len;
            if let Some(ref txn) = session.txn {
                self.storage.rollback_write_set_after(txn.txn_id, write_snap);
                self.storage.rollback_read_set_after(txn.txn_id, read_snap);
            }
            session.savepoints.truncate(pos + 1);
            vec![BackendMessage::CommandComplete {
                tag: "ROLLBACK".into(),
            }]
        } else {
            vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "3B001".into(),
                message: format!("savepoint \"{}\" does not exist", name),
            }]
        }
    }

    fn handle_release_savepoint(&self, sql_lower: &str, session: &mut PgSession) -> Vec<BackendMessage> {
        let rest = sql_lower.trim_start_matches("release ").trim();
        let name = rest.strip_prefix("savepoint ").unwrap_or(rest).trim();
        if let Some(pos) = session.savepoints.iter().rposition(|sp| sp.name == name) {
            session.savepoints.remove(pos);
            vec![BackendMessage::CommandComplete {
                tag: "RELEASE".into(),
            }]
        } else {
            vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "3B001".into(),
                message: format!("savepoint \"{}\" does not exist", name),
            }]
        }
    }

    // ── Tenant helpers ──

    fn handle_create_tenant(&self, sql_lower: &str) -> Vec<BackendMessage> {
        let rest = sql_lower.trim_start_matches("create tenant ").trim();
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.is_empty() {
            return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42601".into(),
                message: "CREATE TENANT requires a tenant name".into(),
            }];
        }
        let tenant_name = parts[0].to_string();
        let mut max_qps = 0u64;
        let mut max_storage_bytes = 0u64;
        let mut i = 1;
        while i + 1 < parts.len() {
            match parts[i] {
                "max_qps" => { max_qps = parts[i+1].parse().unwrap_or(0); i += 2; }
                "max_storage_bytes" => { max_storage_bytes = parts[i+1].parse().unwrap_or(0); i += 2; }
                _ => { i += 1; }
            }
        }
        let tenant_id = self.tenant_registry.alloc_tenant_id();
        let mut config = falcon_common::tenant::TenantConfig::new(tenant_id, tenant_name.clone());
        config.quota.max_qps = max_qps;
        config.quota.max_storage_bytes = max_storage_bytes;
        if self.tenant_registry.register_tenant(config) {
            vec![BackendMessage::CommandComplete {
                tag: format!("CREATE TENANT {}", tenant_name),
            }]
        } else {
            vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42710".into(),
                message: format!("tenant \"{}\" already exists", tenant_name),
            }]
        }
    }

    fn handle_drop_tenant(&self, sql_lower: &str) -> Vec<BackendMessage> {
        let tenant_name = sql_lower.trim_start_matches("drop tenant ").trim().to_string();
        let found = self.tenant_registry.tenant_ids().into_iter().find(|tid| {
            self.tenant_registry.get_config(*tid).map(|c| c.name == tenant_name).unwrap_or(false)
        });
        if let Some(tid) = found {
            self.tenant_registry.remove_tenant(tid);
            vec![BackendMessage::CommandComplete {
                tag: format!("DROP TENANT {}", tenant_name),
            }]
        } else {
            vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42704".into(),
                message: format!("tenant \"{}\" does not exist", tenant_name),
            }]
        }
    }

    // ── Cursor helpers ──

    /// Handle DECLARE cursor_name [NO SCROLL] CURSOR [WITH HOLD] FOR query
    fn handle_declare_cursor(
        &self,
        sql: &str,
        sql_lower: &str,
        session: &mut PgSession,
    ) -> Vec<BackendMessage> {
        // Parse: DECLARE name [NO SCROLL] CURSOR [WITH HOLD] FOR <query>
        let rest = sql_lower.trim_start_matches("declare ").trim();
        let parts: Vec<&str> = rest.splitn(2, " cursor").collect();
        if parts.len() < 2 {
            return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42601".into(),
                message: "syntax error in DECLARE CURSOR".into(),
            }];
        }

        let cursor_name = parts[0].trim_end_matches(" no scroll").trim().to_string();
        let with_hold = parts[1].contains("with hold");

        // Extract the FOR query
        let for_pos = rest.find(" for ");
        if for_pos.is_none() {
            return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42601".into(),
                message: "DECLARE CURSOR requires FOR <query>".into(),
            }];
        }
        let for_idx = match for_pos {
            Some(i) => i,
            None => return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42601".into(),
                message: "DECLARE CURSOR: FOR position not found".into(),
            }],
        };
        // Get the original-case SQL for the query portion
        let declare_prefix_len = "declare ".len();
        let query_start = declare_prefix_len + for_idx + " for ".len();
        let query = if query_start < sql.len() {
            sql[query_start..].trim().trim_end_matches(';').to_string()
        } else {
            return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42601".into(),
                message: "empty query in DECLARE CURSOR".into(),
            }];
        };

        // Execute the query and store results in cursor
        let txn = session.txn.as_ref();
        match self.execute_sql_for_cursor(&query, txn) {
            Ok((columns, rows)) => {
                session.cursors.insert(cursor_name.clone(), crate::session::CursorState {
                    name: cursor_name,
                    rows,
                    columns,
                    position: 0,
                    with_hold,
                });
                vec![BackendMessage::CommandComplete {
                    tag: "DECLARE CURSOR".into(),
                }]
            }
            Err(msg) => vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "42P03".into(),
                message: msg,
            }],
        }
    }

    /// Execute a SQL query and return (columns, rows) for cursor materialization.
    #[allow(clippy::type_complexity)]
    fn execute_sql_for_cursor(
        &self,
        query: &str,
        txn: Option<&falcon_txn::TxnHandle>,
    ) -> Result<(Vec<(String, falcon_common::types::DataType)>, Vec<falcon_common::datum::OwnedRow>), String> {
        let catalog = self.storage.get_catalog();
        let mut binder = Binder::new(catalog);
        let stmts = parse_sql(query).map_err(|e| format!("parse error: {}", e))?;
        if stmts.is_empty() {
            return Err("empty query".into());
        }
        let bound = binder.bind(&stmts[0]).map_err(|e| format!("bind error: {}", e))?;
        let plan = Planner::plan(&bound).map_err(|e| format!("plan error: {}", e))?;
        let result = self.executor.execute(&plan, txn).map_err(|e| format!("exec error: {}", e))?;
        match result {
            falcon_executor::ExecutionResult::Query { columns, rows } => Ok((columns, rows)),
            _ => Err("DECLARE CURSOR requires a SELECT query".into()),
        }
    }

    /// Handle FETCH [count] FROM cursor_name
    fn handle_fetch_cursor(
        &self,
        sql_lower: &str,
        session: &mut PgSession,
    ) -> Vec<BackendMessage> {
        let rest = sql_lower.trim_start_matches("fetch ").trim();

        // Parse: FETCH [NEXT | FORWARD | count] [FROM | IN] cursor_name
        let (count, cursor_name) = Self::parse_fetch_args(rest);

        let cursor = match session.cursors.get_mut(&cursor_name) {
            Some(c) => c,
            None => return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "34000".into(),
                message: format!("cursor \"{}\" does not exist", cursor_name),
            }],
        };

        // Build field descriptions
        let fields: Vec<FieldDescription> = cursor.columns.iter().map(|(name, dt)| {
            let (type_oid, type_len) = Self::data_type_to_pg_oid(dt);
            FieldDescription {
                name: name.clone(),
                table_oid: 0, column_attr: 0,
                type_oid, type_len,
                type_modifier: -1, format_code: 0,
            }
        }).collect();

        let mut msgs = vec![BackendMessage::RowDescription { fields }];

        // Fetch up to `count` rows from current position
        let end = (cursor.position + count).min(cursor.rows.len());
        for i in cursor.position..end {
            let row_vals: Vec<Option<String>> = cursor.rows[i].values.iter()
                .map(|d| if d.is_null() { None } else { Some(format!("{}", d)) })
                .collect();
            msgs.push(BackendMessage::DataRow { values: row_vals });
        }
        let fetched = end - cursor.position;
        cursor.position = end;

        msgs.push(BackendMessage::CommandComplete {
            tag: format!("FETCH {}", fetched),
        });
        msgs
    }

    /// Parse FETCH arguments: [NEXT | FORWARD | ALL | count] [FROM | IN] cursor_name
    fn parse_fetch_args(rest: &str) -> (usize, String) {
        let tokens: Vec<&str> = rest.split_whitespace().collect();
        match tokens.len() {
            1 => (1, tokens[0].to_string()),
            2 => {
                if tokens[0] == "from" || tokens[0] == "in"
                    || tokens[0] == "next" || tokens[0] == "forward"
                {
                    (1, tokens[1].to_string())
                } else if tokens[0] == "all" {
                    (usize::MAX, tokens[1].to_string())
                } else if let Ok(n) = tokens[0].parse::<usize>() {
                    (n, tokens[1].to_string())
                } else {
                    (1, tokens[0].to_string())
                }
            }
            3.. => {
                // Try: count FROM name / FORWARD count FROM name / NEXT FROM name
                let first = tokens[0];
                let last_token = tokens.last().copied().unwrap_or("").to_string();
                if first == "next" || first == "forward" {
                    let count = tokens.get(1).and_then(|t| t.parse::<usize>().ok()).unwrap_or(1);
                    (count, last_token)
                } else if first == "all" {
                    (usize::MAX, last_token)
                } else if let Ok(n) = first.parse::<usize>() {
                    (n, last_token)
                } else {
                    (1, last_token)
                }
            }
            _ => (1, String::new()),
        }
    }

    /// Parse MOVE arguments: [BACKWARD | FORWARD] [count | ALL] [FROM | IN] cursor_name
    /// Returns (count, cursor_name, backward).
    fn parse_move_args(rest: &str) -> (usize, String, bool) {
        let tokens: Vec<&str> = rest.split_whitespace().collect();
        let mut backward = false;
        let mut count = 1usize;
        let mut cursor_name = String::new();

        let mut i = 0;
        if i < tokens.len() && tokens[i] == "backward" {
            backward = true;
            i += 1;
        } else if i < tokens.len() && tokens[i] == "forward" {
            i += 1;
        }

        if i < tokens.len() {
            if tokens[i] == "all" {
                count = usize::MAX;
                i += 1;
            } else if let Ok(n) = tokens[i].parse::<usize>() {
                count = n;
                i += 1;
            }
        }

        // Skip FROM/IN
        if i < tokens.len() && (tokens[i] == "from" || tokens[i] == "in") {
            i += 1;
        }

        if i < tokens.len() {
            cursor_name = tokens[i].to_string();
        }

        (count, cursor_name, backward)
    }

    /// Map DataType to PG OID and type_len for cursor column descriptions.
    fn data_type_to_pg_oid(dt: &falcon_common::types::DataType) -> (i32, i16) {
        use falcon_common::types::DataType;
        match dt {
            DataType::Int32 => (23, 4),    // int4
            DataType::Int64 => (20, 8),    // int8
            DataType::Float64 => (701, 8), // float8
            DataType::Text => (25, -1),    // text
            DataType::Boolean => (16, 1),  // bool
            DataType::Timestamp => (1114, 8), // timestamp
            DataType::Date => (1082, 4),   // date
            DataType::Decimal(_, _) => (1700, -1), // numeric
            DataType::Time => (1083, 8),   // time
            DataType::Interval => (1186, 16), // interval
            DataType::Uuid => (2950, 16),  // uuid
            DataType::Bytea => (17, -1),   // bytea
            DataType::Jsonb => (3802, -1), // jsonb
            _ => (25, -1),                 // fallback to text
        }
    }

    fn handle_move_cursor(&self, sql_lower: &str, session: &mut PgSession) -> Vec<BackendMessage> {
        let rest = sql_lower.trim_start_matches("move ").trim();
        let (count, cursor_name, backward) = Self::parse_move_args(rest);
        match session.cursors.get_mut(&cursor_name) {
            Some(cursor) => {
                let moved = if backward {
                    let actual = count.min(cursor.position);
                    cursor.position -= actual;
                    actual
                } else {
                    let remaining = cursor.rows.len().saturating_sub(cursor.position);
                    let actual = count.min(remaining);
                    cursor.position += actual;
                    actual
                };
                vec![BackendMessage::CommandComplete {
                    tag: format!("MOVE {}", moved),
                }]
            }
            None => {
                vec![BackendMessage::ErrorResponse {
                    severity: "ERROR".into(),
                    code: "34000".into(),
                    message: format!("cursor \"{}\" does not exist", cursor_name),
                }]
            }
        }
    }
}
