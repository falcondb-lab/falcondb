use falcon_common::datum::Datum;
use falcon_common::error::FalconError;
use falcon_common::types::ShardId;
use falcon_planner::PhysicalPlan;
use falcon_sql_frontend::types::{BinOp, BoundExpr, BoundInsert, OnConflictAction};

use crate::codec::{BackendMessage, FieldDescription};
use crate::handler::{BeginOptions, QueryHandler};
use crate::session::PgSession;
use falcon_executor::ExecutionResult;
use falcon_txn::TxnClassification;

pub(crate) fn datum_add(a: &Datum, b: &Datum) -> Option<Datum> {
    match (a, b) {
        (Datum::Int32(x), Datum::Int32(y)) => Some(Datum::Int32(x.wrapping_add(*y))),
        (Datum::Int64(x), Datum::Int64(y)) => Some(Datum::Int64(x.wrapping_add(*y))),
        (Datum::Int32(x), Datum::Int64(y)) => Some(Datum::Int64((*x as i64).wrapping_add(*y))),
        (Datum::Int64(x), Datum::Int32(y)) => Some(Datum::Int64(x.wrapping_add(*y as i64))),
        (Datum::Float64(x), Datum::Float64(y)) => Some(Datum::Float64(x + y)),
        _ => None,
    }
}

pub(crate) fn datum_sub(a: &Datum, b: &Datum) -> Option<Datum> {
    match (a, b) {
        (Datum::Int32(x), Datum::Int32(y)) => Some(Datum::Int32(x.wrapping_sub(*y))),
        (Datum::Int64(x), Datum::Int64(y)) => Some(Datum::Int64(x.wrapping_sub(*y))),
        (Datum::Int32(x), Datum::Int64(y)) => Some(Datum::Int64((*x as i64).wrapping_sub(*y))),
        (Datum::Int64(x), Datum::Int32(y)) => Some(Datum::Int64(x.wrapping_sub(*y as i64))),
        (Datum::Float64(x), Datum::Float64(y)) => Some(Datum::Float64(x - y)),
        _ => None,
    }
}

impl QueryHandler {
    /// Fast-path parser for simple INSERT INTO table (cols) VALUES (...), ...
    /// Bypasses sqlparser-rs entirely, producing BoundInsert directly.
    /// Returns None if the SQL doesn't match the simple pattern (falls back to standard path).
    pub(crate) fn try_fast_insert_parse(&self, sql: &str) -> Option<BoundInsert> {
        let trimmed = sql.trim();
        // Quick prefix check (case-insensitive)
        if trimmed.len() < 20 || !trimmed[..11].eq_ignore_ascii_case("INSERT INTO") {
            return None;
        }
        let rest = trimmed[11..].trim_start();

        // Extract table name (identifier chars)
        let tbl_end = rest.find(|c: char| !c.is_alphanumeric() && c != '_' && c != '"')?;
        if tbl_end == 0 { return None; }
        let table_name = rest[..tbl_end].trim_matches('"');
        let rest = rest[tbl_end..].trim_start();

        // Extract column list: (col1, col2, ...)
        if !rest.starts_with('(') { return None; }
        let col_end = rest.find(')')?;
        let col_names: Vec<&str> = rest[1..col_end].split(',').map(|s| s.trim().trim_matches('"')).collect();
        if col_names.is_empty() { return None; }
        let rest = rest[col_end + 1..].trim_start();

        // Expect VALUES keyword
        if rest.len() < 6 || !rest[..6].eq_ignore_ascii_case("VALUES") { return None; }
        // Reject RETURNING / ON CONFLICT DO UPDATE (fall back to standard path)
        // Allow ON CONFLICT DO NOTHING
        let mut on_conflict_do_nothing = false;
        {
            fn contains_ci(hay: &str, needle: &str) -> bool {
                hay.as_bytes().windows(needle.len()).any(|w|
                    w.eq_ignore_ascii_case(needle.as_bytes()))
            }
            if contains_ci(rest, "RETURNING") {
                return None;
            }
            if contains_ci(rest, "ON CONFLICT") {
                if contains_ci(rest, "DO NOTHING") {
                    on_conflict_do_nothing = true;
                } else {
                    return None;
                }
            }
        }
        let values_str = rest[6..].trim_start();
        // Strip trailing ON CONFLICT DO NOTHING
        let values_str = if on_conflict_do_nothing {
            let bytes = values_str.as_bytes();
            let needle = b"ON CONFLICT";
            let idx = bytes.windows(needle.len()).position(|w|
                w.eq_ignore_ascii_case(needle));
            if let Some(i) = idx {
                values_str[..i].trim_end()
            } else {
                values_str
            }
        } else {
            values_str
        };

        // Last-1 schema cache: skip catalog RwLock + deep clone for repeated table
        let schema = {
            let cached = self.schema_cache.lock();
            if let Some((ref name, ref s)) = *cached {
                if name == table_name { Some(s.clone()) } else { None }
            } else { None }
        }.or_else(|| {
            let s = self.storage.get_table_schema(table_name)?;
            *self.schema_cache.lock() = Some((table_name.to_owned(), s.clone()));
            Some(s)
        })?;
        let col_indices: Vec<usize> = col_names.iter().map(|name| {
            schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
        }).collect::<Option<Vec<_>>>()?;

        // Collect column DataTypes for value parsing
        use falcon_common::types::DataType;
        let col_types: Vec<&DataType> = col_indices.iter().map(|&i| &schema.columns[i].data_type).collect();
        let ncols = col_indices.len();

        // Parse VALUES tuples using byte-level scanner
        let bytes = values_str.as_bytes();
        let len = bytes.len();
        let mut pos = 0;
        let mut rows: Vec<Vec<BoundExpr>> = Vec::new();

        while pos < len {
            // Skip whitespace and commas between tuples
            while pos < len && matches!(bytes[pos], b' ' | b'\t' | b'\n' | b'\r' | b',') {
                pos += 1;
            }
            if pos >= len || bytes[pos] == b';' { break; }
            if bytes[pos] != b'(' { return None; }
            pos += 1;

            let mut vals = Vec::with_capacity(ncols);
            #[allow(clippy::needless_range_loop)]
            for vi in 0..ncols {
                // Skip whitespace
                while pos < len && bytes[pos] == b' ' { pos += 1; }
                if vi > 0 {
                    if pos < len && bytes[pos] == b',' { pos += 1; }
                    while pos < len && bytes[pos] == b' ' { pos += 1; }
                }

                // Parse one value
                let datum = if bytes[pos] == b'\'' {
                    // String literal
                    pos += 1;
                    let mut s = String::new();
                    let mut start = pos;
                    loop {
                        if pos >= len { return None; }
                        if bytes[pos] == b'\'' {
                            s.push_str(std::str::from_utf8(&bytes[start..pos]).ok()?);
                            pos += 1;
                            if pos < len && bytes[pos] == b'\'' {
                                // Escaped quote
                                s.push('\'');
                                pos += 1;
                                start = pos;
                            } else {
                                break;
                            }
                        } else {
                            pos += 1;
                        }
                    }
                    Datum::Text(s)
                } else if bytes[pos] == b'N' || bytes[pos] == b'n' {
                    // NULL
                    if pos + 4 <= len && values_str[pos..pos+4].eq_ignore_ascii_case("null") {
                        pos += 4;
                        Datum::Null
                    } else {
                        return None;
                    }
                } else if bytes[pos] == b't' || bytes[pos] == b'T' {
                    // true
                    if pos + 4 <= len && values_str[pos..pos+4].eq_ignore_ascii_case("true") {
                        pos += 4;
                        Datum::Boolean(true)
                    } else {
                        return None;
                    }
                } else if bytes[pos] == b'f' || bytes[pos] == b'F' {
                    // false
                    if pos + 5 <= len && values_str[pos..pos+5].eq_ignore_ascii_case("false") {
                        pos += 5;
                        Datum::Boolean(false)
                    } else {
                        return None;
                    }
                } else if bytes[pos] == b'c' || bytes[pos] == b'C' {
                    // CURRENT_TIMESTAMP / CURRENT_DATE
                    if pos + 17 <= len && values_str[pos..pos+17].eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
                        pos += 17;
                        let now = chrono::Utc::now();
                        Datum::Timestamp(now.timestamp() * 1_000_000 + now.timestamp_subsec_micros() as i64)
                    } else if pos + 12 <= len && values_str[pos..pos+12].eq_ignore_ascii_case("CURRENT_DATE") {
                        pos += 12;
                        let now = chrono::Utc::now();
                        Datum::Timestamp(now.timestamp() * 1_000_000 + now.timestamp_subsec_micros() as i64)
                    } else {
                        return None;
                    }
                } else if bytes[pos] == b'-' || bytes[pos].is_ascii_digit() {
                    // Number
                    let num_start = pos;
                    if bytes[pos] == b'-' { pos += 1; }
                    let mut is_float = false;
                    while pos < len && bytes[pos].is_ascii_digit() { pos += 1; }
                    if pos < len && bytes[pos] == b'.' {
                        is_float = true;
                        pos += 1;
                        while pos < len && bytes[pos].is_ascii_digit() { pos += 1; }
                    }
                    // Scientific notation
                    if pos < len && (bytes[pos] == b'e' || bytes[pos] == b'E') {
                        is_float = true;
                        pos += 1;
                        if pos < len && (bytes[pos] == b'+' || bytes[pos] == b'-') { pos += 1; }
                        while pos < len && bytes[pos].is_ascii_digit() { pos += 1; }
                    }
                    let num_str = std::str::from_utf8(&bytes[num_start..pos]).ok()?;
                    if is_float {
                        Datum::Float64(num_str.parse().ok()?)
                    } else {
                        match col_types[vi] {
                            DataType::Int32 => Datum::Int32(num_str.parse().ok()?),
                            DataType::Float64 => Datum::Float64(num_str.parse().ok()?),
                            _ => Datum::Int64(num_str.parse().ok()?),
                        }
                    }
                } else {
                    return None;
                };
                vals.push(BoundExpr::Literal(datum));
            }

            // Skip whitespace and expect ')'
            while pos < len && bytes[pos] == b' ' { pos += 1; }
            if pos >= len || bytes[pos] != b')' { return None; }
            pos += 1;
            rows.push(vals);
        }

        if rows.is_empty() { return None; }

        Some(BoundInsert {
            table_id: schema.id,
            table_name: table_name.to_owned(),
            schema,
            columns: col_indices,
            rows,
            source_select: None,
            returning: vec![],
            on_conflict: if on_conflict_do_nothing {
                Some(OnConflictAction::DoNothing)
            } else {
                None
            },
        })
    }

    /// Ultra-fast direct insert: skip executor entirely for simple schemas.
    /// Handles dynamic defaults (CURRENT_TIMESTAMP etc.) and partial column lists.
    /// Returns None if schema is too complex (constraints, serials, etc.).
    #[allow(unreachable_code, unused_variables)]
    pub(crate) fn try_direct_insert(
        &self,
        ins: &BoundInsert,
        session: &mut PgSession,
    ) -> Option<Result<Vec<BackendMessage>, FalconError>> {
        // Disabled: saves ~1µs per txn at 16c but causes convoy at 64c (51K→20K).
        // The executor path's overhead naturally staggers flush_lock arrivals.
        return None;
        use falcon_common::datum::OwnedRow;
        use falcon_common::schema::DefaultFn;
        if self.cluster.dist_engine.is_some() || session.txn.is_some() {
            return None;
        }
        let schema = &ins.schema;
        if !schema.check_constraints.is_empty()
            || !schema.unique_constraints.is_empty()
            || !schema.foreign_keys.is_empty()
            || schema.columns.iter().any(|c| c.is_serial)
        {
            return None;
        }
        let on_conflict_skip = matches!(ins.on_conflict, Some(OnConflictAction::DoNothing));
        if ins.on_conflict.is_some() && !on_conflict_skip {
            return None;
        }
        let ncols = schema.columns.len();
        let mut rows: Vec<OwnedRow> = Vec::with_capacity(ins.rows.len());
        for row_exprs in &ins.rows {
            // Start with static defaults for all columns
            let mut values: Vec<Datum> = schema.columns.iter()
                .map(|c| c.default_value.clone().unwrap_or(Datum::Null))
                .collect();
            // Overlay provided columns with literal values
            for (i, expr) in row_exprs.iter().enumerate() {
                match expr {
                    BoundExpr::Literal(d) => values[ins.columns[i]] = d.clone(),
                    _ => return None,
                }
            }
            // Fill dynamic defaults for still-NULL columns
            for (&col_idx, dfn) in &schema.dynamic_defaults {
                if col_idx < ncols && values[col_idx].is_null() {
                    values[col_idx] = match dfn {
                        DefaultFn::CurrentTimestamp => {
                            Datum::Timestamp(chrono::Utc::now().timestamp_micros())
                        }
                        DefaultFn::CurrentDate => {
                            let today = chrono::Utc::now().date_naive();
                            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            Datum::Date((today - epoch).num_days() as i32)
                        }
                        DefaultFn::CurrentTime => {
                            use chrono::Timelike;
                            let t = chrono::Utc::now().time();
                            Datum::Time(
                                t.num_seconds_from_midnight() as i64 * 1_000_000
                                    + t.nanosecond() as i64 / 1_000,
                            )
                        }
                        DefaultFn::Nextval(_) => continue,
                    };
                }
            }
            rows.push(OwnedRow::new(values));
        }
        let txn = self.txn_mgr.begin_autocommit(session.default_isolation);
        let txn_id = txn.txn_id;
        session.txn = Some(txn);
        let mut count = 0u64;
        for row in rows {
            match self.storage.insert(ins.table_id, row, txn_id) {
                Ok(_) => count += 1,
                Err(falcon_common::error::StorageError::DuplicateKey) if on_conflict_skip => {}
                Err(e) => {
                    self.txn_mgr.abort_autocommit(txn_id);
                    session.txn = None;
                    return Some(Ok(vec![self.error_response(&FalconError::Storage(e))]));
                }
            }
        }
        if let Err(e) = self.txn_mgr.commit_autocommit(txn_id) {
            session.txn = None;
            return Some(Ok(vec![self.error_response(&FalconError::Txn(e))]));
        }
        session.txn = None;
        let tag = if count == 1 { "INSERT 0 1".to_owned() } else { format!("INSERT 0 {count}") };
        Some(Ok(vec![BackendMessage::CommandComplete { tag }]))
    }

    /// Fast-path parser for simple UPDATE table SET col = expr WHERE col = val.
    /// Handles: col = literal, col = col +/- literal.
    /// Returns None if the SQL doesn't match (falls back to standard path).
    pub(crate) fn try_fast_update_parse(&self, sql: &str) -> Option<PhysicalPlan> {
        let trimmed = sql.trim().trim_end_matches(';').trim_end();
        if trimmed.len() < 20 || !trimmed[..6].eq_ignore_ascii_case("UPDATE") {
            return None;
        }
        let rest = trimmed[6..].trim_start();

        // Table name
        let tbl_end = rest.find(|c: char| !c.is_alphanumeric() && c != '_' && c != '"')?;
        if tbl_end == 0 { return None; }
        let table_name = rest[..tbl_end].trim_matches('"');
        let rest = rest[tbl_end..].trim_start();

        // SET keyword
        if rest.len() < 4 || !rest[..3].eq_ignore_ascii_case("SET") { return None; }
        let rest = rest[3..].trim_start();

        // Reject RETURNING / FROM (complex) — zero-alloc
        fn contains_ci_u(hay: &str, needle: &str) -> bool {
            hay.as_bytes().windows(needle.len()).any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
        }
        if contains_ci_u(rest, "RETURNING") || contains_ci_u(rest, " FROM ") {
            return None;
        }

        // Find WHERE — zero-alloc byte scan
        let where_pos = {
            let bytes = rest.as_bytes();
            let mut found = None;
            for i in 0..bytes.len().saturating_sub(6) {
                if bytes[i] == b' ' && bytes[i+1..i+6].eq_ignore_ascii_case(b"WHERE") && (i + 6 >= bytes.len() || bytes[i+6] == b' ') {
                    found = Some(i);
                    break;
                }
            }
            found?
        };
        let set_part = rest[..where_pos].trim();
        let where_part = rest[where_pos + 7..].trim();

        // Look up table (use schema cache)
        let schema = {
            let cached = self.schema_cache.lock();
            if let Some((ref name, ref s)) = *cached {
                if name == table_name { Some(s.clone()) } else { None }
            } else { None }
        }.or_else(|| {
            let s = self.storage.get_table_schema(table_name)?;
            *self.schema_cache.lock() = Some((table_name.to_owned(), s.clone()));
            Some(s)
        })?;

        // Parse assignments: col = expr [, col = expr]
        let mut assignments = Vec::new();
        for assign_str in set_part.split(',') {
            let assign_str = assign_str.trim();
            let eq_pos = assign_str.find('=')?;
            let col_name = assign_str[..eq_pos].trim().trim_matches('"');
            let expr_str = assign_str[eq_pos + 1..].trim();

            let col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))?;

            // Parse expr: literal | col +/- literal
            let expr = self.parse_fast_expr(expr_str, &schema)?;
            assignments.push((col_idx, expr));
        }

        // Parse WHERE: col = literal [AND col = literal]
        let mut filters: Vec<BoundExpr> = Vec::new();
        // AND splitting — zero-alloc byte scan
        let conds: Vec<&str> = {
            let bytes = where_part.as_bytes();
            let mut parts = Vec::new();
            let mut start = 0;
            let mut i = 0;
            while i + 5 <= bytes.len() {
                if bytes[i] == b' ' && bytes[i+1..i+4].eq_ignore_ascii_case(b"AND") && bytes[i+4] == b' ' {
                    parts.push(where_part[start..i].trim());
                    start = i + 5;
                    i = start;
                } else {
                    i += 1;
                }
            }
            parts.push(where_part[start..].trim());
            parts
        };
        for cond in conds {
            let eq_pos = cond.find('=')?;
            // Reject !=, >=, <=
            if eq_pos > 0 && matches!(cond.as_bytes()[eq_pos - 1], b'!' | b'>' | b'<') {
                return None;
            }
            if eq_pos + 1 < cond.len() && cond.as_bytes()[eq_pos + 1] == b'=' {
                return None; // == not valid SQL but bail
            }
            let col_name = cond[..eq_pos].trim().trim_matches('"');
            let val_str = cond[eq_pos + 1..].trim();
            let col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))?;
            let val = self.parse_fast_literal(val_str, &schema.columns[col_idx].data_type)?;
            filters.push(BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::ColumnRef(col_idx)),
                op: BinOp::Eq,
                right: Box::new(BoundExpr::Literal(val)),
            });
        }

        let filter = if filters.len() == 1 {
            Some(filters.pop().unwrap())
        } else if filters.len() > 1 {
            let mut combined = filters.pop().unwrap();
            while let Some(f) = filters.pop() {
                combined = BoundExpr::BinaryOp {
                    left: Box::new(f),
                    op: BinOp::And,
                    right: Box::new(combined),
                };
            }
            Some(combined)
        } else {
            None
        };

        Some(PhysicalPlan::Update {
            table_id: schema.id,
            schema,
            assignments,
            filter,
            returning: vec![],
            from_table: None,
        })
    }

    pub(crate) fn parse_fast_expr(&self, s: &str, schema: &falcon_common::schema::TableSchema) -> Option<BoundExpr> {
        let s = s.trim();
        // Try: col + literal or col - literal
        if let Some(plus_pos) = s.find('+') {
            let lhs = s[..plus_pos].trim();
            let rhs = s[plus_pos + 1..].trim();
            if let Some(col_idx) = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(lhs)) {
                let val = self.parse_fast_literal(rhs, &schema.columns[col_idx].data_type)?;
                return Some(BoundExpr::BinaryOp {
                    left: Box::new(BoundExpr::ColumnRef(col_idx)),
                    op: BinOp::Plus,
                    right: Box::new(BoundExpr::Literal(val)),
                });
            }
        }
        if let Some(minus_pos) = s.rfind('-') {
            if minus_pos > 0 {
                let lhs = s[..minus_pos].trim();
                let rhs = s[minus_pos + 1..].trim();
                if let Some(col_idx) = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(lhs)) {
                    let val = self.parse_fast_literal(rhs, &schema.columns[col_idx].data_type)?;
                    return Some(BoundExpr::BinaryOp {
                        left: Box::new(BoundExpr::ColumnRef(col_idx)),
                        op: BinOp::Minus,
                        right: Box::new(BoundExpr::Literal(val)),
                    });
                }
            }
        }
        // Plain literal
        let dt = falcon_common::types::DataType::Text;
        let val = self.parse_fast_literal(s, &dt)?;
        Some(BoundExpr::Literal(val))
    }

    pub(crate) fn parse_fast_literal(&self, s: &str, dt: &falcon_common::types::DataType) -> Option<Datum> {
        use falcon_common::types::DataType;
        let s = s.trim();
        if s.eq_ignore_ascii_case("NULL") { return Some(Datum::Null); }
        if s.starts_with('\'') {
            return Some(Datum::Text(s.trim_matches('\'').to_owned()));
        }
        match dt {
            DataType::Int32 => Some(Datum::Int32(s.parse().ok()?)),
            DataType::Float64 => Some(Datum::Float64(s.parse().ok()?)),
            _ => Some(Datum::Int64(s.parse().ok()?)),
        }
    }

    /// Execute a single DML plan (fast-path for INSERT). Handles autocommit.
    pub(crate) fn execute_single_plan(
        &self,
        _sql: &str,
        plan: PhysicalPlan,
        session: &mut PgSession,
    ) -> Result<Vec<BackendMessage>, FalconError> {
        let mut messages = Vec::new();

        let auto_txn = if session.txn.is_none() {
            let txn = self.txn_mgr.begin_autocommit(session.default_isolation);
            session.txn = Some(txn);
            true
        } else {
            false
        };

        // Execute
        let result = if let Some(dist) = &self.cluster.dist_engine {
            dist.execute(&plan, session.txn.as_ref())
        } else {
            self.executor.execute(&plan, session.txn.as_ref())
        };

        match result {
            Ok(ExecutionResult::Dml { rows_affected, tag }) => {
                let cmd_tag = match (tag, rows_affected) {
                    ("INSERT", 1) => "INSERT 0 1".to_owned(),
                    ("INSERT", n) => format!("INSERT 0 {n}"),
                    ("UPDATE", 1) => "UPDATE 1".to_owned(),
                    ("UPDATE", n) => format!("UPDATE {n}"),
                    ("DELETE", 1) => "DELETE 1".to_owned(),
                    ("DELETE", n) => format!("DELETE {n}"),
                    (t, n) => format!("{t} {n}"),
                };
                messages.push(BackendMessage::CommandComplete { tag: cmd_tag });
            }
            Ok(other) => {
                // Unexpected result type for fast-path; shouldn't happen
                messages.push(BackendMessage::CommandComplete {
                    tag: format!("{other:?}"),
                });
            }
            Err(e) => {
                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        self.txn_mgr.abort_autocommit(txn.txn_id);
                    }
                    session.txn = None;
                }
                messages.push(self.error_response(&e));
                return Ok(messages);
            }
        }

        if auto_txn {
            if let Some(ref txn) = session.txn {
                let _ = self.txn_mgr.commit_autocommit(txn.txn_id);
                falcon_common::globals::db_stats().xact_commit.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            session.txn = None;
        }

        Ok(messages)
    }

    pub(crate) fn fast_begin(&self, session: &mut PgSession) -> Vec<BackendMessage> {
        if session.in_transaction() {
            return vec![
                BackendMessage::NoticeResponse {
                    message: "there is already a transaction in progress".into(),
                },
                BackendMessage::CommandComplete { tag: "BEGIN".into() },
            ];
        }
        if let Err(reason) = self.storage.check_tenant_quota(session.tenant_id) {
            return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "53400".into(),
                message: reason,
            }];
        }
        let txn = match self.txn_mgr.try_begin_with_classification(
            session.default_isolation,
            TxnClassification::local(ShardId(0)),
        ) {
            Ok(t) => t,
            Err(e) => {
                let ce: FalconError = e.into();
                return vec![self.error_response(&ce)];
            }
        };
        self.storage.ext.cdc_manager.emit_begin(txn.txn_id);
        self.storage.record_tenant_txn_begin(session.tenant_id);
        session.txn = Some(txn);
        session.autocommit = false;
        vec![BackendMessage::CommandComplete { tag: "BEGIN".into() }]
    }

    pub(crate) fn fast_begin_with_options(&self, session: &mut PgSession, opts: BeginOptions) -> Vec<BackendMessage> {
        if session.in_transaction() {
            return vec![
                BackendMessage::NoticeResponse {
                    message: "there is already a transaction in progress".into(),
                },
                BackendMessage::CommandComplete { tag: "BEGIN".into() },
            ];
        }
        if let Err(reason) = self.storage.check_tenant_quota(session.tenant_id) {
            return vec![BackendMessage::ErrorResponse {
                severity: "ERROR".into(),
                code: "53400".into(),
                message: reason,
            }];
        }
        let isolation = opts.isolation.unwrap_or(session.default_isolation);
        let txn = match self.txn_mgr.try_begin_with_classification(
            isolation,
            TxnClassification::local(ShardId(0)),
        ) {
            Ok(t) => t,
            Err(e) => {
                let ce: FalconError = e.into();
                return vec![self.error_response(&ce)];
            }
        };
        self.storage.ext.cdc_manager.emit_begin(txn.txn_id);
        self.storage.record_tenant_txn_begin(session.tenant_id);
        session.txn = Some(txn);
        session.autocommit = false;
        // read_only is accepted but not enforced (no write barrier yet)
        vec![BackendMessage::CommandComplete { tag: "BEGIN".into() }]
    }

    pub(crate) fn fast_commit(&self, session: &mut PgSession) -> Vec<BackendMessage> {
        if let Some(ref txn) = session.txn {
            match self.txn_mgr.commit(txn.txn_id) {
                Ok(commit_ts) => {
                    if let (Some(ref waiter), Some(ref group)) =
                        (&self.cluster.sync_waiter, &self.cluster.ha_group)
                    {
                        let timeout = std::time::Duration::from_secs(5);
                        let group_read = group.read();
                        if let Err(e) = waiter.wait_for_commit(commit_ts.0, &group_read, timeout) {
                            tracing::warn!("Sync replication wait failed after COMMIT: {}", e);
                        }
                    }
                    self.storage.record_tenant_txn_commit(session.tenant_id);
                    falcon_observability::record_txn_metrics("committed");
                    falcon_common::globals::db_stats().xact_commit.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    session.txn = None;
                    session.autocommit = true;
                    session.revert_local_gucs();
                    self.flush_txn_stats();
                    vec![BackendMessage::CommandComplete { tag: "COMMIT".into() }]
                }
                Err(e) => {
                    self.storage.record_tenant_txn_abort(session.tenant_id);
                    falcon_observability::record_txn_metrics("aborted");
                    falcon_common::globals::db_stats().xact_rollback.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    session.txn = None;
                    session.autocommit = true;
                    session.revert_local_gucs();
                    self.flush_txn_stats();
                    vec![self.error_response(&FalconError::Txn(e))]
                }
            }
        } else {
            vec![
                BackendMessage::NoticeResponse {
                    message: "there is no transaction in progress".into(),
                },
                BackendMessage::CommandComplete { tag: "COMMIT".into() },
            ]
        }
    }

    pub(crate) fn fast_rollback(&self, session: &mut PgSession) -> Vec<BackendMessage> {
        if let Some(ref txn) = session.txn {
            let _ = self.txn_mgr.abort(txn.txn_id);
            self.storage.record_tenant_txn_abort(session.tenant_id);
            falcon_observability::record_txn_metrics("aborted");
            falcon_common::globals::db_stats().xact_rollback.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            session.txn = None;
            session.autocommit = true;
            session.revert_local_gucs();
            self.flush_txn_stats();
        }
        vec![BackendMessage::CommandComplete { tag: "ROLLBACK".into() }]
    }

    /// Fast-path SELECT for simple point lookups: SELECT col [, col2] FROM table WHERE pk = val
    /// Bypasses sqlparser, binder, planner, and build_indexed_columns entirely.
    pub(crate) fn try_fast_select(&self, sql: &str, session: &mut PgSession) -> Option<Vec<BackendMessage>> {
        let trimmed = sql.trim().trim_end_matches(';').trim_end();
        if trimmed.len() < 20 || !trimmed[..6].eq_ignore_ascii_case("SELECT") {
            return None;
        }
        let rest = trimmed[6..].trim_start();

        // Find FROM keyword
        let from_pos = {
            let bytes = rest.as_bytes();
            let mut i = 0;
            let mut found = None;
            while i + 5 <= bytes.len() {
                if bytes[i] == b' ' && bytes[i+1..i+5].eq_ignore_ascii_case(b"FROM") && (i + 5 >= bytes.len() || bytes[i+5] == b' ') {
                    found = Some(i);
                    break;
                }
                i += 1;
            }
            found?
        };
        let cols_str = rest[..from_pos].trim();
        let after_from = rest[from_pos + 5..].trim_start();

        // Table name
        let tbl_end = after_from.find(|c: char| !c.is_alphanumeric() && c != '_' && c != '"')?;
        if tbl_end == 0 { return None; }
        let table_name = after_from[..tbl_end].trim_matches('"');
        let after_table = after_from[tbl_end..].trim_start();

        // WHERE keyword
        if after_table.len() < 6 || !after_table[..5].eq_ignore_ascii_case("WHERE") {
            return None;
        }
        let where_part = after_table[5..].trim_start();

        // Reject complex WHERE (AND, OR, subqueries, etc. beyond simple equality)
        fn contains_ci(hay: &str, needle: &str) -> bool {
            hay.as_bytes().windows(needle.len()).any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
        }
        if contains_ci(where_part, " AND ") || contains_ci(where_part, " OR ") || where_part.contains('(') {
            return None;
        }

        // Parse: col = value
        let eq_pos = where_part.find('=')?;
        if eq_pos > 0 && matches!(where_part.as_bytes()[eq_pos - 1], b'!' | b'>' | b'<') {
            return None;
        }
        let where_col = where_part[..eq_pos].trim().trim_matches('"');
        let where_val_str = where_part[eq_pos + 1..].trim();

        // Look up schema (use cache)
        let schema = {
            let cached = self.schema_cache.lock();
            if let Some((ref name, ref s)) = *cached {
                if name == table_name { Some(s.clone()) } else { None }
            } else { None }
        }.or_else(|| {
            let s = self.storage.get_table_schema(table_name)?;
            *self.schema_cache.lock() = Some((table_name.to_owned(), s.clone()));
            Some(s)
        })?;

        // WHERE column must be the (single) PK
        if schema.primary_key_columns.len() != 1 { return None; }
        let pk_col_idx = schema.primary_key_columns[0];
        let where_col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(where_col))?;
        if where_col_idx != pk_col_idx { return None; }

        // Parse PK value
        let pk_val = self.parse_fast_literal(where_val_str, &schema.columns[pk_col_idx].data_type)?;
        let pk = falcon_storage::memtable::encode_pk_from_datums(&[&pk_val]);

        // Parse select columns
        let sel_col_names: Vec<&str> = cols_str.split(',').map(|s| s.trim().trim_matches('"')).collect();
        let sel_col_indices: Vec<usize> = sel_col_names.iter().map(|name| {
            schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
        }).collect::<Option<Vec<_>>>()?;

        // Ensure txn exists
        let auto_txn = if session.txn.is_none() {
            let txn = self.txn_mgr.begin_autocommit(session.default_isolation);
            session.txn = Some(txn);
            true
        } else {
            false
        };

        let txn = session.txn.as_ref().unwrap();
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let result = self.storage.get(schema.id, &pk, txn.txn_id, read_ts);

        let mut messages = Vec::new();

        // Build RowDescription
        let fields: Vec<FieldDescription> = sel_col_indices.iter().map(|&ci| {
            let col = &schema.columns[ci];
            FieldDescription {
                name: col.name.clone(),
                table_oid: 0,
                column_attr: 0,
                type_oid: col.data_type.pg_oid(),
                type_len: col.data_type.type_len(),
                type_modifier: -1,
                format_code: 0,
            }
        }).collect();
        messages.push(BackendMessage::RowDescription { fields });

        match result {
            Ok(Some(row)) => {
                let values: Vec<Option<String>> = sel_col_indices.iter()
                    .map(|&ci| row.values.get(ci).map(|d| d.to_pg_text()).unwrap_or(None))
                    .collect();
                messages.push(BackendMessage::DataRow { values });
                messages.push(BackendMessage::CommandComplete { tag: "SELECT 1".into() });
            }
            Ok(None) => {
                messages.push(BackendMessage::CommandComplete { tag: "SELECT 0".into() });
            }
            Err(e) => {
                if auto_txn {
                    if let Some(ref txn) = session.txn {
                        self.txn_mgr.abort_autocommit(txn.txn_id);
                    }
                    session.txn = None;
                }
                return Some(vec![self.error_response(&FalconError::Storage(e))]);
            }
        }

        if auto_txn {
            self.txn_mgr.commit_autocommit_readonly();
            session.txn = None;
        }

        Some(messages)
    }

    /// Direct UPDATE bypass for simple point updates on constraint-free tables.
    /// Parses SQL, reads row, applies SET, writes back — skips executor entirely.
    pub(crate) fn try_fast_direct_update(&self, sql: &str, session: &mut PgSession) -> Option<Vec<BackendMessage>> {
        let trimmed = sql.trim().trim_end_matches(';').trim_end();
        if trimmed.len() < 20 || !trimmed[..6].eq_ignore_ascii_case("UPDATE") {
            return None;
        }
        let rest = trimmed[6..].trim_start();

        // Table name
        let tbl_end = rest.find(|c: char| !c.is_alphanumeric() && c != '_' && c != '"')?;
        if tbl_end == 0 { return None; }
        let table_name = rest[..tbl_end].trim_matches('"');
        let rest = rest[tbl_end..].trim_start();

        // SET keyword
        if rest.len() < 4 || !rest[..3].eq_ignore_ascii_case("SET") { return None; }
        let rest = rest[3..].trim_start();

        // Reject RETURNING / FROM
        fn has_ci(hay: &str, needle: &str) -> bool {
            hay.as_bytes().windows(needle.len()).any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
        }
        if has_ci(rest, "RETURNING") || has_ci(rest, " FROM ") { return None; }

        // Find WHERE
        let where_pos = {
            let bytes = rest.as_bytes();
            let mut found = None;
            for i in 0..bytes.len().saturating_sub(6) {
                if bytes[i] == b' ' && bytes[i+1..i+6].eq_ignore_ascii_case(b"WHERE") && (i + 6 >= bytes.len() || bytes[i+6] == b' ') {
                    found = Some(i);
                    break;
                }
            }
            found?
        };
        let set_part = rest[..where_pos].trim();
        let where_part = rest[where_pos + 7..].trim();

        // Schema (cached)
        let schema = {
            let cached = self.schema_cache.lock();
            if let Some((ref name, ref s)) = *cached {
                if name == table_name { Some(s.clone()) } else { None }
            } else { None }
        }.or_else(|| {
            let s = self.storage.get_table_schema(table_name)?;
            *self.schema_cache.lock() = Some((table_name.to_owned(), s.clone()));
            Some(s)
        })?;

        // Eligibility: no constraints, single PK, no RETURNING
        if !schema.unique_constraints.is_empty()
            || !schema.check_constraints.is_empty()
            || !schema.foreign_keys.is_empty()
            || schema.primary_key_columns.len() != 1
        {
            return None;
        }
        let pk_col_idx = schema.primary_key_columns[0];

        // Parse WHERE: only pk = literal (no AND)
        if has_ci(where_part, " AND ") || has_ci(where_part, " OR ") { return None; }
        let eq_pos = where_part.find('=')?;
        if eq_pos > 0 && matches!(where_part.as_bytes()[eq_pos - 1], b'!' | b'>' | b'<') { return None; }
        let w_col = where_part[..eq_pos].trim().trim_matches('"');
        let w_val_str = where_part[eq_pos + 1..].trim();
        let w_col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(w_col))?;
        if w_col_idx != pk_col_idx { return None; }
        let pk_val = self.parse_fast_literal(w_val_str, &schema.columns[pk_col_idx].data_type)?;
        let pk = falcon_storage::memtable::encode_pk_from_datums(&[&pk_val]);

        // Parse SET assignments: col = expr [, col = expr]
        // For direct execution, we store (col_idx, SetOp) instead of BoundExpr
        enum SetOp { Literal(Datum), Plus(usize, Datum), Minus(usize, Datum) }
        let mut ops: Vec<(usize, SetOp)> = Vec::new();
        for assign_str in set_part.split(',') {
            let assign_str = assign_str.trim();
            let aeq = assign_str.find('=')?;
            let col_name = assign_str[..aeq].trim().trim_matches('"');
            let expr_str = assign_str[aeq + 1..].trim();
            let col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))?;

            if let Some(plus_pos) = expr_str.find('+') {
                let lhs = expr_str[..plus_pos].trim();
                let rhs = expr_str[plus_pos + 1..].trim();
                if schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(lhs))? == col_idx {
                    let val = self.parse_fast_literal(rhs, &schema.columns[col_idx].data_type)?;
                    ops.push((col_idx, SetOp::Plus(col_idx, val)));
                } else { return None; }
            } else if let Some(minus_pos) = expr_str.rfind('-') {
                if minus_pos > 0 {
                    let lhs = expr_str[..minus_pos].trim();
                    let rhs = expr_str[minus_pos + 1..].trim();
                    if schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(lhs))? == col_idx {
                        let val = self.parse_fast_literal(rhs, &schema.columns[col_idx].data_type)?;
                        ops.push((col_idx, SetOp::Minus(col_idx, val)));
                    } else { return None; }
                } else { return None; }
            } else {
                let val = self.parse_fast_literal(expr_str, &schema.columns[col_idx].data_type)?;
                ops.push((col_idx, SetOp::Literal(val)));
            }
        }

        // Ensure txn
        let auto_txn = if session.txn.is_none() {
            let txn = self.txn_mgr.begin_autocommit(session.default_isolation);
            session.txn = Some(txn);
            true
        } else {
            false
        };

        let txn = session.txn.as_ref().unwrap();
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());

        // Read current row
        let row = match self.storage.get(schema.id, &pk, txn.txn_id, read_ts) {
            Ok(Some(r)) => r,
            Ok(None) => {
                let tag = "UPDATE 0".to_owned();
                if auto_txn {
                    let _ = self.txn_mgr.commit_autocommit(txn.txn_id);
                    session.txn = None;
                }
                return Some(vec![BackendMessage::CommandComplete { tag }]);
            }
            Err(e) => {
                if auto_txn {
                    self.txn_mgr.abort_autocommit(txn.txn_id);
                    session.txn = None;
                }
                return Some(vec![self.error_response(&FalconError::Storage(e))]);
            }
        };

        // Apply assignments
        let mut new_values = row.values.clone();
        for (col_idx, op) in &ops {
            new_values[*col_idx] = match op {
                SetOp::Literal(v) => v.clone(),
                SetOp::Plus(src, v) => datum_add(&row.values[*src], v)?,
                SetOp::Minus(src, v) => datum_sub(&row.values[*src], v)?,
            };
        }

        let new_row = falcon_common::datum::OwnedRow::new(new_values);
        match self.storage.update(schema.id, &pk, new_row, txn.txn_id) {
            Ok(()) => {}
            Err(e) => {
                if auto_txn {
                    self.txn_mgr.abort_autocommit(txn.txn_id);
                    session.txn = None;
                }
                return Some(vec![self.error_response(&FalconError::Storage(e))]);
            }
        }

        if auto_txn {
            let _ = self.txn_mgr.commit_autocommit(txn.txn_id);
            session.txn = None;
        }

        Some(vec![BackendMessage::CommandComplete { tag: "UPDATE 1".to_owned() }])
    }
}
