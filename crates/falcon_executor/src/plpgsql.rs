//! PL/pgSQL parser and interpreter for user-defined functions.

use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use falcon_common::schema::FunctionDef;
use std::collections::HashMap;

// ── AST ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum PlStmt {
    /// Variable assignment: var := expr_sql;
    Assign { var: String, expr_sql: String },
    /// RETURN expr_sql;
    Return { expr_sql: String },
    /// RETURN QUERY sql;
    ReturnQuery { sql: String },
    /// IF cond THEN stmts [ELSIF cond THEN stmts]* [ELSE stmts] END IF;
    If {
        condition: String,
        then_body: Vec<PlStmt>,
        elsif_branches: Vec<(String, Vec<PlStmt>)>,
        else_body: Vec<PlStmt>,
    },
    /// WHILE cond LOOP stmts END LOOP;
    WhileLoop {
        condition: String,
        body: Vec<PlStmt>,
    },
    /// FOR var IN start..end LOOP stmts END LOOP;
    ForLoop {
        var: String,
        start_sql: String,
        end_sql: String,
        body: Vec<PlStmt>,
    },
    /// RAISE NOTICE 'fmt', args...;
    Raise { level: String, message: String },
    /// PERFORM sql; (execute SQL, discard result)
    Perform { sql: String },
    /// Plain SQL execution (INSERT, UPDATE, DELETE, SELECT INTO)
    ExecuteSql { sql: String },
    /// FOR row IN SELECT ... LOOP ... END LOOP
    ForRowLoop {
        row_var: String,
        query_sql: String,
        body: Vec<PlStmt>,
    },
    /// EXIT [label] [WHEN cond];
    Exit { when_cond: Option<String> },
    /// CONTINUE [label] [WHEN cond];
    Continue { when_cond: Option<String> },
    /// BEGIN stmts EXCEPTION WHEN ... THEN stmts END
    ExceptionBlock {
        body: Vec<PlStmt>,
        handlers: Vec<ExceptionHandler>,
    },
}

#[derive(Debug, Clone)]
pub struct ExceptionHandler {
    /// Error condition names (e.g. "OTHERS", "UNIQUE_VIOLATION", "NOT_FOUND")
    pub conditions: Vec<String>,
    pub body: Vec<PlStmt>,
}

#[derive(Debug, Clone)]
pub struct PlBlock {
    pub declarations: Vec<(String, String)>, // (name, type_sql)
    pub body: Vec<PlStmt>,
}

// ── Parser ───────────────────────────────────────────────────────────

pub fn parse_plpgsql(body: &str) -> Result<PlBlock, ExecutionError> {
    let body = body.trim();
    // Strip optional outer $$ or $body$ delimiters
    let body = strip_dollar_quoting(body);
    let mut declarations = Vec::new();
    let mut rest = body;

    // Parse DECLARE section
    if let Some(pos) = find_keyword_ci(rest, "DECLARE") {
        rest = &rest[pos + 7..];
        if let Some(begin_pos) = find_keyword_ci(rest, "BEGIN") {
            let decl_section = rest[..begin_pos].trim();
            for line in decl_section.split(';') {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = line.splitn(2, char::is_whitespace).collect();
                if parts.len() == 2 {
                    declarations.push((parts[0].to_lowercase(), parts[1].trim().to_owned()));
                }
            }
            rest = &rest[begin_pos..];
        }
    }

    // Parse BEGIN...END block
    if let Some(begin_pos) = find_keyword_ci(rest, "BEGIN") {
        rest = &rest[begin_pos + 5..];
    }
    // Strip trailing END [label];
    if let Some(end_pos) = rfind_keyword_ci(rest, "END") {
        rest = &rest[..end_pos];
    }

    // Check for EXCEPTION section at depth-0 (not inside nested BEGIN/END)
    let body_stmts = if let Some(exc_pos) = find_exception_at_depth0(rest) {
        let main_body = parse_statements(rest[..exc_pos].trim())?;
        let handlers = parse_exception_handlers(&rest[exc_pos + 9..])?;
        vec![PlStmt::ExceptionBlock {
            body: main_body,
            handlers,
        }]
    } else {
        parse_statements(rest.trim())?
    };

    Ok(PlBlock {
        declarations,
        body: body_stmts,
    })
}

fn parse_statements(input: &str) -> Result<Vec<PlStmt>, ExecutionError> {
    let mut stmts = Vec::new();
    let mut rest = input.trim();

    while !rest.is_empty() {
        rest = rest.trim();
        if rest.is_empty() {
            break;
        }

        let upper = rest.to_uppercase();

        if upper.starts_with("RETURN QUERY ") {
            let after = &rest[13..];
            let end = find_stmt_end(after);
            stmts.push(PlStmt::ReturnQuery {
                sql: after[..end].trim().to_owned(),
            });
            rest = skip_past_semicolon(&after[end..]);
        } else if upper.starts_with("RETURN ") || upper.starts_with("RETURN;") {
            let after = &rest[6..].trim_start();
            let end = find_stmt_end(after);
            let expr = after[..end].trim().trim_end_matches(';').trim().to_owned();
            stmts.push(PlStmt::Return { expr_sql: expr });
            rest = skip_past_semicolon(&after[end..]);
        } else if upper.starts_with("IF ") {
            let (if_stmt, remaining) = parse_if_stmt(rest)?;
            stmts.push(if_stmt);
            rest = remaining;
        } else if upper.starts_with("WHILE ") {
            let (while_stmt, remaining) = parse_while_stmt(rest)?;
            stmts.push(while_stmt);
            rest = remaining;
        } else if upper.starts_with("RAISE ") {
            let after = &rest[6..];
            let end = find_stmt_end(after);
            let content = after[..end].trim().to_owned();
            let (level, msg) = content.split_once(' ').unwrap_or(("NOTICE", &content));
            stmts.push(PlStmt::Raise {
                level: level.to_uppercase(),
                message: msg.trim().trim_matches('\'').to_owned(),
            });
            rest = skip_past_semicolon(&after[end..]);
        } else if upper.starts_with("FOR ") {
            let (stmt, remaining) = parse_for_stmt_or_row(rest)?;
            stmts.push(stmt);
            rest = remaining;
        } else if upper.starts_with("PERFORM ") {
            let after = &rest[8..];
            let end = find_stmt_end(after);
            stmts.push(PlStmt::Perform {
                sql: after[..end].trim().to_owned(),
            });
            rest = skip_past_semicolon(&after[end..]);
        } else if upper.starts_with("EXIT") && (upper.starts_with("EXIT;") || upper.starts_with("EXIT ") || upper.as_bytes().get(4).map_or(true, |b| !b.is_ascii_alphanumeric())) {
            let after = &rest[4..];
            let end = find_stmt_end(after);
            let clause = after[..end].trim();
            let when_cond = parse_when_clause(clause);
            stmts.push(PlStmt::Exit { when_cond });
            rest = skip_past_semicolon(&after[end..]);
        } else if upper.starts_with("CONTINUE") && (upper.starts_with("CONTINUE;") || upper.starts_with("CONTINUE ") || upper.as_bytes().get(8).map_or(true, |b| !b.is_ascii_alphanumeric())) {
            let after = &rest[8..];
            let end = find_stmt_end(after);
            let clause = after[..end].trim();
            let when_cond = parse_when_clause(clause);
            stmts.push(PlStmt::Continue { when_cond });
            rest = skip_past_semicolon(&after[end..]);
        } else {
            // Check for assignment (var := expr;) or plain SQL
            if let Some(assign_pos) = rest.find(":=") {
                let semi = find_stmt_end(rest);
                if assign_pos < semi {
                    let var = rest[..assign_pos].trim().to_lowercase();
                    let expr = rest[assign_pos + 2..semi].trim().to_owned();
                    stmts.push(PlStmt::Assign {
                        var,
                        expr_sql: expr,
                    });
                    rest = skip_past_semicolon(&rest[semi..]);
                    continue;
                }
            }
            let end = find_stmt_end(rest);
            if end > 0 {
                let sql = rest[..end].trim().to_owned();
                if !sql.is_empty() {
                    stmts.push(PlStmt::ExecuteSql { sql });
                }
            }
            rest = skip_past_semicolon(&rest[end..]);
        }
    }
    Ok(stmts)
}

fn parse_if_stmt(input: &str) -> Result<(PlStmt, &str), ExecutionError> {
    // IF condition THEN body [ELSIF condition THEN body]* [ELSE body] END IF;
    let rest = &input[3..]; // skip "IF "
    let then_pos = find_keyword_ci(rest, "THEN")
        .ok_or_else(|| ExecutionError::TypeError("PL/pgSQL: IF without THEN".into()))?;
    let condition = rest[..then_pos].trim().to_owned();
    let after_then = &rest[then_pos + 4..];

    // Find matching END IF, accounting for nested IFs
    let (then_body_str, elsif_branches, else_body_str, remaining) = parse_if_body(after_then)?;

    let then_body = parse_statements(then_body_str.trim())?;
    let mut elsif = Vec::new();
    for (cond, body_str) in elsif_branches {
        elsif.push((cond, parse_statements(body_str.trim())?));
    }
    let else_body = if !else_body_str.is_empty() {
        parse_statements(else_body_str.trim())?
    } else {
        Vec::new()
    };

    Ok((
        PlStmt::If {
            condition,
            then_body,
            elsif_branches: elsif,
            else_body,
        },
        remaining,
    ))
}

#[allow(clippy::type_complexity)]
fn parse_if_body(
    input: &str,
) -> Result<(String, Vec<(String, String)>, String, &str), ExecutionError> {
    let mut depth = 1i32;
    let upper = input.to_uppercase();
    let bytes = upper.as_bytes();
    let mut i = 0;
    let mut then_body_end = None;
    let mut elsif_branches = Vec::new();
    let mut else_start = None;
    let mut current_start = 0;
    let mut current_cond: Option<String> = None;

    while i < bytes.len() {
        if i + 6 <= bytes.len() && &upper[i..i + 6] == "END IF" && is_word_boundary(bytes, i, 6) {
            depth -= 1;
            if depth == 0 {
                let body_part = &input[current_start..i];
                if let Some(cond) = current_cond.take() {
                    elsif_branches.push((cond, body_part.to_owned()));
                } else if else_start.is_some() {
                    let remaining = skip_past_semicolon(&input[i + 6..]);
                    let then_end = then_body_end.unwrap_or(i);
                    let then_body = input[0..then_end].to_owned();
                    return Ok((then_body, elsif_branches, body_part.to_owned(), remaining));
                } else {
                    let remaining = skip_past_semicolon(&input[i + 6..]);
                    return Ok((
                        body_part.to_owned(),
                        elsif_branches,
                        String::new(),
                        remaining,
                    ));
                }
            }
        }
        if i + 2 <= bytes.len()
            && &upper[i..i + 2] == "IF"
            && is_word_boundary(bytes, i, 2)
            && (i == 0 || !upper[..i].trim_end().ends_with("END"))
        {
            depth += 1;
        }
        if depth == 1 {
            if i + 5 <= bytes.len() && &upper[i..i + 5] == "ELSIF" && is_word_boundary(bytes, i, 5)
            {
                let body_part = &input[current_start..i];
                if let Some(cond) = current_cond.take() {
                    elsif_branches.push((cond, body_part.to_owned()));
                } else {
                    then_body_end = Some(i);
                }
                let after_elsif = &input[i + 5..];
                let then_pos = find_keyword_ci(after_elsif, "THEN").ok_or_else(|| {
                    ExecutionError::TypeError("PL/pgSQL: ELSIF without THEN".into())
                })?;
                current_cond = Some(after_elsif[..then_pos].trim().to_owned());
                i = i + 5 + then_pos + 4;
                current_start = i;
                continue;
            }
            if i + 4 <= bytes.len() && &upper[i..i + 4] == "ELSE" && is_word_boundary(bytes, i, 4) {
                let body_part = &input[current_start..i];
                if let Some(cond) = current_cond.take() {
                    elsif_branches.push((cond, body_part.to_owned()));
                } else {
                    then_body_end = Some(i);
                }
                else_start = Some(i + 4);
                current_start = i + 4;
                current_cond = None;
                i += 4;
                continue;
            }
        }
        i += 1;
    }
    Err(ExecutionError::TypeError(
        "PL/pgSQL: IF without matching END IF".into(),
    ))
}

fn parse_while_stmt(input: &str) -> Result<(PlStmt, &str), ExecutionError> {
    let rest = &input[6..]; // skip "WHILE "
    let loop_pos = find_keyword_ci(rest, "LOOP")
        .ok_or_else(|| ExecutionError::TypeError("PL/pgSQL: WHILE without LOOP".into()))?;
    let condition = rest[..loop_pos].trim().to_owned();
    let after_loop = &rest[loop_pos + 4..];
    let end_loop = find_keyword_ci(after_loop, "END LOOP")
        .ok_or_else(|| ExecutionError::TypeError("PL/pgSQL: LOOP without END LOOP".into()))?;
    let body = parse_statements(after_loop[..end_loop].trim())?;
    let remaining = skip_past_semicolon(&after_loop[end_loop + 8..]);
    Ok((PlStmt::WhileLoop { condition, body }, remaining))
}

fn has_keyword_before_loop(input: &str, keyword: &str) -> bool {
    let upper = input.to_uppercase();
    let kw_upper = keyword.to_uppercase();
    if let Some(kw_pos) = upper.find(&kw_upper) {
        if let Some(loop_pos) = find_keyword_ci(input, "LOOP") {
            return kw_pos < loop_pos;
        }
    }
    false
}

fn parse_when_clause(clause: &str) -> Option<String> {
    let upper = clause.to_uppercase();
    if let Some(pos) = upper.find("WHEN ") {
        Some(clause[pos + 5..].trim().to_owned())
    } else {
        None
    }
}

fn parse_for_stmt_or_row(input: &str) -> Result<(PlStmt, &str), ExecutionError> {
    let rest = &input[4..]; // skip "FOR "
    // Find " IN " by looking for the keyword "IN" with word boundaries
    let in_pos = find_kw_with_spaces(rest, "IN")
        .ok_or_else(|| ExecutionError::TypeError("PL/pgSQL: FOR without IN".into()))?;
    let var = rest[..in_pos].trim().to_lowercase();
    // skip past ' IN '
    let after_in_raw = &rest[in_pos + 2..]; // skip 'IN'
    let after_in = after_in_raw.trim_start();
    let loop_pos = find_keyword_ci(after_in, "LOOP")
        .ok_or_else(|| ExecutionError::TypeError("PL/pgSQL: FOR without LOOP".into()))?;
    let range_expr = after_in[..loop_pos].trim();

    let after_loop = &after_in[loop_pos + 4..];
    let end_loop_pos = find_keyword_ci(after_loop, "END LOOP")
        .ok_or_else(|| ExecutionError::TypeError("PL/pgSQL: FOR LOOP without END LOOP".into()))?;
    let body = parse_statements(after_loop[..end_loop_pos].trim())?;
    let remaining = skip_past_semicolon(&after_loop[end_loop_pos + 8..]);

    // Detect FOR row IN SELECT ... (row query loop) vs FOR i IN 1..10 (integer range)
    let upper_range = range_expr.to_uppercase();
    if upper_range.starts_with("SELECT ") || upper_range.starts_with("(") {
        Ok((
            PlStmt::ForRowLoop {
                row_var: var,
                query_sql: range_expr.to_owned(),
                body,
            },
            remaining,
        ))
    } else if let Some(dot_pos) = range_expr.find("..") {
        Ok((
            PlStmt::ForLoop {
                var,
                start_sql: range_expr[..dot_pos].trim().to_owned(),
                end_sql: range_expr[dot_pos + 2..].trim().to_owned(),
                body,
            },
            remaining,
        ))
    } else {
        // Treat as query (expression that returns a set)
        Ok((
            PlStmt::ForRowLoop {
                row_var: var,
                query_sql: range_expr.to_owned(),
                body,
            },
            remaining,
        ))
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Find position of `EXCEPTION` keyword at nesting depth 0 (not inside BEGIN/END).
fn find_exception_at_depth0(input: &str) -> Option<usize> {
    let upper = input.to_uppercase();
    let bytes = upper.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    while i < bytes.len() {
        if i + 5 <= bytes.len() && &upper[i..i + 5] == "BEGIN" && is_word_boundary(bytes, i, 5) {
            depth += 1;
            i += 5;
            continue;
        }
        if i + 3 <= bytes.len() && &upper[i..i + 3] == "END" && is_word_boundary(bytes, i, 3) {
            depth -= 1;
            i += 3;
            continue;
        }
        if depth == 0
            && i + 9 <= bytes.len()
            && &upper[i..i + 9] == "EXCEPTION"
            && is_word_boundary(bytes, i, 9)
        {
            // Skip RAISE EXCEPTION — not the handler section
            let before = upper[..i].trim_end();
            if !before.ends_with("RAISE") {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Parse `WHEN cond [OR cond] THEN stmts` handler list after the EXCEPTION keyword.
fn parse_exception_handlers(input: &str) -> Result<Vec<ExceptionHandler>, ExecutionError> {
    let mut handlers = Vec::new();
    let mut rest = input.trim();
    let upper_full = rest.to_uppercase();

    // Split on WHEN at depth 0
    let mut i = 0;
    let upper = upper_full.as_bytes();
    let mut segments: Vec<&str> = Vec::new();
    let mut seg_start = 0;

    while i < rest.len() {
        let up = &upper_full[i..];
        if up.starts_with("WHEN") && is_word_boundary(upper, i, 4) && i > 0 {
            segments.push(rest[seg_start..i].trim());
            seg_start = i;
        }
        i += 1;
    }
    segments.push(rest[seg_start..].trim());

    for seg in segments {
        if seg.is_empty() {
            continue;
        }
        let seg_upper = seg.to_uppercase();
        // strip leading WHEN
        let seg = if seg_upper.starts_with("WHEN ") { &seg[5..] } else { seg };
        // find THEN keyword with word boundaries
        let then_pos = find_kw_with_spaces(seg, "THEN")
            .ok_or_else(|| ExecutionError::TypeError("EXCEPTION handler: WHEN without THEN".into()))?;
        let cond_str = seg[..then_pos].trim();
        let body_str = seg[then_pos + 4..].trim(); // skip "THEN"
        let conditions: Vec<String> = cond_str
            .split(" OR ")
            .map(|c| c.trim().to_uppercase())
            .collect();
        let body = parse_statements(body_str)?;
        handlers.push(ExceptionHandler { conditions, body });
    }
    Ok(handlers)
}

fn strip_dollar_quoting(s: &str) -> &str {
    let s = s.trim();
    if s.starts_with("$$") && s.ends_with("$$") && s.len() > 4 {
        return &s[2..s.len() - 2];
    }
    // $tag$...$tag$
    if let Some(rest) = s.strip_prefix('$') {
        if let Some(end) = rest.find('$') {
            let tag = &s[..end + 2];
            if s.ends_with(tag) && s.len() > tag.len() * 2 {
                return &s[tag.len()..s.len() - tag.len()];
            }
        }
    }
    s
}

/// Find `keyword` (e.g. "IN") surrounded by word boundaries; returns the position of the
/// first space *before* the keyword (i.e. the byte offset of the space in `... kw ...`).
fn find_kw_with_spaces(input: &str, keyword: &str) -> Option<usize> {
    let upper = input.to_uppercase();
    let kw = keyword.to_uppercase();
    let kw_len = kw.len();
    let bytes = upper.as_bytes();
    let mut search_from = 0;
    while search_from < upper.len() {
        if let Some(rel) = upper[search_from..].find(&kw) {
            let abs = search_from + rel;
            if is_word_boundary(bytes, abs, kw_len) {
                // Return position just before the keyword (trim will handle spaces)
                return Some(abs);
            }
            search_from = abs + 1;
        } else {
            break;
        }
    }
    None
}

fn find_keyword_ci(input: &str, keyword: &str) -> Option<usize> {
    let upper = input.to_uppercase();
    let kw = keyword.to_uppercase();
    let bytes = upper.as_bytes();
    let kw_len = kw.len();
    upper.find(&kw).and_then(|pos| {
        if is_word_boundary(bytes, pos, kw_len) {
            Some(pos)
        } else {
            // Search further
            let mut search_from = pos + 1;
            while search_from < upper.len() {
                if let Some(next) = upper[search_from..].find(&kw) {
                    let abs = search_from + next;
                    if is_word_boundary(bytes, abs, kw_len) {
                        return Some(abs);
                    }
                    search_from = abs + 1;
                } else {
                    break;
                }
            }
            None
        }
    })
}

fn rfind_keyword_ci(input: &str, keyword: &str) -> Option<usize> {
    let upper = input.to_uppercase();
    let kw = keyword.to_uppercase();
    let bytes = upper.as_bytes();
    let kw_len = kw.len();
    upper.rfind(&kw).and_then(|pos| {
        if is_word_boundary(bytes, pos, kw_len) {
            Some(pos)
        } else {
            None
        }
    })
}

fn is_word_boundary(bytes: &[u8], pos: usize, len: usize) -> bool {
    let before_ok = pos == 0 || !bytes[pos - 1].is_ascii_alphanumeric();
    let after_ok = pos + len >= bytes.len() || !bytes[pos + len].is_ascii_alphanumeric();
    before_ok && after_ok
}

fn find_stmt_end(input: &str) -> usize {
    // Find the next `;` not inside a string literal
    let mut in_string = false;
    let bytes = input.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\'' && !in_string {
            in_string = true;
        } else if b == b'\'' && in_string {
            // Check for escaped quote
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                continue;
            }
            in_string = false;
        } else if b == b';' && !in_string {
            return i;
        }
    }
    input.len()
}

fn skip_past_semicolon(input: &str) -> &str {
    let s = input.trim_start();
    if let Some(rest) = s.strip_prefix(';') {
        rest.trim_start()
    } else {
        s
    }
}

// ── Interpreter ──────────────────────────────────────────────────────

/// Try to resolve an expression: if it's a bare variable name in `vars`, return it directly;
/// otherwise delegate to `eval_sql`.  This avoids requiring the external eval callback to
/// implement variable-lookup for simple `RETURN x` / `x := y` cases.
fn resolve_expr<F>(
    expr: &str,
    vars: &HashMap<String, Datum>,
    eval_sql: &mut F,
) -> Result<Datum, ExecutionError>
where
    F: FnMut(&str, &HashMap<String, Datum>) -> Result<Datum, ExecutionError>,
{
    let trimmed = expr.trim();
    // Single bare identifier → try vars first
    if trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
        && !trimmed.is_empty()
    {
        if let Some(val) = vars.get(&trimmed.to_lowercase()) {
            return Ok(val.clone());
        }
    }
    eval_sql(trimmed, vars)
}

/// Internal control-flow signal from execute_block.
enum BlockSignal {
    /// RETURN value — bubbles out of all nested blocks.
    Return(Datum),
    /// EXIT — exit the enclosing loop.
    Exit,
    /// CONTINUE — skip to next loop iteration.
    Continue,
    /// Normal fall-through.
    Done,
}

/// Execute a PL/pgSQL function with the given arguments.
///
/// - `eval_sql`  — evaluates an expression / DML, returns a single Datum.
/// - `eval_rows` — executes a SELECT and returns all rows as `Vec<HashMap<col, Datum>>`.
///                 Column names are lowercase. Used by `FOR row IN SELECT`.
pub fn execute_plpgsql<F, R>(
    func: &FunctionDef,
    args: &[Datum],
    mut eval_sql: F,
    mut eval_rows: R,
) -> Result<Datum, ExecutionError>
where
    F: FnMut(&str, &HashMap<String, Datum>) -> Result<Datum, ExecutionError>,
    R: FnMut(&str, &HashMap<String, Datum>) -> Result<Vec<HashMap<String, Datum>>, ExecutionError>,
{
    execute_plpgsql_with_vars(func, args, &HashMap::new(), eval_sql, eval_rows)
}

pub fn execute_plpgsql_with_vars<F, R>(
    func: &FunctionDef,
    args: &[Datum],
    extra_vars: &HashMap<String, Datum>,
    mut eval_sql: F,
    mut eval_rows: R,
) -> Result<Datum, ExecutionError>
where
    F: FnMut(&str, &HashMap<String, Datum>) -> Result<Datum, ExecutionError>,
    R: FnMut(&str, &HashMap<String, Datum>) -> Result<Vec<HashMap<String, Datum>>, ExecutionError>,
{
    let block = parse_plpgsql(&func.body)?;
    let mut vars: HashMap<String, Datum> = HashMap::new();

    for (name, _type_sql) in &block.declarations {
        vars.insert(name.clone(), Datum::Null);
    }

    for (k, v) in extra_vars {
        vars.insert(k.clone(), v.clone());
    }

    for (i, param) in func.params.iter().enumerate() {
        let val = args.get(i).cloned().unwrap_or(Datum::Null);
        if let Some(ref name) = param.name {
            vars.insert(name.to_lowercase(), val.clone());
        }
        vars.insert(format!("${}", i + 1), val);
    }

    match execute_block(&block.body, &mut vars, &mut eval_sql, &mut eval_rows)? {
        BlockSignal::Return(v) => Ok(v),
        _ => Ok(Datum::Null),
    }
}

/// Execute a SQL-language function (body is a single SQL expression/query).
pub fn execute_sql_function<F>(
    func: &FunctionDef,
    args: &[Datum],
    mut eval_sql: F,
) -> Result<Datum, ExecutionError>
where
    F: FnMut(&str, &HashMap<String, Datum>) -> Result<Datum, ExecutionError>,
{
    let mut vars: HashMap<String, Datum> = HashMap::new();
    for (i, param) in func.params.iter().enumerate() {
        let val = args.get(i).cloned().unwrap_or(Datum::Null);
        if let Some(ref name) = param.name {
            vars.insert(name.to_lowercase(), val.clone());
        }
        vars.insert(format!("${}", i + 1), val);
    }

    let body = func.body.trim();
    // Handle "RETURN expr" prefix (from CREATE FUNCTION ... RETURN expr syntax)
    let sql = if body.to_uppercase().starts_with("RETURN ") {
        &body[7..]
    } else {
        body
    };

    eval_sql(sql, &vars)
}

fn execute_block<F, R>(
    stmts: &[PlStmt],
    vars: &mut HashMap<String, Datum>,
    eval_sql: &mut F,
    eval_rows: &mut R,
) -> Result<BlockSignal, ExecutionError>
where
    F: FnMut(&str, &HashMap<String, Datum>) -> Result<Datum, ExecutionError>,
    R: FnMut(&str, &HashMap<String, Datum>) -> Result<Vec<HashMap<String, Datum>>, ExecutionError>,
{
    for stmt in stmts {
        match stmt {
            PlStmt::Assign { var, expr_sql } => {
                let val = resolve_expr(expr_sql, vars, eval_sql)?;
                vars.insert(var.clone(), val);
            }
            PlStmt::Return { expr_sql } => {
                if expr_sql.is_empty() {
                    return Ok(BlockSignal::Return(Datum::Null));
                }
                let val = resolve_expr(expr_sql, vars, eval_sql)?;
                return Ok(BlockSignal::Return(val));
            }
            PlStmt::ReturnQuery { sql } => {
                let val = eval_sql(sql, vars)?;
                return Ok(BlockSignal::Return(val));
            }
            PlStmt::If {
                condition,
                then_body,
                elsif_branches,
                else_body,
            } => {
                let cond_val = eval_sql(condition, vars)?;
                let sig = if datum_is_truthy(&cond_val) {
                    execute_block(then_body, vars, eval_sql, eval_rows)?
                } else {
                    let mut sig = BlockSignal::Done;
                    let mut handled = false;
                    for (elsif_cond, elsif_body) in elsif_branches {
                        let elsif_val = eval_sql(elsif_cond, vars)?;
                        if datum_is_truthy(&elsif_val) {
                            sig = execute_block(elsif_body, vars, eval_sql, eval_rows)?;
                            handled = true;
                            break;
                        }
                    }
                    if !handled && !else_body.is_empty() {
                        sig = execute_block(else_body, vars, eval_sql, eval_rows)?;
                    }
                    sig
                };
                match sig {
                    BlockSignal::Done => {}
                    other => return Ok(other),
                }
            }
            PlStmt::WhileLoop { condition, body } => {
                let mut iterations = 0u64;
                loop {
                    let cond_val = eval_sql(condition, vars)?;
                    if !datum_is_truthy(&cond_val) {
                        break;
                    }
                    match execute_block(body, vars, eval_sql, eval_rows)? {
                        BlockSignal::Return(v) => return Ok(BlockSignal::Return(v)),
                        BlockSignal::Exit => break,
                        BlockSignal::Continue | BlockSignal::Done => {}
                    }
                    iterations += 1;
                    if iterations > 1_000_000 {
                        return Err(ExecutionError::TypeError(
                            "PL/pgSQL: WHILE loop exceeded 1M iterations".into(),
                        ));
                    }
                }
            }
            PlStmt::ForLoop { var, start_sql, end_sql, body } => {
                let start_val = eval_sql(start_sql, vars)?;
                let end_val = eval_sql(end_sql, vars)?;
                let start = datum_to_i64(&start_val)?;
                let end = datum_to_i64(&end_val)?;
                'int_loop: for i in start..=end {
                    vars.insert(var.clone(), Datum::Int64(i));
                    match execute_block(body, vars, eval_sql, eval_rows)? {
                        BlockSignal::Return(v) => return Ok(BlockSignal::Return(v)),
                        BlockSignal::Exit => break 'int_loop,
                        BlockSignal::Continue | BlockSignal::Done => {}
                    }
                }
            }
            PlStmt::ForRowLoop { row_var, query_sql, body } => {
                let rows = eval_rows(query_sql, vars)?;
                'row_loop: for row_map in rows {
                    // Expose column values as row_var.col and also directly as col names
                    for (col, val) in &row_map {
                        vars.insert(format!("{}.{}", row_var, col), val.clone());
                        vars.insert(col.clone(), val.clone());
                    }
                    match execute_block(body, vars, eval_sql, eval_rows)? {
                        BlockSignal::Return(v) => return Ok(BlockSignal::Return(v)),
                        BlockSignal::Exit => break 'row_loop,
                        BlockSignal::Continue | BlockSignal::Done => {}
                    }
                }
            }
            PlStmt::Exit { when_cond } => {
                if let Some(cond) = when_cond {
                    let val = eval_sql(cond, vars)?;
                    if datum_is_truthy(&val) {
                        return Ok(BlockSignal::Exit);
                    }
                } else {
                    return Ok(BlockSignal::Exit);
                }
            }
            PlStmt::Continue { when_cond } => {
                if let Some(cond) = when_cond {
                    let val = eval_sql(cond, vars)?;
                    if datum_is_truthy(&val) {
                        return Ok(BlockSignal::Continue);
                    }
                } else {
                    return Ok(BlockSignal::Continue);
                }
            }
            PlStmt::Raise { level, message } => {
                let resolved = substitute_vars(message, vars);
                match level.as_str() {
                    "EXCEPTION" => return Err(ExecutionError::TypeError(resolved)),
                    _ => tracing::info!(target: "plpgsql", level = %level, "{}", resolved),
                }
            }
            PlStmt::Perform { sql } => {
                let _ = eval_sql(sql, vars)?;
            }
            PlStmt::ExecuteSql { sql } => {
                let _ = eval_sql(sql, vars)?;
            }
            PlStmt::ExceptionBlock { body, handlers } => {
                match execute_block(body, vars, eval_sql, eval_rows) {
                    Ok(sig) => match sig {
                        BlockSignal::Done => {}
                        other => return Ok(other),
                    },
                    Err(err) => {
                        let err_msg = err.to_string();
                        let mut handled = false;
                        for handler in handlers {
                            let matches = handler.conditions.iter().any(|c| {
                                let cu = c.to_uppercase();
                                cu == "OTHERS"
                                    || cu == "SQLEXCEPTION"
                                    || err_msg.to_uppercase().contains(&cu)
                            });
                            if matches {
                                // Bind SQLERRM for the handler body
                                vars.insert("sqlerrm".into(), Datum::Text(err_msg.clone()));
                                match execute_block(&handler.body, vars, eval_sql, eval_rows)? {
                                    BlockSignal::Done => {}
                                    other => return Ok(other),
                                }
                                handled = true;
                                break;
                            }
                        }
                        if !handled {
                            return Err(err);
                        }
                    }
                }
            }
        }
    }
    Ok(BlockSignal::Done)
}

fn datum_is_truthy(d: &Datum) -> bool {
    match d {
        Datum::Boolean(b) => *b,
        Datum::Null => false,
        Datum::Int32(i) => *i != 0,
        Datum::Int64(i) => *i != 0,
        Datum::Text(s) => !s.is_empty() && s != "f" && s != "false",
        _ => true,
    }
}

fn datum_to_i64(d: &Datum) -> Result<i64, ExecutionError> {
    match d {
        Datum::Int32(i) => Ok(*i as i64),
        Datum::Int64(i) => Ok(*i),
        Datum::Float64(f) => Ok(*f as i64),
        Datum::Text(s) => s
            .parse::<i64>()
            .map_err(|_| ExecutionError::TypeError(format!("cannot convert '{}' to integer", s))),
        other => Err(ExecutionError::TypeError(format!(
            "cannot convert {:?} to integer",
            other
        ))),
    }
}

fn substitute_vars(template: &str, vars: &HashMap<String, Datum>) -> String {
    let mut result = template.to_owned();
    for (k, v) in vars {
        let placeholder = format!("%{}", k);
        if result.contains(&placeholder) {
            result = result.replace(&placeholder, &format!("{}", v));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_return() {
        let body = "BEGIN RETURN 42; END";
        let block = parse_plpgsql(body).unwrap();
        assert!(block.declarations.is_empty());
        assert_eq!(block.body.len(), 1);
        match &block.body[0] {
            PlStmt::Return { expr_sql } => assert_eq!(expr_sql, "42"),
            other => panic!("expected Return, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_declare_and_assign() {
        let body = "DECLARE x INTEGER; BEGIN x := 10; RETURN x; END";
        let block = parse_plpgsql(body).unwrap();
        assert_eq!(block.declarations.len(), 1);
        assert_eq!(block.declarations[0].0, "x");
        assert_eq!(block.body.len(), 2);
    }

    #[test]
    fn test_parse_if() {
        let body = "BEGIN IF true THEN RETURN 1; ELSE RETURN 2; END IF; END";
        let block = parse_plpgsql(body).unwrap();
        assert_eq!(block.body.len(), 1);
        match &block.body[0] {
            PlStmt::If {
                condition,
                then_body,
                else_body,
                ..
            } => {
                assert_eq!(condition, "true");
                assert_eq!(then_body.len(), 1);
                assert_eq!(else_body.len(), 1);
            }
            other => panic!("expected If, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_dollar_quoted() {
        let body = "$$BEGIN RETURN 1; END$$";
        let block = parse_plpgsql(body).unwrap();
        assert_eq!(block.body.len(), 1);
    }

    #[test]
    fn test_execute_simple_sql_function() {
        let func = FunctionDef {
            name: "add".into(),
            params: vec![
                falcon_common::schema::FunctionParam {
                    name: Some("a".into()),
                    data_type: falcon_common::types::DataType::Int32,
                },
                falcon_common::schema::FunctionParam {
                    name: Some("b".into()),
                    data_type: falcon_common::types::DataType::Int32,
                },
            ],
            return_type: Some(falcon_common::types::DataType::Int32),
            language: falcon_common::schema::FunctionLanguage::Sql,
            body: "$1 + $2".into(),
            volatility: falcon_common::schema::FunctionVolatility::Immutable,
            is_strict: false,
            or_replace: false,
        };

        let result =
            execute_sql_function(&func, &[Datum::Int32(3), Datum::Int32(4)], |_sql, vars| {
                // Simple mock: evaluate "$1 + $2" by looking up vars
                let a = vars.get("$1").cloned().unwrap_or(Datum::Null);
                let b = vars.get("$2").cloned().unwrap_or(Datum::Null);
                match (a, b) {
                    (Datum::Int32(x), Datum::Int32(y)) => Ok(Datum::Int32(x + y)),
                    _ => Ok(Datum::Null),
                }
            })
            .unwrap();
        assert_eq!(result, Datum::Int32(7));
    }

    #[test]
    fn test_execute_plpgsql_if() {
        let func = FunctionDef {
            name: "test_if".into(),
            params: vec![falcon_common::schema::FunctionParam {
                name: Some("x".into()),
                data_type: falcon_common::types::DataType::Int32,
            }],
            return_type: Some(falcon_common::types::DataType::Text),
            language: falcon_common::schema::FunctionLanguage::PlPgSql,
            body: "BEGIN IF x > 0 THEN RETURN 'positive'; ELSE RETURN 'non-positive'; END IF; END"
                .into(),
            volatility: falcon_common::schema::FunctionVolatility::Immutable,
            is_strict: false,
            or_replace: false,
        };

        let noop_rows = |_sql: &str, _vars: &std::collections::HashMap<String, Datum>|
            -> Result<Vec<std::collections::HashMap<String, Datum>>, ExecutionError> {
            Ok(vec![])
        };
        let result = execute_plpgsql(&func, &[Datum::Int32(5)], |sql, vars| {
            let sql_lower = sql.to_lowercase();
            if sql_lower.contains("> 0") {
                let x = vars.get("x").cloned().unwrap_or(Datum::Null);
                match x {
                    Datum::Int32(v) => Ok(Datum::Boolean(v > 0)),
                    _ => Ok(Datum::Boolean(false)),
                }
            } else if sql == "'positive'" || sql == "positive" {
                Ok(Datum::Text("positive".into()))
            } else if sql == "'non-positive'" || sql == "non-positive" {
                Ok(Datum::Text("non-positive".into()))
            } else {
                Ok(Datum::Null)
            }
        }, noop_rows)
        .unwrap();
        assert_eq!(result, Datum::Text("positive".into()));
    }

    fn noop_rows_fn(
        _sql: &str,
        _vars: &std::collections::HashMap<String, Datum>,
    ) -> Result<Vec<std::collections::HashMap<String, Datum>>, ExecutionError> {
        Ok(vec![])
    }

    #[test]
    fn test_parse_for_row_loop() {
        let body = "BEGIN FOR r IN SELECT id FROM t LOOP RAISE NOTICE 'row'; END LOOP; END";
        let block = parse_plpgsql(body).unwrap();
        match &block.body[0] {
            PlStmt::ForRowLoop { row_var, query_sql, .. } => {
                assert_eq!(row_var, "r");
                assert!(query_sql.to_uppercase().starts_with("SELECT"));
            }
            other => panic!("expected ForRowLoop, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_exit_continue() {
        let body = "BEGIN WHILE true LOOP EXIT WHEN true; CONTINUE WHEN false; END LOOP; END";
        let block = parse_plpgsql(body).unwrap();
        match &block.body[0] {
            PlStmt::WhileLoop { body, .. } => {
                assert!(matches!(&body[0], PlStmt::Exit { when_cond: Some(_) }));
                assert!(matches!(&body[1], PlStmt::Continue { when_cond: Some(_) }));
            }
            other => panic!("expected WhileLoop, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_exception_block() {
        let body = "BEGIN RAISE EXCEPTION 'fail'; EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'caught'; END";
        let block = parse_plpgsql(body).unwrap();
        match &block.body[0] {
            PlStmt::ExceptionBlock { body, handlers } => {
                assert_eq!(body.len(), 1);
                assert_eq!(handlers.len(), 1);
                assert_eq!(handlers[0].conditions[0], "OTHERS");
            }
            other => panic!("expected ExceptionBlock, got {:?}", other),
        }
    }

    #[test]
    fn test_exit_breaks_integer_for_loop() {
        let func = FunctionDef {
            name: "test_exit".into(),
            params: vec![],
            return_type: Some(falcon_common::types::DataType::Int32),
            language: falcon_common::schema::FunctionLanguage::PlPgSql,
            body: "DECLARE total INTEGER; BEGIN total := 0; FOR i IN 1..10 LOOP total := total + 1; EXIT WHEN i = 3; END LOOP; RETURN total; END".into(),
            volatility: falcon_common::schema::FunctionVolatility::Immutable,
            is_strict: false,
            or_replace: false,
        };
        let result = execute_plpgsql(&func, &[], |sql, vars| {
            if let Ok(n) = sql.trim().parse::<i64>() {
                return Ok(Datum::Int64(n));
            }
            if sql.contains("total + 1") {
                let t = vars.get("total").cloned().unwrap_or(Datum::Int64(0));
                return Ok(match t { Datum::Int64(n) => Datum::Int64(n + 1), _ => Datum::Int64(1) });
            }
            if sql.contains("i = 3") {
                let i = vars.get("i").cloned().unwrap_or(Datum::Int64(0));
                return Ok(Datum::Boolean(matches!(i, Datum::Int64(3))));
            }
            Ok(Datum::Null)
        }, noop_rows_fn).unwrap();
        assert_eq!(result, Datum::Int64(3));
    }

    #[test]
    fn test_exception_handler_catches_error() {
        let func = FunctionDef {
            name: "test_exc".into(),
            params: vec![],
            return_type: Some(falcon_common::types::DataType::Text),
            language: falcon_common::schema::FunctionLanguage::PlPgSql,
            body: "BEGIN RAISE EXCEPTION 'boom'; EXCEPTION WHEN OTHERS THEN RETURN 'caught'; END".into(),
            volatility: falcon_common::schema::FunctionVolatility::Immutable,
            is_strict: false,
            or_replace: false,
        };
        let result = execute_plpgsql(&func, &[], |sql, _vars| {
            if sql == "'caught'" || sql == "caught" {
                Ok(Datum::Text("caught".into()))
            } else {
                Ok(Datum::Null)
            }
        }, noop_rows_fn).unwrap();
        assert_eq!(result, Datum::Text("caught".into()));
    }

    #[test]
    fn test_for_row_loop_iterates() {
        let func = FunctionDef {
            name: "test_for_row".into(),
            params: vec![],
            return_type: Some(falcon_common::types::DataType::Int32),
            language: falcon_common::schema::FunctionLanguage::PlPgSql,
            body: "DECLARE total INTEGER; BEGIN total := 0; FOR r IN SELECT id FROM t LOOP total := total + 1; END LOOP; RETURN total; END".into(),
            volatility: falcon_common::schema::FunctionVolatility::Immutable,
            is_strict: false,
            or_replace: false,
        };
        let mock_rows = |_sql: &str, _vars: &std::collections::HashMap<String, Datum>| {
            Ok(vec![
                [("id".to_string(), Datum::Int64(1))].into_iter().collect(),
                [("id".to_string(), Datum::Int64(2))].into_iter().collect(),
                [("id".to_string(), Datum::Int64(3))].into_iter().collect(),
            ])
        };
        let result = execute_plpgsql(&func, &[], |sql, vars| {
            if let Ok(n) = sql.trim().parse::<i64>() {
                return Ok(Datum::Int64(n));
            }
            if sql.contains("total + 1") {
                let t = vars.get("total").cloned().unwrap_or(Datum::Int64(0));
                return Ok(match t { Datum::Int64(n) => Datum::Int64(n + 1), _ => Datum::Int64(1) });
            }
            Ok(Datum::Null)
        }, mock_rows).unwrap();
        assert_eq!(result, Datum::Int64(3));
    }
}
