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

    let body_stmts = parse_statements(rest.trim())?;
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
        } else if upper.starts_with("FOR ") {
            let (for_stmt, remaining) = parse_for_stmt(rest)?;
            stmts.push(for_stmt);
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
        } else if upper.starts_with("PERFORM ") {
            let after = &rest[8..];
            let end = find_stmt_end(after);
            stmts.push(PlStmt::Perform {
                sql: after[..end].trim().to_owned(),
            });
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

fn parse_for_stmt(input: &str) -> Result<(PlStmt, &str), ExecutionError> {
    let rest = &input[4..]; // skip "FOR "
    let in_pos = find_keyword_ci(rest, " IN ")
        .ok_or_else(|| ExecutionError::TypeError("PL/pgSQL: FOR without IN".into()))?;
    let var = rest[..in_pos].trim().to_lowercase();
    let after_in = &rest[in_pos + 4..];
    let loop_pos = find_keyword_ci(after_in, "LOOP")
        .ok_or_else(|| ExecutionError::TypeError("PL/pgSQL: FOR without LOOP".into()))?;
    let range_expr = after_in[..loop_pos].trim();
    let (start_sql, end_sql) = if let Some(dot_pos) = range_expr.find("..") {
        (
            range_expr[..dot_pos].trim().to_owned(),
            range_expr[dot_pos + 2..].trim().to_owned(),
        )
    } else {
        (range_expr.to_owned(), range_expr.to_owned())
    };
    let after_loop = &after_in[loop_pos + 4..];
    let end_loop = find_keyword_ci(after_loop, "END LOOP")
        .ok_or_else(|| ExecutionError::TypeError("PL/pgSQL: FOR LOOP without END LOOP".into()))?;
    let body = parse_statements(after_loop[..end_loop].trim())?;
    let remaining = skip_past_semicolon(&after_loop[end_loop + 8..]);
    Ok((
        PlStmt::ForLoop {
            var,
            start_sql,
            end_sql,
            body,
        },
        remaining,
    ))
}

// ── Helpers ──────────────────────────────────────────────────────────

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

/// Result of executing a PL/pgSQL block.
pub enum PlResult {
    /// RETURN value
    Value(Datum),
    /// No explicit RETURN (procedure)
    Void,
}

/// Execute a PL/pgSQL function with the given arguments.
/// `eval_sql` is a callback that evaluates a SQL expression string in the current
/// context and returns a Datum.
pub fn execute_plpgsql<F>(
    func: &FunctionDef,
    args: &[Datum],
    mut eval_sql: F,
) -> Result<Datum, ExecutionError>
where
    F: FnMut(&str, &HashMap<String, Datum>) -> Result<Datum, ExecutionError>,
{
    let block = parse_plpgsql(&func.body)?;
    let mut vars: HashMap<String, Datum> = HashMap::new();

    // Initialize declared variables to NULL
    for (name, _type_sql) in &block.declarations {
        vars.insert(name.clone(), Datum::Null);
    }

    // Bind parameters
    for (i, param) in func.params.iter().enumerate() {
        let val = args.get(i).cloned().unwrap_or(Datum::Null);
        if let Some(ref name) = param.name {
            vars.insert(name.to_lowercase(), val.clone());
        }
        // Also bind positional: $1, $2, ...
        vars.insert(format!("${}", i + 1), val);
    }

    match execute_block(&block.body, &mut vars, &mut eval_sql)? {
        PlResult::Value(v) => Ok(v),
        PlResult::Void => Ok(Datum::Null),
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

fn execute_block<F>(
    stmts: &[PlStmt],
    vars: &mut HashMap<String, Datum>,
    eval_sql: &mut F,
) -> Result<PlResult, ExecutionError>
where
    F: FnMut(&str, &HashMap<String, Datum>) -> Result<Datum, ExecutionError>,
{
    for stmt in stmts {
        match stmt {
            PlStmt::Assign { var, expr_sql } => {
                let val = eval_sql(expr_sql, vars)?;
                vars.insert(var.clone(), val);
            }
            PlStmt::Return { expr_sql } => {
                if expr_sql.is_empty() {
                    return Ok(PlResult::Value(Datum::Null));
                }
                let val = eval_sql(expr_sql, vars)?;
                return Ok(PlResult::Value(val));
            }
            PlStmt::ReturnQuery { sql } => {
                let val = eval_sql(sql, vars)?;
                return Ok(PlResult::Value(val));
            }
            PlStmt::If {
                condition,
                then_body,
                elsif_branches,
                else_body,
            } => {
                let cond_val = eval_sql(condition, vars)?;
                if datum_is_truthy(&cond_val) {
                    let result = execute_block(then_body, vars, eval_sql)?;
                    if let PlResult::Value(_) = &result {
                        return Ok(result);
                    }
                } else {
                    let mut handled = false;
                    for (elsif_cond, elsif_body) in elsif_branches {
                        let elsif_val = eval_sql(elsif_cond, vars)?;
                        if datum_is_truthy(&elsif_val) {
                            let result = execute_block(elsif_body, vars, eval_sql)?;
                            if let PlResult::Value(_) = &result {
                                return Ok(result);
                            }
                            handled = true;
                            break;
                        }
                    }
                    if !handled && !else_body.is_empty() {
                        let result = execute_block(else_body, vars, eval_sql)?;
                        if let PlResult::Value(_) = &result {
                            return Ok(result);
                        }
                    }
                }
            }
            PlStmt::WhileLoop { condition, body } => {
                let mut iterations = 0u64;
                loop {
                    let cond_val = eval_sql(condition, vars)?;
                    if !datum_is_truthy(&cond_val) {
                        break;
                    }
                    let result = execute_block(body, vars, eval_sql)?;
                    if let PlResult::Value(_) = &result {
                        return Ok(result);
                    }
                    iterations += 1;
                    if iterations > 1_000_000 {
                        return Err(ExecutionError::TypeError(
                            "PL/pgSQL: WHILE loop exceeded 1M iterations".into(),
                        ));
                    }
                }
            }
            PlStmt::ForLoop {
                var,
                start_sql,
                end_sql,
                body,
            } => {
                let start_val = eval_sql(start_sql, vars)?;
                let end_val = eval_sql(end_sql, vars)?;
                let start = datum_to_i64(&start_val)?;
                let end = datum_to_i64(&end_val)?;
                for i in start..=end {
                    vars.insert(var.clone(), Datum::Int64(i));
                    let result = execute_block(body, vars, eval_sql)?;
                    if let PlResult::Value(_) = &result {
                        return Ok(result);
                    }
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
        }
    }
    Ok(PlResult::Void)
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
        })
        .unwrap();
        assert_eq!(result, Datum::Text("positive".into()));
    }
}
