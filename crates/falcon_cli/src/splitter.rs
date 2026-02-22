/// Split a SQL string into individual statements, respecting:
/// - Single-quoted string literals (don't split on `;` inside)
/// - Double-quoted identifiers (don't split on `;` inside)
/// - `--` line comments
/// - `/* ... */` block comments
/// - Empty / whitespace-only statements are skipped
pub fn split_statements(input: &str) -> Vec<String> {
    let mut statements: Vec<String> = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let ch = chars[i];

        // -- line comment: consume until end of line
        if ch == '-' && i + 1 < len && chars[i + 1] == '-' {
            while i < len && chars[i] != '\n' {
                current.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // /* block comment */
        if ch == '/' && i + 1 < len && chars[i + 1] == '*' {
            current.push(chars[i]);
            current.push(chars[i + 1]);
            i += 2;
            while i < len {
                if chars[i] == '*' && i + 1 < len && chars[i + 1] == '/' {
                    current.push(chars[i]);
                    current.push(chars[i + 1]);
                    i += 2;
                    break;
                }
                current.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // single-quoted string literal
        if ch == '\'' {
            current.push(ch);
            i += 1;
            while i < len {
                let c = chars[i];
                current.push(c);
                i += 1;
                if c == '\'' {
                    // check for escaped quote ''
                    if i < len && chars[i] == '\'' {
                        current.push(chars[i]);
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }

        // double-quoted identifier
        if ch == '"' {
            current.push(ch);
            i += 1;
            while i < len {
                let c = chars[i];
                current.push(c);
                i += 1;
                if c == '"' {
                    // check for escaped quote ""
                    if i < len && chars[i] == '"' {
                        current.push(chars[i]);
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }

        // statement terminator
        if ch == ';' {
            let stmt = current.trim().to_string();
            if !stmt.is_empty() {
                statements.push(stmt);
            }
            current.clear();
            i += 1;
            continue;
        }

        current.push(ch);
        i += 1;
    }

    // trailing statement without terminator
    let stmt = current.trim().to_string();
    if !stmt.is_empty() {
        statements.push(stmt);
    }

    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_single_statement() {
        let stmts = split_statements("SELECT 1");
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn test_basic_terminated() {
        let stmts = split_statements("SELECT 1;");
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn test_multiple_statements() {
        let stmts = split_statements("SELECT 1; SELECT 2; SELECT 3");
        assert_eq!(stmts, vec!["SELECT 1", "SELECT 2", "SELECT 3"]);
    }

    #[test]
    fn test_multiple_statements_per_line() {
        let stmts = split_statements("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);");
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_semicolon_inside_string() {
        let stmts = split_statements("SELECT 'hello; world' AS v");
        assert_eq!(stmts, vec!["SELECT 'hello; world' AS v"]);
    }

    #[test]
    fn test_semicolon_inside_double_quoted_ident() {
        let stmts = split_statements(r#"SELECT "col;name" FROM t"#);
        assert_eq!(stmts, vec![r#"SELECT "col;name" FROM t"#]);
    }

    #[test]
    fn test_line_comment_with_semicolon() {
        let input = "SELECT 1 -- this; is a comment\n;";
        let stmts = split_statements(input);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("SELECT 1"));
    }

    #[test]
    fn test_block_comment_with_semicolon() {
        let stmts = split_statements("SELECT /* semi; colon */ 1;");
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("SELECT"));
    }

    #[test]
    fn test_empty_statements_skipped() {
        let stmts = split_statements(";;;   ;  ");
        assert!(stmts.is_empty(), "empty statements must be skipped");
    }

    #[test]
    fn test_multiline_sql() {
        let input = "SELECT\n  id,\n  name\nFROM\n  users\nWHERE id = 1;";
        let stmts = split_statements(input);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("users"));
    }

    #[test]
    fn test_escaped_single_quote_in_string() {
        let stmts = split_statements("SELECT 'it''s fine; really' AS v;");
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("it''s fine; really"));
    }

    #[test]
    fn test_whitespace_only_input() {
        let stmts = split_statements("   \n\t  ");
        assert!(stmts.is_empty());
    }

    #[test]
    fn test_mixed_comments_and_statements() {
        let input = "-- first\nSELECT 1;\n/* second */\nSELECT 2;";
        let stmts = split_statements(input);
        assert_eq!(stmts.len(), 2);
    }
}
