use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper};
use tracing::debug;

/// Static list of SQL keywords for tab completion.
static SQL_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "CREATE",
    "TABLE",
    "DROP",
    "ALTER",
    "INDEX",
    "PRIMARY",
    "KEY",
    "NOT",
    "NULL",
    "DEFAULT",
    "UNIQUE",
    "REFERENCES",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "ON",
    "AND",
    "OR",
    "IN",
    "IS",
    "LIKE",
    "BETWEEN",
    "ORDER",
    "BY",
    "GROUP",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "DISTINCT",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "AS",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "TRANSACTION",
    "WITH",
    "UNION",
    "ALL",
    "EXCEPT",
    "INTERSECT",
    "RETURNING",
    "ON CONFLICT",
    "DO",
    "NOTHING",
    "TRUNCATE",
    "EXPLAIN",
    "ANALYZE",
    "SHOW",
    "SET",
    "RESET",
];

/// Static list of meta-commands for tab completion.
static META_COMMANDS: &[&str] = &[
    "\\q",
    "\\?",
    "\\conninfo",
    "\\dt",
    "\\d",
    "\\l",
    "\\c",
    "\\x",
    "\\timing",
];

/// rustyline Helper that provides tab completion for SQL keywords and meta-commands.
pub struct FsqlHelper;

impl Completer for FsqlHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let prefix = &line[..pos];

        // Find the start of the current word
        let word_start = prefix
            .rfind(|c: char| c.is_whitespace())
            .map(|i| i + 1)
            .unwrap_or(0);
        let word = &prefix[word_start..];

        debug!("Completing word: {:?}", word);

        let mut candidates: Vec<Pair> = Vec::new();

        if word.starts_with('\\') {
            // Meta-command completion
            for &cmd in META_COMMANDS {
                if cmd.to_lowercase().starts_with(&word.to_lowercase()) {
                    candidates.push(Pair {
                        display: cmd.to_string(),
                        replacement: cmd.to_string(),
                    });
                }
            }
        } else if !word.is_empty() {
            // SQL keyword completion (case-insensitive, complete in uppercase)
            for &kw in SQL_KEYWORDS {
                if kw.to_lowercase().starts_with(&word.to_lowercase()) {
                    candidates.push(Pair {
                        display: kw.to_string(),
                        replacement: kw.to_string(),
                    });
                }
            }
        }

        Ok((word_start, candidates))
    }
}

impl Hinter for FsqlHelper {
    type Hint = String;
    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        None
    }
}

impl Highlighter for FsqlHelper {}
impl Validator for FsqlHelper {}
impl Helper for FsqlHelper {}

#[cfg(test)]
mod tests {
    use super::*;
    use rustyline::history::DefaultHistory;
    use rustyline::Context;

    fn ctx() -> DefaultHistory {
        DefaultHistory::new()
    }

    #[test]
    fn test_complete_sql_keyword_select() {
        let helper = FsqlHelper;
        let history = ctx();
        let ctx = Context::new(&history);
        let (start, candidates) = helper.complete("SEL", 3, &ctx).unwrap();
        assert_eq!(start, 0);
        assert!(candidates.iter().any(|c| c.replacement == "SELECT"));
    }

    #[test]
    fn test_complete_sql_keyword_case_insensitive() {
        let helper = FsqlHelper;
        let history = ctx();
        let ctx = Context::new(&history);
        let (_, candidates) = helper.complete("sel", 3, &ctx).unwrap();
        assert!(candidates.iter().any(|c| c.replacement == "SELECT"));
    }

    #[test]
    fn test_complete_meta_command() {
        let helper = FsqlHelper;
        let history = ctx();
        let ctx = Context::new(&history);
        let (start, candidates) = helper.complete("\\ti", 3, &ctx).unwrap();
        assert_eq!(start, 0);
        assert!(candidates.iter().any(|c| c.replacement == "\\timing"));
    }

    #[test]
    fn test_complete_meta_command_backslash_only() {
        let helper = FsqlHelper;
        let history = ctx();
        let ctx = Context::new(&history);
        let (_, candidates) = helper.complete("\\", 1, &ctx).unwrap();
        assert!(!candidates.is_empty(), "should suggest all meta-commands");
    }

    #[test]
    fn test_no_completion_for_empty() {
        let helper = FsqlHelper;
        let history = ctx();
        let ctx = Context::new(&history);
        let (_, candidates) = helper.complete("", 0, &ctx).unwrap();
        assert!(
            candidates.is_empty(),
            "empty input should yield no completions"
        );
    }

    #[test]
    fn test_complete_mid_line_word() {
        let helper = FsqlHelper;
        let history = ctx();
        let ctx = Context::new(&history);
        // "SELECT FRO" — complete the second word
        let (start, candidates) = helper.complete("SELECT FRO", 10, &ctx).unwrap();
        assert_eq!(start, 7); // "FRO" starts at position 7
        assert!(candidates.iter().any(|c| c.replacement == "FROM"));
    }
}
