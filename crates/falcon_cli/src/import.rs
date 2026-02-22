use crate::client::DbClient;
use crate::csv::{parse_csv_line, CsvOptions};
use anyhow::{bail, Context, Result};
use std::io::{BufRead, BufReader};
use tracing::debug;

/// Parsed options for \import.
#[derive(Debug, Clone)]
pub struct ImportCmd {
    pub file: String,
    pub table: String,
    pub opts: CsvOptions,
    pub batch_size: usize,
    pub on_error_stop: bool,
}

/// Summary returned after a completed import.
#[derive(Debug, Default)]
pub struct ImportSummary {
    pub total: u64,
    pub inserted: u64,
    pub failed: u64,
}

/// Parse a \import command line.
///
/// Syntax:
///   \import <file> INTO <table> [WITH HEADER] [DELIMITER '<c>'] [NULL AS '<s>']
///           [QUOTE '<c>'] [ESCAPE '<c>'] [BATCH SIZE <n>]
///           [ON ERROR STOP | ON ERROR CONTINUE]
pub fn parse_import(line: &str) -> Result<ImportCmd> {
    let rest = line.trim();
    let rest = rest
        .strip_prefix("\\import")
        .or_else(|| rest.strip_prefix("import"))
        .unwrap_or(rest)
        .trim();

    // Find " INTO " keyword
    let into_pos = find_keyword_into(rest)?;
    let file_part = rest[..into_pos].trim();
    let after_into = rest[into_pos..].trim();
    let after_into = after_into[4..].trim(); // strip "INTO"

    // Table name is the next token
    let (table, remainder) = parse_token(after_into)?;

    let mut opts = CsvOptions::default();
    let mut batch_size: usize = 1000;
    let mut on_error_stop = false;

    parse_options(remainder, &mut opts, &mut batch_size, &mut on_error_stop)?;

    let file = strip_outer_quotes(file_part);

    Ok(ImportCmd {
        file,
        table,
        opts,
        batch_size,
        on_error_stop,
    })
}

/// Execute an import: read CSV file, batch INSERT into table.
pub async fn run_import(client: &DbClient, cmd: &ImportCmd) -> Result<ImportSummary> {
    let path = std::path::Path::new(&cmd.file);
    if !path.exists() {
        bail!("IMPORT: file '{}' not found.", cmd.file);
    }

    let file =
        std::fs::File::open(path).with_context(|| format!("IMPORT: cannot open '{}'", cmd.file))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    // Read header row to determine column names
    let col_names: Vec<String> = if cmd.opts.header {
        let header_line = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("IMPORT: file '{}' is empty.", cmd.file))?
            .with_context(|| format!("IMPORT: read error in '{}'", cmd.file))?;
        parse_csv_line(&header_line, &cmd.opts)
            .ok_or_else(|| anyhow::anyhow!("IMPORT: malformed header in '{}'", cmd.file))?
    } else {
        // No header: query table column order from information_schema
        get_table_columns(client, &cmd.table).await?
    };

    if col_names.is_empty() {
        bail!(
            "IMPORT: could not determine column names for table '{}'.",
            cmd.table
        );
    }

    debug!(
        "IMPORT: table={}, columns={:?}, batch={}",
        cmd.table, col_names, cmd.batch_size
    );

    let ncols = col_names.len();
    let mut summary = ImportSummary::default();
    let mut batch: Vec<Vec<String>> = Vec::with_capacity(cmd.batch_size);
    let mut line_num: u64 = if cmd.opts.header { 1 } else { 0 };

    for raw_line in lines {
        line_num += 1;
        let raw = raw_line.with_context(|| {
            format!("IMPORT: read error at line {} in '{}'", line_num, cmd.file)
        })?;

        // Skip blank lines
        if raw.trim().is_empty() {
            continue;
        }

        summary.total += 1;

        let fields = match parse_csv_line(&raw, &cmd.opts) {
            Some(f) => f,
            None => {
                let msg = format!(
                    "IMPORT: malformed CSV at line {} in '{}'",
                    line_num, cmd.file
                );
                if cmd.on_error_stop {
                    bail!("{}", msg);
                }
                eprintln!("ERROR: {}", msg);
                summary.failed += 1;
                continue;
            }
        };

        if fields.len() != ncols {
            let msg = format!(
                "IMPORT: column count mismatch at line {} (expected {}, got {})",
                line_num,
                ncols,
                fields.len()
            );
            if cmd.on_error_stop {
                bail!("{}", msg);
            }
            eprintln!("ERROR: {}", msg);
            summary.failed += 1;
            continue;
        }

        batch.push(fields);

        if batch.len() >= cmd.batch_size {
            let (ok, fail) = flush_batch(
                client,
                &cmd.table,
                &col_names,
                &batch,
                &cmd.opts,
                cmd.on_error_stop,
            )
            .await?;
            summary.inserted += ok;
            summary.failed += fail;
            batch.clear();

            // Periodic progress
            let done = summary.inserted + summary.failed;
            if done % 10_000 == 0 {
                eprintln!(
                    "Imported {} rows (failed: {})",
                    summary.inserted, summary.failed
                );
            }
        }
    }

    // Flush remaining
    if !batch.is_empty() {
        let (ok, fail) = flush_batch(
            client,
            &cmd.table,
            &col_names,
            &batch,
            &cmd.opts,
            cmd.on_error_stop,
        )
        .await?;
        summary.inserted += ok;
        summary.failed += fail;
    }

    Ok(summary)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Flush a batch of rows via individual INSERT statements.
/// Returns (inserted_count, failed_count).
async fn flush_batch(
    client: &DbClient,
    table: &str,
    col_names: &[String],
    batch: &[Vec<String>],
    opts: &CsvOptions,
    on_error_stop: bool,
) -> Result<(u64, u64)> {
    let mut inserted: u64 = 0;
    let mut failed: u64 = 0;

    let col_list = col_names
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    for row in batch {
        let values = row
            .iter()
            .map(|v| {
                if !opts.null_as.is_empty() && v == &opts.null_as {
                    "NULL".to_string()
                } else {
                    format!("'{}'", v.replace('\'', "''"))
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            table.replace('"', "\"\""),
            col_list,
            values
        );

        if let Err(e) = client.query_simple(&sql).await {
            let msg = format!("IMPORT: INSERT failed: {}", e);
            if on_error_stop {
                bail!("{}", msg);
            }
            eprintln!("ERROR: {}", msg);
            failed += 1;
        } else {
            inserted += 1;
        }
    }

    Ok((inserted, failed))
}

/// Query information_schema to get ordered column names for a table.
async fn get_table_columns(client: &DbClient, table: &str) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = '{}' ORDER BY ordinal_position",
        table.replace('\'', "''")
    );
    let (rows, _) = client
        .query_simple(&sql)
        .await
        .context("IMPORT: failed to query column names")?;
    if rows.is_empty() {
        bail!("IMPORT: table '{}' not found or has no columns.", table);
    }
    Ok(rows
        .iter()
        .map(|r| r.get(0).unwrap_or("").to_string())
        .collect())
}

/// Find the byte position of " INTO " (case-insensitive) in the string,
/// skipping single-quoted regions.
fn find_keyword_into(s: &str) -> Result<usize> {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut in_quote = false;

    while i < len {
        if bytes[i] == b'\'' {
            in_quote = !in_quote;
            i += 1;
            continue;
        }
        if !in_quote {
            let slice = &s[i..];
            let upper = slice.to_uppercase();
            if upper.starts_with("INTO ") || upper.starts_with("INTO\t") {
                return Ok(i);
            }
        }
        i += 1;
    }
    bail!("IMPORT: missing INTO keyword. Usage: \\import <file> INTO <table>")
}

/// Parse the next whitespace-delimited token (or single-quoted string).
fn parse_token(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        bail!("IMPORT: expected a value");
    }
    if s.starts_with('\'') {
        let end = s[1..]
            .find('\'')
            .ok_or_else(|| anyhow::anyhow!("IMPORT: unclosed quote"))?;
        let token = s[1..end + 1].to_string();
        let rest = s[end + 2..].trim_start();
        Ok((token, rest))
    } else {
        let end = s.find(char::is_whitespace).unwrap_or(s.len());
        let token = s[..end].to_string();
        let rest = s[end..].trim_start();
        Ok((token, rest))
    }
}

fn parse_options(
    mut s: &str,
    opts: &mut CsvOptions,
    batch_size: &mut usize,
    on_error_stop: &mut bool,
) -> Result<()> {
    s = s.trim();
    while !s.is_empty() {
        let upper = s.to_uppercase();
        if upper.starts_with("WITH HEADER") {
            opts.header = true;
            s = s[11..].trim();
        } else if upper.starts_with("WITHOUT HEADER") {
            opts.header = false;
            s = s[14..].trim();
        } else if upper.starts_with("DELIMITER ") {
            s = s[10..].trim();
            let (tok, rest) = parse_token(s)?;
            opts.delimiter = crate::csv::parse_delimiter(&tok)
                .ok_or_else(|| anyhow::anyhow!("IMPORT: invalid delimiter '{}'", tok))?;
            s = rest;
        } else if upper.starts_with("NULL AS ") {
            s = s[8..].trim();
            let (tok, rest) = parse_token(s)?;
            opts.null_as = strip_outer_quotes(&tok);
            s = rest;
        } else if upper.starts_with("QUOTE ") {
            s = s[6..].trim();
            let (tok, rest) = parse_token(s)?;
            opts.quote = crate::csv::parse_delimiter(&tok)
                .ok_or_else(|| anyhow::anyhow!("IMPORT: invalid QUOTE char '{}'", tok))?;
            s = rest;
        } else if upper.starts_with("ESCAPE ") {
            s = s[7..].trim();
            let (tok, rest) = parse_token(s)?;
            opts.escape = crate::csv::parse_delimiter(&tok)
                .ok_or_else(|| anyhow::anyhow!("IMPORT: invalid ESCAPE char '{}'", tok))?;
            s = rest;
        } else if upper.starts_with("ENCODING ") {
            s = s[9..].trim();
            let (_tok, rest) = parse_token(s)?;
            s = rest;
        } else if upper.starts_with("BATCH SIZE ") {
            s = s[11..].trim();
            let (tok, rest) = parse_token(s)?;
            *batch_size = tok
                .parse::<usize>()
                .with_context(|| format!("IMPORT: invalid BATCH SIZE '{}'", tok))?;
            s = rest;
        } else if upper.starts_with("ON ERROR STOP") {
            *on_error_stop = true;
            s = s[13..].trim();
        } else if upper.starts_with("ON ERROR CONTINUE") {
            *on_error_stop = false;
            s = s[17..].trim();
        } else {
            bail!("IMPORT: unknown option near '{}'", s);
        }
    }
    Ok(())
}

fn strip_outer_quotes(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_import_basic() {
        let cmd = parse_import("\\import /tmp/data.csv INTO mytable").unwrap();
        assert_eq!(cmd.file, "/tmp/data.csv");
        assert_eq!(cmd.table, "mytable");
        assert!(cmd.opts.header);
        assert_eq!(cmd.batch_size, 1000);
        assert!(!cmd.on_error_stop);
    }

    #[test]
    fn test_parse_import_batch_size() {
        let cmd = parse_import("\\import /tmp/data.csv INTO mytable BATCH SIZE 500").unwrap();
        assert_eq!(cmd.batch_size, 500);
    }

    #[test]
    fn test_parse_import_on_error_stop() {
        let cmd = parse_import("\\import /tmp/data.csv INTO mytable ON ERROR STOP").unwrap();
        assert!(cmd.on_error_stop);
    }

    #[test]
    fn test_parse_import_on_error_continue() {
        let cmd = parse_import("\\import /tmp/data.csv INTO mytable ON ERROR CONTINUE").unwrap();
        assert!(!cmd.on_error_stop);
    }

    #[test]
    fn test_parse_import_without_header() {
        let cmd = parse_import("\\import /tmp/data.csv INTO mytable WITHOUT HEADER").unwrap();
        assert!(!cmd.opts.header);
    }

    #[test]
    fn test_parse_import_delimiter_tab() {
        let cmd = parse_import("\\import /tmp/data.csv INTO mytable DELIMITER '\\t'").unwrap();
        assert_eq!(cmd.opts.delimiter, b'\t');
    }

    #[test]
    fn test_parse_import_null_as() {
        let cmd = parse_import("\\import /tmp/data.csv INTO mytable NULL AS 'NULL'").unwrap();
        assert_eq!(cmd.opts.null_as, "NULL");
    }

    #[test]
    fn test_parse_import_missing_into() {
        assert!(parse_import("\\import /tmp/data.csv mytable").is_err());
    }

    #[test]
    fn test_parse_import_quoted_file() {
        let cmd = parse_import("\\import '/tmp/my data.csv' INTO mytable").unwrap();
        assert_eq!(cmd.file, "/tmp/my data.csv");
    }

    #[test]
    fn test_parse_import_combined_options() {
        let cmd = parse_import(
            "\\import /tmp/data.csv INTO mytable DELIMITER '\\t' WITHOUT HEADER BATCH SIZE 200 ON ERROR STOP",
        )
        .unwrap();
        assert_eq!(cmd.opts.delimiter, b'\t');
        assert!(!cmd.opts.header);
        assert_eq!(cmd.batch_size, 200);
        assert!(cmd.on_error_stop);
    }
}
