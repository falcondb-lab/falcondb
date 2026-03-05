use crate::client::DbClient;
use crate::csv::{quote_field, CsvOptions};
use anyhow::{bail, Context, Result};
use futures::TryStreamExt;
use std::io::{BufWriter, Write};
use tracing::debug;

/// Parsed options for \export.
#[derive(Debug, Clone)]
pub struct ExportCmd {
    pub query: String,
    pub file: String,
    pub opts: CsvOptions,
    pub overwrite: bool,
    /// Use COPY TO STDOUT for streaming (fast path). Falls back to SELECT if false.
    pub use_copy: bool,
}

/// Parse a \export command line.
///
/// Syntax:
///   \export <query> TO <file> [WITH HEADER] [DELIMITER '<c>'] [NULL AS '<s>']
///           [QUOTE '<c>'] [ESCAPE '<c>'] [OVERWRITE]
///
/// The query may be quoted with single quotes or unquoted up to the keyword TO.
pub fn parse_export(line: &str) -> Result<ExportCmd> {
    // Strip leading \export
    let rest = line.trim();
    let rest = rest
        .strip_prefix("\\export")
        .or_else(|| rest.strip_prefix("export"))
        .unwrap_or(rest)
        .trim();

    // Find " TO " keyword (case-insensitive) that separates query from file+options
    let to_pos = find_keyword_to(rest)?;
    let query_part = rest[..to_pos].trim();
    let after_to = rest[to_pos..].trim();
    // Strip "TO"
    let after_to = after_to[2..].trim();

    // The file path is the next token (possibly quoted)
    let (file, remainder) = parse_token(after_to)?;

    let mut opts = CsvOptions::default();
    let mut overwrite = false;

    parse_options(remainder, &mut opts, &mut overwrite)?;

    let query = strip_outer_quotes(query_part);
    // Detect whether the query is a bare table name or a SELECT — COPY only works with table names
    // or SELECT; we always emit COPY (SELECT ...) TO STDOUT which works for both.
    let use_copy = true;

    Ok(ExportCmd {
        query,
        file,
        opts,
        overwrite,
        use_copy,
    })
}

/// Execute an export. Fast path: COPY (query) TO STDOUT CSV — streams directly to disk.
/// Fallback (use_copy=false): SELECT all rows into memory then write.
pub async fn run_export(client: &DbClient, cmd: &ExportCmd) -> Result<u64> {
    let path = std::path::Path::new(&cmd.file);
    if path.exists() && !cmd.overwrite {
        bail!(
            "EXPORT: file '{}' already exists. Use OVERWRITE to replace it.",
            cmd.file
        );
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            bail!("EXPORT: directory '{}' does not exist.", parent.display());
        }
    }

    if cmd.use_copy {
        run_export_copy(client, cmd).await
    } else {
        run_export_select(client, cmd).await
    }
}

/// Fast path: COPY (query) TO STDOUT WITH (FORMAT CSV, ...) — server streams CSV bytes directly.
async fn run_export_copy(client: &DbClient, cmd: &ExportCmd) -> Result<u64> {
    let delim = cmd.opts.delimiter as char;
    let quote = cmd.opts.quote as char;
    let null_str = if cmd.opts.null_as.is_empty() {
        String::new()
    } else {
        format!(", NULL '{}'", cmd.opts.null_as.replace('\'', "''"))
    };
    let header_opt = if cmd.opts.header { ", HEADER" } else { "" };

    // Wrap any query in parens for COPY; if it's already a simple table name, COPY handles it too.
    let source = if looks_like_table_name(&cmd.query) {
        cmd.query.clone()
    } else {
        format!("({})", cmd.query)
    };

    let copy_sql = format!(
        "COPY {source} TO STDOUT WITH (FORMAT CSV, DELIMITER '{delim}', QUOTE '{quote}'{null_str}{header_opt})"
    );

    debug!("EXPORT COPY: {}", copy_sql);

    let file = std::fs::File::create(&cmd.file)
        .with_context(|| format!("EXPORT: cannot create file '{}'", cmd.file))?;
    // 1 MB write buffer — amortises syscall cost for large exports
    let mut writer = BufWriter::with_capacity(1 << 20, file);

    let stream = client
        .client
        .copy_out(&copy_sql)
        .await
        .context("EXPORT: COPY TO STDOUT failed")?;

    let mut stream = Box::pin(stream);
    let mut bytes_written: u64 = 0;
    while let Some(chunk) = stream.as_mut().try_next().await.context("EXPORT: stream read error")? {
        writer.write_all(&chunk).context("EXPORT: write error")?;
        bytes_written += chunk.len() as u64;
    }
    writer.flush().context("EXPORT: flush error")?;

    // Count rows by counting newlines (each CSV row ends with \n).
    // This avoids re-reading the file; not 100% accurate for multi-line fields but good enough.
    let rows = if bytes_written == 0 {
        0
    } else {
        // Re-open and count — only for small overhead reporting
        let content = std::fs::read(&cmd.file).unwrap_or_default();
        let n = content.iter().filter(|&&b| b == b'\n').count() as u64;
        if cmd.opts.header && n > 0 { n - 1 } else { n }
    };

    Ok(rows)
}

/// Slow path: SELECT all rows into memory, write manually (used when COPY is unavailable).
async fn run_export_select(client: &DbClient, cmd: &ExportCmd) -> Result<u64> {
    debug!("EXPORT SELECT: {}", cmd.query);
    let (rows, _tag) = client
        .query_simple(&cmd.query)
        .await
        .context("EXPORT: query failed")?;

    let file = std::fs::File::create(&cmd.file)
        .with_context(|| format!("EXPORT: cannot create file '{}'", cmd.file))?;
    let mut writer = BufWriter::with_capacity(1 << 20, file);

    let col_names: Vec<String> = if rows.is_empty() {
        Vec::new()
    } else {
        rows[0].columns().iter().map(|c| c.name().to_owned()).collect()
    };

    if cmd.opts.header && !col_names.is_empty() {
        let delim_str = (cmd.opts.delimiter as char).to_string();
        let header: Vec<String> = col_names.iter().map(|c| quote_field(c, &cmd.opts)).collect();
        writeln!(writer, "{}", header.join(&delim_str)).context("EXPORT: write error")?;
    }

    let ncols = col_names.len();
    let delim_str = (cmd.opts.delimiter as char).to_string();
    let mut exported: u64 = 0;

    for row in &rows {
        let fields: Vec<String> = (0..ncols)
            .map(|i| {
                let v = row.get(i).unwrap_or("");
                if v.is_empty() && !cmd.opts.null_as.is_empty() {
                    cmd.opts.null_as.clone()
                } else {
                    quote_field(v, &cmd.opts)
                }
            })
            .collect();
        writeln!(writer, "{}", fields.join(&delim_str)).context("EXPORT: write error")?;
        exported += 1;
    }

    writer.flush().context("EXPORT: flush error")?;
    Ok(exported)
}

/// Heuristic: does the string look like a plain table name (no spaces, no parens)?
fn looks_like_table_name(s: &str) -> bool {
    let s = s.trim();
    !s.is_empty() && !s.contains(' ') && !s.contains('(') && !s.contains('\n')
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Find the byte position of " TO " (case-insensitive) in the string,
/// skipping over single-quoted regions.
fn find_keyword_to(s: &str) -> Result<usize> {
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
        if !in_quote && i + 2 <= len {
            // Look for whitespace + TO + whitespace
            let slice = &s[i..];
            if let Some(rest) = slice.strip_prefix(' ') {
                let upper = rest.to_uppercase();
                if upper.starts_with("TO ") || upper.starts_with("TO\t") {
                    return Ok(i + 1); // position of 'T'
                }
            }
        }
        i += 1;
    }
    bail!("EXPORT: missing TO keyword. Usage: \\export <query> TO <file>")
}

/// Parse the next whitespace-delimited token (or single-quoted string).
fn parse_token(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        bail!("EXPORT: expected a file path");
    }
    if let Some(after_quote) = s.strip_prefix('\'') {
        // Quoted path
        let end = after_quote
            .find('\'')
            .ok_or_else(|| anyhow::anyhow!("EXPORT: unclosed quote in file path"))?;
        let token = after_quote[..end].to_string();
        let rest = after_quote[end + 1..].trim_start();
        Ok((token, rest))
    } else {
        // Unquoted — up to next whitespace
        let end = s.find(char::is_whitespace).unwrap_or(s.len());
        let token = s[..end].to_string();
        let rest = s[end..].trim_start();
        Ok((token, rest))
    }
}

/// Parse the options tail (WITH HEADER, DELIMITER, NULL AS, QUOTE, ESCAPE, OVERWRITE).
fn parse_options(mut s: &str, opts: &mut CsvOptions, overwrite: &mut bool) -> Result<()> {
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
                .ok_or_else(|| anyhow::anyhow!("EXPORT: invalid delimiter '{tok}'"))?;
            s = rest;
        } else if upper.starts_with("NULL AS ") {
            s = s[8..].trim();
            let (tok, rest) = parse_token(s)?;
            opts.null_as = strip_outer_quotes(&tok);
            s = rest;
        } else if upper.starts_with("QUOTE ") {
            s = s[6..].trim();
            let (tok, rest) = parse_token(s)?;
            let q = crate::csv::parse_delimiter(&tok)
                .ok_or_else(|| anyhow::anyhow!("EXPORT: invalid QUOTE char '{tok}'"))?;
            opts.quote = q;
            s = rest;
        } else if upper.starts_with("ESCAPE ") {
            s = s[7..].trim();
            let (tok, rest) = parse_token(s)?;
            let e = crate::csv::parse_delimiter(&tok)
                .ok_or_else(|| anyhow::anyhow!("EXPORT: invalid ESCAPE char '{tok}'"))?;
            opts.escape = e;
            s = rest;
        } else if upper.starts_with("ENCODING ") {
            // Accept but ignore — only UTF-8 supported
            s = s[9..].trim();
            let (_tok, rest) = parse_token(s)?;
            s = rest;
        } else if upper.starts_with("OVERWRITE") {
            *overwrite = true;
            s = s[9..].trim();
        } else {
            bail!("EXPORT: unknown option near '{s}'");
        }
    }
    Ok(())
}

fn strip_outer_quotes(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_export_basic() {
        let cmd = parse_export("\\export SELECT 1 TO /tmp/out.csv").unwrap();
        assert_eq!(cmd.query, "SELECT 1");
        assert_eq!(cmd.file, "/tmp/out.csv");
        assert!(cmd.opts.header);
        assert!(!cmd.overwrite);
    }

    #[test]
    fn test_parse_export_with_overwrite() {
        let cmd = parse_export("\\export SELECT 1 TO /tmp/out.csv OVERWRITE").unwrap();
        assert!(cmd.overwrite);
    }

    #[test]
    fn test_parse_export_delimiter_tab() {
        let cmd = parse_export("\\export SELECT 1 TO /tmp/out.csv DELIMITER '\\t'").unwrap();
        assert_eq!(cmd.opts.delimiter, b'\t');
    }

    #[test]
    fn test_parse_export_without_header() {
        let cmd = parse_export("\\export SELECT 1 TO /tmp/out.csv WITHOUT HEADER").unwrap();
        assert!(!cmd.opts.header);
    }

    #[test]
    fn test_parse_export_null_as() {
        let cmd = parse_export("\\export SELECT 1 TO /tmp/out.csv NULL AS 'NULL'").unwrap();
        assert_eq!(cmd.opts.null_as, "NULL");
    }

    #[test]
    fn test_parse_export_missing_to() {
        assert!(parse_export("\\export SELECT 1 /tmp/out.csv").is_err());
    }

    #[test]
    fn test_parse_export_quoted_query() {
        let cmd = parse_export("\\export 'SELECT a, b FROM t' TO /tmp/out.csv").unwrap();
        assert_eq!(cmd.query, "SELECT a, b FROM t");
    }
}
