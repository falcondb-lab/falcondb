use crate::client::DbClient;
use crate::format::{format_command_tag, OutputMode};
use crate::pager::print_with_pager;
use anyhow::Result;
use tracing::debug;

/// Execute a single SQL statement and print the result.
/// Returns Err on SQL error.
pub async fn run_statement(
    client: &DbClient,
    sql: &str,
    mode: OutputMode,
    tuples_only: bool,
) -> Result<()> {
    debug!("run_statement: {}", sql);

    let (rows, tag) = client.query_simple(sql).await?;

    if rows.is_empty() {
        // DML or DDL — just print the command tag
        if !tag.is_empty() && !tuples_only {
            println!("{}", format_command_tag(&tag));
        }
        return Ok(());
    }

    // SELECT-like result — format and print
    // Pager is disabled for CSV/JSON (scriptable formats) and non-TTY
    let disable_pager = matches!(
        mode,
        OutputMode::Csv | OutputMode::Json | OutputMode::TuplesOnly
    );

    let output = match mode {
        OutputMode::Table => {
            let mut s = format_table_simple(&rows);
            if !tuples_only {
                let count = rows.len();
                s.push_str(&format!(
                    "({} row{})\n",
                    count,
                    if count == 1 { "" } else { "s" }
                ));
            }
            s
        }
        OutputMode::TuplesOnly => format_tuples_only_str(&rows),
        OutputMode::Expanded => {
            let mut s = format_expanded_str(&rows);
            if !tuples_only {
                let count = rows.len();
                s.push_str(&format!(
                    "({} row{})\n",
                    count,
                    if count == 1 { "" } else { "s" }
                ));
            }
            s
        }
        OutputMode::Csv => format_csv_str(&rows),
        OutputMode::Json => format_json_str(&rows),
    };

    print_with_pager(&output, disable_pager);

    Ok(())
}

/// Format a result set as a string using the given output mode.
/// Used by operational introspection modules (cluster, txn, node, etc.).
pub fn format_rows_as_string(
    rows: &[tokio_postgres::SimpleQueryRow],
    mode: OutputMode,
    _title: &str,
) -> String {
    if rows.is_empty() {
        return String::new();
    }
    match mode {
        OutputMode::Json => format_json_str(rows),
        OutputMode::Csv => format_csv_str(rows),
        OutputMode::TuplesOnly => format_tuples_only_str(rows),
        OutputMode::Expanded => {
            let mut s = format_expanded_str(rows);
            let count = rows.len();
            s.push_str(&format!(
                "({} row{})\n",
                count,
                if count == 1 { "" } else { "s" }
            ));
            s
        }
        OutputMode::Table => {
            let mut s = format_table_simple(rows);
            let count = rows.len();
            s.push_str(&format!(
                "({} row{})\n",
                count,
                if count == 1 { "" } else { "s" }
            ));
            s
        }
    }
}

fn get_col_names(rows: &[tokio_postgres::SimpleQueryRow]) -> Vec<String> {
    if rows.is_empty() {
        return Vec::new();
    }
    // SimpleQueryRow exposes column count but not names directly via index;
    // use the columns() method
    rows[0]
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect()
}

fn get_val(row: &tokio_postgres::SimpleQueryRow, idx: usize) -> &str {
    row.get(idx).unwrap_or("NULL")
}

fn format_table_simple(rows: &[tokio_postgres::SimpleQueryRow]) -> String {
    let cols = get_col_names(rows);
    let ncols = cols.len();
    let mut out = String::new();

    // Compute column widths
    let mut widths: Vec<usize> = cols.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, w) in widths.iter_mut().enumerate() {
            *w = (*w).max(get_val(row, i).len());
        }
    }

    // Header
    let header: Vec<String> = cols
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:<width$}", c, width = widths[i]))
        .collect();
    out.push_str(&format!(" {} \n", header.join(" | ")));

    // Separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w + 2)).collect();
    out.push_str(&format!("{}\n", sep.join("+")));

    // Rows
    for row in rows {
        let cells: Vec<String> = (0..ncols)
            .map(|i| format!("{:<width$}", get_val(row, i), width = widths[i]))
            .collect();
        out.push_str(&format!(" {} \n", cells.join(" | ")));
    }
    out
}

fn format_tuples_only_str(rows: &[tokio_postgres::SimpleQueryRow]) -> String {
    let ncols = if rows.is_empty() {
        0
    } else {
        rows[0].columns().len()
    };
    let mut out = String::new();
    for row in rows {
        let vals: Vec<&str> = (0..ncols).map(|i| get_val(row, i)).collect();
        out.push_str(&vals.join("|"));
        out.push('\n');
    }
    out
}

fn format_expanded_str(rows: &[tokio_postgres::SimpleQueryRow]) -> String {
    let cols = get_col_names(rows);
    let col_width = cols.iter().map(|c| c.len()).max().unwrap_or(0);
    let mut out = String::new();
    for (ridx, row) in rows.iter().enumerate() {
        out.push_str(&format!("-[ RECORD {} ]\n", ridx + 1));
        for (cidx, col) in cols.iter().enumerate() {
            let val = get_val(row, cidx);
            let display_val = if val == "NULL" {
                "NULL".to_string()
            } else {
                val.to_string()
            };
            out.push_str(&format!(
                "{:<width$} | {}\n",
                col,
                display_val,
                width = col_width
            ));
        }
    }
    out
}

fn format_csv_str(rows: &[tokio_postgres::SimpleQueryRow]) -> String {
    let cols = get_col_names(rows);
    let ncols = cols.len();
    let mut out = String::new();

    // Header
    out.push_str(&cols.join(","));
    out.push('\n');

    for row in rows {
        let vals: Vec<String> = (0..ncols)
            .map(|i| {
                let v = get_val(row, i);
                if v.contains(',') || v.contains('"') || v.contains('\n') {
                    format!("\"{}\"", v.replace('"', "\"\""))
                } else {
                    v.to_string()
                }
            })
            .collect();
        out.push_str(&vals.join(","));
        out.push('\n');
    }
    out
}

fn format_json_str(rows: &[tokio_postgres::SimpleQueryRow]) -> String {
    let cols = get_col_names(rows);
    let ncols = cols.len();

    let json_rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for (i, col) in cols.iter().enumerate() {
                map.insert(
                    col.clone(),
                    serde_json::Value::String(get_val(row, i).to_string()),
                );
            }
            serde_json::Value::Object(map)
        })
        .collect();

    let _ = ncols;
    let mut out =
        serde_json::to_string_pretty(&serde_json::Value::Array(json_rows)).unwrap_or_default();
    out.push('\n');
    out
}
