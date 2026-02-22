use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{DataType, TableId};
use falcon_txn::TxnHandle;

use falcon_common::error::ExecutionError;

use crate::executor::{ExecutionResult, Executor};

impl Executor {
    /// Execute COPY FROM STDIN: parse the received text/CSV data and insert rows.
    /// Called by the protocol layer after collecting all CopyData messages.
    #[allow(clippy::too_many_arguments)]
    pub fn exec_copy_from_data(
        &self,
        table_id: TableId,
        schema: &TableSchema,
        columns: &[usize],
        data: &[u8],
        csv: bool,
        delimiter: char,
        header: bool,
        null_string: &str,
        quote: char,
        escape: char,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let text = String::from_utf8(data.to_vec()).map_err(|e| {
            FalconError::Execution(ExecutionError::TypeError(format!(
                "Invalid UTF-8 in COPY data: {}",
                e
            )))
        })?;

        let mut rows_inserted: u64 = 0;

        for (line_idx, line) in text.lines().enumerate() {
            // Skip header row
            if header && line_idx == 0 {
                continue;
            }

            // Skip empty lines
            if line.is_empty() {
                continue;
            }

            // Parse fields from the line
            let fields = if csv {
                parse_csv_line(line, delimiter, quote, escape)
            } else {
                parse_text_line(line, delimiter)
            };

            if fields.len() != columns.len() {
                return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                    "COPY line {}: expected {} columns but got {}",
                    line_idx + 1,
                    columns.len(),
                    fields.len()
                ))));
            }

            // Build row with default values
            let mut values: Vec<Datum> = schema
                .columns
                .iter()
                .map(|c| c.default_value.clone().unwrap_or(Datum::Null))
                .collect();

            // Parse each field into the appropriate Datum type
            for (i, field) in fields.iter().enumerate() {
                let col_idx = columns[i];
                let col_type = &schema.columns[col_idx].data_type;

                if field == null_string {
                    values[col_idx] = Datum::Null;
                } else {
                    values[col_idx] = parse_datum(field, col_type).map_err(|e| {
                        FalconError::Execution(ExecutionError::TypeError(format!(
                            "COPY line {}, column {}: {}",
                            line_idx + 1,
                            col_idx,
                            e
                        )))
                    })?;
                }
            }

            // NOT NULL constraint check
            for (col_idx, col) in schema.columns.iter().enumerate() {
                if !col.nullable && values[col_idx].is_null() {
                    return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                        "COPY: NOT NULL constraint violated for column '{}'",
                        col.name,
                    ))));
                }
            }

            let row = OwnedRow::new(values);
            self.storage
                .insert(table_id, row, txn.txn_id)
                .map_err(FalconError::Storage)?;
            rows_inserted += 1;
        }

        Ok(ExecutionResult::Dml {
            rows_affected: rows_inserted,
            tag: "COPY".into(),
        })
    }

    /// Execute COPY (query) TO STDOUT: run the inner query and format results as text/CSV.
    #[allow(clippy::too_many_arguments)]
    pub fn exec_copy_query_to(
        &self,
        query_plan: &falcon_planner::PhysicalPlan,
        csv: bool,
        delimiter: char,
        header: bool,
        null_string: &str,
        quote: char,
        escape: char,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        // Execute the inner query
        let inner_result = self.execute(query_plan, Some(txn))?;
        let (columns, rows) = match inner_result {
            ExecutionResult::Query { columns, rows } => (columns, rows),
            _ => {
                return Err(FalconError::Internal(
                    "COPY (query) inner plan did not return Query result".into(),
                ))
            }
        };

        let mut output_lines: Vec<Vec<u8>> = Vec::new();

        // Header row
        if header {
            let header_fields: Vec<String> = columns.iter().map(|(name, _)| name.clone()).collect();
            let line = if csv {
                format_csv_line(&header_fields, delimiter, quote, escape)
            } else {
                header_fields.join(&delimiter.to_string())
            };
            output_lines.push(format!("{}\n", line).into_bytes());
        }

        for row in &rows {
            let fields: Vec<String> = row
                .values
                .iter()
                .map(|datum| {
                    if datum.is_null() {
                        null_string.to_string()
                    } else {
                        datum_to_text(datum)
                    }
                })
                .collect();

            let line = if csv {
                format_csv_line(&fields, delimiter, quote, escape)
            } else {
                fields.join(&delimiter.to_string())
            };
            output_lines.push(format!("{}\n", line).into_bytes());
        }

        let result_columns = vec![("copy_data".into(), DataType::Text)];
        let result_rows: Vec<OwnedRow> = output_lines
            .into_iter()
            .map(|bytes| {
                let s = String::from_utf8(bytes).unwrap_or_default();
                OwnedRow::new(vec![Datum::Text(s)])
            })
            .collect();

        Ok(ExecutionResult::Query {
            columns: result_columns,
            rows: result_rows,
        })
    }

    /// Execute COPY TO STDOUT: scan all rows and format as text/CSV lines.
    /// Returns the formatted data as a Vec of byte vectors (one per line).
    #[allow(clippy::too_many_arguments)]
    pub fn exec_copy_to(
        &self,
        table_id: TableId,
        schema: &TableSchema,
        columns: &[usize],
        csv: bool,
        delimiter: char,
        header: bool,
        null_string: &str,
        quote: char,
        escape: char,
        txn: &TxnHandle,
    ) -> Result<ExecutionResult, FalconError> {
        let rows = self
            .storage
            .scan(table_id, txn.txn_id, txn.start_ts)
            .map_err(FalconError::Storage)?;

        let mut output_lines: Vec<Vec<u8>> = Vec::new();

        // Header row
        if header {
            let header_fields: Vec<String> = columns
                .iter()
                .map(|&i| schema.columns[i].name.clone())
                .collect();
            let line = if csv {
                format_csv_line(&header_fields, delimiter, quote, escape)
            } else {
                header_fields.join(&delimiter.to_string())
            };
            output_lines.push(format!("{}\n", line).into_bytes());
        }

        for row in &rows {
            let fields: Vec<String> = columns
                .iter()
                .map(|&i| {
                    let datum = &row.1.values[i];
                    if datum.is_null() {
                        null_string.to_string()
                    } else {
                        datum_to_text(datum)
                    }
                })
                .collect();

            let line = if csv {
                format_csv_line(&fields, delimiter, quote, escape)
            } else {
                fields.join(&delimiter.to_string())
            };
            output_lines.push(format!("{}\n", line).into_bytes());
        }

        // Return the formatted lines as a special Query result.
        // The handler will convert these into CopyData messages.
        let result_columns = vec![("copy_data".into(), DataType::Text)];
        let result_rows: Vec<OwnedRow> = output_lines
            .into_iter()
            .map(|bytes| {
                let s = String::from_utf8(bytes).unwrap_or_default();
                OwnedRow::new(vec![Datum::Text(s)])
            })
            .collect();

        // We store the row count in a special way — the handler will use it for CommandComplete.
        // We use tag "COPY" and store the lines as rows.
        Ok(ExecutionResult::Query {
            columns: result_columns,
            rows: result_rows,
        })
    }
}

/// Parse a text-format line (tab-delimited by default).
fn parse_text_line(line: &str, delimiter: char) -> Vec<String> {
    line.split(delimiter).map(|s| s.to_string()).collect()
}

/// Parse a CSV-format line with quoting support.
fn parse_csv_line(line: &str, delimiter: char, quote: char, escape: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quotes {
            if c == escape && chars.peek() == Some(&quote) {
                // Escaped quote
                current.push(quote);
                chars.next();
            } else if c == quote {
                in_quotes = false;
            } else {
                current.push(c);
            }
        } else if c == quote {
            in_quotes = true;
        } else if c == delimiter {
            fields.push(current.clone());
            current.clear();
        } else {
            current.push(c);
        }
    }
    fields.push(current);
    fields
}

/// Format fields as a CSV line with quoting.
fn format_csv_line(fields: &[String], delimiter: char, quote: char, escape: char) -> String {
    fields
        .iter()
        .map(|f| {
            if f.contains(delimiter) || f.contains(quote) || f.contains('\n') || f.contains('\r') {
                let escaped = f.replace(quote, &format!("{}{}", escape, quote));
                format!("{}{}{}", quote, escaped, quote)
            } else {
                f.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(&delimiter.to_string())
}

/// Parse a text field into a Datum based on column type.
fn parse_datum(field: &str, data_type: &DataType) -> Result<Datum, String> {
    match data_type {
        DataType::Int32 => field
            .parse::<i32>()
            .map(Datum::Int32)
            .map_err(|e| format!("Cannot parse '{}' as INT: {}", field, e)),
        DataType::Int64 => field
            .parse::<i64>()
            .map(Datum::Int64)
            .map_err(|e| format!("Cannot parse '{}' as BIGINT: {}", field, e)),
        DataType::Float64 => field
            .parse::<f64>()
            .map(Datum::Float64)
            .map_err(|e| format!("Cannot parse '{}' as FLOAT: {}", field, e)),
        DataType::Boolean => match field.to_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "on" => Ok(Datum::Boolean(true)),
            "f" | "false" | "0" | "no" | "off" => Ok(Datum::Boolean(false)),
            _ => Err(format!("Cannot parse '{}' as BOOLEAN", field)),
        },
        DataType::Text => Ok(Datum::Text(field.to_string())),
        DataType::Timestamp => {
            use chrono::NaiveDateTime;
            let dt = NaiveDateTime::parse_from_str(field, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(field, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| NaiveDateTime::parse_from_str(field, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| NaiveDateTime::parse_from_str(field, "%Y-%m-%dT%H:%M:%S%.f"))
                .map_err(|e| format!("Cannot parse '{}' as TIMESTAMP: {}", field, e))?;
            Ok(Datum::Timestamp(dt.and_utc().timestamp_micros()))
        }
        DataType::Date => {
            use chrono::NaiveDate;
            let date = NaiveDate::parse_from_str(field, "%Y-%m-%d")
                .map_err(|e| format!("Cannot parse '{}' as DATE: {}", field, e))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap_or_else(|| NaiveDate::from_ymd_opt(2000, 1, 1).unwrap_or(NaiveDate::MIN));
            let days = (date - epoch).num_days() as i32;
            Ok(Datum::Date(days))
        }
        DataType::Jsonb => {
            let v: serde_json::Value = serde_json::from_str(field)
                .map_err(|e| format!("Cannot parse '{}' as JSONB: {}", field, e))?;
            Ok(Datum::Jsonb(v))
        }
        DataType::Array(_) => {
            // Basic array parsing: {1,2,3} or {a,b,c}
            let trimmed = field.trim();
            if trimmed.starts_with('{') && trimmed.ends_with('}') {
                let inner = &trimmed[1..trimmed.len() - 1];
                if inner.is_empty() {
                    return Ok(Datum::Array(vec![]));
                }
                let elements: Vec<Datum> = inner
                    .split(',')
                    .map(|s| Datum::Text(s.trim().to_string()))
                    .collect();
                Ok(Datum::Array(elements))
            } else {
                Err(format!("Cannot parse '{}' as ARRAY", field))
            }
        }
        DataType::Decimal(_, _) => Datum::parse_decimal(field)
            .ok_or_else(|| format!("Cannot parse '{}' as DECIMAL", field)),
        DataType::Time => {
            // Parse HH:MM:SS or HH:MM:SS.ffffff
            let parts: Vec<&str> = field.split(':').collect();
            if parts.len() < 3 {
                return Err(format!("Cannot parse '{}' as TIME", field));
            }
            let h: i64 = parts[0]
                .parse()
                .map_err(|_| format!("Cannot parse '{}' as TIME", field))?;
            let m: i64 = parts[1]
                .parse()
                .map_err(|_| format!("Cannot parse '{}' as TIME", field))?;
            let sec_parts: Vec<&str> = parts[2].split('.').collect();
            let s: i64 = sec_parts[0]
                .parse()
                .map_err(|_| format!("Cannot parse '{}' as TIME", field))?;
            let frac: i64 = if sec_parts.len() > 1 {
                let f = sec_parts[1];
                let padded = format!("{:0<6}", &f[..f.len().min(6)]);
                padded.parse().unwrap_or(0)
            } else {
                0
            };
            Ok(Datum::Time(
                h * 3_600_000_000 + m * 60_000_000 + s * 1_000_000 + frac,
            ))
        }
        DataType::Interval => {
            // Simplified: just store as text-parsed microseconds
            Ok(Datum::Text(field.to_string()))
        }
        DataType::Uuid => {
            let hex: String = field.chars().filter(|c| c.is_ascii_hexdigit()).collect();
            if hex.len() != 32 {
                return Err(format!("Cannot parse '{}' as UUID", field));
            }
            let v = u128::from_str_radix(&hex, 16)
                .map_err(|e| format!("Cannot parse '{}' as UUID: {}", field, e))?;
            Ok(Datum::Uuid(v))
        }
        DataType::Bytea => {
            // Accept PG hex format: \x<hex> or raw hex string
            let hex_str = field.strip_prefix("\\x").unwrap_or(field);
            let bytes = (0..hex_str.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex_str[i..i + 2.min(hex_str.len())], 16))
                .collect::<Result<Vec<u8>, _>>()
                .map_err(|e| format!("Cannot parse '{}' as BYTEA: {}", field, e))?;
            Ok(Datum::Bytea(bytes))
        }
    }
}

/// Convert a Datum to its text representation for COPY output.
fn datum_to_text(datum: &Datum) -> String {
    match datum {
        Datum::Null => String::new(),
        Datum::Int32(v) => v.to_string(),
        Datum::Int64(v) => v.to_string(),
        Datum::Float64(v) => v.to_string(),
        Datum::Boolean(v) => if *v { "t" } else { "f" }.to_string(),
        Datum::Text(s) => s.clone(),
        Datum::Timestamp(us) => {
            let secs = us / 1_000_000;
            let nsecs = ((us % 1_000_000) * 1000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                us.to_string()
            }
        }
        Datum::Date(days) => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap_or(chrono::NaiveDate::MIN));
            if let Some(date) = epoch.checked_add_signed(chrono::Duration::days(*days as i64)) {
                date.format("%Y-%m-%d").to_string()
            } else {
                days.to_string()
            }
        }
        Datum::Jsonb(v) => v.to_string(),
        Datum::Array(elements) => {
            let inner: Vec<String> = elements.iter().map(datum_to_text).collect();
            format!("{{{}}}", inner.join(","))
        }
        Datum::Decimal(m, s) => falcon_common::datum::decimal_to_string(*m, *s),
        Datum::Time(_) | Datum::Interval(_, _, _) | Datum::Uuid(_) => format!("{}", datum),
        Datum::Bytea(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
            format!("\\x{}", hex)
        }
    }
}
