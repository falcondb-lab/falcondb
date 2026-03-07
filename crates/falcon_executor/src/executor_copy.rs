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
                "Invalid UTF-8 in COPY data: {e}"
            )))
        })?;

        let mut rows_inserted: u64 = 0;

        let default_values: Vec<Datum> = schema
            .columns
            .iter()
            .map(|c| c.default_value.clone().unwrap_or(Datum::Null))
            .collect();

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());

        // Pre-scan existing rows once for UNIQUE checks
        let mut unique_sets: Vec<std::collections::HashSet<Vec<Datum>>> =
            if !schema.unique_constraints.is_empty() {
                let existing = self
                    .storage
                    .scan(table_id, txn.txn_id, read_ts)
                    .map_err(FalconError::Storage)?;
                schema
                    .unique_constraints
                    .iter()
                    .map(|uniq_cols| {
                        existing
                            .iter()
                            .filter_map(|(_, row)| {
                                let key: Vec<Datum> = uniq_cols
                                    .iter()
                                    .map(|&idx| row.values[idx].clone())
                                    .collect();
                                if key.iter().any(Datum::is_null) {
                                    None
                                } else {
                                    Some(key)
                                }
                            })
                            .collect()
                    })
                    .collect()
            } else {
                vec![]
            };

        // Pre-build FK lookup sets once
        let fk_lookup: Vec<(Vec<usize>, std::collections::HashSet<Vec<Datum>>)> =
            if !schema.foreign_keys.is_empty() {
                schema
                    .foreign_keys
                    .iter()
                    .map(|fk| {
                        let ref_schema =
                            self.storage
                                .get_table_schema(&fk.ref_table)
                                .ok_or_else(|| {
                                    FalconError::Execution(ExecutionError::TypeError(format!(
                                        "Referenced table '{}' not found",
                                        fk.ref_table
                                    )))
                                })?;
                        let ref_col_indices: Vec<usize> = fk
                            .ref_columns
                            .iter()
                            .map(|name| {
                                ref_schema.find_column(name).ok_or_else(|| {
                                    FalconError::Execution(ExecutionError::TypeError(format!(
                                        "Referenced column '{}' not found in '{}'",
                                        name, fk.ref_table
                                    )))
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        let ref_rows = self
                            .storage
                            .scan(ref_schema.id, txn.txn_id, read_ts)
                            .map_err(FalconError::Storage)?;
                        let fk_set: std::collections::HashSet<Vec<Datum>> = ref_rows
                            .iter()
                            .map(|(_, r)| {
                                ref_col_indices
                                    .iter()
                                    .map(|&idx| r.values[idx].clone())
                                    .collect()
                            })
                            .collect();
                        Ok((fk.columns.clone(), fk_set))
                    })
                    .collect::<Result<Vec<_>, FalconError>>()?
            } else {
                vec![]
            };

        for (line_idx, line) in text.lines().enumerate() {
            if header && line_idx == 0 {
                continue;
            }
            if line.is_empty() {
                continue;
            }

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

            let mut values: Vec<Datum> = default_values.clone();

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

            // Auto-fill SERIAL columns still NULL
            for (col_idx, col) in schema.columns.iter().enumerate() {
                if col.is_serial && values[col_idx].is_null() {
                    let next_val = self
                        .storage
                        .next_serial_value(&schema.name, col_idx)
                        .map_err(FalconError::Storage)?;
                    values[col_idx] =
                        if col.data_type == DataType::Int64 {
                            Datum::Int64(next_val)
                        } else {
                            Datum::Int32(i32::try_from(next_val).map_err(|_| {
                                FalconError::Execution(ExecutionError::NumericOverflow)
                            })?)
                        };
                }
            }

            // Dynamic defaults (CURRENT_TIMESTAMP, nextval, etc.)
            Self::eval_dynamic_defaults(schema, &mut values);
            for (&col_idx, dfn) in &schema.dynamic_defaults {
                if let falcon_common::schema::DefaultFn::Nextval(ref seq_name) = dfn {
                    if col_idx < values.len() && values[col_idx].is_null() {
                        let next_val = self
                            .storage
                            .sequence_nextval(seq_name)
                            .map_err(FalconError::Storage)?;
                        values[col_idx] = Datum::Int64(next_val);
                    }
                }
            }

            // Enforce VARCHAR(n)/CHAR(n) max length
            Self::enforce_max_length(schema, &values)?;

            // NOT NULL
            for (col_idx, col) in schema.columns.iter().enumerate() {
                if !col.nullable && values[col_idx].is_null() {
                    return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                        "COPY: NOT NULL constraint violated for column '{}'",
                        col.name,
                    ))));
                }
            }

            // Build row once — check constraints directly (avoids clone)
            let row = OwnedRow::new(values);
            self.eval_check_constraints(schema, &row)?;

            // UNIQUE
            for (set_idx, uniq_cols) in schema.unique_constraints.iter().enumerate() {
                let key: Vec<Datum> = uniq_cols
                    .iter()
                    .map(|&idx| row.values[idx].clone())
                    .collect();
                if key.iter().any(Datum::is_null) {
                    continue;
                }
                if !unique_sets[set_idx].insert(key) {
                    let col_names: Vec<&str> = uniq_cols
                        .iter()
                        .map(|&idx| schema.columns[idx].name.as_str())
                        .collect();
                    return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                        "COPY: UNIQUE constraint violated on column(s): {}",
                        col_names.join(", ")
                    ))));
                }
            }

            // FK
            for (fk_cols, fk_set) in &fk_lookup {
                let fk_vals: Vec<Datum> =
                    fk_cols.iter().map(|&idx| row.values[idx].clone()).collect();
                if fk_vals.iter().any(Datum::is_null) {
                    continue;
                }
                if !fk_set.contains(&fk_vals) {
                    return Err(FalconError::Execution(ExecutionError::TypeError(
                        "COPY: FOREIGN KEY constraint violated".into(),
                    )));
                }
            }
            self.storage
                .insert(table_id, row, txn.txn_id)
                .map_err(FalconError::Storage)?;
            rows_inserted += 1;
        }

        Ok(ExecutionResult::Dml {
            rows_affected: rows_inserted,
            tag: "COPY",
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

        let mut result_rows: Vec<OwnedRow> = Vec::with_capacity(rows.len() + 1);
        let delim_str = delimiter.to_string();

        // Header row
        if header {
            let header_fields: Vec<String> = columns.iter().map(|(name, _)| name.clone()).collect();
            let mut line = if csv {
                format_csv_line(&header_fields, delimiter, quote, escape)
            } else {
                header_fields.join(&delim_str)
            };
            line.push('\n');
            result_rows.push(OwnedRow::new(vec![Datum::Text(line)]));
        }

        // Reusable fields buffer for non-CSV text mode
        let mut fields_buf: Vec<String> = Vec::with_capacity(columns.len());
        let mut line_buf = String::with_capacity(256);
        for row in &rows {
            fields_buf.clear();
            for datum in &row.values {
                if datum.is_null() {
                    fields_buf.push(null_string.to_owned());
                } else {
                    fields_buf.push(datum_to_text(datum));
                }
            }
            line_buf.clear();
            if csv {
                line_buf.push_str(&format_csv_line(&fields_buf, delimiter, quote, escape));
            } else {
                for (i, f) in fields_buf.iter().enumerate() {
                    if i > 0 {
                        line_buf.push_str(&delim_str);
                    }
                    line_buf.push_str(f);
                }
            }
            line_buf.push('\n');
            result_rows.push(OwnedRow::new(vec![Datum::Text(line_buf.clone())]));
        }

        let result_columns = vec![("copy_data".into(), DataType::Text)];
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

        let mut result_rows: Vec<OwnedRow> = Vec::with_capacity(rows.len() + 1);
        let delim_str = delimiter.to_string();

        // Header row
        if header {
            let header_fields: Vec<String> = columns
                .iter()
                .map(|&i| schema.columns[i].name.clone())
                .collect();
            let mut line = if csv {
                format_csv_line(&header_fields, delimiter, quote, escape)
            } else {
                header_fields.join(&delim_str)
            };
            line.push('\n');
            result_rows.push(OwnedRow::new(vec![Datum::Text(line)]));
        }

        // Reuse fields and line buffers across rows
        let mut fields_buf: Vec<String> = Vec::with_capacity(columns.len());
        let mut line_buf = String::with_capacity(256);
        for (_pk, row) in &rows {
            fields_buf.clear();
            for &i in columns {
                let datum = &row.values[i];
                if datum.is_null() {
                    fields_buf.push(null_string.to_owned());
                } else {
                    fields_buf.push(datum_to_text(datum));
                }
            }
            line_buf.clear();
            if csv {
                line_buf.push_str(&format_csv_line(&fields_buf, delimiter, quote, escape));
            } else {
                for (j, f) in fields_buf.iter().enumerate() {
                    if j > 0 {
                        line_buf.push_str(&delim_str);
                    }
                    line_buf.push_str(f);
                }
            }
            line_buf.push('\n');
            result_rows.push(OwnedRow::new(vec![Datum::Text(line_buf.clone())]));
        }

        // Return the formatted lines as a special Query result.
        // The handler will convert these into CopyData messages.
        let result_columns = vec![("copy_data".into(), DataType::Text)];
        Ok(ExecutionResult::Query {
            columns: result_columns,
            rows: result_rows,
        })
    }
}

/// Parse a text-format line (tab-delimited by default).
fn parse_text_line(line: &str, delimiter: char) -> Vec<String> {
    line.split(delimiter)
        .map(std::string::ToString::to_string)
        .collect()
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
                let escaped = f.replace(quote, &format!("{escape}{quote}"));
                format!("{quote}{escaped}{quote}")
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
        DataType::Int16 => field
            .parse::<i16>()
            .map(|v| Datum::Int32(v as i32))
            .map_err(|e| format!("Cannot parse '{field}' as SMALLINT: {e}")),
        DataType::Int32 => field
            .parse::<i32>()
            .map(Datum::Int32)
            .map_err(|e| format!("Cannot parse '{field}' as INT: {e}")),
        DataType::Int64 => field
            .parse::<i64>()
            .map(Datum::Int64)
            .map_err(|e| format!("Cannot parse '{field}' as BIGINT: {e}")),
        DataType::Float32 => field
            .parse::<f32>()
            .map(|v| Datum::Float64(v as f64))
            .map_err(|e| format!("Cannot parse '{field}' as REAL: {e}")),
        DataType::Float64 => field
            .parse::<f64>()
            .map(Datum::Float64)
            .map_err(|e| format!("Cannot parse '{field}' as FLOAT: {e}")),
        DataType::Boolean => match field.to_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "on" => Ok(Datum::Boolean(true)),
            "f" | "false" | "0" | "no" | "off" => Ok(Datum::Boolean(false)),
            _ => Err(format!("Cannot parse '{field}' as BOOLEAN")),
        },
        DataType::Text => Ok(Datum::Text(field.to_owned())),
        DataType::Timestamp => {
            use chrono::NaiveDateTime;
            let dt = NaiveDateTime::parse_from_str(field, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(field, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| NaiveDateTime::parse_from_str(field, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| NaiveDateTime::parse_from_str(field, "%Y-%m-%dT%H:%M:%S%.f"))
                .map_err(|e| format!("Cannot parse '{field}' as TIMESTAMP: {e}"))?;
            Ok(Datum::Timestamp(dt.and_utc().timestamp_micros()))
        }
        DataType::Date => {
            use chrono::NaiveDate;
            let date = NaiveDate::parse_from_str(field, "%Y-%m-%d")
                .map_err(|e| format!("Cannot parse '{field}' as DATE: {e}"))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap_or_else(|| NaiveDate::from_ymd_opt(2000, 1, 1).unwrap_or(NaiveDate::MIN));
            let days = i32::try_from((date - epoch).num_days())
                .map_err(|_| "Date value out of range".to_string())?;
            Ok(Datum::Date(days))
        }
        DataType::Jsonb => {
            let v: serde_json::Value = serde_json::from_str(field)
                .map_err(|e| format!("Cannot parse '{field}' as JSONB: {e}"))?;
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
                    .map(|s| Datum::Text(s.trim().to_owned()))
                    .collect();
                Ok(Datum::Array(elements))
            } else {
                Err(format!("Cannot parse '{field}' as ARRAY"))
            }
        }
        DataType::Decimal(_, _) => {
            Datum::parse_decimal(field).ok_or_else(|| format!("Cannot parse '{field}' as DECIMAL"))
        }
        DataType::Time => {
            // Parse HH:MM:SS or HH:MM:SS.ffffff
            let parts: Vec<&str> = field.split(':').collect();
            if parts.len() < 3 {
                return Err(format!("Cannot parse '{field}' as TIME"));
            }
            let h: i64 = parts[0]
                .parse()
                .map_err(|_| format!("Cannot parse '{field}' as TIME"))?;
            let m: i64 = parts[1]
                .parse()
                .map_err(|_| format!("Cannot parse '{field}' as TIME"))?;
            let sec_parts: Vec<&str> = parts[2].split('.').collect();
            let s: i64 = sec_parts[0]
                .parse()
                .map_err(|_| format!("Cannot parse '{field}' as TIME"))?;
            let frac: i64 = if sec_parts.len() > 1 {
                let f = sec_parts[1];
                let padded = format!("{:0<6}", &f[..f.len().min(6)]);
                padded
                    .parse()
                    .map_err(|_| format!("Cannot parse '{field}' as TIME"))?
            } else {
                0
            };
            let us = h
                .checked_mul(3_600_000_000)
                .and_then(|v| v.checked_add(m.checked_mul(60_000_000)?))
                .and_then(|v| v.checked_add(s.checked_mul(1_000_000)?))
                .and_then(|v| v.checked_add(frac))
                .ok_or_else(|| format!("TIME value out of range: '{field}'"))?;
            Ok(Datum::Time(us))
        }
        DataType::Interval => {
            // Simplified: just store as text-parsed microseconds
            Ok(Datum::Text(field.to_owned()))
        }
        DataType::Uuid => {
            let hex: String = field.chars().filter(char::is_ascii_hexdigit).collect();
            if hex.len() != 32 {
                return Err(format!("Cannot parse '{field}' as UUID"));
            }
            let v = u128::from_str_radix(&hex, 16)
                .map_err(|e| format!("Cannot parse '{field}' as UUID: {e}"))?;
            Ok(Datum::Uuid(v))
        }
        DataType::Bytea => {
            // Accept PG hex format: \x<hex> or raw hex string
            let hex_str = field.strip_prefix("\\x").unwrap_or(field);
            let bytes = (0..hex_str.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex_str[i..(i + 2).min(hex_str.len())], 16))
                .collect::<Result<Vec<u8>, _>>()
                .map_err(|e| format!("Cannot parse '{field}' as BYTEA: {e}"))?;
            Ok(Datum::Bytea(bytes))
        }
        DataType::TsVector | DataType::TsQuery => Ok(Datum::Text(field.to_owned())),
    }
}

/// Convert a Datum to its text representation for COPY output.
fn datum_to_text(datum: &Datum) -> String {
    match datum {
        Datum::Null => String::new(),
        Datum::Int32(v) => v.to_string(),
        Datum::Int64(v) => v.to_string(),
        Datum::Float64(v) => v.to_string(),
        Datum::Boolean(v) => if *v { "t" } else { "f" }.to_owned(),
        Datum::Text(s) => s.clone(),
        Datum::Timestamp(us) => {
            let secs = us / 1_000_000;
            let nsecs = ((us % 1_000_000).abs() * 1000) as u32;
            chrono::DateTime::from_timestamp(secs, nsecs).map_or_else(
                || us.to_string(),
                |dt| dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            )
        }
        Datum::Date(days) => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_else(|| {
                chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap_or(chrono::NaiveDate::MIN)
            });
            epoch
                .checked_add_signed(chrono::Duration::days(i64::from(*days)))
                .map_or_else(
                    || days.to_string(),
                    |date| date.format("%Y-%m-%d").to_string(),
                )
        }
        Datum::Jsonb(v) => v.to_string(),
        Datum::Array(elements) => {
            let inner: Vec<String> = elements.iter().map(datum_to_text).collect();
            format!("{{{}}}", inner.join(","))
        }
        Datum::Decimal(m, s) => falcon_common::datum::decimal_to_string(*m, *s),
        Datum::Time(_) | Datum::Interval(_, _, _) | Datum::Uuid(_) => format!("{datum}"),
        Datum::Bytea(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            format!("\\x{hex}")
        }
        Datum::TsVector(_) | Datum::TsQuery(_) => format!("{datum}"),
    }
}
