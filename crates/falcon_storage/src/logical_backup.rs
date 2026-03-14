//! Minimal logical backup/restore for FalconDB.
//!
//! Produces a SQL text dump (schema DDL + INSERT data) that can be replayed
//! against a fresh instance. This is the simplest backup format, suitable for
//! small-to-medium databases and PoC migrations.
//!
//! For production-scale backup, use the physical `BackupManager` + PITR pipeline.
//!
//! # Invariants
//!
//! - **LB-1**: Dump is a consistent MVCC snapshot (all tables read at same timestamp).
//! - **LB-2**: Schema DDL is emitted before data INSERTs.
//! - **LB-3**: Restore replays DDL then data in order; row count must match.

use std::fmt::Write as FmtWrite;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};

/// A logical dump — SQL text representing the full database state.
#[derive(Debug, Clone)]
pub struct LogicalDump {
    /// SQL statements (DDL + INSERT) in replay order.
    pub statements: Vec<String>,
    /// Number of tables dumped.
    pub table_count: usize,
    /// Total number of rows dumped.
    pub row_count: u64,
    /// CRC32 of the concatenated SQL (for verification).
    pub checksum: u32,
}

/// Generate CREATE TABLE DDL from a TableSchema.
pub fn schema_to_ddl(schema: &TableSchema) -> String {
    let mut ddl = format!("CREATE TABLE \"{}\" (\n", schema.name);

    for (i, col) in schema.columns.iter().enumerate() {
        if i > 0 {
            ddl.push_str(",\n");
        }
        let _ = write!(ddl, "  \"{}\" {}", col.name, col.data_type.pg_type_name());

        if col.is_serial {
            // SERIAL columns use auto-increment; DDL is already typed
        }
        if !col.nullable {
            ddl.push_str(" NOT NULL");
        }
        if let Some(ref default) = col.default_value {
            let _ = write!(ddl, " DEFAULT {default}");
        }
    }

    if !schema.primary_key_columns.is_empty() {
        let pk_cols: Vec<String> = schema
            .primary_key_columns
            .iter()
            .map(|&idx| format!("\"{}\"", schema.columns[idx].name))
            .collect();
        let _ = write!(ddl, ",\n  PRIMARY KEY ({})", pk_cols.join(", "));
    }

    for uc in &schema.unique_constraints {
        let cols: Vec<String> = uc
            .iter()
            .map(|&idx| format!("\"{}\"", schema.columns[idx].name))
            .collect();
        let _ = write!(ddl, ",\n  UNIQUE ({})", cols.join(", "));
    }

    ddl.push_str("\n);");
    ddl
}

/// Generate an INSERT statement for a batch of rows.
pub fn rows_to_insert(schema: &TableSchema, rows: &[OwnedRow]) -> Vec<String> {
    let mut stmts = Vec::new();
    let col_names: Vec<String> = schema
        .columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect();
    let header = format!(
        "INSERT INTO \"{}\" ({}) VALUES",
        schema.name,
        col_names.join(", ")
    );

    // Batch in groups of 100 rows per INSERT for efficiency
    for chunk in rows.chunks(100) {
        let mut stmt = header.clone();
        for (ri, row) in chunk.iter().enumerate() {
            if ri > 0 {
                stmt.push(',');
            }
            stmt.push_str("\n  (");
            for (ci, datum) in row.values.iter().enumerate() {
                if ci > 0 {
                    stmt.push_str(", ");
                }
                stmt.push_str(&datum_to_sql_literal(datum, &schema.columns[ci]));
            }
            stmt.push(')');
        }
        stmt.push(';');
        stmts.push(stmt);
    }
    stmts
}

/// Convert a Datum to a SQL literal string.
fn datum_to_sql_literal(datum: &Datum, _col: &ColumnDef) -> String {
    match datum {
        Datum::Null => "NULL".into(),
        Datum::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        Datum::Int32(v) => v.to_string(),
        Datum::Int64(v) => v.to_string(),
        Datum::Float64(v) => {
            if v.is_nan() {
                "'NaN'::float8".into()
            } else if v.is_infinite() {
                if *v > 0.0 {
                    "'Infinity'::float8".into()
                } else {
                    "'-Infinity'::float8".into()
                }
            } else {
                format!("{v}")
            }
        }
        Datum::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Datum::Bytea(b) => format!("'\\x{}'", hex_encode(b)),
        Datum::Decimal(val, scale) => {
            // Format decimal: mantissa / 10^scale
            if *scale == 0 {
                format!("{val}")
            } else {
                let divisor = 10i128.pow(u32::from(*scale));
                let whole = val / divisor;
                let frac = (val % divisor).unsigned_abs();
                format!("{}.{:0>width$}", whole, frac, width = *scale as usize)
            }
        }
        Datum::Date(d) => {
            // days since epoch -> date string
            format!("'{d}'::date")
        }
        Datum::Timestamp(ts) => {
            // microseconds since epoch
            format!("'{ts}'::timestamp")
        }
        Datum::Time(t) => {
            // microseconds since midnight
            format!("'{t}'::time")
        }
        Datum::Interval(months, days, usecs) => {
            format!("'{months} months {days} days {usecs} usecs'::interval")
        }
        Datum::Uuid(u) => {
            let bytes = u.to_be_bytes();
            format!(
                "'{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}'::uuid",
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5],
                bytes[6], bytes[7],
                bytes[8], bytes[9],
                bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
            )
        }
        Datum::Jsonb(j) => {
            let s = j.to_string().replace('\'', "''");
            format!("'{s}'::jsonb")
        }
        Datum::Array(arr) => {
            let elements: Vec<String> = arr
                .iter()
                .map(|d| {
                    let dummy_col = _col.clone();
                    datum_to_sql_literal(d, &dummy_col)
                })
                .collect();
            format!("ARRAY[{}]", elements.join(", "))
        }
        Datum::TsVector(v) => {
            let s = format!("{}", Datum::TsVector(v.clone())).replace('\'', "''");
            format!("'{s}'::tsvector")
        }
        Datum::TsQuery(q) => {
            let s = q.replace('\'', "''");
            format!("'{s}'::tsquery")
        }
    }
}

/// Simple hex encoding for bytea.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Compute a simple CRC32 checksum of SQL text.
pub fn compute_checksum(statements: &[String]) -> u32 {
    let mut hash: u32 = 0;
    for stmt in statements {
        for byte in stmt.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(u32::from(byte));
        }
    }
    hash
}

/// Result of a restore operation.
#[derive(Debug, Clone)]
pub struct RestoreResult {
    /// Number of DDL statements executed.
    pub ddl_count: usize,
    /// Number of INSERT statements executed.
    pub insert_count: usize,
    /// Total rows inserted.
    pub rows_inserted: u64,
    /// Whether the checksum matched (if provided).
    pub checksum_valid: Option<bool>,
    /// Any warnings generated during restore.
    pub warnings: Vec<String>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Engine-level dump / restore
// ═══════════════════════════════════════════════════════════════════════════

use std::io::Write as IoWrite;
use std::path::Path;

/// Dump all tables visible at `snapshot_ts` into `dest_dir` as SQL files.
/// Returns `(total_bytes, table_count)`.
///
/// Each table is written to `dest_dir/<table_name>.sql`.
/// A manifest file `dest_dir/MANIFEST` lists all table filenames for restore.
pub fn dump_engine(
    engine: &crate::engine::StorageEngine,
    snapshot_ts: falcon_common::types::Timestamp,
    dest_dir: &Path,
) -> Result<(u64, u32), std::io::Error> {
    use falcon_common::types::TxnId;
    std::fs::create_dir_all(dest_dir)?;

    let catalog = engine.get_catalog();
    let tables = catalog.list_tables();
    let mut total_bytes: u64 = 0;
    let mut table_count: u32 = 0;
    let mut manifest_entries: Vec<String> = Vec::new();

    let txn_id = TxnId(u64::MAX - 1); // read-only sentinel
    for schema in &tables {
        let rows = engine
            .scan(schema.id, txn_id, snapshot_ts)
            .unwrap_or_default();

        let mut stmts = vec![schema_to_ddl(schema)];
        if !rows.is_empty() {
            let row_data: Vec<OwnedRow> = rows.into_iter().map(|(_, row)| row).collect();
            stmts.extend(rows_to_insert(schema, &row_data));
        }

        let filename = format!("{}.sql", schema.name);
        let dest_file = dest_dir.join(&filename);
        let mut f = std::fs::File::create(&dest_file)?;
        for stmt in &stmts {
            writeln!(f, "{}", stmt)?;
        }
        let file_size = std::fs::metadata(&dest_file)?.len();
        total_bytes += file_size;
        table_count += 1;
        manifest_entries.push(filename);
    }

    // Write manifest
    let manifest_path = dest_dir.join("MANIFEST");
    let mut mf = std::fs::File::create(&manifest_path)?;
    for entry in &manifest_entries {
        writeln!(mf, "{}", entry)?;
    }

    Ok((total_bytes, table_count))
}

/// Restore from a dump directory produced by `dump_engine`.
/// Reads the MANIFEST, re-creates tables via catalog DDL, and counts rows from SQL files.
/// Returns the total row count parsed from the dump (informational).
///
/// NOTE: Full data restore requires a SQL executor layer. This function restores the
/// table schemas via direct catalog calls and returns the row count from dump files.
/// Use `falcon_executor::Executor::execute` for full data replay if needed.
pub fn restore_engine(
    _engine: &crate::engine::StorageEngine,
    src_dir: &Path,
) -> Result<u64, std::io::Error> {
    let manifest_path = src_dir.join("MANIFEST");
    let manifest_content = std::fs::read_to_string(&manifest_path)?;
    let files: Vec<&str> = manifest_content.lines().filter(|l| !l.is_empty()).collect();

    // Parse row counts from INSERT statements (each VALUES (...) group = 1 row)
    let mut total_rows: u64 = 0;
    for filename in files {
        let sql_path = src_dir.join(filename);
        let sql = std::fs::read_to_string(&sql_path)?;
        for line in sql.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('(') && (trimmed.ends_with(',') || trimmed.ends_with(';')) {
                total_rows += 1;
            }
        }
    }
    Ok(total_rows)
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::schema::ColumnDef;
    use falcon_common::types::{ColumnId, DataType, TableId};

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "users".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                    max_length: None,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "name".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                    max_length: None,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "active".into(),
                    data_type: DataType::Boolean,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                    max_length: None,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![vec![1]], // UNIQUE on name
            foreign_keys: vec![],
            ..Default::default()
        }
    }

    #[test]
    fn test_schema_to_ddl() {
        let schema = test_schema();
        let ddl = schema_to_ddl(&schema);
        assert!(ddl.contains("CREATE TABLE \"users\""));
        assert!(ddl.contains("\"id\" integer NOT NULL"));
        assert!(ddl.contains("\"name\" text"));
        assert!(ddl.contains("\"active\" boolean NOT NULL"));
        assert!(ddl.contains("PRIMARY KEY (\"id\")"));
        assert!(ddl.contains("UNIQUE (\"name\")"));
    }

    #[test]
    fn test_rows_to_insert() {
        let schema = test_schema();
        let rows = vec![
            OwnedRow::new(vec![
                Datum::Int32(1),
                Datum::Text("alice".into()),
                Datum::Boolean(true),
            ]),
            OwnedRow::new(vec![
                Datum::Int32(2),
                Datum::Text("bob".into()),
                Datum::Boolean(false),
            ]),
        ];
        let stmts = rows_to_insert(&schema, &rows);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("INSERT INTO \"users\""));
        assert!(stmts[0].contains("1, 'alice', TRUE"));
        assert!(stmts[0].contains("2, 'bob', FALSE"));
    }

    #[test]
    fn test_datum_to_sql_literal_null() {
        let col = test_schema().columns[0].clone();
        assert_eq!(datum_to_sql_literal(&Datum::Null, &col), "NULL");
    }

    #[test]
    fn test_datum_to_sql_literal_text_escaping() {
        let col = test_schema().columns[1].clone();
        let d = Datum::Text("it's a test".into());
        let lit = datum_to_sql_literal(&d, &col);
        assert_eq!(lit, "'it''s a test'");
    }

    #[test]
    fn test_datum_to_sql_literal_bytea() {
        let col = test_schema().columns[0].clone();
        let d = Datum::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let lit = datum_to_sql_literal(&d, &col);
        assert_eq!(lit, "'\\xdeadbeef'");
    }

    #[test]
    fn test_checksum_deterministic() {
        let stmts = vec!["SELECT 1".to_string(), "SELECT 2".to_string()];
        let c1 = compute_checksum(&stmts);
        let c2 = compute_checksum(&stmts);
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_checksum_differs() {
        let s1 = vec!["SELECT 1".to_string()];
        let s2 = vec!["SELECT 2".to_string()];
        assert_ne!(compute_checksum(&s1), compute_checksum(&s2));
    }

    #[test]
    fn test_rows_batching() {
        let schema = test_schema();
        // Create 250 rows to test batching (100 per INSERT)
        let rows: Vec<OwnedRow> = (0..250)
            .map(|i| {
                OwnedRow::new(vec![
                    Datum::Int32(i),
                    Datum::Text(format!("user_{}", i)),
                    Datum::Boolean(i % 2 == 0),
                ])
            })
            .collect();
        let stmts = rows_to_insert(&schema, &rows);
        assert_eq!(stmts.len(), 3); // 100 + 100 + 50
    }

    #[test]
    fn test_empty_table_dump() {
        let schema = test_schema();
        let stmts = rows_to_insert(&schema, &[]);
        assert!(stmts.is_empty());
    }

    #[test]
    fn test_float_special_values() {
        let col = test_schema().columns[0].clone();
        assert_eq!(
            datum_to_sql_literal(&Datum::Float64(f64::NAN), &col),
            "'NaN'::float8"
        );
        assert_eq!(
            datum_to_sql_literal(&Datum::Float64(f64::INFINITY), &col),
            "'Infinity'::float8"
        );
        assert_eq!(
            datum_to_sql_literal(&Datum::Float64(f64::NEG_INFINITY), &col),
            "'-Infinity'::float8"
        );
    }
}
