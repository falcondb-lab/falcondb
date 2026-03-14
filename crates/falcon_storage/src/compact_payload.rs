//! # Compact Row Payload Encoding
//!
//! Replaces the `Vec<Datum>` (N × 40 bytes) row representation with a dense
//! binary format that stores fixed-length columns inline and variable-length
//! columns in a trailing payload section.
//!
//! ## Wire Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ Header                                                          │
//! │   col_count : u16          (2 bytes, offset 0)                 │
//! │   null_bitmap: [u8; ⌈N/8⌉] (N bits, 1 = null)                 │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ Fixed-length section  (schema-ordered, only non-null columns)  │
//! │   Boolean   → 1 byte                                           │
//! │   Int32     → 4 bytes (little-endian)                          │
//! │   Date      → 4 bytes (little-endian i32)                      │
//! │   Int64     → 8 bytes (little-endian)                          │
//! │   Float64   → 8 bytes (little-endian IEEE 754)                 │
//! │   Timestamp → 8 bytes (little-endian i64 µs)                   │
//! │   Time      → 8 bytes (little-endian i64 µs)                   │
//! │   Decimal   → 17 bytes (1 byte scale + 16 bytes i128 LE)       │
//! │   Interval  → 16 bytes (4+4+8 LE)                              │
//! │   Uuid      → 16 bytes (u128 big-endian)                       │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ Varlen offset table   (only for varlen cols, schema-ordered)   │
//! │   per varlen col: u32 offset from start of varlen data section │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ Varlen data section   (contiguous bytes, no padding)           │
//! │   Text   → raw UTF-8 bytes (length derived from offset delta)  │
//! │   Bytea  → raw bytes                                           │
//! │   Array  → bincode-serialised Vec<Datum>                       │
//! │   Jsonb  → serde_json compact bytes                            │
//! │   TsVector / TsQuery → bincode serialised                      │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! For a typical 8-column INT64 OLTP row this reduces per-row memory from
//! ~320 bytes (`Vec<Datum>`) to **64 bytes** (8 × 8 bytes), an 80% reduction.
//!
//! ## Design Constraints
//! - Schema-driven: encoding/decoding always requires a `&TableSchema`.
//! - The schema column order determines slot order.
//! - No padding bytes are inserted (tight packing for cache density).
//! - Null values occupy zero bytes in the fixed/varlen sections; the null
//!   bitmap is the only overhead for null columns.
//! - The encoding is NOT self-describing: you cannot decode without a schema.
//! - The format is stable within a minor version; schema evolution is handled
//!   by the WAL and DDL migration layers above this module.

use std::mem;

use falcon_common::datum::Datum;
use falcon_common::schema::TableSchema;
use falcon_common::types::DataType;
use smallvec::SmallVec;

/// A compact, densely-packed row buffer.
///
/// Backed by `SmallVec<[u8; 128]>` so rows up to 128 bytes need zero heap
/// allocations.  Rows with large varlen columns spill to the heap transparently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactRow(pub SmallVec<[u8; 128]>);

impl CompactRow {
    /// Returns the raw byte slice of this compact row.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the byte length of the encoded row.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<[u8]> for CompactRow {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

// ── Encoding ──────────────────────────────────────────────────────────────────

/// Encode an `OwnedRow` into a `CompactRow` using the given table schema.
///
/// Columns that do not appear in the row (`row.get(i)` returns `None` or the
/// row is shorter than the schema) are treated as `Null`.
pub fn encode(row: &[Datum], schema: &TableSchema) -> CompactRow {
    let ncols = schema.columns.len();
    let bitmap_bytes = (ncols + 7) / 8;

    // Count varlen columns for offset table sizing.
    let varlen_count = schema
        .columns
        .iter()
        .filter(|c| is_varlen(&c.data_type))
        .count();

    // Pre-allocate: header + bitmap + fixed section estimate + offset table.
    let fixed_estimate: usize = schema.columns.iter().map(|c| fixed_size(&c.data_type)).sum();
    let capacity = 2 + bitmap_bytes + fixed_estimate + varlen_count * 4 + 64;
    let mut buf: SmallVec<[u8; 128]> = SmallVec::with_capacity(capacity);

    // ── Header ────────────────────────────────────────────────────────────────
    buf.extend_from_slice(&(ncols as u16).to_le_bytes());

    // ── Null bitmap ───────────────────────────────────────────────────────────
    let bitmap_start = buf.len();
    buf.resize(bitmap_start + bitmap_bytes, 0u8);
    for (i, col) in schema.columns.iter().enumerate() {
        let datum = row.get(i).unwrap_or(&Datum::Null);
        if is_null_datum(datum, col.nullable) {
            buf[bitmap_start + i / 8] |= 1 << (i % 8);
        }
    }

    // ── Fixed-length section ──────────────────────────────────────────────────
    for (i, col) in schema.columns.iter().enumerate() {
        if is_varlen(&col.data_type) {
            continue;
        }
        let datum = row.get(i).unwrap_or(&Datum::Null);
        if is_null_datum(datum, col.nullable) {
            continue; // null: no bytes emitted
        }
        encode_fixed(datum, &mut buf);
    }

    // ── Varlen offset table + data ────────────────────────────────────────────
    if varlen_count > 0 {
        // Reserve space for offset table (varlen_count × u32).
        let offset_table_start = buf.len();
        buf.resize(offset_table_start + varlen_count * 4, 0u8);

        let mut vl_idx = 0usize;
        let mut data_buf: SmallVec<[u8; 64]> = SmallVec::new();

        for (i, col) in schema.columns.iter().enumerate() {
            if !is_varlen(&col.data_type) {
                continue;
            }
            let datum = row.get(i).unwrap_or(&Datum::Null);
            // Write the byte offset into the offset table BEFORE appending data.
            let offset = data_buf.len() as u32;
            let table_pos = offset_table_start + vl_idx * 4;
            buf[table_pos..table_pos + 4].copy_from_slice(&offset.to_le_bytes());

            if !is_null_datum(datum, col.nullable) {
                encode_varlen(datum, &mut data_buf);
            }
            // null varlen: offset == next offset, zero-length entry.
            vl_idx += 1;
        }
        buf.extend_from_slice(&data_buf);
    }

    CompactRow(buf)
}

/// Decode a `CompactRow` back into a `Vec<Datum>` (i.e. `OwnedRow`).
///
/// Returns `Err` if the buffer is malformed.
pub fn decode(bytes: &[u8], schema: &TableSchema) -> Result<Vec<Datum>, DecodeError> {
    if bytes.len() < 2 {
        return Err(DecodeError::TooShort);
    }
    let ncols = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    if ncols != schema.columns.len() {
        return Err(DecodeError::ColCountMismatch { encoded: ncols, schema: schema.columns.len() });
    }
    let bitmap_bytes = (ncols + 7) / 8;
    if bytes.len() < 2 + bitmap_bytes {
        return Err(DecodeError::TooShort);
    }

    let bitmap = &bytes[2..2 + bitmap_bytes];
    let mut cursor = 2 + bitmap_bytes;
    let mut row = Vec::with_capacity(ncols);

    // ── Fixed-length decode ───────────────────────────────────────────────────
    let mut fixed_datums: Vec<Option<Datum>> = Vec::with_capacity(ncols);
    for (i, col) in schema.columns.iter().enumerate() {
        if is_varlen(&col.data_type) {
            fixed_datums.push(None); // placeholder; varlen filled below
            continue;
        }
        if is_null_bit(bitmap, i) {
            fixed_datums.push(Some(Datum::Null));
            continue;
        }
        let (datum, consumed) = decode_fixed(&col.data_type, &bytes[cursor..])
            .ok_or(DecodeError::TooShort)?;
        cursor += consumed;
        fixed_datums.push(Some(datum));
    }

    // ── Varlen decode ─────────────────────────────────────────────────────────
    let varlen_count = schema.columns.iter().filter(|c| is_varlen(&c.data_type)).count();
    let offset_table_start = cursor;
    if varlen_count > 0 {
        if bytes.len() < offset_table_start + varlen_count * 4 {
            return Err(DecodeError::TooShort);
        }
        let varlen_data_start = offset_table_start + varlen_count * 4;
        let varlen_data = &bytes[varlen_data_start..];

        let mut vl_idx = 0usize;
        let mut varlen_datums: Vec<Option<Datum>> = vec![None; varlen_count];

        for (i, col) in schema.columns.iter().enumerate() {
            if !is_varlen(&col.data_type) {
                continue;
            }
            if is_null_bit(bitmap, i) {
                varlen_datums[vl_idx] = Some(Datum::Null);
                vl_idx += 1;
                continue;
            }
            let off_pos = offset_table_start + vl_idx * 4;
            let start = u32::from_le_bytes(bytes[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            let end = if vl_idx + 1 < varlen_count {
                let next_pos = off_pos + 4;
                u32::from_le_bytes(bytes[next_pos..next_pos + 4].try_into().unwrap()) as usize
            } else {
                varlen_data.len()
            };
            if end < start || end > varlen_data.len() {
                return Err(DecodeError::BadOffset);
            }
            let datum = decode_varlen(&col.data_type, &varlen_data[start..end])
                .ok_or(DecodeError::BadVarlen)?;
            varlen_datums[vl_idx] = Some(datum);
            vl_idx += 1;
        }

        // Merge fixed and varlen datums in schema order.
        let mut vl_iter = varlen_datums.into_iter();
        for (i, col) in schema.columns.iter().enumerate() {
            if is_varlen(&col.data_type) {
                row.push(vl_iter.next().unwrap().unwrap_or(Datum::Null));
            } else {
                row.push(fixed_datums[i].take().unwrap_or(Datum::Null));
            }
        }
    } else {
        // All fixed: just move datums into row.
        for d in fixed_datums {
            row.push(d.unwrap_or(Datum::Null));
        }
    }

    Ok(row)
}

/// Read a single column from a compact row **without decoding the full row**.
///
/// This is the zero-copy column projection path used by the executor hot path.
/// Returns `None` if `col_idx` is out of range or the buffer is malformed.
pub fn read_column(bytes: &[u8], schema: &TableSchema, col_idx: usize) -> Option<Datum> {
    if bytes.len() < 2 || col_idx >= schema.columns.len() {
        return None;
    }
    let ncols = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    if ncols != schema.columns.len() {
        return None;
    }
    let bitmap_bytes = (ncols + 7) / 8;
    if bytes.len() < 2 + bitmap_bytes {
        return None;
    }
    let bitmap = &bytes[2..2 + bitmap_bytes];

    // Null fast path.
    if is_null_bit(bitmap, col_idx) {
        return Some(Datum::Null);
    }

    let col = &schema.columns[col_idx];

    if is_varlen(&col.data_type) {
        // Must locate the varlen section.
        let mut fixed_cursor = 2 + bitmap_bytes;
        // Skip all fixed columns.
        for (i, c) in schema.columns.iter().enumerate() {
            if is_varlen(&c.data_type) {
                continue;
            }
            if !is_null_bit(bitmap, i) {
                fixed_cursor += fixed_size(&c.data_type);
            }
        }
        let offset_table_start = fixed_cursor;
        let varlen_count = schema.columns.iter().filter(|c| is_varlen(&c.data_type)).count();
        let varlen_data_start = offset_table_start + varlen_count * 4;
        if bytes.len() < varlen_data_start {
            return None;
        }
        let varlen_data = &bytes[varlen_data_start..];

        // Find which varlen slot this column is.
        let vl_idx = schema.columns[..col_idx]
            .iter()
            .filter(|c| is_varlen(&c.data_type))
            .count();
        let off_pos = offset_table_start + vl_idx * 4;
        let start = u32::from_le_bytes(bytes[off_pos..off_pos + 4].try_into().ok()?) as usize;
        let end = if vl_idx + 1 < varlen_count {
            u32::from_le_bytes(bytes[off_pos + 4..off_pos + 8].try_into().ok()?) as usize
        } else {
            varlen_data.len()
        };
        if end < start || end > varlen_data.len() {
            return None;
        }
        decode_varlen(&col.data_type, &varlen_data[start..end])
    } else {
        // Seek to this fixed column's offset.
        let mut fixed_cursor = 2 + bitmap_bytes;
        for (i, c) in schema.columns.iter().enumerate() {
            if i == col_idx {
                break;
            }
            if is_varlen(&c.data_type) {
                continue;
            }
            if !is_null_bit(bitmap, i) {
                fixed_cursor += fixed_size(&c.data_type);
            }
        }
        let (datum, _) = decode_fixed(&col.data_type, &bytes[fixed_cursor..])?;
        Some(datum)
    }
}

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    TooShort,
    ColCountMismatch { encoded: usize, schema: usize },
    BadOffset,
    BadVarlen,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort => write!(f, "compact row buffer too short"),
            Self::ColCountMismatch { encoded, schema } => {
                write!(f, "column count mismatch: encoded={encoded} schema={schema}")
            }
            Self::BadOffset => write!(f, "bad varlen offset in compact row"),
            Self::BadVarlen => write!(f, "failed to decode varlen payload"),
        }
    }
}

impl std::error::Error for DecodeError {}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Returns `true` if `col_idx` is marked null in the bitmap.
#[inline]
fn is_null_bit(bitmap: &[u8], col_idx: usize) -> bool {
    bitmap[col_idx / 8] & (1 << (col_idx % 8)) != 0
}

/// Returns `true` if the datum should be treated as null.
#[inline]
fn is_null_datum(datum: &Datum, _nullable: bool) -> bool {
    matches!(datum, Datum::Null)
}

/// Returns `true` if the DataType is variable-length (stored in the varlen section).
#[inline]
pub fn is_varlen(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Text
            | DataType::Bytea
            | DataType::Array(_)
            | DataType::Jsonb
            | DataType::TsVector
            | DataType::TsQuery
    )
}

/// Fixed-length byte size for fixed-width DataTypes.  Returns 0 for varlen.
#[inline]
pub fn fixed_size(dt: &DataType) -> usize {
    match dt {
        DataType::Boolean => 1,
        DataType::Int32 | DataType::Date => 4,
        DataType::Int64
        | DataType::Float64
        | DataType::Timestamp
        | DataType::Time => 8,
        DataType::Decimal(_, _) => 17, // 1 byte scale + 16 bytes i128 LE
        DataType::Interval => 16,      // 4+4+8
        DataType::Uuid => 16,
        _ => 0, // varlen
    }
}

/// Encode a fixed-length datum into `buf`.  Panics if datum/type mismatch.
fn encode_fixed(datum: &Datum, buf: &mut SmallVec<[u8; 128]>) {
    match datum {
        Datum::Boolean(b) => buf.push(if *b { 1 } else { 0 }),
        Datum::Int32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        Datum::Int64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        Datum::Float64(v) => buf.extend_from_slice(&v.to_bits().to_le_bytes()),
        Datum::Timestamp(v) => buf.extend_from_slice(&v.to_le_bytes()),
        Datum::Date(v) => buf.extend_from_slice(&v.to_le_bytes()),
        Datum::Time(v) => buf.extend_from_slice(&v.to_le_bytes()),
        Datum::Decimal(m, s) => {
            buf.push(*s);
            buf.extend_from_slice(&m.to_le_bytes());
        }
        Datum::Interval(months, days, us) => {
            buf.extend_from_slice(&months.to_le_bytes());
            buf.extend_from_slice(&days.to_le_bytes());
            buf.extend_from_slice(&us.to_le_bytes());
        }
        Datum::Uuid(v) => buf.extend_from_slice(&v.to_be_bytes()),
        Datum::Null => {} // null cols are skipped before calling this
        _ => {} // varlen types not handled here
    }
}

/// Encode a varlen datum into a staging buffer.
fn encode_varlen(datum: &Datum, buf: &mut SmallVec<[u8; 64]>) {
    match datum {
        Datum::Text(s) => buf.extend_from_slice(s.as_bytes()),
        Datum::Bytea(b) => buf.extend_from_slice(b),
        Datum::Array(elems) => {
            // Use bincode for arrays (typically rare in hot OLTP path).
            if let Ok(encoded) = bincode::serialize(elems) {
                buf.extend_from_slice(&encoded);
            }
        }
        Datum::Jsonb(v) => {
            let s = v.to_string();
            buf.extend_from_slice(s.as_bytes());
        }
        Datum::TsVector(v) => {
            if let Ok(encoded) = bincode::serialize(v) {
                buf.extend_from_slice(&encoded);
            }
        }
        Datum::TsQuery(q) => buf.extend_from_slice(q.as_bytes()),
        _ => {} // fixed-width or null: caller should not reach here
    }
}

/// Decode a fixed-length datum.  Returns `(datum, bytes_consumed)` or `None`.
fn decode_fixed(dt: &DataType, bytes: &[u8]) -> Option<(Datum, usize)> {
    match dt {
        DataType::Boolean => {
            let b = *bytes.first()?;
            Some((Datum::Boolean(b != 0), 1))
        }
        DataType::Int32 => {
            let v = i32::from_le_bytes(bytes.get(..4)?.try_into().ok()?);
            Some((Datum::Int32(v), 4))
        }
        DataType::Date => {
            let v = i32::from_le_bytes(bytes.get(..4)?.try_into().ok()?);
            Some((Datum::Date(v), 4))
        }
        DataType::Int64 => {
            let v = i64::from_le_bytes(bytes.get(..8)?.try_into().ok()?);
            Some((Datum::Int64(v), 8))
        }
        DataType::Float64 => {
            let bits = u64::from_le_bytes(bytes.get(..8)?.try_into().ok()?);
            Some((Datum::Float64(f64::from_bits(bits)), 8))
        }
        DataType::Timestamp => {
            let v = i64::from_le_bytes(bytes.get(..8)?.try_into().ok()?);
            Some((Datum::Timestamp(v), 8))
        }
        DataType::Time => {
            let v = i64::from_le_bytes(bytes.get(..8)?.try_into().ok()?);
            Some((Datum::Time(v), 8))
        }
        DataType::Decimal(_, _) => {
            if bytes.len() < 17 {
                return None;
            }
            let scale = bytes[0];
            let mantissa = i128::from_le_bytes(bytes[1..17].try_into().ok()?);
            Some((Datum::Decimal(mantissa, scale), 17))
        }
        DataType::Interval => {
            if bytes.len() < 16 {
                return None;
            }
            let months = i32::from_le_bytes(bytes[0..4].try_into().ok()?);
            let days = i32::from_le_bytes(bytes[4..8].try_into().ok()?);
            let us = i64::from_le_bytes(bytes[8..16].try_into().ok()?);
            Some((Datum::Interval(months, days, us), 16))
        }
        DataType::Uuid => {
            if bytes.len() < 16 {
                return None;
            }
            let v = u128::from_be_bytes(bytes[0..16].try_into().ok()?);
            Some((Datum::Uuid(v), 16))
        }
        _ => None, // varlen
    }
}

/// Decode a varlen datum from a raw byte slice.
fn decode_varlen(dt: &DataType, bytes: &[u8]) -> Option<Datum> {
    match dt {
        DataType::Text => {
            let s = std::str::from_utf8(bytes).ok()?;
            Some(Datum::Text(s.to_string()))
        }
        DataType::Bytea => Some(Datum::Bytea(bytes.to_vec())),
        DataType::Array(_) => {
            let elems: Vec<Datum> = bincode::deserialize(bytes).ok()?;
            Some(Datum::Array(elems))
        }
        DataType::Jsonb => {
            let v: serde_json::Value = serde_json::from_slice(bytes).ok()?;
            Some(Datum::Jsonb(v))
        }
        DataType::TsVector => {
            let v: Vec<(String, Vec<u16>)> = bincode::deserialize(bytes).ok()?;
            Some(Datum::TsVector(v))
        }
        DataType::TsQuery => {
            let s = std::str::from_utf8(bytes).ok()?.to_owned();
            Some(Datum::TsQuery(s))
        }
        _ => None,
    }
}

// ── Estimated in-memory size ──────────────────────────────────────────────────

/// Return the **estimated** encoded byte size of a row without actually encoding it.
/// Used for memory tracking and pre-allocation hints.
pub fn estimate_encoded_size(row: &[Datum], schema: &TableSchema) -> usize {
    let ncols = schema.columns.len();
    let bitmap_bytes = (ncols + 7) / 8;
    let mut size = 2 + bitmap_bytes; // header + bitmap

    let mut varlen_count = 0usize;
    for (i, col) in schema.columns.iter().enumerate() {
        let datum = row.get(i).unwrap_or(&Datum::Null);
        if is_null_datum(datum, col.nullable) {
            continue;
        }
        if is_varlen(&col.data_type) {
            varlen_count += 1;
            size += estimate_varlen_size(datum);
        } else {
            size += fixed_size(&col.data_type);
        }
    }
    size += varlen_count * 4; // offset table
    size
}

fn estimate_varlen_size(datum: &Datum) -> usize {
    match datum {
        Datum::Text(s) => s.len(),
        Datum::Bytea(b) => b.len(),
        Datum::Array(elems) => elems.len() * mem::size_of::<Datum>(), // rough
        Datum::Jsonb(v) => v.to_string().len(),
        Datum::TsVector(v) => v.iter().map(|(s, pos)| s.len() + pos.len() * 2 + 8).sum(),
        Datum::TsQuery(q) => q.len(),
        _ => 0,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::Datum;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};

    fn schema_int64_8col() -> TableSchema {
        let cols: Vec<ColumnDef> = (0..8)
            .map(|i| ColumnDef::new(
                ColumnId(i),
                format!("c{i}"),
                DataType::Int64,
                false,
                i == 0,
                None,
                false,
            ))
            .collect();
        TableSchema {
            id: TableId(1),
            name: "t".into(),
            columns: cols,
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            storage_type: Default::default(),
            shard_key: vec![],
            sharding_policy: Default::default(),
            range_bounds: vec![],
            dynamic_defaults: Default::default(),
        }
    }

    fn schema_mixed() -> TableSchema {
        let cols = vec![
            ColumnDef::new(ColumnId(0), "id".into(),   DataType::Int64,   false, true,  None, false),
            ColumnDef::new(ColumnId(1), "name".into(), DataType::Text,    true,  false, None, false),
            ColumnDef::new(ColumnId(2), "age".into(),  DataType::Int32,   true,  false, None, false),
            ColumnDef::new(ColumnId(3), "score".into(),DataType::Float64, true,  false, None, false),
            ColumnDef::new(ColumnId(4), "ts".into(),   DataType::Timestamp,false,false, None, false),
            ColumnDef::new(ColumnId(5), "bio".into(),  DataType::Text,    true,  false, None, false),
        ];
        TableSchema {
            id: TableId(2),
            name: "mixed".into(),
            columns: cols,
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            storage_type: Default::default(),
            shard_key: vec![],
            sharding_policy: Default::default(),
            range_bounds: vec![],
            dynamic_defaults: Default::default(),
        }
    }

    #[test]
    fn test_roundtrip_int64_8col() {
        let schema = schema_int64_8col();
        let row: Vec<Datum> = (0i64..8).map(Datum::Int64).collect();
        let compact = encode(&row, &schema);
        // 8 cols × 8 bytes = 64 bytes + 2 header + 1 bitmap = 67 bytes
        assert_eq!(compact.len(), 67, "encoded size mismatch");
        let decoded = decode(compact.as_bytes(), &schema).unwrap();
        assert_eq!(decoded, row);
    }

    #[test]
    fn test_roundtrip_mixed_with_nulls() {
        let schema = schema_mixed();
        let row = vec![
            Datum::Int64(42),
            Datum::Text("Alice".into()),
            Datum::Null,
            Datum::Float64(9.5),
            Datum::Timestamp(1_700_000_000_000_000),
            Datum::Null,
        ];
        let compact = encode(&row, &schema);
        let decoded = decode(compact.as_bytes(), &schema).unwrap();
        assert_eq!(decoded, row);
    }

    #[test]
    fn test_read_column_zero_copy() {
        let schema = schema_mixed();
        let row = vec![
            Datum::Int64(99),
            Datum::Text("Bob".into()),
            Datum::Int32(30),
            Datum::Float64(7.0),
            Datum::Timestamp(1_000_000),
            Datum::Text("bio text".into()),
        ];
        let compact = encode(&row, &schema);
        // Read each column individually.
        assert_eq!(read_column(compact.as_bytes(), &schema, 0), Some(Datum::Int64(99)));
        assert_eq!(read_column(compact.as_bytes(), &schema, 1), Some(Datum::Text("Bob".into())));
        assert_eq!(read_column(compact.as_bytes(), &schema, 2), Some(Datum::Int32(30)));
        assert_eq!(read_column(compact.as_bytes(), &schema, 4), Some(Datum::Timestamp(1_000_000)));
        assert_eq!(read_column(compact.as_bytes(), &schema, 5), Some(Datum::Text("bio text".into())));
    }

    #[test]
    fn test_null_column_read() {
        let schema = schema_mixed();
        let row = vec![
            Datum::Int64(1),
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Timestamp(0),
            Datum::Null,
        ];
        let compact = encode(&row, &schema);
        assert_eq!(read_column(compact.as_bytes(), &schema, 1), Some(Datum::Null));
        assert_eq!(read_column(compact.as_bytes(), &schema, 2), Some(Datum::Null));
    }

    #[test]
    fn test_memory_density_vs_vec_datum() {
        let schema = schema_int64_8col();
        let row: Vec<Datum> = (0i64..8).map(Datum::Int64).collect();

        // Current format: Vec<Datum> — each Datum is 40 bytes (discriminant + i128 payload)
        let current_size = std::mem::size_of::<Vec<Datum>>() + row.len() * std::mem::size_of::<Datum>();
        let compact = encode(&row, &schema);
        let compact_size = compact.len();

        // Compact should be at least 4× smaller.
        assert!(
            compact_size * 4 < current_size,
            "compact={compact_size} current={current_size}: expected 4× savings"
        );
    }

    #[test]
    fn test_all_fixed_types() {
        let cols = vec![
            ColumnDef::new(ColumnId(0), "b".into(),   DataType::Boolean,    false, false, None, false),
            ColumnDef::new(ColumnId(1), "i32".into(), DataType::Int32,      false, false, None, false),
            ColumnDef::new(ColumnId(2), "i64".into(), DataType::Int64,      false, false, None, false),
            ColumnDef::new(ColumnId(3), "f64".into(), DataType::Float64,    false, false, None, false),
            ColumnDef::new(ColumnId(4), "ts".into(),  DataType::Timestamp,  false, false, None, false),
            ColumnDef::new(ColumnId(5), "d".into(),   DataType::Date,       false, false, None, false),
            ColumnDef::new(ColumnId(6), "t".into(),   DataType::Time,       false, false, None, false),
            ColumnDef::new(ColumnId(7), "dec".into(), DataType::Decimal(18,4), false, false, None, false),
            ColumnDef::new(ColumnId(8), "iv".into(),  DataType::Interval,   false, false, None, false),
            ColumnDef::new(ColumnId(9), "u".into(),   DataType::Uuid,       false, false, None, false),
        ];
        let schema = TableSchema {
            id: TableId(3), name: "ft".into(), columns: cols,
            primary_key_columns: vec![],
            next_serial_values: Default::default(),
            check_constraints: vec![], unique_constraints: vec![],
            foreign_keys: vec![],
            storage_type: Default::default(),
            shard_key: vec![], sharding_policy: Default::default(),
            range_bounds: vec![], dynamic_defaults: Default::default(),
        };
        let row = vec![
            Datum::Boolean(true),
            Datum::Int32(123),
            Datum::Int64(456),
            Datum::Float64(1.5),
            Datum::Timestamp(9999),
            Datum::Date(18000),
            Datum::Time(3600_000_000),
            Datum::Decimal(12345, 4),
            Datum::Interval(1, 2, 3),
            Datum::Uuid(0xDEADBEEF_CAFEBABE_u128),
        ];
        let compact = encode(&row, &schema);
        let decoded = decode(compact.as_bytes(), &schema).unwrap();
        assert_eq!(decoded, row);
    }

    #[test]
    fn test_decode_error_col_count_mismatch() {
        let schema = schema_int64_8col();
        let row: Vec<Datum> = (0i64..8).map(Datum::Int64).collect();
        let compact = encode(&row, &schema);

        // Build a fake schema with a different column count.
        let short_schema = {
            let mut s = schema.clone();
            s.columns.truncate(4);
            s
        };
        let result = decode(compact.as_bytes(), &short_schema);
        assert!(matches!(result, Err(DecodeError::ColCountMismatch { .. })));
    }
}
