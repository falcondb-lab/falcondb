//! Columnar storage engine — modeled after SingleStore COLUMNSTORE.
//!
//! Data is stored column-by-column in compressed "segments".  Each segment
//! holds a fixed number of rows (`SEGMENT_ROW_COUNT`) and uses lightweight
//! encoding per column (dictionary / RLE / raw).
//!
//! Write path:  rows are first buffered in an in-memory write-buffer
//!   (row-oriented `Vec<OwnedRow>`).  When the buffer reaches the segment
//!   threshold it is *frozen* into a columnar segment.
//!
//! Read path:  segments are scanned column-at-a-time; the executor can push
//!   down simple predicates to skip entire segments via min/max zone maps.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::StorageError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{TableId, Timestamp, TxnId};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Number of rows per frozen segment.
pub const SEGMENT_ROW_COUNT: usize = 65_536;

// ---------------------------------------------------------------------------
// Column encoding
// ---------------------------------------------------------------------------

/// A single column's data within a segment.
#[derive(Debug, Clone)]
pub enum ColumnEncoding {
    /// Plain / uncompressed – stores each value individually.
    Plain(Vec<Datum>),
    /// Dictionary encoding – distinct values + per-row index.
    Dictionary {
        dict: Vec<Datum>,
        /// Indices into `dict` for each row (u32 is enough for 64K rows).
        indices: Vec<u32>,
    },
    /// Run-length encoding – (value, run_length) pairs.
    Rle(Vec<(Datum, u32)>),
}

impl ColumnEncoding {
    /// Number of logical rows.
    pub fn len(&self) -> usize {
        match self {
            ColumnEncoding::Plain(v) => v.len(),
            ColumnEncoding::Dictionary { indices, .. } => indices.len(),
            ColumnEncoding::Rle(runs) => runs.iter().map(|(_, n)| *n as usize).sum(),
        }
    }

    /// Retrieve a single value by row index.
    pub fn get(&self, row_idx: usize) -> Option<Datum> {
        match self {
            ColumnEncoding::Plain(v) => v.get(row_idx).cloned(),
            ColumnEncoding::Dictionary { dict, indices } => {
                indices.get(row_idx).and_then(|&i| dict.get(i as usize).cloned())
            }
            ColumnEncoding::Rle(runs) => {
                let mut offset = 0usize;
                for (val, count) in runs {
                    let end = offset + *count as usize;
                    if row_idx < end {
                        return Some(val.clone());
                    }
                    offset = end;
                }
                None
            }
        }
    }

    /// Materialise the full column as a `Vec<Datum>`.
    pub fn to_vec(&self) -> Vec<Datum> {
        match self {
            ColumnEncoding::Plain(v) => v.clone(),
            ColumnEncoding::Dictionary { dict, indices } => {
                indices.iter().map(|&i| dict[i as usize].clone()).collect()
            }
            ColumnEncoding::Rle(runs) => {
                let mut out = Vec::new();
                for (val, count) in runs {
                    for _ in 0..*count {
                        out.push(val.clone());
                    }
                }
                out
            }
        }
    }
}

/// Choose the best encoding for a column of values.
fn encode_column(values: &[Datum]) -> ColumnEncoding {
    if values.is_empty() {
        return ColumnEncoding::Plain(Vec::new());
    }

    // Try RLE: if average run length > 4, use RLE.
    let runs = compute_rle(values);
    if values.len() > 4 && runs.len() * 4 < values.len() {
        return ColumnEncoding::Rle(runs);
    }

    // Try dictionary: if distinct count < 50% of row count, use dict.
    let mut distinct: Vec<Datum> = Vec::new();
    let mut idx_map: HashMap<String, u32> = HashMap::new();
    let mut indices: Vec<u32> = Vec::with_capacity(values.len());
    for v in values {
        let key = format!("{:?}", v);
        let idx = if let Some(&i) = idx_map.get(&key) {
            i
        } else {
            let i = distinct.len() as u32;
            idx_map.insert(key, i);
            distinct.push(v.clone());
            i
        };
        indices.push(idx);
    }
    if distinct.len() * 2 < values.len() {
        return ColumnEncoding::Dictionary {
            dict: distinct,
            indices,
        };
    }

    // Fall back to plain.
    ColumnEncoding::Plain(values.to_vec())
}

fn compute_rle(values: &[Datum]) -> Vec<(Datum, u32)> {
    let mut runs = Vec::new();
    if values.is_empty() {
        return runs;
    }
    let mut current = values[0].clone();
    let mut count: u32 = 1;
    for v in &values[1..] {
        if datum_eq(v, &current) {
            count += 1;
        } else {
            runs.push((current, count));
            current = v.clone();
            count = 1;
        }
    }
    runs.push((current, count));
    runs
}

fn datum_eq(a: &Datum, b: &Datum) -> bool {
    format!("{:?}", a) == format!("{:?}", b)
}

// ---------------------------------------------------------------------------
// Zone map (min/max per segment per column)
// ---------------------------------------------------------------------------

/// Lightweight min/max statistics for one column in one segment.
#[derive(Debug, Clone)]
pub struct ZoneMap {
    pub min: Datum,
    pub max: Datum,
    pub has_null: bool,
    pub row_count: u32,
}

impl ZoneMap {
    fn from_values(values: &[Datum]) -> Self {
        let mut has_null = false;
        let mut min_f: Option<f64> = None;
        let mut max_f: Option<f64> = None;

        for v in values {
            match v {
                Datum::Null => { has_null = true; }
                Datum::Int32(n) => {
                    let f = *n as f64;
                    min_f = Some(min_f.map_or(f, |m: f64| m.min(f)));
                    max_f = Some(max_f.map_or(f, |m: f64| m.max(f)));
                }
                Datum::Int64(n) => {
                    let f = *n as f64;
                    min_f = Some(min_f.map_or(f, |m: f64| m.min(f)));
                    max_f = Some(max_f.map_or(f, |m: f64| m.max(f)));
                }
                Datum::Float64(n) => {
                    min_f = Some(min_f.map_or(*n, |m: f64| m.min(*n)));
                    max_f = Some(max_f.map_or(*n, |m: f64| m.max(*n)));
                }
                _ => {}
            }
        }

        let min = min_f.map_or(Datum::Null, Datum::Float64);
        let max = max_f.map_or(Datum::Null, Datum::Float64);

        ZoneMap {
            min,
            max,
            has_null,
            row_count: values.len() as u32,
        }
    }
}

// ---------------------------------------------------------------------------
// Segment
// ---------------------------------------------------------------------------

/// A frozen, immutable columnar segment.
#[derive(Debug, Clone)]
pub struct Segment {
    /// Per-column encoded data (indexed by column ordinal).
    pub columns: Vec<ColumnEncoding>,
    /// Per-column zone map for predicate pushdown.
    pub zone_maps: Vec<ZoneMap>,
    /// Logical row count.
    pub row_count: usize,
    /// Segment sequence number (monotonically increasing).
    pub seq: u64,
}

impl Segment {
    /// Build a segment from a batch of rows.
    pub fn from_rows(rows: &[OwnedRow], num_cols: usize, seq: u64) -> Self {
        let row_count = rows.len();
        let mut col_values: Vec<Vec<Datum>> = (0..num_cols).map(|_| Vec::with_capacity(row_count)).collect();

        for row in rows {
            for col_idx in 0..num_cols {
                let val = row.values.get(col_idx).cloned().unwrap_or(Datum::Null);
                col_values[col_idx].push(val);
            }
        }

        let zone_maps: Vec<ZoneMap> = col_values.iter().map(|v| ZoneMap::from_values(v)).collect();
        let columns: Vec<ColumnEncoding> = col_values.iter().map(|v| encode_column(v)).collect();

        Segment {
            columns,
            zone_maps,
            row_count,
            seq,
        }
    }

    /// Read a single row by index.
    pub fn read_row(&self, row_idx: usize) -> Option<OwnedRow> {
        if row_idx >= self.row_count {
            return None;
        }
        let values: Vec<Datum> = self.columns.iter()
            .map(|col| col.get(row_idx).unwrap_or(Datum::Null))
            .collect();
        Some(OwnedRow::new(values))
    }
}

// ---------------------------------------------------------------------------
// ColumnStoreTable
// ---------------------------------------------------------------------------

/// A columnar table: frozen segments + a mutable write buffer.
pub struct ColumnStoreTable {
    pub schema: TableSchema,
    /// Frozen segments (append-only, read by analytics).
    segments: RwLock<Vec<Arc<Segment>>>,
    /// Mutable write-buffer (row-oriented until frozen).
    write_buffer: RwLock<Vec<OwnedRow>>,
    /// Delete bitmap: (segment_seq, row_idx) pairs of logically deleted rows.
    deletes: RwLock<HashMap<(u64, usize), bool>>,
    /// Next segment sequence number.
    next_seg_seq: AtomicU64,
}

impl ColumnStoreTable {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            segments: RwLock::new(Vec::new()),
            write_buffer: RwLock::new(Vec::new()),
            deletes: RwLock::new(HashMap::new()),
            next_seg_seq: AtomicU64::new(1),
        }
    }

    pub fn table_id(&self) -> TableId {
        self.schema.id
    }

    /// Insert a row into the write buffer.  If the buffer reaches the
    /// segment threshold, freeze it into a new columnar segment.
    pub fn insert(&self, row: OwnedRow, _txn_id: TxnId) -> Result<(), StorageError> {
        let should_freeze;
        {
            let mut buf = self.write_buffer.write();
            buf.push(row);
            should_freeze = buf.len() >= SEGMENT_ROW_COUNT;
        }
        if should_freeze {
            self.freeze_buffer();
        }
        Ok(())
    }

    /// Freeze the current write-buffer into a columnar segment.
    pub fn freeze_buffer(&self) {
        let rows: Vec<OwnedRow>;
        {
            let mut buf = self.write_buffer.write();
            if buf.is_empty() {
                return;
            }
            rows = std::mem::take(&mut *buf);
        }
        let seq = self.next_seg_seq.fetch_add(1, Ordering::Relaxed);
        let seg = Segment::from_rows(&rows, self.schema.columns.len(), seq);
        let mut segs = self.segments.write();
        segs.push(Arc::new(seg));
    }

    /// Mark a row as deleted (by segment_seq + row_idx).
    pub fn delete_row(&self, segment_seq: u64, row_idx: usize) {
        let mut dels = self.deletes.write();
        dels.insert((segment_seq, row_idx), true);
    }

    fn is_deleted(&self, segment_seq: u64, row_idx: usize) -> bool {
        let dels = self.deletes.read();
        dels.contains_key(&(segment_seq, row_idx))
    }

    /// Full-table scan: iterate segments + write buffer, skip deleted rows.
    /// Returns rows as `(synthetic_pk, OwnedRow)` where the PK is a
    /// segment_seq:row_idx composite.
    pub fn scan(&self, _txn_id: TxnId, _read_ts: Timestamp) -> Vec<(Vec<u8>, OwnedRow)> {
        let mut results = Vec::new();

        // Frozen segments
        let segs = self.segments.read();
        for seg in segs.iter() {
            for row_idx in 0..seg.row_count {
                if self.is_deleted(seg.seq, row_idx) {
                    continue;
                }
                if let Some(row) = seg.read_row(row_idx) {
                    let pk = encode_cs_pk(seg.seq, row_idx);
                    results.push((pk, row));
                }
            }
        }

        // Write buffer (unfrozen rows)
        let buf = self.write_buffer.read();
        for (i, row) in buf.iter().enumerate() {
            let pk = encode_cs_pk(0, i); // seg_seq=0 means write buffer
            results.push((pk, row.clone()));
        }

        results
    }

    /// Column-scan: return a single column's values across all segments.
    /// Useful for vectorised aggregation.
    pub fn column_scan(&self, col_idx: usize, _txn_id: TxnId, _read_ts: Timestamp) -> Vec<Datum> {
        let mut values = Vec::new();
        let segs = self.segments.read();
        for seg in segs.iter() {
            if col_idx < seg.columns.len() {
                let col = &seg.columns[col_idx];
                for (row_idx, val) in col.to_vec().into_iter().enumerate() {
                    if !self.is_deleted(seg.seq, row_idx) {
                        values.push(val);
                    }
                }
            }
        }
        // Write buffer
        let buf = self.write_buffer.read();
        for row in buf.iter() {
            values.push(row.values.get(col_idx).cloned().unwrap_or(Datum::Null));
        }
        values
    }

    /// Number of rows (approximate — includes unfrozen buffer).
    pub fn row_count_approx(&self) -> usize {
        let segs = self.segments.read();
        let seg_rows: usize = segs.iter().map(|s| s.row_count).sum();
        let buf_rows = self.write_buffer.read().len();
        seg_rows + buf_rows
    }

    /// Number of frozen segments.
    pub fn segment_count(&self) -> usize {
        self.segments.read().len()
    }

    /// Snapshot statistics for observability.
    pub fn stats(&self) -> ColumnStoreStats {
        let segs = self.segments.read();
        ColumnStoreStats {
            segment_count: segs.len(),
            total_rows: segs.iter().map(|s| s.row_count).sum::<usize>()
                + self.write_buffer.read().len(),
            buffer_rows: self.write_buffer.read().len(),
        }
    }
}

/// Encode a columnstore "PK" as (segment_seq:row_idx) composite bytes.
fn encode_cs_pk(segment_seq: u64, row_idx: usize) -> Vec<u8> {
    let mut pk = Vec::with_capacity(16);
    pk.extend_from_slice(&segment_seq.to_be_bytes());
    pk.extend_from_slice(&(row_idx as u64).to_be_bytes());
    pk
}

/// Observability snapshot for a columnstore table.
#[derive(Debug, Clone)]
pub struct ColumnStoreStats {
    pub segment_count: usize,
    pub total_rows: usize,
    pub buffer_rows: usize,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::schema::{ColumnDef, StorageType};
    use falcon_common::types::{ColumnId, TableId, Timestamp, TxnId};

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(100),
            name: "cs_test".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".to_string(),
                    data_type: falcon_common::types::DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "name".to_string(),
                    data_type: falcon_common::types::DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "score".to_string(),
                    data_type: falcon_common::types::DataType::Float64,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            storage_type: StorageType::Columnstore,
            ..Default::default()
        }
    }

    #[test]
    fn test_insert_and_scan() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..10 {
            let row = OwnedRow::new(vec![
                Datum::Int64(i),
                Datum::Text(format!("name_{}", i)),
                Datum::Float64(i as f64 * 1.5),
            ]);
            table.insert(row, txn).unwrap();
        }
        let rows = table.scan(txn, Timestamp(100));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn test_freeze_buffer() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        // Insert enough rows to trigger a freeze
        for i in 0..SEGMENT_ROW_COUNT + 5 {
            let row = OwnedRow::new(vec![
                Datum::Int64(i as i64),
                Datum::Text(format!("row_{}", i)),
                Datum::Float64(i as f64),
            ]);
            table.insert(row, txn).unwrap();
        }
        assert_eq!(table.segment_count(), 1);
        let rows = table.scan(txn, Timestamp(100));
        assert_eq!(rows.len(), SEGMENT_ROW_COUNT + 5);
    }

    #[test]
    fn test_column_scan() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..100 {
            let row = OwnedRow::new(vec![
                Datum::Int64(i),
                Datum::Text("x".to_string()),
                Datum::Float64(i as f64),
            ]);
            table.insert(row, txn).unwrap();
        }
        // Force freeze so we have columnar data
        table.freeze_buffer();

        let scores = table.column_scan(2, txn, Timestamp(100));
        assert_eq!(scores.len(), 100);
        if let Datum::Float64(v) = &scores[0] {
            assert!((v - 0.0).abs() < 0.001);
        } else {
            panic!("expected Float64");
        }
    }

    #[test]
    fn test_encoding_rle() {
        let values: Vec<Datum> = (0..100).map(|_| Datum::Int64(42)).collect();
        let enc = encode_column(&values);
        assert!(matches!(enc, ColumnEncoding::Rle(_)));
        assert_eq!(enc.len(), 100);
        assert_eq!(enc.get(50), Some(Datum::Int64(42)));
    }

    #[test]
    fn test_encoding_dictionary() {
        // 100 rows with only 3 distinct values — should get dict encoding
        let values: Vec<Datum> = (0..100).map(|i| Datum::Text(format!("v{}", i % 3))).collect();
        let enc = encode_column(&values);
        assert!(matches!(enc, ColumnEncoding::Dictionary { .. }));
        assert_eq!(enc.len(), 100);
    }

    #[test]
    fn test_delete_row() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..10 {
            table.insert(OwnedRow::new(vec![
                Datum::Int64(i),
                Datum::Text(format!("r{}", i)),
                Datum::Float64(0.0),
            ]), txn).unwrap();
        }
        table.freeze_buffer();
        assert_eq!(table.scan(txn, Timestamp(100)).len(), 10);

        // Delete row 5 in segment 1
        table.delete_row(1, 5);
        assert_eq!(table.scan(txn, Timestamp(100)).len(), 9);
    }
}
