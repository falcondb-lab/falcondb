//! Columnar storage engine for HTAP workloads.
//!
//! Feature-gated (`columnstore`, default OFF).  Modeled after SingleStore COLUMNSTORE.
//!
//! Data is stored column-by-column in compressed segments.  Each segment holds
//! up to `SEGMENT_ROW_COUNT` rows and uses lightweight encoding (dict / RLE / plain).
//!
//! Write path: rows enter an MVCC write-buffer with per-row txn_id + commit_ts.
//! When the buffer reaches the segment threshold it is frozen into a columnar segment.
//! Batch insert is supported for ETL / bulk-load.
//!
//! Read path: segments are scanned column-at-a-time with MVCC visibility filtering.
//! Zone-map pushdown skips entire segments.  Aggregate pushdown computes
//! COUNT/SUM/MIN/MAX directly from segment metadata when possible.

use std::collections::{BTreeMap, HashMap};
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

/// Minimum segments to trigger compaction consideration.
const COMPACTION_MIN_SEGMENTS: usize = 4;

/// Segments with fewer rows than this are "small" and eligible for merge.
const COMPACTION_SMALL_THRESHOLD: usize = SEGMENT_ROW_COUNT / 4;

// ---------------------------------------------------------------------------
// Datum equality (structural, no format!("{:?}") allocation)
// ---------------------------------------------------------------------------

fn datum_eq(a: &Datum, b: &Datum) -> bool {
    match (a, b) {
        (Datum::Null, Datum::Null) => true,
        (Datum::Boolean(x), Datum::Boolean(y)) => x == y,
        (Datum::Int32(x), Datum::Int32(y)) => x == y,
        (Datum::Int64(x), Datum::Int64(y)) => x == y,
        (Datum::Float64(x), Datum::Float64(y)) => x.to_bits() == y.to_bits(),
        (Datum::Text(x), Datum::Text(y)) => x == y,
        (Datum::Bytea(x), Datum::Bytea(y)) => x == y,
        (Datum::Date(x), Datum::Date(y)) => x == y,
        (Datum::Timestamp(x), Datum::Timestamp(y)) => x == y,
        // Cross-int promotion
        (Datum::Int32(x), Datum::Int64(y)) => i64::from(*x) == *y,
        (Datum::Int64(x), Datum::Int32(y)) => *x == i64::from(*y),
        _ => false,
    }
}

/// Datum ordering key for BTreeMap: canonical byte string.
fn datum_sort_key(d: &Datum) -> Vec<u8> {
    match d {
        Datum::Null => vec![0],
        Datum::Boolean(b) => vec![1, *b as u8],
        Datum::Int32(n) => {
            let mut v = vec![2];
            v.extend_from_slice(&n.to_be_bytes());
            v
        }
        Datum::Int64(n) => {
            let mut v = vec![3];
            v.extend_from_slice(&n.to_be_bytes());
            v
        }
        Datum::Float64(f) => {
            let mut v = vec![4];
            v.extend_from_slice(&f.to_bits().to_be_bytes());
            v
        }
        Datum::Text(s) => {
            let mut v = vec![5];
            v.extend_from_slice(s.as_bytes());
            v
        }
        Datum::Bytea(b) => {
            let mut v = vec![6];
            v.extend_from_slice(b);
            v
        }
        Datum::Date(d) => {
            let mut v = vec![7];
            v.extend_from_slice(&d.to_be_bytes());
            v
        }
        Datum::Timestamp(t) => {
            let mut v = vec![8];
            v.extend_from_slice(&t.to_be_bytes());
            v
        }
        _ => {
            let mut v = vec![255];
            v.extend_from_slice(format!("{d:?}").as_bytes());
            v
        }
    }
}

// ---------------------------------------------------------------------------
// Column encoding
// ---------------------------------------------------------------------------

/// A single column's data within a segment.
#[derive(Debug, Clone)]
pub enum ColumnEncoding {
    Plain(Vec<Datum>),
    Dictionary {
        dict: Vec<Datum>,
        indices: Vec<u32>,
    },
    Rle(Vec<(Datum, u32)>),
}

impl ColumnEncoding {
    pub fn len(&self) -> usize {
        match self {
            ColumnEncoding::Plain(v) => v.len(),
            ColumnEncoding::Dictionary { indices, .. } => indices.len(),
            ColumnEncoding::Rle(runs) => runs.iter().map(|(_, n)| *n as usize).sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, row_idx: usize) -> Option<Datum> {
        match self {
            ColumnEncoding::Plain(v) => v.get(row_idx).cloned(),
            ColumnEncoding::Dictionary { dict, indices } => indices
                .get(row_idx)
                .and_then(|&i| dict.get(i as usize).cloned()),
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

fn encode_column(values: &[Datum]) -> ColumnEncoding {
    if values.is_empty() {
        return ColumnEncoding::Plain(Vec::new());
    }

    // RLE: if average run length > 4, use RLE.
    let runs = compute_rle(values);
    if values.len() > 4 && runs.len() * 4 < values.len() {
        return ColumnEncoding::Rle(runs);
    }

    // Dict: if distinct count < 50% of row count, use dict.
    let mut distinct: Vec<Datum> = Vec::new();
    let mut idx_map: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut indices: Vec<u32> = Vec::with_capacity(values.len());
    for v in values {
        let key = datum_sort_key(v);
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

// ---------------------------------------------------------------------------
// Zone map (min/max per segment per column)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ZoneMap {
    pub min: Datum,
    pub max: Datum,
    pub has_null: bool,
    pub row_count: u32,
    /// Pre-computed sum for numeric columns (None for non-numeric).
    pub sum: Option<f64>,
}

impl ZoneMap {
    fn from_values(values: &[Datum]) -> Self {
        let mut has_null = false;
        let mut min_f: Option<f64> = None;
        let mut max_f: Option<f64> = None;
        let mut sum: f64 = 0.0;
        let mut numeric_count: u32 = 0;

        for v in values {
            match v {
                Datum::Null => has_null = true,
                Datum::Int32(n) => {
                    let f = *n as f64;
                    min_f = Some(min_f.map_or(f, |m: f64| m.min(f)));
                    max_f = Some(max_f.map_or(f, |m: f64| m.max(f)));
                    sum += f;
                    numeric_count += 1;
                }
                Datum::Int64(n) => {
                    let f = *n as f64;
                    min_f = Some(min_f.map_or(f, |m: f64| m.min(f)));
                    max_f = Some(max_f.map_or(f, |m: f64| m.max(f)));
                    sum += f;
                    numeric_count += 1;
                }
                Datum::Float64(n) => {
                    min_f = Some(min_f.map_or(*n, |m: f64| m.min(*n)));
                    max_f = Some(max_f.map_or(*n, |m: f64| m.max(*n)));
                    sum += n;
                    numeric_count += 1;
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
            sum: if numeric_count > 0 { Some(sum) } else { None },
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
        let mut col_values: Vec<Vec<Datum>> = (0..num_cols)
            .map(|_| Vec::with_capacity(row_count))
            .collect();

        for row in rows {
            for (col_idx, col_vec) in col_values.iter_mut().enumerate().take(num_cols) {
                let val = row.values.get(col_idx).cloned().unwrap_or(Datum::Null);
                col_vec.push(val);
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
        let values: Vec<Datum> = self
            .columns
            .iter()
            .map(|col| col.get(row_idx).unwrap_or(Datum::Null))
            .collect();
        Some(OwnedRow::new(values))
    }
}

// ---------------------------------------------------------------------------
// MVCC write-buffer entry
// ---------------------------------------------------------------------------

/// A row in the write buffer with MVCC metadata.
struct BufferRow {
    row: OwnedRow,
    txn_id: TxnId,
    /// 0 = uncommitted (Prepared state). Set on commit.
    commit_ts: AtomicU64,
    /// Logically deleted (tombstone).
    deleted: bool,
}

impl BufferRow {
    fn is_visible(&self, reader_txn: TxnId, read_ts: Timestamp) -> bool {
        if self.deleted {
            return false;
        }
        let cts = self.commit_ts.load(Ordering::Acquire);
        if cts == 0 {
            // Uncommitted — only visible to the owning txn.
            return self.txn_id == reader_txn;
        }
        cts <= read_ts.0
    }

    fn is_committed(&self) -> bool {
        self.commit_ts.load(Ordering::Acquire) != 0
    }
}

// ---------------------------------------------------------------------------
// ColumnStoreTable
// ---------------------------------------------------------------------------

/// A columnar table: frozen segments + a mutable MVCC write buffer.
pub struct ColumnStoreTable {
    pub schema: TableSchema,
    /// Frozen segments (append-only, read by analytics).
    segments: RwLock<Vec<Arc<Segment>>>,
    /// Mutable write-buffer with MVCC versioning.
    write_buffer: RwLock<Vec<BufferRow>>,
    /// PK index: maps PK bytes → location.
    /// Location is either `PkLoc::Buffer(idx)` or `PkLoc::Segment(seg_seq, row_idx)`.
    pk_index: RwLock<BTreeMap<Vec<u8>, PkLoc>>,
    /// Delete bitmap: (segment_seq, row_idx) pairs of logically deleted rows.
    deletes: RwLock<HashMap<(u64, usize), TxnId>>,
    /// Next segment sequence number.
    next_seg_seq: AtomicU64,
    /// Total committed rows (for fast COUNT(*) when no deletes).
    committed_row_count: AtomicU64,
    /// Total deleted rows.
    deleted_row_count: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
enum PkLoc {
    Buffer(usize),
    Segment(u64, usize), // (seg_seq, row_idx)
}

impl ColumnStoreTable {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            segments: RwLock::new(Vec::new()),
            write_buffer: RwLock::new(Vec::new()),
            pk_index: RwLock::new(BTreeMap::new()),
            deletes: RwLock::new(HashMap::new()),
            next_seg_seq: AtomicU64::new(1),
            committed_row_count: AtomicU64::new(0),
            deleted_row_count: AtomicU64::new(0),
        }
    }

    pub fn table_id(&self) -> TableId {
        self.schema.id
    }

    /// Encode PK from a row using schema pk_indices.
    fn encode_pk(&self, row: &OwnedRow) -> Vec<u8> {
        let pk_cols = self.schema.pk_indices();
        if pk_cols.is_empty() {
            // No PK — use a synthetic auto-increment key.
            return self.next_seg_seq.fetch_add(1, Ordering::Relaxed).to_be_bytes().to_vec();
        }
        let mut key = Vec::new();
        for &idx in pk_cols {
            let d = row.values.get(idx).unwrap_or(&Datum::Null);
            key.extend_from_slice(&datum_sort_key(d));
        }
        key
    }

    /// Insert a row into the write buffer with MVCC metadata.
    /// Returns a synthetic PK. Freezes buffer if threshold reached.
    pub fn insert(&self, row: OwnedRow, txn_id: TxnId) -> Result<(), StorageError> {
        let pk = self.encode_pk(&row);

        // Check PK uniqueness
        {
            let idx = self.pk_index.read();
            if idx.contains_key(&pk) {
                return Err(StorageError::DuplicateKey);
            }
        }

        let should_freeze;
        let buf_idx;
        {
            let mut buf = self.write_buffer.write();
            buf_idx = buf.len();
            buf.push(BufferRow {
                row,
                txn_id,
                commit_ts: AtomicU64::new(0),
                deleted: false,
            });
            should_freeze = buf.len() >= SEGMENT_ROW_COUNT;
        }

        {
            let mut idx = self.pk_index.write();
            idx.insert(pk, PkLoc::Buffer(buf_idx));
        }

        if should_freeze {
            self.freeze_buffer();
        }
        Ok(())
    }

    /// Batch insert for ETL / bulk-load. Inserts all rows in a single lock acquisition.
    pub fn insert_batch(&self, rows: Vec<OwnedRow>, txn_id: TxnId) -> Result<usize, StorageError> {
        let count = rows.len();
        let mut pks: Vec<Vec<u8>> = Vec::with_capacity(count);
        for r in &rows {
            pks.push(self.encode_pk(r));
        }

        // Check all PKs unique
        {
            let idx = self.pk_index.read();
            for pk in &pks {
                if idx.contains_key(pk) {
                    return Err(StorageError::DuplicateKey);
                }
            }
        }

        let should_freeze;
        {
            let mut buf = self.write_buffer.write();
            let mut pk_idx = self.pk_index.write();
            for (i, row) in rows.into_iter().enumerate() {
                let bi = buf.len();
                buf.push(BufferRow {
                    row,
                    txn_id,
                    commit_ts: AtomicU64::new(0),
                    deleted: false,
                });
                pk_idx.insert(std::mem::take(&mut pks[i]), PkLoc::Buffer(bi));
            }
            should_freeze = buf.len() >= SEGMENT_ROW_COUNT;
        }

        if should_freeze {
            self.freeze_buffer();
        }
        Ok(count)
    }

    /// Commit all rows written by `txn_id` at the given timestamp.
    pub fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) {
        let buf = self.write_buffer.read();
        let mut committed = 0u64;
        for entry in buf.iter() {
            if entry.txn_id == txn_id && !entry.is_committed() {
                entry.commit_ts.store(commit_ts.0, Ordering::Release);
                committed += 1;
            }
        }
        self.committed_row_count.fetch_add(committed, Ordering::Relaxed);
    }

    /// Abort all uncommitted rows written by `txn_id`.
    pub fn abort(&self, txn_id: TxnId) {
        let mut buf = self.write_buffer.write();
        let mut pk_idx = self.pk_index.write();
        buf.retain(|entry| {
            if entry.txn_id == txn_id && !entry.is_committed() {
                // Remove from PK index
                let pk = self.encode_pk_from_row_inner(&entry.row);
                pk_idx.remove(&pk);
                false
            } else {
                true
            }
        });
        // Note: buffer indices in pk_index may be stale after retain.
        // Rebuild buffer portion of PK index.
        for (i, entry) in buf.iter().enumerate() {
            let pk = self.encode_pk_from_row_inner(&entry.row);
            if let Some(loc) = pk_idx.get_mut(&pk) {
                if matches!(loc, PkLoc::Buffer(_)) {
                    *loc = PkLoc::Buffer(i);
                }
            }
        }
    }

    fn encode_pk_from_row_inner(&self, row: &OwnedRow) -> Vec<u8> {
        let pk_cols = self.schema.pk_indices();
        if pk_cols.is_empty() {
            return Vec::new();
        }
        let mut key = Vec::new();
        for &idx in pk_cols {
            let d = row.values.get(idx).unwrap_or(&Datum::Null);
            key.extend_from_slice(&datum_sort_key(d));
        }
        key
    }

    /// Freeze committed rows in the write-buffer into a columnar segment.
    pub fn freeze_buffer(&self) {
        let committed_rows: Vec<OwnedRow>;
        let committed_pks: Vec<Vec<u8>>;
        {
            let buf = self.write_buffer.read();
            let pairs: Vec<(Vec<u8>, &OwnedRow)> = buf.iter()
                .filter(|e| e.is_committed() && !e.deleted)
                .map(|e| (self.encode_pk_from_row_inner(&e.row), &e.row))
                .collect();
            if pairs.is_empty() {
                return;
            }
            committed_pks = pairs.iter().map(|(pk, _)| pk.clone()).collect();
            committed_rows = pairs.into_iter().map(|(_, r)| r.clone()).collect();
        }

        let seq = self.next_seg_seq.fetch_add(1, Ordering::Relaxed);
        let seg = Segment::from_rows(&committed_rows, self.schema.columns.len(), seq);

        // Update PK index to point to segment locations
        {
            let mut pk_idx = self.pk_index.write();
            for (row_idx, pk) in committed_pks.iter().enumerate() {
                pk_idx.insert(pk.clone(), PkLoc::Segment(seq, row_idx));
            }
        }

        // Remove frozen rows from buffer
        {
            let mut buf = self.write_buffer.write();
            buf.retain(|e| !(e.is_committed() && !e.deleted));
            // Rebuild buffer indices
            let mut pk_idx = self.pk_index.write();
            for (i, entry) in buf.iter().enumerate() {
                let pk = self.encode_pk_from_row_inner(&entry.row);
                if let Some(loc) = pk_idx.get_mut(&pk) {
                    if matches!(loc, PkLoc::Buffer(_)) {
                        *loc = PkLoc::Buffer(i);
                    }
                }
            }
        }

        let mut segs = self.segments.write();
        segs.push(Arc::new(seg));
    }

    /// Delete a row by PK. Marks it as deleted in the delete bitmap.
    pub fn delete_by_pk(&self, pk: &[u8], txn_id: TxnId) -> bool {
        let loc = {
            let idx = self.pk_index.read();
            idx.get(pk).copied()
        };
        match loc {
            Some(PkLoc::Segment(seg_seq, row_idx)) => {
                let mut dels = self.deletes.write();
                dels.insert((seg_seq, row_idx), txn_id);
                self.deleted_row_count.fetch_add(1, Ordering::Relaxed);
                true
            }
            Some(PkLoc::Buffer(idx)) => {
                let buf = self.write_buffer.read();
                if buf.get(idx).is_some() {
                    drop(buf);
                    let mut dels = self.deletes.write();
                    dels.insert((0, idx), txn_id);
                    self.deleted_row_count.fetch_add(1, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }

    /// Mark a row as deleted (by segment_seq + row_idx). Legacy API.
    pub fn delete_row(&self, segment_seq: u64, row_idx: usize) {
        let mut dels = self.deletes.write();
        dels.insert((segment_seq, row_idx), TxnId(0));
        self.deleted_row_count.fetch_add(1, Ordering::Relaxed);
    }

    fn is_deleted(&self, segment_seq: u64, row_idx: usize) -> bool {
        let dels = self.deletes.read();
        dels.contains_key(&(segment_seq, row_idx))
    }

    /// Point get by PK bytes.
    pub fn get(&self, pk: &[u8], txn_id: TxnId, read_ts: Timestamp) -> Option<OwnedRow> {
        let loc = {
            let idx = self.pk_index.read();
            idx.get(pk).copied()
        };
        match loc? {
            PkLoc::Buffer(idx) => {
                let buf = self.write_buffer.read();
                let entry = buf.get(idx)?;
                if entry.is_visible(txn_id, read_ts) {
                    Some(entry.row.clone())
                } else {
                    None
                }
            }
            PkLoc::Segment(seg_seq, row_idx) => {
                if self.is_deleted(seg_seq, row_idx) {
                    return None;
                }
                let segs = self.segments.read();
                let seg = segs.iter().find(|s| s.seq == seg_seq)?;
                seg.read_row(row_idx)
            }
        }
    }

    /// Full-table scan with MVCC visibility.
    pub fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(Vec<u8>, OwnedRow)> {
        let mut results = Vec::new();

        // Frozen segments (all committed)
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

        // Write buffer (MVCC-filtered)
        let buf = self.write_buffer.read();
        for (i, entry) in buf.iter().enumerate() {
            if entry.is_visible(txn_id, read_ts) && !self.is_deleted(0, i) {
                let pk = encode_cs_pk(0, i);
                results.push((pk, entry.row.clone()));
            }
        }

        results
    }

    /// Column-scan: return a single column's values across all segments + buffer.
    pub fn column_scan(&self, col_idx: usize, txn_id: TxnId, read_ts: Timestamp) -> Vec<Datum> {
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
        let buf = self.write_buffer.read();
        for (i, entry) in buf.iter().enumerate() {
            if entry.is_visible(txn_id, read_ts) && !self.is_deleted(0, i) {
                values.push(entry.row.values.get(col_idx).cloned().unwrap_or(Datum::Null));
            }
        }
        values
    }

    /// Number of rows (approximate — includes unfrozen buffer).
    pub fn row_count_approx(&self) -> usize {
        let committed = self.committed_row_count.load(Ordering::Relaxed);
        let deleted = self.deleted_row_count.load(Ordering::Relaxed);
        (committed as usize).saturating_sub(deleted as usize)
            + self.write_buffer.read().len()
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

    // ── Segment compaction ───────────────────────────────────────────

    /// Compact small segments into larger ones, purging deleted rows.
    /// Returns number of segments merged.
    pub fn compact(&self) -> usize {
        let segs_snapshot: Vec<Arc<Segment>>;
        {
            let segs = self.segments.read();
            if segs.len() < COMPACTION_MIN_SEGMENTS {
                return 0;
            }
            segs_snapshot = segs.clone();
        }

        // Find small segments eligible for merge.
        let small: Vec<usize> = segs_snapshot.iter().enumerate()
            .filter(|(_, s)| s.row_count < COMPACTION_SMALL_THRESHOLD)
            .map(|(i, _)| i)
            .collect();

        if small.len() < 2 {
            return 0;
        }

        // Merge small segments into one.
        let num_cols = self.schema.columns.len();
        let mut merged_rows: Vec<OwnedRow> = Vec::new();
        let merged_seqs: Vec<u64> = small.iter().map(|&i| segs_snapshot[i].seq).collect();

        for &idx in &small {
            let seg = &segs_snapshot[idx];
            for row_idx in 0..seg.row_count {
                if !self.is_deleted(seg.seq, row_idx) {
                    if let Some(row) = seg.read_row(row_idx) {
                        merged_rows.push(row);
                    }
                }
            }
        }

        if merged_rows.is_empty() {
            // All rows were deleted — just remove the segments.
            let mut segs = self.segments.write();
            segs.retain(|s| !merged_seqs.contains(&s.seq));
            return small.len();
        }

        let new_seq = self.next_seg_seq.fetch_add(1, Ordering::Relaxed);
        let new_seg = Segment::from_rows(&merged_rows, num_cols, new_seq);

        // Update PK index for merged rows.
        {
            let mut pk_idx = self.pk_index.write();
            for (row_idx, row) in merged_rows.iter().enumerate() {
                let pk = self.encode_pk(row);
                pk_idx.insert(pk, PkLoc::Segment(new_seq, row_idx));
            }
        }

        // Swap segments.
        {
            let mut segs = self.segments.write();
            segs.retain(|s| !merged_seqs.contains(&s.seq));
            segs.push(Arc::new(new_seg));
        }

        // Clean up delete entries for merged segments.
        {
            let mut dels = self.deletes.write();
            dels.retain(|&(seq, _), _| !merged_seqs.contains(&seq));
        }

        small.len()
    }

    // ── Aggregate pushdown ───────────────────────────────────────────

    /// Fast COUNT(*) using segment metadata + buffer scan.
    pub fn count_pushdown(&self, txn_id: TxnId, read_ts: Timestamp) -> usize {
        let mut count = 0usize;

        let segs = self.segments.read();
        let dels = self.deletes.read();
        for seg in segs.iter() {
            let del_count = (0..seg.row_count)
                .filter(|&ri| dels.contains_key(&(seg.seq, ri)))
                .count();
            count += seg.row_count - del_count;
        }
        drop(dels);
        drop(segs);

        let buf = self.write_buffer.read();
        for (i, entry) in buf.iter().enumerate() {
            if entry.is_visible(txn_id, read_ts) && !self.is_deleted(0, i) {
                count += 1;
            }
        }
        count
    }

    /// Fast SUM on a numeric column using zone-map pre-computed sums.
    /// Falls back to full scan if any segment has deletes in that column.
    pub fn sum_pushdown(&self, col_idx: usize, txn_id: TxnId, read_ts: Timestamp) -> Option<f64> {
        let mut total = 0.0f64;
        let segs = self.segments.read();
        let dels = self.deletes.read();

        for seg in segs.iter() {
            if col_idx >= seg.zone_maps.len() {
                return None;
            }
            let has_deletes = (0..seg.row_count).any(|ri| dels.contains_key(&(seg.seq, ri)));
            if has_deletes {
                // Can't use zone-map sum, fall back to row scan for this segment.
                let col = &seg.columns[col_idx];
                for ri in 0..seg.row_count {
                    if !dels.contains_key(&(seg.seq, ri)) {
                        if let Some(d) = col.get(ri) {
                            total += datum_as_f64(&d)?;
                        }
                    }
                }
            } else {
                total += seg.zone_maps[col_idx].sum?;
            }
        }
        drop(dels);
        drop(segs);

        // Buffer rows.
        let buf = self.write_buffer.read();
        for (i, entry) in buf.iter().enumerate() {
            if entry.is_visible(txn_id, read_ts) && !self.is_deleted(0, i) {
                let d = entry.row.values.get(col_idx).unwrap_or(&Datum::Null);
                if !d.is_null() {
                    total += datum_as_f64(d)?;
                }
            }
        }
        Some(total)
    }

    /// Fast MIN on a numeric column. Uses zone-map min when no deletes.
    pub fn min_pushdown(&self, col_idx: usize, txn_id: TxnId, read_ts: Timestamp) -> Option<f64> {
        let mut global_min: Option<f64> = None;
        let segs = self.segments.read();
        let dels = self.deletes.read();

        for seg in segs.iter() {
            if col_idx >= seg.zone_maps.len() {
                return None;
            }
            let has_deletes = (0..seg.row_count).any(|ri| dels.contains_key(&(seg.seq, ri)));
            let seg_min = if has_deletes {
                segment_col_min_manual(seg, col_idx, &dels)
            } else {
                match &seg.zone_maps[col_idx].min {
                    Datum::Float64(f) => Some(*f),
                    _ => None,
                }
            };
            if let Some(m) = seg_min {
                global_min = Some(global_min.map_or(m, |g: f64| g.min(m)));
            }
        }
        drop(dels);
        drop(segs);

        let buf = self.write_buffer.read();
        for (i, entry) in buf.iter().enumerate() {
            if entry.is_visible(txn_id, read_ts) && !self.is_deleted(0, i) {
                if let Some(f) = entry.row.values.get(col_idx).and_then(datum_as_f64) {
                    global_min = Some(global_min.map_or(f, |g: f64| g.min(f)));
                }
            }
        }
        global_min
    }

    /// Fast MAX on a numeric column.
    pub fn max_pushdown(&self, col_idx: usize, txn_id: TxnId, read_ts: Timestamp) -> Option<f64> {
        let mut global_max: Option<f64> = None;
        let segs = self.segments.read();
        let dels = self.deletes.read();

        for seg in segs.iter() {
            if col_idx >= seg.zone_maps.len() {
                return None;
            }
            let has_deletes = (0..seg.row_count).any(|ri| dels.contains_key(&(seg.seq, ri)));
            let seg_max = if has_deletes {
                segment_col_max_manual(seg, col_idx, &dels)
            } else {
                match &seg.zone_maps[col_idx].max {
                    Datum::Float64(f) => Some(*f),
                    _ => None,
                }
            };
            if let Some(m) = seg_max {
                global_max = Some(global_max.map_or(m, |g: f64| g.max(m)));
            }
        }
        drop(dels);
        drop(segs);

        let buf = self.write_buffer.read();
        for (i, entry) in buf.iter().enumerate() {
            if entry.is_visible(txn_id, read_ts) && !self.is_deleted(0, i) {
                if let Some(f) = entry.row.values.get(col_idx).and_then(datum_as_f64) {
                    global_max = Some(global_max.map_or(f, |g: f64| g.max(f)));
                }
            }
        }
        global_max
    }
}

// ── Aggregate pushdown helpers ──────────────────────────────────────

fn datum_as_f64(d: &Datum) -> Option<f64> {
    match d {
        Datum::Int32(n) => Some(*n as f64),
        Datum::Int64(n) => Some(*n as f64),
        Datum::Float64(f) => Some(*f),
        Datum::Null => None,
        _ => None,
    }
}

fn segment_col_min_manual(seg: &Segment, col_idx: usize, dels: &HashMap<(u64, usize), TxnId>) -> Option<f64> {
    let col = &seg.columns[col_idx];
    let mut min: Option<f64> = None;
    for ri in 0..seg.row_count {
        if dels.contains_key(&(seg.seq, ri)) {
            continue;
        }
        if let Some(d) = col.get(ri) {
            if let Some(f) = datum_as_f64(&d) {
                min = Some(min.map_or(f, |m: f64| m.min(f)));
            }
        }
    }
    min
}

fn segment_col_max_manual(seg: &Segment, col_idx: usize, dels: &HashMap<(u64, usize), TxnId>) -> Option<f64> {
    let col = &seg.columns[col_idx];
    let mut max: Option<f64> = None;
    for ri in 0..seg.row_count {
        if dels.contains_key(&(seg.seq, ri)) {
            continue;
        }
        if let Some(d) = col.get(ri) {
            if let Some(f) = datum_as_f64(&d) {
                max = Some(max.map_or(f, |m: f64| m.max(f)));
            }
        }
    }
    max
}

// ── Pushdown predicates ─────────────────────────────────────────────

/// A simple pushdown predicate: col_idx op literal (numeric only).
#[derive(Debug, Clone)]
pub struct PushdownPredicate {
    pub col_idx: usize,
    pub op: PushdownOp,
    pub value: f64,
}

#[derive(Debug, Clone, Copy)]
pub enum PushdownOp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
}

impl ColumnStoreTable {
    /// Multi-column scan with optional zone-map pushdown.
    pub fn scan_columnar_projected(
        &self,
        col_indices: &[usize],
        predicate: Option<&PushdownPredicate>,
        _txn_id: TxnId,
        _read_ts: Timestamp,
    ) -> Vec<Vec<Datum>> {
        let num_out = col_indices.len();
        let mut result: Vec<Vec<Datum>> = (0..num_out).map(|_| Vec::new()).collect();

        let segs = self.segments.read();
        for seg in segs.iter() {
            if let Some(pred) = predicate {
                if can_skip_segment(seg, pred) {
                    continue;
                }
            }
            let row_count = seg.row_count;
            for row_idx in 0..row_count {
                if self.is_deleted(seg.seq, row_idx) {
                    continue;
                }
                for (out_idx, &col_idx) in col_indices.iter().enumerate() {
                    let val = if col_idx < seg.columns.len() {
                        seg.columns[col_idx].get(row_idx).unwrap_or(Datum::Null)
                    } else {
                        Datum::Null
                    };
                    result[out_idx].push(val);
                }
            }
        }

        // Write buffer
        let buf = self.write_buffer.read();
        for row in buf.iter() {
            if !row.is_committed() {
                continue;
            }
            for (out_idx, &col_idx) in col_indices.iter().enumerate() {
                result[out_idx].push(row.row.values.get(col_idx).cloned().unwrap_or(Datum::Null));
            }
        }
        result
    }

    pub fn segments_skipped_count(&self, predicate: &PushdownPredicate) -> usize {
        let segs = self.segments.read();
        segs.iter()
            .filter(|s| can_skip_segment(s, predicate))
            .count()
    }
}

fn can_skip_segment(seg: &Segment, pred: &PushdownPredicate) -> bool {
    if pred.col_idx >= seg.zone_maps.len() {
        return false;
    }
    let zm = &seg.zone_maps[pred.col_idx];
    let (min, max) = match (&zm.min, &zm.max) {
        (Datum::Float64(lo), Datum::Float64(hi)) => (*lo, *hi),
        _ => return false,
    };
    match pred.op {
        PushdownOp::Gt => max <= pred.value,
        PushdownOp::Gte => max < pred.value,
        PushdownOp::Lt => min >= pred.value,
        PushdownOp::Lte => min > pred.value,
        PushdownOp::Eq => pred.value < min || pred.value > max,
    }
}

impl ColumnStoreTable {
    /// GC: purge aborted rows from write buffer (uncommitted rows whose txn_id
    /// is below the safepoint are considered aborted and can be removed).
    /// Returns the number of rows purged.
    pub fn gc_purge(&self, safepoint_ts: Timestamp) -> usize {
        let mut buf = self.write_buffer.write();
        let mut pk_idx = self.pk_index.write();
        let before = buf.len();
        buf.retain(|entry| {
            if !entry.is_committed() {
                // Uncommitted row — remove from PK index
                let pk = self.encode_pk_from_row_inner(&entry.row);
                pk_idx.remove(&pk);
                return false;
            }
            true
        });
        let purged = before - buf.len();
        if purged > 0 {
            // Rebuild buffer indices
            for (i, entry) in buf.iter().enumerate() {
                let pk = self.encode_pk_from_row_inner(&entry.row);
                if let Some(loc) = pk_idx.get_mut(&pk) {
                    if matches!(loc, PkLoc::Buffer(_)) {
                        *loc = PkLoc::Buffer(i);
                    }
                }
            }
        }
        let _ = safepoint_ts; // reserved for future version-chain GC
        purged
    }

    /// Clear all data (used by TRUNCATE).
    pub fn truncate(&self) {
        let mut buf = self.write_buffer.write();
        buf.clear();
        let mut segs = self.segments.write();
        segs.clear();
        let mut pk_idx = self.pk_index.write();
        pk_idx.clear();
        let mut dels = self.deletes.write();
        dels.clear();
        self.committed_row_count.store(0, Ordering::Relaxed);
        self.deleted_row_count.store(0, Ordering::Relaxed);
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
                    max_length: None,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "name".to_string(),
                    data_type: falcon_common::types::DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                    max_length: None,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "score".to_string(),
                    data_type: falcon_common::types::DataType::Float64,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                    max_length: None,
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

    fn insert_row(table: &ColumnStoreTable, id: i64, name: &str, score: f64, txn: TxnId) {
        table.insert(
            OwnedRow::new(vec![Datum::Int64(id), Datum::Text(name), Datum::Float64(score)]),
            txn,
        ).unwrap();
    }

    #[test]
    fn test_insert_and_scan() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..10 {
            insert_row(&table, i, &format!("name_{i}"), i as f64 * 1.5, txn);
        }
        table.commit(txn, Timestamp(10));
        let rows = table.scan(txn, Timestamp(100));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn test_mvcc_visibility() {
        let table = ColumnStoreTable::new(test_schema());
        let t1 = TxnId(1);
        let t2 = TxnId(2);
        insert_row(&table, 1, "a", 1.0, t1);
        insert_row(&table, 2, "b", 2.0, t2);

        // t1 uncommitted: only visible to t1
        assert_eq!(table.scan(t1, Timestamp(100)).len(), 1);
        // t2 sees only its own row
        assert_eq!(table.scan(t2, Timestamp(100)).len(), 1);

        // Commit t1 at ts=10
        table.commit(t1, Timestamp(10));
        // t2 with read_ts=5 can't see t1's row (committed at 10)
        assert_eq!(table.scan(t2, Timestamp(5)).len(), 1);
        // t2 with read_ts=10 can see t1's row
        assert_eq!(table.scan(t2, Timestamp(10)).len(), 2);
    }

    #[test]
    fn test_abort_removes_rows() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        insert_row(&table, 1, "a", 1.0, txn);
        insert_row(&table, 2, "b", 2.0, txn);
        assert_eq!(table.scan(txn, Timestamp(100)).len(), 2);

        table.abort(txn);
        assert_eq!(table.scan(txn, Timestamp(100)).len(), 0);
    }

    #[test]
    fn test_duplicate_pk_rejected() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        insert_row(&table, 1, "a", 1.0, txn);
        let result = table.insert(
            OwnedRow::new(vec![Datum::Int64(1), Datum::Text("dup".into()), Datum::Float64(0.0)]),
            txn,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_insert() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        let rows: Vec<OwnedRow> = (0..100).map(|i| {
            OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("r{i}")), Datum::Float64(i as f64)])
        }).collect();
        let count = table.insert_batch(rows, txn).unwrap();
        assert_eq!(count, 100);
        table.commit(txn, Timestamp(10));
        assert_eq!(table.scan(txn, Timestamp(100)).len(), 100);
    }

    #[test]
    fn test_freeze_buffer() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..SEGMENT_ROW_COUNT + 5 {
            let row = OwnedRow::new(vec![
                Datum::Int64(i as i64),
                Datum::Text(format!("row_{i}")),
                Datum::Float64(i as f64),
            ]);
            table.insert(row, txn).unwrap();
        }
        table.commit(txn, Timestamp(10));
        // Explicit freeze after commit (auto-freeze during insert skips uncommitted rows)
        table.freeze_buffer();
        assert!(table.segment_count() >= 1);
        let rows = table.scan(txn, Timestamp(100));
        assert_eq!(rows.len(), SEGMENT_ROW_COUNT + 5);
    }

    #[test]
    fn test_column_scan() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..100 {
            insert_row(&table, i, "x", i as f64, txn);
        }
        table.commit(txn, Timestamp(10));
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
    fn test_point_get() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        insert_row(&table, 42, "answer", 3.14, txn);
        table.commit(txn, Timestamp(10));

        let pk = datum_sort_key(&Datum::Int64(42));
        let row = table.get(&pk, TxnId(2), Timestamp(20));
        assert!(row.is_some());
        assert_eq!(row.unwrap().values[1], Datum::Text("answer".into()));

        // Non-existent PK
        let pk2 = datum_sort_key(&Datum::Int64(999));
        assert!(table.get(&pk2, TxnId(2), Timestamp(20)).is_none());
    }

    #[test]
    fn test_delete_by_pk() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..5 {
            insert_row(&table, i, &format!("r{i}"), 0.0, txn);
        }
        table.commit(txn, Timestamp(10));
        assert_eq!(table.scan(TxnId(2), Timestamp(20)).len(), 5);

        let pk = datum_sort_key(&Datum::Int64(2));
        assert!(table.delete_by_pk(&pk, TxnId(3)));
        assert_eq!(table.scan(TxnId(2), Timestamp(20)).len(), 4);
    }

    #[test]
    fn test_delete_row_legacy() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..10 {
            insert_row(&table, i, &format!("r{i}"), 0.0, txn);
        }
        table.commit(txn, Timestamp(10));
        table.freeze_buffer();
        assert_eq!(table.scan(txn, Timestamp(100)).len(), 10);

        table.delete_row(1, 5);
        assert_eq!(table.scan(txn, Timestamp(100)).len(), 9);
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
        let values: Vec<Datum> = (0..100)
            .map(|i| Datum::Text(format!("v{}", i % 3)))
            .collect();
        let enc = encode_column(&values);
        assert!(matches!(enc, ColumnEncoding::Dictionary { .. }));
        assert_eq!(enc.len(), 100);
    }

    #[test]
    fn test_scan_columnar_projected() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..50i64 {
            insert_row(&table, i, &format!("n{i}"), i as f64 * 2.0, txn);
        }
        table.commit(txn, Timestamp(10));
        table.freeze_buffer();

        let vecs = table.scan_columnar_projected(&[0, 2], None, txn, Timestamp(100));
        assert_eq!(vecs.len(), 2);
        assert_eq!(vecs[0].len(), 50);
        assert_eq!(vecs[1].len(), 50);
        assert_eq!(vecs[0][0], Datum::Int64(0));
        assert_eq!(vecs[1][25], Datum::Float64(50.0));
    }

    #[test]
    fn test_aggregate_pushdown_count() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..100 {
            insert_row(&table, i, &format!("r{i}"), i as f64, txn);
        }
        table.commit(txn, Timestamp(10));
        assert_eq!(table.count_pushdown(TxnId(2), Timestamp(20)), 100);

        // Delete 5 rows
        for i in 0..5 {
            let pk = datum_sort_key(&Datum::Int64(i));
            table.delete_by_pk(&pk, TxnId(3));
        }
        assert_eq!(table.count_pushdown(TxnId(2), Timestamp(20)), 95);
    }

    #[test]
    fn test_aggregate_pushdown_sum() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..10 {
            insert_row(&table, i, &format!("r{i}"), i as f64, txn);
        }
        table.commit(txn, Timestamp(10));
        table.freeze_buffer();

        // sum(score) = 0+1+2+..+9 = 45
        let sum = table.sum_pushdown(2, TxnId(2), Timestamp(20));
        assert!((sum.unwrap() - 45.0).abs() < 0.001);
    }

    #[test]
    fn test_aggregate_pushdown_min_max() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..10 {
            insert_row(&table, i, &format!("r{i}"), (i * 10) as f64, txn);
        }
        table.commit(txn, Timestamp(10));
        table.freeze_buffer();

        assert!((table.min_pushdown(2, TxnId(2), Timestamp(20)).unwrap() - 0.0).abs() < 0.001);
        assert!((table.max_pushdown(2, TxnId(2), Timestamp(20)).unwrap() - 90.0).abs() < 0.001);
    }

    #[test]
    fn test_datum_eq_structural() {
        assert!(datum_eq(&Datum::Int64(42), &Datum::Int64(42)));
        assert!(!datum_eq(&Datum::Int64(42), &Datum::Int64(43)));
        assert!(datum_eq(&Datum::Text("abc".into()), &Datum::Text("abc".into())));
        assert!(datum_eq(&Datum::Null, &Datum::Null));
        assert!(datum_eq(&Datum::Int32(5), &Datum::Int64(5))); // cross-int promotion
        assert!(!datum_eq(&Datum::Int32(5), &Datum::Text("5".into())));
    }

    #[test]
    fn test_compaction() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);

        // Create many small segments by freezing small batches.
        for batch in 0..6 {
            for i in 0..10 {
                let id = batch * 100 + i;
                insert_row(&table, id, &format!("r{id}"), id as f64, txn);
            }
            table.commit(txn, Timestamp(10 + batch as u64));
            table.freeze_buffer();
        }
        assert!(table.segment_count() >= 4);

        let merged = table.compact();
        assert!(merged >= 2);

        // Verify data integrity after compaction.
        let rows = table.scan(TxnId(2), Timestamp(100));
        assert_eq!(rows.len(), 60);
    }

    #[test]
    fn test_zone_map_pushdown_skips_segment() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);

        // Segment 1: scores 0..99
        for i in 0..100i64 {
            table
                .insert(
                    OwnedRow::new(vec![
                        Datum::Int64(i),
                        Datum::Text("a".into()),
                        Datum::Float64(i as f64),
                    ]),
                    txn,
                )
                .unwrap();
        }
        table.commit(txn, Timestamp(10));
        table.freeze_buffer();
        assert_eq!(table.segment_count(), 1);

        // Predicate: score > 200 → should skip the segment (max=99)
        let pred = super::PushdownPredicate {
            col_idx: 2,
            op: super::PushdownOp::Gt,
            value: 200.0,
        };
        assert_eq!(table.segments_skipped_count(&pred), 1);

        let vecs = table.scan_columnar_projected(&[0, 2], Some(&pred), txn, Timestamp(100));
        assert_eq!(vecs[0].len(), 0, "all rows should be skipped via zone map");
    }

    #[test]
    fn test_gc_purge_removes_uncommitted() {
        let table = ColumnStoreTable::new(test_schema());
        insert_row(&table, 1, "committed", 1.0, TxnId(1));
        insert_row(&table, 2, "aborted", 2.0, TxnId(2));
        table.commit(TxnId(1), Timestamp(10));
        // TxnId(2) never committed

        let purged = table.gc_purge(Timestamp(20));
        assert_eq!(purged, 1);

        let rows = table.scan(TxnId(3), Timestamp(30));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[0], Datum::Int64(1));
    }

    #[test]
    fn test_truncate_clears_all() {
        let table = ColumnStoreTable::new(test_schema());
        for i in 0..10 {
            insert_row(&table, i, &format!("r{i}"), i as f64, TxnId(1));
        }
        table.commit(TxnId(1), Timestamp(10));
        table.freeze_buffer();
        assert!(table.segment_count() > 0);

        table.truncate();
        assert_eq!(table.segment_count(), 0);
        assert_eq!(table.row_count_approx(), 0);
        let rows = table.scan(TxnId(2), Timestamp(20));
        assert!(rows.is_empty());
    }

    #[test]
    fn test_abort_removes_uncommitted_rows() {
        let table = ColumnStoreTable::new(test_schema());
        insert_row(&table, 1, "keep", 1.0, TxnId(1));
        insert_row(&table, 2, "remove", 2.0, TxnId(2));
        table.commit(TxnId(1), Timestamp(10));

        table.abort(TxnId(2));

        // Only committed row visible
        let rows = table.scan(TxnId(3), Timestamp(20));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[0], Datum::Int64(1));

        // PK 2 should be free for reuse
        insert_row(&table, 2, "reinserted", 2.0, TxnId(3));
        table.commit(TxnId(3), Timestamp(20));
        let rows = table.scan(TxnId(4), Timestamp(30));
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_commit_then_scan_visibility() {
        let table = ColumnStoreTable::new(test_schema());
        insert_row(&table, 1, "a", 1.0, TxnId(10));
        insert_row(&table, 2, "b", 2.0, TxnId(10));

        // Before commit: only own txn sees rows
        let own = table.scan(TxnId(10), Timestamp(5));
        assert_eq!(own.len(), 2);
        let other = table.scan(TxnId(20), Timestamp(5));
        assert_eq!(other.len(), 0);

        // After commit at ts=10
        table.commit(TxnId(10), Timestamp(10));
        let visible = table.scan(TxnId(20), Timestamp(15));
        assert_eq!(visible.len(), 2);
        let too_early = table.scan(TxnId(20), Timestamp(5));
        assert_eq!(too_early.len(), 0);
    }

    #[test]
    fn test_zone_map_no_skip_when_in_range() {
        let table = ColumnStoreTable::new(test_schema());
        let txn = TxnId(1);
        for i in 0..100i64 {
            table
                .insert(
                    OwnedRow::new(vec![
                        Datum::Int64(i),
                        Datum::Text("b".into()),
                        Datum::Float64(i as f64),
                    ]),
                    txn,
                )
                .unwrap();
        }
        table.commit(txn, Timestamp(10));
        table.freeze_buffer();

        // Predicate: score > 50 → should NOT skip (max=99 > 50)
        let pred = super::PushdownPredicate {
            col_idx: 2,
            op: super::PushdownOp::Gt,
            value: 50.0,
        };
        assert_eq!(table.segments_skipped_count(&pred), 0);

        let vecs = table.scan_columnar_projected(&[2], Some(&pred), txn, Timestamp(100));
        assert_eq!(
            vecs[0].len(),
            100,
            "no segment-level pruning, row-level filter not applied here"
        );
    }
}
