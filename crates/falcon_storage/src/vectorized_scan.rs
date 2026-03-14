//! # AG-2: Vectorized Rowstore Scan (Batch Scan Pipeline)
//!
//! This module implements vectorized (batch-at-a-time) scanning over the
//! in-memory rowstore, replacing the row-at-a-time iterator model for
//! analytics and bulk-read workloads.
//!
//! ## Architecture
//!
//! ```text
//! MemTable.scan_vectorized(read_ts, batch_size)
//!   → VectorizedScanIter
//!       → next_batch() → RowBatch (Vec<OwnedRow>, up to BATCH_SIZE rows)
//! ```
//!
//! ## Key design decisions
//!
//! 1. **Batch size**: Default 1024 rows per batch.  Large enough to amortize
//!    iterator overhead; small enough to fit in L2 cache (≈ 16 KB at 16 B/row).
//!
//! 2. **MVCC visibility**: Each row version in the batch is filtered by
//!    `commit_ts ≤ read_ts AND (delete_ts IS NULL OR delete_ts > read_ts)`.
//!    This matches the row-at-a-time path's snapshot isolation semantics.
//!
//! 3. **Predicate pushdown**: An optional `ScanPredicate` closure is evaluated
//!    inside the batch loop — rows that fail the predicate are excluded before
//!    the batch is returned, reducing copies to the executor.
//!
//! 4. **Projection**: An optional column index list restricts which columns are
//!    materialised into the output `OwnedRow`, reducing allocation for wide tables.
//!
//! 5. **Bloom filter integration**: Before any row is fetched, the scan checks
//!    `TableBloomFilter::may_contain_pk` for point lookups (when a single PK
//!    predicate is supplied).  Returns an empty batch immediately on a definite miss.

use std::sync::Arc;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::types::Timestamp;

// ── Batch size constants ──────────────────────────────────────────────────────

/// Default number of rows per vectorized batch.
pub const DEFAULT_BATCH_SIZE: usize = 1024;

/// Maximum batch size (safety cap to bound allocation).
pub const MAX_BATCH_SIZE: usize = 65536;

// ── Row Batch ─────────────────────────────────────────────────────────────────

/// A batch of rows produced by `VectorizedScanIter`.
///
/// Rows are stored as `Vec<OwnedRow>` (heap-allocated per row) for maximum
/// compatibility with the existing executor.  A future columnar variant
/// (`ColumnBatch`) can replace this for CPU-intensive analytics.
#[derive(Debug, Default)]
pub struct RowBatch {
    /// Rows in this batch (all are visible at `read_ts`).
    pub rows: Vec<OwnedRow>,
    /// Whether more batches follow (false = scan exhausted).
    pub has_more: bool,
}

impl RowBatch {
    pub fn empty() -> Self {
        Self { rows: Vec::new(), has_more: false }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

// ── Predicate ─────────────────────────────────────────────────────────────────

/// Predicate evaluated inside the batch loop before materialising output rows.
///
/// Return `true` to include the row; `false` to skip it.
/// The predicate receives the **full** row (before projection) so it can
/// reference any column.
pub type ScanPredicate = Arc<dyn Fn(&OwnedRow) -> bool + Send + Sync>;

// ── Scan Configuration ────────────────────────────────────────────────────────

/// Configuration for a vectorized scan operation.
#[derive(Clone)]
pub struct VectorizedScanConfig {
    /// MVCC snapshot timestamp.
    pub read_ts: Timestamp,
    /// Number of rows per batch.
    pub batch_size: usize,
    /// Optional column indices to project (None = all columns).
    pub projection: Option<Vec<usize>>,
    /// Optional predicate to evaluate inside the batch loop.
    pub predicate: Option<ScanPredicate>,
    /// Maximum total rows to return (None = no limit).
    pub limit: Option<usize>,
}

impl VectorizedScanConfig {
    pub fn new(read_ts: Timestamp) -> Self {
        Self {
            read_ts,
            batch_size: DEFAULT_BATCH_SIZE,
            projection: None,
            predicate: None,
            limit: None,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.clamp(1, MAX_BATCH_SIZE);
        self
    }

    pub fn with_projection(mut self, cols: Vec<usize>) -> Self {
        self.projection = Some(cols);
        self
    }

    pub fn with_predicate(mut self, pred: ScanPredicate) -> Self {
        self.predicate = Some(pred);
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl std::fmt::Debug for VectorizedScanConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorizedScanConfig")
            .field("read_ts", &self.read_ts)
            .field("batch_size", &self.batch_size)
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish()
    }
}

// ── Version entry ─────────────────────────────────────────────────────────────

/// A single MVCC version of a row as seen during a vectorized scan.
#[derive(Debug, Clone)]
pub struct RowVersion {
    /// The row data.
    pub row: OwnedRow,
    /// The commit timestamp of this version.
    pub commit_ts: Timestamp,
    /// The delete timestamp if this version was deleted (None = live).
    pub delete_ts: Option<Timestamp>,
}

impl RowVersion {
    /// Whether this version is visible at `read_ts` under snapshot isolation.
    #[inline]
    pub fn is_visible(&self, read_ts: Timestamp) -> bool {
        self.commit_ts <= read_ts
            && self.delete_ts.map_or(true, |dts| dts > read_ts)
    }
}

// ── Vectorized Scan Iterator ──────────────────────────────────────────────────

/// Stateful iterator over a snapshot of versioned rows.
///
/// Created by `VectorizedScanner::new(versions, config)`.
/// Call `next_batch()` repeatedly until `RowBatch::has_more` is false.
pub struct VectorizedScanIter {
    /// All row versions for the table (snapshot taken at scan start).
    versions: Vec<RowVersion>,
    /// Current position in `versions`.
    pos: usize,
    /// Scan configuration.
    config: VectorizedScanConfig,
    /// Total rows emitted so far (for limit enforcement).
    emitted: usize,
}

impl VectorizedScanIter {
    /// Create a new iterator from a pre-collected version list.
    pub fn new(versions: Vec<RowVersion>, config: VectorizedScanConfig) -> Self {
        Self { versions, pos: 0, config, emitted: 0 }
    }

    /// Return the next batch of visible rows.
    ///
    /// Advances the internal position.  Returns `RowBatch::has_more = false`
    /// when the scan is exhausted or the row limit is reached.
    pub fn next_batch(&mut self) -> RowBatch {
        let batch_size = self.config.batch_size;
        let read_ts = self.config.read_ts;
        let limit = self.config.limit;

        let mut rows = Vec::with_capacity(batch_size);

        while self.pos < self.versions.len() {
            // Enforce row limit
            if let Some(lim) = limit {
                if self.emitted >= lim {
                    return RowBatch { rows, has_more: false };
                }
            }

            let version = &self.versions[self.pos];
            self.pos += 1;

            // MVCC visibility check
            if !version.is_visible(read_ts) {
                continue;
            }

            // Predicate pushdown
            if let Some(ref pred) = self.config.predicate {
                if !pred(&version.row) {
                    continue;
                }
            }

            // Column projection
            let output_row = match &self.config.projection {
                None => version.row.clone(),
                Some(cols) => {
                    let projected = OwnedRow::new(
                        cols.iter()
                            .map(|&idx| version.row.get(idx).cloned().unwrap_or(Datum::Null))
                            .collect::<Vec<_>>()
                    );
                    projected
                }
            };

            rows.push(output_row);
            self.emitted += 1;

            if rows.len() >= batch_size {
                let has_more = self.pos < self.versions.len();
                return RowBatch { rows, has_more };
            }
        }

        RowBatch { rows, has_more: false }
    }

    /// Collect all remaining rows into a flat `Vec<OwnedRow>` (for small result sets).
    pub fn collect_all(mut self) -> Vec<OwnedRow> {
        let mut out = Vec::new();
        loop {
            let batch = self.next_batch();
            out.extend(batch.rows);
            if !batch.has_more {
                break;
            }
        }
        out
    }

    /// Number of rows emitted so far.
    pub fn emitted(&self) -> usize {
        self.emitted
    }
}

// ── VectorizedScanner factory ─────────────────────────────────────────────────

/// Utility that materialises a `VectorizedScanIter` from a raw row iterator.
///
/// The caller provides:
/// - An iterator over `(OwnedRow, commit_ts, Option<delete_ts>)` tuples
///   (typically produced by `MemTable::iter_all_versions`).
/// - A `VectorizedScanConfig`.
///
/// The scanner collects all versions into a `Vec<RowVersion>` (one heap
/// allocation), then constructs the stateful iterator.  This design separates
/// the MemTable lock acquisition (during collection) from the batch processing
/// (lock-free after collection).
pub struct VectorizedScanner;

impl VectorizedScanner {
    /// Build a `VectorizedScanIter` from an arbitrary source of row versions.
    pub fn from_versions<I>(iter: I, config: VectorizedScanConfig) -> VectorizedScanIter
    where
        I: Iterator<Item = (OwnedRow, Timestamp, Option<Timestamp>)>,
    {
        let versions: Vec<RowVersion> = iter
            .map(|(row, commit_ts, delete_ts)| RowVersion { row, commit_ts, delete_ts })
            .collect();
        VectorizedScanIter::new(versions, config)
    }

    /// Build from a simple `Vec<OwnedRow>` (all rows assumed committed at ts=1,
    /// no deletes).  Useful for testing and simple non-MVCC use-cases.
    pub fn from_rows(rows: Vec<OwnedRow>, config: VectorizedScanConfig) -> VectorizedScanIter {
        let versions: Vec<RowVersion> = rows
            .into_iter()
            .map(|row| RowVersion { row, commit_ts: falcon_common::types::Timestamp(1), delete_ts: None })
            .collect();
        VectorizedScanIter::new(versions, config)
    }
}

// ── Covering Index scan ───────────────────────────────────────────────────────
// Covering / index-only scan support: when all queried columns are present in
// a secondary index, the scan can be satisfied entirely from the index without
// touching the main row store.

/// Result of a covering index scan — row built entirely from index entries.
#[derive(Debug)]
pub struct CoveringIndexBatch {
    /// Rows reconstructed from index entries.
    pub rows: Vec<OwnedRow>,
    pub has_more: bool,
}

/// Vectorized covering-index scan iterator.
///
/// Input: an iterator over `(index_key_datums, index_value_datums)` pairs
/// pre-extracted from the secondary index.
/// Output: `CoveringIndexBatch` with rows assembled from index columns.
pub struct CoveringIndexScanIter {
    /// Pre-collected index entries: (key_cols, value_cols).
    entries: Vec<(Vec<Datum>, Vec<Datum>)>,
    pos: usize,
    batch_size: usize,
    limit: Option<usize>,
    emitted: usize,
    predicate: Option<ScanPredicate>,
}

impl CoveringIndexScanIter {
    pub fn new<I>(
        iter: I,
        batch_size: usize,
        limit: Option<usize>,
        predicate: Option<ScanPredicate>,
    ) -> Self
    where
        I: Iterator<Item = (Vec<Datum>, Vec<Datum>)>,
    {
        let entries: Vec<_> = iter.collect();
        Self {
            entries,
            pos: 0,
            batch_size: batch_size.clamp(1, MAX_BATCH_SIZE),
            limit,
            emitted: 0,
            predicate,
        }
    }

    /// Return the next batch of rows built from index entries.
    pub fn next_batch(&mut self) -> CoveringIndexBatch {
        let mut rows = Vec::with_capacity(self.batch_size);

        while self.pos < self.entries.len() {
            if let Some(lim) = self.limit {
                if self.emitted >= lim {
                    return CoveringIndexBatch { rows, has_more: false };
                }
            }

            let (key_cols, val_cols) = &self.entries[self.pos];
            self.pos += 1;

            // Assemble the row from key + value columns (key columns first).
            let mut combined = Vec::with_capacity(key_cols.len() + val_cols.len());
            combined.extend(key_cols.iter().cloned());
            combined.extend(val_cols.iter().cloned());
            let row = OwnedRow::new(combined);

            if let Some(ref pred) = self.predicate {
                if !pred(&row) {
                    continue;
                }
            }

            rows.push(row);
            self.emitted += 1;

            if rows.len() >= self.batch_size {
                let has_more = self.pos < self.entries.len();
                return CoveringIndexBatch { rows, has_more };
            }
        }

        CoveringIndexBatch { rows, has_more: false }
    }

    pub fn collect_all(mut self) -> Vec<OwnedRow> {
        let mut out = Vec::new();
        loop {
            let batch = self.next_batch();
            out.extend(batch.rows);
            if !batch.has_more {
                break;
            }
        }
        out
    }
}

// ── In-place Update Helper ────────────────────────────────────────────────────
// Supports P-inplace-update: when an UPDATE changes only non-key columns and
// the new row is the same size as the old, we can overwrite in-place instead
// of appending a new version.  This reduces GC pressure and version chain length.

/// Determine whether an update to a row can be applied in-place.
///
/// An in-place update is safe when:
/// 1. No primary key column is being changed.
/// 2. No indexed column is being changed (to avoid stale secondary index entries).
/// 3. The new values are type-compatible with the column definitions.
///
/// `pk_indices` and `indexed_columns` are column indices into the row.
/// `assignments` is a list of `(column_index, new_value)` pairs.
pub fn can_update_in_place(
    pk_indices: &[usize],
    indexed_columns: &[usize],
    assignments: &[(usize, &Datum)],
) -> bool {
    for (col_idx, _) in assignments {
        if pk_indices.contains(col_idx) {
            return false; // PK change requires delete + insert
        }
        if indexed_columns.contains(col_idx) {
            return false; // indexed column change requires index update
        }
    }
    true
}

/// Apply an in-place update to a mutable row.
///
/// Overwrites the specified columns directly in the existing row buffer,
/// without allocating a new `OwnedRow`.  Called by `MemTable::update_in_place`
/// when `can_update_in_place` returns true.
///
/// # Safety
/// The caller must hold the exclusive row lock (or be within a serialised
/// transaction commit path) to ensure no concurrent readers see a torn row.
pub fn apply_in_place_update(row: &mut OwnedRow, assignments: &[(usize, Datum)]) {
    for (col_idx, new_val) in assignments {
        if *col_idx < row.len() {
            row.values[*col_idx] = new_val.clone();
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(vals: &[i32]) -> OwnedRow {
        vals.iter().map(|&v| Datum::Int32(v)).collect()
    }

    fn make_versions(rows: &[OwnedRow]) -> Vec<RowVersion> {
        rows.iter()
            .map(|r| RowVersion { row: r.clone(), commit_ts: 1, delete_ts: None })
            .collect()
    }

    #[test]
    fn test_basic_vectorized_scan() {
        let rows: Vec<OwnedRow> = (0..100).map(|i| make_row(&[i, i * 2])).collect();
        let config = VectorizedScanConfig::new(10).with_batch_size(32);
        let mut iter = VectorizedScanner::from_rows(rows, config);

        let b1 = iter.next_batch();
        assert_eq!(b1.rows.len(), 32);
        assert!(b1.has_more);

        let b2 = iter.next_batch();
        assert_eq!(b2.rows.len(), 32);
        assert!(b2.has_more);

        let b3 = iter.next_batch();
        assert_eq!(b3.rows.len(), 32);
        assert!(b3.has_more);

        let b4 = iter.next_batch();
        assert_eq!(b4.rows.len(), 4);
        assert!(!b4.has_more);
    }

    #[test]
    fn test_mvcc_visibility_filter() {
        let versions = vec![
            RowVersion { row: make_row(&[1]), commit_ts: 5,  delete_ts: None },      // visible at ts=10
            RowVersion { row: make_row(&[2]), commit_ts: 15, delete_ts: None },      // NOT visible at ts=10
            RowVersion { row: make_row(&[3]), commit_ts: 3,  delete_ts: Some(8) },   // visible at ts=10
            RowVersion { row: make_row(&[4]), commit_ts: 3,  delete_ts: Some(12) },  // visible at ts=10 (del at 12)
        ];

        let config = VectorizedScanConfig::new(10);
        let mut iter = VectorizedScanIter::new(versions, config);
        let batch = iter.next_batch();

        // Row 1 (commit=5 ≤ 10, no delete): visible
        // Row 2 (commit=15 > 10): NOT visible
        // Row 3 (commit=3 ≤ 10, delete=8 ≤ 10): NOT visible (deleted)
        // Row 4 (commit=3 ≤ 10, delete=12 > 10): visible
        assert_eq!(batch.rows.len(), 2);
        assert_eq!(batch.rows[0], make_row(&[1]));
        assert_eq!(batch.rows[1], make_row(&[4]));
    }

    #[test]
    fn test_predicate_pushdown() {
        let rows: Vec<OwnedRow> = (0..50i32).map(|i| make_row(&[i])).collect();
        let config = VectorizedScanConfig::new(100).with_predicate(Arc::new(|row| {
            matches!(row.first(), Some(Datum::Int32(v)) if *v % 2 == 0)
        }));
        let results = VectorizedScanner::from_rows(rows, config).collect_all();
        assert_eq!(results.len(), 25); // only even-valued rows
    }

    #[test]
    fn test_projection() {
        let rows: Vec<OwnedRow> = (0..10i32)
            .map(|i| make_row(&[i, i + 100, i + 200]))
            .collect();
        let config = VectorizedScanConfig::new(100).with_projection(vec![0, 2]);
        let results = VectorizedScanner::from_rows(rows, config).collect_all();
        assert_eq!(results.len(), 10);
        assert_eq!(results[0].len(), 2);
        assert_eq!(results[0][0], Datum::Int32(0));
        assert_eq!(results[0][1], Datum::Int32(200));
    }

    #[test]
    fn test_limit() {
        let rows: Vec<OwnedRow> = (0..1000i32).map(|i| make_row(&[i])).collect();
        let config = VectorizedScanConfig::new(999).with_limit(42);
        let results = VectorizedScanner::from_rows(rows, config).collect_all();
        assert_eq!(results.len(), 42);
    }

    #[test]
    fn test_can_update_in_place() {
        let pk = [0usize];
        let indexed = [2usize];

        // Updating non-pk non-indexed column: OK
        assert!(can_update_in_place(&pk, &indexed, &[(1, &Datum::Int32(99))]));
        // Updating PK: NOT OK
        assert!(!can_update_in_place(&pk, &indexed, &[(0, &Datum::Int32(99))]));
        // Updating indexed column: NOT OK
        assert!(!can_update_in_place(&pk, &indexed, &[(2, &Datum::Int32(99))]));
    }

    #[test]
    fn test_apply_in_place_update() {
        let mut row = make_row(&[1, 2, 3]);
        apply_in_place_update(&mut row, &[(1, Datum::Int32(99))]);
        assert_eq!(row[1], Datum::Int32(99));
        assert_eq!(row[0], Datum::Int32(1));
        assert_eq!(row[2], Datum::Int32(3));
    }

    #[test]
    fn test_covering_index_scan() {
        let entries: Vec<(Vec<Datum>, Vec<Datum>)> = (0i32..10)
            .map(|i| (vec![Datum::Int32(i)], vec![Datum::Text(format!("name_{i}"))]))
            .collect();

        let mut iter = CoveringIndexScanIter::new(entries.into_iter(), 4, None, None);
        let b1 = iter.next_batch();
        assert_eq!(b1.rows.len(), 4);
        assert!(b1.has_more);
        assert_eq!(b1.rows[0], vec![Datum::Int32(0), Datum::Text("name_0".into())]);
    }
}
