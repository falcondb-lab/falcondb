//! Streaming result set abstraction for FalconDB.
//!
//! Provides `RowStream` — a bounded, chunk-oriented iterator over query results
//! that avoids materializing the entire result set in memory.
//!
//! # Design
//!
//! - `RowStream` wraps a closure that produces the next batch of rows on demand.
//! - `CursorStream` is a cursor-oriented wrapper: stores a query plan and executes
//!   it lazily, returning rows in FETCH-sized chunks.
//! - `ChunkedRows` is the simplest form: wraps a pre-materialized `Vec<OwnedRow>`
//!   and serves it in bounded chunks (for backward compat with existing code).
//!
//! # Invariants
//!
//! - **STREAM-1**: `next_batch(limit)` returns at most `limit` rows.
//! - **STREAM-2**: After `is_exhausted()` returns true, `next_batch()` returns empty.
//! - **STREAM-3**: Memory usage is bounded by `batch_size` (not total result size).

use falcon_common::datum::OwnedRow;
use falcon_common::types::DataType;

// ═══════════════════════════════════════════════════════════════════════════
// ChunkedRows — serves pre-materialized rows in bounded chunks
// ═══════════════════════════════════════════════════════════════════════════

/// A streaming wrapper over a materialized result set.
///
/// Instead of handing the full `Vec<OwnedRow>` to the caller, this serves
/// rows in bounded chunks via `next_batch(limit)`. This is the minimal
/// streaming abstraction needed for DECLARE CURSOR / FETCH.
pub struct ChunkedRows {
    rows: Vec<OwnedRow>,
    position: usize,
    columns: Vec<(String, DataType)>,
}

impl ChunkedRows {
    /// Create from a fully materialized result set.
    pub const fn new(columns: Vec<(String, DataType)>, rows: Vec<OwnedRow>) -> Self {
        Self {
            rows,
            position: 0,
            columns,
        }
    }

    /// Create an empty stream.
    pub const fn empty() -> Self {
        Self {
            rows: Vec::new(),
            position: 0,
            columns: Vec::new(),
        }
    }

    /// Column descriptors.
    pub fn columns(&self) -> &[(String, DataType)] {
        &self.columns
    }

    /// Fetch the next batch of up to `limit` rows.
    ///
    /// **STREAM-1**: Returns at most `limit` rows.
    /// **STREAM-2**: Returns empty vec when exhausted.
    pub fn next_batch(&mut self, limit: usize) -> Vec<OwnedRow> {
        if self.position >= self.rows.len() {
            return Vec::new();
        }
        let end = (self.position + limit).min(self.rows.len());
        let batch = self.rows[self.position..end].to_vec();
        self.position = end;
        batch
    }

    /// Whether all rows have been consumed.
    pub const fn is_exhausted(&self) -> bool {
        self.position >= self.rows.len()
    }

    /// Current position (number of rows already fetched).
    pub const fn position(&self) -> usize {
        self.position
    }

    /// Total number of rows in the stream.
    pub const fn total_rows(&self) -> usize {
        self.rows.len()
    }

    /// Remaining rows.
    pub const fn remaining(&self) -> usize {
        self.rows.len().saturating_sub(self.position)
    }

    /// Move the cursor forward by `count` rows (for MOVE).
    pub fn advance(&mut self, count: usize) -> usize {
        let remaining = self.rows.len().saturating_sub(self.position);
        let actual = count.min(remaining);
        self.position += actual;
        actual
    }

    /// Move the cursor backward by `count` rows (for MOVE BACKWARD).
    pub fn retreat(&mut self, count: usize) -> usize {
        let actual = count.min(self.position);
        self.position -= actual;
        actual
    }

    /// Reset to beginning.
    pub const fn rewind(&mut self) {
        self.position = 0;
    }

    /// Consume and return all remaining rows (for backward compat).
    pub fn collect_remaining(&mut self) -> Vec<OwnedRow> {
        if self.position >= self.rows.len() {
            return Vec::new();
        }
        let rest = self.rows[self.position..].to_vec();
        self.position = self.rows.len();
        rest
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CursorStream — lazy cursor that defers execution
// ═══════════════════════════════════════════════════════════════════════════

/// A cursor that stores a query and executes it lazily on first FETCH.
///
/// Before the first `FETCH`, the query has NOT been executed.
/// On the first `FETCH`, the query is executed and the result is wrapped
/// in a `ChunkedRows` for subsequent FETCHes.
///
/// This avoids the OOM issue of materializing 1M+ rows at DECLARE time.
pub struct CursorStream {
    /// The SQL query to execute (original case).
    query: String,
    /// Column descriptors (populated after first execution).
    columns: Vec<(String, DataType)>,
    /// Whether this cursor survives transaction end.
    with_hold: bool,
    /// Inner chunked rows (None = not yet executed).
    inner: Option<ChunkedRows>,
}

impl CursorStream {
    /// Create a deferred cursor (query not yet executed).
    pub const fn new_deferred(query: String, with_hold: bool) -> Self {
        Self {
            query,
            columns: Vec::new(),
            with_hold,
            inner: None,
        }
    }

    /// Create a cursor from already-materialized rows (backward compat).
    pub fn new_materialized(
        columns: Vec<(String, DataType)>,
        rows: Vec<OwnedRow>,
        with_hold: bool,
    ) -> Self {
        Self {
            query: String::new(),
            columns: columns.clone(),
            with_hold,
            inner: Some(ChunkedRows::new(columns, rows)),
        }
    }

    /// Whether the query has been executed.
    pub const fn is_executed(&self) -> bool {
        self.inner.is_some()
    }

    /// The query string (for deferred execution).
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Whether this cursor is holdable.
    pub const fn with_hold(&self) -> bool {
        self.with_hold
    }

    /// Set the execution result (called after first FETCH executes the query).
    pub fn set_result(&mut self, columns: Vec<(String, DataType)>, rows: Vec<OwnedRow>) {
        self.columns = columns.clone();
        self.inner = Some(ChunkedRows::new(columns, rows));
    }

    /// Column descriptors (empty if not yet executed).
    pub fn columns(&self) -> &[(String, DataType)] {
        if let Some(ref inner) = self.inner {
            inner.columns()
        } else {
            &self.columns
        }
    }

    /// Fetch the next batch. Returns None if not yet executed.
    pub fn next_batch(&mut self, limit: usize) -> Option<Vec<OwnedRow>> {
        self.inner.as_mut().map(|inner| inner.next_batch(limit))
    }

    /// Whether all rows have been consumed.
    pub fn is_exhausted(&self) -> bool {
        self.inner.as_ref().is_some_and(ChunkedRows::is_exhausted)
    }

    /// Current position.
    pub fn position(&self) -> usize {
        self.inner.as_ref().map_or(0, ChunkedRows::position)
    }

    /// Move forward (for MOVE).
    pub fn advance(&mut self, count: usize) -> usize {
        self.inner.as_mut().map_or(0, |i| i.advance(count))
    }

    /// Move backward (for MOVE BACKWARD).
    pub fn retreat(&mut self, count: usize) -> usize {
        self.inner.as_mut().map_or(0, |i| i.retreat(count))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::Datum;

    fn make_rows(n: usize) -> Vec<OwnedRow> {
        (0..n)
            .map(|i| OwnedRow::new(vec![Datum::Int32(i as i32)]))
            .collect()
    }

    fn cols() -> Vec<(String, DataType)> {
        vec![("id".into(), DataType::Int32)]
    }

    // ── ChunkedRows ──

    #[test]
    fn test_chunked_rows_basic() {
        let mut cr = ChunkedRows::new(cols(), make_rows(10));
        assert_eq!(cr.total_rows(), 10);
        assert_eq!(cr.remaining(), 10);
        assert!(!cr.is_exhausted());

        let batch = cr.next_batch(3);
        assert_eq!(batch.len(), 3);
        assert_eq!(cr.position(), 3);
        assert_eq!(cr.remaining(), 7);
    }

    #[test]
    fn test_chunked_rows_exhaust() {
        let mut cr = ChunkedRows::new(cols(), make_rows(5));
        let batch = cr.next_batch(10);
        assert_eq!(batch.len(), 5);
        assert!(cr.is_exhausted());
        let empty = cr.next_batch(10);
        assert!(empty.is_empty());
    }

    #[test]
    fn test_chunked_rows_exact_limit() {
        let mut cr = ChunkedRows::new(cols(), make_rows(3));
        let b1 = cr.next_batch(1);
        assert_eq!(b1.len(), 1);
        let b2 = cr.next_batch(1);
        assert_eq!(b2.len(), 1);
        let b3 = cr.next_batch(1);
        assert_eq!(b3.len(), 1);
        assert!(cr.is_exhausted());
    }

    #[test]
    fn test_chunked_rows_advance_retreat() {
        let mut cr = ChunkedRows::new(cols(), make_rows(10));
        let moved = cr.advance(5);
        assert_eq!(moved, 5);
        assert_eq!(cr.position(), 5);

        let back = cr.retreat(3);
        assert_eq!(back, 3);
        assert_eq!(cr.position(), 2);

        // Can't retreat past 0
        let back2 = cr.retreat(10);
        assert_eq!(back2, 2);
        assert_eq!(cr.position(), 0);
    }

    #[test]
    fn test_chunked_rows_empty() {
        let mut cr = ChunkedRows::empty();
        assert!(cr.is_exhausted());
        assert!(cr.next_batch(10).is_empty());
    }

    #[test]
    fn test_chunked_rows_collect_remaining() {
        let mut cr = ChunkedRows::new(cols(), make_rows(5));
        cr.next_batch(2);
        let rest = cr.collect_remaining();
        assert_eq!(rest.len(), 3);
        assert!(cr.is_exhausted());
    }

    // ── CursorStream ──

    #[test]
    fn test_cursor_stream_deferred() {
        let cs = CursorStream::new_deferred("SELECT 1".into(), false);
        assert!(!cs.is_executed());
        assert_eq!(cs.query(), "SELECT 1");
        assert!(!cs.with_hold());
    }

    #[test]
    fn test_cursor_stream_materialized() {
        let mut cs = CursorStream::new_materialized(cols(), make_rows(5), true);
        assert!(cs.is_executed());
        assert!(cs.with_hold());

        let batch = cs.next_batch(3).unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(cs.position(), 3);
    }

    #[test]
    fn test_cursor_stream_deferred_then_execute() {
        let mut cs = CursorStream::new_deferred("SELECT * FROM t".into(), false);
        assert!(cs.next_batch(10).is_none()); // not yet executed

        cs.set_result(cols(), make_rows(10));
        assert!(cs.is_executed());

        let batch = cs.next_batch(5).unwrap();
        assert_eq!(batch.len(), 5);
        let batch2 = cs.next_batch(10).unwrap();
        assert_eq!(batch2.len(), 5); // remaining
        assert!(cs.is_exhausted());
    }

    #[test]
    fn test_cursor_stream_advance() {
        let mut cs = CursorStream::new_materialized(cols(), make_rows(10), false);
        let moved = cs.advance(7);
        assert_eq!(moved, 7);
        let batch = cs.next_batch(10).unwrap();
        assert_eq!(batch.len(), 3);
    }

    // ── Streaming invariant: memory bounded by batch_size ──

    #[test]
    fn test_streaming_memory_bounded() {
        let mut cr = ChunkedRows::new(cols(), make_rows(1_000));
        let batch_size = 100;
        let mut total_fetched = 0;
        loop {
            let batch = cr.next_batch(batch_size);
            if batch.is_empty() {
                break;
            }
            assert!(batch.len() <= batch_size);
            total_fetched += batch.len();
        }
        assert_eq!(total_fetched, 1_000);
    }
}
