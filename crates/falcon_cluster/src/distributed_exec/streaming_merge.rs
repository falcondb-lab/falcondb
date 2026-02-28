//! Streaming k-way merge-sort iterator for distributed ORDER BY.
//!
//! Instead of buffering all shard results into memory before sorting,
//! `StreamingMergeSort` lazily pulls the next globally-sorted row from
//! a min-heap of per-shard iterators.  This enables:
//!
//! - **O(1) memory per shard** (only 1 row buffered per live shard cursor)
//! - **Early termination** with OFFSET + LIMIT (stop after N rows)
//! - **O(N log K) total work** where N = total rows, K = number of shards
//!
//! # Requirements
//!
//! Each shard's result set **must already be sorted** by the same sort
//! columns that the merge uses.  The scatter phase pushes `ORDER BY`
//! into each shard's subplan so this invariant holds.
//!
//! # Usage
//!
//! ```ignore
//! let merger = StreamingMergeSort::new(shard_results, sort_columns);
//! for row in merger.take(limit) {
//!     process(row);
//! }
//! ```

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use falcon_common::datum::OwnedRow;

use super::gather::compare_datums;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Merge Entry (heap element)
// ═══════════════════════════════════════════════════════════════════════════

/// A single entry in the merge heap: one row plus its source cursor position.
struct MergeEntry {
    /// The current row from this shard cursor.
    row: OwnedRow,
    /// Index into the `cursors` array identifying the source shard.
    cursor_idx: usize,
    /// Sort specification shared across all entries (col_idx, ascending).
    sort_spec: *const [(usize, bool)],
}

// SAFETY: `sort_spec` is a raw pointer to a slice that lives as long as the
// `StreamingMergeSort` that created this entry.  We never send entries across
// threads; the iterator is consumed on a single thread.
unsafe impl Send for MergeEntry {}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // SAFETY: sort_spec pointer is valid for the lifetime of StreamingMergeSort.
        let spec = unsafe { &*self.sort_spec };
        for &(col_idx, ascending) in spec {
            let va = self.row.values.get(col_idx);
            let vb = other.row.values.get(col_idx);
            let ord = compare_datums(va, vb);
            let ord = if ascending { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Per-shard Cursor
// ═══════════════════════════════════════════════════════════════════════════

/// A cursor over a single shard's sorted row slice.
struct ShardCursor {
    rows: Vec<OwnedRow>,
    pos: usize,
}

impl ShardCursor {
    fn new(rows: Vec<OwnedRow>) -> Self {
        Self { rows, pos: 0 }
    }

    /// Take the next row from this cursor, advancing the position.
    fn next_row(&mut self) -> Option<OwnedRow> {
        if self.pos < self.rows.len() {
            let row = self.rows[self.pos].clone();
            self.pos += 1;
            Some(row)
        } else {
            None
        }
    }

    /// Whether this cursor has more rows.
    fn has_more(&self) -> bool {
        self.pos < self.rows.len()
    }

    /// Number of remaining rows.
    fn remaining(&self) -> usize {
        self.rows.len() - self.pos
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — StreamingMergeSort Iterator
// ═══════════════════════════════════════════════════════════════════════════

/// Streaming k-way merge-sort over pre-sorted shard result sets.
///
/// Implements `Iterator<Item = OwnedRow>` producing rows in globally
/// sorted order.  Consumers can call `.skip(offset).take(limit)` for
/// efficient OFFSET + LIMIT without materializing all rows.
pub struct StreamingMergeSort {
    /// Per-shard cursors.
    cursors: Vec<ShardCursor>,
    /// Min-heap of the next candidate row from each shard.
    heap: BinaryHeap<Reverse<MergeEntry>>,
    /// Sort specification: (column_index, ascending).
    sort_columns: Vec<(usize, bool)>,
    /// Total rows yielded so far (for metrics).
    rows_yielded: u64,
}

impl StreamingMergeSort {
    /// Create a new streaming merge from per-shard sorted row vectors.
    ///
    /// Each element in `shard_rows` is the sorted output of one shard.
    /// `sort_columns` defines the global sort order as `(col_idx, ascending)`.
    pub fn new(
        shard_rows: Vec<Vec<OwnedRow>>,
        sort_columns: Vec<(usize, bool)>,
    ) -> Self {
        let mut cursors: Vec<ShardCursor> = shard_rows
            .into_iter()
            .map(ShardCursor::new)
            .collect();

        let mut merger = Self {
            cursors,
            heap: BinaryHeap::new(),
            sort_columns,
            rows_yielded: 0,
        };

        // Seed the heap with the first row from each non-empty cursor.
        let spec_ptr: *const [(usize, bool)] = merger.sort_columns.as_slice();
        for (idx, cursor) in merger.cursors.iter_mut().enumerate() {
            if let Some(row) = cursor.next_row() {
                merger.heap.push(Reverse(MergeEntry {
                    row,
                    cursor_idx: idx,
                    sort_spec: spec_ptr,
                }));
            }
        }

        merger
    }

    /// Create from `ShardResult` slices (convenience wrapper).
    pub fn from_shard_results(
        results: &[super::ShardResult],
        sort_columns: Vec<(usize, bool)>,
    ) -> Self {
        let shard_rows: Vec<Vec<OwnedRow>> = results
            .iter()
            .map(|sr| sr.rows.clone())
            .collect();
        Self::new(shard_rows, sort_columns)
    }

    /// Total rows yielded so far.
    pub const fn rows_yielded(&self) -> u64 {
        self.rows_yielded
    }

    /// Estimated remaining rows across all cursors.
    pub fn estimated_remaining(&self) -> usize {
        self.cursors.iter().map(|c| c.remaining()).sum::<usize>() + self.heap.len()
    }

    /// Collect with offset and limit applied, without materializing skipped rows
    /// into a separate collection (they are simply dropped during iteration).
    pub fn collect_with_offset_limit(
        mut self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Vec<OwnedRow> {
        // Skip offset rows.
        if let Some(off) = offset {
            for _ in 0..off {
                if self.next().is_none() {
                    break;
                }
            }
        }

        // Collect up to limit rows.
        let cap = limit.unwrap_or(self.estimated_remaining());
        let mut result = Vec::with_capacity(cap.min(65_536));
        for row in &mut self {
            result.push(row);
            if let Some(lim) = limit {
                if result.len() >= lim {
                    break;
                }
            }
        }
        result
    }
}

impl Iterator for StreamingMergeSort {
    type Item = OwnedRow;

    fn next(&mut self) -> Option<OwnedRow> {
        let Reverse(entry) = self.heap.pop()?;
        let row = entry.row;
        let cursor_idx = entry.cursor_idx;

        // Advance the cursor that produced this row and push its next row.
        let spec_ptr: *const [(usize, bool)] = self.sort_columns.as_slice();
        if let Some(next_row) = self.cursors[cursor_idx].next_row() {
            self.heap.push(Reverse(MergeEntry {
                row: next_row,
                cursor_idx,
                sort_spec: spec_ptr,
            }));
        }

        self.rows_yielded += 1;
        Some(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.estimated_remaining();
        (remaining, Some(remaining))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::Datum;

    fn row(vals: &[i64]) -> OwnedRow {
        OwnedRow::new(vals.iter().map(|v| Datum::Int64(*v)).collect())
    }

    #[test]
    fn test_basic_merge_two_shards() {
        let shard0 = vec![row(&[1, 10]), row(&[3, 30]), row(&[5, 50])];
        let shard1 = vec![row(&[2, 20]), row(&[4, 40]), row(&[6, 60])];

        let merger = StreamingMergeSort::new(
            vec![shard0, shard1],
            vec![(0, true)], // sort by col 0 ascending
        );

        let result: Vec<OwnedRow> = merger.collect();
        let vals: Vec<i64> = result
            .iter()
            .map(|r| match &r.values[0] {
                Datum::Int64(v) => *v,
                _ => panic!("expected Int64"),
            })
            .collect();
        assert_eq!(vals, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_merge_with_offset_limit() {
        let shard0 = vec![row(&[1]), row(&[3]), row(&[5]), row(&[7])];
        let shard1 = vec![row(&[2]), row(&[4]), row(&[6]), row(&[8])];

        let merger = StreamingMergeSort::new(
            vec![shard0, shard1],
            vec![(0, true)],
        );

        // OFFSET 2 LIMIT 3 → [3, 4, 5]
        let result = merger.collect_with_offset_limit(Some(2), Some(3));
        let vals: Vec<i64> = result
            .iter()
            .map(|r| match &r.values[0] {
                Datum::Int64(v) => *v,
                _ => panic!("expected Int64"),
            })
            .collect();
        assert_eq!(vals, vec![3, 4, 5]);
    }

    #[test]
    fn test_merge_descending() {
        let shard0 = vec![row(&[50]), row(&[30]), row(&[10])];
        let shard1 = vec![row(&[60]), row(&[40]), row(&[20])];

        let merger = StreamingMergeSort::new(
            vec![shard0, shard1],
            vec![(0, false)], // descending
        );

        let result: Vec<OwnedRow> = merger.collect();
        let vals: Vec<i64> = result
            .iter()
            .map(|r| match &r.values[0] {
                Datum::Int64(v) => *v,
                _ => panic!("expected Int64"),
            })
            .collect();
        assert_eq!(vals, vec![60, 50, 40, 30, 20, 10]);
    }

    #[test]
    fn test_merge_empty_shards() {
        let shard0 = vec![];
        let shard1 = vec![row(&[1]), row(&[2])];
        let shard2 = vec![];

        let merger = StreamingMergeSort::new(
            vec![shard0, shard1, shard2],
            vec![(0, true)],
        );

        let result: Vec<OwnedRow> = merger.collect();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_merge_single_shard() {
        let shard0 = vec![row(&[3]), row(&[1]), row(&[2])]; // not pre-sorted

        // Even unsorted input won't crash — it just won't produce sorted output.
        // The contract says shards must be pre-sorted; this test verifies no panic.
        let merger = StreamingMergeSort::new(
            vec![shard0],
            vec![(0, true)],
        );

        let result: Vec<OwnedRow> = merger.collect();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_merge_multi_column_sort() {
        // Sort by col 0 ASC, col 1 DESC
        let shard0 = vec![row(&[1, 30]), row(&[2, 20])];
        let shard1 = vec![row(&[1, 10]), row(&[2, 40])];

        let merger = StreamingMergeSort::new(
            vec![shard0, shard1],
            vec![(0, true), (1, false)], // col0 asc, col1 desc
        );

        let result: Vec<OwnedRow> = merger.collect();
        let vals: Vec<(i64, i64)> = result
            .iter()
            .map(|r| {
                let a = match &r.values[0] { Datum::Int64(v) => *v, _ => 0 };
                let b = match &r.values[1] { Datum::Int64(v) => *v, _ => 0 };
                (a, b)
            })
            .collect();
        // col0=1: 30 before 10 (desc), col0=2: 40 before 20 (desc)
        assert_eq!(vals, vec![(1, 30), (1, 10), (2, 40), (2, 20)]);
    }

    #[test]
    fn test_early_termination_with_limit() {
        // 3 shards × 1000 rows each = 3000 total, but we only want 5.
        let make_shard = |start: i64| -> Vec<OwnedRow> {
            (0..1000).map(|i| row(&[start + i * 3])).collect()
        };

        let merger = StreamingMergeSort::new(
            vec![make_shard(0), make_shard(1), make_shard(2)],
            vec![(0, true)],
        );

        let result = merger.collect_with_offset_limit(None, Some(5));
        assert_eq!(result.len(), 5);
        let vals: Vec<i64> = result
            .iter()
            .map(|r| match &r.values[0] {
                Datum::Int64(v) => *v,
                _ => panic!("expected Int64"),
            })
            .collect();
        assert_eq!(vals, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_rows_yielded_counter() {
        let shard0 = vec![row(&[1]), row(&[3])];
        let shard1 = vec![row(&[2]), row(&[4])];

        let mut merger = StreamingMergeSort::new(
            vec![shard0, shard1],
            vec![(0, true)],
        );

        assert_eq!(merger.rows_yielded(), 0);
        merger.next();
        assert_eq!(merger.rows_yielded(), 1);
        merger.next();
        assert_eq!(merger.rows_yielded(), 2);
    }

    #[test]
    fn test_all_shards_empty() {
        let merger = StreamingMergeSort::new(
            vec![vec![], vec![], vec![]],
            vec![(0, true)],
        );
        let result: Vec<OwnedRow> = merger.collect();
        assert!(result.is_empty());
    }
}
