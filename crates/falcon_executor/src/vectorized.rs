//! Vectorized execution engine — operates on column batches instead of row-at-a-time.
//!
//! Key types:
//!   - `ColumnVector`: a typed column of values (like a mini Arrow array)
//!   - `RecordBatch`: a collection of column vectors with a selection vector
//!   - Vectorized filter, project, and aggregate operations
//!
//! The vectorized path is used for full-table scans and aggregations where
//! batch processing amortises per-row overhead and enables SIMD-friendly loops.

use std::borrow::Cow;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::*;

// ---------------------------------------------------------------------------
// NullBitmap — packed bitset (8× more memory-efficient than Vec<bool>)
// ---------------------------------------------------------------------------

/// A packed null bitmap using `Vec<u64>` as backing storage.
///
/// Each bit represents one row: `1` = null, `0` = not-null.
/// This is 8× more compact than `Vec<bool>` (1 bit vs 1 byte per row)
/// and is cache-friendlier for SIMD-style vectorized scans.
#[derive(Debug, Clone)]
pub struct NullBitmap {
    words: Vec<u64>,
    len: usize,
}

impl NullBitmap {
    /// Create an empty bitmap.
    pub fn new() -> Self {
        Self {
            words: Vec::new(),
            len: 0,
        }
    }

    /// Create a bitmap with pre-allocated capacity for `cap` bits.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            words: Vec::with_capacity(cap.div_ceil(64)),
            len: 0,
        }
    }

    /// Create a bitmap of `len` bits, all cleared (not-null).
    pub fn all_false(len: usize) -> Self {
        Self {
            words: vec![0u64; len.div_ceil(64)],
            len,
        }
    }

    /// Number of bits.
    #[inline]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether the bitmap is empty.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Append a bit.
    #[inline]
    pub fn push(&mut self, is_null: bool) {
        let word_idx = self.len / 64;
        let bit_idx = self.len % 64;
        if word_idx >= self.words.len() {
            self.words.push(0);
        }
        if is_null {
            self.words[word_idx] |= 1u64 << bit_idx;
        }
        self.len += 1;
    }

    /// Get the bit at `idx`. Returns `true` if null.
    #[inline]
    pub fn get(&self, idx: usize) -> bool {
        debug_assert!(
            idx < self.len,
            "NullBitmap index {idx} out of bounds (len={})",
            self.len
        );
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        (self.words[word_idx] >> bit_idx) & 1 == 1
    }

    /// Set the bit at `idx`.
    #[inline]
    pub fn set(&mut self, idx: usize, is_null: bool) {
        debug_assert!(idx < self.len);
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        if is_null {
            self.words[word_idx] |= 1u64 << bit_idx;
        } else {
            self.words[word_idx] &= !(1u64 << bit_idx);
        }
    }
}

impl Default for NullBitmap {
    fn default() -> Self {
        Self::new()
    }
}

/// Allow `nulls[idx]` syntax — returns `true` if the bit is set (null).
impl std::ops::Index<usize> for NullBitmap {
    type Output = bool;

    #[inline]
    fn index(&self, idx: usize) -> &bool {
        // We cannot return a reference to a computed value, so we use a
        // static pair.  This is a well-known pattern for bitset Index impls.
        if self.get(idx) {
            &true
        } else {
            &false
        }
    }
}

// ---------------------------------------------------------------------------
// ColumnVector
// ---------------------------------------------------------------------------

/// A typed column of homogeneous values.  Unlike `Vec<Datum>` this layout
/// is more cache-friendly because the type tag is stored once, not per-value.
#[derive(Debug, Clone)]
pub enum ColumnVector {
    /// Boolean column with packed null bitmap.
    Booleans {
        values: Vec<bool>,
        nulls: NullBitmap,
    },
    /// 32-bit integer column.
    Int32s { values: Vec<i32>, nulls: NullBitmap },
    /// 64-bit integer column.
    Int64s { values: Vec<i64>, nulls: NullBitmap },
    /// 64-bit float column.
    Float64s { values: Vec<f64>, nulls: NullBitmap },
    /// Text column (heap-allocated strings).
    Texts {
        values: Vec<String>,
        nulls: NullBitmap,
    },
    /// Timestamp column (microseconds since epoch).
    Timestamps { values: Vec<i64>, nulls: NullBitmap },
    /// Date column (days since epoch).
    Dates { values: Vec<i32>, nulls: NullBitmap },
    /// Bytea column.
    Byteas {
        values: Vec<Vec<u8>>,
        nulls: NullBitmap,
    },
    /// Fallback: heterogeneous Datum column (for mixed / unsupported types).
    Mixed(Vec<Datum>),
}

impl ColumnVector {
    /// Whether the column is empty.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of logical rows.
    pub const fn len(&self) -> usize {
        match self {
            Self::Booleans { values, .. } => values.len(),
            Self::Int32s { values, .. } => values.len(),
            Self::Int64s { values, .. } => values.len(),
            Self::Float64s { values, .. } => values.len(),
            Self::Texts { values, .. } => values.len(),
            Self::Timestamps { values, .. } => values.len(),
            Self::Dates { values, .. } => values.len(),
            Self::Byteas { values, .. } => values.len(),
            Self::Mixed(v) => v.len(),
        }
    }

    /// Get value at index as a Datum (clones Text/Mixed).
    pub fn get_datum(&self, idx: usize) -> Datum {
        match self {
            Self::Booleans { values, nulls } => {
                if nulls[idx] {
                    Datum::Null
                } else {
                    Datum::Boolean(values[idx])
                }
            }
            Self::Int32s { values, nulls } => {
                if nulls[idx] {
                    Datum::Null
                } else {
                    Datum::Int32(values[idx])
                }
            }
            Self::Int64s { values, nulls } => {
                if nulls[idx] {
                    Datum::Null
                } else {
                    Datum::Int64(values[idx])
                }
            }
            Self::Float64s { values, nulls } => {
                if nulls[idx] {
                    Datum::Null
                } else {
                    Datum::Float64(values[idx])
                }
            }
            Self::Texts { values, nulls } => {
                if nulls[idx] {
                    Datum::Null
                } else {
                    Datum::Text(values[idx].clone())
                }
            }
            Self::Timestamps { values, nulls } => {
                if nulls[idx] {
                    Datum::Null
                } else {
                    Datum::Timestamp(values[idx])
                }
            }
            Self::Dates { values, nulls } => {
                if nulls[idx] {
                    Datum::Null
                } else {
                    Datum::Date(values[idx])
                }
            }
            Self::Byteas { values, nulls } => {
                if nulls[idx] {
                    Datum::Null
                } else {
                    Datum::Bytea(values[idx].clone())
                }
            }
            Self::Mixed(v) => v[idx].clone(),
        }
    }

    /// Get a zero-copy reference to the value at `idx`.
    /// For Mixed columns this returns `Cow::Borrowed`, avoiding a Datum clone.
    /// For typed columns (primitives + Text), this returns `Cow::Owned` which
    /// is still cheaper than `get_datum` for Text because callers that only
    /// need `&Datum` can use `&*cow` without moving.
    pub fn get_datum_ref(&self, idx: usize) -> Cow<'_, Datum> {
        match self {
            Self::Mixed(v) => Cow::Borrowed(&v[idx]),
            _ => Cow::Owned(self.get_datum(idx)),
        }
    }

    /// Build a ColumnVector from a slice of Datum values.
    pub fn from_datums(datums: &[Datum]) -> Self {
        if datums.is_empty() {
            return Self::Mixed(Vec::new());
        }

        // Infer type from first non-null value
        let first_type = datums.iter().find(|d| !d.is_null());
        match first_type {
            Some(Datum::Int32(_)) => {
                let mut values = Vec::with_capacity(datums.len());
                let mut nulls = NullBitmap::with_capacity(datums.len());
                for d in datums {
                    match d {
                        Datum::Int32(v) => {
                            values.push(*v);
                            nulls.push(false);
                        }
                        Datum::Null => {
                            values.push(0);
                            nulls.push(true);
                        }
                        _ => return Self::Mixed(datums.to_vec()),
                    }
                }
                Self::Int32s { values, nulls }
            }
            Some(Datum::Int64(_)) => {
                let mut values = Vec::with_capacity(datums.len());
                let mut nulls = NullBitmap::with_capacity(datums.len());
                for d in datums {
                    match d {
                        Datum::Int64(v) => {
                            values.push(*v);
                            nulls.push(false);
                        }
                        Datum::Int32(v) => {
                            values.push(i64::from(*v));
                            nulls.push(false);
                        }
                        Datum::Null => {
                            values.push(0);
                            nulls.push(true);
                        }
                        _ => return Self::Mixed(datums.to_vec()),
                    }
                }
                Self::Int64s { values, nulls }
            }
            Some(Datum::Float64(_)) => {
                let mut values = Vec::with_capacity(datums.len());
                let mut nulls = NullBitmap::with_capacity(datums.len());
                for d in datums {
                    match d {
                        Datum::Float64(v) => {
                            values.push(*v);
                            nulls.push(false);
                        }
                        Datum::Int32(v) => {
                            values.push(f64::from(*v));
                            nulls.push(false);
                        }
                        Datum::Int64(v) => {
                            values.push(*v as f64);
                            nulls.push(false);
                        }
                        Datum::Null => {
                            values.push(0.0);
                            nulls.push(true);
                        }
                        _ => return Self::Mixed(datums.to_vec()),
                    }
                }
                Self::Float64s { values, nulls }
            }
            Some(Datum::Boolean(_)) => {
                let mut values = Vec::with_capacity(datums.len());
                let mut nulls = NullBitmap::with_capacity(datums.len());
                for d in datums {
                    match d {
                        Datum::Boolean(v) => {
                            values.push(*v);
                            nulls.push(false);
                        }
                        Datum::Null => {
                            values.push(false);
                            nulls.push(true);
                        }
                        _ => return Self::Mixed(datums.to_vec()),
                    }
                }
                Self::Booleans { values, nulls }
            }
            Some(Datum::Text(_)) => {
                let mut values = Vec::with_capacity(datums.len());
                let mut nulls = NullBitmap::with_capacity(datums.len());
                for d in datums {
                    match d {
                        Datum::Text(v) => {
                            values.push(v.clone());
                            nulls.push(false);
                        }
                        Datum::Null => {
                            values.push(String::new());
                            nulls.push(true);
                        }
                        _ => return Self::Mixed(datums.to_vec()),
                    }
                }
                Self::Texts { values, nulls }
            }
            Some(Datum::Timestamp(_)) => {
                let mut values = Vec::with_capacity(datums.len());
                let mut nulls = NullBitmap::with_capacity(datums.len());
                for d in datums {
                    match d {
                        Datum::Timestamp(v) => {
                            values.push(*v);
                            nulls.push(false);
                        }
                        Datum::Null => {
                            values.push(0);
                            nulls.push(true);
                        }
                        _ => return Self::Mixed(datums.to_vec()),
                    }
                }
                Self::Timestamps { values, nulls }
            }
            Some(Datum::Date(_)) => {
                let mut values = Vec::with_capacity(datums.len());
                let mut nulls = NullBitmap::with_capacity(datums.len());
                for d in datums {
                    match d {
                        Datum::Date(v) => {
                            values.push(*v);
                            nulls.push(false);
                        }
                        Datum::Null => {
                            values.push(0);
                            nulls.push(true);
                        }
                        _ => return Self::Mixed(datums.to_vec()),
                    }
                }
                Self::Dates { values, nulls }
            }
            Some(Datum::Bytea(_)) => {
                let mut values = Vec::with_capacity(datums.len());
                let mut nulls = NullBitmap::with_capacity(datums.len());
                for d in datums {
                    match d {
                        Datum::Bytea(v) => {
                            values.push(v.clone());
                            nulls.push(false);
                        }
                        Datum::Null => {
                            values.push(Vec::new());
                            nulls.push(true);
                        }
                        _ => return Self::Mixed(datums.to_vec()),
                    }
                }
                Self::Byteas { values, nulls }
            }
            _ => Self::Mixed(datums.to_vec()),
        }
    }
}

impl ColumnVector {
    /// Build a new ColumnVector by gathering elements at the given indices.
    /// Stays in the same typed representation — avoids intermediate Vec<Datum>
    /// and the from_datums re-dispatch overhead.
    pub fn gather(&self, indices: &[usize]) -> Self {
        let n = indices.len();
        match self {
            Self::Int32s { values, nulls } => {
                let mut v = Vec::with_capacity(n);
                let mut nl = NullBitmap::with_capacity(n);
                for &i in indices {
                    v.push(values[i]);
                    nl.push(nulls[i]);
                }
                Self::Int32s {
                    values: v,
                    nulls: nl,
                }
            }
            Self::Int64s { values, nulls } => {
                let mut v = Vec::with_capacity(n);
                let mut nl = NullBitmap::with_capacity(n);
                for &i in indices {
                    v.push(values[i]);
                    nl.push(nulls[i]);
                }
                Self::Int64s {
                    values: v,
                    nulls: nl,
                }
            }
            Self::Float64s { values, nulls } => {
                let mut v = Vec::with_capacity(n);
                let mut nl = NullBitmap::with_capacity(n);
                for &i in indices {
                    v.push(values[i]);
                    nl.push(nulls[i]);
                }
                Self::Float64s {
                    values: v,
                    nulls: nl,
                }
            }
            Self::Booleans { values, nulls } => {
                let mut v = Vec::with_capacity(n);
                let mut nl = NullBitmap::with_capacity(n);
                for &i in indices {
                    v.push(values[i]);
                    nl.push(nulls[i]);
                }
                Self::Booleans {
                    values: v,
                    nulls: nl,
                }
            }
            Self::Texts { values, nulls } => {
                let mut v = Vec::with_capacity(n);
                let mut nl = NullBitmap::with_capacity(n);
                for &i in indices {
                    v.push(values[i].clone());
                    nl.push(nulls[i]);
                }
                Self::Texts {
                    values: v,
                    nulls: nl,
                }
            }
            Self::Timestamps { values, nulls } => {
                let mut v = Vec::with_capacity(n);
                let mut nl = NullBitmap::with_capacity(n);
                for &i in indices {
                    v.push(values[i]);
                    nl.push(nulls[i]);
                }
                Self::Timestamps {
                    values: v,
                    nulls: nl,
                }
            }
            Self::Dates { values, nulls } => {
                let mut v = Vec::with_capacity(n);
                let mut nl = NullBitmap::with_capacity(n);
                for &i in indices {
                    v.push(values[i]);
                    nl.push(nulls[i]);
                }
                Self::Dates {
                    values: v,
                    nulls: nl,
                }
            }
            Self::Byteas { values, nulls } => {
                let mut v = Vec::with_capacity(n);
                let mut nl = NullBitmap::with_capacity(n);
                for &i in indices {
                    v.push(values[i].clone());
                    nl.push(nulls[i]);
                }
                Self::Byteas {
                    values: v,
                    nulls: nl,
                }
            }
            Self::Mixed(data) => {
                let mut v = Vec::with_capacity(n);
                for &i in indices {
                    v.push(data[i].clone());
                }
                Self::Mixed(v)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RecordBatch
// ---------------------------------------------------------------------------

/// A batch of rows stored in columnar layout, with an optional selection vector
/// to represent filtered rows without copying.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    /// Column data.
    pub columns: Vec<ColumnVector>,
    /// Number of logical rows (before selection).
    pub num_rows: usize,
    /// Selection vector: indices of "active" rows. If None, all rows are active.
    pub selection: Option<Vec<usize>>,
}

impl RecordBatch {
    /// Build a RecordBatch from a slice of OwnedRows.
    pub fn from_rows(rows: &[OwnedRow], num_cols: usize) -> Self {
        let num_rows = rows.len();
        let mut col_data: Vec<Vec<Datum>> = (0..num_cols)
            .map(|_| Vec::with_capacity(num_rows))
            .collect();

        for row in rows {
            for (col_idx, col) in col_data.iter_mut().enumerate() {
                col.push(row.values.get(col_idx).cloned().unwrap_or(Datum::Null));
            }
        }

        let columns: Vec<ColumnVector> = col_data
            .iter()
            .map(|col| ColumnVector::from_datums(col))
            .collect();

        Self {
            columns,
            num_rows,
            selection: None,
        }
    }

    /// Build a RecordBatch from scan result pairs `(pk_bytes, row)` without
    /// cloning the rows into an intermediate `Vec<OwnedRow>` first.
    /// Eliminates one full-table clone on the vectorized filter path.
    pub fn from_row_pairs(pairs: &[(Vec<u8>, OwnedRow)], num_cols: usize) -> Self {
        let num_rows = pairs.len();
        let mut col_data: Vec<Vec<Datum>> = (0..num_cols)
            .map(|_| Vec::with_capacity(num_rows))
            .collect();

        for (_, row) in pairs {
            for (col_idx, col) in col_data.iter_mut().enumerate() {
                col.push(row.values.get(col_idx).cloned().unwrap_or(Datum::Null));
            }
        }

        let columns: Vec<ColumnVector> = col_data
            .iter()
            .map(|col| ColumnVector::from_datums(col))
            .collect();

        Self {
            columns,
            num_rows,
            selection: None,
        }
    }

    /// Build a RecordBatch directly from pre-computed column vectors (columnar scan path).
    /// Each element of `columns` is a Vec<Datum> for one column; all must have equal length.
    pub fn from_columns(columns: Vec<Vec<Datum>>) -> Self {
        let num_rows = columns.first().map_or(0, std::vec::Vec::len);
        let col_vecs = columns
            .iter()
            .map(|c| ColumnVector::from_datums(c))
            .collect();
        Self {
            columns: col_vecs,
            num_rows,
            selection: None,
        }
    }

    /// Materialise active rows back to OwnedRow format.
    pub fn to_rows(&self) -> Vec<OwnedRow> {
        let num_cols = self.columns.len();
        let count = self.active_count();
        let mut rows = Vec::with_capacity(count);
        // Inline iteration avoids the Vec<usize> allocation in active_indices()
        // when no selection vector exists (all rows active).
        match &self.selection {
            Some(sel) => {
                for &idx in sel {
                    let mut values = Vec::with_capacity(num_cols);
                    for col in &self.columns {
                        values.push(col.get_datum(idx));
                    }
                    rows.push(OwnedRow::new(values));
                }
            }
            None => {
                for idx in 0..self.num_rows {
                    let mut values = Vec::with_capacity(num_cols);
                    for col in &self.columns {
                        values.push(col.get_datum(idx));
                    }
                    rows.push(OwnedRow::new(values));
                }
            }
        }
        rows
    }

    /// Return active row indices.
    /// Returns `Cow::Borrowed` when a selection vector exists (zero-copy),
    /// or `Cow::Owned` with a range vector when all rows are active.
    pub fn active_indices(&self) -> Cow<'_, [usize]> {
        match &self.selection {
            Some(sel) => Cow::Borrowed(sel.as_slice()),
            None => Cow::Owned((0..self.num_rows).collect()),
        }
    }

    /// Number of active (non-filtered) rows.
    pub const fn active_count(&self) -> usize {
        match &self.selection {
            Some(sel) => sel.len(),
            None => self.num_rows,
        }
    }
}

// ---------------------------------------------------------------------------
// Vectorized filter
// ---------------------------------------------------------------------------

/// Evaluate a simple filter expression against a RecordBatch and update its
/// selection vector.  Only handles common patterns vectorially; falls back
/// to row-at-a-time for complex expressions.
pub fn vectorized_filter(batch: &mut RecordBatch, filter: &BoundExpr) {
    let indices = batch.active_indices();
    let mut new_sel = Vec::with_capacity(indices.len());

    match filter {
        // col = literal
        BoundExpr::BinaryOp { left, op, right } => {
            if let (BoundExpr::ColumnRef(col_idx), BoundExpr::Literal(lit))
            | (BoundExpr::Literal(lit), BoundExpr::ColumnRef(col_idx)) =
                (left.as_ref(), right.as_ref())
            {
                if *col_idx < batch.columns.len() {
                    vectorized_compare(&batch.columns[*col_idx], &indices, *op, lit, &mut new_sel);
                    batch.selection = Some(new_sel);
                    return;
                }
            }
            // AND: apply left, then right
            if *op == BinOp::And {
                vectorized_filter(batch, left);
                vectorized_filter(batch, right);
                return;
            }
            // Fallback: row-at-a-time (reuse buffer across rows)
            let mut row_buf: smallvec::SmallVec<[Datum; 8]> = smallvec::SmallVec::with_capacity(batch.columns.len());
            for &idx in &*indices {
                if eval_filter_on_batch_row(batch, idx, filter, &mut row_buf) {
                    new_sel.push(idx);
                }
            }
            batch.selection = Some(new_sel);
        }
        BoundExpr::Not(inner) => {
            // Apply inner, then invert using a bitvec instead of 2× HashSet.
            let before_indices = indices.into_owned();
            vectorized_filter(batch, inner);
            let after = batch.active_indices();
            // Build bitvec marking rows that passed the inner filter
            let mut passed = vec![false; batch.num_rows];
            for &idx in &*after {
                passed[idx] = true;
            }
            // Collect rows from "before" that did NOT pass the inner filter
            let inverted: Vec<usize> = before_indices
                .into_iter()
                .filter(|&idx| !passed[idx])
                .collect();
            batch.selection = Some(inverted);
        }
        _ => {
            // Fallback: row-at-a-time (reuse buffer across rows)
            let mut row_buf: smallvec::SmallVec<[Datum; 8]> = smallvec::SmallVec::with_capacity(batch.columns.len());
            for &idx in &*indices {
                if eval_filter_on_batch_row(batch, idx, filter, &mut row_buf) {
                    new_sel.push(idx);
                }
            }
            batch.selection = Some(new_sel);
        }
    }
}

/// Vectorized comparison: col <op> literal.
fn vectorized_compare(
    col: &ColumnVector,
    indices: &[usize],
    op: BinOp,
    literal: &Datum,
    out: &mut Vec<usize>,
) {
    match (col, literal) {
        (ColumnVector::Int64s { values, nulls }, Datum::Int64(lit)) => {
            for &idx in indices {
                if nulls[idx] {
                    continue;
                }
                let v = values[idx];
                if int_cmp(v, *lit, op) {
                    out.push(idx);
                }
            }
        }
        (ColumnVector::Int64s { values, nulls }, Datum::Int32(lit)) => {
            let lit64 = i64::from(*lit);
            for &idx in indices {
                if nulls[idx] {
                    continue;
                }
                if int_cmp(values[idx], lit64, op) {
                    out.push(idx);
                }
            }
        }
        (ColumnVector::Int32s { values, nulls }, Datum::Int32(lit)) => {
            for &idx in indices {
                if nulls[idx] {
                    continue;
                }
                if int_cmp(i64::from(values[idx]), i64::from(*lit), op) {
                    out.push(idx);
                }
            }
        }
        (ColumnVector::Float64s { values, nulls }, Datum::Float64(lit)) => {
            for &idx in indices {
                if nulls[idx] {
                    continue;
                }
                if float_cmp(values[idx], *lit, op) {
                    out.push(idx);
                }
            }
        }
        (ColumnVector::Texts { values, nulls }, Datum::Text(lit)) => {
            for &idx in indices {
                if nulls[idx] {
                    continue;
                }
                if str_cmp(&values[idx], lit, op) {
                    out.push(idx);
                }
            }
        }
        _ => {
            // Fallback: per-value Datum comparison
            for &idx in indices {
                let d = col.get_datum(idx);
                if d.is_null() {
                    continue;
                }
                if datum_cmp(&d, literal, op) {
                    out.push(idx);
                }
            }
        }
    }
}

#[inline(always)]
const fn int_cmp(a: i64, b: i64, op: BinOp) -> bool {
    match op {
        BinOp::Eq => a == b,
        BinOp::NotEq => a != b,
        BinOp::Lt => a < b,
        BinOp::LtEq => a <= b,
        BinOp::Gt => a > b,
        BinOp::GtEq => a >= b,
        _ => false,
    }
}

#[inline(always)]
fn float_cmp(a: f64, b: f64, op: BinOp) -> bool {
    match op {
        BinOp::Eq => (a - b).abs() < f64::EPSILON,
        BinOp::NotEq => (a - b).abs() >= f64::EPSILON,
        BinOp::Lt => a < b,
        BinOp::LtEq => a <= b,
        BinOp::Gt => a > b,
        BinOp::GtEq => a >= b,
        _ => false,
    }
}

#[inline(always)]
fn str_cmp(a: &str, b: &str, op: BinOp) -> bool {
    match op {
        BinOp::Eq => a == b,
        BinOp::NotEq => a != b,
        BinOp::Lt => a < b,
        BinOp::LtEq => a <= b,
        BinOp::Gt => a > b,
        BinOp::GtEq => a >= b,
        _ => false,
    }
}

fn datum_cmp(a: &Datum, b: &Datum, op: BinOp) -> bool {
    if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
        float_cmp(af, bf, op)
    } else {
        // Use cmp_datum_values to avoid per-comparison format!() allocation
        let ord = cmp_datum_values(a, b);
        match op {
            BinOp::Eq => ord == std::cmp::Ordering::Equal,
            BinOp::NotEq => ord != std::cmp::Ordering::Equal,
            BinOp::Lt => ord == std::cmp::Ordering::Less,
            BinOp::LtEq => ord != std::cmp::Ordering::Greater,
            BinOp::Gt => ord == std::cmp::Ordering::Greater,
            BinOp::GtEq => ord != std::cmp::Ordering::Less,
            _ => false,
        }
    }
}

#[allow(dead_code)]
fn make_row_from_batch(batch: &RecordBatch, idx: usize) -> OwnedRow {
    let mut values = Vec::with_capacity(batch.columns.len());
    for col in &batch.columns {
        values.push(col.get_datum(idx));
    }
    OwnedRow::new(values)
}

/// Evaluate a filter on a single row from a batch, reusing `buf` across calls
/// to avoid per-row Vec allocation. The Vec is borrowed, filled, wrapped in
/// OwnedRow for eval, then reclaimed.
#[inline]
fn eval_filter_on_batch_row(
    batch: &RecordBatch,
    idx: usize,
    filter: &BoundExpr,
    buf: &mut smallvec::SmallVec<[Datum; 8]>,
) -> bool {
    buf.clear();
    for col in &batch.columns {
        buf.push(col.get_datum(idx));
    }
    let row = OwnedRow::new(std::mem::take(buf));
    let result = crate::expr_engine::ExprEngine::eval_filter(filter, &row).unwrap_or(false);
    // Reclaim the SmallVec allocation for reuse
    *buf = row.values;
    result
}

// ---------------------------------------------------------------------------
// Vectorized aggregation
// ---------------------------------------------------------------------------

/// Compute a simple aggregate over a ColumnVector (for active indices).
/// Returns the aggregated Datum.
pub fn vectorized_aggregate(
    col: &ColumnVector,
    indices: &[usize],
    func: &AggFunc,
) -> Result<Datum, ExecutionError> {
    match func {
        AggFunc::Count => Ok(Datum::Int64(indices.len() as i64)),
        AggFunc::Sum => vectorized_sum(col, indices),
        AggFunc::Avg => vectorized_avg(col, indices),
        AggFunc::Min => vectorized_min(col, indices),
        AggFunc::Max => vectorized_max(col, indices),
        AggFunc::StddevPop => vectorized_stddev(col, indices, true),
        AggFunc::StddevSamp => vectorized_stddev(col, indices, false),
        AggFunc::VarPop => vectorized_variance(col, indices, true),
        AggFunc::VarSamp => vectorized_variance(col, indices, false),
        _ => Err(ExecutionError::TypeError(format!(
            "Vectorized aggregate not supported for {func:?}"
        ))),
    }
}

fn vectorized_sum(col: &ColumnVector, indices: &[usize]) -> Result<Datum, ExecutionError> {
    match col {
        ColumnVector::Int64s { values, nulls } => {
            let mut sum: i64 = 0;
            let mut sum_f: f64 = 0.0;
            let mut overflowed = false;
            let mut has_value = false;
            for &idx in indices {
                if !nulls[idx] {
                    if !overflowed {
                        if let Some(s) = sum.checked_add(values[idx]) {
                            sum = s;
                        } else {
                            sum_f += sum as f64 + values[idx] as f64;
                            sum = 0;
                            overflowed = true;
                        }
                    } else {
                        sum_f += values[idx] as f64;
                    }
                    has_value = true;
                }
            }
            Ok(if !has_value {
                Datum::Null
            } else if overflowed {
                Datum::Float64(sum_f)
            } else {
                Datum::Int64(sum)
            })
        }
        ColumnVector::Int32s { values, nulls } => {
            let mut sum: i64 = 0;
            let mut sum_f: f64 = 0.0;
            let mut overflowed = false;
            let mut has_value = false;
            for &idx in indices {
                if !nulls[idx] {
                    if !overflowed {
                        if let Some(s) = sum.checked_add(i64::from(values[idx])) {
                            sum = s;
                        } else {
                            sum_f += sum as f64 + f64::from(values[idx]);
                            sum = 0;
                            overflowed = true;
                        }
                    } else {
                        sum_f += f64::from(values[idx]);
                    }
                    has_value = true;
                }
            }
            Ok(if !has_value {
                Datum::Null
            } else if overflowed {
                Datum::Float64(sum_f)
            } else {
                Datum::Int64(sum)
            })
        }
        ColumnVector::Float64s { values, nulls } => {
            let mut sum: f64 = 0.0;
            let mut has_value = false;
            for &idx in indices {
                if !nulls[idx] {
                    sum += values[idx];
                    has_value = true;
                }
            }
            Ok(if has_value {
                Datum::Float64(sum)
            } else {
                Datum::Null
            })
        }
        _ => {
            // Fallback
            let mut sum = 0.0f64;
            let mut count = 0;
            for &idx in indices {
                if let Some(f) = col.get_datum(idx).as_f64() {
                    sum += f;
                    count += 1;
                }
            }
            Ok(if count > 0 {
                Datum::Float64(sum)
            } else {
                Datum::Null
            })
        }
    }
}

fn vectorized_avg(col: &ColumnVector, indices: &[usize]) -> Result<Datum, ExecutionError> {
    match col {
        ColumnVector::Int64s { values, nulls } => {
            let mut sum: f64 = 0.0;
            let mut count: usize = 0;
            for &idx in indices {
                if !nulls[idx] {
                    sum += values[idx] as f64;
                    count += 1;
                }
            }
            Ok(if count > 0 {
                Datum::Float64(sum / count as f64)
            } else {
                Datum::Null
            })
        }
        ColumnVector::Float64s { values, nulls } => {
            let mut sum: f64 = 0.0;
            let mut count: usize = 0;
            for &idx in indices {
                if !nulls[idx] {
                    sum += values[idx];
                    count += 1;
                }
            }
            Ok(if count > 0 {
                Datum::Float64(sum / count as f64)
            } else {
                Datum::Null
            })
        }
        _ => {
            let mut sum = 0.0f64;
            let mut count = 0usize;
            for &idx in indices {
                if let Some(f) = col.get_datum(idx).as_f64() {
                    sum += f;
                    count += 1;
                }
            }
            Ok(if count > 0 {
                Datum::Float64(sum / count as f64)
            } else {
                Datum::Null
            })
        }
    }
}

fn vectorized_min(col: &ColumnVector, indices: &[usize]) -> Result<Datum, ExecutionError> {
    match col {
        ColumnVector::Int64s { values, nulls } => {
            let mut min: Option<i64> = None;
            for &idx in indices {
                if !nulls[idx] {
                    min = Some(min.map_or(values[idx], |m: i64| m.min(values[idx])));
                }
            }
            Ok(min.map_or(Datum::Null, Datum::Int64))
        }
        ColumnVector::Float64s { values, nulls } => {
            let mut min: Option<f64> = None;
            for &idx in indices {
                if !nulls[idx] {
                    min = Some(min.map_or(values[idx], |m: f64| m.min(values[idx])));
                }
            }
            Ok(min.map_or(Datum::Null, Datum::Float64))
        }
        _ => {
            let mut result = Datum::Null;
            for &idx in indices {
                let d = col.get_datum(idx);
                if d.is_null() {
                    continue;
                }
                if result.is_null() || d < result {
                    result = d;
                }
            }
            Ok(result)
        }
    }
}

fn vectorized_max(col: &ColumnVector, indices: &[usize]) -> Result<Datum, ExecutionError> {
    match col {
        ColumnVector::Int64s { values, nulls } => {
            let mut max: Option<i64> = None;
            for &idx in indices {
                if !nulls[idx] {
                    max = Some(max.map_or(values[idx], |m: i64| m.max(values[idx])));
                }
            }
            Ok(max.map_or(Datum::Null, Datum::Int64))
        }
        ColumnVector::Float64s { values, nulls } => {
            let mut max: Option<f64> = None;
            for &idx in indices {
                if !nulls[idx] {
                    max = Some(max.map_or(values[idx], |m: f64| m.max(values[idx])));
                }
            }
            Ok(max.map_or(Datum::Null, Datum::Float64))
        }
        _ => {
            let mut result = Datum::Null;
            for &idx in indices {
                let d = col.get_datum(idx);
                if d.is_null() {
                    continue;
                }
                if result.is_null() || d > result {
                    result = d;
                }
            }
            Ok(result)
        }
    }
}

fn vectorized_stddev(
    col: &ColumnVector,
    indices: &[usize],
    population: bool,
) -> Result<Datum, ExecutionError> {
    let var = vectorized_variance(col, indices, population)?;
    match var {
        Datum::Float64(v) => Ok(Datum::Float64(v.sqrt())),
        _ => Ok(Datum::Null),
    }
}

fn vectorized_variance(
    col: &ColumnVector,
    indices: &[usize],
    population: bool,
) -> Result<Datum, ExecutionError> {
    // Two-pass: compute mean, then sum of squared deviations
    let (sum, count) = match col {
        ColumnVector::Int64s { values, nulls } => {
            let mut s = 0.0f64;
            let mut c = 0usize;
            for &idx in indices {
                if !nulls[idx] {
                    s += values[idx] as f64;
                    c += 1;
                }
            }
            (s, c)
        }
        ColumnVector::Float64s { values, nulls } => {
            let mut s = 0.0f64;
            let mut c = 0usize;
            for &idx in indices {
                if !nulls[idx] {
                    s += values[idx];
                    c += 1;
                }
            }
            (s, c)
        }
        _ => {
            let mut s = 0.0f64;
            let mut c = 0usize;
            for &idx in indices {
                if let Some(f) = col.get_datum(idx).as_f64() {
                    s += f;
                    c += 1;
                }
            }
            (s, c)
        }
    };

    if count == 0 {
        return Ok(Datum::Null);
    }
    if !population && count < 2 {
        return Ok(Datum::Null);
    }

    let mean = sum / count as f64;
    let sum_sq = match col {
        ColumnVector::Int64s { values, nulls } => {
            let mut ss = 0.0f64;
            for &idx in indices {
                if !nulls[idx] {
                    let d = values[idx] as f64 - mean;
                    ss += d * d;
                }
            }
            ss
        }
        ColumnVector::Float64s { values, nulls } => {
            let mut ss = 0.0f64;
            for &idx in indices {
                if !nulls[idx] {
                    let d = values[idx] - mean;
                    ss += d * d;
                }
            }
            ss
        }
        _ => {
            let mut ss = 0.0f64;
            for &idx in indices {
                if let Some(f) = col.get_datum(idx).as_f64() {
                    let d = f - mean;
                    ss += d * d;
                }
            }
            ss
        }
    };

    let divisor = if population {
        count as f64
    } else {
        (count - 1) as f64
    };
    Ok(Datum::Float64(sum_sq / divisor))
}

// ---------------------------------------------------------------------------
// Vectorized projection
// ---------------------------------------------------------------------------

/// Apply projections to a RecordBatch, producing a new batch.
///
/// Handles Column, Expr (constant-only), and Aggregate refs.
/// Complex expression projections fall back to row-at-a-time.
pub fn vectorized_project(
    batch: &RecordBatch,
    projections: &[BoundProjection],
) -> Result<RecordBatch, ExecutionError> {
    let indices = batch.active_indices();
    let num_active = indices.len();
    let mut out_cols: Vec<ColumnVector> = Vec::with_capacity(projections.len());

    for proj in projections {
        match proj {
            BoundProjection::Column(col_idx, _) => {
                if *col_idx < batch.columns.len() {
                    // Extract only active rows from the source column
                    out_cols.push(extract_column(&batch.columns[*col_idx], &indices));
                } else {
                    // Column doesn't exist → NULL column
                    out_cols.push(ColumnVector::Mixed(vec![Datum::Null; num_active]));
                }
            }
            BoundProjection::Expr(expr, _) => {
                // Evaluate expression per active row — reuse buffer across rows
                let mut datums = Vec::with_capacity(num_active);
                let mut row_buf: smallvec::SmallVec<[Datum; 8]> = smallvec::SmallVec::with_capacity(batch.columns.len());
                for &idx in &*indices {
                    row_buf.clear();
                    for col in &batch.columns {
                        row_buf.push(col.get_datum(idx));
                    }
                    let row = OwnedRow::new(std::mem::take(&mut row_buf));
                    let val = crate::expr_engine::ExprEngine::eval_row(expr, &row)?;
                    // Reclaim buffer for reuse
                    row_buf = row.values;
                    datums.push(val);
                }
                out_cols.push(ColumnVector::from_datums(&datums));
            }
            BoundProjection::Aggregate(func, arg, _, _, _) => {
                // For aggregates in projection, compute over all active rows
                if let Some(BoundExpr::ColumnRef(col_idx)) = arg.as_ref() {
                    if *col_idx < batch.columns.len() {
                        let result =
                            vectorized_aggregate(&batch.columns[*col_idx], &indices, func)?;
                        out_cols.push(ColumnVector::Mixed(vec![result; num_active]));
                        continue;
                    }
                }
                // COUNT(*) or fallback
                let result = Datum::Int64(num_active as i64);
                out_cols.push(ColumnVector::Mixed(vec![result; num_active]));
            }
            BoundProjection::Window(_) => {
                // Window functions not vectorizable — placeholder NULLs
                out_cols.push(ColumnVector::Mixed(vec![Datum::Null; num_active]));
            }
        }
    }

    Ok(RecordBatch {
        columns: out_cols,
        num_rows: num_active,
        selection: None, // all rows are active in the output
    })
}

/// Extract active rows from a ColumnVector based on a selection index list.
fn extract_column(col: &ColumnVector, indices: &[usize]) -> ColumnVector {
    match col {
        ColumnVector::Int32s { values, nulls } => {
            let mut v = Vec::with_capacity(indices.len());
            let mut n = NullBitmap::with_capacity(indices.len());
            for &i in indices {
                v.push(values[i]);
                n.push(nulls[i]);
            }
            ColumnVector::Int32s {
                values: v,
                nulls: n,
            }
        }
        ColumnVector::Int64s { values, nulls } => {
            let mut v = Vec::with_capacity(indices.len());
            let mut n = NullBitmap::with_capacity(indices.len());
            for &i in indices {
                v.push(values[i]);
                n.push(nulls[i]);
            }
            ColumnVector::Int64s {
                values: v,
                nulls: n,
            }
        }
        ColumnVector::Float64s { values, nulls } => {
            let mut v = Vec::with_capacity(indices.len());
            let mut n = NullBitmap::with_capacity(indices.len());
            for &i in indices {
                v.push(values[i]);
                n.push(nulls[i]);
            }
            ColumnVector::Float64s {
                values: v,
                nulls: n,
            }
        }
        ColumnVector::Booleans { values, nulls } => {
            let mut v = Vec::with_capacity(indices.len());
            let mut n = NullBitmap::with_capacity(indices.len());
            for &i in indices {
                v.push(values[i]);
                n.push(nulls[i]);
            }
            ColumnVector::Booleans {
                values: v,
                nulls: n,
            }
        }
        ColumnVector::Texts { values, nulls } => {
            let mut v = Vec::with_capacity(indices.len());
            let mut n = NullBitmap::with_capacity(indices.len());
            for &i in indices {
                v.push(values[i].clone());
                n.push(nulls[i]);
            }
            ColumnVector::Texts {
                values: v,
                nulls: n,
            }
        }
        ColumnVector::Timestamps { values, nulls } => {
            let mut v = Vec::with_capacity(indices.len());
            let mut n = NullBitmap::with_capacity(indices.len());
            for &i in indices {
                v.push(values[i]);
                n.push(nulls[i]);
            }
            ColumnVector::Timestamps {
                values: v,
                nulls: n,
            }
        }
        ColumnVector::Dates { values, nulls } => {
            let mut v = Vec::with_capacity(indices.len());
            let mut n = NullBitmap::with_capacity(indices.len());
            for &i in indices {
                v.push(values[i]);
                n.push(nulls[i]);
            }
            ColumnVector::Dates {
                values: v,
                nulls: n,
            }
        }
        ColumnVector::Byteas { values, nulls } => {
            let mut v = Vec::with_capacity(indices.len());
            let mut n = NullBitmap::with_capacity(indices.len());
            for &i in indices {
                v.push(values[i].clone());
                n.push(nulls[i]);
            }
            ColumnVector::Byteas {
                values: v,
                nulls: n,
            }
        }
        ColumnVector::Mixed(vals) => {
            ColumnVector::Mixed(indices.iter().map(|&i| vals[i].clone()).collect())
        }
    }
}

// ---------------------------------------------------------------------------
// Vectorized hash join
// ---------------------------------------------------------------------------

/// Vectorized hash join: build phase creates a hash table from the right batch,
/// probe phase scans the left batch. Supports inner equi-join only.
///
/// `left_key_cols` / `right_key_cols`: column indices in each batch for the
/// equi-join keys (must be same length).
pub fn vectorized_hash_join(
    left: &RecordBatch,
    right: &RecordBatch,
    left_key_cols: &[usize],
    right_key_cols: &[usize],
) -> RecordBatch {
    use std::collections::HashMap;

    let right_indices = right.active_indices();
    let left_indices = left.active_indices();
    let left_ncols = left.columns.len();
    let right_ncols = right.columns.len();

    // Build phase: hash table keyed by right key columns → list of right row indices.
    // Reuse a single key buffer to avoid per-row Vec allocation.
    let mut hash_table: HashMap<Vec<u64>, Vec<usize>> = HashMap::with_capacity(right_indices.len());
    let mut key_buf: Vec<u64> = Vec::with_capacity(right_key_cols.len());
    for &ri in &*right_indices {
        key_buf.clear();
        let mut has_null = false;
        for &c in right_key_cols {
            let d = right.columns[c].get_datum_ref(ri);
            if matches!(*d, Datum::Null) {
                has_null = true;
                break;
            }
            key_buf.push(hash_datum(&d));
        }
        if !has_null {
            hash_table.entry(key_buf.clone()).or_default().push(ri);
        }
    }

    // Probe phase: for each left row, look up matching right rows.
    // Reuse key_buf for probing — zero allocation per probe.
    let mut out_left_idxs: Vec<usize> = Vec::new();
    let mut out_right_idxs: Vec<usize> = Vec::new();

    for &li in &*left_indices {
        key_buf.clear();
        let mut has_null = false;
        for &c in left_key_cols {
            let d = left.columns[c].get_datum_ref(li);
            if matches!(*d, Datum::Null) {
                has_null = true;
                break;
            }
            key_buf.push(hash_datum(&d));
        }
        if has_null {
            continue;
        }
        if let Some(matches) = hash_table.get(&key_buf) {
            for &ri in matches {
                // Verify actual equality (hash collision check)
                let mut eq = true;
                for (&lk, &rk) in left_key_cols.iter().zip(right_key_cols.iter()) {
                    let lv = left.columns[lk].get_datum_ref(li);
                    let rv = right.columns[rk].get_datum_ref(ri);
                    if !datum_equal(&lv, &rv) {
                        eq = false;
                        break;
                    }
                }
                if eq {
                    out_left_idxs.push(li);
                    out_right_idxs.push(ri);
                }
            }
        }
    }

    // Build output batch: [left_cols..., right_cols...]
    // Use gather() to stay in typed representation — avoids intermediate Vec<Datum>
    // and the from_datums() type-inference re-dispatch per column.
    let num_out = out_left_idxs.len();
    let mut out_columns: Vec<ColumnVector> = Vec::with_capacity(left_ncols + right_ncols);

    // Left columns
    for col_idx in 0..left_ncols {
        out_columns.push(left.columns[col_idx].gather(&out_left_idxs));
    }
    // Right columns
    for col_idx in 0..right_ncols {
        out_columns.push(right.columns[col_idx].gather(&out_right_idxs));
    }

    RecordBatch {
        columns: out_columns,
        num_rows: num_out,
        selection: None,
    }
}

/// Hash a Datum for the vectorized hash join build phase.
fn hash_datum(d: &Datum) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match d {
        Datum::Null => 0u8.hash(&mut hasher),
        Datum::Boolean(v) => v.hash(&mut hasher),
        Datum::Int32(v) => v.hash(&mut hasher),
        Datum::Int64(v) => v.hash(&mut hasher),
        Datum::Float64(v) => v.to_bits().hash(&mut hasher),
        Datum::Text(v) => v.hash(&mut hasher),
        Datum::Timestamp(v) => v.hash(&mut hasher),
        Datum::Date(v) => v.hash(&mut hasher),
        Datum::Array(arr) => {
            for a in arr {
                hash_datum(a).hash(&mut hasher);
            }
            arr.len().hash(&mut hasher);
        }
        Datum::Jsonb(v) => v.to_string().hash(&mut hasher),
        Datum::Decimal(m, s) => {
            m.hash(&mut hasher);
            s.hash(&mut hasher);
        }
        Datum::Time(us) => us.hash(&mut hasher),
        Datum::Interval(mo, d, us) => {
            mo.hash(&mut hasher);
            d.hash(&mut hasher);
            us.hash(&mut hasher);
        }
        Datum::Uuid(v) => v.hash(&mut hasher),
        Datum::Bytea(bytes) => bytes.hash(&mut hasher),
        Datum::TsVector(v) => v.len().hash(&mut hasher),
        Datum::TsQuery(q) => q.hash(&mut hasher),
    }
    hasher.finish()
}

/// Check if two Datum values are equal (for hash join collision verification).
fn datum_equal(a: &Datum, b: &Datum) -> bool {
    match (a, b) {
        (Datum::Null, _) | (_, Datum::Null) => false,
        (Datum::Boolean(x), Datum::Boolean(y)) => x == y,
        (Datum::Int32(x), Datum::Int32(y)) => x == y,
        (Datum::Int64(x), Datum::Int64(y)) => x == y,
        (Datum::Int32(x), Datum::Int64(y)) => i64::from(*x) == *y,
        (Datum::Int64(x), Datum::Int32(y)) => *x == i64::from(*y),
        (Datum::Float64(x), Datum::Float64(y)) => x.to_bits() == y.to_bits(),
        (Datum::Text(x), Datum::Text(y)) => x == y,
        (Datum::Timestamp(x), Datum::Timestamp(y)) => x == y,
        (Datum::Date(x), Datum::Date(y)) => x == y,
        (Datum::Jsonb(x), Datum::Jsonb(y)) => x == y,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Vectorized sort
// ---------------------------------------------------------------------------

/// Sort a RecordBatch by one or more columns. Produces a new RecordBatch with
/// rows rearranged. Supports ascending/descending and nulls-first/nulls-last.
pub fn vectorized_sort(batch: &RecordBatch, sort_keys: &[VecSortKey]) -> RecordBatch {
    if batch.active_count() == 0 {
        return RecordBatch {
            columns: Vec::new(),
            num_rows: 0,
            selection: None,
        };
    }
    if sort_keys.is_empty() {
        return batch.clone();
    }

    let mut indices = batch.active_indices().into_owned();

    indices.sort_unstable_by(|&a, &b| {
        for key in sort_keys {
            let da = batch.columns[key.col_idx].get_datum_ref(a);
            let db = batch.columns[key.col_idx].get_datum_ref(b);
            let ord = cmp_datum_sort(&da, &db, key.nulls_first);
            if ord != std::cmp::Ordering::Equal {
                return if key.descending { ord.reverse() } else { ord };
            }
        }
        std::cmp::Ordering::Equal
    });

    // Build sorted output
    let num_cols = batch.columns.len();
    let mut out_cols = Vec::with_capacity(num_cols);
    for col_idx in 0..num_cols {
        out_cols.push(extract_column(&batch.columns[col_idx], &indices));
    }

    RecordBatch {
        columns: out_cols,
        num_rows: indices.len(),
        selection: None,
    }
}

/// Sort key descriptor for vectorized sort.
#[derive(Debug, Clone)]
pub struct VecSortKey {
    pub col_idx: usize,
    pub descending: bool,
    pub nulls_first: bool,
}

impl VecSortKey {
    /// Build from BoundOrderBy.
    pub const fn from_order_by(ob: &BoundOrderBy) -> Self {
        Self {
            col_idx: ob.column_idx,
            descending: !ob.asc,
            nulls_first: !ob.asc, // default: nulls last for ASC, nulls first for DESC
        }
    }
}

/// Compare two Datum values for sorting, with configurable null handling.
fn cmp_datum_sort(a: &Datum, b: &Datum, nulls_first: bool) -> std::cmp::Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => {
            if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
        (false, true) => {
            if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
        (false, false) => cmp_datum_values(a, b),
    }
}

fn cmp_datum_values(a: &Datum, b: &Datum) -> std::cmp::Ordering {
    match (a, b) {
        (Datum::Int32(x), Datum::Int32(y)) => x.cmp(y),
        (Datum::Int64(x), Datum::Int64(y)) => x.cmp(y),
        (Datum::Int32(x), Datum::Int64(y)) => i64::from(*x).cmp(y),
        (Datum::Int64(x), Datum::Int32(y)) => x.cmp(&i64::from(*y)),
        (Datum::Float64(x), Datum::Float64(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Datum::Text(x), Datum::Text(y)) => x.cmp(y),
        (Datum::Boolean(x), Datum::Boolean(y)) => x.cmp(y),
        (Datum::Timestamp(x), Datum::Timestamp(y)) => x.cmp(y),
        (Datum::Date(x), Datum::Date(y)) => x.cmp(y),
        (Datum::Time(x), Datum::Time(y)) => x.cmp(y),
        (Datum::Decimal(mx, sx), Datum::Decimal(my, sy)) => {
            // Normalize to common scale for correct comparison
            if sx == sy {
                mx.cmp(my)
            } else if sx < sy {
                let shift = u32::from(*sy - *sx);
                let mx_scaled = mx.saturating_mul(10i128.saturating_pow(shift));
                mx_scaled.cmp(my)
            } else {
                let shift = u32::from(*sx - *sy);
                let my_scaled = my.saturating_mul(10i128.saturating_pow(shift));
                mx.cmp(&my_scaled)
            }
        }
        (Datum::Uuid(x), Datum::Uuid(y)) => x.cmp(y),
        (Datum::Bytea(x), Datum::Bytea(y)) => x.cmp(y),
        (Datum::Interval(m1, d1, u1), Datum::Interval(m2, d2, u2)) => {
            // Compare by total microseconds approximation (saturating to avoid overflow)
            let total_a = (*m1 as i64)
                .saturating_mul(30 * 86_400_000_000)
                .saturating_add((*d1 as i64).saturating_mul(86_400_000_000))
                .saturating_add(*u1);
            let total_b = (*m2 as i64)
                .saturating_mul(30 * 86_400_000_000)
                .saturating_add((*d2 as i64).saturating_mul(86_400_000_000))
                .saturating_add(*u2);
            total_a.cmp(&total_b)
        }
        (Datum::Null, Datum::Null) => std::cmp::Ordering::Equal,
        _ => {
            // Rare fallback for Array, Jsonb, or cross-type comparisons.
            // All common same-type pairs are handled above, so format!()
            // only fires for genuinely uncommon cases.
            let sa = format!("{a}");
            let sb = format!("{b}");
            sa.cmp(&sb)
        }
    }
}

// ---------------------------------------------------------------------------
// Vectorized hash GROUP BY aggregation
// ---------------------------------------------------------------------------

/// Per-group accumulator state for a single aggregate function.
#[derive(Debug, Clone)]
enum GroupAccum {
    Count(i64),
    SumI64(i64, bool), // (sum, has_value)
    SumF64(f64, bool),
    MinI64(Option<i64>),
    MinF64(Option<f64>),
    MinDatum(Datum),
    MaxI64(Option<i64>),
    MaxF64(Option<f64>),
    MaxDatum(Datum),
    /// (sum, count) — avg = sum / count at finalize
    AvgF64(f64, usize),
}

impl GroupAccum {
    fn new_for(func: &AggFunc, col: Option<&ColumnVector>) -> Self {
        match func {
            AggFunc::Count => GroupAccum::Count(0),
            AggFunc::Sum => match col {
                Some(ColumnVector::Float64s { .. }) => GroupAccum::SumF64(0.0, false),
                _ => GroupAccum::SumI64(0, false),
            },
            AggFunc::Avg => GroupAccum::AvgF64(0.0, 0),
            AggFunc::Min => match col {
                Some(ColumnVector::Int64s { .. } | ColumnVector::Int32s { .. }) => {
                    GroupAccum::MinI64(None)
                }
                Some(ColumnVector::Float64s { .. }) => GroupAccum::MinF64(None),
                _ => GroupAccum::MinDatum(Datum::Null),
            },
            AggFunc::Max => match col {
                Some(ColumnVector::Int64s { .. } | ColumnVector::Int32s { .. }) => {
                    GroupAccum::MaxI64(None)
                }
                Some(ColumnVector::Float64s { .. }) => GroupAccum::MaxF64(None),
                _ => GroupAccum::MaxDatum(Datum::Null),
            },
            _ => GroupAccum::Count(0), // fallback
        }
    }

    fn accumulate(&mut self, datum: &Datum) {
        match self {
            GroupAccum::Count(c) => {
                if !datum.is_null() {
                    *c += 1;
                }
            }
            GroupAccum::SumI64(s, has) => {
                if let Some(v) = datum.as_i64() {
                    *s = s.wrapping_add(v);
                    *has = true;
                }
            }
            GroupAccum::SumF64(s, has) => {
                if let Some(v) = datum.as_f64() {
                    *s += v;
                    *has = true;
                }
            }
            GroupAccum::AvgF64(s, c) => {
                if let Some(v) = datum.as_f64() {
                    *s += v;
                    *c += 1;
                }
            }
            GroupAccum::MinI64(cur) => {
                if let Some(v) = datum.as_i64() {
                    *cur = Some(cur.map_or(v, |m| m.min(v)));
                }
            }
            GroupAccum::MinF64(cur) => {
                if let Some(v) = datum.as_f64() {
                    *cur = Some(cur.map_or(v, |m| m.min(v)));
                }
            }
            GroupAccum::MinDatum(cur) => {
                if !datum.is_null() && (cur.is_null() || datum < cur) {
                    *cur = datum.clone();
                }
            }
            GroupAccum::MaxI64(cur) => {
                if let Some(v) = datum.as_i64() {
                    *cur = Some(cur.map_or(v, |m| m.max(v)));
                }
            }
            GroupAccum::MaxF64(cur) => {
                if let Some(v) = datum.as_f64() {
                    *cur = Some(cur.map_or(v, |m| m.max(v)));
                }
            }
            GroupAccum::MaxDatum(cur) => {
                if !datum.is_null() && (cur.is_null() || datum > cur) {
                    *cur = datum.clone();
                }
            }
        }
    }

    fn finalize(&self) -> Datum {
        match self {
            GroupAccum::Count(c) => Datum::Int64(*c),
            GroupAccum::SumI64(s, has) => {
                if *has {
                    Datum::Int64(*s)
                } else {
                    Datum::Null
                }
            }
            GroupAccum::SumF64(s, has) => {
                if *has {
                    Datum::Float64(*s)
                } else {
                    Datum::Null
                }
            }
            GroupAccum::AvgF64(s, c) => {
                if *c > 0 {
                    Datum::Float64(*s / *c as f64)
                } else {
                    Datum::Null
                }
            }
            GroupAccum::MinI64(v) => v.map_or(Datum::Null, Datum::Int64),
            GroupAccum::MinF64(v) => v.map_or(Datum::Null, Datum::Float64),
            GroupAccum::MinDatum(d) => d.clone(),
            GroupAccum::MaxI64(v) => v.map_or(Datum::Null, Datum::Int64),
            GroupAccum::MaxF64(v) => v.map_or(Datum::Null, Datum::Float64),
            GroupAccum::MaxDatum(d) => d.clone(),
        }
    }
}

/// Descriptor for one aggregate in a GROUP BY query.
#[derive(Debug, Clone)]
pub struct AggDescriptor {
    pub func: AggFunc,
    /// Column index for the aggregate argument (None = COUNT(*)).
    pub col_idx: Option<usize>,
}

/// Result of a vectorized hash GROUP BY aggregation.
pub struct HashAggResult {
    /// One row per group: [group_key_cols..., agg_result_cols...].
    pub rows: Vec<OwnedRow>,
}

/// Vectorized hash GROUP BY aggregation.
///
/// Hashes group key columns in batch, accumulates typed aggregates per group,
/// and outputs one row per group with [key_cols..., agg_cols...].
///
/// `group_key_cols`: column indices in the batch that form the GROUP BY key.
/// `aggs`: aggregate descriptors (function + argument column).
///
/// Returns one `OwnedRow` per group: first the key columns, then the aggregate results.
pub fn vectorized_hash_agg(
    batch: &RecordBatch,
    group_key_cols: &[usize],
    aggs: &[AggDescriptor],
) -> HashAggResult {
    use std::collections::HashMap;

    let indices = batch.active_indices();
    let num_aggs = aggs.len();

    // Pre-compute accumulator templates based on column types.
    let accum_templates: Vec<GroupAccum> = aggs
        .iter()
        .map(|a| {
            let col = a.col_idx.and_then(|ci| batch.columns.get(ci));
            GroupAccum::new_for(&a.func, col)
        })
        .collect();

    // group_key_hash → (group_id, key_datums)
    // group_accums[group_id] → Vec<GroupAccum>
    let mut group_map: HashMap<u64, Vec<usize>> = HashMap::new();
    let mut group_keys: Vec<Vec<Datum>> = Vec::new();
    let mut group_accums: Vec<Vec<GroupAccum>> = Vec::new();
    let mut key_hash_buf: u64;

    for &idx in &*indices {
        // Hash the group key columns.
        key_hash_buf = 0;
        for &kc in group_key_cols {
            let d = batch.columns[kc].get_datum(idx);
            key_hash_buf = key_hash_buf.wrapping_mul(31).wrapping_add(hash_datum(&d));
        }

        // Look up or insert group.
        let candidates = group_map.entry(key_hash_buf).or_default();
        let mut found_gid: Option<usize> = None;
        for &gid in candidates.iter() {
            // Verify key equality (handle hash collisions).
            let gk = &group_keys[gid];
            let mut eq = true;
            for (ki, &kc) in group_key_cols.iter().enumerate() {
                let d = batch.columns[kc].get_datum(idx);
                if !datum_equal(&d, &gk[ki]) {
                    eq = false;
                    break;
                }
            }
            if eq {
                found_gid = Some(gid);
                break;
            }
        }

        let gid = match found_gid {
            Some(g) => g,
            None => {
                let new_gid = group_keys.len();
                let key: Vec<Datum> = group_key_cols
                    .iter()
                    .map(|&kc| batch.columns[kc].get_datum(idx))
                    .collect();
                group_keys.push(key);
                group_accums.push(accum_templates.clone());
                candidates.push(new_gid);
                new_gid
            }
        };

        // Accumulate each aggregate for this group.
        for (ai, agg) in aggs.iter().enumerate() {
            let datum = match agg.col_idx {
                Some(ci) => batch.columns[ci].get_datum(idx),
                None => Datum::Int64(1), // COUNT(*)
            };
            group_accums[gid][ai].accumulate(&datum);
        }
    }

    // Finalize: one OwnedRow per group = [key_cols..., agg_results...].
    let num_keys = group_key_cols.len();
    let mut rows = Vec::with_capacity(group_keys.len());
    for (gid, key) in group_keys.into_iter().enumerate() {
        let mut values = Vec::with_capacity(num_keys + num_aggs);
        values.extend(key);
        for accum in group_accums[gid].iter().take(num_aggs) {
            values.push(accum.finalize());
        }
        rows.push(OwnedRow::new(values));
    }

    HashAggResult { rows }
}

// ---------------------------------------------------------------------------
// Vectorized LIMIT / OFFSET
// ---------------------------------------------------------------------------

/// Apply LIMIT and OFFSET to a RecordBatch without materializing rows.
/// Adjusts the selection vector to skip `offset` active rows and keep at most `limit`.
pub fn vectorized_limit_offset(
    batch: &mut RecordBatch,
    limit: Option<usize>,
    offset: Option<usize>,
) {
    let off = offset.unwrap_or(0);
    let indices = batch.active_indices();
    let total = indices.len();

    if off >= total {
        batch.selection = Some(Vec::new());
        return;
    }

    let remaining = total - off;
    let take = limit.map_or(remaining, |l| l.min(remaining));

    if off == 0 && take == total {
        return; // no-op
    }

    let new_sel: Vec<usize> = indices[off..off + take].to_vec();
    batch.selection = Some(new_sel);
}

// ---------------------------------------------------------------------------
// Batch size configuration
// ---------------------------------------------------------------------------

/// Default vectorized batch size (number of rows per batch).
pub const DEFAULT_BATCH_SIZE: usize = 1024;

/// Check if a query is eligible for vectorized execution.
/// Criteria: no correlated subqueries, no window functions, simple projections.
pub fn is_vectorizable(projections: &[BoundProjection], filter: Option<&BoundExpr>) -> bool {
    // No window functions
    if projections
        .iter()
        .any(|p| matches!(p, BoundProjection::Window(..)))
    {
        return false;
    }
    // No subqueries in filter
    if let Some(f) = filter {
        if expr_has_subquery(f) {
            return false;
        }
    }
    true
}

fn expr_has_subquery(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::ScalarSubquery(_)
        | BoundExpr::InSubquery { .. }
        | BoundExpr::Exists { .. }
        | BoundExpr::OuterColumnRef(_) => true,
        BoundExpr::BinaryOp { left, right, .. } => {
            expr_has_subquery(left) || expr_has_subquery(right)
        }
        BoundExpr::Not(inner) => expr_has_subquery(inner),
        BoundExpr::IsNull(inner) | BoundExpr::IsNotNull(inner) => expr_has_subquery(inner),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_vector_from_datums_int64() {
        let datums = vec![
            Datum::Int64(1),
            Datum::Int64(2),
            Datum::Null,
            Datum::Int64(4),
        ];
        let col = ColumnVector::from_datums(&datums);
        assert_eq!(col.len(), 4);
        assert_eq!(col.get_datum(0), Datum::Int64(1));
        assert!(col.get_datum(2).is_null());
    }

    #[test]
    fn test_record_batch_roundtrip() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(1), Datum::Text("a".into())]),
            OwnedRow::new(vec![Datum::Int64(2), Datum::Text("b".into())]),
        ];
        let batch = RecordBatch::from_rows(&rows, 2);
        assert_eq!(batch.num_rows, 2);
        let back = batch.to_rows();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].values[0], Datum::Int64(1));
        assert_eq!(back[1].values[1], Datum::Text("b".into()));
    }

    #[test]
    fn test_vectorized_filter_eq() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(10)]),
            OwnedRow::new(vec![Datum::Int64(20)]),
            OwnedRow::new(vec![Datum::Int64(10)]),
        ];
        let mut batch = RecordBatch::from_rows(&rows, 1);
        let filter = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Int64(10))),
        };
        vectorized_filter(&mut batch, &filter);
        assert_eq!(batch.active_count(), 2);
    }

    #[test]
    fn test_vectorized_sum() {
        let datums = vec![
            Datum::Int64(10),
            Datum::Int64(20),
            Datum::Null,
            Datum::Int64(30),
        ];
        let col = ColumnVector::from_datums(&datums);
        let indices: Vec<usize> = (0..4).collect();
        let result = vectorized_sum(&col, &indices).unwrap();
        assert_eq!(result, Datum::Int64(60));
    }

    #[test]
    fn test_vectorized_avg() {
        let datums = vec![
            Datum::Float64(10.0),
            Datum::Float64(20.0),
            Datum::Float64(30.0),
        ];
        let col = ColumnVector::from_datums(&datums);
        let indices: Vec<usize> = (0..3).collect();
        let result = vectorized_avg(&col, &indices).unwrap();
        match result {
            Datum::Float64(v) => assert!((v - 20.0).abs() < 0.01),
            _ => panic!("expected Float64"),
        }
    }

    #[test]
    fn test_vectorized_variance() {
        let datums = vec![Datum::Int64(10), Datum::Int64(20), Datum::Int64(30)];
        let col = ColumnVector::from_datums(&datums);
        let indices: Vec<usize> = (0..3).collect();
        let result = vectorized_variance(&col, &indices, true).unwrap();
        match result {
            Datum::Float64(v) => assert!((v - 66.67).abs() < 0.1),
            _ => panic!("expected Float64"),
        }
    }

    #[test]
    fn test_vectorized_project_column() {
        let rows = vec![
            OwnedRow::new(vec![
                Datum::Int64(1),
                Datum::Text("a".into()),
                Datum::Int32(10),
            ]),
            OwnedRow::new(vec![
                Datum::Int64(2),
                Datum::Text("b".into()),
                Datum::Int32(20),
            ]),
            OwnedRow::new(vec![
                Datum::Int64(3),
                Datum::Text("c".into()),
                Datum::Int32(30),
            ]),
        ];
        let batch = RecordBatch::from_rows(&rows, 3);
        // Project only columns 0 and 2
        let projs = vec![
            BoundProjection::Column(0, "id".into()),
            BoundProjection::Column(2, "val".into()),
        ];
        let result = vectorized_project(&batch, &projs).unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.num_rows, 3);
        let out_rows = result.to_rows();
        assert_eq!(out_rows[0].values[0], Datum::Int64(1));
        assert_eq!(out_rows[0].values[1], Datum::Int32(10));
        assert_eq!(out_rows[2].values[0], Datum::Int64(3));
        assert_eq!(out_rows[2].values[1], Datum::Int32(30));
    }

    #[test]
    fn test_vectorized_project_with_filter() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(1), Datum::Text("keep".into())]),
            OwnedRow::new(vec![Datum::Int64(2), Datum::Text("drop".into())]),
            OwnedRow::new(vec![Datum::Int64(1), Datum::Text("keep2".into())]),
        ];
        let mut batch = RecordBatch::from_rows(&rows, 2);
        let filter = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal(Datum::Int64(1))),
        };
        vectorized_filter(&mut batch, &filter);
        assert_eq!(batch.active_count(), 2);

        let projs = vec![BoundProjection::Column(1, "name".into())];
        let result = vectorized_project(&batch, &projs).unwrap();
        assert_eq!(result.num_rows, 2);
        let out_rows = result.to_rows();
        assert_eq!(out_rows[0].values[0], Datum::Text("keep".into()));
        assert_eq!(out_rows[1].values[0], Datum::Text("keep2".into()));
    }

    #[test]
    fn test_vectorized_hash_join() {
        let left_rows = vec![
            OwnedRow::new(vec![Datum::Int64(1), Datum::Text("a".into())]),
            OwnedRow::new(vec![Datum::Int64(2), Datum::Text("b".into())]),
            OwnedRow::new(vec![Datum::Int64(3), Datum::Text("c".into())]),
        ];
        let right_rows = vec![
            OwnedRow::new(vec![Datum::Int64(2), Datum::Int32(200)]),
            OwnedRow::new(vec![Datum::Int64(3), Datum::Int32(300)]),
            OwnedRow::new(vec![Datum::Int64(4), Datum::Int32(400)]),
        ];
        let left = RecordBatch::from_rows(&left_rows, 2);
        let right = RecordBatch::from_rows(&right_rows, 2);

        let result = vectorized_hash_join(&left, &right, &[0], &[0]);
        // Should match rows with key 2 and 3
        assert_eq!(result.num_rows, 2);
        let out = result.to_rows();
        // Output: [left_col0, left_col1, right_col0, right_col1]
        assert_eq!(out[0].values.len(), 4);
        // First match: left key=2
        assert_eq!(out[0].values[0], Datum::Int64(2));
        assert_eq!(out[0].values[1], Datum::Text("b".into()));
        assert_eq!(out[0].values[3], Datum::Int32(200));
        // Second match: left key=3
        assert_eq!(out[1].values[0], Datum::Int64(3));
        assert_eq!(out[1].values[3], Datum::Int32(300));
    }

    #[test]
    fn test_vectorized_hash_join_no_matches() {
        let left = RecordBatch::from_rows(&[OwnedRow::new(vec![Datum::Int64(1)])], 1);
        let right = RecordBatch::from_rows(&[OwnedRow::new(vec![Datum::Int64(99)])], 1);
        let result = vectorized_hash_join(&left, &right, &[0], &[0]);
        assert_eq!(result.num_rows, 0);
    }

    #[test]
    fn test_vectorized_sort_ascending() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(30)]),
            OwnedRow::new(vec![Datum::Int64(10)]),
            OwnedRow::new(vec![Datum::Int64(20)]),
        ];
        let batch = RecordBatch::from_rows(&rows, 1);
        let sorted = vectorized_sort(
            &batch,
            &[VecSortKey {
                col_idx: 0,
                descending: false,
                nulls_first: false,
            }],
        );
        let out = sorted.to_rows();
        assert_eq!(out[0].values[0], Datum::Int64(10));
        assert_eq!(out[1].values[0], Datum::Int64(20));
        assert_eq!(out[2].values[0], Datum::Int64(30));
    }

    #[test]
    fn test_vectorized_sort_descending() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(10)]),
            OwnedRow::new(vec![Datum::Int64(30)]),
            OwnedRow::new(vec![Datum::Int64(20)]),
        ];
        let batch = RecordBatch::from_rows(&rows, 1);
        let sorted = vectorized_sort(
            &batch,
            &[VecSortKey {
                col_idx: 0,
                descending: true,
                nulls_first: false,
            }],
        );
        let out = sorted.to_rows();
        assert_eq!(out[0].values[0], Datum::Int64(30));
        assert_eq!(out[1].values[0], Datum::Int64(20));
        assert_eq!(out[2].values[0], Datum::Int64(10));
    }

    #[test]
    fn test_vectorized_sort_with_nulls() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(20)]),
            OwnedRow::new(vec![Datum::Null]),
            OwnedRow::new(vec![Datum::Int64(10)]),
        ];
        let batch = RecordBatch::from_rows(&rows, 1);
        // nulls_first = true → NULL should come first
        let sorted = vectorized_sort(
            &batch,
            &[VecSortKey {
                col_idx: 0,
                descending: false,
                nulls_first: true,
            }],
        );
        let out = sorted.to_rows();
        assert!(out[0].values[0].is_null());
        assert_eq!(out[1].values[0], Datum::Int64(10));
        assert_eq!(out[2].values[0], Datum::Int64(20));
    }

    #[test]
    fn test_vectorized_sort_multi_key() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(1), Datum::Text("b".into())]),
            OwnedRow::new(vec![Datum::Int64(2), Datum::Text("a".into())]),
            OwnedRow::new(vec![Datum::Int64(1), Datum::Text("a".into())]),
        ];
        let batch = RecordBatch::from_rows(&rows, 2);
        let sorted = vectorized_sort(
            &batch,
            &[
                VecSortKey {
                    col_idx: 0,
                    descending: false,
                    nulls_first: false,
                },
                VecSortKey {
                    col_idx: 1,
                    descending: false,
                    nulls_first: false,
                },
            ],
        );
        let out = sorted.to_rows();
        assert_eq!(out[0].values[0], Datum::Int64(1));
        assert_eq!(out[0].values[1], Datum::Text("a".into()));
        assert_eq!(out[1].values[0], Datum::Int64(1));
        assert_eq!(out[1].values[1], Datum::Text("b".into()));
        assert_eq!(out[2].values[0], Datum::Int64(2));
    }

    #[test]
    fn test_vectorized_hash_agg_group_by() {
        // 3 rows: (dept=1, sal=100), (dept=2, sal=200), (dept=1, sal=300)
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(1), Datum::Int64(100)]),
            OwnedRow::new(vec![Datum::Int64(2), Datum::Int64(200)]),
            OwnedRow::new(vec![Datum::Int64(1), Datum::Int64(300)]),
        ];
        let batch = RecordBatch::from_rows(&rows, 2);
        let aggs = vec![
            AggDescriptor {
                func: AggFunc::Count,
                col_idx: Some(1),
            },
            AggDescriptor {
                func: AggFunc::Sum,
                col_idx: Some(1),
            },
            AggDescriptor {
                func: AggFunc::Avg,
                col_idx: Some(1),
            },
        ];
        let result = vectorized_hash_agg(&batch, &[0], &aggs);
        assert_eq!(result.rows.len(), 2); // 2 groups

        // Find dept=1 group
        let g1 = result
            .rows
            .iter()
            .find(|r| r.values[0] == Datum::Int64(1))
            .unwrap();
        assert_eq!(g1.values[1], Datum::Int64(2)); // COUNT = 2
        assert_eq!(g1.values[2], Datum::Int64(400)); // SUM = 400
        match g1.values[3] {
            Datum::Float64(v) => assert!((v - 200.0).abs() < 0.01), // AVG = 200
            _ => panic!("expected Float64 for AVG"),
        }

        // Find dept=2 group
        let g2 = result
            .rows
            .iter()
            .find(|r| r.values[0] == Datum::Int64(2))
            .unwrap();
        assert_eq!(g2.values[1], Datum::Int64(1)); // COUNT = 1
        assert_eq!(g2.values[2], Datum::Int64(200)); // SUM = 200
    }

    #[test]
    fn test_vectorized_hash_agg_with_nulls() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(1), Datum::Int64(10)]),
            OwnedRow::new(vec![Datum::Int64(1), Datum::Null]),
            OwnedRow::new(vec![Datum::Int64(1), Datum::Int64(30)]),
        ];
        let batch = RecordBatch::from_rows(&rows, 2);
        let aggs = vec![
            AggDescriptor {
                func: AggFunc::Count,
                col_idx: Some(1),
            },
            AggDescriptor {
                func: AggFunc::Sum,
                col_idx: Some(1),
            },
            AggDescriptor {
                func: AggFunc::Min,
                col_idx: Some(1),
            },
            AggDescriptor {
                func: AggFunc::Max,
                col_idx: Some(1),
            },
        ];
        let result = vectorized_hash_agg(&batch, &[0], &aggs);
        assert_eq!(result.rows.len(), 1);
        let g = &result.rows[0];
        assert_eq!(g.values[1], Datum::Int64(2)); // COUNT skips NULL
        assert_eq!(g.values[2], Datum::Int64(40)); // SUM skips NULL
        assert_eq!(g.values[3], Datum::Int64(10)); // MIN
        assert_eq!(g.values[4], Datum::Int64(30)); // MAX
    }

    #[test]
    fn test_extract_column_typed() {
        let mut nulls = NullBitmap::all_false(4);
        nulls.set(2, true);
        let col = ColumnVector::Int32s {
            values: vec![10, 20, 30, 40],
            nulls,
        };
        let extracted = extract_column(&col, &[0, 2, 3]);
        assert_eq!(extracted.len(), 3);
        assert_eq!(extracted.get_datum(0), Datum::Int32(10));
        assert!(extracted.get_datum(1).is_null());
        assert_eq!(extracted.get_datum(2), Datum::Int32(40));
    }

    #[test]
    fn test_column_vector_timestamp() {
        let datums = vec![Datum::Timestamp(1000), Datum::Null, Datum::Timestamp(3000)];
        let col = ColumnVector::from_datums(&datums);
        assert!(matches!(col, ColumnVector::Timestamps { .. }));
        assert_eq!(col.len(), 3);
        assert_eq!(col.get_datum(0), Datum::Timestamp(1000));
        assert!(col.get_datum(1).is_null());
        assert_eq!(col.get_datum(2), Datum::Timestamp(3000));

        let gathered = col.gather(&[2, 0]);
        assert_eq!(gathered.len(), 2);
        assert_eq!(gathered.get_datum(0), Datum::Timestamp(3000));
        assert_eq!(gathered.get_datum(1), Datum::Timestamp(1000));
    }

    #[test]
    fn test_column_vector_date() {
        let datums = vec![Datum::Date(19000), Datum::Date(19500)];
        let col = ColumnVector::from_datums(&datums);
        assert!(matches!(col, ColumnVector::Dates { .. }));
        assert_eq!(col.get_datum(0), Datum::Date(19000));
        assert_eq!(col.get_datum(1), Datum::Date(19500));
    }

    #[test]
    fn test_column_vector_bytea() {
        let datums = vec![
            Datum::Bytea(vec![0xDE, 0xAD]),
            Datum::Null,
            Datum::Bytea(vec![0xBE, 0xEF]),
        ];
        let col = ColumnVector::from_datums(&datums);
        assert!(matches!(col, ColumnVector::Byteas { .. }));
        assert_eq!(col.get_datum(0), Datum::Bytea(vec![0xDE, 0xAD]));
        assert!(col.get_datum(1).is_null());

        let extracted = extract_column(&col, &[0, 2]);
        assert_eq!(extracted.len(), 2);
        assert_eq!(extracted.get_datum(1), Datum::Bytea(vec![0xBE, 0xEF]));
    }

    #[test]
    fn test_record_batch_timestamp_roundtrip() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Timestamp(100), Datum::Date(200)]),
            OwnedRow::new(vec![Datum::Timestamp(300), Datum::Date(400)]),
        ];
        let batch = RecordBatch::from_rows(&rows, 2);
        let back = batch.to_rows();
        assert_eq!(back[0].values[0], Datum::Timestamp(100));
        assert_eq!(back[1].values[1], Datum::Date(400));
    }

    #[test]
    fn test_vectorized_limit_offset() {
        let rows: Vec<OwnedRow> = (0..10)
            .map(|i| OwnedRow::new(vec![Datum::Int64(i)]))
            .collect();
        let mut batch = RecordBatch::from_rows(&rows, 1);

        // OFFSET 3, LIMIT 4 → rows 3..7
        vectorized_limit_offset(&mut batch, Some(4), Some(3));
        assert_eq!(batch.active_count(), 4);
        let out = batch.to_rows();
        assert_eq!(out[0].values[0], Datum::Int64(3));
        assert_eq!(out[3].values[0], Datum::Int64(6));
    }

    #[test]
    fn test_vectorized_limit_offset_only_limit() {
        let rows: Vec<OwnedRow> = (0..5)
            .map(|i| OwnedRow::new(vec![Datum::Int64(i)]))
            .collect();
        let mut batch = RecordBatch::from_rows(&rows, 1);
        vectorized_limit_offset(&mut batch, Some(2), None);
        assert_eq!(batch.active_count(), 2);
        let out = batch.to_rows();
        assert_eq!(out[0].values[0], Datum::Int64(0));
        assert_eq!(out[1].values[0], Datum::Int64(1));
    }

    #[test]
    fn test_vectorized_limit_offset_beyond() {
        let rows = vec![OwnedRow::new(vec![Datum::Int64(1)])];
        let mut batch = RecordBatch::from_rows(&rows, 1);
        vectorized_limit_offset(&mut batch, Some(10), Some(100));
        assert_eq!(batch.active_count(), 0);
    }

    #[test]
    fn test_vectorized_limit_on_filtered_batch() {
        let rows: Vec<OwnedRow> = (0..10)
            .map(|i| OwnedRow::new(vec![Datum::Int64(i)]))
            .collect();
        let mut batch = RecordBatch::from_rows(&rows, 1);
        // Filter: keep even numbers (0,2,4,6,8)
        let filter = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(0)),
            op: BinOp::Lt,
            right: Box::new(BoundExpr::Literal(Datum::Int64(6))),
        };
        vectorized_filter(&mut batch, &filter);
        assert_eq!(batch.active_count(), 6); // 0..5

        // OFFSET 2 LIMIT 2 on the filtered set → rows at indices 2,3
        vectorized_limit_offset(&mut batch, Some(2), Some(2));
        assert_eq!(batch.active_count(), 2);
        let out = batch.to_rows();
        assert_eq!(out[0].values[0], Datum::Int64(2));
        assert_eq!(out[1].values[0], Datum::Int64(3));
    }
}
