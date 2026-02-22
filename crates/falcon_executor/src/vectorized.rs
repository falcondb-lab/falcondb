//! Vectorized execution engine — operates on column batches instead of row-at-a-time.
//!
//! Key types:
//!   - `ColumnVector`: a typed column of values (like a mini Arrow array)
//!   - `RecordBatch`: a collection of column vectors with a selection vector
//!   - Vectorized filter, project, and aggregate operations
//!
//! The vectorized path is used for full-table scans and aggregations where
//! batch processing amortises per-row overhead and enables SIMD-friendly loops.

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::*;

// ---------------------------------------------------------------------------
// ColumnVector
// ---------------------------------------------------------------------------

/// A typed column of homogeneous values.  Unlike `Vec<Datum>` this layout
/// is more cache-friendly because the type tag is stored once, not per-value.
#[derive(Debug, Clone)]
pub enum ColumnVector {
    /// Boolean column with optional null bitmap.
    Booleans { values: Vec<bool>, nulls: Vec<bool> },
    /// 32-bit integer column.
    Int32s { values: Vec<i32>, nulls: Vec<bool> },
    /// 64-bit integer column.
    Int64s { values: Vec<i64>, nulls: Vec<bool> },
    /// 64-bit float column.
    Float64s { values: Vec<f64>, nulls: Vec<bool> },
    /// Text column (heap-allocated strings).
    Texts {
        values: Vec<String>,
        nulls: Vec<bool>,
    },
    /// Fallback: heterogeneous Datum column (for mixed / unsupported types).
    Mixed(Vec<Datum>),
}

impl ColumnVector {
    /// Whether the column is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of logical rows.
    pub fn len(&self) -> usize {
        match self {
            Self::Booleans { values, .. } => values.len(),
            Self::Int32s { values, .. } => values.len(),
            Self::Int64s { values, .. } => values.len(),
            Self::Float64s { values, .. } => values.len(),
            Self::Texts { values, .. } => values.len(),
            Self::Mixed(v) => v.len(),
        }
    }

    /// Get value at index as a Datum.
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
            Self::Mixed(v) => v[idx].clone(),
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
                let mut nulls = Vec::with_capacity(datums.len());
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
                let mut nulls = Vec::with_capacity(datums.len());
                for d in datums {
                    match d {
                        Datum::Int64(v) => {
                            values.push(*v);
                            nulls.push(false);
                        }
                        Datum::Int32(v) => {
                            values.push(*v as i64);
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
                let mut nulls = Vec::with_capacity(datums.len());
                for d in datums {
                    match d {
                        Datum::Float64(v) => {
                            values.push(*v);
                            nulls.push(false);
                        }
                        Datum::Int32(v) => {
                            values.push(*v as f64);
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
                let mut nulls = Vec::with_capacity(datums.len());
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
                let mut nulls = Vec::with_capacity(datums.len());
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
            _ => Self::Mixed(datums.to_vec()),
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

        RecordBatch {
            columns,
            num_rows,
            selection: None,
        }
    }

    /// Build a RecordBatch directly from pre-computed column vectors (columnar scan path).
    /// Each element of `columns` is a Vec<Datum> for one column; all must have equal length.
    pub fn from_columns(columns: Vec<Vec<Datum>>) -> Self {
        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);
        let col_vecs = columns
            .iter()
            .map(|c| ColumnVector::from_datums(c))
            .collect();
        RecordBatch {
            columns: col_vecs,
            num_rows,
            selection: None,
        }
    }

    /// Materialise active rows back to OwnedRow format.
    pub fn to_rows(&self) -> Vec<OwnedRow> {
        let indices = self.active_indices();
        let num_cols = self.columns.len();
        let mut rows = Vec::with_capacity(indices.len());
        for &idx in &indices {
            let mut values = Vec::with_capacity(num_cols);
            for col in &self.columns {
                values.push(col.get_datum(idx));
            }
            rows.push(OwnedRow::new(values));
        }
        rows
    }

    /// Return active row indices.
    pub fn active_indices(&self) -> Vec<usize> {
        match &self.selection {
            Some(sel) => sel.clone(),
            None => (0..self.num_rows).collect(),
        }
    }

    /// Number of active (non-filtered) rows.
    pub fn active_count(&self) -> usize {
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
            // Fallback: row-at-a-time
            for &idx in &indices {
                let row = make_row_from_batch(batch, idx);
                if crate::expr_engine::ExprEngine::eval_filter(filter, &row).unwrap_or(false) {
                    new_sel.push(idx);
                }
            }
            batch.selection = Some(new_sel);
        }
        BoundExpr::Not(inner) => {
            // Apply inner, then invert
            let before: std::collections::HashSet<usize> = indices.iter().copied().collect();
            vectorized_filter(batch, inner);
            let after: std::collections::HashSet<usize> =
                batch.active_indices().into_iter().collect();
            let inverted: Vec<usize> = before.difference(&after).copied().collect();
            batch.selection = Some(inverted);
        }
        _ => {
            // Fallback: row-at-a-time
            for &idx in &indices {
                let row = make_row_from_batch(batch, idx);
                if crate::expr_engine::ExprEngine::eval_filter(filter, &row).unwrap_or(false) {
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
            let lit64 = *lit as i64;
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
                if int_cmp(values[idx] as i64, *lit as i64, op) {
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
fn int_cmp(a: i64, b: i64, op: BinOp) -> bool {
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
    match (a.as_f64(), b.as_f64()) {
        (Some(af), Some(bf)) => float_cmp(af, bf, op),
        _ => {
            let sa = format!("{}", a);
            let sb = format!("{}", b);
            str_cmp(&sa, &sb, op)
        }
    }
}

fn make_row_from_batch(batch: &RecordBatch, idx: usize) -> OwnedRow {
    let values: Vec<Datum> = batch.columns.iter().map(|c| c.get_datum(idx)).collect();
    OwnedRow::new(values)
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
            "Vectorized aggregate not supported for {:?}",
            func
        ))),
    }
}

fn vectorized_sum(col: &ColumnVector, indices: &[usize]) -> Result<Datum, ExecutionError> {
    match col {
        ColumnVector::Int64s { values, nulls } => {
            let mut sum: i64 = 0;
            let mut has_value = false;
            for &idx in indices {
                if !nulls[idx] {
                    sum += values[idx];
                    has_value = true;
                }
            }
            Ok(if has_value {
                Datum::Int64(sum)
            } else {
                Datum::Null
            })
        }
        ColumnVector::Int32s { values, nulls } => {
            let mut sum: i64 = 0;
            let mut has_value = false;
            for &idx in indices {
                if !nulls[idx] {
                    sum += values[idx] as i64;
                    has_value = true;
                }
            }
            Ok(if has_value {
                Datum::Int64(sum)
            } else {
                Datum::Null
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
            Ok(min.map(Datum::Int64).unwrap_or(Datum::Null))
        }
        ColumnVector::Float64s { values, nulls } => {
            let mut min: Option<f64> = None;
            for &idx in indices {
                if !nulls[idx] {
                    min = Some(min.map_or(values[idx], |m: f64| m.min(values[idx])));
                }
            }
            Ok(min.map(Datum::Float64).unwrap_or(Datum::Null))
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
            Ok(max.map(Datum::Int64).unwrap_or(Datum::Null))
        }
        ColumnVector::Float64s { values, nulls } => {
            let mut max: Option<f64> = None;
            for &idx in indices {
                if !nulls[idx] {
                    max = Some(max.map_or(values[idx], |m: f64| m.max(values[idx])));
                }
            }
            Ok(max.map(Datum::Float64).unwrap_or(Datum::Null))
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

/// Apply projections to a RecordBatch, producing a new batch with only the
/// projected columns. Handles Column, Expr (constant-only), and Aggregate refs.
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
                // Evaluate expression per active row
                let mut datums = Vec::with_capacity(num_active);
                for &idx in &indices {
                    let row = make_row_from_batch(batch, idx);
                    let val =
                        crate::expr_engine::ExprEngine::eval_row(expr, &row).unwrap_or(Datum::Null);
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
            let mut n = Vec::with_capacity(indices.len());
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
            let mut n = Vec::with_capacity(indices.len());
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
            let mut n = Vec::with_capacity(indices.len());
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
            let mut n = Vec::with_capacity(indices.len());
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
            let mut n = Vec::with_capacity(indices.len());
            for &i in indices {
                v.push(values[i].clone());
                n.push(nulls[i]);
            }
            ColumnVector::Texts {
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

    // Build phase: hash table keyed by right key columns → list of right row indices
    let mut hash_table: HashMap<Vec<u64>, Vec<usize>> = HashMap::new();
    for &ri in &right_indices {
        let key: Vec<u64> = right_key_cols
            .iter()
            .map(|&c| hash_datum(&right.columns[c].get_datum(ri)))
            .collect();
        hash_table.entry(key).or_default().push(ri);
    }

    // Probe phase: for each left row, look up matching right rows
    let mut out_left_idxs: Vec<usize> = Vec::new();
    let mut out_right_idxs: Vec<usize> = Vec::new();

    for &li in &left_indices {
        let probe_key: Vec<u64> = left_key_cols
            .iter()
            .map(|&c| hash_datum(&left.columns[c].get_datum(li)))
            .collect();
        if let Some(matches) = hash_table.get(&probe_key) {
            for &ri in matches {
                // Verify actual equality (hash collision check)
                let mut eq = true;
                for (&lk, &rk) in left_key_cols.iter().zip(right_key_cols.iter()) {
                    let lv = left.columns[lk].get_datum(li);
                    let rv = right.columns[rk].get_datum(ri);
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
    let num_out = out_left_idxs.len();
    let mut out_columns: Vec<ColumnVector> = Vec::with_capacity(left_ncols + right_ncols);

    // Left columns
    for col_idx in 0..left_ncols {
        let mut datums = Vec::with_capacity(num_out);
        for &li in &out_left_idxs {
            datums.push(left.columns[col_idx].get_datum(li));
        }
        out_columns.push(ColumnVector::from_datums(&datums));
    }
    // Right columns
    for col_idx in 0..right_ncols {
        let mut datums = Vec::with_capacity(num_out);
        for &ri in &out_right_idxs {
            datums.push(right.columns[col_idx].get_datum(ri));
        }
        out_columns.push(ColumnVector::from_datums(&datums));
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
                let _ = hash_datum(a);
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
    }
    hasher.finish()
}

/// Check if two Datum values are equal (for hash join collision verification).
fn datum_equal(a: &Datum, b: &Datum) -> bool {
    match (a, b) {
        (Datum::Null, Datum::Null) => true,
        (Datum::Boolean(x), Datum::Boolean(y)) => x == y,
        (Datum::Int32(x), Datum::Int32(y)) => x == y,
        (Datum::Int64(x), Datum::Int64(y)) => x == y,
        (Datum::Int32(x), Datum::Int64(y)) => (*x as i64) == *y,
        (Datum::Int64(x), Datum::Int32(y)) => *x == (*y as i64),
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
    if sort_keys.is_empty() || batch.active_count() == 0 {
        return batch.clone();
    }

    let mut indices = batch.active_indices();

    indices.sort_by(|&a, &b| {
        for key in sort_keys {
            let da = batch.columns[key.col_idx].get_datum(a);
            let db = batch.columns[key.col_idx].get_datum(b);
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
    pub fn from_order_by(ob: &BoundOrderBy) -> Self {
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
        (Datum::Int32(x), Datum::Int64(y)) => (*x as i64).cmp(y),
        (Datum::Int64(x), Datum::Int32(y)) => x.cmp(&(*y as i64)),
        (Datum::Float64(x), Datum::Float64(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Datum::Text(x), Datum::Text(y)) => x.cmp(y),
        (Datum::Boolean(x), Datum::Boolean(y)) => x.cmp(y),
        (Datum::Timestamp(x), Datum::Timestamp(y)) => x.cmp(y),
        (Datum::Date(x), Datum::Date(y)) => x.cmp(y),
        _ => {
            let sa = format!("{}", a);
            let sb = format!("{}", b);
            sa.cmp(&sb)
        }
    }
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
    fn test_extract_column_typed() {
        let col = ColumnVector::Int32s {
            values: vec![10, 20, 30, 40],
            nulls: vec![false, false, true, false],
        };
        let extracted = extract_column(&col, &[0, 2, 3]);
        assert_eq!(extracted.len(), 3);
        assert_eq!(extracted.get_datum(0), Datum::Int32(10));
        assert!(extracted.get_datum(1).is_null());
        assert_eq!(extracted.get_datum(2), Datum::Int32(40));
    }
}
