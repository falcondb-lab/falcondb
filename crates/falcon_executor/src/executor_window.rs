use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_sql_frontend::types::{BoundExpr, BoundProjection, WindowFrame, WindowFrameBound};

use crate::executor::Executor;

impl Executor {
    /// Compute window function values and inject them into projected result rows.
    pub(crate) fn compute_window_functions(
        &self,
        source_rows: &[&OwnedRow],
        projections: &[BoundProjection],
        result_rows: &mut [OwnedRow],
    ) -> Result<(), FalconError> {
        use falcon_sql_frontend::types::WindowFunc;

        for (proj_idx, proj) in projections.iter().enumerate() {
            if let BoundProjection::Window(wf) = proj {
                // Build partition groups: partition_key -> vec of row indices
                let mut partitions: std::collections::HashMap<Vec<u8>, Vec<usize>> =
                    std::collections::HashMap::new();
                for (i, row) in source_rows.iter().enumerate() {
                    let mut key = Vec::new();
                    for &col in &wf.partition_by {
                        let datum = row.get(col).unwrap_or(&Datum::Null);
                        key.extend_from_slice(&format!("{:?}|", datum).into_bytes());
                    }
                    partitions.entry(key).or_default().push(i);
                }

                // For each partition, sort by window ORDER BY and compute values
                for (_key, mut indices) in partitions {
                    // Sort partition rows by ORDER BY
                    if !wf.order_by.is_empty() {
                        indices.sort_by(|&a, &b| {
                            for ob in &wf.order_by {
                                let av = source_rows[a].get(ob.column_idx).unwrap_or(&Datum::Null);
                                let bv = source_rows[b].get(ob.column_idx).unwrap_or(&Datum::Null);
                                let cmp = if ob.asc { av.cmp(bv) } else { bv.cmp(av) };
                                if cmp != std::cmp::Ordering::Equal {
                                    return cmp;
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                    }

                    match &wf.func {
                        WindowFunc::RowNumber => {
                            for (rank, &row_idx) in indices.iter().enumerate() {
                                result_rows[row_idx].values[proj_idx] = Datum::Int64((rank + 1) as i64);
                            }
                        }
                        WindowFunc::Rank => {
                            let mut rank = 1usize;
                            for (i, &row_idx) in indices.iter().enumerate() {
                                if i > 0 {
                                    // Check if ORDER BY values differ from previous
                                    let prev_idx = indices[i - 1];
                                    let differs = wf.order_by.iter().any(|ob| {
                                        let a = source_rows[prev_idx]
                                            .get(ob.column_idx)
                                            .unwrap_or(&Datum::Null);
                                        let b = source_rows[row_idx]
                                            .get(ob.column_idx)
                                            .unwrap_or(&Datum::Null);
                                        a.cmp(b) != std::cmp::Ordering::Equal
                                    });
                                    if differs {
                                        rank = i + 1;
                                    }
                                }
                                result_rows[row_idx].values[proj_idx] = Datum::Int64(rank as i64);
                            }
                        }
                        WindowFunc::DenseRank => {
                            let mut rank = 1usize;
                            for (i, &row_idx) in indices.iter().enumerate() {
                                if i > 0 {
                                    let prev_idx = indices[i - 1];
                                    let differs = wf.order_by.iter().any(|ob| {
                                        let a = source_rows[prev_idx]
                                            .get(ob.column_idx)
                                            .unwrap_or(&Datum::Null);
                                        let b = source_rows[row_idx]
                                            .get(ob.column_idx)
                                            .unwrap_or(&Datum::Null);
                                        a.cmp(b) != std::cmp::Ordering::Equal
                                    });
                                    if differs {
                                        rank += 1;
                                    }
                                }
                                result_rows[row_idx].values[proj_idx] = Datum::Int64(rank as i64);
                            }
                        }
                        WindowFunc::Ntile(n) => {
                            let total = indices.len();
                            for (i, &row_idx) in indices.iter().enumerate() {
                                let bucket = (i as i64 * *n) / total as i64 + 1;
                                result_rows[row_idx].values[proj_idx] = Datum::Int64(bucket);
                            }
                        }
                        WindowFunc::Lag(col_idx, offset) => {
                            for (i, &row_idx) in indices.iter().enumerate() {
                                let off = *offset as usize;
                                if i >= off {
                                    let prev_row_idx = indices[i - off];
                                    let val = source_rows[prev_row_idx]
                                        .get(*col_idx)
                                        .cloned()
                                        .unwrap_or(Datum::Null);
                                    result_rows[row_idx].values[proj_idx] = val;
                                } else {
                                    result_rows[row_idx].values[proj_idx] = Datum::Null;
                                }
                            }
                        }
                        WindowFunc::Lead(col_idx, offset) => {
                            for (i, &row_idx) in indices.iter().enumerate() {
                                let off = *offset as usize;
                                if i + off < indices.len() {
                                    let next_row_idx = indices[i + off];
                                    let val = source_rows[next_row_idx]
                                        .get(*col_idx)
                                        .cloned()
                                        .unwrap_or(Datum::Null);
                                    result_rows[row_idx].values[proj_idx] = val;
                                } else {
                                    result_rows[row_idx].values[proj_idx] = Datum::Null;
                                }
                            }
                        }
                        WindowFunc::PercentRank => {
                            let total = indices.len();
                            if total <= 1 {
                                for &row_idx in &indices {
                                    result_rows[row_idx].values[proj_idx] = Datum::Float64(0.0);
                                }
                            } else {
                                let mut rank = 1usize;
                                for (i, &row_idx) in indices.iter().enumerate() {
                                    if i > 0 {
                                        let prev_idx = indices[i - 1];
                                        let differs = wf.order_by.iter().any(|ob| {
                                            let a = source_rows[prev_idx]
                                                .get(ob.column_idx)
                                                .unwrap_or(&Datum::Null);
                                            let b = source_rows[row_idx]
                                                .get(ob.column_idx)
                                                .unwrap_or(&Datum::Null);
                                            a.cmp(b) != std::cmp::Ordering::Equal
                                        });
                                        if differs {
                                            rank = i + 1;
                                        }
                                    }
                                    let pr = (rank - 1) as f64 / (total - 1) as f64;
                                    result_rows[row_idx].values[proj_idx] = Datum::Float64(pr);
                                }
                            }
                        }
                        WindowFunc::CumeDist => {
                            let total = indices.len();
                            for (i, &row_idx) in indices.iter().enumerate() {
                                // CUME_DIST = number of rows <= current / total
                                let mut num_le = i + 1;
                                // Extend to include ties after current
                                for (j, &next_idx) in indices.iter().enumerate().skip(i + 1) {
                                    let same = wf.order_by.iter().all(|ob| {
                                        let a = source_rows[row_idx]
                                            .get(ob.column_idx)
                                            .unwrap_or(&Datum::Null);
                                        let b = source_rows[next_idx]
                                            .get(ob.column_idx)
                                            .unwrap_or(&Datum::Null);
                                        a.cmp(b) == std::cmp::Ordering::Equal
                                    });
                                    if same {
                                        num_le = j + 1;
                                    } else {
                                        break;
                                    }
                                }
                                let cd = num_le as f64 / total as f64;
                                result_rows[row_idx].values[proj_idx] = Datum::Float64(cd);
                            }
                        }
                        WindowFunc::NthValue(col_idx, n) => {
                            let nth_idx = (*n as usize).saturating_sub(1); // 1-indexed to 0-indexed
                            let val = if nth_idx < indices.len() {
                                let target_row_idx = indices[nth_idx];
                                source_rows[target_row_idx]
                                    .get(*col_idx)
                                    .cloned()
                                    .unwrap_or(Datum::Null)
                            } else {
                                Datum::Null
                            };
                            for &row_idx in &indices {
                                result_rows[row_idx].values[proj_idx] = val.clone();
                            }
                        }
                        WindowFunc::FirstValue(col_idx) => {
                            let first_row_idx = indices[0];
                            let val = source_rows[first_row_idx]
                                .get(*col_idx)
                                .cloned()
                                .unwrap_or(Datum::Null);
                            for &row_idx in &indices {
                                result_rows[row_idx].values[proj_idx] = val.clone();
                            }
                        }
                        WindowFunc::LastValue(col_idx) => {
                            let last_row_idx = *indices.last().unwrap();
                            let val = source_rows[last_row_idx]
                                .get(*col_idx)
                                .cloned()
                                .unwrap_or(Datum::Null);
                            for &row_idx in &indices {
                                result_rows[row_idx].values[proj_idx] = val.clone();
                            }
                        }
                        WindowFunc::Agg(agg_func, col_idx) => {
                            let agg_expr = col_idx.map(BoundExpr::ColumnRef);
                            if wf.frame.is_full_partition() {
                                // Entire partition — compute once
                                let partition_rows: Vec<&OwnedRow> =
                                    indices.iter().map(|&i| source_rows[i]).collect();
                                let val = self.compute_aggregate(
                                    agg_func,
                                    agg_expr.as_ref(),
                                    false,
                                    &partition_rows,
                                )?;
                                for &row_idx in &indices {
                                    result_rows[row_idx].values[proj_idx] = val.clone();
                                }
                            } else {
                                // Per-row frame computation
                                let n = indices.len();
                                for (pos, &row_idx) in indices.iter().enumerate() {
                                    let start = Self::resolve_frame_start(&wf.frame, pos, n);
                                    let end = Self::resolve_frame_end(&wf.frame, pos, n);
                                    let frame_rows: Vec<&OwnedRow> = (start..=end)
                                        .filter(|&j| j < n)
                                        .map(|j| source_rows[indices[j]])
                                        .collect();
                                    let val = self.compute_aggregate(
                                        agg_func,
                                        agg_expr.as_ref(),
                                        false,
                                        &frame_rows,
                                    )?;
                                    result_rows[row_idx].values[proj_idx] = val;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Resolve the starting row index for a window frame given the current position.
    fn resolve_frame_start(frame: &WindowFrame, pos: usize, _partition_len: usize) -> usize {
        match &frame.start {
            WindowFrameBound::UnboundedPreceding => 0,
            WindowFrameBound::CurrentRow => pos,
            WindowFrameBound::Preceding(n) => pos.saturating_sub(*n as usize),
            WindowFrameBound::Following(n) => pos + *n as usize,
            WindowFrameBound::UnboundedFollowing => _partition_len,
        }
    }

    /// Resolve the ending row index (inclusive) for a window frame given the current position.
    fn resolve_frame_end(frame: &WindowFrame, pos: usize, partition_len: usize) -> usize {
        match &frame.end {
            WindowFrameBound::UnboundedFollowing => partition_len.saturating_sub(1),
            WindowFrameBound::CurrentRow => pos,
            WindowFrameBound::Following(n) => (pos + *n as usize).min(partition_len.saturating_sub(1)),
            WindowFrameBound::Preceding(n) => pos.saturating_sub(*n as usize),
            WindowFrameBound::UnboundedPreceding => 0,
        }
    }
}
