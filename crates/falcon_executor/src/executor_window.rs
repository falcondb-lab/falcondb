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
                let mut partitions: std::collections::HashMap<Box<[u8]>, Vec<usize>> =
                    std::collections::HashMap::with_capacity(source_rows.len().min(256));
                let mut buf = Vec::with_capacity(64);
                for (i, row) in source_rows.iter().enumerate() {
                    crate::executor_aggregate::encode_group_key(&mut buf, row, &wf.partition_by);
                    // Use a two-step lookup to avoid cloning buf when the key already exists
                    if let Some(v) = partitions.get_mut(buf.as_slice()) {
                        v.push(i);
                    } else {
                        partitions.insert(buf.clone().into_boxed_slice(), vec![i]);
                    }
                }

                // For each partition, sort by window ORDER BY and compute values
                for (_key, mut indices) in partitions {
                    // Sort partition rows by ORDER BY
                    if !wf.order_by.is_empty() {
                        indices.sort_unstable_by(|&a, &b| {
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
                                result_rows[row_idx].values[proj_idx] =
                                    Datum::Int64((rank + 1) as i64);
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
                            let last_row_idx = match indices.last() {
                                Some(idx) => *idx,
                                None => continue, // empty partition — skip
                            };
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
                                // Entire partition — compute once, reuse buffer
                                let mut partition_rows: Vec<&OwnedRow> =
                                    Vec::with_capacity(indices.len());
                                partition_rows.extend(indices.iter().map(|&i| source_rows[i]));
                                let val = self.compute_aggregate(
                                    agg_func,
                                    agg_expr.as_ref(),
                                    false,
                                    &partition_rows,
                                )?;
                                for &row_idx in &indices {
                                    result_rows[row_idx].values[proj_idx] = val.clone();
                                }
                            } else if let Some(ci) = col_idx {
                                // Try sliding window O(N) fast path for SUM/COUNT/AVG
                                // on numeric ColumnRef expressions.
                                let use_sliding = matches!(
                                    agg_func,
                                    falcon_sql_frontend::types::AggFunc::Sum
                                        | falcon_sql_frontend::types::AggFunc::Count
                                        | falcon_sql_frontend::types::AggFunc::Avg
                                );
                                if use_sliding {
                                    Self::sliding_window_agg(
                                        source_rows,
                                        &indices,
                                        *ci,
                                        agg_func,
                                        &wf.frame,
                                        result_rows,
                                        proj_idx,
                                    );
                                } else {
                                    // Fallback: per-row recompute with reusable buffer
                                    let n = indices.len();
                                    let mut frame_rows: Vec<&OwnedRow> = Vec::with_capacity(n);
                                    for (pos, &row_idx) in indices.iter().enumerate() {
                                        let start = Self::resolve_frame_start(&wf.frame, pos, n);
                                        let end = Self::resolve_frame_end(&wf.frame, pos, n);
                                        frame_rows.clear();
                                        frame_rows.extend(
                                            (start..=end)
                                                .filter(|&j| j < n)
                                                .map(|j| source_rows[indices[j]]),
                                        );
                                        let val = self.compute_aggregate(
                                            agg_func,
                                            agg_expr.as_ref(),
                                            false,
                                            &frame_rows,
                                        )?;
                                        result_rows[row_idx].values[proj_idx] = val;
                                    }
                                }
                            } else {
                                // No column index (e.g. COUNT(*)) — per-row recompute
                                let n = indices.len();
                                let mut frame_rows: Vec<&OwnedRow> = Vec::with_capacity(n);
                                for (pos, &row_idx) in indices.iter().enumerate() {
                                    let start = Self::resolve_frame_start(&wf.frame, pos, n);
                                    let end = Self::resolve_frame_end(&wf.frame, pos, n);
                                    frame_rows.clear();
                                    frame_rows.extend(
                                        (start..=end)
                                            .filter(|&j| j < n)
                                            .map(|j| source_rows[indices[j]]),
                                    );
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

    /// O(N) sliding window aggregation for SUM/COUNT/AVG on a numeric column.
    /// Instead of recomputing the aggregate from scratch for each row's frame,
    /// we maintain a running (sum, count) and add entering / subtract leaving values.
    fn sliding_window_agg(
        source_rows: &[&OwnedRow],
        indices: &[usize],
        col_idx: usize,
        agg_func: &falcon_sql_frontend::types::AggFunc,
        frame: &WindowFrame,
        result_rows: &mut [OwnedRow],
        proj_idx: usize,
    ) {
        use falcon_sql_frontend::types::AggFunc;

        let n = indices.len();
        if n == 0 {
            return;
        }

        // Extract f64 values (None for NULL) — single pass over the partition.
        let vals: Vec<Option<f64>> = indices
            .iter()
            .map(|&i| source_rows[i].get(col_idx).and_then(|d| d.as_f64()))
            .collect();

        let mut sum: f64 = 0.0;
        let mut count: i64 = 0;
        let mut prev_start: usize = 0;
        let mut prev_end_plus1: usize = 0; // exclusive end of the previous frame

        for pos in 0..n {
            let start = Self::resolve_frame_start(frame, pos, n).min(n);
            let end = Self::resolve_frame_end(frame, pos, n).min(n.saturating_sub(1));
            let end_plus1 = end + 1; // exclusive

            if pos == 0 {
                // Initial frame: compute from scratch
                for j in start..end_plus1 {
                    if let Some(v) = vals[j] {
                        sum += v;
                        count += 1;
                    }
                }
            } else {
                // Subtract values that left the frame (prev_start..start)
                for j in prev_start..start.min(prev_end_plus1) {
                    if let Some(v) = vals[j] {
                        sum -= v;
                        count -= 1;
                    }
                }
                // Add values that entered the frame (prev_end_plus1..end_plus1)
                for j in prev_end_plus1.max(start)..end_plus1 {
                    if let Some(v) = vals[j] {
                        sum += v;
                        count += 1;
                    }
                }
            }

            prev_start = start;
            prev_end_plus1 = end_plus1;

            let row_idx = indices[pos];
            result_rows[row_idx].values[proj_idx] = if count == 0 {
                Datum::Null
            } else {
                match agg_func {
                    AggFunc::Sum => Datum::Float64(sum),
                    AggFunc::Count => Datum::Int64(count),
                    AggFunc::Avg => Datum::Float64(sum / count as f64),
                    _ => Datum::Null,
                }
            };
        }
    }

    /// Resolve the starting row index for a window frame given the current position.
    const fn resolve_frame_start(frame: &WindowFrame, pos: usize, _partition_len: usize) -> usize {
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
            WindowFrameBound::Following(n) => {
                (pos + *n as usize).min(partition_len.saturating_sub(1))
            }
            WindowFrameBound::Preceding(n) => pos.saturating_sub(*n as usize),
            WindowFrameBound::UnboundedPreceding => 0,
        }
    }
}
