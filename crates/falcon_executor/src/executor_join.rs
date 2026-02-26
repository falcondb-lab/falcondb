#![allow(clippy::too_many_arguments)]

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_common::schema::TableSchema;
use falcon_sql_frontend::types::*;
use falcon_txn::TxnHandle;

use crate::executor::{CteData, ExecutionResult, Executor};
use crate::expr_engine::ExprEngine;

impl Executor {
    pub(crate) fn exec_nested_loop_join(
        &self,
        left_table_id: falcon_common::types::TableId,
        _left_schema: &TableSchema,
        joins: &[BoundJoin],
        combined_schema: &TableSchema,
        projections: &[BoundProjection],
        visible_projection_count: usize,
        filter: Option<&BoundExpr>,
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: &DistinctMode,
        txn: &TxnHandle,
        cte_data: &CteData,
    ) -> Result<ExecutionResult, FalconError> {
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let mat_filter = self.materialize_filter(filter, txn)?;

        // Scan left table (check CTE data first)
        let left_data: Vec<OwnedRow> = if let Some(cte_rows) = cte_data.get(&left_table_id) {
            cte_rows.clone()
        } else {
            let left_rows = self.storage.scan(left_table_id, txn.txn_id, read_ts)?;
            left_rows.into_iter().map(|(_, row)| row).collect()
        };

        // Start with left rows as combined rows
        let mut combined_rows: Vec<OwnedRow> = left_data;

        // For each join, extend the combined rows
        for join in joins {
            let right_data: Vec<OwnedRow> =
                if let Some(cte_rows) = cte_data.get(&join.right_table_id) {
                    cte_rows.clone()
                } else {
                    let right_rows = self
                        .storage
                        .scan(join.right_table_id, txn.txn_id, read_ts)?;
                    right_rows.into_iter().map(|(_, row)| row).collect()
                };

            let mut new_combined = Vec::new();

            match join.join_type {
                JoinType::Inner => {
                    for left_row in &combined_rows {
                        for right_row in &right_data {
                            let merged = self.merge_rows(left_row, right_row);
                            if let Some(ref cond) = join.condition {
                                if !ExprEngine::eval_filter(cond, &merged)
                                    .map_err(FalconError::Execution)?
                                {
                                    continue;
                                }
                            }
                            new_combined.push(merged);
                        }
                    }
                }
                JoinType::Left => {
                    for left_row in &combined_rows {
                        let mut matched = false;
                        for right_row in &right_data {
                            let merged = self.merge_rows(left_row, right_row);
                            if let Some(ref cond) = join.condition {
                                if !ExprEngine::eval_filter(cond, &merged)
                                    .map_err(FalconError::Execution)?
                                {
                                    continue;
                                }
                            }
                            matched = true;
                            new_combined.push(merged);
                        }
                        if !matched {
                            // Emit left row with NULLs for right columns
                            let null_right =
                                OwnedRow::new(vec![Datum::Null; join.right_schema.columns.len()]);
                            new_combined.push(self.merge_rows(left_row, &null_right));
                        }
                    }
                }
                JoinType::Right => {
                    for right_row in &right_data {
                        let mut matched = false;
                        for left_row in &combined_rows {
                            let merged = self.merge_rows(left_row, right_row);
                            if let Some(ref cond) = join.condition {
                                if !ExprEngine::eval_filter(cond, &merged)
                                    .map_err(FalconError::Execution)?
                                {
                                    continue;
                                }
                            }
                            matched = true;
                            new_combined.push(merged);
                        }
                        if !matched {
                            let left_width =
                                combined_rows.first().map_or(0, |r| r.values.len());
                            let null_left = OwnedRow::new(vec![Datum::Null; left_width]);
                            new_combined.push(self.merge_rows(&null_left, right_row));
                        }
                    }
                }
                JoinType::FullOuter => {
                    let left_width = combined_rows.first().map_or(0, |r| r.values.len());
                    let mut right_matched = vec![false; right_data.len()];
                    for left_row in &combined_rows {
                        let mut left_matched = false;
                        for (ri, right_row) in right_data.iter().enumerate() {
                            let merged = self.merge_rows(left_row, right_row);
                            if let Some(ref cond) = join.condition {
                                if !ExprEngine::eval_filter(cond, &merged)
                                    .map_err(FalconError::Execution)?
                                {
                                    continue;
                                }
                            }
                            left_matched = true;
                            right_matched[ri] = true;
                            new_combined.push(merged);
                        }
                        if !left_matched {
                            let null_right =
                                OwnedRow::new(vec![Datum::Null; join.right_schema.columns.len()]);
                            new_combined.push(self.merge_rows(left_row, &null_right));
                        }
                    }
                    // Emit unmatched right rows with NULL left
                    for (ri, right_row) in right_data.iter().enumerate() {
                        if !right_matched[ri] {
                            let null_left = OwnedRow::new(vec![Datum::Null; left_width]);
                            new_combined.push(self.merge_rows(&null_left, right_row));
                        }
                    }
                }
                JoinType::Cross => {
                    for left_row in &combined_rows {
                        for right_row in &right_data {
                            new_combined.push(self.merge_rows(left_row, right_row));
                        }
                    }
                }
            }

            combined_rows = new_combined;
        }

        self.finish_join_pipeline(
            combined_rows, mat_filter, projections, visible_projection_count,
            combined_schema, distinct, order_by, limit, offset,
        )
    }

    /// Hash join: build a hash table on the right side of each join,
    /// then probe with left-side rows. O(n+m) for equi-joins.
    pub(crate) fn exec_hash_join(
        &self,
        left_table_id: falcon_common::types::TableId,
        _left_schema: &TableSchema,
        joins: &[BoundJoin],
        combined_schema: &TableSchema,
        projections: &[BoundProjection],
        visible_projection_count: usize,
        filter: Option<&BoundExpr>,
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: &DistinctMode,
        txn: &TxnHandle,
        cte_data: &CteData,
    ) -> Result<ExecutionResult, FalconError> {
        use std::collections::HashMap;

        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let mat_filter = self.materialize_filter(filter, txn)?;

        // Scan left table
        let left_data: Vec<OwnedRow> = if let Some(cte_rows) = cte_data.get(&left_table_id) {
            cte_rows.clone()
        } else {
            self.storage
                .scan(left_table_id, txn.txn_id, read_ts)?
                .into_iter()
                .map(|(_, row)| row)
                .collect()
        };

        let mut combined_rows: Vec<OwnedRow> = left_data;

        for join in joins {
            let right_data: Vec<OwnedRow> =
                if let Some(cte_rows) = cte_data.get(&join.right_table_id) {
                    cte_rows.clone()
                } else {
                    self.storage
                        .scan(join.right_table_id, txn.txn_id, read_ts)?
                        .into_iter()
                        .map(|(_, row)| row)
                        .collect()
                };

            // Extract equi-join key column indices from the condition
            let left_width = combined_rows.first().map_or(0, |r| r.values.len());
            let key_pairs = Self::extract_equi_key_pairs(join.condition.as_ref(), left_width);

            let mut new_combined = Vec::new();

            if key_pairs.is_empty() {
                // No equi-join keys extracted — fall back to nested loop for this join
                match join.join_type {
                    JoinType::Inner => {
                        for left_row in &combined_rows {
                            for right_row in &right_data {
                                let merged = self.merge_rows(left_row, right_row);
                                if let Some(ref cond) = join.condition {
                                    if !ExprEngine::eval_filter(cond, &merged)
                                        .map_err(FalconError::Execution)?
                                    {
                                        continue;
                                    }
                                }
                                new_combined.push(merged);
                            }
                        }
                    }
                    JoinType::Cross => {
                        for left_row in &combined_rows {
                            for right_row in &right_data {
                                new_combined.push(self.merge_rows(left_row, right_row));
                            }
                        }
                    }
                    JoinType::FullOuter => {
                        let mut right_matched = vec![false; right_data.len()];
                        for left_row in &combined_rows {
                            let mut left_matched = false;
                            for (ri, right_row) in right_data.iter().enumerate() {
                                let merged = self.merge_rows(left_row, right_row);
                                if let Some(ref cond) = join.condition {
                                    if !ExprEngine::eval_filter(cond, &merged)
                                        .map_err(FalconError::Execution)?
                                    {
                                        continue;
                                    }
                                }
                                left_matched = true;
                                right_matched[ri] = true;
                                new_combined.push(merged);
                            }
                            if !left_matched {
                                let null_right = OwnedRow::new(vec![
                                    Datum::Null;
                                    join.right_schema.columns.len()
                                ]);
                                new_combined.push(self.merge_rows(left_row, &null_right));
                            }
                        }
                        for (ri, right_row) in right_data.iter().enumerate() {
                            if !right_matched[ri] {
                                let null_left = OwnedRow::new(vec![Datum::Null; left_width]);
                                new_combined.push(self.merge_rows(&null_left, right_row));
                            }
                        }
                    }
                    _ => {
                        // LEFT/RIGHT with no equi keys — fall back to nested loop
                        for left_row in &combined_rows {
                            let mut matched = false;
                            for right_row in &right_data {
                                let merged = self.merge_rows(left_row, right_row);
                                if let Some(ref cond) = join.condition {
                                    if !ExprEngine::eval_filter(cond, &merged)
                                        .map_err(FalconError::Execution)?
                                    {
                                        continue;
                                    }
                                }
                                matched = true;
                                new_combined.push(merged);
                            }
                            if !matched && join.join_type == JoinType::Left {
                                let null_right = OwnedRow::new(vec![
                                    Datum::Null;
                                    join.right_schema.columns.len()
                                ]);
                                new_combined.push(self.merge_rows(left_row, &null_right));
                            }
                        }
                    }
                }
            } else {
                // Derive column-index slices for encode_join_key_into.
                // (col_idx, _unused_bool) — we reuse the same helper for both sides.
                let right_key_cols: Vec<(usize, bool)> =
                    key_pairs.iter().map(|(_, rc)| (*rc, false)).collect();
                let left_key_cols: Vec<(usize, bool)> =
                    key_pairs.iter().map(|(lc, _)| (*lc, false)).collect();

                // Build hash table on right side using byte-encoded keys.
                // One Vec<u8> per distinct right key — no Datum cloning.
                let mut hash_table: HashMap<Vec<u8>, Vec<usize>> =
                    HashMap::with_capacity(right_data.len());
                let mut key_buf = Vec::with_capacity(key_pairs.len() * 9);
                for (ri, right_row) in right_data.iter().enumerate() {
                    key_buf.clear();
                    Self::encode_join_key_into(right_row, &right_key_cols, &mut key_buf);
                    hash_table.entry(key_buf.clone()).or_default().push(ri);
                }

                // Probe buffer: reused across all left rows — zero alloc per probe.
                let mut probe_buf = Vec::with_capacity(key_pairs.len() * 9);

                match join.join_type {
                    JoinType::Inner => {
                        for left_row in &combined_rows {
                            probe_buf.clear();
                            Self::encode_join_key_into(left_row, &left_key_cols, &mut probe_buf);
                            if let Some(indices) = hash_table.get(probe_buf.as_slice()) {
                                for &ri in indices {
                                    let merged = self.merge_rows(left_row, &right_data[ri]);
                                    // Check full condition (may have non-equi parts via AND)
                                    if let Some(ref cond) = join.condition {
                                        if !ExprEngine::eval_filter(cond, &merged)
                                            .map_err(FalconError::Execution)?
                                        {
                                            continue;
                                        }
                                    }
                                    new_combined.push(merged);
                                }
                            }
                        }
                    }
                    JoinType::Left => {
                        for left_row in &combined_rows {
                            probe_buf.clear();
                            Self::encode_join_key_into(left_row, &left_key_cols, &mut probe_buf);
                            let mut matched = false;
                            if let Some(indices) = hash_table.get(probe_buf.as_slice()) {
                                for &ri in indices {
                                    let merged = self.merge_rows(left_row, &right_data[ri]);
                                    if let Some(ref cond) = join.condition {
                                        if !ExprEngine::eval_filter(cond, &merged)
                                            .map_err(FalconError::Execution)?
                                        {
                                            continue;
                                        }
                                    }
                                    matched = true;
                                    new_combined.push(merged);
                                }
                            }
                            if !matched {
                                let null_right = OwnedRow::new(vec![
                                    Datum::Null;
                                    join.right_schema.columns.len()
                                ]);
                                new_combined.push(self.merge_rows(left_row, &null_right));
                            }
                        }
                    }
                    JoinType::Right => {
                        // Build hash table on LEFT side for right join
                        let mut left_hash: HashMap<Vec<u8>, Vec<usize>> =
                            HashMap::with_capacity(combined_rows.len());
                        for (li, left_row) in combined_rows.iter().enumerate() {
                            key_buf.clear();
                            Self::encode_join_key_into(left_row, &left_key_cols, &mut key_buf);
                            left_hash.entry(key_buf.clone()).or_default().push(li);
                        }
                        for right_row in &right_data {
                            probe_buf.clear();
                            Self::encode_join_key_into(right_row, &right_key_cols, &mut probe_buf);
                            let mut matched = false;
                            if let Some(indices) = left_hash.get(probe_buf.as_slice()) {
                                for &li in indices {
                                    let merged = self.merge_rows(&combined_rows[li], right_row);
                                    if let Some(ref cond) = join.condition {
                                        if !ExprEngine::eval_filter(cond, &merged)
                                            .map_err(FalconError::Execution)?
                                        {
                                            continue;
                                        }
                                    }
                                    matched = true;
                                    new_combined.push(merged);
                                }
                            }
                            if !matched {
                                let null_left = OwnedRow::new(vec![Datum::Null; left_width]);
                                new_combined.push(self.merge_rows(&null_left, right_row));
                            }
                        }
                    }
                    JoinType::FullOuter => {
                        // Probe right hash table with left rows, track matched right rows
                        let mut right_matched = vec![false; right_data.len()];
                        for left_row in &combined_rows {
                            probe_buf.clear();
                            Self::encode_join_key_into(left_row, &left_key_cols, &mut probe_buf);
                            let mut left_matched = false;
                            if let Some(indices) = hash_table.get(probe_buf.as_slice()) {
                                for &ri in indices {
                                    let merged = self.merge_rows(left_row, &right_data[ri]);
                                    if let Some(ref cond) = join.condition {
                                        if !ExprEngine::eval_filter(cond, &merged)
                                            .map_err(FalconError::Execution)?
                                        {
                                            continue;
                                        }
                                    }
                                    left_matched = true;
                                    right_matched[ri] = true;
                                    new_combined.push(merged);
                                }
                            }
                            if !left_matched {
                                let null_right = OwnedRow::new(vec![
                                    Datum::Null;
                                    join.right_schema.columns.len()
                                ]);
                                new_combined.push(self.merge_rows(left_row, &null_right));
                            }
                        }
                        // Emit unmatched right rows with NULL left
                        for (ri, right_row) in right_data.iter().enumerate() {
                            if !right_matched[ri] {
                                let null_left = OwnedRow::new(vec![Datum::Null; left_width]);
                                new_combined.push(self.merge_rows(&null_left, right_row));
                            }
                        }
                    }
                    JoinType::Cross => {
                        for left_row in &combined_rows {
                            for right_row in &right_data {
                                new_combined.push(self.merge_rows(left_row, right_row));
                            }
                        }
                    }
                }
            }

            combined_rows = new_combined;
        }

        self.finish_join_pipeline(
            combined_rows, mat_filter, projections, visible_projection_count,
            combined_schema, distinct, order_by, limit, offset,
        )
    }

    /// Sort-merge join: sort both sides on join key, then merge.
    /// Falls back to hash join for non-equi or complex conditions.
    pub(crate) fn exec_merge_sort_join(
        &self,
        left_table_id: falcon_common::types::TableId,
        _left_schema: &TableSchema,
        joins: &[BoundJoin],
        combined_schema: &TableSchema,
        projections: &[BoundProjection],
        visible_projection_count: usize,
        filter: Option<&BoundExpr>,
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: &DistinctMode,
        txn: &TxnHandle,
        cte_data: &CteData,
    ) -> Result<ExecutionResult, FalconError> {
        let read_ts = txn.read_ts(self.txn_mgr.current_ts());
        let mat_filter = self.materialize_filter(filter, txn)?;

        // Scan left table
        let left_data: Vec<OwnedRow> = if let Some(cte_rows) = cte_data.get(&left_table_id) {
            cte_rows.clone()
        } else {
            self.storage
                .scan(left_table_id, txn.txn_id, read_ts)?
                .into_iter()
                .map(|(_, row)| row)
                .collect()
        };

        let mut combined_rows: Vec<OwnedRow> = left_data;

        for join in joins {
            let right_data: Vec<OwnedRow> =
                if let Some(cte_rows) = cte_data.get(&join.right_table_id) {
                    cte_rows.clone()
                } else {
                    self.storage
                        .scan(join.right_table_id, txn.txn_id, read_ts)?
                        .into_iter()
                        .map(|(_, row)| row)
                        .collect()
                };

            let left_width = combined_rows.first().map_or(0, |r| r.values.len());
            let key_pairs = Self::extract_equi_key_pairs(join.condition.as_ref(), left_width);

            let mut new_combined = Vec::new();

            if key_pairs.is_empty() || !matches!(join.join_type, JoinType::Inner) {
                // Fall back to nested loop for non-equi or non-inner joins
                match join.join_type {
                    JoinType::Inner | JoinType::Cross => {
                        for left_row in &combined_rows {
                            for right_row in &right_data {
                                let merged = self.merge_rows(left_row, right_row);
                                if let Some(ref cond) = join.condition {
                                    if !ExprEngine::eval_filter(cond, &merged)
                                        .map_err(FalconError::Execution)?
                                    {
                                        continue;
                                    }
                                }
                                new_combined.push(merged);
                            }
                        }
                    }
                    _ => {
                        for left_row in &combined_rows {
                            let mut matched = false;
                            for right_row in &right_data {
                                let merged = self.merge_rows(left_row, right_row);
                                if let Some(ref cond) = join.condition {
                                    if !ExprEngine::eval_filter(cond, &merged)
                                        .map_err(FalconError::Execution)?
                                    {
                                        continue;
                                    }
                                }
                                matched = true;
                                new_combined.push(merged);
                            }
                            if !matched
                                && matches!(join.join_type, JoinType::Left | JoinType::FullOuter)
                            {
                                let null_right = OwnedRow::new(vec![
                                    Datum::Null;
                                    join.right_schema.columns.len()
                                ]);
                                new_combined.push(self.merge_rows(left_row, &null_right));
                            }
                        }
                    }
                }
            } else {
                // Sort-merge join for equi INNER joins
                // Sort left side by join key columns
                let mut left_sorted: Vec<(Vec<Datum>, usize)> = combined_rows
                    .iter()
                    .enumerate()
                    .map(|(i, row)| {
                        let key: Vec<Datum> = key_pairs
                            .iter()
                            .map(|(lc, _)| row.get(*lc).cloned().unwrap_or(Datum::Null))
                            .collect();
                        (key, i)
                    })
                    .collect();
                left_sorted.sort_by(|a, b| Self::cmp_datum_keys(&a.0, &b.0));

                // Sort right side by join key columns
                let mut right_sorted: Vec<(Vec<Datum>, usize)> = right_data
                    .iter()
                    .enumerate()
                    .map(|(i, row)| {
                        let key: Vec<Datum> = key_pairs
                            .iter()
                            .map(|(_, rc)| row.get(*rc).cloned().unwrap_or(Datum::Null))
                            .collect();
                        (key, i)
                    })
                    .collect();
                right_sorted.sort_by(|a, b| Self::cmp_datum_keys(&a.0, &b.0));

                // Merge pass
                let mut li = 0;
                let mut ri = 0;
                while li < left_sorted.len() && ri < right_sorted.len() {
                    let cmp = Self::cmp_datum_keys(&left_sorted[li].0, &right_sorted[ri].0);
                    match cmp {
                        std::cmp::Ordering::Less => {
                            li += 1;
                        }
                        std::cmp::Ordering::Greater => {
                            ri += 1;
                        }
                        std::cmp::Ordering::Equal => {
                            // Find all left rows with this key
                            let left_key = left_sorted[li].0.clone();
                            let mut left_group_end = li;
                            while left_group_end < left_sorted.len()
                                && Self::cmp_datum_keys(&left_sorted[left_group_end].0, &left_key)
                                    == std::cmp::Ordering::Equal
                            {
                                left_group_end += 1;
                            }
                            // Find all right rows with this key
                            let right_key = right_sorted[ri].0.clone();
                            let mut right_group_end = ri;
                            while right_group_end < right_sorted.len()
                                && Self::cmp_datum_keys(
                                    &right_sorted[right_group_end].0,
                                    &right_key,
                                ) == std::cmp::Ordering::Equal
                            {
                                right_group_end += 1;
                            }
                            // Cross-product of matching groups
                            for l in li..left_group_end {
                                for r in ri..right_group_end {
                                    let merged = self.merge_rows(
                                        &combined_rows[left_sorted[l].1],
                                        &right_data[right_sorted[r].1],
                                    );
                                    if let Some(ref cond) = join.condition {
                                        if !ExprEngine::eval_filter(cond, &merged)
                                            .map_err(FalconError::Execution)?
                                        {
                                            continue;
                                        }
                                    }
                                    new_combined.push(merged);
                                }
                            }
                            li = left_group_end;
                            ri = right_group_end;
                        }
                    }
                }
            }

            combined_rows = new_combined;
        }

        self.finish_join_pipeline(
            combined_rows, mat_filter, projections, visible_projection_count,
            combined_schema, distinct, order_by, limit, offset,
        )
    }

    /// Shared post-join pipeline: filter → project → window → distinct → sort → offset/limit → strip.
    /// Called by all three join strategies (nested-loop, hash, merge-sort) to avoid duplication.
    #[allow(clippy::too_many_arguments)]
    fn finish_join_pipeline(
        &self,
        combined_rows: Vec<OwnedRow>,
        mat_filter: Option<BoundExpr>,
        projections: &[BoundProjection],
        visible_projection_count: usize,
        combined_schema: &falcon_common::schema::TableSchema,
        distinct: &DistinctMode,
        order_by: &[BoundOrderBy],
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<ExecutionResult, FalconError> {
        // Apply WHERE filter
        let mut filtered: Vec<OwnedRow> = Vec::with_capacity(combined_rows.len());
        for row in combined_rows {
            if let Some(ref f) = mat_filter {
                if !ExprEngine::eval_filter(f, &row).map_err(FalconError::Execution)? {
                    continue;
                }
            }
            filtered.push(row);
        }

        let has_window = projections
            .iter()
            .any(|p| matches!(p, BoundProjection::Window(..)));

        let mut columns = self.resolve_output_columns(projections, combined_schema);
        let filtered_refs: Vec<&OwnedRow> = filtered.iter().collect();
        let mut result_rows: Vec<OwnedRow> = Vec::with_capacity(filtered.len());
        for row in &filtered {
            result_rows.push(self.project_row(row, projections)?);
        }

        if has_window {
            self.compute_window_functions(&filtered_refs, projections, &mut result_rows)?;
        }

        self.apply_distinct(distinct, &mut result_rows);

        crate::external_sort::sort_rows(&mut result_rows, order_by, self.external_sorter.as_ref())?;

        if let Some(off) = offset {
            if off < result_rows.len() {
                result_rows = result_rows.split_off(off);
            } else {
                result_rows.clear();
            }
        }
        if let Some(lim) = limit {
            result_rows.truncate(lim);
        }

        if visible_projection_count < projections.len() {
            for row in &mut result_rows {
                row.values.truncate(visible_projection_count);
            }
            columns.truncate(visible_projection_count);
        }

        Ok(ExecutionResult::Query {
            columns,
            rows: result_rows,
        })
    }

    /// Compare two datum key vectors for sort-merge join ordering.
    fn cmp_datum_keys(a: &[Datum], b: &[Datum]) -> std::cmp::Ordering {
        for (av, bv) in a.iter().zip(b.iter()) {
            let ord = Self::cmp_datum(av, bv);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    }

    /// Compare two Datum values for ordering. NULL sorts last.
    fn cmp_datum(a: &Datum, b: &Datum) -> std::cmp::Ordering {
        match (a, b) {
            (Datum::Null, Datum::Null) => std::cmp::Ordering::Equal,
            (Datum::Null, _) => std::cmp::Ordering::Greater,
            (_, Datum::Null) => std::cmp::Ordering::Less,
            (Datum::Int32(x), Datum::Int32(y)) => x.cmp(y),
            (Datum::Int64(x), Datum::Int64(y)) => x.cmp(y),
            (Datum::Float64(x), Datum::Float64(y)) => {
                x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Datum::Text(x), Datum::Text(y)) => x.cmp(y),
            (Datum::Timestamp(x), Datum::Timestamp(y)) => x.cmp(y),
            (Datum::Date(x), Datum::Date(y)) => x.cmp(y),
            (Datum::Boolean(x), Datum::Boolean(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        }
    }

    /// Extract (left_col_idx, right_col_idx_relative_to_right_table) pairs
    /// from an equi-join condition like `col_a = col_b` or `a = b AND c = d`.
    /// `left_width` is the number of columns in the current left/combined row;
    /// columns >= left_width belong to the right table.
    fn extract_equi_key_pairs(
        condition: Option<&BoundExpr>,
        left_width: usize,
    ) -> Vec<(usize, usize)> {
        let mut pairs = Vec::new();
        if let Some(expr) = condition {
            Self::collect_equi_pairs(expr, left_width, &mut pairs);
        }
        pairs
    }

    fn collect_equi_pairs(expr: &BoundExpr, left_width: usize, pairs: &mut Vec<(usize, usize)>) {
        match expr {
            BoundExpr::BinaryOp {
                op: BinOp::Eq,
                left,
                right,
            } => {
                if let (BoundExpr::ColumnRef(l), BoundExpr::ColumnRef(r)) =
                    (left.as_ref(), right.as_ref())
                {
                    if *l < left_width && *r >= left_width {
                        pairs.push((*l, *r - left_width));
                    } else if *r < left_width && *l >= left_width {
                        pairs.push((*r, *l - left_width));
                    }
                }
            }
            BoundExpr::BinaryOp {
                op: BinOp::And,
                left,
                right,
            } => {
                Self::collect_equi_pairs(left, left_width, pairs);
                Self::collect_equi_pairs(right, left_width, pairs);
            }
            _ => {}
        }
    }

    /// Encode join key columns from `row` into `buf` (appending, not clearing).
    /// Uses a compact binary format: tag byte + fixed-width payload per datum variant.
    /// This allows the caller to `clear()` and reuse `buf` across rows, achieving
    /// zero heap allocations on the probe side of a hash join.
    #[inline]
    fn encode_join_key_into(row: &OwnedRow, col_indices: &[(usize, bool)], buf: &mut Vec<u8>) {
        for &(col, _) in col_indices {
            match row.get(col).unwrap_or(&Datum::Null) {
                Datum::Null => buf.push(0x00),
                Datum::Boolean(v) => {
                    buf.push(0x01);
                    buf.push(*v as u8);
                }
                Datum::Int32(v) => {
                    buf.push(0x02);
                    buf.extend_from_slice(&v.to_be_bytes());
                }
                Datum::Int64(v) => {
                    buf.push(0x03);
                    buf.extend_from_slice(&v.to_be_bytes());
                }
                Datum::Float64(v) => {
                    buf.push(0x04);
                    // Normalize -0.0 → 0.0 for consistent hashing
                    let bits = if *v == 0.0 { 0u64 } else { v.to_bits() };
                    buf.extend_from_slice(&bits.to_be_bytes());
                }
                Datum::Text(s) => {
                    buf.push(0x05);
                    buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                Datum::Timestamp(v) => {
                    buf.push(0x06);
                    buf.extend_from_slice(&v.to_be_bytes());
                }
                Datum::Date(v) => {
                    buf.push(0x07);
                    buf.extend_from_slice(&v.to_be_bytes());
                }
                Datum::Time(v) => {
                    buf.push(0x08);
                    buf.extend_from_slice(&v.to_be_bytes());
                }
                Datum::Decimal(m, s) => {
                    buf.push(0x09);
                    buf.extend_from_slice(&m.to_be_bytes());
                    buf.push(*s);
                }
                Datum::Uuid(v) => {
                    buf.push(0x0A);
                    buf.extend_from_slice(&v.to_be_bytes());
                }
                Datum::Bytea(b) => {
                    buf.push(0x0B);
                    buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
                    buf.extend_from_slice(b);
                }
                Datum::Interval(months, days, us) => {
                    buf.push(0x0D);
                    buf.extend_from_slice(&months.to_be_bytes());
                    buf.extend_from_slice(&days.to_be_bytes());
                    buf.extend_from_slice(&us.to_be_bytes());
                }
                Datum::Array(_) | Datum::Jsonb(_) => {
                    // Rare in join keys: fall back to debug repr for correctness
                    buf.push(0x0C);
                    let s = format!("{:?}", row.get(col).unwrap_or(&Datum::Null));
                    buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
            }
        }
    }
}
