use std::collections::HashMap;

use falcon_common::datum::Datum;
use falcon_common::types::TableId;
use falcon_sql_frontend::types::*;

/// Planner-level table row counts for cost estimation.
/// Populated from `StorageEngine::get_table_stats()` / `row_count_approx()`.
pub type TableRowCounts = HashMap<TableId, u64>;

/// Planner-level index metadata: maps table_id → list of indexed column indices.
/// Populated from `StorageEngine::get_indexed_columns()`.
pub type IndexedColumns = HashMap<TableId, Vec<usize>>;

// ── Planner-level statistics interface ────────────────────────────────────

/// Per-column statistics summary passed from storage to the planner.
#[derive(Debug, Clone)]
pub struct ColumnStatsInfo {
    pub column_idx: usize,
    pub distinct_count: u64,
    pub null_fraction: f64,
    pub min_value: Option<Datum>,
    pub max_value: Option<Datum>,
    /// MCV: (value, frequency) pairs sorted by frequency desc.
    pub mcv: Vec<(Datum, f64)>,
    /// Equi-depth histogram bucket upper bounds.
    pub histogram_bounds: Vec<Datum>,
    pub histogram_rows: u64,
}

/// Per-table statistics for cost-based planning.
#[derive(Debug, Clone)]
pub struct TableStatsInfo {
    pub table_id: TableId,
    pub row_count: u64,
    pub columns: Vec<ColumnStatsInfo>,
}

impl TableStatsInfo {
    pub fn column(&self, idx: usize) -> Option<&ColumnStatsInfo> {
        self.columns.iter().find(|c| c.column_idx == idx)
    }
}

/// Map from TableId to full table statistics.
pub type TableStatsMap = HashMap<TableId, TableStatsInfo>;

// ── Selectivity estimation ────────────────────────────────────────────────

/// Default selectivity when no stats available.
const DEFAULT_EQ_SEL: f64 = 0.1;
const DEFAULT_RANGE_SEL: f64 = 0.33;
const DEFAULT_LIKE_SEL: f64 = 0.05;
const DEFAULT_IS_NULL_SEL: f64 = 0.01;

/// Estimate selectivity of a filter expression given table stats.
/// Returns a value in [0.0, 1.0] representing the fraction of rows passing.
pub fn estimate_selectivity(expr: &BoundExpr, stats: Option<&TableStatsInfo>) -> f64 {
    match expr {
        BoundExpr::BinaryOp { left, op, right } => {
            match op {
                BinOp::And => {
                    let l = estimate_selectivity(left, stats);
                    let r = estimate_selectivity(right, stats);
                    l * r // independence assumption
                }
                BinOp::Or => {
                    let l = estimate_selectivity(left, stats);
                    let r = estimate_selectivity(right, stats);
                    (l + r - l * r).min(1.0)
                }
                BinOp::Eq => estimate_comparison_sel(left, right, *op, stats),
                BinOp::NotEq => 1.0 - estimate_comparison_sel(left, right, BinOp::Eq, stats),
                BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq => {
                    estimate_comparison_sel(left, right, *op, stats)
                }
                _ => 0.5,
            }
        }
        BoundExpr::Not(inner) => 1.0 - estimate_selectivity(inner, stats),
        BoundExpr::IsNull(_) => DEFAULT_IS_NULL_SEL,
        BoundExpr::IsNotNull(_) => 1.0 - DEFAULT_IS_NULL_SEL,
        BoundExpr::Between { expr, low, high, negated } => {
            let sel = estimate_between_sel(expr, low, high, stats);
            if *negated { 1.0 - sel } else { sel }
        }
        BoundExpr::InList { expr, list, negated } => {
            let sel = estimate_in_list_sel(expr, list, stats);
            if *negated { 1.0 - sel } else { sel }
        }
        BoundExpr::Like { .. } => DEFAULT_LIKE_SEL,
        BoundExpr::Literal(Datum::Boolean(true)) => 1.0,
        BoundExpr::Literal(Datum::Boolean(false)) => 0.0,
        _ => 0.5,
    }
}

fn estimate_comparison_sel(
    left: &BoundExpr,
    right: &BoundExpr,
    op: BinOp,
    stats: Option<&TableStatsInfo>,
) -> f64 {
    let stats = match stats {
        Some(s) => s,
        None => return if op == BinOp::Eq { DEFAULT_EQ_SEL } else { DEFAULT_RANGE_SEL },
    };

    // col = literal or literal = col
    if let (Some(col_idx), Some(lit)) = (extract_column_ref(left), extract_literal(right)) {
        return col_literal_sel(col_idx, &lit, op, stats);
    }
    if let (Some(lit), Some(col_idx)) = (extract_literal(left), extract_column_ref(right)) {
        let flipped = match op {
            BinOp::Lt => BinOp::Gt,
            BinOp::LtEq => BinOp::GtEq,
            BinOp::Gt => BinOp::Lt,
            BinOp::GtEq => BinOp::LtEq,
            other => other,
        };
        return col_literal_sel(col_idx, &lit, flipped, stats);
    }

    // col = col (join selectivity approximation)
    if let (Some(l_col), Some(r_col)) = (extract_column_ref(left), extract_column_ref(right)) {
        if op == BinOp::Eq {
            let l_ndv = stats.column(l_col).map_or(100, |c| c.distinct_count.max(1));
            let r_ndv = stats.column(r_col).map_or(100, |c| c.distinct_count.max(1));
            return 1.0 / l_ndv.max(r_ndv) as f64;
        }
    }

    if op == BinOp::Eq { DEFAULT_EQ_SEL } else { DEFAULT_RANGE_SEL }
}

fn col_literal_sel(col_idx: usize, value: &Datum, op: BinOp, stats: &TableStatsInfo) -> f64 {
    let cs = match stats.column(col_idx) {
        Some(c) => c,
        None => return if op == BinOp::Eq { DEFAULT_EQ_SEL } else { DEFAULT_RANGE_SEL },
    };

    match op {
        BinOp::Eq => {
            // Check MCV first
            for (v, freq) in &cs.mcv {
                if datum_eq_loose(v, value) {
                    return *freq;
                }
            }
            // Histogram-based
            if !cs.histogram_bounds.is_empty() && cs.histogram_rows > 0 {
                return histogram_eq_sel(&cs.histogram_bounds, cs.histogram_rows, value);
            }
            // 1/NDV
            if cs.distinct_count > 0 {
                return 1.0 / cs.distinct_count as f64;
            }
            DEFAULT_EQ_SEL
        }
        BinOp::Lt | BinOp::LtEq => {
            if !cs.histogram_bounds.is_empty() && cs.histogram_rows > 0 {
                let sel = histogram_lt_sel(&cs.histogram_bounds, value);
                if op == BinOp::LtEq {
                    return (sel + histogram_eq_sel(&cs.histogram_bounds, cs.histogram_rows, value)).min(1.0);
                }
                return sel;
            }
            DEFAULT_RANGE_SEL
        }
        BinOp::Gt | BinOp::GtEq => {
            if !cs.histogram_bounds.is_empty() && cs.histogram_rows > 0 {
                let lt_sel = histogram_lt_sel(&cs.histogram_bounds, value);
                let eq_sel = histogram_eq_sel(&cs.histogram_bounds, cs.histogram_rows, value);
                if op == BinOp::GtEq {
                    return (1.0 - lt_sel).max(0.0);
                }
                return (1.0 - lt_sel - eq_sel).max(0.0);
            }
            DEFAULT_RANGE_SEL
        }
        _ => DEFAULT_RANGE_SEL,
    }
}

fn estimate_between_sel(
    expr: &BoundExpr,
    low: &BoundExpr,
    high: &BoundExpr,
    stats: Option<&TableStatsInfo>,
) -> f64 {
    let sel_ge = estimate_comparison_sel(expr, low, BinOp::GtEq, stats);
    let sel_le = estimate_comparison_sel(expr, high, BinOp::LtEq, stats);
    (sel_ge * sel_le).max(0.001) // assume some correlation
}

fn estimate_in_list_sel(
    expr: &BoundExpr,
    list: &[BoundExpr],
    stats: Option<&TableStatsInfo>,
) -> f64 {
    if list.is_empty() {
        return 0.0;
    }
    // Each element contributes eq selectivity, combined with OR semantics
    let mut total = 0.0;
    for item in list {
        let s = estimate_comparison_sel(expr, item, BinOp::Eq, stats);
        total = total + s - total * s;
    }
    total.min(1.0)
}

fn extract_column_ref(expr: &BoundExpr) -> Option<usize> {
    match expr {
        BoundExpr::ColumnRef(idx) => Some(*idx),
        BoundExpr::Cast { expr: inner, .. } => extract_column_ref(inner),
        _ => None,
    }
}

fn extract_literal(expr: &BoundExpr) -> Option<Datum> {
    match expr {
        BoundExpr::Literal(d) => Some(d.clone()),
        _ => None,
    }
}

fn histogram_lt_sel(bounds: &[Datum], value: &Datum) -> f64 {
    if bounds.is_empty() {
        return DEFAULT_RANGE_SEL;
    }
    let n = bounds.len();
    let mut pos = 0;
    for (i, b) in bounds.iter().enumerate() {
        if datum_cmp_loose(value, b) != std::cmp::Ordering::Greater {
            pos = i;
            break;
        }
        pos = i + 1;
    }
    let bucket_frac = 1.0 / n as f64;
    let base = pos as f64 * bucket_frac;
    if pos < n {
        (base + 0.5 * bucket_frac).min(1.0)
    } else {
        1.0
    }
}

fn histogram_eq_sel(bounds: &[Datum], total_rows: u64, _value: &Datum) -> f64 {
    if bounds.is_empty() || total_rows == 0 {
        return DEFAULT_EQ_SEL;
    }
    let rows_per_bucket = total_rows as f64 / bounds.len() as f64;
    // Approximate: 1/distinct_per_bucket ≈ 1/rows_per_bucket
    (1.0 / rows_per_bucket.max(1.0)).min(0.5)
}

fn datum_eq_loose(a: &Datum, b: &Datum) -> bool {
    datum_cmp_loose(a, b) == std::cmp::Ordering::Equal
}

fn datum_cmp_loose(a: &Datum, b: &Datum) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Datum::Int32(x), Datum::Int32(y)) => x.cmp(y),
        (Datum::Int64(x), Datum::Int64(y)) => x.cmp(y),
        (Datum::Float64(x), Datum::Float64(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Datum::Text(x), Datum::Text(y)) => x.cmp(y),
        (Datum::Timestamp(x), Datum::Timestamp(y)) => x.cmp(y),
        (Datum::Date(x), Datum::Date(y)) => x.cmp(y),
        (Datum::Boolean(x), Datum::Boolean(y)) => x.cmp(y),
        _ => Ordering::Equal,
    }
}

// ── Scan cost model ───────────────────────────────────────────────────────

/// Cost constants (relative units, not absolute time).
const SEQ_SCAN_PAGE_COST: f64 = 1.0;
const RANDOM_PAGE_COST: f64 = 4.0;
const CPU_TUPLE_COST: f64 = 0.01;
const CPU_INDEX_TUPLE_COST: f64 = 0.005;
const CPU_OPERATOR_COST: f64 = 0.0025;

/// Estimated cost of a sequential scan.
pub fn seq_scan_cost(row_count: u64, filter_sel: f64) -> f64 {
    let pages = (row_count as f64 / 100.0).max(1.0); // ~100 rows/page
    let io = pages * SEQ_SCAN_PAGE_COST;
    let cpu = row_count as f64 * (CPU_TUPLE_COST + CPU_OPERATOR_COST);
    io + cpu + (row_count as f64 * filter_sel * CPU_TUPLE_COST) // output cost
}

/// Estimated cost of an index scan.
pub fn index_scan_cost(row_count: u64, selectivity: f64) -> f64 {
    let matching = (row_count as f64 * selectivity).max(1.0);
    let total_pages = (row_count as f64 / 100.0).max(1.0);
    // B-tree traversal: ~log2(total_pages) random reads
    let tree_depth = (total_pages.ln() / 2.0_f64.ln()).ceil().max(1.0);
    // Heap fetches: assume partial correlation — sqrt(matching * total_pages) / total_pages
    // gives a mix between fully correlated (selectivity * pages) and uncorrelated (matching pages)
    let heap_pages = (selectivity * total_pages).max(1.0);
    let io = tree_depth * RANDOM_PAGE_COST + heap_pages * RANDOM_PAGE_COST;
    let cpu = matching * (CPU_INDEX_TUPLE_COST + CPU_TUPLE_COST + CPU_OPERATOR_COST);
    io + cpu
}

/// Should we prefer index scan over seq scan for this selectivity?
pub fn prefer_index_scan(row_count: u64, selectivity: f64) -> bool {
    if row_count < 50 {
        return false; // tiny tables always seq scan
    }
    index_scan_cost(row_count, selectivity) < seq_scan_cost(row_count, selectivity)
}

/// Reorder INNER joins in a query to minimise hash-table build cost.
///
/// **Strategy** (greedy, smallest-build-side first):
/// 1. Only reorder joins that are INNER and whose condition is
///    *self-contained* (references only the original left-table columns
///    and the join's own right-table columns).
/// 2. Sort those reorderable joins by ascending right-table row count
///    so the smallest hash table is built first.
/// 3. After sorting, remap `right_col_offset` values and adjust
///    `ColumnRef` indices in conditions so they match the new layout.
///
/// LEFT / RIGHT / CROSS joins and joins with cross-referencing
/// conditions are left in their original positions.
pub fn reorder_joins(
    left_table_col_count: usize,
    joins: &[BoundJoin],
    stats: &TableRowCounts,
) -> Vec<BoundJoin> {
    if joins.len() <= 1 {
        return joins.to_vec();
    }

    let left_cols = left_table_col_count;

    // ── 1. Classify each join as reorderable or fixed ───────────────
    // A join is reorderable iff:
    //   (a) it is INNER, and
    //   (b) its condition only references columns in [0, left_cols)
    //       (original left table) and [own_offset, own_offset+own_cols)
    //       (its own right table).
    let mut reorderable: Vec<usize> = Vec::new(); // indices into `joins`
    let mut fixed: Vec<usize> = Vec::new();

    for (i, j) in joins.iter().enumerate() {
        if j.join_type == JoinType::Inner && is_self_contained(j, left_cols) {
            reorderable.push(i);
        } else {
            fixed.push(i);
        }
    }

    // Nothing to reorder
    if reorderable.len() <= 1 {
        return joins.to_vec();
    }

    // ── 2. Sort reorderable joins by right-table row count ──────────
    reorderable.sort_by_key(|&i| {
        stats
            .get(&joins[i].right_table_id)
            .copied()
            .unwrap_or(u64::MAX)
    });

    // ── 3. Build the new join order ─────────────────────────────────
    // Interleave: reorderable joins come first, then fixed joins in
    // their original relative order. This is correct because:
    //   - reorderable joins are INNER (commutative + associative)
    //   - fixed joins keep their original relative order
    let mut new_order: Vec<usize> = Vec::with_capacity(joins.len());
    new_order.extend_from_slice(&reorderable);
    new_order.extend_from_slice(&fixed);

    // ── 4. Remap offsets and condition column references ─────────────
    let mut result: Vec<BoundJoin> = Vec::with_capacity(joins.len());
    let mut current_offset = left_cols;

    for &orig_idx in &new_order {
        let orig = &joins[orig_idx];
        let orig_offset = orig.right_col_offset;
        let right_cols = orig.right_schema.columns.len();
        let new_offset = current_offset;

        let new_condition = if orig_offset != new_offset {
            orig.condition
                .as_ref()
                .map(|c| remap_column_refs(c, orig_offset, right_cols, new_offset))
        } else {
            orig.condition.clone()
        };

        result.push(BoundJoin {
            join_type: orig.join_type,
            right_table_id: orig.right_table_id,
            right_table_name: orig.right_table_name.clone(),
            right_schema: orig.right_schema.clone(),
            right_col_offset: new_offset,
            condition: new_condition,
        });

        current_offset += right_cols;
    }

    result
}

/// Check whether a join condition only references columns in the
/// original left table (`[0, left_cols)`) and its own right table
/// (`[right_col_offset, right_col_offset + right_cols)`).
fn is_self_contained(join: &BoundJoin, left_cols: usize) -> bool {
    join.condition.as_ref().is_none_or(|expr| {
        let right_start = join.right_col_offset;
        let right_end = right_start + join.right_schema.columns.len();
        all_refs_in_ranges(expr, left_cols, right_start, right_end)
    })
}

/// Return true iff every `ColumnRef(idx)` in the expression satisfies
/// `idx < left_cols || (right_start <= idx && idx < right_end)`.
fn all_refs_in_ranges(
    expr: &BoundExpr,
    left_cols: usize,
    right_start: usize,
    right_end: usize,
) -> bool {
    match expr {
        BoundExpr::ColumnRef(idx) => *idx < left_cols || (*idx >= right_start && *idx < right_end),
        BoundExpr::BinaryOp { left, right, .. }
        | BoundExpr::AnyOp { left, right, .. }
        | BoundExpr::AllOp { left, right, .. } => {
            all_refs_in_ranges(left, left_cols, right_start, right_end)
                && all_refs_in_ranges(right, left_cols, right_start, right_end)
        }
        BoundExpr::Not(inner)
        | BoundExpr::IsNull(inner)
        | BoundExpr::IsNotNull(inner)
        | BoundExpr::Cast { expr: inner, .. } => {
            all_refs_in_ranges(inner, left_cols, right_start, right_end)
        }
        BoundExpr::Literal(_) => true,
        BoundExpr::Function { args, .. } => args
            .iter()
            .all(|a| all_refs_in_ranges(a, left_cols, right_start, right_end)),
        BoundExpr::ArraySlice {
            array,
            lower,
            upper,
        } => {
            all_refs_in_ranges(array, left_cols, right_start, right_end)
                && lower
                    .as_ref()
                    .is_none_or(|l| all_refs_in_ranges(l, left_cols, right_start, right_end))
                && upper
                    .as_ref()
                    .is_none_or(|u| all_refs_in_ranges(u, left_cols, right_start, right_end))
        }
        _ => {
            // Conservative: treat complex expressions as non-self-contained
            false
        }
    }
}

/// Remap `ColumnRef` indices that fall in the old right-table range
/// `[old_offset, old_offset+right_cols)` to `[new_offset, ...)`.
/// Left-table references (`< old_offset` when old_offset == left_cols,
/// or generally anything outside the old right range) are unchanged.
fn remap_column_refs(
    expr: &BoundExpr,
    old_offset: usize,
    right_cols: usize,
    new_offset: usize,
) -> BoundExpr {
    match expr {
        BoundExpr::ColumnRef(idx) => {
            if *idx >= old_offset && *idx < old_offset + right_cols {
                BoundExpr::ColumnRef(new_offset + (*idx - old_offset))
            } else {
                expr.clone()
            }
        }
        BoundExpr::BinaryOp { left, op, right } => BoundExpr::BinaryOp {
            left: Box::new(remap_column_refs(left, old_offset, right_cols, new_offset)),
            op: *op,
            right: Box::new(remap_column_refs(right, old_offset, right_cols, new_offset)),
        },
        BoundExpr::Not(inner) => BoundExpr::Not(Box::new(remap_column_refs(
            inner, old_offset, right_cols, new_offset,
        ))),
        BoundExpr::IsNull(inner) => BoundExpr::IsNull(Box::new(remap_column_refs(
            inner, old_offset, right_cols, new_offset,
        ))),
        BoundExpr::IsNotNull(inner) => BoundExpr::IsNotNull(Box::new(remap_column_refs(
            inner, old_offset, right_cols, new_offset,
        ))),
        BoundExpr::Cast {
            expr: inner,
            target_type,
        } => BoundExpr::Cast {
            expr: Box::new(remap_column_refs(inner, old_offset, right_cols, new_offset)),
            target_type: target_type.clone(),
        },
        BoundExpr::Function { func, args } => BoundExpr::Function {
            func: func.clone(),
            args: args
                .iter()
                .map(|a| remap_column_refs(a, old_offset, right_cols, new_offset))
                .collect(),
        },
        BoundExpr::AnyOp {
            left,
            compare_op,
            right,
        } => BoundExpr::AnyOp {
            left: Box::new(remap_column_refs(left, old_offset, right_cols, new_offset)),
            compare_op: *compare_op,
            right: Box::new(remap_column_refs(right, old_offset, right_cols, new_offset)),
        },
        BoundExpr::AllOp {
            left,
            compare_op,
            right,
        } => BoundExpr::AllOp {
            left: Box::new(remap_column_refs(left, old_offset, right_cols, new_offset)),
            compare_op: *compare_op,
            right: Box::new(remap_column_refs(right, old_offset, right_cols, new_offset)),
        },
        BoundExpr::ArraySlice {
            array,
            lower,
            upper,
        } => BoundExpr::ArraySlice {
            array: Box::new(remap_column_refs(array, old_offset, right_cols, new_offset)),
            lower: lower
                .as_ref()
                .map(|l| Box::new(remap_column_refs(l, old_offset, right_cols, new_offset))),
            upper: upper
                .as_ref()
                .map(|u| Box::new(remap_column_refs(u, old_offset, right_cols, new_offset))),
        },
        // Leaf / complex nodes — clone unchanged
        _ => expr.clone(),
    }
}

/// Estimate the output row count of a join for intermediate-result sizing.
pub fn estimate_join_output(left_rows: u64, right_rows: u64, join_type: JoinType) -> u64 {
    match join_type {
        JoinType::Inner => left_rows.min(right_rows),
        JoinType::Left => left_rows,
        JoinType::Right => right_rows,
        JoinType::FullOuter => left_rows.max(right_rows),
        JoinType::Cross => left_rows.saturating_mul(right_rows),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::ColumnId;

    fn make_schema(id: u64, name: &str, cols: usize) -> TableSchema {
        TableSchema {
            id: TableId(id),
            name: name.into(),
            columns: (0..cols)
                .map(|i| ColumnDef {
                    id: ColumnId(i as u32),
                    name: format!("c{}", i),
                    data_type: falcon_common::types::DataType::Int32,
                    nullable: false,
                    is_primary_key: i == 0,
                    default_value: None,
                    is_serial: false,
                })
                .collect(),
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        }
    }

    /// Build an equi-join condition: ColumnRef(left_idx) = ColumnRef(right_idx)
    fn eq_cond(left_idx: usize, right_idx: usize) -> BoundExpr {
        BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::ColumnRef(left_idx)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::ColumnRef(right_idx)),
        }
    }

    #[test]
    fn test_reorder_single_join_noop() {
        let joins = vec![BoundJoin {
            join_type: JoinType::Inner,
            right_table_id: TableId(2),
            right_table_name: "b".into(),
            right_schema: make_schema(2, "b", 2),
            right_col_offset: 3,
            condition: Some(eq_cond(0, 3)),
        }];
        let stats: TableRowCounts = [(TableId(2), 100)].into();
        let result = reorder_joins(3, &joins, &stats);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].right_table_id, TableId(2));
    }

    #[test]
    fn test_reorder_two_inner_joins_smallest_first() {
        // Left table A has 3 columns
        // Join B (2 cols, 1000 rows) at offset 3, then C (2 cols, 10 rows) at offset 5
        // After reorder: C (10 rows) first, then B (1000 rows)
        let joins = vec![
            BoundJoin {
                join_type: JoinType::Inner,
                right_table_id: TableId(2),
                right_table_name: "b".into(),
                right_schema: make_schema(2, "b", 2),
                right_col_offset: 3,
                condition: Some(eq_cond(0, 3)),
            },
            BoundJoin {
                join_type: JoinType::Inner,
                right_table_id: TableId(3),
                right_table_name: "c".into(),
                right_schema: make_schema(3, "c", 2),
                right_col_offset: 5,
                condition: Some(eq_cond(1, 5)),
            },
        ];
        let stats: TableRowCounts = [(TableId(2), 1000), (TableId(3), 10)].into();
        let result = reorder_joins(3, &joins, &stats);

        // C should come first (10 rows), B second (1000 rows)
        assert_eq!(result[0].right_table_id, TableId(3));
        assert_eq!(result[1].right_table_id, TableId(2));

        // Check remapped offsets: C now at offset 3, B at offset 5
        assert_eq!(result[0].right_col_offset, 3);
        assert_eq!(result[1].right_col_offset, 5);

        // Check remapped condition for C: was ColumnRef(1) = ColumnRef(5), now ColumnRef(1) = ColumnRef(3)
        if let Some(BoundExpr::BinaryOp { right, .. }) = &result[0].condition {
            assert!(matches!(right.as_ref(), BoundExpr::ColumnRef(3)));
        } else {
            panic!("Expected BinaryOp condition for C");
        }

        // Check remapped condition for B: was ColumnRef(0) = ColumnRef(3), now ColumnRef(0) = ColumnRef(5)
        if let Some(BoundExpr::BinaryOp { right, .. }) = &result[1].condition {
            assert!(matches!(right.as_ref(), BoundExpr::ColumnRef(5)));
        } else {
            panic!("Expected BinaryOp condition for B");
        }
    }

    #[test]
    fn test_reorder_preserves_outer_joins() {
        // LEFT JOIN should not be reordered
        let joins = vec![
            BoundJoin {
                join_type: JoinType::Left,
                right_table_id: TableId(2),
                right_table_name: "b".into(),
                right_schema: make_schema(2, "b", 2),
                right_col_offset: 3,
                condition: Some(eq_cond(0, 3)),
            },
            BoundJoin {
                join_type: JoinType::Inner,
                right_table_id: TableId(3),
                right_table_name: "c".into(),
                right_schema: make_schema(3, "c", 2),
                right_col_offset: 5,
                condition: Some(eq_cond(1, 5)),
            },
        ];
        let stats: TableRowCounts = [(TableId(2), 1000), (TableId(3), 10)].into();
        let result = reorder_joins(3, &joins, &stats);

        // Only 1 reorderable (the INNER), so no reorder happens
        assert_eq!(result[0].right_table_id, TableId(2));
        assert_eq!(result[1].right_table_id, TableId(3));
    }

    #[test]
    fn test_reorder_no_stats_noop() {
        let joins = vec![
            BoundJoin {
                join_type: JoinType::Inner,
                right_table_id: TableId(2),
                right_table_name: "b".into(),
                right_schema: make_schema(2, "b", 2),
                right_col_offset: 3,
                condition: Some(eq_cond(0, 3)),
            },
            BoundJoin {
                join_type: JoinType::Inner,
                right_table_id: TableId(3),
                right_table_name: "c".into(),
                right_schema: make_schema(3, "c", 2),
                right_col_offset: 5,
                condition: Some(eq_cond(1, 5)),
            },
        ];
        // No stats — both get u64::MAX, order preserved
        let stats: TableRowCounts = HashMap::new();
        let result = reorder_joins(3, &joins, &stats);
        assert_eq!(result[0].right_table_id, TableId(2));
        assert_eq!(result[1].right_table_id, TableId(3));
    }

    #[test]
    fn test_reorder_three_inner_joins() {
        // A(3 cols) JOIN B(2, 500 rows) JOIN C(2, 5 rows) JOIN D(2, 50 rows)
        let joins = vec![
            BoundJoin {
                join_type: JoinType::Inner,
                right_table_id: TableId(2),
                right_table_name: "b".into(),
                right_schema: make_schema(2, "b", 2),
                right_col_offset: 3,
                condition: Some(eq_cond(0, 3)),
            },
            BoundJoin {
                join_type: JoinType::Inner,
                right_table_id: TableId(3),
                right_table_name: "c".into(),
                right_schema: make_schema(3, "c", 2),
                right_col_offset: 5,
                condition: Some(eq_cond(1, 5)),
            },
            BoundJoin {
                join_type: JoinType::Inner,
                right_table_id: TableId(4),
                right_table_name: "d".into(),
                right_schema: make_schema(4, "d", 2),
                right_col_offset: 7,
                condition: Some(eq_cond(2, 7)),
            },
        ];
        let stats: TableRowCounts = [(TableId(2), 500), (TableId(3), 5), (TableId(4), 50)].into();
        let result = reorder_joins(3, &joins, &stats);

        // Expected order: C(5) → D(50) → B(500)
        assert_eq!(result[0].right_table_id, TableId(3));
        assert_eq!(result[1].right_table_id, TableId(4));
        assert_eq!(result[2].right_table_id, TableId(2));

        // Offsets: C at 3, D at 5, B at 7
        assert_eq!(result[0].right_col_offset, 3);
        assert_eq!(result[1].right_col_offset, 5);
        assert_eq!(result[2].right_col_offset, 7);
    }

    #[test]
    fn test_estimate_join_output() {
        assert_eq!(estimate_join_output(100, 10, JoinType::Inner), 10);
        assert_eq!(estimate_join_output(100, 10, JoinType::Left), 100);
        assert_eq!(estimate_join_output(100, 10, JoinType::Right), 10);
        assert_eq!(estimate_join_output(100, 10, JoinType::Cross), 1000);
    }

    #[test]
    fn test_is_self_contained_cross_ref_blocked() {
        // Condition references column 3 which belongs to a *different* right table
        // (not the one at offset 5). This should NOT be considered self-contained.
        let join = BoundJoin {
            join_type: JoinType::Inner,
            right_table_id: TableId(3),
            right_table_name: "c".into(),
            right_schema: make_schema(3, "c", 2),
            right_col_offset: 5,
            condition: Some(eq_cond(3, 5)), // col 3 is in B's range, not left table
        };
        // left_cols = 3: columns 0,1,2 are left table
        assert!(!is_self_contained(&join, 3));
    }
}
