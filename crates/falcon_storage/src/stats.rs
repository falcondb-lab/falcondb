use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use falcon_common::datum::Datum;
use falcon_common::types::TableId;

/// Per-column statistics collected by ANALYZE.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Column index in the table schema.
    pub column_idx: usize,
    /// Column name (for display).
    pub column_name: String,
    /// Number of NULL values.
    pub null_count: u64,
    /// Approximate number of distinct non-null values.
    pub distinct_count: u64,
    /// Minimum value (None if all NULL or column is non-comparable).
    pub min_value: Option<Datum>,
    /// Maximum value (None if all NULL or column is non-comparable).
    pub max_value: Option<Datum>,
    /// Average width in bytes (for Text columns, average string length; 0 for fixed-size).
    pub avg_width: u32,
    /// Equi-depth histogram for selectivity estimation.
    /// Each bucket covers approximately `total_rows / num_buckets` rows.
    pub histogram: Option<Histogram>,
    /// Most common values and their frequencies (for skewed distributions).
    pub mcv: Option<McvList>,
}

/// Equi-depth (equi-height) histogram.
///
/// Divides the value domain into `num_buckets` ranges, each containing
/// approximately the same number of rows. Bucket boundaries are stored
/// as `Datum` values (sorted ascending).
///
/// Used for range-predicate selectivity estimation:
///   `selectivity(col < v)` ≈ fraction of buckets whose upper bound ≤ v,
///   plus interpolation within the straddling bucket.
#[derive(Debug, Clone)]
pub struct Histogram {
    /// Bucket upper boundaries (sorted ascending, length = num_buckets).
    /// bucket[i] is the upper bound of the i-th bucket.
    pub bounds: Vec<Datum>,
    /// Number of distinct values per bucket (optional, for join size estimation).
    pub distinct_per_bucket: Vec<u64>,
    /// Total number of non-null values used to build this histogram.
    pub total_rows: u64,
}

impl Histogram {
    /// Estimate selectivity for `col < value` (less-than).
    pub fn selectivity_lt(&self, value: &Datum) -> f64 {
        if self.bounds.is_empty() || self.total_rows == 0 {
            return 0.33;
        }
        let num_buckets = self.bounds.len();
        // Find the first bucket whose upper bound >= value
        let mut pos = 0;
        for (i, bound) in self.bounds.iter().enumerate() {
            match datum_cmp(value, bound) {
                Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal) => {
                    pos = i;
                    break;
                }
                _ => { pos = i + 1; }
            }
        }
        // Linear interpolation
        let bucket_fraction = 1.0 / num_buckets as f64;
        let base = pos as f64 * bucket_fraction;
        // Within the straddling bucket, assume uniform distribution → ~0.5 of bucket
        if pos < num_buckets {
            (base + 0.5 * bucket_fraction).min(1.0)
        } else {
            1.0
        }
    }

    /// Estimate selectivity for `col = value` (equality).
    /// Uses 1/distinct_per_bucket for the bucket containing value.
    pub fn selectivity_eq(&self, value: &Datum) -> f64 {
        if self.bounds.is_empty() || self.total_rows == 0 {
            return 0.1;
        }
        let num_buckets = self.bounds.len();
        // Find the bucket containing the value
        let mut bucket_idx = num_buckets - 1;
        for (i, bound) in self.bounds.iter().enumerate() {
            if let Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) = datum_cmp(value, bound) {
                bucket_idx = i;
                break;
            }
        }
        // Selectivity ≈ (rows_per_bucket / total_rows) * (1 / distinct_in_bucket)
        let rows_per_bucket = self.total_rows as f64 / num_buckets as f64;
        let distinct = if bucket_idx < self.distinct_per_bucket.len() && self.distinct_per_bucket[bucket_idx] > 0 {
            self.distinct_per_bucket[bucket_idx] as f64
        } else {
            rows_per_bucket.max(1.0)
        };
        (rows_per_bucket / distinct) / self.total_rows as f64
    }

    /// Estimate selectivity for `col BETWEEN low AND high`.
    pub fn selectivity_between(&self, low: &Datum, high: &Datum) -> f64 {
        let sel_high = self.selectivity_lt(high);
        let sel_low = self.selectivity_lt(low);
        let eq_high = self.selectivity_eq(high);
        (sel_high - sel_low + eq_high).max(0.0).min(1.0)
    }
}

/// Most Common Values list — top-N most frequent values and their frequencies.
#[derive(Debug, Clone)]
pub struct McvList {
    /// (value, frequency) pairs sorted by frequency descending.
    pub entries: Vec<(Datum, f64)>,
}

impl McvList {
    /// Look up the frequency of a specific value. Returns None if not in MCV.
    pub fn frequency(&self, value: &Datum) -> Option<f64> {
        for (v, freq) in &self.entries {
            if datum_eq(v, value) {
                return Some(*freq);
            }
        }
        None
    }
}

/// Per-table statistics collected by ANALYZE.
#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub table_id: TableId,
    pub table_name: String,
    /// Total number of live (committed, non-tombstone) rows at ANALYZE time.
    pub row_count: u64,
    /// Per-column statistics.
    pub column_stats: Vec<ColumnStats>,
    /// Timestamp (epoch ms) when ANALYZE was last run.
    pub last_analyzed_ms: u64,
}

impl TableStatistics {
    /// Get column stats by column index.
    pub fn column(&self, idx: usize) -> Option<&ColumnStats> {
        self.column_stats.iter().find(|c| c.column_idx == idx)
    }

    /// Selectivity estimate for equality predicate: 1 / distinct_count.
    /// Returns 0.1 as fallback if no stats.
    pub fn eq_selectivity(&self, col_idx: usize) -> f64 {
        if let Some(cs) = self.column(col_idx) {
            if cs.distinct_count > 0 {
                return 1.0 / cs.distinct_count as f64;
            }
        }
        0.1
    }

    /// Selectivity estimate for range predicate (`col < value`).
    /// Uses histogram if available, otherwise falls back to 0.33.
    pub fn range_selectivity(&self, col_idx: usize) -> f64 {
        if let Some(cs) = self.column(col_idx) {
            if let Some(ref hist) = cs.histogram {
                // Use average bucket selectivity for unknown range
                return (1.0 / hist.bounds.len() as f64).max(0.01);
            }
        }
        0.33
    }

    /// Selectivity for `col < value` using histogram.
    pub fn lt_selectivity(&self, col_idx: usize, value: &Datum) -> f64 {
        if let Some(cs) = self.column(col_idx) {
            if let Some(ref hist) = cs.histogram {
                return hist.selectivity_lt(value);
            }
        }
        0.33
    }

    /// Selectivity for `col = value` using histogram + MCV.
    pub fn eq_selectivity_value(&self, col_idx: usize, value: &Datum) -> f64 {
        if let Some(cs) = self.column(col_idx) {
            // Check MCV first (most accurate for skewed data)
            if let Some(ref mcv) = cs.mcv {
                if let Some(freq) = mcv.frequency(value) {
                    return freq;
                }
            }
            // Fall back to histogram
            if let Some(ref hist) = cs.histogram {
                return hist.selectivity_eq(value);
            }
            // Fall back to 1/distinct
            if cs.distinct_count > 0 {
                return 1.0 / cs.distinct_count as f64;
            }
        }
        0.1
    }

    /// Selectivity for `col BETWEEN low AND high` using histogram.
    pub fn between_selectivity(&self, col_idx: usize, low: &Datum, high: &Datum) -> f64 {
        if let Some(cs) = self.column(col_idx) {
            if let Some(ref hist) = cs.histogram {
                return hist.selectivity_between(low, high);
            }
        }
        0.33
    }

    /// Estimate the join output cardinality when joining this table's
    /// `col_idx` with another table of `other_rows` rows using equi-join.
    /// Formula: max(rows, other_rows) / max(distinct, 1)
    pub fn join_cardinality(&self, col_idx: usize, other_rows: u64) -> u64 {
        let my_rows = self.row_count;
        if let Some(cs) = self.column(col_idx) {
            if cs.distinct_count > 0 {
                let max_input = my_rows.max(other_rows);
                return max_input / cs.distinct_count.max(1);
            }
        }
        // Fallback: min of the two
        my_rows.min(other_rows)
    }
}

/// Wrapper for hashing Datum values during distinct-count computation.
/// We need this because Datum derives Clone but not Hash/Eq natively
/// due to f64. We handle f64 by converting to bits.
#[derive(Debug, Clone)]
struct DatumKey(Datum);

impl PartialEq for DatumKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Datum::Null, Datum::Null) => true,
            (Datum::Boolean(a), Datum::Boolean(b)) => a == b,
            (Datum::Int32(a), Datum::Int32(b)) => a == b,
            (Datum::Int64(a), Datum::Int64(b)) => a == b,
            (Datum::Float64(a), Datum::Float64(b)) => a.to_bits() == b.to_bits(),
            (Datum::Text(a), Datum::Text(b)) => a == b,
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a == b,
            (Datum::Date(a), Datum::Date(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for DatumKey {}

impl Hash for DatumKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            Datum::Null => {}
            Datum::Boolean(v) => v.hash(state),
            Datum::Int32(v) => v.hash(state),
            Datum::Int64(v) => v.hash(state),
            Datum::Float64(v) => v.to_bits().hash(state),
            Datum::Text(v) => v.hash(state),
            Datum::Timestamp(v) => v.hash(state),
            Datum::Date(v) => v.hash(state),
            Datum::Array(arr) => {
                arr.len().hash(state);
                // Shallow hash — just use length for arrays
            }
            Datum::Jsonb(v) => v.to_string().hash(state),
            Datum::Decimal(m, s) => { m.hash(state); s.hash(state); }
        }
    }
}

/// Compare two Datum values for min/max tracking.
/// Returns None if types are incomparable.
fn datum_cmp(a: &Datum, b: &Datum) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Datum::Int32(x), Datum::Int32(y)) => Some(x.cmp(y)),
        (Datum::Int64(x), Datum::Int64(y)) => Some(x.cmp(y)),
        (Datum::Float64(x), Datum::Float64(y)) => x.partial_cmp(y),
        (Datum::Text(x), Datum::Text(y)) => Some(x.cmp(y)),
        (Datum::Timestamp(x), Datum::Timestamp(y)) => Some(x.cmp(y)),
        (Datum::Date(x), Datum::Date(y)) => Some(x.cmp(y)),
        (Datum::Boolean(x), Datum::Boolean(y)) => Some(x.cmp(y)),
        (Datum::Decimal(a, sa), Datum::Decimal(b, sb)) => {
            let (na, nb) = if sa == sb { (*a, *b) } else if sa > sb {
                (*a, *b * 10i128.pow((*sa - *sb) as u32))
            } else {
                (*a * 10i128.pow((*sb - *sa) as u32), *b)
            };
            Some(na.cmp(&nb))
        }
        _ => None,
    }
}

/// Equality check for Datum (handles f64 with to_bits, NULL == NULL).
fn datum_eq(a: &Datum, b: &Datum) -> bool {
    match (a, b) {
        (Datum::Null, Datum::Null) => true,
        (Datum::Boolean(x), Datum::Boolean(y)) => x == y,
        (Datum::Int32(x), Datum::Int32(y)) => x == y,
        (Datum::Int64(x), Datum::Int64(y)) => x == y,
        (Datum::Float64(x), Datum::Float64(y)) => x.to_bits() == y.to_bits(),
        (Datum::Text(x), Datum::Text(y)) => x == y,
        (Datum::Timestamp(x), Datum::Timestamp(y)) => x == y,
        (Datum::Date(x), Datum::Date(y)) => x == y,
        _ => false,
    }
}

/// Datum byte width estimate (for avg_width).
fn datum_width(d: &Datum) -> usize {
    match d {
        Datum::Null => 0,
        Datum::Boolean(_) => 1,
        Datum::Int32(_) => 4,
        Datum::Int64(_) | Datum::Timestamp(_) | Datum::Float64(_) => 8,
        Datum::Date(_) => 4,
        Datum::Text(s) => s.len(),
        Datum::Array(arr) => arr.len() * 8, // rough estimate
        Datum::Jsonb(v) => v.to_string().len(),
        Datum::Decimal(_, _) => 16, // i128 = 16 bytes
    }
}

/// Collect statistics for a set of rows (all committed visible rows of a table).
pub fn collect_column_stats(
    columns: &[(usize, String)], // (col_idx, col_name)
    rows: &[Vec<Datum>],
) -> Vec<ColumnStats> {
    let num_cols = columns.len();
    let mut null_counts = vec![0u64; num_cols];
    let mut distinct_sets: Vec<HashSet<DatumKey>> = (0..num_cols).map(|_| HashSet::new()).collect();
    let mut min_vals: Vec<Option<Datum>> = vec![None; num_cols];
    let mut max_vals: Vec<Option<Datum>> = vec![None; num_cols];
    let mut width_sums: Vec<u64> = vec![0; num_cols];
    let mut non_null_counts: Vec<u64> = vec![0; num_cols];

    for row in rows {
        for (i, &(col_idx, _)) in columns.iter().enumerate() {
            if col_idx >= row.len() {
                null_counts[i] += 1;
                continue;
            }
            let datum = &row[col_idx];
            if datum.is_null() {
                null_counts[i] += 1;
                continue;
            }

            non_null_counts[i] += 1;
            width_sums[i] += datum_width(datum) as u64;
            distinct_sets[i].insert(DatumKey(datum.clone()));

            // Update min
            match &min_vals[i] {
                None => min_vals[i] = Some(datum.clone()),
                Some(cur_min) => {
                    if let Some(std::cmp::Ordering::Less) = datum_cmp(datum, cur_min) {
                        min_vals[i] = Some(datum.clone());
                    }
                }
            }
            // Update max
            match &max_vals[i] {
                None => max_vals[i] = Some(datum.clone()),
                Some(cur_max) => {
                    if let Some(std::cmp::Ordering::Greater) = datum_cmp(datum, cur_max) {
                        max_vals[i] = Some(datum.clone());
                    }
                }
            }
        }
    }

    // Collect per-column sorted non-null values for histogram building
    let mut col_values: Vec<Vec<Datum>> = (0..num_cols).map(|_| Vec::new()).collect();
    for row in rows {
        for (i, &(col_idx, _)) in columns.iter().enumerate() {
            if col_idx < row.len() && !row[col_idx].is_null() {
                col_values[i].push(row[col_idx].clone());
            }
        }
    }

    columns
        .iter()
        .enumerate()
        .map(|(i, (col_idx, col_name))| {
            let avg_width = if non_null_counts[i] > 0 {
                (width_sums[i] / non_null_counts[i]) as u32
            } else {
                0
            };
            let histogram = build_histogram(&mut col_values[i], DEFAULT_HISTOGRAM_BUCKETS);
            let mcv = build_mcv(&col_values[i], non_null_counts[i], DEFAULT_MCV_SIZE);
            ColumnStats {
                column_idx: *col_idx,
                column_name: col_name.clone(),
                null_count: null_counts[i],
                distinct_count: distinct_sets[i].len() as u64,
                min_value: min_vals[i].clone(),
                max_value: max_vals[i].clone(),
                avg_width,
                histogram,
                mcv,
            }
        })
        .collect()
}

/// Default number of histogram buckets (matches PostgreSQL's default_statistics_target).
const DEFAULT_HISTOGRAM_BUCKETS: usize = 100;

/// Default number of MCV entries to track.
const DEFAULT_MCV_SIZE: usize = 10;

/// Build an equi-depth histogram from a column's non-null values.
/// Sorts the values in-place, then picks evenly-spaced bucket boundaries.
fn build_histogram(values: &mut Vec<Datum>, num_buckets: usize) -> Option<Histogram> {
    if values.len() < 2 * num_buckets {
        // Not enough data to build a meaningful histogram
        return None;
    }

    // Sort the values
    values.sort_by(|a, b| datum_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal));

    let n = values.len();
    let bucket_size = n / num_buckets;
    if bucket_size == 0 {
        return None;
    }

    let mut bounds = Vec::with_capacity(num_buckets);
    let mut distinct_per_bucket = Vec::with_capacity(num_buckets);

    for b in 0..num_buckets {
        let end_idx = if b == num_buckets - 1 {
            n - 1
        } else {
            (b + 1) * bucket_size - 1
        };
        bounds.push(values[end_idx].clone());

        // Count distinct values in this bucket
        let start_idx = b * bucket_size;
        let mut distinct = HashSet::new();
        let actual_end = if b == num_buckets - 1 { n } else { (b + 1) * bucket_size };
        for v in &values[start_idx..actual_end] {
            distinct.insert(DatumKey(v.clone()));
        }
        distinct_per_bucket.push(distinct.len() as u64);
    }

    Some(Histogram {
        bounds,
        distinct_per_bucket,
        total_rows: n as u64,
    })
}

/// Build a Most Common Values list from column values.
fn build_mcv(values: &[Datum], total_non_null: u64, max_entries: usize) -> Option<McvList> {
    if values.is_empty() || total_non_null == 0 {
        return None;
    }

    // Count frequencies
    let mut freq_map: std::collections::HashMap<DatumKey, u64> = std::collections::HashMap::new();
    for v in values {
        *freq_map.entry(DatumKey(v.clone())).or_insert(0) += 1;
    }

    // Only build MCV if there's some skew (at least one value appears > 1% of rows)
    let threshold = (total_non_null as f64 * 0.01).max(2.0) as u64;
    let mut entries: Vec<(Datum, f64)> = freq_map
        .into_iter()
        .filter(|(_, count)| *count >= threshold)
        .map(|(key, count)| (key.0, count as f64 / total_non_null as f64))
        .collect();

    if entries.is_empty() {
        return None;
    }

    // Sort by frequency descending, take top N
    entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    entries.truncate(max_entries);

    Some(McvList { entries })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_empty_rows() {
        let cols = vec![(0, "id".into()), (1, "name".into())];
        let stats = collect_column_stats(&cols, &[]);
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].null_count, 0);
        assert_eq!(stats[0].distinct_count, 0);
        assert!(stats[0].min_value.is_none());
    }

    #[test]
    fn test_collect_basic_stats() {
        let cols = vec![(0, "id".into()), (1, "name".into())];
        let rows = vec![
            vec![Datum::Int32(1), Datum::Text("alice".into())],
            vec![Datum::Int32(2), Datum::Text("bob".into())],
            vec![Datum::Int32(3), Datum::Text("alice".into())],
            vec![Datum::Int32(1), Datum::Null],
        ];
        let stats = collect_column_stats(&cols, &rows);

        // id column
        assert_eq!(stats[0].null_count, 0);
        assert_eq!(stats[0].distinct_count, 3); // 1, 2, 3
        assert_eq!(stats[0].min_value, Some(Datum::Int32(1)));
        assert_eq!(stats[0].max_value, Some(Datum::Int32(3)));

        // name column
        assert_eq!(stats[1].null_count, 1);
        assert_eq!(stats[1].distinct_count, 2); // "alice", "bob"
        assert_eq!(stats[1].min_value, Some(Datum::Text("alice".into())));
        assert_eq!(stats[1].max_value, Some(Datum::Text("bob".into())));
    }

    #[test]
    fn test_collect_all_nulls() {
        let cols = vec![(0, "x".into())];
        let rows = vec![vec![Datum::Null], vec![Datum::Null]];
        let stats = collect_column_stats(&cols, &rows);
        assert_eq!(stats[0].null_count, 2);
        assert_eq!(stats[0].distinct_count, 0);
        assert!(stats[0].min_value.is_none());
        assert!(stats[0].max_value.is_none());
    }

    #[test]
    fn test_table_statistics_eq_selectivity() {
        let ts = TableStatistics {
            table_id: falcon_common::types::TableId(1),
            table_name: "test".into(),
            row_count: 100,
            column_stats: vec![ColumnStats {
                column_idx: 0,
                column_name: "id".into(),
                null_count: 0,
                distinct_count: 50,
                min_value: Some(Datum::Int32(1)),
                max_value: Some(Datum::Int32(100)),
                avg_width: 4,
                histogram: None,
                mcv: None,
            }],
            last_analyzed_ms: 0,
        };
        let sel = ts.eq_selectivity(0);
        assert!((sel - 0.02).abs() < 0.001);
        // Unknown column falls back to 0.1
        assert!((ts.eq_selectivity(99) - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_datum_key_float_nan() {
        // Ensure NaN == NaN for DatumKey (since we use to_bits)
        let a = DatumKey(Datum::Float64(f64::NAN));
        let b = DatumKey(Datum::Float64(f64::NAN));
        assert_eq!(a, b);
    }

    #[test]
    fn test_histogram_build_small_data() {
        // Too few rows for histogram
        let mut values = vec![Datum::Int32(1), Datum::Int32(2), Datum::Int32(3)];
        let hist = build_histogram(&mut values, 100);
        assert!(hist.is_none());
    }

    #[test]
    fn test_histogram_build_and_selectivity() {
        // Build a histogram with 5 buckets from 500 values
        let mut values: Vec<Datum> = (0..500).map(|i| Datum::Int32(i)).collect();
        let hist = build_histogram(&mut values, 5).expect("should build histogram");
        assert_eq!(hist.bounds.len(), 5);
        assert_eq!(hist.total_rows, 500);

        // col < 0 → should be very low
        let sel_low = hist.selectivity_lt(&Datum::Int32(0));
        assert!(sel_low < 0.2, "sel_lt(0) = {}", sel_low);

        // col < 500 → should be ~1.0
        let sel_high = hist.selectivity_lt(&Datum::Int32(500));
        assert!(sel_high > 0.8, "sel_lt(500) = {}", sel_high);

        // col = 250 → should be small
        let sel_eq = hist.selectivity_eq(&Datum::Int32(250));
        assert!(sel_eq < 0.1, "sel_eq(250) = {}", sel_eq);
    }

    #[test]
    fn test_mcv_build_and_lookup() {
        // Build MCV from skewed data
        let mut values = Vec::new();
        for _ in 0..100 { values.push(Datum::Int32(1)); } // 50%
        for _ in 0..60 { values.push(Datum::Int32(2)); } // 30%
        for _ in 0..40 { values.push(Datum::Int32(3)); } // 20%
        let mcv = build_mcv(&values, 200, 10).expect("should build MCV");
        assert!(!mcv.entries.is_empty());

        // Value 1 should have highest frequency
        let freq_1 = mcv.frequency(&Datum::Int32(1)).expect("should find value 1");
        assert!(freq_1 > 0.4, "freq(1) = {}", freq_1);

        // Value 999 should not be in MCV
        assert!(mcv.frequency(&Datum::Int32(999)).is_none());
    }

    #[test]
    fn test_collect_stats_with_histogram() {
        let cols = vec![(0, "id".into())];
        let rows: Vec<Vec<Datum>> = (0..500).map(|i| vec![Datum::Int32(i)]).collect();
        let stats = collect_column_stats(&cols, &rows);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].distinct_count, 500);
        // Should have histogram (500 >= 2*100)
        assert!(stats[0].histogram.is_some(), "should have histogram for 500 rows");
    }

    #[test]
    fn test_table_statistics_lt_selectivity() {
        let mut values: Vec<Datum> = (0..500).map(|i| Datum::Int32(i)).collect();
        let hist = build_histogram(&mut values, 5).unwrap();
        let ts = TableStatistics {
            table_id: falcon_common::types::TableId(1),
            table_name: "test".into(),
            row_count: 500,
            column_stats: vec![ColumnStats {
                column_idx: 0,
                column_name: "id".into(),
                null_count: 0,
                distinct_count: 500,
                min_value: Some(Datum::Int32(0)),
                max_value: Some(Datum::Int32(499)),
                avg_width: 4,
                histogram: Some(hist),
                mcv: None,
            }],
            last_analyzed_ms: 0,
        };
        let sel = ts.lt_selectivity(0, &Datum::Int32(250));
        assert!(sel > 0.3 && sel < 0.7, "lt_selectivity(250) = {}", sel);
    }

    #[test]
    fn test_join_cardinality_estimation() {
        let ts = TableStatistics {
            table_id: falcon_common::types::TableId(1),
            table_name: "test".into(),
            row_count: 1000,
            column_stats: vec![ColumnStats {
                column_idx: 0,
                column_name: "id".into(),
                null_count: 0,
                distinct_count: 100,
                min_value: Some(Datum::Int32(1)),
                max_value: Some(Datum::Int32(100)),
                avg_width: 4,
                histogram: None,
                mcv: None,
            }],
            last_analyzed_ms: 0,
        };
        // join with table of 500 rows → max(1000,500)/100 = 10
        let card = ts.join_cardinality(0, 500);
        assert_eq!(card, 10);
    }
}
