//! Core sharding utilities: multi-column shard key hashing, range routing,
//! and shard-aware DDL/DML dispatch.
//!
//! # Design (SingleStore-inspired)
//!
//! - **Hash sharding**: rows are assigned to shards by hashing the shard key
//!   columns (or PK if no explicit shard key) with xxHash3.
//! - **Range sharding**: rows are assigned to shards by comparing the first
//!   shard key column against ordered boundary values. Ideal for time-series,
//!   log-partitioned, or naturally ordered datasets.
//! - **Reference tables**: fully replicated on every shard (no shard key).
//! - **Auto-sharding**: `CREATE TABLE ... WITH (shard_key='col', sharding='hash')`
//!   automatically distributes the table across all shards.
//!
//! # Shard Key Hashing
//!
//! Multi-column shard keys are hashed by concatenating the binary encoding of
//! each column value (using the same PK encoding as MemTable) and computing
//! xxHash3-64 over the result.  This gives uniform distribution for any
//! combination of Datum types.
//!
//! # Range Routing
//!
//! For range-sharded tables, `range_bounds` in the schema contains N-1 split
//! points for N shards.  A row with shard-key value `v` is routed to shard `i`
//! where `range_bounds[i-1] <= v < range_bounds[i]` (implicit -∞/+∞ at ends).
//! Range queries can be pruned to only the shards whose boundaries overlap.

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::TableSchema;
use falcon_common::types::ShardId;
use xxhash_rust::xxh3::xxh3_64;

/// Compute the shard hash for a row given the shard key column indices.
/// Returns a u64 hash value suitable for shard assignment via modulo.
pub fn compute_shard_hash(row: &OwnedRow, shard_key_indices: &[usize]) -> u64 {
    if shard_key_indices.is_empty() {
        return 0;
    }
    let mut buf = Vec::with_capacity(64);
    for &col_idx in shard_key_indices {
        if let Some(datum) = row.values.get(col_idx) {
            encode_datum_for_hash(&mut buf, datum);
        } else {
            buf.push(0x00); // NULL sentinel
        }
    }
    xxh3_64(&buf)
}

/// Compute the shard hash from a slice of Datum references (for WHERE-clause extraction).
pub fn compute_shard_hash_from_datums(datums: &[&Datum]) -> u64 {
    if datums.is_empty() {
        return 0;
    }
    let mut buf = Vec::with_capacity(64);
    for datum in datums {
        encode_datum_for_hash(&mut buf, datum);
    }
    xxh3_64(&buf)
}

/// Compare two `Datum` values for range routing.
/// NULL is treated as less-than any non-NULL value.
pub fn cmp_datum_for_range(a: &Datum, b: &Datum) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Datum::Null, Datum::Null) => Ordering::Equal,
        (Datum::Null, _) => Ordering::Less,
        (_, Datum::Null) => Ordering::Greater,
        (Datum::Int32(x), Datum::Int32(y)) => x.cmp(y),
        (Datum::Int64(x), Datum::Int64(y)) => x.cmp(y),
        (Datum::Int32(x), Datum::Int64(y)) => i64::from(*x).cmp(y),
        (Datum::Int64(x), Datum::Int32(y)) => x.cmp(&i64::from(*y)),
        (Datum::Float64(x), Datum::Float64(y)) => {
            x.partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        (Datum::Float64(x), Datum::Int64(y)) => {
            x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal)
        }
        (Datum::Int64(x), Datum::Float64(y)) => {
            (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        (Datum::Float64(x), Datum::Int32(y)) => {
            x.partial_cmp(&f64::from(*y)).unwrap_or(Ordering::Equal)
        }
        (Datum::Int32(x), Datum::Float64(y)) => {
            f64::from(*x).partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        (Datum::Text(x), Datum::Text(y)) => x.cmp(y),
        (Datum::Boolean(x), Datum::Boolean(y)) => x.cmp(y),
        (Datum::Timestamp(x), Datum::Timestamp(y)) => x.cmp(y),
        (Datum::Date(x), Datum::Date(y)) => x.cmp(y),
        (Datum::Time(x), Datum::Time(y)) => x.cmp(y),
        (Datum::Uuid(x), Datum::Uuid(y)) => x.cmp(y),
        (Datum::Decimal(mx, sx), Datum::Decimal(my, sy)) => {
            // Normalize to same scale before comparing mantissas.
            if sx == sy {
                mx.cmp(my)
            } else if sx > sy {
                let shift = 10i128.saturating_pow((*sx - *sy) as u32);
                mx.cmp(&(my.saturating_mul(shift)))
            } else {
                let shift = 10i128.saturating_pow((*sy - *sx) as u32);
                (mx.saturating_mul(shift)).cmp(my)
            }
        }
        _ => std::cmp::Ordering::Equal,
    }
}

/// Find the shard index for a value given sorted range boundaries.
/// Returns the 0-based shard index (0..=range_bounds.len()).
///
/// Boundary semantics (N-1 bounds for N shards):
/// - shard 0: value < range_bounds[0]
/// - shard i: range_bounds[i-1] <= value < range_bounds[i]
/// - shard N-1: value >= range_bounds[N-2]
fn range_shard_index(value: &Datum, range_bounds: &[Datum]) -> u64 {
    // Binary search: find first bound where value < bound.
    let mut lo = 0usize;
    let mut hi = range_bounds.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if cmp_datum_for_range(value, &range_bounds[mid]) == std::cmp::Ordering::Less {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo as u64
}

/// Determine the target shard for a row given the table schema and number of shards.
/// Handles all sharding policies:
/// - Hash: hash shard key columns → shard_id = hash % num_shards
/// - Range: compare first shard key column against range_bounds
/// - Reference: returns None (caller must replicate to all shards)
/// - None: falls back to PK hash or shard 0
pub fn target_shard_for_row(
    row: &OwnedRow,
    schema: &TableSchema,
    num_shards: u64,
) -> Option<ShardId> {
    use falcon_common::schema::ShardingPolicy;

    if num_shards == 0 {
        return Some(ShardId(0));
    }

    match schema.sharding_policy {
        ShardingPolicy::Reference => None, // replicate to all
        ShardingPolicy::Hash => {
            let key_cols = schema.effective_shard_key();
            let hash = compute_shard_hash(row, key_cols);
            Some(ShardId(hash % num_shards))
        }
        ShardingPolicy::Range => {
            let key_cols = schema.effective_shard_key();
            let first_col = *key_cols.first().unwrap_or(&0);
            let value = row.values.get(first_col).unwrap_or(&Datum::Null);
            let idx = range_shard_index(value, &schema.range_bounds);
            Some(ShardId(idx.min(num_shards - 1)))
        }
        ShardingPolicy::None => {
            // Fall back to PK-based hash if PK exists, else shard 0
            if schema.primary_key_columns.is_empty() {
                Some(ShardId(0))
            } else {
                let hash = compute_shard_hash(row, &schema.primary_key_columns);
                Some(ShardId(hash % num_shards))
            }
        }
    }
}

/// Determine the target shard from extracted shard key datums (for point queries).
/// Returns None for reference tables.
pub fn target_shard_from_datums(
    datums: &[&Datum],
    schema: &TableSchema,
    num_shards: u64,
) -> Option<ShardId> {
    use falcon_common::schema::ShardingPolicy;

    if num_shards == 0 {
        return Some(ShardId(0));
    }

    match schema.sharding_policy {
        ShardingPolicy::Reference => None,
        ShardingPolicy::Range => {
            let value = datums.first().map(|d| *d).unwrap_or(&Datum::Null);
            let idx = range_shard_index(value, &schema.range_bounds);
            Some(ShardId(idx.min(num_shards - 1)))
        }
        _ => {
            let hash = compute_shard_hash_from_datums(datums);
            Some(ShardId(hash % num_shards))
        }
    }
}

/// Determine the subset of shards that a range query [lower, upper] may touch.
/// Enables **shard pruning** for range-sharded tables: only scatter to shards
/// whose partition boundaries overlap with the query range.
///
/// - `lower`: inclusive lower bound (None = -∞)
/// - `upper`: inclusive upper bound (None = +∞)
///
/// For non-range-sharded tables, returns all shards (no pruning possible).
pub fn shards_for_range_query(
    schema: &TableSchema,
    num_shards: u64,
    lower: Option<&Datum>,
    upper: Option<&Datum>,
) -> Vec<ShardId> {
    use falcon_common::schema::ShardingPolicy;

    if num_shards == 0 {
        return vec![ShardId(0)];
    }

    if schema.sharding_policy != ShardingPolicy::Range || schema.range_bounds.is_empty() {
        return (0..num_shards).map(ShardId).collect();
    }

    let bounds = &schema.range_bounds;
    // Find the first shard that could contain `lower`.
    let start_shard = match lower {
        Some(lo) => range_shard_index(lo, bounds).min(num_shards - 1),
        None => 0,
    };
    // Find the last shard that could contain `upper`.
    let end_shard = match upper {
        Some(hi) => range_shard_index(hi, bounds).min(num_shards - 1),
        None => num_shards - 1,
    };

    (start_shard..=end_shard).map(ShardId).collect()
}

/// Determine all target shards for a table.
/// Reference tables → all shards.
/// Hash/None tables → all shards (for full scans; point queries use target_shard_for_row).
pub fn all_shards_for_table(_schema: &TableSchema, num_shards: u64) -> Vec<ShardId> {
    (0..num_shards).map(ShardId).collect()
}

/// Encode a Datum value into a byte buffer for hashing.
/// Uses a type tag + value encoding to avoid collisions across types.
fn encode_datum_for_hash(buf: &mut Vec<u8>, datum: &Datum) {
    match datum {
        Datum::Null => buf.push(0x00),
        Datum::Boolean(b) => {
            buf.push(0x01);
            buf.push(if *b { 1 } else { 0 });
        }
        Datum::Int32(v) => {
            buf.push(0x02);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Datum::Int64(v) => {
            buf.push(0x03);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Datum::Float64(v) => {
            buf.push(0x04);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Datum::Text(s) => {
            buf.push(0x05);
            buf.extend_from_slice(s.as_bytes());
            buf.push(0x00); // null terminator to avoid prefix collisions
        }
        Datum::Timestamp(v) => {
            buf.push(0x06);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Datum::Date(v) => {
            buf.push(0x07);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Datum::Array(elems) => {
            buf.push(0x08);
            for elem in elems {
                encode_datum_for_hash(buf, elem);
            }
            buf.push(0xFF); // array terminator
        }
        Datum::Jsonb(v) => {
            buf.push(0x09);
            let s = v.to_string();
            buf.extend_from_slice(s.as_bytes());
            buf.push(0x00);
        }
        Datum::Decimal(m, s) => {
            buf.push(0x0A);
            buf.push(*s);
            buf.extend_from_slice(&m.to_le_bytes());
        }
        Datum::Time(us) => {
            buf.push(0x0B);
            buf.extend_from_slice(&us.to_le_bytes());
        }
        Datum::Interval(mo, d, us) => {
            buf.push(0x0C);
            buf.extend_from_slice(&mo.to_le_bytes());
            buf.extend_from_slice(&d.to_le_bytes());
            buf.extend_from_slice(&us.to_le_bytes());
        }
        Datum::Uuid(v) => {
            buf.push(0x0D);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Datum::Bytea(bytes) => {
            buf.push(0x0E);
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Datum::TsVector(_) | Datum::TsQuery(_) => {
            buf.push(0x0F);
            let s = format!("{datum}");
            buf.extend_from_slice(s.as_bytes());
            buf.push(0x00);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::Datum;
    use falcon_common::schema::{ColumnDef, ShardingPolicy};
    use falcon_common::types::{ColumnId, DataType, TableId};

    // ── Range sharding helpers ────────────────────────────────────────────

    fn make_range_schema() -> TableSchema {
        TableSchema {
            id: TableId(3),
            name: "events".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "ts".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false, max_length: None,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "payload".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false, max_length: None,
                },
            ],
            primary_key_columns: vec![0],
            shard_key: vec![0], // range-shard on ts
            sharding_policy: ShardingPolicy::Range,
            // 3 boundaries → 4 shards: [−∞,100), [100,200), [200,300), [300,+∞)
            range_bounds: vec![Datum::Int64(100), Datum::Int64(200), Datum::Int64(300)],
            ..Default::default()
        }
    }

    #[test]
    fn test_range_shard_first_partition() {
        let schema = make_range_schema();
        let row = OwnedRow::new(vec![Datum::Int64(50), Datum::Text("a".into())]);
        let shard = target_shard_for_row(&row, &schema, 4);
        assert_eq!(shard, Some(ShardId(0)));
    }

    #[test]
    fn test_range_shard_middle_partition() {
        let schema = make_range_schema();
        let row = OwnedRow::new(vec![Datum::Int64(150), Datum::Text("b".into())]);
        let shard = target_shard_for_row(&row, &schema, 4);
        assert_eq!(shard, Some(ShardId(1)));
    }

    #[test]
    fn test_range_shard_boundary_exact() {
        let schema = make_range_schema();
        // Exact boundary value 200 → shard 2 (range_bounds[1] <= 200 < range_bounds[2])
        let row = OwnedRow::new(vec![Datum::Int64(200), Datum::Text("c".into())]);
        let shard = target_shard_for_row(&row, &schema, 4);
        assert_eq!(shard, Some(ShardId(2)));
    }

    #[test]
    fn test_range_shard_last_partition() {
        let schema = make_range_schema();
        let row = OwnedRow::new(vec![Datum::Int64(999), Datum::Text("d".into())]);
        let shard = target_shard_for_row(&row, &schema, 4);
        assert_eq!(shard, Some(ShardId(3)));
    }

    #[test]
    fn test_range_shard_from_datums() {
        let schema = make_range_schema();
        let d = Datum::Int64(150);
        let shard = target_shard_from_datums(&[&d], &schema, 4);
        assert_eq!(shard, Some(ShardId(1)));
    }

    #[test]
    fn test_range_shard_null_goes_to_first() {
        let schema = make_range_schema();
        let row = OwnedRow::new(vec![Datum::Null, Datum::Text("x".into())]);
        let shard = target_shard_for_row(&row, &schema, 4);
        assert_eq!(shard, Some(ShardId(0)), "NULL should route to first shard");
    }

    #[test]
    fn test_range_shard_text_boundaries() {
        let schema = TableSchema {
            id: TableId(4),
            name: "logs".to_string(),
            columns: vec![ColumnDef {
                id: ColumnId(0),
                name: "region".into(),
                data_type: DataType::Text,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false, max_length: None,
            }],
            primary_key_columns: vec![0],
            shard_key: vec![0],
            sharding_policy: ShardingPolicy::Range,
            range_bounds: vec![Datum::Text("eu".into()), Datum::Text("us".into())],
            ..Default::default()
        };
        let row_ap = OwnedRow::new(vec![Datum::Text("ap".into())]);
        let row_eu = OwnedRow::new(vec![Datum::Text("eu".into())]);
        let row_us = OwnedRow::new(vec![Datum::Text("us-west".into())]);
        assert_eq!(target_shard_for_row(&row_ap, &schema, 3), Some(ShardId(0)));
        assert_eq!(target_shard_for_row(&row_eu, &schema, 3), Some(ShardId(1)));
        assert_eq!(target_shard_for_row(&row_us, &schema, 3), Some(ShardId(2)));
    }

    #[test]
    fn test_shards_for_range_query_full() {
        let schema = make_range_schema();
        // Full scan → all 4 shards
        let shards = shards_for_range_query(&schema, 4, None, None);
        assert_eq!(shards.len(), 4);
    }

    #[test]
    fn test_shards_for_range_query_pruned() {
        let schema = make_range_schema();
        // Query: ts BETWEEN 150 AND 250 → shards 1 and 2
        let lo = Datum::Int64(150);
        let hi = Datum::Int64(250);
        let shards = shards_for_range_query(&schema, 4, Some(&lo), Some(&hi));
        assert_eq!(shards, vec![ShardId(1), ShardId(2)]);
    }

    #[test]
    fn test_shards_for_range_query_single_shard() {
        let schema = make_range_schema();
        // Query: ts BETWEEN 50 AND 99 → shard 0 only
        let lo = Datum::Int64(50);
        let hi = Datum::Int64(99);
        let shards = shards_for_range_query(&schema, 4, Some(&lo), Some(&hi));
        assert_eq!(shards, vec![ShardId(0)]);
    }

    #[test]
    fn test_shards_for_range_query_open_upper() {
        let schema = make_range_schema();
        // Query: ts >= 250 → shards 2, 3
        let lo = Datum::Int64(250);
        let shards = shards_for_range_query(&schema, 4, Some(&lo), None);
        assert_eq!(shards, vec![ShardId(2), ShardId(3)]);
    }

    #[test]
    fn test_shards_for_range_non_range_table() {
        let schema = make_hash_schema();
        // Hash-sharded tables get no pruning → all shards
        let lo = Datum::Int64(50);
        let shards = shards_for_range_query(&schema, 4, Some(&lo), None);
        assert_eq!(shards.len(), 4);
    }

    #[test]
    fn test_cmp_datum_cross_type() {
        assert_eq!(
            cmp_datum_for_range(&Datum::Int32(100), &Datum::Int64(100)),
            std::cmp::Ordering::Equal,
        );
        assert_eq!(
            cmp_datum_for_range(&Datum::Int32(99), &Datum::Int64(100)),
            std::cmp::Ordering::Less,
        );
    }

    fn make_hash_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "orders".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false, max_length: None,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "customer_id".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false, max_length: None,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "amount".into(),
                    data_type: DataType::Float64,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false, max_length: None,
                },
            ],
            primary_key_columns: vec![0],
            shard_key: vec![1], // shard by customer_id
            sharding_policy: ShardingPolicy::Hash,
            ..Default::default()
        }
    }

    fn make_reference_schema() -> TableSchema {
        TableSchema {
            id: TableId(2),
            name: "countries".to_string(),
            columns: vec![ColumnDef {
                id: ColumnId(0),
                name: "code".into(),
                data_type: DataType::Text,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false, max_length: None,
            }],
            primary_key_columns: vec![0],
            sharding_policy: ShardingPolicy::Reference,
            ..Default::default()
        }
    }

    #[test]
    fn test_hash_deterministic() {
        let row = OwnedRow::new(vec![
            Datum::Int64(42),
            Datum::Int64(100),
            Datum::Float64(99.99),
        ]);
        let h1 = compute_shard_hash(&row, &[1]);
        let h2 = compute_shard_hash(&row, &[1]);
        assert_eq!(h1, h2, "same input must produce same hash");
    }

    #[test]
    fn test_different_keys_different_hashes() {
        let r1 = OwnedRow::new(vec![Datum::Int64(1), Datum::Int64(100)]);
        let r2 = OwnedRow::new(vec![Datum::Int64(1), Datum::Int64(200)]);
        let h1 = compute_shard_hash(&r1, &[1]);
        let h2 = compute_shard_hash(&r2, &[1]);
        // Statistically should differ (not guaranteed but very likely)
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_multi_column_shard_key() {
        let row = OwnedRow::new(vec![
            Datum::Int64(1),
            Datum::Int64(100),
            Datum::Text("hello".into()),
        ]);
        let h_single = compute_shard_hash(&row, &[1]);
        let h_multi = compute_shard_hash(&row, &[1, 2]);
        assert_ne!(
            h_single, h_multi,
            "multi-col hash should differ from single-col"
        );
    }

    #[test]
    fn test_target_shard_hash() {
        let schema = make_hash_schema();
        let row = OwnedRow::new(vec![
            Datum::Int64(42),
            Datum::Int64(100),
            Datum::Float64(50.0),
        ]);
        let shard = target_shard_for_row(&row, &schema, 4);
        assert!(shard.is_some());
        assert!(shard.unwrap().0 < 4);
    }

    #[test]
    fn test_target_shard_reference_returns_none() {
        let schema = make_reference_schema();
        let row = OwnedRow::new(vec![Datum::Text("US".into())]);
        let shard = target_shard_for_row(&row, &schema, 4);
        assert!(shard.is_none(), "reference tables should return None");
    }

    #[test]
    fn test_effective_shard_key_falls_back_to_pk() {
        let mut schema = make_hash_schema();
        schema.shard_key = vec![]; // clear explicit shard key
        assert_eq!(schema.effective_shard_key(), &[0]); // falls back to PK
    }

    #[test]
    fn test_uniform_distribution() {
        // Insert 10000 rows with sequential customer_ids, check distribution across 4 shards
        let schema = make_hash_schema();
        let num_shards = 4u64;
        let mut counts = vec![0u64; num_shards as usize];

        for i in 0..10000i64 {
            let row = OwnedRow::new(vec![
                Datum::Int64(i),
                Datum::Int64(i), // customer_id = i
                Datum::Float64(0.0),
            ]);
            if let Some(shard) = target_shard_for_row(&row, &schema, num_shards) {
                counts[shard.0 as usize] += 1;
            }
        }

        // Each shard should have roughly 2500 rows (±30% tolerance)
        for (i, count) in counts.iter().enumerate() {
            assert!(
                *count > 1500 && *count < 3500,
                "shard {} has {} rows, expected ~2500",
                i,
                count
            );
        }
    }

    #[test]
    fn test_text_shard_key() {
        let row1 = OwnedRow::new(vec![Datum::Text("alice".into())]);
        let row2 = OwnedRow::new(vec![Datum::Text("bob".into())]);
        let h1 = compute_shard_hash(&row1, &[0]);
        let h2 = compute_shard_hash(&row2, &[0]);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_shard_from_datums() {
        let schema = make_hash_schema();
        let d = Datum::Int64(100);
        let shard = target_shard_from_datums(&[&d], &schema, 4);
        assert!(shard.is_some());
        assert!(shard.unwrap().0 < 4);
    }
}
