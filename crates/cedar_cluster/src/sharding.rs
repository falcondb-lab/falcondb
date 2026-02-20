//! Core sharding utilities: multi-column shard key hashing, shard routing,
//! and shard-aware DDL/DML dispatch.
//!
//! # Design (SingleStore-inspired)
//!
//! - **Hash sharding**: rows are assigned to shards by hashing the shard key
//!   columns (or PK if no explicit shard key) with xxHash3.
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

use cedar_common::datum::{Datum, OwnedRow};
use cedar_common::schema::TableSchema;
use cedar_common::types::ShardId;
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

/// Determine the target shard for a row given the table schema and number of shards.
/// Handles all sharding policies:
/// - Hash: hash shard key columns → shard_id = hash % num_shards
/// - Reference: returns None (caller must replicate to all shards)
/// - None: falls back to PK hash or shard 0
pub fn target_shard_for_row(
    row: &OwnedRow,
    schema: &TableSchema,
    num_shards: u64,
) -> Option<ShardId> {
    use cedar_common::schema::ShardingPolicy;

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
    use cedar_common::schema::ShardingPolicy;

    if num_shards == 0 {
        return Some(ShardId(0));
    }

    match schema.sharding_policy {
        ShardingPolicy::Reference => None,
        _ => {
            let hash = compute_shard_hash_from_datums(datums);
            Some(ShardId(hash % num_shards))
        }
    }
}

/// Determine all target shards for a table.
/// Reference tables → all shards.
/// Hash/None tables → all shards (for full scans; point queries use target_shard_for_row).
pub fn all_shards_for_table(
    _schema: &TableSchema,
    num_shards: u64,
) -> Vec<ShardId> {
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
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use cedar_common::datum::Datum;
    use cedar_common::schema::{ColumnDef, ShardingPolicy};
    use cedar_common::types::{ColumnId, DataType, TableId};

    fn make_hash_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "orders".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0), name: "id".into(),
                    data_type: DataType::Int64, nullable: false,
                    is_primary_key: true, default_value: None, is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1), name: "customer_id".into(),
                    data_type: DataType::Int64, nullable: false,
                    is_primary_key: false, default_value: None, is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2), name: "amount".into(),
                    data_type: DataType::Float64, nullable: true,
                    is_primary_key: false, default_value: None, is_serial: false,
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
            columns: vec![
                ColumnDef {
                    id: ColumnId(0), name: "code".into(),
                    data_type: DataType::Text, nullable: false,
                    is_primary_key: true, default_value: None, is_serial: false,
                },
            ],
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
        assert_ne!(h_single, h_multi, "multi-col hash should differ from single-col");
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
                i, count
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
