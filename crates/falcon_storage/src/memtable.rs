//! # Module Status: PRODUCTION
//! In-memory row store — the primary storage engine for FalconDB OLTP.
//! This is the **only** production write path for row data.
//!
//! ## Golden Path (OLTP Write)
//! ```text
//! TxnManager.begin() → Executor DML
//!   → MemTable.insert / update / delete  [MVCC version chain]
//!   → TxnManager.commit()
//!     → OCC validation (read-set / write-set)
//!     → WAL append (WalRecord::InsertRow / UpdateRow / DeleteRow)
//!     → Version chain commit (set commit_ts on all written versions)
//! ```
//!
//! ## Prohibited Patterns
//! - Direct MemTable mutation without an active TxnId → violates MVCC
//! - Bypassing WAL append on commit → violates crash-safety
//! - Non-transactional reads that skip visibility checks → violates isolation

use std::collections::BTreeMap;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::TableSchema;
use falcon_common::types::{TableId, Timestamp, TxnId};

use crate::mvcc::VersionChain;

/// Hex-encode a byte slice for diagnostic/observability output.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Primary key encoded as a byte vector for hashing / comparison.
pub type PrimaryKey = Vec<u8>;

/// Encode a row's primary key columns into a comparable byte vector.
pub fn encode_pk(row: &OwnedRow, pk_indices: &[usize]) -> PrimaryKey {
    let mut buf = Vec::with_capacity(64);
    for &idx in pk_indices {
        if let Some(datum) = row.get(idx) {
            encode_datum_to_bytes(datum, &mut buf);
        }
    }
    buf
}

/// Encode datums from individual values (for WHERE pk = <value>).
pub fn encode_pk_from_datums(datums: &[&Datum]) -> PrimaryKey {
    let mut buf = Vec::with_capacity(64);
    for datum in datums {
        encode_datum_to_bytes(datum, &mut buf);
    }
    buf
}

/// Encode a single column value into bytes for secondary index keys.
pub fn encode_column_value(datum: &Datum) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_datum_to_bytes(datum, &mut buf);
    buf
}

fn encode_datum_to_bytes(datum: &Datum, buf: &mut Vec<u8>) {
    match datum {
        Datum::Null => {
            buf.push(0x00);
        }
        Datum::Boolean(b) => {
            buf.push(0x01);
            buf.push(if *b { 1 } else { 0 });
        }
        Datum::Int32(v) => {
            buf.push(0x02);
            // Encode as big-endian with sign flip for correct ordering
            let encoded = (*v as u32) ^ (1u32 << 31);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Int64(v) => {
            buf.push(0x03);
            let encoded = (*v as u64) ^ (1u64 << 63);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Float64(v) => {
            buf.push(0x04);
            let bits = v.to_bits();
            let encoded = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Text(s) => {
            buf.push(0x05);
            buf.extend_from_slice(s.as_bytes());
            buf.push(0x00); // null terminator for ordering
        }
        Datum::Timestamp(v) => {
            buf.push(0x06);
            let encoded = (*v as u64) ^ (1u64 << 63);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Date(v) => {
            buf.push(0x09);
            let encoded = (*v as u32) ^ (1u32 << 31);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Array(elems) => {
            buf.push(0x07);
            // Encode length then each element
            buf.extend_from_slice(&(elems.len() as u32).to_be_bytes());
            for elem in elems {
                encode_datum_to_bytes(elem, buf);
            }
        }
        Datum::Jsonb(v) => {
            buf.push(0x08);
            let s = v.to_string();
            buf.extend_from_slice(s.as_bytes());
            buf.push(0x00);
        }
        Datum::Decimal(mantissa, scale) => {
            buf.push(0x0A);
            buf.push(*scale);
            // Encode mantissa as big-endian i128 with sign flip for correct ordering
            let encoded = (*mantissa as u128) ^ (1u128 << 127);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Time(us) => {
            buf.push(0x0B);
            let encoded = (*us as u64) ^ (1u64 << 63);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Interval(months, days, us) => {
            buf.push(0x0C);
            let em = (*months as u32) ^ (1u32 << 31);
            let ed = (*days as u32) ^ (1u32 << 31);
            let eu = (*us as u64) ^ (1u64 << 63);
            buf.extend_from_slice(&em.to_be_bytes());
            buf.extend_from_slice(&ed.to_be_bytes());
            buf.extend_from_slice(&eu.to_be_bytes());
        }
        Datum::Uuid(v) => {
            buf.push(0x0D);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        Datum::Bytea(bytes) => {
            buf.push(0x0E);
            buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
    }
}

/// In-memory table: concurrent hash map of PK → VersionChain.
/// Also maintains secondary indexes.
pub struct MemTable {
    pub schema: TableSchema,
    /// Primary data: PK bytes → version chain.
    pub data: DashMap<PrimaryKey, Arc<VersionChain>>,
    /// Secondary B-tree indexes: column_index → (encoded_value → set of PKs).
    pub secondary_indexes: RwLock<Vec<SecondaryIndex>>,
}

/// A secondary index on one or more columns using a BTreeMap.
/// Single-column indexes use `column_idx`; composite indexes use `column_indices`.
pub struct SecondaryIndex {
    pub column_idx: usize,
    /// For composite indexes: ordered list of column indices.
    /// Empty means single-column index (use `column_idx`).
    pub column_indices: Vec<usize>,
    pub unique: bool,
    /// Optional list of column indices whose values are stored alongside the
    /// index key ("covering" / "INCLUDE" columns). When present, an index-only
    /// scan can return these values without touching the heap.
    pub covering_columns: Vec<usize>,
    /// For prefix indexes: max byte length of the indexed key prefix.
    /// 0 means full-value index (no prefix truncation).
    pub prefix_len: usize,
    pub tree: RwLock<BTreeMap<Vec<u8>, Vec<PrimaryKey>>>,
}

impl SecondaryIndex {
    pub fn new(column_idx: usize) -> Self {
        Self {
            column_idx,
            column_indices: vec![],
            unique: false,
            covering_columns: vec![],
            prefix_len: 0,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn new_unique(column_idx: usize) -> Self {
        Self {
            column_idx,
            column_indices: vec![],
            unique: true,
            covering_columns: vec![],
            prefix_len: 0,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a composite (multi-column) index.
    pub fn new_composite(column_indices: Vec<usize>, unique: bool) -> Self {
        let first = column_indices.first().copied().unwrap_or(0);
        Self {
            column_idx: first,
            column_indices,
            unique,
            covering_columns: vec![],
            prefix_len: 0,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a composite index with covering (INCLUDE) columns.
    pub fn new_covering(column_indices: Vec<usize>, covering: Vec<usize>, unique: bool) -> Self {
        let first = column_indices.first().copied().unwrap_or(0);
        Self {
            column_idx: first,
            column_indices,
            unique,
            covering_columns: covering,
            prefix_len: 0,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a prefix index (truncates key to prefix_len bytes).
    pub fn new_prefix(column_idx: usize, prefix_len: usize) -> Self {
        Self {
            column_idx,
            column_indices: vec![],
            unique: false, // prefix indexes cannot enforce uniqueness
            covering_columns: vec![],
            prefix_len,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    /// Returns true if this is a composite (multi-column) index.
    pub fn is_composite(&self) -> bool {
        self.column_indices.len() > 1
    }

    /// Returns the column indices this index covers (for key matching).
    pub fn key_columns(&self) -> &[usize] {
        if self.column_indices.is_empty() {
            std::slice::from_ref(&self.column_idx)
        } else {
            &self.column_indices
        }
    }

    /// Encode a composite key from a row.
    pub fn encode_key(&self, row: &OwnedRow) -> Vec<u8> {
        let cols = self.key_columns();
        let mut buf = Vec::new();
        for &col_idx in cols {
            let datum = row.get(col_idx).unwrap_or(&Datum::Null);
            encode_datum_to_bytes(datum, &mut buf);
        }
        if self.prefix_len > 0 && buf.len() > self.prefix_len {
            buf.truncate(self.prefix_len);
        }
        buf
    }

    pub fn insert(&self, key_bytes: Vec<u8>, pk: PrimaryKey) {
        let mut tree = self.tree.write();
        tree.entry(key_bytes).or_default().push(pk);
    }

    /// Check uniqueness: if this index is unique and the key already maps to
    /// a different PK, return Err(DuplicateKey).
    pub fn check_unique(
        &self,
        key_bytes: &[u8],
        pk: &PrimaryKey,
    ) -> Result<(), falcon_common::error::StorageError> {
        if !self.unique {
            return Ok(());
        }
        let tree = self.tree.read();
        if let Some(pks) = tree.get(key_bytes) {
            if pks.iter().any(|existing| existing != pk) {
                return Err(falcon_common::error::StorageError::DuplicateKey);
            }
        }
        Ok(())
    }

    pub fn remove(&self, key_bytes: &[u8], pk: &PrimaryKey) {
        let mut tree = self.tree.write();
        if let Some(pks) = tree.get_mut(key_bytes) {
            pks.retain(|p| p != pk);
            if pks.is_empty() {
                tree.remove(key_bytes);
            }
        }
    }

    /// Prefix scan: return all PKs whose key starts with the given prefix.
    /// Useful for composite index leftmost-prefix queries and prefix indexes.
    pub fn prefix_scan(&self, prefix: &[u8]) -> Vec<PrimaryKey> {
        let tree = self.tree.read();
        let mut result = Vec::new();
        // BTreeMap range scan from prefix to prefix+1
        let start = prefix.to_vec();
        let mut end = prefix.to_vec();
        // Increment the last byte to form an exclusive upper bound
        let mut carry = true;
        for byte in end.iter_mut().rev() {
            if carry {
                if *byte == 0xFF {
                    *byte = 0x00;
                } else {
                    *byte += 1;
                    carry = false;
                }
            }
        }
        if carry {
            // All 0xFF — scan to end
            for (k, pks) in tree.range(start..) {
                if k.starts_with(prefix) {
                    result.extend(pks.iter().cloned());
                } else {
                    break;
                }
            }
        } else {
            for (_k, pks) in tree.range(start..end) {
                result.extend(pks.iter().cloned());
            }
        }
        result
    }
}

impl MemTable {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            data: DashMap::new(),
            secondary_indexes: RwLock::new(Vec::new()),
        }
    }

    pub fn table_id(&self) -> TableId {
        self.schema.id
    }

    /// Insert a new row. Creates a new version chain or prepends to existing.
    /// Indexes are NOT updated here — they are updated at commit time (方案A).
    /// However, unique index constraints are checked eagerly against the committed index.
    pub fn insert(
        &self,
        row: OwnedRow,
        txn_id: TxnId,
    ) -> Result<PrimaryKey, falcon_common::error::StorageError> {
        let pk = encode_pk(&row, self.schema.pk_indices());

        // Check unique index constraints against committed index (read-only check)
        self.check_unique_indexes(&pk, &row)?;

        // Check for existing key (duplicate key detection)
        if let Some(chain) = self.data.get(&pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(falcon_common::error::StorageError::DuplicateKey);
            }
            if chain.has_live_version(txn_id) {
                return Err(falcon_common::error::StorageError::DuplicateKey);
            }
            chain.prepend(txn_id, Some(row));
        } else {
            let chain = Arc::new(VersionChain::new());
            chain.prepend(txn_id, Some(row));
            self.data.insert(pk.clone(), chain);
        }

        Ok(pk)
    }

    /// Update a row by PK. Prepends a new version.
    /// Indexes are NOT updated here — they are updated at commit time (方案A).
    pub fn update(
        &self,
        pk: &PrimaryKey,
        new_row: OwnedRow,
        txn_id: TxnId,
    ) -> Result<(), falcon_common::error::StorageError> {
        if let Some(chain) = self.data.get(pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(falcon_common::error::StorageError::DuplicateKey);
            }
            chain.prepend(txn_id, Some(new_row));
            Ok(())
        } else {
            Err(falcon_common::error::StorageError::KeyNotFound)
        }
    }

    /// Delete a row by PK. Prepends a tombstone version.
    /// Indexes are NOT updated here — they are updated at commit time (方案A).
    pub fn delete(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
    ) -> Result<(), falcon_common::error::StorageError> {
        if let Some(chain) = self.data.get(pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(falcon_common::error::StorageError::DuplicateKey);
            }
            chain.prepend(txn_id, None); // tombstone
            Ok(())
        } else {
            Err(falcon_common::error::StorageError::KeyNotFound)
        }
    }

    /// Point read by PK for a specific transaction.
    pub fn get(&self, pk: &PrimaryKey, txn_id: TxnId, read_ts: Timestamp) -> Option<OwnedRow> {
        self.data
            .get(pk)
            .and_then(|chain| chain.read_for_txn(txn_id, read_ts))
    }

    /// Full table scan visible to a transaction.
    pub fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        let mut results = Vec::new();
        for entry in self.data.iter() {
            if let Some(row) = entry.value().read_for_txn(txn_id, read_ts) {
                results.push((entry.key().clone(), row));
            }
        }
        results
    }

    /// Commit all writes by a transaction (legacy full-scan path).
    /// Also maintains secondary indexes at commit time (方案A).
    pub fn commit_txn(&self, txn_id: TxnId, commit_ts: Timestamp) {
        for entry in self.data.iter() {
            let pk = entry.key().clone();
            let (new_data, old_data) = entry.value().commit_and_report(txn_id, commit_ts);
            self.index_update_on_commit(&pk, new_data.as_ref(), old_data.as_ref());
        }
    }

    /// Commit writes for specific keys of a transaction.
    /// Performs **commit-time unique constraint re-validation** before applying
    /// MVCC commits and index updates (方案A).
    ///
    /// Atomicity: if any unique constraint is violated, NO writes are committed.
    /// Returns `Err(UniqueViolation)` if a concurrent committed txn created a
    /// conflicting index entry since the insert-time check.
    pub fn commit_keys(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
        keys: &[PrimaryKey],
    ) -> Result<(), falcon_common::error::StorageError> {
        // Phase 1: Validate unique constraints for all affected rows.
        // We must check BEFORE committing any version, so that a failure
        // leaves the data unchanged.
        self.validate_unique_constraints_for_commit(txn_id, keys)?;

        // Phase 2: All checks passed — apply MVCC commits + index updates.
        for pk in keys {
            if let Some(chain) = self.data.get(pk) {
                let (new_data, old_data) = chain.commit_and_report(txn_id, commit_ts);
                self.index_update_on_commit(pk, new_data.as_ref(), old_data.as_ref());
            }
        }
        Ok(())
    }

    /// Abort all writes by a transaction (legacy full-scan path).
    /// Under 方案A, indexes are not touched at DML time, so no index rollback needed.
    pub fn abort_txn(&self, txn_id: TxnId) {
        for entry in self.data.iter() {
            entry.value().abort_and_report(txn_id);
        }
    }

    /// Abort writes for specific keys of a transaction.
    /// Under 方案A, indexes are not touched at DML time, so no index rollback needed.
    pub fn abort_keys(&self, txn_id: TxnId, keys: &[PrimaryKey]) {
        for pk in keys {
            if let Some(chain) = self.data.get(pk) {
                chain.abort_and_report(txn_id);
            }
        }
    }

    /// Check if a key has a version committed after `after_ts` by another transaction.
    /// Used for OCC read-set validation.
    pub fn has_committed_write_after(
        &self,
        pk: &PrimaryKey,
        exclude_txn: TxnId,
        after_ts: Timestamp,
    ) -> bool {
        if let Some(chain) = self.data.get(pk) {
            chain.has_committed_write_after(exclude_txn, after_ts)
        } else {
            false
        }
    }

    /// Row count (approximate — counts all chains with at least one committed version).
    pub fn row_count_approx(&self) -> usize {
        self.data.len()
    }

    // ── Secondary index helpers ─────────────────────────────────────

    /// Commit-time unique constraint re-validation.
    /// For each key in the write-set that has an uncommitted insert/update,
    /// check all unique indexes to ensure no *other* committed PK holds the
    /// same index key. This catches concurrent-insert races that the eager
    /// insert-time check cannot detect under 方案A.
    fn validate_unique_constraints_for_commit(
        &self,
        txn_id: TxnId,
        keys: &[PrimaryKey],
    ) -> Result<(), falcon_common::error::StorageError> {
        let indexes = self.secondary_indexes.read();
        let has_unique = indexes.iter().any(|idx| idx.unique);
        if !has_unique {
            return Ok(());
        }

        for pk in keys {
            // Read the uncommitted row this txn is about to commit
            let uncommitted = if let Some(chain) = self.data.get(pk) {
                chain.read_uncommitted_for_txn(txn_id)
            } else {
                None
            };

            // Only check inserts/updates (Some(Some(row))), skip deletes (Some(None))
            let new_row = match uncommitted {
                Some(Some(row)) => row,
                _ => continue,
            };

            for idx in indexes.iter() {
                if !idx.unique {
                    continue;
                }
                {
                    let key_bytes = idx.encode_key(&new_row);
                    // Check if another committed PK already owns this key
                    idx.check_unique(&key_bytes, pk).map_err(|_| {
                        falcon_common::error::StorageError::UniqueViolation {
                            column_idx: idx.column_idx,
                            index_key_hex: hex_encode(&key_bytes),
                        }
                    })?;
                }
            }
        }
        Ok(())
    }

    /// Check unique index constraints against the committed index (read-only).
    /// Used at DML time to eagerly detect violations under 方案A.
    fn check_unique_indexes(
        &self,
        pk: &PrimaryKey,
        row: &OwnedRow,
    ) -> Result<(), falcon_common::error::StorageError> {
        let indexes = self.secondary_indexes.read();
        for idx in indexes.iter() {
            if idx.unique {
                let key_bytes = idx.encode_key(row);
                idx.check_unique(&key_bytes, pk)?;
            }
        }
        Ok(())
    }

    /// Update secondary indexes at commit time (方案A).
    /// Given the newly committed row data and the prior committed row data,
    /// remove old index entries and add new ones.
    fn index_update_on_commit(
        &self,
        pk: &PrimaryKey,
        new_data: Option<&OwnedRow>,
        old_data: Option<&OwnedRow>,
    ) {
        let indexes = self.secondary_indexes.read();
        if indexes.is_empty() {
            return;
        }
        // Remove old index entries (if there was a prior committed version)
        if let Some(old_row) = old_data {
            for idx in indexes.iter() {
                let key_bytes = idx.encode_key(old_row);
                idx.remove(&key_bytes, pk);
            }
        }
        // Add new index entries (if the new version is not a tombstone)
        if let Some(new_row) = new_data {
            for idx in indexes.iter() {
                let key_bytes = idx.encode_key(new_row);
                idx.insert(key_bytes, pk.clone());
            }
        }
    }

    /// Add a row's indexed column values to all secondary indexes.
    /// Returns Err(DuplicateKey) if a unique index is violated.
    /// Used by backfill (create_index) and rebuild.
    #[allow(dead_code)]
    pub(crate) fn index_insert_row(
        &self,
        pk: &PrimaryKey,
        row: &OwnedRow,
    ) -> Result<(), falcon_common::error::StorageError> {
        let indexes = self.secondary_indexes.read();
        // Check unique constraints first (before mutating any index)
        for idx in indexes.iter() {
            if idx.unique {
                let key_bytes = idx.encode_key(row);
                idx.check_unique(&key_bytes, pk)?;
            }
        }
        // All checks passed — insert into all indexes
        for idx in indexes.iter() {
            let key_bytes = idx.encode_key(row);
            idx.insert(key_bytes, pk.clone());
        }
        Ok(())
    }

    /// Remove a row's indexed column values from all secondary indexes.
    #[allow(dead_code)]
    fn index_remove_row(&self, pk: &PrimaryKey, row: &OwnedRow) {
        let indexes = self.secondary_indexes.read();
        for idx in indexes.iter() {
            let key_bytes = idx.encode_key(row);
            idx.remove(&key_bytes, pk);
        }
    }

    /// Rebuild all secondary indexes from current data.
    /// Used after WAL recovery to restore index state.
    pub fn rebuild_secondary_indexes(&self) {
        let indexes = self.secondary_indexes.read();
        // Clear all existing index entries
        for idx in indexes.iter() {
            let mut tree = idx.tree.write();
            tree.clear();
        }
        // Re-insert from current data
        for entry in self.data.iter() {
            let pk = entry.key().clone();
            let chain = entry.value();
            if let Some(row) = chain.read_latest() {
                for idx in indexes.iter() {
                    let key_bytes = idx.encode_key(&row);
                    idx.insert(key_bytes, pk.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod index_tests {
    use super::*;
    use falcon_common::datum::Datum;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{DataType, TableId, TxnId};

    fn col(name: &str, dt: DataType, pk: bool) -> ColumnDef {
        ColumnDef {
            id: falcon_common::types::ColumnId(0),
            name: name.to_string(),
            data_type: dt,
            nullable: !pk,
            is_primary_key: pk,
            default_value: None,
            is_serial: false,
        }
    }

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "test".to_string(),
            columns: vec![
                col("id", DataType::Int64, true),
                col("a", DataType::Int64, false),
                col("b", DataType::Text, false),
                col("c", DataType::Int64, false),
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        }
    }

    fn make_pk(id: i64) -> PrimaryKey {
        encode_column_value(&Datum::Int64(id))
    }

    fn make_row(id: i64, a: i64, b: &str, c: i64) -> OwnedRow {
        OwnedRow::new(vec![
            Datum::Int64(id),
            Datum::Int64(a),
            Datum::Text(b.to_string()),
            Datum::Int64(c),
        ])
    }

    #[test]
    fn test_composite_index_encode_key() {
        let idx = SecondaryIndex::new_composite(vec![1, 2], false);
        let row = make_row(1, 10, "hello", 20);
        let key = idx.encode_key(&row);
        assert!(!key.is_empty());

        // Different row with same (a, b) should produce same key
        let row2 = make_row(2, 10, "hello", 99);
        assert_eq!(idx.encode_key(&row2), key);

        // Different (a, b) should produce different key
        let row3 = make_row(3, 10, "world", 20);
        assert_ne!(idx.encode_key(&row3), key);
    }

    #[test]
    fn test_composite_index_insert_lookup() {
        let idx = SecondaryIndex::new_composite(vec![1, 2], false);
        let row1 = make_row(1, 10, "hello", 20);
        let row2 = make_row(2, 10, "hello", 30);
        let row3 = make_row(3, 20, "world", 40);

        let pk1 = make_pk(1);
        let pk2 = make_pk(2);
        let pk3 = make_pk(3);

        idx.insert(idx.encode_key(&row1), pk1.clone());
        idx.insert(idx.encode_key(&row2), pk2.clone());
        idx.insert(idx.encode_key(&row3), pk3.clone());

        // Lookup by exact composite key (a=10, b="hello")
        let key = idx.encode_key(&row1);
        let tree = idx.tree.read();
        let found = tree.get(&key).unwrap();
        assert_eq!(found.len(), 2);
        assert!(found.contains(&pk1));
        assert!(found.contains(&pk2));
    }

    #[test]
    fn test_composite_unique_index() {
        let idx = SecondaryIndex::new_composite(vec![1, 2], true);
        let row1 = make_row(1, 10, "hello", 20);
        let pk1 = make_pk(1);
        let pk2 = make_pk(2);

        idx.insert(idx.encode_key(&row1), pk1.clone());

        let key = idx.encode_key(&row1);
        assert!(idx.check_unique(&key, &pk2).is_err());
        assert!(idx.check_unique(&key, &pk1).is_ok());
    }

    #[test]
    fn test_prefix_index() {
        let idx = SecondaryIndex::new_prefix(2, 3);
        let row1 = make_row(1, 10, "hello", 20);
        let row2 = make_row(2, 10, "help", 30);
        let row3 = make_row(3, 10, "world", 40);

        let key1 = idx.encode_key(&row1);
        let key2 = idx.encode_key(&row2);
        let key3 = idx.encode_key(&row3);

        assert_eq!(key1.len(), 3);
        assert_eq!(key2.len(), 3);
        assert_eq!(key1, key2); // both truncated to same prefix
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_prefix_scan() {
        let idx = SecondaryIndex::new(1);
        let pk1 = make_pk(1);
        let pk2 = make_pk(2);

        let key1 = encode_column_value(&Datum::Int64(10));
        let key2 = encode_column_value(&Datum::Int64(20));

        idx.insert(key1.clone(), pk1.clone());
        idx.insert(key2.clone(), pk2.clone());

        let results = idx.prefix_scan(&key1);
        assert!(results.contains(&pk1));
        assert!(!results.contains(&pk2));
    }

    #[test]
    fn test_covering_index_metadata() {
        let idx = SecondaryIndex::new_covering(vec![1, 2], vec![3], false);
        assert!(idx.is_composite());
        assert_eq!(idx.key_columns(), &[1, 2]);
        assert_eq!(idx.covering_columns, vec![3]);
        assert!(!idx.unique);
    }

    #[test]
    fn test_single_column_key_columns() {
        let idx = SecondaryIndex::new(2);
        assert!(!idx.is_composite());
        assert_eq!(idx.key_columns(), &[2]);
    }

    #[test]
    fn test_composite_index_remove() {
        let idx = SecondaryIndex::new_composite(vec![1, 2], false);
        let row = make_row(1, 10, "hello", 20);
        let pk = make_pk(1);

        let key = idx.encode_key(&row);
        idx.insert(key.clone(), pk.clone());
        {
            let tree = idx.tree.read();
            assert!(tree.get(&key).is_some());
        }

        idx.remove(&key, &pk);
        {
            let tree = idx.tree.read();
            assert!(tree.get(&key).is_none());
        }
    }

    #[test]
    fn test_decimal_index_encoding() {
        let d1 = Datum::Decimal(12345, 2);
        let d2 = Datum::Decimal(12346, 2);
        let d3 = Datum::Decimal(-100, 2);

        let k1 = encode_column_value(&d1);
        let k2 = encode_column_value(&d2);
        let k3 = encode_column_value(&d3);

        assert!(k3 < k1);
        assert!(k1 < k2);
    }

    #[test]
    fn test_memtable_composite_index_commit() {
        let schema = test_schema();
        let mt = MemTable::new(schema);

        {
            let mut indexes = mt.secondary_indexes.write();
            indexes.push(SecondaryIndex::new_composite(vec![1, 2], false));
        }

        let txn = TxnId(100);
        let row = make_row(1, 10, "hello", 20);

        let pk = mt.insert(row.clone(), txn).unwrap();
        mt.commit_keys(txn, falcon_common::types::Timestamp(200), &[pk.clone()])
            .unwrap();

        let indexes = mt.secondary_indexes.read();
        let idx = &indexes[0];
        let key = idx.encode_key(&row);
        let tree = idx.tree.read();
        let pks = tree.get(&key).unwrap();
        assert!(pks.contains(&pk));
    }
}
