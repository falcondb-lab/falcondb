use std::collections::BTreeMap;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

use cedar_common::datum::{Datum, OwnedRow};
use cedar_common::schema::TableSchema;
use cedar_common::types::{TableId, Timestamp, TxnId};

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

/// A secondary index on a single column using a BTreeMap.
pub struct SecondaryIndex {
    pub column_idx: usize,
    pub unique: bool,
    pub tree: RwLock<BTreeMap<Vec<u8>, Vec<PrimaryKey>>>,
}

impl SecondaryIndex {
    pub fn new(column_idx: usize) -> Self {
        Self {
            column_idx,
            unique: false,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn new_unique(column_idx: usize) -> Self {
        Self {
            column_idx,
            unique: true,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn insert(&self, key_bytes: Vec<u8>, pk: PrimaryKey) {
        let mut tree = self.tree.write();
        tree.entry(key_bytes).or_default().push(pk);
    }

    /// Check uniqueness: if this index is unique and the key already maps to
    /// a different PK, return Err(DuplicateKey).
    pub fn check_unique(&self, key_bytes: &[u8], pk: &PrimaryKey) -> Result<(), cedar_common::error::StorageError> {
        if !self.unique {
            return Ok(());
        }
        let tree = self.tree.read();
        if let Some(pks) = tree.get(key_bytes) {
            if pks.iter().any(|existing| existing != pk) {
                return Err(cedar_common::error::StorageError::DuplicateKey);
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
    ) -> Result<PrimaryKey, cedar_common::error::StorageError> {
        let pk = encode_pk(&row, self.schema.pk_indices());

        // Check unique index constraints against committed index (read-only check)
        self.check_unique_indexes(&pk, &row)?;

        // Check for existing key (duplicate key detection)
        if let Some(chain) = self.data.get(&pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(cedar_common::error::StorageError::DuplicateKey);
            }
            if chain.has_live_version(txn_id) {
                return Err(cedar_common::error::StorageError::DuplicateKey);
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
    ) -> Result<(), cedar_common::error::StorageError> {
        if let Some(chain) = self.data.get(pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(cedar_common::error::StorageError::DuplicateKey);
            }
            chain.prepend(txn_id, Some(new_row));
            Ok(())
        } else {
            Err(cedar_common::error::StorageError::KeyNotFound)
        }
    }

    /// Delete a row by PK. Prepends a tombstone version.
    /// Indexes are NOT updated here — they are updated at commit time (方案A).
    pub fn delete(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
    ) -> Result<(), cedar_common::error::StorageError> {
        if let Some(chain) = self.data.get(pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(cedar_common::error::StorageError::DuplicateKey);
            }
            chain.prepend(txn_id, None); // tombstone
            Ok(())
        } else {
            Err(cedar_common::error::StorageError::KeyNotFound)
        }
    }

    /// Point read by PK for a specific transaction.
    pub fn get(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Option<OwnedRow> {
        self.data
            .get(pk)
            .and_then(|chain| chain.read_for_txn(txn_id, read_ts))
    }

    /// Full table scan visible to a transaction.
    pub fn scan(
        &self,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Vec<(PrimaryKey, OwnedRow)> {
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
    ) -> Result<(), cedar_common::error::StorageError> {
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
    pub fn has_committed_write_after(&self, pk: &PrimaryKey, exclude_txn: TxnId, after_ts: Timestamp) -> bool {
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
    ) -> Result<(), cedar_common::error::StorageError> {
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
                if let Some(datum) = new_row.get(idx.column_idx) {
                    let key_bytes = encode_column_value(datum);
                    // Check if another committed PK already owns this key
                    idx.check_unique(&key_bytes, pk).map_err(|_| {
                        cedar_common::error::StorageError::UniqueViolation {
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
    fn check_unique_indexes(&self, pk: &PrimaryKey, row: &OwnedRow) -> Result<(), cedar_common::error::StorageError> {
        let indexes = self.secondary_indexes.read();
        for idx in indexes.iter() {
            if idx.unique {
                if let Some(datum) = row.get(idx.column_idx) {
                    let key_bytes = encode_column_value(datum);
                    idx.check_unique(&key_bytes, pk)?;
                }
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
                if let Some(datum) = old_row.get(idx.column_idx) {
                    let key_bytes = encode_column_value(datum);
                    idx.remove(&key_bytes, pk);
                }
            }
        }
        // Add new index entries (if the new version is not a tombstone)
        if let Some(new_row) = new_data {
            for idx in indexes.iter() {
                if let Some(datum) = new_row.get(idx.column_idx) {
                    let key_bytes = encode_column_value(datum);
                    idx.insert(key_bytes, pk.clone());
                }
            }
        }
    }

    /// Add a row's indexed column values to all secondary indexes.
    /// Returns Err(DuplicateKey) if a unique index is violated.
    /// Used by backfill (create_index) and rebuild.
    #[allow(dead_code)]
    pub(crate) fn index_insert_row(&self, pk: &PrimaryKey, row: &OwnedRow) -> Result<(), cedar_common::error::StorageError> {
        let indexes = self.secondary_indexes.read();
        // Check unique constraints first (before mutating any index)
        for idx in indexes.iter() {
            if idx.unique {
                if let Some(datum) = row.get(idx.column_idx) {
                    let key_bytes = encode_column_value(datum);
                    idx.check_unique(&key_bytes, pk)?;
                }
            }
        }
        // All checks passed — insert into all indexes
        for idx in indexes.iter() {
            if let Some(datum) = row.get(idx.column_idx) {
                let key_bytes = encode_column_value(datum);
                idx.insert(key_bytes, pk.clone());
            }
        }
        Ok(())
    }

    /// Remove a row's indexed column values from all secondary indexes.
    #[allow(dead_code)]
    fn index_remove_row(&self, pk: &PrimaryKey, row: &OwnedRow) {
        let indexes = self.secondary_indexes.read();
        for idx in indexes.iter() {
            if let Some(datum) = row.get(idx.column_idx) {
                let key_bytes = encode_column_value(datum);
                idx.remove(&key_bytes, pk);
            }
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
                    if let Some(datum) = row.get(idx.column_idx) {
                        let key_bytes = encode_column_value(datum);
                        idx.insert(key_bytes, pk.clone());
                    }
                }
            }
        }
    }
}
