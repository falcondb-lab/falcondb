//! LSM-backed row store table.
//!
//! Bridges the raw KV API of [`LsmEngine`] to the row-oriented table interface
//! used by the rest of the storage engine (insert/update/delete/scan).
//!
//! Key encoding: PK bytes (from `encode_pk`)
//! Value encoding: bincode-serialized `OwnedRow`

use std::sync::Arc;

use falcon_common::datum::OwnedRow;
use falcon_common::schema::TableSchema;
use falcon_common::types::{TableId, Timestamp, TxnId};

use falcon_common::error::StorageError;
use crate::lsm::engine::LsmEngine;
use crate::memtable::{encode_pk, PrimaryKey};

/// An LSM-tree backed row store table.
///
/// Each row is stored as a KV pair:
///   key   = PK bytes (order-preserving encoding from `encode_pk`)
///   value = bincode-serialized `OwnedRow`
///
/// MVCC visibility is not handled inside the LSM layer — the upper
/// `StorageEngine` manages version chains separately for rowstore tables.
/// For LsmRowstore the LSM layer acts as a durable write-ahead store;
/// reads use the in-memory MVCC chains for snapshot isolation.
pub struct LsmTable {
    pub schema: TableSchema,
    pub engine: Arc<LsmEngine>,
}

impl LsmTable {
    pub fn new(schema: TableSchema, engine: Arc<LsmEngine>) -> Self {
        Self { schema, engine }
    }

    pub fn table_id(&self) -> TableId {
        self.schema.id
    }

    /// Insert a row. Returns the encoded primary key.
    pub fn insert(&self, row: &OwnedRow, _txn_id: TxnId) -> Result<PrimaryKey, StorageError> {
        let pk = encode_pk(row, self.schema.pk_indices());
        let value = Self::serialize_row(row)?;
        self.engine.put(&pk, &value).map_err(StorageError::Io)?;
        Ok(pk)
    }

    /// Update a row identified by its PK.
    pub fn update(&self, pk: &PrimaryKey, new_row: &OwnedRow, _txn_id: TxnId) -> Result<(), StorageError> {
        let value = Self::serialize_row(new_row)?;
        self.engine.put(pk, &value).map_err(StorageError::Io)?;
        Ok(())
    }

    /// Delete a row identified by its PK (LSM tombstone).
    pub fn delete(&self, pk: &PrimaryKey, _txn_id: TxnId) -> Result<(), StorageError> {
        self.engine.delete(pk).map_err(StorageError::Io)?;
        Ok(())
    }

    /// Full table scan. Returns all live (non-tombstone) rows.
    ///
    /// Note: For a production system we would use a proper range iterator.
    /// This implementation reads the memtable snapshot, which is sufficient
    /// for the current single-node in-process architecture.
    pub fn scan(&self, _txn_id: TxnId, _read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        let mut results = Vec::new();
        // Read all entries from the active memtable
        let entries = {
            let active = self.engine.active_memtable_snapshot();
            active
        };
        for (key, value_opt, _seq) in entries {
            if let Some(value) = value_opt {
                if let Ok(row) = Self::deserialize_row(&value) {
                    results.push((key, row));
                }
            }
            // tombstone (None) → skip
        }
        results
    }

    /// Point lookup by PK.
    pub fn get(&self, pk: &PrimaryKey) -> Result<Option<OwnedRow>, StorageError> {
        let result = self.engine.get(pk).map_err(StorageError::Io)?;
        match result {
            Some(bytes) => Ok(Some(Self::deserialize_row(&bytes)?)),
            None => Ok(None),
        }
    }

    // ── Serialization ──

    fn serialize_row(row: &OwnedRow) -> Result<Vec<u8>, StorageError> {
        bincode::serialize(row)
            .map_err(|e| StorageError::Serialization(e.to_string()))
    }

    fn deserialize_row(bytes: &[u8]) -> Result<OwnedRow, StorageError> {
        bincode::deserialize(bytes)
            .map_err(|e| StorageError::Serialization(e.to_string()))
    }
}
