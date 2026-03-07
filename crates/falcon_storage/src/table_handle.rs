//! Unified table handle — one enum that wraps every storage engine variant.
//!
//! `StorageEngine` keeps a single `DashMap<TableId, TableHandle>` (`engine_tables`)
//! for O(1) dispatch. All DML/commit/abort operations route through `as_storage_table()`
//! which returns `&dyn StorageTable` for MVCC-capable engines.
//!
//! Adding a new engine:
//!   1. Add a variant here + impl `StorageTable`.
//!   2. Return `Some` from `as_storage_table()`.
//!   3. Wire DDL in `engine_ddl.rs`.

use std::sync::Arc;

use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{Timestamp, TxnId};

use crate::memtable::{MemTable, PrimaryKey};

/// One handle per table, carrying the engine-specific `Arc`.
pub enum TableHandle {
    Rowstore(Arc<MemTable>),
    #[cfg(feature = "columnstore")]
    Columnstore(Arc<crate::columnstore::ColumnStoreTable>),
    #[cfg(feature = "disk_rowstore")]
    DiskRowstore(Arc<crate::disk_rowstore::DiskRowstoreTable>),
    #[cfg(feature = "lsm")]
    Lsm(Arc<crate::lsm_table::LsmTable>),
    #[cfg(feature = "rocksdb")]
    RocksDb(Arc<crate::rocksdb_table::RocksDbTable>),
    #[cfg(feature = "redb")]
    Redb(Arc<crate::redb_table::RedbTable>),
}

impl TableHandle {
    pub fn schema(&self) -> &TableSchema {
        match self {
            TableHandle::Rowstore(t) => &t.schema,
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(t) => &t.schema,
            #[allow(unreachable_patterns)]
            _ => self
                .as_storage_table()
                .expect("unknown TableHandle variant")
                .schema(),
        }
    }

    /// Insert a row (MVCC Prepared state).
    pub fn insert(&self, row: &OwnedRow, txn_id: TxnId) -> Result<PrimaryKey, StorageError> {
        match self {
            TableHandle::Rowstore(t) => t.insert(row.clone(), txn_id),
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(t) => {
                let pk = crate::memtable::encode_pk(row, t.schema.pk_indices());
                let _ = t.insert(row.clone(), txn_id);
                Ok(pk)
            }
            #[allow(unreachable_patterns)]
            _ => self
                .as_storage_table()
                .ok_or(StorageError::TableNotFound(falcon_common::types::TableId(
                    0,
                )))
                .and_then(|t| t.insert(row, txn_id)),
        }
    }

    /// Update an existing row.
    pub fn update(
        &self,
        pk: &PrimaryKey,
        new_row: &OwnedRow,
        txn_id: TxnId,
    ) -> Result<(), StorageError> {
        match self {
            TableHandle::Rowstore(t) => t.update(pk, new_row.clone(), txn_id),
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(_) => Err(StorageError::Io(std::io::Error::other(
                "UPDATE not supported on COLUMNSTORE tables",
            ))),
            #[allow(unreachable_patterns)]
            _ => self
                .as_storage_table()
                .ok_or(StorageError::Io(std::io::Error::other(
                    "UPDATE not supported on this engine",
                )))
                .and_then(|t| t.update(pk, new_row, txn_id)),
        }
    }

    /// Delete a row (insert tombstone).
    pub fn delete(&self, pk: &PrimaryKey, txn_id: TxnId) -> Result<(), StorageError> {
        match self {
            TableHandle::Rowstore(t) => t.delete(pk, txn_id),
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(_) => Err(StorageError::Io(std::io::Error::other(
                "DELETE not supported on COLUMNSTORE tables",
            ))),
            #[allow(unreachable_patterns)]
            _ => self
                .as_storage_table()
                .ok_or(StorageError::Io(std::io::Error::other(
                    "DELETE not supported on this engine",
                )))
                .and_then(|t| t.delete(pk, txn_id)),
        }
    }

    /// Point get.
    pub fn get(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Option<OwnedRow>, StorageError> {
        match self {
            TableHandle::Rowstore(t) => Ok(t.get(pk, txn_id, read_ts)),
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(_) => Ok(None),
            #[allow(unreachable_patterns)]
            _ => self
                .as_storage_table()
                .map_or(Ok(None), |t| t.get(pk, txn_id, read_ts)),
        }
    }

    /// Full table scan returning (pk, row) pairs.
    pub fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        match self {
            TableHandle::Rowstore(t) => t.scan(txn_id, read_ts),
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(t) => t.scan(txn_id, read_ts),
            #[allow(unreachable_patterns)]
            _ => self
                .as_storage_table()
                .map_or_else(Vec::new, |t| t.scan(txn_id, read_ts)),
        }
    }

    /// Stream visible rows to a closure (zero-copy for Rowstore, scan-based for others).
    pub fn for_each_visible<F>(&self, txn_id: TxnId, read_ts: Timestamp, mut f: F)
    where
        F: FnMut(&OwnedRow),
    {
        match self {
            TableHandle::Rowstore(t) => t.for_each_visible(txn_id, read_ts, |r| f(r)),
            #[allow(unreachable_patterns)]
            _ => {
                if let Some(t) = self.as_storage_table() {
                    t.for_each_visible(txn_id, read_ts, &mut f);
                } else {
                    for (_, row) in self.scan(txn_id, read_ts) {
                        f(&row);
                    }
                }
            }
        }
    }

    /// Count visible rows.
    pub fn count_visible(&self, txn_id: TxnId, read_ts: Timestamp) -> usize {
        match self {
            TableHandle::Rowstore(t) => t.count_visible(txn_id, read_ts),
            #[allow(unreachable_patterns)]
            _ => self.as_storage_table().map_or_else(
                || self.scan(txn_id, read_ts).len(),
                |t| t.count_visible(txn_id, read_ts),
            ),
        }
    }

    /// Commit MVCC versions for the given keys.
    pub fn commit_key(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        match self {
            TableHandle::Rowstore(t) => t.commit_keys(txn_id, commit_ts, std::slice::from_ref(pk)),
            #[allow(unreachable_patterns)]
            _ => self
                .as_storage_table()
                .map_or(Ok(()), |t| t.commit_key(pk, txn_id, commit_ts)),
        }
    }

    /// Abort MVCC versions for the given key.
    pub fn abort_key(&self, pk: &PrimaryKey, txn_id: TxnId) {
        match self {
            TableHandle::Rowstore(t) => t.abort_keys(txn_id, std::slice::from_ref(pk)),
            #[allow(unreachable_patterns)]
            _ => {
                if let Some(t) = self.as_storage_table() {
                    t.abort_key(pk, txn_id);
                }
            }
        }
    }

    /// Returns the StorageTable trait object for engines that implement the trait.
    /// Only Rowstore and Columnstore return None (handled explicitly).
    #[allow(unreachable_patterns)]
    pub fn as_storage_table(&self) -> Option<&dyn crate::storage_trait::StorageTable> {
        match self {
            TableHandle::Rowstore(_) => None,
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(_) => None,
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(t) => Some(t.as_ref()),
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => Some(t.as_ref()),
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => Some(t.as_ref()),
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => Some(t.as_ref()),
        }
    }

    /// Returns true if this is an LSM-backed table (needs USTM tracking).
    pub fn is_lsm(&self) -> bool {
        #[cfg(feature = "lsm")]
        return matches!(self, TableHandle::Lsm(_));
        #[cfg(not(feature = "lsm"))]
        false
    }

    /// Batch insert for non-rowstore engines (no WAL/CDC/memory tracking).
    /// Returns None for Rowstore and Columnstore (need special handling).
    pub fn batch_insert_disk(
        &self,
        rows: Vec<OwnedRow>,
        txn_id: TxnId,
    ) -> Option<Result<Vec<crate::memtable::PrimaryKey>, StorageError>> {
        self.as_storage_table().map(|t| {
            let mut pks = Vec::with_capacity(rows.len());
            for row in &rows {
                pks.push(t.insert(row, txn_id)?);
            }
            Ok(pks)
        })
    }

    /// Batch commit for disk-backed engines. No-op for rowstore (handled separately).
    pub fn commit_keys_batch(
        &self,
        pks: &[PrimaryKey],
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        self.as_storage_table()
            .map_or(Ok(()), |t| t.commit_batch(pks, txn_id, commit_ts))
    }

    /// Batch abort for disk-backed engines. No-op for rowstore (handled separately).
    pub fn abort_keys_batch(&self, pks: &[PrimaryKey], txn_id: TxnId) {
        if let Some(t) = self.as_storage_table() {
            t.abort_batch(pks, txn_id);
        }
    }

    /// Returns true if this handle wraps an in-memory rowstore.
    pub fn is_rowstore(&self) -> bool {
        matches!(self, TableHandle::Rowstore(_))
    }

    /// Extract the inner `Arc<MemTable>` if this is a Rowstore handle.
    pub fn as_rowstore(&self) -> Option<&Arc<MemTable>> {
        match self {
            TableHandle::Rowstore(t) => Some(t),
            _ => None,
        }
    }

    #[cfg(feature = "columnstore")]
    pub fn as_columnstore(&self) -> Option<&Arc<crate::columnstore::ColumnStoreTable>> {
        match self {
            TableHandle::Columnstore(t) => Some(t),
            _ => None,
        }
    }

    /// Returns true if this is a disk rowstore table.
    pub fn is_disk_rowstore(&self) -> bool {
        #[cfg(feature = "disk_rowstore")]
        return matches!(self, TableHandle::DiskRowstore(_));
        #[cfg(not(feature = "disk_rowstore"))]
        false
    }

    /// Returns true if this is a columnstore table.
    pub fn is_columnstore(&self) -> bool {
        #[cfg(feature = "columnstore")]
        return matches!(self, TableHandle::Columnstore(_));
        #[cfg(not(feature = "columnstore"))]
        false
    }
}
