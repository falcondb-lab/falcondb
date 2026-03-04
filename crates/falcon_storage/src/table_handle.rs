//! Unified table handle — one enum that wraps every storage engine variant.
//!
//! `StorageEngine` keeps a `DashMap<TableId, TableHandle>` alongside the existing
//! per-engine maps.  DDL registers the new table in both places; DML uses the
//! unified map for O(1) dispatch without walking a chain of `if let Some(...)`.
//!
//! Adding a new engine requires:
//!   1. Add a variant here.
//!   2. Insert into `engine_tables` in `engine_ddl.rs` `create_table` /
//!      `truncate_table` / `drop_table`.
//!   3. Implement the dispatch arms in `engine_dml.rs` helper methods below.

use std::sync::Arc;

use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{Timestamp, TxnId};

use crate::memtable::{PrimaryKey, MemTable};

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
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(t) => &t.schema,
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => &t.schema,
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => &t.schema,
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => &t.schema,
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
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(t) => t.insert(row.clone(), txn_id),
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => t.insert(row, txn_id),
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => t.insert(row, txn_id),
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => t.insert(row, txn_id),
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
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(t) => t.update(pk, new_row.clone(), txn_id),
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => t.update(pk, new_row, txn_id),
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => t.update(pk, new_row, txn_id),
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => t.update(pk, new_row, txn_id),
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
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(t) => t.delete(pk, txn_id),
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => t.delete(pk, txn_id),
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => t.delete(pk, txn_id),
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => t.delete(pk, txn_id),
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
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(t) => Ok(t.get(pk, txn_id, read_ts)),
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => t.get(pk, txn_id, read_ts),
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => t.get(pk, txn_id, read_ts),
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => t.get(pk, txn_id, read_ts),
        }
    }

    /// Full table scan returning (pk, row) pairs.
    pub fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        match self {
            TableHandle::Rowstore(t) => t.scan(txn_id, read_ts),
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(t) => t.scan(txn_id, read_ts),
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(t) => t.scan(txn_id, read_ts),
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => t.scan(txn_id, read_ts),
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => t.scan(txn_id, read_ts),
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => t.scan(txn_id, read_ts),
        }
    }

    /// Stream visible rows to a closure (zero-copy for Rowstore, scan-based for others).
    pub fn for_each_visible<F>(&self, txn_id: TxnId, read_ts: Timestamp, mut f: F)
    where
        F: FnMut(&OwnedRow),
    {
        match self {
            TableHandle::Rowstore(t) => t.for_each_visible(txn_id, read_ts, |r| f(r)),
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => t.for_each_visible(txn_id, read_ts, |_, row| f(&row)),
            _ => {
                for (_, row) in self.scan(txn_id, read_ts) {
                    f(&row);
                }
            }
        }
    }

    /// Count visible rows.
    pub fn count_visible(&self, txn_id: TxnId, read_ts: Timestamp) -> usize {
        match self {
            TableHandle::Rowstore(t) => t.count_visible(txn_id, read_ts),
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => {
                let mut n = 0usize;
                t.for_each_visible(txn_id, read_ts, |_, _| n += 1);
                n
            }
            _ => self.scan(txn_id, read_ts).len(),
        }
    }

    /// Commit MVCC versions for the given keys.
    #[allow(unreachable_patterns)]
    pub fn commit_key(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        match self {
            TableHandle::Rowstore(t) => t.commit_keys(txn_id, commit_ts, &[pk.clone()]),
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => t.commit(pk, txn_id, commit_ts),
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => t.commit(pk, txn_id, commit_ts),
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => t.commit(pk, txn_id, commit_ts),
            _ => Ok(()),
        }
    }

    /// Abort MVCC versions for the given key.
    #[allow(unreachable_patterns)]
    pub fn abort_key(&self, pk: &PrimaryKey, txn_id: TxnId) {
        match self {
            TableHandle::Rowstore(t) => t.abort_keys(txn_id, &[pk.clone()]),
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => { let _ = t.abort(pk, txn_id); }
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => { let _ = t.abort(pk, txn_id); }
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => { let _ = t.abort(pk, txn_id); }
            _ => {}
        }
    }

    /// Batch insert for non-rowstore engines (no WAL/CDC/memory tracking).
    /// Returns None if this variant needs special handling (Rowstore, Columnstore, DiskRowstore).
    pub fn batch_insert_disk(
        &self,
        rows: Vec<OwnedRow>,
        txn_id: TxnId,
    ) -> Option<Result<Vec<crate::memtable::PrimaryKey>, StorageError>> {
        fn bulk<T: crate::storage_trait::StorageTable>(
            t: &T,
            rows: Vec<OwnedRow>,
            txn_id: TxnId,
        ) -> Result<Vec<crate::memtable::PrimaryKey>, StorageError> {
            let mut pks = Vec::with_capacity(rows.len());
            for row in &rows {
                pks.push(t.insert(row, txn_id)?);
            }
            Ok(pks)
        }

        match self {
            TableHandle::Rowstore(_) => None,
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(_) => None,
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(_) => None,
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => Some(bulk(t.as_ref(), rows, txn_id)),
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => Some(bulk(t.as_ref(), rows, txn_id)),
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => Some(bulk(t.as_ref(), rows, txn_id)),
        }
    }

    /// Batch commit for disk-backed engines. No-op for rowstore (handled separately).
    #[allow(unreachable_patterns)]
    pub fn commit_keys_batch(&self, pks: &[PrimaryKey], txn_id: TxnId, commit_ts: Timestamp) -> Result<(), StorageError> {
        match self {
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => t.commit_batch(pks, txn_id, commit_ts),
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => t.commit_batch(pks, txn_id, commit_ts),
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => {
                for pk in pks { t.commit(pk, txn_id, commit_ts)?; }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Batch abort for disk-backed engines. No-op for rowstore (handled separately).
    #[allow(unreachable_patterns)]
    pub fn abort_keys_batch(&self, pks: &[PrimaryKey], txn_id: TxnId) {
        match self {
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(t) => { let _ = t.abort_batch(pks, txn_id); }
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(t) => { let _ = t.abort_batch(pks, txn_id); }
            #[cfg(feature = "redb")]
            TableHandle::Redb(t) => {
                for pk in pks { let _ = t.abort(pk, txn_id); }
            }
            _ => {}
        }
    }

    /// Returns true if this handle wraps an in-memory rowstore.
    pub fn is_rowstore(&self) -> bool {
        matches!(self, TableHandle::Rowstore(_))
    }

    /// Returns true if this is a columnstore table.
    pub fn is_columnstore(&self) -> bool {
        #[cfg(feature = "columnstore")]
        return matches!(self, TableHandle::Columnstore(_));
        #[cfg(not(feature = "columnstore"))]
        false
    }
}
