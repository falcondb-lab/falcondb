use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{TableId, Timestamp, TxnId};

use crate::memtable::PrimaryKey;
use crate::ustm::UstmEngine;

/// Core DML contract every storage backend must implement.
pub trait StorageTable: Send + Sync {
    fn schema(&self) -> &TableSchema;
    fn insert(&self, row: &OwnedRow, txn_id: TxnId) -> Result<PrimaryKey, StorageError>;
    fn update(
        &self,
        pk: &PrimaryKey,
        new_row: &OwnedRow,
        txn_id: TxnId,
    ) -> Result<(), StorageError>;
    fn delete(&self, pk: &PrimaryKey, txn_id: TxnId) -> Result<(), StorageError>;
    fn get(
        &self,
        pk: &[u8],
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Option<OwnedRow>, StorageError>;
    fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)>;
    fn commit_key(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError>;
    fn abort_key(&self, pk: &PrimaryKey, txn_id: TxnId);

    fn commit_batch(
        &self,
        pks: &[PrimaryKey],
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        for pk in pks {
            self.commit_key(pk, txn_id, commit_ts)?;
        }
        Ok(())
    }
    fn abort_batch(&self, pks: &[PrimaryKey], txn_id: TxnId) {
        for pk in pks {
            self.abort_key(pk, txn_id);
        }
    }
    fn for_each_visible(&self, txn_id: TxnId, read_ts: Timestamp, f: &mut dyn FnMut(&OwnedRow)) {
        for (_, row) in self.scan(txn_id, read_ts) {
            f(&row);
        }
    }
    fn count_visible(&self, txn_id: TxnId, read_ts: Timestamp) -> usize {
        self.scan(txn_id, read_ts).len()
    }

    /// USTM warm hint after write/read. Default: no-op. Disk-backed engines override.
    fn prefetch_hint(&self, _table_id: TableId, _pk: &[u8], _ustm: &UstmEngine) {}
    /// USTM evict hint after delete. Default: no-op.
    fn prefetch_evict(&self, _table_id: TableId, _pk: &[u8], _ustm: &UstmEngine) {}
    /// USTM scan prefetch hint after full scan. Default: no-op.
    fn scan_prefetch_hint(&self, _table_id: TableId, _row_count: usize, _ustm: &UstmEngine) {}
}
