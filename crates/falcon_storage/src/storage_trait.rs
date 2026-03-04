use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{Timestamp, TxnId};

use crate::memtable::PrimaryKey;

/// Core DML contract every storage backend must implement.
pub trait StorageTable: Send + Sync {
    fn schema(&self) -> &TableSchema;
    fn insert(&self, row: &OwnedRow, txn_id: TxnId) -> Result<PrimaryKey, StorageError>;
    fn update(&self, pk: &PrimaryKey, new_row: &OwnedRow, txn_id: TxnId) -> Result<(), StorageError>;
    fn delete(&self, pk: &PrimaryKey, txn_id: TxnId) -> Result<(), StorageError>;
    fn get(&self, pk: &PrimaryKey, txn_id: TxnId, read_ts: Timestamp) -> Result<Option<OwnedRow>, StorageError>;
    fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)>;
    fn commit_key(&self, pk: &PrimaryKey, txn_id: TxnId, commit_ts: Timestamp) -> Result<(), StorageError>;
    fn abort_key(&self, pk: &PrimaryKey, txn_id: TxnId);
}
