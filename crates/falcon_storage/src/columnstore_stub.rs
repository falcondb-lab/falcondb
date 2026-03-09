//! Stub (feature-gated, default OFF).
//! Minimal type stubs so the crate compiles without `columnstore` feature.

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::StorageError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{Timestamp, TxnId};

pub struct ColumnStoreTable {
    pub schema: TableSchema,
}

impl ColumnStoreTable {
    pub const fn new(schema: TableSchema) -> Self {
        Self { schema }
    }
    pub fn insert(&self, _row: OwnedRow, _txn_id: TxnId) -> Result<(), StorageError> {
        unreachable!("columnstore feature not enabled");
    }
    pub fn insert_batch(&self, _rows: Vec<OwnedRow>, _txn_id: TxnId) -> Result<usize, StorageError> {
        unreachable!("columnstore feature not enabled");
    }
    pub fn commit(&self, _txn_id: TxnId, _commit_ts: Timestamp) {
        unreachable!("columnstore feature not enabled");
    }
    pub fn abort(&self, _txn_id: TxnId) {
        unreachable!("columnstore feature not enabled");
    }
    pub fn freeze_buffer(&self) {
        unreachable!("columnstore feature not enabled");
    }
    pub fn get(&self, _pk: &[u8], _txn_id: TxnId, _read_ts: Timestamp) -> Option<OwnedRow> {
        unreachable!("columnstore feature not enabled");
    }
    pub fn delete_by_pk(&self, _pk: &[u8], _txn_id: TxnId) -> bool {
        unreachable!("columnstore feature not enabled");
    }
    pub fn delete_row(&self, _seg_seq: u64, _row_idx: usize) {
        unreachable!("columnstore feature not enabled");
    }
    pub fn scan(&self, _txn_id: TxnId, _read_ts: Timestamp) -> Vec<(Vec<u8>, OwnedRow)> {
        unreachable!("columnstore feature not enabled");
    }
    pub fn column_scan(&self, _col_idx: usize, _txn_id: TxnId, _read_ts: Timestamp) -> Vec<Datum> {
        unreachable!("columnstore feature not enabled");
    }
    pub fn scan_columnar_projected(
        &self, _col_indices: &[usize], _predicate: Option<&()>,
        _txn_id: TxnId, _read_ts: Timestamp,
    ) -> Vec<Vec<Datum>> {
        unreachable!("columnstore feature not enabled");
    }
    pub fn row_count_approx(&self) -> usize {
        unreachable!("columnstore feature not enabled");
    }
    pub fn segment_count(&self) -> usize {
        unreachable!("columnstore feature not enabled");
    }
}
