//! # Module Status: STUB (feature-gated, default OFF)
//! Minimal type stubs so `StorageEngine` compiles without `columnstore` feature.

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::TableSchema;
use falcon_common::types::{Timestamp, TxnId};

use crate::memtable::PrimaryKey;

/// Stub — not available in v1.0 default build.
pub struct ColumnStoreTable {
    pub schema: TableSchema,
}

impl ColumnStoreTable {
    pub fn new(schema: TableSchema) -> Self {
        Self { schema }
    }
    pub fn insert(&self, _txn_id: TxnId, _row: OwnedRow) {
        unreachable!("columnstore feature not enabled");
    }
    pub fn scan(&self, _txn_id: TxnId, _read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        unreachable!("columnstore feature not enabled");
    }
    pub fn column_scan(&self, _col_idx: usize, _txn_id: TxnId, _read_ts: Timestamp) -> Vec<Datum> {
        unreachable!("columnstore feature not enabled");
    }
}
