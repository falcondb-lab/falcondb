//! DML operations on StorageEngine: INSERT, UPDATE, DELETE, GET, SCAN, index scan.
//!
//! Each operation first checks the in-memory rowstore map, then the columnstore
//! map, then the disk-rowstore map.  This avoids coupling to the catalog's
//! `storage_type` at the DML layer — the table simply lives in one map.

use std::sync::atomic::Ordering as AtomicOrdering;

use falcon_common::config::NodeRole;
use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::types::{TableId, Timestamp, TxnId, TxnType};

use crate::memtable::PrimaryKey;
use crate::wal::WalRecord;

use super::engine::StorageEngine;

impl StorageEngine {
    // ── Core DML ─────────────────────────────────────────────────────

    pub fn insert(
        &self,
        table_id: TableId,
        row: OwnedRow,
        txn_id: TxnId,
    ) -> Result<PrimaryKey, StorageError> {
        // Rowstore (in-memory, default)
        if let Some(table) = self.tables.get(&table_id) {
            let row_bytes = crate::mvcc::estimate_row_bytes(&row);
            self.memory_tracker.alloc_write_buffer(row_bytes);
            self.append_wal(&WalRecord::Insert {
                txn_id,
                table_id,
                row: row.clone(),
            })?;
            let pk = table.insert(row, txn_id)?;
            self.record_write(txn_id, table_id, pk.clone());
            return Ok(pk);
        }

        // Columnstore
        if let Some(cs) = self.columnstore_tables.get(&table_id) {
            self.record_write_path_violation_columnstore()?;
            self.append_wal(&WalRecord::Insert {
                txn_id,
                table_id,
                row: row.clone(),
            })?;
            let pk = crate::memtable::encode_pk(&row, cs.schema.pk_indices());
            cs.insert(row, txn_id)?;
            return Ok(pk);
        }

        // Disk rowstore
        if let Some(disk) = self.disk_tables.get(&table_id) {
            self.record_write_path_violation_disk()?;
            self.append_wal(&WalRecord::Insert {
                txn_id,
                table_id,
                row: row.clone(),
            })?;
            let pk = disk.insert(row, txn_id)?;
            return Ok(pk);
        }

        // LSM rowstore
        if let Some(lsm) = self.lsm_tables.get(&table_id) {
            self.append_wal(&WalRecord::Insert {
                txn_id,
                table_id,
                row: row.clone(),
            })?;
            let pk = lsm.insert(&row, txn_id)?;
            self.record_write(txn_id, table_id, pk.clone());
            return Ok(pk);
        }

        Err(StorageError::TableNotFound(table_id))
    }

    pub fn update(
        &self,
        table_id: TableId,
        pk: &PrimaryKey,
        new_row: OwnedRow,
        txn_id: TxnId,
    ) -> Result<(), StorageError> {
        // Rowstore
        if let Some(table) = self.tables.get(&table_id) {
            let row_bytes = crate::mvcc::estimate_row_bytes(&new_row);
            self.memory_tracker.alloc_write_buffer(row_bytes);
            self.append_wal(&WalRecord::Update {
                txn_id,
                table_id,
                pk: pk.clone(),
                new_row: new_row.clone(),
            })?;
            table.update(pk, new_row, txn_id)?;
            self.record_write(txn_id, table_id, pk.clone());
            return Ok(());
        }

        // Disk rowstore
        if let Some(disk) = self.disk_tables.get(&table_id) {
            self.record_write_path_violation_disk()?;
            self.append_wal(&WalRecord::Update {
                txn_id,
                table_id,
                pk: pk.clone(),
                new_row: new_row.clone(),
            })?;
            disk.update(pk, new_row, txn_id)?;
            return Ok(());
        }

        // LSM rowstore
        if let Some(lsm) = self.lsm_tables.get(&table_id) {
            self.append_wal(&WalRecord::Update {
                txn_id,
                table_id,
                pk: pk.clone(),
                new_row: new_row.clone(),
            })?;
            lsm.update(pk, &new_row, txn_id)?;
            return Ok(());
        }

        // Columnstore: UPDATE not natively supported (analytics workload)
        if self.columnstore_tables.contains_key(&table_id) {
            self.record_write_path_violation_columnstore()?;
            return Err(StorageError::Io(std::io::Error::other(
                "UPDATE not supported on COLUMNSTORE tables",
            )));
        }

        Err(StorageError::TableNotFound(table_id))
    }

    pub fn delete(
        &self,
        table_id: TableId,
        pk: &PrimaryKey,
        txn_id: TxnId,
    ) -> Result<(), StorageError> {
        // Rowstore
        if let Some(table) = self.tables.get(&table_id) {
            let tombstone_bytes = std::mem::size_of::<crate::mvcc::Version>() as u64;
            self.memory_tracker.alloc_write_buffer(tombstone_bytes);
            self.append_wal(&WalRecord::Delete {
                txn_id,
                table_id,
                pk: pk.clone(),
            })?;
            table.delete(pk, txn_id)?;
            self.record_write(txn_id, table_id, pk.clone());
            return Ok(());
        }

        // Disk rowstore
        if let Some(disk) = self.disk_tables.get(&table_id) {
            self.record_write_path_violation_disk()?;
            self.append_wal(&WalRecord::Delete {
                txn_id,
                table_id,
                pk: pk.clone(),
            })?;
            disk.delete(pk, txn_id)?;
            return Ok(());
        }

        // LSM rowstore
        if let Some(lsm) = self.lsm_tables.get(&table_id) {
            self.append_wal(&WalRecord::Delete {
                txn_id,
                table_id,
                pk: pk.clone(),
            })?;
            lsm.delete(pk, txn_id)?;
            self.record_write(txn_id, table_id, pk.clone());
            return Ok(());
        }

        // Columnstore: DELETE not natively supported
        if self.columnstore_tables.contains_key(&table_id) {
            self.record_write_path_violation_columnstore()?;
            return Err(StorageError::Io(std::io::Error::other(
                "DELETE not supported on COLUMNSTORE tables",
            )));
        }

        Err(StorageError::TableNotFound(table_id))
    }

    pub fn get(
        &self,
        table_id: TableId,
        pk: &PrimaryKey,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Option<OwnedRow>, StorageError> {
        // Rowstore
        if let Some(table) = self.tables.get(&table_id) {
            self.record_read(txn_id, table_id, pk.clone());
            return Ok(table.get(pk, txn_id, read_ts));
        }

        // Disk rowstore
        if let Some(disk) = self.disk_tables.get(&table_id) {
            return Ok(disk.get(pk, txn_id, read_ts));
        }

        // Columnstore: point-get not efficient, return not found
        if self.columnstore_tables.contains_key(&table_id) {
            return Ok(None);
        }

        Err(StorageError::TableNotFound(table_id))
    }

    pub fn scan(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Vec<(PrimaryKey, OwnedRow)>, StorageError> {
        // Rowstore
        if let Some(table) = self.tables.get(&table_id) {
            let results = table.scan(txn_id, read_ts);
            for (pk, _) in &results {
                self.record_read(txn_id, table_id, pk.clone());
            }
            return Ok(results);
        }

        // Columnstore
        if let Some(cs) = self.columnstore_tables.get(&table_id) {
            let results = cs.scan(txn_id, read_ts);
            return Ok(results);
        }

        // Disk rowstore
        if let Some(disk) = self.disk_tables.get(&table_id) {
            let results = disk.scan(txn_id, read_ts);
            return Ok(results);
        }

        // LSM rowstore
        if let Some(lsm) = self.lsm_tables.get(&table_id) {
            let results = lsm.scan(txn_id, read_ts);
            return Ok(results);
        }

        Err(StorageError::TableNotFound(table_id))
    }

    /// Columnar scan: returns one Vec<Datum> per column for vectorized aggregate execution.
    /// Only available for ColumnStore tables; returns None for rowstore/disk tables.
    /// The executor uses this to bypass row-at-a-time deserialization for analytics queries.
    pub fn scan_columnar(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Option<Vec<Vec<falcon_common::datum::Datum>>> {
        let cs = self.columnstore_tables.get(&table_id)?;
        let num_cols = cs.schema.columns.len();
        let columns = (0..num_cols)
            .map(|col_idx| cs.column_scan(col_idx, txn_id, read_ts))
            .collect();
        Some(columns)
    }

    /// Perform an index scan: look up PKs via secondary index, then fetch rows.
    /// Returns (pk_bytes, row) pairs visible to the given txn.
    pub fn index_scan(
        &self,
        table_id: TableId,
        column_idx: usize,
        key: &[u8],
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, OwnedRow)>, StorageError> {
        let table = self
            .tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        // Look up PKs from the secondary index
        let pks = {
            let indexes = table.secondary_indexes.read();
            let mut found = Vec::new();
            for idx in indexes.iter() {
                if idx.column_idx == column_idx {
                    let tree = idx.tree.read();
                    if let Some(pk_list) = tree.get(key) {
                        found = pk_list.clone();
                    }
                    break;
                }
            }
            found
        };

        // Fetch visible rows for each PK
        let mut results = Vec::new();
        for pk in pks {
            if let Some(chain) = table.data.get(&pk) {
                let row = chain.read_for_txn(txn_id, read_ts);
                if let Some(r) = row {
                    // Record read for OCC
                    self.record_read(txn_id, table_id, pk.clone());
                    results.push((pk, r));
                }
            }
        }
        Ok(results)
    }

    /// Lookup primary keys via a secondary index on a table column.
    pub fn index_lookup(
        &self,
        table_id: TableId,
        column_idx: usize,
        key: &[u8],
    ) -> Result<Vec<PrimaryKey>, StorageError> {
        let table = self
            .tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        let indexes = table.secondary_indexes.read();
        for idx in indexes.iter() {
            if idx.column_idx == column_idx {
                let tree = idx.tree.read();
                return Ok(tree.get(key).cloned().unwrap_or_default());
            }
        }
        Ok(vec![])
    }

    // ── Auto-commit helpers (for internal migration / rebalancer) ───

    /// Insert a single row and immediately commit (local txn).
    /// Intended for internal operations like shard migration.
    pub fn insert_row(
        &self,
        table_id: TableId,
        _pk: PrimaryKey,
        row: OwnedRow,
    ) -> Result<PrimaryKey, StorageError> {
        let txn_id = self.next_internal_txn_id();
        let commit_ts = Timestamp(txn_id.0);
        let pk = self.insert(table_id, row, txn_id)?;
        self.commit_txn(txn_id, commit_ts, TxnType::Local)?;
        Ok(pk)
    }

    /// Delete a single row by primary key and immediately commit (local txn).
    /// Intended for internal operations like shard migration.
    pub fn delete_row(&self, table_id: TableId, pk: &PrimaryKey) -> Result<(), StorageError> {
        let txn_id = self.next_internal_txn_id();
        let commit_ts = Timestamp(txn_id.0);
        self.delete(table_id, pk, txn_id)?;
        self.commit_txn(txn_id, commit_ts, TxnType::Local)?;
        Ok(())
    }

    // ── OLTP write-path violation tracking ────────────────────────────

    /// Record that a write operation touched a columnstore table.
    ///
    /// Enforcement levels (configured via `write_path_enforcement`):
    /// - `Warn`     — log a warning, allow the write (backward-compatible default).
    /// - `FailFast` — return an error immediately; the caller must abort.
    /// - `HardDeny` — same as FailFast; the transaction is unconditionally rejected.
    ///
    /// Returns `Ok(())` when the write may proceed, `Err` when it must be rejected.
    pub(crate) fn record_write_path_violation_columnstore(&self) -> Result<(), StorageError> {
        use falcon_common::config::WritePathEnforcement;
        self.write_path_columnstore_violations
            .fetch_add(1, AtomicOrdering::Relaxed);
        match self.write_path_enforcement {
            WritePathEnforcement::Warn => {
                if self.node_role == NodeRole::Primary {
                    tracing::warn!(
                        "OLTP write-path violation: write touched COLUMNSTORE on Primary node \
                         (enforcement=warn, total={})",
                        self.write_path_columnstore_violations
                            .load(AtomicOrdering::Relaxed),
                    );
                }
                Ok(())
            }
            WritePathEnforcement::FailFast | WritePathEnforcement::HardDeny => {
                tracing::error!(
                    "OLTP write-path violation DENIED: write touched COLUMNSTORE on {:?} node \
                     (enforcement={:?})",
                    self.node_role,
                    self.write_path_enforcement,
                );
                Err(StorageError::Io(std::io::Error::other(
                    "write-path violation: COLUMNSTORE write forbidden on this node \
                     (enforcement=hard-deny)",
                )))
            }
        }
    }

    /// Record that a write operation touched a disk-rowstore table.
    ///
    /// Same enforcement semantics as `record_write_path_violation_columnstore`.
    pub(crate) fn record_write_path_violation_disk(&self) -> Result<(), StorageError> {
        use falcon_common::config::WritePathEnforcement;
        self.write_path_disk_violations
            .fetch_add(1, AtomicOrdering::Relaxed);
        match self.write_path_enforcement {
            WritePathEnforcement::Warn => {
                if self.node_role == NodeRole::Primary {
                    tracing::warn!(
                        "OLTP write-path violation: write touched DISK_ROWSTORE on Primary node \
                         (enforcement=warn, total={})",
                        self.write_path_disk_violations
                            .load(AtomicOrdering::Relaxed),
                    );
                }
                Ok(())
            }
            WritePathEnforcement::FailFast | WritePathEnforcement::HardDeny => {
                tracing::error!(
                    "OLTP write-path violation DENIED: write touched DISK_ROWSTORE on {:?} node \
                     (enforcement={:?})",
                    self.node_role,
                    self.write_path_enforcement,
                );
                Err(StorageError::Io(std::io::Error::other(
                    "write-path violation: DISK_ROWSTORE write forbidden on this node \
                     (enforcement=hard-deny)",
                )))
            }
        }
    }
}
