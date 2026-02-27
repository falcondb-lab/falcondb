//! DML operations on StorageEngine: INSERT, UPDATE, DELETE, GET, SCAN, index scan.
//!
//! Each operation first checks the in-memory rowstore map, then the columnstore
//! map, then the disk-rowstore map.  This avoids coupling to the catalog's
//! `storage_type` at the DML layer — the table simply lives in one map.

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use falcon_common::config::NodeRole;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::StorageError;
use falcon_common::types::{TableId, Timestamp, TxnId, TxnType};

use crate::memtable::{PrimaryKey, SimpleAggOp};
use crate::ustm::{AccessPriority, PageData, PageId};
use crate::wal::WalRecord;

use super::engine::StorageEngine;

/// Derive a USTM PageId from a table id and primary key hash.
#[inline]
fn ustm_page_id(table_id: TableId, pk: &PrimaryKey) -> PageId {
    // Combine table_id (upper 32 bits) with a hash of the PK (lower 32 bits)
    let pk_hash = crate::ustm::page::fast_hash_pk(pk);
    PageId(table_id.0 << 32 | u64::from(pk_hash))
}

/// Derive a USTM PageId for a table-level page (DDL / scan).
#[inline]
const fn ustm_table_page_id(table_id: TableId, page_seq: u32) -> PageId {
    PageId(table_id.0 << 32 | page_seq as u64)
}

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

            // USTM: track the written row in the Hot zone for cache warming.
            let page_id = ustm_page_id(table_id, &pk);
            let data = PageData::new(pk.clone());
            let _ = self.ustm.alloc_hot(page_id, data, AccessPriority::HotRow);

            return Ok(pk);
        }

        // Columnstore (feature-gated)
        if let Some(_cs) = self.columnstore_tables.get(&table_id) {
            self.record_write_path_violation_columnstore()?;
            self.append_wal(&WalRecord::Insert {
                txn_id,
                table_id,
                row: row.clone(),
            })?;
            let pk = crate::memtable::encode_pk(&row, _cs.schema.pk_indices());
            _cs.insert(txn_id, row);
            return Ok(pk);
        }

        // Disk rowstore (feature-gated)
        #[cfg(feature = "disk_rowstore")]
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

        // LSM rowstore (feature-gated)
        #[cfg(feature = "lsm")]
        if let Some(lsm) = self.lsm_tables.get(&table_id) {
            self.append_wal(&WalRecord::Insert {
                txn_id,
                table_id,
                row: row.clone(),
            })?;
            let pk = lsm.insert(&row, txn_id)?;
            self.record_write(txn_id, table_id, pk.clone());

            // USTM: LSM writes go to Warm zone (data lives on disk, not Hot DRAM).
            let page_id = ustm_page_id(table_id, &pk);
            let data = PageData::new(pk.clone());
            self.ustm.insert_warm(page_id, data, AccessPriority::HotRow);

            return Ok(pk);
        }

        Err(StorageError::TableNotFound(table_id))
    }

    /// Batch insert multiple rows with a single WAL record.
    /// Much faster than calling `insert()` in a loop for multi-row INSERT.
    /// Only supports the default rowstore path.
    pub fn batch_insert(
        &self,
        table_id: TableId,
        rows: Vec<OwnedRow>,
        txn_id: TxnId,
    ) -> Result<Vec<PrimaryKey>, StorageError> {
        if rows.is_empty() {
            return Ok(vec![]);
        }

        // LSM rowstore (feature-gated) — fall back to per-row insert
        #[cfg(feature = "lsm")]
        if let Some(lsm) = self.lsm_tables.get(&table_id) {
            let row_count = rows.len();
            let wal_record = WalRecord::BatchInsert { txn_id, table_id, rows };
            self.append_wal(&wal_record)?;
            let rows = match wal_record {
                WalRecord::BatchInsert { rows, .. } => rows,
                _ => unreachable!(),
            };
            let mut pks = Vec::with_capacity(row_count);
            for row in rows {
                let pk = lsm.insert(&row, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                pks.push(pk);
            }
            return Ok(pks);
        }

        let table = self
            .tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        // Batch memory tracking
        let total_bytes: u64 = rows.iter().map(crate::mvcc::estimate_row_bytes).sum();
        self.memory_tracker.alloc_write_buffer(total_bytes);

        // Pre-track bytes so commit can skip estimate_write_set_bytes re-scan
        self.txn_write_bytes
            .entry(txn_id)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(total_bytes, AtomicOrdering::Relaxed);

        // Single WAL record for all rows — move rows in, serialize, then take back.
        // This avoids an expensive O(rows) clone.
        let row_count = rows.len();
        let wal_record = WalRecord::BatchInsert { txn_id, table_id, rows };
        self.append_wal(&wal_record)?;
        let rows = match wal_record {
            WalRecord::BatchInsert { rows, .. } => rows,
            _ => unreachable!(),
        };

        // Insert rows into memtable and collect PKs
        let mut pks = Vec::with_capacity(row_count);
        for row in rows {
            let pk = table.insert(row, txn_id)?;
            self.record_write_no_dedup(txn_id, table_id, pk.clone());
            pks.push(pk);
        }

        Ok(pks)
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

        // Disk rowstore (feature-gated)
        #[cfg(feature = "disk_rowstore")]
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

        // LSM rowstore (feature-gated)
        #[cfg(feature = "lsm")]
        if let Some(lsm) = self.lsm_tables.get(&table_id) {
            self.append_wal(&WalRecord::Update {
                txn_id,
                table_id,
                pk: pk.clone(),
                new_row: new_row.clone(),
            })?;
            lsm.update(pk, &new_row, txn_id)?;

            // USTM: refresh Warm zone entry after update.
            let page_id = ustm_page_id(table_id, pk);
            let data = PageData::new(pk.clone());
            self.ustm.insert_warm(page_id, data, AccessPriority::HotRow);

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

        // Disk rowstore (feature-gated)
        #[cfg(feature = "disk_rowstore")]
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

        // LSM rowstore (feature-gated)
        #[cfg(feature = "lsm")]
        if let Some(lsm) = self.lsm_tables.get(&table_id) {
            self.append_wal(&WalRecord::Delete {
                txn_id,
                table_id,
                pk: pk.clone(),
            })?;
            lsm.delete(pk, txn_id)?;
            self.record_write(txn_id, table_id, pk.clone());

            // USTM: evict the deleted page from Warm zone.
            let page_id = ustm_page_id(table_id, pk);
            self.ustm.unregister_page(page_id);

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

            // USTM: record page access in Warm zone for LIRS-2 tracking.
            let page_id = ustm_page_id(table_id, pk);
            let data = PageData::new(pk.clone());
            self.ustm.insert_warm(page_id, data, AccessPriority::HotRow);

            return Ok(table.get(pk, txn_id, read_ts));
        }

        // Disk rowstore (feature-gated)
        #[cfg(feature = "disk_rowstore")]
        if let Some(disk) = self.disk_tables.get(&table_id) {
            return Ok(disk.get(pk, txn_id, read_ts));
        }

        // LSM rowstore (feature-gated)
        #[cfg(feature = "lsm")]
        if let Some(lsm) = self.lsm_tables.get(&table_id) {
            self.record_read(txn_id, table_id, pk.clone());

            // USTM: LSM point-get — check Warm zone first, then fall through to LSM.
            // The Warm zone acts as a read cache for recently accessed LSM rows.
            let page_id = ustm_page_id(table_id, pk);
            let result = lsm.get(pk)?;

            // Cache the access in Warm zone for LIRS-2 tracking.
            let data = PageData::new(pk.clone());
            self.ustm.insert_warm(page_id, data, AccessPriority::HotRow);

            return Ok(result);
        }

        // Columnstore: point-get not efficient, return not found
        if self.columnstore_tables.contains_key(&table_id) {
            return Ok(None);
        }

        Err(StorageError::TableNotFound(table_id))
    }

    /// Count visible rows without materializing any row data.
    /// Used for fast COUNT(*) without WHERE clause.
    pub fn count_visible(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<usize, StorageError> {
        if let Some(table) = self.tables.get(&table_id) {
            return Ok(table.count_visible(txn_id, read_ts));
        }
        Err(StorageError::TableNotFound(table_id))
    }

    /// Scan top-K rows ordered by PK. Only materializes K rows instead of all N.
    /// Used for ORDER BY <pk_col> [ASC|DESC] LIMIT K.
    pub fn scan_top_k_by_pk(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
        k: usize,
        ascending: bool,
    ) -> Result<Vec<(PrimaryKey, OwnedRow)>, StorageError> {
        if let Some(table) = self.tables.get(&table_id) {
            return Ok(table.scan_top_k_by_pk(txn_id, read_ts, k, ascending));
        }
        Err(StorageError::TableNotFound(table_id))
    }

    /// Compute simple aggregates in a streaming pass without materializing rows.
    pub fn compute_simple_aggs(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
        agg_specs: &[(SimpleAggOp, Option<usize>)],
    ) -> Result<Vec<Datum>, StorageError> {
        if let Some(table) = self.tables.get(&table_id) {
            return Ok(table.compute_simple_aggs(txn_id, read_ts, agg_specs));
        }
        Err(StorageError::TableNotFound(table_id))
    }

    /// Scan with an inline predicate: only materializes rows that pass `predicate`.
    /// For selective queries (e.g. WHERE id = 5 on a large table), this avoids
    /// cloning the vast majority of rows that would be discarded post-scan.
    /// `predicate` receives `&OwnedRow` and returns true if the row should be kept.
    /// Optional `limit` stops scanning after collecting enough rows.
    pub fn scan_with_filter<F>(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
        predicate: F,
        limit: Option<usize>,
    ) -> Result<Vec<(PrimaryKey, OwnedRow)>, StorageError>
    where
        F: Fn(&OwnedRow) -> bool,
    {
        if let Some(table) = self.tables.get(&table_id) {
            let mut results = Vec::new();
            for entry in table.data.iter() {
                if let Some(lim) = limit {
                    if results.len() >= lim {
                        break;
                    }
                }
                let pk = entry.key().clone();
                entry.value().with_visible_data(txn_id, read_ts, |row| {
                    if predicate(row) {
                        results.push((pk.clone(), row.clone()));
                    }
                });
            }
            return Ok(results);
        }
        Err(StorageError::TableNotFound(table_id))
    }

    /// Stream through all visible rows without cloning (zero-copy).
    /// The closure receives a reference to each visible row.
    pub fn for_each_visible<F>(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
        f: F,
    ) -> Result<(), StorageError>
    where
        F: FnMut(&OwnedRow),
    {
        if let Some(table) = self.tables.get(&table_id) {
            table.for_each_visible(txn_id, read_ts, f);
            return Ok(());
        }
        Err(StorageError::TableNotFound(table_id))
    }

    /// Parallel map-reduce over all visible rows.
    /// Each thread gets its own accumulator; partial results are merged at the end.
    /// Automatically falls back to sequential for small tables.
    pub fn map_reduce_visible<T, Init, Map, Reduce>(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
        init: Init,
        map: Map,
        reduce: Reduce,
    ) -> Result<T, StorageError>
    where
        T: Send,
        Init: Fn() -> T + Sync,
        Map: Fn(&mut T, &OwnedRow) + Sync,
        Reduce: Fn(T, T) -> T,
    {
        if let Some(table) = self.tables.get(&table_id) {
            return Ok(table.map_reduce_visible(txn_id, read_ts, init, map, reduce));
        }
        Err(StorageError::TableNotFound(table_id))
    }

    /// Scan returning only row data — avoids cloning the PK per row.
    /// Use this when the caller only needs row values (SELECT, aggregates, joins).
    /// Does not record reads for conflict detection (caller must handle if needed).
    pub fn scan_rows_only(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Vec<OwnedRow>, StorageError> {
        if let Some(table) = self.tables.get(&table_id) {
            let results = table.scan_rows_only(txn_id, read_ts);
            return Ok(results);
        }
        // Fall back to full scan for non-rowstore tables
        let full = self.scan(table_id, txn_id, read_ts)?;
        Ok(full.into_iter().map(|(_, row)| row).collect())
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

            // USTM: issue prefetch hint for sequential scan pages.
            if results.len() > 1 {
                let scan_pages: Vec<PageId> = (0..results.len().min(16) as u32)
                    .map(|i| ustm_table_page_id(table_id, i))
                    .collect();
                self.ustm.prefetch_hint(
                    crate::ustm::PrefetchSource::SeqScan {
                        start_page: scan_pages[0],
                        count: scan_pages.len(),
                    },
                    std::path::PathBuf::from(format!("table_{}", table_id.0)),
                );
                self.ustm.prefetch_tick();
            }

            return Ok(results);
        }

        // Columnstore (feature-gated via stub)
        if let Some(cs) = self.columnstore_tables.get(&table_id) {
            let results = cs.scan(txn_id, read_ts);
            return Ok(results);
        }

        // Disk rowstore (feature-gated)
        #[cfg(feature = "disk_rowstore")]
        if let Some(disk) = self.disk_tables.get(&table_id) {
            let results = disk.scan(txn_id, read_ts);
            return Ok(results);
        }

        // LSM rowstore (feature-gated)
        #[cfg(feature = "lsm")]
        if let Some(lsm) = self.lsm_tables.get(&table_id) {
            let results = lsm.scan(txn_id, read_ts);

            // USTM: LSM scan — issue prefetch hints for upcoming SST pages.
            // LSM data lives on disk, so prefetch is especially valuable.
            if results.len() > 1 {
                let data_dir = self
                    .data_dir
                    .as_deref()
                    .unwrap_or_else(|| std::path::Path::new("."));
                let sst_path = data_dir.join(format!("lsm_table_{}", table_id.0));
                self.ustm.prefetch_hint(
                    crate::ustm::PrefetchSource::SeqScan {
                        start_page: ustm_table_page_id(table_id, 0),
                        count: results.len().min(16),
                    },
                    sst_path,
                );
                self.ustm.prefetch_tick();
            }

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

    /// Perform an index range scan: look up PKs via secondary index range,
    /// then fetch visible rows.
    ///
    /// - `lower`: optional `(encoded_key, inclusive)` lower bound
    /// - `upper`: optional `(encoded_key, inclusive)` upper bound
    ///
    /// Returns `(pk_bytes, row)` pairs visible to the given txn.
    pub fn index_range_scan(
        &self,
        table_id: TableId,
        column_idx: usize,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, OwnedRow)>, StorageError> {
        let table = self
            .tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        let pks = {
            let indexes = table.secondary_indexes.read();
            let mut found = Vec::new();
            for idx in indexes.iter() {
                if idx.column_idx == column_idx {
                    found = idx.range_scan(lower, upper);
                    break;
                }
            }
            found
        };

        let mut results = Vec::new();
        for pk in pks {
            if let Some(chain) = table.data.get(&pk) {
                if let Some(r) = chain.read_for_txn(txn_id, read_ts) {
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
    #[allow(dead_code)]
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
