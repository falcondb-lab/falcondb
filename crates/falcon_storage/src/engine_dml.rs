//! DML operations on StorageEngine: INSERT, UPDATE, DELETE, GET, SCAN, index scan.
//!
//! Each operation first checks the in-memory rowstore map, then the columnstore
//! map, then the disk-rowstore map.  This avoids coupling to the catalog's
//! `storage_type` at the DML layer — the table simply lives in one map.

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

#[cfg(any(feature = "columnstore", feature = "disk_rowstore"))]
use falcon_common::config::NodeRole;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::StorageError;
use falcon_common::types::{TableId, Timestamp, TxnId, TxnType};

use crate::memtable::{PrimaryKey, SimpleAggOp};
use crate::ustm::{AccessPriority, PageData, PageId};
use crate::wal::WalRecord;

use super::engine::StorageEngine;

impl StorageEngine {
    /// Resolve a table name from its ID via the catalog (for CDC events).
    /// Returns empty string if the table is not found (avoid blocking on catalog).
    #[inline]
    fn cdc_table_name(&self, table_id: TableId) -> String {
        self.catalog
            .read()
            .find_table_by_id(table_id)
            .map(|s| s.name.clone())
            .unwrap_or_default()
    }
}

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
        use crate::table_handle::TableHandle;

        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => {
                let row_bytes = crate::mvcc::estimate_row_bytes(&row);
                self.memory_tracker.alloc_write_buffer(row_bytes);
                self.txn_write_bytes
                    .entry(txn_id)
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(row_bytes, AtomicOrdering::Relaxed);
                if self.wal.is_some() {
                    self.txn_wal_buf.entry(txn_id).or_default().push(
                        WalRecord::Insert { txn_id, table_id, row: row.clone() },
                    );
                }
                let cdc_row = if self.ext.cdc_manager.is_enabled() { Some(row.clone()) } else { None };
                let pk = table.insert(row, txn_id)?;
                self.record_write_no_dedup(txn_id, table_id, pk.clone());
                if let Some(cdc_row) = cdc_row {
                    let tname = self.cdc_table_name(table_id);
                    self.ext.cdc_manager.emit_insert(txn_id, table_id, &tname, cdc_row, Some(format!("{pk:?}")));
                }
                Ok(pk)
            }
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(cs) => {
                self.record_write_path_violation_columnstore()?;
                self.append_wal(&WalRecord::Insert { txn_id, table_id, row: row.clone() })?;
                let pk = crate::memtable::encode_pk(&row, cs.schema.pk_indices());
                let _ = cs.insert(row, txn_id);
                Ok(pk)
            }
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(disk) => {
                self.record_write_path_violation_disk()?;
                self.append_wal(&WalRecord::Insert { txn_id, table_id, row: row.clone() })?;
                let pk = disk.insert(row, txn_id)?;
                Ok(pk)
            }
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(lsm) => {
                self.append_wal(&WalRecord::Insert { txn_id, table_id, row: row.clone() })?;
                let pk = lsm.insert(&row, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                let page_id = ustm_page_id(table_id, &pk);
                let data = PageData::new(pk.clone());
                self.ustm.insert_warm(page_id, data, AccessPriority::HotRow);
                Ok(pk)
            }
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(rdb) => {
                self.append_wal(&WalRecord::Insert { txn_id, table_id, row: row.clone() })?;
                let pk = rdb.insert(&row, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                Ok(pk)
            }
            #[cfg(feature = "redb")]
            TableHandle::Redb(rdb) => {
                self.append_wal(&WalRecord::Insert { txn_id, table_id, row: row.clone() })?;
                let pk = rdb.insert(&row, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                Ok(pk)
            }
        }
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
        use crate::table_handle::TableHandle;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => {
                let total_bytes: u64 = rows.iter().map(crate::mvcc::estimate_row_bytes).sum();
                self.memory_tracker.alloc_write_buffer(total_bytes);
                self.txn_write_bytes
                    .entry(txn_id)
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(total_bytes, AtomicOrdering::Relaxed);
                let row_count = rows.len();
                let wal_record = WalRecord::BatchInsert { txn_id, table_id, rows };
                self.append_wal(&wal_record)?;
                let rows = match wal_record {
                    WalRecord::BatchInsert { rows, .. } => rows,
                    _ => unreachable!(),
                };
                let cdc_enabled = self.ext.cdc_manager.is_enabled();
                let cdc_table_name = if cdc_enabled { Some(self.cdc_table_name(table_id)) } else { None };
                let mut pks = Vec::with_capacity(row_count);
                for row in rows {
                    let cdc_row = if cdc_enabled { Some(row.clone()) } else { None };
                    let pk = table.insert(row, txn_id)?;
                    self.record_write_no_dedup(txn_id, table_id, pk.clone());
                    if let (Some(cdc_row), Some(ref tname)) = (cdc_row, &cdc_table_name) {
                        self.ext.cdc_manager.emit_insert(txn_id, table_id, tname, cdc_row, Some(format!("{pk:?}")));
                    }
                    pks.push(pk);
                }
                Ok(pks)
            }
            #[allow(unreachable_patterns)]
            _ => {
                if let Some(result) = handle.batch_insert_disk(rows.clone(), txn_id) {
                    // lsm / rocksdb / redb: WAL first, then bulk insert
                    let wal_record = WalRecord::BatchInsert { txn_id, table_id, rows };
                    self.append_wal(&wal_record)?;
                    let pks = result?;
                    for pk in &pks {
                        self.record_write(txn_id, table_id, pk.clone());
                    }
                    Ok(pks)
                } else {
                    // columnstore / disk_rowstore: fall through to per-row insert
                    drop(handle);
                    let mut pks = Vec::new();
                    for row in rows {
                        pks.push(self.insert(table_id, row, txn_id)?);
                    }
                    Ok(pks)
                }
            }
        }
    }

    pub fn update(
        &self,
        table_id: TableId,
        pk: &PrimaryKey,
        new_row: OwnedRow,
        txn_id: TxnId,
    ) -> Result<(), StorageError> {
        use crate::table_handle::TableHandle;

        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => {
                let row_bytes = crate::mvcc::estimate_row_bytes(&new_row);
                self.memory_tracker.alloc_write_buffer(row_bytes);
                self.append_wal(&WalRecord::Update { txn_id, table_id, pk: pk.clone(), new_row: new_row.clone() })?;
                let cdc_row = if self.ext.cdc_manager.is_enabled() { Some(new_row.clone()) } else { None };
                table.update(pk, new_row, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                if let Some(cdc_row) = cdc_row {
                    let tname = self.cdc_table_name(table_id);
                    self.ext.cdc_manager.emit_update(txn_id, table_id, &tname, None, cdc_row, Some(format!("{pk:?}")));
                }
                Ok(())
            }
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(_) => {
                self.record_write_path_violation_columnstore()?;
                Err(StorageError::Io(std::io::Error::other("UPDATE not supported on COLUMNSTORE tables")))
            }
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(disk) => {
                self.record_write_path_violation_disk()?;
                self.append_wal(&WalRecord::Update { txn_id, table_id, pk: pk.clone(), new_row: new_row.clone() })?;
                disk.update(pk, new_row, txn_id)
            }
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(lsm) => {
                self.append_wal(&WalRecord::Update { txn_id, table_id, pk: pk.clone(), new_row: new_row.clone() })?;
                lsm.update(pk, &new_row, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                let page_id = ustm_page_id(table_id, pk);
                self.ustm.insert_warm(page_id, PageData::new(pk.clone()), AccessPriority::HotRow);
                Ok(())
            }
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(rdb) => {
                self.append_wal(&WalRecord::Update { txn_id, table_id, pk: pk.clone(), new_row: new_row.clone() })?;
                rdb.update(pk, &new_row, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                Ok(())
            }
            #[cfg(feature = "redb")]
            TableHandle::Redb(rdb) => {
                self.append_wal(&WalRecord::Update { txn_id, table_id, pk: pk.clone(), new_row: new_row.clone() })?;
                rdb.update(pk, &new_row, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                Ok(())
            }
        }
    }

    pub fn delete(
        &self,
        table_id: TableId,
        pk: &PrimaryKey,
        txn_id: TxnId,
    ) -> Result<(), StorageError> {
        use crate::table_handle::TableHandle;

        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => {
                let tombstone_bytes = std::mem::size_of::<crate::mvcc::Version>() as u64;
                self.memory_tracker.alloc_write_buffer(tombstone_bytes);
                self.append_wal(&WalRecord::Delete { txn_id, table_id, pk: pk.clone() })?;
                table.delete(pk, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                if self.ext.cdc_manager.is_enabled() {
                    let tname = self.cdc_table_name(table_id);
                    self.ext.cdc_manager.emit_delete(txn_id, table_id, &tname, None, Some(format!("{pk:?}")));
                }
                Ok(())
            }
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(_) => {
                self.record_write_path_violation_columnstore()?;
                Err(StorageError::Io(std::io::Error::other("DELETE not supported on COLUMNSTORE tables")))
            }
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(disk) => {
                self.record_write_path_violation_disk()?;
                self.append_wal(&WalRecord::Delete { txn_id, table_id, pk: pk.clone() })?;
                disk.delete(pk, txn_id)
            }
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(lsm) => {
                self.append_wal(&WalRecord::Delete { txn_id, table_id, pk: pk.clone() })?;
                lsm.delete(pk, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                self.ustm.unregister_page(ustm_page_id(table_id, pk));
                Ok(())
            }
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(rdb) => {
                self.append_wal(&WalRecord::Delete { txn_id, table_id, pk: pk.clone() })?;
                rdb.delete(pk, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                Ok(())
            }
            #[cfg(feature = "redb")]
            TableHandle::Redb(rdb) => {
                self.append_wal(&WalRecord::Delete { txn_id, table_id, pk: pk.clone() })?;
                rdb.delete(pk, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                Ok(())
            }
        }
    }

    pub fn get(
        &self,
        table_id: TableId,
        pk: &PrimaryKey,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Option<OwnedRow>, StorageError> {
        use crate::table_handle::TableHandle;

        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        #[allow(unreachable_patterns)]
        match handle.value() {
            TableHandle::Rowstore(table) => Ok(table.get(pk, txn_id, read_ts)),
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(_) => Ok(None),
            #[cfg(feature = "disk_rowstore")]
            TableHandle::DiskRowstore(disk) => Ok(disk.get(pk, txn_id, read_ts)),
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(lsm) => {
                self.record_read(txn_id, table_id, pk.clone());
                let page_id = ustm_page_id(table_id, pk);
                let result = lsm.get(pk, txn_id, read_ts)?;
                self.ustm.insert_warm(page_id, PageData::new(pk.clone()), AccessPriority::HotRow);
                Ok(result)
            }
            #[cfg(feature = "rocksdb")]
            TableHandle::RocksDb(rdb) => {
                self.record_read(txn_id, table_id, pk.clone());
                rdb.get(pk, txn_id, read_ts)
            }
            #[cfg(feature = "redb")]
            TableHandle::Redb(rdb) => {
                self.record_read(txn_id, table_id, pk.clone());
                rdb.get(pk, txn_id, read_ts)
            }
        }
    }

    /// Count visible rows without materializing any row data.
    /// Used for fast COUNT(*) without WHERE clause.
    pub fn count_visible(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<usize, StorageError> {
        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        Ok(handle.count_visible(txn_id, read_ts))
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
        use crate::table_handle::TableHandle;
        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        match handle.value() {
            TableHandle::Rowstore(table) => Ok(table.scan_top_k_by_pk(txn_id, read_ts, k, ascending)),
            #[allow(unreachable_patterns)]
            _ => {
                let mut rows = handle.scan(txn_id, read_ts);
                rows.sort_by(|(a, _), (b, _)| if ascending { a.cmp(b) } else { b.cmp(a) });
                rows.truncate(k);
                Ok(rows)
            }
        }
    }

    /// Compute simple aggregates in a streaming pass without materializing rows.
    pub fn compute_simple_aggs(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
        agg_specs: &[(SimpleAggOp, Option<usize>)],
    ) -> Result<Vec<Datum>, StorageError> {
        let compute_from_rows = |rows: Vec<(PrimaryKey, OwnedRow)>| {
            let n = agg_specs.len();
            let mut counts = vec![0i64; n];
            let mut sums_i = vec![0i64; n];
            let mut sums_f = vec![0f64; n];
            let mut is_float = vec![false; n];
            let mut mins: Vec<Option<Datum>> = vec![None; n];
            let mut maxs: Vec<Option<Datum>> = vec![None; n];

            for (_, row) in rows {
                for (i, (op, col_opt)) in agg_specs.iter().enumerate() {
                    match op {
                        SimpleAggOp::CountStar => counts[i] += 1,
                        SimpleAggOp::Count => {
                            if let Some(ci) = col_opt {
                                if !matches!(row.values.get(*ci), Some(Datum::Null) | None) {
                                    counts[i] += 1;
                                }
                            }
                        }
                        SimpleAggOp::Sum => {
                            if let Some(ci) = col_opt {
                                match row.values.get(*ci) {
                                    Some(Datum::Int32(v)) => {
                                        counts[i] += 1;
                                        sums_i[i] += i64::from(*v);
                                        sums_f[i] += f64::from(*v);
                                    }
                                    Some(Datum::Int64(v)) => {
                                        counts[i] += 1;
                                        sums_i[i] += *v;
                                        sums_f[i] += *v as f64;
                                    }
                                    Some(Datum::Float64(v)) => {
                                        counts[i] += 1;
                                        is_float[i] = true;
                                        sums_f[i] += *v;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        SimpleAggOp::Min => {
                            if let Some(ci) = col_opt {
                                if let Some(d) = row.values.get(*ci) {
                                    if !matches!(d, Datum::Null)
                                        && (mins[i].is_none()
                                            || d.partial_cmp(mins[i].as_ref().expect("min exists after is_none check"))
                                                == Some(std::cmp::Ordering::Less))
                                    {
                                        mins[i] = Some(d.clone());
                                    }
                                }
                            }
                        }
                        SimpleAggOp::Max => {
                            if let Some(ci) = col_opt {
                                if let Some(d) = row.values.get(*ci) {
                                    if !matches!(d, Datum::Null)
                                        && (maxs[i].is_none()
                                            || d.partial_cmp(maxs[i].as_ref().expect("max exists after is_none check"))
                                                == Some(std::cmp::Ordering::Greater))
                                    {
                                        maxs[i] = Some(d.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            agg_specs
                .iter()
                .enumerate()
                .map(|(i, (op, _))| match op {
                    SimpleAggOp::CountStar | SimpleAggOp::Count => Datum::Int64(counts[i]),
                    SimpleAggOp::Sum => {
                        if counts[i] == 0 {
                            Datum::Null
                        } else if is_float[i] {
                            Datum::Float64(sums_f[i])
                        } else {
                            Datum::Int64(sums_i[i])
                        }
                    }
                    SimpleAggOp::Min => mins[i].clone().unwrap_or(Datum::Null),
                    SimpleAggOp::Max => maxs[i].clone().unwrap_or(Datum::Null),
                })
                .collect::<Vec<_>>()
        };

        use crate::table_handle::TableHandle;
        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => Ok(table.compute_simple_aggs(txn_id, read_ts, agg_specs)),
            #[allow(unreachable_patterns)]
            _ => Ok(compute_from_rows(handle.scan(txn_id, read_ts))),
        }
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
        // non-rowstore fallback: full scan then filter
        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        let mut results = Vec::new();
        for (pk, row) in handle.scan(txn_id, read_ts) {
            if let Some(lim) = limit {
                if results.len() >= lim { break; }
            }
            if predicate(&row) {
                results.push((pk, row));
            }
        }
        Ok(results)
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
        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        handle.for_each_visible(txn_id, read_ts, f);
        Ok(())
    }

    /// Parallel map-reduce over all visible rows.
    /// Each thread gets its own accumulator; partial results are merged at the end.
    /// Rowstore uses parallel rayon shards; other engines fall back to sequential.
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
        use crate::table_handle::TableHandle;

        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => {
                Ok(table.map_reduce_visible(txn_id, read_ts, init, map, reduce))
            }
            _ => {
                let mut acc = init();
                handle.for_each_visible(txn_id, read_ts, |row| map(&mut acc, row));
                let _ = reduce;
                Ok(acc)
            }
        }
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
            return Ok(table.scan_rows_only(txn_id, read_ts));
        }
        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        Ok(handle.scan(txn_id, read_ts).into_iter().map(|(_, row)| row).collect())
    }

    pub fn scan(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Vec<(PrimaryKey, OwnedRow)>, StorageError> {
        use crate::table_handle::TableHandle;

        let handle = self.engine_tables.get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        #[allow(unreachable_patterns)]
        match handle.value() {
            TableHandle::Rowstore(table) => {
                let results = table.scan(txn_id, read_ts);
                for (pk, _) in &results {
                    self.record_read(txn_id, table_id, pk.clone());
                }
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
                Ok(results)
            }
            #[cfg(feature = "lsm")]
            TableHandle::Lsm(lsm) => {
                let results = lsm.scan(txn_id, read_ts);
                if results.len() > 1 {
                    let data_dir = self.data_dir.as_deref().unwrap_or_else(|| std::path::Path::new("."));
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
                Ok(results)
            }
            _ => Ok(handle.scan(txn_id, read_ts)),
        }
    }

    /// Columnar scan: returns one Vec<Datum> per column for vectorized aggregate execution.
    /// Only available for ColumnStore tables; returns None for rowstore/disk tables.
    /// The executor uses this to bypass row-at-a-time deserialization for analytics queries.
    #[cfg(feature = "columnstore")]
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

    /// Columnar scan stub — always returns None when columnstore feature is disabled.
    #[cfg(not(feature = "columnstore"))]
    pub fn scan_columnar(
        &self,
        _table_id: TableId,
        _txn_id: TxnId,
        _read_ts: Timestamp,
    ) -> Option<Vec<Vec<falcon_common::datum::Datum>>> {
        None
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
    #[cfg(feature = "columnstore")]
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
    #[cfg(feature = "disk_rowstore")]
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
