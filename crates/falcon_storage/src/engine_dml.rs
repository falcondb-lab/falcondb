//! DML operations on StorageEngine: INSERT, UPDATE, DELETE, GET, SCAN, index scan.
//!
//! All operations dispatch through `engine_tables` (`DashMap<TableId, TableHandle>`).
//! Rowstore/Columnstore/DiskRowstore have explicit branches for special semantics;
//! LSM/RocksDB/redb go through `TableHandle::as_storage_table()` unified path.

use std::sync::Arc;

use crate::memtable::MemTable;

#[cfg(any(feature = "columnstore", feature = "disk_rowstore"))]
use std::sync::atomic::Ordering as AtomicOrdering;

thread_local! {
    /// (engine_id, ddl_epoch, table_id, Arc<MemTable>) — skip engine_tables DashMap lookup.
    static TABLE_CACHE: std::cell::RefCell<Option<(u64, u64, TableId, Arc<MemTable>)>> = const { std::cell::RefCell::new(None) };
}

#[cfg(any(feature = "columnstore", feature = "disk_rowstore"))]
use falcon_common::config::NodeRole;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::StorageError;
use falcon_common::types::{TableId, Timestamp, TxnId, TxnType};

use crate::memtable::{PrimaryKey, SimpleAggOp};
use crate::ustm::PageId;
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

use crate::ustm::page::page_id_for_table;

impl StorageEngine {
    // ── Core DML ─────────────────────────────────────────────────────

    pub fn insert(
        &self,
        table_id: TableId,
        row: OwnedRow,
        txn_id: TxnId,
    ) -> Result<PrimaryKey, StorageError> {
        use crate::table_handle::TableHandle;

        self.check_write_pressure()?;

        let eid = self.engine_id;
        let epoch = self.ddl_epoch.load(std::sync::atomic::Ordering::Relaxed);
        let cached = TABLE_CACHE.with(|c| {
            let slot = c.borrow();
            match slot.as_ref() {
                Some((e, ep, id, t)) if *e == eid && *ep == epoch && *id == table_id => Some(Arc::clone(t)),
                _ => None,
            }
        });
        if let Some(table) = cached {
            return self.insert_rowstore(&table, table_id, row, txn_id);
        }

        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => {
                TABLE_CACHE.with(|c| *c.borrow_mut() = Some((eid, epoch, table_id, Arc::clone(table))));
                self.insert_rowstore(table, table_id, row, txn_id)
            }
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(cs) => {
                self.record_write_path_violation_columnstore()?;
                self.append_wal(&WalRecord::Insert {
                    txn_id,
                    table_id,
                    row: row.clone(),
                })?;
                let pk = crate::memtable::encode_pk(&row, cs.schema.pk_indices());
                cs.insert(row, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                Ok(pk)
            }
            #[allow(unreachable_patterns)]
            _ => {
                if let Some(t) = handle.as_storage_table() {
                    #[cfg(feature = "disk_rowstore")]
                    if handle.is_disk_rowstore() {
                        self.record_write_path_violation_disk()?;
                    }
                    self.append_wal(&WalRecord::Insert {
                        txn_id,
                        table_id,
                        row: row.clone(),
                    })?;
                    let pk = t.insert(&row, txn_id)?;
                    self.record_write(txn_id, table_id, pk.clone());
                    t.prefetch_hint(table_id, &pk, &self.ustm);
                    Ok(pk)
                } else {
                    Err(StorageError::TableNotFound(table_id))
                }
            }
        }
    }

    fn insert_rowstore(
        &self,
        table: &Arc<MemTable>,
        table_id: TableId,
        row: OwnedRow,
        txn_id: TxnId,
    ) -> Result<PrimaryKey, StorageError> {
        let row_bytes = crate::mvcc::estimate_row_bytes(&row);
        self.memory_tracker.alloc_write_buffer(row_bytes);
        let wal_row = if self.wal.is_some() || self.wal_observer.is_some() {
            Some(row.clone())
        } else {
            None
        };
        let cdc_row = if self.ext.cdc_manager.is_enabled() {
            Some(row.clone())
        } else {
            None
        };
        let pk = table.insert(row, txn_id)?;
        let wal_rec = wal_row.map(|r| WalRecord::Insert {
            txn_id,
            table_id,
            row: r,
        });
        // Pre-serialize WAL record when no observer needs the original WalRecord
        let pre_bytes = if self.wal_observer.is_none() {
            wal_rec
                .as_ref()
                .and_then(|wr| crate::wal::WalWriter::serialize_record(wr).ok())
        } else {
            None
        };
        // Try thread-local fast path. Returns true if consumed, false if DashMap needed.
        let pre_count = usize::from(pre_bytes.is_some());
        let consumed = crate::engine::TXN_LOCAL_CACHE.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                return true; // will fill below
            }
            let prev_eid = slot.as_ref().unwrap().0;
            if prev_eid == self.engine_id {
                let (_, prev_id, prev) = slot.take().unwrap();
                let mut state = self.txn_local.entry(prev_id).or_default();
                state.write_bytes += prev.write_bytes;
                state.wal_buf.extend(prev.wal_buf);
                if !prev.wal_preserialized.is_empty() {
                    state.wal_preserialized.extend(prev.wal_preserialized);
                    state.wal_pre_count += prev.wal_pre_count;
                }
                state.write_set.extend(prev.write_set);
                if state.cached_table.is_none() {
                    state.cached_table = prev.cached_table;
                }
                drop(state);
            }
            false
        });
        if consumed {
            crate::engine::TXN_LOCAL_CACHE.with(|cell| {
                let s = crate::engine::TxnLocalState {
                    write_bytes: row_bytes,
                    wal_buf: wal_rec.into_iter().collect(),
                    wal_preserialized: pre_bytes.unwrap_or_default(),
                    wal_pre_count: pre_count,
                    write_set: vec![crate::engine::TxnWriteOp {
                        table_id,
                        pk: pk.clone(),
                    }],
                    cached_table: Some(Arc::clone(table)),
                };
                *cell.borrow_mut() = Some((self.engine_id, txn_id, s));
            });
        } else {
            let mut state = self.txn_local.entry(txn_id).or_default();
            state.write_bytes += row_bytes;
            if let Some(wr) = wal_rec {
                state.wal_buf.push(wr);
            }
            state.write_set.push(crate::engine::TxnWriteOp {
                table_id,
                pk: pk.clone(),
            });
            if state.cached_table.is_none() {
                state.cached_table = Some(Arc::clone(table));
            }
        }
        if let Some(cdc_row) = cdc_row {
            let tname = self.cdc_table_name(table_id);
            self.ext.cdc_manager.emit_insert(
                txn_id,
                table_id,
                &tname,
                cdc_row,
                Some(format!("{pk:?}")),
            );
        }
        Ok(pk)
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

        self.check_write_pressure()?;

        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => {
                let total_bytes: u64 = rows.iter().map(crate::mvcc::estimate_row_bytes).sum();
                self.memory_tracker.alloc_write_buffer(total_bytes);
                self.txn_local.entry(txn_id).or_default().write_bytes += total_bytes;
                let row_count = rows.len();
                let wal_record = WalRecord::BatchInsert {
                    txn_id,
                    table_id,
                    rows,
                };
                self.append_wal(&wal_record)?;
                let rows = match wal_record {
                    WalRecord::BatchInsert { rows, .. } => rows,
                    _ => unreachable!(),
                };
                let cdc_enabled = self.ext.cdc_manager.is_enabled();
                let cdc_table_name = if cdc_enabled {
                    Some(self.cdc_table_name(table_id))
                } else {
                    None
                };
                let mut pks = Vec::with_capacity(row_count);
                for row in rows {
                    let cdc_row = if cdc_enabled { Some(row.clone()) } else { None };
                    let pk = table.insert(row, txn_id)?;
                    self.record_write_no_dedup(txn_id, table_id, pk.clone());
                    if let (Some(cdc_row), Some(ref tname)) = (cdc_row, &cdc_table_name) {
                        self.ext.cdc_manager.emit_insert(
                            txn_id,
                            table_id,
                            tname,
                            cdc_row,
                            Some(format!("{pk:?}")),
                        );
                    }
                    pks.push(pk);
                }
                Ok(pks)
            }
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(cs) => {
                self.record_write_path_violation_columnstore()?;
                let wal_record = WalRecord::BatchInsert {
                    txn_id,
                    table_id,
                    rows,
                };
                self.append_wal(&wal_record)?;
                let rows = match wal_record {
                    WalRecord::BatchInsert { rows, .. } => rows,
                    _ => unreachable!(),
                };
                let mut pks = Vec::with_capacity(rows.len());
                for row in &rows {
                    pks.push(crate::memtable::encode_pk(row, cs.schema.pk_indices()));
                }
                cs.insert_batch(rows, txn_id)?;
                for pk in &pks {
                    self.record_write_no_dedup(txn_id, table_id, pk.clone());
                }
                Ok(pks)
            }
            #[allow(unreachable_patterns)]
            _ => {
                if let Some(result) = handle.batch_insert_disk(rows.clone(), txn_id) {
                    let wal_record = WalRecord::BatchInsert {
                        txn_id,
                        table_id,
                        rows,
                    };
                    self.append_wal(&wal_record)?;
                    let pks = result?;
                    for pk in &pks {
                        self.record_write(txn_id, table_id, pk.clone());
                    }
                    Ok(pks)
                } else {
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

        self.check_write_pressure()?;

        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => {
                let row_bytes = crate::mvcc::estimate_row_bytes(&new_row);
                self.memory_tracker.alloc_write_buffer(row_bytes);
                self.append_wal(&WalRecord::Update {
                    txn_id,
                    table_id,
                    pk: pk.clone(),
                    new_row: new_row.clone(),
                })?;
                let old_row = if self.ext.cdc_manager.is_enabled() {
                    table.get(pk, txn_id, falcon_common::types::Timestamp(u64::MAX))
                } else {
                    None
                };
                let cdc_new = if self.ext.cdc_manager.is_enabled() {
                    Some(new_row.clone())
                } else {
                    None
                };
                table.update(pk, new_row, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                if let Some(cdc_new) = cdc_new {
                    let tname = self.cdc_table_name(table_id);
                    self.ext.cdc_manager.emit_update(
                        txn_id,
                        table_id,
                        &tname,
                        old_row,
                        cdc_new,
                        Some(format!("{pk:?}")),
                    );
                }
                Ok(())
            }
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(_) => {
                self.record_write_path_violation_columnstore()?;
                Err(StorageError::Io(std::io::Error::other(
                    "UPDATE not supported on COLUMNSTORE tables",
                )))
            }
            #[allow(unreachable_patterns)]
            _ => {
                if let Some(t) = handle.as_storage_table() {
                    #[cfg(feature = "disk_rowstore")]
                    if handle.is_disk_rowstore() {
                        self.record_write_path_violation_disk()?;
                    }
                    self.append_wal(&WalRecord::Update {
                        txn_id,
                        table_id,
                        pk: pk.clone(),
                        new_row: new_row.clone(),
                    })?;
                    t.update(pk, &new_row, txn_id)?;
                    self.record_write(txn_id, table_id, pk.clone());
                    t.prefetch_hint(table_id, pk, &self.ustm);
                    Ok(())
                } else {
                    Err(StorageError::Io(std::io::Error::other(
                        "UPDATE not supported on this engine",
                    )))
                }
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

        self.check_write_pressure()?;

        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => {
                let tombstone_bytes = std::mem::size_of::<crate::mvcc::Version>() as u64;
                self.memory_tracker.alloc_write_buffer(tombstone_bytes);
                self.append_wal(&WalRecord::Delete {
                    txn_id,
                    table_id,
                    pk: pk.clone(),
                })?;
                let old_row = if self.ext.cdc_manager.is_enabled() {
                    table.get(pk, txn_id, falcon_common::types::Timestamp(u64::MAX))
                } else {
                    None
                };
                table.delete(pk, txn_id)?;
                self.record_write(txn_id, table_id, pk.clone());
                if self.ext.cdc_manager.is_enabled() {
                    let tname = self.cdc_table_name(table_id);
                    self.ext.cdc_manager.emit_delete(
                        txn_id,
                        table_id,
                        &tname,
                        old_row,
                        Some(format!("{pk:?}")),
                    );
                }
                Ok(())
            }
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(t) => {
                self.append_wal(&WalRecord::Delete {
                    txn_id,
                    table_id,
                    pk: pk.clone(),
                })?;
                t.delete_by_pk(pk, txn_id);
                self.record_write(txn_id, table_id, pk.clone());
                Ok(())
            }
            #[allow(unreachable_patterns)]
            _ => {
                if let Some(t) = handle.as_storage_table() {
                    #[cfg(feature = "disk_rowstore")]
                    if handle.is_disk_rowstore() {
                        self.record_write_path_violation_disk()?;
                    }
                    self.append_wal(&WalRecord::Delete {
                        txn_id,
                        table_id,
                        pk: pk.clone(),
                    })?;
                    t.delete(pk, txn_id)?;
                    self.record_write(txn_id, table_id, pk.clone());
                    t.prefetch_evict(table_id, pk, &self.ustm);
                    Ok(())
                } else {
                    Err(StorageError::Io(std::io::Error::other(
                        "DELETE not supported on this engine",
                    )))
                }
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

        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => {
                self.record_read(txn_id, table_id, pk);
                Ok(table.get(pk, txn_id, read_ts))
            }
            #[cfg(feature = "columnstore")]
            TableHandle::Columnstore(cs) => Ok(cs.get(pk, txn_id, read_ts)),
            #[allow(unreachable_patterns)]
            _ => {
                if let Some(t) = handle.as_storage_table() {
                    self.record_read(txn_id, table_id, pk);
                    let result = t.get(pk, txn_id, read_ts)?;
                    t.prefetch_hint(table_id, pk, &self.ustm);
                    Ok(result)
                } else {
                    Ok(None)
                }
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
        let handle = self
            .engine_tables
            .get(&table_id)
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
        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        match handle.value() {
            TableHandle::Rowstore(table) => {
                Ok(table.scan_top_k_by_pk(txn_id, read_ts, k, ascending))
            }
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
                                            || d.partial_cmp(
                                                mins[i]
                                                    .as_ref()
                                                    .expect("min exists after is_none check"),
                                            ) == Some(std::cmp::Ordering::Less))
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
                                            || d.partial_cmp(
                                                maxs[i]
                                                    .as_ref()
                                                    .expect("max exists after is_none check"),
                                            ) == Some(std::cmp::Ordering::Greater))
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
        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        match handle.value() {
            TableHandle::Rowstore(table) => {
                Ok(table.compute_simple_aggs(txn_id, read_ts, agg_specs))
            }
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
        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        if let Some(table) = handle.as_rowstore() {
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
        let mut results = Vec::new();
        for (pk, row) in handle.scan(txn_id, read_ts) {
            if let Some(lim) = limit {
                if results.len() >= lim {
                    break;
                }
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
        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        if let Some(table) = handle.as_rowstore() {
            let results = table.scan(txn_id, read_ts);
            for (pk, _) in &results {
                self.record_read(txn_id, table_id, pk);
            }
            let mut f = f;
            for (_, row) in results {
                f(&row);
            }
            return Ok(());
        }
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

        let handle = self
            .engine_tables
            .get(&table_id)
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
        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        if let Some(table) = handle.as_rowstore() {
            let results = table.scan(txn_id, read_ts);
            for (pk, _) in &results {
                self.record_read(txn_id, table_id, pk);
            }
            return Ok(results.into_iter().map(|(_, row)| row).collect());
        }
        Ok(handle
            .scan(txn_id, read_ts)
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    pub fn scan(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Vec<(PrimaryKey, OwnedRow)>, StorageError> {
        use crate::table_handle::TableHandle;

        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;

        #[allow(unreachable_patterns)]
        match handle.value() {
            TableHandle::Rowstore(table) => {
                let results = table.scan(txn_id, read_ts);
                for (pk, _) in &results {
                    self.record_read(txn_id, table_id, pk);
                }
                if results.len() > 1 {
                    let scan_pages: Vec<PageId> = (0..results.len().min(16) as u32)
                        .map(|i| page_id_for_table(table_id, i))
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
            _ => {
                let results = handle.scan(txn_id, read_ts);
                if let Some(t) = handle.as_storage_table() {
                    t.scan_prefetch_hint(table_id, results.len(), &self.ustm);
                }
                Ok(results)
            }
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
        let handle = self.engine_tables.get(&table_id)?;
        let cs = handle.as_columnstore()?;
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
        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        let table = handle.as_rowstore().ok_or_else(|| {
            StorageError::Io(std::io::Error::other(
                "secondary indexes only supported on rowstore",
            ))
        })?;
        let results = table.index_scan(column_idx, key, txn_id, read_ts);
        for (pk, _) in &results {
            self.record_read(txn_id, table_id, pk);
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
        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        let table = handle.as_rowstore().ok_or_else(|| {
            StorageError::Io(std::io::Error::other(
                "secondary indexes only supported on rowstore",
            ))
        })?;
        let results = table.index_range_scan(column_idx, lower, upper, txn_id, read_ts);
        for (pk, _) in &results {
            self.record_read(txn_id, table_id, pk);
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
        let handle = self
            .engine_tables
            .get(&table_id)
            .ok_or(StorageError::TableNotFound(table_id))?;
        let table = handle.as_rowstore().ok_or_else(|| {
            StorageError::Io(std::io::Error::other(
                "secondary indexes only supported on rowstore",
            ))
        })?;
        Ok(table.index_lookup_pks(column_idx, key))
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

#[cfg(test)]
mod cdc_before_after_tests {
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, TxnId}; // Timestamp not needed here

    use crate::cdc::ChangeOp;
    use crate::engine::StorageEngine;
    use crate::memtable::PrimaryKey;

    fn make_engine_with_cdc() -> StorageEngine {
        let engine = StorageEngine::new_in_memory();
        engine.ext.cdc_manager.set_enabled(true);
        engine
    }

    fn col(id: u32, name: &str, dt: DataType, pk: bool) -> ColumnDef {
        ColumnDef {
            id: ColumnId(id),
            name: name.into(),
            data_type: dt,
            nullable: !pk,
            is_primary_key: pk,
            default_value: None,
            is_serial: false,
            max_length: None,
        }
    }

    fn make_schema(table_id: TableId) -> TableSchema {
        TableSchema {
            id: table_id,
            name: "test_tbl".into(),
            columns: vec![
                col(0, "id", DataType::Int64, true),
                col(1, "val", DataType::Text, false),
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        }
    }

    #[allow(dead_code)]
    fn pk(id: i64) -> PrimaryKey {
        bincode::serialize(&id).unwrap()
    }

    fn row(id: i64, val: &str) -> OwnedRow {
        OwnedRow::new(vec![Datum::Int64(id), Datum::Text(val.into())])
    }

    #[test]
    fn test_cdc_update_has_old_and_new_row() {
        use falcon_common::types::{Timestamp, TxnType};
        let engine = make_engine_with_cdc();
        let schema = make_schema(TableId(1));
        engine.create_table(schema).unwrap();

        let slot_id = engine.ext.cdc_manager.create_slot("s1").unwrap();
        let tid = TableId(1);
        let txn = TxnId(1);

        let actual_pk = engine.insert(tid, row(42, "original"), txn).unwrap();
        engine
            .commit_txn(txn, Timestamp(1), TxnType::Local)
            .unwrap();
        let _ = engine.ext.cdc_manager.poll_changes(slot_id, 100); // drain insert

        let txn2 = TxnId(2);
        engine
            .update(tid, &actual_pk, row(42, "updated"), txn2)
            .unwrap();

        let events = engine.ext.cdc_manager.poll_changes(slot_id, 100);
        let upd = events
            .iter()
            .find(|e| e.op == ChangeOp::Update)
            .expect("UPDATE event missing");
        assert!(upd.new_row.is_some(), "new_row should be present");
        assert!(
            upd.old_row.is_some(),
            "old_row (before-image) should be present"
        );
        let old = upd.old_row.as_ref().unwrap();
        assert_eq!(
            old.values[1],
            Datum::Text("original".into()),
            "old_row should have original value"
        );
        let new = upd.new_row.as_ref().unwrap();
        assert_eq!(
            new.values[1],
            Datum::Text("updated".into()),
            "new_row should have updated value"
        );
    }

    #[test]
    fn test_cdc_delete_has_old_row() {
        use falcon_common::types::{Timestamp, TxnType};
        let engine = make_engine_with_cdc();
        let schema = make_schema(TableId(2));
        engine.create_table(schema).unwrap();

        let slot_id = engine.ext.cdc_manager.create_slot("s2").unwrap();
        let tid = TableId(2);

        let actual_pk = engine.insert(tid, row(99, "to_delete"), TxnId(1)).unwrap();
        engine
            .commit_txn(TxnId(1), Timestamp(1), TxnType::Local)
            .unwrap();
        let _ = engine.ext.cdc_manager.poll_changes(slot_id, 100); // drain insert

        engine.delete(tid, &actual_pk, TxnId(2)).unwrap();

        let events = engine.ext.cdc_manager.poll_changes(slot_id, 100);
        let del = events
            .iter()
            .find(|e| e.op == ChangeOp::Delete)
            .expect("DELETE event missing");
        assert!(
            del.old_row.is_some(),
            "old_row (before-image) should be present on DELETE"
        );
        let old = del.old_row.as_ref().unwrap();
        assert_eq!(
            old.values[1],
            Datum::Text("to_delete".into()),
            "old_row should have the deleted row's value"
        );
        assert!(del.new_row.is_none(), "new_row should be None for DELETE");
    }
}
