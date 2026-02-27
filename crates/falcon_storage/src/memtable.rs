//! # Module Status: PRODUCTION
//! In-memory row store — the primary storage engine for FalconDB OLTP.
//! This is the **only** production write path for row data.
//!
//! ## Golden Path (OLTP Write)
//! ```text
//! TxnManager.begin() → Executor DML
//!   → MemTable.insert / update / delete  [MVCC version chain]
//!   → TxnManager.commit()
//!     → OCC validation (read-set / write-set)
//!     → WAL append (WalRecord::InsertRow / UpdateRow / DeleteRow)
//!     → Version chain commit (set commit_ts on all written versions)
//! ```
//!
//! ## Prohibited Patterns
//! - Direct MemTable mutation without an active TxnId → violates MVCC
//! - Bypassing WAL append on commit → violates crash-safety
//! - Non-transactional reads that skip visibility checks → violates isolation

use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::cmp::Reverse;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::TableSchema;
use falcon_common::types::{TableId, Timestamp, TxnId};

use crate::mvcc::VersionChain;

/// Hex-encode a byte slice for diagnostic/observability output.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Simple aggregate operation for streaming computation (no GROUP BY).
#[derive(Debug, Clone, Copy)]
pub enum SimpleAggOp {
    CountStar,
    Count,
    Sum,
    Min,
    Max,
}

/// Primary key encoded as a byte vector for hashing / comparison.
pub type PrimaryKey = Vec<u8>;

/// Encode a row's primary key columns into a comparable byte vector.
pub fn encode_pk(row: &OwnedRow, pk_indices: &[usize]) -> PrimaryKey {
    let mut buf = Vec::with_capacity(64);
    for &idx in pk_indices {
        if let Some(datum) = row.get(idx) {
            encode_datum_to_bytes(datum, &mut buf);
        }
    }
    buf
}

/// Encode datums from individual values (for WHERE pk = <value>).
pub fn encode_pk_from_datums(datums: &[&Datum]) -> PrimaryKey {
    let mut buf = Vec::with_capacity(64);
    for datum in datums {
        encode_datum_to_bytes(datum, &mut buf);
    }
    buf
}

/// Encode a single column value into bytes for secondary index keys.
pub fn encode_column_value(datum: &Datum) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_datum_to_bytes(datum, &mut buf);
    buf
}

fn encode_datum_to_bytes(datum: &Datum, buf: &mut Vec<u8>) {
    match datum {
        Datum::Null => {
            buf.push(0x00);
        }
        Datum::Boolean(b) => {
            buf.push(0x01);
            buf.push(if *b { 1 } else { 0 });
        }
        Datum::Int32(v) => {
            buf.push(0x02);
            // Encode as big-endian with sign flip for correct ordering
            let encoded = (*v as u32) ^ (1u32 << 31);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Int64(v) => {
            buf.push(0x03);
            let encoded = (*v as u64) ^ (1u64 << 63);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Float64(v) => {
            buf.push(0x04);
            let bits = v.to_bits();
            let encoded = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Text(s) => {
            buf.push(0x05);
            buf.extend_from_slice(s.as_bytes());
            buf.push(0x00); // null terminator for ordering
        }
        Datum::Timestamp(v) => {
            buf.push(0x06);
            let encoded = (*v as u64) ^ (1u64 << 63);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Date(v) => {
            buf.push(0x09);
            let encoded = (*v as u32) ^ (1u32 << 31);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Array(elems) => {
            buf.push(0x07);
            // Encode length then each element
            buf.extend_from_slice(&(elems.len() as u32).to_be_bytes());
            for elem in elems {
                encode_datum_to_bytes(elem, buf);
            }
        }
        Datum::Jsonb(v) => {
            buf.push(0x08);
            let s = v.to_string();
            buf.extend_from_slice(s.as_bytes());
            buf.push(0x00);
        }
        Datum::Decimal(mantissa, scale) => {
            buf.push(0x0A);
            buf.push(*scale);
            // Encode mantissa as big-endian i128 with sign flip for correct ordering
            let encoded = (*mantissa as u128) ^ (1u128 << 127);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Time(us) => {
            buf.push(0x0B);
            let encoded = (*us as u64) ^ (1u64 << 63);
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        Datum::Interval(months, days, us) => {
            buf.push(0x0C);
            let em = (*months as u32) ^ (1u32 << 31);
            let ed = (*days as u32) ^ (1u32 << 31);
            let eu = (*us as u64) ^ (1u64 << 63);
            buf.extend_from_slice(&em.to_be_bytes());
            buf.extend_from_slice(&ed.to_be_bytes());
            buf.extend_from_slice(&eu.to_be_bytes());
        }
        Datum::Uuid(v) => {
            buf.push(0x0D);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        Datum::Bytea(bytes) => {
            buf.push(0x0E);
            buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
    }
}

/// In-memory table: concurrent hash map of PK → VersionChain.
/// Also maintains secondary indexes.
pub struct MemTable {
    pub schema: TableSchema,
    /// Primary data: PK bytes → version chain.
    pub data: DashMap<PrimaryKey, Arc<VersionChain>>,
    /// Secondary B-tree indexes: column_index → (encoded_value → set of PKs).
    pub secondary_indexes: RwLock<Vec<SecondaryIndex>>,
    /// Hint counter: approximate number of writes that created multi-version
    /// chains (insert-to-existing, update, delete). GC can skip the full
    /// DashMap iteration when this is 0 — eliminating O(n) shard-lock
    /// contention on INSERT-only workloads.
    gc_candidates: AtomicU64,
    /// Exact count of committed live (non-tombstone) rows.
    /// Incremented on INSERT commit, decremented on DELETE commit.
    /// Enables O(1) COUNT(*) without WHERE clause.
    committed_row_count: AtomicI64,
    /// Ordered PK index: maintained at commit time.
    /// Enables O(K) scan_top_k_by_pk instead of O(N) full DashMap scan.
    pk_order: RwLock<BTreeSet<PrimaryKey>>,
}

/// A secondary index on one or more columns using a BTreeMap.
/// Single-column indexes use `column_idx`; composite indexes use `column_indices`.
pub struct SecondaryIndex {
    pub column_idx: usize,
    /// For composite indexes: ordered list of column indices.
    /// Empty means single-column index (use `column_idx`).
    pub column_indices: Vec<usize>,
    pub unique: bool,
    /// Optional list of column indices whose values are stored alongside the
    /// index key ("covering" / "INCLUDE" columns). When present, an index-only
    /// scan can return these values without touching the heap.
    pub covering_columns: Vec<usize>,
    /// For prefix indexes: max byte length of the indexed key prefix.
    /// 0 means full-value index (no prefix truncation).
    pub prefix_len: usize,
    pub tree: RwLock<BTreeMap<Vec<u8>, Vec<PrimaryKey>>>,
}

impl SecondaryIndex {
    pub const fn new(column_idx: usize) -> Self {
        Self {
            column_idx,
            column_indices: vec![],
            unique: false,
            covering_columns: vec![],
            prefix_len: 0,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    pub const fn new_unique(column_idx: usize) -> Self {
        Self {
            column_idx,
            column_indices: vec![],
            unique: true,
            covering_columns: vec![],
            prefix_len: 0,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a composite (multi-column) index.
    pub fn new_composite(column_indices: Vec<usize>, unique: bool) -> Self {
        let first = column_indices.first().copied().unwrap_or(0);
        Self {
            column_idx: first,
            column_indices,
            unique,
            covering_columns: vec![],
            prefix_len: 0,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a composite index with covering (INCLUDE) columns.
    pub fn new_covering(column_indices: Vec<usize>, covering: Vec<usize>, unique: bool) -> Self {
        let first = column_indices.first().copied().unwrap_or(0);
        Self {
            column_idx: first,
            column_indices,
            unique,
            covering_columns: covering,
            prefix_len: 0,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a prefix index (truncates key to prefix_len bytes).
    pub const fn new_prefix(column_idx: usize, prefix_len: usize) -> Self {
        Self {
            column_idx,
            column_indices: vec![],
            unique: false, // prefix indexes cannot enforce uniqueness
            covering_columns: vec![],
            prefix_len,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    /// Returns true if this is a composite (multi-column) index.
    pub const fn is_composite(&self) -> bool {
        self.column_indices.len() > 1
    }

    /// Returns the column indices this index covers (for key matching).
    pub fn key_columns(&self) -> &[usize] {
        if self.column_indices.is_empty() {
            std::slice::from_ref(&self.column_idx)
        } else {
            &self.column_indices
        }
    }

    /// Encode a composite key from a row.
    pub fn encode_key(&self, row: &OwnedRow) -> Vec<u8> {
        let cols = self.key_columns();
        let mut buf = Vec::with_capacity(cols.len() * 8);
        for &col_idx in cols {
            let datum = row.get(col_idx).unwrap_or(&Datum::Null);
            encode_datum_to_bytes(datum, &mut buf);
        }
        if self.prefix_len > 0 && buf.len() > self.prefix_len {
            buf.truncate(self.prefix_len);
        }
        buf
    }

    pub fn insert(&self, key_bytes: Vec<u8>, pk: PrimaryKey) {
        let mut tree = self.tree.write();
        tree.entry(key_bytes).or_default().push(pk);
    }

    /// Check uniqueness: if this index is unique and the key already maps to
    /// a different PK, return Err(DuplicateKey).
    pub fn check_unique(
        &self,
        key_bytes: &[u8],
        pk: &PrimaryKey,
    ) -> Result<(), falcon_common::error::StorageError> {
        if !self.unique {
            return Ok(());
        }
        let tree = self.tree.read();
        if let Some(pks) = tree.get(key_bytes) {
            if pks.iter().any(|existing| existing != pk) {
                return Err(falcon_common::error::StorageError::DuplicateKey);
            }
        }
        Ok(())
    }

    pub fn remove(&self, key_bytes: &[u8], pk: &PrimaryKey) {
        let mut tree = self.tree.write();
        if let Some(pks) = tree.get_mut(key_bytes) {
            pks.retain(|p| p != pk);
            if pks.is_empty() {
                tree.remove(key_bytes);
            }
        }
    }

    /// Range scan: return all PKs with index key in the specified range.
    ///
    /// - `lower`: optional lower bound `(key, inclusive)`
    /// - `upper`: optional upper bound `(key, inclusive)`
    ///
    /// Examples:
    /// - `range_scan(Some((k, true)), None)` → `key >= k`
    /// - `range_scan(Some((k, false)), None)` → `key > k`
    /// - `range_scan(None, Some((k, false)))` → `key < k`
    /// - `range_scan(Some((lo, true)), Some((hi, true)))` → `lo <= key <= hi`
    pub fn range_scan(
        &self,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
    ) -> Vec<PrimaryKey> {
        use std::ops::Bound;
        let tree = self.tree.read();
        let lo = match lower {
            Some((k, true)) => Bound::Included(k.to_vec()),
            Some((k, false)) => Bound::Excluded(k.to_vec()),
            None => Bound::Unbounded,
        };
        let hi = match upper {
            Some((k, true)) => Bound::Included(k.to_vec()),
            Some((k, false)) => Bound::Excluded(k.to_vec()),
            None => Bound::Unbounded,
        };
        let mut result = Vec::new();
        for (_k, pks) in tree.range((lo, hi)) {
            result.extend(pks.iter().cloned());
        }
        result
    }

    /// Prefix scan: return all PKs whose key starts with the given prefix.
    /// Useful for composite index leftmost-prefix queries and prefix indexes.
    pub fn prefix_scan(&self, prefix: &[u8]) -> Vec<PrimaryKey> {
        let tree = self.tree.read();
        let mut result = Vec::new();
        // BTreeMap range scan from prefix to prefix+1
        let start = prefix.to_vec();
        let mut end = prefix.to_vec();
        // Increment the last byte to form an exclusive upper bound
        let mut carry = true;
        for byte in end.iter_mut().rev() {
            if carry {
                if *byte == 0xFF {
                    *byte = 0x00;
                } else {
                    *byte += 1;
                    carry = false;
                }
            }
        }
        if carry {
            // All 0xFF — scan to end
            for (k, pks) in tree.range(start..) {
                if k.starts_with(prefix) {
                    result.extend(pks.iter().cloned());
                } else {
                    break;
                }
            }
        } else {
            for (_k, pks) in tree.range(start..end) {
                result.extend(pks.iter().cloned());
            }
        }
        result
    }
}

impl MemTable {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            data: DashMap::new(),
            secondary_indexes: RwLock::new(Vec::new()),
            gc_candidates: AtomicU64::new(0),
            committed_row_count: AtomicI64::new(0),
            pk_order: RwLock::new(BTreeSet::new()),
        }
    }

    pub const fn table_id(&self) -> TableId {
        self.schema.id
    }

    /// Insert a new row. Creates a new version chain or prepends to existing.
    /// Indexes are NOT updated here — they are updated at commit time (方案A).
    /// However, unique index constraints are checked eagerly against the committed index.
    pub fn insert(
        &self,
        row: OwnedRow,
        txn_id: TxnId,
    ) -> Result<PrimaryKey, falcon_common::error::StorageError> {
        let pk = encode_pk(&row, self.schema.pk_indices());

        // Check unique index constraints against committed index (read-only check)
        self.check_unique_indexes(&pk, &row)?;

        // Use entry() to avoid double DashMap lookup (get + insert → single shard lock).
        let entry = self.data.entry(pk.clone());
        match entry {
            dashmap::mapref::entry::Entry::Occupied(e) => {
                let chain = e.get();
                if chain.has_write_conflict(txn_id) {
                    return Err(falcon_common::error::StorageError::DuplicateKey);
                }
                if chain.has_live_version(txn_id) {
                    return Err(falcon_common::error::StorageError::DuplicateKey);
                }
                chain.prepend(txn_id, Some(row));
                self.gc_candidates.fetch_add(1, AtomicOrdering::Relaxed);
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let chain = Arc::new(VersionChain::new());
                chain.prepend(txn_id, Some(row));
                e.insert(chain);
            }
        }

        Ok(pk)
    }

    /// Update a row by PK. Prepends a new version.
    /// Indexes are NOT updated here — they are updated at commit time (方案A).
    pub fn update(
        &self,
        pk: &PrimaryKey,
        new_row: OwnedRow,
        txn_id: TxnId,
    ) -> Result<(), falcon_common::error::StorageError> {
        if let Some(chain) = self.data.get(pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(falcon_common::error::StorageError::DuplicateKey);
            }
            chain.prepend(txn_id, Some(new_row));
            self.gc_candidates.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(())
        } else {
            Err(falcon_common::error::StorageError::KeyNotFound)
        }
    }

    /// Delete a row by PK. Prepends a tombstone version.
    /// Indexes are NOT updated here — they are updated at commit time (方案A).
    pub fn delete(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
    ) -> Result<(), falcon_common::error::StorageError> {
        if let Some(chain) = self.data.get(pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(falcon_common::error::StorageError::DuplicateKey);
            }
            chain.prepend(txn_id, None); // tombstone
            self.gc_candidates.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(())
        } else {
            Err(falcon_common::error::StorageError::KeyNotFound)
        }
    }

    /// Point read by PK for a specific transaction.
    pub fn get(&self, pk: &PrimaryKey, txn_id: TxnId, read_ts: Timestamp) -> Option<OwnedRow> {
        self.data
            .get(pk)
            .and_then(|chain| chain.read_for_txn(txn_id, read_ts))
    }

    /// Stream through all visible rows without cloning.
    /// Calls the closure with a reference to each visible row's data.
    /// This avoids the O(N) allocation cost of scan() for aggregate/filter queries.
    pub fn for_each_visible<F>(&self, txn_id: TxnId, read_ts: Timestamp, mut f: F)
    where
        F: FnMut(&OwnedRow),
    {
        for entry in &self.data {
            entry.value().with_visible_data(txn_id, read_ts, |row| {
                f(row);
            });
        }
    }

    /// Parallel map-reduce over all visible rows.
    ///
    /// Phase 1: collect Arc<VersionChain> refs from DashMap (sequential, ~10ms for 1M).
    /// Phase 2: split into chunks, each thread creates its own accumulator via `init`,
    ///          processes rows via `map`, then all partial results are merged via `reduce`.
    ///
    /// Falls back to sequential for small tables (< 50K rows).
    #[allow(clippy::redundant_closure)]
    pub fn map_reduce_visible<T, Init, Map, Reduce>(
        &self,
        txn_id: TxnId,
        read_ts: Timestamp,
        init: Init,
        map: Map,
        reduce: Reduce,
    ) -> T
    where
        T: Send,
        Init: Fn() -> T + Sync,
        Map: Fn(&mut T, &OwnedRow) + Sync,
        Reduce: Fn(T, T) -> T,
    {
        let len = self.data.len();
        if len < 50_000 {
            let mut acc = init();
            for entry in &self.data {
                entry.value().with_visible_data(txn_id, read_ts, |row| {
                    map(&mut acc, row);
                });
            }
            return acc;
        }

        // Phase 1: collect chain refs (Arc pointer copies, no row data clone)
        let chains: Vec<Arc<VersionChain>> =
            self.data.iter().map(|e| e.value().clone()).collect();

        // Phase 2: parallel map-reduce
        let num_threads = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(4)
            .min(8);
        let chunk_size = chains.len().div_ceil(num_threads);

        std::thread::scope(|s| {
            let handles: Vec<_> = chains
                .chunks(chunk_size)
                .map(|chunk| {
                    let init = &init;
                    let map = &map;
                    s.spawn(move || {
                        let mut acc = init();
                        for chain in chunk {
                            chain.with_visible_data(txn_id, read_ts, |row| {
                                map(&mut acc, row);
                            });
                        }
                        acc
                    })
                })
                .collect();

            let results: Vec<T> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let merged = results.into_iter().reduce(|a, b| reduce(a, b));
            merged.unwrap_or_else(|| init())
        })
    }

    /// Full table scan visible to a transaction.
    pub fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        let mut results = Vec::with_capacity(self.data.len());
        for entry in &self.data {
            if let Some(row) = entry.value().read_for_txn(txn_id, read_ts) {
                results.push((entry.key().clone(), row));
            }
        }
        results
    }

    /// Scan returning only row data — avoids cloning the PK per row.
    /// Use this when the caller only needs row values (SELECT, aggregates, joins).
    pub fn scan_rows_only(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<OwnedRow> {
        let mut results = Vec::with_capacity(self.data.len());
        for entry in &self.data {
            if let Some(row) = entry.value().read_for_txn(txn_id, read_ts) {
                results.push(row);
            }
        }
        results
    }

    /// Commit all writes by a transaction (legacy full-scan path).
    /// Also maintains secondary indexes at commit time (方案A).
    pub fn commit_txn(&self, txn_id: TxnId, commit_ts: Timestamp) {
        // Collect pk_order changes locally to avoid acquiring the global RwLock
        // once per key. A single bulk write at the end reduces contention from
        // O(N) write-lock acquisitions to O(1), matching commit_keys behaviour.
        let mut pk_inserts: Vec<PrimaryKey> = Vec::new();
        let mut pk_deletes: Vec<PrimaryKey> = Vec::new();
        let mut row_delta: i64 = 0;

        for entry in &self.data {
            let pk = entry.key().clone();
            let (new_data, old_data) = entry.value().commit_and_report(txn_id, commit_ts);
            match (new_data.is_some(), old_data.is_some()) {
                (true, false) => { row_delta += 1; pk_inserts.push(pk.clone()); }
                (false, true) => { row_delta -= 1; pk_deletes.push(pk.clone()); }
                _ => {}
            }
            self.index_update_on_commit(&pk, new_data.as_ref(), old_data.as_ref());
        }

        if row_delta > 0 {
            self.committed_row_count.fetch_add(row_delta, AtomicOrdering::Relaxed);
        } else if row_delta < 0 {
            self.committed_row_count.fetch_sub(-row_delta, AtomicOrdering::Relaxed);
        }
        if !pk_inserts.is_empty() || !pk_deletes.is_empty() {
            let mut order = self.pk_order.write();
            for pk in pk_inserts { order.insert(pk); }
            for pk in pk_deletes { order.remove(&pk); }
        }
    }

    /// Commit writes for specific keys of a transaction.
    /// Performs **commit-time unique constraint re-validation** before applying
    /// MVCC commits and index updates (方案A).
    ///
    /// Atomicity: if any unique constraint is violated, NO writes are committed.
    /// Returns `Err(UniqueViolation)` if a concurrent committed txn created a
    /// conflicting index entry since the insert-time check.
    pub fn commit_keys(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
        keys: &[PrimaryKey],
    ) -> Result<(), falcon_common::error::StorageError> {
        // Phase 1: Validate unique constraints for all affected rows.
        // We must check BEFORE committing any version, so that a failure
        // leaves the data unchanged.
        self.validate_unique_constraints_for_commit(txn_id, keys)?;

        // Phase 2: All checks passed — apply MVCC commits + index updates.
        // Collect pk_order changes in local vecs to avoid acquiring the
        // global RwLock N times (one per key). A single bulk write at the
        // end reduces contention from O(N) locks to O(1) per transaction.
        let mut pk_inserts: Vec<PrimaryKey> = Vec::new();
        let mut pk_deletes: Vec<PrimaryKey> = Vec::new();
        let mut row_delta: i64 = 0;

        for pk in keys {
            if let Some(chain) = self.data.get(pk) {
                let (new_data, old_data) = chain.commit_and_report(txn_id, commit_ts);
                match (new_data.is_some(), old_data.is_some()) {
                    (true, false) => {
                        row_delta += 1;
                        pk_inserts.push(pk.clone());
                    }
                    (false, true) => {
                        row_delta -= 1;
                        pk_deletes.push(pk.clone());
                    }
                    _ => {}
                }
                self.index_update_on_commit(pk, new_data.as_ref(), old_data.as_ref());
            }
        }

        // Apply row count atomically.
        if row_delta > 0 {
            self.committed_row_count.fetch_add(row_delta, AtomicOrdering::Relaxed);
        } else if row_delta < 0 {
            self.committed_row_count.fetch_sub(-row_delta, AtomicOrdering::Relaxed);
        }

        // Apply pk_order changes with a single write-lock acquisition.
        if !pk_inserts.is_empty() || !pk_deletes.is_empty() {
            let mut order = self.pk_order.write();
            for pk in pk_inserts {
                order.insert(pk);
            }
            for pk in pk_deletes {
                order.remove(&pk);
            }
        }

        Ok(())
    }

    /// Abort all writes by a transaction (legacy full-scan path).
    /// Under 方案A, indexes are not touched at DML time, so no index rollback needed.
    pub fn abort_txn(&self, txn_id: TxnId) {
        for entry in &self.data {
            entry.value().abort_and_report(txn_id);
        }
    }

    /// Abort writes for specific keys of a transaction.
    /// Under 方案A, indexes are not touched at DML time, so no index rollback needed.
    pub fn abort_keys(&self, txn_id: TxnId, keys: &[PrimaryKey]) {
        for pk in keys {
            if let Some(chain) = self.data.get(pk) {
                chain.abort_and_report(txn_id);
            }
        }
    }

    /// Check if a key has a version committed after `after_ts` by another transaction.
    /// Used for OCC read-set validation.
    pub fn has_committed_write_after(
        &self,
        pk: &PrimaryKey,
        exclude_txn: TxnId,
        after_ts: Timestamp,
    ) -> bool {
        self.data.get(pk).is_some_and(|chain| chain.has_committed_write_after(exclude_txn, after_ts))
    }

    /// Count visible rows without cloning row data.
    /// Fast path: if no GC candidates exist (INSERT-only workload), the
    /// committed_row_count is exact and avoids the O(N) DashMap scan entirely.
    pub fn count_visible(&self, txn_id: TxnId, read_ts: Timestamp) -> usize {
        let count = self.committed_row_count.load(AtomicOrdering::Acquire);
        if count >= 0 && self.gc_candidates.load(AtomicOrdering::Relaxed) == 0 {
            return count as usize;
        }
        // Slow path: full scan with per-row visibility checks
        let mut n = 0;
        for entry in &self.data {
            if entry.value().is_visible(txn_id, read_ts) {
                n += 1;
            }
        }
        n
    }

    /// Compute simple aggregates (COUNT*, SUM, MIN, MAX) in a single streaming
    /// pass over visible rows WITHOUT cloning any row data.
    /// `agg_specs`: list of (AggOp, Option<col_index>). None col = COUNT(*).
    /// Returns one Datum per spec.
    pub fn compute_simple_aggs(
        &self,
        txn_id: TxnId,
        read_ts: Timestamp,
        agg_specs: &[(SimpleAggOp, Option<usize>)],
    ) -> Vec<Datum> {
        let n = agg_specs.len();
        let mut counts = vec![0i64; n];
        let mut sums_i = vec![0i64; n];
        let mut sums_f = vec![0f64; n];
        let mut is_float = vec![false; n];
        let mut mins: Vec<Option<Datum>> = vec![None; n];
        let mut maxs: Vec<Option<Datum>> = vec![None; n];

        for entry in &self.data {
            entry.value().with_visible_data(txn_id, read_ts, |row| {
                for (i, (op, col_opt)) in agg_specs.iter().enumerate() {
                    match op {
                        SimpleAggOp::CountStar => {
                            counts[i] += 1;
                        }
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
                                let d = &row.values[*ci];
                                if !matches!(d, Datum::Null)
                                    && (mins[i].is_none()
                                        || d.partial_cmp(mins[i].as_ref().unwrap())
                                            == Some(std::cmp::Ordering::Less))
                                {
                                    mins[i] = Some(d.clone());
                                }
                            }
                        }
                        SimpleAggOp::Max => {
                            if let Some(ci) = col_opt {
                                let d = &row.values[*ci];
                                if !matches!(d, Datum::Null)
                                    && (maxs[i].is_none()
                                        || d.partial_cmp(maxs[i].as_ref().unwrap())
                                            == Some(std::cmp::Ordering::Greater))
                                {
                                    maxs[i] = Some(d.clone());
                                }
                            }
                        }
                    }
                }
            });
        }

        // Build result Datums
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
            .collect()
    }

    /// Scan top-K rows ordered by PK.
    /// Fast path: uses the ordered PK index (BTreeSet) for O(K) lookups.
    /// Fallback: bounded heap scan O(N log K) when pk_order is empty.
    pub fn scan_top_k_by_pk(
        &self,
        txn_id: TxnId,
        read_ts: Timestamp,
        k: usize,
        ascending: bool,
    ) -> Vec<(PrimaryKey, OwnedRow)> {
        if k == 0 {
            return vec![];
        }

        // Fast path: use ordered PK index (O(K) point lookups instead of O(N) scan)
        let pk_set = self.pk_order.read();
        if !pk_set.is_empty() {
            let mut results = Vec::with_capacity(k);
            if ascending {
                for pk in pk_set.iter() {
                    if let Some(row) = self.get(pk, txn_id, read_ts) {
                        results.push((pk.clone(), row));
                        if results.len() >= k {
                            break;
                        }
                    }
                }
            } else {
                for pk in pk_set.iter().rev() {
                    if let Some(row) = self.get(pk, txn_id, read_ts) {
                        results.push((pk.clone(), row));
                        if results.len() >= k {
                            break;
                        }
                    }
                }
            }
            return results;
        }
        drop(pk_set);

        // Fallback: bounded heap scan (O(N log K))
        let top_pks = if ascending {
            let mut heap: BinaryHeap<PrimaryKey> = BinaryHeap::with_capacity(k + 1);
            for entry in &self.data {
                if entry.value().is_visible(txn_id, read_ts) {
                    let pk = entry.key();
                    if heap.len() < k {
                        heap.push(pk.clone());
                    } else if let Some(top) = heap.peek() {
                        if pk.as_slice() < top.as_slice() {
                            heap.pop();
                            heap.push(pk.clone());
                        }
                    }
                }
            }
            heap.into_sorted_vec()
        } else {
            let mut heap: BinaryHeap<Reverse<PrimaryKey>> =
                BinaryHeap::with_capacity(k + 1);
            for entry in &self.data {
                if entry.value().is_visible(txn_id, read_ts) {
                    let pk = entry.key();
                    if heap.len() < k {
                        heap.push(Reverse(pk.clone()));
                    } else if let Some(Reverse(top)) = heap.peek() {
                        if pk.as_slice() > top.as_slice() {
                            heap.pop();
                            heap.push(Reverse(pk.clone()));
                        }
                    }
                }
            }
            let mut pks: Vec<PrimaryKey> =
                heap.into_sorted_vec().into_iter().map(|r| r.0).collect();
            pks.reverse();
            pks
        };
        // Phase 2: materialize only K rows via point lookups
        let mut results = Vec::with_capacity(top_pks.len());
        for pk in top_pks {
            if let Some(row) = self.get(&pk, txn_id, read_ts) {
                results.push((pk, row));
            }
        }
        results
    }

    /// Row count (approximate — counts all chains with at least one committed version).
    pub fn row_count_approx(&self) -> usize {
        self.data.len()
    }

    /// Approximate number of multi-version chain writes pending GC.
    /// When 0, GC can safely skip the full DashMap iteration.
    pub fn gc_candidates(&self) -> u64 {
        self.gc_candidates.load(AtomicOrdering::Relaxed)
    }

    /// Decrement the GC candidate counter after reclaiming versions.
    pub fn gc_candidates_sub(&self, n: u64) {
        // Saturating subtract to avoid underflow from races
        let prev = self.gc_candidates.fetch_sub(n, AtomicOrdering::Relaxed);
        if prev < n {
            // Raced past zero; reset to 0
            self.gc_candidates.store(0, AtomicOrdering::Relaxed);
        }
    }

    // ── Secondary index helpers ─────────────────────────────────────

    /// Commit-time unique constraint re-validation.
    /// For each key in the write-set that has an uncommitted insert/update,
    /// check all unique indexes to ensure no *other* committed PK holds the
    /// same index key. This catches concurrent-insert races that the eager
    /// insert-time check cannot detect under 方案A.
    fn validate_unique_constraints_for_commit(
        &self,
        txn_id: TxnId,
        keys: &[PrimaryKey],
    ) -> Result<(), falcon_common::error::StorageError> {
        let indexes = self.secondary_indexes.read();
        let has_unique = indexes.iter().any(|idx| idx.unique);
        if !has_unique {
            return Ok(());
        }

        for pk in keys {
            // Read the uncommitted row this txn is about to commit
            let uncommitted = self.data.get(pk).and_then(|chain| chain.read_uncommitted_for_txn(txn_id));

            // Only check inserts/updates (Some(Some(row))), skip deletes (Some(None))
            let new_row = match uncommitted {
                Some(Some(row)) => row,
                _ => continue,
            };

            for idx in indexes.iter() {
                if !idx.unique {
                    continue;
                }
                {
                    let key_bytes = idx.encode_key(&new_row);
                    // Check if another committed PK already owns this key
                    idx.check_unique(&key_bytes, pk).map_err(|_| {
                        falcon_common::error::StorageError::UniqueViolation {
                            column_idx: idx.column_idx,
                            index_key_hex: hex_encode(&key_bytes),
                        }
                    })?;
                }
            }
        }
        Ok(())
    }

    /// Check unique index constraints against the committed index (read-only).
    /// Used at DML time to eagerly detect violations under 方案A.
    fn check_unique_indexes(
        &self,
        pk: &PrimaryKey,
        row: &OwnedRow,
    ) -> Result<(), falcon_common::error::StorageError> {
        let indexes = self.secondary_indexes.read();
        for idx in indexes.iter() {
            if idx.unique {
                let key_bytes = idx.encode_key(row);
                idx.check_unique(&key_bytes, pk)?;
            }
        }
        Ok(())
    }

    /// Update secondary indexes at commit time (方案A).
    /// Given the newly committed row data and the prior committed row data,
    /// remove old index entries and add new ones.
    fn index_update_on_commit(
        &self,
        pk: &PrimaryKey,
        new_data: Option<&OwnedRow>,
        old_data: Option<&OwnedRow>,
    ) {
        let indexes = self.secondary_indexes.read();
        if indexes.is_empty() {
            return;
        }
        // Remove old index entries (if there was a prior committed version)
        if let Some(old_row) = old_data {
            for idx in indexes.iter() {
                let key_bytes = idx.encode_key(old_row);
                idx.remove(&key_bytes, pk);
            }
        }
        // Add new index entries (if the new version is not a tombstone)
        if let Some(new_row) = new_data {
            for idx in indexes.iter() {
                let key_bytes = idx.encode_key(new_row);
                idx.insert(key_bytes, pk.clone());
            }
        }
    }

    /// Add a row's indexed column values to all secondary indexes.
    /// Returns Err(DuplicateKey) if a unique index is violated.
    /// Used by backfill (create_index) and rebuild.
    #[allow(dead_code)]
    pub(crate) fn index_insert_row(
        &self,
        pk: &PrimaryKey,
        row: &OwnedRow,
    ) -> Result<(), falcon_common::error::StorageError> {
        let indexes = self.secondary_indexes.read();
        // Check unique constraints first (before mutating any index)
        for idx in indexes.iter() {
            if idx.unique {
                let key_bytes = idx.encode_key(row);
                idx.check_unique(&key_bytes, pk)?;
            }
        }
        // All checks passed — insert into all indexes
        for idx in indexes.iter() {
            let key_bytes = idx.encode_key(row);
            idx.insert(key_bytes, pk.clone());
        }
        Ok(())
    }

    /// Remove a row's indexed column values from all secondary indexes.
    #[allow(dead_code)]
    fn index_remove_row(&self, pk: &PrimaryKey, row: &OwnedRow) {
        let indexes = self.secondary_indexes.read();
        for idx in indexes.iter() {
            let key_bytes = idx.encode_key(row);
            idx.remove(&key_bytes, pk);
        }
    }

    /// Rebuild all secondary indexes from current data.
    /// Used after WAL recovery to restore index state.
    pub fn rebuild_secondary_indexes(&self) {
        let indexes = self.secondary_indexes.read();
        // Clear all existing index entries
        for idx in indexes.iter() {
            let mut tree = idx.tree.write();
            tree.clear();
        }
        // Re-insert from current data
        for entry in &self.data {
            let pk = entry.key().clone();
            let chain = entry.value();
            if let Some(row) = chain.read_latest() {
                for idx in indexes.iter() {
                    let key_bytes = idx.encode_key(&row);
                    idx.insert(key_bytes, pk.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod index_tests {
    use super::*;
    use falcon_common::datum::Datum;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{DataType, TableId, TxnId};

    fn col(name: &str, dt: DataType, pk: bool) -> ColumnDef {
        ColumnDef {
            id: falcon_common::types::ColumnId(0),
            name: name.to_string(),
            data_type: dt,
            nullable: !pk,
            is_primary_key: pk,
            default_value: None,
            is_serial: false,
        }
    }

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "test".to_string(),
            columns: vec![
                col("id", DataType::Int64, true),
                col("a", DataType::Int64, false),
                col("b", DataType::Text, false),
                col("c", DataType::Int64, false),
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        }
    }

    fn make_pk(id: i64) -> PrimaryKey {
        encode_column_value(&Datum::Int64(id))
    }

    fn make_row(id: i64, a: i64, b: &str, c: i64) -> OwnedRow {
        OwnedRow::new(vec![
            Datum::Int64(id),
            Datum::Int64(a),
            Datum::Text(b.to_string()),
            Datum::Int64(c),
        ])
    }

    #[test]
    fn test_composite_index_encode_key() {
        let idx = SecondaryIndex::new_composite(vec![1, 2], false);
        let row = make_row(1, 10, "hello", 20);
        let key = idx.encode_key(&row);
        assert!(!key.is_empty());

        // Different row with same (a, b) should produce same key
        let row2 = make_row(2, 10, "hello", 99);
        assert_eq!(idx.encode_key(&row2), key);

        // Different (a, b) should produce different key
        let row3 = make_row(3, 10, "world", 20);
        assert_ne!(idx.encode_key(&row3), key);
    }

    #[test]
    fn test_composite_index_insert_lookup() {
        let idx = SecondaryIndex::new_composite(vec![1, 2], false);
        let row1 = make_row(1, 10, "hello", 20);
        let row2 = make_row(2, 10, "hello", 30);
        let row3 = make_row(3, 20, "world", 40);

        let pk1 = make_pk(1);
        let pk2 = make_pk(2);
        let pk3 = make_pk(3);

        idx.insert(idx.encode_key(&row1), pk1.clone());
        idx.insert(idx.encode_key(&row2), pk2.clone());
        idx.insert(idx.encode_key(&row3), pk3.clone());

        // Lookup by exact composite key (a=10, b="hello")
        let key = idx.encode_key(&row1);
        let tree = idx.tree.read();
        let found = tree.get(&key).unwrap();
        assert_eq!(found.len(), 2);
        assert!(found.contains(&pk1));
        assert!(found.contains(&pk2));
    }

    #[test]
    fn test_composite_unique_index() {
        let idx = SecondaryIndex::new_composite(vec![1, 2], true);
        let row1 = make_row(1, 10, "hello", 20);
        let pk1 = make_pk(1);
        let pk2 = make_pk(2);

        idx.insert(idx.encode_key(&row1), pk1.clone());

        let key = idx.encode_key(&row1);
        assert!(idx.check_unique(&key, &pk2).is_err());
        assert!(idx.check_unique(&key, &pk1).is_ok());
    }

    #[test]
    fn test_prefix_index() {
        let idx = SecondaryIndex::new_prefix(2, 3);
        let row1 = make_row(1, 10, "hello", 20);
        let row2 = make_row(2, 10, "help", 30);
        let row3 = make_row(3, 10, "world", 40);

        let key1 = idx.encode_key(&row1);
        let key2 = idx.encode_key(&row2);
        let key3 = idx.encode_key(&row3);

        assert_eq!(key1.len(), 3);
        assert_eq!(key2.len(), 3);
        assert_eq!(key1, key2); // both truncated to same prefix
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_prefix_scan() {
        let idx = SecondaryIndex::new(1);
        let pk1 = make_pk(1);
        let pk2 = make_pk(2);

        let key1 = encode_column_value(&Datum::Int64(10));
        let key2 = encode_column_value(&Datum::Int64(20));

        idx.insert(key1.clone(), pk1.clone());
        idx.insert(key2.clone(), pk2.clone());

        let results = idx.prefix_scan(&key1);
        assert!(results.contains(&pk1));
        assert!(!results.contains(&pk2));
    }

    #[test]
    fn test_covering_index_metadata() {
        let idx = SecondaryIndex::new_covering(vec![1, 2], vec![3], false);
        assert!(idx.is_composite());
        assert_eq!(idx.key_columns(), &[1, 2]);
        assert_eq!(idx.covering_columns, vec![3]);
        assert!(!idx.unique);
    }

    #[test]
    fn test_single_column_key_columns() {
        let idx = SecondaryIndex::new(2);
        assert!(!idx.is_composite());
        assert_eq!(idx.key_columns(), &[2]);
    }

    #[test]
    fn test_composite_index_remove() {
        let idx = SecondaryIndex::new_composite(vec![1, 2], false);
        let row = make_row(1, 10, "hello", 20);
        let pk = make_pk(1);

        let key = idx.encode_key(&row);
        idx.insert(key.clone(), pk.clone());
        {
            let tree = idx.tree.read();
            assert!(tree.get(&key).is_some());
        }

        idx.remove(&key, &pk);
        {
            let tree = idx.tree.read();
            assert!(tree.get(&key).is_none());
        }
    }

    #[test]
    fn test_decimal_index_encoding() {
        let d1 = Datum::Decimal(12345, 2);
        let d2 = Datum::Decimal(12346, 2);
        let d3 = Datum::Decimal(-100, 2);

        let k1 = encode_column_value(&d1);
        let k2 = encode_column_value(&d2);
        let k3 = encode_column_value(&d3);

        assert!(k3 < k1);
        assert!(k1 < k2);
    }

    #[test]
    fn test_memtable_composite_index_commit() {
        let schema = test_schema();
        let mt = MemTable::new(schema);

        {
            let mut indexes = mt.secondary_indexes.write();
            indexes.push(SecondaryIndex::new_composite(vec![1, 2], false));
        }

        let txn = TxnId(100);
        let row = make_row(1, 10, "hello", 20);

        let pk = mt.insert(row.clone(), txn).unwrap();
        mt.commit_keys(txn, falcon_common::types::Timestamp(200), &[pk.clone()])
            .unwrap();

        let indexes = mt.secondary_indexes.read();
        let idx = &indexes[0];
        let key = idx.encode_key(&row);
        let tree = idx.tree.read();
        let pks = tree.get(&key).unwrap();
        assert!(pks.contains(&pk));
    }
}
