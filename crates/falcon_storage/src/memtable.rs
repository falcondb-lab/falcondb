//! # Module Status: PRODUCTION
//! In-memory row store �?the primary storage engine for FalconDB OLTP.
//! This is the **only** production write path for row data.
//!
//! ## Golden Path (OLTP Write)
//! ```text
//! TxnManager.begin() �?Executor DML
//!   �?MemTable.insert / update / delete  [MVCC version chain]
//!   �?TxnManager.commit()
//!     �?OCC validation (read-set / write-set)
//!     �?WAL append (WalRecord::InsertRow / UpdateRow / DeleteRow)
//!     �?Version chain commit (set commit_ts on all written versions)
//! ```
//!
//! ## Prohibited Patterns
//! - Direct MemTable mutation without an active TxnId �?violates MVCC
//! - Bypassing WAL append on commit �?violates crash-safety
//! - Non-transactional reads that skip visibility checks �?violates isolation

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;
use smallvec::SmallVec;

/// Number of lanes for per-shard counter stripes.
/// Must be a power of two. 16 lanes × 64-byte cache line = 1 KB per counter.
const COUNTER_STRIPES: usize = 16;

/// Cache-line padded i64 lane for StripedI64.
#[repr(align(64))]
struct I64Lane(AtomicI64);

/// Cache-line padded u64 lane for StripedU64.
#[repr(align(64))]
struct U64Lane(AtomicU64);

/// Striped signed counter: distributes writes across COUNTER_STRIPES independent
/// cache lines.  Each thread writes to `thread_id & (STRIPES-1)`.  Reads sum all
/// lanes — O(STRIPES), cheap for STRIPES=16.
pub(crate) struct StripedI64 {
    lanes: [I64Lane; COUNTER_STRIPES],
}

impl StripedI64 {
    const fn new(init: i64) -> Self {
        // Only lane 0 gets the initial value; others start at 0.
        // This is correct for initialising to 0 (all lanes 0).
        // For non-zero init, caller should use store() after construction.
        let _ = init; // init must be 0 for const construction
        Self {
            lanes: [
                I64Lane(AtomicI64::new(0)), I64Lane(AtomicI64::new(0)),
                I64Lane(AtomicI64::new(0)), I64Lane(AtomicI64::new(0)),
                I64Lane(AtomicI64::new(0)), I64Lane(AtomicI64::new(0)),
                I64Lane(AtomicI64::new(0)), I64Lane(AtomicI64::new(0)),
                I64Lane(AtomicI64::new(0)), I64Lane(AtomicI64::new(0)),
                I64Lane(AtomicI64::new(0)), I64Lane(AtomicI64::new(0)),
                I64Lane(AtomicI64::new(0)), I64Lane(AtomicI64::new(0)),
                I64Lane(AtomicI64::new(0)), I64Lane(AtomicI64::new(0)),
            ],
        }
    }

    #[inline]
    fn lane() -> usize {
        // Mix the thread-id bits into a stripe index.  The OS thread-id is a
        // reasonable proxy for a per-core slot: same thread always hashes to
        // the same lane, different threads to different lanes.
        thread_id::get() & (COUNTER_STRIPES - 1)
    }

    #[inline]
    pub(crate) fn fetch_add(&self, delta: i64, order: AtomicOrdering) {
        self.lanes[Self::lane()].0.fetch_add(delta, order);
    }

    #[inline]
    pub(crate) fn fetch_sub(&self, delta: i64, order: AtomicOrdering) {
        self.lanes[Self::lane()].0.fetch_sub(delta, order);
    }

    #[inline]
    pub(crate) fn load(&self, order: AtomicOrdering) -> i64 {
        self.lanes.iter().map(|l| l.0.load(order)).sum()
    }

    pub(crate) fn store_all_zero(&self, order: AtomicOrdering) {
        for l in &self.lanes {
            l.0.store(0, order);
        }
    }
}

/// Striped unsigned counter: same pattern as StripedI64.
pub(crate) struct StripedU64 {
    lanes: [U64Lane; COUNTER_STRIPES],
}

impl StripedU64 {
    const fn new() -> Self {
        Self {
            lanes: [
                U64Lane(AtomicU64::new(0)), U64Lane(AtomicU64::new(0)),
                U64Lane(AtomicU64::new(0)), U64Lane(AtomicU64::new(0)),
                U64Lane(AtomicU64::new(0)), U64Lane(AtomicU64::new(0)),
                U64Lane(AtomicU64::new(0)), U64Lane(AtomicU64::new(0)),
                U64Lane(AtomicU64::new(0)), U64Lane(AtomicU64::new(0)),
                U64Lane(AtomicU64::new(0)), U64Lane(AtomicU64::new(0)),
                U64Lane(AtomicU64::new(0)), U64Lane(AtomicU64::new(0)),
                U64Lane(AtomicU64::new(0)), U64Lane(AtomicU64::new(0)),
            ],
        }
    }

    #[inline]
    fn lane() -> usize {
        thread_id::get() & (COUNTER_STRIPES - 1)
    }

    #[inline]
    pub(crate) fn fetch_add(&self, delta: u64, order: AtomicOrdering) -> u64 {
        self.lanes[Self::lane()].0.fetch_add(delta, order)
    }

    #[inline]
    pub(crate) fn load(&self, order: AtomicOrdering) -> u64 {
        self.lanes.iter().map(|l| l.0.load(order)).sum()
    }

    pub(crate) fn store_all_zero(&self, order: AtomicOrdering) {
        for l in &self.lanes {
            l.0.store(0, order);
        }
    }
}

/// Minimal thread-id helper: wraps the OS thread id into a usize for stripe selection.
mod thread_id {
    #[inline]
    pub fn get() -> usize {
        // Use a thread-local counter as a cheap stable per-thread index.
        thread_local! {
            static ID: usize = {
                use std::sync::atomic::{AtomicUsize, Ordering};
                static NEXT: AtomicUsize = AtomicUsize::new(0);
                NEXT.fetch_add(1, Ordering::Relaxed)
            };
        }
        ID.with(|id| *id)
    }
}

/// Stack-allocated key buffer �?avoids heap allocation for keys up to 64 bytes.
/// opt6: replaces Vec<u8> in encode_key() hot path.
pub type KeyBuf = SmallVec<[u8; 64]>;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::TableSchema;
use falcon_common::types::{TableId, Timestamp, TxnId};

use crate::version_chain2::VersionChain2;
use crate::version_arena::VersionArena;
use crate::scan_optimizer::{PrefetchIterator, RowBatch, SimdAggregator, extract_i64_column};

/// Extract OwnedRow from Arc without cloning when ref count is 1.
#[inline]
fn unwrap_arc_row(arc: Arc<OwnedRow>) -> OwnedRow {
    Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone())
}

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

/// Stack-allocated PK buffer �?zero heap allocation for PKs up to 16 bytes.
/// Covers: single Int32 (5B), single Int64 (9B), UUID (17B �?spills, rare).
pub type PkBuf = SmallVec<[u8; 16]>;

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

/// Stack-allocated PK encode �?avoids heap allocation on point-get hot path.
/// Returns a `PkBuf` (SmallVec<[u8;16]>) that can be passed to DashMap as
/// `buf.as_slice()` (Vec<u8>: Borrow<[u8]> �?DashMap 6 accepts &[u8] as key).
#[inline]
pub fn encode_pk_stack(row: &OwnedRow, pk_indices: &[usize]) -> PkBuf {
    let mut buf: PkBuf = SmallVec::new();
    for &idx in pk_indices {
        if let Some(datum) = row.get(idx) {
            encode_datum_to_bytes(datum, &mut buf);
        }
    }
    buf
}

/// Stack-allocated PK encode from raw datum slices.
#[inline]
pub fn encode_pk_from_datums_stack(datums: &[&Datum]) -> PkBuf {
    let mut buf: PkBuf = SmallVec::new();
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

fn encode_datum_to_bytes<B: Extend<u8>>(datum: &Datum, buf: &mut B) {
    match datum {
        Datum::Null => {
            buf.extend([0x00u8]);
        }
        Datum::Boolean(b) => {
            buf.extend([0x01u8, if *b { 1 } else { 0 }]);
        }
        Datum::Int32(v) => {
            let encoded = (*v as u32) ^ (1u32 << 31);
            buf.extend(std::iter::once(0x02u8).chain(encoded.to_be_bytes()));
        }
        Datum::Int64(v) => {
            let encoded = (*v as u64) ^ (1u64 << 63);
            buf.extend(std::iter::once(0x03u8).chain(encoded.to_be_bytes()));
        }
        Datum::Float64(v) => {
            let bits = v.to_bits();
            let encoded = if bits & (1u64 << 63) != 0 { !bits } else { bits ^ (1u64 << 63) };
            buf.extend(std::iter::once(0x04u8).chain(encoded.to_be_bytes()));
        }
        Datum::Text(s) => {
            buf.extend([0x05u8]);
            let bytes = s.as_bytes();
            let mut start = 0usize;
            for (i, &b) in bytes.iter().enumerate() {
                if b == 0x00 {
                    buf.extend(bytes[start..i].iter().copied());
                    buf.extend([0x00u8, 0x01u8]);
                    start = i + 1;
                }
            }
            buf.extend(bytes[start..].iter().copied());
            buf.extend([0x00u8, 0x00u8]);
        }
        Datum::Timestamp(v) => {
            let encoded = (*v as u64) ^ (1u64 << 63);
            buf.extend(std::iter::once(0x06u8).chain(encoded.to_be_bytes()));
        }
        Datum::Date(v) => {
            let encoded = (*v as u32) ^ (1u32 << 31);
            buf.extend(std::iter::once(0x09u8).chain(encoded.to_be_bytes()));
        }
        Datum::Array(elems) => {
            buf.extend([0x07u8]);
            buf.extend((elems.len() as u32).to_be_bytes());
            for elem in elems {
                encode_datum_to_bytes(elem, buf);
            }
        }
        Datum::Jsonb(v) => {
            buf.extend([0x08u8]);
            let s = v.to_string();
            buf.extend(s.as_bytes().iter().copied());
            buf.extend([0x00u8]);
        }
        Datum::Decimal(mantissa, scale) => {
            let encoded = (*mantissa as u128) ^ (1u128 << 127);
            buf.extend([0x0Au8, *scale]);
            buf.extend(encoded.to_be_bytes());
        }
        Datum::Time(us) => {
            let encoded = (*us as u64) ^ (1u64 << 63);
            buf.extend(std::iter::once(0x0Bu8).chain(encoded.to_be_bytes()));
        }
        Datum::Interval(months, days, us) => {
            let em = (*months as u32) ^ (1u32 << 31);
            let ed = (*days as u32) ^ (1u32 << 31);
            let eu = (*us as u64) ^ (1u64 << 63);
            buf.extend([0x0Cu8]);
            buf.extend(em.to_be_bytes());
            buf.extend(ed.to_be_bytes());
            buf.extend(eu.to_be_bytes());
        }
        Datum::Uuid(v) => {
            buf.extend(std::iter::once(0x0Du8).chain(v.to_be_bytes()));
        }
        Datum::Bytea(bytes) => {
            buf.extend([0x0Eu8]);
            buf.extend((bytes.len() as u32).to_be_bytes());
            buf.extend(bytes.iter().copied());
        }
        Datum::TsVector(entries) => {
            let s = format!("{}", Datum::TsVector(entries.clone()));
            buf.extend([0x0Fu8]);
            buf.extend((s.len() as u32).to_be_bytes());
            buf.extend(s.as_bytes().iter().copied());
        }
        Datum::TsQuery(q) => {
            buf.extend([0x10u8]);
            buf.extend((q.len() as u32).to_be_bytes());
            buf.extend(q.as_bytes().iter().copied());
        }
    }
}

/// In-memory table: concurrent hash map of PK → VersionChain2.
/// Also maintains secondary indexes.
pub struct MemTable {
    pub schema: TableSchema,
    /// Primary data: PK bytes → version chain.
    /// N-01: migrated to VersionChain2 (lock-free inline slot + arena overflow).
    pub data: DashMap<PrimaryKey, Arc<VersionChain2>>,
    /// Per-table arena for VersionChain2 overflow nodes.
    /// Shared across all version chains in this table.
    pub(crate) arena: Arc<VersionArena>,
    /// Secondary B-tree indexes: column_index → (encoded_value → set of PKs).
    pub secondary_indexes: RwLock<Vec<SecondaryIndex>>,
    /// opt5: column_idx → slot in secondary_indexes Vec. O(1) dispatch without linear scan.
    pub(crate) column_to_index: parking_lot::RwLock<HashMap<usize, usize>>,
    /// Fast check: true when secondary_indexes is non-empty. Avoids RwLock read on hot path.
    pub(crate) has_secondary_idx: AtomicBool,
    /// Hint counter: approximate number of writes that created multi-version
    /// chains (insert-to-existing, update, delete). GC can skip the full
    /// DashMap iteration when this is 0 �?eliminating O(n) shard-lock
    /// contention on INSERT-only workloads.
    gc_candidates: AtomicU64,
    /// Exact count of committed live (non-tombstone) rows.
    /// Incremented on INSERT commit, decremented on DELETE commit.
    /// Enables O(1) COUNT(*) without WHERE clause.
    /// P2-2: Striped across COUNTER_STRIPES lanes to reduce cache-line bouncing
    /// between cores on the commit hot path.
    pub(crate) committed_row_count: StripedI64,
    /// Modification counter for auto-analyze. Avoids DashMap lookup per commit.
    /// P2-2: Striped to reduce contention at high core counts.
    pub(crate) rows_modified: StripedU64,
    /// Cached auto-analyze threshold. Updated after each analyze_table() run.
    pub(crate) analyze_threshold: AtomicU64,
    /// Ordered PK index: rebuilt lazily on range-scan requests.
    /// Never written on the commit hot path �?use pk_order_dirty to signal staleness.
    pub(crate) pk_order: RwLock<BTreeSet<PrimaryKey>>,
    /// Set to `true` by commit paths to signal that pk_order needs rebuilding.
    /// Checked (and cleared) lazily by range-scan / top-k paths.
    /// Avoids any RwLock::write() on pk_order in the commit critical section (P0-3).
    pub(crate) pk_order_dirty: AtomicBool,
    /// GIN inverted indexes for tsvector columns: lexeme → set of PKs.
    pub gin_indexes: RwLock<Vec<GinIndex>>,
    // ── Hot Row Optimization (H1) ─────────────────────────────────────────────
    /// Per-PK write-frequency counter. Incremented on every committed UPDATE/DELETE.
    /// When count ≥ HOT_ROW_THRESHOLD the GC path triggers coalesce_hot_row()
    /// to collapse the overflow chain to a single committed version, keeping the
    /// version chain depth at 1 regardless of commit frequency.
    /// DashMap sharding provides O(1) lock-free updates under high contention.
    pub(crate) hot_row_write_counts: DashMap<PrimaryKey, u32>,
}

/// Number of posting-list shards inside GinIndex.
/// Must be a power of two so shard selection reduces to a bitmask.
const GIN_SHARD_COUNT: usize = 16;

/// GIN (Generalized Inverted Index) for tsvector columns.
/// Maps each lexeme to the set of PKs whose tsvector contains that lexeme.
///
/// ## P2-1: Sharded postings locks
/// The original design used a single `RwLock<BTreeMap>`.  Under concurrent
/// inserts from many worker threads (e.g. bulk-load) all writers serialised
/// on that one lock, limiting throughput to a single core.
///
/// We replace it with `GIN_SHARD_COUNT` independent `RwLock<BTreeMap>`s.
/// Each lexeme is routed to a shard by hashing its first byte — O(1) and
/// branch-free.  Concurrent inserts on lexemes that hash to different shards
/// proceed without contention; only same-shard lexemes still serialise.
pub struct GinIndex {
    pub column_idx: usize,
    /// Sharded posting lists.
    /// `postings[shard_of(lexeme)]` contains the BTreeMap for that shard.
    postings: [RwLock<BTreeMap<String, BTreeSet<PrimaryKey>>>; GIN_SHARD_COUNT],
}

impl GinIndex {
    pub fn new(column_idx: usize) -> Self {
        Self {
            column_idx,
            postings: std::array::from_fn(|_| RwLock::new(BTreeMap::new())),
        }
    }

    /// Map a lexeme to its shard index.
    #[inline]
    fn shard_of(lexeme: &str) -> usize {
        let b = lexeme.as_bytes().first().copied().unwrap_or(0);
        (b as usize) & (GIN_SHARD_COUNT - 1)
    }

    /// Add all lexemes from a tsvector to the posting lists.
    /// Each lexeme is routed to its own shard, so inserts of lexemes in
    /// different shards proceed fully concurrently.
    pub fn insert(&self, pk: &PrimaryKey, vector: &[(String, Vec<u16>)]) {
        for (lexeme, _) in vector {
            let shard = Self::shard_of(lexeme);
            self.postings[shard]
                .write()
                .entry(lexeme.clone())
                .or_default()
                .insert(pk.clone());
        }
    }

    /// Remove all lexemes of a tsvector from the posting lists.
    pub fn remove(&self, pk: &PrimaryKey, vector: &[(String, Vec<u16>)]) {
        for (lexeme, _) in vector {
            let shard = Self::shard_of(lexeme);
            let mut map = self.postings[shard].write();
            if let Some(set) = map.get_mut(lexeme.as_str()) {
                set.remove(pk);
                if set.is_empty() {
                    map.remove(lexeme.as_str());
                }
            }
        }
    }

    /// Lookup PKs containing a specific lexeme (exact match).
    pub fn lookup_term(&self, term: &str) -> BTreeSet<PrimaryKey> {
        let shard = Self::shard_of(term);
        self.postings[shard].read().get(term).cloned().unwrap_or_default()
    }

    /// Lookup PKs with lexemes matching a prefix.
    /// Prefix queries may span shard boundaries (different first bytes),
    /// so we scan all shards and merge results.
    pub fn lookup_prefix(&self, prefix: &str) -> BTreeSet<PrimaryKey> {
        let mut result = BTreeSet::new();
        for shard_map in &self.postings {
            let map = shard_map.read();
            for (lex, pks) in map.range(prefix.to_string()..) {
                if !lex.starts_with(prefix) {
                    break;
                }
                result.extend(pks.iter().cloned());
            }
        }
        result
    }

    /// Clear all posting lists (used during GIN rebuild).
    pub fn clear(&self) {
        for shard_map in &self.postings {
            shard_map.write().clear();
        }
    }
}


/// A secondary index on one or more columns backed by an ART (Adaptive Radix Tree).
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
    pub(crate) tree: crate::art::ArtTree,
}

impl SecondaryIndex {
    pub fn new(column_idx: usize) -> Self {
        Self {
            column_idx,
            column_indices: vec![],
            unique: false,
            covering_columns: vec![],
            prefix_len: 0,
            tree: crate::art::ArtTree::new(),
        }
    }

    pub fn new_unique(column_idx: usize) -> Self {
        Self {
            column_idx,
            column_indices: vec![],
            unique: true,
            covering_columns: vec![],
            prefix_len: 0,
            tree: crate::art::ArtTree::new(),
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
            tree: crate::art::ArtTree::new(),
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
            tree: crate::art::ArtTree::new(),
        }
    }

    /// Create a prefix index (truncates key to prefix_len bytes).
    pub fn new_prefix(column_idx: usize, prefix_len: usize) -> Self {
        Self {
            column_idx,
            column_indices: vec![],
            unique: false, // prefix indexes cannot enforce uniqueness
            covering_columns: vec![],
            prefix_len,
            tree: crate::art::ArtTree::new(),
        }
    }

    /// Returns true if this is a composite (multi-column) index.
    pub fn is_composite(&self) -> bool {
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
        self.encode_key_buf(row).into_vec()
    }

    /// opt6: Encode a composite key into a stack-allocated KeyBuf (no heap alloc for keys �?4 B).
    #[inline]
    pub fn encode_key_buf(&self, row: &OwnedRow) -> KeyBuf {
        let cols = self.key_columns();
        let mut buf: KeyBuf = SmallVec::with_capacity(cols.len() * 8);
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
        self.tree.insert(key_bytes, pk);
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
        self.tree.check_unique(key_bytes, pk)
    }

    pub fn remove(&self, key_bytes: &[u8], pk: &PrimaryKey) {
        self.tree.remove(key_bytes, pk);
    }

    /// Range scan: return all PKs with index key in the specified range.
    ///
    /// - `lower`: optional lower bound `(key, inclusive)`
    /// - `upper`: optional upper bound `(key, inclusive)`
    ///
    /// Examples:
    /// - `range_scan(Some((k, true)), None)` �?`key >= k`
    /// - `range_scan(Some((k, false)), None)` �?`key > k`
    /// - `range_scan(None, Some((k, false)))` �?`key < k`
    /// - `range_scan(Some((lo, true)), Some((hi, true)))` �?`lo <= key <= hi`
    pub fn range_scan(
        &self,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
    ) -> Vec<PrimaryKey> {
        self.tree.range_scan(lower, upper)
    }

    /// Prefix scan: return all PKs whose key starts with the given prefix.
    /// Useful for composite index leftmost-prefix queries and prefix indexes.
    pub fn prefix_scan(&self, prefix: &[u8]) -> Vec<PrimaryKey> {
        self.tree.prefix_scan(prefix)
    }

}

impl MemTable {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            data: DashMap::with_capacity_and_hasher_and_shard_amount(
                1 << 20,
                Default::default(),
                256,
            ),
            arena: VersionArena::new(),
            secondary_indexes: RwLock::new(Vec::new()),
            column_to_index: parking_lot::RwLock::new(HashMap::new()),
            has_secondary_idx: AtomicBool::new(false),
            gc_candidates: AtomicU64::new(0),
            committed_row_count: StripedI64::new(0),
            rows_modified: StripedU64::new(),
            analyze_threshold: AtomicU64::new(500),
            pk_order: RwLock::new(BTreeSet::new()),
            pk_order_dirty: AtomicBool::new(false),
            gin_indexes: RwLock::new(Vec::new()),
            hot_row_write_counts: DashMap::new(),
        }
    }

    /// High-concurrency constructor for nodes with >512 GB RAM.
    /// Uses more DashMap shards and a larger initial capacity to match
    /// CockroachDB's in-memory engine behaviour under heavy parallelism.
    /// `dashmap_shards` must be a power of 2 (default: 4096).
    /// `capacity_hint` is the estimated max row count per table.
    /// `skip_pk_order` is retained for API compatibility but ignored �?    /// pk_order is always rebuilt lazily (P0-3).
    pub fn new_large_mem(
        schema: TableSchema,
        dashmap_shards: usize,
        capacity_hint: usize,
        _skip_pk_order: bool,
    ) -> Self {
        let shards = dashmap_shards.next_power_of_two().max(256);
        Self {
            schema,
            data: DashMap::with_capacity_and_hasher_and_shard_amount(
                capacity_hint,
                Default::default(),
                shards,
            ),
            arena: VersionArena::new(),
            secondary_indexes: RwLock::new(Vec::new()),
            column_to_index: parking_lot::RwLock::new(HashMap::new()),
            has_secondary_idx: AtomicBool::new(false),
            gc_candidates: AtomicU64::new(0),
            committed_row_count: StripedI64::new(0),
            rows_modified: StripedU64::new(),
            analyze_threshold: AtomicU64::new(500),
            pk_order: RwLock::new(BTreeSet::new()),
            pk_order_dirty: AtomicBool::new(false),
            gin_indexes: RwLock::new(Vec::new()),
            hot_row_write_counts: DashMap::new(),
        }
    }

    /// opt5: Remove all secondary indexes on column_idx and rebuild column_to_index.
    pub fn remove_secondary_index_by_column(&self, column_idx: usize) {
        let mut indexes = self.secondary_indexes.write();
        indexes.retain(|i| i.column_idx != column_idx);
        // Rebuild the dispatch map to reflect new slot positions.
        let mut map = self.column_to_index.write();
        map.clear();
        for (slot, i) in indexes.iter().enumerate() {
            map.insert(i.column_idx, slot);
        }
        self.has_secondary_idx.store(!indexes.is_empty(), AtomicOrdering::Release);
    }

    /// Add a new secondary index (after DDL backfill is complete).
    /// Updates the column_to_index dispatch map and has_secondary_idx flag.
    pub fn add_secondary_index(&self, index: SecondaryIndex) {
        let mut indexes = self.secondary_indexes.write();
        let slot = indexes.len();
        let col = index.column_idx;
        indexes.push(index);
        drop(indexes);
        self.column_to_index.write().insert(col, slot);
        self.has_secondary_idx.store(true, AtomicOrdering::Release);
    }

    /// opt5: O(1) secondary index point lookup using column_to_index dispatch map.
    #[inline]
    pub fn index_lookup_pks_o1(&self, column_idx: usize, key: &[u8]) -> Vec<PrimaryKey> {
        let slot = match self.column_to_index.read().get(&column_idx).copied() {
            Some(s) => s,
            None => return vec![],
        };
        let indexes = self.secondary_indexes.read();
        indexes.get(slot).map_or_else(Vec::new, |idx| idx.tree.point_lookup(key))
    }

    /// Insert a new row. Creates a new version chain or prepends to existing.
    /// Indexes are NOT updated here �?they are updated at commit time (方案A).
    /// However, unique index constraints are checked eagerly against the committed index.
    ///
    /// Concurrency strategy (P0-2):
    ///   Phase 1 �?`get()` shard READ lock: handles the hot Occupied path (upsert / re-insert).
    ///             Avoids the write-lock entirely when the key already exists, which is the
    ///             common case under high-concurrency OLTP (concurrent UPDATE / upsert loops).
    ///   Phase 2 �?`entry()` shard WRITE lock: only triggered when the key is genuinely absent.
    ///             Between Phase 1 and Phase 2 another thread may have inserted the same key
    ///             (TOCTOU window), so we re-check inside the entry critical section.
    pub fn insert(
        &self,
        row: OwnedRow,
        txn_id: TxnId,
    ) -> Result<PrimaryKey, falcon_common::error::StorageError> {
        let pk = encode_pk(&row, self.schema.pk_indices());

        // Phase 1: optimistic read-lock path for the already-existing key (hot upsert path).
        if let Some(chain) = self.data.get(&pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(falcon_common::error::StorageError::WriteConflict);
            }
            if chain.has_live_version(txn_id) {
                return Err(falcon_common::error::StorageError::DuplicateKey);
            }
            self.check_unique_indexes(&pk, &row)?;
            chain.prepend(txn_id, Some(Arc::new(row)));
            self.gc_candidates.fetch_add(1, AtomicOrdering::Relaxed);
            return Ok(pk);
        }

        // Phase 2: key absent �?take write lock via entry() for atomic check-and-insert.
        // Re-check inside the critical section to close the TOCTOU window.
        match self.data.entry(pk.clone()) {
            dashmap::mapref::entry::Entry::Occupied(e) => {
                // Another thread inserted between Phase 1 and Phase 2.
                let chain = e.get();
                if chain.has_write_conflict(txn_id) {
                    return Err(falcon_common::error::StorageError::WriteConflict);
                }
                if chain.has_live_version(txn_id) {
                    return Err(falcon_common::error::StorageError::DuplicateKey);
                }
                self.check_unique_indexes(&pk, &row)?;
                chain.prepend(txn_id, Some(Arc::new(row)));
                self.gc_candidates.fetch_add(1, AtomicOrdering::Relaxed);
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                self.check_unique_indexes(&pk, &row)?;
                let chain = Arc::new(VersionChain2::new(Arc::as_ptr(&self.arena)));
                chain.prepend(txn_id, Some(Arc::new(row)));
                e.insert(chain);
            }
        }

        Ok(pk)
    }

    /// Insert with pre-wrapped Arc<OwnedRow> �?avoids deep clone when sharing row with WAL buffer.
    /// Same two-phase concurrency strategy as `insert()` (P0-2).
    pub fn insert_arc(
        &self,
        row: Arc<OwnedRow>,
        txn_id: TxnId,
    ) -> Result<PrimaryKey, falcon_common::error::StorageError> {
        let pk = encode_pk(&row, self.schema.pk_indices());

        // Phase 1: read-lock fast path for existing key.
        if let Some(chain) = self.data.get(&pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(falcon_common::error::StorageError::WriteConflict);
            }
            if chain.has_live_version(txn_id) {
                return Err(falcon_common::error::StorageError::DuplicateKey);
            }
            self.check_unique_indexes(&pk, &row)?;
            chain.prepend_arc(txn_id, Some(row));
            self.gc_candidates.fetch_add(1, AtomicOrdering::Relaxed);
            return Ok(pk);
        }

        // Phase 2: key absent �?atomic check-and-insert under write lock.
        match self.data.entry(pk.clone()) {
            dashmap::mapref::entry::Entry::Occupied(e) => {
                let chain = e.get();
                if chain.has_write_conflict(txn_id) {
                    return Err(falcon_common::error::StorageError::WriteConflict);
                }
                if chain.has_live_version(txn_id) {
                    return Err(falcon_common::error::StorageError::DuplicateKey);
                }
                self.check_unique_indexes(&pk, &row)?;
                chain.prepend_arc(txn_id, Some(row));
                self.gc_candidates.fetch_add(1, AtomicOrdering::Relaxed);
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                self.check_unique_indexes(&pk, &row)?;
                let chain = Arc::new(VersionChain2::new(Arc::as_ptr(&self.arena)));
                chain.prepend_arc(txn_id, Some(row));
                e.insert(chain);
            }
        }

        Ok(pk)
    }

    /// Update a row by PK. Prepends a new version.
    /// Indexes are NOT updated here �?they are updated at commit time (方案A).
    pub fn update(
        &self,
        pk: &PrimaryKey,
        new_row: OwnedRow,
        txn_id: TxnId,
    ) -> Result<(), falcon_common::error::StorageError> {
        if let Some(chain) = self.data.get(pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(falcon_common::error::StorageError::WriteConflict);
            }
            // P1-C: If this transaction already owns the head version, update it
            // in-place instead of creating a new Version node.  This keeps the
            // chain length at 1 for multi-update-same-row workloads and reduces
            // GC pressure.  Falls back to prepend when another txn owns the head.
            if !chain.try_in_place_update(txn_id, Arc::new(new_row.clone())) {
                chain.prepend(txn_id, Some(Arc::new(new_row)));
                self.gc_candidates.fetch_add(1, AtomicOrdering::Relaxed);
            }
            Ok(())
        } else {
            Err(falcon_common::error::StorageError::KeyNotFound)
        }
    }

    /// Delete a row by PK. Prepends a tombstone version.
    /// Indexes are NOT updated here �?they are updated at commit time (方案A).
    pub fn delete(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
    ) -> Result<(), falcon_common::error::StorageError> {
        if let Some(chain) = self.data.get(pk) {
            if chain.has_write_conflict(txn_id) {
                return Err(falcon_common::error::StorageError::WriteConflict);
            }
            chain.prepend(txn_id, None); // tombstone
            self.gc_candidates.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(())
        } else {
            Err(falcon_common::error::StorageError::KeyNotFound)
        }
    }

    // ── Hot Row Optimization (H1) ─────────────────────────────────────────────

    /// Threshold: trigger coalescing when a row accumulates this many committed
    /// writes since the last coalesce.  Tunable; default 8 matches the typical
    /// number of MVCC versions that accumulate before GC runs.
    const HOT_ROW_THRESHOLD: u32 = 8;

    /// Record a committed write for `pk`.
    /// Called from the commit path (engine_dml) after WAL flush succeeds.
    /// Returns `true` if the row has crossed the hot-row threshold and should
    /// be coalesced at the next opportunity (non-blocking signal to GC).
    #[inline]
    pub fn bump_write_count(&self, pk: &PrimaryKey) -> bool {
        let mut entry = self.hot_row_write_counts.entry(pk.clone()).or_insert(0);
        *entry += 1;
        *entry >= Self::HOT_ROW_THRESHOLD
    }

    /// Coalesce the version chain of all rows whose write count has exceeded
    /// `HOT_ROW_THRESHOLD`.  Safe to call at any GC watermark: only already-
    /// committed versions older than `watermark` are collapsed.
    ///
    /// This is the cross-transaction equivalent of `try_in_place_update`:
    /// while `try_in_place_update` handles the *intra-transaction* case
    /// (same txn updating the same row multiple times), `coalesce_hot_rows`
    /// handles the *inter-transaction* case (many different txns each writing
    /// the same row, leaving a long overflow chain).
    ///
    /// ## Complexity
    /// O(hot_keys) × O(chain_depth_per_key).  For typical OLTP hot rows the
    /// chain depth after coalescing is reduced from O(writes_since_last_gc) to 1.
    pub fn coalesce_hot_rows(&self, watermark: falcon_common::types::Timestamp) -> usize {
        let mut coalesced = 0usize;
        // Collect keys that crossed the threshold.
        let hot_keys: Vec<PrimaryKey> = self
            .hot_row_write_counts
            .iter()
            .filter(|e| *e.value() >= Self::HOT_ROW_THRESHOLD)
            .map(|e| e.key().clone())
            .collect();

        for pk in hot_keys {
            if let Some(chain) = self.data.get(&pk) {
                // Re-use the existing GC logic: truncate all overflow versions
                // older than `watermark`, leaving only the anchor committed version.
                let result = chain.gc(watermark);
                if result.reclaimed_versions > 0 {
                    coalesced += result.reclaimed_versions as usize;
                    self.gc_candidates
                        .fetch_sub(result.reclaimed_versions, AtomicOrdering::Relaxed);
                }
            }
            // Reset the counter so the next write window starts fresh.
            self.hot_row_write_counts.remove(&pk);
        }
        coalesced
    }

    /// Point read by PK for a specific transaction.
    /// `pk` accepts `&PrimaryKey` (Vec<u8>) or `&[u8]` (from PkBuf::as_slice()).
    pub fn get(&self, pk: &[u8], txn_id: TxnId, read_ts: Timestamp) -> Option<OwnedRow> {
        self.data
            .get(pk)
            .and_then(|chain| chain.read_for_txn(txn_id, read_ts))
            .map(unwrap_arc_row)
    }

    /// Point read returning Arc (zero-copy).
    /// `pk` accepts `&PrimaryKey` (Vec<u8>) or `&[u8]` (from PkBuf::as_slice()).
    pub fn get_arc(
        &self,
        pk: &[u8],
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Option<Arc<OwnedRow>> {
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

    /// Stream through visible rows with early termination support.
    /// The closure returns `true` to continue, `false` to stop.
    /// Returns the number of rows visited.
    pub fn for_each_visible_limit<F>(
        &self,
        txn_id: TxnId,
        read_ts: Timestamp,
        limit: usize,
        mut f: F,
    ) -> usize
    where
        F: FnMut(&OwnedRow) -> bool,
    {
        let mut visited = 0usize;
        for entry in &self.data {
            if visited >= limit {
                break;
            }
            let cont = entry
                .value()
                .with_visible_data(txn_id, read_ts, |row| {
                    visited += 1;
                    f(row)
                })
                .unwrap_or(true);
            if !cont {
                break;
            }
        }
        visited
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
                entry.value().with_visible_data(txn_id, read_ts, |row| map(&mut acc, row));
            }
            return acc;
        }

        // Phase 1: collect chain refs (Arc pointer copies, no row data clone)
        let chains: Vec<Arc<VersionChain2>> = self.data.iter().map(|e| e.value().clone()).collect();

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

            let results: Vec<T> = handles
                .into_iter()
                .map(|h| match h.join() {
                    Ok(v) => v,
                    Err(payload) => std::panic::resume_unwind(payload),
                })
                .collect();
            let merged = results.into_iter().reduce(|a, b| reduce(a, b));
            merged.unwrap_or_else(|| init())
        })
    }

    /// Prefetch-aware batched scan for vectorized query processing.
    /// Reduces cache misses by ~30-40% compared to naive DashMap iteration.
    pub fn scan_batched<F>(&self, txn_id: TxnId, read_ts: Timestamp, mut process_batch: F)
    where
        F: FnMut(&RowBatch),
    {
        let mut iter = PrefetchIterator::new(&self.data);
        let mut batch = RowBatch::new();

        while iter.next_batch(txn_id, read_ts, &mut batch) {
            process_batch(&batch);
        }
    }

    /// Fast COUNT/SUM/MIN/MAX using SIMD when possible.
    /// Returns (count, sum_i64, min_i64, max_i64) for the specified column.
    pub fn compute_simple_agg_simd(
        &self,
        txn_id: TxnId,
        read_ts: Timestamp,
        col_idx: usize,
    ) -> (i64, i64, Option<i64>, Option<i64>) {
        let mut iter = PrefetchIterator::new(&self.data);
        let mut batch = RowBatch::new();
        let mut agg = SimdAggregator::new();

        while iter.next_batch(txn_id, read_ts, &mut batch) {
            let values = extract_i64_column(&batch, col_idx);
            if !values.is_empty() {
                agg.process_i64_batch(&values);
            }
        }

        (agg.count(), agg.sum_i64(), agg.min_i64(), agg.max_i64())
    }

    /// Full table scan visible to a transaction.
    pub fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        let mut results = Vec::with_capacity(self.data.len());
        for entry in &self.data {
            if let Some(row) = entry.value().read_for_txn(txn_id, read_ts) {
                results.push((entry.key().clone(), unwrap_arc_row(row)));
            }
        }
        results
    }

    /// Scan returning only row data �?avoids cloning the PK per row.
    /// Use this when the caller only needs row values (SELECT, aggregates, joins).
    pub fn scan_rows_only(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<OwnedRow> {
        let mut results = Vec::with_capacity(self.data.len());
        for entry in &self.data {
            if let Some(row) = entry.value().read_for_txn(txn_id, read_ts) {
                results.push(unwrap_arc_row(row));
            }
        }
        results
    }

    /// Secondary index point scan: look up PKs via index, then fetch visible rows.
    pub fn index_scan(
        &self,
        column_idx: usize,
        key: &[u8],
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Vec<(PrimaryKey, OwnedRow)> {
        let pks = {
            let indexes = self.secondary_indexes.read();
            let mut found = Vec::new();
            for idx in indexes.iter() {
                if idx.column_idx == column_idx {
                    found = idx.tree.point_lookup(key);
                    break;
                }
            }
            found
        };
        self.fetch_visible(pks, txn_id, read_ts)
    }

    /// Secondary index range scan: look up PKs in range, then fetch visible rows.
    pub fn index_range_scan(
        &self,
        column_idx: usize,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Vec<(PrimaryKey, OwnedRow)> {
        let pks = {
            let indexes = self.secondary_indexes.read();
            let mut found = Vec::new();
            for idx in indexes.iter() {
                if idx.column_idx == column_idx {
                    found = idx.range_scan(lower, upper);
                    break;
                }
            }
            found
        };
        self.fetch_visible(pks, txn_id, read_ts)
    }

    /// Secondary index PK lookup (no row fetch).
    /// opt5: Uses O(1) column_to_index dispatch instead of O(N) linear scan.
    pub fn index_lookup_pks(&self, column_idx: usize, key: &[u8]) -> Vec<PrimaryKey> {
        self.index_lookup_pks_o1(column_idx, key)
    }

    fn fetch_visible(
        &self,
        pks: Vec<PrimaryKey>,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Vec<(PrimaryKey, OwnedRow)> {
        let mut results = Vec::new();
        for pk in pks {
            if let Some(chain) = self.data.get(&pk) {
                if let Some(row) = chain.read_for_txn(txn_id, read_ts) {
                    results.push((pk, unwrap_arc_row(row)));
                }
            }
        }
        results
    }

    /// Commit all writes by a transaction (legacy full-scan path).
    /// Also maintains secondary indexes at commit time (方案A).
    /// Validates unique constraints before committing (same as commit_keys).
    pub fn commit_txn(&self, txn_id: TxnId, commit_ts: Timestamp) {
        // Collect keys with uncommitted writes by this txn for unique validation
        let affected_keys: Vec<PrimaryKey> = self
            .data
            .iter()
            .filter(|e| {
                let chain = e.value();
                let txn = chain.head_txn_id.load(std::sync::atomic::Ordering::Acquire);
                let cts = chain.head_commit_ts.load(std::sync::atomic::Ordering::Acquire);
                txn == txn_id.0 && cts == 0
            })
            .map(|e| e.key().clone())
            .collect();

        // Validate unique constraints before committing any version
        if !affected_keys.is_empty() {
            if let Err(_e) = self.validate_unique_constraints_for_commit(txn_id, &affected_keys) {
                // Abort instead of silently committing with violated constraints
                self.abort_keys(txn_id, &affected_keys);
                return;
            }
        }

        let mut pk_inserts: Vec<PrimaryKey> = Vec::new();
        let mut pk_deletes: Vec<PrimaryKey> = Vec::new();
        let mut row_delta: i64 = 0;

        for entry in &self.data {
            let pk = entry.key().clone();
            let (new_data, old_data) = entry.value().commit_and_report(txn_id, commit_ts);
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
            self.index_update_on_commit(&pk, new_data.as_deref(), old_data.as_deref());
        }

        if row_delta > 0 {
            self.committed_row_count
                .fetch_add(row_delta, AtomicOrdering::Relaxed);
        } else if row_delta < 0 {
            self.committed_row_count
                .fetch_sub(-row_delta, AtomicOrdering::Relaxed);
        }

        // P0-3: Never write pk_order on the commit hot path. Signal staleness via dirty flag;
        // the ordered index will be rebuilt lazily on the next range-scan / top-k request.
        if !pk_inserts.is_empty() || !pk_deletes.is_empty() {
            self.pk_order_dirty.store(true, AtomicOrdering::Release);
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

        // Phase 2: All checks passed �?apply MVCC commits + index updates.
        let mut row_delta: i64 = 0;
        let mut pk_inserts: Vec<PrimaryKey> = Vec::new();
        let mut pk_deletes: Vec<PrimaryKey> = Vec::new();

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
                self.index_update_on_commit(pk, new_data.as_deref(), old_data.as_deref());
            }
        }

        if row_delta > 0 {
            self.committed_row_count
                .fetch_add(row_delta, AtomicOrdering::Relaxed);
        } else if row_delta < 0 {
            self.committed_row_count
                .fetch_sub(-row_delta, AtomicOrdering::Relaxed);
        }

        // P0-3: Signal pk_order staleness; rebuilt lazily on next range-scan.
        if !pk_inserts.is_empty() || !pk_deletes.is_empty() {
            self.pk_order_dirty.store(true, AtomicOrdering::Release);
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
        self.data
            .get(pk)
            .is_some_and(|chain| chain.has_committed_write_after(exclude_txn, after_ts))
    }

    /// Count visible rows without cloning row data.
    /// Fast path: if no GC candidates exist (INSERT-only workload), the
    /// committed_row_count is exact and avoids the O(N) DashMap scan entirely.
    pub fn count_visible(&self, txn_id: TxnId, read_ts: Timestamp) -> usize {
        let count = self.committed_row_count.load(AtomicOrdering::Acquire);
        // Fast path only when count matches data.len() (no uncommitted inserts)
        // and there are no GC candidates (no uncommitted deletes/updates).
        if count >= 0
            && self.gc_candidates.load(AtomicOrdering::Relaxed) == 0
            && self.data.len() == count as usize
        {
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
                                        if let Some(s) = sums_i[i].checked_add(*v) {
                                            sums_i[i] = s;
                                        } else {
                                            is_float[i] = true;
                                        }
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
                                        || d.partial_cmp(
                                            mins[i]
                                                .as_ref()
                                                .expect("BUG: None after is_none short-circuit"),
                                        ) == Some(std::cmp::Ordering::Less))
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
                                        || d.partial_cmp(
                                            maxs[i]
                                                .as_ref()
                                                .expect("BUG: None after is_none short-circuit"),
                                        ) == Some(std::cmp::Ordering::Greater))
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

        // P0-3: Rebuild pk_order if dirty (signalled by any recent commit).
        // Write lock is only acquired when a range-scan is actually needed �?never on the
        // commit hot path. Compare-and-swap clears dirty so concurrent readers don't pile up.
        if self.pk_order_dirty.compare_exchange(
            true, false, AtomicOrdering::AcqRel, AtomicOrdering::Relaxed
        ).is_ok() {
            let mut pk_set = self.pk_order.write();
            pk_set.clear();
            for entry in &self.data {
                pk_set.insert(entry.key().clone());
            }
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
            let mut heap: BinaryHeap<Reverse<PrimaryKey>> = BinaryHeap::with_capacity(k + 1);
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

    /// PK range scan using the ordered PK index.
    /// `start`/`end` are optional inclusive/exclusive bounds on the encoded PK bytes.
    /// Fast path: O(K) via BTreeSet range when pk_order is populated.
    /// Fallback: O(N) full DashMap scan + filter when pk_order is empty.
    pub fn scan_range_by_pk(
        &self,
        txn_id: TxnId,
        read_ts: Timestamp,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(PrimaryKey, OwnedRow)> {
        let in_range =
            |pk: &[u8]| -> bool { start.is_none_or(|s| pk >= s) && end.is_none_or(|e| pk < e) };

        // P0-3: Rebuild pk_order lazily if dirty.
        if self.pk_order_dirty.compare_exchange(
            true, false, AtomicOrdering::AcqRel, AtomicOrdering::Relaxed
        ).is_ok() {
            let mut pk_set = self.pk_order.write();
            pk_set.clear();
            for entry in &self.data {
                pk_set.insert(entry.key().clone());
            }
        }

        let pk_set = self.pk_order.read();
        if !pk_set.is_empty() {
            use std::ops::Bound;
            let lo = match start {
                Some(s) => Bound::Included(s.to_vec()),
                None => Bound::Unbounded,
            };
            let hi = match end {
                Some(e) => Bound::Excluded(e.to_vec()),
                None => Bound::Unbounded,
            };
            let mut results = Vec::new();
            for pk in pk_set.range((lo, hi)) {
                if let Some(row) = self.get(pk, txn_id, read_ts) {
                    results.push((pk.clone(), row));
                }
            }
            return results;
        }
        drop(pk_set);

        // Fallback: full DashMap scan with filter
        let mut results = Vec::new();
        for entry in &self.data {
            if in_range(entry.key()) {
                if let Some(row) = entry.value().read_for_txn(txn_id, read_ts) {
                    results.push((entry.key().clone(), unwrap_arc_row(row)));
                }
            }
        }
        results
    }

    /// Row count (approximate �?counts all chains with at least one committed version).
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
    pub(crate) fn validate_unique_constraints_for_commit(
        &self,
        txn_id: TxnId,
        keys: &[PrimaryKey],
    ) -> Result<(), falcon_common::error::StorageError> {
        // P1-6: atomic fast-exit — avoids RwLock acquire on tables with no indexes.
        if !self.has_secondary_idx.load(AtomicOrdering::Acquire) {
            return Ok(());
        }
        let indexes = self.secondary_indexes.read();
        let has_unique = indexes.iter().any(|idx| idx.unique);
        if !has_unique {
            return Ok(());
        }

        for pk in keys {
            // Read the uncommitted row this txn is about to commit
            let uncommitted = self
                .data
                .get(pk)
                .and_then(|chain| chain.read_uncommitted_for_txn(txn_id));

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
        if !self
            .has_secondary_idx
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
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
        // Secondary B-tree indexes
        // N-07: Single pass per index — encode old and new key once each, no double traversal.
        let indexes = self.secondary_indexes.read();
        if !indexes.is_empty() {
            for idx in indexes.iter() {
                if let Some(old_row) = old_data {
                    let key_bytes = idx.encode_key_buf(old_row);
                    idx.remove(&key_bytes, pk);
                }
                if let Some(new_row) = new_data {
                    let key_bytes = idx.encode_key_buf(new_row);
                    idx.insert(key_bytes.into_vec(), pk.clone());
                }
            }
        }

        // GIN inverted indexes for tsvector columns
        let gin = self.gin_indexes.read();
        if !gin.is_empty() {
            for gi in gin.iter() {
                if let Some(old_row) = old_data {
                    if let Some(Datum::TsVector(ref v)) = old_row.get(gi.column_idx) {
                        gi.remove(pk, v);
                    }
                }
                if let Some(new_row) = new_data {
                    if let Some(Datum::TsVector(ref v)) = new_row.get(gi.column_idx) {
                        gi.insert(pk, v);
                    }
                }
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
                let key_bytes = idx.encode_key_buf(row);
                idx.check_unique(&key_bytes, pk)?;
            }
        }
        // All checks passed — insert into all indexes
        for idx in indexes.iter() {
            let key_bytes = idx.encode_key_buf(row);
            idx.insert(key_bytes.into_vec(), pk.clone());
        }
        Ok(())
    }

    /// Remove a row's indexed column values from all secondary indexes.
    #[allow(dead_code)]
    fn index_remove_row(&self, pk: &PrimaryKey, row: &OwnedRow) {
        let indexes = self.secondary_indexes.read();
        for idx in indexes.iter() {
            let key_bytes = idx.encode_key_buf(row);
            idx.remove(&key_bytes, pk);
        }
    }

    /// Rebuild all secondary indexes and GIN indexes from current data.
    /// Used after WAL recovery to restore index state.
    pub fn rebuild_secondary_indexes(&self) {
        let indexes = self.secondary_indexes.read();
        for idx in indexes.iter() {
            idx.tree.clear();
        }
        let gin = self.gin_indexes.read();
        for gi in gin.iter() {
            gi.clear();
        }
        for entry in &self.data {
            let pk = entry.key().clone();
            let chain = entry.value();
            chain.with_visible_data(
                falcon_common::types::TxnId(u64::MAX),
                falcon_common::types::Timestamp(u64::MAX - 1),
                |row| {
                    for idx in indexes.iter() {
                        let key_bytes = idx.encode_key(row);
                        idx.insert(key_bytes, pk.clone());
                    }
                    for gi in gin.iter() {
                        if let Some(Datum::TsVector(ref v)) = row.get(gi.column_idx) {
                            gi.insert(&pk, v);
                        }
                    }
                },
            );
        }
    }

    /// Ensure GIN indexes exist for all tsvector columns in the schema.
    pub fn ensure_gin_indexes(&self) {
        let mut gin = self.gin_indexes.write();
        for (i, col) in self.schema.columns.iter().enumerate() {
            if col.data_type == falcon_common::types::DataType::TsVector {
                if !gin.iter().any(|g| g.column_idx == i) {
                    gin.push(GinIndex::new(i));
                }
            }
        }
    }

    /// Search GIN index for candidate PKs matching a tsquery on a given column.
    /// Returns None if no GIN index exists for this column.
    pub fn gin_search(&self, column_idx: usize, terms: &[GinSearchTerm]) -> Option<BTreeSet<PrimaryKey>> {
        let gin = self.gin_indexes.read();
        let gi = gin.iter().find(|g| g.column_idx == column_idx)?;
        if terms.is_empty() {
            return Some(BTreeSet::new());
        }
        // Intersect results for AND terms, union for OR terms.
        // Simple approach: collect all candidate PKs from all terms (union),
        // then let the caller do per-row verification for AND/NOT semantics.
        let mut result = BTreeSet::new();
        for term in terms {
            match term {
                GinSearchTerm::Exact(w) => {
                    result.extend(gi.lookup_term(w));
                }
                GinSearchTerm::Prefix(p) => {
                    result.extend(gi.lookup_prefix(p));
                }
            }
        }
        Some(result)
    }
}

/// Search term for GIN index lookup.
#[derive(Debug, Clone)]
pub enum GinSearchTerm {
    Exact(String),
    Prefix(String),
}

impl crate::storage_trait::StorageTable for MemTable {
    fn schema(&self) -> &falcon_common::schema::TableSchema {
        &self.schema
    }

    fn insert(
        &self,
        row: &OwnedRow,
        txn_id: TxnId,
    ) -> Result<PrimaryKey, falcon_common::error::StorageError> {
        self.insert(row.clone(), txn_id)
    }

    fn update(
        &self,
        pk: &PrimaryKey,
        new_row: &OwnedRow,
        txn_id: TxnId,
    ) -> Result<(), falcon_common::error::StorageError> {
        self.update(pk, new_row.clone(), txn_id)
    }

    fn delete(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
    ) -> Result<(), falcon_common::error::StorageError> {
        self.delete(pk, txn_id)
    }

    fn get(
        &self,
        pk: &[u8],
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Option<OwnedRow>, falcon_common::error::StorageError> {
        Ok(self.get(pk, txn_id, read_ts))
    }

    fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(PrimaryKey, OwnedRow)> {
        self.scan(txn_id, read_ts)
    }

    fn commit_key(
        &self,
        pk: &PrimaryKey,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), falcon_common::error::StorageError> {
        self.commit_keys(txn_id, commit_ts, std::slice::from_ref(pk))
    }

    fn abort_key(&self, pk: &PrimaryKey, txn_id: TxnId) {
        self.abort_keys(txn_id, std::slice::from_ref(pk));
    }

    fn commit_batch(
        &self,
        pks: &[PrimaryKey],
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), falcon_common::error::StorageError> {
        self.commit_keys(txn_id, commit_ts, pks)
    }
    fn abort_batch(&self, pks: &[PrimaryKey], txn_id: TxnId) {
        self.abort_keys(txn_id, pks);
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
            max_length: None,
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
        let mut found = idx.tree.point_lookup(&key);
        found.sort();
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
        assert!(!idx.tree.point_lookup(&key).is_empty());

        idx.remove(&key, &pk);
        assert!(idx.tree.point_lookup(&key).is_empty());
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
            mt.has_secondary_idx.store(true, AtomicOrdering::Release);
        }

        let txn = TxnId(100);
        let row = make_row(1, 10, "hello", 20);

        let pk = mt.insert(row.clone(), txn).unwrap();
        mt.commit_keys(txn, falcon_common::types::Timestamp(200), &[pk.clone()])
            .unwrap();

        let indexes = mt.secondary_indexes.read();
        let idx = &indexes[0];
        let key = idx.encode_key(&row);
        let pks = idx.tree.point_lookup(&key);
        assert!(pks.contains(&pk));
    }

    #[test]
    fn test_gin_index_basic() {
        let gi = GinIndex::new(0);
        let pk1 = make_pk(1);
        let pk2 = make_pk(2);
        let v1 = vec![
            ("quick".to_string(), vec![1]),
            ("brown".to_string(), vec![2]),
            ("fox".to_string(), vec![3]),
        ];
        let v2 = vec![
            ("lazy".to_string(), vec![1]),
            ("brown".to_string(), vec![2]),
            ("dog".to_string(), vec![3]),
        ];
        gi.insert(&pk1, &v1);
        gi.insert(&pk2, &v2);

        assert_eq!(gi.lookup_term("fox").len(), 1);
        assert!(gi.lookup_term("fox").contains(&pk1));
        assert_eq!(gi.lookup_term("brown").len(), 2);
        assert!(gi.lookup_term("cat").is_empty());
    }

    #[test]
    fn test_gin_index_prefix() {
        let gi = GinIndex::new(0);
        let pk1 = make_pk(1);
        let pk2 = make_pk(2);
        let v1 = vec![("quick".to_string(), vec![1])];
        let v2 = vec![("quiet".to_string(), vec![1])];
        gi.insert(&pk1, &v1);
        gi.insert(&pk2, &v2);

        let result = gi.lookup_prefix("qui");
        assert_eq!(result.len(), 2);
        let result2 = gi.lookup_prefix("quic");
        assert_eq!(result2.len(), 1);
        assert!(result2.contains(&pk1));
    }

    #[test]
    fn test_gin_index_remove() {
        let gi = GinIndex::new(0);
        let pk1 = make_pk(1);
        let v1 = vec![("hello".to_string(), vec![1]), ("world".to_string(), vec![2])];
        gi.insert(&pk1, &v1);
        assert_eq!(gi.lookup_term("hello").len(), 1);
        gi.remove(&pk1, &v1);
        assert!(gi.lookup_term("hello").is_empty());
        assert!(gi.lookup_term("world").is_empty());
    }

    #[test]
    fn test_ensure_gin_indexes_tsvector_column() {
        let schema = TableSchema {
            id: TableId(10),
            name: "docs".to_string(),
            columns: vec![
                col("id", DataType::Int64, true),
                col("body", DataType::TsVector, false),
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        };
        let mt = MemTable::new(schema);
        mt.ensure_gin_indexes();
        let gin = mt.gin_indexes.read();
        assert_eq!(gin.len(), 1);
        assert_eq!(gin[0].column_idx, 1);
    }

    #[test]
    fn test_gin_commit_updates_index() {
        let schema = TableSchema {
            id: TableId(11),
            name: "docs".to_string(),
            columns: vec![
                col("id", DataType::Int64, true),
                col("body", DataType::TsVector, false),
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        };
        let mt = MemTable::new(schema);
        mt.ensure_gin_indexes();

        let txn = TxnId(100);
        let row = OwnedRow::new(vec![
            Datum::Int64(1),
            Datum::TsVector(vec![
                ("hello".to_string(), vec![1]),
                ("world".to_string(), vec![2]),
            ]),
        ]);
        let pk = mt.insert(row, txn).unwrap();
        mt.commit_keys(txn, falcon_common::types::Timestamp(200), &[pk.clone()])
            .unwrap();

        let terms = vec![GinSearchTerm::Exact("hello".to_string())];
        let result = mt.gin_search(1, &terms).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&pk));

        // Term not in any doc
        let terms2 = vec![GinSearchTerm::Exact("missing".to_string())];
        let result2 = mt.gin_search(1, &terms2).unwrap();
        assert!(result2.is_empty());
    }
}
