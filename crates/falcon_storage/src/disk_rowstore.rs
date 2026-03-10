//! On-disk B-tree row store — page-based storage engine.
//!
//! Layout:
//!   - Page 1: metadata (root_page_id, first_leaf_id)
//!   - Internal pages: sorted keys + child page pointers
//!   - Leaf pages: sorted (pk, row) slots + next_leaf pointer for chained scan
//!
//! All pages are 8 KiB, cached in an LRU buffer pool.
//! Leaf pages form a singly-linked chain for sequential scan.
//! Insert into a full leaf triggers a page split with median key pushed to parent.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{TableId, Timestamp, TxnId};

// ---------------------------------------------------------------------------
// MVCC version layer
// ---------------------------------------------------------------------------

/// A single version of a row in the MVCC chain.
#[derive(Clone)]
struct DiskVersion {
    txn_id: TxnId,
    /// None = tombstone (DELETE)
    row: Option<OwnedRow>,
    /// Set to Some(ts) after commit; None while pending.
    commit_ts: Option<Timestamp>,
}

/// Per-key version chain (newest first).
type VersionChain = VecDeque<DiskVersion>;

pub const PAGE_SIZE: usize = 8192;
pub const DEFAULT_BUFFER_POOL_PAGES: usize = 1024;
pub type PageId = u64;

const META_PAGE_ID: PageId = 1;
/// 0 = metadata, 1 = leaf, 2 = internal
const PAGE_TYPE_META: u8 = 0;
const PAGE_TYPE_LEAF: u8 = 1;
const PAGE_TYPE_INTERNAL: u8 = 2;

fn serialize_row(row: &OwnedRow) -> Result<Vec<u8>, StorageError> {
    bincode::serialize(row).map_err(|e| StorageError::Serialization(format!("serialize: {e}")))
}

fn deserialize_row(bytes: &[u8]) -> Result<OwnedRow, StorageError> {
    bincode::deserialize(bytes)
        .map_err(|e| StorageError::Serialization(format!("deserialize: {e}")))
}

fn read_u32(data: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(data[off..off + 4].try_into().unwrap_or([0; 4]))
}
fn read_u64(data: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(data[off..off + 8].try_into().unwrap_or([0; 8]))
}
fn write_u32(data: &mut [u8], off: usize, v: u32) {
    data[off..off + 4].copy_from_slice(&v.to_le_bytes());
}
fn write_u64(data: &mut [u8], off: usize, v: u64) {
    data[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------
//
// Leaf page layout:
//   [0]    u8   page_type = 1
//   [1..5] u32  slot_count
//   [5..13] u64 next_leaf (0 = none)
//   [13..]  slot directory: per slot 2×u32 (offset, length)
//   followed by slot payloads: [pk_len:u32][pk][row_data]
//
// Internal page layout:
//   [0]    u8   page_type = 2
//   [1..5] u32  key_count (N keys, N+1 children)
//   [5..]  children[0..=N] as u64, then keys[0..N] as (u32 len, bytes)

const LEAF_HEADER: usize = 13; // type(1) + count(4) + next_leaf(8)

#[derive(Clone)]
pub struct Page {
    pub id: PageId,
    pub data: Vec<u8>,
    pub dirty: bool,
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: vec![0u8; PAGE_SIZE],
            dirty: false,
        }
    }

    fn page_type(&self) -> u8 {
        self.data[0]
    }
    fn set_page_type(&mut self, t: u8) {
        self.data[0] = t;
        self.dirty = true;
    }

    // -- leaf helpers --
    fn leaf_slot_count(&self) -> usize {
        read_u32(&self.data, 1) as usize
    }
    fn set_leaf_slot_count(&mut self, n: usize) {
        write_u32(&mut self.data, 1, n as u32);
    }
    fn next_leaf(&self) -> PageId {
        read_u64(&self.data, 5)
    }
    fn set_next_leaf(&mut self, id: PageId) {
        write_u64(&mut self.data, 5, id);
    }

    fn leaf_read_rows(&self) -> Vec<(Vec<u8>, OwnedRow)> {
        let count = self.leaf_slot_count();
        let mut rows = Vec::with_capacity(count);
        for i in 0..count {
            let dir_off = LEAF_HEADER + i * 8;
            if dir_off + 8 > self.data.len() {
                break;
            }
            let offset = read_u32(&self.data, dir_off) as usize;
            let length = read_u32(&self.data, dir_off + 4) as usize;
            if offset + length > self.data.len() || length < 4 {
                continue;
            }
            let slot = &self.data[offset..offset + length];
            let pk_len = read_u32(slot, 0) as usize;
            if 4 + pk_len > slot.len() {
                continue;
            }
            let pk = slot[4..4 + pk_len].to_vec();
            if let Ok(row) = deserialize_row(&slot[4 + pk_len..]) {
                rows.push((pk, row));
            }
        }
        rows
    }

    fn leaf_try_append(&mut self, pk: &[u8], row: &OwnedRow) -> bool {
        let row_bytes = match serialize_row(row) {
            Ok(b) => b,
            Err(_) => return false,
        };
        let slot_data_len = 4 + pk.len() + row_bytes.len();
        let count = self.leaf_slot_count();
        let new_count = count + 1;
        let new_dir_end = LEAF_HEADER + new_count * 8;

        // find data_end
        let mut data_end = LEAF_HEADER + count * 8; // at least past current dir
        for i in 0..count {
            let dir_off = LEAF_HEADER + i * 8;
            let off = read_u32(&self.data, dir_off) as usize;
            let len = read_u32(&self.data, dir_off + 4) as usize;
            let end = off + len;
            if end > data_end {
                data_end = end;
            }
        }

        let old_dir_end = LEAF_HEADER + count * 8;
        let growth = new_dir_end - old_dir_end; // always 8
        let needed = data_end + growth + slot_data_len;
        if needed > PAGE_SIZE {
            return false;
        }

        // shift payload data to make room for new dir entry
        if growth > 0 {
            if data_end > old_dir_end {
                self.data
                    .copy_within(old_dir_end..data_end, old_dir_end + growth);
                for i in 0..count {
                    let d = LEAF_HEADER + i * 8;
                    let v = read_u32(&self.data, d) + growth as u32;
                    write_u32(&mut self.data, d, v);
                }
            }
            data_end = new_dir_end.max(data_end + growth);
        }
        // write new dir entry
        let dir_off = LEAF_HEADER + count * 8;
        write_u32(&mut self.data, dir_off, data_end as u32);
        write_u32(&mut self.data, dir_off + 4, slot_data_len as u32);
        // write payload
        write_u32(&mut self.data, data_end, pk.len() as u32);
        self.data[data_end + 4..data_end + 4 + pk.len()].copy_from_slice(pk);
        self.data[data_end + 4 + pk.len()..data_end + 4 + pk.len() + row_bytes.len()]
            .copy_from_slice(&row_bytes);

        self.set_leaf_slot_count(new_count);
        self.dirty = true;
        true
    }

    // -- internal helpers --
    fn internal_key_count(&self) -> usize {
        read_u32(&self.data, 1) as usize
    }
    fn set_internal_key_count(&mut self, n: usize) {
        write_u32(&mut self.data, 1, n as u32);
    }

    // children start at offset 5, each 8 bytes
    fn internal_child(&self, idx: usize) -> PageId {
        read_u64(&self.data, 5 + idx * 8)
    }
    fn set_internal_child(&mut self, idx: usize, pid: PageId) {
        write_u64(&mut self.data, 5 + idx * 8, pid);
        self.dirty = true;
    }

    // keys start after children: offset = 5 + (key_count+1)*8, variable length
    fn internal_keys(&self) -> Vec<Vec<u8>> {
        let n = self.internal_key_count();
        let mut off = 5 + (n + 1) * 8;
        let mut keys = Vec::with_capacity(n);
        for _ in 0..n {
            if off + 4 > self.data.len() {
                break;
            }
            let klen = read_u32(&self.data, off) as usize;
            off += 4;
            if off + klen > self.data.len() {
                break;
            }
            keys.push(self.data[off..off + klen].to_vec());
            off += klen;
        }
        keys
    }

    fn internal_write_keys_and_children(&mut self, keys: &[Vec<u8>], children: &[PageId]) {
        let n = keys.len();
        self.set_internal_key_count(n);
        for (i, &c) in children.iter().enumerate() {
            self.set_internal_child(i, c);
        }
        let mut off = 5 + children.len() * 8;
        for k in keys {
            write_u32(&mut self.data, off, k.len() as u32);
            off += 4;
            self.data[off..off + k.len()].copy_from_slice(k);
            off += k.len();
        }
        self.dirty = true;
    }

    fn internal_can_fit_key(&self, key: &[u8]) -> bool {
        let n = self.internal_key_count();
        let mut off = 5 + (n + 1) * 8;
        for _ in 0..n {
            if off + 4 > self.data.len() {
                return false;
            }
            let klen = read_u32(&self.data, off) as usize;
            off += 4 + klen;
        }
        // after inserting: +8 (child) + 4 (key len) + key.len()
        off + 8 + 4 + key.len() <= PAGE_SIZE
    }
}

// ---------------------------------------------------------------------------
// Buffer Pool (unchanged from original)
// ---------------------------------------------------------------------------

pub struct BufferPool {
    capacity: usize,
    pages: RwLock<HashMap<PageId, Page>>,
    lru: RwLock<VecDeque<PageId>>,
    file: RwLock<File>,
    next_page_id: AtomicU64,
}

impl BufferPool {
    pub fn open(path: &Path, capacity: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let file_len = file.metadata()?.len();
        let next_page = if file_len == 0 {
            1
        } else {
            file_len / PAGE_SIZE as u64 + 1
        };
        Ok(Self {
            capacity,
            pages: RwLock::new(HashMap::new()),
            lru: RwLock::new(VecDeque::new()),
            file: RwLock::new(file),
            next_page_id: AtomicU64::new(next_page),
        })
    }

    pub fn allocate_page(&self) -> PageId {
        let id = self.next_page_id.fetch_add(1, Ordering::Relaxed);
        let page = Page::new(id);
        self.put_page(page);
        id
    }

    pub fn fetch_page(&self, page_id: PageId) -> Option<Page> {
        {
            let pages = self.pages.read();
            if let Some(page) = pages.get(&page_id) {
                self.touch_lru(page_id);
                return Some(page.clone());
            }
        }
        self.load_page(page_id)
    }

    pub fn put_page(&self, page: Page) {
        let page_id = page.id;
        {
            self.pages.write().insert(page_id, page);
        }
        self.touch_lru(page_id);
        self.maybe_evict();
    }

    pub fn flush_all(&self) -> io::Result<()> {
        let mut pages = self.pages.write();
        let mut file = self.file.write();
        for page in pages.values_mut() {
            if page.dirty {
                let offset = (page.id - 1) * PAGE_SIZE as u64;
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(&page.data)?;
                page.dirty = false;
            }
        }
        file.flush()
    }

    fn load_page(&self, page_id: PageId) -> Option<Page> {
        let mut page = Page::new(page_id);
        let offset = (page_id - 1) * PAGE_SIZE as u64;
        {
            let mut file = self.file.write();
            if file.seek(SeekFrom::Start(offset)).is_err() {
                return None;
            }
            if file.read_exact(&mut page.data).is_err() {
                return None;
            }
        }
        self.put_page(page.clone());
        Some(page)
    }

    fn touch_lru(&self, page_id: PageId) {
        let mut lru = self.lru.write();
        lru.retain(|&id| id != page_id);
        lru.push_back(page_id);
    }

    fn maybe_evict(&self) {
        let mut pages = self.pages.write();
        let mut lru = self.lru.write();
        let mut file = self.file.write();
        while pages.len() > self.capacity && !lru.is_empty() {
            if let Some(victim_id) = lru.pop_front() {
                if let Some(page) = pages.get(&victim_id) {
                    if page.dirty {
                        let offset = (page.id - 1) * PAGE_SIZE as u64;
                        let _ = file
                            .seek(SeekFrom::Start(offset))
                            .and_then(|_| file.write_all(&page.data));
                    }
                }
                pages.remove(&victim_id);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Metadata page helpers
// ---------------------------------------------------------------------------

fn read_meta(pool: &BufferPool) -> (PageId, PageId) {
    if let Some(p) = pool.fetch_page(META_PAGE_ID) {
        if p.page_type() == PAGE_TYPE_META {
            let root = read_u64(&p.data, 1);
            let first_leaf = read_u64(&p.data, 9);
            return (root, first_leaf);
        }
    }
    (0, 0)
}

fn write_meta(pool: &BufferPool, root: PageId, first_leaf: PageId) {
    let mut p = pool
        .fetch_page(META_PAGE_ID)
        .unwrap_or_else(|| Page::new(META_PAGE_ID));
    p.data[0] = PAGE_TYPE_META;
    write_u64(&mut p.data, 1, root);
    write_u64(&mut p.data, 9, first_leaf);
    p.dirty = true;
    pool.put_page(p);
}

// ---------------------------------------------------------------------------
// DiskRowstoreTable
// ---------------------------------------------------------------------------

pub struct DiskRowstoreTable {
    pub schema: TableSchema,
    pool: Arc<BufferPool>,
    // In-memory PK → (page_id, slot_idx) cache. Rebuilt from leaf chain on open.
    pk_index: RwLock<BTreeMap<Vec<u8>, (PageId, usize)>>,
    root_page: RwLock<PageId>,
    first_leaf: RwLock<PageId>,
    rows_written: AtomicU64,
    rows_deleted: AtomicU64,
    /// MVCC overlay: uncommitted + recent committed versions, keyed by PK.
    /// Committed versions are flushed to disk pages; pending versions live here only.
    mvcc: RwLock<BTreeMap<Vec<u8>, VersionChain>>,
}

impl DiskRowstoreTable {
    pub fn new(schema: TableSchema, data_dir: &Path) -> Result<Self, StorageError> {
        let table_file = data_dir.join(format!("table_{}.dat", schema.id.0));
        let pool = Arc::new(
            BufferPool::open(&table_file, DEFAULT_BUFFER_POOL_PAGES).map_err(StorageError::Io)?,
        );

        let (root, first_leaf) = read_meta(&pool);

        let tbl = Self {
            schema,
            pool,
            pk_index: RwLock::new(BTreeMap::new()),
            root_page: RwLock::new(root),
            first_leaf: RwLock::new(first_leaf),
            rows_written: AtomicU64::new(0),
            rows_deleted: AtomicU64::new(0),
            mvcc: RwLock::new(BTreeMap::new()),
        };

        if root != 0 {
            tbl.rebuild_index();
        }
        Ok(tbl)
    }

    pub fn new_in_memory(schema: TableSchema) -> Result<Self, StorageError> {
        use std::sync::atomic::AtomicU64 as AU64;
        static COUNTER: AU64 = AU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_dir = std::env::temp_dir().join(format!("falcon_disk_rs_{id}"));
        std::fs::create_dir_all(&tmp_dir).map_err(StorageError::Io)?;
        Self::new(schema, &tmp_dir)
    }

    // ── MVCC helpers ─────────────────────────────────────────────────────────

    /// Write a pending (uncommitted) version for a key.
    fn mvcc_write(&self, pk: Vec<u8>, txn_id: TxnId, row: Option<OwnedRow>) {
        let mut mvcc = self.mvcc.write();
        let chain = mvcc.entry(pk).or_default();
        chain.push_front(DiskVersion { txn_id, row, commit_ts: None });
    }

    /// Commit all versions belonging to `txn_id` with `commit_ts`.
    /// Committed INSERTs/UPDATEs are flushed to the B-tree; DELETEs remove from index.
    pub fn commit_mvcc(&self, txn_id: TxnId, commit_ts: Timestamp) {
        let mut mvcc = self.mvcc.write();
        let mut to_flush: Vec<(Vec<u8>, Option<OwnedRow>)> = Vec::new();
        for (pk, chain) in mvcc.iter_mut() {
            for ver in chain.iter_mut() {
                if ver.txn_id == txn_id && ver.commit_ts.is_none() {
                    ver.commit_ts = Some(commit_ts);
                    to_flush.push((pk.clone(), ver.row.clone()));
                }
            }
        }
        drop(mvcc);
        for (pk, row_opt) in to_flush {
            match row_opt {
                Some(row) => {
                    // Upsert into B-tree (delete old slot if exists, then insert)
                    let _ = self.btree_delete(&pk);
                    let _ = self.btree_insert(pk, row);
                }
                None => {
                    // Tombstone — remove from B-tree
                    let _ = self.btree_delete(&pk);
                }
            }
        }
    }

    /// Abort all pending versions for `txn_id` (drop without applying).
    pub fn abort_mvcc(&self, txn_id: TxnId) {
        let mut mvcc = self.mvcc.write();
        for chain in mvcc.values_mut() {
            chain.retain(|v| !(v.txn_id == txn_id && v.commit_ts.is_none()));
        }
        mvcc.retain(|_, chain| !chain.is_empty());
    }

    /// Read the visible version of `pk` at `read_ts`.
    /// Returns the latest committed version with commit_ts <= read_ts,
    /// or falls back to the B-tree page data if no MVCC entry exists.
    pub fn mvcc_get(&self, pk: &[u8], txn_id: TxnId, read_ts: Timestamp) -> Option<OwnedRow> {
        let mvcc = self.mvcc.read();
        if let Some(chain) = mvcc.get(pk) {
            // Own uncommitted write is visible to itself
            for ver in chain.iter() {
                if ver.txn_id == txn_id && ver.commit_ts.is_none() {
                    return ver.row.clone();
                }
            }
            // Latest committed version at or before read_ts
            for ver in chain.iter() {
                if let Some(cts) = ver.commit_ts {
                    if cts.0 <= read_ts.0 {
                        return ver.row.clone();
                    }
                }
            }
        }
        drop(mvcc);
        // Fallback: B-tree has the committed baseline
        self.btree_get(pk)
    }

    /// GC: remove all committed versions older than `watermark` (keeping 1 per key).
    pub fn gc_mvcc(&self, watermark: Timestamp) {
        let mut mvcc = self.mvcc.write();
        for chain in mvcc.values_mut() {
            let mut keep_one = false;
            chain.retain(|v| {
                if v.commit_ts.is_none() {
                    return true; // keep pending
                }
                let ts = v.commit_ts.unwrap();
                if ts.0 <= watermark.0 {
                    if !keep_one {
                        keep_one = true;
                        return true; // keep the newest below watermark
                    }
                    return false;
                }
                true
            });
        }
        mvcc.retain(|_, chain| !chain.is_empty());
    }

    // ── B-tree helpers (raw, no MVCC) ────────────────────────────────────────

    fn btree_insert(&self, pk: Vec<u8>, row: OwnedRow) -> Result<(), StorageError> {
        let (path, leaf_id) = self.find_leaf_path(&pk);
        let mut leaf = self
            .pool
            .fetch_page(leaf_id)
            .ok_or_else(|| StorageError::Serialization("leaf page missing".into()))?;
        if leaf.leaf_try_append(&pk, &row) {
            let slot_idx = leaf.leaf_slot_count() - 1;
            self.pool.put_page(leaf);
            self.pk_index.write().insert(pk, (leaf_id, slot_idx));
            self.rows_written.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.split_and_insert(path, leaf_id, &pk, &row)?;
        self.rows_written.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn btree_delete(&self, pk: &[u8]) -> Result<(), StorageError> {
        let mut idx = self.pk_index.write();
        if idx.remove(pk).is_some() {
            self.rows_deleted.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            Err(StorageError::KeyNotFound)
        }
    }

    pub fn table_id(&self) -> TableId {
        self.schema.id
    }

    /// Rebuild in-memory pk_index by walking the leaf chain.
    fn rebuild_index(&self) {
        let mut idx = self.pk_index.write();
        idx.clear();
        let mut leaf_id = *self.first_leaf.read();
        let mut count = 0u64;
        while leaf_id != 0 {
            if let Some(page) = self.pool.fetch_page(leaf_id) {
                for (slot, (pk, _)) in page.leaf_read_rows().into_iter().enumerate() {
                    idx.insert(pk, (leaf_id, slot));
                    count += 1;
                }
                leaf_id = page.next_leaf();
            } else {
                break;
            }
        }
        self.rows_written.store(count, Ordering::Relaxed);
    }

    fn ensure_root(&self) -> PageId {
        let r = *self.root_page.read();
        if r != 0 {
            return r;
        }
        let _ = r;

        // Allocate meta page if needed
        let _meta_id = self.pool.allocate_page(); // page 1 = meta
        let leaf_id = self.pool.allocate_page(); // page 2 = first leaf

        let mut leaf = self.pool.fetch_page(leaf_id).unwrap();
        leaf.set_page_type(PAGE_TYPE_LEAF);
        leaf.set_next_leaf(0);
        leaf.set_leaf_slot_count(0);
        self.pool.put_page(leaf);

        let root_id = self.pool.allocate_page(); // page 3 = root (internal)
        let mut root = self.pool.fetch_page(root_id).unwrap();
        root.set_page_type(PAGE_TYPE_INTERNAL);
        root.internal_write_keys_and_children(&[], &[leaf_id]);
        self.pool.put_page(root);

        write_meta(&self.pool, root_id, leaf_id);

        *self.root_page.write() = root_id;
        *self.first_leaf.write() = leaf_id;
        root_id
    }

    /// Find the leaf page that should contain `pk` by traversing internal pages.
    fn find_leaf(&self, pk: &[u8]) -> PageId {
        let root = self.ensure_root();
        let mut cur = root;
        loop {
            let page = match self.pool.fetch_page(cur) {
                Some(p) => p,
                None => return cur,
            };
            if page.page_type() == PAGE_TYPE_LEAF {
                return cur;
            }
            let keys = page.internal_keys();
            let n = keys.len();
            let mut child_idx = n; // default: rightmost child
            for (i, k) in keys.iter().enumerate() {
                if pk < k.as_slice() {
                    child_idx = i;
                    break;
                }
            }
            cur = page.internal_child(child_idx);
            if cur == 0 {
                return *self.first_leaf.read();
            }
        }
    }

    /// Find the path from root to the leaf containing `pk`.
    /// Returns Vec of (page_id, child_index_chosen) for internal pages, ending at the leaf.
    fn find_leaf_path(&self, pk: &[u8]) -> (Vec<(PageId, usize)>, PageId) {
        let root = self.ensure_root();
        let mut path = Vec::new();
        let mut cur = root;
        loop {
            let page = match self.pool.fetch_page(cur) {
                Some(p) => p,
                None => return (path, cur),
            };
            if page.page_type() == PAGE_TYPE_LEAF {
                return (path, cur);
            }
            let keys = page.internal_keys();
            let mut child_idx = keys.len();
            for (i, k) in keys.iter().enumerate() {
                if pk < k.as_slice() {
                    child_idx = i;
                    break;
                }
            }
            path.push((cur, child_idx));
            cur = page.internal_child(child_idx);
            if cur == 0 {
                return (path, *self.first_leaf.read());
            }
        }
    }

    pub fn insert(&self, row: OwnedRow, txn_id: TxnId) -> Result<Vec<u8>, StorageError> {
        let pk = crate::memtable::encode_pk(&row, self.schema.pk_indices());
        // Reject duplicate only if committed (pending writes from other txns are invisible)
        {
            let idx = self.pk_index.read();
            if idx.contains_key(&pk) {
                return Err(StorageError::DuplicateKey);
            }
        }
        self.mvcc_write(pk.clone(), txn_id, Some(row));
        Ok(pk)
    }

    fn split_and_insert(
        &self,
        path: Vec<(PageId, usize)>,
        leaf_id: PageId,
        pk: &[u8],
        row: &OwnedRow,
    ) -> Result<(), StorageError> {
        let leaf = self
            .pool
            .fetch_page(leaf_id)
            .ok_or_else(|| StorageError::Serialization("leaf missing".into()))?;

        let mut all_rows = leaf.leaf_read_rows();
        let pos = all_rows.partition_point(|(k, _): &(Vec<u8>, OwnedRow)| k.as_slice() < pk);
        all_rows.insert(pos, (pk.to_vec(), row.clone()));

        let mid = all_rows.len() / 2;
        let left_rows = &all_rows[..mid];
        let right_rows = &all_rows[mid..];
        let split_key = right_rows[0].0.clone();

        // Reuse old leaf for left half
        let mut left_page = Page::new(leaf_id);
        left_page.set_page_type(PAGE_TYPE_LEAF);
        // Allocate new leaf for right half
        let right_id = self.pool.allocate_page();
        let mut right_page = self.pool.fetch_page(right_id).unwrap();
        right_page.set_page_type(PAGE_TYPE_LEAF);
        right_page.set_next_leaf(leaf.next_leaf()); // preserve chain
        left_page.set_next_leaf(right_id);

        for (k, r) in left_rows {
            left_page.leaf_try_append(k, r);
        }
        for (k, r) in right_rows {
            right_page.leaf_try_append(k, r);
        }

        // Update first_leaf if needed
        if *self.first_leaf.read() == leaf_id && !left_rows.is_empty() {
            // left still has old leaf_id, so first_leaf stays
        }

        self.pool.put_page(left_page);
        self.pool.put_page(right_page);

        // Rebuild pk_index entries for both pages
        self.reindex_leaf(leaf_id);
        self.reindex_leaf(right_id);

        // Push split_key up to parent
        self.insert_into_parent(path, leaf_id, &split_key, right_id);
        Ok(())
    }

    fn reindex_leaf(&self, leaf_id: PageId) {
        if let Some(page) = self.pool.fetch_page(leaf_id) {
            let mut idx = self.pk_index.write();
            for (slot, (pk, _)) in page.leaf_read_rows().into_iter().enumerate() {
                idx.insert(pk, (leaf_id, slot));
            }
        }
    }

    fn insert_into_parent(
        &self,
        path: Vec<(PageId, usize)>,
        left_child: PageId,
        key: &[u8],
        right_child: PageId,
    ) {
        if path.is_empty() {
            // Split root: create new root
            let new_root_id = self.pool.allocate_page();
            let mut new_root = self.pool.fetch_page(new_root_id).unwrap();
            new_root.set_page_type(PAGE_TYPE_INTERNAL);
            new_root.internal_write_keys_and_children(&[key.to_vec()], &[left_child, right_child]);
            self.pool.put_page(new_root);
            *self.root_page.write() = new_root_id;
            write_meta(&self.pool, new_root_id, *self.first_leaf.read());
            return;
        }

        let (parent_id, child_idx) = path[path.len() - 1];
        let mut parent = match self.pool.fetch_page(parent_id) {
            Some(p) => p,
            None => return,
        };

        if parent.internal_can_fit_key(key) {
            let mut keys = parent.internal_keys();
            let n = keys.len();
            let mut children: Vec<PageId> = (0..=n).map(|i| parent.internal_child(i)).collect();

            keys.insert(child_idx, key.to_vec());
            children.insert(child_idx + 1, right_child);

            parent.internal_write_keys_and_children(&keys, &children);
            self.pool.put_page(parent);
        } else {
            // Internal page split (rare)
            let mut keys = parent.internal_keys();
            let n = keys.len();
            let mut children: Vec<PageId> = (0..=n).map(|i| parent.internal_child(i)).collect();
            keys.insert(child_idx, key.to_vec());
            children.insert(child_idx + 1, right_child);

            let mid = keys.len() / 2;
            let push_up_key = keys[mid].clone();
            let left_keys = keys[..mid].to_vec();
            let right_keys = keys[mid + 1..].to_vec();
            let left_children = children[..=mid].to_vec();
            let right_children = children[mid + 1..].to_vec();

            // Reuse parent for left
            parent.internal_write_keys_and_children(&left_keys, &left_children);
            self.pool.put_page(parent);

            let right_int_id = self.pool.allocate_page();
            let mut right_int = self.pool.fetch_page(right_int_id).unwrap();
            right_int.set_page_type(PAGE_TYPE_INTERNAL);
            right_int.internal_write_keys_and_children(&right_keys, &right_children);
            self.pool.put_page(right_int);

            let parent_path = path[..path.len() - 1].to_vec();
            self.insert_into_parent(parent_path, parent_id, &push_up_key, right_int_id);
        }
    }

    fn btree_get(&self, pk: &[u8]) -> Option<OwnedRow> {
        let idx = self.pk_index.read();
        let &(page_id, slot_idx) = idx.get(pk)?;
        let page = self.pool.fetch_page(page_id)?;
        let rows = page.leaf_read_rows();
        rows.get(slot_idx).map(|(_, row)| row.clone())
    }

    pub fn get(&self, pk: &[u8], txn_id: TxnId, read_ts: Timestamp) -> Option<OwnedRow> {
        self.mvcc_get(pk, txn_id, read_ts)
    }

    pub fn delete(&self, pk: &[u8], txn_id: TxnId) -> Result<(), StorageError> {
        // Key must exist (committed) or be pending from same txn
        let exists_committed = self.pk_index.read().contains_key(pk);
        let exists_pending = {
            let mvcc = self.mvcc.read();
            mvcc.get(pk).map_or(false, |c| c.iter().any(|v| v.txn_id == txn_id && v.commit_ts.is_none() && v.row.is_some()))
        };
        if !exists_committed && !exists_pending {
            return Err(StorageError::KeyNotFound);
        }
        self.mvcc_write(pk.to_vec(), txn_id, None);
        Ok(())
    }

    pub fn update(&self, pk: &[u8], new_row: OwnedRow, txn_id: TxnId) -> Result<(), StorageError> {
        // Overwrite: write a new version (tombstone old implicitly via same-key commit upsert)
        self.mvcc_write(pk.to_vec(), txn_id, Some(new_row));
        Ok(())
    }

    /// Sequential scan via leaf chain, applying MVCC visibility at `read_ts`.
    pub fn scan(&self, txn_id: TxnId, read_ts: Timestamp) -> Vec<(Vec<u8>, OwnedRow)> {
        // Collect committed baseline from B-tree
        let idx = self.pk_index.read();
        let mut leaf_id = *self.first_leaf.read();
        let mut base: BTreeMap<Vec<u8>, OwnedRow> = BTreeMap::new();
        while leaf_id != 0 {
            if let Some(page) = self.pool.fetch_page(leaf_id) {
                if page.page_type() != PAGE_TYPE_LEAF { break; }
                for (slot, (pk, row)) in page.leaf_read_rows().into_iter().enumerate() {
                    if let Some(&(ip, is)) = idx.get(&pk) {
                        if ip == leaf_id && is == slot {
                            base.insert(pk, row);
                        }
                    }
                }
                leaf_id = page.next_leaf();
            } else { break; }
        }
        drop(idx);

        // Apply MVCC overlay: own pending writes + committed versions at read_ts.
        // Also: if the MVCC chain's newest committed version is > read_ts, the B-tree
        // baseline (which stores the latest committed state) is "too new" and must be hidden.
        let mvcc = self.mvcc.read();
        for (pk, chain) in mvcc.iter() {
            // Own uncommitted write is always visible to itself
            let own_pending = chain.iter().find(|v| v.txn_id == txn_id && v.commit_ts.is_none());
            if let Some(ver) = own_pending {
                match &ver.row {
                    Some(r) => { base.insert(pk.clone(), r.clone()); }
                    None => { base.remove(pk); }
                }
                continue;
            }
            // Find the latest committed version at or before read_ts
            let visible = chain.iter().find(|v| {
                v.commit_ts.map_or(false, |ts| ts.0 <= read_ts.0)
            });
            // Find if there is a committed version newer than read_ts (B-tree may have it)
            let has_newer = chain.iter().any(|v| {
                v.commit_ts.map_or(false, |ts| ts.0 > read_ts.0)
            });
            if has_newer {
                // B-tree stores the latest committed state which is newer than read_ts.
                // Replace with the correct version at read_ts (or remove if none exists).
                match visible {
                    Some(ver) => match &ver.row {
                        Some(r) => { base.insert(pk.clone(), r.clone()); }
                        None => { base.remove(pk); }
                    },
                    None => { base.remove(pk); }
                }
            } else if let Some(ver) = visible {
                // B-tree baseline is already correct; only apply overlay delta
                match &ver.row {
                    Some(r) => { base.insert(pk.clone(), r.clone()); }
                    None => { base.remove(pk); }
                }
            }
        }
        drop(mvcc);
        base.into_iter().collect()
    }

    /// Range scan: return rows with pk in [start, end).
    pub fn range_scan(
        &self,
        start: &[u8],
        end: &[u8],
        _txn_id: TxnId,
        _read_ts: Timestamp,
    ) -> Vec<(Vec<u8>, OwnedRow)> {
        let idx = self.pk_index.read();
        let mut leaf_id = self.find_leaf(start);
        let mut results = Vec::new();

        while leaf_id != 0 {
            if let Some(page) = self.pool.fetch_page(leaf_id) {
                if page.page_type() != PAGE_TYPE_LEAF {
                    break;
                }
                for (slot, (pk, row)) in page.leaf_read_rows().into_iter().enumerate() {
                    if pk.as_slice() >= end {
                        return results;
                    }
                    if pk.as_slice() >= start {
                        if let Some(&(ip, is)) = idx.get(&pk) {
                            if ip == leaf_id && is == slot {
                                results.push((pk, row));
                            }
                        }
                    }
                }
                leaf_id = page.next_leaf();
            } else {
                break;
            }
        }
        results
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        self.pool.flush_all().map_err(StorageError::Io)
    }

    pub fn stats(&self) -> DiskRowstoreStats {
        let mut leaf_count = 0;
        let mut lid = *self.first_leaf.read();
        while lid != 0 {
            leaf_count += 1;
            if let Some(page) = self.pool.fetch_page(lid) {
                lid = page.next_leaf();
            } else {
                break;
            }
        }
        DiskRowstoreStats {
            leaf_pages: leaf_count,
            rows_indexed: self.pk_index.read().len(),
            rows_written: self.rows_written.load(Ordering::Relaxed),
            rows_deleted: self.rows_deleted.load(Ordering::Relaxed),
        }
    }

    /// B-tree depth (1 = root is leaf, 2 = root→leaf, etc.)
    pub fn tree_depth(&self) -> usize {
        let root = *self.root_page.read();
        if root == 0 {
            return 0;
        }
        let mut depth = 0;
        let mut cur = root;
        loop {
            depth += 1;
            let page = match self.pool.fetch_page(cur) {
                Some(p) => p,
                None => return depth,
            };
            if page.page_type() == PAGE_TYPE_LEAF {
                return depth;
            }
            cur = page.internal_child(0);
            if cur == 0 {
                return depth;
            }
        }
    }
}

/// Observability snapshot for a disk rowstore table.
#[derive(Debug, Clone)]
pub struct DiskRowstoreStats {
    pub leaf_pages: usize,
    pub rows_indexed: usize,
    pub rows_written: u64,
    pub rows_deleted: u64,
}

impl crate::storage_trait::StorageTable for DiskRowstoreTable {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn insert(
        &self,
        row: &OwnedRow,
        txn_id: TxnId,
    ) -> Result<crate::memtable::PrimaryKey, StorageError> {
        DiskRowstoreTable::insert(self, row.clone(), txn_id)
    }
    fn update(
        &self,
        pk: &crate::memtable::PrimaryKey,
        new_row: &OwnedRow,
        txn_id: TxnId,
    ) -> Result<(), StorageError> {
        DiskRowstoreTable::update(self, pk, new_row.clone(), txn_id)
    }
    fn delete(&self, pk: &crate::memtable::PrimaryKey, txn_id: TxnId) -> Result<(), StorageError> {
        DiskRowstoreTable::delete(self, pk, txn_id)
    }
    fn get(
        &self,
        pk: &crate::memtable::PrimaryKey,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Result<Option<OwnedRow>, StorageError> {
        Ok(DiskRowstoreTable::mvcc_get(self, pk, txn_id, read_ts))
    }
    fn scan(
        &self,
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Vec<(crate::memtable::PrimaryKey, OwnedRow)> {
        DiskRowstoreTable::scan(self, txn_id, read_ts)
    }
    fn commit_key(
        &self,
        _pk: &crate::memtable::PrimaryKey,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        self.commit_mvcc(txn_id, commit_ts);
        Ok(())
    }
    fn abort_key(&self, _pk: &crate::memtable::PrimaryKey, txn_id: TxnId) {
        self.abort_mvcc(txn_id);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, StorageType};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(200),
            name: "disk_test".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                    max_length: None,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "val".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                    max_length: None,
                },
            ],
            primary_key_columns: vec![0],
            storage_type: StorageType::DiskRowstore,
            ..Default::default()
        }
    }

    #[test]
    fn test_disk_insert_and_get() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("hello".into())]);
        let pk = table.insert(row.clone(), txn).unwrap();
        let got = table.get(&pk, txn, Timestamp(100));
        assert!(got.is_some());
        assert_eq!(got.unwrap().values, row.values);
    }

    #[test]
    fn test_disk_scan() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        for i in 0..20 {
            table
                .insert(
                    OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("v{i}"))]),
                    txn,
                )
                .unwrap();
        }
        let rows = table.scan(txn, Timestamp(100));
        assert_eq!(rows.len(), 20);
    }

    #[test]
    fn test_disk_delete() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int64(42), Datum::Text("bye".into())]);
        let pk = table.insert(row, txn).unwrap();
        assert!(table.get(&pk, txn, Timestamp(100)).is_some());
        table.delete(&pk, txn).unwrap();
        assert!(table.get(&pk, txn, Timestamp(100)).is_none());
    }

    #[test]
    fn test_disk_update() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        let pk = table
            .insert(
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("old".into())]),
                txn,
            )
            .unwrap();
        table
            .update(
                &pk,
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("new".into())]),
                txn,
            )
            .unwrap();
        let all = table.scan(txn, Timestamp(100));
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].1.values[1], Datum::Text("new".into()));
    }

    #[test]
    fn test_disk_duplicate_key() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("a".into())]);
        table.insert(row.clone(), txn).unwrap();
        // Commit so the key is visible in pk_index
        table.commit_mvcc(txn, Timestamp(1));
        // Now a second insert with a different txn should be rejected
        assert!(table.insert(row, TxnId(2)).is_err());
    }

    #[test]
    fn test_page_split_many_rows() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        for i in 0..200 {
            table
                .insert(
                    OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("row_{i:04}"))]),
                    txn,
                )
                .unwrap();
        }
        // Commit to flush into B-tree pages
        table.commit_mvcc(txn, Timestamp(1));
        let rows = table.scan(TxnId(99), Timestamp(100));
        assert_eq!(rows.len(), 200, "all 200 rows must survive page splits");
        assert!(table.stats().leaf_pages > 1, "should have split into multiple leaves");

        for i in [0, 50, 100, 150, 199] {
            let pk = crate::memtable::encode_pk(
                &OwnedRow::new(vec![Datum::Int64(i), Datum::Text(String::new())]),
                &[0],
            );
            assert!(table.get(&pk, TxnId(99), Timestamp(100)).is_some(), "pk lookup failed for {i}");
        }
    }

    #[test]
    fn test_tree_depth_grows() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        assert_eq!(table.tree_depth(), 0, "empty tree has depth 0");
        table.insert(OwnedRow::new(vec![Datum::Int64(1), Datum::Text("x".into())]), txn).unwrap();
        // Commit to flush into B-tree pages
        table.commit_mvcc(txn, Timestamp(1));
        assert!(table.tree_depth() >= 2, "after commit: root(internal) → leaf");
    }

    #[test]
    fn test_range_scan() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        for i in 0..50i64 {
            table
                .insert(
                    OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("r{i}"))]),
                    txn,
                )
                .unwrap();
        }
        table.commit_mvcc(txn, Timestamp(1));
        let start = crate::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int64(10), Datum::Text(String::new())]),
            &[0],
        );
        let end = crate::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int64(20), Datum::Text(String::new())]),
            &[0],
        );
        let results = table.range_scan(&start, &end, TxnId(99), Timestamp(100));
        assert_eq!(results.len(), 10, "range [10,20) should have 10 rows");
    }

    #[test]
    fn test_leaf_chain_integrity() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        for i in 0..300i64 {
            table
                .insert(
                    OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("v{i}"))]),
                    txn,
                )
                .unwrap();
        }
        table.commit_mvcc(txn, Timestamp(1));
        // Walk the leaf chain and count
        let mut leaf_id = *table.first_leaf.read();
        let mut leaf_count = 0;
        let mut row_count = 0;
        while leaf_id != 0 {
            leaf_count += 1;
            if let Some(page) = table.pool.fetch_page(leaf_id) {
                row_count += page.leaf_slot_count();
                leaf_id = page.next_leaf();
            } else {
                break;
            }
        }
        assert!(leaf_count > 1, "300 rows should span multiple leaves");
        assert!(
            row_count >= 300,
            "leaf chain must contain all rows (some may be stale from splits)"
        );
    }
}
