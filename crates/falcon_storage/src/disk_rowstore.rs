//! On-disk B-tree row store — modeled after InnoDB / SingleStore disk tables.
//!
//! Provides a page-based storage engine with:
//!   - Fixed-size 8 KiB pages
//!   - LRU buffer pool for caching hot pages in memory
//!   - B-tree index on primary key for O(log N) point lookups
//!   - Sequential scan via leaf-page chain
//!
//! This engine is suitable for datasets larger than available memory
//! while still supporting OLTP-style row-oriented access.

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
// Configuration
// ---------------------------------------------------------------------------

/// Page size in bytes (8 KiB).
pub const PAGE_SIZE: usize = 8192;

/// Default buffer pool capacity (number of pages).
pub const DEFAULT_BUFFER_POOL_PAGES: usize = 1024;

/// Page ID type.
pub type PageId = u64;

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

fn serialize_row(row: &OwnedRow) -> Vec<u8> {
    bincode::serialize(row).unwrap_or_default()
}

fn deserialize_row(bytes: &[u8]) -> Option<OwnedRow> {
    bincode::deserialize(bytes).ok()
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

/// An 8 KiB page that stores a variable number of row slots.
/// Layout: [row_count: u32][slot_offsets: u32 * row_count][row_data...]
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

    fn row_count(&self) -> usize {
        if self.data.len() < 4 { return 0; }
        u32::from_le_bytes(self.data[0..4].try_into().unwrap_or([0; 4])) as usize
    }

    fn set_row_count(&mut self, count: usize) {
        let bytes = (count as u32).to_le_bytes();
        self.data[0..4].copy_from_slice(&bytes);
    }

    /// Read all rows stored in this page.
    fn read_rows(&self) -> Vec<(Vec<u8>, OwnedRow)> {
        let count = self.row_count();
        if count == 0 { return Vec::new(); }

        let header_size = 4 + count * 8; // row_count + (offset, length) per row
        let mut rows = Vec::with_capacity(count);

        for i in 0..count {
            let slot_base = 4 + i * 8;
            if slot_base + 8 > self.data.len() { break; }
            let offset = u32::from_le_bytes(
                self.data[slot_base..slot_base + 4].try_into().unwrap_or([0; 4])
            ) as usize;
            let length = u32::from_le_bytes(
                self.data[slot_base + 4..slot_base + 8].try_into().unwrap_or([0; 4])
            ) as usize;

            if offset + length > self.data.len() { continue; }
            let row_bytes = &self.data[offset..offset + length];

            // First 4 bytes of row_bytes = pk_len, then pk, then row data
            if row_bytes.len() < 4 { continue; }
            let pk_len = u32::from_le_bytes(row_bytes[0..4].try_into().unwrap_or([0; 4])) as usize;
            if 4 + pk_len > row_bytes.len() { continue; }
            let pk = row_bytes[4..4 + pk_len].to_vec();
            let row_data = &row_bytes[4 + pk_len..];
            if let Some(row) = deserialize_row(row_data) {
                rows.push((pk, row));
            }
        }
        let _ = header_size; // suppress unused warning
        rows
    }

    /// Try to append a row to this page. Returns false if page is full.
    fn try_append(&mut self, pk: &[u8], row: &OwnedRow) -> bool {
        let row_bytes = serialize_row(row);
        let pk_len = pk.len();
        // Total slot data: 4 (pk_len) + pk + row_bytes
        let slot_data_len = 4 + pk_len + row_bytes.len();

        let count = self.row_count();
        let new_count = count + 1;
        let header_size = 4 + new_count * 8;

        // Find current data end
        let mut data_end = header_size;
        for i in 0..count {
            let slot_base = 4 + i * 8;
            let offset = u32::from_le_bytes(
                self.data[slot_base..slot_base + 4].try_into().unwrap_or([0; 4])
            ) as usize;
            let length = u32::from_le_bytes(
                self.data[slot_base + 4..slot_base + 8].try_into().unwrap_or([0; 4])
            ) as usize;
            let end = offset + length;
            if end > data_end { data_end = end; }
        }

        // Check if we have enough space
        let old_header = 4 + count * 8;
        // We need to shift data if the header grew
        let header_growth = header_size - old_header; // always 8 (one more slot)
        let needed = data_end + header_growth + slot_data_len;
        if needed > PAGE_SIZE {
            return false;
        }

        // Shift existing data to make room for the new slot entry in the header
        if header_growth > 0 && data_end > old_header {
            self.data.copy_within(old_header..data_end, old_header + header_growth);
            // Update existing slot offsets
            for i in 0..count {
                let slot_base = 4 + i * 8;
                let old_offset = u32::from_le_bytes(
                    self.data[slot_base..slot_base + 4].try_into().unwrap_or([0; 4])
                );
                let new_offset = old_offset + header_growth as u32;
                self.data[slot_base..slot_base + 4].copy_from_slice(&new_offset.to_le_bytes());
            }
            data_end += header_growth;
        }

        // Write slot entry
        let slot_base = 4 + count * 8;
        self.data[slot_base..slot_base + 4].copy_from_slice(&(data_end as u32).to_le_bytes());
        self.data[slot_base + 4..slot_base + 8].copy_from_slice(&(slot_data_len as u32).to_le_bytes());

        // Write slot data: pk_len + pk + row_bytes
        let pk_len_bytes = (pk_len as u32).to_le_bytes();
        self.data[data_end..data_end + 4].copy_from_slice(&pk_len_bytes);
        self.data[data_end + 4..data_end + 4 + pk_len].copy_from_slice(pk);
        self.data[data_end + 4 + pk_len..data_end + 4 + pk_len + row_bytes.len()]
            .copy_from_slice(&row_bytes);

        self.set_row_count(new_count);
        self.dirty = true;
        true
    }
}

// ---------------------------------------------------------------------------
// Buffer Pool
// ---------------------------------------------------------------------------

/// LRU buffer pool that caches disk pages in memory.
pub struct BufferPool {
    capacity: usize,
    /// page_id -> cached page
    pages: RwLock<HashMap<PageId, Page>>,
    /// LRU order: front = least recently used
    lru: RwLock<VecDeque<PageId>>,
    /// The backing data file.
    file: RwLock<File>,
    /// Next page ID to allocate.
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
        let next_page = if file_len == 0 { 1 } else { file_len / PAGE_SIZE as u64 + 1 };

        Ok(Self {
            capacity,
            pages: RwLock::new(HashMap::new()),
            lru: RwLock::new(VecDeque::new()),
            file: RwLock::new(file),
            next_page_id: AtomicU64::new(next_page),
        })
    }

    /// Allocate a new page.
    pub fn allocate_page(&self) -> PageId {
        let id = self.next_page_id.fetch_add(1, Ordering::Relaxed);
        let page = Page::new(id);
        self.put_page(page);
        id
    }

    /// Fetch a page, loading from disk if not cached.
    pub fn fetch_page(&self, page_id: PageId) -> Option<Page> {
        // Check cache first
        {
            let pages = self.pages.read();
            if let Some(page) = pages.get(&page_id) {
                self.touch_lru(page_id);
                return Some(page.clone());
            }
        }
        // Load from disk
        self.load_page(page_id)
    }

    /// Put a page into the buffer pool.
    pub fn put_page(&self, page: Page) {
        let page_id = page.id;
        {
            let mut pages = self.pages.write();
            pages.insert(page_id, page);
        }
        self.touch_lru(page_id);
        self.maybe_evict();
    }

    /// Flush all dirty pages to disk.
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
        file.flush()?;
        Ok(())
    }

    fn load_page(&self, page_id: PageId) -> Option<Page> {
        let mut page = Page::new(page_id);
        let offset = (page_id - 1) * PAGE_SIZE as u64;
        let mut file = self.file.write();
        if file.seek(SeekFrom::Start(offset)).is_err() { return None; }
        if file.read_exact(&mut page.data).is_err() { return None; }
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
        while pages.len() > self.capacity && !lru.is_empty() {
            if let Some(victim_id) = lru.pop_front() {
                if let Some(page) = pages.get(&victim_id) {
                    if page.dirty {
                        // Flush before evict
                        let offset = (page.id - 1) * PAGE_SIZE as u64;
                        let mut file = self.file.write();
                        let _ = file.seek(SeekFrom::Start(offset));
                        let _ = file.write_all(&page.data);
                    }
                }
                pages.remove(&victim_id);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// DiskRowstoreTable
// ---------------------------------------------------------------------------

/// On-disk B-tree row store table.
pub struct DiskRowstoreTable {
    pub schema: TableSchema,
    /// Buffer pool for page I/O.
    pool: Arc<BufferPool>,
    /// B-tree index: PK -> (page_id, slot_index).
    pk_index: RwLock<BTreeMap<Vec<u8>, (PageId, usize)>>,
    /// List of leaf page IDs (in insertion order for sequential scan).
    leaf_pages: RwLock<Vec<PageId>>,
    /// Metrics.
    rows_written: AtomicU64,
    rows_deleted: AtomicU64,
}

impl DiskRowstoreTable {
    pub fn new(schema: TableSchema, data_dir: &Path) -> Result<Self, StorageError> {
        let table_file = data_dir.join(format!("table_{}.dat", schema.id.0));
        let pool = Arc::new(
            BufferPool::open(&table_file, DEFAULT_BUFFER_POOL_PAGES)
                .map_err(StorageError::Io)?
        );

        Ok(Self {
            schema,
            pool,
            pk_index: RwLock::new(BTreeMap::new()),
            leaf_pages: RwLock::new(Vec::new()),
            rows_written: AtomicU64::new(0),
            rows_deleted: AtomicU64::new(0),
        })
    }

    /// Create an in-memory-only disk rowstore (for testing without actual files).
    pub fn new_in_memory(schema: TableSchema) -> Result<Self, StorageError> {
        // Use a temp file
        let tmp_dir = std::env::temp_dir().join("falcon_disk_rs");
        std::fs::create_dir_all(&tmp_dir)
            .map_err(StorageError::Io)?;
        Self::new(schema, &tmp_dir)
    }

    pub fn table_id(&self) -> TableId {
        self.schema.id
    }

    /// Insert a row. Encodes PK, appends to the latest leaf page (or allocates
    /// a new one if the current page is full), and updates the B-tree index.
    pub fn insert(&self, row: OwnedRow, _txn_id: TxnId) -> Result<Vec<u8>, StorageError> {
        let pk = crate::memtable::encode_pk(&row, self.schema.pk_indices());

        // Check for duplicate PK
        {
            let idx = self.pk_index.read();
            if idx.contains_key(&pk) {
                return Err(StorageError::DuplicateKey);
            }
        }

        // Find or allocate a leaf page
        let page_id = self.get_or_alloc_leaf_page(&pk, &row);

        // Fetch page, append row
        if let Some(mut page) = self.pool.fetch_page(page_id) {
            let slot_idx = page.row_count();
            if page.try_append(&pk, &row) {
                // Update index
                let mut idx = self.pk_index.write();
                idx.insert(pk.clone(), (page_id, slot_idx));
                self.pool.put_page(page);
                self.rows_written.fetch_add(1, Ordering::Relaxed);
                return Ok(pk);
            }
            // Page was full — allocate a new page
            let new_page_id = self.pool.allocate_page();
            {
                let mut leaves = self.leaf_pages.write();
                leaves.push(new_page_id);
            }
            if let Some(mut new_page) = self.pool.fetch_page(new_page_id) {
                let slot_idx = 0;
                if new_page.try_append(&pk, &row) {
                    let mut idx = self.pk_index.write();
                    idx.insert(pk.clone(), (new_page_id, slot_idx));
                    self.pool.put_page(new_page);
                    self.rows_written.fetch_add(1, Ordering::Relaxed);
                    return Ok(pk);
                }
            }
        }

        Err(StorageError::Io(std::io::Error::other("Failed to insert row into page")))
    }

    /// Point read by primary key.
    pub fn get(&self, pk: &[u8], _txn_id: TxnId, _read_ts: Timestamp) -> Option<OwnedRow> {
        let idx = self.pk_index.read();
        let &(page_id, slot_idx) = idx.get(pk)?;
        let page = self.pool.fetch_page(page_id)?;
        let rows = page.read_rows();
        rows.get(slot_idx).map(|(_, row)| row.clone())
    }

    /// Delete a row by PK (removes from index; page space is not reclaimed).
    pub fn delete(&self, pk: &[u8], _txn_id: TxnId) -> Result<(), StorageError> {
        let mut idx = self.pk_index.write();
        if idx.remove(pk).is_some() {
            self.rows_deleted.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            Err(StorageError::KeyNotFound)
        }
    }

    /// Update: delete + insert.
    pub fn update(&self, pk: &[u8], new_row: OwnedRow, txn_id: TxnId) -> Result<(), StorageError> {
        self.delete(pk, txn_id)?;
        self.insert(new_row, txn_id)?;
        Ok(())
    }

    /// Full sequential scan through all leaf pages.
    pub fn scan(&self, _txn_id: TxnId, _read_ts: Timestamp) -> Vec<(Vec<u8>, OwnedRow)> {
        let idx = self.pk_index.read();
        let leaves = self.leaf_pages.read();
        let mut results = Vec::new();

        for &page_id in leaves.iter() {
            if let Some(page) = self.pool.fetch_page(page_id) {
                for (pk, row) in page.read_rows() {
                    // Only include rows still in the index (not deleted)
                    if idx.contains_key(&pk) {
                        results.push((pk, row));
                    }
                }
            }
        }
        results
    }

    /// Flush all dirty pages to disk.
    pub fn flush(&self) -> Result<(), StorageError> {
        self.pool.flush_all().map_err(StorageError::Io)
    }

    /// Stats snapshot.
    pub fn stats(&self) -> DiskRowstoreStats {
        DiskRowstoreStats {
            leaf_pages: self.leaf_pages.read().len(),
            rows_indexed: self.pk_index.read().len(),
            rows_written: self.rows_written.load(Ordering::Relaxed),
            rows_deleted: self.rows_deleted.load(Ordering::Relaxed),
        }
    }

    fn get_or_alloc_leaf_page(&self, _pk: &[u8], _row: &OwnedRow) -> PageId {
        let leaves = self.leaf_pages.read();
        if let Some(&last) = leaves.last() {
            return last;
        }
        drop(leaves);
        let page_id = self.pool.allocate_page();
        let mut leaves = self.leaf_pages.write();
        leaves.push(page_id);
        page_id
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, StorageType};
    use falcon_common::types::{ColumnId, TableId, Timestamp, TxnId};

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(200),
            name: "disk_test".to_string(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".to_string(),
                    data_type: falcon_common::types::DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "val".to_string(),
                    data_type: falcon_common::types::DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: Default::default(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            storage_type: StorageType::DiskRowstore,
            ..Default::default()
        }
    }

    #[test]
    fn test_disk_insert_and_get() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("hello".to_string())]);
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
            let row = OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("v{}", i))]);
            table.insert(row, txn).unwrap();
        }
        let rows = table.scan(txn, Timestamp(100));
        assert_eq!(rows.len(), 20);
    }

    #[test]
    fn test_disk_delete() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int64(42), Datum::Text("bye".to_string())]);
        let pk = table.insert(row, txn).unwrap();
        assert!(table.get(&pk, txn, Timestamp(100)).is_some());

        table.delete(&pk, txn).unwrap();
        assert!(table.get(&pk, txn, Timestamp(100)).is_none());
    }

    #[test]
    fn test_disk_update() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("old".to_string())]);
        let pk = table.insert(row, txn).unwrap();

        let new_row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("new".to_string())]);
        table.update(&pk, new_row.clone(), txn).unwrap();

        // The PK might change due to re-insert, so scan
        let all = table.scan(txn, Timestamp(100));
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].1.values[1], Datum::Text("new".to_string()));
    }

    #[test]
    fn test_disk_duplicate_key() {
        let table = DiskRowstoreTable::new_in_memory(test_schema()).unwrap();
        let txn = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("a".to_string())]);
        table.insert(row.clone(), txn).unwrap();
        let result = table.insert(row, txn);
        assert!(result.is_err());
    }
}
