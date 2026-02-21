//! LSM MemTable — sorted in-memory write buffer.
//!
//! All writes go to the active memtable first. When the memtable exceeds
//! its size budget, it is frozen (made immutable) and a new active memtable
//! is created. The frozen memtable is then flushed to an L0 SST file by
//! the background flush thread.
//!
//! Uses a `BTreeMap` for sorted iteration during flush.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

/// A value in the memtable. `None` represents a tombstone (deletion marker).
#[derive(Debug, Clone)]
pub struct MemTableValue {
    /// The raw value bytes, or None for a tombstone.
    pub data: Option<Vec<u8>>,
    /// Sequence number for ordering within the same key.
    pub seq: u64,
}

/// Sorted in-memory write buffer backed by a BTreeMap.
pub struct LsmMemTable {
    /// Sorted map: key → value (latest write wins within a memtable).
    map: RwLock<BTreeMap<Vec<u8>, MemTableValue>>,
    /// Approximate size in bytes (keys + values).
    approx_bytes: AtomicU64,
    /// Number of entries.
    entry_count: AtomicU64,
    /// Whether this memtable is frozen (immutable, pending flush).
    frozen: RwLock<bool>,
    /// Global sequence counter reference for assigning seq numbers.
    next_seq: AtomicU64,
}

impl LsmMemTable {
    /// Create a new empty memtable.
    pub fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
            approx_bytes: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
            frozen: RwLock::new(false),
            next_seq: AtomicU64::new(0),
        }
    }

    /// Create a new memtable with a starting sequence number.
    pub fn with_seq(start_seq: u64) -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
            approx_bytes: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
            frozen: RwLock::new(false),
            next_seq: AtomicU64::new(start_seq),
        }
    }

    /// Put a key-value pair. Overwrites any existing value for the key.
    /// Returns `Err` if the memtable is frozen.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemTableError> {
        if *self.frozen.read() {
            return Err(MemTableError::Frozen);
        }

        let entry_size = key.len() + value.len() + std::mem::size_of::<MemTableValue>();
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);

        let mut map = self.map.write();
        let old_size = map.get(&key).map(|v| {
            key.len() + v.data.as_ref().map(|d| d.len()).unwrap_or(0)
                + std::mem::size_of::<MemTableValue>()
        }).unwrap_or(0);

        map.insert(key, MemTableValue { data: Some(value), seq });

        // Update approximate size
        if old_size > 0 {
            // Replacement: subtract old, add new
            self.approx_bytes.fetch_sub(old_size as u64, Ordering::Relaxed);
        } else {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }
        self.approx_bytes.fetch_add(entry_size as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Delete a key by inserting a tombstone.
    /// Returns `Err` if the memtable is frozen.
    pub fn delete(&self, key: Vec<u8>) -> Result<(), MemTableError> {
        if *self.frozen.read() {
            return Err(MemTableError::Frozen);
        }

        let entry_size = key.len() + std::mem::size_of::<MemTableValue>();
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);

        let mut map = self.map.write();
        let old_size = map.get(&key).map(|v| {
            key.len() + v.data.as_ref().map(|d| d.len()).unwrap_or(0)
                + std::mem::size_of::<MemTableValue>()
        }).unwrap_or(0);

        map.insert(key, MemTableValue { data: None, seq });

        if old_size > 0 {
            self.approx_bytes.fetch_sub(old_size as u64, Ordering::Relaxed);
        } else {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }
        self.approx_bytes.fetch_add(entry_size as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Point lookup. Returns `Some(Some(value))` for live entries,
    /// `Some(None)` for tombstones, `None` if key not found.
    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let map = self.map.read();
        map.get(key).map(|v| v.data.clone())
    }

    /// Freeze this memtable (make it immutable). After freezing, no more
    /// writes are accepted and the memtable is ready for flush to SST.
    pub fn freeze(&self) {
        *self.frozen.write() = true;
    }

    /// Whether this memtable is frozen.
    pub fn is_frozen(&self) -> bool {
        *self.frozen.read()
    }

    /// Approximate memory usage in bytes.
    pub fn approx_bytes(&self) -> u64 {
        self.approx_bytes.load(Ordering::Relaxed)
    }

    /// Number of entries (including tombstones).
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Whether the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.entry_count() == 0
    }

    /// Current sequence number.
    pub fn current_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Relaxed)
    }

    /// Iterate all entries in sorted key order. Used during flush to SST.
    /// Returns (key, value_bytes_or_none_for_tombstone, seq).
    pub fn iter_sorted(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        let map = self.map.read();
        map.iter()
            .map(|(k, v)| (k.clone(), v.data.clone(), v.seq))
            .collect()
    }
}

/// Errors from memtable operations.
#[derive(Debug, Clone)]
pub enum MemTableError {
    /// The memtable is frozen and cannot accept writes.
    Frozen,
}

impl std::fmt::Display for MemTableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemTableError::Frozen => write!(f, "memtable is frozen"),
        }
    }
}

impl std::error::Error for MemTableError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_put_get() {
        let mt = LsmMemTable::new();
        mt.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        mt.put(b"key2".to_vec(), b"val2".to_vec()).unwrap();

        assert_eq!(mt.get(b"key1"), Some(Some(b"val1".to_vec())));
        assert_eq!(mt.get(b"key2"), Some(Some(b"val2".to_vec())));
        assert_eq!(mt.get(b"key3"), None);
        assert_eq!(mt.entry_count(), 2);
    }

    #[test]
    fn test_memtable_overwrite() {
        let mt = LsmMemTable::new();
        mt.put(b"key1".to_vec(), b"old".to_vec()).unwrap();
        mt.put(b"key1".to_vec(), b"new".to_vec()).unwrap();

        assert_eq!(mt.get(b"key1"), Some(Some(b"new".to_vec())));
        assert_eq!(mt.entry_count(), 1); // still 1 entry
    }

    #[test]
    fn test_memtable_delete_tombstone() {
        let mt = LsmMemTable::new();
        mt.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        mt.delete(b"key1".to_vec()).unwrap();

        // Tombstone: key exists but value is None
        assert_eq!(mt.get(b"key1"), Some(None));
    }

    #[test]
    fn test_memtable_freeze() {
        let mt = LsmMemTable::new();
        mt.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        mt.freeze();

        assert!(mt.is_frozen());
        assert!(mt.put(b"key2".to_vec(), b"val2".to_vec()).is_err());
        assert!(mt.delete(b"key1".to_vec()).is_err());

        // Reads still work
        assert_eq!(mt.get(b"key1"), Some(Some(b"val1".to_vec())));
    }

    #[test]
    fn test_memtable_sorted_iteration() {
        let mt = LsmMemTable::new();
        mt.put(b"ccc".to_vec(), b"3".to_vec()).unwrap();
        mt.put(b"aaa".to_vec(), b"1".to_vec()).unwrap();
        mt.put(b"bbb".to_vec(), b"2".to_vec()).unwrap();

        let entries = mt.iter_sorted();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, b"aaa");
        assert_eq!(entries[1].0, b"bbb");
        assert_eq!(entries[2].0, b"ccc");
    }

    #[test]
    fn test_memtable_approx_bytes() {
        let mt = LsmMemTable::new();
        assert_eq!(mt.approx_bytes(), 0);

        mt.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        assert!(mt.approx_bytes() > 0);
    }

    #[test]
    fn test_memtable_delete_nonexistent() {
        let mt = LsmMemTable::new();
        mt.delete(b"ghost".to_vec()).unwrap();
        assert_eq!(mt.get(b"ghost"), Some(None)); // tombstone
        assert_eq!(mt.entry_count(), 1);
    }
}
