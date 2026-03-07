//! LSM MemTable — sharded sorted in-memory write buffer.
//!
//! RocksDB-inspired: 16 shards keyed by FNV hash of the key.
//! Each shard has its own RwLock<BTreeMap>, so writes to different
//! keys proceed in parallel. Frozen flag is AtomicBool (no lock on hot path).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::RwLock;

const NUM_SHARDS: usize = 16;

#[derive(Debug, Clone)]
pub struct MemTableValue {
    pub data: Option<Vec<u8>>,
    pub seq: u64,
}

struct MemShard {
    map: RwLock<BTreeMap<Vec<u8>, MemTableValue>>,
}

fn shard_idx(key: &[u8]) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h as usize) & (NUM_SHARDS - 1)
}

fn entry_bytes(key_len: usize, val: &MemTableValue) -> usize {
    key_len + val.data.as_ref().map(|d| d.len()).unwrap_or(0) + std::mem::size_of::<MemTableValue>()
}

pub struct LsmMemTable {
    shards: Box<[MemShard]>,
    approx_bytes: AtomicU64,
    entry_count: AtomicU64,
    frozen: AtomicBool,
    next_seq: AtomicU64,
}

impl Default for LsmMemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl LsmMemTable {
    pub fn new() -> Self {
        let shards: Vec<MemShard> = (0..NUM_SHARDS)
            .map(|_| MemShard {
                map: RwLock::new(BTreeMap::new()),
            })
            .collect();
        Self {
            shards: shards.into_boxed_slice(),
            approx_bytes: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
            frozen: AtomicBool::new(false),
            next_seq: AtomicU64::new(0),
        }
    }

    pub fn with_seq(start_seq: u64) -> Self {
        let mt = Self::new();
        mt.next_seq.store(start_seq, Ordering::Relaxed);
        mt
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemTableError> {
        if self.frozen.load(Ordering::Acquire) {
            return Err(MemTableError::Frozen);
        }
        let new_size = key.len() + value.len() + std::mem::size_of::<MemTableValue>();
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);

        let shard = &self.shards[shard_idx(&key)];
        let mut map = shard.map.write();
        let old_size = map
            .get(&key)
            .map(|v| entry_bytes(key.len(), v))
            .unwrap_or(0);
        map.insert(
            key,
            MemTableValue {
                data: Some(value),
                seq,
            },
        );
        drop(map);

        if old_size > 0 {
            self.approx_bytes
                .fetch_sub(old_size as u64, Ordering::Relaxed);
        } else {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }
        self.approx_bytes
            .fetch_add(new_size as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<(), MemTableError> {
        if self.frozen.load(Ordering::Acquire) {
            return Err(MemTableError::Frozen);
        }
        let new_size = key.len() + std::mem::size_of::<MemTableValue>();
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);

        let shard = &self.shards[shard_idx(&key)];
        let mut map = shard.map.write();
        let old_size = map
            .get(&key)
            .map(|v| entry_bytes(key.len(), v))
            .unwrap_or(0);
        map.insert(key, MemTableValue { data: None, seq });
        drop(map);

        if old_size > 0 {
            self.approx_bytes
                .fetch_sub(old_size as u64, Ordering::Relaxed);
        } else {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }
        self.approx_bytes
            .fetch_add(new_size as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let shard = &self.shards[shard_idx(key)];
        let map = shard.map.read();
        map.get(key).map(|v| v.data.clone())
    }

    pub fn freeze(&self) {
        self.frozen.store(true, Ordering::Release);
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Acquire)
    }

    pub fn approx_bytes(&self) -> u64 {
        self.approx_bytes.load(Ordering::Relaxed)
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count() == 0
    }

    pub fn current_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Relaxed)
    }

    /// Iterate all entries in sorted key order (used during flush).
    /// Merges all shards into a single sorted result.
    pub fn iter_sorted(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        let mut entries = Vec::new();
        for shard in self.shards.iter() {
            let map = shard.map.read();
            entries.extend(map.iter().map(|(k, v)| (k.clone(), v.data.clone(), v.seq)));
        }
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    /// Batch put: insert multiple key-value pairs under a single lock per shard.
    pub fn put_batch(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<(), MemTableError> {
        if self.frozen.load(Ordering::Acquire) {
            return Err(MemTableError::Frozen);
        }
        // Group by shard
        let mut by_shard: [Vec<usize>; NUM_SHARDS] = Default::default();
        for (i, (key, _)) in pairs.iter().enumerate() {
            by_shard[shard_idx(key)].push(i);
        }
        for (si, indices) in by_shard.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let shard = &self.shards[si];
            let mut map = shard.map.write();
            for &i in indices {
                let (key, value) = &pairs[i];
                let new_size = key.len() + value.len() + std::mem::size_of::<MemTableValue>();
                let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
                let old_size = map.get(key).map(|v| entry_bytes(key.len(), v)).unwrap_or(0);
                map.insert(
                    key.clone(),
                    MemTableValue {
                        data: Some(value.clone()),
                        seq,
                    },
                );
                if old_size > 0 {
                    self.approx_bytes
                        .fetch_sub(old_size as u64, Ordering::Relaxed);
                } else {
                    self.entry_count.fetch_add(1, Ordering::Relaxed);
                }
                self.approx_bytes
                    .fetch_add(new_size as u64, Ordering::Relaxed);
            }
        }
        Ok(())
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
