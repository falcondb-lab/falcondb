//! LRU block cache for SST data blocks.
//!
//! Caches recently-read SST data blocks to avoid repeated disk I/O.
//! The cache is sharded by block key (sst_id, block_offset) for concurrency.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

/// Unique identifier for a cached block: (SST file id, block offset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub sst_id: u64,
    pub offset: u64,
}

/// A cached data block (raw bytes from an SST file).
#[derive(Debug, Clone)]
pub struct CachedBlock {
    pub data: Vec<u8>,
}

/// Entry in the LRU list.
struct LruEntry {
    key: BlockKey,
    block: CachedBlock,
    size: usize,
}

/// LRU block cache with a fixed memory budget.
pub struct BlockCache {
    /// Maximum cache size in bytes.
    capacity_bytes: usize,
    /// Current size in bytes.
    current_bytes: Mutex<usize>,
    /// Map from block key to index in the entries list.
    map: Mutex<HashMap<BlockKey, usize>>,
    /// LRU-ordered entries (index 0 = most recently used).
    entries: Mutex<Vec<LruEntry>>,
    /// Statistics.
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    inserts: AtomicU64,
}

impl BlockCache {
    /// Create a new block cache with the given capacity in bytes.
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            current_bytes: Mutex::new(0),
            map: Mutex::new(HashMap::new()),
            entries: Mutex::new(Vec::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
        }
    }

    /// Look up a block in the cache. Returns None on miss.
    pub fn get(&self, key: &BlockKey) -> Option<CachedBlock> {
        let map = self.map.lock();
        if let Some(&idx) = map.get(key) {
            drop(map);
            // Move to front (most recently used)
            let mut entries = self.entries.lock();
            if idx < entries.len() {
                let block = entries[idx].block.clone();
                // Simple promotion: swap with front
                if idx > 0 {
                    entries.swap(0, idx);
                    // Update map indices
                    let mut map = self.map.lock();
                    if let Some(v) = map.get_mut(&entries[0].key) { *v = 0; }
                    if let Some(v) = map.get_mut(&entries[idx].key) { *v = idx; }
                }
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(block);
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Insert a block into the cache. Evicts LRU entries if over capacity.
    pub fn insert(&self, key: BlockKey, block: CachedBlock) {
        let block_size = block.data.len() + std::mem::size_of::<LruEntry>();

        // Don't cache blocks larger than the entire cache
        if block_size > self.capacity_bytes {
            return;
        }

        let mut map = self.map.lock();
        let mut entries = self.entries.lock();
        let mut current = self.current_bytes.lock();

        // If already cached, update in place
        if let Some(&idx) = map.get(&key) {
            if idx < entries.len() {
                let old_size = entries[idx].size;
                entries[idx].block = block;
                entries[idx].size = block_size;
                *current = current.wrapping_sub(old_size).wrapping_add(block_size);
                return;
            }
        }

        // Evict LRU entries until we have room
        while *current + block_size > self.capacity_bytes && !entries.is_empty() {
            let evicted = entries.pop().unwrap();
            map.remove(&evicted.key);
            *current -= evicted.size;
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }

        // Insert at front (most recently used)
        let new_idx = 0;
        entries.insert(0, LruEntry { key, block, size: block_size });
        *current += block_size;

        // Rebuild map indices (shift everything after insert point)
        map.clear();
        for (i, entry) in entries.iter().enumerate() {
            map.insert(entry.key, i);
        }

        self.inserts.fetch_add(1, Ordering::Relaxed);
        let _ = new_idx; // suppress warning
    }

    /// Current number of cached blocks.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }

    /// Current memory usage in bytes.
    pub fn current_bytes(&self) -> usize {
        *self.current_bytes.lock()
    }

    /// Cache capacity in bytes.
    pub fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    /// Cache hit count.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Cache miss count.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Cache eviction count.
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Hit rate as a fraction (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let h = self.hits() as f64;
        let m = self.misses() as f64;
        if h + m == 0.0 { 0.0 } else { h / (h + m) }
    }

    /// Snapshot of cache statistics.
    pub fn snapshot(&self) -> BlockCacheSnapshot {
        BlockCacheSnapshot {
            capacity_bytes: self.capacity_bytes,
            current_bytes: *self.current_bytes.lock(),
            block_count: self.entries.lock().len(),
            hits: self.hits(),
            misses: self.misses(),
            evictions: self.evictions(),
            inserts: self.inserts.load(Ordering::Relaxed),
            hit_rate: self.hit_rate(),
        }
    }
}

/// Point-in-time snapshot of block cache statistics.
#[derive(Debug, Clone)]
pub struct BlockCacheSnapshot {
    pub capacity_bytes: usize,
    pub current_bytes: usize,
    pub block_count: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub inserts: u64,
    pub hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic_insert_get() {
        let cache = BlockCache::new(4096);
        let key = BlockKey { sst_id: 1, offset: 0 };
        let block = CachedBlock { data: vec![1, 2, 3, 4] };

        cache.insert(key, block.clone());
        let result = cache.get(&key);
        assert!(result.is_some());
        assert_eq!(result.unwrap().data, vec![1, 2, 3, 4]);
        assert_eq!(cache.hits(), 1);
    }

    #[test]
    fn test_cache_miss() {
        let cache = BlockCache::new(4096);
        let key = BlockKey { sst_id: 99, offset: 0 };
        assert!(cache.get(&key).is_none());
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn test_cache_eviction() {
        // Small cache: 200 bytes
        let cache = BlockCache::new(200);

        for i in 0..10u64 {
            let key = BlockKey { sst_id: i, offset: 0 };
            let block = CachedBlock { data: vec![0u8; 50] };
            cache.insert(key, block);
        }

        // Should have evicted some entries
        assert!(cache.evictions() > 0);
        assert!(cache.current_bytes() <= 200);
    }

    #[test]
    fn test_cache_hit_rate() {
        let cache = BlockCache::new(4096);
        let key = BlockKey { sst_id: 1, offset: 0 };
        cache.insert(key, CachedBlock { data: vec![1] });

        cache.get(&key); // hit
        cache.get(&key); // hit
        cache.get(&BlockKey { sst_id: 99, offset: 0 }); // miss

        assert_eq!(cache.hits(), 2);
        assert_eq!(cache.misses(), 1);
        assert!((cache.hit_rate() - 0.6667).abs() < 0.01);
    }

    #[test]
    fn test_cache_oversized_block_rejected() {
        let cache = BlockCache::new(100);
        let key = BlockKey { sst_id: 1, offset: 0 };
        let block = CachedBlock { data: vec![0u8; 200] };
        cache.insert(key, block);
        assert!(cache.is_empty());
    }
}
