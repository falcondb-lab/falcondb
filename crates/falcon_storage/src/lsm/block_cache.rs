//! Sharded block cache for SST data blocks (RocksDB-inspired).
//!
//! 16 shards keyed by (sst_id ^ offset). Each shard is a HashMap with
//! capacity-based eviction. O(1) insert/lookup per shard.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

const CACHE_SHARDS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub sst_id: u64,
    pub offset: u64,
}

#[derive(Debug, Clone)]
pub struct CachedBlock {
    pub data: Vec<u8>,
}

struct CacheShard {
    map: HashMap<BlockKey, CachedBlock>,
    lru_order: VecDeque<BlockKey>, // front = most recent, back = least recent
    current_bytes: usize,
    capacity_bytes: usize,
}

impl CacheShard {
    fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            lru_order: VecDeque::new(),
            current_bytes: 0,
            capacity_bytes: capacity,
        }
    }

    fn touch(&mut self, key: &BlockKey) {
        if let Some(pos) = self.lru_order.iter().position(|k| k == key) {
            self.lru_order.remove(pos);
        }
        self.lru_order.push_front(*key);
    }

    fn get(&mut self, key: &BlockKey) -> Option<CachedBlock> {
        let block = self.map.get(key).cloned();
        if block.is_some() {
            self.touch(key);
        }
        block
    }

    fn insert(&mut self, key: BlockKey, block: CachedBlock) -> (bool, u32) {
        let block_size = block.data.len();
        if block_size > self.capacity_bytes {
            return (false, 0);
        }

        // Update existing
        if let Some(old) = self.map.get_mut(&key) {
            self.current_bytes -= old.data.len();
            self.current_bytes += block_size;
            *old = block;
            self.touch(&key);
            return (false, 0);
        }

        // Evict LRU entries from back until room
        let mut evicted = 0u32;
        while self.current_bytes + block_size > self.capacity_bytes {
            let evict_key = match self.lru_order.pop_back() {
                Some(k) => k,
                None => break,
            };
            if let Some(removed) = self.map.remove(&evict_key) {
                self.current_bytes -= removed.data.len();
                evicted += 1;
            }
        }

        self.current_bytes += block_size;
        self.map.insert(key, block.clone());
        self.lru_order.push_front(key);
        (true, evicted)
    }
}

fn shard_for(key: &BlockKey) -> usize {
    ((key.sst_id ^ key.offset) as usize) & (CACHE_SHARDS - 1)
}

pub struct BlockCache {
    shards: Box<[Mutex<CacheShard>]>,
    capacity_bytes: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    inserts: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity_bytes: usize) -> Self {
        let per_shard = capacity_bytes / CACHE_SHARDS;
        let shards: Vec<_> = (0..CACHE_SHARDS)
            .map(|_| Mutex::new(CacheShard::new(per_shard.max(1))))
            .collect();
        Self {
            shards: shards.into_boxed_slice(),
            capacity_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
        }
    }

    pub fn get(&self, key: &BlockKey) -> Option<CachedBlock> {
        let mut shard = self.shards[shard_for(key)].lock();
        match shard.get(key) {
            Some(b) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(b)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn insert(&self, key: BlockKey, block: CachedBlock) {
        let mut shard = self.shards[shard_for(&key)].lock();
        let (inserted, evicted) = shard.insert(key, block);
        if inserted {
            self.inserts.fetch_add(1, Ordering::Relaxed);
        }
        if evicted > 0 {
            self.evictions.fetch_add(evicted as u64, Ordering::Relaxed);
        }
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().map.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn current_bytes(&self) -> usize {
        self.shards.iter().map(|s| s.lock().current_bytes).sum()
    }

    pub fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    pub fn hit_rate(&self) -> f64 {
        let h = self.hits() as f64;
        let m = self.misses() as f64;
        if h + m == 0.0 {
            0.0
        } else {
            h / (h + m)
        }
    }

    pub fn snapshot(&self) -> BlockCacheSnapshot {
        BlockCacheSnapshot {
            capacity_bytes: self.capacity_bytes,
            current_bytes: self.current_bytes(),
            block_count: self.len(),
            hits: self.hits(),
            misses: self.misses(),
            evictions: self.evictions(),
            inserts: self.inserts.load(Ordering::Relaxed),
            hit_rate: self.hit_rate(),
        }
    }
}

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
        let key = BlockKey {
            sst_id: 1,
            offset: 0,
        };
        let block = CachedBlock {
            data: vec![1, 2, 3, 4],
        };

        cache.insert(key, block.clone());
        let result = cache.get(&key);
        assert!(result.is_some());
        assert_eq!(result.unwrap().data, vec![1, 2, 3, 4]);
        assert_eq!(cache.hits(), 1);
    }

    #[test]
    fn test_cache_miss() {
        let cache = BlockCache::new(4096);
        let key = BlockKey {
            sst_id: 99,
            offset: 0,
        };
        assert!(cache.get(&key).is_none());
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn test_cache_eviction() {
        // 1600 bytes / 16 shards = 100 per shard, fits ~1-2 blocks of 50 bytes
        let cache = BlockCache::new(1600);
        for i in 0..100u64 {
            let key = BlockKey {
                sst_id: i,
                offset: 0,
            };
            let block = CachedBlock {
                data: vec![0u8; 50],
            };
            cache.insert(key, block);
        }
        assert!(cache.evictions() > 0);
        assert!(cache.current_bytes() <= 1600);
    }

    #[test]
    fn test_cache_hit_rate() {
        let cache = BlockCache::new(4096);
        let key = BlockKey {
            sst_id: 1,
            offset: 0,
        };
        cache.insert(key, CachedBlock { data: vec![1] });

        cache.get(&key); // hit
        cache.get(&key); // hit
        cache.get(&BlockKey {
            sst_id: 99,
            offset: 0,
        }); // miss

        assert_eq!(cache.hits(), 2);
        assert_eq!(cache.misses(), 1);
        assert!((cache.hit_rate() - 0.6667).abs() < 0.01);
    }

    #[test]
    fn test_cache_oversized_block_rejected() {
        let cache = BlockCache::new(100);
        let key = BlockKey {
            sst_id: 1,
            offset: 0,
        };
        let block = CachedBlock {
            data: vec![0u8; 200],
        };
        cache.insert(key, block);
        assert!(cache.is_empty());
    }
}
