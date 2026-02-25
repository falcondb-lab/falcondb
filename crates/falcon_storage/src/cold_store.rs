//! Cold Store — compressed segment-based storage for old MVCC version payloads.
//!
//! # Architecture
//! - **Hot Store**: current `MemTable` / `VersionChain` — uncompressed, fast path
//! - **Cold Store**: append-only segments with block-level LZ4 compression
//!
//! MVCC headers (`created_by`, `commit_ts`, `prev`) remain in hot memory.
//! Only the row payload (`Option<OwnedRow>`) is migrated to cold segments,
//! replaced by a `ColdHandle { segment_id, offset, len }` reference.
//!
//! # Compression
//! - Default codec: LZ4 (via `lz4_flex`)
//! - Fallback: None (uncompressed)
//! - Global toggle: `compression.enabled`
//! - Per-block compression with inline codec tag
//!
//! # Segments
//! - Fixed max size (configurable, default 64 MB)
//! - Append-only, never mutated after write
//! - Each block: `[codec:u8][original_len:u32][compressed_len:u32][data...]`

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;

// ═══════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// Compression codec for cold blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionCodec {
    /// No compression — raw bytes.
    None = 0,
    /// LZ4 block compression (lz4_flex).
    Lz4 = 1,
}

impl CompressionCodec {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::None),
            1 => Some(Self::Lz4),
            _ => None,
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
        }
    }
}

impl std::fmt::Display for CompressionCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Cold store configuration.
#[derive(Debug, Clone)]
pub struct ColdStoreConfig {
    /// Whether cold storage is enabled.
    pub enabled: bool,
    /// Maximum segment size in bytes (default: 64 MB).
    pub max_segment_size: u64,
    /// Compression codec (default: LZ4).
    pub codec: CompressionCodec,
    /// Whether compression is enabled globally.
    pub compression_enabled: bool,
    /// LRU block cache capacity in bytes (default: 16 MB).
    pub block_cache_capacity: u64,
    /// Directory for cold segment files (None = in-memory only).
    pub segment_dir: Option<PathBuf>,
}

impl Default for ColdStoreConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_segment_size: 64 * 1024 * 1024, // 64 MB
            codec: CompressionCodec::Lz4,
            compression_enabled: true,
            block_cache_capacity: 16 * 1024 * 1024, // 16 MB
            segment_dir: None,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Cold Handle — reference from hot Version to cold payload
// ═══════════════════════════════════════════════════════════════════════════

/// A handle pointing to a row payload stored in the cold store.
/// Replaces `Option<OwnedRow>` in cold-migrated versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColdHandle {
    /// Segment ID containing this block.
    pub segment_id: u64,
    /// Byte offset within the segment.
    pub offset: u64,
    /// Length of the compressed block (including header).
    pub len: u32,
}

impl ColdHandle {
    /// Estimated memory footprint of this handle (constant, small).
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

// ═══════════════════════════════════════════════════════════════════════════
// Block format: [codec:u8][original_len:u32][compressed_len:u32][data...]
// ═══════════════════════════════════════════════════════════════════════════

const BLOCK_HEADER_SIZE: usize = 1 + 4 + 4; // codec + original_len + compressed_len

fn encode_block(data: &[u8], codec: CompressionCodec, compression_enabled: bool) -> Vec<u8> {
    let actual_codec = if compression_enabled { codec } else { CompressionCodec::None };
    let original_len = data.len() as u32;

    let compressed = match actual_codec {
        CompressionCodec::None => data.to_vec(),
        CompressionCodec::Lz4 => lz4_flex::compress_prepend_size(data),
    };

    let compressed_len = compressed.len() as u32;
    let mut block = Vec::with_capacity(BLOCK_HEADER_SIZE + compressed.len());
    block.push(actual_codec as u8);
    block.extend_from_slice(&original_len.to_le_bytes());
    block.extend_from_slice(&compressed_len.to_le_bytes());
    block.extend_from_slice(&compressed);
    block
}

fn decode_block(block: &[u8]) -> Result<Vec<u8>, StorageError> {
    if block.len() < BLOCK_HEADER_SIZE {
        return Err(StorageError::ColdStore("block too small".into()));
    }

    let codec = CompressionCodec::from_u8(block[0])
        .ok_or_else(|| StorageError::ColdStore(format!("unknown codec: {}", block[0])))?;
    let _original_len = u32::from_le_bytes([block[1], block[2], block[3], block[4]]);
    let compressed_len = u32::from_le_bytes([block[5], block[6], block[7], block[8]]) as usize;

    let compressed_data = &block[BLOCK_HEADER_SIZE..];
    if compressed_data.len() < compressed_len {
        return Err(StorageError::ColdStore("truncated block data".into()));
    }

    match codec {
        CompressionCodec::None => Ok(compressed_data[..compressed_len].to_vec()),
        CompressionCodec::Lz4 => {
            lz4_flex::decompress_size_prepended(&compressed_data[..compressed_len])
                .map_err(|e| StorageError::ColdStore(format!("LZ4 decompress error: {e}")))
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Row serialization (OwnedRow ↔ bytes)
// ═══════════════════════════════════════════════════════════════════════════

fn serialize_row(row: &OwnedRow) -> Vec<u8> {
    bincode::serialize(row).unwrap_or_default()
}

fn deserialize_row(data: &[u8]) -> Result<OwnedRow, StorageError> {
    bincode::deserialize(data)
        .map_err(|e| StorageError::ColdStore(format!("row deserialize error: {e}")))
}

// ═══════════════════════════════════════════════════════════════════════════
// In-memory Segment
// ═══════════════════════════════════════════════════════════════════════════

struct Segment {
    id: u64,
    data: Vec<u8>,
    max_size: u64,
}

impl Segment {
    fn new(id: u64, max_size: u64) -> Self {
        Self {
            id,
            data: Vec::with_capacity(std::cmp::min(max_size as usize, 4 * 1024 * 1024)),
            max_size,
        }
    }

    const fn remaining(&self) -> u64 {
        self.max_size.saturating_sub(self.data.len() as u64)
    }

    fn append(&mut self, block: &[u8]) -> (u64, u32) {
        let offset = self.data.len() as u64;
        self.data.extend_from_slice(block);
        (offset, block.len() as u32)
    }

    fn read(&self, offset: u64, len: u32) -> Result<&[u8], StorageError> {
        let start = offset as usize;
        let end = start + len as usize;
        if end > self.data.len() {
            return Err(StorageError::ColdStore(format!(
                "read out of bounds: segment={}, offset={}, len={}, segment_size={}",
                self.id, offset, len, self.data.len()
            )));
        }
        Ok(&self.data[start..end])
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// LRU Block Cache
// ═══════════════════════════════════════════════════════════════════════════

struct CacheEntry {
    data: Arc<Vec<u8>>,
    size: usize,
}

/// Simple LRU-by-insertion-order block cache with byte capacity limit.
pub struct BlockCache {
    entries: parking_lot::Mutex<BlockCacheInner>,
    capacity: u64,
    hits: AtomicU64,
    misses: AtomicU64,
}

struct BlockCacheInner {
    map: HashMap<ColdHandle, CacheEntry>,
    insertion_order: Vec<ColdHandle>,
    current_bytes: u64,
}

impl BlockCache {
    pub fn new(capacity: u64) -> Self {
        Self {
            entries: parking_lot::Mutex::new(BlockCacheInner {
                map: HashMap::new(),
                insertion_order: Vec::new(),
                current_bytes: 0,
            }),
            capacity,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, handle: &ColdHandle) -> Option<Arc<Vec<u8>>> {
        let inner = self.entries.lock();
        if let Some(entry) = inner.map.get(handle) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(Arc::clone(&entry.data))
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn insert(&self, handle: ColdHandle, data: Vec<u8>) {
        let size = data.len();
        let arc_data = Arc::new(data);
        let mut inner = self.entries.lock();

        // Evict oldest entries until we have space
        while inner.current_bytes + size as u64 > self.capacity && !inner.insertion_order.is_empty()
        {
            let oldest = inner.insertion_order.remove(0);
            if let Some(evicted) = inner.map.remove(&oldest) {
                inner.current_bytes -= evicted.size as u64;
            }
        }

        inner.current_bytes += size as u64;
        inner.insertion_order.push(handle);
        inner.map.insert(
            handle,
            CacheEntry {
                data: arc_data,
                size,
            },
        );
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
    pub fn current_bytes(&self) -> u64 {
        self.entries.lock().current_bytes
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Cold Store Metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Atomic metrics for observability.
#[derive(Debug)]
pub struct ColdStoreMetrics {
    /// Total bytes in cold store (compressed).
    pub cold_bytes: AtomicU64,
    /// Total bytes before compression (original).
    pub cold_original_bytes: AtomicU64,
    /// Number of cold segments.
    pub cold_segments_total: AtomicU64,
    /// Total reads from cold store.
    pub cold_read_total: AtomicU64,
    /// Total decompress operations.
    pub cold_decompress_total: AtomicU64,
    /// Cumulative decompress latency in microseconds.
    pub cold_decompress_latency_us: AtomicU64,
    /// Peak single decompress latency in microseconds.
    pub cold_decompress_peak_us: AtomicU64,
    /// Total rows migrated to cold.
    pub cold_migrate_total: AtomicU64,
}

impl ColdStoreMetrics {
    pub const fn new() -> Self {
        Self {
            cold_bytes: AtomicU64::new(0),
            cold_original_bytes: AtomicU64::new(0),
            cold_segments_total: AtomicU64::new(0),
            cold_read_total: AtomicU64::new(0),
            cold_decompress_total: AtomicU64::new(0),
            cold_decompress_latency_us: AtomicU64::new(0),
            cold_decompress_peak_us: AtomicU64::new(0),
            cold_migrate_total: AtomicU64::new(0),
        }
    }

    /// Compression ratio: original / compressed. > 1.0 means compression is effective.
    pub fn compression_ratio(&self) -> f64 {
        let orig = self.cold_original_bytes.load(Ordering::Relaxed) as f64;
        let comp = self.cold_bytes.load(Ordering::Relaxed) as f64;
        if comp > 0.0 { orig / comp } else { 1.0 }
    }

    /// Average decompress latency in microseconds.
    pub fn avg_decompress_us(&self) -> f64 {
        let total = self.cold_decompress_latency_us.load(Ordering::Relaxed) as f64;
        let count = self.cold_decompress_total.load(Ordering::Relaxed) as f64;
        if count > 0.0 { total / count } else { 0.0 }
    }

    pub fn snapshot(&self) -> ColdStoreMetricsSnapshot {
        ColdStoreMetricsSnapshot {
            cold_bytes: self.cold_bytes.load(Ordering::Relaxed),
            cold_original_bytes: self.cold_original_bytes.load(Ordering::Relaxed),
            cold_segments_total: self.cold_segments_total.load(Ordering::Relaxed),
            cold_read_total: self.cold_read_total.load(Ordering::Relaxed),
            cold_decompress_total: self.cold_decompress_total.load(Ordering::Relaxed),
            cold_decompress_latency_us: self.cold_decompress_latency_us.load(Ordering::Relaxed),
            cold_decompress_peak_us: self.cold_decompress_peak_us.load(Ordering::Relaxed),
            cold_migrate_total: self.cold_migrate_total.load(Ordering::Relaxed),
            compression_ratio: self.compression_ratio(),
            avg_decompress_us: self.avg_decompress_us(),
        }
    }
}

impl Default for ColdStoreMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of cold store metrics.
#[derive(Debug, Clone, Default)]
pub struct ColdStoreMetricsSnapshot {
    pub cold_bytes: u64,
    pub cold_original_bytes: u64,
    pub cold_segments_total: u64,
    pub cold_read_total: u64,
    pub cold_decompress_total: u64,
    pub cold_decompress_latency_us: u64,
    pub cold_decompress_peak_us: u64,
    pub cold_migrate_total: u64,
    pub compression_ratio: f64,
    pub avg_decompress_us: f64,
}

// ═══════════════════════════════════════════════════════════════════════════
// Cold Store
// ═══════════════════════════════════════════════════════════════════════════

/// The Cold Store manages compressed segments containing old MVCC row payloads.
///
/// Thread-safe: all mutation is behind `RwLock`. Reads use the block cache
/// and only acquire the segment lock on cache miss.
pub struct ColdStore {
    config: ColdStoreConfig,
    /// All segments (closed + active).
    segments: RwLock<Vec<Segment>>,
    /// ID for the next segment to create.
    next_segment_id: AtomicU64,
    /// Block cache for decompressed data.
    cache: BlockCache,
    /// Metrics.
    pub metrics: ColdStoreMetrics,
}

impl ColdStore {
    /// Create a new cold store with the given config.
    pub fn new(config: ColdStoreConfig) -> Self {
        let cache_capacity = config.block_cache_capacity;
        let seg = Segment::new(0, config.max_segment_size);
        Self {
            config,
            segments: RwLock::new(vec![seg]),
            next_segment_id: AtomicU64::new(1),
            cache: BlockCache::new(cache_capacity),
            metrics: ColdStoreMetrics::new(),
        }
    }

    /// Create an in-memory cold store with default config.
    pub fn new_in_memory() -> Self {
        Self::new(ColdStoreConfig::default())
    }

    /// Store a row payload in the cold store. Returns a ColdHandle.
    pub fn store_row(&self, row: &OwnedRow) -> Result<ColdHandle, StorageError> {
        let raw_bytes = serialize_row(row);
        let original_len = raw_bytes.len() as u64;
        let block = encode_block(&raw_bytes, self.config.codec, self.config.compression_enabled);
        let block_len = block.len() as u64;

        let handle = {
            let mut segments = self.segments.write().map_err(|_| {
                StorageError::ColdStore("segment lock poisoned".into())
            })?;

            // Check if active segment has space
            let active_idx = segments.len() - 1;
            if segments[active_idx].remaining() < block_len {
                // Rotate: create new segment
                let new_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
                segments.push(Segment::new(new_id, self.config.max_segment_size));
                self.metrics.cold_segments_total.store(
                    segments.len() as u64,
                    Ordering::Relaxed,
                );
            }

            let active_idx = segments.len() - 1;
            let (offset, len) = segments[active_idx].append(&block);
            ColdHandle {
                segment_id: segments[active_idx].id,
                offset,
                len,
            }
        };

        // Update metrics
        self.metrics.cold_bytes.fetch_add(block_len, Ordering::Relaxed);
        self.metrics.cold_original_bytes.fetch_add(original_len, Ordering::Relaxed);
        self.metrics.cold_migrate_total.fetch_add(1, Ordering::Relaxed);

        Ok(handle)
    }

    /// Read a row payload from the cold store by handle.
    /// Uses the block cache; on miss, reads from segment and decompresses.
    pub fn read_row(&self, handle: &ColdHandle) -> Result<OwnedRow, StorageError> {
        self.metrics.cold_read_total.fetch_add(1, Ordering::Relaxed);

        // Check cache first
        if let Some(cached) = self.cache.get(handle) {
            return deserialize_row(&cached);
        }

        // Cache miss: read from segment and decompress
        let start = Instant::now();

        let raw_block = {
            let segments = self.segments.read().map_err(|_| {
                StorageError::ColdStore("segment lock poisoned".into())
            })?;
            let segment = segments
                .iter()
                .find(|s| s.id == handle.segment_id)
                .ok_or_else(|| {
                    StorageError::ColdStore(format!(
                        "segment {} not found",
                        handle.segment_id
                    ))
                })?;
            segment.read(handle.offset, handle.len)?.to_vec()
        };

        let decompressed = decode_block(&raw_block)?;

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.metrics.cold_decompress_total.fetch_add(1, Ordering::Relaxed);
        self.metrics.cold_decompress_latency_us.fetch_add(elapsed_us, Ordering::Relaxed);
        let _ = self.metrics.cold_decompress_peak_us.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |cur| if elapsed_us > cur { Some(elapsed_us) } else { None },
        );

        // Populate cache
        self.cache.insert(*handle, decompressed.clone());

        deserialize_row(&decompressed)
    }

    /// Get the current config.
    pub const fn config(&self) -> &ColdStoreConfig {
        &self.config
    }

    /// Get cache hit/miss stats.
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache.hits() as f64;
        let misses = self.cache.misses() as f64;
        let total = hits + misses;
        if total > 0.0 { hits / total } else { 0.0 }
    }

    /// Get current cache usage in bytes.
    pub fn cache_bytes(&self) -> u64 {
        self.cache.current_bytes()
    }

    /// Total number of segments.
    pub fn segment_count(&self) -> u64 {
        self.segments
            .read()
            .map(|s| s.len() as u64)
            .unwrap_or(0)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Compactor — background cold migration
// ═══════════════════════════════════════════════════════════════════════════

/// Compactor configuration.
#[derive(Debug, Clone)]
pub struct CompactorConfig {
    /// Minimum version age (in timestamp units) before migration.
    pub min_version_age: u64,
    /// Maximum versions to migrate per sweep.
    pub batch_size: usize,
    /// Sweep interval in milliseconds.
    pub interval_ms: u64,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            min_version_age: 300, // ~5 min in timestamp units
            batch_size: 1000,
            interval_ms: 5000, // 5 seconds
        }
    }
}

/// Result of a single compaction sweep.
#[derive(Debug, Clone, Default)]
pub struct CompactionResult {
    /// Number of version chains inspected.
    pub chains_inspected: u64,
    /// Number of versions migrated to cold.
    pub versions_migrated: u64,
    /// Bytes freed from hot memory.
    pub hot_bytes_freed: u64,
    /// Bytes written to cold store (compressed).
    pub cold_bytes_written: u64,
    /// Duration of the sweep in microseconds.
    pub sweep_duration_us: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// String Intern Pool
// ═══════════════════════════════════════════════════════════════════════════

/// Interned string ID — 4 bytes replacing a heap-allocated String.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InternId(pub u32);

/// Thread-safe string intern pool.
/// Deduplicates identical strings, returning a compact `InternId`.
pub struct StringInternPool {
    /// String → InternId mapping.
    map: parking_lot::RwLock<HashMap<String, InternId>>,
    /// InternId → String reverse mapping.
    strings: parking_lot::RwLock<Vec<String>>,
    /// Hit counter (string already interned).
    hits: AtomicU64,
    /// Miss counter (new string interned).
    misses: AtomicU64,
}

impl StringInternPool {
    pub fn new() -> Self {
        Self {
            map: parking_lot::RwLock::new(HashMap::new()),
            strings: parking_lot::RwLock::new(Vec::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Intern a string. Returns its InternId.
    pub fn intern(&self, s: &str) -> InternId {
        // Fast path: read lock
        {
            let map = self.map.read();
            if let Some(&id) = map.get(s) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return id;
            }
        }
        // Slow path: write lock
        let mut map = self.map.write();
        // Double-check after acquiring write lock
        if let Some(&id) = map.get(s) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return id;
        }
        let mut strings = self.strings.write();
        let id = InternId(strings.len() as u32);
        strings.push(s.to_owned());
        map.insert(s.to_owned(), id);
        self.misses.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Resolve an InternId back to a string.
    pub fn resolve(&self, id: InternId) -> Option<String> {
        let strings = self.strings.read();
        strings.get(id.0 as usize).cloned()
    }

    /// Hit rate: fraction of intern() calls that found an existing entry.
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed) as f64;
        let misses = self.misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total > 0.0 { hits / total } else { 0.0 }
    }

    /// Number of unique strings in the pool.
    pub fn len(&self) -> usize {
        self.strings.read().len()
    }

    /// Whether the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total hits.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Total misses.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

impl Default for StringInternPool {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::Datum;

    fn sample_row(i: u64) -> OwnedRow {
        OwnedRow {
            values: vec![
                Datum::Int64(i as i64),
                Datum::Text(format!("row_data_{}", i)),
                Datum::Float64(i as f64 * 1.5),
            ],
        }
    }

    // ── ColdStore basic ──

    #[test]
    fn test_cold_store_store_and_read() {
        let store = ColdStore::new_in_memory();
        let row = sample_row(42);

        let handle = store.store_row(&row).unwrap();
        let recovered = store.read_row(&handle).unwrap();

        assert_eq!(row.values.len(), recovered.values.len());
        assert_eq!(format!("{:?}", row), format!("{:?}", recovered));
    }

    #[test]
    fn test_cold_store_multiple_rows() {
        let store = ColdStore::new_in_memory();
        let mut handles = Vec::new();

        for i in 0..100 {
            let row = sample_row(i);
            let handle = store.store_row(&row).unwrap();
            handles.push((i, handle));
        }

        for (i, handle) in &handles {
            let row = store.read_row(handle).unwrap();
            match &row.values[0] {
                Datum::Int64(v) => assert_eq!(*v, *i as i64),
                other => panic!("expected Int64, got {:?}", other),
            }
        }

        assert!(store.metrics.cold_migrate_total.load(Ordering::Relaxed) >= 100);
        assert!(store.metrics.cold_bytes.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_cold_store_compression_ratio() {
        let store = ColdStore::new(ColdStoreConfig {
            compression_enabled: true,
            codec: CompressionCodec::Lz4,
            ..Default::default()
        });

        // Store rows with repetitive data (compresses well)
        for i in 0..50 {
            let row = OwnedRow {
                values: vec![
                    Datum::Int64(i),
                    Datum::Text("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string()),
                ],
            };
            store.store_row(&row).unwrap();
        }

        let ratio = store.metrics.compression_ratio();
        println!("compression_ratio = {:.2}", ratio);
        assert!(ratio > 1.0, "LZ4 should compress repetitive data: ratio={}", ratio);
    }

    #[test]
    fn test_cold_store_no_compression() {
        let store = ColdStore::new(ColdStoreConfig {
            compression_enabled: false,
            ..Default::default()
        });

        let row = sample_row(1);
        let handle = store.store_row(&row).unwrap();
        let recovered = store.read_row(&handle).unwrap();

        assert_eq!(format!("{:?}", row), format!("{:?}", recovered));

        let ratio = store.metrics.compression_ratio();
        // Without compression, ratio = original / (original + block_header).
        // For small rows the 9-byte header overhead makes ratio < 1.0.
        assert!(ratio > 0.5 && ratio < 1.1, "no-compression ratio should be < 1.1: {}", ratio);
    }

    #[test]
    fn test_cold_store_segment_rotation() {
        let store = ColdStore::new(ColdStoreConfig {
            max_segment_size: 512, // Very small — forces rotation
            ..Default::default()
        });

        for i in 0..50 {
            store.store_row(&sample_row(i)).unwrap();
        }

        let seg_count = store.segment_count();
        assert!(seg_count > 1, "should have multiple segments: got {}", seg_count);
    }

    #[test]
    fn test_cold_store_cache_hit() {
        let store = ColdStore::new_in_memory();
        let row = sample_row(99);
        let handle = store.store_row(&row).unwrap();

        // First read: cache miss
        let _ = store.read_row(&handle).unwrap();
        assert_eq!(store.cache.misses(), 1);

        // Second read: cache hit
        let _ = store.read_row(&handle).unwrap();
        assert_eq!(store.cache.hits(), 1);

        assert!(store.cache_hit_rate() > 0.0);
    }

    #[test]
    fn test_cold_store_metrics_snapshot() {
        let store = ColdStore::new_in_memory();
        for i in 0..10 {
            store.store_row(&sample_row(i)).unwrap();
        }

        let snap = store.metrics.snapshot();
        assert_eq!(snap.cold_migrate_total, 10);
        assert!(snap.cold_bytes > 0);
        assert!(snap.cold_original_bytes > 0);
        assert!(snap.compression_ratio > 0.0);
    }

    // ── Block encode/decode ──

    #[test]
    fn test_block_encode_decode_lz4() {
        let data = b"hello world repeated repeated repeated repeated repeated";
        let block = encode_block(data, CompressionCodec::Lz4, true);
        let decoded = decode_block(&block).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_block_encode_decode_none() {
        let data = b"some uncompressed data";
        let block = encode_block(data, CompressionCodec::None, true);
        let decoded = decode_block(&block).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_block_compression_disabled() {
        let data = b"should not compress even with LZ4 codec";
        let block = encode_block(data, CompressionCodec::Lz4, false);
        // With compression disabled, codec byte should be 0 (None)
        assert_eq!(block[0], CompressionCodec::None as u8);
        let decoded = decode_block(&block).unwrap();
        assert_eq!(decoded, data);
    }

    // ── String Intern Pool ──

    #[test]
    fn test_intern_pool_basic() {
        let pool = StringInternPool::new();

        let id1 = pool.intern("hello");
        let id2 = pool.intern("world");
        let id3 = pool.intern("hello"); // duplicate

        assert_eq!(id1, id3); // same string → same ID
        assert_ne!(id1, id2);

        assert_eq!(pool.resolve(id1), Some("hello".to_string()));
        assert_eq!(pool.resolve(id2), Some("world".to_string()));
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn test_intern_pool_hit_rate() {
        let pool = StringInternPool::new();

        // 10 unique strings
        for i in 0..10 {
            pool.intern(&format!("str_{}", i));
        }
        // Re-intern all 10 (hits)
        for i in 0..10 {
            pool.intern(&format!("str_{}", i));
        }

        assert_eq!(pool.hits(), 10);
        assert_eq!(pool.misses(), 10);
        assert!((pool.hit_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_intern_pool_concurrent() {
        let pool = Arc::new(StringInternPool::new());
        let mut handles = Vec::new();

        for t in 0..4 {
            let p = Arc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    p.intern(&format!("key_{}_{}", t % 2, i)); // 50% overlap between threads
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Should have 200 unique strings (2 thread groups × 100)
        assert_eq!(pool.len(), 200);
        assert!(pool.hits() > 0); // Overlapping threads should hit
    }

    // ── Compactor config ──

    #[test]
    fn test_compactor_config_default() {
        let cfg = CompactorConfig::default();
        assert_eq!(cfg.min_version_age, 300);
        assert_eq!(cfg.batch_size, 1000);
        assert_eq!(cfg.interval_ms, 5000);
    }

    // ── ColdHandle ──

    #[test]
    fn test_cold_handle_size() {
        assert!(ColdHandle::SIZE <= 24, "ColdHandle should be compact: {} bytes", ColdHandle::SIZE);
    }

    // ── CompressionCodec ──

    #[test]
    fn test_codec_roundtrip() {
        assert_eq!(CompressionCodec::from_u8(0), Some(CompressionCodec::None));
        assert_eq!(CompressionCodec::from_u8(1), Some(CompressionCodec::Lz4));
        assert_eq!(CompressionCodec::from_u8(99), None);
    }

    #[test]
    fn test_codec_display() {
        assert_eq!(CompressionCodec::Lz4.to_string(), "lz4");
        assert_eq!(CompressionCodec::None.to_string(), "none");
    }
}
