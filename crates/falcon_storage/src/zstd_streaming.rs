//! §D+§E — Streaming Codec Negotiation & Decompression Isolation

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::unified_data_plane::SegmentCodec;
use crate::zstd_segment::{decompress_block, ZstdBlock};

// ═══════════════════════════════════════════════════════════════════════════
// §D — Streaming Codec Negotiation
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingCodecCaps {
    pub supported: Vec<SegmentCodec>,
    pub preferred: SegmentCodec,
    pub estimated_bandwidth_bps: u64,
}

impl StreamingCodecCaps {
    pub fn high_bandwidth() -> Self {
        Self {
            supported: vec![SegmentCodec::None, SegmentCodec::Lz4, SegmentCodec::Zstd],
            preferred: SegmentCodec::Lz4,
            estimated_bandwidth_bps: 10 * 1024 * 1024 * 1024,
        }
    }

    pub fn low_bandwidth() -> Self {
        Self {
            supported: vec![SegmentCodec::Lz4, SegmentCodec::Zstd],
            preferred: SegmentCodec::Zstd,
            estimated_bandwidth_bps: 100 * 1024 * 1024,
        }
    }
}

pub fn negotiate_streaming_codec(
    sender: &StreamingCodecCaps,
    receiver: &StreamingCodecCaps,
) -> SegmentCodec {
    let common: Vec<SegmentCodec> = sender
        .supported
        .iter()
        .filter(|c| receiver.supported.contains(c))
        .copied()
        .collect();
    if common.is_empty() {
        return SegmentCodec::None;
    }
    if (sender.preferred == SegmentCodec::Zstd || receiver.preferred == SegmentCodec::Zstd)
        && common.contains(&SegmentCodec::Zstd)
    {
        return SegmentCodec::Zstd;
    }
    if common.contains(&sender.preferred) {
        return sender.preferred;
    }
    common[0]
}

pub fn compress_streaming_chunk(
    data: &[u8],
    codec: SegmentCodec,
    level: i32,
) -> Result<Vec<u8>, String> {
    match codec {
        SegmentCodec::None => Ok(data.to_vec()),
        SegmentCodec::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
        SegmentCodec::Zstd => {
            zstd::bulk::compress(data, level).map_err(|e| format!("zstd stream compress: {e}"))
        }
    }
}

pub fn decompress_streaming_chunk(
    data: &[u8],
    codec: SegmentCodec,
    max_size: usize,
) -> Result<Vec<u8>, String> {
    match codec {
        SegmentCodec::None => Ok(data.to_vec()),
        SegmentCodec::Lz4 => lz4_flex::decompress_size_prepended(data)
            .map_err(|e| format!("lz4 stream decompress: {e}")),
        SegmentCodec::Zstd => zstd::bulk::decompress(data, max_size)
            .map_err(|e| format!("zstd stream decompress: {e}")),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §E — Decompression Isolation: Thread Pool + Cache
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecompressCacheKey {
    pub segment_id: u64,
    pub block_index: u32,
}

pub struct DecompressCache {
    capacity_bytes: u64,
    used_bytes: AtomicU64,
    entries: Mutex<HashMap<DecompressCacheKey, Vec<u8>>>,
    order: Mutex<VecDeque<DecompressCacheKey>>,
    pub metrics: DecompressCacheMetrics,
}

#[derive(Debug, Default)]
pub struct DecompressCacheMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub bytes_cached: AtomicU64,
}

impl DecompressCacheMetrics {
    pub fn hit_rate(&self) -> f64 {
        let h = self.hits.load(Ordering::Relaxed) as f64;
        let m = self.misses.load(Ordering::Relaxed) as f64;
        if h + m == 0.0 {
            return 0.0;
        }
        h / (h + m)
    }
}

impl DecompressCache {
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            used_bytes: AtomicU64::new(0),
            entries: Mutex::new(HashMap::new()),
            order: Mutex::new(VecDeque::new()),
            metrics: DecompressCacheMetrics::default(),
        }
    }

    pub fn get(&self, key: &DecompressCacheKey) -> Option<Vec<u8>> {
        let entries = self.entries.lock();
        if let Some(data) = entries.get(key) {
            self.metrics.hits.fetch_add(1, Ordering::Relaxed);
            Some(data.clone())
        } else {
            self.metrics.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn insert(&self, key: DecompressCacheKey, data: Vec<u8>) {
        let data_len = data.len() as u64;
        while self.used_bytes.load(Ordering::Relaxed) + data_len > self.capacity_bytes {
            let evict_key = {
                let mut order = self.order.lock();
                order.pop_front()
            };
            if let Some(ek) = evict_key {
                let mut entries = self.entries.lock();
                if let Some(evicted) = entries.remove(&ek) {
                    self.used_bytes
                        .fetch_sub(evicted.len() as u64, Ordering::Relaxed);
                    self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                break;
            }
        }
        let mut entries = self.entries.lock();
        if let std::collections::hash_map::Entry::Vacant(e) = entries.entry(key) {
            e.insert(data);
            self.used_bytes.fetch_add(data_len, Ordering::Relaxed);
            self.metrics
                .bytes_cached
                .fetch_add(data_len, Ordering::Relaxed);
            self.order.lock().push_back(key);
        }
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct DecompressPool {
    pub max_concurrent: u32,
    inflight: AtomicU64,
    pub cache: DecompressCache,
    pub metrics: DecompressPoolMetrics,
}

#[derive(Debug, Default)]
pub struct DecompressPoolMetrics {
    pub decompress_total: AtomicU64,
    pub decompress_bytes: AtomicU64,
    pub decompress_ns_total: AtomicU64,
    pub decompress_errors: AtomicU64,
    pub rejected_overload: AtomicU64,
}

impl DecompressPool {
    pub fn new(max_concurrent: u32, cache_capacity_bytes: u64) -> Self {
        Self {
            max_concurrent,
            inflight: AtomicU64::new(0),
            cache: DecompressCache::new(cache_capacity_bytes),
            metrics: DecompressPoolMetrics::default(),
        }
    }

    pub fn decompress(
        &self,
        segment_id: u64,
        block_index: u32,
        block: &ZstdBlock,
        dict: Option<&[u8]>,
    ) -> Result<Vec<u8>, String> {
        let cache_key = DecompressCacheKey {
            segment_id,
            block_index,
        };
        if let Some(cached) = self.cache.get(&cache_key) {
            return Ok(cached);
        }
        let current = self.inflight.fetch_add(1, Ordering::Relaxed);
        if current >= u64::from(self.max_concurrent) {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            self.metrics
                .rejected_overload
                .fetch_add(1, Ordering::Relaxed);
            return Err("decompress pool overloaded".to_owned());
        }
        let start = std::time::Instant::now();
        let result = decompress_block(block, dict);
        let elapsed_ns = start.elapsed().as_nanos() as u64;
        self.inflight.fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .decompress_total
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .decompress_ns_total
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        match result {
            Ok(data) => {
                self.metrics
                    .decompress_bytes
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                self.cache.insert(cache_key, data.clone());
                Ok(data)
            }
            Err(e) => {
                self.metrics
                    .decompress_errors
                    .fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    pub fn inflight(&self) -> u64 {
        self.inflight.load(Ordering::Relaxed)
    }
}
