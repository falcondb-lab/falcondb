//! §F+§G — GC Recompression & Zstd Metrics

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::unified_data_plane::{Manifest, SegmentCodec, SegmentKind, SegmentStore};
use crate::zstd_dict::DictionaryStore;
use crate::zstd_segment::{read_zstd_cold_segment, write_zstd_cold_segment};
use crate::zstd_streaming::DecompressPool;

// ═══════════════════════════════════════════════════════════════════════════
// §F — GC Rewrite = Recompression Opportunity
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct RecompressRequest {
    pub source_segment_id: u64,
    pub target_codec: SegmentCodec,
    pub target_level: i32,
    pub new_dictionary_id: u64,
    pub reason: RecompressReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecompressReason {
    GcMerge,
    DictUpgrade,
    CodecChange,
    Manual,
}

impl fmt::Display for RecompressReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GcMerge => write!(f, "gc_merge"),
            Self::DictUpgrade => write!(f, "dict_upgrade"),
            Self::CodecChange => write!(f, "codec_change"),
            Self::Manual => write!(f, "manual"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecompressResult {
    pub old_segment_id: u64,
    pub new_segment_id: u64,
    pub old_codec: SegmentCodec,
    pub new_codec: SegmentCodec,
    pub old_bytes: u64,
    pub new_bytes: u64,
    pub bytes_saved: i64,
}

pub fn recompress_segment(
    store: &SegmentStore,
    request: &RecompressRequest,
    old_dict: Option<&[u8]>,
    new_dict: Option<&[u8]>,
    table_id: u64,
    shard_id: u64,
) -> Result<RecompressResult, String> {
    let rows = read_zstd_cold_segment(store, request.source_segment_id, old_dict)?;
    let old_size = store
        .segment_size(request.source_segment_id)
        .map_err(|e| format!("segment size: {e}"))?;
    let (new_seg_id, _meta) = write_zstd_cold_segment(
        store,
        table_id,
        shard_id,
        &rows,
        request.target_level,
        new_dict,
        request.new_dictionary_id,
    )
    .map_err(|e| format!("write recompressed: {e}"))?;
    let new_size = store
        .segment_size(new_seg_id)
        .map_err(|e| format!("new segment size: {e}"))?;
    Ok(RecompressResult {
        old_segment_id: request.source_segment_id,
        new_segment_id: new_seg_id,
        old_codec: SegmentCodec::Zstd,
        new_codec: request.target_codec,
        old_bytes: old_size,
        new_bytes: new_size,
        bytes_saved: old_size as i64 - new_size as i64,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// §G — Metrics & Admin Status
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Default)]
pub struct ZstdMetricsSnapshot {
    pub compress_total: u64,
    pub compress_bytes_in: u64,
    pub compress_bytes_out: u64,
    pub compress_ratio: f64,
    pub decompress_total: u64,
    pub decompress_bytes: u64,
    pub decompress_ns_total: u64,
    pub decompress_avg_us: f64,
    pub decompress_errors: u64,
    pub decompress_rejected: u64,
    pub cache_hit_rate: f64,
    pub cache_used_bytes: u64,
    pub cache_evictions: u64,
    pub dict_count: u64,
    pub dict_bytes_total: u64,
    pub dict_hit_rate: f64,
    pub dict_training_runs: u64,
    pub stream_bytes_saved_ratio: f64,
}

#[derive(Debug, Default)]
pub struct ZstdCompressMetrics {
    pub compress_total: AtomicU64,
    pub compress_bytes_in: AtomicU64,
    pub compress_bytes_out: AtomicU64,
}

impl ZstdCompressMetrics {
    pub fn record(&self, bytes_in: u64, bytes_out: u64) {
        self.compress_total.fetch_add(1, Ordering::Relaxed);
        self.compress_bytes_in
            .fetch_add(bytes_in, Ordering::Relaxed);
        self.compress_bytes_out
            .fetch_add(bytes_out, Ordering::Relaxed);
    }

    pub fn ratio(&self) -> f64 {
        let i = self.compress_bytes_in.load(Ordering::Relaxed) as f64;
        let o = self.compress_bytes_out.load(Ordering::Relaxed) as f64;
        if o > 0.0 {
            i / o
        } else {
            1.0
        }
    }
}

pub fn build_zstd_metrics(
    compress: &ZstdCompressMetrics,
    pool: &DecompressPool,
    dict_store: &DictionaryStore,
) -> ZstdMetricsSnapshot {
    let decompress_total = pool.metrics.decompress_total.load(Ordering::Relaxed);
    let decompress_ns = pool.metrics.decompress_ns_total.load(Ordering::Relaxed);
    let avg_us = if decompress_total > 0 {
        (decompress_ns as f64 / decompress_total as f64) / 1000.0
    } else {
        0.0
    };
    ZstdMetricsSnapshot {
        compress_total: compress.compress_total.load(Ordering::Relaxed),
        compress_bytes_in: compress.compress_bytes_in.load(Ordering::Relaxed),
        compress_bytes_out: compress.compress_bytes_out.load(Ordering::Relaxed),
        compress_ratio: compress.ratio(),
        decompress_total,
        decompress_bytes: pool.metrics.decompress_bytes.load(Ordering::Relaxed),
        decompress_ns_total: decompress_ns,
        decompress_avg_us: avg_us,
        decompress_errors: pool.metrics.decompress_errors.load(Ordering::Relaxed),
        decompress_rejected: pool.metrics.rejected_overload.load(Ordering::Relaxed),
        cache_hit_rate: pool.cache.metrics.hit_rate(),
        cache_used_bytes: pool.cache.used_bytes(),
        cache_evictions: pool.cache.metrics.evictions.load(Ordering::Relaxed),
        dict_count: dict_store.list().len() as u64,
        dict_bytes_total: dict_store
            .metrics
            .dictionary_bytes_total
            .load(Ordering::Relaxed),
        dict_hit_rate: dict_store.metrics.hit_rate(),
        dict_training_runs: dict_store.metrics.training_runs.load(Ordering::Relaxed),
        stream_bytes_saved_ratio: compress.ratio(),
    }
}

#[derive(Debug, Clone)]
pub struct SegmentCodecInfo {
    pub segment_id: u64,
    pub kind: SegmentKind,
    pub codec: SegmentCodec,
    pub size_bytes: u64,
    pub dictionary_id: u64,
    pub sealed: bool,
}

pub fn list_segments_by_codec(
    manifest: &Manifest,
    codec: Option<SegmentCodec>,
) -> Vec<SegmentCodecInfo> {
    manifest
        .segments
        .values()
        .filter(|e| codec.is_none_or(|c| e.codec == c))
        .map(|e| SegmentCodecInfo {
            segment_id: e.segment_id,
            kind: e.kind,
            codec: e.codec,
            size_bytes: e.size_bytes,
            dictionary_id: 0,
            sealed: e.sealed,
        })
        .collect()
}
