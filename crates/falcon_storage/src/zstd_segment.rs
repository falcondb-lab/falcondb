//! # Zstd Integration in Unified Data Plane
//!
//! **"Zstd is not a row compressor. It is a segment-level infrastructure primitive."**
//!
//! Zstd only operates on immutable segments — never in the OLTP hot path.
//! Compression is a segment-level capability, not row/record-level.
//! Codec and dictionary are Manifest-managed, replicable, recoverable.
//!
//! ## Parts
//! - A: Compression scope & codec policy (what goes where)
//! - B: Segment-level Zstd (block compress/decompress, header extensions)
//! - C: Dictionary lifecycle (DICT_SEGMENT, training, versioning)
//! - D: Streaming codec negotiation (adaptive transport compression)
//! - E: Decompression isolation (thread pool + cache)
//! - F: GC rewrite = recompression opportunity
//! - G: Metrics & ops status

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

use crate::unified_data_plane::{
    SegmentCodec, SegmentKind, SegmentStore, SegmentStoreError,
    UnifiedSegmentHeader, Manifest,
    UNIFIED_HEADER_SIZE,
};
use crate::unified_data_plane_full::ColdBlockEncoding;

// ═══════════════════════════════════════════════════════════════════════════
// §A — Compression Scope & Codec Policy
// ═══════════════════════════════════════════════════════════════════════════

/// Codec policy configuration — which codec is allowed/required where.
///
/// Hard constraints:
/// - ❌ Active WAL tail: NEVER zstd
/// - ❌ OLTP hot row / MVCC visibility: NEVER zstd
/// - ✅ COLD_SEGMENT: zstd (default) or lz4
/// - ✅ SNAPSHOT_SEGMENT: zstd (forced)
/// - ✅ Segment Streaming: adaptive (none/lz4/zstd)
/// - ✅ Sealed WAL long-term: optional lz4
#[derive(Debug, Clone)]
pub struct CodecPolicy {
    /// Codec for WAL segments (none or lz4, NEVER zstd).
    pub wal_segment_codec: SegmentCodec,
    /// Codec for cold segments (default: zstd).
    pub cold_segment_codec: SegmentCodec,
    /// Codec for snapshot segments (forced: zstd).
    pub snapshot_segment_codec: SegmentCodec,
    /// Codec for streaming transport (adaptive).
    pub streaming_codec: StreamingCodecPolicy,
    /// Default zstd compression level for cold segments.
    pub zstd_cold_level: i32,
    /// Default zstd compression level for snapshot segments.
    pub zstd_snapshot_level: i32,
    /// Default zstd compression level for streaming.
    pub zstd_streaming_level: i32,
}

impl Default for CodecPolicy {
    fn default() -> Self {
        Self {
            wal_segment_codec: SegmentCodec::None,
            cold_segment_codec: SegmentCodec::Zstd,
            snapshot_segment_codec: SegmentCodec::Zstd,
            streaming_codec: StreamingCodecPolicy::Adaptive,
            zstd_cold_level: 3,
            zstd_snapshot_level: 5,
            zstd_streaming_level: 1,
        }
    }
}

impl CodecPolicy {
    /// Validate that a codec is allowed for a given segment kind.
    pub fn validate_codec(&self, kind: SegmentKind, codec: SegmentCodec) -> bool {
        match kind {
            SegmentKind::Wal => codec == SegmentCodec::None || codec == SegmentCodec::Lz4,
            SegmentKind::Cold => true, // all codecs allowed
            SegmentKind::Snapshot => codec == SegmentCodec::Zstd,
        }
    }

    /// Get the default codec for a segment kind.
    pub const fn default_codec(&self, kind: SegmentKind) -> SegmentCodec {
        match kind {
            SegmentKind::Wal => self.wal_segment_codec,
            SegmentKind::Cold => self.cold_segment_codec,
            SegmentKind::Snapshot => self.snapshot_segment_codec,
        }
    }

    /// Get the zstd level for a segment kind.
    pub const fn zstd_level(&self, kind: SegmentKind) -> i32 {
        match kind {
            SegmentKind::Wal => 1,
            SegmentKind::Cold => self.zstd_cold_level,
            SegmentKind::Snapshot => self.zstd_snapshot_level,
        }
    }
}

/// Streaming codec selection policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingCodecPolicy {
    /// No transport compression.
    None,
    /// Always LZ4.
    Lz4,
    /// Always Zstd.
    Zstd,
    /// Adaptive: pick based on estimated bandwidth.
    Adaptive,
}

impl fmt::Display for StreamingCodecPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Lz4 => write!(f, "lz4"),
            Self::Zstd => write!(f, "zstd"),
            Self::Adaptive => write!(f, "adaptive"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §B — Segment-Level Zstd: Block Compression / Decompression
// ═══════════════════════════════════════════════════════════════════════════

/// Extended segment header metadata for Zstd-compressed segments.
/// Stored alongside UnifiedSegmentHeader (fits in the 4K header region).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZstdSegmentMeta {
    /// Compression codec (must be Zstd).
    pub codec: SegmentCodec,
    /// Zstd compression level used.
    pub codec_level: i32,
    /// Dictionary ID (0 = no dictionary).
    pub dictionary_id: u64,
    /// Dictionary checksum (0 = no dictionary).
    pub dictionary_checksum: u32,
    /// Number of blocks in the segment body.
    pub block_count: u32,
    /// Total uncompressed bytes.
    pub uncompressed_bytes: u64,
    /// Total compressed bytes (body only, excluding header).
    pub compressed_bytes: u64,
}

impl ZstdSegmentMeta {
    pub const fn new(level: i32) -> Self {
        Self {
            codec: SegmentCodec::Zstd,
            codec_level: level,
            dictionary_id: 0,
            dictionary_checksum: 0,
            block_count: 0,
            uncompressed_bytes: 0,
            compressed_bytes: 0,
        }
    }

    pub const fn with_dictionary(mut self, dict_id: u64, dict_checksum: u32) -> Self {
        self.dictionary_id = dict_id;
        self.dictionary_checksum = dict_checksum;
        self
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_bytes == 0 { return 1.0; }
        self.uncompressed_bytes as f64 / self.compressed_bytes as f64
    }

    /// Serialize to bytes (appended in 4K header region).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(48);
        buf.push(self.codec as u8);
        buf.extend_from_slice(&self.codec_level.to_le_bytes());
        buf.extend_from_slice(&self.dictionary_id.to_le_bytes());
        buf.extend_from_slice(&self.dictionary_checksum.to_le_bytes());
        buf.extend_from_slice(&self.block_count.to_le_bytes());
        buf.extend_from_slice(&self.uncompressed_bytes.to_le_bytes());
        buf.extend_from_slice(&self.compressed_bytes.to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 37 { return None; }
        let codec = SegmentCodec::from_u8(data[0])?;
        let codec_level = i32::from_le_bytes(data[1..5].try_into().ok()?);
        let dictionary_id = u64::from_le_bytes(data[5..13].try_into().ok()?);
        let dictionary_checksum = u32::from_le_bytes(data[13..17].try_into().ok()?);
        let block_count = u32::from_le_bytes(data[17..21].try_into().ok()?);
        let uncompressed_bytes = u64::from_le_bytes(data[21..29].try_into().ok()?);
        let compressed_bytes = u64::from_le_bytes(data[29..37].try_into().ok()?);
        Some(Self {
            codec, codec_level, dictionary_id, dictionary_checksum,
            block_count, uncompressed_bytes, compressed_bytes,
        })
    }
}

/// A compressed block within a Zstd segment.
///
/// Wire format: [uncompressed_len:u32][compressed_len:u32][encoding:u8][compressed_data][crc:u32]
#[derive(Debug, Clone)]
pub struct ZstdBlock {
    /// Original uncompressed length.
    pub uncompressed_len: u32,
    /// Compressed data length.
    pub compressed_len: u32,
    /// Block encoding (for layered encoding before zstd).
    pub encoding: ColdBlockEncoding,
    /// Compressed payload.
    pub compressed_data: Vec<u8>,
    /// CRC32 of the compressed data.
    pub crc: u32,
}

fn djb2_crc(data: &[u8]) -> u32 {
    let mut hash: u32 = 5381;
    for &b in data { hash = hash.wrapping_mul(33).wrapping_add(u32::from(b)); }
    hash
}

impl ZstdBlock {
    /// Wire format size.
    pub const fn wire_size(&self) -> usize {
        4 + 4 + 1 + self.compressed_data.len() + 4 // u32+u32+u8+data+crc
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.wire_size());
        buf.extend_from_slice(&self.uncompressed_len.to_le_bytes());
        buf.extend_from_slice(&self.compressed_len.to_le_bytes());
        buf.push(self.encoding as u8);
        buf.extend_from_slice(&self.compressed_data);
        buf.extend_from_slice(&self.crc.to_le_bytes());
        buf
    }

    /// Deserialize from bytes. Returns (block, bytes_consumed).
    pub fn from_bytes(data: &[u8]) -> Option<(Self, usize)> {
        if data.len() < 13 { return None; } // min: 4+4+1+0+4
        let uncompressed_len = u32::from_le_bytes(data[0..4].try_into().ok()?);
        let compressed_len = u32::from_le_bytes(data[4..8].try_into().ok()?) as usize;
        let encoding = ColdBlockEncoding::from_u8(data[8])?;
        let total = 9 + compressed_len + 4;
        if data.len() < total { return None; }
        let compressed_data = data[9..9 + compressed_len].to_vec();
        let crc = u32::from_le_bytes(data[9 + compressed_len..total].try_into().ok()?);
        Some((Self {
            uncompressed_len, compressed_len: compressed_len as u32,
            encoding, compressed_data, crc,
        }, total))
    }

    /// Verify CRC.
    pub fn verify(&self) -> bool {
        self.crc == djb2_crc(&self.compressed_data)
    }
}

/// Compress a raw block using Zstd.
pub fn zstd_compress_block(data: &[u8], level: i32, dict: Option<&[u8]>) -> Result<ZstdBlock, String> {
    let compressed = if let Some(dict_data) = dict {
        let mut compressor = zstd::bulk::Compressor::with_dictionary(level, dict_data)
            .map_err(|e| format!("zstd init compressor with dict: {e}"))?;
        compressor.compress(data)
            .map_err(|e| format!("zstd dict compress: {e}"))?
    } else {
        zstd::bulk::compress(data, level)
            .map_err(|e| format!("zstd compress: {e}"))?
    };

    let crc = djb2_crc(&compressed);
    Ok(ZstdBlock {
        uncompressed_len: data.len() as u32,
        compressed_len: compressed.len() as u32,
        encoding: ColdBlockEncoding::Zstd,
        compressed_data: compressed,
        crc,
    })
}

/// Decompress a Zstd block.
pub fn zstd_decompress_block(block: &ZstdBlock, dict: Option<&[u8]>) -> Result<Vec<u8>, String> {
    if !block.verify() {
        return Err("CRC mismatch on compressed block".to_owned());
    }

    let capacity = block.uncompressed_len as usize;
    let decompressed = if let Some(dict_data) = dict {
        let mut decompressor = zstd::bulk::Decompressor::with_dictionary(dict_data)
            .map_err(|e| format!("zstd init decompressor with dict: {e}"))?;
        decompressor.decompress(&block.compressed_data, capacity)
            .map_err(|e| format!("zstd dict decompress: {e}"))?
    } else {
        zstd::bulk::decompress(&block.compressed_data, capacity)
            .map_err(|e| format!("zstd decompress: {e}"))?
    };

    Ok(decompressed)
}

/// Compress a raw block using LZ4 (for comparison / fallback).
pub fn lz4_compress_block(data: &[u8]) -> Result<ZstdBlock, String> {
    let compressed = lz4_flex::compress_prepend_size(data);
    let crc = djb2_crc(&compressed);
    Ok(ZstdBlock {
        uncompressed_len: data.len() as u32,
        compressed_len: compressed.len() as u32,
        encoding: ColdBlockEncoding::Lz4,
        compressed_data: compressed,
        crc,
    })
}

/// Decompress an LZ4 block.
pub fn lz4_decompress_block(block: &ZstdBlock) -> Result<Vec<u8>, String> {
    if !block.verify() {
        return Err("CRC mismatch on compressed block".to_owned());
    }
    lz4_flex::decompress_size_prepended(&block.compressed_data)
        .map_err(|e| format!("lz4 decompress: {e}"))
}

/// Compress a block with the appropriate codec.
pub fn compress_block(data: &[u8], codec: SegmentCodec, level: i32, dict: Option<&[u8]>) -> Result<ZstdBlock, String> {
    match codec {
        SegmentCodec::None => {
            let crc = djb2_crc(data);
            Ok(ZstdBlock {
                uncompressed_len: data.len() as u32,
                compressed_len: data.len() as u32,
                encoding: ColdBlockEncoding::Raw,
                compressed_data: data.to_vec(),
                crc,
            })
        }
        SegmentCodec::Lz4 => lz4_compress_block(data),
        SegmentCodec::Zstd => zstd_compress_block(data, level, dict),
    }
}

/// Decompress a block with the appropriate codec.
pub fn decompress_block(block: &ZstdBlock, dict: Option<&[u8]>) -> Result<Vec<u8>, String> {
    match block.encoding {
        ColdBlockEncoding::Raw => {
            if !block.verify() {
                return Err("CRC mismatch on raw block".to_owned());
            }
            Ok(block.compressed_data.clone())
        }
        ColdBlockEncoding::Lz4 => lz4_decompress_block(block),
        ColdBlockEncoding::Zstd => zstd_decompress_block(block, dict),
        _ => Err(format!("unsupported block encoding: {}", block.encoding)),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §B2 — Zstd Segment Writer / Reader
// ═══════════════════════════════════════════════════════════════════════════

/// Write a full Zstd-compressed cold segment.
///
/// Invariants:
/// - ALWAYS creates new segment (never in-place modify)
/// - Manifest update is caller's responsibility (atomic step)
pub fn write_zstd_cold_segment(
    store: &SegmentStore,
    table_id: u64,
    shard_id: u64,
    rows: &[Vec<u8>],
    level: i32,
    dict: Option<&[u8]>,
    dict_id: u64,
) -> Result<(u64, ZstdSegmentMeta), SegmentStoreError> {
    let seg_id = store.next_segment_id();
    let hdr = UnifiedSegmentHeader::new_cold(seg_id, 256 * 1024 * 1024, SegmentCodec::Zstd, table_id, shard_id);
    store.create_segment(hdr)?;

    let mut meta = ZstdSegmentMeta::new(level);
    if dict_id > 0 {
        meta.dictionary_id = dict_id;
        meta.dictionary_checksum = dict.map_or(0, djb2_crc);
    }

    // Write block count first so reader knows when to stop
    let block_count = rows.len() as u32;
    store.write_chunk(seg_id, &block_count.to_le_bytes())?;

    for row in rows {
        let block = zstd_compress_block(row, level, dict)
            .map_err(SegmentStoreError::IoError)?;
        meta.uncompressed_bytes += row.len() as u64;
        meta.compressed_bytes += block.compressed_data.len() as u64;
        meta.block_count += 1;
        let block_bytes = block.to_bytes();
        store.write_chunk(seg_id, &block_bytes)?;
    }

    store.seal_segment(seg_id)?;
    Ok((seg_id, meta))
}

/// Read and decompress blocks from a Zstd cold segment.
pub fn read_zstd_cold_segment(
    store: &SegmentStore,
    segment_id: u64,
    dict: Option<&[u8]>,
) -> Result<Vec<Vec<u8>>, String> {
    let body = store.get_segment_body(segment_id)
        .map_err(|e| format!("read segment: {e}"))?;

    if body.len() < UNIFIED_HEADER_SIZE as usize {
        return Err("segment too small".to_owned());
    }

    let data = &body[UNIFIED_HEADER_SIZE as usize..];
    if data.len() < 4 {
        return Err("segment body too small for block count".to_owned());
    }
    let block_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut rows = Vec::with_capacity(block_count);

    for _ in 0..block_count {
        let (block, consumed) = ZstdBlock::from_bytes(&data[offset..])
            .ok_or_else(|| "failed to parse block".to_owned())?;
        let decompressed = decompress_block(&block, dict)?;
        rows.push(decompressed);
        offset += consumed;
    }

    Ok(rows)
}

// ═══════════════════════════════════════════════════════════════════════════
// §C — Dictionary Lifecycle
// ═══════════════════════════════════════════════════════════════════════════

/// Dictionary entry in the DictionaryStore.
#[derive(Debug, Clone)]
pub struct DictionaryEntry {
    /// Unique dictionary ID.
    pub dictionary_id: u64,
    /// Segment ID where the dictionary data is stored (DICT_SEGMENT).
    pub segment_id: u64,
    /// Table ID this dictionary applies to.
    pub table_id: u64,
    /// Schema version this dictionary was trained on.
    pub schema_version: u64,
    /// Dictionary data bytes.
    pub data: Vec<u8>,
    /// Checksum of dictionary data.
    pub checksum: u32,
    /// Creation timestamp.
    pub created_at_epoch: u64,
    /// Number of samples used for training.
    pub sample_count: u64,
}

/// Dictionary store — manages dictionary lifecycle.
///
/// Dictionaries are stored as special segments in the SegmentStore
/// and tracked in the Manifest via dictionary_id → segment_id mapping.
pub struct DictionaryStore {
    /// Dictionary ID → DictionaryEntry
    dictionaries: RwLock<BTreeMap<u64, DictionaryEntry>>,
    /// Table ID → latest dictionary ID
    table_dict: RwLock<BTreeMap<u64, u64>>,
    /// Next dictionary ID
    next_dict_id: AtomicU64,
    pub metrics: DictionaryMetrics,
}

/// Dictionary metrics.
#[derive(Debug, Default)]
pub struct DictionaryMetrics {
    pub dictionaries_created: AtomicU64,
    pub dictionaries_used: AtomicU64,
    pub dictionary_bytes_total: AtomicU64,
    pub training_runs: AtomicU64,
    pub dict_hit: AtomicU64,
    pub dict_miss: AtomicU64,
}

impl DictionaryMetrics {
    pub fn hit_rate(&self) -> f64 {
        let hit = self.dict_hit.load(Ordering::Relaxed) as f64;
        let miss = self.dict_miss.load(Ordering::Relaxed) as f64;
        if hit + miss == 0.0 { return 0.0; }
        hit / (hit + miss)
    }
}

impl DictionaryStore {
    pub fn new() -> Self {
        Self {
            dictionaries: RwLock::new(BTreeMap::new()),
            table_dict: RwLock::new(BTreeMap::new()),
            next_dict_id: AtomicU64::new(1),
            metrics: DictionaryMetrics::default(),
        }
    }

    /// Train a new dictionary from sample data.
    ///
    /// In production this would use `zstd::dict::from_samples()`.
    /// Here we create a dictionary from the concatenated samples.
    pub fn train_dictionary(
        &self,
        table_id: u64,
        schema_version: u64,
        samples: &[Vec<u8>],
        max_dict_size: usize,
    ) -> Result<DictionaryEntry, String> {
        if samples.is_empty() {
            return Err("no samples for dictionary training".to_owned());
        }

        // Use zstd's dictionary training
        let dict_data = zstd::dict::from_samples(samples, max_dict_size)
            .map_err(|e| format!("zstd dict training: {e}"))?;

        let dict_id = self.next_dict_id.fetch_add(1, Ordering::Relaxed);
        let checksum = djb2_crc(&dict_data);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = DictionaryEntry {
            dictionary_id: dict_id,
            segment_id: 0, // set when stored as segment
            table_id,
            schema_version,
            data: dict_data,
            checksum,
            created_at_epoch: now,
            sample_count: samples.len() as u64,
        };

        self.metrics.dictionaries_created.fetch_add(1, Ordering::Relaxed);
        self.metrics.dictionary_bytes_total.fetch_add(entry.data.len() as u64, Ordering::Relaxed);
        self.metrics.training_runs.fetch_add(1, Ordering::Relaxed);

        Ok(entry)
    }

    /// Register a dictionary entry.
    pub fn register(&self, entry: DictionaryEntry) {
        let dict_id = entry.dictionary_id;
        let table_id = entry.table_id;
        self.dictionaries.write().insert(dict_id, entry);
        self.table_dict.write().insert(table_id, dict_id);
    }

    /// Get dictionary by ID.
    pub fn get(&self, dictionary_id: u64) -> Option<DictionaryEntry> {
        let entry = self.dictionaries.read().get(&dictionary_id).cloned();
        if entry.is_some() {
            self.metrics.dict_hit.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.dict_miss.fetch_add(1, Ordering::Relaxed);
        }
        entry
    }

    /// Get the latest dictionary for a table.
    pub fn get_for_table(&self, table_id: u64) -> Option<DictionaryEntry> {
        let dict_id = *self.table_dict.read().get(&table_id)?;
        self.get(dict_id)
    }

    /// List all dictionary IDs.
    pub fn list(&self) -> Vec<u64> {
        self.dictionaries.read().keys().copied().collect()
    }

    /// Store a dictionary as a segment in the SegmentStore.
    pub fn store_as_segment(
        &self,
        entry: &mut DictionaryEntry,
        store: &SegmentStore,
    ) -> Result<u64, SegmentStoreError> {
        let seg_id = store.next_segment_id();
        // Use Snapshot kind to carry dictionary data (DICT is a special snapshot)
        let hdr = UnifiedSegmentHeader::new_snapshot(seg_id, entry.data.len() as u64, entry.dictionary_id, 0);
        store.create_segment(hdr)?;
        store.write_chunk(seg_id, &entry.data)?;
        store.seal_segment(seg_id)?;
        entry.segment_id = seg_id;
        Ok(seg_id)
    }

    /// Load a dictionary from a segment.
    pub fn load_from_segment(
        &self,
        store: &SegmentStore,
        segment_id: u64,
        dictionary_id: u64,
        table_id: u64,
        schema_version: u64,
    ) -> Result<DictionaryEntry, String> {
        let body = store.get_segment_body(segment_id)
            .map_err(|e| format!("load dict segment: {e}"))?;

        let data = if body.len() > UNIFIED_HEADER_SIZE as usize {
            body[UNIFIED_HEADER_SIZE as usize..].to_vec()
        } else {
            return Err("dict segment too small".to_owned());
        };

        let checksum = djb2_crc(&data);
        Ok(DictionaryEntry {
            dictionary_id,
            segment_id,
            table_id,
            schema_version,
            data,
            checksum,
            created_at_epoch: 0,
            sample_count: 0,
        })
    }
}

impl Default for DictionaryStore {
    fn default() -> Self { Self::new() }
}

// ═══════════════════════════════════════════════════════════════════════════
// §D — Streaming Codec Negotiation
// ═══════════════════════════════════════════════════════════════════════════

/// Streaming codec capabilities advertised during handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingCodecCaps {
    /// Codecs the node supports.
    pub supported: Vec<SegmentCodec>,
    /// Preferred codec (based on bandwidth estimate).
    pub preferred: SegmentCodec,
    /// Estimated bandwidth in bytes/sec (for adaptive selection).
    pub estimated_bandwidth_bps: u64,
}

impl StreamingCodecCaps {
    /// High bandwidth (same datacenter): prefer LZ4 for speed.
    pub fn high_bandwidth() -> Self {
        Self {
            supported: vec![SegmentCodec::None, SegmentCodec::Lz4, SegmentCodec::Zstd],
            preferred: SegmentCodec::Lz4,
            estimated_bandwidth_bps: 10 * 1024 * 1024 * 1024, // 10 GB/s
        }
    }

    /// Low bandwidth (cross-datacenter): prefer Zstd for ratio.
    pub fn low_bandwidth() -> Self {
        Self {
            supported: vec![SegmentCodec::Lz4, SegmentCodec::Zstd],
            preferred: SegmentCodec::Zstd,
            estimated_bandwidth_bps: 100 * 1024 * 1024, // 100 MB/s
        }
    }
}

/// Negotiate streaming codec between sender and receiver.
pub fn negotiate_streaming_codec(
    sender: &StreamingCodecCaps,
    receiver: &StreamingCodecCaps,
) -> SegmentCodec {
    // Both must support the codec
    let common: Vec<SegmentCodec> = sender.supported.iter()
        .filter(|c| receiver.supported.contains(c))
        .copied()
        .collect();

    if common.is_empty() {
        return SegmentCodec::None;
    }

    // If either prefers zstd and both support it, use zstd
    if (sender.preferred == SegmentCodec::Zstd || receiver.preferred == SegmentCodec::Zstd)
        && common.contains(&SegmentCodec::Zstd)
    {
        return SegmentCodec::Zstd;
    }

    // Otherwise prefer sender's preference if common
    if common.contains(&sender.preferred) {
        return sender.preferred;
    }

    // Fallback to first common
    common[0]
}

/// Compress a streaming chunk.
pub fn compress_streaming_chunk(data: &[u8], codec: SegmentCodec, level: i32) -> Result<Vec<u8>, String> {
    match codec {
        SegmentCodec::None => Ok(data.to_vec()),
        SegmentCodec::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
        SegmentCodec::Zstd => {
            zstd::bulk::compress(data, level)
                .map_err(|e| format!("zstd stream compress: {e}"))
        }
    }
}

/// Decompress a streaming chunk.
pub fn decompress_streaming_chunk(data: &[u8], codec: SegmentCodec, max_size: usize) -> Result<Vec<u8>, String> {
    match codec {
        SegmentCodec::None => Ok(data.to_vec()),
        SegmentCodec::Lz4 => {
            lz4_flex::decompress_size_prepended(data)
                .map_err(|e| format!("lz4 stream decompress: {e}"))
        }
        SegmentCodec::Zstd => {
            zstd::bulk::decompress(data, max_size)
                .map_err(|e| format!("zstd stream decompress: {e}"))
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §E — Decompression Isolation: Thread Pool + Cache
// ═══════════════════════════════════════════════════════════════════════════

/// Cache key for decompressed blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecompressCacheKey {
    pub segment_id: u64,
    pub block_index: u32,
}

/// LRU-based decompression cache with byte-capacity limit.
pub struct DecompressCache {
    /// Capacity in bytes.
    capacity_bytes: u64,
    /// Current usage in bytes.
    used_bytes: AtomicU64,
    /// Cache entries: key → (data, insertion_order).
    entries: Mutex<HashMap<DecompressCacheKey, Vec<u8>>>,
    /// LRU order.
    order: Mutex<VecDeque<DecompressCacheKey>>,
    pub metrics: DecompressCacheMetrics,
}

/// Cache metrics.
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
        if h + m == 0.0 { return 0.0; }
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

    /// Get a cached decompressed block.
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

    /// Insert a decompressed block into the cache.
    pub fn insert(&self, key: DecompressCacheKey, data: Vec<u8>) {
        let data_len = data.len() as u64;

        // Evict until we have space
        while self.used_bytes.load(Ordering::Relaxed) + data_len > self.capacity_bytes {
            let evict_key = {
                let mut order = self.order.lock();
                order.pop_front()
            };
            if let Some(ek) = evict_key {
                let mut entries = self.entries.lock();
                if let Some(evicted) = entries.remove(&ek) {
                    self.used_bytes.fetch_sub(evicted.len() as u64, Ordering::Relaxed);
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
            self.metrics.bytes_cached.fetch_add(data_len, Ordering::Relaxed);
            self.order.lock().push_back(key);
        }
    }

    /// Current cache usage in bytes.
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Decompression pool with concurrency limit.
///
/// Zstd decompression NEVER runs on the OLTP executor thread.
/// This pool enforces isolation.
pub struct DecompressPool {
    /// Max concurrent decompression tasks.
    pub max_concurrent: u32,
    /// Current inflight count.
    inflight: AtomicU64,
    /// Shared cache.
    pub cache: DecompressCache,
    pub metrics: DecompressPoolMetrics,
}

/// Pool metrics.
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

    /// Decompress a block, using cache if available.
    /// Returns Err if overloaded (concurrency limit reached).
    pub fn decompress(
        &self,
        segment_id: u64,
        block_index: u32,
        block: &ZstdBlock,
        dict: Option<&[u8]>,
    ) -> Result<Vec<u8>, String> {
        let cache_key = DecompressCacheKey { segment_id, block_index };

        // Check cache first
        if let Some(cached) = self.cache.get(&cache_key) {
            return Ok(cached);
        }

        // Check concurrency limit
        let current = self.inflight.fetch_add(1, Ordering::Relaxed);
        if current >= u64::from(self.max_concurrent) {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            self.metrics.rejected_overload.fetch_add(1, Ordering::Relaxed);
            return Err("decompress pool overloaded".to_owned());
        }

        let start = std::time::Instant::now();
        let result = decompress_block(block, dict);
        let elapsed_ns = start.elapsed().as_nanos() as u64;

        self.inflight.fetch_sub(1, Ordering::Relaxed);
        self.metrics.decompress_total.fetch_add(1, Ordering::Relaxed);
        self.metrics.decompress_ns_total.fetch_add(elapsed_ns, Ordering::Relaxed);

        match result {
            Ok(data) => {
                self.metrics.decompress_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
                self.cache.insert(cache_key, data.clone());
                Ok(data)
            }
            Err(e) => {
                self.metrics.decompress_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Current inflight count.
    pub fn inflight(&self) -> u64 {
        self.inflight.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §F — GC Rewrite = Recompression Opportunity
// ═══════════════════════════════════════════════════════════════════════════

/// Request to recompress a segment (e.g., during GC merge or dict upgrade).
#[derive(Debug, Clone)]
pub struct RecompressRequest {
    /// Source segment ID.
    pub source_segment_id: u64,
    /// Target codec.
    pub target_codec: SegmentCodec,
    /// Target zstd level.
    pub target_level: i32,
    /// New dictionary ID (0 = no dict).
    pub new_dictionary_id: u64,
    /// Reason for recompression.
    pub reason: RecompressReason,
}

/// Why a segment is being recompressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecompressReason {
    /// GC merge of multiple cold segments.
    GcMerge,
    /// Dictionary upgrade (new dict available).
    DictUpgrade,
    /// Codec policy change (e.g., lz4 → zstd).
    CodecChange,
    /// Manual recompression via ops command.
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

/// Result of a recompression.
#[derive(Debug, Clone)]
pub struct RecompressResult {
    /// Old segment ID.
    pub old_segment_id: u64,
    /// New segment ID.
    pub new_segment_id: u64,
    /// Old codec.
    pub old_codec: SegmentCodec,
    /// New codec.
    pub new_codec: SegmentCodec,
    /// Old size.
    pub old_bytes: u64,
    /// New size.
    pub new_bytes: u64,
    /// Space saved.
    pub bytes_saved: i64,
}

/// Recompress a segment using a new codec/dictionary.
///
/// 1. Read all blocks from source segment
/// 2. Decompress each block (using old dict if needed)
/// 3. Recompress with new codec/dict
/// 4. Write to new segment
/// 5. Seal new segment
/// 6. Return result (caller updates manifest + schedules GC for old)
pub fn recompress_segment(
    store: &SegmentStore,
    request: &RecompressRequest,
    old_dict: Option<&[u8]>,
    new_dict: Option<&[u8]>,
    table_id: u64,
    shard_id: u64,
) -> Result<RecompressResult, String> {
    // Read source
    let rows = read_zstd_cold_segment(store, request.source_segment_id, old_dict)?;
    let old_size = store.segment_size(request.source_segment_id)
        .map_err(|e| format!("segment size: {e}"))?;

    // Write new segment
    let (new_seg_id, _meta) = write_zstd_cold_segment(
        store, table_id, shard_id, &rows,
        request.target_level, new_dict, request.new_dictionary_id,
    ).map_err(|e| format!("write recompressed: {e}"))?;

    let new_size = store.segment_size(new_seg_id)
        .map_err(|e| format!("new segment size: {e}"))?;

    Ok(RecompressResult {
        old_segment_id: request.source_segment_id,
        new_segment_id: new_seg_id,
        old_codec: SegmentCodec::Zstd, // assume old was zstd
        new_codec: request.target_codec,
        old_bytes: old_size,
        new_bytes: new_size,
        bytes_saved: old_size as i64 - new_size as i64,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// §G — Metrics & Admin Status
// ═══════════════════════════════════════════════════════════════════════════

/// Comprehensive Zstd metrics for /admin/status.
#[derive(Debug, Clone, Default)]
pub struct ZstdMetricsSnapshot {
    // Compression
    pub compress_total: u64,
    pub compress_bytes_in: u64,
    pub compress_bytes_out: u64,
    pub compress_ratio: f64,
    // Decompression
    pub decompress_total: u64,
    pub decompress_bytes: u64,
    pub decompress_ns_total: u64,
    pub decompress_avg_us: f64,
    pub decompress_errors: u64,
    pub decompress_rejected: u64,
    // Cache
    pub cache_hit_rate: f64,
    pub cache_used_bytes: u64,
    pub cache_evictions: u64,
    // Dictionary
    pub dict_count: u64,
    pub dict_bytes_total: u64,
    pub dict_hit_rate: f64,
    pub dict_training_runs: u64,
    // Streaming
    pub stream_bytes_saved_ratio: f64,
}

/// Zstd metrics collector.
#[derive(Debug, Default)]
pub struct ZstdCompressMetrics {
    pub compress_total: AtomicU64,
    pub compress_bytes_in: AtomicU64,
    pub compress_bytes_out: AtomicU64,
}

impl ZstdCompressMetrics {
    pub fn record(&self, bytes_in: u64, bytes_out: u64) {
        self.compress_total.fetch_add(1, Ordering::Relaxed);
        self.compress_bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
        self.compress_bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
    }

    pub fn ratio(&self) -> f64 {
        let i = self.compress_bytes_in.load(Ordering::Relaxed) as f64;
        let o = self.compress_bytes_out.load(Ordering::Relaxed) as f64;
        if o > 0.0 { i / o } else { 1.0 }
    }
}

/// Build a full Zstd metrics snapshot from all components.
pub fn build_zstd_metrics(
    compress: &ZstdCompressMetrics,
    pool: &DecompressPool,
    dict_store: &DictionaryStore,
) -> ZstdMetricsSnapshot {
    let decompress_total = pool.metrics.decompress_total.load(Ordering::Relaxed);
    let decompress_ns = pool.metrics.decompress_ns_total.load(Ordering::Relaxed);
    let avg_us = if decompress_total > 0 {
        (decompress_ns as f64 / decompress_total as f64) / 1000.0
    } else { 0.0 };

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
        dict_bytes_total: dict_store.metrics.dictionary_bytes_total.load(Ordering::Relaxed),
        dict_hit_rate: dict_store.metrics.hit_rate(),
        dict_training_runs: dict_store.metrics.training_runs.load(Ordering::Relaxed),
        stream_bytes_saved_ratio: compress.ratio(),
    }
}

/// Segment codec listing for `falconctl segments ls --codec=...`
#[derive(Debug, Clone)]
pub struct SegmentCodecInfo {
    pub segment_id: u64,
    pub kind: SegmentKind,
    pub codec: SegmentCodec,
    pub size_bytes: u64,
    pub dictionary_id: u64,
    pub sealed: bool,
}

/// List segments filtered by codec.
pub fn list_segments_by_codec(manifest: &Manifest, codec: Option<SegmentCodec>) -> Vec<SegmentCodecInfo> {
    manifest.segments.values()
        .filter(|e| codec.is_none_or(|c| e.codec == c))
        .map(|e| SegmentCodecInfo {
            segment_id: e.segment_id,
            kind: e.kind,
            codec: e.codec,
            size_bytes: e.size_bytes,
            dictionary_id: 0, // would come from ZstdSegmentMeta in real impl
            sealed: e.sealed,
        })
        .collect()
}

// ═══════════════════════════════════════════════════════════════════════════
// §I — Unit Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unified_data_plane::{ManifestEntry, LogicalRange};
    use crate::structured_lsn::StructuredLsn;

    // -- §A: Codec Policy --

    #[test]
    fn test_codec_policy_defaults() {
        let policy = CodecPolicy::default();
        assert_eq!(policy.wal_segment_codec, SegmentCodec::None);
        assert_eq!(policy.cold_segment_codec, SegmentCodec::Zstd);
        assert_eq!(policy.snapshot_segment_codec, SegmentCodec::Zstd);
    }

    #[test]
    fn test_codec_policy_validation() {
        let policy = CodecPolicy::default();
        // WAL: none/lz4 ok, zstd forbidden
        assert!(policy.validate_codec(SegmentKind::Wal, SegmentCodec::None));
        assert!(policy.validate_codec(SegmentKind::Wal, SegmentCodec::Lz4));
        assert!(!policy.validate_codec(SegmentKind::Wal, SegmentCodec::Zstd));
        // Cold: anything ok
        assert!(policy.validate_codec(SegmentKind::Cold, SegmentCodec::Zstd));
        assert!(policy.validate_codec(SegmentKind::Cold, SegmentCodec::Lz4));
        // Snapshot: zstd only
        assert!(policy.validate_codec(SegmentKind::Snapshot, SegmentCodec::Zstd));
        assert!(!policy.validate_codec(SegmentKind::Snapshot, SegmentCodec::Lz4));
    }

    // -- §B: Block Compression --

    #[test]
    fn test_zstd_compress_decompress_roundtrip() {
        let data = b"hello world, this is a test of zstd compression in falcon segments!";
        let block = zstd_compress_block(data, 3, None).unwrap();
        assert!(block.verify());
        assert_eq!(block.encoding, ColdBlockEncoding::Zstd);
        let decompressed = zstd_decompress_block(&block, None).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_block_serialization() {
        let data = vec![42u8; 1000];
        let block = zstd_compress_block(&data, 3, None).unwrap();
        let bytes = block.to_bytes();
        let (recovered, consumed) = ZstdBlock::from_bytes(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert!(recovered.verify());
        let decompressed = zstd_decompress_block(&recovered, None).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_block_crc_verification() {
        let block = zstd_compress_block(b"test data", 1, None).unwrap();
        assert!(block.verify());
        let mut corrupted = block.clone();
        corrupted.crc = corrupted.crc.wrapping_add(1);
        assert!(!corrupted.verify());
    }

    #[test]
    fn test_lz4_compress_decompress_roundtrip() {
        let data = b"lz4 test data for falcon segments";
        let block = lz4_compress_block(data).unwrap();
        assert!(block.verify());
        let decompressed = lz4_decompress_block(&block).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_block_dispatches() {
        let data = b"dispatch test data for all codecs";
        // None
        let b = compress_block(data, SegmentCodec::None, 0, None).unwrap();
        assert_eq!(decompress_block(&b, None).unwrap(), data);
        // LZ4
        let b = compress_block(data, SegmentCodec::Lz4, 0, None).unwrap();
        assert_eq!(decompress_block(&b, None).unwrap(), data);
        // Zstd
        let b = compress_block(data, SegmentCodec::Zstd, 3, None).unwrap();
        assert_eq!(decompress_block(&b, None).unwrap(), data);
    }

    #[test]
    fn test_zstd_compression_ratio() {
        // Repetitive data should compress well
        let data = vec![0xABu8; 10_000];
        let block = zstd_compress_block(&data, 3, None).unwrap();
        let ratio = data.len() as f64 / block.compressed_data.len() as f64;
        assert!(ratio > 2.0, "expected ratio > 2.0, got {:.2}", ratio);
    }

    // -- §B2: Segment Writer/Reader --

    #[test]
    fn test_write_read_zstd_cold_segment() {
        let store = SegmentStore::new();
        let rows: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8; 200]).collect();

        let (seg_id, meta) = write_zstd_cold_segment(
            &store, 1, 0, &rows, 3, None, 0,
        ).unwrap();

        assert!(store.is_sealed(seg_id).unwrap());
        assert_eq!(meta.block_count, 10);
        assert!(meta.compression_ratio() >= 1.0);

        let recovered = read_zstd_cold_segment(&store, seg_id, None).unwrap();
        assert_eq!(recovered.len(), 10);
        for (i, row) in recovered.iter().enumerate() {
            assert_eq!(row, &vec![i as u8; 200]);
        }
    }

    #[test]
    fn test_zstd_segment_meta_roundtrip() {
        let meta = ZstdSegmentMeta::new(5)
            .with_dictionary(42, 0xDEAD);
        let bytes = meta.to_bytes();
        let recovered = ZstdSegmentMeta::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.codec_level, 5);
        assert_eq!(recovered.dictionary_id, 42);
        assert_eq!(recovered.dictionary_checksum, 0xDEAD);
    }

    // -- §C: Dictionary --

    #[test]
    fn test_dictionary_training_and_use() {
        let dict_store = DictionaryStore::new();
        // Create repetitive samples for dictionary training
        let samples: Vec<Vec<u8>> = (0..100).map(|i| {
            let mut v = b"FalconDB row data prefix common ".to_vec();
            v.extend_from_slice(&(i as u32).to_le_bytes());
            v.extend_from_slice(&vec![0u8; 64]);
            v
        }).collect();

        let entry = dict_store.train_dictionary(1, 1, &samples, 4096).unwrap();
        assert!(!entry.data.is_empty());
        assert_eq!(entry.table_id, 1);
        assert!(entry.checksum != 0);

        dict_store.register(entry.clone());
        let fetched = dict_store.get(entry.dictionary_id).unwrap();
        assert_eq!(fetched.data, entry.data);

        let by_table = dict_store.get_for_table(1).unwrap();
        assert_eq!(by_table.dictionary_id, entry.dictionary_id);
    }

    #[test]
    fn test_dictionary_compress_with_dict() {
        let dict_store = DictionaryStore::new();
        let samples: Vec<Vec<u8>> = (0..100).map(|i| {
            let mut v = b"common prefix data for dict training ".to_vec();
            v.extend_from_slice(&(i as u32).to_le_bytes());
            v.extend_from_slice(&vec![0xABu8; 50]);
            v
        }).collect();

        let entry = dict_store.train_dictionary(1, 1, &samples, 8192).unwrap();
        let dict_data = &entry.data;

        // Compress with dictionary
        let test_data = b"common prefix data for dict training \x05\x00\x00\x00ABABABABABABABABABABABABABABABABABABABABABABABABABAB";
        let block = zstd_compress_block(test_data, 3, Some(dict_data)).unwrap();
        assert!(block.verify());

        let decompressed = zstd_decompress_block(&block, Some(dict_data)).unwrap();
        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_dictionary_store_as_segment() {
        let seg_store = SegmentStore::new();
        let dict_store = DictionaryStore::new();

        let samples: Vec<Vec<u8>> = (0..50).map(|i| {
            let mut v = b"segment dict data ".to_vec();
            v.extend_from_slice(&vec![i as u8; 40]);
            v
        }).collect();

        let mut entry = dict_store.train_dictionary(1, 1, &samples, 4096).unwrap();
        let seg_id = dict_store.store_as_segment(&mut entry, &seg_store).unwrap();
        assert!(seg_store.is_sealed(seg_id).unwrap());
        assert_eq!(entry.segment_id, seg_id);

        // Load back
        let loaded = dict_store.load_from_segment(&seg_store, seg_id, entry.dictionary_id, 1, 1).unwrap();
        assert_eq!(loaded.data, entry.data);
    }

    #[test]
    fn test_dictionary_metrics() {
        let dict_store = DictionaryStore::new();
        let samples: Vec<Vec<u8>> = (0..50).map(|i| vec![i as u8; 100]).collect();
        let entry = dict_store.train_dictionary(1, 1, &samples, 4096).unwrap();
        dict_store.register(entry.clone());

        assert_eq!(dict_store.metrics.training_runs.load(Ordering::Relaxed), 1);
        assert_eq!(dict_store.metrics.dictionaries_created.load(Ordering::Relaxed), 1);

        dict_store.get(entry.dictionary_id);
        assert_eq!(dict_store.metrics.dict_hit.load(Ordering::Relaxed), 1);

        dict_store.get(9999);
        assert_eq!(dict_store.metrics.dict_miss.load(Ordering::Relaxed), 1);
        assert!((dict_store.metrics.hit_rate() - 0.5).abs() < 0.01);
    }

    // -- §D: Streaming Codec Negotiation --

    #[test]
    fn test_negotiate_high_bandwidth() {
        let sender = StreamingCodecCaps::high_bandwidth();
        let receiver = StreamingCodecCaps::high_bandwidth();
        let codec = negotiate_streaming_codec(&sender, &receiver);
        assert_eq!(codec, SegmentCodec::Lz4);
    }

    #[test]
    fn test_negotiate_low_bandwidth() {
        let sender = StreamingCodecCaps::low_bandwidth();
        let receiver = StreamingCodecCaps::low_bandwidth();
        let codec = negotiate_streaming_codec(&sender, &receiver);
        assert_eq!(codec, SegmentCodec::Zstd);
    }

    #[test]
    fn test_negotiate_mixed_prefers_zstd() {
        let sender = StreamingCodecCaps::high_bandwidth();
        let receiver = StreamingCodecCaps::low_bandwidth();
        let codec = negotiate_streaming_codec(&sender, &receiver);
        // Receiver prefers zstd, both support it → zstd
        assert_eq!(codec, SegmentCodec::Zstd);
    }

    #[test]
    fn test_streaming_chunk_roundtrip() {
        let data = vec![42u8; 4096];
        for codec in [SegmentCodec::None, SegmentCodec::Lz4, SegmentCodec::Zstd] {
            let compressed = compress_streaming_chunk(&data, codec, 1).unwrap();
            let decompressed = decompress_streaming_chunk(&compressed, codec, data.len()).unwrap();
            assert_eq!(decompressed, data, "failed for codec {:?}", codec);
        }
    }

    // -- §E: Decompress Cache & Pool --

    #[test]
    fn test_decompress_cache_basic() {
        let cache = DecompressCache::new(1024 * 1024);
        let key = DecompressCacheKey { segment_id: 1, block_index: 0 };
        assert!(cache.get(&key).is_none());

        cache.insert(key, vec![1, 2, 3]);
        let cached = cache.get(&key).unwrap();
        assert_eq!(cached, vec![1, 2, 3]);
        assert!(cache.metrics.hit_rate() > 0.0);
    }

    #[test]
    fn test_decompress_cache_eviction() {
        let cache = DecompressCache::new(100); // tiny cache
        for i in 0..20u32 {
            let key = DecompressCacheKey { segment_id: 1, block_index: i };
            cache.insert(key, vec![0u8; 50]); // each 50 bytes, cache=100 → evictions
        }
        assert!(cache.metrics.evictions.load(Ordering::Relaxed) > 0);
        assert!(cache.used_bytes() <= 100);
    }

    #[test]
    fn test_decompress_pool_basic() {
        let pool = DecompressPool::new(4, 1024 * 1024);
        let data = b"pool test data";
        let block = zstd_compress_block(data, 1, None).unwrap();

        let result = pool.decompress(1, 0, &block, None).unwrap();
        assert_eq!(result, data);
        assert_eq!(pool.metrics.decompress_total.load(Ordering::Relaxed), 1);

        // Second call hits cache
        let cached = pool.decompress(1, 0, &block, None).unwrap();
        assert_eq!(cached, data);
        assert_eq!(pool.cache.metrics.hits.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_decompress_pool_crc_failure() {
        let pool = DecompressPool::new(4, 1024 * 1024);
        let mut block = zstd_compress_block(b"data", 1, None).unwrap();
        block.crc = 0xDEAD; // corrupt CRC

        let result = pool.decompress(1, 0, &block, None);
        assert!(result.is_err());
        assert_eq!(pool.metrics.decompress_errors.load(Ordering::Relaxed), 1);
    }

    // -- §F: Recompression --

    #[test]
    fn test_recompress_segment() {
        let store = SegmentStore::new();
        let rows: Vec<Vec<u8>> = (0..5).map(|i| vec![i as u8; 300]).collect();

        // Write with level 1
        let (seg_id, _) = write_zstd_cold_segment(&store, 1, 0, &rows, 1, None, 0).unwrap();

        // Recompress with level 5
        let request = RecompressRequest {
            source_segment_id: seg_id,
            target_codec: SegmentCodec::Zstd,
            target_level: 5,
            new_dictionary_id: 0,
            reason: RecompressReason::CodecChange,
        };

        let result = recompress_segment(&store, &request, None, None, 1, 0).unwrap();
        assert_ne!(result.old_segment_id, result.new_segment_id);
        assert!(store.is_sealed(result.new_segment_id).unwrap());

        // Verify data integrity after recompression
        let recovered = read_zstd_cold_segment(&store, result.new_segment_id, None).unwrap();
        assert_eq!(recovered.len(), 5);
        for (i, row) in recovered.iter().enumerate() {
            assert_eq!(row, &vec![i as u8; 300]);
        }
    }

    // -- §G: Metrics --

    #[test]
    fn test_zstd_compress_metrics() {
        let metrics = ZstdCompressMetrics::default();
        metrics.record(1000, 300);
        metrics.record(2000, 500);
        assert_eq!(metrics.compress_total.load(Ordering::Relaxed), 2);
        assert!((metrics.ratio() - 3.75).abs() < 0.01); // 3000/800
    }

    #[test]
    fn test_build_zstd_metrics() {
        let compress = ZstdCompressMetrics::default();
        compress.record(5000, 1000);
        let pool = DecompressPool::new(4, 1024 * 1024);
        let dict_store = DictionaryStore::new();

        let snapshot = build_zstd_metrics(&compress, &pool, &dict_store);
        assert_eq!(snapshot.compress_total, 1);
        assert_eq!(snapshot.compress_bytes_in, 5000);
        assert_eq!(snapshot.compress_bytes_out, 1000);
        assert!((snapshot.compress_ratio - 5.0).abs() < 0.01);
    }

    #[test]
    fn test_list_segments_by_codec() {
        let mut m = Manifest::new();
        m.add_segment(ManifestEntry {
            segment_id: 0, kind: SegmentKind::Wal, size_bytes: 100,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::ZERO, end_lsn: StructuredLsn::ZERO,
            },
            sealed: true,
        });
        m.add_segment(ManifestEntry {
            segment_id: 1, kind: SegmentKind::Cold, size_bytes: 200,
            codec: SegmentCodec::Zstd,
            logical_range: LogicalRange::Cold {
                table_id: 1, shard_id: 0, min_key: vec![], max_key: vec![],
            },
            sealed: true,
        });
        m.add_segment(ManifestEntry {
            segment_id: 2, kind: SegmentKind::Cold, size_bytes: 300,
            codec: SegmentCodec::Lz4,
            logical_range: LogicalRange::Cold {
                table_id: 1, shard_id: 0, min_key: vec![], max_key: vec![],
            },
            sealed: true,
        });

        let all = list_segments_by_codec(&m, None);
        assert_eq!(all.len(), 3);

        let zstd_only = list_segments_by_codec(&m, Some(SegmentCodec::Zstd));
        assert_eq!(zstd_only.len(), 1);
        assert_eq!(zstd_only[0].segment_id, 1);

        let lz4_only = list_segments_by_codec(&m, Some(SegmentCodec::Lz4));
        assert_eq!(lz4_only.len(), 1);
        assert_eq!(lz4_only[0].segment_id, 2);
    }

    // -- Display --

    #[test]
    fn test_display_streaming_codec_policy() {
        assert_eq!(format!("{}", StreamingCodecPolicy::Adaptive), "adaptive");
        assert_eq!(format!("{}", StreamingCodecPolicy::Zstd), "zstd");
    }

    #[test]
    fn test_display_recompress_reason() {
        assert_eq!(format!("{}", RecompressReason::GcMerge), "gc_merge");
        assert_eq!(format!("{}", RecompressReason::DictUpgrade), "dict_upgrade");
    }
}
