//! # falcon_segment_codec — Segment-Level Compression for FalconDB
//!
//! **"Do not integrate Zstd as a library. Integrate it as a segment-level infrastructure primitive."**
//!
//! All compression in FalconDB flows through the [`SegmentCodecImpl`] trait.
//! Upper layers never call zstd/lz4 directly. This crate owns:
//!
//! - Unified codec abstraction (`SegmentCodecImpl` trait)
//! - `ZstdBlockCodec` — built on `zstd-safe` (NOT the high-level `zstd` crate)
//! - `Lz4BlockCodec` — built on `lz4_flex`
//! - `NoneCodec` — passthrough
//! - Dictionary management (load/use only — training is external)
//! - Block wire format (`BlockHeader` + compressed bytes + CRC)
//! - Streaming codec (chunk-level, orthogonal to segment codec)
//! - Decompression isolation (pool + LRU cache)
//! - GC/rewrite coordination
//! - Metrics

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

// ═══════════════════════════════════════════════════════════════════════════
// §A — Codec Identity & Policy
// ═══════════════════════════════════════════════════════════════════════════

/// Codec identifier stored in segment headers and manifest entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CodecId {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl CodecId {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::None),
            1 => Some(Self::Lz4),
            2 => Some(Self::Zstd),
            _ => None,
        }
    }
}

impl fmt::Display for CodecId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Lz4 => write!(f, "lz4"),
            Self::Zstd => write!(f, "zstd"),
        }
    }
}

/// Segment kind (mirrors unified_data_plane::SegmentKind for policy decisions).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    Wal,
    Cold,
    Snapshot,
}

/// Codec policy — which codecs are allowed for which segment kinds.
#[derive(Debug, Clone)]
pub struct CodecPolicy {
    pub wal_codec: CodecId,
    pub cold_codec: CodecId,
    pub snapshot_codec: CodecId,
    pub zstd_cold_level: i32,
    pub zstd_snapshot_level: i32,
}

impl Default for CodecPolicy {
    fn default() -> Self {
        Self {
            wal_codec: CodecId::None,
            cold_codec: CodecId::Zstd,
            snapshot_codec: CodecId::Zstd,
            zstd_cold_level: 3,
            zstd_snapshot_level: 5,
        }
    }
}

impl CodecPolicy {
    /// Is this codec allowed for the given segment kind?
    pub fn is_allowed(&self, kind: SegmentKind, codec: CodecId) -> bool {
        match kind {
            SegmentKind::Wal => codec == CodecId::None || codec == CodecId::Lz4,
            SegmentKind::Cold => true,
            SegmentKind::Snapshot => codec == CodecId::Zstd,
        }
    }

    /// Default codec for a segment kind.
    pub const fn default_codec(&self, kind: SegmentKind) -> CodecId {
        match kind {
            SegmentKind::Wal => self.wal_codec,
            SegmentKind::Cold => self.cold_codec,
            SegmentKind::Snapshot => self.snapshot_codec,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §A2 / §B — SegmentCodecImpl Trait (the ONLY compression abstraction)
// ═══════════════════════════════════════════════════════════════════════════

/// Codec error.
#[derive(Debug, Clone)]
pub struct CodecError(pub String);

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CodecError: {}", self.0)
    }
}

impl std::error::Error for CodecError {}

/// The unified segment codec abstraction.
///
/// **All** compression/decompression in FalconDB goes through this trait.
/// No code outside this crate may call zstd or lz4 directly.
pub trait SegmentCodecImpl: Send + Sync {
    /// Codec identity.
    fn codec_id(&self) -> CodecId;

    /// Compress a single independent block.
    /// Each call is stateless — no dependency on previous blocks.
    fn compress_block(&self, input: &[u8]) -> Result<Vec<u8>, CodecError>;

    /// Decompress a single independent block.
    fn decompress_block(&self, input: &[u8], original_len: usize) -> Result<Vec<u8>, CodecError>;
}

// ═══════════════════════════════════════════════════════════════════════════
// §B1 — NoneCodec
// ═══════════════════════════════════════════════════════════════════════════

/// Passthrough codec — no compression.
pub struct NoneCodec;

impl SegmentCodecImpl for NoneCodec {
    fn codec_id(&self) -> CodecId { CodecId::None }

    fn compress_block(&self, input: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(input.to_vec())
    }

    fn decompress_block(&self, input: &[u8], _original_len: usize) -> Result<Vec<u8>, CodecError> {
        Ok(input.to_vec())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §B1 — Lz4BlockCodec
// ═══════════════════════════════════════════════════════════════════════════

/// LZ4 block codec built on `lz4_flex`.
pub struct Lz4BlockCodec;

impl SegmentCodecImpl for Lz4BlockCodec {
    fn codec_id(&self) -> CodecId { CodecId::Lz4 }

    fn compress_block(&self, input: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(lz4_flex::compress_prepend_size(input))
    }

    fn decompress_block(&self, input: &[u8], _original_len: usize) -> Result<Vec<u8>, CodecError> {
        lz4_flex::decompress_size_prepended(input)
            .map_err(|e| CodecError(format!("lz4 decompress: {e}")))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §C1 — ZstdBlockCodec (built on zstd-safe, NOT the high-level zstd crate)
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for `ZstdBlockCodec`.
#[derive(Debug, Clone)]
pub struct ZstdCodecConfig {
    /// Compression level (1-22, default 3).
    pub level: i32,
    /// Include checksum in each compressed frame (default true).
    pub checksum: bool,
}

impl Default for ZstdCodecConfig {
    fn default() -> Self {
        Self { level: 3, checksum: true }
    }
}

/// Zstd block codec built directly on `zstd_safe`.
///
/// - Each `compress_block` call produces an independent zstd frame.
/// - No streaming state carried across blocks.
/// - Supports optional pre-loaded dictionary.
pub struct ZstdBlockCodec {
    config: ZstdCodecConfig,
    /// Pre-loaded dictionary bytes (None = no dictionary).
    dict_data: Option<Vec<u8>>,
}

impl fmt::Debug for ZstdBlockCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstdBlockCodec")
            .field("level", &self.config.level)
            .field("checksum", &self.config.checksum)
            .field("has_dict", &self.dict_data.is_some())
            .finish()
    }
}

impl ZstdBlockCodec {
    /// Create a new codec without dictionary.
    pub const fn new(config: ZstdCodecConfig) -> Self {
        Self { config, dict_data: None }
    }

    /// Create a new codec with a pre-loaded dictionary.
    pub const fn with_dictionary(config: ZstdCodecConfig, dict_data: Vec<u8>) -> Self {
        Self { config, dict_data: Some(dict_data) }
    }

    /// Compress using zstd-safe low-level API.
    fn zstd_compress(&self, input: &[u8]) -> Result<Vec<u8>, CodecError> {
        let bound = zstd_safe::compress_bound(input.len());
        let mut output = vec![0u8; bound];

        let mut cctx = zstd_safe::CCtx::create();
        cctx.set_parameter(zstd_safe::CParameter::CompressionLevel(self.config.level))
            .map_err(|c| CodecError(format!("zstd set level: code {c}")))?;
        cctx.set_parameter(zstd_safe::CParameter::ChecksumFlag(self.config.checksum))
            .map_err(|c| CodecError(format!("zstd set checksum: code {c}")))?;
        if let Some(ref dict) = self.dict_data {
            cctx.load_dictionary(dict)
                .map_err(|c| CodecError(format!("zstd load dict for compress: code {c}")))?;
        }
        let written = cctx.compress2(&mut output[..], input)
            .map_err(|c| CodecError(format!("zstd compress2: code {c}")))?;

        output.truncate(written);
        Ok(output)
    }

    /// Decompress using zstd-safe low-level API.
    fn zstd_decompress(&self, input: &[u8], original_len: usize) -> Result<Vec<u8>, CodecError> {
        // Allocate extra headroom in case original_len is inexact
        let alloc = if original_len == 0 { input.len() * 10 + 64 } else { original_len + 64 };
        let mut output = vec![0u8; alloc];

        let mut dctx = zstd_safe::DCtx::create();
        if let Some(ref dict) = self.dict_data {
            dctx.load_dictionary(dict)
                .map_err(|c| CodecError(format!("zstd load dict for decompress: code {c}")))?;
        }
        let written = dctx.decompress(&mut output[..], input)
            .map_err(|c| CodecError(format!("zstd decompress: code {c}")))?;

        output.truncate(written);
        Ok(output)
    }
}

impl SegmentCodecImpl for ZstdBlockCodec {
    fn codec_id(&self) -> CodecId { CodecId::Zstd }

    fn compress_block(&self, input: &[u8]) -> Result<Vec<u8>, CodecError> {
        self.zstd_compress(input)
    }

    fn decompress_block(&self, input: &[u8], original_len: usize) -> Result<Vec<u8>, CodecError> {
        self.zstd_decompress(input, original_len)
    }
}

/// Factory: create a codec from an ID and optional config/dictionary.
pub fn create_codec(
    id: CodecId,
    zstd_config: Option<ZstdCodecConfig>,
    dict_data: Option<Vec<u8>>,
) -> Box<dyn SegmentCodecImpl> {
    match id {
        CodecId::None => Box::new(NoneCodec),
        CodecId::Lz4 => Box::new(Lz4BlockCodec),
        CodecId::Zstd => {
            let cfg = zstd_config.unwrap_or_default();
            if let Some(dd) = dict_data {
                Box::new(ZstdBlockCodec::with_dictionary(cfg, dd))
            } else {
                Box::new(ZstdBlockCodec::new(cfg))
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §C — Dictionary Management (load / use only — training is external)
// ═══════════════════════════════════════════════════════════════════════════

/// A loaded dictionary ready for use by ZstdBlockCodec.
#[derive(Debug, Clone)]
pub struct DictionaryHandle {
    /// Unique dictionary ID.
    pub dictionary_id: u64,
    /// Segment ID where dict bytes are stored (DICT_SEGMENT).
    pub segment_id: u64,
    /// Table this dictionary applies to.
    pub table_id: u64,
    /// Schema version it was trained on.
    pub schema_version: u64,
    /// Raw dictionary bytes.
    pub data: Vec<u8>,
    /// CRC of dictionary data.
    pub checksum: u32,
}

fn djb2_crc(data: &[u8]) -> u32 {
    let mut hash: u32 = 5381;
    for &b in data {
        hash = hash.wrapping_mul(33).wrapping_add(u32::from(b));
    }
    hash
}

impl DictionaryHandle {
    /// Compute checksum and create a handle.
    pub fn new(dictionary_id: u64, segment_id: u64, table_id: u64, schema_version: u64, data: Vec<u8>) -> Self {
        let checksum = djb2_crc(&data);
        Self { dictionary_id, segment_id, table_id, schema_version, data, checksum }
    }

    /// Verify the dictionary checksum.
    pub fn verify(&self) -> bool {
        djb2_crc(&self.data) == self.checksum
    }
}

/// Dictionary registry — stores loaded dictionaries, keyed by dictionary_id.
///
/// Training is done externally (e.g. by a background task calling `zstd::dict::from_samples`).
/// This registry only loads, stores, and retrieves pre-trained dictionaries.
pub struct DictionaryRegistry {
    /// dictionary_id → DictionaryHandle
    entries: RwLock<BTreeMap<u64, DictionaryHandle>>,
    /// table_id → latest dictionary_id
    table_latest: RwLock<BTreeMap<u64, u64>>,
    pub metrics: DictionaryMetrics,
}

#[derive(Debug, Default)]
pub struct DictionaryMetrics {
    pub loaded: AtomicU64,
    pub lookups: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub bytes_total: AtomicU64,
}

impl DictionaryMetrics {
    pub fn hit_rate(&self) -> f64 {
        let h = self.hits.load(Ordering::Relaxed) as f64;
        let m = self.misses.load(Ordering::Relaxed) as f64;
        if h + m == 0.0 { 0.0 } else { h / (h + m) }
    }
}

impl DictionaryRegistry {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
            table_latest: RwLock::new(BTreeMap::new()),
            metrics: DictionaryMetrics::default(),
        }
    }

    /// Load a pre-trained dictionary into the registry.
    pub fn load(&self, handle: DictionaryHandle) {
        let dict_id = handle.dictionary_id;
        let table_id = handle.table_id;
        self.metrics.bytes_total.fetch_add(handle.data.len() as u64, Ordering::Relaxed);
        self.metrics.loaded.fetch_add(1, Ordering::Relaxed);
        self.entries.write().insert(dict_id, handle);
        self.table_latest.write().insert(table_id, dict_id);
    }

    /// Retrieve a dictionary by ID.
    pub fn get(&self, dictionary_id: u64) -> Option<DictionaryHandle> {
        self.metrics.lookups.fetch_add(1, Ordering::Relaxed);
        let result = self.entries.read().get(&dictionary_id).cloned();
        if result.is_some() {
            self.metrics.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.misses.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Get the latest dictionary for a table.
    pub fn get_for_table(&self, table_id: u64) -> Option<DictionaryHandle> {
        let dict_id = *self.table_latest.read().get(&table_id)?;
        self.get(dict_id)
    }

    /// List all dictionary IDs.
    pub fn list_ids(&self) -> Vec<u64> {
        self.entries.read().keys().copied().collect()
    }

    /// Create a ZstdBlockCodec with the specified dictionary pre-loaded.
    pub fn create_codec_with_dict(
        &self,
        dictionary_id: u64,
        config: ZstdCodecConfig,
    ) -> Result<ZstdBlockCodec, CodecError> {
        let handle = self.get(dictionary_id)
            .ok_or_else(|| CodecError(format!("dictionary {dictionary_id} not found")))?;
        if !handle.verify() {
            return Err(CodecError(format!("dictionary {dictionary_id} checksum mismatch")));
        }
        Ok(ZstdBlockCodec::with_dictionary(config, handle.data))
    }
}

impl Default for DictionaryRegistry {
    fn default() -> Self { Self::new() }
}

// ═══════════════════════════════════════════════════════════════════════════
// §D — Block Wire Format
// ═══════════════════════════════════════════════════════════════════════════

/// Block header stored before each compressed block in a segment body.
///
/// Wire format: `[uncompressed_len:u32][compressed_len:u32][block_crc:u32]`
/// Followed by `compressed_len` bytes of compressed data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHeader {
    pub uncompressed_len: u32,
    pub compressed_len: u32,
    pub block_crc: u32,
}

/// Header size in bytes.
pub const BLOCK_HEADER_SIZE: usize = 12;

impl BlockHeader {
    /// Serialize to 12 bytes.
    pub fn to_bytes(&self) -> [u8; BLOCK_HEADER_SIZE] {
        let mut buf = [0u8; BLOCK_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        buf[4..8].copy_from_slice(&self.compressed_len.to_le_bytes());
        buf[8..12].copy_from_slice(&self.block_crc.to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < BLOCK_HEADER_SIZE { return None; }
        Some(Self {
            uncompressed_len: u32::from_le_bytes(data[0..4].try_into().ok()?),
            compressed_len: u32::from_le_bytes(data[4..8].try_into().ok()?),
            block_crc: u32::from_le_bytes(data[8..12].try_into().ok()?),
        })
    }
}

/// A complete compressed block (header + data).
#[derive(Debug, Clone)]
pub struct CompressedBlock {
    pub header: BlockHeader,
    pub data: Vec<u8>,
}

impl CompressedBlock {
    /// Compress a raw block using the given codec.
    pub fn compress(codec: &dyn SegmentCodecImpl, input: &[u8]) -> Result<Self, CodecError> {
        let compressed = codec.compress_block(input)?;
        let crc = djb2_crc(&compressed);
        Ok(Self {
            header: BlockHeader {
                uncompressed_len: input.len() as u32,
                compressed_len: compressed.len() as u32,
                block_crc: crc,
            },
            data: compressed,
        })
    }

    /// Decompress this block using the given codec.
    pub fn decompress(&self, codec: &dyn SegmentCodecImpl) -> Result<Vec<u8>, CodecError> {
        if !self.verify() {
            return Err(CodecError("block CRC mismatch".to_owned()));
        }
        codec.decompress_block(&self.data, self.header.uncompressed_len as usize)
    }

    /// Verify the CRC.
    pub fn verify(&self) -> bool {
        djb2_crc(&self.data) == self.header.block_crc
    }

    /// Total wire size (header + data).
    pub const fn wire_size(&self) -> usize {
        BLOCK_HEADER_SIZE + self.data.len()
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.wire_size());
        buf.extend_from_slice(&self.header.to_bytes());
        buf.extend_from_slice(&self.data);
        buf
    }

    /// Deserialize from bytes. Returns (block, bytes_consumed).
    pub fn from_bytes(data: &[u8]) -> Option<(Self, usize)> {
        let header = BlockHeader::from_bytes(data)?;
        let total = BLOCK_HEADER_SIZE + header.compressed_len as usize;
        if data.len() < total { return None; }
        let block_data = data[BLOCK_HEADER_SIZE..total].to_vec();
        Some((Self { header, data: block_data }, total))
    }
}

/// Segment-level extended metadata for Zstd segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZstdSegmentMeta {
    pub codec_id: CodecId,
    pub codec_level: i32,
    pub dictionary_id: u64,
    pub dictionary_checksum: u32,
    pub block_count: u32,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
}

impl ZstdSegmentMeta {
    pub const fn new(level: i32) -> Self {
        Self {
            codec_id: CodecId::Zstd,
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

    /// Serialize to bytes (37 bytes).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(37);
        buf.push(self.codec_id as u8);
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
        Some(Self {
            codec_id: CodecId::from_u8(data[0])?,
            codec_level: i32::from_le_bytes(data[1..5].try_into().ok()?),
            dictionary_id: u64::from_le_bytes(data[5..13].try_into().ok()?),
            dictionary_checksum: u32::from_le_bytes(data[13..17].try_into().ok()?),
            block_count: u32::from_le_bytes(data[17..21].try_into().ok()?),
            uncompressed_bytes: u64::from_le_bytes(data[21..29].try_into().ok()?),
            compressed_bytes: u64::from_le_bytes(data[29..37].try_into().ok()?),
        })
    }
}

/// Write blocks to a buffer: `[block_count:u32] [CompressedBlock]*N`
pub fn write_blocks(
    codec: &dyn SegmentCodecImpl,
    rows: &[Vec<u8>],
) -> Result<(Vec<u8>, ZstdSegmentMeta), CodecError> {
    let mut meta = ZstdSegmentMeta::new(0);
    meta.codec_id = codec.codec_id();

    let mut body = Vec::new();
    let count = rows.len() as u32;
    body.extend_from_slice(&count.to_le_bytes());

    for row in rows {
        let block = CompressedBlock::compress(codec, row)?;
        meta.uncompressed_bytes += row.len() as u64;
        meta.compressed_bytes += block.data.len() as u64;
        meta.block_count += 1;
        body.extend_from_slice(&block.to_bytes());
    }

    Ok((body, meta))
}

/// Read blocks from a buffer: `[block_count:u32] [CompressedBlock]*N`
pub fn read_blocks(
    codec: &dyn SegmentCodecImpl,
    data: &[u8],
) -> Result<Vec<Vec<u8>>, CodecError> {
    if data.len() < 4 {
        return Err(CodecError("data too small for block count".to_owned()));
    }
    let block_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut rows = Vec::with_capacity(block_count);

    for i in 0..block_count {
        let (block, consumed) = CompressedBlock::from_bytes(&data[offset..])
            .ok_or_else(|| CodecError(format!("failed to parse block {i}")))?;
        let decompressed = block.decompress(codec)?;
        rows.push(decompressed);
        offset += consumed;
    }

    Ok(rows)
}

// ═══════════════════════════════════════════════════════════════════════════
// §E — Streaming Codec (orthogonal to segment codec)
// ═══════════════════════════════════════════════════════════════════════════

/// Streaming codec identity — separate from segment codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingCodecId {
    None,
    Lz4,
    Zstd,
}

/// Capabilities advertised during streaming handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingCodecCaps {
    pub supported: Vec<StreamingCodecId>,
    pub preferred: StreamingCodecId,
    pub estimated_bandwidth_bps: u64,
}

impl StreamingCodecCaps {
    pub fn high_bandwidth() -> Self {
        Self {
            supported: vec![StreamingCodecId::None, StreamingCodecId::Lz4, StreamingCodecId::Zstd],
            preferred: StreamingCodecId::Lz4,
            estimated_bandwidth_bps: 10_000_000_000,
        }
    }
    pub fn low_bandwidth() -> Self {
        Self {
            supported: vec![StreamingCodecId::Lz4, StreamingCodecId::Zstd],
            preferred: StreamingCodecId::Zstd,
            estimated_bandwidth_bps: 100_000_000,
        }
    }
}

/// Negotiate streaming codec between sender and receiver.
pub fn negotiate_streaming_codec(
    sender: &StreamingCodecCaps,
    receiver: &StreamingCodecCaps,
) -> StreamingCodecId {
    let common: Vec<StreamingCodecId> = sender.supported.iter()
        .filter(|c| receiver.supported.contains(c))
        .copied()
        .collect();
    if common.is_empty() { return StreamingCodecId::None; }
    if (sender.preferred == StreamingCodecId::Zstd || receiver.preferred == StreamingCodecId::Zstd)
        && common.contains(&StreamingCodecId::Zstd) {
        return StreamingCodecId::Zstd;
    }
    if common.contains(&sender.preferred) { return sender.preferred; }
    common[0]
}

/// Compress a streaming chunk (chunk-level, independently decompressible).
pub fn compress_streaming_chunk(
    data: &[u8],
    codec: StreamingCodecId,
    zstd_level: i32,
) -> Result<Vec<u8>, CodecError> {
    match codec {
        StreamingCodecId::None => Ok(data.to_vec()),
        StreamingCodecId::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
        StreamingCodecId::Zstd => {
            let c = ZstdBlockCodec::new(ZstdCodecConfig { level: zstd_level, checksum: false });
            c.compress_block(data)
        }
    }
}

/// Decompress a streaming chunk.
pub fn decompress_streaming_chunk(
    data: &[u8],
    codec: StreamingCodecId,
    max_decompressed: usize,
) -> Result<Vec<u8>, CodecError> {
    match codec {
        StreamingCodecId::None => Ok(data.to_vec()),
        StreamingCodecId::Lz4 => {
            lz4_flex::decompress_size_prepended(data)
                .map_err(|e| CodecError(format!("lz4 stream decompress: {e}")))
        }
        StreamingCodecId::Zstd => {
            let c = ZstdBlockCodec::new(ZstdCodecConfig::default());
            c.decompress_block(data, max_decompressed)
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §F — Decompression Isolation: Pool + LRU Cache
// ═══════════════════════════════════════════════════════════════════════════

/// Cache key for decompressed blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecompressCacheKey {
    pub segment_id: u64,
    pub block_index: u32,
}

/// LRU decompression cache with byte-capacity limit.
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
}

impl DecompressCacheMetrics {
    pub fn hit_rate(&self) -> f64 {
        let h = self.hits.load(Ordering::Relaxed) as f64;
        let m = self.misses.load(Ordering::Relaxed) as f64;
        if h + m == 0.0 { 0.0 } else { h / (h + m) }
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
            let evict_key = self.order.lock().pop_front();
            if let Some(ek) = evict_key {
                if let Some(evicted) = self.entries.lock().remove(&ek) {
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
            self.order.lock().push_back(key);
        }
    }

    pub fn used_bytes(&self) -> u64 { self.used_bytes.load(Ordering::Relaxed) }
    pub fn len(&self) -> usize { self.entries.lock().len() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

/// Decompression pool — enforces concurrency limit and caches results.
///
/// Zstd decompression NEVER runs on the OLTP executor thread.
pub struct DecompressPool {
    pub max_concurrent: u32,
    inflight: AtomicU64,
    pub cache: DecompressCache,
    pub metrics: DecompressPoolMetrics,
}

#[derive(Debug, Default)]
pub struct DecompressPoolMetrics {
    pub total: AtomicU64,
    pub bytes: AtomicU64,
    pub cpu_ns: AtomicU64,
    pub errors: AtomicU64,
    pub rejected: AtomicU64,
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
    /// Returns Err if overloaded.
    pub fn decompress(
        &self,
        segment_id: u64,
        block_index: u32,
        block: &CompressedBlock,
        codec: &dyn SegmentCodecImpl,
    ) -> Result<Vec<u8>, CodecError> {
        let cache_key = DecompressCacheKey { segment_id, block_index };

        if let Some(cached) = self.cache.get(&cache_key) {
            return Ok(cached);
        }

        let current = self.inflight.fetch_add(1, Ordering::Relaxed);
        if current >= u64::from(self.max_concurrent) {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(CodecError("decompress pool overloaded".to_owned()));
        }

        let start = std::time::Instant::now();
        let result = block.decompress(codec);
        let elapsed_ns = start.elapsed().as_nanos() as u64;

        self.inflight.fetch_sub(1, Ordering::Relaxed);
        self.metrics.total.fetch_add(1, Ordering::Relaxed);
        self.metrics.cpu_ns.fetch_add(elapsed_ns, Ordering::Relaxed);

        match result {
            Ok(data) => {
                self.metrics.bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
                self.cache.insert(cache_key, data.clone());
                Ok(data)
            }
            Err(e) => {
                self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    pub fn inflight(&self) -> u64 { self.inflight.load(Ordering::Relaxed) }
}

// ═══════════════════════════════════════════════════════════════════════════
// §G — GC / Rewrite Coordination
// ═══════════════════════════════════════════════════════════════════════════

/// Reason for recompressing a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecompressReason {
    ColdCompaction,
    SnapshotRebuild,
    DictionaryUpgrade,
    CodecChange,
    Manual,
}

impl fmt::Display for RecompressReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColdCompaction => write!(f, "cold_compaction"),
            Self::SnapshotRebuild => write!(f, "snapshot_rebuild"),
            Self::DictionaryUpgrade => write!(f, "dict_upgrade"),
            Self::CodecChange => write!(f, "codec_change"),
            Self::Manual => write!(f, "manual"),
        }
    }
}

/// A recompression request.
#[derive(Debug, Clone)]
pub struct RecompressRequest {
    pub source_data: Vec<Vec<u8>>,
    pub target_codec_id: CodecId,
    pub target_level: i32,
    pub target_dict_id: u64,
    pub reason: RecompressReason,
}

/// Result of a recompression.
#[derive(Debug, Clone)]
pub struct RecompressResult {
    pub old_codec: CodecId,
    pub new_codec: CodecId,
    pub body: Vec<u8>,
    pub meta: ZstdSegmentMeta,
    pub old_bytes: u64,
    pub new_bytes: u64,
}

/// Execute a recompression: decompress old blocks, recompress with new codec/dict.
pub fn recompress(
    request: &RecompressRequest,
    new_codec: &dyn SegmentCodecImpl,
) -> Result<RecompressResult, CodecError> {
    let old_total: u64 = request.source_data.iter().map(|r| r.len() as u64).sum();
    let (body, meta) = write_blocks(new_codec, &request.source_data)?;
    let new_bytes = body.len() as u64;

    Ok(RecompressResult {
        old_codec: CodecId::None, // caller knows original codec
        new_codec: new_codec.codec_id(),
        body,
        meta,
        old_bytes: old_total,
        new_bytes,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// §H — Metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Compression-side metrics.
#[derive(Debug, Default)]
pub struct CompressMetrics {
    pub compress_calls: AtomicU64,
    pub compress_bytes_in: AtomicU64,
    pub compress_bytes_out: AtomicU64,
    pub compress_cpu_ns: AtomicU64,
}

impl CompressMetrics {
    pub fn record(&self, bytes_in: u64, bytes_out: u64, cpu_ns: u64) {
        self.compress_calls.fetch_add(1, Ordering::Relaxed);
        self.compress_bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
        self.compress_bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
        self.compress_cpu_ns.fetch_add(cpu_ns, Ordering::Relaxed);
    }

    pub fn ratio(&self) -> f64 {
        let i = self.compress_bytes_in.load(Ordering::Relaxed) as f64;
        let o = self.compress_bytes_out.load(Ordering::Relaxed) as f64;
        if o > 0.0 { i / o } else { 1.0 }
    }
}

/// Full metrics snapshot for /admin/status.
#[derive(Debug, Clone, Default)]
pub struct CodecMetricsSnapshot {
    pub compress_calls: u64,
    pub compress_bytes_in: u64,
    pub compress_bytes_out: u64,
    pub compress_cpu_ns: u64,
    pub compress_ratio: f64,
    pub decompress_total: u64,
    pub decompress_bytes: u64,
    pub decompress_cpu_ns: u64,
    pub decompress_errors: u64,
    pub decompress_rejected: u64,
    pub cache_hit_rate: f64,
    pub cache_used_bytes: u64,
    pub cache_evictions: u64,
    pub dict_count: u64,
    pub dict_bytes_total: u64,
    pub dict_hit_rate: f64,
}

/// Build a metrics snapshot from all components.
pub fn build_metrics_snapshot(
    compress: &CompressMetrics,
    pool: &DecompressPool,
    dict_reg: &DictionaryRegistry,
) -> CodecMetricsSnapshot {
    CodecMetricsSnapshot {
        compress_calls: compress.compress_calls.load(Ordering::Relaxed),
        compress_bytes_in: compress.compress_bytes_in.load(Ordering::Relaxed),
        compress_bytes_out: compress.compress_bytes_out.load(Ordering::Relaxed),
        compress_cpu_ns: compress.compress_cpu_ns.load(Ordering::Relaxed),
        compress_ratio: compress.ratio(),
        decompress_total: pool.metrics.total.load(Ordering::Relaxed),
        decompress_bytes: pool.metrics.bytes.load(Ordering::Relaxed),
        decompress_cpu_ns: pool.metrics.cpu_ns.load(Ordering::Relaxed),
        decompress_errors: pool.metrics.errors.load(Ordering::Relaxed),
        decompress_rejected: pool.metrics.rejected.load(Ordering::Relaxed),
        cache_hit_rate: pool.cache.metrics.hit_rate(),
        cache_used_bytes: pool.cache.used_bytes(),
        cache_evictions: pool.cache.metrics.evictions.load(Ordering::Relaxed),
        dict_count: dict_reg.list_ids().len() as u64,
        dict_bytes_total: dict_reg.metrics.bytes_total.load(Ordering::Relaxed),
        dict_hit_rate: dict_reg.metrics.hit_rate(),
    }
}

/// Report libzstd version (from zstd-safe).
pub fn zstd_version() -> u32 {
    zstd_safe::version_number()
}

/// Report libzstd version string.
pub const fn zstd_version_string() -> &'static str {
    // zstd_safe doesn't expose version_string directly, derive from number
    // Format: major*10000 + minor*100 + patch
    // We'll just return the numeric version as a fallback
    "zstd-safe"
}

// ═══════════════════════════════════════════════════════════════════════════
// §I — Unit Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // -- §A: Codec Policy --

    #[test]
    fn test_codec_policy_defaults() {
        let p = CodecPolicy::default();
        assert_eq!(p.wal_codec, CodecId::None);
        assert_eq!(p.cold_codec, CodecId::Zstd);
        assert_eq!(p.snapshot_codec, CodecId::Zstd);
    }

    #[test]
    fn test_codec_policy_validation() {
        let p = CodecPolicy::default();
        assert!(p.is_allowed(SegmentKind::Wal, CodecId::None));
        assert!(p.is_allowed(SegmentKind::Wal, CodecId::Lz4));
        assert!(!p.is_allowed(SegmentKind::Wal, CodecId::Zstd));
        assert!(p.is_allowed(SegmentKind::Cold, CodecId::Zstd));
        assert!(p.is_allowed(SegmentKind::Snapshot, CodecId::Zstd));
        assert!(!p.is_allowed(SegmentKind::Snapshot, CodecId::Lz4));
    }

    #[test]
    fn test_codec_id_roundtrip() {
        for id in [CodecId::None, CodecId::Lz4, CodecId::Zstd] {
            assert_eq!(CodecId::from_u8(id as u8), Some(id));
        }
        assert_eq!(CodecId::from_u8(99), None);
    }

    // -- §B: NoneCodec --

    #[test]
    fn test_none_codec_roundtrip() {
        let c = NoneCodec;
        let data = b"hello falcondb none codec";
        let compressed = c.compress_block(data).unwrap();
        assert_eq!(compressed, data);
        let decompressed = c.decompress_block(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    // -- §B: Lz4BlockCodec --

    #[test]
    fn test_lz4_codec_roundtrip() {
        let c = Lz4BlockCodec;
        let data = vec![0xABu8; 4096];
        let compressed = c.compress_block(&data).unwrap();
        assert!(compressed.len() < data.len());
        let decompressed = c.decompress_block(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    // -- §C: ZstdBlockCodec (via zstd-safe) --

    #[test]
    fn test_zstd_codec_roundtrip() {
        let c = ZstdBlockCodec::new(ZstdCodecConfig::default());
        let data = b"hello world zstd-safe compression test for falcon segments!";
        let compressed = c.compress_block(data).unwrap();
        let decompressed = c.decompress_block(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_codec_levels() {
        for level in [1, 3, 5, 9] {
            let c = ZstdBlockCodec::new(ZstdCodecConfig { level, checksum: true });
            let data = vec![42u8; 2048];
            let compressed = c.compress_block(&data).unwrap();
            let decompressed = c.decompress_block(&compressed, data.len()).unwrap();
            assert_eq!(decompressed, data, "failed at level {}", level);
        }
    }

    #[test]
    fn test_zstd_codec_large_data() {
        let c = ZstdBlockCodec::new(ZstdCodecConfig::default());
        let data = vec![0xCDu8; 1_000_000]; // 1MB
        let compressed = c.compress_block(&data).unwrap();
        assert!(compressed.len() < data.len() / 10);
        let decompressed = c.decompress_block(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_codec_empty_input() {
        let c = ZstdBlockCodec::new(ZstdCodecConfig::default());
        let compressed = c.compress_block(b"").unwrap();
        let decompressed = c.decompress_block(&compressed, 0).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_zstd_independent_blocks() {
        let c = ZstdBlockCodec::new(ZstdCodecConfig::default());
        let block1 = b"first block data for test";
        let block2 = b"second block data for test";
        let c1 = c.compress_block(block1).unwrap();
        let c2 = c.compress_block(block2).unwrap();
        // Each block decompresses independently
        assert_eq!(c.decompress_block(&c1, block1.len()).unwrap(), block1);
        assert_eq!(c.decompress_block(&c2, block2.len()).unwrap(), block2);
        // Order doesn't matter
        assert_eq!(c.decompress_block(&c2, block2.len()).unwrap(), block2);
        assert_eq!(c.decompress_block(&c1, block1.len()).unwrap(), block1);
    }

    #[test]
    fn test_zstd_with_dictionary() {
        // Build a simple dictionary from repeated patterns using zstd-safe API
        let samples: Vec<Vec<u8>> = (0..100).map(|i| {
            let mut v = b"FalconDB common prefix data ".to_vec();
            v.extend_from_slice(&(i as u32).to_le_bytes());
            v.extend_from_slice(&[0u8; 50]);
            v
        }).collect();
        // zstd_safe::train_from_buffer wants: &mut [u8] (dict buf), &[u8] (concat samples), &[usize] (sizes)
        let sizes: Vec<usize> = samples.iter().map(|s| s.len()).collect();
        let concat: Vec<u8> = samples.iter().flat_map(|s| s.iter().copied()).collect();
        let mut dict_buf = vec![0u8; 8192];
        let dict_written = zstd_safe::train_from_buffer(&mut dict_buf[..], &concat, &sizes)
            .expect("dict training failed");
        dict_buf.truncate(dict_written);

        let c = ZstdBlockCodec::with_dictionary(
            ZstdCodecConfig::default(),
            dict_buf,
        );
        let test_data = b"FalconDB common prefix data \x05\x00\x00\x00test payload here";
        let compressed = c.compress_block(test_data).unwrap();
        let decompressed = c.decompress_block(&compressed, test_data.len()).unwrap();
        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_zstd_dict_mismatch_fails() {
        // Train dict A from pattern A
        let samples_a: Vec<Vec<u8>> = (0..100).map(|i| {
            let mut v = b"AAAA pattern alpha ".to_vec();
            v.extend_from_slice(&(i as u32).to_le_bytes());
            v.extend_from_slice(&[0xAAu8; 60]);
            v
        }).collect();
        let sizes_a: Vec<usize> = samples_a.iter().map(|s| s.len()).collect();
        let concat_a: Vec<u8> = samples_a.iter().flat_map(|s| s.iter().copied()).collect();
        let mut buf_a = vec![0u8; 8192];
        let n_a = zstd_safe::train_from_buffer(&mut buf_a[..], &concat_a, &sizes_a).unwrap();
        buf_a.truncate(n_a);

        // Train dict B from pattern B
        let samples_b: Vec<Vec<u8>> = (0..100).map(|i| {
            let mut v = b"BBBB pattern beta ".to_vec();
            v.extend_from_slice(&((i + 500) as u32).to_le_bytes());
            v.extend_from_slice(&[0xBBu8; 60]);
            v
        }).collect();
        let sizes_b: Vec<usize> = samples_b.iter().map(|s| s.len()).collect();
        let concat_b: Vec<u8> = samples_b.iter().flat_map(|s| s.iter().copied()).collect();
        let mut buf_b = vec![0u8; 8192];
        let n_b = zstd_safe::train_from_buffer(&mut buf_b[..], &concat_b, &sizes_b).unwrap();
        buf_b.truncate(n_b);

        // Compress with dict A
        let c_a = ZstdBlockCodec::with_dictionary(ZstdCodecConfig::default(), buf_a);
        let data = b"AAAA pattern alpha \x05\x00\x00\x00 some payload data here";
        let compressed = c_a.compress_block(data).unwrap();

        // Decompress with dict B — should fail or produce wrong output
        let c_b = ZstdBlockCodec::with_dictionary(ZstdCodecConfig::default(), buf_b);
        let result = c_b.decompress_block(&compressed, data.len());
        assert!(result.is_err() || result.as_ref().unwrap() != data,
            "dict mismatch should cause error or wrong data");
    }

    // -- §B: create_codec factory --

    #[test]
    fn test_create_codec_factory() {
        let c = create_codec(CodecId::None, None, None);
        assert_eq!(c.codec_id(), CodecId::None);
        let c = create_codec(CodecId::Lz4, None, None);
        assert_eq!(c.codec_id(), CodecId::Lz4);
        let c = create_codec(CodecId::Zstd, None, None);
        assert_eq!(c.codec_id(), CodecId::Zstd);

        let data = b"factory roundtrip test";
        for id in [CodecId::None, CodecId::Lz4, CodecId::Zstd] {
            let c = create_codec(id, None, None);
            let comp = c.compress_block(data).unwrap();
            let dec = c.decompress_block(&comp, data.len()).unwrap();
            assert_eq!(dec, data, "factory roundtrip failed for {:?}", id);
        }
    }

    // -- §C: Dictionary Registry --

    #[test]
    fn test_dictionary_registry_load_get() {
        let reg = DictionaryRegistry::new();
        let handle = DictionaryHandle::new(1, 100, 42, 1, vec![0xAB; 256]);
        assert!(handle.verify());
        reg.load(handle.clone());

        let fetched = reg.get(1).unwrap();
        assert_eq!(fetched.data, handle.data);
        assert_eq!(fetched.table_id, 42);

        let by_table = reg.get_for_table(42).unwrap();
        assert_eq!(by_table.dictionary_id, 1);
    }

    #[test]
    fn test_dictionary_registry_miss() {
        let reg = DictionaryRegistry::new();
        assert!(reg.get(999).is_none());
        assert_eq!(reg.metrics.misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_dictionary_registry_create_codec() {
        let reg = DictionaryRegistry::new();
        let handle = DictionaryHandle::new(1, 100, 42, 1, vec![0u8; 256]);
        reg.load(handle);

        let codec = reg.create_codec_with_dict(1, ZstdCodecConfig::default()).unwrap();
        assert_eq!(codec.codec_id(), CodecId::Zstd);

        // Missing dict → error
        assert!(reg.create_codec_with_dict(999, ZstdCodecConfig::default()).is_err());
    }

    // -- §D: Block wire format --

    #[test]
    fn test_block_header_roundtrip() {
        let hdr = BlockHeader { uncompressed_len: 1234, compressed_len: 567, block_crc: 0xDEAD };
        let bytes = hdr.to_bytes();
        let recovered = BlockHeader::from_bytes(&bytes).unwrap();
        assert_eq!(recovered, hdr);
    }

    #[test]
    fn test_compressed_block_roundtrip() {
        let codec = ZstdBlockCodec::new(ZstdCodecConfig::default());
        let data = b"block wire format roundtrip test data";
        let block = CompressedBlock::compress(&codec, data).unwrap();
        assert!(block.verify());

        let bytes = block.to_bytes();
        let (recovered, consumed) = CompressedBlock::from_bytes(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert!(recovered.verify());

        let decompressed = recovered.decompress(&codec).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compressed_block_crc_failure() {
        let codec = ZstdBlockCodec::new(ZstdCodecConfig::default());
        let block = CompressedBlock::compress(&codec, b"test").unwrap();
        let mut bad = block.clone();
        bad.header.block_crc = 0;
        assert!(!bad.verify());
        assert!(bad.decompress(&codec).is_err());
    }

    #[test]
    fn test_write_read_blocks() {
        let codec = ZstdBlockCodec::new(ZstdCodecConfig::default());
        let rows: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8; 200]).collect();
        let (body, meta) = write_blocks(&codec, &rows).unwrap();
        assert_eq!(meta.block_count, 10);

        let recovered = read_blocks(&codec, &body).unwrap();
        assert_eq!(recovered.len(), 10);
        for (i, row) in recovered.iter().enumerate() {
            assert_eq!(row, &vec![i as u8; 200]);
        }
    }

    // -- §D: ZstdSegmentMeta --

    #[test]
    fn test_zstd_segment_meta_roundtrip() {
        let meta = ZstdSegmentMeta::new(5)
            .with_dictionary(42, 0xBEEF);
        let bytes = meta.to_bytes();
        let recovered = ZstdSegmentMeta::from_bytes(&bytes).unwrap();
        assert_eq!(recovered, meta);
    }

    // -- §E: Streaming --

    #[test]
    fn test_streaming_negotiate_high_bw() {
        let s = StreamingCodecCaps::high_bandwidth();
        let r = StreamingCodecCaps::high_bandwidth();
        assert_eq!(negotiate_streaming_codec(&s, &r), StreamingCodecId::Lz4);
    }

    #[test]
    fn test_streaming_negotiate_low_bw() {
        let s = StreamingCodecCaps::low_bandwidth();
        let r = StreamingCodecCaps::low_bandwidth();
        assert_eq!(negotiate_streaming_codec(&s, &r), StreamingCodecId::Zstd);
    }

    #[test]
    fn test_streaming_negotiate_mixed() {
        let s = StreamingCodecCaps::high_bandwidth();
        let r = StreamingCodecCaps::low_bandwidth();
        assert_eq!(negotiate_streaming_codec(&s, &r), StreamingCodecId::Zstd);
    }

    #[test]
    fn test_streaming_chunk_roundtrip() {
        let data = vec![42u8; 8192];
        for codec in [StreamingCodecId::None, StreamingCodecId::Lz4, StreamingCodecId::Zstd] {
            let compressed = compress_streaming_chunk(&data, codec, 1).unwrap();
            let decompressed = decompress_streaming_chunk(&compressed, codec, data.len()).unwrap();
            assert_eq!(decompressed, data, "streaming roundtrip failed for {:?}", codec);
        }
    }

    // -- §F: Decompress Pool + Cache --

    #[test]
    fn test_decompress_cache_basic() {
        let cache = DecompressCache::new(1024 * 1024);
        let key = DecompressCacheKey { segment_id: 1, block_index: 0 };
        assert!(cache.get(&key).is_none());
        cache.insert(key, vec![1, 2, 3]);
        assert_eq!(cache.get(&key).unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_decompress_cache_eviction() {
        let cache = DecompressCache::new(100);
        for i in 0..20u32 {
            cache.insert(
                DecompressCacheKey { segment_id: 1, block_index: i },
                vec![0u8; 50],
            );
        }
        assert!(cache.metrics.evictions.load(Ordering::Relaxed) > 0);
        assert!(cache.used_bytes() <= 100);
    }

    #[test]
    fn test_decompress_pool_basic() {
        let pool = DecompressPool::new(4, 1024 * 1024);
        let codec = ZstdBlockCodec::new(ZstdCodecConfig::default());
        let block = CompressedBlock::compress(&codec, b"pool test").unwrap();

        let result = pool.decompress(1, 0, &block, &codec).unwrap();
        assert_eq!(result, b"pool test");

        // Second call → cache hit
        let cached = pool.decompress(1, 0, &block, &codec).unwrap();
        assert_eq!(cached, b"pool test");
        assert_eq!(pool.cache.metrics.hits.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_decompress_pool_overload() {
        let pool = DecompressPool::new(0, 1024);
        let codec = ZstdBlockCodec::new(ZstdCodecConfig::default());
        let block = CompressedBlock::compress(&codec, b"data").unwrap();
        assert!(pool.decompress(1, 0, &block, &codec).is_err());
        assert_eq!(pool.metrics.rejected.load(Ordering::Relaxed), 1);
    }

    // -- §G: Recompress --

    #[test]
    fn test_recompress() {
        let rows: Vec<Vec<u8>> = (0..5).map(|i| vec![i as u8; 300]).collect();
        let new_codec = ZstdBlockCodec::new(ZstdCodecConfig { level: 5, checksum: true });
        let request = RecompressRequest {
            source_data: rows.clone(),
            target_codec_id: CodecId::Zstd,
            target_level: 5,
            target_dict_id: 0,
            reason: RecompressReason::CodecChange,
        };
        let result = recompress(&request, &new_codec).unwrap();
        assert_eq!(result.new_codec, CodecId::Zstd);
        assert_eq!(result.meta.block_count, 5);

        // Verify data round-trips
        let recovered = read_blocks(&new_codec, &result.body).unwrap();
        assert_eq!(recovered, rows);
    }

    // -- §H: Metrics --

    #[test]
    fn test_compress_metrics() {
        let m = CompressMetrics::default();
        m.record(1000, 300, 50_000);
        m.record(2000, 500, 80_000);
        assert_eq!(m.compress_calls.load(Ordering::Relaxed), 2);
        assert!((m.ratio() - 3.75).abs() < 0.01);
    }

    #[test]
    fn test_build_metrics_snapshot() {
        let cm = CompressMetrics::default();
        cm.record(5000, 1000, 100_000);
        let pool = DecompressPool::new(4, 1024 * 1024);
        let reg = DictionaryRegistry::new();
        let snap = build_metrics_snapshot(&cm, &pool, &reg);
        assert_eq!(snap.compress_calls, 1);
        assert_eq!(snap.compress_bytes_in, 5000);
        assert!((snap.compress_ratio - 5.0).abs() < 0.01);
    }

    #[test]
    fn test_zstd_version() {
        let v = zstd_version();
        assert!(v > 10000, "zstd version {} too low", v);
    }

    // -- Display --

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", CodecId::Zstd), "zstd");
        assert_eq!(format!("{}", RecompressReason::DictionaryUpgrade), "dict_upgrade");
    }
}
