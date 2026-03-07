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

use std::fmt;

use crate::unified_data_plane::{
    SegmentCodec, SegmentKind, SegmentStore, SegmentStoreError, UnifiedSegmentHeader,
    UNIFIED_HEADER_SIZE,
};
use crate::unified_data_plane_full::ColdBlockEncoding;

pub use crate::zstd_dict::{DictionaryEntry, DictionaryMetrics, DictionaryStore};
pub use crate::zstd_recompress::{
    build_zstd_metrics, list_segments_by_codec, recompress_segment, RecompressReason,
    RecompressRequest, RecompressResult, SegmentCodecInfo, ZstdCompressMetrics,
    ZstdMetricsSnapshot,
};
pub use crate::zstd_streaming::{
    compress_streaming_chunk, decompress_streaming_chunk, negotiate_streaming_codec,
    DecompressCache, DecompressCacheKey, DecompressCacheMetrics, DecompressPool,
    DecompressPoolMetrics, StreamingCodecCaps,
};

// Compression Scope & Codec Policy
/// Codec policy configuration — which codec is allowed/required where.
///
/// Hard constraints:
/// - [X] Active WAL tail: NEVER zstd
/// - [X] OLTP hot row / MVCC visibility: NEVER zstd
/// - [OK] COLD_SEGMENT: zstd (default) or lz4
/// - [OK] SNAPSHOT_SEGMENT: zstd (forced)
/// - [OK] Segment Streaming: adaptive (none/lz4/zstd)
/// - [OK] Sealed WAL long-term: optional lz4
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

// ----------------------------------------------------------------
// Part B — Segment-Level Zstd: Block Compression / Decompression
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
        if self.compressed_bytes == 0 {
            return 1.0;
        }
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
        if data.len() < 37 {
            return None;
        }
        let codec = SegmentCodec::from_u8(data[0])?;
        let codec_level = i32::from_le_bytes(data[1..5].try_into().ok()?);
        let dictionary_id = u64::from_le_bytes(data[5..13].try_into().ok()?);
        let dictionary_checksum = u32::from_le_bytes(data[13..17].try_into().ok()?);
        let block_count = u32::from_le_bytes(data[17..21].try_into().ok()?);
        let uncompressed_bytes = u64::from_le_bytes(data[21..29].try_into().ok()?);
        let compressed_bytes = u64::from_le_bytes(data[29..37].try_into().ok()?);
        Some(Self {
            codec,
            codec_level,
            dictionary_id,
            dictionary_checksum,
            block_count,
            uncompressed_bytes,
            compressed_bytes,
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
    for &b in data {
        hash = hash.wrapping_mul(33).wrapping_add(u32::from(b));
    }
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
        if data.len() < 13 {
            return None;
        } // min: 4+4+1+0+4
        let uncompressed_len = u32::from_le_bytes(data[0..4].try_into().ok()?);
        let compressed_len = u32::from_le_bytes(data[4..8].try_into().ok()?) as usize;
        let encoding = ColdBlockEncoding::from_u8(data[8])?;
        let total = 9 + compressed_len + 4;
        if data.len() < total {
            return None;
        }
        let compressed_data = data[9..9 + compressed_len].to_vec();
        let crc = u32::from_le_bytes(data[9 + compressed_len..total].try_into().ok()?);
        Some((
            Self {
                uncompressed_len,
                compressed_len: compressed_len as u32,
                encoding,
                compressed_data,
                crc,
            },
            total,
        ))
    }

    /// Verify CRC.
    pub fn verify(&self) -> bool {
        self.crc == djb2_crc(&self.compressed_data)
    }
}

/// Compress a raw block using Zstd.
pub fn zstd_compress_block(
    data: &[u8],
    level: i32,
    dict: Option<&[u8]>,
) -> Result<ZstdBlock, String> {
    let compressed = if let Some(dict_data) = dict {
        let mut compressor = zstd::bulk::Compressor::with_dictionary(level, dict_data)
            .map_err(|e| format!("zstd init compressor with dict: {e}"))?;
        compressor
            .compress(data)
            .map_err(|e| format!("zstd dict compress: {e}"))?
    } else {
        zstd::bulk::compress(data, level).map_err(|e| format!("zstd compress: {e}"))?
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
        decompressor
            .decompress(&block.compressed_data, capacity)
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
pub fn compress_block(
    data: &[u8],
    codec: SegmentCodec,
    level: i32,
    dict: Option<&[u8]>,
) -> Result<ZstdBlock, String> {
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

// ----------------------------------------------------------------
// Part B2 — Zstd Segment Writer / Reader
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
    let hdr = UnifiedSegmentHeader::new_cold(
        seg_id,
        256 * 1024 * 1024,
        SegmentCodec::Zstd,
        table_id,
        shard_id,
    );
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
        let block = zstd_compress_block(row, level, dict).map_err(SegmentStoreError::IoError)?;
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
    let body = store
        .get_segment_body(segment_id)
        .map_err(|e| format!("read segment: {e}"))?;

    if body.len() < UNIFIED_HEADER_SIZE as usize {
        return Err("segment too small".to_owned());
    }

    let data = &body[UNIFIED_HEADER_SIZE as usize..];
    if data.len() < 4 {
        return Err("segment body too small for block count".to_owned());
    }
    let block_count = u32::from_le_bytes(
        data[0..4]
            .try_into()
            .expect("infallible: 4-byte slice to [u8;4]"),
    ) as usize;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structured_lsn::StructuredLsn;
    use crate::unified_data_plane::{LogicalRange, ManifestEntry};

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
        assert!(policy.validate_codec(SegmentKind::Wal, SegmentCodec::None));
        assert!(policy.validate_codec(SegmentKind::Wal, SegmentCodec::Lz4));
        assert!(!policy.validate_codec(SegmentKind::Wal, SegmentCodec::Zstd));
        assert!(policy.validate_codec(SegmentKind::Cold, SegmentCodec::Zstd));
        assert!(policy.validate_codec(SegmentKind::Cold, SegmentCodec::Lz4));
        assert!(policy.validate_codec(SegmentKind::Snapshot, SegmentCodec::Zstd));
        assert!(!policy.validate_codec(SegmentKind::Snapshot, SegmentCodec::Lz4));
    }

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
        let b = compress_block(data, SegmentCodec::None, 0, None).unwrap();
        assert_eq!(decompress_block(&b, None).unwrap(), data);
        let b = compress_block(data, SegmentCodec::Lz4, 0, None).unwrap();
        assert_eq!(decompress_block(&b, None).unwrap(), data);
        let b = compress_block(data, SegmentCodec::Zstd, 3, None).unwrap();
        assert_eq!(decompress_block(&b, None).unwrap(), data);
    }

    #[test]
    fn test_zstd_compression_ratio() {
        let data = vec![0xABu8; 10_000];
        let block = zstd_compress_block(&data, 3, None).unwrap();
        let ratio = data.len() as f64 / block.compressed_data.len() as f64;
        assert!(ratio > 2.0, "expected ratio > 2.0, got {:.2}", ratio);
    }

    #[test]
    fn test_write_read_zstd_cold_segment() {
        let store = SegmentStore::new();
        let rows: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8; 200]).collect();
        let (seg_id, meta) = write_zstd_cold_segment(&store, 1, 0, &rows, 3, None, 0).unwrap();
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
        let meta = ZstdSegmentMeta::new(5).with_dictionary(42, 0xDEAD);
        let bytes = meta.to_bytes();
        let recovered = ZstdSegmentMeta::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.codec_level, 5);
        assert_eq!(recovered.dictionary_id, 42);
        assert_eq!(recovered.dictionary_checksum, 0xDEAD);
    }

    #[test]
    fn test_display_streaming_codec_policy() {
        use crate::zstd_streaming::StreamingCodecCaps;
        let hi = StreamingCodecCaps::high_bandwidth();
        let lo = StreamingCodecCaps::low_bandwidth();
        assert_eq!(hi.preferred, SegmentCodec::Lz4);
        assert_eq!(lo.preferred, SegmentCodec::Zstd);
    }

    #[test]
    fn test_list_segments_by_codec() {
        use crate::zstd_recompress::list_segments_by_codec;
        let mut m = crate::unified_data_plane::Manifest::new();
        m.add_segment(ManifestEntry {
            segment_id: 0,
            kind: SegmentKind::Wal,
            size_bytes: 100,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::ZERO,
                end_lsn: StructuredLsn::ZERO,
            },
            sealed: true,
        });
        m.add_segment(ManifestEntry {
            segment_id: 1,
            kind: SegmentKind::Cold,
            size_bytes: 200,
            codec: SegmentCodec::Zstd,
            logical_range: LogicalRange::Cold {
                table_id: 1,
                shard_id: 0,
                min_key: vec![],
                max_key: vec![],
            },
            sealed: true,
        });
        let all = list_segments_by_codec(&m, None);
        assert_eq!(all.len(), 2);
        let zstd_only = list_segments_by_codec(&m, Some(SegmentCodec::Zstd));
        assert_eq!(zstd_only.len(), 1);
        assert_eq!(zstd_only[0].segment_id, 1);
    }
}
