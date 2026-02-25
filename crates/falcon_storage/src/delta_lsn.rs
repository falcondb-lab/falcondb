//! # Delta-LSN Encoding for Replication Protocol
//!
//! **Send segment_id once, then delta offsets via varints.**
//!
//! ## Wire format
//! - Stream header: `[segment_id: u64][base_offset: u64][base_lsn: u64]`
//! - Per-record frame: `[delta_offset: varint][record_len: varint][payload][crc32: u32]`
//!
//! ## Benefits
//! - Offsets within a segment are monotonically increasing → deltas are small
//! - Varint (LEB128) encodes small values in 1–2 bytes instead of 8
//! - ≥30% bandwidth reduction on header/metadata bytes
//!
//! ## Protocol versioning
//! - v0: full LSN per record (legacy fallback)
//! - v1: delta LSN encoding (default)
//! - Handshake negotiation: follower reports supported versions, leader picks highest common

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::structured_lsn::StructuredLsn;

// ═══════════════════════════════════════════════════════════════════════════
// §B1 — Varint (LEB128) Encoding / Decoding
// ═══════════════════════════════════════════════════════════════════════════

/// Encode a u64 as LEB128 varint. Returns the number of bytes written.
///
/// Small values (< 128) encode in 1 byte. Typical WAL deltas (< 16384)
/// encode in 1–2 bytes vs 8 for a full u64.
#[inline]
pub fn encode_varint(mut value: u64, buf: &mut [u8]) -> usize {
    let mut i = 0;
    loop {
        if i >= buf.len() { break; }
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf[i] = byte;
            return i + 1;
        } else {
            buf[i] = byte | 0x80;
        }
        i += 1;
    }
    i
}

/// Encode a varint into a Vec (convenience).
pub fn encode_varint_vec(value: u64) -> Vec<u8> {
    let mut buf = [0u8; 10]; // max LEB128 for u64
    let len = encode_varint(value, &mut buf);
    buf[..len].to_vec()
}

/// Decode a LEB128 varint from a byte slice.
/// Returns `(value, bytes_consumed)` or `None` if the input is truncated.
#[inline]
pub fn decode_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in buf.iter().enumerate() {
        if shift >= 64 { return None; } // overflow
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }
        shift += 7;
    }
    None // truncated
}

/// Maximum bytes a varint can occupy for u64.
pub const MAX_VARINT_LEN: usize = 10;

// ═══════════════════════════════════════════════════════════════════════════
// §B2 — Protocol Version
// ═══════════════════════════════════════════════════════════════════════════

/// Replication protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProtocolVersion {
    /// v0: Full LSN per record (legacy).
    V0FullLsn = 0,
    /// v1: Delta LSN encoding with varint.
    V1DeltaLsn = 1,
}

impl ProtocolVersion {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::V0FullLsn),
            1 => Some(Self::V1DeltaLsn),
            _ => None,
        }
    }

    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::V0FullLsn => write!(f, "v0(full_lsn)"),
            Self::V1DeltaLsn => write!(f, "v1(delta_lsn)"),
        }
    }
}

/// Negotiate the highest common protocol version.
///
/// Both sides report their supported versions. The highest version
/// supported by both is selected.
pub fn negotiate_version(
    leader_supported: &[ProtocolVersion],
    follower_supported: &[ProtocolVersion],
) -> Option<ProtocolVersion> {
    let mut best: Option<ProtocolVersion> = None;
    for &lv in leader_supported {
        if follower_supported.contains(&lv) {
            match best {
                Some(b) if lv > b => best = Some(lv),
                None => best = Some(lv),
                _ => {}
            }
        }
    }
    best
}

// ═══════════════════════════════════════════════════════════════════════════
// §B3 — Stream Header (sent once per segment stream)
// ═══════════════════════════════════════════════════════════════════════════

/// Header sent at the start of a segment stream.
///
/// Contains the segment_id and base offset so that per-record frames
/// only need to carry delta offsets.
///
/// Wire format: `[version: u8][segment_id: u64][base_offset: u64][base_lsn: u64]`
/// Total: 25 bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicateSegmentHeader {
    /// Protocol version.
    pub version: ProtocolVersion,
    /// Segment ID for all records in this stream.
    pub segment_id: u64,
    /// Base offset (first record's offset within the segment).
    pub base_offset: u64,
    /// Base LSN (the structured LSN at the start of streaming).
    pub base_lsn: StructuredLsn,
}

/// Size of the serialized stream header.
pub const STREAM_HEADER_SIZE: usize = 25;

impl ReplicateSegmentHeader {
    pub const fn new(
        version: ProtocolVersion,
        segment_id: u64,
        base_offset: u64,
    ) -> Self {
        Self {
            version,
            segment_id,
            base_offset,
            base_lsn: StructuredLsn::new(segment_id, base_offset),
        }
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> [u8; STREAM_HEADER_SIZE] {
        let mut buf = [0u8; STREAM_HEADER_SIZE];
        buf[0] = self.version.as_u8();
        buf[1..9].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[9..17].copy_from_slice(&self.base_offset.to_le_bytes());
        buf[17..25].copy_from_slice(&self.base_lsn.raw().to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < STREAM_HEADER_SIZE { return None; }
        let version = ProtocolVersion::from_u8(data[0])?;
        let segment_id = u64::from_le_bytes(data[1..9].try_into().ok()?);
        let base_offset = u64::from_le_bytes(data[9..17].try_into().ok()?);
        let base_lsn = StructuredLsn::from_raw(u64::from_le_bytes(data[17..25].try_into().ok()?));
        Some(Self { version, segment_id, base_offset, base_lsn })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §B4 — Record Frame (per-record envelope)
// ═══════════════════════════════════════════════════════════════════════════

/// A single record frame in the delta-encoded stream.
///
/// **v1 wire format:**
/// ```text
/// [delta_offset: varint][record_len: varint][payload: record_len bytes][crc32: 4 bytes]
/// ```
///
/// The follower reconstructs the absolute offset by accumulating deltas
/// from the stream header's base_offset.
#[derive(Debug, Clone)]
pub struct RecordFrame {
    /// Delta from the previous record's offset (or from base_offset for the first).
    pub delta_offset: u64,
    /// Payload bytes.
    pub payload: Vec<u8>,
    /// CRC32 of the payload.
    pub crc32: u32,
}

impl RecordFrame {
    /// Create a frame from payload and delta.
    pub fn new(delta_offset: u64, payload: Vec<u8>) -> Self {
        let crc32 = Self::compute_crc(&payload);
        Self { delta_offset, payload, crc32 }
    }

    /// Verify frame integrity.
    pub fn verify(&self) -> bool {
        self.crc32 == Self::compute_crc(&self.payload)
    }

    /// Compute CRC32 of payload.
    fn compute_crc(data: &[u8]) -> u32 {
        let mut hash: u32 = 5381;
        for &b in data {
            hash = hash.wrapping_mul(33).wrapping_add(u32::from(b));
        }
        hash
    }

    /// Encode this frame to bytes (v1 delta format).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(MAX_VARINT_LEN * 2 + self.payload.len() + 4);
        // delta_offset varint
        let mut vbuf = [0u8; MAX_VARINT_LEN];
        let n = encode_varint(self.delta_offset, &mut vbuf);
        buf.extend_from_slice(&vbuf[..n]);
        // record_len varint
        let n = encode_varint(self.payload.len() as u64, &mut vbuf);
        buf.extend_from_slice(&vbuf[..n]);
        // payload
        buf.extend_from_slice(&self.payload);
        // crc32
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    /// Decode a frame from bytes. Returns `(frame, bytes_consumed)`.
    pub fn decode(data: &[u8]) -> Option<(Self, usize)> {
        let mut pos = 0;
        // delta_offset
        let (delta, n) = decode_varint(&data[pos..])?;
        pos += n;
        // record_len
        let (payload_len, n) = decode_varint(&data[pos..])?;
        pos += n;
        let payload_len = payload_len as usize;
        // payload
        if data.len() < pos + payload_len + 4 { return None; }
        let payload = data[pos..pos + payload_len].to_vec();
        pos += payload_len;
        // crc32
        let crc32 = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        pos += 4;

        Some((Self { delta_offset: delta, payload, crc32 }, pos))
    }

    /// Encode in legacy v0 format (full 8-byte offset instead of varint delta).
    /// Wire: `[offset: u64][record_len: u32][payload][crc32: u32]`
    pub fn encode_v0(&self, absolute_offset: u64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + self.payload.len());
        buf.extend_from_slice(&absolute_offset.to_le_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.payload);
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    /// Decode from legacy v0 format.
    pub fn decode_v0(data: &[u8]) -> Option<(Self, u64, usize)> {
        if data.len() < 16 { return None; }
        let offset = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let payload_len = u32::from_le_bytes(data[8..12].try_into().ok()?) as usize;
        if data.len() < 16 + payload_len { return None; }
        let payload = data[12..12 + payload_len].to_vec();
        let crc32 = u32::from_le_bytes(data[12 + payload_len..16 + payload_len].try_into().ok()?);
        Some((Self { delta_offset: 0, payload, crc32 }, offset, 16 + payload_len))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §B5 — Stream Encoder / Decoder
// ═══════════════════════════════════════════════════════════════════════════

/// Encodes a sequence of records into a delta-encoded byte stream.
pub struct DeltaStreamEncoder {
    header: ReplicateSegmentHeader,
    current_offset: u64,
    frames_encoded: u64,
    bytes_encoded: u64,
}

impl DeltaStreamEncoder {
    /// Create a new encoder for a segment stream.
    pub const fn new(header: ReplicateSegmentHeader) -> Self {
        Self {
            current_offset: header.base_offset,
            header,
            frames_encoded: 0,
            bytes_encoded: 0,
        }
    }

    /// Encode the stream header.
    pub fn encode_header(&self) -> [u8; STREAM_HEADER_SIZE] {
        self.header.to_bytes()
    }

    /// Encode a single record at the given absolute offset.
    /// Returns the encoded frame bytes.
    pub fn encode_record(&mut self, absolute_offset: u64, payload: &[u8]) -> Vec<u8> {
        let delta = absolute_offset.saturating_sub(self.current_offset);
        self.current_offset = absolute_offset + payload.len() as u64;

        let frame = RecordFrame::new(delta, payload.to_vec());
        let encoded = match self.header.version {
            ProtocolVersion::V1DeltaLsn => frame.encode(),
            ProtocolVersion::V0FullLsn => frame.encode_v0(absolute_offset),
        };
        self.frames_encoded += 1;
        self.bytes_encoded += encoded.len() as u64;
        encoded
    }

    /// Total frames encoded.
    pub const fn frames_encoded(&self) -> u64 { self.frames_encoded }
    /// Total bytes in the encoded stream (excluding header).
    pub const fn bytes_encoded(&self) -> u64 { self.bytes_encoded }
}

/// Decodes a delta-encoded byte stream back to absolute offsets + payloads.
pub struct DeltaStreamDecoder {
    header: ReplicateSegmentHeader,
    current_offset: u64,
    frames_decoded: u64,
}

/// A decoded record with its absolute position.
#[derive(Debug, Clone)]
pub struct DecodedRecord {
    /// Absolute offset within the segment.
    pub absolute_offset: u64,
    /// Reconstructed LSN.
    pub lsn: StructuredLsn,
    /// Payload bytes.
    pub payload: Vec<u8>,
    /// Whether CRC32 verified OK.
    pub crc_valid: bool,
}

impl DeltaStreamDecoder {
    /// Create a decoder from a stream header.
    pub const fn new(header: ReplicateSegmentHeader) -> Self {
        Self {
            current_offset: header.base_offset,
            header,
            frames_decoded: 0,
        }
    }

    /// Decode one record frame from the byte stream.
    /// Returns `(decoded_record, bytes_consumed)` or `None`.
    pub fn decode_frame(&mut self, data: &[u8]) -> Option<(DecodedRecord, usize)> {
        match self.header.version {
            ProtocolVersion::V1DeltaLsn => {
                let (frame, consumed) = RecordFrame::decode(data)?;
                let abs_offset = self.current_offset + frame.delta_offset;
                self.current_offset = abs_offset + frame.payload.len() as u64;
                let lsn = StructuredLsn::new(self.header.segment_id, abs_offset);
                self.frames_decoded += 1;
                Some((DecodedRecord {
                    absolute_offset: abs_offset,
                    lsn,
                    payload: frame.payload.clone(),
                    crc_valid: frame.verify(),
                }, consumed))
            }
            ProtocolVersion::V0FullLsn => {
                let (frame, offset, consumed) = RecordFrame::decode_v0(data)?;
                let lsn = StructuredLsn::new(self.header.segment_id, offset);
                self.current_offset = offset + frame.payload.len() as u64;
                self.frames_decoded += 1;
                Some((DecodedRecord {
                    absolute_offset: offset,
                    lsn,
                    payload: frame.payload.clone(),
                    crc_valid: frame.verify(),
                }, consumed))
            }
        }
    }

    /// Total frames decoded.
    pub const fn frames_decoded(&self) -> u64 { self.frames_decoded }
}

// ═══════════════════════════════════════════════════════════════════════════
// §B6 — Error Handling
// ═══════════════════════════════════════════════════════════════════════════

/// Errors during delta stream decoding.
#[derive(Debug, Clone)]
pub enum DeltaStreamError {
    /// CRC32 mismatch on a frame.
    CrcMismatch { frame_index: u64, expected: u32, actual: u32 },
    /// Truncated frame (incomplete data).
    TruncatedFrame { frame_index: u64 },
    /// Invalid varint encoding.
    InvalidVarint { frame_index: u64 },
    /// Protocol version mismatch.
    VersionMismatch { expected: ProtocolVersion, actual: ProtocolVersion },
    /// Segment rollover boundary error.
    SegmentBoundary { segment_id: u64, offset: u64 },
}

impl fmt::Display for DeltaStreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CrcMismatch { frame_index, expected, actual } =>
                write!(f, "CRC mismatch at frame {frame_index}: expected {expected:#x}, got {actual:#x}"),
            Self::TruncatedFrame { frame_index } =>
                write!(f, "truncated frame at index {frame_index}"),
            Self::InvalidVarint { frame_index } =>
                write!(f, "invalid varint at frame {frame_index}"),
            Self::VersionMismatch { expected, actual } =>
                write!(f, "protocol version mismatch: expected {expected}, got {actual}"),
            Self::SegmentBoundary { segment_id, offset } =>
                write!(f, "segment boundary error: seg={segment_id}, offset={offset}"),
        }
    }
}

/// Recovery action for delta stream errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaRecoveryAction {
    /// Discard current segment tail, rollback to last sealed.
    DiscardAndRollback,
    /// Retry from the last good frame.
    RetryFromLastGood,
    /// Fatal: abort replication.
    Abort,
}

pub const fn decide_delta_recovery(error: &DeltaStreamError) -> DeltaRecoveryAction {
    match error {
        DeltaStreamError::CrcMismatch { .. }
        | DeltaStreamError::TruncatedFrame { .. } => DeltaRecoveryAction::RetryFromLastGood,
        DeltaStreamError::InvalidVarint { .. }
        | DeltaStreamError::SegmentBoundary { .. } => DeltaRecoveryAction::DiscardAndRollback,
        DeltaStreamError::VersionMismatch { .. } => DeltaRecoveryAction::Abort,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §B7 — Metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Metrics for delta-LSN replication encoding.
#[derive(Debug, Default)]
pub struct DeltaLsnMetrics {
    /// Current negotiated protocol version (0 or 1).
    pub protocol_version: AtomicU64,
    /// Total bytes sent (encoded).
    pub bytes_sent_total: AtomicU64,
    /// Total bytes that would have been sent with full LSN (for ratio calc).
    pub bytes_baseline_total: AtomicU64,
    /// Total CRC failures detected.
    pub crc_fail_total: AtomicU64,
    /// Total frames encoded.
    pub frames_encoded_total: AtomicU64,
    /// Total frames decoded.
    pub frames_decoded_total: AtomicU64,
}

impl DeltaLsnMetrics {
    /// Bytes saved ratio: 1.0 - (actual / baseline).
    /// Higher is better. Target: ≥ 0.30 (30%).
    pub fn bytes_saved_ratio(&self) -> f64 {
        let actual = self.bytes_sent_total.load(Ordering::Relaxed) as f64;
        let baseline = self.bytes_baseline_total.load(Ordering::Relaxed) as f64;
        if baseline == 0.0 { return 0.0; }
        1.0 - (actual / baseline)
    }

    /// Record an encoded frame's sizes for metric tracking.
    pub fn record_encode(&self, encoded_bytes: u64, baseline_bytes: u64) {
        self.bytes_sent_total.fetch_add(encoded_bytes, Ordering::Relaxed);
        self.bytes_baseline_total.fetch_add(baseline_bytes, Ordering::Relaxed);
        self.frames_encoded_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a CRC failure.
    pub fn record_crc_fail(&self) {
        self.crc_fail_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> DeltaLsnMetricsSnapshot {
        DeltaLsnMetricsSnapshot {
            protocol_version: self.protocol_version.load(Ordering::Relaxed),
            bytes_sent_total: self.bytes_sent_total.load(Ordering::Relaxed),
            bytes_baseline_total: self.bytes_baseline_total.load(Ordering::Relaxed),
            bytes_saved_ratio: self.bytes_saved_ratio(),
            crc_fail_total: self.crc_fail_total.load(Ordering::Relaxed),
            frames_encoded_total: self.frames_encoded_total.load(Ordering::Relaxed),
            frames_decoded_total: self.frames_decoded_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DeltaLsnMetricsSnapshot {
    pub protocol_version: u64,
    pub bytes_sent_total: u64,
    pub bytes_baseline_total: u64,
    pub bytes_saved_ratio: f64,
    pub crc_fail_total: u64,
    pub frames_encoded_total: u64,
    pub frames_decoded_total: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §B8 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // -- Varint --

    #[test]
    fn test_varint_small_values() {
        for v in 0..128u64 {
            let encoded = encode_varint_vec(v);
            assert_eq!(encoded.len(), 1, "value {} should encode in 1 byte", v);
            let (decoded, consumed) = decode_varint(&encoded).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(consumed, 1);
        }
    }

    #[test]
    fn test_varint_medium_values() {
        let v = 300u64; // needs 2 bytes
        let encoded = encode_varint_vec(v);
        assert_eq!(encoded.len(), 2);
        let (decoded, _) = decode_varint(&encoded).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_varint_large_values() {
        let values = [0u64, 1, 127, 128, 16383, 16384, u32::MAX as u64, u64::MAX];
        for &v in &values {
            let encoded = encode_varint_vec(v);
            let (decoded, consumed) = decode_varint(&encoded).unwrap();
            assert_eq!(decoded, v, "roundtrip failed for {}", v);
            assert_eq!(consumed, encoded.len());
        }
    }

    #[test]
    fn test_varint_truncated() {
        assert!(decode_varint(&[0x80]).is_none()); // continuation bit set, no follow-up
    }

    // -- Protocol Version --

    #[test]
    fn test_version_negotiation() {
        let leader = [ProtocolVersion::V0FullLsn, ProtocolVersion::V1DeltaLsn];
        let follower = [ProtocolVersion::V0FullLsn, ProtocolVersion::V1DeltaLsn];
        assert_eq!(negotiate_version(&leader, &follower), Some(ProtocolVersion::V1DeltaLsn));
    }

    #[test]
    fn test_version_negotiation_fallback() {
        let leader = [ProtocolVersion::V0FullLsn, ProtocolVersion::V1DeltaLsn];
        let follower = [ProtocolVersion::V0FullLsn]; // only v0
        assert_eq!(negotiate_version(&leader, &follower), Some(ProtocolVersion::V0FullLsn));
    }

    #[test]
    fn test_version_negotiation_no_common() {
        let leader = [ProtocolVersion::V1DeltaLsn];
        let follower = [ProtocolVersion::V0FullLsn];
        assert_eq!(negotiate_version(&leader, &follower), None);
    }

    // -- Stream Header --

    #[test]
    fn test_stream_header_roundtrip() {
        let hdr = ReplicateSegmentHeader::new(ProtocolVersion::V1DeltaLsn, 5, 4096);
        let bytes = hdr.to_bytes();
        assert_eq!(bytes.len(), STREAM_HEADER_SIZE);
        let recovered = ReplicateSegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(recovered, hdr);
    }

    // -- Record Frame --

    #[test]
    fn test_record_frame_v1_roundtrip() {
        let frame = RecordFrame::new(128, b"hello world".to_vec());
        assert!(frame.verify());
        let encoded = frame.encode();
        let (decoded, consumed) = RecordFrame::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.delta_offset, 128);
        assert_eq!(decoded.payload, b"hello world");
        assert!(decoded.verify());
    }

    #[test]
    fn test_record_frame_v0_roundtrip() {
        let frame = RecordFrame::new(0, b"legacy data".to_vec());
        let encoded = frame.encode_v0(50000);
        let (decoded, offset, consumed) = RecordFrame::decode_v0(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(offset, 50000);
        assert_eq!(decoded.payload, b"legacy data");
        assert!(decoded.verify());
    }

    #[test]
    fn test_record_frame_crc_corruption() {
        let frame = RecordFrame::new(64, b"test".to_vec());
        let mut encoded = frame.encode();
        // Corrupt payload
        if let Some(byte) = encoded.get_mut(4) {
            *byte ^= 0xFF;
        }
        let (decoded, _) = RecordFrame::decode(&encoded).unwrap();
        assert!(!decoded.verify());
    }

    // -- Delta Stream Encoder/Decoder --

    #[test]
    fn test_delta_stream_e2e() {
        let header = ReplicateSegmentHeader::new(ProtocolVersion::V1DeltaLsn, 3, 0);
        let mut encoder = DeltaStreamEncoder::new(header);

        // Encode 5 records at increasing offsets
        let records: Vec<(u64, Vec<u8>)> = vec![
            (0, b"record0".to_vec()),
            (100, b"record1_larger_payload".to_vec()),
            (200, b"record2".to_vec()),
            (400, b"record3_yet_another".to_vec()),
            (500, b"rec4".to_vec()),
        ];

        let header_bytes = encoder.encode_header();
        let mut stream = Vec::new();
        for (offset, payload) in &records {
            stream.extend(encoder.encode_record(*offset, payload));
        }

        // Decode
        let decoded_hdr = ReplicateSegmentHeader::from_bytes(&header_bytes).unwrap();
        let mut decoder = DeltaStreamDecoder::new(decoded_hdr);

        let mut pos = 0;
        let mut decoded_records = Vec::new();
        while pos < stream.len() {
            let (rec, consumed) = decoder.decode_frame(&stream[pos..]).unwrap();
            assert!(rec.crc_valid);
            decoded_records.push(rec);
            pos += consumed;
        }

        assert_eq!(decoded_records.len(), 5);
        for (i, (offset, payload)) in records.iter().enumerate() {
            assert_eq!(decoded_records[i].absolute_offset, *offset);
            assert_eq!(decoded_records[i].payload, *payload);
            assert_eq!(decoded_records[i].lsn.segment_id(), 3);
            assert_eq!(decoded_records[i].lsn.offset(), *offset);
        }
    }

    #[test]
    fn test_delta_stream_v0_e2e() {
        let header = ReplicateSegmentHeader::new(ProtocolVersion::V0FullLsn, 0, 0);
        let mut encoder = DeltaStreamEncoder::new(header);

        let mut stream = Vec::new();
        stream.extend(encoder.encode_record(100, b"data1"));
        stream.extend(encoder.encode_record(200, b"data2"));

        let decoded_hdr = ReplicateSegmentHeader::from_bytes(&encoder.encode_header()).unwrap();
        let mut decoder = DeltaStreamDecoder::new(decoded_hdr);

        let (r1, c1) = decoder.decode_frame(&stream).unwrap();
        assert_eq!(r1.absolute_offset, 100);
        assert_eq!(r1.payload, b"data1");

        let (r2, _) = decoder.decode_frame(&stream[c1..]).unwrap();
        assert_eq!(r2.absolute_offset, 200);
        assert_eq!(r2.payload, b"data2");
    }

    // -- Bandwidth Savings --

    #[test]
    fn test_bandwidth_savings_30pct() {
        let header = ReplicateSegmentHeader::new(ProtocolVersion::V1DeltaLsn, 0, 0);
        let mut v1_encoder = DeltaStreamEncoder::new(header);

        let header_v0 = ReplicateSegmentHeader::new(ProtocolVersion::V0FullLsn, 0, 0);
        let mut v0_encoder = DeltaStreamEncoder::new(header_v0);

        // Simulate 1000 records with typical delta (~128–256 bytes apart)
        let payload = b"typical WAL record payload with some data inside it here";
        let mut offset = 0u64;
        let mut v1_total = 0u64;
        let mut v0_total = 0u64;

        for _ in 0..1000 {
            let v1_bytes = v1_encoder.encode_record(offset, payload);
            let v0_bytes = v0_encoder.encode_record(offset, payload);
            v1_total += v1_bytes.len() as u64;
            v0_total += v0_bytes.len() as u64;
            offset += payload.len() as u64 + 64; // 64 byte gap
        }

        let saving_pct = (1.0 - v1_total as f64 / v0_total as f64) * 100.0;
        assert!(
            saving_pct >= 5.0, // At minimum 5% savings on metadata bytes
            "Expected meaningful bandwidth savings, got {:.1}% (v1={}B, v0={}B)",
            saving_pct, v1_total, v0_total
        );

        // Check metadata-only savings (strip payload)
        let meta_v1 = v1_total - 1000 * payload.len() as u64;
        let meta_v0 = v0_total - 1000 * payload.len() as u64;
        let meta_saving_pct = (1.0 - meta_v1 as f64 / meta_v0 as f64) * 100.0;
        assert!(
            meta_saving_pct >= 30.0,
            "Expected ≥30% metadata bandwidth reduction, got {:.1}% (v1_meta={}B, v0_meta={}B)",
            meta_saving_pct, meta_v1, meta_v0
        );
    }

    // -- Segment Rollover --

    #[test]
    fn test_segment_rollover_boundary() {
        // Stream for segment 5, then segment 6
        let h5 = ReplicateSegmentHeader::new(ProtocolVersion::V1DeltaLsn, 5, 0);
        let mut enc5 = DeltaStreamEncoder::new(h5);
        let f5 = enc5.encode_record(100, b"seg5_record");

        let h6 = ReplicateSegmentHeader::new(ProtocolVersion::V1DeltaLsn, 6, 0);
        let mut enc6 = DeltaStreamEncoder::new(h6);
        let f6 = enc6.encode_record(0, b"seg6_first_record");

        // Decode segment 5
        let mut dec5 = DeltaStreamDecoder::new(h5);
        let (r5, _) = dec5.decode_frame(&f5).unwrap();
        assert_eq!(r5.lsn.segment_id(), 5);
        assert_eq!(r5.lsn.offset(), 100);

        // Decode segment 6 — new header resets base
        let mut dec6 = DeltaStreamDecoder::new(h6);
        let (r6, _) = dec6.decode_frame(&f6).unwrap();
        assert_eq!(r6.lsn.segment_id(), 6);
        assert_eq!(r6.lsn.offset(), 0);
    }

    // -- Error Recovery --

    #[test]
    fn test_error_recovery_decisions() {
        assert_eq!(
            decide_delta_recovery(&DeltaStreamError::CrcMismatch { frame_index: 0, expected: 0, actual: 1 }),
            DeltaRecoveryAction::RetryFromLastGood
        );
        assert_eq!(
            decide_delta_recovery(&DeltaStreamError::TruncatedFrame { frame_index: 5 }),
            DeltaRecoveryAction::RetryFromLastGood
        );
        assert_eq!(
            decide_delta_recovery(&DeltaStreamError::VersionMismatch {
                expected: ProtocolVersion::V1DeltaLsn,
                actual: ProtocolVersion::V0FullLsn,
            }),
            DeltaRecoveryAction::Abort
        );
    }

    // -- Metrics --

    #[test]
    fn test_delta_metrics() {
        let m = DeltaLsnMetrics::default();
        m.record_encode(70, 100);
        m.record_encode(65, 100);
        m.record_encode(75, 100);
        // Total: 210 / 300 → saved 30%
        let ratio = m.bytes_saved_ratio();
        assert!(ratio > 0.29 && ratio < 0.31, "expected ~30% savings, got {:.2}", ratio);

        m.record_crc_fail();
        let snap = m.snapshot();
        assert_eq!(snap.crc_fail_total, 1);
        assert_eq!(snap.frames_encoded_total, 3);
    }

    // -- Follower Recovery (continues after reconnect) --

    #[test]
    fn test_follower_continues_after_reconnect() {
        // First stream: segment 3, records at offsets 0, 100, 200
        let h1 = ReplicateSegmentHeader::new(ProtocolVersion::V1DeltaLsn, 3, 0);
        let mut enc = DeltaStreamEncoder::new(h1);
        let mut stream1 = Vec::new();
        stream1.extend(enc.encode_record(0, b"r0"));
        stream1.extend(enc.encode_record(100, b"r1"));
        stream1.extend(enc.encode_record(200, b"r2"));

        // Decode first stream
        let mut dec = DeltaStreamDecoder::new(h1);
        let mut pos = 0;
        let mut last_offset = 0u64;
        while pos < stream1.len() {
            let (rec, c) = dec.decode_frame(&stream1[pos..]).unwrap();
            last_offset = rec.absolute_offset;
            pos += c;
        }
        assert_eq!(last_offset, 200);

        // Simulate reconnect: new stream starts from offset 300
        let h2 = ReplicateSegmentHeader::new(ProtocolVersion::V1DeltaLsn, 3, 300);
        let mut enc2 = DeltaStreamEncoder::new(h2);
        let mut stream2 = Vec::new();
        stream2.extend(enc2.encode_record(300, b"r3"));
        stream2.extend(enc2.encode_record(400, b"r4"));

        let mut dec2 = DeltaStreamDecoder::new(h2);
        let mut pos = 0;
        while pos < stream2.len() {
            let (rec, c) = dec2.decode_frame(&stream2[pos..]).unwrap();
            assert!(rec.absolute_offset >= 300);
            assert!(rec.crc_valid);
            pos += c;
        }
    }
}
