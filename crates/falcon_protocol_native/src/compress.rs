//! Compression support for the native protocol.
//!
//! When compression is negotiated (via feature flags in the handshake),
//! message payloads larger than `MIN_COMPRESS_SIZE` are compressed before
//! framing. The frame header gains a 1-byte compression tag after the
//! standard 5-byte header:
//!
//! ```text
//! [msg_type: u8][length: u32 LE][compress_tag: u8][payload...]
//! ```
//!
//! - `compress_tag = 0x00` → payload is uncompressed (length = raw payload size)
//! - `compress_tag = 0x01` → payload is LZ4-compressed; first 4 bytes of payload
//!   are the uncompressed size (LE u32), followed by compressed data
//!
//! This module provides a software-only LZ4-style compressor for small payloads.
//! For production use, replace the inner functions with `lz4-flex` or similar.

use bytes::{BufMut, BytesMut};

use crate::error::NativeProtocolError;
use crate::types::FRAME_HEADER_SIZE;

/// Compression algorithm identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionAlgo {
    None = 0x00,
    Lz4 = 0x01,
}

impl CompressionAlgo {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x00 => Some(Self::None),
            0x01 => Some(Self::Lz4),
            _ => None,
        }
    }
}

/// Minimum payload size to attempt compression (below this, overhead > savings).
pub const MIN_COMPRESS_SIZE: usize = 256;

/// Extended frame header size when compression is active: standard 5 + 1 tag byte.
pub const COMPRESSED_FRAME_HEADER_SIZE: usize = FRAME_HEADER_SIZE + 1;

/// Compress a payload using the specified algorithm.
/// Returns `(compressed_payload_with_header, algo_used)`.
///
/// If the payload is too small or compression doesn't shrink it,
/// returns the original payload with `CompressionAlgo::None`.
pub fn compress_payload(data: &[u8], algo: CompressionAlgo) -> (Vec<u8>, CompressionAlgo) {
    if algo == CompressionAlgo::None || data.len() < MIN_COMPRESS_SIZE {
        return (data.to_vec(), CompressionAlgo::None);
    }

    match algo {
        CompressionAlgo::Lz4 => {
            let compressed = lz4_compress(data);
            // Only use compressed if it actually saves space (+ 4 bytes for uncompressed size header)
            if compressed.len() + 4 < data.len() {
                let mut out = Vec::with_capacity(4 + compressed.len());
                out.extend_from_slice(&(data.len() as u32).to_le_bytes());
                out.extend_from_slice(&compressed);
                (out, CompressionAlgo::Lz4)
            } else {
                (data.to_vec(), CompressionAlgo::None)
            }
        }
        CompressionAlgo::None => (data.to_vec(), CompressionAlgo::None),
    }
}

/// Decompress a payload based on the compression tag.
pub fn decompress_payload(
    data: &[u8],
    algo: CompressionAlgo,
) -> Result<Vec<u8>, NativeProtocolError> {
    match algo {
        CompressionAlgo::None => Ok(data.to_vec()),
        CompressionAlgo::Lz4 => {
            if data.len() < 4 {
                return Err(NativeProtocolError::Truncated {
                    expected: 4,
                    actual: data.len(),
                });
            }
            let uncompressed_size =
                u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            if uncompressed_size > 64 * 1024 * 1024 {
                return Err(NativeProtocolError::FrameTooLarge {
                    size: uncompressed_size as u32,
                    max: 64 * 1024 * 1024,
                });
            }
            let compressed = &data[4..];
            lz4_decompress(compressed, uncompressed_size).map_err(|e| {
                NativeProtocolError::Corruption(format!("LZ4 decompression failed: {}", e))
            })
        }
    }
}

/// Encode a message with optional compression.
/// Returns the full frame bytes including the compression tag.
pub fn encode_compressed_frame(msg_type: u8, payload: &[u8], algo: CompressionAlgo) -> BytesMut {
    let (compressed, actual_algo) = compress_payload(payload, algo);
    let frame_len = 1 + compressed.len(); // 1 byte tag + compressed payload
    let mut frame = BytesMut::with_capacity(FRAME_HEADER_SIZE + frame_len);
    frame.put_u8(msg_type);
    frame.put_u32_le(frame_len as u32);
    frame.put_u8(actual_algo as u8);
    frame.put_slice(&compressed);
    frame
}

/// Decode a compressed frame. Input must start at the compression tag byte
/// (i.e., after the standard 5-byte frame header has been consumed).
/// Returns the decompressed payload.
pub fn decode_compressed_payload(tag_and_payload: &[u8]) -> Result<Vec<u8>, NativeProtocolError> {
    if tag_and_payload.is_empty() {
        return Err(NativeProtocolError::Truncated {
            expected: 1,
            actual: 0,
        });
    }
    let tag = tag_and_payload[0];
    let algo = CompressionAlgo::from_byte(tag).ok_or_else(|| {
        NativeProtocolError::Corruption(format!("unknown compression tag: 0x{:02x}", tag))
    })?;
    decompress_payload(&tag_and_payload[1..], algo)
}

// ── Minimal LZ4-compatible compression ───────────────────────────────
//
// This is a simplified implementation for protocol development.
// For production, replace with `lz4-flex` crate.

fn lz4_compress(input: &[u8]) -> Vec<u8> {
    // Simple RLE-style compression as a placeholder.
    // Real LZ4 would use hash-based match finding.
    let mut output = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        // Look for a run of identical bytes
        let start = i;
        let b = input[i];
        i += 1;
        while i < input.len() && input[i] == b && (i - start) < 255 {
            i += 1;
        }
        let run_len = i - start;
        if run_len >= 4 {
            // Encode as: 0xFF marker, byte, count
            output.push(0xFF);
            output.push(b);
            output.push(run_len as u8);
        } else {
            // Literal bytes
            for &byte in &input[start..i] {
                if byte == 0xFF {
                    // Escape the marker
                    output.push(0xFF);
                    output.push(0xFF);
                    output.push(1);
                } else {
                    output.push(byte);
                }
            }
        }
    }
    output
}

fn lz4_decompress(input: &[u8], expected_size: usize) -> Result<Vec<u8>, String> {
    let mut output = Vec::with_capacity(expected_size);
    let mut i = 0;
    while i < input.len() {
        if input[i] == 0xFF {
            if i + 2 >= input.len() {
                return Err("truncated RLE sequence".into());
            }
            let byte = input[i + 1];
            let count = input[i + 2] as usize;
            for _ in 0..count {
                output.push(byte);
            }
            i += 3;
        } else {
            output.push(input[i]);
            i += 1;
        }
    }
    if output.len() != expected_size {
        return Err(format!(
            "size mismatch: expected {}, got {}",
            expected_size,
            output.len()
        ));
    }
    Ok(output)
}

// ── Negotiation helper ───────────────────────────────────────────────

/// Determine the compression algorithm from negotiated feature flags.
pub fn negotiated_compression(features: u64) -> CompressionAlgo {
    use crate::types::FEATURE_COMPRESSION_LZ4;
    if features & FEATURE_COMPRESSION_LZ4 != 0 {
        CompressionAlgo::Lz4
    } else {
        // ZSTD not yet implemented
        CompressionAlgo::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_roundtrip() {
        // Use data with enough repetition for our simple RLE compressor to shrink it
        let mut data = Vec::with_capacity(512);
        for i in 0..32u8 {
            // 16 copies of each byte → good RLE compression
            data.extend(std::iter::repeat(i).take(16));
        }
        let (compressed, algo) = compress_payload(&data, CompressionAlgo::Lz4);
        assert_eq!(algo, CompressionAlgo::Lz4);
        let decompressed = decompress_payload(&compressed, algo).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_small_payload_not_compressed() {
        let data = b"small";
        let (result, algo) = compress_payload(data, CompressionAlgo::Lz4);
        assert_eq!(algo, CompressionAlgo::None);
        assert_eq!(result, data);
    }

    #[test]
    fn test_none_algo_passthrough() {
        let data = vec![0u8; 1024];
        let (result, algo) = compress_payload(&data, CompressionAlgo::None);
        assert_eq!(algo, CompressionAlgo::None);
        assert_eq!(result, data);
    }

    #[test]
    fn test_decompress_none() {
        let data = b"raw payload";
        let result = decompress_payload(data, CompressionAlgo::None).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_compressed_frame_roundtrip() {
        let payload = vec![0x42u8; 512]; // repetitive data compresses well
        let frame = encode_compressed_frame(0x10, &payload, CompressionAlgo::Lz4);

        // Parse frame header
        assert_eq!(frame[0], 0x10); // msg_type
        let frame_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        let tag_and_payload = &frame[5..5 + frame_len];

        let decompressed = decode_compressed_payload(tag_and_payload).unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn test_compressed_frame_small_payload_uncompressed() {
        let payload = b"tiny";
        let frame = encode_compressed_frame(0x20, payload, CompressionAlgo::Lz4);

        let frame_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        let tag_and_payload = &frame[5..5 + frame_len];

        // Should be uncompressed (tag = 0x00)
        assert_eq!(tag_and_payload[0], 0x00);
        let decompressed = decode_compressed_payload(tag_and_payload).unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn test_negotiated_compression_lz4() {
        use crate::types::FEATURE_COMPRESSION_LZ4;
        assert_eq!(
            negotiated_compression(FEATURE_COMPRESSION_LZ4),
            CompressionAlgo::Lz4
        );
    }

    #[test]
    fn test_negotiated_compression_none() {
        assert_eq!(negotiated_compression(0), CompressionAlgo::None);
    }

    #[test]
    fn test_algo_from_byte() {
        assert_eq!(
            CompressionAlgo::from_byte(0x00),
            Some(CompressionAlgo::None)
        );
        assert_eq!(CompressionAlgo::from_byte(0x01), Some(CompressionAlgo::Lz4));
        assert_eq!(CompressionAlgo::from_byte(0x99), None);
    }

    #[test]
    fn test_decompress_truncated_lz4() {
        let result = decompress_payload(&[0x01, 0x00], CompressionAlgo::Lz4);
        // Only 2 bytes but needs at least 4 for uncompressed size
        assert!(result.is_err());
    }

    #[test]
    fn test_repetitive_data_compresses_well() {
        let data = vec![0xAA; 4096];
        let (compressed, algo) = compress_payload(&data, CompressionAlgo::Lz4);
        assert_eq!(algo, CompressionAlgo::Lz4);
        // Compressed should be much smaller than 4096
        assert!(compressed.len() < data.len() / 2);
        let decompressed = decompress_payload(&compressed, algo).unwrap();
        assert_eq!(decompressed, data);
    }
}
