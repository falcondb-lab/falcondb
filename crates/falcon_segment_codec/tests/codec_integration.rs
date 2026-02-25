//! Integration tests for falcon_segment_codec
//!
//! I1 — Correctness: CRC verification, dictionary required, codec policy enforcement
//! I2 — Performance Guardrails: cached read latency, compression ratio, throughput
//! I3 — End-to-End: full lifecycle, recompression, streaming negotiation, metrics

use std::sync::atomic::Ordering;

use falcon_segment_codec::*;

// ═══════════════════════════════════════════════════════════════════════════
// I1 — Correctness
// ═══════════════════════════════════════════════════════════════════════════

/// I1.1: Block CRC failure rejects decompression.
#[test]
fn test_i1_crc_failure_rejects_decompress() {
    let codec = ZstdBlockCodec::new(ZstdCodecConfig::default());
    let mut block = CompressedBlock::compress(&codec, b"crc test data payload").unwrap();
    assert!(block.verify());

    // Corrupt the CRC
    block.header.block_crc ^= 0xDEAD;
    assert!(!block.verify());
    assert!(block.decompress(&codec).is_err());
}

/// I1.2: Block data corruption detected via CRC.
#[test]
fn test_i1_data_corruption_detected() {
    let codec = ZstdBlockCodec::new(ZstdCodecConfig::default());
    let mut block = CompressedBlock::compress(&codec, b"corruption detection test").unwrap();
    assert!(block.verify());

    // Corrupt the compressed data
    if !block.data.is_empty() {
        block.data[0] ^= 0xFF;
    }
    // CRC should now mismatch
    assert!(!block.verify());
}

/// I1.3: Dictionary missing → codec creation fails.
#[test]
fn test_i1_dictionary_missing_fails() {
    let reg = DictionaryRegistry::new();
    let result = reg.create_codec_with_dict(999, ZstdCodecConfig::default());
    assert!(result.is_err());
    assert!(result.unwrap_err().0.contains("not found"));
}

/// I1.4: Dictionary checksum verified on load.
#[test]
fn test_i1_dictionary_checksum_verified() {
    let handle = DictionaryHandle::new(1, 100, 42, 1, vec![0xAB; 256]);
    assert!(handle.verify());

    let mut bad = handle.clone();
    bad.checksum ^= 1;
    assert!(!bad.verify());
}

/// I1.5: Codec policy — Zstd forbidden on WAL.
#[test]
fn test_i1_codec_policy_wal_zstd_forbidden() {
    let policy = CodecPolicy::default();
    assert!(!policy.is_allowed(SegmentKind::Wal, CodecId::Zstd));
    assert!(policy.is_allowed(SegmentKind::Wal, CodecId::None));
    assert!(policy.is_allowed(SegmentKind::Wal, CodecId::Lz4));
}

/// I1.6: Codec policy — Snapshot requires Zstd.
#[test]
fn test_i1_codec_policy_snapshot_zstd_only() {
    let policy = CodecPolicy::default();
    assert!(policy.is_allowed(SegmentKind::Snapshot, CodecId::Zstd));
    assert!(!policy.is_allowed(SegmentKind::Snapshot, CodecId::None));
    assert!(!policy.is_allowed(SegmentKind::Snapshot, CodecId::Lz4));
}

/// I1.7: All three codecs produce correct roundtrips through write_blocks/read_blocks.
#[test]
fn test_i1_all_codecs_block_roundtrip() {
    let rows: Vec<Vec<u8>> = (0..20).map(|i| vec![i as u8; 128]).collect();

    for id in [CodecId::None, CodecId::Lz4, CodecId::Zstd] {
        let codec = create_codec(id, None, None);
        let (body, meta) = write_blocks(codec.as_ref(), &rows).unwrap();
        assert_eq!(meta.block_count, 20);

        let recovered = read_blocks(codec.as_ref(), &body).unwrap();
        assert_eq!(recovered.len(), 20);
        for (i, row) in recovered.iter().enumerate() {
            assert_eq!(row, &vec![i as u8; 128], "mismatch at row {} for {:?}", i, id);
        }
    }
}

/// I1.8: ZstdSegmentMeta serialization roundtrip.
#[test]
fn test_i1_segment_meta_roundtrip() {
    let meta = ZstdSegmentMeta::new(7)
        .with_dictionary(42, 0xCAFE);
    let bytes = meta.to_bytes();
    assert_eq!(bytes.len(), 37);
    let recovered = ZstdSegmentMeta::from_bytes(&bytes).unwrap();
    assert_eq!(recovered, meta);
    assert_eq!(recovered.dictionary_id, 42);
    assert_eq!(recovered.dictionary_checksum, 0xCAFE);
}

// ═══════════════════════════════════════════════════════════════════════════
// I2 — Performance Guardrails
// ═══════════════════════════════════════════════════════════════════════════

/// I2.1: Cached decompress p99 < 1ms (100 iterations).
#[test]
fn test_i2_cached_read_latency() {
    let pool = DecompressPool::new(8, 64 * 1024 * 1024);
    let codec = ZstdBlockCodec::new(ZstdCodecConfig::default());
    let block = CompressedBlock::compress(&codec, &vec![0xABu8; 4096]).unwrap();

    // Warm cache
    pool.decompress(1, 0, &block, &codec).unwrap();

    let mut max_us = 0u64;
    for _ in 0..100 {
        let start = std::time::Instant::now();
        pool.decompress(1, 0, &block, &codec).unwrap();
        let us = start.elapsed().as_micros() as u64;
        if us > max_us { max_us = us; }
    }
    assert!(max_us < 1000, "cached read p99 = {}µs, expected < 1000µs", max_us);
}

/// I2.2: Zstd compression ratio ≥ 3x for repetitive data.
#[test]
fn test_i2_compression_ratio() {
    let codec = ZstdBlockCodec::new(ZstdCodecConfig { level: 3, checksum: true });
    let rows: Vec<Vec<u8>> = (0..100).map(|_| {
        let mut v = b"SELECT * FROM users WHERE id = ".to_vec();
        v.extend_from_slice(&[0u8; 200]);
        v
    }).collect();
    let (body, meta) = write_blocks(&codec, &rows).unwrap();
    let ratio = meta.compression_ratio();
    assert!(ratio >= 3.0, "ratio {:.2}x below 3x guardrail", ratio);

    // Verify integrity
    let recovered = read_blocks(&codec, &body).unwrap();
    assert_eq!(recovered.len(), 100);
}

/// I2.3: Streaming throughput ≥ 100 MB/s for LZ4 and Zstd.
#[test]
fn test_i2_streaming_throughput() {
    let chunk = vec![0x42u8; 1024 * 1024]; // 1MB
    let iters = 20;
    for codec in [StreamingCodecId::Lz4, StreamingCodecId::Zstd] {
        let start = std::time::Instant::now();
        for _ in 0..iters {
            let c = compress_streaming_chunk(&chunk, codec, 1).unwrap();
            let d = decompress_streaming_chunk(&c, codec, chunk.len()).unwrap();
            assert_eq!(d.len(), chunk.len());
        }
        let mbps = (iters as f64 * chunk.len() as f64) / start.elapsed().as_secs_f64() / (1024.0 * 1024.0);
        assert!(mbps >= 100.0, "{:?} streaming {:.1} MB/s < 100 MB/s", codec, mbps);
    }
}

/// I2.4: Decompress pool rejects when overloaded (max_concurrent=0).
#[test]
fn test_i2_pool_overload_rejection() {
    let pool = DecompressPool::new(0, 1024);
    let codec = ZstdBlockCodec::new(ZstdCodecConfig::default());
    let block = CompressedBlock::compress(&codec, b"overload test").unwrap();

    let result = pool.decompress(1, 0, &block, &codec);
    assert!(result.is_err());
    assert!(result.unwrap_err().0.contains("overloaded"));
    assert_eq!(pool.metrics.rejected.load(Ordering::Relaxed), 1);
}

/// I2.5: Zstd block compression throughput ≥ 5K blocks/sec for 256-byte blocks.
#[test]
fn test_i2_block_compress_throughput() {
    let codec = ZstdBlockCodec::new(ZstdCodecConfig { level: 1, checksum: false });
    let rows: Vec<Vec<u8>> = (0..5_000).map(|i| {
        let mut r = vec![0u8; 256];
        r[..8].copy_from_slice(&(i as u64).to_le_bytes());
        r
    }).collect();

    let start = std::time::Instant::now();
    let (_body, meta) = write_blocks(&codec, &rows).unwrap();
    let elapsed = start.elapsed();
    let rate = meta.block_count as f64 / elapsed.as_secs_f64();
    assert!(rate >= 5_000.0, "block rate {:.0}/s < 5K guardrail", rate);
}

// ═══════════════════════════════════════════════════════════════════════════
// I3 — End-to-End
// ═══════════════════════════════════════════════════════════════════════════

/// I3.1: Full dictionary lifecycle — train → load → compress → decompress → verify.
#[test]
fn test_i3_full_dictionary_lifecycle() {
    let reg = DictionaryRegistry::new();

    // Train via zstd-safe (real dict)
    let samples: Vec<Vec<u8>> = (0..200).map(|i| {
        let mut v = b"FalconDB table row payload ".to_vec();
        v.extend_from_slice(&(i as u32).to_le_bytes());
        v.extend_from_slice(&[0xABu8; 80]);
        v
    }).collect();
    let sizes: Vec<usize> = samples.iter().map(|s| s.len()).collect();
    let concat: Vec<u8> = samples.iter().flat_map(|s| s.iter().copied()).collect();
    let mut dict_buf = vec![0u8; 16384];
    let n = zstd_safe::train_from_buffer(&mut dict_buf[..], &concat, &sizes).unwrap();
    dict_buf.truncate(n);

    let handle = DictionaryHandle::new(1, 100, 42, 1, dict_buf.clone());
    assert!(handle.verify());
    reg.load(handle);

    // Create codec with dict
    let codec = reg.create_codec_with_dict(1, ZstdCodecConfig::default()).unwrap();

    // Compress rows with dictionary
    let rows: Vec<Vec<u8>> = (0..50).map(|i| {
        let mut v = b"FalconDB table row payload ".to_vec();
        v.extend_from_slice(&((i + 1000) as u32).to_le_bytes());
        v.extend_from_slice(&[0xCDu8; 80]);
        v
    }).collect();
    let (body, meta) = write_blocks(&codec, &rows).unwrap();
    assert_eq!(meta.block_count, 50);

    // Read back with same dict codec
    let recovered = read_blocks(&codec, &body).unwrap();
    assert_eq!(recovered.len(), 50);
    for (i, row) in recovered.iter().enumerate() {
        let expected_i = (i + 1000) as u32;
        let mut expected = b"FalconDB table row payload ".to_vec();
        expected.extend_from_slice(&expected_i.to_le_bytes());
        expected.extend_from_slice(&[0xCDu8; 80]);
        assert_eq!(row, &expected);
    }

    // Verify metrics
    assert_eq!(reg.metrics.loaded.load(Ordering::Relaxed), 1);
    assert!(reg.metrics.bytes_total.load(Ordering::Relaxed) > 0);
}

/// I3.2: Recompression preserves data integrity across codec change.
#[test]
fn test_i3_recompress_preserves_data() {
    let rows: Vec<Vec<u8>> = (0..20).map(|i| vec![i as u8; 256]).collect();

    // Original with LZ4
    let lz4 = create_codec(CodecId::Lz4, None, None);
    let (body_lz4, _) = write_blocks(lz4.as_ref(), &rows).unwrap();
    let lz4_rows = read_blocks(lz4.as_ref(), &body_lz4).unwrap();

    // Recompress to Zstd
    let zstd = create_codec(CodecId::Zstd, Some(ZstdCodecConfig { level: 5, checksum: true }), None);
    let request = RecompressRequest {
        source_data: lz4_rows,
        target_codec_id: CodecId::Zstd,
        target_level: 5,
        target_dict_id: 0,
        reason: RecompressReason::CodecChange,
    };
    let result = recompress(&request, zstd.as_ref()).unwrap();
    assert_eq!(result.new_codec, CodecId::Zstd);
    assert_eq!(result.meta.block_count, 20);

    // Verify data
    let recovered = read_blocks(zstd.as_ref(), &result.body).unwrap();
    assert_eq!(recovered, rows);
}

/// I3.3: Streaming codec negotiation E2E with actual data transfer.
#[test]
fn test_i3_streaming_negotiation_e2e() {
    // Same DC → LZ4
    let leader = StreamingCodecCaps::high_bandwidth();
    let follower = StreamingCodecCaps::high_bandwidth();
    let codec = negotiate_streaming_codec(&leader, &follower);
    assert_eq!(codec, StreamingCodecId::Lz4);

    let payload = vec![0xABu8; 16 * 1024];
    let compressed = compress_streaming_chunk(&payload, codec, 1).unwrap();
    let decompressed = decompress_streaming_chunk(&compressed, codec, payload.len()).unwrap();
    assert_eq!(decompressed, payload);

    // Cross DC → Zstd
    let remote = StreamingCodecCaps::low_bandwidth();
    let codec2 = negotiate_streaming_codec(&leader, &remote);
    assert_eq!(codec2, StreamingCodecId::Zstd);

    let c2 = compress_streaming_chunk(&payload, codec2, 1).unwrap();
    assert!(c2.len() < compressed.len(), "zstd should compress better than lz4");
    let d2 = decompress_streaming_chunk(&c2, codec2, payload.len()).unwrap();
    assert_eq!(d2, payload);
}

/// I3.4: Decompress cache effectiveness — second pass is all hits.
#[test]
fn test_i3_cache_effectiveness() {
    let pool = DecompressPool::new(4, 64 * 1024 * 1024);
    let codec = ZstdBlockCodec::new(ZstdCodecConfig::default());

    let blocks: Vec<(u32, CompressedBlock)> = (0..10).map(|i| {
        (i, CompressedBlock::compress(&codec, &vec![i as u8; 1024]).unwrap())
    }).collect();

    // First pass: all misses
    for (idx, block) in &blocks {
        pool.decompress(1, *idx, block, &codec).unwrap();
    }
    assert_eq!(pool.cache.metrics.misses.load(Ordering::Relaxed), 10);

    // Second pass: all hits
    for (idx, block) in &blocks {
        pool.decompress(1, *idx, block, &codec).unwrap();
    }
    assert_eq!(pool.cache.metrics.hits.load(Ordering::Relaxed), 10);
    assert!(pool.cache.metrics.hit_rate() > 0.49);
}

/// I3.5: Full metrics snapshot captures all dimensions.
#[test]
fn test_i3_metrics_complete() {
    let cm = CompressMetrics::default();
    cm.record(10000, 3000, 200_000);

    let pool = DecompressPool::new(4, 1024 * 1024);
    let codec = ZstdBlockCodec::new(ZstdCodecConfig::default());
    let block = CompressedBlock::compress(&codec, b"metrics test").unwrap();
    pool.decompress(1, 0, &block, &codec).unwrap();
    pool.decompress(1, 0, &block, &codec).unwrap(); // cache hit

    let reg = DictionaryRegistry::new();
    reg.load(DictionaryHandle::new(1, 100, 42, 1, vec![0u8; 128]));

    let snap = build_metrics_snapshot(&cm, &pool, &reg);
    assert_eq!(snap.compress_calls, 1);
    assert_eq!(snap.compress_bytes_in, 10000);
    assert!(snap.compress_ratio > 3.0);
    assert_eq!(snap.decompress_total, 1);
    assert!(snap.decompress_bytes > 0);
    assert!(snap.cache_hit_rate > 0.0);
    assert_eq!(snap.dict_count, 1);
    assert!(snap.dict_bytes_total > 0);
}

/// I3.6: libzstd version is reported.
#[test]
fn test_i3_zstd_version_reported() {
    let v = zstd_version();
    // zstd 1.5.x = 10500+
    assert!(v >= 10000, "zstd version {} too low", v);
}
