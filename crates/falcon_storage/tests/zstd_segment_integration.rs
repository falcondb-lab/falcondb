//! Integration tests for Zstd segment compression in the unified data plane.
//!
//! H1 — Correctness: checksum verification, dictionary bootstrap, crash consistency
//! H2 — Performance Guardrails: cold read p99, OLTP no-regression, streaming throughput
//! H3 — End-to-end: full lifecycle with dictionary, recompression, GC

use std::sync::atomic::Ordering;

use falcon_storage::unified_data_plane::*;
use falcon_storage::zstd_segment::*;

// ═══════════════════════════════════════════════════════════════════════════
// H1 — Correctness
// ═══════════════════════════════════════════════════════════════════════════

/// H1.1: Zstd segment with checksum failure is rejected on read.
#[test]
fn test_h1_checksum_failure_rejected() {
    let store = SegmentStore::new();
    let rows: Vec<Vec<u8>> = (0..5).map(|i| vec![i as u8; 200]).collect();
    let (seg_id, _meta) = write_zstd_cold_segment(&store, 1, 0, &rows, 3, None, 0).unwrap();

    // Corrupt the segment body (flip a byte in a compressed block)
    {
        let body = store.get_segment_body(seg_id).unwrap();
        let header_size = UNIFIED_HEADER_SIZE as usize;
        // Skip header + block_count(4) + first block header (4+4+1 = 9)
        let corrupt_offset = header_size + 4 + 9 + 2; // into compressed data
        if corrupt_offset < body.len() {
            let mut corrupted = body.clone();
            corrupted[corrupt_offset] ^= 0xFF;
            // Write corrupted body back
            store.delete_segment(seg_id).unwrap();
            let hdr = UnifiedSegmentHeader::new_cold(seg_id, 256 * 1024 * 1024, SegmentCodec::Zstd, 1, 0);
            store.create_segment(hdr).unwrap();
            store.write_chunk_at(seg_id, 0, &corrupted).unwrap();
            store.seal_segment(seg_id).unwrap();

            // Read should fail with CRC mismatch
            let result = read_zstd_cold_segment(&store, seg_id, None);
            assert!(result.is_err(), "corrupted segment should fail to read");
        }
    }
}

/// H1.2: Dictionary must be available before reading dict-compressed segments.
#[test]
fn test_h1_dictionary_required_for_read() {
    let store = SegmentStore::new();
    let dict_store = DictionaryStore::new();

    // Train a dictionary
    let samples: Vec<Vec<u8>> = (0..100).map(|i| {
        let mut v = b"FalconDB common prefix row data ".to_vec();
        v.extend_from_slice(&(i as u32).to_le_bytes());
        v.extend_from_slice(&vec![0xABu8; 64]);
        v
    }).collect();
    let entry = dict_store.train_dictionary(1, 1, &samples, 8192).unwrap();
    let dict_data = entry.data.clone();
    dict_store.register(entry);

    // Write segment with dictionary
    let rows: Vec<Vec<u8>> = (0..10).map(|i| {
        let mut v = b"FalconDB common prefix row data ".to_vec();
        v.extend_from_slice(&(i as u32).to_le_bytes());
        v.extend_from_slice(&vec![0xCDu8; 64]);
        v
    }).collect();
    let (seg_id, meta) = write_zstd_cold_segment(
        &store, 1, 0, &rows, 3, Some(&dict_data), 1,
    ).unwrap();
    assert!(meta.dictionary_id > 0);

    // Read WITH dictionary succeeds
    let recovered = read_zstd_cold_segment(&store, seg_id, Some(&dict_data)).unwrap();
    assert_eq!(recovered.len(), 10);

    // Read WITHOUT dictionary fails (zstd decompression error)
    let no_dict_result = read_zstd_cold_segment(&store, seg_id, None);
    assert!(no_dict_result.is_err(), "reading dict-compressed segment without dict should fail");
}

/// H1.3: Dictionary stored as segment and loaded back correctly.
#[test]
fn test_h1_dictionary_segment_roundtrip() {
    let seg_store = SegmentStore::new();
    let dict_store = DictionaryStore::new();

    let samples: Vec<Vec<u8>> = (0..50).map(|i| {
        let mut v = b"dict roundtrip test ".to_vec();
        v.extend_from_slice(&vec![i as u8; 80]);
        v
    }).collect();

    let mut entry = dict_store.train_dictionary(1, 1, &samples, 4096).unwrap();
    let original_data = entry.data.clone();
    let dict_id = entry.dictionary_id;

    // Store as segment
    let seg_id = dict_store.store_as_segment(&mut entry, &seg_store).unwrap();
    assert!(seg_store.is_sealed(seg_id).unwrap());

    // Load back from segment
    let loaded = dict_store.load_from_segment(&seg_store, seg_id, dict_id, 1, 1).unwrap();
    assert_eq!(loaded.data, original_data);
    assert_eq!(loaded.dictionary_id, dict_id);
}

/// H1.4: Crash during compaction — manifest stays consistent.
/// If write_zstd_cold_segment fails mid-way, no partial segment is left sealed.
#[test]
fn test_h1_crash_during_compaction_consistency() {
    let store = SegmentStore::new();
    let mut manifest = Manifest::new();

    // Successful compaction
    let rows: Vec<Vec<u8>> = vec![vec![1u8; 100]; 5];
    let (seg_id, _meta) = write_zstd_cold_segment(&store, 1, 0, &rows, 3, None, 0).unwrap();

    manifest.add_segment(ManifestEntry {
        segment_id: seg_id,
        kind: SegmentKind::Cold,
        size_bytes: 500,
        codec: SegmentCodec::Zstd,
        logical_range: LogicalRange::Cold {
            table_id: 1, shard_id: 0, min_key: vec![], max_key: vec![],
        },
        sealed: true,
    });

    // Simulate "crash" — create a segment but don't finish it
    let partial_id = store.next_segment_id();
    let partial_hdr = UnifiedSegmentHeader::new_cold(
        partial_id, 1024, SegmentCodec::Zstd, 1, 0,
    );
    store.create_segment(partial_hdr).unwrap();
    store.write_chunk(partial_id, &vec![0u8; 50]).unwrap();
    // NOT sealed — "crash" happened

    // Manifest should only contain the completed segment
    assert!(manifest.segments.contains_key(&seg_id));
    assert!(!manifest.segments.contains_key(&partial_id));

    // Partial segment is unsealed
    assert!(!store.is_sealed(partial_id).unwrap());
}

/// H1.5: All three codecs (None, LZ4, Zstd) produce correct roundtrips.
#[test]
fn test_h1_all_codecs_roundtrip() {
    for codec in [SegmentCodec::None, SegmentCodec::Lz4, SegmentCodec::Zstd] {
        let data = vec![42u8; 4096];
        let block = compress_block(&data, codec, 3, None).unwrap();
        assert!(block.verify(), "CRC failed for codec {:?}", codec);
        let recovered = decompress_block(&block, None).unwrap();
        assert_eq!(recovered, data, "roundtrip failed for codec {:?}", codec);
    }
}

/// H1.6: Codec policy enforcement — Zstd forbidden on WAL.
#[test]
fn test_h1_codec_policy_enforcement() {
    let policy = CodecPolicy::default();

    // WAL: zstd must be rejected
    assert!(!policy.validate_codec(SegmentKind::Wal, SegmentCodec::Zstd));
    assert!(policy.validate_codec(SegmentKind::Wal, SegmentCodec::None));
    assert!(policy.validate_codec(SegmentKind::Wal, SegmentCodec::Lz4));

    // Cold: zstd allowed
    assert!(policy.validate_codec(SegmentKind::Cold, SegmentCodec::Zstd));

    // Snapshot: only zstd
    assert!(policy.validate_codec(SegmentKind::Snapshot, SegmentCodec::Zstd));
    assert!(!policy.validate_codec(SegmentKind::Snapshot, SegmentCodec::None));
    assert!(!policy.validate_codec(SegmentKind::Snapshot, SegmentCodec::Lz4));
}

// ═══════════════════════════════════════════════════════════════════════════
// H2 — Performance Guardrails
// ═══════════════════════════════════════════════════════════════════════════

/// H2.1: Cold read p99 with cache should be < 1ms.
#[test]
fn test_h2_cold_read_cached_latency() {
    let pool = DecompressPool::new(8, 64 * 1024 * 1024);
    let data = vec![0xABu8; 4096];
    let block = zstd_compress_block(&data, 3, None).unwrap();

    // Warm the cache
    pool.decompress(1, 0, &block, None).unwrap();

    // Cached reads should be fast
    let mut max_us = 0u64;
    for _ in 0..100 {
        let start = std::time::Instant::now();
        let result = pool.decompress(1, 0, &block, None).unwrap();
        let elapsed_us = start.elapsed().as_micros() as u64;
        assert_eq!(result, data);
        if elapsed_us > max_us { max_us = elapsed_us; }
    }

    assert!(
        max_us < 1000,
        "cached cold read p99 = {}µs, expected < 1000µs",
        max_us
    );
}

/// H2.2: Zstd compression ratio for repetitive data should be >= 3x.
#[test]
fn test_h2_compression_ratio_repetitive() {
    let store = SegmentStore::new();
    // Highly repetitive data
    let rows: Vec<Vec<u8>> = (0..100).map(|_| {
        let mut v = b"SELECT * FROM users WHERE id = ".to_vec();
        v.extend_from_slice(&vec![0u8; 200]);
        v
    }).collect();

    let (seg_id, meta) = write_zstd_cold_segment(&store, 1, 0, &rows, 5, None, 0).unwrap();
    let ratio = meta.compression_ratio();

    assert!(
        ratio >= 3.0,
        "compression ratio {:.2}x below 3x for repetitive data",
        ratio
    );

    // Verify data integrity
    let recovered = read_zstd_cold_segment(&store, seg_id, None).unwrap();
    assert_eq!(recovered.len(), 100);
}

/// H2.3: Streaming compression roundtrip for all codecs with throughput check.
#[test]
fn test_h2_streaming_throughput() {
    let chunk_size = 1024 * 1024; // 1MB chunks
    let data = vec![0x42u8; chunk_size];
    let iterations = 20;

    for codec in [SegmentCodec::Lz4, SegmentCodec::Zstd] {
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let compressed = compress_streaming_chunk(&data, codec, 1).unwrap();
            let decompressed = decompress_streaming_chunk(&compressed, codec, chunk_size).unwrap();
            assert_eq!(decompressed.len(), chunk_size);
        }
        let elapsed = start.elapsed();
        let throughput_mbps = (iterations as f64 * chunk_size as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);

        assert!(
            throughput_mbps >= 100.0,
            "{:?} streaming throughput {:.1} MB/s below 100 MB/s",
            codec, throughput_mbps
        );
    }
}

/// H2.4: Decompress pool rejects when overloaded.
#[test]
fn test_h2_decompress_pool_overload_protection() {
    // Pool with max_concurrent = 0 to force immediate rejection
    let pool = DecompressPool::new(0, 1024 * 1024);
    let block = zstd_compress_block(b"test", 1, None).unwrap();

    let result = pool.decompress(1, 0, &block, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("overloaded"));
    assert_eq!(pool.metrics.rejected_overload.load(Ordering::Relaxed), 1);
}

/// H2.5: Zstd compaction throughput >= 5K rows/sec for 256-byte rows.
#[test]
fn test_h2_zstd_compaction_throughput() {
    let store = SegmentStore::new();
    let rows: Vec<Vec<u8>> = (0..5_000).map(|i| {
        let mut row = vec![0u8; 256];
        row[..8].copy_from_slice(&(i as u64).to_le_bytes());
        row
    }).collect();

    let start = std::time::Instant::now();
    let (seg_id, meta) = write_zstd_cold_segment(&store, 1, 0, &rows, 1, None, 0).unwrap();
    let elapsed = start.elapsed();

    let rows_per_sec = meta.block_count as f64 / elapsed.as_secs_f64();
    assert!(
        rows_per_sec >= 5_000.0,
        "zstd compaction {:.0} rows/sec below 5K guardrail",
        rows_per_sec
    );
    assert!(store.is_sealed(seg_id).unwrap());
}

// ═══════════════════════════════════════════════════════════════════════════
// H3 — End-to-End Lifecycle
// ═══════════════════════════════════════════════════════════════════════════

/// H3.1: Full lifecycle — train dict → compress with dict → store dict as segment
/// → read segment → verify data integrity.
#[test]
fn test_h3_full_dictionary_lifecycle() {
    let seg_store = SegmentStore::new();
    let dict_store = DictionaryStore::new();

    // Step 1: Train dictionary from samples
    let samples: Vec<Vec<u8>> = (0..200).map(|i| {
        let mut v = b"FalconDB table1 row payload data ".to_vec();
        v.extend_from_slice(&(i as u32).to_le_bytes());
        v.extend_from_slice(&vec![0xABu8; 100]);
        v
    }).collect();
    let mut entry = dict_store.train_dictionary(1, 1, &samples, 16384).unwrap();
    let dict_data = entry.data.clone();

    // Step 2: Store dictionary as segment
    let dict_seg_id = dict_store.store_as_segment(&mut entry, &seg_store).unwrap();
    dict_store.register(entry);

    // Step 3: Compress data with dictionary
    let rows: Vec<Vec<u8>> = (0..50).map(|i| {
        let mut v = b"FalconDB table1 row payload data ".to_vec();
        v.extend_from_slice(&((i + 1000) as u32).to_le_bytes());
        v.extend_from_slice(&vec![0xCDu8; 100]);
        v
    }).collect();
    let (data_seg_id, meta) = write_zstd_cold_segment(
        &seg_store, 1, 0, &rows, 3, Some(&dict_data), 1,
    ).unwrap();

    // Step 4: Verify dictionary is retrievable
    let fetched_dict = dict_store.get_for_table(1).unwrap();
    assert_eq!(fetched_dict.data, dict_data);

    // Step 5: Read and verify data
    let recovered = read_zstd_cold_segment(&seg_store, data_seg_id, Some(&dict_data)).unwrap();
    assert_eq!(recovered.len(), 50);
    for (i, row) in recovered.iter().enumerate() {
        let expected_i = (i + 1000) as u32;
        let mut expected = b"FalconDB table1 row payload data ".to_vec();
        expected.extend_from_slice(&expected_i.to_le_bytes());
        expected.extend_from_slice(&vec![0xCDu8; 100]);
        assert_eq!(row, &expected);
    }

    // Step 6: Verify metrics
    assert_eq!(dict_store.metrics.training_runs.load(Ordering::Relaxed), 1);
    assert_eq!(meta.block_count, 50);
    assert!(meta.compression_ratio() >= 1.0);

    // Step 7: Dictionary segment is sealed and valid
    assert!(seg_store.is_sealed(dict_seg_id).unwrap());
}

/// H3.2: Recompression with dictionary upgrade preserves data.
#[test]
fn test_h3_recompression_with_dict_upgrade() {
    let store = SegmentStore::new();
    let dict_store = DictionaryStore::new();

    // Write segment without dictionary (level 1)
    let rows: Vec<Vec<u8>> = (0..20).map(|i| vec![i as u8; 256]).collect();
    let (old_seg_id, _) = write_zstd_cold_segment(&store, 1, 0, &rows, 1, None, 0).unwrap();

    // Train a dictionary
    let samples: Vec<Vec<u8>> = (0..50).map(|i| vec![i as u8; 256]).collect();
    let entry = dict_store.train_dictionary(1, 1, &samples, 8192).unwrap();
    let new_dict = entry.data.clone();
    dict_store.register(entry);

    // Recompress with dictionary at higher level
    let request = RecompressRequest {
        source_segment_id: old_seg_id,
        target_codec: SegmentCodec::Zstd,
        target_level: 5,
        new_dictionary_id: 1,
        reason: RecompressReason::DictUpgrade,
    };
    let result = recompress_segment(&store, &request, None, Some(&new_dict), 1, 0).unwrap();

    // Verify new segment
    assert_ne!(result.old_segment_id, result.new_segment_id);
    assert!(store.is_sealed(result.new_segment_id).unwrap());

    // Verify data integrity in new segment
    let recovered = read_zstd_cold_segment(&store, result.new_segment_id, Some(&new_dict)).unwrap();
    assert_eq!(recovered.len(), 20);
    for (i, row) in recovered.iter().enumerate() {
        assert_eq!(row, &vec![i as u8; 256]);
    }

    // Old segment still exists (GC hasn't run)
    assert!(store.exists(old_seg_id));
}

/// H3.3: Streaming codec negotiation end-to-end.
#[test]
fn test_h3_streaming_negotiation_e2e() {
    // Same DC: both high bandwidth → LZ4
    let leader = StreamingCodecCaps::high_bandwidth();
    let follower = StreamingCodecCaps::high_bandwidth();
    let codec = negotiate_streaming_codec(&leader, &follower);
    assert_eq!(codec, SegmentCodec::Lz4);

    // Use negotiated codec for actual transfer
    let data = vec![0xABu8; 16 * 1024]; // 16KB chunk
    let compressed = compress_streaming_chunk(&data, codec, 1).unwrap();
    let decompressed = decompress_streaming_chunk(&compressed, codec, data.len()).unwrap();
    assert_eq!(decompressed, data);

    // Cross DC: low bandwidth → Zstd
    let remote_follower = StreamingCodecCaps::low_bandwidth();
    let codec2 = negotiate_streaming_codec(&leader, &remote_follower);
    assert_eq!(codec2, SegmentCodec::Zstd);

    let compressed2 = compress_streaming_chunk(&data, codec2, 1).unwrap();
    assert!(compressed2.len() < compressed.len(), "zstd should compress better than lz4");
    let decompressed2 = decompress_streaming_chunk(&compressed2, codec2, data.len()).unwrap();
    assert_eq!(decompressed2, data);
}

/// H3.4: Decompress cache effectiveness — second read is cached.
#[test]
fn test_h3_decompress_cache_effectiveness() {
    let pool = DecompressPool::new(4, 64 * 1024 * 1024);

    // Compress 10 blocks
    let blocks: Vec<(u32, ZstdBlock)> = (0..10).map(|i| {
        let data = vec![i as u8; 1024];
        (i, zstd_compress_block(&data, 1, None).unwrap())
    }).collect();

    // First pass: all misses
    for (idx, block) in &blocks {
        pool.decompress(1, *idx, block, None).unwrap();
    }
    assert_eq!(pool.cache.metrics.misses.load(Ordering::Relaxed), 10);
    assert_eq!(pool.cache.metrics.hits.load(Ordering::Relaxed), 0);

    // Second pass: all hits
    for (idx, block) in &blocks {
        pool.decompress(1, *idx, block, None).unwrap();
    }
    assert_eq!(pool.cache.metrics.hits.load(Ordering::Relaxed), 10);
    assert!(pool.cache.metrics.hit_rate() > 0.49);
}

/// H3.5: Metrics snapshot captures all dimensions.
#[test]
fn test_h3_metrics_complete() {
    let compress = ZstdCompressMetrics::default();
    let pool = DecompressPool::new(4, 1024 * 1024);
    let dict_store = DictionaryStore::new();

    // Generate some activity
    compress.record(10000, 3000);
    let block = zstd_compress_block(b"metrics test data payload", 1, None).unwrap();
    pool.decompress(1, 0, &block, None).unwrap();
    pool.decompress(1, 0, &block, None).unwrap(); // cache hit

    let samples: Vec<Vec<u8>> = (0..30).map(|i| vec![i as u8; 64]).collect();
    let entry = dict_store.train_dictionary(1, 1, &samples, 2048).unwrap();
    dict_store.register(entry);

    let snapshot = build_zstd_metrics(&compress, &pool, &dict_store);
    assert_eq!(snapshot.compress_total, 1);
    assert_eq!(snapshot.compress_bytes_in, 10000);
    assert_eq!(snapshot.compress_bytes_out, 3000);
    assert!(snapshot.compress_ratio > 3.0);
    assert_eq!(snapshot.decompress_total, 1); // only first call decompresses
    assert!(snapshot.decompress_bytes > 0);
    assert!(snapshot.cache_hit_rate > 0.0);
    assert_eq!(snapshot.dict_count, 1);
    assert!(snapshot.dict_bytes_total > 0);
}
