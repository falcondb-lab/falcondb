//! Cold Store Compression Correctness Tests — v1.0.7
//!
//! Validates:
//! - Store/read roundtrip with LZ4 and None codecs
//! - Multiple rows across segment rotation
//! - Concurrent reads/writes safety
//! - Cache behavior under pressure
//! - Compression ratio observability
//! - String intern pool correctness
//! - Cold handle compactness

use std::sync::atomic::Ordering;
use std::sync::Arc;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_storage::cold_store::*;

fn sample_row(i: u64) -> OwnedRow {
    OwnedRow {
        values: vec![
            Datum::Int64(i as i64),
            Datum::Text(format!("row_payload_{}", i)),
            Datum::Float64(i as f64 * 3.14),
        ],
    }
}

fn repetitive_row(i: u64) -> OwnedRow {
    OwnedRow {
        values: vec![
            Datum::Int64(i as i64),
            Datum::Text("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string()),
            Datum::Text("STATUS_ACTIVE".to_string()),
            Datum::Text("US-EAST-1".to_string()),
        ],
    }
}

// ─── Basic roundtrip ────────────────────────────────────────────────────

#[test]
fn test_store_read_roundtrip_lz4() {
    let store = ColdStore::new(ColdStoreConfig {
        compression_enabled: true,
        codec: CompressionCodec::Lz4,
        ..Default::default()
    });

    for i in 0..50 {
        let row = sample_row(i);
        let handle = store.store_row(&row).unwrap();
        let recovered = store.read_row(&handle).unwrap();

        assert_eq!(row.values.len(), recovered.values.len());
        for (a, b) in row.values.iter().zip(recovered.values.iter()) {
            assert_eq!(format!("{:?}", a), format!("{:?}", b));
        }
    }
}

#[test]
fn test_store_read_roundtrip_none() {
    let store = ColdStore::new(ColdStoreConfig {
        compression_enabled: false,
        ..Default::default()
    });

    for i in 0..50 {
        let row = sample_row(i);
        let handle = store.store_row(&row).unwrap();
        let recovered = store.read_row(&handle).unwrap();

        assert_eq!(row.values.len(), recovered.values.len());
        for (a, b) in row.values.iter().zip(recovered.values.iter()) {
            assert_eq!(format!("{:?}", a), format!("{:?}", b));
        }
    }
}

// ─── Consistency: LZ4 vs None produce identical results ─────────────────

#[test]
fn test_lz4_vs_none_identical_results() {
    let store_lz4 = ColdStore::new(ColdStoreConfig {
        compression_enabled: true,
        codec: CompressionCodec::Lz4,
        ..Default::default()
    });
    let store_none = ColdStore::new(ColdStoreConfig {
        compression_enabled: false,
        ..Default::default()
    });

    for i in 0..100 {
        let row = sample_row(i);
        let h_lz4 = store_lz4.store_row(&row).unwrap();
        let h_none = store_none.store_row(&row).unwrap();

        let r_lz4 = store_lz4.read_row(&h_lz4).unwrap();
        let r_none = store_none.read_row(&h_none).unwrap();

        assert_eq!(
            format!("{:?}", r_lz4),
            format!("{:?}", r_none),
            "LZ4 and None should produce identical row data for row {}",
            i
        );
    }
}

// ─── Segment rotation ───────────────────────────────────────────────────

#[test]
fn test_segment_rotation_data_integrity() {
    let store = ColdStore::new(ColdStoreConfig {
        max_segment_size: 1024, // Very small: forces rotation
        compression_enabled: true,
        codec: CompressionCodec::Lz4,
        ..Default::default()
    });

    let mut handles = Vec::new();
    for i in 0..200 {
        let row = sample_row(i);
        let handle = store.store_row(&row).unwrap();
        handles.push((i, handle));
    }

    // Verify all rows are readable after rotation
    for (i, handle) in &handles {
        let row = store.read_row(handle).unwrap();
        match &row.values[0] {
            Datum::Int64(v) => assert_eq!(*v, *i as i64),
            other => panic!("expected Int64({}), got {:?}", i, other),
        }
    }

    let seg_count = store.segment_count();
    assert!(seg_count > 1, "should have rotated segments: got {}", seg_count);
    println!("segments after 200 rows (1KB max): {}", seg_count);
}

// ─── Concurrent read/write safety ───────────────────────────────────────

#[test]
fn test_concurrent_store_and_read() {
    let store = Arc::new(ColdStore::new_in_memory());
    let num_writers = 4;
    let rows_per_writer = 100;

    // Phase 1: concurrent writes
    let mut write_handles = Vec::new();
    for t in 0..num_writers {
        let s = Arc::clone(&store);
        write_handles.push(std::thread::spawn(move || {
            let mut handles = Vec::new();
            for i in 0..rows_per_writer {
                let id = (t * 1000 + i) as u64;
                let row = sample_row(id);
                let handle = s.store_row(&row).unwrap();
                handles.push((id, handle));
            }
            handles
        }));
    }

    let all_handles: Vec<(u64, ColdHandle)> = write_handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    assert_eq!(all_handles.len(), num_writers * rows_per_writer);

    // Phase 2: concurrent reads
    let all_handles = Arc::new(all_handles);
    let mut read_handles = Vec::new();
    for t in 0..num_writers {
        let s = Arc::clone(&store);
        let ah = Arc::clone(&all_handles);
        read_handles.push(std::thread::spawn(move || {
            let chunk_size = ah.len() / num_writers;
            let start = t * chunk_size;
            let end = if t == num_writers - 1 { ah.len() } else { start + chunk_size };
            for idx in start..end {
                let (expected_id, ref handle) = ah[idx];
                let row = s.read_row(handle).unwrap();
                match &row.values[0] {
                    Datum::Int64(v) => assert_eq!(*v, expected_id as i64),
                    other => panic!("expected Int64({}), got {:?}", expected_id, other),
                }
            }
        }));
    }

    for h in read_handles {
        h.join().unwrap();
    }
}

// ─── Compression ratio with repetitive data ─────────────────────────────

#[test]
fn test_compression_ratio_repetitive_data() {
    let store = ColdStore::new(ColdStoreConfig {
        compression_enabled: true,
        codec: CompressionCodec::Lz4,
        ..Default::default()
    });

    for i in 0..100 {
        store.store_row(&repetitive_row(i)).unwrap();
    }

    let snap = store.metrics.snapshot();
    println!(
        "repetitive data: original={}B, compressed={}B, ratio={:.2}",
        snap.cold_original_bytes, snap.cold_bytes, snap.compression_ratio
    );

    assert!(
        snap.compression_ratio > 1.0,
        "LZ4 should compress repetitive data: ratio={}",
        snap.compression_ratio
    );
    assert_eq!(snap.cold_migrate_total, 100);
}

// ─── Compression disabled: ratio ~1.0 ──────────────────────────────────

#[test]
fn test_compression_disabled_ratio() {
    let store = ColdStore::new(ColdStoreConfig {
        compression_enabled: false,
        ..Default::default()
    });

    for i in 0..50 {
        store.store_row(&repetitive_row(i)).unwrap();
    }

    let snap = store.metrics.snapshot();
    // Without compression, ratio accounts for block header overhead (~9 bytes)
    // Original bytes < cold_bytes because of headers, so ratio may be slightly < 1.0
    assert!(
        snap.compression_ratio > 0.8 && snap.compression_ratio < 1.2,
        "no-compression ratio should be ~1.0: got {}",
        snap.compression_ratio
    );
}

// ─── Cache behavior ─────────────────────────────────────────────────────

#[test]
fn test_cache_hit_miss_behavior() {
    let store = ColdStore::new(ColdStoreConfig {
        block_cache_capacity: 1024 * 1024, // 1 MB
        ..Default::default()
    });

    let mut handles = Vec::new();
    for i in 0..20 {
        handles.push(store.store_row(&sample_row(i)).unwrap());
    }

    // First reads: all cache misses
    for h in &handles {
        let _ = store.read_row(h).unwrap();
    }
    assert_eq!(store.metrics.cold_read_total.load(Ordering::Relaxed), 20);

    // Second reads: all cache hits
    for h in &handles {
        let _ = store.read_row(h).unwrap();
    }
    assert_eq!(store.metrics.cold_read_total.load(Ordering::Relaxed), 40);
    assert!(store.cache_hit_rate() > 0.3, "should have cache hits");
}

#[test]
fn test_cache_eviction_under_pressure() {
    let store = ColdStore::new(ColdStoreConfig {
        block_cache_capacity: 256, // Very small: forces eviction
        ..Default::default()
    });

    let mut handles = Vec::new();
    for i in 0..50 {
        handles.push(store.store_row(&sample_row(i)).unwrap());
    }

    // Read all: cache will evict older entries
    for h in &handles {
        let _ = store.read_row(h).unwrap();
    }

    // Cache should not exceed capacity (approximately)
    assert!(store.cache_bytes() <= 512, "cache should respect capacity limit");
}

// ─── Decompress latency tracking ────────────────────────────────────────

#[test]
fn test_decompress_latency_tracking() {
    let store = ColdStore::new_in_memory();

    let mut handles = Vec::new();
    for i in 0..10 {
        handles.push(store.store_row(&sample_row(i)).unwrap());
    }

    for h in &handles {
        let _ = store.read_row(h).unwrap();
    }

    let snap = store.metrics.snapshot();
    assert_eq!(snap.cold_decompress_total, 10);
    // Latency should be tracked (may be 0 on fast systems)
    assert!(snap.cold_decompress_latency_us < 10_000_000, "latency should be reasonable");
}

// ─── String Intern Pool ─────────────────────────────────────────────────

#[test]
fn test_intern_pool_deduplication() {
    let pool = StringInternPool::new();

    let id1 = pool.intern("STATUS_ACTIVE");
    let id2 = pool.intern("STATUS_INACTIVE");
    let id3 = pool.intern("STATUS_ACTIVE"); // duplicate

    assert_eq!(id1, id3, "same string should produce same InternId");
    assert_ne!(id1, id2, "different strings should produce different InternIds");

    assert_eq!(pool.resolve(id1), Some("STATUS_ACTIVE".to_string()));
    assert_eq!(pool.resolve(id2), Some("STATUS_INACTIVE".to_string()));
    assert_eq!(pool.len(), 2);
}

#[test]
fn test_intern_pool_hit_rate_tracking() {
    let pool = StringInternPool::new();

    // 5 unique + 5 duplicates = 50% hit rate
    for i in 0..5 {
        pool.intern(&format!("key_{}", i));
    }
    for i in 0..5 {
        pool.intern(&format!("key_{}", i));
    }

    assert_eq!(pool.hits(), 5);
    assert_eq!(pool.misses(), 5);
    let rate = pool.hit_rate();
    assert!((rate - 0.5).abs() < 0.01, "expected ~50% hit rate, got {}", rate);
}

#[test]
fn test_intern_pool_concurrent_safety() {
    let pool = Arc::new(StringInternPool::new());
    let mut handles = Vec::new();

    for t in 0..8 {
        let p = Arc::clone(&pool);
        handles.push(std::thread::spawn(move || {
            for i in 0..200 {
                // Threads 0-3 and 4-7 share keys (t % 4)
                let key = format!("shared_{}_{}", t % 4, i);
                let id = p.intern(&key);
                assert!(p.resolve(id).is_some());
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // 4 groups × 200 unique keys = 800 unique strings
    assert_eq!(pool.len(), 800);
    // Each key was interned twice (by 2 threads), so 800 hits
    assert!(pool.hits() >= 800, "should have significant hits from thread overlap");
}

// ─── ColdHandle compactness ─────────────────────────────────────────────

#[test]
fn test_cold_handle_is_compact() {
    assert!(
        ColdHandle::SIZE <= 24,
        "ColdHandle should be ≤ 24 bytes, got {}",
        ColdHandle::SIZE
    );
}

// ─── Metrics snapshot consistency ───────────────────────────────────────

#[test]
fn test_metrics_snapshot_after_operations() {
    let store = ColdStore::new_in_memory();

    // Store 10 rows
    let mut handles = Vec::new();
    for i in 0..10 {
        handles.push(store.store_row(&sample_row(i)).unwrap());
    }

    // Read 5 rows (2 times each = 5 cache misses + 5 cache hits)
    for h in &handles[..5] {
        let _ = store.read_row(h).unwrap();
    }
    for h in &handles[..5] {
        let _ = store.read_row(h).unwrap();
    }

    let snap = store.metrics.snapshot();
    assert_eq!(snap.cold_migrate_total, 10);
    assert_eq!(snap.cold_read_total, 10); // 5 + 5
    assert_eq!(snap.cold_decompress_total, 5); // only cache misses decompress
    assert!(snap.cold_bytes > 0);
    assert!(snap.cold_original_bytes > 0);
    assert!(snap.compression_ratio > 0.0);
}

// ─── Edge case: empty row ───────────────────────────────────────────────

#[test]
fn test_store_empty_row() {
    let store = ColdStore::new_in_memory();
    let row = OwnedRow { values: vec![] };
    let handle = store.store_row(&row).unwrap();
    let recovered = store.read_row(&handle).unwrap();
    assert!(recovered.values.is_empty());
}

// ─── Edge case: large row ───────────────────────────────────────────────

#[test]
fn test_store_large_row() {
    let store = ColdStore::new_in_memory();
    let big_text = "X".repeat(100_000);
    let row = OwnedRow {
        values: vec![Datum::Int64(1), Datum::Text(big_text.clone())],
    };
    let handle = store.store_row(&row).unwrap();
    let recovered = store.read_row(&handle).unwrap();

    match &recovered.values[1] {
        Datum::Text(s) => assert_eq!(s.len(), 100_000),
        other => panic!("expected Text, got {:?}", other),
    }
}

// ─── CompressionCodec roundtrip ─────────────────────────────────────────

#[test]
fn test_codec_from_u8_roundtrip() {
    for v in 0..=1u8 {
        let codec = CompressionCodec::from_u8(v).unwrap();
        assert_eq!(codec as u8, v);
    }
    assert!(CompressionCodec::from_u8(2).is_none());
    assert!(CompressionCodec::from_u8(255).is_none());
}
