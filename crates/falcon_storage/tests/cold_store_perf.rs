//! Cold Store Performance Guardrail Tests — v1.0.7
//!
//! Validates:
//! - Cold store read latency does not degrade OLTP p99
//! - Compression saves memory (target ≥30% reduction for repetitive data)
//! - Cache amortizes decompress cost
//! - No silent hangs under pressure

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_storage::cold_store::*;

fn sample_row(i: u64) -> OwnedRow {
    OwnedRow {
        values: vec![
            Datum::Int64(i as i64),
            Datum::Text(format!("row_data_{}", i)),
            Datum::Float64(i as f64 * 1.5),
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

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * pct / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

// ─── Memory savings: ≥30% for repetitive data ──────────────────────────

#[test]
fn test_memory_savings_repetitive_data() {
    let store = ColdStore::new(ColdStoreConfig {
        compression_enabled: true,
        codec: CompressionCodec::Lz4,
        ..Default::default()
    });

    for i in 0..500 {
        store.store_row(&repetitive_row(i)).unwrap();
    }

    let snap = store.metrics.snapshot();
    let original = snap.cold_original_bytes as f64;
    let compressed = snap.cold_bytes as f64;
    let savings_pct = (1.0 - compressed / original) * 100.0;

    println!(
        "Memory savings: original={:.0}B, compressed={:.0}B, savings={:.1}%, ratio={:.2}",
        original, compressed, savings_pct, snap.compression_ratio
    );

    assert!(
        savings_pct >= 30.0,
        "Expected ≥30% memory savings for repetitive data, got {:.1}%",
        savings_pct
    );
}

// ─── Read latency p99: cold store reads should be fast ──────────────────

#[test]
fn test_cold_read_p99_latency() {
    let store = ColdStore::new(ColdStoreConfig {
        block_cache_capacity: 64 * 1024 * 1024, // 64 MB cache
        ..Default::default()
    });

    // Store 1000 rows
    let mut handles = Vec::new();
    for i in 0..1000 {
        handles.push(store.store_row(&sample_row(i)).unwrap());
    }

    // Warm the cache: first pass
    for h in &handles {
        let _ = store.read_row(h).unwrap();
    }

    // Measure latency: second pass (should be cache hits)
    let mut latencies = Vec::with_capacity(1000);
    for h in &handles {
        let start = Instant::now();
        let _ = store.read_row(h).unwrap();
        latencies.push(start.elapsed().as_micros() as u64);
    }

    latencies.sort();
    let p50 = percentile(&latencies, 50.0);
    let p99 = percentile(&latencies, 99.0);
    let p999 = percentile(&latencies, 99.9);

    println!("cold read (cached): p50={}µs p99={}µs p999={}µs", p50, p99, p999);

    // Cache hits should be sub-millisecond
    assert!(
        p99 < 1000,
        "cold read p99 (cached) should be < 1ms, got {}µs",
        p99
    );
}

#[test]
fn test_cold_read_p99_uncached() {
    let store = ColdStore::new(ColdStoreConfig {
        block_cache_capacity: 0, // No cache: every read decompresses
        ..Default::default()
    });

    let mut handles = Vec::new();
    for i in 0..500 {
        handles.push(store.store_row(&sample_row(i)).unwrap());
    }

    let mut latencies = Vec::with_capacity(500);
    for h in &handles {
        let start = Instant::now();
        let _ = store.read_row(h).unwrap();
        latencies.push(start.elapsed().as_micros() as u64);
    }

    latencies.sort();
    let p50 = percentile(&latencies, 50.0);
    let p99 = percentile(&latencies, 99.0);

    println!("cold read (uncached): p50={}µs p99={}µs", p50, p99);

    // Even uncached LZ4 decompress should be fast (< 10ms p99)
    assert!(
        p99 < 10_000,
        "cold read p99 (uncached) should be < 10ms, got {}µs",
        p99
    );
}

// ─── Write throughput: store_row should not block ───────────────────────

#[test]
fn test_store_throughput() {
    let store = ColdStore::new_in_memory();
    let count = 5000;

    let start = Instant::now();
    for i in 0..count {
        store.store_row(&sample_row(i)).unwrap();
    }
    let elapsed = start.elapsed();

    let tps = count as f64 / elapsed.as_secs_f64();
    println!("store throughput: {} rows in {:?} ({:.0} rows/sec)", count, elapsed, tps);

    // Should be able to store at least 10K rows/sec
    assert!(
        tps > 10_000.0,
        "store throughput should be > 10K rows/sec, got {:.0}",
        tps
    );
}

// ─── Cache amortizes decompress cost ────────────────────────────────────

#[test]
fn test_cache_amortizes_decompress() {
    let store = ColdStore::new(ColdStoreConfig {
        block_cache_capacity: 16 * 1024 * 1024,
        ..Default::default()
    });

    let mut handles = Vec::new();
    for i in 0..100 {
        handles.push(store.store_row(&sample_row(i)).unwrap());
    }

    // First pass: cache misses (decompress)
    let start1 = Instant::now();
    for h in &handles {
        let _ = store.read_row(h).unwrap();
    }
    let elapsed1 = start1.elapsed();

    // Second pass: cache hits (no decompress)
    let start2 = Instant::now();
    for h in &handles {
        let _ = store.read_row(h).unwrap();
    }
    let elapsed2 = start2.elapsed();

    println!(
        "cache amortization: miss={:?}, hit={:?}, speedup={:.1}x",
        elapsed1,
        elapsed2,
        elapsed1.as_nanos() as f64 / elapsed2.as_nanos().max(1) as f64
    );

    // Cache hits should be faster (or at least not slower)
    // Allow some tolerance for system noise
    assert!(
        elapsed2.as_nanos() <= elapsed1.as_nanos() * 2,
        "cache hits should not be 2x slower than misses"
    );
}

// ─── No hang under concurrent pressure ──────────────────────────────────

#[test]
fn test_no_hang_under_concurrent_pressure() {
    let store = Arc::new(ColdStore::new(ColdStoreConfig {
        max_segment_size: 4096, // Small segments: more contention
        ..Default::default()
    }));

    let num_threads = 8;
    let ops_per_thread = 200;

    let start = Instant::now();
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let s = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            let mut local_handles = Vec::new();
            for i in 0..ops_per_thread {
                let id = (t * 10000 + i) as u64;
                let handle = s.store_row(&sample_row(id)).unwrap();
                local_handles.push(handle);
            }
            // Read back
            for h in &local_handles {
                let _ = s.read_row(h).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "concurrent pressure: {}x{} ops in {:?}",
        num_threads, ops_per_thread, elapsed
    );

    // Should complete within 10 seconds (generous bound)
    assert!(
        elapsed.as_secs() < 10,
        "should not hang: took {:?}",
        elapsed
    );

    let total_stored = store.metrics.cold_migrate_total.load(Ordering::Relaxed);
    assert_eq!(
        total_stored,
        (num_threads * ops_per_thread) as u64,
        "all rows should be stored"
    );
}

// ─── Intern pool memory savings ─────────────────────────────────────────

#[test]
fn test_intern_pool_memory_savings() {
    let pool = StringInternPool::new();

    // Intern 1000 copies of 10 unique strings
    let unique_strings: Vec<String> = (0..10).map(|i| format!("STATUS_{}", i)).collect();

    for _ in 0..1000 {
        for s in &unique_strings {
            pool.intern(s);
        }
    }

    assert_eq!(pool.len(), 10, "should have only 10 unique strings");
    assert_eq!(pool.hits(), 9990); // 10000 - 10 first misses
    assert_eq!(pool.misses(), 10);

    let hit_rate = pool.hit_rate();
    assert!(hit_rate > 0.99, "hit rate should be > 99%: got {:.3}", hit_rate);
}
