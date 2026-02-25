//! Integration tests for Structured LSN & Reservation-Based Allocation.
//!
//! Tests cover:
//! - End-to-end reservation + cursor workflow
//! - Multi-writer concurrent allocation (no overlaps)
//! - Segment boundary exhaustion and rollover
//! - Crash recovery from segment headers
//! - Performance guardrail: ≥90% reduction in atomic ops vs per-record

use std::sync::atomic::Ordering;
use std::sync::Arc;

use falcon_storage::structured_lsn::*;

// ═══════════════════════════════════════════════════════════════════════════
// End-to-end: reserve → cursor → allocate → verify LSN ordering
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_e2e_single_writer_flow() {
    let alloc = LsnAllocator::new(LsnAllocatorConfig::default(), 0, 0);

    // Simulate a writer thread: reserve, allocate records, reserve again
    let mut all_lsns: Vec<u64> = Vec::new();
    let record_size = 128u64;

    let result = alloc.reserve(record_size * 100);
    let mut cursor = WriterCursor::new(*result.reservation());

    for _ in 0..100 {
        if let Some(lsn) = cursor.allocate(record_size) {
            all_lsns.push(lsn.raw());
        } else {
            // Exhausted — get new reservation
            let result = alloc.reserve(record_size * 100);
            cursor.reset(*result.reservation());
            let lsn = cursor.allocate(record_size).unwrap();
            all_lsns.push(lsn.raw());
        }
    }

    // Verify strict monotonic ordering
    for i in 1..all_lsns.len() {
        assert!(
            all_lsns[i] > all_lsns[i - 1],
            "LSN not monotonically increasing at index {}: {} vs {}",
            i, all_lsns[i - 1], all_lsns[i]
        );
    }
}

#[test]
fn test_e2e_lsn_to_physical_position() {
    let alloc = LsnAllocator::new(LsnAllocatorConfig::default(), 0, 0);

    let result = alloc.reserve(1024);
    let mut cursor = WriterCursor::new(*result.reservation());

    let lsn = cursor.allocate(256).unwrap();
    let pos = lsn_to_physical(lsn);

    assert_eq!(pos.segment_id, 0);
    assert_eq!(pos.file_offset, SEGMENT_HEADER_SIZE); // first record starts after header

    let lsn2 = cursor.allocate(256).unwrap();
    let pos2 = lsn_to_physical(lsn2);
    assert_eq!(pos2.file_offset, SEGMENT_HEADER_SIZE + 256);
}

// ═══════════════════════════════════════════════════════════════════════════
// Multi-writer: concurrent reservation guarantees no overlap
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_multi_writer_no_lsn_overlap() {
    let alloc = Arc::new(LsnAllocator::new(
        LsnAllocatorConfig {
            segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            default_reservation_bytes: 4096,
            record_alignment: 8,
        },
        0, 0,
    ));

    let num_writers = 8;
    let records_per_writer = 500;
    let record_size = 64u64;

    let mut handles = Vec::new();
    for _ in 0..num_writers {
        let a = Arc::clone(&alloc);
        handles.push(std::thread::spawn(move || {
            let mut lsns = Vec::with_capacity(records_per_writer);
            let result = a.reserve(record_size * 10);
            let mut cursor = WriterCursor::new(*result.reservation());

            for _ in 0..records_per_writer {
                loop {
                    if let Some(lsn) = cursor.allocate(record_size) {
                        lsns.push(lsn.raw());
                        break;
                    } else {
                        let result = a.reserve(record_size * 10);
                        cursor.reset(*result.reservation());
                    }
                }
            }
            lsns
        }));
    }

    let mut all_lsns: Vec<u64> = Vec::new();
    for h in handles {
        all_lsns.extend(h.join().unwrap());
    }

    // Check uniqueness
    all_lsns.sort();
    for i in 1..all_lsns.len() {
        assert_ne!(
            all_lsns[i], all_lsns[i - 1],
            "Duplicate LSN detected: {}",
            all_lsns[i]
        );
    }

    assert_eq!(all_lsns.len(), num_writers * records_per_writer);
}

// ═══════════════════════════════════════════════════════════════════════════
// Segment boundary: fill segment, verify rollover, verify LSN continuity
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_segment_boundary_fill_and_rollover() {
    let segment_size = 4096u64;
    let config = LsnAllocatorConfig {
        segment_size,
        default_reservation_bytes: 1024,
        record_alignment: 8,
    };
    let alloc = LsnAllocator::new(config, 0, 0);

    let mut prev_segment = 0u64;
    let mut rollover_count = 0u64;
    let mut prev_lsn_raw = 0u64;

    // Allocate enough to cross several segments
    for _ in 0..50 {
        let result = alloc.reserve(1024);
        let r = result.reservation();

        if result.had_rollover() {
            rollover_count += 1;
            // After rollover, the new segment starts at offset 0
            assert_eq!(r.base_lsn.offset(), 0);
            assert!(r.base_lsn.segment_id() > prev_segment);
            prev_segment = r.base_lsn.segment_id();
        }

        // LSN must always increase
        assert!(
            r.base_lsn.raw() >= prev_lsn_raw,
            "LSN decreased after reservation: {} < {}",
            r.base_lsn.raw(), prev_lsn_raw
        );
        prev_lsn_raw = r.limit_lsn.raw();
    }

    assert!(
        rollover_count >= 10,
        "Expected multiple rollovers with tiny segments, got {}",
        rollover_count
    );
    assert_eq!(
        alloc.metrics.segment_rollover_total.load(Ordering::Relaxed),
        rollover_count
    );
}

#[test]
fn test_segment_rollover_produces_valid_headers() {
    let config = LsnAllocatorConfig {
        segment_size: 2048,
        default_reservation_bytes: 1024,
        record_alignment: 8,
    };
    let alloc = LsnAllocator::new(config, 0, 0);

    let mut headers: Vec<SegmentHeader> = Vec::new();

    for _ in 0..20 {
        let result = alloc.reserve(1024);
        if let ReserveResult::OkWithRollover { new_header, .. } = result {
            assert!(new_header.validate());
            assert_eq!(new_header.magic, SEGMENT_MAGIC);
            assert_eq!(new_header.version, 2);
            headers.push(new_header);
        }
    }

    // Headers should have increasing segment IDs
    for i in 1..headers.len() {
        assert!(headers[i].segment_id > headers[i - 1].segment_id);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Crash recovery: rebuild LSN state from headers only
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_crash_recovery_rebuild_lsn_state() {
    // Simulate: allocator wrote to 3 segments, then crashed
    let seg_size = DEFAULT_MAX_SEGMENT_SIZE;

    let mut h0 = SegmentHeader::new(0, seg_size, StructuredLsn::new(0, 0));
    h0.last_valid_offset = 100_000;
    h0.checksum = h0.compute_checksum();

    let mut h1 = SegmentHeader::new(1, seg_size, StructuredLsn::new(1, 0));
    h1.last_valid_offset = 200_000;
    h1.checksum = h1.compute_checksum();

    let mut h2 = SegmentHeader::new(2, seg_size, StructuredLsn::new(2, 0));
    h2.last_valid_offset = 50_000; // partially written at crash
    h2.checksum = h2.compute_checksum();

    let headers = vec![
        (0, h0.to_bytes()),
        (1, h1.to_bytes()),
        (2, h2.to_bytes()),
    ];

    let state = recover_from_headers(&headers);
    assert_eq!(state.segments.len(), 3);
    assert!(state.segments.iter().all(|s| s.header_valid));

    // Resume point should be at the end of the last segment
    assert_eq!(state.resume_lsn.segment_id(), 2);
    assert_eq!(state.resume_lsn.offset(), 50_000);

    // Now create an allocator from recovered state
    let alloc = LsnAllocator::new(LsnAllocatorConfig::default(), 0, 0);
    for seg in &state.segments {
        let mut hdr = SegmentHeader::new(
            seg.segment_id, DEFAULT_MAX_SEGMENT_SIZE, seg.start_lsn,
        );
        hdr.last_valid_offset = seg.last_valid_offset;
        alloc.recover_from_header(&hdr);
    }

    let (seg, off) = alloc.current_position();
    assert_eq!(seg, 2);
    assert_eq!(off, 50_000);

    // New reservations continue from recovered position
    let result = alloc.reserve(1024);
    let r = result.reservation();
    assert_eq!(r.base_lsn.segment_id(), 2);
    assert!(r.base_lsn.offset() >= 50_000);
}

#[test]
fn test_crash_recovery_partial_header_corruption() {
    // Segment 0: valid, Segment 1: corrupted header
    let mut h0 = SegmentHeader::new(0, DEFAULT_MAX_SEGMENT_SIZE, StructuredLsn::new(0, 0));
    h0.last_valid_offset = 80_000;
    h0.checksum = h0.compute_checksum();

    let mut h1_bytes = SegmentHeader::new(1, DEFAULT_MAX_SEGMENT_SIZE, StructuredLsn::new(1, 0)).to_bytes();
    h1_bytes[30] ^= 0xFF; // corrupt

    let headers = vec![
        (0, h0.to_bytes()),
        (1, h1_bytes),
    ];

    let state = recover_from_headers(&headers);
    assert_eq!(state.segments.len(), 2);
    assert!(state.segments[0].header_valid);
    assert!(!state.segments[1].header_valid);

    // Corrupted segment falls back to SEGMENT_HEADER_SIZE
    assert_eq!(state.segments[1].last_valid_offset, SEGMENT_HEADER_SIZE);
}

// ═══════════════════════════════════════════════════════════════════════════
// Group commit simulation: reserve_exact for a batch
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_group_commit_reserve_exact_batch() {
    let alloc = LsnAllocator::new(LsnAllocatorConfig {
        segment_size: DEFAULT_MAX_SEGMENT_SIZE,
        default_reservation_bytes: 256 * 1024,
        record_alignment: 8,
    }, 0, 0);

    // Simulate group commit: 32 records × 256 bytes = 8 KB exact
    let batch_records = 32u64;
    let record_size = 256u64;
    let batch_bytes = batch_records * record_size;

    let result = alloc.reserve_exact(batch_bytes);
    let r = result.reservation();
    assert_eq!(r.size(), batch_bytes);

    let mut cursor = WriterCursor::new(*r);
    let mut lsns = Vec::new();
    for _ in 0..batch_records {
        let lsn = cursor.allocate(record_size).unwrap();
        lsns.push(lsn);
    }
    assert!(cursor.is_exhausted());

    // All LSNs in same segment, monotonically increasing
    for i in 1..lsns.len() {
        assert_eq!(lsns[i].segment_id(), lsns[0].segment_id());
        assert!(lsns[i].raw() > lsns[i - 1].raw());
    }

    // Only 1 atomic touch for the entire batch
    assert_eq!(alloc.metrics.reservation_batches_total.load(Ordering::Relaxed), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// Alignment: FILE_FLAG_NO_BUFFERING / raw disk readiness
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_4k_alignment_for_direct_io() {
    let config = LsnAllocatorConfig {
        segment_size: DEFAULT_MAX_SEGMENT_SIZE,
        default_reservation_bytes: 64 * 1024,
        record_alignment: 4096,
    };
    let alloc = LsnAllocator::new(config, 0, 0);

    for _ in 0..20 {
        let result = alloc.reserve(100); // small request → aligned to 4096
        let r = result.reservation();
        assert_eq!(r.base_lsn.offset() % 4096, 0, "base not 4K aligned");
        assert_eq!(r.size() % 4096, 0, "size not 4K aligned");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Performance guardrail: atomic operation count reduction
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_perf_guardrail_atomic_reduction() {
    let config = LsnAllocatorConfig {
        segment_size: DEFAULT_MAX_SEGMENT_SIZE,
        default_reservation_bytes: 64 * 1024,
        record_alignment: 8,
    };
    let alloc = Arc::new(LsnAllocator::new(config, 0, 0));

    let num_threads = 4;
    let records_per_thread = 10_000;
    let record_size = 128u64;

    let mut handles = Vec::new();
    for _ in 0..num_threads {
        let a = Arc::clone(&alloc);
        handles.push(std::thread::spawn(move || {
            let result = a.reserve(record_size * 100);
            let mut cursor = WriterCursor::new(*result.reservation());
            for _ in 0..records_per_thread {
                if cursor.allocate(record_size).is_none() {
                    let result = a.reserve(record_size * 100);
                    cursor.reset(*result.reservation());
                    cursor.allocate(record_size).unwrap();
                }
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    let total_records = (num_threads * records_per_thread) as u64;
    let atomic_touches = alloc.metrics.reservation_batches_total.load(Ordering::Relaxed);
    let reduction_pct = (1.0 - atomic_touches as f64 / total_records as f64) * 100.0;

    assert!(
        reduction_pct >= 90.0,
        "Expected ≥90% reduction in atomic ops vs per-record baseline. \
         Got {:.1}% (atomic_touches={}, total_records={})",
        reduction_pct, atomic_touches, total_records
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Observability: metrics correctness
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_metrics_accuracy() {
    let config = LsnAllocatorConfig {
        segment_size: 8192,
        default_reservation_bytes: 2048,
        record_alignment: 8,
    };
    let alloc = LsnAllocator::new(config, 0, 0);

    // 10 reservations of 2048 in 8192 segment → 4 per segment → ~2 rollovers
    for _ in 0..10 {
        alloc.reserve(100);
    }

    let m = &alloc.metrics;
    assert_eq!(m.reservation_batches_total.load(Ordering::Relaxed), 10);

    let total_reserved = m.reserved_bytes_total.load(Ordering::Relaxed);
    assert!(total_reserved >= 10 * 2048, "expected ≥20480 reserved bytes, got {}", total_reserved);

    let rollovers = m.segment_rollover_total.load(Ordering::Relaxed);
    assert!(rollovers >= 2, "expected ≥2 rollovers, got {}", rollovers);

    let avg_util = m.avg_segment_utilization();
    assert!(avg_util > 0.5, "expected >50% segment utilization, got {:.2}", avg_util);

    let avg_res = m.avg_reservation_bytes();
    assert!(avg_res >= 2048.0, "expected avg reservation ≥2048, got {:.0}", avg_res);
}
