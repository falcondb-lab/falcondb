//! Gateway pressure + failover joint tests — v1.0.6
//!
//! Validates gateway determinism under load:
//! - Admission control: fast reject when overloaded
//! - Disposition classification correctness
//! - Concurrent admission control safety
//! - Metrics accumulation under pressure
//! - No silent hangs under overload

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use falcon_cluster::{
    DistributedQueryEngine, GatewayAdmissionConfig, GatewayAdmissionControl,
    GatewayDisposition, GatewayMetrics, GatewayMetricsSnapshot,
};
use falcon_cluster::sharded_engine::ShardedEngine;
fn make_engine(shards: u64) -> Arc<ShardedEngine> {
    Arc::new(ShardedEngine::new(shards))
}

// ─── GatewayDisposition tests ───────────────────────────────────────────

#[test]
fn test_disposition_is_success() {
    assert!(GatewayDisposition::LocalExec.is_success());
    assert!(GatewayDisposition::ForwardedToLeader.is_success());
    assert!(!GatewayDisposition::RejectNoLeader.is_success());
    assert!(!GatewayDisposition::RejectOverloaded.is_success());
    assert!(!GatewayDisposition::RejectTimeout.is_success());
}

#[test]
fn test_disposition_is_retryable() {
    assert!(!GatewayDisposition::LocalExec.is_retryable());
    assert!(!GatewayDisposition::ForwardedToLeader.is_retryable());
    assert!(!GatewayDisposition::RejectNoLeader.is_retryable());
    assert!(GatewayDisposition::RejectOverloaded.is_retryable());
    assert!(GatewayDisposition::RejectTimeout.is_retryable());
}

#[test]
fn test_disposition_as_str() {
    assert_eq!(GatewayDisposition::LocalExec.as_str(), "LOCAL_EXEC");
    assert_eq!(
        GatewayDisposition::ForwardedToLeader.as_str(),
        "FORWARDED_TO_LEADER"
    );
    assert_eq!(
        GatewayDisposition::RejectNoLeader.as_str(),
        "REJECT_NO_LEADER"
    );
    assert_eq!(
        GatewayDisposition::RejectOverloaded.as_str(),
        "REJECT_OVERLOADED"
    );
    assert_eq!(
        GatewayDisposition::RejectTimeout.as_str(),
        "REJECT_TIMEOUT"
    );
}

#[test]
fn test_disposition_display() {
    let d = GatewayDisposition::RejectOverloaded;
    assert_eq!(format!("{}", d), "REJECT_OVERLOADED");
}

// ─── Admission Control tests ────────────────────────────────────────────

#[test]
fn test_admission_unlimited_default() {
    let ac = GatewayAdmissionControl::default();
    // Unlimited — all acquires succeed
    for _ in 0..1000 {
        assert!(ac.try_acquire_inflight());
    }
    assert_eq!(ac.inflight(), 1000);

    for _ in 0..1000 {
        ac.release_inflight();
    }
    assert_eq!(ac.inflight(), 0);
}

#[test]
fn test_admission_inflight_limit() {
    let ac = GatewayAdmissionControl::new(GatewayAdmissionConfig {
        max_inflight: 5,
        max_forwarded: 0,
    });

    // Acquire up to limit
    for _ in 0..5 {
        assert!(ac.try_acquire_inflight(), "should acquire within limit");
    }
    assert_eq!(ac.inflight(), 5);

    // 6th should be rejected
    assert!(
        !ac.try_acquire_inflight(),
        "should reject when at limit"
    );
    assert_eq!(ac.inflight(), 5); // counter unchanged

    // Release one, then acquire should succeed
    ac.release_inflight();
    assert_eq!(ac.inflight(), 4);
    assert!(ac.try_acquire_inflight());
    assert_eq!(ac.inflight(), 5);

    // Clean up
    for _ in 0..5 {
        ac.release_inflight();
    }
    assert_eq!(ac.inflight(), 0);
}

#[test]
fn test_admission_forwarded_limit() {
    let ac = GatewayAdmissionControl::new(GatewayAdmissionConfig {
        max_inflight: 0,
        max_forwarded: 3,
    });

    for _ in 0..3 {
        assert!(ac.try_acquire_forwarded());
    }
    assert!(!ac.try_acquire_forwarded()); // 4th rejected

    ac.release_forwarded();
    assert!(ac.try_acquire_forwarded()); // slot freed

    for _ in 0..3 {
        ac.release_forwarded();
    }
    assert_eq!(ac.forwarded(), 0);
}

#[test]
fn test_admission_concurrent_safety() {
    let ac = Arc::new(GatewayAdmissionControl::new(GatewayAdmissionConfig {
        max_inflight: 100,
        max_forwarded: 0,
    }));

    let num_threads = 8;
    let ops_per_thread = 200;
    let acquired = Arc::new(AtomicU64::new(0));
    let rejected = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..num_threads {
        let ac = Arc::clone(&ac);
        let acq = Arc::clone(&acquired);
        let rej = Arc::clone(&rejected);
        handles.push(std::thread::spawn(move || {
            for _ in 0..ops_per_thread {
                if ac.try_acquire_inflight() {
                    acq.fetch_add(1, Ordering::Relaxed);
                    // Brief hold
                    std::thread::yield_now();
                    ac.release_inflight();
                } else {
                    rej.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let total_acq = acquired.load(Ordering::Relaxed);
    let total_rej = rejected.load(Ordering::Relaxed);
    let total_ops = (num_threads * ops_per_thread) as u64;

    assert_eq!(total_acq + total_rej, total_ops);
    assert_eq!(ac.inflight(), 0, "all slots should be released");

    println!(
        "concurrent admission: acquired={}, rejected={}, total={}",
        total_acq, total_rej, total_ops
    );
}

// ─── GatewayMetrics disposition recording ───────────────────────────────

#[test]
fn test_metrics_record_disposition() {
    let m = GatewayMetrics::new();

    m.record_disposition(GatewayDisposition::LocalExec);
    m.record_disposition(GatewayDisposition::LocalExec);
    m.record_disposition(GatewayDisposition::ForwardedToLeader);
    m.record_disposition(GatewayDisposition::RejectNoLeader);
    m.record_disposition(GatewayDisposition::RejectOverloaded);
    m.record_disposition(GatewayDisposition::RejectOverloaded);
    m.record_disposition(GatewayDisposition::RejectTimeout);

    let snap = m.snapshot();
    assert_eq!(snap.local_exec_total, 2);
    assert_eq!(snap.forward_total, 1);
    assert_eq!(snap.reject_no_leader, 1);
    assert_eq!(snap.reject_overloaded, 2);
    assert_eq!(snap.reject_timeout, 1);
    assert_eq!(snap.reject_total, 4); // 1 + 2 + 1
}

#[test]
fn test_metrics_concurrent_disposition() {
    let m = Arc::new(GatewayMetrics::new());
    let num_threads = 4;
    let ops_per_thread = 500;

    let mut handles = Vec::new();
    for t in 0..num_threads {
        let m = Arc::clone(&m);
        handles.push(std::thread::spawn(move || {
            for i in 0..ops_per_thread {
                let disp = match (t + i) % 3 {
                    0 => GatewayDisposition::LocalExec,
                    1 => GatewayDisposition::ForwardedToLeader,
                    _ => GatewayDisposition::RejectOverloaded,
                };
                m.record_disposition(disp);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let snap = m.snapshot();
    let total = snap.local_exec_total + snap.forward_total + snap.reject_total;
    let expected = (num_threads * ops_per_thread) as u64;
    assert_eq!(
        total, expected,
        "all dispositions should be counted: got {}, expected {}",
        total, expected
    );
}

// ─── Engine admission + metrics integration ─────────────────────────────

#[test]
fn test_engine_admission_snapshot() {
    let engine = make_engine(2);
    let dqe = DistributedQueryEngine::new_with_admission(
        engine,
        Duration::from_secs(5),
        GatewayAdmissionConfig {
            max_inflight: 100,
            max_forwarded: 50,
        },
    );

    let (inflight, forwarded) = dqe.admission_snapshot();
    assert_eq!(inflight, 0);
    assert_eq!(forwarded, 0);

    let snap = dqe.gateway_metrics_snapshot();
    assert_eq!(snap.local_exec_total, 0);
    assert_eq!(snap.reject_total, 0);
}

// ─── Timeout bounding: no hang under overload ───────────────────────────

#[test]
fn test_no_hang_under_pressure() {
    let start = Instant::now();
    let ac = GatewayAdmissionControl::new(GatewayAdmissionConfig {
        max_inflight: 1,
        max_forwarded: 1,
    });

    // Saturate
    assert!(ac.try_acquire_inflight());

    // 1000 rejections should be instant (no queuing)
    for _ in 0..1000 {
        assert!(!ac.try_acquire_inflight());
    }

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "1000 rejections should complete in <100ms, took {:?}",
        elapsed
    );

    ac.release_inflight();
}

// ─── Overload behavior: explicit reject, not long tail ──────────────────

#[test]
fn test_overload_explicit_reject_not_long_tail() {
    let ac = GatewayAdmissionControl::new(GatewayAdmissionConfig {
        max_inflight: 10,
        max_forwarded: 0,
    });
    let metrics = GatewayMetrics::new();

    // Fill up inflight
    for _ in 0..10 {
        assert!(ac.try_acquire_inflight());
    }

    // Subsequent requests should be explicitly rejected
    let mut reject_latencies = Vec::new();
    for _ in 0..100 {
        let start = Instant::now();
        let acquired = ac.try_acquire_inflight();
        let elapsed = start.elapsed().as_micros() as u64;
        reject_latencies.push(elapsed);

        if acquired {
            // Shouldn't happen since we're at limit
            ac.release_inflight();
            metrics.record_disposition(GatewayDisposition::LocalExec);
        } else {
            metrics.record_disposition(GatewayDisposition::RejectOverloaded);
        }
    }

    let snap = metrics.snapshot();
    assert_eq!(snap.reject_overloaded, 100);
    assert_eq!(snap.reject_total, 100);

    // All rejections should be sub-microsecond (no blocking)
    reject_latencies.sort();
    let p99 = reject_latencies[98]; // 99th percentile of 100 samples
    println!("reject p99={}µs", p99);
    assert!(
        p99 < 1000, // < 1ms
        "rejection should be near-instant, got p99={}µs",
        p99
    );

    // Clean up
    for _ in 0..10 {
        ac.release_inflight();
    }
}
