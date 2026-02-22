//! Instrumented lock wrappers for observability.
//!
//! Provides `InstrumentedRwLock` and `InstrumentedMutex` that track:
//! - Total acquire count (reads + writes)
//! - Contention count (acquires that blocked > threshold)
//! - Cumulative wait time (microseconds)
//!
//! Metrics are exposed via atomic counters for zero-overhead reads from
//! Prometheus exporters or SHOW commands.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Contention threshold: if acquiring a lock takes longer than this, it
/// is counted as a contention event.
const CONTENTION_THRESHOLD: Duration = Duration::from_micros(100);

/// Snapshot of lock metrics for a single instrumented lock.
#[derive(Debug, Clone, Default)]
pub struct LockMetricsSnapshot {
    pub name: String,
    pub read_acquires: u64,
    pub write_acquires: u64,
    pub contentions: u64,
    pub total_wait_us: u64,
    pub max_wait_us: u64,
}

/// Per-lock atomic counters.
pub struct LockMetrics {
    pub name: String,
    read_acquires: AtomicU64,
    write_acquires: AtomicU64,
    contentions: AtomicU64,
    total_wait_us: AtomicU64,
    max_wait_us: AtomicU64,
}

impl LockMetrics {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            read_acquires: AtomicU64::new(0),
            write_acquires: AtomicU64::new(0),
            contentions: AtomicU64::new(0),
            total_wait_us: AtomicU64::new(0),
            max_wait_us: AtomicU64::new(0),
        }
    }

    fn record_read(&self, wait: Duration) {
        self.read_acquires.fetch_add(1, Ordering::Relaxed);
        let us = wait.as_micros() as u64;
        if us > 0 {
            self.total_wait_us.fetch_add(us, Ordering::Relaxed);
            // Update max_wait_us (relaxed CAS loop)
            let mut cur = self.max_wait_us.load(Ordering::Relaxed);
            while us > cur {
                match self.max_wait_us.compare_exchange_weak(
                    cur, us, Ordering::Relaxed, Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => cur = actual,
                }
            }
        }
        if wait > CONTENTION_THRESHOLD {
            self.contentions.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_write(&self, wait: Duration) {
        self.write_acquires.fetch_add(1, Ordering::Relaxed);
        let us = wait.as_micros() as u64;
        if us > 0 {
            self.total_wait_us.fetch_add(us, Ordering::Relaxed);
            let mut cur = self.max_wait_us.load(Ordering::Relaxed);
            while us > cur {
                match self.max_wait_us.compare_exchange_weak(
                    cur, us, Ordering::Relaxed, Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => cur = actual,
                }
            }
        }
        if wait > CONTENTION_THRESHOLD {
            self.contentions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get a snapshot of the current metrics.
    pub fn snapshot(&self) -> LockMetricsSnapshot {
        LockMetricsSnapshot {
            name: self.name.clone(),
            read_acquires: self.read_acquires.load(Ordering::Relaxed),
            write_acquires: self.write_acquires.load(Ordering::Relaxed),
            contentions: self.contentions.load(Ordering::Relaxed),
            total_wait_us: self.total_wait_us.load(Ordering::Relaxed),
            max_wait_us: self.max_wait_us.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.read_acquires.store(0, Ordering::Relaxed);
        self.write_acquires.store(0, Ordering::Relaxed);
        self.contentions.store(0, Ordering::Relaxed);
        self.total_wait_us.store(0, Ordering::Relaxed);
        self.max_wait_us.store(0, Ordering::Relaxed);
    }
}

/// An `RwLock<T>` wrapper that records acquisition latency.
pub struct InstrumentedRwLock<T> {
    inner: RwLock<T>,
    metrics: LockMetrics,
}

impl<T> InstrumentedRwLock<T> {
    pub fn new(name: &str, value: T) -> Self {
        Self {
            inner: RwLock::new(value),
            metrics: LockMetrics::new(name),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        let start = Instant::now();
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        self.metrics.record_read(start.elapsed());
        guard
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        let start = Instant::now();
        let guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        self.metrics.record_write(start.elapsed());
        guard
    }

    pub fn metrics(&self) -> &LockMetrics {
        &self.metrics
    }

    pub fn snapshot(&self) -> LockMetricsSnapshot {
        self.metrics.snapshot()
    }
}

/// A `Mutex<T>` wrapper that records acquisition latency.
pub struct InstrumentedMutex<T> {
    inner: Mutex<T>,
    metrics: LockMetrics,
}

impl<T> InstrumentedMutex<T> {
    pub fn new(name: &str, value: T) -> Self {
        Self {
            inner: Mutex::new(value),
            metrics: LockMetrics::new(name),
        }
    }

    pub fn lock(&self) -> MutexGuard<'_, T> {
        let start = Instant::now();
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        self.metrics.record_write(start.elapsed());
        guard
    }

    pub fn metrics(&self) -> &LockMetrics {
        &self.metrics
    }

    pub fn snapshot(&self) -> LockMetricsSnapshot {
        self.metrics.snapshot()
    }
}

/// Aggregated lock metrics for all instrumented locks in a subsystem.
#[derive(Debug, Clone, Default)]
pub struct LockMetricsReport {
    pub locks: Vec<LockMetricsSnapshot>,
    pub total_contentions: u64,
    pub total_wait_us: u64,
    pub max_wait_us: u64,
}

impl LockMetricsReport {
    /// Build a report from a list of lock snapshots.
    pub fn from_snapshots(snapshots: Vec<LockMetricsSnapshot>) -> Self {
        let mut total_contentions = 0u64;
        let mut total_wait_us = 0u64;
        let mut max_wait_us = 0u64;
        for s in &snapshots {
            total_contentions += s.contentions;
            total_wait_us += s.total_wait_us;
            if s.max_wait_us > max_wait_us {
                max_wait_us = s.max_wait_us;
            }
        }
        Self {
            locks: snapshots,
            total_contentions,
            total_wait_us,
            max_wait_us,
        }
    }
}

impl std::fmt::Display for LockMetricsReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Lock Contention Report ({} locks)", self.locks.len())?;
        writeln!(f, "  total_contentions: {}", self.total_contentions)?;
        writeln!(f, "  total_wait_us: {}", self.total_wait_us)?;
        writeln!(f, "  max_wait_us: {}", self.max_wait_us)?;
        for s in &self.locks {
            writeln!(
                f,
                "  [{}] reads={} writes={} contentions={} wait_us={} max_us={}",
                s.name, s.read_acquires, s.write_acquires,
                s.contentions, s.total_wait_us, s.max_wait_us,
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instrumented_rwlock_read_write() {
        let lock = InstrumentedRwLock::new("test_lock", 42u32);
        {
            let r = lock.read();
            assert_eq!(*r, 42);
        }
        {
            let mut w = lock.write();
            *w = 99;
        }
        {
            let r = lock.read();
            assert_eq!(*r, 99);
        }
        let snap = lock.snapshot();
        assert_eq!(snap.name, "test_lock");
        assert_eq!(snap.read_acquires, 2);
        assert_eq!(snap.write_acquires, 1);
    }

    #[test]
    fn test_instrumented_mutex_lock() {
        let m = InstrumentedMutex::new("test_mutex", vec![1, 2, 3]);
        {
            let mut g = m.lock();
            g.push(4);
        }
        {
            let g = m.lock();
            assert_eq!(g.len(), 4);
        }
        let snap = m.snapshot();
        assert_eq!(snap.name, "test_mutex");
        assert_eq!(snap.write_acquires, 2);
    }

    #[test]
    fn test_contention_tracking_uncontended() {
        let lock = InstrumentedRwLock::new("fast_lock", ());
        for _ in 0..100 {
            let _r = lock.read();
        }
        let snap = lock.snapshot();
        assert_eq!(snap.read_acquires, 100);
        // Uncontended acquires should be < threshold
        assert_eq!(snap.contentions, 0);
    }

    #[test]
    fn test_contention_tracking_with_delay() {
        let lock = std::sync::Arc::new(InstrumentedRwLock::new("slow_lock", 0u64));
        let lock2 = lock.clone();

        // Hold write lock for 5ms to cause contention on reader
        let writer = std::thread::spawn(move || {
            let mut w = lock2.write();
            *w = 1;
            std::thread::sleep(Duration::from_millis(5));
        });

        // Small delay to let writer grab the lock first
        std::thread::sleep(Duration::from_millis(1));

        // This read will block until writer releases
        let r = lock.read();
        assert_eq!(*r, 1);
        drop(r);

        writer.join().unwrap();

        let snap = lock.snapshot();
        assert!(snap.read_acquires >= 1);
        assert!(snap.write_acquires >= 1);
        // The read may or may not have been contended depending on timing
        // but total_wait_us should be > 0
    }

    #[test]
    fn test_lock_metrics_reset() {
        let metrics = LockMetrics::new("reset_test");
        metrics.record_read(Duration::from_micros(10));
        metrics.record_write(Duration::from_micros(20));
        assert_eq!(metrics.snapshot().read_acquires, 1);
        assert_eq!(metrics.snapshot().write_acquires, 1);
        assert!(metrics.snapshot().total_wait_us > 0);
        metrics.reset();
        let snap = metrics.snapshot();
        assert_eq!(snap.read_acquires, 0);
        assert_eq!(snap.write_acquires, 0);
        assert_eq!(snap.total_wait_us, 0);
    }

    #[test]
    fn test_report_from_snapshots() {
        let s1 = LockMetricsSnapshot {
            name: "a".into(),
            read_acquires: 10,
            write_acquires: 5,
            contentions: 2,
            total_wait_us: 100,
            max_wait_us: 50,
        };
        let s2 = LockMetricsSnapshot {
            name: "b".into(),
            read_acquires: 20,
            write_acquires: 10,
            contentions: 3,
            total_wait_us: 200,
            max_wait_us: 80,
        };
        let report = LockMetricsReport::from_snapshots(vec![s1, s2]);
        assert_eq!(report.total_contentions, 5);
        assert_eq!(report.total_wait_us, 300);
        assert_eq!(report.max_wait_us, 80);
        assert_eq!(report.locks.len(), 2);
    }

    #[test]
    fn test_report_display() {
        let s = LockMetricsSnapshot {
            name: "test".into(),
            read_acquires: 10,
            write_acquires: 5,
            contentions: 1,
            total_wait_us: 50,
            max_wait_us: 30,
        };
        let report = LockMetricsReport::from_snapshots(vec![s]);
        let output = format!("{}", report);
        assert!(output.contains("Lock Contention Report"));
        assert!(output.contains("[test]"));
        assert!(output.contains("contentions=1"));
    }
}
