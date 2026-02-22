use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A single slow query entry.
#[derive(Debug, Clone)]
pub struct SlowQueryEntry {
    /// The SQL text.
    pub sql: String,
    /// Execution duration.
    pub duration: Duration,
    /// Wall-clock timestamp (epoch millis) when the query completed.
    pub timestamp_ms: u64,
    /// Session ID that executed the query.
    pub session_id: i32,
}

/// Thread-safe slow query log with a configurable threshold and ring buffer.
///
/// Queries whose execution time exceeds `threshold` are recorded.
/// The log retains at most `capacity` recent entries (FIFO eviction).
pub struct SlowQueryLog {
    inner: Mutex<Inner>,
}

struct Inner {
    /// Minimum duration for a query to be logged. `Duration::ZERO` means disabled.
    threshold: Duration,
    /// Ring buffer of recent slow queries.
    entries: VecDeque<SlowQueryEntry>,
    /// Maximum number of entries to keep.
    capacity: usize,
    /// Total number of slow queries observed (may exceed capacity).
    total_count: u64,
}

impl SlowQueryLog {
    /// Create a new slow query log.
    /// `threshold` of `Duration::ZERO` means logging is disabled.
    /// `capacity` is the maximum number of entries retained.
    pub fn new(threshold: Duration, capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                threshold,
                entries: VecDeque::with_capacity(capacity.min(1024)),
                capacity,
                total_count: 0,
            }),
        }
    }

    /// Create a disabled slow query log (threshold = 0, capacity = 100).
    pub fn disabled() -> Self {
        Self::new(Duration::ZERO, 100)
    }

    /// Record a query if it exceeds the threshold.
    /// Returns `true` if the query was logged.
    pub fn record(&self, sql: &str, duration: Duration, session_id: i32) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if inner.threshold.is_zero() || duration < inner.threshold {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        let entry = SlowQueryEntry {
            sql: sql.to_string(),
            duration,
            timestamp_ms: now,
            session_id,
        };
        inner.total_count += 1;
        if inner.entries.len() >= inner.capacity {
            inner.entries.pop_front();
        }
        inner.entries.push_back(entry);
        true
    }

    /// Get the current threshold.
    pub fn threshold(&self) -> Duration {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).threshold
    }

    /// Set the threshold. `Duration::ZERO` disables logging.
    pub fn set_threshold(&self, threshold: Duration) {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).threshold = threshold;
    }

    /// Get a snapshot of all entries and the total count.
    pub fn snapshot(&self) -> (Vec<SlowQueryEntry>, u64) {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        (inner.entries.iter().cloned().collect(), inner.total_count)
    }

    /// Clear all entries and reset the counter.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.entries.clear();
        inner.total_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_log_does_not_record() {
        let log = SlowQueryLog::disabled();
        let recorded = log.record("SELECT 1", Duration::from_millis(500), 1);
        assert!(!recorded);
        let (entries, count) = log.snapshot();
        assert!(entries.is_empty());
        assert_eq!(count, 0);
    }

    #[test]
    fn test_records_slow_query() {
        let log = SlowQueryLog::new(Duration::from_millis(100), 10);
        // Below threshold — not recorded
        assert!(!log.record("SELECT 1", Duration::from_millis(50), 1));
        // At or above threshold — recorded
        assert!(log.record("SELECT * FROM big_table", Duration::from_millis(150), 2));
        let (entries, count) = log.snapshot();
        assert_eq!(entries.len(), 1);
        assert_eq!(count, 1);
        assert_eq!(entries[0].sql, "SELECT * FROM big_table");
        assert_eq!(entries[0].session_id, 2);
    }

    #[test]
    fn test_ring_buffer_eviction() {
        let log = SlowQueryLog::new(Duration::from_millis(1), 3);
        log.record("q1", Duration::from_millis(10), 1);
        log.record("q2", Duration::from_millis(10), 1);
        log.record("q3", Duration::from_millis(10), 1);
        log.record("q4", Duration::from_millis(10), 1);
        let (entries, count) = log.snapshot();
        assert_eq!(count, 4);
        assert_eq!(entries.len(), 3);
        // q1 was evicted
        assert_eq!(entries[0].sql, "q2");
        assert_eq!(entries[2].sql, "q4");
    }

    #[test]
    fn test_set_threshold() {
        let log = SlowQueryLog::disabled();
        assert!(log.threshold().is_zero());
        log.set_threshold(Duration::from_millis(200));
        assert_eq!(log.threshold(), Duration::from_millis(200));
        assert!(log.record("slow", Duration::from_millis(300), 1));
    }

    #[test]
    fn test_clear() {
        let log = SlowQueryLog::new(Duration::from_millis(1), 10);
        log.record("q1", Duration::from_millis(10), 1);
        log.clear();
        let (entries, count) = log.snapshot();
        assert!(entries.is_empty());
        assert_eq!(count, 0);
    }
}
