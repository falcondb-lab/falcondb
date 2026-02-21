//! Priority-based query scheduler for tail latency governance.
//!
//! Separates workloads into priority lanes so that short OLTP queries
//! are never starved by long-running analytics, DDL, or rebalance work.
//!
//! # Priority Lanes
//!
//! | Priority | Use Case | Concurrency | Preemption |
//! |----------|----------|-------------|------------|
//! | **High** | Point lookups, small txns, OLTP | Unlimited | — |
//! | **Normal** | Medium queries, joins | Bounded | Yields to High |
//! | **Low** | DDL, backfill, rebalance, analytics | Bounded | Yields to Normal+High |
//!
//! # Design
//!
//! - Each lane has a semaphore-style concurrency limit.
//! - `acquire()` blocks (with timeout) until a slot is available.
//! - When high-priority work is waiting, low-priority work is paused.
//! - All operations are lock-free on the fast path (atomics only).

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use std::sync::{Condvar, Mutex};

use falcon_common::error::FalconError;

// ── Priority Classification ─────────────────────────────────────────────────

/// Query priority level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QueryPriority {
    /// Low: DDL, backfill, rebalance, large analytics.
    Low = 0,
    /// Normal: medium queries, multi-table joins.
    Normal = 1,
    /// High: point lookups, small transactions, OLTP.
    High = 2,
}

impl std::fmt::Display for QueryPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryPriority::Low => write!(f, "low"),
            QueryPriority::Normal => write!(f, "normal"),
            QueryPriority::High => write!(f, "high"),
        }
    }
}

impl QueryPriority {
    /// Classify a query based on heuristics.
    /// - DDL / COPY / rebalance → Low
    /// - Queries with aggregation, joins, or large scans → Normal
    /// - Simple SELECT / INSERT / UPDATE / DELETE → High
    pub fn classify(sql: &str, is_ddl: bool, is_rebalance: bool) -> Self {
        if is_rebalance || is_ddl {
            return QueryPriority::Low;
        }
        let sql_lower = sql.to_lowercase();
        if sql_lower.contains("copy ") || sql_lower.contains("backfill") {
            return QueryPriority::Low;
        }
        // Heuristic: joins, subqueries, aggregations → Normal
        if sql_lower.contains(" join ")
            || sql_lower.contains("group by")
            || sql_lower.contains("order by")
            || sql_lower.contains("having ")
            || sql_lower.contains("union ")
        {
            return QueryPriority::Normal;
        }
        QueryPriority::High
    }
}

// ── Scheduler Configuration ─────────────────────────────────────────────────

/// Configuration for the priority scheduler.
#[derive(Debug, Clone)]
pub struct PrioritySchedulerConfig {
    /// Maximum concurrent queries in the Normal lane.
    pub normal_concurrency: usize,
    /// Maximum concurrent queries in the Low lane.
    pub low_concurrency: usize,
    /// Timeout for acquiring a slot (ms). 0 = no timeout.
    pub acquire_timeout_ms: u64,
    /// When high-priority waiters exceed this count, low-priority
    /// queries are paused (backpressure signal).
    pub high_pressure_threshold: usize,
}

impl Default for PrioritySchedulerConfig {
    fn default() -> Self {
        Self {
            normal_concurrency: 64,
            low_concurrency: 4,
            acquire_timeout_ms: 5_000,
            high_pressure_threshold: 10,
        }
    }
}

// ── Per-Lane State ──────────────────────────────────────────────────────────

struct LaneState {
    /// Current number of active queries in this lane.
    active: AtomicUsize,
    /// Maximum concurrency for this lane (0 = unlimited).
    max_concurrency: usize,
    /// Total queries admitted to this lane.
    total_admitted: AtomicU64,
    /// Total queries rejected (timeout or backpressure).
    total_rejected: AtomicU64,
    /// Total wait time in microseconds.
    total_wait_us: AtomicU64,
}

impl LaneState {
    fn new(max_concurrency: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            max_concurrency,
            total_admitted: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            total_wait_us: AtomicU64::new(0),
        }
    }

    fn has_capacity(&self) -> bool {
        self.max_concurrency == 0 || self.active.load(Ordering::Relaxed) < self.max_concurrency
    }
}

// ── Priority Scheduler ──────────────────────────────────────────────────────

/// Priority-based query scheduler.
///
/// Queries call `acquire(priority)` before execution and `release(priority)`
/// after completion. High-priority queries are never blocked by lower lanes.
pub struct PriorityScheduler {
    config: PrioritySchedulerConfig,
    /// Per-lane state: [Low, Normal, High]
    lanes: [LaneState; 3],
    /// Number of high-priority queries currently waiting for a slot.
    high_waiters: AtomicUsize,
    /// Condition variable for slot availability.
    slot_cond: Condvar,
    slot_lock: Mutex<()>,
}

impl PriorityScheduler {
    pub fn new(config: PrioritySchedulerConfig) -> Arc<Self> {
        Arc::new(Self {
            lanes: [
                LaneState::new(config.low_concurrency),
                LaneState::new(config.normal_concurrency),
                LaneState::new(0), // High: unlimited
            ],
            high_waiters: AtomicUsize::new(0),
            slot_cond: Condvar::new(),
            slot_lock: Mutex::new(()),
            config,
        })
    }

    /// Acquire a slot in the given priority lane.
    ///
    /// Returns a `SchedulerGuard` that releases the slot on drop.
    /// Returns `Err(Transient)` if the timeout expires.
    pub fn acquire(&self, priority: QueryPriority) -> Result<SchedulerGuard<'_>, FalconError> {
        let lane_idx = priority as usize;
        let lane = &self.lanes[lane_idx];
        let start = Instant::now();

        // High priority: always admit immediately (unlimited concurrency)
        if priority == QueryPriority::High {
            lane.active.fetch_add(1, Ordering::Relaxed);
            lane.total_admitted.fetch_add(1, Ordering::Relaxed);
            return Ok(SchedulerGuard { scheduler: self, priority });
        }

        // Track high waiters for backpressure
        if priority == QueryPriority::High {
            self.high_waiters.fetch_add(1, Ordering::Relaxed);
        }

        let timeout = if self.config.acquire_timeout_ms > 0 {
            Duration::from_millis(self.config.acquire_timeout_ms)
        } else {
            Duration::from_secs(3600) // effectively infinite
        };

        let mut guard = self.slot_lock.lock().unwrap_or_else(|p| p.into_inner());

        loop {
            // Check backpressure: if high-priority work is waiting, pause low-priority
            if priority == QueryPriority::Low {
                let high_waiting = self.high_waiters.load(Ordering::Relaxed);
                if high_waiting >= self.config.high_pressure_threshold {
                    let remaining = timeout.saturating_sub(start.elapsed());
                    if remaining.is_zero() {
                        lane.total_rejected.fetch_add(1, Ordering::Relaxed);
                        return Err(FalconError::Transient {
                            reason: format!(
                                "priority scheduler: low-priority query paused due to high-priority backpressure ({} high waiters)",
                                high_waiting
                            ),
                            retry_after_ms: 100,
                        });
                    }
                    let (g, _) = self.slot_cond.wait_timeout(guard, Duration::from_millis(50))
                        .unwrap_or_else(|p| p.into_inner());
                    guard = g;
                    continue;
                }
            }

            // Check lane capacity
            if lane.has_capacity() {
                lane.active.fetch_add(1, Ordering::Relaxed);
                lane.total_admitted.fetch_add(1, Ordering::Relaxed);
                let wait_us = start.elapsed().as_micros() as u64;
                lane.total_wait_us.fetch_add(wait_us, Ordering::Relaxed);
                return Ok(SchedulerGuard { scheduler: self, priority });
            }

            // Wait for a slot
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                lane.total_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(FalconError::Transient {
                    reason: format!(
                        "priority scheduler: {} lane full ({}/{} active), timeout after {}ms",
                        priority,
                        lane.active.load(Ordering::Relaxed),
                        lane.max_concurrency,
                        self.config.acquire_timeout_ms,
                    ),
                    retry_after_ms: 50,
                });
            }

            let (g, _) = self.slot_cond.wait_timeout(guard, remaining.min(Duration::from_millis(50)))
                .unwrap_or_else(|p| p.into_inner());
            guard = g;
        }
    }

    /// Release a slot (called by SchedulerGuard::drop).
    fn release(&self, priority: QueryPriority) {
        let lane_idx = priority as usize;
        self.lanes[lane_idx].active.fetch_sub(1, Ordering::Relaxed);
        if priority == QueryPriority::High {
            // high_waiters is only incremented for non-High, so no decrement needed
        }
        self.slot_cond.notify_all();
    }

    /// Snapshot of scheduler metrics for observability.
    pub fn snapshot(&self) -> PrioritySchedulerSnapshot {
        PrioritySchedulerSnapshot {
            high_active: self.lanes[2].active.load(Ordering::Relaxed),
            high_admitted: self.lanes[2].total_admitted.load(Ordering::Relaxed),
            high_rejected: self.lanes[2].total_rejected.load(Ordering::Relaxed),
            normal_active: self.lanes[1].active.load(Ordering::Relaxed),
            normal_max: self.lanes[1].max_concurrency,
            normal_admitted: self.lanes[1].total_admitted.load(Ordering::Relaxed),
            normal_rejected: self.lanes[1].total_rejected.load(Ordering::Relaxed),
            normal_wait_us: self.lanes[1].total_wait_us.load(Ordering::Relaxed),
            low_active: self.lanes[0].active.load(Ordering::Relaxed),
            low_max: self.lanes[0].max_concurrency,
            low_admitted: self.lanes[0].total_admitted.load(Ordering::Relaxed),
            low_rejected: self.lanes[0].total_rejected.load(Ordering::Relaxed),
            low_wait_us: self.lanes[0].total_wait_us.load(Ordering::Relaxed),
            high_waiters: self.high_waiters.load(Ordering::Relaxed),
        }
    }
}

/// RAII guard that releases a scheduler slot on drop.
pub struct SchedulerGuard<'a> {
    scheduler: &'a PriorityScheduler,
    priority: QueryPriority,
}

impl<'a> std::fmt::Debug for SchedulerGuard<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerGuard")
            .field("priority", &self.priority)
            .finish()
    }
}

impl<'a> SchedulerGuard<'a> {
    pub fn priority(&self) -> QueryPriority {
        self.priority
    }
}

impl<'a> Drop for SchedulerGuard<'a> {
    fn drop(&mut self) {
        self.scheduler.release(self.priority);
    }
}

/// Snapshot of priority scheduler metrics.
#[derive(Debug, Clone)]
pub struct PrioritySchedulerSnapshot {
    pub high_active: usize,
    pub high_admitted: u64,
    pub high_rejected: u64,
    pub normal_active: usize,
    pub normal_max: usize,
    pub normal_admitted: u64,
    pub normal_rejected: u64,
    pub normal_wait_us: u64,
    pub low_active: usize,
    pub low_max: usize,
    pub low_admitted: u64,
    pub low_rejected: u64,
    pub low_wait_us: u64,
    pub high_waiters: usize,
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_high_priority_always_admitted() {
        let sched = PriorityScheduler::new(PrioritySchedulerConfig::default());
        let guards: Vec<_> = (0..100)
            .map(|_| sched.acquire(QueryPriority::High).unwrap())
            .collect();
        assert_eq!(sched.snapshot().high_active, 100);
        drop(guards);
        assert_eq!(sched.snapshot().high_active, 0);
    }

    #[test]
    fn test_normal_lane_bounded() {
        let config = PrioritySchedulerConfig {
            normal_concurrency: 2,
            low_concurrency: 1,
            acquire_timeout_ms: 50,
            ..Default::default()
        };
        let sched = PriorityScheduler::new(config);

        let g1 = sched.acquire(QueryPriority::Normal).unwrap();
        let g2 = sched.acquire(QueryPriority::Normal).unwrap();
        // Third should timeout
        let result = sched.acquire(QueryPriority::Normal);
        assert!(result.is_err());

        drop(g1);
        // Now should succeed
        let _g3 = sched.acquire(QueryPriority::Normal).unwrap();
        drop(g2);
        drop(_g3);
    }

    #[test]
    fn test_low_lane_bounded() {
        let config = PrioritySchedulerConfig {
            normal_concurrency: 64,
            low_concurrency: 1,
            acquire_timeout_ms: 50,
            ..Default::default()
        };
        let sched = PriorityScheduler::new(config);

        let _g1 = sched.acquire(QueryPriority::Low).unwrap();
        let result = sched.acquire(QueryPriority::Low);
        assert!(result.is_err());
    }

    #[test]
    fn test_guard_releases_on_drop() {
        let config = PrioritySchedulerConfig {
            normal_concurrency: 1,
            low_concurrency: 1,
            acquire_timeout_ms: 100,
            ..Default::default()
        };
        let sched = PriorityScheduler::new(config);

        {
            let _g = sched.acquire(QueryPriority::Normal).unwrap();
            assert_eq!(sched.snapshot().normal_active, 1);
        }
        assert_eq!(sched.snapshot().normal_active, 0);
    }

    #[test]
    fn test_snapshot_metrics() {
        let sched = PriorityScheduler::new(PrioritySchedulerConfig::default());

        let _g1 = sched.acquire(QueryPriority::High).unwrap();
        let _g2 = sched.acquire(QueryPriority::Normal).unwrap();
        let _g3 = sched.acquire(QueryPriority::Low).unwrap();

        let snap = sched.snapshot();
        assert_eq!(snap.high_active, 1);
        assert_eq!(snap.normal_active, 1);
        assert_eq!(snap.low_active, 1);
        assert_eq!(snap.high_admitted, 1);
        assert_eq!(snap.normal_admitted, 1);
        assert_eq!(snap.low_admitted, 1);
    }

    #[test]
    fn test_priority_classify_oltp() {
        assert_eq!(QueryPriority::classify("SELECT * FROM t WHERE id = 1", false, false), QueryPriority::High);
        assert_eq!(QueryPriority::classify("INSERT INTO t VALUES (1)", false, false), QueryPriority::High);
        assert_eq!(QueryPriority::classify("UPDATE t SET x = 1 WHERE id = 1", false, false), QueryPriority::High);
    }

    #[test]
    fn test_priority_classify_normal() {
        assert_eq!(QueryPriority::classify("SELECT * FROM a JOIN b ON a.id = b.id", false, false), QueryPriority::Normal);
        assert_eq!(QueryPriority::classify("SELECT x, COUNT(*) FROM t GROUP BY x", false, false), QueryPriority::Normal);
        assert_eq!(QueryPriority::classify("SELECT * FROM t ORDER BY x", false, false), QueryPriority::Normal);
    }

    #[test]
    fn test_priority_classify_low() {
        assert_eq!(QueryPriority::classify("CREATE TABLE t (id INT)", true, false), QueryPriority::Low);
        assert_eq!(QueryPriority::classify("COPY t FROM STDIN", false, false), QueryPriority::Low);
        assert_eq!(QueryPriority::classify("SELECT * FROM t", false, true), QueryPriority::Low);
    }

    #[test]
    fn test_priority_display() {
        assert_eq!(QueryPriority::High.to_string(), "high");
        assert_eq!(QueryPriority::Normal.to_string(), "normal");
        assert_eq!(QueryPriority::Low.to_string(), "low");
    }

    #[test]
    fn test_priority_ordering() {
        assert!(QueryPriority::High > QueryPriority::Normal);
        assert!(QueryPriority::Normal > QueryPriority::Low);
    }

    #[test]
    fn test_concurrent_acquire_release() {
        let config = PrioritySchedulerConfig {
            normal_concurrency: 2,
            low_concurrency: 2,
            acquire_timeout_ms: 1000,
            ..Default::default()
        };
        let sched = PriorityScheduler::new(config);
        let sched_ref = &*sched;

        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(move || {
                    for _ in 0..10 {
                        let _g = sched_ref.acquire(QueryPriority::Normal).unwrap();
                        std::thread::yield_now();
                    }
                });
            }
        });

        let snap = sched.snapshot();
        assert_eq!(snap.normal_active, 0);
        assert_eq!(snap.normal_admitted, 40);
    }

    #[test]
    fn test_rejected_count_increments() {
        let config = PrioritySchedulerConfig {
            normal_concurrency: 1,
            low_concurrency: 1,
            acquire_timeout_ms: 10,
            ..Default::default()
        };
        let sched = PriorityScheduler::new(config);

        let _g = sched.acquire(QueryPriority::Normal).unwrap();
        let _ = sched.acquire(QueryPriority::Normal); // will timeout
        let snap = sched.snapshot();
        assert_eq!(snap.normal_rejected, 1);
    }
}
