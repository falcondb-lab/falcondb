//! I/O Scheduler — Priority-based unified I/O dispatch.
//!
//! All disk I/O flows through this scheduler so that user-facing query reads
//! always take precedence over background compaction and speculative prefetch.
//! Each priority lane has an independent token-bucket rate limiter to prevent
//! background work from starving foreground queries.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

use super::page::{PageData, PageId};

// ── I/O Priority ─────────────────────────────────────────────────────────────

/// Priority class for an I/O request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum IoPriority {
    /// Background: compaction, GC, checkpoint.
    Background = 0,
    /// Speculative: prefetch, read-ahead.
    Prefetch = 1,
    /// Foreground: user query data reads.
    Query = 2,
}

// ── I/O Request / Response ───────────────────────────────────────────────────

/// A pending read request submitted to the scheduler.
#[derive(Debug)]
pub struct IoRequest {
    pub page_id: PageId,
    pub file_path: PathBuf,
    pub offset: u64,
    pub length: u32,
    pub priority: IoPriority,
    /// Monotonic submit timestamp.
    pub submitted_at: Instant,
}

/// Result of a completed I/O.
#[derive(Debug)]
pub struct IoResponse {
    pub page_id: PageId,
    pub data: Result<PageData, IoError>,
    pub latency_us: u64,
}

/// I/O error types.
#[derive(Debug, Clone)]
pub enum IoError {
    NotFound(String),
    PermissionDenied(String),
    Io(String),
    RateLimited,
    Cancelled,
}

impl std::fmt::Display for IoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(s) => write!(f, "not found: {s}"),
            Self::PermissionDenied(s) => write!(f, "permission denied: {s}"),
            Self::Io(s) => write!(f, "I/O error: {s}"),
            Self::RateLimited => write!(f, "rate limited"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

// ── Token Bucket Rate Limiter ────────────────────────────────────────────────

/// Simple token-bucket rate limiter for I/O bandwidth control.
#[derive(Debug)]
pub struct TokenBucket {
    /// Maximum tokens (bytes or IOPS).
    capacity: u64,
    /// Current available tokens.
    tokens: AtomicU64,
    /// Tokens added per refill.
    refill_amount: u64,
    /// Last refill timestamp.
    last_refill: Mutex<Instant>,
    /// Refill interval in microseconds.
    refill_interval_us: u64,
}

impl TokenBucket {
    pub fn new(capacity: u64, refill_amount: u64, refill_interval_us: u64) -> Self {
        Self {
            capacity,
            tokens: AtomicU64::new(capacity),
            refill_amount,
            last_refill: Mutex::new(Instant::now()),
            refill_interval_us,
        }
    }

    /// Unlimited rate limiter (never blocks).
    pub fn unlimited() -> Self {
        Self::new(u64::MAX, u64::MAX, 1_000)
    }

    /// Try to consume `n` tokens.  Returns `true` if allowed.
    pub fn try_acquire(&self, n: u64) -> bool {
        self.maybe_refill();
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current < n {
                return false;
            }
            if self
                .tokens
                .compare_exchange_weak(current, current - n, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Force-acquire (for Query priority — never rate-limited).
    pub fn force_acquire(&self, n: u64) {
        self.maybe_refill();
        self.tokens.fetch_sub(n.min(self.tokens.load(Ordering::Relaxed)), Ordering::Relaxed);
    }

    fn maybe_refill(&self) {
        let mut last = self.last_refill.lock();
        let elapsed_us = last.elapsed().as_micros() as u64;
        if elapsed_us >= self.refill_interval_us {
            let refills = elapsed_us / self.refill_interval_us;
            let add = refills * self.refill_amount;
            let current = self.tokens.load(Ordering::Relaxed);
            let new_val = (current + add).min(self.capacity);
            self.tokens.store(new_val, Ordering::Release);
            *last = Instant::now();
        }
    }

    pub fn available(&self) -> u64 {
        self.tokens.load(Ordering::Relaxed)
    }
}

// ── I/O Scheduler ────────────────────────────────────────────────────────────

/// Configuration for the I/O scheduler.
#[derive(Debug, Clone)]
pub struct IoSchedulerConfig {
    /// Max IOPS for background I/O (compaction, GC).
    pub background_iops_limit: u64,
    /// Max IOPS for prefetch I/O.
    pub prefetch_iops_limit: u64,
    /// Query I/O is unlimited (never throttled).
    /// Max pending requests per queue before new submissions are rejected.
    pub max_pending_per_queue: usize,
}

impl Default for IoSchedulerConfig {
    fn default() -> Self {
        Self {
            background_iops_limit: 500,
            prefetch_iops_limit: 200,
            max_pending_per_queue: 1024,
        }
    }
}

/// Scheduler statistics.
#[derive(Debug, Clone, Default)]
pub struct IoSchedulerStats {
    pub query_submitted: u64,
    pub prefetch_submitted: u64,
    pub background_submitted: u64,
    pub query_completed: u64,
    pub prefetch_completed: u64,
    pub background_completed: u64,
    pub rate_limited: u64,
    pub total_read_bytes: u64,
    pub total_latency_us: u64,
}

/// The unified I/O scheduler.
///
/// In production this would use `io_uring` (Linux) or `IOCP` (Windows).
/// This implementation uses synchronous file reads wrapped in tokio::spawn_blocking
/// as a portable baseline, with the scheduler logic handling prioritization.
pub struct IoScheduler {
    config: IoSchedulerConfig,
    /// Per-priority request queues.
    query_queue: Mutex<VecDeque<IoRequest>>,
    prefetch_queue: Mutex<VecDeque<IoRequest>>,
    background_queue: Mutex<VecDeque<IoRequest>>,
    /// Per-priority rate limiters.
    background_limiter: TokenBucket,
    prefetch_limiter: TokenBucket,
    /// Completed responses waiting to be consumed.
    completed: Mutex<VecDeque<IoResponse>>,
    /// Stats.
    pub stats: Mutex<IoSchedulerStats>,
}

impl IoScheduler {
    pub fn new(config: IoSchedulerConfig) -> Self {
        let bg_limiter = TokenBucket::new(
            config.background_iops_limit * 2,
            config.background_iops_limit,
            1_000_000, // refill every second
        );
        let pf_limiter = TokenBucket::new(
            config.prefetch_iops_limit * 2,
            config.prefetch_iops_limit,
            1_000_000,
        );
        Self {
            query_queue: Mutex::new(VecDeque::with_capacity(config.max_pending_per_queue)),
            prefetch_queue: Mutex::new(VecDeque::with_capacity(config.max_pending_per_queue)),
            background_queue: Mutex::new(VecDeque::with_capacity(config.max_pending_per_queue)),
            background_limiter: bg_limiter,
            prefetch_limiter: pf_limiter,
            completed: Mutex::new(VecDeque::new()),
            stats: Mutex::new(IoSchedulerStats::default()),
            config,
        }
    }

    /// Submit a read request.  Returns `Err` if the queue is full or rate-limited.
    pub fn submit(&self, req: IoRequest) -> Result<(), IoError> {
        match req.priority {
            IoPriority::Query => {
                let mut q = self.query_queue.lock();
                if q.len() >= self.config.max_pending_per_queue {
                    return Err(IoError::RateLimited);
                }
                self.stats.lock().query_submitted += 1;
                q.push_back(req);
            }
            IoPriority::Prefetch => {
                if !self.prefetch_limiter.try_acquire(1) {
                    self.stats.lock().rate_limited += 1;
                    return Err(IoError::RateLimited);
                }
                let mut q = self.prefetch_queue.lock();
                if q.len() >= self.config.max_pending_per_queue {
                    return Err(IoError::RateLimited);
                }
                self.stats.lock().prefetch_submitted += 1;
                q.push_back(req);
            }
            IoPriority::Background => {
                if !self.background_limiter.try_acquire(1) {
                    self.stats.lock().rate_limited += 1;
                    return Err(IoError::RateLimited);
                }
                let mut q = self.background_queue.lock();
                if q.len() >= self.config.max_pending_per_queue {
                    return Err(IoError::RateLimited);
                }
                self.stats.lock().background_submitted += 1;
                q.push_back(req);
            }
        }
        Ok(())
    }

    /// Drain the next batch of requests, prioritised: Query > Prefetch > Background.
    /// Returns up to `max_batch` requests.
    pub fn drain_batch(&self, max_batch: usize) -> Vec<IoRequest> {
        let mut batch = Vec::with_capacity(max_batch);

        // 1. Query (highest priority — drain all pending).
        {
            let mut q = self.query_queue.lock();
            while batch.len() < max_batch {
                match q.pop_front() {
                    Some(r) => batch.push(r),
                    None => break,
                }
            }
        }

        // 2. Prefetch.
        if batch.len() < max_batch {
            let mut q = self.prefetch_queue.lock();
            while batch.len() < max_batch {
                match q.pop_front() {
                    Some(r) => batch.push(r),
                    None => break,
                }
            }
        }

        // 3. Background.
        if batch.len() < max_batch {
            let mut q = self.background_queue.lock();
            while batch.len() < max_batch {
                match q.pop_front() {
                    Some(r) => batch.push(r),
                    None => break,
                }
            }
        }

        batch
    }

    /// Record a completed I/O.
    pub fn complete(&self, resp: IoResponse) {
        let mut stats = self.stats.lock();
        stats.total_latency_us += resp.latency_us;
        if let Ok(ref data) = resp.data {
            stats.total_read_bytes += data.len() as u64;
        }
        stats.query_completed += 1;
        self.completed.lock().push_back(resp);
    }

    /// Take all completed responses.
    pub fn take_completed(&self) -> Vec<IoResponse> {
        let mut c = self.completed.lock();
        c.drain(..).collect()
    }

    /// Execute a single synchronous read (portable fallback).
    /// In production, this would be replaced by io_uring / IOCP.
    pub fn execute_read_sync(req: &IoRequest) -> IoResponse {
        let start = Instant::now();
        let result = std::fs::File::open(&req.file_path)
            .and_then(|f| {
                use std::io::{Read, Seek, SeekFrom};
                let mut f = f;
                f.seek(SeekFrom::Start(req.offset))?;
                let mut buf = vec![0u8; req.length as usize];
                f.read_exact(&mut buf)?;
                Ok(PageData::new(buf))
            })
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => IoError::NotFound(e.to_string()),
                std::io::ErrorKind::PermissionDenied => IoError::PermissionDenied(e.to_string()),
                _ => IoError::Io(e.to_string()),
            });

        IoResponse {
            page_id: req.page_id,
            data: result,
            latency_us: start.elapsed().as_micros() as u64,
        }
    }

    /// Cancel all pending requests for a specific page.
    pub fn cancel_page(&self, page_id: PageId) {
        for q in [&self.query_queue, &self.prefetch_queue, &self.background_queue] {
            q.lock().retain(|r| r.page_id != page_id);
        }
    }

    pub fn pending_count(&self) -> (usize, usize, usize) {
        (
            self.query_queue.lock().len(),
            self.prefetch_queue.lock().len(),
            self.background_queue.lock().len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_basic() {
        let tb = TokenBucket::new(10, 5, 1_000_000);
        assert!(tb.try_acquire(5));
        assert!(tb.try_acquire(5));
        assert!(!tb.try_acquire(1)); // exhausted
    }

    #[test]
    fn test_token_bucket_unlimited() {
        let tb = TokenBucket::unlimited();
        assert!(tb.try_acquire(u64::MAX / 2));
    }

    #[test]
    fn test_scheduler_priority_ordering() {
        let sched = IoScheduler::new(IoSchedulerConfig {
            background_iops_limit: 10000,
            prefetch_iops_limit: 10000,
            max_pending_per_queue: 100,
        });

        // Submit background first, then prefetch, then query.
        sched.submit(IoRequest {
            page_id: PageId(1),
            file_path: PathBuf::from("bg.dat"),
            offset: 0,
            length: 4096,
            priority: IoPriority::Background,
            submitted_at: Instant::now(),
        }).unwrap();

        sched.submit(IoRequest {
            page_id: PageId(2),
            file_path: PathBuf::from("pf.dat"),
            offset: 0,
            length: 4096,
            priority: IoPriority::Prefetch,
            submitted_at: Instant::now(),
        }).unwrap();

        sched.submit(IoRequest {
            page_id: PageId(3),
            file_path: PathBuf::from("q.dat"),
            offset: 0,
            length: 4096,
            priority: IoPriority::Query,
            submitted_at: Instant::now(),
        }).unwrap();

        // Drain — query should come first regardless of submission order.
        let batch = sched.drain_batch(10);
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].page_id, PageId(3)); // Query
        assert_eq!(batch[1].page_id, PageId(2)); // Prefetch
        assert_eq!(batch[2].page_id, PageId(1)); // Background
    }

    #[test]
    fn test_scheduler_queue_limit() {
        let sched = IoScheduler::new(IoSchedulerConfig {
            background_iops_limit: 100000,
            prefetch_iops_limit: 100000,
            max_pending_per_queue: 2,
        });

        for i in 0..2 {
            sched.submit(IoRequest {
                page_id: PageId(i),
                file_path: PathBuf::from("test.dat"),
                offset: 0,
                length: 4096,
                priority: IoPriority::Query,
                submitted_at: Instant::now(),
            }).unwrap();
        }

        // Third submission should fail.
        let result = sched.submit(IoRequest {
            page_id: PageId(99),
            file_path: PathBuf::from("test.dat"),
            offset: 0,
            length: 4096,
            priority: IoPriority::Query,
            submitted_at: Instant::now(),
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_cancel_page() {
        let sched = IoScheduler::new(IoSchedulerConfig::default());
        for i in 0..5 {
            sched.submit(IoRequest {
                page_id: PageId(i % 2), // pages 0 and 1 alternating
                file_path: PathBuf::from("test.dat"),
                offset: 0,
                length: 4096,
                priority: IoPriority::Query,
                submitted_at: Instant::now(),
            }).unwrap();
        }
        sched.cancel_page(PageId(0));
        let batch = sched.drain_batch(100);
        // Only page 1 requests should remain.
        assert!(batch.iter().all(|r| r.page_id == PageId(1)));
    }
}
