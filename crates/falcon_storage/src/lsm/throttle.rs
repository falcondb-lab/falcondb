use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// I/O rate limiter for flush and compaction background work.
/// Token-bucket style: refills `rate` bytes per second, burst up to `rate`.
pub struct RateLimiter {
    rate: AtomicU64,
    tokens: Mutex<f64>,
    last_refill: Mutex<Instant>,
}

impl RateLimiter {
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            rate: AtomicU64::new(bytes_per_sec),
            tokens: Mutex::new(bytes_per_sec as f64),
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Unlimited throughput (no throttling).
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    pub fn set_rate(&self, bytes_per_sec: u64) {
        self.rate.store(bytes_per_sec, Ordering::Relaxed);
    }

    pub fn rate(&self) -> u64 {
        self.rate.load(Ordering::Relaxed)
    }

    /// Request `n` bytes of I/O budget. Returns how long to sleep before proceeding.
    /// If rate == 0, returns Duration::ZERO (unlimited).
    pub fn request(&self, n: u64) -> Duration {
        let rate = self.rate.load(Ordering::Relaxed);
        if rate == 0 {
            return Duration::ZERO;
        }

        let mut tokens = self.tokens.lock();
        let mut last = self.last_refill.lock();

        // Refill tokens based on elapsed time
        let now = Instant::now();
        let elapsed = now.duration_since(*last).as_secs_f64();
        *tokens = (*tokens + elapsed * rate as f64).min(rate as f64);
        *last = now;

        *tokens -= n as f64;
        if *tokens >= 0.0 {
            Duration::ZERO
        } else {
            let deficit = -*tokens;
            Duration::from_secs_f64(deficit / rate as f64)
        }
    }
}

/// Backpressure state — checked before accepting writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureLevel {
    /// Normal operation.
    Normal,
    /// Approaching limits — writes may be delayed.
    Delayed,
    /// At hard limit — writes must be rejected (retryable error).
    Stopped,
}

/// Backpressure configuration thresholds.
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Immutable memtable count that triggers Delayed.
    pub immutable_delay_threshold: usize,
    /// Immutable memtable count that triggers Stopped.
    pub immutable_stop_threshold: usize,
    /// L0 file count that triggers Delayed (soft compaction trigger).
    pub l0_delay_threshold: usize,
    /// L0 file count that triggers Stopped (hard limit).
    pub l0_stop_threshold: usize,
    /// Pending compaction bytes that triggers Delayed.
    pub compaction_backlog_delay_bytes: u64,
    /// Pending compaction bytes that triggers Stopped.
    pub compaction_backlog_stop_bytes: u64,
    /// How long to sleep per Delayed write (microseconds).
    pub delay_us: u64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            immutable_delay_threshold: 3,
            immutable_stop_threshold: 6,
            l0_delay_threshold: 8,
            l0_stop_threshold: 20,
            compaction_backlog_delay_bytes: 256 * 1024 * 1024,
            compaction_backlog_stop_bytes: 1024 * 1024 * 1024,
            delay_us: 1000,
        }
    }
}

/// Tracks current system load for backpressure decisions.
pub struct BackpressureController {
    pub config: BackpressureConfig,
    immutable_count: AtomicU64,
    l0_count: AtomicU64,
    compaction_backlog_bytes: AtomicU64,
    stopped: AtomicBool,
    writes_delayed: AtomicU64,
    writes_stopped: AtomicU64,
}

impl BackpressureController {
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            config,
            immutable_count: AtomicU64::new(0),
            l0_count: AtomicU64::new(0),
            compaction_backlog_bytes: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
            writes_delayed: AtomicU64::new(0),
            writes_stopped: AtomicU64::new(0),
        }
    }

    pub fn update_immutable_count(&self, count: usize) {
        self.immutable_count.store(count as u64, Ordering::Relaxed);
    }

    pub fn update_l0_count(&self, count: usize) {
        self.l0_count.store(count as u64, Ordering::Relaxed);
    }

    pub fn update_compaction_backlog(&self, bytes: u64) {
        self.compaction_backlog_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Evaluate current backpressure level.
    pub fn level(&self) -> BackpressureLevel {
        if self.stopped.load(Ordering::Relaxed) {
            return BackpressureLevel::Stopped;
        }
        let imm = self.immutable_count.load(Ordering::Relaxed) as usize;
        let l0 = self.l0_count.load(Ordering::Relaxed) as usize;
        let backlog = self.compaction_backlog_bytes.load(Ordering::Relaxed);

        if imm >= self.config.immutable_stop_threshold
            || l0 >= self.config.l0_stop_threshold
            || backlog >= self.config.compaction_backlog_stop_bytes
        {
            return BackpressureLevel::Stopped;
        }
        if imm >= self.config.immutable_delay_threshold
            || l0 >= self.config.l0_delay_threshold
            || backlog >= self.config.compaction_backlog_delay_bytes
        {
            return BackpressureLevel::Delayed;
        }
        BackpressureLevel::Normal
    }

    /// Check write admission. Returns Ok(()) or Err with a retryable error message.
    /// For Delayed level, sleeps briefly before returning Ok.
    pub fn check_write(&self) -> Result<(), BackpressureError> {
        match self.level() {
            BackpressureLevel::Normal => Ok(()),
            BackpressureLevel::Delayed => {
                self.writes_delayed.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_micros(self.config.delay_us));
                Ok(())
            }
            BackpressureLevel::Stopped => {
                self.writes_stopped.fetch_add(1, Ordering::Relaxed);
                Err(BackpressureError::WritesStopped {
                    immutable_count: self.immutable_count.load(Ordering::Relaxed) as usize,
                    l0_count: self.l0_count.load(Ordering::Relaxed) as usize,
                    compaction_backlog: self.compaction_backlog_bytes.load(Ordering::Relaxed),
                })
            }
        }
    }

    pub fn force_stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    pub fn clear_stop(&self) {
        self.stopped.store(false, Ordering::Relaxed);
    }

    pub fn writes_delayed(&self) -> u64 {
        self.writes_delayed.load(Ordering::Relaxed)
    }

    pub fn writes_stopped(&self) -> u64 {
        self.writes_stopped.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub enum BackpressureError {
    WritesStopped {
        immutable_count: usize,
        l0_count: usize,
        compaction_backlog: u64,
    },
}

impl std::fmt::Display for BackpressureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WritesStopped {
                immutable_count,
                l0_count,
                compaction_backlog,
            } => write!(
                f,
                "writes stopped: imm_count={} l0_count={} backlog={}B",
                immutable_count, l0_count, compaction_backlog
            ),
        }
    }
}

impl std::error::Error for BackpressureError {}

impl From<BackpressureError> for std::io::Error {
    fn from(e: BackpressureError) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_unlimited() {
        let rl = RateLimiter::unlimited();
        assert_eq!(rl.request(1_000_000), Duration::ZERO);
    }

    #[test]
    fn test_rate_limiter_burst() {
        let rl = RateLimiter::new(10_000);
        // First request within burst should not sleep
        assert_eq!(rl.request(5_000), Duration::ZERO);
    }

    #[test]
    fn test_rate_limiter_over_budget() {
        let rl = RateLimiter::new(10_000);
        let d = rl.request(20_000);
        // Should need to sleep ~1s for the 10K deficit
        assert!(d > Duration::from_millis(500));
    }

    #[test]
    fn test_backpressure_normal() {
        let bp = BackpressureController::new(BackpressureConfig::default());
        assert_eq!(bp.level(), BackpressureLevel::Normal);
        assert!(bp.check_write().is_ok());
    }

    #[test]
    fn test_backpressure_l0_stop() {
        let bp = BackpressureController::new(BackpressureConfig {
            l0_stop_threshold: 5,
            ..Default::default()
        });
        bp.update_l0_count(5);
        assert_eq!(bp.level(), BackpressureLevel::Stopped);
        assert!(bp.check_write().is_err());
    }

    #[test]
    fn test_backpressure_immutable_delay() {
        let bp = BackpressureController::new(BackpressureConfig {
            immutable_delay_threshold: 2,
            delay_us: 0, // no actual sleep in test
            ..Default::default()
        });
        bp.update_immutable_count(2);
        assert_eq!(bp.level(), BackpressureLevel::Delayed);
        assert!(bp.check_write().is_ok()); // delayed but ok
        assert_eq!(bp.writes_delayed(), 1);
    }

    #[test]
    fn test_backpressure_force_stop() {
        let bp = BackpressureController::new(BackpressureConfig::default());
        bp.force_stop();
        assert_eq!(bp.level(), BackpressureLevel::Stopped);
        bp.clear_stop();
        assert_eq!(bp.level(), BackpressureLevel::Normal);
    }
}
