//! Query Governor — enforces per-query resource limits.
//!
//! Limits enforced:
//! - `max_rows`: maximum rows returned per query
//! - `max_result_bytes`: maximum result set size in bytes
//! - `max_execution_time_ms`: maximum wall-clock execution time
//! - `max_memory_bytes`: maximum memory allocated per query
//!
//! When a limit is exceeded, returns `FalconError::Transient` (timeout)
//! or `FalconError::Sql(Unsupported)` (resource limit) as appropriate.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use falcon_common::error::{FalconError, FalconResult, SqlError};

/// Per-query resource limits configuration.
#[derive(Debug, Clone)]
pub struct QueryLimits {
    /// Maximum rows returned. 0 = unlimited.
    pub max_rows: u64,
    /// Maximum result set size in bytes. 0 = unlimited.
    pub max_result_bytes: u64,
    /// Maximum execution time in milliseconds. 0 = unlimited.
    pub max_execution_time_ms: u64,
    /// Maximum memory allocated per query in bytes. 0 = unlimited.
    pub max_memory_bytes: u64,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            max_rows: 1_000_000,
            max_result_bytes: 256 * 1024 * 1024, // 256MB
            max_execution_time_ms: 30_000,        // 30s
            max_memory_bytes: 512 * 1024 * 1024,  // 512MB
        }
    }
}

impl QueryLimits {
    /// No limits (for internal/admin queries).
    pub fn unlimited() -> Self {
        Self {
            max_rows: 0,
            max_result_bytes: 0,
            max_execution_time_ms: 0,
            max_memory_bytes: 0,
        }
    }

    /// Strict limits for untrusted/external queries.
    pub fn strict() -> Self {
        Self {
            max_rows: 100_000,
            max_result_bytes: 64 * 1024 * 1024, // 64MB
            max_execution_time_ms: 10_000,       // 10s
            max_memory_bytes: 128 * 1024 * 1024, // 128MB
        }
    }
}

/// Per-query resource tracker. Created at query start, checked throughout execution.
///
/// Usage:
/// ```ignore
/// let governor = QueryGovernor::new(limits);
/// // ... during execution:
/// governor.check_rows(rows_so_far)?;
/// governor.check_bytes(bytes_so_far)?;
/// governor.check_timeout()?;
/// governor.record_memory(allocated_bytes)?;
/// ```
pub struct QueryGovernor {
    limits: QueryLimits,
    started_at: Instant,
    memory_used: AtomicU64,
    rows_checked: AtomicU64,
    bytes_checked: AtomicU64,
}

impl QueryGovernor {
    /// Create a new governor with the given limits.
    pub fn new(limits: QueryLimits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            started_at: Instant::now(),
            memory_used: AtomicU64::new(0),
            rows_checked: AtomicU64::new(0),
            bytes_checked: AtomicU64::new(0),
        })
    }

    /// Create with default limits.
    pub fn default_limits() -> Arc<Self> {
        Self::new(QueryLimits::default())
    }

    /// Create with no limits (for internal queries).
    pub fn unlimited() -> Arc<Self> {
        Self::new(QueryLimits::unlimited())
    }

    /// Check row count limit. Call after each batch of rows is produced.
    pub fn check_rows(&self, total_rows: u64) -> FalconResult<()> {
        self.rows_checked.store(total_rows, Ordering::Relaxed);
        if self.limits.max_rows > 0 && total_rows > self.limits.max_rows {
            return Err(FalconError::Sql(SqlError::Unsupported(format!(
                "query result exceeds row limit: {} rows (max {})",
                total_rows, self.limits.max_rows
            ))));
        }
        Ok(())
    }

    /// Check result size limit. Call after each batch of bytes is produced.
    pub fn check_bytes(&self, total_bytes: u64) -> FalconResult<()> {
        self.bytes_checked.store(total_bytes, Ordering::Relaxed);
        if self.limits.max_result_bytes > 0 && total_bytes > self.limits.max_result_bytes {
            return Err(FalconError::Sql(SqlError::Unsupported(format!(
                "query result exceeds size limit: {} bytes (max {})",
                total_bytes, self.limits.max_result_bytes
            ))));
        }
        Ok(())
    }

    /// Check execution time limit. Call periodically during execution.
    pub fn check_timeout(&self) -> FalconResult<()> {
        if self.limits.max_execution_time_ms == 0 {
            return Ok(());
        }
        let elapsed_ms = self.started_at.elapsed().as_millis() as u64;
        if elapsed_ms > self.limits.max_execution_time_ms {
            return Err(FalconError::Transient {
                reason: format!(
                    "query execution timeout: {}ms elapsed (max {}ms)",
                    elapsed_ms, self.limits.max_execution_time_ms
                ),
                retry_after_ms: 0,
            });
        }
        Ok(())
    }

    /// Record memory allocation. Returns error if over budget.
    pub fn record_memory(&self, bytes: u64) -> FalconResult<()> {
        let current = self.memory_used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if self.limits.max_memory_bytes > 0 && current > self.limits.max_memory_bytes {
            self.memory_used.fetch_sub(bytes, Ordering::Relaxed);
            return Err(FalconError::Transient {
                reason: format!(
                    "query memory limit exceeded: {} bytes (max {})",
                    current, self.limits.max_memory_bytes
                ),
                retry_after_ms: 0,
            });
        }
        Ok(())
    }

    /// Release previously recorded memory.
    pub fn release_memory(&self, bytes: u64) {
        let current = self.memory_used.load(Ordering::Relaxed);
        self.memory_used.store(current.saturating_sub(bytes), Ordering::Relaxed);
    }

    /// Elapsed execution time in milliseconds.
    pub fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    /// Current memory usage in bytes.
    pub fn memory_used_bytes(&self) -> u64 {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Current row count (last checked).
    pub fn rows_produced(&self) -> u64 {
        self.rows_checked.load(Ordering::Relaxed)
    }

    /// Current result bytes (last checked).
    pub fn bytes_produced(&self) -> u64 {
        self.bytes_checked.load(Ordering::Relaxed)
    }

    /// Check all limits at once. Convenience method.
    pub fn check_all(&self, rows: u64, bytes: u64) -> FalconResult<()> {
        self.check_timeout()?;
        self.check_rows(rows)?;
        self.check_bytes(bytes)?;
        Ok(())
    }

    /// Get a snapshot of current resource usage.
    pub fn snapshot(&self) -> GovernorSnapshot {
        GovernorSnapshot {
            elapsed_ms: self.elapsed_ms(),
            rows_produced: self.rows_produced(),
            bytes_produced: self.bytes_produced(),
            memory_used_bytes: self.memory_used_bytes(),
            limits: self.limits.clone(),
        }
    }
}

/// Snapshot of query governor state for observability.
#[derive(Debug, Clone)]
pub struct GovernorSnapshot {
    pub elapsed_ms: u64,
    pub rows_produced: u64,
    pub bytes_produced: u64,
    pub memory_used_bytes: u64,
    pub limits: QueryLimits,
}

/// Global query governor configuration (shared across sessions).
pub struct QueryGovernorConfig {
    pub default_limits: QueryLimits,
    /// Whether to enforce limits on internal/system queries.
    pub enforce_on_internal: bool,
}

impl Default for QueryGovernorConfig {
    fn default() -> Self {
        Self {
            default_limits: QueryLimits::default(),
            enforce_on_internal: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_row_limit_ok() {
        let g = QueryGovernor::new(QueryLimits { max_rows: 100, ..QueryLimits::unlimited() });
        assert!(g.check_rows(50).is_ok());
        assert!(g.check_rows(100).is_ok());
    }

    #[test]
    fn test_row_limit_exceeded() {
        let g = QueryGovernor::new(QueryLimits { max_rows: 100, ..QueryLimits::unlimited() });
        let err = g.check_rows(101).unwrap_err();
        assert!(err.is_user_error(), "row limit should be user error");
        assert!(err.to_string().contains("row limit"));
    }

    #[test]
    fn test_bytes_limit_ok() {
        let g = QueryGovernor::new(QueryLimits { max_result_bytes: 1024, ..QueryLimits::unlimited() });
        assert!(g.check_bytes(512).is_ok());
        assert!(g.check_bytes(1024).is_ok());
    }

    #[test]
    fn test_bytes_limit_exceeded() {
        let g = QueryGovernor::new(QueryLimits { max_result_bytes: 1024, ..QueryLimits::unlimited() });
        let err = g.check_bytes(1025).unwrap_err();
        assert!(err.is_user_error(), "bytes limit should be user error");
        assert!(err.to_string().contains("size limit"));
    }

    #[test]
    fn test_timeout_ok() {
        let g = QueryGovernor::new(QueryLimits { max_execution_time_ms: 10_000, ..QueryLimits::unlimited() });
        assert!(g.check_timeout().is_ok());
    }

    #[test]
    fn test_timeout_exceeded() {
        let g = QueryGovernor::new(QueryLimits { max_execution_time_ms: 1, ..QueryLimits::unlimited() });
        thread::sleep(Duration::from_millis(5));
        let err = g.check_timeout().unwrap_err();
        assert!(err.is_transient(), "timeout should be Transient");
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn test_memory_limit_ok() {
        let g = QueryGovernor::new(QueryLimits { max_memory_bytes: 1024, ..QueryLimits::unlimited() });
        assert!(g.record_memory(512).is_ok());
        assert_eq!(g.memory_used_bytes(), 512);
    }

    #[test]
    fn test_memory_limit_exceeded() {
        let g = QueryGovernor::new(QueryLimits { max_memory_bytes: 1024, ..QueryLimits::unlimited() });
        g.record_memory(512).unwrap();
        let err = g.record_memory(600).unwrap_err();
        assert!(err.is_transient(), "memory limit should be Transient");
        assert!(err.to_string().contains("memory limit"));
        // Memory should not have been incremented
        assert_eq!(g.memory_used_bytes(), 512);
    }

    #[test]
    fn test_memory_release() {
        let g = QueryGovernor::new(QueryLimits { max_memory_bytes: 1024, ..QueryLimits::unlimited() });
        g.record_memory(512).unwrap();
        g.release_memory(256);
        assert_eq!(g.memory_used_bytes(), 256);
    }

    #[test]
    fn test_unlimited_no_errors() {
        let g = QueryGovernor::unlimited();
        assert!(g.check_rows(u64::MAX).is_ok());
        assert!(g.check_bytes(u64::MAX).is_ok());
        assert!(g.check_timeout().is_ok());
        assert!(g.record_memory(u64::MAX).is_ok());
    }

    #[test]
    fn test_check_all_ok() {
        let g = QueryGovernor::new(QueryLimits {
            max_rows: 1000,
            max_result_bytes: 1024 * 1024,
            max_execution_time_ms: 10_000,
            max_memory_bytes: 0,
        });
        assert!(g.check_all(500, 512 * 1024).is_ok());
    }

    #[test]
    fn test_check_all_rows_fail() {
        let g = QueryGovernor::new(QueryLimits {
            max_rows: 100,
            max_result_bytes: 0,
            max_execution_time_ms: 0,
            max_memory_bytes: 0,
        });
        let err = g.check_all(200, 0).unwrap_err();
        assert!(err.is_user_error());
    }

    #[test]
    fn test_snapshot() {
        let g = QueryGovernor::new(QueryLimits::default());
        g.check_rows(42).unwrap();
        g.check_bytes(1024).unwrap();
        let snap = g.snapshot();
        assert_eq!(snap.rows_produced, 42);
        assert_eq!(snap.bytes_produced, 1024);
        assert!(snap.elapsed_ms < 1000);
    }

    #[test]
    fn test_elapsed_ms_increases() {
        let g = QueryGovernor::new(QueryLimits::unlimited());
        let t0 = g.elapsed_ms();
        thread::sleep(Duration::from_millis(5));
        let t1 = g.elapsed_ms();
        assert!(t1 >= t0);
    }

    #[test]
    fn test_default_limits_reasonable() {
        let limits = QueryLimits::default();
        assert!(limits.max_rows > 0);
        assert!(limits.max_result_bytes > 0);
        assert!(limits.max_execution_time_ms > 0);
        assert!(limits.max_memory_bytes > 0);
    }

    #[test]
    fn test_strict_limits_smaller_than_default() {
        let default = QueryLimits::default();
        let strict = QueryLimits::strict();
        assert!(strict.max_rows <= default.max_rows);
        assert!(strict.max_result_bytes <= default.max_result_bytes);
        assert!(strict.max_execution_time_ms <= default.max_execution_time_ms);
    }
}
