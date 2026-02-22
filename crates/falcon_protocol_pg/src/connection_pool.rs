//! Server-side connection pool for QueryHandler instances.
//!
//! Instead of creating a new QueryHandler per TCP connection, the pool
//! pre-allocates a bounded number of handlers that share the same plan cache
//! and slow query log. When a connection arrives, it checks out a handler;
//! when it disconnects, the handler is returned to the pool for reuse.
//!
//! Benefits:
//! - Bounded concurrency: at most `pool_size` handlers active simultaneously.
//! - Shared plan cache and slow query log across all pooled connections.
//! - Observable pool statistics (checkouts, returns, waits, timeouts).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use falcon_cluster::DistributedQueryEngine;
use falcon_common::types::ShardId;
use falcon_executor::Executor;
use falcon_storage::engine::StorageEngine;
use falcon_txn::TxnManager;

use crate::handler::QueryHandler;
use crate::plan_cache::PlanCache;
use crate::slow_query_log::SlowQueryLog;

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of concurrent handler checkouts (0 = unlimited).
    pub max_size: usize,
    /// Timeout for waiting to acquire a handler from the pool (milliseconds).
    /// 0 = no timeout (wait indefinitely).
    pub acquire_timeout_ms: u64,
    /// Plan cache capacity shared across all pooled handlers.
    pub plan_cache_capacity: usize,
    /// Slow query threshold in milliseconds (0 = disabled).
    pub slow_query_threshold_ms: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 100,
            acquire_timeout_ms: 30_000, // 30 seconds
            plan_cache_capacity: 256,
            slow_query_threshold_ms: 0,
        }
    }
}

/// Observable pool statistics.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total number of successful handler checkouts.
    pub total_checkouts: u64,
    /// Total number of handler returns.
    pub total_returns: u64,
    /// Number of checkout attempts that had to wait for a slot.
    pub total_waits: u64,
    /// Number of checkout attempts that timed out.
    pub total_timeouts: u64,
    /// Current number of checked-out handlers.
    pub active: u64,
    /// Pool capacity.
    pub max_size: usize,
}

/// Atomic counters for pool statistics.
struct PoolStatsInner {
    total_checkouts: AtomicU64,
    total_returns: AtomicU64,
    total_waits: AtomicU64,
    total_timeouts: AtomicU64,
}

impl PoolStatsInner {
    fn new() -> Self {
        Self {
            total_checkouts: AtomicU64::new(0),
            total_returns: AtomicU64::new(0),
            total_waits: AtomicU64::new(0),
            total_timeouts: AtomicU64::new(0),
        }
    }

    fn snapshot(&self, _semaphore: &Semaphore, max_size: usize) -> PoolStats {
        let checkouts = self.total_checkouts.load(Ordering::Relaxed);
        let returns = self.total_returns.load(Ordering::Relaxed);
        PoolStats {
            total_checkouts: checkouts,
            total_returns: returns,
            total_waits: self.total_waits.load(Ordering::Relaxed),
            total_timeouts: self.total_timeouts.load(Ordering::Relaxed),
            active: checkouts.saturating_sub(returns),
            max_size,
        }
    }
}

/// A checked-out handler guard. Returns the permit to the pool on drop.
pub struct PooledHandler {
    handler: QueryHandler,
    _permit: tokio::sync::OwnedSemaphorePermit,
    stats: Arc<PoolStatsInner>,
}

impl PooledHandler {
    /// Access the handler.
    pub fn handler(&self) -> &QueryHandler {
        &self.handler
    }

    /// Clone the handler for use in spawn_blocking etc.
    pub fn handler_clone(&self) -> QueryHandler {
        self.handler.clone()
    }
}

impl Drop for PooledHandler {
    fn drop(&mut self) {
        self.stats.total_returns.fetch_add(1, Ordering::Relaxed);
    }
}

impl std::ops::Deref for PooledHandler {
    type Target = QueryHandler;
    fn deref(&self) -> &Self::Target {
        &self.handler
    }
}

/// Server-side connection pool.
pub struct ConnectionPool {
    /// Shared storage engine.
    storage: Arc<StorageEngine>,
    /// Shared transaction manager.
    txn_mgr: Arc<TxnManager>,
    /// Shared executor.
    executor: Arc<Executor>,
    /// Optional distributed engine.
    dist_engine: Option<Arc<DistributedQueryEngine>>,
    /// Shard IDs for distributed mode.
    shard_ids: Vec<ShardId>,
    /// Shared plan cache across all pooled handlers.
    plan_cache: Arc<PlanCache>,
    /// Shared slow query log across all pooled handlers.
    slow_query_log: Arc<SlowQueryLog>,
    /// Semaphore controlling max concurrent checkouts.
    semaphore: Arc<Semaphore>,
    /// Pool configuration.
    config: PoolConfig,
    /// Atomic statistics.
    stats: Arc<PoolStatsInner>,
}

impl ConnectionPool {
    /// Create a new connection pool.
    pub fn new(
        storage: Arc<StorageEngine>,
        txn_mgr: Arc<TxnManager>,
        executor: Arc<Executor>,
        config: PoolConfig,
    ) -> Self {
        let max = if config.max_size == 0 {
            usize::MAX >> 1
        } else {
            config.max_size
        };
        let plan_cache = Arc::new(PlanCache::new(config.plan_cache_capacity));
        let slow_query_log = if config.slow_query_threshold_ms > 0 {
            Arc::new(SlowQueryLog::new(
                Duration::from_millis(config.slow_query_threshold_ms),
                1000,
            ))
        } else {
            Arc::new(SlowQueryLog::disabled())
        };

        Self {
            storage,
            txn_mgr,
            executor,
            dist_engine: None,
            shard_ids: vec![ShardId(0)],
            plan_cache,
            slow_query_log,
            semaphore: Arc::new(Semaphore::new(max)),
            config,
            stats: Arc::new(PoolStatsInner::new()),
        }
    }

    /// Create a new connection pool with distributed engine.
    pub fn new_distributed(
        storage: Arc<StorageEngine>,
        txn_mgr: Arc<TxnManager>,
        executor: Arc<Executor>,
        shard_ids: Vec<ShardId>,
        dist_engine: Arc<DistributedQueryEngine>,
        config: PoolConfig,
    ) -> Self {
        let mut pool = Self::new(storage, txn_mgr, executor, config);
        pool.dist_engine = Some(dist_engine);
        pool.shard_ids = shard_ids;
        pool
    }

    /// Try to check out a handler from the pool.
    /// Returns `None` if the pool is exhausted and the timeout expires.
    pub async fn acquire(&self) -> Option<PooledHandler> {
        let permit = if self.config.acquire_timeout_ms > 0 {
            let timeout = Duration::from_millis(self.config.acquire_timeout_ms);
            let available = self.semaphore.clone().try_acquire_owned().ok();
            match available {
                Some(permit) => permit,
                None => {
                    self.stats.total_waits.fetch_add(1, Ordering::Relaxed);
                    match tokio::time::timeout(timeout, self.semaphore.clone().acquire_owned())
                        .await
                    {
                        Ok(Ok(permit)) => permit,
                        _ => {
                            self.stats.total_timeouts.fetch_add(1, Ordering::Relaxed);
                            return None;
                        }
                    }
                }
            }
        } else {
            match self.semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return None,
            }
        };

        self.stats.total_checkouts.fetch_add(1, Ordering::Relaxed);

        let mut handler = if let Some(ref dist) = self.dist_engine {
            QueryHandler::new_distributed(
                self.storage.clone(),
                self.txn_mgr.clone(),
                self.executor.clone(),
                self.shard_ids.clone(),
                dist.clone(),
            )
        } else {
            QueryHandler::new(
                self.storage.clone(),
                self.txn_mgr.clone(),
                self.executor.clone(),
            )
        };

        // Inject shared plan cache and slow query log
        handler.plan_cache = self.plan_cache.clone();
        handler.slow_query_log = self.slow_query_log.clone();

        Some(PooledHandler {
            handler,
            _permit: permit,
            stats: self.stats.clone(),
        })
    }

    /// Get a snapshot of pool statistics.
    pub fn stats(&self) -> PoolStats {
        self.stats.snapshot(&self.semaphore, self.config.max_size)
    }

    /// Get the shared plan cache (for observability).
    pub fn plan_cache(&self) -> &Arc<PlanCache> {
        &self.plan_cache
    }

    /// Get the shared slow query log (for observability).
    pub fn slow_query_log(&self) -> &Arc<SlowQueryLog> {
        &self.slow_query_log
    }

    /// Current number of available permits (free slots).
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }
}
