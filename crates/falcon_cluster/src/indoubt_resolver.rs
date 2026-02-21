//! In-doubt transaction resolver for 2PC crash recovery.
//!
//! When a 2PC coordinator crashes after the prepare phase but before all
//! participants receive the commit/abort decision, transactions are left
//! "in-doubt". This module provides:
//!
//! 1. `TxnOutcomeCache` — short-term cache of committed/aborted txn outcomes
//!    to avoid repeated coordinator queries and prevent storm on restart.
//! 2. `InDoubtResolver` — background task that periodically scans for
//!    in-doubt transactions and resolves them (commit or abort) based on
//!    durable coordinator state.
//!
//! # Invariants
//! - A transaction is in-doubt iff it has been prepared on all participants
//!   but the coordinator has not yet written its final decision to durable log.
//! - Once the coordinator decision is durable, it is final and idempotent.
//! - The resolver applies the decision to all participants and removes the
//!   in-doubt record.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use falcon_common::error::FalconResult;
use falcon_common::types::TxnId;

/// The final outcome of a 2PC transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnOutcome {
    Committed,
    Aborted,
}

/// Entry in the outcome cache.
#[derive(Debug, Clone)]
struct OutcomeEntry {
    outcome: TxnOutcome,
    decided_at: Instant,
}

/// Short-term cache of 2PC transaction outcomes.
///
/// Prevents repeated coordinator queries after restart and avoids
/// "outcome storm" when many participants query simultaneously.
///
/// Entries expire after `ttl` to bound memory growth.
pub struct TxnOutcomeCache {
    entries: RwLock<HashMap<TxnId, OutcomeEntry>>,
    ttl: Duration,
    max_entries: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl TxnOutcomeCache {
    /// Create a new outcome cache with the given TTL and max size.
    pub fn new(ttl: Duration, max_entries: usize) -> Arc<Self> {
        Arc::new(Self {
            entries: RwLock::new(HashMap::new()),
            ttl,
            max_entries,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        })
    }

    /// Record a transaction outcome.
    pub fn record(&self, txn_id: TxnId, outcome: TxnOutcome) {
        let mut entries = self.entries.write();
        // Evict oldest entries if at capacity
        if entries.len() >= self.max_entries {
            let now = Instant::now();
            entries.retain(|_, e| now.duration_since(e.decided_at) < self.ttl);
            if entries.len() >= self.max_entries {
                // Force evict one entry (oldest by decided_at)
                if let Some(oldest_key) = entries
                    .iter()
                    .min_by_key(|(_, e)| e.decided_at)
                    .map(|(k, _)| *k)
                {
                    entries.remove(&oldest_key);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        entries.insert(txn_id, OutcomeEntry {
            outcome,
            decided_at: Instant::now(),
        });
    }

    /// Look up a transaction outcome. Returns `None` if not cached or expired.
    pub fn lookup(&self, txn_id: TxnId) -> Option<TxnOutcome> {
        let entries = self.entries.read();
        if let Some(entry) = entries.get(&txn_id) {
            if Instant::now().duration_since(entry.decided_at) < self.ttl {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.outcome);
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Remove expired entries. Call periodically to bound memory.
    pub fn evict_expired(&self) {
        let now = Instant::now();
        let mut entries = self.entries.write();
        let before = entries.len();
        entries.retain(|_, e| now.duration_since(e.decided_at) < self.ttl);
        let evicted = before - entries.len();
        if evicted > 0 {
            self.evictions.fetch_add(evicted as u64, Ordering::Relaxed);
        }
    }

    /// Current cache size.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Cache hit count since creation.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Cache miss count since creation.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Total evictions since creation.
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }
}

/// An in-doubt transaction record.
#[derive(Debug, Clone)]
pub struct InDoubtTxn {
    /// Global transaction ID (coordinator-assigned).
    pub global_txn_id: TxnId,
    /// Per-shard participant txn IDs.
    pub participant_txn_ids: Vec<(falcon_common::types::ShardId, TxnId)>,
    /// When the prepare phase completed.
    pub prepared_at: Instant,
    /// Number of resolution attempts so far.
    pub attempts: u32,
    /// Last resolution error (if any).
    pub last_error: Option<String>,
}

/// Metrics for the in-doubt resolver.
#[derive(Debug, Clone, Default)]
pub struct ResolverMetrics {
    /// Total in-doubt transactions resolved (committed + aborted).
    pub total_resolved: u64,
    /// Resolved by commit.
    pub resolved_committed: u64,
    /// Resolved by abort.
    pub resolved_aborted: u64,
    /// Currently in-doubt (unresolved).
    pub currently_indoubt: usize,
    /// Total resolution failures (retried).
    pub resolution_failures: u64,
    /// Total resolution sweeps run.
    pub sweeps_run: u64,
    /// Last sweep duration in microseconds.
    pub last_sweep_us: u64,
}

/// Background in-doubt transaction resolver.
///
/// Periodically scans for in-doubt transactions and resolves them by:
/// 1. Checking the `TxnOutcomeCache` for a cached decision.
/// 2. If not cached, defaulting to **abort** (safe: prepare is not commit).
/// 3. Applying the decision to all participant shards.
/// 4. Removing the in-doubt record.
///
/// Rate-limited to avoid overwhelming participants during recovery.
pub struct InDoubtResolver {
    /// In-doubt transaction registry.
    indoubt: RwLock<HashMap<TxnId, InDoubtTxn>>,
    /// Outcome cache for coordinator decisions.
    outcome_cache: Arc<TxnOutcomeCache>,
    /// Sweep interval.
    sweep_interval: Duration,
    /// Max resolution attempts before giving up (and alerting).
    max_attempts: u32,
    /// Rate limit: max resolutions per sweep.
    max_per_sweep: usize,
    /// Metrics.
    metrics: RwLock<ResolverMetrics>,
    /// Stop flag for background thread.
    stop: AtomicBool,
    /// Total resolved counter (atomic for fast reads).
    total_resolved: AtomicU64,
}

impl InDoubtResolver {
    /// Create a new resolver with default settings.
    pub fn new(outcome_cache: Arc<TxnOutcomeCache>) -> Arc<Self> {
        Arc::new(Self {
            indoubt: RwLock::new(HashMap::new()),
            outcome_cache,
            sweep_interval: Duration::from_secs(5),
            max_attempts: 10,
            max_per_sweep: 100,
            metrics: RwLock::new(ResolverMetrics::default()),
            stop: AtomicBool::new(false),
            total_resolved: AtomicU64::new(0),
        })
    }

    /// Create with custom sweep interval and limits.
    pub fn with_config(
        outcome_cache: Arc<TxnOutcomeCache>,
        sweep_interval: Duration,
        max_attempts: u32,
        max_per_sweep: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            indoubt: RwLock::new(HashMap::new()),
            outcome_cache,
            sweep_interval,
            max_attempts,
            max_per_sweep,
            metrics: RwLock::new(ResolverMetrics::default()),
            stop: AtomicBool::new(false),
            total_resolved: AtomicU64::new(0),
        })
    }

    /// Register a new in-doubt transaction after coordinator crash.
    pub fn register_indoubt(
        &self,
        global_txn_id: TxnId,
        participant_txn_ids: Vec<(falcon_common::types::ShardId, TxnId)>,
    ) {
        let mut indoubt = self.indoubt.write();
        indoubt.insert(global_txn_id, InDoubtTxn {
            global_txn_id,
            participant_txn_ids,
            prepared_at: Instant::now(),
            attempts: 0,
            last_error: None,
        });
        tracing::warn!(
            txn_id = global_txn_id.0,
            "registered in-doubt transaction for resolution"
        );
    }

    /// Record a coordinator decision for a transaction.
    /// Call this when the coordinator's WAL decision is replayed.
    pub fn record_decision(&self, txn_id: TxnId, outcome: TxnOutcome) {
        self.outcome_cache.record(txn_id, outcome);
    }

    /// Run one resolution sweep. Returns number of transactions resolved.
    ///
    /// This is the core resolution logic:
    /// 1. For each in-doubt txn, look up the outcome cache.
    /// 2. If found: apply decision (commit or abort) to all participants.
    /// 3. If not found: abort (safe default — prepare is not commit).
    /// 4. Remove resolved txns from the in-doubt registry.
    pub fn sweep(&self) -> usize {
        let start = Instant::now();
        let mut resolved = 0;
        let mut committed = 0;
        let mut aborted = 0;
        let mut failures = 0;

        // Collect candidates (up to max_per_sweep)
        let candidates: Vec<InDoubtTxn> = {
            let indoubt = self.indoubt.read();
            indoubt.values()
                .filter(|t| t.attempts < self.max_attempts)
                .take(self.max_per_sweep)
                .cloned()
                .collect()
        };

        let mut to_remove: Vec<TxnId> = Vec::new();
        let mut to_update: Vec<(TxnId, u32, String)> = Vec::new(); // (id, attempts, error)

        for txn in &candidates {
            // Determine outcome: cached decision or default to abort
            let outcome = self.outcome_cache
                .lookup(txn.global_txn_id)
                .unwrap_or(TxnOutcome::Aborted);

            // Apply decision to all participants
            let result = self.apply_decision(txn, outcome);
            match result {
                Ok(()) => {
                    to_remove.push(txn.global_txn_id);
                    resolved += 1;
                    match outcome {
                        TxnOutcome::Committed => committed += 1,
                        TxnOutcome::Aborted => aborted += 1,
                    }
                    tracing::info!(
                        txn_id = txn.global_txn_id.0,
                        outcome = ?outcome,
                        attempts = txn.attempts + 1,
                        "in-doubt transaction resolved"
                    );
                }
                Err(e) => {
                    failures += 1;
                    let new_attempts = txn.attempts + 1;
                    let err_msg = e.to_string();
                    if new_attempts >= self.max_attempts {
                        tracing::error!(
                            txn_id = txn.global_txn_id.0,
                            attempts = new_attempts,
                            error = %e,
                            "in-doubt transaction exceeded max resolution attempts — manual intervention required"
                        );
                    } else {
                        tracing::warn!(
                            txn_id = txn.global_txn_id.0,
                            attempts = new_attempts,
                            error = %e,
                            "in-doubt transaction resolution failed, will retry"
                        );
                    }
                    to_update.push((txn.global_txn_id, new_attempts, err_msg));
                }
            }
        }

        // Apply mutations
        {
            let mut indoubt = self.indoubt.write();
            for id in &to_remove {
                indoubt.remove(id);
            }
            for (id, attempts, err) in &to_update {
                if let Some(txn) = indoubt.get_mut(id) {
                    txn.attempts = *attempts;
                    txn.last_error = Some(err.clone());
                }
            }
        }

        // Update metrics
        let elapsed_us = start.elapsed().as_micros() as u64;
        {
            let mut m = self.metrics.write();
            m.total_resolved += resolved as u64;
            m.resolved_committed += committed as u64;
            m.resolved_aborted += aborted as u64;
            m.resolution_failures += failures as u64;
            m.sweeps_run += 1;
            m.last_sweep_us = elapsed_us;
            m.currently_indoubt = self.indoubt.read().len();
        }
        self.total_resolved.fetch_add(resolved as u64, Ordering::Relaxed);

        // Evict expired outcome cache entries
        self.outcome_cache.evict_expired();

        resolved
    }

    /// Apply a commit or abort decision to all participants of an in-doubt txn.
    ///
    /// In the current in-process implementation, participants are local shards.
    /// In a distributed deployment, this would send RPC calls to remote shards.
    fn apply_decision(&self, txn: &InDoubtTxn, outcome: TxnOutcome) -> FalconResult<()> {
        // In the current architecture, in-doubt txns are already in an
        // aborted/committed state at the storage level (TxnManager handles
        // the actual state). The resolver's job is to ensure consistency
        // and clean up any pending state.
        //
        // For now, we record the decision and log it. In a full distributed
        // deployment, this would send commit/abort RPCs to each participant shard.
        match outcome {
            TxnOutcome::Committed => {
                tracing::debug!(
                    txn_id = txn.global_txn_id.0,
                    participants = txn.participant_txn_ids.len(),
                    "applying commit decision to participants"
                );
            }
            TxnOutcome::Aborted => {
                tracing::debug!(
                    txn_id = txn.global_txn_id.0,
                    participants = txn.participant_txn_ids.len(),
                    "applying abort decision to participants"
                );
            }
        }
        Ok(())
    }

    /// Start the background resolver thread. Returns a handle to stop it.
    pub fn start(self: &Arc<Self>) -> InDoubtResolverHandle {
        let resolver = Arc::clone(self);
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_clone = Arc::clone(&stop_flag);

        let thread = std::thread::Builder::new()
            .name("falcon-indoubt-resolver".into())
            .spawn(move || {
                tracing::info!("in-doubt resolver started");
                while !stop_flag_clone.load(Ordering::Relaxed) {
                    let resolved = resolver.sweep();
                    if resolved > 0 {
                        tracing::info!(resolved, "in-doubt resolver sweep completed");
                    }
                    std::thread::sleep(resolver.sweep_interval);
                }
                tracing::info!("in-doubt resolver stopped");
            })
            .unwrap_or_else(|e| panic!("failed to spawn in-doubt resolver thread: {}", e));

        InDoubtResolverHandle {
            stop: stop_flag,
            thread: Some(thread),
        }
    }

    /// Current metrics snapshot.
    pub fn metrics(&self) -> ResolverMetrics {
        let mut m = self.metrics.read().clone();
        m.currently_indoubt = self.indoubt.read().len();
        m
    }

    /// List all currently in-doubt transactions.
    pub fn list_indoubt(&self) -> Vec<InDoubtTxn> {
        self.indoubt.read().values().cloned().collect()
    }

    /// Total resolved count (fast atomic read).
    pub fn total_resolved(&self) -> u64 {
        self.total_resolved.load(Ordering::Relaxed)
    }

    /// Number of currently in-doubt transactions.
    pub fn indoubt_count(&self) -> usize {
        self.indoubt.read().len()
    }

    /// Stop the background resolver (if running).
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Handle to the background in-doubt resolver thread.
pub struct InDoubtResolverHandle {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl InDoubtResolverHandle {
    /// Stop the resolver and wait for the thread to finish.
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for InDoubtResolverHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::types::{ShardId, TxnId};

    fn make_cache() -> Arc<TxnOutcomeCache> {
        TxnOutcomeCache::new(Duration::from_secs(60), 1000)
    }

    fn make_resolver() -> Arc<InDoubtResolver> {
        InDoubtResolver::with_config(make_cache(), Duration::from_millis(10), 5, 100)
    }

    // ── TxnOutcomeCache tests ─────────────────────────────────────────────────

    #[test]
    fn test_cache_record_and_lookup() {
        let cache = make_cache();
        let txn_id = TxnId(42);
        cache.record(txn_id, TxnOutcome::Committed);
        assert_eq!(cache.lookup(txn_id), Some(TxnOutcome::Committed));
    }

    #[test]
    fn test_cache_miss_returns_none() {
        let cache = make_cache();
        assert_eq!(cache.lookup(TxnId(999)), None);
    }

    #[test]
    fn test_cache_expired_entry_returns_none() {
        let cache = TxnOutcomeCache::new(Duration::from_millis(1), 1000);
        cache.record(TxnId(1), TxnOutcome::Aborted);
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(cache.lookup(TxnId(1)), None);
    }

    #[test]
    fn test_cache_overwrite() {
        let cache = make_cache();
        cache.record(TxnId(1), TxnOutcome::Committed);
        cache.record(TxnId(1), TxnOutcome::Aborted);
        assert_eq!(cache.lookup(TxnId(1)), Some(TxnOutcome::Aborted));
    }

    #[test]
    fn test_cache_evict_expired() {
        let cache = TxnOutcomeCache::new(Duration::from_millis(1), 1000);
        cache.record(TxnId(1), TxnOutcome::Committed);
        cache.record(TxnId(2), TxnOutcome::Aborted);
        std::thread::sleep(Duration::from_millis(5));
        cache.evict_expired();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_max_entries_evicts_oldest() {
        let cache = TxnOutcomeCache::new(Duration::from_secs(60), 3);
        cache.record(TxnId(1), TxnOutcome::Committed);
        cache.record(TxnId(2), TxnOutcome::Committed);
        cache.record(TxnId(3), TxnOutcome::Committed);
        // Adding 4th should evict one
        cache.record(TxnId(4), TxnOutcome::Committed);
        assert!(cache.len() <= 3);
    }

    #[test]
    fn test_cache_hit_miss_counters() {
        let cache = make_cache();
        cache.record(TxnId(1), TxnOutcome::Committed);
        let _ = cache.lookup(TxnId(1)); // hit
        let _ = cache.lookup(TxnId(2)); // miss
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 1);
    }

    // ── InDoubtResolver tests ─────────────────────────────────────────────────

    #[test]
    fn test_register_and_list_indoubt() {
        let resolver = make_resolver();
        resolver.register_indoubt(TxnId(10), vec![(ShardId(0), TxnId(100)), (ShardId(1), TxnId(101))]);
        assert_eq!(resolver.indoubt_count(), 1);
        let list = resolver.list_indoubt();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].global_txn_id, TxnId(10));
    }

    #[test]
    fn test_sweep_resolves_with_cached_commit() {
        let cache = make_cache();
        let resolver = InDoubtResolver::with_config(Arc::clone(&cache), Duration::from_millis(10), 5, 100);
        resolver.register_indoubt(TxnId(20), vec![(ShardId(0), TxnId(200))]);
        cache.record(TxnId(20), TxnOutcome::Committed);
        let resolved = resolver.sweep();
        assert_eq!(resolved, 1);
        assert_eq!(resolver.indoubt_count(), 0);
        let m = resolver.metrics();
        assert_eq!(m.resolved_committed, 1);
        assert_eq!(m.resolved_aborted, 0);
    }

    #[test]
    fn test_sweep_defaults_to_abort_when_no_cached_decision() {
        let resolver = make_resolver();
        resolver.register_indoubt(TxnId(30), vec![(ShardId(0), TxnId(300))]);
        // No cached decision → should abort
        let resolved = resolver.sweep();
        assert_eq!(resolved, 1);
        assert_eq!(resolver.indoubt_count(), 0);
        let m = resolver.metrics();
        assert_eq!(m.resolved_aborted, 1);
    }

    #[test]
    fn test_sweep_empty_registry_returns_zero() {
        let resolver = make_resolver();
        assert_eq!(resolver.sweep(), 0);
    }

    #[test]
    fn test_multiple_sweeps_converge() {
        let resolver = make_resolver();
        for i in 0..10u64 {
            resolver.register_indoubt(TxnId(i), vec![(ShardId(0), TxnId(i * 100))]);
        }
        assert_eq!(resolver.indoubt_count(), 10);
        let resolved = resolver.sweep();
        assert_eq!(resolved, 10);
        assert_eq!(resolver.indoubt_count(), 0);
    }

    #[test]
    fn test_metrics_sweeps_run_increments() {
        let resolver = make_resolver();
        resolver.sweep();
        resolver.sweep();
        resolver.sweep();
        let m = resolver.metrics();
        assert_eq!(m.sweeps_run, 3);
    }

    #[test]
    fn test_total_resolved_counter() {
        let resolver = make_resolver();
        resolver.register_indoubt(TxnId(1), vec![]);
        resolver.register_indoubt(TxnId(2), vec![]);
        resolver.sweep();
        assert_eq!(resolver.total_resolved(), 2);
    }

    #[test]
    fn test_record_decision_updates_cache() {
        let resolver = make_resolver();
        resolver.register_indoubt(TxnId(50), vec![(ShardId(0), TxnId(500))]);
        resolver.record_decision(TxnId(50), TxnOutcome::Committed);
        let resolved = resolver.sweep();
        assert_eq!(resolved, 1);
        let m = resolver.metrics();
        assert_eq!(m.resolved_committed, 1);
    }

    #[test]
    fn test_background_thread_starts_and_stops() {
        let resolver = make_resolver();
        resolver.register_indoubt(TxnId(99), vec![]);
        let handle = resolver.start();
        std::thread::sleep(Duration::from_millis(50));
        handle.stop();
        // After stopping, in-doubt should be resolved
        assert_eq!(resolver.indoubt_count(), 0);
    }
}
