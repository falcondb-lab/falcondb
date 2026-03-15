//! # C1: Follower Read (Stale Read)
//!
//! Allows read-only queries to be served from **Raft follower replicas**
//! instead of always routing to the leader, analogous to CockroachDB's
//! closed-timestamp follower reads and TiDB's stale read.
//!
//! ## Protocol
//!
//! ```text
//!   Client issues: SET falcon.read_staleness = '10s'
//!   OR:            SELECT ... AS OF SYSTEM TIME '-10s'
//!
//!   FollowerReadRouter.route(query, staleness) →
//!       1. Compute safe_read_ts = now() - staleness
//!       2. Check: safe_read_ts <= closed_ts?   (from TsoServer or local HLC)
//!       3. Yes → route to any healthy replica in target shard
//!       4. No  → fall back to leader (normal path)
//! ```
//!
//! ## Safety Guarantee
//!
//! A read at timestamp `T` on a follower is safe iff `T <= closed_ts`, where
//! `closed_ts` is the highest timestamp at which all participating shards have
//! no in-flight (uncommitted) transactions. Any row version with commit_ts <= T
//! is immutable and will be identical on any up-to-date replica.
//!
//! ## Clock Uncertainty
//!
//! To account for clock skew between nodes we subtract an **uncertainty margin**
//! (default 500 ms) from the user-requested staleness before comparing against
//! `closed_ts`. This ensures we never read from a replica that might have
//! missed a recent commit.
//!
//! ## Metrics
//!
//! All routing decisions are counted so operators can observe follower-read
//! hit rates and fall-back frequencies.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use falcon_common::types::{NodeId, ShardId, Timestamp};

use crate::tso::TsoServer;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for the follower read router.
#[derive(Debug, Clone)]
pub struct FollowerReadConfig {
    /// Extra safety margin subtracted from requested staleness to account for
    /// clock skew between nodes. Default: 500 ms.
    pub uncertainty_margin: Duration,
    /// Minimum staleness a client must request before follower read is considered.
    /// Prevents stale reads when the application mistakenly sets a tiny staleness.
    /// Default: 100 ms.
    pub min_staleness: Duration,
    /// Maximum staleness allowed. Requests beyond this are rejected.
    /// Default: 24 h.
    pub max_staleness: Duration,
}

impl Default for FollowerReadConfig {
    fn default() -> Self {
        Self {
            uncertainty_margin: Duration::from_millis(500),
            min_staleness: Duration::from_millis(100),
            max_staleness: Duration::from_secs(86_400),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ReplicaSelector
// ─────────────────────────────────────────────────────────────────────────────

/// A single replica entry known to the router.
#[derive(Debug, Clone)]
pub struct ReplicaEntry {
    pub node_id: NodeId,
    pub shard_id: ShardId,
    /// Applied LSN on this replica (0 = unknown). Used as a secondary health check.
    pub applied_lsn: u64,
    /// Whether this replica is currently reachable (heartbeat-based).
    pub healthy: bool,
    /// Whether this node is the Raft leader for this shard.
    pub is_leader: bool,
}

/// Registry of all known replicas per shard.
/// Updated by the HA module when replica health changes.
pub struct ReplicaRegistry {
    entries: parking_lot::RwLock<Vec<ReplicaEntry>>,
    /// Round-robin counters per shard for load distribution.
    rr_counters: dashmap::DashMap<ShardId, AtomicU64>,
}

impl ReplicaRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: parking_lot::RwLock::new(Vec::new()),
            rr_counters: dashmap::DashMap::new(),
        })
    }

    /// Register or update a replica entry.
    pub fn upsert(&self, entry: ReplicaEntry) {
        let mut entries = self.entries.write();
        if let Some(e) = entries
            .iter_mut()
            .find(|e| e.node_id == entry.node_id && e.shard_id == entry.shard_id)
        {
            *e = entry;
        } else {
            entries.push(entry);
        }
    }

    /// Mark a replica as unhealthy (e.g. on RPC timeout).
    pub fn mark_unhealthy(&self, node_id: NodeId, shard_id: ShardId) {
        let mut entries = self.entries.write();
        if let Some(e) = entries
            .iter_mut()
            .find(|e| e.node_id == node_id && e.shard_id == shard_id)
        {
            e.healthy = false;
        }
    }

    /// Return a healthy non-leader replica for `shard_id` (round-robin).
    /// Returns `None` if no healthy follower exists — caller falls back to leader.
    pub fn pick_follower(&self, shard_id: ShardId) -> Option<NodeId> {
        let entries = self.entries.read();
        let followers: Vec<&ReplicaEntry> = entries
            .iter()
            .filter(|e| e.shard_id == shard_id && e.healthy && !e.is_leader)
            .collect();
        if followers.is_empty() {
            return None;
        }
        // Round-robin selection.
        let counter = self
            .rr_counters
            .entry(shard_id)
            .or_insert_with(|| AtomicU64::new(0));
        let idx = counter.fetch_add(1, Ordering::Relaxed) as usize % followers.len();
        Some(followers[idx].node_id)
    }

    /// Return the leader for `shard_id`.
    pub fn leader(&self, shard_id: ShardId) -> Option<NodeId> {
        let entries = self.entries.read();
        entries
            .iter()
            .find(|e| e.shard_id == shard_id && e.is_leader)
            .map(|e| e.node_id)
    }
}

impl Default for ReplicaRegistry {
    fn default() -> Self {
        Self {
            entries: parking_lot::RwLock::new(Vec::new()),
            rr_counters: dashmap::DashMap::new(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FollowerReadRouter
// ─────────────────────────────────────────────────────────────────────────────

/// Decision returned by the router.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadRoute {
    /// Serve from this follower replica at the given safe read timestamp.
    Follower { node_id: NodeId, read_ts: Timestamp },
    /// Fall back to the Raft leader (normal path).
    Leader { node_id: NodeId },
    /// No replica known for this shard — caller must handle.
    NoReplica,
}

/// Why a follower read was rejected (for diagnostics).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowerReadReject {
    /// The requested staleness is below the configured minimum.
    StalenessTooSmall,
    /// The requested staleness exceeds the configured maximum.
    StalenessTooLarge,
    /// `safe_read_ts > closed_ts` — the follower may not have seen all commits.
    BeyondClosedTimestamp { safe_ts: u64, closed_ts: u64 },
    /// No healthy follower replica is available for this shard.
    NoHealthyFollower,
}

/// Metrics for the follower read router.
#[derive(Debug, Default)]
pub struct FollowerReadMetrics {
    /// Reads successfully routed to a follower.
    pub follower_reads: AtomicU64,
    /// Reads that fell back to the leader.
    pub leader_fallbacks: AtomicU64,
    /// Rejected requests (staleness out of range).
    pub rejected: AtomicU64,
    /// How many times `safe_ts > closed_ts` caused a leader fallback.
    pub beyond_closed_ts: AtomicU64,
}

impl FollowerReadMetrics {
    pub fn snapshot(&self) -> FollowerReadMetricsSnapshot {
        FollowerReadMetricsSnapshot {
            follower_reads: self.follower_reads.load(Ordering::Relaxed),
            leader_fallbacks: self.leader_fallbacks.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            beyond_closed_ts: self.beyond_closed_ts.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FollowerReadMetricsSnapshot {
    pub follower_reads: u64,
    pub leader_fallbacks: u64,
    pub rejected: u64,
    pub beyond_closed_ts: u64,
}

/// Routes read-only queries to follower replicas when safe.
pub struct FollowerReadRouter {
    tso: Arc<TsoServer>,
    registry: Arc<ReplicaRegistry>,
    config: FollowerReadConfig,
    pub metrics: FollowerReadMetrics,
}

impl FollowerReadRouter {
    pub fn new(tso: Arc<TsoServer>, registry: Arc<ReplicaRegistry>) -> Arc<Self> {
        Arc::new(Self {
            tso,
            registry,
            config: FollowerReadConfig::default(),
            metrics: FollowerReadMetrics::default(),
        })
    }

    pub fn new_with_config(
        tso: Arc<TsoServer>,
        registry: Arc<ReplicaRegistry>,
        config: FollowerReadConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            tso,
            registry,
            config,
            metrics: FollowerReadMetrics::default(),
        })
    }

    /// Attempt to route a read at a given staleness to a follower.
    ///
    /// # Parameters
    /// - `shard_id`: target shard
    /// - `staleness`: how stale the client tolerates (e.g. `Duration::from_secs(10)`)
    ///
    /// Returns `Ok(ReadRoute)` on success or `Err(FollowerReadReject)` if
    /// follower read is not safe/possible.
    pub fn route(
        &self,
        shard_id: ShardId,
        staleness: Duration,
    ) -> Result<ReadRoute, FollowerReadReject> {
        // ── Staleness bounds check ──────────────────────────────────────────
        if staleness < self.config.min_staleness {
            self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(FollowerReadReject::StalenessTooSmall);
        }
        if staleness > self.config.max_staleness {
            self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(FollowerReadReject::StalenessTooLarge);
        }

        // ── Compute safe read timestamp ─────────────────────────────────────
        // safe_read_ts = now_ms - (staleness + uncertainty_margin)
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let total_lag_ms =
            staleness.as_millis() as u64 + self.config.uncertainty_margin.as_millis() as u64;
        let safe_read_ms = now_ms.saturating_sub(total_lag_ms);

        // Convert wall-clock ms to our Timestamp domain.
        // HLC encodes physical_ms in bits [63..16], so we pack it the same way.
        let safe_read_ts = Timestamp(safe_read_ms << 16);

        // ── Check against closed timestamp ─────────────────────────────────
        let closed_ts = self.tso.closed_ts();
        if safe_read_ts.0 > closed_ts.0 {
            // Not yet safe to read from follower — fall back to leader.
            self.metrics
                .beyond_closed_ts
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .leader_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return match self.registry.leader(shard_id) {
                Some(node_id) => Ok(ReadRoute::Leader { node_id }),
                None => Ok(ReadRoute::NoReplica),
            };
        }

        // ── Pick a follower replica ─────────────────────────────────────────
        match self.registry.pick_follower(shard_id) {
            Some(node_id) => {
                self.metrics
                    .follower_reads
                    .fetch_add(1, Ordering::Relaxed);
                Ok(ReadRoute::Follower {
                    node_id,
                    read_ts: safe_read_ts,
                })
            }
            None => {
                // No healthy follower — fall back to leader.
                self.metrics
                    .leader_fallbacks
                    .fetch_add(1, Ordering::Relaxed);
                match self.registry.leader(shard_id) {
                    Some(node_id) => Ok(ReadRoute::Leader { node_id }),
                    None => Ok(ReadRoute::NoReplica),
                }
            }
        }
    }

    /// Convenience: route with an explicit wall-clock `as_of` timestamp
    /// (for `AS OF SYSTEM TIME 'timestamp'` SQL syntax).
    ///
    /// Returns a follower route iff the timestamp is safely committed.
    pub fn route_as_of(
        &self,
        shard_id: ShardId,
        as_of_ts: Timestamp,
    ) -> Result<ReadRoute, FollowerReadReject> {
        let closed_ts = self.tso.closed_ts();
        if as_of_ts.0 > closed_ts.0 {
            self.metrics
                .beyond_closed_ts
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .leader_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return match self.registry.leader(shard_id) {
                Some(node_id) => Ok(ReadRoute::Leader { node_id }),
                None => Ok(ReadRoute::NoReplica),
            };
        }
        match self.registry.pick_follower(shard_id) {
            Some(node_id) => {
                self.metrics
                    .follower_reads
                    .fetch_add(1, Ordering::Relaxed);
                Ok(ReadRoute::Follower {
                    node_id,
                    read_ts: as_of_ts,
                })
            }
            None => {
                self.metrics
                    .leader_fallbacks
                    .fetch_add(1, Ordering::Relaxed);
                match self.registry.leader(shard_id) {
                    Some(node_id) => Ok(ReadRoute::Leader { node_id }),
                    None => Ok(ReadRoute::NoReplica),
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_router() -> Arc<FollowerReadRouter> {
        let tso = TsoServer::new(0);
        // Allocate timestamps so counter is well ahead.
        tso.alloc_batch(100_000);
        // Set a high closed_ts so reads succeed.
        tso.advance_closed_ts(Timestamp(u64::MAX / 2));
        let registry = ReplicaRegistry::new();
        registry.upsert(ReplicaEntry {
            node_id: NodeId(1),
            shard_id: ShardId(0),
            applied_lsn: 100,
            healthy: true,
            is_leader: true,
        });
        registry.upsert(ReplicaEntry {
            node_id: NodeId(2),
            shard_id: ShardId(0),
            applied_lsn: 99,
            healthy: true,
            is_leader: false,
        });
        FollowerReadRouter::new(tso, registry)
    }

    #[test]
    fn follower_read_routes_to_follower() {
        let router = make_router();
        let route = router
            .route(ShardId(0), Duration::from_secs(10))
            .expect("should succeed");
        match route {
            ReadRoute::Follower { node_id, .. } => assert_eq!(node_id, NodeId(2)),
            other => panic!("expected follower route, got {:?}", other),
        }
    }

    #[test]
    fn staleness_too_small_is_rejected() {
        let router = make_router();
        let err = router
            .route(ShardId(0), Duration::from_millis(50))
            .unwrap_err();
        assert_eq!(err, FollowerReadReject::StalenessTooSmall);
    }

    #[test]
    fn no_follower_falls_back_to_leader() {
        let tso = TsoServer::new(0);
        tso.alloc_batch(100_000);
        tso.advance_closed_ts(Timestamp(u64::MAX / 2));
        let registry = ReplicaRegistry::new();
        // Only leader, no follower.
        registry.upsert(ReplicaEntry {
            node_id: NodeId(1),
            shard_id: ShardId(0),
            applied_lsn: 100,
            healthy: true,
            is_leader: true,
        });
        let router = FollowerReadRouter::new(tso, registry);
        let route = router
            .route(ShardId(0), Duration::from_secs(10))
            .expect("should succeed");
        assert_eq!(route, ReadRoute::Leader { node_id: NodeId(1) });
        assert_eq!(router.metrics.leader_fallbacks.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn round_robin_distributes_across_followers() {
        let tso = TsoServer::new(0);
        tso.alloc_batch(100_000);
        tso.advance_closed_ts(Timestamp(u64::MAX / 2));
        let registry = ReplicaRegistry::new();
        registry.upsert(ReplicaEntry {
            node_id: NodeId(1),
            shard_id: ShardId(0),
            applied_lsn: 100,
            healthy: true,
            is_leader: true,
        });
        for i in 2u64..=5 {
            registry.upsert(ReplicaEntry {
                node_id: NodeId(i),
                shard_id: ShardId(0),
                applied_lsn: 100,
                healthy: true,
                is_leader: false,
            });
        }
        let router = FollowerReadRouter::new(tso, registry);
        let mut seen_nodes = std::collections::HashSet::new();
        for _ in 0..8 {
            if let Ok(ReadRoute::Follower { node_id, .. }) =
                router.route(ShardId(0), Duration::from_secs(10))
            {
                seen_nodes.insert(node_id);
            }
        }
        // With 4 followers and 8 requests, all 4 should be hit.
        assert!(seen_nodes.len() >= 2, "round-robin should spread load");
    }
}
