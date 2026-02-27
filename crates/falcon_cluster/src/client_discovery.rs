//! P2: Client Discovery & Shard-Aware Routing
//!
//! Provides the client-facing topology discovery protocol:
//! - `TopologySnapshot`: serializable routing table clients can poll
//! - `TopologySubscriptionManager`: push-based change notifications
//! - `ClientConnectionManager`: connection-level failover with exponential backoff
//! - `NotLeaderRedirector`: auto-redirect on NOT_LEADER with retry budget
//! - `ClientRoutingTable`: client-local shard→leader cache with epoch validation

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use falcon_common::types::{NodeId, ShardId};

// ═══════════════════════════════════════════════════════════════════════════
// §1: TopologySnapshot — Serializable Routing Snapshot
// ═══════════════════════════════════════════════════════════════════════════

/// A point-in-time snapshot of the cluster topology that clients can poll.
/// Epoch-versioned so clients can detect stale data.
#[derive(Debug, Clone)]
pub struct TopologySnapshot {
    /// Monotonically increasing epoch — bumped on every topology change.
    pub epoch: u64,
    /// Timestamp when this snapshot was created (millis since UNIX epoch).
    pub timestamp_ms: u64,
    /// Shard routing entries: shard_id → leader node + replicas.
    pub shards: Vec<ShardRouteEntry>,
    /// Node directory: node_id → address + role.
    pub nodes: Vec<NodeDirectoryEntry>,
    /// Gateway endpoints that accept client connections.
    pub gateway_endpoints: Vec<String>,
}

/// Routing entry for a single shard.
#[derive(Debug, Clone)]
pub struct ShardRouteEntry {
    pub shard_id: ShardId,
    pub leader_node: NodeId,
    pub leader_addr: String,
    pub replica_addrs: Vec<String>,
    pub hash_range_start: u64,
    pub hash_range_end: u64,
    /// Epoch at which this shard's leader was last changed.
    pub leader_epoch: u64,
}

/// Directory entry for a single node.
#[derive(Debug, Clone)]
pub struct NodeDirectoryEntry {
    pub node_id: NodeId,
    pub address: String,
    pub role: String,
    pub is_alive: bool,
}

impl TopologySnapshot {
    /// Check if this snapshot is newer than the client's cached epoch.
    pub const fn is_newer_than(&self, client_epoch: u64) -> bool {
        self.epoch > client_epoch
    }

    /// Find the leader address for a given shard.
    pub fn leader_for_shard(&self, shard_id: ShardId) -> Option<&str> {
        self.shards
            .iter()
            .find(|s| s.shard_id == shard_id)
            .map(|s| s.leader_addr.as_str())
    }

    /// Find which shard owns a given hash value.
    pub fn shard_for_hash(&self, hash: u64) -> Option<&ShardRouteEntry> {
        self.shards
            .iter()
            .find(|s| hash >= s.hash_range_start && hash < s.hash_range_end)
    }

    /// Number of shards in this snapshot.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Number of alive nodes.
    pub fn alive_node_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.is_alive).count()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2: TopologySubscriptionManager — Push-Based Change Notifications
// ═══════════════════════════════════════════════════════════════════════════

/// Unique identifier for a topology subscription.
pub type SubscriptionId = u64;

/// A topology change event pushed to subscribers.
#[derive(Debug, Clone)]
pub struct TopologyChangeEvent {
    /// New epoch after the change.
    pub new_epoch: u64,
    /// What changed.
    pub change_type: TopologyChangeType,
    /// Timestamp of the change (millis since UNIX epoch).
    pub timestamp_ms: u64,
    /// Optional: new leader address for shard leader changes.
    pub leader_hint: Option<String>,
}

/// Types of topology changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyChangeType {
    /// A shard's leader changed (failover or rebalance).
    LeaderChange { shard_id: ShardId, new_leader: NodeId },
    /// A node joined the cluster.
    NodeJoined { node_id: NodeId },
    /// A node left the cluster.
    NodeLeft { node_id: NodeId },
    /// A shard was split.
    ShardSplit { original: ShardId, new_shard: ShardId },
    /// A shard was merged.
    ShardMerge { shard_a: ShardId, shard_b: ShardId, result: ShardId },
    /// Full topology refresh (e.g., after prolonged partition).
    FullRefresh,
}

/// Registered subscriber info.
struct Subscriber {
    _id: SubscriptionId,
    /// Client-reported epoch at subscription time.
    _client_epoch: u64,
    /// Pending events not yet delivered.
    pending_events: Vec<TopologyChangeEvent>,
    /// Maximum pending events before oldest are dropped.
    max_pending: usize,
    /// When this subscription was created.
    _created_at: Instant,
    /// When events were last polled.
    last_poll: Instant,
    /// Whether this subscription is still active.
    active: bool,
}

/// Manages topology change subscriptions for connected clients.
pub struct TopologySubscriptionManager {
    next_id: AtomicU64,
    subscribers: RwLock<HashMap<SubscriptionId, Subscriber>>,
    /// Current cluster epoch.
    current_epoch: AtomicU64,
    /// How long an idle subscription lives before eviction.
    idle_timeout: Duration,
    pub metrics: SubscriptionMetrics,
}

/// Metrics for the subscription manager.
#[derive(Debug)]
pub struct SubscriptionMetrics {
    pub active_subscriptions: AtomicU64,
    pub total_subscriptions: AtomicU64,
    pub events_published: AtomicU64,
    pub events_delivered: AtomicU64,
    pub events_dropped: AtomicU64,
    pub evictions: AtomicU64,
}

impl Default for SubscriptionMetrics {
    fn default() -> Self {
        Self {
            active_subscriptions: AtomicU64::new(0),
            total_subscriptions: AtomicU64::new(0),
            events_published: AtomicU64::new(0),
            events_delivered: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }
}

impl TopologySubscriptionManager {
    pub fn new(initial_epoch: u64, idle_timeout: Duration) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            subscribers: RwLock::new(HashMap::new()),
            current_epoch: AtomicU64::new(initial_epoch),
            idle_timeout,
            metrics: SubscriptionMetrics::default(),
        }
    }

    /// Register a new subscription. Returns the subscription ID.
    pub fn subscribe(&self, client_epoch: u64) -> SubscriptionId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let sub = Subscriber {
            _id: id,
            _client_epoch: client_epoch,
            pending_events: Vec::new(),
            max_pending: 100,
            _created_at: now,
            last_poll: now,
            active: true,
        };
        let mut subs = self.subscribers.write().unwrap_or_else(|e| e.into_inner());
        subs.insert(id, sub);
        self.metrics.active_subscriptions.fetch_add(1, Ordering::Relaxed);
        self.metrics.total_subscriptions.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Unsubscribe.
    pub fn unsubscribe(&self, id: SubscriptionId) -> bool {
        let mut subs = self.subscribers.write().unwrap_or_else(|e| e.into_inner());
        if let Some(sub) = subs.get_mut(&id) {
            if sub.active {
                sub.active = false;
                self.metrics.active_subscriptions.fetch_sub(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Publish a topology change event to all active subscribers.
    pub fn publish(&self, event: TopologyChangeEvent) {
        self.current_epoch.store(event.new_epoch, Ordering::Relaxed);
        let mut subs = self.subscribers.write().unwrap_or_else(|e| e.into_inner());
        let mut dropped = 0u64;
        for sub in subs.values_mut() {
            if !sub.active {
                continue;
            }
            if sub.pending_events.len() >= sub.max_pending {
                sub.pending_events.remove(0);
                dropped += 1;
            }
            sub.pending_events.push(event.clone());
        }
        self.metrics.events_published.fetch_add(1, Ordering::Relaxed);
        if dropped > 0 {
            self.metrics.events_dropped.fetch_add(dropped, Ordering::Relaxed);
        }
    }

    /// Poll pending events for a subscription. Returns events since last poll.
    pub fn poll(&self, id: SubscriptionId) -> Vec<TopologyChangeEvent> {
        let mut subs = self.subscribers.write().unwrap_or_else(|e| e.into_inner());
        if let Some(sub) = subs.get_mut(&id) {
            sub.last_poll = Instant::now();
            let events: Vec<TopologyChangeEvent> = sub.pending_events.drain(..).collect();
            let count = events.len() as u64;
            if count > 0 {
                self.metrics.events_delivered.fetch_add(count, Ordering::Relaxed);
            }
            events
        } else {
            Vec::new()
        }
    }

    /// Evict idle subscriptions. Returns count of evicted subscriptions.
    pub fn evict_idle(&self) -> usize {
        let now = Instant::now();
        let mut subs = self.subscribers.write().unwrap_or_else(|e| e.into_inner());
        let before = subs.len();
        let timeout = self.idle_timeout;
        let mut deactivated = 0usize;
        for sub in subs.values_mut() {
            if sub.active && now.duration_since(sub.last_poll) > timeout {
                sub.active = false;
                deactivated += 1;
            }
        }
        subs.retain(|_, sub| sub.active);
        let evicted = before - subs.len();
        if deactivated > 0 {
            self.metrics
                .active_subscriptions
                .fetch_sub(deactivated as u64, Ordering::Relaxed);
        }
        if evicted > 0 {
            self.metrics.evictions.fetch_add(evicted as u64, Ordering::Relaxed);
        }
        evicted
    }

    /// Current active subscription count.
    pub fn active_count(&self) -> u64 {
        self.metrics.active_subscriptions.load(Ordering::Relaxed)
    }

    /// Current cluster epoch.
    pub fn epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3: ClientRoutingTable — Client-Local Shard→Leader Cache
// ═══════════════════════════════════════════════════════════════════════════

/// Client-local routing table: caches shard→leader mappings with epoch validation.
/// When a NOT_LEADER error is received, the stale entry is invalidated and the
/// client fetches a new snapshot.
pub struct ClientRoutingTable {
    /// Current cached epoch.
    epoch: AtomicU64,
    /// shard_id → CachedRoute
    routes: RwLock<HashMap<u64, CachedRoute>>,
    pub metrics: ClientRoutingMetrics,
}

/// A cached route entry.
#[derive(Debug, Clone)]
struct CachedRoute {
    _shard_id: ShardId,
    _leader_node: NodeId,
    leader_addr: String,
    _leader_epoch: u64,
    _cached_at: Instant,
}

/// Routing table metrics.
#[derive(Debug)]
pub struct ClientRoutingMetrics {
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub invalidations: AtomicU64,
    pub full_refreshes: AtomicU64,
}

impl Default for ClientRoutingMetrics {
    fn default() -> Self {
        Self {
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            full_refreshes: AtomicU64::new(0),
        }
    }
}

impl ClientRoutingTable {
    pub fn new() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            routes: RwLock::new(HashMap::new()),
            metrics: ClientRoutingMetrics::default(),
        }
    }

    /// Apply a topology snapshot — replaces all cached routes.
    pub fn apply_snapshot(&self, snapshot: &TopologySnapshot) {
        let mut routes = self.routes.write().unwrap_or_else(|e| e.into_inner());
        routes.clear();
        let now = Instant::now();
        for entry in &snapshot.shards {
            routes.insert(
                entry.shard_id.0,
                CachedRoute {
                    _shard_id: entry.shard_id,
                    _leader_node: entry.leader_node,
                    leader_addr: entry.leader_addr.clone(),
                    _leader_epoch: entry.leader_epoch,
                    _cached_at: now,
                },
            );
        }
        self.epoch.store(snapshot.epoch, Ordering::Relaxed);
        self.metrics.full_refreshes.fetch_add(1, Ordering::Relaxed);
    }

    /// Look up leader address for a shard.
    pub fn get_leader(&self, shard_id: ShardId) -> Option<String> {
        let routes = self.routes.read().unwrap_or_else(|e| e.into_inner());
        if let Some(route) = routes.get(&shard_id.0) {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            Some(route.leader_addr.clone())
        } else {
            self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Invalidate a shard's leader (after NOT_LEADER error).
    pub fn invalidate_shard(&self, shard_id: ShardId) {
        let mut routes = self.routes.write().unwrap_or_else(|e| e.into_inner());
        routes.remove(&shard_id.0);
        self.metrics.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    /// Update a single shard's leader (from NOT_LEADER hint).
    pub fn update_leader(&self, shard_id: ShardId, new_leader: NodeId, new_addr: String, new_epoch: u64) {
        let mut routes = self.routes.write().unwrap_or_else(|e| e.into_inner());
        routes.insert(
            shard_id.0,
            CachedRoute {
                _shard_id: shard_id,
                _leader_node: new_leader,
                leader_addr: new_addr,
                _leader_epoch: new_epoch,
                _cached_at: Instant::now(),
            },
        );
    }

    /// Current cached epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Number of cached routes.
    pub fn cached_route_count(&self) -> usize {
        let routes = self.routes.read().unwrap_or_else(|e| e.into_inner());
        routes.len()
    }
}

impl Default for ClientRoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4: NotLeaderRedirector — Auto-Redirect with Retry Budget
// ═══════════════════════════════════════════════════════════════════════════

/// Outcome of a redirect attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedirectOutcome {
    /// Successfully redirected to new leader — provide the address.
    Redirected { new_addr: String, attempt: u32 },
    /// Retry budget exhausted — give up.
    BudgetExhausted { attempts: u32 },
    /// Error is not retryable (fatal).
    NotRetryable,
}

/// Configuration for the redirector.
#[derive(Debug, Clone)]
pub struct RedirectorConfig {
    /// Maximum redirect attempts before giving up.
    pub max_attempts: u32,
    /// Base delay between retries (exponential backoff applied).
    pub base_delay: Duration,
    /// Maximum delay cap.
    pub max_delay: Duration,
    /// Jitter factor (0.0 to 1.0).
    pub jitter_factor: f64,
}

impl Default for RedirectorConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(2000),
            jitter_factor: 0.2,
        }
    }
}

/// Handles NOT_LEADER errors by redirecting to the correct leader with retry budget.
pub struct NotLeaderRedirector {
    config: RedirectorConfig,
    routing_table: Arc<ClientRoutingTable>,
    pub metrics: RedirectorMetrics,
}

/// Redirector metrics.
#[derive(Debug)]
pub struct RedirectorMetrics {
    pub redirect_attempts: AtomicU64,
    pub redirect_successes: AtomicU64,
    pub budget_exhaustions: AtomicU64,
    pub leader_hints_applied: AtomicU64,
}

impl Default for RedirectorMetrics {
    fn default() -> Self {
        Self {
            redirect_attempts: AtomicU64::new(0),
            redirect_successes: AtomicU64::new(0),
            budget_exhaustions: AtomicU64::new(0),
            leader_hints_applied: AtomicU64::new(0),
        }
    }
}

impl NotLeaderRedirector {
    pub fn new(config: RedirectorConfig, routing_table: Arc<ClientRoutingTable>) -> Self {
        Self {
            config,
            routing_table,
            metrics: RedirectorMetrics::default(),
        }
    }

    /// Handle a NOT_LEADER error.
    ///
    /// - If `leader_hint` is provided, update the routing table and redirect immediately.
    /// - Otherwise, invalidate the shard and try to look up a new leader.
    /// - Returns the redirect outcome.
    pub fn handle_not_leader(
        &self,
        shard_id: ShardId,
        leader_hint: Option<(NodeId, String)>,
        attempt: u32,
    ) -> RedirectOutcome {
        self.metrics.redirect_attempts.fetch_add(1, Ordering::Relaxed);

        if attempt >= self.config.max_attempts {
            self.metrics.budget_exhaustions.fetch_add(1, Ordering::Relaxed);
            return RedirectOutcome::BudgetExhausted { attempts: attempt };
        }

        // If we have a leader hint, apply it immediately.
        if let Some((node_id, addr)) = leader_hint {
            self.routing_table
                .update_leader(shard_id, node_id, addr.clone(), 0);
            self.metrics.leader_hints_applied.fetch_add(1, Ordering::Relaxed);
            self.metrics.redirect_successes.fetch_add(1, Ordering::Relaxed);
            return RedirectOutcome::Redirected {
                new_addr: addr,
                attempt: attempt + 1,
            };
        }

        // No hint — invalidate and check if we have another route cached.
        self.routing_table.invalidate_shard(shard_id);

        // After invalidation, the caller must refresh the routing table (poll snapshot).
        // For now, indicate budget is not yet exhausted, but we don't have a new address.
        RedirectOutcome::BudgetExhausted { attempts: attempt + 1 }
    }

    /// Calculate backoff delay for a given attempt number.
    pub fn backoff_delay(&self, attempt: u32) -> Duration {
        let base_ms = self.config.base_delay.as_millis() as f64;
        let exp = base_ms * 2.0_f64.powi(attempt as i32);
        let capped = exp.min(self.config.max_delay.as_millis() as f64);
        // Simple deterministic "jitter" — proportional reduction based on attempt
        let jittered = capped * (1.0 - self.config.jitter_factor * ((attempt % 3) as f64 / 3.0));
        Duration::from_millis(jittered.max(1.0) as u64)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5: ClientConnectionManager — Connection-Level Failover
// ═══════════════════════════════════════════════════════════════════════════

/// State of a connection to a gateway node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected.
    Disconnected,
    /// Connecting (handshake in progress).
    Connecting,
    /// Connected and healthy.
    Connected,
    /// Connection failed, will retry.
    Failed,
    /// Draining (graceful close).
    Draining,
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disconnected => write!(f, "disconnected"),
            Self::Connecting => write!(f, "connecting"),
            Self::Connected => write!(f, "connected"),
            Self::Failed => write!(f, "failed"),
            Self::Draining => write!(f, "draining"),
        }
    }
}

/// Tracked connection to a single gateway.
#[derive(Debug)]
struct GatewayConnection {
    address: String,
    state: ConnectionState,
    consecutive_failures: u32,
    last_success: Option<Instant>,
    last_failure: Option<Instant>,
    total_requests: u64,
    total_failures: u64,
}

/// Configuration for the connection manager.
#[derive(Debug, Clone)]
pub struct ConnectionManagerConfig {
    /// Seed gateway addresses.
    pub seeds: Vec<String>,
    /// Max consecutive failures before switching gateway.
    pub max_failures_before_switch: u32,
    /// Health check interval.
    pub health_check_interval: Duration,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Maximum reconnection backoff.
    pub max_reconnect_backoff: Duration,
}

impl Default for ConnectionManagerConfig {
    fn default() -> Self {
        Self {
            seeds: Vec::new(),
            max_failures_before_switch: 3,
            health_check_interval: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(3),
            max_reconnect_backoff: Duration::from_secs(30),
        }
    }
}

/// Manages connections to gateway nodes with automatic failover.
pub struct ClientConnectionManager {
    config: ConnectionManagerConfig,
    connections: RwLock<Vec<GatewayConnection>>,
    active_index: AtomicU64,
    _routing_table: Arc<ClientRoutingTable>,
    pub metrics: ConnectionManagerMetrics,
}

/// Connection manager metrics.
#[derive(Debug)]
pub struct ConnectionManagerMetrics {
    pub failovers: AtomicU64,
    pub total_connects: AtomicU64,
    pub total_connect_failures: AtomicU64,
    pub health_checks: AtomicU64,
    pub health_check_failures: AtomicU64,
}

impl Default for ConnectionManagerMetrics {
    fn default() -> Self {
        Self {
            failovers: AtomicU64::new(0),
            total_connects: AtomicU64::new(0),
            total_connect_failures: AtomicU64::new(0),
            health_checks: AtomicU64::new(0),
            health_check_failures: AtomicU64::new(0),
        }
    }
}

impl ClientConnectionManager {
    pub fn new(config: ConnectionManagerConfig, routing_table: Arc<ClientRoutingTable>) -> Self {
        let connections: Vec<GatewayConnection> = config
            .seeds
            .iter()
            .map(|addr| GatewayConnection {
                address: addr.clone(),
                state: ConnectionState::Disconnected,
                consecutive_failures: 0,
                last_success: None,
                last_failure: None,
                total_requests: 0,
                total_failures: 0,
            })
            .collect();

        Self {
            config,
            connections: RwLock::new(connections),
            active_index: AtomicU64::new(0),
            _routing_table: routing_table,
            metrics: ConnectionManagerMetrics::default(),
        }
    }

    /// Get the address of the currently active gateway.
    pub fn active_address(&self) -> Option<String> {
        let conns = self.connections.read().unwrap_or_else(|e| e.into_inner());
        let idx = self.active_index.load(Ordering::Relaxed) as usize;
        conns.get(idx).map(|c| c.address.clone())
    }

    /// Get the state of the currently active connection.
    pub fn active_state(&self) -> ConnectionState {
        let conns = self.connections.read().unwrap_or_else(|e| e.into_inner());
        let idx = self.active_index.load(Ordering::Relaxed) as usize;
        conns
            .get(idx)
            .map(|c| c.state)
            .unwrap_or(ConnectionState::Disconnected)
    }

    /// Simulate establishing a connection to the active gateway.
    pub fn connect(&self) -> Result<String, String> {
        let mut conns = self.connections.write().unwrap_or_else(|e| e.into_inner());
        let idx = self.active_index.load(Ordering::Relaxed) as usize;
        if idx >= conns.len() {
            return Err("No gateways configured".into());
        }

        conns[idx].state = ConnectionState::Connecting;
        self.metrics.total_connects.fetch_add(1, Ordering::Relaxed);

        // Simulate: connection succeeds
        conns[idx].state = ConnectionState::Connected;
        conns[idx].consecutive_failures = 0;
        conns[idx].last_success = Some(Instant::now());

        Ok(conns[idx].address.clone())
    }

    /// Report a request success on the active connection.
    pub fn report_success(&self) {
        let mut conns = self.connections.write().unwrap_or_else(|e| e.into_inner());
        let idx = self.active_index.load(Ordering::Relaxed) as usize;
        if let Some(conn) = conns.get_mut(idx) {
            conn.consecutive_failures = 0;
            conn.total_requests += 1;
            conn.last_success = Some(Instant::now());
        }
    }

    /// Report a request failure. Returns true if failover occurred.
    pub fn report_failure(&self) -> bool {
        let mut conns = self.connections.write().unwrap_or_else(|e| e.into_inner());
        let idx = self.active_index.load(Ordering::Relaxed) as usize;
        if let Some(conn) = conns.get_mut(idx) {
            conn.consecutive_failures += 1;
            conn.total_failures += 1;
            conn.last_failure = Some(Instant::now());

            if conn.consecutive_failures >= self.config.max_failures_before_switch {
                conn.state = ConnectionState::Failed;
                drop(conns);
                return self.failover();
            }
        }
        false
    }

    /// Switch to the next available gateway.
    pub fn failover(&self) -> bool {
        let conns = self.connections.read().unwrap_or_else(|e| e.into_inner());
        if conns.len() <= 1 {
            return false;
        }
        let current = self.active_index.load(Ordering::Relaxed) as usize;
        let next = (current + 1) % conns.len();
        drop(conns);

        self.active_index.store(next as u64, Ordering::Relaxed);
        self.metrics.failovers.fetch_add(1, Ordering::Relaxed);

        // Mark new connection as connecting
        let mut conns = self.connections.write().unwrap_or_else(|e| e.into_inner());
        if let Some(conn) = conns.get_mut(next) {
            conn.state = ConnectionState::Connecting;
            conn.consecutive_failures = 0;
        }
        true
    }

    /// Calculate reconnection backoff for a given attempt number.
    pub fn reconnect_backoff(&self, attempt: u32) -> Duration {
        let base = 100u64; // 100ms base
        let exp = base.saturating_mul(2u64.saturating_pow(attempt));
        let capped = exp.min(self.config.max_reconnect_backoff.as_millis() as u64);
        Duration::from_millis(capped)
    }

    /// Number of configured gateways.
    pub fn gateway_count(&self) -> usize {
        let conns = self.connections.read().unwrap_or_else(|e| e.into_inner());
        conns.len()
    }

    /// Connection summary for diagnostics.
    pub fn connection_summary(&self) -> Vec<(String, ConnectionState, u32)> {
        let conns = self.connections.read().unwrap_or_else(|e| e.into_inner());
        conns
            .iter()
            .map(|c| (c.address.clone(), c.state, c.consecutive_failures))
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6: Discovery Endpoint — Server-Side Topology Provider
// ═══════════════════════════════════════════════════════════════════════════

/// Server-side topology provider that builds snapshots for clients.
pub struct TopologyProvider {
    /// Current epoch.
    epoch: AtomicU64,
    /// Latest snapshot.
    latest_snapshot: RwLock<Option<TopologySnapshot>>,
    /// Subscription manager for push notifications.
    subscriptions: Arc<TopologySubscriptionManager>,
    pub metrics: ProviderMetrics,
}

/// Provider metrics.
#[derive(Debug)]
pub struct ProviderMetrics {
    pub snapshots_served: AtomicU64,
    pub snapshot_builds: AtomicU64,
}

impl Default for ProviderMetrics {
    fn default() -> Self {
        Self {
            snapshots_served: AtomicU64::new(0),
            snapshot_builds: AtomicU64::new(0),
        }
    }
}

impl TopologyProvider {
    pub fn new(subscriptions: Arc<TopologySubscriptionManager>) -> Self {
        Self {
            epoch: AtomicU64::new(0),
            latest_snapshot: RwLock::new(None),
            subscriptions,
            metrics: ProviderMetrics::default(),
        }
    }

    /// Build and cache a new topology snapshot from current cluster state.
    pub fn build_snapshot(
        &self,
        shards: Vec<ShardRouteEntry>,
        nodes: Vec<NodeDirectoryEntry>,
        gateway_endpoints: Vec<String>,
    ) -> TopologySnapshot {
        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let snapshot = TopologySnapshot {
            epoch,
            timestamp_ms: now,
            shards,
            nodes,
            gateway_endpoints,
        };

        let mut cached = self.latest_snapshot.write().unwrap_or_else(|e| e.into_inner());
        *cached = Some(snapshot.clone());
        self.metrics.snapshot_builds.fetch_add(1, Ordering::Relaxed);

        snapshot
    }

    /// Get the latest cached snapshot, if client's epoch is stale.
    /// Returns None if the client is already up-to-date.
    pub fn get_snapshot_if_newer(&self, client_epoch: u64) -> Option<TopologySnapshot> {
        let cached = self.latest_snapshot.read().unwrap_or_else(|e| e.into_inner());
        if let Some(ref snapshot) = *cached {
            if snapshot.is_newer_than(client_epoch) {
                self.metrics.snapshots_served.fetch_add(1, Ordering::Relaxed);
                return Some(snapshot.clone());
            }
        }
        None
    }

    /// Get the latest snapshot unconditionally.
    pub fn get_latest_snapshot(&self) -> Option<TopologySnapshot> {
        let cached = self.latest_snapshot.read().unwrap_or_else(|e| e.into_inner());
        if cached.is_some() {
            self.metrics.snapshots_served.fetch_add(1, Ordering::Relaxed);
        }
        cached.clone()
    }

    /// Publish a topology change and notify all subscribers.
    pub fn publish_change(&self, change: TopologyChangeEvent) {
        self.subscriptions.publish(change);
    }

    /// Current epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_snapshot(epoch: u64, num_shards: u64) -> TopologySnapshot {
        let shards: Vec<ShardRouteEntry> = (0..num_shards)
            .map(|i| {
                let range_size = u64::MAX / num_shards;
                ShardRouteEntry {
                    shard_id: ShardId(i),
                    leader_node: NodeId(i + 1),
                    leader_addr: format!("node{}:5443", i + 1),
                    replica_addrs: vec![
                        format!("node{}:5443", i + 1),
                        format!("node{}:5443", (i + 1) % num_shards + 1),
                    ],
                    hash_range_start: i * range_size,
                    hash_range_end: if i == num_shards - 1 {
                        u64::MAX
                    } else {
                        (i + 1) * range_size
                    },
                    leader_epoch: epoch,
                }
            })
            .collect();

        let nodes: Vec<NodeDirectoryEntry> = (0..num_shards)
            .map(|i| NodeDirectoryEntry {
                node_id: NodeId(i + 1),
                address: format!("node{}:5443", i + 1),
                role: "smart_gateway".into(),
                is_alive: true,
            })
            .collect();

        TopologySnapshot {
            epoch,
            timestamp_ms: 1000000,
            shards,
            nodes,
            gateway_endpoints: vec!["gw1:5443".into(), "gw2:5443".into()],
        }
    }

    // ── TopologySnapshot tests ──

    #[test]
    fn test_snapshot_is_newer_than() {
        let snap = make_snapshot(5, 3);
        assert!(snap.is_newer_than(4));
        assert!(snap.is_newer_than(0));
        assert!(!snap.is_newer_than(5));
        assert!(!snap.is_newer_than(6));
    }

    #[test]
    fn test_snapshot_leader_lookup() {
        let snap = make_snapshot(1, 3);
        assert_eq!(snap.leader_for_shard(ShardId(0)), Some("node1:5443"));
        assert_eq!(snap.leader_for_shard(ShardId(1)), Some("node2:5443"));
        assert_eq!(snap.leader_for_shard(ShardId(99)), None);
    }

    #[test]
    fn test_snapshot_shard_for_hash() {
        let snap = make_snapshot(1, 3);
        let entry = snap.shard_for_hash(0).unwrap();
        assert_eq!(entry.shard_id, ShardId(0));
        let entry = snap.shard_for_hash(u64::MAX - 1).unwrap();
        assert_eq!(entry.shard_id, ShardId(2));
    }

    #[test]
    fn test_snapshot_counts() {
        let snap = make_snapshot(1, 4);
        assert_eq!(snap.shard_count(), 4);
        assert_eq!(snap.alive_node_count(), 4);
    }

    // ── Subscription tests ──

    #[test]
    fn test_subscribe_and_poll() {
        let mgr = TopologySubscriptionManager::new(1, Duration::from_secs(60));
        let id = mgr.subscribe(0);
        assert_eq!(mgr.active_count(), 1);

        mgr.publish(TopologyChangeEvent {
            new_epoch: 2,
            change_type: TopologyChangeType::LeaderChange {
                shard_id: ShardId(0),
                new_leader: NodeId(2),
            },
            timestamp_ms: 1000,
            leader_hint: Some("node2:5443".into()),
        });

        let events = mgr.poll(id);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].new_epoch, 2);
        assert_eq!(mgr.epoch(), 2);

        // Second poll returns empty
        let events = mgr.poll(id);
        assert!(events.is_empty());
    }

    #[test]
    fn test_unsubscribe() {
        let mgr = TopologySubscriptionManager::new(1, Duration::from_secs(60));
        let id = mgr.subscribe(0);
        assert_eq!(mgr.active_count(), 1);
        assert!(mgr.unsubscribe(id));
        assert_eq!(mgr.active_count(), 0);
        // Unsubscribing again returns false
        assert!(!mgr.unsubscribe(id));
    }

    #[test]
    fn test_subscription_overflow_drops_oldest() {
        let mgr = TopologySubscriptionManager::new(1, Duration::from_secs(60));
        let id = mgr.subscribe(0);

        // Publish 101 events (max_pending = 100)
        for i in 0..101 {
            mgr.publish(TopologyChangeEvent {
                new_epoch: i + 2,
                change_type: TopologyChangeType::FullRefresh,
                timestamp_ms: i * 100,
                leader_hint: None,
            });
        }

        let events = mgr.poll(id);
        assert_eq!(events.len(), 100); // oldest was dropped
        assert_eq!(events[0].new_epoch, 3); // epoch 2 was dropped
        assert!(mgr.metrics.events_dropped.load(Ordering::Relaxed) > 0);
    }

    // ── ClientRoutingTable tests ──

    #[test]
    fn test_routing_table_apply_snapshot() {
        let table = ClientRoutingTable::new();
        let snap = make_snapshot(5, 3);
        table.apply_snapshot(&snap);

        assert_eq!(table.epoch(), 5);
        assert_eq!(table.cached_route_count(), 3);
        assert_eq!(table.get_leader(ShardId(0)), Some("node1:5443".into()));
        assert_eq!(table.get_leader(ShardId(2)), Some("node3:5443".into()));
        assert_eq!(table.get_leader(ShardId(99)), None);
    }

    #[test]
    fn test_routing_table_invalidate_and_update() {
        let table = ClientRoutingTable::new();
        let snap = make_snapshot(5, 2);
        table.apply_snapshot(&snap);

        // Invalidate shard 0
        table.invalidate_shard(ShardId(0));
        assert_eq!(table.get_leader(ShardId(0)), None);
        assert_eq!(table.cached_route_count(), 1);

        // Update with new leader
        table.update_leader(ShardId(0), NodeId(99), "newnode:5443".into(), 6);
        assert_eq!(table.get_leader(ShardId(0)), Some("newnode:5443".into()));
    }

    // ── NotLeaderRedirector tests ──

    #[test]
    fn test_redirector_with_leader_hint() {
        let table = Arc::new(ClientRoutingTable::new());
        let redirector = NotLeaderRedirector::new(RedirectorConfig::default(), table.clone());

        let outcome = redirector.handle_not_leader(
            ShardId(0),
            Some((NodeId(5), "node5:5443".into())),
            0,
        );

        assert_eq!(
            outcome,
            RedirectOutcome::Redirected {
                new_addr: "node5:5443".into(),
                attempt: 1,
            }
        );
        // Routing table should be updated
        assert_eq!(table.get_leader(ShardId(0)), Some("node5:5443".into()));
    }

    #[test]
    fn test_redirector_budget_exhausted() {
        let table = Arc::new(ClientRoutingTable::new());
        let config = RedirectorConfig {
            max_attempts: 3,
            ..Default::default()
        };
        let redirector = NotLeaderRedirector::new(config, table);

        let outcome = redirector.handle_not_leader(ShardId(0), None, 3);
        assert_eq!(outcome, RedirectOutcome::BudgetExhausted { attempts: 3 });
    }

    #[test]
    fn test_redirector_backoff_exponential() {
        let table = Arc::new(ClientRoutingTable::new());
        let config = RedirectorConfig {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(5000),
            jitter_factor: 0.0,
            ..Default::default()
        };
        let redirector = NotLeaderRedirector::new(config, table);

        let d0 = redirector.backoff_delay(0);
        let d1 = redirector.backoff_delay(1);
        let d2 = redirector.backoff_delay(2);

        assert_eq!(d0.as_millis(), 100); // 100 * 2^0
        assert_eq!(d1.as_millis(), 200); // 100 * 2^1
        assert_eq!(d2.as_millis(), 400); // 100 * 2^2
    }

    // ── ClientConnectionManager tests ──

    #[test]
    fn test_connection_manager_connect_and_failover() {
        let table = Arc::new(ClientRoutingTable::new());
        let config = ConnectionManagerConfig {
            seeds: vec!["gw1:5443".into(), "gw2:5443".into(), "gw3:5443".into()],
            max_failures_before_switch: 2,
            ..Default::default()
        };
        let mgr = ClientConnectionManager::new(config, table);

        // Connect to first gateway
        let addr = mgr.connect().unwrap();
        assert_eq!(addr, "gw1:5443");
        assert_eq!(mgr.active_state(), ConnectionState::Connected);

        // Report failures until failover
        assert!(!mgr.report_failure()); // 1st failure — no failover yet
        assert!(mgr.report_failure());  // 2nd failure — failover!

        let addr = mgr.active_address().unwrap();
        assert_eq!(addr, "gw2:5443");
        assert_eq!(mgr.metrics.failovers.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_connection_manager_wraps_around() {
        let table = Arc::new(ClientRoutingTable::new());
        let config = ConnectionManagerConfig {
            seeds: vec!["gw1:5443".into(), "gw2:5443".into()],
            max_failures_before_switch: 1,
            ..Default::default()
        };
        let mgr = ClientConnectionManager::new(config, table);

        mgr.report_failure(); // failover to gw2
        assert_eq!(mgr.active_address().unwrap(), "gw2:5443");

        mgr.report_failure(); // failover wraps back to gw1
        assert_eq!(mgr.active_address().unwrap(), "gw1:5443");
    }

    #[test]
    fn test_connection_manager_reconnect_backoff() {
        let table = Arc::new(ClientRoutingTable::new());
        let config = ConnectionManagerConfig {
            max_reconnect_backoff: Duration::from_secs(10),
            ..Default::default()
        };
        let mgr = ClientConnectionManager::new(config, table);

        let d0 = mgr.reconnect_backoff(0);
        let d1 = mgr.reconnect_backoff(1);
        let d5 = mgr.reconnect_backoff(5);

        assert_eq!(d0.as_millis(), 100);
        assert_eq!(d1.as_millis(), 200);
        // d5 = 100 * 2^5 = 3200, capped at 10000
        assert_eq!(d5.as_millis(), 3200);
    }

    #[test]
    fn test_connection_summary() {
        let table = Arc::new(ClientRoutingTable::new());
        let config = ConnectionManagerConfig {
            seeds: vec!["gw1:5443".into(), "gw2:5443".into()],
            ..Default::default()
        };
        let mgr = ClientConnectionManager::new(config, table);
        let summary = mgr.connection_summary();
        assert_eq!(summary.len(), 2);
        assert_eq!(summary[0].0, "gw1:5443");
        assert_eq!(summary[0].1, ConnectionState::Disconnected);
    }

    // ── TopologyProvider tests ──

    #[test]
    fn test_provider_build_and_serve_snapshot() {
        let sub_mgr = Arc::new(TopologySubscriptionManager::new(0, Duration::from_secs(60)));
        let provider = TopologyProvider::new(sub_mgr);

        // No snapshot yet
        assert!(provider.get_latest_snapshot().is_none());

        // Build a snapshot
        let snap = provider.build_snapshot(
            vec![ShardRouteEntry {
                shard_id: ShardId(0),
                leader_node: NodeId(1),
                leader_addr: "node1:5443".into(),
                replica_addrs: vec!["node1:5443".into()],
                hash_range_start: 0,
                hash_range_end: u64::MAX,
                leader_epoch: 1,
            }],
            vec![NodeDirectoryEntry {
                node_id: NodeId(1),
                address: "node1:5443".into(),
                role: "smart_gateway".into(),
                is_alive: true,
            }],
            vec!["gw1:5443".into()],
        );

        assert_eq!(snap.epoch, 1);
        assert_eq!(provider.epoch(), 1);

        // Client with epoch 0 gets the snapshot
        let fetched = provider.get_snapshot_if_newer(0).unwrap();
        assert_eq!(fetched.epoch, 1);

        // Client with epoch 1 gets None (already up to date)
        assert!(provider.get_snapshot_if_newer(1).is_none());
    }

    #[test]
    fn test_provider_publishes_to_subscribers() {
        let sub_mgr = Arc::new(TopologySubscriptionManager::new(0, Duration::from_secs(60)));
        let provider = TopologyProvider::new(sub_mgr.clone());

        let sub_id = sub_mgr.subscribe(0);

        provider.publish_change(TopologyChangeEvent {
            new_epoch: 1,
            change_type: TopologyChangeType::LeaderChange {
                shard_id: ShardId(0),
                new_leader: NodeId(2),
            },
            timestamp_ms: 1000,
            leader_hint: Some("node2:5443".into()),
        });

        let events = sub_mgr.poll(sub_id);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].new_epoch, 1);
    }

    // ── E2E: Discovery lifecycle test ──

    #[test]
    fn test_e2e_client_discovery_lifecycle() {
        // 1. Server builds topology
        let sub_mgr = Arc::new(TopologySubscriptionManager::new(0, Duration::from_secs(60)));
        let provider = TopologyProvider::new(sub_mgr.clone());

        let snap = provider.build_snapshot(
            vec![
                ShardRouteEntry {
                    shard_id: ShardId(0),
                    leader_node: NodeId(1),
                    leader_addr: "node1:5443".into(),
                    replica_addrs: vec!["node1:5443".into(), "node2:5443".into()],
                    hash_range_start: 0,
                    hash_range_end: u64::MAX / 2,
                    leader_epoch: 1,
                },
                ShardRouteEntry {
                    shard_id: ShardId(1),
                    leader_node: NodeId(2),
                    leader_addr: "node2:5443".into(),
                    replica_addrs: vec!["node2:5443".into(), "node1:5443".into()],
                    hash_range_start: u64::MAX / 2,
                    hash_range_end: u64::MAX,
                    leader_epoch: 1,
                },
            ],
            vec![
                NodeDirectoryEntry { node_id: NodeId(1), address: "node1:5443".into(), role: "smart_gateway".into(), is_alive: true },
                NodeDirectoryEntry { node_id: NodeId(2), address: "node2:5443".into(), role: "smart_gateway".into(), is_alive: true },
            ],
            vec!["node1:5443".into(), "node2:5443".into()],
        );

        // 2. Client connects and applies snapshot
        let table = Arc::new(ClientRoutingTable::new());
        table.apply_snapshot(&snap);
        assert_eq!(table.cached_route_count(), 2);
        assert_eq!(table.get_leader(ShardId(0)), Some("node1:5443".into()));

        // 3. Client subscribes for changes
        let sub_id = sub_mgr.subscribe(snap.epoch);

        // 4. Leader changes: shard 0 fails over to node 2
        provider.publish_change(TopologyChangeEvent {
            new_epoch: 2,
            change_type: TopologyChangeType::LeaderChange {
                shard_id: ShardId(0),
                new_leader: NodeId(2),
            },
            timestamp_ms: 2000,
            leader_hint: Some("node2:5443".into()),
        });

        // 5. Client polls and gets the change
        let events = sub_mgr.poll(sub_id);
        assert_eq!(events.len(), 1);

        // 6. Client handles NOT_LEADER using the redirector
        let redirector = NotLeaderRedirector::new(RedirectorConfig::default(), table.clone());
        let outcome = redirector.handle_not_leader(
            ShardId(0),
            Some((NodeId(2), "node2:5443".into())),
            0,
        );
        assert_eq!(
            outcome,
            RedirectOutcome::Redirected {
                new_addr: "node2:5443".into(),
                attempt: 1,
            }
        );

        // 7. Routing table is now updated
        assert_eq!(table.get_leader(ShardId(0)), Some("node2:5443".into()));
    }
}
