//! Smart Gateway v1 — Unified Cluster Access (v1.0.8)
//!
//! # Cluster Access Model
//! - Clients connect to **any** gateway node (dedicated or compute node)
//! - Gateway routes requests to the correct shard leader transparently
//! - Leader changes never require client reconnection
//! - No global unique primary node — every gateway is a valid entry point
//!
//! # Smart Routing
//! - Local shard/leader topology cache (epoch-versioned)
//! - Fast leader change detection via epoch bumps
//! - Request classification: LOCAL_EXEC / FORWARD_TO_LEADER / REJECT_*
//! - No blind forwarding, no silent retry, no infinite queuing
//!
//! # JDBC Error Contract
//! - Every error carries a `GatewayErrorCode` with explicit retry semantics
//! - Server returns retry hints (delay, leader address) in error responses
//! - Clients never need to guess whether to retry

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use falcon_common::types::{NodeId, ShardId};

// ═══════════════════════════════════════════════════════════════════════════
// Cluster Access Model
// ═══════════════════════════════════════════════════════════════════════════

/// The role a node plays in the cluster access model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GatewayRole {
    /// Dedicated gateway node — only routes, does not own shards.
    DedicatedGateway,
    /// Compute node acting as a smart gateway — owns shards AND routes.
    SmartGateway,
    /// Compute-only node — owns shards, does not accept external client connections.
    ComputeOnly,
}

impl GatewayRole {
    pub const fn accepts_client_connections(&self) -> bool {
        matches!(self, Self::DedicatedGateway | Self::SmartGateway)
    }

    pub const fn owns_shards(&self) -> bool {
        matches!(self, Self::SmartGateway | Self::ComputeOnly)
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DedicatedGateway => "dedicated_gateway",
            Self::SmartGateway => "smart_gateway",
            Self::ComputeOnly => "compute_only",
        }
    }
}

impl std::fmt::Display for GatewayRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Deployment topology recommendation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterTopology {
    /// 1–3 nodes: every node is SmartGateway.
    Small,
    /// 4–8 nodes: every node is SmartGateway, consider dedicated gateway.
    Medium,
    /// 9+ nodes: dedicated gateway nodes recommended.
    Large,
}

impl ClusterTopology {
    pub const fn recommend(node_count: usize) -> Self {
        match node_count {
            0..=3 => Self::Small,
            4..=8 => Self::Medium,
            _ => Self::Large,
        }
    }

    pub const fn description(&self) -> &'static str {
        match self {
            Self::Small => "All nodes act as SmartGateway (routes + computes)",
            Self::Medium => "All SmartGateway; consider 1-2 DedicatedGateway for isolation",
            Self::Large => "Dedicated gateway tier recommended for routing isolation",
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Gateway Error Codes — JDBC Retry Contract
// ═══════════════════════════════════════════════════════════════════════════

/// Error codes returned by the gateway to clients.
/// Each code has well-defined retry semantics that JDBC drivers MUST follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GatewayErrorCode {
    /// Leader changed — client SHOULD retry immediately (new leader hint provided).
    NotLeader,
    /// No route to target shard — client SHOULD retry after short delay.
    NoRoute,
    /// Gateway is overloaded — client SHOULD retry after backoff delay.
    Overloaded,
    /// Request timed out — client MAY retry with backoff.
    Timeout,
    /// Fatal error — client MUST NOT retry (bad SQL, auth failure, etc.).
    Fatal,
}

impl GatewayErrorCode {
    /// Whether the client should retry this error.
    pub const fn is_retryable(&self) -> bool {
        !matches!(self, Self::Fatal)
    }

    /// Recommended retry delay in milliseconds.
    /// 0 = retry immediately, None = do not retry.
    pub const fn retry_delay_ms(&self) -> Option<u64> {
        match self {
            Self::NotLeader => Some(0),       // Immediate retry with new leader
            Self::NoRoute => Some(100),       // Short delay, topology may be updating
            Self::Overloaded => Some(500),    // Backoff, gateway is under pressure
            Self::Timeout => Some(200),       // Moderate delay
            Self::Fatal => None,              // Do not retry
        }
    }

    /// SQLSTATE-compatible error code string.
    pub const fn sqlstate(&self) -> &'static str {
        match self {
            Self::NotLeader => "FD001",    // FalconDB-specific: leader changed
            Self::NoRoute => "FD002",      // FalconDB-specific: no route
            Self::Overloaded => "FD003",   // FalconDB-specific: overloaded
            Self::Timeout => "FD004",      // FalconDB-specific: timeout
            Self::Fatal => "FD000",        // FalconDB-specific: fatal
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotLeader => "NOT_LEADER",
            Self::NoRoute => "NO_ROUTE",
            Self::Overloaded => "OVERLOADED",
            Self::Timeout => "TIMEOUT",
            Self::Fatal => "FATAL",
        }
    }
}

impl std::fmt::Display for GatewayErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A gateway error with retry hints for the client.
#[derive(Debug, Clone)]
pub struct GatewayError {
    pub code: GatewayErrorCode,
    pub message: String,
    /// Hint: address of the current leader (for NOT_LEADER errors).
    pub leader_hint: Option<String>,
    /// Hint: recommended retry delay in milliseconds.
    pub retry_after_ms: Option<u64>,
    /// The epoch at the time of the error.
    pub epoch: u64,
    /// The shard that was targeted (0 if not shard-specific).
    pub shard_id: u64,
}

impl GatewayError {
    pub fn not_leader(shard_id: u64, epoch: u64, leader_hint: Option<String>) -> Self {
        Self {
            code: GatewayErrorCode::NotLeader,
            message: format!("Shard {shard_id} leader changed at epoch {epoch}"),
            leader_hint,
            retry_after_ms: Some(0),
            epoch,
            shard_id,
        }
    }

    pub fn no_route(shard_id: u64, epoch: u64) -> Self {
        Self {
            code: GatewayErrorCode::NoRoute,
            message: format!("No route to shard {shard_id} at epoch {epoch}"),
            leader_hint: None,
            retry_after_ms: Some(100),
            epoch,
            shard_id,
        }
    }

    pub fn overloaded(reason: &str) -> Self {
        Self {
            code: GatewayErrorCode::Overloaded,
            message: format!("Gateway overloaded: {reason}"),
            leader_hint: None,
            retry_after_ms: Some(500),
            epoch: 0,
            shard_id: 0,
        }
    }

    pub fn timeout(shard_id: u64, elapsed_ms: u64) -> Self {
        Self {
            code: GatewayErrorCode::Timeout,
            message: format!("Request to shard {shard_id} timed out after {elapsed_ms}ms"),
            leader_hint: None,
            retry_after_ms: Some(200),
            epoch: 0,
            shard_id,
        }
    }

    pub const fn fatal(message: String) -> Self {
        Self {
            code: GatewayErrorCode::Fatal,
            message,
            leader_hint: None,
            retry_after_ms: None,
            epoch: 0,
            shard_id: 0,
        }
    }
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// JDBC Multi-Host URL
// ═══════════════════════════════════════════════════════════════════════════

/// Parsed JDBC connection URL with multi-host support.
/// Format: `jdbc:falcondb://host1:port1,host2:port2,host3:port3/dbname?params`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JdbcConnectionUrl {
    /// Seed gateway list (host:port pairs).
    pub seeds: Vec<HostPort>,
    /// Database name.
    pub database: String,
    /// Connection parameters.
    pub params: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPort {
    pub host: String,
    pub port: u16,
}

impl std::fmt::Display for HostPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl JdbcConnectionUrl {
    /// Parse a JDBC URL.
    /// Supports: `jdbc:falcondb://host1:port,host2:port/dbname?key=val&key2=val2`
    pub fn parse(url: &str) -> Result<Self, String> {
        let stripped = url
            .strip_prefix("jdbc:falcondb://")
            .ok_or_else(|| format!("URL must start with 'jdbc:falcondb://': {url}"))?;

        // Split off query params
        let (path_part, params) = if let Some(idx) = stripped.find('?') {
            let params_str = &stripped[idx + 1..];
            let params: HashMap<String, String> = params_str
                .split('&')
                .filter_map(|kv| {
                    let mut parts = kv.splitn(2, '=');
                    let key = parts.next()?.to_owned();
                    let val = parts.next().unwrap_or("").to_owned();
                    Some((key, val))
                })
                .collect();
            (&stripped[..idx], params)
        } else {
            (stripped, HashMap::new())
        };

        // Split host list from database
        let (hosts_str, database) = if let Some(idx) = path_part.find('/') {
            (&path_part[..idx], path_part[idx + 1..].to_string())
        } else {
            (path_part, "falcon".to_owned())
        };

        // Parse host:port pairs
        let seeds: Vec<HostPort> = hosts_str
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|hp| {
                if let Some(colon) = hp.rfind(':') {
                    let host = hp[..colon].to_string();
                    let port = hp[colon + 1..]
                        .parse::<u16>()
                        .map_err(|_| format!("Invalid port in '{hp}'"))?;
                    Ok(HostPort { host, port })
                } else {
                    Ok(HostPort {
                        host: hp.to_owned(),
                        port: 5443, // Default FalconDB port
                    })
                }
            })
            .collect::<Result<Vec<_>, String>>()?;

        if seeds.is_empty() {
            return Err("At least one host is required".to_owned());
        }

        Ok(Self {
            seeds,
            database,
            params,
        })
    }

    /// Format back to JDBC URL string.
    pub fn to_url(&self) -> String {
        let hosts: Vec<String> = self.seeds.iter().map(std::string::ToString::to_string).collect();
        let mut url = format!("jdbc:falcondb://{}/{}", hosts.join(","), self.database);
        if !self.params.is_empty() {
            let params: Vec<String> = self
                .params
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            url.push('?');
            url.push_str(&params.join("&"));
        }
        url
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Gateway Topology Cache — epoch-versioned shard→leader mapping
// ═══════════════════════════════════════════════════════════════════════════

/// A single entry in the topology cache: shard → leader + metadata.
#[derive(Debug, Clone)]
pub struct TopologyEntry {
    pub shard_id: ShardId,
    pub leader_node: NodeId,
    pub leader_addr: String,
    /// Epoch at which this mapping was established.
    pub epoch: u64,
    /// When this entry was last verified.
    pub last_verified: Instant,
}

/// Epoch-versioned topology cache for the gateway.
///
/// - Versioned: every update bumps the epoch
/// - Invalidation: stale entries detected via epoch mismatch
/// - Thread-safe: RwLock for read-heavy workload
pub struct TopologyCache {
    /// Current epoch (monotonically increasing).
    epoch: AtomicU64,
    /// shard_id → TopologyEntry
    entries: RwLock<HashMap<u64, TopologyEntry>>,
    /// Node directory: node_id → address
    node_directory: RwLock<HashMap<u64, NodeInfo>>,
    /// Metrics
    pub metrics: TopologyCacheMetrics,
}

/// Node info in the topology.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub address: String,
    pub role: GatewayRole,
    pub is_alive: bool,
    pub last_heartbeat: Instant,
}

/// Metrics for the topology cache.
#[derive(Debug)]
pub struct TopologyCacheMetrics {
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub epoch_bumps: AtomicU64,
    pub invalidations: AtomicU64,
    pub leader_changes: AtomicU64,
}

impl Default for TopologyCacheMetrics {
    fn default() -> Self {
        Self {
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            epoch_bumps: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            leader_changes: AtomicU64::new(0),
        }
    }
}

/// Snapshot of topology cache metrics.
#[derive(Debug, Clone)]
pub struct TopologyCacheMetricsSnapshot {
    pub epoch: u64,
    pub shard_count: usize,
    pub node_count: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub epoch_bumps: u64,
    pub invalidations: u64,
    pub leader_changes: u64,
    pub hit_rate: f64,
}

impl TopologyCache {
    pub fn new() -> Self {
        Self {
            epoch: AtomicU64::new(1),
            entries: RwLock::new(HashMap::new()),
            node_directory: RwLock::new(HashMap::new()),
            metrics: TopologyCacheMetrics::default(),
        }
    }

    /// Current epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Bump epoch and return the new value.
    pub fn bump_epoch(&self) -> u64 {
        self.metrics.epoch_bumps.fetch_add(1, Ordering::Relaxed);
        self.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Register or update a node in the directory.
    pub fn register_node(&self, node_id: NodeId, address: String, role: GatewayRole) {
        let mut dir = self.node_directory.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        dir.insert(
            node_id.0,
            NodeInfo {
                node_id,
                address,
                role,
                is_alive: true,
                last_heartbeat: Instant::now(),
            },
        );
    }

    /// Mark a node as dead.
    pub fn mark_node_dead(&self, node_id: NodeId) {
        let mut dir = self.node_directory.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(info) = dir.get_mut(&node_id.0) {
            info.is_alive = false;
        }
    }

    /// Mark a node as alive (heartbeat received).
    pub fn mark_node_alive(&self, node_id: NodeId) {
        let mut dir = self.node_directory.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(info) = dir.get_mut(&node_id.0) {
            info.is_alive = true;
            info.last_heartbeat = Instant::now();
        }
    }

    /// Update the leader for a shard. Bumps epoch if leader changed.
    pub fn update_leader(
        &self,
        shard_id: ShardId,
        leader_node: NodeId,
        leader_addr: String,
    ) -> u64 {
        let mut entries = self.entries.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let current_epoch = self.epoch.load(Ordering::Acquire);

        let changed = entries
            .get(&shard_id.0)
            .is_none_or(|e| e.leader_node != leader_node);

        let epoch = if changed {
            self.metrics.leader_changes.fetch_add(1, Ordering::Relaxed);
            let new_epoch = self.bump_epoch();
            entries.insert(
                shard_id.0,
                TopologyEntry {
                    shard_id,
                    leader_node,
                    leader_addr,
                    epoch: new_epoch,
                    last_verified: Instant::now(),
                },
            );
            new_epoch
        } else {
            // Same leader — just refresh timestamp
            if let Some(entry) = entries.get_mut(&shard_id.0) {
                entry.last_verified = Instant::now();
            }
            current_epoch
        };
        epoch
    }

    /// Look up the leader for a shard.
    pub fn get_leader(&self, shard_id: ShardId) -> Option<TopologyEntry> {
        let entries = self.entries.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = entries.get(&shard_id.0) {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.clone())
        } else {
            self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Invalidate a shard's leader entry (e.g., after a NOT_LEADER error).
    pub fn invalidate(&self, shard_id: ShardId) {
        let mut entries = self.entries.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.remove(&shard_id.0);
        self.metrics.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    /// Invalidate all entries for a given node (node crash).
    pub fn invalidate_node(&self, node_id: NodeId) {
        let mut entries = self.entries.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = entries.len();
        entries.retain(|_, v| v.leader_node != node_id);
        let removed = before - entries.len();
        if removed > 0 {
            self.metrics
                .invalidations
                .fetch_add(removed as u64, Ordering::Relaxed);
            self.bump_epoch();
        }
    }

    /// Get a snapshot of the node directory.
    pub fn node_directory_snapshot(&self) -> Vec<NodeInfo> {
        let dir = self.node_directory.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        dir.values().cloned().collect()
    }

    /// Get node address by ID.
    pub fn node_address(&self, node_id: NodeId) -> Option<String> {
        let dir = self.node_directory.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        dir.get(&node_id.0).map(|n| n.address.clone())
    }

    /// Check if a node is alive.
    pub fn is_node_alive(&self, node_id: NodeId) -> bool {
        let dir = self.node_directory.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        dir.get(&node_id.0).is_some_and(|n| n.is_alive)
    }

    /// Metrics snapshot.
    pub fn metrics_snapshot(&self) -> TopologyCacheMetricsSnapshot {
        let entries = self.entries.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = self.node_directory.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        let hits = self.metrics.cache_hits.load(Ordering::Relaxed);
        let misses = self.metrics.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        TopologyCacheMetricsSnapshot {
            epoch: self.epoch(),
            shard_count: entries.len(),
            node_count: dir.len(),
            cache_hits: hits,
            cache_misses: misses,
            epoch_bumps: self.metrics.epoch_bumps.load(Ordering::Relaxed),
            invalidations: self.metrics.invalidations.load(Ordering::Relaxed),
            leader_changes: self.metrics.leader_changes.load(Ordering::Relaxed),
            hit_rate: if total > 0 { hits as f64 / total as f64 } else { 0.0 },
        }
    }
}

impl Default for TopologyCache {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Smart Gateway — Request Classification & Routing
// ═══════════════════════════════════════════════════════════════════════════

/// How the smart gateway classified a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestClassification {
    /// Execute locally — this node is the leader for the target shard.
    LocalExec,
    /// Forward to the shard leader on another node.
    ForwardToLeader,
    /// Reject: no route to target shard (topology unknown or shard offline).
    RejectNoRoute,
    /// Reject: gateway is overloaded.
    RejectOverloaded,
}

impl RequestClassification {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::LocalExec => "LOCAL_EXEC",
            Self::ForwardToLeader => "FORWARD_TO_LEADER",
            Self::RejectNoRoute => "REJECT_NO_ROUTE",
            Self::RejectOverloaded => "REJECT_OVERLOADED",
        }
    }
}

impl std::fmt::Display for RequestClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Result of classifying a request.
#[derive(Debug, Clone)]
pub struct RouteDecision {
    pub classification: RequestClassification,
    /// Target node (for ForwardToLeader).
    pub target_node: Option<NodeId>,
    /// Target address (for ForwardToLeader).
    pub target_addr: Option<String>,
    /// Epoch at decision time.
    pub epoch: u64,
    /// Shard targeted.
    pub shard_id: ShardId,
}

/// Smart Gateway configuration.
#[derive(Debug, Clone)]
pub struct SmartGatewayConfig {
    /// This node's ID.
    pub node_id: NodeId,
    /// This node's role.
    pub role: GatewayRole,
    /// Maximum inflight requests before rejecting.
    pub max_inflight: u64,
    /// Maximum forwarded requests before rejecting.
    pub max_forwarded: u64,
    /// Request timeout (for forward operations).
    pub forward_timeout: Duration,
    /// Topology entry staleness threshold — entries older than this are refreshed.
    pub topology_staleness: Duration,
    /// Maximum queue depth for pending forward requests (0 = no queuing, immediate reject).
    pub max_queue_depth: u64,
}

impl Default for SmartGatewayConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId(0),
            role: GatewayRole::SmartGateway,
            max_inflight: 10_000,
            max_forwarded: 5_000,
            forward_timeout: Duration::from_secs(5),
            topology_staleness: Duration::from_secs(30),
            max_queue_depth: 0, // No queuing by default — immediate reject
        }
    }
}

/// Gateway-level metrics (v1.0.8 — enhanced from v1.0.6).
#[derive(Debug)]
pub struct SmartGatewayMetrics {
    /// Total requests by classification.
    pub local_exec_total: AtomicU64,
    pub forward_total: AtomicU64,
    pub reject_no_route_total: AtomicU64,
    pub reject_overloaded_total: AtomicU64,
    pub reject_timeout_total: AtomicU64,
    /// Current inflight and forwarded counts.
    pub inflight: AtomicU64,
    pub forwarded: AtomicU64,
    /// Cumulative forward latency in microseconds.
    pub forward_latency_us: AtomicU64,
    /// Peak forward latency in microseconds.
    pub forward_latency_peak_us: AtomicU64,
    /// Total forward failures.
    pub forward_failed: AtomicU64,
    /// Client connection metrics.
    pub client_connect_total: AtomicU64,
    pub client_failover_total: AtomicU64,
}

impl Default for SmartGatewayMetrics {
    fn default() -> Self {
        Self {
            local_exec_total: AtomicU64::new(0),
            forward_total: AtomicU64::new(0),
            reject_no_route_total: AtomicU64::new(0),
            reject_overloaded_total: AtomicU64::new(0),
            reject_timeout_total: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
            forwarded: AtomicU64::new(0),
            forward_latency_us: AtomicU64::new(0),
            forward_latency_peak_us: AtomicU64::new(0),
            forward_failed: AtomicU64::new(0),
            client_connect_total: AtomicU64::new(0),
            client_failover_total: AtomicU64::new(0),
        }
    }
}

/// Snapshot of gateway metrics.
#[derive(Debug, Clone)]
pub struct SmartGatewayMetricsSnapshot {
    pub local_exec_total: u64,
    pub forward_total: u64,
    pub reject_no_route_total: u64,
    pub reject_overloaded_total: u64,
    pub reject_timeout_total: u64,
    pub inflight: u64,
    pub forwarded: u64,
    pub forward_latency_avg_us: u64,
    pub forward_latency_peak_us: u64,
    pub forward_failed: u64,
    pub client_connect_total: u64,
    pub client_failover_total: u64,
    pub requests_total: u64,
}

impl SmartGatewayMetrics {
    pub fn snapshot(&self) -> SmartGatewayMetricsSnapshot {
        let local = self.local_exec_total.load(Ordering::Relaxed);
        let forward = self.forward_total.load(Ordering::Relaxed);
        let rej_nr = self.reject_no_route_total.load(Ordering::Relaxed);
        let rej_ol = self.reject_overloaded_total.load(Ordering::Relaxed);
        let rej_to = self.reject_timeout_total.load(Ordering::Relaxed);
        let total = local + forward + rej_nr + rej_ol + rej_to;
        let cum_latency = self.forward_latency_us.load(Ordering::Relaxed);
        SmartGatewayMetricsSnapshot {
            local_exec_total: local,
            forward_total: forward,
            reject_no_route_total: rej_nr,
            reject_overloaded_total: rej_ol,
            reject_timeout_total: rej_to,
            inflight: self.inflight.load(Ordering::Relaxed),
            forwarded: self.forwarded.load(Ordering::Relaxed),
            forward_latency_avg_us: if forward > 0 { cum_latency / forward } else { 0 },
            forward_latency_peak_us: self.forward_latency_peak_us.load(Ordering::Relaxed),
            forward_failed: self.forward_failed.load(Ordering::Relaxed),
            client_connect_total: self.client_connect_total.load(Ordering::Relaxed),
            client_failover_total: self.client_failover_total.load(Ordering::Relaxed),
            requests_total: total,
        }
    }

    fn record_classification(&self, class: RequestClassification) {
        match class {
            RequestClassification::LocalExec => {
                self.local_exec_total.fetch_add(1, Ordering::Relaxed);
            }
            RequestClassification::ForwardToLeader => {
                self.forward_total.fetch_add(1, Ordering::Relaxed);
            }
            RequestClassification::RejectNoRoute => {
                self.reject_no_route_total.fetch_add(1, Ordering::Relaxed);
            }
            RequestClassification::RejectOverloaded => {
                self.reject_overloaded_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_forward_latency(&self, latency_us: u64) {
        self.forward_latency_us.fetch_add(latency_us, Ordering::Relaxed);
        // Best-effort peak tracking (no CAS needed for approximate peak)
        let current_peak = self.forward_latency_peak_us.load(Ordering::Relaxed);
        if latency_us > current_peak {
            self.forward_latency_peak_us.store(latency_us, Ordering::Relaxed);
        }
    }
}

/// The Smart Gateway — unified cluster access point.
pub struct SmartGateway {
    pub config: SmartGatewayConfig,
    pub topology: Arc<TopologyCache>,
    pub metrics: SmartGatewayMetrics,
}

impl SmartGateway {
    pub fn new(config: SmartGatewayConfig) -> Self {
        Self {
            config,
            topology: Arc::new(TopologyCache::new()),
            metrics: SmartGatewayMetrics::default(),
        }
    }

    pub fn with_topology(config: SmartGatewayConfig, topology: Arc<TopologyCache>) -> Self {
        Self {
            config,
            topology,
            metrics: SmartGatewayMetrics::default(),
        }
    }

    /// Classify a request: determine whether to execute locally, forward, or reject.
    /// This is the core routing decision — no blind forwarding.
    pub fn classify_request(&self, shard_id: ShardId) -> RouteDecision {
        // 1. Check admission — overload protection (no infinite queuing)
        let current_inflight = self.metrics.inflight.load(Ordering::Relaxed);
        if self.config.max_inflight > 0 && current_inflight >= self.config.max_inflight {
            self.metrics.record_classification(RequestClassification::RejectOverloaded);
            return RouteDecision {
                classification: RequestClassification::RejectOverloaded,
                target_node: None,
                target_addr: None,
                epoch: self.topology.epoch(),
                shard_id,
            };
        }

        // 2. Look up topology — no blind forward
        let entry = self.topology.get_leader(shard_id);
        match entry {
            None => {
                // No route: topology unknown for this shard
                self.metrics.record_classification(RequestClassification::RejectNoRoute);
                RouteDecision {
                    classification: RequestClassification::RejectNoRoute,
                    target_node: None,
                    target_addr: None,
                    epoch: self.topology.epoch(),
                    shard_id,
                }
            }
            Some(topo) => {
                if topo.leader_node == self.config.node_id {
                    // Local execution
                    self.metrics.record_classification(RequestClassification::LocalExec);
                    RouteDecision {
                        classification: RequestClassification::LocalExec,
                        target_node: Some(topo.leader_node),
                        target_addr: Some(topo.leader_addr),
                        epoch: topo.epoch,
                        shard_id,
                    }
                } else {
                    // Forward to leader — check forwarded slot limit
                    let current_fwd = self.metrics.forwarded.load(Ordering::Relaxed);
                    if self.config.max_forwarded > 0 && current_fwd >= self.config.max_forwarded {
                        self.metrics.record_classification(RequestClassification::RejectOverloaded);
                        return RouteDecision {
                            classification: RequestClassification::RejectOverloaded,
                            target_node: None,
                            target_addr: None,
                            epoch: self.topology.epoch(),
                            shard_id,
                        };
                    }

                    // Verify target node is alive — no blind forward to dead nodes
                    if !self.topology.is_node_alive(topo.leader_node) {
                        self.topology.invalidate(shard_id);
                        self.metrics.record_classification(RequestClassification::RejectNoRoute);
                        return RouteDecision {
                            classification: RequestClassification::RejectNoRoute,
                            target_node: None,
                            target_addr: None,
                            epoch: self.topology.epoch(),
                            shard_id,
                        };
                    }

                    self.metrics.record_classification(RequestClassification::ForwardToLeader);
                    RouteDecision {
                        classification: RequestClassification::ForwardToLeader,
                        target_node: Some(topo.leader_node),
                        target_addr: Some(topo.leader_addr),
                        epoch: topo.epoch,
                        shard_id,
                    }
                }
            }
        }
    }

    /// Record that a forward operation started (call release_forward when done).
    pub fn acquire_forward(&self) -> bool {
        if self.config.max_forwarded > 0 {
            let current = self.metrics.forwarded.load(Ordering::Relaxed);
            if current >= self.config.max_forwarded {
                return false;
            }
        }
        self.metrics.inflight.fetch_add(1, Ordering::Relaxed);
        self.metrics.forwarded.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Record that a forward operation completed.
    pub fn release_forward(&self, latency_us: u64, success: bool) {
        self.metrics.inflight.fetch_sub(1, Ordering::Relaxed);
        self.metrics.forwarded.fetch_sub(1, Ordering::Relaxed);
        self.metrics.record_forward_latency(latency_us);
        if !success {
            self.metrics.forward_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a local execution started/completed.
    pub fn acquire_local(&self) {
        self.metrics.inflight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn release_local(&self) {
        self.metrics.inflight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Handle a NOT_LEADER response: invalidate cache and return error with hints.
    pub fn handle_not_leader(
        &self,
        shard_id: ShardId,
        new_leader: Option<(NodeId, String)>,
    ) -> GatewayError {
        // Invalidate stale cache entry
        self.topology.invalidate(shard_id);

        // If we know the new leader, update cache eagerly
        let leader_hint = if let Some((node_id, addr)) = new_leader {
            self.topology.update_leader(shard_id, node_id, addr.clone());
            Some(addr)
        } else {
            None
        };

        GatewayError::not_leader(shard_id.0, self.topology.epoch(), leader_hint)
    }

    /// Record a client connection event.
    pub fn record_client_connect(&self) {
        self.metrics.client_connect_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a client failover event (switched from one gateway to this one).
    pub fn record_client_failover(&self) {
        self.metrics.client_failover_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Get metrics snapshot.
    pub fn metrics_snapshot(&self) -> SmartGatewayMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Get topology cache metrics snapshot.
    pub fn topology_metrics(&self) -> TopologyCacheMetricsSnapshot {
        self.topology.metrics_snapshot()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// JDBC Client-Side Seed List & Failover
// ═══════════════════════════════════════════════════════════════════════════

/// Client-side gateway seed list manager.
/// Maintains a list of seed gateways and tracks which is currently active.
#[derive(Debug)]
pub struct SeedGatewayList {
    seeds: Vec<HostPort>,
    /// Index of the currently active seed.
    active_index: usize,
    /// Number of consecutive failures on the active seed.
    consecutive_failures: u32,
    /// Maximum failures before switching to next seed.
    max_failures_before_switch: u32,
}

impl SeedGatewayList {
    pub fn new(seeds: Vec<HostPort>) -> Self {
        assert!(!seeds.is_empty(), "At least one seed gateway required");
        Self {
            seeds,
            active_index: 0,
            consecutive_failures: 0,
            max_failures_before_switch: 3,
        }
    }

    pub fn from_url(url: &JdbcConnectionUrl) -> Self {
        Self::new(url.seeds.clone())
    }

    /// Get the currently active gateway address.
    pub fn active(&self) -> &HostPort {
        &self.seeds[self.active_index]
    }

    /// Report a successful operation — resets failure counter.
    pub const fn report_success(&mut self) {
        self.consecutive_failures = 0;
    }

    /// Report a failure. Returns true if failover occurred.
    pub const fn report_failure(&mut self) -> bool {
        self.consecutive_failures += 1;
        if self.consecutive_failures >= self.max_failures_before_switch {
            self.failover();
            true
        } else {
            false
        }
    }

    /// Switch to the next gateway in the seed list.
    pub const fn failover(&mut self) {
        self.active_index = (self.active_index + 1) % self.seeds.len();
        self.consecutive_failures = 0;
    }

    /// Number of seed gateways.
    pub const fn seed_count(&self) -> usize {
        self.seeds.len()
    }

    /// Whether we've cycled through all seeds without success.
    pub const fn all_seeds_exhausted(&self, total_failures: u32) -> bool {
        total_failures >= (self.seeds.len() as u32 * self.max_failures_before_switch)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Compression Policy Profiles (v1.0.8 — product-level)
// ═══════════════════════════════════════════════════════════════════════════

/// Compression profile — user-facing configuration.
/// Users don't need to understand cold/hot details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionProfile {
    /// No compression — all data stays in hot memory.
    Off,
    /// Default: moderate cold migration, LZ4, standard cache.
    Balanced,
    /// Aggressive: lower migration threshold, LZ4, larger cache.
    Aggressive,
}

impl CompressionProfile {
    pub fn from_str_loose(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "off" | "none" | "disabled" => Self::Off,
            "aggressive" | "high" => Self::Aggressive,
            _ => Self::Balanced,
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Balanced => "balanced",
            Self::Aggressive => "aggressive",
        }
    }

    /// Cold migration min_version_age (timestamp units).
    pub const fn min_version_age(&self) -> u64 {
        match self {
            Self::Off => u64::MAX, // Never migrate
            Self::Balanced => 300, // ~5 minutes
            Self::Aggressive => 60, // ~1 minute
        }
    }

    /// Compression enabled?
    pub const fn compression_enabled(&self) -> bool {
        !matches!(self, Self::Off)
    }

    /// Compression codec.
    pub const fn codec(&self) -> &'static str {
        match self {
            Self::Off => "none",
            Self::Balanced | Self::Aggressive => "lz4",
        }
    }

    /// Block cache capacity in bytes.
    pub const fn block_cache_capacity(&self) -> u64 {
        match self {
            Self::Off => 0,
            Self::Balanced => 16 * 1024 * 1024,      // 16 MB
            Self::Aggressive => 64 * 1024 * 1024,     // 64 MB
        }
    }

    /// Compactor batch size.
    pub const fn compactor_batch_size(&self) -> usize {
        match self {
            Self::Off => 0,
            Self::Balanced => 1000,
            Self::Aggressive => 5000,
        }
    }

    /// Compactor interval in milliseconds.
    pub const fn compactor_interval_ms(&self) -> u64 {
        match self {
            Self::Off => 0,
            Self::Balanced => 5000,
            Self::Aggressive => 1000,
        }
    }
}

impl std::fmt::Display for CompressionProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// WAL Backend Policy (v1.0.8 — product-level)
// ═══════════════════════════════════════════════════════════════════════════

/// WAL backend mode — user-facing selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WalMode {
    /// Auto-detect: use doctor to recommend.
    Auto,
    /// Standard POSIX file I/O.
    Posix,
    /// Windows IOCP async file with optional NO_BUFFERING.
    WinAsync,
    /// Raw disk — experimental, requires admin privileges.
    RawExperimental,
}

impl WalMode {
    pub fn from_str_loose(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "posix" | "file" => Self::Posix,
            "win_async" | "win_async_file" => Self::WinAsync,
            "raw" | "raw_experimental" => Self::RawExperimental,
            _ => Self::Auto,
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Posix => "posix",
            Self::WinAsync => "win_async",
            Self::RawExperimental => "raw_experimental",
        }
    }

    /// Whether this mode is production-ready.
    pub const fn is_stable(&self) -> bool {
        matches!(self, Self::Auto | Self::Posix | Self::WinAsync)
    }

    /// Recommended sync mode for this backend.
    pub const fn recommended_sync_mode(&self) -> &'static str {
        match self {
            Self::Auto | Self::Posix => "fdatasync",
            Self::WinAsync | Self::RawExperimental => "fsync",
        }
    }

    /// Risk level description.
    pub const fn risk_description(&self) -> &'static str {
        match self {
            Self::Auto => "Low — system selects the safest available backend",
            Self::Posix => "Low — standard file I/O, works everywhere",
            Self::WinAsync => "Low-Medium — requires Windows + NTFS for NO_BUFFERING",
            Self::RawExperimental => "HIGH — data loss on misconfiguration, requires admin",
        }
    }

    /// Auto-detect the best WAL mode for the current platform.
    pub const fn auto_detect() -> Self {
        if cfg!(windows) {
            Self::WinAsync
        } else {
            Self::Posix
        }
    }
}

impl std::fmt::Display for WalMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── JDBC URL parsing ────────────────────────────────────────────

    #[test]
    fn test_jdbc_url_single_host() {
        let url = JdbcConnectionUrl::parse("jdbc:falcondb://localhost:5443/mydb").unwrap();
        assert_eq!(url.seeds.len(), 1);
        assert_eq!(url.seeds[0].host, "localhost");
        assert_eq!(url.seeds[0].port, 5443);
        assert_eq!(url.database, "mydb");
    }

    #[test]
    fn test_jdbc_url_multi_host() {
        let url = JdbcConnectionUrl::parse(
            "jdbc:falcondb://node1:5443,node2:5443,node3:5443/falcon",
        )
        .unwrap();
        assert_eq!(url.seeds.len(), 3);
        assert_eq!(url.seeds[0].host, "node1");
        assert_eq!(url.seeds[1].host, "node2");
        assert_eq!(url.seeds[2].host, "node3");
        assert_eq!(url.database, "falcon");
    }

    #[test]
    fn test_jdbc_url_with_params() {
        let url = JdbcConnectionUrl::parse(
            "jdbc:falcondb://host1:5443/db?user=admin&password=secret",
        )
        .unwrap();
        assert_eq!(url.seeds.len(), 1);
        assert_eq!(url.database, "db");
        assert_eq!(url.params.get("user"), Some(&"admin".to_string()));
        assert_eq!(url.params.get("password"), Some(&"secret".to_string()));
    }

    #[test]
    fn test_jdbc_url_default_port() {
        let url = JdbcConnectionUrl::parse("jdbc:falcondb://myhost/falcon").unwrap();
        assert_eq!(url.seeds[0].host, "myhost");
        assert_eq!(url.seeds[0].port, 5443);
    }

    #[test]
    fn test_jdbc_url_default_database() {
        let url = JdbcConnectionUrl::parse("jdbc:falcondb://host1:5443").unwrap();
        assert_eq!(url.database, "falcon");
    }

    #[test]
    fn test_jdbc_url_roundtrip() {
        let original = "jdbc:falcondb://node1:5443,node2:5443/mydb";
        let parsed = JdbcConnectionUrl::parse(original).unwrap();
        let formatted = parsed.to_url();
        assert!(formatted.starts_with("jdbc:falcondb://"));
        assert!(formatted.contains("node1:5443"));
        assert!(formatted.contains("node2:5443"));
        assert!(formatted.contains("/mydb"));
    }

    #[test]
    fn test_jdbc_url_invalid_prefix() {
        assert!(JdbcConnectionUrl::parse("postgres://localhost/db").is_err());
    }

    #[test]
    fn test_jdbc_url_empty_hosts() {
        assert!(JdbcConnectionUrl::parse("jdbc:falcondb:///db").is_err());
    }

    // ── Gateway Error Codes ─────────────────────────────────────────

    #[test]
    fn test_error_code_retry_semantics() {
        assert!(GatewayErrorCode::NotLeader.is_retryable());
        assert!(GatewayErrorCode::NoRoute.is_retryable());
        assert!(GatewayErrorCode::Overloaded.is_retryable());
        assert!(GatewayErrorCode::Timeout.is_retryable());
        assert!(!GatewayErrorCode::Fatal.is_retryable());

        assert_eq!(GatewayErrorCode::NotLeader.retry_delay_ms(), Some(0));
        assert_eq!(GatewayErrorCode::NoRoute.retry_delay_ms(), Some(100));
        assert_eq!(GatewayErrorCode::Overloaded.retry_delay_ms(), Some(500));
        assert_eq!(GatewayErrorCode::Fatal.retry_delay_ms(), None);
    }

    #[test]
    fn test_error_code_sqlstate() {
        assert_eq!(GatewayErrorCode::NotLeader.sqlstate(), "FD001");
        assert_eq!(GatewayErrorCode::NoRoute.sqlstate(), "FD002");
        assert_eq!(GatewayErrorCode::Fatal.sqlstate(), "FD000");
    }

    #[test]
    fn test_gateway_error_constructors() {
        let e = GatewayError::not_leader(1, 5, Some("node2:5443".into()));
        assert_eq!(e.code, GatewayErrorCode::NotLeader);
        assert_eq!(e.shard_id, 1);
        assert_eq!(e.epoch, 5);
        assert_eq!(e.leader_hint, Some("node2:5443".to_string()));
        assert_eq!(e.retry_after_ms, Some(0));

        let e = GatewayError::overloaded("too many inflight");
        assert_eq!(e.code, GatewayErrorCode::Overloaded);
        assert!(e.retry_after_ms.unwrap() > 0);

        let e = GatewayError::fatal("bad SQL".to_string());
        assert_eq!(e.code, GatewayErrorCode::Fatal);
        assert_eq!(e.retry_after_ms, None);
    }

    // ── Topology Cache ──────────────────────────────────────────────

    #[test]
    fn test_topology_cache_basic() {
        let cache = TopologyCache::new();
        assert_eq!(cache.epoch(), 1);

        // Register node
        cache.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
        assert!(cache.is_node_alive(NodeId(1)));

        // Update leader
        let epoch = cache.update_leader(ShardId(0), NodeId(1), "node1:5443".into());
        assert!(epoch >= 2); // Epoch bumped

        // Lookup
        let entry = cache.get_leader(ShardId(0)).unwrap();
        assert_eq!(entry.leader_node, NodeId(1));
        assert_eq!(entry.leader_addr, "node1:5443");

        // Miss
        assert!(cache.get_leader(ShardId(99)).is_none());
    }

    #[test]
    fn test_topology_cache_leader_change() {
        let cache = TopologyCache::new();
        cache.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
        cache.register_node(NodeId(2), "node2:5443".into(), GatewayRole::SmartGateway);

        let e1 = cache.update_leader(ShardId(0), NodeId(1), "node1:5443".into());
        let e2 = cache.update_leader(ShardId(0), NodeId(2), "node2:5443".into());

        assert!(e2 > e1, "epoch should increase on leader change");

        let entry = cache.get_leader(ShardId(0)).unwrap();
        assert_eq!(entry.leader_node, NodeId(2));

        let snap = cache.metrics_snapshot();
        assert_eq!(snap.leader_changes, 2); // Both updates are "changes" (initial + change)
    }

    #[test]
    fn test_topology_cache_invalidation() {
        let cache = TopologyCache::new();
        cache.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
        cache.update_leader(ShardId(0), NodeId(1), "node1:5443".into());

        assert!(cache.get_leader(ShardId(0)).is_some());
        cache.invalidate(ShardId(0));
        assert!(cache.get_leader(ShardId(0)).is_none());

        let snap = cache.metrics_snapshot();
        assert!(snap.invalidations >= 1);
    }

    #[test]
    fn test_topology_cache_invalidate_node() {
        let cache = TopologyCache::new();
        cache.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
        cache.update_leader(ShardId(0), NodeId(1), "node1:5443".into());
        cache.update_leader(ShardId(1), NodeId(1), "node1:5443".into());

        cache.invalidate_node(NodeId(1));
        assert!(cache.get_leader(ShardId(0)).is_none());
        assert!(cache.get_leader(ShardId(1)).is_none());
    }

    #[test]
    fn test_topology_cache_same_leader_no_epoch_bump() {
        let cache = TopologyCache::new();
        cache.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);

        let e1 = cache.update_leader(ShardId(0), NodeId(1), "node1:5443".into());
        let e2 = cache.update_leader(ShardId(0), NodeId(1), "node1:5443".into());

        // Same leader → no epoch bump
        assert_eq!(e1, e2);
    }

    #[test]
    fn test_topology_cache_metrics() {
        let cache = TopologyCache::new();
        cache.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
        cache.update_leader(ShardId(0), NodeId(1), "node1:5443".into());

        // Hits
        cache.get_leader(ShardId(0));
        cache.get_leader(ShardId(0));
        // Misses
        cache.get_leader(ShardId(99));

        let snap = cache.metrics_snapshot();
        assert_eq!(snap.cache_hits, 2);
        assert_eq!(snap.cache_misses, 1);
        assert!((snap.hit_rate - 2.0 / 3.0).abs() < 0.01);
    }

    // ── Smart Gateway Routing ───────────────────────────────────────

    #[test]
    fn test_smart_gateway_local_exec() {
        let config = SmartGatewayConfig {
            node_id: NodeId(1),
            ..Default::default()
        };
        let gw = SmartGateway::new(config);
        gw.topology.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
        gw.topology.update_leader(ShardId(0), NodeId(1), "node1:5443".into());

        let decision = gw.classify_request(ShardId(0));
        assert_eq!(decision.classification, RequestClassification::LocalExec);
        assert_eq!(decision.target_node, Some(NodeId(1)));
    }

    #[test]
    fn test_smart_gateway_forward_to_leader() {
        let config = SmartGatewayConfig {
            node_id: NodeId(1),
            ..Default::default()
        };
        let gw = SmartGateway::new(config);
        gw.topology.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
        gw.topology.register_node(NodeId(2), "node2:5443".into(), GatewayRole::SmartGateway);
        gw.topology.update_leader(ShardId(0), NodeId(2), "node2:5443".into());

        let decision = gw.classify_request(ShardId(0));
        assert_eq!(decision.classification, RequestClassification::ForwardToLeader);
        assert_eq!(decision.target_node, Some(NodeId(2)));
        assert_eq!(decision.target_addr, Some("node2:5443".to_string()));
    }

    #[test]
    fn test_smart_gateway_reject_no_route() {
        let config = SmartGatewayConfig {
            node_id: NodeId(1),
            ..Default::default()
        };
        let gw = SmartGateway::new(config);

        let decision = gw.classify_request(ShardId(99));
        assert_eq!(decision.classification, RequestClassification::RejectNoRoute);
    }

    #[test]
    fn test_smart_gateway_reject_overloaded() {
        let config = SmartGatewayConfig {
            node_id: NodeId(1),
            max_inflight: 2,
            ..Default::default()
        };
        let gw = SmartGateway::new(config);
        gw.topology.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
        gw.topology.update_leader(ShardId(0), NodeId(1), "node1:5443".into());

        // Fill up inflight slots
        gw.metrics.inflight.store(2, Ordering::Relaxed);

        let decision = gw.classify_request(ShardId(0));
        assert_eq!(decision.classification, RequestClassification::RejectOverloaded);
    }

    #[test]
    fn test_smart_gateway_no_blind_forward_to_dead_node() {
        let config = SmartGatewayConfig {
            node_id: NodeId(1),
            ..Default::default()
        };
        let gw = SmartGateway::new(config);
        gw.topology.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
        gw.topology.register_node(NodeId(2), "node2:5443".into(), GatewayRole::SmartGateway);
        gw.topology.update_leader(ShardId(0), NodeId(2), "node2:5443".into());

        // Mark node 2 as dead
        gw.topology.mark_node_dead(NodeId(2));

        let decision = gw.classify_request(ShardId(0));
        assert_eq!(
            decision.classification,
            RequestClassification::RejectNoRoute,
            "should not blind-forward to dead node"
        );
    }

    #[test]
    fn test_smart_gateway_handle_not_leader() {
        let config = SmartGatewayConfig {
            node_id: NodeId(1),
            ..Default::default()
        };
        let gw = SmartGateway::new(config);
        gw.topology.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
        gw.topology.register_node(NodeId(2), "node2:5443".into(), GatewayRole::SmartGateway);
        gw.topology.update_leader(ShardId(0), NodeId(1), "node1:5443".into());

        // Simulate NOT_LEADER response — leader is now node 2
        let err = gw.handle_not_leader(
            ShardId(0),
            Some((NodeId(2), "node2:5443".into())),
        );

        assert_eq!(err.code, GatewayErrorCode::NotLeader);
        assert_eq!(err.leader_hint, Some("node2:5443".to_string()));

        // Verify cache was updated
        let entry = gw.topology.get_leader(ShardId(0)).unwrap();
        assert_eq!(entry.leader_node, NodeId(2));
    }

    #[test]
    fn test_smart_gateway_metrics() {
        let config = SmartGatewayConfig {
            node_id: NodeId(1),
            ..Default::default()
        };
        let gw = SmartGateway::new(config);
        gw.topology.register_node(NodeId(1), "node1:5443".into(), GatewayRole::SmartGateway);
        gw.topology.register_node(NodeId(2), "node2:5443".into(), GatewayRole::SmartGateway);
        gw.topology.update_leader(ShardId(0), NodeId(1), "node1:5443".into());
        gw.topology.update_leader(ShardId(1), NodeId(2), "node2:5443".into());

        gw.classify_request(ShardId(0)); // LocalExec
        gw.classify_request(ShardId(1)); // ForwardToLeader
        gw.classify_request(ShardId(99)); // RejectNoRoute

        let snap = gw.metrics_snapshot();
        assert_eq!(snap.local_exec_total, 1);
        assert_eq!(snap.forward_total, 1);
        assert_eq!(snap.reject_no_route_total, 1);
        assert_eq!(snap.requests_total, 3);
    }

    // ── Seed Gateway List ───────────────────────────────────────────

    #[test]
    fn test_seed_list_failover() {
        let seeds = vec![
            HostPort { host: "node1".into(), port: 5443 },
            HostPort { host: "node2".into(), port: 5443 },
            HostPort { host: "node3".into(), port: 5443 },
        ];
        let mut list = SeedGatewayList::new(seeds);

        assert_eq!(list.active().host, "node1");

        // 3 failures → failover to node2
        list.report_failure();
        list.report_failure();
        assert!(list.report_failure()); // Returns true on failover
        assert_eq!(list.active().host, "node2");

        // Success resets counter
        list.report_success();
        assert_eq!(list.active().host, "node2");
    }

    #[test]
    fn test_seed_list_wrap_around() {
        let seeds = vec![
            HostPort { host: "node1".into(), port: 5443 },
            HostPort { host: "node2".into(), port: 5443 },
        ];
        let mut list = SeedGatewayList::new(seeds);

        // Fail node1 → node2
        for _ in 0..3 { list.report_failure(); }
        assert_eq!(list.active().host, "node2");

        // Fail node2 → wraps back to node1
        for _ in 0..3 { list.report_failure(); }
        assert_eq!(list.active().host, "node1");
    }

    #[test]
    fn test_seed_list_all_exhausted() {
        let seeds = vec![
            HostPort { host: "a".into(), port: 1 },
            HostPort { host: "b".into(), port: 2 },
        ];
        let list = SeedGatewayList::new(seeds);
        assert!(!list.all_seeds_exhausted(5));
        assert!(list.all_seeds_exhausted(6)); // 2 seeds × 3 max_failures = 6
    }

    // ── Gateway Role ────────────────────────────────────────────────

    #[test]
    fn test_gateway_role_capabilities() {
        assert!(GatewayRole::DedicatedGateway.accepts_client_connections());
        assert!(!GatewayRole::DedicatedGateway.owns_shards());

        assert!(GatewayRole::SmartGateway.accepts_client_connections());
        assert!(GatewayRole::SmartGateway.owns_shards());

        assert!(!GatewayRole::ComputeOnly.accepts_client_connections());
        assert!(GatewayRole::ComputeOnly.owns_shards());
    }

    // ── Cluster Topology ────────────────────────────────────────────

    #[test]
    fn test_cluster_topology_recommendation() {
        assert_eq!(ClusterTopology::recommend(1), ClusterTopology::Small);
        assert_eq!(ClusterTopology::recommend(3), ClusterTopology::Small);
        assert_eq!(ClusterTopology::recommend(5), ClusterTopology::Medium);
        assert_eq!(ClusterTopology::recommend(10), ClusterTopology::Large);
    }

    // ── Compression Profile ─────────────────────────────────────────

    #[test]
    fn test_compression_profiles() {
        let off = CompressionProfile::Off;
        assert!(!off.compression_enabled());
        assert_eq!(off.min_version_age(), u64::MAX);
        assert_eq!(off.block_cache_capacity(), 0);

        let balanced = CompressionProfile::Balanced;
        assert!(balanced.compression_enabled());
        assert_eq!(balanced.min_version_age(), 300);
        assert_eq!(balanced.codec(), "lz4");
        assert_eq!(balanced.block_cache_capacity(), 16 * 1024 * 1024);

        let aggressive = CompressionProfile::Aggressive;
        assert!(aggressive.compression_enabled());
        assert_eq!(aggressive.min_version_age(), 60);
        assert_eq!(aggressive.block_cache_capacity(), 64 * 1024 * 1024);
    }

    #[test]
    fn test_compression_profile_from_str() {
        assert_eq!(CompressionProfile::from_str_loose("off"), CompressionProfile::Off);
        assert_eq!(CompressionProfile::from_str_loose("none"), CompressionProfile::Off);
        assert_eq!(CompressionProfile::from_str_loose("balanced"), CompressionProfile::Balanced);
        assert_eq!(CompressionProfile::from_str_loose("aggressive"), CompressionProfile::Aggressive);
        assert_eq!(CompressionProfile::from_str_loose("unknown"), CompressionProfile::Balanced);
    }

    // ── WAL Backend Policy ──────────────────────────────────────────

    #[test]
    fn test_wal_mode_properties() {
        assert!(WalMode::Posix.is_stable());
        assert!(WalMode::WinAsync.is_stable());
        assert!(!WalMode::RawExperimental.is_stable());
    }

    #[test]
    fn test_wal_mode_from_str() {
        assert_eq!(WalMode::from_str_loose("auto"), WalMode::Auto);
        assert_eq!(WalMode::from_str_loose("posix"), WalMode::Posix);
        assert_eq!(WalMode::from_str_loose("file"), WalMode::Posix);
        assert_eq!(WalMode::from_str_loose("win_async"), WalMode::WinAsync);
        assert_eq!(WalMode::from_str_loose("raw_experimental"), WalMode::RawExperimental);
    }

    #[test]
    fn test_wal_mode_auto_detect() {
        let mode = WalMode::auto_detect();
        if cfg!(windows) {
            assert_eq!(mode, WalMode::WinAsync);
        } else {
            assert_eq!(mode, WalMode::Posix);
        }
    }

    // ── Concurrent topology cache ───────────────────────────────────

    #[test]
    fn test_topology_cache_concurrent() {
        let cache = Arc::new(TopologyCache::new());
        for i in 0..4u64 {
            cache.register_node(
                NodeId(i),
                format!("node{}:5443", i),
                GatewayRole::SmartGateway,
            );
        }

        let mut handles = Vec::new();
        for t in 0..8 {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let shard = ShardId((t * 100 + i) % 16);
                    let leader = NodeId(((t + i) % 4) as u64);
                    c.update_leader(shard, leader, format!("node{}:5443", leader.0));
                    c.get_leader(shard);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let snap = cache.metrics_snapshot();
        assert!(snap.cache_hits + snap.cache_misses > 0);
        assert!(snap.epoch > 1);
    }

    // ── Smart Gateway concurrent ────────────────────────────────────

    #[test]
    fn test_smart_gateway_concurrent_classify() {
        let config = SmartGatewayConfig {
            node_id: NodeId(0),
            max_inflight: 10_000,
            ..Default::default()
        };
        let gw = Arc::new(SmartGateway::new(config));

        // Setup: 4 nodes, 8 shards
        for i in 0..4u64 {
            gw.topology.register_node(
                NodeId(i),
                format!("node{}:5443", i),
                GatewayRole::SmartGateway,
            );
        }
        for s in 0..8u64 {
            gw.topology.update_leader(
                ShardId(s),
                NodeId(s % 4),
                format!("node{}:5443", s % 4),
            );
        }

        let mut handles = Vec::new();
        for t in 0..8 {
            let g = Arc::clone(&gw);
            handles.push(std::thread::spawn(move || {
                for i in 0..200 {
                    let shard = ShardId(((t * 200 + i) % 8) as u64);
                    let _ = g.classify_request(shard);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let snap = gw.metrics_snapshot();
        assert_eq!(snap.requests_total, 1600);
        // Node 0 is our gateway — shards 0 and 4 are local
        assert!(snap.local_exec_total > 0);
        assert!(snap.forward_total > 0);
    }
}
