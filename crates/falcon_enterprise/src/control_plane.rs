//! v1.1.0 §1–2: Enterprise Control Plane & Consistent Metadata Store.
//!
//! Separates "control logic" from "data plane" so that data nodes do not
//! carry operational complexity. The controller manages:
//! - Cluster topology (node registry, heartbeat, liveness)
//! - Configuration distribution (dynamic, versioned)
//! - Version/capability negotiation
//! - Ops command dispatch (drain / upgrade / rebalance)
//!
//! Metadata is stored in a Raft-backed consistent store so that:
//! - Cluster config never drifts
//! - Shard placement is authoritative
//! - Security policies are globally consistent
//!
//! **Invariant**: Controller restart ≠ data-plane interruption.
//! Data nodes cache config and continue serving reads/writes when
//! the controller is temporarily unavailable.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use parking_lot::{Mutex, RwLock};

use falcon_common::types::NodeId;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Controller Identity & HA
// ═══════════════════════════════════════════════════════════════════════════

/// Controller node role in the HA group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerRole {
    Leader,
    Follower,
    Candidate,
}

impl fmt::Display for ControllerRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leader => write!(f, "LEADER"),
            Self::Follower => write!(f, "FOLLOWER"),
            Self::Candidate => write!(f, "CANDIDATE"),
        }
    }
}

/// Identity of a controller node within the HA group.
#[derive(Debug, Clone)]
pub struct ControllerNode {
    pub id: u64,
    pub address: String,
    pub role: ControllerRole,
    pub last_heartbeat: Option<Instant>,
    pub term: u64,
}

/// Controller HA group — manages leader election among 3+ controllers.
pub struct ControllerHAGroup {
    pub nodes: RwLock<Vec<ControllerNode>>,
    pub self_id: u64,
    pub current_term: AtomicU64,
    pub leader_id: RwLock<Option<u64>>,
    pub metrics: ControllerHAMetrics,
}

#[derive(Debug, Default)]
pub struct ControllerHAMetrics {
    pub elections_held: AtomicU64,
    pub leader_changes: AtomicU64,
    pub heartbeats_sent: AtomicU64,
    pub heartbeats_received: AtomicU64,
}

impl ControllerHAGroup {
    pub fn new(self_id: u64, peers: Vec<(u64, String)>) -> Self {
        let mut nodes = Vec::new();
        for (id, addr) in peers {
            nodes.push(ControllerNode {
                id,
                address: addr,
                role: ControllerRole::Follower,
                last_heartbeat: None,
                term: 0,
            });
        }
        Self {
            nodes: RwLock::new(nodes),
            self_id,
            current_term: AtomicU64::new(1),
            leader_id: RwLock::new(None),
            metrics: ControllerHAMetrics::default(),
        }
    }

    /// Elect self as leader (simplified — in production uses Raft).
    pub fn elect_self(&self) -> u64 {
        let term = self.current_term.fetch_add(1, Ordering::SeqCst) + 1;
        *self.leader_id.write() = Some(self.self_id);
        let mut nodes = self.nodes.write();
        for n in nodes.iter_mut() {
            if n.id == self.self_id {
                n.role = ControllerRole::Leader;
                n.term = term;
            } else {
                n.role = ControllerRole::Follower;
            }
        }
        self.metrics.elections_held.fetch_add(1, Ordering::Relaxed);
        self.metrics.leader_changes.fetch_add(1, Ordering::Relaxed);
        term
    }

    /// Set leader to a specific node.
    pub fn set_leader(&self, leader_id: u64, term: u64) {
        self.current_term.store(term, Ordering::SeqCst);
        *self.leader_id.write() = Some(leader_id);
        let mut nodes = self.nodes.write();
        for n in nodes.iter_mut() {
            n.role = if n.id == leader_id {
                ControllerRole::Leader
            } else {
                ControllerRole::Follower
            };
            n.term = term;
        }
        self.metrics.leader_changes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn is_leader(&self) -> bool {
        *self.leader_id.read() == Some(self.self_id)
    }

    pub fn leader(&self) -> Option<u64> {
        *self.leader_id.read()
    }

    pub fn term(&self) -> u64 {
        self.current_term.load(Ordering::SeqCst)
    }

    pub fn record_heartbeat(&self, from_id: u64) {
        self.metrics.heartbeats_received.fetch_add(1, Ordering::Relaxed);
        let mut nodes = self.nodes.write();
        if let Some(n) = nodes.iter_mut().find(|n| n.id == from_id) {
            n.last_heartbeat = Some(Instant::now());
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Data-Plane Node Registry
// ═══════════════════════════════════════════════════════════════════════════

/// Data-plane node state as seen by the controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataNodeState {
    Online,
    Suspect,
    Offline,
    Draining,
    Joining,
    Upgrading,
}

impl fmt::Display for DataNodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Online => write!(f, "ONLINE"),
            Self::Suspect => write!(f, "SUSPECT"),
            Self::Offline => write!(f, "OFFLINE"),
            Self::Draining => write!(f, "DRAINING"),
            Self::Joining => write!(f, "JOINING"),
            Self::Upgrading => write!(f, "UPGRADING"),
        }
    }
}

/// Capabilities a data node advertises on registration.
#[derive(Debug, Clone)]
pub struct NodeCapabilities {
    pub version: String,
    pub protocol_version: u32,
    pub max_shards: u32,
    pub memory_bytes: u64,
    pub cpu_cores: u32,
    pub storage_bytes: u64,
    pub roles: Vec<String>,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            version: "1.1.0".into(),
            protocol_version: 2,
            max_shards: 64,
            memory_bytes: 0,
            cpu_cores: 0,
            storage_bytes: 0,
            roles: vec!["data".into()],
        }
    }
}

/// Data-plane node record in the controller's registry.
#[derive(Debug, Clone)]
pub struct DataNodeRecord {
    pub node_id: NodeId,
    pub address: String,
    pub state: DataNodeState,
    pub capabilities: NodeCapabilities,
    pub registered_at: u64,
    pub last_heartbeat: Option<Instant>,
    pub assigned_shards: Vec<u64>,
    pub config_version: u64,
}

/// Node registry — the controller's view of all data-plane nodes.
pub struct NodeRegistry {
    nodes: RwLock<HashMap<NodeId, DataNodeRecord>>,
    suspect_threshold: Duration,
    offline_threshold: Duration,
    pub metrics: NodeRegistryMetrics,
}

#[derive(Debug, Default)]
pub struct NodeRegistryMetrics {
    pub registrations: AtomicU64,
    pub deregistrations: AtomicU64,
    pub heartbeats: AtomicU64,
    pub state_transitions: AtomicU64,
}

impl NodeRegistry {
    pub fn new(suspect_threshold: Duration, offline_threshold: Duration) -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            suspect_threshold,
            offline_threshold,
            metrics: NodeRegistryMetrics::default(),
        }
    }

    /// Register a data-plane node. Returns true if new, false if already registered.
    pub fn register(
        &self,
        node_id: NodeId,
        address: String,
        capabilities: NodeCapabilities,
    ) -> bool {
        let now_ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut nodes = self.nodes.write();
        let is_new = !nodes.contains_key(&node_id);
        nodes.insert(node_id, DataNodeRecord {
            node_id,
            address,
            state: DataNodeState::Online,
            capabilities,
            registered_at: now_ts,
            last_heartbeat: Some(Instant::now()),
            assigned_shards: Vec::new(),
            config_version: 0,
        });
        if is_new {
            self.metrics.registrations.fetch_add(1, Ordering::Relaxed);
        }
        is_new
    }

    /// Deregister a node.
    pub fn deregister(&self, node_id: NodeId) -> bool {
        let removed = self.nodes.write().remove(&node_id).is_some();
        if removed {
            self.metrics.deregistrations.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    /// Record heartbeat from a data node.
    pub fn heartbeat(&self, node_id: NodeId, config_version: u64) -> bool {
        self.metrics.heartbeats.fetch_add(1, Ordering::Relaxed);
        let mut nodes = self.nodes.write();
        if let Some(record) = nodes.get_mut(&node_id) {
            record.last_heartbeat = Some(Instant::now());
            record.config_version = config_version;
            if record.state == DataNodeState::Suspect {
                record.state = DataNodeState::Online;
                self.metrics.state_transitions.fetch_add(1, Ordering::Relaxed);
            }
            true
        } else {
            false
        }
    }

    /// Evaluate all node states based on heartbeat freshness.
    pub fn evaluate(&self) -> Vec<(NodeId, DataNodeState, DataNodeState)> {
        let now = Instant::now();
        let mut transitions = Vec::new();
        let mut nodes = self.nodes.write();
        for record in nodes.values_mut() {
            if record.state == DataNodeState::Draining
                || record.state == DataNodeState::Joining
                || record.state == DataNodeState::Upgrading
            {
                continue;
            }
            let old = record.state;
            if let Some(last) = record.last_heartbeat {
                let elapsed = now.duration_since(last);
                if elapsed > self.offline_threshold {
                    record.state = DataNodeState::Offline;
                } else if elapsed > self.suspect_threshold {
                    record.state = DataNodeState::Suspect;
                } else {
                    record.state = DataNodeState::Online;
                }
            } else {
                record.state = DataNodeState::Suspect;
            }
            if record.state != old {
                self.metrics.state_transitions.fetch_add(1, Ordering::Relaxed);
                transitions.push((record.node_id, old, record.state));
            }
        }
        transitions
    }

    /// Set node state explicitly (for drain/join/upgrade operations).
    pub fn set_state(&self, node_id: NodeId, state: DataNodeState) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(record) = nodes.get_mut(&node_id) {
            record.state = state;
            self.metrics.state_transitions.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Assign shards to a node.
    pub fn assign_shards(&self, node_id: NodeId, shards: Vec<u64>) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(record) = nodes.get_mut(&node_id) {
            record.assigned_shards = shards;
            true
        } else {
            false
        }
    }

    /// Get a node record.
    pub fn get(&self, node_id: NodeId) -> Option<DataNodeRecord> {
        self.nodes.read().get(&node_id).cloned()
    }

    /// Get all nodes.
    pub fn all_nodes(&self) -> Vec<DataNodeRecord> {
        self.nodes.read().values().cloned().collect()
    }

    /// Count nodes by state.
    pub fn count_by_state(&self) -> HashMap<DataNodeState, usize> {
        let nodes = self.nodes.read();
        let mut counts: HashMap<DataNodeState, usize> = HashMap::new();
        for r in nodes.values() {
            *counts.entry(r.state).or_insert(0) += 1;
        }
        counts
    }

    /// Get online node IDs.
    pub fn online_nodes(&self) -> Vec<NodeId> {
        self.nodes.read().values()
            .filter(|r| r.state == DataNodeState::Online)
            .map(|r| r.node_id)
            .collect()
    }

    /// Total node count.
    pub fn node_count(&self) -> usize {
        self.nodes.read().len()
    }

    /// Nodes needing config update (config_version < target).
    pub fn nodes_needing_config(&self, target_version: u64) -> Vec<NodeId> {
        self.nodes.read().values()
            .filter(|r| r.config_version < target_version && r.state == DataNodeState::Online)
            .map(|r| r.node_id)
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Versioned Dynamic Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// A versioned configuration entry.
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    pub key: String,
    pub value: String,
    pub version: u64,
    pub updated_by: String,
    pub updated_at: u64,
}

/// Dynamic configuration store — versioned, distributable.
pub struct ConfigStore {
    entries: RwLock<BTreeMap<String, ConfigEntry>>,
    version: AtomicU64,
    history: Mutex<VecDeque<ConfigEntry>>,
    history_capacity: usize,
}

impl Default for ConfigStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigStore {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
            version: AtomicU64::new(1),
            history: Mutex::new(VecDeque::with_capacity(1000)),
            history_capacity: 1000,
        }
    }

    /// Set a configuration key. Returns the new version.
    pub fn set(&self, key: &str, value: &str, updated_by: &str) -> u64 {
        let version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entry = ConfigEntry {
            key: key.to_owned(),
            value: value.to_owned(),
            version,
            updated_by: updated_by.to_owned(),
            updated_at: now,
        };
        // Record in history
        {
            let mut hist = self.history.lock();
            if hist.len() >= self.history_capacity {
                hist.pop_front();
            }
            hist.push_back(entry.clone());
        }
        self.entries.write().insert(key.to_owned(), entry);
        version
    }

    /// Get a configuration value.
    pub fn get(&self, key: &str) -> Option<ConfigEntry> {
        self.entries.read().get(key).cloned()
    }

    /// Get all entries.
    pub fn all(&self) -> Vec<ConfigEntry> {
        self.entries.read().values().cloned().collect()
    }

    /// Current global version.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Get entries changed since a given version.
    pub fn changes_since(&self, since_version: u64) -> Vec<ConfigEntry> {
        self.entries.read().values()
            .filter(|e| e.version > since_version)
            .cloned()
            .collect()
    }

    /// Get config history (most recent first).
    pub fn history(&self, limit: usize) -> Vec<ConfigEntry> {
        self.history.lock().iter().rev().take(limit).cloned().collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Consistent Metadata Store (Raft-backed)
// ═══════════════════════════════════════════════════════════════════════════

/// Metadata key domain — what kind of metadata is being stored.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetadataDomain {
    ClusterConfig,
    ShardPlacement,
    VersionEpoch,
    SecurityPolicy,
    NodeRegistry,
}

impl fmt::Display for MetadataDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClusterConfig => write!(f, "cluster_config"),
            Self::ShardPlacement => write!(f, "shard_placement"),
            Self::VersionEpoch => write!(f, "version_epoch"),
            Self::SecurityPolicy => write!(f, "security_policy"),
            Self::NodeRegistry => write!(f, "node_registry"),
        }
    }
}

/// Read consistency level for metadata reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadConsistency {
    /// Read from leader only (strongly consistent).
    Leader,
    /// Read from any node (may be stale).
    Any,
    /// Read from leader, but tolerate bounded staleness.
    BoundedStaleness { max_age_ms: u64 },
}

/// A single metadata mutation in the Raft log.
#[derive(Debug, Clone)]
pub struct MetadataCommand {
    pub id: u64,
    pub domain: MetadataDomain,
    pub operation: MetadataOperation,
    pub issued_by: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub enum MetadataOperation {
    Put { key: String, value: String },
    Delete { key: String },
    CAS { key: String, expected: Option<String>, new_value: String },
}

impl fmt::Display for MetadataOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Put { key, .. } => write!(f, "PUT {key}"),
            Self::Delete { key } => write!(f, "DELETE {key}"),
            Self::CAS { key, .. } => write!(f, "CAS {key}"),
        }
    }
}

/// Result of a metadata write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataWriteResult {
    Ok { version: u64 },
    CasFailed { current: Option<String> },
    NotLeader { leader_hint: Option<u64> },
    Error(String),
}

/// Consistent metadata store — the source of truth for cluster state.
/// In production, this is Raft-replicated across 3+ controller nodes.
/// Here we provide the API surface and single-node implementation.
pub struct ConsistentMetadataStore {
    /// Per-domain key-value stores.
    domains: RwLock<HashMap<MetadataDomain, BTreeMap<String, String>>>,
    /// Raft log index (monotonic).
    log_index: AtomicU64,
    /// Committed log entries (ring buffer for observability).
    committed_log: Mutex<VecDeque<MetadataCommand>>,
    log_capacity: usize,
    /// Whether this node is the Raft leader.
    is_leader: AtomicBool,
    pub metrics: MetadataStoreMetrics,
}

#[derive(Debug, Default)]
pub struct MetadataStoreMetrics {
    pub writes: AtomicU64,
    pub reads: AtomicU64,
    pub cas_attempts: AtomicU64,
    pub cas_failures: AtomicU64,
    pub not_leader_rejections: AtomicU64,
}

impl ConsistentMetadataStore {
    pub fn new(is_leader: bool) -> Self {
        let mut domains = HashMap::new();
        for domain in &[
            MetadataDomain::ClusterConfig,
            MetadataDomain::ShardPlacement,
            MetadataDomain::VersionEpoch,
            MetadataDomain::SecurityPolicy,
            MetadataDomain::NodeRegistry,
        ] {
            domains.insert(domain.clone(), BTreeMap::new());
        }
        Self {
            domains: RwLock::new(domains),
            log_index: AtomicU64::new(0),
            committed_log: Mutex::new(VecDeque::with_capacity(10_000)),
            log_capacity: 10_000,
            is_leader: AtomicBool::new(is_leader),
            metrics: MetadataStoreMetrics::default(),
        }
    }

    /// Write a key-value pair. Only the leader can write.
    pub fn put(
        &self,
        domain: MetadataDomain,
        key: &str,
        value: &str,
        issued_by: &str,
    ) -> MetadataWriteResult {
        if !self.is_leader.load(Ordering::SeqCst) {
            self.metrics.not_leader_rejections.fetch_add(1, Ordering::Relaxed);
            return MetadataWriteResult::NotLeader { leader_hint: None };
        }
        self.metrics.writes.fetch_add(1, Ordering::Relaxed);
        let idx = self.log_index.fetch_add(1, Ordering::SeqCst) + 1;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let cmd = MetadataCommand {
            id: idx,
            domain: domain.clone(),
            operation: MetadataOperation::Put {
                key: key.to_owned(),
                value: value.to_owned(),
            },
            issued_by: issued_by.to_owned(),
            timestamp: now,
        };
        // Apply to state machine
        {
            let mut domains = self.domains.write();
            let store = domains.entry(domain).or_default();
            store.insert(key.to_owned(), value.to_owned());
        }
        // Append to committed log
        {
            let mut log = self.committed_log.lock();
            if log.len() >= self.log_capacity {
                log.pop_front();
            }
            log.push_back(cmd);
        }
        MetadataWriteResult::Ok { version: idx }
    }

    /// Compare-and-swap. Only succeeds if current value matches `expected`.
    pub fn cas(
        &self,
        domain: MetadataDomain,
        key: &str,
        expected: Option<&str>,
        new_value: &str,
        _issued_by: &str,
    ) -> MetadataWriteResult {
        if !self.is_leader.load(Ordering::SeqCst) {
            self.metrics.not_leader_rejections.fetch_add(1, Ordering::Relaxed);
            return MetadataWriteResult::NotLeader { leader_hint: None };
        }
        self.metrics.cas_attempts.fetch_add(1, Ordering::Relaxed);
        let mut domains = self.domains.write();
        let store = domains.entry(domain).or_default();
        let current = store.get(key).cloned();
        let expected_str = expected.map(std::string::ToString::to_string);
        if current != expected_str {
            self.metrics.cas_failures.fetch_add(1, Ordering::Relaxed);
            return MetadataWriteResult::CasFailed { current };
        }
        store.insert(key.to_owned(), new_value.to_owned());
        let idx = self.log_index.fetch_add(1, Ordering::SeqCst) + 1;
        self.metrics.writes.fetch_add(1, Ordering::Relaxed);
        MetadataWriteResult::Ok { version: idx }
    }

    /// Delete a key.
    pub fn delete(
        &self,
        domain: MetadataDomain,
        key: &str,
        _issued_by: &str,
    ) -> MetadataWriteResult {
        if !self.is_leader.load(Ordering::SeqCst) {
            self.metrics.not_leader_rejections.fetch_add(1, Ordering::Relaxed);
            return MetadataWriteResult::NotLeader { leader_hint: None };
        }
        self.metrics.writes.fetch_add(1, Ordering::Relaxed);
        let idx = self.log_index.fetch_add(1, Ordering::SeqCst) + 1;
        {
            let mut domains = self.domains.write();
            if let Some(store) = domains.get_mut(&domain) {
                store.remove(key);
            }
        }
        MetadataWriteResult::Ok { version: idx }
    }

    /// Read a key. Consistency level determines whether we require leader.
    pub fn get(
        &self,
        domain: &MetadataDomain,
        key: &str,
        consistency: ReadConsistency,
    ) -> Option<String> {
        if consistency == ReadConsistency::Leader && !self.is_leader.load(Ordering::SeqCst) {
            return None; // Caller should retry on leader
        }
        self.metrics.reads.fetch_add(1, Ordering::Relaxed);
        let domains = self.domains.read();
        domains.get(domain).and_then(|store| store.get(key).cloned())
    }

    /// List all keys in a domain.
    pub fn list_keys(&self, domain: &MetadataDomain) -> Vec<String> {
        self.metrics.reads.fetch_add(1, Ordering::Relaxed);
        let domains = self.domains.read();
        domains.get(domain)
            .map(|store| store.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Get all key-value pairs in a domain.
    pub fn list_all(&self, domain: &MetadataDomain) -> Vec<(String, String)> {
        self.metrics.reads.fetch_add(1, Ordering::Relaxed);
        let domains = self.domains.read();
        domains.get(domain)
            .map(|store| store.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    /// Current log index.
    pub fn log_index(&self) -> u64 {
        self.log_index.load(Ordering::SeqCst)
    }

    /// Set leader status.
    pub fn set_leader(&self, is_leader: bool) {
        self.is_leader.store(is_leader, Ordering::SeqCst);
    }

    /// Get recent committed log entries.
    pub fn recent_log(&self, count: usize) -> Vec<MetadataCommand> {
        self.committed_log.lock().iter().rev().take(count).cloned().collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Shard Placement Manager
// ═══════════════════════════════════════════════════════════════════════════

/// Shard placement record — which nodes host which shards.
#[derive(Debug, Clone)]
pub struct ShardPlacement {
    pub shard_id: u64,
    pub leader: Option<NodeId>,
    pub replicas: Vec<NodeId>,
    pub target_replica_count: usize,
    pub state: ShardPlacementState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardPlacementState {
    Healthy,
    UnderReplicated,
    Migrating,
    Orphaned,
}

impl fmt::Display for ShardPlacementState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Healthy => write!(f, "HEALTHY"),
            Self::UnderReplicated => write!(f, "UNDER_REPLICATED"),
            Self::Migrating => write!(f, "MIGRATING"),
            Self::Orphaned => write!(f, "ORPHANED"),
        }
    }
}

/// Shard placement manager — authoritative shard→node mapping.
pub struct ShardPlacementManager {
    placements: RwLock<HashMap<u64, ShardPlacement>>,
    pub metrics: PlacementMetrics,
}

#[derive(Debug, Default)]
pub struct PlacementMetrics {
    pub placements_updated: AtomicU64,
    pub leaders_changed: AtomicU64,
    pub replicas_added: AtomicU64,
    pub replicas_removed: AtomicU64,
}

impl Default for ShardPlacementManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardPlacementManager {
    pub fn new() -> Self {
        Self {
            placements: RwLock::new(HashMap::new()),
            metrics: PlacementMetrics::default(),
        }
    }

    /// Create or update a shard placement.
    pub fn set_placement(&self, placement: ShardPlacement) {
        self.metrics.placements_updated.fetch_add(1, Ordering::Relaxed);
        self.placements.write().insert(placement.shard_id, placement);
    }

    /// Get placement for a shard.
    pub fn get_placement(&self, shard_id: u64) -> Option<ShardPlacement> {
        self.placements.read().get(&shard_id).cloned()
    }

    /// Set leader for a shard.
    pub fn set_leader(&self, shard_id: u64, leader: NodeId) -> bool {
        let mut placements = self.placements.write();
        if let Some(p) = placements.get_mut(&shard_id) {
            p.leader = Some(leader);
            self.metrics.leaders_changed.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Add a replica to a shard.
    pub fn add_replica(&self, shard_id: u64, replica: NodeId) -> bool {
        let mut placements = self.placements.write();
        if let Some(p) = placements.get_mut(&shard_id) {
            if !p.replicas.contains(&replica) {
                p.replicas.push(replica);
                self.metrics.replicas_added.fetch_add(1, Ordering::Relaxed);
            }
            true
        } else {
            false
        }
    }

    /// Remove a replica from a shard.
    pub fn remove_replica(&self, shard_id: u64, replica: NodeId) -> bool {
        let mut placements = self.placements.write();
        if let Some(p) = placements.get_mut(&shard_id) {
            let before = p.replicas.len();
            p.replicas.retain(|r| *r != replica);
            if p.replicas.len() < before {
                self.metrics.replicas_removed.fetch_add(1, Ordering::Relaxed);
            }
            true
        } else {
            false
        }
    }

    /// Remove a node from all placements (node going offline).
    pub fn remove_node_from_all(&self, node_id: NodeId) {
        let mut placements = self.placements.write();
        for p in placements.values_mut() {
            if p.leader == Some(node_id) {
                p.leader = None;
            }
            p.replicas.retain(|r| *r != node_id);
        }
    }

    /// Get all placements.
    pub fn all_placements(&self) -> Vec<ShardPlacement> {
        self.placements.read().values().cloned().collect()
    }

    /// Get shards on a specific node (as leader or replica).
    pub fn shards_on_node(&self, node_id: NodeId) -> Vec<u64> {
        self.placements.read().values()
            .filter(|p| p.leader == Some(node_id) || p.replicas.contains(&node_id))
            .map(|p| p.shard_id)
            .collect()
    }

    /// Find under-replicated shards.
    pub fn under_replicated(&self) -> Vec<ShardPlacement> {
        self.placements.read().values()
            .filter(|p| p.replicas.len() < p.target_replica_count || p.leader.is_none())
            .cloned()
            .collect()
    }

    /// Evaluate placement health.
    pub fn evaluate_health(&self) {
        let mut placements = self.placements.write();
        for p in placements.values_mut() {
            if p.leader.is_none() {
                p.state = ShardPlacementState::Orphaned;
            } else if p.replicas.len() < p.target_replica_count {
                p.state = ShardPlacementState::UnderReplicated;
            } else {
                p.state = ShardPlacementState::Healthy;
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — Ops Command Dispatch
// ═══════════════════════════════════════════════════════════════════════════

/// Ops command that can be dispatched via the controller.
#[derive(Debug, Clone)]
pub enum ControlPlaneCommand {
    DrainNode(NodeId),
    JoinNode { node_id: NodeId, address: String },
    UpgradeNode { node_id: NodeId, target_version: String },
    RebalanceShard { shard_id: u64, from: NodeId, to: NodeId },
    SetConfig { key: String, value: String },
    ForceLeaderElection { shard_id: u64 },
}

impl fmt::Display for ControlPlaneCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DrainNode(id) => write!(f, "DRAIN {id:?}"),
            Self::JoinNode { node_id, address } =>
                write!(f, "JOIN {node_id:?} at {address}"),
            Self::UpgradeNode { node_id, target_version } =>
                write!(f, "UPGRADE {node_id:?} to {target_version}"),
            Self::RebalanceShard { shard_id, from, to } =>
                write!(f, "REBALANCE shard {shard_id} {from:?}→{to:?}"),
            Self::SetConfig { key, value } =>
                write!(f, "CONFIG {key}={value}"),
            Self::ForceLeaderElection { shard_id } =>
                write!(f, "FORCE_ELECTION shard {shard_id}"),
        }
    }
}

/// Command execution result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandResult {
    Accepted { command_id: u64 },
    Rejected { reason: String },
    NotLeader { leader_hint: Option<u64> },
}

/// Command dispatcher — queues and tracks ops commands.
pub struct CommandDispatcher {
    queue: Mutex<VecDeque<(u64, ControlPlaneCommand)>>,
    next_id: AtomicU64,
    completed: Mutex<Vec<(u64, bool, String)>>,
    pub metrics: DispatcherMetrics,
}

#[derive(Debug, Default)]
pub struct DispatcherMetrics {
    pub commands_accepted: AtomicU64,
    pub commands_rejected: AtomicU64,
    pub commands_completed: AtomicU64,
}

impl Default for CommandDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandDispatcher {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            next_id: AtomicU64::new(1),
            completed: Mutex::new(Vec::new()),
            metrics: DispatcherMetrics::default(),
        }
    }

    /// Submit a command.
    pub fn submit(&self, cmd: ControlPlaneCommand) -> CommandResult {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.queue.lock().push_back((id, cmd));
        self.metrics.commands_accepted.fetch_add(1, Ordering::Relaxed);
        CommandResult::Accepted { command_id: id }
    }

    /// Pop the next command to execute.
    pub fn next_command(&self) -> Option<(u64, ControlPlaneCommand)> {
        self.queue.lock().pop_front()
    }

    /// Mark a command as completed.
    pub fn complete(&self, command_id: u64, success: bool, message: String) {
        self.completed.lock().push((command_id, success, message));
        self.metrics.commands_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Get pending command count.
    pub fn pending_count(&self) -> usize {
        self.queue.lock().len()
    }

    /// Get completed commands.
    pub fn completed_commands(&self) -> Vec<(u64, bool, String)> {
        self.completed.lock().clone()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // -- Controller HA Tests --

    #[test]
    fn test_controller_ha_elect_self() {
        let ha = ControllerHAGroup::new(1, vec![(1, "127.0.0.1:9000".into()), (2, "127.0.0.1:9001".into())]);
        let term = ha.elect_self();
        assert!(term >= 2);
        assert!(ha.is_leader());
        assert_eq!(ha.leader(), Some(1));
    }

    #[test]
    fn test_controller_ha_set_leader() {
        let ha = ControllerHAGroup::new(1, vec![(1, "a".into()), (2, "b".into()), (3, "c".into())]);
        ha.set_leader(2, 5);
        assert!(!ha.is_leader());
        assert_eq!(ha.leader(), Some(2));
        assert_eq!(ha.term(), 5);
    }

    #[test]
    fn test_controller_heartbeat() {
        let ha = ControllerHAGroup::new(1, vec![(1, "a".into()), (2, "b".into())]);
        ha.record_heartbeat(2);
        assert_eq!(ha.metrics.heartbeats_received.load(Ordering::Relaxed), 1);
    }

    // -- Node Registry Tests --

    #[test]
    fn test_node_registration() {
        let reg = NodeRegistry::new(Duration::from_secs(3), Duration::from_secs(10));
        assert!(reg.register(NodeId(1), "host1:5000".into(), NodeCapabilities::default()));
        assert!(!reg.register(NodeId(1), "host1:5000".into(), NodeCapabilities::default())); // dup
        assert_eq!(reg.node_count(), 1);
    }

    #[test]
    fn test_node_heartbeat_and_evaluate() {
        let reg = NodeRegistry::new(Duration::from_millis(30), Duration::from_millis(80));
        reg.register(NodeId(1), "h1".into(), NodeCapabilities::default());
        reg.register(NodeId(2), "h2".into(), NodeCapabilities::default());

        // Both online
        assert_eq!(reg.online_nodes().len(), 2);

        // Wait for suspect
        std::thread::sleep(Duration::from_millis(40));
        reg.heartbeat(NodeId(1), 1); // refresh node 1
        let transitions = reg.evaluate();
        // Node 2 should be suspect
        let n2 = reg.get(NodeId(2)).unwrap();
        assert_eq!(n2.state, DataNodeState::Suspect);
    }

    #[test]
    fn test_node_offline_detection() {
        let reg = NodeRegistry::new(Duration::from_millis(10), Duration::from_millis(30));
        reg.register(NodeId(1), "h1".into(), NodeCapabilities::default());
        std::thread::sleep(Duration::from_millis(40));
        reg.evaluate();
        assert_eq!(reg.get(NodeId(1)).unwrap().state, DataNodeState::Offline);
    }

    #[test]
    fn test_node_deregister() {
        let reg = NodeRegistry::new(Duration::from_secs(3), Duration::from_secs(10));
        reg.register(NodeId(1), "h1".into(), NodeCapabilities::default());
        assert!(reg.deregister(NodeId(1)));
        assert!(reg.get(NodeId(1)).is_none());
    }

    #[test]
    fn test_config_needs_update() {
        let reg = NodeRegistry::new(Duration::from_secs(3), Duration::from_secs(10));
        reg.register(NodeId(1), "h1".into(), NodeCapabilities::default());
        reg.register(NodeId(2), "h2".into(), NodeCapabilities::default());
        reg.heartbeat(NodeId(1), 5); // config v5
        // Node 2 still at config v0
        let needing = reg.nodes_needing_config(5);
        assert_eq!(needing.len(), 1);
        assert_eq!(needing[0], NodeId(2));
    }

    // -- Config Store Tests --

    #[test]
    fn test_config_set_and_get() {
        let store = ConfigStore::new();
        let v = store.set("cluster.name", "prod-1", "admin");
        assert!(v >= 2);
        let entry = store.get("cluster.name").unwrap();
        assert_eq!(entry.value, "prod-1");
        assert_eq!(entry.updated_by, "admin");
    }

    #[test]
    fn test_config_versioning() {
        let store = ConfigStore::new();
        let v1 = store.set("a", "1", "admin");
        let v2 = store.set("b", "2", "admin");
        assert!(v2 > v1);
        let changes = store.changes_since(v1);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].key, "b");
    }

    #[test]
    fn test_config_history() {
        let store = ConfigStore::new();
        store.set("a", "1", "admin");
        store.set("a", "2", "admin");
        store.set("b", "3", "admin");
        let hist = store.history(2);
        assert_eq!(hist.len(), 2);
        assert_eq!(hist[0].key, "b");
    }

    // -- Metadata Store Tests --

    #[test]
    fn test_metadata_put_and_get() {
        let store = ConsistentMetadataStore::new(true);
        let result = store.put(
            MetadataDomain::ClusterConfig,
            "cluster.name",
            "prod",
            "admin",
        );
        assert!(matches!(result, MetadataWriteResult::Ok { .. }));
        let val = store.get(
            &MetadataDomain::ClusterConfig,
            "cluster.name",
            ReadConsistency::Leader,
        );
        assert_eq!(val, Some("prod".to_string()));
    }

    #[test]
    fn test_metadata_cas_success() {
        let store = ConsistentMetadataStore::new(true);
        store.put(MetadataDomain::ClusterConfig, "key", "v1", "admin");
        let result = store.cas(
            MetadataDomain::ClusterConfig,
            "key",
            Some("v1"),
            "v2",
            "admin",
        );
        assert!(matches!(result, MetadataWriteResult::Ok { .. }));
        assert_eq!(
            store.get(&MetadataDomain::ClusterConfig, "key", ReadConsistency::Any),
            Some("v2".to_string())
        );
    }

    #[test]
    fn test_metadata_cas_failure() {
        let store = ConsistentMetadataStore::new(true);
        store.put(MetadataDomain::ClusterConfig, "key", "v1", "admin");
        let result = store.cas(
            MetadataDomain::ClusterConfig,
            "key",
            Some("wrong"),
            "v2",
            "admin",
        );
        assert!(matches!(result, MetadataWriteResult::CasFailed { .. }));
    }

    #[test]
    fn test_metadata_not_leader_rejection() {
        let store = ConsistentMetadataStore::new(false);
        let result = store.put(MetadataDomain::ClusterConfig, "key", "v1", "admin");
        assert!(matches!(result, MetadataWriteResult::NotLeader { .. }));
    }

    #[test]
    fn test_metadata_delete() {
        let store = ConsistentMetadataStore::new(true);
        store.put(MetadataDomain::ClusterConfig, "key", "v1", "admin");
        store.delete(MetadataDomain::ClusterConfig, "key", "admin");
        assert_eq!(
            store.get(&MetadataDomain::ClusterConfig, "key", ReadConsistency::Any),
            None
        );
    }

    #[test]
    fn test_metadata_list_keys() {
        let store = ConsistentMetadataStore::new(true);
        store.put(MetadataDomain::ShardPlacement, "s0", "node1", "system");
        store.put(MetadataDomain::ShardPlacement, "s1", "node2", "system");
        let keys = store.list_keys(&MetadataDomain::ShardPlacement);
        assert_eq!(keys.len(), 2);
    }

    // -- Shard Placement Tests --

    #[test]
    fn test_shard_placement_basic() {
        let mgr = ShardPlacementManager::new();
        mgr.set_placement(ShardPlacement {
            shard_id: 0,
            leader: Some(NodeId(1)),
            replicas: vec![NodeId(2), NodeId(3)],
            target_replica_count: 2,
            state: ShardPlacementState::Healthy,
        });
        let p = mgr.get_placement(0).unwrap();
        assert_eq!(p.leader, Some(NodeId(1)));
        assert_eq!(p.replicas.len(), 2);
    }

    #[test]
    fn test_shard_placement_remove_node() {
        let mgr = ShardPlacementManager::new();
        mgr.set_placement(ShardPlacement {
            shard_id: 0,
            leader: Some(NodeId(1)),
            replicas: vec![NodeId(2), NodeId(3)],
            target_replica_count: 2,
            state: ShardPlacementState::Healthy,
        });
        mgr.remove_node_from_all(NodeId(1));
        let p = mgr.get_placement(0).unwrap();
        assert_eq!(p.leader, None);
    }

    #[test]
    fn test_shard_placement_health_evaluation() {
        let mgr = ShardPlacementManager::new();
        mgr.set_placement(ShardPlacement {
            shard_id: 0,
            leader: Some(NodeId(1)),
            replicas: vec![NodeId(2)],
            target_replica_count: 2,
            state: ShardPlacementState::Healthy,
        });
        mgr.set_placement(ShardPlacement {
            shard_id: 1,
            leader: None,
            replicas: vec![],
            target_replica_count: 2,
            state: ShardPlacementState::Healthy,
        });
        mgr.evaluate_health();
        assert_eq!(mgr.get_placement(0).unwrap().state, ShardPlacementState::UnderReplicated);
        assert_eq!(mgr.get_placement(1).unwrap().state, ShardPlacementState::Orphaned);
    }

    #[test]
    fn test_under_replicated() {
        let mgr = ShardPlacementManager::new();
        mgr.set_placement(ShardPlacement {
            shard_id: 0,
            leader: Some(NodeId(1)),
            replicas: vec![NodeId(2)],
            target_replica_count: 3,
            state: ShardPlacementState::Healthy,
        });
        let under = mgr.under_replicated();
        assert_eq!(under.len(), 1);
    }

    // -- Command Dispatcher Tests --

    #[test]
    fn test_command_dispatch_lifecycle() {
        let disp = CommandDispatcher::new();
        let result = disp.submit(ControlPlaneCommand::DrainNode(NodeId(1)));
        let cmd_id = match result {
            CommandResult::Accepted { command_id } => command_id,
            _ => panic!("expected accepted"),
        };
        assert_eq!(disp.pending_count(), 1);
        let (id, cmd) = disp.next_command().unwrap();
        assert_eq!(id, cmd_id);
        disp.complete(id, true, "done".into());
        assert_eq!(disp.pending_count(), 0);
        assert_eq!(disp.completed_commands().len(), 1);
    }

    // -- Concurrent Tests --

    #[test]
    fn test_registry_concurrent() {
        use std::sync::Arc;
        let reg = Arc::new(NodeRegistry::new(Duration::from_secs(3), Duration::from_secs(10)));
        for i in 1..=10u64 {
            reg.register(NodeId(i), format!("h{}", i), NodeCapabilities::default());
        }
        let mut handles = Vec::new();
        for t in 0..4 {
            let reg_c = Arc::clone(&reg);
            handles.push(std::thread::spawn(move || {
                for i in 0..500u64 {
                    let node = NodeId((t * 500 + i) % 10 + 1);
                    reg_c.heartbeat(node, i);
                    if i % 100 == 0 {
                        reg_c.evaluate();
                    }
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(reg.metrics.heartbeats.load(Ordering::Relaxed), 2000);
    }

    #[test]
    fn test_metadata_store_concurrent() {
        use std::sync::Arc;
        let store = Arc::new(ConsistentMetadataStore::new(true));
        let mut handles = Vec::new();
        for t in 0..4 {
            let s = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for i in 0..250u64 {
                    let key = format!("key-{}-{}", t, i);
                    s.put(MetadataDomain::ClusterConfig, &key, "v", "test");
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(store.metrics.writes.load(Ordering::Relaxed), 1000);
    }
}
