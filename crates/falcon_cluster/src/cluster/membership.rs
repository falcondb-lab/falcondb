//! Production-grade cluster membership management.
//!
//! Provides the full lifecycle for cluster nodes:
//! - **Join**: A node requests to join the cluster, receives an epoch.
//! - **Leave**: A node announces departure; routing updated.
//! - **Drain**: A node enters draining state; new writes routed away.
//! - **Heartbeat**: Periodic liveness signal with node-level metrics.
//! - **View**: Snapshot of membership with monotonic epoch/version.
//!
//! # Invariants
//!
//! - **M-1**: `MembershipView.epoch` is monotonically increasing across all mutations.
//! - **M-2**: Epoch mismatch between coordinator and participant → fail-fast reject writes.
//! - **M-3**: If quorum is not met, writes are rejected (reads may be allowed with degraded flag).
//! - **M-4**: Node state transitions are forward-only within a single epoch.
//! - **M-5**: Every membership mutation is logged to `ClusterEventLog` for observability.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use falcon_common::types::NodeId;

// ═══════════════════════════════════════════════════════════════════════════
// Node State Machine
// ═══════════════════════════════════════════════════════════════════════════

/// Lifecycle state of a cluster node.
///
/// ```text
/// Joining ──► Active ──► Draining ──► Leaving ──► Dead
///                │                                  ▲
///                └──────── (crash/timeout) ──────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is being added to the cluster; not yet receiving traffic.
    Joining,
    /// Node is fully operational; receives reads and writes.
    Active,
    /// Node is draining; new writes are routed away, in-flight traffic completes.
    Draining,
    /// Node has announced departure; being removed from routing.
    Leaving,
    /// Node is unreachable or has been removed.
    Dead,
}

impl NodeState {
    /// Whether this node should receive new write traffic.
    pub const fn accepts_writes(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether this node should receive new read traffic.
    pub const fn accepts_reads(&self) -> bool {
        matches!(self, Self::Active | Self::Draining)
    }

    /// Whether this node counts toward quorum.
    pub const fn counts_for_quorum(&self) -> bool {
        matches!(self, Self::Active | Self::Draining)
    }

    /// Whether the transition from `self` to `target` is valid.
    pub const fn can_transition_to(&self, target: Self) -> bool {
        use NodeState::*;
        matches!(
            (*self, target),
            (Joining, Active)
                | (Active, Draining)
                | (Active, Leaving)
                | (Active, Dead)
                | (Draining, Leaving)
                | (Draining, Dead)
                | (Leaving, Dead)
                | (Joining, Dead)
        )
    }
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Joining => write!(f, "Joining"),
            Self::Active => write!(f, "Active"),
            Self::Draining => write!(f, "Draining"),
            Self::Leaving => write!(f, "Leaving"),
            Self::Dead => write!(f, "Dead"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Member Info
// ═══════════════════════════════════════════════════════════════════════════

/// Capacity/role hint for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum NodeRole {
    /// Can be elected coordinator; accepts reads and writes.
    #[default]
    Primary,
    /// Read-only replica; receives replicated data.
    Replica,
    /// Witness node for quorum (no data).
    Witness,
}


/// Full information about a cluster member.
#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub node_id: NodeId,
    pub addr: String,
    pub pg_port: u16,
    pub rpc_port: u16,
    pub role: NodeRole,
    pub state: NodeState,
    /// When this node joined the cluster (epoch at join time).
    pub joined_at_epoch: u64,
    /// Last heartbeat received.
    pub last_heartbeat: Instant,
    /// Optional node-level metrics from last heartbeat.
    pub metrics: NodeMetrics,
}

/// Node-level metrics reported via heartbeat.
#[derive(Debug, Clone, Default)]
pub struct NodeMetrics {
    pub cpu_usage_pct: f64,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub active_connections: u64,
    pub queries_per_sec: f64,
    pub replication_lag_ms: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// Membership View
// ═══════════════════════════════════════════════════════════════════════════

/// Snapshot of the cluster membership at a point in time.
///
/// **Invariant M-1**: `epoch` is monotonically increasing.
/// **Invariant**: `version` increments within an epoch for minor updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipView {
    /// Monotonically increasing epoch. Bumped on every membership mutation.
    pub epoch: u64,
    /// Version within epoch (minor updates like heartbeat metric refresh).
    pub version: u64,
    /// Number of nodes that count toward quorum.
    pub quorum_size: usize,
    /// Minimum nodes required for quorum (⌊n/2⌋ + 1).
    pub quorum_threshold: usize,
    /// Whether quorum is currently met.
    pub quorum_met: bool,
    /// Member summaries (serializable).
    pub members: Vec<MemberSummary>,
}

/// Serializable summary of a member (for view/metrics).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberSummary {
    pub node_id: u64,
    pub addr: String,
    pub state: String,
    pub role: String,
    pub heartbeat_age_ms: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// Membership Error
// ═══════════════════════════════════════════════════════════════════════════

/// Errors from membership operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipError {
    /// Node already exists in the cluster.
    NodeAlreadyExists(NodeId),
    /// Node not found.
    NodeNotFound(NodeId),
    /// Invalid state transition.
    InvalidTransition(NodeId, NodeState, NodeState),
    /// Quorum not met — writes rejected.
    QuorumNotMet { have: usize, need: usize },
    /// Epoch mismatch — stale request.
    EpochMismatch { expected: u64, got: u64 },
}

impl std::fmt::Display for MembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeAlreadyExists(id) => write!(f, "node {id} already in cluster"),
            Self::NodeNotFound(id) => write!(f, "node {id} not found"),
            Self::InvalidTransition(id, from, to) => {
                write!(f, "node {id}: invalid transition {from} → {to}")
            }
            Self::QuorumNotMet { have, need } => {
                write!(f, "quorum not met: have {have}, need {need}")
            }
            Self::EpochMismatch { expected, got } => {
                write!(f, "epoch mismatch: expected {expected}, got {got}")
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Membership Manager
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for membership management.
#[derive(Debug, Clone)]
pub struct MembershipConfig {
    /// How long without heartbeat before marking node Dead.
    pub heartbeat_timeout: Duration,
    /// Whether to require quorum for write operations.
    pub require_quorum_for_writes: bool,
    /// Whether to allow reads when quorum is not met.
    pub allow_reads_without_quorum: bool,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(30),
            require_quorum_for_writes: true,
            allow_reads_without_quorum: true,
        }
    }
}

/// Production-grade cluster membership manager.
///
/// Thread-safe via `RwLock`. All mutating operations bump the epoch.
pub struct MembershipManager {
    members: RwLock<HashMap<NodeId, MemberInfo>>,
    epoch: AtomicU64,
    version: AtomicU64,
    config: RwLock<MembershipConfig>,
    /// Event log for observability (bounded).
    events: RwLock<Vec<MembershipEvent>>,
    max_events: usize,
}

/// Membership event for audit/observability.
#[derive(Debug, Clone)]
pub struct MembershipEvent {
    pub epoch: u64,
    pub node_id: NodeId,
    pub event_type: MembershipEventType,
    pub timestamp: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipEventType {
    Joined,
    Activated,
    DrainStarted,
    LeaveStarted,
    MarkedDead,
    Heartbeat,
    EpochBumped,
}

impl MembershipManager {
    pub fn new(config: MembershipConfig) -> Self {
        Self {
            members: RwLock::new(HashMap::new()),
            epoch: AtomicU64::new(1),
            version: AtomicU64::new(0),
            config: RwLock::new(config),
            events: RwLock::new(Vec::new()),
            max_events: 1000,
        }
    }

    /// Create with default config.
    pub fn new_default() -> Self {
        Self::new(MembershipConfig::default())
    }

    /// Current epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Current version.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    fn bump_epoch(&self) -> u64 {
        self.version.store(0, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn bump_version(&self) -> u64 {
        self.version.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn record_event(&self, node_id: NodeId, event_type: MembershipEventType) {
        let epoch = self.epoch();
        let mut events = self.events.write();
        if events.len() >= self.max_events {
            events.remove(0);
        }
        events.push(MembershipEvent {
            epoch,
            node_id,
            event_type,
            timestamp: Instant::now(),
        });
    }

    // ── Join ──

    /// Add a node to the cluster in `Joining` state. Returns the new epoch.
    pub fn join(
        &self,
        node_id: NodeId,
        addr: String,
        pg_port: u16,
        rpc_port: u16,
        role: NodeRole,
    ) -> Result<u64, MembershipError> {
        let mut members = self.members.write();
        if members.contains_key(&node_id) {
            return Err(MembershipError::NodeAlreadyExists(node_id));
        }
        let epoch = self.bump_epoch();
        members.insert(
            node_id,
            MemberInfo {
                node_id,
                addr,
                pg_port,
                rpc_port,
                role,
                state: NodeState::Joining,
                joined_at_epoch: epoch,
                last_heartbeat: Instant::now(),
                metrics: NodeMetrics::default(),
            },
        );
        drop(members);
        self.record_event(node_id, MembershipEventType::Joined);
        tracing::info!(node = node_id.0, epoch, "node joined cluster (Joining)");
        Ok(epoch)
    }

    /// Activate a Joining node → Active. Returns the new epoch.
    pub fn activate(&self, node_id: NodeId) -> Result<u64, MembershipError> {
        self.transition(node_id, NodeState::Active)
    }

    /// Convenience: join + activate in one call. Returns epoch.
    pub fn join_and_activate(
        &self,
        node_id: NodeId,
        addr: String,
        pg_port: u16,
        rpc_port: u16,
        role: NodeRole,
    ) -> Result<u64, MembershipError> {
        self.join(node_id, addr, pg_port, rpc_port, role)?;
        self.activate(node_id)
    }

    // ── Drain ──

    /// Start draining a node: new writes routed away. Returns new epoch.
    pub fn drain(&self, node_id: NodeId) -> Result<u64, MembershipError> {
        self.transition(node_id, NodeState::Draining)
    }

    // ── Leave ──

    /// Announce node departure. Returns new epoch.
    pub fn leave(&self, node_id: NodeId) -> Result<u64, MembershipError> {
        self.transition(node_id, NodeState::Leaving)
    }

    // ── Mark Dead ──

    /// Mark a node as dead (unreachable/crashed). Returns new epoch.
    pub fn mark_dead(&self, node_id: NodeId) -> Result<u64, MembershipError> {
        self.transition(node_id, NodeState::Dead)
    }

    // ── Generic transition ──

    fn transition(&self, node_id: NodeId, target: NodeState) -> Result<u64, MembershipError> {
        let mut members = self.members.write();
        let member = members
            .get_mut(&node_id)
            .ok_or(MembershipError::NodeNotFound(node_id))?;
        let from = member.state;
        if !from.can_transition_to(target) {
            return Err(MembershipError::InvalidTransition(node_id, from, target));
        }
        member.state = target;
        let epoch = self.bump_epoch();
        drop(members);

        let event_type = match target {
            NodeState::Active => MembershipEventType::Activated,
            NodeState::Draining => MembershipEventType::DrainStarted,
            NodeState::Leaving => MembershipEventType::LeaveStarted,
            NodeState::Dead => MembershipEventType::MarkedDead,
            _ => MembershipEventType::EpochBumped,
        };
        self.record_event(node_id, event_type);
        tracing::info!(node = node_id.0, epoch, from = %from, to = %target, "node state transition");
        Ok(epoch)
    }

    // ── Heartbeat ──

    /// Record a heartbeat from a node. Updates last_seen and metrics.
    /// Does NOT bump epoch (minor update — bumps version only).
    pub fn heartbeat(
        &self,
        node_id: NodeId,
        metrics: NodeMetrics,
    ) -> Result<u64, MembershipError> {
        let mut members = self.members.write();
        let member = members
            .get_mut(&node_id)
            .ok_or(MembershipError::NodeNotFound(node_id))?;
        member.last_heartbeat = Instant::now();
        member.metrics = metrics;
        drop(members);
        let ver = self.bump_version();
        Ok(ver)
    }

    // ── View ──

    /// Get a snapshot of the current membership.
    pub fn get_view(&self) -> MembershipView {
        let members = self.members.read();
        let quorum_size = members
            .values()
            .filter(|m| m.state.counts_for_quorum())
            .count();
        // Quorum threshold is based on total membership (including Dead/Leaving),
        // so losing nodes actually reduces quorum availability.
        let total_members = members.len();
        let quorum_threshold = if total_members == 0 {
            1
        } else {
            total_members / 2 + 1
        };

        let member_summaries: Vec<MemberSummary> = members
            .values()
            .map(|m| MemberSummary {
                node_id: m.node_id.0,
                addr: m.addr.clone(),
                state: m.state.to_string(),
                role: format!("{:?}", m.role),
                heartbeat_age_ms: m.last_heartbeat.elapsed().as_millis() as u64,
            })
            .collect();

        MembershipView {
            epoch: self.epoch(),
            version: self.version(),
            quorum_size,
            quorum_threshold,
            quorum_met: quorum_size >= quorum_threshold,
            members: member_summaries,
        }
    }

    // ── Quorum ──

    /// Check if writes should be allowed based on quorum.
    pub fn check_write_quorum(&self) -> Result<(), MembershipError> {
        let config = self.config.read();
        if !config.require_quorum_for_writes {
            return Ok(());
        }
        drop(config);
        let view = self.get_view();
        if view.quorum_met {
            Ok(())
        } else {
            Err(MembershipError::QuorumNotMet {
                have: view.quorum_size,
                need: view.quorum_threshold,
            })
        }
    }

    /// Check if reads should be allowed.
    pub fn check_read_allowed(&self) -> Result<(), MembershipError> {
        let config = self.config.read();
        if config.allow_reads_without_quorum {
            return Ok(());
        }
        drop(config);
        let view = self.get_view();
        if view.quorum_met {
            Ok(())
        } else {
            Err(MembershipError::QuorumNotMet {
                have: view.quorum_size,
                need: view.quorum_threshold,
            })
        }
    }

    // ── Epoch validation ──

    /// Validate that a request's epoch matches the current epoch.
    /// Used by distributed execution to fail-fast on stale routing.
    pub fn validate_epoch(&self, request_epoch: u64) -> Result<(), MembershipError> {
        let current = self.epoch();
        if request_epoch != current {
            Err(MembershipError::EpochMismatch {
                expected: current,
                got: request_epoch,
            })
        } else {
            Ok(())
        }
    }

    // ── Dead node detection ──

    /// Scan all members and mark timed-out nodes as Dead.
    /// Returns the list of nodes that were marked dead.
    pub fn detect_dead_nodes(&self) -> Vec<NodeId> {
        let timeout = self.config.read().heartbeat_timeout;
        let members = self.members.read();
        let candidates: Vec<NodeId> = members
            .values()
            .filter(|m| {
                m.state.counts_for_quorum() && m.last_heartbeat.elapsed() > timeout
            })
            .map(|m| m.node_id)
            .collect();
        drop(members);

        let mut marked = Vec::new();
        for node_id in candidates {
            if self.mark_dead(node_id).is_ok() {
                marked.push(node_id);
            }
        }
        marked
    }

    // ── Query helpers ──

    /// Get nodes that accept writes (Active only).
    pub fn write_eligible_nodes(&self) -> Vec<NodeId> {
        self.members
            .read()
            .values()
            .filter(|m| m.state.accepts_writes())
            .map(|m| m.node_id)
            .collect()
    }

    /// Get nodes that accept reads (Active + Draining).
    pub fn read_eligible_nodes(&self) -> Vec<NodeId> {
        self.members
            .read()
            .values()
            .filter(|m| m.state.accepts_reads())
            .map(|m| m.node_id)
            .collect()
    }

    /// Number of members (all states).
    pub fn member_count(&self) -> usize {
        self.members.read().len()
    }

    /// Get a specific member's info.
    pub fn get_member(&self, node_id: NodeId) -> Option<MemberInfo> {
        self.members.read().get(&node_id).cloned()
    }

    /// Get the state of a specific node.
    pub fn get_state(&self, node_id: NodeId) -> Option<NodeState> {
        self.members.read().get(&node_id).map(|m| m.state)
    }

    /// Recent events for observability.
    pub fn recent_events(&self, limit: usize) -> Vec<MembershipEvent> {
        let events = self.events.read();
        let start = events.len().saturating_sub(limit);
        events[start..].to_vec()
    }

    /// Metrics snapshot for Prometheus.
    pub fn metrics_snapshot(&self) -> MembershipMetrics {
        let members = self.members.read();
        let mut by_state: HashMap<String, usize> = HashMap::new();
        let mut max_heartbeat_lag_ms: u64 = 0;
        for m in members.values() {
            *by_state.entry(m.state.to_string()).or_insert(0) += 1;
            let lag = m.last_heartbeat.elapsed().as_millis() as u64;
            if lag > max_heartbeat_lag_ms {
                max_heartbeat_lag_ms = lag;
            }
        }
        MembershipMetrics {
            epoch: self.epoch(),
            version: self.version(),
            total_members: members.len(),
            active: *by_state.get("Active").unwrap_or(&0),
            draining: *by_state.get("Draining").unwrap_or(&0),
            joining: *by_state.get("Joining").unwrap_or(&0),
            leaving: *by_state.get("Leaving").unwrap_or(&0),
            dead: *by_state.get("Dead").unwrap_or(&0),
            max_heartbeat_lag_ms,
            quorum_met: self.get_view().quorum_met,
        }
    }
}

/// Metrics snapshot for Prometheus/observability.
#[derive(Debug, Clone)]
pub struct MembershipMetrics {
    pub epoch: u64,
    pub version: u64,
    pub total_members: usize,
    pub active: usize,
    pub draining: usize,
    pub joining: usize,
    pub leaving: usize,
    pub dead: usize,
    pub max_heartbeat_lag_ms: u64,
    pub quorum_met: bool,
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::types::NodeId;

    fn mgr() -> MembershipManager {
        MembershipManager::new_default()
    }

    // ── Join / Activate ──

    #[test]
    fn test_join_creates_joining_node() {
        let m = mgr();
        let epoch = m.join(NodeId(1), "127.0.0.1".into(), 5432, 9100, NodeRole::Primary).unwrap();
        assert!(epoch > 0);
        assert_eq!(m.get_state(NodeId(1)), Some(NodeState::Joining));
        assert_eq!(m.member_count(), 1);
    }

    #[test]
    fn test_join_duplicate_rejected() {
        let m = mgr();
        m.join(NodeId(1), "127.0.0.1".into(), 5432, 9100, NodeRole::Primary).unwrap();
        let err = m.join(NodeId(1), "127.0.0.1".into(), 5432, 9100, NodeRole::Primary);
        assert!(matches!(err, Err(MembershipError::NodeAlreadyExists(_))));
    }

    #[test]
    fn test_activate_from_joining() {
        let m = mgr();
        m.join(NodeId(1), "127.0.0.1".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.activate(NodeId(1)).unwrap();
        assert_eq!(m.get_state(NodeId(1)), Some(NodeState::Active));
    }

    #[test]
    fn test_join_and_activate() {
        let m = mgr();
        let epoch = m.join_and_activate(NodeId(1), "host1".into(), 5432, 9100, NodeRole::Primary).unwrap();
        assert_eq!(m.get_state(NodeId(1)), Some(NodeState::Active));
        assert!(epoch > 1); // join bumps, activate bumps again
    }

    // ── Drain / Leave ──

    #[test]
    fn test_drain_from_active() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.drain(NodeId(1)).unwrap();
        assert_eq!(m.get_state(NodeId(1)), Some(NodeState::Draining));
        assert!(!NodeState::Draining.accepts_writes());
        assert!(NodeState::Draining.accepts_reads());
    }

    #[test]
    fn test_leave_from_draining() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.drain(NodeId(1)).unwrap();
        m.leave(NodeId(1)).unwrap();
        assert_eq!(m.get_state(NodeId(1)), Some(NodeState::Leaving));
    }

    #[test]
    fn test_leave_from_active() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.leave(NodeId(1)).unwrap();
        assert_eq!(m.get_state(NodeId(1)), Some(NodeState::Leaving));
    }

    // ── Invalid transitions ──

    #[test]
    fn test_invalid_transition_dead_to_active() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.mark_dead(NodeId(1)).unwrap();
        let err = m.activate(NodeId(1));
        assert!(matches!(err, Err(MembershipError::InvalidTransition(..))));
    }

    #[test]
    fn test_invalid_transition_joining_to_draining() {
        let m = mgr();
        m.join(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        let err = m.drain(NodeId(1));
        assert!(matches!(err, Err(MembershipError::InvalidTransition(..))));
    }

    // ── Epoch ──

    #[test]
    fn test_epoch_monotonic() {
        let m = mgr();
        let e1 = m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        let e2 = m.join_and_activate(NodeId(2), "h2".into(), 5432, 9100, NodeRole::Primary).unwrap();
        assert!(e2 > e1);
    }

    #[test]
    fn test_epoch_validation() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        let current = m.epoch();
        assert!(m.validate_epoch(current).is_ok());
        assert!(m.validate_epoch(current + 1).is_err());
        assert!(m.validate_epoch(current - 1).is_err());
    }

    // ── Heartbeat ──

    #[test]
    fn test_heartbeat_updates_metrics() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        let metrics = NodeMetrics {
            cpu_usage_pct: 42.0,
            active_connections: 100,
            ..Default::default()
        };
        m.heartbeat(NodeId(1), metrics).unwrap();
        let info = m.get_member(NodeId(1)).unwrap();
        assert!((info.metrics.cpu_usage_pct - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_heartbeat_unknown_node() {
        let m = mgr();
        let err = m.heartbeat(NodeId(99), NodeMetrics::default());
        assert!(matches!(err, Err(MembershipError::NodeNotFound(_))));
    }

    // ── View ──

    #[test]
    fn test_get_view() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h1".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.join_and_activate(NodeId(2), "h2".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.join_and_activate(NodeId(3), "h3".into(), 5432, 9100, NodeRole::Primary).unwrap();
        let view = m.get_view();
        assert_eq!(view.members.len(), 3);
        assert!(view.quorum_met);
        assert_eq!(view.quorum_threshold, 2); // ⌊3/2⌋ + 1
    }

    // ── Quorum ──

    #[test]
    fn test_quorum_with_3_nodes() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.join_and_activate(NodeId(2), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.join_and_activate(NodeId(3), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        assert!(m.check_write_quorum().is_ok());

        // Kill 2 of 3 → quorum lost
        m.mark_dead(NodeId(2)).unwrap();
        m.mark_dead(NodeId(3)).unwrap();
        let err = m.check_write_quorum();
        assert!(matches!(err, Err(MembershipError::QuorumNotMet { .. })));
    }

    #[test]
    fn test_single_node_quorum() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        assert!(m.check_write_quorum().is_ok());
    }

    // ── Write/Read eligibility ──

    #[test]
    fn test_write_eligible_excludes_draining() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.join_and_activate(NodeId(2), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.drain(NodeId(2)).unwrap();
        let writers = m.write_eligible_nodes();
        assert_eq!(writers.len(), 1);
        assert_eq!(writers[0], NodeId(1));
    }

    #[test]
    fn test_read_eligible_includes_draining() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.join_and_activate(NodeId(2), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.drain(NodeId(2)).unwrap();
        let readers = m.read_eligible_nodes();
        assert_eq!(readers.len(), 2);
    }

    // ── Detect dead ──

    #[test]
    fn test_detect_dead_nodes() {
        let config = MembershipConfig {
            heartbeat_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        let m = MembershipManager::new(config);
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        // Sleep just enough for the timeout
        std::thread::sleep(Duration::from_millis(5));
        let dead = m.detect_dead_nodes();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0], NodeId(1));
        assert_eq!(m.get_state(NodeId(1)), Some(NodeState::Dead));
    }

    // ── Events ──

    #[test]
    fn test_events_recorded() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        let events = m.recent_events(10);
        assert!(events.len() >= 2); // join + activate
    }

    // ── Metrics snapshot ──

    #[test]
    fn test_metrics_snapshot() {
        let m = mgr();
        m.join_and_activate(NodeId(1), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.join_and_activate(NodeId(2), "h".into(), 5432, 9100, NodeRole::Primary).unwrap();
        m.drain(NodeId(2)).unwrap();
        let snap = m.metrics_snapshot();
        assert_eq!(snap.active, 1);
        assert_eq!(snap.draining, 1);
        assert_eq!(snap.total_members, 2);
        assert!(snap.quorum_met);
    }

    // ── NodeState Display ──

    #[test]
    fn test_node_state_display() {
        assert_eq!(NodeState::Joining.to_string(), "Joining");
        assert_eq!(NodeState::Active.to_string(), "Active");
        assert_eq!(NodeState::Draining.to_string(), "Draining");
        assert_eq!(NodeState::Leaving.to_string(), "Leaving");
        assert_eq!(NodeState::Dead.to_string(), "Dead");
    }

    // ── Error Display ──

    #[test]
    fn test_membership_error_display() {
        let e = MembershipError::QuorumNotMet { have: 1, need: 2 };
        assert!(e.to_string().contains("quorum"));
        let e = MembershipError::EpochMismatch { expected: 5, got: 3 };
        assert!(e.to_string().contains("epoch"));
    }
}
