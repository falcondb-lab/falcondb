//! v1.0.9 Self-Healing: Failure Detection, Auto Re-election, Replica Catch-up,
//! Rolling Upgrade, Node Drain/Join, SLA/SLO, Backpressure v2, Ops Audit.
//!
//! Core principle: "Cluster problems are solved by the cluster, not by humans."

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use parking_lot::{Mutex, RwLock};

use falcon_common::types::NodeId;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Node Lifecycle State Machine (ALIVE / SUSPECT / DEAD)
// ═══════════════════════════════════════════════════════════════════════════

/// Node liveness state — the single source of truth for failure detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeLiveness {
    /// Heartbeat received within timeout. Fully operational.
    Alive,
    /// Heartbeat late (> suspect_threshold) but not yet timed out.
    /// Reads may still be served; writes are paused for this node.
    Suspect,
    /// No heartbeat for > failure_timeout. Node is declared dead.
    /// All shards on this node must be re-elected or re-routed.
    Dead,
    /// Node is being drained (graceful shutdown). No new work assigned.
    Draining,
    /// Node is joining the cluster. Syncing data.
    Joining,
}

impl fmt::Display for NodeLiveness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Alive => write!(f, "ALIVE"),
            Self::Suspect => write!(f, "SUSPECT"),
            Self::Dead => write!(f, "DEAD"),
            Self::Draining => write!(f, "DRAINING"),
            Self::Joining => write!(f, "JOINING"),
        }
    }
}

/// Per-node health record tracked by the failure detector.
#[derive(Debug, Clone)]
pub struct NodeHealthRecord {
    pub node_id: NodeId,
    pub liveness: NodeLiveness,
    pub last_heartbeat: Option<Instant>,
    pub consecutive_misses: u32,
    pub last_lsn: u64,
    pub replication_lag: u64,
    pub version: String,
    pub uptime_secs: u64,
    /// Reason for current state transition.
    pub reason: String,
}

/// Configuration for the failure detector.
#[derive(Debug, Clone)]
pub struct FailureDetectorConfig {
    /// Heartbeat interval — how often nodes ping each other.
    pub heartbeat_interval: Duration,
    /// After this duration without heartbeat, node becomes SUSPECT.
    pub suspect_threshold: Duration,
    /// After this duration without heartbeat, node becomes DEAD.
    pub failure_timeout: Duration,
    /// Number of consecutive misses before DEAD declaration.
    pub max_consecutive_misses: u32,
    /// Whether to auto-declare DEAD (vs. require manual confirmation).
    pub auto_declare_dead: bool,
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(1),
            suspect_threshold: Duration::from_secs(3),
            failure_timeout: Duration::from_secs(10),
            max_consecutive_misses: 5,
            auto_declare_dead: true,
        }
    }
}

/// Cluster-wide failure detector. Tracks all nodes' liveness.
pub struct ClusterFailureDetector {
    pub config: FailureDetectorConfig,
    nodes: RwLock<HashMap<NodeId, NodeHealthRecord>>,
    /// Monotonic epoch — bumped on every state transition.
    epoch: AtomicU64,
    /// Metrics.
    pub metrics: FailureDetectorMetrics,
}

#[derive(Debug, Default)]
pub struct FailureDetectorMetrics {
    pub heartbeats_received: AtomicU64,
    pub suspect_transitions: AtomicU64,
    pub dead_transitions: AtomicU64,
    pub alive_transitions: AtomicU64,
    pub evaluations: AtomicU64,
}

impl ClusterFailureDetector {
    pub fn new(config: FailureDetectorConfig) -> Self {
        Self {
            config,
            nodes: RwLock::new(HashMap::new()),
            epoch: AtomicU64::new(1),
            metrics: FailureDetectorMetrics::default(),
        }
    }

    /// Register a node in the detector.
    pub fn register_node(&self, node_id: NodeId, version: String) {
        let mut nodes = self.nodes.write();
        nodes.insert(node_id, NodeHealthRecord {
            node_id,
            liveness: NodeLiveness::Alive,
            last_heartbeat: Some(Instant::now()),
            consecutive_misses: 0,
            last_lsn: 0,
            replication_lag: 0,
            version,
            uptime_secs: 0,
            reason: "registered".into(),
        });
    }

    /// Record a heartbeat from a node.
    pub fn record_heartbeat(&self, node_id: NodeId, lsn: u64, uptime_secs: u64) {
        self.metrics.heartbeats_received.fetch_add(1, Ordering::Relaxed);
        let mut nodes = self.nodes.write();
        if let Some(record) = nodes.get_mut(&node_id) {
            let was_dead = record.liveness == NodeLiveness::Dead;
            let was_suspect = record.liveness == NodeLiveness::Suspect;
            record.last_heartbeat = Some(Instant::now());
            record.consecutive_misses = 0;
            record.last_lsn = lsn;
            record.uptime_secs = uptime_secs;
            if record.liveness == NodeLiveness::Dead || record.liveness == NodeLiveness::Suspect {
                record.liveness = NodeLiveness::Alive;
                record.reason = "heartbeat restored".into();
                self.metrics.alive_transitions.fetch_add(1, Ordering::Relaxed);
                self.epoch.fetch_add(1, Ordering::SeqCst);
            }
            let _ = (was_dead, was_suspect); // suppress unused
        }
    }

    /// Evaluate all nodes' liveness. Returns list of nodes that changed state.
    pub fn evaluate(&self) -> Vec<(NodeId, NodeLiveness, NodeLiveness)> {
        self.metrics.evaluations.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let mut transitions = Vec::new();
        let mut nodes = self.nodes.write();

        for record in nodes.values_mut() {
            // Skip nodes in managed states
            if record.liveness == NodeLiveness::Draining
                || record.liveness == NodeLiveness::Joining
            {
                continue;
            }

            let old_state = record.liveness;
            if let Some(last) = record.last_heartbeat {
                let elapsed = now.duration_since(last);
                if elapsed > self.config.failure_timeout
                    || record.consecutive_misses >= self.config.max_consecutive_misses
                {
                    if self.config.auto_declare_dead && record.liveness != NodeLiveness::Dead {
                        record.liveness = NodeLiveness::Dead;
                        record.reason = format!(
                            "no heartbeat for {:.1}s (misses: {})",
                            elapsed.as_secs_f64(),
                            record.consecutive_misses
                        );
                        self.metrics.dead_transitions.fetch_add(1, Ordering::Relaxed);
                    }
                } else if elapsed > self.config.suspect_threshold {
                    if record.liveness == NodeLiveness::Alive {
                        record.liveness = NodeLiveness::Suspect;
                        record.reason = format!(
                            "heartbeat late: {:.1}s",
                            elapsed.as_secs_f64()
                        );
                        self.metrics.suspect_transitions.fetch_add(1, Ordering::Relaxed);
                    }
                    record.consecutive_misses += 1;
                } else {
                    // Healthy
                    if record.liveness != NodeLiveness::Alive {
                        record.liveness = NodeLiveness::Alive;
                        record.reason = "heartbeat on time".into();
                        self.metrics.alive_transitions.fetch_add(1, Ordering::Relaxed);
                    }
                    record.consecutive_misses = 0;
                }
            } else {
                // Never received heartbeat — suspect
                if record.liveness == NodeLiveness::Alive {
                    record.liveness = NodeLiveness::Suspect;
                    record.reason = "no heartbeat ever received".into();
                    self.metrics.suspect_transitions.fetch_add(1, Ordering::Relaxed);
                }
            }

            if record.liveness != old_state {
                self.epoch.fetch_add(1, Ordering::SeqCst);
                transitions.push((record.node_id, old_state, record.liveness));
            }
        }

        transitions
    }

    /// Get current liveness of a node.
    pub fn get_liveness(&self, node_id: NodeId) -> Option<NodeLiveness> {
        self.nodes.read().get(&node_id).map(|r| r.liveness)
    }

    /// Get all node health records.
    pub fn snapshot(&self) -> Vec<NodeHealthRecord> {
        self.nodes.read().values().cloned().collect()
    }

    /// Count nodes by liveness state.
    pub fn count_by_state(&self) -> HashMap<NodeLiveness, usize> {
        let nodes = self.nodes.read();
        let mut counts = HashMap::new();
        for record in nodes.values() {
            *counts.entry(record.liveness).or_insert(0) += 1;
        }
        counts
    }

    /// Current epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Mark a node as draining.
    pub fn mark_draining(&self, node_id: NodeId) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(record) = nodes.get_mut(&node_id) {
            record.liveness = NodeLiveness::Draining;
            record.reason = "drain requested".into();
            self.epoch.fetch_add(1, Ordering::SeqCst);
            return true;
        }
        false
    }

    /// Mark a node as joining.
    pub fn mark_joining(&self, node_id: NodeId) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(record) = nodes.get_mut(&node_id) {
            record.liveness = NodeLiveness::Joining;
            record.reason = "join initiated".into();
            self.epoch.fetch_add(1, Ordering::SeqCst);
            return true;
        }
        false
    }

    /// Remove a node from the detector.
    pub fn remove_node(&self, node_id: NodeId) -> bool {
        self.nodes.write().remove(&node_id).is_some()
    }

    /// Get alive node count.
    pub fn alive_count(&self) -> usize {
        self.nodes.read().values().filter(|r| r.liveness == NodeLiveness::Alive).count()
    }

    /// Get dead node IDs.
    pub fn dead_nodes(&self) -> Vec<NodeId> {
        self.nodes.read().values()
            .filter(|r| r.liveness == NodeLiveness::Dead)
            .map(|r| r.node_id)
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Leader Election v1 (term-based, no split-brain)
// ═══════════════════════════════════════════════════════════════════════════

/// Election term — monotonically increasing, prevents stale leaders.
pub type Term = u64;

/// Election state for a single shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionState {
    /// Stable leader exists. Writes are accepted.
    Stable,
    /// Election in progress. Writes are blocked.
    Electing,
    /// No leader. Writes are rejected.
    NoLeader,
}

impl fmt::Display for ElectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stable => write!(f, "STABLE"),
            Self::Electing => write!(f, "ELECTING"),
            Self::NoLeader => write!(f, "NO_LEADER"),
        }
    }
}

/// Configuration for leader election.
#[derive(Debug, Clone)]
pub struct ElectionConfig {
    /// Election timeout — how long to wait before starting a new election.
    pub election_timeout: Duration,
    /// Maximum time an election can take before being aborted.
    pub election_max_duration: Duration,
    /// Whether to require majority quorum (true) or just pick highest-LSN (false).
    pub require_quorum: bool,
}

impl Default for ElectionConfig {
    fn default() -> Self {
        Self {
            election_timeout: Duration::from_secs(5),
            election_max_duration: Duration::from_secs(15),
            require_quorum: true,
        }
    }
}

/// Per-shard election record.
#[derive(Debug, Clone)]
pub struct ShardElection {
    pub shard_id: u64,
    pub term: Term,
    pub state: ElectionState,
    pub leader: Option<NodeId>,
    pub candidates: Vec<(NodeId, u64)>, // (node_id, lsn)
    pub election_started: Option<Instant>,
    pub last_stable: Option<Instant>,
    pub vote_count: usize,
    pub required_votes: usize,
}

/// Leader election coordinator — manages elections across all shards.
pub struct LeaderElectionCoordinator {
    pub config: ElectionConfig,
    shards: RwLock<HashMap<u64, ShardElection>>,
    /// Global term counter (shared across shards for simplicity).
    global_term: AtomicU64,
    pub metrics: ElectionMetrics,
}

#[derive(Debug, Default)]
pub struct ElectionMetrics {
    pub elections_started: AtomicU64,
    pub elections_completed: AtomicU64,
    pub elections_failed: AtomicU64,
    pub elections_timed_out: AtomicU64,
    pub term_bumps: AtomicU64,
    pub split_brain_prevented: AtomicU64,
}

impl LeaderElectionCoordinator {
    pub fn new(config: ElectionConfig) -> Self {
        Self {
            config,
            shards: RwLock::new(HashMap::new()),
            global_term: AtomicU64::new(1),
            metrics: ElectionMetrics::default(),
        }
    }

    /// Register a shard with its current leader.
    pub fn register_shard(&self, shard_id: u64, leader: Option<NodeId>, num_nodes: usize) {
        let quorum = num_nodes / 2 + 1;
        let mut shards = self.shards.write();
        shards.insert(shard_id, ShardElection {
            shard_id,
            term: self.global_term.load(Ordering::SeqCst),
            state: if leader.is_some() { ElectionState::Stable } else { ElectionState::NoLeader },
            leader,
            candidates: Vec::new(),
            election_started: None,
            last_stable: if leader.is_some() { Some(Instant::now()) } else { None },
            vote_count: 0,
            required_votes: quorum,
        });
    }

    /// Trigger an election for a shard (e.g., when leader is detected DEAD).
    /// Returns the new term.
    pub fn start_election(&self, shard_id: u64) -> Option<Term> {
        let new_term = self.global_term.fetch_add(1, Ordering::SeqCst) + 1;
        self.metrics.elections_started.fetch_add(1, Ordering::Relaxed);
        self.metrics.term_bumps.fetch_add(1, Ordering::Relaxed);

        let mut shards = self.shards.write();
        if let Some(election) = shards.get_mut(&shard_id) {
            election.term = new_term;
            election.state = ElectionState::Electing;
            election.leader = None;
            election.candidates.clear();
            election.vote_count = 0;
            election.election_started = Some(Instant::now());
            return Some(new_term);
        }
        None
    }

    /// Submit a candidate for election.
    pub fn submit_candidate(&self, shard_id: u64, node_id: NodeId, lsn: u64, term: Term) -> bool {
        let mut shards = self.shards.write();
        if let Some(election) = shards.get_mut(&shard_id) {
            if election.state != ElectionState::Electing || election.term != term {
                return false;
            }
            // Prevent duplicate candidates
            if election.candidates.iter().any(|(n, _)| *n == node_id) {
                return false;
            }
            election.candidates.push((node_id, lsn));
            election.vote_count += 1;
            return true;
        }
        false
    }

    /// Resolve an election — pick the candidate with the highest LSN.
    /// Requires quorum if configured. Returns the new leader.
    pub fn resolve_election(&self, shard_id: u64) -> Result<NodeId, ElectionError> {
        let mut shards = self.shards.write();
        let election = shards.get_mut(&shard_id)
            .ok_or(ElectionError::ShardNotFound(shard_id))?;

        if election.state != ElectionState::Electing {
            return Err(ElectionError::NotElecting(shard_id));
        }

        // Check timeout
        if let Some(started) = election.election_started {
            if started.elapsed() > self.config.election_max_duration {
                election.state = ElectionState::NoLeader;
                self.metrics.elections_timed_out.fetch_add(1, Ordering::Relaxed);
                return Err(ElectionError::Timeout(shard_id));
            }
        }

        // Check quorum
        if self.config.require_quorum && election.vote_count < election.required_votes {
            return Err(ElectionError::NoQuorum {
                shard_id,
                have: election.vote_count,
                need: election.required_votes,
            });
        }

        // Pick highest LSN candidate
        let winner = election.candidates.iter()
            .max_by_key(|(_, lsn)| *lsn)
            .map(|(node, _)| *node)
            .ok_or(ElectionError::NoCandidates(shard_id))?;

        // Split-brain check: ensure no existing leader with higher term
        // (In a real system, this would be a Raft-style check)

        election.state = ElectionState::Stable;
        election.leader = Some(winner);
        election.last_stable = Some(Instant::now());
        self.metrics.elections_completed.fetch_add(1, Ordering::Relaxed);

        Ok(winner)
    }

    /// Get current leader for a shard.
    pub fn get_leader(&self, shard_id: u64) -> Option<NodeId> {
        self.shards.read().get(&shard_id).and_then(|e| e.leader)
    }

    /// Get election state for a shard.
    pub fn get_state(&self, shard_id: u64) -> Option<ElectionState> {
        self.shards.read().get(&shard_id).map(|e| e.state)
    }

    /// Get current term.
    pub fn current_term(&self) -> Term {
        self.global_term.load(Ordering::SeqCst)
    }

    /// Validate a term — reject operations with stale terms.
    pub fn validate_term(&self, term: Term) -> bool {
        term >= self.global_term.load(Ordering::SeqCst)
    }

    /// Get snapshot of all shard elections.
    pub fn snapshot(&self) -> Vec<ShardElection> {
        self.shards.read().values().cloned().collect()
    }
}

/// Election errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionError {
    ShardNotFound(u64),
    NotElecting(u64),
    Timeout(u64),
    NoQuorum { shard_id: u64, have: usize, need: usize },
    NoCandidates(u64),
    SplitBrain { shard_id: u64, existing_leader: NodeId, new_leader: NodeId },
}

impl fmt::Display for ElectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShardNotFound(s) => write!(f, "shard {s} not found"),
            Self::NotElecting(s) => write!(f, "shard {s} not in election"),
            Self::Timeout(s) => write!(f, "election for shard {s} timed out"),
            Self::NoQuorum { shard_id, have, need } =>
                write!(f, "shard {shard_id}: no quorum ({have}/{need})"),
            Self::NoCandidates(s) => write!(f, "shard {s}: no candidates"),
            Self::SplitBrain { shard_id, existing_leader, new_leader } =>
                write!(f, "shard {shard_id}: split-brain detected (existing={existing_leader:?}, new={new_leader:?})"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Replica Catch-up Strategy
// ═══════════════════════════════════════════════════════════════════════════

/// How far behind a replica is — determines catch-up strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaLagClass {
    /// Within WAL replay range. Use incremental WAL streaming.
    WalReplay,
    /// Too far behind for WAL. Need full snapshot + WAL tail.
    SnapshotRequired,
    /// Caught up (lag < threshold).
    CaughtUp,
}

impl fmt::Display for ReplicaLagClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WalReplay => write!(f, "WAL_REPLAY"),
            Self::SnapshotRequired => write!(f, "SNAPSHOT_REQUIRED"),
            Self::CaughtUp => write!(f, "CAUGHT_UP"),
        }
    }
}

/// Configuration for replica catch-up.
#[derive(Debug, Clone)]
pub struct CatchUpConfig {
    /// WAL gap threshold — if lag exceeds this, use snapshot.
    pub snapshot_threshold_lsn: u64,
    /// Maximum WAL entries to stream per batch.
    pub wal_batch_size: u64,
    /// Rate limit for snapshot streaming (bytes/sec, 0 = unlimited).
    pub snapshot_rate_limit: u64,
    /// Timeout for a single catch-up attempt.
    pub catch_up_timeout: Duration,
    /// Whether catch-up is allowed to impact leader performance.
    pub allow_leader_impact: bool,
}

impl Default for CatchUpConfig {
    fn default() -> Self {
        Self {
            snapshot_threshold_lsn: 100_000,
            wal_batch_size: 1000,
            snapshot_rate_limit: 0,
            catch_up_timeout: Duration::from_secs(300),
            allow_leader_impact: false,
        }
    }
}

/// Catch-up state for a single replica.
#[derive(Debug, Clone)]
pub struct ReplicaCatchUpState {
    pub node_id: NodeId,
    pub shard_id: u64,
    pub lag_class: ReplicaLagClass,
    pub leader_lsn: u64,
    pub replica_lsn: u64,
    pub lag: u64,
    pub phase: CatchUpPhase,
    pub progress_pct: f64,
    pub started: Option<Instant>,
    pub bytes_transferred: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUpPhase {
    Idle,
    Assessing,
    StreamingWal,
    StreamingSnapshot,
    Finalizing,
    Complete,
    Failed,
}

impl fmt::Display for CatchUpPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Idle => write!(f, "IDLE"),
            Self::Assessing => write!(f, "ASSESSING"),
            Self::StreamingWal => write!(f, "STREAMING_WAL"),
            Self::StreamingSnapshot => write!(f, "STREAMING_SNAPSHOT"),
            Self::Finalizing => write!(f, "FINALIZING"),
            Self::Complete => write!(f, "COMPLETE"),
            Self::Failed => write!(f, "FAILED"),
        }
    }
}

/// Replica catch-up coordinator.
pub struct ReplicaCatchUpCoordinator {
    pub config: CatchUpConfig,
    states: RwLock<HashMap<(NodeId, u64), ReplicaCatchUpState>>,
    pub metrics: CatchUpMetrics,
}

#[derive(Debug, Default)]
pub struct CatchUpMetrics {
    pub wal_replays_started: AtomicU64,
    pub wal_replays_completed: AtomicU64,
    pub snapshots_started: AtomicU64,
    pub snapshots_completed: AtomicU64,
    pub catch_ups_failed: AtomicU64,
    pub bytes_streamed: AtomicU64,
}

impl ReplicaCatchUpCoordinator {
    pub fn new(config: CatchUpConfig) -> Self {
        Self {
            config,
            states: RwLock::new(HashMap::new()),
            metrics: CatchUpMetrics::default(),
        }
    }

    /// Classify a replica's lag and determine catch-up strategy.
    pub const fn classify_lag(&self, leader_lsn: u64, replica_lsn: u64) -> ReplicaLagClass {
        let lag = leader_lsn.saturating_sub(replica_lsn);
        if lag == 0 {
            ReplicaLagClass::CaughtUp
        } else if lag <= self.config.snapshot_threshold_lsn {
            ReplicaLagClass::WalReplay
        } else {
            ReplicaLagClass::SnapshotRequired
        }
    }

    /// Start a catch-up for a replica on a shard.
    pub fn start_catch_up(
        &self,
        node_id: NodeId,
        shard_id: u64,
        leader_lsn: u64,
        replica_lsn: u64,
    ) -> ReplicaLagClass {
        let lag_class = self.classify_lag(leader_lsn, replica_lsn);
        let lag = leader_lsn.saturating_sub(replica_lsn);

        let phase = match lag_class {
            ReplicaLagClass::CaughtUp => CatchUpPhase::Complete,
            ReplicaLagClass::WalReplay => {
                self.metrics.wal_replays_started.fetch_add(1, Ordering::Relaxed);
                CatchUpPhase::StreamingWal
            }
            ReplicaLagClass::SnapshotRequired => {
                self.metrics.snapshots_started.fetch_add(1, Ordering::Relaxed);
                CatchUpPhase::StreamingSnapshot
            }
        };

        let state = ReplicaCatchUpState {
            node_id,
            shard_id,
            lag_class,
            leader_lsn,
            replica_lsn,
            lag,
            phase,
            progress_pct: if lag == 0 { 100.0 } else { 0.0 },
            started: Some(Instant::now()),
            bytes_transferred: 0,
        };

        self.states.write().insert((node_id, shard_id), state);
        lag_class
    }

    /// Update catch-up progress.
    pub fn update_progress(
        &self,
        node_id: NodeId,
        shard_id: u64,
        new_replica_lsn: u64,
        bytes: u64,
    ) {
        let mut states = self.states.write();
        if let Some(state) = states.get_mut(&(node_id, shard_id)) {
            state.replica_lsn = new_replica_lsn;
            state.lag = state.leader_lsn.saturating_sub(new_replica_lsn);
            state.bytes_transferred += bytes;
            self.metrics.bytes_streamed.fetch_add(bytes, Ordering::Relaxed);
            if state.lag > 0 {
                let total = state.leader_lsn.saturating_sub(
                    state.leader_lsn.saturating_sub(state.lag + (new_replica_lsn - state.replica_lsn))
                );
                if total > 0 {
                    state.progress_pct = ((state.leader_lsn - state.lag) as f64
                        / state.leader_lsn as f64 * 100.0).min(100.0);
                }
            }
            if state.lag == 0 {
                state.phase = CatchUpPhase::Complete;
                state.progress_pct = 100.0;
                match state.lag_class {
                    ReplicaLagClass::WalReplay => {
                        self.metrics.wal_replays_completed.fetch_add(1, Ordering::Relaxed);
                    }
                    ReplicaLagClass::SnapshotRequired => {
                        self.metrics.snapshots_completed.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
        }
    }

    /// Mark a catch-up as failed.
    pub fn mark_failed(&self, node_id: NodeId, shard_id: u64, reason: &str) {
        let mut states = self.states.write();
        if let Some(state) = states.get_mut(&(node_id, shard_id)) {
            state.phase = CatchUpPhase::Failed;
            self.metrics.catch_ups_failed.fetch_add(1, Ordering::Relaxed);
        }
        let _ = reason;
    }

    /// Get catch-up state for a replica.
    pub fn get_state(&self, node_id: NodeId, shard_id: u64) -> Option<ReplicaCatchUpState> {
        self.states.read().get(&(node_id, shard_id)).cloned()
    }

    /// Get all active catch-ups.
    pub fn active_catch_ups(&self) -> Vec<ReplicaCatchUpState> {
        self.states.read().values()
            .filter(|s| s.phase != CatchUpPhase::Complete && s.phase != CatchUpPhase::Failed)
            .cloned()
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Rolling Upgrade
// ═══════════════════════════════════════════════════════════════════════════

/// Protocol version for wire compatibility during rolling upgrades.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProtocolVersion {
    pub major: u32,
    pub minor: u32,
}

impl ProtocolVersion {
    pub const fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    /// Check if two versions are compatible for rolling upgrade.
    /// Rule: same major, minor within 1 step.
    pub const fn is_compatible(&self, other: &Self) -> bool {
        self.major == other.major && (self.minor as i32 - other.minor as i32).unsigned_abs() <= 1
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// Node role in upgrade ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UpgradeOrder {
    /// Follower nodes upgraded first (least disruptive).
    Follower = 0,
    /// Gateway nodes upgraded second.
    Gateway = 1,
    /// Leader nodes upgraded last (most disruptive).
    Leader = 2,
}

impl fmt::Display for UpgradeOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Follower => write!(f, "follower"),
            Self::Gateway => write!(f, "gateway"),
            Self::Leader => write!(f, "leader"),
        }
    }
}

/// Upgrade state for a single node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeNodeState {
    Pending,
    Draining,
    Upgrading,
    Rejoining,
    Complete,
    Failed(String),
}

impl fmt::Display for UpgradeNodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Draining => write!(f, "DRAINING"),
            Self::Upgrading => write!(f, "UPGRADING"),
            Self::Rejoining => write!(f, "REJOINING"),
            Self::Complete => write!(f, "COMPLETE"),
            Self::Failed(r) => write!(f, "FAILED: {r}"),
        }
    }
}

/// Per-node upgrade record.
#[derive(Debug, Clone)]
pub struct UpgradeNodeRecord {
    pub node_id: NodeId,
    pub order: UpgradeOrder,
    pub from_version: String,
    pub to_version: String,
    pub state: UpgradeNodeState,
    pub started: Option<Instant>,
}

/// Rolling upgrade plan and executor.
pub struct RollingUpgradeCoordinator {
    pub target_version: String,
    pub current_protocol: ProtocolVersion,
    pub target_protocol: ProtocolVersion,
    nodes: RwLock<Vec<UpgradeNodeRecord>>,
    pub metrics: UpgradeMetrics,
}

#[derive(Debug, Default)]
pub struct UpgradeMetrics {
    pub nodes_upgraded: AtomicU64,
    pub nodes_failed: AtomicU64,
    pub total_drain_time_ms: AtomicU64,
    pub total_rejoin_time_ms: AtomicU64,
}

impl RollingUpgradeCoordinator {
    pub fn new(
        target_version: String,
        current_protocol: ProtocolVersion,
        target_protocol: ProtocolVersion,
    ) -> Self {
        Self {
            target_version,
            current_protocol,
            target_protocol,
            nodes: RwLock::new(Vec::new()),
            metrics: UpgradeMetrics::default(),
        }
    }

    /// Check protocol version compatibility.
    pub const fn check_compatibility(&self) -> bool {
        self.current_protocol.is_compatible(&self.target_protocol)
    }

    /// Plan the upgrade: sort nodes by upgrade order.
    pub fn plan_upgrade(&self, nodes: Vec<(NodeId, UpgradeOrder, String)>) {
        let mut records: Vec<UpgradeNodeRecord> = nodes.into_iter()
            .map(|(id, order, from_ver)| UpgradeNodeRecord {
                node_id: id,
                order,
                from_version: from_ver,
                to_version: self.target_version.clone(),
                state: UpgradeNodeState::Pending,
                started: None,
            })
            .collect();
        records.sort_by_key(|r| r.order);
        *self.nodes.write() = records;
    }

    /// Get the next node to upgrade.
    pub fn next_node(&self) -> Option<NodeId> {
        self.nodes.read().iter()
            .find(|r| r.state == UpgradeNodeState::Pending)
            .map(|r| r.node_id)
    }

    /// Start upgrading a node.
    pub fn start_node_upgrade(&self, node_id: NodeId) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(record) = nodes.iter_mut().find(|r| r.node_id == node_id) {
            record.state = UpgradeNodeState::Draining;
            record.started = Some(Instant::now());
            return true;
        }
        false
    }

    /// Mark a node as upgrading (binary replaced).
    pub fn mark_upgrading(&self, node_id: NodeId) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(record) = nodes.iter_mut().find(|r| r.node_id == node_id) {
            record.state = UpgradeNodeState::Upgrading;
            return true;
        }
        false
    }

    /// Mark a node as rejoining.
    pub fn mark_rejoining(&self, node_id: NodeId) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(record) = nodes.iter_mut().find(|r| r.node_id == node_id) {
            record.state = UpgradeNodeState::Rejoining;
            return true;
        }
        false
    }

    /// Mark a node upgrade as complete.
    pub fn mark_complete(&self, node_id: NodeId) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(record) = nodes.iter_mut().find(|r| r.node_id == node_id) {
            record.state = UpgradeNodeState::Complete;
            self.metrics.nodes_upgraded.fetch_add(1, Ordering::Relaxed);
            if let Some(started) = record.started {
                self.metrics.total_rejoin_time_ms.fetch_add(
                    started.elapsed().as_millis() as u64,
                    Ordering::Relaxed,
                );
            }
            return true;
        }
        false
    }

    /// Mark a node upgrade as failed.
    pub fn mark_failed(&self, node_id: NodeId, reason: String) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(record) = nodes.iter_mut().find(|r| r.node_id == node_id) {
            record.state = UpgradeNodeState::Failed(reason);
            self.metrics.nodes_failed.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Check if upgrade is complete.
    pub fn is_complete(&self) -> bool {
        self.nodes.read().iter().all(|r| r.state == UpgradeNodeState::Complete)
    }

    /// Get upgrade plan snapshot.
    pub fn plan_snapshot(&self) -> Vec<UpgradeNodeRecord> {
        self.nodes.read().clone()
    }

    /// Progress: (completed, total).
    pub fn progress(&self) -> (usize, usize) {
        let nodes = self.nodes.read();
        let total = nodes.len();
        let done = nodes.iter().filter(|r| r.state == UpgradeNodeState::Complete).count();
        (done, total)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Node Drain & Join
// ═══════════════════════════════════════════════════════════════════════════

/// Drain state for a single node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainPhase {
    /// Not draining.
    Active,
    /// Stopped accepting new requests.
    RejectingNew,
    /// Waiting for in-flight requests to complete.
    DrainingInflight,
    /// Transferring leader/shard ownership.
    TransferringShards,
    /// Fully drained, safe to stop.
    Drained,
}

impl fmt::Display for DrainPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "ACTIVE"),
            Self::RejectingNew => write!(f, "REJECTING_NEW"),
            Self::DrainingInflight => write!(f, "DRAINING_INFLIGHT"),
            Self::TransferringShards => write!(f, "TRANSFERRING_SHARDS"),
            Self::Drained => write!(f, "DRAINED"),
        }
    }
}

/// Per-node drain record.
#[derive(Debug, Clone)]
pub struct NodeDrainState {
    pub node_id: NodeId,
    pub phase: DrainPhase,
    pub inflight_remaining: u64,
    pub shards_to_transfer: Vec<u64>,
    pub shards_transferred: Vec<u64>,
    pub started: Option<Instant>,
    pub completed: Option<Instant>,
}

/// Join state for a node entering the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinPhase {
    /// Requesting to join.
    Requesting,
    /// Accepted, syncing data.
    Syncing,
    /// Data synced, ready to serve.
    Ready,
    /// Fully active.
    Active,
    /// Join failed.
    Failed(String),
}

impl fmt::Display for JoinPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Requesting => write!(f, "REQUESTING"),
            Self::Syncing => write!(f, "SYNCING"),
            Self::Ready => write!(f, "READY"),
            Self::Active => write!(f, "ACTIVE"),
            Self::Failed(r) => write!(f, "FAILED: {r}"),
        }
    }
}

/// Per-node join record.
#[derive(Debug, Clone)]
pub struct NodeJoinState {
    pub node_id: NodeId,
    pub phase: JoinPhase,
    pub shards_assigned: Vec<u64>,
    pub shards_synced: Vec<u64>,
    pub started: Option<Instant>,
}

/// Node lifecycle coordinator — manages drain and join operations.
pub struct NodeLifecycleCoordinator {
    drains: RwLock<HashMap<NodeId, NodeDrainState>>,
    joins: RwLock<HashMap<NodeId, NodeJoinState>>,
    pub metrics: LifecycleMetrics,
}

#[derive(Debug, Default)]
pub struct LifecycleMetrics {
    pub drains_started: AtomicU64,
    pub drains_completed: AtomicU64,
    pub joins_started: AtomicU64,
    pub joins_completed: AtomicU64,
    pub shards_transferred: AtomicU64,
}

impl Default for NodeLifecycleCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeLifecycleCoordinator {
    pub fn new() -> Self {
        Self {
            drains: RwLock::new(HashMap::new()),
            joins: RwLock::new(HashMap::new()),
            metrics: LifecycleMetrics::default(),
        }
    }

    /// Start draining a node.
    pub fn start_drain(&self, node_id: NodeId, owned_shards: Vec<u64>) -> bool {
        let mut drains = self.drains.write();
        if drains.contains_key(&node_id) {
            return false; // Already draining
        }
        drains.insert(node_id, NodeDrainState {
            node_id,
            phase: DrainPhase::RejectingNew,
            inflight_remaining: 0,
            shards_to_transfer: owned_shards,
            shards_transferred: Vec::new(),
            started: Some(Instant::now()),
            completed: None,
        });
        self.metrics.drains_started.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Update drain progress.
    pub fn update_drain(&self, node_id: NodeId, inflight: u64) {
        let mut drains = self.drains.write();
        if let Some(state) = drains.get_mut(&node_id) {
            state.inflight_remaining = inflight;
            if state.phase == DrainPhase::RejectingNew && inflight == 0 {
                state.phase = DrainPhase::DrainingInflight;
            }
            if state.phase == DrainPhase::DrainingInflight && inflight == 0 {
                if state.shards_to_transfer.is_empty() {
                    state.phase = DrainPhase::Drained;
                    state.completed = Some(Instant::now());
                    self.metrics.drains_completed.fetch_add(1, Ordering::Relaxed);
                } else {
                    state.phase = DrainPhase::TransferringShards;
                }
            }
        }
    }

    /// Record a shard transfer completion.
    pub fn record_shard_transferred(&self, node_id: NodeId, shard_id: u64) {
        let mut drains = self.drains.write();
        if let Some(state) = drains.get_mut(&node_id) {
            state.shards_to_transfer.retain(|s| *s != shard_id);
            state.shards_transferred.push(shard_id);
            self.metrics.shards_transferred.fetch_add(1, Ordering::Relaxed);
            if state.shards_to_transfer.is_empty() && state.inflight_remaining == 0 {
                state.phase = DrainPhase::Drained;
                state.completed = Some(Instant::now());
                self.metrics.drains_completed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Check if a node is fully drained.
    pub fn is_drained(&self, node_id: NodeId) -> bool {
        self.drains.read().get(&node_id)
            .is_some_and(|s| s.phase == DrainPhase::Drained)
    }

    /// Get drain state.
    pub fn get_drain(&self, node_id: NodeId) -> Option<NodeDrainState> {
        self.drains.read().get(&node_id).cloned()
    }

    /// Start a node join.
    pub fn start_join(&self, node_id: NodeId, shards: Vec<u64>) -> bool {
        let mut joins = self.joins.write();
        if joins.contains_key(&node_id) {
            return false;
        }
        joins.insert(node_id, NodeJoinState {
            node_id,
            phase: JoinPhase::Syncing,
            shards_assigned: shards,
            shards_synced: Vec::new(),
            started: Some(Instant::now()),
        });
        self.metrics.joins_started.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Record a shard sync completion during join.
    pub fn record_shard_synced(&self, node_id: NodeId, shard_id: u64) {
        let mut joins = self.joins.write();
        if let Some(state) = joins.get_mut(&node_id) {
            if !state.shards_synced.contains(&shard_id) {
                state.shards_synced.push(shard_id);
            }
            if state.shards_synced.len() >= state.shards_assigned.len() {
                state.phase = JoinPhase::Ready;
            }
        }
    }

    /// Activate a joined node.
    pub fn activate_join(&self, node_id: NodeId) -> bool {
        let mut joins = self.joins.write();
        if let Some(state) = joins.get_mut(&node_id) {
            if state.phase == JoinPhase::Ready {
                state.phase = JoinPhase::Active;
                self.metrics.joins_completed.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Get join state.
    pub fn get_join(&self, node_id: NodeId) -> Option<NodeJoinState> {
        self.joins.read().get(&node_id).cloned()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — SLA/SLO Metrics
// ═══════════════════════════════════════════════════════════════════════════

/// SLO metric snapshot — the output of /admin/slo.
#[derive(Debug, Clone)]
pub struct SloSnapshot {
    pub write_availability: f64,
    pub read_availability: f64,
    pub write_p99_us: u64,
    pub read_p99_us: u64,
    pub replication_lag_max_lsn: u64,
    pub gateway_reject_rate: f64,
    pub window_secs: u64,
    pub timestamp: u64,
}

/// SLO tracker — accumulates metrics over a sliding window.
pub struct SloTracker {
    /// Window size in seconds.
    window_secs: u64,
    // Write metrics
    write_total: AtomicU64,
    write_success: AtomicU64,
    write_latencies: Mutex<Vec<u64>>,
    // Read metrics
    read_total: AtomicU64,
    read_success: AtomicU64,
    read_latencies: Mutex<Vec<u64>>,
    // Gateway metrics
    gateway_total: AtomicU64,
    gateway_rejected: AtomicU64,
    // Replication
    max_replication_lag: AtomicU64,
    // Latency window capacity
    latency_capacity: usize,
}

impl SloTracker {
    pub fn new(window_secs: u64) -> Self {
        Self {
            window_secs,
            write_total: AtomicU64::new(0),
            write_success: AtomicU64::new(0),
            write_latencies: Mutex::new(Vec::with_capacity(10_000)),
            read_total: AtomicU64::new(0),
            read_success: AtomicU64::new(0),
            read_latencies: Mutex::new(Vec::with_capacity(10_000)),
            gateway_total: AtomicU64::new(0),
            gateway_rejected: AtomicU64::new(0),
            max_replication_lag: AtomicU64::new(0),
            latency_capacity: 10_000,
        }
    }

    pub fn record_write(&self, latency_us: u64, success: bool) {
        self.write_total.fetch_add(1, Ordering::Relaxed);
        if success {
            self.write_success.fetch_add(1, Ordering::Relaxed);
        }
        let mut lats = self.write_latencies.lock();
        if lats.len() >= self.latency_capacity {
            lats.remove(0);
        }
        lats.push(latency_us);
    }

    pub fn record_read(&self, latency_us: u64, success: bool) {
        self.read_total.fetch_add(1, Ordering::Relaxed);
        if success {
            self.read_success.fetch_add(1, Ordering::Relaxed);
        }
        let mut lats = self.read_latencies.lock();
        if lats.len() >= self.latency_capacity {
            lats.remove(0);
        }
        lats.push(latency_us);
    }

    pub fn record_gateway_request(&self, rejected: bool) {
        self.gateway_total.fetch_add(1, Ordering::Relaxed);
        if rejected {
            self.gateway_rejected.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn update_replication_lag(&self, lag: u64) {
        self.max_replication_lag.fetch_max(lag, Ordering::Relaxed);
    }

    fn p99(sorted: &[u64]) -> u64 {
        if sorted.is_empty() { return 0; }
        let idx = ((sorted.len() as f64 * 0.99).ceil() as usize).saturating_sub(1);
        sorted[idx.min(sorted.len() - 1)]
    }

    pub fn snapshot(&self) -> SloSnapshot {
        let wt = self.write_total.load(Ordering::Relaxed);
        let ws = self.write_success.load(Ordering::Relaxed);
        let rt = self.read_total.load(Ordering::Relaxed);
        let rs = self.read_success.load(Ordering::Relaxed);
        let gt = self.gateway_total.load(Ordering::Relaxed);
        let gr = self.gateway_rejected.load(Ordering::Relaxed);

        let write_avail = if wt > 0 { ws as f64 / wt as f64 } else { 1.0 };
        let read_avail = if rt > 0 { rs as f64 / rt as f64 } else { 1.0 };
        let gw_reject = if gt > 0 { gr as f64 / gt as f64 } else { 0.0 };

        let wp99 = {
            let mut lats = self.write_latencies.lock().clone();
            lats.sort_unstable();
            Self::p99(&lats)
        };
        let rp99 = {
            let mut lats = self.read_latencies.lock().clone();
            lats.sort_unstable();
            Self::p99(&lats)
        };

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        SloSnapshot {
            write_availability: write_avail,
            read_availability: read_avail,
            write_p99_us: wp99,
            read_p99_us: rp99,
            replication_lag_max_lsn: self.max_replication_lag.load(Ordering::Relaxed),
            gateway_reject_rate: gw_reject,
            window_secs: self.window_secs,
            timestamp: now,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Backpressure v2 (multi-signal)
// ═══════════════════════════════════════════════════════════════════════════

/// Pressure signal — which subsystem is reporting pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PressureSignal {
    Cpu,
    Memory,
    WalBacklog,
    ReplicationLag,
    Inflight,
}

impl fmt::Display for PressureSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cpu => write!(f, "CPU"),
            Self::Memory => write!(f, "MEMORY"),
            Self::WalBacklog => write!(f, "WAL_BACKLOG"),
            Self::ReplicationLag => write!(f, "REPLICATION_LAG"),
            Self::Inflight => write!(f, "INFLIGHT"),
        }
    }
}

/// Pressure level — how severe the pressure is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PressureLevel {
    Normal,
    Elevated,
    High,
    Critical,
}

impl fmt::Display for PressureLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Normal => write!(f, "NORMAL"),
            Self::Elevated => write!(f, "ELEVATED"),
            Self::High => write!(f, "HIGH"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Backpressure v2 controller — multi-signal, auto-adjusting.
pub struct BackpressureController {
    pub config: BackpressureConfig,
    signals: RwLock<HashMap<PressureSignal, f64>>,
    level: RwLock<PressureLevel>,
    /// Admission rate multiplier (0.0 = reject all, 1.0 = accept all).
    admission_rate: RwLock<f64>,
    pub metrics: BackpressureMetrics,
}

#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// CPU usage threshold for ELEVATED (fraction, 0.0–1.0).
    pub cpu_elevated: f64,
    /// CPU usage threshold for HIGH.
    pub cpu_high: f64,
    /// CPU usage threshold for CRITICAL.
    pub cpu_critical: f64,
    /// Memory usage threshold for ELEVATED (fraction).
    pub memory_elevated: f64,
    pub memory_high: f64,
    pub memory_critical: f64,
    /// WAL backlog threshold in bytes for ELEVATED.
    pub wal_backlog_elevated: u64,
    pub wal_backlog_high: u64,
    pub wal_backlog_critical: u64,
    /// Replication lag in LSNs for ELEVATED.
    pub replication_lag_elevated: u64,
    pub replication_lag_high: u64,
    pub replication_lag_critical: u64,
    /// Admission rate reduction per level.
    pub rate_normal: f64,
    pub rate_elevated: f64,
    pub rate_high: f64,
    pub rate_critical: f64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            cpu_elevated: 0.7, cpu_high: 0.85, cpu_critical: 0.95,
            memory_elevated: 0.7, memory_high: 0.85, memory_critical: 0.95,
            wal_backlog_elevated: 10_000_000,
            wal_backlog_high: 50_000_000,
            wal_backlog_critical: 100_000_000,
            replication_lag_elevated: 1000,
            replication_lag_high: 10_000,
            replication_lag_critical: 100_000,
            rate_normal: 1.0,
            rate_elevated: 0.8,
            rate_high: 0.5,
            rate_critical: 0.1,
        }
    }
}

#[derive(Debug, Default)]
pub struct BackpressureMetrics {
    pub evaluations: AtomicU64,
    pub level_changes: AtomicU64,
    pub requests_throttled: AtomicU64,
    pub requests_admitted: AtomicU64,
}

impl BackpressureController {
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            config,
            signals: RwLock::new(HashMap::new()),
            level: RwLock::new(PressureLevel::Normal),
            admission_rate: RwLock::new(1.0),
            metrics: BackpressureMetrics::default(),
        }
    }

    /// Update a pressure signal.
    pub fn update_signal(&self, signal: PressureSignal, value: f64) {
        self.signals.write().insert(signal, value);
    }

    /// Evaluate all signals and compute overall pressure level.
    pub fn evaluate(&self) -> PressureLevel {
        self.metrics.evaluations.fetch_add(1, Ordering::Relaxed);
        let signals = self.signals.read();
        let c = &self.config;

        let mut max_level = PressureLevel::Normal;

        if let Some(&cpu) = signals.get(&PressureSignal::Cpu) {
            let lvl = if cpu >= c.cpu_critical { PressureLevel::Critical }
                else if cpu >= c.cpu_high { PressureLevel::High }
                else if cpu >= c.cpu_elevated { PressureLevel::Elevated }
                else { PressureLevel::Normal };
            if lvl > max_level { max_level = lvl; }
        }
        if let Some(&mem) = signals.get(&PressureSignal::Memory) {
            let lvl = if mem >= c.memory_critical { PressureLevel::Critical }
                else if mem >= c.memory_high { PressureLevel::High }
                else if mem >= c.memory_elevated { PressureLevel::Elevated }
                else { PressureLevel::Normal };
            if lvl > max_level { max_level = lvl; }
        }
        if let Some(&wal) = signals.get(&PressureSignal::WalBacklog) {
            let wal_bytes = wal as u64;
            let lvl = if wal_bytes >= c.wal_backlog_critical { PressureLevel::Critical }
                else if wal_bytes >= c.wal_backlog_high { PressureLevel::High }
                else if wal_bytes >= c.wal_backlog_elevated { PressureLevel::Elevated }
                else { PressureLevel::Normal };
            if lvl > max_level { max_level = lvl; }
        }
        if let Some(&lag) = signals.get(&PressureSignal::ReplicationLag) {
            let lag_lsn = lag as u64;
            let lvl = if lag_lsn >= c.replication_lag_critical { PressureLevel::Critical }
                else if lag_lsn >= c.replication_lag_high { PressureLevel::High }
                else if lag_lsn >= c.replication_lag_elevated { PressureLevel::Elevated }
                else { PressureLevel::Normal };
            if lvl > max_level { max_level = lvl; }
        }

        // Update level and admission rate
        let old_level = *self.level.read();
        *self.level.write() = max_level;
        let rate = match max_level {
            PressureLevel::Normal => c.rate_normal,
            PressureLevel::Elevated => c.rate_elevated,
            PressureLevel::High => c.rate_high,
            PressureLevel::Critical => c.rate_critical,
        };
        *self.admission_rate.write() = rate;
        if old_level != max_level {
            self.metrics.level_changes.fetch_add(1, Ordering::Relaxed);
        }

        max_level
    }

    /// Should a request be admitted? Uses admission rate as probabilistic gate.
    pub fn should_admit(&self, request_hash: u64) -> bool {
        let rate = *self.admission_rate.read();
        if rate >= 1.0 {
            self.metrics.requests_admitted.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        if rate <= 0.0 {
            self.metrics.requests_throttled.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        // Deterministic: hash mod 1000 < rate * 1000
        let threshold = (rate * 1000.0) as u64;
        let admitted = (request_hash % 1000) < threshold;
        if admitted {
            self.metrics.requests_admitted.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.requests_throttled.fetch_add(1, Ordering::Relaxed);
        }
        admitted
    }

    /// Current pressure level.
    pub fn current_level(&self) -> PressureLevel {
        *self.level.read()
    }

    /// Current admission rate (0.0–1.0).
    pub fn current_rate(&self) -> f64 {
        *self.admission_rate.read()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 — falconctl Ops Model
// ═══════════════════════════════════════════════════════════════════════════

/// Ops command — what falconctl can do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpsCommand {
    ClusterStatus,
    ClusterHealth,
    NodeDrain(NodeId),
    NodeJoin(NodeId),
    UpgradePlan,
    UpgradeApply,
}

impl fmt::Display for OpsCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClusterStatus => write!(f, "cluster status"),
            Self::ClusterHealth => write!(f, "cluster health"),
            Self::NodeDrain(id) => write!(f, "node drain {id:?}"),
            Self::NodeJoin(id) => write!(f, "node join {id:?}"),
            Self::UpgradePlan => write!(f, "upgrade plan"),
            Self::UpgradeApply => write!(f, "upgrade apply"),
        }
    }
}

/// Cluster status response.
#[derive(Debug, Clone)]
pub struct ClusterStatusResponse {
    pub total_nodes: usize,
    pub alive_nodes: usize,
    pub suspect_nodes: usize,
    pub dead_nodes: usize,
    pub draining_nodes: usize,
    pub joining_nodes: usize,
    pub total_shards: usize,
    pub leader_shards: usize,
    pub election_term: Term,
    pub epoch: u64,
    pub slo: SloSnapshot,
}

/// Cluster health response (more detailed).
#[derive(Debug, Clone)]
pub struct ClusterHealthResponse {
    pub overall: ClusterHealthLevel,
    pub nodes: Vec<NodeHealthRecord>,
    pub pressure: PressureLevel,
    pub slo: SloSnapshot,
    pub active_elections: usize,
    pub active_catchups: usize,
    pub active_drains: usize,
    pub active_joins: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterHealthLevel {
    Healthy,
    Degraded,
    Critical,
}

impl fmt::Display for ClusterHealthLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Healthy => write!(f, "HEALTHY"),
            Self::Degraded => write!(f, "DEGRADED"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §9 — Ops Audit Log
// ═══════════════════════════════════════════════════════════════════════════

/// Ops event type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpsEventType {
    NodeUp(NodeId),
    NodeDown(NodeId),
    NodeSuspect(NodeId),
    LeaderElected { shard_id: u64, leader: NodeId, term: Term },
    LeaderLost { shard_id: u64, old_leader: NodeId },
    DrainStarted(NodeId),
    DrainCompleted(NodeId),
    JoinStarted(NodeId),
    JoinCompleted(NodeId),
    UpgradeStarted { target_version: String },
    UpgradeNodeComplete { node_id: NodeId, version: String },
    UpgradeComplete { version: String },
    ConfigChanged { key: String, old_value: String, new_value: String },
    BackpressureChanged { old_level: PressureLevel, new_level: PressureLevel },
    CatchUpStarted { node_id: NodeId, shard_id: u64, strategy: ReplicaLagClass },
    CatchUpCompleted { node_id: NodeId, shard_id: u64 },
}

impl fmt::Display for OpsEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NodeUp(id) => write!(f, "NODE_UP {id:?}"),
            Self::NodeDown(id) => write!(f, "NODE_DOWN {id:?}"),
            Self::NodeSuspect(id) => write!(f, "NODE_SUSPECT {id:?}"),
            Self::LeaderElected { shard_id, leader, term } =>
                write!(f, "LEADER_ELECTED shard={shard_id} leader={leader:?} term={term}"),
            Self::LeaderLost { shard_id, old_leader } =>
                write!(f, "LEADER_LOST shard={shard_id} old_leader={old_leader:?}"),
            Self::DrainStarted(id) => write!(f, "DRAIN_STARTED {id:?}"),
            Self::DrainCompleted(id) => write!(f, "DRAIN_COMPLETED {id:?}"),
            Self::JoinStarted(id) => write!(f, "JOIN_STARTED {id:?}"),
            Self::JoinCompleted(id) => write!(f, "JOIN_COMPLETED {id:?}"),
            Self::UpgradeStarted { target_version } =>
                write!(f, "UPGRADE_STARTED target={target_version}"),
            Self::UpgradeNodeComplete { node_id, version } =>
                write!(f, "UPGRADE_NODE_COMPLETE {node_id:?} v={version}"),
            Self::UpgradeComplete { version } =>
                write!(f, "UPGRADE_COMPLETE v={version}"),
            Self::ConfigChanged { key, old_value, new_value } =>
                write!(f, "CONFIG_CHANGED {key}={old_value}->{new_value}"),
            Self::BackpressureChanged { old_level, new_level } =>
                write!(f, "BACKPRESSURE_CHANGED {old_level}→{new_level}"),
            Self::CatchUpStarted { node_id, shard_id, strategy } =>
                write!(f, "CATCHUP_STARTED {node_id:?} shard={shard_id} strategy={strategy}"),
            Self::CatchUpCompleted { node_id, shard_id } =>
                write!(f, "CATCHUP_COMPLETED {node_id:?} shard={shard_id}"),
        }
    }
}

/// Ops audit event.
#[derive(Debug, Clone)]
pub struct OpsAuditEvent {
    pub id: u64,
    pub timestamp: u64,
    pub event_type: OpsEventType,
    pub initiator: String,
}

/// Ops audit log — ring buffer of cluster operations.
pub struct OpsAuditLog {
    events: Mutex<VecDeque<OpsAuditEvent>>,
    capacity: usize,
    next_id: AtomicU64,
}

impl OpsAuditLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            events: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            next_id: AtomicU64::new(1),
        }
    }

    /// Record an ops event.
    pub fn record(&self, event_type: OpsEventType, initiator: &str) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let event = OpsAuditEvent {
            id,
            timestamp,
            event_type,
            initiator: initiator.to_owned(),
        };
        let mut events = self.events.lock();
        if events.len() >= self.capacity {
            events.pop_front();
        }
        events.push_back(event);
    }

    /// Get recent events (last N).
    pub fn recent(&self, count: usize) -> Vec<OpsAuditEvent> {
        let events = self.events.lock();
        events.iter().rev().take(count).cloned().collect()
    }

    /// Get all events.
    pub fn all(&self) -> Vec<OpsAuditEvent> {
        self.events.lock().iter().cloned().collect()
    }

    /// Total events recorded.
    pub fn total(&self) -> u64 {
        self.next_id.load(Ordering::Relaxed) - 1
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §10 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // -- Failure Detector Tests --

    #[test]
    fn test_node_registration_and_heartbeat() {
        let fd = ClusterFailureDetector::new(FailureDetectorConfig::default());
        fd.register_node(NodeId(1), "1.0.9".into());
        fd.register_node(NodeId(2), "1.0.9".into());
        assert_eq!(fd.alive_count(), 2);
        fd.record_heartbeat(NodeId(1), 100, 60);
        let snap = fd.snapshot();
        let n1 = snap.iter().find(|r| r.node_id == NodeId(1)).unwrap();
        assert_eq!(n1.liveness, NodeLiveness::Alive);
        assert_eq!(n1.last_lsn, 100);
    }

    #[test]
    fn test_suspect_transition() {
        let config = FailureDetectorConfig {
            suspect_threshold: Duration::from_millis(20),
            failure_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let fd = ClusterFailureDetector::new(config);
        fd.register_node(NodeId(1), "1.0.9".into());
        fd.record_heartbeat(NodeId(1), 0, 0);
        std::thread::sleep(Duration::from_millis(30));
        let transitions = fd.evaluate();
        assert!(!transitions.is_empty());
        assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Suspect));
    }

    #[test]
    fn test_dead_transition() {
        let config = FailureDetectorConfig {
            suspect_threshold: Duration::from_millis(10),
            failure_timeout: Duration::from_millis(30),
            ..Default::default()
        };
        let fd = ClusterFailureDetector::new(config);
        fd.register_node(NodeId(1), "1.0.9".into());
        fd.record_heartbeat(NodeId(1), 0, 0);
        std::thread::sleep(Duration::from_millis(40));
        fd.evaluate();
        assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Dead));
        assert_eq!(fd.dead_nodes(), vec![NodeId(1)]);
    }

    #[test]
    fn test_heartbeat_restores_from_dead() {
        let config = FailureDetectorConfig {
            suspect_threshold: Duration::from_millis(10),
            failure_timeout: Duration::from_millis(30),
            ..Default::default()
        };
        let fd = ClusterFailureDetector::new(config);
        fd.register_node(NodeId(1), "1.0.9".into());
        fd.record_heartbeat(NodeId(1), 0, 0);
        std::thread::sleep(Duration::from_millis(40));
        fd.evaluate();
        assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Dead));
        // Heartbeat restores
        fd.record_heartbeat(NodeId(1), 100, 60);
        assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Alive));
    }

    #[test]
    fn test_drain_and_join_lifecycle() {
        let fd = ClusterFailureDetector::new(FailureDetectorConfig::default());
        fd.register_node(NodeId(1), "1.0.9".into());
        assert!(fd.mark_draining(NodeId(1)));
        assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Draining));
        // Draining skipped by evaluate
        fd.evaluate();
        assert_eq!(fd.get_liveness(NodeId(1)), Some(NodeLiveness::Draining));
    }

    #[test]
    fn test_epoch_bumps_on_transitions() {
        let config = FailureDetectorConfig {
            suspect_threshold: Duration::from_millis(10),
            failure_timeout: Duration::from_millis(30),
            ..Default::default()
        };
        let fd = ClusterFailureDetector::new(config);
        let e0 = fd.epoch();
        fd.register_node(NodeId(1), "1.0.9".into());
        fd.record_heartbeat(NodeId(1), 0, 0);
        std::thread::sleep(Duration::from_millis(40));
        fd.evaluate();
        assert!(fd.epoch() > e0, "epoch should bump on dead transition");
    }

    // -- Election Tests --

    #[test]
    fn test_election_happy_path() {
        let coord = LeaderElectionCoordinator::new(ElectionConfig {
            require_quorum: false,
            ..Default::default()
        });
        coord.register_shard(0, Some(NodeId(1)), 3);
        assert_eq!(coord.get_leader(0), Some(NodeId(1)));

        // Leader dies → start election
        let term = coord.start_election(0).unwrap();
        assert_eq!(coord.get_state(0), Some(ElectionState::Electing));

        // Candidates submit
        coord.submit_candidate(0, NodeId(2), 100, term);
        coord.submit_candidate(0, NodeId(3), 200, term);

        // Resolve — highest LSN wins
        let winner = coord.resolve_election(0).unwrap();
        assert_eq!(winner, NodeId(3));
        assert_eq!(coord.get_leader(0), Some(NodeId(3)));
        assert_eq!(coord.get_state(0), Some(ElectionState::Stable));
    }

    #[test]
    fn test_election_quorum_required() {
        let coord = LeaderElectionCoordinator::new(ElectionConfig::default());
        coord.register_shard(0, None, 3); // quorum = 2
        let term = coord.start_election(0).unwrap();
        coord.submit_candidate(0, NodeId(1), 100, term);
        // Only 1 candidate, need 2 for quorum
        let result = coord.resolve_election(0);
        assert!(matches!(result, Err(ElectionError::NoQuorum { .. })));
    }

    #[test]
    fn test_election_no_candidates() {
        let coord = LeaderElectionCoordinator::new(ElectionConfig {
            require_quorum: false,
            ..Default::default()
        });
        coord.register_shard(0, None, 3);
        coord.start_election(0);
        let result = coord.resolve_election(0);
        assert!(matches!(result, Err(ElectionError::NoCandidates(_))));
    }

    #[test]
    fn test_term_monotonicity() {
        let coord = LeaderElectionCoordinator::new(ElectionConfig::default());
        coord.register_shard(0, None, 3);
        let t1 = coord.start_election(0).unwrap();
        let t2 = coord.start_election(0).unwrap();
        assert!(t2 > t1);
        assert!(coord.validate_term(t2));
        assert!(!coord.validate_term(0));
    }

    #[test]
    fn test_stale_term_candidate_rejected() {
        let coord = LeaderElectionCoordinator::new(ElectionConfig::default());
        coord.register_shard(0, None, 3);
        let t1 = coord.start_election(0).unwrap();
        let _t2 = coord.start_election(0).unwrap();
        // Submit with stale term
        assert!(!coord.submit_candidate(0, NodeId(1), 100, t1));
    }

    // -- Catch-up Tests --

    #[test]
    fn test_lag_classification() {
        let coord = ReplicaCatchUpCoordinator::new(CatchUpConfig::default());
        assert_eq!(coord.classify_lag(100, 100), ReplicaLagClass::CaughtUp);
        assert_eq!(coord.classify_lag(1000, 500), ReplicaLagClass::WalReplay);
        assert_eq!(coord.classify_lag(200_000, 0), ReplicaLagClass::SnapshotRequired);
    }

    #[test]
    fn test_catch_up_wal_replay() {
        let coord = ReplicaCatchUpCoordinator::new(CatchUpConfig::default());
        let lag = coord.start_catch_up(NodeId(1), 0, 1000, 500);
        assert_eq!(lag, ReplicaLagClass::WalReplay);
        let state = coord.get_state(NodeId(1), 0).unwrap();
        assert_eq!(state.phase, CatchUpPhase::StreamingWal);
    }

    #[test]
    fn test_catch_up_progress_to_complete() {
        let coord = ReplicaCatchUpCoordinator::new(CatchUpConfig::default());
        coord.start_catch_up(NodeId(1), 0, 1000, 500);
        coord.update_progress(NodeId(1), 0, 1000, 5000);
        let state = coord.get_state(NodeId(1), 0).unwrap();
        assert_eq!(state.phase, CatchUpPhase::Complete);
        assert_eq!(state.progress_pct, 100.0);
    }

    // -- Rolling Upgrade Tests --

    #[test]
    fn test_protocol_version_compatibility() {
        let v1 = ProtocolVersion::new(1, 9);
        let v2 = ProtocolVersion::new(1, 10);
        let v3 = ProtocolVersion::new(1, 11);
        let v4 = ProtocolVersion::new(2, 0);
        assert!(v1.is_compatible(&v2));
        assert!(v2.is_compatible(&v1));
        assert!(!v1.is_compatible(&v3)); // 2 steps apart
        assert!(!v1.is_compatible(&v4)); // different major
    }

    #[test]
    fn test_upgrade_ordering() {
        let coord = RollingUpgradeCoordinator::new(
            "1.0.10".into(),
            ProtocolVersion::new(1, 9),
            ProtocolVersion::new(1, 10),
        );
        assert!(coord.check_compatibility());
        coord.plan_upgrade(vec![
            (NodeId(3), UpgradeOrder::Leader, "1.0.9".into()),
            (NodeId(1), UpgradeOrder::Follower, "1.0.9".into()),
            (NodeId(2), UpgradeOrder::Gateway, "1.0.9".into()),
        ]);
        let plan = coord.plan_snapshot();
        assert_eq!(plan[0].order, UpgradeOrder::Follower);
        assert_eq!(plan[1].order, UpgradeOrder::Gateway);
        assert_eq!(plan[2].order, UpgradeOrder::Leader);
    }

    #[test]
    fn test_upgrade_lifecycle() {
        let coord = RollingUpgradeCoordinator::new(
            "1.0.10".into(),
            ProtocolVersion::new(1, 9),
            ProtocolVersion::new(1, 10),
        );
        coord.plan_upgrade(vec![
            (NodeId(1), UpgradeOrder::Follower, "1.0.9".into()),
            (NodeId(2), UpgradeOrder::Leader, "1.0.9".into()),
        ]);
        assert_eq!(coord.next_node(), Some(NodeId(1)));
        coord.start_node_upgrade(NodeId(1));
        coord.mark_upgrading(NodeId(1));
        coord.mark_rejoining(NodeId(1));
        coord.mark_complete(NodeId(1));
        assert_eq!(coord.next_node(), Some(NodeId(2)));
        assert_eq!(coord.progress(), (1, 2));
    }

    // -- Drain/Join Tests --

    #[test]
    fn test_drain_lifecycle() {
        let lc = NodeLifecycleCoordinator::new();
        assert!(lc.start_drain(NodeId(1), vec![0, 1, 2]));
        assert!(!lc.start_drain(NodeId(1), vec![])); // duplicate

        let state = lc.get_drain(NodeId(1)).unwrap();
        assert_eq!(state.phase, DrainPhase::RejectingNew);
        assert_eq!(state.shards_to_transfer.len(), 3);

        lc.update_drain(NodeId(1), 0);
        lc.record_shard_transferred(NodeId(1), 0);
        lc.record_shard_transferred(NodeId(1), 1);
        lc.record_shard_transferred(NodeId(1), 2);

        assert!(lc.is_drained(NodeId(1)));
    }

    #[test]
    fn test_join_lifecycle() {
        let lc = NodeLifecycleCoordinator::new();
        assert!(lc.start_join(NodeId(5), vec![0, 1]));
        let state = lc.get_join(NodeId(5)).unwrap();
        assert_eq!(state.phase, JoinPhase::Syncing);

        lc.record_shard_synced(NodeId(5), 0);
        lc.record_shard_synced(NodeId(5), 1);
        let state = lc.get_join(NodeId(5)).unwrap();
        assert_eq!(state.phase, JoinPhase::Ready);

        assert!(lc.activate_join(NodeId(5)));
        let state = lc.get_join(NodeId(5)).unwrap();
        assert_eq!(state.phase, JoinPhase::Active);
    }

    // -- SLO Tests --

    #[test]
    fn test_slo_tracker() {
        let tracker = SloTracker::new(60);
        for i in 0..100 {
            tracker.record_write(i * 10, true);
            tracker.record_read(i * 5, true);
        }
        tracker.record_write(1000, false); // one failure
        tracker.record_gateway_request(false);
        tracker.record_gateway_request(true);
        tracker.update_replication_lag(50);

        let snap = tracker.snapshot();
        assert!(snap.write_availability > 0.98);
        assert!(snap.read_availability > 0.99);
        assert!(snap.write_p99_us > 0);
        assert!(snap.read_p99_us > 0);
        assert_eq!(snap.replication_lag_max_lsn, 50);
        assert!(snap.gateway_reject_rate > 0.0);
    }

    // -- Backpressure Tests --

    #[test]
    fn test_backpressure_normal() {
        let bp = BackpressureController::new(BackpressureConfig::default());
        bp.update_signal(PressureSignal::Cpu, 0.3);
        bp.update_signal(PressureSignal::Memory, 0.4);
        let level = bp.evaluate();
        assert_eq!(level, PressureLevel::Normal);
        assert_eq!(bp.current_rate(), 1.0);
    }

    #[test]
    fn test_backpressure_elevated() {
        let bp = BackpressureController::new(BackpressureConfig::default());
        bp.update_signal(PressureSignal::Cpu, 0.75);
        let level = bp.evaluate();
        assert_eq!(level, PressureLevel::Elevated);
        assert_eq!(bp.current_rate(), 0.8);
    }

    #[test]
    fn test_backpressure_critical() {
        let bp = BackpressureController::new(BackpressureConfig::default());
        bp.update_signal(PressureSignal::Memory, 0.96);
        let level = bp.evaluate();
        assert_eq!(level, PressureLevel::Critical);
        assert_eq!(bp.current_rate(), 0.1);
    }

    #[test]
    fn test_backpressure_admission_deterministic() {
        let bp = BackpressureController::new(BackpressureConfig::default());
        bp.update_signal(PressureSignal::Cpu, 0.90);
        bp.evaluate(); // HIGH → rate 0.5
        let mut admitted = 0;
        for i in 0..1000u64 {
            if bp.should_admit(i) { admitted += 1; }
        }
        // ~50% should be admitted
        assert!(admitted > 400 && admitted < 600,
            "expected ~500 admitted, got {}", admitted);
    }

    #[test]
    fn test_backpressure_wal_backlog() {
        let bp = BackpressureController::new(BackpressureConfig::default());
        bp.update_signal(PressureSignal::WalBacklog, 60_000_000.0);
        let level = bp.evaluate();
        assert_eq!(level, PressureLevel::High);
    }

    #[test]
    fn test_backpressure_replication_lag() {
        let bp = BackpressureController::new(BackpressureConfig::default());
        bp.update_signal(PressureSignal::ReplicationLag, 200_000.0);
        let level = bp.evaluate();
        assert_eq!(level, PressureLevel::Critical);
    }

    // -- Ops Audit Log Tests --

    #[test]
    fn test_audit_log_record_and_retrieve() {
        let log = OpsAuditLog::new(100);
        log.record(OpsEventType::NodeUp(NodeId(1)), "system");
        log.record(OpsEventType::NodeDown(NodeId(2)), "failure_detector");
        log.record(OpsEventType::LeaderElected {
            shard_id: 0, leader: NodeId(1), term: 5
        }, "election_coordinator");

        assert_eq!(log.total(), 3);
        let recent = log.recent(2);
        assert_eq!(recent.len(), 2);
        // Most recent first
        assert!(matches!(recent[0].event_type, OpsEventType::LeaderElected { .. }));
    }

    #[test]
    fn test_audit_log_ring_buffer() {
        let log = OpsAuditLog::new(3);
        for i in 0..5 {
            log.record(OpsEventType::NodeUp(NodeId(i)), "test");
        }
        let all = log.all();
        assert_eq!(all.len(), 3); // Only last 3 kept
        assert_eq!(all[0].event_type, OpsEventType::NodeUp(NodeId(2)));
    }

    // -- Concurrent Safety Tests --

    #[test]
    fn test_failure_detector_concurrent() {
        let fd = Arc::new(ClusterFailureDetector::new(FailureDetectorConfig::default()));
        for i in 1..=10u64 {
            fd.register_node(NodeId(i), "1.0.9".into());
        }

        let mut handles = Vec::new();
        for t in 0..4 {
            let fd_clone = Arc::clone(&fd);
            handles.push(std::thread::spawn(move || {
                for i in 0..500 {
                    let node = NodeId(((t * 500 + i) % 10) as u64 + 1);
                    fd_clone.record_heartbeat(node, i as u64, i as u64);
                    if i % 50 == 0 {
                        fd_clone.evaluate();
                    }
                }
            }));
        }
        for h in handles {
            h.join().expect("thread must not panic");
        }
        assert!(fd.metrics.heartbeats_received.load(Ordering::Relaxed) == 2000);
    }

    #[test]
    fn test_backpressure_concurrent() {
        let bp = Arc::new(BackpressureController::new(BackpressureConfig::default()));
        let mut handles = Vec::new();
        for t in 0..4 {
            let bp_clone = Arc::clone(&bp);
            handles.push(std::thread::spawn(move || {
                for i in 0..500u64 {
                    bp_clone.update_signal(PressureSignal::Cpu, (i % 100) as f64 / 100.0);
                    bp_clone.evaluate();
                    bp_clone.should_admit(t * 500 + i);
                }
            }));
        }
        for h in handles {
            h.join().expect("thread must not panic");
        }
    }

    #[test]
    fn test_audit_log_concurrent() {
        let log = Arc::new(OpsAuditLog::new(1000));
        let mut handles = Vec::new();
        for t in 0..4 {
            let log_clone = Arc::clone(&log);
            handles.push(std::thread::spawn(move || {
                for i in 0..250 {
                    log_clone.record(
                        OpsEventType::NodeUp(NodeId((t * 250 + i) as u64)),
                        "test",
                    );
                }
            }));
        }
        for h in handles {
            h.join().expect("thread must not panic");
        }
        assert_eq!(log.total(), 1000);
        assert_eq!(log.all().len(), 1000);
    }
}
