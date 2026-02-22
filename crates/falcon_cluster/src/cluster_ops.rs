//! Cluster operations closure: state machines for scale-out, scale-in,
//! leader transfer, rebalance; structured event log for all transitions.
//!
//! # Design
//!
//! Every cluster operation is modelled as a **state machine** with:
//! - Named states (enum variants)
//! - Explicit transitions logged to a structured event log
//! - Interruptible + resumable semantics (state is persisted in-memory)
//!
//! The `ClusterEventLog` is a bounded ring buffer of `ClusterEvent` entries
//! that can be queried via `SHOW falcon.cluster_events`.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use falcon_common::types::{NodeId, ShardId};

// ── Structured Event Log ────────────────────────────────────────────────────

/// Category of cluster event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventCategory {
    ScaleOut,
    ScaleIn,
    Rebalance,
    LeaderTransfer,
    Failover,
    Membership,
}

impl std::fmt::Display for EventCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventCategory::ScaleOut => write!(f, "scale_out"),
            EventCategory::ScaleIn => write!(f, "scale_in"),
            EventCategory::Rebalance => write!(f, "rebalance"),
            EventCategory::LeaderTransfer => write!(f, "leader_transfer"),
            EventCategory::Failover => write!(f, "failover"),
            EventCategory::Membership => write!(f, "membership"),
        }
    }
}

/// Severity / outcome of an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSeverity {
    Info,
    Warn,
    Error,
}

impl std::fmt::Display for EventSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventSeverity::Info => write!(f, "info"),
            EventSeverity::Warn => write!(f, "warn"),
            EventSeverity::Error => write!(f, "error"),
        }
    }
}

/// A single structured cluster event.
#[derive(Debug, Clone)]
pub struct ClusterEvent {
    /// Monotonically increasing sequence number.
    pub seq: u64,
    /// Wall-clock timestamp (ms since epoch).
    pub timestamp_ms: u64,
    /// Event category.
    pub category: EventCategory,
    /// Severity.
    pub severity: EventSeverity,
    /// Which node this event pertains to (0 = cluster-wide).
    pub node_id: u64,
    /// Which shard this event pertains to (u64::MAX = N/A).
    pub shard_id: u64,
    /// Previous state (for state transitions).
    pub from_state: String,
    /// New state.
    pub to_state: String,
    /// Human-readable message.
    pub message: String,
}

/// Bounded ring buffer of cluster events.
pub struct ClusterEventLog {
    events: Mutex<VecDeque<ClusterEvent>>,
    max_events: usize,
    next_seq: Mutex<u64>,
}

impl ClusterEventLog {
    pub fn new(max_events: usize) -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(VecDeque::with_capacity(max_events)),
            max_events,
            next_seq: Mutex::new(1),
        })
    }

    /// Append an event to the log.
    #[allow(clippy::too_many_arguments)]
    pub fn log(
        &self,
        category: EventCategory,
        severity: EventSeverity,
        node_id: u64,
        shard_id: u64,
        from_state: impl Into<String>,
        to_state: impl Into<String>,
        message: impl Into<String>,
    ) {
        let mut seq = self.next_seq.lock();
        let event = ClusterEvent {
            seq: *seq,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            category,
            severity,
            node_id,
            shard_id,
            from_state: from_state.into(),
            to_state: to_state.into(),
            message: message.into(),
        };
        *seq += 1;

        tracing::info!(
            seq = event.seq,
            category = %event.category,
            severity = %event.severity,
            node_id = event.node_id,
            shard_id = event.shard_id,
            from = %event.from_state,
            to = %event.to_state,
            "CLUSTER_EVENT: {}",
            event.message,
        );

        let mut events = self.events.lock();
        if events.len() >= self.max_events {
            events.pop_front();
        }
        events.push_back(event);
    }

    /// Snapshot of all events (most recent last).
    pub fn snapshot(&self) -> Vec<ClusterEvent> {
        self.events.lock().iter().cloned().collect()
    }

    /// Number of events in the log.
    pub fn len(&self) -> usize {
        self.events.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.lock().is_empty()
    }
}

// ── Scale-Out State Machine ─────────────────────────────────────────────────

/// States for the scale-out (add node) lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleOutState {
    /// Initial: node has been registered but not yet started catch-up.
    Joining,
    /// WAL catch-up in progress — node is receiving historical data.
    CatchingUp,
    /// Caught up — node can serve read queries.
    ServingReads,
    /// Fully caught up and serving writes (after rebalance).
    ServingWrites,
    /// Rebalance in progress — shards are being migrated to this node.
    Rebalancing,
    /// Terminal: node is fully integrated.
    Active,
    /// Terminal: join was aborted or failed.
    Failed,
}

impl std::fmt::Display for ScaleOutState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScaleOutState::Joining => write!(f, "joining"),
            ScaleOutState::CatchingUp => write!(f, "catching_up"),
            ScaleOutState::ServingReads => write!(f, "serving_reads"),
            ScaleOutState::ServingWrites => write!(f, "serving_writes"),
            ScaleOutState::Rebalancing => write!(f, "rebalancing"),
            ScaleOutState::Active => write!(f, "active"),
            ScaleOutState::Failed => write!(f, "failed"),
        }
    }
}

/// Scale-out lifecycle tracker for a single node.
#[derive(Debug, Clone)]
pub struct ScaleOutLifecycle {
    pub node_id: NodeId,
    pub state: ScaleOutState,
    pub started_at: Instant,
    pub last_transition: Instant,
    pub error: Option<String>,
}

impl ScaleOutLifecycle {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            state: ScaleOutState::Joining,
            started_at: Instant::now(),
            last_transition: Instant::now(),
            error: None,
        }
    }

    /// Attempt a state transition. Returns Ok if valid, Err if invalid.
    pub fn transition(
        &mut self,
        to: ScaleOutState,
        event_log: &ClusterEventLog,
    ) -> Result<(), String> {
        let valid = matches!(
            (self.state, to),
            (ScaleOutState::Joining, ScaleOutState::CatchingUp)
            | (ScaleOutState::CatchingUp, ScaleOutState::ServingReads)
            | (ScaleOutState::ServingReads, ScaleOutState::Rebalancing)
            | (ScaleOutState::Rebalancing, ScaleOutState::ServingWrites)
            | (ScaleOutState::ServingWrites, ScaleOutState::Active)
            // Any state can fail
            | (_, ScaleOutState::Failed)
        );

        if !valid {
            let msg = format!(
                "Invalid scale-out transition: {} → {} for node {}",
                self.state, to, self.node_id.0
            );
            event_log.log(
                EventCategory::ScaleOut,
                EventSeverity::Error,
                self.node_id.0,
                u64::MAX,
                self.state.to_string(),
                to.to_string(),
                &msg,
            );
            return Err(msg);
        }

        let from = self.state;
        self.state = to;
        self.last_transition = Instant::now();

        event_log.log(
            EventCategory::ScaleOut,
            EventSeverity::Info,
            self.node_id.0,
            u64::MAX,
            from.to_string(),
            to.to_string(),
            format!("Node {} scale-out: {} → {}", self.node_id.0, from, to),
        );

        Ok(())
    }

    /// Mark as failed with an error message.
    pub fn fail(&mut self, reason: &str, event_log: &ClusterEventLog) {
        self.error = Some(reason.to_string());
        let _ = self.transition(ScaleOutState::Failed, event_log);
    }

    /// Elapsed time since the lifecycle started.
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self.state, ScaleOutState::Active | ScaleOutState::Failed)
    }
}

// ── Scale-In State Machine ──────────────────────────────────────────────────

/// States for the scale-in (remove node) lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleInState {
    /// Initial: drain has been requested.
    Draining,
    /// Leadership is being moved off this node.
    MovingLeadership,
    /// Data (shards) are being migrated off this node.
    MovingData,
    /// Node has been removed from membership.
    Removed,
    /// Terminal: scale-in failed.
    Failed,
}

impl std::fmt::Display for ScaleInState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScaleInState::Draining => write!(f, "draining"),
            ScaleInState::MovingLeadership => write!(f, "moving_leadership"),
            ScaleInState::MovingData => write!(f, "moving_data"),
            ScaleInState::Removed => write!(f, "removed"),
            ScaleInState::Failed => write!(f, "failed"),
        }
    }
}

/// Scale-in lifecycle tracker for a single node.
#[derive(Debug, Clone)]
pub struct ScaleInLifecycle {
    pub node_id: NodeId,
    pub state: ScaleInState,
    pub started_at: Instant,
    pub last_transition: Instant,
    pub error: Option<String>,
}

impl ScaleInLifecycle {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            state: ScaleInState::Draining,
            started_at: Instant::now(),
            last_transition: Instant::now(),
            error: None,
        }
    }

    /// Attempt a state transition.
    pub fn transition(
        &mut self,
        to: ScaleInState,
        event_log: &ClusterEventLog,
    ) -> Result<(), String> {
        let valid = matches!(
            (self.state, to),
            (ScaleInState::Draining, ScaleInState::MovingLeadership)
            | (ScaleInState::MovingLeadership, ScaleInState::MovingData)
            | (ScaleInState::MovingData, ScaleInState::Removed)
            // Any state can fail
            | (_, ScaleInState::Failed)
        );

        if !valid {
            let msg = format!(
                "Invalid scale-in transition: {} → {} for node {}",
                self.state, to, self.node_id.0
            );
            event_log.log(
                EventCategory::ScaleIn,
                EventSeverity::Error,
                self.node_id.0,
                u64::MAX,
                self.state.to_string(),
                to.to_string(),
                &msg,
            );
            return Err(msg);
        }

        let from = self.state;
        self.state = to;
        self.last_transition = Instant::now();

        event_log.log(
            EventCategory::ScaleIn,
            EventSeverity::Info,
            self.node_id.0,
            u64::MAX,
            from.to_string(),
            to.to_string(),
            format!("Node {} scale-in: {} → {}", self.node_id.0, from, to),
        );

        Ok(())
    }

    pub fn fail(&mut self, reason: &str, event_log: &ClusterEventLog) {
        self.error = Some(reason.to_string());
        let _ = self.transition(ScaleInState::Failed, event_log);
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self.state, ScaleInState::Removed | ScaleInState::Failed)
    }
}

// ── Cluster Admin ───────────────────────────────────────────────────────────

/// Central cluster administration coordinator.
///
/// Manages node lifecycles, rebalance operations, and leader transfers.
/// All operations are logged to the structured event log.
pub struct ClusterAdmin {
    event_log: Arc<ClusterEventLog>,
    scale_out_ops: Mutex<Vec<ScaleOutLifecycle>>,
    scale_in_ops: Mutex<Vec<ScaleInLifecycle>>,
}

impl ClusterAdmin {
    pub fn new(event_log: Arc<ClusterEventLog>) -> Arc<Self> {
        Arc::new(Self {
            event_log,
            scale_out_ops: Mutex::new(Vec::new()),
            scale_in_ops: Mutex::new(Vec::new()),
        })
    }

    /// Get the event log.
    pub fn event_log(&self) -> &Arc<ClusterEventLog> {
        &self.event_log
    }

    // ── Scale-Out ───────────────────────────────────────────────────────

    /// Initiate a scale-out operation for a new node.
    pub fn begin_scale_out(&self, node_id: NodeId) -> Result<(), String> {
        let mut ops = self.scale_out_ops.lock();
        // Check for duplicate
        if ops.iter().any(|o| o.node_id == node_id && !o.is_terminal()) {
            return Err(format!(
                "Scale-out already in progress for node {}",
                node_id.0
            ));
        }

        let lifecycle = ScaleOutLifecycle::new(node_id);
        self.event_log.log(
            EventCategory::ScaleOut,
            EventSeverity::Info,
            node_id.0,
            u64::MAX,
            "",
            "joining",
            format!("Scale-out initiated for node {}", node_id.0),
        );
        ops.push(lifecycle);
        Ok(())
    }

    /// Advance a scale-out operation to the next state.
    pub fn advance_scale_out(&self, node_id: NodeId, to: ScaleOutState) -> Result<(), String> {
        let mut ops = self.scale_out_ops.lock();
        let op = ops
            .iter_mut()
            .find(|o| o.node_id == node_id && !o.is_terminal())
            .ok_or_else(|| format!("No active scale-out for node {}", node_id.0))?;
        op.transition(to, &self.event_log)
    }

    /// Fail a scale-out operation.
    pub fn fail_scale_out(&self, node_id: NodeId, reason: &str) {
        let mut ops = self.scale_out_ops.lock();
        if let Some(op) = ops
            .iter_mut()
            .find(|o| o.node_id == node_id && !o.is_terminal())
        {
            op.fail(reason, &self.event_log);
        }
    }

    /// Snapshot of all scale-out operations.
    pub fn scale_out_snapshot(&self) -> Vec<ScaleOutLifecycle> {
        self.scale_out_ops.lock().clone()
    }

    // ── Scale-In ────────────────────────────────────────────────────────

    /// Initiate a scale-in (remove node) operation.
    pub fn begin_scale_in(&self, node_id: NodeId) -> Result<(), String> {
        let mut ops = self.scale_in_ops.lock();
        if ops.iter().any(|o| o.node_id == node_id && !o.is_terminal()) {
            return Err(format!(
                "Scale-in already in progress for node {}",
                node_id.0
            ));
        }

        let lifecycle = ScaleInLifecycle::new(node_id);
        self.event_log.log(
            EventCategory::ScaleIn,
            EventSeverity::Info,
            node_id.0,
            u64::MAX,
            "",
            "draining",
            format!("Scale-in initiated for node {}", node_id.0),
        );
        ops.push(lifecycle);
        Ok(())
    }

    /// Advance a scale-in operation.
    pub fn advance_scale_in(&self, node_id: NodeId, to: ScaleInState) -> Result<(), String> {
        let mut ops = self.scale_in_ops.lock();
        let op = ops
            .iter_mut()
            .find(|o| o.node_id == node_id && !o.is_terminal())
            .ok_or_else(|| format!("No active scale-in for node {}", node_id.0))?;
        op.transition(to, &self.event_log)
    }

    /// Fail a scale-in operation.
    pub fn fail_scale_in(&self, node_id: NodeId, reason: &str) {
        let mut ops = self.scale_in_ops.lock();
        if let Some(op) = ops
            .iter_mut()
            .find(|o| o.node_id == node_id && !o.is_terminal())
        {
            op.fail(reason, &self.event_log);
        }
    }

    /// Snapshot of all scale-in operations.
    pub fn scale_in_snapshot(&self) -> Vec<ScaleInLifecycle> {
        self.scale_in_ops.lock().clone()
    }

    // ── Leader Transfer ─────────────────────────────────────────────────

    /// Log a leader transfer event (promote a specific replica for a shard).
    pub fn log_leader_transfer(&self, shard_id: ShardId, from_node: NodeId, to_node: NodeId) {
        self.event_log.log(
            EventCategory::LeaderTransfer,
            EventSeverity::Info,
            to_node.0,
            shard_id.0,
            format!("node_{}", from_node.0),
            format!("node_{}", to_node.0),
            format!(
                "Leader transfer: shard {} from node {} to node {}",
                shard_id.0, from_node.0, to_node.0
            ),
        );
    }

    // ── Rebalance ───────────────────────────────────────────────────────

    /// Log a rebalance plan event (dry-run).
    pub fn log_rebalance_plan(&self, num_tasks: usize, total_rows: u64) {
        self.event_log.log(
            EventCategory::Rebalance,
            EventSeverity::Info,
            0,
            u64::MAX,
            "idle",
            "planned",
            format!(
                "Rebalance plan generated: {} tasks, {} rows to move",
                num_tasks, total_rows
            ),
        );
    }

    /// Log a rebalance apply start.
    pub fn log_rebalance_start(&self, num_tasks: usize) {
        self.event_log.log(
            EventCategory::Rebalance,
            EventSeverity::Info,
            0,
            u64::MAX,
            "planned",
            "executing",
            format!("Rebalance apply started: {} tasks", num_tasks),
        );
    }

    /// Log a rebalance completion.
    pub fn log_rebalance_complete(
        &self,
        tasks_completed: usize,
        tasks_failed: usize,
        rows_migrated: u64,
        duration: Duration,
    ) {
        let severity = if tasks_failed > 0 {
            EventSeverity::Warn
        } else {
            EventSeverity::Info
        };
        self.event_log.log(
            EventCategory::Rebalance,
            severity,
            0,
            u64::MAX,
            "executing",
            "idle",
            format!(
                "Rebalance complete: {}/{} tasks succeeded, {} rows migrated in {:.1}s",
                tasks_completed,
                tasks_completed + tasks_failed,
                rows_migrated,
                duration.as_secs_f64(),
            ),
        );
    }
}

// ── Node Operational Mode (v1.5) ────────────────────────────────────────────

/// Operational mode for a node, independent of replication role.
/// Controls what types of operations the node accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeOperationalMode {
    /// Normal operation: accepts reads, writes, and DDL.
    Normal,
    /// Read-only mode: accepts reads only. Writes and DDL are rejected.
    ReadOnly,
    /// Drain mode: rejects all new operations. Existing transactions
    /// are allowed to complete. Used before scale-in or maintenance.
    Drain,
}

impl std::fmt::Display for NodeOperationalMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeOperationalMode::Normal => write!(f, "normal"),
            NodeOperationalMode::ReadOnly => write!(f, "read_only"),
            NodeOperationalMode::Drain => write!(f, "drain"),
        }
    }
}

/// Controller for node operational mode transitions.
/// All transitions are logged to the cluster event log.
pub struct NodeModeController {
    mode: parking_lot::RwLock<NodeOperationalMode>,
    node_id: NodeId,
    event_log: Arc<ClusterEventLog>,
    /// Timestamp (Instant) of the last mode change.
    last_change: parking_lot::RwLock<Instant>,
}

impl NodeModeController {
    pub fn new(node_id: NodeId, event_log: Arc<ClusterEventLog>) -> Arc<Self> {
        Arc::new(Self {
            mode: parking_lot::RwLock::new(NodeOperationalMode::Normal),
            node_id,
            event_log,
            last_change: parking_lot::RwLock::new(Instant::now()),
        })
    }

    /// Current operational mode.
    pub fn mode(&self) -> NodeOperationalMode {
        *self.mode.read()
    }

    /// Transition to a new mode. Logs the transition.
    pub fn set_mode(&self, new_mode: NodeOperationalMode) {
        let mut mode = self.mode.write();
        let old = *mode;
        if old == new_mode {
            return;
        }
        *mode = new_mode;
        *self.last_change.write() = Instant::now();

        self.event_log.log(
            EventCategory::Membership,
            EventSeverity::Info,
            self.node_id.0,
            u64::MAX,
            old.to_string(),
            new_mode.to_string(),
            format!(
                "Node {} mode change: {} → {}",
                self.node_id.0, old, new_mode
            ),
        );
    }

    /// Enter drain mode. Convenience method.
    pub fn drain(&self) {
        self.set_mode(NodeOperationalMode::Drain);
    }

    /// Enter read-only mode. Convenience method.
    pub fn set_read_only(&self) {
        self.set_mode(NodeOperationalMode::ReadOnly);
    }

    /// Return to normal mode. Convenience method.
    pub fn resume(&self) {
        self.set_mode(NodeOperationalMode::Normal);
    }

    /// Check if reads are allowed in the current mode.
    pub fn allows_reads(&self) -> bool {
        matches!(
            *self.mode.read(),
            NodeOperationalMode::Normal | NodeOperationalMode::ReadOnly
        )
    }

    /// Check if writes are allowed in the current mode.
    pub fn allows_writes(&self) -> bool {
        matches!(*self.mode.read(), NodeOperationalMode::Normal)
    }

    /// Check if DDL is allowed in the current mode.
    pub fn allows_ddl(&self) -> bool {
        matches!(*self.mode.read(), NodeOperationalMode::Normal)
    }

    /// Duration since the last mode change.
    pub fn time_in_current_mode(&self) -> Duration {
        self.last_change.read().elapsed()
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_log_basic() {
        let log = ClusterEventLog::new(100);
        assert!(log.is_empty());

        log.log(
            EventCategory::Membership,
            EventSeverity::Info,
            1,
            u64::MAX,
            "",
            "active",
            "Node 1 joined",
        );
        assert_eq!(log.len(), 1);

        let events = log.snapshot();
        assert_eq!(events[0].seq, 1);
        assert_eq!(events[0].category, EventCategory::Membership);
        assert_eq!(events[0].message, "Node 1 joined");
    }

    #[test]
    fn test_event_log_ring_buffer() {
        let log = ClusterEventLog::new(3);
        for i in 0..5 {
            log.log(
                EventCategory::Membership,
                EventSeverity::Info,
                i,
                u64::MAX,
                "",
                "",
                format!("event {}", i),
            );
        }
        assert_eq!(log.len(), 3);
        let events = log.snapshot();
        // Should have events 3, 4, 5 (seq 3, 4, 5)
        assert_eq!(events[0].seq, 3);
        assert_eq!(events[2].seq, 5);
    }

    #[test]
    fn test_scale_out_lifecycle_happy_path() {
        let log = ClusterEventLog::new(100);
        let mut lc = ScaleOutLifecycle::new(NodeId(1));

        assert_eq!(lc.state, ScaleOutState::Joining);
        lc.transition(ScaleOutState::CatchingUp, &log).unwrap();
        lc.transition(ScaleOutState::ServingReads, &log).unwrap();
        lc.transition(ScaleOutState::Rebalancing, &log).unwrap();
        lc.transition(ScaleOutState::ServingWrites, &log).unwrap();
        lc.transition(ScaleOutState::Active, &log).unwrap();

        assert!(lc.is_terminal());
        assert_eq!(log.len(), 5);
    }

    #[test]
    fn test_scale_out_invalid_transition() {
        let log = ClusterEventLog::new(100);
        let mut lc = ScaleOutLifecycle::new(NodeId(1));

        // Can't skip from Joining to Active
        let result = lc.transition(ScaleOutState::Active, &log);
        assert!(result.is_err());
        assert_eq!(lc.state, ScaleOutState::Joining); // unchanged
    }

    #[test]
    fn test_scale_out_fail_from_any_state() {
        let log = ClusterEventLog::new(100);
        let mut lc = ScaleOutLifecycle::new(NodeId(1));

        lc.transition(ScaleOutState::CatchingUp, &log).unwrap();
        lc.fail("network error", &log);
        assert_eq!(lc.state, ScaleOutState::Failed);
        assert!(lc.is_terminal());
        assert_eq!(lc.error.as_deref(), Some("network error"));
    }

    #[test]
    fn test_scale_in_lifecycle_happy_path() {
        let log = ClusterEventLog::new(100);
        let mut lc = ScaleInLifecycle::new(NodeId(2));

        assert_eq!(lc.state, ScaleInState::Draining);
        lc.transition(ScaleInState::MovingLeadership, &log).unwrap();
        lc.transition(ScaleInState::MovingData, &log).unwrap();
        lc.transition(ScaleInState::Removed, &log).unwrap();

        assert!(lc.is_terminal());
        assert_eq!(log.len(), 3);
    }

    #[test]
    fn test_scale_in_invalid_transition() {
        let log = ClusterEventLog::new(100);
        let mut lc = ScaleInLifecycle::new(NodeId(2));

        // Can't skip from Draining to Removed
        let result = lc.transition(ScaleInState::Removed, &log);
        assert!(result.is_err());
    }

    #[test]
    fn test_cluster_admin_scale_out() {
        let log = ClusterEventLog::new(100);
        let admin = ClusterAdmin::new(log);

        admin.begin_scale_out(NodeId(10)).unwrap();

        // Duplicate should fail
        let result = admin.begin_scale_out(NodeId(10));
        assert!(result.is_err());

        // Advance through states
        admin
            .advance_scale_out(NodeId(10), ScaleOutState::CatchingUp)
            .unwrap();
        admin
            .advance_scale_out(NodeId(10), ScaleOutState::ServingReads)
            .unwrap();

        let ops = admin.scale_out_snapshot();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].state, ScaleOutState::ServingReads);
    }

    #[test]
    fn test_cluster_admin_scale_in() {
        let log = ClusterEventLog::new(100);
        let admin = ClusterAdmin::new(log);

        admin.begin_scale_in(NodeId(5)).unwrap();
        admin
            .advance_scale_in(NodeId(5), ScaleInState::MovingLeadership)
            .unwrap();
        admin
            .advance_scale_in(NodeId(5), ScaleInState::MovingData)
            .unwrap();
        admin
            .advance_scale_in(NodeId(5), ScaleInState::Removed)
            .unwrap();

        let ops = admin.scale_in_snapshot();
        assert_eq!(ops.len(), 1);
        assert!(ops[0].is_terminal());
    }

    #[test]
    fn test_cluster_admin_leader_transfer_logged() {
        let log = ClusterEventLog::new(100);
        let admin = ClusterAdmin::new(log.clone());

        admin.log_leader_transfer(ShardId(0), NodeId(1), NodeId(2));

        let events = log.snapshot();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].category, EventCategory::LeaderTransfer);
        assert!(events[0].message.contains("shard 0"));
    }

    #[test]
    fn test_cluster_admin_rebalance_lifecycle() {
        let log = ClusterEventLog::new(100);
        let admin = ClusterAdmin::new(log.clone());

        admin.log_rebalance_plan(3, 1500);
        admin.log_rebalance_start(3);
        admin.log_rebalance_complete(3, 0, 1500, Duration::from_secs(5));

        let events = log.snapshot();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].category, EventCategory::Rebalance);
        assert!(events[2].message.contains("1500 rows"));
    }

    #[test]
    fn test_event_category_display() {
        assert_eq!(EventCategory::ScaleOut.to_string(), "scale_out");
        assert_eq!(EventCategory::LeaderTransfer.to_string(), "leader_transfer");
    }

    #[test]
    fn test_scale_out_state_display() {
        assert_eq!(ScaleOutState::CatchingUp.to_string(), "catching_up");
        assert_eq!(ScaleOutState::ServingWrites.to_string(), "serving_writes");
    }

    #[test]
    fn test_scale_in_state_display() {
        assert_eq!(
            ScaleInState::MovingLeadership.to_string(),
            "moving_leadership"
        );
        assert_eq!(ScaleInState::Removed.to_string(), "removed");
    }

    #[test]
    fn test_node_operational_mode_display() {
        assert_eq!(NodeOperationalMode::Normal.to_string(), "normal");
        assert_eq!(NodeOperationalMode::ReadOnly.to_string(), "read_only");
        assert_eq!(NodeOperationalMode::Drain.to_string(), "drain");
    }

    #[test]
    fn test_node_mode_controller_default_normal() {
        let log = ClusterEventLog::new(100);
        let ctrl = NodeModeController::new(NodeId(1), log);
        assert_eq!(ctrl.mode(), NodeOperationalMode::Normal);
        assert!(ctrl.allows_reads());
        assert!(ctrl.allows_writes());
        assert!(ctrl.allows_ddl());
    }

    #[test]
    fn test_node_mode_controller_read_only() {
        let log = ClusterEventLog::new(100);
        let ctrl = NodeModeController::new(NodeId(1), log.clone());
        ctrl.set_read_only();
        assert_eq!(ctrl.mode(), NodeOperationalMode::ReadOnly);
        assert!(ctrl.allows_reads());
        assert!(!ctrl.allows_writes());
        assert!(!ctrl.allows_ddl());
        // Should have logged the transition
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn test_node_mode_controller_drain() {
        let log = ClusterEventLog::new(100);
        let ctrl = NodeModeController::new(NodeId(1), log.clone());
        ctrl.drain();
        assert_eq!(ctrl.mode(), NodeOperationalMode::Drain);
        assert!(!ctrl.allows_reads());
        assert!(!ctrl.allows_writes());
        assert!(!ctrl.allows_ddl());
    }

    #[test]
    fn test_node_mode_controller_resume() {
        let log = ClusterEventLog::new(100);
        let ctrl = NodeModeController::new(NodeId(1), log.clone());
        ctrl.drain();
        ctrl.resume();
        assert_eq!(ctrl.mode(), NodeOperationalMode::Normal);
        assert!(ctrl.allows_reads());
        assert!(ctrl.allows_writes());
        // Two transitions logged: normal→drain, drain→normal
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn test_node_mode_controller_noop_same_mode() {
        let log = ClusterEventLog::new(100);
        let ctrl = NodeModeController::new(NodeId(1), log.clone());
        ctrl.set_mode(NodeOperationalMode::Normal); // same as current
                                                    // No event logged for no-op
        assert_eq!(log.len(), 0);
    }
}
