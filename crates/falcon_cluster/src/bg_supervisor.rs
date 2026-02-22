//! Unified background task supervisor.
//!
//! All background tasks (failover orchestrator, rebalancer, GC, in-doubt
//! resolver, etc.) register with a single `BgTaskSupervisor` which:
//!
//! - Tracks task state: Starting → Running → Failed → Stopped
//! - Distinguishes Critical vs NonCritical tasks
//! - Derives node health: if any Critical task is Failed → DEGRADED
//! - Exposes metrics (`bg_task_failed{task=...}`) and admin query support
//!
//! # Usage
//! ```ignore
//! let sup = BgTaskSupervisor::new();
//! sup.register("failover-orchestrator", BgTaskCriticality::Critical);
//! sup.set_running("failover-orchestrator");
//! // ... on failure:
//! sup.set_failed("failover-orchestrator", "thread panicked");
//! assert_eq!(sup.node_health(), NodeHealth::Degraded);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::RwLock;

/// State of a background task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BgTaskState {
    Starting,
    Running,
    Failed,
    Stopped,
}

impl std::fmt::Display for BgTaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BgTaskState::Starting => write!(f, "STARTING"),
            BgTaskState::Running => write!(f, "RUNNING"),
            BgTaskState::Failed => write!(f, "FAILED"),
            BgTaskState::Stopped => write!(f, "STOPPED"),
        }
    }
}

/// Whether a task is critical to node health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BgTaskCriticality {
    /// Failure of this task degrades the node.
    Critical,
    /// Failure is logged but does not affect node health.
    NonCritical,
}

/// Derived node health based on task states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeHealth {
    Healthy,
    Degraded,
}

impl std::fmt::Display for NodeHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeHealth::Healthy => write!(f, "HEALTHY"),
            NodeHealth::Degraded => write!(f, "DEGRADED"),
        }
    }
}

/// Per-task metadata.
#[derive(Debug, Clone)]
pub struct BgTaskInfo {
    pub name: String,
    pub criticality: BgTaskCriticality,
    pub state: BgTaskState,
    pub failure_reason: Option<String>,
    pub registered_at: Instant,
    pub last_state_change: Instant,
}

/// Snapshot of all tasks for admin/metrics queries.
#[derive(Debug, Clone)]
pub struct BgTaskSnapshot {
    pub tasks: Vec<BgTaskInfo>,
    pub node_health: NodeHealth,
    pub total_failures: u64,
}

/// Unified supervisor for all background tasks.
pub struct BgTaskSupervisor {
    tasks: RwLock<HashMap<String, BgTaskInfo>>,
    total_failures: AtomicU64,
}

impl BgTaskSupervisor {
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
            total_failures: AtomicU64::new(0),
        }
    }

    /// Register a new background task with its criticality.
    pub fn register(&self, name: &str, criticality: BgTaskCriticality) {
        let now = Instant::now();
        let mut tasks = self.tasks.write();
        tasks.insert(
            name.to_string(),
            BgTaskInfo {
                name: name.to_string(),
                criticality,
                state: BgTaskState::Starting,
                failure_reason: None,
                registered_at: now,
                last_state_change: now,
            },
        );
        tracing::info!(
            component = name,
            criticality = ?criticality,
            "bg task registered"
        );
    }

    /// Mark a task as running.
    pub fn set_running(&self, name: &str) {
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.get_mut(name) {
            task.state = BgTaskState::Running;
            task.failure_reason = None;
            task.last_state_change = Instant::now();
            tracing::info!(component = name, "bg task now RUNNING");
        }
    }

    /// Mark a task as failed with a reason.
    pub fn set_failed(&self, name: &str, reason: &str) {
        self.total_failures.fetch_add(1, Ordering::Relaxed);
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.get_mut(name) {
            task.state = BgTaskState::Failed;
            task.failure_reason = Some(reason.to_string());
            task.last_state_change = Instant::now();
            let is_critical = task.criticality == BgTaskCriticality::Critical;
            tracing::error!(
                component = name,
                reason = reason,
                critical = is_critical,
                "bg task FAILED{}",
                if is_critical { " — node DEGRADED" } else { "" }
            );
        }
    }

    /// Mark a task as stopped (clean shutdown).
    pub fn set_stopped(&self, name: &str) {
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.get_mut(name) {
            task.state = BgTaskState::Stopped;
            task.last_state_change = Instant::now();
            tracing::info!(component = name, "bg task STOPPED");
        }
    }

    /// Derive overall node health from task states.
    pub fn node_health(&self) -> NodeHealth {
        let tasks = self.tasks.read();
        for task in tasks.values() {
            if task.criticality == BgTaskCriticality::Critical
                && task.state == BgTaskState::Failed
            {
                return NodeHealth::Degraded;
            }
        }
        NodeHealth::Healthy
    }

    /// Snapshot of all task states for admin queries.
    pub fn snapshot(&self) -> BgTaskSnapshot {
        let tasks = self.tasks.read();
        BgTaskSnapshot {
            tasks: tasks.values().cloned().collect(),
            node_health: self.node_health(),
            total_failures: self.total_failures.load(Ordering::Relaxed),
        }
    }

    /// Number of registered tasks.
    pub fn task_count(&self) -> usize {
        self.tasks.read().len()
    }

    /// Number of tasks currently in Failed state.
    pub fn failed_count(&self) -> usize {
        self.tasks
            .read()
            .values()
            .filter(|t| t.state == BgTaskState::Failed)
            .count()
    }

    /// Number of tasks currently Running.
    pub fn running_count(&self) -> usize {
        self.tasks
            .read()
            .values()
            .filter(|t| t.state == BgTaskState::Running)
            .count()
    }
}

impl Default for BgTaskSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_running() {
        let sup = BgTaskSupervisor::new();
        sup.register("gc", BgTaskCriticality::NonCritical);
        assert_eq!(sup.task_count(), 1);
        assert_eq!(sup.node_health(), NodeHealth::Healthy);

        sup.set_running("gc");
        assert_eq!(sup.running_count(), 1);
        assert_eq!(sup.node_health(), NodeHealth::Healthy);
    }

    #[test]
    fn test_critical_failure_degrades_node() {
        let sup = BgTaskSupervisor::new();
        sup.register("failover", BgTaskCriticality::Critical);
        sup.set_running("failover");
        assert_eq!(sup.node_health(), NodeHealth::Healthy);

        sup.set_failed("failover", "thread panicked");
        assert_eq!(sup.node_health(), NodeHealth::Degraded);
        assert_eq!(sup.failed_count(), 1);
    }

    #[test]
    fn test_noncritical_failure_does_not_degrade() {
        let sup = BgTaskSupervisor::new();
        sup.register("rebalancer", BgTaskCriticality::NonCritical);
        sup.set_running("rebalancer");
        sup.set_failed("rebalancer", "transient error");
        assert_eq!(sup.node_health(), NodeHealth::Healthy);
        assert_eq!(sup.failed_count(), 1);
    }

    #[test]
    fn test_stopped_does_not_degrade() {
        let sup = BgTaskSupervisor::new();
        sup.register("gc", BgTaskCriticality::Critical);
        sup.set_running("gc");
        sup.set_stopped("gc");
        assert_eq!(sup.node_health(), NodeHealth::Healthy);
    }

    #[test]
    fn test_snapshot() {
        let sup = BgTaskSupervisor::new();
        sup.register("a", BgTaskCriticality::Critical);
        sup.register("b", BgTaskCriticality::NonCritical);
        sup.set_running("a");
        sup.set_failed("b", "oom");
        let snap = sup.snapshot();
        assert_eq!(snap.tasks.len(), 2);
        assert_eq!(snap.node_health, NodeHealth::Healthy); // only "b" failed, non-critical
        assert_eq!(snap.total_failures, 1);
    }

    #[test]
    fn test_multiple_critical_tasks() {
        let sup = BgTaskSupervisor::new();
        sup.register("failover", BgTaskCriticality::Critical);
        sup.register("indoubt", BgTaskCriticality::Critical);
        sup.set_running("failover");
        sup.set_running("indoubt");
        assert_eq!(sup.node_health(), NodeHealth::Healthy);

        sup.set_failed("indoubt", "resolve failed");
        assert_eq!(sup.node_health(), NodeHealth::Degraded);
    }

    #[test]
    fn test_recovery_from_failure() {
        let sup = BgTaskSupervisor::new();
        sup.register("gc", BgTaskCriticality::Critical);
        sup.set_failed("gc", "crash");
        assert_eq!(sup.node_health(), NodeHealth::Degraded);

        // Task restarts and recovers
        sup.set_running("gc");
        assert_eq!(sup.node_health(), NodeHealth::Healthy);
    }

    #[test]
    fn test_total_failures_counter() {
        let sup = BgTaskSupervisor::new();
        sup.register("gc", BgTaskCriticality::NonCritical);
        sup.set_failed("gc", "err1");
        sup.set_running("gc");
        sup.set_failed("gc", "err2");
        assert_eq!(sup.snapshot().total_failures, 2);
    }
}
