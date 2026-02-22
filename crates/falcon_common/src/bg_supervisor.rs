//! Unified background task supervisor.
//!
//! All background tasks (GC, failover orchestrator, rebalancer, in-doubt
//! resolver, etc.) register with the supervisor. The supervisor tracks
//! their state and exposes a unified health view.
//!
//! If any **critical** task fails, the node enters `DEGRADED` state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Instant;

/// How important a background task is to node health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskCriticality {
    /// Node cannot serve correctly without this task (e.g. GC, failover).
    Critical,
    /// Degradation is tolerable short-term (e.g. rebalancer, audit drain).
    BestEffort,
}

/// Lifecycle state of a background task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Starting,
    Running,
    Failed,
    Stopped,
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskState::Starting => write!(f, "STARTING"),
            TaskState::Running => write!(f, "RUNNING"),
            TaskState::Failed => write!(f, "FAILED"),
            TaskState::Stopped => write!(f, "STOPPED"),
        }
    }
}

/// Registration info for a single background task.
#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub name: String,
    pub role: String,
    pub criticality: TaskCriticality,
    pub state: TaskState,
    pub registered_at: Instant,
    pub last_state_change: Instant,
    pub failure_reason: Option<String>,
}

/// Node-level health derived from background task states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeHealth {
    /// All tasks healthy.
    Healthy,
    /// At least one critical task has failed.
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

/// Snapshot of all background task states for observability.
#[derive(Debug, Clone)]
pub struct SupervisorSnapshot {
    pub node_health: NodeHealth,
    pub tasks: Vec<TaskInfo>,
    pub critical_failures: usize,
    pub total_registered: u64,
}

/// Unified supervisor for background tasks.
///
/// Thread-safe. Tasks call `register()` on startup, `report_running()` when
/// their loop is active, and `report_failed()` / `report_stopped()` on exit.
pub struct BgTaskSupervisor {
    tasks: RwLock<HashMap<String, TaskInfo>>,
    total_registered: AtomicU64,
}

impl BgTaskSupervisor {
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
            total_registered: AtomicU64::new(0),
        }
    }

    /// Register a new background task.
    pub fn register(&self, name: &str, role: &str, criticality: TaskCriticality) {
        let now = Instant::now();
        let info = TaskInfo {
            name: name.to_string(),
            role: role.to_string(),
            criticality,
            state: TaskState::Starting,
            registered_at: now,
            last_state_change: now,
            failure_reason: None,
        };
        let mut tasks = self.tasks.write().unwrap_or_else(|e| e.into_inner());
        tasks.insert(name.to_string(), info);
        self.total_registered.fetch_add(1, Ordering::Relaxed);
        tracing::info!(task = name, role, ?criticality, "bg task registered");
    }

    /// Report that a task is now running.
    pub fn report_running(&self, name: &str) {
        let mut tasks = self.tasks.write().unwrap_or_else(|e| e.into_inner());
        if let Some(info) = tasks.get_mut(name) {
            info.state = TaskState::Running;
            info.last_state_change = Instant::now();
            info.failure_reason = None;
        }
    }

    /// Report that a task has failed.
    pub fn report_failed(&self, name: &str, reason: &str) {
        let mut tasks = self.tasks.write().unwrap_or_else(|e| e.into_inner());
        if let Some(info) = tasks.get_mut(name) {
            let was = info.state;
            info.state = TaskState::Failed;
            info.last_state_change = Instant::now();
            info.failure_reason = Some(reason.to_string());

            if info.criticality == TaskCriticality::Critical {
                tracing::error!(
                    task = name,
                    role = %info.role,
                    previous_state = %was,
                    reason,
                    "CRITICAL bg task failed — node DEGRADED"
                );
            } else {
                tracing::warn!(
                    task = name,
                    role = %info.role,
                    reason,
                    "best-effort bg task failed"
                );
            }
        }
    }

    /// Report that a task has stopped gracefully.
    pub fn report_stopped(&self, name: &str) {
        let mut tasks = self.tasks.write().unwrap_or_else(|e| e.into_inner());
        if let Some(info) = tasks.get_mut(name) {
            info.state = TaskState::Stopped;
            info.last_state_change = Instant::now();
            info.failure_reason = None;
        }
    }

    /// Unregister a task (e.g. on clean shutdown).
    pub fn unregister(&self, name: &str) {
        let mut tasks = self.tasks.write().unwrap_or_else(|e| e.into_inner());
        tasks.remove(name);
    }

    /// Compute current node health.
    pub fn node_health(&self) -> NodeHealth {
        let tasks = self.tasks.read().unwrap_or_else(|e| e.into_inner());
        for info in tasks.values() {
            if info.criticality == TaskCriticality::Critical && info.state == TaskState::Failed {
                return NodeHealth::Degraded;
            }
        }
        NodeHealth::Healthy
    }

    /// Get the state of a specific task.
    pub fn task_state(&self, name: &str) -> Option<TaskState> {
        let tasks = self.tasks.read().unwrap_or_else(|e| e.into_inner());
        tasks.get(name).map(|i| i.state)
    }

    /// Full observability snapshot.
    pub fn snapshot(&self) -> SupervisorSnapshot {
        let tasks = self.tasks.read().unwrap_or_else(|e| e.into_inner());
        let mut critical_failures = 0;
        let mut task_list = Vec::with_capacity(tasks.len());
        for info in tasks.values() {
            if info.criticality == TaskCriticality::Critical && info.state == TaskState::Failed {
                critical_failures += 1;
            }
            task_list.push(info.clone());
        }
        // Sort by name for stable output
        task_list.sort_by(|a, b| a.name.cmp(&b.name));
        SupervisorSnapshot {
            node_health: if critical_failures > 0 {
                NodeHealth::Degraded
            } else {
                NodeHealth::Healthy
            },
            tasks: task_list,
            critical_failures,
            total_registered: self.total_registered.load(Ordering::Relaxed),
        }
    }

    /// Number of currently registered tasks.
    pub fn task_count(&self) -> usize {
        self.tasks.read().unwrap_or_else(|e| e.into_inner()).len()
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
    fn test_register_and_query() {
        let sv = BgTaskSupervisor::new();
        sv.register("gc", "garbage-collector", TaskCriticality::Critical);
        assert_eq!(sv.task_count(), 1);
        assert_eq!(sv.task_state("gc"), Some(TaskState::Starting));
        assert_eq!(sv.node_health(), NodeHealth::Healthy);
    }

    #[test]
    fn test_running_state() {
        let sv = BgTaskSupervisor::new();
        sv.register("gc", "garbage-collector", TaskCriticality::Critical);
        sv.report_running("gc");
        assert_eq!(sv.task_state("gc"), Some(TaskState::Running));
        assert_eq!(sv.node_health(), NodeHealth::Healthy);
    }

    #[test]
    fn test_critical_failure_degrades_node() {
        let sv = BgTaskSupervisor::new();
        sv.register("gc", "garbage-collector", TaskCriticality::Critical);
        sv.report_running("gc");
        sv.report_failed("gc", "thread panicked");
        assert_eq!(sv.task_state("gc"), Some(TaskState::Failed));
        assert_eq!(sv.node_health(), NodeHealth::Degraded);
    }

    #[test]
    fn test_besteffort_failure_stays_healthy() {
        let sv = BgTaskSupervisor::new();
        sv.register(
            "rebalancer",
            "shard-rebalancer",
            TaskCriticality::BestEffort,
        );
        sv.report_running("rebalancer");
        sv.report_failed("rebalancer", "no shards");
        assert_eq!(sv.task_state("rebalancer"), Some(TaskState::Failed));
        assert_eq!(sv.node_health(), NodeHealth::Healthy);
    }

    #[test]
    fn test_stopped_state() {
        let sv = BgTaskSupervisor::new();
        sv.register("gc", "garbage-collector", TaskCriticality::Critical);
        sv.report_running("gc");
        sv.report_stopped("gc");
        assert_eq!(sv.task_state("gc"), Some(TaskState::Stopped));
        assert_eq!(sv.node_health(), NodeHealth::Healthy);
    }

    #[test]
    fn test_unregister() {
        let sv = BgTaskSupervisor::new();
        sv.register("gc", "garbage-collector", TaskCriticality::Critical);
        assert_eq!(sv.task_count(), 1);
        sv.unregister("gc");
        assert_eq!(sv.task_count(), 0);
        assert_eq!(sv.task_state("gc"), None);
    }

    #[test]
    fn test_snapshot() {
        let sv = BgTaskSupervisor::new();
        sv.register("gc", "garbage-collector", TaskCriticality::Critical);
        sv.register(
            "rebalancer",
            "shard-rebalancer",
            TaskCriticality::BestEffort,
        );
        sv.report_running("gc");
        sv.report_failed("rebalancer", "no shards");

        let snap = sv.snapshot();
        assert_eq!(snap.node_health, NodeHealth::Healthy);
        assert_eq!(snap.tasks.len(), 2);
        assert_eq!(snap.critical_failures, 0);
        assert_eq!(snap.total_registered, 2);
    }

    #[test]
    fn test_snapshot_with_critical_failure() {
        let sv = BgTaskSupervisor::new();
        sv.register("gc", "garbage-collector", TaskCriticality::Critical);
        sv.register("failover", "ha-orchestrator", TaskCriticality::Critical);
        sv.report_running("gc");
        sv.report_failed("failover", "spawn failed");

        let snap = sv.snapshot();
        assert_eq!(snap.node_health, NodeHealth::Degraded);
        assert_eq!(snap.critical_failures, 1);
    }

    #[test]
    fn test_multiple_tasks_mixed() {
        let sv = BgTaskSupervisor::new();
        sv.register("gc", "garbage-collector", TaskCriticality::Critical);
        sv.register(
            "rebalancer",
            "shard-rebalancer",
            TaskCriticality::BestEffort,
        );
        sv.register("indoubt", "2pc-resolver", TaskCriticality::Critical);

        sv.report_running("gc");
        sv.report_running("rebalancer");
        sv.report_running("indoubt");
        assert_eq!(sv.node_health(), NodeHealth::Healthy);

        sv.report_failed("gc", "OOM");
        assert_eq!(sv.node_health(), NodeHealth::Degraded);

        // Even fixing rebalancer doesn't help — gc is still failed
        sv.report_stopped("rebalancer");
        assert_eq!(sv.node_health(), NodeHealth::Degraded);

        // Fix gc
        sv.report_running("gc");
        assert_eq!(sv.node_health(), NodeHealth::Healthy);
    }
}
