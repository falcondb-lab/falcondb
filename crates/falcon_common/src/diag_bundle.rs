//! Diagnostic bundle — one-shot export of system state for incident response.
//!
//! Collects:
//! - Recent panic events
//! - Key metric snapshots (counters provided by caller)
//! - Recent log lines (caller-provided ring buffer)
//! - Topology / epoch / leader / apply-lag snapshot
//! - Inflight txn / queue / budget snapshot
//!
//! Output: structured JSON written to a file or returned as a String.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::crash_domain::{recent_panic_events, panic_count, PanicEvent};

/// Topology snapshot for the diagnostic bundle.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TopologySnapshot {
    pub node_id: String,
    pub role: String,
    pub epoch: u64,
    pub leader_id: Option<String>,
    pub apply_lag_lsn: u64,
    pub shard_count: usize,
    pub replica_count: usize,
}

/// Inflight resource snapshot.
#[derive(Debug, Clone, serde::Serialize)]
pub struct InflightSnapshot {
    pub active_connections: usize,
    pub inflight_queries: usize,
    pub inflight_writes: usize,
    pub indoubt_txns: usize,
    pub wal_backlog_bytes: u64,
    pub replication_lag_bytes: u64,
    pub memory_used_bytes: u64,
    pub memory_budget_bytes: u64,
}

/// A single metric entry in the bundle.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricEntry {
    pub name: String,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

/// The full diagnostic bundle.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DiagBundle {
    /// Unix timestamp (ms) when bundle was collected.
    pub collected_at_ms: u64,
    /// Node identifier.
    pub node_id: String,
    /// Total panic count since process start.
    pub panic_count: u64,
    /// Recent panic events.
    pub recent_panics: Vec<PanicEventSer>,
    /// Topology snapshot.
    pub topology: Option<TopologySnapshot>,
    /// Inflight resource snapshot.
    pub inflight: Option<InflightSnapshot>,
    /// Key metric snapshots.
    pub metrics: Vec<MetricEntry>,
    /// Recent log lines (last N).
    pub recent_logs: Vec<String>,
    /// Circuit breaker states per shard.
    pub circuit_breakers: HashMap<String, String>,
    /// Additional freeform context.
    pub context: HashMap<String, String>,
}

/// Serializable version of PanicEvent (avoids Instant serialization issues).
#[derive(Debug, Clone, serde::Serialize)]
pub struct PanicEventSer {
    pub message: String,
    pub location: String,
    pub thread_name: String,
    pub occurred_at_ms: u64,
}

impl From<PanicEvent> for PanicEventSer {
    fn from(e: PanicEvent) -> Self {
        Self {
            message: e.message,
            location: e.location,
            thread_name: e.thread_name,
            occurred_at_ms: e.occurred_at_ms,
        }
    }
}

/// Builder for constructing a `DiagBundle`.
#[derive(Default)]
pub struct DiagBundleBuilder {
    node_id: String,
    topology: Option<TopologySnapshot>,
    inflight: Option<InflightSnapshot>,
    metrics: Vec<MetricEntry>,
    recent_logs: Vec<String>,
    circuit_breakers: HashMap<String, String>,
    context: HashMap<String, String>,
}

impl DiagBundleBuilder {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            ..Default::default()
        }
    }

    pub fn topology(mut self, t: TopologySnapshot) -> Self {
        self.topology = Some(t);
        self
    }

    pub fn inflight(mut self, i: InflightSnapshot) -> Self {
        self.inflight = Some(i);
        self
    }

    pub fn metric(mut self, name: impl Into<String>, value: f64) -> Self {
        self.metrics.push(MetricEntry {
            name: name.into(),
            value,
            labels: HashMap::new(),
        });
        self
    }

    pub fn metric_with_labels(
        mut self,
        name: impl Into<String>,
        value: f64,
        labels: HashMap<String, String>,
    ) -> Self {
        self.metrics.push(MetricEntry { name: name.into(), value, labels });
        self
    }

    pub fn recent_logs(mut self, logs: Vec<String>) -> Self {
        self.recent_logs = logs;
        self
    }

    pub fn circuit_breaker(mut self, shard: impl Into<String>, state: impl Into<String>) -> Self {
        self.circuit_breakers.insert(shard.into(), state.into());
        self
    }

    pub fn context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> DiagBundle {
        let collected_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        DiagBundle {
            collected_at_ms,
            node_id: self.node_id,
            panic_count: panic_count(),
            recent_panics: recent_panic_events()
                .into_iter()
                .map(PanicEventSer::from)
                .collect(),
            topology: self.topology,
            inflight: self.inflight,
            metrics: self.metrics,
            recent_logs: self.recent_logs,
            circuit_breakers: self.circuit_breakers,
            context: self.context,
        }
    }
}

impl DiagBundle {
    /// Serialize to a pretty-printed JSON string.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self)
            .unwrap_or_else(|e| format!("{{\"error\": \"serialization failed: {}\"}}", e))
    }

    /// Write bundle to a file. Returns the path written.
    pub fn write_to_file(&self, path: &str) -> std::io::Result<()> {
        std::fs::write(path, self.to_json())
    }

    /// Write bundle to a timestamped file in `dir`. Returns the path.
    pub fn write_to_dir(&self, dir: &str) -> std::io::Result<String> {
        std::fs::create_dir_all(dir)?;
        let path = format!("{}/diag_bundle_{}.json", dir, self.collected_at_ms);
        self.write_to_file(&path)?;
        Ok(path)
    }

    /// Summary string for log output (single line).
    pub fn summary(&self) -> String {
        format!(
            "DiagBundle node={} panics={} metrics={} logs={} circuit_breakers={}",
            self.node_id,
            self.panic_count,
            self.metrics.len(),
            self.recent_logs.len(),
            self.circuit_breakers.len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bundle() -> DiagBundle {
        DiagBundleBuilder::new("node-1")
            .topology(TopologySnapshot {
                node_id: "node-1".into(),
                role: "primary".into(),
                epoch: 7,
                leader_id: Some("node-1".into()),
                apply_lag_lsn: 0,
                shard_count: 3,
                replica_count: 2,
            })
            .inflight(InflightSnapshot {
                active_connections: 42,
                inflight_queries: 10,
                inflight_writes: 5,
                indoubt_txns: 0,
                wal_backlog_bytes: 1024,
                replication_lag_bytes: 0,
                memory_used_bytes: 512 * 1024 * 1024,
                memory_budget_bytes: 4 * 1024 * 1024 * 1024,
            })
            .metric("falcon_txn_active", 10.0)
            .metric("falcon_replication_lag_lsn", 0.0)
            .metric("falcon_memory_used_bytes", 512.0 * 1024.0 * 1024.0)
            .recent_logs(vec![
                "[INFO] recovery: complete in 234ms".into(),
                "[WARN] slow query: 1200ms SELECT * FROM orders".into(),
            ])
            .circuit_breaker("shard-0", "closed")
            .circuit_breaker("shard-1", "closed")
            .context("version", "0.4.0")
            .context("build", "debug")
            .build()
    }

    #[test]
    fn test_bundle_builds() {
        let b = make_bundle();
        assert_eq!(b.node_id, "node-1");
        assert!(b.collected_at_ms > 0);
        assert_eq!(b.metrics.len(), 3);
        assert_eq!(b.recent_logs.len(), 2);
        assert_eq!(b.circuit_breakers.len(), 2);
    }

    #[test]
    fn test_topology_snapshot() {
        let b = make_bundle();
        let t = b.topology.unwrap();
        assert_eq!(t.role, "primary");
        assert_eq!(t.epoch, 7);
        assert_eq!(t.shard_count, 3);
    }

    #[test]
    fn test_inflight_snapshot() {
        let b = make_bundle();
        let i = b.inflight.unwrap();
        assert_eq!(i.active_connections, 42);
        assert_eq!(i.indoubt_txns, 0);
    }

    #[test]
    fn test_to_json_valid() {
        let b = make_bundle();
        let json = b.to_json();
        assert!(json.contains("node-1"));
        assert!(json.contains("primary"));
        assert!(json.contains("falcon_txn_active"));
        assert!(json.contains("circuit_breakers"));
        // Valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["node_id"], "node-1");
    }

    #[test]
    fn test_summary_line() {
        let b = make_bundle();
        let s = b.summary();
        assert!(s.contains("node=node-1"));
        assert!(s.contains("metrics=3"));
        assert!(s.contains("logs=2"));
    }

    #[test]
    fn test_write_to_dir() {
        let dir = std::env::temp_dir()
            .join("falcon_diag_test")
            .to_string_lossy()
            .to_string();
        let b = make_bundle();
        let path = b.write_to_dir(&dir).unwrap();
        assert!(std::path::Path::new(&path).exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("node-1"));
        // Cleanup
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_metric_with_labels() {
        let b = DiagBundleBuilder::new("node-1")
            .metric_with_labels(
                "falcon_shard_lag",
                100.0,
                [("shard".into(), "0".into())].into_iter().collect(),
            )
            .build();
        assert_eq!(b.metrics[0].labels.get("shard").unwrap(), "0");
    }

    #[test]
    fn test_context_fields() {
        let b = make_bundle();
        assert_eq!(b.context.get("version").unwrap(), "0.4.0");
    }

    #[test]
    fn test_empty_bundle() {
        let b = DiagBundleBuilder::new("node-empty").build();
        assert_eq!(b.node_id, "node-empty");
        assert!(b.topology.is_none());
        assert!(b.inflight.is_none());
        assert!(b.metrics.is_empty());
        let json = b.to_json();
        assert!(json.contains("node-empty"));
    }
}
