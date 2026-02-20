//! P3-2/P3-4/P3-5: Cloud-native deployment, control plane, and cross-region types.
//!
//! Provides the type foundations for:
//! - Kubernetes deployment configuration (P3-2)
//! - Control plane instance lifecycle (P3-4)
//! - Cross-region replication and multi-active (P3-5)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

// ── P3-2: Cloud-Native Deployment ──

/// Kubernetes deployment configuration for a CedarDB cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct K8sDeployConfig {
    /// Kubernetes namespace.
    pub namespace: String,
    /// StatefulSet name.
    pub statefulset_name: String,
    /// Number of replicas (pods).
    pub replicas: u32,
    /// Docker image for CedarDB.
    pub image: String,
    /// CPU request per pod (millicores, e.g. 1000 = 1 core).
    pub cpu_request_milli: u32,
    /// CPU limit per pod (millicores).
    pub cpu_limit_milli: u32,
    /// Memory request per pod (bytes).
    pub memory_request_bytes: u64,
    /// Memory limit per pod (bytes).
    pub memory_limit_bytes: u64,
    /// Persistent volume size per pod (bytes).
    pub storage_size_bytes: u64,
    /// Storage class name.
    pub storage_class: String,
    /// Whether anti-affinity is enabled (spread across nodes).
    pub anti_affinity: bool,
    /// Additional labels for pods.
    pub labels: HashMap<String, String>,
    /// Rolling update strategy: max surge.
    pub rolling_update_max_surge: u32,
    /// Rolling update strategy: max unavailable.
    pub rolling_update_max_unavailable: u32,
}

impl Default for K8sDeployConfig {
    fn default() -> Self {
        Self {
            namespace: "cedardb".into(),
            statefulset_name: "cedardb".into(),
            replicas: 3,
            image: "cedardb/cedardb:latest".into(),
            cpu_request_milli: 2000,
            cpu_limit_milli: 4000,
            memory_request_bytes: 4 * 1024 * 1024 * 1024,  // 4 GiB
            memory_limit_bytes: 8 * 1024 * 1024 * 1024,    // 8 GiB
            storage_size_bytes: 100 * 1024 * 1024 * 1024,   // 100 GiB
            storage_class: "standard".into(),
            anti_affinity: true,
            labels: HashMap::new(),
            rolling_update_max_surge: 1,
            rolling_update_max_unavailable: 0,
        }
    }
}

/// Health status of a single pod in the StatefulSet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl fmt::Display for PodPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Succeeded => write!(f, "succeeded"),
            Self::Failed => write!(f, "failed"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Status of a single CedarDB pod.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodHealth {
    /// Pod name (e.g. "cedardb-0").
    pub name: String,
    /// Pod phase.
    pub phase: PodPhase,
    /// Whether the pod is ready (passing readiness probes).
    pub ready: bool,
    /// Node role within the cluster (primary/replica/standalone).
    pub role: String,
    /// Shard IDs owned by this pod.
    pub shard_ids: Vec<u32>,
    /// Restart count.
    pub restarts: u32,
    /// Age in seconds.
    pub age_secs: u64,
}

// ── P3-4: Control Plane ──

/// Lifecycle state of a managed database instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceState {
    /// Being provisioned.
    Creating,
    /// Running and available.
    Running,
    /// Being scaled (up or down).
    Scaling,
    /// Being upgraded (rolling update).
    Upgrading,
    /// Stopped (but data preserved).
    Stopped,
    /// Being deleted.
    Deleting,
    /// In a failed/error state.
    Failed,
}

impl fmt::Display for InstanceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Creating => write!(f, "creating"),
            Self::Running => write!(f, "running"),
            Self::Scaling => write!(f, "scaling"),
            Self::Upgrading => write!(f, "upgrading"),
            Self::Stopped => write!(f, "stopped"),
            Self::Deleting => write!(f, "deleting"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// Managed database instance metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceMetadata {
    /// Unique instance ID.
    pub instance_id: String,
    /// Display name.
    pub name: String,
    /// Owning tenant ID.
    pub tenant_id: u64,
    /// Current state.
    pub state: InstanceState,
    /// Region where the instance is deployed.
    pub region: String,
    /// Number of nodes.
    pub node_count: u32,
    /// Number of shards.
    pub shard_count: u32,
    /// CedarDB version running.
    pub version: String,
    /// Creation time (unix seconds).
    pub created_at: u64,
    /// Last state change time (unix seconds).
    pub updated_at: u64,
    /// Kubernetes deployment config (if cloud edition).
    pub k8s_config: Option<K8sDeployConfig>,
}

/// Scale request for a managed instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleRequest {
    pub instance_id: String,
    /// Target number of nodes.
    pub target_nodes: u32,
    /// Target number of shards (optional, auto-computed if 0).
    pub target_shards: u32,
}

// ── P3-5: Cross-Region ──

/// Unique identifier for a deployment region.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RegionId(pub String);

impl fmt::Display for RegionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Configuration for a deployment region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionConfig {
    pub region_id: RegionId,
    /// Human-readable name (e.g. "US East (Virginia)").
    pub display_name: String,
    /// Whether this is the primary region for writes.
    pub is_primary: bool,
    /// Endpoint for this region's control plane.
    pub endpoint: String,
    /// Expected latency to the primary region (milliseconds).
    pub latency_to_primary_ms: u32,
}

/// Cross-region replication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossRegionConfig {
    /// Primary region.
    pub primary_region: RegionId,
    /// Replica regions with their configurations.
    pub replica_regions: Vec<RegionConfig>,
    /// Whether multi-active mode is enabled (writes in multiple regions).
    pub multi_active: bool,
    /// Maximum acceptable replication lag (milliseconds).
    pub max_replication_lag_ms: u64,
    /// Whether to enable latency-aware query routing.
    pub latency_aware_routing: bool,
}

impl CrossRegionConfig {
    /// Get all region IDs (primary + replicas).
    pub fn all_regions(&self) -> Vec<RegionId> {
        let mut regions = vec![self.primary_region.clone()];
        for r in &self.replica_regions {
            regions.push(r.region_id.clone());
        }
        regions
    }

    /// Number of total regions.
    pub fn region_count(&self) -> usize {
        1 + self.replica_regions.len()
    }
}

/// Per-region replication status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionReplicationStatus {
    pub region_id: RegionId,
    /// Current replication lag in milliseconds.
    pub lag_ms: u64,
    /// Whether the region is reachable.
    pub reachable: bool,
    /// Bytes replicated in the last reporting period.
    pub bytes_replicated: u64,
    /// Whether this region is serving read traffic.
    pub serving_reads: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_k8s_config_defaults() {
        let config = K8sDeployConfig::default();
        assert_eq!(config.replicas, 3);
        assert!(config.anti_affinity);
        assert_eq!(config.namespace, "cedardb");
    }

    #[test]
    fn test_pod_phase_display() {
        assert_eq!(PodPhase::Running.to_string(), "running");
        assert_eq!(PodPhase::Failed.to_string(), "failed");
    }

    #[test]
    fn test_instance_state_display() {
        assert_eq!(InstanceState::Running.to_string(), "running");
        assert_eq!(InstanceState::Scaling.to_string(), "scaling");
        assert_eq!(InstanceState::Creating.to_string(), "creating");
    }

    #[test]
    fn test_cross_region_config() {
        let config = CrossRegionConfig {
            primary_region: RegionId("us-east-1".into()),
            replica_regions: vec![
                RegionConfig {
                    region_id: RegionId("eu-west-1".into()),
                    display_name: "EU West".into(),
                    is_primary: false,
                    endpoint: "https://eu-west-1.cedardb.io".into(),
                    latency_to_primary_ms: 80,
                },
                RegionConfig {
                    region_id: RegionId("ap-southeast-1".into()),
                    display_name: "Asia Pacific".into(),
                    is_primary: false,
                    endpoint: "https://ap-southeast-1.cedardb.io".into(),
                    latency_to_primary_ms: 200,
                },
            ],
            multi_active: false,
            max_replication_lag_ms: 1000,
            latency_aware_routing: true,
        };
        assert_eq!(config.region_count(), 3);
        assert_eq!(config.all_regions().len(), 3);
    }

    #[test]
    fn test_region_id_display() {
        let r = RegionId("us-east-1".into());
        assert_eq!(r.to_string(), "us-east-1");
    }

    #[test]
    fn test_scale_request() {
        let req = ScaleRequest {
            instance_id: "inst-001".into(),
            target_nodes: 6,
            target_shards: 12,
        };
        assert_eq!(req.target_nodes, 6);
    }

    #[test]
    fn test_instance_metadata() {
        let meta = InstanceMetadata {
            instance_id: "inst-001".into(),
            name: "production".into(),
            tenant_id: 1,
            state: InstanceState::Running,
            region: "us-east-1".into(),
            node_count: 3,
            shard_count: 6,
            version: "0.2.0".into(),
            created_at: 1700000000,
            updated_at: 1700000100,
            k8s_config: Some(K8sDeployConfig::default()),
        };
        assert_eq!(meta.state, InstanceState::Running);
        assert!(meta.k8s_config.is_some());
    }
}
