//! P3-7: Enterprise ops tools — health scoring and capacity prediction.
//!
//! Provides automated health assessment and capacity planning:
//! - Health score (0-100) based on multiple signals
//! - Component-level health breakdown
//! - Capacity prediction based on growth trends
//! - Actionable recommendations

use serde::{Deserialize, Serialize};
use std::fmt;

/// Overall health score for the database instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    /// Overall score 0-100 (100 = perfectly healthy).
    pub overall_score: u32,
    /// Human-readable status.
    pub status: HealthStatus,
    /// Per-component scores.
    pub components: Vec<ComponentHealth>,
    /// Actionable recommendations.
    pub recommendations: Vec<String>,
}

/// High-level health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

impl fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Healthy => write!(f, "healthy"),
            Self::Warning => write!(f, "warning"),
            Self::Critical => write!(f, "critical"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

impl From<u32> for HealthStatus {
    fn from(score: u32) -> Self {
        match score {
            80..=100 => Self::Healthy,
            50..=79 => Self::Warning,
            0..=49 => Self::Critical,
            _ => Self::Unknown,
        }
    }
}

/// Health of an individual component.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub name: String,
    pub score: u32,
    pub status: HealthStatus,
    pub detail: String,
}

/// Inputs to the health scorer from various subsystems.
#[derive(Debug, Clone, Default)]
pub struct HealthInputs {
    /// Memory pressure ratio (0.0 = no pressure, 1.0 = OOM).
    pub memory_pressure_ratio: f64,
    /// WAL backlog in bytes.
    pub wal_backlog_bytes: u64,
    /// Replication lag in microseconds.
    pub replication_lag_us: u64,
    /// Number of active transactions.
    pub active_txns: usize,
    /// Transaction abort rate (0.0-1.0).
    pub txn_abort_rate: f64,
    /// OCC conflict rate (0.0-1.0).
    pub occ_conflict_rate: f64,
    /// GC safepoint stalled.
    pub gc_stalled: bool,
    /// Number of failed backups.
    pub backup_failures: u64,
    /// SLA violation count.
    pub sla_violations: u64,
    /// Disk usage ratio (0.0-1.0).
    pub disk_usage_ratio: f64,
    /// Number of connected clients.
    pub connected_clients: usize,
    /// Max connections allowed.
    pub max_connections: usize,
}

/// Health scorer — computes health reports from system metrics.
pub struct HealthScorer;

impl HealthScorer {
    /// Compute a health report from the given inputs.
    pub fn score(inputs: &HealthInputs) -> HealthReport {
        let mut components = Vec::new();
        let mut recommendations = Vec::new();

        // Memory health (weight: 20)
        let mem_score = Self::score_memory(inputs, &mut recommendations);
        components.push(ComponentHealth {
            name: "memory".into(),
            score: mem_score,
            status: HealthStatus::from(mem_score),
            detail: format!("pressure_ratio={:.2}", inputs.memory_pressure_ratio),
        });

        // WAL health (weight: 15)
        let wal_score = Self::score_wal(inputs, &mut recommendations);
        components.push(ComponentHealth {
            name: "wal".into(),
            score: wal_score,
            status: HealthStatus::from(wal_score),
            detail: format!("backlog={}B", inputs.wal_backlog_bytes),
        });

        // Replication health (weight: 15)
        let repl_score = Self::score_replication(inputs, &mut recommendations);
        components.push(ComponentHealth {
            name: "replication".into(),
            score: repl_score,
            status: HealthStatus::from(repl_score),
            detail: format!("lag={}us", inputs.replication_lag_us),
        });

        // Transaction health (weight: 20)
        let txn_score = Self::score_transactions(inputs, &mut recommendations);
        components.push(ComponentHealth {
            name: "transactions".into(),
            score: txn_score,
            status: HealthStatus::from(txn_score),
            detail: format!(
                "abort_rate={:.3} occ_rate={:.3}",
                inputs.txn_abort_rate, inputs.occ_conflict_rate
            ),
        });

        // GC health (weight: 10)
        let gc_score = Self::score_gc(inputs, &mut recommendations);
        components.push(ComponentHealth {
            name: "gc".into(),
            score: gc_score,
            status: HealthStatus::from(gc_score),
            detail: format!("stalled={}", inputs.gc_stalled),
        });

        // Storage health (weight: 10)
        let storage_score = Self::score_storage(inputs, &mut recommendations);
        components.push(ComponentHealth {
            name: "storage".into(),
            score: storage_score,
            status: HealthStatus::from(storage_score),
            detail: format!("disk_usage={:.1}%", inputs.disk_usage_ratio * 100.0),
        });

        // Connections health (weight: 10)
        let conn_score = Self::score_connections(inputs, &mut recommendations);
        components.push(ComponentHealth {
            name: "connections".into(),
            score: conn_score,
            status: HealthStatus::from(conn_score),
            detail: format!("{}/{}", inputs.connected_clients, inputs.max_connections),
        });

        // Weighted average
        let overall = (mem_score as f64 * 0.20
            + wal_score as f64 * 0.15
            + repl_score as f64 * 0.15
            + txn_score as f64 * 0.20
            + gc_score as f64 * 0.10
            + storage_score as f64 * 0.10
            + conn_score as f64 * 0.10) as u32;

        let overall = overall.min(100);

        HealthReport {
            overall_score: overall,
            status: HealthStatus::from(overall),
            components,
            recommendations,
        }
    }

    fn score_memory(inputs: &HealthInputs, recs: &mut Vec<String>) -> u32 {
        let score = if inputs.memory_pressure_ratio < 0.5 {
            100
        } else if inputs.memory_pressure_ratio < 0.8 {
            70
        } else if inputs.memory_pressure_ratio < 0.95 {
            40
        } else {
            10
        };
        if score < 70 {
            recs.push(format!(
                "Memory pressure is high ({:.0}%). Consider scaling up or reducing workload.",
                inputs.memory_pressure_ratio * 100.0
            ));
        }
        score
    }

    fn score_wal(inputs: &HealthInputs, recs: &mut Vec<String>) -> u32 {
        let backlog_mb = inputs.wal_backlog_bytes / (1024 * 1024);
        let score = if backlog_mb < 10 {
            100
        } else if backlog_mb < 100 {
            70
        } else if backlog_mb < 500 {
            40
        } else {
            10
        };
        if score < 70 {
            recs.push(format!(
                "WAL backlog is {}MB. Check replication consumers.",
                backlog_mb
            ));
        }
        score
    }

    fn score_replication(inputs: &HealthInputs, recs: &mut Vec<String>) -> u32 {
        let lag_ms = inputs.replication_lag_us / 1000;
        let score = if lag_ms < 100 {
            100
        } else if lag_ms < 1000 {
            70
        } else if lag_ms < 10000 {
            40
        } else {
            10
        };
        if score < 70 {
            recs.push(format!(
                "Replication lag is {}ms. Check replica health.",
                lag_ms
            ));
        }
        score
    }

    fn score_transactions(inputs: &HealthInputs, recs: &mut Vec<String>) -> u32 {
        let abort_penalty = (inputs.txn_abort_rate * 100.0) as u32;
        let occ_penalty = (inputs.occ_conflict_rate * 50.0) as u32;
        let score = 100u32
            .saturating_sub(abort_penalty)
            .saturating_sub(occ_penalty);
        if inputs.txn_abort_rate > 0.1 {
            recs.push(format!(
                "Transaction abort rate is {:.1}%. Investigate contention.",
                inputs.txn_abort_rate * 100.0
            ));
        }
        if inputs.sla_violations > 0 {
            recs.push(format!(
                "{} SLA violations detected. Review priority configuration.",
                inputs.sla_violations
            ));
        }
        score
    }

    fn score_gc(inputs: &HealthInputs, recs: &mut Vec<String>) -> u32 {
        if inputs.gc_stalled {
            recs.push("GC safepoint is stalled. A long-running transaction may be blocking garbage collection.".into());
            30
        } else {
            100
        }
    }

    fn score_storage(inputs: &HealthInputs, recs: &mut Vec<String>) -> u32 {
        let score = if inputs.disk_usage_ratio < 0.7 {
            100
        } else if inputs.disk_usage_ratio < 0.85 {
            60
        } else if inputs.disk_usage_ratio < 0.95 {
            30
        } else {
            5
        };
        if score < 60 {
            recs.push(format!(
                "Disk usage is {:.0}%. Plan for capacity expansion.",
                inputs.disk_usage_ratio * 100.0
            ));
        }
        score
    }

    fn score_connections(inputs: &HealthInputs, recs: &mut Vec<String>) -> u32 {
        if inputs.max_connections == 0 {
            return 100;
        }
        let ratio = inputs.connected_clients as f64 / inputs.max_connections as f64;
        let score = if ratio < 0.7 {
            100
        } else if ratio < 0.9 {
            60
        } else {
            20
        };
        if score < 60 {
            recs.push(format!(
                "Connection pool is {:.0}% full. Consider increasing max_connections.",
                ratio * 100.0
            ));
        }
        score
    }
}

/// Capacity prediction based on growth trends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityPrediction {
    /// Resource being predicted.
    pub resource: String,
    /// Current usage (bytes or count).
    pub current_usage: u64,
    /// Maximum capacity (bytes or count).
    pub max_capacity: u64,
    /// Current utilization ratio (0.0-1.0).
    pub utilization: f64,
    /// Estimated days until capacity is reached at current growth rate.
    pub days_until_full: Option<u64>,
    /// Growth rate per day.
    pub growth_rate_per_day: f64,
    /// Recommended action.
    pub recommendation: String,
}

/// Capacity predictor — estimates when resources will be exhausted.
pub struct CapacityPredictor;

impl CapacityPredictor {
    /// Predict capacity exhaustion for a resource given two data points.
    pub fn predict(
        resource: &str,
        current_usage: u64,
        max_capacity: u64,
        usage_days_ago: u64,
        days_ago: u64,
    ) -> CapacityPrediction {
        let utilization = if max_capacity > 0 {
            current_usage as f64 / max_capacity as f64
        } else {
            0.0
        };

        let growth_per_day = if days_ago > 0 && current_usage > usage_days_ago {
            (current_usage - usage_days_ago) as f64 / days_ago as f64
        } else {
            0.0
        };

        let remaining = max_capacity.saturating_sub(current_usage);
        let days_until_full = if growth_per_day > 0.0 && max_capacity > 0 {
            Some((remaining as f64 / growth_per_day) as u64)
        } else {
            None
        };

        let recommendation = match days_until_full {
            Some(d) if d < 7 => format!(
                "CRITICAL: {} will be exhausted in ~{} days. Immediate action required.",
                resource, d
            ),
            Some(d) if d < 30 => format!(
                "WARNING: {} will be exhausted in ~{} days. Plan capacity expansion.",
                resource, d
            ),
            Some(d) => format!("{} has ~{} days of capacity remaining.", resource, d),
            None if utilization > 0.9 => format!(
                "{} is at {:.0}% utilization with no growth trend.",
                resource,
                utilization * 100.0
            ),
            None => format!("{} capacity is healthy.", resource),
        };

        CapacityPrediction {
            resource: resource.into(),
            current_usage,
            max_capacity,
            utilization,
            days_until_full,
            growth_rate_per_day: growth_per_day,
            recommendation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_healthy_system() {
        let inputs = HealthInputs {
            memory_pressure_ratio: 0.3,
            wal_backlog_bytes: 1024,
            replication_lag_us: 50_000,
            active_txns: 5,
            txn_abort_rate: 0.01,
            occ_conflict_rate: 0.005,
            gc_stalled: false,
            backup_failures: 0,
            sla_violations: 0,
            disk_usage_ratio: 0.4,
            connected_clients: 10,
            max_connections: 100,
        };
        let report = HealthScorer::score(&inputs);
        assert!(report.overall_score >= 80);
        assert_eq!(report.status, HealthStatus::Healthy);
        assert!(report.recommendations.is_empty());
    }

    #[test]
    fn test_critical_memory() {
        let inputs = HealthInputs {
            memory_pressure_ratio: 0.98,
            ..HealthInputs::default()
        };
        let report = HealthScorer::score(&inputs);
        let mem = report
            .components
            .iter()
            .find(|c| c.name == "memory")
            .unwrap();
        assert_eq!(mem.score, 10);
        assert_eq!(mem.status, HealthStatus::Critical);
        assert!(!report.recommendations.is_empty());
    }

    #[test]
    fn test_gc_stalled_warning() {
        let inputs = HealthInputs {
            gc_stalled: true,
            ..HealthInputs::default()
        };
        let report = HealthScorer::score(&inputs);
        let gc = report.components.iter().find(|c| c.name == "gc").unwrap();
        assert_eq!(gc.score, 30);
    }

    #[test]
    fn test_high_abort_rate() {
        let inputs = HealthInputs {
            txn_abort_rate: 0.5,
            occ_conflict_rate: 0.2,
            ..HealthInputs::default()
        };
        let report = HealthScorer::score(&inputs);
        let txn = report
            .components
            .iter()
            .find(|c| c.name == "transactions")
            .unwrap();
        assert!(txn.score < 60);
    }

    #[test]
    fn test_disk_full_critical() {
        let inputs = HealthInputs {
            disk_usage_ratio: 0.96,
            ..HealthInputs::default()
        };
        let report = HealthScorer::score(&inputs);
        let storage = report
            .components
            .iter()
            .find(|c| c.name == "storage")
            .unwrap();
        assert_eq!(storage.score, 5);
    }

    #[test]
    fn test_health_status_from_score() {
        assert_eq!(HealthStatus::from(100), HealthStatus::Healthy);
        assert_eq!(HealthStatus::from(80), HealthStatus::Healthy);
        assert_eq!(HealthStatus::from(79), HealthStatus::Warning);
        assert_eq!(HealthStatus::from(50), HealthStatus::Warning);
        assert_eq!(HealthStatus::from(49), HealthStatus::Critical);
        assert_eq!(HealthStatus::from(0), HealthStatus::Critical);
    }

    #[test]
    fn test_capacity_prediction_growth() {
        let pred = CapacityPredictor::predict(
            "storage", 800,  // current
            1000, // max
            600,  // 10 days ago
            10,   // days
        );
        assert_eq!(pred.days_until_full, Some(10)); // 200 remaining / 20 per day = 10
        assert!((pred.growth_rate_per_day - 20.0).abs() < 0.1);
        assert!(pred.recommendation.contains("WARNING")); // 7-30 days range
    }

    #[test]
    fn test_capacity_prediction_critical() {
        let pred = CapacityPredictor::predict("memory", 950, 1000, 900, 5);
        // 50 remaining / 10 per day = 5 days
        assert_eq!(pred.days_until_full, Some(5));
        assert!(pred.recommendation.contains("CRITICAL"));
    }

    #[test]
    fn test_capacity_prediction_no_growth() {
        let pred = CapacityPredictor::predict("disk", 500, 1000, 500, 30);
        assert!(pred.days_until_full.is_none());
        assert!((pred.growth_rate_per_day - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_all_components_present() {
        let inputs = HealthInputs::default();
        let report = HealthScorer::score(&inputs);
        assert_eq!(report.components.len(), 7);
        let names: Vec<&str> = report.components.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"memory"));
        assert!(names.contains(&"wal"));
        assert!(names.contains(&"replication"));
        assert!(names.contains(&"transactions"));
        assert!(names.contains(&"gc"));
        assert!(names.contains(&"storage"));
        assert!(names.contains(&"connections"));
    }
}
