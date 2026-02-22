/// Status of a policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyStatus {
    Enabled,
    Disabled,
}

impl PolicyStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "enabled" => Some(Self::Enabled),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }
}

impl std::fmt::Display for PolicyStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Scope of a policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyScope {
    Cluster,
    Shard,
    Node,
}

impl PolicyScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
            Self::Shard => "shard",
            Self::Node => "node",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "cluster" => Some(Self::Cluster),
            "shard" => Some(Self::Shard),
            "node" => Some(Self::Node),
            _ => None,
        }
    }
}

impl std::fmt::Display for PolicyScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Supported trigger conditions (v0.6).
#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    NodeUnavailableForSecs(u64),
    ShardLeaderUnavailable,
    WalLagExceedsBytes(u64),
    MemoryPressureExceedsPct(u8),
    InDoubtTransactionsExceed(u32),
    ClusterInReadonlyMode,
}

impl Condition {
    pub fn description(&self) -> String {
        match self {
            Self::NodeUnavailableForSecs(n) => {
                format!("node unavailable for {} seconds", n)
            }
            Self::ShardLeaderUnavailable => "shard leader unavailable".to_string(),
            Self::WalLagExceedsBytes(b) => format!("WAL lag exceeds {} bytes", b),
            Self::MemoryPressureExceedsPct(p) => {
                format!("memory pressure exceeds {}%", p)
            }
            Self::InDoubtTransactionsExceed(n) => {
                format!("in-doubt transactions exceed {}", n)
            }
            Self::ClusterInReadonlyMode => "cluster is in readonly mode".to_string(),
        }
    }
}

/// Supported automated actions (v0.6).
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    DrainNode(String),
    MoveShardLeader {
        shard_id: String,
        target_node: String,
    },
    SetClusterReadonly,
    EmitAlert(String),
    RequireHumanApproval,
}

impl Action {
    pub fn description(&self) -> String {
        match self {
            Self::DrainNode(node) => format!("drain node '{}'", node),
            Self::MoveShardLeader {
                shard_id,
                target_node,
            } => {
                format!("move shard '{}' leader to '{}'", shard_id, target_node)
            }
            Self::SetClusterReadonly => "set cluster to readonly mode".to_string(),
            Self::EmitAlert(msg) => format!("emit alert: {}", msg),
            Self::RequireHumanApproval => "require human approval (block automation)".to_string(),
        }
    }
}

/// Risk level ceiling for guardrails.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum RiskCeiling {
    Low,
    Medium,
    High,
}

impl RiskCeiling {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Low => "LOW",
            Self::Medium => "MEDIUM",
            Self::High => "HIGH",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_uppercase().as_str() {
            "LOW" => Some(Self::Low),
            "MEDIUM" => Some(Self::Medium),
            "HIGH" => Some(Self::High),
            _ => None,
        }
    }
}

/// Guardrails that constrain when a policy may fire.
#[derive(Debug, Clone)]
pub struct Guardrail {
    /// Maximum number of times this policy may fire per window.
    pub max_frequency: u32,
    /// Time window in seconds for frequency counting.
    pub time_window_secs: u64,
    /// Cluster health must be at least this good (e.g. "healthy").
    pub health_prerequisite: String,
    /// Policy may not execute actions above this risk level.
    pub risk_ceiling: RiskCeiling,
    /// Minimum seconds between consecutive firings (cooldown).
    pub cooldown_secs: u64,
}

impl Default for Guardrail {
    fn default() -> Self {
        Self {
            max_frequency: 3,
            time_window_secs: 3600,
            health_prerequisite: "healthy".to_string(),
            risk_ceiling: RiskCeiling::Medium,
            cooldown_secs: 300,
        }
    }
}

/// A complete policy definition.
#[derive(Debug, Clone)]
pub struct Policy {
    pub id: String,
    pub description: String,
    pub scope: PolicyScope,
    pub status: PolicyStatus,
    pub condition: Condition,
    pub action: Action,
    pub guardrail: Guardrail,
    pub severity: String,
    pub created_by: String,
    pub created_at: String,
    pub last_evaluated_at: Option<String>,
}

impl Policy {
    #[allow(dead_code)]
    pub fn summary_row(&self) -> Vec<String> {
        vec![
            self.id.clone(),
            self.description.clone(),
            self.scope.to_string(),
            self.status.to_string(),
            self.severity.clone(),
            self.last_evaluated_at
                .clone()
                .unwrap_or_else(|| "never".to_string()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_status_parse() {
        assert_eq!(PolicyStatus::parse("enabled"), Some(PolicyStatus::Enabled));
        assert_eq!(
            PolicyStatus::parse("disabled"),
            Some(PolicyStatus::Disabled)
        );
        assert_eq!(PolicyStatus::parse("unknown"), None);
    }

    #[test]
    fn test_policy_scope_parse() {
        assert_eq!(PolicyScope::parse("cluster"), Some(PolicyScope::Cluster));
        assert_eq!(PolicyScope::parse("shard"), Some(PolicyScope::Shard));
        assert_eq!(PolicyScope::parse("node"), Some(PolicyScope::Node));
        assert_eq!(PolicyScope::parse("unknown"), None);
    }

    #[test]
    fn test_risk_ceiling_ordering() {
        assert!(RiskCeiling::Low < RiskCeiling::Medium);
        assert!(RiskCeiling::Medium < RiskCeiling::High);
    }

    #[test]
    fn test_risk_ceiling_parse() {
        assert_eq!(RiskCeiling::parse("LOW"), Some(RiskCeiling::Low));
        assert_eq!(RiskCeiling::parse("MEDIUM"), Some(RiskCeiling::Medium));
        assert_eq!(RiskCeiling::parse("HIGH"), Some(RiskCeiling::High));
        assert_eq!(RiskCeiling::parse("unknown"), None);
    }

    #[test]
    fn test_condition_description() {
        let c = Condition::NodeUnavailableForSecs(30);
        assert!(c.description().contains("30"));
        let c2 = Condition::MemoryPressureExceedsPct(85);
        assert!(c2.description().contains("85%"));
    }

    #[test]
    fn test_action_description() {
        let a = Action::DrainNode("node1".to_string());
        assert!(a.description().contains("node1"));
        let a2 = Action::EmitAlert("disk full".to_string());
        assert!(a2.description().contains("disk full"));
    }

    #[test]
    fn test_guardrail_default() {
        let g = Guardrail::default();
        assert_eq!(g.max_frequency, 3);
        assert_eq!(g.risk_ceiling, RiskCeiling::Medium);
        assert_eq!(g.cooldown_secs, 300);
    }
}
