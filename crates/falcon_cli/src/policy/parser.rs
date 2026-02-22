use crate::policy::model::{
    Action, Condition, Guardrail, Policy, PolicyScope, PolicyStatus, RiskCeiling,
};
use anyhow::{bail, Result};

/// Parse a `\policy create` command from a key=value argument string.
/// Format: \policy create id=<id> description=<desc> scope=<scope>
///         condition=<cond> action=<action> severity=<sev>
///         [max_frequency=N] [time_window=N] [cooldown=N]
///         [risk_ceiling=LOW|MEDIUM|HIGH] [health_prereq=<s>]
pub fn parse_policy_create(arg: &str) -> Result<Policy> {
    let kv = parse_kv_pairs(arg);

    let id = require_key(&kv, "id")?;
    let description = kv.get("description").cloned().unwrap_or_else(|| id.clone());
    let scope_str = kv
        .get("scope")
        .cloned()
        .unwrap_or_else(|| "cluster".to_string());
    let scope = PolicyScope::parse(&scope_str).ok_or_else(|| {
        anyhow::anyhow!("Unknown scope '{}'. Use: cluster, shard, node", scope_str)
    })?;

    let condition_str = require_key(&kv, "condition")?;
    let condition = parse_condition(&condition_str)?;

    let action_str = require_key(&kv, "action")?;
    let action = parse_action(&action_str)?;

    let severity = kv
        .get("severity")
        .cloned()
        .unwrap_or_else(|| "warning".to_string());

    let max_frequency = kv
        .get("max_frequency")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(3);
    let time_window_secs = kv
        .get("time_window")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(3600);
    let cooldown_secs = kv
        .get("cooldown")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(300);
    let risk_ceiling_str = kv
        .get("risk_ceiling")
        .cloned()
        .unwrap_or_else(|| "MEDIUM".to_string());
    let risk_ceiling = RiskCeiling::parse(&risk_ceiling_str).ok_or_else(|| {
        anyhow::anyhow!(
            "Unknown risk_ceiling '{}'. Use: LOW, MEDIUM, HIGH",
            risk_ceiling_str
        )
    })?;
    let health_prerequisite = kv
        .get("health_prereq")
        .cloned()
        .unwrap_or_else(|| "healthy".to_string());

    Ok(Policy {
        id,
        description,
        scope,
        status: PolicyStatus::Disabled,
        condition,
        action,
        guardrail: Guardrail {
            max_frequency,
            time_window_secs,
            cooldown_secs,
            risk_ceiling,
            health_prerequisite,
        },
        severity,
        created_by: "operator".to_string(),
        created_at: chrono_now(),
        last_evaluated_at: None,
    })
}

fn parse_condition(s: &str) -> Result<Condition> {
    let lower = s.trim().to_lowercase();
    if let Some(rest) = lower.strip_prefix("node_unavailable_secs=") {
        let n = rest
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("Invalid seconds in condition: {}", s))?;
        return Ok(Condition::NodeUnavailableForSecs(n));
    }
    if let Some(rest) = lower.strip_prefix("wal_lag_bytes=") {
        let n = rest
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("Invalid bytes in condition: {}", s))?;
        return Ok(Condition::WalLagExceedsBytes(n));
    }
    if let Some(rest) = lower.strip_prefix("memory_pct=") {
        let n = rest
            .parse::<u8>()
            .map_err(|_| anyhow::anyhow!("Invalid pct in condition: {}", s))?;
        return Ok(Condition::MemoryPressureExceedsPct(n));
    }
    if let Some(rest) = lower.strip_prefix("in_doubt_txns=") {
        let n = rest
            .parse::<u32>()
            .map_err(|_| anyhow::anyhow!("Invalid count in condition: {}", s))?;
        return Ok(Condition::InDoubtTransactionsExceed(n));
    }
    match lower.as_str() {
        "shard_leader_unavailable" => Ok(Condition::ShardLeaderUnavailable),
        "cluster_readonly" => Ok(Condition::ClusterInReadonlyMode),
        _ => bail!(
            "Unknown condition '{}'. Supported: node_unavailable_secs=N, \
             wal_lag_bytes=N, memory_pct=N, in_doubt_txns=N, \
             shard_leader_unavailable, cluster_readonly",
            s
        ),
    }
}

fn parse_action(s: &str) -> Result<Action> {
    let lower = s.trim().to_lowercase();
    if let Some(rest) = lower.strip_prefix("drain_node=") {
        return Ok(Action::DrainNode(rest.trim().to_string()));
    }
    if let Some(rest) = lower.strip_prefix("move_shard=") {
        // format: move_shard=<shard_id>:<target_node>
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        if parts.len() != 2 {
            bail!("move_shard format: move_shard=<shard_id>:<target_node>");
        }
        return Ok(Action::MoveShardLeader {
            shard_id: parts[0].trim().to_string(),
            target_node: parts[1].trim().to_string(),
        });
    }
    if let Some(rest) = lower.strip_prefix("alert=") {
        return Ok(Action::EmitAlert(rest.trim().to_string()));
    }
    match lower.as_str() {
        "set_cluster_readonly" => Ok(Action::SetClusterReadonly),
        "require_human_approval" => Ok(Action::RequireHumanApproval),
        _ => bail!(
            "Unknown action '{}'. Supported: drain_node=<node>, \
             move_shard=<shard>:<node>, set_cluster_readonly, \
             alert=<message>, require_human_approval",
            s
        ),
    }
}

/// Parse space-separated key=value pairs, handling quoted values.
fn parse_kv_pairs(s: &str) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    let mut rest = s.trim();
    while !rest.is_empty() {
        // Find next key=
        let eq_pos = match rest.find('=') {
            Some(p) => p,
            None => break,
        };
        let key = rest[..eq_pos].trim().to_lowercase();
        rest = &rest[eq_pos + 1..];

        // Value: quoted or space-terminated
        let value = if rest.starts_with('"') {
            let end = rest[1..].find('"').map(|p| p + 1).unwrap_or(rest.len() - 1);
            let v = rest[1..end].to_string();
            rest = rest[end + 1..].trim_start();
            v
        } else {
            let end = rest.find(' ').unwrap_or(rest.len());
            let v = rest[..end].to_string();
            rest = rest[end..].trim_start();
            v
        };

        if !key.is_empty() {
            map.insert(key, value);
        }
    }
    map
}

fn require_key(kv: &std::collections::HashMap<String, String>, key: &str) -> Result<String> {
    kv.get(key)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Missing required field '{}' in \\policy create", key))
}

fn chrono_now() -> String {
    // Use a simple timestamp without chrono dependency
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| format!("unix:{}", d.as_secs()))
        .unwrap_or_else(|_| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kv_pairs_basic() {
        let kv = parse_kv_pairs("id=pol1 scope=cluster condition=cluster_readonly");
        assert_eq!(kv.get("id").unwrap(), "pol1");
        assert_eq!(kv.get("scope").unwrap(), "cluster");
        assert_eq!(kv.get("condition").unwrap(), "cluster_readonly");
    }

    #[test]
    fn test_parse_condition_node_unavailable() {
        let c = parse_condition("node_unavailable_secs=30").unwrap();
        assert_eq!(c, Condition::NodeUnavailableForSecs(30));
    }

    #[test]
    fn test_parse_condition_wal_lag() {
        let c = parse_condition("wal_lag_bytes=1048576").unwrap();
        assert_eq!(c, Condition::WalLagExceedsBytes(1048576));
    }

    #[test]
    fn test_parse_condition_memory_pct() {
        let c = parse_condition("memory_pct=85").unwrap();
        assert_eq!(c, Condition::MemoryPressureExceedsPct(85));
    }

    #[test]
    fn test_parse_condition_in_doubt() {
        let c = parse_condition("in_doubt_txns=5").unwrap();
        assert_eq!(c, Condition::InDoubtTransactionsExceed(5));
    }

    #[test]
    fn test_parse_condition_named() {
        assert_eq!(
            parse_condition("shard_leader_unavailable").unwrap(),
            Condition::ShardLeaderUnavailable
        );
        assert_eq!(
            parse_condition("cluster_readonly").unwrap(),
            Condition::ClusterInReadonlyMode
        );
    }

    #[test]
    fn test_parse_condition_invalid() {
        assert!(parse_condition("unknown_condition").is_err());
    }

    #[test]
    fn test_parse_action_drain_node() {
        let a = parse_action("drain_node=node1").unwrap();
        assert_eq!(a, Action::DrainNode("node1".to_string()));
    }

    #[test]
    fn test_parse_action_move_shard() {
        let a = parse_action("move_shard=shard1:node2").unwrap();
        assert_eq!(
            a,
            Action::MoveShardLeader {
                shard_id: "shard1".to_string(),
                target_node: "node2".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_action_alert() {
        let a = parse_action("alert=high memory pressure").unwrap();
        assert_eq!(a, Action::EmitAlert("high memory pressure".to_string()));
    }

    #[test]
    fn test_parse_action_named() {
        assert_eq!(
            parse_action("set_cluster_readonly").unwrap(),
            Action::SetClusterReadonly
        );
        assert_eq!(
            parse_action("require_human_approval").unwrap(),
            Action::RequireHumanApproval
        );
    }

    #[test]
    fn test_parse_policy_create_minimal() {
        let p =
            parse_policy_create("id=pol1 condition=cluster_readonly action=require_human_approval")
                .unwrap();
        assert_eq!(p.id, "pol1");
        assert_eq!(p.condition, Condition::ClusterInReadonlyMode);
        assert_eq!(p.action, Action::RequireHumanApproval);
        assert_eq!(p.status, PolicyStatus::Disabled);
    }

    #[test]
    fn test_parse_policy_create_full() {
        let p = parse_policy_create(
            "id=pol2 description=test scope=node condition=memory_pct=90 \
             action=drain_node=node1 severity=critical max_frequency=2 \
             time_window=1800 cooldown=600 risk_ceiling=HIGH",
        )
        .unwrap();
        assert_eq!(p.id, "pol2");
        assert_eq!(p.scope, PolicyScope::Node);
        assert_eq!(p.guardrail.max_frequency, 2);
        assert_eq!(p.guardrail.risk_ceiling, RiskCeiling::High);
        assert_eq!(p.guardrail.cooldown_secs, 600);
    }

    #[test]
    fn test_parse_policy_create_missing_id() {
        assert!(
            parse_policy_create("condition=cluster_readonly action=require_human_approval")
                .is_err()
        );
    }
}
