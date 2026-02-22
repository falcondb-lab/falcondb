use crate::client::DbClient;
use crate::format::OutputMode;
use crate::manage::apply::{require_apply, ApplyResult};
use crate::manage::plan::{PlanOutput, RiskLevel};
use anyhow::Result;

/// Plan a shard move operation.
pub async fn plan_shard_move(
    client: &DbClient,
    shard_id: &str,
    target_node: &str,
    mode: OutputMode,
) -> Result<String> {
    // Query current shard state
    let sql = format!(
        "SELECT leader_node, follower_nodes, shard_state, replica_count \
         FROM falcon.shards \
         WHERE shard_id = '{}'",
        shard_id.replace('\'', "''")
    );
    let (current_leader, followers, shard_state, replica_count) = client
        .query_simple(&sql)
        .await
        .ok()
        .and_then(|(rows, _)| rows.into_iter().next())
        .map(|r| {
            (
                r.get(0).unwrap_or("unknown").to_string(),
                r.get(1).unwrap_or("unknown").to_string(),
                r.get(2).unwrap_or("unknown").to_string(),
                r.get(3).unwrap_or("unknown").to_string(),
            )
        })
        .unwrap_or_else(|| {
            (
                "unknown".to_string(),
                "unknown".to_string(),
                "unknown".to_string(),
                "unknown".to_string(),
            )
        });

    // Determine risk
    let risk = match shard_state.as_str() {
        "recovering" | "rebalancing" => RiskLevel::High,
        _ => RiskLevel::Medium,
    };

    let mut plan = PlanOutput::new(format!("shard move {} to {}", shard_id, target_node), risk)
        .field("Shard ID", shard_id)
        .field("Current Leader", &current_leader)
        .field("Target Leader", target_node)
        .field("Replica Set", &followers)
        .field("Replica Count", &replica_count)
        .field("Shard State", &shard_state)
        .field(
            "Expected Impact",
            "Brief leader election; reads may see elevated latency",
        );

    if shard_state == "recovering" || shard_state == "rebalancing" {
        plan = plan.warn(format!(
            "Shard '{}' is currently in state '{}' — moving now is HIGH RISK",
            shard_id, shard_state
        ));
    }
    if current_leader == target_node {
        plan = plan.warn(format!(
            "Target node '{}' is already the leader for shard '{}'",
            target_node, shard_id
        ));
    }

    Ok(plan.render(mode))
}

/// Apply a shard move operation.
pub async fn apply_shard_move(
    client: &DbClient,
    shard_id: &str,
    target_node: &str,
    apply: bool,
) -> Result<ApplyResult> {
    require_apply(
        apply,
        &format!("shard move {} to {}", shard_id, target_node),
    )?;

    let sql = format!(
        "SELECT falcon.admin_move_shard_leader('{}', '{}')",
        shard_id.replace('\'', "''"),
        target_node.replace('\'', "''")
    );
    match client.query_simple(&sql).await {
        Ok(_) => Ok(ApplyResult::success(
            format!("shard move {} to {}", shard_id, target_node),
            format!(
                "Shard '{}' leader transition to '{}' initiated.",
                shard_id, target_node
            ),
        )),
        Err(e) => Ok(ApplyResult::rejected(
            format!("shard move {} to {}", shard_id, target_node),
            format!("Server rejected shard move: {}", e),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_move_requires_apply() {
        let result = require_apply(false, "shard move 1 to node2");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("PLAN mode"));
    }
}
