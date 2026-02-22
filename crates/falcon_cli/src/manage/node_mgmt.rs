use crate::client::DbClient;
use crate::format::OutputMode;
use crate::manage::apply::{require_apply, ApplyResult};
use crate::manage::plan::{PlanOutput, RiskLevel};
use anyhow::Result;

/// Plan a node drain operation.
pub async fn plan_node_drain(client: &DbClient, node_id: &str, mode: OutputMode) -> Result<String> {
    // Query active transaction count on the node
    let sql = format!(
        "SELECT count(*) AS active_txns \
         FROM falcon.active_transactions \
         WHERE coordinator_node = '{}'",
        node_id.replace('\'', "''")
    );
    let active_txns = client
        .query_simple(&sql)
        .await
        .ok()
        .and_then(|(rows, _)| rows.into_iter().next())
        .and_then(|r| r.get(0).map(|v| v.to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    // Query node health
    let health_sql = format!(
        "SELECT health_status, role \
         FROM falcon.cluster_nodes \
         WHERE node_id = '{}'",
        node_id.replace('\'', "''")
    );
    let (health, role) = client
        .query_simple(&health_sql)
        .await
        .ok()
        .and_then(|(rows, _)| rows.into_iter().next())
        .map(|r| {
            (
                r.get(0).unwrap_or("unknown").to_string(),
                r.get(1).unwrap_or("unknown").to_string(),
            )
        })
        .unwrap_or_else(|| ("unknown".to_string(), "unknown".to_string()));

    let risk = if role == "leader" {
        RiskLevel::High
    } else {
        RiskLevel::Medium
    };

    let mut plan = PlanOutput::new(format!("node drain {}", node_id), risk)
        .field("Node ID", node_id)
        .field("Current Role", &role)
        .field("Health Status", &health)
        .field("Active Transactions", &active_txns)
        .field("Effect", "New transactions will not be routed to this node");

    if role == "leader" {
        plan = plan.warn(format!(
            "Node '{}' is a leader — draining may trigger leader election",
            node_id
        ));
    }
    if active_txns != "0" && active_txns != "unknown" {
        plan = plan.warn(format!(
            "{} active transaction(s) must complete before drain finishes",
            active_txns
        ));
    }

    Ok(plan.render(mode))
}

/// Apply a node drain operation.
pub async fn apply_node_drain(
    client: &DbClient,
    node_id: &str,
    apply: bool,
) -> Result<ApplyResult> {
    require_apply(apply, &format!("node drain {}", node_id))?;

    let sql = format!(
        "SELECT falcon.admin_drain_node('{}')",
        node_id.replace('\'', "''")
    );
    match client.query_simple(&sql).await {
        Ok(_) => Ok(ApplyResult::success(
            format!("node drain {}", node_id),
            format!(
                "Node '{}' is now draining. No new transactions will be routed to it.",
                node_id
            ),
        )),
        Err(e) => Ok(ApplyResult::rejected(
            format!("node drain {}", node_id),
            format!("Server rejected drain: {}", e),
        )),
    }
}

/// Plan a node resume operation.
pub async fn plan_node_resume(
    client: &DbClient,
    node_id: &str,
    mode: OutputMode,
) -> Result<String> {
    let health_sql = format!(
        "SELECT health_status, role \
         FROM falcon.cluster_nodes \
         WHERE node_id = '{}'",
        node_id.replace('\'', "''")
    );
    let (health, role) = client
        .query_simple(&health_sql)
        .await
        .ok()
        .and_then(|(rows, _)| rows.into_iter().next())
        .map(|r| {
            (
                r.get(0).unwrap_or("unknown").to_string(),
                r.get(1).unwrap_or("unknown").to_string(),
            )
        })
        .unwrap_or_else(|| ("unknown".to_string(), "unknown".to_string()));

    let risk = if health == "degraded" {
        RiskLevel::High
    } else {
        RiskLevel::Low
    };

    let mut plan = PlanOutput::new(format!("node resume {}", node_id), risk)
        .field("Node ID", node_id)
        .field("Current Role", &role)
        .field("Health Status", &health)
        .field(
            "Effect",
            "Transaction routing will be re-enabled for this node",
        );

    if health == "degraded" {
        plan = plan.warn(format!(
            "Node '{}' health is degraded — resuming may route traffic to an unhealthy node",
            node_id
        ));
    }

    Ok(plan.render(mode))
}

/// Apply a node resume operation.
pub async fn apply_node_resume(
    client: &DbClient,
    node_id: &str,
    apply: bool,
) -> Result<ApplyResult> {
    require_apply(apply, &format!("node resume {}", node_id))?;

    let sql = format!(
        "SELECT falcon.admin_resume_node('{}')",
        node_id.replace('\'', "''")
    );
    match client.query_simple(&sql).await {
        Ok(_) => Ok(ApplyResult::success(
            format!("node resume {}", node_id),
            format!(
                "Node '{}' has been resumed. Transaction routing is re-enabled.",
                node_id
            ),
        )),
        Err(e) => Ok(ApplyResult::rejected(
            format!("node resume {}", node_id),
            format!("Server rejected resume: {}", e),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_node_drain_requires_apply_flag() {
        // We can't run async in a sync test without a runtime, but we can test
        // the require_apply guard directly
        let result = require_apply(false, "node drain node1");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("PLAN mode"));
    }

    #[test]
    fn test_apply_node_resume_requires_apply_flag() {
        let result = require_apply(false, "node resume node1");
        assert!(result.is_err());
    }
}
