use crate::client::DbClient;
use crate::format::OutputMode;
use crate::runner::format_rows_as_string;
use anyhow::Result;

/// Sub-command for \failover.
#[derive(Debug, Clone, PartialEq)]
pub enum FailoverCmd {
    Status,
    History,
    Simulate(String),
}

impl FailoverCmd {
    pub fn parse(arg: &str) -> Self {
        let s = arg.trim();
        let lower = s.to_lowercase();
        if lower.starts_with("simulate") {
            let node = s[8..].trim().to_string();
            return Self::Simulate(node);
        }
        match lower.as_str() {
            "history" => Self::History,
            _ => Self::Status,
        }
    }
}

pub async fn run_failover(
    client: &DbClient,
    cmd: &FailoverCmd,
    mode: OutputMode,
) -> Result<String> {
    match cmd {
        FailoverCmd::Status => failover_status(client, mode).await,
        FailoverCmd::History => failover_history(client, mode).await,
        FailoverCmd::Simulate(node_id) => failover_simulate(client, node_id, mode).await,
    }
}

async fn failover_status(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT recovery_state, ongoing_reconfiguration, \
                      affected_shards, started_at \
               FROM falcon.failover_status \
               LIMIT 1";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback: derive from pg_stat_replication and pg_is_in_recovery
        let fallback = "SELECT \
                          CASE WHEN pg_is_in_recovery() THEN 'standby' ELSE 'primary' END \
                            AS recovery_state, \
                          'none' AS ongoing_reconfiguration, \
                          'unknown' AS affected_shards, \
                          NULL::timestamptz AS started_at";
        let (fb_rows, _) = client.query_simple(fallback).await?;
        return Ok(format_rows_as_string(&fb_rows, mode, "Failover Status"));
    }

    Ok(format_rows_as_string(&rows, mode, "Failover Status"))
}

async fn failover_simulate(client: &DbClient, node_id: &str, mode: OutputMode) -> Result<String> {
    use crate::manage::plan::{PlanOutput, RiskLevel};

    if node_id.is_empty() {
        return Ok("Usage: \\failover simulate <node_id>\n".to_string());
    }

    // Query shards affected by losing this node
    let sql = format!(
        "SELECT shard_id, leader_node, follower_nodes \
         FROM falcon.shards \
         WHERE leader_node = '{}' OR follower_nodes LIKE '%{}%'",
        node_id.replace('\'', "''"),
        node_id.replace('\'', "''")
    );
    let (shard_rows, _) = client
        .query_simple(&sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    let affected_shards = if shard_rows.is_empty() {
        "unknown (falcon.shards view not available)".to_string()
    } else {
        shard_rows
            .iter()
            .filter_map(|r| r.get(0))
            .collect::<Vec<_>>()
            .join(", ")
    };

    // Query replica count for RTO estimate
    let replica_sql = "SELECT count(*) FROM pg_stat_replication";
    let replica_count = client
        .query_simple(replica_sql)
        .await
        .ok()
        .and_then(|(rows, _)| rows.into_iter().next())
        .and_then(|r| r.get(0).map(|v| v.to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    let plan = PlanOutput::new(format!("failover simulate {}", node_id), RiskLevel::Low)
        .field("Simulated Node", node_id)
        .field("Affected Shards", &affected_shards)
        .field("Replica Count", &replica_count)
        .field(
            "Estimated RTO",
            "5–30 seconds (depends on election timeout)",
        )
        .field("Data Safety", "No data loss if all writes were replicated")
        .field("Leader Candidates", "Determined by Raft election")
        .no_apply();

    Ok(plan.render(mode))
}

async fn failover_history(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT event_time, affected_shard, old_leader, new_leader, reason \
               FROM falcon.failover_history \
               ORDER BY event_time DESC \
               LIMIT 50";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok("No failover history available. \
                   (falcon.failover_history view not present or no events recorded)\n"
            .to_string());
    }

    Ok(format_rows_as_string(&rows, mode, "Failover History"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failover_cmd_parse_status() {
        assert_eq!(FailoverCmd::parse("status"), FailoverCmd::Status);
        assert_eq!(FailoverCmd::parse(""), FailoverCmd::Status);
        assert_eq!(FailoverCmd::parse("STATUS"), FailoverCmd::Status);
    }

    #[test]
    fn test_failover_cmd_parse_history() {
        assert_eq!(FailoverCmd::parse("history"), FailoverCmd::History);
        assert_eq!(FailoverCmd::parse("HISTORY"), FailoverCmd::History);
    }

    #[test]
    fn test_failover_cmd_parse_simulate() {
        assert_eq!(
            FailoverCmd::parse("simulate node1"),
            FailoverCmd::Simulate("node1".to_string())
        );
        assert_eq!(
            FailoverCmd::parse("simulate"),
            FailoverCmd::Simulate("".to_string())
        );
    }
}
