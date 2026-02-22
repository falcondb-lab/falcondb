use crate::client::DbClient;
use crate::format::OutputMode;
use crate::runner::format_rows_as_string;
use anyhow::Result;

/// Sub-command for \cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterCmd {
    Overview,
    Nodes,
    Leaders,
    Shards,
}

impl ClusterCmd {
    pub fn parse(arg: &str) -> Self {
        match arg.trim().to_lowercase().as_str() {
            "nodes" => Self::Nodes,
            "leaders" => Self::Leaders,
            "shards" => Self::Shards,
            _ => Self::Overview,
        }
    }
}

pub async fn run_cluster(client: &DbClient, cmd: &ClusterCmd, mode: OutputMode) -> Result<String> {
    match cmd {
        ClusterCmd::Overview => cluster_overview(client, mode).await,
        ClusterCmd::Nodes => cluster_nodes(client, mode).await,
        ClusterCmd::Leaders => cluster_leaders(client, mode).await,
        ClusterCmd::Shards => cluster_shards(client, mode).await,
    }
}

async fn cluster_overview(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT name, setting FROM falcon.cluster_info ORDER BY name";
    let (rows, _) = client.query_simple(sql).await.unwrap_or_else(|_| {
        // Fallback: use pg_settings for basic info
        (Vec::new(), String::new())
    });

    if rows.is_empty() {
        // Graceful fallback when falcon.cluster_info is not available
        let fallback_sql =
            "SELECT 'server_version' AS name, current_setting('server_version') AS setting \
                            UNION ALL \
                            SELECT 'connected_node', inet_server_addr()::text \
                            UNION ALL \
                            SELECT 'server_port', inet_server_port()::text";
        let (fb_rows, _) = client.query_simple(fallback_sql).await?;
        return Ok(format_rows_as_string(&fb_rows, mode, "Cluster Overview"));
    }

    Ok(format_rows_as_string(&rows, mode, "Cluster Overview"))
}

async fn cluster_nodes(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT node_id, address, role, health_status, uptime \
               FROM falcon.cluster_nodes \
               ORDER BY role DESC, node_id";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback: show current connection info
        let fallback = "SELECT 'local' AS node_id, \
                               inet_server_addr()::text AS address, \
                               'unknown' AS role, \
                               'up' AS health_status, \
                               NULL AS uptime";
        let (fb_rows, _) = client.query_simple(fallback).await?;
        return Ok(format_rows_as_string(&fb_rows, mode, "Cluster Nodes"));
    }

    Ok(format_rows_as_string(&rows, mode, "Cluster Nodes"))
}

async fn cluster_leaders(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT shard_id, leader_node, replica_count \
               FROM falcon.shard_leaders \
               ORDER BY shard_id";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok("No shard leader information available. \
                   (falcon.shard_leaders view not present)\n"
            .to_string());
    }

    Ok(format_rows_as_string(&rows, mode, "Shard Leaders"))
}

async fn cluster_shards(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT shard_id, partition_desc, leader_node, follower_nodes, shard_state \
               FROM falcon.shards \
               ORDER BY shard_id";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok("No shard information available. \
                   (falcon.shards view not present)\n"
            .to_string());
    }

    Ok(format_rows_as_string(&rows, mode, "Cluster Shards"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_cmd_parse_overview() {
        assert_eq!(ClusterCmd::parse(""), ClusterCmd::Overview);
        assert_eq!(ClusterCmd::parse("overview"), ClusterCmd::Overview);
    }

    #[test]
    fn test_cluster_cmd_parse_nodes() {
        assert_eq!(ClusterCmd::parse("nodes"), ClusterCmd::Nodes);
        assert_eq!(ClusterCmd::parse("NODES"), ClusterCmd::Nodes);
    }

    #[test]
    fn test_cluster_cmd_parse_leaders() {
        assert_eq!(ClusterCmd::parse("leaders"), ClusterCmd::Leaders);
    }

    #[test]
    fn test_cluster_cmd_parse_shards() {
        assert_eq!(ClusterCmd::parse("shards"), ClusterCmd::Shards);
    }
}
