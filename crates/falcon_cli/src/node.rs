use crate::client::DbClient;
use crate::format::OutputMode;
use crate::runner::format_rows_as_string;
use anyhow::Result;

/// Sub-command for \node.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeCmd {
    Stats,
    Memory,
    Wal,
}

impl NodeCmd {
    pub fn parse(arg: &str) -> Self {
        match arg.trim().to_lowercase().as_str() {
            "memory" | "mem" => Self::Memory,
            "wal" => Self::Wal,
            _ => Self::Stats,
        }
    }
}

pub async fn run_node(client: &DbClient, cmd: &NodeCmd, mode: OutputMode) -> Result<String> {
    match cmd {
        NodeCmd::Stats => node_stats(client, mode).await,
        NodeCmd::Memory => node_memory(client, mode).await,
        NodeCmd::Wal => node_wal(client, mode).await,
    }
}

async fn node_stats(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT metric, value, unit \
               FROM falcon.node_stats \
               ORDER BY metric";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback: derive stats from pg_stat_activity and pg_stat_bgwriter
        let fallback = "SELECT 'active_connections' AS metric, \
                               count(*)::text AS value, 'count' AS unit \
                        FROM pg_stat_activity WHERE state != 'idle' \
                        UNION ALL \
                        SELECT 'total_connections', count(*)::text, 'count' \
                        FROM pg_stat_activity \
                        UNION ALL \
                        SELECT 'active_transactions', count(*)::text, 'count' \
                        FROM pg_stat_activity \
                        WHERE state != 'idle' AND xact_start IS NOT NULL \
                        UNION ALL \
                        SELECT 'server_version', current_setting('server_version'), '' \
                        ORDER BY metric";
        let (fb_rows, _) = client.query_simple(fallback).await?;
        return Ok(format_rows_as_string(&fb_rows, mode, "Node Stats"));
    }

    Ok(format_rows_as_string(&rows, mode, "Node Stats"))
}

async fn node_memory(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT metric, value, unit \
               FROM falcon.node_memory \
               ORDER BY metric";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback: pg_settings for memory-related settings
        let fallback = "SELECT name AS metric, \
                               setting AS value, \
                               unit \
                        FROM pg_settings \
                        WHERE name IN ( \
                          'shared_buffers', \
                          'work_mem', \
                          'maintenance_work_mem', \
                          'effective_cache_size', \
                          'max_connections' \
                        ) \
                        ORDER BY name";
        let (fb_rows, _) = client.query_simple(fallback).await?;
        return Ok(format_rows_as_string(&fb_rows, mode, "Node Memory"));
    }

    Ok(format_rows_as_string(&rows, mode, "Node Memory"))
}

async fn node_wal(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT metric, value, unit \
               FROM falcon.node_wal_stats \
               ORDER BY metric";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback: pg_stat_replication + pg_current_wal_lsn
        let fallback = "SELECT 'current_wal_lsn' AS metric, \
                               pg_current_wal_lsn()::text AS value, \
                               'lsn' AS unit \
                        UNION ALL \
                        SELECT 'wal_level', current_setting('wal_level'), '' \
                        UNION ALL \
                        SELECT 'max_wal_size', current_setting('max_wal_size'), 'bytes' \
                        UNION ALL \
                        SELECT 'replica_count', count(*)::text, 'count' \
                        FROM pg_stat_replication \
                        ORDER BY metric";
        let (fb_rows, _) = client.query_simple(fallback).await?;
        return Ok(format_rows_as_string(&fb_rows, mode, "Node WAL"));
    }

    Ok(format_rows_as_string(&rows, mode, "Node WAL"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_cmd_parse_stats() {
        assert_eq!(NodeCmd::parse("stats"), NodeCmd::Stats);
        assert_eq!(NodeCmd::parse(""), NodeCmd::Stats);
        assert_eq!(NodeCmd::parse("STATS"), NodeCmd::Stats);
    }

    #[test]
    fn test_node_cmd_parse_memory() {
        assert_eq!(NodeCmd::parse("memory"), NodeCmd::Memory);
        assert_eq!(NodeCmd::parse("mem"), NodeCmd::Memory);
        assert_eq!(NodeCmd::parse("MEMORY"), NodeCmd::Memory);
    }

    #[test]
    fn test_node_cmd_parse_wal() {
        assert_eq!(NodeCmd::parse("wal"), NodeCmd::Wal);
        assert_eq!(NodeCmd::parse("WAL"), NodeCmd::Wal);
    }
}
