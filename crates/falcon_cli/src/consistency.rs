use crate::client::DbClient;
use crate::format::OutputMode;
use crate::runner::format_rows_as_string;
use anyhow::Result;

/// Sub-command for \consistency.
#[derive(Debug, Clone, PartialEq)]
pub enum ConsistencyCmd {
    Status,
    InDoubt,
}

impl ConsistencyCmd {
    pub fn parse(arg: &str) -> Self {
        match arg.trim().to_lowercase().as_str() {
            "in-doubt" | "indoubt" => Self::InDoubt,
            _ => Self::Status,
        }
    }
}

pub async fn run_consistency(
    client: &DbClient,
    cmd: &ConsistencyCmd,
    mode: OutputMode,
) -> Result<String> {
    match cmd {
        ConsistencyCmd::Status => consistency_status(client, mode).await,
        ConsistencyCmd::InDoubt => consistency_in_doubt(client, mode).await,
    }
}

async fn consistency_status(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT name, setting, description \
               FROM falcon.consistency_config \
               ORDER BY name";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback: expose relevant pg_settings
        let fallback = "SELECT name, setting, short_desc AS description \
                        FROM pg_settings \
                        WHERE name IN ( \
                          'synchronous_commit', \
                          'wal_level', \
                          'max_wal_senders', \
                          'wal_sync_method', \
                          'synchronous_standby_names' \
                        ) \
                        ORDER BY name";
        let (fb_rows, _) = client.query_simple(fallback).await?;
        return Ok(format_rows_as_string(&fb_rows, mode, "Consistency Status"));
    }

    Ok(format_rows_as_string(&rows, mode, "Consistency Status"))
}

async fn consistency_in_doubt(client: &DbClient, mode: OutputMode) -> Result<String> {
    // Try FalconDB view first
    let sql = "SELECT txn_id, shards_involved, \
                      now() - prepared_at AS elapsed, \
                      recovery_recommendation \
               FROM falcon.in_doubt_transactions \
               ORDER BY prepared_at";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback: pg_prepared_xacts
        let fallback = "SELECT gid AS txn_id, \
                               'unknown' AS shards_involved, \
                               now() - prepared AS elapsed, \
                               'manual resolution required' AS recovery_recommendation \
                        FROM pg_prepared_xacts \
                        ORDER BY prepared";
        let (fb_rows, _) = client.query_simple(fallback).await?;
        if fb_rows.is_empty() {
            return Ok("No in-doubt transactions found.\n".to_string());
        }
        return Ok(format_rows_as_string(
            &fb_rows,
            mode,
            "In-Doubt Transactions",
        ));
    }

    Ok(format_rows_as_string(&rows, mode, "In-Doubt Transactions"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistency_cmd_parse_status() {
        assert_eq!(ConsistencyCmd::parse("status"), ConsistencyCmd::Status);
        assert_eq!(ConsistencyCmd::parse(""), ConsistencyCmd::Status);
        assert_eq!(ConsistencyCmd::parse("STATUS"), ConsistencyCmd::Status);
    }

    #[test]
    fn test_consistency_cmd_parse_in_doubt() {
        assert_eq!(ConsistencyCmd::parse("in-doubt"), ConsistencyCmd::InDoubt);
        assert_eq!(ConsistencyCmd::parse("indoubt"), ConsistencyCmd::InDoubt);
        assert_eq!(ConsistencyCmd::parse("IN-DOUBT"), ConsistencyCmd::InDoubt);
    }
}
