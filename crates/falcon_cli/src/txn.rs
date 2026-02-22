use crate::client::DbClient;
use crate::format::OutputMode;
use crate::runner::format_rows_as_string;
use anyhow::Result;

/// Sub-command for \txn.
#[derive(Debug, Clone, PartialEq)]
pub enum TxnCmd {
    Active,
    Prepared,
    Detail(String),
}

impl TxnCmd {
    pub fn parse(arg: &str) -> Self {
        match arg.trim().to_lowercase().as_str() {
            "active" | "" => Self::Active,
            "prepared" => Self::Prepared,
            other => Self::Detail(other.to_string()),
        }
    }
}

pub async fn run_txn(client: &DbClient, cmd: &TxnCmd, mode: OutputMode) -> Result<String> {
    match cmd {
        TxnCmd::Active => txn_active(client, mode).await,
        TxnCmd::Prepared => txn_prepared(client, mode).await,
        TxnCmd::Detail(id) => txn_detail(client, id, mode).await,
    }
}

async fn txn_active(client: &DbClient, mode: OutputMode) -> Result<String> {
    // Try FalconDB-specific view first, fall back to pg_stat_activity
    let sql = "SELECT txn_id, state, start_time, \
                      now() - start_time AS duration, \
                      shard_scope, coordinator_node \
               FROM falcon.active_transactions \
               ORDER BY start_time";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback to pg_stat_activity
        let fallback = "SELECT pid::text AS txn_id, \
                               state, \
                               xact_start AS start_time, \
                               now() - xact_start AS duration, \
                               'unknown' AS shard_scope, \
                               client_addr::text AS coordinator_node \
                        FROM pg_stat_activity \
                        WHERE state != 'idle' AND xact_start IS NOT NULL \
                        ORDER BY xact_start";
        let (fb_rows, _) = client.query_simple(fallback).await?;
        if fb_rows.is_empty() {
            return Ok("No active transactions.\n".to_string());
        }
        return Ok(format_rows_as_string(&fb_rows, mode, "Active Transactions"));
    }

    Ok(format_rows_as_string(&rows, mode, "Active Transactions"))
}

async fn txn_prepared(client: &DbClient, mode: OutputMode) -> Result<String> {
    // Try FalconDB view, fall back to pg_prepared_xacts
    let sql = "SELECT txn_id, prepared_time, involved_shards, coordinator_node \
               FROM falcon.prepared_transactions \
               ORDER BY prepared_time";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        let fallback = "SELECT gid AS txn_id, \
                               prepared AS prepared_time, \
                               'unknown' AS involved_shards, \
                               owner AS coordinator_node \
                        FROM pg_prepared_xacts \
                        ORDER BY prepared";
        let (fb_rows, _) = client.query_simple(fallback).await?;
        if fb_rows.is_empty() {
            return Ok("No prepared (in-doubt) transactions.\n".to_string());
        }
        return Ok(format_rows_as_string(
            &fb_rows,
            mode,
            "Prepared Transactions",
        ));
    }

    Ok(format_rows_as_string(&rows, mode, "Prepared Transactions"))
}

async fn txn_detail(client: &DbClient, txn_id: &str, mode: OutputMode) -> Result<String> {
    let sql = format!(
        "SELECT txn_id, state, start_time, commit_point, \
                participants, recovery_status \
         FROM falcon.transaction_detail \
         WHERE txn_id = '{}'",
        txn_id.replace('\'', "''")
    );
    let (rows, _) = client
        .query_simple(&sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback: check pg_stat_activity by pid
        let fallback = format!(
            "SELECT pid::text AS txn_id, state, xact_start AS start_time, \
                    query AS last_query, client_addr::text AS client \
             FROM pg_stat_activity \
             WHERE pid::text = '{}'",
            txn_id.replace('\'', "''")
        );
        let (fb_rows, _) = client.query_simple(&fallback).await?;
        if fb_rows.is_empty() {
            return Ok(format!("Transaction '{}' not found.\n", txn_id));
        }
        return Ok(format_rows_as_string(
            &fb_rows,
            mode,
            &format!("Transaction {}", txn_id),
        ));
    }

    Ok(format_rows_as_string(
        &rows,
        mode,
        &format!("Transaction {}", txn_id),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_cmd_parse_active() {
        assert_eq!(TxnCmd::parse("active"), TxnCmd::Active);
        assert_eq!(TxnCmd::parse(""), TxnCmd::Active);
    }

    #[test]
    fn test_txn_cmd_parse_prepared() {
        assert_eq!(TxnCmd::parse("prepared"), TxnCmd::Prepared);
    }

    #[test]
    fn test_txn_cmd_parse_detail() {
        assert_eq!(
            TxnCmd::parse("txn-42"),
            TxnCmd::Detail("txn-42".to_string())
        );
        assert_eq!(TxnCmd::parse("12345"), TxnCmd::Detail("12345".to_string()));
    }

    #[test]
    fn test_txn_cmd_parse_case_insensitive() {
        assert_eq!(TxnCmd::parse("ACTIVE"), TxnCmd::Active);
        assert_eq!(TxnCmd::parse("PREPARED"), TxnCmd::Prepared);
    }
}
