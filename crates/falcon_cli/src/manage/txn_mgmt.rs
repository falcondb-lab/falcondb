use crate::client::DbClient;
use crate::format::OutputMode;
use crate::manage::apply::{require_apply, ApplyResult};
use crate::manage::plan::{PlanOutput, RiskLevel};
use anyhow::{bail, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum TxnResolution {
    Commit,
    Abort,
}

impl TxnResolution {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "commit" => Some(Self::Commit),
            "abort" | "rollback" => Some(Self::Abort),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Abort => "abort",
        }
    }
}

/// Plan a transaction resolution.
pub async fn plan_txn_resolve(
    client: &DbClient,
    txn_id: &str,
    output_mode: OutputMode,
) -> Result<String> {
    // Only in-doubt transactions may be resolved
    let sql = format!(
        "SELECT txn_id, state, shards_involved, commit_point, \
                now() - prepared_at AS elapsed \
         FROM falcon.in_doubt_transactions \
         WHERE txn_id = '{}'",
        txn_id.replace('\'', "''")
    );
    let (rows, _) = client
        .query_simple(&sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback: check pg_prepared_xacts
        let fallback = format!(
            "SELECT gid AS txn_id, 'prepared' AS state, \
                    'unknown' AS shards_involved, \
                    'unknown' AS commit_point, \
                    now() - prepared AS elapsed \
             FROM pg_prepared_xacts \
             WHERE gid = '{}'",
            txn_id.replace('\'', "''")
        );
        let (fb_rows, _) = client
            .query_simple(&fallback)
            .await
            .unwrap_or_else(|_| (Vec::new(), String::new()));

        if fb_rows.is_empty() {
            bail!(
                "Transaction '{}' not found in in-doubt transactions. \
                 Only prepared/in-doubt transactions may be resolved.",
                txn_id
            );
        }

        let row = &fb_rows[0];
        let state = row.get(1).unwrap_or("unknown");
        let shards = row.get(2).unwrap_or("unknown");
        let cp = row.get(3).unwrap_or("unknown");
        let elapsed = row.get(4).unwrap_or("unknown");

        let plan = PlanOutput::new(format!("txn resolve {}", txn_id), RiskLevel::High)
            .field("Transaction ID", txn_id)
            .field("State", state)
            .field("Shards Involved", shards)
            .field("Commit Point", cp)
            .field("Elapsed Since Prepare", elapsed)
            .field("Safe Options", "commit | abort")
            .warn("Resolution is irreversible — verify commit point before proceeding")
            .warn("Re-run with --apply and specify resolution: \\txn resolve <id> commit|abort");

        return Ok(plan.render(output_mode));
    }

    let row = &rows[0];
    let state = row.get(1).unwrap_or("unknown");
    let shards = row.get(2).unwrap_or("unknown");
    let cp = row.get(3).unwrap_or("unknown");
    let elapsed = row.get(4).unwrap_or("unknown");

    let plan = PlanOutput::new(format!("txn resolve {}", txn_id), RiskLevel::High)
        .field("Transaction ID", txn_id)
        .field("State", state)
        .field("Shards Involved", shards)
        .field("Commit Point", cp)
        .field("Elapsed Since Prepare", elapsed)
        .field("Safe Options", "commit | abort")
        .warn("Resolution is irreversible — verify commit point before proceeding")
        .warn("Re-run with --apply and specify resolution: \\txn resolve <id> commit|abort");

    Ok(plan.render(output_mode))
}

/// Apply a transaction resolution.
pub async fn apply_txn_resolve(
    client: &DbClient,
    txn_id: &str,
    resolution: &TxnResolution,
    apply: bool,
) -> Result<ApplyResult> {
    require_apply(apply, &format!("txn resolve {}", txn_id))?;

    let sql = match resolution {
        TxnResolution::Commit => format!(
            "SELECT falcon.admin_resolve_transaction('{}', 'commit')",
            txn_id.replace('\'', "''")
        ),
        TxnResolution::Abort => format!(
            "SELECT falcon.admin_resolve_transaction('{}', 'abort')",
            txn_id.replace('\'', "''")
        ),
    };

    match client.query_simple(&sql).await {
        Ok(_) => Ok(ApplyResult::success(
            format!("txn resolve {}", txn_id),
            format!(
                "Transaction '{}' has been {}ed. Audit record created.",
                txn_id,
                resolution.as_str()
            ),
        )),
        Err(e) => Ok(ApplyResult::rejected(
            format!("txn resolve {}", txn_id),
            format!("Server rejected resolution: {}", e),
        )),
    }
}

/// Parse "resolve <txn_id> [commit|abort]" from the argument string.
pub fn parse_txn_resolve_arg(arg: &str) -> Result<(String, Option<TxnResolution>)> {
    let parts: Vec<&str> = arg.trim().splitn(3, char::is_whitespace).collect();
    // parts[0] = "resolve", parts[1] = txn_id, parts[2] = optional resolution
    let txn_id = parts.get(1).copied().unwrap_or("").trim().to_string();
    if txn_id.is_empty() {
        bail!("Usage: \\txn resolve <txn_id> [commit|abort]");
    }
    let resolution = parts.get(2).copied().and_then(|s| TxnResolution::parse(s));
    Ok((txn_id, resolution))
}

/// Check if the argument starts with "resolve".
pub fn is_txn_resolve_cmd(arg: &str) -> bool {
    arg.trim().to_lowercase().starts_with("resolve")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_resolution_parse() {
        assert_eq!(TxnResolution::parse("commit"), Some(TxnResolution::Commit));
        assert_eq!(TxnResolution::parse("abort"), Some(TxnResolution::Abort));
        assert_eq!(TxnResolution::parse("rollback"), Some(TxnResolution::Abort));
        assert_eq!(TxnResolution::parse("unknown"), None);
    }

    #[test]
    fn test_parse_txn_resolve_arg_basic() {
        let (id, res) = parse_txn_resolve_arg("resolve txn-42").unwrap();
        assert_eq!(id, "txn-42");
        assert!(res.is_none());
    }

    #[test]
    fn test_parse_txn_resolve_arg_with_resolution() {
        let (id, res) = parse_txn_resolve_arg("resolve txn-42 commit").unwrap();
        assert_eq!(id, "txn-42");
        assert_eq!(res, Some(TxnResolution::Commit));
    }

    #[test]
    fn test_parse_txn_resolve_arg_abort() {
        let (id, res) = parse_txn_resolve_arg("resolve txn-42 abort").unwrap();
        assert_eq!(res, Some(TxnResolution::Abort));
        let _ = id;
    }

    #[test]
    fn test_parse_txn_resolve_arg_missing_id() {
        assert!(parse_txn_resolve_arg("resolve").is_err());
    }

    #[test]
    fn test_is_txn_resolve_cmd() {
        assert!(is_txn_resolve_cmd("resolve txn-42"));
        assert!(is_txn_resolve_cmd("RESOLVE txn-42 commit"));
        assert!(!is_txn_resolve_cmd("active"));
        assert!(!is_txn_resolve_cmd("prepared"));
    }

    #[test]
    fn test_txn_resolve_requires_apply() {
        let result = require_apply(false, "txn resolve txn-42");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("PLAN mode"));
    }
}
