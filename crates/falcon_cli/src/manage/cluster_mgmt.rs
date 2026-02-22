use crate::client::DbClient;
use crate::format::OutputMode;
use crate::manage::apply::{require_apply, ApplyResult};
use crate::manage::plan::{PlanOutput, RiskLevel};
use anyhow::{bail, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum ClusterMode {
    ReadOnly,
    ReadWrite,
}

impl ClusterMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "readonly" | "read-only" | "read_only" => Some(Self::ReadOnly),
            "readwrite" | "read-write" | "read_write" | "normal" => Some(Self::ReadWrite),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadOnly => "readonly",
            Self::ReadWrite => "readwrite",
        }
    }
}

/// Plan a cluster mode change.
pub async fn plan_cluster_mode(
    client: &DbClient,
    mode_target: &ClusterMode,
    output_mode: OutputMode,
) -> Result<String> {
    // Query current cluster mode
    let sql = "SELECT setting FROM falcon.cluster_config WHERE name = 'cluster_mode'";
    let current_mode = client
        .query_simple(sql)
        .await
        .ok()
        .and_then(|(rows, _)| rows.into_iter().next())
        .and_then(|r| r.get(0).map(|v| v.to_string()))
        .unwrap_or_else(|| "readwrite".to_string());

    // Count active writers
    let writers_sql = "SELECT count(*) FROM falcon.active_transactions \
                       WHERE txn_type = 'write'";
    let active_writers = client
        .query_simple(writers_sql)
        .await
        .ok()
        .and_then(|(rows, _)| rows.into_iter().next())
        .and_then(|r| r.get(0).map(|v| v.to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    let risk = match mode_target {
        ClusterMode::ReadOnly => RiskLevel::High,
        ClusterMode::ReadWrite => RiskLevel::Medium,
    };

    let effect = match mode_target {
        ClusterMode::ReadOnly => {
            "New write transactions will be rejected; reads and admin inspection remain available"
        }
        ClusterMode::ReadWrite => "Normal read/write operation will be restored",
    };

    let mut plan = PlanOutput::new(format!("cluster mode {}", mode_target.as_str()), risk)
        .field("Current Mode", &current_mode)
        .field("Target Mode", mode_target.as_str())
        .field("Active Writers", &active_writers)
        .field("Effect", effect);

    if *mode_target == ClusterMode::ReadOnly && active_writers != "0" && active_writers != "unknown"
    {
        plan = plan.warn(format!(
            "{} active write transaction(s) will be blocked when mode switches",
            active_writers
        ));
    }
    if current_mode == mode_target.as_str() {
        plan = plan.warn(format!(
            "Cluster is already in '{}' mode — this is a no-op",
            current_mode
        ));
    }

    Ok(plan.render(output_mode))
}

/// Apply a cluster mode change.
pub async fn apply_cluster_mode(
    client: &DbClient,
    mode_target: &ClusterMode,
    apply: bool,
) -> Result<ApplyResult> {
    require_apply(apply, &format!("cluster mode {}", mode_target.as_str()))?;

    let sql = format!(
        "SELECT falcon.admin_set_cluster_mode('{}')",
        mode_target.as_str()
    );
    match client.query_simple(&sql).await {
        Ok(_) => Ok(ApplyResult::success(
            format!("cluster mode {}", mode_target.as_str()),
            format!("Cluster mode has been set to '{}'.", mode_target.as_str()),
        )),
        Err(e) => Ok(ApplyResult::rejected(
            format!("cluster mode {}", mode_target.as_str()),
            format!("Server rejected mode change: {}", e),
        )),
    }
}

/// Parse "mode <target>" from the argument string.
pub fn parse_cluster_mode_arg(arg: &str) -> Result<ClusterMode> {
    let parts: Vec<&str> = arg.trim().splitn(2, char::is_whitespace).collect();
    let target = parts.get(1).copied().unwrap_or("").trim();
    ClusterMode::parse(target).ok_or_else(|| {
        anyhow::anyhow!(
            "Unknown cluster mode '{}'. Use 'readonly' or 'readwrite'.",
            target
        )
    })
}

/// Check if the argument starts with "mode".
pub fn is_cluster_mode_cmd(arg: &str) -> bool {
    arg.trim().to_lowercase().starts_with("mode")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_mode_parse_readonly() {
        assert_eq!(ClusterMode::parse("readonly"), Some(ClusterMode::ReadOnly));
        assert_eq!(ClusterMode::parse("read-only"), Some(ClusterMode::ReadOnly));
        assert_eq!(ClusterMode::parse("READONLY"), Some(ClusterMode::ReadOnly));
    }

    #[test]
    fn test_cluster_mode_parse_readwrite() {
        assert_eq!(
            ClusterMode::parse("readwrite"),
            Some(ClusterMode::ReadWrite)
        );
        assert_eq!(ClusterMode::parse("normal"), Some(ClusterMode::ReadWrite));
    }

    #[test]
    fn test_cluster_mode_parse_invalid() {
        assert_eq!(ClusterMode::parse("unknown"), None);
        assert_eq!(ClusterMode::parse(""), None);
    }

    #[test]
    fn test_parse_cluster_mode_arg() {
        let m = parse_cluster_mode_arg("mode readonly").unwrap();
        assert_eq!(m, ClusterMode::ReadOnly);
        let m2 = parse_cluster_mode_arg("mode readwrite").unwrap();
        assert_eq!(m2, ClusterMode::ReadWrite);
    }

    #[test]
    fn test_parse_cluster_mode_arg_invalid() {
        assert!(parse_cluster_mode_arg("mode foobar").is_err());
    }

    #[test]
    fn test_is_cluster_mode_cmd() {
        assert!(is_cluster_mode_cmd("mode readonly"));
        assert!(is_cluster_mode_cmd("MODE READWRITE"));
        assert!(!is_cluster_mode_cmd("nodes"));
        assert!(!is_cluster_mode_cmd("shards"));
    }

    #[test]
    fn test_cluster_mode_requires_apply() {
        let result = require_apply(false, "cluster mode readonly");
        assert!(result.is_err());
    }
}
