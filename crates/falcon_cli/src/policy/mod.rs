pub mod automation;
pub mod guardrail;
pub mod model;
pub mod parser;
pub mod simulator;

use crate::client::DbClient;
use crate::format::OutputMode;
use crate::manage::apply::require_apply;
use crate::manage::plan::{PlanOutput, RiskLevel};
use crate::policy::model::PolicyStatus;
use crate::policy::parser::parse_policy_create;
use crate::policy::simulator::{simulate_local_policy, simulate_policy};
use crate::runner::format_rows_as_string;
use anyhow::{bail, Result};

/// Sub-command for \policy.
#[derive(Debug, Clone, PartialEq)]
pub enum PolicyCmd {
    List,
    Show(String),
    Create(String),
    Enable(String),
    Disable(String),
    Delete(String),
    Simulate(String),
    Audit(String),
}

impl PolicyCmd {
    pub fn parse(arg: &str) -> Result<Self> {
        let s = arg.trim();
        let lower = s.to_lowercase();
        let (verb, rest) = split_verb(s);
        match verb.as_str() {
            "list" | "" => Ok(Self::List),
            "show" => {
                if rest.is_empty() {
                    bail!("Usage: \\policy show <policy_id>");
                }
                Ok(Self::Show(rest))
            }
            "create" => Ok(Self::Create(rest)),
            "enable" => {
                if rest.is_empty() {
                    bail!("Usage: \\policy enable <policy_id>");
                }
                Ok(Self::Enable(rest))
            }
            "disable" => {
                if rest.is_empty() {
                    bail!("Usage: \\policy disable <policy_id>");
                }
                Ok(Self::Disable(rest))
            }
            "delete" => {
                if rest.is_empty() {
                    bail!("Usage: \\policy delete <policy_id>");
                }
                Ok(Self::Delete(rest))
            }
            "simulate" => {
                if rest.is_empty() {
                    bail!("Usage: \\policy simulate <policy_id>");
                }
                Ok(Self::Simulate(rest))
            }
            "audit" => Ok(Self::Audit(rest)),
            _ => bail!(
                "Unknown \\policy sub-command '{}'. Try: list, show, create, enable, disable, delete, simulate, audit",
                lower
            ),
        }
    }
}

fn split_verb(s: &str) -> (String, String) {
    let mut parts = s.splitn(2, char::is_whitespace);
    let verb = parts.next().unwrap_or("").to_lowercase();
    let rest = parts.next().unwrap_or("").trim().to_string();
    (verb, rest)
}

pub async fn run_policy(
    client: &DbClient,
    cmd: &PolicyCmd,
    mode: OutputMode,
    apply: bool,
) -> Result<String> {
    match cmd {
        PolicyCmd::List => policy_list(client, mode).await,
        PolicyCmd::Show(id) => policy_show(client, id, mode).await,
        PolicyCmd::Create(arg) => policy_create(client, arg, mode, apply).await,
        PolicyCmd::Enable(id) => policy_set_status(client, id, PolicyStatus::Enabled, apply).await,
        PolicyCmd::Disable(id) => {
            policy_set_status(client, id, PolicyStatus::Disabled, apply).await
        }
        PolicyCmd::Delete(id) => policy_delete(client, id, apply).await,
        PolicyCmd::Simulate(id) => simulate_policy(client, id, mode).await,
        PolicyCmd::Audit(id) => policy_audit(client, id, mode).await,
    }
}

async fn policy_list(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT policy_id, description, scope, status, severity, \
                      last_evaluated_at \
               FROM falcon.policies \
               ORDER BY status DESC, policy_id";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok("No policies defined. Use \\policy create to define one.\n".to_string());
    }

    Ok(format_rows_as_string(&rows, mode, "Policies"))
}

async fn policy_show(client: &DbClient, policy_id: &str, mode: OutputMode) -> Result<String> {
    let sql = format!(
        "SELECT policy_id, description, scope, status, condition_expr, \
                action_expr, severity, guardrail_config, \
                created_by, created_at, last_evaluated_at \
         FROM falcon.policies \
         WHERE policy_id = '{}'",
        policy_id.replace('\'', "''")
    );
    let (rows, _) = client
        .query_simple(&sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok(format!("Policy '{}' not found.\n", policy_id));
    }

    Ok(format_rows_as_string(
        &rows,
        mode,
        &format!("Policy {}", policy_id),
    ))
}

async fn policy_create(
    client: &DbClient,
    arg: &str,
    mode: OutputMode,
    apply: bool,
) -> Result<String> {
    let policy = parse_policy_create(arg)?;

    // Always show plan first
    let plan = PlanOutput::new(format!("policy create {}", policy.id), RiskLevel::Low)
        .field("Policy ID", &policy.id)
        .field("Description", &policy.description)
        .field("Scope", policy.scope.as_str())
        .field("Condition", &policy.condition.description())
        .field("Action", &policy.action.description())
        .field("Severity", &policy.severity)
        .field("Initial Status", "disabled (must be explicitly enabled)")
        .field(
            "Guardrail",
            &format!(
                "max_frequency={} time_window={}s cooldown={}s risk_ceiling={}",
                policy.guardrail.max_frequency,
                policy.guardrail.time_window_secs,
                policy.guardrail.cooldown_secs,
                policy.guardrail.risk_ceiling.as_str()
            ),
        );

    if !apply {
        return Ok(plan.render(mode));
    }

    require_apply(apply, &format!("policy create {}", policy.id))?;

    // Persist to server
    let sql = format!(
        "SELECT falcon.admin_create_policy('{}', '{}', '{}', '{}', '{}', '{}')",
        policy.id.replace('\'', "''"),
        policy.description.replace('\'', "''"),
        policy.scope.as_str(),
        policy.condition.description().replace('\'', "''"),
        policy.action.description().replace('\'', "''"),
        policy.severity.replace('\'', "''"),
    );
    match client.query_simple(&sql).await {
        Ok(_) => Ok(format!(
            "Policy '{}' created successfully (status: disabled).\n\
             Use \\policy enable {} to activate it.\n",
            policy.id, policy.id
        )),
        Err(e) => Ok(format!(
            "Policy '{}' creation rejected by server: {}\n\
             (falcon.admin_create_policy() may not be available)\n",
            policy.id, e
        )),
    }
}

async fn policy_set_status(
    client: &DbClient,
    policy_id: &str,
    status: PolicyStatus,
    apply: bool,
) -> Result<String> {
    let cmd_name = format!("policy {} {}", status.as_str(), policy_id);

    let plan = PlanOutput::new(&cmd_name, RiskLevel::Low)
        .field("Policy ID", policy_id)
        .field("New Status", status.as_str())
        .field(
            "Effect",
            match status {
                PolicyStatus::Enabled => "Policy will be evaluated by automation engine",
                PolicyStatus::Disabled => "Policy will not fire until re-enabled",
            },
        );

    if !apply {
        return Ok(plan.render(OutputMode::Table));
    }

    require_apply(apply, &cmd_name)?;

    let sql = format!(
        "SELECT falcon.admin_set_policy_status('{}', '{}')",
        policy_id.replace('\'', "''"),
        status.as_str()
    );
    match client.query_simple(&sql).await {
        Ok(_) => Ok(format!(
            "Policy '{}' is now {}.\n",
            policy_id,
            status.as_str()
        )),
        Err(e) => Ok(format!("Policy status update rejected: {}\n", e)),
    }
}

async fn policy_delete(client: &DbClient, policy_id: &str, apply: bool) -> Result<String> {
    let cmd_name = format!("policy delete {}", policy_id);

    let plan = PlanOutput::new(&cmd_name, RiskLevel::Medium)
        .field("Policy ID", policy_id)
        .field("Effect", "Policy will be permanently removed")
        .warn("This action cannot be undone. Audit records are preserved.");

    if !apply {
        return Ok(plan.render(OutputMode::Table));
    }

    require_apply(apply, &cmd_name)?;

    let sql = format!(
        "SELECT falcon.admin_delete_policy('{}')",
        policy_id.replace('\'', "''")
    );
    match client.query_simple(&sql).await {
        Ok(_) => Ok(format!(
            "Policy '{}' deleted. Audit records preserved.\n",
            policy_id
        )),
        Err(e) => Ok(format!("Policy deletion rejected: {}\n", e)),
    }
}

async fn policy_audit(client: &DbClient, policy_id: &str, mode: OutputMode) -> Result<String> {
    let sql = if policy_id.is_empty() {
        "SELECT event_time, policy_id, trigger_type, condition_snapshot, \
                action_attempted, guardrail_result, outcome \
         FROM falcon.audit_log \
         WHERE command_issued LIKE '%policy%' \
         ORDER BY event_time DESC \
         LIMIT 50"
            .to_string()
    } else {
        format!(
            "SELECT event_time, policy_id, trigger_type, condition_snapshot, \
                    action_attempted, guardrail_result, outcome \
             FROM falcon.policy_audit_log \
             WHERE policy_id = '{}' \
             ORDER BY event_time DESC \
             LIMIT 50",
            policy_id.replace('\'', "''")
        )
    };

    let (rows, _) = client
        .query_simple(&sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok(format!(
            "No audit records found for policy '{}'.\n",
            if policy_id.is_empty() {
                "all"
            } else {
                policy_id
            }
        ));
    }

    Ok(format_rows_as_string(
        &rows,
        mode,
        &format!("Policy Audit: {}", policy_id),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_cmd_parse_list() {
        assert!(matches!(PolicyCmd::parse("list").unwrap(), PolicyCmd::List));
        assert!(matches!(PolicyCmd::parse("").unwrap(), PolicyCmd::List));
    }

    #[test]
    fn test_policy_cmd_parse_show() {
        let cmd = PolicyCmd::parse("show pol1").unwrap();
        assert_eq!(cmd, PolicyCmd::Show("pol1".to_string()));
    }

    #[test]
    fn test_policy_cmd_parse_show_missing_id() {
        assert!(PolicyCmd::parse("show").is_err());
    }

    #[test]
    fn test_policy_cmd_parse_create() {
        let cmd = PolicyCmd::parse(
            "create id=pol1 condition=cluster_readonly action=require_human_approval",
        )
        .unwrap();
        assert!(matches!(cmd, PolicyCmd::Create(_)));
    }

    #[test]
    fn test_policy_cmd_parse_enable() {
        let cmd = PolicyCmd::parse("enable pol1").unwrap();
        assert_eq!(cmd, PolicyCmd::Enable("pol1".to_string()));
    }

    #[test]
    fn test_policy_cmd_parse_disable() {
        let cmd = PolicyCmd::parse("disable pol1").unwrap();
        assert_eq!(cmd, PolicyCmd::Disable("pol1".to_string()));
    }

    #[test]
    fn test_policy_cmd_parse_delete() {
        let cmd = PolicyCmd::parse("delete pol1").unwrap();
        assert_eq!(cmd, PolicyCmd::Delete("pol1".to_string()));
    }

    #[test]
    fn test_policy_cmd_parse_simulate() {
        let cmd = PolicyCmd::parse("simulate pol1").unwrap();
        assert_eq!(cmd, PolicyCmd::Simulate("pol1".to_string()));
    }

    #[test]
    fn test_policy_cmd_parse_audit() {
        let cmd = PolicyCmd::parse("audit pol1").unwrap();
        assert_eq!(cmd, PolicyCmd::Audit("pol1".to_string()));
    }

    #[test]
    fn test_policy_cmd_parse_unknown() {
        assert!(PolicyCmd::parse("foobar").is_err());
    }
}
