use crate::client::DbClient;
use crate::format::OutputMode;
use crate::policy::guardrail::{
    all_pass, evaluate_guardrails, render_guardrail_report, ClusterSnapshot,
};
use crate::policy::model::{Condition, Policy, RiskCeiling};
use anyhow::Result;

/// Result of simulating a policy.
#[derive(Debug)]
pub struct SimulationResult {
    pub policy_id: String,
    pub condition_met: bool,
    pub condition_description: String,
    pub action_description: String,
    pub guardrails_pass: bool,
    pub guardrail_report: String,
    pub would_fire: bool,
    pub risk_assessment: String,
}

impl SimulationResult {
    pub fn render(&self, mode: OutputMode) -> String {
        match mode {
            OutputMode::Json => self.render_json(),
            _ => self.render_table(),
        }
    }

    fn render_table(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("╔══ SIMULATION: policy '{}' ══\n", self.policy_id));
        out.push_str(&format!(
            "  Condition     : {} [{}]\n",
            self.condition_description,
            if self.condition_met { "MET" } else { "NOT MET" }
        ));
        out.push_str(&format!("  Action        : {}\n", self.action_description));
        out.push_str(&format!("  Risk          : {}\n", self.risk_assessment));
        out.push_str(&self.guardrail_report);
        out.push_str(&format!(
            "  Would Fire    : {}\n",
            if self.would_fire { "YES" } else { "NO" }
        ));
        out.push_str("╚══ DRY-RUN ONLY — no cluster state was modified ══\n");
        out
    }

    fn render_json(&self) -> String {
        let v = serde_json::json!({
            "simulation": {
                "policy_id": self.policy_id,
                "condition_met": self.condition_met,
                "condition": self.condition_description,
                "action": self.action_description,
                "guardrails_pass": self.guardrails_pass,
                "would_fire": self.would_fire,
                "risk": self.risk_assessment,
                "dry_run": true,
            }
        });
        let mut s = serde_json::to_string_pretty(&v).unwrap_or_default();
        s.push('\n');
        s
    }
}

/// Simulate a policy by ID — queries server for current state, evaluates
/// conditions and guardrails, returns a dry-run result. Never mutates state.
pub async fn simulate_policy(
    client: &DbClient,
    policy_id: &str,
    mode: OutputMode,
) -> Result<String> {
    // Fetch policy from server
    let sql = format!(
        "SELECT policy_id, description, scope, status, condition_expr, \
                action_expr, severity, guardrail_config \
         FROM falcon.policies \
         WHERE policy_id = '{}'",
        policy_id.replace('\'', "''")
    );
    let (rows, _) = client
        .query_simple(&sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Policy not found on server — produce a synthetic simulation with defaults
        return Ok(simulate_not_found(policy_id, mode));
    }

    // Build a synthetic policy from server data for simulation
    let row = &rows[0];
    let condition_expr = row.get(4).unwrap_or("unknown");
    let action_expr = row.get(5).unwrap_or("unknown");

    // Evaluate condition against live cluster state
    let (condition_met, condition_desc) = evaluate_condition_live(client, condition_expr).await;

    // Get cluster snapshot for guardrail evaluation
    let snapshot = get_cluster_snapshot(client, policy_id).await;

    // Use default guardrail for simulation
    let guardrail = crate::policy::model::Guardrail::default();
    let verdicts = evaluate_guardrails(&guardrail, &snapshot);
    let guardrails_pass = all_pass(&verdicts);
    let guardrail_report = render_guardrail_report(&guardrail, &verdicts);

    let would_fire = condition_met && guardrails_pass;
    let risk_assessment = assess_risk(action_expr, &snapshot);

    let result = SimulationResult {
        policy_id: policy_id.to_string(),
        condition_met,
        condition_description: condition_desc,
        action_description: action_expr.to_string(),
        guardrails_pass,
        guardrail_report,
        would_fire,
        risk_assessment,
    };

    Ok(result.render(mode))
}

/// Simulate a locally-defined policy (not yet persisted to server).
#[allow(dead_code)]
pub fn simulate_local_policy(policy: &Policy, mode: OutputMode) -> String {
    let snapshot = ClusterSnapshot::unknown();
    let guardrail = &policy.guardrail;
    let verdicts = evaluate_guardrails(guardrail, &snapshot);
    let guardrails_pass = all_pass(&verdicts);
    let guardrail_report = render_guardrail_report(guardrail, &verdicts);

    // Condition is not evaluable without live data — mark as unknown
    let result = SimulationResult {
        policy_id: policy.id.clone(),
        condition_met: false,
        condition_description: format!(
            "{} (live evaluation requires server connection)",
            policy.condition.description()
        ),
        action_description: policy.action.description(),
        guardrails_pass,
        guardrail_report,
        would_fire: false,
        risk_assessment: "Unknown (no live cluster data)".to_string(),
    };

    result.render(mode)
}

async fn evaluate_condition_live(client: &DbClient, condition_expr: &str) -> (bool, String) {
    let lower = condition_expr.to_lowercase();

    if lower.contains("cluster_readonly") {
        let sql = "SELECT setting FROM falcon.cluster_config WHERE name = 'cluster_mode'";
        let mode = client
            .query_simple(sql)
            .await
            .ok()
            .and_then(|(rows, _)| rows.into_iter().next())
            .and_then(|r| r.get(0).map(|v| v.to_string()))
            .unwrap_or_else(|| "readwrite".to_string());
        let met = mode == "readonly";
        return (
            met,
            format!("cluster_mode = '{}' (readonly: {})", mode, met),
        );
    }

    if lower.contains("in_doubt") {
        let sql = "SELECT count(*) FROM pg_prepared_xacts";
        let count = client
            .query_simple(sql)
            .await
            .ok()
            .and_then(|(rows, _)| rows.into_iter().next())
            .and_then(|r| r.get(0).map(|v| v.to_string()))
            .unwrap_or_else(|| "0".to_string());
        let n: u32 = count.parse().unwrap_or(0);
        return (n > 0, format!("in-doubt transactions: {}", n));
    }

    // Generic: return not-evaluable
    (
        false,
        format!(
            "'{}' (live evaluation not available for this condition type)",
            condition_expr
        ),
    )
}

async fn get_cluster_snapshot(client: &DbClient, _policy_id: &str) -> ClusterSnapshot {
    let health_sql = "SELECT CASE WHEN pg_is_in_recovery() THEN 'standby' ELSE 'healthy' END";
    let health = client
        .query_simple(health_sql)
        .await
        .ok()
        .and_then(|(rows, _)| rows.into_iter().next())
        .and_then(|r| r.get(0).map(|v| v.to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    ClusterSnapshot {
        health,
        recent_fire_count: 0,
        secs_since_last_fire: None,
        action_risk_level: RiskCeiling::Low,
    }
}

fn assess_risk(action_expr: &str, _snapshot: &ClusterSnapshot) -> String {
    let lower = action_expr.to_lowercase();
    if lower.contains("drain") || lower.contains("move_shard") {
        "MEDIUM — leader transition may cause brief latency spike".to_string()
    } else if lower.contains("readonly") {
        "HIGH — write transactions will be rejected".to_string()
    } else if lower.contains("alert") {
        "LOW — alert emission only, no cluster state change".to_string()
    } else if lower.contains("human_approval") {
        "LOW — automation blocked, human intervention required".to_string()
    } else {
        "UNKNOWN".to_string()
    }
}

fn simulate_not_found(policy_id: &str, mode: OutputMode) -> String {
    match mode {
        OutputMode::Json => {
            let v = serde_json::json!({
                "simulation": {
                    "policy_id": policy_id,
                    "error": "Policy not found on server",
                    "dry_run": true,
                }
            });
            let mut s = serde_json::to_string_pretty(&v).unwrap_or_default();
            s.push('\n');
            s
        }
        _ => format!(
            "╔══ SIMULATION: policy '{}' ══\n\
             Policy '{}' not found in falcon.policies.\n\
             Use \\policy create to define it first.\n\
             ╚══ DRY-RUN ONLY — no cluster state was modified ══\n",
            policy_id, policy_id
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::model::{Action, Condition, Guardrail, Policy, PolicyScope, PolicyStatus};

    fn make_policy(id: &str) -> Policy {
        Policy {
            id: id.to_string(),
            description: "test policy".to_string(),
            scope: PolicyScope::Cluster,
            status: PolicyStatus::Disabled,
            condition: Condition::ClusterInReadonlyMode,
            action: Action::RequireHumanApproval,
            guardrail: Guardrail::default(),
            severity: "warning".to_string(),
            created_by: "test".to_string(),
            created_at: "2026-01-01".to_string(),
            last_evaluated_at: None,
        }
    }

    #[test]
    fn test_simulate_local_policy_renders_table() {
        let p = make_policy("pol-test");
        let out = simulate_local_policy(&p, OutputMode::Table);
        assert!(out.contains("pol-test"));
        assert!(out.contains("DRY-RUN"));
    }

    #[test]
    fn test_simulate_local_policy_renders_json() {
        let p = make_policy("pol-json");
        let out = simulate_local_policy(&p, OutputMode::Json);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["simulation"]["policy_id"], "pol-json");
        assert_eq!(v["simulation"]["dry_run"], true);
    }

    #[test]
    fn test_simulate_not_found_table() {
        let out = simulate_not_found("missing-pol", OutputMode::Table);
        assert!(out.contains("not found"));
        assert!(out.contains("DRY-RUN"));
    }

    #[test]
    fn test_simulate_not_found_json() {
        let out = simulate_not_found("missing-pol", OutputMode::Json);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert!(v["simulation"]["error"]
            .as_str()
            .unwrap()
            .contains("not found"));
    }
}
