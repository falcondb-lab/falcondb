use crate::client::DbClient;
use crate::format::OutputMode;
use crate::runner::format_rows_as_string;
use anyhow::Result;

/// Sub-command for \automation.
#[derive(Debug, Clone, PartialEq)]
pub enum AutomationCmd {
    Status,
    Events,
    Pause,
    Resume,
}

impl AutomationCmd {
    pub fn parse(arg: &str) -> Self {
        match arg.trim().to_lowercase().as_str() {
            "events" => Self::Events,
            "pause" => Self::Pause,
            "resume" => Self::Resume,
            _ => Self::Status,
        }
    }
}

pub async fn run_automation(
    client: &DbClient,
    cmd: &AutomationCmd,
    mode: OutputMode,
) -> Result<String> {
    match cmd {
        AutomationCmd::Status => automation_status(client, mode).await,
        AutomationCmd::Events => automation_events(client, mode).await,
        AutomationCmd::Pause => automation_pause(client, mode).await,
        AutomationCmd::Resume => automation_resume(client, mode).await,
    }
}

async fn automation_status(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT engine_state, policies_enabled, policies_disabled, \
                      last_evaluation_at, next_evaluation_in_secs \
               FROM falcon.automation_status \
               LIMIT 1";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback: derive from policy count
        let fallback = "SELECT 'unknown' AS engine_state, \
                               count(*) FILTER (WHERE status = 'enabled') AS policies_enabled, \
                               count(*) FILTER (WHERE status = 'disabled') AS policies_disabled, \
                               NULL AS last_evaluation_at, \
                               NULL AS next_evaluation_in_secs \
                        FROM falcon.policies";
        let (fb_rows, _) = client
            .query_simple(fallback)
            .await
            .unwrap_or_else(|_| (Vec::new(), String::new()));

        if fb_rows.is_empty() {
            return Ok("Automation engine status unavailable. \
                 (falcon.automation_status view not present)\n"
                .to_string());
        }
        return Ok(format_rows_as_string(&fb_rows, mode, "Automation Status"));
    }

    Ok(format_rows_as_string(&rows, mode, "Automation Status"))
}

async fn automation_events(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT event_time, policy_id, trigger_type, condition_snapshot, \
                      action_attempted, guardrail_result, outcome \
               FROM falcon.automation_events \
               ORDER BY event_time DESC \
               LIMIT 50";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok("No automation events recorded. \
             (falcon.automation_events view not present or no events yet)\n"
            .to_string());
    }

    Ok(format_rows_as_string(&rows, mode, "Automation Events"))
}

async fn automation_pause(client: &DbClient, _mode: OutputMode) -> Result<String> {
    let sql = "SELECT falcon.admin_pause_automation()";
    match client.query_simple(sql).await {
        Ok(_) => Ok(
            "Automation engine paused. No policies will fire until resumed.\n\
             Use \\automation resume to re-enable.\n"
                .to_string(),
        ),
        Err(_) => Ok("Automation engine pause requested. \
             (falcon.admin_pause_automation() not available — \
             update may be required on the server)\n"
            .to_string()),
    }
}

async fn automation_resume(client: &DbClient, _mode: OutputMode) -> Result<String> {
    let sql = "SELECT falcon.admin_resume_automation()";
    match client.query_simple(sql).await {
        Ok(_) => Ok("Automation engine resumed. Enabled policies will be evaluated.\n".to_string()),
        Err(_) => Ok("Automation engine resume requested. \
             (falcon.admin_resume_automation() not available — \
             update may be required on the server)\n"
            .to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_automation_cmd_parse_status() {
        assert_eq!(AutomationCmd::parse("status"), AutomationCmd::Status);
        assert_eq!(AutomationCmd::parse(""), AutomationCmd::Status);
        assert_eq!(AutomationCmd::parse("STATUS"), AutomationCmd::Status);
    }

    #[test]
    fn test_automation_cmd_parse_events() {
        assert_eq!(AutomationCmd::parse("events"), AutomationCmd::Events);
        assert_eq!(AutomationCmd::parse("EVENTS"), AutomationCmd::Events);
    }

    #[test]
    fn test_automation_cmd_parse_pause() {
        assert_eq!(AutomationCmd::parse("pause"), AutomationCmd::Pause);
    }

    #[test]
    fn test_automation_cmd_parse_resume() {
        assert_eq!(AutomationCmd::parse("resume"), AutomationCmd::Resume);
    }
}
