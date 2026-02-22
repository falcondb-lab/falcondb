use crate::client::DbClient;
use crate::format::OutputMode;
use crate::runner::format_rows_as_string;
use anyhow::Result;

/// Sub-command for \audit.
#[derive(Debug, Clone, PartialEq)]
pub enum AuditCmd {
    Recent,
    Detail(String),
}

impl AuditCmd {
    pub fn parse(arg: &str) -> Self {
        match arg.trim().to_lowercase().as_str() {
            "recent" | "" => Self::Recent,
            other => Self::Detail(other.to_string()),
        }
    }
}

pub async fn run_audit(client: &DbClient, cmd: &AuditCmd, mode: OutputMode) -> Result<String> {
    match cmd {
        AuditCmd::Recent => audit_recent(client, mode).await,
        AuditCmd::Detail(id) => audit_detail(client, id, mode).await,
    }
}

async fn audit_recent(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT event_id, event_time, operator, command_issued, \
                      plan_summary, apply_result, outcome \
               FROM falcon.audit_log \
               ORDER BY event_time DESC \
               LIMIT 50";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok(
            "No audit records found. (falcon.audit_log view not present or no events recorded)\n"
                .to_string(),
        );
    }

    Ok(format_rows_as_string(&rows, mode, "Recent Audit Events"))
}

async fn audit_detail(client: &DbClient, event_id: &str, mode: OutputMode) -> Result<String> {
    let sql = format!(
        "SELECT event_id, event_time, operator, command_issued, \
                plan_summary, apply_result, outcome \
         FROM falcon.audit_log \
         WHERE event_id = '{}'",
        event_id.replace('\'', "''")
    );
    let (rows, _) = client
        .query_simple(&sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok(format!("Audit event '{}' not found.\n", event_id));
    }

    Ok(format_rows_as_string(
        &rows,
        mode,
        &format!("Audit Event {}", event_id),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_cmd_parse_recent() {
        assert_eq!(AuditCmd::parse("recent"), AuditCmd::Recent);
        assert_eq!(AuditCmd::parse(""), AuditCmd::Recent);
        assert_eq!(AuditCmd::parse("RECENT"), AuditCmd::Recent);
    }

    #[test]
    fn test_audit_cmd_parse_detail() {
        assert_eq!(
            AuditCmd::parse("evt-42"),
            AuditCmd::Detail("evt-42".to_string())
        );
        assert_eq!(
            AuditCmd::parse("12345"),
            AuditCmd::Detail("12345".to_string())
        );
    }
}
