#![allow(dead_code)]

use crate::client::DbClient;
use crate::format::OutputMode;
use crate::runner::format_rows_as_string;
use anyhow::Result;

/// Type of integration endpoint.
#[derive(Debug, Clone, PartialEq)]
pub enum IntegrationType {
    Webhook,
}

impl IntegrationType {
    pub fn as_str(&self) -> &'static str {
        "webhook"
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "webhook" => Some(Self::Webhook),
            _ => None,
        }
    }
}

/// A registered integration.
#[derive(Debug, Clone)]
pub struct Integration {
    pub id: String,
    pub kind: IntegrationType,
    pub endpoint: String,
    pub enabled: bool,
    pub created_at: String,
}

/// Sub-command for \integration.
#[derive(Debug, Clone, PartialEq)]
pub enum IntegrationCmd {
    List,
    AddWebhook(String),
    Remove(String),
    Disable(String),
    Enable(String),
}

impl IntegrationCmd {
    pub fn parse(arg: &str) -> Self {
        let s = arg.trim();
        let lower = s.to_lowercase();
        let mut parts = s.splitn(3, char::is_whitespace);
        let verb = parts.next().unwrap_or("").to_lowercase();
        let rest: Vec<&str> = parts.collect();

        match verb.as_str() {
            "add" => {
                let kind = rest.get(0).copied().unwrap_or("").to_lowercase();
                let endpoint = rest.get(1).copied().unwrap_or("").to_string();
                if kind == "webhook" {
                    Self::AddWebhook(endpoint)
                } else {
                    Self::List
                }
            }
            "remove" | "delete" => {
                let id = rest.get(0).copied().unwrap_or("").to_string();
                Self::Remove(id)
            }
            "disable" => {
                let id = rest.get(0).copied().unwrap_or("").to_string();
                Self::Disable(id)
            }
            "enable" => {
                let id = rest.get(0).copied().unwrap_or("").to_string();
                Self::Enable(id)
            }
            _ => {
                let _ = lower;
                Self::List
            }
        }
    }
}

pub async fn run_integration(
    client: &DbClient,
    cmd: &IntegrationCmd,
    mode: OutputMode,
) -> Result<String> {
    match cmd {
        IntegrationCmd::List => integration_list(client, mode).await,
        IntegrationCmd::AddWebhook(url) => integration_add_webhook(client, url, mode).await,
        IntegrationCmd::Remove(id) => integration_remove(client, id, mode).await,
        IntegrationCmd::Disable(id) => integration_set_enabled(client, id, false, mode).await,
        IntegrationCmd::Enable(id) => integration_set_enabled(client, id, true, mode).await,
    }
}

async fn integration_list(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT integration_id, kind, endpoint, enabled, created_at \
               FROM falcon.integrations \
               ORDER BY created_at DESC";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok("No integrations registered. \
             Use \\integration add webhook <url> to register one.\n"
            .to_string());
    }

    Ok(format_rows_as_string(&rows, mode, "Integrations"))
}

async fn integration_add_webhook(
    client: &DbClient,
    url: &str,
    _mode: OutputMode,
) -> Result<String> {
    if url.is_empty() {
        return Ok("Usage: \\integration add webhook <url>\n".to_string());
    }

    let sql = format!(
        "SELECT falcon.admin_register_integration('webhook', '{}')",
        url.replace('\'', "''")
    );
    match client.query_simple(&sql).await {
        Ok(rows) => {
            let id = rows
                .0
                .into_iter()
                .next()
                .and_then(|r| r.get(0).map(|v| v.to_string()))
                .unwrap_or_else(|| "unknown".to_string());
            Ok(format!(
                "Webhook integration registered (id: {}).\n\
                 Events will be delivered to: {}\n",
                id, url
            ))
        }
        Err(e) => Ok(format!(
            "Integration registration rejected: {}\n\
             (falcon.admin_register_integration() may not be available)\n",
            e
        )),
    }
}

async fn integration_remove(client: &DbClient, id: &str, _mode: OutputMode) -> Result<String> {
    if id.is_empty() {
        return Ok("Usage: \\integration remove <integration_id>\n".to_string());
    }

    let sql = format!(
        "SELECT falcon.admin_remove_integration('{}')",
        id.replace('\'', "''")
    );
    match client.query_simple(&sql).await {
        Ok(_) => Ok(format!("Integration '{}' removed.\n", id)),
        Err(e) => Ok(format!("Integration removal rejected: {}\n", e)),
    }
}

async fn integration_set_enabled(
    client: &DbClient,
    id: &str,
    enabled: bool,
    _mode: OutputMode,
) -> Result<String> {
    if id.is_empty() {
        return Ok(format!(
            "Usage: \\integration {} <integration_id>\n",
            if enabled { "enable" } else { "disable" }
        ));
    }

    let sql = format!(
        "SELECT falcon.admin_set_integration_enabled('{}', {})",
        id.replace('\'', "''"),
        enabled
    );
    match client.query_simple(&sql).await {
        Ok(_) => Ok(format!(
            "Integration '{}' {}.\n",
            id,
            if enabled { "enabled" } else { "disabled" }
        )),
        Err(e) => Ok(format!("Integration update rejected: {}\n", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integration_cmd_parse_list() {
        assert_eq!(IntegrationCmd::parse("list"), IntegrationCmd::List);
        assert_eq!(IntegrationCmd::parse(""), IntegrationCmd::List);
    }

    #[test]
    fn test_integration_cmd_parse_add_webhook() {
        assert_eq!(
            IntegrationCmd::parse("add webhook https://example.com/hook"),
            IntegrationCmd::AddWebhook("https://example.com/hook".to_string())
        );
    }

    #[test]
    fn test_integration_cmd_parse_remove() {
        assert_eq!(
            IntegrationCmd::parse("remove int-1"),
            IntegrationCmd::Remove("int-1".to_string())
        );
        assert_eq!(
            IntegrationCmd::parse("delete int-1"),
            IntegrationCmd::Remove("int-1".to_string())
        );
    }

    #[test]
    fn test_integration_cmd_parse_disable() {
        assert_eq!(
            IntegrationCmd::parse("disable int-1"),
            IntegrationCmd::Disable("int-1".to_string())
        );
    }

    #[test]
    fn test_integration_cmd_parse_enable() {
        assert_eq!(
            IntegrationCmd::parse("enable int-1"),
            IntegrationCmd::Enable("int-1".to_string())
        );
    }

    #[test]
    fn test_integration_type_parse() {
        assert_eq!(
            IntegrationType::parse("webhook"),
            Some(IntegrationType::Webhook)
        );
        assert_eq!(IntegrationType::parse("unknown"), None);
    }
}
