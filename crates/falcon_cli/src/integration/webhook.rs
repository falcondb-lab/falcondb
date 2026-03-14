#![allow(dead_code)]

use serde_json::{json, Value};

/// Configuration for a webhook endpoint.
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    pub id: String,
    pub url: String,
    pub enabled: bool,
}

/// An event payload to be delivered to a webhook.
#[derive(Debug, Clone)]
pub struct WebhookEvent {
    pub event_type: String,
    pub severity: String,
    pub source: String,
    pub operator: String,
    pub payload: Value,
}

impl WebhookEvent {
    pub fn new(
        event_type: impl Into<String>,
        severity: impl Into<String>,
        source: impl Into<String>,
        operator: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            severity: severity.into(),
            source: source.into(),
            operator: operator.into(),
            payload,
        }
    }
}

/// Build the JSON payload for a webhook delivery.
pub fn build_payload(config: &WebhookConfig, event: &WebhookEvent) -> Value {
    json!({
        "integration_id": config.id,
        "event_type": event.event_type,
        "severity": event.severity,
        "source": event.source,
        "operator": event.operator,
        "timestamp_unix": current_unix_secs(),
        "payload": event.payload,
    })
}

/// Deliver a webhook event (async, at-least-once semantics).
/// Returns Ok(delivery_id) on success, Err on failure.
/// In v0.7 this is a stub — actual HTTP delivery requires the server-side
/// `falcon.admin_deliver_webhook()` function or an external HTTP client.
pub async fn deliver(config: &WebhookConfig, event: &WebhookEvent) -> Result<String, String> {
    if !config.enabled {
        return Err(format!(
            "Integration '{}' is disabled — delivery skipped",
            config.id
        ));
    }

    let payload = build_payload(config, event);
    let delivery_id = format!("dlv-{}-{}", config.id, current_unix_secs());

    // Log the delivery attempt (actual HTTP call is server-side)
    tracing::debug!(
        "Webhook delivery: id={} url={} payload={}",
        delivery_id,
        config.url,
        serde_json::to_string(&payload).unwrap_or_default()
    );

    Ok(delivery_id)
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(enabled: bool) -> WebhookConfig {
        WebhookConfig {
            id: "wh-1".to_string(),
            url: "https://example.com/hook".to_string(),
            enabled,
        }
    }

    fn make_event() -> WebhookEvent {
        WebhookEvent::new(
            "policy.fired",
            "warning",
            "automation",
            "bot",
            json!({"policy_id": "pol1"}),
        )
    }

    #[test]
    fn test_build_payload_structure() {
        let cfg = make_config(true);
        let evt = make_event();
        let p = build_payload(&cfg, &evt);
        assert_eq!(p["integration_id"], "wh-1");
        assert_eq!(p["event_type"], "policy.fired");
        assert_eq!(p["severity"], "warning");
        assert_eq!(p["payload"]["policy_id"], "pol1");
    }

    #[tokio::test]
    async fn test_deliver_disabled_returns_err() {
        let cfg = make_config(false);
        let evt = make_event();
        let result = deliver(&cfg, &evt).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("disabled"));
    }

    #[tokio::test]
    async fn test_deliver_enabled_returns_ok() {
        let cfg = make_config(true);
        let evt = make_event();
        let result = deliver(&cfg, &evt).await;
        assert!(result.is_ok());
        assert!(result.unwrap().starts_with("dlv-"));
    }
}
