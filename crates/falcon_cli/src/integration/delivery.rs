#![allow(dead_code)]

/// Status of a webhook delivery attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryStatus {
    Pending,
    Delivered,
    Failed(String),
    Retrying(u32),
}

impl DeliveryStatus {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "pending",
            Self::Delivered => "delivered",
            Self::Failed(_) => "failed",
            Self::Retrying(_) => "retrying",
        }
    }
}

impl std::fmt::Display for DeliveryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(reason) => write!(f, "failed: {}", reason),
            Self::Retrying(n) => write!(f, "retrying (attempt {})", n),
            other => f.write_str(other.as_str()),
        }
    }
}

/// A record of a single delivery attempt.
#[derive(Debug, Clone)]
pub struct DeliveryRecord {
    pub delivery_id: String,
    pub integration_id: String,
    pub event_type: String,
    pub status: DeliveryStatus,
    pub attempt_count: u32,
    pub last_attempt_at: u64,
    pub next_retry_at: Option<u64>,
}

impl DeliveryRecord {
    pub fn new(
        delivery_id: impl Into<String>,
        integration_id: impl Into<String>,
        event_type: impl Into<String>,
    ) -> Self {
        Self {
            delivery_id: delivery_id.into(),
            integration_id: integration_id.into(),
            event_type: event_type.into(),
            status: DeliveryStatus::Pending,
            attempt_count: 0,
            last_attempt_at: current_unix_secs(),
            next_retry_at: None,
        }
    }

    pub fn mark_delivered(&mut self) {
        self.status = DeliveryStatus::Delivered;
        self.attempt_count += 1;
        self.last_attempt_at = current_unix_secs();
        self.next_retry_at = None;
    }

    pub fn mark_failed(&mut self, reason: impl Into<String>) {
        self.attempt_count += 1;
        self.last_attempt_at = current_unix_secs();
        let backoff = backoff_secs(self.attempt_count);
        self.next_retry_at = Some(current_unix_secs() + backoff);
        self.status = DeliveryStatus::Failed(reason.into());
    }

    pub fn mark_retrying(&mut self) {
        self.status = DeliveryStatus::Retrying(self.attempt_count + 1);
    }

    pub fn should_retry(&self, max_attempts: u32) -> bool {
        self.attempt_count < max_attempts
            && matches!(
                self.status,
                DeliveryStatus::Failed(_) | DeliveryStatus::Retrying(_)
            )
    }
}

/// Exponential backoff: 30s, 60s, 120s, 240s, max 300s.
fn backoff_secs(attempt: u32) -> u64 {
    let base: u64 = 30;
    let max: u64 = 300;
    (base * (1u64 << attempt.min(4))).min(max)
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

    #[test]
    fn test_delivery_status_display() {
        assert_eq!(DeliveryStatus::Pending.as_str(), "pending");
        assert_eq!(DeliveryStatus::Delivered.as_str(), "delivered");
        assert_eq!(
            DeliveryStatus::Failed("timeout".to_string()).to_string(),
            "failed: timeout"
        );
        assert_eq!(
            DeliveryStatus::Retrying(2).to_string(),
            "retrying (attempt 2)"
        );
    }

    #[test]
    fn test_delivery_record_lifecycle() {
        let mut rec = DeliveryRecord::new("dlv-1", "wh-1", "policy.fired");
        assert_eq!(rec.status, DeliveryStatus::Pending);
        assert_eq!(rec.attempt_count, 0);

        rec.mark_failed("connection refused");
        assert_eq!(rec.attempt_count, 1);
        assert!(rec.next_retry_at.is_some());
        assert!(rec.should_retry(3));

        rec.mark_retrying();
        assert!(matches!(rec.status, DeliveryStatus::Retrying(_)));

        rec.mark_delivered();
        assert_eq!(rec.status, DeliveryStatus::Delivered);
        assert!(!rec.should_retry(3));
    }

    #[test]
    fn test_backoff_secs() {
        assert_eq!(backoff_secs(0), 30);
        assert_eq!(backoff_secs(1), 60);
        assert_eq!(backoff_secs(2), 120);
        assert_eq!(backoff_secs(3), 240);
        assert_eq!(backoff_secs(4), 300);
        assert_eq!(backoff_secs(10), 300); // capped at max
    }

    #[test]
    fn test_should_retry_respects_max() {
        let mut rec = DeliveryRecord::new("dlv-2", "wh-1", "test");
        rec.mark_failed("err");
        rec.mark_failed("err");
        rec.mark_failed("err");
        assert!(!rec.should_retry(3));
    }
}
