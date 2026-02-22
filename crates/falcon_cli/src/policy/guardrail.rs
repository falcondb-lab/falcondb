use crate::policy::model::{Guardrail, RiskCeiling};

/// Result of evaluating a guardrail.
#[derive(Debug, Clone, PartialEq)]
pub enum GuardrailVerdict {
    Pass,
    Blocked(String),
}

impl GuardrailVerdict {
    pub fn is_pass(&self) -> bool {
        matches!(self, Self::Pass)
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Blocked(r) => Some(r.as_str()),
            Self::Pass => None,
        }
    }
}

/// Snapshot of cluster state used for guardrail evaluation.
#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    pub health: String,
    pub recent_fire_count: u32,
    pub secs_since_last_fire: Option<u64>,
    pub action_risk_level: RiskCeiling,
}

impl ClusterSnapshot {
    pub fn unknown() -> Self {
        Self {
            health: "unknown".to_string(),
            recent_fire_count: 0,
            secs_since_last_fire: None,
            action_risk_level: RiskCeiling::Low,
        }
    }
}

/// Evaluate all guardrails for a policy against a cluster snapshot.
pub fn evaluate_guardrails(
    guardrail: &Guardrail,
    snapshot: &ClusterSnapshot,
) -> Vec<GuardrailVerdict> {
    let mut results = Vec::new();

    // Health prerequisite check
    if snapshot.health != guardrail.health_prerequisite
        && guardrail.health_prerequisite == "healthy"
        && snapshot.health != "healthy"
    {
        results.push(GuardrailVerdict::Blocked(format!(
            "Cluster health is '{}', required '{}' for this policy to fire",
            snapshot.health, guardrail.health_prerequisite
        )));
    } else {
        results.push(GuardrailVerdict::Pass);
    }

    // Frequency check
    if snapshot.recent_fire_count >= guardrail.max_frequency {
        results.push(GuardrailVerdict::Blocked(format!(
            "Policy has fired {} times in the last {} seconds (max: {})",
            snapshot.recent_fire_count, guardrail.time_window_secs, guardrail.max_frequency
        )));
    } else {
        results.push(GuardrailVerdict::Pass);
    }

    // Cooldown check
    if let Some(secs) = snapshot.secs_since_last_fire {
        if secs < guardrail.cooldown_secs {
            results.push(GuardrailVerdict::Blocked(format!(
                "Cooldown active: {}s remaining (cooldown: {}s)",
                guardrail.cooldown_secs - secs,
                guardrail.cooldown_secs
            )));
        } else {
            results.push(GuardrailVerdict::Pass);
        }
    } else {
        results.push(GuardrailVerdict::Pass);
    }

    // Risk ceiling check
    if snapshot.action_risk_level > guardrail.risk_ceiling {
        results.push(GuardrailVerdict::Blocked(format!(
            "Action risk '{}' exceeds policy ceiling '{}'",
            snapshot.action_risk_level.as_str(),
            guardrail.risk_ceiling.as_str()
        )));
    } else {
        results.push(GuardrailVerdict::Pass);
    }

    results
}

/// Returns true if all guardrails pass.
pub fn all_pass(verdicts: &[GuardrailVerdict]) -> bool {
    verdicts.iter().all(|v| v.is_pass())
}

/// Render guardrail evaluation results as a human-readable string.
pub fn render_guardrail_report(guardrail: &Guardrail, verdicts: &[GuardrailVerdict]) -> String {
    let labels = [
        "Health prerequisite",
        "Frequency limit",
        "Cooldown",
        "Risk ceiling",
    ];
    let mut out = String::from("  Guardrail Evaluation:\n");
    for (i, verdict) in verdicts.iter().enumerate() {
        let label = labels.get(i).copied().unwrap_or("Check");
        match verdict {
            GuardrailVerdict::Pass => {
                out.push_str(&format!("    ✓ {}: PASS\n", label));
            }
            GuardrailVerdict::Blocked(reason) => {
                out.push_str(&format!("    ✗ {}: BLOCKED — {}\n", label, reason));
            }
        }
    }
    out.push_str(&format!(
        "  Config: max_frequency={} time_window={}s cooldown={}s risk_ceiling={}\n",
        guardrail.max_frequency,
        guardrail.time_window_secs,
        guardrail.cooldown_secs,
        guardrail.risk_ceiling.as_str()
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_guardrail() -> Guardrail {
        Guardrail::default()
    }

    #[test]
    fn test_all_pass_healthy_cluster() {
        let g = default_guardrail();
        let snap = ClusterSnapshot {
            health: "healthy".to_string(),
            recent_fire_count: 0,
            secs_since_last_fire: None,
            action_risk_level: RiskCeiling::Low,
        };
        let verdicts = evaluate_guardrails(&g, &snap);
        assert!(all_pass(&verdicts));
    }

    #[test]
    fn test_blocked_by_frequency() {
        let g = default_guardrail(); // max_frequency = 3
        let snap = ClusterSnapshot {
            health: "healthy".to_string(),
            recent_fire_count: 3,
            secs_since_last_fire: None,
            action_risk_level: RiskCeiling::Low,
        };
        let verdicts = evaluate_guardrails(&g, &snap);
        assert!(!all_pass(&verdicts));
        let blocked: Vec<_> = verdicts.iter().filter(|v| !v.is_pass()).collect();
        assert!(!blocked.is_empty());
    }

    #[test]
    fn test_blocked_by_cooldown() {
        let g = default_guardrail(); // cooldown = 300s
        let snap = ClusterSnapshot {
            health: "healthy".to_string(),
            recent_fire_count: 0,
            secs_since_last_fire: Some(100), // only 100s since last fire
            action_risk_level: RiskCeiling::Low,
        };
        let verdicts = evaluate_guardrails(&g, &snap);
        assert!(!all_pass(&verdicts));
    }

    #[test]
    fn test_blocked_by_risk_ceiling() {
        let mut g = default_guardrail();
        g.risk_ceiling = RiskCeiling::Low;
        let snap = ClusterSnapshot {
            health: "healthy".to_string(),
            recent_fire_count: 0,
            secs_since_last_fire: None,
            action_risk_level: RiskCeiling::High,
        };
        let verdicts = evaluate_guardrails(&g, &snap);
        assert!(!all_pass(&verdicts));
    }

    #[test]
    fn test_cooldown_passes_after_window() {
        let g = default_guardrail(); // cooldown = 300s
        let snap = ClusterSnapshot {
            health: "healthy".to_string(),
            recent_fire_count: 0,
            secs_since_last_fire: Some(400), // 400s > 300s cooldown
            action_risk_level: RiskCeiling::Low,
        };
        let verdicts = evaluate_guardrails(&g, &snap);
        assert!(all_pass(&verdicts));
    }

    #[test]
    fn test_guardrail_verdict_reason() {
        let v = GuardrailVerdict::Blocked("too frequent".to_string());
        assert_eq!(v.reason(), Some("too frequent"));
        let p = GuardrailVerdict::Pass;
        assert_eq!(p.reason(), None);
    }

    #[test]
    fn test_render_guardrail_report_contains_labels() {
        let g = default_guardrail();
        let verdicts = vec![
            GuardrailVerdict::Pass,
            GuardrailVerdict::Blocked("too frequent".to_string()),
            GuardrailVerdict::Pass,
            GuardrailVerdict::Pass,
        ];
        let report = render_guardrail_report(&g, &verdicts);
        assert!(report.contains("Frequency limit"));
        assert!(report.contains("BLOCKED"));
        assert!(report.contains("PASS"));
    }
}
