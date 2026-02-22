use crate::format::OutputMode;
use serde_json::{json, Value};

/// Risk level for a management plan.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

impl RiskLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Low => "LOW",
            Self::Medium => "MEDIUM",
            Self::High => "HIGH",
        }
    }
}

impl std::fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single field in a plan output.
#[derive(Debug, Clone)]
pub struct PlanField {
    pub label: String,
    pub value: String,
}

impl PlanField {
    pub fn new(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
        }
    }
}

/// The full plan output for a management command.
#[derive(Debug, Clone)]
pub struct PlanOutput {
    pub command: String,
    pub risk: RiskLevel,
    pub fields: Vec<PlanField>,
    pub warnings: Vec<String>,
    pub requires_apply: bool,
}

impl PlanOutput {
    pub fn new(command: impl Into<String>, risk: RiskLevel) -> Self {
        Self {
            command: command.into(),
            risk,
            fields: Vec::new(),
            warnings: Vec::new(),
            requires_apply: true,
        }
    }

    pub fn field(mut self, label: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push(PlanField::new(label, value));
        self
    }

    pub fn warn(mut self, msg: impl Into<String>) -> Self {
        self.warnings.push(msg.into());
        self
    }

    pub fn no_apply(mut self) -> Self {
        self.requires_apply = false;
        self
    }

    /// Render the plan as a human-readable string (table mode) or JSON.
    pub fn render(&self, mode: OutputMode) -> String {
        match mode {
            OutputMode::Json => self.render_json(),
            _ => self.render_table(),
        }
    }

    fn render_table(&self) -> String {
        let mut out = String::new();

        out.push_str(&format!("╔══ PLAN: {} ══\n", self.command));
        out.push_str(&format!("  Risk Level : {}\n", self.risk));

        if !self.fields.is_empty() {
            let label_width = self
                .fields
                .iter()
                .map(|f| f.label.len())
                .max()
                .unwrap_or(0)
                .max(12);
            for f in &self.fields {
                out.push_str(&format!(
                    "  {:<width$} : {}\n",
                    f.label,
                    f.value,
                    width = label_width
                ));
            }
        }

        for w in &self.warnings {
            out.push_str(&format!("  ⚠  WARNING : {}\n", w));
        }

        if self.requires_apply {
            out.push_str(
                "╚══ To execute, re-run with --apply (add --yes to skip confirmation) ══\n",
            );
        } else {
            out.push_str("╚══ DRY-RUN ONLY — no cluster state was modified ══\n");
        }

        out
    }

    fn render_json(&self) -> String {
        let fields: Value = self
            .fields
            .iter()
            .map(|f| (f.label.clone(), Value::String(f.value.clone())))
            .collect::<serde_json::Map<_, _>>()
            .into();

        let v = json!({
            "plan": {
                "command": self.command,
                "risk": self.risk.as_str(),
                "fields": fields,
                "warnings": self.warnings,
                "requires_apply": self.requires_apply,
            }
        });
        let mut s = serde_json::to_string_pretty(&v).unwrap_or_default();
        s.push('\n');
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_level_ordering() {
        assert!(RiskLevel::Low < RiskLevel::Medium);
        assert!(RiskLevel::Medium < RiskLevel::High);
    }

    #[test]
    fn test_risk_level_display() {
        assert_eq!(RiskLevel::Low.as_str(), "LOW");
        assert_eq!(RiskLevel::Medium.as_str(), "MEDIUM");
        assert_eq!(RiskLevel::High.as_str(), "HIGH");
    }

    #[test]
    fn test_plan_output_render_table_contains_command() {
        let plan = PlanOutput::new("node drain node1", RiskLevel::Medium)
            .field("Node ID", "node1")
            .field("Active Txns", "3");
        let out = plan.render(OutputMode::Table);
        assert!(out.contains("node drain node1"));
        assert!(out.contains("MEDIUM"));
        assert!(out.contains("node1"));
        assert!(out.contains("--apply"));
    }

    #[test]
    fn test_plan_output_render_json() {
        let plan = PlanOutput::new("cluster mode readonly", RiskLevel::High)
            .field("Current Mode", "readwrite")
            .warn("Active writers will be blocked");
        let out = plan.render(OutputMode::Json);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["plan"]["risk"], "HIGH");
        assert_eq!(v["plan"]["command"], "cluster mode readonly");
    }

    #[test]
    fn test_plan_no_apply_shows_dry_run() {
        let plan = PlanOutput::new("failover simulate node1", RiskLevel::Low).no_apply();
        let out = plan.render(OutputMode::Table);
        assert!(out.contains("DRY-RUN"));
        assert!(!out.contains("--apply"));
    }

    #[test]
    fn test_plan_warnings_appear() {
        let plan = PlanOutput::new("node drain node1", RiskLevel::Medium)
            .warn("Node has 5 active transactions");
        let out = plan.render(OutputMode::Table);
        assert!(out.contains("WARNING"));
        assert!(out.contains("5 active transactions"));
    }
}
