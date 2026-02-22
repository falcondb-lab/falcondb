use serde_json::{json, Value};

/// Output mode for the CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliMode {
    Interactive,
    Machine,
}

impl CliMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "machine" => Some(Self::Machine),
            "interactive" | "human" => Some(Self::Interactive),
            _ => None,
        }
    }

    pub fn is_machine(&self) -> bool {
        matches!(self, Self::Machine)
    }
}

impl Default for CliMode {
    fn default() -> Self {
        Self::Interactive
    }
}

/// Wrap any string output in a machine-mode JSON envelope.
/// In machine mode all output is JSON with a stable schema.
pub fn wrap_machine_output(
    command: &str,
    output: &str,
    success: bool,
    operator: &str,
    source: &str,
) -> String {
    // Try to parse output as JSON first (already structured)
    let payload: Value = serde_json::from_str(output).unwrap_or_else(|_| {
        // Plain text — wrap as lines array
        let lines: Vec<Value> = output
            .lines()
            .map(|l| Value::String(l.to_string()))
            .collect();
        json!({ "text": lines })
    });

    let envelope = json!({
        "fsql": {
            "version": "0.7",
            "command": command,
            "success": success,
            "operator": operator,
            "source": source,
            "timestamp_unix": current_unix_secs(),
            "output": payload,
        }
    });

    let mut s = serde_json::to_string_pretty(&envelope).unwrap_or_default();
    s.push('\n');
    s
}

/// Wrap an error in a machine-mode JSON envelope.
pub fn wrap_machine_error(command: &str, error: &str, operator: &str, source: &str) -> String {
    let envelope = json!({
        "fsql": {
            "version": "0.7",
            "command": command,
            "success": false,
            "operator": operator,
            "source": source,
            "timestamp_unix": current_unix_secs(),
            "error": error,
        }
    });
    let mut s = serde_json::to_string_pretty(&envelope).unwrap_or_default();
    s.push('\n');
    s
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Identity context carried through a CLI session.
#[derive(Debug, Clone)]
pub struct Identity {
    pub operator: String,
    pub source: String,
}

impl Identity {
    pub fn new(operator: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            operator: operator.into(),
            source: source.into(),
        }
    }

    /// Resolve identity from CLI flags or environment variables.
    pub fn resolve(operator_flag: Option<&str>, source_flag: Option<&str>) -> Self {
        let operator = operator_flag
            .map(|s| s.to_string())
            .or_else(|| std::env::var("FSQL_OPERATOR").ok())
            .or_else(|| std::env::var("USER").ok())
            .or_else(|| std::env::var("USERNAME").ok())
            .unwrap_or_else(|| "unknown".to_string());

        let source = source_flag
            .map(|s| s.to_string())
            .or_else(|| std::env::var("FSQL_SOURCE").ok())
            .unwrap_or_else(|| "human".to_string());

        Self { operator, source }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_mode_parse_machine() {
        assert_eq!(CliMode::parse("machine"), Some(CliMode::Machine));
        assert_eq!(CliMode::parse("MACHINE"), Some(CliMode::Machine));
    }

    #[test]
    fn test_cli_mode_parse_interactive() {
        assert_eq!(CliMode::parse("interactive"), Some(CliMode::Interactive));
        assert_eq!(CliMode::parse("human"), Some(CliMode::Interactive));
    }

    #[test]
    fn test_cli_mode_parse_invalid() {
        assert_eq!(CliMode::parse("unknown"), None);
    }

    #[test]
    fn test_cli_mode_is_machine() {
        assert!(CliMode::Machine.is_machine());
        assert!(!CliMode::Interactive.is_machine());
    }

    #[test]
    fn test_wrap_machine_output_valid_json() {
        let out = wrap_machine_output(r"\cluster status", "hello", true, "alice", "human");
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["fsql"]["success"], true);
        assert_eq!(v["fsql"]["operator"], "alice");
        assert_eq!(v["fsql"]["version"], "0.7");
    }

    #[test]
    fn test_wrap_machine_output_json_passthrough() {
        let inner = r#"{"key": "value"}"#;
        let out = wrap_machine_output(r"\events list", inner, true, "bot", "automation");
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["fsql"]["output"]["key"], "value");
    }

    #[test]
    fn test_wrap_machine_error_valid_json() {
        let out = wrap_machine_error(r"\cluster status", "connection refused", "ci", "automation");
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["fsql"]["success"], false);
        assert!(v["fsql"]["error"].as_str().unwrap().contains("connection"));
    }

    #[test]
    fn test_identity_resolve_defaults() {
        let id = Identity::resolve(None, None);
        assert!(!id.operator.is_empty());
    }

    #[test]
    fn test_identity_resolve_explicit() {
        let id = Identity::resolve(Some("alice"), Some("ci"));
        assert_eq!(id.operator, "alice");
        assert_eq!(id.source, "ci");
    }
}
