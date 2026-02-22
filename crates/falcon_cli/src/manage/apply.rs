use anyhow::{bail, Result};

/// Result of applying a management command.
#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub command: String,
    pub outcome: ApplyOutcome,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ApplyOutcome {
    Success,
    Aborted,
    Rejected,
}

impl ApplyOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Aborted => "aborted",
            Self::Rejected => "rejected",
        }
    }
}

impl ApplyResult {
    pub fn success(command: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            outcome: ApplyOutcome::Success,
            message: message.into(),
        }
    }

    #[allow(dead_code)]
    pub fn aborted(command: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            outcome: ApplyOutcome::Aborted,
            message: reason.into(),
        }
    }

    pub fn rejected(command: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            outcome: ApplyOutcome::Rejected,
            message: reason.into(),
        }
    }

    pub fn render(&self) -> String {
        format!(
            "[{}] {} — {}\n",
            self.outcome.as_str().to_uppercase(),
            self.command,
            self.message
        )
    }
}

/// Guard: ensure --apply was passed before executing a mutation.
/// Returns Err with a clear message if apply is false.
pub fn require_apply(apply: bool, command: &str) -> Result<()> {
    if !apply {
        bail!(
            "Management command '{}' is in PLAN mode.\n\
             Re-run with --apply to execute (add --yes to skip confirmation).",
            command
        );
    }
    Ok(())
}

/// Guard: prompt for confirmation unless --yes was passed.
/// In non-interactive (piped) mode, always requires --yes.
#[allow(dead_code)]
pub fn confirm(yes: bool, prompt: &str) -> Result<bool> {
    if yes {
        return Ok(true);
    }

    // Check if stdin is a TTY
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let is_tty = unsafe { libc::isatty(std::io::stdin().as_raw_fd()) } != 0;
        if !is_tty {
            bail!(
                "Non-interactive mode: use --yes to confirm management commands without a prompt."
            );
        }
    }
    #[cfg(not(unix))]
    {
        // On non-Unix (Windows), require --yes in non-interactive contexts
        // For simplicity, always require --yes in script mode
        bail!("Use --yes to confirm management commands in non-interactive mode.");
    }

    eprint!("{} [y/N] ", prompt);
    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .map_err(|e| anyhow::anyhow!("Failed to read confirmation: {}", e))?;
    Ok(input.trim().eq_ignore_ascii_case("y"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_require_apply_fails_when_false() {
        let result = require_apply(false, "node drain node1");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("PLAN mode"));
        assert!(msg.contains("--apply"));
    }

    #[test]
    fn test_require_apply_passes_when_true() {
        assert!(require_apply(true, "node drain node1").is_ok());
    }

    #[test]
    fn test_apply_result_render_success() {
        let r = ApplyResult::success("node drain node1", "Node drained successfully");
        let out = r.render();
        assert!(out.contains("SUCCESS"));
        assert!(out.contains("node drain node1"));
    }

    #[test]
    fn test_apply_result_render_aborted() {
        let r = ApplyResult::aborted("shard move 1 to node2", "User cancelled");
        let out = r.render();
        assert!(out.contains("ABORTED"));
    }

    #[test]
    fn test_apply_result_render_rejected() {
        let r = ApplyResult::rejected("cluster mode readonly", "Insufficient privileges");
        let out = r.render();
        assert!(out.contains("REJECTED"));
    }

    #[test]
    fn test_apply_outcome_as_str() {
        assert_eq!(ApplyOutcome::Success.as_str(), "success");
        assert_eq!(ApplyOutcome::Aborted.as_str(), "aborted");
        assert_eq!(ApplyOutcome::Rejected.as_str(), "rejected");
    }
}
