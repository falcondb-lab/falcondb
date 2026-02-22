use std::io::Write;
use std::process::{Child, ChildStdin, Command, Stdio};
use tracing::debug;

/// A pager instance wrapping a child process.
pub struct Pager {
    child: Child,
    stdin: ChildStdin,
}

impl Pager {
    /// Spawn the pager process. Returns None if stdout is not a TTY or pager
    /// is disabled (e.g. CSV/JSON mode).
    pub fn try_spawn(disabled: bool) -> Option<Self> {
        if disabled || !stdout_is_tty() {
            debug!(
                "Pager disabled (disabled={}, tty={})",
                disabled,
                stdout_is_tty()
            );
            return None;
        }

        let pager_cmd = std::env::var("PAGER").unwrap_or_else(|_| "less".to_string());
        let args: Vec<&str> = if pager_cmd == "less" {
            vec!["-S", "-F", "-X"]
        } else {
            vec![]
        };

        debug!("Spawning pager: {} {:?}", pager_cmd, args);

        let mut cmd = Command::new(&pager_cmd);
        cmd.args(&args).stdin(Stdio::piped());

        match cmd.spawn() {
            Ok(mut child) => {
                let stdin = child.stdin.take()?;
                Some(Pager { child, stdin })
            }
            Err(e) => {
                debug!("Could not spawn pager '{}': {}", pager_cmd, e);
                None
            }
        }
    }

    /// Write output to the pager's stdin.
    pub fn write(&mut self, data: &str) {
        let _ = self.stdin.write_all(data.as_bytes());
    }

    /// Close the pager and wait for it to exit.
    pub fn finish(mut self) {
        drop(self.stdin);
        let _ = self.child.wait();
    }
}

/// Returns true if stdout is connected to a terminal.
pub fn stdout_is_tty() -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        unsafe { libc::isatty(io::stdout().as_raw_fd()) != 0 }
    }
    #[cfg(not(unix))]
    {
        // On Windows, assume not a TTY for safety (pager disabled)
        false
    }
}

/// Print output, routing through pager if appropriate.
/// `disable_pager` should be true for CSV/JSON modes.
pub fn print_with_pager(output: &str, disable_pager: bool) {
    if let Some(mut pager) = Pager::try_spawn(disable_pager) {
        pager.write(output);
        pager.finish();
    } else {
        print!("{}", output);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pager_disabled_when_flag_set() {
        // When disabled=true, try_spawn must return None
        let pager = Pager::try_spawn(true);
        assert!(pager.is_none(), "pager must be None when disabled=true");
    }

    #[test]
    fn test_stdout_is_tty_does_not_panic() {
        // Just verify the function runs without panicking
        let _ = stdout_is_tty();
    }
}
