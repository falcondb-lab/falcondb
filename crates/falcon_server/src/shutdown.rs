//! Graceful Shutdown Protocol for FalconDB.
//!
//! ## Design
//!
//! FalconDB uses a **single root `CancellationToken`** as the global shutdown
//! primitive. Every server task (PG, HTTP health, gRPC replication) receives
//! a child token and must:
//!
//! 1. Stop accepting new work when the token is cancelled
//! 2. Drain in-flight work (with timeout)
//! 3. **Explicitly drop** the listener/socket before returning
//! 4. Log shutdown with server name, port, and reason
//!
//! ## Shutdown Sequence
//!
//! ```text
//! OS Signal (Ctrl+C / SIGTERM)
//!    ↓
//! root_token.cancel()          ← main()
//!    ↓
//! All listeners stop accept   ← each server task observes token
//!    ↓
//! Servers drain + drop listeners
//!    ↓
//! main() awaits ALL JoinHandles
//!    ↓
//! Process exits
//! ```
//!
//! ## Invariant
//!
//! **Port release is NOT a side-effect of process exit.**
//! Every listener is explicitly dropped inside its task before the task
//! returns. This guarantees deterministic port release on both Windows
//! and Linux, regardless of Tokio runtime drop ordering.

use tokio_util::sync::CancellationToken;

/// Global shutdown coordinator.
///
/// Created once in `main()`, cloned into every server task.
/// Cancelling the root token triggers orderly shutdown of all subsystems.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    token: CancellationToken,
    reason: std::sync::Arc<std::sync::Mutex<ShutdownReason>>,
}

/// Why the process is shutting down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownReason {
    /// Not yet shutting down.
    Running,
    /// Ctrl+C / SIGINT received.
    CtrlC,
    /// SIGTERM received (Unix only).
    Sigterm,
    /// Windows Service Control Manager sent STOP or SHUTDOWN.
    ServiceStop,
    /// Programmatic shutdown request.
    Requested,
}

impl std::fmt::Display for ShutdownReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::CtrlC => write!(f, "ctrl_c"),
            Self::Sigterm => write!(f, "sigterm"),
            Self::ServiceStop => write!(f, "service_stop"),
            Self::Requested => write!(f, "requested"),
        }
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownCoordinator {
    /// Create a new coordinator (call once in main).
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            reason: std::sync::Arc::new(std::sync::Mutex::new(ShutdownReason::Running)),
        }
    }

    /// Get the underlying `CancellationToken` for use in `tokio::select!`.
    pub const fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Create a child token for a subsystem.
    /// When the root is cancelled, all children are also cancelled.
    pub fn child_token(&self) -> CancellationToken {
        self.token.child_token()
    }

    /// Trigger global shutdown with a reason.
    pub fn shutdown(&self, reason: ShutdownReason) {
        if let Ok(mut r) = self.reason.lock() {
            if *r == ShutdownReason::Running {
                *r = reason;
            }
        }
        self.token.cancel();
    }

    /// Check if shutdown has been initiated.
    pub fn is_shutting_down(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Get the shutdown reason.
    pub fn reason(&self) -> ShutdownReason {
        self.reason
            .lock()
            .map(|g| *g)
            .unwrap_or(ShutdownReason::Running)
    }

    /// Wait until shutdown is signalled. Use in `tokio::select!`.
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}

/// Wait for OS shutdown signal and return the reason.
///
/// Listens for:
/// - Ctrl+C (SIGINT) on all platforms
/// - SIGTERM on Unix
pub async fn wait_for_os_signal() -> ShutdownReason {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => ShutdownReason::CtrlC,
                    _ = sigterm.recv() => ShutdownReason::Sigterm,
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                ShutdownReason::CtrlC
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        ShutdownReason::CtrlC
    }
}

/// Log a server shutdown event with structured fields.
pub fn log_server_shutdown(server_name: &str, port: &str, reason: ShutdownReason) {
    tracing::info!(
        server = server_name,
        port = port,
        reason = %reason,
        "{} shutdown (port={}, reason={})",
        server_name,
        port,
        reason,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_coordinator_new_is_running() {
        let coord = ShutdownCoordinator::new();
        assert!(!coord.is_shutting_down());
        assert_eq!(coord.reason(), ShutdownReason::Running);
    }

    #[test]
    fn test_shutdown_coordinator_cancel() {
        let coord = ShutdownCoordinator::new();
        coord.shutdown(ShutdownReason::CtrlC);
        assert!(coord.is_shutting_down());
        assert_eq!(coord.reason(), ShutdownReason::CtrlC);
    }

    #[test]
    fn test_shutdown_coordinator_reason_sticky() {
        let coord = ShutdownCoordinator::new();
        coord.shutdown(ShutdownReason::CtrlC);
        // Second shutdown call doesn't overwrite reason
        coord.shutdown(ShutdownReason::Sigterm);
        assert_eq!(coord.reason(), ShutdownReason::CtrlC);
    }

    #[test]
    fn test_child_token_cancelled_on_parent() {
        let coord = ShutdownCoordinator::new();
        let child = coord.child_token();
        assert!(!child.is_cancelled());
        coord.shutdown(ShutdownReason::Requested);
        assert!(child.is_cancelled());
    }

    #[test]
    fn test_clone_shares_state() {
        let coord = ShutdownCoordinator::new();
        let clone = coord.clone();
        clone.shutdown(ShutdownReason::Sigterm);
        assert!(coord.is_shutting_down());
        assert_eq!(coord.reason(), ShutdownReason::Sigterm);
    }

    #[tokio::test]
    async fn test_cancelled_future_resolves() {
        let coord = ShutdownCoordinator::new();
        let coord2 = coord.clone();

        let handle = tokio::spawn(async move {
            coord2.cancelled().await;
            true
        });

        // Brief delay then cancel
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        coord.shutdown(ShutdownReason::CtrlC);

        let result = handle.await.unwrap();
        assert!(result);
    }

    #[test]
    fn test_shutdown_reason_display() {
        assert_eq!(format!("{}", ShutdownReason::CtrlC), "ctrl_c");
        assert_eq!(format!("{}", ShutdownReason::Sigterm), "sigterm");
        assert_eq!(format!("{}", ShutdownReason::Requested), "requested");
        assert_eq!(format!("{}", ShutdownReason::Running), "running");
    }

    #[test]
    fn test_log_server_shutdown_does_not_panic() {
        // Just verify it doesn't panic — tracing output not captured
        log_server_shutdown("test_server", "9999", ShutdownReason::CtrlC);
    }
}
