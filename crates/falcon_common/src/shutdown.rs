//! Interruptible shutdown signal for background tasks.
//!
//! Replaces bare `thread::sleep` loops with Condvar-based waits that respond
//! to stop signals within milliseconds instead of waiting for the full sleep
//! duration to elapse.
//!
//! # Usage
//! ```ignore
//! let signal = ShutdownSignal::new();
//! let signal_clone = signal.clone();
//!
//! // In the background thread:
//! while !signal_clone.is_shutdown() {
//!     // do work ...
//!     signal_clone.wait_timeout(Duration::from_secs(5));
//! }
//!
//! // From the control plane:
//! signal.shutdown(); // wakes the background thread immediately
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// A cooperative shutdown signal backed by `Condvar` for sub-millisecond
/// wakeup latency.
///
/// When `shutdown()` is called, any thread blocked in `wait_timeout()` is
/// woken immediately — no need to wait for the full interval to expire.
#[derive(Clone)]
pub struct ShutdownSignal {
    inner: Arc<ShutdownInner>,
}

struct ShutdownInner {
    flag: AtomicBool,
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl ShutdownSignal {
    /// Create a new signal in the non-shutdown state.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownInner {
                flag: AtomicBool::new(false),
                mutex: Mutex::new(()),
                condvar: Condvar::new(),
            }),
        }
    }

    /// Signal shutdown. Wakes all waiters immediately.
    pub fn shutdown(&self) {
        self.inner.flag.store(true, Ordering::SeqCst);
        self.inner.condvar.notify_all();
    }

    /// Check if shutdown has been requested (non-blocking).
    pub fn is_shutdown(&self) -> bool {
        self.inner.flag.load(Ordering::SeqCst)
    }

    /// Sleep for at most `duration`, but wake immediately if `shutdown()` is
    /// called. Returns `true` if shutdown was requested (caller should exit).
    pub fn wait_timeout(&self, duration: Duration) -> bool {
        if self.is_shutdown() {
            return true;
        }
        let guard = self.inner.mutex.lock().unwrap_or_else(|e| e.into_inner());
        let (_guard, _timeout) = self
            .inner
            .condvar
            .wait_timeout(guard, duration)
            .unwrap_or_else(|e| e.into_inner());
        self.is_shutdown()
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_signal_default_not_shutdown() {
        let sig = ShutdownSignal::new();
        assert!(!sig.is_shutdown());
    }

    #[test]
    fn test_shutdown_signal_set() {
        let sig = ShutdownSignal::new();
        sig.shutdown();
        assert!(sig.is_shutdown());
    }

    #[test]
    fn test_wait_timeout_returns_immediately_when_shutdown() {
        let sig = ShutdownSignal::new();
        sig.shutdown();
        let start = std::time::Instant::now();
        let result = sig.wait_timeout(Duration::from_secs(10));
        assert!(result);
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn test_wait_timeout_wakes_on_shutdown() {
        let sig = ShutdownSignal::new();
        let sig2 = sig.clone();
        let handle = std::thread::spawn(move || {
            let start = std::time::Instant::now();
            let result = sig2.wait_timeout(Duration::from_secs(10));
            (result, start.elapsed())
        });
        std::thread::sleep(Duration::from_millis(20));
        sig.shutdown();
        let (result, elapsed) = handle.join().unwrap();
        assert!(result);
        assert!(
            elapsed < Duration::from_secs(1),
            "should wake within 1s, took {:?}",
            elapsed
        );
    }

    #[test]
    fn test_wait_timeout_expires_normally() {
        let sig = ShutdownSignal::new();
        let start = std::time::Instant::now();
        let result = sig.wait_timeout(Duration::from_millis(20));
        assert!(!result);
        assert!(start.elapsed() >= Duration::from_millis(15));
    }

    #[test]
    fn test_clone_shares_state() {
        let sig1 = ShutdownSignal::new();
        let sig2 = sig1.clone();
        sig1.shutdown();
        assert!(sig2.is_shutdown());
    }
}
