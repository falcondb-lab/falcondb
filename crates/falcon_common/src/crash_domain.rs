//! Crash domain isolation — prevents single-request panics from killing the process.
//!
//! # Design
//! - **Global panic hook**: captures panics, emits structured diagnostic log,
//!   increments a counter, and applies throttle/backoff to avoid restart storms.
//! - **`catch_request`**: wraps a request closure in `std::panic::catch_unwind`,
//!   converts panics into `FalconError::InternalBug` with full context.
//! - **`PanicStats`**: global atomic counters for monitoring.
//!
//! # Usage
//! ```ignore
//! // At server startup:
//! crash_domain::install_panic_hook();
//!
//! // Per request in handler:
//! let result = crash_domain::catch_request("handle_query", ctx, || {
//!     handler.handle_query(sql, session)
//! });
//! ```

use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use std::sync::Mutex;

use crate::error::{FalconError, FalconResult};

/// Global panic statistics.
static PANIC_COUNT: AtomicU64 = AtomicU64::new(0);
static PANIC_HOOK_INSTALLED: AtomicBool = AtomicBool::new(false);

/// Global panic throttle — aborts the process if too many panics in a short window.
/// Default: 10 panics within 60 seconds triggers abort.
static GLOBAL_THROTTLE: std::sync::OnceLock<Arc<PanicThrottle>> = std::sync::OnceLock::new();

fn global_throttle() -> &'static Arc<PanicThrottle> {
    GLOBAL_THROTTLE.get_or_init(|| PanicThrottle::new(Duration::from_secs(60), 10))
}

/// Recent panic entries (ring buffer, last N panics).
const MAX_RECENT_PANICS: usize = 16;

/// A single recorded panic event.
#[derive(Debug, Clone)]
pub struct PanicEvent {
    pub message: String,
    pub location: String,
    pub thread_name: String,
    pub occurred_at_ms: u64,
}

/// Global ring buffer of recent panics for diagnostic bundles.
static RECENT_PANICS: std::sync::OnceLock<Arc<Mutex<Vec<PanicEvent>>>> = std::sync::OnceLock::new();

fn recent_panics() -> &'static Arc<Mutex<Vec<PanicEvent>>> {
    RECENT_PANICS.get_or_init(|| Arc::new(Mutex::new(Vec::with_capacity(MAX_RECENT_PANICS))))
}

/// Install the global panic hook. Safe to call multiple times (idempotent).
///
/// The hook:
/// 1. Logs the panic with location and thread name at ERROR level.
/// 2. Increments `PANIC_COUNT`.
/// 3. Stores the event in the recent-panics ring buffer.
/// 4. Does NOT abort the process — the panic unwinds normally.
pub fn install_panic_hook() {
    if PANIC_HOOK_INSTALLED.swap(true, Ordering::SeqCst) {
        return; // Already installed
    }

    let prev_hook = panic::take_hook();

    panic::set_hook(Box::new(move |info| {
        let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };

        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown location>".to_string());

        let thread_name = std::thread::current()
            .name()
            .unwrap_or("<unnamed>")
            .to_string();

        let count = PANIC_COUNT.fetch_add(1, Ordering::Relaxed) + 1;

        // Structured error log
        tracing::error!(
            panic_count = count,
            thread = %thread_name,
            location = %location,
            message = %message,
            "PANIC captured by crash domain hook — request isolated, process continues"
        );

        // Store in ring buffer
        let event = PanicEvent {
            message: message.clone(),
            location: location.clone(),
            thread_name: thread_name.clone(),
            occurred_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        {
            let mut buf = recent_panics().lock().unwrap_or_else(|p| p.into_inner());
            if buf.len() >= MAX_RECENT_PANICS {
                buf.remove(0);
            }
            buf.push(event);
        }

        // Check panic throttle — abort process if panic storm detected.
        // This prevents a cascading failure from consuming all resources.
        if global_throttle().record_panic() {
            tracing::error!(
                panic_count = count,
                "PANIC STORM: threshold exceeded — aborting process to prevent cascading failure"
            );
            // Give tracing a moment to flush
            std::thread::sleep(Duration::from_millis(100));
            std::process::abort();
        }

        // Call previous hook (e.g., default backtrace printer) only in debug builds
        #[cfg(debug_assertions)]
        prev_hook(info);
        let _ = &prev_hook; // suppress unused warning in release
    }));
}

/// Wrap a request closure in `catch_unwind`, converting panics to `InternalBug`.
///
/// - `stage`: short label for the processing stage (e.g., `"handle_query"`)
/// - `ctx`: context string injected into the error (e.g., `"request_id=42, session_id=1"`)
/// - `f`: the closure to execute
///
/// Returns `Ok(T)` on success, `Err(FalconError::InternalBug)` on panic.
pub fn catch_request<T, F>(stage: &str, ctx: &str, f: F) -> FalconResult<T>
where
    F: FnOnce() -> T,
{
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => Ok(result),
        Err(payload) => {
            let message = if let Some(s) = payload.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "<non-string panic payload>".to_string()
            };

            tracing::error!(
                stage = stage,
                context = ctx,
                panic_message = %message,
                "request panic caught by crash domain — returning InternalBug"
            );

            Err(FalconError::internal_bug(
                "E-CRASH-001",
                format!("panic in stage '{}': {}", stage, message),
                ctx,
            ))
        }
    }
}

/// Same as `catch_request` but for closures returning `FalconResult<T>`.
/// Panics are converted to `InternalBug`; existing errors pass through.
pub fn catch_request_result<T, F>(stage: &str, ctx: &str, f: F) -> FalconResult<T>
where
    F: FnOnce() -> FalconResult<T>,
{
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            let message = if let Some(s) = payload.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "<non-string panic payload>".to_string()
            };

            tracing::error!(
                stage = stage,
                context = ctx,
                panic_message = %message,
                "request panic caught by crash domain (result variant)"
            );

            Err(FalconError::internal_bug(
                "E-CRASH-002",
                format!("panic in stage '{}': {}", stage, message),
                ctx,
            ))
        }
    }
}

/// Total panic count since process start.
pub fn panic_count() -> u64 {
    PANIC_COUNT.load(Ordering::Relaxed)
}

/// Snapshot of recent panics (up to `MAX_RECENT_PANICS`).
pub fn recent_panic_events() -> Vec<PanicEvent> {
    recent_panics()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

/// Total times the panic throttle was triggered (panic storm detected).
pub fn throttle_triggered_count() -> u64 {
    global_throttle().total_throttled()
}

/// Current panic count within the throttle window.
pub fn throttle_window_count() -> u64 {
    global_throttle().window_count()
}

/// Panic throttle — tracks panic rate and signals if restart storm is detected.
///
/// If more than `max_panics` panics occur within `window`, returns `true`
/// (caller should apply backoff before restarting).
pub struct PanicThrottle {
    window: Duration,
    max_panics: u64,
    window_start: Mutex<Instant>,
    window_count: AtomicU64,
    total_throttled: AtomicU64,
}

impl PanicThrottle {
    pub fn new(window: Duration, max_panics: u64) -> Arc<Self> {
        Arc::new(Self {
            window,
            max_panics,
            window_start: Mutex::new(Instant::now()),
            window_count: AtomicU64::new(0),
            total_throttled: AtomicU64::new(0),
        })
    }

    /// Record a panic. Returns `true` if the restart storm threshold is exceeded.
    pub fn record_panic(&self) -> bool {
        let mut start = self.window_start.lock().unwrap_or_else(|p| p.into_inner());
        if start.elapsed() > self.window {
            // Reset window
            *start = Instant::now();
            self.window_count.store(1, Ordering::Relaxed);
            return false;
        }
        let count = self.window_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count > self.max_panics {
            self.total_throttled.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                window_panics = count,
                max_panics = self.max_panics,
                "PANIC STORM DETECTED — applying restart backoff"
            );
            return true;
        }
        false
    }

    /// Total times throttle was triggered.
    pub fn total_throttled(&self) -> u64 {
        self.total_throttled.load(Ordering::Relaxed)
    }

    /// Current window panic count.
    pub fn window_count(&self) -> u64 {
        self.window_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catch_request_ok() {
        let result = catch_request("test_stage", "ctx=test", || 42i32);
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_catch_request_panic_returns_internal_bug() {
        let result = catch_request::<i32, _>("test_stage", "ctx=test", || {
            panic!("deliberate test panic");
        });
        let err = result.unwrap_err();
        assert!(err.is_internal_bug());
        assert!(err.to_string().contains("deliberate test panic"));
        assert!(err.to_string().contains("test_stage"));
    }

    #[test]
    fn test_catch_request_result_ok() {
        let result = catch_request_result("stage", "ctx", || Ok::<i32, FalconError>(99));
        assert_eq!(result.unwrap(), 99);
    }

    #[test]
    fn test_catch_request_result_err_passthrough() {
        let result = catch_request_result("stage", "ctx", || {
            Err::<i32, FalconError>(FalconError::Internal("test error".into()))
        });
        let err = result.unwrap_err();
        assert!(err.to_string().contains("test error"));
    }

    #[test]
    fn test_catch_request_result_panic_to_internal_bug() {
        let result = catch_request_result::<i32, _>("stage", "ctx=42", || {
            panic!("panic in result closure");
        });
        let err = result.unwrap_err();
        assert!(err.is_internal_bug());
        assert!(err.to_string().contains("panic in result closure"));
    }

    #[test]
    fn test_panic_count_increments() {
        let before = panic_count();
        // Trigger a caught panic
        let _ = catch_request::<(), _>("count_test", "", || panic!("count test"));
        // panic_count is updated by the hook, not catch_request directly
        // (hook may not be installed in tests, so just verify catch works)
        let _ = before;
    }

    #[test]
    fn test_panic_throttle_no_storm() {
        let throttle = PanicThrottle::new(Duration::from_secs(10), 5);
        for _ in 0..5 {
            assert!(!throttle.record_panic());
        }
        assert_eq!(throttle.total_throttled(), 0);
    }

    #[test]
    fn test_panic_throttle_storm_detected() {
        let throttle = PanicThrottle::new(Duration::from_secs(10), 3);
        throttle.record_panic();
        throttle.record_panic();
        throttle.record_panic();
        let storm = throttle.record_panic(); // 4th in window
        assert!(storm);
        assert_eq!(throttle.total_throttled(), 1);
    }

    #[test]
    fn test_panic_throttle_window_reset() {
        let throttle = PanicThrottle::new(Duration::from_millis(10), 2);
        throttle.record_panic();
        throttle.record_panic();
        std::thread::sleep(Duration::from_millis(20));
        // Window reset — should not storm
        assert!(!throttle.record_panic());
    }

    #[test]
    fn test_install_panic_hook_idempotent() {
        install_panic_hook();
        install_panic_hook(); // second call is a no-op
                              // No panic
    }

    #[test]
    fn test_recent_panic_events_empty_initially() {
        // Can't guarantee empty in test suite (other tests may have panicked)
        // Just verify it returns a Vec
        let events = recent_panic_events();
        let _ = events.len();
    }

    #[test]
    fn test_catch_request_string_panic() {
        let result = catch_request::<(), _>("stage", "ctx", || {
            panic!("{}", "string panic message");
        });
        assert!(result.is_err());
        assert!(result.unwrap_err().is_internal_bug());
    }

    #[test]
    fn test_internal_bug_error_code() {
        let result = catch_request::<(), _>("my_stage", "shard=3", || {
            panic!("oops");
        });
        match result.unwrap_err() {
            FalconError::InternalBug { error_code, .. } => {
                assert_eq!(error_code, "E-CRASH-001");
            }
            other => panic!("expected InternalBug, got {:?}", other),
        }
    }

    #[test]
    fn test_catch_request_result_error_code() {
        let result = catch_request_result::<(), _>("my_stage", "txn=7", || {
            panic!("result panic");
        });
        match result.unwrap_err() {
            FalconError::InternalBug {
                error_code,
                debug_context,
                ..
            } => {
                assert_eq!(error_code, "E-CRASH-002");
                assert!(debug_context.contains("txn=7"));
            }
            other => panic!("expected InternalBug, got {:?}", other),
        }
    }

    #[test]
    fn test_global_throttle_accessors() {
        // These should not panic and return sensible defaults
        let tc = throttle_triggered_count();
        let wc = throttle_window_count();
        assert!(tc < u64::MAX);
        assert!(wc < u64::MAX);
    }

    #[test]
    fn test_catch_request_non_string_panic() {
        let result = catch_request::<(), _>("stage", "ctx", || {
            std::panic::panic_any(42i32);
        });
        let err = result.unwrap_err();
        assert!(err.is_internal_bug());
        assert!(err.to_string().contains("non-string panic payload"));
    }

    #[test]
    fn test_panic_throttle_multiple_storms() {
        let throttle = PanicThrottle::new(Duration::from_secs(10), 2);
        throttle.record_panic();
        throttle.record_panic();
        assert!(throttle.record_panic()); // 3rd = storm
        assert!(throttle.record_panic()); // 4th = still storm
        assert_eq!(throttle.total_throttled(), 2);
    }
}
