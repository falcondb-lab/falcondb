//! File-based logging for Windows Service mode.
//!
//! When running as a service, stderr is not visible. This module sets up
//! a daily-rotating file logger under the service log directory.

use std::path::Path;

use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

/// Guard that must be held for the lifetime of the process.
/// Dropping it flushes pending log writes.
pub type LogGuard = tracing_appender::non_blocking::WorkerGuard;

/// Initialize tracing with a daily-rotating file logger.
///
/// Returns a guard that **must** be kept alive until shutdown completes.
/// Dropping the guard flushes buffered log output.
pub fn init_file_logger(log_dir: &Path, filename_prefix: &str) -> LogGuard {
    let file_appender = RollingFileAppender::new(
        Rotation::DAILY,
        log_dir,
        filename_prefix,
    );
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_ansi(false)
        .with_writer(non_blocking);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();

    guard
}
