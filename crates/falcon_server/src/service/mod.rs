//! Windows Service support for FalconDB.
//!
//! This module provides:
//! - Native Windows SCM integration (`windows.rs`)
//! - Service lifecycle commands: install/uninstall/start/stop/restart/status (`commands.rs`)
//! - Standard directory conventions for service mode (`paths.rs`)
//! - File-based logging for service mode (`logger.rs`)

pub mod commands;
pub mod logger;
pub mod paths;
pub mod windows;
