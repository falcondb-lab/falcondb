//! FalconDB server library — exposes the shutdown protocol, service
//! management, config migration, and diagnostics.

pub mod config_migrate;
pub mod doctor;
pub mod service;
pub mod shutdown;
