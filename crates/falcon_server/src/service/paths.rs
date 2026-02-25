//! Standard directory conventions for FalconDB on Windows.
//!
//! Service mode uses fixed ProgramData paths:
//!   - Config: C:\ProgramData\FalconDB\conf\falcon.toml
//!   - Data:   C:\ProgramData\FalconDB\data\
//!   - Logs:   C:\ProgramData\FalconDB\logs\
//!
//! Console mode allows relative paths (Phase A behavior).

use std::path::PathBuf;

/// Service name used for Windows Service registration.
pub const SERVICE_NAME: &str = "FalconDB";

/// Display name shown in services.msc.
pub const SERVICE_DISPLAY_NAME: &str = "FalconDB Database";

/// Service description.
pub const SERVICE_DESCRIPTION: &str =
    "FalconDB — PG-Compatible In-Memory OLTP Database";

/// Root directory under ProgramData for service mode.
pub fn program_data_root() -> PathBuf {
    let base = std::env::var("ProgramData")
        .unwrap_or_else(|_| r"C:\ProgramData".to_owned());
    PathBuf::from(base).join("FalconDB")
}

/// Config directory: C:\ProgramData\FalconDB\conf\
pub fn service_conf_dir() -> PathBuf {
    program_data_root().join("conf")
}

/// Default config file path for service mode.
pub fn service_config_path() -> PathBuf {
    service_conf_dir().join("falcon.toml")
}

/// Data directory: C:\ProgramData\FalconDB\data\
pub fn service_data_dir() -> PathBuf {
    program_data_root().join("data")
}

/// Log directory: C:\ProgramData\FalconDB\logs\
pub fn service_log_dir() -> PathBuf {
    program_data_root().join("logs")
}

/// Certificates directory: C:\ProgramData\FalconDB\certs\
pub fn service_certs_dir() -> PathBuf {
    program_data_root().join("certs")
}

/// Install directory: C:\Program Files\FalconDB\
pub fn program_files_root() -> PathBuf {
    let base = std::env::var("ProgramFiles")
        .unwrap_or_else(|_| r"C:\Program Files".to_owned());
    PathBuf::from(base).join("FalconDB")
}

/// Ensure all service directories exist. Returns an error message on failure.
pub fn ensure_service_dirs() -> Result<(), String> {
    for dir in &[
        program_data_root(),
        service_conf_dir(),
        service_data_dir(),
        service_log_dir(),
        service_certs_dir(),
    ] {
        if !dir.exists() {
            std::fs::create_dir_all(dir)
                .map_err(|e| format!("Failed to create {}: {}", dir.display(), e))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_program_data_root_is_absolute() {
        let root = program_data_root();
        assert!(root.is_absolute());
        assert!(root.ends_with("FalconDB"));
    }

    #[test]
    fn test_service_paths_under_root() {
        let root = program_data_root();
        assert!(service_conf_dir().starts_with(&root));
        assert!(service_data_dir().starts_with(&root));
        assert!(service_log_dir().starts_with(&root));
    }

    #[test]
    fn test_service_config_path() {
        let path = service_config_path();
        assert!(path.ends_with("falcon.toml"));
    }
}
