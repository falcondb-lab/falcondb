//! Service lifecycle management commands.
//!
//! Implements `falcon service install/uninstall/start/stop/restart/status`
//! using Windows `sc.exe` — no third-party tools required.

use std::process::Command;

use super::paths::*;

/// Install FalconDB as a Windows Service.
pub fn install(config_path: &str) -> Result<(), String> {
    ensure_service_dirs()?;

    // Resolve absolute paths
    let exe_path = std::env::current_exe()
        .map_err(|e| format!("Cannot determine exe path: {e}"))?;

    let config_abs = if std::path::Path::new(config_path).is_absolute() {
        config_path.to_owned()
    } else {
        std::env::current_dir()
            .map(|d| d.join(config_path)).map_or_else(|_| config_path.to_owned(), |p| p.to_string_lossy().to_string())
    };

    // Copy config to ProgramData if it's not already there
    let service_conf = service_config_path();
    if !service_conf.exists() {
        if std::path::Path::new(&config_abs).exists() {
            std::fs::copy(&config_abs, &service_conf)
                .map_err(|e| format!("Failed to copy config to {}: {}", service_conf.display(), e))?;
            println!("  Config copied to: {}", service_conf.display());
        } else {
            return Err(format!(
                "Config file not found: {config_abs}\n  Provide a valid --config path or place falcon.toml in conf/"
            ));
        }
    } else {
        println!("  Config already exists: {}", service_conf.display());
    }

    // Patch data_dir in the copied config to use ProgramData path
    patch_service_config(&service_conf)?;

    // Build binPath for sc.exe
    // Service mode: falcon.exe service dispatch --config <ProgramData config>
    let bin_path = format!(
        "\"{}\" service dispatch --config \"{}\"",
        exe_path.display(),
        service_conf.display()
    );

    println!("  binPath: {bin_path}");

    // Check if service already exists
    let query = Command::new("sc.exe")
        .args(["query", SERVICE_NAME])
        .output()
        .map_err(|e| format!("Failed to run sc.exe: {e}"))?;

    if query.status.success() {
        return Err(format!(
            "Service '{SERVICE_NAME}' already exists. Run 'falcon service uninstall' first."
        ));
    }

    // Create the service
    let create = Command::new("sc.exe")
        .args([
            "create",
            SERVICE_NAME,
            &format!("binPath= {bin_path}"),
            &format!("DisplayName= {SERVICE_DISPLAY_NAME}"),
            "start= auto",
        ])
        .output()
        .map_err(|e| format!("Failed to run sc.exe create: {e}"))?;

    if !create.status.success() {
        let stderr = String::from_utf8_lossy(&create.stderr);
        let stdout = String::from_utf8_lossy(&create.stdout);
        return Err(format!(
            "sc.exe create failed:\n  stdout: {}\n  stderr: {}",
            stdout.trim(),
            stderr.trim()
        ));
    }

    // Set description
    let _ = Command::new("sc.exe")
        .args(["description", SERVICE_NAME, SERVICE_DESCRIPTION])
        .output();

    // Set failure recovery: restart on failure
    let _ = Command::new("sc.exe")
        .args([
            "failure",
            SERVICE_NAME,
            "reset=",
            "86400",
            "actions=",
            "restart/5000/restart/10000/restart/30000",
        ])
        .output();

    println!();
    println!("Service '{SERVICE_NAME}' installed successfully.");
    println!();
    println!("  Config:  {}", service_conf.display());
    println!("  Data:    {}", service_data_dir().display());
    println!("  Logs:    {}", service_log_dir().display());
    println!();
    println!("  Start:   falcon service start");
    println!("  Status:  falcon service status");
    println!("  Stop:    falcon service stop");

    Ok(())
}

/// Uninstall the FalconDB Windows Service.
pub fn uninstall() -> Result<(), String> {
    // Check if service exists
    let query = Command::new("sc.exe")
        .args(["query", SERVICE_NAME])
        .output()
        .map_err(|e| format!("Failed to run sc.exe: {e}"))?;

    if !query.status.success() {
        println!("Service '{SERVICE_NAME}' does not exist. Nothing to do.");
        return Ok(());
    }

    // Stop if running
    let stdout = String::from_utf8_lossy(&query.stdout);
    if stdout.contains("RUNNING") {
        println!("Stopping service '{SERVICE_NAME}'...");
        let _ = Command::new("sc.exe")
            .args(["stop", SERVICE_NAME])
            .output();
        // Wait for stop
        for _ in 0..30 {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let check = Command::new("sc.exe")
                .args(["query", SERVICE_NAME])
                .output()
                .map_err(|e| format!("sc.exe query failed: {e}"))?;
            let out = String::from_utf8_lossy(&check.stdout);
            if out.contains("STOPPED") {
                println!("  Service stopped.");
                break;
            }
        }
    }

    // Delete
    let delete = Command::new("sc.exe")
        .args(["delete", SERVICE_NAME])
        .output()
        .map_err(|e| format!("Failed to run sc.exe delete: {e}"))?;

    if !delete.status.success() {
        let stderr = String::from_utf8_lossy(&delete.stderr);
        return Err(format!("sc.exe delete failed: {}", stderr.trim()));
    }

    println!("Service '{SERVICE_NAME}' uninstalled.");
    println!();
    println!("Note: Data and config in {} are NOT deleted.", program_data_root().display());
    println!("  Remove manually if desired.");

    Ok(())
}

/// Start the FalconDB Windows Service.
pub fn start() -> Result<(), String> {
    let output = Command::new("sc.exe")
        .args(["start", SERVICE_NAME])
        .output()
        .map_err(|e| format!("Failed to run sc.exe: {e}"))?;

    if output.status.success() {
        println!("Service '{SERVICE_NAME}' start requested.");
        println!("  Check status: falcon service status");
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(format!(
            "Failed to start service:\n  {}\n  {}",
            stdout.trim(),
            stderr.trim()
        ));
    }

    Ok(())
}

/// Stop the FalconDB Windows Service.
pub fn stop() -> Result<(), String> {
    let output = Command::new("sc.exe")
        .args(["stop", SERVICE_NAME])
        .output()
        .map_err(|e| format!("Failed to run sc.exe: {e}"))?;

    if output.status.success() {
        println!("Service '{SERVICE_NAME}' stop requested.");
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(format!(
            "Failed to stop service:\n  {}\n  {}",
            stdout.trim(),
            stderr.trim()
        ));
    }

    Ok(())
}

/// Restart the FalconDB Windows Service.
pub fn restart() -> Result<(), String> {
    stop()?;
    // Wait a moment for the service to fully stop
    println!("Waiting for service to stop...");
    for _ in 0..15 {
        std::thread::sleep(std::time::Duration::from_secs(1));
        let check = Command::new("sc.exe")
            .args(["query", SERVICE_NAME])
            .output()
            .map_err(|e| format!("sc.exe query failed: {e}"))?;
        let out = String::from_utf8_lossy(&check.stdout);
        if out.contains("STOPPED") {
            break;
        }
    }
    start()
}

/// Show the status of the FalconDB Windows Service.
pub fn status() -> Result<(), String> {
    let output = Command::new("sc.exe")
        .args(["query", SERVICE_NAME])
        .output()
        .map_err(|e| format!("Failed to run sc.exe: {e}"))?;

    if !output.status.success() {
        println!("Service '{SERVICE_NAME}' is not installed.");
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Parse state from sc output
    let state = if stdout.contains("RUNNING") {
        "RUNNING"
    } else if stdout.contains("STOPPED") {
        "STOPPED"
    } else if stdout.contains("START_PENDING") {
        "START_PENDING"
    } else if stdout.contains("STOP_PENDING") {
        "STOP_PENDING"
    } else {
        "UNKNOWN"
    };

    // Parse PID
    let pid = stdout
        .lines()
        .find(|l| l.contains("PID"))
        .and_then(|l| l.split(':').nth(1)).map_or_else(|| "N/A".to_owned(), |s| s.trim().to_owned());

    println!("FalconDB Service Status");
    println!("=======================");
    println!("  Service:  {SERVICE_NAME}");
    println!("  State:    {state}");
    println!("  PID:      {pid}");
    println!("  Config:   {}", service_config_path().display());
    println!("  Data:     {}", service_data_dir().display());
    println!("  Logs:     {}", service_log_dir().display());

    if state == "RUNNING" {
        println!();
        println!("  Ports:");
        println!("    PG:     5443 (default)");
        println!("    Admin:  8080 (default)");
    }

    Ok(())
}

/// Patch the service config to use ProgramData paths for data_dir.
fn patch_service_config(config_path: &std::path::Path) -> Result<(), String> {
    let content = std::fs::read_to_string(config_path)
        .map_err(|e| format!("Failed to read config: {e}"))?;

    // Replace relative data_dir with absolute ProgramData path
    let data_dir_abs = service_data_dir()
        .to_string_lossy()
        .replace('\\', "\\\\");

    let patched = content
        .lines()
        .map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("data_dir") && !trimmed.starts_with('#') {
                // Check if it's a relative path (not starting with drive letter)
                if let Some(val) = trimmed.split('=').nth(1) {
                    let val = val.trim().trim_matches('"');
                    if !val.contains(':') && !val.starts_with("\\\\") {
                        return format!("data_dir = \"{data_dir_abs}\"");
                    }
                }
                line.to_owned()
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    std::fs::write(config_path, patched)
        .map_err(|e| format!("Failed to write patched config: {e}"))?;

    Ok(())
}
