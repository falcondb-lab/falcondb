//! Diagnostic checks for FalconDB deployment.
//!
//! `falcon doctor` runs pre-flight checks to identify common issues
//! before starting the server. Outputs OK / WARN / FAIL per check
//! with actionable fix suggestions.

use std::net::TcpListener;
use std::path::Path;

/// Run all diagnostic checks and print results.
pub fn run_doctor(config_path: &str) {
    println!("FalconDB Doctor v{}", env!("CARGO_PKG_VERSION"));
    println!("===============");
    println!();

    let mut pass = 0u32;
    let mut fail = 0u32;
    let mut warn = 0u32;

    // 1. Config file
    print!("  [check] Config file: {config_path} ... ");
    if Path::new(config_path).exists() {
        println!("OK");
        pass += 1;

        // 1b. Config version check
        print!("  [check] Config version ... ");
        match std::fs::read_to_string(config_path) {
            Ok(content) => match toml::from_str::<falcon_common::config::FalconConfig>(&content) {
                Ok(cfg) => {
                    let current = falcon_common::config::CURRENT_CONFIG_VERSION;
                    if cfg.config_version == current {
                        println!("OK (v{})", cfg.config_version);
                        pass += 1;
                    } else if cfg.config_version < current {
                        println!(
                            "WARN — v{} (current: v{}). Run: falcon config migrate",
                            cfg.config_version, current
                        );
                        warn += 1;
                    } else {
                        println!(
                            "FAIL — v{} is newer than this binary (v{}). Upgrade falcon.exe",
                            cfg.config_version, current
                        );
                        fail += 1;
                    }
                }
                Err(e) => {
                    println!("FAIL — parse error: {e}");
                    fail += 1;
                }
            },
            Err(e) => {
                println!("FAIL — read error: {e}");
                fail += 1;
            }
        }
    } else {
        println!("MISSING");
        fail += 1;
    }

    // 2. Ports
    for (name, port) in &[("PG", 5443u16), ("Admin/Health", 8080)] {
        print!("  [check] Port {port} ({name}) ... ");
        match TcpListener::bind(("127.0.0.1", *port)) {
            Ok(_) => {
                println!("AVAILABLE");
                pass += 1;
            }
            Err(e) => {
                println!("IN USE ({e})");
                println!("         Fix: netstat -ano | findstr :{port}");
                fail += 1;
            }
        }
    }

    // 3. Data directory
    let data_dirs = [
        "data".to_owned(),
        crate::service::paths::service_data_dir()
            .to_string_lossy()
            .to_string(),
    ];
    for dir in &data_dirs {
        let p = Path::new(dir);
        if p.exists() {
            print!("  [check] Data dir: {dir} ... ");
            let test_file = p.join(".falcon_doctor_test");
            match std::fs::write(&test_file, b"test") {
                Ok(_) => {
                    let _ = std::fs::remove_file(&test_file);
                    println!("WRITABLE");
                    pass += 1;
                }
                Err(e) => {
                    println!("NOT WRITABLE ({e})");
                    println!("         Fix: check directory permissions");
                    fail += 1;
                }
            }
        }
    }

    // 4. WAL directory
    let wal_dir = crate::service::paths::service_data_dir();
    print!("  [check] WAL dir: {} ... ", wal_dir.display());
    if wal_dir.exists() {
        // Check for WAL segment files
        let has_wal = std::fs::read_dir(&wal_dir)
            .map(|entries| {
                entries
                    .flatten()
                    .any(|e| {
                        let name = e.file_name();
                        let name = name.to_string_lossy();
                        name.ends_with(".wal")
                    })
            })
            .unwrap_or(false);
        if has_wal {
            println!("OK (WAL segments present)");
        } else {
            println!("OK (empty — first run)");
        }
        pass += 1;
    } else {
        println!("NOT FOUND (will be created on first start)");
        warn += 1;
    }

    // 5. Clock skew (basic — warn if system clock looks stale)
    print!("  [check] System clock ... ");
    let now = std::time::SystemTime::now();
    if let Ok(d) = now.duration_since(std::time::UNIX_EPOCH) {
        let secs = d.as_secs();
        // Sanity: epoch > 2020-01-01 (1577836800) and < 2040-01-01 (2208988800)
        if secs > 1_577_836_800 && secs < 2_208_988_800 {
            println!("OK");
            pass += 1;
        } else {
            println!("WARN — clock looks incorrect (epoch={secs})");
            println!("         Fix: sync system clock (w32tm /resync or NTP)");
            warn += 1;
        }
    } else {
        println!("WARN — clock before UNIX epoch");
        warn += 1;
    }

    // 6. Service status (Windows only)
    #[cfg(windows)]
    {
        print!("  [check] Windows Service ... ");
        let output = std::process::Command::new("sc.exe")
            .args(["query", "FalconDB"])
            .output();
        match output {
            Ok(o) if o.status.success() => {
                let stdout = String::from_utf8_lossy(&o.stdout);
                if stdout.contains("RUNNING") {
                    println!("INSTALLED (RUNNING)");
                } else if stdout.contains("STOPPED") {
                    println!("INSTALLED (STOPPED)");
                } else {
                    println!("INSTALLED (state unknown)");
                }
                pass += 1;
            }
            _ => {
                println!("NOT INSTALLED");
                warn += 1;
            }
        }
    }

    // 7. ProgramData directory
    let pd_root = crate::service::paths::program_data_root();
    print!(
        "  [check] ProgramData dir: {} ... ",
        pd_root.display()
    );
    if pd_root.exists() {
        println!("EXISTS");
        pass += 1;
    } else {
        println!("NOT CREATED (run 'falcon service install' to create)");
        warn += 1;
    }

    // 8. WAL I/O diagnostics (Windows-specific)
    println!();
    println!("WAL I/O Diagnostics:");
    {
        print!("  [check] IOCP support ... ");
        if falcon_storage::wal_win_async::iocp_available() {
            println!("OK");
            pass += 1;
        } else {
            println!("NOT AVAILABLE (non-Windows platform)");
            warn += 1;
        }

        let wal_test_dir = crate::service::paths::service_data_dir();
        let check_dir = if wal_test_dir.exists() {
            wal_test_dir
        } else {
            std::env::temp_dir()
        };

        print!("  [check] FILE_FLAG_NO_BUFFERING on {} ... ", check_dir.display());
        let (no_buf_ok, sector_size) =
            falcon_storage::wal_win_async::check_no_buffering_support(&check_dir);
        if no_buf_ok {
            println!("OK (sector_size={sector_size})");
            pass += 1;
        } else {
            println!("NOT SUPPORTED");
            println!("         Hint: NO_BUFFERING requires NTFS and aligned writes");
            warn += 1;
        }

        print!("  [check] Disk alignment ... ");
        let (aligned, sector, fs_hint) =
            falcon_storage::wal_win_async::check_disk_alignment(&check_dir);
        if aligned {
            println!("OK ({fs_hint}, sector={sector})");
            pass += 1;
        } else {
            println!("WARN — alignment check failed ({fs_hint})");
            warn += 1;
        }

        // Recommended WAL mode
        println!();
        print!("  [info]  Recommended WAL mode: ");
        if falcon_storage::wal_win_async::iocp_available() && no_buf_ok {
            println!("WalDeviceWinAsync (IOCP + NO_BUFFERING)");
        } else if falcon_storage::wal_win_async::iocp_available() {
            println!("WalDeviceWinAsync (IOCP, buffered)");
        } else {
            println!("WalDeviceFile (standard file I/O)");
        }

        println!("  [info]  Raw Disk WAL: not enabled (future v1.1+)");
    }

    // Summary
    println!();
    println!("Summary: {pass} passed, {fail} failed, {warn} warnings");
    if fail > 0 {
        println!("  Fix the failures above before starting FalconDB.");
        std::process::exit(1);
    } else if warn > 0 {
        println!("  Warnings found — FalconDB can start but review above.");
    } else {
        println!("  FalconDB is ready to start.");
    }
}
