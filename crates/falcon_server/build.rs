// FalconDB — build.rs for falcon_server
// Injects build-time metadata as environment variables:
//   FALCONDB_GIT_HASH    — short git commit hash (or "unknown")
//   FALCONDB_BUILD_TIME  — UTC build timestamp
//   FALCONDB_VERSION     — from CARGO_PKG_VERSION (workspace)

use std::process::Command;

fn main() {
    // Git short hash
    let git_hash = Command::new("git")
        .args(["rev-parse", "--short=8", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success()).map_or_else(|| "unknown".to_owned(), |o| String::from_utf8_lossy(&o.stdout).trim().to_owned());

    // Build timestamp (UTC, second precision)
    let build_time = chrono_free_utc_now();

    println!("cargo:rustc-env=FALCONDB_GIT_HASH={git_hash}");
    println!("cargo:rustc-env=FALCONDB_BUILD_TIME={build_time}");
    // Rerun only when HEAD changes or Cargo.toml changes
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../Cargo.toml");
}

/// UTC timestamp without pulling in chrono — uses system time.
fn chrono_free_utc_now() -> String {
    // Format: 2025-02-24T21:42:00Z
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    // Simple UTC formatting
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Compute year/month/day from days since epoch (1970-01-01)
    let (year, month, day) = days_to_ymd(days_since_epoch);
    format!(
        "{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z"
    )
}

fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    let mut year = 1970u64;
    loop {
        let days_in_year = if is_leap(year) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    let month_days: [u64; 12] = if is_leap(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut month = 1u64;
    for &md in &month_days {
        if days < md {
            break;
        }
        days -= md;
        month += 1;
    }
    (year, month, days + 1)
}

const fn is_leap(y: u64) -> bool {
    (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400)
}
