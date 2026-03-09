//! FalconDB PoC #3 — Deterministic Commit Marker Writer
//!
//! Writes monotonically increasing tx_markers to FalconDB. Each marker_id is
//! logged to `committed_markers.log` ONLY after the server confirms COMMIT.
//!
//! Key capability: **automatic reconnect to a new primary after failover**.
//! The writer detects connection loss, waits, then reconnects to the
//! configured failover endpoint. It never silently drops a committed marker.
//!
//! Environment variables:
//!   FALCON_HOST          — primary host (default: 127.0.0.1)
//!   FALCON_PORT          — primary port (default: 5433)
//!   FALCON_DB            — database name (default: falcon)
//!   FALCON_USER          — database user (default: falcon)
//!   FAILOVER_HOST        — new primary host after failover (default: 127.0.0.1)
//!   FAILOVER_PORT        — new primary port after failover (default: 5434)
//!   MARKER_COUNT         — total markers to write (default: 50000)
//!   OUTPUT_DIR           — directory for output files (default: ./output)
//!   START_MARKER_ID      — first marker_id (default: 1)
//!   STOP_FILE            — if this file exists, writer stops gracefully

use chrono::Utc;
use postgres::{Client, NoTls};
use serde::Serialize;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::process;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct LoadMetrics {
    start_time: String,
    end_time: String,
    duration_ms: u128,
    markers_attempted: u64,
    markers_committed: u64,
    markers_failed: u64,
    first_marker_id: u64,
    last_committed_marker_id: u64,
    reconnect_count: u64,
    last_commit_before_failure_ts: String,
    first_commit_after_reconnect_ts: String,
    primary_host: String,
    primary_port: u16,
    failover_host: String,
    failover_port: u16,
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn build_connstr(host: &str, port: u16, db: &str, user: &str) -> String {
    format!(
        "host={} port={} dbname={} user={} connect_timeout=5",
        host, port, db, user
    )
}

fn try_connect(connstr: &str) -> Option<Client> {
    match Client::connect(connstr, NoTls) {
        Ok(c) => Some(c),
        Err(e) => {
            eprintln!("[tx_marker_writer] Connect failed: {}", e);
            None
        }
    }
}

fn connect_with_retry(connstr: &str, max_attempts: u32) -> Option<Client> {
    for attempt in 1..=max_attempts {
        if let Some(c) = try_connect(connstr) {
            return Some(c);
        }
        if attempt < max_attempts {
            thread::sleep(Duration::from_millis(500));
        }
    }
    None
}

fn main() {
    let primary_host = env_or("FALCON_HOST", "127.0.0.1");
    let primary_port: u16 = env_or("FALCON_PORT", "5433").parse().unwrap_or(5433);
    let failover_host = env_or("FAILOVER_HOST", "127.0.0.1");
    let failover_port: u16 = env_or("FAILOVER_PORT", "5434").parse().unwrap_or(5434);
    let db = env_or("FALCON_DB", "falcon");
    let user = env_or("FALCON_USER", "falcon");
    let marker_count: u64 = env_or("MARKER_COUNT", "50000").parse().unwrap_or(50000);
    let output_dir = PathBuf::from(env_or("OUTPUT_DIR", "./output"));
    let start_id: u64 = env_or("START_MARKER_ID", "1").parse().unwrap_or(1);
    let stop_file = env_or("STOP_FILE", "");

    fs::create_dir_all(&output_dir).unwrap_or_else(|e| {
        eprintln!("[tx_marker_writer] Cannot create output dir: {}", e);
        process::exit(1);
    });

    let committed_log_path = output_dir.join("committed_markers.log");
    let metrics_path = output_dir.join("load_metrics.json");

    let primary_connstr = build_connstr(&primary_host, primary_port, &db, &user);
    let failover_connstr = build_connstr(&failover_host, failover_port, &db, &user);

    // Connect to primary
    eprintln!(
        "[tx_marker_writer] Connecting to primary {}:{}...",
        primary_host, primary_port
    );
    let mut client = match connect_with_retry(&primary_connstr, 30) {
        Some(c) => c,
        None => {
            eprintln!("[tx_marker_writer] FATAL: Could not connect to primary");
            process::exit(1);
        }
    };
    eprintln!("[tx_marker_writer] Connected to primary.");

    let mut current_connstr = primary_connstr.clone();
    let mut current_label = format!("{}:{}", primary_host, primary_port);

    // Open committed log
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&committed_log_path)
        .unwrap_or_else(|e| {
            eprintln!(
                "[tx_marker_writer] Cannot open {}: {}",
                committed_log_path.display(),
                e
            );
            process::exit(1);
        });
    let mut log_writer = BufWriter::new(log_file);

    let start_time = Utc::now();
    let timer = Instant::now();

    let mut committed: u64 = 0;
    let mut failed: u64 = 0;
    let mut reconnect_count: u64 = 0;
    let mut last_committed_id: u64 = 0;
    let mut last_commit_before_failure_ts = String::new();
    let mut first_commit_after_reconnect_ts = String::new();
    let mut just_reconnected = false;

    eprintln!(
        "[tx_marker_writer] Writing {} markers (id {} to {})...",
        marker_count,
        start_id,
        start_id + marker_count - 1
    );

    let mut i: u64 = 0;
    while i < marker_count {
        // Check stop file
        if !stop_file.is_empty() && std::path::Path::new(&stop_file).exists() {
            eprintln!("[tx_marker_writer] Stop file detected. Stopping gracefully.");
            break;
        }

        let marker_id = start_id + i;
        let source_node = &current_label;

        let result = (|| -> Result<(), postgres::Error> {
            let mut txn = client.transaction()?;
            txn.execute(
                "INSERT INTO tx_markers (marker_id, source_node) VALUES ($1, $2)",
                &[&(marker_id as i64), &source_node.as_str()],
            )?;
            txn.commit()?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                // COMMIT confirmed — safe to log
                let ts = Utc::now().to_rfc3339();
                writeln!(log_writer, "{}|{}", marker_id, ts).ok();
                log_writer.flush().ok();
                committed += 1;
                last_committed_id = marker_id;

                if just_reconnected {
                    first_commit_after_reconnect_ts = ts;
                    just_reconnected = false;
                    eprintln!(
                        "[tx_marker_writer] First commit after reconnect: marker_id={}",
                        marker_id
                    );
                }

                i += 1;
            }
            Err(e) => {
                eprintln!(
                    "[tx_marker_writer] marker_id={} FAILED: {}",
                    marker_id, e
                );
                failed += 1;

                // Record last successful timestamp before failure
                if last_commit_before_failure_ts.is_empty() && committed > 0 {
                    last_commit_before_failure_ts = Utc::now().to_rfc3339();
                }

                // Try to reconnect — first to current, then to failover
                eprintln!("[tx_marker_writer] Connection lost. Attempting reconnect...");

                let mut reconnected = false;

                // Try failover endpoint first (primary is likely dead)
                eprintln!(
                    "[tx_marker_writer] Trying failover endpoint {}:{}...",
                    failover_host, failover_port
                );
                if let Some(c) = connect_with_retry(&failover_connstr, 20) {
                    client = c;
                    current_connstr = failover_connstr.clone();
                    current_label = format!("{}:{}", failover_host, failover_port);
                    reconnected = true;
                    eprintln!(
                        "[tx_marker_writer] Reconnected to failover {}",
                        current_label
                    );
                }

                if !reconnected {
                    // Try original primary
                    eprintln!(
                        "[tx_marker_writer] Trying original primary {}:{}...",
                        primary_host, primary_port
                    );
                    if let Some(c) = connect_with_retry(&primary_connstr, 10) {
                        client = c;
                        current_connstr = primary_connstr.clone();
                        current_label = format!("{}:{}", primary_host, primary_port);
                        reconnected = true;
                    }
                }

                if reconnected {
                    reconnect_count += 1;
                    just_reconnected = true;
                    // Do NOT increment i — retry the same marker_id
                } else {
                    eprintln!(
                        "[tx_marker_writer] Cannot reconnect to any endpoint. Stopping."
                    );
                    break;
                }
            }
        }

        // Progress every 5000 markers
        if committed > 0 && committed % 5000 == 0 {
            eprintln!(
                "[tx_marker_writer] Progress: committed={}, failed={}, reconnects={}",
                committed, failed, reconnect_count
            );
        }
    }

    let elapsed = timer.elapsed();
    let end_time = Utc::now();

    // Write load metrics
    let metrics = LoadMetrics {
        start_time: start_time.to_rfc3339(),
        end_time: end_time.to_rfc3339(),
        duration_ms: elapsed.as_millis(),
        markers_attempted: marker_count,
        markers_committed: committed,
        markers_failed: failed,
        first_marker_id: start_id,
        last_committed_marker_id: last_committed_id,
        reconnect_count,
        last_commit_before_failure_ts: if last_commit_before_failure_ts.is_empty() {
            "N/A".to_string()
        } else {
            last_commit_before_failure_ts
        },
        first_commit_after_reconnect_ts: if first_commit_after_reconnect_ts.is_empty() {
            "N/A".to_string()
        } else {
            first_commit_after_reconnect_ts
        },
        primary_host: primary_host.clone(),
        primary_port,
        failover_host: failover_host.clone(),
        failover_port,
    };

    let metrics_json = serde_json::to_string_pretty(&metrics).unwrap();
    fs::write(&metrics_path, &metrics_json).unwrap_or_else(|e| {
        eprintln!("[tx_marker_writer] Cannot write metrics: {}", e);
    });

    eprintln!(
        "[tx_marker_writer] Done. {} committed, {} failed, {} reconnects.",
        committed, failed, reconnect_count
    );
    eprintln!("[tx_marker_writer] Log: {}", committed_log_path.display());
    eprintln!("[tx_marker_writer] Metrics: {}", metrics_path.display());

    println!("{}", metrics_json);
}
