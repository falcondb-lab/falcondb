//! FalconDB DCG PoC — Deterministic Order Writer
//!
//! Connects to a FalconDB (or PostgreSQL-compatible) server and writes
//! monotonically increasing orders. Each order_id is logged to
//! `committed_orders.log` ONLY after the server confirms COMMIT.
//!
//! Usage:
//!   order_writer [OPTIONS]
//!
//! Environment variables:
//!   FALCON_HOST       — database host (default: 127.0.0.1)
//!   FALCON_PORT       — database port (default: 5433)
//!   FALCON_DB         — database name (default: falcon)
//!   FALCON_USER       — database user (default: falcon)
//!   ORDER_COUNT       — number of orders to write (default: 1000)
//!   OUTPUT_DIR        — directory for output files (default: ./output)
//!   START_ORDER_ID    — first order_id (default: 1)

use chrono::Utc;
use postgres::{Client, NoTls};
use serde::Serialize;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::process;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct WorkloadSummary {
    start_time: String,
    end_time: String,
    duration_ms: u128,
    orders_attempted: u64,
    orders_committed: u64,
    orders_failed: u64,
    first_order_id: u64,
    last_committed_order_id: u64,
    host: String,
    port: u16,
    database: String,
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn connect_with_retry(connstr: &str, max_attempts: u32) -> Option<Client> {
    for attempt in 1..=max_attempts {
        match Client::connect(connstr, NoTls) {
            Ok(client) => return Some(client),
            Err(e) => {
                eprintln!(
                    "[order_writer] Connection attempt {}/{} failed: {}",
                    attempt, max_attempts, e
                );
                if attempt < max_attempts {
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }
    None
}

fn main() {
    let host = env_or("FALCON_HOST", "127.0.0.1");
    let port: u16 = env_or("FALCON_PORT", "5433").parse().unwrap_or(5433);
    let db = env_or("FALCON_DB", "falcon");
    let user = env_or("FALCON_USER", "falcon");
    let order_count: u64 = env_or("ORDER_COUNT", "1000").parse().unwrap_or(1000);
    let output_dir = PathBuf::from(env_or("OUTPUT_DIR", "./output"));
    let start_id: u64 = env_or("START_ORDER_ID", "1").parse().unwrap_or(1);

    // Ensure output directory exists
    fs::create_dir_all(&output_dir).unwrap_or_else(|e| {
        eprintln!("[order_writer] Cannot create output dir: {}", e);
        process::exit(1);
    });

    let committed_log_path = output_dir.join("committed_orders.log");
    let summary_path = output_dir.join("workload_summary.json");

    let connstr = format!(
        "host={} port={} dbname={} user={} connect_timeout=10",
        host, port, db, user
    );

    // Connect with retry
    eprintln!("[order_writer] Connecting to {}:{}/{}...", host, port, db);
    let mut client = match connect_with_retry(&connstr, 30) {
        Some(c) => c,
        None => {
            eprintln!("[order_writer] FATAL: Could not connect after 30 attempts");
            process::exit(1);
        }
    };
    eprintln!("[order_writer] Connected.");

    // Open committed log for append (line-buffered via BufWriter)
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&committed_log_path)
        .unwrap_or_else(|e| {
            eprintln!("[order_writer] Cannot open {}: {}", committed_log_path.display(), e);
            process::exit(1);
        });
    let mut log_writer = BufWriter::new(log_file);

    let start_time = Utc::now();
    let timer = Instant::now();

    let mut committed: u64 = 0;
    let mut failed: u64 = 0;
    let mut last_committed_id: u64 = 0;

    eprintln!(
        "[order_writer] Writing {} orders (order_id {} to {})...",
        order_count,
        start_id,
        start_id + order_count - 1
    );

    for i in 0..order_count {
        let order_id = start_id + i;
        let payload = format!("order-{:08}", order_id);

        // Each order is an individual transaction:
        //   BEGIN → INSERT → COMMIT
        // order_id is logged ONLY after COMMIT succeeds.
        let result = (|| -> Result<(), postgres::Error> {
            let mut txn = client.transaction()?;
            txn.execute(
                "INSERT INTO orders (order_id, payload) VALUES ($1, $2)",
                &[&(order_id as i64), &payload],
            )?;
            txn.commit()?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                // COMMIT confirmed by server — safe to log
                writeln!(log_writer, "{}", order_id).ok();
                log_writer.flush().ok();
                committed += 1;
                last_committed_id = order_id;
            }
            Err(e) => {
                eprintln!("[order_writer] order_id={} FAILED: {}", order_id, e);
                failed += 1;

                // Reconnect if connection is broken
                if client.is_valid(Duration::from_secs(2)).is_err() {
                    eprintln!("[order_writer] Reconnecting...");
                    match connect_with_retry(&connstr, 10) {
                        Some(c) => client = c,
                        None => {
                            eprintln!("[order_writer] Cannot reconnect. Stopping.");
                            break;
                        }
                    }
                }
            }
        }

        // Progress every 1000 orders
        if (i + 1) % 1000 == 0 || i + 1 == order_count {
            eprintln!(
                "[order_writer] Progress: {}/{} (committed={}, failed={})",
                i + 1,
                order_count,
                committed,
                failed
            );
        }
    }

    let elapsed = timer.elapsed();
    let end_time = Utc::now();

    // Write workload summary
    let summary = WorkloadSummary {
        start_time: start_time.to_rfc3339(),
        end_time: end_time.to_rfc3339(),
        duration_ms: elapsed.as_millis(),
        orders_attempted: order_count,
        orders_committed: committed,
        orders_failed: failed,
        first_order_id: start_id,
        last_committed_order_id: last_committed_id,
        host: host.clone(),
        port,
        database: db.clone(),
    };

    let summary_json = serde_json::to_string_pretty(&summary).unwrap();
    fs::write(&summary_path, &summary_json).unwrap_or_else(|e| {
        eprintln!("[order_writer] Cannot write summary: {}", e);
    });

    eprintln!("[order_writer] Done. {} committed, {} failed.", committed, failed);
    eprintln!("[order_writer] Log: {}", committed_log_path.display());
    eprintln!("[order_writer] Summary: {}", summary_path.display());

    // Print summary to stdout for scripts to capture
    println!("{}", summary_json);
}
