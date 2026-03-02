use chrono::Utc;
use postgres::{Client, NoTls};
use serde::Serialize;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

const NODES: &[(&str, u16)] = &[
    ("127.0.0.1", 5433),
    ("127.0.0.1", 5434),
    ("127.0.0.1", 5435),
];

#[derive(Serialize)]
struct Metrics {
    start_time: String,
    end_time: String,
    duration_ms: u128,
    committed: u64,
    failed: u64,
    reconnects: u64,
    last_committed_id: u64,
    last_commit_before_failure: String,
    first_commit_after_reconnect: String,
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn connstr(host: &str, port: u16) -> String {
    format!("host={host} port={port} dbname=falcon user=falcon connect_timeout=3")
}

fn try_connect(host: &str, port: u16) -> Option<Client> {
    Client::connect(&connstr(host, port), NoTls).ok()
}

/// Try all known nodes, return first successful connection.
/// Prefers the leader — writable node accepts INSERT without error.
fn connect_any(max_rounds: u32) -> Option<(Client, String)> {
    for round in 0..max_rounds {
        for &(host, port) in NODES {
            if let Some(c) = try_connect(host, port) {
                let label = format!("{host}:{port}");
                return Some((c, label));
            }
        }
        if round < max_rounds - 1 {
            thread::sleep(Duration::from_millis(500));
        }
    }
    None
}

fn ensure_table(client: &mut Client) {
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS tx_commits (
                commit_id  BIGINT PRIMARY KEY,
                payload    TEXT NOT NULL,
                ts         TIMESTAMP DEFAULT now()
            );",
        )
        .unwrap_or_else(|e| {
            eprintln!("[writer] WARN: CREATE TABLE: {e}");
        });
}

fn main() {
    let total: u64 = env_or("COMMIT_COUNT", "20000").parse().unwrap_or(20000);
    let output_dir = PathBuf::from(env_or("OUTPUT_DIR", "./output"));
    let stop_file = output_dir.join("stop_writer");

    fs::create_dir_all(&output_dir).expect("cannot create output dir");

    let committed_path = output_dir.join("committed.log");
    let metrics_path = output_dir.join("writer_metrics.json");

    // Initial connect
    eprintln!("[writer] Connecting to cluster...");
    let (mut client, mut node_label) = connect_any(60).expect("cannot connect to any node");
    eprintln!("[writer] Connected to {node_label}");

    ensure_table(&mut client);

    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&committed_path)
        .expect("cannot open committed.log");
    let mut log = BufWriter::new(log_file);

    // Write PID for demo.sh
    fs::write(output_dir.join("writer.pid"), std::process::id().to_string()).ok();

    let start = Instant::now();
    let start_ts = Utc::now();

    let mut committed: u64 = 0;
    let mut failed: u64 = 0;
    let mut reconnects: u64 = 0;
    let mut last_id: u64 = 0;
    let mut pre_failure_ts = String::new();
    let mut post_reconnect_ts = String::new();
    let mut just_reconnected = false;

    let mut i: u64 = 0;
    while i < total {
        if stop_file.exists() {
            eprintln!("[writer] Stop file detected, exiting.");
            break;
        }

        let cid = i + 1;
        let payload = format!("txn-{cid:08}");

        let ok = (|| -> Result<(), postgres::Error> {
            let txn = client.transaction()?;
            txn.execute(
                "INSERT INTO tx_commits (commit_id, payload) VALUES ($1, $2)",
                &[&(cid as i64), &payload],
            )?;
            txn.commit()?;
            Ok(())
        })();

        match ok {
            Ok(()) => {
                let ts = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
                writeln!(log, "{cid}|{ts}|{node_label}").ok();
                log.flush().ok();

                committed += 1;
                last_id = cid;
                i += 1;

                if just_reconnected {
                    post_reconnect_ts = ts.clone();
                    just_reconnected = false;
                    eprintln!("[writer] First commit after reconnect: id={cid} on {node_label}");
                }

                if committed % 2000 == 0 {
                    let rate = committed as f64 / start.elapsed().as_secs_f64();
                    eprintln!("[writer] {committed}/{total} committed ({rate:.0} tx/s)");
                }
            }
            Err(e) => {
                failed += 1;
                eprintln!("[writer] id={cid} FAILED: {e}");

                if pre_failure_ts.is_empty() && committed > 0 {
                    pre_failure_ts = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
                }

                eprintln!("[writer] Reconnecting to cluster...");
                match connect_any(40) {
                    Some((c, label)) => {
                        client = c;
                        node_label = label;
                        reconnects += 1;
                        just_reconnected = true;
                        eprintln!("[writer] Reconnected to {node_label} (reconnect #{reconnects})");
                        // Do NOT increment i — retry same commit_id
                    }
                    None => {
                        eprintln!("[writer] FATAL: Cannot reach any node. Aborting.");
                        break;
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();

    let metrics = Metrics {
        start_time: start_ts.to_rfc3339(),
        end_time: Utc::now().to_rfc3339(),
        duration_ms: elapsed.as_millis(),
        committed,
        failed,
        reconnects,
        last_committed_id: last_id,
        last_commit_before_failure: if pre_failure_ts.is_empty() { "N/A".into() } else { pre_failure_ts },
        first_commit_after_reconnect: if post_reconnect_ts.is_empty() { "N/A".into() } else { post_reconnect_ts },
    };

    let json = serde_json::to_string_pretty(&metrics).unwrap();
    fs::write(&metrics_path, &json).ok();

    eprintln!("[writer] Done. committed={committed} failed={failed} reconnects={reconnects}");
    eprintln!("[writer] Log: {}", committed_path.display());

    // Remove PID file
    fs::remove_file(output_dir.join("writer.pid")).ok();
}
