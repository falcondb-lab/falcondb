//! FalconDB PoC #6 — Cost Efficiency: Rate-Limited OLTP Workload Generator
//!
//! Generates a fixed-rate OLTP workload against any PostgreSQL-compatible database.
//! Records throughput, latency distribution, and errors.
//!
//! Usage:
//!   oltp_writer --host 127.0.0.1 --port 5433 --user falcon --db bench \
//!               --rate 1000 --duration 60 --output results.json

use chrono::Utc;
use clap::Parser;
use postgres::{Client, NoTls};
use rand::Rng;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "oltp_writer", about = "Rate-limited OLTP workload generator")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value = "5433")]
    port: u16,
    #[arg(long, default_value = "falcon")]
    user: String,
    #[arg(long, default_value = "bench")]
    db: String,
    #[arg(long, default_value = "")]
    password: String,
    /// Target transactions per second (0 = unlimited)
    #[arg(long, default_value = "1000")]
    rate: u64,
    /// Duration in seconds
    #[arg(long, default_value = "60")]
    duration: u64,
    /// Output JSON file path
    #[arg(long, default_value = "output/workload_results.json")]
    output: String,
    /// Number of concurrent worker threads
    #[arg(long, default_value = "4")]
    threads: usize,
    /// Label for this run (e.g. "falcon" or "postgres")
    #[arg(long, default_value = "unknown")]
    label: String,
}

#[derive(Serialize)]
struct WorkloadResult {
    label: String,
    host: String,
    port: u16,
    target_rate: u64,
    actual_rate: f64,
    duration_sec: u64,
    total_committed: u64,
    total_errors: u64,
    latency_min_us: u64,
    latency_max_us: u64,
    latency_avg_us: u64,
    latency_p50_us: u64,
    latency_p95_us: u64,
    latency_p99_us: u64,
    timestamp: String,
}

fn setup_schema(client: &mut Client) {
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS bench_accounts (
                id      BIGINT PRIMARY KEY,
                balance BIGINT NOT NULL DEFAULT 0,
                name    TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMP DEFAULT NOW()
            );",
        )
        .expect("failed to create bench_accounts table");

    // Seed 10,000 accounts if empty
    let row = client
        .query_one("SELECT COUNT(*) AS c FROM bench_accounts;", &[])
        .expect("count failed");
    let count: i64 = row.get("c");
    if count == 0 {
        eprintln!("  Seeding 10,000 accounts...");
        for batch_start in (1..=10_000i64).step_by(500) {
            let batch_end = std::cmp::min(batch_start + 499, 10_000);
            let mut sql = String::from("INSERT INTO bench_accounts (id, balance, name) VALUES ");
            for id in batch_start..=batch_end {
                if id > batch_start {
                    sql.push(',');
                }
                sql.push_str(&format!("({}, 10000, 'account_{}')", id, id));
            }
            sql.push(';');
            client.batch_execute(&sql).expect("seed insert failed");
        }
        eprintln!("  Seeded.");
    }
}

fn worker(
    args: &Args,
    per_thread_rate: u64,
    committed: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    latencies: Arc<std::sync::Mutex<Vec<u64>>>,
) {
    let conn_str = if args.password.is_empty() {
        format!(
            "host={} port={} user={} dbname={}",
            args.host, args.port, args.user, args.db
        )
    } else {
        format!(
            "host={} port={} user={} dbname={} password={}",
            args.host, args.port, args.user, args.db, args.password
        )
    };

    let mut client = match Client::connect(&conn_str, NoTls) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("  Worker connection failed: {}", e);
            return;
        }
    };

    let mut rng = rand::thread_rng();
    let interval = if per_thread_rate > 0 {
        Duration::from_micros(1_000_000 / per_thread_rate)
    } else {
        Duration::ZERO
    };

    let mut local_latencies: Vec<u64> = Vec::with_capacity(65536);

    while !stop.load(Ordering::Relaxed) {
        let tx_start = Instant::now();

        let from_id: i64 = rng.gen_range(1..=10_000);
        let to_id: i64 = rng.gen_range(1..=10_000);
        let amount: i64 = rng.gen_range(1..=100);

        let result = (|| -> Result<(), postgres::Error> {
            let mut txn = client.transaction()?;
            txn.execute(
                "UPDATE bench_accounts SET balance = balance - $1, updated_at = NOW() WHERE id = $2;",
                &[&amount, &from_id],
            )?;
            txn.execute(
                "UPDATE bench_accounts SET balance = balance + $1, updated_at = NOW() WHERE id = $2;",
                &[&amount, &to_id],
            )?;
            txn.commit()?;
            Ok(())
        })();

        let elapsed_us = tx_start.elapsed().as_micros() as u64;

        match result {
            Ok(_) => {
                committed.fetch_add(1, Ordering::Relaxed);
                local_latencies.push(elapsed_us);
            }
            Err(_) => {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Rate limiting
        if interval > Duration::ZERO {
            let spent = tx_start.elapsed();
            if spent < interval {
                std::thread::sleep(interval - spent);
            }
        }
    }

    // Merge local latencies
    let mut global = latencies.lock().unwrap();
    global.extend_from_slice(&local_latencies);
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p / 100.0).ceil() as usize;
    sorted[idx.saturating_sub(1).min(sorted.len() - 1)]
}

fn main() {
    let args = Args::parse();

    eprintln!(
        "\n  OLTP Writer — {}:{}/{} (label: {})",
        args.host, args.port, args.db, args.label
    );
    eprintln!(
        "  Target: {} tx/s × {}s = {} tx, {} threads\n",
        args.rate,
        args.duration,
        args.rate * args.duration,
        args.threads
    );

    // Setup schema on one connection
    {
        let conn_str = if args.password.is_empty() {
            format!(
                "host={} port={} user={} dbname={}",
                args.host, args.port, args.user, args.db
            )
        } else {
            format!(
                "host={} port={} user={} dbname={} password={}",
                args.host, args.port, args.user, args.db, args.password
            )
        };
        let mut client = Client::connect(&conn_str, NoTls).expect("setup connection failed");
        setup_schema(&mut client);
    }

    let committed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let latencies: Arc<std::sync::Mutex<Vec<u64>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(
            (args.rate * args.duration) as usize,
        )));

    let per_thread_rate = if args.rate > 0 {
        args.rate / args.threads as u64
    } else {
        0
    };

    let wall_start = Instant::now();

    let handles: Vec<_> = (0..args.threads)
        .map(|_| {
            let a_host = args.host.clone();
            let a_user = args.user.clone();
            let a_db = args.db.clone();
            let a_pw = args.password.clone();
            let c = committed.clone();
            let e = errors.clone();
            let s = stop.clone();
            let l = latencies.clone();
            let port = args.port;
            let label = args.label.clone();
            std::thread::spawn(move || {
                let local_args = Args {
                    host: a_host,
                    port,
                    user: a_user,
                    db: a_db,
                    password: a_pw,
                    rate: per_thread_rate * (args.threads as u64), // unused in worker
                    duration: args.duration,
                    output: String::new(),
                    threads: args.threads,
                    label,
                };
                worker(&local_args, per_thread_rate, c, e, s, l);
            })
        })
        .collect();

    // Progress reporting
    let progress_stop = stop.clone();
    let progress_committed = committed.clone();
    let progress_errors = errors.clone();
    let progress_handle = std::thread::spawn(move || {
        let mut last = 0u64;
        for sec in 1..=args.duration {
            std::thread::sleep(Duration::from_secs(1));
            if progress_stop.load(Ordering::Relaxed) {
                break;
            }
            let current = progress_committed.load(Ordering::Relaxed);
            let errs = progress_errors.load(Ordering::Relaxed);
            let delta = current - last;
            last = current;
            if sec % 10 == 0 || sec == args.duration {
                eprintln!(
                    "  [{:>3}s] committed={} (+{}/s) errors={}",
                    sec, current, delta, errs
                );
            }
        }
    });

    std::thread::sleep(Duration::from_secs(args.duration));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        let _ = h.join();
    }
    let _ = progress_handle.join();

    let wall_elapsed = wall_start.elapsed();
    let total_committed = committed.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);
    let actual_rate = total_committed as f64 / wall_elapsed.as_secs_f64();

    // Compute latency stats
    let mut lats = latencies.lock().unwrap();
    lats.sort_unstable();

    let (min, max, avg, p50, p95, p99) = if lats.is_empty() {
        (0, 0, 0, 0, 0, 0)
    } else {
        let sum: u64 = lats.iter().sum();
        (
            lats[0],
            lats[lats.len() - 1],
            sum / lats.len() as u64,
            percentile(&lats, 50.0),
            percentile(&lats, 95.0),
            percentile(&lats, 99.0),
        )
    };

    let result = WorkloadResult {
        label: args.label.clone(),
        host: args.host.clone(),
        port: args.port,
        target_rate: args.rate,
        actual_rate,
        duration_sec: args.duration,
        total_committed,
        total_errors,
        latency_min_us: min,
        latency_max_us: max,
        latency_avg_us: avg,
        latency_p50_us: p50,
        latency_p95_us: p95,
        latency_p99_us: p99,
        timestamp: Utc::now().to_rfc3339(),
    };

    // Write JSON
    if !args.output.is_empty() {
        if let Some(parent) = std::path::Path::new(&args.output).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let json = serde_json::to_string_pretty(&result).expect("JSON serialize failed");
        std::fs::write(&args.output, &json).expect("write output failed");
        eprintln!("\n  Results written to {}", args.output);
    }

    eprintln!("\n  === {} Summary ===", args.label);
    eprintln!("  Committed:  {}", total_committed);
    eprintln!("  Errors:     {}", total_errors);
    eprintln!("  Actual rate: {:.0} tx/s", actual_rate);
    eprintln!("  Latency avg: {} µs", avg);
    eprintln!("  Latency p50: {} µs", p50);
    eprintln!("  Latency p95: {} µs", p95);
    eprintln!("  Latency p99: {} µs\n", p99);
}
