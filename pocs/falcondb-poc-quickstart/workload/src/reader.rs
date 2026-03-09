use postgres::{Client, NoTls};
use std::env;
use std::thread;
use std::time::Duration;

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn try_connect(host: &str, port: u16) -> Option<Client> {
    let cs = format!("host={host} port={port} dbname=falcon user=falcon connect_timeout=3");
    Client::connect(&cs, NoTls).ok()
}

fn connect_survivor() -> Client {
    let ports: Vec<u16> = vec![5433, 5434, 5435];
    loop {
        for port in &ports {
            if let Some(c) = try_connect("127.0.0.1", *port) {
                eprintln!("[reader] Connected to 127.0.0.1:{port}");
                return c;
            }
        }
        eprintln!("[reader] No node reachable, retrying...");
        thread::sleep(Duration::from_secs(1));
    }
}

fn main() {
    let expected: u64 = env_or("EXPECTED_COUNT", "0").parse().unwrap_or(0);

    let mut client = connect_survivor();

    let row = client
        .query_one("SELECT COUNT(*) FROM tx_commits", &[])
        .expect("query failed");
    let count: i64 = row.get(0);

    println!("Rows in tx_commits: {count}");

    if expected > 0 {
        let missing = expected as i64 - count;
        if missing == 0 {
            println!("PASS: all {expected} committed rows present.");
        } else {
            println!("FAIL: expected {expected}, found {count}, missing {missing}");
            std::process::exit(1);
        }
    }

    // Show first/last few rows
    let rows = client
        .query(
            "SELECT commit_id, payload, ts FROM tx_commits ORDER BY commit_id LIMIT 5",
            &[],
        )
        .unwrap_or_default();
    println!("\nFirst rows:");
    for r in &rows {
        let id: i64 = r.get(0);
        let payload: String = r.get(1);
        println!("  {id} | {payload}");
    }

    let rows = client
        .query(
            "SELECT commit_id, payload, ts FROM tx_commits ORDER BY commit_id DESC LIMIT 5",
            &[],
        )
        .unwrap_or_default();
    println!("\nLast rows:");
    for r in rows.iter().rev() {
        let id: i64 = r.get(0);
        let payload: String = r.get(1);
        println!("  {id} | {payload}");
    }
}
