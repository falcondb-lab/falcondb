#!/usr/bin/env python3
"""Rate-limited OLTP workload generator (Python fallback for oltp_writer.exe)"""

import argparse, json, os, random, sys, threading, time
from datetime import datetime, timezone

try:
    import psycopg2
except ImportError:
    print("ERROR: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)


def setup_schema(host, port, user, db):
    conn = psycopg2.connect(host=host, port=port, user=user, dbname=db)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS bench_accounts (
        id BIGINT PRIMARY KEY, balance BIGINT NOT NULL DEFAULT 0,
        name TEXT NOT NULL DEFAULT '', updated_at TIMESTAMP DEFAULT NOW())""")
    cur.execute("SELECT COUNT(*) FROM bench_accounts")
    cnt = cur.fetchone()[0]
    if cnt == 0:
        print("  Seeding 10,000 accounts...", file=sys.stderr)
        for start in range(1, 10001, 500):
            end = min(start + 499, 10000)
            vals = ",".join(f"({i},10000,'account_{i}')" for i in range(start, end + 1))
            cur.execute(f"INSERT INTO bench_accounts (id, balance, name) VALUES {vals}")
        print("  Seeded.", file=sys.stderr)
    conn.close()


def worker(host, port, user, db, per_thread_rate, stop_event, committed, errors, latencies, lock):
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, dbname=db)
        conn.autocommit = False
    except Exception as e:
        print(f"  Worker connection failed: {e}", file=sys.stderr)
        return

    interval = 1.0 / per_thread_rate if per_thread_rate > 0 else 0
    local_lats = []

    while not stop_event.is_set():
        t0 = time.perf_counter()
        from_id = random.randint(1, 10000)
        to_id = random.randint(1, 10000)
        amount = random.randint(1, 100)
        try:
            cur = conn.cursor()
            cur.execute("UPDATE bench_accounts SET balance = balance - %s, updated_at = NOW() WHERE id = %s", (amount, from_id))
            cur.execute("UPDATE bench_accounts SET balance = balance + %s, updated_at = NOW() WHERE id = %s", (amount, to_id))
            conn.commit()
            elapsed_us = int((time.perf_counter() - t0) * 1_000_000)
            local_lats.append(elapsed_us)
            with lock:
                committed[0] += 1
        except Exception:
            with lock:
                errors[0] += 1
            try:
                conn.rollback()
            except:
                pass

        if interval > 0:
            spent = time.perf_counter() - t0
            if spent < interval:
                time.sleep(interval - spent)

    with lock:
        latencies.extend(local_lats)
    try:
        conn.close()
    except:
        pass


def percentile(sorted_lats, p):
    if not sorted_lats:
        return 0
    idx = max(0, min(int(len(sorted_lats) * p / 100.0 + 0.5) - 1, len(sorted_lats) - 1))
    return sorted_lats[idx]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5433)
    ap.add_argument("--user", default="falcon")
    ap.add_argument("--db", default="bench")
    ap.add_argument("--rate", type=int, default=1000)
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--label", default="unknown")
    ap.add_argument("--output", default="output/workload_results.json")
    args = ap.parse_args()

    print(f"\n  OLTP Writer — {args.host}:{args.port}/{args.db} (label: {args.label})", file=sys.stderr)
    print(f"  Target: {args.rate} tx/s × {args.duration}s = {args.rate * args.duration} tx, {args.threads} threads\n", file=sys.stderr)

    setup_schema(args.host, args.port, args.user, args.db)

    stop = threading.Event()
    committed = [0]
    errors = [0]
    latencies = []
    lock = threading.Lock()
    per_thread_rate = args.rate // args.threads if args.rate > 0 else 0

    wall_start = time.perf_counter()
    threads = []
    for _ in range(args.threads):
        t = threading.Thread(target=worker, args=(args.host, args.port, args.user, args.db, per_thread_rate, stop, committed, errors, latencies, lock))
        t.start()
        threads.append(t)

    # Progress
    last = 0
    for sec in range(1, args.duration + 1):
        time.sleep(1)
        with lock:
            cur = committed[0]
            errs = errors[0]
        delta = cur - last
        last = cur
        if sec % 5 == 0 or sec == args.duration:
            print(f"  [{sec:>3}s] committed={cur} (+{delta}/s) errors={errs}", file=sys.stderr)

    stop.set()
    for t in threads:
        t.join()

    wall_elapsed = time.perf_counter() - wall_start
    total_c = committed[0]
    total_e = errors[0]
    actual_rate = total_c / wall_elapsed if wall_elapsed > 0 else 0

    latencies.sort()
    if latencies:
        lat_min, lat_max = latencies[0], latencies[-1]
        lat_avg = sum(latencies) // len(latencies)
    else:
        lat_min = lat_max = lat_avg = 0

    result = {
        "label": args.label,
        "host": args.host,
        "port": args.port,
        "target_rate": args.rate,
        "actual_rate": round(actual_rate, 1),
        "duration_sec": args.duration,
        "total_committed": total_c,
        "total_errors": total_e,
        "latency_min_us": lat_min,
        "latency_max_us": lat_max,
        "latency_avg_us": lat_avg,
        "latency_p50_us": percentile(latencies, 50),
        "latency_p95_us": percentile(latencies, 95),
        "latency_p99_us": percentile(latencies, 99),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\n  Results written to {args.output}", file=sys.stderr)

    print(f"\n  === {args.label} Summary ===", file=sys.stderr)
    print(f"  Committed:  {total_c}", file=sys.stderr)
    print(f"  Errors:     {total_e}", file=sys.stderr)
    print(f"  Actual rate: {actual_rate:.0f} tx/s", file=sys.stderr)
    print(f"  Latency avg: {lat_avg} µs", file=sys.stderr)
    print(f"  Latency p50: {percentile(latencies, 50)} µs", file=sys.stderr)
    print(f"  Latency p95: {percentile(latencies, 95)} µs", file=sys.stderr)
    print(f"  Latency p99: {percentile(latencies, 99)} µs\n", file=sys.stderr)


if __name__ == "__main__":
    main()
