#!/usr/bin/env python3
"""
FalconDB DCG PoC — Deterministic Order Writer (Python fallback)

Connects to a FalconDB (or PostgreSQL-compatible) server and writes
monotonically increasing orders. Each order_id is logged to
committed_orders.log ONLY after the server confirms COMMIT.

Usage:
    python3 order_writer.py

Environment variables:
    FALCON_HOST       — database host (default: 127.0.0.1)
    FALCON_PORT       — database port (default: 5433)
    FALCON_DB         — database name (default: falcon)
    FALCON_USER       — database user (default: falcon)
    ORDER_COUNT       — number of orders to write (default: 1000)
    OUTPUT_DIR        — directory for output files (default: ./output)
    START_ORDER_ID    — first order_id (default: 1)

Requirements:
    pip install psycopg2-binary
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)


def env_or(key: str, default: str) -> str:
    return os.environ.get(key, default)


def connect_with_retry(connstr: str, max_attempts: int = 30):
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(connstr)
            conn.autocommit = False
            return conn
        except psycopg2.Error as e:
            print(f"[order_writer] Connection attempt {attempt}/{max_attempts} failed: {e}",
                  file=sys.stderr)
            if attempt < max_attempts:
                time.sleep(1)
    return None


def main():
    host = env_or("FALCON_HOST", "127.0.0.1")
    port = int(env_or("FALCON_PORT", "5433"))
    db = env_or("FALCON_DB", "falcon")
    user = env_or("FALCON_USER", "falcon")
    order_count = int(env_or("ORDER_COUNT", "1000"))
    output_dir = Path(env_or("OUTPUT_DIR", "./output"))
    start_id = int(env_or("START_ORDER_ID", "1"))

    output_dir.mkdir(parents=True, exist_ok=True)

    committed_log_path = output_dir / "committed_orders.log"
    summary_path = output_dir / "workload_summary.json"

    connstr = f"host={host} port={port} dbname={db} user={user} connect_timeout=10"

    print(f"[order_writer] Connecting to {host}:{port}/{db}...", file=sys.stderr)
    conn = connect_with_retry(connstr)
    if conn is None:
        print("[order_writer] FATAL: Could not connect after 30 attempts", file=sys.stderr)
        sys.exit(1)
    print("[order_writer] Connected.", file=sys.stderr)

    start_time = datetime.now(timezone.utc)
    timer_start = time.monotonic()

    committed = 0
    failed = 0
    last_committed_id = 0

    print(f"[order_writer] Writing {order_count} orders "
          f"(order_id {start_id} to {start_id + order_count - 1})...", file=sys.stderr)

    with open(committed_log_path, "a") as log_file:
        for i in range(order_count):
            order_id = start_id + i
            payload = f"order-{order_id:08d}"

            try:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO orders (order_id, payload) VALUES (%s, %s)",
                    (order_id, payload),
                )
                conn.commit()

                # COMMIT confirmed by server — safe to log
                log_file.write(f"{order_id}\n")
                log_file.flush()
                committed += 1
                last_committed_id = order_id

            except psycopg2.Error as e:
                print(f"[order_writer] order_id={order_id} FAILED: {e}", file=sys.stderr)
                failed += 1
                try:
                    conn.rollback()
                except Exception:
                    pass

                # Reconnect if connection is broken
                try:
                    conn.close()
                except Exception:
                    pass
                print("[order_writer] Reconnecting...", file=sys.stderr)
                conn = connect_with_retry(connstr, 10)
                if conn is None:
                    print("[order_writer] Cannot reconnect. Stopping.", file=sys.stderr)
                    break

            # Progress every 1000 orders
            if (i + 1) % 1000 == 0 or i + 1 == order_count:
                print(f"[order_writer] Progress: {i + 1}/{order_count} "
                      f"(committed={committed}, failed={failed})", file=sys.stderr)

    elapsed_ms = int((time.monotonic() - timer_start) * 1000)
    end_time = datetime.now(timezone.utc)

    summary = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_ms": elapsed_ms,
        "orders_attempted": order_count,
        "orders_committed": committed,
        "orders_failed": failed,
        "first_order_id": start_id,
        "last_committed_order_id": last_committed_id,
        "host": host,
        "port": port,
        "database": db,
    }

    summary_json = json.dumps(summary, indent=2)
    summary_path.write_text(summary_json)

    print(f"[order_writer] Done. {committed} committed, {failed} failed.", file=sys.stderr)
    print(f"[order_writer] Log: {committed_log_path}", file=sys.stderr)
    print(f"[order_writer] Summary: {summary_path}", file=sys.stderr)

    # Print summary to stdout for scripts to capture
    print(summary_json)

    try:
        conn.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
