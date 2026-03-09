"""FalconDB PoC #3 — Deterministic Commit Marker Writer (Python fallback)

Writes monotonically increasing tx_markers. Each marker_id is logged to
committed_markers.log ONLY after COMMIT confirmed. Reconnects to failover
endpoint on connection loss.

Environment variables: same as the Rust version (see workload/src/main.rs).
"""

import os
import sys
import json
import time
from datetime import datetime, timezone

try:
    import psycopg2
except ImportError:
    print("[tx_marker_writer] psycopg2 not installed. pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)


def env_or(key, default):
    return os.environ.get(key, default)


def try_connect(host, port, db, user):
    try:
        conn = psycopg2.connect(host=host, port=int(port), dbname=db, user=user, connect_timeout=5)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"[tx_marker_writer] Connect failed: {e}", file=sys.stderr)
        return None


def connect_with_retry(host, port, db, user, max_attempts=30):
    for attempt in range(1, max_attempts + 1):
        conn = try_connect(host, port, db, user)
        if conn:
            return conn
        if attempt < max_attempts:
            time.sleep(0.5)
    return None


def main():
    primary_host = env_or("FALCON_HOST", "127.0.0.1")
    primary_port = env_or("FALCON_PORT", "5433")
    failover_host = env_or("FAILOVER_HOST", "127.0.0.1")
    failover_port = env_or("FAILOVER_PORT", "5434")
    db = env_or("FALCON_DB", "falcon")
    user = env_or("FALCON_USER", "falcon")
    marker_count = int(env_or("MARKER_COUNT", "50000"))
    output_dir = env_or("OUTPUT_DIR", "./output")
    start_id = int(env_or("START_MARKER_ID", "1"))
    stop_file = env_or("STOP_FILE", "")

    os.makedirs(output_dir, exist_ok=True)
    committed_log_path = os.path.join(output_dir, "committed_markers.log")
    metrics_path = os.path.join(output_dir, "load_metrics.json")

    print(f"[tx_marker_writer] Connecting to primary {primary_host}:{primary_port}...", file=sys.stderr)
    conn = connect_with_retry(primary_host, primary_port, db, user)
    if not conn:
        print("[tx_marker_writer] FATAL: Could not connect to primary", file=sys.stderr)
        sys.exit(1)
    print("[tx_marker_writer] Connected to primary.", file=sys.stderr)

    current_label = f"{primary_host}:{primary_port}"
    log_file = open(committed_log_path, "a", buffering=1)

    start_time = datetime.now(timezone.utc)
    t0 = time.monotonic()

    committed = 0
    failed = 0
    reconnect_count = 0
    last_committed_id = 0
    last_commit_before_failure_ts = ""
    first_commit_after_reconnect_ts = ""
    just_reconnected = False

    print(f"[tx_marker_writer] Writing {marker_count} markers (id {start_id} to {start_id + marker_count - 1})...", file=sys.stderr)

    i = 0
    while i < marker_count:
        if stop_file and os.path.exists(stop_file):
            print("[tx_marker_writer] Stop file detected. Stopping gracefully.", file=sys.stderr)
            break

        marker_id = start_id + i
        source_node = current_label

        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO tx_markers (marker_id, source_node) VALUES (%s, %s)",
                        (marker_id, source_node))
            conn.commit()
            cur.close()

            ts = datetime.now(timezone.utc).isoformat()
            log_file.write(f"{marker_id}|{ts}\n")
            log_file.flush()
            committed += 1
            last_committed_id = marker_id

            if just_reconnected:
                first_commit_after_reconnect_ts = ts
                just_reconnected = False
                print(f"[tx_marker_writer] First commit after reconnect: marker_id={marker_id}", file=sys.stderr)

            i += 1

        except Exception as e:
            print(f"[tx_marker_writer] marker_id={marker_id} FAILED: {e}", file=sys.stderr)
            failed += 1

            if not last_commit_before_failure_ts and committed > 0:
                last_commit_before_failure_ts = datetime.now(timezone.utc).isoformat()

            try:
                conn.close()
            except Exception:
                pass

            print("[tx_marker_writer] Connection lost. Attempting reconnect...", file=sys.stderr)
            reconnected = False

            # Try failover first (primary is likely dead)
            print(f"[tx_marker_writer] Trying failover endpoint {failover_host}:{failover_port}...", file=sys.stderr)
            conn = connect_with_retry(failover_host, failover_port, db, user, max_attempts=20)
            if conn:
                current_label = f"{failover_host}:{failover_port}"
                reconnected = True
                print(f"[tx_marker_writer] Reconnected to failover {current_label}", file=sys.stderr)

            if not reconnected:
                print(f"[tx_marker_writer] Trying original primary {primary_host}:{primary_port}...", file=sys.stderr)
                conn = connect_with_retry(primary_host, primary_port, db, user, max_attempts=10)
                if conn:
                    current_label = f"{primary_host}:{primary_port}"
                    reconnected = True

            if reconnected:
                reconnect_count += 1
                just_reconnected = True
            else:
                print("[tx_marker_writer] Cannot reconnect to any endpoint. Stopping.", file=sys.stderr)
                break

        if committed > 0 and committed % 5000 == 0:
            print(f"[tx_marker_writer] Progress: committed={committed}, failed={failed}, reconnects={reconnect_count}", file=sys.stderr)

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    end_time = datetime.now(timezone.utc)
    log_file.close()

    metrics = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_ms": elapsed_ms,
        "markers_attempted": marker_count,
        "markers_committed": committed,
        "markers_failed": failed,
        "first_marker_id": start_id,
        "last_committed_marker_id": last_committed_id,
        "reconnect_count": reconnect_count,
        "last_commit_before_failure_ts": last_commit_before_failure_ts or "N/A",
        "first_commit_after_reconnect_ts": first_commit_after_reconnect_ts or "N/A",
        "primary_host": primary_host,
        "primary_port": int(primary_port),
        "failover_host": failover_host,
        "failover_port": int(failover_port),
    }

    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"[tx_marker_writer] Done. {committed} committed, {failed} failed, {reconnect_count} reconnects.", file=sys.stderr)
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
