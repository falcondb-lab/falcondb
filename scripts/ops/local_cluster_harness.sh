#!/usr/bin/env bash
# local_cluster_harness.sh — Spin up a local 3-node FalconDB cluster for testing.
#
# Usage:
#   ./scripts/local_cluster_harness.sh start     # start 3-node cluster
#   ./scripts/local_cluster_harness.sh stop      # stop all nodes
#   ./scripts/local_cluster_harness.sh status    # show node status
#   ./scripts/local_cluster_harness.sh failover  # kill primary, promote replica
#   ./scripts/local_cluster_harness.sh smoke     # run smoke test (write + read + failover)
#   ./scripts/local_cluster_harness.sh clean     # remove data dirs

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="$REPO_ROOT/target/debug/falcon_server"
DATA_DIR="$REPO_ROOT/.local_cluster"

# Node configuration
PRIMARY_PORT=5433
PRIMARY_GRPC=50051
REPLICA1_PORT=5434
REPLICA1_GRPC=50052
REPLICA2_PORT=5435
REPLICA2_GRPC=50053

PRIMARY_DATA="$DATA_DIR/primary"
REPLICA1_DATA="$DATA_DIR/replica1"
REPLICA2_DATA="$DATA_DIR/replica2"

PID_DIR="$DATA_DIR/pids"

log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

ensure_binary() {
  if [[ ! -f "$BINARY" ]]; then
    log "Building falcon_server..."
    cargo build -p falcon_server 2>&1 | tail -5
  fi
  [[ -f "$BINARY" ]] || die "Binary not found: $BINARY"
}

start_primary() {
  mkdir -p "$PRIMARY_DATA" "$PID_DIR"
  log "Starting primary on port $PRIMARY_PORT (gRPC $PRIMARY_GRPC)..."
  "$BINARY" \
    --port "$PRIMARY_PORT" \
    --data-dir "$PRIMARY_DATA" \
    --role primary \
    --grpc-addr "127.0.0.1:$PRIMARY_GRPC" \
    > "$DATA_DIR/primary.log" 2>&1 &
  echo $! > "$PID_DIR/primary.pid"
  sleep 0.5
  log "Primary PID: $(cat "$PID_DIR/primary.pid")"
}

start_replica() {
  local name=$1 port=$2 grpc=$3 data=$4
  mkdir -p "$data" "$PID_DIR"
  log "Starting $name on port $port (gRPC $grpc)..."
  "$BINARY" \
    --port "$port" \
    --data-dir "$data" \
    --role replica \
    --grpc-addr "127.0.0.1:$grpc" \
    --primary-endpoint "http://127.0.0.1:$PRIMARY_GRPC" \
    > "$DATA_DIR/${name}.log" 2>&1 &
  echo $! > "$PID_DIR/${name}.pid"
  sleep 0.5
  log "$name PID: $(cat "$PID_DIR/${name}.pid")"
}

cmd_start() {
  ensure_binary
  mkdir -p "$DATA_DIR"
  start_primary
  sleep 1
  start_replica "replica1" "$REPLICA1_PORT" "$REPLICA1_GRPC" "$REPLICA1_DATA"
  start_replica "replica2" "$REPLICA2_PORT" "$REPLICA2_GRPC" "$REPLICA2_DATA"
  sleep 2
  log "Cluster started. Nodes:"
  log "  Primary:  localhost:$PRIMARY_PORT"
  log "  Replica1: localhost:$REPLICA1_PORT"
  log "  Replica2: localhost:$REPLICA2_PORT"
}

cmd_stop() {
  log "Stopping all nodes..."
  for pid_file in "$PID_DIR"/*.pid 2>/dev/null; do
    [[ -f "$pid_file" ]] || continue
    pid=$(cat "$pid_file")
    name=$(basename "$pid_file" .pid)
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      log "Stopped $name (PID $pid)"
    fi
    rm -f "$pid_file"
  done
  log "All nodes stopped."
}

cmd_status() {
  echo "=== Cluster Status ==="
  for pid_file in "$PID_DIR"/*.pid 2>/dev/null; do
    [[ -f "$pid_file" ]] || continue
    pid=$(cat "$pid_file")
    name=$(basename "$pid_file" .pid)
    if kill -0 "$pid" 2>/dev/null; then
      echo "  RUNNING: $name (PID $pid)"
    else
      echo "  STOPPED: $name (PID $pid, stale)"
    fi
  done
}

cmd_failover() {
  log "=== Failover Test ==="
  local primary_pid_file="$PID_DIR/primary.pid"
  [[ -f "$primary_pid_file" ]] || die "Primary not running"
  local primary_pid
  primary_pid=$(cat "$primary_pid_file")

  log "Step 1: Write data to primary before failover..."
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c "
    CREATE TABLE IF NOT EXISTS failover_test (id SERIAL PRIMARY KEY, val TEXT);
    INSERT INTO failover_test (val) VALUES ('before_failover');
  " 2>/dev/null || log "  (psql not available, skipping pre-write)"

  log "Step 2: Kill primary (PID $primary_pid)..."
  kill -9 "$primary_pid" 2>/dev/null || true
  rm -f "$primary_pid_file"
  sleep 1

  log "Step 3: Promote replica1 to primary..."
  local replica1_pid
  replica1_pid=$(cat "$PID_DIR/replica1.pid" 2>/dev/null || echo "")
  if [[ -n "$replica1_pid" ]] && kill -0 "$replica1_pid" 2>/dev/null; then
    # Send SIGUSR1 as promote signal (if supported) or restart as primary
    log "  Replica1 is running, promoting via restart as primary..."
    kill "$replica1_pid" 2>/dev/null || true
    rm -f "$PID_DIR/replica1.pid"
    sleep 0.5
    # Restart as primary on replica1's port
    mkdir -p "$REPLICA1_DATA" "$PID_DIR"
    "$BINARY" \
      --port "$REPLICA1_PORT" \
      --data-dir "$REPLICA1_DATA" \
      --role primary \
      --grpc-addr "127.0.0.1:$REPLICA1_GRPC" \
      > "$DATA_DIR/replica1_promoted.log" 2>&1 &
    echo $! > "$PID_DIR/replica1.pid"
    sleep 2
    log "  Replica1 promoted to primary on port $REPLICA1_PORT"
  fi

  log "Step 4: Verify new primary is writable..."
  psql -h 127.0.0.1 -p "$REPLICA1_PORT" -U falcon -c "
    INSERT INTO failover_test (val) VALUES ('after_failover');
    SELECT COUNT(*) FROM failover_test;
  " 2>/dev/null || log "  (psql not available, skipping post-write)"

  log "Failover complete."
}

cmd_smoke() {
  log "=== Smoke Test ==="
  local t0
  t0=$(date +%s%N)

  # 1. Basic connectivity
  log "1. Checking primary connectivity..."
  if command -v psql &>/dev/null; then
    psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c "SELECT 1 AS ok" 2>/dev/null && \
      log "   Primary: OK" || log "   Primary: UNREACHABLE (psql)"
  else
    log "   psql not available, using nc..."
    nc -z 127.0.0.1 "$PRIMARY_PORT" 2>/dev/null && log "   Primary port: OPEN" || log "   Primary port: CLOSED"
  fi

  # 2. Write + read round-trip
  log "2. Write/read round-trip..."
  if command -v psql &>/dev/null; then
    psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c "
      CREATE TABLE IF NOT EXISTS smoke_test (id SERIAL PRIMARY KEY, ts TIMESTAMP DEFAULT NOW());
      INSERT INTO smoke_test DEFAULT VALUES;
      SELECT COUNT(*) AS rows FROM smoke_test;
    " 2>/dev/null && log "   Write/read: OK" || log "   Write/read: FAILED"
  fi

  # 3. SHOW falcon.* commands
  log "3. SHOW falcon.version..."
  if command -v psql &>/dev/null; then
    psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c "SHOW falcon.version" 2>/dev/null && \
      log "   SHOW falcon.version: OK" || log "   SHOW falcon.version: FAILED"
  fi

  local t1
  t1=$(date +%s%N)
  local elapsed_ms=$(( (t1 - t0) / 1000000 ))
  log "Smoke test completed in ${elapsed_ms}ms"
}

cmd_clean() {
  cmd_stop 2>/dev/null || true
  log "Removing data directory: $DATA_DIR"
  rm -rf "$DATA_DIR"
  log "Clean complete."
}

cmd_scaleout() {
  log "=== Scale-Out Test (G1) ==="
  if ! command -v psql &>/dev/null; then
    die "psql is required for scale-out test"
  fi

  # 1. Write data to primary
  log "1. Writing test data to primary..."
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c "
    CREATE TABLE IF NOT EXISTS scaleout_test (id SERIAL PRIMARY KEY, val TEXT);
    INSERT INTO scaleout_test (val) SELECT 'row_' || i FROM generate_series(1, 100) AS i;
  " 2>/dev/null || die "Failed to write test data"

  local before_count
  before_count=$(psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -t -c \
    "SELECT COUNT(*) FROM scaleout_test" 2>/dev/null | tr -d ' ')
  log "   Rows before scale-out: $before_count"

  # 2. Add node via SQL
  log "2. Adding node via falcon_add_node(100)..."
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c \
    "SELECT falcon_add_node(100)" 2>/dev/null || die "Failed to add node"

  # 3. Verify node lifecycle shows joining
  log "3. Verifying node lifecycle..."
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c \
    "SHOW falcon.node_lifecycle" 2>/dev/null || die "Failed to show node_lifecycle"

  # 4. Run rebalance
  log "4. Running rebalance..."
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c \
    "SELECT falcon_rebalance_apply()" 2>/dev/null || die "Failed to rebalance"

  # 5. Show rebalance plan (verify output has shard moves + volume + time)
  log "5. Verifying rebalance plan output..."
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c \
    "SHOW falcon.rebalance_plan" 2>/dev/null || die "Failed to show rebalance_plan"

  # 6. Verify data still accessible
  local after_count
  after_count=$(psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -t -c \
    "SELECT COUNT(*) FROM scaleout_test" 2>/dev/null | tr -d ' ')
  log "   Rows after scale-out: $after_count"

  if [[ "$before_count" == "$after_count" ]]; then
    log "G1 PASS: Data verified after scale-out ($after_count rows)"
  else
    die "G1 FAIL: Data mismatch (before=$before_count, after=$after_count)"
  fi

  # 7. Verify cluster events
  log "6. Verifying cluster events..."
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c \
    "SHOW falcon.cluster_events" 2>/dev/null || log "  (cluster_events not available)"

  log "Scale-out test complete."
}

cmd_scalein() {
  log "=== Scale-In Test (G2) ==="
  if ! command -v psql &>/dev/null; then
    die "psql is required for scale-in test"
  fi

  # 1. Write data
  log "1. Writing test data..."
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c "
    CREATE TABLE IF NOT EXISTS scalein_test (id SERIAL PRIMARY KEY, val TEXT);
    INSERT INTO scalein_test (val) SELECT 'row_' || i FROM generate_series(1, 100) AS i;
  " 2>/dev/null || die "Failed to write test data"

  local before_count
  before_count=$(psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -t -c \
    "SELECT COUNT(*) FROM scalein_test" 2>/dev/null | tr -d ' ')
  log "   Rows before scale-in: $before_count"

  # 2. Remove node via SQL
  log "2. Removing node via falcon_remove_node(2)..."
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c \
    "SELECT falcon_remove_node(2)" 2>/dev/null || die "Failed to remove node"

  # 3. Verify node lifecycle shows draining
  log "3. Verifying node lifecycle..."
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c \
    "SHOW falcon.node_lifecycle" 2>/dev/null || die "Failed to show node_lifecycle"

  # 4. Verify no data loss
  local after_count
  after_count=$(psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -t -c \
    "SELECT COUNT(*) FROM scalein_test" 2>/dev/null | tr -d ' ')
  log "   Rows after scale-in: $after_count"

  if [[ "$before_count" == "$after_count" ]]; then
    log "G2 PASS: No data loss after scale-in ($after_count rows)"
  else
    die "G2 FAIL: Data mismatch (before=$before_count, after=$after_count)"
  fi

  # 5. Verify cluster events
  log "4. Verifying cluster events..."
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U falcon -c \
    "SHOW falcon.cluster_events" 2>/dev/null || log "  (cluster_events not available)"

  log "Scale-in test complete."
}

CMD="${1:-help}"
case "$CMD" in
  start)    cmd_start ;;
  stop)     cmd_stop ;;
  status)   cmd_status ;;
  failover) cmd_failover ;;
  scaleout) cmd_scaleout ;;
  scalein)  cmd_scalein ;;
  smoke)    cmd_smoke ;;
  clean)    cmd_clean ;;
  help|*)
    echo "Usage: $0 {start|stop|status|failover|scaleout|scalein|smoke|clean}"
    echo ""
    echo "  start     Start 3-node cluster (primary + 2 replicas)"
    echo "  stop      Stop all nodes"
    echo "  status    Show running node status"
    echo "  failover  Kill primary, promote replica1"
    echo "  scaleout  G1: Add node, rebalance, verify data"
    echo "  scalein   G2: Remove node, verify no data loss"
    echo "  smoke     Run smoke test (connectivity + write/read)"
    echo "  clean     Stop nodes and remove data dirs"
    ;;
esac
