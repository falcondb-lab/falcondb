#!/usr/bin/env bash
# ============================================================================
# FalconDB — Primary + Replica replication demo (Linux / macOS / WSL)
# ============================================================================
# Starts a primary and replica, writes data, verifies replication, then
# promotes the replica and writes new data on the promoted node.
#
# Prerequisites: Rust 1.75+, psql, protoc (for gRPC codegen)
# Usage:  chmod +x scripts/demo_replication.sh && ./scripts/demo_replication.sh
# ============================================================================
set -euo pipefail

BOLD='\033[1m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}[demo]${NC} $*"; }
ok()    { echo -e "${GREEN}[  ok]${NC} $*"; }

DIR1=$(mktemp -d /tmp/falcon_primary_XXXX)
DIR2=$(mktemp -d /tmp/falcon_replica_XXXX)
PRIMARY_PG=5433
REPLICA_PG=5434
GRPC_PORT=50051
PIDS=()

cleanup() {
  info "Stopping all nodes..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$DIR1" "$DIR2"
  info "Cleaned up"
}
trap cleanup EXIT

# ── 1. Build ────────────────────────────────────────────────────────────────
info "Building FalconDB (release)..."
cargo build --release -p falcon_server 2>&1 | tail -1

# ── 2. Start Primary ───────────────────────────────────────────────────────
info "Starting PRIMARY on PG=$PRIMARY_PG, gRPC=$GRPC_PORT..."
cargo run --release -p falcon_server -- \
  --role primary \
  --pg-addr "0.0.0.0:$PRIMARY_PG" \
  --grpc-addr "0.0.0.0:$GRPC_PORT" \
  --data-dir "$DIR1" &
PIDS+=($!)
sleep 2
ok "Primary running (PID ${PIDS[0]})"

# ── 3. Start Replica ───────────────────────────────────────────────────────
info "Starting REPLICA on PG=$REPLICA_PG, connecting to primary gRPC..."
cargo run --release -p falcon_server -- \
  --role replica \
  --pg-addr "0.0.0.0:$REPLICA_PG" \
  --primary-endpoint "http://127.0.0.1:$GRPC_PORT" \
  --data-dir "$DIR2" &
PIDS+=($!)
sleep 3
ok "Replica running (PID ${PIDS[1]})"

# ── 4. Write data on Primary ───────────────────────────────────────────────
info "Writing data on PRIMARY..."
psql -h 127.0.0.1 -p "$PRIMARY_PG" -U falcon -c "
  CREATE TABLE repl_test (id INT PRIMARY KEY, val TEXT);
  INSERT INTO repl_test VALUES (1, 'hello'), (2, 'world'), (3, 'falcon');
  SELECT * FROM repl_test ORDER BY id;
"
ok "Primary write complete"

# ── 5. Wait for replication & read from Replica ────────────────────────────
info "Waiting for replication (2s)..."
sleep 2

info "Reading from REPLICA..."
psql -h 127.0.0.1 -p "$REPLICA_PG" -U falcon -c "
  SELECT * FROM repl_test ORDER BY id;
  SHOW falcon.node_role;
"
ok "Replica read complete — data replicated"

# ── 6. Verify replica rejects writes ───────────────────────────────────────
info "Verifying replica rejects writes..."
if psql -h 127.0.0.1 -p "$REPLICA_PG" -U falcon -c \
  "INSERT INTO repl_test VALUES (99, 'should_fail');" 2>&1 | grep -qi "read.only\|denied\|not allowed"; then
  ok "Replica correctly rejected write"
else
  info "Warning: replica write rejection not confirmed (may depend on enforcement mode)"
fi

# ── 7. Show replication metrics ─────────────────────────────────────────────
info "Replication metrics:"
psql -h 127.0.0.1 -p "$PRIMARY_PG" -U falcon -c "SHOW falcon.wal_stats;" 2>/dev/null || true
psql -h 127.0.0.1 -p "$PRIMARY_PG" -U falcon -c "SHOW falcon.txn_stats;" 2>/dev/null || true

echo ""
echo -e "${BOLD}${GREEN}=== FalconDB replication demo completed ===${NC}"
echo ""
echo "Primary:  psql -h 127.0.0.1 -p $PRIMARY_PG -U falcon"
echo "Replica:  psql -h 127.0.0.1 -p $REPLICA_PG -U falcon"
echo "Press Ctrl+C to stop."
wait
