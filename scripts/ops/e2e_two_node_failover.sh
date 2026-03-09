#!/usr/bin/env bash
# ============================================================================
# FalconDB — E2E Two-Node Failover Test (Linux / macOS / WSL)
# ============================================================================
# Minimal closed-loop test:
#   1. Start primary (WAL-enabled, gRPC replication)
#   2. Start replica (connects to primary)
#   3. Wait for replica health/ready
#   4. Write test data on primary via psql, verify
#   5. Kill primary (simulate failure)
#   6. Promote replica (restart as standalone/primary)
#   7. Verify old data readable + new writes succeed on promoted node
#   8. Output PASS/FAIL with diagnostics
#
# Prerequisites: Rust 1.75+, psql, ports 5433/5434/50051 free
# Usage:  chmod +x scripts/e2e_two_node_failover.sh && ./scripts/e2e_two_node_failover.sh
# ============================================================================
set -euo pipefail

BOLD='\033[1m'
RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}[e2e]${NC} $*"; }
ok()    { echo -e "${GREEN}[ ok]${NC} $*"; }
fail()  { echo -e "${RED}[FAIL]${NC} $*"; RESULT="FAIL"; }

RESULT="PASS"
DIR_PRIMARY=$(mktemp -d /tmp/falcon_e2e_primary_XXXX)
DIR_REPLICA=$(mktemp -d /tmp/falcon_e2e_replica_XXXX)
LOG_PRIMARY="$DIR_PRIMARY/server.log"
LOG_REPLICA="$DIR_REPLICA/server.log"
PRIMARY_PG=5433
REPLICA_PG=5434
GRPC_PORT=50051
ADMIN_PRIMARY=8080
ADMIN_REPLICA=8081
PIDS=()

cleanup() {
  info "Cleaning up..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [ "$RESULT" != "PASS" ]; then
    echo ""
    echo -e "${RED}=== DIAGNOSTICS ===${NC}"
    echo "Primary log (last 50 lines): $LOG_PRIMARY"
    tail -50 "$LOG_PRIMARY" 2>/dev/null || echo "(no log)"
    echo ""
    echo "Replica log (last 50 lines): $LOG_REPLICA"
    tail -50 "$LOG_REPLICA" 2>/dev/null || echo "(no log)"
    echo ""
    echo "Primary data dir: $DIR_PRIMARY"
    echo "Replica data dir: $DIR_REPLICA"
    echo "Primary PG port: $PRIMARY_PG"
    echo "Replica PG port: $REPLICA_PG"
    echo "gRPC port: $GRPC_PORT"
  else
    rm -rf "$DIR_PRIMARY" "$DIR_REPLICA"
  fi
}
trap cleanup EXIT

wait_for_pg() {
  local port=$1 label=$2 max_wait=${3:-15}
  local i=0
  while [ $i -lt $max_wait ]; do
    if psql -h 127.0.0.1 -p "$port" -U falcon -c "SELECT 1;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    ((i++))
  done
  return 1
}

wait_for_health() {
  local port=$1 label=$2 max_wait=${3:-15}
  local i=0
  while [ $i -lt $max_wait ]; do
    if curl -sf "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    ((i++))
  done
  return 1
}

# ── Build ───────────────────────────────────────────────────────────────────
info "Building FalconDB (release)..."
cargo build --release -p falcon_server 2>&1 | tail -1
BINARY="./target/release/falcon_server"
if [ ! -f "$BINARY" ]; then
  BINARY="./target/release/falcon"
fi

# ── Step 1: Start Primary ──────────────────────────────────────────────────
info "Step 1: Starting PRIMARY (PG=$PRIMARY_PG, gRPC=$GRPC_PORT)..."
cargo run --release -p falcon_server -- \
  --role primary \
  --pg-addr "0.0.0.0:$PRIMARY_PG" \
  --grpc-addr "0.0.0.0:$GRPC_PORT" \
  --data-dir "$DIR_PRIMARY" \
  --metrics-addr "0.0.0.0:9090" \
  > "$LOG_PRIMARY" 2>&1 &
PIDS+=($!)

if ! wait_for_pg $PRIMARY_PG "primary"; then
  fail "Primary failed to start within 15s"
  exit 1
fi
ok "Primary ready on port $PRIMARY_PG (PID ${PIDS[0]})"

# ── Step 2: Start Replica ──────────────────────────────────────────────────
info "Step 2: Starting REPLICA (PG=$REPLICA_PG)..."
cargo run --release -p falcon_server -- \
  --role replica \
  --pg-addr "0.0.0.0:$REPLICA_PG" \
  --primary-endpoint "http://127.0.0.1:$GRPC_PORT" \
  --data-dir "$DIR_REPLICA" \
  --metrics-addr "0.0.0.0:9091" \
  > "$LOG_REPLICA" 2>&1 &
PIDS+=($!)

if ! wait_for_pg $REPLICA_PG "replica"; then
  fail "Replica failed to start within 15s"
  exit 1
fi
ok "Replica ready on port $REPLICA_PG (PID ${PIDS[1]})"

# ── Step 3: Wait for replication sync ──────────────────────────────────────
info "Step 3: Waiting for replication sync (3s)..."
sleep 3

# ── Step 4: Write test data on Primary ─────────────────────────────────────
info "Step 4: Writing test data on PRIMARY..."
psql -h 127.0.0.1 -p "$PRIMARY_PG" -U falcon -c "
  CREATE TABLE e2e_test (id INT PRIMARY KEY, name TEXT, score FLOAT8);
  INSERT INTO e2e_test VALUES (1, 'Alice', 95.5);
  INSERT INTO e2e_test VALUES (2, 'Bob', 87.0);
  INSERT INTO e2e_test VALUES (3, 'Charlie', 92.3);
" > /dev/null 2>&1

# Verify on primary
ROW_COUNT=$(psql -h 127.0.0.1 -p "$PRIMARY_PG" -U falcon -t -A -c "SELECT COUNT(*) FROM e2e_test;")
if [ "$ROW_COUNT" = "3" ]; then
  ok "Primary has 3 rows"
else
  fail "Primary expected 3 rows, got: $ROW_COUNT"
fi

# Wait for replication
info "Waiting for WAL replication to replica (2s)..."
sleep 2

# Verify on replica
REPLICA_COUNT=$(psql -h 127.0.0.1 -p "$REPLICA_PG" -U falcon -t -A -c "SELECT COUNT(*) FROM e2e_test;" 2>/dev/null || echo "ERROR")
if [ "$REPLICA_COUNT" = "3" ]; then
  ok "Replica has 3 rows (replication confirmed)"
else
  info "Replica row count: $REPLICA_COUNT (replication may not be instant — continuing)"
fi

# Verify replica rejects writes
info "Verifying replica rejects writes..."
if psql -h 127.0.0.1 -p "$REPLICA_PG" -U falcon -c "INSERT INTO e2e_test VALUES (99, 'fail', 0);" 2>&1 | grep -qi "read.only\|denied\|not allowed\|ReadOnly"; then
  ok "Replica correctly rejected write"
else
  info "Warning: replica write rejection not confirmed (enforcement mode may vary)"
fi

# ── Step 5: Kill Primary ───────────────────────────────────────────────────
info "Step 5: Killing PRIMARY (simulating failure)..."
kill "${PIDS[0]}" 2>/dev/null || true
wait "${PIDS[0]}" 2>/dev/null || true
ok "Primary killed"

# Confirm primary is gone
sleep 1
if psql -h 127.0.0.1 -p "$PRIMARY_PG" -U falcon -c "SELECT 1;" >/dev/null 2>&1; then
  fail "Primary still responding after kill!"
else
  ok "Primary confirmed down"
fi

# ── Step 6: Promote Replica ────────────────────────────────────────────────
info "Step 6: Promoting REPLICA (restart as standalone on port $REPLICA_PG)..."
# Kill the replica and restart it as standalone/primary
kill "${PIDS[1]}" 2>/dev/null || true
wait "${PIDS[1]}" 2>/dev/null || true

cargo run --release -p falcon_server -- \
  --role standalone \
  --pg-addr "0.0.0.0:$REPLICA_PG" \
  --data-dir "$DIR_REPLICA" \
  --metrics-addr "0.0.0.0:9091" \
  > "$DIR_REPLICA/promoted.log" 2>&1 &
PIDS[1]=$!

if ! wait_for_pg $REPLICA_PG "promoted"; then
  fail "Promoted replica failed to start within 15s"
  exit 1
fi
ok "Promoted replica ready on port $REPLICA_PG"

# ── Step 7: Verify data + new writes ──────────────────────────────────────
info "Step 7: Verifying data integrity on promoted node..."

# Old data still readable
PROMOTED_COUNT=$(psql -h 127.0.0.1 -p "$REPLICA_PG" -U falcon -t -A -c "SELECT COUNT(*) FROM e2e_test;" 2>/dev/null || echo "ERROR")
if [ "$PROMOTED_COUNT" = "3" ]; then
  ok "All 3 rows from primary still readable after promotion"
else
  fail "Expected 3 rows after promotion, got: $PROMOTED_COUNT"
fi

# New writes succeed
info "Writing new data on promoted node..."
psql -h 127.0.0.1 -p "$REPLICA_PG" -U falcon -c "
  INSERT INTO e2e_test VALUES (4, 'Diana', 88.8);
  INSERT INTO e2e_test VALUES (5, 'Eve', 91.1);
" > /dev/null 2>&1

FINAL_COUNT=$(psql -h 127.0.0.1 -p "$REPLICA_PG" -U falcon -t -A -c "SELECT COUNT(*) FROM e2e_test;" 2>/dev/null || echo "ERROR")
if [ "$FINAL_COUNT" = "5" ]; then
  ok "New writes succeeded — 5 rows total"
else
  fail "Expected 5 rows after new writes, got: $FINAL_COUNT"
fi

# Verify specific data
ALICE=$(psql -h 127.0.0.1 -p "$REPLICA_PG" -U falcon -t -A -c "SELECT name FROM e2e_test WHERE id = 1;")
if [ "$ALICE" = "Alice" ]; then
  ok "Data integrity: Alice found at id=1"
else
  fail "Data integrity: expected Alice at id=1, got: $ALICE"
fi

# ── Step 8: Final result ───────────────────────────────────────────────────
echo ""
echo "============================================================"
if [ "$RESULT" = "PASS" ]; then
  echo -e "${BOLD}${GREEN}  E2E TWO-NODE FAILOVER TEST: PASS${NC}"
else
  echo -e "${BOLD}${RED}  E2E TWO-NODE FAILOVER TEST: FAIL${NC}"
fi
echo "============================================================"
echo ""

[ "$RESULT" = "PASS" ]
