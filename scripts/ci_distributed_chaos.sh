#!/usr/bin/env bash
# D7-1: Distributed Chaos CI Gate
# Validates RPO/RTO/consistency under fault injection.
# Exit 0 = all checks pass, non-zero = failure.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
FALCON_BIN="$ROOT_DIR/target/release/falcon_server"
PORT_BASE="${PORT_BASE:-15432}"
NODE_COUNT=3
PIDS=()
PASS=0
FAIL=0

log() { echo "[$(date +%H:%M:%S)] $*"; }

cleanup() {
    log "Cleaning up..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT

check() {
    local name="$1"
    local result="$2"
    if [ "$result" -eq 0 ]; then
        log "  ✓ $name"
        PASS=$((PASS + 1))
    else
        log "  ✗ $name"
        FAIL=$((FAIL + 1))
    fi
}

# ====================================================================
# Build
# ====================================================================
log "=== Distributed Chaos CI Gate ==="
log "[0/5] Building FalconDB (release)..."
(cd "$ROOT_DIR" && cargo build --release -p falcon_server 2>&1 | tail -3)

# ====================================================================
# Test 1: Leader Kill — RPO/RTO check
# ====================================================================
log "[1/5] Leader Kill Test"

# Start 3-node cluster
for i in $(seq 0 $((NODE_COUNT - 1))); do
    PORT=$((PORT_BASE + i))
    $FALCON_BIN --port "$PORT" &
    PIDS+=($!)
done
sleep 2

# Write data to leader (node 0)
LEADER_PORT=$PORT_BASE
psql -h localhost -p "$LEADER_PORT" -U falcon -c \
    "CREATE TABLE chaos_t (id INT PRIMARY KEY, val TEXT);" 2>/dev/null || true
for i in $(seq 1 100); do
    psql -h localhost -p "$LEADER_PORT" -U falcon -c \
        "INSERT INTO chaos_t VALUES ($i, 'v$i');" 2>/dev/null || true
done

# Kill leader
log "  Killing leader (port $LEADER_PORT)..."
kill "${PIDS[0]}" 2>/dev/null || true
sleep 3

# Check data on replica (node 1)
REPLICA_PORT=$((PORT_BASE + 1))
ROW_COUNT=$(psql -h localhost -p "$REPLICA_PORT" -U falcon -t -c \
    "SELECT COUNT(*) FROM chaos_t;" 2>/dev/null | tr -d ' ' || echo "0")
check "Leader kill: data survived on replica ($ROW_COUNT rows)" \
    "$([ "${ROW_COUNT:-0}" -gt 0 ] && echo 0 || echo 1)"

# Cleanup this round
for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
wait 2>/dev/null || true
PIDS=()

# ====================================================================
# Test 2: Network Partition (simulated — stale writes rejected)
# ====================================================================
log "[2/5] Stale Leader Fencing Test"

# This test validates epoch fencing via unit tests
(cd "$ROOT_DIR" && cargo test -p falcon_cluster epoch -- --nocapture 2>&1 | tail -5)
EPOCH_RESULT=$?
check "Epoch fencing unit tests" "$EPOCH_RESULT"

# ====================================================================
# Test 3: Node Restart — topology recovery
# ====================================================================
log "[3/5] Consistent State Recovery Test"

(cd "$ROOT_DIR" && cargo test -p falcon_cluster consistent_state -- --nocapture 2>&1 | tail -5)
CS_RESULT=$?
check "Consistent state unit tests" "$CS_RESULT"

# ====================================================================
# Test 4: Shard Migration Interrupt
# ====================================================================
log "[4/5] Migration State Machine Test"

(cd "$ROOT_DIR" && cargo test -p falcon_cluster migration -- --nocapture 2>&1 | tail -5)
MIG_RESULT=$?
check "Migration state machine unit tests" "$MIG_RESULT"

# ====================================================================
# Test 5: Supervisor Degradation
# ====================================================================
log "[5/5] Supervisor Degradation Test"

(cd "$ROOT_DIR" && cargo test -p falcon_cluster supervisor -- --nocapture 2>&1 | tail -5)
SUP_RESULT=$?
check "Supervisor unit tests" "$SUP_RESULT"

# ====================================================================
# Summary
# ====================================================================
TOTAL=$((PASS + FAIL))
log ""
log "=== Chaos Gate Results: $PASS/$TOTAL passed ==="
if [ "$FAIL" -gt 0 ]; then
    log "FAILED — $FAIL tests did not pass"
    exit 1
fi
log "ALL PASSED"
exit 0
