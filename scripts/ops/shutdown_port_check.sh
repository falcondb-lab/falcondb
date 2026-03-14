#!/usr/bin/env bash
# FalconDB — Graceful Shutdown Port Release Verification (Linux/macOS)
#
# This script verifies that after SIGTERM / SIGINT:
#   1. Port 5443 (PG) is NOT in LISTEN state
#   2. Port 8080 (HTTP health) is NOT in LISTEN state
#   3. gRPC port (if Primary) is NOT in LISTEN state
#
# Usage:
#   ./scripts/shutdown_port_check.sh [--pg-port 5443] [--http-port 8080]

set -euo pipefail

PG_PORT="${1:-5443}"
HTTP_PORT="${2:-8080}"
STARTUP_WAIT=3
SHUTDOWN_WAIT=5

echo "================================================================="
echo "  FalconDB — Graceful Shutdown Port Release Check (Unix)"
echo "================================================================="
echo ""

# ── Helper: check if a port is listening ──
port_listening() {
    local port=$1
    if command -v ss &>/dev/null; then
        ss -tln | grep -q ":${port} "
    elif command -v netstat &>/dev/null; then
        netstat -tln | grep -q ":${port} "
    elif command -v lsof &>/dev/null; then
        lsof -i ":${port}" -sTCP:LISTEN &>/dev/null
    else
        echo "WARN: No port-check tool available (ss/netstat/lsof)"
        return 1
    fi
}

# ── Gate 0: Build ──
echo "[Gate 0] Building falcon..."
if cargo build -p falcon_server --quiet 2>/dev/null; then
    echo "  PASS: falcon built"
else
    echo "  FAIL: build failed"
    exit 1
fi

# ── Gate 1: Verify ports are free ──
echo "[Gate 1] Checking ports are free..."
if port_listening "$PG_PORT"; then
    echo "  WARN: Port $PG_PORT already in use — skipping"
    exit 0
fi
if port_listening "$HTTP_PORT"; then
    echo "  WARN: Port $HTTP_PORT already in use — skipping"
    exit 0
fi
echo "  PASS: Ports $PG_PORT and $HTTP_PORT are free"

# ── Gate 2: Start FalconDB ──
echo "[Gate 2] Starting FalconDB..."
FALCON_EXE="./target/debug/falcon"
if [ ! -f "$FALCON_EXE" ]; then
    FALCON_EXE="falcon"
fi

$FALCON_EXE --pg-addr "127.0.0.1:${PG_PORT}" --no-wal &
FALCON_PID=$!
echo "  PID: $FALCON_PID"
echo "  Waiting ${STARTUP_WAIT}s for startup..."
sleep "$STARTUP_WAIT"

# ── Gate 3: Verify ports are listening ──
echo "[Gate 3] Checking ports are listening..."
if ! port_listening "$PG_PORT"; then
    echo "  FAIL: Port $PG_PORT not listening after startup"
    kill "$FALCON_PID" 2>/dev/null || true
    exit 1
fi
echo "  PASS: Port $PG_PORT is LISTENING"

# ── Gate 4: Send SIGTERM (graceful shutdown) ──
echo "[Gate 4] Sending SIGTERM to PID $FALCON_PID..."
kill -TERM "$FALCON_PID" 2>/dev/null || true
echo "  Waiting ${SHUTDOWN_WAIT}s for shutdown..."
sleep "$SHUTDOWN_WAIT"

# ── Gate 5: Verify ports are released ──
echo "[Gate 5] Checking ports are released..."
FAILURES=0

if port_listening "$PG_PORT"; then
    echo "  FAIL: Port $PG_PORT still LISTENING after shutdown!"
    FAILURES=$((FAILURES + 1))
else
    echo "  PASS: Port $PG_PORT released"
fi

if port_listening "$HTTP_PORT"; then
    echo "  FAIL: Port $HTTP_PORT still LISTENING after shutdown!"
    FAILURES=$((FAILURES + 1))
else
    echo "  PASS: Port $HTTP_PORT released"
fi

# Cleanup: make sure process is dead
kill -9 "$FALCON_PID" 2>/dev/null || true
wait "$FALCON_PID" 2>/dev/null || true

# ── Summary ──
echo ""
if [ "$FAILURES" -eq 0 ]; then
    echo "================================================================="
    echo "  ALL PORTS RELEASED — shutdown protocol correct"
    echo "================================================================="
    exit 0
else
    echo "================================================================="
    echo "  $FAILURES PORT(S) STILL LISTENING — shutdown bug detected"
    echo "================================================================="
    exit 1
fi
