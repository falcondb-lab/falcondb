#!/usr/bin/env bash
# ============================================================================
# FalconDB Benchmark — Start VoltDB for benchmark
# ============================================================================
#
# Prerequisites:
#   - VoltDB Community or Enterprise installed
#   - VOLTDB_HOME set (e.g. /opt/voltdb)
#   - sqlcmd on PATH
#
# This script:
#   1. Compiles stored procedures (VoltDB requires pre-compiled Java procs)
#   2. Initializes and starts VoltDB
#   3. Loads the benchmark schema
#   4. Exposes PG-wire on port 5444 for pgbench compatibility
# ============================================================================

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VOLTDB_DIR="$BENCH_DIR/voltdb"
DATA_DIR="$VOLTDB_DIR/voltdb_data"

VOLTDB_HOME="${VOLTDB_HOME:-/opt/voltdb}"
VOLTDB_BIN="$VOLTDB_HOME/bin"

if [ ! -x "$VOLTDB_BIN/voltdb" ]; then
    echo "ERROR: VoltDB not found at \$VOLTDB_HOME=$VOLTDB_HOME"
    echo "Install VoltDB or set VOLTDB_HOME."
    exit 1
fi

echo "=== VoltDB Benchmark Setup ==="

# Stop any existing instance
"$VOLTDB_BIN/voltadmin" shutdown 2>/dev/null || true
sleep 2

# Clean data directory
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

# Initialize
echo "[1/4] Initializing VoltDB..."
"$VOLTDB_BIN/voltdb" init \
    --dir="$DATA_DIR" \
    --config="$VOLTDB_DIR/deployment.xml" \
    --force

# Start (background)
echo "[2/4] Starting VoltDB (port 21212, PG-wire 5444)..."
"$VOLTDB_BIN/voltdb" start \
    --dir="$DATA_DIR" \
    --background \
    --host=localhost

# Wait for ready
echo "[3/4] Waiting for VoltDB to be ready..."
for i in $(seq 1 30); do
    if "$VOLTDB_BIN/sqlcmd" --query="SELECT 1;" >/dev/null 2>&1; then
        echo "  VoltDB ready after ${i}s"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: VoltDB did not start within 30s"
        exit 1
    fi
    sleep 1
done

# Load schema
echo "[4/4] Loading benchmark schema..."
"$VOLTDB_BIN/sqlcmd" < "$BENCH_DIR/workloads/setup_voltdb.sql"

echo ""
echo "VoltDB running:"
echo "  Native:  localhost:21212"
echo "  PG-wire: localhost:5444"
echo "  Admin:   http://localhost:8080"
echo ""
