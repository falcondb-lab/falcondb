#!/usr/bin/env bash
# Start FalconDB for benchmarking
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
FALCON_BIN="$REPO_ROOT/target/release/falcon"
DATA_DIR="$BENCH_DIR/bench_data"
CONFIG="$BENCH_DIR/config.toml"
PID_FILE="$BENCH_DIR/falcon.pid"

if [ ! -f "$FALCON_BIN" ]; then
    echo "ERROR: falcon binary not found. Run: cargo build --release -p falcon_server"
    exit 1
fi

# Clean previous data
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

echo "Starting FalconDB (benchmark mode)..."
echo "  Binary:  $FALCON_BIN"
echo "  Config:  $CONFIG"
echo "  Data:    $DATA_DIR"
echo "  Listen:  127.0.0.1:5443"

"$FALCON_BIN" --config "$CONFIG" --data-dir "$DATA_DIR" &
FALCON_PID=$!
echo $FALCON_PID > "$PID_FILE"
echo "  PID:     $FALCON_PID"

# Wait for readiness
echo -n "  Waiting for ready..."
for i in $(seq 1 30); do
    if psql -h 127.0.0.1 -p 5443 -U falcon -c "SELECT 1" >/dev/null 2>&1; then
        echo " OK (${i}s)"
        exit 0
    fi
    sleep 1
    echo -n "."
done
echo " TIMEOUT"
kill $FALCON_PID 2>/dev/null || true
exit 1
