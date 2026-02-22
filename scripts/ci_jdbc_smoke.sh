#!/usr/bin/env bash
# CI gate: Start FalconDB and run JDBC smoke tests.
# Exit code 0 = all green, non-zero = failure.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
JDBC_DIR="$ROOT_DIR/tools/jdbc-smoke"
FALCON_PORT="${FALCON_PORT:-15432}"
FALCON_BIN="$ROOT_DIR/target/release/falcon_server"

echo "=== CI JDBC Smoke Gate ==="

# 1. Build FalconDB (release)
echo "[1/4] Building FalconDB..."
(cd "$ROOT_DIR" && cargo build --release -p falcon_server 2>&1 | tail -3)

# 2. Start FalconDB in background
echo "[2/4] Starting FalconDB on port $FALCON_PORT..."
$FALCON_BIN --port "$FALCON_PORT" &
FALCON_PID=$!
trap "kill $FALCON_PID 2>/dev/null || true; wait $FALCON_PID 2>/dev/null || true" EXIT

# Wait for FalconDB to be ready (up to 10s)
for i in $(seq 1 20); do
    if pg_isready -h localhost -p "$FALCON_PORT" -q 2>/dev/null; then
        echo "  FalconDB ready after ${i}x500ms"
        break
    fi
    sleep 0.5
done

if ! pg_isready -h localhost -p "$FALCON_PORT" -q 2>/dev/null; then
    echo "ERROR: FalconDB did not become ready within 10s"
    exit 1
fi

# 3. Run JDBC smoke tests
echo "[3/4] Running JDBC smoke tests..."
(cd "$JDBC_DIR" && mvn -q test \
    -Dfalcon.host=localhost \
    -Dfalcon.port="$FALCON_PORT" \
    -Dfalcon.user=falcon \
    -Dfalcon.password=falcon)
RESULT=$?

# 4. Report
echo "[4/4] Result: $([ $RESULT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
exit $RESULT
