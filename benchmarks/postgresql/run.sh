#!/usr/bin/env bash
# Start PostgreSQL for benchmarking
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
PG_CONF="$BENCH_DIR/postgresql.conf"
PG_DATA="$BENCH_DIR/pg_data"
PID_FILE="$BENCH_DIR/postgres.pid"

echo "Starting PostgreSQL (benchmark mode)..."

# Check if pg_isready already shows a running instance
if pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then
    echo "  PostgreSQL already running on port 5432"
    echo "  Skipping init — using existing instance"
    exit 0
fi

# Init fresh data directory
if [ -d "$PG_DATA" ]; then
    rm -rf "$PG_DATA"
fi

echo "  Initializing data directory: $PG_DATA"
initdb -D "$PG_DATA" --no-locale -E UTF8 >/dev/null 2>&1

# Copy our tuned config
cp "$PG_CONF" "$PG_DATA/postgresql.conf"

echo "  Starting PostgreSQL..."
pg_ctl -D "$PG_DATA" -l "$BENCH_DIR/pg.log" start >/dev/null 2>&1

# Wait for readiness
echo -n "  Waiting for ready..."
for i in $(seq 1 30); do
    if pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then
        echo " OK (${i}s)"
        # Create benchmark database
        createdb -h 127.0.0.1 -p 5432 falconbench 2>/dev/null || true
        exit 0
    fi
    sleep 1
    echo -n "."
done
echo " TIMEOUT"
pg_ctl -D "$PG_DATA" stop 2>/dev/null || true
exit 1
