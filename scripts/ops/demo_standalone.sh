#!/usr/bin/env bash
# ============================================================================
# FalconDB — One-click standalone demo (Linux / macOS / WSL)
# ============================================================================
# Prerequisites: Rust 1.75+, psql (postgresql-client)
#
# Usage:  chmod +x scripts/demo_standalone.sh && ./scripts/demo_standalone.sh
# ============================================================================
set -euo pipefail

BOLD='\033[1m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}[demo]${NC} $*"; }
ok()    { echo -e "${GREEN}[  ok]${NC} $*"; }

DATA_DIR=$(mktemp -d /tmp/falcondb_demo_XXXX)
PG_PORT=5433
ADMIN_PORT=8080

cleanup() {
  info "Stopping FalconDB..."
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  rm -rf "$DATA_DIR"
  info "Cleaned up $DATA_DIR"
}
trap cleanup EXIT

# ── 1. Build ────────────────────────────────────────────────────────────────
info "Building FalconDB (release)..."
cargo build --release -p falcon_server 2>&1 | tail -1

# ── 2. Start server ────────────────────────────────────────────────────────
info "Starting FalconDB on port $PG_PORT (data: $DATA_DIR)..."
cargo run --release -p falcon_server -- \
  --pg-addr "0.0.0.0:$PG_PORT" \
  --data-dir "$DATA_DIR" \
  --no-wal &
SERVER_PID=$!
sleep 2

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "ERROR: FalconDB failed to start"; exit 1
fi
ok "FalconDB running (PID $SERVER_PID)"

# ── 3. Health check ────────────────────────────────────────────────────────
info "Health check..."
curl -sf "http://127.0.0.1:$ADMIN_PORT/health" && echo
ok "Health endpoint OK"

# ── 4. SQL smoke test via psql ──────────────────────────────────────────────
info "Running SQL smoke test..."
psql -h 127.0.0.1 -p "$PG_PORT" -U falcon -c "
  -- DDL
  CREATE TABLE demo (id INT PRIMARY KEY, name TEXT, score FLOAT8);

  -- DML
  INSERT INTO demo VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.0), (3, 'Eve', 92.3);
  UPDATE demo SET score = 99.0 WHERE id = 1;
  DELETE FROM demo WHERE id = 3;

  -- Query
  SELECT * FROM demo ORDER BY id;

  -- Aggregation
  SELECT COUNT(*), AVG(score), MAX(score) FROM demo;

  -- Observability
  SHOW falcon.txn_stats;
  SHOW falcon.node_role;

  -- Cleanup
  DROP TABLE demo;
"
ok "SQL smoke test passed"

# ── 5. Benchmark ────────────────────────────────────────────────────────────
info "Running quick benchmark (1000 ops)..."
cargo run --release -p falcon_bench -- --ops 1000 --shards 2 2>&1 | grep -E '(TPS|latency|committed)'
ok "Benchmark complete"

echo ""
echo -e "${BOLD}${GREEN}=== FalconDB demo completed successfully ===${NC}"
echo ""
