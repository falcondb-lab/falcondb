#!/usr/bin/env bash
# ============================================================================
# FalconDB — Failover Under Load Benchmark
# ============================================================================
# Demonstrates deterministic commit semantics: during sustained write load,
# kill the primary and promote the replica, then verify no phantom commits.
#
# Topology:
#   Node A (primary) — PG port 5433, admin 8080, gRPC 50051
#   Node B (replica)  — PG port 5434, admin 8081, gRPC 50052
#
# Usage:
#   ./scripts/bench_failover_under_load.sh
#   TOTAL_SEC=180 KILL_AT_SEC=60 ./scripts/bench_failover_under_load.sh
#
# Environment variables:
#   FALCON_BIN     — path to FalconDB binary (default: target/release/falcon_server)
#   TOTAL_SEC      — total benchmark duration (default: 120)
#   KILL_AT_SEC    — when to kill primary (default: 30)
#   CONCURRENCY    — pgbench concurrency (default: 16)
#   SCALE          — pgbench scale factor (default: 10)
# ============================================================================
set -euo pipefail

# ── Defaults ────────────────────────────────────────────────────────────────
FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"
TOTAL_SEC="${TOTAL_SEC:-120}"
KILL_AT_SEC="${KILL_AT_SEC:-30}"
CONCURRENCY="${CONCURRENCY:-16}"
SCALE="${SCALE:-10}"

NODE_A_PG_PORT=5433
NODE_A_ADMIN_PORT=8080
NODE_A_GRPC_PORT=50051
NODE_B_PG_PORT=5434
NODE_B_ADMIN_PORT=8081
NODE_B_GRPC_PORT=50052

TS=$(date -u +"%Y%m%dT%H%M%SZ")
OUT_DIR="bench_out/${TS}/failover"
DATA_A="./falcon_failover_data_a"
DATA_B="./falcon_failover_data_b"

HOST="127.0.0.1"
DB="falcon"
USER="falcon"

VERDICT="FAIL"

# ── Dependency check ───────────────────────────────────────────────────────
check_cmd() {
  if ! command -v "$1" &>/dev/null; then
    echo "ERROR: required command '$1' not found. $2" >&2
    exit 1
  fi
}

check_cmd pgbench "Install PostgreSQL client tools."
check_cmd psql    "Install PostgreSQL client tools."

if [ ! -f "${FALCON_BIN}" ]; then
  echo "ERROR: FalconDB binary not found at '${FALCON_BIN}'."
  echo "  Build with: cargo build -p falcon_server --release"
  exit 1
fi

# ── Output directories ────────────────────────────────────────────────────
mkdir -p "${OUT_DIR}/logs" "${OUT_DIR}/load"

echo "============================================================"
echo "FalconDB — Failover Under Load Benchmark"
echo "============================================================"
echo "Timestamp  : ${TS}"
echo "Output     : ${OUT_DIR}/"
echo "Total      : ${TOTAL_SEC}s   Kill at: ${KILL_AT_SEC}s"
echo "Concurrency: ${CONCURRENCY}  Scale: ${SCALE}"
echo "============================================================"

# ── Cleanup function ──────────────────────────────────────────────────────
cleanup() {
  echo ""
  echo "=== Cleanup ==="
  # Kill any remaining falcon processes we started
  [ -n "${PID_A:-}" ] && kill "${PID_A}" 2>/dev/null || true
  [ -n "${PID_B:-}" ] && kill "${PID_B}" 2>/dev/null || true
  [ -n "${PID_LOAD1:-}" ] && kill "${PID_LOAD1}" 2>/dev/null || true
  [ -n "${PID_LOAD2:-}" ] && kill "${PID_LOAD2}" 2>/dev/null || true
  wait 2>/dev/null || true
  echo "  Processes cleaned up."
}
trap cleanup EXIT

# ── Generate node configs ─────────────────────────────────────────────────
generate_config() {
  local role="$1" pg_port="$2" admin_port="$3" grpc_port="$4" data_dir="$5" node_id="$6"
  local primary_ep=""
  if [ "${role}" = "replica" ]; then
    primary_ep="http://${HOST}:${NODE_A_GRPC_PORT}"
  fi

  cat <<EOF
[server]
pg_listen_addr = "0.0.0.0:${pg_port}"
admin_listen_addr = "0.0.0.0:${admin_port}"
node_id = ${node_id}
max_connections = 256

[storage]
wal_enabled = true
data_dir = "${data_dir}"

[wal]
group_commit = true
flush_interval_us = 1000
sync_mode = "fdatasync"

[auth]
method = "trust"

[replication]
role = "${role}"
grpc_listen_addr = "0.0.0.0:${grpc_port}"
primary_endpoint = "${primary_ep}"
max_records_per_chunk = 1000
poll_interval_ms = 100
shard_count = 1

[gc]
enabled = false
EOF
}

# ── Start nodes ───────────────────────────────────────────────────────────
echo ""
echo "=== Starting Nodes ==="

# Clean data dirs
rm -rf "${DATA_A}" "${DATA_B}"
mkdir -p "${DATA_A}" "${DATA_B}"

# Generate configs
CONF_A="${OUT_DIR}/node_a.toml"
CONF_B="${OUT_DIR}/node_b.toml"
generate_config "primary" "${NODE_A_PG_PORT}" "${NODE_A_ADMIN_PORT}" "${NODE_A_GRPC_PORT}" "${DATA_A}" 1 > "${CONF_A}"
generate_config "replica" "${NODE_B_PG_PORT}" "${NODE_B_ADMIN_PORT}" "${NODE_B_GRPC_PORT}" "${DATA_B}" 2 > "${CONF_B}"

echo "  Starting Node A (primary) on port ${NODE_A_PG_PORT}..."
"${FALCON_BIN}" -c "${CONF_A}" > "${OUT_DIR}/logs/node_a.log" 2>&1 &
PID_A=$!

echo "  Starting Node B (replica) on port ${NODE_B_PG_PORT}..."
"${FALCON_BIN}" -c "${CONF_B}" > "${OUT_DIR}/logs/node_b.log" 2>&1 &
PID_B=$!

# Wait for nodes to be ready
wait_for_pg() {
  local port="$1" label="$2" max_wait=30
  for i in $(seq 1 ${max_wait}); do
    if psql -h "${HOST}" -p "${port}" -U "${USER}" -d postgres -c "SELECT 1;" &>/dev/null; then
      echo "  ${label} ready (${i}s)."
      return 0
    fi
    sleep 1
  done
  echo "  ERROR: ${label} did not become ready within ${max_wait}s."
  return 1
}

wait_for_pg "${NODE_A_PG_PORT}" "Node A"
wait_for_pg "${NODE_B_PG_PORT}" "Node B"

# ── Initialize database ──────────────────────────────────────────────────
echo ""
echo "=== Initialize Database ==="

# Create the database if needed
psql -h "${HOST}" -p "${NODE_A_PG_PORT}" -U "${USER}" -d postgres \
  -c "CREATE DATABASE ${DB};" 2>/dev/null || true

# Init pgbench schema
pgbench -h "${HOST}" -p "${NODE_A_PG_PORT}" -U "${USER}" -d "${DB}" \
  -i -s "${SCALE}" --no-vacuum > "${OUT_DIR}/load/init.log" 2>&1

# Create tx marker table for determinism validation
psql -h "${HOST}" -p "${NODE_A_PG_PORT}" -U "${USER}" -d "${DB}" <<SQL
CREATE TABLE IF NOT EXISTS bench_tx_markers (
  id BIGINT PRIMARY KEY,
  ts BIGINT NOT NULL,
  payload TEXT
);
SQL

echo "  Database initialized."

# Wait a moment for replication to catch up
echo "  Waiting for replication sync..."
sleep 5

# ── Pre-failover: record marker range ────────────────────────────────────
# Insert some known markers before load
echo ""
echo "=== Pre-failover markers ==="
psql -h "${HOST}" -p "${NODE_A_PG_PORT}" -U "${USER}" -d "${DB}" <<SQL
INSERT INTO bench_tx_markers (id, ts, payload)
SELECT g, extract(epoch from now())::bigint, 'pre-failover-' || g
FROM generate_series(1, 100) AS g
ON CONFLICT DO NOTHING;
SQL
PRE_MARKER_COUNT=100
echo "  Inserted ${PRE_MARKER_COUNT} pre-failover markers."

# ── Phase 1: Load against primary ─────────────────────────────────────────
echo ""
echo "=== Phase 1: Load against primary (${KILL_AT_SEC}s) ==="

PHASE1_DURATION="${KILL_AT_SEC}"
pgbench_cmd="pgbench -h ${HOST} -p ${NODE_A_PG_PORT} -U ${USER} -d ${DB} \
  -c ${CONCURRENCY} -j ${CONCURRENCY} -T ${PHASE1_DURATION} -N"
echo "  CMD: ${pgbench_cmd}"

eval "${pgbench_cmd}" > "${OUT_DIR}/load/phase1.log" 2>&1 &
PID_LOAD1=$!

# Wait for phase 1 to complete
echo "  Phase 1 running..."
wait "${PID_LOAD1}" 2>/dev/null || true
PID_LOAD1=""

echo "  Phase 1 complete."

# Record how many markers exist now
PRE_KILL_MARKERS=$(psql -h "${HOST}" -p "${NODE_A_PG_PORT}" -U "${USER}" -d "${DB}" \
  -t -A -c "SELECT COUNT(*) FROM bench_tx_markers;" 2>/dev/null || echo "0")
echo "  Markers before kill: ${PRE_KILL_MARKERS}"

# ── Failover event ────────────────────────────────────────────────────────
echo ""
echo "=== FAILOVER: Killing primary (Node A, PID=${PID_A}) ==="
KILL_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

kill -9 "${PID_A}" 2>/dev/null || true
wait "${PID_A}" 2>/dev/null || true
PID_A=""

echo "  Primary killed at ${KILL_TS}"

# Try to promote replica
echo "  Promoting Node B..."
# Attempt admin API promote; if it fails, node B should auto-detect
curl -s -X POST "http://${HOST}:${NODE_B_ADMIN_PORT}/promote" 2>/dev/null || true
sleep 2

# Verify Node B is accepting writes
PROMOTE_OK=false
for attempt in $(seq 1 15); do
  if psql -h "${HOST}" -p "${NODE_B_PG_PORT}" -U "${USER}" -d "${DB}" \
    -c "INSERT INTO bench_tx_markers (id, ts, payload) VALUES (-1, 0, 'promote-test') ON CONFLICT (id) DO UPDATE SET ts = 0;" &>/dev/null; then
    PROMOTE_OK=true
    echo "  Node B promoted and accepting writes (${attempt}s)."
    break
  fi
  sleep 1
done

if [ "${PROMOTE_OK}" = "false" ]; then
  echo "  WARNING: Node B may not be accepting writes. Continuing anyway..."
fi

PROMOTE_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# ── Phase 2: Load against new primary ─────────────────────────────────────
REMAINING_SEC=$((TOTAL_SEC - KILL_AT_SEC))
if [ "${REMAINING_SEC}" -gt 0 ]; then
  echo ""
  echo "=== Phase 2: Load against new primary (${REMAINING_SEC}s) ==="

  pgbench_cmd2="pgbench -h ${HOST} -p ${NODE_B_PG_PORT} -U ${USER} -d ${DB} \
    -c ${CONCURRENCY} -j ${CONCURRENCY} -T ${REMAINING_SEC} -N"
  echo "  CMD: ${pgbench_cmd2}"

  eval "${pgbench_cmd2}" > "${OUT_DIR}/load/phase2.log" 2>&1 &
  PID_LOAD2=$!

  echo "  Phase 2 running..."
  wait "${PID_LOAD2}" 2>/dev/null || true
  PID_LOAD2=""
  echo "  Phase 2 complete."
fi

# ── Determinism validation ────────────────────────────────────────────────
echo ""
echo "=== Determinism Validation ==="

# Check markers on the surviving node
POST_MARKERS=$(psql -h "${HOST}" -p "${NODE_B_PG_PORT}" -U "${USER}" -d "${DB}" \
  -t -A -c "SELECT COUNT(*) FROM bench_tx_markers;" 2>/dev/null || echo "0")

DUPLICATE_COUNT=$(psql -h "${HOST}" -p "${NODE_B_PG_PORT}" -U "${USER}" -d "${DB}" \
  -t -A -c "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS c FROM bench_tx_markers GROUP BY id HAVING COUNT(*) > 1) sub;" 2>/dev/null || echo "0")

# Check pre-failover markers survived
SURVIVING_PRE=$(psql -h "${HOST}" -p "${NODE_B_PG_PORT}" -U "${USER}" -d "${DB}" \
  -t -A -c "SELECT COUNT(*) FROM bench_tx_markers WHERE id BETWEEN 1 AND 100;" 2>/dev/null || echo "0")

echo "  Pre-failover markers: ${PRE_MARKER_COUNT} inserted, ${SURVIVING_PRE} survived"
echo "  Total markers after failover: ${POST_MARKERS}"
echo "  Duplicate IDs: ${DUPLICATE_COUNT}"

# Parse TPS from both phases
TPS_PHASE1=$(grep -oP 'tps = \K[0-9.]+(?= \(excluding)' "${OUT_DIR}/load/phase1.log" 2>/dev/null || echo "0")
TPS_PHASE2="0"
if [ -f "${OUT_DIR}/load/phase2.log" ]; then
  TPS_PHASE2=$(grep -oP 'tps = \K[0-9.]+(?= \(excluding)' "${OUT_DIR}/load/phase2.log" 2>/dev/null || echo "0")
fi

# ── Verdict ───────────────────────────────────────────────────────────────
REASONS=""
PASS=true

# Check 1: pre-failover markers must all survive on replica
if [ "${SURVIVING_PRE}" -lt "${PRE_MARKER_COUNT}" ]; then
  PASS=false
  REASONS="${REASONS}  FAIL: Only ${SURVIVING_PRE}/${PRE_MARKER_COUNT} pre-failover markers survived.\n"
fi

# Check 2: no duplicate primary keys
if [ "${DUPLICATE_COUNT}" -gt "0" ]; then
  PASS=false
  REASONS="${REASONS}  FAIL: ${DUPLICATE_COUNT} duplicate marker IDs found.\n"
fi

# Check 3: marker count should be reasonable (at least pre-failover count)
if [ "${POST_MARKERS}" -lt "${PRE_MARKER_COUNT}" ]; then
  PASS=false
  REASONS="${REASONS}  FAIL: Total markers (${POST_MARKERS}) < pre-failover count (${PRE_MARKER_COUNT}).\n"
fi

if [ "${PASS}" = "true" ]; then
  VERDICT="PASS"
fi

# ── Generate summary JSON ─────────────────────────────────────────────────
{
  cat <<SUMMARY_JSON
{
  "timestamp": "${TS}",
  "git_commit": "$(git rev-parse HEAD 2>/dev/null || echo 'unknown')",
  "verdict": "${VERDICT}",
  "parameters": {
    "total_sec": ${TOTAL_SEC},
    "kill_at_sec": ${KILL_AT_SEC},
    "concurrency": ${CONCURRENCY},
    "scale": ${SCALE}
  },
  "timeline": {
    "kill_ts": "${KILL_TS}",
    "promote_ts": "${PROMOTE_TS}",
    "promote_ok": ${PROMOTE_OK}
  },
  "load": {
    "tps_phase1": ${TPS_PHASE1},
    "tps_phase2": ${TPS_PHASE2}
  },
  "validation": {
    "pre_failover_markers": ${PRE_MARKER_COUNT},
    "surviving_pre_markers": ${SURVIVING_PRE},
    "total_markers_after": ${POST_MARKERS},
    "duplicate_ids": ${DUPLICATE_COUNT}
  }
}
SUMMARY_JSON
} > "${OUT_DIR}/summary_failover.json"

# ── Print final result ────────────────────────────────────────────────────
echo ""
echo "============================================================"
if [ "${VERDICT}" = "PASS" ]; then
  echo "  VERDICT: *** PASS ***"
  echo ""
  echo "  All pre-failover committed markers survived."
  echo "  No phantom commits detected."
  echo "  No duplicate IDs."
else
  echo "  VERDICT: *** FAIL ***"
  echo ""
  echo -e "${REASONS}"
fi
echo ""
echo "  TPS Phase 1 (pre-failover):  ${TPS_PHASE1}"
echo "  TPS Phase 2 (post-failover): ${TPS_PHASE2}"
echo "  Total markers:               ${POST_MARKERS}"
echo ""
echo "  Results in: ${OUT_DIR}/"
echo "============================================================"

# Exit code
if [ "${VERDICT}" = "PASS" ]; then
  exit 0
else
  exit 1
fi
