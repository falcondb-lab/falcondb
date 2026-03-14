#!/usr/bin/env bash
# Workload A: Write-Heavy — Runner
# Usage:
#   ./run.sh                           # defaults: falcondb, 16 clients, 60s
#   ./run.sh --target postgres --clients 32 --duration 120
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
mkdir -p "${RESULTS_DIR}"

# ── Defaults ──────────────────────────────────────────────────────────────
TARGET="falcondb"
CLIENTS=16
DURATION=60
HOST="127.0.0.1"
PORT=""
DB="falcon"
USER="falcon"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)   TARGET="$2";   shift 2 ;;
        --clients)  CLIENTS="$2";  shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --host)     HOST="$2";     shift 2 ;;
        --port)     PORT="$2";     shift 2 ;;
        --db)       DB="$2";       shift 2 ;;
        --user)     USER="$2";     shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

if [ -z "${PORT}" ]; then
    case "${TARGET}" in
        falcondb) PORT=5433 ;;
        postgres) PORT=5432 ;;
        *) PORT=5433 ;;
    esac
fi

TIMESTAMP=$(date -u +"%Y%m%dT%H%M%SZ")
RESULT_FILE="${RESULTS_DIR}/workload_a_${TARGET}_${TIMESTAMP}.json"
RAW_LOG="${RESULTS_DIR}/workload_a_${TARGET}_${TIMESTAMP}.log"

echo ""
echo "═══ Workload A: Write-Heavy ═══"
echo "  Target:     ${TARGET} (${HOST}:${PORT})"
echo "  Clients:    ${CLIENTS}"
echo "  Duration:   ${DURATION}s"
echo "  Mix:        90% INSERT / 10% UPDATE"
echo ""

# ── Fairness: enforce durability ──────────────────────────────────────────
if [ "${TARGET}" = "postgres" ]; then
    psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" -c \
        "ALTER SYSTEM SET synchronous_commit = 'on';
         ALTER SYSTEM SET fsync = 'on';
         SELECT pg_reload_conf();" >/dev/null 2>&1 || true
fi

# ── Schema setup ──────────────────────────────────────────────────────────
echo "  Setting up schema..."
psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -f "${SCRIPT_DIR}/schema.sql" >/dev/null 2>&1
echo "  ✓ Schema ready (10K seed rows)"

# ── Pre-run row count ────────────────────────────────────────────────────
PRE_COUNT=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT COUNT(*) FROM orders;")

# ── Run pgbench ──────────────────────────────────────────────────────────
echo "  Running pgbench (${DURATION}s, ${CLIENTS} clients)..."

pgbench -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -f "${SCRIPT_DIR}/workload.sql" \
    -c "${CLIENTS}" \
    -j "${CLIENTS}" \
    -T "${DURATION}" \
    -P 5 \
    --no-vacuum \
    -r \
    2>&1 | tee "${RAW_LOG}"

# ── Parse results ────────────────────────────────────────────────────────
TPS=$(grep -oP 'tps = \K[0-9.]+' "${RAW_LOG}" | tail -1 || echo "0")
LATENCY_AVG=$(grep -oP 'latency average = \K[0-9.]+' "${RAW_LOG}" || echo "0")

# Post-run row count
POST_COUNT=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT COUNT(*) FROM orders;")

INSERTED=$((POST_COUNT - PRE_COUNT))

# ── Verification ──────────────────────────────────────────────────────────
echo ""
echo "  Running verification..."
VERIFY_OUTPUT=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -f "${SCRIPT_DIR}/verify.sql" -t -A 2>&1)
echo "  ${VERIFY_OUTPUT}"

# Check PK uniqueness
DUP_COUNT=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT COUNT(*) FROM (SELECT order_id FROM orders GROUP BY order_id HAVING COUNT(*) > 1) d;")

VERIFY_PASS="true"
if [ "${DUP_COUNT}" -gt 0 ]; then
    VERIFY_PASS="false"
fi

# ── JSON output ──────────────────────────────────────────────────────────
cat > "${RESULT_FILE}" <<EOF
{
  "workload": "A",
  "name": "write_heavy",
  "target": "${TARGET}",
  "timestamp": "${TIMESTAMP}",
  "config": {
    "clients": ${CLIENTS},
    "duration_sec": ${DURATION},
    "host": "${HOST}",
    "port": ${PORT},
    "mix": "90% INSERT / 10% UPDATE",
    "durability": "fsync + synchronous_commit"
  },
  "results": {
    "tps": ${TPS},
    "latency_avg_ms": ${LATENCY_AVG},
    "rows_before": ${PRE_COUNT},
    "rows_after": ${POST_COUNT},
    "rows_inserted": ${INSERTED}
  },
  "verification": {
    "pk_unique": ${VERIFY_PASS},
    "duplicate_pks": ${DUP_COUNT}
  }
}
EOF

echo ""
echo "  ═══ Results ═══"
echo "  TPS:              ${TPS}"
echo "  Avg Latency:      ${LATENCY_AVG} ms"
echo "  Rows Inserted:    ${INSERTED}"
echo "  PK Unique:        ${VERIFY_PASS}"
echo "  Output:           ${RESULT_FILE}"
echo ""
