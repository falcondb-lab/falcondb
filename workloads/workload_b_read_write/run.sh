#!/usr/bin/env bash
# Workload B: Read-Write OLTP — Runner
# Usage:
#   ./run.sh                           # defaults: falcondb, 16 clients, 60s
#   ./run.sh --target postgres --clients 32 --duration 120
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
mkdir -p "${RESULTS_DIR}"

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
RESULT_FILE="${RESULTS_DIR}/workload_b_${TARGET}_${TIMESTAMP}.json"
RAW_LOG="${RESULTS_DIR}/workload_b_${TARGET}_${TIMESTAMP}.log"

echo ""
echo "═══ Workload B: Read-Write OLTP ═══"
echo "  Target:     ${TARGET} (${HOST}:${PORT})"
echo "  Clients:    ${CLIENTS}"
echo "  Duration:   ${DURATION}s"
echo "  Mix:        50% SELECT / 25% INSERT / 25% UPDATE"
echo ""

# Fairness: enforce durability on PG
if [ "${TARGET}" = "postgres" ]; then
    psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" -c \
        "ALTER SYSTEM SET synchronous_commit = 'on';
         ALTER SYSTEM SET fsync = 'on';
         SELECT pg_reload_conf();" >/dev/null 2>&1 || true
fi

# Schema
echo "  Setting up schema..."
psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -f "${SCRIPT_DIR}/schema.sql" >/dev/null 2>&1
echo "  ✓ Schema ready (50K users, 100K orders)"

# Snapshot pre-run state
PRE_USERS=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT COUNT(*) FROM users;")
PRE_ORDERS=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT COUNT(*) FROM orders;")
PRE_BALANCE=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT SUM(balance) FROM users;")

# Run
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

# Parse
TPS=$(grep -oP 'tps = \K[0-9.]+' "${RAW_LOG}" | tail -1 || echo "0")
LATENCY_AVG=$(grep -oP 'latency average = \K[0-9.]+' "${RAW_LOG}" || echo "0")

# Post-run state
POST_USERS=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT COUNT(*) FROM users;")
POST_ORDERS=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT COUNT(*) FROM orders;")
POST_BALANCE=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT SUM(balance) FROM users;")

NEW_ORDERS=$((POST_ORDERS - PRE_ORDERS))

# Verify
echo ""
echo "  Running verification..."
psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -f "${SCRIPT_DIR}/verify.sql" -t -A 2>&1 | head -20

DUP_USERS=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT COUNT(*) FROM (SELECT user_id FROM users GROUP BY user_id HAVING COUNT(*) > 1) d;")
DUP_ORDERS=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT COUNT(*) FROM (SELECT order_id FROM orders GROUP BY order_id HAVING COUNT(*) > 1) d;")

VERIFY_PASS="true"
[ "${DUP_USERS}" -gt 0 ] && VERIFY_PASS="false"
[ "${DUP_ORDERS}" -gt 0 ] && VERIFY_PASS="false"

# JSON
cat > "${RESULT_FILE}" <<EOF
{
  "workload": "B",
  "name": "read_write_oltp",
  "target": "${TARGET}",
  "timestamp": "${TIMESTAMP}",
  "config": {
    "clients": ${CLIENTS},
    "duration_sec": ${DURATION},
    "host": "${HOST}",
    "port": ${PORT},
    "mix": "50% SELECT / 25% INSERT / 25% UPDATE",
    "durability": "fsync + synchronous_commit"
  },
  "results": {
    "tps": ${TPS},
    "latency_avg_ms": ${LATENCY_AVG},
    "users_before": ${PRE_USERS},
    "users_after": ${POST_USERS},
    "orders_before": ${PRE_ORDERS},
    "orders_after": ${POST_ORDERS},
    "new_orders": ${NEW_ORDERS},
    "balance_before": ${PRE_BALANCE},
    "balance_after": ${POST_BALANCE}
  },
  "verification": {
    "pk_unique_users": $([ "${DUP_USERS}" -eq 0 ] && echo "true" || echo "false"),
    "pk_unique_orders": $([ "${DUP_ORDERS}" -eq 0 ] && echo "true" || echo "false"),
    "pass": ${VERIFY_PASS}
  }
}
EOF

echo ""
echo "  ═══ Results ═══"
echo "  TPS:              ${TPS}"
echo "  Avg Latency:      ${LATENCY_AVG} ms"
echo "  New Orders:       ${NEW_ORDERS}"
echo "  PK Unique:        ${VERIFY_PASS}"
echo "  Output:           ${RESULT_FILE}"
echo ""
