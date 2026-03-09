#!/usr/bin/env bash
# ============================================================================
# FalconDB DCG PoC — Run Workload
# ============================================================================
# Starts the order writer against the primary node.
# Supports both Rust binary and Python fallback.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

export FALCON_HOST="${FALCON_HOST:-127.0.0.1}"
export FALCON_PORT="${FALCON_PORT:-5433}"
export FALCON_DB="${FALCON_DB:-falcon}"
export FALCON_USER="${FALCON_USER:-falcon}"
export ORDER_COUNT="${ORDER_COUNT:-1000}"
export OUTPUT_DIR="${OUTPUT_DIR}"
export START_ORDER_ID="${START_ORDER_ID:-1}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

mkdir -p "${OUTPUT_DIR}"

# Clear previous committed log
> "${OUTPUT_DIR}/committed_orders.log"

RUST_BIN="${POC_ROOT}/workload/target/release/order_writer"

if [ -f "${RUST_BIN}" ]; then
  info "Using Rust workload generator"
  "${RUST_BIN}"
elif command -v python3 &>/dev/null && python3 -c "import psycopg2" 2>/dev/null; then
  info "Using Python workload generator"
  python3 "${POC_ROOT}/workload/order_writer.py"
else
  # Fallback: pure psql
  info "Using psql fallback (no Rust binary or psycopg2 found)"

  COMMITTED=0
  FAILED=0

  for i in $(seq 1 ${ORDER_COUNT}); do
    ORDER_ID=$((START_ORDER_ID + i - 1))
    PAYLOAD="order-$(printf '%08d' ${ORDER_ID})"

    RESULT=$(psql -h "${FALCON_HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
      -t -A -c "
        BEGIN;
        INSERT INTO orders (order_id, payload) VALUES (${ORDER_ID}, '${PAYLOAD}');
        COMMIT;
        SELECT ${ORDER_ID};
      " 2>/dev/null) || true

    if [ "${RESULT}" = "${ORDER_ID}" ]; then
      echo "${ORDER_ID}" >> "${OUTPUT_DIR}/committed_orders.log"
      COMMITTED=$((COMMITTED + 1))
    else
      FAILED=$((FAILED + 1))
    fi

    if [ $((i % 500)) -eq 0 ] || [ "${i}" -eq "${ORDER_COUNT}" ]; then
      info "Progress: ${i}/${ORDER_COUNT} (committed=${COMMITTED}, failed=${FAILED})"
    fi
  done

  # Write summary JSON
  cat > "${OUTPUT_DIR}/workload_summary.json" <<EOF
{
  "orders_attempted": ${ORDER_COUNT},
  "orders_committed": ${COMMITTED},
  "orders_failed": ${FAILED},
  "first_order_id": ${START_ORDER_ID},
  "last_committed_order_id": $((START_ORDER_ID + ORDER_COUNT - 1))
}
EOF
fi

COMMITTED_COUNT=$(wc -l < "${OUTPUT_DIR}/committed_orders.log" | tr -d ' ')
ok "Workload complete: ${COMMITTED_COUNT} orders committed"
ok "Log: ${OUTPUT_DIR}/committed_orders.log"
