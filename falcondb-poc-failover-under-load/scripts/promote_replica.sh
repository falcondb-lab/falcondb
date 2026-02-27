#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #3 — Failover Under Load: Promote Replica
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
REPLICA_PORT=5434
DB="falcon"
USER="falcon"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

info "Promoting replica to primary..."

curl -s -X POST "http://${HOST}:8081/promote" 2>/dev/null || true
sleep 2

PROMOTE_OK=false
for attempt in $(seq 1 20); do
  if psql -h "${HOST}" -p "${REPLICA_PORT}" -U "${USER}" -d "${DB}" \
    -c "SELECT 1;" &>/dev/null; then
    PROMOTE_OK=true
    ok "Replica promoted and accepting queries (${attempt}s)"
    break
  fi
  sleep 1
done

if [ "${PROMOTE_OK}" = "false" ]; then
  fail "Replica did not become available after promotion"
  exit 1
fi

PROMOTE_TS=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "${PROMOTE_TS}" > "${OUTPUT_DIR}/promote_timestamp.txt"

echo ""
echo "  New primary: psql -h ${HOST} -p ${REPLICA_PORT} -U ${USER} -d ${DB}"
echo ""
