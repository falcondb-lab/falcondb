#!/usr/bin/env bash
# Kill the current Raft leader via SIGKILL. No graceful shutdown. No flush.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${DEMO_ROOT}/output"
mkdir -p "${OUTPUT_DIR}"

RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; BOLD='\033[1m'; NC='\033[0m'

# ── Detect current leader ─────────────────────────────────────────────────
# Query each node's admin API to find who is the Raft leader.
CONTAINERS=("falcon-node1" "falcon-node2" "falcon-node3")
ADMIN_PORTS=(8080 8081 8082)
PG_PORTS=(5433 5434 5435)

LEADER_CONTAINER=""
LEADER_PG_PORT=""
LEADER_IDX=""

for idx in 0 1 2; do
    port="${ADMIN_PORTS[$idx]}"
    container="${CONTAINERS[$idx]}"

    # Try admin /health endpoint — leader responds with role=leader
    ROLE=$(curl -sf "http://127.0.0.1:${port}/health" 2>/dev/null | grep -oi '"role"\s*:\s*"leader"' || true)
    if [ -n "${ROLE}" ]; then
        LEADER_CONTAINER="${container}"
        LEADER_PG_PORT="${PG_PORTS[$idx]}"
        LEADER_IDX=$idx
        break
    fi

    # Fallback: try writing to detect leader (writable = leader)
    if psql -h 127.0.0.1 -p "${PG_PORTS[$idx]}" -U falcon -d falcon \
        -c "SELECT 1" >/dev/null 2>&1; then
        # Try a write — only leader accepts writes
        if psql -h 127.0.0.1 -p "${PG_PORTS[$idx]}" -U falcon -d falcon \
            -c "CREATE TABLE IF NOT EXISTS _leader_probe (x INT); DROP TABLE IF EXISTS _leader_probe;" >/dev/null 2>&1; then
            LEADER_CONTAINER="${container}"
            LEADER_PG_PORT="${PG_PORTS[$idx]}"
            LEADER_IDX=$idx
            break
        fi
    fi
done

if [ -z "${LEADER_CONTAINER}" ]; then
    echo -e "  ${RED}✗${NC} Cannot detect leader. Are the nodes running?"
    exit 1
fi

echo -e "  ${YELLOW}→${NC} Detected leader: ${BOLD}${LEADER_CONTAINER}${NC} (pg port ${LEADER_PG_PORT})"

# Record pre-kill committed count
PRE_KILL=0
if [ -f "${OUTPUT_DIR}/committed.log" ]; then
    PRE_KILL=$(wc -l < "${OUTPUT_DIR}/committed.log" | tr -d ' ')
fi

KILL_TS=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")

echo ""
echo -e "  ${RED}${BOLD}>>> docker kill --signal=KILL ${LEADER_CONTAINER} <<<${NC}"
echo ""
echo -e "  ${YELLOW}→${NC} Committed before crash: ${PRE_KILL}"
echo -e "  ${YELLOW}→${NC} No graceful shutdown. No final flush. No signal handler."
echo ""

docker kill --signal=KILL "${LEADER_CONTAINER}"

echo -e "  ${GREEN}✓${NC} Leader killed at ${KILL_TS}"

# Write timeline entry
{
    echo "leader_container=${LEADER_CONTAINER}"
    echo "leader_pg_port=${LEADER_PG_PORT}"
    echo "kill_timestamp=${KILL_TS}"
    echo "committed_before_kill=${PRE_KILL}"
} > "${OUTPUT_DIR}/kill_info.txt"

echo "${KILL_TS}|LEADER_KILLED|${LEADER_CONTAINER}|committed=${PRE_KILL}" >> "${OUTPUT_DIR}/timeline.log"
echo ""
