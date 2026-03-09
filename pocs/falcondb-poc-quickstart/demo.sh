#!/usr/bin/env bash
# ============================================================================
# FalconDB — Failover Under Load · 5-Minute Demo
# ============================================================================
#
# Usage:
#   ./demo.sh start      — Start 3-node cluster + sustained write load
#   ./demo.sh failover   — Kill the current leader (SIGKILL)
#   ./demo.sh verify     — Verify 0 data loss after failover
#   ./demo.sh all        — Run start → failover → verify end-to-end
#
# ============================================================================
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="${DEMO_DIR}/cluster"
OUTPUT_DIR="${DEMO_DIR}/output"
WORKLOAD_BIN="${DEMO_DIR}/workload/target/release/failover_writer"

export COMMIT_COUNT="${COMMIT_COUNT:-20000}"
RAMP_SEC="${RAMP_SEC:-8}"

BLUE='\033[0;34m'; GREEN='\033[0;32m'; RED='\033[0;31m'
YELLOW='\033[1;33m'; BOLD='\033[1m'; NC='\033[0m'

ts()     { date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ"; }
banner() { echo -e "\n${BLUE}${BOLD}═══  $1  ═══${NC}\n"; }
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }

# ── Cleanup helper ────────────────────────────────────────────────────────
cleanup() {
    # Kill writer if running
    if [ -f "${OUTPUT_DIR}/writer.pid" ]; then
        local pid
        pid=$(cat "${OUTPUT_DIR}/writer.pid" 2>/dev/null || true)
        if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
            touch "${OUTPUT_DIR}/stop_writer"
            sleep 2
            kill "${pid}" 2>/dev/null || true
        fi
    fi
}

# ══════════════════════════════════════════════════════════════════════════
# Phase 1: START — cluster + write load
# ══════════════════════════════════════════════════════════════════════════
cmd_start() {
    banner "Phase 1: Starting 3-Node FalconDB Cluster"

    mkdir -p "${OUTPUT_DIR}"
    rm -f "${OUTPUT_DIR}"/*.log "${OUTPUT_DIR}"/*.txt "${OUTPUT_DIR}"/*.json 2>/dev/null || true

    echo "$(ts)|DEMO_START" > "${OUTPUT_DIR}/timeline.log"

    # Build workload binary if needed
    if [ ! -f "${WORKLOAD_BIN}" ]; then
        info "Building workload writer..."
        (cd "${DEMO_DIR}/workload" && cargo build --release --quiet)
        ok "Writer built"
    fi

    # Start cluster
    info "Starting docker compose cluster..."
    docker compose -f "${CLUSTER_DIR}/docker-compose.yaml" up -d --build --wait 2>&1 | tail -5
    ok "3-node cluster started"

    echo "$(ts)|CLUSTER_UP" >> "${OUTPUT_DIR}/timeline.log"

    # Wait for all nodes healthy
    info "Waiting for cluster health..."
    local ready=0
    for attempt in $(seq 1 30); do
        local healthy=0
        for port in 8080 8081 8082; do
            if curl -sf "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
                healthy=$((healthy + 1))
            fi
        done
        if [ "${healthy}" -ge 3 ]; then
            ready=1
            break
        fi
        sleep 1
    done

    if [ "${ready}" -ne 1 ]; then
        fail "Cluster not healthy after 30s"
        exit 1
    fi
    ok "All 3 nodes healthy"

    # Create table on leader
    info "Creating tx_commits table..."
    local table_created=0
    for port in 5433 5434 5435; do
        if psql -h 127.0.0.1 -p "${port}" -U falcon -d falcon \
            -c "CREATE TABLE IF NOT EXISTS tx_commits (
                commit_id BIGINT PRIMARY KEY,
                payload TEXT NOT NULL,
                ts TIMESTAMP DEFAULT now()
            );" >/dev/null 2>&1; then
            table_created=1
            ok "Table created via port ${port}"
            break
        fi
    done
    if [ "${table_created}" -ne 1 ]; then
        fail "Cannot create table on any node"
        exit 1
    fi

    # Start writer in background
    banner "Starting Sustained Write Load (${COMMIT_COUNT} transactions)"

    export OUTPUT_DIR
    "${WORKLOAD_BIN}" &
    local writer_pid=$!
    echo "${writer_pid}" > "${OUTPUT_DIR}/writer.pid"
    ok "Writer started (PID ${writer_pid})"

    echo "$(ts)|WRITE_START|pid=${writer_pid}" >> "${OUTPUT_DIR}/timeline.log"

    # Let writes ramp up
    info "Letting writes ramp up (${RAMP_SEC}s)..."
    for i in $(seq 1 "${RAMP_SEC}"); do
        sleep 1
        local count=0
        if [ -f "${OUTPUT_DIR}/committed.log" ]; then
            count=$(wc -l < "${OUTPUT_DIR}/committed.log" | tr -d ' ')
        fi
        echo "  [${i}/${RAMP_SEC}s] ${count} committed"
    done

    local pre_count=0
    if [ -f "${OUTPUT_DIR}/committed.log" ]; then
        pre_count=$(wc -l < "${OUTPUT_DIR}/committed.log" | tr -d ' ')
    fi
    ok "Ramp complete. ${pre_count} transactions committed so far."
    echo ""
}

# ══════════════════════════════════════════════════════════════════════════
# Phase 2: FAILOVER — kill the leader
# ══════════════════════════════════════════════════════════════════════════
cmd_failover() {
    banner "Phase 2: Killing the Leader (SIGKILL)"

    echo "$(ts)|FAILOVER_START" >> "${OUTPUT_DIR}/timeline.log"

    bash "${DEMO_DIR}/chaos/kill_leader.sh"

    echo "$(ts)|LEADER_DEAD" >> "${OUTPUT_DIR}/timeline.log"

    # Wait for new leader election + writer reconnect
    info "Waiting for new leader election and writer reconnect..."

    local resumed=0
    local pre_count=0
    if [ -f "${OUTPUT_DIR}/committed.log" ]; then
        pre_count=$(wc -l < "${OUTPUT_DIR}/committed.log" | tr -d ' ')
    fi

    for attempt in $(seq 1 30); do
        sleep 1
        local cur=0
        if [ -f "${OUTPUT_DIR}/committed.log" ]; then
            cur=$(wc -l < "${OUTPUT_DIR}/committed.log" | tr -d ' ')
        fi
        if [ "${cur}" -gt "${pre_count}" ]; then
            resumed=1
            echo "$(ts)|WRITES_RESUMED|count=${cur}" >> "${OUTPUT_DIR}/timeline.log"
            ok "Writes resumed! (${cur} committed, was ${pre_count} at kill time)"
            break
        fi
        echo "  [${attempt}s] Waiting for writes to resume (still ${cur})..."
    done

    if [ "${resumed}" -ne 1 ]; then
        fail "Writes did not resume within 30s"
    fi

    # Wait for writer to finish
    info "Waiting for writer to complete all ${COMMIT_COUNT} transactions..."
    if [ -f "${OUTPUT_DIR}/writer.pid" ]; then
        local pid
        pid=$(cat "${OUTPUT_DIR}/writer.pid" 2>/dev/null || true)
        if [ -n "${pid}" ]; then
            local max_wait=120
            for i in $(seq 1 "${max_wait}"); do
                if ! kill -0 "${pid}" 2>/dev/null; then
                    break
                fi
                if [ $((i % 10)) -eq 0 ]; then
                    local c=0
                    [ -f "${OUTPUT_DIR}/committed.log" ] && c=$(wc -l < "${OUTPUT_DIR}/committed.log" | tr -d ' ')
                    echo "  [${i}/${max_wait}s] Writer running (${c} committed)"
                fi
                sleep 1
            done
            # Force stop if still going
            if kill -0 "${pid}" 2>/dev/null; then
                touch "${OUTPUT_DIR}/stop_writer"
                sleep 3
                kill "${pid}" 2>/dev/null || true
            fi
        fi
    fi

    local final_count=0
    [ -f "${OUTPUT_DIR}/committed.log" ] && final_count=$(wc -l < "${OUTPUT_DIR}/committed.log" | tr -d ' ')
    ok "Writer finished. Total committed: ${final_count}"
    echo "$(ts)|WRITE_DONE|committed=${final_count}" >> "${OUTPUT_DIR}/timeline.log"
    echo ""
}

# ══════════════════════════════════════════════════════════════════════════
# Phase 3: VERIFY — 0 data loss
# ══════════════════════════════════════════════════════════════════════════
cmd_verify() {
    banner "Phase 3: Consistency Verification"

    local committed_file="${OUTPUT_DIR}/committed.log"
    if [ ! -f "${committed_file}" ]; then
        fail "No committed.log found. Run 'start' and 'failover' first."
        exit 1
    fi

    # Count committed by client
    local client_count
    client_count=$(wc -l < "${committed_file}" | tr -d ' ')
    info "Committed by client (from log): ${client_count}"

    # Find a surviving node
    local survivor_port=""
    for port in 5433 5434 5435; do
        if psql -h 127.0.0.1 -p "${port}" -U falcon -d falcon \
            -c "SELECT 1" >/dev/null 2>&1; then
            survivor_port="${port}"
            break
        fi
    done

    if [ -z "${survivor_port}" ]; then
        fail "No surviving node reachable."
        exit 1
    fi
    info "Survivor node: 127.0.0.1:${survivor_port}"

    # Query surviving rows
    local db_count
    db_count=$(psql -h 127.0.0.1 -p "${survivor_port}" -U falcon -d falcon \
        -t -A -c "SELECT COUNT(*) FROM tx_commits;" 2>/dev/null | tr -d ' ')
    info "Rows in database: ${db_count}"

    # Extract client commit_ids and DB commit_ids
    local client_ids="${OUTPUT_DIR}/client_ids.txt"
    local db_ids="${OUTPUT_DIR}/db_ids.txt"

    grep -oP '^\d+' "${committed_file}" | sort -n > "${client_ids}"

    psql -h 127.0.0.1 -p "${survivor_port}" -U falcon -d falcon \
        -t -A -c "SELECT commit_id FROM tx_commits ORDER BY commit_id;" \
        > "${db_ids}" 2>/dev/null

    # Missing: committed by client but not in DB (= data loss)
    local missing_count
    missing_count=$(comm -23 "${client_ids}" "${db_ids}" | wc -l | tr -d ' ')

    # Phantom: in DB but not committed by client
    local phantom_count
    phantom_count=$(comm -13 "${client_ids}" "${db_ids}" | wc -l | tr -d ' ')

    # Duplicates in DB
    local dup_count
    dup_count=$(sort "${db_ids}" | uniq -d | wc -l | tr -d ' ')

    # Failover duration from timeline
    local failover_dur="N/A"
    if [ -f "${OUTPUT_DIR}/timeline.log" ]; then
        local kill_line
        kill_line=$(grep "LEADER_KILLED\|LEADER_DEAD" "${OUTPUT_DIR}/timeline.log" | tail -1 || true)
        local resume_line
        resume_line=$(grep "WRITES_RESUMED" "${OUTPUT_DIR}/timeline.log" | head -1 || true)

        if [ -n "${kill_line}" ] && [ -n "${resume_line}" ]; then
            local kill_ts
            kill_ts=$(echo "${kill_line}" | cut -d'|' -f1)
            local resume_ts
            resume_ts=$(echo "${resume_line}" | cut -d'|' -f1)
            # Calculate diff in seconds using date
            local kill_epoch resume_epoch
            kill_epoch=$(date -d "${kill_ts}" +%s 2>/dev/null || echo "0")
            resume_epoch=$(date -d "${resume_ts}" +%s 2>/dev/null || echo "0")
            if [ "${kill_epoch}" -gt 0 ] && [ "${resume_epoch}" -gt 0 ]; then
                failover_dur="$((resume_epoch - kill_epoch))s"
            fi
        fi
    fi

    # Verdict
    local verdict="PASS"
    if [ "${missing_count}" -gt 0 ] || [ "${phantom_count}" -gt 0 ] || [ "${dup_count}" -gt 0 ]; then
        verdict="FAIL"
    fi

    # ── result.txt ─────────────────────────────────────────────────────
    local result_file="${OUTPUT_DIR}/result.txt"
    {
        echo "FAILOVER UNDER LOAD RESULT"
        echo "--------------------------"
        echo "Committed before failover: ${client_count}"
        echo "Committed after failover:  ${db_count}"
        echo "Missing commits:           ${missing_count}"
        echo "Phantom commits:           ${phantom_count}"
        echo "Duplicates:                ${dup_count}"
        echo "Failover duration:         ${failover_dur}"
        echo ""
        echo "RESULT: ${verdict} (${missing_count} data loss)"
    } | tee "${result_file}"

    echo ""
    echo "$(ts)|VERIFY|verdict=${verdict}|missing=${missing_count}" >> "${OUTPUT_DIR}/timeline.log"

    # Final banner
    echo ""
    if [ "${verdict}" = "PASS" ]; then
        echo -e "  ${GREEN}${BOLD}════════════════════════════════════════════════════════${NC}"
        echo -e "  ${GREEN}${BOLD}  PASS — 0 data loss under failover.${NC}"
        echo -e "  ${GREEN}${BOLD}  All ${client_count} committed transactions survived.${NC}"
        echo -e "  ${GREEN}${BOLD}════════════════════════════════════════════════════════${NC}"
    else
        echo -e "  ${RED}${BOLD}════════════════════════════════════════════════════════${NC}"
        echo -e "  ${RED}${BOLD}  FAIL — Data integrity violation detected.${NC}"
        echo -e "  ${RED}${BOLD}════════════════════════════════════════════════════════${NC}"
    fi
    echo ""
    info "Timeline:  ${OUTPUT_DIR}/timeline.log"
    info "Result:    ${result_file}"
    echo ""

    [ "${verdict}" = "PASS" ] && return 0 || return 1
}

# ══════════════════════════════════════════════════════════════════════════
# Phase ALL: start → failover → verify
# ══════════════════════════════════════════════════════════════════════════
cmd_all() {
    echo ""
    echo -e "${BOLD}FalconDB — Failover Under Load · 5-Minute Demo${NC}"
    echo ""
    echo "  This demo will:"
    echo "    1. Start a 3-node Raft cluster"
    echo "    2. Run ${COMMIT_COUNT} concurrent transactions"
    echo "    3. SIGKILL the leader during peak writes"
    echo "    4. Wait for automatic recovery"
    echo "    5. Verify 0 data loss"
    echo ""

    trap cleanup EXIT

    cmd_start
    cmd_failover
    cmd_verify
}

# ══════════════════════════════════════════════════════════════════════════
# Teardown
# ══════════════════════════════════════════════════════════════════════════
cmd_teardown() {
    banner "Teardown"
    cleanup
    docker compose -f "${CLUSTER_DIR}/docker-compose.yaml" down -v 2>/dev/null || true
    ok "Cluster stopped and volumes removed"
}

# ══════════════════════════════════════════════════════════════════════════
# Dispatch
# ══════════════════════════════════════════════════════════════════════════
case "${1:-help}" in
    start)    cmd_start ;;
    failover) cmd_failover ;;
    verify)   cmd_verify ;;
    all)      cmd_all ;;
    teardown) cmd_teardown ;;
    *)
        echo "Usage: $0 {start|failover|verify|all|teardown}"
        echo ""
        echo "  start      Start 3-node cluster + sustained write load"
        echo "  failover   Kill the current leader (SIGKILL)"
        echo "  verify     Verify 0 data loss after failover"
        echo "  all        Full demo: start → failover → verify"
        echo "  teardown   Stop cluster and clean up"
        exit 1
        ;;
esac
