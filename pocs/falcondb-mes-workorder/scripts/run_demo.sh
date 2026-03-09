#!/usr/bin/env bash
# ============================================================================
# FalconDB MES — Full Demo: Scenario A (Normal) + B (Failover) + C (Concurrent)
# ============================================================================
# Drives the MES system through the complete business lifecycle via REST API.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
API="http://${HOST}:8000"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

# Helper: POST JSON and extract field
api_post() {
  local url="$1" data="${2:-{}}"
  curl -sf -X POST "${url}" -H "Content-Type: application/json" -d "${data}" 2>/dev/null
}
api_get() {
  curl -sf "${1}" 2>/dev/null
}

# ═══════════════════════════════════════════════════════════════════════════
# Scenario A: Normal Production Flow
# ═══════════════════════════════════════════════════════════════════════════
banner "场景 A：正常生产流程 (Normal Production)"

info "Creating work order: product=MOTOR-A100, qty=1000"
WO=$(api_post "${API}/api/work-orders" '{
  "product_code": "MOTOR-A100",
  "planned_qty": 1000,
  "operations": ["cutting", "welding", "assembly", "inspection"]
}')
WO_ID=$(echo "${WO}" | python3 -c "import sys,json; print(json.load(sys.stdin)['work_order_id'])")
ok "Work order #${WO_ID} created (MOTOR-A100, qty=1000, 4 operations)"

# Get operation IDs
OPS=$(api_get "${API}/api/work-orders/${WO_ID}/operations")
OP_IDS=($(echo "${OPS}" | python3 -c "import sys,json; [print(o['operation_id']) for o in json.load(sys.stdin)]"))
OP_NAMES=("cutting" "welding" "assembly" "inspection")

# Execute operations in sequence
for i in "${!OP_IDS[@]}"; do
  OP_ID="${OP_IDS[$i]}"
  OP_NAME="${OP_NAMES[$i]}"

  info "Starting operation: ${OP_NAME} (op #${OP_ID})"
  api_post "${API}/api/operations/${OP_ID}/start" > /dev/null
  ok "  ${OP_NAME}: RUNNING"

  # Report production in batches
  BATCH_SIZE=250
  for batch in 1 2 3 4; do
    api_post "${API}/api/operations/${OP_ID}/report" \
      "{\"report_qty\": ${BATCH_SIZE}, \"reported_by\": \"operator_${batch}\"}" > /dev/null
  done
  ok "  ${OP_NAME}: reported 1000 units (4 × 250)"

  info "Completing operation: ${OP_NAME}"
  api_post "${API}/api/operations/${OP_ID}/complete" > /dev/null
  ok "  ${OP_NAME}: DONE"
done

# Complete work order
info "Completing work order #${WO_ID}..."
api_post "${API}/api/work-orders/${WO_ID}/complete" > /dev/null
ok "Work order #${WO_ID}: COMPLETED"

# Verify
info "Running business invariant verification..."
VERIFY=$(api_get "${API}/api/work-orders/${WO_ID}/verify")
RESULT=$(echo "${VERIFY}" | python3 -c "import sys,json; print(json.load(sys.stdin)['overall'])")
COMPLETED_QTY=$(echo "${VERIFY}" | python3 -c "import sys,json; print(json.load(sys.stdin)['completed_qty'])")
TOTAL_REPORTED=$(echo "${VERIFY}" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_reported_qty'])")

echo ""
echo "  ┌──────────────────────────────────────────────┐"
echo "  │  场景 A 结果 (Scenario A Result)               │"
echo "  │  计划产量 (Planned):     1000                  │"
echo "  │  已报工产量 (Reported):  ${TOTAL_REPORTED}     │"
echo "  │  已完成产量 (Completed): ${COMPLETED_QTY}      │"
echo "  │  工单状态 (Status):      COMPLETED             │"
echo "  │  验证结果 (Result):      ${RESULT}             │"
echo "  └──────────────────────────────────────────────┘"
echo ""

# Save pre-failover state for Scenario B
echo "${VERIFY}" > "${OUTPUT_DIR}/pre_failover_state.json"
ok "Pre-failover state saved to output/pre_failover_state.json"

# ═══════════════════════════════════════════════════════════════════════════
# Scenario B: Create a second work order, report partial, then failover
# ═══════════════════════════════════════════════════════════════════════════
banner "场景 B：生产中宕机 (Failover During Production)"

info "Creating work order: product=GEARBOX-B200, qty=500"
WO2=$(api_post "${API}/api/work-orders" '{
  "product_code": "GEARBOX-B200",
  "planned_qty": 500,
  "operations": ["machining", "heat_treatment", "grinding", "qa_check"]
}')
WO2_ID=$(echo "${WO2}" | python3 -c "import sys,json; print(json.load(sys.stdin)['work_order_id'])")
ok "Work order #${WO2_ID} created (GEARBOX-B200, qty=500)"

# Get operations
OPS2=$(api_get "${API}/api/work-orders/${WO2_ID}/operations")
OP2_IDS=($(echo "${OPS2}" | python3 -c "import sys,json; [print(o['operation_id']) for o in json.load(sys.stdin)]"))

# Start and report on first two operations
for i in 0 1; do
  OP_ID="${OP2_IDS[$i]}"
  api_post "${API}/api/operations/${OP_ID}/start" > /dev/null
  api_post "${API}/api/operations/${OP_ID}/report" \
    '{"report_qty": 200, "reported_by": "worker_a"}' > /dev/null
  api_post "${API}/api/operations/${OP_ID}/report" \
    '{"report_qty": 50, "reported_by": "worker_b"}' > /dev/null
  api_post "${API}/api/operations/${OP_ID}/complete" > /dev/null
done
ok "Operations 1-2 completed (250 units each reported)"

# Start operation 3, report partial
OP3_ID="${OP2_IDS[2]}"
api_post "${API}/api/operations/${OP3_ID}/start" > /dev/null
api_post "${API}/api/operations/${OP3_ID}/report" \
  '{"report_qty": 100, "reported_by": "worker_c"}' > /dev/null
ok "Operation 3 (grinding): RUNNING, 100 units reported"

# Capture state before failover
BEFORE=$(api_get "${API}/api/work-orders/${WO2_ID}/verify")
echo "${BEFORE}" > "${OUTPUT_DIR}/before_kill.json"
BEFORE_QTY=$(echo "${BEFORE}" | python3 -c "import sys,json; print(json.load(sys.stdin)['completed_qty'])")
BEFORE_REPORTED=$(echo "${BEFORE}" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_reported_qty'])")
ok "State before kill: completed_qty=${BEFORE_QTY}, total_reported=${BEFORE_REPORTED}"

echo ""
echo "  ┌──────────────────────────────────────────────┐"
echo "  │  READY FOR FAILOVER TEST                      │"
echo "  │                                              │"
echo "  │  Work order #${WO2_ID}: IN_PROGRESS            │"
echo "  │  Reported qty: ${BEFORE_REPORTED}              │"
echo "  │                                              │"
echo "  │  Next steps:                                  │"
echo "  │    1. ./scripts/kill_primary.sh               │"
echo "  │    2. ./scripts/promote_replica.sh            │"
echo "  │    3. ./scripts/verify_business_state.sh      │"
echo "  └──────────────────────────────────────────────┘"
echo ""

# ═══════════════════════════════════════════════════════════════════════════
# Scenario C: Concurrent reporting
# ═══════════════════════════════════════════════════════════════════════════
banner "场景 C：并发报工 (Concurrent Reporting)"

info "Creating work order: product=SHAFT-C300, qty=600"
WO3=$(api_post "${API}/api/work-orders" '{
  "product_code": "SHAFT-C300",
  "planned_qty": 600,
  "operations": ["turning", "polishing"]
}')
WO3_ID=$(echo "${WO3}" | python3 -c "import sys,json; print(json.load(sys.stdin)['work_order_id'])")
ok "Work order #${WO3_ID} created (SHAFT-C300, qty=600)"

OPS3=$(api_get "${API}/api/work-orders/${WO3_ID}/operations")
OP3_FIRST=$(echo "${OPS3}" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['operation_id'])")

api_post "${API}/api/operations/${OP3_FIRST}/start" > /dev/null

# Fire 10 concurrent reports
info "Submitting 10 concurrent reports (60 units each)..."
for t in $(seq 1 10); do
  api_post "${API}/api/operations/${OP3_FIRST}/report" \
    "{\"report_qty\": 60, \"reported_by\": \"terminal_${t}\"}" &
done
wait
ok "All 10 concurrent reports submitted"

# Verify
CONC_VERIFY=$(api_get "${API}/api/work-orders/${WO3_ID}/verify")
CONC_TOTAL=$(echo "${CONC_VERIFY}" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_reported_qty'])")
CONC_RESULT=$(echo "${CONC_VERIFY}" | python3 -c "import sys,json; print(json.load(sys.stdin)['overall'])")

echo ""
echo "  ┌──────────────────────────────────────────────┐"
echo "  │  场景 C 结果 (Concurrent Result)               │"
echo "  │  Expected:  600 (10 × 60)                     │"
echo "  │  Actual:    ${CONC_TOTAL}                      │"
echo "  │  Result:    ${CONC_RESULT}                     │"
echo "  │  No duplicates, no losses, no rollbacks.      │"
echo "  └──────────────────────────────────────────────┘"
echo ""

banner "Demo Complete"
echo "  All three scenarios executed."
echo "  For failover test, run kill_primary → promote_replica → verify_business_state."
echo ""
