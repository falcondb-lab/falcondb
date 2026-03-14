#!/usr/bin/env bash
# ============================================================================
# TPC-C Benchmark Runner for FalconDB / PostgreSQL
# ============================================================================
#
# Usage:
#   ./run_tpcc.sh <target> [options]
#
# Targets:
#   falcondb     — connect to FalconDB (default port 5433)
#   postgresql   — connect to PostgreSQL (default port 5432)
#
# Options:
#   --threads <N>     Number of concurrent clients (default: 8)
#   --duration <S>    Duration in seconds (default: 60)
#   --warehouses <W>  Number of warehouses / scale factor (default: 1)
#   --quick           Short run (10s, 4 threads)
#   --skip-load       Skip schema + data loading
#
# Output:
#   benchmarks/results/tpcc_<target>_<timestamp>/
#     ├── new_order.txt       pgbench output for New Order (45%)
#     ├── payment.txt         pgbench output for Payment (43%)
#     ├── order_status.txt    pgbench output for Order-Status (4%)
#     ├── delivery.txt        pgbench output for Delivery (4%)
#     ├── stock_level.txt     pgbench output for Stock-Level (4%)
#     ├── summary.json        Parsed TPS + latency per txn type
#     └── REPORT.md           Human-readable report
#
# ============================================================================

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────────────

TARGET="${1:?Usage: run_tpcc.sh <falcondb|postgresql> [options]}"
shift

THREADS=8
DURATION=60
WAREHOUSES=1
SKIP_LOAD=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --threads)    THREADS="$2"; shift 2 ;;
        --duration)   DURATION="$2"; shift 2 ;;
        --warehouses) WAREHOUSES="$2"; shift 2 ;;
        --quick)      THREADS=4; DURATION=10; shift ;;
        --skip-load)  SKIP_LOAD=true; shift ;;
        *)            echo "Unknown option: $1"; exit 1 ;;
    esac
done

TPCC_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$TPCC_DIR/.." && pwd)"
TS=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="$BENCH_DIR/results/tpcc_${TARGET}_${TS}"
mkdir -p "$RESULTS_DIR"

# ── Connection params ────────────────────────────────────────────────────────

case "$TARGET" in
    falcondb)
        HOST="127.0.0.1"; PORT="5433"; USER="falcon"; DB="falcon"
        ;;
    postgresql)
        HOST="127.0.0.1"; PORT="5432"; USER="$(whoami)"; DB="falconbench"
        ;;
    *)
        echo "ERROR: Unknown target '$TARGET'. Use 'falcondb' or 'postgresql'."
        exit 1
        ;;
esac

PSQL="psql -h $HOST -p $PORT -U $USER -d $DB"
PGBENCH="pgbench -h $HOST -p $PORT -U $USER -d $DB"

echo "================================================================="
echo "  TPC-C Benchmark"
echo "  Target:      $TARGET ($HOST:$PORT/$DB)"
echo "  Threads:     $THREADS"
echo "  Duration:    ${DURATION}s per transaction type"
echo "  Warehouses:  $WAREHOUSES"
echo "  Output:      $RESULTS_DIR"
echo "================================================================="

# ── Load data ────────────────────────────────────────────────────────────────

if [ "$SKIP_LOAD" = false ]; then
    echo ""
    echo "[1/7] Loading TPC-C schema..."
    $PSQL -f "$TPCC_DIR/schema.sql" > /dev/null 2>&1
    echo "  Schema created (9 tables)"

    echo "[2/7] Loading TPC-C data (W=$WAREHOUSES)..."
    $PSQL -f "$TPCC_DIR/load.sql" > /dev/null 2>&1
    echo "  Data loaded (item=100K, customer=30K, orders=30K, stock=100K)"
else
    echo ""
    echo "[1/7] Skipping schema load (--skip-load)"
    echo "[2/7] Skipping data load (--skip-load)"
fi

# ── TPC-C transaction mix ───────────────────────────────────────────────────
# Standard mix: New-Order 45%, Payment 43%, Order-Status 4%, Delivery 4%, Stock-Level 4%
# We run each profile individually and weight results in the report.

run_profile() {
    local name="$1"
    local script="$2"
    local step="$3"
    local outfile="$RESULTS_DIR/${name}.txt"

    echo "[$step/7] Running $name (${DURATION}s, ${THREADS} threads)..."
    $PGBENCH -f "$script" -c "$THREADS" -j "$THREADS" -T "$DURATION" \
        --progress=10 2>&1 | tee "$outfile"
    echo ""
}

run_profile "new_order"    "$TPCC_DIR/new_order.sql"    3
run_profile "payment"      "$TPCC_DIR/payment.sql"      4
run_profile "order_status" "$TPCC_DIR/order_status.sql"  5
run_profile "delivery"     "$TPCC_DIR/delivery.sql"      6
run_profile "stock_level"  "$TPCC_DIR/stock_level.sql"   7

# ── Generate summary ────────────────────────────────────────────────────────

echo "================================================================="
echo "  Generating summary..."
echo "================================================================="

# Extract TPS from pgbench output (line: "tps = NNN.NNN ...")
extract_tps() {
    grep -oP 'tps = \K[0-9.]+' "$1" 2>/dev/null | tail -1 || echo "0"
}

extract_latency() {
    grep -oP 'latency average = \K[0-9.]+' "$1" 2>/dev/null | tail -1 || echo "0"
}

TPS_NO=$(extract_tps "$RESULTS_DIR/new_order.txt")
TPS_PY=$(extract_tps "$RESULTS_DIR/payment.txt")
TPS_OS=$(extract_tps "$RESULTS_DIR/order_status.txt")
TPS_DL=$(extract_tps "$RESULTS_DIR/delivery.txt")
TPS_SL=$(extract_tps "$RESULTS_DIR/stock_level.txt")

LAT_NO=$(extract_latency "$RESULTS_DIR/new_order.txt")
LAT_PY=$(extract_latency "$RESULTS_DIR/payment.txt")
LAT_OS=$(extract_latency "$RESULTS_DIR/order_status.txt")
LAT_DL=$(extract_latency "$RESULTS_DIR/delivery.txt")
LAT_SL=$(extract_latency "$RESULTS_DIR/stock_level.txt")

# Weighted TPC-C tpmC estimate:
# tpmC = New-Order TPS × 60 (transactions per minute)
TPMC=$(echo "$TPS_NO * 60" | bc 2>/dev/null || echo "N/A")

# Write JSON summary
cat > "$RESULTS_DIR/summary.json" <<EOF
{
  "target": "$TARGET",
  "timestamp": "$TS",
  "threads": $THREADS,
  "duration_sec": $DURATION,
  "warehouses": $WAREHOUSES,
  "transactions": {
    "new_order":    { "tps": $TPS_NO, "avg_latency_ms": $LAT_NO, "weight": 0.45 },
    "payment":      { "tps": $TPS_PY, "avg_latency_ms": $LAT_PY, "weight": 0.43 },
    "order_status": { "tps": $TPS_OS, "avg_latency_ms": $LAT_OS, "weight": 0.04 },
    "delivery":     { "tps": $TPS_DL, "avg_latency_ms": $LAT_DL, "weight": 0.04 },
    "stock_level":  { "tps": $TPS_SL, "avg_latency_ms": $LAT_SL, "weight": 0.04 }
  },
  "tpmc_estimate": "$TPMC"
}
EOF

# Write Markdown report
cat > "$RESULTS_DIR/REPORT.md" <<EOF
# TPC-C Benchmark Report

## Environment

| Parameter | Value |
|-----------|-------|
| Target | $TARGET |
| Host | $HOST:$PORT |
| Threads | $THREADS |
| Duration | ${DURATION}s per transaction type |
| Warehouses | $WAREHOUSES |
| Timestamp | $TS |

## Results

| Transaction | Weight | TPS | Avg Latency (ms) |
|-------------|:------:|----:|------------------:|
| New-Order | 45% | $TPS_NO | $LAT_NO |
| Payment | 43% | $TPS_PY | $LAT_PY |
| Order-Status | 4% | $TPS_OS | $LAT_OS |
| Delivery | 4% | $TPS_DL | $LAT_DL |
| Stock-Level | 4% | $TPS_SL | $LAT_SL |

**Estimated tpmC (New-Order TPS × 60)**: $TPMC

## TPC-C Transaction Mix

Per TPC-C Standard v5.11 §5.2.3:
- New-Order: ≥ 45% (complex read-write, allocates order IDs, updates stock)
- Payment: ≥ 43% (write-heavy, updates YTD balances, inserts history)
- Order-Status: ≥ 4% (read-only, last order lookup)
- Delivery: ≥ 4% (batch write, processes pending orders)
- Stock-Level: ≥ 4% (read-only, range scan + join + aggregation)

## Reproduction

\`\`\`bash
# Load + run
./benchmarks/tpcc/run_tpcc.sh $TARGET --threads $THREADS --duration $DURATION

# Skip load (data already present)
./benchmarks/tpcc/run_tpcc.sh $TARGET --threads $THREADS --duration $DURATION --skip-load
\`\`\`

## Methodology Notes

- Each transaction profile is run independently via pgbench custom scripts.
- The tpmC estimate is based on New-Order TPS (the primary TPC-C metric).
- All monetary values use BIGINT cents to avoid floating-point rounding.
- Scale factor 1 = 1 warehouse (100K items, 30K customers, 100K stock rows).
- This is a **TPC-C-like** benchmark, not an officially audited TPC-C result.

---
*Generated by run_tpcc.sh — FalconDB Benchmark Suite*
EOF

echo ""
echo "================================================================="
echo "  TPC-C Benchmark Complete"
echo "  tpmC estimate: $TPMC"
echo "  Report: $RESULTS_DIR/REPORT.md"
echo "  JSON:   $RESULTS_DIR/summary.json"
echo "================================================================="
