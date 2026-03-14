#!/usr/bin/env bash
# ============================================================================
# FalconDB Benchmark — One-Click Full Comparison
# ============================================================================
#
# Runs all workloads against both FalconDB and PostgreSQL, then generates
# a comparison report.
#
# Prerequisites:
#   - cargo build --release -p falcon_server
#   - PostgreSQL installed (initdb, pg_ctl, pgbench, psql on PATH)
#
# Usage:
#   chmod +x benchmarks/scripts/run_all.sh
#   ./benchmarks/scripts/run_all.sh
#   ./benchmarks/scripts/run_all.sh --quick   # 10s per run instead of 60s
# ============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BENCH_DIR="$REPO_ROOT/benchmarks"
SCRIPTS_DIR="$BENCH_DIR/scripts"
RESULTS_DIR="$BENCH_DIR/results"

DURATION=60
if [ "${1:-}" = "--quick" ]; then
    DURATION=10
    echo "Quick mode: ${DURATION}s per workload"
fi

TIMESTAMP=$(date +%Y%m%dT%H%M%S)

echo "================================================================="
echo " FalconDB vs PostgreSQL — Full Benchmark Suite"
echo " Timestamp: $TIMESTAMP"
echo " Duration:  ${DURATION}s per workload per concurrency level"
echo "================================================================="
echo ""

mkdir -p "$RESULTS_DIR"

# ── Capture environment ──
ENV_FILE="$RESULTS_DIR/environment_${TIMESTAMP}.txt"
{
    echo "=== System ==="
    uname -a 2>/dev/null || echo "OS: unknown"
    echo ""
    echo "=== CPU ==="
    nproc 2>/dev/null || echo "CPU: unknown"
    lscpu 2>/dev/null | head -20 || true
    echo ""
    echo "=== Memory ==="
    free -h 2>/dev/null || echo "Memory: unknown"
    echo ""
    echo "=== FalconDB ==="
    "$REPO_ROOT/target/release/falcon" version 2>/dev/null || echo "FalconDB: not built"
    echo ""
    echo "=== PostgreSQL ==="
    psql --version 2>/dev/null || echo "PostgreSQL: not installed"
} > "$ENV_FILE" 2>&1
echo "Environment captured: $ENV_FILE"
echo ""

# ── Start databases ──
CLEANUP_PIDS=()
cleanup() {
    echo ""
    echo "Cleaning up..."
    for pid_file in "$BENCH_DIR/falcondb/falcon.pid" "$BENCH_DIR/postgresql/pg_data/postmaster.pid"; do
        if [ -f "$pid_file" ]; then
            pid=$(head -1 "$pid_file" 2>/dev/null || true)
            if [ -n "$pid" ]; then
                kill "$pid" 2>/dev/null || true
            fi
        fi
    done
    pg_ctl -D "$BENCH_DIR/postgresql/pg_data" stop 2>/dev/null || true
    # Kill FalconDB if PID file exists
    if [ -f "$BENCH_DIR/falcondb/falcon.pid" ]; then
        kill "$(cat "$BENCH_DIR/falcondb/falcon.pid")" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "Starting FalconDB..."
chmod +x "$BENCH_DIR/falcondb/run.sh"
"$BENCH_DIR/falcondb/run.sh" || { echo "FAIL: Could not start FalconDB"; exit 1; }
echo ""

echo "Starting PostgreSQL..."
chmod +x "$BENCH_DIR/postgresql/run.sh"
"$BENCH_DIR/postgresql/run.sh" || { echo "FAIL: Could not start PostgreSQL"; exit 1; }
echo ""

# ── Run workloads ──
chmod +x "$SCRIPTS_DIR/run_workload.sh"

WORKLOADS=("w1" "w2")
THREAD_COUNTS=("1" "4" "8" "16")
TARGETS=("falcondb" "postgresql")

for workload in "${WORKLOADS[@]}"; do
    for threads in "${THREAD_COUNTS[@]}"; do
        for target in "${TARGETS[@]}"; do
            echo ""
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            "$SCRIPTS_DIR/run_workload.sh" "$target" "$workload" "$threads" "$DURATION" || {
                echo "WARN: $target/$workload/t$threads failed — continuing"
            }
        done
    done
done

# ── Generate report ──
echo ""
echo "================================================================="
echo " Generating comparison report..."
echo "================================================================="

REPORT="$RESULTS_DIR/comparison_${TIMESTAMP}.txt"
{
    echo "FalconDB vs PostgreSQL — Benchmark Comparison"
    echo "Generated: $TIMESTAMP"
    echo "Duration per test: ${DURATION}s"
    echo ""
    echo "Environment: see environment_${TIMESTAMP}.txt"
    echo ""
    echo "=== Results ==="
    echo ""
    for workload in "${WORKLOADS[@]}"; do
        echo "--- Workload: $workload ---"
        for threads in "${THREAD_COUNTS[@]}"; do
            echo "  Threads: $threads"
            for target in "${TARGETS[@]}"; do
                result_file="$RESULTS_DIR/${target}_${workload}_t${threads}.txt"
                if [ -f "$result_file" ]; then
                    tps=$(grep -oP 'tps = \K[0-9.]+' "$result_file" | tail -1 || echo "N/A")
                    latency=$(grep -oP 'latency average = \K[0-9.]+' "$result_file" || echo "N/A")
                    echo "    $target: TPS=$tps  Avg_Latency=${latency}ms"
                else
                    echo "    $target: NO DATA"
                fi
            done
        done
        echo ""
    done
} > "$REPORT"

cat "$REPORT"

echo ""
echo "================================================================="
echo " Benchmark complete!"
echo " Results: $RESULTS_DIR/"
echo " Report:  $REPORT"
echo "================================================================="
