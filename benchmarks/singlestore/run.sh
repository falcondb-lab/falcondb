#!/usr/bin/env bash
# ============================================================================
# FalconDB Benchmark — Start SingleStore for benchmark
# ============================================================================
#
# Supports two modes:
#   1. Docker (recommended): uses singlestoredb-dev-image
#   2. Native: requires singlestore-client and sdb-deploy
#
# Prerequisites (Docker mode):
#   - Docker installed and running
#   - Port 3306 available
#
# Prerequisites (Native mode):
#   - singlestore installed via sdb-deploy
#   - SINGLESTORE_LICENSE set
#   - singlestore-client (memsql) on PATH
# ============================================================================

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SINGLESTORE_DIR="$BENCH_DIR/singlestore"
MODE="${SINGLESTORE_MODE:-docker}"

CONTAINER_NAME="falcondb-bench-singlestore"
SS_PORT="${SINGLESTORE_PORT:-3306}"
SS_PASSWORD="${SINGLESTORE_PASSWORD:-bench_password}"

echo "=== SingleStore Benchmark Setup (mode: $MODE) ==="

case "$MODE" in
    docker)
        # Stop existing container
        docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

        echo "[1/4] Starting SingleStore via Docker..."
        docker run -d \
            --name "$CONTAINER_NAME" \
            -p "${SS_PORT}:3306" \
            -p 8080:8080 \
            -e ROOT_PASSWORD="$SS_PASSWORD" \
            -e SINGLESTORE_LICENSE="${SINGLESTORE_LICENSE:-}" \
            --memory=3g \
            singlestore/cluster-in-a-box:latest

        echo "[2/4] Waiting for SingleStore to be ready..."
        for i in $(seq 1 60); do
            if docker exec "$CONTAINER_NAME" memsql -u root -p"$SS_PASSWORD" -e "SELECT 1;" >/dev/null 2>&1; then
                echo "  SingleStore ready after ${i}s"
                break
            fi
            if [ "$i" -eq 60 ]; then
                echo "ERROR: SingleStore did not start within 60s"
                docker logs "$CONTAINER_NAME" | tail -20
                exit 1
            fi
            sleep 1
        done

        echo "[3/4] Creating benchmark database..."
        docker exec "$CONTAINER_NAME" memsql -u root -p"$SS_PASSWORD" \
            -e "CREATE DATABASE IF NOT EXISTS falconbench;"

        echo "[4/4] Loading benchmark schema..."
        docker exec -i "$CONTAINER_NAME" memsql -u root -p"$SS_PASSWORD" falconbench \
            < "$BENCH_DIR/workloads/setup_singlestore.sql"

        echo ""
        echo "SingleStore running (Docker):"
        echo "  MySQL wire: localhost:${SS_PORT}"
        echo "  Studio:     http://localhost:8080"
        echo "  Password:   $SS_PASSWORD"
        echo "  Database:   falconbench"
        echo ""
        echo "Connect: mysql -h 127.0.0.1 -P ${SS_PORT} -u root -p${SS_PASSWORD} falconbench"
        ;;

    native)
        echo "[1/3] Starting SingleStore (native)..."
        sdb-deploy start 2>/dev/null || true
        sleep 3

        echo "[2/3] Creating benchmark database..."
        singlestore -u root -e "CREATE DATABASE IF NOT EXISTS falconbench;"

        echo "[3/3] Loading benchmark schema..."
        singlestore -u root falconbench < "$BENCH_DIR/workloads/setup_singlestore.sql"

        echo ""
        echo "SingleStore running (native):"
        echo "  MySQL wire: localhost:3306"
        echo ""
        ;;

    *)
        echo "ERROR: Unknown SINGLESTORE_MODE='$MODE'. Use 'docker' or 'native'."
        exit 1
        ;;
esac
