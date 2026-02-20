#!/bin/bash
# Cedar Benchmark Runner
# Runs the standard benchmark suite and exports results.

set -euo pipefail

echo "=== Cedar Benchmark Suite ==="
echo ""

echo "--- YCSB Workload A (default) ---"
cargo run --release -p cedar_bench -- --ops 10000 --export text

echo ""
echo "--- Fast-Path ON vs OFF Comparison ---"
cargo run --release -p cedar_bench -- --ops 10000 --compare --export text

echo ""
echo "--- Scale-Out (1/2/4/8 shards) ---"
cargo run --release -p cedar_bench -- --scaleout --ops 5000 --export text

echo ""
echo "--- Failover Benchmark ---"
cargo run --release -p cedar_bench -- --failover --ops 10000 --export text

echo ""
echo "=== Benchmark Suite Complete ==="
