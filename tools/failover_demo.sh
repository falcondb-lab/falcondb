#!/bin/bash
# Falcon Failover Demonstration
# Runs the failover exercise example.

set -euo pipefail

echo "=== Falcon Failover Exercise ==="
echo ""
cargo run -p falcon_cluster --example failover_exercise
echo ""
echo "=== Failover Exercise Complete ==="
