#!/bin/bash
# Cedar Failover Demonstration
# Runs the failover exercise example.

set -euo pipefail

echo "=== Cedar Failover Exercise ==="
echo ""
cargo run -p cedar_cluster --example failover_exercise
echo ""
echo "=== Failover Exercise Complete ==="
