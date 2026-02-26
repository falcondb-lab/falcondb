#!/bin/bash
# install_linux_service.sh — Install FalconDB as a systemd service on Ubuntu 24.04
# Usage: sudo bash scripts/install_linux_service.sh [--io-uring]
set -euo pipefail

FALCON_BIN="target/release/falcon"
INSTALL_DIR="/opt/falcondb/bin"
CONF_DIR="/etc/falcondb"
DATA_DIR="/var/lib/falcondb"
LOG_DIR="/var/log/falcondb"
SERVICE_FILE="dist/systemd/falcondb.service"

# ── Pre-flight checks ──
if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: Must run as root (sudo)." >&2
    exit 1
fi

if [ ! -f "$FALCON_BIN" ]; then
    echo "ERROR: Binary not found at $FALCON_BIN. Build first:" >&2
    echo "  cargo build --release -p falcon_server" >&2
    exit 1
fi

if [ ! -f "$SERVICE_FILE" ]; then
    echo "ERROR: Service file not found at $SERVICE_FILE." >&2
    exit 1
fi

echo "=== FalconDB Linux Service Installer ==="
echo ""

# ── Create user ──
if ! id -u falcondb &>/dev/null; then
    useradd --system --no-create-home --shell /usr/sbin/nologin falcondb
    echo "[+] Created system user: falcondb"
else
    echo "[=] User falcondb already exists"
fi

# ── Create directories ──
mkdir -p "$INSTALL_DIR" "$CONF_DIR" "$DATA_DIR" "$LOG_DIR"
chown falcondb:falcondb "$DATA_DIR" "$LOG_DIR"
echo "[+] Directories created"

# ── Copy binary ──
cp "$FALCON_BIN" "$INSTALL_DIR/falcon"
chmod 755 "$INSTALL_DIR/falcon"
echo "[+] Binary installed to $INSTALL_DIR/falcon"

# ── Copy config (don't overwrite existing) ──
if [ ! -f "$CONF_DIR/falcon.toml" ]; then
    cp dist/conf/falcon.toml "$CONF_DIR/falcon.toml"
    chown falcondb:falcondb "$CONF_DIR/falcon.toml"
    echo "[+] Config installed to $CONF_DIR/falcon.toml"
else
    echo "[=] Config already exists at $CONF_DIR/falcon.toml (not overwritten)"
fi

# ── Install systemd service ──
cp "$SERVICE_FILE" /etc/systemd/system/falcondb.service
systemctl daemon-reload
systemctl enable falcondb
echo "[+] systemd service installed and enabled"

# ── Apply sysctl tuning ──
if [ -f "dist/sysctl/99-falcondb.conf" ]; then
    cp dist/sysctl/99-falcondb.conf /etc/sysctl.d/
    sysctl -p /etc/sysctl.d/99-falcondb.conf 2>/dev/null || true
    echo "[+] sysctl tuning applied"
fi

# ── Platform detection ──
echo ""
echo "=== Platform Detection ==="
KERNEL=$(uname -r)
echo "  Kernel:     $KERNEL"

# Filesystem
FS_TYPE=$(df -T "$DATA_DIR" | awk 'NR==2 {print $2}')
echo "  Filesystem: $FS_TYPE"

# Block device
BLOCK_DEV=$(df "$DATA_DIR" | awk 'NR==2 {print $1}')
echo "  Block dev:  $BLOCK_DEV"

# NUMA
NUMA_NODES=$(ls -d /sys/devices/system/node/node* 2>/dev/null | wc -l)
echo "  NUMA nodes: $NUMA_NODES"

# THP
THP=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo "unknown")
echo "  THP:        $THP"

# io_uring
if [ -e /proc/sys/kernel/io_uring_disabled ]; then
    IO_URING_DISABLED=$(cat /proc/sys/kernel/io_uring_disabled)
    if [ "$IO_URING_DISABLED" = "0" ]; then
        echo "  io_uring:   available"
    else
        echo "  io_uring:   disabled (kernel.io_uring_disabled=$IO_URING_DISABLED)"
    fi
else
    echo "  io_uring:   likely available (no disable flag found)"
fi

# ── Advisories ──
echo ""
echo "=== Advisories ==="

if echo "$THP" | grep -q '\[always\]'; then
    echo "  WARN: THP is 'always'. Set to 'madvise':"
    echo "    echo madvise > /sys/kernel/mm/transparent_hugepage/enabled"
fi

if swapon --show 2>/dev/null | grep -q .; then
    echo "  WARN: Swap is enabled. Disable for production:"
    echo "    swapoff -a"
fi

if [ "$FS_TYPE" = "tmpfs" ]; then
    echo "  CRITICAL: Data dir is on tmpfs — fsync is a no-op. DCG is void!"
fi

if [ "$NUMA_NODES" -gt 1 ]; then
    echo "  INFO: Multi-NUMA detected. Consider:"
    echo "    numactl --cpunodebind=0 --membind=0 /opt/falcondb/bin/falcon ..."
fi

echo ""
echo "=== Installation Complete ==="
echo "Start:   systemctl start falcondb"
echo "Status:  systemctl status falcondb"
echo "Logs:    journalctl -u falcondb -f"
echo "Config:  $CONF_DIR/falcon.toml"
echo ""
echo "See docs/os/linux_ubuntu_24_04.md for tuning guide."
