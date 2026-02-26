# FalconDB — RHEL 9 / Rocky Linux 9 / AlmaLinux 9 Production Tuning Guide

> **Purpose**: Comprehensive OS-level optimization guide for running FalconDB on
> RHEL 9.x, Rocky Linux 9.x, or AlmaLinux 9.x (Linux 5.14+ kernel). Every
> recommendation is specific to FalconDB's transaction engine, WAL, and
> replication — not generic Linux tuning.

---

## Table of Contents

1. [I/O Backend Selection](#1-io-backend-selection)
2. [WAL & Filesystem Awareness](#2-wal--filesystem-awareness)
3. [NUMA & Memory Allocation](#3-numa--memory-allocation)
4. [Thread Scheduling & CPU Affinity](#4-thread-scheduling--cpu-affinity)
5. [systemd & cgroup v2 Deployment](#5-systemd--cgroup-v2-deployment)
6. [Network Stack Optimization](#6-network-stack-optimization)
7. [SELinux Integration](#7-selinux-integration)
8. [Quick Reference Checklist](#8-quick-reference-checklist)

---

## 1. I/O Backend Selection

FalconDB supports three I/O backends for the WAL write path:

| Backend | Config Value | When to Use | Kernel Requirement |
|---------|-------------|-------------|-------------------|
| **POSIX sync** | `wal_mode = "posix"` | Default, works everywhere | Any |
| **io_uring** | `wal_mode = "io_uring"` | NVMe storage, lowest tail latency | Kernel 5.14+ (RHEL 9 default) |
| **Auto** | `wal_mode = "auto"` | Let FalconDB detect best backend | Any |

### Enabling io_uring

```bash
# Build with io_uring support
cargo build --release -p falcon_server --features io_uring

# In falcon.toml
wal_mode = "io_uring"
```

### RHEL 9 io_uring Specifics

RHEL 9 ships with kernel 5.14, which supports io_uring but with some security
restrictions compared to Ubuntu 24.04 (kernel 6.x):

- **Unprivileged io_uring** may be restricted by default in RHEL 9.3+
- Check: `sysctl io_uring_disabled`
- If restricted, either run FalconDB as root (not recommended) or adjust:

```bash
# Allow unprivileged io_uring (RHEL 9.3+)
echo 0 > /proc/sys/kernel/io_uring_disabled
# Persist:
echo "kernel.io_uring_disabled = 0" >> /etc/sysctl.d/99-falcondb.conf
```

### Verifying io_uring is Active

```sql
SHOW falcon.wal_backend;
-- Expected: "io_uring" (or "posix" if fallback occurred)
```

---

## 2. WAL & Filesystem Awareness

### Filesystem Recommendations

RHEL 9 defaults to **XFS** for all non-boot partitions.

| Filesystem | WAL Suitability | sync_mode | Notes |
|------------|----------------|-----------|-------|
| **XFS** | ✅ Recommended (RHEL default) | `fdatasync` | Extent-based, O(1) append. Best for sequential WAL writes. |
| **ext4** | ✅ Recommended | `fdatasync` | Use `data=ordered` journaling (default). |
| **Btrfs** | ⚠️ Not recommended | `fsync` | Not supported in RHEL 9. Available in Rocky/Alma but CoW causes write amplification. |

### XFS-Specific Tuning

```bash
# Recommended XFS mount options for WAL partition
/dev/nvme0n1p1 /var/lib/falcondb xfs noatime,nodiratime,logbufs=8,logbsize=256k,inode64 0 2
```

- `logbufs=8,logbsize=256k` — increases XFS journal buffer for write-heavy WAL
- `inode64` — allows inode allocation across entire device (default on RHEL 9)
- `noatime,nodiratime` — eliminates unnecessary metadata writes

### fsync vs fdatasync

```toml
[wal]
# Recommended for XFS and ext4:
sync_mode = "fdatasync"
```

### Mount Options for ext4 (Alternative)

```bash
/dev/nvme0n1p1 /var/lib/falcondb ext4 noatime,nodiratime,data=ordered,barrier=1 0 2
```

### I/O Scheduler

RHEL 9 uses `mq-deadline` by default for NVMe devices. This is optimal for
FalconDB's sequential WAL writes:

```bash
# Verify I/O scheduler
cat /sys/block/nvme0n1/queue/scheduler
# Expected: [none] mq-deadline kyber  (mq-deadline selected)

# For NVMe, "none" is also acceptable (hardware queuing)
echo none > /sys/block/nvme0n1/queue/scheduler
```

---

## 3. NUMA & Memory Allocation

### NUMA Binding

```bash
# Single-node binding (recommended for most deployments)
numactl --cpunodebind=0 --membind=0 /opt/falcondb/bin/falcon -c /etc/falcondb/falcon.toml

# Interleave mode (only if dataset exceeds single NUMA node's memory)
numactl --interleave=all /opt/falcondb/bin/falcon -c /etc/falcondb/falcon.toml
```

### Verifying NUMA on RHEL 9

```bash
# Install numactl if not present
dnf install -y numactl

# Check NUMA topology
numactl --hardware
lscpu | grep -i numa
```

### Allocator Strategy

| Allocator | When to Use | How to Enable |
|-----------|-------------|---------------|
| **jemalloc** | Multi-NUMA, >16 cores | `LD_PRELOAD=/usr/lib64/libjemalloc.so.2` |
| **mimalloc** | Single-NUMA, low thread count | `LD_PRELOAD=/usr/lib64/libmimalloc.so` |
| **glibc malloc** | Default, acceptable for <8 cores | No action needed |

```bash
# Install jemalloc on RHEL 9
dnf install -y jemalloc

# jemalloc configuration for FalconDB
export MALLOC_CONF="narenas:4,background_thread:true,metadata_thp:auto"
export LD_PRELOAD=/usr/lib64/libjemalloc.so.2
/opt/falcondb/bin/falcon -c /etc/falcondb/falcon.toml
```

### Transparent Huge Pages (THP)

**Recommendation: set THP to `madvise`.**

RHEL 9 ships with THP set to `madvise` by default — **do not change this**.

```bash
# Verify THP setting
cat /sys/kernel/mm/transparent_hugepage/enabled
# Should show: always [madvise] never

# If set to "always", fix it:
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
echo madvise > /sys/kernel/mm/transparent_hugepage/defrag

# Persist via tuned (see below)
```

### tuned Profile

RHEL 9 uses `tuned` for system-wide performance tuning. Create a custom profile:

```bash
mkdir -p /etc/tuned/falcondb
cat > /etc/tuned/falcondb/tuned.conf << 'EOF'
[main]
summary=FalconDB OLTP Database Tuning Profile
include=throughput-performance

[vm]
transparent_hugepages=madvise
transparent_hugepage.defrag=madvise

[sysctl]
vm.swappiness=1
vm.overcommit_memory=2
vm.overcommit_ratio=80
vm.dirty_ratio=10
vm.dirty_background_ratio=3

[cpu]
governor=performance
energy_perf_bias=performance
EOF

# Activate
tuned-adm profile falcondb
tuned-adm active
```

### vm.overcommit and Swap

```bash
# Disable overcommit — FalconDB relies on accurate memory budgets
echo 2 > /proc/sys/vm/overcommit_memory
echo 80 > /proc/sys/vm/overcommit_ratio

# Minimize swap usage
echo 1 > /proc/sys/vm/swappiness

# Or disable swap entirely (recommended if memory budget is correctly configured)
swapoff -a
# Remove swap entries from /etc/fstab
```

---

## 4. Thread Scheduling & CPU Affinity

### FalconDB Thread Roles

| Thread Name | Role | Priority | CPU Affinity |
|-------------|------|----------|-------------|
| `falcon-wal-writer` | WAL append + group commit syncer | **High** (nice -10) | Pin to dedicated core |
| `falcon-async-wal-syncer` | Background WAL flush timer | **High** (nice -10) | Same core as WAL writer |
| `falcon-repl-sender` | Replication WAL shipping | Medium (nice -5) | Separate from WAL writer |
| `tokio-runtime-*` | Txn executor, SQL processing | Normal (nice 0) | Remaining cores |
| `falcon-gc-*` | MVCC garbage collection | **Low** (nice 10) | No affinity |

### Post-Startup Thread Tuning Script

```bash
#!/bin/bash
# scripts/tune_threads_rhel.sh — Apply thread priorities for RHEL 9 production
PID=$(pidof falcon)
if [ -z "$PID" ]; then echo "FalconDB not running"; exit 1; fi

for tid in $(ls /proc/$PID/task/); do
    name=$(cat /proc/$PID/task/$tid/comm 2>/dev/null)
    case "$name" in
        falcon-wal-writ*|falcon-async-wa*)
            renice -n -10 -p $tid 2>/dev/null
            echo "Set $name (tid=$tid) to nice -10"
            ;;
        falcon-repl-sen*)
            renice -n -5 -p $tid 2>/dev/null
            echo "Set $name (tid=$tid) to nice -5"
            ;;
        falcon-gc-*)
            renice -n 10 -p $tid 2>/dev/null
            echo "Set $name (tid=$tid) to nice 10"
            ;;
    esac
done
```

---

## 5. systemd & cgroup v2 Deployment

RHEL 9 uses cgroup v2 by default (unlike RHEL 8 which used cgroup v1).

### systemd Service Unit

Create `/etc/systemd/system/falcondb.service`:

```ini
[Unit]
Description=FalconDB OLTP Database
Documentation=https://github.com/falcondb-lab/falcondb
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=60
StartLimitBurst=3

[Service]
Type=simple
User=falcondb
Group=falcondb
WorkingDirectory=/var/lib/falcondb

# Core process
ExecStart=/opt/falcondb/bin/falcon -c /etc/falcondb/falcon.toml
ExecReload=/bin/kill -HUP $MAINPID

# ── Resource Limits (cgroup v2) ──
CPUQuota=800%
CPUWeight=200
MemoryMax=24G
MemoryHigh=20G
MemorySwapMax=0
IOWeight=500

# ── Process Limits ──
LimitNOFILE=65536
LimitNPROC=4096
LimitMEMLOCK=infinity
LimitCORE=infinity

# ── OOM & Crash Behavior ──
OOMScoreAdjust=-900
OOMPolicy=continue
Restart=on-failure
RestartSec=5
TimeoutStartSec=30
TimeoutStopSec=60

# ── Security Hardening ──
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/var/lib/falcondb /var/log/falcondb
PrivateTmp=yes
NoNewPrivileges=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes

# ── SELinux ──
# See §7 for SELinux context configuration

# ── Scheduling ──
Nice=-5
CPUSchedulingPolicy=other
IOSchedulingClass=best-effort
IOSchedulingPriority=2

# ── Environment ──
# Uncomment for jemalloc:
# Environment="LD_PRELOAD=/usr/lib64/libjemalloc.so.2"
# Environment="MALLOC_CONF=narenas:4,background_thread:true"

[Install]
WantedBy=multi-user.target
```

### Installation Script

```bash
#!/bin/bash
# scripts/install_rhel_service.sh
set -e

# Create user
useradd --system --no-create-home --shell /sbin/nologin falcondb

# Create directories
mkdir -p /opt/falcondb/bin /etc/falcondb /var/lib/falcondb /var/log/falcondb
chown -R falcondb:falcondb /var/lib/falcondb /var/log/falcondb

# Copy binary and config
cp target/release/falcon /opt/falcondb/bin/
chmod 755 /opt/falcondb/bin/falcon
cp dist/conf/falcon.toml /etc/falcondb/
chmod 640 /etc/falcondb/falcon.toml
chown root:falcondb /etc/falcondb/falcon.toml

# Install service
cp docs/os/falcondb.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable falcondb
systemctl start falcondb

# Open firewall ports
firewall-cmd --permanent --add-port=5433/tcp   # PG wire
firewall-cmd --permanent --add-port=50051/tcp  # gRPC replication
firewall-cmd --permanent --add-port=9090/tcp   # Prometheus metrics
firewall-cmd --permanent --add-port=8080/tcp   # Admin HTTP
firewall-cmd --reload

echo "FalconDB installed. Check: systemctl status falcondb"
```

---

## 6. Network Stack Optimization

### TCP Settings

```bash
# /etc/sysctl.d/99-falcondb.conf

# ── TCP Latency ──
net.ipv4.tcp_low_latency = 1

# ── Buffer Sizes ──
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 1048576 16777216
net.ipv4.tcp_wmem = 4096 1048576 16777216

# ── Connection Handling ──
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6

# ── Backlog ──
net.core.somaxconn = 4096
net.ipv4.tcp_max_syn_backlog = 4096

# ── Congestion Control ──
net.ipv4.tcp_congestion_control = bbr
net.core.default_qdisc = fq
```

```bash
# Apply
sysctl -p /etc/sysctl.d/99-falcondb.conf

# Load BBR module (if not already loaded)
modprobe tcp_bbr
echo "tcp_bbr" >> /etc/modules-load.d/falcondb.conf
```

### firewalld Configuration

```bash
# Create a FalconDB firewalld service
cat > /etc/firewalld/services/falcondb.xml << 'EOF'
<?xml version="1.0" encoding="utf-8"?>
<service>
  <short>FalconDB</short>
  <description>FalconDB OLTP Database</description>
  <port protocol="tcp" port="5433"/>
  <port protocol="tcp" port="50051"/>
  <port protocol="tcp" port="9090"/>
  <port protocol="tcp" port="8080"/>
</service>
EOF

firewall-cmd --reload
firewall-cmd --permanent --add-service=falcondb
firewall-cmd --reload
```

---

## 7. SELinux Integration

RHEL 9 enforces SELinux by default. **Do not disable SELinux.** Instead, create a
custom policy for FalconDB.

### Quick Start (Permissive for FalconDB only)

```bash
# Check SELinux status
getenforce  # Should be: Enforcing

# Set FalconDB file contexts
semanage fcontext -a -t bin_t "/opt/falcondb/bin(/.*)?"
semanage fcontext -a -t var_lib_t "/var/lib/falcondb(/.*)?"
semanage fcontext -a -t var_log_t "/var/log/falcondb(/.*)?"
semanage fcontext -a -t etc_t "/etc/falcondb(/.*)?"

# Apply contexts
restorecon -Rv /opt/falcondb /var/lib/falcondb /var/log/falcondb /etc/falcondb
```

### Allow Network Ports

```bash
# Allow FalconDB to bind to non-standard ports
semanage port -a -t postgresql_port_t -p tcp 5433
semanage port -a -t http_port_t -p tcp 8080
semanage port -a -t http_port_t -p tcp 9090
```

### Custom SELinux Policy Module (Production)

For full enforcement, create a custom policy module:

```bash
# 1. Run FalconDB in permissive mode and collect denials
semanage permissive -a falcon_t 2>/dev/null || true
systemctl start falcondb
# Run workload...

# 2. Generate policy from audit log
ausearch -m avc -ts recent | audit2allow -M falcondb_policy

# 3. Install the policy
semodule -i falcondb_policy.pp

# 4. Remove permissive exception
semanage permissive -d falcon_t 2>/dev/null || true
```

### Verify SELinux is Not Blocking FalconDB

```bash
# Check for recent AVC denials
ausearch -m avc -ts recent | grep falcon
# Should return no results if policy is correct

# Check FalconDB file contexts
ls -lZ /opt/falcondb/bin/falcon
ls -lZ /var/lib/falcondb/
```

---

## 8. Quick Reference Checklist

### Before First Production Deploy

```bash
# 1. OS version
cat /etc/redhat-release  # Should be: Rocky Linux release 9.x / Red Hat Enterprise Linux release 9.x

# 2. Kernel version
uname -r  # Should be 5.14.x+

# 3. Filesystem
df -T /var/lib/falcondb  # Should be xfs or ext4

# 4. THP
cat /sys/kernel/mm/transparent_hugepage/enabled
# Should show: always [madvise] never

# 5. Swap
swapon --show  # Should be empty, or swappiness=1

# 6. File limits
ulimit -n  # Should be >= 65536

# 7. SELinux
getenforce  # Should be: Enforcing
# Verify no AVC denials for falcon

# 8. Firewall
firewall-cmd --list-all | grep 5433  # Port should be open

# 9. I/O scheduler
cat /sys/block/nvme0n1/queue/scheduler  # none or mq-deadline

# 10. tuned profile
tuned-adm active  # Should show: falcondb (or throughput-performance)
```

### Production Configuration Template

```toml
config_version = 4

[production_safety]
enforce = true

[server]
pg_listen_addr = "0.0.0.0:5433"
admin_listen_addr = "0.0.0.0:8080"
node_id = 1
max_connections = 512
statement_timeout_ms = 30000
shutdown_drain_timeout_secs = 30

[server.auth]
method = "scram-sha-256"
username = "falcon"

[storage]
wal_enabled = true
data_dir = "/var/lib/falcondb/data"

[wal]
sync_mode = "fdatasync"
group_commit = true
flush_interval_us = 1000
segment_size_bytes = 67108864
group_commit_window_us = 200

[memory]
shard_soft_limit_bytes = 8589934592    # 8 GB
shard_hard_limit_bytes = 12884901888   # 12 GB

[gc]
enabled = true
interval_ms = 1000
batch_size = 5000
```

### Monitoring Checklist (Post-Deploy)

| Metric | Source | Alert Threshold |
|--------|--------|----------------|
| WAL fsync p99 | `SHOW falcon.wal_stats` | > 2ms (NVMe), > 10ms (SSD) |
| Replication lag | `SHOW falcon.replication_lag` | > 500ms |
| Memory usage | `SHOW falcon.memory_status` | > 80% of hard limit |
| GC frequency | `SHOW falcon.gc_stats` | Stalled for > 10s |
| Connection count | `:8080/metrics` | > 80% of max_connections |
| SELinux denials | `ausearch -m avc` | Any denial for falcon |

---

*This guide is specific to RHEL 9 / Rocky Linux 9 / AlmaLinux 9. For Ubuntu 24.04,
see [linux_ubuntu_24_04.md](linux_ubuntu_24_04.md). For Windows Server 2022,
see [windows_server_2022.md](windows_server_2022.md). For general operations,
see [docs/OPERATIONS.md](../OPERATIONS.md).*
