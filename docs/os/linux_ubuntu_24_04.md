# FalconDB — Ubuntu 24.04 LTS Production Tuning Guide

> **Purpose**: Comprehensive OS-level optimization guide for running FalconDB on
> Ubuntu 24.04 LTS (Linux 6.x). Every recommendation is specific to FalconDB's
> transaction engine, WAL, and replication — not generic Linux tuning.

---

## Table of Contents

1. [I/O Backend Selection](#1-io-backend-selection)
2. [WAL & Filesystem Awareness](#2-wal--filesystem-awareness)
3. [NUMA & Memory Allocation](#3-numa--memory-allocation)
4. [Thread Scheduling & CPU Affinity](#4-thread-scheduling--cpu-affinity)
5. [systemd & cgroup v2 Deployment](#5-systemd--cgroup-v2-deployment)
6. [Network Stack Optimization](#6-network-stack-optimization)
7. [Quick Reference Checklist](#7-quick-reference-checklist)

---

## 1. I/O Backend Selection

FalconDB supports three I/O backends for the WAL write path:

| Backend | Config Value | When to Use | Kernel Requirement |
|---------|-------------|-------------|-------------------|
| **POSIX sync** | `wal_mode = "posix"` | Default, works everywhere | Any |
| **io_uring** | `wal_mode = "io_uring"` | Ubuntu 24.04 + NVMe, lowest tail latency | Linux 5.10+ (6.x recommended) |
| **Auto** | `wal_mode = "auto"` | Let FalconDB detect best backend | Any |

### Enabling io_uring

```bash
# Build with io_uring support
cargo build --release -p falcon_server --features io_uring

# In falcon.toml
wal_mode = "io_uring"        # or "auto" to let FalconDB choose
```

### How io_uring Helps

The critical WAL path is: **write() → fdatasync()**. With POSIX, this is 2 syscalls
per group commit batch. With io_uring, both are submitted as linked SQEs in a single
`io_uring_enter()` call, eliminating one context switch per commit.

**Expected improvement**: 10–30% reduction in p99/p999 WAL fsync latency under high
concurrency (>16 threads). Minimal improvement at low concurrency where syscall overhead
is not the bottleneck.

### Verifying io_uring is Active

```sql
SHOW falcon.wal_backend;
-- Expected: "io_uring" (or "posix" if fallback occurred)
```

Check startup logs:
```
INFO  io_uring backend initialized (ring_size=256)
```

If io_uring fails to initialize (e.g., kernel too old, seccomp restrictions in containers),
FalconDB automatically falls back to POSIX sync and logs a warning.

---

## 2. WAL & Filesystem Awareness

### Startup Detection

FalconDB automatically detects the filesystem and block device type at startup on Linux
and logs advisory messages:

```
INFO  [Platform] filesystem=ext4 block_device=NVMe kernel=6.8.0-45-generic
INFO  [Platform] NUMA: 2 nodes, 32 CPUs
WARN  [Platform] LPD-5: THP is set to 'always' — may cause latency spikes
```

### Filesystem Recommendations

| Filesystem | WAL Suitability | sync_mode | Notes |
|------------|----------------|-----------|-------|
| **ext4** | ✅ Recommended | `fdatasync` | Use `data=ordered` journaling (default). `fdatasync` avoids unnecessary metadata sync. |
| **XFS** | ✅ Recommended | `fdatasync` | Extent-based allocation — O(1) append. `fdatasync` is optimal. |
| **Btrfs** | ⚠️ Caution | `fsync` | CoW causes write amplification. Disable CoW for WAL dir: `chattr +C /path/to/falcon_data`. |
| **tmpfs** | ❌ Never | N/A | fsync is a no-op on tmpfs. **The DCG is void.** |
| **ZFS** | ⚠️ Caution | `fsync` | ZFS ARC + ZIL interaction with FalconDB's own buffering may cause double-caching. |

### fsync vs fdatasync

```toml
[wal]
# Recommended for ext4 and XFS:
sync_mode = "fdatasync"

# Use full fsync only if:
# - Filesystem is Btrfs or ZFS (need metadata consistency)
# - You need to guarantee file size metadata is durable
sync_mode = "fsync"
```

**Why fdatasync?** `fdatasync()` only flushes data and metadata that is necessary to
retrieve the data (e.g., file size), skipping unnecessary metadata like atime/mtime.
On ext4, this saves ~5–15µs per call compared to full `fsync()`.

### O_DIRECT Considerations

FalconDB does **not** use `O_DIRECT` by default because:
- WAL writes are sequential and small (8–64 KB per group commit batch)
- The OS page cache provides beneficial read-ahead for WAL replay
- `O_DIRECT` requires sector-aligned buffers, adding complexity

For extreme tail latency requirements, `O_DIRECT` can be enabled experimentally:
```toml
[wal]
no_buffering = true    # Linux: O_DIRECT; Windows: FILE_FLAG_NO_BUFFERING
```

### Mount Options

```bash
# Recommended mount options for ext4 WAL partition:
/dev/nvme0n1p1 /var/lib/falcondb ext4 noatime,nodiratime,data=ordered,barrier=1 0 2

# Recommended mount options for XFS WAL partition:
/dev/nvme0n1p1 /var/lib/falcondb xfs noatime,nodiratime,logbufs=8,logbsize=256k 0 2
```

- `noatime,nodiratime` — eliminates unnecessary metadata writes on every read
- `barrier=1` (ext4) — ensures write barriers are enforced (default, do not disable)
- `logbufs=8,logbsize=256k` (XFS) — increases journal buffer for write-heavy WAL workloads

---

## 3. NUMA & Memory Allocation

### NUMA Binding

FalconDB is a memory-first database — **NUMA locality directly impacts transaction latency**.
Remote memory access adds 40–100ns per access, which compounds across MVCC lookups.

**Recommended: bind FalconDB to a single NUMA node.**

```bash
# Single-node binding (recommended for most deployments)
numactl --cpunodebind=0 --membind=0 ./target/release/falcon -c falcon.toml

# Interleave mode (only if dataset exceeds single node's memory)
numactl --interleave=all ./target/release/falcon -c falcon.toml
```

### Verifying NUMA Configuration

```bash
# Check NUMA topology
numactl --hardware

# Check FalconDB's NUMA placement after startup
cat /proc/$(pidof falcon)/numa_maps | head -20
```

### Allocator Strategy

FalconDB uses the system allocator by default. For production on NUMA systems:

| Allocator | When to Use | How to Enable |
|-----------|-------------|---------------|
| **jemalloc** | Multi-NUMA, >16 cores | `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2` |
| **mimalloc** | Single-NUMA, low thread count | `LD_PRELOAD=/usr/lib/libmimalloc.so` |
| **glibc malloc** | Default, acceptable for <8 cores | No action needed |

**jemalloc configuration for FalconDB:**

```bash
# Per-CPU arena + NUMA-aware allocation
export MALLOC_CONF="narenas:4,background_thread:true,metadata_thp:auto"
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
./target/release/falcon -c falcon.toml
```

- `narenas:4` — match shard count to reduce cross-arena contention
- `background_thread:true` — async purge of dirty pages, reduces latency spikes
- `metadata_thp:auto` — allow huge pages for jemalloc metadata only

### Transparent Huge Pages (THP)

**Recommendation: set THP to `madvise` (not `always`, not `never`).**

```bash
# Set THP to madvise (persistent via sysctl or systemd)
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
echo madvise > /sys/kernel/mm/transparent_hugepage/defrag
```

**Why not `always`?**
- THP compaction runs in-kernel and can cause 1–10ms latency spikes
- FalconDB's hot path (MVCC index lookups, WAL append) uses small allocations
  that don't benefit from 2MB pages
- THP `always` causes RSS overreporting, confusing memory budget enforcement

**Why not `never`?**
- Some large allocations (hash table resize, sort buffer) genuinely benefit from THP
- `madvise` allows the allocator to opt in where beneficial

### vm.overcommit and Swap

```bash
# Disable overcommit — FalconDB relies on accurate memory budgets
echo 2 > /proc/sys/vm/overcommit_memory
echo 80 > /proc/sys/vm/overcommit_ratio

# Minimize swap usage — swapping MVCC pages causes catastrophic latency
echo 1 > /proc/sys/vm/swappiness

# Or disable swap entirely (recommended if memory budget is correctly configured)
swapoff -a
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

### Applying Thread Priorities

```bash
# Option 1: Use nice to run FalconDB with elevated priority
nice -n -5 ./target/release/falcon -c falcon.toml

# Option 2: Use systemd service (see §5) with CPUSchedulingPriority
# Option 3: Post-startup tuning via script
```

**Post-startup priority tuning script:**

```bash
#!/bin/bash
# scripts/tune_threads.sh — Apply thread priorities for production
PID=$(pidof falcon)
if [ -z "$PID" ]; then echo "FalconDB not running"; exit 1; fi

# Find WAL writer thread and set high priority
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

### CPU Pinning (Optional, Advanced)

For ultra-low-latency deployments, pin the WAL writer to a dedicated CPU:

```bash
# Reserve CPU 0 for WAL writer, CPUs 1-7 for everything else
taskset -pc 0 $(pgrep -f falcon-wal-writ)
taskset -pc 1-7 $(pidof falcon)
```

**Caution**: CPU pinning reduces scheduling flexibility. Only use if:
- You have dedicated hardware (not shared VMs)
- Benchmarks confirm improvement for your workload
- You are comfortable managing CPU topology changes

### Avoiding Priority Inversion

The key invariant: **WAL writer and replication sender must never be preempted by
GC or bulk query threads.** This is why:
- WAL writer is nice -10 (elevated)
- GC threads are nice 10 (lowered)
- Normal executor threads are nice 0 (default)

If you see WAL fsync latency spikes (>5ms p99) correlated with GC cycles, increase
the priority gap or pin the WAL writer to a dedicated core.

---

## 5. systemd & cgroup v2 Deployment

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
# CPU: guarantee 4 cores, allow burst to 8
CPUQuota=800%
CPUWeight=200

# Memory: hard limit prevents OOM from affecting other services
MemoryMax=24G
MemoryHigh=20G
MemorySwapMax=0

# I/O: WAL writes get priority
IOWeight=500
IODeviceLatencyTargetSec=/dev/nvme0n1 50ms

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

# ── Scheduling ──
Nice=-5
CPUSchedulingPolicy=other
IOSchedulingClass=best-effort
IOSchedulingPriority=2

[Install]
WantedBy=multi-user.target
```

### Key cgroup v2 Parameters Explained

| Parameter | Value | Why |
|-----------|-------|-----|
| `CPUQuota=800%` | 8 cores max | Prevents FalconDB from starving other services |
| `CPUWeight=200` | 2× default priority | FalconDB gets more CPU time under contention |
| `MemoryMax=24G` | Hard limit | Prevents kernel OOM killer from choosing FalconDB |
| `MemoryHigh=20G` | Soft limit | Triggers kernel memory reclaim before hitting max |
| `MemorySwapMax=0` | No swap | Swapping MVCC pages causes catastrophic latency |
| `IOWeight=500` | 5× default | WAL fsync gets disk priority over backup/log rotation |
| `OOMScoreAdjust=-900` | Nearly immune to OOM | Database should be the last process killed |

### FalconDB's Position on Swap

**FalconDB should never be swapped.** The entire MVCC state is in memory — a single
swapped page can cause a 10ms+ stall that violates p99 SLAs.

```bash
# Verify swap is disabled for FalconDB's cgroup
cat /sys/fs/cgroup/system.slice/falcondb.service/memory.swap.max
# Expected: 0
```

### Memory Pressure Monitoring

FalconDB's memory budget (`memory.shard_hard_limit_bytes`) operates independently from
cgroup memory limits. The relationship:

```
cgroup MemoryMax  ≥  FalconDB shard_hard_limit × shard_count + OS overhead (~2 GB)
```

If `MemoryMax` is too close to FalconDB's own limit, the kernel's memory reclaim may
interfere with FalconDB's admission control. Leave at least 2 GB headroom.

### Installation Script

```bash
#!/bin/bash
# scripts/install_linux_service.sh
set -e

# Create user
useradd --system --no-create-home --shell /usr/sbin/nologin falcondb

# Create directories
mkdir -p /opt/falcondb/bin /etc/falcondb /var/lib/falcondb /var/log/falcondb
chown -R falcondb:falcondb /var/lib/falcondb /var/log/falcondb

# Copy binary and config
cp target/release/falcon /opt/falcondb/bin/
cp dist/conf/falcon.toml /etc/falcondb/

# Install service
cp dist/systemd/falcondb.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable falcondb
systemctl start falcondb

echo "FalconDB installed. Check: systemctl status falcondb"
```

---

## 6. Network Stack Optimization

### TCP Settings for Replication & Cross-Shard

FalconDB's replication (gRPC WAL streaming) and cross-shard 2PC are latency-sensitive.

```bash
# /etc/sysctl.d/99-falcondb.conf

# ── TCP Latency ──
# Disable Nagle's algorithm at OS level (FalconDB also sets TCP_NODELAY per-socket)
net.ipv4.tcp_low_latency = 1

# ── Buffer Sizes ──
# gRPC replication streams benefit from larger buffers
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 1048576 16777216
net.ipv4.tcp_wmem = 4096 1048576 16777216

# ── Connection Handling ──
# FalconDB uses persistent gRPC connections — increase keepalive
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6

# ── Backlog ──
# Accept more pending connections during startup/failover bursts
net.core.somaxconn = 4096
net.ipv4.tcp_max_syn_backlog = 4096

# ── Congestion Control ──
# BBR provides better throughput for WAL streaming over long links
net.ipv4.tcp_congestion_control = bbr
net.core.default_qdisc = fq
```

Apply:
```bash
sysctl -p /etc/sysctl.d/99-falcondb.conf
```

### Socket Configuration per Connection Type

| Connection Type | TCP_NODELAY | Buffer Size | Keepalive | Notes |
|----------------|-------------|-------------|-----------|-------|
| **PG client** | Yes | Default (128 KB) | OS default | Low-latency query responses |
| **gRPC replication** | Yes | 4 MB send/recv | 60s | Large WAL chunks, persistent stream |
| **gRPC cross-shard** | Yes | 1 MB send/recv | 30s | Small RPC payloads, latency-critical |
| **Admin/health HTTP** | No | Default | N/A | Not latency-sensitive |

### Connection Model: Single Stream vs Multi-Connection

FalconDB uses **single persistent gRPC connection with multiplexed streams** for
replication. This is the optimal model because:

- WAL shipping is sequential per shard — no benefit from parallel connections
- Multiplexed streams avoid TCP slow-start on reconnection
- Single connection simplifies firewall rules and monitoring
- HTTP/2 flow control prevents one shard's burst from starving others

**When to use multiple connections**: Only if cross-datacenter latency (>5ms RTT)
causes head-of-line blocking. In that case, configure one gRPC connection per shard:

```toml
[replication]
# Default: single connection (recommended for same-datacenter)
connection_per_shard = false

# For high-latency links: separate connection per shard
# connection_per_shard = true
```

### IRQ Affinity for Network Interfaces

On multi-queue NVMe + 10G NIC systems, ensure NIC IRQs don't compete with WAL I/O:

```bash
# Pin NIC IRQs to CPUs not used by WAL writer
# Example: WAL writer on CPU 0, NIC IRQs on CPUs 4-7
for irq in $(grep eth0 /proc/interrupts | awk '{print $1}' | tr -d ':'); do
    echo 4-7 > /proc/irq/$irq/smp_affinity_list
done
```

---

## 7. Quick Reference Checklist

### Before First Production Deploy

```bash
# 1. Kernel version
uname -r  # Should be 6.x on Ubuntu 24.04

# 2. Filesystem
df -T /var/lib/falcondb  # Should be ext4 or xfs

# 3. THP
cat /sys/kernel/mm/transparent_hugepage/enabled
# Should show: always [madvise] never

# 4. Swap
swapon --show  # Should be empty, or swappiness=1

# 5. File limits
ulimit -n  # Should be >= 65536

# 6. I/O scheduler (NVMe)
cat /sys/block/nvme0n1/queue/scheduler
# Should show: [none] mq-deadline kyber
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

---

*This guide is specific to Ubuntu 24.04 LTS on Linux 6.x. For Windows deployment,
see [docs/README_WINDOWS.md](../README_WINDOWS.md). For general operations,
see [docs/OPERATIONS.md](../OPERATIONS.md).*
