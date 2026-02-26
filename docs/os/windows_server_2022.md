# FalconDB — Windows Server 2022 Production Tuning Guide

> **Purpose**: Comprehensive OS-level optimization guide for running FalconDB on
> Windows Server 2022 (and Windows 11). Every recommendation is specific to
> FalconDB's transaction engine, WAL, and replication — not generic Windows tuning.

---

## Table of Contents

1. [I/O Backend Selection](#1-io-backend-selection)
2. [WAL & Filesystem Awareness](#2-wal--filesystem-awareness)
3. [Memory & Large Pages](#3-memory--large-pages)
4. [Thread Scheduling & CPU Affinity](#4-thread-scheduling--cpu-affinity)
5. [Windows Service Deployment](#5-windows-service-deployment)
6. [Network Stack Optimization](#6-network-stack-optimization)
7. [Security Hardening](#7-security-hardening)
8. [Quick Reference Checklist](#8-quick-reference-checklist)

---

## 1. I/O Backend Selection

FalconDB supports two I/O backends on Windows:

| Backend | Config Value | When to Use | Requirement |
|---------|-------------|-------------|-------------|
| **Standard sync** | `wal_mode = "posix"` | Default, works everywhere | Any Windows |
| **IOCP** | `wal_mode = "iocp"` | NVMe storage, lowest tail latency | Windows Server 2019+ |
| **Auto** | `wal_mode = "auto"` | Let FalconDB detect best backend | Any Windows |

### Enabling IOCP

```toml
# In falcon.toml
wal_mode = "iocp"        # or "auto" to let FalconDB choose
```

### How IOCP Helps

The critical WAL path is: **WriteFile() → FlushFileBuffers()**. With standard sync,
these are blocking calls. With IOCP (I/O Completion Ports), write + flush are
submitted asynchronously and completed via a shared completion port, reducing
context switches under high concurrency.

**Expected improvement**: 15–25% reduction in p99 WAL fsync latency under high
concurrency (>16 threads). Minimal improvement at low concurrency.

### Verifying IOCP is Active

```sql
SHOW falcon.wal_backend;
-- Expected: "iocp" (or "standard" if fallback occurred)
```

Check startup logs:
```
INFO  IOCP WAL backend initialized (completion_threads=4)
```

---

## 2. WAL & Filesystem Awareness

### Startup Detection

FalconDB detects the filesystem and storage type at startup on Windows:

```
INFO  [Platform] filesystem=NTFS volume=C:\ block_device=NVMe kernel=Windows_NT_10.0.20348
INFO  [Platform] physical_memory=64GB logical_processors=16
WARN  [Platform] Antivirus may impact WAL latency — consider exclusions
```

### Filesystem Recommendations

| Filesystem | WAL Suitability | Notes |
|------------|----------------|-------|
| **NTFS** | ✅ Recommended | Default. Use 64K allocation unit for WAL volume. |
| **ReFS** | ⚠️ Caution | Integrity streams cause write amplification. Disable for WAL volume. |
| **FAT32** | ❌ Never | No journaling, 4 GB file limit. |

### FlushFileBuffers vs fsync

On Windows, `FlushFileBuffers()` is the equivalent of `fsync()`. FalconDB uses it
for WAL durability. There is no `fdatasync` equivalent — `FlushFileBuffers` always
flushes all metadata.

### FILE_FLAG_NO_BUFFERING

FalconDB does **not** use `FILE_FLAG_NO_BUFFERING` by default because:
- WAL writes are sequential and small (8–64 KB per group commit batch)
- The OS file cache provides beneficial read-ahead for WAL replay
- `NO_BUFFERING` requires sector-aligned buffers (512 or 4096 bytes)

For extreme tail latency requirements:
```toml
[wal]
no_buffering = true    # Linux: O_DIRECT; Windows: FILE_FLAG_NO_BUFFERING
```

### NTFS Allocation Unit Size

For dedicated WAL volumes, format with 64 KB allocation units:

```powershell
# Format WAL volume with 64K clusters (reduces NTFS metadata overhead for sequential writes)
Format-Volume -DriveLetter W -FileSystem NTFS -AllocationUnitSize 65536 -NewFileSystemLabel "FalconDB_WAL"
```

### Volume Mount Points

```powershell
# Recommended: separate NVMe volume for WAL
# C:\ProgramData\FalconDB\data\  → OS volume (NTFS, default cluster)
# W:\                            → dedicated WAL volume (NTFS, 64K cluster)
```

### Antivirus Exclusions

Windows Defender (and third-party AV) real-time scanning can add 0.5–5ms per WAL
write. **Exclude the FalconDB data and WAL directories:**

```powershell
# Windows Defender exclusions
Add-MpPreference -ExclusionPath "C:\ProgramData\FalconDB\data"
Add-MpPreference -ExclusionPath "W:\falcon_wal"
Add-MpPreference -ExclusionProcess "falcon.exe"
```

> **⚠️ Security tradeoff**: Excluding directories from AV reduces WAL latency but
> removes real-time malware scanning on those paths. Evaluate your threat model.

---

## 3. Memory & Large Pages

### Large Pages

FalconDB's hot path (MVCC index lookups, hash table probes) benefits from 2 MB
large pages, which reduce TLB misses.

**Enable Lock Pages in Memory privilege:**

```powershell
# Grant "Lock pages in memory" to the FalconDB service account
# Local Security Policy → User Rights Assignment → Lock pages in memory → Add User
# Or via ntrights.exe:
ntrights +r SeLockMemoryPrivilege -u "NT SERVICE\FalconDB"
```

FalconDB will automatically attempt to use large pages at startup. Check logs:

```
INFO  Large pages enabled (page_size=2MB, allocated=8GB)
```

If the privilege is not granted:
```
WARN  Large pages not available — falling back to standard 4KB pages
```

### Memory Budgeting

On Windows, FalconDB's memory budget operates independently from OS memory management.
The relationship:

```
Physical RAM  ≥  FalconDB shard_hard_limit × shard_count + OS overhead (~4 GB)
```

**Recommendations:**
- Reserve at least 4 GB for Windows + services
- Do not over-commit: FalconDB should use ≤75% of physical RAM
- Disable the page file on dedicated database servers (optional, advanced)

```powershell
# Check current memory configuration
Get-CimInstance Win32_PhysicalMemory | Measure-Object -Property Capacity -Sum |
    ForEach-Object { "Physical RAM: {0:N0} GB" -f ($_.Sum / 1GB) }

# Check page file
Get-CimInstance Win32_PageFileUsage
```

### Disable Page File (Optional)

> **Only for dedicated database servers with correctly budgeted memory.**

```powershell
# Disable page file (requires reboot)
$cs = Get-CimInstance Win32_ComputerSystem
$cs | Set-CimInstance -Property @{AutomaticManagedPagefile = $false}
# Then: System Properties → Advanced → Performance → Settings → Advanced → Virtual Memory → No paging file
```

**Why?** Paging MVCC data causes catastrophic latency spikes (10–100ms per page fault).

---

## 4. Thread Scheduling & CPU Affinity

### FalconDB Thread Roles on Windows

| Thread Name | Role | Priority | Affinity |
|-------------|------|----------|----------|
| `falcon-wal-writer` | WAL append + group commit | **Above Normal** | Pin to dedicated core (optional) |
| `falcon-repl-sender` | Replication WAL shipping | Above Normal | Separate from WAL writer |
| `tokio-runtime-*` | Txn executor, SQL processing | Normal | Remaining cores |
| `falcon-gc-*` | MVCC garbage collection | **Below Normal** | No affinity |

### Process Priority

```powershell
# Set FalconDB process priority to Above Normal
Get-Process falcon | ForEach-Object { $_.PriorityClass = 'AboveNormal' }

# Or configure in the service definition (see §5)
```

### Processor Affinity (Optional, Advanced)

For ultra-low-latency deployments, pin FalconDB to a specific NUMA node:

```powershell
# Pin FalconDB to CPUs 0-7 (first NUMA node on dual-socket systems)
$proc = Get-Process falcon
$proc.ProcessorAffinity = 0xFF  # bitmask for CPUs 0-7
```

### Processor Groups (>64 CPUs)

Windows uses Processor Groups for systems with >64 logical processors. FalconDB's
Tokio runtime automatically spreads across all groups. Verify with:

```powershell
Get-CimInstance Win32_Processor | Select-Object NumberOfLogicalProcessors
```

---

## 5. Windows Service Deployment

### Service Installation (MSI)

The recommended method is the MSI installer:

```powershell
msiexec /i FalconDB-1.2.0-x64.msi /qn
```

This installs:
- Binary: `C:\Program Files\FalconDB\bin\falcon.exe`
- Config: `C:\ProgramData\FalconDB\conf\falcon.toml`
- Data: `C:\ProgramData\FalconDB\data\`
- Logs: `C:\ProgramData\FalconDB\logs\`
- Certs: `C:\ProgramData\FalconDB\certs\`

### Service Configuration

The FalconDB Windows Service is configured with:

| Setting | Value | Why |
|---------|-------|-----|
| **Start type** | Automatic (Delayed Start) | Start after core Windows services |
| **Recovery** | Restart on failure (3 attempts) | Auto-recover from crashes |
| **Service account** | `NT SERVICE\FalconDB` (virtual) | Least privilege, no password |
| **Dependencies** | `Tcpip` | Network must be ready |

### Manual Service Installation

```powershell
# Install as Windows Service
falcon service install

# Start the service
falcon service start

# Check status
falcon service status

# View logs
Get-Content "C:\ProgramData\FalconDB\logs\falcon.log" -Tail 50 -Wait
```

### Service Recovery Policy

```powershell
# Configure recovery: restart after 5s, 30s, 60s
sc.exe failure FalconDB reset=86400 actions=restart/5000/restart/30000/restart/60000
```

### Resource Limits via Job Objects

Windows does not have cgroups. For memory/CPU limits, use Job Objects (applied
automatically when running as a service via the FalconDB service wrapper):

```toml
# In falcon.toml — FalconDB self-enforces these limits
[memory]
shard_soft_limit_bytes = 8589934592    # 8 GB
shard_hard_limit_bytes = 12884901888   # 12 GB
```

---

## 6. Network Stack Optimization

### TCP Settings for Replication & Cross-Shard

```powershell
# /etc/sysctl.d equivalent on Windows — registry or netsh

# Increase TCP window size for gRPC replication streams
netsh int tcp set global autotuninglevel=normal

# Enable TCP timestamps (helps with RTT measurement for cross-datacenter)
netsh int tcp set global timestamps=enabled

# Increase ephemeral port range (important for many connections)
netsh int ipv4 set dynamicport tcp start=10000 num=55535

# Increase max SYN retransmissions (failover resilience)
netsh int tcp set global maxsynretransmissions=4
```

### Registry TCP Tuning

```powershell
# Disable Nagle's algorithm globally (FalconDB also sets TCP_NODELAY per-socket)
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" `
    -Name "TcpNoDelay" -Value 1 -Type DWord

# Increase TCP keepalive interval (for persistent gRPC connections)
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" `
    -Name "KeepAliveTime" -Value 60000 -Type DWord

# Increase max connections per port
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" `
    -Name "MaxUserPort" -Value 65534 -Type DWord
```

### RSS (Receive Side Scaling)

On servers with multi-queue NICs, ensure RSS is enabled and queues are spread
across CPUs not used by the WAL writer:

```powershell
# Enable RSS
Enable-NetAdapterRss -Name "Ethernet"

# Check RSS configuration
Get-NetAdapterRss -Name "Ethernet"

# Set RSS processor range (avoid CPU 0 if used by WAL writer)
Set-NetAdapterRss -Name "Ethernet" -BaseProcessorNumber 2 -MaxProcessorNumber 15
```

### Windows Firewall Rules

```powershell
# Allow FalconDB PG wire port
New-NetFirewallRule -DisplayName "FalconDB PG Wire" `
    -Direction Inbound -Protocol TCP -LocalPort 5433 -Action Allow

# Allow FalconDB gRPC replication
New-NetFirewallRule -DisplayName "FalconDB gRPC Replication" `
    -Direction Inbound -Protocol TCP -LocalPort 50051 -Action Allow

# Allow FalconDB metrics endpoint
New-NetFirewallRule -DisplayName "FalconDB Metrics" `
    -Direction Inbound -Protocol TCP -LocalPort 9090 -Action Allow

# Allow FalconDB admin HTTP
New-NetFirewallRule -DisplayName "FalconDB Admin" `
    -Direction Inbound -Protocol TCP -LocalPort 8080 -Action Allow
```

---

## 7. Security Hardening

### Service Account

The FalconDB service runs as `NT SERVICE\FalconDB` — a virtual service account with
minimal privileges. **Do not run as LocalSystem or Administrator.**

### ACL Configuration

```powershell
# Restrict data directory to FalconDB service account only
$acl = Get-Acl "C:\ProgramData\FalconDB"
$acl.SetAccessRuleProtection($true, $false)  # disable inheritance
$rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
    "NT SERVICE\FalconDB", "FullControl", "ContainerInherit,ObjectInherit", "None", "Allow")
$acl.AddAccessRule($rule)
$adminRule = New-Object System.Security.AccessControl.FileSystemAccessRule(
    "BUILTIN\Administrators", "FullControl", "ContainerInherit,ObjectInherit", "None", "Allow")
$acl.AddAccessRule($adminRule)
Set-Acl "C:\ProgramData\FalconDB" $acl
```

### TLS Certificate Paths

| Certificate | Default Path | Purpose |
|-------------|-------------|---------|
| Server cert | `C:\ProgramData\FalconDB\certs\server.crt` | PG wire + admin HTTPS |
| Server key | `C:\ProgramData\FalconDB\certs\server.key` | Private key |
| CA cert | `C:\ProgramData\FalconDB\certs\ca.crt` | Client verification |
| Replication cert | `C:\ProgramData\FalconDB\certs\repl.crt` | Inter-node mTLS |

### Audit Logging

Enable Windows Event Log integration:

```toml
[logging]
# Write to both file and Windows Event Log
event_log = true
event_log_source = "FalconDB"
```

---

## 8. Quick Reference Checklist

### Before First Production Deploy

```powershell
# 1. OS version
[System.Environment]::OSVersion.Version  # Should be 10.0.20348+ (Server 2022)

# 2. Filesystem
Get-Volume | Where-Object DriveLetter -eq 'C' | Select-Object FileSystemType
# Should be: NTFS

# 3. Physical memory
(Get-CimInstance Win32_PhysicalMemory | Measure-Object Capacity -Sum).Sum / 1GB
# Should be >= 16 GB for production

# 4. Antivirus exclusions
Get-MpPreference | Select-Object -ExpandProperty ExclusionPath
# Should include FalconDB data/WAL directories

# 5. Large pages privilege
whoami /priv | Select-String "SeLockMemoryPrivilege"
# Should show: Enabled

# 6. TCP settings
netsh int tcp show global
# AutoTuningLevel should be: normal
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
data_dir = "C:\\ProgramData\\FalconDB\\data"

[wal]
sync_mode = "fsync"
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
| WAL fsync p99 | `SHOW falcon.wal_stats` | > 3ms (NVMe), > 15ms (SSD) |
| Replication lag | `SHOW falcon.replication_lag` | > 500ms |
| Memory usage | `SHOW falcon.memory_status` | > 80% of hard limit |
| GC frequency | `SHOW falcon.gc_stats` | Stalled for > 10s |
| Connection count | `:8080/metrics` | > 80% of max_connections |
| Windows Event Log | Event Viewer → Application | Error/Critical events |

---

*This guide is specific to Windows Server 2022 and Windows 11. For Linux deployment,
see [linux_ubuntu_24_04.md](linux_ubuntu_24_04.md). For general operations,
see [docs/OPERATIONS.md](../OPERATIONS.md).*
