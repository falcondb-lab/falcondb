# FalconDB — Official Platform Support Statement

> **Version**: 1.2.0  
> **Last Updated**: 2026-02-26  
> **Purpose**: Define the officially supported, tested, and recommended runtime
> environments for FalconDB production deployments.

---

## Executive Summary

**FalconDB's official production platform is Ubuntu 24.04 LTS on x86_64 with NVMe storage.**

This is the environment where FalconDB is developed, CI-tested, benchmarked, and
performance-tuned. Other platforms are supported at varying tiers as described below.

---

## Support Tiers

| Tier | Definition | SLA |
|------|-----------|-----|
| **Tier 1** | Full CI, benchmarked, performance-tuned, recommended for production | Bug fixes within release cycle |
| **Tier 2** | CI-tested, builds and passes tests, community production use | Bug fixes on best-effort basis |
| **Tier 3** | Builds, basic tests pass, not recommended for production | Community-supported only |

---

## Platform Matrix

### Operating Systems

| OS | Architecture | Tier | CI | Benchmark | Notes |
|----|-------------|------|-----|-----------|-------|
| **Ubuntu 24.04 LTS** (kernel 6.x) | x86_64 | **Tier 1** | ✅ | ✅ | **Recommended production platform** |
| Ubuntu 22.04 LTS (kernel 5.15) | x86_64 | Tier 2 | ✅ | ✅ | Supported; io_uring available but older kernel |
| Debian 12 (Bookworm) | x86_64 | Tier 2 | — | — | Compatible; same package ecosystem as Ubuntu |
| RHEL 9 / Rocky 9 / Alma 9 | x86_64 | Tier 2 | — | — | Kernel 5.14+; systemd + cgroup v2 |
| **Windows Server 2022** | x86_64 | Tier 2 | ✅ | ✅ | IOCP backend; MSI installer available |
| Windows 11 (dev) | x86_64 | Tier 2 | ✅ | — | Development and testing |
| macOS 14+ (Sonoma) | x86_64 / aarch64 | Tier 3 | ✅ | — | Development only; no production support |
| Alpine Linux | x86_64 | Tier 3 | — | — | musl libc; Docker base image |

### Hardware Requirements

#### Minimum (Development / PoC)

| Component | Requirement |
|-----------|-------------|
| CPU | 2 cores, x86_64 |
| Memory | 4 GB |
| Storage | 10 GB SSD (any type) |
| Network | 1 Gbps (if replicated) |

#### Recommended (Production)

| Component | Requirement | Why |
|-----------|-------------|-----|
| **CPU** | 8+ cores, x86_64, single NUMA node | Avoids cross-NUMA latency for MVCC lookups |
| **Memory** | 32 GB+ DDR4/DDR5 | In-memory rowstore; shard_hard_limit = 60% of RAM |
| **Storage** | NVMe SSD, ≥ 500 GB | WAL fsync latency: 0.1–0.5ms (vs 5–15ms HDD) |
| **Network** | 10 Gbps (if replicated) | WAL streaming throughput for large write bursts |
| **NUMA** | Single-node preferred | Bind with `numactl --cpunodebind=0 --membind=0` |

#### Reference Configuration (Benchmarked)

| Component | Spec |
|-----------|------|
| CPU | AMD EPYC 7763, 8 vCPUs |
| Memory | 32 GB DDR4 |
| Storage | Samsung PM9A3 NVMe (or equivalent) |
| OS | Ubuntu 24.04 LTS, kernel 6.8 |
| Filesystem | ext4 (noatime, data=ordered) |

All numbers in [docs/benchmarks/v1.0_baseline.md](benchmarks/v1.0_baseline.md) were
collected on this reference configuration.

---

## Kernel Requirements

| Feature | Minimum Kernel | Recommended | FalconDB Usage |
|---------|---------------|-------------|----------------|
| Basic operation | 4.18+ | 6.x | Standard POSIX I/O |
| io_uring WAL backend | 5.10+ | 6.1+ | Reduced syscall overhead for WAL |
| cgroup v2 | 5.2+ | 6.x | systemd resource limits |
| io_uring fixed buffers | 5.19+ | 6.x | Zero-copy WAL writes (planned) |

### io_uring Compatibility

```bash
# Check io_uring availability
python3 -c "import os; os.close(os.open('/dev/io_uring', os.O_RDONLY))" 2>/dev/null && echo "available" || echo "not available"

# Or simply: FalconDB logs at startup
# INFO  io_uring backend initialized
# WARN  io_uring backend failed, falling back to sync: ...
```

---

## Filesystem Requirements

| Filesystem | Support Level | WAL | Data | Notes |
|------------|-------------|-----|------|-------|
| **ext4** | ✅ Tier 1 | ✅ | ✅ | Default on Ubuntu; best tested |
| **XFS** | ✅ Tier 1 | ✅ | ✅ | Excellent for large sequential I/O |
| Btrfs | ⚠️ Tier 2 | ⚠️ | ⚠️ | Disable CoW for WAL dir (`chattr +C`) |
| ZFS | ⚠️ Tier 2 | ⚠️ | ⚠️ | Disable ZIL for WAL partition |
| tmpfs | ❌ Unsupported | ❌ | ❌ | fsync is a no-op — DCG is void |
| NFS / CIFS | ❌ Unsupported | ❌ | ❌ | Network filesystem latency is unpredictable |

---

## Container & Orchestration Support

| Platform | Support | Notes |
|----------|---------|-------|
| **Docker** (Linux host) | Tier 2 | `Dockerfile` provided; bind-mount WAL volume with `--privileged` or `--cap-add SYS_ADMIN` for io_uring |
| **Kubernetes** | Tier 2 | StatefulSet with persistent volumes; requires `allowPrivilegeEscalation: true` for io_uring |
| Docker Desktop (macOS/Win) | Tier 3 | Development only; VM overhead affects I/O benchmarks |
| Podman | Tier 2 | Same as Docker with rootless caveats |

### Docker Considerations

```dockerfile
FROM ubuntu:24.04
# io_uring requires: --security-opt seccomp=unconfined
# Or a custom seccomp profile that allows io_uring_setup, io_uring_enter, io_uring_register
```

FalconDB's default seccomp profile in Docker blocks `io_uring_*` syscalls. Either:
1. Run with `--security-opt seccomp=unconfined` (not recommended for multi-tenant)
2. Use a custom seccomp profile that allows io_uring
3. Use the POSIX backend (default, no special permissions needed)

---

## Cloud Provider Guidance

| Provider | Instance Type | Storage | Tier |
|----------|-------------|---------|------|
| **AWS** | r6i.2xlarge (8 vCPU, 64 GB) | gp3 or io2 EBS | Tier 1 (Ubuntu 24.04 AMI) |
| **GCP** | n2-highmem-8 (8 vCPU, 64 GB) | pd-ssd or local SSD | Tier 1 (Ubuntu 24.04) |
| **Azure** | E8s_v5 (8 vCPU, 64 GB) | Premium SSD v2 | Tier 2 |
| **Hetzner** | CCX33 (8 vCPU, 32 GB) | Local NVMe | Tier 1 |

**EBS/Cloud disk note**: Cloud block storage (EBS, pd-ssd) has higher and less predictable
fsync latency than local NVMe. For lowest tail latency, use local instance storage and
rely on replication for durability (rather than cloud disk durability).

---

## What Is NOT Supported

| Configuration | Status | Reason |
|--------------|--------|--------|
| 32-bit architectures | ❌ | Memory-first engine requires 64-bit address space |
| ARM64 (production) | ❌ (Tier 3) | Not benchmarked; community contributions welcome |
| Network filesystems (NFS, CIFS) | ❌ | Unpredictable fsync latency voids DCG |
| Shared-nothing clusters > 32 nodes | ❌ | Not tested at this scale |
| FreeBSD / illumos | ❌ | No CI, no io_uring |
| Windows ARM64 | ❌ | No IOCP testing on ARM |

---

## Versioning & EOL

FalconDB follows the platform's own EOL schedule:

| Platform | FalconDB Support Until |
|----------|----------------------|
| Ubuntu 24.04 LTS | April 2029 (Ubuntu EOL) |
| Ubuntu 22.04 LTS | April 2027 (Ubuntu EOL) |
| Windows Server 2022 | October 2031 (Microsoft EOL) |
| RHEL 9 | May 2032 (Red Hat EOL) |

---

## How to Verify Your Environment

```bash
# Run FalconDB's built-in environment checker
./target/release/falcon doctor

# Expected output includes:
# ✓ OS: Ubuntu 24.04.1 LTS (kernel 6.8.0-45-generic)
# ✓ Filesystem: ext4 on /var/lib/falcondb
# ✓ Block device: NVMe (Samsung PM9A3)
# ✓ THP: madvise
# ✓ Swap: disabled
# ✓ File limit: 65536
# ✓ io_uring: available
```

---

## FAQ

**Q: Can I run FalconDB on CentOS 7?**  
A: No. CentOS 7 has kernel 3.10, which lacks cgroup v2, io_uring, and many performance
features FalconDB relies on. Upgrade to RHEL 9 or Rocky 9.

**Q: Is ARM64 on the roadmap?**  
A: ARM64 (aarch64) builds and passes tests on macOS (Apple Silicon). Linux ARM64
production support is planned but not yet benchmarked. Contributions welcome.

**Q: Do I need io_uring for production?**  
A: No. The POSIX backend is production-ready and used by all current deployments.
io_uring is an optimization for high-concurrency workloads where p99 tail latency
is critical. FalconDB falls back to POSIX automatically if io_uring is unavailable.

**Q: Can I use ZFS for FalconDB data?**  
A: With caveats. ZFS's copy-on-write and ARC can interact unpredictably with FalconDB's
own buffering. If you must use ZFS, set `recordsize=8K`, `logbias=throughput`, and
ensure the ZIL (SLOG) is on fast storage.
