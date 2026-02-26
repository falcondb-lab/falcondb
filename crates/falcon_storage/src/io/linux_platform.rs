//! Linux platform detection for FalconDB production optimization.
//!
//! Detects filesystem type, block device characteristics, NUMA topology,
//! and kernel features at startup to provide advisory recommendations and
//! auto-tune WAL behavior.
//!
//! This module is only compiled on `cfg(target_os = "linux")`.

#[cfg(target_os = "linux")]
mod inner {
    use std::path::Path;

    // ═══════════════════════════════════════════════════════════════════
    // §1 — Filesystem Detection
    // ═══════════════════════════════════════════════════════════════════

    /// Detected filesystem type.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum FilesystemType {
        Ext4,
        Xfs,
        Btrfs,
        Tmpfs,
        Zfs,
        Unknown,
    }

    impl std::fmt::Display for FilesystemType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Ext4 => write!(f, "ext4"),
                Self::Xfs => write!(f, "xfs"),
                Self::Btrfs => write!(f, "btrfs"),
                Self::Tmpfs => write!(f, "tmpfs"),
                Self::Zfs => write!(f, "zfs"),
                Self::Unknown => write!(f, "unknown"),
            }
        }
    }

    /// Detected block device type.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum BlockDeviceType {
        Nvme,
        Ssd,
        Hdd,
        VirtualDisk,
        Unknown,
    }

    impl std::fmt::Display for BlockDeviceType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Nvme => write!(f, "NVMe"),
                Self::Ssd => write!(f, "SSD"),
                Self::Hdd => write!(f, "HDD"),
                Self::VirtualDisk => write!(f, "virtual-disk"),
                Self::Unknown => write!(f, "unknown"),
            }
        }
    }

    /// Detect filesystem type for a given path by reading /proc/mounts.
    pub fn detect_filesystem(path: &Path) -> FilesystemType {
        let canonical = match path.canonicalize() {
            Ok(p) => p,
            Err(_) => path.to_path_buf(),
        };
        let target = canonical.to_string_lossy();

        let mounts = match std::fs::read_to_string("/proc/mounts") {
            Ok(s) => s,
            Err(_) => return FilesystemType::Unknown,
        };

        // Find the longest mount point prefix that matches our path
        let mut best_match: Option<(&str, &str)> = None;
        for line in mounts.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 3 {
                continue;
            }
            let mount_point = parts[1];
            let fs_type = parts[2];
            if target.starts_with(mount_point) {
                match best_match {
                    None => best_match = Some((mount_point, fs_type)),
                    Some((prev_mp, _)) if mount_point.len() > prev_mp.len() => {
                        best_match = Some((mount_point, fs_type));
                    }
                    _ => {}
                }
            }
        }

        match best_match {
            Some((_, fs)) => match fs {
                "ext4" => FilesystemType::Ext4,
                "xfs" => FilesystemType::Xfs,
                "btrfs" => FilesystemType::Btrfs,
                "tmpfs" => FilesystemType::Tmpfs,
                "zfs" => FilesystemType::Zfs,
                _ => FilesystemType::Unknown,
            },
            None => FilesystemType::Unknown,
        }
    }

    /// Detect block device type for a given path.
    pub fn detect_block_device(path: &Path) -> BlockDeviceType {
        let canonical = match path.canonicalize() {
            Ok(p) => p,
            Err(_) => path.to_path_buf(),
        };

        // Find the device for this mount point
        let mounts = match std::fs::read_to_string("/proc/mounts") {
            Ok(s) => s,
            Err(_) => return BlockDeviceType::Unknown,
        };

        let target = canonical.to_string_lossy();
        let mut device: Option<String> = None;
        let mut best_len = 0;

        for line in mounts.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 {
                continue;
            }
            let mount_point = parts[1];
            if target.starts_with(mount_point) && mount_point.len() > best_len {
                best_len = mount_point.len();
                device = Some(parts[0].to_string());
            }
        }

        let dev = match device {
            Some(d) => d,
            None => return BlockDeviceType::Unknown,
        };

        // Check if NVMe
        if dev.contains("nvme") {
            return BlockDeviceType::Nvme;
        }

        // Check rotational flag via sysfs
        // Extract base device name (e.g., /dev/sda1 -> sda)
        let base_dev = dev
            .trim_start_matches("/dev/")
            .trim_end_matches(|c: char| c.is_ascii_digit());

        let rotational_path = format!("/sys/block/{}/queue/rotational", base_dev);
        match std::fs::read_to_string(&rotational_path) {
            Ok(val) => {
                if val.trim() == "0" {
                    BlockDeviceType::Ssd
                } else if val.trim() == "1" {
                    BlockDeviceType::Hdd
                } else {
                    BlockDeviceType::Unknown
                }
            }
            Err(_) => {
                // Virtual disks (virtio, xvd) may not have sysfs entries
                if dev.contains("vd") || dev.contains("xvd") {
                    BlockDeviceType::VirtualDisk
                } else {
                    BlockDeviceType::Unknown
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // §2 — NUMA Detection
    // ═══════════════════════════════════════════════════════════════════

    /// NUMA topology information.
    #[derive(Debug, Clone)]
    pub struct NumaTopology {
        /// Number of NUMA nodes detected.
        pub node_count: usize,
        /// Total online CPUs.
        pub cpu_count: usize,
        /// CPUs per NUMA node (indexed by node ID).
        pub cpus_per_node: Vec<Vec<usize>>,
        /// Whether NUMA balancing is enabled in the kernel.
        pub auto_balancing: bool,
    }

    /// Detect NUMA topology from /sys/devices/system/node.
    pub fn detect_numa() -> NumaTopology {
        let node_base = Path::new("/sys/devices/system/node");
        let mut nodes: Vec<Vec<usize>> = Vec::new();

        if let Ok(entries) = std::fs::read_dir(node_base) {
            let mut node_dirs: Vec<_> = entries
                .flatten()
                .filter(|e| {
                    e.file_name()
                        .to_string_lossy()
                        .starts_with("node")
                })
                .collect();
            node_dirs.sort_by_key(|e| e.file_name());

            for entry in &node_dirs {
                let cpulist_path = entry.path().join("cpulist");
                let cpus = match std::fs::read_to_string(&cpulist_path) {
                    Ok(s) => parse_cpu_list(s.trim()),
                    Err(_) => Vec::new(),
                };
                nodes.push(cpus);
            }
        }

        let cpu_count = nodes.iter().map(|n| n.len()).sum();
        let node_count = nodes.len().max(1);

        // Check if kernel NUMA balancing is enabled
        let auto_balancing = std::fs::read_to_string("/proc/sys/kernel/numa_balancing")
            .map(|s| s.trim() == "1")
            .unwrap_or(false);

        NumaTopology {
            node_count,
            cpu_count,
            cpus_per_node: nodes,
            auto_balancing,
        }
    }

    /// Parse a CPU list string like "0-3,8-11" into individual CPU IDs.
    fn parse_cpu_list(s: &str) -> Vec<usize> {
        let mut cpus = Vec::new();
        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            if let Some((start, end)) = part.split_once('-') {
                if let (Ok(s), Ok(e)) = (start.parse::<usize>(), end.parse::<usize>()) {
                    cpus.extend(s..=e);
                }
            } else if let Ok(cpu) = part.parse::<usize>() {
                cpus.push(cpu);
            }
        }
        cpus
    }

    // ═══════════════════════════════════════════════════════════════════
    // §3 — Kernel Feature Detection
    // ═══════════════════════════════════════════════════════════════════

    /// Kernel version info.
    #[derive(Debug, Clone)]
    pub struct KernelInfo {
        pub release: String,
        pub major: u32,
        pub minor: u32,
    }

    /// Parse kernel version from /proc/version or uname.
    pub fn detect_kernel() -> KernelInfo {
        let release = std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .unwrap_or_default()
            .trim()
            .to_string();

        let (major, minor) = parse_kernel_version(&release);

        KernelInfo {
            release,
            major,
            minor,
        }
    }

    fn parse_kernel_version(release: &str) -> (u32, u32) {
        let parts: Vec<&str> = release.split('.').collect();
        let major = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
        let minor = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        (major, minor)
    }

    /// Check if Transparent Huge Pages (THP) is enabled.
    pub fn detect_thp_status() -> String {
        std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled")
            .unwrap_or_else(|_| "unknown".into())
            .trim()
            .to_string()
    }

    /// Check the I/O scheduler for the block device underlying a path.
    pub fn detect_io_scheduler(path: &Path) -> String {
        let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        let target = canonical.to_string_lossy();

        let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
        let mut device = String::new();
        let mut best_len = 0;

        for line in mounts.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 && target.starts_with(parts[1]) && parts[1].len() > best_len {
                best_len = parts[1].len();
                device = parts[0].to_string();
            }
        }

        let base_dev = device
            .trim_start_matches("/dev/")
            .trim_end_matches(|c: char| c.is_ascii_digit());

        let sched_path = format!("/sys/block/{}/queue/scheduler", base_dev);
        std::fs::read_to_string(&sched_path)
            .unwrap_or_else(|_| "unknown".into())
            .trim()
            .to_string()
    }

    // ═══════════════════════════════════════════════════════════════════
    // §4 — Platform Report
    // ═══════════════════════════════════════════════════════════════════

    /// Severity level for advisory messages.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum AdvisorySeverity {
        Info,
        Warn,
        Critical,
    }

    /// A single advisory recommendation.
    #[derive(Debug, Clone)]
    pub struct Advisory {
        pub id: String,
        pub severity: AdvisorySeverity,
        pub message: String,
        pub recommendation: String,
    }

    /// Full platform detection report.
    #[derive(Debug, Clone)]
    pub struct PlatformReport {
        pub kernel: KernelInfo,
        pub filesystem: FilesystemType,
        pub block_device: BlockDeviceType,
        pub numa: NumaTopology,
        pub thp_status: String,
        pub io_scheduler: String,
        pub advisories: Vec<Advisory>,
    }

    /// Run full platform detection and generate advisories.
    pub fn detect_platform(data_dir: &Path) -> PlatformReport {
        let kernel = detect_kernel();
        let filesystem = detect_filesystem(data_dir);
        let block_device = detect_block_device(data_dir);
        let numa = detect_numa();
        let thp_status = detect_thp_status();
        let io_scheduler = detect_io_scheduler(data_dir);

        let mut advisories = Vec::new();

        // LPD-1: Kernel version check
        if kernel.major < 5 || (kernel.major == 5 && kernel.minor < 10) {
            advisories.push(Advisory {
                id: "LPD-1".into(),
                severity: AdvisorySeverity::Warn,
                message: format!(
                    "Kernel {} is older than recommended (5.10+). io_uring may not be available.",
                    kernel.release
                ),
                recommendation: "Upgrade to Ubuntu 24.04 LTS (kernel 6.x) for best performance.".into(),
            });
        }

        // LPD-2: Filesystem advisory
        match filesystem {
            FilesystemType::Ext4 => {
                advisories.push(Advisory {
                    id: "LPD-2a".into(),
                    severity: AdvisorySeverity::Info,
                    message: "ext4 detected — good choice for WAL workloads.".into(),
                    recommendation: "Ensure data=ordered (default) journaling mode. \
                        Use fdatasync for WAL sync (avoids metadata overhead of full fsync).".into(),
                });
            }
            FilesystemType::Xfs => {
                advisories.push(Advisory {
                    id: "LPD-2b".into(),
                    severity: AdvisorySeverity::Info,
                    message: "XFS detected — good choice for WAL workloads.".into(),
                    recommendation: "XFS uses extent-based allocation with O(1) append. \
                        fdatasync is recommended (XFS metadata journaling is separate).".into(),
                });
            }
            FilesystemType::Btrfs => {
                advisories.push(Advisory {
                    id: "LPD-2c".into(),
                    severity: AdvisorySeverity::Warn,
                    message: "Btrfs detected — CoW semantics may cause write amplification \
                        on WAL-heavy workloads.".into(),
                    recommendation: "Disable CoW for WAL directory: \
                        chattr +C /path/to/falcon_data. \
                        Or use ext4/xfs for the WAL partition.".into(),
                });
            }
            FilesystemType::Tmpfs => {
                advisories.push(Advisory {
                    id: "LPD-2d".into(),
                    severity: AdvisorySeverity::Critical,
                    message: "tmpfs detected — data is stored in RAM only. \
                        fsync is a no-op. The DCG is void.".into(),
                    recommendation: "Use a real filesystem (ext4/xfs) on NVMe/SSD for WAL.".into(),
                });
            }
            _ => {}
        }

        // LPD-3: Block device type
        if block_device == BlockDeviceType::Hdd {
            advisories.push(Advisory {
                id: "LPD-3".into(),
                severity: AdvisorySeverity::Warn,
                message: "HDD detected — fsync latency will be 5-15ms (vs 0.1-0.5ms for NVMe).".into(),
                recommendation: "Use NVMe or SSD for the WAL directory. \
                    HDD is acceptable for snapshot/backup storage only.".into(),
            });
        }

        // LPD-4: NUMA topology
        if numa.node_count > 1 {
            advisories.push(Advisory {
                id: "LPD-4".into(),
                severity: AdvisorySeverity::Info,
                message: format!(
                    "NUMA system detected: {} nodes, {} CPUs.",
                    numa.node_count, numa.cpu_count
                ),
                recommendation: "Pin FalconDB to a single NUMA node: \
                    numactl --cpunodebind=0 --membind=0 ./falcon. \
                    Or configure shard-to-node affinity in falcon.toml.".into(),
            });
        }

        // LPD-5: THP advisory
        if thp_status.contains("[always]") {
            advisories.push(Advisory {
                id: "LPD-5".into(),
                severity: AdvisorySeverity::Warn,
                message: "Transparent Huge Pages (THP) is set to 'always'. \
                    This can cause latency spikes due to compaction.".into(),
                recommendation: "Set THP to 'madvise': \
                    echo madvise > /sys/kernel/mm/transparent_hugepage/enabled. \
                    FalconDB does not currently use madvise(MADV_HUGEPAGE), \
                    so 'madvise' effectively disables THP for FalconDB.".into(),
            });
        }

        // LPD-6: I/O scheduler
        if io_scheduler.contains("cfq") {
            advisories.push(Advisory {
                id: "LPD-6".into(),
                severity: AdvisorySeverity::Warn,
                message: "CFQ I/O scheduler detected — suboptimal for NVMe/SSD.".into(),
                recommendation: "Use 'none' (noop) or 'mq-deadline' for NVMe: \
                    echo none > /sys/block/nvme0n1/queue/scheduler".into(),
            });
        }

        PlatformReport {
            kernel,
            filesystem,
            block_device,
            numa,
            thp_status,
            io_scheduler,
            advisories,
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // §5 — Tests
    // ═══════════════════════════════════════════════════════════════════

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse_cpu_list_simple() {
            assert_eq!(parse_cpu_list("0-3"), vec![0, 1, 2, 3]);
        }

        #[test]
        fn test_parse_cpu_list_mixed() {
            assert_eq!(parse_cpu_list("0-3,8-11"), vec![0, 1, 2, 3, 8, 9, 10, 11]);
        }

        #[test]
        fn test_parse_cpu_list_single() {
            assert_eq!(parse_cpu_list("5"), vec![5]);
        }

        #[test]
        fn test_parse_cpu_list_empty() {
            assert_eq!(parse_cpu_list(""), Vec::<usize>::new());
        }

        #[test]
        fn test_parse_kernel_version() {
            assert_eq!(parse_kernel_version("6.8.0-45-generic"), (6, 8));
            assert_eq!(parse_kernel_version("5.15.0"), (5, 15));
            assert_eq!(parse_kernel_version(""), (0, 0));
        }

        #[test]
        fn test_filesystem_type_display() {
            assert_eq!(FilesystemType::Ext4.to_string(), "ext4");
            assert_eq!(FilesystemType::Xfs.to_string(), "xfs");
            assert_eq!(FilesystemType::Btrfs.to_string(), "btrfs");
        }
    }
}

#[cfg(target_os = "linux")]
pub use inner::*;
