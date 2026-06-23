//! Production, cgroup-aware system-memory probe for the block-sync in-flight
//! memory ceiling (Layer 0 of the OOM defense).
//!
//! The [`MemoryProbe`](zebra_network::zakura::MemoryProbe) trait lives in
//! `zebra-network` with no `sysinfo` dependency, so all system-memory probing is
//! confined here in `zebrad` and injected through that trait. This keeps the
//! dependency flow downward and `zebra-network` free of `sysinfo`.
//!
//! Container awareness is mandatory: a node in a small cgroup on a large host
//! must resolve its `auto` ceiling against the **cgroup** limit, never the host,
//! or it would auto-provision above its `memory.max` and be `SIGKILL`ed,
//! bypassing every other OOM-defense layer. The probe therefore returns the
//! **min of host and cgroup limit** for both total and available memory.
//!
//! On Linux the probe resolves the process's active cgroup from
//! `/proc/self/cgroup` plus `/proc/self/mountinfo`, then walks ancestor cgroups
//! to find the tightest memory cap and headroom. This covers cgroup v1, v2,
//! cgroup namespaces, systemd slices, and parent-capped child cgroups.

use std::path::{Path, PathBuf};

use sysinfo::{MemoryRefreshKind, RefreshKind, System};

use zebra_network::zakura::MemoryProbe;

/// A cgroup-aware system-memory probe backed by `sysinfo`.
///
/// Both [`available`](Self::available) and [`total`](Self::total) are clamped to
/// the active cgroup limit (when one is in effect), so a containerized node never
/// sees the host figures.
#[derive(Clone, Copy, Debug)]
pub struct SysinfoProbe {
    total: u64,
    available: u64,
}

impl SysinfoProbe {
    /// Probe system memory once at startup, clamping to any active cgroup limit.
    ///
    /// Uses `System::new()` + a memory-only refresh (NOT `new_all()`, which would
    /// also scan every process). On Linux it additionally consults the cgroup v2
    /// `memory.max` / v1 `memory.limit_in_bytes` files directly so a missing or
    /// "unlimited" cgroup figure from `sysinfo` cannot let the host total leak
    /// through.
    pub fn new() -> Self {
        let mut system = System::new_with_specifics(
            RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
        );
        system.refresh_memory();

        let host_total = system.total_memory();
        let host_available = system.available_memory();

        let cgroup = effective_cgroup_memory();
        let total = cgroup
            .and_then(|memory| memory.limit)
            .map_or(host_total, |limit| host_total.min(limit));
        let available = cgroup
            .and_then(|memory| memory.available)
            .map_or(host_available.min(total), |headroom| {
                host_available.min(headroom).min(total)
            });

        Self { total, available }
    }
}

impl Default for SysinfoProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryProbe for SysinfoProbe {
    fn available(&self) -> u64 {
        self.available
    }

    fn total(&self) -> u64 {
        self.total
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CgroupMemory {
    limit: Option<u64>,
    available: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CgroupVersion {
    V1,
    V2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CgroupMount {
    version: CgroupVersion,
    mount_root: PathBuf,
    mount_point: PathBuf,
    controllers: Vec<String>,
}

fn effective_cgroup_memory() -> Option<CgroupMemory> {
    effective_cgroup_memory_from(
        Path::new("/proc/self/cgroup"),
        Path::new("/proc/self/mountinfo"),
    )
}

fn effective_cgroup_memory_from(cgroup_path: &Path, mountinfo_path: &Path) -> Option<CgroupMemory> {
    let cgroups = parse_proc_self_cgroup(&std::fs::read_to_string(cgroup_path).ok()?);
    let mounts = parse_proc_self_mountinfo(&std::fs::read_to_string(mountinfo_path).ok()?);

    let mut effective = CgroupMemory {
        limit: None,
        available: None,
    };

    for (version, path) in cgroups {
        let mount = mounts.iter().find(|mount| match version {
            CgroupVersion::V2 => mount.version == CgroupVersion::V2,
            CgroupVersion::V1 => {
                mount.version == CgroupVersion::V1
                    && mount
                        .controllers
                        .iter()
                        .any(|controller| controller == "memory")
            }
        })?;
        let memory = memory_from_cgroup_hierarchy(mount, &path);
        effective.limit = min_option(effective.limit, memory.limit);
        effective.available = min_option(effective.available, memory.available);
    }

    if effective.limit.is_some() || effective.available.is_some() {
        Some(effective)
    } else {
        None
    }
}

fn parse_proc_self_cgroup(contents: &str) -> Vec<(CgroupVersion, PathBuf)> {
    contents
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, ':');
            let _hierarchy_id = fields.next()?;
            let controllers = fields.next()?;
            let path = PathBuf::from(fields.next()?);
            if controllers.is_empty() {
                Some((CgroupVersion::V2, path))
            } else if controllers
                .split(',')
                .any(|controller| controller == "memory")
            {
                Some((CgroupVersion::V1, path))
            } else {
                None
            }
        })
        .collect()
}

fn parse_proc_self_mountinfo(contents: &str) -> Vec<CgroupMount> {
    contents
        .lines()
        .filter_map(|line| {
            let (prelude, filesystem) = line.split_once(" - ")?;
            let mut prelude_fields = prelude.split_whitespace();
            let _mount_id = prelude_fields.next()?;
            let _parent_id = prelude_fields.next()?;
            let _major_minor = prelude_fields.next()?;
            let mount_root = PathBuf::from(unescape_mountinfo_path(prelude_fields.next()?));
            let mount_point = PathBuf::from(unescape_mountinfo_path(prelude_fields.next()?));
            let mut filesystem_fields = filesystem.split_whitespace();
            let fs_type = filesystem_fields.next()?;
            let _mount_source = filesystem_fields.next()?;
            let super_options = filesystem_fields.next().unwrap_or_default();
            match fs_type {
                "cgroup2" => Some(CgroupMount {
                    version: CgroupVersion::V2,
                    mount_root,
                    mount_point,
                    controllers: Vec::new(),
                }),
                "cgroup" => Some(CgroupMount {
                    version: CgroupVersion::V1,
                    mount_root,
                    mount_point,
                    controllers: super_options.split(',').map(str::to_owned).collect(),
                }),
                _ => None,
            }
        })
        .collect()
}

fn unescape_mountinfo_path(path: &str) -> String {
    path.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

fn memory_from_cgroup_hierarchy(mount: &CgroupMount, cgroup_path: &Path) -> CgroupMemory {
    let Some(mut current) = cgroup_leaf_path(mount, cgroup_path) else {
        return CgroupMemory {
            limit: None,
            available: None,
        };
    };

    let mut memory = CgroupMemory {
        limit: None,
        available: None,
    };

    loop {
        let (limit_file, usage_file) = match mount.version {
            CgroupVersion::V2 => ("memory.max", "memory.current"),
            CgroupVersion::V1 => ("memory.limit_in_bytes", "memory.usage_in_bytes"),
        };
        let limit = read_cgroup_limit_file(&current.join(limit_file));
        memory.limit = min_option(memory.limit, limit);
        if let Some(limit) = limit {
            if let Some(usage) = read_cgroup_usage_file(&current.join(usage_file)) {
                memory.available = min_option(memory.available, Some(limit.saturating_sub(usage)));
            }
        }

        if current == mount.mount_point {
            break;
        }
        if !current.pop() {
            break;
        }
        if !current.starts_with(&mount.mount_point) {
            break;
        }
    }

    memory
}

fn cgroup_leaf_path(mount: &CgroupMount, cgroup_path: &Path) -> Option<PathBuf> {
    let relative = cgroup_path
        .strip_prefix(&mount.mount_root)
        .or_else(|_| cgroup_path.strip_prefix("/"))
        .ok()?;
    Some(mount.mount_point.join(relative))
}

fn min_option(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Parse a single cgroup usage file into a byte count, or `None`.
fn read_cgroup_usage_file(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Parse a single cgroup limit file into a meaningful byte cap, or `None`.
fn read_cgroup_limit_file(path: &Path) -> Option<u64> {
    let raw = std::fs::read_to_string(path).ok()?;
    let trimmed = raw.trim();

    // cgroup v2 uses the literal "max" to mean "no limit".
    if trimmed == "max" {
        return None;
    }

    let value: u64 = trimmed.parse().ok()?;

    // cgroup v1 reports an unset limit as a near-`u64::MAX` page-rounded sentinel
    // (e.g. 0x7fff_ffff_ffff_f000). Treat anything within a page of `u64::MAX` as
    // "unlimited" so we don't mistake it for a real cap.
    if value >= u64::MAX - 4096 {
        return None;
    }

    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- GA.2: production cgroup probe (Linux integration check) ----
    //
    // Designed to be non-flaky in CI: it never asserts an exact figure (which
    // varies by host). It asserts the *structural* container-correctness
    // guarantees — the probe reads real RAM, never exceeds the host, and when a
    // cgroup limit file is present the reported total reflects the min of host and
    // cgroup, not the bare host figure.

    #[test]
    #[cfg(target_os = "linux")]
    fn ga2_probe_reads_real_memory_and_never_exceeds_host() {
        // Bare host figures (no cgroup clamp), via the same refresh the probe uses.
        let mut system = System::new_with_specifics(
            RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
        );
        system.refresh_memory();
        let host_total = system.total_memory();

        let probe = SysinfoProbe::new();

        // The probe reads real memory (a CI host always reports a positive total).
        assert!(probe.total() > 0, "probe must read a real total");
        // Container-correctness: the probe never reports MORE than the host total —
        // it is the min of host and cgroup. `total` is stable across reads, so this
        // is a deterministic check (unlike `available`, which fluctuates between two
        // independent live reads and so cannot be cross-compared without flaking).
        assert!(
            probe.total() <= host_total,
            "probe total {} must not exceed host total {}",
            probe.total(),
            host_total
        );
        // Available can never exceed total (internal invariant, always holds).
        assert!(probe.available() <= probe.total());

        // When a real active-cgroup memory cap is present, the probe must reflect
        // it for both figures, proving it does not silently report the host figure.
        if let Some(cap) = effective_cgroup_memory().and_then(|memory| memory.limit) {
            assert!(
                probe.total() <= cap,
                "with cgroup cap {cap} present, probe total {} must be clamped",
                probe.total()
            );
            assert!(
                probe.available() <= cap,
                "with cgroup cap {cap} present, probe available {} must be clamped",
                probe.available()
            );
        }
    }

    #[test]
    fn cgroup_v2_non_root_path_walks_to_parent_cap() {
        let dir =
            std::env::temp_dir().join(format!("zebrad-cgroup-v2-path-test-{}", std::process::id()));
        let mount = dir.join("sys/fs/cgroup");
        let leaf = mount.join("kubepods/pod/node");
        std::fs::create_dir_all(&leaf).expect("create fake cgroup");
        std::fs::write(dir.join("cgroup"), "0::/kubepods/pod/node\n").expect("write cgroup");
        std::fs::write(
            dir.join("mountinfo"),
            format!("1 0 0:1 / {} rw - cgroup2 cgroup rw\n", mount.display()),
        )
        .expect("write mountinfo");
        std::fs::write(mount.join("memory.max"), "max\n").expect("write root max");
        std::fs::write(mount.join("memory.current"), "0\n").expect("write root current");
        std::fs::write(mount.join("kubepods/memory.max"), "1073741824\n")
            .expect("write parent max");
        std::fs::write(mount.join("kubepods/memory.current"), "268435456\n")
            .expect("write parent current");
        std::fs::write(leaf.join("memory.max"), "max\n").expect("write leaf max");
        std::fs::write(leaf.join("memory.current"), "134217728\n").expect("write leaf current");

        let memory = effective_cgroup_memory_from(&dir.join("cgroup"), &dir.join("mountinfo"))
            .expect("detect fake cgroup");
        assert_eq!(memory.limit, Some(1024 * 1024 * 1024));
        assert_eq!(memory.available, Some(768 * 1024 * 1024));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cgroup_v1_memory_mount_uses_process_controller_path() {
        let dir =
            std::env::temp_dir().join(format!("zebrad-cgroup-v1-path-test-{}", std::process::id()));
        let mount = dir.join("sys/fs/cgroup/memory");
        let leaf = mount.join("lxc/abc");
        std::fs::create_dir_all(&leaf).expect("create fake cgroup");
        std::fs::write(dir.join("cgroup"), "5:cpu:/ignored\n6:memory:/lxc/abc\n")
            .expect("write cgroup");
        std::fs::write(
            dir.join("mountinfo"),
            format!(
                "2 0 0:2 / {} rw - cgroup cgroup rw,memory\n",
                mount.display()
            ),
        )
        .expect("write mountinfo");
        std::fs::write(
            mount.join("memory.limit_in_bytes"),
            format!("{}\n", u64::MAX),
        )
        .expect("write root limit");
        std::fs::write(mount.join("memory.usage_in_bytes"), "0\n").expect("write root usage");
        std::fs::write(mount.join("lxc/memory.limit_in_bytes"), "536870912\n")
            .expect("write parent limit");
        std::fs::write(mount.join("lxc/memory.usage_in_bytes"), "134217728\n")
            .expect("write parent usage");
        std::fs::write(
            leaf.join("memory.limit_in_bytes"),
            format!("{}\n", u64::MAX),
        )
        .expect("write leaf limit");
        std::fs::write(leaf.join("memory.usage_in_bytes"), "67108864\n").expect("write leaf usage");

        let memory = effective_cgroup_memory_from(&dir.join("cgroup"), &dir.join("mountinfo"))
            .expect("detect fake cgroup");
        assert_eq!(memory.limit, Some(512 * 1024 * 1024));
        assert_eq!(memory.available, Some(384 * 1024 * 1024));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cgroup_v2_max_literal_is_unlimited() {
        let dir = std::env::temp_dir().join(format!("zebrad-cgroup-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("memory.max");
        std::fs::write(&path, "max\n").expect("write");
        assert_eq!(read_cgroup_limit_file(&path), None);
        std::fs::write(&path, "2147483648\n").expect("write");
        assert_eq!(read_cgroup_limit_file(&path), Some(2 * 1024 * 1024 * 1024));
        // v1 unlimited sentinel.
        std::fs::write(&path, format!("{}\n", u64::MAX)).expect("write");
        assert_eq!(read_cgroup_limit_file(&path), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cgroup_usage_file_parses_current_charge() {
        let dir =
            std::env::temp_dir().join(format!("zebrad-cgroup-usage-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("memory.current");
        std::fs::write(&path, "1073741824\n").expect("write");
        assert_eq!(read_cgroup_usage_file(&path), Some(1024 * 1024 * 1024));
        // A garbage/empty file yields None so the fallback leaves available alone.
        std::fs::write(&path, "not-a-number\n").expect("write");
        assert_eq!(read_cgroup_usage_file(&path), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
