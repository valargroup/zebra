//! Linux hardware preflight checks for zcashd-compat mode.

#[cfg(target_os = "linux")]
use std::{
    collections::HashMap,
    fs,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    thread::available_parallelism,
};

#[cfg(target_os = "linux")]
use color_eyre::eyre::{eyre, Report};
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "linux")]
use tracing::warn;

use crate::config::ZebradConfig;

#[cfg(target_os = "linux")]
const GIB: u64 = 1024 * 1024 * 1024;
#[cfg(target_os = "linux")]
const TIB: u64 = 1024 * GIB;

#[cfg(target_os = "linux")]
const MIN_CPU_LOGICAL: usize = 4;
#[cfg(target_os = "linux")]
const RECOMMENDED_CPU_LOGICAL: usize = 8;

#[cfg(target_os = "linux")]
const MIN_RAM_BYTES: u64 = 16 * GIB;
#[cfg(target_os = "linux")]
const RECOMMENDED_RAM_BYTES: u64 = 32 * GIB;

#[cfg(target_os = "linux")]
const MIN_ZEBRA_AVAILABLE_BYTES: u64 = 350 * GIB;
#[cfg(target_os = "linux")]
const MIN_ZCASHD_AVAILABLE_BYTES: u64 = 300 * GIB;
#[cfg(target_os = "linux")]
const MIN_ZEBRA_TOTAL_BYTES: u64 = 500 * GIB;
#[cfg(target_os = "linux")]
const MIN_ZCASHD_TOTAL_BYTES: u64 = 300 * GIB;
#[cfg(target_os = "linux")]
const RECOMMENDED_COMBINED_TOTAL_BYTES: u64 = TIB;

/// Runs zcashd-compat hardware preflight checks.
///
/// On Linux, checks CPU, effective memory and mount-aware disk availability.
/// On non-Linux, startup fails unless `unsafe_low_specs` is explicitly set.
pub fn run_preflight(
    config: &ZebradConfig,
    unsafe_low_specs: bool,
) -> Result<(), color_eyre::eyre::Report> {
    #[cfg(target_os = "linux")]
    {
        return run_linux_preflight(config, unsafe_low_specs);
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (config, unsafe_low_specs);
        let message = "zcashd-compat mode is supported on Linux only";

        if unsafe_low_specs {
            tracing::warn!(
                "{message}. continuing because --unsafe-low-specs was explicitly provided"
            );
            Ok(())
        } else {
            Err(color_eyre::eyre::eyre!(message))
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
enum DiskRole {
    ZebraState,
    ZcashdData,
}

#[cfg(target_os = "linux")]
impl DiskRole {
    fn label(self) -> &'static str {
        match self {
            DiskRole::ZebraState => "zebra state",
            DiskRole::ZcashdData => "zcashd datadir",
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct PathRequirement {
    role: DiskRole,
    target_path: PathBuf,
    min_available_bytes: u64,
    min_total_bytes: u64,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, Default)]
struct FilesystemRequirements {
    roles: Vec<DiskRole>,
    target_paths: Vec<PathBuf>,
    min_available_sum_bytes: u64,
    min_total_bytes: u64,
    available_bytes: u64,
    total_bytes: u64,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, Default)]
struct PreflightSummary {
    errors: Vec<String>,
    warnings: Vec<String>,
}

#[cfg(target_os = "linux")]
fn run_linux_preflight(config: &ZebradConfig, unsafe_low_specs: bool) -> Result<(), Report> {
    let zcashd_datadir = config
        .zcashd_compat
        .zcashd_datadir
        .clone()
        .unwrap_or_else(|| config.state.cache_dir.join("zcashd-compat-zcashd"));

    let mut summary = PreflightSummary::default();
    check_cpu(&mut summary)?;
    check_memory(&mut summary)?;
    check_disk(&mut summary, &config.state.cache_dir, &zcashd_datadir)?;

    for warning in finalize_preflight(summary, unsafe_low_specs)? {
        warn!("{warning}");
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn finalize_preflight(
    mut summary: PreflightSummary,
    unsafe_low_specs: bool,
) -> Result<Vec<String>, Report> {
    if !summary.errors.is_empty() {
        if unsafe_low_specs {
            summary
                .warnings
                .extend(summary.errors.into_iter().map(|error| {
                    format!(
                        "{error}. continuing because --unsafe-low-specs was explicitly provided"
                    )
                }));
        } else {
            return Err(eyre!(
                "zcashd-compat preflight failed:\n- {}",
                summary.errors.join("\n- ")
            ));
        }
    }

    Ok(summary.warnings)
}

#[cfg(target_os = "linux")]
fn check_cpu(summary: &mut PreflightSummary) -> Result<(), Report> {
    let cpu_count = available_parallelism()
        .map(NonZeroUsize::get)
        .map_err(|error| eyre!("failed to read available logical CPU count: {error}"))?;

    if cpu_count < MIN_CPU_LOGICAL {
        summary.errors.push(format!(
            "detected {cpu_count} logical CPUs, minimum required is {MIN_CPU_LOGICAL}"
        ));
    } else if cpu_count < RECOMMENDED_CPU_LOGICAL {
        summary.warnings.push(format!(
            "detected {cpu_count} logical CPUs, recommended is {RECOMMENDED_CPU_LOGICAL}"
        ));
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn check_memory(summary: &mut PreflightSummary) -> Result<(), Report> {
    let mem_total = meminfo_total_bytes()?;
    let cgroup_limit = cgroup_memory_limit_bytes()?;
    let effective_memory = cgroup_limit.map_or(mem_total, |limit| limit.min(mem_total));

    if effective_memory < MIN_RAM_BYTES {
        summary.errors.push(format!(
            "detected effective memory {}, minimum required is {}",
            human_gib(effective_memory),
            human_gib(MIN_RAM_BYTES)
        ));
    } else if effective_memory < RECOMMENDED_RAM_BYTES {
        summary.warnings.push(format!(
            "detected effective memory {}, recommended is {}",
            human_gib(effective_memory),
            human_gib(RECOMMENDED_RAM_BYTES)
        ));
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn check_disk(
    summary: &mut PreflightSummary,
    zebra_cache_dir: &Path,
    zcashd_datadir: &Path,
) -> Result<(), Report> {
    let requirements = vec![
        PathRequirement {
            role: DiskRole::ZebraState,
            target_path: zebra_cache_dir.to_path_buf(),
            min_available_bytes: MIN_ZEBRA_AVAILABLE_BYTES,
            min_total_bytes: MIN_ZEBRA_TOTAL_BYTES,
        },
        PathRequirement {
            role: DiskRole::ZcashdData,
            target_path: zcashd_datadir.to_path_buf(),
            min_available_bytes: MIN_ZCASHD_AVAILABLE_BYTES,
            min_total_bytes: MIN_ZCASHD_TOTAL_BYTES,
        },
    ];

    let grouped_filesystems = grouped_requirements_by_filesystem(&requirements)?;
    evaluate_disk_thresholds(summary, &grouped_filesystems);

    Ok(())
}

#[cfg(target_os = "linux")]
fn evaluate_disk_thresholds(
    summary: &mut PreflightSummary,
    grouped_filesystems: &HashMap<u64, FilesystemRequirements>,
) {
    let combined_total_capacity = grouped_filesystems
        .values()
        .map(|filesystem| filesystem.total_bytes)
        .sum::<u64>();

    for filesystem in grouped_filesystems.values() {
        if filesystem.total_bytes < filesystem.min_total_bytes {
            summary.errors.push(format!(
                "{} mount (paths: {}) has total capacity {}, minimum required is {}",
                role_labels(&filesystem.roles),
                display_paths(&filesystem.target_paths),
                human_gib(filesystem.total_bytes),
                human_gib(filesystem.min_total_bytes),
            ));
        }

        if filesystem.available_bytes < filesystem.min_available_sum_bytes {
            summary.errors.push(format!(
                "{} mount (paths: {}) has available space {}, minimum required is {}",
                role_labels(&filesystem.roles),
                display_paths(&filesystem.target_paths),
                human_gib(filesystem.available_bytes),
                human_gib(filesystem.min_available_sum_bytes),
            ));
        }
    }

    if combined_total_capacity < RECOMMENDED_COMBINED_TOTAL_BYTES {
        summary.warnings.push(format!(
            "combined zcashd-compat filesystem capacity is {}, recommended is {}",
            human_gib(combined_total_capacity),
            human_gib(RECOMMENDED_COMBINED_TOTAL_BYTES)
        ));
    }
}

#[cfg(target_os = "linux")]
fn grouped_requirements_by_filesystem(
    requirements: &[PathRequirement],
) -> Result<HashMap<u64, FilesystemRequirements>, Report> {
    let mut grouped = HashMap::new();

    for requirement in requirements {
        let probed_path = nearest_existing_ancestor(&requirement.target_path)?;
        let metadata = fs::metadata(&probed_path).map_err(|error| {
            eyre!(
                "failed to read metadata for {}: {error}",
                probed_path.display()
            )
        })?;
        let device_id = metadata.dev();
        let (total_bytes, available_bytes) = statvfs_bytes(&probed_path)?;

        let entry = grouped
            .entry(device_id)
            .or_insert_with(|| FilesystemRequirements {
                total_bytes,
                available_bytes,
                ..FilesystemRequirements::default()
            });

        if !entry.roles.contains(&requirement.role) {
            entry.roles.push(requirement.role);
        }
        entry.target_paths.push(requirement.target_path.clone());
        entry.min_available_sum_bytes = entry
            .min_available_sum_bytes
            .saturating_add(requirement.min_available_bytes);
        entry.min_total_bytes = entry.min_total_bytes.max(requirement.min_total_bytes);
    }

    Ok(grouped)
}

#[cfg(target_os = "linux")]
fn nearest_existing_ancestor(path: &Path) -> Result<PathBuf, Report> {
    let mut current = path.to_path_buf();

    loop {
        if current.exists() {
            return Ok(current);
        }

        if let Some(parent) = current.parent() {
            current = parent.to_path_buf();
            continue;
        }

        return Err(eyre!(
            "no existing ancestor path found for {}",
            path.display()
        ));
    }
}

#[cfg(target_os = "linux")]
fn statvfs_bytes(path: &Path) -> Result<(u64, u64), Report> {
    let stats = nix::sys::statvfs::statvfs(path).map_err(|error| {
        eyre!(
            "failed to get filesystem stats for {}: {error}",
            path.display()
        )
    })?;

    let fragment_size = stats.fragment_size();
    let total_bytes = stats.blocks().saturating_mul(fragment_size);
    let available_bytes = stats.blocks_available().saturating_mul(fragment_size);

    Ok((total_bytes, available_bytes))
}

#[cfg(target_os = "linux")]
fn meminfo_total_bytes() -> Result<u64, Report> {
    let meminfo = fs::read_to_string("/proc/meminfo")
        .map_err(|error| eyre!("failed to read /proc/meminfo: {error}"))?;
    let mem_total_kib = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| eyre!("MemTotal field missing in /proc/meminfo"))?
        .parse::<u64>()
        .map_err(|error| eyre!("failed to parse MemTotal from /proc/meminfo: {error}"))?;

    Ok(mem_total_kib.saturating_mul(1024))
}

#[cfg(target_os = "linux")]
fn cgroup_memory_limit_bytes() -> Result<Option<u64>, Report> {
    let v2_limit = parse_cgroup_limit("/sys/fs/cgroup/memory.max")?;
    let v1_limit = parse_cgroup_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes")?;

    Ok(select_cgroup_memory_limit(v2_limit, v1_limit))
}

#[cfg(target_os = "linux")]
fn parse_cgroup_limit(path: &str) -> Result<Option<u64>, Report> {
    let raw_limit = match fs::read_to_string(path) {
        Ok(raw_limit) => raw_limit,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(eyre!("failed to read {path}: {error}")),
    };

    let trimmed = raw_limit.trim();
    if trimmed.eq_ignore_ascii_case("max") {
        return Ok(None);
    }

    let parsed_limit = trimmed
        .parse::<u64>()
        .map_err(|error| eyre!("failed to parse cgroup memory limit from {path}: {error}"))?;

    // cgroup v1 can report very large sentinel values for "unlimited".
    if parsed_limit >= 0x7fff_ffff_ffff_f000 {
        return Ok(None);
    }

    Ok(Some(parsed_limit))
}

#[cfg(target_os = "linux")]
fn select_cgroup_memory_limit(v2_limit: Option<u64>, v1_limit: Option<u64>) -> Option<u64> {
    match (v2_limit, v1_limit) {
        (Some(v2), Some(v1)) => Some(v2.min(v1)),
        (Some(v2), None) => Some(v2),
        (None, Some(v1)) => Some(v1),
        (None, None) => None,
    }
}

#[cfg(target_os = "linux")]
fn role_labels(roles: &[DiskRole]) -> String {
    roles
        .iter()
        .map(|role| role.label())
        .collect::<Vec<_>>()
        .join(" + ")
}

#[cfg(target_os = "linux")]
fn display_paths(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(target_os = "linux")]
fn human_gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / GIB as f64)
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    #[test]
    fn merges_available_requirement_when_paths_share_filesystem() {
        use super::*;

        let requirements = vec![
            PathRequirement {
                role: DiskRole::ZebraState,
                target_path: PathBuf::from("/tmp"),
                min_available_bytes: MIN_ZEBRA_AVAILABLE_BYTES,
                min_total_bytes: MIN_ZEBRA_TOTAL_BYTES,
            },
            PathRequirement {
                role: DiskRole::ZcashdData,
                target_path: PathBuf::from("/tmp"),
                min_available_bytes: MIN_ZCASHD_AVAILABLE_BYTES,
                min_total_bytes: MIN_ZCASHD_TOTAL_BYTES,
            },
        ];

        let grouped = grouped_requirements_by_filesystem(&requirements)
            .expect("filesystem grouping should succeed");
        let filesystem = grouped.values().next().expect("group should not be empty");

        assert_eq!(
            filesystem.min_available_sum_bytes,
            MIN_ZEBRA_AVAILABLE_BYTES + MIN_ZCASHD_AVAILABLE_BYTES
        );
        assert_eq!(filesystem.min_total_bytes, MIN_ZEBRA_TOTAL_BYTES);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parses_cgroup_max_as_unlimited() {
        use super::*;

        assert_eq!(parse_cgroup_value("max").expect("valid cgroup value"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parses_cgroup_numeric_value() {
        use super::*;

        assert_eq!(
            parse_cgroup_value("17179869184").expect("valid cgroup value"),
            Some(17_179_869_184)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn prefers_v1_when_v2_is_unavailable() {
        use super::*;

        assert_eq!(select_cgroup_memory_limit(None, Some(16)), Some(16));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn chooses_tighter_limit_when_both_are_available() {
        use super::*;

        assert_eq!(select_cgroup_memory_limit(Some(32), Some(16)), Some(16));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reports_disk_failures_when_below_minimums() {
        use super::*;

        let mut summary = PreflightSummary::default();
        let mut grouped = HashMap::new();
        grouped.insert(
            1,
            FilesystemRequirements {
                roles: vec![DiskRole::ZebraState, DiskRole::ZcashdData],
                target_paths: vec!["/zebra".into(), "/zcashd".into()],
                min_available_sum_bytes: MIN_ZEBRA_AVAILABLE_BYTES + MIN_ZCASHD_AVAILABLE_BYTES,
                min_total_bytes: MIN_ZEBRA_TOTAL_BYTES,
                available_bytes: 200 * GIB,
                total_bytes: 400 * GIB,
            },
        );

        evaluate_disk_thresholds(&mut summary, &grouped);

        assert_eq!(summary.errors.len(), 2);
        assert!(
            summary
                .errors
                .iter()
                .any(|error| error.contains("minimum required")),
            "expected minimum requirement errors: {:?}",
            summary.errors
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bypass_turns_failures_into_warnings() {
        use super::*;

        let summary = PreflightSummary {
            errors: vec!["cpu below minimum".to_string()],
            warnings: vec!["disk below recommendation".to_string()],
        };

        let warnings = finalize_preflight(summary, true).expect("unsafe bypass should continue");

        assert_eq!(warnings.len(), 2);
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("--unsafe-low-specs")),
            "expected unsafe bypass warning message: {warnings:?}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn fails_when_below_minimum_without_bypass() {
        use super::*;

        let summary = PreflightSummary {
            errors: vec!["ram below minimum".to_string()],
            warnings: Vec::new(),
        };

        let error = finalize_preflight(summary, false)
            .expect_err("preflight should fail without unsafe bypass");
        assert!(
            error.to_string().contains("preflight failed"),
            "unexpected error: {error}"
        );
    }

    #[cfg(target_os = "linux")]
    fn parse_cgroup_value(value: &str) -> Result<Option<u64>, Report> {
        if value.trim().eq_ignore_ascii_case("max") {
            return Ok(None);
        }

        let parsed_limit = value
            .trim()
            .parse::<u64>()
            .map_err(|error| eyre!("failed to parse cgroup memory limit: {error}"))?;

        if parsed_limit >= 0x7fff_ffff_ffff_f000 {
            return Ok(None);
        }

        Ok(Some(parsed_limit))
    }
}
