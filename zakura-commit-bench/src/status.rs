//! Discover local benchmark artifacts and print reusable commands.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use clap::Args;
use color_eyre::eyre::{Result, WrapErr};

use zebra_chain::{block, parameters::Network};

use crate::{
    fetch::{block_path, roots_path},
    mode::RunMode,
    range::{parse_network, plan_checkpoint_range},
    state_dir::{finalized_tip_from_state_dir, read_metadata, StateDirMetadata, METADATA_FILE},
};

#[derive(Args, Debug)]
pub struct StatusArgs {
    /// Number of blocks to plan for the suggested commands.
    #[arg(long, default_value_t = 401)]
    pub blocks: u32,

    /// Replay mode to plan for the suggested commands.
    #[arg(long, value_enum, default_value_t = RunMode::DirectVerifier)]
    pub mode: RunMode,

    /// Require roots sidecars when checking whether a planned range is cached.
    #[arg(long, default_value_t = false)]
    pub with_roots: bool,

    /// Explicit Zebra state cache dir to report first.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// Explicit block cache dir to report first.
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,

    /// Network used for planning and optional DB tip reads.
    #[arg(long, default_value = "mainnet")]
    pub network: String,

    /// Open discovered state DBs to report their current finalized tip.
    #[arg(long, default_value_t = false)]
    pub deep: bool,
}

pub async fn run(args: StatusArgs) -> Result<()> {
    let network = parse_network(&args.network)?;
    let roots = artifact_roots(args.state_dir.as_deref(), args.cache_dir.as_deref());
    let artifacts =
        discover_artifacts(&roots, args.state_dir.as_deref(), args.cache_dir.as_deref())?;
    let planned_tip = planned_tip(&artifacts.state_dirs, args.deep, &network).await;

    println!("=== zakura-commit-bench status ===");
    println!("artifact roots:");
    for root in &roots {
        println!("  {}", root.display());
    }

    print_archives(&artifacts.archives);
    print_state_dirs(&artifacts.state_dirs, args.deep, &network).await;

    let plan = planned_tip
        .and_then(|tip| {
            plan_checkpoint_range(&network, Some(tip), args.blocks, args.mode, args.with_roots).ok()
        })
        .or_else(|| {
            plan_checkpoint_range(&network, None, args.blocks, args.mode, args.with_roots).ok()
        });

    print_caches(&artifacts.caches, plan.as_ref(), args.with_roots);
    print_trace_dirs(&artifacts.trace_dirs);
    print_suggested_commands(&artifacts, &args, plan.as_ref(), planned_tip.is_some());

    Ok(())
}

struct Artifacts {
    archives: Vec<PathBuf>,
    state_dirs: Vec<StateDirInfo>,
    caches: Vec<CacheInfo>,
    trace_dirs: Vec<PathBuf>,
}

#[derive(Clone)]
struct StateDirInfo {
    path: PathBuf,
    metadata: Option<StateDirMetadata>,
}

#[derive(Clone)]
struct CacheInfo {
    path: PathBuf,
    block_count: usize,
    root_count: usize,
    min_height: Option<u32>,
    max_height: Option<u32>,
}

fn artifact_roots(state_dir: Option<&Path>, cache_dir: Option<&Path>) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(state_dir) = state_dir {
        roots.push(state_dir.to_path_buf());
    }
    if let Some(cache_dir) = cache_dir {
        roots.push(cache_dir.to_path_buf());
    }
    if let Ok(env_roots) = std::env::var("ZAKURA_COMMIT_BENCH_ARTIFACT_ROOTS") {
        roots.extend(
            env_roots
                .split(':')
                .filter(|root| !root.is_empty())
                .map(PathBuf::from),
        );
    }
    roots.push(PathBuf::from("target/zakura-commit-bench"));
    if let Ok(home) = std::env::var("HOME") {
        roots.push(PathBuf::from(home).join(".zakura/snapshots"));
    }
    roots.push(PathBuf::from(
        "/home/evan/src/valar/experiments/commit-bench",
    ));
    roots.push(PathBuf::from(
        "/home/evan/src/valar/art/debug/benchmark/glue",
    ));

    dedup_existing_or_explicit(roots)
}

fn dedup_existing_or_explicit(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();
    for path in paths {
        if seen.insert(path.clone()) {
            deduped.push(path);
        }
    }
    deduped
}

fn discover_artifacts(
    roots: &[PathBuf],
    explicit_state_dir: Option<&Path>,
    explicit_cache_dir: Option<&Path>,
) -> Result<Artifacts> {
    let mut archives = Vec::new();
    let mut state_dirs = Vec::new();
    let mut caches = Vec::new();
    let mut trace_dirs = Vec::new();

    if let Some(state_dir) = explicit_state_dir {
        if is_state_dir(state_dir) {
            state_dirs.push(state_info(state_dir)?);
        }
    }
    if let Some(cache_dir) = explicit_cache_dir {
        if let Some(cache) = scan_cache(cache_dir)? {
            caches.push(cache);
        }
    }

    for root in roots {
        for path in walk_dirs_limited(root, 3) {
            if Some(path.as_path()) == explicit_state_dir
                || Some(path.as_path()) == explicit_cache_dir
            {
                continue;
            }
            if is_state_dir(&path) {
                state_dirs.push(state_info(&path)?);
            }
            if let Some(cache) = scan_cache(&path)? {
                caches.push(cache);
            }
            if is_trace_dir(&path) {
                trace_dirs.push(path.clone());
            }
            archives.extend(snapshot_archives_in_dir(&path)?);
        }

        if is_snapshot_archive(root) {
            archives.push(root.clone());
        }
    }

    archives.sort();
    state_dirs.sort_by(|a, b| a.path.cmp(&b.path));
    caches.sort_by(|a, b| a.path.cmp(&b.path));
    trace_dirs.sort();
    archives.dedup();
    state_dirs.dedup_by(|a, b| a.path == b.path);
    caches.dedup_by(|a, b| a.path == b.path);
    trace_dirs.dedup();

    Ok(Artifacts {
        archives,
        state_dirs,
        caches,
        trace_dirs,
    })
}

fn walk_dirs_limited(root: &Path, max_depth: usize) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if !root.is_dir() {
        return dirs;
    }
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    while let Some((dir, depth)) = stack.pop() {
        dirs.push(dir.clone());
        if depth >= max_depth {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push((path, depth + 1));
            }
        }
    }
    dirs
}

fn is_state_dir(path: &Path) -> bool {
    path.join(METADATA_FILE).is_file() || path.join("state").is_dir()
}

fn state_info(path: &Path) -> Result<StateDirInfo> {
    Ok(StateDirInfo {
        path: path.to_path_buf(),
        metadata: read_metadata(path)?,
    })
}

fn is_trace_dir(path: &Path) -> bool {
    if path.file_name().is_some_and(|name| name == "traces") {
        return true;
    }
    path.join("block_sync.jsonl").is_file()
}

fn is_snapshot_archive(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".tar.zst"))
}

fn snapshot_archives_in_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut archives = Vec::new();
    if !dir.is_dir() {
        return Ok(archives);
    }
    for entry in
        std::fs::read_dir(dir).wrap_err_with(|| format!("reading archive dir {}", dir.display()))?
    {
        let path = entry?.path();
        if is_snapshot_archive(&path) {
            archives.push(path);
        }
    }
    Ok(archives)
}

fn scan_cache(path: &Path) -> Result<Option<CacheInfo>> {
    if !path.is_dir() {
        return Ok(None);
    }

    let mut block_count = 0usize;
    let mut root_count = 0usize;
    let mut min_height = None;
    let mut max_height = None;

    for entry in std::fs::read_dir(path)
        .wrap_err_with(|| format!("reading cache candidate {}", path.display()))?
    {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if let Some(height) = parse_block_file_height(file_name) {
            block_count += 1;
            min_height = Some(min_height.map_or(height, |min: u32| min.min(height)));
            max_height = Some(max_height.map_or(height, |max: u32| max.max(height)));
        } else if parse_roots_file_height(file_name).is_some() {
            root_count += 1;
        }
    }

    if block_count == 0 && root_count == 0 {
        return Ok(None);
    }

    Ok(Some(CacheInfo {
        path: path.to_path_buf(),
        block_count,
        root_count,
        min_height,
        max_height,
    }))
}

fn parse_block_file_height(file_name: &str) -> Option<u32> {
    parse_zero_padded_height(file_name.strip_suffix(".bin")?)
}

fn parse_roots_file_height(file_name: &str) -> Option<u32> {
    parse_zero_padded_height(file_name.strip_suffix(".roots.json")?)
}

fn parse_zero_padded_height(raw: &str) -> Option<u32> {
    if raw.len() == 8 && raw.bytes().all(|byte| byte.is_ascii_digit()) {
        raw.parse().ok()
    } else {
        None
    }
}

async fn planned_tip(
    state_dirs: &[StateDirInfo],
    deep: bool,
    network: &Network,
) -> Option<block::Height> {
    let state_dir = state_dirs.first()?;
    if deep {
        if let Ok(tip) = finalized_tip_from_state_dir(&state_dir.path, network).await {
            return Some(tip);
        }
    }

    state_dir
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.expected_tip.or(metadata.rollback_new_tip))
        .map(block::Height)
}

async fn print_state_dirs(state_dirs: &[StateDirInfo], deep: bool, network: &Network) {
    println!("state dirs:");
    if state_dirs.is_empty() {
        println!("  none found");
        return;
    }
    for state_dir in state_dirs {
        println!("  {}", state_dir.path.display());
        if let Some(metadata) = &state_dir.metadata {
            if let Some(name) = &metadata.snapshot_name {
                println!("    snapshot: {name}");
            }
            if let Some(tip) = metadata.expected_tip {
                println!("    metadata tip: {tip}");
            }
            println!("    used by bench: {}", metadata.used_by_bench);
            if let Some(target) = metadata.rollback_target {
                println!("    last rollback target: {target}");
            }
        } else {
            println!("    metadata: missing");
        }
        if deep {
            match finalized_tip_from_state_dir(&state_dir.path, network).await {
                Ok(tip) => println!("    current DB tip: {}", tip.0),
                Err(error) => println!("    current DB tip: unavailable ({error})"),
            }
        }
    }
}

fn print_archives(archives: &[PathBuf]) {
    println!("snapshot archives:");
    if archives.is_empty() {
        println!("  none found");
        return;
    }
    for archive in archives {
        println!("  {}", archive.display());
    }
}

fn print_caches(caches: &[CacheInfo], plan: Option<&crate::range::RangePlan>, with_roots: bool) {
    println!("block caches:");
    if caches.is_empty() {
        println!("  none found");
        return;
    }
    for cache in caches {
        println!("  {}", cache.path.display());
        println!("    block files: {}", cache.block_count);
        println!("    root sidecars: {}", cache.root_count);
        match (cache.min_height, cache.max_height) {
            (Some(min), Some(max)) => println!("    height span: {min:08}..={max:08}"),
            _ => println!("    height span: unknown"),
        }
        if let Some(plan) = plan {
            let coverage =
                cache_coverage(cache, plan.first_height, plan.load_checkpoint.0, with_roots);
            println!(
                "    planned range {}..={}: {}",
                plan.first_height,
                plan.load_checkpoint.0,
                coverage.label()
            );
        }
    }
}

fn print_trace_dirs(trace_dirs: &[PathBuf]) {
    println!("trace dirs:");
    if trace_dirs.is_empty() {
        println!("  none found");
        return;
    }
    for trace_dir in trace_dirs {
        println!("  {}", trace_dir.display());
    }
}

fn print_suggested_commands(
    artifacts: &Artifacts,
    args: &StatusArgs,
    plan: Option<&crate::range::RangePlan>,
    use_state_dir: bool,
) {
    let Some(cache) = artifacts
        .caches
        .iter()
        .find(|cache| {
            plan.is_some_and(|plan| {
                cache_coverage(
                    cache,
                    plan.first_height,
                    plan.load_checkpoint.0,
                    args.with_roots,
                )
                .is_complete()
            })
        })
        .or_else(|| artifacts.caches.first())
    else {
        return;
    };

    let state_dir = use_state_dir
        .then(|| {
            artifacts
                .state_dirs
                .first()
                .map(|state_dir| state_dir.path.as_path())
        })
        .flatten();
    if matches!(args.mode, RunMode::ApplyQueue) && state_dir.is_none() {
        println!(
            "suggested commands: rerun status with --state-dir and --deep, or use metadata from a snapshot, to plan apply-queue commands"
        );
        return;
    }

    println!("suggested commands:");
    println!(
        "  {}",
        validate_cache_command(cache.path.as_path(), state_dir, args)
    );
    println!("  {}", run_command(cache.path.as_path(), state_dir, args));
}

fn validate_cache_command(cache_dir: &Path, state_dir: Option<&Path>, args: &StatusArgs) -> String {
    let mut command = format!(
        "cargo xtask zakura-commit-bench -- validate-cache --cache-dir {} --blocks {} --mode {} --network {}",
        cache_dir.display(),
        args.blocks,
        args.mode.as_str(),
        args.network
    );
    if let Some(state_dir) = state_dir {
        command.push_str(&format!(" --state-dir {}", state_dir.display()));
    }
    if args.with_roots {
        command.push_str(" --with-roots");
    }
    command
}

fn run_command(cache_dir: &Path, state_dir: Option<&Path>, args: &StatusArgs) -> String {
    let mut command = format!(
        "cargo xtask zakura-commit-bench -- run --cache-dir {} --blocks {} --mode {} --network {}",
        cache_dir.display(),
        args.blocks,
        args.mode.as_str(),
        args.network
    );
    if let Some(state_dir) = state_dir {
        command.push_str(&format!(" --state-dir {}", state_dir.display()));
    }
    if args.with_roots {
        command.push_str(" --with-roots");
    }
    if matches!(args.mode, RunMode::ApplyQueue) {
        command.push_str(" --disk-peers 4");
    }
    command
}

struct CacheCoverage {
    missing_blocks: usize,
    missing_roots: usize,
}

impl CacheCoverage {
    fn is_complete(&self) -> bool {
        self.missing_blocks == 0 && self.missing_roots == 0
    }

    fn label(&self) -> String {
        if self.is_complete() {
            "complete".to_string()
        } else if self.missing_roots == 0 {
            format!("missing {} block files", self.missing_blocks)
        } else {
            format!(
                "missing {} block files and {} root sidecars",
                self.missing_blocks, self.missing_roots
            )
        }
    }
}

fn cache_coverage(cache: &CacheInfo, start: u32, end: u32, with_roots: bool) -> CacheCoverage {
    let mut missing_blocks = 0usize;
    let mut missing_roots = 0usize;
    for height in start..=end {
        if !block_path(&cache.path, height).is_file() {
            missing_blocks += 1;
        }
        if with_roots && !roots_path(&cache.path, height).is_file() {
            missing_roots += 1;
        }
    }
    CacheCoverage {
        missing_blocks,
        missing_roots,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_zero_padded_cache_layout_and_root_sidecars() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("00000042.bin"), b"block").unwrap();
        std::fs::write(temp.path().join("00000043.bin"), b"block").unwrap();
        std::fs::write(temp.path().join("00000042.roots.json"), b"{}").unwrap();
        std::fs::write(temp.path().join("42.bin"), b"ignored").unwrap();

        let cache = scan_cache(temp.path()).unwrap().unwrap();

        assert_eq!(cache.block_count, 2);
        assert_eq!(cache.root_count, 1);
        assert_eq!(cache.min_height, Some(42));
        assert_eq!(cache.max_height, Some(43));

        let coverage = cache_coverage(&cache, 42, 43, true);
        assert_eq!(coverage.missing_blocks, 0);
        assert_eq!(coverage.missing_roots, 1);
    }

    #[test]
    fn parses_only_exact_zero_padded_heights() {
        assert_eq!(parse_block_file_height("00000001.bin"), Some(1));
        assert_eq!(parse_roots_file_height("00000001.roots.json"), Some(1));
        assert_eq!(parse_block_file_height("1.bin"), None);
        assert_eq!(parse_block_file_height("000000001.bin"), None);
        assert_eq!(parse_roots_file_height("00000001.bin"), None);
    }
}
