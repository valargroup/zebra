//! Download + extract a Zebra mainnet state snapshot so `run --state-dir` can
//! hydrate from it and benchmark commits in a post-NU5 (sandblasting) range,
//! instead of replaying from genesis.

use std::{path::PathBuf, process::Command};

use clap::Args;
use color_eyre::eyre::{bail, eyre, Result, WrapErr};

use crate::state_dir::{
    ensure_empty_or_replace, expected_tip_from_snapshot_name, write_metadata, StateDirMetadata,
};

/// Default snapshot: pruned mainnet state at height 1,707,210 (post-NU5).
const DEFAULT_NAME: &str = "zebra-mainnet-20260616T032721Z-1707210";
const DEFAULT_SHA256: &str = "19ac5d24eaa4e912cc8bbd4e7f5f2aaa2b6c132854e75d93678316016f0f2769";
const DEFAULT_URLS: &[&str] = &[
    "https://zebra.valargroup.org/mainnet/historical/zebra-mainnet-20260616T032721Z-1707210.tar.zst",
    "https://zebra-snapshots.nyc3.cdn.digitaloceanspaces.com/mainnet/historical/zebra-mainnet-20260616T032721Z-1707210.tar.zst",
];

#[derive(Args, Debug)]
pub struct SnapshotArgs {
    /// Snapshot name (archive is <name>.tar.zst, extracted to <dir>/<name>).
    #[arg(long, default_value = DEFAULT_NAME)]
    pub name: String,

    /// Mirror URL(s); repeat for several. Defaults to the built-in mirrors.
    #[arg(long)]
    pub url: Vec<String>,

    /// Expected sha-256 of the archive.
    #[arg(long, default_value = DEFAULT_SHA256)]
    pub sha256: String,

    /// Snapshots directory (default ~/.zakura/snapshots).
    #[arg(long)]
    pub dir: Option<PathBuf>,

    /// Skip download/verify; just (re)extract an already-downloaded archive.
    #[arg(long, default_value_t = false)]
    pub extract_only: bool,

    /// Replace a non-empty extracted snapshot directory.
    #[arg(long, default_value_t = false)]
    pub replace: bool,
}

pub async fn run(args: SnapshotArgs) -> Result<()> {
    let dir = match args.dir {
        Some(dir) => dir,
        None => default_snapshots_dir()?,
    };
    std::fs::create_dir_all(&dir)
        .wrap_err_with(|| format!("creating snapshots dir {}", dir.display()))?;

    let archive = dir.join(format!("{}.tar.zst", args.name));
    let urls: Vec<String> = if args.url.is_empty() {
        DEFAULT_URLS.iter().map(|u| u.to_string()).collect()
    } else {
        args.url.clone()
    };

    if !args.extract_only {
        download(&archive, &urls, &args.sha256)?;
    } else if !archive.is_file() {
        bail!(
            "--extract-only set but {} does not exist",
            archive.display()
        );
    } else {
        verify_sha256(&archive, &args.sha256)?;
    }

    let extract_dir = dir.join(&args.name);
    extract(&archive, &extract_dir, args.replace)?;

    let state_dir = find_state_root(&extract_dir).ok_or_else(|| {
        eyre!(
            "extracted snapshot at {} has no `state/` dir; pass its parent to --state-dir manually",
            extract_dir.display()
        )
    })?;
    let metadata = StateDirMetadata::new_snapshot(
        args.name.clone(),
        args.sha256.clone(),
        archive.clone(),
        expected_tip_from_snapshot_name(&args.name),
    );
    write_metadata(&state_dir, &metadata)?;

    println!("\nsnapshot ready.");
    println!("  archive:   {}", archive.display());
    println!("  state dir: {}", state_dir.display());
    println!("\nprepare and benchmark commits above the snapshot tip with:");
    println!(
        "  cargo xtask zakura-commit-bench -- fetch --rpc-url http://127.0.0.1:8232 --state-dir {} --blocks 800 --mode apply-queue --with-roots",
        state_dir.display()
    );
    println!(
        "  cargo xtask zakura-commit-bench -- validate-cache --state-dir {} --blocks 800 --mode apply-queue --with-roots",
        state_dir.display()
    );
    println!(
        "  cargo xtask zakura-commit-bench -- run --state-dir {} --blocks 800 --mode apply-queue --with-roots",
        state_dir.display()
    );
    Ok(())
}

fn default_snapshots_dir() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| eyre!("HOME is not set"))?;
    Ok(PathBuf::from(home).join(".zakura").join("snapshots"))
}

fn download(archive: &std::path::Path, urls: &[String], expected_sha256: &str) -> Result<()> {
    if program_exists("aria2c") {
        // Parallel, multi-mirror, checksum-verified, resumable.
        let mut command = Command::new("aria2c");
        command
            .arg("-x16")
            .arg("-s16")
            .arg("--continue=true")
            .arg("-d")
            .arg(archive.parent().unwrap_or(std::path::Path::new(".")))
            .arg("-o")
            .arg(archive.file_name().expect("archive has a file name"));
        for url in urls {
            command.arg(url);
        }
        run_command(&mut command, "aria2c download")?;
        return verify_sha256(archive, expected_sha256);
    }

    eprintln!("note: aria2c not found; falling back to `curl` (single mirror at a time).");
    let mut last_error = None;
    for url in urls {
        match curl_download_with_resume(archive, url) {
            Ok(()) => match verify_sha256(archive, expected_sha256) {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_error = Some(error);
                    eprintln!("checksum failed for {url}; trying next mirror");
                    let _ = std::fs::remove_file(archive);
                }
            },
            Err(error) => {
                last_error = Some(error);
                eprintln!("download failed from {url}; trying next mirror");
            }
        }
    }

    Err(last_error.unwrap_or_else(|| eyre!("no download URL")))
}

fn verify_sha256(archive: &std::path::Path, expected: &str) -> Result<()> {
    if !program_exists("sha256sum") {
        eprintln!("warning: sha256sum not found; skipping checksum verification.");
        return Ok(());
    }
    let output = Command::new("sha256sum")
        .arg(archive)
        .output()
        .wrap_err("running sha256sum")?;
    if !output.status.success() {
        bail!(
            "sha256sum failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let actual = stdout
        .split_whitespace()
        .next()
        .ok_or_else(|| eyre!("sha256sum produced no output"))?;
    if !actual.eq_ignore_ascii_case(expected) {
        bail!("checksum mismatch: expected {expected}, got {actual}");
    }
    println!("checksum OK ({expected})");
    Ok(())
}

fn extract(archive: &std::path::Path, extract_dir: &std::path::Path, replace: bool) -> Result<()> {
    ensure_empty_or_replace(extract_dir, replace)?;
    // tar + zstd are the available decompressors.
    let mut command = Command::new("tar");
    command
        .arg("--use-compress-program=unzstd")
        .arg("-xf")
        .arg(archive)
        .arg("-C")
        .arg(extract_dir);
    run_command(&mut command, "tar extract")
}

fn curl_download_with_resume(archive: &std::path::Path, url: &str) -> Result<()> {
    let mut command = Command::new("curl");
    command
        .arg("-L")
        .arg("--fail")
        .arg("-C")
        .arg("-")
        .arg("-o")
        .arg(archive)
        .arg(url);

    match command.output().wrap_err("spawning curl download")? {
        output if output.status.success() => Ok(()),
        output if is_curl_resume_error(output.status.code()) => {
            eprintln!(
                "curl resume failed for {url}; retrying a clean full download from that mirror"
            );
            let _ = std::fs::remove_file(archive);
            let mut clean = Command::new("curl");
            clean
                .arg("-L")
                .arg("--fail")
                .arg("-o")
                .arg(archive)
                .arg(url);
            run_command(&mut clean, "curl clean download")
        }
        output => bail!(
            "curl download failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

fn is_curl_resume_error(code: Option<i32>) -> bool {
    // 33: range request failed. 18 can happen when resuming a truncated partial.
    matches!(code, Some(18 | 33))
}

/// Find the directory whose child is the `state/vN/<network>` tree, i.e. the
/// path to pass as `--state-dir`. Searches the extract root and one level down.
fn find_state_root(extract_dir: &std::path::Path) -> Option<PathBuf> {
    if extract_dir.join("state").is_dir() {
        return Some(extract_dir.to_path_buf());
    }
    for entry in std::fs::read_dir(extract_dir).ok()?.flatten() {
        let path = entry.path();
        if path.is_dir() && path.join("state").is_dir() {
            return Some(path);
        }
    }
    None
}

fn program_exists(program: &str) -> bool {
    Command::new(program)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn run_command(command: &mut Command, label: &str) -> Result<()> {
    let status = command
        .status()
        .wrap_err_with(|| format!("spawning {label}"))?;
    if status.success() {
        Ok(())
    } else {
        bail!("{label} failed with status {status}");
    }
}
