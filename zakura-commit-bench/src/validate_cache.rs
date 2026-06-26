//! Validate cached block artifacts before replaying them.

use std::path::PathBuf;

use clap::Args;
use color_eyre::eyre::{bail, eyre, Result, WrapErr};

use zebra_chain::{block, serialization::ZcashDeserializeInto};

use crate::fetch::{block_path, roots_path, CachedRoots};

#[derive(Args, Debug)]
pub struct ValidateArgs {
    /// Directory cached block bytes were written to by `fetch`.
    #[arg(long, default_value = "target/zakura-commit-bench/blocks")]
    pub cache_dir: PathBuf,

    /// First cached height to validate.
    #[arg(long, default_value_t = 0)]
    pub start: u32,

    /// Number of contiguous cached blocks to validate.
    #[arg(long, default_value_t = 401, conflicts_with = "end")]
    pub blocks: u32,

    /// Last cached height to validate (inclusive). Overrides `--blocks`.
    #[arg(long)]
    pub end: Option<u32>,

    /// Also require and validate cached `z_gettreestate` roots for each height.
    #[arg(long, default_value_t = false)]
    pub with_roots: bool,
}

pub fn run(args: ValidateArgs) -> Result<()> {
    if args.end.is_none() && args.blocks == 0 {
        bail!("--blocks must be greater than 0 when --end is not supplied");
    }

    let end = args
        .end
        .unwrap_or_else(|| args.start.saturating_add(args.blocks).saturating_sub(1));
    if end < args.start {
        bail!("--end ({end}) must be >= --start ({})", args.start);
    }

    let mut total_bytes = 0u64;
    let mut previous_hash = None;
    let mut first_hash = None;
    let mut last_hash = None;

    for height in args.start..=end {
        let path = block_path(&args.cache_dir, height);
        let bytes = std::fs::read(&path)
            .wrap_err_with(|| format!("missing cached block {height} at {}", path.display()))?;
        let block: block::Block = bytes
            .zcash_deserialize_into()
            .wrap_err_with(|| format!("cached block {height} failed to deserialize"))?;

        match block.coinbase_height() {
            Some(block::Height(actual)) if actual == height => {}
            other => bail!("cached block {height} has coinbase height {other:?}"),
        }

        if let Some(expected_previous_hash) = previous_hash {
            if block.header.previous_block_hash != expected_previous_hash {
                bail!(
                    "cached block {height} does not link to the previous cached block: \
                     previous_block_hash={} expected={}",
                    block.header.previous_block_hash,
                    expected_previous_hash
                );
            }
        }

        let hash = block.hash();
        first_hash.get_or_insert(hash);
        last_hash = Some(hash);
        previous_hash = Some(hash);
        total_bytes = total_bytes.saturating_add(
            u64::try_from(bytes.len()).map_err(|_| eyre!("cached block {height} is too large"))?,
        );

        if args.with_roots {
            validate_roots(&args.cache_dir, height)?;
        }
    }

    let count = u64::from(end.saturating_sub(args.start)) + 1;
    println!("cache OK:       {}..={end} ({count} blocks)", args.start);
    println!("cache dir:      {}", args.cache_dir.display());
    // This lossy cast is only for human-readable MiB output, not validation logic.
    println!(
        "total bytes:    {total_bytes} ({:.2} MiB)",
        total_bytes as f64 / 1024.0 / 1024.0
    );
    if let Some(hash) = first_hash {
        println!("first hash:     {hash}");
    }
    if let Some(hash) = last_hash {
        println!("last hash:      {hash}");
    }

    Ok(())
}

fn validate_roots(cache_dir: &std::path::Path, height: u32) -> Result<()> {
    let path = roots_path(cache_dir, height);
    let raw = std::fs::read(&path)
        .wrap_err_with(|| format!("missing cached roots {height} at {}", path.display()))?;
    let roots: CachedRoots = serde_json::from_slice(&raw)
        .wrap_err_with(|| format!("cached roots {height} failed to parse"))?;
    let sapling = roots
        .sapling
        .ok_or_else(|| eyre!("cached roots {height} has no sapling finalRoot"))?;
    validate_root_hex("sapling", height, &sapling)?;
    if let Some(orchard) = roots.orchard {
        validate_root_hex("orchard", height, &orchard)?;
    }
    Ok(())
}

fn validate_root_hex(pool: &str, height: u32, hex: &str) -> Result<()> {
    let raw = hex::decode(hex.trim())
        .wrap_err_with(|| format!("cached {pool} root {height} is not hex"))?;
    if raw.len() != 32 {
        bail!(
            "cached {pool} root {height} is {} bytes, expected 32",
            raw.len()
        );
    }
    Ok(())
}
