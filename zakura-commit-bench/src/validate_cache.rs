//! Validate cached block artifacts before replaying them.

use std::path::PathBuf;

use clap::Args;
use color_eyre::eyre::{bail, eyre, Result, WrapErr};

use zebra_chain::{block, serialization::ZcashDeserializeInto};

use crate::{
    fetch::block_path,
    mode::RunMode,
    range::{parse_network, plan_checkpoint_range},
    roots::parse_cached_roots,
    state_dir::planning_tip_from_state_dir,
};

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

    /// Replay mode to plan for when using `--state-dir`.
    #[arg(long, value_enum, default_value_t = RunMode::DirectVerifier)]
    pub mode: RunMode,

    /// Existing Zebra state cache dir whose finalized tip anchors planned ranges.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// Network (mainnet only for now; the embedded checkpoint list is per-network).
    #[arg(long, default_value = "mainnet")]
    pub network: String,

    /// Also require and validate cached `z_gettreestate` roots for each height.
    #[arg(long, default_value_t = false)]
    pub with_roots: bool,
}

pub async fn run(args: ValidateArgs) -> Result<()> {
    let (start, end) = planned_validate_range(&args).await?;

    let mut total_bytes = 0u64;
    let mut previous_hash = None;
    let mut first_hash = None;
    let mut last_hash = None;

    for height in start..=end {
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
            parse_cached_roots(&args.cache_dir, block::Height(height))?;
        }
    }

    let count = u64::from(end.saturating_sub(start)) + 1;
    println!("cache OK:       {start}..={end} ({count} blocks)");
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

async fn planned_validate_range(args: &ValidateArgs) -> Result<(u32, u32)> {
    if let Some(end) = args.end {
        if end < args.start {
            bail!("--end ({end}) must be >= --start ({})", args.start);
        }
        return Ok((args.start, end));
    }

    if args.blocks == 0 {
        bail!("--blocks must be greater than 0 when --end is not supplied");
    }

    if let Some(state_dir) = &args.state_dir {
        let network = parse_network(&args.network)?;
        let state_tip = planning_tip_from_state_dir(state_dir, &network).await?;
        let plan = plan_checkpoint_range(
            &network,
            Some(state_tip),
            args.blocks,
            args.mode,
            args.with_roots,
        )?;
        tracing::info!(
            state_tip = state_tip.0,
            requested_last = plan.requested_last,
            measured_checkpoint = plan.measured_checkpoint.0,
            load_checkpoint = plan.load_checkpoint.0,
            "planned validate-cache range from state dir"
        );
        return Ok((plan.first_height, plan.load_checkpoint.0));
    }

    Ok((
        args.start,
        args.start.saturating_add(args.blocks).saturating_sub(1),
    ))
}
