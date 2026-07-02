//! Phase 1: read blocks from a snapshot state DB (read-only) into a flat cache.

use std::path::Path;

use color_eyre::eyre::{bail, eyre, Result};
use zebra_chain::{block::Height, parameters::Network, serialization::ZcashSerialize};
use zebra_state::{FinalizedState, HashOrHeight};

use crate::{cache::CacheWriter, config::state_config, roots_cache};

/// Reads blocks `start..=end` from the snapshot at `src` into `cache_path`.
///
/// The fork is opened writable only so RocksDB can create column families this
/// binary adds that older snapshots lack; point it at a disposable fork, never the
/// pristine snapshot. Each block's hash chain is validated as it is read, so the
/// `apply` phase can trust the cache without re-checking continuity.
pub fn run(src: &Path, cache_path: &Path, start: u32, end: u32, network: Network) -> Result<()> {
    if end < start {
        bail!("end height {end} is below start height {start}");
    }

    let config = state_config(src.to_path_buf());
    // Opened writable so RocksDB can create any column families this binary adds
    // that the (older) snapshot lacks; point this at a disposable fork, never the
    // pristine snapshot. No format upgrade runs when the fork already matches the
    // code version.
    tracing::info!(src = %src.display(), "opening source fork (writable, for CF creation)");
    let state = FinalizedState::new_writable(&config, &network);

    let tip = state
        .db
        .finalized_tip_height()
        .ok_or_else(|| eyre!("source snapshot has no finalized tip"))?;
    tracing::info!(source_tip = tip.0, "source snapshot opened");
    if end > tip.0 {
        bail!("requested end height {end} exceeds source tip {}", tip.0);
    }

    let mut writer = CacheWriter::create(cache_path, &network, Height(start))?;
    let mut prev_hash: Option<zebra_chain::block::Hash> = None;
    let mut last_hash = [0u8; 32];
    let total = end - start + 1;

    for height in start..=end {
        let block = state
            .db
            .block(HashOrHeight::Height(Height(height)))
            .ok_or_else(|| eyre!("source snapshot is missing a body at height {height}"))?;

        if let Some(prev) = prev_hash {
            if block.header.previous_block_hash != prev {
                bail!("hash chain break at height {height}: block parent != previous block hash");
            }
        }
        let hash = block.hash();
        prev_hash = Some(hash);
        last_hash = hash.0;

        let bytes = block
            .zcash_serialize_to_vec()
            .map_err(|e| eyre!("serializing block {height}: {e}"))?;
        writer.append(&bytes)?;

        let done = height - start + 1;
        if done.is_multiple_of(5000) || done == total {
            tracing::info!(height, done, total, "indexing");
        }
    }

    let count = writer.finish(last_hash)?;
    tracing::info!(count, cache = %cache_path.display(), "index complete");
    Ok(())
}

/// Reads the per-height commitment roots for `start..=end`, plus the successor
/// block at `end+1`, from the snapshot at `src` into a sidecar for later VCT
/// fast-sync replay branches.
///
/// Roots are derived through the finalized-state root range helper, which fills
/// the current [`BlockCommitmentRoots`] shape from existing finalized state.
///
/// [`BlockCommitmentRoots`]: zebra_chain::parallel::commitment_aux::BlockCommitmentRoots
pub fn run_roots(
    src: &Path,
    sidecar_path: &Path,
    start: u32,
    end: u32,
    network: Network,
) -> Result<()> {
    if end < start {
        bail!("end height {end} is below start height {start}");
    }

    let config = state_config(src.to_path_buf());
    tracing::info!(src = %src.display(), "opening source fork (writable, for CF creation)");
    let state = FinalizedState::new_writable(&config, &network);

    let tip = state
        .db
        .finalized_tip_height()
        .ok_or_else(|| eyre!("source snapshot has no finalized tip"))?;
    // Later fast-sync replay branches need the successor of the last committed
    // block (end+1) to confirm end's roots, so the window must leave one block
    // of headroom.
    if end + 1 > tip.0 {
        bail!(
            "sidecar needs a successor at height {}, but source tip is {}",
            end + 1,
            tip.0
        );
    }

    let total = end - start + 1;
    let roots = state
        .db
        .finalized_commitment_roots_by_height_range(Height(start)..=Height(end));
    if roots.len() != total as usize {
        bail!(
            "source returned {} roots for {total} requested heights",
            roots.len()
        );
    }
    tracing::info!(count = roots.len(), "derived roots");

    let successor = state
        .db
        .block(HashOrHeight::Height(Height(end + 1)))
        .ok_or_else(|| eyre!("source missing successor block at height {}", end + 1))?;

    roots_cache::write(sidecar_path, start, &roots, &successor)?;
    tracing::info!(
        count = roots.len(),
        sidecar = %sidecar_path.display(),
        "roots sidecar complete"
    );
    Ok(())
}
