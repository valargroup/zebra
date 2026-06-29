//! One-time seeding of the Zakura header store on a base snapshot.
//!
//! The production block-sync apply driver commits checkpoint-class blocks through the
//! **header-authenticated** fast path (`Request::CommitCheckpointAuthenticated`), and
//! refuses any block whose height is not header-authenticated. Authentication reads
//! strictly from the Zakura header store (`zakura_header_hash`), which live header sync
//! populates. An offline base snapshot has no header sync, so this command writes the
//! frontier header rows (hash + header) for every cached block height — exactly what
//! header sync would persist — directly into the base.
//!
//! Run it **once** on the base: forks inherit the rows via the hard-link clone, and the
//! driver releases each row when it commits that height, so a fresh fork always presents
//! the full frontier above the body tip. The rows are independent of `tx_by_loc`
//! pruning, so seeding does not disturb the base's cleanly-pruned state.

use std::{path::Path, sync::Arc};

use color_eyre::eyre::{bail, eyre, Result};
use zebra_chain::{
    block::{Block, Height},
    parameters::Network,
    serialization::ZcashDeserialize,
};
use zebra_state::{FinalizedState, PruningConfig, StorageMode};

use crate::{cache::CacheReader, config::state_config};

/// Seeds the Zakura header store on `base` for every block in `cache_path`.
///
/// `base` must be at `cache.start_height - 1` (the frontier sits strictly above the
/// body tip). `archive` mirrors the run's storage mode (default Pruned).
pub fn run(base: &Path, cache_path: &Path, network: Network, archive: bool) -> Result<()> {
    let mut reader = CacheReader::open(cache_path)?;
    let header = reader.header();
    let start = header.start_height;
    let expected_parent = start.checked_sub(1).ok_or_else(|| {
        eyre!("cache starts at genesis (height 0); seeding needs a base at start-1")
    })?;

    let mut config = state_config(base.to_path_buf(), /* force_legacy */ false);
    if !archive {
        config.storage_mode = StorageMode::Pruned(PruningConfig::default());
    }

    let state = FinalizedState::new_writable(&config, &network);
    match state.db.finalized_tip_height() {
        Some(tip) if tip.0 == expected_parent => {}
        Some(tip) => bail!(
            "base tip is {} but cache window starts at {start}; base must be at height {expected_parent}",
            tip.0
        ),
        None => bail!("base has no finalized tip; expected height {expected_parent}"),
    }

    let mut height = start;
    let mut chunk: Vec<(Height, Arc<Block>)> = Vec::with_capacity(2000);
    let mut seeded = 0u64;

    let flush = |state: &FinalizedState, chunk: &mut Vec<(Height, Arc<Block>)>| -> Result<u64> {
        let n = chunk.len() as u64;
        state
            .db
            .seed_zakura_headers_from_blocks(chunk.drain(..))
            .map_err(|e| eyre!("seeding zakura header rows: {e}"))?;
        Ok(n)
    };

    loop {
        let bytes = match reader
            .next_block()
            .map_err(|e| eyre!("reading cache block {height}: {e}"))?
        {
            Some(b) => b,
            None => break,
        };
        let block = Arc::new(
            Block::zcash_deserialize(&bytes[..])
                .map_err(|e| eyre!("deserializing block {height}: {e}"))?,
        );
        chunk.push((Height(height), block));
        height += 1;
        if chunk.len() >= 2000 {
            seeded += flush(&state, &mut chunk)?;
            if seeded.is_multiple_of(20000) {
                tracing::info!(seeded, "seeding zakura header store");
            }
        }
    }
    if !chunk.is_empty() {
        seeded += flush(&state, &mut chunk)?;
    }

    let end = height.saturating_sub(1);
    tracing::info!(seeded, start, end, "zakura header store seeded");
    println!(
        "seeded {seeded} zakura header rows ({start}..={end}) into {}",
        base.display()
    );
    Ok(())
}
