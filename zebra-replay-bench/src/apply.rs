//! Phase 2: replay cached blocks through the real state committer, timed.
//!
//! Two modes:
//! * **legacy** — `disable_vct_fast_sync = true`, full per-block note-commitment
//!   recompute; `next_checkpoint = None`.
//! * **VCT** (`--vct-sidecar`) — `disable_vct_fast_sync = false`, the per-height
//!   anchor roots from the sidecar are written into the base fork's header-roots
//!   column family so the committer folds them in and skips the recompute. Each
//!   block is committed with its successor as `next_checkpoint`, the one-block-lag
//!   confirmation the peer-source fast path requires.

use std::{path::Path, sync::Arc, time::Instant};

use color_eyre::eyre::{bail, eyre, Result};
use zebra_chain::{
    block::{Block, Height},
    parameters::Network,
    serialization::ZcashDeserialize,
};
use zebra_state::{CheckpointVerifiedBlock, FinalizedState};

use crate::{cache::CacheReader, config::state_config, roots_cache::RootsSidecar, stats::Stats};

fn deserialize(bytes: &[u8], height: u32) -> Result<Arc<Block>> {
    Block::zcash_deserialize(bytes)
        .map(Arc::new)
        .map_err(|e| eyre!("deserializing block {height}: {e}"))
}

/// Replays every block in `cache_path` onto the writable DB at `base`.
///
/// `base` must be a snapshot/fork whose finalized tip is exactly `start - 1`.
/// When `vct_sidecar` is `Some`, the VCT fast path is exercised; otherwise the
/// legacy recompute path runs. After the replay the resulting tip hash is checked
/// against the hash recorded in the cache header.
pub fn run(
    base: &Path,
    cache_path: &Path,
    vct_sidecar: Option<&Path>,
    network: Network,
) -> Result<Stats> {
    let mut reader = CacheReader::open(cache_path)?;
    let header = reader.header();
    let expected_net = match network {
        Network::Mainnet => 0,
        _ => 1,
    };
    if header.network != expected_net {
        bail!(
            "cache network byte {} does not match the requested network",
            header.network
        );
    }
    let start = header.start_height;
    let expected_parent = start.checked_sub(1).ok_or_else(|| {
        eyre!("cache starts at genesis (height 0); apply needs a base at start-1")
    })?;

    // Load the VCT sidecar first (if any) so we fail fast on a mismatch.
    let sidecar = match vct_sidecar {
        Some(path) => {
            let s = RootsSidecar::read(path)?;
            if s.start != start {
                bail!("sidecar start {} != cache start {start}", s.start);
            }
            if s.roots.len() != header.count as usize {
                bail!(
                    "sidecar has {} roots but cache has {} blocks",
                    s.roots.len(),
                    header.count
                );
            }
            Some(s)
        }
        None => None,
    };

    // VCT mode forces the fast path on (disable_vct_fast_sync = false); legacy
    // mode forces the full recompute (= true).
    let force_legacy = sidecar.is_none();
    let config = state_config(base.to_path_buf(), force_legacy);
    tracing::info!(base = %base.display(), vct = sidecar.is_some(), "opening base fork writable");
    let mut state = FinalizedState::new_writable(&config, &network);

    match state.db.finalized_tip_height() {
        Some(tip) if tip.0 == expected_parent => {}
        Some(tip) => bail!(
            "base tip is {} but cache window starts at {start}; base must be at height {expected_parent}",
            tip.0
        ),
        None => bail!("base fork has no finalized tip; expected height {expected_parent}"),
    }

    // VCT: write the per-height roots into the base fork's header-roots column
    // family, exactly where header sync would have placed them, so the committer's
    // PeerSource reads and folds them per height.
    if let Some(s) = &sidecar {
        tracing::info!(
            roots = s.roots.len(),
            "injecting VCT roots into header-roots CF"
        );
        state
            .db
            .insert_zakura_header_commitment_roots(s.roots.iter().cloned())
            .map_err(|e| eyre!("inserting VCT roots: {e}"))?;
    }

    tracing::info!(start, count = header.count, "replaying window");

    let mut stats = Stats::default();
    let mut prev_trees = None;
    let mut height = start;
    let wall_start = Instant::now();

    // First block.
    let first = reader
        .next_block()?
        .ok_or_else(|| eyre!("cache is empty"))?;
    let mut cur: Arc<Block> = deserialize(&first, height)?;
    let mut cur_len = first.len();

    loop {
        // Look ahead one block for the successor (VCT one-block-lag); fall back to
        // the sidecar successor for the final block.
        let next = reader.next_block()?;
        let next_block = match &next {
            Some(bytes) => Some(deserialize(bytes, height + 1)?),
            None => None,
        };

        // Build next_checkpoint for the VCT path: the successor block + its auth root.
        let next_checkpoint = if let Some(s) = &sidecar {
            let successor = match &next_block {
                Some(nb) => nb.clone(),
                None => Arc::new(s.successor.clone()),
            };
            let auth = successor.auth_data_root();
            Some((successor, Some(auth)))
        } else {
            None
        };

        let cv = CheckpointVerifiedBlock::from(cur.clone());
        let commit_start = Instant::now();
        let (_hash, trees) = state
            .commit_finalized_direct(
                cv.into(),
                prev_trees.take(),
                None,
                next_checkpoint,
                if sidecar.is_some() {
                    "replay-bench-vct"
                } else {
                    "replay-bench"
                },
            )
            .map_err(|e| eyre!("commit failed at height {height}: {e}"))?;
        let latency = commit_start.elapsed();

        prev_trees = Some(trees);
        stats.record(cur_len, latency);

        let done = height - start + 1;
        if done.is_multiple_of(5000) || done == header.count {
            tracing::info!(height, done, count = header.count, "applying");
        }

        match next_block {
            Some(nb) => {
                cur = nb;
                cur_len = next.as_ref().map(|b| b.len()).unwrap_or(0);
                height += 1;
            }
            None => break,
        }
    }

    let wall = wall_start.elapsed();

    // Correctness gate: the committer validates each block's commitments (against
    // the recomputed trees in legacy mode, or the folded supplied roots in VCT
    // mode), so reaching the end height already proves consensus correctness.
    // Confirm the final tip hash equals the source tip for good measure.
    let tip = state
        .db
        .tip()
        .ok_or_else(|| eyre!("target has no tip after replay"))?;
    if tip.1 .0 != header.tip_hash {
        bail!(
            "final tip hash mismatch: target tip at height {} does not match the source snapshot",
            tip.0 .0
        );
    }
    if tip.0 != Height(start + header.count - 1) {
        bail!(
            "final tip height {} != expected {}",
            tip.0 .0,
            start + header.count - 1
        );
    }
    tracing::info!(
        tip = tip.0 .0,
        vct = sidecar.is_some(),
        "replay verified: tip hash matches source"
    );

    println!("{}", stats.report(wall));
    Ok(stats)
}
