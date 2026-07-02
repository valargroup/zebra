//! Phase 2: replay cached blocks through the real state committer, timed.
//!
//! This split replay-tooling PR runs the legacy full-recompute path that exists
//! on `ironwood-main`. The VCT sidecar CLI flag is kept for compatibility with
//! later benchmark branches, but is rejected until the header-root fast-path APIs
//! are present on this base.

use std::{path::Path, time::Instant};

use color_eyre::eyre::{bail, eyre, Result};
use zebra_chain::{block::Height, parameters::Network};
use zebra_state::FinalizedState;

use crate::{cache::CacheReader, config::state_config, prefetch, stats::Stats};

/// Replays every block in `cache_path` onto the writable DB at `base`.
///
/// `base` must be a snapshot/fork whose finalized tip is exactly `start - 1`.
/// The legacy recompute path runs. After the replay, the resulting tip hash is
/// checked against the hash recorded in the cache header.
pub fn run(
    base: &Path,
    cache_path: &Path,
    vct_sidecar: Option<&Path>,
    network: Network,
) -> Result<Stats> {
    let reader = CacheReader::open(cache_path)?;
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

    if vct_sidecar.is_some() {
        bail!("--vct-sidecar replay requires VCT fast-sync APIs not present on ironwood-main");
    }

    let config = state_config(base.to_path_buf());
    tracing::info!(base = %base.display(), "opening base fork writable");
    let mut state = FinalizedState::new_writable(&config, &network);

    match state.db.finalized_tip_height() {
        Some(tip) if tip.0 == expected_parent => {}
        Some(tip) => bail!(
            "base tip is {} but cache window starts at {start}; base must be at height {expected_parent}",
            tip.0
        ),
        None => bail!("base fork has no finalized tip; expected height {expected_parent}"),
    }

    tracing::info!(start, count = header.count, "replaying window");

    // Stream the window through a bounded prefetch: a producer thread reads,
    // deserializes, and builds each CheckpointVerifiedBlock (the verifier-side
    // `prepare_block_data`) ahead of the committer, so the timed loop measures the
    // committer only while memory stays bounded to the prefetch capacity.
    let (_producer, rx) = prefetch::spawn(reader, prefetch::capacity());

    let mut stats = Stats::default();
    let mut prev_trees = None;

    // Pull the first prepared block before starting the clock.
    let mut cur = match rx.recv() {
        Ok(item) => Some(item?),
        Err(_) => bail!("cache is empty"),
    };
    let wall_start = Instant::now();

    while let Some(p) = cur.take() {
        // With a full prefetch buffer this receive does not block; if the
        // producer lags it briefly waits (counted in the wall, not the
        // per-commit latency).
        let next = match rx.recv() {
            Ok(item) => Some(item?),
            Err(_) => None,
        };

        let height = p.height;
        let commit_start = Instant::now();
        let (_hash, trees) = state
            .commit_finalized_direct(p.cv.into(), prev_trees.take(), "replay-bench")
            .map_err(|e| eyre!("commit failed at height {height}: {e}"))?;
        let latency = commit_start.elapsed();

        prev_trees = Some(trees);
        stats.record(p.len, latency);

        let done = height - start + 1;
        if done.is_multiple_of(5000) || done == header.count {
            tracing::info!(height, done, count = header.count, "applying");
        }

        cur = next;
    }

    let wall = wall_start.elapsed();

    // Correctness gate: the committer validates each block's commitments against
    // the recomputed trees, so reaching the end height already proves consensus
    // correctness. Confirm the final tip hash equals the source tip for good
    // measure.
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
    tracing::info!(tip = tip.0 .0, "replay verified: tip hash matches source");

    println!("{}", stats.report(wall));
    Ok(stats)
}
