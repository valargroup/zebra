//! Phase 2, write-worker altitude: replay cached blocks through the real
//! `zebra-state` write worker ([`BlockWriteSender::spawn`] /
//! `WriteBlockWorkerTask::run`), timed — one rung above [`crate::apply`], which
//! calls the committer directly.
//!
//! Driving the worker rather than the committer adds exactly the orchestration
//! the production node wraps around each commit: the in-order channel feed, the
//! one-block look-ahead note-commitment precompute overlap (idle-core hashing),
//! the park/poll loop, and the chain-tip channel updates. Blocks are read,
//! parsed, and prepared off the commit thread by a bounded [`crate::prefetch`]
//! producer and fed to the worker with a bounded in-flight window, so the timed
//! window measures the commit pipeline only while memory stays flat regardless of
//! window size.
//!
//! This split replay-tooling PR runs the legacy full-recompute path that exists
//! on `ironwood-main`. The VCT sidecar CLI flag is kept for compatibility with
//! later benchmark branches, but is rejected until the header-root fast-path APIs
//! are present on this base.

use std::{path::Path, sync::Arc, time::Instant};

use std::collections::VecDeque;

use color_eyre::eyre::{bail, eyre, Result};
use tokio::sync::{oneshot, watch};
use zebra_chain::{block::Height, parameters::Network};
use zebra_state::{BlockWriteSender, ChainTipSender, FinalizedState, NonFinalizedState};

use crate::{cache::CacheReader, config::state_config, prefetch, stats::Stats};

/// Replays every block in `cache_path` through the write worker, onto the
/// writable DB at `base` (whose finalized tip must be exactly `start - 1`).
///
/// After the replay, the resulting tip hash is checked against the hash recorded
/// in the cache header.
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
    tracing::info!(base = %base.display(), "opening base fork writable (worker)");
    let state = FinalizedState::new_writable(&config, &network);

    match state.db.finalized_tip_height() {
        Some(tip) if tip.0 == expected_parent => {}
        Some(tip) => bail!(
            "base tip is {} but cache window starts at {start}; base must be at height {expected_parent}",
            tip.0
        ),
        None => bail!("base fork has no finalized tip; expected height {expected_parent}"),
    }

    // Keep a DB handle for the post-run correctness gate: the worker drops its own
    // `FinalizedState` (and calls `db.shutdown`) on exit, but this clone shares the
    // Arc-backed RocksDB and keeps it open for the final tip read.
    let db = state.db.clone();

    // Wire the worker exactly as `StateService` does, minus the service plumbing.
    let nf = NonFinalizedState::new(&network);
    let (nf_tx, nf_rx) = watch::channel(nf.clone());
    let (tip_tx, latest, change) = ChainTipSender::new(None, &network);
    let (mut sender, invalid_rx, rejected_rx, join) =
        BlockWriteSender::spawn(state, nf, tip_tx, nf_tx, true, None);

    // These must stay alive for the whole run: dropping `invalid_rx`/`rejected_rx`
    // closes channels the worker reads as a shutdown signal; the rest are realistic
    // receivers the worker broadcasts to. We expect no resets on a valid forward
    // replay, so nothing is read off them.
    let _guards = (invalid_rx, rejected_rx, nf_rx, latest, change);

    // Stream the window through a bounded prefetch (read + deserialize + build
    // CheckpointVerifiedBlock off the commit thread) and feed it to the worker with
    // a bounded in-flight window: never more than `in_flight` blocks sent-but-not-
    // committed. This keeps memory flat regardless of window size and avoids dumping
    // a 30K backlog into the worker's unbounded channel (which would trigger a
    // RocksDB write-stall).
    let in_flight = prefetch::capacity();
    let (_producer, rx) = prefetch::spawn(reader, in_flight);

    let fin = sender
        .finalized
        .take()
        .expect("finalized sender present: should_use_finalized = true");

    let mut stats = Stats::default();
    let mut completions = VecDeque::new();
    let mut window_done = false;

    let wall_start = Instant::now();

    // Prime the in-flight window with up to `in_flight` prepared blocks.
    while completions.len() < in_flight && !window_done {
        match rx.recv() {
            Ok(item) => {
                let p = item?;
                let (tx, c) = oneshot::channel();
                fin.send((p.cv, tx))
                    .map_err(|_| eyre!("worker finalized channel closed early"))?;
                completions.push_back((c, p.len));
            }
            Err(_) => window_done = true,
        }
    }

    // Drain completions in commit order, refilling to keep the in-flight window
    // bounded. Inter-arrival deltas are the worker-side per-block latency.
    let mut last = wall_start;
    while let Some((c, len)) = completions.pop_front() {
        let committed = c
            .blocking_recv()
            .map_err(|e| eyre!("worker dropped a commit response: {e}"))?;
        committed.map_err(|e| eyre!("commit failed: {e}"))?;
        let now = Instant::now();
        stats.record(len, now - last);
        last = now;

        // Refill with the next window block.
        if !window_done {
            match rx.recv() {
                Ok(item) => {
                    let p = item?;
                    let (tx, c2) = oneshot::channel();
                    fin.send((p.cv, tx))
                        .map_err(|_| eyre!("worker finalized channel closed early"))?;
                    completions.push_back((c2, p.len));
                }
                Err(_) => window_done = true,
            }
        }
    }
    let wall = wall_start.elapsed();

    // Drop both worker senders so the worker drains and exits.
    drop(fin);
    drop(sender);

    // Correctness gate (via the cloned DB handle). Reaching the end height already
    // proves consensus correctness — the committer validates each block's
    // commitments — so this just confirms the final tip.
    let tip = db
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

    // The worker has drained and is shutting down; join it.
    if let Some(join) = join {
        if let Ok(handle) = Arc::try_unwrap(join) {
            handle
                .join()
                .map_err(|_| eyre!("write worker thread panicked"))?;
        }
    }
    tracing::info!(
        tip = tip.0 .0,
        "replay verified (worker, legacy): tip hash matches source"
    );

    println!("mode=writer-worker (legacy)");
    println!("{}", stats.report(wall));
    Ok(stats)
}
