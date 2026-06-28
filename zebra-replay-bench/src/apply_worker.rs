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
//! Two modes, differing only at the window boundary:
//! * **legacy** (`vct_fast_sync = false`, no sidecar): feed `start..=end`,
//!   drop both worker senders so the worker drains, hits `Disconnected`, and
//!   exits; the worker thread is joined.
//! * **VCT** (`--vct-sidecar`): the per-height roots are injected into the base
//!   fork's header-roots CF and the worker builds its own `next_checkpoint` from
//!   the look-ahead. Every committed height needs its successor buffered, so we
//!   feed one extra trailing block (the sidecar's `successor`, height `end+1`) to
//!   give `end` its successor. The worker then parks on `end+1` (whose successor
//!   is never fed) and cannot be drained to exit, so we verify against a cloned
//!   DB handle and return without joining — the parked thread is reaped at exit.

use std::{path::Path, sync::Arc, time::Instant};

use std::collections::VecDeque;

use color_eyre::eyre::{bail, eyre, Result};
use tokio::sync::{oneshot, watch};
use zebra_chain::{block::Height, parameters::Network};
use zebra_state::{
    BlockWriteSender, ChainTipSender, CheckpointVerifiedBlock, FinalizedState, NonFinalizedState,
};

use crate::{
    cache::CacheReader, config::state_config, prefetch, roots_cache::RootsSidecar, stats::Stats,
};

/// Replays every block in `cache_path` through the write worker, onto the
/// writable DB at `base` (whose finalized tip must be exactly `start - 1`).
///
/// When `vct_sidecar` is `Some`, the VCT fast path is exercised; otherwise the
/// legacy recompute path runs. After the replay the resulting tip hash is checked
/// against the hash recorded in the cache header.
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

    // VCT mode forces the fast path on (vct_fast_sync = true); legacy
    // mode forces the full recompute (vct_fast_sync = false).
    let force_legacy = sidecar.is_none();
    let config = state_config(base.to_path_buf(), force_legacy);
    tracing::info!(base = %base.display(), vct = sidecar.is_some(), "opening base fork writable (worker)");
    let state = FinalizedState::new_writable(&config, &network);

    match state.db.finalized_tip_height() {
        Some(tip) if tip.0 == expected_parent => {}
        Some(tip) => bail!(
            "base tip is {} but cache window starts at {start}; base must be at height {expected_parent}",
            tip.0
        ),
        None => bail!("base fork has no finalized tip; expected height {expected_parent}"),
    }

    // VCT: write the per-height roots into the base fork's header-roots column
    // family, exactly where header sync would have placed them, so the worker's
    // committer reads and folds them per height.
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
    // RocksDB write-stall). The worker builds its own VCT next_checkpoint from the
    // look-ahead, so it only needs the CVs fed in order.
    let in_flight = prefetch::capacity();
    let (_producer, rx) = prefetch::spawn(reader, in_flight);

    let fin = sender
        .finalized
        .take()
        .expect("finalized sender present: should_use_finalized = true");

    let mut stats = Stats::default();
    let mut completions = VecDeque::new();
    let mut window_done = false;
    let mut successor_fed = false;

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

        // Refill with the next window block; once the window is drained, feed the
        // single VCT successor (height end+1) so the last counted block can commit.
        // Its completion is intentionally not tracked — the worker parks on it.
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
        if window_done && !successor_fed {
            if let Some(s) = &sidecar {
                let scv = CheckpointVerifiedBlock::from(Arc::new(s.successor.clone()));
                let (tx, _c) = oneshot::channel();
                fin.send((scv, tx))
                    .map_err(|_| eyre!("worker finalized channel closed early (successor)"))?;
            }
            successor_fed = true;
        }
    }
    let wall = wall_start.elapsed();

    // Legacy: drop both worker senders so the worker drains and exits. VCT keeps
    // them alive — the worker parks on the unfed successor of end+1 regardless.
    if sidecar.is_none() {
        drop(fin);
        drop(sender);
    }

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

    if sidecar.is_none() {
        // Legacy: the worker has drained and is shutting down — join it.
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
    } else {
        // VCT: the worker is parked on the unfed successor of end+1 and cannot be
        // drained to exit. All counted blocks committed and the tip gate passed;
        // leave the thread parked (reaped at process exit).
        drop(join);
        tracing::info!(
            tip = tip.0 .0,
            "replay verified (worker, vct): tip hash matches source; worker left parked (reaped at exit)"
        );
    }

    let mode = if vct_sidecar.is_some() {
        "vct"
    } else {
        "legacy"
    };
    println!("mode=writer-worker ({mode})");
    println!("{}", stats.report(wall));
    Ok(stats)
}
