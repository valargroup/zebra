//! Phase 2, checkpoint-verifier altitude: replay cached blocks through the real
//! `zebra-consensus::CheckpointVerifier`, which internally commits to a real
//! `zebra-state` `StateService` — one rung above [`crate::apply_worker`], which
//! drives the write worker directly.
//!
//! This is the first rung that adds genuinely new CPU above the commit pipeline:
//! per-block proof-of-work (difficulty + equihash) and Merkle-root validity, plus
//! checkpoint-range batching. The verifier is a Tower service, so unlike `apply`
//! and `apply_worker` (sync) this path runs on a multi-thread tokio runtime.
//!
//! Boundedness: blocks are read/parsed off-thread by the bounded [`crate::prefetch`]
//! producer and fed to the verifier with a bounded in-flight window (>= the largest
//! checkpoint gap, so ranges always complete), keeping memory flat. A periodic
//! progress log (fed/done/front-height) makes any stall observable.
//!
//! This split replay-tooling PR runs the legacy full-recompute path that exists
//! on `ironwood-main`. The VCT sidecar CLI flag is kept for compatibility with
//! later benchmark branches, but is rejected until the header-root fast-path APIs
//! are present on this base.

use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use color_eyre::eyre::{bail, eyre, Result};
use tower::{buffer::Buffer, Service, ServiceExt};
use zebra_chain::{block::Height, parameters::Network};
use zebra_consensus::{CheckpointVerifier, MAX_CHECKPOINT_HEIGHT_GAP};
use zebra_state::FinalizedState;

use crate::{cache::CacheReader, config::state_config, prefetch, stats::Stats};

/// Request-channel bound for the cloneable (buffered) state service the verifier
/// commits through. Generous so concurrent per-block commit requests don't queue.
const STATE_BUFFER_BOUND: usize = 1024;

/// Concurrency limit passed to `zebra_state::init` (checkpoint raw-tx retention
/// buffering); not throughput-critical for this bench.
const STATE_CHECKPOINT_CONCURRENCY: usize = 1000;

/// Replays the cache through the checkpoint verifier (which commits to a real
/// `StateService`) onto the writable base fork at `base` (tip must be `start-1`).
///
/// Feeds, commits, and counts up to the last checkpoint `<= end`, and gates that
/// block's committed hash against the embedded checkpoint hash.
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
    let end = start + header.count - 1;
    let expected_parent = start.checked_sub(1).ok_or_else(|| {
        eyre!("cache starts at genesis (height 0); apply needs a base at start-1")
    })?;

    if vct_sidecar.is_some() {
        bail!("--vct-sidecar replay requires VCT fast-sync APIs not present on ironwood-main");
    }

    let config = state_config(base.to_path_buf());

    // Open the fork directly first: assert the tip, capture the parent hash (the
    // verifier's initial tip). Drop this handle before `zebra_state::init`
    // reopens the fork.
    let parent_hash = {
        let state = FinalizedState::new_writable(&config, &network);
        match state.db.finalized_tip_height() {
            Some(tip) if tip.0 == expected_parent => {}
            Some(tip) => bail!(
                "base tip is {} but cache window starts at {start}; base must be at height {expected_parent}",
                tip.0
            ),
            None => bail!("base fork has no finalized tip; expected height {expected_parent}"),
        }
        state.db.finalized_tip_hash()
    };

    // The verifier only releases (and the worker commits) a block once its whole
    // checkpoint range is contiguous, so feed, count, and gate at the last
    // checkpoint <= end. The embedded checkpoint hash there is the gate.
    let checkpoint_list = network.checkpoint_list();
    let last_checkpoint = checkpoint_list
        .max_height_in_range(..=Height(end))
        .ok_or_else(|| eyre!("no checkpoint at or below end height {end}"))?;
    if last_checkpoint.0 < start {
        bail!(
            "the last checkpoint {} is below the window start {start}; widen the window",
            last_checkpoint.0
        );
    }
    let expected_tip_hash = checkpoint_list
        .hash(last_checkpoint)
        .ok_or_else(|| eyre!("checkpoint list has no hash for {}", last_checkpoint.0))?;
    let target = last_checkpoint.0 - start + 1;
    tracing::info!(
        start,
        end,
        last_checkpoint = last_checkpoint.0,
        committed = target,
        "verifier feeds, commits, and gates to the last checkpoint <= end"
    );

    let max_checkpoint_height = checkpoint_list.max_height();

    // The in-flight window must exceed the largest checkpoint gap, or a range never
    // completes and its blocks' futures never resolve. `ZRB_PREFETCH_CAP` can raise it.
    let in_flight = prefetch::capacity().max(MAX_CHECKPOINT_HEIGHT_GAP + 64);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| eyre!("building tokio runtime: {e}"))?;

    let stats = runtime.block_on(async move {
        // Real buffered StateService on the base fork (the verifier commits into it).
        let (state, _read, _latest, _change) = zebra_state::init(
            config,
            &network,
            max_checkpoint_height,
            STATE_CHECKPOINT_CONCURRENCY,
        )
        .await;
        let state = Buffer::new(state, STATE_BUFFER_BOUND);

        let mut verifier = CheckpointVerifier::new(
            &network,
            Some((Height(expected_parent), parent_hash)),
            state.clone(),
        );

        let (_producer, rx) = prefetch::spawn(reader, in_flight);

        let mut stats = Stats::default();
        let mut inflight = VecDeque::new();
        let mut fed = 0u32;
        let mut done = 0u32;

        // Instrumentation: a periodic task logs these counters so a stall is
        // observable — which height we're blocked awaiting, whether it's a
        // checkpoint boundary, and whether feed/commit are still advancing.
        let fed_ctr = Arc::new(AtomicU32::new(0));
        let done_ctr = Arc::new(AtomicU32::new(0));
        let front_ctr = Arc::new(AtomicU32::new(0));
        let inflight_ctr = Arc::new(AtomicU32::new(0));
        let progress = {
            let (f, d, fr, il, cl) = (
                fed_ctr.clone(),
                done_ctr.clone(),
                front_ctr.clone(),
                inflight_ctr.clone(),
                checkpoint_list.clone(),
            );
            tokio::spawn(async move {
                let mut prev = 0u32;
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let (fed, done, front, inf) = (
                        f.load(Ordering::Relaxed),
                        d.load(Ordering::Relaxed),
                        fr.load(Ordering::Relaxed),
                        il.load(Ordering::Relaxed),
                    );
                    tracing::info!(
                        fed,
                        done,
                        inflight = inf,
                        front_height = front,
                        front_is_checkpoint = cl.contains(Height(front)),
                        committed_last_5s = done.wrapping_sub(prev),
                        "verifier-progress"
                    );
                    prev = done;
                }
            })
        };

        let wall_start = Instant::now();
        let mut last = wall_start;

        // Pull, call (eagerly queues), and spawn one block's verify+commit future.
        // Returns Ok(false) if the producer ended before `target`.
        macro_rules! feed_one {
            () => {{
                match rx.recv() {
                    Ok(item) => {
                        let p = item?;
                        let (height, len) = (p.height, p.len);
                        let fut = verifier
                            .ready()
                            .await
                            .map_err(|e| eyre!("verifier not ready: {e}"))?
                            .call(p.block);
                        inflight.push_back((tokio::spawn(fut), height, len));
                        fed += 1;
                        fed_ctr.store(fed, Ordering::Relaxed);
                        true
                    }
                    Err(_) => false,
                }
            }};
        }

        // Prime the in-flight window (>= one checkpoint range, so ranges
        // complete).
        while inflight.len() < in_flight && fed < target {
            if !feed_one!() {
                break;
            }
        }

        // Drain completions in commit order until the checkpoint target; refill
        // toward `target` to keep the window bounded.
        while done < target {
            let Some((handle, height, len)) = inflight.pop_front() else {
                bail!("ran out of in-flight blocks before the counted target (feed/checkpoint logic bug)");
            };
            // Publish the block we're about to block on, so the progress task can
            // show where a stall is (and whether `height` is a checkpoint).
            front_ctr.store(height, Ordering::Relaxed);
            // `inflight.len()` is bounded by `in_flight` (a few thousand), fits u32.
            inflight_ctr.store(inflight.len() as u32, Ordering::Relaxed);
            // The verify+commit future resolves to the block's hash *after* it is
            // committed, so the returned hash is an authenticated, post-commit value.
            let committed_hash = handle
                .await
                .map_err(|e| eyre!("verifier task join error at height {height}: {e}"))?
                .map_err(|e| eyre!("verify/commit failed at height {height}: {e}"))?;
            // Correctness gate: the counted final block is a checkpoint; its
            // committed hash must match the embedded checkpoint hash.
            if height == last_checkpoint.0 && committed_hash != expected_tip_hash {
                bail!(
                    "committed hash at checkpoint {} does not match the embedded checkpoint hash",
                    last_checkpoint.0
                );
            }
            done += 1;
            done_ctr.store(done, Ordering::Relaxed);
            let now = Instant::now();
            stats.record(len, now - last);
            last = now;

            if fed < target {
                feed_one!();
            }
        }
        progress.abort();
        let wall = wall_start.elapsed();

        tracing::info!(
            committed = done,
            last_checkpoint = last_checkpoint.0,
            "replay verified (checkpoint-verifier): committed through the last checkpoint; hash matches"
        );

        println!("mode=checkpoint-verifier (legacy)");
        println!("{}", stats.report(wall));
        Ok::<Stats, color_eyre::Report>(stats)
    })?;

    Ok(stats)
}
