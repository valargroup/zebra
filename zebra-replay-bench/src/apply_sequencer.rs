//! Phase 2, sequencer altitude: replay cached mainnet bodies through the **real**
//! Zakura block-sync `Sequencer` (`zebra-network`), one rung above [`crate::apply_verifier`].
//!
//! The sequencer is the body reorder + ordered-submit pipeline: bodies are fed into
//! its reorder queue (here in height order, sequentially — random/out-of-order arrival
//! is a future knob), it drains the contiguous prefix into `applying` and emits
//! `SubmitBlock`s, and a thin driver here commits each through the same real
//! `CheckpointVerifier` → `StateService` the verifier rung uses, reporting the commit
//! back so the sequencer frontier advances and releases the next blocks.
//!
//! This split replay-tooling PR runs the legacy full-recompute path that exists
//! on `ironwood-main`. The VCT sidecar CLI flag is kept for compatibility with
//! later benchmark branches, but is rejected until the header-root fast-path APIs
//! are present on this base.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use color_eyre::eyre::{bail, eyre, Result};
use futures::stream::{FuturesOrdered, StreamExt};
use tower::{buffer::Buffer, Service, ServiceExt};
use zebra_chain::{
    block::{self, Height},
    parameters::Network,
};
use zebra_consensus::CheckpointVerifier;
use zebra_network::zakura::{spawn_bench_sequencer, BenchSubmit};
use zebra_state::{FinalizedState, PruningConfig, StorageMode};

use crate::{cache::CacheReader, config::state_config, prefetch, stats::Stats};

/// Request-channel bound for the cloneable (buffered) state service the verifier
/// commits through (same as `apply_verifier`).
const STATE_BUFFER_BOUND: usize = 1024;

/// Concurrency limit passed to `zebra_state::init` (same as `apply_verifier`).
const STATE_CHECKPOINT_CONCURRENCY: usize = 1000;

/// Byte budget for the sequencer's in-flight bodies (reorder + applying). Bounds
/// memory: the sequencer backpressures the feed once this many body bytes are
/// buffered, so the `applying` set can't grow with the whole window. 4 GiB is plenty
/// of pipeline depth (>> one checkpoint range) while staying well within RAM.
const SEQUENCER_MAX_INFLIGHT_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Replays the cache through the real block-sync `Sequencer` (which submits to
/// the checkpoint verifier -> state) onto the writable base fork at `base`.
pub fn run(
    base: &Path,
    cache_path: &Path,
    vct_sidecar: Option<&Path>,
    network: Network,
    archive: bool,
    trace_dir: Option<&Path>,
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

    // Storage mode: Pruned by default (the base must already be a pruned
    // snapshot — pruning is one-way), matching the production mainnet config;
    // `--archive` opts back into full raw-tx + indexes.
    let mut config = state_config(base.to_path_buf());
    if !archive {
        config.storage_mode = StorageMode::Pruned(PruningConfig::default());
    }
    let storage_label = if archive { "archive" } else { "pruned" };

    // Optional structured Zakura JSONL traces (same tables `perf-run-mainnet` writes
    // via `[network.zakura] trace_dir`). Created up front so a stale dir surfaces early.
    let trace_dir = trace_dir.map(|p| p.to_path_buf());
    if let Some(dir) = trace_dir.as_deref() {
        std::fs::create_dir_all(dir).map_err(|e| eyre!("creating trace dir {dir:?}: {e}"))?;
        tracing::info!(trace_dir = ?dir, "Zakura JSONL tracing enabled for the sequencer");
    }

    // Open the fork directly first: assert the tip, capture the parent hash (the
    // sequencer/verifier initial tip). Drop before `zebra_state::init` reopens
    // the fork.
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

    // Same checkpoint boundary as apply_verifier: feed, commit, and gate at the
    // last checkpoint <= end.
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
        "sequencer feeds, commits, and gates to the last checkpoint <= end"
    );

    let max_checkpoint_height = checkpoint_list.max_height();
    // In-flight window / submit limit must exceed the largest checkpoint gap (same as
    // apply_verifier), else a range never completes.
    let in_flight = prefetch::capacity().max(zebra_consensus::MAX_CHECKPOINT_HEIGHT_GAP + 64);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| eyre!("building tokio runtime: {e}"))?;

    let stats = runtime.block_on(async move {
        // Real buffered StateService + checkpoint verifier on the base fork.
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

        // Per-height serialized byte length, written by the feed task and read by the
        // driver (the SubmitBlock action does not carry the size). One slot per fed
        // block; small (8 bytes each).
        let lens: Arc<Vec<AtomicU64>> =
            Arc::new((0..target).map(|_| AtomicU64::new(0)).collect());

        // Spawn the real block-sync Sequencer starting from the base tip.
        let (feeder, mut submissions, mut committer) = spawn_bench_sequencer(
            Height(expected_parent),
            Height(expected_parent),
            parent_hash,
            in_flight,
            SEQUENCER_MAX_INFLIGHT_BYTES,
            trace_dir.clone(),
        )
        .into_parts();

        let (_producer, rx) = prefetch::spawn(reader, in_flight);

        // Feed task: stream prepared blocks into the reorder queue, in height
        // order, up to the last checkpoint (`target`). Records each block's size
        // in `lens`.
        let feed_lens = lens.clone();
        let feed = tokio::spawn(async move {
            let mut fed = 0u32;
            while fed < target {
                // The prefetch producer is a std thread; do the blocking recv off the
                // async executor.
                let item = tokio::task::block_in_place(|| rx.recv());
                let prepared = match item {
                    Ok(Ok(p)) => p,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => break, // producer exhausted before target
                };
                let height = prepared.height;
                let hash = prepared.block.hash();
                let len = prepared.len as u64;
                feed_lens[(height - start) as usize].store(len, Ordering::Relaxed);
                if !feeder
                    .feed_body(Height(height), hash, prepared.block, len)
                    .await
                {
                    break; // sequencer gone
                }
                fed += 1;
            }
            Ok::<(), color_eyre::Report>(())
        });

        let mut stats = Stats::default();
        let mut verifies = FuturesOrdered::new();
        let mut submitted = 0u32;
        let mut done = 0u32;

        let wall_start = Instant::now();
        let mut last = wall_start;

        // Cadence for the `block_sync_state` trace snapshots (only when tracing is on).
        // Dense enough for a smooth throughput/applying curve; the plotter downsamples.
        let tracing_on = trace_dir.is_some();
        if tracing_on {
            committer.emit_state_snapshot();
        }

        // Drive: pull ordered submissions and verify+commit them concurrently; report
        // each commit back so the sequencer frontier advances. The sequencer's submit
        // limit bounds the in-flight set.
        while done < target {
            tokio::select! {
                biased;
                // A verify+commit completed: report it back and record it.
                Some(item) = verifies.next(), if !verifies.is_empty() => {
                    let (token, height, committed_hash, len): (_, Height, block::Hash, u64) = item?;
                    if height == last_checkpoint && committed_hash != expected_tip_hash {
                        bail!(
                            "committed hash at checkpoint {} does not match the embedded checkpoint hash",
                            last_checkpoint.0
                        );
                    }
                    committer.apply_committed(token, height, committed_hash);
                    done += 1;
                    let now = Instant::now();
                    stats.record(len as usize, now - last);
                    last = now;
                    if tracing_on && (done.is_multiple_of(20) || done == target) {
                        committer.emit_state_snapshot();
                    }
                    if done.is_multiple_of(5000) || done == target {
                        let p = committer.progress();
                        tracing::info!(
                            done, submitted,
                            verified_tip = p.verified_tip.0,
                            reorder = p.reorder_len,
                            applying = p.applying_len,
                            "sequencer-progress"
                        );
                    }
                }
                // The next ordered submission from the sequencer: start its verify.
                // The next ordered submission from the sequencer: start its verify.
                // `None` (action channel closed) just leaves the drain arm to finish.
                maybe = submissions.next_submit(), if submitted < target => {
                    if let Some(BenchSubmit { token, block }) = maybe {
                        let height = block
                            .coinbase_height()
                            .expect("submitted checkpoint block has a coinbase height");
                        let len = lens[(height.0 - start) as usize].load(Ordering::Relaxed);
                        let fut = verifier
                            .ready()
                            .await
                            .map_err(|e| eyre!("verifier not ready: {e}"))?
                            .call(block);
                        verifies.push_back(async move {
                            let committed = fut.await.map_err(|e| {
                                eyre!("verify/commit failed at height {}: {e}", height.0)
                            })?;
                            Ok::<(_, Height, block::Hash, u64), color_eyre::Report>((
                                token, height, committed, len,
                            ))
                        });
                        submitted += 1;
                    }
                }
                else => break,
            }
        }
        let wall = wall_start.elapsed();

        if done < target {
            // Drive couldn't reach the target: surface a feed error if there was one.
            match feed.await {
                Ok(Ok(())) => bail!("sequencer stalled at {done}/{target} committed (no feed error)"),
                Ok(Err(e)) => return Err(e),
                Err(e) => bail!("feed task panicked: {e}"),
            }
        }

        // Drain + flush the JSONL trace writer so the tables are complete on disk.
        committer.flush_trace().await;

        tracing::info!(
            committed = done,
            last_checkpoint = last_checkpoint.0,
            "replay verified (sequencer, legacy): committed through the last checkpoint; hash matches"
        );
        println!("mode=sequencer (legacy, {storage_label})");
        if let Some(dir) = trace_dir.as_deref() {
            println!("zakura-traces={}", dir.display());
        }
        println!("{}", stats.report(wall));
        Ok::<Stats, color_eyre::Report>(stats)
    })?;

    Ok(stats)
}
