//! Phase 2, sequencer altitude: replay cached mainnet bodies through the **real**
//! Zakura block-sync `Sequencer` (`zebra-network`), one rung above [`crate::apply_verifier`].
//!
//! The sequencer is the body reorder + ordered-submit pipeline: bodies are fed into
//! its reorder queue (here in height order, sequentially — random/out-of-order arrival
//! is a future knob), it drains the contiguous prefix into `applying` and emits
//! `SubmitBlock`s.
//!
//! Those actions are driven by the **production** Zakura block-sync apply driver
//! (`zebrad::bench_api::drive_block_sync_actions`, unmodified): it applies each body
//! through the real consensus router into the real `StateService` and reports the
//! commit back to the sequencer so the frontier advances. Checkpoint-class blocks take
//! the header-authenticated fast path (`Request::CommitCheckpointAuthenticated`), which
//! requires the base's Zakura header store to be seeded first
//! (`zebra-replay-bench seed-headers`). The bench only feeds bodies and watches the
//! sequencer view for the gate checkpoint — there is no hand-rolled apply loop.
//!
//! VCT mode only. Checkpoint batching is the same as `apply_verifier`: feed up to the
//! last checkpoint `<= end` so the last range delivers the successors the worker's VCT
//! path needs, but count/gate to the second-to-last checkpoint.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use color_eyre::eyre::{bail, eyre, Result};
use tokio::sync::oneshot;
use tower::{buffer::Buffer, util::BoxService};
use zebra_chain::{block::Height, chain_tip::NoChainTip, parameters::Network};
use zebra_consensus::{router, BoxError, Config as ConsensusConfig};
use zebra_network::zakura::{
    spawn_bench_sequencer, BenchDriverParts, ZakuraSupervisorHandle, ZakuraTrace,
};
use zebra_node_services::mempool;
use zebra_state::{FinalizedState, PruningConfig, StorageMode};

use crate::{
    cache::CacheReader, config::state_config, prefetch, roots_cache::RootsSidecar, stats::Stats,
};

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

/// Replays the cache through the real block-sync `Sequencer` (which submits to the
/// checkpoint verifier → state) onto the writable base fork at `base`. VCT only.
pub fn run(
    base: &Path,
    cache_path: &Path,
    vct_sidecar: Option<&Path>,
    network: Network,
    archive: bool,
    stop_height: Option<u32>,
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
    let cache_end = start + header.count - 1;
    // Optionally bench a sub-range of a larger cache: clamp the window's effective end
    // to `--stop-height` (the checkpoint logic below still feeds to the last checkpoint
    // <= end and gates to the second-to-last, so the prefetch stops early).
    let end = match stop_height {
        Some(h) if h < start => bail!("--stop-height {h} is below the window start {start}"),
        Some(h) if h > cache_end => {
            bail!("--stop-height {h} exceeds the cache end {cache_end}")
        }
        Some(h) => h,
        None => cache_end,
    };
    let expected_parent = start.checked_sub(1).ok_or_else(|| {
        eyre!("cache starts at genesis (height 0); apply needs a base at start-1")
    })?;

    // The sequencer rung is VCT-only: it exercises the Zakura fast-sync pipeline.
    let sidecar_path =
        vct_sidecar.ok_or_else(|| eyre!("apply-sequencer is VCT-only; pass --vct-sidecar"))?;
    let sidecar = {
        let s = RootsSidecar::read(sidecar_path)?;
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
        s
    };

    // VCT (force_legacy = false). Storage mode: Pruned by default (the base must
    // already be a pruned snapshot — pruning is one-way), matching the production
    // mainnet config; `--archive` opts back into full raw-tx + indexes.
    let mut config = state_config(base.to_path_buf(), /* force_legacy */ false);
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
    // sequencer/verifier initial tip), inject the per-height VCT roots. Drop before
    // `zebra_state::init` reopens the fork.
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
        tracing::info!(
            roots = sidecar.roots.len(),
            "injecting VCT roots into header-roots CF"
        );
        state
            .db
            .insert_zakura_header_commitment_roots(sidecar.roots.iter().cloned())
            .map_err(|e| eyre!("inserting VCT roots: {e}"))?;
        state.db.finalized_tip_hash()
    };

    // Same checkpoint boundary as apply_verifier: feed to the last checkpoint <= end,
    // commit/gate to the second-to-last (the last block's successor is in the tail).
    let checkpoint_list = network.checkpoint_list();
    let feed_checkpoint = checkpoint_list
        .max_height_in_range(..=Height(end))
        .ok_or_else(|| eyre!("no checkpoint at or below end height {end}"))?;
    let last_checkpoint = checkpoint_list
        .max_height_in_range(..Height(feed_checkpoint.0))
        .ok_or_else(|| {
            eyre!(
                "window [{start}, {end}] spans fewer than two checkpoints; \
                 pick a window covering at least two checkpoints"
            )
        })?;
    if last_checkpoint.0 < start {
        bail!(
            "the second-to-last checkpoint {} is below the window start {start}; widen the window",
            last_checkpoint.0
        );
    }
    let expected_tip_hash = checkpoint_list
        .hash(last_checkpoint)
        .ok_or_else(|| eyre!("checkpoint list has no hash for {}", last_checkpoint.0))?;
    let target = last_checkpoint.0 - start + 1;
    let feed_target = feed_checkpoint.0 - start + 1;
    tracing::info!(
        start,
        end,
        feed_checkpoint = feed_checkpoint.0,
        last_checkpoint = last_checkpoint.0,
        committed = target,
        "sequencer feeds to the last checkpoint <= end, commits/gates to the second-to-last"
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
        let (state, read_state, _latest, _change) = zebra_state::init(
            config,
            &network,
            max_checkpoint_height,
            STATE_CHECKPOINT_CONCURRENCY,
        )
        .await;
        let state = Buffer::new(state, STATE_BUFFER_BOUND);
        // Drive through the production block-verifier router (`zebra_consensus::router::init`),
        // not the bare `CheckpointVerifier`, so the bench exercises the real Zakura apply
        // entry point (`Request::Commit` → checkpoint verifier → state). The router is
        // already buffered, so its synchronous verify work runs on the router's worker,
        // off this single driver loop. It seeds its checkpoint verifier from the state
        // tip (the forked base at `expected_parent`), so no explicit initial tip is
        // needed. The mempool input is never sent (block verification does not use it).
        let consensus_config = ConsensusConfig {
            checkpoint_sync: true,
            vct_fast_sync: true,
        };
        let (verifier, _tx_verifier, _bg_handles, _max_ckpt) = router::init(
            consensus_config,
            &network,
            state.clone(),
            oneshot::channel::<
                Buffer<BoxService<mempool::Request, mempool::Response, BoxError>, mempool::Request>,
            >()
            .1,
        )
        .await;

        // Spawn the real block-sync Sequencer from the base tip, split into
        // production-driver parts: the raw `BlockSyncAction` stream + an inert
        // `BlockSyncHandle` whose `BlockApplyFinished` feedback is forwarded into the
        // sequencer's `ApplyFinished` input (the hop the production reactor performs).
        let BenchDriverParts {
            feeder,
            actions,
            block_sync,
            mut committer,
        } = spawn_bench_sequencer(
            Height(expected_parent),
            Height(expected_parent),
            parent_hash,
            in_flight,
            SEQUENCER_MAX_INFLIGHT_BYTES,
            trace_dir.clone(),
        )
        .into_driver_parts();

        // Spawn the *production* Zakura block-sync apply driver against the bench's
        // action stream, state, and consensus router. It owns verify+commit+report:
        // checkpoint-class blocks take the header-authenticated fast path
        // (`Request::CommitCheckpointAuthenticated`), so the base must be header-seeded
        // (`zebra-replay-bench seed-headers`). Applies drain through `FuturesUnordered`
        // with the production checkpoint/full/combined limits — no bench apply loop.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let driver = tokio::spawn(zebrad::bench_api::drive_block_sync_actions(
            actions,
            ZakuraSupervisorHandle::new(1),
            None,
            block_sync,
            NoChainTip,
            read_state.clone(),
            verifier,
            max_checkpoint_height,
            in_flight,
            in_flight,
            in_flight,
            ZakuraTrace::noop(),
            None,
            async move {
                let _ = shutdown_rx.await;
            },
        ));

        let (_producer, rx) = prefetch::spawn_raw(reader, in_flight);

        // Feed task: stream raw bodies into the sequencer's reorder queue in height
        // order, up to the last checkpoint (`feed_target`). Accumulates fed bytes for
        // the throughput report.
        let fed_bytes = Arc::new(AtomicU64::new(0));
        let feed_bytes = fed_bytes.clone();
        let feed = tokio::spawn(async move {
            let mut fed = 0u32;
            while fed < feed_target {
                // The prefetch producer is a std thread; do the blocking recv off the
                // async executor.
                let item = tokio::task::block_in_place(|| rx.recv());
                let raw = match item {
                    Ok(Ok(p)) => p,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => break, // producer exhausted before feed_target
                };
                let height = raw.height;
                let hash = raw.block.hash();
                let len = raw.len as u64;
                feed_bytes.fetch_add(len, Ordering::Relaxed);
                if !feeder.feed_body(Height(height), hash, raw.block, len).await {
                    break; // sequencer gone
                }
                fed += 1;
            }
            Ok::<(), color_eyre::Report>(())
        });

        let tracing_on = trace_dir.is_some();
        if tracing_on {
            committer.emit_state_snapshot();
        }

        // The production driver owns verify+commit+report; the bench just feeds bodies
        // and waits for the verified tip to reach the gate checkpoint, emitting periodic
        // progress + `block_sync_state` snapshots from the sequencer view as it advances.
        let target_height = last_checkpoint;
        let wall_start = Instant::now();
        loop {
            let reached = tokio::time::timeout(
                Duration::from_secs(5),
                committer.wait_for_verified_tip(target_height),
            )
            .await
            .is_ok();
            let p = committer.progress();
            if tracing_on {
                committer.emit_state_snapshot();
            }
            tracing::info!(
                verified_tip = p.verified_tip.0,
                reorder = p.reorder_len,
                applying = p.applying_len,
                bps = p.committed_blocks_per_sec,
                "sequencer-progress (prod driver)"
            );
            if p.verified_tip >= target_height {
                break;
            }
            // `wait_*` returned without reaching the target => the sequencer task ended
            // (driver gone); or the feed drained with nothing left in flight. Either way
            // stop and let the gate below surface the failure.
            if reached || (feed.is_finished() && p.reorder_len == 0 && p.applying_len == 0) {
                break;
            }
        }
        let wall = wall_start.elapsed();

        // Gate via the sequencer view (the driver's `ApplyFinished` feedback), not a
        // read-state query: reaching the gate is itself the correctness proof. The
        // header-authenticated fast path refuses any block whose hash doesn't match the
        // hardcoded checkpoint, and a refused block never advances `verified_tip` — so
        // `verified_tip >= target` means every block up to it committed against the
        // authenticated checkpoint chain. When the tip lands exactly on the gate (the
        // common case), additionally assert the verified hash matches.
        let gate = committer.progress();
        if gate.verified_tip < target_height {
            let _ = shutdown_tx.send(());
            if let Ok(Err(e)) = feed.await {
                return Err(e);
            }
            bail!(
                "driver stalled at verified tip {} before reaching the gate checkpoint {} (a non-authenticated block would refuse to commit)",
                gate.verified_tip.0,
                target_height.0
            );
        }
        if gate.verified_tip == target_height && gate.verified_hash != expected_tip_hash {
            let _ = shutdown_tx.send(());
            bail!(
                "verified hash at the second-to-last checkpoint {} ({}) does not match the embedded checkpoint hash {}",
                target_height.0,
                gate.verified_hash,
                expected_tip_hash
            );
        }

        // Stop the driver and drain the feed.
        let _ = shutdown_tx.send(());
        let _ = driver.await;
        match feed.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => bail!("feed task panicked: {e}"),
        }

        committer.flush_trace().await;

        let committed = (target_height.0 - start + 1) as usize;
        let total_bytes = fed_bytes.load(Ordering::Relaxed);
        let mut stats = Stats::default();
        stats.record(total_bytes as usize, wall);

        tracing::info!(
            committed,
            last_checkpoint = last_checkpoint.0,
            "replay verified (sequencer, vct, prod driver): committed through the second-to-last checkpoint; hash matches"
        );
        println!("mode=sequencer-prod (vct, {storage_label})");
        if let Some(dir) = trace_dir.as_deref() {
            println!("zakura-traces={}", dir.display());
        }
        let secs = wall.as_secs_f64().max(f64::MIN_POSITIVE);
        println!(
            "throughput: {:.1} blk/s  {:.2} MiB/s",
            committed as f64 / secs,
            total_bytes as f64 / secs / (1024.0 * 1024.0)
        );
        Ok::<Stats, color_eyre::Report>(stats)
    })?;

    Ok(stats)
}
