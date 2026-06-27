//! Replay cached blocks through the real checkpoint verifier + real finalized
//! state at production apply concurrency, and report commit throughput.
//!
//! This is the stack *below* Zakura block-sync: no networking, no reactor — just
//! verify → commit → (optional) post-commit frontier read, driven at a bounded
//! in-flight window exactly like the sequencer's `submitted_apply` window.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::{Args, ValueEnum};
use color_eyre::eyre::{bail, eyre, Result, WrapErr};
use futures::{stream::FuturesUnordered, StreamExt};
use serde_json::{Map, Value};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tower::{buffer::Buffer, util::BoxService, ServiceExt};

use zebra_chain::{
    block, parallel::commitment_aux::BlockCommitmentRoots, parameters::Network,
    serialization::ZcashDeserializeInto,
};
use zebra_jsonl_trace::{JsonlTraceConfig, JsonlTracer, JsonlWriteEvent};
use zebra_network::zakura::{
    testkit::{SyntheticBlockSyncPeer, SyntheticBlockSyncPeers},
    BlockSyncFrontiers, BlockSyncMessage, BlockSyncStartup, BlockSyncStatus, ZakuraBlockSyncConfig,
    ZakuraPeerId, ZakuraSupervisorHandle, ZakuraTrace,
};

use crate::{
    fetch::block_path,
    mode::RunMode,
    range::{parse_network, plan_checkpoint_range},
    roots::parse_cached_roots,
    state_dir::{check_state_dir_not_used, exact_fetch_command, mark_used},
    stats::Stats,
};

/// Cadence of the off-hot-path frontier read in `coalesced` mode (matches the
/// sequencer's `CHECKPOINT_FRONTIER_REFRESH_INTERVAL`).
const COALESCED_REFRESH_INTERVAL: Duration = Duration::from_millis(200);

/// Where the post-commit frontier read happens relative to the apply slot.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum FrontierRead {
    /// Never read the durable frontier (models the optimistic-verified-tip ideal).
    None,
    /// Read off the hot path on a 200ms cadence (models the shipped coalesce).
    Coalesced,
    /// Read inside each apply, holding the slot (models the pre-coalesce baseline).
    PerBlock,
}

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Directory cached block bytes were written to by `fetch`.
    #[arg(long, default_value = "target/zakura-commit-bench/blocks")]
    pub cache_dir: PathBuf,

    /// Number of contiguous blocks to replay, starting at genesis (height 0).
    #[arg(long, default_value_t = 20_000)]
    pub blocks: u32,

    /// Replay mode.
    #[arg(long, value_enum, default_value_t = RunMode::DirectVerifier)]
    pub mode: RunMode,

    /// In-flight apply window. Production is MAX_CHECKPOINT_HEIGHT_GAP + 1 = 401;
    /// in `apply-queue`, this maps to the block-sync apply/body-input capacity.
    #[arg(long, default_value_t = 401)]
    pub concurrency: usize,

    /// Number of synthetic disk-backed stream-6 peers in `apply-queue` mode.
    #[arg(long, default_value_t = 1)]
    pub disk_peers: usize,

    /// Where the post-commit frontier read happens.
    #[arg(long, value_enum, default_value_t = FrontierRead::Coalesced)]
    pub frontier_read: FrontierRead,

    /// Optional directory for JSONL traces (emits block_sync.jsonl rollups).
    #[arg(long)]
    pub trace_dir: Option<PathBuf>,

    /// Network (mainnet only for now; the embedded checkpoint list is per-network).
    #[arg(long, default_value = "mainnet")]
    pub network: String,

    /// Feed cached per-height tree roots (from `fetch --with-roots`) into the VCT
    /// fast path, matching production's header-carried roots. Verify it took
    /// effect via the reported `state.vct.fast_path.hit/miss` counts.
    #[arg(long, default_value_t = false)]
    pub with_roots: bool,

    /// Hydrate from an existing Zebra state cache dir (from the `snapshot`
    /// command) instead of a fresh ephemeral DB. `--blocks` then commits that
    /// many heights *above* the snapshot's finalized tip — the way to benchmark a
    /// post-NU5 (sandblasting) range without replaying from genesis.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// Allow benchmarking against a state dir that metadata says was already
    /// mutated by a previous benchmark run.
    #[arg(long, default_value_t = false)]
    pub allow_used_state_dir: bool,
}

pub async fn run(args: RunArgs) -> Result<()> {
    let network = parse_network(&args.network)?;
    if let Some(state_dir) = &args.state_dir {
        check_state_dir_not_used(state_dir, args.allow_used_state_dir)?;
    }

    // Install the metrics recorder before the state registers its counters, so we
    // can read `state.vct.fast_path.hit/miss` at the end.
    let recorder = crate::metrics_rec::install();

    let (_checkpoint_list, max_checkpoint_height) =
        zebra_consensus::router::init_checkpoint_list(zebra_consensus::Config::default(), &network);

    // Build state: a fresh ephemeral DB (commit from genesis), or hydrate from a
    // snapshot dir (commit above its finalized tip — the only way to reach a
    // post-NU5 range without replaying ~1.7M blocks).
    let (state_config, hydrated) = match &args.state_dir {
        Some(dir) => (
            zebra_state::Config {
                cache_dir: dir.clone(),
                ephemeral: false,
                ..zebra_state::Config::default()
            },
            true,
        ),
        None => (zebra_state::Config::ephemeral(), false),
    };
    let (mut state_service, read_state, latest_chain_tip, chain_tip_change) = zebra_state::init(
        state_config,
        &network,
        max_checkpoint_height,
        // checkpoint-verify concurrency the state pipeline is sized for
        args.concurrency.max(1),
    )
    .await;

    // Anchor the commit range: resume above the snapshot's finalized tip, or at
    // genesis. The checkpoint verifier needs this as its initial tip.
    let initial_tip = if hydrated {
        match read_state
            .clone()
            .oneshot(zebra_state::ReadRequest::FinalizedTip)
            .await
        {
            Ok(zebra_state::ReadResponse::FinalizedTip(Some((height, hash)))) => {
                tracing::info!(tip = height.0, "hydrated from snapshot; resuming above tip");
                Some((height, hash))
            }
            Ok(zebra_state::ReadResponse::FinalizedTip(None)) => {
                bail!("--state-dir state has no finalized tip (empty snapshot?)")
            }
            other => bail!("unexpected FinalizedTip response: {other:?}"),
        }
    } else {
        None
    };

    // Cap the drive at the last reachable checkpoint above the anchor: the
    // verifier only commits a *complete* range to a checkpoint, so trailing
    // blocks past it would queue forever. In apply-queue + VCT fast-sync mode,
    // also load one successor checkpoint range so the finalized writer can
    // authenticate the last measured block's supplied tree roots.
    let plan = plan_checkpoint_range(
        &network,
        initial_tip.map(|(height, _)| height),
        args.blocks,
        args.mode,
        args.with_roots,
    )?;
    let first_height = plan.first_height;
    let wait_target = plan.measured_checkpoint;
    let load_target = plan.load_checkpoint;
    tracing::info!(
        first_height,
        requested_last = plan.requested_last,
        wait_target = wait_target.0,
        load_target = load_target.0,
        "commit range (capped to a checkpoint boundary)"
    );

    // Load contiguous blocks [first_height..=load_target] from the cache.
    let fetch_command = exact_fetch_command(
        &args.cache_dir,
        args.state_dir.as_deref(),
        args.blocks,
        args.mode,
        args.with_roots,
        &args.network,
    );
    let blocks = load_blocks(&args.cache_dir, first_height, load_target.0, &fetch_command)?;
    tracing::info!(
        count = blocks.len(),
        total_bytes = blocks.iter().map(|(_, _, b)| *b).sum::<u64>(),
        "loaded blocks from cache"
    );

    // Feed cached tree roots into the VCT fast path the same way production does:
    // commit the matching header range with one provisional root per header.
    let preloaded_roots = if args.with_roots {
        Some(load_roots(&args.cache_dir, &blocks, &fetch_command)?)
    } else {
        None
    };

    if let Some(state_dir) = &args.state_dir {
        mark_used(state_dir, args.mode, wait_target, load_target)?;
    }

    if args.with_roots {
        let Some((_, anchor)) = initial_tip else {
            bail!(
                "--with-roots requires --state-dir for now: header roots must be committed \
                 above an existing snapshot tip"
            );
        };
        preload_header_roots(
            &mut state_service,
            anchor,
            &blocks,
            preloaded_roots.expect("--with-roots loaded roots"),
        )
        .await?;
    }

    let state = Buffer::new(state_service, 64);

    if matches!(args.mode, RunMode::ApplyQueue) {
        return run_apply_queue(
            args,
            network,
            state,
            read_state,
            latest_chain_tip,
            chain_tip_change,
            initial_tip,
            blocks,
            wait_target,
            max_checkpoint_height,
            recorder,
        )
        .await;
    }

    let checkpoint_verifier =
        zebra_consensus::CheckpointVerifier::new(&network, initial_tip, state);
    let verifier = Buffer::new(
        BoxService::new(checkpoint_verifier),
        args.concurrency.saturating_mul(2).max(64),
    );

    // 3. Optional trace sink (real block_sync.jsonl rollups).
    let mut bench_trace = BenchTrace::new(args.trace_dir.as_deref(), args.mode)?;

    // Coalesced mode reads the durable frontier off the hot path on a 200ms cadence,
    // modeling the read *load* the shipped change keeps (without holding the slot).
    let coalesced_reader = if matches!(args.frontier_read, FrontierRead::Coalesced) {
        let read_state = read_state.clone();
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(COALESCED_REFRESH_INTERVAL);
            ticker.tick().await; // consume the immediate first tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => read_frontiers(read_state.clone()).await,
                    _ = &mut stop_rx => break,
                }
            }
        });
        Some((stop_tx, handle))
    } else {
        None
    };

    tracing::info!(
        blocks = blocks.len(),
        mode = args.mode.as_str(),
        concurrency = args.concurrency,
        frontier_read = ?args.frontier_read,
        ?max_checkpoint_height,
        "starting commit benchmark"
    );

    // 4. Drive the apply window.
    let mut stats = Stats::default();
    let mut rollup = Rollup::new();
    let started = Instant::now();
    let mut in_flight = FuturesUnordered::new();
    let mut next = 0usize;

    loop {
        while in_flight.len() < args.concurrency && next < blocks.len() {
            let (height, block, bytes) = blocks[next].clone();
            next += 1;
            let verifier = verifier.clone();
            let read_state = read_state.clone();
            let frontier_read = args.frontier_read;
            in_flight.push(async move {
                let submitted = Instant::now();
                let result = verifier.oneshot(block).await;
                let commit_latency = submitted.elapsed();
                let read_latency =
                    if matches!(frontier_read, FrontierRead::PerBlock) && result.is_ok() {
                        let read_started = Instant::now();
                        read_frontiers(read_state).await;
                        Some(read_started.elapsed())
                    } else {
                        None
                    };
                Completion {
                    height,
                    bytes,
                    ok: result.is_ok(),
                    error: result.err().map(|e| e.to_string()),
                    commit_latency,
                    read_latency,
                }
            });
        }

        let Some(completion) = in_flight.next().await else {
            break;
        };

        if completion.ok {
            stats.record_commit(
                completion.bytes,
                completion.commit_latency,
                completion.read_latency,
            );
            rollup.record(
                completion.height,
                completion.bytes,
                completion.commit_latency,
            );
            rollup.maybe_emit(&mut bench_trace);
        } else {
            stats.record_error();
            tracing::warn!(
                height = completion.height.0,
                error = completion.error.unwrap_or_default(),
                "block commit failed",
            );
        }
    }

    if let Some((stop_tx, handle)) = coalesced_reader {
        let _ = stop_tx.send(());
        let _ = handle.await;
    }

    rollup.emit(&mut bench_trace); // flush the tail window
    let summary = stats.summary(started.elapsed());
    summary.print();
    print_tip_targets(read_state.clone(), wait_target, load_target).await;
    report_fast_path(&recorder, args.with_roots);
    bench_trace.shutdown().await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_apply_queue<StateService>(
    args: RunArgs,
    network: Network,
    state_service: StateService,
    read_state: zebra_state::ReadStateService,
    latest_chain_tip: zebra_state::LatestChainTip,
    chain_tip_change: zebra_state::ChainTipChange,
    initial_tip: Option<(block::Height, block::Hash)>,
    blocks: Vec<(block::Height, Arc<block::Block>, u64)>,
    wait_target_height: block::Height,
    max_checkpoint_height: block::Height,
    recorder: crate::metrics_rec::BenchRecorder,
) -> Result<()>
where
    StateService: tower::Service<
            zebra_state::Request,
            Response = zebra_state::Response,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + 'static,
    StateService::Future: Send + 'static,
{
    if !args.with_roots {
        bail!("--mode apply-queue requires --with-roots so headers and roots are preloaded");
    }
    let Some((anchor_height, anchor_hash)) = initial_tip else {
        bail!("--mode apply-queue requires --state-dir with a finalized snapshot tip");
    };
    if args.disk_peers == 0 {
        bail!("--disk-peers must be at least 1");
    }

    let (target_height, target_hash) = blocks
        .last()
        .map(|(height, block, _)| (*height, block.hash()))
        .ok_or_else(|| eyre!("apply-queue mode needs at least one cached block"))?;
    let first_height = blocks
        .first()
        .map(|(height, _, _)| *height)
        .expect("non-empty block list has first height");
    if first_height != anchor_height.next().unwrap_or(anchor_height) {
        bail!(
            "--mode apply-queue cache must start at snapshot_tip + 1: snapshot tip {}, cache starts {}",
            anchor_height.0,
            first_height.0
        );
    }

    let measured_blocks = blocks
        .iter()
        .take_while(|(height, _, _)| *height <= wait_target_height)
        .count();
    let measured_bytes = blocks
        .iter()
        .take_while(|(height, _, _)| *height <= wait_target_height)
        .map(|(_, _, bytes)| *bytes)
        .sum::<u64>();
    let bench_trace = BenchTrace::new(args.trace_dir.as_deref(), args.mode)?;
    let trace = bench_trace.zakura_trace();

    let (block_verifier, _tx_verifier, consensus_tasks, _router_max_checkpoint_height) =
        zebra_consensus::router::init_test(
            zebra_consensus::Config::default(),
            &network,
            state_service.clone(),
        )
        .await;

    let shutdown = CancellationToken::new();
    let (_header_tip_tx, header_tip_rx) =
        watch::channel::<(block::Height, block::Hash)>((target_height, target_hash));
    let frontiers = BlockSyncFrontiers {
        finalized_height: anchor_height,
        verified_block_tip: anchor_height,
        verified_block_hash: anchor_hash,
    };
    let mut block_sync_config = ZakuraBlockSyncConfig {
        max_submitted_block_applies: args.concurrency.max(1),
        max_inflight_requests: u32::try_from(args.concurrency).unwrap_or(u32::MAX).max(1),
        initial_inflight_requests: u32::try_from(args.concurrency)
            .unwrap_or(u32::MAX)
            .clamp(1, 64),
        status_refresh_interval: Duration::from_millis(200),
        peer_limits: zebra_network::zakura::ServicePeerLimits {
            max_inbound_peers: args.disk_peers.saturating_add(1),
            max_outbound_peers: args.disk_peers.saturating_add(1),
            inbound_queue_depth: args.concurrency.saturating_mul(4).max(128),
            outbound_queue_depth: args.concurrency.saturating_mul(4).max(128),
            ..zebra_network::zakura::ServicePeerLimits::default()
        },
        ..ZakuraBlockSyncConfig::default()
    };
    block_sync_config.initial_inflight_requests = block_sync_config
        .initial_inflight_requests
        .min(block_sync_config.max_inflight_requests)
        .max(1);

    let mut startup = BlockSyncStartup::new(
        frontiers,
        (target_height, target_hash),
        header_tip_rx,
        block_sync_config.clone(),
    );
    startup.trace = trace.clone();
    startup.shutdown = shutdown.clone();
    let (block_sync, block_actions, reactor_task) =
        zebra_network::zakura::spawn_block_sync_reactor(startup);

    let driver_task = tokio::spawn(zebrad::commands::start::zakura::drive_block_sync_actions(
        block_actions,
        ZakuraSupervisorHandle::new(args.disk_peers.saturating_add(1)),
        block_sync.clone(),
        read_state.clone(),
        trace.clone(),
        shutdown.clone().cancelled_owned(),
    ));

    let durable_task = tokio::spawn(
        zebrad::commands::start::zakura::drive_block_sync_durable_frontier(
            chain_tip_change,
            latest_chain_tip,
            read_state.clone(),
            block_sync.clone(),
            shutdown.clone().cancelled_owned(),
        ),
    );

    let apply_rx = block_sync
        .take_apply_queue()
        .ok_or_else(|| eyre!("block-sync applyQ was already taken"))?;
    let committer = zebrad::commands::start::zakura::committer::Committer::new_without_probe(
        apply_rx,
        block_verifier,
        Arc::new(block_sync.clone()),
        max_checkpoint_height,
        trace.clone(),
    );
    let committer_task = tokio::spawn(committer.run(shutdown.clone().cancelled_owned()));

    let peers = SyntheticBlockSyncPeers::new(
        block_sync_config.clone(),
        block_sync.clone(),
        args.concurrency.saturating_mul(4).max(128),
    );
    let mut peer_tasks = Vec::with_capacity(args.disk_peers);
    for index in 0..args.disk_peers {
        let byte = u8::try_from((index % 250) + 1).expect("bounded peer byte fits u8");
        let peer_id =
            ZakuraPeerId::new(vec![byte; 32]).expect("synthetic peer id is within bounds");
        let status = BlockSyncStatus {
            servable_low: first_height,
            servable_high: target_height,
            tip_hash: target_hash,
            max_blocks_per_response: block_sync_config.advertised_max_blocks_per_response(),
            max_inflight_requests: block_sync_config.advertised_max_inflight_requests(),
            max_response_bytes: block_sync_config.advertised_max_response_bytes(),
        };
        let peer = peers
            .add_peer(peer_id, status)
            .await
            .map_err(|error| eyre!("failed to add synthetic disk peer: {error}"))?;
        let cache_dir = args.cache_dir.clone();
        peer_tasks.push(tokio::spawn(run_disk_peer(
            peer,
            cache_dir,
            first_height,
            target_height,
            shutdown.clone(),
        )));
    }

    tracing::info!(
        first = first_height.0,
        target = target_height.0,
        wait_target = wait_target_height.0,
        disk_peers = args.disk_peers,
        concurrency = args.concurrency,
        "starting apply-queue commit benchmark"
    );
    let started = Instant::now();
    wait_for_state_tip(read_state.clone(), wait_target_height, shutdown.clone()).await?;
    let elapsed = started.elapsed();

    shutdown.cancel();
    for peer_task in peer_tasks {
        let _ = peer_task.await;
    }
    committer_task.abort();
    let _ = committer_task.await;
    driver_task.abort();
    durable_task.abort();
    reactor_task.abort();
    consensus_tasks.state_checkpoint_verify_handle.abort();
    let final_tip_height = match read_state
        .clone()
        .oneshot(zebra_state::ReadRequest::Tip)
        .await
    {
        Ok(zebra_state::ReadResponse::Tip(Some((height, _)))) => Some(height),
        _ => None,
    };

    println!("\n=== zakura-commit-bench summary ===");
    println!("mode:               {}", args.mode.as_str());
    println!("elapsed:            {:.3}s", elapsed.as_secs_f64());
    println!("committed blocks:   {}", measured_blocks);
    println!(
        "committed bytes:    {} ({:.1} MiB)",
        measured_bytes,
        measured_bytes as f64 / (1024.0 * 1024.0)
    );
    let secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "throughput:         {:.1} blk/s   {:.2} MiB/s",
        measured_blocks as f64 / secs,
        (measured_bytes as f64 / (1024.0 * 1024.0)) / secs
    );
    println!("measured tip:       {}", wait_target_height.0);
    if let Some(final_tip_height) = final_tip_height {
        println!("state tip:          {}", final_tip_height.0);
    }
    if target_height > wait_target_height {
        println!("lookahead target:   {}", target_height.0);
    }
    report_fast_path(&recorder, args.with_roots);
    bench_trace.shutdown().await;
    drop(peers);
    Ok(())
}

async fn run_disk_peer(
    mut peer: SyntheticBlockSyncPeer,
    cache_dir: PathBuf,
    first_height: block::Height,
    target_height: block::Height,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                peer.cancel();
                return;
            }
            message = peer.recv() => {
                let Ok(Some(message)) = message else {
                    return;
                };
                match message {
                    BlockSyncMessage::Status(_) => {}
                    BlockSyncMessage::GetBlocks { start_height, count } => {
                        if start_height < first_height || start_height > target_height {
                            let _ = peer
                                .send(BlockSyncMessage::RangeUnavailable { start_height, count })
                                .await;
                            continue;
                        }
                        let mut returned = 0u32;
                        for height in start_height.0..=target_height.0 {
                            if returned >= count {
                                break;
                            }
                            match load_cached_block(&cache_dir, block::Height(height)) {
                                Ok(block) => {
                                    if peer.send(BlockSyncMessage::Block(block)).await.is_err() {
                                        return;
                                    }
                                    returned = returned.saturating_add(1);
                                }
                                Err(error) => {
                                    tracing::warn!(height, ?error, "disk peer could not load cached block");
                                    break;
                                }
                            }
                        }
                        let _ = peer
                            .send(BlockSyncMessage::BlocksDone { start_height, returned })
                            .await;
                    }
                    BlockSyncMessage::Block(_)
                    | BlockSyncMessage::BlocksDone { .. }
                    | BlockSyncMessage::RangeUnavailable { .. } => {}
                }
            }
        }
    }
}

fn load_cached_block(
    cache_dir: &std::path::Path,
    height: block::Height,
) -> Result<Arc<block::Block>> {
    let path = block_path(cache_dir, height.0);
    let bytes = std::fs::read(&path)
        .wrap_err_with(|| format!("missing cached block {} at {}", height.0, path.display()))?;
    let block: block::Block = bytes
        .zcash_deserialize_into()
        .wrap_err_with(|| format!("cached block {} failed to deserialize", height.0))?;
    Ok(Arc::new(block))
}

async fn wait_for_state_tip(
    read_state: zebra_state::ReadStateService,
    target_height: block::Height,
    shutdown: CancellationToken,
) -> Result<()> {
    let deadline = Duration::from_secs(60 * 60);
    let started = Instant::now();
    loop {
        match read_state
            .clone()
            .oneshot(zebra_state::ReadRequest::Tip)
            .await
        {
            Ok(zebra_state::ReadResponse::Tip(Some((height, _)))) if height >= target_height => {
                return Ok(());
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(?error, "failed to read apply-queue benchmark state tip"),
        }
        if started.elapsed() > deadline {
            bail!(
                "timed out waiting for apply-queue state tip to reach {}",
                target_height.0
            );
        }
        tokio::select! {
            () = shutdown.cancelled() => bail!("apply-queue benchmark shut down before reaching target"),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
}

struct Completion {
    height: block::Height,
    bytes: u64,
    ok: bool,
    error: Option<String>,
    commit_latency: std::time::Duration,
    read_latency: Option<std::time::Duration>,
}

/// Two durable reads, mirroring `query_block_sync_frontiers` (FinalizedTip + Tip).
async fn read_frontiers(read_state: zebra_state::ReadStateService) {
    let _ = read_state
        .clone()
        .oneshot(zebra_state::ReadRequest::FinalizedTip)
        .await;
    let _ = read_state.oneshot(zebra_state::ReadRequest::Tip).await;
}

fn load_blocks(
    cache_dir: &std::path::Path,
    lo: u32,
    hi: u32,
    fetch_command: &str,
) -> Result<Vec<(block::Height, Arc<block::Block>, u64)>> {
    let mut blocks = Vec::with_capacity((hi.saturating_sub(lo) + 1) as usize);
    for height in lo..=hi {
        let path = block_path(cache_dir, height);
        let bytes = std::fs::read(&path).wrap_err_with(|| {
            format!(
                "missing cached block {height} at {}\nfetch the required range with:\n  {fetch_command}",
                path.display(),
            )
        })?;
        let len = bytes.len() as u64;
        let block: block::Block = bytes
            .zcash_deserialize_into()
            .wrap_err_with(|| format!("cached block {height} failed to deserialize"))?;
        blocks.push((block::Height(height), Arc::new(block), len));
    }
    Ok(blocks)
}

async fn preload_header_roots<S>(
    state: &mut S,
    anchor: block::Hash,
    blocks: &[(block::Height, Arc<block::Block>, u64)],
    roots: Vec<BlockCommitmentRoots>,
) -> Result<()>
where
    S: tower::Service<zebra_state::Request, Response = zebra_state::Response> + Send + 'static,
    S::Error: Into<tower::BoxError>,
    S::Future: Send,
{
    if roots.len() != blocks.len() {
        bail!(
            "--with-roots needs one cached root file per replayed block: loaded {} roots for {} blocks",
            roots.len(),
            blocks.len()
        );
    }

    let headers: Vec<_> = blocks
        .iter()
        .map(|(_, block, _)| block.header.clone())
        .collect();
    let mut body_sizes = Vec::with_capacity(blocks.len());
    for (height, _, bytes) in blocks {
        body_sizes.push(u32::try_from(*bytes).wrap_err_with(|| {
            format!("cached block {} is too large for body size hint", height.0)
        })?);
    }

    let start = blocks
        .first()
        .map(|(height, _, _)| *height)
        .ok_or_else(|| eyre!("cannot preload roots for an empty block range"))?;
    let end = blocks
        .last()
        .map(|(height, _, _)| *height)
        .expect("non-empty block range has a last block");

    match state
        .ready()
        .await
        .map_err(|error| {
            let error: tower::BoxError = error.into();
            eyre!("state service not ready for header root preload: {error}")
        })?
        .call(zebra_state::Request::CommitHeaderRange {
            anchor,
            headers,
            body_sizes,
            tree_aux_roots: roots,
        })
        .await
    {
        Ok(zebra_state::Response::Committed(tip_hash)) => {
            tracing::info!(
                start = start.0,
                end = end.0,
                %tip_hash,
                "preloaded header roots through CommitHeaderRange"
            );
            Ok(())
        }
        Ok(response) => bail!("unexpected CommitHeaderRange response: {response:?}"),
        Err(error) => bail!("CommitHeaderRange root preload failed: {}", error.into()),
    }
}

/// Load cached `z_gettreestate` roots and rebuild `BlockCommitmentRoots`. Heights
/// with no cached sapling root (pre-Sapling, or not fetched) are skipped — the
/// fast path doesn't apply there.
fn load_roots(
    cache_dir: &std::path::Path,
    blocks: &[(block::Height, Arc<block::Block>, u64)],
    fetch_command: &str,
) -> Result<Vec<BlockCommitmentRoots>> {
    let mut roots = Vec::new();
    for (height, _, _) in blocks {
        roots.push(parse_cached_roots(cache_dir, *height).wrap_err_with(|| {
            format!("fetch the required block roots with:\n  {fetch_command}")
        })?);
    }
    Ok(roots)
}

async fn print_tip_targets(
    read_state: zebra_state::ReadStateService,
    measured_target: block::Height,
    lookahead_target: block::Height,
) {
    println!("measured tip:       {}", measured_target.0);
    if let Ok(zebra_state::ReadResponse::Tip(Some((height, _)))) =
        read_state.oneshot(zebra_state::ReadRequest::Tip).await
    {
        println!("state tip:          {}", height.0);
    }
    if lookahead_target > measured_target {
        println!("lookahead target:   {}", lookahead_target.0);
    }
}

/// Report whether the fed roots engaged the VCT fast path — the verification that
/// our root reconstruction is correct.
fn report_fast_path(recorder: &crate::metrics_rec::BenchRecorder, with_roots: bool) {
    let hit = recorder.counter("state.vct.fast_path.hit");
    let miss = recorder.counter("state.vct.fast_path.miss");
    let retry = recorder.counter("state.vct.root.retry.count");
    if !with_roots && hit == 0 && miss == 0 {
        return;
    }
    let total = hit + miss;
    let pct = if total > 0 {
        100.0 * hit as f64 / total as f64
    } else {
        0.0
    };
    println!("VCT fast path:      {hit} hit / {miss} miss ({pct:.1}% hit), {retry} retries");
    if with_roots && hit == 0 {
        println!(
            "  WARNING: 0 fast-path hits with --with-roots — roots may be wrong (byte order/range)."
        );
    }
}

// ---- trace rollup (mirrors the sequencer's block_commit_progress schema) ----

const ROLLUP_BLOCK_INTERVAL: u64 = 256;

struct Rollup {
    window_start: Instant,
    blocks: u64,
    bytes: u64,
    latency_sum_us: u128,
    verified_tip: block::Height,
}

impl Rollup {
    fn new() -> Self {
        Self {
            window_start: Instant::now(),
            blocks: 0,
            bytes: 0,
            latency_sum_us: 0,
            verified_tip: block::Height(0),
        }
    }

    fn record(&mut self, height: block::Height, bytes: u64, latency: std::time::Duration) {
        self.blocks += 1;
        self.bytes = self.bytes.saturating_add(bytes);
        self.latency_sum_us = self.latency_sum_us.saturating_add(latency.as_micros());
        self.verified_tip = self.verified_tip.max(height);
    }

    fn maybe_emit(&mut self, trace: &mut BenchTrace) {
        if self.blocks >= ROLLUP_BLOCK_INTERVAL {
            self.emit(trace);
        }
    }

    fn emit(&mut self, trace: &mut BenchTrace) {
        if self.blocks == 0 {
            return;
        }
        let interval_ms = self.window_start.elapsed().as_millis() as u64;
        let blocks_per_sec = if interval_ms > 0 {
            self.blocks.saturating_mul(1000) / interval_ms
        } else {
            0
        };
        let latency_avg_us = (self.latency_sum_us / u128::from(self.blocks)) as u64;
        trace.emit_block_commit_progress(
            self.verified_tip,
            self.blocks,
            self.bytes,
            interval_ms,
            blocks_per_sec,
            latency_avg_us,
        );
        self.window_start = Instant::now();
        self.blocks = 0;
        self.bytes = 0;
        self.latency_sum_us = 0;
    }
}

struct BenchTrace {
    trace: Option<JsonlTracer>,
    guard: Option<zebra_jsonl_trace::JsonlTraceGuard>,
    started: Instant,
    mode: RunMode,
}

impl BenchTrace {
    fn new(trace_dir: Option<&std::path::Path>, mode: RunMode) -> Result<Self> {
        let Some(dir) = trace_dir else {
            return Ok(Self {
                trace: None,
                guard: None,
                started: Instant::now(),
                mode,
            });
        };
        std::fs::create_dir_all(dir)
            .wrap_err_with(|| format!("creating trace dir {}", dir.display()))?;
        let guard =
            JsonlTracer::spawn_guard_with_config(dir.to_path_buf(), JsonlTraceConfig::default());
        let trace = guard.tracer().clone();
        Ok(Self {
            trace: Some(trace),
            guard: Some(guard),
            started: Instant::now(),
            mode,
        })
    }

    fn emit_block_commit_progress(
        &self,
        verified_tip: block::Height,
        blocks: u64,
        bytes: u64,
        interval_ms: u64,
        blocks_per_sec: u64,
        latency_avg_us: u64,
    ) {
        let Some(trace) = &self.trace else { return };
        let Ok(permit) = trace.try_reserve() else {
            return;
        };

        let mut row = Map::new();
        row.insert("ts".into(), elapsed_micros(self.started.elapsed()));
        row.insert("node".into(), "commit-bench".into());
        row.insert("event".into(), "block_commit_progress".into());
        row.insert("mode".into(), self.mode.as_str().into());
        row.insert("verified_block_tip".into(), verified_tip.0.into());
        row.insert("committed_blocks".into(), blocks.into());
        row.insert("committed_bytes".into(), bytes.into());
        row.insert("interval_ms".into(), interval_ms.into());
        row.insert("committed_blocks_per_sec".into(), blocks_per_sec.into());
        row.insert("apply_latency_avg_us".into(), latency_avg_us.into());

        if let Ok(line) = serde_json::to_vec(&Value::Object(row)) {
            permit.send(JsonlWriteEvent {
                table: "block_sync",
                file_name: "block_sync.jsonl",
                line,
            });
        }
    }

    fn zakura_trace(&self) -> ZakuraTrace {
        self.trace
            .as_ref()
            .map(|trace| ZakuraTrace::new(trace.clone(), "commit-bench"))
            .unwrap_or_else(ZakuraTrace::noop)
    }

    async fn shutdown(self) {
        if let Some(guard) = self.guard {
            guard.shutdown().await;
        }
    }
}

fn elapsed_micros(duration: Duration) -> Value {
    u64::try_from(duration.as_micros())
        .unwrap_or(u64::MAX)
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The harness's central operation — committing real contiguous mainnet
    /// blocks through the real `CheckpointVerifier` into real finalized state —
    /// works offline against the bundled 0..=10 block vectors. Uses a custom
    /// checkpoint at height 10 so the small range completes one checkpoint batch.
    #[tokio::test]
    async fn drives_real_blocks_through_real_checkpoint_verifier() {
        let network = Network::Mainnet;
        let chain: Vec<Arc<block::Block>> = (0..=10u32)
            .map(|height| {
                let bytes: &[u8] = zebra_test::vectors::CONTINUOUS_MAINNET_BLOCKS
                    .get(&height)
                    .copied()
                    .expect("a contiguous mainnet block vector exists for 0..=10");
                Arc::new(bytes.zcash_deserialize_into().expect("block vector parses"))
            })
            .collect();
        let genesis_hash = chain[0].hash();
        let checkpoint_hash = chain[10].hash();

        let state_config = zebra_state::Config::ephemeral();
        let (state_service, read_state, _tip, _change) =
            zebra_state::init(state_config, &network, block::Height(10), 401).await;
        let state = Buffer::new(state_service, 16);

        let checkpoint_verifier = zebra_consensus::CheckpointVerifier::from_list(
            [
                (block::Height(0), genesis_hash),
                (block::Height(10), checkpoint_hash),
            ],
            &network,
            None,
            state,
        )
        .expect("a checkpoint list with genesis and one mid-chain checkpoint is valid");
        let verifier = Buffer::new(BoxService::new(checkpoint_verifier), 32);

        let mut in_flight = FuturesUnordered::new();
        for block in &chain {
            let verifier = verifier.clone();
            let block = block.clone();
            in_flight.push(async move { verifier.oneshot(block).await });
        }
        let mut committed = 0;
        while let Some(result) = in_flight.next().await {
            result.expect("real block commits through the checkpoint verifier");
            committed += 1;
        }
        assert_eq!(committed, 11, "all bundled blocks commit");

        match read_state
            .oneshot(zebra_state::ReadRequest::FinalizedTip)
            .await
            .expect("finalized tip read succeeds")
        {
            zebra_state::ReadResponse::FinalizedTip(Some((height, _))) => {
                assert_eq!(
                    height,
                    block::Height(10),
                    "finalized tip advanced to the checkpoint"
                );
            }
            other => panic!("unexpected FinalizedTip response: {other:?}"),
        }
    }
}
