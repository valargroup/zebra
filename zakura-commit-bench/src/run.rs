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
use tower::{buffer::Buffer, util::BoxService, ServiceExt};

use zebra_chain::{
    block, orchard, parallel::commitment_aux::BlockCommitmentRoots, parameters::Network, sapling,
    serialization::ZcashDeserializeInto,
};
use zebra_jsonl_trace::{JsonlTraceConfig, JsonlTracer};
use zebra_network::zakura::{ZakuraTrace, BLOCK_SYNC_TABLE};

use crate::{
    fetch::{block_path, roots_path, CachedRoots},
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

    /// In-flight apply window. Production is MAX_CHECKPOINT_HEIGHT_GAP + 1 = 401;
    /// must be >= the checkpoint spacing or the verifier can't complete a range.
    #[arg(long, default_value_t = 401)]
    pub concurrency: usize,

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
}

pub async fn run(args: RunArgs) -> Result<()> {
    let network = match args.network.to_ascii_lowercase().as_str() {
        "mainnet" => Network::Mainnet,
        other => bail!("only --network mainnet is supported for now (got {other:?})"),
    };

    // Install the metrics recorder before the state registers its counters, so we
    // can read `state.vct.fast_path.hit/miss` at the end.
    let recorder = crate::metrics_rec::install();

    let (checkpoint_list, max_checkpoint_height) =
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
    let (state_service, read_state, _latest_tip, _tip_change) = zebra_state::init(
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
    // blocks past it would queue forever. Mainnet checkpoints are every 400.
    let first_height = initial_tip.map_or(0, |(h, _)| h.0.saturating_add(1));
    let requested_last = block::Height(first_height.saturating_add(args.blocks).saturating_sub(1));
    let last_committable = checkpoint_list
        .max_height_in_range(block::Height(first_height)..=requested_last)
        .ok_or_else(|| {
            eyre!(
                "blocks {first_height}..={} reach no checkpoint above the anchor; increase --blocks",
                requested_last.0
            )
        })?;
    tracing::info!(
        first_height,
        last = last_committable.0,
        "commit range (capped to a checkpoint boundary)"
    );

    // Load contiguous blocks [first_height..=last_committable] from the cache.
    let blocks = load_blocks(&args.cache_dir, first_height, last_committable.0)?;
    tracing::info!(
        count = blocks.len(),
        total_bytes = blocks.iter().map(|(_, _, b)| *b).sum::<u64>(),
        "loaded blocks from cache"
    );

    // Feed cached tree roots into the VCT fast path (production gets these from
    // header-carried roots over the wire; here from `fetch --with-roots`).
    if args.with_roots {
        // The old `tree_aux` peer-source writer that fed roots straight into the
        // committer cache has been removed. Roots now reach the committer only via the
        // header-sync `CommitHeaderRange` path, persisted to the
        // `zakura_header_commitment_roots_by_height` column family. Re-port `--with-roots`
        // onto that path to benchmark the VCT fast path again.
        let _ = load_roots; // keep the loader wired for the re-port
        bail!(
            "--with-roots is not supported after the tree_aux stream removal; roots now \
             arrive via header sync (CommitHeaderRange) — re-port the bench onto that path"
        );
    }

    let state = Buffer::new(state_service, 64);
    let checkpoint_verifier =
        zebra_consensus::CheckpointVerifier::new(&network, initial_tip, state);
    let verifier = Buffer::new(
        BoxService::new(checkpoint_verifier),
        args.concurrency.saturating_mul(2).max(64),
    );

    // 3. Optional trace sink (real block_sync.jsonl rollups).
    let mut bench_trace = BenchTrace::new(args.trace_dir.as_deref())?;

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
    report_fast_path(&recorder, args.with_roots);
    bench_trace.shutdown().await;
    Ok(())
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
) -> Result<Vec<(block::Height, Arc<block::Block>, u64)>> {
    let mut blocks = Vec::with_capacity((hi.saturating_sub(lo) + 1) as usize);
    for height in lo..=hi {
        let path = block_path(cache_dir, height);
        let bytes = std::fs::read(&path).wrap_err_with(|| {
            format!(
                "missing cached block {height} at {} — run `fetch` first",
                path.display()
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

/// Load cached `z_gettreestate` roots and rebuild `BlockCommitmentRoots`. Heights
/// with no cached sapling root (pre-Sapling, or not fetched) are skipped — the
/// fast path doesn't apply there.
fn load_roots(
    cache_dir: &std::path::Path,
    blocks: &[(block::Height, Arc<block::Block>, u64)],
) -> Result<Vec<BlockCommitmentRoots>> {
    let mut roots = Vec::new();
    for (height, _, _) in blocks {
        let path = roots_path(cache_dir, height.0);
        let Ok(raw) = std::fs::read(&path) else {
            continue;
        };
        let cached: CachedRoots = serde_json::from_slice(&raw)
            .wrap_err_with(|| format!("parsing cached roots {}", path.display()))?;
        let Some(sapling_hex) = cached.sapling else {
            continue;
        };
        let sapling_root = parse_sapling_root(&sapling_hex)
            .wrap_err_with(|| format!("sapling root {}", height.0))?;
        let orchard_root = match cached.orchard {
            Some(hex) => {
                parse_orchard_root(&hex).wrap_err_with(|| format!("orchard root {}", height.0))?
            }
            None => orchard::tree::NoteCommitmentTree::default().root(),
        };
        roots.push(BlockCommitmentRoots {
            height: *height,
            sapling_root,
            orchard_root,
        });
    }
    Ok(roots)
}

fn decode_root_bytes(hex_str: &str) -> Result<[u8; 32]> {
    let raw = hex::decode(hex_str.trim()).wrap_err("root hex decode")?;
    <[u8; 32]>::try_from(raw.as_slice()).map_err(|_| eyre!("root hex was not 32 bytes"))
}

// z_gettreestate returns the root in display order (`root.reverse()` of the
// internal bytes), so reverse it back before reconstructing the internal `Root`.
fn parse_sapling_root(hex_str: &str) -> Result<sapling::tree::Root> {
    let mut bytes = decode_root_bytes(hex_str)?;
    bytes.reverse();
    sapling::tree::Root::try_from(bytes).map_err(|e| eyre!("invalid sapling root: {e:?}"))
}

fn parse_orchard_root(hex_str: &str) -> Result<orchard::tree::Root> {
    let mut bytes = decode_root_bytes(hex_str)?;
    bytes.reverse();
    orchard::tree::Root::try_from(bytes).map_err(|e| eyre!("invalid orchard root: {e:?}"))
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
    trace: Option<ZakuraTrace>,
    guard: Option<zebra_jsonl_trace::JsonlTraceGuard>,
}

impl BenchTrace {
    fn new(trace_dir: Option<&std::path::Path>) -> Result<Self> {
        let Some(dir) = trace_dir else {
            return Ok(Self {
                trace: None,
                guard: None,
            });
        };
        std::fs::create_dir_all(dir)
            .wrap_err_with(|| format!("creating trace dir {}", dir.display()))?;
        let guard =
            JsonlTracer::spawn_guard_with_config(dir.to_path_buf(), JsonlTraceConfig::default());
        let trace = ZakuraTrace::new(guard.tracer(), "commit-bench");
        Ok(Self {
            trace: Some(trace),
            guard: Some(guard),
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
        trace.emit_with(BLOCK_SYNC_TABLE, |row| {
            row.insert("event".into(), "block_commit_progress".into());
            row.insert("verified_block_tip".into(), verified_tip.0.into());
            row.insert("committed_blocks".into(), blocks.into());
            row.insert("committed_bytes".into(), bytes.into());
            row.insert("interval_ms".into(), interval_ms.into());
            row.insert("committed_blocks_per_sec".into(), blocks_per_sec.into());
            row.insert("apply_latency_avg_us".into(), latency_avg_us.into());
        });
    }

    async fn shutdown(self) {
        if let Some(guard) = self.guard {
            guard.shutdown().await;
        }
    }
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
