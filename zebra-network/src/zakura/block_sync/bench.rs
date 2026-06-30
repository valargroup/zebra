//! Offline benchmark helper for the block-sync [`Sequencer`](super::sequencer::Sequencer).
//!
//! Spawns the **real** [`SequencerTask`](super::sequencer_task::SequencerTask) — the
//! body reorder + ordered-submit pipeline — with no peers and no reactor, and exposes
//! a minimal handle (split into parts via [`BenchSequencerHandle::into_parts`]) to
//! drive it from an offline benchmark:
//!
//! * [`BenchBodyFeeder::feed_body`] feeds a downloaded body into the reorder queue
//!   (the task's real bounded body input);
//! * [`BenchSubmissions::next_submit`] drains the ordered `SubmitBlock` actions the
//!   sequencer emits once a body is contiguous above the verified tip;
//! * [`BenchCommitter::apply_committed`] reports a commit back, which advances the
//!   sequencer frontier and releases the next contiguous blocks.
//!
//! The caller (e.g. `zebra-replay-bench`'s `apply-sequencer`) supplies the
//! verify+commit between `next_submit` and `apply_committed`, using the same real
//! checkpoint verifier + state service as the `apply-verifier` rung. This is
//! feature-gated (`internal-bench`) and is not part of the production API.

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use zebra_chain::block::{self, Block};
use zebra_jsonl_trace::{JsonlTraceGuard, JsonlTracer};

use crate::zakura::{
    trace::{block_sync_trace as bs_trace, BLOCK_SYNC_TABLE},
    transport::ByteBudget,
    ServicePeerSnapshot, ZakuraBlockSyncCandidateState, ZakuraPeerId, ZakuraTrace,
};

use super::{
    config::ZakuraBlockSyncConfig,
    events::{BlockApplyResult, BlockApplyToken, BlockSyncAction, BlockSyncEvent},
    reactor::{bs_insert_height, bs_insert_u64},
    reorder::BufferedBlockBody,
    sequencer::Sequencer,
    sequencer_task::{
        initial_view, SequencedBody, SequencerControlInput, SequencerTask, SequencerView,
    },
    state::{BlockSyncFrontiers, BlockSyncHandle, ThroughputMeter},
    work_queue::WorkQueue,
};

/// How long the sequencer task waits to send an action before giving up. Generous
/// for the bench (the action channel is drained by the caller's commit loop).
const BENCH_ACTION_SEND_TIMEOUT: Duration = Duration::from_secs(60);

/// A block the sequencer ordered and asked the caller to commit. The `token` must
/// be echoed back via [`BenchCommitter::apply_committed`] after committing.
#[derive(Clone, Debug)]
pub struct BenchSubmit {
    /// Submission token to echo back on commit.
    pub token: BlockApplyToken,
    /// The block to verify and commit (contiguous above the verified tip).
    pub block: Arc<Block>,
}

/// A read-only progress snapshot copied from the internal sequencer view.
#[derive(Copy, Clone, Debug)]
pub struct SequencerProgress {
    /// Verified block tip (last committed height the sequencer knows about).
    pub verified_tip: block::Height,
    /// Hash of the verified block tip (the committed hash reported for `verified_tip`).
    pub verified_hash: block::Hash,
    /// Bodies buffered out-of-order in the reorder queue.
    pub reorder_len: u64,
    /// Bodies drained into the contiguous `applying` set.
    pub applying_len: u64,
    /// The sequencer's own committed-throughput estimate.
    pub committed_blocks_per_sec: u64,
}

/// Drives the real block-sync `SequencerTask` for an offline benchmark. Split into
/// independent parts via [`into_parts`](Self::into_parts) so the feed, submission
/// drain, and commit feedback can run concurrently without borrow conflicts.
pub struct BenchSequencerHandle {
    feeder: BenchBodyFeeder,
    submissions: BenchSubmissions,
    committer: BenchCommitter,
}

/// A cloneable handle for feeding bodies into the sequencer's reorder queue.
#[derive(Clone)]
pub struct BenchBodyFeeder {
    body_input: mpsc::Sender<SequencedBody>,
    body_input_bytes: Arc<AtomicU64>,
    bench_peer: ZakuraPeerId,
}

/// Drains the ordered `SubmitBlock`s the sequencer emits (the `&mut` side).
pub struct BenchSubmissions {
    actions: mpsc::Receiver<BlockSyncAction>,
}

/// Reports commit completions back to the sequencer and reads its progress view.
pub struct BenchCommitter {
    control: mpsc::UnboundedSender<SequencerControlInput>,
    view: watch::Receiver<SequencerView>,
    // A clone of the sequencer's trace emitter, so the bench driver can write the
    // periodic `block_sync_state` snapshot rows the full reactor emits in production
    // (the rows the zakura-trace-plots skill consumes).
    trace: ZakuraTrace,
    finalized_height: block::Height,
    // The JSONL trace writer guard (when `trace_dir` was supplied). Flushed via
    // [`BenchCommitter::flush_trace`] so the trace tables are complete for review.
    trace_guard: Option<JsonlTraceGuard>,
    // Keeps the sequencer task alive for the lifetime of the committer.
    _join: JoinHandle<()>,
}

/// Spawns the real `SequencerTask` starting from `verified_block_tip` (typically
/// `start - 1`), with no peers and no reactor.
///
/// `submit_in_flight_limit` caps blocks submitted-but-not-applied; `max_inflight_bytes`
/// caps total in-flight body bytes (reorder + applying), which backpressures the feed
/// so the `applying` buffer can't grow unbounded — keep it finite for large windows.
///
/// When `trace_dir` is `Some`, the sequencer's structured Zakura JSONL trace tables
/// (the `BLOCK_SYNC_STATE` body lifecycle, etc.) are written there — the same tables
/// `perf-run-mainnet` produces via `[network.zakura] trace_dir`. The writer is flushed
/// by [`BenchCommitter::flush_trace`]. `None` runs with a no-op tracer (zero overhead).
pub fn spawn_bench_sequencer(
    finalized_height: block::Height,
    verified_block_tip: block::Height,
    verified_block_hash: block::Hash,
    submit_in_flight_limit: usize,
    max_inflight_bytes: u64,
    trace_dir: Option<PathBuf>,
) -> BenchSequencerHandle {
    let frontiers = BlockSyncFrontiers {
        finalized_height,
        verified_block_tip,
        verified_block_hash,
    };
    let limit = submit_in_flight_limit.max(1);

    // Real JSONL trace (same path as production) when a directory is supplied; the
    // guard is handed to the committer so the bench can flush+drain it at the end.
    let (trace, trace_guard) = match trace_dir {
        Some(dir) => {
            let guard = JsonlTracer::spawn_guard(dir);
            let trace = ZakuraTrace::new(guard.tracer(), "01");
            (trace, Some(guard))
        }
        None => (ZakuraTrace::noop(), None),
    };

    let sequencer = Sequencer::new(verified_block_tip, limit);
    let throughput = ThroughputMeter::new(Instant::now());
    let budget = ByteBudget::new(max_inflight_bytes.max(1));
    let work = Arc::new(WorkQueue::new(verified_block_tip));

    let (actions_tx, actions_rx) = mpsc::channel(limit + 128);
    let (body_input_tx, body_input_rx) = mpsc::channel(limit);
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let body_input_bytes = Arc::new(AtomicU64::new(0));
    let (view_tx, view_rx) = watch::channel(initial_view(frontiers));

    let task = SequencerTask::new(
        sequencer,
        budget,
        work,
        actions_tx,
        throughput,
        frontiers,
        body_input_rx,
        control_rx,
        body_input_bytes.clone(),
        view_tx,
        BENCH_ACTION_SEND_TIMEOUT,
        trace.clone(),
    );
    let join = tokio::spawn(task.run());

    BenchSequencerHandle {
        feeder: BenchBodyFeeder {
            body_input: body_input_tx,
            body_input_bytes,
            bench_peer: ZakuraPeerId::new(vec![0xB1; 32]).expect("32-byte bench peer id is valid"),
        },
        submissions: BenchSubmissions {
            actions: actions_rx,
        },
        committer: BenchCommitter {
            control: control_tx,
            view: view_rx,
            trace,
            finalized_height,
            trace_guard,
            _join: join,
        },
    }
}

impl BenchSequencerHandle {
    /// Splits into the (feeder, submissions, committer) parts so each can be driven
    /// on its own task.
    pub fn into_parts(self) -> (BenchBodyFeeder, BenchSubmissions, BenchCommitter) {
        (self.feeder, self.submissions, self.committer)
    }

    /// Production-driver split: returns the raw `BlockSyncAction` stream and an inert
    /// [`BlockSyncHandle`] so the bench can drive the **real** block-sync apply driver
    /// (`zebrad`'s `drive_block_sync_actions`) directly, instead of a hand-rolled
    /// verify/commit loop. The driver reports completions through
    /// [`BlockSyncHandle::send_control`]; with no reactor present, a spawned translator
    /// forwards each `BlockApplyFinished` into the sequencer's `ApplyFinished` control
    /// input — the exact hop `reactor::handle_block_apply_finished` performs in
    /// production. Every other handle channel is inert: the bench feeds bodies directly,
    /// so the driver never emits peer queries or reads peer/status/candidate state.
    pub fn into_driver_parts(self) -> BenchDriverParts {
        let BenchSequencerHandle {
            feeder,
            submissions,
            committer,
        } = self;

        // Highest committed height the driver has reported, so the feed can backpressure
        // on the *committed* tip (the submit window caps submission, but the sequencer's
        // `applying` set drains all contiguous fed bodies unbounded — a fast feed would
        // otherwise pile up the whole window in memory and OOM).
        let committed_tip = Arc::new(AtomicU64::new(0));
        let committed_tip_shim = committed_tip.clone();
        let control = committer.control.clone();
        let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel::<BlockSyncEvent>();
        tokio::spawn(async move {
            while let Some(event) = lifecycle_rx.recv().await {
                if let BlockSyncEvent::BlockApplyFinished {
                    token,
                    height,
                    hash,
                    result,
                    local_frontier,
                } = event
                {
                    committed_tip_shim.fetch_max(u64::from(height.0), Ordering::Relaxed);
                    if control
                        .send(SequencerControlInput::ApplyFinished {
                            token,
                            height,
                            hash,
                            result,
                            local_frontier,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            }
        });

        let config = ZakuraBlockSyncConfig::default();
        let (events, _events_rx) = mpsc::channel(1);
        let (_peers_tx, peers) = watch::channel(ServicePeerSnapshot::default());
        let (_status_tx, status) = watch::channel(config.initial_status());
        let (_candidates_tx, candidates) = watch::channel(ZakuraBlockSyncCandidateState::default());
        let block_sync = BlockSyncHandle {
            events,
            lifecycle: lifecycle_tx,
            peers,
            status,
            candidates,
            routine_wiring: None,
        };

        BenchDriverParts {
            feeder,
            actions: submissions.actions,
            block_sync,
            committer,
            committed_tip,
        }
    }
}

/// The driver-shaped split returned by [`BenchSequencerHandle::into_driver_parts`].
///
/// Hand `actions` + `block_sync` to the production `drive_block_sync_actions`, feed
/// cached bodies through `feeder`, and use `committer` for progress/trace snapshots
/// (it also keeps the sequencer task alive).
pub struct BenchDriverParts {
    /// Feeds cached bodies into the sequencer's reorder queue.
    pub feeder: BenchBodyFeeder,
    /// The ordered `SubmitBlock` (and other) actions the sequencer emits.
    pub actions: mpsc::Receiver<BlockSyncAction>,
    /// Inert handle the production driver reports completions through; its
    /// `BlockApplyFinished` events are forwarded to the sequencer.
    pub block_sync: BlockSyncHandle,
    /// Progress/trace snapshots; retained to keep the sequencer task alive.
    pub committer: BenchCommitter,
    /// Highest committed height reported by the driver. The feed reads this to cap how
    /// far ahead of the committed tip it runs, bounding the in-flight body backlog.
    pub committed_tip: Arc<AtomicU64>,
}

impl BenchBodyFeeder {
    /// Feeds one body into the reorder queue (awaits on backpressure). Returns
    /// `false` if the sequencer task has gone away.
    pub async fn feed_body(
        &self,
        height: block::Height,
        hash: block::Hash,
        block: Arc<Block>,
        bytes: u64,
    ) -> bool {
        let body = SequencedBody {
            height,
            hash,
            body: BufferedBlockBody::Decoded(block),
            bytes,
            peer: self.bench_peer.clone(),
            received_at: Instant::now(),
        };
        // Mirror the peer routine's byte accounting: reserve before send, release on
        // failure (the task decrements as it drains/applies).
        self.body_input_bytes.fetch_add(bytes, Ordering::Relaxed);
        if self.body_input.send(body).await.is_err() {
            self.body_input_bytes.fetch_sub(bytes, Ordering::Relaxed);
            return false;
        }
        true
    }
}

impl BenchSubmissions {
    /// Drains the next ordered submission, skipping non-submit actions (the bench
    /// has no peers/reactor, so only `SubmitBlock`/`Misbehavior` appear). Returns
    /// `None` once the action channel closes.
    pub async fn next_submit(&mut self) -> Option<BenchSubmit> {
        while let Some(action) = self.actions.recv().await {
            if let BlockSyncAction::SubmitBlock { token, block } = action {
                return Some(BenchSubmit { token, block });
            }
        }
        None
    }
}

impl BenchCommitter {
    /// Reports that a submitted block committed, advancing the sequencer frontier so
    /// it releases the next contiguous blocks.
    pub fn apply_committed(
        &self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
    ) {
        let local_frontier = BlockSyncFrontiers {
            finalized_height: self.finalized_height,
            verified_block_tip: height,
            verified_block_hash: hash,
        };
        let _ = self.control.send(SequencerControlInput::ApplyFinished {
            token,
            height,
            hash,
            result: BlockApplyResult::Committed,
            local_frontier: Some(local_frontier),
        });
    }

    /// Emit one `block_sync_state` snapshot row into `block_sync.jsonl`, mirroring the
    /// periodic row the full block-sync reactor writes in production (the row the
    /// zakura-trace-plots skill reads: `verified_block_tip`, `applying`, `reorder`,
    /// `submitted_applies`, and the in-flight byte counters). Cheap and non-blocking;
    /// a no-op when tracing is disabled. Call it on a cadence from the bench driver.
    pub fn emit_state_snapshot(&self) {
        let view = *self.view.borrow();
        self.trace.emit_with(BLOCK_SYNC_TABLE, |row| {
            row.insert(
                bs_trace::EVENT.to_string(),
                Value::String(bs_trace::BLOCK_SYNC_STATE.to_string()),
            );
            bs_insert_height(row, bs_trace::VERIFIED_BLOCK_TIP, view.verified_tip);
            bs_insert_u64(row, bs_trace::APPLYING, view.applying_len);
            bs_insert_u64(row, bs_trace::REORDER, view.reorder_len);
            bs_insert_u64(
                row,
                bs_trace::SUBMITTED_APPLIES,
                view.submitted_applying_count,
            );
            bs_insert_u64(row, "applying_buffered_bytes", view.applying_buffered_bytes);
            bs_insert_u64(row, "reorder_buffered_bytes", view.reorder_buffered_bytes);
            bs_insert_u64(
                row,
                "retained_pipeline_wire_bytes",
                view.applying_buffered_bytes
                    .saturating_add(view.reorder_buffered_bytes),
            );
        });
    }

    /// Flush and drain the JSONL trace writer (if tracing was enabled), so the trace
    /// tables on disk are complete before the bench process exits. A no-op when no
    /// `trace_dir` was supplied. Call after the drive loop finishes.
    pub async fn flush_trace(&mut self) {
        if let Some(guard) = self.trace_guard.take() {
            guard.shutdown().await;
        }
    }

    /// Awaits sequencer progress until the verified tip reaches `target` (or the
    /// sequencer task ends). Used to detect completion when the production driver owns
    /// the apply loop and the bench no longer sees individual commits.
    pub async fn wait_for_verified_tip(&mut self, target: block::Height) {
        loop {
            if self.view.borrow().verified_tip >= target {
                return;
            }
            if self.view.changed().await.is_err() {
                return;
            }
        }
    }

    /// Latest progress snapshot from the sequencer view.
    pub fn progress(&self) -> SequencerProgress {
        let view = *self.view.borrow();
        SequencerProgress {
            verified_tip: view.verified_tip,
            verified_hash: view.verified_hash,
            reorder_len: view.reorder_len,
            applying_len: view.applying_len,
            committed_blocks_per_sec: view.committed_blocks_per_sec,
        }
    }
}
