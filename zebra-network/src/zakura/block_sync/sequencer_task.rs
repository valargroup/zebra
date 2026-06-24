//! The Sequencer's own serial task (Sequencer task boundary split).
//!
//! Sequencer task moves the consensus-critical commit pipeline (`Sequencer`: reorder →
//! applying → verifier apply completion) off the reactor's single thread
//! and into this spawned serial task. The reactor keeps issuance, peer matching,
//! serving, and the producer; peer routines forward block bodies over a bounded
//! body input channel, while the reactor forwards rare external control events
//! over a non-blocking control channel. The reactor learns committed
//! progress back over a non-blocking `watch` ([`SequencerView`]).
//!
//! The logic in each input handler is the **verbatim** logic that used to run
//! inline in the matching reactor handler (`handle_block`'s body-acceptance tail,
//! `apply_state_frontiers_changed`'s Sequencer half, `handle_chain_tip_reset`,
//! and apply-completion handling); only its location and the budget/work/actions
//! handles it uses move here. See the  "Sequencer task".

use super::{
    events::*,
    reactor::{bs_insert_height, bs_insert_u64},
    reorder::BufferedBlockBody,
    sequencer::*,
    state::*,
    work_queue::WorkQueue,
    *,
};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};

/// How often the Sequencer task checks whether the byte budget is starving the
/// commit-unblocking (lowest pending) height and sheds the speculative top of the
/// reorder buffer to fund it. Bounds the recovery latency when no bodies are
/// flowing to trigger the inline check (e.g. once outstanding requests drain).
const FLOOR_STARVATION_SHED_INTERVAL: Duration = Duration::from_millis(500);

/// Maximum downloaded bodies handled in one fair-loop turn.
///
/// Body handling can drain/submit a large contiguous prefix, so one body per turn
/// is enough to keep floor bodies moving without letting body traffic bury
/// verifier completions and frontier/reset controls.
const MAX_BODY_BATCH_PER_TURN: usize = 1;

const CHECKPOINT_FRONTIER_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const CHECKPOINT_FRONTIER_REFRESH_ATTEMPTS: usize = 24;

#[derive(Debug)]
struct SubmittedBlockApply {
    class: BlockApplyClass,
    output: BlockApplyOutput,
}

#[derive(Clone, Debug, Default)]
struct CheckpointFrontierRefresh {
    highest_sent: Option<block::Height>,
    attempts_remaining: usize,
    next_attempt_at: Option<tokio::time::Instant>,
}

impl CheckpointFrontierRefresh {
    fn observe_checkpoint_commit(&mut self, highest_observed_at_apply: block::Height) {
        self.highest_sent = Some(
            self.highest_sent
                .map(|height| height.max(highest_observed_at_apply))
                .unwrap_or(highest_observed_at_apply),
        );
        self.attempts_remaining = CHECKPOINT_FRONTIER_REFRESH_ATTEMPTS;
        if self.next_attempt_at.is_none() {
            self.next_attempt_at =
                Some(tokio::time::Instant::now() + CHECKPOINT_FRONTIER_REFRESH_INTERVAL);
        }
    }

    fn next_attempt_at(&self) -> Option<tokio::time::Instant> {
        (self.attempts_remaining > 0)
            .then_some(self.next_attempt_at)
            .flatten()
    }

    fn finish_attempt(&mut self, highest_sent: block::Height) {
        self.highest_sent = Some(highest_sent);
        self.attempts_remaining = self.attempts_remaining.saturating_sub(1);
        self.next_attempt_at = (self.attempts_remaining > 0)
            .then_some(tokio::time::Instant::now() + CHECKPOINT_FRONTIER_REFRESH_INTERVAL);
    }
}

/// Favor the lowest needed height over the speculative high tail.
///
/// While the byte budget cannot fund even one worst-case request yet the lowest
/// needed height (pending or outstanding) sits *below* the highest buffered body,
/// drop that top body: release its bytes to the budget and return its height to
/// `pending` (it was held, hence in `work.in_flight` per the `held ⟺ in_flight`
/// invariant) for later re-fetch. Because another top can always be shed, a low
/// retry never blocks on budget; the floor can never wedge behind a full buffer,
/// and under a stall the speculative tail is shed and the chain fills bottom-up,
/// which also bounds the reorder backlog. Returns whether it shed
/// anything.
pub(super) fn shed_top_until_available(
    budget: &mut ByteBudget,
    work: &WorkQueue,
    sequencer: &mut Sequencer,
    target_available: u64,
) -> bool {
    let mut shed_any = false;
    while budget.available() < target_available {
        let lowest_needed = match (work.min_pending(), work.min_in_flight()) {
            (Some(pending), Some(in_flight)) => pending.min(in_flight),
            (Some(pending), None) => pending,
            (None, Some(in_flight)) => in_flight,
            (None, None) => break,
        };
        let Some(top) = sequencer.reorder_max_height() else {
            break;
        };
        // Only shed a body that sits above a starved lower height: we trade a
        // far-from-floor body for the ability to fetch a nearer, higher-value one.
        if lowest_needed >= top {
            break;
        }
        let freed = sequencer.drop_reorder_from(top);
        if freed == 0 {
            break;
        }
        let released = work.release_and_return_items([top]);
        debug_assert!(
            released == 0 || released == freed,
            "shed reorder release must match the per-height budget ledger when present"
        );
        budget.release(if released == 0 { freed } else { released });
        shed_any = true;
    }
    shed_any
}

pub(super) fn shed_top_for_floor_starvation(
    budget: &mut ByteBudget,
    work: &WorkQueue,
    sequencer: &mut Sequencer,
) -> bool {
    shed_top_until_available(
        budget,
        work,
        sequencer,
        super::config::BS_PER_BLOCK_WORST_CASE_BYTES,
    )
}

/// A received body a peer routine matched (or accepted unmatched) and forwards
/// to the commit pipeline. This is the only bounded Sequencer input: a slow
/// verifier can backpressure body intake, but must not block apply/frontier
/// control events that release budget and drive the next scheduling reaction.
#[derive(Clone, Debug)]
pub(super) struct SequencedBody {
    pub(super) height: block::Height,
    pub(super) hash: block::Hash,
    pub(super) body: BufferedBlockBody,
    pub(super) bytes: u64,
    pub(super) peer: ZakuraPeerId,
    pub(super) received_at: Instant,
}

/// Rare external Sequencer events forwarded by the reactor.
///
/// These events must not sit behind downloaded bodies. Frontier/reset events can
/// release or discard stale body work, and floor-funding requests synchronously
/// pop speculative tail bodies. They are locally generated and tiny, so they use
/// a separate unbounded channel.
#[derive(Debug)]
pub(super) enum SequencerControlInput {
    /// A verified-tip advance (frontier growth/commit).
    FrontierAdvance {
        frontiers: BlockSyncFrontiers,
        release_applied: bool,
    },
    /// A chain-tip reset (reorg/checkpoint/coalesced update). The two `peer_*`
    /// bools are the peer-outstanding-derived halves of the reset decision,
    /// precomputed by the reactor (which owns peer state); the task ORs them with
    /// its own Sequencer-internal predicates.
    FrontierReset {
        frontiers: BlockSyncFrontiers,
        preserve_active_successors: bool,
        /// `peers.any(outstanding.end_height() >= tip+1)` — half of
        /// `has_active_successor_after`.
        peer_has_successor_after: bool,
        /// `peers.any(outstanding.expected_hash(tip) is Some(h) && h != hash)` —
        /// the peer-outstanding clause of `reset_tip_conflicts_with_local_work`.
        peer_outstanding_conflicts_at_tip: bool,
    },
    /// Synchronously pop the speculative high tail until a floor request can
    /// reserve `needed_bytes`, then wake the requester to retry the reservation.
    FundFloorReservation {
        needed_bytes: u64,
        reply: oneshot::Sender<bool>,
    },
}

/// The committed view the reactor reacts to. A `watch` (latest-wins) send never
/// blocks, so the task never blocks on the reactor and the bounded input channel
/// cannot deadlock against it.
#[derive(Copy, Clone, Debug)]
pub(super) struct SequencerView {
    pub(super) verified_tip: block::Height,
    pub(super) verified_hash: block::Hash,
    pub(super) download_floor: block::Height,
    pub(super) finalized: block::Height,
    /// Increments only when the task performs a destructive `reset_to`, so the
    /// reactor distinguishes an advance (drop outstanding *through* tip) from a
    /// reset (drop *all* outstanding).
    pub(super) reset_epoch: u64,
    /// Increments once per processed frontier/reset/apply input (NOT per accepted
    /// body). The reactor runs its heavy serving/producer/schedule reaction only
    /// when this advances, mirroring the single-task version where a pure body
    /// buffer/submit reran nothing but the forwarding peer's reschedule, while a
    /// frontier advance, reset, or apply completion always reran query/schedule.
    pub(super) reaction_epoch: u64,
    pub(super) reorder_len: u64,
    pub(super) applying_len: u64,
    pub(super) reorder_buffered_bytes: u64,
    pub(super) applying_buffered_bytes: u64,
    pub(super) unsubmitted_applying_count: u64,
    pub(super) submitted_applying_count: u64,
    pub(super) submitted_applying_bytes: u64,
    pub(super) committed_bytes_per_sec: u64,
    pub(super) committed_blocks_per_sec: u64,
}

/// Build the initial view from the startup frontiers, before the task runs.
pub(super) fn initial_view(frontiers: BlockSyncFrontiers) -> SequencerView {
    SequencerView {
        verified_tip: frontiers.verified_block_tip,
        verified_hash: frontiers.verified_block_hash,
        download_floor: frontiers.verified_block_tip,
        finalized: frontiers.finalized_height,
        reset_epoch: 0,
        reaction_epoch: 0,
        reorder_len: 0,
        applying_len: 0,
        reorder_buffered_bytes: 0,
        applying_buffered_bytes: 0,
        unsubmitted_applying_count: 0,
        submitted_applying_count: 0,
        submitted_applying_bytes: 0,
        committed_bytes_per_sec: 0,
        committed_blocks_per_sec: 0,
    }
}

/// The serial commit-pipeline task. Owns the `Sequencer` (moved out of state), a
/// `ByteBudget` clone, an `Arc<WorkQueue>` clone, an action sender clone, and the
/// committed throughput meter. Releases bytes directly, drives verifier applies
/// through the installed executor, and emits `Misbehavior` on the same action
/// channel the reactor uses.
pub(super) struct SequencerTask {
    sequencer: Sequencer,
    budget: ByteBudget,
    work: Arc<WorkQueue>,
    actions: mpsc::Sender<BlockSyncAction>,
    committed_throughput: ThroughputMeter,
    /// Tracks the finalized height so the published view carries it forward; the
    /// reactor folds it into its `finalized_height` mirror with a `max`.
    finalized_height: block::Height,
    verified_block_hash: block::Hash,
    reset_epoch: u64,
    reaction_epoch: u64,
    body_input_rx: mpsc::Receiver<SequencedBody>,
    control_input_rx: mpsc::UnboundedReceiver<SequencerControlInput>,
    apply_executor_rx: watch::Receiver<Option<BlockApplyExecutorPort>>,
    apply_executor: Option<BlockApplyExecutorPort>,
    in_flight_applies: FuturesUnordered<BoxFuture<'static, SubmittedBlockApply>>,
    checkpoint_in_flight: usize,
    full_in_flight: usize,
    checkpoint_frontier_refresh: CheckpointFrontierRefresh,
    body_input_bytes: Arc<std::sync::atomic::AtomicU64>,
    view_tx: watch::Sender<SequencerView>,
    action_send_timeout: Duration,
    trace: ZakuraTrace,
}

impl SequencerTask {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        sequencer: Sequencer,
        budget: ByteBudget,
        work: Arc<WorkQueue>,
        actions: mpsc::Sender<BlockSyncAction>,
        committed_throughput: ThroughputMeter,
        frontiers: BlockSyncFrontiers,
        body_input_rx: mpsc::Receiver<SequencedBody>,
        control_input_rx: mpsc::UnboundedReceiver<SequencerControlInput>,
        apply_executor_rx: watch::Receiver<Option<BlockApplyExecutorPort>>,
        body_input_bytes: Arc<std::sync::atomic::AtomicU64>,
        view_tx: watch::Sender<SequencerView>,
        action_send_timeout: Duration,
        trace: ZakuraTrace,
    ) -> Self {
        Self {
            sequencer,
            budget,
            work,
            actions,
            committed_throughput,
            finalized_height: frontiers.finalized_height,
            verified_block_hash: frontiers.verified_block_hash,
            reset_epoch: 0,
            reaction_epoch: 0,
            body_input_rx,
            control_input_rx,
            apply_executor_rx,
            apply_executor: None,
            in_flight_applies: FuturesUnordered::new(),
            checkpoint_in_flight: 0,
            full_in_flight: 0,
            checkpoint_frontier_refresh: CheckpointFrontierRefresh::default(),
            body_input_bytes,
            view_tx,
            action_send_timeout,
            trace,
        }
    }

    pub(super) async fn run(mut self) {
        // Periodic shed backstop: catches budget starvation of the floor even when
        // no sequencer inputs are arriving to trigger the inline checks.
        let mut shed_tick = tokio::time::interval(FLOOR_STARVATION_SHED_INTERVAL);
        shed_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        self.install_apply_executor_if_ready().await;
        // Track input closure explicitly: the always-ready shed timer means the
        // `select!` never falls through to an `else`, so shut down only once both
        // input channels have closed.
        let mut control_open = true;
        let mut body_open = true;
        let mut apply_executor_open = true;
        loop {
            if !control_open && !body_open && self.in_flight_applies.is_empty() {
                break;
            }

            let mut progressed = false;

            if control_open {
                match self.control_input_rx.try_recv() {
                    Ok(input) => {
                        progressed = true;
                        self.process_control_input(input).await;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {}
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        control_open = false;
                    }
                }
            }

            if body_open {
                for _ in 0..MAX_BODY_BATCH_PER_TURN {
                    match self.body_input_rx.try_recv() {
                        Ok(body) => {
                            progressed = true;
                            self.process_body_input(body).await;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            body_open = false;
                            break;
                        }
                    }
                }
            }

            if self.refresh_due() {
                progressed = true;
                self.process_checkpoint_refresh().await;
            }

            if progressed {
                continue;
            }

            tokio::select! {
                input = self.control_input_rx.recv(), if control_open => {
                    match input {
                        Some(input) => self.process_control_input(input).await,
                        None => control_open = false,
                    }
                }
                body = self.body_input_rx.recv(), if body_open => {
                    match body {
                        Some(body) => self.process_body_input(body).await,
                        None => body_open = false,
                    }
                }
                completed = self.in_flight_applies.next(), if !self.in_flight_applies.is_empty() => {
                    if let Some(completed) = completed {
                        self.process_apply_completion(completed).await;
                    }
                }
                changed = self.apply_executor_rx.changed(), if apply_executor_open && self.should_watch_apply_executor() => {
                    if changed.is_ok() {
                        self.install_apply_executor_if_ready().await;
                    } else {
                        apply_executor_open = false;
                    }
                }
                _ = async {
                    match self.checkpoint_frontier_refresh.next_attempt_at() {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                }, if self.checkpoint_frontier_refresh.next_attempt_at().is_some() => {
                    self.process_checkpoint_refresh().await;
                }
                _ = shed_tick.tick() => {
                    if shed_top_for_floor_starvation(
                        &mut self.budget,
                        &self.work,
                        &mut self.sequencer,
                    ) {
                        self.publish_view();
                    }
                }
            }
        }
    }

    fn refresh_due(&self) -> bool {
        self.checkpoint_frontier_refresh
            .next_attempt_at()
            .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
    }

    async fn install_apply_executor_if_ready(&mut self) {
        if self.should_replace_apply_executor() {
            self.apply_executor = self.apply_executor_rx.borrow().clone();
        }
        self.submit_pending_blocks().await;
        self.publish_view();
    }

    fn should_watch_apply_executor(&self) -> bool {
        self.apply_executor.is_none() || cfg!(test)
    }

    fn should_replace_apply_executor(&self) -> bool {
        self.apply_executor.is_none() || cfg!(test)
    }

    async fn process_control_input(&mut self, input: SequencerControlInput) {
        let needs_reaction = self.handle_control_input(input).await;
        if needs_reaction {
            self.reaction_epoch = self.reaction_epoch.saturating_add(1);
        }
        self.publish_view();
    }

    async fn process_body_input(&mut self, body: SequencedBody) {
        self.release_body_input_bytes(body.bytes);
        self.handle_accept_body(body).await;
        shed_top_for_floor_starvation(&mut self.budget, &self.work, &mut self.sequencer);
        self.publish_view();
    }

    async fn process_apply_completion(&mut self, completed: SubmittedBlockApply) {
        self.decrement_in_flight_apply_count(completed.class);
        self.observe_apply_completion(completed.class, completed.output);
        let needs_reaction = self
            .handle_apply_finished(
                completed.output.token,
                completed.output.height,
                completed.output.hash,
                completed.output.result,
                completed.output.local_frontier,
            )
            .await;
        if needs_reaction {
            self.reaction_epoch = self.reaction_epoch.saturating_add(1);
        }
        self.publish_view();
    }

    async fn process_checkpoint_refresh(&mut self) {
        let Some(executor) = self.apply_executor.clone() else {
            return;
        };
        let Some(highest_sent) = self.checkpoint_frontier_refresh.highest_sent else {
            return;
        };
        let attempts_remaining = self.checkpoint_frontier_refresh.attempts_remaining;
        let frontiers = executor
            .refresh_checkpoint_frontier(highest_sent, attempts_remaining)
            .await;
        let next_highest_sent = frontiers
            .map(|frontiers| frontiers.verified_block_tip)
            .unwrap_or(highest_sent);
        self.checkpoint_frontier_refresh
            .finish_attempt(next_highest_sent);
        if let Some(frontiers) = frontiers {
            self.handle_frontier_advance(frontiers, true).await;
            self.reaction_epoch = self.reaction_epoch.saturating_add(1);
        }
        self.publish_view();
    }

    async fn handle_control_input(&mut self, input: SequencerControlInput) -> bool {
        // Each handler reports whether it did work that the single-task version
        // would have followed with the reactor's heavy serving/producer/schedule
        // tail. Bumping `reaction_epoch` only then keeps the reactor from
        // re-querying/-scheduling on a pure body buffer/submit or a no-op
        // (stale/duplicate) apply completion.
        match input {
            SequencerControlInput::FrontierAdvance {
                frontiers,
                release_applied,
            } => {
                self.handle_frontier_advance(frontiers, release_applied)
                    .await;
                true
            }
            SequencerControlInput::FrontierReset {
                frontiers,
                preserve_active_successors,
                peer_has_successor_after,
                peer_outstanding_conflicts_at_tip,
            } => {
                self.handle_frontier_reset(
                    frontiers,
                    preserve_active_successors,
                    peer_has_successor_after,
                    peer_outstanding_conflicts_at_tip,
                )
                .await;
                true
            }
            SequencerControlInput::FundFloorReservation {
                needed_bytes,
                reply,
            } => {
                let shed = shed_top_until_available(
                    &mut self.budget,
                    &self.work,
                    &mut self.sequencer,
                    needed_bytes,
                );
                let _ = reply.send(self.budget.available() >= needed_bytes);
                shed
            }
        }
    }

    fn release_body_input_bytes(&self, bytes: u64) {
        let _ = self.body_input_bytes.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |current| Some(current.saturating_sub(bytes)),
        );
    }

    /// Body-acceptance tail (verbatim from `handle_block` ~885-907 and
    /// `accept_unmatched_queued_body` ~1170-1183): offer the body, release on
    /// `Redundant`, then drain ready prefix into applying and submit.
    async fn handle_accept_body(&mut self, body: SequencedBody) {
        let queued_elapsed = body.received_at.elapsed();
        let outcome = match self.sequencer.accept_buffered_body(
            body.height,
            body.hash,
            body.body,
            body.bytes,
            body.peer,
        ) {
            AcceptOutcome::Buffered { .. } => "buffered",
            AcceptOutcome::Redundant { release_bytes } => {
                self.budget.release(release_bytes);
                "redundant"
            }
        };
        self.trace_body_accepted(body.height, queued_elapsed, outcome);
        self.release_contiguous_blocks().await;
    }

    /// Sequencer half of `apply_state_frontiers_changed` (verbatim from
    /// reactor.rs ~447-478, including the stale guard).
    async fn handle_frontier_advance(
        &mut self,
        frontiers: BlockSyncFrontiers,
        release_applied: bool,
    ) {
        // Fold the finalized height forward unconditionally (matches the original's
        // first line), then drop a stale update. The verified tip is monotonic: an
        // advance whose target is below our verified tip must be a no-op, never a
        // regression. This guard is the original `apply_state_frontiers_changed`'s
        // `verified_block_tip < verified_tip() => return None`; without it the
        // second growth-reset path (`< floor`, which permits `< verified_tip`) would
        // call `advance_verified_tip` with a lower tip and regress it.
        self.finalized_height = self.finalized_height.max(frontiers.finalized_height);
        if frontiers.verified_block_tip < self.sequencer.verified_tip() {
            return;
        }
        self.verified_block_hash = frontiers.verified_block_hash;
        let advance = self
            .sequencer
            .advance_verified_tip(frontiers.verified_block_tip, release_applied);
        self.budget.release(advance.release_bytes);
        if advance.changed {
            let released = self.work.advance_floor(frontiers.verified_block_tip);
            self.budget.release(released);
            self.release_contiguous_blocks().await;
        }
    }

    /// The Sequencer/work/budget body of `handle_chain_tip_reset` (verbatim from
    /// reactor.rs 502-576). The peer-outstanding reads are replaced by the
    /// precomputed `peer_*` bools.
    async fn handle_frontier_reset(
        &mut self,
        frontiers: BlockSyncFrontiers,
        preserve_active_successors: bool,
        peer_has_successor_after: bool,
        peer_outstanding_conflicts_at_tip: bool,
    ) {
        let reset_tip_matches_local_work = !self.reset_tip_conflicts_with_local_work(
            &frontiers,
            frontiers.verified_block_tip <= self.sequencer.floor(),
            peer_outstanding_conflicts_at_tip,
        );

        // State can report a forward `Reset` while checkpoint commits advance
        // under already-submitted or still-downloading successor bodies. Treat
        // that as verified growth once it is inside our submitted/downloaded
        // floor, or when we already have successor work in flight. Keep fork
        // resets destructive when they are not anchored by active successor
        // work.
        if frontiers.verified_block_tip > self.sequencer.verified_tip()
            && (frontiers.verified_block_tip <= self.sequencer.floor()
                || self.has_active_successor_after(
                    frontiers.verified_block_tip,
                    peer_has_successor_after,
                ))
            && reset_tip_matches_local_work
        {
            // Growth-classified reset: treat as a frontier advance (same as the
            // reactor's `handle_state_frontiers_changed` path), `release_applied`.
            self.handle_frontier_advance(frontiers, true).await;
            return;
        }

        metrics::counter!("sync.block.reorg.reset").increment(1);

        // A `Reset` can also be a stale or coalesced state update for a tip
        // already inside our contiguous submitted/downloaded body floor. Do not
        // destructively clear successor bodies in that case: a stale reset
        // snapshot can otherwise erase `applying`/covered state and re-request
        // the same bodies while their first apply is still in flight.
        if preserve_active_successors
            && frontiers.verified_block_tip < self.sequencer.floor()
            && reset_tip_matches_local_work
            && self
                .has_active_successor_after(frontiers.verified_block_tip, peer_has_successor_after)
            && self.active_successor_links_to_anchor(
                frontiers.verified_block_tip,
                frontiers.verified_block_hash,
            )
        {
            self.handle_frontier_advance(frontiers, true).await;
            return;
        }

        let remember_released_applies = frontiers.verified_block_tip > frontiers.finalized_height
            && frontiers.verified_block_tip <= self.sequencer.floor();

        self.finalized_height = frontiers.finalized_height;
        self.verified_block_hash = frontiers.verified_block_hash;

        // The Sequencer pins its verified tip and floor to the reset target and
        // clears the reorder/applying buffers, returning the freed bytes for
        // release.
        let released = self
            .sequencer
            .reset_to(frontiers.verified_block_tip, remember_released_applies);
        self.budget.release(released);
        // Drop every download work item above the reset target (their buffers
        // were cleared by `reset_to`); the reactor's `query_needed_blocks`
        // re-fills.
        let released = self.work.reset_above(self.sequencer.floor());
        self.budget.release(released);
        // A destructive reset: bump the epoch so the reactor drops *all*
        // outstanding requests (not just those through the tip).
        self.reset_epoch = self.reset_epoch.saturating_add(1);
    }

    /// Apply-completion bookkeeping, minus the reactor-side serving/query/
    /// schedule/status tail (which the view reaction runs). The embedded
    /// `local_frontier` advance is folded in as a frontier advance with
    /// `release_applied: false`.
    async fn handle_apply_finished(
        &mut self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
        result: BlockApplyResult,
        local_frontier: Option<BlockSyncFrontiers>,
    ) -> bool {
        // A stale completion (no live applying entry, or token/hash mismatch)
        // only decrements the submitted-apply record and returns; the single-task
        // version ran no query/schedule tail here, so it needs no reaction.
        let Some((applying_token, applying_hash)) = self.sequencer.applying_token_hash(height)
        else {
            self.sequencer.decrement_submitted_apply(height, hash);
            return false;
        };
        if applying_hash != hash || applying_token != token {
            self.sequencer.decrement_submitted_apply(height, hash);
            return false;
        }

        let accepted_local_frontier = if let Some(frontiers) = local_frontier {
            // Fold the `local_frontier` advance in as a frontier advance without
            // releasing committed applying bodies (`release_applied: false`),
            // matching the inline `apply_state_frontiers_changed(.., false)` call.
            // It is accepted only when it is not a stale (older-tip) update.
            if frontiers.verified_block_tip < self.sequencer.verified_tip() {
                None
            } else {
                self.handle_frontier_advance(frontiers, false).await;
                Some(frontiers)
            }
        } else {
            None
        };

        if matches!(result, BlockApplyResult::Duplicate) && self.sequencer.verified_tip() < height {
            // Stale duplicate for a height we have not verified to: the single-task
            // version ran the serving/query tail only when the accepted local
            // frontier advanced serving (an `old_serving_tip` existed).
            return accepted_local_frontier.is_some();
        }
        let applying = self
            .sequencer
            .remove_applying(height)
            .expect("applying entry exists because it was just checked");

        self.budget.release(applying.bytes);
        // A `Committed` result is a body that newly extended the chain; count it
        // toward commit throughput (the apply rate the download path is racing).
        if matches!(result, BlockApplyResult::Committed) {
            self.committed_throughput.record(applying.bytes);
        }
        self.sequencer.decrement_submitted_apply(height, hash);
        match result {
            BlockApplyResult::Committed | BlockApplyResult::Duplicate => {}
            BlockApplyResult::Rejected | BlockApplyResult::TimedOut
                if height > self.sequencer.verified_tip() =>
            {
                // Drop the rejected body and every successor (in applying and
                // reorder), roll the floor back below it, and drop the WorkQueue
                // entries above the rolled-back floor so the heights are
                // re-requestable (the reactor's `query_needed_blocks` re-fills).
                let released = self.sequencer.release_applying_blocks_from(height);
                self.budget.release(released);
                self.sequencer.reset_floor_below(height);
                let released = self.work.reset_above(self.sequencer.floor());
                self.budget.release(released);
                let dropped = self.sequencer.drop_reorder_from(height);
                self.budget.release(dropped);
                // A `Rejected` result means consensus found the body invalid.
                // Attribute it to the delivering peer so repeat offenders are
                // scored and eventually disconnected. `TimedOut` is a local apply
                // timeout, not a peer fault, so it is not scored.
                if matches!(result, BlockApplyResult::Rejected) {
                    Self::send_action(
                        self.actions.clone(),
                        self.action_send_timeout,
                        BlockSyncAction::Misbehavior {
                            peer: applying.source_peer.clone(),
                            reason: BlockSyncMisbehavior::InvalidBlock,
                        },
                    )
                    .await;
                }
            }
            BlockApplyResult::Rejected | BlockApplyResult::TimedOut => {}
        }
        if let Some(frontiers) = accepted_local_frontier {
            let released = self
                .sequencer
                .release_applied_through(frontiers.verified_block_tip);
            self.budget.release(released);
        }

        self.release_contiguous_blocks().await;
        true
    }

    /// Drain the contiguous reorder prefix into applying and submit (verbatim
    /// from `release_contiguous_blocks` + `submit_pending_blocks`).
    async fn release_contiguous_blocks(&mut self) {
        let _ = self.sequencer.drain_ready_into_applying();
        self.submit_pending_blocks().await;
    }

    async fn submit_pending_blocks(&mut self) {
        let Some(executor) = self.apply_executor.clone() else {
            return;
        };
        let limits = executor.limits();
        for height in self.sequencer.submittable_heights() {
            let Some(item) = self.sequencer.prepare_submit(height) else {
                continue;
            };
            let class = executor.block_apply_class(item.block.as_ref());
            if !self.can_submit_class(class, limits) {
                self.sequencer.unsubmit(item.height, item.token);
                continue;
            }

            metrics::counter!("sync.block.submit.sent").increment(1);
            self.increment_in_flight_apply_count(class);
            self.sequencer
                .record_submitted_apply(item.height, item.hash);
            self.trace_body_submitted(item.height, item.token);
            #[cfg(test)]
            {
                let _ = self.actions.try_send(BlockSyncAction::ApplySubmitted {
                    token: item.token,
                    block: item.block.clone(),
                });
            }
            let apply = executor.apply(BlockApplyRequest {
                token: item.token,
                block: item.block,
            });
            self.in_flight_applies.push(
                async move {
                    let output = apply.await;
                    SubmittedBlockApply { class, output }
                }
                .boxed(),
            );
        }
    }

    fn can_submit_class(&self, class: BlockApplyClass, limits: BlockApplyLimits) -> bool {
        // The checkpoint verifier can hold a complete range until its checkpoint is
        // reached. Keep room for the current range and the next complete range.
        let checkpoint_pipeline_apply_limit = limits.checkpoint_apply_limit.saturating_mul(2);
        let checkpoint_combined_apply_limit = limits
            .combined_apply_limit
            .max(checkpoint_pipeline_apply_limit);
        match class {
            BlockApplyClass::Checkpoint => {
                self.checkpoint_in_flight
                    .saturating_add(self.full_in_flight)
                    < checkpoint_combined_apply_limit
                    && self.checkpoint_in_flight < checkpoint_pipeline_apply_limit
            }
            BlockApplyClass::Full => {
                self.checkpoint_in_flight
                    .saturating_add(self.full_in_flight)
                    < limits.combined_apply_limit
                    && self.full_in_flight < limits.full_apply_limit
            }
        }
    }

    fn increment_in_flight_apply_count(&mut self, class: BlockApplyClass) {
        match class {
            BlockApplyClass::Checkpoint => {
                self.checkpoint_in_flight = self.checkpoint_in_flight.saturating_add(1);
            }
            BlockApplyClass::Full => {
                self.full_in_flight = self.full_in_flight.saturating_add(1);
            }
        }
    }

    fn decrement_in_flight_apply_count(&mut self, class: BlockApplyClass) {
        match class {
            BlockApplyClass::Checkpoint => {
                self.checkpoint_in_flight = self.checkpoint_in_flight.saturating_sub(1);
            }
            BlockApplyClass::Full => {
                self.full_in_flight = self.full_in_flight.saturating_sub(1);
            }
        }
    }

    fn observe_apply_completion(&mut self, class: BlockApplyClass, output: BlockApplyOutput) {
        if class == BlockApplyClass::Checkpoint && output.result == BlockApplyResult::Committed {
            let highest_observed_at_apply = output
                .local_frontier
                .map(|frontiers| frontiers.verified_block_tip)
                .unwrap_or_else(|| output.height.previous().unwrap_or(output.height));
            self.checkpoint_frontier_refresh
                .observe_checkpoint_commit(highest_observed_at_apply);
        }
    }

    fn trace_body_submitted(&self, height: block::Height, token: BlockApplyToken) {
        self.trace.emit_with(BLOCK_SYNC_TABLE, |row| {
            row.insert(
                bs_trace::EVENT.to_string(),
                serde_json::Value::String(bs_trace::BLOCK_BODY_SUBMITTED.to_string()),
            );
            bs_insert_height(row, bs_trace::HEIGHT, height);
            bs_insert_u64(row, bs_trace::APPLY_TOKEN, token);
        });
    }

    fn trace_body_accepted(&self, height: block::Height, queued_elapsed: Duration, outcome: &str) {
        self.trace.emit_with(BLOCK_SYNC_TABLE, |row| {
            row.insert(
                bs_trace::EVENT.to_string(),
                serde_json::Value::String(bs_trace::BLOCK_BODY_ACCEPTED.to_string()),
            );
            bs_insert_height(row, bs_trace::HEIGHT, height);
            bs_insert_u64(
                row,
                "sequencer_queue_elapsed_us",
                u64::try_from(queued_elapsed.as_micros()).unwrap_or(u64::MAX),
            );
            row.insert(
                bs_trace::RESULT.to_string(),
                serde_json::Value::String(outcome.to_string()),
            );
        });
    }

    /// `reset_tip_conflicts_with_local_work`'s Sequencer-internal predicates,
    /// with the peer-outstanding clause supplied by the reactor.
    fn reset_tip_conflicts_with_local_work(
        &self,
        frontiers: &BlockSyncFrontiers,
        ignore_non_material_conflicts: bool,
        peer_outstanding_conflicts_at_tip: bool,
    ) -> bool {
        let height = frontiers.verified_block_tip;
        let hash = frontiers.verified_block_hash;

        if self
            .sequencer
            .reorder_hash(height)
            .is_some_and(|buffered_hash| buffered_hash != hash)
        {
            return true;
        }
        if self
            .sequencer
            .applying_hash(height)
            .is_some_and(|applying_hash| applying_hash != hash)
        {
            return true;
        }
        if !ignore_non_material_conflicts
            && self.sequencer.submitted_has_only_other_hashes(height, hash)
        {
            return true;
        }
        if !ignore_non_material_conflicts && peer_outstanding_conflicts_at_tip {
            return true;
        }
        false
    }

    fn has_active_successor_after(
        &self,
        height: block::Height,
        peer_has_successor_after: bool,
    ) -> bool {
        let Some(next) = next_height(height) else {
            return false;
        };

        self.sequencer.has_buffered_at_or_above(next) || peer_has_successor_after
    }

    fn active_successor_links_to_anchor(
        &self,
        height: block::Height,
        anchor_hash: block::Hash,
    ) -> bool {
        let Some(next) = next_height(height) else {
            return true;
        };

        self.sequencer
            .applying_previous_block_hash(next)
            .map(|previous_block_hash| previous_block_hash == anchor_hash)
            .unwrap_or(true)
    }

    async fn send_action(
        actions: mpsc::Sender<BlockSyncAction>,
        action_send_timeout: Duration,
        action: BlockSyncAction,
    ) -> bool {
        match time::timeout(action_send_timeout, actions.send(action)).await {
            Ok(Ok(())) => true,
            Ok(Err(_)) => false,
            Err(_) => {
                metrics::counter!("sync.block.action.send_timeout").increment(1);
                false
            }
        }
    }

    fn publish_view(&mut self) {
        self.committed_throughput.sample(Instant::now());
        let reorder_buffered_bytes = self.sequencer.reorder_buffered_bytes();
        let applying_buffered_bytes = self.sequencer.applying_buffered_bytes();
        let body_input_bytes = self
            .body_input_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let expected_budget = self
            .work
            .reserved_bytes()
            .saturating_add(reorder_buffered_bytes)
            .saturating_add(applying_buffered_bytes)
            .saturating_add(body_input_bytes);
        self.budget
            .audit(expected_budget, "block-sync sequencer view");
        let _ = self.view_tx.send_replace(SequencerView {
            verified_tip: self.sequencer.verified_tip(),
            verified_hash: self.verified_block_hash,
            download_floor: self.sequencer.floor(),
            finalized: self.finalized_height,
            reset_epoch: self.reset_epoch,
            reaction_epoch: self.reaction_epoch,
            reorder_len: self.sequencer.reorder_len() as u64,
            applying_len: self.sequencer.applying_len() as u64,
            reorder_buffered_bytes,
            applying_buffered_bytes,
            unsubmitted_applying_count: self.sequencer.unsubmitted_applying_count() as u64,
            submitted_applying_count: self.sequencer.submitted_applying_count() as u64,
            submitted_applying_bytes: self.sequencer.submitted_applying_bytes(),
            committed_bytes_per_sec: self.committed_throughput.bytes_per_sec(),
            committed_blocks_per_sec: self.committed_throughput.blocks_per_sec(),
        });
    }
}
