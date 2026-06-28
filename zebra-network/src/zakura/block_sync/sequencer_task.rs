//! The Sequencer's own serial task (Sequencer task boundary split).
//!
//! The Sequencer task moves the consensus-critical commit pipeline (`Sequencer`:
//! reorder → applying) off the reactor's single thread and into this spawned
//! serial task. The reactor keeps issuance, peer matching, serving, and the
//! producer; peer routines forward block bodies over a bounded body input channel,
//! while the reactor forwards rare external control events over a non-blocking
//! control channel. The reactor learns committed progress back over a non-blocking
//! `watch` ([`SequencerView`]).
//!
//! The commit *tail* is now the apply seam: the task drains its contiguous reorder
//! prefix into [`ApplyItem`]s and pushes them onto the `applyQ`
//! (`apply_tx`). A node-side `Committer` drains that queue and fires
//! `Request::Commit`; on a commit failure it raises a [`CommitterReset`] back here.
//! Byte release is gated on **durability**: the held ledger entries are released
//! only when a [`SequencerControlInput::FrontierAdvance`] (driven by the durable
//! chain-tip watch) crosses their height, so the byte budget is a true end-to-end
//! memory bound.

use super::{
    reactor::{bs_insert_height, bs_insert_u64},
    reorder::BufferedBlockBody,
    sequencer::*,
    state::*,
    work_queue::WorkQueue,
    *,
};

#[derive(Copy, Clone, Debug)]
enum ReadySource {
    Control,
    Body,
}

impl ReadySource {
    const COUNT: usize = 2;

    fn from_index(index: usize) -> Self {
        match index % Self::COUNT {
            0 => Self::Control,
            1 => Self::Body,
            _ => unreachable!("ready source index is modulo source count"),
        }
    }

    fn next(self) -> Self {
        Self::from_index(self.index() + 1)
    }

    fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::Body => 1,
        }
    }
}

/// Favor the lowest needed height over the speculative high tail.
///
/// When a floor reservation cannot be funded, and the lowest needed height
/// (pending or outstanding) sits *below* the highest buffered body, drop that top
/// body: release its bytes to the budget and return its height to `pending` (it
/// was held, hence in `work.in_flight` per the `held ⟺ in_flight` invariant) for
/// later re-fetch. The floor requester calls this synchronously through
/// [`SequencerControlInput::FundFloorReservation`], so the rescue path is
/// demand-driven instead of timer-driven. Returns whether it shed anything.
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
    /// A verified-tip advance (frontier growth/commit). Driven by the durable
    /// chain-tip watch (and, idempotently, the endpoint frontier mirror); its
    /// `release_applied` frees the now-durable held ledger entries.
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
    /// The Committer rejected a body (consensus-invalid or apply-timeout). Roll the
    /// download floor back below the failed height, drop the body and every
    /// successor (reorder + the held ledger + the work queue above the rolled-back
    /// floor), and — for [`CommitRejection::Invalid`] — score the delivering peer.
    /// Guarded on a per-height apply epoch so a stale reset (older generation,
    /// already committed, or already rolled back) is a no-op.
    CommitRejected(CommitterReset),
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
    /// Increments once per processed frontier/reset/reject input (NOT per accepted
    /// body). The reactor runs its heavy serving/producer/schedule reaction only
    /// when this advances, mirroring the single-task version where a pure body
    /// buffer/drain reran nothing but the forwarding peer's reschedule, while a
    /// frontier advance or reset always reran query/schedule.
    pub(super) reaction_epoch: u64,
    pub(super) reorder_len: u64,
    /// Heights drained onto the applyQ and held until durable.
    pub(super) applying_len: u64,
    pub(super) reorder_buffered_bytes: u64,
    /// Bytes held against the budget for drained-but-not-yet-durable heights.
    pub(super) applying_buffered_bytes: u64,
    pub(super) lowest_applying_height: Option<block::Height>,
    pub(super) commit_frontier_stall_seconds: u64,
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
        lowest_applying_height: None,
        commit_frontier_stall_seconds: 0,
        committed_bytes_per_sec: 0,
        committed_blocks_per_sec: 0,
    }
}

/// The serial commit-pipeline task. Owns the `Sequencer` (moved out of state), a
/// `ByteBudget` clone, an `Arc<WorkQueue>` clone, an action sender clone, and the
/// committed-throughput meter. Drains the contiguous reorder prefix onto the
/// applyQ, releases bytes on the durable frontier advance, and emits `Misbehavior`
/// on the same action channel the reactor uses.
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
    /// Apply generation stamped onto every drained `ApplyItem` and held ledger
    /// entry. Bumped on every rollback (a processed `CommitRejected` and a
    /// destructive `reset_to`) so re-drained heights get a fresh generation and a
    /// stale reset/commit from a superseded generation is ignored. 1-based, so the
    /// Committer's initial `last_reset_epoch` of 0 discards nothing.
    apply_epoch: u64,
    body_input_rx: mpsc::Receiver<SequencedBody>,
    control_input_rx: mpsc::UnboundedReceiver<SequencerControlInput>,
    /// The applyQ sender: contiguous, hash-verified bodies handed to the node-side
    /// `Committer`. Unbounded so a push never blocks the serial task; the byte
    /// budget bounds total in-flight memory (every item is a held, reserved block).
    apply_tx: mpsc::UnboundedSender<ApplyItem>,
    body_input_bytes: Arc<std::sync::atomic::AtomicU64>,
    view_tx: watch::Sender<SequencerView>,
    action_send_timeout: Duration,
    trace: ZakuraTrace,
    next_ready_source: ReadySource,
    commit_frontier_since: Instant,
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
        apply_tx: mpsc::UnboundedSender<ApplyItem>,
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
            apply_epoch: 1,
            body_input_rx,
            control_input_rx,
            apply_tx,
            body_input_bytes,
            view_tx,
            action_send_timeout,
            trace,
            next_ready_source: ReadySource::Control,
            commit_frontier_since: Instant::now(),
        }
    }

    pub(super) async fn run(mut self) {
        self.publish_view();
        // Track input closure explicitly so each channel can close independently
        // while the task continues draining the other.
        let mut control_open = true;
        let mut body_open = true;
        loop {
            if !control_open && !body_open {
                break;
            }

            if self
                .process_one_ready(&mut control_open, &mut body_open)
                .await
            {
                continue;
            }

            tokio::select! {
                input = self.control_input_rx.recv(), if control_open => {
                    self.next_ready_source = ReadySource::Body;
                    match input {
                        Some(input) => self.process_control_input(input).await,
                        None => control_open = false,
                    }
                }
                body = self.body_input_rx.recv(), if body_open => {
                    self.next_ready_source = ReadySource::Control;
                    match body {
                        Some(body) => self.process_body_input(body).await,
                        None => body_open = false,
                    }
                }
            }
        }
    }

    async fn process_one_ready(&mut self, control_open: &mut bool, body_open: &mut bool) -> bool {
        let start = self.next_ready_source.index();
        for offset in 0..ReadySource::COUNT {
            let source = ReadySource::from_index(start + offset);
            if self
                .process_ready_source(source, control_open, body_open)
                .await
            {
                self.next_ready_source = source.next();
                return true;
            }
        }
        false
    }

    async fn process_ready_source(
        &mut self,
        source: ReadySource,
        control_open: &mut bool,
        body_open: &mut bool,
    ) -> bool {
        match source {
            ReadySource::Control => {
                if !*control_open {
                    return false;
                }
                match self.control_input_rx.try_recv() {
                    Ok(input) => {
                        self.process_control_input(input).await;
                        true
                    }
                    Err(mpsc::error::TryRecvError::Empty) => false,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        *control_open = false;
                        false
                    }
                }
            }
            ReadySource::Body => {
                if !*body_open {
                    return false;
                }
                match self.body_input_rx.try_recv() {
                    Ok(body) => {
                        self.process_body_input(body).await;
                        true
                    }
                    Err(mpsc::error::TryRecvError::Empty) => false,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        *body_open = false;
                        false
                    }
                }
            }
        }
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
        self.publish_view();
    }

    async fn handle_control_input(&mut self, input: SequencerControlInput) -> bool {
        // Each handler reports whether it did work that the single-task version
        // would have followed with the reactor's heavy serving/producer/schedule
        // tail. Bumping `reaction_epoch` only then keeps the reactor from
        // re-querying/-scheduling on a pure body buffer/drain or a no-op (stale)
        // reject.
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
            SequencerControlInput::CommitRejected(reset) => {
                self.handle_commit_rejected(reset).await
            }
        }
    }

    /// Relocated reject/timeout floor-rollback, now triggered by the out-of-task
    /// Committer.
    ///
    /// Drops the rejected body and every successor (the held ledger, the reorder
    /// buffer, and the work queue above the rolled-back floor), rolls the download
    /// floor back below the failed height so it is re-requestable, bumps the apply
    /// epoch so re-drained heights get a fresh generation, and scores the
    /// delivering peer for a consensus-invalid body (never for a local apply
    /// timeout).
    ///
    /// Guarded on the held height still carrying the reset's apply epoch: a height
    /// already dropped by a lower reset, already committed by a coalesced durable
    /// advance, or re-pushed at a newer generation is a no-op. This makes
    /// lowest-reset-wins fall out regardless of the order completions resolve in,
    /// and ignores a stale reset from a superseded generation.
    async fn handle_commit_rejected(&mut self, reset: CommitterReset) -> bool {
        let height = reset.height;
        if self.sequencer.applying_epoch(height) != Some(reset.epoch) {
            return false;
        }

        // The per-height guard above fired, so `height` is genuinely being rolled
        // back at its own generation. The purge below then drops *every* successor
        // at or above `height` unconditionally — deliberately NOT epoch-bounded:
        // the chain is contiguous, so any successor (even one freshly re-drained
        // at a newer generation after an intervening higher reset) descends from
        // the rolled-back `height` and must be redone too. Narrowing the purge to
        // `entry.epoch == reset.epoch` would orphan such successors. Bytes for an
        // already-committed successor are simply released once here and become a
        // no-op for the later durable advance (`release_applied_through` finds it
        // gone), so there is no double-release.
        let released = self.sequencer.release_applying_blocks_from(height);
        self.budget.release(released);
        self.sequencer.reset_floor_below(height);
        let released = self.work.reset_above(self.sequencer.floor());
        self.budget.release(released);
        let dropped = self.sequencer.drop_reorder_from(height);
        self.budget.release(dropped);
        self.apply_epoch = self.apply_epoch.saturating_add(1);

        if matches!(reset.rejection, CommitRejection::Invalid) {
            Self::send_action(
                self.actions.clone(),
                self.action_send_timeout,
                BlockSyncAction::Misbehavior {
                    peer: reset.source_peer.clone(),
                    reason: BlockSyncMisbehavior::InvalidBlock,
                },
            )
            .await;
        }

        self.release_contiguous_blocks().await;
        true
    }

    fn release_body_input_bytes(&self, bytes: u64) {
        let _ = self.body_input_bytes.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |current| Some(current.saturating_sub(bytes)),
        );
    }

    /// Body-acceptance tail: offer the body, release on `Redundant`, then drain the
    /// ready contiguous prefix onto the applyQ.
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

    /// Sequencer half of `apply_state_frontiers_changed` (including the stale
    /// guard). The verified-tip advance is monotonic and releases the now-durable
    /// held ledger entries when `release_applied`.
    async fn handle_frontier_advance(
        &mut self,
        frontiers: BlockSyncFrontiers,
        release_applied: bool,
    ) {
        // Fold the finalized height forward unconditionally, then drop a stale
        // update. The verified tip is monotonic: an advance whose target is below
        // our verified tip must be a no-op, never a regression. This is the
        // original `apply_state_frontiers_changed`'s
        // `verified_block_tip < verified_tip() => return None`.
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
            self.commit_frontier_since = Instant::now();
            // Durable commit throughput: the held heights this advance made durable.
            if advance.committed_blocks > 0 {
                self.committed_throughput
                    .record_n(advance.committed_blocks, advance.committed_bytes);
            }
            let released = self.work.advance_floor(frontiers.verified_block_tip);
            self.budget.release(released);
            self.release_contiguous_blocks().await;
        }
    }

    /// The Sequencer/work/budget body of `handle_chain_tip_reset`. The
    /// peer-outstanding reads are replaced by the precomputed `peer_*` bools.
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
        // under already-drained or still-downloading successor bodies. Treat that
        // as verified growth once it is inside our drained/downloaded floor, or
        // when we already have successor work in flight. Keep fork resets
        // destructive when they are not anchored by active successor work.
        if frontiers.verified_block_tip > self.sequencer.verified_tip()
            && (frontiers.verified_block_tip <= self.sequencer.floor()
                || self.has_active_successor_after(
                    frontiers.verified_block_tip,
                    peer_has_successor_after,
                ))
            && reset_tip_matches_local_work
        {
            // Growth-classified reset: treat as a frontier advance, `release_applied`.
            self.handle_frontier_advance(frontiers, true).await;
            return;
        }

        metrics::counter!("sync.block.reorg.reset").increment(1);

        // A `Reset` can also be a stale or coalesced state update for a tip already
        // inside our contiguous drained/downloaded body floor. Do not destructively
        // clear successor bodies in that case: a stale reset snapshot can otherwise
        // erase applying/covered state and re-request the same bodies while their
        // first commit is still in flight.
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

        self.finalized_height = frontiers.finalized_height;
        self.verified_block_hash = frontiers.verified_block_hash;

        // The Sequencer pins its verified tip and floor to the reset target and
        // clears the reorder/held buffers, returning the freed bytes for release.
        let released = self.sequencer.reset_to(frontiers.verified_block_tip);
        self.budget.release(released);
        self.commit_frontier_since = Instant::now();
        // Drop every download work item above the reset target (their buffers were
        // cleared by `reset_to`); the reactor's `query_needed_blocks` re-fills.
        let released = self.work.reset_above(self.sequencer.floor());
        self.budget.release(released);
        // A destructive reset supersedes the current apply generation: bump the
        // epoch so a stale `CommitRejected` for a cleared height (or a re-pushed
        // height) does not alias the new generation.
        self.apply_epoch = self.apply_epoch.saturating_add(1);
        // A destructive reset: bump the reset epoch so the reactor drops *all*
        // outstanding requests (not just those through the tip).
        self.reset_epoch = self.reset_epoch.saturating_add(1);
    }

    /// Drain the contiguous reorder prefix onto the applyQ, stamping each drained
    /// block with the current apply epoch.
    async fn release_contiguous_blocks(&mut self) {
        for drained in self.sequencer.drain_ready_into_applying(self.apply_epoch) {
            let height = drained.height;
            self.trace_body_applying(height);
            let DrainedBlock {
                height,
                hash,
                block,
                bytes,
                source_peer,
                epoch,
            } = drained;
            // Unbounded send never blocks the serial task and never drops a body;
            // the byte budget (every item is a reserved, held block) is the real
            // bound. A closed receiver (Committer gone) means shutdown — drop it.
            let _ = self.apply_tx.send(ApplyItem {
                height,
                hash,
                block,
                bytes,
                source_peer,
                epoch,
            });
        }
    }

    fn trace_body_applying(&self, height: block::Height) {
        self.trace.emit_with(BLOCK_SYNC_TABLE, |row| {
            row.insert(
                bs_trace::EVENT.to_string(),
                serde_json::Value::String(bs_trace::BLOCK_BODY_APPLYING.to_string()),
            );
            bs_insert_height(row, bs_trace::HEIGHT, height);
            bs_insert_height(row, bs_trace::BODY_DOWNLOAD_FLOOR, self.sequencer.floor());
            bs_insert_u64(
                row,
                bs_trace::APPLYING,
                self.sequencer.applying_len() as u64,
            );
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

    /// `reset_tip_conflicts_with_local_work`'s Sequencer-internal predicates, with
    /// the peer-outstanding clause supplied by the reactor.
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
        let now = Instant::now();
        self.committed_throughput.sample(now);
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
            lowest_applying_height: self.sequencer.lowest_applying_height(),
            commit_frontier_stall_seconds: now
                .saturating_duration_since(self.commit_frontier_since)
                .as_secs(),
            committed_bytes_per_sec: self.committed_throughput.bytes_per_sec(),
            committed_blocks_per_sec: self.committed_throughput.blocks_per_sec(),
        });
    }
}
