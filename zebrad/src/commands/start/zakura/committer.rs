//! The block-sync commit pump (node-wiring side of the apply seam).
//!
//! The block-sync Sequencer (in `zebra-network`) drains its contiguous reorder
//! prefix into [`ApplyItem`]s and pushes them onto the `applyQ`
//! (`mpsc::UnboundedReceiver<ApplyItem>`). The [`Committer`] lives here, in node
//! wiring, because it names the consensus verifier
//! (`zebra_consensus::Request::Commit`), which `zebra-network` must not depend on.
//!
//! It is a pump: for each item it fires `Request::Commit` **without awaiting**,
//! pushing the commit future into a [`FuturesUnordered`], and drains completions
//! concurrently. Firing the whole contiguous range concurrently is required, not an
//! optimization: the checkpoint verifier batch-verifies a range and resolves a
//! block's commit only once the entire range to the next checkpoint has been
//! submitted, so a serial "await each commit" loop would deadlock.
//!
//! The Committer never touches the byte budget — the Sequencer releases bytes on
//! durable frontier reports. Real commits use the durable-tip watch; throughput
//! probe commits report their synthetic frontier directly. On a commit failure
//! (consensus-invalid body or local apply timeout) it raises a [`CommitterReset`]
//! back to the Sequencer (via [`BlockSyncHandle::report_commit_rejected`]); within
//! one contiguous range only the lowest failing height is raised (higher siblings
//! are coalesced), and the Sequencer's per-height apply-epoch guard discards any
//! remaining stale resets.

use std::{future::Future, sync::Arc, time::Instant};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::{pin, sync::mpsc};
use tower::Service;
use tracing::debug;

use zebra_chain::block;
use zebra_network::zakura::{
    commit_state_trace as cs_trace, ApplyItem, BlockApplyClass, BlockApplyResult, BlockApplyToken,
    BlockSyncFrontiers, BlockSyncHandle, CommitRejection, CommitterReset, ZakuraTrace,
};

use super::{
    block_apply_result_label,
    block_sync_driver::{
        block_apply_class, block_apply_class_label, commit_block_sync_body_with_stall_trace,
        CommitPipelineProbe,
    },
    emit_commit_state, insert_cs_hash, insert_cs_height, insert_cs_str, insert_cs_u64,
    BlocksyncThroughputProbe,
};

/// Sink the Committer uses to report commit feedback to the Sequencer.
///
/// Production wiring uses [`BlockSyncHandle`]; tests inject a recording double.
pub trait CommitFeedbackSink: Send + Sync + 'static {
    /// Report a commit rejection back to the Sequencer.
    fn report_commit_rejected(&self, reset: CommitterReset);

    /// Report a synthetic durable frontier in throughput-probe mode.
    fn report_probe_frontier(&self, frontiers: BlockSyncFrontiers);
}

impl CommitFeedbackSink for BlockSyncHandle {
    fn report_commit_rejected(&self, reset: CommitterReset) {
        BlockSyncHandle::report_commit_rejected(self, reset);
    }

    fn report_probe_frontier(&self, frontiers: BlockSyncFrontiers) {
        BlockSyncHandle::report_durable_frontier(self, frontiers);
    }
}

/// Metadata carried alongside a fired commit so its completion can be attributed
/// to the right height/peer/generation when it resolves.
#[derive(Clone, Debug)]
struct CommitMeta {
    height: block::Height,
    source_peer: zebra_network::zakura::ZakuraPeerId,
    /// Generation the item was stamped with; echoed in a [`CommitterReset`] and
    /// used for stale-generation coalescing.
    epoch: u64,
}

/// A resolved commit: its metadata plus the verifier result.
struct CommitOutcome {
    meta: CommitMeta,
    result: BlockApplyResult,
}

/// The block-sync commit pump. Generic over the consensus verifier service.
pub struct Committer<BlockVerifier> {
    /// The applyQ: contiguous, hash-verified items from the Sequencer. Unbounded so
    /// the serial Sequencer task never blocks on a push; the byte budget bounds
    /// total in-flight memory.
    apply_rx: mpsc::UnboundedReceiver<ApplyItem>,
    /// The consensus verifier (`Request::Commit`).
    block_verifier: BlockVerifier,
    /// Sink used to report commit feedback back to the Sequencer.
    feedback_sink: Arc<dyn CommitFeedbackSink>,
    /// Boundary between the checkpoint and full verifier paths.
    max_checkpoint_height: block::Height,
    /// Fired-but-unresolved commits.
    in_flight: FuturesUnordered<BoxFuture<'static, CommitOutcome>>,
    /// Highest committed height (contiguity assertion + trace).
    committed_marker: block::Height,
    /// Highest generation a reset has already been raised for. Items at or below
    /// this are stale (superseded by a reset) and discarded. Epochs are 1-based, so
    /// the initial `0` discards nothing.
    last_reset_epoch: u64,
    /// Lowest height a reset has already been raised for *within* `last_reset_epoch`.
    /// Within one generation, only a strictly-lower failure extends the rollback (a
    /// higher one would be a no-op at the Sequencer's per-height guard); a fresh
    /// generation resets this to `Height::MAX`. This makes lowest-reset-wins hold
    /// even when several blocks of one contiguous range fail (e.g. an invalid full
    /// block and its children).
    last_reset_height: block::Height,
    /// Monotonic per-commit id used for trace correlation (replaces the deleted
    /// verifier apply token).
    commit_seq: BlockApplyToken,
    /// Lock-free view of the pipeline (committed marker / submission high-water /
    /// in-flight) that each fired commit reads at its stall deadline to attribute the
    /// `commit_stalled` trace row. Diagnostic only — never gates a commit.
    commit_probe: CommitPipelineProbe,
    trace: ZakuraTrace,
    /// Debug-only synthetic-commit probe; when set, commits are skipped.
    throughput_probe: Option<BlocksyncThroughputProbe>,
}

impl<BlockVerifier> Committer<BlockVerifier>
where
    BlockVerifier:
        Service<zebra_consensus::Request, Response = block::Hash> + Clone + Send + Sync + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
{
    /// Construct a Committer for callers that do not use the debug throughput probe.
    pub fn new_without_probe(
        apply_rx: mpsc::UnboundedReceiver<ApplyItem>,
        block_verifier: BlockVerifier,
        feedback_sink: Arc<dyn CommitFeedbackSink>,
        max_checkpoint_height: block::Height,
        trace: ZakuraTrace,
    ) -> Self {
        Self::new(
            apply_rx,
            block_verifier,
            feedback_sink,
            max_checkpoint_height,
            trace,
            None,
        )
    }

    pub(crate) fn new(
        apply_rx: mpsc::UnboundedReceiver<ApplyItem>,
        block_verifier: BlockVerifier,
        feedback_sink: Arc<dyn CommitFeedbackSink>,
        max_checkpoint_height: block::Height,
        trace: ZakuraTrace,
        throughput_probe: Option<BlocksyncThroughputProbe>,
    ) -> Self {
        Self {
            apply_rx,
            block_verifier,
            feedback_sink,
            max_checkpoint_height,
            in_flight: FuturesUnordered::new(),
            committed_marker: block::Height::MIN,
            last_reset_epoch: 0,
            last_reset_height: block::Height::MAX,
            commit_seq: 0,
            commit_probe: CommitPipelineProbe::default(),
            trace,
            throughput_probe,
        }
    }

    /// Drain the applyQ, firing commits concurrently, until the queue closes (and
    /// in-flight commits drain) or `shutdown` fires (then drain in-flight and exit).
    /// Returns the highest committed height observed.
    pub async fn run(mut self, shutdown: impl Future<Output = ()> + Send) -> block::Height {
        pin!(shutdown);
        let mut apply_open = true;
        let mut shutting_down = false;
        loop {
            if (!apply_open || shutting_down) && self.in_flight.is_empty() {
                break;
            }
            // Plain select: the two genuine event sources (a new item; a commit
            // completing) do not mutually starve, so random fairness suffices.
            tokio::select! {
                _ = &mut shutdown, if !shutting_down => {
                    shutting_down = true;
                }
                item = self.apply_rx.recv(), if apply_open && !shutting_down => {
                    match item {
                        Some(item) => self.on_item(item),
                        None => apply_open = false,
                    }
                }
                Some(done) = self.in_flight.next(), if !self.in_flight.is_empty() => {
                    self.on_commit_done(done);
                }
            }
        }
        self.committed_marker
    }

    /// Fire one item's commit into `in_flight` without awaiting. Stale items (from
    /// a superseded generation) are discarded.
    fn on_item(&mut self, item: ApplyItem) {
        if item.epoch <= self.last_reset_epoch {
            debug!(
                height = ?item.height,
                epoch = item.epoch,
                last_reset_epoch = self.last_reset_epoch,
                "Zakura committer discarded stale applyQ item"
            );
            return;
        }

        let ApplyItem {
            height,
            hash,
            block,
            bytes: _,
            source_peer,
            epoch,
        } = item;
        let class = block_apply_class(block.as_ref(), self.max_checkpoint_height);
        let commit_seq = self.next_commit_seq();
        let meta = CommitMeta {
            height,
            source_peer,
            epoch,
        };
        self.commit_probe.note_fired(height);

        let verifier = self.block_verifier.clone();
        let trace = self.trace.clone();
        let commit_probe = self.commit_probe.clone();
        let throughput_probe = self.throughput_probe.clone();
        self.in_flight.push(
            async move {
                let result = commit_one(
                    verifier,
                    block,
                    class,
                    &trace,
                    &commit_probe,
                    commit_seq,
                    height,
                    hash,
                    throughput_probe,
                )
                .await;
                CommitOutcome { meta, result }
            }
            .boxed(),
        );
    }

    /// Handle a resolved commit. Success advances the committed marker; probe-mode
    /// successes also report the synthetic frontier so the Sequencer can release
    /// held bytes. Failures raise a [`CommitterReset`] back to the Sequencer.
    fn on_commit_done(&mut self, done: CommitOutcome) {
        let CommitOutcome { meta, result } = done;
        match result {
            BlockApplyResult::Committed | BlockApplyResult::Duplicate => {
                self.committed_marker = self.committed_marker.max(meta.height);
                self.commit_probe.note_resolved(Some(meta.height));
                if let Some(probe) = self.throughput_probe.as_ref() {
                    self.feedback_sink
                        .report_probe_frontier(probe.synthetic_frontier());
                }
            }
            BlockApplyResult::Rejected | BlockApplyResult::TimedOut => {
                self.commit_probe.note_resolved(None);
                self.on_commit_error(meta, result);
            }
        }
    }

    /// Raise a [`CommitterReset`] for a failed commit, coalescing failures that a
    /// reset already covers.
    ///
    /// Two failures the Sequencer's per-height guard would no-op are dropped here so
    /// we do not flood it: a failure from a superseded generation (`epoch <
    /// last_reset_epoch`), and a same-generation failure that is not strictly lower
    /// than one already raised (`height >= last_reset_height`). A strictly-lower
    /// same-generation failure *is* raised, so several invalid blocks in one
    /// contiguous range each roll the floor back lowest-wins.
    fn on_commit_error(&mut self, meta: CommitMeta, result: BlockApplyResult) {
        // A failure from a superseded generation is already covered by the reset
        // that bumped the generation; the Sequencer's per-height guard would
        // no-op it anyway.
        if meta.epoch < self.last_reset_epoch {
            return;
        }
        // Entering a fresh generation: re-key the within-generation lowest
        // tracker to `Height::MAX` (the field's documented invariant) *before*
        // the height test below. Doing this explicitly — rather than relying on
        // the `==`-epoch guard clause to be skipped — keeps lowest-reset-wins
        // correct even if these guards are later reordered: the first failure of
        // a new generation always raises, and only a strictly-lower one extends
        // the rollback.
        if meta.epoch > self.last_reset_epoch {
            self.last_reset_epoch = meta.epoch;
            self.last_reset_height = block::Height::MAX;
        }
        // Within one generation, only a strictly-lower failure extends the
        // rollback; a higher one is a no-op at the Sequencer's per-height guard.
        if meta.height >= self.last_reset_height {
            return;
        }
        self.last_reset_height = meta.height;
        let rejection = if matches!(result, BlockApplyResult::Rejected) {
            CommitRejection::Invalid
        } else {
            CommitRejection::TimedOut
        };
        self.feedback_sink.report_commit_rejected(CommitterReset {
            height: meta.height,
            epoch: meta.epoch,
            source_peer: meta.source_peer,
            rejection,
        });
    }

    fn next_commit_seq(&mut self) -> BlockApplyToken {
        let seq = self.commit_seq;
        self.commit_seq = self.commit_seq.checked_add(1).unwrap_or(1);
        seq
    }
}

/// Commit one body through the verifier (or the synthetic probe), emitting the
/// commit-state trace rows. Returns the apply result; never reads state frontiers
/// — the durable-tip watch drives the verified-tip advance and byte release.
#[allow(clippy::too_many_arguments)]
async fn commit_one<BlockVerifier>(
    verifier: BlockVerifier,
    block: Arc<block::Block>,
    class: BlockApplyClass,
    trace: &ZakuraTrace,
    commit_probe: &CommitPipelineProbe,
    commit_seq: BlockApplyToken,
    height: block::Height,
    expected_hash: block::Hash,
    throughput_probe: Option<BlocksyncThroughputProbe>,
) -> BlockApplyResult
where
    BlockVerifier:
        Service<zebra_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
{
    emit_commit_state(trace, cs_trace::COMMIT_START, "committer", |row| {
        insert_cs_u64(row, cs_trace::APPLY_TOKEN, commit_seq);
        insert_cs_str(row, cs_trace::APPLY_CLASS, block_apply_class_label(class));
        insert_cs_height(row, cs_trace::HEIGHT, height);
        insert_cs_hash(row, cs_trace::HASH, expected_hash);
    });
    let started = Instant::now();
    let result = match throughput_probe.as_ref() {
        // Throughput-probe mode (debug only): skip consensus verify+commit.
        Some(probe) => probe.apply_block(block.as_ref()),
        None => {
            commit_block_sync_body_with_stall_trace(
                verifier,
                block,
                class,
                trace,
                commit_probe,
                commit_seq,
                height,
                expected_hash,
            )
            .await
        }
    };
    emit_commit_state(trace, cs_trace::COMMIT_FINISH, "committer", |row| {
        insert_cs_u64(row, cs_trace::APPLY_TOKEN, commit_seq);
        insert_cs_str(row, cs_trace::APPLY_CLASS, block_apply_class_label(class));
        insert_cs_height(row, cs_trace::HEIGHT, height);
        insert_cs_hash(row, cs_trace::HASH, expected_hash);
        insert_cs_str(row, cs_trace::RESULT, block_apply_result_label(result));
        insert_cs_u64(
            row,
            cs_trace::ELAPSED_MS,
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        );
    });
    result
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    //! Apply-half property tests (the committer's share of the A-list in
    //! `docs/plans/glue_refactor/`). A2 (byte-release-on-durable), A7 (the
    //! budget-floor config invariant), and A8 (in-flight bounded by the budget) are
    //! Sequencer/config-side properties and live with those changes, not here.
    //!
    //! A3 (zero reads on the commit hot path) is satisfied **by construction**: the
    //! [`Committer`] holds no `ReadState`/chain-tip handle and never queries state —
    //! the durable-tip watch drives the verified-tip advance. Reintroducing a read
    //! would require a new field, which this module's mere compilation forbids.

    use std::{future::Future, sync::Mutex as StdMutex};

    use tokio::sync::{mpsc, oneshot, watch, Barrier};
    use tower::Service;

    use zebra_chain::serialization::ZcashDeserializeInto;
    use zebra_network::zakura::ZakuraPeerId;
    use zebra_test::vectors::{
        BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES, BLOCK_MAINNET_3_BYTES, BLOCK_MAINNET_4_BYTES,
    };

    use super::*;

    type CommitLog = Arc<StdMutex<Vec<block::Height>>>;

    /// Records commit feedback the committer raises, standing in for `BlockSyncHandle`.
    #[derive(Default)]
    struct RecordingSink {
        resets: StdMutex<Vec<CommitterReset>>,
        probe_frontiers: StdMutex<Vec<BlockSyncFrontiers>>,
    }

    impl CommitFeedbackSink for RecordingSink {
        fn report_commit_rejected(&self, reset: CommitterReset) {
            self.resets.lock().unwrap().push(reset);
        }

        fn report_probe_frontier(&self, frontiers: BlockSyncFrontiers) {
            self.probe_frontiers.lock().unwrap().push(frontiers);
        }
    }

    impl RecordingSink {
        fn resets(&self) -> Vec<CommitterReset> {
            self.resets.lock().unwrap().clone()
        }

        fn probe_frontiers(&self) -> Vec<BlockSyncFrontiers> {
            self.probe_frontiers.lock().unwrap().clone()
        }
    }

    fn peer() -> ZakuraPeerId {
        ZakuraPeerId::new(vec![7u8; 32]).expect("valid test peer id")
    }

    fn block_from(bytes: &[u8]) -> Arc<block::Block> {
        Arc::new(bytes.zcash_deserialize_into().expect("block vector parses"))
    }

    fn apply_item(block: Arc<block::Block>, epoch: u64) -> ApplyItem {
        let height = block
            .coinbase_height()
            .expect("test block has a coinbase height");
        let hash = block.hash();
        ApplyItem {
            height,
            hash,
            block,
            bytes: 1,
            source_peer: peer(),
            epoch,
        }
    }

    fn req_block(req: zebra_consensus::Request) -> Arc<block::Block> {
        match req {
            zebra_consensus::Request::Commit(block) => block,
            other => panic!("committer issued an unexpected verifier request: {other:?}"),
        }
    }

    /// A verifier that commits every body immediately, logging committed heights.
    fn ok_verifier(
        log: CommitLog,
    ) -> impl Service<
        zebra_consensus::Request,
        Response = block::Hash,
        Error = zebra_consensus::BoxError,
        Future: Send + 'static,
    > + Clone {
        tower::service_fn(move |req| {
            let log = log.clone();
            async move {
                let block = req_block(req);
                log.lock().unwrap().push(block.coinbase_height().unwrap());
                Ok::<_, zebra_consensus::BoxError>(block.hash())
            }
        })
    }

    /// A verifier that withholds every commit until a full range of `n` has been
    /// submitted, then resolves them all — the batch semantics of the checkpoint
    /// verifier. A serial "await each commit" committer deadlocks against it.
    fn batching_verifier(
        barrier: Arc<Barrier>,
        log: CommitLog,
    ) -> impl Service<
        zebra_consensus::Request,
        Response = block::Hash,
        Error = zebra_consensus::BoxError,
        Future: Send + 'static,
    > + Clone {
        tower::service_fn(move |req| {
            let barrier = barrier.clone();
            let log = log.clone();
            async move {
                let block = req_block(req);
                barrier.wait().await;
                log.lock().unwrap().push(block.coinbase_height().unwrap());
                Ok::<_, zebra_consensus::BoxError>(block.hash())
            }
        })
    }

    /// A verifier that rejects every body (a non-duplicate error → `Rejected`).
    fn reject_verifier() -> impl Service<
        zebra_consensus::Request,
        Response = block::Hash,
        Error = zebra_consensus::BoxError,
        Future: Send + 'static,
    > + Clone {
        tower::service_fn(move |req| async move {
            let _ = req_block(req);
            Err::<block::Hash, zebra_consensus::BoxError>("rejected by test verifier".into())
        })
    }

    /// A verifier that signals entry (so the test can confirm a body is in flight)
    /// then blocks until `release` flips true.
    fn gated_verifier(
        entry: Arc<Barrier>,
        release: watch::Receiver<bool>,
        log: CommitLog,
    ) -> impl Service<
        zebra_consensus::Request,
        Response = block::Hash,
        Error = zebra_consensus::BoxError,
        Future: Send + 'static,
    > + Clone {
        tower::service_fn(move |req| {
            let entry = entry.clone();
            let mut release = release.clone();
            let log = log.clone();
            async move {
                let block = req_block(req);
                entry.wait().await;
                while !*release.borrow() {
                    let _ = release.changed().await;
                }
                log.lock().unwrap().push(block.coinbase_height().unwrap());
                Ok::<_, zebra_consensus::BoxError>(block.hash())
            }
        })
    }

    fn committer<V>(
        rx: mpsc::UnboundedReceiver<ApplyItem>,
        verifier: V,
        sink: Arc<RecordingSink>,
        max_checkpoint_height: block::Height,
    ) -> Committer<V>
    where
        V: Service<zebra_consensus::Request, Response = block::Hash>
            + Clone
            + Send
            + Sync
            + 'static,
        V::Error: std::fmt::Debug + Send + Sync + 'static,
        V::Future: Send + 'static,
    {
        Committer::new(
            rx,
            verifier,
            sink,
            max_checkpoint_height,
            ZakuraTrace::noop(),
            None,
        )
    }

    fn never() -> impl Future<Output = ()> + Send {
        std::future::pending()
    }

    fn sorted(log: &CommitLog) -> Vec<block::Height> {
        let mut heights = log.lock().unwrap().clone();
        heights.sort();
        heights
    }

    /// Happy path: a contiguous range commits in full and raises no reset.
    #[tokio::test]
    async fn commits_a_contiguous_range() {
        let log: CommitLog = Default::default();
        let sink = Arc::new(RecordingSink::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let committer = committer(
            rx,
            ok_verifier(log.clone()),
            sink.clone(),
            block::Height(100),
        );
        let handle = tokio::spawn(committer.run(never()));

        for bytes in [
            &BLOCK_MAINNET_1_BYTES[..],
            &BLOCK_MAINNET_2_BYTES[..],
            &BLOCK_MAINNET_3_BYTES[..],
        ] {
            tx.send(apply_item(block_from(bytes), 1)).unwrap();
        }
        drop(tx);

        let marker = handle.await.unwrap();
        assert_eq!(marker, block::Height(3));
        assert_eq!(
            sorted(&log),
            vec![block::Height(1), block::Height(2), block::Height(3)]
        );
        assert!(sink.resets().is_empty());
    }

    #[tokio::test]
    async fn throughput_probe_commit_reports_synthetic_frontier() {
        let log: CommitLog = Default::default();
        let sink = Arc::new(RecordingSink::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let initial_frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: zebra_chain::parameters::Network::Mainnet.genesis_hash(),
        };
        let (probe, _completion_rx) =
            BlocksyncThroughputProbe::new(initial_frontiers, block::Height(1));
        let committer = Committer::new(
            rx,
            ok_verifier(log.clone()),
            sink.clone(),
            block::Height(100),
            ZakuraTrace::noop(),
            Some(probe),
        );
        let handle = tokio::spawn(committer.run(never()));
        let block = block_from(&BLOCK_MAINNET_1_BYTES);
        let expected = BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: block.hash(),
        };

        tx.send(apply_item(block, 1)).unwrap();
        drop(tx);

        let marker = handle.await.unwrap();
        assert_eq!(marker, block::Height(1));
        assert!(sorted(&log).is_empty(), "probe mode skips the verifier");
        assert_eq!(sink.probe_frontiers(), vec![expected]);
        assert!(sink.resets().is_empty());
    }

    /// A1: the committer fires the whole range concurrently, so a verifier that
    /// only resolves once the entire range is submitted still drives every block to
    /// committed. A serial "await each commit" implementation hangs this test.
    #[tokio::test]
    async fn fires_whole_range_concurrently_for_batch_resolution() {
        let log: CommitLog = Default::default();
        let sink = Arc::new(RecordingSink::default());
        let barrier = Arc::new(Barrier::new(3));
        let (tx, rx) = mpsc::unbounded_channel();
        let committer = committer(
            rx,
            batching_verifier(barrier, log.clone()),
            sink.clone(),
            block::Height(100),
        );
        let handle = tokio::spawn(committer.run(never()));

        for bytes in [
            &BLOCK_MAINNET_1_BYTES[..],
            &BLOCK_MAINNET_2_BYTES[..],
            &BLOCK_MAINNET_3_BYTES[..],
        ] {
            tx.send(apply_item(block_from(bytes), 1)).unwrap();
        }
        drop(tx);

        let marker = handle.await.unwrap();
        assert_eq!(marker, block::Height(3));
        assert_eq!(
            sorted(&log),
            vec![block::Height(1), block::Height(2), block::Height(3)]
        );
    }

    /// A4: a rejected body raises exactly one reset, attributed to the delivering
    /// peer and carrying the item's height/epoch, and commits nothing.
    #[tokio::test]
    async fn rejection_raises_one_attributed_reset() {
        let sink = Arc::new(RecordingSink::default());
        let (tx, rx) = mpsc::unbounded_channel();
        // max_checkpoint_height 0 ⇒ height-1 body takes the full (rejectable) path.
        let committer = committer(rx, reject_verifier(), sink.clone(), block::Height(0));
        let handle = tokio::spawn(committer.run(never()));

        tx.send(apply_item(block_from(&BLOCK_MAINNET_1_BYTES), 7))
            .unwrap();
        drop(tx);

        let marker = handle.await.unwrap();
        assert_eq!(marker, block::Height::MIN);
        let resets = sink.resets();
        assert_eq!(resets.len(), 1);
        assert_eq!(resets[0].height, block::Height(1));
        assert_eq!(resets[0].epoch, 7);
        assert_eq!(resets[0].rejection, CommitRejection::Invalid);
        assert_eq!(resets[0].source_peer, peer());
    }

    /// A4 (coalescing contract): within one generation a higher failure raised
    /// first never suppresses a strictly-lower one (lowest-reset-wins), a higher
    /// failure after a lower one is coalesced, and a fresh generation re-keys the
    /// tracker so its first failure always raises regardless of the previous
    /// generation's lowest height.
    #[test]
    fn coalesces_resets_lowest_wins_within_a_generation() {
        let log: CommitLog = Default::default();
        let sink = Arc::new(RecordingSink::default());
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut committer = committer(rx, ok_verifier(log), sink.clone(), block::Height(0));

        let fail = |committer: &mut Committer<_>, height: u32, epoch: u64| {
            committer.on_commit_error(
                CommitMeta {
                    height: block::Height(height),
                    source_peer: peer(),
                    epoch,
                },
                BlockApplyResult::Rejected,
            );
        };

        // Higher height fails first, then a strictly-lower one in the same gen.
        fail(&mut committer, 20, 1);
        fail(&mut committer, 10, 1);
        // A higher one after the low is coalesced (no-op at the Sequencer guard).
        fail(&mut committer, 15, 1);
        // A fresh generation re-keys: its first failure raises even though its
        // height (30) is above the previous generation's lowest reset (10).
        fail(&mut committer, 30, 2);

        let raised: Vec<_> = sink
            .resets()
            .iter()
            .map(|reset| (reset.height, reset.epoch))
            .collect();
        assert_eq!(
            raised,
            vec![
                (block::Height(20), 1),
                (block::Height(10), 1),
                (block::Height(30), 2),
            ],
            "higher-first then lower both raise; mid-height coalesced; fresh gen re-keys"
        );
    }

    /// A5: items from a superseded generation (epoch ≤ last reset) are discarded,
    /// never fired; a fresh generation is fired.
    #[test]
    fn discards_items_from_a_superseded_generation() {
        let log: CommitLog = Default::default();
        let sink = Arc::new(RecordingSink::default());
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut committer = committer(rx, ok_verifier(log), sink, block::Height(100));
        committer.last_reset_epoch = 5;

        committer.on_item(apply_item(block_from(&BLOCK_MAINNET_1_BYTES), 5));
        assert!(committer.in_flight.is_empty(), "equal epoch is stale");
        committer.on_item(apply_item(block_from(&BLOCK_MAINNET_2_BYTES), 3));
        assert!(committer.in_flight.is_empty(), "older epoch is stale");
        committer.on_item(apply_item(block_from(&BLOCK_MAINNET_3_BYTES), 6));
        assert_eq!(committer.in_flight.len(), 1, "newer epoch is fired");
    }

    /// A6: the committer commits bodies on both sides of the checkpoint boundary.
    #[tokio::test]
    async fn commits_across_the_checkpoint_boundary() {
        let log: CommitLog = Default::default();
        let sink = Arc::new(RecordingSink::default());
        let (tx, rx) = mpsc::unbounded_channel();
        // Boundary at height 2: blocks 1,2 are checkpoint; 3,4 are full.
        let committer = committer(rx, ok_verifier(log.clone()), sink.clone(), block::Height(2));
        let handle = tokio::spawn(committer.run(never()));

        for bytes in [
            &BLOCK_MAINNET_1_BYTES[..],
            &BLOCK_MAINNET_2_BYTES[..],
            &BLOCK_MAINNET_3_BYTES[..],
            &BLOCK_MAINNET_4_BYTES[..],
        ] {
            tx.send(apply_item(block_from(bytes), 1)).unwrap();
        }
        drop(tx);

        let marker = handle.await.unwrap();
        assert_eq!(marker, block::Height(4));
        assert_eq!(
            sorted(&log),
            vec![
                block::Height(1),
                block::Height(2),
                block::Height(3),
                block::Height(4)
            ]
        );
        assert!(sink.resets().is_empty());
    }

    /// A9: on shutdown the committer stops accepting items but drains the commits
    /// already in flight before exiting — none is silently dropped.
    #[tokio::test]
    async fn drains_in_flight_commits_on_shutdown() {
        let log: CommitLog = Default::default();
        let sink = Arc::new(RecordingSink::default());
        // entry barrier: 3 in-flight commits + the test, so the test can confirm
        // all three are fired before it signals shutdown.
        let entry = Arc::new(Barrier::new(4));
        let (release_tx, release_rx) = watch::channel(false);
        let (tx, rx) = mpsc::unbounded_channel();
        let committer = committer(
            rx,
            gated_verifier(entry.clone(), release_rx, log.clone()),
            sink.clone(),
            block::Height(100),
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(committer.run(async move {
            let _ = shutdown_rx.await;
        }));

        for bytes in [
            &BLOCK_MAINNET_1_BYTES[..],
            &BLOCK_MAINNET_2_BYTES[..],
            &BLOCK_MAINNET_3_BYTES[..],
        ] {
            tx.send(apply_item(block_from(bytes), 1)).unwrap();
        }
        // All three commits are now in flight (entered the verifier).
        entry.wait().await;
        drop(tx);
        // Shut down *before* releasing the gated commits.
        let _ = shutdown_tx.send(());
        release_tx.send(true).unwrap();

        let marker = handle.await.unwrap();
        assert_eq!(marker, block::Height(3), "in-flight commits drained");
        assert_eq!(
            sorted(&log),
            vec![block::Height(1), block::Height(2), block::Height(3)]
        );
    }
}
