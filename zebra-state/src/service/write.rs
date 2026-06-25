//! Writing blocks to the finalized and non-finalized states.

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use indexmap::IndexMap;
use tokio::sync::{
    mpsc::{error::TryRecvError, UnboundedReceiver, UnboundedSender},
    oneshot, watch,
};

use tracing::Span;
use zebra_chain::block::{self, Height};

use zebra_chain::parallel::{
    commitment_aux::BlockCommitmentRoots,
    tree::{BlockNotePrecompute, NoteCommitmentTrees},
};

use crate::{
    constants::MAX_BLOCK_REORG_HEIGHT,
    error::CommitHeaderRangeError,
    service::{
        check,
        finalized_state::{spawn_note_precompute, FinalizedState, ZebraDb},
        non_finalized_state::NonFinalizedState,
        queued_blocks::{QueuedCheckpointVerified, QueuedSemanticallyVerified},
        ChainTipBlock, ChainTipSender, InvalidateError, ReconsiderError,
    },
    SemanticallyVerifiedBlock, ValidateContextError,
};

// These types are used in doc links
#[allow(unused_imports)]
use crate::service::{
    chain_tip::{ChainTipChange, LatestChainTip},
    non_finalized_state::Chain,
};

/// A speculatively-started note-commitment precompute for an upcoming finalized
/// block: the block hash it was started for, the channel to receive the result on,
/// and a flag to cancel it if the block is no longer going to be committed.
type PendingPrecompute = (
    block::Hash,
    crossbeam_channel::Receiver<BlockNotePrecompute>,
    Arc<AtomicBool>,
);

/// Delay between retryable VCT root-miss commit attempts while the peer cache refills.
const VCT_ROOT_RETRY_WAIT: Duration = Duration::from_millis(500);

/// Delay between retryable VCT await-successor commit attempts. Shorter than
/// [`VCT_ROOT_RETRY_WAIT`]: the root is already cached and only the next block needs to be
/// downloaded into the look-ahead, so a tighter poll keeps the one-block commit lag small.
const VCT_AWAIT_SUCCESSOR_WAIT: Duration = Duration::from_millis(20);

/// How long a single checkpoint height may stay stuck on a retryable VCT root stall before
/// the committer escalates to an error-level log and a `state.vct.root.stalled.height` gauge.
/// Transient waits (a successor still downloading, a root still in flight) clear well within
/// this; staying stuck past it means no peer can serve a root the frozen frontier requires,
/// and — by design — the committer will not recompute against the stale frontier, so the node
/// cannot advance until a peer supplies it. Surfacing that loudly is the operator's only signal.
const VCT_ROOT_STALL_WARN_AFTER: Duration = Duration::from_secs(30);

/// Cancels and drops a pending look-ahead precompute, if any.
///
/// Tripping the flag tells the spawned task (started before the current block
/// committed) to stop instead of hashing a block that will not be committed.
fn cancel_pending_precompute(pending: &mut Option<PendingPrecompute>) {
    if let Some((_hash, _rx, cancel)) = pending.take() {
        cancel.store(true, Ordering::Relaxed);
    }
}

/// The maximum size of the parent error map.
///
/// We allow enough space for multiple concurrent chain forks with errors.
const PARENT_ERROR_MAP_LIMIT: usize = MAX_BLOCK_REORG_HEIGHT as usize * 2;

/// Run contextual validation on the prepared block and add it to the
/// non-finalized state if it is contextually valid.
#[tracing::instrument(
    level = "debug",
    skip(finalized_state, non_finalized_state, prepared),
    fields(
        height = ?prepared.height,
        hash = %prepared.hash,
        chains = non_finalized_state.chain_count()
    )
)]
pub(crate) fn validate_and_commit_non_finalized(
    finalized_state: &ZebraDb,
    non_finalized_state: &mut NonFinalizedState,
    prepared: SemanticallyVerifiedBlock,
) -> Result<(), ValidateContextError> {
    check::initial_contextual_validity(finalized_state, non_finalized_state, &prepared)?;
    let parent_hash = prepared.block.header.previous_block_hash;

    if finalized_state.finalized_tip_hash() == parent_hash {
        non_finalized_state.commit_new_chain(prepared, finalized_state)?;
    } else {
        non_finalized_state.commit_block(prepared, finalized_state)?;
    }

    Ok(())
}

/// Update the [`LatestChainTip`], [`ChainTipChange`], and `non_finalized_state_sender`
/// channels with the latest non-finalized [`ChainTipBlock`] and
/// [`Chain`].
///
/// `last_zebra_mined_log_height` is used to rate-limit logging.
///
/// If `backup_dir_path` is `Some`, the non-finalized state is written to the backup
/// directory before updating the channels.
///
/// Returns the latest non-finalized chain tip height.
///
/// # Panics
///
/// If the `non_finalized_state` is empty.
#[instrument(
    level = "debug",
    skip(
        non_finalized_state,
        chain_tip_sender,
        non_finalized_state_sender,
        backup_dir_path,
    ),
    fields(chains = non_finalized_state.chain_count())
)]
fn update_latest_chain_channels(
    non_finalized_state: &NonFinalizedState,
    chain_tip_sender: &mut ChainTipSender,
    non_finalized_state_sender: &watch::Sender<NonFinalizedState>,
    backup_dir_path: Option<&Path>,
) -> block::Height {
    let best_chain = non_finalized_state.best_chain().expect("unexpected empty non-finalized state: must commit at least one block before updating channels");

    let tip_block = best_chain
        .tip_block()
        .expect("unexpected empty chain: must commit at least one block before updating channels")
        .clone();
    let tip_block = ChainTipBlock::from(tip_block);

    let tip_block_height = tip_block.height;

    if let Some(backup_dir_path) = backup_dir_path {
        non_finalized_state.write_to_backup(backup_dir_path);
    }

    // If the final receiver was just dropped, ignore the error.
    let _ = non_finalized_state_sender.send(non_finalized_state.clone());

    chain_tip_sender.set_best_non_finalized_tip(tip_block);

    tip_block_height
}

fn commit_header_range(
    finalized_state: &FinalizedState,
    anchor: block::Hash,
    headers: Vec<Arc<block::Header>>,
    body_sizes: Vec<u32>,
    tree_aux_roots: Vec<BlockCommitmentRoots>,
    rsp_tx: oneshot::Sender<Result<block::Hash, CommitHeaderRangeError>>,
) {
    let mut batch = crate::service::finalized_state::DiskWriteBatch::new();
    let result = batch
        .prepare_header_range_batch_with_roots(
            &finalized_state.db,
            anchor,
            &headers,
            &body_sizes,
            &tree_aux_roots,
        )
        .and_then(|hash| {
            finalized_state
                .db
                .write_batch(batch)
                .map(|()| hash)
                .map_err(|error| {
                    tracing::error!(?error, "failed to write validated header range");

                    CommitHeaderRangeError::StorageWriteError {
                        error: error.to_string(),
                    }
                })
        });

    let _ = rsp_tx.send(result);
}

/// A worker task that reads, validates, and writes blocks to the
/// `finalized_state` or `non_finalized_state`.
struct WriteBlockWorkerTask {
    finalized_block_write_receiver: UnboundedReceiver<QueuedCheckpointVerified>,
    non_finalized_block_write_receiver: UnboundedReceiver<NonFinalizedWriteMessage>,
    finalized_state: FinalizedState,
    non_finalized_state: NonFinalizedState,
    seed_zakura_header_from_best_chain_commits: bool,
    invalid_block_reset_sender: UnboundedSender<block::Hash>,
    /// Signals the [`crate::service::StateService`] that a non-finalized block was rejected by
    /// the write task, so its hash should be removed from
    /// `non_finalized_block_write_sent_hashes`.
    ///
    /// Without this, a rejected same-hash block locks out a later honest
    /// re-delivery of a block at the same hash as a "duplicate" until restart
    /// or reorg.
    non_finalized_rejected_sender: UnboundedSender<block::Hash>,
    chain_tip_sender: ChainTipSender,
    non_finalized_state_sender: watch::Sender<NonFinalizedState>,
    /// If `Some`, the non-finalized state is written to this backup directory
    /// synchronously before each channel update, instead of via the async backup task.
    backup_dir_path: Option<PathBuf>,
}

/// The message type for the non-finalized block write task channel.
pub enum NonFinalizedWriteMessage {
    /// A newly downloaded and semantically verified block prepared for
    /// contextual validation and insertion into the non-finalized state.
    Commit(QueuedSemanticallyVerified),
    /// A validated header range prepared for contextual storage checks and
    /// insertion into the durable header store.
    CommitHeaderRange {
        anchor: block::Hash,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
        tree_aux_roots: Vec<BlockCommitmentRoots>,
        rsp_tx: oneshot::Sender<Result<block::Hash, CommitHeaderRangeError>>,
    },
    /// The hash of a block that should be invalidated and removed from
    /// the non-finalized state, if present.
    Invalidate {
        hash: block::Hash,
        rsp_tx: oneshot::Sender<Result<block::Hash, InvalidateError>>,
    },
    /// The hash of a block that was previously invalidated but should be
    /// reconsidered and reinserted into the non-finalized state.
    Reconsider {
        hash: block::Hash,
        rsp_tx: oneshot::Sender<Result<Vec<block::Hash>, ReconsiderError>>,
    },
}

impl From<QueuedSemanticallyVerified> for NonFinalizedWriteMessage {
    fn from(block: QueuedSemanticallyVerified) -> Self {
        NonFinalizedWriteMessage::Commit(block)
    }
}

/// A worker with a task that reads, validates, and writes blocks to the
/// `finalized_state` or `non_finalized_state` and channels for sending
/// it blocks.
#[derive(Clone, Debug)]
pub(super) struct BlockWriteSender {
    /// A channel to send blocks to the `block_write_task`,
    /// so they can be written to the [`NonFinalizedState`].
    pub non_finalized: Option<tokio::sync::mpsc::UnboundedSender<NonFinalizedWriteMessage>>,

    /// A channel to send blocks to the `block_write_task`,
    /// so they can be written to the [`FinalizedState`].
    ///
    /// This sender is dropped after the state has finished sending all the checkpointed blocks,
    /// and the lowest semantically verified block arrives.
    pub finalized: Option<tokio::sync::mpsc::UnboundedSender<QueuedCheckpointVerified>>,
}

impl BlockWriteSender {
    /// Creates a new [`BlockWriteSender`] with the given receivers and states.
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            network = %non_finalized_state.network
        )
    )]
    pub fn spawn(
        finalized_state: FinalizedState,
        non_finalized_state: NonFinalizedState,
        chain_tip_sender: ChainTipSender,
        non_finalized_state_sender: watch::Sender<NonFinalizedState>,
        should_use_finalized_block_write_sender: bool,
        backup_dir_path: Option<PathBuf>,
    ) -> (
        Self,
        tokio::sync::mpsc::UnboundedReceiver<block::Hash>,
        tokio::sync::mpsc::UnboundedReceiver<block::Hash>,
        Option<Arc<std::thread::JoinHandle<()>>>,
    ) {
        // Security: The number of blocks in these channels is limited by
        //           the syncer and inbound lookahead limits.
        let (non_finalized_block_write_sender, non_finalized_block_write_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (finalized_block_write_sender, finalized_block_write_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (invalid_block_reset_sender, invalid_block_write_reset_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (non_finalized_rejected_sender, non_finalized_rejected_receiver) =
            tokio::sync::mpsc::unbounded_channel();

        let seed_zakura_header_from_best_chain_commits = finalized_state
            .db
            .config()
            .enable_zakura_header_seed_from_committed_blocks;

        let span = Span::current();
        let task = std::thread::spawn(move || {
            span.in_scope(|| {
                WriteBlockWorkerTask {
                    finalized_block_write_receiver,
                    non_finalized_block_write_receiver,
                    finalized_state,
                    non_finalized_state,
                    seed_zakura_header_from_best_chain_commits,
                    invalid_block_reset_sender,
                    non_finalized_rejected_sender,
                    chain_tip_sender,
                    non_finalized_state_sender,
                    backup_dir_path,
                }
                .run()
            })
        });

        (
            Self {
                non_finalized: Some(non_finalized_block_write_sender),
                finalized: should_use_finalized_block_write_sender
                    .then_some(finalized_block_write_sender),
            },
            invalid_block_write_reset_receiver,
            non_finalized_rejected_receiver,
            Some(Arc::new(task)),
        )
    }
}

impl WriteBlockWorkerTask {
    /// Reads blocks from the channels, writes them to the `finalized_state` or `non_finalized_state`,
    /// sends any errors on the `invalid_block_reset_sender`, then updates the `chain_tip_sender` and
    /// `non_finalized_state_sender`.
    #[instrument(
        level = "debug",
        skip(self),
        fields(
            network = %self.non_finalized_state.network
        )
    )]
    pub fn run(mut self) {
        let Self {
            finalized_block_write_receiver,
            non_finalized_block_write_receiver,
            finalized_state,
            non_finalized_state,
            invalid_block_reset_sender,
            non_finalized_rejected_sender,
            chain_tip_sender,
            non_finalized_state_sender,
            seed_zakura_header_from_best_chain_commits,
            backup_dir_path,
        } = &mut self;

        let mut prev_finalized_note_commitment_trees: Option<NoteCommitmentTrees> = None;
        let mut deferred_non_finalized_messages = VecDeque::new();

        // One-block look-ahead so the next block's note-commitment tree hashing can
        // be precomputed off the committer (on idle cores) while the current block
        // commits. `pending_precompute` holds the receiver and cancellation flag for
        // the block started last iteration; `finalized_lookahead` buffers the peeked
        // next block. The precompute is keyed on the running tree sizes and only
        // applied if those still match at commit time, so this never affects
        // correctness, only speed.
        //
        // Because the next block's precompute is started before the current block
        // commits, a current block that fails to commit (e.g. an invalid block from
        // a peer) leaves that speculative work unwanted. Whenever this loop discards
        // a pending precompute it trips the cancellation flag via
        // [`cancel_pending_precompute`], so the spawned task stops instead of hashing
        // a block that will never be committed.
        let mut pending_precompute: Option<PendingPrecompute> = None;
        let mut finalized_lookahead: VecDeque<QueuedCheckpointVerified> = VecDeque::new();
        let mut retry_finalized_block: Option<QueuedCheckpointVerified> = None;

        // Tracks how long the committer has been stuck retrying a single VCT root stall, so a
        // genuine stall (no peer can serve a frozen-frontier height) escalates to a loud,
        // observable signal while a transient wait stays quiet. `(height, first-seen)`.
        let mut vct_root_stall: Option<(Height, Instant)> = None;
        let mut vct_root_stall_logged = false;

        // Write all the finalized blocks sent by the state,
        // until the state closes the finalized block channel's sender.
        loop {
            match non_finalized_block_write_receiver.try_recv() {
                Ok(NonFinalizedWriteMessage::CommitHeaderRange {
                    anchor,
                    headers,
                    body_sizes,
                    tree_aux_roots,
                    rsp_tx,
                }) => {
                    commit_header_range(
                        finalized_state,
                        anchor,
                        headers,
                        body_sizes,
                        tree_aux_roots,
                        rsp_tx,
                    );
                    continue;
                }
                Ok(msg) => deferred_non_finalized_messages.push_back(msg),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {}
            }

            let ordered_block = match retry_finalized_block
                .take()
                .or_else(|| finalized_lookahead.pop_front())
            {
                Some(block) => block,
                None => match finalized_block_write_receiver.try_recv() {
                    Ok(block) => block,
                    Err(TryRecvError::Empty) => {
                        std::thread::park_timeout(Duration::from_millis(10));
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => break,
                },
            };

            // TODO: split these checks into separate functions

            if invalid_block_reset_sender.is_closed() {
                info!("StateService closed the block reset channel. Is Zebra shutting down?");
                return;
            }

            // Discard any children of invalid blocks in the channel
            //
            // `commit_finalized()` requires blocks in height order.
            // So if there has been a block commit error,
            // we need to drop all the descendants of that block,
            // until we receive a block at the required next height.
            let next_valid_height = finalized_state
                .db
                .finalized_tip_height()
                .map(|height| (height + 1).expect("committed heights are valid"))
                .unwrap_or(Height(0));

            if ordered_block.0.height != next_valid_height {
                debug!(
                    ?next_valid_height,
                    invalid_height = ?ordered_block.0.height,
                    invalid_hash = ?ordered_block.0.hash,
                    "got a block that was the wrong height. \
                     Assuming a parent block failed, and dropping this block",
                );

                // The pipeline is broken; cancel and drop any look-ahead so the next
                // precompute re-seeds from the real tip (a stale precompute would
                // only fall back anyway, but cancelling stops the wasted hashing).
                cancel_pending_precompute(&mut pending_precompute);
                finalized_lookahead.clear();
                finalized_state.clear_vct_prevalidated_next();

                // We don't want to send a reset here, because it could overwrite a valid sent hash
                std::mem::drop(ordered_block);
                continue;
            }

            // Peek the next block and start its precompute, so the heavy hashing
            // overlaps this block's commit. Its start sizes are the current tree
            // sizes plus this block's note counts (the sizes after this block).
            if finalized_lookahead.is_empty() {
                if let Ok(next) = finalized_block_write_receiver.try_recv() {
                    finalized_lookahead.push_back(next);
                }
            }

            // A non-handoff VCT fast block's supplied roots are authenticated by
            // its successor's header. If the successor is not buffered yet, keep
            // this block local and wait instead of surfacing a checkpoint commit
            // error through the invalid-block reset path.
            if finalized_lookahead.is_empty()
                && finalized_state.vct_fast_needs_successor(ordered_block.0.height)
            {
                tracing::trace!(
                    height = ?ordered_block.0.height,
                    hash = ?ordered_block.0.hash,
                    "VCT: deferring fast checkpoint commit until successor is buffered"
                );
                retry_finalized_block = Some(ordered_block);
                std::thread::park_timeout(Duration::from_millis(10));
                continue;
            }

            // Use the precompute for this block if we started it last iteration and
            // it is for this exact block; otherwise cancel it (so the spawned task
            // stops) and let the committer hash inline.
            let note_precompute = match pending_precompute.take() {
                Some((hash, rx, _cancel)) if hash == ordered_block.0.hash => rx.recv().ok(),
                Some((_hash, _rx, cancel)) => {
                    cancel.store(true, Ordering::Relaxed);
                    None
                }
                None => None,
            };

            // In verified-commitment-trees mode, the committer skips the
            // note-commitment frontier entirely, so the off-thread precompute would
            // just be discarded. Skip it only when the *next* block will actually
            // take the vct path (its roots are already supplied). A legacy-fallback block
            // (no peer roots yet, or never) still gets the precompute overlap.
            let next_block_takes_vct_path = finalized_lookahead
                .front()
                .is_some_and(|next| finalized_state.vct_fast_will_apply(next.0.height));
            if !next_block_takes_vct_path {
                if let (Some(trees), Some(next)) = (
                    prev_finalized_note_commitment_trees.as_ref(),
                    finalized_lookahead.front(),
                ) {
                    let block = &ordered_block.0.block;
                    let sapling_start =
                        trees.sapling.count() + block.sapling_note_commitments().count() as u64;
                    let orchard_start =
                        trees.orchard.count() + block.orchard_note_commitments().count() as u64;
                    let (rx, cancel) =
                        spawn_note_precompute(sapling_start, orchard_start, next.0.block.clone());
                    pending_precompute = Some((next.0.hash, rx, cancel));
                }
            }

            // The buffered successor (if any) lets the committer verify this block's
            // verified-commitment-trees fixture roots before trusting them: a block's
            // roots are only committed by the next block's header. Its auth data root
            // is already precomputed by the checkpoint verifier.
            let next_checkpoint = finalized_lookahead
                .front()
                .map(|next| (next.0.block.clone(), next.0.auth_data_root));
            let prev_note_commitment_trees = prev_finalized_note_commitment_trees.take();
            let prev_note_commitment_trees_for_retry = prev_note_commitment_trees.clone();

            let next_block_took_vct_path =
                finalized_state.vct_fast_will_apply(ordered_block.0.height);

            // Try committing the block
            match finalized_state.commit_finalized(
                ordered_block,
                prev_note_commitment_trees,
                note_precompute,
                next_checkpoint,
            ) {
                Ok((finalized, note_commitment_trees)) => {
                    // Whether this successful commit consumed header-carried
                    // tree-aux roots to skip the note-commitment frontier rebuild.
                    if next_block_took_vct_path {
                        metrics::counter!("state.vct.fast_path.hit").increment(1);
                    } else {
                        metrics::counter!("state.vct.fast_path.miss").increment(1);
                    }

                    // A successful commit clears any VCT root stall: log recovery and reset
                    // the stalled-height gauge if it had been raised.
                    if vct_root_stall.is_some() {
                        if vct_root_stall_logged {
                            info!(
                                stalled_height = ?vct_root_stall.map(|(h, _)| h),
                                "VCT: checkpoint commit recovered; the stalled height now has a verifiable supplied root"
                            );
                            metrics::gauge!("state.vct.root.stalled.height").set(0.0);
                        }
                        vct_root_stall = None;
                        vct_root_stall_logged = false;
                    }

                    let tip_block = ChainTipBlock::from(finalized);
                    prev_finalized_note_commitment_trees = Some(note_commitment_trees);
                    chain_tip_sender.set_finalized_tip(tip_block);
                }
                Err((ordered_block, error)) => {
                    // Retryable VCT root stalls (an absent/evicted root, or one not yet
                    // verifiable for lack of a buffered successor) park-and-retry the same
                    // block in place rather than resetting the queue. An absent root waits
                    // for header sync to deliver it; an await-successor stall just waits for
                    // the next block to be downloaded into the look-ahead, so it polls faster.
                    if let Some(height) = error.vct_retryable_height() {
                        metrics::counter!("state.vct.root.retry.count").increment(1);
                        let needs_refetch = error.vct_supplied_root_unavailable_height();

                        // Escalate a stall that persists on the same height past the warn
                        // threshold: a transient wait resolves in a few polls and stays
                        // quiet, but a height stuck longer means no peer can serve a root the
                        // frozen frontier requires — the node will not advance (it will not,
                        // by design, recompute against the stale frontier). Surface it loudly.
                        match vct_root_stall {
                            Some((stuck, _)) if stuck == height => {}
                            _ => {
                                vct_root_stall = Some((height, Instant::now()));
                                vct_root_stall_logged = false;
                            }
                        }
                        if !vct_root_stall_logged
                            && vct_root_stall.is_some_and(|(_, since)| {
                                since.elapsed() >= VCT_ROOT_STALL_WARN_AFTER
                            })
                        {
                            tracing::error!(
                                ?height,
                                awaiting_refetch = needs_refetch.is_some(),
                                stalled_for = ?VCT_ROOT_STALL_WARN_AFTER,
                                "VCT: checkpoint commit stalled with no verifiable supplied root; \
                                 the node cannot advance until a peer serves this height (it will \
                                 not recompute against the frozen frontier)"
                            );
                            metrics::gauge!("state.vct.root.stalled.height")
                                .set(f64::from(height.0));
                            vct_root_stall_logged = true;
                        } else {
                            tracing::warn!(
                                ?height,
                                block_height = ?ordered_block.0.height,
                                block_hash = ?ordered_block.0.hash,
                                awaiting_refetch = needs_refetch.is_some(),
                                "VCT: supplied root not yet verifiable; retrying checkpoint commit in place"
                            );
                        }

                        prev_finalized_note_commitment_trees = prev_note_commitment_trees_for_retry;
                        retry_finalized_block = Some(ordered_block);
                        cancel_pending_precompute(&mut pending_precompute);
                        std::thread::park_timeout(if needs_refetch.is_some() {
                            VCT_ROOT_RETRY_WAIT
                        } else {
                            VCT_AWAIT_SUCCESSOR_WAIT
                        });
                        continue;
                    }

                    let finalized_tip = finalized_state.db.tip();
                    let _ = ordered_block.1.send(Err(error.clone()));

                    // The commit failed and the queue is being reset, so any
                    // look-ahead precompute is for a block that will not be
                    // committed: cancel it so the spawned task stops instead of
                    // hashing the discarded child, and clear the look-ahead.
                    cancel_pending_precompute(&mut pending_precompute);
                    finalized_lookahead.clear();
                    finalized_state.clear_vct_prevalidated_next();

                    // The last block in the queue failed, so we can't commit the next block.
                    // Instead, we need to reset the state queue,
                    // and discard any children of the invalid block in the channel.
                    info!(
                        ?error,
                        last_valid_height = ?finalized_tip.map(|tip| tip.0),
                        last_valid_hash = ?finalized_tip.map(|tip| tip.1),
                        "committing a block to the finalized state failed, resetting state queue",
                    );

                    let send_result =
                        invalid_block_reset_sender.send(finalized_state.db.finalized_tip_hash());

                    if send_result.is_err() {
                        info!(
                            "StateService closed the block reset channel. Is Zebra shutting down?"
                        );
                        return;
                    }
                }
            }
        }

        // Do this check even if the channel got closed before any finalized blocks were sent.
        // This can happen if we're past the finalized tip.
        if invalid_block_reset_sender.is_closed() {
            info!("StateService closed the block reset channel. Is Zebra shutting down?");
            return;
        }

        // Save any errors to propagate down to queued child blocks
        let mut parent_error_map: IndexMap<block::Hash, ValidateContextError> = IndexMap::new();

        while let Some(msg) = deferred_non_finalized_messages
            .pop_front()
            .or_else(|| non_finalized_block_write_receiver.blocking_recv())
        {
            let queued_child_and_rsp_tx = match msg {
                NonFinalizedWriteMessage::Commit(queued_child) => Some(queued_child),
                NonFinalizedWriteMessage::CommitHeaderRange {
                    anchor,
                    headers,
                    body_sizes,
                    tree_aux_roots,
                    rsp_tx,
                } => {
                    commit_header_range(
                        finalized_state,
                        anchor,
                        headers,
                        body_sizes,
                        tree_aux_roots,
                        rsp_tx,
                    );
                    continue;
                }
                NonFinalizedWriteMessage::Invalidate { hash, rsp_tx } => {
                    tracing::info!(?hash, "invalidating a block in the non-finalized state");
                    let _ = rsp_tx.send(non_finalized_state.invalidate_block(hash));
                    None
                }
                NonFinalizedWriteMessage::Reconsider { hash, rsp_tx } => {
                    tracing::info!(?hash, "reconsidering a block in the non-finalized state");
                    let _ = rsp_tx
                        .send(non_finalized_state.reconsider_block(hash, &finalized_state.db));
                    None
                }
            };

            let Some((queued_child, rsp_tx)) = queued_child_and_rsp_tx else {
                update_latest_chain_channels(
                    non_finalized_state,
                    chain_tip_sender,
                    non_finalized_state_sender,
                    backup_dir_path.as_deref(),
                );
                continue;
            };

            let child_hash = queued_child.hash;
            let parent_hash = queued_child.block.header.previous_block_hash;
            let child_height = queued_child.height;
            let child_block = queued_child.block.clone();
            let parent_error = parent_error_map.get(&parent_hash);

            // If the parent block was marked as rejected, also reject all its children.
            //
            // At this point, we know that all the block's descendants
            // are invalid, because we checked all the consensus rules before
            // committing the failing ancestor block to the non-finalized state.
            let result = if let Some(parent_error) = parent_error {
                Err(parent_error.clone())
            } else {
                tracing::trace!(?child_hash, "validating queued child");
                validate_and_commit_non_finalized(
                    &finalized_state.db,
                    non_finalized_state,
                    queued_child,
                )
            };

            // TODO: fix the test timing bugs that require the result to be sent
            //       after `update_latest_chain_channels()`,
            //       and send the result on rsp_tx here

            if let Err(ref error) = result {
                // If the block is invalid, mark any descendant blocks as rejected.
                parent_error_map.insert(child_hash, error.clone());

                // Make sure the error map doesn't get too big.
                if parent_error_map.len() > PARENT_ERROR_MAP_LIMIT {
                    // We only add one hash at a time, so we only need to remove one extra here.
                    parent_error_map.shift_remove_index(0);
                }

                // Signal the StateService to drop this hash from
                // `non_finalized_block_write_sent_hashes`, so a subsequent
                // re-delivery of a block at the same hash is not short-circuited
                // as a "duplicate" against a rejected variant that never reached
                // any chain.
                //
                // If the receiver was dropped (the StateService is shutting
                // down), ignore the error: the lockout cannot matter once the
                // service exits.
                let _ = non_finalized_rejected_sender.send(child_hash);

                // Update the caller with the error.
                let _ = rsp_tx.send(result.map(|()| child_hash).map_err(Into::into));

                // Skip the things we only need to do for successfully committed blocks
                continue;
            }

            if should_seed_zakura_header_from_non_finalized_commit(
                *seed_zakura_header_from_best_chain_commits,
                non_finalized_state,
                child_height,
                child_hash,
            ) {
                seed_zakura_header_from_committed_block(
                    &finalized_state.db,
                    child_height,
                    &child_block,
                );
            }

            // Committing blocks to the finalized state keeps the same chain,
            // so we can update the chain seen by the rest of the application now.
            //
            // TODO: if this causes state request errors due to chain conflicts,
            //       fix the `service::read` bugs,
            //       or do the channel update after the finalized state commit
            let tip_block_height = update_latest_chain_channels(
                non_finalized_state,
                chain_tip_sender,
                non_finalized_state_sender,
                backup_dir_path.as_deref(),
            );

            // Update the caller with the result.
            let _ = rsp_tx.send(result.map(|()| child_hash).map_err(Into::into));

            while non_finalized_state
                .best_chain_len()
                .expect("just successfully inserted a non-finalized block above")
                > MAX_BLOCK_REORG_HEIGHT
            {
                tracing::trace!("finalizing block past the reorg limit");
                let contextually_verified_with_trees = non_finalized_state.finalize();
                prev_finalized_note_commitment_trees = finalized_state
                            .commit_finalized_direct(contextually_verified_with_trees, prev_finalized_note_commitment_trees.take(), None, None, "commit contextually-verified request")
                            .expect(
                                "unexpected finalized block commit error: note commitment and history trees were already checked by the non-finalized state",
                            ).1.into();
            }

            // Update the metrics if semantic and contextual validation passes
            //
            // TODO: split this out into a function?
            metrics::counter!("state.full_verifier.committed.block.count").increment(1);
            metrics::counter!("zcash.chain.verified.block.total").increment(1);

            metrics::gauge!("state.full_verifier.committed.block.height")
                .set(tip_block_height.0 as f64);

            // This height gauge is updated for both fully verified and checkpoint blocks.
            // These updates can't conflict, because this block write task makes sure that blocks
            // are committed in order.
            metrics::gauge!("zcash.chain.verified.block.height").set(tip_block_height.0 as f64);

            tracing::trace!("finished processing queued block");
        }

        // We're finished receiving non-finalized blocks from the state, and
        // done writing to the finalized state, so we can force it to shut down.
        finalized_state.db.shutdown(true);
        std::mem::drop(self.finalized_state);
    }
}

fn seed_zakura_header_from_committed_block(
    finalized_state: &ZebraDb,
    height: block::Height,
    block: &Arc<block::Block>,
) {
    match finalized_state.seed_zakura_header_from_committed_block(height, block) {
        Ok(()) => {
            tracing::trace!(?height, hash = ?block.hash(), "seeded Zakura header from committed block");
        }
        Err(error) => {
            tracing::warn!(
                ?height,
                hash = ?block.hash(),
                ?error,
                "failed to seed Zakura header from committed block"
            );
        }
    }
}

fn should_seed_zakura_header_from_non_finalized_commit(
    enabled: bool,
    non_finalized_state: &NonFinalizedState,
    height: block::Height,
    hash: block::Hash,
) -> bool {
    enabled && non_finalized_state.best_tip() == Some((height, hash))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zebra_chain::{
        parameters::Network, serialization::ZcashDeserializeInto, value_balance::ValueBalance,
    };

    use crate::{
        arbitrary::Prepare,
        service::{
            finalized_state::FinalizedState,
            non_finalized_state::NonFinalizedState,
            write::{
                seed_zakura_header_from_committed_block,
                should_seed_zakura_header_from_non_finalized_commit,
            },
        },
        tests::FakeChainHelper,
        Config,
    };

    #[test]
    fn side_chain_commit_does_not_seed_zakura_headers() {
        let _init_guard = zebra_test::init();

        let network = Network::Mainnet;
        let mut config = Config::ephemeral();
        config.enable_zakura_header_seed_from_committed_blocks = true;
        let finalized_state = FinalizedState::new(
            &config,
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        );
        finalized_state.set_finalized_value_pool(ValueBalance::fake_populated_pool());

        let parent = zebra_test::vectors::BLOCK_MAINNET_434873_BYTES
            .zcash_deserialize_into::<Arc<zebra_chain::block::Block>>()
            .expect("block deserializes");
        let best_block = parent.make_fake_child().set_work(10);
        let side_block = parent.make_fake_child().set_work(1);
        let best_height = best_block
            .coinbase_height()
            .expect("fake child block has a coinbase height");

        let mut non_finalized_state = NonFinalizedState::new(&network);

        non_finalized_state
            .commit_new_chain(best_block.clone().prepare(), &finalized_state)
            .expect("best block commits to a new chain");
        assert!(should_seed_zakura_header_from_non_finalized_commit(
            true,
            &non_finalized_state,
            best_height,
            best_block.hash(),
        ));
        seed_zakura_header_from_committed_block(&finalized_state.db, best_height, &best_block);

        non_finalized_state
            .commit_new_chain(side_block.clone().prepare(), &finalized_state)
            .expect("side block commits to a losing fork");
        assert!(!should_seed_zakura_header_from_non_finalized_commit(
            true,
            &non_finalized_state,
            best_height,
            side_block.hash(),
        ));

        assert_eq!(
            finalized_state.db.best_header_tip(),
            Some((best_height, best_block.hash()))
        );
        assert_eq!(
            finalized_state.db.headers_by_height_range(best_height, 1),
            vec![(best_height, best_block.hash(), best_block.header.clone())],
        );
    }
}
