use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::Arc,
    time::Duration,
};

use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};
use tokio::{pin, select, sync::mpsc};
use tower::{Service, ServiceExt};
use tracing::{debug, warn};
use tracing_futures::Instrument;

use zebra_chain::{block, chain_tip::ChainTip};
use zebra_network::zakura::{
    BlockApplyResult, BlockApplyToken, BlockSizeEstimate, BlockSyncAction, BlockSyncBlockMeta,
    BlockSyncEvent, BlockSyncHandle, BlockSyncMisbehavior,
};

use crate::components::sync;

use super::{
    block_verify_error_is_duplicate, query_block_sync_frontiers, ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
};

pub(crate) const ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_INTERVAL: Duration =
    Duration::from_secs(5);
const ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_ATTEMPTS: usize = 24;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum BlockApplyClass {
    Checkpoint,
    Full,
}

#[derive(Clone, Debug)]
struct PendingBlockApply {
    token: BlockApplyToken,
    class: BlockApplyClass,
    block: Arc<block::Block>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn drive_block_sync_actions<ReadState, BlockVerifier>(
    mut actions: mpsc::Receiver<BlockSyncAction>,
    supervisor: zebra_network::zakura::ZakuraSupervisorHandle,
    block_sync: BlockSyncHandle,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    read_state: ReadState,
    block_verifier: BlockVerifier,
    max_checkpoint_height: block::Height,
    checkpoint_apply_limit: usize,
    full_apply_limit: usize,
    shutdown: impl Future<Output = ()> + Send + 'static,
) where
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + 'static,
    ReadState::Future: Send + 'static,
    BlockVerifier:
        Service<zebra_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
{
    pin!(shutdown);
    let checkpoint_apply_limit = checkpoint_apply_limit.max(sync::MIN_CHECKPOINT_CONCURRENCY_LIMIT);
    let full_apply_limit = full_apply_limit.max(sync::MIN_CONCURRENCY_LIMIT);
    let mut pending_applies = VecDeque::new();
    let mut in_flight_applies = FuturesUnordered::new();
    let mut checkpoint_in_flight = 0usize;
    let mut full_in_flight = 0usize;

    loop {
        let action = select! {
            _ = &mut shutdown => return,
            completed = in_flight_applies.next(), if !in_flight_applies.is_empty() => {
                let Some(completed) = completed else {
                    continue;
                };
                match completed {
                    BlockApplyClass::Checkpoint => {
                        checkpoint_in_flight = checkpoint_in_flight.saturating_sub(1);
                    }
                    BlockApplyClass::Full => {
                        full_in_flight = full_in_flight.saturating_sub(1);
                    }
                }
                drain_pending_block_applies(
                    &mut pending_applies,
                    &mut in_flight_applies,
                    &mut checkpoint_in_flight,
                    &mut full_in_flight,
                    checkpoint_apply_limit,
                    full_apply_limit,
                    latest_chain_tip.clone(),
                    read_state.clone(),
                    block_verifier.clone(),
                    block_sync.clone(),
                );
                continue;
            }
            action = actions.recv() => {
                let Some(action) = action else {
                    return;
                };
                action
            }
        };

        match action {
            BlockSyncAction::SendMessage { .. } => {}
            BlockSyncAction::Misbehavior { peer, reason } => {
                if block_sync_misbehavior_is_hard(reason) {
                    debug!(
                        ?peer,
                        ?reason,
                        "disconnecting peer for Zakura block-sync violation"
                    );
                    let _ = supervisor.disconnect_peer(&peer).await;
                } else {
                    debug!(
                        ?peer,
                        ?reason,
                        "recorded soft Zakura block-sync peer violation"
                    );
                }
            }
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip,
                best_header_tip,
            } => {
                match query_block_sync_needed_blocks(
                    read_state.clone(),
                    verified_block_tip,
                    best_header_tip,
                )
                .await
                {
                    Ok(blocks) => {
                        let _ = block_sync.send(BlockSyncEvent::NeededBlocks(blocks)).await;
                    }
                    Err(error) => {
                        warn!(
                            ?verified_block_tip,
                            ?best_header_tip,
                            ?error,
                            "failed to query Zakura block-sync needed blocks"
                        );
                    }
                }
            }
            BlockSyncAction::QueryBlocksByHeightRange { peer, start, count } => {
                match tokio::time::timeout(
                    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
                    read_state
                        .clone()
                        .oneshot(zebra_state::ReadRequest::BlocksByHeightRange { start, count }),
                )
                .await
                {
                    Ok(Ok(zebra_state::ReadResponse::Blocks(blocks))) => {
                        let _ = block_sync
                            .send(BlockSyncEvent::BlockRangeResponseReady {
                                peer,
                                start_height: start,
                                requested_count: count,
                                blocks,
                            })
                            .await;
                    }
                    Ok(Ok(response)) => {
                        warn!(?peer, ?response, "unexpected BlocksByHeightRange response");
                        let _ = block_sync
                            .send(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            })
                            .await;
                    }
                    Ok(Err(error)) => {
                        warn!(
                            ?peer,
                            ?error,
                            "failed to read Zakura Blocks response from state"
                        );
                        let _ = block_sync
                            .send(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            })
                            .await;
                    }
                    Err(_elapsed) => {
                        warn!(?peer, "timed out reading Zakura block-sync serving range");
                        let _ = block_sync
                            .send(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            })
                            .await;
                    }
                }
            }
            BlockSyncAction::SubmitBlock { token, block } => {
                pending_applies.push_back(PendingBlockApply {
                    token,
                    class: block_apply_class(block.as_ref(), max_checkpoint_height),
                    block,
                });
                drain_pending_block_applies(
                    &mut pending_applies,
                    &mut in_flight_applies,
                    &mut checkpoint_in_flight,
                    &mut full_in_flight,
                    checkpoint_apply_limit,
                    full_apply_limit,
                    latest_chain_tip.clone(),
                    read_state.clone(),
                    block_verifier.clone(),
                    block_sync.clone(),
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_pending_block_applies<ReadState, BlockVerifier>(
    pending_applies: &mut VecDeque<PendingBlockApply>,
    in_flight_applies: &mut FuturesUnordered<BoxFuture<'static, BlockApplyClass>>,
    checkpoint_in_flight: &mut usize,
    full_in_flight: &mut usize,
    checkpoint_apply_limit: usize,
    full_apply_limit: usize,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    read_state: ReadState,
    block_verifier: BlockVerifier,
    block_sync: BlockSyncHandle,
) where
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + 'static,
    ReadState::Future: Send + 'static,
    BlockVerifier:
        Service<zebra_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
{
    while let Some(index) = pending_applies
        .iter()
        .position(|pending| match pending.class {
            BlockApplyClass::Checkpoint => *checkpoint_in_flight < checkpoint_apply_limit,
            BlockApplyClass::Full => *full_in_flight < full_apply_limit,
        })
    {
        let pending = pending_applies
            .remove(index)
            .expect("pending apply index was found in queue");

        match pending.class {
            BlockApplyClass::Checkpoint => {
                *checkpoint_in_flight = checkpoint_in_flight.saturating_add(1);
            }
            BlockApplyClass::Full => {
                *full_in_flight = full_in_flight.saturating_add(1);
            }
        }

        let class = pending.class;
        in_flight_applies.push(
            apply_block_sync_body(
                block_verifier.clone(),
                latest_chain_tip.clone(),
                read_state.clone(),
                block_sync.clone(),
                pending.token,
                pending.block,
                class,
            )
            .map(move |_| class)
            .boxed(),
        );
    }
}

pub(crate) fn block_apply_class(
    block: &block::Block,
    max_checkpoint_height: block::Height,
) -> BlockApplyClass {
    if block
        .coinbase_height()
        .is_some_and(|height| height <= max_checkpoint_height)
    {
        BlockApplyClass::Checkpoint
    } else {
        BlockApplyClass::Full
    }
}

pub(crate) async fn apply_block_sync_body<BlockVerifier, ReadState>(
    block_verifier: BlockVerifier,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    read_state: ReadState,
    block_sync: BlockSyncHandle,
    token: BlockApplyToken,
    block: Arc<block::Block>,
    class: BlockApplyClass,
) where
    BlockVerifier:
        Service<zebra_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let expected_hash = block.hash();
    let Some(height) = block.coinbase_height() else {
        warn!(
            ?expected_hash,
            "Zakura block sync cannot apply body without coinbase height"
        );
        return;
    };

    let result = commit_block_sync_body(block_verifier.clone(), block, class).await;
    let local_frontier =
        query_block_sync_frontiers(read_state.clone(), latest_chain_tip.clone()).await;

    let _ = block_sync
        .send(BlockSyncEvent::BlockApplyFinished {
            token,
            height,
            hash: expected_hash,
            result,
            local_frontier,
        })
        .await;

    if class == BlockApplyClass::Checkpoint && result == BlockApplyResult::Committed {
        tokio::spawn(
            refresh_block_sync_frontiers_for_checkpoint_window(
                read_state,
                latest_chain_tip,
                block_sync,
                local_frontier
                    .map(|frontiers| frontiers.verified_block_tip)
                    .unwrap_or_else(|| height.previous().unwrap_or(height)),
            )
            .in_current_span(),
        );
    }
}

pub(crate) fn block_sync_misbehavior_is_hard(reason: BlockSyncMisbehavior) -> bool {
    matches!(
        reason,
        BlockSyncMisbehavior::MalformedMessage
            | BlockSyncMisbehavior::UnsolicitedBlock
            | BlockSyncMisbehavior::GetBlocksTooLong
            | BlockSyncMisbehavior::InvalidBlock
            | BlockSyncMisbehavior::InvalidStatus
            | BlockSyncMisbehavior::UnsolicitedDone
            | BlockSyncMisbehavior::StatusSpam
    )
}

pub(crate) async fn commit_block_sync_body<BlockVerifier>(
    block_verifier: BlockVerifier,
    block: Arc<block::Block>,
    class: BlockApplyClass,
) -> BlockApplyResult
where
    BlockVerifier:
        Service<zebra_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
{
    let expected_hash = block.hash();
    let height = block.coinbase_height();
    let commit = block_verifier
        .clone()
        .oneshot(zebra_consensus::Request::Commit(block));
    let outcome = match class {
        BlockApplyClass::Checkpoint => Ok(commit.await),
        BlockApplyClass::Full => {
            tokio::time::timeout(ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT, commit).await
        }
    };
    match outcome {
        Ok(Ok(committed_hash)) if committed_hash == expected_hash => {
            debug!(
                ?height,
                ?committed_hash,
                "Zakura block sync committed block body through verifier"
            );
            BlockApplyResult::Committed
        }
        Ok(Ok(committed_hash)) => {
            warn!(
                ?height,
                ?expected_hash,
                ?committed_hash,
                "Zakura block-sync verifier returned an unexpected hash"
            );
            BlockApplyResult::Rejected
        }
        Ok(Err(error)) => {
            if block_verify_error_is_duplicate(&error) {
                debug!(
                    ?height,
                    ?expected_hash,
                    ?error,
                    "Zakura block-sync body was already known by the block verifier"
                );
                BlockApplyResult::Duplicate
            } else {
                debug!(
                    ?height,
                    ?expected_hash,
                    ?error,
                    "Zakura block-sync body rejected by block verifier"
                );
                BlockApplyResult::Rejected
            }
        }
        Err(_elapsed) => {
            warn!(
                ?height,
                ?expected_hash,
                "timed out committing Zakura block-sync body"
            );
            BlockApplyResult::TimedOut
        }
    }
}

async fn refresh_block_sync_frontiers_for_checkpoint_window<ReadState>(
    read_state: ReadState,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    block_sync: BlockSyncHandle,
    highest_observed_at_apply: block::Height,
) where
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let mut highest_sent = highest_observed_at_apply;
    for _ in 0..ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_ATTEMPTS {
        tokio::time::sleep(ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_INTERVAL).await;

        let Some(frontiers) =
            query_block_sync_frontiers(read_state.clone(), latest_chain_tip.clone()).await
        else {
            continue;
        };

        if frontiers.verified_block_tip <= highest_sent {
            continue;
        }

        highest_sent = frontiers.verified_block_tip;
        let _ = block_sync
            .send(BlockSyncEvent::StateFrontiersChanged(frontiers))
            .await;
    }
}

async fn query_block_sync_needed_blocks<ReadState>(
    read_state: ReadState,
    verified_block_tip: block::Height,
    best_header_tip: block::Height,
) -> Result<Vec<BlockSyncBlockMeta>, zebra_state::BoxError>
where
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let Some((from, limit)) = block_sync_missing_body_window(verified_block_tip, best_header_tip)
    else {
        return Ok(Vec::new());
    };

    let missing = match tokio::time::timeout(
        ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
        read_state
            .clone()
            .oneshot(zebra_state::ReadRequest::MissingBlockBodies { from, limit }),
    )
    .await
    {
        Ok(Ok(zebra_state::ReadResponse::MissingBlockBodies(heights))) => heights,
        Ok(Ok(response)) => {
            warn!(?response, "unexpected MissingBlockBodies response");
            return Ok(Vec::new());
        }
        Ok(Err(error)) => return Err(error),
        Err(elapsed) => return Err(Box::new(elapsed)),
    };

    let Some(first) = missing.first().copied() else {
        return Ok(Vec::new());
    };
    let Some(last) = missing.last().copied() else {
        return Ok(Vec::new());
    };
    let span = last.0.saturating_sub(first.0).saturating_add(1);

    let headers = match tokio::time::timeout(
        ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
        read_state
            .clone()
            .oneshot(zebra_state::ReadRequest::HeadersByHeightRange {
                start: first,
                count: span,
            }),
    )
    .await
    {
        Ok(Ok(zebra_state::ReadResponse::Headers(headers))) => headers,
        Ok(Ok(response)) => {
            warn!(?response, "unexpected HeadersByHeightRange response");
            return Ok(Vec::new());
        }
        Ok(Err(error)) => return Err(error),
        Err(elapsed) => return Err(Box::new(elapsed)),
    };

    let size_hints = match tokio::time::timeout(
        ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
        read_state.oneshot(zebra_state::ReadRequest::BlockSizeHints {
            from: first,
            count: span,
        }),
    )
    .await
    {
        Ok(Ok(zebra_state::ReadResponse::BlockSizeHints(hints))) => hints,
        Ok(Ok(response)) => {
            warn!(?response, "unexpected BlockSizeHints response");
            Vec::new()
        }
        Ok(Err(error)) => return Err(error),
        Err(elapsed) => return Err(Box::new(elapsed)),
    };

    Ok(block_sync_needed_blocks_from_state(
        missing, headers, size_hints,
    ))
}

pub(crate) fn block_sync_missing_body_window(
    verified_block_tip: block::Height,
    best_header_tip: block::Height,
) -> Option<(block::Height, u32)> {
    if best_header_tip <= verified_block_tip {
        return None;
    }

    let from = block::Height(verified_block_tip.0.saturating_add(1));
    let limit = best_header_tip
        .0
        .saturating_sub(verified_block_tip.0)
        .clamp(1, zebra_state::MAX_BLOCK_REORG_HEIGHT);
    Some((from, limit))
}

pub(crate) fn block_sync_needed_blocks_from_state(
    missing: Vec<block::Height>,
    headers: Vec<(block::Height, block::Hash, Arc<block::Header>)>,
    size_hints: Vec<(block::Height, Option<u32>)>,
) -> Vec<BlockSyncBlockMeta> {
    let headers: HashMap<_, _> = headers
        .into_iter()
        .map(|(height, hash, _header)| (height, hash))
        .collect();
    let size_hints: HashMap<_, _> = size_hints.into_iter().collect();

    missing
        .into_iter()
        .filter_map(|height| {
            let hash = *headers.get(&height)?;
            let size = size_hints
                .get(&height)
                .copied()
                .flatten()
                .filter(|size| *size > 0)
                .map(BlockSizeEstimate::Advertised)
                .unwrap_or(BlockSizeEstimate::Unknown);

            Some(BlockSyncBlockMeta { height, hash, size })
        })
        .collect()
}
