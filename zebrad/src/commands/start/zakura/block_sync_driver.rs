use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{pin, select, sync::mpsc};
use tower::{Service, ServiceExt};
use tracing::{debug, warn};

use zebra_chain::block;
use zebra_network::zakura::{
    commit_state_trace as cs_trace, BlockApplyClass, BlockApplyResult, BlockApplyToken,
    BlockSizeEstimate, BlockSyncAction, BlockSyncBlockMeta, BlockSyncEvent, BlockSyncHandle,
    BlockSyncMisbehavior, ZakuraTrace,
};

use super::{
    block_verify_error_is_duplicate, emit_commit_state, insert_cs_hash, insert_cs_height,
    insert_cs_peer, insert_cs_str, insert_cs_u64, query_block_sync_frontiers,
    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
};

pub(crate) const ZAKURA_BLOCK_SYNC_MISSING_BODY_WINDOW: u32 = 262_144;

/// Drive the node-side reads the block-sync reactor asks for: needed-blocks
/// queries and inbound `GetBlocks` serving. The commit *tail* no longer runs here
/// — bodies are committed by the [`Committer`](super::committer::Committer)
/// draining the applyQ; this loop is purely the state-read seam.
pub(crate) async fn drive_block_sync_actions<ReadState>(
    mut actions: mpsc::Receiver<BlockSyncAction>,
    // Retained so the disconnect capability stays wired into the driver, even
    // though peer scoring no longer drives disconnects (misbehavior is record-only).
    _supervisor: zebra_network::zakura::ZakuraSupervisorHandle,
    block_sync: BlockSyncHandle,
    read_state: ReadState,
    trace: ZakuraTrace,
    shutdown: impl Future<Output = ()> + Send + 'static,
) where
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    ReadState::Future: Send + 'static,
{
    pin!(shutdown);
    let mut deferred_actions = VecDeque::new();

    loop {
        let action = if let Some(action) =
            coalesce_ready_needed_block_queries(&mut actions, &mut deferred_actions)
        {
            action
        } else if let Some(action) = deferred_actions.pop_front() {
            action
        } else {
            select! {
                _ = &mut shutdown => return,
                action = actions.recv() => {
                    let Some(action) = action else {
                        return;
                    };
                    action
                }
            }
        };
        let action =
            coalesce_stale_needed_block_queries(action, &mut actions, &mut deferred_actions);

        trace_block_driver_action(&trace, &action);
        match action {
            BlockSyncAction::Misbehavior { peer, reason } => {
                // Record-only: peer scoring no longer drives disconnects.
                debug!(?peer, ?reason, "recorded Zakura block-sync peer violation");
            }
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip,
                best_header_tip,
            } => {
                emit_commit_state(
                    &trace,
                    cs_trace::STATE_READ_START,
                    "block_sync_driver",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "query_needed_blocks");
                        insert_cs_height(row, cs_trace::VERIFIED_BLOCK_TIP, verified_block_tip);
                        insert_cs_height(row, cs_trace::BEST_HEADER_TIP, best_header_tip);
                    },
                );
                let started = Instant::now();
                match query_block_sync_needed_blocks(
                    read_state.clone(),
                    verified_block_tip,
                    best_header_tip,
                )
                .await
                {
                    Ok(blocks) => {
                        emit_commit_state(
                            &trace,
                            cs_trace::STATE_READ_SUCCESS,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "query_needed_blocks");
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, blocks.len() as u64);
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
                        let _ = block_sync.send_control(BlockSyncEvent::NeededBlocks(blocks));
                        emit_commit_state(
                            &trace,
                            cs_trace::REACTOR_EVENT_SENT,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "needed_blocks");
                            },
                        );
                    }
                    Err(error) => {
                        emit_commit_state(
                            &trace,
                            cs_trace::STATE_READ_ERROR,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "query_needed_blocks");
                                insert_cs_str(row, cs_trace::RESULT, "error");
                                insert_cs_str(row, cs_trace::REASON, &format!("{error}"));
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
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
                emit_commit_state(
                    &trace,
                    cs_trace::STATE_READ_START,
                    "block_sync_driver",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "query_blocks_by_height_range");
                        insert_cs_peer(row, cs_trace::PEER, &peer);
                        insert_cs_height(row, cs_trace::RANGE_START, start);
                        insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
                    },
                );
                let started = Instant::now();
                match tokio::time::timeout(
                    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
                    read_state
                        .clone()
                        .oneshot(zebra_state::ReadRequest::BlocksByHeightRange { start, count }),
                )
                .await
                {
                    Ok(Ok(zebra_state::ReadResponse::Blocks(blocks))) => {
                        emit_commit_state(
                            &trace,
                            cs_trace::STATE_READ_SUCCESS,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(
                                    row,
                                    cs_trace::ACTION,
                                    "query_blocks_by_height_range",
                                );
                                insert_cs_peer(row, cs_trace::PEER, &peer);
                                insert_cs_height(row, cs_trace::RANGE_START, start);
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, blocks.len() as u64);
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
                        emit_commit_state(
                            &trace,
                            cs_trace::REACTOR_EVENT_SENT,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "block_range_response_ready");
                                insert_cs_peer(row, cs_trace::PEER, &peer);
                                insert_cs_height(row, cs_trace::RANGE_START, start);
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
                            },
                        );
                        let _ = block_sync.send_control(BlockSyncEvent::BlockRangeResponseReady {
                            peer,
                            start_height: start,
                            requested_count: count,
                            blocks,
                        });
                    }
                    Ok(Ok(response)) => {
                        trace_block_range_error(
                            &trace,
                            &peer,
                            start,
                            count,
                            "unexpected_response",
                            started,
                        );
                        warn!(?peer, ?response, "unexpected BlocksByHeightRange response");
                        trace_block_range_finished(&trace, &peer, start, count, 0);
                        let _ =
                            block_sync.send_control(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            });
                    }
                    Ok(Err(error)) => {
                        trace_block_range_error(
                            &trace,
                            &peer,
                            start,
                            count,
                            &format!("{error}"),
                            started,
                        );
                        warn!(
                            ?peer,
                            ?error,
                            "failed to read Zakura Blocks response from state"
                        );
                        trace_block_range_finished(&trace, &peer, start, count, 0);
                        let _ =
                            block_sync.send_control(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            });
                    }
                    Err(_elapsed) => {
                        emit_commit_state(
                            &trace,
                            cs_trace::STATE_READ_TIMEOUT,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(
                                    row,
                                    cs_trace::ACTION,
                                    "query_blocks_by_height_range",
                                );
                                insert_cs_peer(row, cs_trace::PEER, &peer);
                                insert_cs_height(row, cs_trace::RANGE_START, start);
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
                        warn!(?peer, "timed out reading Zakura block-sync serving range");
                        trace_block_range_finished(&trace, &peer, start, count, 0);
                        let _ =
                            block_sync.send_control(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            });
                    }
                }
            }
        }
    }
}

pub(crate) fn coalesce_ready_needed_block_queries(
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    deferred_actions: &mut VecDeque<BlockSyncAction>,
) -> Option<BlockSyncAction> {
    let mut latest_query = None;
    let mut retained = VecDeque::new();
    while let Some(action) = deferred_actions.pop_front() {
        match action {
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip,
                best_header_tip,
            } => {
                latest_query = Some((verified_block_tip, best_header_tip));
            }
            action => retained.push_back(action),
        }
    }
    *deferred_actions = retained;

    while let Ok(action) = actions.try_recv() {
        match action {
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip,
                best_header_tip,
            } => {
                latest_query = Some((verified_block_tip, best_header_tip));
            }
            action => deferred_actions.push_back(action),
        }
    }

    latest_query.map(
        |(verified_block_tip, best_header_tip)| BlockSyncAction::QueryNeededBlocks {
            verified_block_tip,
            best_header_tip,
        },
    )
}

pub(crate) fn coalesce_stale_needed_block_queries(
    action: BlockSyncAction,
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    deferred_actions: &mut VecDeque<BlockSyncAction>,
) -> BlockSyncAction {
    let BlockSyncAction::QueryNeededBlocks {
        mut verified_block_tip,
        mut best_header_tip,
    } = action
    else {
        return action;
    };

    let mut coalesced_count = 0u64;
    while let Ok(action) = actions.try_recv() {
        match action {
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip: latest_verified_block_tip,
                best_header_tip: latest_best_header_tip,
            } => {
                verified_block_tip = latest_verified_block_tip;
                best_header_tip = latest_best_header_tip;
                coalesced_count = coalesced_count.saturating_add(1);
            }
            action => deferred_actions.push_back(action),
        }
    }

    if coalesced_count > 0 {
        metrics::counter!("sync.block.needed_query.coalesced").increment(coalesced_count);
    }

    BlockSyncAction::QueryNeededBlocks {
        verified_block_tip,
        best_header_tip,
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

#[cfg(test)]
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
    match class {
        BlockApplyClass::Checkpoint => block_commit_result(height, expected_hash, commit.await),
        BlockApplyClass::Full => {
            match tokio::time::timeout(ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT, commit).await {
                Ok(outcome) => block_commit_result(height, expected_hash, outcome),
                Err(_elapsed) => block_commit_timed_out(height, expected_hash),
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn commit_block_sync_body_with_stall_trace<BlockVerifier>(
    block_verifier: BlockVerifier,
    block: Arc<block::Block>,
    class: BlockApplyClass,
    trace: &ZakuraTrace,
    token: BlockApplyToken,
    height: block::Height,
    expected_hash: block::Hash,
) -> BlockApplyResult
where
    BlockVerifier:
        Service<zebra_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
{
    let commit = block_verifier
        .clone()
        .oneshot(zebra_consensus::Request::Commit(block));

    match class {
        BlockApplyClass::Checkpoint => {
            tokio::pin!(commit);
            tokio::select! {
                outcome = &mut commit => block_commit_result(Some(height), expected_hash, outcome),
                _ = tokio::time::sleep(ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT) => {
                    emit_commit_state(
                        trace,
                        cs_trace::COMMIT_STALLED,
                        "block_sync_driver",
                        |row| {
                            insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
                            insert_cs_str(row, cs_trace::APPLY_CLASS, block_apply_class_label(class));
                            insert_cs_height(row, cs_trace::HEIGHT, height);
                            insert_cs_hash(row, cs_trace::HASH, expected_hash);
                            insert_cs_u64(
                                row,
                                cs_trace::ELAPSED_MS,
                                ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT.as_millis().try_into().unwrap_or(u64::MAX),
                            );
                        },
                    );
                    block_commit_result(Some(height), expected_hash, commit.await)
                }
            }
        }
        BlockApplyClass::Full => {
            match tokio::time::timeout(ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT, commit).await {
                Ok(outcome) => block_commit_result(Some(height), expected_hash, outcome),
                Err(_elapsed) => block_commit_timed_out(Some(height), expected_hash),
            }
        }
    }
}

fn block_commit_result<E>(
    height: Option<block::Height>,
    expected_hash: block::Hash,
    outcome: Result<block::Hash, E>,
) -> BlockApplyResult
where
    E: std::fmt::Debug + Send + Sync + 'static,
{
    match outcome {
        Ok(committed_hash) if committed_hash == expected_hash => {
            debug!(
                ?height,
                ?committed_hash,
                "Zakura block sync committed block body through verifier"
            );
            BlockApplyResult::Committed
        }
        Ok(committed_hash) => {
            warn!(
                ?height,
                ?expected_hash,
                ?committed_hash,
                "Zakura block-sync verifier returned an unexpected hash"
            );
            BlockApplyResult::Rejected
        }
        Err(error) => {
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
    }
}

fn block_commit_timed_out(
    height: Option<block::Height>,
    expected_hash: block::Hash,
) -> BlockApplyResult {
    warn!(
        ?height,
        ?expected_hash,
        "timed out committing Zakura block-sync body"
    );
    BlockApplyResult::TimedOut
}

/// How many times the durable-frontier watcher retries a failed frontier read
/// before deferring to the next chain-tip change. The watcher is edge-triggered,
/// so silently dropping a read could strand held budget until the next — possibly
/// never — tip change; a bounded retry heals a transient failure on the final
/// advance without an unbounded loop.
const DURABLE_FRONTIER_READ_MAX_ATTEMPTS: u32 = 5;
/// Delay between durable-frontier read retries after a transient `None`.
const DURABLE_FRONTIER_READ_RETRY_DELAY: Duration = Duration::from_millis(200);

/// Inject a durable frontier advance into the block-sync Sequencer on every
/// `set_finalized_tip` change.
///
/// Replaces the deleted 200 ms checkpoint-frontier poll: on each durable chain-tip
/// change it reads the finalized + verified frontier and reports it to the
/// Sequencer (via [`BlockSyncHandle::report_durable_frontier`]), which advances the
/// verified tip and releases the held byte reservations ≤ the durable tip. The byte
/// budget therefore recycles continuously (per durable advance) instead of once per
/// poll tick, which is the bottleneck the scoped apply-side change removes.
///
/// Idempotent with the endpoint-frontier mirror path
/// (`mirror_zakura_full_block_commits`) via the Sequencer's stale guard; the
/// dedicated watcher guarantees the checkpoint frontier advance is driven directly
/// off durability rather than the mirror/endpoint hop the poll worked around. A
/// transient frontier-read failure is retried (see
/// [`DURABLE_FRONTIER_READ_MAX_ATTEMPTS`]) so the edge-triggered release cannot be
/// silently dropped on the final advance.
pub(crate) async fn drive_block_sync_durable_frontier<ReadState>(
    mut chain_tip_change: zebra_state::ChainTipChange,
    latest_chain_tip: zebra_state::LatestChainTip,
    read_state: ReadState,
    block_sync: BlockSyncHandle,
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
{
    pin!(shutdown);
    loop {
        select! {
            _ = &mut shutdown => return,
            change = chain_tip_change.wait_for_tip_change() => {
                let Ok(_action) = change else {
                    return;
                };
            }
        }
        // Read the durable frontier and report it so the Sequencer releases the
        // held byte budget ≤ the durable tip. This watcher is edge-triggered on
        // tip changes, so a dropped report — a transient state-read failure
        // returning `None` — would strand the held bytes until the *next* tip
        // change, which may never arrive once sync reaches the header tip. Retry
        // a bounded number of times so a transient failure on the final advance
        // does not leave bytes reserved with no re-trigger. (The endpoint-frontier
        // mirror is a second, independent release path, but it is edge-triggered
        // on the same event and can also fail, so this watcher heals itself rather
        // than relying on it.)
        let mut attempt: u32 = 0;
        loop {
            match query_block_sync_frontiers(read_state.clone(), latest_chain_tip.clone()).await {
                Some(frontiers) => {
                    block_sync.report_durable_frontier(frontiers);
                    break;
                }
                None => {
                    attempt += 1;
                    if attempt >= DURABLE_FRONTIER_READ_MAX_ATTEMPTS {
                        warn!(
                            attempt,
                            "Zakura durable-frontier read kept failing; deferring \
                             budget release to the next chain-tip change"
                        );
                        break;
                    }
                    select! {
                        _ = &mut shutdown => return,
                        _ = tokio::time::sleep(DURABLE_FRONTIER_READ_RETRY_DELAY) => {}
                    }
                }
            }
        }
    }
}

pub(crate) async fn query_block_sync_needed_blocks<ReadState>(
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

    let mut needed = Vec::new();
    let mut next_from = from;
    let mut remaining = limit;

    while remaining > 0 {
        let chunk_limit = remaining.min(zebra_state::constants::MAX_HEADER_SYNC_HEIGHT_RANGE);
        needed.extend(
            query_block_sync_needed_blocks_chunk(read_state.clone(), next_from, chunk_limit)
                .await?,
        );

        remaining = remaining.saturating_sub(chunk_limit);
        let Some(after_chunk) = next_from.0.checked_add(chunk_limit).map(block::Height) else {
            break;
        };
        next_from = after_chunk;
    }

    Ok(needed)
}

async fn query_block_sync_needed_blocks_chunk<ReadState>(
    read_state: ReadState,
    from: block::Height,
    limit: u32,
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
        .clamp(1, ZAKURA_BLOCK_SYNC_MISSING_BODY_WINDOW);
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

fn trace_block_driver_action(trace: &ZakuraTrace, action: &BlockSyncAction) {
    emit_commit_state(
        trace,
        cs_trace::ACTION_RECEIVED,
        "block_sync_driver",
        |row| match action {
            BlockSyncAction::Misbehavior { peer, reason } => {
                insert_cs_str(row, cs_trace::ACTION, "misbehavior");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_str(row, cs_trace::REASON, block_sync_misbehavior_label(*reason));
            }
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip,
                best_header_tip,
            } => {
                insert_cs_str(row, cs_trace::ACTION, "query_needed_blocks");
                insert_cs_height(row, cs_trace::VERIFIED_BLOCK_TIP, *verified_block_tip);
                insert_cs_height(row, cs_trace::BEST_HEADER_TIP, *best_header_tip);
            }
            BlockSyncAction::QueryBlocksByHeightRange { peer, start, count } => {
                insert_cs_str(row, cs_trace::ACTION, "query_blocks_by_height_range");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_height(row, cs_trace::RANGE_START, *start);
                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(*count));
            }
        },
    );
}

fn trace_block_range_error(
    trace: &ZakuraTrace,
    peer: &zebra_network::zakura::ZakuraPeerId,
    start: block::Height,
    count: u32,
    reason: &str,
    started: Instant,
) {
    emit_commit_state(
        trace,
        cs_trace::STATE_READ_ERROR,
        "block_sync_driver",
        |row| {
            insert_cs_str(row, cs_trace::ACTION, "query_blocks_by_height_range");
            insert_cs_peer(row, cs_trace::PEER, peer);
            insert_cs_height(row, cs_trace::RANGE_START, start);
            insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
            insert_cs_str(row, cs_trace::RESULT, "error");
            insert_cs_str(row, cs_trace::REASON, reason);
            insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
        },
    );
}

fn trace_block_range_finished(
    trace: &ZakuraTrace,
    peer: &zebra_network::zakura::ZakuraPeerId,
    start: block::Height,
    requested_count: u32,
    returned_count: u32,
) {
    emit_commit_state(
        trace,
        cs_trace::REACTOR_EVENT_SENT,
        "block_sync_driver",
        |row| {
            insert_cs_str(row, cs_trace::ACTION, "block_range_response_finished");
            insert_cs_peer(row, cs_trace::PEER, peer);
            insert_cs_height(row, cs_trace::RANGE_START, start);
            insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(returned_count));
            insert_cs_u64(row, "requested_count", u64::from(requested_count));
        },
    );
}

pub(super) fn block_apply_class_label(class: BlockApplyClass) -> &'static str {
    match class {
        BlockApplyClass::Checkpoint => "checkpoint",
        BlockApplyClass::Full => "full",
    }
}

fn block_sync_misbehavior_label(reason: BlockSyncMisbehavior) -> &'static str {
    match reason {
        BlockSyncMisbehavior::MalformedMessage => "malformed_message",
        BlockSyncMisbehavior::UnsolicitedBlock => "unsolicited_block",
        BlockSyncMisbehavior::GetBlocksTooLong => "get_blocks_too_long",
        BlockSyncMisbehavior::GetBlocksSpam => "get_blocks_spam",
        BlockSyncMisbehavior::InvalidBlock => "invalid_block",
        BlockSyncMisbehavior::SizeMismatch => "size_mismatch",
        BlockSyncMisbehavior::InvalidStatus => "invalid_status",
        BlockSyncMisbehavior::UnsolicitedDone => "unsolicited_done",
        BlockSyncMisbehavior::RangeUnavailable => "range_unavailable",
        BlockSyncMisbehavior::StatusSpam => "status_spam",
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
