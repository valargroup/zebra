use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::Arc,
    time::Instant,
};

use futures::{future::BoxFuture, FutureExt};
use tokio::{pin, select, sync::mpsc};
use tower::{Service, ServiceExt};
use tracing::{debug, warn};

use zebra_chain::{block, chain_tip::ChainTip};
use zebra_network::zakura::{
    commit_state_trace as cs_trace, BlockApplyClass, BlockApplyExecutor, BlockApplyExecutorPort,
    BlockApplyLimits, BlockApplyOutput, BlockApplyRequest, BlockApplyResult, BlockApplyToken,
    BlockSizeEstimate, BlockSyncAction, BlockSyncBlockMeta, BlockSyncEvent, BlockSyncHandle,
    BlockSyncMisbehavior, Frontier, FrontierChange, ZakuraEndpoint, ZakuraTrace,
};

use crate::components::sync;

use super::{
    block_apply_result_label, block_verify_error_is_duplicate, emit_commit_state, insert_cs_bool,
    insert_cs_frontiers, insert_cs_hash, insert_cs_height, insert_cs_peer, insert_cs_str,
    insert_cs_u64, query_block_sync_frontiers, BlocksyncThroughputProbe,
    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
};

pub(crate) const ZAKURA_BLOCK_SYNC_MISSING_BODY_WINDOW: u32 = 262_144;

#[derive(Clone)]
pub(crate) struct ZebradBlockApplyExecutor<ReadState, BlockVerifier, LatestChainTip> {
    latest_chain_tip: LatestChainTip,
    endpoint: Option<ZakuraEndpoint>,
    read_state: ReadState,
    block_verifier: BlockVerifier,
    max_checkpoint_height: block::Height,
    trace: ZakuraTrace,
    throughput_probe: Option<BlocksyncThroughputProbe>,
}

impl<ReadState, BlockVerifier, LatestChainTip>
    ZebradBlockApplyExecutor<ReadState, BlockVerifier, LatestChainTip>
{
    pub(crate) fn new(
        latest_chain_tip: LatestChainTip,
        endpoint: Option<ZakuraEndpoint>,
        read_state: ReadState,
        block_verifier: BlockVerifier,
        max_checkpoint_height: block::Height,
        trace: ZakuraTrace,
        throughput_probe: Option<BlocksyncThroughputProbe>,
    ) -> Self {
        Self {
            latest_chain_tip,
            endpoint,
            read_state,
            block_verifier,
            max_checkpoint_height,
            trace,
            throughput_probe,
        }
    }

    pub(crate) fn block_apply_class(&self, block: &block::Block) -> BlockApplyClass {
        block_apply_class(block, self.max_checkpoint_height)
    }
}

impl<ReadState, BlockVerifier, LatestChainTip> BlockApplyExecutor
    for ZebradBlockApplyExecutor<ReadState, BlockVerifier, LatestChainTip>
where
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    ReadState::Future: Send + 'static,
    BlockVerifier:
        Service<zebra_consensus::Request, Response = block::Hash> + Clone + Send + Sync + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
    LatestChainTip: ChainTip + Clone + Send + Sync + 'static,
{
    fn block_apply_class(&self, block: &block::Block) -> BlockApplyClass {
        self.block_apply_class(block)
    }

    fn apply(&self, request: BlockApplyRequest) -> BoxFuture<'static, BlockApplyOutput> {
        let executor = self.clone();
        async move {
            let class = executor.block_apply_class(request.block.as_ref());
            apply_block_sync_body_to_output(
                executor.block_verifier,
                executor.latest_chain_tip,
                executor.endpoint,
                executor.read_state,
                request.token,
                request.block,
                class,
                executor.trace,
                executor.throughput_probe,
            )
            .await
        }
        .boxed()
    }

    fn refresh_checkpoint_frontier(
        &self,
        highest_sent: block::Height,
        attempts_remaining: usize,
    ) -> BoxFuture<'static, Option<zebra_network::zakura::BlockSyncFrontiers>> {
        let executor = self.clone();
        async move {
            refresh_block_sync_frontiers_for_checkpoint_window(
                executor.read_state,
                executor.latest_chain_tip,
                executor.endpoint,
                executor.trace,
                highest_sent,
                attempts_remaining,
            )
            .await
        }
        .boxed()
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn drive_block_sync_actions<ReadState, BlockVerifier>(
    mut actions: mpsc::Receiver<BlockSyncAction>,
    // Retained so the disconnect capability stays wired into the driver, even
    // though peer scoring no longer drives disconnects (misbehavior is record-only).
    _supervisor: zebra_network::zakura::ZakuraSupervisorHandle,
    endpoint: Option<ZakuraEndpoint>,
    block_sync: BlockSyncHandle,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    read_state: ReadState,
    block_verifier: BlockVerifier,
    max_checkpoint_height: block::Height,
    checkpoint_apply_limit: usize,
    full_apply_limit: usize,
    combined_apply_limit: usize,
    trace: ZakuraTrace,
    throughput_probe: Option<BlocksyncThroughputProbe>,
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
    BlockVerifier:
        Service<zebra_consensus::Request, Response = block::Hash> + Clone + Send + Sync + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
{
    pin!(shutdown);
    const {
        assert!(
            sync::MIN_CHECKPOINT_CONCURRENCY_LIMIT <= zebra_consensus::MAX_CHECKPOINT_HEIGHT_GAP
        );
    }
    let checkpoint_apply_limit = checkpoint_apply_limit.clamp(
        sync::MIN_CHECKPOINT_CONCURRENCY_LIMIT,
        zebra_consensus::MAX_CHECKPOINT_HEIGHT_GAP,
    );
    let full_apply_limit = full_apply_limit.max(sync::MIN_CONCURRENCY_LIMIT);
    let combined_apply_limit = combined_apply_limit.max(sync::MIN_CONCURRENCY_LIMIT);
    let mut deferred_actions = VecDeque::new();
    let apply_executor = ZebradBlockApplyExecutor::new(
        latest_chain_tip.clone(),
        endpoint.clone(),
        read_state.clone(),
        block_verifier.clone(),
        max_checkpoint_height,
        trace.clone(),
        throughput_probe.clone(),
    );
    let apply_limits = if throughput_probe.is_some() {
        BlockApplyLimits::single()
    } else {
        BlockApplyLimits {
            checkpoint_apply_limit,
            full_apply_limit,
            combined_apply_limit,
        }
    };
    let _ = block_sync.install_block_apply_executor(BlockApplyExecutorPort::with_limits(
        Arc::new(apply_executor),
        apply_limits,
    ));

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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_block_sync_body<BlockVerifier, ReadState>(
    block_verifier: BlockVerifier,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    endpoint: Option<ZakuraEndpoint>,
    read_state: ReadState,
    _block_sync: BlockSyncHandle,
    token: BlockApplyToken,
    block: Arc<block::Block>,
    class: BlockApplyClass,
    trace: ZakuraTrace,
    throughput_probe: Option<BlocksyncThroughputProbe>,
) -> BlockApplyOutput
where
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
    apply_block_sync_body_to_output(
        block_verifier,
        latest_chain_tip,
        endpoint,
        read_state,
        token,
        block,
        class,
        trace.clone(),
        throughput_probe,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn apply_block_sync_body_to_output<BlockVerifier, ReadState>(
    block_verifier: BlockVerifier,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    endpoint: Option<ZakuraEndpoint>,
    read_state: ReadState,
    token: BlockApplyToken,
    block: Arc<block::Block>,
    class: BlockApplyClass,
    trace: ZakuraTrace,
    throughput_probe: Option<BlocksyncThroughputProbe>,
) -> BlockApplyOutput
where
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
        return BlockApplyOutput {
            token,
            height: block::Height(0),
            hash: expected_hash,
            result: BlockApplyResult::Rejected,
            local_frontier: None,
        };
    };

    emit_commit_state(&trace, cs_trace::COMMIT_START, "block_sync_driver", |row| {
        insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
        insert_cs_str(row, cs_trace::APPLY_CLASS, block_apply_class_label(class));
        insert_cs_height(row, cs_trace::HEIGHT, height);
        insert_cs_hash(row, cs_trace::HASH, expected_hash);
    });
    let started = Instant::now();
    // Throughput-probe mode (debug only): skip consensus verify+commit and
    // advance an in-memory synthetic frontier instead, discarding the body. In
    // normal mode the frontier comes from re-reading committed state below.
    let (result, probe_frontier) = match throughput_probe.as_ref() {
        Some(probe) => probe.apply_block(block.as_ref()),
        None => (
            commit_block_sync_body_with_stall_trace(
                block_verifier.clone(),
                block,
                class,
                &trace,
                token,
                height,
                expected_hash,
            )
            .await,
            None,
        ),
    };
    emit_commit_state(
        &trace,
        cs_trace::COMMIT_FINISH,
        "block_sync_driver",
        |row| {
            insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
            insert_cs_str(row, cs_trace::APPLY_CLASS, block_apply_class_label(class));
            insert_cs_height(row, cs_trace::HEIGHT, height);
            insert_cs_hash(row, cs_trace::HASH, expected_hash);
            insert_cs_str(row, cs_trace::RESULT, block_apply_result_label(result));
            insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
        },
    );
    emit_commit_state(
        &trace,
        cs_trace::FRONTIER_QUERY_START,
        "block_sync_driver",
        |row| {
            insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
            insert_cs_height(row, cs_trace::HEIGHT, height);
            insert_cs_hash(row, cs_trace::HASH, expected_hash);
        },
    );
    let local_frontier = match throughput_probe.as_ref() {
        Some(_) => probe_frontier,
        None => query_block_sync_frontiers(read_state.clone(), latest_chain_tip.clone()).await,
    };
    if let Some(frontiers) = local_frontier {
        let change =
            if result == BlockApplyResult::Committed || result == BlockApplyResult::Duplicate {
                FrontierChange::VerifiedGrow
            } else {
                FrontierChange::Snapshot
            };
        if class == BlockApplyClass::Full || change != FrontierChange::VerifiedGrow {
            publish_body_frontier(endpoint.as_ref(), frontiers, change);
        }
    }
    emit_commit_state(
        &trace,
        cs_trace::FRONTIER_QUERY_FINISH,
        "block_sync_driver",
        |row| {
            insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
            insert_cs_height(row, cs_trace::HEIGHT, height);
            insert_cs_hash(row, cs_trace::HASH, expected_hash);
            insert_cs_bool(row, cs_trace::LOCAL_FRONTIER, local_frontier.is_some());
            if let Some(frontiers) = &local_frontier {
                insert_cs_frontiers(row, frontiers);
            }
        },
    );

    BlockApplyOutput {
        token,
        height,
        hash: expected_hash,
        result,
        local_frontier,
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
async fn commit_block_sync_body_with_stall_trace<BlockVerifier>(
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

async fn refresh_block_sync_frontiers_for_checkpoint_window<ReadState>(
    read_state: ReadState,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    endpoint: Option<ZakuraEndpoint>,
    trace: ZakuraTrace,
    highest_sent: block::Height,
    attempts_remaining: usize,
) -> Option<zebra_network::zakura::BlockSyncFrontiers>
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
    emit_commit_state(
        &trace,
        cs_trace::CHECKPOINT_REFRESH_ATTEMPT,
        "block_sync_driver",
        |row| {
            insert_cs_u64(row, "attempts_remaining", attempts_remaining as u64);
            insert_cs_height(row, cs_trace::VERIFIED_BLOCK_TIP, highest_sent);
        },
    );
    let frontiers =
        query_block_sync_frontiers(read_state.clone(), latest_chain_tip.clone()).await?;

    if frontiers.verified_block_tip <= highest_sent {
        return None;
    }

    publish_body_frontier(endpoint.as_ref(), frontiers, FrontierChange::VerifiedGrow);
    emit_commit_state(
        &trace,
        cs_trace::CHECKPOINT_REFRESH_SENT,
        "block_sync_driver",
        |row| {
            insert_cs_frontiers(row, &frontiers);
        },
    );
    Some(frontiers)
}

fn publish_body_frontier(
    endpoint: Option<&ZakuraEndpoint>,
    frontiers: zebra_network::zakura::BlockSyncFrontiers,
    change: FrontierChange,
) {
    let Some(endpoint) = endpoint else {
        return;
    };
    let Some(mut update) = endpoint.current_sync_frontier() else {
        return;
    };
    if frontiers.finalized_height == frontiers.verified_block_tip {
        update.frontier.finalized =
            Frontier::new(frontiers.finalized_height, frontiers.verified_block_hash);
    }
    update.frontier.verified_body =
        Frontier::new(frontiers.verified_block_tip, frontiers.verified_block_hash);
    update.change = change;
    endpoint.publish_sync_frontier_from(update, "block_sync_driver");
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

fn block_apply_class_label(class: BlockApplyClass) -> &'static str {
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
