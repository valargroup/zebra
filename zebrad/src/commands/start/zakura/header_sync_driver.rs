use std::future::Future;

use color_eyre::eyre::{eyre, Report};
use tokio::{pin, select, sync::mpsc};
use tower::{Service, ServiceExt};
use tracing::{debug, warn};

use zebra_chain::{
    block::{self},
    chain_tip::ChainTip,
};
use zebra_network::zakura::{
    BlockSyncEvent, BlockSyncFrontiers, BlockSyncHandle, HeaderSyncAction,
    HeaderSyncCommitFailureKind, HeaderSyncEvent, HeaderSyncFrontiers, ZakuraEndpoint,
    ZakuraHeaderSyncDriverStartup, DEFAULT_HS_RANGE,
};

use super::{block_verify_error_is_duplicate, verified_block_tip_from_state};

pub(crate) async fn zakura_header_sync_driver_startup(
    read_state: zebra_state::ReadStateService,
    network: &zebra_chain::parameters::Network,
) -> Result<ZakuraHeaderSyncDriverStartup, Report> {
    let best_header_tip = match read_state
        .clone()
        .oneshot(zebra_state::ReadRequest::BestHeaderTip)
        .await
        .map_err(|error| eyre!("{error}"))?
    {
        zebra_state::ReadResponse::BestHeaderTip(tip) => tip,
        response => Err(eyre!("unexpected BestHeaderTip response: {response:?}"))?,
    };

    let finalized_tip = match read_state
        .clone()
        .oneshot(zebra_state::ReadRequest::FinalizedTip)
        .await
        .map_err(|error| eyre!("{error}"))?
    {
        zebra_state::ReadResponse::FinalizedTip(tip) => tip,
        response => Err(eyre!("unexpected FinalizedTip response: {response:?}"))?,
    };

    let verified_block_tip = match read_state
        .oneshot(zebra_state::ReadRequest::Tip)
        .await
        .map_err(|error| eyre!("{error}"))?
    {
        zebra_state::ReadResponse::Tip(tip) => tip,
        response => Err(eyre!("unexpected Tip response: {response:?}"))?,
    };

    let empty_state_tip = (block::Height(0), network.genesis_hash());
    let finalized_height = finalized_tip.map_or(block::Height(0), |(height, _)| height);
    let verified_block_tip =
        verified_block_tip_from_state(finalized_tip, verified_block_tip, empty_state_tip);
    Ok(ZakuraHeaderSyncDriverStartup {
        frontiers: HeaderSyncFrontiers {
            finalized_height,
            verified_block_tip: verified_block_tip.0,
        },
        best_header_tip: Some(best_header_tip.unwrap_or(empty_state_tip)),
        verified_block_tip_hash: verified_block_tip.1,
    })
}

#[derive(Clone)]
pub(crate) struct ZakuraHeaderSyncDriverHandles {
    pub(crate) endpoint: ZakuraEndpoint,
    pub(crate) header_sync: zebra_network::zakura::HeaderSyncHandle,
    pub(crate) block_sync: Option<BlockSyncHandle>,
}

pub(crate) async fn drive_zakura_header_sync_actions<State, ReadState, BlockVerifier>(
    mut actions: mpsc::Receiver<HeaderSyncAction>,
    handles: ZakuraHeaderSyncDriverHandles,
    state: State,
    read_state: ReadState,
    block_verifier: BlockVerifier,
    shutdown: impl Future<Output = ()> + Send + 'static,
) where
    State: Service<
            zebra_state::Request,
            Response = zebra_state::Response,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + 'static,
    State::Future: Send + 'static,
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
    loop {
        let action = select! {
            _ = &mut shutdown => return,
            action = actions.recv() => {
                let Some(action) = action else {
                    return;
                };
                action
            }
        };

        match action {
            HeaderSyncAction::Misbehavior { peer, reason } => {
                debug!(
                    ?peer,
                    ?reason,
                    "disconnecting peer for Zakura header-sync violation"
                );
                let _ = handles.endpoint.supervisor().disconnect_peer(&peer).await;
            }
            HeaderSyncAction::NewBlockReceived {
                peer,
                height,
                hash,
                block,
            } => {
                match block_verifier
                    .clone()
                    .oneshot(zebra_consensus::Request::Commit(block.clone()))
                    .await
                {
                    Ok(committed_hash) if committed_hash == hash => {
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::NewBlockAccepted {
                                peer,
                                height,
                                hash,
                                block,
                            })
                            .await;
                    }
                    Ok(committed_hash) => {
                        warn!(
                            ?peer,
                            ?hash,
                            ?committed_hash,
                            "Zakura NewBlock verifier returned an unexpected hash"
                        );
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::NewBlockRejected { peer, hash })
                            .await;
                    }
                    Err(error) => {
                        if block_verify_error_is_duplicate(&error) {
                            debug!(
                                ?peer,
                                ?height,
                                ?hash,
                                ?error,
                                "Zakura NewBlock was already known by the block verifier"
                            );
                            let _ = handles
                                .header_sync
                                .send(HeaderSyncEvent::NewBlockDuplicate { peer, height, hash })
                                .await;
                            continue;
                        }

                        debug!(
                            ?peer,
                            ?hash,
                            ?error,
                            "Zakura NewBlock rejected by block verifier"
                        );
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::NewBlockRejected { peer, hash })
                            .await;
                    }
                }
            }
            HeaderSyncAction::QueryHeadersByHeightRange { peer, start, count } => {
                match read_state
                    .clone()
                    .oneshot(zebra_state::ReadRequest::HeadersByHeightRange { start, count })
                    .await
                {
                    Ok(zebra_state::ReadResponse::Headers(headers)) => {
                        let body_size_hints = match read_state
                            .clone()
                            .oneshot(zebra_state::ReadRequest::BlockSizeHints {
                                from: start,
                                count,
                            })
                            .await
                        {
                            Ok(zebra_state::ReadResponse::BlockSizeHints(hints)) => hints,
                            Ok(response) => {
                                warn!(?peer, ?response, "unexpected BlockSizeHints response");
                                Vec::new()
                            }
                            Err(error) => {
                                warn!(
                                    ?peer,
                                    ?error,
                                    "failed to read Zakura BlockSizeHints response from state"
                                );
                                Vec::new()
                            }
                        };
                        let body_sizes = body_sizes_for_served_header_range(
                            start,
                            headers.iter().map(|(height, _, _)| *height),
                            &body_size_hints,
                        );
                        let headers = headers
                            .into_iter()
                            .map(|(_height, _hash, header)| header)
                            .collect();
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::HeaderRangeResponseReady {
                                peer,
                                start_height: start,
                                requested_count: count,
                                headers,
                                body_sizes,
                            })
                            .await;
                    }
                    Ok(response) => {
                        warn!(?peer, ?response, "unexpected HeadersByHeightRange response");
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::HeaderRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            })
                            .await;
                    }
                    Err(error) => {
                        warn!(
                            ?peer,
                            ?error,
                            "failed to read Zakura Headers response from state"
                        );
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::HeaderRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            })
                            .await;
                    }
                }
            }
            HeaderSyncAction::CommitHeaderRange {
                peer,
                anchor,
                start_height,
                headers,
                body_sizes,
                finalized: _finalized,
            } => {
                let count = u32::try_from(headers.len()).unwrap_or(u32::MAX);
                match state
                    .clone()
                    .oneshot(zebra_state::Request::CommitHeaderRange {
                        anchor,
                        headers,
                        body_sizes,
                    })
                    .await
                {
                    Ok(zebra_state::Response::Committed(tip_hash)) => {
                        let tip_height =
                            block::Height(start_height.0.saturating_add(count.saturating_sub(1)));
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::HeaderRangeCommitted {
                                start_height,
                                tip_height,
                                tip_hash,
                            })
                            .await;
                        notify_block_sync_header_tip(
                            handles.block_sync.as_ref(),
                            tip_height,
                            tip_hash,
                        )
                        .await;
                    }
                    Ok(response) => {
                        warn!(?peer, ?response, "unexpected CommitHeaderRange response");
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::HeaderRangeCommitFailed {
                                peer,
                                start_height,
                                count,
                                kind: HeaderSyncCommitFailureKind::Local,
                            })
                            .await;
                    }
                    Err(error) => {
                        let kind = header_range_commit_failure_kind(error.as_ref());
                        debug!(
                            ?peer,
                            ?start_height,
                            ?count,
                            ?kind,
                            ?error,
                            "Zakura header range commit failed"
                        );
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::HeaderRangeCommitFailed {
                                peer,
                                start_height,
                                count,
                                kind,
                            })
                            .await;
                    }
                }
            }
            HeaderSyncAction::QueryBestHeaderTip => {
                match read_state
                    .clone()
                    .oneshot(zebra_state::ReadRequest::BestHeaderTip)
                    .await
                {
                    Ok(zebra_state::ReadResponse::BestHeaderTip(Some((tip_height, tip_hash)))) => {
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::HeaderRangeCommitted {
                                start_height: tip_height,
                                tip_height,
                                tip_hash,
                            })
                            .await;
                        notify_block_sync_header_tip(
                            handles.block_sync.as_ref(),
                            tip_height,
                            tip_hash,
                        )
                        .await;
                    }
                    Ok(zebra_state::ReadResponse::BestHeaderTip(None)) => {}
                    Ok(response) => warn!(?response, "unexpected BestHeaderTip response"),
                    Err(error) => warn!(?error, "failed to query Zakura best header tip"),
                }
            }
            HeaderSyncAction::QueryMissingBlockBodies { from, limit } => {
                log_missing_block_bodies(read_state.clone(), from, limit).await;
            }
            HeaderSyncAction::BodyGaps { from, to } => {
                let limit =
                    to.0.saturating_sub(from.0)
                        .saturating_add(1)
                        .min(DEFAULT_HS_RANGE);
                log_missing_block_bodies(read_state.clone(), from, limit).await;
            }
        }
    }
}

pub(crate) async fn notify_block_sync_header_tip(
    block_sync: Option<&BlockSyncHandle>,
    height: block::Height,
    hash: block::Hash,
) {
    if let Some(block_sync) = block_sync {
        let _ = block_sync
            .send(BlockSyncEvent::HeaderTipChanged { height, hash })
            .await;
    }
}

pub(crate) fn body_sizes_for_served_header_range(
    start: block::Height,
    header_heights: impl IntoIterator<Item = block::Height>,
    body_size_hints: &[(block::Height, Option<u32>)],
) -> Vec<u32> {
    header_heights
        .into_iter()
        .map(|height| {
            let Some(offset) = usize::try_from(height - start).ok() else {
                return 0;
            };

            body_size_hints
                .get(offset)
                .and_then(|(hint_height, size)| {
                    (*hint_height == height).then_some(size.unwrap_or(0))
                })
                .unwrap_or(0)
        })
        .collect()
}

async fn log_missing_block_bodies<ReadState>(read_state: ReadState, from: block::Height, limit: u32)
where
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    match read_state
        .oneshot(zebra_state::ReadRequest::MissingBlockBodies { from, limit })
        .await
    {
        Ok(zebra_state::ReadResponse::MissingBlockBodies(heights)) => {
            let first = heights.first().copied();
            let last = heights.last().copied();
            let count = heights.len();
            debug!(
                ?from,
                ?limit,
                ?count,
                ?first,
                ?last,
                "Zakura header-known body gaps from state"
            );
        }
        Ok(response) => warn!(?response, "unexpected MissingBlockBodies response"),
        Err(error) => warn!(?error, "failed to query Zakura missing block bodies"),
    }
}

pub(crate) fn header_range_commit_failure_kind(
    error: &(dyn std::error::Error + Send + Sync + 'static),
) -> HeaderSyncCommitFailureKind {
    let Some(error) = error.downcast_ref::<zebra_state::CommitHeaderRangeError>() else {
        return HeaderSyncCommitFailureKind::Local;
    };

    match error {
        zebra_state::CommitHeaderRangeError::StorageWriteError { .. }
        | zebra_state::CommitHeaderRangeError::MissingGenesisAnchor { .. }
        | zebra_state::CommitHeaderRangeError::SendCommitRequestFailed
        | zebra_state::CommitHeaderRangeError::CommitResponseDropped => {
            HeaderSyncCommitFailureKind::Local
        }
        zebra_state::CommitHeaderRangeError::EmptyRange
        | zebra_state::CommitHeaderRangeError::RangeTooLong { .. }
        | zebra_state::CommitHeaderRangeError::UnknownAnchor { .. }
        | zebra_state::CommitHeaderRangeError::HeightOverflow
        | zebra_state::CommitHeaderRangeError::ImmutableConflict { .. }
        | zebra_state::CommitHeaderRangeError::ReorgTooDeep { .. }
        | zebra_state::CommitHeaderRangeError::CheckpointConflict { .. }
        | zebra_state::CommitHeaderRangeError::ConflictingFullBlockHeader { .. }
        | zebra_state::CommitHeaderRangeError::ValidateContextError(_) => {
            HeaderSyncCommitFailureKind::InvalidPeerRange
        }
        _ => HeaderSyncCommitFailureKind::Local,
    }
}

pub(crate) async fn mirror_zakura_full_block_commits<ReadState>(
    mut chain_tip_change: zebra_state::ChainTipChange,
    latest_chain_tip: zebra_state::LatestChainTip,
    read_state: ReadState,
    header_sync: zebra_network::zakura::HeaderSyncHandle,
    block_sync: Option<BlockSyncHandle>,
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
        let action = select! {
            _ = &mut shutdown => return,
            action = chain_tip_change.wait_for_tip_change() => {
                let Ok(action) = action else {
                    return;
                };
                action
            }
        };
        let height = action.best_tip_height();
        let hash = action.best_tip_hash();

        let finalized_tip = match read_state
            .clone()
            .oneshot(zebra_state::ReadRequest::FinalizedTip)
            .await
        {
            Ok(zebra_state::ReadResponse::FinalizedTip(tip)) => tip,
            Ok(response) => {
                warn!(?response, "unexpected FinalizedTip response");
                None
            }
            Err(error) => {
                warn!(?error, "failed to query Zakura finalized frontier");
                None
            }
        };
        let finalized_height = finalized_tip.map_or(block::Height(0), |(height, _)| height);
        let action_tip = Some((height, hash));
        let verified_block_tip =
            verified_block_tip_from_state(finalized_tip, action_tip, (height, hash));
        let verified_block_tip = verified_block_tip_from_state(
            Some(verified_block_tip),
            latest_chain_tip.best_tip_height_and_hash(),
            verified_block_tip,
        );

        let _ = header_sync
            .send(HeaderSyncEvent::StateFrontiersChanged(
                HeaderSyncFrontiers {
                    finalized_height,
                    verified_block_tip: verified_block_tip.0,
                },
            ))
            .await;
        if let Some(block_sync) = &block_sync {
            let frontiers = BlockSyncFrontiers {
                finalized_height,
                verified_block_tip: verified_block_tip.0,
                verified_block_hash: verified_block_tip.1,
            };
            let _ = block_sync
                .send(block_sync_chain_tip_event(&action, frontiers))
                .await;
        }

        match read_state
            .clone()
            .oneshot(zebra_state::ReadRequest::Block(hash.into()))
            .await
        {
            Ok(zebra_state::ReadResponse::Block(Some(block))) => {
                let _ = header_sync
                    .send(HeaderSyncEvent::FullBlockCommitted {
                        height,
                        hash,
                        header: block.header.clone(),
                    })
                    .await;
            }
            Ok(zebra_state::ReadResponse::Block(None)) => {
                debug!(
                    ?height,
                    ?hash,
                    "Zakura full-block mirror could not find committed tip block"
                );
            }
            Ok(response) => warn!(?response, "unexpected block lookup response"),
            Err(error) => warn!(?error, "failed to mirror Zakura full-block commit"),
        }
    }
}

pub(crate) fn block_sync_chain_tip_event(
    action: &zebra_state::TipAction,
    frontiers: BlockSyncFrontiers,
) -> BlockSyncEvent {
    match action {
        zebra_state::TipAction::Grow { .. } => BlockSyncEvent::ChainTipGrow(frontiers),
        zebra_state::TipAction::Reset { .. } => BlockSyncEvent::ChainTipReset(frontiers),
    }
}
