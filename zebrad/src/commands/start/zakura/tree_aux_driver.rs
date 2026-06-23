//! The `tree_aux` peer-source driver and serving port (verified-commitment-trees POC).
//!
//! Serving side: [`StateTreeAuxPort`] answers inbound `GetRoots` from local state via
//! [`ReadRequest::BlockRoots`]. Client side: [`run_tree_aux_driver`] fetches the per-block
//! commitment roots for the verified-tip→checkpoint range from peers, then publishes the
//! complete range into the committer's cache ([`TreeAuxRootsWriter`]) *ahead of* body commit.
//! The handoff frontier is embedded in the binary, so only roots travel over the wire.
//!
//! Runs when the state committer exposes a `tree_aux` roots writer. On Mainnet this
//! is the default fast path under checkpoint sync; `consensus.checkpoint_sync = false`
//! opts out to the legacy recompute path.

use std::time::Duration;

use tower::{Service, ServiceExt};
use zebra_chain::{block, parallel::commitment_aux::BlockCommitmentRoots, parameters::Network};
use zebra_network::zakura::{
    fetch_roots, BoxRunFuture, TreeAuxStatePort, ZakuraSupervisorHandle, MAX_TA_ROOTS_PER_REQUEST,
};
use zebra_state::{BoxError, ReadRequest, ReadResponse, ReadStateService, TreeAuxRootsWriter};

use super::frontier::verified_block_tip_from_state;

/// Delay between driver attempts while waiting for a peer, or after a fetch error.
const TREE_AUX_DRIVER_RETRY: Duration = Duration::from_secs(5);

/// Number of peer-root request batches the driver may keep ahead of committed state.
const TREE_AUX_FETCH_AHEAD_BATCHES: u32 = 16;

/// Maximum speculative peer roots held in the committer cache ahead of finalized progress.
const TREE_AUX_FETCH_AHEAD_ROOTS: u32 = MAX_TA_ROOTS_PER_REQUEST * TREE_AUX_FETCH_AHEAD_BATCHES;

/// Serves inbound `tree_aux` `GetRoots` from local finalized state, through the read
/// service ([`ReadRequest::BlockRoots`]). An archive/produced node serves the roots it
/// derives from its per-height trees; a fast-synced node holds no per-height trees and so
/// returns an empty (unavailable) range, which the wire layer reports as `RangeUnavailable`.
///
/// Generic over the read service so the mapping is unit-testable with a mock; production
/// uses the default [`ReadStateService`].
pub(crate) struct StateTreeAuxPort<S = ReadStateService> {
    read_state: S,
}

impl<S> StateTreeAuxPort<S> {
    pub(crate) fn new(read_state: S) -> Self {
        Self { read_state }
    }
}

impl<S> TreeAuxStatePort for StateTreeAuxPort<S>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    fn read_block_roots(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> BoxRunFuture<'static, Vec<BlockCommitmentRoots>> {
        // Clone the buffered read service before the await (the Tower clone-before-call
        // pattern); the trait read is `&self`, so each request drives its own clone.
        let mut read_state = self.read_state.clone();
        Box::pin(async move {
            let ready = match read_state.ready().await {
                Ok(ready) => ready,
                Err(error) => {
                    tracing::debug!(?error, "tree_aux serve: read service not ready");
                    return Vec::new();
                }
            };
            match ready
                .call(ReadRequest::BlockRoots {
                    start_height,
                    count,
                })
                .await
            {
                Ok(ReadResponse::BlockRoots(roots)) => roots,
                // An unavailable range (fast-synced node, pruned, or wrong response) is an
                // empty serve, never wrong data — the client treats it as unavailable.
                Ok(_) => Vec::new(),
                Err(error) => {
                    tracing::debug!(?error, "tree_aux serve: BlockRoots read failed");
                    Vec::new()
                }
            }
        })
    }
}

/// Fetch and refetch verified-tip→checkpoint per-block roots from peers into the committer cache.
///
/// Once an outbound peer is available, fetch the roots the committer still needs — the range from
/// this node's verified tip up to the checkpoint — in bounded windows tied to committed progress.
/// Each window is published to `writer` only after that window has been fetched completely. The
/// driver then stays alive for targeted root-refetch requests from the committer: a frozen-frontier
/// root miss parks the checkpoint block, asks this driver to refill that height from peers, and
/// retries the same commit without resetting the block queue.
///
/// The fetch starts at `verified_tip + 1`, not genesis: heights at or below the verified tip are
/// already committed, so their roots are never looked up. Fetching from genesis on a node that
/// starts above it (e.g. a snapshot) would spend the whole fetch on already-committed heights and
/// never cache the window roots before the committer reaches them — every block would then fall
/// back to legacy recompute.
pub(crate) async fn run_tree_aux_driver(
    supervisor: ZakuraSupervisorHandle,
    writer: TreeAuxRootsWriter,
    network: Network,
    read_state: ReadStateService,
    shutdown: impl std::future::Future<Output = ()>,
) {
    let handoff = network.checkpoint_list().max_height();

    // The first root the committer still needs: one above the current verified tip. Read once at
    // startup; the fetched range stays a superset of what the committer will commit even if it
    // advances meanwhile (extra cached roots below its position are harmless).
    let finalized_tip = read_state_tip(&read_state, ReadRequest::FinalizedTip).await;
    let tip = read_state_tip(&read_state, ReadRequest::Tip).await;
    let from = root_fetch_start(finalized_tip, tip, &network);

    let mut refetch_rx = Some(writer.subscribe_refetch());

    let driver = async {
        let mut initial_fetch_complete = false;
        let mut next_fetch = from;

        loop {
            // Wait for an outbound peer before issuing requests (fetch_roots needs one).
            if supervisor.outbound_peer_handles().await.is_empty() {
                tokio::time::sleep(TREE_AUX_DRIVER_RETRY).await;
                continue;
            }

            if !initial_fetch_complete {
                if next_fetch > handoff {
                    tracing::info!(
                        from_height = from.0,
                        handoff_height = handoff.0,
                        "tree_aux: fetched verified-tip→checkpoint roots from peer into the committer cache"
                    );
                    initial_fetch_complete = true;
                    continue;
                }

                let committed_through = writer.committed_through();
                if let Some(committed_height) = committed_through {
                    if next_fetch <= committed_height {
                        let Some(next_height) = committed_height.0.checked_add(1) else {
                            initial_fetch_complete = true;
                            continue;
                        };
                        next_fetch = block::Height(next_height);
                        if next_fetch > handoff {
                            continue;
                        }
                    }
                }

                let Some((window_from, window_to)) = next_fetch_window(
                    from,
                    next_fetch,
                    handoff,
                    committed_through,
                    TREE_AUX_FETCH_AHEAD_ROOTS,
                ) else {
                    if let Some(rx) = &mut refetch_rx {
                        let mut refetch_closed = false;
                        tokio::select! {
                            _ = tokio::time::sleep(TREE_AUX_DRIVER_RETRY) => {}
                            request = rx.recv() => {
                                refetch_closed = handle_refetch_request(request, &supervisor, &writer)
                                    .await
                                    .is_err();
                            }
                        }
                        if refetch_closed {
                            refetch_rx = None;
                        }
                    } else {
                        tokio::time::sleep(TREE_AUX_DRIVER_RETRY).await;
                    }
                    continue;
                };

                let (result, refetch_closed) = if let Some(rx) = &mut refetch_rx {
                    let mut refetch_closed = false;
                    let result = tokio::select! {
                        result = fetch_roots_into_writer(&supervisor, &writer, window_from, window_to) => {
                            Some(result)
                        }
                        request = rx.recv() => {
                            refetch_closed = handle_refetch_request(request, &supervisor, &writer)
                                .await
                                .is_err();
                            None
                        }
                    };
                    (result, refetch_closed)
                } else {
                    (
                        Some(
                            fetch_roots_into_writer(&supervisor, &writer, window_from, window_to)
                                .await,
                        ),
                        false,
                    )
                };
                if refetch_closed {
                    refetch_rx = None;
                }

                let Some(result) = result else {
                    continue;
                };

                match result {
                    Ok(()) => {
                        tracing::debug!(
                            from_height = window_from.0,
                            to_height = window_to.0,
                            committed_through = ?writer.committed_through(),
                            "tree_aux: fetched bounded peer-root window into the committer cache"
                        );
                        if window_to >= handoff {
                            initial_fetch_complete = true;
                            tracing::info!(
                                from_height = from.0,
                                handoff_height = handoff.0,
                                "tree_aux: fetched verified-tip→checkpoint roots from peer into the committer cache"
                            );
                        } else {
                            next_fetch = block::Height(
                                window_to
                                    .0
                                    .checked_add(1)
                                    .expect("window end is below handoff, so next height exists"),
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(?error, "tree_aux: root fetch failed, retrying");
                        tokio::time::sleep(TREE_AUX_DRIVER_RETRY).await;
                    }
                }

                continue;
            }

            let Some(rx) = &mut refetch_rx else {
                tokio::time::sleep(TREE_AUX_DRIVER_RETRY).await;
                continue;
            };

            if handle_refetch_request(rx.recv().await, &supervisor, &writer)
                .await
                .is_err()
            {
                break;
            }
        }
    };

    tokio::pin!(shutdown);
    tokio::select! {
        _ = driver => {}
        _ = &mut shutdown => {
            tracing::info!("tree_aux driver shutting down");
        }
    }
}

/// Return the next bounded fetch window, if committed progress has opened one.
fn next_fetch_window(
    from: block::Height,
    next_fetch: block::Height,
    handoff: block::Height,
    committed_through: Option<block::Height>,
    fetch_ahead_roots: u32,
) -> Option<(block::Height, block::Height)> {
    if next_fetch > handoff {
        return None;
    }

    let window_end = fetch_window_end(from, handoff, committed_through, fetch_ahead_roots);
    (next_fetch <= window_end).then_some((next_fetch, window_end))
}

/// Highest root the driver may fetch while staying within the fetch-ahead cap.
fn fetch_window_end(
    from: block::Height,
    handoff: block::Height,
    committed_through: Option<block::Height>,
    fetch_ahead_roots: u32,
) -> block::Height {
    let committed_floor = committed_through
        .map(|height| height.0)
        .unwrap_or_else(|| from.0.saturating_sub(1));

    block::Height(
        committed_floor
            .saturating_add(fetch_ahead_roots)
            .min(handoff.0),
    )
}

/// Fetch `[from, to]` roots and insert the complete range into the committer's peer cache.
///
/// This is the write side of the `tree_aux` source: `fetch_roots` validates the transport
/// response shape while staging each batch locally. Only a full-range success makes those roots
/// visible to the finalized committer. The roots are still untrusted here; the committer verifies
/// them against checkpoint headers.
async fn fetch_roots_into_writer(
    supervisor: &ZakuraSupervisorHandle,
    writer: &TreeAuxRootsWriter,
    from: block::Height,
    to: block::Height,
) -> Result<(), BoxError> {
    let mut staged = Vec::new();
    // Keep partial batches private: if the fetch errors or this future is cancelled by
    // `select!`, the staged prefix drops here without reaching `PeerSource`.
    let result = fetch_roots(supervisor, from, to, |batch| staged.extend(batch)).await;
    insert_staged_roots_after_success(staged, result, |roots| writer.insert_roots(roots))
}

/// Publish staged roots only if the fetch for their whole requested range succeeded.
fn insert_staged_roots_after_success<F>(
    staged: Vec<BlockCommitmentRoots>,
    result: Result<(), BoxError>,
    insert_roots: F,
) -> Result<(), BoxError>
where
    F: FnOnce(Vec<BlockCommitmentRoots>),
{
    result?;
    insert_roots(staged);
    Ok(())
}

/// Handle one targeted root-refetch request from the finalized committer.
///
/// A concrete height requests an immediate one-root fetch into `writer`. Lagged requests are
/// recoverable because later retry iterations can send the height again; a closed channel means
/// the peer-source committer is gone, so the driver can stop waiting for refetches.
async fn handle_refetch_request(
    request: Result<block::Height, tokio::sync::broadcast::error::RecvError>,
    supervisor: &ZakuraSupervisorHandle,
    writer: &TreeAuxRootsWriter,
) -> Result<(), ()> {
    handle_refetch_request_with_fetch(request, |height| async move {
        fetch_roots_into_writer(supervisor, writer, height, height).await
    })
    .await
}

/// Testable core of [`handle_refetch_request`], with the peer fetch supplied by the caller.
async fn handle_refetch_request_with_fetch<F, Fut>(
    request: Result<block::Height, tokio::sync::broadcast::error::RecvError>,
    fetch_height: F,
) -> Result<(), ()>
where
    F: FnOnce(block::Height) -> Fut,
    Fut: std::future::Future<Output = Result<(), BoxError>>,
{
    match request {
        Ok(height) => {
            let result = fetch_height(height).await;
            match result {
                Ok(()) => tracing::info!(
                    ?height,
                    "tree_aux: fetched retryable missing root into the committer cache"
                ),
                Err(error) => tracing::warn!(
                    ?height,
                    ?error,
                    "tree_aux: retryable missing root fetch failed"
                ),
            }
            Ok(())
        }
        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
            tracing::warn!(
                skipped,
                "tree_aux: missed root refetch requests, continuing with latest requests"
            );
            Ok(())
        }
        Err(tokio::sync::broadcast::error::RecvError::Closed) => Err(()),
    }
}

/// The first height the committer still needs roots for: one above the node's verified tip
/// (`max(finalized_tip, best_tip)`). Heights at or below the verified tip are already committed, so
/// their roots are never looked up — re-fetching them is wasted and, on a node that starts well
/// above genesis, starves the roots it actually needs. Returns `Height(1)` only for a genesis-empty
/// node, i.e. "fetch from genesis" falls out exactly when the node really is at genesis.
fn root_fetch_start(
    finalized_tip: Option<(block::Height, block::Hash)>,
    tip: Option<(block::Height, block::Hash)>,
    network: &Network,
) -> block::Height {
    let empty_state_tip = (block::Height(0), network.genesis_hash());
    let (verified_tip, _) = verified_block_tip_from_state(finalized_tip, tip, empty_state_tip);
    block::Height(verified_tip.0.saturating_add(1))
}

/// Read a finalized/verified tip height+hash via a one-shot state read, returning `None` on any
/// error or unexpected response (the caller then treats the tip as the empty-state genesis).
async fn read_state_tip(
    read_state: &ReadStateService,
    request: ReadRequest,
) -> Option<(block::Height, block::Hash)> {
    match read_state.clone().oneshot(request).await {
        Ok(ReadResponse::FinalizedTip(tip)) | Ok(ReadResponse::Tip(tip)) => tip,
        Ok(response) => {
            tracing::warn!(?response, "tree_aux: unexpected tip response");
            None
        }
        Err(error) => {
            tracing::warn!(?error, "tree_aux: failed to read tip for root-fetch start");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;

    use tower::service_fn;
    use zebra_chain::{orchard, sapling};

    use super::*;

    fn root_at(height: u32) -> BlockCommitmentRoots {
        BlockCommitmentRoots {
            height: block::Height(height),
            sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
            orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
        }
    }

    #[tokio::test]
    async fn port_returns_block_roots_from_the_read_service() {
        let held: Vec<_> = (5..8).map(root_at).collect();
        let served = held.clone();
        let read_state = service_fn(move |request: ReadRequest| {
            let served = served.clone();
            future::ready(match request {
                ReadRequest::BlockRoots { .. } => Ok(ReadResponse::BlockRoots(served)),
                other => Err(format!("unexpected request: {other:?}").into()),
            })
        });

        let port = StateTreeAuxPort::new(read_state);
        let roots = port.read_block_roots(block::Height(5), 3).await;

        assert_eq!(
            roots, held,
            "the port returns the roots the read service serves"
        );
    }

    #[tokio::test]
    async fn port_maps_read_errors_to_an_empty_serve() {
        // A read error must degrade to an empty (unavailable) serve, never a panic or
        // wrong data — the client treats an empty range as unavailable.
        let read_state = service_fn(|_request: ReadRequest| {
            future::ready(Err::<ReadResponse, BoxError>("read failed".into()))
        });

        let port = StateTreeAuxPort::new(read_state);
        let roots = port.read_block_roots(block::Height(5), 3).await;

        assert!(roots.is_empty(), "a failed read serves an empty range");
    }

    #[tokio::test]
    async fn port_maps_an_unexpected_response_to_an_empty_serve() {
        let read_state = service_fn(|_request: ReadRequest| {
            future::ready(Ok::<_, BoxError>(ReadResponse::ValidBlockProposal))
        });

        let port = StateTreeAuxPort::new(read_state);
        let roots = port.read_block_roots(block::Height(5), 3).await;

        assert!(
            roots.is_empty(),
            "a non-BlockRoots response serves an empty range, not wrong data"
        );
    }

    /// End-to-end serving integration: a real finalized state serves per-block roots
    /// through the production [`StateTreeAuxPort`] → `TreeAuxService` over the real
    /// loopback Zakura transport, and a peer's [`fetch_roots`] receives exactly what the
    /// state serves (`ReadRequest::BlockRoots`). This joins the serving stack and the
    /// wire on real state — the piece the in-crate unit tests (mock port / committer
    /// PeerSource) cannot cover. The committer consumption of these roots is covered by
    /// `vct_peer_source_filled_incrementally_drives_byte_identical_state` in `zebra-state`.
    #[tokio::test]
    async fn tree_aux_serves_real_state_roots_over_the_wire() -> Result<(), BoxError> {
        use std::time::Duration;

        use tower::ServiceExt;
        use zebra_chain::{block::Block, parameters::Network, serialization::ZcashDeserializeInto};
        use zebra_network::zakura::{testkit::ZakuraTestNode, TreeAuxService};
        use zebra_state::{populated_state, ReadResponse};

        let _guard = zebra_test::init();
        let network = Network::Mainnet;
        const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

        // A real finalized state with per-height trees: a contiguous mainnet prefix
        // (genesis onward) committed as checkpoint-verified blocks.
        let blocks: Vec<std::sync::Arc<Block>> = zebra_test::vectors::CONTINUOUS_MAINNET_BLOCKS
            .values()
            .map(|bytes| {
                std::sync::Arc::new(
                    bytes
                        .zcash_deserialize_into::<Block>()
                        .expect("test vector block deserializes"),
                )
            })
            .collect();
        let tip = (blocks.len() - 1) as u32;
        assert!(tip >= 2, "need a few blocks to serve a range");
        let (_state, read_state, _latest_tip, _change) =
            populated_state(blocks.clone(), &network).await;

        // The roots the state serves directly for [1, tip] — the expected truth.
        let count = tip;
        let ReadResponse::BlockRoots(expected) = read_state
            .clone()
            .oneshot(ReadRequest::BlockRoots {
                start_height: block::Height(1),
                count,
            })
            .await?
        else {
            panic!("expected a BlockRoots response");
        };
        assert!(
            !expected.is_empty(),
            "the populated state serves roots for the range"
        );

        // Two real Zakura nodes; the server serves roots from the real state via the
        // production port, the client only advertises the cap and requests.
        let server = ZakuraTestNode::builder(101)
            .max_connections_per_ip(16)
            .service(std::sync::Arc::new(TreeAuxService::new(
                std::sync::Arc::new(StateTreeAuxPort::new(read_state.clone())),
            )))
            .spawn()
            .await?;
        let client = ZakuraTestNode::builder(102)
            .max_connections_per_ip(16)
            .service(std::sync::Arc::new(TreeAuxService::new(
                std::sync::Arc::new(StateTreeAuxPort::new(read_state.clone())),
            )))
            .spawn()
            .await?;

        client.connect_native(&server, CONNECT_TIMEOUT).await?;

        let mut collected = Vec::new();
        fetch_roots(
            &client.supervisor(),
            block::Height(1),
            block::Height(tip),
            |batch| collected.extend(batch),
        )
        .await?;

        assert_eq!(
            collected, expected,
            "roots fetched over tree_aux match the roots the real state serves"
        );

        // Negative / safe-fallback: a range the server's state cannot serve (above the
        // tip) yields an error from the fetch, so the driver leaves that range un-fetched
        // and the committer keeps it on the legacy path — never wrong data.
        let mut unavailable = Vec::new();
        let result = fetch_roots(
            &client.supervisor(),
            block::Height(tip + 5),
            block::Height(tip + 10),
            |batch| unavailable.extend(batch),
        )
        .await;
        assert!(
            result.is_err(),
            "an unavailable range returns an error (the committer keeps it legacy)"
        );
        assert!(
            unavailable.is_empty(),
            "no roots are delivered for an unavailable range"
        );

        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    #[test]
    fn root_fetch_start_is_one_above_the_verified_tip() {
        let net = Network::Mainnet;
        let h = block::Height;
        // The hash is irrelevant to the height math; reuse the genesis hash.
        let at = |n| Some((h(n), net.genesis_hash()));

        // Genesis-empty node: fetch from height 1 — the only case where "from genesis" is correct.
        assert_eq!(root_fetch_start(None, None, &net), h(1));

        // Snapshot node at T (finalized == best tip): fetch from T + 1, NOT genesis. This is the
        // regression this guards: the old driver hard-coded Height(1), refetching ~T already-held
        // roots and starving the window the committer actually needs.
        assert_eq!(
            root_fetch_start(at(3_338_006), at(3_338_006), &net),
            h(3_338_007)
        );

        // Non-finalized blocks above the finalized tip: the verified tip is the higher best tip.
        assert_eq!(root_fetch_start(at(100), at(105), &net), h(106));

        // Finalized tip above the best-chain tip: use the finalized tip.
        assert_eq!(root_fetch_start(at(200), at(150), &net), h(201));
    }

    #[test]
    fn fetch_window_starts_ahead_of_initial_tip() {
        let h = block::Height;

        assert_eq!(
            fetch_window_end(h(10), h(100), None, 16),
            h(25),
            "without a commit watermark, the window starts one below the initial fetch height"
        );
        assert_eq!(
            next_fetch_window(h(10), h(10), h(100), None, 16),
            Some((h(10), h(25))),
            "the initial fetch fills only the bounded fetch-ahead window"
        );
    }

    #[test]
    fn fetch_window_waits_until_commits_open_room() {
        let h = block::Height;

        assert_eq!(
            next_fetch_window(h(10), h(26), h(100), None, 16),
            None,
            "a full fetch-ahead window blocks further prefetch"
        );
        assert_eq!(
            next_fetch_window(h(10), h(26), h(100), Some(h(10)), 16),
            Some((h(26), h(26))),
            "committing one height opens room for one more fetched root"
        );
        assert_eq!(
            next_fetch_window(h(10), h(27), h(100), Some(h(20)), 16),
            Some((h(27), h(36))),
            "additional commit progress opens a larger contiguous window"
        );
    }

    #[test]
    fn fetch_window_does_not_go_behind_next_fetch() {
        let h = block::Height;

        assert_eq!(
            next_fetch_window(h(100), h(100), h(200), Some(h(10)), 16),
            None,
            "a stale watermark below the startup fetch height does not fetch below next_fetch"
        );
    }

    #[test]
    fn fetch_window_clamps_at_handoff() {
        let h = block::Height;

        assert_eq!(
            next_fetch_window(h(10), h(10), h(20), None, 16),
            Some((h(10), h(20))),
            "the bounded window never extends past the checkpoint handoff"
        );
        assert_eq!(
            next_fetch_window(h(10), h(21), h(20), Some(h(20)), 16),
            None,
            "there is no fetch window after the handoff"
        );
    }

    #[test]
    fn fetch_window_uses_saturating_height_math() {
        let h = block::Height;

        assert_eq!(
            fetch_window_end(h(u32::MAX - 5), h(u32::MAX), None, 16),
            h(u32::MAX),
            "fetch-ahead arithmetic saturates before clamping to the handoff"
        );
    }

    #[test]
    fn staged_roots_insert_after_full_fetch_success() -> Result<(), BoxError> {
        let staged = vec![root_at(42), root_at(43)];
        let expected = staged.clone();
        let mut inserted = None;

        insert_staged_roots_after_success(staged, Ok(()), |roots| inserted = Some(roots))?;

        assert_eq!(
            inserted,
            Some(expected),
            "a successful full-range fetch publishes the staged roots exactly once"
        );
        Ok(())
    }

    #[test]
    fn staged_roots_are_dropped_after_fetch_error() {
        let staged_prefix = vec![root_at(42), root_at(43)];
        let mut inserted = false;

        let result = insert_staged_roots_after_success(
            staged_prefix,
            Err("suffix unavailable".into()),
            |_roots| inserted = true,
        );

        assert!(
            result.is_err(),
            "the original fetch error is returned to the retry loop"
        );
        assert!(
            !inserted,
            "a failed full-range fetch must not publish its staged prefix"
        );
    }

    #[tokio::test]
    async fn refetch_request_fetches_requested_height() {
        let mut fetched = None;

        let result = handle_refetch_request_with_fetch(Ok(block::Height(42)), |height| {
            fetched = Some(height);
            future::ready(Ok(()))
        })
        .await;

        assert!(
            result.is_ok(),
            "a fetched refetch height keeps the driver alive"
        );
        assert_eq!(
            fetched,
            Some(block::Height(42)),
            "the refetch handler fetches exactly the requested height"
        );
    }

    #[tokio::test]
    async fn refetch_request_keeps_running_after_fetch_error() {
        let result = handle_refetch_request_with_fetch(Ok(block::Height(42)), |_height| {
            future::ready(Err::<(), BoxError>("fetch failed".into()))
        })
        .await;

        assert!(
            result.is_ok(),
            "a peer fetch error is retryable and keeps the driver subscribed"
        );
    }

    #[tokio::test]
    async fn refetch_request_ignores_lagged_notifications() {
        let mut fetched = false;

        let result = handle_refetch_request_with_fetch(
            Err(tokio::sync::broadcast::error::RecvError::Lagged(3)),
            |_height| {
                fetched = true;
                future::ready(Ok(()))
            },
        )
        .await;

        assert!(
            result.is_ok(),
            "lagged notifications are recoverable because future retries can resend heights"
        );
        assert!(
            !fetched,
            "lagged notifications do not fetch an unknown height"
        );
    }

    #[tokio::test]
    async fn refetch_request_stops_after_closed_channel() {
        let mut fetched = false;

        let result = handle_refetch_request_with_fetch(
            Err(tokio::sync::broadcast::error::RecvError::Closed),
            |_height| {
                fetched = true;
                future::ready(Ok(()))
            },
        )
        .await;

        assert!(
            result.is_err(),
            "a closed refetch channel tells the driver there are no more refetch requests"
        );
        assert!(!fetched, "closed channels do not fetch an unknown height");
    }
}
