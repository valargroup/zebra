//! The `tree_aux` peer-source driver and serving port (verified-commitment-trees POC).
//!
//! Serving side: [`StateTreeAuxPort`] answers inbound `GetRoots` from local state via
//! [`ReadRequest::BlockRoots`]. Client side: [`run_tree_aux_driver`] fetches the per-block
//! commitment roots for the genesis→checkpoint range from a peer and writes them into the
//! committer's cache ([`TreeAuxRootsWriter`]) *ahead of* body commit, so the fast path can
//! fold them in at commit time. The handoff frontier is embedded in the binary, so only
//! roots travel over the wire.
//!
//! Gated on the `VCT_PEER` experiment toggle; a no-op in the default configuration.

use std::time::Duration;

use tower::{Service, ServiceExt};
use zebra_chain::{block, parallel::commitment_aux::BlockCommitmentRoots, parameters::Network};
use zebra_network::zakura::{
    fetch_roots, BoxRunFuture, TreeAuxStatePort, ZakuraSupervisorHandle,
};
use zebra_state::{BoxError, ReadRequest, ReadResponse, ReadStateService, TreeAuxRootsWriter};

/// Delay between driver attempts while waiting for a peer, or after a fetch error.
const TREE_AUX_DRIVER_RETRY: Duration = Duration::from_secs(5);

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

/// Fetch the genesis→checkpoint per-block roots from a peer into the committer's cache.
///
/// Minimal POC driver: once an outbound peer is available, fetch the whole checkpoint
/// range and write each batch into `writer` ahead of body commit, then stop. Retries on a
/// fetch error or while no peer is connected. Because header sync runs ahead of bodies,
/// the cache is filled before the committer reaches a height; a height the peer cannot
/// supply simply stays in legacy mode (safe by construction — never wrong state).
pub(crate) async fn run_tree_aux_driver(
    supervisor: ZakuraSupervisorHandle,
    writer: TreeAuxRootsWriter,
    network: Network,
    shutdown: impl std::future::Future<Output = ()>,
) {
    let handoff = network.checkpoint_list().max_height();

    let driver = async {
        loop {
            // Wait for an outbound peer before issuing requests (fetch_roots needs one).
            if supervisor.outbound_peer_handles().await.is_empty() {
                tokio::time::sleep(TREE_AUX_DRIVER_RETRY).await;
                continue;
            }

            let result = fetch_roots(&supervisor, block::Height(1), handoff, |batch| {
                writer.insert_roots(batch);
            })
            .await;

            match result {
                Ok(()) => {
                    tracing::info!(
                        handoff_height = handoff.0,
                        "tree_aux: fetched genesis→checkpoint roots from peer into the committer cache"
                    );
                    break;
                }
                Err(error) => {
                    tracing::warn!(?error, "tree_aux: root fetch failed, retrying");
                    tokio::time::sleep(TREE_AUX_DRIVER_RETRY).await;
                }
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

        assert_eq!(roots, held, "the port returns the roots the read service serves");
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
}
