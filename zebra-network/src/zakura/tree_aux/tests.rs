//! Two-node integration test for the `tree_aux` stream.
//!
//! Two real Zakura nodes negotiate the `tree_aux` capability over the loopback
//! transport and exchange per-block commitment roots — the "proof of peers" for the
//! verified-commitment-trees peer source. The server serves roots from an in-memory
//! state port; the client issues a real `GetRoots` request and receives `Roots`.

use std::{sync::Arc, time::Duration};

use zebra_chain::{block, orchard, parallel::commitment_aux::BlockCommitmentRoots, sapling};

use super::{
    fetch_roots, BoxRunFuture, TreeAuxMessage, TreeAuxService, TreeAuxStatePort,
    ZAKURA_STREAM_TREE_AUX,
};
use crate::{zakura::testkit::ZakuraTestNode, BoxError};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// An in-memory `tree_aux` state port over a fixed set of roots (the server's holdings).
struct InMemoryPort(Vec<BlockCommitmentRoots>);

impl TreeAuxStatePort for InMemoryPort {
    fn read_block_roots(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> BoxRunFuture<'static, Vec<BlockCommitmentRoots>> {
        let roots: Vec<_> = self
            .0
            .iter()
            .filter(|r| r.height >= start_height && r.height.0 < start_height.0 + count)
            .cloned()
            .collect();
        Box::pin(async move { roots })
    }
}

fn root_at(height: u32) -> BlockCommitmentRoots {
    BlockCommitmentRoots {
        height: block::Height(height),
        sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
        orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
    }
}

async fn tree_aux_node(
    seed: u64,
    held: Vec<BlockCommitmentRoots>,
) -> Result<ZakuraTestNode, BoxError> {
    ZakuraTestNode::builder(seed)
        // Both nodes share 127.0.0.1, so raise the per-IP cap above the default of 1.
        .max_connections_per_ip(16)
        .service(Arc::new(TreeAuxService::new(Arc::new(InMemoryPort(held)))))
        .spawn()
        .await
}

#[tokio::test]
async fn two_nodes_exchange_roots_over_tree_aux() -> Result<(), BoxError> {
    let _guard = zebra_test::init();

    // The server holds a contiguous run of roots; the client holds none (it only fetches),
    // but still runs the service so it advertises the `tree_aux` capability for negotiation.
    let served: Vec<_> = (1_687_104..1_687_204).map(root_at).collect();
    let server = tree_aux_node(1, served.clone()).await?;
    let client = tree_aux_node(2, Vec::new()).await?;

    client.connect_native(&server, CONNECT_TIMEOUT).await?;

    // The client issues a real `GetRoots` over the negotiated request/response stream.
    let handle = client
        .supervisor()
        .outbound_peer_handles()
        .await
        .into_iter()
        .next()
        .expect("client has an outbound handle to the server after connecting");

    let request = TreeAuxMessage::GetRoots {
        start_height: block::Height(1_687_104),
        count: 50,
    }
    .encode_frame()
    .expect("encodes the request");

    let frames = handle
        .request(
            ZAKURA_STREAM_TREE_AUX,
            1,
            request.message_type,
            request.flags,
            request.payload,
        )
        .await
        .expect("the server answers the tree_aux request over the wire");

    let response =
        TreeAuxMessage::decode_frame(frames.into_iter().next().expect("one response frame"))
            .expect("decodes the response");

    match response {
        TreeAuxMessage::Roots { roots } => {
            assert_eq!(
                roots,
                served[..50],
                "the roots received over the wire match the server's holdings"
            );
        }
        other => panic!("expected Roots over the wire, got {other:?}"),
    }

    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

/// The client driver ([`fetch_roots`]) pulls a multi-request height range from a peer
/// and delivers it to a sink — the path the node wires to a `PeerSource`. Exercises the
/// fetch loop (range advance) over the real transport, beyond a single manual request.
#[tokio::test]
async fn client_driver_fetches_a_root_range_over_tree_aux() -> Result<(), BoxError> {
    let _guard = zebra_test::init();

    let served: Vec<_> = (1_687_104..1_687_204).map(root_at).collect();
    let server = tree_aux_node(3, served.clone()).await?;
    let client = tree_aux_node(4, Vec::new()).await?;

    client.connect_native(&server, CONNECT_TIMEOUT).await?;

    // The driver fetches the whole range; the sink collects each delivered batch (as the
    // node would write each batch into a PeerSource).
    let mut collected = Vec::new();
    fetch_roots(
        &client.supervisor(),
        block::Height(1_687_104),
        block::Height(1_687_203),
        |batch| collected.extend(batch),
    )
    .await?;

    assert_eq!(
        collected, served,
        "the driver fetched the full range over the wire, matching the server's holdings"
    );

    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn client_driver_rejects_gapped_root_batches() -> Result<(), BoxError> {
    let _guard = zebra_test::init();

    let served: Vec<_> = (1_687_105..1_687_204).map(root_at).collect();
    let server = tree_aux_node(5, served).await?;
    let client = tree_aux_node(6, Vec::new()).await?;

    client.connect_native(&server, CONNECT_TIMEOUT).await?;

    let mut collected = Vec::new();
    let result = fetch_roots(
        &client.supervisor(),
        block::Height(1_687_104),
        block::Height(1_687_203),
        |batch| collected.extend(batch),
    )
    .await;

    assert!(
        result.is_err(),
        "a peer response that skips the requested first height is rejected"
    );
    assert!(
        collected.is_empty(),
        "gapped roots are not delivered to the sink"
    );

    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn client_driver_falls_back_to_another_tree_aux_peer() -> Result<(), BoxError> {
    let _guard = zebra_test::init();

    let expected: Vec<_> = (1_687_104..1_687_204).map(root_at).collect();
    let gapped: Vec<_> = (1_687_105..1_687_204).map(root_at).collect();
    let bad_server = tree_aux_node(7, gapped).await?;
    let good_server = tree_aux_node(8, expected.clone()).await?;
    let client = tree_aux_node(9, Vec::new()).await?;

    client.connect_native(&bad_server, CONNECT_TIMEOUT).await?;
    client.connect_native(&good_server, CONNECT_TIMEOUT).await?;

    let mut collected = Vec::new();
    fetch_roots(
        &client.supervisor(),
        block::Height(1_687_104),
        block::Height(1_687_203),
        |batch| collected.extend(batch),
    )
    .await?;

    assert_eq!(
        collected, expected,
        "the client skips an unusable tree_aux peer and fetches roots from another peer"
    );

    client.shutdown().await;
    good_server.shutdown().await;
    bad_server.shutdown().await;
    Ok(())
}
