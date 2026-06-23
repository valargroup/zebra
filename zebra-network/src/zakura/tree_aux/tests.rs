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
    MAX_TA_MESSAGE_BYTES, ZAKURA_CAP_TREE_AUX, ZAKURA_STREAM_TREE_AUX,
};
use crate::{
    zakura::{
        testkit::{HostilePeer, ZakuraTestNode},
        Frame, ZakuraLocalLimits, ZakuraPeerHandle, FRAME_HEADER_BYTES,
    },
    BoxError, Config,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SMALL_MAX_MESSAGE_BYTES: u32 = 256;
const LARGE_MAX_FRAME_BYTES: u32 = 64 * 1024;

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

async fn tree_aux_node_with_limits(seed: u64) -> Result<ZakuraTestNode, BoxError> {
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    limits.max_message_bytes = SMALL_MAX_MESSAGE_BYTES;
    limits.max_frame_bytes = LARGE_MAX_FRAME_BYTES;

    ZakuraTestNode::builder(seed)
        .limits(limits)
        .max_connections_per_ip(16)
        .service(Arc::new(TreeAuxService::new(Arc::new(InMemoryPort {
            roots: Vec::new(),
            requests: None,
        }))))
        .spawn()
        .await
}

async fn next_outbound_handle(node: &ZakuraTestNode) -> Result<ZakuraPeerHandle, BoxError> {
    tokio::time::timeout(CONNECT_TIMEOUT, async {
        loop {
            if let Some(handle) = node.supervisor().outbound_peer_handles().await.pop() {
                return handle;
            }

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| -> BoxError { "timed out waiting for an outbound tree_aux peer".into() })
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

#[tokio::test]
async fn hostile_tree_aux_response_above_message_cap_is_rejected() -> Result<(), BoxError> {
    let _guard = zebra_test::init();

    let victim = tree_aux_node_with_limits(10).await?;
    let hostile =
        HostilePeer::connect_native_with_capabilities(&victim, 11, ZAKURA_CAP_TREE_AUX).await?;
    let handle = next_outbound_handle(&victim).await?;

    let mut oversized_roots = Vec::new();
    let mut height = 1_687_104u32;
    let response_frame = loop {
        oversized_roots.push(root_at(height));
        let candidate = TreeAuxMessage::Roots {
            roots: oversized_roots.clone(),
        }
        .encode_frame()
        .expect("candidate tree_aux roots frame encodes under the hard tree_aux limit");

        if candidate.payload.len()
            > usize::try_from(SMALL_MAX_MESSAGE_BYTES).expect("test message cap fits usize")
        {
            assert!(
                candidate.payload.len() <= MAX_TA_MESSAGE_BYTES,
                "candidate must stay within the hard tree_aux payload limit"
            );
            assert!(
                candidate.payload.len().saturating_add(FRAME_HEADER_BYTES)
                    <= usize::try_from(LARGE_MAX_FRAME_BYTES).expect("test frame cap fits usize"),
                "candidate must still fit the negotiated frame cap"
            );
            break candidate;
        }

        height += 1;
    };

    let responder = hostile.respond_to_next_request_with(|_request_id| {
        vec![Frame {
            message_type: response_frame.message_type,
            flags: response_frame.flags,
            payload: response_frame.payload.clone(),
        }]
    });

    let request = async {
        let get_roots = TreeAuxMessage::GetRoots {
            start_height: oversized_roots
                .first()
                .expect("oversized batch has at least one root")
                .height,
            count: u32::try_from(oversized_roots.len()).expect("batch length fits in u32"),
        }
        .encode_frame()
        .expect("GetRoots request encodes");

        handle
            .request(
                ZAKURA_STREAM_TREE_AUX,
                7,
                get_roots.message_type,
                get_roots.flags,
                get_roots.payload,
            )
            .await
    };

    let (request_result, responder_result) = tokio::join!(request, responder);
    responder_result?;
    assert!(
        request_result.is_err(),
        "tree_aux requester must reject a response payload above negotiated max_message_bytes"
    );

    hostile.shutdown().await;
    victim.shutdown().await;
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
