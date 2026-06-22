//! The `tree_aux` request/response service: serves `GetRoots` from local state.
//!
//! `tree_aux` is a one-shot request/response stream, so the service is small: it decodes
//! a `GetRoots`, reads the roots from local state through [`TreeAuxStatePort`], and
//! returns a `Roots` (or `RangeUnavailable`) response frame. There is no ordered-stream
//! reactor, scheduler, or per-peer task.

use std::sync::Arc;

use zebra_chain::{block, parallel::commitment_aux::BlockCommitmentRoots};

use super::{
    BoxRunFuture, Frame, Peer, RequestResponseService, SinkReject, Stream, StreamMode,
    TreeAuxMessage, ZakuraPeerId, ZakuraService, FRAME_HEADER_BYTES, MAX_TA_MESSAGE_BYTES,
    MAX_TA_ROOTS_PER_REQUEST, ZAKURA_CAP_TREE_AUX, ZAKURA_STREAM_TREE_AUX,
    ZAKURA_TREE_AUX_STREAM_VERSION,
};

/// The local-state read path the `tree_aux` server uses to answer `GetRoots`.
///
/// Implemented by the node (in `zebrad`) over `zebra-state`'s `produce_block_roots`;
/// kept as a trait so `zebra-network` does not depend on `zebra-state`.
pub trait TreeAuxStatePort: Send + Sync + 'static {
    /// Return the per-block roots for `[start_height, start_height + count)` that this
    /// node can serve, in ascending height order. May return fewer than `count` (or an
    /// empty vec) if the node does not hold the whole range.
    fn read_block_roots(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> Vec<BlockCommitmentRoots>;
}

/// Advisory frame cap; the authoritative inbound cap is `app_frame_cap_for_stream_kind`.
const TREE_AUX_FRAME_CAP: u32 = (MAX_TA_MESSAGE_BYTES + FRAME_HEADER_BYTES) as u32;

const TREE_AUX_SERVICE_STREAMS: [Stream; 1] = [Stream {
    kind: ZAKURA_STREAM_TREE_AUX,
    version: ZAKURA_TREE_AUX_STREAM_VERSION,
    frame_cap: TREE_AUX_FRAME_CAP,
    capability: ZAKURA_CAP_TREE_AUX,
    mode: StreamMode::RequestResponse,
}];

/// The `tree_aux` request/response service. Serves `GetRoots` from local state via a
/// [`TreeAuxStatePort`]; it has no outbound/client state (clients use
/// `ZakuraPeerHandle::request` directly through the driver).
#[derive(Clone)]
pub struct TreeAuxService {
    port: Arc<dyn TreeAuxStatePort>,
}

impl std::fmt::Debug for TreeAuxService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TreeAuxService")
    }
}

impl TreeAuxService {
    /// Build the service over a local-state read port.
    pub fn new(port: Arc<dyn TreeAuxStatePort>) -> Self {
        TreeAuxService { port }
    }
}

impl ZakuraService for TreeAuxService {
    fn name(&self) -> &'static str {
        "tree-aux"
    }

    fn streams(&self) -> &[Stream] {
        &TREE_AUX_SERVICE_STREAMS
    }

    // Request/response only: no persistent per-peer stream to spawn or tear down.
    fn add_peer(&self, _peer: Peer) {}
    fn remove_peer(&self, _peer: &ZakuraPeerId) {}

    fn as_request_response(&self) -> Option<&dyn RequestResponseService> {
        Some(self)
    }
}

impl RequestResponseService for TreeAuxService {
    fn request_frame<'a>(
        &'a self,
        _peer_id: ZakuraPeerId,
        stream_kind: u16,
        _request_id: u64,
        _max_frame_bytes: u32,
        max_message_bytes: u32,
        frame: Frame,
    ) -> BoxRunFuture<'a, Result<Vec<Frame>, SinkReject>> {
        Box::pin(async move {
            if stream_kind != ZAKURA_STREAM_TREE_AUX {
                return Err(SinkReject::protocol("unsupported tree_aux stream kind"));
            }

            let request = TreeAuxMessage::decode_frame(frame)
                .map_err(|error| SinkReject::protocol(error.to_string()))?;
            let TreeAuxMessage::GetRoots {
                start_height,
                count,
            } = request
            else {
                return Err(SinkReject::protocol(
                    "tree_aux server only answers GetRoots requests",
                ));
            };

            // Bound the response: by the per-request cap, and by what fits the peer's
            // negotiated message size in a single frame (each root is ~68 bytes).
            let fit_by_bytes = (max_message_bytes as usize / 68).max(1) as u32;
            let count = count.min(MAX_TA_ROOTS_PER_REQUEST).min(fit_by_bytes);

            let roots = self.port.read_block_roots(start_height, count);
            let response = if roots.is_empty() {
                TreeAuxMessage::RangeUnavailable {
                    start_height,
                    count,
                }
            } else {
                TreeAuxMessage::Roots { roots }
            };

            let frame = response
                .encode_frame()
                .map_err(|error| SinkReject::local(error.to_string()))?;
            Ok(vec![frame])
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zebra_chain::{orchard, sapling};

    struct MockPort(Vec<BlockCommitmentRoots>);

    impl TreeAuxStatePort for MockPort {
        fn read_block_roots(
            &self,
            start_height: block::Height,
            count: u32,
        ) -> Vec<BlockCommitmentRoots> {
            self.0
                .iter()
                .filter(|r| r.height >= start_height && r.height.0 < start_height.0 + count)
                .cloned()
                .collect()
        }
    }

    fn root_at(height: u32) -> BlockCommitmentRoots {
        BlockCommitmentRoots {
            height: block::Height(height),
            sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
            orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
        }
    }

    fn peer() -> ZakuraPeerId {
        ZakuraPeerId::new(vec![7u8; 32]).expect("valid test peer id")
    }

    #[tokio::test]
    async fn serves_get_roots_from_the_state_port() {
        let held: Vec<_> = (100..110).map(root_at).collect();
        let service = TreeAuxService::new(Arc::new(MockPort(held.clone())));

        let request = TreeAuxMessage::GetRoots {
            start_height: block::Height(100),
            count: 5,
        }
        .encode_frame()
        .expect("encodes");

        let frames = service
            .request_frame(peer(), ZAKURA_STREAM_TREE_AUX, 1, 1 << 20, 1 << 20, request)
            .await
            .expect("serves the request");

        let response = TreeAuxMessage::decode_frame(frames.into_iter().next().expect("one frame"))
            .expect("decodes");
        match response {
            TreeAuxMessage::Roots { roots } => {
                assert_eq!(roots, held[..5], "serves exactly the requested range");
            }
            other => panic!("expected Roots, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unheld_range_is_reported_unavailable() {
        let service = TreeAuxService::new(Arc::new(MockPort(vec![])));

        let request = TreeAuxMessage::GetRoots {
            start_height: block::Height(100),
            count: 5,
        }
        .encode_frame()
        .expect("encodes");

        let frames = service
            .request_frame(peer(), ZAKURA_STREAM_TREE_AUX, 1, 1 << 20, 1 << 20, request)
            .await
            .expect("serves the request");

        let response = TreeAuxMessage::decode_frame(frames.into_iter().next().expect("one frame"))
            .expect("decodes");
        assert!(
            matches!(response, TreeAuxMessage::RangeUnavailable { .. }),
            "an unheld range is reported unavailable, not wrong data"
        );
    }
}
