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
/// Implemented by the node (in `zebrad`) over `zebra-state`'s async read service
/// (`ReadRequest::BlockRoots`); kept as a trait so `zebra-network` does not depend on
/// `zebra-state`. Async because the state read goes through the buffered read service.
pub trait TreeAuxStatePort: Send + Sync + 'static {
    /// Return the per-block roots for `[start_height, start_height + count)` that this
    /// node can serve, in ascending height order. May return fewer than `count` (or an
    /// empty vec) if the node does not hold the whole range.
    fn read_block_roots(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> BoxRunFuture<'static, Vec<BlockCommitmentRoots>>;
}

/// Advisory frame cap; the authoritative inbound cap is `app_frame_cap_for_stream_kind`.
const TREE_AUX_FRAME_CAP: u32 = (MAX_TA_MESSAGE_BYTES + FRAME_HEADER_BYTES) as u32;
/// Encoded `Roots` payload prefix: root count.
const ROOTS_PREFIX_BYTES: usize = 4;
/// Encoded `RangeUnavailable` payload: start height + count.
const RANGE_UNAVAILABLE_BYTES: usize = 8;
/// Encoded [`BlockCommitmentRoots`]: height + Sapling root + Orchard root.
const BLOCK_COMMITMENT_ROOTS_BYTES: usize = 4 + 32 + 32;

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

/// Returns the maximum response payload bytes allowed by the negotiated frame
/// and message caps.
///
/// The frame cap includes the frame header, while the message cap applies to
/// the encoded payload, so responses must fit in the smaller of
/// `max_frame_bytes - FRAME_HEADER_BYTES` and `max_message_bytes`. Returns an
/// error if the negotiated caps cannot fit even the smallest `tree_aux`
/// response (`RangeUnavailable`).
fn effective_response_payload_bytes(
    max_frame_bytes: u32,
    max_message_bytes: u32,
) -> Result<usize, SinkReject> {
    let max_frame_bytes = usize::try_from(max_frame_bytes)
        .map_err(|_| SinkReject::local("tree_aux max_frame_bytes does not fit usize"))?;
    let max_message_bytes = usize::try_from(max_message_bytes)
        .map_err(|_| SinkReject::local("tree_aux max_message_bytes does not fit usize"))?;

    let max_payload_bytes = max_frame_bytes
        .saturating_sub(FRAME_HEADER_BYTES)
        .min(max_message_bytes);
    if max_payload_bytes < RANGE_UNAVAILABLE_BYTES {
        return Err(SinkReject::local(
            "tree_aux negotiated response cap cannot fit RangeUnavailable",
        ));
    }

    Ok(max_payload_bytes)
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
        max_frame_bytes: u32,
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

            // Bound the response by the per-request cap and by the effective payload
            // cap: the smaller of the negotiated frame payload and message caps.
            let max_payload_bytes =
                effective_response_payload_bytes(max_frame_bytes, max_message_bytes)?;
            let fit_by_bytes = max_payload_bytes
                .saturating_sub(ROOTS_PREFIX_BYTES)
                .saturating_div(BLOCK_COMMITMENT_ROOTS_BYTES);
            let fit_by_bytes = u32::try_from(fit_by_bytes)
                .expect("u32 frame/message caps limit tree_aux roots to less than u32::MAX");
            let requested_count = count.min(MAX_TA_ROOTS_PER_REQUEST);
            let count = requested_count.min(fit_by_bytes);

            if count == 0 {
                let frame = TreeAuxMessage::RangeUnavailable {
                    start_height,
                    count: requested_count,
                }
                .encode_frame()
                .map_err(|error| SinkReject::local(error.to_string()))?;
                return Ok(vec![frame]);
            }

            let mut roots = self.port.read_block_roots(start_height, count).await;
            let count = usize::try_from(count)
                .map_err(|_| SinkReject::local("tree_aux root count does not fit usize"))?;
            roots.truncate(count);
            let response = if roots.is_empty() {
                TreeAuxMessage::RangeUnavailable {
                    start_height,
                    count: u32::try_from(count)
                        .expect("tree_aux root count came from a u32 request cap"),
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
    async fn response_roots_are_bounded_by_the_frame_cap() {
        let held: Vec<_> = (100..110).map(root_at).collect();
        let service = TreeAuxService::new(Arc::new(MockPort(held.clone())));

        let request = TreeAuxMessage::GetRoots {
            start_height: block::Height(100),
            count: 10,
        }
        .encode_frame()
        .expect("encodes");
        let max_frame_bytes = u32::try_from(
            FRAME_HEADER_BYTES + ROOTS_PREFIX_BYTES + 2 * BLOCK_COMMITMENT_ROOTS_BYTES,
        )
        .expect("test frame cap fits in u32");

        let frames = service
            .request_frame(
                peer(),
                ZAKURA_STREAM_TREE_AUX,
                1,
                max_frame_bytes,
                1 << 20,
                request,
            )
            .await
            .expect("serves the request");
        let frame = frames.into_iter().next().expect("one frame");

        assert!(
            frame.payload.len()
                <= usize::try_from(max_frame_bytes)
                    .expect("test frame cap fits usize")
                    .saturating_sub(FRAME_HEADER_BYTES),
            "response payload must fit the negotiated frame cap"
        );

        let response = TreeAuxMessage::decode_frame(frame).expect("decodes");
        match response {
            TreeAuxMessage::Roots { roots } => {
                assert_eq!(
                    roots,
                    held[..2],
                    "only roots that fit the smaller frame cap are read and returned"
                );
            }
            other => panic!("expected capped Roots, got {other:?}"),
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
