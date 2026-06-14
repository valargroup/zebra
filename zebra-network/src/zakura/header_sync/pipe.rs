//! header_sync/pipe.rs — the per-peer header-sync pipe (stream 5).
//!
//! THE PHASE-2 DAG SLICE IS THIS DIAGRAM. The code below is a mechanical
//! transcription; the [`PIPE_SHAPE`] const is the inspectable, drift-checked
//! copy of it.
//!
//!  queued(GetHeaders) ─▶ command(record expected) ─▶ expected_headers.push_back
//!  recv ─▶ guard ─┬─ Headers ─▶ expected_headers.pop_front ─▶ decode ─▶ forward(WireMessage)
//!                 └─ Control ───────────────────────────────▶ decode ─▶ forward(WireMessage)
//!
//! Phase 2 moves request/response correlation out of
//! [`HeaderSyncPeerSession`] and into [`HsLocal`]. The shared scheduler still
//! decides when to ask a peer for headers, but it sends that decision to this
//! peer-owned pipe as a command after the outbound `GetHeaders` is queued
//! successfully. The pipe prioritizes and drains those commands before inbound
//! frames so a response cannot beat its local expectation. This retires the
//! session mutex without changing the reactor's synthetic `WireMessage` test
//! path.

use std::{collections::VecDeque, sync::Arc};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{events::*, service::HeaderSyncPeerCommand, wire::*, *};
use crate::zakura::{
    Edge, Flow, FramedRecv, Node, NodeKind, Pipe, PipeCx, PipeShape, SinkReject, ZakuraPeerId,
};

pub(super) struct HsLocal {
    /// Plain peer-local response expectations, owned by this pipe task.
    expected_headers: VecDeque<ExpectedHeadersResponse>,
    /// Commands from shared scheduling state into this peer-local pipe.
    commands: mpsc::UnboundedReceiver<HeaderSyncPeerCommand>,
}

impl HsLocal {
    /// Build per-peer local state around this peer's stream-5 session.
    pub(super) fn new(commands: mpsc::UnboundedReceiver<HeaderSyncPeerCommand>) -> Self {
        Self {
            expected_headers: VecDeque::new(),
            commands,
        }
    }

    fn pop_expected_headers_response(&mut self) -> Option<ExpectedHeadersResponse> {
        self.expected_headers.pop_front()
    }

    fn handle_command(&mut self, command: HeaderSyncPeerCommand) {
        match command {
            HeaderSyncPeerCommand::RecordExpectedHeaders(expected) => {
                self.expected_headers.push_back(expected);
            }
        }
    }

    fn drain_ready_commands(&mut self) {
        while let Ok(command) = self.commands.try_recv() {
            self.handle_command(command);
        }
    }
}

/// Shared environment handed to every header-sync pipe.
///
/// Phase 1's environment is just the cloneable reactor handle: the decode stage
/// forwards each decoded message (or decode failure) to the unchanged reactor
/// over this handle. Cross-peer shared core state arrives in Phase 2.
#[derive(Clone)]
pub(super) struct HsEnv {
    /// Handle used to forward inbound wire events to the header-sync reactor.
    handle: HeaderSyncHandle,
}

impl HsEnv {
    /// Wrap a cloneable reactor handle as the pipe's shared environment.
    pub(super) fn new(handle: HeaderSyncHandle) -> Self {
        Self { handle }
    }
}

/// The Phase-2 header-sync pipe DAG slice, as checked documentation.
pub(super) const PIPE_SHAPE: PipeShape = PipeShape {
    service: "header-sync",
    nodes: &[
        Node {
            id: "guard",
            kind: NodeKind::Guard,
        },
        Node {
            id: "decode",
            kind: NodeKind::Decode,
        },
        Node {
            id: "correlate",
            kind: NodeKind::Mutate,
        },
        Node {
            id: "emit",
            kind: NodeKind::Emit,
        },
    ],
    edges: &[
        Edge {
            from: "guard",
            to: "correlate",
            on: "Headers",
        },
        Edge {
            from: "guard",
            to: "decode",
            on: "Control",
        },
        Edge {
            from: "correlate",
            to: "decode",
            on: "Expected",
        },
        Edge {
            from: "decode",
            to: "emit",
            on: "Ok",
        },
    ],
};

/// Executable transcription of [`PIPE_SHAPE`] — the production entry function.
///
/// The guard already admitted this frame (oversize-only) before `run_inbound`
/// is reached, so this is the `Headers|Control → correlate → decode → emit` tail. It
/// delegates to the single [`deliver`] implementation with the peer-owned
/// expected-response value, so the production pipe and the test/recorder
/// `deliver_frame` path can never diverge on *what* they decode or emit.
///
/// The two callers differ only in how they treat a closed reactor queue, which
/// reproduces the old per-caller handling exactly: the production sink logged
/// the `SinkReject::Local` and continued the loop, so `run_inbound` maps that
/// one case to a debug log plus [`Flow::Done`] (which [`run_peer`] treats as
/// "continue"). Protocol rejects pass straight through and tear the peer down.
pub(super) fn run_inbound(cx: &mut PipeCx<'_, HsLocal, HsEnv>, frame: Frame) -> Flow<()> {
    let expected = (u8::try_from(frame.message_type).ok() == Some(MSG_HS_HEADERS))
        .then(|| cx.local.pop_expected_headers_response())
        .flatten();
    match deliver(&cx.env.handle, expected, cx.peer_id.clone(), frame) {
        Flow::Reject(SinkReject::Local(error)) => {
            tracing::debug!(
                ?error,
                peer_id = ?cx.peer_id,
                "header-sync stream could not deliver frame locally"
            );
            Flow::Done
        }
        other => other,
    }
}

/// The single inbound decode/branch/forward stage, shared by both paths.
///
/// This is the one decode implementation reachable from:
///
/// - the production pipe's [`run_inbound`] (with `Some(session)` so a `Headers`
///   response is correlated against the peer's outstanding `GetHeaders`), and
/// - [`HeaderSyncService::deliver_frame`](super::service::HeaderSyncService) (the
///   test/recorder path, which passes `None` so a `Headers` response with no
///   outstanding request is rejected as `UnsolicitedHeaders`).
///
/// It is a faithful port of the old `deliver_header_sync_frame`: the same events
/// fire on the same conditions, mapped onto [`Flow`]:
///
/// - a successful forward to the reactor ⇒ [`Flow::Continue`],
/// - the old `SinkReject::Protocol` cases ⇒ [`Flow::Reject`] with a `Protocol`
///   reason (fatal — disconnect the peer), and
/// - the old `SinkReject::Local` "queue closed" case ⇒ [`Flow::Reject`] with a
///   `Local` reason. Each caller then maps `Local` to its old behavior:
///   `run_inbound` logs and continues, while `deliver_frame` returns it to the
///   registry as `Err(SinkReject::Local(_))`.
pub(super) fn deliver(
    handle: &HeaderSyncHandle,
    expected: Option<ExpectedHeadersResponse>,
    peer_id: ZakuraPeerId,
    frame: Frame,
) -> Flow<()> {
    if u8::try_from(frame.message_type).ok() == Some(MSG_HS_HEADERS) {
        let Some(expected) = expected else {
            let error = Arc::new(HeaderSyncWireError::UnsolicitedHeaders);
            let _ = handle.try_send(HeaderSyncEvent::WireProtocolFailure {
                peer: peer_id.clone(),
                reason: HeaderSyncMisbehavior::UnsolicitedHeaders,
                error: error.clone(),
            });
            let protocol_error =
                std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string());
            return Flow::Reject(SinkReject::protocol(protocol_error));
        };

        let msg = match HeaderSyncMessage::decode_frame(
            frame,
            HeaderSyncDecodeContext::for_headers_response(expected, expected.count),
        ) {
            Ok(msg) => msg,
            Err(error) => {
                let protocol_error =
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string());
                let _ = handle.try_send(HeaderSyncEvent::WireProtocolFailure {
                    peer: peer_id.clone(),
                    reason: HeaderSyncMisbehavior::MalformedMessage,
                    error: Arc::new(error),
                });
                return Flow::Reject(SinkReject::protocol(protocol_error));
            }
        };

        return forward(handle, HeaderSyncEvent::WireMessage { peer: peer_id, msg });
    }

    let msg = match decode_control_frame(frame) {
        Ok(msg) => msg,
        Err(error) => {
            let protocol_error =
                std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string());
            let _ = handle.try_send(HeaderSyncEvent::WireDecodeFailed {
                peer: peer_id,
                error: Arc::new(error),
            });
            return Flow::Reject(SinkReject::protocol(protocol_error));
        }
    };

    forward(handle, HeaderSyncEvent::WireMessage { peer: peer_id, msg })
}

/// Run one peer-owned header-sync pipe until stream close, cancellation, or reject.
///
/// Correlation-ordering invariant: a `RecordExpectedHeaders` command is enqueued
/// synchronously the moment its outbound `GetHeaders` is queued, so it is already
/// in this peer's command channel before any network response can return (a round
/// trip is orders of magnitude slower than a local enqueue). The loop drains
/// ready commands before every inbound frame, and the `biased` select prefers the
/// command channel over `recv`, so the expectation is always popped into
/// `HsLocal.expected_headers` before the matching `Headers` frame is decoded —
/// never the reverse, which would reject a solicited response as
/// `UnsolicitedHeaders`.
pub(super) async fn run_peer(
    mut pipe: Pipe<HsLocal, HsEnv>,
    mut recv: FramedRecv,
    cancel: CancellationToken,
) -> Result<(), SinkReject> {
    enum Input {
        Frame(Frame),
        Command(HeaderSyncPeerCommand),
        Done,
    }

    loop {
        pipe.local_mut().drain_ready_commands();

        let input = {
            let local = pipe.local_mut();
            tokio::select! {
                biased;
                () = cancel.cancelled() => Input::Done,
                command = local.commands.recv() => match command {
                    Some(command) => Input::Command(command),
                    None => Input::Done,
                },
                frame = recv.recv() => match frame {
                    Some(frame) => Input::Frame(frame),
                    None => Input::Done,
                },
            }
        };

        match input {
            Input::Done => return Ok(()),
            Input::Frame(frame) => {
                pipe.local_mut().drain_ready_commands();
                match pipe.run_one(frame) {
                    Flow::Continue(()) | Flow::Done => {}
                    Flow::Reject(reject) => return Err(reject),
                }
            }
            Input::Command(command) => pipe.local_mut().handle_command(command),
        }
    }
}

/// Forward a successfully decoded inbound event to the reactor.
///
/// A closed reactor queue is a local, non-fatal condition for the peer: the old
/// `deliver_header_sync_frame` returned `SinkReject::local` here, so this returns
/// [`Flow::Reject`] with a `Local` reason. Callers decide whether to continue or
/// surface it (see [`deliver`]).
fn forward(handle: &HeaderSyncHandle, event: HeaderSyncEvent) -> Flow<()> {
    match handle.try_send(event) {
        Ok(()) => Flow::Continue(()),
        Err(error) => Flow::Reject(SinkReject::local(format!(
            "header-sync queue closed: {error}"
        ))),
    }
}

/// Decode a non-`Headers` (control) frame.
///
/// `Headers` frames need the peer's outstanding-request context and are handled
/// in [`deliver`]; a `Headers` frame reaching this path has no correlated
/// request, so it is rejected as `UnsolicitedHeaders` exactly as the old
/// `decode_header_sync_frame` did.
fn decode_control_frame(frame: Frame) -> Result<HeaderSyncMessage, HeaderSyncWireError> {
    if u8::try_from(frame.message_type).ok() == Some(MSG_HS_HEADERS) {
        return Err(HeaderSyncWireError::UnsolicitedHeaders);
    }

    HeaderSyncMessage::decode_frame(frame, HeaderSyncDecodeContext::control())
}

#[cfg(test)]
mod tests {
    use tokio::sync::watch;

    use super::*;
    use crate::zakura::{ServicePeerSnapshot, ZakuraHeaderSyncCandidateState};

    const FRAME_FORKS: [&str; 2] = ["Headers", "Control"];

    fn peer() -> ZakuraPeerId {
        ZakuraPeerId::new(vec![5; 32]).expect("test peer id is within bounds")
    }

    /// Build a `HeaderSyncHandle` whose bounded `events` queue the test can drain.
    /// The watch frontiers are never read on the inbound decode path, so dummy
    /// values suffice.
    fn test_handle() -> (HeaderSyncHandle, mpsc::Receiver<HeaderSyncEvent>) {
        let (events, events_rx) = mpsc::channel(16);
        let (lifecycle, _lifecycle_rx) = mpsc::unbounded_channel();
        let (_tip_tx, tip) = watch::channel((block::Height(0), block::Hash([0; 32])));
        let (_peers_tx, peers) = watch::channel(ServicePeerSnapshot::default());
        let (_candidates_tx, candidates) =
            watch::channel(ZakuraHeaderSyncCandidateState::default());
        (
            HeaderSyncHandle {
                events,
                lifecycle,
                tip,
                peers,
                candidates,
            },
            events_rx,
        )
    }

    fn headers_frame(payload: Vec<u8>) -> Frame {
        Frame {
            message_type: u16::from(MSG_HS_HEADERS),
            flags: 0,
            payload,
        }
    }

    /// A `Headers` frame with no recorded expectation is unsolicited: it reports
    /// `UnsolicitedHeaders` misbehavior and rejects the peer, before any decode.
    #[test]
    fn deliver_unsolicited_headers_rejects_without_expectation() {
        let (handle, mut events) = test_handle();

        let flow = deliver(&handle, None, peer(), headers_frame(Vec::new()));

        assert!(matches!(flow, Flow::Reject(SinkReject::Protocol(_))));
        match events.try_recv() {
            Ok(HeaderSyncEvent::WireProtocolFailure { reason, .. }) => {
                assert!(matches!(reason, HeaderSyncMisbehavior::UnsolicitedHeaders));
            }
            other => panic!("expected WireProtocolFailure(UnsolicitedHeaders), got {other:?}"),
        }
    }

    /// With a recorded expectation, the same `Headers` frame is *correlated* and
    /// decoded: a malformed payload now reports `MalformedMessage`, not
    /// `UnsolicitedHeaders`, proving the expectation was consumed before decode.
    #[test]
    fn deliver_correlated_headers_decodes_against_expectation() {
        let (handle, mut events) = test_handle();
        let expected = ExpectedHeadersResponse::new(block::Height(1), 1).expect("count is valid");

        let flow = deliver(&handle, Some(expected), peer(), headers_frame(Vec::new()));

        assert!(matches!(flow, Flow::Reject(SinkReject::Protocol(_))));
        match events.try_recv() {
            Ok(HeaderSyncEvent::WireProtocolFailure { reason, .. }) => {
                assert!(matches!(reason, HeaderSyncMisbehavior::MalformedMessage));
            }
            other => panic!("expected WireProtocolFailure(MalformedMessage), got {other:?}"),
        }
    }

    /// The peer-local correlation queue is FIFO and is filled by draining ready
    /// commands. This is the invariant `run_peer` relies on: an expectation
    /// recorded by a `RecordExpectedHeaders` command is drained and available to
    /// pop before the matching `Headers` response is processed.
    #[test]
    fn local_correlation_queue_drains_commands_in_fifo_order() {
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let mut local = HsLocal::new(commands_rx);

        let first = ExpectedHeadersResponse::new(block::Height(1), 1).expect("count is valid");
        let second = ExpectedHeadersResponse::new(block::Height(2), 2).expect("count is valid");
        commands_tx
            .send(HeaderSyncPeerCommand::RecordExpectedHeaders(first))
            .expect("pipe is alive");
        commands_tx
            .send(HeaderSyncPeerCommand::RecordExpectedHeaders(second))
            .expect("pipe is alive");

        // Nothing is available until the pipe drains its ready commands.
        assert_eq!(local.pop_expected_headers_response(), None);
        local.drain_ready_commands();

        assert_eq!(local.pop_expected_headers_response(), Some(first));
        assert_eq!(local.pop_expected_headers_response(), Some(second));
        assert_eq!(local.pop_expected_headers_response(), None);
    }

    #[test]
    fn pipe_shape_matches_runtime() {
        // (a) The declared shape is internally consistent.
        PIPE_SHAPE
            .validate()
            .expect("header-sync PIPE_SHAPE edges name only real nodes");

        // (b) Phase 2's real runtime fork is still frame-shape based:
        // `Headers` needs peer-local request correlation, while all other
        // stream-5 messages decode as `Control` and are forwarded to the
        // compatibility reactor for semantic dispatch.
        let frame_forks: Vec<&str> = PIPE_SHAPE
            .edges
            .iter()
            .filter(|edge| edge.from == "guard")
            .map(|edge| edge.on)
            .collect();

        assert_eq!(
            frame_forks.len(),
            FRAME_FORKS.len(),
            "guard has exactly the runtime frame-shape forks"
        );
        for fork in FRAME_FORKS {
            assert!(
                frame_forks.contains(&fork),
                "guard edge missing for runtime fork {fork}"
            );
        }

        // (c) `Headers` responses correlate before decode; all decoded messages
        // terminate at the single forward/emit stage.
        assert!(
            PIPE_SHAPE
                .edges
                .iter()
                .any(|edge| edge.from == "correlate" && edge.to == "decode"),
            "headers responses correlate before decode"
        );
        assert!(
            PIPE_SHAPE
                .nodes
                .iter()
                .any(|node| node.id == "emit" && matches!(node.kind, NodeKind::Emit)),
            "the pipe terminates at a single `emit` node"
        );
    }
}
