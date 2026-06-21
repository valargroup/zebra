//! header_sync/pipe.rs — the per-peer header-sync pipe (stream 5).
//!
//! THE DAG SLICE IS THIS DIAGRAM. The code below is a mechanical transcription;
//! the [`PIPE_SHAPE`] const is the inspectable, drift-checked copy of it.
//!
//!  queued(GetHeaders) ─▶ command(record expected) ─▶ expected_headers.push_back
//!  recv ─▶ guard ─┬─ Headers ─▶ expected_headers.pop_front ─▶ decode ─▶ validate ─▶ emit(narrowed)
//!                 └─ Control ───────────────────────────────▶ decode ─▶ validate ─▶ emit(narrowed)
//!
//! Request/response correlation lives in [`HsLocal`]. The shared scheduler still
//! decides when to ask a peer for headers, but it sends that decision to this
//! peer-owned pipe as a command after the outbound `GetHeaders` is queued
//! successfully. The pipe prioritizes and drains those commands before inbound
//! frames so a response cannot beat its local expectation.
//!
//! The concrete production owner of this loop is [`HeaderSyncPeerRoutine`]: it
//! owns the [`HsLocal`] decode/correlation state, the peer-local advertised caps
//! and rate meters, the inbound stream-guard admission, the command draining, the
//! frame decode, and the relocated peer-local protocol validation. After
//! validating, it forwards only NARROWED shared-effect events
//! ([`HeaderSyncEvent::PeerStatusUpdated`], [`PeerHeadersReceived`],
//! [`InboundGetHeadersRequested`], [`NewBlockCandidate`], [`PeerMisbehavior`]) to
//! the reactor; the reactor no longer matches a raw decoded wire message. Global
//! scheduling, the direct `GetHeaders` sends, and the direct
//! status/`NewBlock`/`Headers`-response sends stay reactor-side (chunks 04/05).
//!
//! [`PeerHeadersReceived`]: super::events::HeaderSyncEvent::PeerHeadersReceived
//! [`InboundGetHeadersRequested`]: super::events::HeaderSyncEvent::InboundGetHeadersRequested
//! [`NewBlockCandidate`]: super::events::HeaderSyncEvent::NewBlockCandidate
//! [`PeerMisbehavior`]: super::events::HeaderSyncEvent::PeerMisbehavior

use std::{collections::VecDeque, sync::Arc};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    config::*, events::*, scheduler::*, service::HeaderSyncPeerCommand, validation::*, wire::*, *,
};
use crate::zakura::{
    Edge, Flow, FramedRecv, Node, NodeKind, Pipe, PipeCx, PipeShape, SinkReject, ZakuraPeerId,
};

#[derive(Debug)]
pub(crate) struct HsLocal {
    /// Plain peer-local response expectations, owned by this pipe task.
    expected_headers: VecDeque<ExpectedHeadersResponse>,
    /// Commands from shared scheduling state into this peer-local pipe.
    commands: mpsc::UnboundedReceiver<HeaderSyncPeerCommand>,
    /// Pre-decode rate gate for inbound `NewBlock` floods.
    ///
    /// `NewBlock` is the only stream-5 message that deserializes a full
    /// `Arc<Block>` (up to `MAX_HS_MESSAGE_BYTES`) directly from the wire. The
    /// reactor's semantic `inbound_new_block` meter only fires *after* that
    /// decode, so an authenticated peer could otherwise force one full-block
    /// deserialization per frame before being metered. This gate enforces the
    /// same minimum interval *before* decode so excess `NewBlock` frames are
    /// dropped without ever reaching `Block::zcash_deserialize`.
    new_block_meter: RateMeter,
    /// Peer-local advertised status, caps, and the status-spam rate gate.
    ///
    /// Owned by the routine (this chunk relocated peer-local `Status` validation
    /// off the reactor). The reactor keeps a write-only snapshot it updates from
    /// [`HeaderSyncEvent::PeerStatusUpdated`].
    advertised_tip: block::Height,
    received_status: bool,
    inbound_status_meter: RateMeter,
}

impl HsLocal {
    /// Build per-peer local state around this peer's stream-5 session.
    pub(crate) fn new(
        commands: mpsc::UnboundedReceiver<HeaderSyncPeerCommand>,
        anchor_height: block::Height,
        inbound_status_min_interval: Duration,
        new_block_min_interval: Duration,
    ) -> Self {
        Self {
            expected_headers: VecDeque::new(),
            commands,
            new_block_meter: RateMeter::new(new_block_min_interval),
            // A fresh session has advertised nothing; the peer's tip starts at the
            // trusted anchor and gates serving on a first valid status.
            advertised_tip: anchor_height,
            received_status: false,
            inbound_status_meter: RateMeter::new(inbound_status_min_interval),
        }
    }

    /// Take one pre-decode `NewBlock` token. `false` means this frame arrived
    /// faster than the minimum interval and must be dropped before decode.
    fn admit_new_block(&mut self) -> bool {
        self.new_block_meter.try_take(Instant::now())
    }

    fn pop_expected_headers_response(&mut self) -> Option<ExpectedHeadersResponse> {
        self.expected_headers.pop_front()
    }

    /// Restore a solicited-response expectation that was popped for decode but
    /// whose decoded `Headers` event could not be handed to the reactor (the
    /// bounded `events` queue was full or closed). It goes back to the *front* so
    /// FIFO order is preserved and the reactor's still-outstanding range stays
    /// correlated, instead of leaving the expectation silently consumed.
    fn restore_expected_headers(&mut self, expected: ExpectedHeadersResponse) {
        self.expected_headers.push_front(expected);
    }

    fn handle_command(&mut self, command: HeaderSyncPeerCommand) {
        match command {
            HeaderSyncPeerCommand::RecordExpectedHeaders(expected) => {
                self.expected_headers.push_back(expected);
            }
        }
    }

    /// Record an expected `Headers` response on the requester side.
    ///
    /// Production records this through the `RecordExpectedHeaders` command the
    /// moment an outbound `GetHeaders` is queued. The synthetic in-process cluster
    /// harness has no command channel, so it records the expectation directly when
    /// it observes the outbound `GetHeaders`, keeping its per-peer ingest state in
    /// lockstep with the production correlation FIFO.
    #[cfg(test)]
    pub(crate) fn record_expected(&mut self, expected: ExpectedHeadersResponse) {
        self.expected_headers.push_back(expected);
    }

    fn drain_ready_commands(&mut self) {
        while let Ok(command) = self.commands.try_recv() {
            self.handle_command(command);
        }
    }
}

/// Shared environment handed to every header-sync pipe.
///
/// Carries the cloneable reactor handle the routine forwards narrowed
/// shared-effect events over, plus the read-only startup facts the relocated
/// peer-local validation needs (the active network, this node's advertised caps,
/// and the negotiated frame cap used to bound an inbound `GetHeaders`).
#[derive(Clone, Debug)]
pub(crate) struct HsEnv {
    /// Handle used to forward narrowed shared-effect events to the reactor.
    handle: HeaderSyncHandle,
    /// Active network, used by the inbound `GetHeaders` count limit.
    network: Network,
    /// This node's local header-sync advertisement and serving caps.
    config: ZakuraHeaderSyncConfig,
    /// Negotiated or local application frame cap for header-sync responses.
    max_frame_bytes: u32,
}

impl HsEnv {
    /// Wrap a cloneable reactor handle plus the read-only startup facts the
    /// relocated peer-local validation reads.
    pub(crate) fn new(
        handle: HeaderSyncHandle,
        network: Network,
        config: ZakuraHeaderSyncConfig,
        max_frame_bytes: u32,
    ) -> Self {
        Self {
            handle,
            network,
            config,
            max_frame_bytes,
        }
    }

    /// Largest inbound `GetHeaders` count this node will serve, bounded by its
    /// advertised cap and the negotiated frame size.
    fn inbound_get_headers_count_limit(&self) -> u32 {
        inbound_get_headers_count_limit(&self.config, &self.network, self.max_frame_bytes)
    }

    /// Trusted anchor height (or genesis when no override is configured). A fresh
    /// peer's advertised tip starts here so the first status advances it.
    pub(crate) fn anchor_height(&self) -> block::Height {
        self.config.anchor_height.unwrap_or(block::Height(0))
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
/// The guard already admitted this frame (oversize-only) before `run_inbound` is
/// reached, so this is the `Headers|Control → correlate → decode → emit` tail. It
/// delegates to [`decode_and_ingest`], which decodes the frame, correlates a
/// `Headers` response against the peer-owned expectation FIFO, runs the relocated
/// peer-local protocol validation, and forwards only the narrowed shared-effect
/// events the reactor consumes. The production pipe and the test/recorder
/// `deliver_frame` path share that one implementation, so they can never diverge
/// on *what* they decode, validate, or emit.
///
/// A closed reactor queue is the only `SinkReject::Local`: it is the peer's
/// non-fault, so `run_inbound` logs it and returns [`Flow::Done`] (which
/// [`HeaderSyncPeerRoutine`] treats as "continue"). Protocol rejects pass straight
/// through and tear the peer down. When a *solicited* `Headers` response hits the
/// local-reject path, the expectation popped before decode is restored to
/// [`HsLocal`] so reactor queue saturation cannot silently consume it and strand
/// the still-outstanding range.
pub(super) fn run_inbound(cx: &mut PipeCx<'_, HsLocal, HsEnv>, frame: Frame) -> Flow<()> {
    let env = cx.env.clone();
    match decode_and_ingest(cx.local, &env, cx.peer_id.clone(), frame) {
        // A closed/full reactor queue is the peer's non-fault: the routine logs it
        // and continues (treated as `Flow::Done`) rather than tearing the peer down.
        // The solicited-`Headers` expectation was already restored inside
        // `decode_and_ingest`, so the still-outstanding range stays correlated.
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

/// Decode one admitted stream-5 frame, run the relocated peer-local validation,
/// and emit the narrowed shared-effect events to the reactor.
///
/// This is the single implementation reachable from both:
///
/// - the production pipe's [`run_inbound`] (with the peer's live [`HsLocal`] so a
///   `Headers` response correlates against the outstanding `GetHeaders` FIFO and
///   the per-peer rate meters/caps carry across frames), and
/// - [`HeaderSyncService::deliver_frame`](super::service::HeaderSyncService) (the
///   test/recorder path, which uses an ephemeral [`HsLocal`] with no recorded
///   expectation, so a `Headers` response is `UnsolicitedHeaders`).
///
/// Effects map onto [`Flow`]:
///
/// - emitting all effects successfully ⇒ [`Flow::Continue`] (or [`Flow::Done`]
///   when nothing was emitted, e.g. a dropped-before-decode `NewBlock` flood);
/// - a routine-classified protocol violation ⇒ the misbehavior event plus
///   [`Flow::Reject`] with a `Protocol` reason (fatal — disconnect the peer);
/// - a full/closed reactor queue ⇒ [`Flow::Reject`] with a `Local` reason. Each
///   caller maps `Local` to its old behavior: `run_inbound` logs and continues,
///   while `deliver_frame` returns it to the registry as `Err(SinkReject::Local)`.
pub(crate) fn decode_and_ingest(
    local: &mut HsLocal,
    env: &HsEnv,
    peer_id: ZakuraPeerId,
    frame: Frame,
) -> Flow<()> {
    let is_headers = u8::try_from(frame.message_type).ok() == Some(MSG_HS_HEADERS);
    let is_new_block = u8::try_from(frame.message_type).ok() == Some(MSG_HS_NEW_BLOCK);

    // Pre-decode `NewBlock` rate gate: a `NewBlock` frame that arrives inside the
    // per-peer minimum interval is dropped *before* the full `Arc<Block>` is
    // deserialized, so a flood cannot force repeated full-block decode ahead of
    // the reactor's semantic meter. Throttling (drop, keep the peer) matches the
    // reactor's cheap dedup-without-scoring policy, so honest re-floods are not
    // penalized; the first frame in each window still reaches the reactor,
    // preserving first-offense malformed/spam disconnects.
    if is_new_block && !local.admit_new_block() {
        metrics::counter!("sync.header.tip.new_block.predecode_throttled").increment(1);
        return Flow::Done;
    }

    // Correlate a `Headers` response against the peer-owned expectation FIFO before
    // decode, so an over-long or otherwise malformed response is bounded by the
    // matching `GetHeaders` count. A `Headers` frame with no expectation is
    // unsolicited; the routine classifies it and rejects the peer.
    let expected = if is_headers {
        let Some(expected) = local.pop_expected_headers_response() else {
            return reject_misbehavior(
                env,
                &peer_id,
                HeaderSyncMisbehavior::UnsolicitedHeaders,
                &HeaderSyncWireError::UnsolicitedHeaders,
            );
        };
        Some(expected)
    } else {
        None
    };

    let decode_context = match expected {
        Some(expected) => HeaderSyncDecodeContext::for_headers_response(expected, expected.count),
        None if is_headers => HeaderSyncDecodeContext::for_headers_response(
            // Unreachable: `is_headers` always sets `expected` above. Kept total
            // for exhaustiveness.
            ExpectedHeadersResponse::new(block::Height(0), 1)
                .expect("count 1 is a valid bounded request"),
            1,
        ),
        None => HeaderSyncDecodeContext::control(),
    };

    let msg = match HeaderSyncMessage::decode_frame(frame, decode_context) {
        Ok(msg) => msg,
        Err(error) => {
            // A `Headers` response over its correlated count, or any malformed
            // frame, is a peer-local protocol violation classified here before the
            // shared misbehavior event is emitted.
            if let Some(expected) = expected {
                // The expectation was popped for decode but the response was
                // malformed: the peer is being torn down, so the FIFO is discarded
                // with the session; restoring would be moot. Leave it consumed.
                let _ = expected;
            }
            return reject_misbehavior(
                env,
                &peer_id,
                HeaderSyncMisbehavior::MalformedMessage,
                &error,
            );
        }
    };

    match ingest_message(local, env, &peer_id, msg) {
        // The reactor `events` queue was full or closed, so this decoded message
        // could not be delivered locally. For a *solicited* `Headers` response the
        // expectation was already popped before decode, so restore it: the reactor's
        // matching range is still outstanding, and a consumed-but-undelivered
        // expectation would otherwise lose the response entirely (recoverable only by
        // the request timeout) and desynchronize the peer-local FIFO from that
        // outstanding range. Restoring keeps the pipe in the same state as a request
        // still awaiting its response, which the timeout/retry machinery handles.
        //
        // The `Local` reject is RETURNED to the caller (not swallowed): `run_inbound`
        // logs it and continues, while `deliver_frame` surfaces it to the registry as
        // `Err(SinkReject::Local)`. Preserving `SinkReject::Local` vs `Protocol` here
        // is what keeps a local queue failure from being mistaken for peer fault.
        reject @ Flow::Reject(SinkReject::Local(_)) => {
            if let Some(expected) = expected {
                local.restore_expected_headers(expected);
            }
            reject
        }
        other => other,
    }
}

/// Build a fresh per-peer ingest [`HsLocal`] seeded from `env` using the default
/// inbound rate intervals.
///
/// Used by the synthetic in-process cluster harness, which has no command channel
/// and constructs one `HsLocal` per remote source peer. Production builds `HsLocal`
/// directly in [`HeaderSyncService::add_peer`](super::service::HeaderSyncService)
/// with the live command receiver.
///
/// The cluster harness exercises the reactor-side global `NewBlock` dedup
/// (seen/pending sets) and the no-double-gossip property, which are a *different*
/// layer than the routine's pre-decode `NewBlock` rate gate. The pre-decode gate is
/// covered by its own routine-harness test, so this ingest state disables it (zero
/// interval) to keep the harness focused on reactor-side dedup; the status spam gate
/// keeps its real interval so hostile-status e2e flows still classify spam.
#[cfg(test)]
pub(crate) fn new_ingest_local(env: &HsEnv) -> HsLocal {
    let (_commands_tx, commands_rx) = mpsc::unbounded_channel();
    HsLocal::new(
        commands_rx,
        env.anchor_height(),
        DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
        Duration::ZERO,
    )
}

/// Run the relocated peer-local validation for one decoded stream-5 message and
/// emit the narrowed shared-effect event(s) to the reactor.
///
/// This is where the chunk-03 demux removal lives: each wire variant is validated
/// against this peer's local state and either forwarded as a narrowed shared
/// effect (`PeerStatusUpdated`, `PeerHeadersReceived`, `InboundGetHeadersRequested`,
/// `NewBlockCandidate`) or rejected as `PeerMisbehavior`. The reactor never sees a
/// raw decoded wire message.
pub(super) fn ingest_message(
    local: &mut HsLocal,
    env: &HsEnv,
    peer_id: &ZakuraPeerId,
    msg: HeaderSyncMessage,
) -> Flow<()> {
    match msg {
        HeaderSyncMessage::Status(status) => ingest_status(local, env, peer_id, status),
        HeaderSyncMessage::Headers {
            headers,
            body_sizes,
        } => ingest_headers(env, peer_id, headers, body_sizes),
        HeaderSyncMessage::GetHeaders {
            start_height,
            count,
        } => ingest_get_headers(local, env, peer_id, start_height, count),
        HeaderSyncMessage::NewBlock(block) => forward(
            &env.handle,
            HeaderSyncEvent::NewBlockCandidate {
                peer: peer_id.clone(),
                block,
            },
        ),
    }
}

/// Peer-local `Status` validation: anchor/tip ordering, the status-spam rate gate,
/// and advertised-cap clamping. A valid status updates this peer's advertised tip
/// and is forwarded as a clamped [`HeaderSyncEvent::PeerStatusUpdated`].
fn ingest_status(
    local: &mut HsLocal,
    env: &HsEnv,
    peer_id: &ZakuraPeerId,
    status: HeaderSyncStatus,
) -> Flow<()> {
    metrics::counter!("sync.header.peer.status.received").increment(1);
    if status.anchor_height > status.tip_height {
        // A decoded `Status` with anchor past tip is semantically invalid but was
        // record-only at the baseline (reactor `report_misbehavior`): report it and
        // keep the connection.
        return record_misbehavior(env, peer_id, HeaderSyncMisbehavior::InvalidStatus);
    }

    // A status is applied if it advances this peer's advertised tip OR the spam
    // rate meter allows it; otherwise it is redundant-or-spammy traffic.
    let advances_advertised_tip = status.tip_height > local.advertised_tip;
    let status_token_available = local.inbound_status_meter.try_take(Instant::now());
    if !advances_advertised_tip && !status_token_available {
        // Status spam is a decoded-message rate violation: record-only at the
        // baseline, so report it and keep the connection.
        return record_misbehavior(env, peer_id, HeaderSyncMisbehavior::StatusSpam);
    }

    local.advertised_tip = status.tip_height;
    local.received_status = true;
    let clamped = HeaderSyncStatus {
        max_headers_per_response: clamp_advertised_range(status.max_headers_per_response),
        max_inflight_requests: status
            .max_inflight_requests
            .clamp(1, LOCAL_MAX_HS_INFLIGHT_PER_PEER),
        ..status
    };
    forward(
        &env.handle,
        HeaderSyncEvent::PeerStatusUpdated {
            peer: peer_id.clone(),
            status: clamped,
        },
    )
}

/// Peer-local `Headers` shape validation: the body-size/header-count parity check.
/// The decode already capped the response against the correlated request count, so
/// the routine only forwards the validated, correlated response as
/// [`HeaderSyncEvent::PeerHeadersReceived`]. The reactor matches it against its
/// outstanding range and drives the link/stateless/checkpoint validation and
/// commit pipeline (range bookkeeping stays reactor-side).
fn ingest_headers(
    env: &HsEnv,
    peer_id: &ZakuraPeerId,
    headers: Vec<Arc<block::Header>>,
    body_sizes: Vec<u32>,
) -> Flow<()> {
    metrics::counter!("sync.header.response.received").increment(1);
    if validate_body_sizes_len(headers.len(), body_sizes.len()).is_err() {
        // A body-size/header-count parity check on an ALREADY-DECODED `Headers` is a
        // semantic shape violation, not a decode failure. At the baseline this was
        // the reactor's record-only `report_misbehavior(MalformedMessage)`, so report
        // it and keep the connection (the correlated-`Headers` *decode* failure, which
        // does disconnect, is handled in `decode_and_ingest`).
        return record_misbehavior(env, peer_id, HeaderSyncMisbehavior::MalformedMessage);
    }
    forward(
        &env.handle,
        HeaderSyncEvent::PeerHeadersReceived {
            peer: peer_id.clone(),
            headers,
            body_sizes,
        },
    )
}

/// Peer-local inbound `GetHeaders` gates owned by the routine: the received-status
/// gate and the requested-count cap. A request that passes both is forwarded as
/// [`HeaderSyncEvent::InboundGetHeadersRequested`]; the reactor accounts the
/// inbound serving slot (the stateful inflight budget tied to backend completion
/// stays reactor-side) and dispatches the state query.
fn ingest_get_headers(
    local: &mut HsLocal,
    env: &HsEnv,
    peer_id: &ZakuraPeerId,
    start_height: block::Height,
    count: u32,
) -> Flow<()> {
    if !local.received_status {
        // A `GetHeaders` before any status is a decoded-message spam violation:
        // record-only at the baseline, so report it and keep the connection.
        return record_misbehavior(env, peer_id, HeaderSyncMisbehavior::GetHeadersSpam);
    }

    let allowed_count = env.inbound_get_headers_count_limit();
    if count == 0 || count > allowed_count {
        // An out-of-bounds requested count on a decoded `GetHeaders` is record-only
        // at the baseline: report it and keep the connection.
        return record_misbehavior(env, peer_id, HeaderSyncMisbehavior::GetHeadersTooLong);
    }

    forward(
        &env.handle,
        HeaderSyncEvent::InboundGetHeadersRequested {
            peer: peer_id.clone(),
            start_height,
            count,
        },
    )
}

/// Emit a routine-classified [`HeaderSyncEvent::PeerMisbehavior`] and reject the
/// peer with a protocol-fatal [`SinkReject`].
///
/// ONLY for the hard protocol failures that disconnected the connection at the
/// chunk-02 baseline: a frame that fails to DECODE (control or correlated
/// `Headers`), and a `Headers` frame with no recorded expectation
/// (`UnsolicitedHeaders`). These are violations the peer commits *before* a
/// well-formed message exists, so the connection is torn down.
///
/// A decoded-but-semantically-invalid message (invalid/spammy `Status`,
/// `GetHeaders` spam/too-long, a body-size check on an already-decoded `Headers`)
/// is RECORD-ONLY at the baseline — it was forwarded to the reactor, which never
/// cancelled the session. Use [`record_misbehavior`] for those: the routine still
/// reports the misbehavior but keeps the connection alive (the reactor owns the
/// disconnect decision, which is currently record-only).
///
/// The misbehavior is classified *before* the shared event is emitted (the reactor
/// only aggregates and owns the disconnect decision). The `error` is used only for
/// the protocol-reject diagnostic and a debug log; misbehavior reporting needs only
/// the peer and the reason.
fn reject_misbehavior(
    env: &HsEnv,
    peer_id: &ZakuraPeerId,
    reason: HeaderSyncMisbehavior,
    error: &HeaderSyncWireError,
) -> Flow<()> {
    tracing::debug!(
        ?peer_id,
        ?reason,
        ?error,
        "invalid Zakura header-sync message"
    );
    let _ = env.handle.try_send(HeaderSyncEvent::PeerMisbehavior {
        peer: peer_id.clone(),
        reason,
    });
    let protocol_error = std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string());
    Flow::Reject(SinkReject::protocol(protocol_error))
}

/// Emit a routine-classified [`HeaderSyncEvent::PeerMisbehavior`] for a
/// decoded-but-semantically-invalid message WITHOUT disconnecting the peer.
///
/// This is the record-only sibling of [`reject_misbehavior`]. At the chunk-02
/// baseline these classifications (invalid/spammy `Status`, `GetHeaders`
/// spam/too-long, a body-size parity check on an already-decoded `Headers`) were
/// forwarded to the reactor and handled by its record-only `report_misbehavior`,
/// which traces/aggregates the violation but never cancels the session (peer
/// scoring no longer drives disconnects). The routine preserves that exactly: it
/// still surfaces the same `PeerMisbehavior { peer, reason }` event so the
/// reactor's aggregation/tracing sees it, then continues processing frames.
fn record_misbehavior(
    env: &HsEnv,
    peer_id: &ZakuraPeerId,
    reason: HeaderSyncMisbehavior,
) -> Flow<()> {
    let _ = env.handle.try_send(HeaderSyncEvent::PeerMisbehavior {
        peer: peer_id.clone(),
        reason,
    });
    Flow::Continue(())
}

/// One inbound input the routine's recv loop selects over.
enum HsRoutineInput {
    /// An inbound stream-5 frame to admit, decode, correlate, and forward.
    Frame(Frame),
    /// A peer command (e.g. `RecordExpectedHeaders`) from shared scheduling state.
    Command(HeaderSyncPeerCommand),
    /// Cancellation or stream close — the routine exits cleanly.
    Done,
}

/// The concrete production owner of one admitted header-sync peer's stream-5 recv
/// loop.
///
/// The routine owns the local decode/correlation state ([`HsLocal`] — the
/// expected-`Headers` FIFO, the command receiver, the pre-decode `NewBlock` rate
/// gate, and the peer-local advertised caps + status-spam meter), the inbound
/// stream-guard admission, the frame decode, and the relocated peer-local protocol
/// validation. It drives the per-peer [`Pipe`] one frame at a time, forwarding only
/// narrowed shared-effect events to [`HeaderSyncReactor`](super::reactor); shared
/// global scheduling, direct `GetHeaders` sends, and direct
/// status/`NewBlock`/`Headers` response sends stay reactor-side (chunks 04/05).
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
pub(super) struct HeaderSyncPeerRoutine {
    /// The per-peer pipe: it owns this peer's [`HsLocal`] decode/correlation
    /// state, its session guard (oversize admission), and the
    /// correlate→decode→emit entry. The routine drives it one frame at a time
    /// through [`Pipe::run_one`].
    pipe: Pipe<HsLocal, HsEnv>,
    /// This peer's ordered stream-5 frame reader, owned and drained by the routine.
    recv: FramedRecv,
    /// The peer's service-session cancellation token. Fires on disconnect, park,
    /// or local shutdown; the routine then exits cleanly.
    cancel: CancellationToken,
}

impl HeaderSyncPeerRoutine {
    /// Build the routine around a peer's pipe, its inbound reader, and its
    /// service-session cancellation token.
    pub(super) fn new(
        pipe: Pipe<HsLocal, HsEnv>,
        recv: FramedRecv,
        cancel: CancellationToken,
    ) -> Self {
        Self { pipe, recv, cancel }
    }

    /// Run the routine until stream close, cancellation, or a reject.
    ///
    /// A protocol reject is the only `Err` returned (a closed-queue `Local` is
    /// mapped to a benign continue inside [`run_inbound`]); the caller composes it
    /// with [`handle_pipe_exit`](crate::zakura::handle_pipe_exit) so a protocol
    /// reject cancels the whole connection while a clean stream-end/cancel leaves
    /// it alone.
    pub(super) async fn run(mut self) -> Result<(), SinkReject> {
        loop {
            self.pipe.local_mut().drain_ready_commands();

            let input = {
                let local = self.pipe.local_mut();
                tokio::select! {
                    biased;
                    () = self.cancel.cancelled() => HsRoutineInput::Done,
                    command = local.commands.recv() => match command {
                        Some(command) => HsRoutineInput::Command(command),
                        None => HsRoutineInput::Done,
                    },
                    frame = self.recv.recv() => match frame {
                        Some(frame) => HsRoutineInput::Frame(frame),
                        None => HsRoutineInput::Done,
                    },
                }
            };

            match input {
                HsRoutineInput::Done => return Ok(()),
                HsRoutineInput::Frame(frame) => {
                    self.pipe.local_mut().drain_ready_commands();
                    match self.pipe.run_one(frame) {
                        Flow::Continue(()) | Flow::Done => {}
                        Flow::Reject(reject) => return Err(reject),
                    }
                }
                HsRoutineInput::Command(command) => self.pipe.local_mut().handle_command(command),
            }
        }
    }
}

/// Forward a narrowed shared-effect event to the reactor.
///
/// A closed/full reactor queue is a local, non-fatal condition for the peer, so
/// this returns [`Flow::Reject`] with a `Local` reason. Callers decide whether to
/// continue or surface it (see [`decode_and_ingest`]).
fn forward(handle: &HeaderSyncHandle, event: HeaderSyncEvent) -> Flow<()> {
    match handle.try_send(event) {
        Ok(()) => Flow::Continue(()),
        Err(error) => Flow::Reject(SinkReject::local(format!(
            "header-sync queue closed: {error}"
        ))),
    }
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

    /// Build a `HeaderSyncHandle` whose bounded `events` queue is already full,
    /// so the next `try_send` from the pipe fails with `Full`. The receiver is
    /// returned (and must be kept alive) so the failure is `Full`, not `Closed`.
    fn saturated_events_handle() -> (HeaderSyncHandle, mpsc::Receiver<HeaderSyncEvent>) {
        let (events, events_rx) = mpsc::channel(1);
        events
            .try_send(HeaderSyncEvent::PeerDisconnected(peer()))
            .expect("the single events slot is free");
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

    /// Wrap a reactor handle as a routine environment for the inbound decode path.
    fn test_env(handle: HeaderSyncHandle) -> HsEnv {
        HsEnv::new(
            handle,
            Network::Mainnet,
            ZakuraHeaderSyncConfig::default(),
            // MAX_HS_MESSAGE_BYTES fits the frame cap; any non-zero value works here.
            MAX_HS_MESSAGE_BYTES as u32,
        )
    }

    /// Build a fresh per-peer ingest `HsLocal` with a detached command channel.
    fn test_local() -> HsLocal {
        let (_commands_tx, commands_rx) = mpsc::unbounded_channel();
        HsLocal::new(
            commands_rx,
            block::Height(0),
            DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
        )
    }

    /// A `Headers` frame with no recorded expectation is unsolicited: the routine
    /// classifies `UnsolicitedHeaders`, emits `PeerMisbehavior`, and rejects the
    /// peer, before any decode.
    #[test]
    fn ingest_unsolicited_headers_rejects_without_expectation() {
        let (handle, mut events) = test_handle();
        let env = test_env(handle);
        let mut local = test_local();

        let flow = decode_and_ingest(&mut local, &env, peer(), headers_frame(Vec::new()));

        assert!(matches!(flow, Flow::Reject(SinkReject::Protocol(_))));
        match events.try_recv() {
            Ok(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
                assert!(matches!(reason, HeaderSyncMisbehavior::UnsolicitedHeaders));
            }
            other => panic!("expected PeerMisbehavior(UnsolicitedHeaders), got {other:?}"),
        }
    }

    /// With a recorded expectation, the same `Headers` frame is *correlated* and
    /// decoded: a malformed payload now reports `MalformedMessage`, not
    /// `UnsolicitedHeaders`, proving the expectation was consumed before decode.
    #[test]
    fn ingest_correlated_headers_decodes_against_expectation() {
        let (handle, mut events) = test_handle();
        let env = test_env(handle);
        let mut local = test_local();
        local.record_expected(
            ExpectedHeadersResponse::new(block::Height(1), 1).expect("count is valid"),
        );

        let flow = decode_and_ingest(&mut local, &env, peer(), headers_frame(Vec::new()));

        assert!(matches!(flow, Flow::Reject(SinkReject::Protocol(_))));
        match events.try_recv() {
            Ok(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
                assert!(matches!(reason, HeaderSyncMisbehavior::MalformedMessage));
            }
            other => panic!("expected PeerMisbehavior(MalformedMessage), got {other:?}"),
        }
    }

    /// The peer-local correlation queue is FIFO and is filled by draining ready
    /// commands. This is the invariant [`HeaderSyncPeerRoutine`] relies on: an
    /// expectation recorded by a `RecordExpectedHeaders` command is drained and
    /// available to pop before the matching `Headers` response is processed.
    #[test]
    fn local_correlation_queue_drains_commands_in_fifo_order() {
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let mut local = HsLocal::new(
            commands_rx,
            block::Height(0),
            DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
        );

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

    /// A `NewBlock` flood is throttled *before* full-block decode: the first
    /// frame in a window is decoded and forwarded to the reactor, but a second
    /// distinct frame inside the per-peer minimum interval is dropped before
    /// `Block::zcash_deserialize` runs, so nothing reaches the reactor and the
    /// peer is kept (`Flow::Done`). This proves the amplification gap is closed —
    /// without the pre-decode gate the second full block is deserialized and
    /// forwarded too.
    #[test]
    fn new_block_flood_is_throttled_before_decode() {
        use zebra_chain::serialization::ZcashDeserializeInto;
        use zebra_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES};

        let (handle, mut events) = test_handle();
        let (_commands_tx, commands_rx) = mpsc::unbounded_channel();

        let block_one: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .expect("block 1 vector parses"),
        );
        let block_two: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_2_BYTES
                .zcash_deserialize_into()
                .expect("block 2 vector parses"),
        );
        let frame_one = HeaderSyncMessage::NewBlock(block_one.clone())
            .encode_frame()
            .expect("new block frame encodes");
        let frame_two = HeaderSyncMessage::NewBlock(block_two.clone())
            .encode_frame()
            .expect("new block frame encodes");

        let mut pipe = Pipe::new(
            peer(),
            HsLocal::new(
                commands_rx,
                block::Height(0),
                DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
                DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
            ),
            HsEnv::new(
                handle,
                Network::Mainnet,
                ZakuraHeaderSyncConfig::default(),
                MAX_HS_MESSAGE_BYTES as u32,
            ),
            crate::zakura::SessionGuard::oversize_only(MAX_HS_MESSAGE_BYTES as u32),
            run_inbound,
            &PIPE_SHAPE,
        );

        // First flood frame: admitted, decoded, and forwarded to the reactor.
        assert!(matches!(pipe.run_one(frame_one), Flow::Continue(())));
        match events.try_recv() {
            Ok(HeaderSyncEvent::NewBlockCandidate { block, .. }) => {
                assert_eq!(block.hash(), block_one.hash())
            }
            other => panic!("expected first NewBlock to be forwarded, got {other:?}"),
        }

        // Second distinct flood frame inside the interval is dropped before
        // decode: the peer is kept and nothing reaches the reactor.
        assert!(matches!(pipe.run_one(frame_two), Flow::Done));
        assert!(
            matches!(events.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "second NewBlock must be throttled before decode, not forwarded"
        );
    }

    /// Under reactor `events`-queue saturation, a valid *solicited* `Headers`
    /// response must not silently consume its peer-local expectation. The pipe
    /// pops the expectation before decode; when the decoded response cannot be
    /// delivered to the full reactor queue, the pipe logs and continues
    /// (`Flow::Done`) — but the popped expectation is restored to the FIFO so the
    /// reactor's still-outstanding range stays correlated. Without the fix the
    /// expectation is consumed and lost, stranding the range until the request
    /// timeout and desynchronizing the peer-local FIFO from the outstanding range.
    #[test]
    fn saturated_events_queue_restores_solicited_expectation() {
        use zebra_chain::serialization::ZcashDeserializeInto;
        use zebra_test::vectors::BLOCK_MAINNET_1_BYTES;

        // Keep `_events_rx` alive so the saturated queue rejects with `Full`
        // (a live receiver), not `Closed`.
        let (handle, _events_rx) = saturated_events_handle();
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();

        let expected = ExpectedHeadersResponse::new(block::Height(1), 1).expect("count is valid");
        commands_tx
            .send(HeaderSyncPeerCommand::RecordExpectedHeaders(expected))
            .expect("pipe is alive");

        // A syntactically valid one-header solicited response: it decodes against
        // the expectation and reaches the reactor forward, where the full queue
        // turns it into a local reject.
        let block_one: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .expect("block 1 vector parses"),
        );
        let solicited_headers = HeaderSyncMessage::Headers {
            headers: vec![block_one.header.clone()],
            body_sizes: vec![0],
        }
        .encode_frame()
        .expect("headers frame encodes");

        let mut pipe = Pipe::new(
            peer(),
            HsLocal::new(
                commands_rx,
                block::Height(0),
                DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
                DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
            ),
            HsEnv::new(
                handle,
                Network::Mainnet,
                ZakuraHeaderSyncConfig::default(),
                MAX_HS_MESSAGE_BYTES as u32,
            ),
            crate::zakura::SessionGuard::oversize_only(MAX_HS_MESSAGE_BYTES as u32),
            run_inbound,
            &PIPE_SHAPE,
        );
        // Drain the recorded expectation into `HsLocal`, mirroring the routine's
        // pre-frame command drain so the `Headers` frame is correlated.
        pipe.local_mut().drain_ready_commands();

        // The decoded response cannot be delivered (events queue is full); the
        // pipe logs and continues, exactly as production does.
        assert!(matches!(pipe.run_one(solicited_headers), Flow::Done));

        // The popped expectation must be restored so the still-outstanding range
        // stays correlated. Without the fix the expectation is gone (returns None).
        assert_eq!(
            pipe.local_mut().pop_expected_headers_response(),
            Some(expected),
            "a solicited Headers response dropped on reactor queue saturation must restore its expectation"
        );
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

    // ===================== HeaderSyncPeerRoutine recv-loop tests =============

    use crate::zakura::{framed_channel, spawn_supervised_pipe, FramedSend};

    /// Build a routine around a fresh handle/commands/stream so a test can drive
    /// its recv loop. Returns the routine, the drained reactor `events` queue, the
    /// peer-side stream sender (closing it ends the stream), the command sender
    /// (records expectations), and the routine's cancel token.
    #[allow(clippy::type_complexity)]
    fn routine_with(
        handle: HeaderSyncHandle,
        cancel: CancellationToken,
    ) -> (
        HeaderSyncPeerRoutine,
        FramedSend,
        mpsc::UnboundedSender<HeaderSyncPeerCommand>,
    ) {
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let (peer_send, service_recv) = framed_channel(16);
        let pipe = Pipe::new(
            peer(),
            HsLocal::new(
                commands_rx,
                block::Height(0),
                DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
                DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
            ),
            HsEnv::new(
                handle,
                Network::Mainnet,
                ZakuraHeaderSyncConfig::default(),
                MAX_HS_MESSAGE_BYTES as u32,
            ),
            // MAX_HS_MESSAGE_BYTES is a small compile-time const that fits in u32.
            crate::zakura::SessionGuard::oversize_only(MAX_HS_MESSAGE_BYTES as u32),
            run_inbound,
            &PIPE_SHAPE,
        );
        let routine = HeaderSyncPeerRoutine::new(pipe, service_recv, cancel);
        (routine, peer_send, commands_tx)
    }

    fn one_header_response_frame() -> Frame {
        use zebra_chain::serialization::ZcashDeserializeInto;
        use zebra_test::vectors::BLOCK_MAINNET_1_BYTES;

        let block_one: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .expect("block 1 vector parses"),
        );
        HeaderSyncMessage::Headers {
            headers: vec![block_one.header.clone()],
            body_sizes: vec![0],
        }
        .encode_frame()
        .expect("headers frame encodes")
    }

    /// Cancelling the routine's token drives its recv loop out cleanly (`Ok`),
    /// even while the stream's send half is still alive (so the exit is the
    /// cancellation, not a stream close).
    #[tokio::test]
    async fn routine_exits_cleanly_on_service_cancellation() {
        let (handle, _events) = test_handle();
        let cancel = CancellationToken::new();
        // Keep the peer-side send half alive so the stream does not close on its
        // own; the routine must exit because of the cancellation.
        let (routine, _peer_send, _commands_tx) = routine_with(handle, cancel.clone());
        let run = tokio::spawn(routine.run());

        cancel.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("a cancelled routine exits promptly")
            .expect("the routine task does not panic");
        assert!(
            result.is_ok(),
            "service cancellation is a clean exit, not a reject"
        );
    }

    /// Closing the peer's send half (stream close) ends `recv.recv()` with `None`,
    /// so the routine exits cleanly (`Ok`).
    #[tokio::test]
    async fn routine_exits_cleanly_on_stream_close() {
        let (handle, _events) = test_handle();
        let cancel = CancellationToken::new();
        let (routine, peer_send, _commands_tx) = routine_with(handle, cancel);
        let run = tokio::spawn(routine.run());

        drop(peer_send);
        let result = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("a stream-closed routine exits promptly")
            .expect("the routine task does not panic");
        assert!(
            result.is_ok(),
            "a stream close is a clean exit, not a reject"
        );
    }

    /// A protocol-invalid frame (an unsolicited `Headers` with no recorded
    /// expectation) makes the routine return `Err(SinkReject::Protocol)`, which the
    /// supervised pipe turns into a connection cancellation. This is the production
    /// recv-loop's decode-failure path, not the bare `deliver` unit.
    #[tokio::test]
    async fn routine_protocol_reject_disconnects() {
        let (handle, mut events) = test_handle();
        let cancel = CancellationToken::new();
        let (routine, peer_send, _commands_tx) = routine_with(handle, cancel);
        let run = tokio::spawn(routine.run());

        // An unsolicited `Headers` frame has no recorded expectation; the routine
        // rejects it as a protocol failure.
        peer_send
            .send(headers_frame(Vec::new()))
            .await
            .expect("the stream has capacity");

        let result = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("the routine rejects promptly")
            .expect("the routine task does not panic");
        assert!(
            matches!(result, Err(SinkReject::Protocol(_))),
            "an unsolicited Headers frame is a protocol reject"
        );
        // The misbehavior was reported to the reactor exactly as `deliver` does.
        match events.try_recv() {
            Ok(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
                assert!(matches!(reason, HeaderSyncMisbehavior::UnsolicitedHeaders));
            }
            other => panic!("expected PeerMisbehavior(UnsolicitedHeaders), got {other:?}"),
        }
    }

    /// A local-only failure (the reactor `events` queue is full) is NOT the peer's
    /// fault: the routine logs and continues without rejecting or scoring the peer,
    /// and a subsequent cancellation still exits cleanly (`Ok`). This proves a
    /// closed/full reactor queue never falsely blames the peer.
    #[tokio::test]
    async fn routine_local_failure_does_not_falsely_score_or_disconnect() {
        // Keep `_events_rx` alive so the saturated queue rejects with `Full`,
        // mapping to `SinkReject::Local` inside `forward`.
        let (handle, _events_rx) = saturated_events_handle();
        let cancel = CancellationToken::new();
        let (routine, peer_send, commands_tx) = routine_with(handle, cancel.clone());
        let run = tokio::spawn(routine.run());

        // Record the expectation, then send the matching solicited response. The
        // routine drains the command, correlates, decodes, and tries to forward —
        // the full queue turns the forward into a `Local` reject, which the routine
        // logs and continues past (it does not reject the peer).
        let expected = ExpectedHeadersResponse::new(block::Height(1), 1).expect("count is valid");
        commands_tx
            .send(HeaderSyncPeerCommand::RecordExpectedHeaders(expected))
            .expect("the routine is alive");
        peer_send
            .send(one_header_response_frame())
            .await
            .expect("the stream has capacity");

        // The routine is still running (it did not reject on the local failure);
        // cancelling it is the only thing that makes it exit, and it exits cleanly.
        cancel.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("the routine exits promptly after cancellation")
            .expect("the routine task does not panic");
        assert!(
            result.is_ok(),
            "a local (full reactor queue) failure must not reject the peer"
        );
    }

    /// Regression guard for the chunk-03 disconnect-vs-record-only split: a decoded
    /// but semantically-invalid header-sync message (here `GetHeadersSpam` — an
    /// inbound `GetHeaders` before any status) must be RECORD-ONLY, exactly as the
    /// chunk-02 baseline forwarded it to the reactor's never-disconnecting
    /// `report_misbehavior`. The routine emits `PeerMisbehavior` but does NOT return
    /// a `SinkReject::Protocol` and does NOT cancel the connection: it keeps running
    /// and processes the next frame. Only a cancellation makes it exit, cleanly.
    #[tokio::test]
    async fn routine_records_semantic_misbehavior_without_disconnecting() {
        let (handle, mut events) = test_handle();
        let cancel = CancellationToken::new();
        let (routine, peer_send, _commands_tx) = routine_with(handle, cancel.clone());
        let run = tokio::spawn(routine.run());

        // An inbound `GetHeaders` before any status is `GetHeadersSpam`: a decoded
        // message that is semantically invalid, which is record-only at the baseline.
        let spam_get_headers = || {
            HeaderSyncMessage::GetHeaders {
                start_height: block::Height(1),
                count: 1,
            }
            .encode_frame()
            .expect("get_headers frame encodes")
        };

        peer_send
            .send(spam_get_headers())
            .await
            .expect("the stream has capacity");

        // The misbehavior is reported as a narrowed `PeerMisbehavior` event, but the
        // connection is NOT cancelled by the routine.
        let first = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("the routine reports the misbehavior promptly");
        match first {
            Some(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
                assert_eq!(reason, HeaderSyncMisbehavior::GetHeadersSpam);
            }
            other => panic!("expected PeerMisbehavior(GetHeadersSpam), got {other:?}"),
        }
        assert!(
            !cancel.is_cancelled(),
            "record-only semantic misbehavior must not cancel the connection token"
        );

        // The routine kept running (it did not protocol-reject and exit): a second
        // spam frame is still processed and reported, proving the loop is alive.
        peer_send
            .send(spam_get_headers())
            .await
            .expect("the routine is still consuming frames");
        let second = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("the routine is still running and reports the second misbehavior");
        assert!(
            matches!(
                second,
                Some(HeaderSyncEvent::PeerMisbehavior {
                    reason: HeaderSyncMisbehavior::GetHeadersSpam,
                    ..
                })
            ),
            "the routine keeps processing frames after a record-only misbehavior"
        );

        // The only thing that ends the routine is the cancellation, and it is a clean
        // `Ok` exit (never an `Err(SinkReject::Protocol)`).
        cancel.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("the routine exits promptly after cancellation")
            .expect("the routine task does not panic");
        assert!(
            result.is_ok(),
            "a record-only semantic misbehavior must not turn into a protocol reject"
        );
    }

    /// A panic inside the routine sends `PeerDisconnected` (via the supervised
    /// pipe's `on_teardown`) and cancels the connection token (via `on_panic`),
    /// wired exactly as `HeaderSyncService::add_peer` wires them. The panic is
    /// contained to this one task.
    #[tokio::test]
    async fn panic_after_peer_connection_disconnects_and_cancels_connection() {
        // `add_peer` sends `PeerDisconnected` from teardown over the *lifecycle*
        // channel (`send_lifecycle`), not the bounded `events` queue, so keep that
        // receiver alive here to observe the teardown send.
        let (events, _events_rx) = mpsc::channel(16);
        let (lifecycle, mut lifecycle_rx) = mpsc::unbounded_channel();
        let (_tip_tx, tip) = watch::channel((block::Height(0), block::Hash([0; 32])));
        let (_peers_tx, peers) = watch::channel(ServicePeerSnapshot::default());
        let (_candidates_tx, candidates) =
            watch::channel(ZakuraHeaderSyncCandidateState::default());
        let handle = HeaderSyncHandle {
            events,
            lifecycle,
            tip,
            peers,
            candidates,
        };

        let service_cancel = CancellationToken::new();
        let connection_cancel = CancellationToken::new();
        let (routine, _peer_send, _commands_tx) =
            routine_with(handle.clone(), service_cancel.clone());

        // Mirror `add_peer`'s teardown wiring: `on_teardown` sends
        // `PeerDisconnected`, `on_panic` cancels the connection.
        let teardown_peer = peer();
        let on_teardown = move || {
            let _ = handle.send_lifecycle(HeaderSyncEvent::PeerDisconnected(teardown_peer));
        };
        let panic_connection_cancel = connection_cancel.clone();
        let on_panic = move || panic_connection_cancel.cancel();

        let handle_task = spawn_supervised_pipe(
            peer(),
            service_cancel.clone(),
            on_teardown,
            on_panic,
            async move {
                // Hold the routine, then panic inside the supervised task.
                let _routine = routine;
                panic!("header-sync routine panics after peer connection");
            },
        );

        let join_error = handle_task
            .await
            .expect_err("a panicking routine surfaces a join error");
        assert!(
            join_error.is_panic(),
            "the routine panic is reported as a panic, not a cancellation"
        );

        // `PeerDisconnected` was sent on teardown so the reactor never leaks the
        // panicked peer's state.
        match lifecycle_rx.try_recv() {
            Ok(HeaderSyncEvent::PeerDisconnected(disconnected)) => {
                assert_eq!(disconnected, peer());
            }
            other => panic!("expected PeerDisconnected on panic teardown, got {other:?}"),
        }
        assert!(
            connection_cancel.is_cancelled(),
            "a panic after peer connection cancels the connection token"
        );
        assert!(
            service_cancel.is_cancelled(),
            "the service token is cancelled on every exit path"
        );
    }

    /// End-to-end through the routine recv loop: a `RecordExpectedHeaders` command
    /// queued before the matching `Headers` frame arrives is drained and popped
    /// before the frame is decoded, so the solicited response is correlated (it
    /// reports `MalformedMessage` against the expectation, never the pre-correlation
    /// `UnsolicitedHeaders`). This proves the routine's ordering: the expectation is
    /// recorded before any matching frame can be decoded.
    #[tokio::test]
    async fn routine_records_expectation_before_decoding_matching_headers() {
        let (handle, mut events) = test_handle();
        let cancel = CancellationToken::new();
        let (routine, peer_send, commands_tx) = routine_with(handle, cancel.clone());
        let run = tokio::spawn(routine.run());

        // Record the expectation, then send an empty (malformed) solicited Headers
        // frame. The routine must correlate it against the expectation: a
        // correlated-but-malformed response reports `MalformedMessage` and rejects;
        // an UNcorrelated one would report `UnsolicitedHeaders`.
        let expected = ExpectedHeadersResponse::new(block::Height(1), 1).expect("count is valid");
        commands_tx
            .send(HeaderSyncPeerCommand::RecordExpectedHeaders(expected))
            .expect("the routine is alive");
        peer_send
            .send(headers_frame(Vec::new()))
            .await
            .expect("the stream has capacity");

        let result = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("the routine processes the frame promptly")
            .expect("the routine task does not panic");
        assert!(
            matches!(result, Err(SinkReject::Protocol(_))),
            "a malformed correlated response is still a protocol reject"
        );
        match events.try_recv() {
            Ok(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
                assert!(
                    matches!(reason, HeaderSyncMisbehavior::MalformedMessage),
                    "the expectation was popped before decode (MalformedMessage, not UnsolicitedHeaders)"
                );
            }
            other => panic!("expected PeerMisbehavior(MalformedMessage), got {other:?}"),
        }
        cancel.cancel();
    }

    /// Multiple `RecordExpectedHeaders` commands the routine drains preserve FIFO
    /// order: the first-recorded expectation is the first popped against the first
    /// matching `Headers` frame. Driven through the routine's command drain (the
    /// same `drain_ready_commands` the recv loop runs before each frame).
    #[test]
    fn routine_preserves_fifo_for_multiple_expectations() {
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let mut local = HsLocal::new(
            commands_rx,
            block::Height(0),
            DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
        );

        let first = ExpectedHeadersResponse::new(block::Height(10), 1).expect("count is valid");
        let second = ExpectedHeadersResponse::new(block::Height(20), 2).expect("count is valid");
        let third = ExpectedHeadersResponse::new(block::Height(30), 3).expect("count is valid");
        for expected in [first, second, third] {
            commands_tx
                .send(HeaderSyncPeerCommand::RecordExpectedHeaders(expected))
                .expect("the routine is alive");
        }

        // The recv loop drains ready commands before touching a frame; after the
        // drain the FIFO pops in record order.
        local.drain_ready_commands();
        assert_eq!(local.pop_expected_headers_response(), Some(first));
        assert_eq!(local.pop_expected_headers_response(), Some(second));
        assert_eq!(local.pop_expected_headers_response(), Some(third));
        assert_eq!(local.pop_expected_headers_response(), None);
    }
}
