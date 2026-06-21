use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use tokio::{
    sync::{mpsc, watch},
    task,
};
use tokio_util::sync::CancellationToken;

use super::{events::*, pipe::*, wire::*, *};
use crate::zakura::{
    handle_pipe_exit, spawn_supervised_pipe, BoxRunFuture, Flow, Frame, FramedRecv, FramedSend,
    OrderedSendError, Peer, PeerStreamSession, Pipe, Service, ServicePeerDirection, SessionGuard,
    Sink, SinkReject, Stream, StreamMode, ZakuraPeerId, ZakuraSupervisorHandle,
    ZAKURA_CAP_HEADER_SYNC,
};

const HEADER_SYNC_SERVICE_STREAMS: [Stream; 1] = [Stream {
    kind: ZAKURA_STREAM_HEADER_SYNC,
    version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
    // Advisory until the transport wires Stream::frame_cap end-to-end; the
    // authoritative inbound cap is app_frame_cap_for_stream_kind. The cast is
    // safe because both terms are small protocol constants checked against the
    // local message cap in header_sync::wire.
    frame_cap: (MAX_HS_MESSAGE_BYTES + FRAME_HEADER_BYTES) as u32,
    capability: ZAKURA_CAP_HEADER_SYNC,
    mode: StreamMode::Ordered,
}];

/// Service-declared streams for native header sync.
pub(crate) fn header_sync_streams() -> &'static [Stream] {
    &HEADER_SYNC_SERVICE_STREAMS
}

/// Mint a fresh, process-monotone header-sync session generation. Each admitted
/// transport gets a unique generation so reconnect/timeout/reset cleanup can be
/// scoped to the exact session that owns the work.
fn next_header_sync_generation() -> u64 {
    static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
    NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)
}

/// Cloneable typed stream-5 sender and peer-local response expectations.
#[derive(Clone, Debug)]
pub struct HeaderSyncPeerSession {
    peer_id: ZakuraPeerId,
    direction: ServicePeerDirection,
    /// Session generation, minted once per admitted transport. Carried so the
    /// reactor can tag this session's `PeerHeaderState` and outstanding work.
    generation: u64,
    inner: Arc<HeaderSyncPeerSessionInner>,
}

#[derive(Debug)]
struct HeaderSyncPeerSessionInner {
    send: FramedSend,
    cancel_token: CancellationToken,
    commands: Option<HeaderSyncCommandSink>,
}

impl HeaderSyncPeerSession {
    fn new_with_commands(
        session: &PeerStreamSession,
        direction: ServicePeerDirection,
        generation: u64,
        commands: HeaderSyncCommandSink,
    ) -> Self {
        Self::from_parts_with_direction_and_commands(
            session.peer_id().clone(),
            direction,
            generation,
            session.sender(),
            session.cancel_token(),
            Some(commands),
        )
    }

    #[cfg(test)]
    pub(crate) fn from_parts(
        peer_id: ZakuraPeerId,
        send: FramedSend,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::from_parts_with_direction(peer_id, ServicePeerDirection::Inbound, send, cancel_token)
    }

    #[cfg(test)]
    pub(crate) fn from_parts_with_direction(
        peer_id: ZakuraPeerId,
        direction: ServicePeerDirection,
        send: FramedSend,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::from_parts_with_direction_and_commands(
            peer_id,
            direction,
            0,
            send,
            cancel_token,
            None,
        )
    }

    /// Test-only constructor that wires a routine command sink and generation, so a
    /// fixture can spawn a real routine and feed it reactor commands (status, headers
    /// responses, NewBlock forwards, wakes, and the reset/timeout `DropExpectation`).
    #[cfg(test)]
    pub(crate) fn from_parts_with_commands(
        peer_id: ZakuraPeerId,
        direction: ServicePeerDirection,
        generation: u64,
        send: FramedSend,
        cancel_token: CancellationToken,
        commands: HeaderSyncCommandSink,
    ) -> Self {
        Self::from_parts_with_direction_and_commands(
            peer_id,
            direction,
            generation,
            send,
            cancel_token,
            Some(commands),
        )
    }

    fn from_parts_with_direction_and_commands(
        peer_id: ZakuraPeerId,
        direction: ServicePeerDirection,
        generation: u64,
        send: FramedSend,
        cancel_token: CancellationToken,
        commands: Option<HeaderSyncCommandSink>,
    ) -> Self {
        Self {
            peer_id,
            direction,
            generation,
            inner: Arc::new(HeaderSyncPeerSessionInner {
                send,
                cancel_token,
                commands,
            }),
        }
    }

    /// Session generation minted when this transport was admitted.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Authenticated peer identity for this header-sync session.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    /// Direction of the underlying Zakura connection.
    pub fn direction(&self) -> ServicePeerDirection {
        self.direction
    }

    /// Peer disconnect/local shutdown cancellation token.
    pub fn cancel_token(&self) -> CancellationToken {
        self.inner.cancel_token.clone()
    }

    /// Send a typed status advertisement.
    pub fn try_send_status(&self, status: HeaderSyncStatus) -> Result<(), OrderedSendError> {
        self.try_send_message(HeaderSyncMessage::Status(status))
    }

    /// Send a typed header range request.
    ///
    /// As of the routine-pulled-work chunk, the requesting routine records its
    /// expected `Headers` response in its own local FIFO before calling this — the
    /// session no longer records expectations through a command channel.
    pub fn try_send_get_headers(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> Result<(), OrderedSendError> {
        // Validate the count bound exactly as before so an out-of-range request is
        // rejected at the seam rather than encoded onto the wire.
        ExpectedHeadersResponse::new(start_height, count)
            .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        self.try_send_message(HeaderSyncMessage::GetHeaders {
            start_height,
            count,
        })
    }

    /// The per-peer routine command sink, if a live routine owns this session.
    ///
    /// `None` for the test/recorder sessions that have no routine. The reactor
    /// enqueues typed commands here (status/headers/new-block/wake/reset) and the
    /// routine writes the frame on this same stream; the sink applies each command's
    /// full-queue policy (coalesce, drop, or signal park).
    pub(crate) fn commands(&self) -> Option<&HeaderSyncCommandSink> {
        self.inner.commands.as_ref()
    }

    /// Send a typed header range response.
    pub fn try_send_headers(
        &self,
        headers: Vec<Arc<block::Header>>,
    ) -> Result<(), OrderedSendError> {
        let body_sizes = vec![0; headers.len()];
        self.try_send_headers_with_sizes(headers, body_sizes)
    }

    /// Send a typed header range response with one advisory body-size hint per header.
    pub fn try_send_headers_with_sizes(
        &self,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
    ) -> Result<(), OrderedSendError> {
        self.try_send_message(HeaderSyncMessage::Headers {
            headers,
            body_sizes,
        })
    }

    /// Send a typed full tip block announcement.
    pub fn try_send_new_block(&self, block: Arc<block::Block>) -> Result<(), OrderedSendError> {
        self.try_send_message(HeaderSyncMessage::NewBlock(block))
    }

    fn try_send_message(&self, msg: HeaderSyncMessage) -> Result<(), OrderedSendError> {
        let frame = msg
            .encode_frame()
            .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        match self.inner.send.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_frame)) => Err(OrderedSendError::Full),
            Err(mpsc::error::TrySendError::Closed(_frame)) => Err(OrderedSendError::Closed),
        }
    }
}

/// Bounded depth of the per-peer routine command queue (the never-drop/best-effort
/// FIFO). Status is coalesced through a separate single-slot watch, so this queue
/// only carries `SendHeaders`, `ForwardNewBlock`, `Wake`, and `DropExpectation`.
/// A peer is admitted with at most a few in-flight inbound `GetHeaders` and one
/// outbound request at a time, so this depth comfortably absorbs a transient burst
/// before the never-drop policy parks the peer.
pub(crate) const HEADER_SYNC_COMMAND_QUEUE_DEPTH: usize = 256;

/// Commands from the reactor into one peer-owned header-sync routine.
///
/// The reactor decides WHO should receive each frame (broadcast/forwarding/backend
/// destination selection stays reactor-side); the routine writes WHAT on its own
/// stream when it dequeues the command. `Status` is delivered out-of-band through a
/// coalescing single-slot watch on the [`HeaderSyncCommandSink`], not this enum.
#[derive(Clone, Debug)]
pub(crate) enum HeaderSyncPeerCommand {
    /// Write a `Headers` response (for an inbound `GetHeaders` the reactor served
    /// from state) on this peer's stream. Must NOT be silently dropped: it is the
    /// correlated reply the peer is waiting for. On a full queue the sink signals a
    /// park instead of dropping it.
    SendHeaders {
        /// Headers to send in ascending height order.
        headers: Vec<Arc<block::Header>>,
        /// Advisory serialized body sizes, parallel to `headers`.
        body_sizes: Vec<u32>,
    },
    /// Forward an accepted, deduplicated tip `NewBlock` to this peer. Advisory tip
    /// gossip: droppable when the queue is full (the peer re-learns the tip via
    /// `Status`/sync), matching the reactor's previous best-effort `try_send`.
    ForwardNewBlock(Arc<block::Block>),
    /// Wake the routine so it re-attempts an outbound work pull. Idempotent and
    /// coalescible: dropping it when the queue already holds work is harmless
    /// because the routine re-pulls on every loop iteration.
    Wake,
    /// The reactor timed out or covered an outstanding request for this session and
    /// returned the range to the shared queue. The routine drops the matching
    /// expectation (front of its FIFO, if its generation matches) and frees its
    /// outbound slot so it can pull fresh work. Idempotent: a no-op if the response
    /// already arrived and the expectation was popped. Must NOT be silently dropped:
    /// it controls request correlation and the outbound slot, so a full queue parks
    /// the peer instead.
    DropExpectation {
        /// Session generation the timed-out request belonged to.
        generation: u64,
        /// First requested height of the timed-out range.
        start_height: block::Height,
        /// Requested count of the timed-out range.
        count: u32,
    },
}

/// The per-peer routine command outcome the reactor observes after enqueue.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum HeaderSyncCommandOutcome {
    /// The command was placed on the routine's queue (or coalesced into the status
    /// slot, fully replacing an older pending status).
    Queued,
    /// A redundant/idempotent command (status or wake) was coalesced or dropped
    /// because a newer command already covers it; no correctness is lost.
    Coalesced,
    /// An advisory command (`ForwardNewBlock`) was dropped because the queue was
    /// full; the peer re-learns the tip through `Status`/sync, so no correctness is
    /// lost.
    Dropped,
    /// There is no live routine for this session (a test/recorder session), so the
    /// command had nowhere to go.
    NoRoutine,
    /// A correctness-critical command (`SendHeaders`/`DropExpectation`) could not be
    /// delivered because the queue was full or the routine had already exited. The
    /// caller MUST park or disconnect this peer rather than continue: silently
    /// losing it would strand a correlated reply or the outbound slot.
    ParkRequired,
}

impl HeaderSyncCommandOutcome {
    /// Whether the reactor must park/disconnect the peer because a
    /// correctness-critical command could not be delivered.
    pub(crate) fn requires_park(self) -> bool {
        matches!(self, Self::ParkRequired)
    }

    /// Stable label for tracing the per-peer enqueue result.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Coalesced => "coalesced",
            Self::Dropped => "dropped",
            Self::NoRoutine => "no_routine",
            Self::ParkRequired => "parked",
        }
    }
}

/// The reactor's per-peer command sink: a coalescing single-slot status watch plus a
/// bounded FIFO for the remaining commands. Cloneable and carried on
/// [`HeaderSyncPeerSession`] so the reactor can enqueue without touching the routine.
///
/// Full-queue policy is applied per variant here so the reactor's call sites stay
/// simple and the policy is defined in one place:
///
/// - `SendStatus`: coalesced through the watch — the newest status fully replaces an
///   unread older one (a `Status` is a full snapshot, so coalescing loses nothing).
/// - `SendHeaders` / `DropExpectation`: never silently dropped — on a full queue or
///   a closed channel the sink returns [`HeaderSyncCommandOutcome::ParkRequired`] so
///   the reactor parks/disconnects the peer.
/// - `ForwardNewBlock`: advisory — dropped on a full queue.
/// - `Wake`: idempotent — coalesced/dropped on a full queue.
#[derive(Clone, Debug)]
pub(crate) struct HeaderSyncCommandSink {
    /// Coalescing single-slot status channel. `None` is the initial value; the
    /// reactor sends `Some(status)`, and the routine reacts on a change.
    status: watch::Sender<Option<HeaderSyncStatus>>,
    /// Bounded FIFO for the remaining (non-status) commands.
    commands: mpsc::Sender<HeaderSyncPeerCommand>,
}

impl HeaderSyncCommandSink {
    /// Build a sink and the matching receivers the routine owns.
    pub(crate) fn channel() -> (Self, HeaderSyncCommandReceivers) {
        let (status_tx, status_rx) = watch::channel(None);
        let (commands_tx, commands_rx) = mpsc::channel(HEADER_SYNC_COMMAND_QUEUE_DEPTH);
        (
            Self {
                status: status_tx,
                commands: commands_tx,
            },
            HeaderSyncCommandReceivers {
                status: status_rx,
                commands: commands_rx,
            },
        )
    }

    /// Coalesce a `Status` into the single-slot watch. The slot always holds only the
    /// latest value: if the routine has not yet read a prior status, this newer one
    /// fully replaces it (a `Status` is a complete snapshot, so nothing is lost). The
    /// sender cannot observe whether the routine had read the prior value, so this
    /// reports `Queued` on a successful enqueue (the coalescing is a transparent slot
    /// property) and `NoRoutine` if the routine already exited (status is advisory —
    /// a re-send goes out on reconnect, so this never forces a park).
    pub(crate) fn enqueue_status(&self, status: HeaderSyncStatus) -> HeaderSyncCommandOutcome {
        if self.status.send(Some(status)).is_err() {
            return HeaderSyncCommandOutcome::NoRoutine;
        }
        HeaderSyncCommandOutcome::Queued
    }

    /// Enqueue a `Headers` response. Correctness-critical: never silently dropped —
    /// a full queue or closed channel returns `ParkRequired`.
    pub(crate) fn enqueue_headers(
        &self,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
    ) -> HeaderSyncCommandOutcome {
        self.enqueue_never_drop(HeaderSyncPeerCommand::SendHeaders {
            headers,
            body_sizes,
        })
    }

    /// Forward an accepted tip `NewBlock`. Advisory: dropped on a full queue.
    pub(crate) fn enqueue_new_block(&self, block: Arc<block::Block>) -> HeaderSyncCommandOutcome {
        match self
            .commands
            .try_send(HeaderSyncPeerCommand::ForwardNewBlock(block))
        {
            Ok(()) => HeaderSyncCommandOutcome::Queued,
            Err(mpsc::error::TrySendError::Full(_)) => HeaderSyncCommandOutcome::Dropped,
            Err(mpsc::error::TrySendError::Closed(_)) => HeaderSyncCommandOutcome::NoRoutine,
        }
    }

    /// Wake the routine to re-attempt a pull. Idempotent: coalesced/dropped on a
    /// full queue (the routine re-pulls every loop, so a dropped wake self-heals).
    pub(crate) fn enqueue_wake(&self) -> HeaderSyncCommandOutcome {
        match self.commands.try_send(HeaderSyncPeerCommand::Wake) {
            Ok(()) => HeaderSyncCommandOutcome::Queued,
            Err(mpsc::error::TrySendError::Full(_)) => HeaderSyncCommandOutcome::Coalesced,
            Err(mpsc::error::TrySendError::Closed(_)) => HeaderSyncCommandOutcome::NoRoutine,
        }
    }

    /// Enqueue a reset/timeout `DropExpectation`. Correctness-critical: never
    /// silently dropped — a full queue or closed channel returns `ParkRequired`.
    pub(crate) fn enqueue_drop_expectation(
        &self,
        generation: u64,
        start_height: block::Height,
        count: u32,
    ) -> HeaderSyncCommandOutcome {
        self.enqueue_never_drop(HeaderSyncPeerCommand::DropExpectation {
            generation,
            start_height,
            count,
        })
    }

    /// Shared never-drop enqueue for the correctness-critical commands.
    fn enqueue_never_drop(&self, command: HeaderSyncPeerCommand) -> HeaderSyncCommandOutcome {
        match self.commands.try_send(command) {
            Ok(()) => HeaderSyncCommandOutcome::Queued,
            // A full or closed queue would silently lose a correlated reply or the
            // outbound-slot reset; the reactor must park/disconnect instead.
            Err(_) => HeaderSyncCommandOutcome::ParkRequired,
        }
    }
}

/// The receivers the routine owns: the coalescing status watch and the bounded
/// command FIFO. Built together with the [`HeaderSyncCommandSink`].
#[derive(Debug)]
pub(crate) struct HeaderSyncCommandReceivers {
    /// Coalescing single-slot status updates from the reactor.
    pub(crate) status: watch::Receiver<Option<HeaderSyncStatus>>,
    /// Bounded FIFO of non-status routine commands.
    pub(crate) commands: mpsc::Receiver<HeaderSyncPeerCommand>,
}

impl HeaderSyncCommandReceivers {
    /// Receivers with no live sender, for the test/recorder `deliver_frame` path and
    /// the in-process cluster harness that decode against an ephemeral `HsLocal`
    /// without a routine. The senders are dropped, so both channels read as closed.
    pub(crate) fn detached() -> Self {
        let (sink, receivers) = HeaderSyncCommandSink::channel();
        drop(sink);
        receivers
    }
}

/// Pump actor actions that can be satisfied at the transport/service seam.
pub(crate) async fn drive_header_sync_actions(
    mut actions: mpsc::Receiver<HeaderSyncAction>,
    handle: HeaderSyncHandle,
    // Retained so the disconnect capability stays wired into the driver, even
    // though peer scoring no longer drives disconnects (misbehavior is record-only).
    _supervisor: ZakuraSupervisorHandle,
    shutdown: CancellationToken,
) {
    loop {
        let action = tokio::select! {
            _ = shutdown.cancelled() => return,
            action = actions.recv() => {
                let Some(action) = action else {
                    return;
                };
                action
            }
        };

        match action {
            #[cfg(test)]
            HeaderSyncAction::SendMessage { .. } | HeaderSyncAction::ForwardNewBlock { .. } => {}
            HeaderSyncAction::Misbehavior { peer, reason } => {
                // Record-only: peer scoring no longer drives disconnects.
                tracing::debug!(?peer, ?reason, "recorded Zakura header-sync peer violation");
            }
            HeaderSyncAction::NewBlockReceived { peer, hash, .. } => {
                tracing::debug!(
                    ?peer,
                    ?hash,
                    "Zakura header-sync NewBlock body arrived before block-acceptance hook is wired"
                );
            }
            HeaderSyncAction::QueryHeadersByHeightRange { peer, start, count } => {
                let _ = handle
                    .send(HeaderSyncEvent::HeaderRangeResponseFinished {
                        peer,
                        start_height: start,
                        requested_count: count,
                        returned_count: 0,
                    })
                    .await;
            }
            HeaderSyncAction::CommitHeaderRange {
                peer,
                start_height,
                headers,
                ..
            } => {
                tracing::debug!(
                    ?peer,
                    ?start_height,
                    count = headers.len(),
                    "suppressing Zakura header range commit until state driver is wired"
                );
            }
            HeaderSyncAction::QueryBestHeaderTip
            | HeaderSyncAction::QueryMissingBlockBodies { .. }
            | HeaderSyncAction::BodyGaps { .. }
            | HeaderSyncAction::HeaderAdvanced { .. }
            | HeaderSyncAction::HeaderReanchored { .. } => {}
        }
    }
}

/// Native stream-5 header-sync service.
#[derive(Debug)]
pub(crate) struct HeaderSyncService {
    header_sync: HeaderSyncHandle,
    network: Network,
    config: ZakuraHeaderSyncConfig,
    max_frame_bytes: u32,
}

impl HeaderSyncService {
    pub(crate) fn new(
        header_sync: HeaderSyncHandle,
        network: Network,
        config: ZakuraHeaderSyncConfig,
        max_frame_bytes: u32,
    ) -> Self {
        Self {
            header_sync,
            network,
            config,
            max_frame_bytes,
        }
    }

    /// Build the per-peer routine environment from this service's startup facts.
    fn pipe_env(&self) -> HsEnv {
        HsEnv::new(
            self.header_sync.clone(),
            self.network.clone(),
            self.config.clone(),
            self.max_frame_bytes,
        )
    }
}

impl Service for HeaderSyncService {
    fn name(&self) -> &'static str {
        "header-sync"
    }

    fn streams(&self) -> &[Stream] {
        header_sync_streams()
    }

    fn wants_peer(
        &self,
        _peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> bool {
        // Escalation is a local-room check. First-party summary usefulness is
        // advisory and is applied by header-sync candidate selection upstream.
        let snapshot = self.header_sync.peer_snapshot();
        match direction {
            ServicePeerDirection::Inbound => snapshot.inbound_slots_free > 0,
            ServicePeerDirection::Outbound => snapshot.outbound_slots_free > 0,
        }
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some((recv, send)) = peer.take_stream(ZAKURA_STREAM_HEADER_SYNC) else {
            return;
        };

        let peer_id = peer.id.clone();
        let session = PeerStreamSession::new(
            peer_id.clone(),
            ZAKURA_STREAM_HEADER_SYNC,
            recv,
            send,
            peer.service_cancel_token(),
        );
        // The sink loop parks on the service token (a child of the connection
        // token) exactly as the old `HeaderSyncSink::run` select did. The
        // connection token is cancelled only on a protocol reject below, never on
        // a normal/parked exit — parking one service must not tear down the
        // shared connection that other services (discovery, block-sync) ride on.
        let service_cancel_token = session.cancel_token();
        let connection_cancel_token = peer.cancel_token();
        // Bounded/coalescing routine command channel. The reactor enqueues
        // status/headers/new-block/wake/reset commands here and the routine writes
        // the matching frame on this same stream; the sink owns the per-variant
        // full-queue policy (coalesce status, drop advisory NewBlock, park on a full
        // never-drop queue).
        let (command_sink, command_receivers) = HeaderSyncCommandSink::channel();
        // Mint a fresh session generation so the reactor and the shared range queue
        // can scope timeout/return/reset cleanup to this exact transport — an older
        // session's stale teardown can never disturb this one's live work.
        let generation = next_header_sync_generation();
        let header_sync_session = HeaderSyncPeerSession::new_with_commands(
            &session,
            peer.direction,
            generation,
            command_sink,
        );

        let _ = self
            .header_sync
            .send_lifecycle(HeaderSyncEvent::PeerConnected(header_sync_session.clone()));

        let (_session_peer, _stream_kind, recv, _send, _session_cancel) = session.into_parts();

        // The concrete `HeaderSyncPeerRoutine` is the production owner of this
        // peer's recv loop AND of its outbound `GetHeaders` work: it owns `HsLocal`
        // (the expected-`Headers` FIFO, the reactor command receiver, the
        // pre-decode `NewBlock` gate, and the peer-local caps/slot state), the
        // inbound stream-guard admission, and the frame decode. It pulls eligible
        // ranges directly from the shared range queue, sends `GetHeaders` on its
        // own stream, and records the expected `Headers` locally before yielding —
        // no reactor push and no command round-trip. The reactor only records the
        // matching outstanding range (via `PeerWorkAssigned`) and keeps the
        // timeout/covered/commit machinery.
        let env = self.pipe_env();
        let pipe = Pipe::new(
            peer_id.clone(),
            HsLocal::new(
                command_receivers,
                env.anchor_height(),
                DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
                DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
            ),
            env,
            SessionGuard::oversize_only(header_sync_guard_max_bytes()),
            run_inbound,
            &PIPE_SHAPE,
        );
        // The routine future reproduces the old sink's connection handling: a
        // protocol reject (the only way the routine returns `Err`, since
        // `run_inbound` maps a closed-queue `Local` to a benign continue)
        // cancels the *connection*, matching the old
        // `connection_cancel_token.cancel()` on `SinkReject::Protocol`. A normal
        // or parked exit leaves the connection alone.
        let pipe_cancel_token = service_cancel_token.clone();
        let protocol_connection_cancel_token = connection_cancel_token.clone();
        let routine = HeaderSyncPeerRoutine::new(
            pipe,
            recv,
            pipe_cancel_token,
            Some(header_sync_session),
            generation,
        );
        let pipe = async move {
            handle_pipe_exit(
                "header-sync",
                &protocol_connection_cancel_token,
                routine.run().await,
            );
        };

        // The supervised teardown runs on every exit path — normal return,
        // protocol reject, or panic. It cancels this peer's *service* token
        // (idempotent; already cancelled on a park/protocol exit) and sends
        // `PeerDisconnected`. Sending it from teardown is the latent-bug fix: the
        // old sink only sent `PeerDisconnected` on the normal return path, so a
        // panicking task leaked the peer's reactor state.
        let teardown_handle = self.header_sync.clone();
        let teardown_peer = peer_id.clone();
        let on_teardown = move || {
            let _ =
                teardown_handle.send_lifecycle(HeaderSyncEvent::PeerDisconnected(teardown_peer));
        };
        let panic_connection_cancel_token = connection_cancel_token.clone();
        let on_panic = move || panic_connection_cancel_token.cancel();

        // Reuse the single supervised launcher; let the returned handle drop to
        // detach the task (the `PipeTeardown` still runs on every exit path).
        spawn_supervised_pipe(peer_id, service_cancel_token, on_teardown, on_panic, pipe);
    }

    fn remove_peer(&self, peer: &ZakuraPeerId) {
        let _ = self
            .header_sync
            .send_lifecycle(HeaderSyncEvent::PeerDisconnected(peer.clone()));
    }

    fn deliver_frame(
        &self,
        peer_id: ZakuraPeerId,
        stream_kind: u16,
        frame: Frame,
    ) -> Result<(), SinkReject> {
        if stream_kind != ZAKURA_STREAM_HEADER_SYNC {
            return Ok(());
        }

        // The test/recorder path has no per-peer routine, so it runs the same
        // decode + peer-local validation against an ephemeral `HsLocal` with no
        // recorded expectation: a `Headers` response with no outstanding request is
        // `UnsolicitedHeaders`. A `Local` reject (closed/full reactor queue) is
        // surfaced to the registry unchanged. The ephemeral state means each frame
        // is validated in isolation (no cross-frame rate metering), which matches
        // the recorder seam's stateless semantics; chunk 07 retires this path.
        let env = self.pipe_env();
        let mut local = HsLocal::new(
            HeaderSyncCommandReceivers::detached(),
            env.anchor_height(),
            DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
        );
        match decode_and_ingest(&mut local, &env, peer_id, frame) {
            Flow::Continue(()) | Flow::Done => Ok(()),
            Flow::Reject(reject) => Err(reject),
        }
    }
}

/// Service-level oversize cap for the header-sync guard.
///
/// Matches the decode stage's `MAX_HS_MESSAGE_BYTES` threshold so the guard
/// rejects nothing the decode stage would have admitted; the transport already
/// caps frames at this payload size before they reach the service, so this is a
/// defense-in-depth bound that never changes which events fire.
fn header_sync_guard_max_bytes() -> u32 {
    // `MAX_HS_MESSAGE_BYTES` is a 2 MiB protocol constant that fits in `u32`;
    // the `const` assertion in `wire.rs` keeps it below the local message cap.
    u32::try_from(MAX_HS_MESSAGE_BYTES)
        .expect("MAX_HS_MESSAGE_BYTES is a 2 MiB constant that fits in u32")
}

/// Testkit/no-reactor mode records stream-5 inbound frames without running header sync.
#[derive(Debug)]
pub(crate) struct HeaderSyncPassthroughService {
    inner: Arc<dyn Service>,
}

impl HeaderSyncPassthroughService {
    pub(crate) fn new(inner: Arc<dyn Service>) -> Self {
        Self { inner }
    }
}

impl Service for HeaderSyncPassthroughService {
    fn name(&self) -> &'static str {
        "header-sync-passthrough"
    }

    fn streams(&self) -> &[Stream] {
        header_sync_streams()
    }

    fn wants_peer(
        &self,
        peer: &ZakuraPeerId,
        negotiated: u64,
        direction: ServicePeerDirection,
    ) -> bool {
        self.inner.wants_peer(peer, negotiated, direction)
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some((recv, _send)) = peer.take_stream(ZAKURA_STREAM_HEADER_SYNC) else {
            return;
        };

        let inner = self.inner.clone();
        let peer_id = peer.id.clone();
        let cancel_token = peer.cancel_token();

        task::spawn(async move {
            let sink = Box::new(HeaderSyncPassthroughSink {
                peer_id: peer_id.clone(),
                inner,
                cancel_token: cancel_token.clone(),
            });

            match sink.run(recv).await {
                Ok(()) => {}
                Err(SinkReject::Protocol(error)) => {
                    tracing::debug!(
                        ?error,
                        ?peer_id,
                        "header-sync passthrough rejected protocol-invalid frame"
                    );
                    cancel_token.cancel();
                }
                Err(SinkReject::Local(error)) => {
                    tracing::debug!(
                        ?error,
                        ?peer_id,
                        "header-sync passthrough could not deliver frame locally"
                    );
                }
            }
        });
    }

    fn remove_peer(&self, _peer: &ZakuraPeerId) {}

    fn deliver_frame(
        &self,
        peer_id: ZakuraPeerId,
        stream_kind: u16,
        frame: Frame,
    ) -> Result<(), SinkReject> {
        self.inner.deliver_frame(peer_id, stream_kind, frame)
    }
}

#[derive(Debug)]
struct HeaderSyncPassthroughSink {
    peer_id: ZakuraPeerId,
    inner: Arc<dyn Service>,
    cancel_token: CancellationToken,
}

impl Sink for HeaderSyncPassthroughSink {
    fn run(self: Box<Self>, mut recv: FramedRecv) -> BoxRunFuture<'static, Result<(), SinkReject>> {
        Box::pin(async move {
            loop {
                let frame = tokio::select! {
                    _ = self.cancel_token.cancelled() => return Ok(()),
                    frame = recv.recv() => {
                        let Some(frame) = frame else {
                            return Ok(());
                        };
                        frame
                    }
                };

                match self.inner.deliver_frame(
                    self.peer_id.clone(),
                    ZAKURA_STREAM_HEADER_SYNC,
                    frame,
                ) {
                    Ok(()) => {}
                    Err(SinkReject::Protocol(error)) => return Err(SinkReject::Protocol(error)),
                    Err(SinkReject::Local(error)) => {
                        tracing::debug!(
                            ?error,
                            peer_id = ?self.peer_id,
                            "header-sync passthrough could not deliver frame locally"
                        );
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::watch;

    use super::*;
    use crate::zakura::{ServicePeerSnapshot, ZakuraHeaderSyncCandidateState};

    fn peer() -> ZakuraPeerId {
        ZakuraPeerId::new(vec![7; 32]).expect("test peer id is within bounds")
    }

    /// Build a `HeaderSyncHandle` whose bounded `events` queue the test can drain.
    /// The watch frontiers are never read on the `deliver_frame` decode path, so
    /// dummy values suffice.
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
                schedule: super::scheduler::SharedHeaderRangeQueue::new(),
            },
            events_rx,
        )
    }

    /// The test/recorder `Service::deliver_frame` path has no peer session, so a
    /// `Headers` response with no outstanding request is still rejected as
    /// `UnsolicitedHeaders` (a `SinkReject::Protocol`) exactly as before the
    /// routine landed. This is the compatibility path chunk 07 retires, not now.
    fn test_service(handle: HeaderSyncHandle) -> HeaderSyncService {
        HeaderSyncService::new(
            handle,
            Network::Mainnet,
            ZakuraHeaderSyncConfig::default(),
            LOCAL_MAX_MESSAGE_BYTES,
        )
    }

    #[test]
    fn deliver_frame_rejects_unsolicited_headers_without_peer_session() {
        let (handle, mut events) = test_handle();
        let service = test_service(handle);

        let headers_frame = Frame {
            message_type: u16::from(MSG_HS_HEADERS),
            flags: 0,
            payload: Vec::new(),
        };

        let result = service.deliver_frame(peer(), ZAKURA_STREAM_HEADER_SYNC, headers_frame);

        assert!(
            matches!(result, Err(SinkReject::Protocol(_))),
            "an unsolicited Headers frame with no peer session is a protocol reject"
        );
        match events.try_recv() {
            Ok(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
                assert!(matches!(reason, HeaderSyncMisbehavior::UnsolicitedHeaders));
            }
            other => panic!("expected PeerMisbehavior(UnsolicitedHeaders), got {other:?}"),
        }
    }

    /// A frame for a different stream kind is ignored by `deliver_frame` (returns
    /// `Ok`), unchanged by the routine landing.
    #[test]
    fn deliver_frame_ignores_other_stream_kinds() {
        let (handle, _events) = test_handle();
        let service = test_service(handle);

        let frame = Frame {
            message_type: u16::from(MSG_HS_HEADERS),
            flags: 0,
            payload: Vec::new(),
        };

        // A non-header-sync stream kind is not this service's concern.
        assert!(service
            .deliver_frame(peer(), ZAKURA_STREAM_HEADER_SYNC + 1, frame)
            .is_ok());
    }

    // ============== outbound-command sink full-queue policy ==============

    fn a_status() -> HeaderSyncStatus {
        HeaderSyncStatus {
            tip_height: block::Height(10),
            tip_hash: block::Hash([1; 32]),
            anchor_height: block::Height(0),
            max_headers_per_response: 100,
            max_inflight_requests: 1,
        }
    }

    /// Fill the bounded command queue right up to its depth by draining nothing, so
    /// the next never-drop enqueue must report `ParkRequired`.
    fn fill_command_queue(sink: &HeaderSyncCommandSink) {
        for _ in 0..HEADER_SYNC_COMMAND_QUEUE_DEPTH {
            assert_eq!(
                sink.enqueue_wake(),
                HeaderSyncCommandOutcome::Queued,
                "the queue accepts commands until it is full"
            );
        }
    }

    /// A `Headers` response is correctness-critical: a full queue parks the peer,
    /// never silently dropping the correlated reply.
    #[test]
    fn headers_command_parks_on_full_queue_never_dropped() {
        let (sink, _receivers) = HeaderSyncCommandSink::channel();
        fill_command_queue(&sink);
        assert_eq!(
            sink.enqueue_headers(Vec::new(), Vec::new()),
            HeaderSyncCommandOutcome::ParkRequired,
            "a Headers response must never be silently dropped on a full queue"
        );
        assert!(sink.enqueue_headers(Vec::new(), Vec::new()).requires_park());
    }

    /// A reset/timeout `DropExpectation` is correctness-critical: a full queue parks
    /// the peer rather than stranding the outbound slot.
    #[test]
    fn drop_expectation_command_parks_on_full_queue_never_dropped() {
        let (sink, _receivers) = HeaderSyncCommandSink::channel();
        fill_command_queue(&sink);
        assert_eq!(
            sink.enqueue_drop_expectation(7, block::Height(1), 1),
            HeaderSyncCommandOutcome::ParkRequired,
            "a reset/generation command must never be silently dropped on a full queue"
        );
    }

    /// A `NewBlock` forward is advisory: a full queue drops it (the peer re-learns
    /// the tip via Status/sync), and the policy never parks.
    #[test]
    fn new_block_command_drops_on_full_queue_without_parking() {
        use zebra_chain::serialization::ZcashDeserializeInto;
        use zebra_test::vectors::BLOCK_MAINNET_1_BYTES;

        let (sink, _receivers) = HeaderSyncCommandSink::channel();
        let block: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .expect("block 1 vector parses"),
        );
        fill_command_queue(&sink);
        let outcome = sink.enqueue_new_block(block);
        assert_eq!(
            outcome,
            HeaderSyncCommandOutcome::Dropped,
            "advisory NewBlock forwarding is dropped on a full queue"
        );
        assert!(!outcome.requires_park(), "dropping NewBlock must not park");
    }

    /// A wake is idempotent: a full queue coalesces it (reported as `Coalesced`,
    /// never parked) because the routine re-pulls every loop iteration anyway.
    #[test]
    fn wake_command_coalesces_on_full_queue_without_parking() {
        let (sink, _receivers) = HeaderSyncCommandSink::channel();
        fill_command_queue(&sink);
        let outcome = sink.enqueue_wake();
        assert_eq!(outcome, HeaderSyncCommandOutcome::Coalesced);
        assert!(!outcome.requires_park(), "a coalesced wake must not park");
    }

    /// Status is coalesced through the single-slot watch: a second unread status
    /// fully replaces the first, and the routine only ever observes the latest value.
    /// A status is never parked, and the coalescing loses nothing because a `Status`
    /// is a complete snapshot.
    #[test]
    fn status_command_coalesces_newest_replaces_unread() {
        let (sink, receivers) = HeaderSyncCommandSink::channel();
        let first = a_status();
        let mut second = a_status();
        second.tip_height = block::Height(20);

        // Two enqueues before the routine reads: the slot holds only the latest.
        assert_eq!(sink.enqueue_status(first), HeaderSyncCommandOutcome::Queued);
        assert_eq!(
            sink.enqueue_status(second),
            HeaderSyncCommandOutcome::Queued
        );
        assert!(!sink.enqueue_status(second).requires_park());

        // The routine reads only the latest coalesced value — the first was dropped
        // because the newer status fully replaced it.
        let mut status_rx = receivers.status;
        assert_eq!(*status_rx.borrow_and_update(), Some(second));
    }

    /// A sink whose routine has exited (receivers dropped) reports a closed channel,
    /// never a false `Queued`. Never-drop commands then surface `ParkRequired`.
    #[test]
    fn closed_channel_surfaces_no_routine_and_park_required() {
        let (sink, receivers) = HeaderSyncCommandSink::channel();
        drop(receivers);
        assert_eq!(
            sink.enqueue_status(a_status()),
            HeaderSyncCommandOutcome::NoRoutine
        );
        assert_eq!(sink.enqueue_wake(), HeaderSyncCommandOutcome::NoRoutine);
        // A correctness-critical command on a dead routine must park, not be lost.
        assert!(sink.enqueue_headers(Vec::new(), Vec::new()).requires_park());
        assert!(sink
            .enqueue_drop_expectation(1, block::Height(1), 1)
            .requires_park());
    }

    /// One peer's full command queue does not affect a second peer's independent
    /// sink: backpressure on one routine never wedges another.
    #[test]
    fn full_queue_on_one_sink_does_not_affect_another() {
        let (slow, _slow_receivers) = HeaderSyncCommandSink::channel();
        let (fast, _fast_receivers) = HeaderSyncCommandSink::channel();
        fill_command_queue(&slow);
        assert!(slow.enqueue_headers(Vec::new(), Vec::new()).requires_park());
        // The unrelated sink still accepts a never-drop command.
        assert_eq!(
            fast.enqueue_headers(Vec::new(), Vec::new()),
            HeaderSyncCommandOutcome::Queued,
            "a full queue on one peer must not block an unrelated peer's sink"
        );
    }
}
