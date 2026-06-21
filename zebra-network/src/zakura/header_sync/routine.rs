//! header_sync/routine.rs — the per-peer header-sync routine (stream 5).
//!
//! Each connected peer is driven by one concrete [`HeaderSyncPeerRoutine`] that
//! owns its transport read, decodes each stream-5 frame inline, and runs the
//! work-pull/correlation logic in the same task — there is no generic pipe runner
//! and no reactor inbound demux. The inbound shape is:
//!
//!  pull(shared queue) ─▶ record expected ─▶ send GetHeaders ─▶ emit(PeerWorkAssigned)
//!  recv ─▶ guard ─┬─ Headers ─▶ expected_headers.pop_front ─▶ decode ─▶ validate ─▶ emit(narrowed)
//!                 └─ Control ───────────────────────────────▶ decode ─▶ validate ─▶ emit(narrowed)
//!
//! Request/response correlation lives in [`HsLocal`]. The routine PULLS its own
//! outbound work: when the shared range queue signals work may be available, the
//! routine takes an eligible range, records the expected `Headers` locally, sends
//! the `GetHeaders` on its own stream, and emits [`PeerWorkAssigned`] so the reactor
//! records the matching outstanding range — all synchronously before yielding, so a
//! network response can never beat the local expectation. The reactor no longer
//! pushes the decision or records the expectation through a command.
//!
//! The concrete production owner of this loop is [`HeaderSyncPeerRoutine`]: it
//! owns the [`HsLocal`] decode/correlation state, the peer-local advertised caps,
//! the outbound-slot/empty-retry state, and rate meters, the inbound stream-guard
//! admission, the work pull, the frame decode, and the relocated peer-local protocol
//! validation. After validating, it forwards only NARROWED shared-effect events
//! ([`HeaderSyncEvent::PeerStatusUpdated`], [`PeerWorkAssigned`], [`PeerHeadersReceived`],
//! [`InboundGetHeadersRequested`], [`NewBlockCandidate`], [`PeerMisbehavior`]) to the
//! reactor; the reactor no longer matches a raw decoded wire message. The reactor
//! still owns commit ordering, timeouts (it enqueues a `DropExpectation` command on
//! timeout), and covered-range cleanup. As of the outbound-commands chunk the routine
//! also owns ALL outbound stream writes: the reactor selects the destination and
//! enqueues a typed command (`SendStatus`/`SendHeaders`/`ForwardNewBlock`), and the
//! routine writes the frame on its own stream in its `select!` loop.
//!
//! [`PeerWorkAssigned`]: super::events::HeaderSyncEvent::PeerWorkAssigned
//!
//! [`PeerHeadersReceived`]: super::events::HeaderSyncEvent::PeerHeadersReceived
//! [`InboundGetHeadersRequested`]: super::events::HeaderSyncEvent::InboundGetHeadersRequested
//! [`NewBlockCandidate`]: super::events::HeaderSyncEvent::NewBlockCandidate
//! [`PeerMisbehavior`]: super::events::HeaderSyncEvent::PeerMisbehavior

use std::{collections::VecDeque, sync::Arc};

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::{
    config::*,
    events::*,
    scheduler::*,
    service::{HeaderSyncCommandReceivers, HeaderSyncPeerCommand},
    state::*,
    validation::*,
    wire::*,
    *,
};
use crate::zakura::{Frame, FramedRecv, SessionGuard, SinkReject, ZakuraPeerId};

/// Outcome of decoding/ingesting one admitted inbound stream-5 frame.
///
/// The routine owns its own inbound control flow, so this small local enum
/// replaces the generic per-stage `Flow`: `Continue` keeps the routine running,
/// `Done` finishes this frame cleanly (throttled/deduped/late-covered — *not* an
/// error), and `Reject` carries the [`SinkReject`] that ends the routine
/// (`Protocol` tears the connection down; `Local` is the peer's non-fault).
#[derive(Debug)]
pub(crate) enum Ingest {
    /// Continue processing frames; the decoded message was forwarded.
    Continue,
    /// Finish this frame cleanly without rejecting the peer.
    Done,
    /// Reject the peer; the routine returns this to the caller.
    Reject(SinkReject),
}

#[derive(Debug)]
pub(crate) struct HsLocal {
    /// Plain peer-local response expectations, owned by this pipe task.
    expected_headers: VecDeque<ExpectedHeadersResponse>,
    /// Bounded command FIFO from the reactor into this peer-local routine: outbound
    /// `Headers` responses to write, accepted `NewBlock` forwards, wakes, and the
    /// timeout/covered `DropExpectation` reset. Each command's full-queue policy is
    /// applied by the reactor's [`HeaderSyncCommandSink`] before it lands here.
    commands: mpsc::Receiver<HeaderSyncPeerCommand>,
    /// Coalescing single-slot status updates from the reactor. The reactor decides
    /// what `Status` to advertise and to whom; the routine writes it on this peer's
    /// stream. A newer status fully replaces an unread older one (a `Status` is a
    /// full snapshot), so the routine only ever sends the latest.
    status: watch::Receiver<Option<HeaderSyncStatus>>,
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
    /// This peer's clamped advertised `max_headers_per_response`, retained so the
    /// routine can clamp its own outbound `GetHeaders` count when it pulls work.
    /// `None` until the first valid `Status` advances it.
    advertised_max_headers_per_response: Option<u32>,
    /// The single range this routine has taken from the shared queue and is
    /// awaiting a response for. `Some` exactly while an outbound `GetHeaders` is
    /// outstanding (the per-peer effective inflight cap is 1), so it doubles as the
    /// slot gate and as the work to return on teardown.
    in_flight: Option<RangeRequest>,
    /// While set, the routine declines to pull new work until this instant: the
    /// peer answered with an empty `Headers`, so the slot stays occupied for a
    /// short re-fan delay, mirroring the reactor's old empty-headers re-arm.
    empty_retry_until: Option<Instant>,
    /// The outcome of the most recently decoded `Headers` response, recorded by
    /// the ingest stage for the routine to act on after the frame is processed
    /// (clear the in-flight slot on a filled response; return the range and arm the
    /// empty-retry window on an empty one). The routine drains this after every
    /// processed frame.
    last_headers_outcome: Option<HeadersOutcome>,
    /// How many late responses for already-dropped expectations to absorb silently
    /// rather than reject as `UnsolicitedHeaders`. Incremented when the reactor
    /// covers this peer's outstanding range (a `DropExpectation` for a still-pending
    /// request): the peer's in-flight response for that range will still arrive
    /// late, and absorbing it mirrors the reactor's old `late_covered_responses`
    /// tolerance — a covered range is not the peer's fault.
    late_covered_tolerance: usize,
}

/// What the most recently decoded `Headers` response means for slot bookkeeping.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum HeadersOutcome {
    /// A non-empty response: the request is complete, free the slot.
    Filled,
    /// An empty response: re-fan the range after a short delay; the slot stays
    /// occupied until the empty-retry window elapses.
    Empty,
}

impl HsLocal {
    /// Build per-peer local state around this peer's stream-5 session.
    pub(crate) fn new(
        command_receivers: HeaderSyncCommandReceivers,
        anchor_height: block::Height,
        inbound_status_min_interval: Duration,
        new_block_min_interval: Duration,
    ) -> Self {
        let HeaderSyncCommandReceivers { status, commands } = command_receivers;
        Self {
            expected_headers: VecDeque::new(),
            commands,
            status,
            new_block_meter: RateMeter::new(new_block_min_interval),
            // A fresh session has advertised nothing; the peer's tip starts at the
            // trusted anchor and gates serving on a first valid status.
            advertised_tip: anchor_height,
            received_status: false,
            inbound_status_meter: RateMeter::new(inbound_status_min_interval),
            advertised_max_headers_per_response: None,
            in_flight: None,
            empty_retry_until: None,
            last_headers_outcome: None,
            late_covered_tolerance: 0,
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

    /// Consume one late-covered tolerance credit. `true` means a `Headers` frame
    /// with no expectation is a late response for a reactor-dropped range and
    /// should be absorbed silently rather than rejected as unsolicited.
    fn take_late_covered_tolerance(&mut self) -> bool {
        if self.late_covered_tolerance == 0 {
            return false;
        }
        self.late_covered_tolerance -= 1;
        true
    }

    /// Restore a solicited-response expectation that was popped for decode but
    /// whose decoded `Headers` event could not be handed to the reactor (the
    /// bounded `events` queue was full or closed). It goes back to the *front* so
    /// FIFO order is preserved and the reactor's still-outstanding range stays
    /// correlated, instead of leaving the expectation silently consumed.
    fn restore_expected_headers(&mut self, expected: ExpectedHeadersResponse) {
        self.expected_headers.push_front(expected);
    }

    /// Apply a reactor reset/timeout `DropExpectation` to this peer-local state: the
    /// reactor timed out or covered this session's outstanding request and returned
    /// the range to the shared queue, so the routine drops the matching front
    /// expectation and frees its outbound slot. It is idempotent — if the response
    /// already arrived and popped the expectation, the front no longer matches and
    /// the drop is a no-op, so a late drop cannot consume a fresh expectation. The
    /// generation guard is enforced by the routine before this is called (the
    /// command only reaches a session whose generation matches). Stream-writing
    /// commands (`SendHeaders`, `ForwardNewBlock`, `Wake`) are handled by the
    /// routine, which owns this peer's outbound session.
    fn apply_drop_expectation(&mut self, start_height: block::Height, count: u32) {
        let matches_front = self.expected_headers.front().is_some_and(|expected| {
            expected.start_height == start_height && expected.count == count
        });
        let matches_in_flight = self
            .in_flight
            .is_some_and(|range| range.start_height == start_height);
        if matches_front {
            self.expected_headers.pop_front();
            // The peer may still send a late response for this dropped expectation;
            // tolerate it instead of rejecting it as unsolicited (the drop was the
            // reactor's covered/timeout decision, not the peer's fault).
            self.late_covered_tolerance = self.late_covered_tolerance.saturating_add(1);
        }
        if matches_front || matches_in_flight {
            self.in_flight = None;
            self.empty_retry_until = None;
            metrics::counter!("sync.header.request.timeout_dropped").increment(1);
        }
    }

    /// Record an expected `Headers` response on the requester side.
    ///
    /// Production records this synchronously in the routine the moment it pulls
    /// work and sends the outbound `GetHeaders` (no command round-trip). The
    /// synthetic in-process cluster harness has no shared queue, so it records the
    /// expectation directly when it observes the outbound `GetHeaders`, keeping its
    /// per-peer ingest state in lockstep with the production correlation FIFO.
    #[cfg(test)]
    pub(crate) fn record_expected(&mut self, expected: ExpectedHeadersResponse) {
        self.expected_headers.push_back(expected);
    }

    /// Test-only: perform the routine's pull-work step against `env`'s shared range
    /// queue using this peer-local state's learned caps, recording the expected
    /// `Headers` and marking the slot in-flight on success. Returns the
    /// [`HeaderSyncEvent::PeerWorkAssigned`] the routine would emit so an in-process
    /// harness can both deliver the matching `GetHeaders` (reading `start_height`/
    /// `count` off the event) and forward the event to the reactor. Mirrors
    /// [`HeaderSyncPeerRoutine::try_pull`] for the synthetic cluster.
    #[cfg(test)]
    pub(crate) fn try_pull_work(
        &mut self,
        env: &HsEnv,
        peer_id: &ZakuraPeerId,
        generation: u64,
    ) -> Option<HeaderSyncEvent> {
        if self.in_flight.is_some() || !self.received_status {
            return None;
        }
        let max_headers_per_response = self.advertised_max_headers_per_response?;
        let caps = PeerPullCaps {
            advertised_tip: self.advertised_tip,
            max_headers_per_response,
        };
        let work = env.schedule().take_for_peer(
            peer_id,
            generation,
            caps,
            env.network(),
            env.max_frame_bytes(),
        )?;
        let expected = ExpectedHeadersResponse::new(work.range.start_height, work.count).ok()?;
        self.expected_headers.push_back(expected);
        self.in_flight = Some(work.range);
        Some(HeaderSyncEvent::PeerWorkAssigned {
            peer: peer_id.clone(),
            generation,
            start_height: work.range.start_height,
            count: work.count,
            anchor_hash: work.range.anchor_hash,
            finalized: work.range.finalized,
            forward: matches!(work.range.priority, RangePriority::Forward),
        })
    }

    /// Test-only: clear the in-flight slot when a harness observes the matching
    /// response, so the peer-local state can pull again. Mirrors the routine's
    /// `apply_headers_outcome` slot-free for the synthetic cluster.
    #[cfg(test)]
    pub(crate) fn clear_in_flight(&mut self) {
        self.in_flight = None;
        self.empty_retry_until = None;
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
    /// Active network, used by the inbound `GetHeaders` count limit and by the
    /// routine's count clamp when it pulls outbound work.
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

    /// The shared header-sync range queue the routine pulls outbound work from.
    pub(super) fn schedule(&self) -> &SharedHeaderRangeQueue {
        &self.handle.schedule
    }

    /// Forward a narrowed shared-effect event to the reactor (non-fatal on a
    /// closed/full queue).
    pub(super) fn handle(&self) -> &HeaderSyncHandle {
        &self.handle
    }

    /// Active network for the outbound count clamp.
    pub(super) fn network(&self) -> &Network {
        &self.network
    }

    /// Negotiated/local frame cap for the outbound count clamp.
    pub(super) fn max_frame_bytes(&self) -> u32 {
        self.max_frame_bytes
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

/// The production inbound-frame handler: decode one admitted stream-5 frame and
/// map a closed-queue local reject to a benign continue.
///
/// The guard already admitted this frame (oversize-only) before `run_inbound` is
/// reached, so this is the `Headers|Control → correlate → decode → emit` tail. It
/// delegates to [`decode_and_ingest`], which decodes the frame, correlates a
/// `Headers` response against the peer-owned expectation FIFO, runs the relocated
/// peer-local protocol validation, and forwards only the narrowed shared-effect
/// events the reactor consumes. The routine and the test/recorder `deliver_frame`
/// path share that one implementation, so they can never diverge on *what* they
/// decode, validate, or emit.
///
/// A closed reactor queue is the only `SinkReject::Local`: it is the peer's
/// non-fault, so `run_inbound` logs it and returns [`Ingest::Done`] (which
/// [`HeaderSyncPeerRoutine`] treats as "continue"). Protocol rejects pass straight
/// through and tear the peer down. When a *solicited* `Headers` response hits the
/// local-reject path, the expectation popped before decode is restored to
/// [`HsLocal`] so reactor queue saturation cannot silently consume it and strand
/// the still-outstanding range.
pub(super) fn run_inbound(
    local: &mut HsLocal,
    env: &HsEnv,
    peer_id: &ZakuraPeerId,
    frame: Frame,
) -> Ingest {
    match decode_and_ingest(local, env, peer_id.clone(), frame) {
        // A closed/full reactor queue is the peer's non-fault: the routine logs it
        // and continues (treated as `Ingest::Done`) rather than tearing the peer
        // down. The solicited-`Headers` expectation was already restored inside
        // `decode_and_ingest`, so the still-outstanding range stays correlated.
        Ingest::Reject(SinkReject::Local(error)) => {
            tracing::debug!(
                ?error,
                ?peer_id,
                "header-sync stream could not deliver frame locally"
            );
            Ingest::Done
        }
        other => other,
    }
}

/// Decode one admitted stream-5 frame, run the relocated peer-local validation,
/// and emit the narrowed shared-effect events to the reactor.
///
/// This is the single implementation reachable from both:
///
/// - the routine's [`run_inbound`] (with the peer's live [`HsLocal`] so a
///   `Headers` response correlates against the outstanding `GetHeaders` FIFO and
///   the per-peer rate meters/caps carry across frames), and
/// - [`HeaderSyncService::deliver_frame`](super::service::HeaderSyncService) (the
///   test/recorder path, which uses an ephemeral [`HsLocal`] with no recorded
///   expectation, so a `Headers` response is `UnsolicitedHeaders`).
///
/// Effects map onto [`Ingest`]:
///
/// - emitting all effects successfully ⇒ [`Ingest::Continue`] (or [`Ingest::Done`]
///   when nothing was emitted, e.g. a dropped-before-decode `NewBlock` flood);
/// - a routine-classified protocol violation ⇒ the misbehavior event plus
///   [`Ingest::Reject`] with a `Protocol` reason (fatal — disconnect the peer);
/// - a full/closed reactor queue ⇒ [`Ingest::Reject`] with a `Local` reason. Each
///   caller maps `Local` to its old behavior: `run_inbound` logs and continues,
///   while `deliver_frame` returns it to the registry as `Err(SinkReject::Local)`.
pub(crate) fn decode_and_ingest(
    local: &mut HsLocal,
    env: &HsEnv,
    peer_id: ZakuraPeerId,
    frame: Frame,
) -> Ingest {
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
        return Ingest::Done;
    }

    // Correlate a `Headers` response against the peer-owned expectation FIFO before
    // decode, so an over-long or otherwise malformed response is bounded by the
    // matching `GetHeaders` count. A `Headers` frame with no expectation is either a
    // late response for a range the reactor already covered/timed-out (absorbed
    // silently if this peer has late-covered tolerance) or genuinely unsolicited
    // (classified and rejected).
    let expected = if is_headers {
        let Some(expected) = local.pop_expected_headers_response() else {
            if local.take_late_covered_tolerance() {
                metrics::counter!("sync.header.response.late_covered_dropped").increment(1);
                return Ingest::Done;
            }
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
        reject @ Ingest::Reject(SinkReject::Local(_)) => {
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
    HsLocal::new(
        HeaderSyncCommandReceivers::detached(),
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
) -> Ingest {
    match msg {
        HeaderSyncMessage::Status(status) => ingest_status(local, env, peer_id, status),
        HeaderSyncMessage::Headers {
            headers,
            body_sizes,
        } => ingest_headers(local, env, peer_id, headers, body_sizes),
        HeaderSyncMessage::GetHeaders {
            start_height,
            count,
        } => ingest_get_headers(local, env, peer_id, start_height, count),
        HeaderSyncMessage::NewBlock(block) => forward(
            env.handle(),
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
) -> Ingest {
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
    let clamped_max_headers = clamp_advertised_range(status.max_headers_per_response);
    // Retain the clamped per-response cap so the routine can clamp its own
    // outbound `GetHeaders` count when it pulls work from the shared queue.
    local.advertised_max_headers_per_response = Some(clamped_max_headers);
    let clamped = HeaderSyncStatus {
        max_headers_per_response: clamped_max_headers,
        max_inflight_requests: status
            .max_inflight_requests
            .clamp(1, LOCAL_MAX_HS_INFLIGHT_PER_PEER),
        ..status
    };
    forward(
        env.handle(),
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
    local: &mut HsLocal,
    env: &HsEnv,
    peer_id: &ZakuraPeerId,
    headers: Vec<Arc<block::Header>>,
    body_sizes: Vec<u32>,
) -> Ingest {
    metrics::counter!("sync.header.response.received").increment(1);
    if validate_body_sizes_len(headers.len(), body_sizes.len()).is_err() {
        // A body-size/header-count parity check on an ALREADY-DECODED `Headers` is a
        // semantic shape violation, not a decode failure. At the baseline this was
        // the reactor's record-only `report_misbehavior(MalformedMessage)`, so report
        // it and keep the connection (the correlated-`Headers` *decode* failure, which
        // does disconnect, is handled in `decode_and_ingest`).
        //
        // Treat this like a filled (terminal) response for slot purposes: the
        // expectation was already popped during correlation, so free the slot for a
        // fresh pull rather than stranding it.
        local.last_headers_outcome = Some(HeadersOutcome::Filled);
        return record_misbehavior(env, peer_id, HeaderSyncMisbehavior::MalformedMessage);
    }
    // Record the slot outcome for the routine to act on after the frame is
    // processed: a non-empty response completes the request (free the slot); an
    // empty one re-fans the range after a short delay (slot stays occupied).
    local.last_headers_outcome = Some(if headers.is_empty() {
        HeadersOutcome::Empty
    } else {
        HeadersOutcome::Filled
    });
    forward(
        env.handle(),
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
) -> Ingest {
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
        env.handle(),
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
) -> Ingest {
    tracing::debug!(
        ?peer_id,
        ?reason,
        ?error,
        "invalid Zakura header-sync message"
    );
    let _ = env.handle().try_send(HeaderSyncEvent::PeerMisbehavior {
        peer: peer_id.clone(),
        reason,
    });
    let protocol_error = std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string());
    Ingest::Reject(SinkReject::protocol(protocol_error))
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
) -> Ingest {
    let _ = env.handle().try_send(HeaderSyncEvent::PeerMisbehavior {
        peer: peer_id.clone(),
        reason,
    });
    Ingest::Continue
}

/// One inbound input the routine's recv loop selects over.
enum HsRoutineInput {
    /// An inbound stream-5 frame to admit, decode, correlate, and forward.
    Frame(Frame),
    /// A reactor command (a `Headers` response to write, a `NewBlock` to forward, a
    /// wake, or the timeout/covered `DropExpectation` reset) into this routine.
    Command(HeaderSyncPeerCommand),
    /// The reactor coalesced a new `Status` for this peer; write it on this stream.
    SendStatus(HeaderSyncStatus),
    /// The shared range queue signalled that work may be available; try to pull.
    TryPull,
    /// Cancellation or stream close — the routine exits cleanly.
    Done,
}

/// The concrete production owner of one admitted header-sync peer's stream-5 recv
/// loop.
///
/// The routine owns the local decode/correlation state ([`HsLocal`] — the
/// expected-`Headers` FIFO, the reactor command receiver, the pre-decode `NewBlock`
/// rate gate, the peer-local advertised caps + status-spam meter, and the
/// outbound-slot/empty-retry state), the inbound stream-guard admission, the work
/// pull, the frame decode, and the relocated peer-local protocol validation. It
/// decodes each inbound frame inline AND pulls its own outbound `GetHeaders` work
/// from the shared range queue, forwarding only narrowed shared-effect events to
/// [`HeaderSyncReactor`](super::reactor). Range production, commit ordering,
/// timeouts, and direct status/`NewBlock`/`Headers`-response sends stay
/// reactor-side (the response sends are chunk 05).
///
/// Work-pull invariant: the routine records its expected `Headers` response
/// synchronously the moment it takes work from the shared queue and sends the
/// outbound `GetHeaders`, all in one non-`await` step before yielding back to the
/// `select!`. A network response cannot beat that local record (a round trip is
/// orders of magnitude slower than a local push), so the expectation is always in
/// `HsLocal.expected_headers` before the matching `Headers` frame is decoded —
/// never the reverse, which would reject a solicited response as
/// `UnsolicitedHeaders`.
pub(super) struct HeaderSyncPeerRoutine {
    /// Authenticated identity of the peer this routine drives.
    peer_id: ZakuraPeerId,
    /// This peer's decode/correlation state: the expected-`Headers` FIFO, the
    /// reactor command receiver, the pre-decode `NewBlock` rate gate, and the
    /// peer-local advertised caps + slot/empty-retry state. No locking.
    local: HsLocal,
    /// Arc-cloneable shared environment: the reactor handle the routine forwards
    /// narrowed events over and the shared range queue it pulls work from.
    env: HsEnv,
    /// The per-peer stream-guard (oversize-only admission) applied to every
    /// inbound frame before decode.
    guard: SessionGuard,
    /// This peer's ordered stream-5 frame reader, owned and drained by the routine.
    recv: FramedRecv,
    /// The peer's service-session cancellation token. Fires on disconnect, park,
    /// or local shutdown; the routine then exits cleanly.
    cancel: CancellationToken,
    /// The typed session this routine sends its own outbound `GetHeaders` over,
    /// inverting the old reactor push. `None` only in unit tests that exercise the
    /// recv loop without an outbound stream.
    session: Option<HeaderSyncPeerSession>,
    /// Session generation, minted once at admission. Tags every assignment and
    /// `PeerWorkAssigned` event so a stale timeout cannot disturb a reconnected
    /// session.
    generation: u64,
}

impl HeaderSyncPeerRoutine {
    /// Build the routine around a peer's decode/correlation state, shared
    /// environment, stream guard, inbound reader, service-session cancellation
    /// token, the typed session it sends outbound `GetHeaders` over, and the
    /// session generation.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        peer_id: ZakuraPeerId,
        local: HsLocal,
        env: HsEnv,
        guard: SessionGuard,
        recv: FramedRecv,
        cancel: CancellationToken,
        session: Option<HeaderSyncPeerSession>,
        generation: u64,
    ) -> Self {
        Self {
            peer_id,
            local,
            env,
            guard,
            recv,
            cancel,
            session,
            generation,
        }
    }

    /// Test-only: build a routine with the production guard around a fresh
    /// per-peer [`HsLocal`] (default inbound intervals) and shared environment.
    /// Used by the reactor test fixture and the recv-loop tests to spawn real
    /// routines that share the reactor's range queue, replacing the old
    /// `test_pipe` constructor.
    #[cfg(test)]
    pub(crate) fn for_test(
        peer_id: ZakuraPeerId,
        command_receivers: HeaderSyncCommandReceivers,
        env: HsEnv,
        recv: FramedRecv,
        cancel: CancellationToken,
        session: Option<HeaderSyncPeerSession>,
        generation: u64,
    ) -> Self {
        let anchor_height = env.anchor_height();
        let local = HsLocal::new(
            command_receivers,
            anchor_height,
            DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
        );
        // MAX_HS_MESSAGE_BYTES is a small compile-time const that fits in u32.
        let guard = SessionGuard::oversize_only(MAX_HS_MESSAGE_BYTES as u32);
        Self::new(
            peer_id, local, env, guard, recv, cancel, session, generation,
        )
    }

    /// Test-only: borrow this routine's peer-local decode/correlation state to set
    /// up or assert on its FIFO/slot bookkeeping in recv-loop tests.
    #[cfg(test)]
    pub(crate) fn local_mut(&mut self) -> &mut HsLocal {
        &mut self.local
    }

    /// Test-only: drive one inbound frame through the routine's guard + decode tail
    /// directly, mirroring what the recv loop does for a `Frame` input.
    #[cfg(test)]
    pub(crate) fn run_one_for_test(&mut self, frame: Frame) -> Ingest {
        self.run_one(frame)
    }

    /// Run one admitted inbound frame: apply the stream guard, then decode and
    /// ingest through [`run_inbound`]. This inlines what the generic pipe runner
    /// used to do — `recv → guard.admit → entry → map` — so the routine owns its
    /// own inbound control flow with no fn-ptr indirection.
    ///
    /// A guard throttle is a clean drop ([`Ingest::Done`]); a guard reject is a
    /// protocol-fatal [`Ingest::Reject`]; an admitted frame runs the decode/ingest
    /// tail.
    fn run_one(&mut self, frame: Frame) -> Ingest {
        match self.guard.admit(&frame) {
            crate::zakura::Admit::Throttle => Ingest::Done,
            crate::zakura::Admit::Reject(reason) => Ingest::Reject(SinkReject::protocol(reason)),
            crate::zakura::Admit::Pass => {
                run_inbound(&mut self.local, &self.env, &self.peer_id, frame)
            }
        }
    }

    /// Run the routine until stream close, cancellation, or a reject.
    ///
    /// A protocol reject is the only `Err` returned (a closed-queue `Local` is
    /// mapped to a benign continue inside [`run_inbound`]); the caller composes it
    /// with [`handle_routine_exit`](crate::zakura::handle_routine_exit) so a
    /// protocol reject cancels the whole connection while a clean stream-end/cancel
    /// leaves it alone. On every exit path (clean or reject) the routine returns any
    /// in-flight range to the shared queue via [`Drop`], so work is never stranded.
    pub(super) async fn run(mut self) -> Result<(), SinkReject> {
        // Pull any work that is already eligible before parking on the first wake.
        self.try_pull();
        loop {
            let schedule = self.env.schedule().clone();
            // When an empty-retry window is armed, schedule a wake at its expiry so
            // the routine re-pulls the re-fanned range without waiting for an
            // unrelated reactor wake. Otherwise this arm is inert.
            let empty_retry_until = self.local.empty_retry_until;
            let input = {
                let local = &mut self.local;
                tokio::select! {
                    biased;
                    () = self.cancel.cancelled() => HsRoutineInput::Done,
                    command = local.commands.recv() => match command {
                        Some(command) => HsRoutineInput::Command(command),
                        None => HsRoutineInput::Done,
                    },
                    // A coalesced `Status` update from the reactor. The watch holds
                    // only the latest value, so a burst is collapsed to one send.
                    changed = local.status.changed() => match changed {
                        Ok(()) => match *local.status.borrow_and_update() {
                            Some(status) => HsRoutineInput::SendStatus(status),
                            // The initial `None` cannot fire `changed`; treat any
                            // unexpected clear as a benign no-op pull attempt.
                            None => HsRoutineInput::TryPull,
                        },
                        // The reactor dropped the sink (peer removed); exit cleanly.
                        Err(_) => HsRoutineInput::Done,
                    },
                    frame = self.recv.recv() => match frame {
                        Some(frame) => HsRoutineInput::Frame(frame),
                        None => HsRoutineInput::Done,
                    },
                    () = async {
                        match empty_retry_until {
                            Some(until) => tokio::time::sleep_until(until).await,
                            None => std::future::pending().await,
                        }
                    } => HsRoutineInput::TryPull,
                    () = schedule.waked() => HsRoutineInput::TryPull,
                }
            };

            match input {
                HsRoutineInput::Done => return Ok(()),
                HsRoutineInput::Frame(frame) => {
                    match self.run_one(frame) {
                        Ingest::Continue | Ingest::Done => {}
                        Ingest::Reject(reject) => return Err(reject),
                    }
                    // A processed `Headers` response may free or re-arm the slot;
                    // act on the recorded outcome. Re-attempt a pull only when a
                    // response actually completed the slot — a `Status` frame must
                    // NOT trigger an immediate pull, because the forward range it
                    // enables is produced by the reactor only after it processes the
                    // `PeerStatusUpdated` this frame forwards; pulling now could take
                    // a lower-priority backward range before the forward range
                    // exists. The reactor's post-status `schedule()` wake drives the
                    // forward pull instead.
                    if self.apply_headers_outcome() {
                        self.try_pull();
                    }
                }
                HsRoutineInput::Command(command) => self.handle_command(command),
                HsRoutineInput::SendStatus(status) => self.send_status(status),
                HsRoutineInput::TryPull => self.try_pull(),
            }
        }
    }

    /// Handle one dequeued reactor command. Stream-writing commands (`SendHeaders`,
    /// `ForwardNewBlock`) are written on this peer's own stream — the routine, not
    /// the reactor, owns every outbound write. `Wake` re-attempts an outbound pull.
    /// `DropExpectation` is the reset/timeout that frees the outbound slot.
    fn handle_command(&mut self, command: HeaderSyncPeerCommand) {
        match command {
            HeaderSyncPeerCommand::SendHeaders {
                headers,
                body_sizes,
            } => self.send_headers(headers, body_sizes),
            HeaderSyncPeerCommand::ForwardNewBlock(block) => self.forward_new_block(block),
            HeaderSyncPeerCommand::Wake => self.try_pull(),
            HeaderSyncPeerCommand::DropExpectation {
                generation,
                start_height,
                count,
            } => {
                // Defense in depth: only apply a reset addressed to this session's
                // generation. The reactor already scopes its send to a matching
                // generation, so a mismatch should never reach here, but ignoring it
                // keeps a misrouted reset from disturbing live work.
                if generation == self.generation {
                    self.local.apply_drop_expectation(start_height, count);
                    // The reset freed the outbound slot; try fresh work.
                    self.try_pull();
                }
            }
        }
    }

    /// Write a reactor-commanded `Status` on this peer's own stream. The reactor
    /// chose the status and the destination; the routine only writes the frame. A
    /// send failure is benign — the peer either reconnects (re-status on the new
    /// session) or the next refresh re-sends; it never strands work.
    fn send_status(&self, status: HeaderSyncStatus) {
        let Some(session) = &self.session else {
            return;
        };
        if let Err(error) = session.try_send_status(status) {
            tracing::debug!(
                peer_id = ?self.peer_id,
                ?error,
                "Zakura header-sync routine could not write commanded Status"
            );
        }
    }

    /// Write a reactor-served `Headers` response on this peer's own stream. This is
    /// the correlated reply to an inbound `GetHeaders` the reactor served from
    /// state; the reactor chose the destination, the routine writes the frame.
    fn send_headers(&self, headers: Vec<Arc<block::Header>>, body_sizes: Vec<u32>) {
        let Some(session) = &self.session else {
            return;
        };
        if let Err(error) = session.try_send_headers_with_sizes(headers, body_sizes) {
            tracing::debug!(
                peer_id = ?self.peer_id,
                ?error,
                "Zakura header-sync routine could not write commanded Headers response"
            );
        }
    }

    /// Forward a reactor-accepted tip `NewBlock` on this peer's own stream. The
    /// reactor chose the forwarding destinations; the routine writes the frame.
    fn forward_new_block(&self, block: Arc<block::Block>) {
        let Some(session) = &self.session else {
            return;
        };
        if let Err(error) = session.try_send_new_block(block) {
            tracing::debug!(
                peer_id = ?self.peer_id,
                ?error,
                "Zakura header-sync routine could not write commanded NewBlock forward"
            );
        }
    }

    /// Act on the outcome of the most recently decoded `Headers` response: a
    /// filled response completes the request (free the slot); an empty one returns
    /// the range to the shared queue and arms the empty-retry window so the slot
    /// stays occupied for a short re-fan delay (mirroring the reactor's old
    /// empty-headers re-arm). The expectation was already popped during
    /// correlation, so only the slot/return bookkeeping is done here.
    ///
    /// Returns `true` only when a filled response freed the slot, signalling the
    /// caller that an immediate re-pull is warranted; an empty response keeps the
    /// slot occupied (the empty-retry window), so it returns `false`.
    fn apply_headers_outcome(&mut self) -> bool {
        let outcome = self.local.last_headers_outcome.take();
        let Some(outcome) = outcome else {
            return false;
        };
        let Some(range) = self.local.in_flight.take() else {
            return false;
        };
        match outcome {
            HeadersOutcome::Filled => {
                // Request complete; the slot is free for the next pull.
                self.local.empty_retry_until = None;
                true
            }
            HeadersOutcome::Empty => {
                metrics::counter!("sync.header.response.empty_returned").increment(1);
                self.env.schedule().return_work(
                    &self.peer_id,
                    self.generation,
                    range,
                    ReturnReason::EmptyResponse,
                );
                self.local.empty_retry_until = Some(Instant::now() + EMPTY_HEADERS_RETRY_DELAY);
                false
            }
        }
    }

    /// Try to pull one eligible range from the shared queue and request it.
    ///
    /// Gated by the peer-local slot/window/deadline state owned by the routine:
    /// the peer must have sent a valid `Status` (so its advertised tip and cap are
    /// known), the single outbound slot must be free, and any empty-retry window
    /// must have elapsed. On a successful pull the routine records the expected
    /// `Headers` locally, sends `GetHeaders` on its own stream, and emits
    /// `PeerWorkAssigned` so the reactor records the matching outstanding range —
    /// all synchronously, with no `.await`, so a response cannot beat the local
    /// expectation.
    fn try_pull(&mut self) {
        // Re-arm a pull as soon as the empty-retry window elapses even with no new
        // wake, by clearing an expired window here.
        {
            let local = &mut self.local;
            if local
                .empty_retry_until
                .is_some_and(|until| Instant::now() >= until)
            {
                local.empty_retry_until = None;
            }
        }

        let caps = {
            let local = &mut self.local;
            // Slot gate: one outbound request per peer; no pull while a request is
            // in flight, before the first status, or during an empty-retry window.
            if local.in_flight.is_some()
                || !local.received_status
                || local.empty_retry_until.is_some()
            {
                return;
            }
            let Some(max_headers_per_response) = local.advertised_max_headers_per_response else {
                return;
            };
            PeerPullCaps {
                advertised_tip: local.advertised_tip,
                max_headers_per_response,
            }
        };

        let peer_id = self.peer_id.clone();
        let env = self.env.clone();
        let Some(work) = env.schedule().take_for_peer(
            &peer_id,
            self.generation,
            caps,
            env.network(),
            env.max_frame_bytes(),
        ) else {
            return;
        };

        // Record the expectation BEFORE sending so a response can never beat it.
        let expected = match ExpectedHeadersResponse::new(work.range.start_height, work.count) {
            Ok(expected) => expected,
            Err(error) => {
                // The clamp guarantees a bounded count, so this is unreachable; if
                // it ever fires, return the range rather than strand it.
                tracing::debug!(?peer_id, ?error, "invalid pulled header work; returning");
                env.schedule().return_work(
                    &peer_id,
                    self.generation,
                    work.range,
                    ReturnReason::SendFailed,
                );
                return;
            }
        };

        // Send `GetHeaders` on this peer's own stream. A send failure returns the
        // range to the queue (it was never requested) and frees the slot.
        if let Some(session) = &self.session {
            if let Err(error) = session.try_send_get_headers(work.range.start_height, work.count) {
                tracing::debug!(
                    ?peer_id,
                    start_height = ?work.range.start_height,
                    count = work.count,
                    ?error,
                    "failed to send Zakura header-sync GetHeaders; returning work"
                );
                env.schedule().return_work(
                    &peer_id,
                    self.generation,
                    work.range,
                    ReturnReason::SendFailed,
                );
                return;
            }
        }

        {
            let local = &mut self.local;
            local.expected_headers.push_back(expected);
            local.in_flight = Some(work.range);
        }
        metrics::counter!("sync.header.request.sent").increment(1);
        tracing::trace!(
            ?peer_id,
            start_height = ?work.range.start_height,
            count = work.count,
            generation = self.generation,
            "Zakura header-sync routine sent GetHeaders and recorded expectation"
        );

        // Tell the reactor to record the matching outstanding range so its timeout,
        // covered-range, and commit machinery stay reactor-owned. The deadline uses
        // the per-request timeout exactly as the old reactor push did.
        let _ = env.handle().try_send(HeaderSyncEvent::PeerWorkAssigned {
            peer: peer_id,
            generation: self.generation,
            start_height: work.range.start_height,
            count: work.count,
            anchor_hash: work.range.anchor_hash,
            finalized: work.range.finalized,
            forward: matches!(work.range.priority, RangePriority::Forward),
        });
    }

    /// Test-only: pre-record an expected `Headers` response and mark a matching
    /// in-flight range, simulating a request the routine already sent. Lets the
    /// recv-loop correlation tests set up a correlated response without driving a
    /// full pull.
    #[cfg(test)]
    fn seed_expected(&mut self, start_height: block::Height, count: u32) {
        let expected =
            ExpectedHeadersResponse::new(start_height, count).expect("test count is valid");
        let local = &mut self.local;
        local.record_expected(expected);
        local.in_flight = Some(RangeRequest {
            start_height,
            count,
            anchor_hash: block::Hash([0; 32]),
            finalized: false,
            priority: RangePriority::Forward,
        });
    }
}

impl Drop for HeaderSyncPeerRoutine {
    /// Return any in-flight range to the shared queue on every exit path (clean
    /// stream close, service cancellation, protocol reject, or routine panic), so
    /// an assigned range is never stranded. The return is generation-scoped, so a
    /// late teardown of an OLD session cannot clear a reconnected session's work.
    fn drop(&mut self) {
        let Some(range) = self.local.in_flight.take() else {
            return;
        };
        let peer_id = self.peer_id.clone();
        self.env
            .schedule()
            .return_work(&peer_id, self.generation, range, ReturnReason::Teardown);
    }
}

/// Forward a narrowed shared-effect event to the reactor.
///
/// A closed/full reactor queue is a local, non-fatal condition for the peer, so
/// this returns [`Ingest::Reject`] with a `Local` reason. Callers decide whether to
/// continue or surface it (see [`decode_and_ingest`]).
fn forward(handle: &HeaderSyncHandle, event: HeaderSyncEvent) -> Ingest {
    match handle.try_send(event) {
        Ok(()) => Ingest::Continue,
        Err(error) => Ingest::Reject(SinkReject::local(format!(
            "header-sync queue closed: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::watch;

    use super::{
        super::service::{HeaderSyncCommandOutcome, HeaderSyncCommandSink},
        *,
    };
    use crate::zakura::{ServicePeerSnapshot, ZakuraHeaderSyncCandidateState};

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
                schedule: SharedHeaderRangeQueue::new(),
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
                schedule: SharedHeaderRangeQueue::new(),
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
        HsLocal::new(
            HeaderSyncCommandReceivers::detached(),
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

        assert!(matches!(flow, Ingest::Reject(SinkReject::Protocol(_))));
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

        assert!(matches!(flow, Ingest::Reject(SinkReject::Protocol(_))));
        match events.try_recv() {
            Ok(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
                assert!(matches!(reason, HeaderSyncMisbehavior::MalformedMessage));
            }
            other => panic!("expected PeerMisbehavior(MalformedMessage), got {other:?}"),
        }
    }

    /// The peer-local correlation queue is FIFO and is filled by draining ready
    /// expectations. This is the invariant [`HeaderSyncPeerRoutine`] relies on: an
    /// expectation the routine records when it pulls work is available to pop in
    /// FIFO order before the matching `Headers` response is processed.
    #[test]
    fn local_correlation_queue_records_in_fifo_order() {
        let mut local = HsLocal::new(
            HeaderSyncCommandReceivers::detached(),
            block::Height(0),
            DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
        );

        let first = ExpectedHeadersResponse::new(block::Height(1), 1).expect("count is valid");
        let second = ExpectedHeadersResponse::new(block::Height(2), 2).expect("count is valid");
        // The routine records each expectation synchronously the moment it pulls
        // and sends the matching `GetHeaders`.
        local.record_expected(first);
        local.record_expected(second);

        assert_eq!(local.pop_expected_headers_response(), Some(first));
        assert_eq!(local.pop_expected_headers_response(), Some(second));
        assert_eq!(local.pop_expected_headers_response(), None);
    }

    /// A `NewBlock` flood is throttled *before* full-block decode: the first
    /// frame in a window is decoded and forwarded to the reactor, but a second
    /// distinct frame inside the per-peer minimum interval is dropped before
    /// `Block::zcash_deserialize` runs, so nothing reaches the reactor and the
    /// peer is kept (`Ingest::Done`). This proves the amplification gap is closed —
    /// without the pre-decode gate the second full block is deserialized and
    /// forwarded too.
    #[test]
    fn new_block_flood_is_throttled_before_decode() {
        use zebra_chain::serialization::ZcashDeserializeInto;
        use zebra_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES};

        let (handle, mut events) = test_handle();

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

        let (_peer_send, service_recv) = crate::zakura::framed_channel(4);
        let mut routine = HeaderSyncPeerRoutine::for_test(
            peer(),
            HeaderSyncCommandReceivers::detached(),
            test_env(handle),
            service_recv,
            CancellationToken::new(),
            None,
            0,
        );

        // First flood frame: admitted, decoded, and forwarded to the reactor.
        assert!(matches!(
            routine.run_one_for_test(frame_one),
            Ingest::Continue
        ));
        match events.try_recv() {
            Ok(HeaderSyncEvent::NewBlockCandidate { block, .. }) => {
                assert_eq!(block.hash(), block_one.hash())
            }
            other => panic!("expected first NewBlock to be forwarded, got {other:?}"),
        }

        // Second distinct flood frame inside the interval is dropped before
        // decode: the peer is kept and nothing reaches the reactor.
        assert!(matches!(routine.run_one_for_test(frame_two), Ingest::Done));
        assert!(
            matches!(events.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "second NewBlock must be throttled before decode, not forwarded"
        );
    }

    /// Under reactor `events`-queue saturation, a valid *solicited* `Headers`
    /// response must not silently consume its peer-local expectation. The pipe
    /// pops the expectation before decode; when the decoded response cannot be
    /// delivered to the full reactor queue, the pipe logs and continues
    /// (`Ingest::Done`) — but the popped expectation is restored to the FIFO so the
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

        let expected = ExpectedHeadersResponse::new(block::Height(1), 1).expect("count is valid");

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

        let (_peer_send, service_recv) = crate::zakura::framed_channel(4);
        let mut routine = HeaderSyncPeerRoutine::for_test(
            peer(),
            HeaderSyncCommandReceivers::detached(),
            test_env(handle),
            service_recv,
            CancellationToken::new(),
            None,
            0,
        );
        // Record the expectation directly, as the routine does the moment it pulls
        // work and sends the matching `GetHeaders`, so the `Headers` frame is
        // correlated.
        routine.local_mut().record_expected(expected);

        // The decoded response cannot be delivered (events queue is full); the
        // routine logs and continues, exactly as production does.
        assert!(matches!(
            routine.run_one_for_test(solicited_headers),
            Ingest::Done
        ));

        // The popped expectation must be restored so the still-outstanding range
        // stays correlated. Without the fix the expectation is gone (returns None).
        assert_eq!(
            routine.local_mut().pop_expected_headers_response(),
            Some(expected),
            "a solicited Headers response dropped on reactor queue saturation must restore its expectation"
        );
    }

    #[test]
    fn correlates_headers_before_control() {
        // The routine's inbound fork is frame-shape based: a `Headers` response
        // needs peer-local request correlation (the expectation FIFO), while all
        // other stream-5 messages decode as `Control` and are validated/forwarded
        // without correlation. This guards that fork directly against the routine
        // rather than against a declared DAG shape: a `Headers` frame with no
        // recorded expectation rejects as `UnsolicitedHeaders` (correlation ran),
        // while a `Status` (Control) frame is accepted without any expectation.
        let (handle, mut events) = test_handle();
        let env = test_env(handle);

        // (a) `Headers` correlates before decode: with no expectation it is
        // unsolicited and protocol-rejected.
        let mut headers_local = test_local();
        assert!(matches!(
            decode_and_ingest(&mut headers_local, &env, peer(), headers_frame(Vec::new())),
            Ingest::Reject(SinkReject::Protocol(_))
        ));
        assert!(matches!(
            events.try_recv(),
            Ok(HeaderSyncEvent::PeerMisbehavior {
                reason: HeaderSyncMisbehavior::UnsolicitedHeaders,
                ..
            })
        ));

        // (b) A Control message (`Status`) needs no correlation and is forwarded.
        let mut control_local = test_local();
        let status_frame = HeaderSyncMessage::Status(HeaderSyncStatus {
            tip_height: block::Height(0),
            tip_hash: block::Hash([0; 32]),
            anchor_height: block::Height(0),
            max_headers_per_response: 100,
            max_inflight_requests: 1,
        })
        .encode_frame()
        .expect("status frame encodes");
        assert!(matches!(
            decode_and_ingest(&mut control_local, &env, peer(), status_frame),
            Ingest::Continue
        ));
        assert!(matches!(
            events.try_recv(),
            Ok(HeaderSyncEvent::PeerStatusUpdated { .. })
        ));
    }

    // ===================== HeaderSyncPeerRoutine recv-loop tests =============

    use crate::zakura::{framed_channel, spawn_supervised_routine, FramedSend};

    /// Build a routine around a fresh handle/commands/stream so a test can drive
    /// its recv loop. Returns the routine, the peer-side stream sender (closing it
    /// ends the stream), and the reactor command sink (used to deliver reset/headers
    /// commands). The routine has no outbound session, so it does not pull;
    /// recv-loop correlation tests seed expectations via
    /// [`HeaderSyncPeerRoutine::seed_expected`].
    #[allow(clippy::type_complexity)]
    fn routine_with(
        handle: HeaderSyncHandle,
        cancel: CancellationToken,
    ) -> (HeaderSyncPeerRoutine, FramedSend, HeaderSyncCommandSink) {
        let (command_sink, command_receivers) = HeaderSyncCommandSink::channel();
        let (peer_send, service_recv) = framed_channel(16);
        let routine = HeaderSyncPeerRoutine::for_test(
            peer(),
            command_receivers,
            test_env(handle),
            service_recv,
            cancel,
            None,
            0,
        );
        (routine, peer_send, command_sink)
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
        let (mut routine, peer_send, _commands_tx) = routine_with(handle, cancel.clone());
        // Seed the expectation the routine would have recorded when it pulled and
        // sent the matching `GetHeaders`, then run it.
        routine.seed_expected(block::Height(1), 1);
        let run = tokio::spawn(routine.run());

        // Send the matching solicited response. The routine correlates, decodes, and
        // tries to forward — the full queue turns the forward into a `Local` reject,
        // which the routine logs and continues past (it does not reject the peer).
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
            schedule: SharedHeaderRangeQueue::new(),
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

        let handle_task = spawn_supervised_routine(
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

    /// End-to-end through the routine recv loop: an expectation the routine recorded
    /// when it pulled work (seeded here) is popped before the matching `Headers` frame
    /// is decoded, so the solicited response is correlated (it reports
    /// `MalformedMessage` against the expectation, never the pre-correlation
    /// `UnsolicitedHeaders`). This proves the routine's ordering: the expectation is
    /// recorded before any matching frame can be decoded.
    #[tokio::test]
    async fn routine_records_expectation_before_decoding_matching_headers() {
        let (handle, mut events) = test_handle();
        let cancel = CancellationToken::new();
        let (mut routine, peer_send, _commands_tx) = routine_with(handle, cancel.clone());
        // Seed the expectation the routine records the moment it pulls work and
        // sends the matching `GetHeaders`, then run it and send an empty (malformed)
        // solicited Headers frame. The routine must correlate it against the
        // expectation: a correlated-but-malformed response reports
        // `MalformedMessage` and rejects; an UNcorrelated one would report
        // `UnsolicitedHeaders`.
        routine.seed_expected(block::Height(1), 1);
        let run = tokio::spawn(routine.run());

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

    /// Multiple expectations the routine records preserve FIFO order: the
    /// first-recorded expectation is the first popped against the first matching
    /// `Headers` frame.
    #[test]
    fn routine_preserves_fifo_for_multiple_expectations() {
        let mut local = HsLocal::new(
            HeaderSyncCommandReceivers::detached(),
            block::Height(0),
            DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
        );

        let first = ExpectedHeadersResponse::new(block::Height(10), 1).expect("count is valid");
        let second = ExpectedHeadersResponse::new(block::Height(20), 2).expect("count is valid");
        let third = ExpectedHeadersResponse::new(block::Height(30), 3).expect("count is valid");
        for expected in [first, second, third] {
            local.record_expected(expected);
        }

        assert_eq!(local.pop_expected_headers_response(), Some(first));
        assert_eq!(local.pop_expected_headers_response(), Some(second));
        assert_eq!(local.pop_expected_headers_response(), Some(third));
        assert_eq!(local.pop_expected_headers_response(), None);
    }

    // ===================== routine-pulled work + strand prevention ==========

    /// Build a `HeaderSyncHandle` around a caller-provided shared range queue so a
    /// test can seed work and observe assignments/returns directly.
    fn handle_with_queue(
        schedule: SharedHeaderRangeQueue,
    ) -> (HeaderSyncHandle, mpsc::Receiver<HeaderSyncEvent>) {
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
                schedule,
            },
            events_rx,
        )
    }

    fn forward_range(start: u32, count: u32) -> RangeRequest {
        RangeRequest {
            start_height: block::Height(start),
            count,
            anchor_hash: block::Hash([7; 32]),
            finalized: false,
            priority: RangePriority::Forward,
        }
    }

    /// Build a routine with a real outbound session sharing `schedule`, seeded with
    /// a status so its caps are known. Returns the routine, the queue, the events
    /// receiver, the outbound `GetHeaders` reader, and the cancel token.
    #[allow(clippy::type_complexity)]
    fn pulling_routine(
        schedule: SharedHeaderRangeQueue,
        generation: u64,
    ) -> (
        HeaderSyncPeerRoutine,
        mpsc::Receiver<HeaderSyncEvent>,
        crate::zakura::FramedRecv,
        CancellationToken,
    ) {
        let (handle, events) = handle_with_queue(schedule);
        let (_peer_send, service_recv) = crate::zakura::framed_channel(16);
        let (outbound_send, outbound_recv) = crate::zakura::framed_channel(16);
        let cancel = CancellationToken::new();
        let env = HsEnv::new(
            handle,
            Network::Mainnet,
            ZakuraHeaderSyncConfig::default(),
            MAX_HS_MESSAGE_BYTES as u32,
        );
        let session = crate::zakura::HeaderSyncPeerSession::from_parts_with_direction(
            peer(),
            crate::zakura::ServicePeerDirection::Inbound,
            outbound_send,
            cancel.clone(),
        );
        let mut routine = HeaderSyncPeerRoutine::for_test(
            peer(),
            HeaderSyncCommandReceivers::detached(),
            env,
            service_recv,
            cancel.clone(),
            Some(session),
            generation,
        );
        // Seed the peer-local caps as a real `Status` would, so `try_pull` is gated
        // open. Drop the forwarded status event from the events queue afterward.
        routine.local_mut().received_status = true;
        routine.local_mut().advertised_tip = block::Height(100);
        routine.local_mut().advertised_max_headers_per_response = Some(100);
        (routine, events, outbound_recv, cancel)
    }

    /// The routine pulls eligible work, records its expectation, sends `GetHeaders`
    /// on its own stream, and emits `PeerWorkAssigned` — all before yielding, so the
    /// expectation is recorded before any response could be decoded.
    #[tokio::test]
    async fn routine_pulls_work_sends_get_headers_and_records_expectation() {
        let queue = SharedHeaderRangeQueue::new();
        queue.ensure_forward(forward_range(1, 5));
        let (mut routine, mut events, mut outbound, _cancel) = pulling_routine(queue.clone(), 9);

        routine.try_pull();

        // The expectation was recorded locally and the slot marked in-flight.
        assert_eq!(
            routine.local_mut().expected_headers.front(),
            Some(&ExpectedHeadersResponse::new(block::Height(1), 5).unwrap()),
            "the expectation is recorded synchronously at pull time"
        );
        assert!(routine.local_mut().in_flight.is_some());

        // A `GetHeaders` frame was sent on the routine's own stream.
        let frame = outbound.recv().await.expect("GetHeaders frame was sent");
        assert_eq!(
            u8::try_from(frame.message_type).ok(),
            Some(MSG_HS_GET_HEADERS)
        );

        // `PeerWorkAssigned` was emitted for the reactor to record the outstanding.
        match events.try_recv() {
            Ok(HeaderSyncEvent::PeerWorkAssigned {
                start_height,
                count,
                generation,
                ..
            }) => {
                assert_eq!(start_height, block::Height(1));
                assert_eq!(count, 5);
                assert_eq!(generation, 9);
            }
            other => panic!("expected PeerWorkAssigned, got {other:?}"),
        }
    }

    /// Dropping a routine that holds in-flight work returns the range to the shared
    /// queue, so a teardown (stream close, cancel, or panic, which all run `Drop`)
    /// never strands an assigned range.
    #[test]
    fn dropping_routine_returns_in_flight_work() {
        let queue = SharedHeaderRangeQueue::new();
        queue.ensure_forward(forward_range(1, 5));
        let (mut routine, _events, _outbound, _cancel) = pulling_routine(queue.clone(), 1);
        routine.try_pull();
        assert!(routine.local_mut().in_flight.is_some());
        // The range is assigned to this peer while in flight.
        assert!(queue
            .snapshot()
            .assigned
            .keys()
            .any(|range| range.start_height == block::Height(1)));

        drop(routine);

        // After teardown the assignment is cleared and the range is re-queued.
        let snapshot = queue.snapshot();
        assert!(
            snapshot
                .assigned
                .get(&forward_range(1, 5))
                .is_none_or(|peers| peers.is_empty()),
            "teardown clears the in-flight assignment"
        );
        assert!(
            snapshot
                .forward
                .iter()
                .any(|range| range.start_height == block::Height(1)),
            "teardown re-queues the in-flight range so it is never stranded"
        );
    }

    /// Service cancellation drives the routine out of its loop, and its `Drop`
    /// returns the in-flight range to the queue (the cancel exit path).
    #[tokio::test]
    async fn cancelled_routine_returns_in_flight_work_on_exit() {
        let queue = SharedHeaderRangeQueue::new();
        queue.ensure_forward(forward_range(1, 5));
        let (routine, _events, _outbound, cancel) = pulling_routine(queue.clone(), 1);
        // The routine pulls on startup; cancel it and let it run to exit + Drop.
        cancel.cancel();
        let run = tokio::spawn(routine.run());
        let _ = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("the cancelled routine exits promptly");

        let snapshot = queue.snapshot();
        assert!(
            snapshot
                .forward
                .iter()
                .any(|range| range.start_height == block::Height(1)),
            "a cancelled routine returns its in-flight range"
        );
    }

    /// A panic after the routine took work still returns the range, because `Drop`
    /// runs during unwind.
    #[test]
    fn panicking_routine_returns_in_flight_work_via_drop() {
        let queue = SharedHeaderRangeQueue::new();
        queue.ensure_forward(forward_range(1, 5));
        let queue_for_panic = queue.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let (mut routine, _events, _outbound, _cancel) = pulling_routine(queue_for_panic, 1);
            routine.try_pull();
            assert!(routine.local_mut().in_flight.is_some());
            panic!("routine panics while holding in-flight work");
        }));
        assert!(result.is_err(), "the closure panicked");

        let snapshot = queue.snapshot();
        assert!(
            snapshot
                .forward
                .iter()
                .any(|range| range.start_height == block::Height(1)),
            "a panicking routine returns its in-flight range via Drop"
        );
    }

    /// A stale-generation routine's teardown return must not clear a newer session's
    /// live assignment for the same peer and range (reconnect generation guard).
    #[test]
    fn stale_generation_teardown_does_not_erase_new_session_assignment() {
        let queue = SharedHeaderRangeQueue::new();
        queue.ensure_forward(forward_range(1, 5));

        // Old session (generation 1) takes the range, then a new session
        // (generation 2) reconnects and takes the same range after the old one's
        // assignment is cleared (simulating reconnect cleanup).
        let (mut old_routine, _e1, _o1, _c1) = pulling_routine(queue.clone(), 1);
        old_routine.try_pull();
        // Simulate reconnect: forget the old peer's assignment, then the new session
        // takes the range.
        queue.forget_peer(&peer());
        let (mut new_routine, _e2, _o2, _c2) = pulling_routine(queue.clone(), 2);
        new_routine.try_pull();
        assert!(new_routine.local_mut().in_flight.is_some());

        // The OLD routine now tears down (Drop), returning under generation 1. The
        // new session (generation 2) still holds the range.
        drop(old_routine);

        let snapshot = queue.snapshot();
        let holds_new_generation = snapshot
            .assigned
            .get(&forward_range(1, 5))
            .is_some_and(|peers| peers.get(&peer()) == Some(&2));
        assert!(
            holds_new_generation,
            "an old session's teardown must not erase the new session's live assignment"
        );
    }

    // ============ routine-owned outbound stream writes (commands) =========

    /// Build a routine with a real outbound session and a live command sink, sharing
    /// a fresh empty range queue (so the routine never pulls and only the commands
    /// drive its outbound stream). Returns the routine, its command sink, its
    /// outbound `FramedRecv` (the frames the routine writes), its kept-alive inbound
    /// `peer_send` (so the inbound stream does not close and exit the routine), and
    /// its cancel token.
    #[allow(clippy::type_complexity)]
    fn commanding_routine() -> (
        HeaderSyncPeerRoutine,
        HeaderSyncCommandSink,
        crate::zakura::FramedRecv,
        FramedSend,
        CancellationToken,
    ) {
        let (handle, _events) = handle_with_queue(SharedHeaderRangeQueue::new());
        let (command_sink, command_receivers) = HeaderSyncCommandSink::channel();
        let (peer_send, service_recv) = crate::zakura::framed_channel(16);
        let (outbound_send, outbound_recv) = crate::zakura::framed_channel(16);
        let cancel = CancellationToken::new();
        let env = HsEnv::new(
            handle,
            Network::Mainnet,
            ZakuraHeaderSyncConfig::default(),
            MAX_HS_MESSAGE_BYTES as u32,
        );
        let session = crate::zakura::HeaderSyncPeerSession::from_parts_with_direction(
            peer(),
            crate::zakura::ServicePeerDirection::Inbound,
            outbound_send,
            cancel.clone(),
        );
        let routine = HeaderSyncPeerRoutine::for_test(
            peer(),
            command_receivers,
            env,
            service_recv,
            cancel.clone(),
            Some(session),
            0,
        );
        (routine, command_sink, outbound_recv, peer_send, cancel)
    }

    fn block_one() -> Arc<block::Block> {
        use zebra_chain::serialization::ZcashDeserializeInto;
        use zebra_test::vectors::BLOCK_MAINNET_1_BYTES;
        Arc::new(
            BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .expect("block 1 vector parses"),
        )
    }

    async fn next_outbound(outbound: &mut crate::zakura::FramedRecv) -> Frame {
        tokio::time::timeout(Duration::from_secs(2), outbound.recv())
            .await
            .expect("the routine wrote a frame promptly")
            .expect("the outbound stream is open")
    }

    /// A `SendStatus` command makes the ROUTINE write the `Status` on its own stream
    /// (the reactor no longer writes status frames directly).
    #[tokio::test]
    async fn routine_writes_commanded_status_on_its_own_stream() {
        let (routine, sink, mut outbound, _peer_send, cancel) = commanding_routine();
        let run = tokio::spawn(routine.run());

        let status = HeaderSyncStatus {
            tip_height: block::Height(42),
            tip_hash: block::Hash([3; 32]),
            anchor_height: block::Height(0),
            max_headers_per_response: 100,
            max_inflight_requests: 1,
        };
        assert_eq!(
            sink.enqueue_status(status),
            HeaderSyncCommandOutcome::Queued
        );

        let frame = next_outbound(&mut outbound).await;
        assert_eq!(u8::try_from(frame.message_type).ok(), Some(MSG_HS_STATUS));

        cancel.cancel();
        let _ = run.await;
    }

    /// A `SendHeaders` command makes the ROUTINE write the `Headers` response on its
    /// own stream.
    #[tokio::test]
    async fn routine_writes_commanded_headers_on_its_own_stream() {
        let (routine, sink, mut outbound, _peer_send, cancel) = commanding_routine();
        let run = tokio::spawn(routine.run());

        let headers = vec![block_one().header.clone()];
        assert_eq!(
            sink.enqueue_headers(headers.clone(), vec![0]),
            HeaderSyncCommandOutcome::Queued
        );

        let frame = next_outbound(&mut outbound).await;
        assert_eq!(u8::try_from(frame.message_type).ok(), Some(MSG_HS_HEADERS));

        cancel.cancel();
        let _ = run.await;
    }

    /// A `ForwardNewBlock` command makes the ROUTINE write the `NewBlock` on its own
    /// stream.
    #[tokio::test]
    async fn routine_writes_commanded_new_block_on_its_own_stream() {
        let (routine, sink, mut outbound, _peer_send, cancel) = commanding_routine();
        let run = tokio::spawn(routine.run());

        assert_eq!(
            sink.enqueue_new_block(block_one()),
            HeaderSyncCommandOutcome::Queued
        );

        let frame = next_outbound(&mut outbound).await;
        assert_eq!(
            u8::try_from(frame.message_type).ok(),
            Some(MSG_HS_NEW_BLOCK)
        );

        cancel.cancel();
        let _ = run.await;
    }

    /// Dropping the command sink closes both command channels; the routine exits
    /// cleanly (the documented channel-close behavior), even with its stream still
    /// open. This proves the routine treats a closed command channel as a clean exit,
    /// not a protocol reject.
    #[tokio::test]
    async fn routine_exits_cleanly_when_command_channel_closes() {
        let (routine, sink, _outbound, _peer_send, _cancel) = commanding_routine();
        let run = tokio::spawn(routine.run());

        // Close the command channels by dropping the only sink.
        drop(sink);

        let result = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("a routine whose command channel closed exits promptly")
            .expect("the routine task does not panic");
        assert!(
            result.is_ok(),
            "a closed command channel is a clean exit, not a reject"
        );
    }

    /// A reset/timeout `DropExpectation` command is applied (never silently dropped):
    /// it pops the matching expectation, frees the outbound slot, and credits the
    /// late-covered tolerance so a late response is absorbed rather than rejected.
    #[test]
    fn routine_applies_drop_expectation_reset_command() {
        let (mut routine, _sink, _outbound, _peer_send, _cancel) = commanding_routine();
        // Seed the expectation+slot the routine would have recorded when it pulled
        // work and sent the matching `GetHeaders` (generation 0 matches the routine).
        routine.seed_expected(block::Height(1), 5);
        assert!(routine.local_mut().in_flight.is_some());
        assert_eq!(routine.local_mut().expected_headers.len(), 1);

        routine.handle_command(HeaderSyncPeerCommand::DropExpectation {
            generation: 0,
            start_height: block::Height(1),
            count: 5,
        });

        assert!(
            routine.local_mut().in_flight.is_none(),
            "the reset freed the outbound slot"
        );
        assert!(
            routine.local_mut().expected_headers.is_empty(),
            "the reset popped the matching expectation"
        );
        assert_eq!(
            routine.local_mut().late_covered_tolerance,
            1,
            "the reset credits a late-covered tolerance so a late response is absorbed"
        );
    }

    /// A `DropExpectation` for a non-matching generation is ignored, so a misrouted or
    /// stale reset cannot disturb a reconnected session's live expectation.
    #[test]
    fn routine_ignores_drop_expectation_for_other_generation() {
        let (mut routine, _sink, _outbound, _peer_send, _cancel) = commanding_routine();
        routine.seed_expected(block::Height(1), 5);

        // The routine's generation is 0; a command for generation 9 is dropped.
        routine.handle_command(HeaderSyncPeerCommand::DropExpectation {
            generation: 9,
            start_height: block::Height(1),
            count: 5,
        });

        assert!(
            routine.local_mut().in_flight.is_some(),
            "a generation-stale reset must not free the live slot"
        );
        assert_eq!(
            routine.local_mut().expected_headers.len(),
            1,
            "a generation-stale reset must not pop the live expectation"
        );
    }

    /// A slow peer (its outbound stream backed up so its own writes are dropped at
    /// the bounded transport seam) affects ONLY that peer's routine: a second routine
    /// with its own stream and command channel keeps writing its commanded frames.
    /// Per-peer command channels and streams mean one peer's backpressure never
    /// wedges another. The slow routine stays alive (its non-blocking write drops the
    /// over-full frame and continues), so when its stream drains it writes again.
    #[tokio::test]
    async fn slow_peer_does_not_wedge_unrelated_peer_routine() {
        let (handle, _events) = handle_with_queue(SharedHeaderRangeQueue::new());
        let env = HsEnv::new(
            handle,
            Network::Mainnet,
            ZakuraHeaderSyncConfig::default(),
            MAX_HS_MESSAGE_BYTES as u32,
        );
        let make = |env: HsEnv, outbound_depth: usize| {
            let (sink, receivers) = HeaderSyncCommandSink::channel();
            // Keep `peer_send` alive so the inbound stream stays open and the routine
            // does not exit on a stream close.
            let (peer_send, service_recv) = crate::zakura::framed_channel(16);
            let (outbound_send, outbound_recv) = crate::zakura::framed_channel(outbound_depth);
            let cancel = CancellationToken::new();
            let session = crate::zakura::HeaderSyncPeerSession::from_parts_with_direction(
                peer(),
                crate::zakura::ServicePeerDirection::Inbound,
                outbound_send,
                cancel.clone(),
            );
            let routine = HeaderSyncPeerRoutine::for_test(
                peer(),
                receivers,
                env,
                service_recv,
                cancel.clone(),
                Some(session),
                0,
            );
            (routine, sink, outbound_recv, peer_send, cancel)
        };
        // The slow peer's outbound stream has depth 1 and is never read until the end.
        let (slow, slow_sink, mut slow_out, _slow_peer_send, slow_cancel) = make(env.clone(), 1);
        let (fast, fast_sink, mut fast_out, _fast_peer_send, fast_cancel) = make(env, 16);
        let slow_run = tokio::spawn(slow.run());
        let fast_run = tokio::spawn(fast.run());

        // Back the slow peer's stream up: many commanded forwards, none read.
        for _ in 0..8 {
            slow_sink.enqueue_new_block(block_one());
        }

        // The fast peer's routine is unaffected: its commanded status is written and
        // observable even while the slow peer's stream is backed up.
        fast_sink.enqueue_status(HeaderSyncStatus {
            tip_height: block::Height(7),
            tip_hash: block::Hash([9; 32]),
            anchor_height: block::Height(0),
            max_headers_per_response: 100,
            max_inflight_requests: 1,
        });
        let fast_frame = next_outbound(&mut fast_out).await;
        assert_eq!(
            u8::try_from(fast_frame.message_type).ok(),
            Some(MSG_HS_STATUS),
            "an unrelated peer's routine keeps writing while another peer's stream is backed up"
        );

        // The slow routine is still alive: its depth-1 stream holds one queued frame,
        // and after the test drains it the routine can write again on a fresh command.
        let queued = next_outbound(&mut slow_out).await;
        assert_eq!(
            u8::try_from(queued.message_type).ok(),
            Some(MSG_HS_NEW_BLOCK)
        );
        slow_sink.enqueue_status(HeaderSyncStatus {
            tip_height: block::Height(11),
            tip_hash: block::Hash([4; 32]),
            anchor_height: block::Height(0),
            max_headers_per_response: 100,
            max_inflight_requests: 1,
        });
        let after_drain = next_outbound(&mut slow_out).await;
        assert!(
            matches!(
                u8::try_from(after_drain.message_type).ok(),
                Some(MSG_HS_NEW_BLOCK) | Some(MSG_HS_STATUS)
            ),
            "the slow routine resumes writing once its stream drains; it was stalled, not dead"
        );

        slow_cancel.cancel();
        fast_cancel.cancel();
        let _ = slow_run.await;
        let _ = fast_run.await;
    }
}
