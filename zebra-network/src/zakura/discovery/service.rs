//! Native discovery service (stream kind 4) on the Zakura transport.
//!
//! Discovery is a single short-lived ordered stream per peer, driven by one
//! supervised [`DiscoveryPeerRoutine`]. The routine admits the peer into shared
//! discovery state, sends the startup exchange (`Hello`, `GetPeers`,
//! `GetServices`), then loops over inbound frames (`Hello`, `GetPeers`, `Peers`,
//! `GetServices`, `Services`) until the exchange settles, the stream closes, the
//! session is cancelled, or a frame is rejected. The wire format is the
//! [`DiscoveryMessage`] payload carried inside a generic transport [`Frame`]
//! (`message_type = DISCOVERY_FRAME_MESSAGE_TYPE`, `flags = 0`), identical to the
//! original native-discovery wire so peers interoperate.

use std::time::Duration;

use iroh::NodeId;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::zakura::{
    handle_routine_exit, spawn_supervised_routine, Admit, BlockSyncHandle, Frame, FramedRecv,
    FramedSend, HeaderSyncEvent, HeaderSyncHandle, OrderedSendError, Peer, PeerStreamSession,
    Service, ServiceAdmissionDecision, ServicePeerDirection, SessionGuard, SinkReject, Stream,
    StreamMode, ZakuraPeerId, LOCAL_MAX_CONTROL_FRAME_BYTES, ZAKURA_CAP_DISCOVERY,
    ZAKURA_CAP_HEADER_SYNC,
};

use super::protocol::{
    BlockSyncServiceSummary, DiscoveryBookError, DiscoveryMessage, DiscoveryRecordError,
    GetServices, HeaderSyncServiceSummary, ServiceSummaryEnvelope, Services, ZakuraDiscoveryHandle,
    ZakuraNodeRecord, ZakuraServiceId, MAX_DISCOVERY_MESSAGE_BYTES,
    MAX_DISCOVERY_RECORDS_PER_RESPONSE, ZAKURA_DISCOVERY_STREAM_VERSION, ZAKURA_STREAM_DISCOVERY,
};

/// Maximum time discovery waits for first-party exchange responses before releasing the session.
const DISCOVERY_EXCHANGE_SETTLE_TIMEOUT: Duration = Duration::from_secs(2);

/// Frame message type carrying a discovery payload (matches the native wire).
pub(super) const DISCOVERY_FRAME_MESSAGE_TYPE: u16 = 1;

/// The single inbound frame envelope type discovery admits at the stream guard.
const DISCOVERY_ALLOWED_FRAME_TYPES: &[u8] = &[1];

const DISCOVERY_SERVICE_STREAMS: [Stream; 1] = [Stream {
    kind: ZAKURA_STREAM_DISCOVERY,
    version: ZAKURA_DISCOVERY_STREAM_VERSION,
    // Advisory until the transport wires Stream::frame_cap end-to-end; the
    // authoritative inbound cap is app_frame_cap_for_stream_kind.
    frame_cap: LOCAL_MAX_CONTROL_FRAME_BYTES,
    capability: ZAKURA_CAP_DISCOVERY,
    mode: StreamMode::Ordered,
}];

/// Service-declared streams for native discovery.
pub(crate) fn discovery_streams() -> &'static [Stream] {
    &DISCOVERY_SERVICE_STREAMS
}

/// Build the discovery stream's inbound guard: the transport already applies the
/// connection-global count bucket, so discovery's guard owns the stream-4 frame
/// type boundary and the payload cap. Protocol-message variants remain
/// payload-level decode decisions because discovery uses one frame envelope type.
fn discovery_guard() -> SessionGuard {
    SessionGuard::new(
        DISCOVERY_ALLOWED_FRAME_TYPES,
        // `MAX_DISCOVERY_MESSAGE_BYTES` is 16 KiB, so it fits in `u32`.
        MAX_DISCOVERY_MESSAGE_BYTES as u32,
        None,
    )
}

/// Decodes a discovery message from a transport frame, rejecting a frame whose
/// envelope is not a discovery payload.
pub(super) fn decode_discovery_frame(frame: &Frame) -> Result<DiscoveryMessage, crate::BoxError> {
    if frame.message_type != DISCOVERY_FRAME_MESSAGE_TYPE || frame.flags != 0 {
        return Err(format!(
            "unexpected discovery frame envelope (message_type={}, flags={})",
            frame.message_type, frame.flags
        )
        .into());
    }
    DiscoveryMessage::decode(&frame.payload).map_err(Into::into)
}

/// Cloneable typed sender for one native discovery ordered stream.
#[derive(Clone, Debug)]
pub struct DiscoveryPeerSession {
    peer_id: ZakuraPeerId,
    direction: ServicePeerDirection,
    send: FramedSend,
    cancel: CancellationToken,
}

impl DiscoveryPeerSession {
    fn new(session: &PeerStreamSession, direction: ServicePeerDirection) -> Self {
        Self {
            peer_id: session.peer_id().clone(),
            direction,
            send: session.sender(),
            cancel: session.cancel_token(),
        }
    }

    /// Authenticated peer identity for this discovery stream.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    /// Direction of the underlying Zakura connection.
    pub fn direction(&self) -> ServicePeerDirection {
        self.direction
    }

    /// Peer disconnect/local shutdown cancellation token.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Send this node's signed self-record.
    pub fn try_send_hello(&self, record: ZakuraNodeRecord) -> Result<(), OrderedSendError> {
        self.try_send_message(DiscoveryMessage::Hello { record })
    }

    /// Ask this peer for more peer records.
    pub fn try_send_get_peers(
        &self,
        limit: u16,
        wanted_services: Vec<ZakuraServiceId>,
        exclude_node_ids: Vec<NodeId>,
    ) -> Result<(), OrderedSendError> {
        self.try_send_message(DiscoveryMessage::GetPeers {
            limit,
            wanted_services,
            exclude_node_ids,
        })
    }

    /// Send peer records to this peer.
    pub fn try_send_peers(&self, records: Vec<ZakuraNodeRecord>) -> Result<(), OrderedSendError> {
        self.try_send_message(DiscoveryMessage::Peers { records })
    }

    /// Ask this peer for its own live service summaries.
    pub fn try_send_get_services(
        &self,
        wanted_services: Vec<ZakuraServiceId>,
    ) -> Result<(), OrderedSendError> {
        self.try_send_message(DiscoveryMessage::GetServices(GetServices {
            wanted_services,
        }))
    }

    /// Send this node's first-party live service summaries.
    pub fn try_send_services(&self, services: Services) -> Result<(), OrderedSendError> {
        self.try_send_message(DiscoveryMessage::Services(services))
    }

    fn try_send_message(&self, message: DiscoveryMessage) -> Result<(), OrderedSendError> {
        let payload = message
            .encode()
            .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        match self.send.try_send(Frame {
            message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
            flags: 0,
            payload,
        }) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_frame)) => {
                Err(OrderedSendError::Full)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_frame)) => {
                Err(OrderedSendError::Closed)
            }
        }
    }
}

/// Native discovery service backed by a [`ZakuraDiscoveryHandle`] runtime.
#[derive(Clone, Debug)]
pub struct DiscoveryService {
    handle: ZakuraDiscoveryHandle,
    header_sync: Option<HeaderSyncHandle>,
    block_sync: Option<BlockSyncHandle>,
}

impl DiscoveryService {
    /// Builds a discovery service driven by `handle`.
    pub fn new(handle: ZakuraDiscoveryHandle) -> Self {
        Self {
            handle,
            header_sync: None,
            block_sync: None,
        }
    }

    /// Builds a discovery service with header-sync and block-sync summary providers.
    pub(crate) fn with_sync_services(
        handle: ZakuraDiscoveryHandle,
        header_sync: HeaderSyncHandle,
        block_sync: Option<BlockSyncHandle>,
    ) -> Self {
        Self {
            handle,
            header_sync: Some(header_sync),
            block_sync,
        }
    }

    /// Returns the underlying discovery runtime handle.
    pub fn handle(&self) -> &ZakuraDiscoveryHandle {
        &self.handle
    }
}

impl Service for DiscoveryService {
    fn name(&self) -> &'static str {
        "discovery"
    }

    fn streams(&self) -> &[Stream] {
        discovery_streams()
    }

    fn wants_peer(
        &self,
        _peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> bool {
        // Discovery escalation only checks this reactor's local room; live
        // summaries are first-party advisory data imported by the runtime.
        let snapshot = self.handle.peer_snapshot();
        match direction {
            ServicePeerDirection::Inbound => snapshot.inbound_slots_free > 0,
            ServicePeerDirection::Outbound => snapshot.outbound_slots_free > 0,
        }
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some((recv, send)) = peer.take_stream(ZAKURA_STREAM_DISCOVERY) else {
            return;
        };
        let Some(peer_node_id) = node_id_from_peer_id(&peer.id) else {
            // A peer id that is not a 32-byte node id cannot be a discovery
            // author; drop the stream without registering an exchange.
            return;
        };
        let session = PeerStreamSession::new(
            peer.id.clone(),
            ZAKURA_STREAM_DISCOVERY,
            recv,
            send,
            peer.service_cancel_token(),
        );
        let discovery_session = DiscoveryPeerSession::new(&session, peer.direction);
        let service_cancel = discovery_session.cancel_token();
        let connection_cancel = peer.cancel_token();
        let other_service_negotiated =
            peer.negotiated & !(ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_HEADER_SYNC) != 0;
        let (peer_id, _stream_kind, recv, _send, _session_cancel) = session.into_parts();

        let inputs = DiscoveryRoutineInputs {
            handle: self.handle.clone(),
            header_sync: self.header_sync.clone(),
            block_sync: self.block_sync.clone(),
            peer_node_id,
            session: discovery_session,
            recv,
            connection_cancel: connection_cancel.clone(),
            other_service_negotiated,
        };

        // One supervised routine owns admission, the exchange IO, progress, the
        // settle deadline, and cleanup. A protocol reject returned from the
        // routine cancels the whole connection via `handle_routine_exit`; the
        // routine's `Drop` guard removes admitted discovery state on every exit
        // path (normal, cancel, stream close, reject, panic). `on_panic` cancels
        // the connection so a panicked routine never leaves a half-live peer.
        let pipe = {
            let connection_cancel = connection_cancel.clone();
            async move {
                let result = DiscoveryPeerRoutine::admit_and_run(inputs).await;
                handle_routine_exit("discovery", &connection_cancel, result);
            }
        };
        let on_panic = move || connection_cancel.cancel();
        // Let the returned handle drop to detach the supervised task; the
        // `PeerRoutineTeardown` still cancels the service token and runs cleanup on
        // every exit path.
        spawn_supervised_routine(peer_id, service_cancel, || {}, on_panic, pipe);
    }

    fn remove_peer(&self, peer: &ZakuraPeerId) {
        let handle = self.handle.clone();
        let peer = peer.clone();
        tokio::spawn(async move {
            handle.remove_peer(&peer).await;
        });
    }
}

/// Constructed inputs handed to one supervised discovery routine.
struct DiscoveryRoutineInputs {
    handle: ZakuraDiscoveryHandle,
    header_sync: Option<HeaderSyncHandle>,
    block_sync: Option<BlockSyncHandle>,
    peer_node_id: NodeId,
    session: DiscoveryPeerSession,
    recv: FramedRecv,
    connection_cancel: CancellationToken,
    other_service_negotiated: bool,
}

/// The single supervised routine that drives one peer's discovery stream from
/// admission to teardown.
///
/// The routine owns the inbound decode/dispatch, the startup exchange sends, the
/// routine-local progress flags, the settle deadline, and — through its [`Drop`]
/// guard — the removal of admitted discovery peer state on every exit path.
struct DiscoveryPeerRoutine {
    handle: ZakuraDiscoveryHandle,
    header_sync: Option<HeaderSyncHandle>,
    block_sync: Option<BlockSyncHandle>,
    peer_node_id: NodeId,
    session: DiscoveryPeerSession,
    recv: FramedRecv,
    connection_cancel: CancellationToken,
    other_service_negotiated: bool,

    // ---- routine-local exchange progress (replaces DiscoveryExchangeProgress) ----
    /// Received a valid first-party `Hello` from the connected peer.
    received_hello: bool,
    /// Received/imported a `Peers` response.
    received_peers: bool,
    /// Received/imported a `Services` response.
    received_services: bool,

    /// Cleanup ownership: while `true`, the `Drop` guard schedules `remove_peer`.
    /// Set once admission succeeds; cleared only after the routine itself runs the
    /// async `remove_peer` on a normal exit so the work is never done twice.
    admitted: bool,
}

impl DiscoveryPeerRoutine {
    /// Admit the peer into shared discovery state, then run the exchange.
    ///
    /// Admission happens here, inside the one supervised task, so the routine's
    /// `Drop` guard is the single owner of admitted-state cleanup. If admission is
    /// rejected the service session is parked (the supervised routine's
    /// `PeerRoutineTeardown` cancels the per-service token on return) and no peer
    /// state was admitted, so
    /// nothing leaks. A clean park returns `Ok(())`, which `handle_routine_exit`
    /// leaves the shared connection alone for.
    async fn admit_and_run(inputs: DiscoveryRoutineInputs) -> Result<(), SinkReject> {
        let DiscoveryRoutineInputs {
            handle,
            header_sync,
            block_sync,
            peer_node_id,
            session,
            recv,
            connection_cancel,
            other_service_negotiated,
        } = inputs;

        let decision = handle
            .admit_peer(session.peer_id().clone(), session.direction())
            .await;
        if decision != ServiceAdmissionDecision::Admit {
            tracing::debug!(
                peer = ?session.peer_id(),
                direction = ?session.direction(),
                ?decision,
                "locally parking Zakura discovery service session"
            );
            return Ok(());
        }

        let routine = DiscoveryPeerRoutine {
            handle,
            header_sync,
            block_sync,
            peer_node_id,
            session,
            recv,
            connection_cancel,
            other_service_negotiated,
            received_hello: false,
            received_peers: false,
            received_services: false,
            admitted: true,
        };
        routine.run().await
    }

    /// Drive the exchange to completion, then handle the discovery-only
    /// disconnect decision. A `SinkReject` from the inbound loop propagates out so
    /// the supervised pipe cancels the connection (protocol) or parks the service
    /// (local). The `Drop` guard removes admitted peer state on every return.
    async fn run(mut self) -> Result<(), SinkReject> {
        let send_ok = self.send_startup_exchange().await;
        let exchanged = if send_ok {
            // Loop over inbound frames until the exchange settles, the stream
            // closes, the session is cancelled, or a frame is rejected.
            self.run_inbound_loop().await?;
            true
        } else {
            // The send side is gone before any inbound work — treat the exchange
            // as not completed (matches the pre-refactor source-side `Err(())`).
            false
        };

        if exchanged {
            self.handle
                .mark_short_lived_exchange(&self.peer_node_id)
                .await;
        }

        // Run the async cleanup here so a normal exit does the remove inline; the
        // `Drop` guard then only fires on the abnormal paths (cancel/reject/panic).
        self.admitted = false;
        self.handle.remove_peer(self.session.peer_id()).await;

        // A successful discovery-only exchange disconnects the peer only when no
        // other negotiated service owns it; a multi-service connection is left
        // alive because discovery merely finished its short exchange.
        if exchanged
            && !peer_has_other_service_owner(
                self.header_sync.as_ref(),
                self.peer_node_id,
                self.other_service_negotiated,
            )
        {
            self.connection_cancel.cancel();
        }

        Ok(())
    }

    /// Send `Hello`, then a sampled `GetPeers`, then a `GetServices`. Returns
    /// `false` once the stream's send side is gone so the routine stops the
    /// exchange (a `Full` queue is tolerated; an `Encode` error is logged and
    /// skipped, matching the pre-refactor source behavior).
    async fn send_startup_exchange(&self) -> bool {
        let record = (*self.handle.current_self_record()).clone();
        if !self.handle_send_result(self.session.try_send_hello(record)) {
            return false;
        }

        let limit = self
            .handle
            .peer_sample_limit()
            .await
            .min(MAX_DISCOVERY_RECORDS_PER_RESPONSE);
        let exclude_node_ids = self.handle.peer_sample_exclusions().await;
        // `peer_sample_limit` is bounded by MAX_DISCOVERY_RECORDS_PER_RESPONSE
        // (<= u16::MAX), so the cast cannot truncate.
        if !self.handle_send_result(self.session.try_send_get_peers(
            limit as u16,
            Vec::new(),
            exclude_node_ids,
        )) {
            return false;
        }

        self.handle_send_result(self.session.try_send_get_services(Vec::new()))
    }

    fn handle_send_result(&self, result: Result<(), OrderedSendError>) -> bool {
        match result {
            Ok(()) | Err(OrderedSendError::Full) => true,
            Err(OrderedSendError::Closed) => false,
            Err(OrderedSendError::Encode(error)) => {
                tracing::debug!(
                    ?error,
                    peer = ?self.session.peer_id(),
                    "failed to encode Zakura discovery message"
                );
                true
            }
        }
    }

    /// Loop over inbound frames until the exchange is complete, the stream closes,
    /// the session is cancelled, or the settle deadline elapses. A rejected frame
    /// returns the `SinkReject` so the supervised pipe applies the connection/
    /// service teardown.
    async fn run_inbound_loop(&mut self) -> Result<(), SinkReject> {
        let cancel = self.session.cancel_token();
        let mut guard = discovery_guard();
        let settle_deadline = Instant::now() + DISCOVERY_EXCHANGE_SETTLE_TIMEOUT;
        let sleep = tokio::time::sleep_until(settle_deadline);
        tokio::pin!(sleep);

        loop {
            if self.exchange_complete() {
                return Ok(());
            }
            let frame = tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                () = &mut sleep => return Ok(()),
                frame = self.recv.recv() => frame,
            };
            let Some(frame) = frame else {
                return Ok(());
            };

            match guard.admit(&frame) {
                Admit::Pass => {}
                // The discovery guard owns only the type filter and the oversize
                // cap; neither yields `Throttle`. A throttle would be a guard bug,
                // not the peer's fault — stop the service without scoring the peer.
                Admit::Throttle => {
                    return Err(SinkReject::local(
                        "discovery guard unexpectedly throttled an inbound frame",
                    ));
                }
                Admit::Reject(reason) => return Err(SinkReject::protocol(reason)),
            }

            let message = match decode_discovery_frame(&frame) {
                Ok(message) => message,
                Err(error) => return Err(SinkReject::protocol(error)),
            };
            self.handle_message(message).await?;
        }
    }

    /// All three first-party exchange responses have been received/imported.
    fn exchange_complete(&self) -> bool {
        self.received_hello && self.received_peers && self.received_services
    }

    async fn handle_message(&mut self, message: DiscoveryMessage) -> Result<(), SinkReject> {
        match message {
            DiscoveryMessage::Hello { record } => self.handle_hello(record).await,
            DiscoveryMessage::GetPeers {
                limit,
                wanted_services,
                exclude_node_ids,
            } => {
                let records = self
                    .handle
                    .sample_peers(usize::from(limit), &wanted_services, &exclude_node_ids)
                    .await;
                self.send_peers(records)
            }
            DiscoveryMessage::Peers { records } => {
                self.handle
                    .import_peer_records(records, Some(self.peer_node_id))
                    .await;
                self.received_peers = true;
                Ok(())
            }
            DiscoveryMessage::GetServices(query) => {
                let services = self.local_services_response(query).await?;
                self.send_services(services)
            }
            DiscoveryMessage::Services(services) => self.handle_services(services).await,
        }
    }

    async fn local_services_response(&self, query: GetServices) -> Result<Services, SinkReject> {
        let mut summaries = Vec::new();

        if service_wanted(&query.wanted_services, &ZakuraServiceId::header_sync()) {
            if let Some(header_sync) = &self.header_sync {
                let (best_height, best_hash) = header_sync.best_header_tip();
                let summary = HeaderSyncServiceSummary::from_snapshot(
                    best_height,
                    best_hash,
                    None,
                    true,
                    header_sync.peer_snapshot(),
                );
                summaries.push(
                    ServiceSummaryEnvelope::header_sync(&summary).map_err(SinkReject::local)?,
                );
            }
        }

        if service_wanted(&query.wanted_services, &ZakuraServiceId::discovery()) {
            let summary = self.handle.local_discovery_summary().await;
            summaries.push(ServiceSummaryEnvelope::discovery(&summary).map_err(SinkReject::local)?);
        }

        if service_wanted(&query.wanted_services, &ZakuraServiceId::block_sync()) {
            if let Some(block_sync) = &self.block_sync {
                let summary = BlockSyncServiceSummary::from_status_and_snapshot(
                    block_sync.local_status(),
                    block_sync.peer_snapshot(),
                );
                summaries
                    .push(ServiceSummaryEnvelope::block_sync(&summary).map_err(SinkReject::local)?);
            }
        }

        Ok(self.handle.local_services_response(summaries))
    }

    async fn handle_services(&mut self, services: Services) -> Result<(), SinkReject> {
        if services.node_id != self.peer_node_id {
            return Err(SinkReject::protocol(
                "Zakura discovery SERVICES authored by a different node id",
            ));
        }

        let header_summaries =
            decode_header_sync_summaries(&services).map_err(SinkReject::protocol)?;
        self.handle
            .import_connected_peer_services(services, self.peer_node_id)
            .await
            .map_err(SinkReject::protocol)?;
        self.received_services = true;

        if let Some(header_sync) = &self.header_sync {
            for summary in header_summaries {
                if let Err(error) = header_sync
                    .send(HeaderSyncEvent::AdvisoryHeaderSummary {
                        peer: self.session.peer_id().clone(),
                        summary,
                    })
                    .await
                {
                    tracing::debug!(
                        ?error,
                        peer = ?self.session.peer_id(),
                        "failed to queue first-party Zakura header-sync advisory summary"
                    );
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_hello(&mut self, record: ZakuraNodeRecord) -> Result<(), SinkReject> {
        if record.body.node_id != self.peer_node_id {
            return Err(SinkReject::protocol(
                "Zakura discovery hello authored by a different node id",
            ));
        }
        match self
            .handle
            .import_connected_peer_record(record, self.peer_node_id)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_advisory_self_record_import_error(&error) => {
                tracing::debug!(?error, "ignoring advisory discovery hello import error");
                Ok(())
            }
            Err(error) => Err(SinkReject::protocol(error)),
        }?;
        self.received_hello = true;
        Ok(())
    }

    fn send_peers(&self, records: Vec<ZakuraNodeRecord>) -> Result<(), SinkReject> {
        match self.session.try_send_peers(records) {
            Ok(()) | Err(OrderedSendError::Full) => Ok(()),
            Err(OrderedSendError::Closed) => {
                Err(SinkReject::local("Zakura discovery send channel closed"))
            }
            Err(OrderedSendError::Encode(error)) => Err(SinkReject::local(error)),
        }
    }

    fn send_services(&self, services: Services) -> Result<(), SinkReject> {
        match self.session.try_send_services(services) {
            Ok(()) | Err(OrderedSendError::Full) => Ok(()),
            Err(OrderedSendError::Closed) => {
                Err(SinkReject::local("Zakura discovery send channel closed"))
            }
            Err(OrderedSendError::Encode(error)) => Err(SinkReject::local(error)),
        }
    }
}

impl Drop for DiscoveryPeerRoutine {
    /// Remove admitted discovery peer state on every abnormal exit path
    /// (cancel/stream-close/reject/panic). The normal exit already ran
    /// `remove_peer` inline and cleared `admitted`, so this guard never double
    /// removes. `remove_peer` is async and `Drop` is sync, so the removal is
    /// scheduled on a detached task — the same pattern `DiscoveryService::remove_peer`
    /// uses for the registry-driven disconnect.
    fn drop(&mut self) {
        if !self.admitted {
            return;
        }
        let handle = self.handle.clone();
        let peer_id = self.session.peer_id().clone();
        tokio::spawn(async move {
            handle.remove_peer(&peer_id).await;
        });
    }
}

fn service_wanted(wanted_services: &[ZakuraServiceId], service_id: &ZakuraServiceId) -> bool {
    wanted_services.is_empty() || wanted_services.iter().any(|wanted| wanted == service_id)
}

fn decode_header_sync_summaries(
    services: &Services,
) -> Result<Vec<HeaderSyncServiceSummary>, crate::BoxError> {
    let mut summaries = Vec::new();
    for envelope in &services.summaries {
        if let Some(summary) = envelope.decode_header_sync()? {
            summaries.push(summary);
        }
    }
    Ok(summaries)
}

fn peer_has_other_service_owner(
    header_sync: Option<&HeaderSyncHandle>,
    peer_node_id: NodeId,
    other_service_negotiated: bool,
) -> bool {
    if other_service_negotiated {
        return true;
    }

    header_sync.is_some_and(|header_sync| {
        header_sync
            .candidate_state()
            .admitted_node_ids
            .contains(&peer_node_id)
    })
}

/// Returns the iroh node id encoded by a discovery peer id, if it is a 32-byte
/// node id.
fn node_id_from_peer_id(peer_id: &ZakuraPeerId) -> Option<NodeId> {
    let bytes: [u8; 32] = peer_id.as_bytes().try_into().ok()?;
    NodeId::from_bytes(&bytes).ok()
}

/// A peer-hello import error that should be logged and ignored rather than
/// closing the live connection. These mean the peer's record is not locally
/// dialable or has drifted out of the freshness window, neither of which is the
/// connected peer's fault.
fn is_advisory_self_record_import_error(error: &DiscoveryBookError) -> bool {
    matches!(
        error,
        DiscoveryBookError::NoUsableDirectAddress
            | DiscoveryBookError::NonDialableDirectAddress { .. }
            | DiscoveryBookError::Record(DiscoveryRecordError::Expired)
            | DiscoveryBookError::Record(DiscoveryRecordError::FarFutureExpiry)
    )
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use iroh::SecretKey;
    use tokio::{sync::watch, task::JoinHandle};

    use super::*;
    use crate::zakura::discovery::protocol::{
        DiscoveryServiceSummary, ZakuraLiveServiceSummary, ZakuraNodeRecordBody,
    };
    use crate::zakura::{
        framed_channel, spawn_block_sync_reactor, spawn_header_sync_reactor, BlockSyncFrontiers,
        BlockSyncStartup, HeaderSyncAction, HeaderSyncFrontiers, HeaderSyncPeerSession,
        HeaderSyncStartup, HeaderSyncStatus, ServicePeerLimits, ZakuraBlockSyncConfig,
        ZakuraDiscoveryConfig, ZakuraDiscoveryLocalConfig, ZakuraHandshakeConfig,
        ZakuraHeaderSyncConfig, LOCAL_MAX_MESSAGE_BYTES, MAX_BS_RESPONSE_BYTES,
        ZAKURA_CAP_BLOCK_SYNC, ZAKURA_CAP_DISCOVERY, ZAKURA_CAP_HEADER_SYNC,
    };
    use zebra_chain::{block, parameters::Network};

    struct HeaderAdvisoryFixture {
        discovery_handle: ZakuraDiscoveryHandle,
        header_sync: HeaderSyncHandle,
        // Held to keep the header-sync reactor's action receiver alive (so its
        // `dispatch_action` does not fail); no longer drained directly now that the
        // routine, not the reactor, pushes outbound `GetHeaders`.
        #[allow(dead_code)]
        header_actions: tokio::sync::mpsc::Receiver<HeaderSyncAction>,
        header_task: JoinHandle<()>,
        peer_node_id: NodeId,
        peer_id: ZakuraPeerId,
        peer_send: FramedSend,
        _peer_recv: FramedRecv,
    }

    impl Drop for HeaderAdvisoryFixture {
        fn drop(&mut self) {
            self.header_task.abort();
        }
    }

    fn current_test_unix_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_secs()
    }

    fn header_summary(best_height: block::Height) -> HeaderSyncServiceSummary {
        HeaderSyncServiceSummary {
            best_height,
            best_hash: block::Hash([7; 32]),
            finalized_height: None,
            serving_headers: true,
            inbound_slots_free: 1,
            inbound_slots_max: 1,
            outbound_slots_free: 1,
            outbound_slots_max: 1,
        }
    }

    fn spawn_test_header_sync() -> Result<
        (
            HeaderSyncHandle,
            tokio::sync::mpsc::Receiver<HeaderSyncAction>,
            JoinHandle<()>,
        ),
        crate::BoxError,
    > {
        let network = Network::new_regtest(Default::default());
        let anchor = (block::Height(0), network.genesis_hash());
        let mut startup = HeaderSyncStartup::new(
            network,
            anchor,
            HeaderSyncFrontiers {
                finalized_height: anchor.0,
                verified_block_tip: anchor.0,
                verified_block_hash: anchor.1,
            },
            Some(anchor),
            ZakuraHeaderSyncConfig::default(),
            LOCAL_MAX_MESSAGE_BYTES,
        );
        startup.range_state_actions_enabled = true;
        spawn_header_sync_reactor(startup).map_err(Into::into)
    }

    fn signed_header_sync_record(
        secret_key: &SecretKey,
        handshake: &ZakuraHandshakeConfig,
    ) -> Result<ZakuraNodeRecord, crate::BoxError> {
        let body = ZakuraNodeRecordBody {
            node_id: secret_key.public(),
            direct_addrs: vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(192, 0, 2, 44)),
                8233,
            )],
            services: vec![ZakuraServiceId::header_sync()],
            zakura_protocol_min: handshake.zakura_protocol_min,
            zakura_protocol_max: handshake.zakura_protocol_max,
            network_id: handshake.network_id,
            chain_id: handshake.chain_id,
            sequence: 1,
            expires_at_unix_secs: current_test_unix_secs().saturating_add(60),
        };
        Ok(ZakuraNodeRecord::sign(body, secret_key)?)
    }

    fn spawn_header_advisory_fixture(
        peer_seed: u8,
    ) -> Result<HeaderAdvisoryFixture, crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = SecretKey::from_bytes(&[31u8; 32]);
        let discovery_handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let (header_sync, header_actions, header_task) = spawn_test_header_sync()?;
        let service = DiscoveryService::with_sync_services(
            discovery_handle.clone(),
            header_sync.clone(),
            None,
        );
        let peer_node_id = SecretKey::from_bytes(&[peer_seed; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let (peer_send, service_recv) = framed_channel(8);
        let (service_send, peer_recv) = framed_channel(8);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id.clone(),
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            CancellationToken::new(),
        ));

        Ok(HeaderAdvisoryFixture {
            discovery_handle,
            header_sync,
            header_actions,
            header_task,
            peer_node_id,
            peer_id,
            peer_send,
            _peer_recv: peer_recv,
        })
    }

    async fn send_discovery_message(
        fixture: &HeaderAdvisoryFixture,
        message: DiscoveryMessage,
    ) -> Result<(), crate::BoxError> {
        fixture
            .peer_send
            .send(Frame {
                message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
                flags: 0,
                payload: message.encode()?,
            })
            .await?;
        Ok(())
    }

    fn discovery_frame(message: DiscoveryMessage) -> Result<Frame, crate::BoxError> {
        Ok(Frame {
            message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
            flags: 0,
            payload: message.encode()?,
        })
    }

    fn signed_discovery_record(
        secret_key: &SecretKey,
        handshake: &ZakuraHandshakeConfig,
    ) -> Result<ZakuraNodeRecord, crate::BoxError> {
        let body = ZakuraNodeRecordBody {
            node_id: secret_key.public(),
            direct_addrs: vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(192, 0, 2, 45)),
                8233,
            )],
            services: vec![ZakuraServiceId::discovery()],
            zakura_protocol_min: handshake.zakura_protocol_min,
            zakura_protocol_max: handshake.zakura_protocol_max,
            network_id: handshake.network_id,
            chain_id: handshake.chain_id,
            sequence: 1,
            expires_at_unix_secs: current_test_unix_secs().saturating_add(60),
        };
        Ok(ZakuraNodeRecord::sign(body, secret_key)?)
    }

    async fn complete_peer_side_discovery_exchange(
        peer_send: &FramedSend,
        peer_recv: &mut FramedRecv,
        peer_secret: &SecretKey,
        handshake: &ZakuraHandshakeConfig,
    ) -> Result<(), crate::BoxError> {
        let mut saw_hello = false;
        let mut saw_get_peers = false;
        let mut saw_get_services = false;
        while !(saw_hello && saw_get_peers && saw_get_services) {
            let frame = tokio::time::timeout(Duration::from_secs(2), peer_recv.recv())
                .await?
                .expect("discovery source sends exchange frames");
            match decode_discovery_frame(&frame)? {
                DiscoveryMessage::Hello { .. } => saw_hello = true,
                DiscoveryMessage::GetPeers { .. } => saw_get_peers = true,
                DiscoveryMessage::GetServices(_) => saw_get_services = true,
                DiscoveryMessage::Peers { .. } | DiscoveryMessage::Services(_) => {}
            }
        }

        peer_send
            .send(discovery_frame(DiscoveryMessage::Hello {
                record: signed_discovery_record(peer_secret, handshake)?,
            })?)
            .await?;
        peer_send
            .send(discovery_frame(DiscoveryMessage::Peers {
                records: Vec::new(),
            })?)
            .await?;
        let summary = DiscoveryServiceSummary {
            peer_exchange_slots_free: 1,
            max_records_per_response: 1,
            expected_disconnect_after_exchange: true,
        };
        peer_send
            .send(discovery_frame(DiscoveryMessage::Services(Services {
                node_id: peer_secret.public(),
                expires_at_unix_secs: u64::MAX,
                summaries: vec![ServiceSummaryEnvelope::discovery(&summary)?],
            }))?)
            .await?;

        Ok(())
    }

    async fn wait_for_discovery_inbound_peers(handle: &ZakuraDiscoveryHandle, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if handle.peer_snapshot().inbound_peers == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("discovery peer snapshot reaches expected inbound count");
    }

    /// Build a discovery handle with a configurable inbound peer cap for the
    /// routine cleanup/admission tests.
    fn discovery_handle_with_inbound_cap(
        local_seed: u8,
        max_inbound_peers: usize,
        connected_rx: watch::Receiver<Vec<ZakuraPeerId>>,
    ) -> Result<ZakuraDiscoveryHandle, crate::BoxError> {
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        Ok(ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: SecretKey::from_bytes(&[local_seed; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig {
                peer_limits: ServicePeerLimits {
                    max_inbound_peers,
                    ..ServicePeerLimits::default()
                },
                ..ZakuraDiscoveryConfig::default()
            },
            connected_rx,
        )?)
    }

    /// Drive the peer side of an exchange far enough to confirm the routine
    /// admitted and started sending, by reading its first `Hello`.
    async fn wait_for_routine_startup_hello(
        peer_recv: &mut FramedRecv,
    ) -> Result<(), crate::BoxError> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = peer_recv
                    .recv()
                    .await
                    .expect("discovery routine sends startup frames");
                if matches!(
                    decode_discovery_frame(&frame).expect("startup frame decodes"),
                    DiscoveryMessage::Hello { .. }
                ) {
                    return;
                }
            }
        })
        .await
        .map_err(|_| "discovery routine never sent its startup Hello".into())
    }

    async fn advisory_backoff_after_empty_headers(
        fixture: &mut HeaderAdvisoryFixture,
    ) -> Result<bool, crate::BoxError> {
        let (send, _recv) = framed_channel(32);
        let session = HeaderSyncPeerSession::from_parts_with_direction(
            fixture.peer_id.clone(),
            ServicePeerDirection::Inbound,
            send,
            CancellationToken::new(),
        );
        fixture
            .header_sync
            .send(HeaderSyncEvent::PeerConnected(session))
            .await?;
        fixture
            .header_sync
            .send(HeaderSyncEvent::PeerStatusUpdated {
                peer: fixture.peer_id.clone(),
                status: HeaderSyncStatus {
                    tip_height: block::Height(1),
                    tip_hash: block::Hash([9; 32]),
                    anchor_height: block::Height(0),
                    max_headers_per_response: 1,
                    max_inflight_requests: 1,
                },
            })
            .await?;

        // Routine-pulled work: the per-peer routine (not the reactor) now sends
        // `GetHeaders` and records the expectation. This advisory-backoff test has no
        // routine, so emit the `PeerWorkAssigned` a routine would have emitted after
        // it pulled and sent a request, then deliver the empty response against that
        // outstanding range — exercising the same advisory-unconfirmed path.
        fixture
            .header_sync
            .send(HeaderSyncEvent::PeerWorkAssigned {
                peer: fixture.peer_id.clone(),
                generation: 0,
                start_height: block::Height(1),
                count: 1,
                anchor_hash: block::Hash([9; 32]),
                finalized: false,
                forward: true,
            })
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;

        fixture
            .header_sync
            .send(HeaderSyncEvent::PeerHeadersReceived {
                peer: fixture.peer_id.clone(),
                headers: Vec::new(),
                body_sizes: Vec::new(),
            })
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;

        Ok(fixture
            .header_sync
            .candidate_state()
            .backed_off_node_ids
            .contains(&fixture.peer_node_id))
    }

    #[tokio::test]
    async fn get_services_returns_local_first_party_discovery_summary(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = SecretKey::from_bytes(&[21u8; 32]);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret.clone(),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig {
                peer_limits: ServicePeerLimits {
                    max_inbound_peers: 4,
                    ..ServicePeerLimits::default()
                },
                ..ZakuraDiscoveryConfig::default()
            },
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_node_id = SecretKey::from_bytes(&[22u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        let (peer_send, service_recv) = framed_channel(8);
        let (service_send, mut peer_recv) = framed_channel(8);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            CancellationToken::new(),
        ));

        peer_send
            .send(Frame {
                message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
                flags: 0,
                payload: DiscoveryMessage::GetServices(GetServices {
                    wanted_services: vec![ZakuraServiceId::discovery()],
                })
                .encode()?,
            })
            .await?;

        let services = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = peer_recv.recv().await.expect("discovery stream stays open");
                let message = decode_discovery_frame(&frame).expect("outbound frame decodes");
                if let DiscoveryMessage::Services(services) = message {
                    return services;
                }
            }
        })
        .await
        .expect("service response is sent");

        assert_eq!(services.node_id, local_secret.public());
        assert_eq!(services.summaries.len(), 1);
        assert_eq!(
            services.summaries[0].service_id,
            ZakuraServiceId::discovery()
        );
        let summary = services.summaries[0]
            .decode_discovery()?
            .expect("discovery summary tag decodes");
        assert_eq!(summary.peer_exchange_slots_free, 3);
        assert!(summary.expected_disconnect_after_exchange);
        assert_eq!(
            summary.max_records_per_response,
            u16::try_from(MAX_DISCOVERY_RECORDS_PER_RESPONSE)
                .expect("record response cap fits in u16")
        );

        Ok(())
    }

    #[tokio::test]
    async fn get_services_returns_local_first_party_block_sync_summary(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = SecretKey::from_bytes(&[24u8; 32]);
        let discovery_handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret.clone(),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery(), ZakuraServiceId::block_sync()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let (header_sync, _header_actions, header_task) = spawn_test_header_sync()?;
        let (tip_tx, tip_rx) = watch::channel((block::Height(5), block::Hash([5; 32])));
        drop(tip_tx);
        let (block_sync, _block_actions, block_task) =
            spawn_block_sync_reactor(BlockSyncStartup::new(
                BlockSyncFrontiers {
                    finalized_height: block::Height(0),
                    verified_block_tip: block::Height(5),
                    verified_block_hash: block::Hash([5; 32]),
                },
                (block::Height(5), block::Hash([5; 32])),
                tip_rx,
                ZakuraBlockSyncConfig::default(),
            ));
        let service = DiscoveryService::with_sync_services(
            discovery_handle,
            header_sync,
            Some(block_sync.clone()),
        );
        let peer_node_id = SecretKey::from_bytes(&[25u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        let (peer_send, service_recv) = framed_channel(8);
        let (service_send, mut peer_recv) = framed_channel(8);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_BLOCK_SYNC,
            streams,
            CancellationToken::new(),
        ));

        peer_send
            .send(Frame {
                message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
                flags: 0,
                payload: DiscoveryMessage::GetServices(GetServices {
                    wanted_services: vec![ZakuraServiceId::block_sync()],
                })
                .encode()?,
            })
            .await?;

        let services = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = peer_recv.recv().await.expect("discovery stream stays open");
                let message = decode_discovery_frame(&frame).expect("outbound frame decodes");
                if let DiscoveryMessage::Services(services) = message {
                    return services;
                }
            }
        })
        .await
        .expect("service response is sent");

        assert_eq!(services.node_id, local_secret.public());
        assert_eq!(services.summaries.len(), 1);
        assert_eq!(
            services.summaries[0].service_id,
            ZakuraServiceId::block_sync()
        );
        let summary = services.summaries[0]
            .decode_block_sync()?
            .expect("block summary tag decodes");
        assert_eq!(summary.servable_low, block::Height(0));
        assert_eq!(summary.servable_high, block::Height(5));
        assert_eq!(summary.tip_hash, block::Hash([5; 32]));
        assert_eq!(
            usize::from(summary.free_slots),
            block_sync.peer_snapshot().inbound_slots_free
        );
        assert_eq!(
            summary.max_blocks_per_response,
            ZakuraBlockSyncConfig::default().advertised_max_blocks_per_response()
        );
        assert_eq!(summary.max_response_bytes, MAX_BS_RESPONSE_BYTES);

        header_task.abort();
        block_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn inbound_services_updates_first_party_live_summary_cache() -> Result<(), crate::BoxError>
    {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = SecretKey::from_bytes(&[23u8; 32]);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_node_id = SecretKey::from_bytes(&[24u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let (peer_send, service_recv) = framed_channel(8);
        let (service_send, _peer_recv) = framed_channel(8);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            CancellationToken::new(),
        ));

        let summary = DiscoveryServiceSummary {
            peer_exchange_slots_free: 7,
            max_records_per_response: 11,
            expected_disconnect_after_exchange: false,
        };
        peer_send
            .send(Frame {
                message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
                flags: 0,
                payload: DiscoveryMessage::Services(Services {
                    node_id: peer_node_id,
                    expires_at_unix_secs: u64::MAX,
                    summaries: vec![ServiceSummaryEnvelope::discovery(&summary)?],
                })
                .encode()?,
            })
            .await?;

        let cached = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(cached) = handle.live_service_summaries(peer_node_id).await {
                    if !cached.is_empty() {
                        return cached;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("inbound SERVICES is imported");

        assert_eq!(cached.len(), 1);
        assert_eq!(
            cached[0].summary,
            ZakuraLiveServiceSummary::Discovery(summary)
        );

        Ok(())
    }

    #[tokio::test]
    async fn first_party_header_services_emit_header_sync_advisory() -> Result<(), crate::BoxError>
    {
        let mut fixture = spawn_header_advisory_fixture(25)?;
        let summary = header_summary(block::Height(10));

        send_discovery_message(
            &fixture,
            DiscoveryMessage::Services(Services {
                node_id: fixture.peer_node_id,
                expires_at_unix_secs: u64::MAX,
                summaries: vec![ServiceSummaryEnvelope::header_sync(&summary)?],
            }),
        )
        .await?;

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(cached) = fixture
                    .discovery_handle
                    .live_service_summaries(fixture.peer_node_id)
                    .await
                {
                    if cached.iter().any(|cached_summary| {
                        cached_summary.summary == ZakuraLiveServiceSummary::HeaderSync(summary)
                    }) {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first-party header summary is cached");

        assert!(
            advisory_backoff_after_empty_headers(&mut fixture).await?,
            "first-party header SERVICES should emit a header-sync advisory event"
        );

        Ok(())
    }

    #[tokio::test]
    async fn mismatched_services_node_id_does_not_emit_header_sync_advisory(
    ) -> Result<(), crate::BoxError> {
        let mut fixture = spawn_header_advisory_fixture(26)?;
        let claimed_node_id = SecretKey::from_bytes(&[27u8; 32]).public();
        let summary = header_summary(block::Height(10));

        send_discovery_message(
            &fixture,
            DiscoveryMessage::Services(Services {
                node_id: claimed_node_id,
                expires_at_unix_secs: u64::MAX,
                summaries: vec![ServiceSummaryEnvelope::header_sync(&summary)?],
            }),
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(
            fixture
                .discovery_handle
                .live_service_summaries(fixture.peer_node_id)
                .await,
            None
        );
        assert_eq!(
            fixture
                .discovery_handle
                .live_service_summaries(claimed_node_id)
                .await,
            None
        );
        assert!(
            !advisory_backoff_after_empty_headers(&mut fixture).await?,
            "mismatched SERVICES node id must not emit a header-sync advisory event"
        );

        Ok(())
    }

    #[tokio::test]
    async fn peers_response_does_not_emit_header_sync_advisory() -> Result<(), crate::BoxError> {
        let mut fixture = spawn_header_advisory_fixture(28)?;
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let record_secret = SecretKey::from_bytes(&[29u8; 32]);
        let record = signed_header_sync_record(&record_secret, &handshake)?;

        send_discovery_message(
            &fixture,
            DiscoveryMessage::Peers {
                records: vec![record.clone()],
            },
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(
            fixture
                .discovery_handle
                .live_service_summaries(record.body.node_id)
                .await,
            None
        );
        assert!(
            !advisory_backoff_after_empty_headers(&mut fixture).await?,
            "PEERS/gossiped records must not emit live header-sync advisory events"
        );

        Ok(())
    }

    #[tokio::test]
    async fn discovery_only_short_lived_exchange_closes_connection_and_backs_off(
    ) -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = SecretKey::from_bytes(&[40u8; 32]);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_secret = SecretKey::from_bytes(&[41u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&handle, 1).await;
        complete_peer_side_discovery_exchange(&peer_send, &mut peer_recv, &peer_secret, &handshake)
            .await?;
        tokio::time::timeout(Duration::from_secs(2), connection_cancel.cancelled())
            .await
            .expect("discovery-only exchange closes the shared connection");
        wait_for_discovery_inbound_peers(&handle, 0).await;

        connected_tx.send_replace(Vec::new());
        assert!(handle
            .dial_candidates(&[ZakuraServiceId::discovery()], &[])
            .await
            .is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn discovery_short_lived_exchange_keeps_header_sync_connection(
    ) -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = SecretKey::from_bytes(&[42u8; 32]);
        let discovery_handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let (header_sync, _header_actions, header_task) = spawn_test_header_sync()?;
        let service = DiscoveryService::with_sync_services(
            discovery_handle.clone(),
            header_sync.clone(),
            None,
        );
        let peer_secret = SecretKey::from_bytes(&[43u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let (header_send, _header_recv) = framed_channel(8);
        let header_session = HeaderSyncPeerSession::from_parts_with_direction(
            peer_id.clone(),
            ServicePeerDirection::Inbound,
            header_send,
            CancellationToken::new(),
        );
        header_sync
            .send(HeaderSyncEvent::PeerConnected(header_session))
            .await?;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if header_sync
                    .candidate_state()
                    .admitted_node_ids
                    .contains(&peer_node_id)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("header sync admits the peer");

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_HEADER_SYNC,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&discovery_handle, 1).await;
        complete_peer_side_discovery_exchange(&peer_send, &mut peer_recv, &peer_secret, &handshake)
            .await?;
        wait_for_discovery_inbound_peers(&discovery_handle, 0).await;
        assert_eq!(header_sync.peer_snapshot().inbound_peers, 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), connection_cancel.cancelled())
                .await
                .is_err(),
            "discovery releases only its own session while header sync owns the connection"
        );

        header_task.abort();
        Ok(())
    }

    /// Cancelling the connection token (a peer disconnect / local shutdown) makes
    /// the routine exit and its `Drop` guard remove admitted discovery peer state.
    #[tokio::test]
    async fn routine_exits_cleanly_on_service_cancellation() -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_inbound_cap(50, 4, connected_rx)?;
        let service = DiscoveryService::new(handle.clone());
        let peer_node_id = SecretKey::from_bytes(&[51u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        // Keep the peer-side send half alive so the stream does not close on its
        // own; the routine must exit because of the cancellation, not stream end.
        let (_peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&handle, 1).await;
        // The routine is mid-exchange (it sent its startup Hello and is waiting on
        // inbound frames). Cancelling the connection must drive it out.
        wait_for_routine_startup_hello(&mut peer_recv).await?;

        connection_cancel.cancel();
        wait_for_discovery_inbound_peers(&handle, 0).await;
        Ok(())
    }

    /// Closing the peer's send half (stream close) makes the routine exit cleanly
    /// and remove admitted discovery peer state.
    #[tokio::test]
    async fn routine_exits_cleanly_on_stream_close() -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_inbound_cap(52, 4, connected_rx)?;
        let service = DiscoveryService::new(handle.clone());
        let peer_node_id = SecretKey::from_bytes(&[53u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&handle, 1).await;
        wait_for_routine_startup_hello(&mut peer_recv).await?;

        // Closing the inbound stream (peer gone) ends `recv.recv()` with `None`,
        // so the routine exits and its `Drop`/inline cleanup removes admitted
        // discovery peer state. This is the clean-exit invariant under test; the
        // discovery-only disconnect decision is covered separately by
        // `discovery_only_short_lived_exchange_closes_connection_and_backs_off`.
        drop(peer_send);
        wait_for_discovery_inbound_peers(&handle, 0).await;
        Ok(())
    }

    /// When discovery admission rejects (its inbound cap is full), the routine
    /// parks its own service session and does not admit/leak any new peer state.
    #[tokio::test]
    async fn admission_reject_parks_service_without_leaking_peer_state(
    ) -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        // Cap inbound discovery at one peer, then occupy that slot directly.
        let handle = discovery_handle_with_inbound_cap(54, 1, connected_rx)?;
        let occupying_node_id = SecretKey::from_bytes(&[55u8; 32]).public();
        let occupying_peer = ZakuraPeerId::new(occupying_node_id.as_bytes().to_vec())?;
        assert_eq!(
            handle
                .admit_peer(occupying_peer, ServicePeerDirection::Inbound)
                .await,
            ServiceAdmissionDecision::Admit
        );
        assert_eq!(handle.peer_snapshot().inbound_peers, 1);

        let service = DiscoveryService::new(handle.clone());
        let rejected_node_id = SecretKey::from_bytes(&[56u8; 32]).public();
        let rejected_peer = ZakuraPeerId::new(rejected_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![rejected_peer.clone()]);

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, _peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
        let peer = Peer::new(
            rejected_peer,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        );
        let service_cancel = peer.service_cancel_token();
        service.add_peer(peer);

        // The parked routine cancels only its own service session; the occupying
        // peer's admitted state is untouched and the rejected peer never admits.
        tokio::time::timeout(Duration::from_secs(2), service_cancel.cancelled())
            .await
            .expect("rejected discovery session parks its own service token");
        assert_eq!(
            handle.peer_snapshot().inbound_peers,
            1,
            "admission reject must not admit or leak the rejected peer's state"
        );
        // Parking the service must not tear down the shared connection.
        assert!(
            !connection_cancel.is_cancelled(),
            "a parked (locally rejected) discovery session does not cancel the connection"
        );
        // The send side stays usable (nothing was queued on the parked session).
        drop(peer_send);
        Ok(())
    }

    /// A panic after admission unwinds through the routine's `Drop` guard, which
    /// removes admitted discovery peer state, and the supervised pipe's `on_panic`
    /// hook cancels the connection. This builds the routine directly (the same
    /// inputs `add_peer` constructs) and panics it inside the supervised pipe so
    /// the contained panic is observable.
    #[tokio::test]
    async fn panic_after_admission_removes_peer_state_and_disconnects(
    ) -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_inbound_cap(57, 4, connected_rx)?;
        let peer_node_id = SecretKey::from_bytes(&[58u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        // Admit directly (the same call the routine makes) so the test owns the
        // routine struct it then panics with.
        assert_eq!(
            handle
                .admit_peer(peer_id.clone(), ServicePeerDirection::Inbound)
                .await,
            ServiceAdmissionDecision::Admit
        );
        assert_eq!(handle.peer_snapshot().inbound_peers, 1);

        let (_peer_send, service_recv) = framed_channel(16);
        let (service_send, _peer_recv) = framed_channel(16);
        let session_inner = PeerStreamSession::new(
            peer_id.clone(),
            ZAKURA_STREAM_DISCOVERY,
            service_recv,
            service_send,
            CancellationToken::new(),
        );
        let session = DiscoveryPeerSession::new(&session_inner, ServicePeerDirection::Inbound);
        let (_p, _k, recv, _s, _c) = session_inner.into_parts();

        let connection_cancel = CancellationToken::new();
        let routine = DiscoveryPeerRoutine {
            handle: handle.clone(),
            header_sync: None,
            block_sync: None,
            peer_node_id,
            session,
            recv,
            connection_cancel: connection_cancel.clone(),
            other_service_negotiated: false,
            received_hello: false,
            received_peers: false,
            received_services: false,
            admitted: true,
        };

        let panic_connection_cancel = connection_cancel.clone();
        let on_panic = move || panic_connection_cancel.cancel();
        let handle_task = spawn_supervised_routine(
            peer_id.clone(),
            CancellationToken::new(),
            || {},
            on_panic,
            async move {
                // Hold the admitted routine, then panic: its `Drop` runs during the
                // unwind and schedules `remove_peer`, and `on_panic` cancels the
                // connection.
                let _routine = routine;
                panic!("discovery routine panics after admission");
            },
        );

        let join_error = handle_task
            .await
            .expect_err("a panicking discovery routine surfaces a join error");
        assert!(
            join_error.is_panic(),
            "the routine panic is reported as a panic, not a cancellation"
        );

        // The `Drop` guard scheduled the async `remove_peer`; wait for it to land.
        wait_for_discovery_inbound_peers(&handle, 0).await;
        assert!(
            connection_cancel.is_cancelled(),
            "a panic after admission cancels the connection"
        );
        Ok(())
    }

    /// A `Hello` authored by a node id other than the connected peer's is a
    /// protocol error: the routine returns `SinkReject::Protocol`, which the
    /// supervised pipe turns into a connection cancellation.
    #[tokio::test]
    async fn wrong_author_hello_disconnects_connection() -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = discovery_handle_with_inbound_cap(59, 4, connected_rx)?;
        let service = DiscoveryService::new(handle.clone());
        let peer_node_id = SecretKey::from_bytes(&[60u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&handle, 1).await;
        wait_for_routine_startup_hello(&mut peer_recv).await?;

        // A `Hello` whose record is signed by a *different* node id than the
        // connected peer is a protocol violation.
        let impostor_secret = SecretKey::from_bytes(&[61u8; 32]);
        peer_send
            .send(discovery_frame(DiscoveryMessage::Hello {
                record: signed_discovery_record(&impostor_secret, &handshake)?,
            })?)
            .await?;

        tokio::time::timeout(Duration::from_secs(2), connection_cancel.cancelled())
            .await
            .expect("wrong-author Hello disconnects the connection");
        wait_for_discovery_inbound_peers(&handle, 0).await;
        Ok(())
    }

    /// A `Services` payload authored by a node id other than the connected peer's
    /// is a protocol error that disconnects the connection.
    #[tokio::test]
    async fn wrong_author_services_disconnects_connection() -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_inbound_cap(62, 4, connected_rx)?;
        let service = DiscoveryService::new(handle.clone());
        let peer_node_id = SecretKey::from_bytes(&[63u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&handle, 1).await;
        wait_for_routine_startup_hello(&mut peer_recv).await?;

        // `Services.node_id` claims an identity other than the connected peer.
        let impostor_node_id = SecretKey::from_bytes(&[64u8; 32]).public();
        let summary = DiscoveryServiceSummary {
            peer_exchange_slots_free: 1,
            max_records_per_response: 1,
            expected_disconnect_after_exchange: true,
        };
        peer_send
            .send(discovery_frame(DiscoveryMessage::Services(Services {
                node_id: impostor_node_id,
                expires_at_unix_secs: u64::MAX,
                summaries: vec![ServiceSummaryEnvelope::discovery(&summary)?],
            }))?)
            .await?;

        tokio::time::timeout(Duration::from_secs(2), connection_cancel.cancelled())
            .await
            .expect("wrong-author Services disconnects the connection");
        wait_for_discovery_inbound_peers(&handle, 0).await;
        Ok(())
    }

    /// An inbound `GetPeers` is answered with a `Peers` response whose record
    /// count is bounded by the requested limit (caps preserved through the
    /// routine).
    #[tokio::test]
    async fn get_peers_response_is_bounded_by_request_limit() -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = discovery_handle_with_inbound_cap(65, 4, connected_rx)?;
        let service = DiscoveryService::new(handle.clone());
        let peer_node_id = SecretKey::from_bytes(&[66u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        // Seed the address book with a couple of dialable records so a sample is
        // possible; the response must still be bounded by the request limit.
        for seed in [70u8, 71u8] {
            let record = signed_discovery_record(&SecretKey::from_bytes(&[seed; 32]), &handshake)?;
            handle.import_peer_records(vec![record], None).await;
        }

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&handle, 1).await;
        wait_for_routine_startup_hello(&mut peer_recv).await?;

        peer_send
            .send(discovery_frame(DiscoveryMessage::GetPeers {
                limit: 1,
                wanted_services: Vec::new(),
                exclude_node_ids: Vec::new(),
            })?)
            .await?;

        let peers = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = peer_recv.recv().await.expect("discovery stream stays open");
                if let DiscoveryMessage::Peers { records } =
                    decode_discovery_frame(&frame).expect("outbound frame decodes")
                {
                    return records;
                }
            }
        })
        .await
        .expect("GetPeers is answered with a Peers response");

        assert!(
            peers.len() <= 1,
            "GetPeers response honours the requested limit of 1, got {}",
            peers.len()
        );
        Ok(())
    }
}
