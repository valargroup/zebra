//! Native discovery service (stream kind 4) on the Zakura transport.
//!
//! Discovery is a single long-lived ordered stream per peer. Each side runs a
//! [`DiscoverySink`] (the reader, which imports peer records and answers
//! `GetPeers`) and a [`DiscoverySource`] (the writer, which periodically gossips
//! the local self-record and asks for more peers). The wire format is the
//! [`DiscoveryMessage`] payload carried inside a generic transport [`Frame`]
//! (`message_type = DISCOVERY_FRAME_MESSAGE_TYPE`, `flags = 0`), identical to the
//! original native-discovery wire so peers interoperate.

use std::time::Duration;

use iroh::NodeId;
use tokio_util::sync::CancellationToken;

use crate::zakura::{
    BoxRunFuture, Frame, FramedRecv, FramedSend, OrderedSendError, Peer, PeerStreamSession,
    Service, Sink, SinkReject, Stream, StreamMode, ZakuraPeerId, LOCAL_MAX_CONTROL_FRAME_BYTES,
    ZAKURA_CAP_DISCOVERY,
};

use super::protocol::{
    DiscoveryBookError, DiscoveryMessage, DiscoveryRecordError, ZakuraDiscoveryHandle,
    ZakuraNodeRecord, ZakuraServiceId, MAX_DISCOVERY_RECORDS_PER_RESPONSE,
    ZAKURA_DISCOVERY_STREAM_VERSION, ZAKURA_STREAM_DISCOVERY,
};

/// Frame message type carrying a discovery payload (matches the native wire).
const DISCOVERY_FRAME_MESSAGE_TYPE: u16 = 1;

/// Minimum spacing between periodic discovery exchanges, regardless of config.
const MIN_DISCOVERY_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

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

/// Cloneable typed sender for one native discovery ordered stream.
#[derive(Clone, Debug)]
pub struct DiscoveryPeerSession {
    peer_id: ZakuraPeerId,
    send: FramedSend,
    cancel: CancellationToken,
}

impl DiscoveryPeerSession {
    fn new(session: &PeerStreamSession) -> Self {
        Self {
            peer_id: session.peer_id().clone(),
            send: session.sender(),
            cancel: session.cancel_token(),
        }
    }

    /// Authenticated peer identity for this discovery stream.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
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
}

impl DiscoveryService {
    /// Builds a discovery service driven by `handle`.
    pub fn new(handle: ZakuraDiscoveryHandle) -> Self {
        Self { handle }
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
            peer.cancel_token(),
        );
        let discovery_session = DiscoveryPeerSession::new(&session);
        let cancel = discovery_session.cancel_token();
        let (_peer_id, _stream_kind, recv, _send, _session_cancel) = session.into_parts();

        let sink = DiscoverySink {
            handle: self.handle.clone(),
            peer_node_id,
            session: discovery_session.clone(),
        };
        let sink_cancel = cancel.clone();
        tokio::spawn(async move {
            match Box::new(sink).run(recv).await {
                Ok(()) => {}
                Err(SinkReject::Protocol(error)) => {
                    tracing::debug!(
                        ?error,
                        "Zakura discovery stream rejected protocol-invalid frame"
                    );
                    sink_cancel.cancel();
                }
                Err(SinkReject::Local(error)) => {
                    tracing::debug!(?error, "Zakura discovery stream stopped on local error");
                }
            }
        });

        let source = DiscoverySource {
            handle: self.handle.clone(),
            session: discovery_session,
        };
        tokio::spawn(async move {
            source.run().await;
        });
    }

    fn remove_peer(&self, _peer: &ZakuraPeerId) {
        // The runtime tracks the connected set through the supervisor watch it
        // was constructed with; active-service queries cross-reference it, so a
        // disconnect needs no explicit bookkeeping here.
    }
}

/// Reader half of the discovery stream: imports peer records and answers queries.
struct DiscoverySink {
    handle: ZakuraDiscoveryHandle,
    peer_node_id: NodeId,
    session: DiscoveryPeerSession,
}

impl Sink for DiscoverySink {
    fn run(self: Box<Self>, mut recv: FramedRecv) -> BoxRunFuture<'static, Result<(), SinkReject>> {
        Box::pin(async move {
            while let Some(frame) = recv.recv().await {
                self.handle_frame(frame).await?;
            }
            Ok(())
        })
    }
}

impl DiscoverySink {
    async fn handle_frame(&self, frame: Frame) -> Result<(), SinkReject> {
        let message = decode_discovery_frame(&frame).map_err(SinkReject::protocol)?;
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
                Ok(())
            }
            DiscoveryMessage::GetServices { .. } | DiscoveryMessage::Services { .. } => {
                // Service discovery rides the self-record service list, not a
                // dedicated message exchange; an explicit service message is a
                // protocol violation.
                Err(SinkReject::protocol(
                    "Zakura discovery service messages are not supported",
                ))
            }
        }
    }

    async fn handle_hello(&self, record: ZakuraNodeRecord) -> Result<(), SinkReject> {
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
        }
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
}

/// Writer half of the discovery stream: periodic self-record gossip + peer asks.
struct DiscoverySource {
    handle: ZakuraDiscoveryHandle,
    session: DiscoveryPeerSession,
}

impl DiscoverySource {
    async fn run(self) {
        if self.exchange().await.is_err() {
            return;
        }
        let refresh = self
            .handle
            .refresh_interval()
            .await
            .max(MIN_DISCOVERY_REFRESH_INTERVAL);
        let cancel = self.session.cancel_token();
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(refresh) => {}
            }
            if self.exchange().await.is_err() {
                return;
            }
        }
    }

    /// Gossips the current self-record and asks the peer for more peers.
    ///
    /// Returns `Err(())` once the stream's send side is gone, so the caller
    /// stops the periodic loop.
    async fn exchange(&self) -> Result<(), ()> {
        let record = (*self.handle.current_self_record()).clone();
        self.handle_send_result(self.session.try_send_hello(record))?;

        let limit = self
            .handle
            .peer_sample_limit()
            .await
            .min(MAX_DISCOVERY_RECORDS_PER_RESPONSE);
        // `peer_sample_limit` is bounded by MAX_DISCOVERY_RECORDS_PER_RESPONSE
        // (<= u16::MAX), so the cast cannot truncate.
        let exclude_node_ids = self.handle.peer_sample_exclusions().await;
        self.handle_send_result(self.session.try_send_get_peers(
            limit as u16,
            Vec::new(),
            exclude_node_ids,
        ))
    }

    fn handle_send_result(&self, result: Result<(), OrderedSendError>) -> Result<(), ()> {
        match result {
            Ok(()) | Err(OrderedSendError::Full) => Ok(()),
            Err(OrderedSendError::Closed) => Err(()),
            Err(OrderedSendError::Encode(error)) => {
                tracing::debug!(
                    ?error,
                    peer = ?self.session.peer_id(),
                    "failed to encode Zakura discovery message"
                );
                Ok(())
            }
        }
    }
}

/// Decodes a discovery message from a transport frame, rejecting a frame whose
/// envelope is not a discovery payload.
fn decode_discovery_frame(frame: &Frame) -> Result<DiscoveryMessage, crate::BoxError> {
    if frame.message_type != DISCOVERY_FRAME_MESSAGE_TYPE || frame.flags != 0 {
        return Err(format!(
            "unexpected discovery frame envelope (message_type={}, flags={})",
            frame.message_type, frame.flags
        )
        .into());
    }
    DiscoveryMessage::decode(&frame.payload).map_err(Into::into)
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
