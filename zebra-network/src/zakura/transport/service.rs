//! Zakura protocol service trait surface.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    net::IpAddr,
    pin::Pin,
    sync::{Arc, Mutex},
};

use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{FramedRecv, FramedSend};
use crate::{
    zakura::{ServicePeerDirection, ZakuraPeerId},
    BoxError,
};

use super::Frame;

/// Boxed future returned by object-safe stream handlers.
pub type BoxRunFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Bounded connection close-cause labels written to conn.jsonl.
pub(crate) mod cause {
    pub const CANCELLED: &str = "cancelled";
    pub const SHUTDOWN: &str = "shutdown";
    pub const IDLE_TIMEOUT: &str = "idle_timeout";
    pub const ACCEPT_FAILED: &str = "accept_failed";
    pub const OUTBOUND_CLOSED: &str = "outbound_closed";
    pub const BAD_RESPONSE: &str = "bad_response";

    pub const MANUAL_DISCONNECT: &str = "manual_disconnect";
    pub const DUPLICATE_EVICTED: &str = "duplicate_evicted";
    pub const RESOURCE_STREAMS: &str = "resource_streams";
    pub const RESOURCE_QUEUE: &str = "resource_queue";
    pub const SETUP_FAILED: &str = "setup_failed";

    pub const UNEXPECTED_STREAM: &str = "unexpected_stream";
    pub const DUPLICATE_STREAM: &str = "duplicate_stream";
    pub const UNEXPECTED_REQ_ID: &str = "unexpected_request_id";
    pub const REQUEST_WITHOUT_ID: &str = "request_without_id";

    pub const FRAME_TIMEOUT: &str = "frame_timeout";
    pub const FRAME_OVERSIZE: &str = "frame_oversize";
    pub const MSG_OVERSIZE: &str = "msg_oversize";
    pub const MSG_RATE_LIMITED: &str = "msg_rate_limited";
    pub const STREAM_PROTOCOL: &str = "stream_protocol_error";
    pub const WRITE_FAILED: &str = "write_failed";

    pub const REQUEST_TIMEOUT: &str = "request_timeout";
    pub const REQUEST_INVALID: &str = "request_invalid";
    pub const REQUEST_OVERSIZE: &str = "request_oversize";
    pub const REQUEST_REJECTED: &str = "request_rejected";

    pub const HEADER_REJECT: &str = "header_reject";
    pub const BLOCK_REJECT: &str = "block_reject";
    pub const DISCOVERY_REJECT: &str = "discovery_reject";
    pub const GOSSIP_REJECT: &str = "gossip_reject";
    pub const PEER_PANIC: &str = "peer_panic";
}

/// Shared per-connection close handle with a bounded first-cause label.
#[derive(Clone, Debug)]
pub(crate) struct Close {
    token: CancellationToken,
    cause: Arc<Mutex<Option<&'static str>>>,
}

impl Close {
    /// Wrap an existing connection cancellation token.
    pub(crate) fn new(token: CancellationToken) -> Self {
        Self {
            token,
            cause: Arc::new(Mutex::new(None)),
        }
    }

    /// Close the connection, preserving the first specific cause.
    pub(crate) fn close(&self, cause: &'static str) {
        {
            let mut stored = self
                .cause
                .lock()
                .expect("Zakura connection close cause mutex is never poisoned");
            if stored.is_none() {
                *stored = Some(cause);
            }
        }
        self.token.cancel();
    }

    /// Return the underlying token for wait-only code.
    pub(crate) fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Return a child token for stream-local work.
    pub(crate) fn child(&self) -> CancellationToken {
        self.token.child_token()
    }

    /// Returns whether the underlying token is cancelled.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Return the first recorded cause, or `default` when only the raw token fired.
    pub(crate) fn cause_or(&self, default: &'static str) -> &'static str {
        self.cause
            .lock()
            .expect("Zakura connection close cause mutex is never poisoned")
            .unwrap_or(default)
    }
}

/// Transport mode for a service-declared stream.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StreamMode {
    /// A long-lived ordered stream between connected peers.
    Ordered,
    /// A short-lived request/response stream opened per request.
    RequestResponse,
}

/// A service-declared Zakura stream.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Stream {
    /// Unique stream kind carried in `StreamPrelude.stream_kind`.
    pub kind: u16,
    /// Version of this stream kind.
    pub version: u16,
    /// Maximum application frame bytes for this stream.
    pub frame_cap: u32,
    /// Capability bit both peers must negotiate before this stream is wired.
    pub capability: u64,
    /// Stream lifetime and opening semantics.
    pub mode: StreamMode,
}

/// Transport state for one ordered service stream.
#[derive(Debug)]
pub(crate) struct ServiceStream {
    pub(crate) recv: FramedRecv,
    pub(crate) send: FramedSend,
    pub(crate) cancel_token: CancellationToken,
}

impl ServiceStream {
    pub(crate) fn new(recv: FramedRecv, send: FramedSend, cancel_token: CancellationToken) -> Self {
        Self {
            recv,
            send,
            cancel_token,
        }
    }
}

/// Per-peer transport state handed to a service when a peer connects.
#[derive(Debug)]
pub struct Peer {
    /// Authenticated Zakura peer identity.
    pub id: ZakuraPeerId,
    /// Remote IP address when the transport knows it.
    pub remote_ip: Option<IpAddr>,
    /// Capabilities accepted by both peers.
    pub negotiated: u64,
    /// Direction of the underlying authenticated connection.
    pub direction: ServicePeerDirection,
    streams: HashMap<u16, ServiceStream>,
    cancel_token: CancellationToken,
    close: Close,
    service_cancel_token: CancellationToken,
}

impl Peer {
    /// Build a peer from already-opened transport streams.
    pub fn new(
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        streams: HashMap<u16, (FramedRecv, FramedSend)>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::new_with_direction(
            id,
            remote_ip,
            negotiated,
            ServicePeerDirection::Inbound,
            streams,
            cancel_token,
        )
    }

    /// Build a peer from already-opened transport streams and a known direction.
    pub fn new_with_direction(
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        direction: ServicePeerDirection,
        streams: HashMap<u16, (FramedRecv, FramedSend)>,
        cancel_token: CancellationToken,
    ) -> Self {
        let streams = streams
            .into_iter()
            .map(|(kind, (recv, send))| {
                (
                    kind,
                    ServiceStream::new(recv, send, cancel_token.child_token()),
                )
            })
            .collect::<HashMap<_, _>>();
        Self::new_with_service_streams(id, remote_ip, negotiated, direction, streams, cancel_token)
    }

    pub(crate) fn new_with_service_streams(
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        direction: ServicePeerDirection,
        streams: HashMap<u16, ServiceStream>,
        cancel_token: CancellationToken,
    ) -> Self {
        let close = Close::new(cancel_token);
        Self::new_with_close(id, remote_ip, negotiated, direction, streams, close)
    }

    pub(crate) fn new_with_close(
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        direction: ServicePeerDirection,
        streams: HashMap<u16, ServiceStream>,
        close: Close,
    ) -> Self {
        let service_cancel_token = streams
            .values()
            .next()
            .map(|stream| stream.cancel_token.clone())
            .unwrap_or_else(|| close.child());
        Self::new_with_close_service_token(
            id,
            remote_ip,
            negotiated,
            direction,
            streams,
            close,
            service_cancel_token,
        )
    }

    pub(crate) fn new_with_close_service_token(
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        direction: ServicePeerDirection,
        streams: HashMap<u16, ServiceStream>,
        close: Close,
        service_cancel_token: CancellationToken,
    ) -> Self {
        let cancel_token = close.token();
        Self {
            id,
            remote_ip,
            negotiated,
            direction,
            streams,
            cancel_token,
            close,
            service_cancel_token,
        }
    }

    /// Take ownership of a stream pair for `kind`.
    pub fn take_stream(&mut self, kind: u16) -> Option<(FramedRecv, FramedSend)> {
        self.streams
            .remove(&kind)
            .map(|stream| (stream.recv, stream.send))
    }

    /// Return the cancellation token for this peer's service tasks.
    ///
    /// The token is the transport supervisor's per-peer disconnect token, so it
    /// fires when this peer disconnects or the local node shuts down.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Return the connection close-cause handle for fatal service paths.
    pub(crate) fn close(&self) -> Close {
        self.close.clone()
    }

    /// Return the cancellation token for this service's local session work.
    ///
    /// Cancelling this token parks only the local service session. The parent
    /// peer token still fires on connection shutdown.
    pub fn service_cancel_token(&self) -> CancellationToken {
        self.service_cancel_token.clone()
    }

    /// Split this peer into fields so the registry can fan streams out by owner.
    pub(crate) fn into_parts(
        self,
    ) -> (
        ZakuraPeerId,
        Option<IpAddr>,
        u64,
        ServicePeerDirection,
        HashMap<u16, ServiceStream>,
        CancellationToken,
        Close,
    ) {
        (
            self.id,
            self.remote_ip,
            self.negotiated,
            self.direction,
            self.streams,
            self.cancel_token,
            self.close,
        )
    }
}

/// A Zakura protocol service.
pub trait Service: fmt::Debug + Send + Sync + 'static {
    /// Stable service name for logs and diagnostics.
    fn name(&self) -> &'static str;

    /// Streams this service owns.
    fn streams(&self) -> &[Stream];

    /// Return whether this service currently wants a new session for `peer`.
    ///
    /// This is a cheap, advisory demand check used by the transport before
    /// opening an ordered stream. Implementations intentionally keep this to
    /// local room/interest state; remote first-party summary preference is
    /// applied upstream before dialing or escalation selection.
    ///
    /// The service remains authoritative at [`Service::add_peer`], where it
    /// can still reject or locally park a session if its state changed
    /// concurrently.
    fn wants_peer(
        &self,
        _peer: &ZakuraPeerId,
        _negotiated: u64,
        _direction: ServicePeerDirection,
    ) -> bool {
        true
    }

    /// Add a connected peer and spawn any per-stream work owned by this service.
    fn add_peer(&self, peer: Peer);

    /// Remove a disconnected peer.
    fn remove_peer(&self, peer: &ZakuraPeerId);

    /// Deliver one request-response frame to this service.
    fn deliver_frame(
        &self,
        _peer_id: ZakuraPeerId,
        _stream_kind: u16,
        _frame: Frame,
    ) -> Result<(), SinkReject> {
        Err(SinkReject::protocol(
            "service does not accept inbound frames",
        ))
    }

    /// Return this service's request/response handler, if it has one.
    fn as_request_response(&self) -> Option<&dyn RequestResponseService> {
        None
    }
}

/// A Zakura service that accepts one-shot request/response streams.
pub trait RequestResponseService: Service {
    /// Deliver one request-response request frame to this service.
    ///
    /// `max_frame_bytes` bounds each encoded outbound frame; `max_message_bytes`
    /// is the peer's negotiated full-message cap. Responders must size outbound
    /// frame payloads against the smaller of the two so the peer never receives
    /// a frame larger than its accepted message cap.
    fn request_frame<'a>(
        &'a self,
        peer_id: ZakuraPeerId,
        stream_kind: u16,
        request_id: u64,
        max_frame_bytes: u32,
        max_message_bytes: u32,
        frame: Frame,
    ) -> BoxRunFuture<'a, Result<Vec<Frame>, SinkReject>>;
}

/// A per-stream reader owned by a service.
///
/// This trait deliberately uses an explicit boxed future instead of native
/// `async fn` or the `async-trait` crate: stream handlers are intended to be
/// object-dispatched, and the explicit signature keeps that object safety without
/// adding another dependency.
pub trait Sink: Send + 'static {
    /// Run the reader until the stream closes or is rejected.
    fn run(self: Box<Self>, recv: FramedRecv) -> BoxRunFuture<'static, Result<(), SinkReject>>;
}

/// A per-stream writer owned by a service.
///
/// P2 services can either run this task shape directly or keep a concrete
/// typed send handle built from the [`FramedSend`] handed to [`Service::add_peer`].
///
/// See [`Sink`] for why this uses an explicit boxed future.
pub trait Source: Send + 'static {
    /// Run the writer until the stream closes.
    fn run(self: Box<Self>, send: FramedSend) -> BoxRunFuture<'static, ()>;
}

/// Reason a service sink rejected a decoded frame stream.
#[derive(Debug, Error)]
pub enum SinkReject {
    /// The peer sent protocol-invalid data, so the connection should close.
    #[error("inbound sink rejected protocol-invalid frame: {0}")]
    Protocol(#[source] BoxError),

    /// Local sink state prevented delivery; the peer is not at fault.
    #[error("inbound sink could not accept frame locally: {0}")]
    Local(#[source] BoxError),
}

impl SinkReject {
    /// Build a fatal peer-protocol rejection.
    pub fn protocol(error: impl Into<BoxError>) -> Self {
        Self::Protocol(error.into())
    }

    /// Build a non-fatal local-delivery rejection.
    pub fn local(error: impl Into<BoxError>) -> Self {
        Self::Local(error.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_reject_constructors_preserve_protocol_and_local_contract() {
        let protocol = SinkReject::protocol("bad frame");
        let local = SinkReject::local("closed queue");

        assert!(matches!(protocol, SinkReject::Protocol(_)));
        assert!(matches!(local, SinkReject::Local(_)));
        assert!(protocol.to_string().contains("protocol-invalid"));
        assert!(local.to_string().contains("locally"));
    }

    #[test]
    fn close_first_cause_wins() {
        let close = Close::new(CancellationToken::new());

        close.close(cause::FRAME_TIMEOUT);
        close.close(cause::SHUTDOWN);

        assert!(close.is_cancelled());
        assert_eq!(close.cause_or(cause::CANCELLED), cause::FRAME_TIMEOUT);
    }

    #[test]
    fn close_plain_token_cancel_falls_back() {
        let token = CancellationToken::new();
        let close = Close::new(token.clone());

        token.cancel();

        assert!(close.is_cancelled());
        assert_eq!(close.cause_or(cause::CANCELLED), cause::CANCELLED);
    }

    #[test]
    fn close_cleanup_does_not_overwrite_specific_cause() {
        let close = Close::new(CancellationToken::new());

        close.close(cause::MSG_OVERSIZE);
        close.close(cause::SHUTDOWN);

        assert_eq!(close.cause_or(cause::CANCELLED), cause::MSG_OVERSIZE);
    }
}
