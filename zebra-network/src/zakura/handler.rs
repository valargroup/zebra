//! Zakura P2P v2 endpoint, protocol handler, and bounded connection serving.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    future::Future,
    io::{Cursor, Read},
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use iroh::Watcher as _;
use iroh::{
    endpoint::{Connection, RecvStream, SendStream, TransportConfig, VarInt},
    protocol::{AcceptError, ProtocolHandler, Router},
    NodeAddr, NodeId, SecretKey,
};
use rand::{rngs::OsRng, RngCore};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, watch, Mutex, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::{timeout, Instant},
};
use tokio_util::sync::CancellationToken;
use zebra_chain::{
    block::{Block, CountedHeader},
    serialization::{CompactSizeMessage, ZcashDeserialize, MAX_HEADERS_PER_MESSAGE},
    transaction::Transaction,
};

use super::{
    trace::{
        peer_label as trace_peer_label, reject_reason_label, ZakuraTrace, CONN_TABLE,
        HANDSHAKE_TABLE, RATELIMIT_TABLE, STREAM_TABLE,
    },
    ZakuraTraceEvent,
};
use crate::{
    protocol::external::InventoryHash,
    zakura::{
        direct_endpoint_builder, DiscoveryBookError, DiscoveryMessage, DiscoveryRecordError, Frame,
        StreamPrelude, ZakuraAcceptedLimits, ZakuraControlAck, ZakuraControlHello,
        ZakuraControlRole, ZakuraControlValidation, ZakuraDiscoveryConfig,
        ZakuraDiscoveryDialCandidate, ZakuraDiscoveryHandle, ZakuraDiscoveryLocalConfig,
        ZakuraHandshakeConfig, ZakuraHandshakePath, ZakuraInitialLimits, ZakuraLimits,
        ZakuraNodeRecord, ZakuraPeerId, ZakuraPeerSupervisor, ZakuraProtocolError,
        ZakuraRejectReason, ZakuraServiceId, ZakuraUpgradeOutcome, CONTROL_ACK_MAGIC,
        CONTROL_HELLO_MAGIC, CONTROL_VERSION, DEFAULT_DISCOVERY_CONNECTION_HEADROOM,
        FRAME_HEADER_BYTES, P2P_V2_ALPN, STREAM_PRELUDE_MAGIC, TRANSCRIPT_HASH_BYTES,
        ZAKURA_PROTOCOL_VERSION_1, ZAKURA_STREAM_DISCOVERY,
    },
};
use crate::{BoxError, Config, MAX_TX_INV_IN_SENT_MESSAGE};

/// Conservative default for total Zakura connections when P2P v2 is enabled.
pub const DEFAULT_ZAKURA_MAX_CONNECTIONS: usize = 32;
/// Conservative default for simultaneous Zakura control handshakes.
pub const DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES: usize = 8;
/// Conservative default for stream-open churn per connection.
pub const DEFAULT_ZAKURA_STREAM_OPEN_RATE_PER_SECOND: u32 = 16;
/// Conservative default for per-kind message rate per connection.
pub const DEFAULT_ZAKURA_MESSAGE_RATE_PER_SECOND: u32 = 128;
/// Default maximum bytes read before the peer's stream prelude is decoded.
pub const DEFAULT_ZAKURA_PRELUDE_TIMEOUT: Duration = Duration::from_secs(3);
/// Default timeout for one control-handshake read or write.
pub const DEFAULT_ZAKURA_CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
/// QUIC idle timeout used by Zakura endpoints.
///
/// This also bounds the application-level idle reaper that closes a connection
/// after a quiet period. A gossip connection is legitimately quiet between
/// blocks (minutes apart on mainnet), so a short timeout would tear down healthy
/// peers and force constant re-dials; the keepalive interval keeps the transport
/// live well within this window.
pub const DEFAULT_ZAKURA_QUIC_IDLE_TIMEOUT: Duration = Duration::from_secs(150);
/// QUIC keepalive interval used by Zakura endpoints.
pub const DEFAULT_ZAKURA_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// QUIC stream receive window used by Zakura endpoints.
pub const DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW: u32 = 512 * 1024;
/// QUIC connection receive window used by Zakura endpoints.
pub const DEFAULT_ZAKURA_RECEIVE_WINDOW: u32 = 2 * 1024 * 1024;
/// QUIC send window used by Zakura endpoints.
pub const DEFAULT_ZAKURA_SEND_WINDOW: u64 = 2 * 1024 * 1024;
/// Initial backoff before re-dialing a configured Zakura bootstrap peer.
pub const DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Maximum backoff between re-dials of a configured Zakura bootstrap peer.
pub const DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A connection that served at least this long is treated as healthy, so the
/// next re-dial after it drops starts from the initial (fast) backoff again
/// instead of penalising a long-lived peer for an eventual disconnect.
const ZAKURA_REDIAL_HEALTHY_CONNECTION: Duration = Duration::from_secs(60);
/// How many times the legacy->Zakura upgrade re-attempts its QUIC dial before
/// giving up and leaving longer-term recovery to the legacy crawler. Kept small
/// so the retry window stays within the liveness keeper's appear timeout
/// (`ZAKURA_LIVENESS_APPEAR_TIMEOUT` in the parent module).
const ZAKURA_UPGRADE_DIAL_ATTEMPTS: usize = 3;
/// Poll interval for discovery-backed candidate dialing.
const ZAKURA_DISCOVERY_DIAL_INTERVAL: Duration = Duration::from_secs(1);

/// Clock used by Zakura rate-limit logic.
pub trait Clock: Clone + Send + Sync + 'static {
    /// Return the current monotonic instant.
    fn now(&self) -> Instant;
}

/// Production clock backed by [`Instant::now`].
#[derive(Copy, Clone, Debug, Default)]
pub struct RealClock;

impl Clock for RealClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

const CONTROL_LENGTH_BYTES: usize = 4;
const STREAM_PRELUDE_FIXED_BYTES: usize = 4 + 2 + 2 + 1;
const STREAM_PRELUDE_REQUEST_ID_FLAG_OFFSET: usize = STREAM_PRELUDE_FIXED_BYTES - 1;
const STREAM_PRELUDE_REQUEST_ID_BYTES: usize = 8;
const STREAM_PRELUDE_CAP_BYTES: usize = 4;
const STREAM_WORKER_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const OUTBOUND_STREAM_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const OUTBOUND_REQUEST_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const DISCOVERY_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(10);
const DISCOVERY_FRAME_MESSAGE_TYPE: u16 = 1;
// Mirrors the legacy gossip compatibility protocol. Compile-time assertions
// below keep this transport-side budget validator pinned to the codec constants.
const LEGACY_GOSSIP_STREAM_KIND: u16 = 2;
const LEGACY_REQUEST_STREAM_KIND: u16 = 3;
const LEGACY_REQUEST_BLOCKS_BY_HASH: u16 = 3;
const LEGACY_REQUEST_TRANSACTIONS_BY_ID: u16 = 4;
const LEGACY_RESPONSE_BLOCK: u16 = 5;
const LEGACY_RESPONSE_TRANSACTION: u16 = 6;
const LEGACY_RESPONSE_MISSING_BLOCKS: u16 = 7;
const LEGACY_RESPONSE_MISSING_TRANSACTIONS: u16 = 8;
const LEGACY_REQUEST_FIND_BLOCKS: u16 = 9;
const LEGACY_REQUEST_FIND_HEADERS: u16 = 10;
const LEGACY_REQUEST_MEMPOOL_TRANSACTION_IDS: u16 = 11;
const LEGACY_REQUEST_PING: u16 = 12;
const LEGACY_REQUEST_PUSH_TRANSACTION: u16 = 13;
const LEGACY_RESPONSE_BLOCK_HASHES: u16 = 14;
const LEGACY_RESPONSE_BLOCK_HEADERS: u16 = 15;
const LEGACY_RESPONSE_TRANSACTION_IDS: u16 = 16;
const LEGACY_RESPONSE_PONG: u16 = 17;
const LEGACY_RESPONSE_NIL: u16 = 18;
const LEGACY_RESPONSE_REQUEST_ID_BYTES: usize = 8;
const LEGACY_RESPONSE_CHUNK_HEADER_BYTES: usize = LEGACY_RESPONSE_REQUEST_ID_BYTES + 1;
const LEGACY_COMPACT_SIZE_PREFIX_BYTES: usize = 9;
const LEGACY_BLOCK_HASH_BYTES: usize = 32;
const LEGACY_INVENTORY_HASH_BYTES: usize = 36;
const LEGACY_RESPONSE_MAX_FRAMES_PER_ITEM: usize = 8;
#[cfg(any(test, feature = "zakura-testkit"))]
/// Test-only native request stream for echo/status service routing coverage.
pub const ZAKURA_STREAM_TEST_ECHO_STATUS: u16 = 5;
#[cfg(any(test, feature = "zakura-testkit"))]
/// Test-only echo/status request message type.
pub const TEST_ECHO_STATUS_REQUEST: u16 = 1;
#[cfg(any(test, feature = "zakura-testkit"))]
/// Test-only echo/status response message type.
pub const TEST_ECHO_STATUS_RESPONSE: u16 = 2;
const _: () = assert!(LEGACY_GOSSIP_STREAM_KIND == super::legacy_gossip::ZAKURA_STREAM_GOSSIP);
const _: () =
    assert!(LEGACY_REQUEST_STREAM_KIND == super::legacy_gossip::ZAKURA_STREAM_LEGACY_REQUESTS);
const _: () =
    assert!(LEGACY_REQUEST_BLOCKS_BY_HASH == super::legacy_gossip::MSG_REQUEST_BLOCKS_BY_HASH);
const _: () = assert!(
    LEGACY_REQUEST_TRANSACTIONS_BY_ID == super::legacy_gossip::MSG_REQUEST_TRANSACTIONS_BY_ID
);
const _: () = assert!(LEGACY_RESPONSE_BLOCK == super::legacy_gossip::MSG_RESPONSE_BLOCK);
const _: () =
    assert!(LEGACY_RESPONSE_TRANSACTION == super::legacy_gossip::MSG_RESPONSE_TRANSACTION);
const _: () =
    assert!(LEGACY_RESPONSE_MISSING_BLOCKS == super::legacy_gossip::MSG_RESPONSE_MISSING_BLOCKS);
const _: () = assert!(
    LEGACY_RESPONSE_MISSING_TRANSACTIONS == super::legacy_gossip::MSG_RESPONSE_MISSING_TRANSACTIONS
);
const _: () = assert!(LEGACY_REQUEST_FIND_BLOCKS == super::legacy_gossip::MSG_REQUEST_FIND_BLOCKS);
const _: () =
    assert!(LEGACY_REQUEST_FIND_HEADERS == super::legacy_gossip::MSG_REQUEST_FIND_HEADERS);
const _: () = assert!(
    LEGACY_REQUEST_MEMPOOL_TRANSACTION_IDS
        == super::legacy_gossip::MSG_REQUEST_MEMPOOL_TRANSACTION_IDS
);
const _: () = assert!(LEGACY_REQUEST_PING == super::legacy_gossip::MSG_REQUEST_PING);
const _: () =
    assert!(LEGACY_REQUEST_PUSH_TRANSACTION == super::legacy_gossip::MSG_REQUEST_PUSH_TRANSACTION);
const _: () =
    assert!(LEGACY_RESPONSE_BLOCK_HASHES == super::legacy_gossip::MSG_RESPONSE_BLOCK_HASHES);
const _: () =
    assert!(LEGACY_RESPONSE_BLOCK_HEADERS == super::legacy_gossip::MSG_RESPONSE_BLOCK_HEADERS);
const _: () =
    assert!(LEGACY_RESPONSE_TRANSACTION_IDS == super::legacy_gossip::MSG_RESPONSE_TRANSACTION_IDS);
const _: () = assert!(LEGACY_RESPONSE_PONG == super::legacy_gossip::MSG_RESPONSE_PONG);
const _: () = assert!(LEGACY_RESPONSE_NIL == super::legacy_gossip::MSG_RESPONSE_NIL);
const ZAKURA_CLOSE_NEUTRAL: u32 = 0;
const ZAKURA_CLOSE_RESOURCE: u32 = 1;
const ZAKURA_CLOSE_BAD_PRELUDE: u32 = 2;
const ZAKURA_CLOSE_RATE_LIMIT: u32 = 3;
const ZAKURA_CLOSE_OVERSIZE: u32 = 4;
/// Stream reset code for a prelude naming an unknown stream kind or an
/// unsupported version of a known kind. Bounded, peer-visible, and distinct
/// from the malformed-prelude code so peers can tell a parse failure from an
/// unsupported-but-well-formed stream.
const ZAKURA_CLOSE_UNKNOWN_STREAM: u32 = 5;
const NATIVE_TRANSCRIPT_HASH: [u8; TRANSCRIPT_HASH_BYTES] = [0; TRANSCRIPT_HASH_BYTES];

/// Native Zakura endpoint and handler configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ZakuraConfig {
    /// Native Zakura bootstrap peers as `node_id@direct_addr`.
    ///
    /// Native bootstrap uses direct iroh addresses only because relays and discovery
    /// are disabled for Zakura v1. Each entry must contain the peer's 32-byte
    /// iroh node id and a directly reachable socket address.
    pub bootstrap_peers: Vec<String>,
    /// Address the native Zakura QUIC endpoint binds to.
    ///
    /// When unset the endpoint binds an OS-assigned ephemeral port on the
    /// unspecified address, which is fine for a node that only dials out. Set a
    /// fixed address to give this node a stable, advertisable Zakura endpoint so
    /// other nodes can list it in their [`bootstrap_peers`](Self::bootstrap_peers)
    /// — required for a node that acts as a Zakura seed, since relays and
    /// discovery are disabled.
    pub listen_addr: Option<SocketAddr>,
    /// Total concurrent Zakura connections, inbound plus outbound.
    pub max_connections: usize,
    /// Connections concurrently running the control handshake.
    pub max_pending_handshakes: usize,
    /// New streams per second admitted per connection after a valid prelude.
    pub stream_open_rate_per_second: u32,
    /// Messages per second admitted per stream kind on a connection.
    pub message_rate_per_second: u32,
    /// Optional directory for structured Zakura JSONL trace tables.
    ///
    /// When unset, Zakura trace emission is disabled. When set, the native
    /// Zakura endpoint writes the production trace schema into this directory.
    pub trace_dir: Option<PathBuf>,
}

impl Default for ZakuraConfig {
    fn default() -> Self {
        Self {
            bootstrap_peers: Vec::new(),
            listen_addr: None,
            max_connections: DEFAULT_ZAKURA_MAX_CONNECTIONS,
            max_pending_handshakes: DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
            stream_open_rate_per_second: DEFAULT_ZAKURA_STREAM_OPEN_RATE_PER_SECOND,
            message_rate_per_second: DEFAULT_ZAKURA_MESSAGE_RATE_PER_SECOND,
            trace_dir: None,
        }
    }
}

/// Hard local ceilings enforced by the Zakura endpoint and handler.
#[derive(Clone, Debug)]
pub struct ZakuraLocalLimits {
    /// Total concurrent Zakura connection cap.
    pub max_connections: usize,
    /// Concurrent control handshakes cap.
    pub max_pending_handshakes: usize,
    /// Per-connection QUIC/app idle timeout.
    pub quic_idle_timeout: Duration,
    /// QUIC keepalive interval.
    pub keep_alive_interval: Duration,
    /// Stream prelude read timeout.
    pub prelude_timeout: Duration,
    /// Control handshake read/write timeout.
    pub control_timeout: Duration,
    /// Per-connection stream-open rate.
    pub stream_open_rate_per_second: u32,
    /// Per-stream-kind message rate.
    pub message_rate_per_second: u32,
    /// Maximum frame bytes accepted locally.
    pub max_frame_bytes: u32,
    /// Maximum reassembled message bytes accepted locally.
    pub max_message_bytes: u32,
    /// Maximum concurrent admitted streams per connection.
    pub max_open_streams: u16,
    /// Maximum inbound queue depth per stream kind.
    pub max_inbound_queue_depth: u16,
}

impl ZakuraLocalLimits {
    /// Build local handler limits from network configuration and handshake policy.
    pub fn from_config(config: &Config) -> Self {
        let handshake = ZakuraHandshakeConfig::for_network(&config.network);
        Self {
            max_connections: config.zakura.max_connections.max(1),
            max_pending_handshakes: config.zakura.max_pending_handshakes.max(1),
            quic_idle_timeout: DEFAULT_ZAKURA_QUIC_IDLE_TIMEOUT,
            keep_alive_interval: DEFAULT_ZAKURA_KEEP_ALIVE_INTERVAL,
            prelude_timeout: DEFAULT_ZAKURA_PRELUDE_TIMEOUT,
            control_timeout: DEFAULT_ZAKURA_CONTROL_TIMEOUT,
            stream_open_rate_per_second: config.zakura.stream_open_rate_per_second.max(1),
            message_rate_per_second: config.zakura.message_rate_per_second.max(1),
            max_frame_bytes: handshake.max_control_frame_bytes,
            max_message_bytes: handshake.max_message_bytes,
            max_open_streams: handshake.max_open_streams,
            max_inbound_queue_depth: handshake.max_inbound_queue_depth,
        }
    }

    /// Clamp peer-negotiated limits to local hard ceilings.
    pub fn clamp(&self, negotiated: &ZakuraAcceptedLimits) -> ZakuraConnectionLimits {
        let max_open_streams = negotiated
            .max_open_streams
            .min(self.max_open_streams)
            .max(1);
        let idle_timeout = Duration::from_millis(
            u64::from(negotiated.idle_timeout_millis)
                .min(self.quic_idle_timeout.as_millis().saturating_sub(1) as u64)
                .max(1),
        );

        ZakuraConnectionLimits {
            max_frame_bytes: negotiated.max_frame_bytes.min(self.max_frame_bytes).max(1),
            max_message_bytes: negotiated
                .max_message_bytes
                .min(self.max_message_bytes)
                .max(1),
            max_open_streams,
            max_inbound_queue_depth: negotiated
                .max_inbound_queue_depth
                .min(self.max_inbound_queue_depth)
                .max(1),
            idle_timeout,
            prelude_timeout: self.prelude_timeout,
            control_timeout: self.control_timeout,
            stream_open_rate_per_second: self.stream_open_rate_per_second,
            message_rate_per_second: self.message_rate_per_second,
        }
    }

    /// Returns the initial limits advertised in a native control hello.
    pub fn initial_limits(&self) -> ZakuraInitialLimits {
        ZakuraLimits {
            max_frame_bytes: self.max_frame_bytes,
            max_message_bytes: self.max_message_bytes,
            max_open_streams: self.max_open_streams,
            max_inbound_queue_depth: self.max_inbound_queue_depth,
            idle_timeout_millis: self.quic_idle_timeout.as_millis().saturating_sub(1) as u32,
        }
    }

    /// Returns the QUIC transport config matching these local limits.
    pub fn transport_config(&self) -> TransportConfig {
        let mut transport = TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(VarInt::from_u32(u32::from(self.max_open_streams.min(64))))
            .max_concurrent_uni_streams(VarInt::from_u32(0))
            .stream_receive_window(VarInt::from_u32(DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW))
            .receive_window(VarInt::from_u32(DEFAULT_ZAKURA_RECEIVE_WINDOW))
            .send_window(DEFAULT_ZAKURA_SEND_WINDOW)
            .max_idle_timeout(Some(
                self.quic_idle_timeout
                    .try_into()
                    .expect("default Zakura idle timeout is a valid QUIC idle timeout"),
            ))
            .keep_alive_interval(Some(self.keep_alive_interval))
            .datagram_receive_buffer_size(None)
            .datagram_send_buffer_size(0);
        transport
    }
}

/// Per-connection limits after negotiation and clamping.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ZakuraConnectionLimits {
    /// Maximum encoded frame bytes.
    pub max_frame_bytes: u32,
    /// Maximum reassembled message bytes.
    pub max_message_bytes: u32,
    /// Maximum concurrent streams.
    pub max_open_streams: u16,
    /// Bounded inbound queue depth.
    pub max_inbound_queue_depth: u16,
    /// Application idle timeout.
    pub idle_timeout: Duration,
    /// Stream prelude read timeout.
    pub prelude_timeout: Duration,
    /// Control handshake timeout.
    pub control_timeout: Duration,
    /// Per-connection stream-open rate.
    pub stream_open_rate_per_second: u32,
    /// Per-stream-kind message rate.
    pub message_rate_per_second: u32,
}

/// Running Zakura endpoint owned by `zebra-network`/`zebrad` startup.
#[derive(Debug, Clone)]
pub struct ZakuraEndpoint {
    router: Router,
    supervisor: ZakuraSupervisorHandle,
    discovery: Option<ZakuraDiscoveryHandle>,
    handler: ZakuraProtocolHandler,
}

impl ZakuraEndpoint {
    /// Returns the connector injected into the legacy handshake path.
    pub fn connector(&self) -> super::ZakuraHandshakeConnector {
        super::ZakuraHandshakeConnector::new_with_endpoint(self.clone())
    }

    /// Returns our local Zakura dial hints (iroh node id, and direct addresses
    /// each encoded as a `SocketAddr` string) for the legacy upgrade prelude.
    ///
    /// Direct addresses are capped at [`MAX_IROH_DIRECT_ADDRESSES`](super::MAX_IROH_DIRECT_ADDRESSES)
    /// so the encoded prelude stays within its bounded hint limits.
    pub(crate) async fn local_upgrade_hints(&self) -> (Vec<u8>, Vec<Vec<u8>>) {
        let endpoint = self.router.endpoint();
        let node_id = endpoint.node_id().as_bytes().to_vec();
        let node_addr = endpoint.node_addr().initialized().await;
        let direct_addresses = node_addr
            .direct_addresses()
            .take(super::MAX_IROH_DIRECT_ADDRESSES)
            .map(|addr| addr.to_string().into_bytes())
            .collect();
        (node_id, direct_addresses)
    }

    /// Returns the active supervisor handle.
    pub fn supervisor(&self) -> ZakuraSupervisorHandle {
        self.supervisor.clone()
    }

    /// Returns the passive native discovery runtime handle, if enabled.
    pub fn discovery(&self) -> Option<ZakuraDiscoveryHandle> {
        self.discovery.clone()
    }

    /// Returns the endpoint's current direct node address.
    pub async fn node_addr(&self) -> NodeAddr {
        self.router.endpoint().node_addr().initialized().await
    }

    /// Teach this endpoint how to reach a peer directly.
    pub fn add_node_addr(
        &self,
        node_addr: NodeAddr,
    ) -> Result<(), iroh::endpoint::AddNodeAddrError> {
        self.router.endpoint().add_node_addr(node_addr)
    }

    /// Start a native Zakura dial in the background, retrying the initial dial a
    /// few times with backoff.
    ///
    /// Used by the legacy->Zakura upgrade hand-off: the legacy handshake just
    /// proved the peer is live, so a transient QUIC dial miss (e.g. the peer's
    /// endpoint is momentarily not ready) is worth retrying promptly instead of
    /// waiting for the legacy crawler to re-dial and re-run the whole upgrade.
    /// Once connected, longer-term recovery is left to the crawler via the
    /// address-book liveness keeper, so this does not re-dial after a drop.
    pub fn spawn_native_dial(&self, node_addr: NodeAddr) -> tokio::task::JoinHandle<()> {
        let endpoint = self.clone();
        let limits = self.handler.limits.clone();
        let policy = RedialPolicy::connect_once(
            DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF,
            DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF,
            ZAKURA_UPGRADE_DIAL_ATTEMPTS,
        );
        tokio::spawn(native_dial_supervised(endpoint, node_addr, limits, policy))
    }

    fn has_native_admission_capacity(&self) -> bool {
        self.handler.admission.available_permits() > 0
    }

    /// Shut down the Router's ordered accept/handler lifecycle.
    pub async fn shutdown(&self) {
        self.supervisor.shutdown();
        let _ = self.router.shutdown().await;
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    /// Route a test-only echo/status request through service-aware candidate selection.
    pub async fn request_test_echo_status(
        &self,
        service: &ZakuraServiceId,
        payload: Vec<u8>,
    ) -> Result<ZakuraTestServiceResponse, BoxError> {
        let discovery = self.discovery.as_ref().ok_or_else(|| -> BoxError {
            "Zakura test echo/status routing requires discovery state".into()
        })?;
        request_test_echo_status_routed(&self.supervisor, discovery, service, payload).await
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    pub(crate) fn from_parts(
        router: Router,
        supervisor: ZakuraSupervisorHandle,
        handler: ZakuraProtocolHandler,
    ) -> Self {
        let discovery = handler.discovery.clone();
        Self {
            router,
            supervisor,
            discovery,
            handler,
        }
    }
}

#[cfg(any(test, feature = "zakura-testkit"))]
/// Outcome of a test-only service-aware echo/status request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZakuraTestServiceResponse {
    /// Connected peer that answered the request.
    pub responder: NodeId,
    /// Response payload after the per-request id prefix is validated and stripped.
    pub payload: Vec<u8>,
    /// Whether the request was answered by the general-peer fallback path.
    pub used_fallback: bool,
    /// Service-advertising connected candidates that failed live request handling.
    pub failed_service_candidates: Vec<NodeId>,
}

/// Shared supervisor handle for Zakura peer registration and outbound work.
#[derive(Clone, Debug)]
pub struct ZakuraSupervisorHandle {
    id: u64,
    inner: Arc<Mutex<ZakuraSupervisorState>>,
    shutdown: CancellationToken,
    peer_set_tx: watch::Sender<Vec<ZakuraPeerId>>,
}

static NEXT_SUPERVISOR_ID: AtomicU64 = AtomicU64::new(1);
#[cfg(any(test, feature = "zakura-testkit"))]
static NEXT_TEST_SERVICE_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct ZakuraSupervisorState {
    supervisor: ZakuraPeerSupervisor,
    active_by_peer: HashMap<ZakuraPeerId, [u8; TRANSCRIPT_HASH_BYTES]>,
    outbound_by_peer: HashMap<ZakuraPeerId, ZakuraPeerHandle>,
    active_by_ip: HashMap<IpAddr, usize>,
    max_connections_per_ip: usize,
}

/// Queue-backed outbound send handle for one authenticated Zakura peer.
#[derive(Clone, Debug)]
pub struct ZakuraPeerHandle {
    peer_id: ZakuraPeerId,
    sender: mpsc::Sender<ZakuraOutboundFrame>,
}

impl ZakuraPeerHandle {
    #[cfg(test)]
    pub(crate) fn new_for_tests(
        peer_id: ZakuraPeerId,
        sender: mpsc::Sender<ZakuraOutboundFrame>,
    ) -> Self {
        Self { peer_id, sender }
    }

    /// Authenticated peer id for this handle.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    fn has_outbound_capacity(&self) -> bool {
        self.sender.capacity() > 0
    }

    /// Try to queue one outbound frame for the connection task that owns this peer's QUIC connection.
    pub fn try_send(
        &self,
        stream_kind: u16,
        message_type: u16,
        flags: u16,
        payload: Vec<u8>,
    ) -> Result<oneshot::Receiver<Result<(), BoxError>>, BoxError> {
        let (completion, completed) = oneshot::channel();
        let frame = ZakuraOutboundFrame::Frame {
            stream_kind,
            message_type,
            flags,
            payload,
            completion,
        };
        self.sender.try_send(frame).map_err(|error| -> BoxError {
            format!("Zakura outbound peer queue unavailable: {error}").into()
        })?;
        Ok(completed)
    }

    /// Queue one outbound frame for the connection task that owns this peer's QUIC connection.
    pub async fn send(
        &self,
        stream_kind: u16,
        message_type: u16,
        flags: u16,
        payload: Vec<u8>,
    ) -> Result<(), BoxError> {
        let (completion, completed) = oneshot::channel();
        let frame = ZakuraOutboundFrame::Frame {
            stream_kind,
            message_type,
            flags,
            payload,
            completion,
        };
        self.sender
            .send(frame)
            .await
            .map_err(|_| -> BoxError { "Zakura outbound peer queue closed".into() })?;
        completed
            .await
            .map_err(|_| -> BoxError { "Zakura outbound completion dropped".into() })?
    }

    /// Open a request stream, write one frame, then return the response frames from the same stream.
    pub async fn request(
        &self,
        stream_kind: u16,
        request_id: u64,
        message_type: u16,
        flags: u16,
        payload: Vec<u8>,
    ) -> Result<Vec<Frame>, BoxError> {
        let (completion, completed) = oneshot::channel();
        let frame = ZakuraOutboundFrame::Request {
            stream_kind,
            request_id,
            message_type,
            flags,
            payload,
            completion,
        };
        self.sender
            .send(frame)
            .await
            .map_err(|_| -> BoxError { "Zakura outbound peer queue closed".into() })?;
        completed
            .await
            .map_err(|_| -> BoxError { "Zakura outbound completion dropped".into() })?
    }
}

/// Outbound frame work owned by a connection-serving task.
#[derive(Debug)]
pub enum ZakuraOutboundFrame {
    /// Fire-and-forget compatibility stream frame.
    Frame {
        /// Application stream kind to open.
        stream_kind: u16,
        /// Application message type.
        message_type: u16,
        /// Message flags.
        flags: u16,
        /// Message payload bytes.
        payload: Vec<u8>,
        /// Completion sent after the frame is written or fails.
        completion: oneshot::Sender<Result<(), BoxError>>,
    },

    /// Compatibility request stream frame expecting response frames on the same stream.
    Request {
        /// Application stream kind to open.
        stream_kind: u16,
        /// Request id written into the stream prelude.
        request_id: u64,
        /// Application message type.
        message_type: u16,
        /// Message flags.
        flags: u16,
        /// Message payload bytes.
        payload: Vec<u8>,
        /// Completion sent with decoded raw response frames or an error.
        completion: oneshot::Sender<Result<Vec<Frame>, BoxError>>,
    },
}

impl ZakuraSupervisorHandle {
    /// Create an empty supervisor.
    pub fn new(max_connections_per_ip: usize) -> Self {
        Self {
            id: NEXT_SUPERVISOR_ID.fetch_add(1, Ordering::Relaxed),
            inner: Arc::new(Mutex::new(ZakuraSupervisorState {
                supervisor: ZakuraPeerSupervisor::default(),
                active_by_peer: HashMap::new(),
                outbound_by_peer: HashMap::new(),
                active_by_ip: HashMap::new(),
                max_connections_per_ip: max_connections_per_ip.max(1),
            })),
            shutdown: CancellationToken::new(),
            peer_set_tx: watch::channel(Vec::new()).0,
        }
    }

    /// Returns the currently registered authenticated Zakura peer ids.
    pub async fn registered_ids(&self) -> Vec<ZakuraPeerId> {
        let state = self.inner.lock().await;
        state.active_by_peer.keys().cloned().collect()
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Returns queue-backed handles for peers currently able to accept outbound work.
    pub async fn outbound_peer_handles(&self) -> Vec<ZakuraPeerHandle> {
        let state = self.inner.lock().await;
        state
            .outbound_by_peer
            .values()
            .filter(|handle| handle.has_outbound_capacity())
            .cloned()
            .collect()
    }

    /// Subscribe to peer-set changes for event-driven tests and diagnostics.
    pub fn subscribe(&self) -> watch::Receiver<Vec<ZakuraPeerId>> {
        self.peer_set_tx.subscribe()
    }

    async fn register(
        &self,
        peer_id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        transcript_hash: [u8; TRANSCRIPT_HASH_BYTES],
        outbound_handle: ZakuraPeerHandle,
    ) -> ZakuraRegistration {
        let mut state = self.inner.lock().await;
        if let Some(remote_ip) = remote_ip {
            let ip_count = state
                .active_by_ip
                .get(&remote_ip)
                .copied()
                .unwrap_or_default();
            if ip_count >= state.max_connections_per_ip {
                metrics::counter!("zakura.p2p.conn.rejected.admission").increment(1);
                return ZakuraRegistration::Rejected(ZakuraRejectReason::ResourceLimit);
            }
        }

        match state
            .supervisor
            .register_authenticated(peer_id.clone(), transcript_hash)
        {
            ZakuraUpgradeOutcome::Upgraded { .. } => {
                if let Some(remote_ip) = remote_ip {
                    *state.active_by_ip.entry(remote_ip).or_default() += 1;
                }
                state
                    .active_by_peer
                    .insert(peer_id.clone(), transcript_hash);
                state
                    .outbound_by_peer
                    .insert(peer_id.clone(), outbound_handle);
                let registered_ids = state.active_by_peer.keys().cloned().collect();
                self.peer_set_tx.send_replace(registered_ids);
                ZakuraRegistration::Registered { peer_id, remote_ip }
            }
            ZakuraUpgradeOutcome::Duplicate { .. } => ZakuraRegistration::Duplicate { peer_id },
            ZakuraUpgradeOutcome::Rejected { reason } => ZakuraRegistration::Rejected(reason),
        }
    }

    async fn can_accept_remote_ip_with_in_flight(
        &self,
        remote_ip: IpAddr,
        in_flight_count: usize,
    ) -> bool {
        let state = self.inner.lock().await;
        let active_count = state
            .active_by_ip
            .get(&remote_ip)
            .copied()
            .unwrap_or_default();
        active_count.saturating_add(in_flight_count) < state.max_connections_per_ip
    }

    async fn deregister(&self, peer_id: &ZakuraPeerId, remote_ip: Option<IpAddr>) {
        let mut state = self.inner.lock().await;
        state.active_by_peer.remove(peer_id);
        state.outbound_by_peer.remove(peer_id);
        if let Some(remote_ip) = remote_ip {
            if let Some(count) = state.active_by_ip.get_mut(&remote_ip) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    state.active_by_ip.remove(&remote_ip);
                }
            }
        }
        state.supervisor.deregister_authenticated(peer_id);
        let registered_ids = state.active_by_peer.keys().cloned().collect();
        self.peer_set_tx.send_replace(registered_ids);
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

#[derive(Debug)]
enum ZakuraRegistration {
    Registered {
        peer_id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
    },
    Duplicate {
        peer_id: ZakuraPeerId,
    },
    Rejected(ZakuraRejectReason),
}

#[derive(Clone, Debug)]
struct ZakuraConnTrace {
    id: u64,
    peer_label: Option<Arc<str>>,
}

impl ZakuraConnTrace {
    fn new(trace: &ZakuraTrace, id: u64, peer_id: &ZakuraPeerId) -> Self {
        Self {
            id,
            peer_label: trace
                .is_enabled()
                .then(|| Arc::<str>::from(trace_peer_label(peer_id))),
        }
    }

    fn without_peer(id: u64) -> Self {
        Self {
            id,
            peer_label: None,
        }
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    fn placeholder() -> Self {
        Self::without_peer(0)
    }

    fn peer(&self) -> Option<&str> {
        self.peer_label.as_deref()
    }

    fn event<'a>(&'a self, event: &'static str) -> ZakuraTraceEvent<'a> {
        ZakuraTraceEvent::new(event)
            .conn(self.id)
            .maybe_peer(self.peer())
    }
}

struct StreamAdmission<'a> {
    trace: ZakuraTrace,
    conn: ZakuraConnTrace,
    peer_id: &'a ZakuraPeerId,
    stream_sem: &'a Arc<Semaphore>,
    open_limiter: &'a mut TokenBucket,
    message_buckets: &'a mut MessageRateBuckets,
    workers: &'a mut JoinSet<()>,
    limits: ZakuraConnectionLimits,
    connection_token: CancellationToken,
    freshness_tx: watch::Sender<Instant>,
    inbound_tx: mpsc::Sender<ZakuraInboundMessage>,
}

impl StreamAdmission<'_> {
    fn event(&self, event: &'static str, stream_id: u64) -> ZakuraTraceEvent<'_> {
        self.conn.event(event).stream(stream_id)
    }
}

struct ConnectionServeContext {
    limits: ZakuraConnectionLimits,
    role: &'static str,
    direction: &'static str,
    conn: ZakuraConnTrace,
}

struct StreamWorkerContext {
    trace: ZakuraTrace,
    conn: ZakuraConnTrace,
    peer_id: ZakuraPeerId,
    stream_id: u64,
    _permit: OwnedSemaphorePermit,
    limits: ZakuraConnectionLimits,
    message_bucket: SharedMessageBucket,
    connection_token: CancellationToken,
    freshness_tx: watch::Sender<Instant>,
    inbound_tx: mpsc::Sender<ZakuraInboundMessage>,
}

impl StreamWorkerContext {
    fn event(&self, event: &'static str) -> ZakuraTraceEvent<'_> {
        self.conn.event(event).stream(self.stream_id)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum InboundMessageAdmission {
    Admit,
    Oversize,
    Throttled,
}

fn admit_inbound_message(
    payload_len: usize,
    context: &StreamWorkerContext,
    stream_kind: u16,
) -> InboundMessageAdmission {
    let stream_kind = stream_kind_label(stream_kind);
    let max_message_bytes = usize::try_from(context.limits.max_message_bytes)
        .expect("u32 message byte limit fits in usize");
    if payload_len > max_message_bytes {
        metrics::counter!(
            "zakura.p2p.ratelimit.message.oversize",
            "stream_kind" => stream_kind,
        )
        .increment(1);
        context.trace.emit(
            RATELIMIT_TABLE,
            context.event("message.oversize").stream_kind(stream_kind),
        );
        return InboundMessageAdmission::Oversize;
    }

    let admitted = {
        let mut bucket = context
            .message_bucket
            .lock()
            .expect("Zakura message-rate bucket mutex is never poisoned");
        bucket.try_take()
    };
    if !admitted {
        metrics::counter!(
            "zakura.p2p.ratelimit.message.throttled",
            "stream_kind" => stream_kind,
        )
        .increment(1);
        context.trace.emit(
            RATELIMIT_TABLE,
            context.event("message.throttled").stream_kind(stream_kind),
        );
        return InboundMessageAdmission::Throttled;
    }

    InboundMessageAdmission::Admit
}

#[derive(Debug)]
struct ZakuraInboundMessage {
    peer_id: ZakuraPeerId,
    stream_kind: u16,
    frame: Frame,
}

/// Application sink for decoded inbound Zakura stream frames.
pub trait InboundSink: fmt::Debug + Send + Sync + 'static {
    /// Deliver one decoded frame from the bounded per-connection queue.
    fn deliver(
        &self,
        peer_id: ZakuraPeerId,
        stream_kind: u16,
        frame: Frame,
    ) -> Result<(), InboundSinkReject>;

    /// Deliver one request-stream frame and return response frames for the same stream.
    fn request<'a>(
        &'a self,
        _peer_id: ZakuraPeerId,
        _stream_kind: u16,
        _request_id: u64,
        _max_frame_bytes: u32,
        _frame: Frame,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Frame>, InboundSinkReject>> + Send + 'a>> {
        Box::pin(async {
            Err(InboundSinkReject::protocol(
                "request streams are not supported by this inbound sink",
            ))
        })
    }
}

/// Reason an [`InboundSink`] rejected a decoded frame.
#[derive(Debug, Error)]
pub enum InboundSinkReject {
    /// The peer sent a protocol-invalid frame, so the connection should close.
    #[error("inbound sink rejected protocol-invalid frame: {0}")]
    Protocol(#[source] BoxError),

    /// Local sink state prevented delivery; the peer is not at fault.
    #[error("inbound sink could not accept frame locally: {0}")]
    Local(#[source] BoxError),
}

impl InboundSinkReject {
    /// Build a fatal peer-protocol rejection.
    pub fn protocol(error: impl Into<BoxError>) -> Self {
        Self::Protocol(error.into())
    }

    /// Build a non-fatal local-delivery rejection.
    pub fn local(error: impl Into<BoxError>) -> Self {
        Self::Local(error.into())
    }
}

#[derive(Debug, Default)]
struct DropInboundSink;

impl InboundSink for DropInboundSink {
    fn deliver(
        &self,
        _peer_id: ZakuraPeerId,
        _stream_kind: u16,
        _frame: Frame,
    ) -> Result<(), InboundSinkReject> {
        Ok(())
    }
}

/// Iroh protocol handler for the Zakura `p2p-v2/1` ALPN.
#[derive(Debug, Clone)]
pub struct ZakuraProtocolHandler {
    supervisor: ZakuraSupervisorHandle,
    handshake_config: ZakuraHandshakeConfig,
    limits: ZakuraLocalLimits,
    inbound_sink: Arc<dyn InboundSink>,
    discovery: Option<ZakuraDiscoveryHandle>,
    trace: ZakuraTrace,
    next_conn_id: Arc<AtomicU64>,
    next_stream_id: Arc<AtomicU64>,
    admission: Arc<Semaphore>,
    pending_handshakes: Arc<Semaphore>,
    shutdown: CancellationToken,
}

impl ZakuraProtocolHandler {
    /// Create a handler sharing the given supervisor.
    pub fn new(
        supervisor: ZakuraSupervisorHandle,
        handshake_config: ZakuraHandshakeConfig,
        limits: ZakuraLocalLimits,
    ) -> Self {
        Self::new_with_sink(
            supervisor,
            handshake_config,
            limits,
            Arc::new(DropInboundSink),
        )
    }

    /// Create a handler with an injected inbound sink.
    pub fn new_with_sink(
        supervisor: ZakuraSupervisorHandle,
        handshake_config: ZakuraHandshakeConfig,
        limits: ZakuraLocalLimits,
        inbound_sink: Arc<dyn InboundSink>,
    ) -> Self {
        Self::new_with_sink_and_trace(
            supervisor,
            handshake_config,
            limits,
            inbound_sink,
            ZakuraTrace::noop(),
        )
    }

    /// Create a handler with an injected inbound sink and trace emitter.
    pub fn new_with_sink_and_trace(
        supervisor: ZakuraSupervisorHandle,
        handshake_config: ZakuraHandshakeConfig,
        limits: ZakuraLocalLimits,
        inbound_sink: Arc<dyn InboundSink>,
        trace: ZakuraTrace,
    ) -> Self {
        Self::new_with_sink_trace_and_discovery(
            supervisor,
            handshake_config,
            limits,
            inbound_sink,
            trace,
            None,
        )
    }

    /// Create a handler with an injected inbound sink, trace emitter, and discovery state.
    pub fn new_with_sink_trace_and_discovery(
        supervisor: ZakuraSupervisorHandle,
        handshake_config: ZakuraHandshakeConfig,
        limits: ZakuraLocalLimits,
        inbound_sink: Arc<dyn InboundSink>,
        trace: ZakuraTrace,
        discovery: Option<ZakuraDiscoveryHandle>,
    ) -> Self {
        Self {
            supervisor,
            handshake_config,
            inbound_sink,
            discovery,
            trace,
            next_conn_id: Arc::new(AtomicU64::new(1)),
            next_stream_id: Arc::new(AtomicU64::new(1)),
            admission: Arc::new(Semaphore::new(limits.max_connections)),
            pending_handshakes: Arc::new(Semaphore::new(limits.max_pending_handshakes)),
            shutdown: CancellationToken::new(),
            limits,
        }
    }

    async fn accept_connection(&self, connection: Connection) -> Result<(), AcceptError> {
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let Ok(_admission) = self.admission.clone().try_acquire_owned() else {
            metrics::counter!("zakura.p2p.conn.rejected.admission").increment(1);
            let conn = ZakuraConnTrace::without_peer(conn_id);
            self.trace.emit(
                CONN_TABLE,
                conn.event("rejected.admission")
                    .direction("inbound")
                    .reason("admission"),
            );
            connection.close(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE), b"admission");
            return Ok(());
        };

        let remote_node_id = connection.remote_node_id()?;
        let remote_peer_id =
            ZakuraPeerId::new(remote_node_id.as_bytes().to_vec()).map_err(AcceptError::from_err)?;
        let conn = ZakuraConnTrace::new(&self.trace, conn_id, &remote_peer_id);

        let negotiated = match self
            .run_native_responder_handshake_with_permit(&connection, &remote_peer_id, &conn)
            .await
        {
            Ok(negotiated) => negotiated,
            Err(ZakuraHandlerError::ResourceLimit("pending handshake")) => return Ok(()),
            Err(error) => {
                debug!(?error, "Zakura control handshake failed");
                connection.close(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL), b"control handshake");
                return Ok(());
            }
        };

        let conn_limits = self.limits.clamp(&negotiated);
        // Iroh's Router hands ProtocolHandler only the established Connection.
        // In iroh 0.92.0 the peer UDP address is exposed on Incoming, which the
        // Router consumes before this point, not on Connection/Connecting. The
        // inbound per-IP cap therefore remains deferred while 01.a keeps Router
        // ownership. Native outbound dials still pass the configured direct IP.
        let remote_ip = None;
        self.register_and_serve(
            connection,
            remote_peer_id,
            remote_ip,
            ConnectionServeContext {
                limits: conn_limits,
                role: "responder",
                direction: "inbound",
                conn,
            },
        )
        .await
        .map_err(AcceptError::from_err)
    }

    async fn run_native_responder_handshake_with_permit(
        &self,
        connection: &Connection,
        remote_peer_id: &ZakuraPeerId,
        conn: &ZakuraConnTrace,
    ) -> Result<ZakuraAcceptedLimits, ZakuraHandlerError> {
        let Ok(_handshake) = self.pending_handshakes.clone().try_acquire_owned() else {
            metrics::counter!("zakura.p2p.conn.rejected.pending_handshake").increment(1);
            self.trace.emit(
                CONN_TABLE,
                conn.event("rejected.admission")
                    .direction("inbound")
                    .reason("pending_handshake"),
            );
            connection.close(
                VarInt::from_u32(ZAKURA_CLOSE_RESOURCE),
                b"pending handshake",
            );
            return Err(ZakuraHandlerError::ResourceLimit("pending handshake"));
        };

        self.run_native_responder_handshake(connection, remote_peer_id, conn)
            .await
    }

    async fn run_native_responder_handshake(
        &self,
        connection: &Connection,
        remote_peer_id: &ZakuraPeerId,
        conn: &ZakuraConnTrace,
    ) -> Result<ZakuraAcceptedLimits, ZakuraHandlerError> {
        self.trace.emit(
            HANDSHAKE_TABLE,
            conn.event("control.started")
                .role("responder")
                .phase("control")
                .network(self.handshake_config.network_label()),
        );
        let (mut send, mut recv) = timeout(self.limits.control_timeout, connection.accept_bi())
            .await
            .map_err(|_| ZakuraHandlerError::Timeout("accept control stream"))??;

        let hello_bytes = read_control_payload(
            &mut recv,
            self.handshake_config.max_control_frame_bytes,
            self.limits.control_timeout,
        )
        .await?;
        let hello = ZakuraControlHello::decode(&hello_bytes)?;
        let expected = ZakuraControlValidation {
            local: &self.handshake_config,
            authenticated_remote_id: remote_peer_id.as_bytes(),
            selected_zakura_protocol: ZAKURA_PROTOCOL_VERSION_1,
            handshake_path: ZakuraHandshakePath::Native,
            remote_role: ZakuraControlRole::Initiator,
            initiator_upgrade_nonce: [0; 32],
            responder_upgrade_nonce: [0; 32],
            legacy_upgrade_transcript: [0; 32],
        };
        hello.validate(&expected)?;

        let mut local_nonce = [0; 32];
        OsRng.fill_bytes(&mut local_nonce);

        let accepted_limits = self.accepted_limits_for(&hello.initial_limits);
        let ack = ZakuraControlAck {
            magic: CONTROL_ACK_MAGIC,
            control_version: CONTROL_VERSION,
            selected_zakura_protocol: hello.selected_zakura_protocol,
            peer_nonce: local_nonce,
            remote_peer_nonce: hello.peer_nonce,
            accepted_capabilities: hello.capabilities
                & self.handshake_config.supported_capabilities,
            accepted_channels: hello.required_channels & self.handshake_config.supported_channels,
            accepted_limits,
        };
        write_control_payload(&mut send, &ack.encode()?, self.limits.control_timeout).await?;
        self.trace.emit(
            HANDSHAKE_TABLE,
            conn.event("control.succeeded")
                .role("responder")
                .phase("control")
                .selected_protocol(ack.selected_zakura_protocol)
                .network(self.handshake_config.network_label()),
        );
        Ok(accepted_limits)
    }

    fn accepted_limits_for(&self, remote_limits: &ZakuraInitialLimits) -> ZakuraAcceptedLimits {
        ZakuraLimits {
            max_frame_bytes: remote_limits
                .max_frame_bytes
                .min(self.limits.max_frame_bytes),
            max_message_bytes: remote_limits
                .max_message_bytes
                .min(self.limits.max_message_bytes),
            max_open_streams: remote_limits
                .max_open_streams
                .min(self.limits.max_open_streams),
            max_inbound_queue_depth: remote_limits
                .max_inbound_queue_depth
                .min(self.limits.max_inbound_queue_depth),
            idle_timeout_millis: remote_limits
                .idle_timeout_millis
                .min(self.limits.initial_limits().idle_timeout_millis),
        }
    }

    async fn serve_connection(
        &self,
        connection: Connection,
        peer_id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        mut outbound_rx: mpsc::Receiver<ZakuraOutboundFrame>,
        limits: ZakuraConnectionLimits,
        conn: ZakuraConnTrace,
    ) -> Result<(), ZakuraHandlerError> {
        let connection_token = self.shutdown.child_token();
        let stream_sem = Arc::new(Semaphore::new(usize::from(limits.max_open_streams)));
        let mut workers = JoinSet::new();
        let mut open_limiter = TokenBucket::new(limits.stream_open_rate_per_second);
        let mut message_buckets = MessageRateBuckets::new();
        let (freshness_tx, freshness_rx) = watch::channel(Instant::now());
        let (inbound_tx, inbound_rx) =
            mpsc::channel::<ZakuraInboundMessage>(usize::from(limits.max_inbound_queue_depth));
        workers.spawn(inbound_message_sink(
            inbound_rx,
            connection_token.clone(),
            self.inbound_sink.clone(),
        ));
        if let Some(discovery) = self.discovery.clone() {
            workers.spawn(discovery_exchange_loop(
                connection.clone(),
                peer_id.clone(),
                limits,
                connection_token.clone(),
                discovery,
            ));
        }

        loop {
            tokio::select! {
                biased;
                _ = connection_token.cancelled() => break,
                _ = freshness_reaper(freshness_rx.clone(), limits.idle_timeout) => {
                    connection.close(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL), b"idle");
                    break;
                }
                Some(joined) = workers.join_next() => {
                    if let Err(error) = joined {
                        debug!(?error, "Zakura stream worker exited unexpectedly");
                    }
                }
                accepted = connection.accept_bi() => {
                    match accepted {
                        Ok((send, recv)) => {
                            let mut admission = StreamAdmission {
                                trace: self.trace.clone(),
                                conn: conn.clone(),
                                peer_id: &peer_id,
                                stream_sem: &stream_sem,
                                open_limiter: &mut open_limiter,
                                message_buckets: &mut message_buckets,
                                workers: &mut workers,
                                limits,
                                connection_token: connection_token.clone(),
                                freshness_tx: freshness_tx.clone(),
                                inbound_tx: inbound_tx.clone(),
                            };
                            self.admit_bi_stream(send, recv, &mut admission).await;
                        }
                        Err(error) => {
                            debug!(?error, "Zakura connection stopped accepting streams");
                            break;
                        }
                    }
                }
                outbound = outbound_rx.recv() => {
                    let Some(outbound) = outbound else {
                        break;
                    };
                    match outbound {
                        ZakuraOutboundFrame::Frame {
                            stream_kind,
                            message_type,
                            flags,
                            payload,
                            completion,
                        } => {
                            let result = write_outbound_frame(
                                &connection,
                                limits,
                                stream_kind,
                                message_type,
                                flags,
                                payload,
                            )
                            .await;
                            let _ = completion.send(result);
                        }
                        ZakuraOutboundFrame::Request {
                            stream_kind,
                            request_id,
                            message_type,
                            flags,
                            payload,
                            mut completion,
                        } => {
                            let result = tokio::select! {
                                biased;
                                _ = completion.closed() => {
                                    Err(OutboundRequestError::Local(
                                        "Zakura outbound request receiver dropped".into(),
                                    ))
                                }
                                result = write_outbound_request_frame(
                                    &connection,
                                    limits,
                                    stream_kind,
                                    request_id,
                                    message_type,
                                    flags,
                                    payload,
                                ) => result,
                            };
                            match result {
                                Ok(frames) => {
                                    let _ = completion.send(Ok(frames));
                                }
                                Err(OutboundRequestError::Local(error)) => {
                                    let _ = completion.send(Err(error));
                                }
                                Err(OutboundRequestError::Fatal(error)) => {
                                    connection.close(
                                        VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE),
                                        b"malformed response",
                                    );
                                    connection_token.cancel();
                                    let _ = completion.send(Err(error));
                                }
                            }
                        }
                    }
                }
            }
        }

        drop(inbound_tx);
        connection_token.cancel();
        while let Some(joined) = timeout(STREAM_WORKER_DRAIN_TIMEOUT, workers.join_next())
            .await
            .ok()
            .flatten()
        {
            if let Err(error) = joined {
                debug!(?error, "Zakura stream worker failed during shutdown");
            }
        }
        workers.abort_all();
        self.supervisor.deregister(&peer_id, remote_ip).await;
        metrics::counter!("zakura.p2p.conn.closed.neutral").increment(1);
        self.trace.emit(CONN_TABLE, conn.event("closed.neutral"));
        Ok(())
    }

    async fn admit_bi_stream(
        &self,
        mut send: SendStream,
        mut recv: RecvStream,
        admission: &mut StreamAdmission<'_>,
    ) {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let Ok(permit) = admission.stream_sem.clone().try_acquire_owned() else {
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
            metrics::counter!("zakura.p2p.stream.rejected.semaphore").increment(1);
            admission.trace.emit(
                STREAM_TABLE,
                admission.event("rejected.semaphore", stream_id),
            );
            return;
        };

        let prelude = match read_stream_prelude(&mut recv, admission.limits.prelude_timeout).await {
            Ok(prelude) => prelude,
            Err(error) => {
                debug!(?error, "rejecting Zakura stream with bad prelude");
                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                metrics::counter!("zakura.p2p.stream.rejected.prelude").increment(1);
                admission
                    .trace
                    .emit(STREAM_TABLE, admission.event("rejected.prelude", stream_id));
                return;
            }
        };
        let stream_kind = stream_kind_label(prelude.stream_kind);

        if !is_supported_stream(prelude.stream_kind, prelude.stream_version) {
            debug!(
                stream_kind = prelude.stream_kind,
                stream_version = prelude.stream_version,
                "rejecting Zakura stream with unknown kind or unsupported version"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_UNKNOWN_STREAM));
            metrics::counter!(
                "zakura.p2p.stream.rejected.unknown_kind",
                "stream_kind" => stream_kind,
            )
            .increment(1);
            admission.trace.emit(
                STREAM_TABLE,
                admission
                    .event("rejected.unknown_kind", stream_id)
                    .stream_kind(stream_kind),
            );
            return;
        }

        if prelude.stream_kind == LEGACY_GOSSIP_STREAM_KIND && prelude.request_id.is_some() {
            debug!("rejecting Zakura gossip stream with request id");
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            admission.connection_token.cancel();
            metrics::counter!("zakura.p2p.stream.rejected.gossip_request_id").increment(1);
            admission.trace.emit(
                STREAM_TABLE,
                admission
                    .event("rejected.gossip_request_id", stream_id)
                    .stream_kind(stream_kind),
            );
            return;
        }

        if prelude.stream_kind == ZAKURA_STREAM_DISCOVERY && prelude.request_id.is_some() {
            debug!("rejecting Zakura discovery stream with request id");
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            metrics::counter!("zakura.p2p.stream.rejected.discovery_request_id").increment(1);
            admission.trace.emit(
                STREAM_TABLE,
                admission
                    .event("rejected.discovery_request_id", stream_id)
                    .stream_kind(stream_kind),
            );
            return;
        }

        if is_request_stream_kind(prelude.stream_kind) && prelude.request_id.is_none() {
            debug!("rejecting Zakura request stream without request id");
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            admission.connection_token.cancel();
            metrics::counter!("zakura.p2p.stream.rejected.request_without_id").increment(1);
            admission.trace.emit(
                STREAM_TABLE,
                admission
                    .event("rejected.request_without_id", stream_id)
                    .stream_kind(stream_kind),
            );
            return;
        }

        if !admission.open_limiter.try_take() {
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RATE_LIMIT));
            metrics::counter!("zakura.p2p.stream.rejected.open_rate").increment(1);
            admission.trace.emit(
                STREAM_TABLE,
                admission
                    .event("rejected.open_rate", stream_id)
                    .stream_kind(stream_kind),
            );
            return;
        }

        metrics::counter!(
            "zakura.p2p.stream.accepted",
            "stream_kind" => stream_kind,
        )
        .increment(1);
        admission.trace.emit(
            STREAM_TABLE,
            admission
                .event("accepted", stream_id)
                .stream_kind(stream_kind),
        );

        let message_bucket = message_bucket_for(
            admission.message_buckets,
            prelude.stream_kind,
            admission.limits.message_rate_per_second,
            RealClock,
        );

        let context = StreamWorkerContext {
            trace: admission.trace.clone(),
            conn: admission.conn.clone(),
            peer_id: admission.peer_id.clone(),
            stream_id,
            _permit: permit,
            limits: admission.limits,
            message_bucket,
            connection_token: admission.connection_token.clone(),
            freshness_tx: admission.freshness_tx.clone(),
            inbound_tx: admission.inbound_tx.clone(),
        };

        if prelude.stream_kind == ZAKURA_STREAM_DISCOVERY {
            let Some(discovery) = self.discovery.clone() else {
                debug!("rejecting Zakura discovery stream without local discovery state");
                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
                return;
            };
            admission.workers.spawn(discovery_stream_worker(
                send, recv, prelude, context, discovery,
            ));
        } else if is_request_stream_kind(prelude.stream_kind) {
            admission.workers.spawn(request_stream_worker(
                send,
                recv,
                prelude,
                context,
                self.inbound_sink.clone(),
            ));
        } else {
            admission
                .workers
                .spawn(stream_worker(send, recv, prelude, context));
        }
    }

    async fn register_and_serve(
        &self,
        connection: Connection,
        peer_id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        context: ConnectionServeContext,
    ) -> Result<(), ZakuraHandlerError> {
        let (outbound_tx, outbound_rx) =
            mpsc::channel(usize::from(context.limits.max_inbound_queue_depth));
        let outbound_handle = ZakuraPeerHandle {
            peer_id: peer_id.clone(),
            sender: outbound_tx,
        };
        let registration = self
            .supervisor
            .register(peer_id, remote_ip, NATIVE_TRANSCRIPT_HASH, outbound_handle)
            .await;

        match registration {
            ZakuraRegistration::Registered { peer_id, remote_ip } => {
                metrics::counter!("zakura.p2p.conn.accepted", "role" => context.role).increment(1);
                self.trace.emit(
                    CONN_TABLE,
                    context
                        .conn
                        .event("accepted")
                        .role(context.role)
                        .direction(context.direction),
                );
                self.serve_connection(
                    connection,
                    peer_id,
                    remote_ip,
                    outbound_rx,
                    context.limits,
                    context.conn,
                )
                .await
            }
            ZakuraRegistration::Duplicate { peer_id } => {
                debug!(?peer_id, "closing duplicate Zakura peer neutrally");
                metrics::counter!("zakura.p2p.conn.duplicate").increment(1);
                self.trace.emit(
                    CONN_TABLE,
                    context
                        .conn
                        .event("duplicate")
                        .role(context.role)
                        .direction(context.direction),
                );
                connection.close(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL), b"duplicate");
                Ok(())
            }
            ZakuraRegistration::Rejected(reason) => {
                debug!(
                    ?reason,
                    "closing Zakura peer neutrally after registration rejection"
                );
                self.trace.emit(
                    CONN_TABLE,
                    context
                        .conn
                        .event("rejected.admission")
                        .role(context.role)
                        .direction(context.direction)
                        .reason(reject_reason_label(reason)),
                );
                connection.close(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE), b"registration");
                Ok(())
            }
        }
    }
}

impl ProtocolHandler for ZakuraProtocolHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.accept_connection(connection).await
    }

    async fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// Start a Zakura endpoint and router when P2P v2 is enabled.
pub async fn spawn_zakura_endpoint(
    config: &Config,
    sink_factory: impl FnOnce(ZakuraSupervisorHandle) -> Arc<dyn InboundSink>,
) -> Result<Option<ZakuraEndpoint>, BoxError> {
    if !config.v2_p2p {
        return Ok(None);
    }

    let limits = ZakuraLocalLimits::from_config(config);
    validate_idle_invariant(&limits)?;
    let secret_key = zakura_secret_key(config)?;
    let discovery_secret_key = secret_key.clone();
    let mut builder =
        direct_endpoint_builder(secret_key).transport_config(limits.transport_config());
    // Bind a fixed address when configured so this node has a stable, advertisable
    // Zakura endpoint; otherwise iroh assigns an ephemeral port (dial-out only).
    match config.zakura.listen_addr {
        Some(SocketAddr::V4(addr)) => builder = builder.bind_addr_v4(addr),
        Some(SocketAddr::V6(addr)) => builder = builder.bind_addr_v6(addr),
        None => {}
    }
    let endpoint = builder.bind().await?;
    let supervisor = ZakuraSupervisorHandle::new(config.max_connections_per_ip);
    let handshake_config = ZakuraHandshakeConfig::for_network(&config.network);
    let bootstrap_peers = parse_bootstrap_peers(&config.zakura.bootstrap_peers);
    let discovery = ZakuraDiscoveryHandle::new(
        ZakuraDiscoveryLocalConfig {
            secret_key: discovery_secret_key,
            direct_addrs: config.zakura.listen_addr.into_iter().collect(),
            services: default_advertised_services(),
            zakura_protocol_min: handshake_config.zakura_protocol_min,
            zakura_protocol_max: handshake_config.zakura_protocol_max,
            network_id: handshake_config.network_id,
            chain_id: handshake_config.chain_id,
            last_authored_sequence: None,
        },
        ZakuraDiscoveryConfig {
            max_zakura_connections: config.zakura.max_connections,
            discovery_connection_headroom: effective_discovery_connection_headroom(
                bootstrap_peers.len(),
            ),
            ..ZakuraDiscoveryConfig::default()
        },
        supervisor.subscribe(),
    )?;
    // Discovery cache persistence is intentionally deferred to the cache-file
    // persistence slice; runtime state exposes import/snapshot hooks for it.
    // Build the inbound sink from the endpoint's supervisor so the adapter and
    // the supervisor share one first-seen cache (see ZakuraDualStackService).
    let inbound_sink = sink_factory(supervisor.clone());
    let tracer = config
        .zakura
        .trace_dir
        .clone()
        .map(zebra_jsonl_trace::JsonlTracer::spawn)
        .unwrap_or_else(zebra_jsonl_trace::JsonlTracer::noop);
    let trace = ZakuraTrace::new(tracer, zebra_jsonl_trace::node_id());
    let handler = ZakuraProtocolHandler::new_with_sink_trace_and_discovery(
        supervisor.clone(),
        handshake_config,
        limits.clone(),
        inbound_sink,
        trace,
        Some(discovery.clone()),
    );
    let router = Router::builder(endpoint)
        .accept(P2P_V2_ALPN, handler.clone())
        .spawn();
    let endpoint = ZakuraEndpoint {
        router,
        supervisor,
        discovery: Some(discovery.clone()),
        handler,
    };

    // Log our own dial address once iroh has resolved it, so operators can hand
    // out `<node_id>@<direct_addr>` for other nodes' `zakura.bootstrap_peers`.
    {
        let endpoint = endpoint.clone();
        tokio::spawn(async move {
            let node_addr = endpoint.node_addr().await;
            let direct_addresses: Vec<String> = node_addr
                .direct_addresses()
                .map(|addr| addr.to_string())
                .collect();
            info!(
                node_id = %node_addr.node_id,
                ?direct_addresses,
                "Zakura P2P endpoint ready; advertise <node_id>@<direct_addr> as a bootstrap peer",
            );
        });
    }

    insert_static_bootstrap_candidates(&discovery, &bootstrap_peers).await;
    spawn_native_bootstrap_dialer(endpoint.clone(), bootstrap_peers, limits.clone());
    spawn_native_discovery_dialer(endpoint.clone(), limits);
    Ok(Some(endpoint))
}

fn spawn_native_bootstrap_dialer(
    endpoint: ZakuraEndpoint,
    bootstrap_peers: Vec<NodeAddr>,
    limits: ZakuraLocalLimits,
) {
    if bootstrap_peers.is_empty() {
        return;
    }

    // Configured bootstrap peers are maintained: keep re-dialing forever so a
    // node whose only peers are over Zakura (`legacy_p2p = false`) tolerates the
    // seed not being up yet at startup and recovers when a peer later drops. The
    // legacy crawler is absent on such a node, so this loop is the only healing
    // path for its seeds.
    let policy = RedialPolicy::maintain(
        DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF,
        DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF,
    );

    for node_addr in bootstrap_peers {
        let endpoint = endpoint.clone();
        let limits = limits.clone();
        tokio::spawn(async move {
            native_dial_supervised(endpoint, node_addr, limits, policy).await;
        });
    }
}

fn spawn_native_discovery_dialer(endpoint: ZakuraEndpoint, limits: ZakuraLocalLimits) {
    let Some(discovery) = endpoint.discovery() else {
        return;
    };
    tokio::spawn(run_native_discovery_dialer(endpoint, discovery, limits));
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum DiscoveryDialResult {
    Registered,
    Failed,
    LocalResourceLimit,
}

#[derive(Debug)]
struct DiscoveryDialWorkerResult {
    node_id: NodeId,
    reserved_ips: Vec<IpAddr>,
    result: DiscoveryDialResult,
}

pub(crate) async fn run_native_discovery_dialer(
    endpoint: ZakuraEndpoint,
    discovery: ZakuraDiscoveryHandle,
    limits: ZakuraLocalLimits,
) {
    let mut registered = endpoint.supervisor().subscribe();
    let mut in_flight = HashSet::new();
    let mut in_flight_by_ip = HashMap::new();
    let mut workers = JoinSet::new();

    loop {
        spawn_discovery_dial_candidates(
            &endpoint,
            &discovery,
            &limits,
            &mut in_flight,
            &mut in_flight_by_ip,
            &mut workers,
        )
        .await;

        tokio::select! {
            joined = workers.join_next(), if !workers.is_empty() => {
                match joined {
                    Some(Ok(worker_result)) => {
                        in_flight.remove(&worker_result.node_id);
                        release_discovery_in_flight_ips(
                            &mut in_flight_by_ip,
                            &worker_result.reserved_ips,
                        );
                        apply_discovery_dial_result(
                            &discovery,
                            &worker_result.node_id,
                            worker_result.result,
                        ).await;
                    }
                    Some(Err(error)) => {
                        debug!(?error, "Zakura discovery dial worker failed");
                        metrics::counter!("zakura.p2p.discovery.dial.worker_failed").increment(1);
                    }
                    None => {}
                }
            }
            changed = registered.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = tokio::time::sleep(ZAKURA_DISCOVERY_DIAL_INTERVAL) => {}
        }
    }
}

async fn spawn_discovery_dial_candidates(
    endpoint: &ZakuraEndpoint,
    discovery: &ZakuraDiscoveryHandle,
    limits: &ZakuraLocalLimits,
    in_flight: &mut HashSet<NodeId>,
    in_flight_by_ip: &mut HashMap<IpAddr, usize>,
    workers: &mut JoinSet<DiscoveryDialWorkerResult>,
) {
    if !endpoint.has_native_admission_capacity() {
        return;
    }

    let in_flight_node_ids: Vec<_> = in_flight.iter().copied().collect();
    for candidate in discovery.dial_candidates(&[], &in_flight_node_ids).await {
        if !endpoint.has_native_admission_capacity() {
            return;
        }
        let Some((node_addr, reserved_ips)) =
            discovery_node_addr_with_reserved_ip_capacity(endpoint, &candidate, in_flight_by_ip)
                .await
        else {
            continue;
        };
        let node_id = candidate.node_id;
        if !in_flight.insert(node_id) {
            continue;
        }
        reserve_discovery_in_flight_ips(in_flight_by_ip, &reserved_ips);

        discovery.mark_dial_attempt(&node_id).await;
        metrics::counter!("zakura.p2p.discovery.dial.started").increment(1);
        workers.spawn(run_discovery_dial_once(
            endpoint.clone(),
            node_addr,
            limits.clone(),
            node_id,
            reserved_ips,
        ));
    }
}

async fn discovery_node_addr_with_reserved_ip_capacity(
    endpoint: &ZakuraEndpoint,
    candidate: &ZakuraDiscoveryDialCandidate,
    in_flight_by_ip: &HashMap<IpAddr, usize>,
) -> Option<(NodeAddr, Vec<IpAddr>)> {
    let mut direct_addrs = Vec::new();
    let mut reserved_ips = Vec::new();
    for addr in &candidate.direct_addrs {
        if can_accept_discovery_dial_ip(endpoint, addr.ip(), in_flight_by_ip).await {
            if !reserved_ips.contains(&addr.ip()) {
                reserved_ips.push(addr.ip());
            }
            direct_addrs.push(*addr);
        }
    }

    (!direct_addrs.is_empty()).then(|| {
        (
            NodeAddr::new(candidate.node_id).with_direct_addresses(direct_addrs),
            reserved_ips,
        )
    })
}

async fn can_accept_discovery_dial_ip(
    endpoint: &ZakuraEndpoint,
    remote_ip: IpAddr,
    in_flight_by_ip: &HashMap<IpAddr, usize>,
) -> bool {
    let in_flight = in_flight_by_ip.get(&remote_ip).copied().unwrap_or_default();
    endpoint
        .supervisor()
        .can_accept_remote_ip_with_in_flight(remote_ip, in_flight)
        .await
}

fn reserve_discovery_in_flight_ips(in_flight_by_ip: &mut HashMap<IpAddr, usize>, ips: &[IpAddr]) {
    for ip in ips {
        *in_flight_by_ip.entry(*ip).or_default() += 1;
    }
}

fn release_discovery_in_flight_ips(in_flight_by_ip: &mut HashMap<IpAddr, usize>, ips: &[IpAddr]) {
    for ip in ips {
        let Some(count) = in_flight_by_ip.get_mut(ip) else {
            continue;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            in_flight_by_ip.remove(ip);
        }
    }
}

async fn run_discovery_dial_once(
    endpoint: ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: ZakuraLocalLimits,
    node_id: NodeId,
    reserved_ips: Vec<IpAddr>,
) -> DiscoveryDialWorkerResult {
    let Ok(peer_id) = ZakuraPeerId::new(node_id.as_bytes().to_vec()) else {
        return DiscoveryDialWorkerResult {
            node_id,
            reserved_ips,
            result: DiscoveryDialResult::Failed,
        };
    };
    let mut registered = endpoint.supervisor().subscribe();
    let dial = tokio::spawn({
        let endpoint = endpoint.clone();
        async move { native_bootstrap_dial(&endpoint, node_addr, &limits).await }
    });
    tokio::pin!(dial);

    let result = loop {
        if registered
            .borrow_and_update()
            .iter()
            .any(|id| id == &peer_id)
        {
            break DiscoveryDialResult::Registered;
        }

        tokio::select! {
            dial_result = &mut dial => {
                break match dial_result {
                    // `native_bootstrap_dial` returns `Ok(())` only after the connection
                    // finishes; discovery success is the peer appearing in the registration watch.
                    Ok(Ok(())) => DiscoveryDialResult::Failed,
                    Ok(Err(ZakuraHandlerError::ResourceLimit(_))) => {
                        DiscoveryDialResult::LocalResourceLimit
                    }
                    Ok(Err(error)) => {
                        debug!(?error, "Zakura discovery dial failed");
                        DiscoveryDialResult::Failed
                    }
                    Err(error) => {
                        debug!(?error, "Zakura discovery dial task failed");
                        DiscoveryDialResult::Failed
                    }
                };
            }
            changed = registered.changed() => {
                if changed.is_err() {
                    break DiscoveryDialResult::Failed;
                }
            }
        }
    };

    DiscoveryDialWorkerResult {
        node_id,
        reserved_ips,
        result,
    }
}

async fn apply_discovery_dial_result(
    discovery: &ZakuraDiscoveryHandle,
    node_id: &NodeId,
    result: DiscoveryDialResult,
) {
    match result {
        DiscoveryDialResult::Registered => {
            discovery.mark_dial_success(node_id).await;
            metrics::counter!("zakura.p2p.discovery.dial.succeeded").increment(1);
        }
        DiscoveryDialResult::Failed => {
            discovery.mark_dial_failure(node_id).await;
            metrics::counter!("zakura.p2p.discovery.dial.failed").increment(1);
        }
        DiscoveryDialResult::LocalResourceLimit => {
            metrics::counter!("zakura.p2p.discovery.dial.local_resource_limit").increment(1);
        }
    }
}

/// Controls how [`native_dial_supervised`] retries and re-dials a peer.
#[derive(Clone, Copy, Debug)]
struct RedialPolicy {
    initial_backoff: Duration,
    max_backoff: Duration,
    /// Stop after this many consecutive failed attempts; `None` retries forever.
    max_attempts: Option<usize>,
    /// Re-dial again after a healthy connection drops. Configured bootstrap
    /// peers set this; the legacy->Zakura upgrade does not (the legacy crawler
    /// owns its longer-term recovery via the address-book keeper).
    redial_after_drop: bool,
}

impl RedialPolicy {
    /// Maintain a connection indefinitely, re-dialing on drop (bootstrap peers).
    fn maintain(initial_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            initial_backoff,
            max_backoff,
            max_attempts: None,
            redial_after_drop: true,
        }
    }

    /// Connect once, retrying only the initial dial up to `attempts` times
    /// (the legacy->Zakura upgrade hand-off).
    fn connect_once(initial_backoff: Duration, max_backoff: Duration, attempts: usize) -> Self {
        Self {
            initial_backoff,
            max_backoff,
            max_attempts: Some(attempts),
            redial_after_drop: false,
        }
    }
}

/// Outcome of one dial attempt, as seen by [`run_dial_supervisor`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum DialResult {
    /// Connected and served at least [`ZAKURA_REDIAL_HEALTHY_CONNECTION`].
    Healthy,
    /// Failed to establish, or served only briefly (e.g. a duplicate was closed).
    Failed,
}

/// Maintain a Zakura connection to `node_addr`, re-dialing with bounded backoff.
///
/// [`native_bootstrap_dial`] returns when the connection fails to establish or,
/// on success, when serving ends (the peer dropped or a duplicate was closed),
/// so a single loop covers both the initial connect — including the startup
/// race where the seed's endpoint is not yet listening — and reconnection after
/// a drop. The retry/backoff policy lives in [`run_dial_supervisor`]; this just
/// supplies the real dial attempt and the supervisor's registration watch.
async fn native_dial_supervised(
    endpoint: ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: ZakuraLocalLimits,
    policy: RedialPolicy,
) {
    let Ok(peer_id) = ZakuraPeerId::new(node_addr.node_id.as_bytes().to_vec()) else {
        warn!(?node_addr, "invalid Zakura bootstrap node id; not dialing");
        return;
    };

    let registered = endpoint.supervisor().subscribe();
    run_dial_supervisor(peer_id, registered, policy, move || {
        let endpoint = endpoint.clone();
        let node_addr = node_addr.clone();
        let limits = limits.clone();
        Box::pin(async move {
            let started = Instant::now();
            match native_bootstrap_dial(&endpoint, node_addr, &limits).await {
                Ok(()) if started.elapsed() >= ZAKURA_REDIAL_HEALTHY_CONNECTION => {
                    DialResult::Healthy
                }
                Ok(()) => DialResult::Failed,
                Err(error) => {
                    debug!(?error, "Zakura native dial failed; will retry");
                    DialResult::Failed
                }
            }
        }) as Pin<Box<dyn Future<Output = DialResult> + Send>>
    })
    .await;
}

/// Retry/backoff loop shared by configured bootstrap peers and the upgrade dial.
///
/// Before each dial it skips a peer that is already registered (it may have
/// dialed us first) so the two directions do not churn duplicate connections.
/// Exits when `policy.max_attempts` consecutive attempts fail, when a
/// `connect_once` peer connects or finishes serving, or when the supervisor's
/// registration watch closes (node shutdown). `dial` is injected so the loop is
/// unit-testable without real network I/O.
async fn run_dial_supervisor<F>(
    peer_id: ZakuraPeerId,
    mut registered: tokio::sync::watch::Receiver<Vec<ZakuraPeerId>>,
    policy: RedialPolicy,
    mut dial: F,
) where
    F: FnMut() -> Pin<Box<dyn Future<Output = DialResult> + Send>>,
{
    let mut backoff = policy.initial_backoff;
    let mut failures = 0usize;

    loop {
        if registered
            .borrow_and_update()
            .iter()
            .any(|id| id == &peer_id)
        {
            // Already connected (possibly an inbound dial from the same peer).
            if !policy.redial_after_drop {
                return;
            }
            // Wait for it to deregister, then re-dial promptly.
            if registered.changed().await.is_err() {
                return;
            }
            backoff = policy.initial_backoff;
            failures = 0;
            continue;
        }

        match dial().await {
            DialResult::Healthy => {
                if !policy.redial_after_drop {
                    return;
                }
                backoff = policy.initial_backoff;
                failures = 0;
                continue;
            }
            DialResult::Failed => {}
        }

        failures += 1;
        if policy.max_attempts.is_some_and(|max| failures >= max) {
            return;
        }

        // Back off, but wake early to re-dial the instant the peer (re)appears
        // in the supervisor, or to exit promptly on shutdown.
        tokio::select! {
            changed = registered.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = backoff.saturating_mul(2).min(policy.max_backoff);
    }
}

async fn native_bootstrap_dial(
    endpoint: &ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: &ZakuraLocalLimits,
) -> Result<(), ZakuraHandlerError> {
    let conn_id = endpoint
        .handler
        .next_conn_id
        .fetch_add(1, Ordering::Relaxed);
    let _admission = endpoint
        .handler
        .admission
        .clone()
        .try_acquire_owned()
        .map_err(|_| ZakuraHandlerError::ResourceLimit("admission"))?;
    let remote_ip = node_addr.direct_addresses().next().map(|addr| addr.ip());
    let connection = timeout(
        limits.control_timeout,
        endpoint.router.endpoint().connect(node_addr, P2P_V2_ALPN),
    )
    .await
    .map_err(|_| ZakuraHandlerError::Timeout("native dial"))??;
    let remote_node_id = connection.remote_node_id()?;
    let peer_id = ZakuraPeerId::new(remote_node_id.as_bytes().to_vec())?;
    let conn = ZakuraConnTrace::new(&endpoint.handler.trace, conn_id, &peer_id);
    let local_node_id = endpoint.router.endpoint().node_id();
    let local_peer_id = ZakuraPeerId::new(local_node_id.as_bytes().to_vec())?;
    let negotiated = {
        let _handshake = endpoint
            .handler
            .pending_handshakes
            .clone()
            .try_acquire_owned()
            .map_err(|_| ZakuraHandlerError::ResourceLimit("pending handshake"))?;
        run_native_initiator_handshake(
            &connection,
            limits,
            &endpoint.handler.handshake_config,
            &local_peer_id,
            &endpoint.handler.trace,
            &conn,
        )
        .await?
    };
    let conn_limits = limits.clamp(&negotiated);
    endpoint
        .handler
        .register_and_serve(
            connection,
            peer_id,
            remote_ip,
            ConnectionServeContext {
                limits: conn_limits,
                role: "initiator",
                direction: "outbound",
                conn,
            },
        )
        .await
}

#[cfg(any(test, feature = "zakura-testkit"))]
pub(crate) async fn run_native_initiator_handshake_without_trace(
    connection: &Connection,
    limits: &ZakuraLocalLimits,
    handshake_config: &ZakuraHandshakeConfig,
    local_peer_id: &ZakuraPeerId,
) -> Result<ZakuraAcceptedLimits, ZakuraHandlerError> {
    run_native_initiator_handshake(
        connection,
        limits,
        handshake_config,
        local_peer_id,
        &ZakuraTrace::noop(),
        &ZakuraConnTrace::placeholder(),
    )
    .await
}

async fn run_native_initiator_handshake(
    connection: &Connection,
    limits: &ZakuraLocalLimits,
    handshake_config: &ZakuraHandshakeConfig,
    local_peer_id: &ZakuraPeerId,
    trace: &ZakuraTrace,
    conn: &ZakuraConnTrace,
) -> Result<ZakuraAcceptedLimits, ZakuraHandlerError> {
    trace.emit(
        HANDSHAKE_TABLE,
        conn.event("control.started")
            .role("initiator")
            .phase("control")
            .network(handshake_config.network_label()),
    );
    let (mut send, mut recv) = timeout(limits.control_timeout, connection.open_bi())
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("open control stream"))??;
    let mut local_nonce = [0; 32];
    OsRng.fill_bytes(&mut local_nonce);

    let hello = ZakuraControlHello {
        magic: CONTROL_HELLO_MAGIC,
        control_version: CONTROL_VERSION,
        selected_zakura_protocol: ZAKURA_PROTOCOL_VERSION_1,
        handshake_path: ZakuraHandshakePath::Native,
        role: ZakuraControlRole::Initiator,
        network_id: handshake_config.network_id,
        chain_id: handshake_config.chain_id,
        iroh_node_id: local_peer_id.as_bytes().to_vec(),
        peer_nonce: local_nonce,
        initiator_upgrade_nonce: [0; 32],
        responder_upgrade_nonce: [0; 32],
        legacy_upgrade_transcript: [0; 32],
        capabilities: 0,
        required_channels: 0,
        initial_limits: limits.initial_limits(),
    };

    write_control_payload(&mut send, &hello.encode()?, limits.control_timeout).await?;
    let ack_bytes =
        read_control_payload(&mut recv, limits.max_frame_bytes, limits.control_timeout).await?;
    let ack = ZakuraControlAck::decode(&ack_bytes)?;
    ack.validate(
        ZAKURA_PROTOCOL_VERSION_1,
        local_nonce,
        ack.peer_nonce,
        &limits.initial_limits(),
        handshake_config,
    )?;
    trace.emit(
        HANDSHAKE_TABLE,
        conn.event("control.succeeded")
            .role("initiator")
            .phase("control")
            .selected_protocol(ack.selected_zakura_protocol)
            .network(handshake_config.network_label()),
    );
    Ok(ack.accepted_limits)
}

async fn stream_worker(
    mut send: SendStream,
    mut recv: RecvStream,
    prelude: StreamPrelude,
    context: StreamWorkerContext,
) {
    loop {
        tokio::select! {
            biased;
            _ = context.connection_token.cancelled() => break,
            frame = read_frame(&mut recv, context.limits.max_frame_bytes, context.limits.idle_timeout) => {
                match frame {
                    Ok(frame) => {
                        let _ = context.freshness_tx.send(Instant::now());
                        match admit_inbound_message(frame.payload.len(), &context, prelude.stream_kind) {
                            InboundMessageAdmission::Admit => {}
                            InboundMessageAdmission::Oversize => {
                                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_OVERSIZE));
                                break;
                            }
                            InboundMessageAdmission::Throttled => continue,
                        }
                        let queue_depth_limit = usize::from(context.limits.max_inbound_queue_depth);
                        let message = ZakuraInboundMessage {
                            peer_id: context.peer_id.clone(),
                            stream_kind: prelude.stream_kind,
                            frame,
                        };
                        if context.inbound_tx.send(message).await.is_err() {
                            break;
                        }
                        metrics::gauge!(
                            "zakura.p2p.queue.depth",
                            "stream_kind" => stream_kind_label(prelude.stream_kind),
                        )
                        .set(queue_depth_limit.saturating_sub(context.inbound_tx.capacity()) as f64);
                    }
                    Err(ZakuraHandlerError::Closed) => break,
                    Err(error) => {
                        if matches!(error, ZakuraHandlerError::Oversize) {
                            context.trace.emit(
                                RATELIMIT_TABLE,
                                context
                                    .event("frame.oversize")
                                    .stream_kind(stream_kind_label(prelude.stream_kind)),
                            );
                        }
                        debug!(?error, "closing Zakura stream worker");
                        let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                        break;
                    }
                }
            }
        }
    }
}

async fn request_stream_worker(
    mut send: SendStream,
    mut recv: RecvStream,
    prelude: StreamPrelude,
    context: StreamWorkerContext,
    inbound_sink: Arc<dyn InboundSink>,
) {
    let Some(request_id) = prelude.request_id else {
        let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
        context.connection_token.cancel();
        return;
    };

    let frame = tokio::select! {
        biased;
        _ = context.connection_token.cancelled() => return,
        frame = read_frame(&mut recv, context.limits.max_frame_bytes, context.limits.idle_timeout) => frame,
    };

    let frame = match frame {
        Ok(frame) => frame,
        Err(error) => {
            debug!(
                ?error,
                "closing Zakura request stream with invalid request frame"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            context.connection_token.cancel();
            return;
        }
    };

    let _ = context.freshness_tx.send(Instant::now());
    match admit_inbound_message(frame.payload.len(), &context, prelude.stream_kind) {
        InboundMessageAdmission::Admit => {}
        InboundMessageAdmission::Oversize => {
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_OVERSIZE));
            context.connection_token.cancel();
            return;
        }
        InboundMessageAdmission::Throttled => {
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RATE_LIMIT));
            return;
        }
    }

    let response_frames = match inbound_sink
        .request(
            context.peer_id.clone(),
            prelude.stream_kind,
            request_id,
            context.limits.max_frame_bytes,
            frame,
        )
        .await
    {
        Ok(frames) => frames,
        Err(InboundSinkReject::Protocol(error)) => {
            debug!(
                ?error,
                "Zakura inbound sink rejected protocol-invalid request"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            context.connection_token.cancel();
            return;
        }
        Err(InboundSinkReject::Local(error)) => {
            debug!(
                ?error,
                "Zakura inbound sink could not answer request locally"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
            return;
        }
    };

    for frame in response_frames {
        if let Err(error) = write_response_frame(&mut send, frame, context.limits).await {
            debug!(?error, "failed to write Zakura request response frame");
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            return;
        }
    }

    let _ = send.finish();
}

async fn discovery_exchange_loop(
    connection: Connection,
    peer_id: ZakuraPeerId,
    limits: ZakuraConnectionLimits,
    connection_token: CancellationToken,
    discovery: ZakuraDiscoveryHandle,
) {
    let refresh_interval = discovery
        .refresh_interval()
        .await
        .max(Duration::from_secs(1));
    loop {
        tokio::select! {
            biased;
            _ = connection_token.cancelled() => return,
            result = run_outbound_discovery_exchange(&connection, &peer_id, limits, &discovery) => {
                if let Err(error) = result {
                    debug!(?peer_id, ?error, "Zakura discovery exchange failed");
                    metrics::counter!("zakura.p2p.discovery.exchange.failed").increment(1);
                }
            }
        }

        tokio::select! {
            biased;
            _ = connection_token.cancelled() => return,
            _ = tokio::time::sleep(refresh_interval) => {}
        }
    }
}

async fn run_outbound_discovery_exchange(
    connection: &Connection,
    peer_id: &ZakuraPeerId,
    limits: ZakuraConnectionLimits,
    discovery: &ZakuraDiscoveryHandle,
) -> Result<(), BoxError> {
    let peer_node_id = peer_node_id(peer_id).ok_or_else(|| -> BoxError {
        "authenticated Zakura peer id is not an iroh node id".into()
    })?;
    let (mut send, mut recv) = timeout(DISCOVERY_EXCHANGE_TIMEOUT, connection.open_bi())
        .await
        .map_err(|_| -> BoxError { "Zakura discovery stream open timed out".into() })??;
    write_stream_prelude(
        &mut send,
        ZAKURA_STREAM_DISCOVERY,
        None,
        limits.max_frame_bytes,
        DISCOVERY_EXCHANGE_TIMEOUT,
    )
    .await?;

    write_discovery_message(
        &mut send,
        DiscoveryMessage::Hello {
            record: discovery.current_self_record().as_ref().clone(),
        },
        limits.max_frame_bytes,
    )
    .await?;

    let peer_hello = read_discovery_message(&mut recv, limits).await?;
    let peer_record = discovery_hello_record(peer_hello, peer_node_id)?;
    if let Err(error) = import_discovery_self_record(discovery, peer_record, peer_node_id).await {
        if is_advisory_self_record_import_error(&error) {
            debug!(
                ?peer_node_id,
                ?error,
                "ignoring unhelpful Zakura discovery peer hello record"
            );
        } else {
            return Err(Box::new(error));
        }
    }

    let limit = discovery
        .peer_sample_limit()
        .await
        .min(super::MAX_DISCOVERY_RECORDS_PER_RESPONSE);
    let exclude_node_ids = discovery.peer_sample_exclusions().await;
    write_discovery_message(
        &mut send,
        DiscoveryMessage::GetPeers {
            limit: u16::try_from(limit).expect("discovery sample limit fits in u16"),
            wanted_services: Vec::new(),
            exclude_node_ids,
        },
        limits.max_frame_bytes,
    )
    .await?;
    let _ = send.finish();

    match read_discovery_message(&mut recv, limits).await? {
        DiscoveryMessage::Peers { records } => {
            discovery
                .import_peer_records(records, Some(peer_node_id))
                .await;
            Ok(())
        }
        message => Err(format!("unexpected discovery response: {message:?}").into()),
    }
}

async fn discovery_stream_worker(
    mut send: SendStream,
    mut recv: RecvStream,
    prelude: StreamPrelude,
    context: StreamWorkerContext,
    discovery: ZakuraDiscoveryHandle,
) {
    let Some(peer_node_id) = peer_node_id(&context.peer_id) else {
        let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
        return;
    };
    let response_frame_cap = context.limits.max_frame_bytes.min(prelude.max_frame_bytes);

    let message = match read_admitted_discovery_message(&mut recv, &context).await {
        Ok(message) => message,
        Err(ZakuraHandlerError::Closed) => return,
        Err(error) => {
            debug!(
                ?peer_node_id,
                ?error,
                "closing Zakura discovery stream with invalid first message"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            return;
        }
    };

    let record = match discovery_hello_record(message, peer_node_id) {
        Ok(record) => record,
        Err(error) => {
            debug!(
                ?peer_node_id,
                ?error,
                "closing Zakura discovery stream whose first message was not a valid peer hello"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            return;
        }
    };
    if let Err(error) = import_discovery_self_record(&discovery, record, peer_node_id).await {
        if is_advisory_self_record_import_error(&error) {
            debug!(
                ?peer_node_id,
                ?error,
                "ignoring unhelpful Zakura discovery peer hello record"
            );
        } else {
            debug!(
                ?peer_node_id,
                ?error,
                "closing Zakura discovery stream with invalid peer hello"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            return;
        }
    }

    if let Err(error) = write_discovery_message(
        &mut send,
        DiscoveryMessage::Hello {
            record: discovery.current_self_record().as_ref().clone(),
        },
        response_frame_cap,
    )
    .await
    {
        debug!(?error, "failed to write Zakura discovery hello response");
        let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
        return;
    }

    let mut accepted_followup_hello = false;
    let mut imported_peer_batch = false;
    loop {
        let message = match read_admitted_discovery_message(&mut recv, &context).await {
            Ok(message) => message,
            Err(ZakuraHandlerError::Closed) => break,
            Err(error) => {
                debug!(
                    ?peer_node_id,
                    ?error,
                    "closing Zakura discovery stream with invalid message"
                );
                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                break;
            }
        };

        match message {
            DiscoveryMessage::Hello { record } => {
                if accepted_followup_hello {
                    debug!(
                        ?peer_node_id,
                        "closing Zakura discovery stream after extra peer hello"
                    );
                    let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                    break;
                }
                if record.body.node_id == peer_node_id {
                    if let Err(error) =
                        import_discovery_self_record(&discovery, record, peer_node_id).await
                    {
                        if is_advisory_self_record_import_error(&error) {
                            debug!(
                                ?peer_node_id,
                                ?error,
                                "ignoring unhelpful Zakura discovery peer hello record"
                            );
                        } else {
                            debug!(
                                ?peer_node_id,
                                ?error,
                                "closing Zakura discovery stream with invalid peer hello"
                            );
                            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                            break;
                        }
                    }
                    accepted_followup_hello = true;
                } else {
                    debug!(
                        ?peer_node_id,
                        "closing Zakura discovery stream with mismatched hello author"
                    );
                    let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                    break;
                }
            }
            DiscoveryMessage::GetPeers {
                limit,
                wanted_services,
                exclude_node_ids,
            } => {
                let records = discovery
                    .sample_peers(usize::from(limit), &wanted_services, &exclude_node_ids)
                    .await;
                if let Err(error) = write_discovery_message(
                    &mut send,
                    DiscoveryMessage::Peers { records },
                    response_frame_cap,
                )
                .await
                {
                    debug!(?error, "failed to write Zakura discovery peer sample");
                    let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                } else {
                    let _ = send.finish();
                }
                break;
            }
            DiscoveryMessage::Peers { records } => {
                if imported_peer_batch {
                    debug!(
                        ?peer_node_id,
                        "closing Zakura discovery stream after extra peer-record batch"
                    );
                    let _ = send.finish();
                    break;
                }
                discovery
                    .import_peer_records(records, Some(peer_node_id))
                    .await;
                imported_peer_batch = true;
            }
            DiscoveryMessage::GetServices { .. } | DiscoveryMessage::Services { .. } => {
                debug!(
                    ?peer_node_id,
                    "closing Zakura discovery stream with unsupported service-discovery message"
                );
                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                break;
            }
        }
    }
}

async fn read_admitted_discovery_message(
    recv: &mut RecvStream,
    context: &StreamWorkerContext,
) -> Result<DiscoveryMessage, ZakuraHandlerError> {
    let frame = read_frame(
        recv,
        context.limits.max_frame_bytes,
        context.limits.idle_timeout,
    )
    .await?;
    let _ = context.freshness_tx.send(Instant::now());
    match admit_inbound_message(frame.payload.len(), context, ZAKURA_STREAM_DISCOVERY) {
        InboundMessageAdmission::Admit => {}
        InboundMessageAdmission::Oversize => return Err(ZakuraHandlerError::Oversize),
        InboundMessageAdmission::Throttled => {
            return Err(ZakuraHandlerError::ResourceLimit("discovery message rate"))
        }
    }
    decode_discovery_frame(frame).map_err(ZakuraHandlerError::Discovery)
}

async fn read_discovery_message(
    recv: &mut RecvStream,
    limits: ZakuraConnectionLimits,
) -> Result<DiscoveryMessage, BoxError> {
    let frame = timeout(
        DISCOVERY_EXCHANGE_TIMEOUT,
        read_frame(recv, limits.max_frame_bytes, limits.idle_timeout),
    )
    .await
    .map_err(|_| -> BoxError { "Zakura discovery frame read timed out".into() })??;
    decode_discovery_frame(frame)
}

fn decode_discovery_frame(frame: Frame) -> Result<DiscoveryMessage, BoxError> {
    if frame.message_type != DISCOVERY_FRAME_MESSAGE_TYPE {
        return Err("Zakura discovery frame used unexpected message type".into());
    }
    if frame.flags != 0 {
        return Err("Zakura discovery frame used non-zero flags".into());
    }
    Ok(DiscoveryMessage::decode(&frame.payload)?)
}

async fn write_discovery_message(
    send: &mut SendStream,
    message: DiscoveryMessage,
    max_frame_bytes: u32,
) -> Result<(), BoxError> {
    let frame = Frame {
        message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
        flags: 0,
        payload: message.encode()?,
    };
    let bytes = frame.encode(max_frame_bytes)?;
    timeout(DISCOVERY_EXCHANGE_TIMEOUT, send.write_all(&bytes))
        .await
        .map_err(|_| -> BoxError { "Zakura discovery frame write timed out".into() })??;
    Ok(())
}

async fn write_stream_prelude(
    send: &mut SendStream,
    stream_kind: u16,
    request_id: Option<u64>,
    max_frame_bytes: u32,
    write_timeout: Duration,
) -> Result<(), BoxError> {
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind,
        stream_version: ZAKURA_STREAM_VERSION_1,
        request_id,
        max_frame_bytes,
    };
    let bytes = prelude.encode()?;
    timeout(write_timeout, send.write_all(&bytes))
        .await
        .map_err(|_| -> BoxError { "Zakura stream prelude write timed out".into() })??;
    Ok(())
}

fn discovery_hello_record(
    message: DiscoveryMessage,
    peer_node_id: NodeId,
) -> Result<ZakuraNodeRecord, BoxError> {
    let DiscoveryMessage::Hello { record } = message else {
        return Err("Zakura discovery stream expected hello".into());
    };
    if record.body.node_id != peer_node_id {
        return Err("Zakura discovery hello author did not match authenticated peer".into());
    }
    Ok(record)
}

async fn import_discovery_self_record(
    discovery: &ZakuraDiscoveryHandle,
    record: ZakuraNodeRecord,
    peer_node_id: NodeId,
) -> Result<(), DiscoveryBookError> {
    discovery
        .import_connected_peer_record(record, peer_node_id)
        .await
        .map(|_| ())
}

fn is_advisory_self_record_import_error(error: &DiscoveryBookError) -> bool {
    matches!(
        error,
        DiscoveryBookError::NoUsableDirectAddress
            | DiscoveryBookError::NonDialableDirectAddress { .. }
            | DiscoveryBookError::Record(
                DiscoveryRecordError::Expired | DiscoveryRecordError::FarFutureExpiry
            )
    )
}

fn peer_node_id(peer_id: &ZakuraPeerId) -> Option<NodeId> {
    let bytes: [u8; 32] = peer_id.as_bytes().try_into().ok()?;
    NodeId::from_bytes(&bytes).ok()
}

#[cfg(any(test, feature = "zakura-testkit"))]
async fn request_test_echo_status_routed(
    supervisor: &ZakuraSupervisorHandle,
    discovery: &ZakuraDiscoveryHandle,
    service: &ZakuraServiceId,
    payload: Vec<u8>,
) -> Result<ZakuraTestServiceResponse, BoxError> {
    // Live request routing can only use already-connected service advertisers.
    // Discovery dial candidates and helper-level fallback are reserved for a
    // future dialer-backed router; this path falls back over connected peers.
    let candidates = discovery.service_candidates(service, false, &[]).await;
    let handles = supervisor.outbound_peer_handles().await;
    let mut failed_service_candidates = Vec::new();

    for node_id in candidates.connected {
        let Some(handle) = select_handle_for_node_id(&handles, node_id) else {
            failed_service_candidates.push(node_id);
            continue;
        };

        match request_test_echo_status_one(handle, payload.clone()).await {
            Ok(payload) => {
                return Ok(ZakuraTestServiceResponse {
                    responder: node_id,
                    payload,
                    used_fallback: false,
                    failed_service_candidates,
                });
            }
            Err(error) => {
                debug!(
                    ?node_id,
                    ?error,
                    "Zakura test service candidate failed live request; falling back"
                );
                failed_service_candidates.push(node_id);
            }
        }
    }

    for handle in handles {
        let Some(node_id) = peer_node_id(handle.peer_id()) else {
            continue;
        };
        if failed_service_candidates.contains(&node_id) {
            continue;
        }

        if let Ok(payload) = request_test_echo_status_one(handle, payload.clone()).await {
            return Ok(ZakuraTestServiceResponse {
                responder: node_id,
                payload,
                used_fallback: true,
                failed_service_candidates,
            });
        }
    }

    Err("no ready Zakura peer answered the test echo/status request".into())
}

#[cfg(any(test, feature = "zakura-testkit"))]
fn select_handle_for_node_id(
    handles: &[ZakuraPeerHandle],
    node_id: NodeId,
) -> Option<ZakuraPeerHandle> {
    handles
        .iter()
        .find(|handle| peer_node_id(handle.peer_id()) == Some(node_id))
        .cloned()
}

#[cfg(any(test, feature = "zakura-testkit"))]
async fn request_test_echo_status_one(
    handle: ZakuraPeerHandle,
    payload: Vec<u8>,
) -> Result<Vec<u8>, BoxError> {
    let request_id = NEXT_TEST_SERVICE_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let frames = timeout(
        OUTBOUND_REQUEST_RESPONSE_TIMEOUT,
        handle.request(
            ZAKURA_STREAM_TEST_ECHO_STATUS,
            request_id,
            TEST_ECHO_STATUS_REQUEST,
            0,
            payload,
        ),
    )
    .await
    .map_err(|_| -> BoxError {
        format!(
            "Zakura test echo/status request timed out for peer {:?}",
            handle.peer_id()
        )
        .into()
    })??;

    let [frame] = frames.as_slice() else {
        return Err("test echo/status response returned an unexpected frame count".into());
    };
    let (_, payload) = test_echo_status_response_payload(&frame.payload)
        .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
    Ok(payload.to_vec())
}

#[cfg(any(test, feature = "zakura-testkit"))]
/// Prefix a test-only echo/status response with the request id for bounded validation.
pub fn encode_test_echo_status_response_payload(request_id: u64, payload: &[u8]) -> Vec<u8> {
    let mut response = Vec::with_capacity(LEGACY_RESPONSE_REQUEST_ID_BYTES + payload.len());
    response.extend_from_slice(&request_id.to_le_bytes());
    response.extend_from_slice(payload);
    response
}

#[cfg(any(test, feature = "zakura-testkit"))]
fn test_echo_status_response_payload(payload: &[u8]) -> Result<(u64, &[u8]), OutboundRequestError> {
    if payload.len() < LEGACY_RESPONSE_REQUEST_ID_BYTES {
        return Err(OutboundRequestError::Fatal(
            "truncated test echo/status response id".into(),
        ));
    }
    let mut id = [0; LEGACY_RESPONSE_REQUEST_ID_BYTES];
    id.copy_from_slice(&payload[..LEGACY_RESPONSE_REQUEST_ID_BYTES]);
    Ok((
        u64::from_le_bytes(id),
        &payload[LEGACY_RESPONSE_REQUEST_ID_BYTES..],
    ))
}

async fn inbound_message_sink(
    mut inbound_rx: mpsc::Receiver<ZakuraInboundMessage>,
    connection_token: CancellationToken,
    inbound_sink: Arc<dyn InboundSink>,
) {
    loop {
        tokio::select! {
            biased;
            _ = connection_token.cancelled() => break,
            message = inbound_rx.recv() => {
                let Some(message) = message else {
                    break;
                };
                let stream_kind = message.stream_kind;
                match inbound_sink.deliver(message.peer_id, stream_kind, message.frame) {
                    Ok(()) => {}
                    Err(InboundSinkReject::Protocol(error)) => {
                        debug!(?error, "Zakura inbound sink rejected protocol-invalid frame");
                        connection_token.cancel();
                        break;
                    }
                    Err(InboundSinkReject::Local(error)) => {
                        debug!(?error, "Zakura inbound sink could not accept frame locally");
                    }
                }
                metrics::gauge!(
                    "zakura.p2p.queue.depth",
                    "stream_kind" => stream_kind_label(stream_kind),
                )
                .set(inbound_rx.len() as f64);
            }
        }
    }
}

async fn freshness_reaper(mut freshness_rx: watch::Receiver<Instant>, idle_timeout: Duration) {
    loop {
        let last = *freshness_rx.borrow_and_update();
        let elapsed = last.elapsed();
        if elapsed >= idle_timeout {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(idle_timeout - elapsed) => return,
            changed = freshness_rx.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

async fn read_stream_prelude(
    recv: &mut RecvStream,
    prelude_timeout: Duration,
) -> Result<StreamPrelude, ZakuraHandlerError> {
    let mut fixed = [0; STREAM_PRELUDE_FIXED_BYTES];
    timeout(prelude_timeout, recv.read_exact(&mut fixed))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("stream prelude"))??;

    let mut fixed_reader = &fixed[..];
    let mut magic = [0; 4];
    fixed_reader.read_exact(&mut magic)?;
    if magic != STREAM_PRELUDE_MAGIC {
        return Err(ZakuraProtocolError::InvalidMagic.into());
    }
    let stream_kind = fixed_reader.read_u16::<LittleEndian>()?;
    let stream_version = fixed_reader.read_u16::<LittleEndian>()?;
    let request_id = match fixed[STREAM_PRELUDE_REQUEST_ID_FLAG_OFFSET] {
        0 => None,
        1 => {
            let mut bytes = [0; STREAM_PRELUDE_REQUEST_ID_BYTES];
            timeout(prelude_timeout, recv.read_exact(&mut bytes))
                .await
                .map_err(|_| ZakuraHandlerError::Timeout("stream request id"))??;
            let mut request_id_reader = &bytes[..];
            Some(request_id_reader.read_u64::<LittleEndian>()?)
        }
        flag => return Err(ZakuraProtocolError::InvalidFlag(flag).into()),
    };

    let mut cap = [0; STREAM_PRELUDE_CAP_BYTES];
    timeout(prelude_timeout, recv.read_exact(&mut cap))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("stream prelude cap"))??;
    let mut cap_reader = &cap[..];
    let max_frame_bytes = cap_reader.read_u32::<LittleEndian>()?;

    Ok(StreamPrelude {
        magic,
        stream_kind,
        stream_version,
        request_id,
        max_frame_bytes,
    })
}

async fn read_frame(
    recv: &mut RecvStream,
    max_frame_bytes: u32,
    read_timeout: Duration,
) -> Result<Frame, ZakuraHandlerError> {
    let mut header = [0; FRAME_HEADER_BYTES];
    match timeout(read_timeout, recv.read_exact(&mut header)).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return Err(ZakuraHandlerError::Closed),
        Err(_) => return Err(ZakuraHandlerError::Timeout("frame header")),
    }
    let mut reader = &header[..];
    let message_type = reader.read_u16::<LittleEndian>()?;
    let flags = reader.read_u16::<LittleEndian>()?;
    let payload_len = usize::try_from(reader.read_u32::<LittleEndian>()?)
        .expect("u32 payload lengths fit usize on supported targets");
    let max_frame_bytes =
        usize::try_from(max_frame_bytes).expect("u32 frame cap fits usize on supported targets");
    let frame_len = FRAME_HEADER_BYTES.saturating_add(payload_len);
    if frame_len > max_frame_bytes {
        metrics::counter!("zakura.p2p.ratelimit.frame.oversize").increment(1);
        return Err(ZakuraHandlerError::Oversize);
    }
    let mut payload = vec![0; payload_len];
    timeout(read_timeout, recv.read_exact(&mut payload))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("frame payload"))??;
    Ok(Frame {
        message_type,
        flags,
        payload,
    })
}

async fn read_control_payload(
    recv: &mut RecvStream,
    max_bytes: u32,
    read_timeout: Duration,
) -> Result<Vec<u8>, ZakuraHandlerError> {
    let mut len_bytes = [0; CONTROL_LENGTH_BYTES];
    timeout(read_timeout, recv.read_exact(&mut len_bytes))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("control length"))??;
    let len = (&len_bytes[..]).read_u32::<LittleEndian>()?;
    if len == 0 || len > max_bytes {
        return Err(ZakuraHandlerError::Oversize);
    }
    let mut bytes = vec![0; len as usize];
    timeout(read_timeout, recv.read_exact(&mut bytes))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("control payload"))??;
    Ok(bytes)
}

async fn write_control_payload(
    send: &mut SendStream,
    bytes: &[u8],
    write_timeout: Duration,
) -> Result<(), ZakuraHandlerError> {
    let mut len = Vec::with_capacity(CONTROL_LENGTH_BYTES);
    len.write_u32::<LittleEndian>(bytes.len() as u32)?;
    timeout(write_timeout, send.write_all(&len))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("control length write"))??;
    timeout(write_timeout, send.write_all(bytes))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("control payload write"))??;
    let _ = send.finish();
    Ok(())
}

async fn write_outbound_frame(
    connection: &Connection,
    limits: ZakuraConnectionLimits,
    stream_kind: u16,
    message_type: u16,
    flags: u16,
    payload: Vec<u8>,
) -> Result<(), BoxError> {
    let (mut send, _recv) = timeout(OUTBOUND_STREAM_WRITE_TIMEOUT, connection.open_bi())
        .await
        .map_err(|_| -> BoxError { "Zakura outbound stream open timed out".into() })??;
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind,
        stream_version: ZAKURA_STREAM_VERSION_1,
        request_id: None,
        max_frame_bytes: limits.max_frame_bytes,
    };
    let frame = Frame {
        message_type,
        flags,
        payload,
    };
    let prelude = prelude.encode()?;
    let frame = frame.encode(limits.max_frame_bytes)?;
    timeout(OUTBOUND_STREAM_WRITE_TIMEOUT, send.write_all(&prelude))
        .await
        .map_err(|_| -> BoxError { "Zakura outbound prelude write timed out".into() })??;
    timeout(OUTBOUND_STREAM_WRITE_TIMEOUT, send.write_all(&frame))
        .await
        .map_err(|_| -> BoxError { "Zakura outbound frame write timed out".into() })??;
    let _ = send.finish();
    Ok(())
}

async fn write_outbound_request_frame(
    connection: &Connection,
    limits: ZakuraConnectionLimits,
    stream_kind: u16,
    request_id: u64,
    message_type: u16,
    flags: u16,
    payload: Vec<u8>,
) -> Result<Vec<Frame>, OutboundRequestError> {
    timeout(
        OUTBOUND_REQUEST_RESPONSE_TIMEOUT,
        write_outbound_request_frame_inner(
            connection,
            limits,
            stream_kind,
            request_id,
            message_type,
            flags,
            payload,
        ),
    )
    .await
    .map_err(|_| OutboundRequestError::Local("Zakura outbound request/response timed out".into()))?
}

async fn write_outbound_request_frame_inner(
    connection: &Connection,
    limits: ZakuraConnectionLimits,
    stream_kind: u16,
    request_id: u64,
    message_type: u16,
    flags: u16,
    payload: Vec<u8>,
) -> Result<Vec<Frame>, OutboundRequestError> {
    let mut state = OutboundResponseReadState::from_request(
        stream_kind,
        request_id,
        message_type,
        &payload,
        limits,
    )?;
    let (mut send, mut recv) = timeout(OUTBOUND_STREAM_WRITE_TIMEOUT, connection.open_bi())
        .await
        .map_err(|_| -> BoxError { "Zakura outbound request stream open timed out".into() })
        .map_err(OutboundRequestError::Local)?
        .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind,
        stream_version: ZAKURA_STREAM_VERSION_1,
        request_id: Some(request_id),
        max_frame_bytes: limits.max_frame_bytes,
    };
    let frame = Frame {
        message_type,
        flags,
        payload,
    };
    let prelude = prelude.encode().map_err(|error| {
        OutboundRequestError::Local(BoxError::from(format!("failed to encode prelude: {error}")))
    })?;
    let frame = frame
        .encode(limits.max_frame_bytes)
        .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
    timeout(OUTBOUND_STREAM_WRITE_TIMEOUT, send.write_all(&prelude))
        .await
        .map_err(|_| -> BoxError { "Zakura outbound request prelude write timed out".into() })
        .map_err(OutboundRequestError::Local)?
        .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
    timeout(OUTBOUND_STREAM_WRITE_TIMEOUT, send.write_all(&frame))
        .await
        .map_err(|_| -> BoxError { "Zakura outbound request frame write timed out".into() })
        .map_err(OutboundRequestError::Local)?
        .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
    let _ = send.finish();

    let mut frames = Vec::new();
    loop {
        match read_frame(&mut recv, limits.max_frame_bytes, limits.idle_timeout).await {
            Ok(frame) => {
                state.validate_frame(&frame)?;
                frames.push(frame);
            }
            Err(ZakuraHandlerError::Closed) => {
                state.finish()?;
                return Ok(frames);
            }
            Err(ZakuraHandlerError::Timeout(_)) => {
                return Err(OutboundRequestError::Local(Box::new(
                    ZakuraHandlerError::Timeout("outbound response"),
                )));
            }
            Err(ZakuraHandlerError::Oversize) => {
                return Err(OutboundRequestError::Fatal(Box::new(
                    ZakuraHandlerError::Oversize,
                )));
            }
            Err(error) => return Err(OutboundRequestError::Fatal(Box::new(error))),
        }
    }
}

#[derive(Debug)]
enum OutboundRequestError {
    Local(BoxError),
    Fatal(BoxError),
}

#[derive(Debug)]
enum OutboundResponseReadState {
    Legacy {
        request_id: u64,
        state: LegacyResponseReadState,
    },
    #[cfg(any(test, feature = "zakura-testkit"))]
    TestEchoStatus(TestEchoStatusResponseReadState),
}

impl OutboundResponseReadState {
    fn from_request(
        stream_kind: u16,
        request_id: u64,
        message_type: u16,
        payload: &[u8],
        limits: ZakuraConnectionLimits,
    ) -> Result<Self, OutboundRequestError> {
        match stream_kind {
            LEGACY_REQUEST_STREAM_KIND => Ok(Self::Legacy {
                request_id,
                state: LegacyResponseReadState::new(LegacyResponseBudget::from_request(
                    message_type,
                    payload,
                    limits,
                )?),
            }),
            #[cfg(any(test, feature = "zakura-testkit"))]
            ZAKURA_STREAM_TEST_ECHO_STATUS => Ok(Self::TestEchoStatus(
                TestEchoStatusResponseReadState::new(request_id, message_type, limits)?,
            )),
            _ => Err(OutboundRequestError::Local(
                format!("unsupported Zakura request stream kind: {stream_kind}").into(),
            )),
        }
    }

    fn validate_frame(&mut self, frame: &Frame) -> Result<(), OutboundRequestError> {
        match self {
            Self::Legacy { request_id, state } => state.validate_frame(*request_id, frame),
            #[cfg(any(test, feature = "zakura-testkit"))]
            Self::TestEchoStatus(state) => state.validate_frame(frame),
        }
    }

    fn finish(self) -> Result<(), OutboundRequestError> {
        match self {
            Self::Legacy { state, .. } => state.finish(),
            #[cfg(any(test, feature = "zakura-testkit"))]
            Self::TestEchoStatus(state) => state.finish(),
        }
    }
}

#[cfg(any(test, feature = "zakura-testkit"))]
#[derive(Debug)]
struct TestEchoStatusResponseReadState {
    request_id: u64,
    frames: usize,
    max_message_bytes: usize,
}

#[cfg(any(test, feature = "zakura-testkit"))]
impl TestEchoStatusResponseReadState {
    fn new(
        request_id: u64,
        message_type: u16,
        limits: ZakuraConnectionLimits,
    ) -> Result<Self, OutboundRequestError> {
        if message_type != TEST_ECHO_STATUS_REQUEST {
            return Err(OutboundRequestError::Local(
                format!("unsupported test echo/status request type: {message_type}").into(),
            ));
        }

        let max_message_bytes = usize::try_from(limits.max_message_bytes)
            .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
        Ok(Self {
            request_id,
            frames: 0,
            max_message_bytes,
        })
    }

    fn validate_frame(&mut self, frame: &Frame) -> Result<(), OutboundRequestError> {
        if self.frames != 0 {
            return Err(OutboundRequestError::Fatal(
                "test echo/status response sent too many frames".into(),
            ));
        }
        self.frames += 1;

        if frame.message_type != TEST_ECHO_STATUS_RESPONSE {
            return Err(OutboundRequestError::Fatal(
                format!(
                    "test echo/status response used wrong message type: {}",
                    frame.message_type
                )
                .into(),
            ));
        }
        if frame.flags != 0 {
            return Err(OutboundRequestError::Fatal(
                "test echo/status response used non-zero flags".into(),
            ));
        }
        if frame.payload.len() > self.max_message_bytes {
            return Err(OutboundRequestError::Fatal(
                "test echo/status response exceeded message byte cap".into(),
            ));
        }
        let (response_id, _) = test_echo_status_response_payload(&frame.payload)?;
        if response_id != self.request_id {
            return Err(OutboundRequestError::Fatal(
                format!(
                    "wrong test echo/status response request id: expected {}, got {}",
                    self.request_id, response_id
                )
                .into(),
            ));
        }
        Ok(())
    }

    fn finish(self) -> Result<(), OutboundRequestError> {
        if self.frames == 0 {
            return Err(OutboundRequestError::Fatal(
                "test echo/status response sent no frames".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum LegacyResponseKind {
    Blocks,
    Transactions,
    BlockHashes,
    BlockHeaders,
    TransactionIds,
    Pong,
    Nil,
}

#[derive(Copy, Clone, Debug)]
struct LegacyResponseBudget {
    kind: LegacyResponseKind,
    max_items: usize,
    max_frames: usize,
    max_bytes: usize,
    max_message_bytes: usize,
}

impl LegacyResponseBudget {
    fn from_request(
        message_type: u16,
        payload: &[u8],
        limits: ZakuraConnectionLimits,
    ) -> Result<Self, OutboundRequestError> {
        let kind = match message_type {
            LEGACY_REQUEST_BLOCKS_BY_HASH => LegacyResponseKind::Blocks,
            LEGACY_REQUEST_TRANSACTIONS_BY_ID => LegacyResponseKind::Transactions,
            LEGACY_REQUEST_FIND_BLOCKS => LegacyResponseKind::BlockHashes,
            LEGACY_REQUEST_FIND_HEADERS => LegacyResponseKind::BlockHeaders,
            LEGACY_REQUEST_MEMPOOL_TRANSACTION_IDS => LegacyResponseKind::TransactionIds,
            LEGACY_REQUEST_PING => LegacyResponseKind::Pong,
            LEGACY_REQUEST_PUSH_TRANSACTION => LegacyResponseKind::Nil,
            _ => {
                return Err(OutboundRequestError::Local(
                    format!("unsupported legacy request message type: {message_type}").into(),
                ));
            }
        };
        let max_message_bytes = usize::try_from(limits.max_message_bytes)
            .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
        let max_inventory_items = usize::try_from(MAX_TX_INV_IN_SENT_MESSAGE)
            .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
        let (max_items, max_frames, max_bytes) = match kind {
            LegacyResponseKind::Blocks | LegacyResponseKind::Transactions => {
                let item_count = legacy_inventory_count(payload, kind)?;
                let max_frames = item_count
                    .saturating_mul(LEGACY_RESPONSE_MAX_FRAMES_PER_ITEM)
                    .saturating_add(1);
                let item_bytes = match kind {
                    LegacyResponseKind::Blocks => LEGACY_BLOCK_HASH_BYTES,
                    LegacyResponseKind::Transactions => LEGACY_INVENTORY_HASH_BYTES,
                    _ => unreachable!("matched inventory response kind"),
                };
                let missing_bytes = LEGACY_RESPONSE_REQUEST_ID_BYTES
                    .saturating_add(LEGACY_COMPACT_SIZE_PREFIX_BYTES)
                    .saturating_add(item_count.saturating_mul(item_bytes));
                let max_bytes = item_count
                    .saturating_mul(max_message_bytes)
                    .saturating_add(missing_bytes)
                    .max(LEGACY_RESPONSE_CHUNK_HEADER_BYTES);
                (item_count, max_frames, max_bytes)
            }
            LegacyResponseKind::BlockHashes => {
                let max_bytes = LEGACY_RESPONSE_REQUEST_ID_BYTES
                    .saturating_add(LEGACY_COMPACT_SIZE_PREFIX_BYTES)
                    .saturating_add(max_inventory_items.saturating_mul(LEGACY_BLOCK_HASH_BYTES));
                (max_inventory_items, 1, max_bytes)
            }
            LegacyResponseKind::BlockHeaders => (
                MAX_HEADERS_PER_MESSAGE,
                1,
                LEGACY_RESPONSE_REQUEST_ID_BYTES.saturating_add(max_message_bytes),
            ),
            LegacyResponseKind::TransactionIds => {
                let max_bytes = LEGACY_RESPONSE_REQUEST_ID_BYTES
                    .saturating_add(LEGACY_COMPACT_SIZE_PREFIX_BYTES)
                    .saturating_add(
                        max_inventory_items.saturating_mul(LEGACY_INVENTORY_HASH_BYTES),
                    );
                (max_inventory_items, 1, max_bytes)
            }
            LegacyResponseKind::Pong | LegacyResponseKind::Nil => {
                (1, 1, LEGACY_RESPONSE_REQUEST_ID_BYTES)
            }
        };

        Ok(Self {
            kind,
            max_items,
            max_frames,
            max_bytes,
            max_message_bytes,
        })
    }
}

#[derive(Debug)]
struct LegacyResponseReadState {
    budget: LegacyResponseBudget,
    frames: usize,
    bytes: usize,
    items: usize,
    active_chunk_type: Option<u16>,
    active_chunk: Vec<u8>,
}

impl LegacyResponseReadState {
    fn new(budget: LegacyResponseBudget) -> Self {
        Self {
            budget,
            frames: 0,
            bytes: 0,
            items: 0,
            active_chunk_type: None,
            active_chunk: Vec::new(),
        }
    }

    fn validate_frame(
        &mut self,
        request_id: u64,
        frame: &Frame,
    ) -> Result<(), OutboundRequestError> {
        if frame.flags != 0 {
            return Err(OutboundRequestError::Fatal(
                format!("unsupported legacy response flags: {}", frame.flags).into(),
            ));
        }

        self.frames = self.frames.saturating_add(1);
        if self.frames > self.budget.max_frames {
            return Err(OutboundRequestError::Fatal(
                "too many legacy response frames".into(),
            ));
        }

        self.bytes = self.bytes.saturating_add(frame.payload.len());
        if self.bytes > self.budget.max_bytes {
            return Err(OutboundRequestError::Fatal(
                "legacy response exceeded cumulative byte budget".into(),
            ));
        }

        match frame.message_type {
            LEGACY_RESPONSE_BLOCK => {
                self.validate_available_chunk(request_id, frame, LegacyResponseKind::Blocks)
            }
            LEGACY_RESPONSE_TRANSACTION => {
                self.validate_available_chunk(request_id, frame, LegacyResponseKind::Transactions)
            }
            LEGACY_RESPONSE_MISSING_BLOCKS => {
                self.validate_missing(request_id, &frame.payload, LegacyResponseKind::Blocks)
            }
            LEGACY_RESPONSE_MISSING_TRANSACTIONS => {
                self.validate_missing(request_id, &frame.payload, LegacyResponseKind::Transactions)
            }
            LEGACY_RESPONSE_BLOCK_HASHES => self.validate_id_prefixed_hashes(
                request_id,
                &frame.payload,
                LegacyResponseKind::BlockHashes,
            ),
            LEGACY_RESPONSE_BLOCK_HEADERS => self.validate_headers(request_id, &frame.payload),
            LEGACY_RESPONSE_TRANSACTION_IDS => self.validate_id_prefixed_hashes(
                request_id,
                &frame.payload,
                LegacyResponseKind::TransactionIds,
            ),
            LEGACY_RESPONSE_PONG => {
                self.validate_id_only(request_id, &frame.payload, LegacyResponseKind::Pong)
            }
            LEGACY_RESPONSE_NIL => self.validate_nil(request_id, &frame.payload),
            message_type => Err(OutboundRequestError::Fatal(
                format!("unknown legacy response message type: {message_type}").into(),
            )),
        }
    }

    fn finish(self) -> Result<(), OutboundRequestError> {
        if self.active_chunk_type.is_some() {
            return Err(OutboundRequestError::Fatal(
                "incomplete legacy response chunk".into(),
            ));
        }
        Ok(())
    }

    fn validate_available_chunk(
        &mut self,
        request_id: u64,
        frame: &Frame,
        kind: LegacyResponseKind,
    ) -> Result<(), OutboundRequestError> {
        if self.budget.kind != kind {
            return Err(OutboundRequestError::Fatal(
                "legacy response kind does not match request".into(),
            ));
        }

        let (response_id, is_last, bytes) = legacy_response_chunk_header(&frame.payload)?;
        if response_id != request_id {
            return Err(OutboundRequestError::Fatal(
                format!(
                    "wrong legacy response request id: expected {request_id}, got {response_id}"
                )
                .into(),
            ));
        }

        match self.active_chunk_type {
            Some(active) if active != frame.message_type => {
                return Err(OutboundRequestError::Fatal(
                    "interleaved legacy response chunks".into(),
                ));
            }
            None => self.active_chunk_type = Some(frame.message_type),
            _ => {}
        }

        let new_len = self.active_chunk.len().saturating_add(bytes.len());
        if new_len > self.budget.max_message_bytes {
            return Err(OutboundRequestError::Fatal(
                "legacy response item exceeded message byte cap".into(),
            ));
        }
        self.active_chunk.extend_from_slice(bytes);

        if is_last {
            self.validate_completed_item(kind)?;
            self.active_chunk_type = None;
            self.active_chunk.clear();
            self.add_items(1)?;
        }

        Ok(())
    }

    fn validate_completed_item(
        &self,
        kind: LegacyResponseKind,
    ) -> Result<(), OutboundRequestError> {
        match kind {
            LegacyResponseKind::Blocks => {
                Block::zcash_deserialize(&mut Cursor::new(self.active_chunk.as_slice()))
                    .map(|_| ())
                    .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))
            }
            LegacyResponseKind::Transactions => {
                Transaction::zcash_deserialize(&mut Cursor::new(self.active_chunk.as_slice()))
                    .map(|_| ())
                    .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))
            }
            LegacyResponseKind::BlockHashes
            | LegacyResponseKind::BlockHeaders
            | LegacyResponseKind::TransactionIds
            | LegacyResponseKind::Pong
            | LegacyResponseKind::Nil => unreachable!("non-chunk legacy response kind"),
        }
    }

    fn validate_missing(
        &mut self,
        request_id: u64,
        payload: &[u8],
        kind: LegacyResponseKind,
    ) -> Result<(), OutboundRequestError> {
        let payload = self.begin_id_prefixed_response(kind, request_id, payload, "missing")?;
        let count = legacy_inventory_count(payload, kind)?;
        self.add_items(count)
    }

    fn validate_id_prefixed_hashes(
        &mut self,
        request_id: u64,
        payload: &[u8],
        kind: LegacyResponseKind,
    ) -> Result<(), OutboundRequestError> {
        let payload = self.begin_id_prefixed_response(kind, request_id, payload, "list")?;
        let count = legacy_inventory_count(payload, kind)?;
        self.add_items(count)
    }

    fn validate_headers(
        &mut self,
        request_id: u64,
        payload: &[u8],
    ) -> Result<(), OutboundRequestError> {
        let payload = self.begin_id_prefixed_response(
            LegacyResponseKind::BlockHeaders,
            request_id,
            payload,
            "headers",
        )?;
        let count = legacy_header_count(payload)?;
        self.add_items(count)
    }

    fn validate_id_only(
        &mut self,
        request_id: u64,
        payload: &[u8],
        kind: LegacyResponseKind,
    ) -> Result<(), OutboundRequestError> {
        let payload =
            self.begin_id_prefixed_response(kind, request_id, payload, "acknowledgement")?;
        if !payload.is_empty() {
            return Err(OutboundRequestError::Fatal(
                "legacy acknowledgement response has trailing bytes".into(),
            ));
        }
        self.add_items(1)
    }

    /// Accept a `MSG_RESPONSE_NIL` empty-result sentinel for any request kind.
    ///
    /// The inbound service answers an empty `FindBlocks`/`FindHeaders`/
    /// `MempoolTransactionIds` (and a queued `PushTransaction`) with
    /// `Response::Nil`, so a lone nil frame is a valid empty response for every
    /// request kind, not just `PushTransaction`. The kind-specific empty
    /// `Response` is produced later by `LegacyResponseCodec::decode_response`.
    fn validate_nil(
        &mut self,
        request_id: u64,
        payload: &[u8],
    ) -> Result<(), OutboundRequestError> {
        if self.active_chunk_type.is_some() {
            return Err(OutboundRequestError::Fatal(
                "legacy nil response interleaved with response chunk".into(),
            ));
        }
        let (response_id, payload) = legacy_response_id(payload)?;
        if response_id != request_id {
            return Err(OutboundRequestError::Fatal(
                format!(
                    "wrong legacy nil response request id: expected {request_id}, got {response_id}"
                )
                .into(),
            ));
        }
        if !payload.is_empty() {
            return Err(OutboundRequestError::Fatal(
                "legacy nil response has trailing bytes".into(),
            ));
        }
        self.add_items(1)
    }

    fn begin_id_prefixed_response<'a>(
        &self,
        kind: LegacyResponseKind,
        request_id: u64,
        payload: &'a [u8],
        label: &'static str,
    ) -> Result<&'a [u8], OutboundRequestError> {
        if self.budget.kind != kind {
            return Err(OutboundRequestError::Fatal(
                format!("legacy {label} response kind does not match request").into(),
            ));
        }
        if self.active_chunk_type.is_some() {
            return Err(OutboundRequestError::Fatal(
                format!("legacy {label} response interleaved with response chunk").into(),
            ));
        }
        let (response_id, payload) = legacy_response_id(payload)?;
        if response_id != request_id {
            return Err(OutboundRequestError::Fatal(
                format!(
                    "wrong legacy {label} response request id: expected {request_id}, got {response_id}"
                )
                    .into(),
            ));
        }
        Ok(payload)
    }

    fn add_items(&mut self, count: usize) -> Result<(), OutboundRequestError> {
        self.items = self.items.saturating_add(count);
        if self.items > self.budget.max_items {
            return Err(OutboundRequestError::Fatal(
                "legacy response contained more items than requested".into(),
            ));
        }
        Ok(())
    }
}

fn legacy_inventory_count(
    payload: &[u8],
    kind: LegacyResponseKind,
) -> Result<usize, OutboundRequestError> {
    let mut reader = Cursor::new(payload);
    let count = usize::from(
        CompactSizeMessage::zcash_deserialize(&mut reader)
            .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?,
    );

    match kind {
        LegacyResponseKind::Blocks | LegacyResponseKind::BlockHashes => {
            let consumed = usize::try_from(reader.position())
                .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?;
            let expected_len =
                consumed.saturating_add(count.saturating_mul(LEGACY_BLOCK_HASH_BYTES));
            if expected_len != payload.len() {
                return Err(OutboundRequestError::Fatal(
                    "legacy block inventory has trailing or truncated bytes".into(),
                ));
            }
        }
        LegacyResponseKind::Transactions | LegacyResponseKind::TransactionIds => {
            for _ in 0..count {
                let inventory = InventoryHash::zcash_deserialize(&mut reader)
                    .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?;
                if inventory.unmined_tx_id().is_none() {
                    return Err(OutboundRequestError::Fatal(
                        "legacy transaction inventory contained a non-transaction item".into(),
                    ));
                }
            }
            reject_legacy_inventory_trailing(payload, &reader)?;
        }
        LegacyResponseKind::BlockHeaders | LegacyResponseKind::Pong | LegacyResponseKind::Nil => {
            return Err(OutboundRequestError::Local(
                "legacy response kind does not use inventory counts".into(),
            ));
        }
    }

    Ok(count)
}

fn legacy_header_count(payload: &[u8]) -> Result<usize, OutboundRequestError> {
    let mut reader = Cursor::new(payload);
    let count = usize::from(
        CompactSizeMessage::zcash_deserialize(&mut reader)
            .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?,
    );
    if count > MAX_HEADERS_PER_MESSAGE {
        return Err(OutboundRequestError::Fatal(
            "legacy headers response exceeded header count cap".into(),
        ));
    }
    for _ in 0..count {
        CountedHeader::zcash_deserialize(&mut reader)
            .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?;
    }
    reject_legacy_inventory_trailing(payload, &reader)?;
    Ok(count)
}

fn reject_legacy_inventory_trailing(
    payload: &[u8],
    reader: &Cursor<&[u8]>,
) -> Result<(), OutboundRequestError> {
    if usize::try_from(reader.position())
        .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?
        != payload.len()
    {
        return Err(OutboundRequestError::Fatal(
            "legacy transaction inventory has trailing bytes".into(),
        ));
    }
    Ok(())
}

fn legacy_response_id(payload: &[u8]) -> Result<(u64, &[u8]), OutboundRequestError> {
    if payload.len() < LEGACY_RESPONSE_REQUEST_ID_BYTES {
        return Err(OutboundRequestError::Fatal(
            "truncated legacy response id".into(),
        ));
    }
    let mut id = [0; LEGACY_RESPONSE_REQUEST_ID_BYTES];
    id.copy_from_slice(&payload[..LEGACY_RESPONSE_REQUEST_ID_BYTES]);
    Ok((
        u64::from_le_bytes(id),
        &payload[LEGACY_RESPONSE_REQUEST_ID_BYTES..],
    ))
}

fn legacy_response_chunk_header(
    payload: &[u8],
) -> Result<(u64, bool, &[u8]), OutboundRequestError> {
    if payload.len() < LEGACY_RESPONSE_CHUNK_HEADER_BYTES {
        return Err(OutboundRequestError::Fatal(
            "truncated legacy response chunk".into(),
        ));
    }
    let (request_id, payload) = legacy_response_id(payload)?;
    Ok((request_id, payload[0] != 0, &payload[1..]))
}

async fn write_response_frame(
    send: &mut SendStream,
    frame: Frame,
    limits: ZakuraConnectionLimits,
) -> Result<(), ZakuraHandlerError> {
    if frame.payload.len() > limits.max_message_bytes as usize {
        return Err(ZakuraHandlerError::Oversize);
    }
    let frame = frame.encode(limits.max_frame_bytes)?;
    timeout(OUTBOUND_STREAM_WRITE_TIMEOUT, send.write_all(&frame))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("frame write"))??;
    Ok(())
}

fn parse_bootstrap_peer(entry: &str) -> Result<NodeAddr, ZakuraHandlerError> {
    let Some((node_id, direct_addr)) = entry.split_once('@') else {
        return Err(ZakuraHandlerError::InvalidBootstrapPeer);
    };
    let node_id =
        NodeId::from_str(node_id).map_err(|_| ZakuraHandlerError::InvalidBootstrapPeer)?;
    let direct_addr = direct_addr
        .parse::<SocketAddr>()
        .map_err(|_| ZakuraHandlerError::InvalidBootstrapPeer)?;
    Ok(NodeAddr::new(node_id).with_direct_addresses([direct_addr]))
}

fn parse_bootstrap_peers(entries: &[String]) -> Vec<NodeAddr> {
    entries
        .iter()
        .filter_map(|entry| match parse_bootstrap_peer(entry) {
            Ok(node_addr) => Some(node_addr),
            Err(error) => {
                warn!(?error, ?entry, "invalid Zakura bootstrap peer");
                None
            }
        })
        .collect()
}

fn effective_discovery_connection_headroom(bootstrap_peer_count: usize) -> usize {
    DEFAULT_DISCOVERY_CONNECTION_HEADROOM.max(bootstrap_peer_count)
}

async fn insert_static_bootstrap_candidates(
    discovery: &ZakuraDiscoveryHandle,
    bootstrap_peers: &[NodeAddr],
) {
    for node_addr in bootstrap_peers {
        if let Err(error) = discovery.insert_static_candidate(node_addr.clone()).await {
            warn!(
                ?error,
                ?node_addr,
                "invalid Zakura static bootstrap discovery candidate"
            );
        }
    }
}

fn default_advertised_services() -> Vec<ZakuraServiceId> {
    vec![
        ZakuraServiceId::discovery(),
        ZakuraServiceId::legacy_gossip(),
        ZakuraServiceId::legacy_requests(),
        ZakuraServiceId::service_discovery(),
    ]
}

fn validate_idle_invariant(limits: &ZakuraLocalLimits) -> Result<(), ZakuraHandlerError> {
    if limits.keep_alive_interval >= limits.quic_idle_timeout {
        return Err(ZakuraHandlerError::InvalidLocalLimits);
    }
    if limits.initial_limits().idle_timeout_millis as u128 >= limits.quic_idle_timeout.as_millis() {
        return Err(ZakuraHandlerError::InvalidLocalLimits);
    }
    Ok(())
}

fn zakura_secret_key(config: &Config) -> Result<SecretKey, ZakuraHandlerError> {
    if let Some(secret) = &config.zakura_node_secret_key {
        return SecretKey::from_str(secret.expose_secret())
            .map_err(|_| ZakuraHandlerError::InvalidSecretKey);
    }

    Ok(SecretKey::generate(OsRng))
}

fn stream_kind_label(stream_kind: u16) -> &'static str {
    match stream_kind {
        0 => "control",
        1 => "request",
        LEGACY_GOSSIP_STREAM_KIND => "gossip",
        LEGACY_REQUEST_STREAM_KIND => "legacy_request",
        ZAKURA_STREAM_DISCOVERY => "discovery",
        #[cfg(any(test, feature = "zakura-testkit"))]
        ZAKURA_STREAM_TEST_ECHO_STATUS => "test_echo_status",
        _ => "unknown",
    }
}

/// The only stream-kind version this v1 handler serves. Every known kind is
/// at version 1; a peer naming any other version of a known kind is rejected.
const ZAKURA_STREAM_VERSION_1: u16 = 1;

/// Returns whether the handler can serve a stream with this kind and version.
///
/// A peer controls both fields of the prelude, so an unknown kind or an
/// unsupported version of a known kind must be rejected before the stream
/// consumes a worker, a stream permit, queue depth, or rate budget. Keeping
/// this in one place means [`stream_kind_label`] (used for metrics/trace) and
/// admission agree on what "known" means.
fn is_supported_stream(stream_kind: u16, stream_version: u16) -> bool {
    let known_kind = stream_kind <= LEGACY_REQUEST_STREAM_KIND
        || stream_kind == ZAKURA_STREAM_DISCOVERY
        || is_test_service_stream_kind(stream_kind);
    known_kind && stream_version == ZAKURA_STREAM_VERSION_1
}

fn is_request_stream_kind(stream_kind: u16) -> bool {
    stream_kind == LEGACY_REQUEST_STREAM_KIND || is_test_service_stream_kind(stream_kind)
}

fn is_test_service_stream_kind(_stream_kind: u16) -> bool {
    #[cfg(any(test, feature = "zakura-testkit"))]
    {
        _stream_kind == ZAKURA_STREAM_TEST_ECHO_STATUS
    }
    #[cfg(not(any(test, feature = "zakura-testkit")))]
    {
        false
    }
}

/// One message-rate [`TokenBucket`] shared by every stream worker serving the
/// same stream kind on one connection.
///
/// A worker holds this briefly (no `.await` while locked) to spend one token
/// per decoded frame, so N concurrent same-kind streams draw from a single
/// per-connection budget instead of N independent ones (FLUP-014).
type SharedMessageBucket<C = RealClock> = Arc<std::sync::Mutex<TokenBucket<C>>>;

/// Per-connection collection of message-rate buckets keyed by validated stream
/// kind. Created lazily (FLUP-015 rejects unknown kinds before this point, so
/// every key here is a known kind) and shared across that connection's workers.
type MessageRateBuckets<C = RealClock> = HashMap<u16, SharedMessageBucket<C>>;

/// Returns the shared message-rate bucket for `stream_kind` on this connection,
/// creating it sized from `message_rate_per_second` on first use.
///
/// Two workers serving the same kind get [`Arc::clone`]s of the same bucket, so
/// their `try_take` calls draw from one budget (FLUP-014). Generic over the
/// clock so tests can drive deterministic refills with a `TestClock`.
fn message_bucket_for<C: Clock>(
    buckets: &mut MessageRateBuckets<C>,
    stream_kind: u16,
    message_rate_per_second: u32,
    clock: C,
) -> SharedMessageBucket<C> {
    buckets
        .entry(stream_kind)
        .or_insert_with(|| {
            Arc::new(std::sync::Mutex::new(TokenBucket::with_clock(
                message_rate_per_second,
                clock,
            )))
        })
        .clone()
}

#[derive(Clone, Debug)]
struct TokenBucket<C = RealClock> {
    capacity: u32,
    tokens: u32,
    refill_per_second: u32,
    last_refill: Instant,
    clock: C,
}

impl TokenBucket<RealClock> {
    fn new(refill_per_second: u32) -> Self {
        Self::with_clock(refill_per_second, RealClock)
    }
}

impl<C: Clock> TokenBucket<C> {
    fn with_clock(refill_per_second: u32, clock: C) -> Self {
        let capacity = refill_per_second.max(1);
        Self {
            capacity,
            tokens: capacity,
            refill_per_second: capacity,
            last_refill: clock.now(),
            clock,
        }
    }

    fn try_take(&mut self) -> bool {
        self.refill(self.clock.now());
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        let elapsed_nanos = elapsed.as_nanos();
        let new_tokens =
            elapsed_nanos.saturating_mul(u128::from(self.refill_per_second)) / 1_000_000_000;
        if new_tokens == 0 {
            return;
        }
        let new_tokens = new_tokens.min(u128::from(u32::MAX)) as u32;
        self.tokens = self.capacity.min(self.tokens.saturating_add(new_tokens));
        self.last_refill = now;
    }
}

/// Errors produced by the Zakura protocol handler.
#[derive(Debug, Error)]
pub enum ZakuraHandlerError {
    /// A bounded read/write timed out.
    #[error("Zakura {0} timed out")]
    Timeout(&'static str),
    /// A peer-controlled payload exceeded its cap.
    #[error("Zakura payload exceeded its cap")]
    Oversize,
    /// The peer closed the stream or connection.
    #[error("Zakura stream closed")]
    Closed,
    /// The configured bootstrap peer is malformed.
    #[error("invalid Zakura bootstrap peer")]
    InvalidBootstrapPeer,
    /// The configured iroh secret key is malformed.
    #[error("invalid Zakura iroh secret key")]
    InvalidSecretKey,
    /// Local Zakura limits violate an invariant.
    #[error("invalid Zakura local limits")]
    InvalidLocalLimits,
    /// A local resource cap rejected the operation.
    #[error("Zakura resource limit exceeded: {0}")]
    ResourceLimit(&'static str),
    /// Iroh connection error.
    #[error(transparent)]
    IrohConnection(#[from] iroh::endpoint::ConnectionError),
    /// Iroh connect error.
    #[error(transparent)]
    IrohConnect(#[from] iroh::endpoint::ConnectError),
    /// Iroh remote id error.
    #[error(transparent)]
    IrohRemoteId(#[from] iroh::endpoint::RemoteNodeIdError),
    /// Iroh write error.
    #[error(transparent)]
    IrohWrite(#[from] iroh::endpoint::WriteError),
    /// Iroh read error.
    #[error(transparent)]
    IrohRead(#[from] iroh::endpoint::ReadExactError),
    /// Closed stream.
    #[error(transparent)]
    IrohClosedStream(#[from] iroh::endpoint::ClosedStream),
    /// Zakura wire format error.
    #[error(transparent)]
    Protocol(#[from] ZakuraProtocolError),
    /// Zakura validation error.
    #[error(transparent)]
    Validation(#[from] super::ZakuraValidationError),
    /// Zakura discovery exchange error.
    #[error("Zakura discovery exchange failed: {0}")]
    Discovery(#[source] BoxError),
    /// I/O error while encoding or decoding local buffers.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::internal::{InventoryResponse, Response},
        zakura::legacy_gossip::{LegacyRequestFrame, LegacyRequestKind, LegacyResponseCodec},
        zakura::testkit::{
            await_until, HostilePeer, LocalEndpointFactory, TestEchoStatusService, ZakuraTestNode,
            TEST_ECHO_STATUS_SERVICE_ID,
        },
        zakura::{DiscoveryWireError, ZakuraDiscoveryPersistedEntry},
    };
    use std::{
        collections::HashSet,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };
    use zebra_chain::{
        block::{self, Block},
        serialization::{ZcashDeserialize, MAX_PROTOCOL_MESSAGE_LEN},
        transaction::{self, UnminedTxId},
    };
    use zebra_test::vectors::BLOCK_TESTNET_141042_BYTES;

    #[test]
    fn local_limits_clamp_negotiated_values_down() {
        let config = Config::default();
        let limits = ZakuraLocalLimits::from_config(&config);
        let negotiated = ZakuraAcceptedLimits {
            max_frame_bytes: u32::MAX,
            max_message_bytes: u32::MAX,
            max_open_streams: u16::MAX,
            max_inbound_queue_depth: u16::MAX,
            idle_timeout_millis: u32::MAX,
        };

        let clamped = limits.clamp(&negotiated);

        assert_eq!(clamped.max_frame_bytes, limits.max_frame_bytes);
        assert_eq!(clamped.max_message_bytes, limits.max_message_bytes);
        assert_eq!(clamped.max_open_streams, limits.max_open_streams);
        assert_eq!(
            clamped.max_inbound_queue_depth,
            limits.max_inbound_queue_depth
        );
        assert!(clamped.idle_timeout < limits.quic_idle_timeout);
    }

    #[test]
    fn token_bucket_rejects_churn_until_refill() {
        let clock = crate::zakura::testkit::TestClock::new();
        let mut bucket = TokenBucket::with_clock(2, clock.clone());

        assert!(bucket.try_take());
        assert!(bucket.try_take());
        assert!(!bucket.try_take());
        clock.advance(Duration::from_millis(500));
        assert!(bucket.try_take());
        assert!(!bucket.try_take());
        clock.advance(Duration::from_millis(500));
        assert!(bucket.try_take());
    }

    #[test]
    fn supported_stream_accepts_known_kinds_at_version_one_only() {
        // FLUP-015: the prelude is peer-controlled. Only known kinds at version 1 are
        // served; everything else is rejected before admission.
        for kind in [
            0u16,
            1,
            2,
            3,
            ZAKURA_STREAM_DISCOVERY,
            ZAKURA_STREAM_TEST_ECHO_STATUS,
        ] {
            assert!(
                is_supported_stream(kind, ZAKURA_STREAM_VERSION_1),
                "known kind {kind} at version 1 must be supported"
            );
            assert!(
                !is_supported_stream(kind, 0),
                "known kind {kind} at version 0 must be rejected"
            );
            assert!(
                !is_supported_stream(kind, 2),
                "known kind {kind} at an unsupported version must be rejected"
            );
        }

        assert_eq!(stream_kind_label(ZAKURA_STREAM_DISCOVERY), "discovery");
        assert_eq!(
            stream_kind_label(ZAKURA_STREAM_TEST_ECHO_STATUS),
            "test_echo_status"
        );

        for kind in [7u16, 255, u16::MAX] {
            assert!(
                !is_supported_stream(kind, ZAKURA_STREAM_VERSION_1),
                "unknown kind {kind} must be rejected even at version 1"
            );
            assert_eq!(stream_kind_label(kind), "unknown");
        }
    }

    #[test]
    fn test_echo_status_response_validator_accepts_one_matching_frame() -> Result<(), BoxError> {
        let request_id = 42;
        let mut state = TestEchoStatusResponseReadState::new(
            request_id,
            TEST_ECHO_STATUS_REQUEST,
            test_connection_limits(),
        )
        .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
        let frame = test_echo_status_response_frame(request_id, b"ok");

        state
            .validate_frame(&frame)
            .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
        state
            .finish()
            .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
        assert_eq!(
            test_echo_status_response_payload(&frame.payload)
                .map_err(|error| -> BoxError { format!("{error:?}").into() })?,
            (request_id, b"ok".as_slice())
        );

        Ok(())
    }

    #[test]
    fn test_echo_status_response_validator_rejects_malformed_frames() {
        let request_id = 42;
        let valid_frame = test_echo_status_response_frame(request_id, b"ok");

        let mut two_frame_state = test_echo_status_response_state(request_id);
        assert!(two_frame_state.validate_frame(&valid_frame).is_ok());
        assert!(two_frame_state.validate_frame(&valid_frame).is_err());

        let mut wrong_type = valid_frame.clone();
        wrong_type.message_type = TEST_ECHO_STATUS_REQUEST;
        assert!(test_echo_status_response_state(request_id)
            .validate_frame(&wrong_type)
            .is_err());

        let mut non_zero_flags = valid_frame.clone();
        non_zero_flags.flags = 1;
        assert!(test_echo_status_response_state(request_id)
            .validate_frame(&non_zero_flags)
            .is_err());

        let mut oversized_limits = test_connection_limits();
        oversized_limits.max_message_bytes =
            u32::try_from(LEGACY_RESPONSE_REQUEST_ID_BYTES).expect("request id length fits in u32");
        let oversized = test_echo_status_response_frame(request_id, b"x");
        let mut oversized_state = TestEchoStatusResponseReadState::new(
            request_id,
            TEST_ECHO_STATUS_REQUEST,
            oversized_limits,
        )
        .expect("test limits fit in usize");
        assert!(oversized_state.validate_frame(&oversized).is_err());

        let wrong_id = test_echo_status_response_frame(request_id + 1, b"ok");
        assert!(test_echo_status_response_state(request_id)
            .validate_frame(&wrong_id)
            .is_err());

        let truncated_id = Frame {
            message_type: TEST_ECHO_STATUS_RESPONSE,
            flags: 0,
            payload: vec![0; LEGACY_RESPONSE_REQUEST_ID_BYTES - 1],
        };
        assert!(test_echo_status_response_state(request_id)
            .validate_frame(&truncated_id)
            .is_err());
        assert!(test_echo_status_response_payload(&truncated_id.payload).is_err());

        assert!(test_echo_status_response_state(request_id)
            .finish()
            .is_err());
    }

    async fn wait_for_discovery_record(
        node: &ZakuraTestNode,
        node_id: NodeId,
    ) -> Result<ZakuraNodeRecord, BoxError> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(record) = node.discovery().record_for(node_id).await {
                    return record;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| -> BoxError { "timed out waiting for discovery record".into() })
    }

    async fn node_id(node: &ZakuraTestNode) -> NodeId {
        node.node_addr().await.node_id
    }

    fn test_record_body(node_id: NodeId) -> crate::zakura::ZakuraNodeRecordBody {
        test_record_body_with_addrs(
            node_id,
            1,
            vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(198, 51, 100, 250)),
                28_233,
            )],
        )
    }

    fn test_record_body_with_addrs(
        node_id: NodeId,
        sequence: u64,
        direct_addrs: Vec<SocketAddr>,
    ) -> crate::zakura::ZakuraNodeRecordBody {
        let config = ZakuraHandshakeConfig::for_network(&Config::default().network);
        let expires_at_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test system clock is after Unix epoch")
            .as_secs()
            .saturating_add(3600);
        crate::zakura::ZakuraNodeRecordBody {
            node_id,
            direct_addrs,
            services: vec![ZakuraServiceId::discovery()],
            zakura_protocol_min: config.zakura_protocol_min,
            zakura_protocol_max: config.zakura_protocol_max,
            network_id: config.network_id,
            chain_id: config.chain_id,
            sequence,
            expires_at_unix_secs,
        }
    }

    fn signed_test_record(
        seed: u64,
        sequence: u64,
        direct_addrs: Vec<SocketAddr>,
    ) -> Result<ZakuraNodeRecord, DiscoveryWireError> {
        let secret = LocalEndpointFactory::secret_key(seed);
        ZakuraNodeRecord::sign(
            test_record_body_with_addrs(secret.public(), sequence, direct_addrs),
            &secret,
        )
    }

    fn dead_routable_addr(index: u8) -> SocketAddr {
        SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, index.max(1))),
            28_000 + u16::from(index),
        )
    }

    fn peer_id_for(node_id: NodeId) -> ZakuraPeerId {
        ZakuraPeerId::new(node_id.as_bytes().to_vec()).expect("iroh node ids are valid peer ids")
    }

    fn test_echo_status_service_id() -> ZakuraServiceId {
        ZakuraServiceId::new(TEST_ECHO_STATUS_SERVICE_ID)
            .expect("test echo/status service id is valid")
    }

    async fn wait_for_registered_peer(
        node: &ZakuraTestNode,
        node_id: NodeId,
    ) -> Result<(), BoxError> {
        let peer_id = peer_id_for(node_id);
        let mut registered = node.supervisor().subscribe();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if registered.borrow_and_update().contains(&peer_id) {
                    return Ok(());
                }
                registered.changed().await.map_err(|_| -> BoxError {
                    "peer-set watcher closed before expected registration".into()
                })?;
            }
        })
        .await
        .map_err(|_| -> BoxError { "timed out waiting for peer registration".into() })?
    }

    async fn wait_for_registered_count_at_least(
        node: &ZakuraTestNode,
        count: usize,
    ) -> Result<(), BoxError> {
        let mut registered = node.supervisor().subscribe();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if registered.borrow_and_update().len() >= count {
                    return Ok(());
                }
                registered.changed().await.map_err(|_| -> BoxError {
                    "peer-set watcher closed before expected count".into()
                })?;
            }
        })
        .await
        .map_err(|_| -> BoxError { "timed out waiting for peer count".into() })?
    }

    async fn wait_for_discovery_attempt(
        discovery: &ZakuraDiscoveryHandle,
        node_id: NodeId,
    ) -> Result<ZakuraDiscoveryPersistedEntry, BoxError> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(entry) = discovery
                    .persisted_entries()
                    .await
                    .into_iter()
                    .find(|entry| entry.record.body.node_id == node_id)
                    .filter(|entry| entry.last_dial_attempt.is_some())
                {
                    return entry;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| -> BoxError { "timed out waiting for discovery dial attempt".into() })
    }

    fn spawn_wired_discovery_dialer(node: &ZakuraTestNode) -> tokio::task::JoinHandle<()> {
        tokio::spawn(run_native_discovery_dialer(
            node.endpoint(),
            node.discovery(),
            node.limits().clone(),
        ))
    }

    async fn insert_static_candidate(
        node: &ZakuraTestNode,
        peer: &ZakuraTestNode,
    ) -> Result<NodeId, BoxError> {
        let node_addr = peer.node_addr().await;
        let node_id = node_addr.node_id;
        node.discovery().insert_static_candidate(node_addr).await?;
        Ok(node_id)
    }

    async fn import_static_loopback_record(
        node: &ZakuraTestNode,
        peer: &ZakuraTestNode,
        sequence: u64,
    ) -> Result<NodeId, BoxError> {
        let node_addr = peer.node_addr().await;
        let direct_addrs = node_addr.direct_addresses().copied().collect();
        let record = signed_test_record(peer.seed(), sequence, direct_addrs)?;
        let node_id = record.body.node_id;
        node.discovery().import_static_record(record).await?;
        Ok(node_id)
    }

    #[tokio::test]
    async fn discovery_exchange_imports_connected_self_records() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let node_a = ZakuraTestNode::builder(301).spawn().await?;
        let node_b = ZakuraTestNode::builder(302).spawn().await?;
        let node_a_id = node_id(&node_a).await;
        let node_b_id = node_id(&node_b).await;

        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;

        let b_on_a = wait_for_discovery_record(&node_a, node_b_id).await?;
        let a_on_b = wait_for_discovery_record(&node_b, node_a_id).await?;
        assert_eq!(b_on_a.body.node_id, node_b_id);
        assert_eq!(a_on_b.body.node_id, node_a_id);
        assert_eq!(
            node_a.discovery().active_services(node_b_id).await,
            Some(b_on_a.body.services)
        );

        node_a.shutdown().await;
        node_b.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn discovery_exchange_imports_third_party_peer_sample() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let node_a = ZakuraTestNode::builder(311).spawn().await?;
        let node_b = ZakuraTestNode::builder(312).spawn().await?;
        let node_c = ZakuraTestNode::builder(313).spawn().await?;
        let node_c_id = node_id(&node_c).await;

        node_b
            .connect_native(&node_c, Duration::from_secs(5))
            .await?;
        let _ = wait_for_discovery_record(&node_b, node_c_id).await?;

        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;
        let c_on_a = wait_for_discovery_record(&node_a, node_c_id).await?;

        assert_eq!(c_on_a.body.node_id, node_c_id);
        assert_eq!(node_a.supervisor().registered_ids().await.len(), 1);

        node_a.shutdown().await;
        node_b.shutdown().await;
        node_c.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_service_advertised_by_one_node_is_discovered_and_called() -> Result<(), BoxError>
    {
        let _guard = zebra_test::init();
        let service = test_echo_status_service_id();
        let echo = Arc::new(TestEchoStatusService::accepting());
        let node_a = ZakuraTestNode::builder(501).spawn().await?;
        let node_b = ZakuraTestNode::builder(502)
            .add_advertised_service(service.clone())
            .inbound_sink(echo.clone())
            .spawn()
            .await?;
        let node_b_id = node_id(&node_b).await;

        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;
        let _ = wait_for_discovery_record(&node_a, node_b_id).await?;

        let response = node_a
            .request_test_echo_status(&service, b"zakura-echo".to_vec())
            .await?;

        assert_eq!(response.responder, node_b_id);
        assert_eq!(response.payload, b"zakura-echo");
        assert!(!response.used_fallback);
        assert!(response.failed_service_candidates.is_empty());
        assert_eq!(echo.call_count(), 1);

        node_a.shutdown().await;
        node_b.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_service_live_request_failure_falls_back_to_general_peer() -> Result<(), BoxError>
    {
        let _guard = zebra_test::init();
        let service = test_echo_status_service_id();
        let rejecting = Arc::new(TestEchoStatusService::rejecting());
        let fallback = Arc::new(TestEchoStatusService::accepting());
        let node_a = ZakuraTestNode::builder(511).spawn().await?;
        let node_b = ZakuraTestNode::builder(512)
            .add_advertised_service(service.clone())
            .inbound_sink(rejecting.clone())
            .spawn()
            .await?;
        let node_c = ZakuraTestNode::builder(513)
            .inbound_sink(fallback.clone())
            .spawn()
            .await?;
        let node_b_id = node_id(&node_b).await;
        let node_c_id = node_id(&node_c).await;

        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;
        node_a
            .connect_native(&node_c, Duration::from_secs(5))
            .await?;
        let _ = wait_for_discovery_record(&node_a, node_b_id).await?;

        let response = node_a
            .request_test_echo_status(&service, b"fallback".to_vec())
            .await?;

        assert_eq!(response.responder, node_c_id);
        assert_eq!(response.payload, b"fallback");
        assert!(response.used_fallback);
        assert_eq!(response.failed_service_candidates, vec![node_b_id]);
        assert_eq!(rejecting.call_count(), 1);
        assert_eq!(fallback.call_count(), 1);

        node_a.shutdown().await;
        node_b.shutdown().await;
        node_c.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_service_advertising_peer_is_preferred_for_matching_request(
    ) -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let service = test_echo_status_service_id();
        let preferred = Arc::new(TestEchoStatusService::accepting());
        let fallback = Arc::new(TestEchoStatusService::accepting());
        let node_a = ZakuraTestNode::builder(521).spawn().await?;
        let node_b = ZakuraTestNode::builder(522)
            .add_advertised_service(service.clone())
            .inbound_sink(preferred.clone())
            .spawn()
            .await?;
        let node_c = ZakuraTestNode::builder(523)
            .inbound_sink(fallback.clone())
            .spawn()
            .await?;
        let node_b_id = node_id(&node_b).await;

        node_a
            .connect_native(&node_c, Duration::from_secs(5))
            .await?;
        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;
        let _ = wait_for_discovery_record(&node_a, node_b_id).await?;

        let response = node_a
            .request_test_echo_status(&service, b"preferred".to_vec())
            .await?;

        assert_eq!(response.responder, node_b_id);
        assert_eq!(response.payload, b"preferred");
        assert!(!response.used_fallback);
        assert_eq!(preferred.call_count(), 1);
        assert_eq!(fallback.call_count(), 0);

        node_a.shutdown().await;
        node_b.shutdown().await;
        node_c.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn wired_discovery_dialer_connects_static_test_candidate() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let node_a = ZakuraTestNode::builder(371)
            .discovery_config(ZakuraDiscoveryConfig {
                discovery_connection_headroom: 0,
                ..ZakuraDiscoveryConfig::default()
            })
            .spawn()
            .await?;
        let node_b = ZakuraTestNode::builder(372).spawn().await?;
        let node_b_id = insert_static_candidate(&node_a, &node_b).await?;
        let discovery_loop = spawn_wired_discovery_dialer(&node_a);

        wait_for_registered_peer(&node_a, node_b_id).await?;

        discovery_loop.abort();
        node_a.shutdown().await;
        node_b.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn wired_discovery_dialer_connects_peer_learned_through_seed() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let node_a = ZakuraTestNode::builder(381)
            .discovery_config(ZakuraDiscoveryConfig {
                discovery_connection_headroom: 0,
                ..ZakuraDiscoveryConfig::default()
            })
            .spawn()
            .await?;
        let node_b = ZakuraTestNode::builder(382).spawn().await?;
        let node_c = ZakuraTestNode::builder(383).spawn().await?;
        let node_c_id = node_id(&node_c).await;

        node_b
            .connect_native(&node_c, Duration::from_secs(5))
            .await?;
        let _ = wait_for_discovery_record(&node_b, node_c_id).await?;

        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;
        let c_on_a = wait_for_discovery_record(&node_a, node_c_id).await?;
        let trusted_c_id =
            import_static_loopback_record(&node_a, &node_c, c_on_a.body.sequence + 1).await?;
        let discovery_loop = spawn_wired_discovery_dialer(&node_a);

        wait_for_registered_peer(&node_a, trusted_c_id).await?;

        discovery_loop.abort();
        node_a.shutdown().await;
        node_b.shutdown().await;
        node_c.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn wired_discovery_dialer_preserves_bootstrap_headroom_under_discovered_flood(
    ) -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 5;
        limits.max_pending_handshakes = 8;
        limits.max_open_streams = 16;
        limits.max_inbound_queue_depth = 64;
        let bootstrap_count = 2;
        let headroom = effective_discovery_connection_headroom(bootstrap_count);
        let discovery_config = ZakuraDiscoveryConfig {
            discovery_connection_headroom: headroom,
            max_concurrent_discovery_dials: 4,
            ..ZakuraDiscoveryConfig::default()
        };
        let node_a = ZakuraTestNode::builder(391)
            .limits(limits.clone())
            .discovery_config(discovery_config)
            .spawn()
            .await?;
        let bootstrap_b = ZakuraTestNode::builder(392).spawn().await?;
        let bootstrap_c = ZakuraTestNode::builder(393).spawn().await?;
        let discovered_d = ZakuraTestNode::builder(394).spawn().await?;
        let discovered_e = ZakuraTestNode::builder(395).spawn().await?;
        let bootstrap_b_id = node_id(&bootstrap_b).await;
        let bootstrap_peers = vec![bootstrap_b.node_addr().await, bootstrap_c.node_addr().await];

        node_a
            .connect_native(&bootstrap_b, Duration::from_secs(5))
            .await?;
        node_a
            .connect_native(&bootstrap_c, Duration::from_secs(5))
            .await?;
        insert_static_bootstrap_candidates(&node_a.discovery(), &bootstrap_peers).await;
        spawn_native_bootstrap_dialer(node_a.endpoint(), bootstrap_peers, limits.clone());
        let discovery_loop = spawn_wired_discovery_dialer(&node_a);
        let _ = import_static_loopback_record(&node_a, &discovered_d, 1).await?;
        let _ = import_static_loopback_record(&node_a, &discovered_e, 1).await?;

        for index in 1..=32 {
            let record = signed_test_record(
                10_000 + u64::from(index),
                1,
                vec![dead_routable_addr(index)],
            )?;
            node_a
                .discovery()
                .import_peer_record(record, Some(node_id(&bootstrap_b).await))
                .await?;
        }
        wait_for_registered_peer(&node_a, bootstrap_b_id).await?;
        wait_for_registered_count_at_least(&node_a, limits.max_connections - headroom).await?;

        node_a
            .supervisor()
            .deregister(
                &peer_id_for(bootstrap_b_id),
                Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            )
            .await;
        await_until("bootstrap peer drops", Duration::from_secs(5), || {
            !node_a
                .supervisor()
                .subscribe()
                .borrow()
                .contains(&peer_id_for(bootstrap_b_id))
        })
        .await?;

        wait_for_registered_peer(&node_a, bootstrap_b_id).await?;
        assert!(
            node_a.supervisor().registered_ids().await.len() <= node_a.limits().max_connections,
            "discovery and bootstrap dials must not exceed max_connections"
        );

        discovery_loop.abort();
        node_a.shutdown().await;
        bootstrap_b.shutdown().await;
        bootstrap_c.shutdown().await;
        discovered_d.shutdown().await;
        discovered_e.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn combined_native_dialers_respect_max_connections() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 3;
        limits.max_pending_handshakes = 8;
        limits.max_open_streams = 16;
        limits.max_inbound_queue_depth = 64;
        let node_a = ZakuraTestNode::builder(401)
            .limits(limits.clone())
            .discovery_config(ZakuraDiscoveryConfig {
                discovery_connection_headroom: 0,
                max_concurrent_discovery_dials: 4,
                ..ZakuraDiscoveryConfig::default()
            })
            .spawn()
            .await?;
        let peers = [
            ZakuraTestNode::builder(402).spawn().await?,
            ZakuraTestNode::builder(403).spawn().await?,
            ZakuraTestNode::builder(404).spawn().await?,
            ZakuraTestNode::builder(405).spawn().await?,
            ZakuraTestNode::builder(406).spawn().await?,
        ];
        let bootstrap_peers = vec![peers[0].node_addr().await, peers[1].node_addr().await];
        spawn_native_bootstrap_dialer(node_a.endpoint(), bootstrap_peers, limits.clone());
        let discovery_loop = spawn_wired_discovery_dialer(&node_a);
        for peer in &peers[2..] {
            insert_static_candidate(&node_a, peer).await?;
        }
        let upgrade_like = node_a
            .endpoint()
            .spawn_native_dial(peers[4].node_addr().await);

        wait_for_registered_count_at_least(&node_a, node_a.limits().max_connections).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            node_a.supervisor().registered_ids().await.len() <= node_a.limits().max_connections,
            "combined bootstrap, upgrade, and discovery dialers exceeded max_connections"
        );

        discovery_loop.abort();
        upgrade_like.abort();
        node_a.shutdown().await;
        for peer in peers {
            peer.shutdown().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn dead_discovered_address_backs_off_and_releases_dial_capacity() -> Result<(), BoxError>
    {
        let _guard = zebra_test::init();
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 1;
        limits.control_timeout = Duration::from_millis(150);
        let node = ZakuraTestNode::builder(411)
            .limits(limits)
            .discovery_config(ZakuraDiscoveryConfig {
                discovery_connection_headroom: 0,
                max_concurrent_discovery_dials: 1,
                dial_backoff_base: Duration::from_secs(60),
                ..ZakuraDiscoveryConfig::default()
            })
            .spawn()
            .await?;
        let first = signed_test_record(412, 1, vec![dead_routable_addr(12)])?;
        let first_id = first.body.node_id;
        let second = signed_test_record(413, 1, vec![dead_routable_addr(12)])?;
        let second_id = second.body.node_id;
        node.discovery().import_peer_record(first, None).await?;
        node.discovery().import_peer_record(second, None).await?;
        let discovery_loop = spawn_wired_discovery_dialer(&node);

        let first_entry = wait_for_discovery_attempt(&node.discovery(), first_id).await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if node
                    .discovery()
                    .persisted_entries()
                    .await
                    .into_iter()
                    .any(|entry| entry.record.body.node_id == first_id && entry.failure_count > 0)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| -> BoxError { "timed out waiting for first dead candidate failure".into() })?;
        let second_entry = wait_for_discovery_attempt(&node.discovery(), second_id).await?;

        assert!(first_entry.last_dial_attempt.is_some());
        assert!(second_entry.last_dial_attempt.is_some());
        assert_eq!(node.supervisor().registered_ids().await.len(), 0);

        discovery_loop.abort();
        node.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn wired_discovery_dialer_filters_connected_local_and_caps_stampede(
    ) -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 16;
        limits.control_timeout = Duration::from_secs(2);
        let node_a = ZakuraTestNode::builder(421)
            .limits(limits)
            .discovery_config(ZakuraDiscoveryConfig {
                discovery_connection_headroom: 0,
                max_concurrent_discovery_dials: 2,
                ..ZakuraDiscoveryConfig::default()
            })
            .spawn()
            .await?;
        let node_b = ZakuraTestNode::builder(422).spawn().await?;
        let node_b_id = node_id(&node_b).await;
        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;
        let _ = wait_for_discovery_record(&node_a, node_b_id).await?;

        for index in 1..=16 {
            let record = signed_test_record(
                20_000 + u64::from(index),
                1,
                vec![dead_routable_addr(index)],
            )?;
            node_a.discovery().import_peer_record(record, None).await?;
        }
        let discovery_loop = spawn_wired_discovery_dialer(&node_a);
        tokio::time::sleep(Duration::from_millis(150)).await;

        let attempted = node_a
            .discovery()
            .persisted_entries()
            .await
            .into_iter()
            .filter(|entry| entry.last_dial_attempt.is_some())
            .collect::<Vec<_>>();
        assert!(
            attempted.len() <= 2,
            "wired dial loop started {} dials with max_concurrent_discovery_dials=2",
            attempted.len()
        );
        assert!(
            node_a
                .discovery()
                .record_for(node_id(&node_a).await)
                .await
                .is_none(),
            "local node record must not be dialable"
        );
        let b_entry = node_a
            .discovery()
            .persisted_entries()
            .await
            .into_iter()
            .find(|entry| entry.record.body.node_id == node_b_id);
        assert!(
            b_entry.is_none_or(|entry| entry.last_dial_attempt.is_none()),
            "already-connected peer must not be dialed by discovery"
        );

        discovery_loop.abort();
        node_a.shutdown().await;
        node_b.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn discovery_exchange_continues_after_addressless_self_record() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let node_a = ZakuraTestNode::builder(315)
            .discovery_direct_addrs(Vec::new())
            .spawn()
            .await?;
        let node_b = ZakuraTestNode::builder(316).spawn().await?;
        let node_c = ZakuraTestNode::builder(317).spawn().await?;
        let node_c_id = node_id(&node_c).await;

        node_b
            .connect_native(&node_c, Duration::from_secs(5))
            .await?;
        let _ = wait_for_discovery_record(&node_b, node_c_id).await?;

        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;
        let c_on_a = wait_for_discovery_record(&node_a, node_c_id).await?;

        assert_eq!(c_on_a.body.node_id, node_c_id);
        assert!(node_b
            .discovery()
            .record_for(node_id(&node_a).await)
            .await
            .is_none());

        node_a.shutdown().await;
        node_b.shutdown().await;
        node_c.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn discovery_get_peers_response_is_capped_by_responder_limit() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let node_a = ZakuraTestNode::builder(321).spawn().await?;
        let node_b = ZakuraTestNode::builder(322)
            .discovery_config(ZakuraDiscoveryConfig {
                peer_sample_limit: 1,
                ..ZakuraDiscoveryConfig::default()
            })
            .spawn()
            .await?;
        let node_c = ZakuraTestNode::builder(323).spawn().await?;
        let node_d = ZakuraTestNode::builder(324).spawn().await?;
        let node_c_id = node_id(&node_c).await;
        let node_d_id = node_id(&node_d).await;

        node_b
            .connect_native(&node_c, Duration::from_secs(5))
            .await?;
        node_b
            .connect_native(&node_d, Duration::from_secs(5))
            .await?;
        let _ = wait_for_discovery_record(&node_b, node_c_id).await?;
        let _ = wait_for_discovery_record(&node_b, node_d_id).await?;

        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let imported = [
                    node_a.discovery().record_for(node_c_id).await,
                    node_a.discovery().record_for(node_d_id).await,
                ]
                .into_iter()
                .flatten()
                .count();
                if imported == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| -> BoxError { "timed out waiting for capped discovery sample".into() })?;

        let imported: HashSet<_> = [
            node_a.discovery().record_for(node_c_id).await,
            node_a.discovery().record_for(node_d_id).await,
        ]
        .into_iter()
        .flatten()
        .map(|record| record.body.node_id)
        .collect();
        assert_eq!(imported.len(), 1);

        node_a.shutdown().await;
        node_b.shutdown().await;
        node_c.shutdown().await;
        node_d.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_discovery_streams_on_one_connection_are_harmless() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let node_a = ZakuraTestNode::builder(331).spawn().await?;
        let node_b = ZakuraTestNode::builder(332).spawn().await?;
        let node_a_id = node_id(&node_a).await;
        let node_b_id = node_id(&node_b).await;

        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;

        let _ = wait_for_discovery_record(&node_a, node_b_id).await?;
        let _ = wait_for_discovery_record(&node_b, node_a_id).await?;
        assert_eq!(node_a.supervisor().registered_ids().await.len(), 1);
        assert_eq!(node_b.supervisor().registered_ids().await.len(), 1);

        node_a.shutdown().await;
        node_b.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn discovery_refresh_task_exits_after_deregistration() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let discovery_config = ZakuraDiscoveryConfig {
            refresh_interval: Duration::from_millis(50),
            ..ZakuraDiscoveryConfig::default()
        };
        let node_a = ZakuraTestNode::builder(335)
            .discovery_config(discovery_config.clone())
            .spawn()
            .await?;
        let node_b = ZakuraTestNode::builder(336)
            .discovery_config(discovery_config)
            .spawn()
            .await?;
        let node_b_id = node_id(&node_b).await;

        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;
        let _ = wait_for_discovery_record(&node_a, node_b_id).await?;

        node_b.shutdown().await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if node_a.supervisor().registered_ids().await.is_empty() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| -> BoxError {
            "timed out waiting for discovery refresh task deregistration".into()
        })?;

        node_a.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn invalid_discovery_hello_signature_does_not_poison_book() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let node = ZakuraTestNode::builder(341).spawn().await?;
        let hostile = HostilePeer::connect_native(&node, 342).await?;
        let hostile_id = hostile.node_id();
        let other_secret = SecretKey::generate(OsRng);
        let mut record =
            ZakuraNodeRecord::sign(test_record_body(other_secret.public()), &other_secret)?;
        record.body.node_id = hostile_id;

        hostile
            .send_frame(
                ZAKURA_STREAM_DISCOVERY,
                DiscoveryMessage::Hello { record }.encode()?,
            )
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(node.discovery().record_for(hostile_id).await.is_none());

        hostile.shutdown().await;
        node.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn addressless_discovery_hello_still_gets_peer_sample_answer() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let node = ZakuraTestNode::builder(351).spawn().await?;
        let hostile_seed = 352;
        let hostile = HostilePeer::connect_native(&node, hostile_seed).await?;
        let hostile_secret = LocalEndpointFactory::secret_key(hostile_seed);
        let mut body = test_record_body(hostile.node_id());
        body.direct_addrs.clear();
        let record = ZakuraNodeRecord::sign(body, &hostile_secret)?;

        let (mut send, mut recv) = hostile.open_discovery_stream().await?;
        hostile
            .write_discovery_message(&mut send, DiscoveryMessage::Hello { record })
            .await?;

        match hostile.read_discovery_message(&mut recv).await? {
            DiscoveryMessage::Hello { .. } => {}
            message => return Err(format!("unexpected discovery response: {message:?}").into()),
        }

        hostile
            .write_discovery_message(
                &mut send,
                DiscoveryMessage::GetPeers {
                    limit: 0,
                    wanted_services: Vec::new(),
                    exclude_node_ids: Vec::new(),
                },
            )
            .await?;

        match hostile.read_discovery_message(&mut recv).await? {
            DiscoveryMessage::Peers { records } => assert!(records.is_empty()),
            message => return Err(format!("unexpected discovery response: {message:?}").into()),
        }

        hostile.shutdown().await;
        node.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn inbound_discovery_stream_imports_only_one_peer_batch() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let node = ZakuraTestNode::builder(353).spawn().await?;
        let hostile_seed = 354;
        let hostile = HostilePeer::connect_native(&node, hostile_seed).await?;
        let hostile_secret = LocalEndpointFactory::secret_key(hostile_seed);
        let hostile_record =
            ZakuraNodeRecord::sign(test_record_body(hostile.node_id()), &hostile_secret)?;
        let first_secret = LocalEndpointFactory::secret_key(355);
        let first_record =
            ZakuraNodeRecord::sign(test_record_body(first_secret.public()), &first_secret)?;
        let first_id = first_record.body.node_id;
        let second_secret = LocalEndpointFactory::secret_key(356);
        let second_record =
            ZakuraNodeRecord::sign(test_record_body(second_secret.public()), &second_secret)?;
        let second_id = second_record.body.node_id;

        let (mut send, mut recv) = hostile.open_discovery_stream().await?;
        hostile
            .write_discovery_message(
                &mut send,
                DiscoveryMessage::Hello {
                    record: hostile_record,
                },
            )
            .await?;
        let _ = hostile.read_discovery_message(&mut recv).await?;
        hostile
            .write_discovery_message(
                &mut send,
                DiscoveryMessage::Peers {
                    records: vec![first_record],
                },
            )
            .await?;
        let _ = wait_for_discovery_record(&node, first_id).await?;

        hostile
            .write_discovery_message(
                &mut send,
                DiscoveryMessage::Peers {
                    records: vec![second_record],
                },
            )
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(node.discovery().record_for(second_id).await.is_none());

        hostile.shutdown().await;
        node.shutdown().await;
        Ok(())
    }

    #[test]
    fn legacy_transport_response_validator_accepts_codec_frames() -> Result<(), BoxError> {
        let block = Arc::new(Block::zcash_deserialize(
            BLOCK_TESTNET_141042_BYTES.as_slice(),
        )?);
        let header = block::CountedHeader {
            header: block.header.clone(),
        };
        let tx_id = legacy_tx_id(7);

        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::BlocksByHash(vec![block.hash()]),
            LegacyRequestKind::Blocks,
            Response::Blocks(vec![InventoryResponse::Available((block, None))]),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::TransactionsById(vec![tx_id]),
            LegacyRequestKind::Transactions,
            Response::Transactions(vec![InventoryResponse::Missing(tx_id)]),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::FindBlocks {
                known_blocks: vec![block_hash(1)],
                stop: None,
            },
            LegacyRequestKind::FindBlocks,
            Response::BlockHashes(vec![block_hash(2)]),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::FindHeaders {
                known_blocks: vec![block_hash(1)],
                stop: None,
            },
            LegacyRequestKind::FindHeaders,
            Response::BlockHeaders(vec![header]),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::MempoolTransactionIds,
            LegacyRequestKind::MempoolTransactionIds,
            Response::TransactionIds(vec![tx_id]),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::Ping,
            LegacyRequestKind::Ping,
            Response::Pong(Duration::ZERO),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::PushTransaction(empty_v5_transaction(8).into()),
            LegacyRequestKind::PushTransaction,
            Response::Nil,
        )?;

        // The inbound service answers an empty FindBlocks/FindHeaders/
        // MempoolTransactionIds with `Response::Nil`, so a lone nil frame must
        // round-trip (encode -> transport validate -> decode) for those data
        // request kinds too, not only PushTransaction.
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::FindBlocks {
                known_blocks: vec![block_hash(1)],
                stop: None,
            },
            LegacyRequestKind::FindBlocks,
            Response::Nil,
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::FindHeaders {
                known_blocks: vec![block_hash(1)],
                stop: None,
            },
            LegacyRequestKind::FindHeaders,
            Response::Nil,
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::MempoolTransactionIds,
            LegacyRequestKind::MempoolTransactionIds,
            Response::Nil,
        )?;

        Ok(())
    }

    fn assert_codec_frames_validate_at_transport(
        request: LegacyRequestFrame,
        request_kind: LegacyRequestKind,
        response: Response,
    ) -> Result<(), BoxError> {
        let limits = test_connection_limits();
        let request_id = 99;
        let request_frame = request.encode_frame()?;
        let budget = LegacyResponseBudget::from_request(
            request_frame.message_type,
            &request_frame.payload,
            limits,
        )
        .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
        let frames =
            LegacyResponseCodec::encode_response(request_id, response, limits.max_frame_bytes)?;
        LegacyResponseCodec::decode_response(request_id, request_kind, frames.clone())?;

        let mut state = LegacyResponseReadState::new(budget);
        for frame in &frames {
            state
                .validate_frame(request_id, frame)
                .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
        }
        state
            .finish()
            .map_err(|error| -> BoxError { format!("{error:?}").into() })?;

        Ok(())
    }

    fn test_echo_status_response_state(request_id: u64) -> TestEchoStatusResponseReadState {
        TestEchoStatusResponseReadState::new(
            request_id,
            TEST_ECHO_STATUS_REQUEST,
            test_connection_limits(),
        )
        .expect("test connection limits fit in usize")
    }

    fn test_echo_status_response_frame(request_id: u64, payload: &[u8]) -> Frame {
        Frame {
            message_type: TEST_ECHO_STATUS_RESPONSE,
            flags: 0,
            payload: encode_test_echo_status_response_payload(request_id, payload),
        }
    }

    fn test_connection_limits() -> ZakuraConnectionLimits {
        let max_protocol_message_len =
            u32::try_from(MAX_PROTOCOL_MESSAGE_LEN).expect("protocol message length fits in u32");

        ZakuraConnectionLimits {
            max_frame_bytes: max_protocol_message_len,
            max_message_bytes: max_protocol_message_len,
            max_open_streams: 16,
            max_inbound_queue_depth: 16,
            idle_timeout: Duration::from_secs(1),
            prelude_timeout: Duration::from_secs(1),
            control_timeout: Duration::from_secs(1),
            stream_open_rate_per_second: 16,
            message_rate_per_second: 128,
        }
    }

    fn block_hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn legacy_tx_id(byte: u8) -> UnminedTxId {
        UnminedTxId::from_legacy_id(transaction::Hash([byte; 32]))
    }

    fn empty_v5_transaction(byte: u8) -> transaction::Transaction {
        transaction::Transaction::V5 {
            network_upgrade: zebra_chain::parameters::NetworkUpgrade::Nu5,
            lock_time: transaction::LockTime::min_lock_time_timestamp(),
            expiry_height: block::Height(u32::from(byte)),
            inputs: Vec::new(),
            outputs: Vec::new(),
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        }
    }

    #[test]
    fn same_kind_streams_share_one_connection_message_budget() {
        // FLUP-014: two workers serving the SAME kind on ONE connection must
        // draw from a single shared bucket, so opening a second stream does not
        // hand the peer a fresh full budget. Driven by TestClock so the aggregate
        // is asserted on return values, not timing or metrics.
        let clock = crate::zakura::testkit::TestClock::new();
        let mut buckets: MessageRateBuckets<crate::zakura::testkit::TestClock> =
            MessageRateBuckets::new();
        let rate = 4;

        // Two streams of the same kind get clones of the SAME bucket.
        let stream_a = message_bucket_for(&mut buckets, 1, rate, clock.clone());
        let stream_b = message_bucket_for(&mut buckets, 1, rate, clock.clone());
        assert!(
            Arc::ptr_eq(&stream_a, &stream_b),
            "same kind must reuse one shared bucket"
        );
        assert_eq!(
            buckets.len(),
            1,
            "no extra bucket created for the same kind"
        );

        // The aggregate across both handles is capped at the single-bucket budget.
        let take = |bucket: &SharedMessageBucket<crate::zakura::testkit::TestClock>| {
            bucket
                .lock()
                .expect("test bucket mutex is never poisoned")
                .try_take()
        };
        let mut accepted = 0;
        for _ in 0..rate as usize * 4 {
            if take(&stream_a) {
                accepted += 1;
            }
            if take(&stream_b) {
                accepted += 1;
            }
        }
        assert_eq!(
            accepted, rate as usize,
            "the second same-kind stream must NOT get a fresh full budget"
        );

        // After a full refill window the shared budget recovers to one capacity,
        // still shared across both streams (not doubled).
        clock.advance(Duration::from_secs(1));
        let mut refilled = 0;
        for _ in 0..rate as usize * 4 {
            if take(&stream_a) {
                refilled += 1;
            }
            if take(&stream_b) {
                refilled += 1;
            }
        }
        assert_eq!(
            refilled, rate as usize,
            "refill restores one shared budget, not one-per-stream"
        );
    }

    #[test]
    fn different_kinds_get_independent_message_budgets() {
        // FLUP-014: the bucket is keyed per stream-kind, so distinct known kinds
        // do not contend for the same budget.
        let clock = crate::zakura::testkit::TestClock::new();
        let mut buckets: MessageRateBuckets<crate::zakura::testkit::TestClock> =
            MessageRateBuckets::new();

        let request = message_bucket_for(&mut buckets, 1, 2, clock.clone());
        let gossip = message_bucket_for(&mut buckets, 2, 2, clock.clone());
        assert!(
            !Arc::ptr_eq(&request, &gossip),
            "distinct kinds must not share a bucket"
        );
        assert_eq!(buckets.len(), 2);

        let take = |bucket: &SharedMessageBucket<crate::zakura::testkit::TestClock>| {
            bucket
                .lock()
                .expect("test bucket mutex is never poisoned")
                .try_take()
        };
        // Drain the request budget; gossip must be untouched.
        assert!(take(&request));
        assert!(take(&request));
        assert!(!take(&request));
        assert!(take(&gossip));
        assert!(take(&gossip));
        assert!(!take(&gossip));
    }

    #[test]
    fn bootstrap_peer_requires_node_id_and_direct_address() {
        assert!(parse_bootstrap_peer("missing-address").is_err());
        assert!(parse_bootstrap_peer("not-a-node@127.0.0.1:8233").is_err());
    }

    #[test]
    fn bootstrap_peer_parser_keeps_valid_entries_and_drops_invalid_entries() {
        let node_id = SecretKey::generate(OsRng).public();
        let valid = format!("{node_id}@127.0.0.1:8233");
        let parsed = parse_bootstrap_peers(&[valid, "missing-address".to_string()]);

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].node_id, node_id);
    }

    #[tokio::test]
    async fn discovery_dial_result_updates_failure_metadata_without_penalizing_local_limits() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let local_secret = SecretKey::generate(OsRng);
        let peer_secret = SecretKey::generate(OsRng);
        let handshake_config = ZakuraHandshakeConfig::for_network(&Config::default().network);
        let discovery = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: vec![SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
                    8233,
                )],
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake_config.zakura_protocol_min,
                zakura_protocol_max: handshake_config.zakura_protocol_max,
                network_id: handshake_config.network_id,
                chain_id: handshake_config.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )
        .expect("discovery state constructs");
        let record = ZakuraNodeRecord::sign(
            crate::zakura::ZakuraNodeRecordBody {
                node_id: peer_secret.public(),
                direct_addrs: vec![SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)),
                    8233,
                )],
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake_config.zakura_protocol_min,
                zakura_protocol_max: handshake_config.zakura_protocol_max,
                network_id: handshake_config.network_id,
                chain_id: handshake_config.chain_id,
                sequence: 1,
                expires_at_unix_secs: crate::zakura::DEFAULT_DISCOVERY_RECORD_TTL.as_secs()
                    + SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("test system clock is after the Unix epoch")
                        .as_secs(),
            },
            &peer_secret,
        )
        .expect("peer record signs");
        let node_id = record.body.node_id;
        discovery
            .import_peer_record(record, Some(SecretKey::generate(OsRng).public()))
            .await
            .expect("peer record imports");

        apply_discovery_dial_result(
            &discovery,
            &node_id,
            DiscoveryDialResult::LocalResourceLimit,
        )
        .await;
        let entry = discovery
            .persisted_entries()
            .await
            .pop()
            .expect("entry remains after local limit");
        assert_eq!(entry.failure_count, 0);
        assert_eq!(entry.last_success, None);

        apply_discovery_dial_result(&discovery, &node_id, DiscoveryDialResult::Failed).await;
        let entry = discovery
            .persisted_entries()
            .await
            .pop()
            .expect("entry remains after failure");
        assert_eq!(entry.failure_count, 1);

        apply_discovery_dial_result(&discovery, &node_id, DiscoveryDialResult::Registered).await;
        let entry = discovery
            .persisted_entries()
            .await
            .pop()
            .expect("entry remains after success");
        assert_eq!(entry.failure_count, 0);
        assert!(entry.last_success.is_some());
    }

    #[test]
    fn discovery_headroom_reserves_at_least_bootstrap_peer_count() {
        assert_eq!(
            effective_discovery_connection_headroom(0),
            DEFAULT_DISCOVERY_CONNECTION_HEADROOM
        );
        assert_eq!(
            effective_discovery_connection_headroom(DEFAULT_DISCOVERY_CONNECTION_HEADROOM - 1),
            DEFAULT_DISCOVERY_CONNECTION_HEADROOM
        );
        assert_eq!(
            effective_discovery_connection_headroom(DEFAULT_DISCOVERY_CONNECTION_HEADROOM + 2),
            DEFAULT_DISCOVERY_CONNECTION_HEADROOM + 2
        );
    }

    #[test]
    fn discovery_in_flight_ip_reservations_are_counted_and_released() {
        let ip = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 10));
        let other_ip = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 11));
        let mut in_flight_by_ip = HashMap::new();

        reserve_discovery_in_flight_ips(&mut in_flight_by_ip, &[ip, ip, other_ip]);
        assert_eq!(in_flight_by_ip.get(&ip), Some(&2));
        assert_eq!(in_flight_by_ip.get(&other_ip), Some(&1));

        release_discovery_in_flight_ips(&mut in_flight_by_ip, &[ip, other_ip]);
        assert_eq!(in_flight_by_ip.get(&ip), Some(&1));
        assert!(!in_flight_by_ip.contains_key(&other_ip));

        release_discovery_in_flight_ips(&mut in_flight_by_ip, &[ip]);
        assert!(in_flight_by_ip.is_empty());
    }

    #[tokio::test]
    async fn supervisor_ip_capacity_includes_discovery_dials_in_flight() {
        let supervisor = ZakuraSupervisorHandle::new(1);
        let remote_ip = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 12));

        assert!(
            supervisor
                .can_accept_remote_ip_with_in_flight(remote_ip, 0)
                .await
        );
        assert!(
            !supervisor
                .can_accept_remote_ip_with_in_flight(remote_ip, 1)
                .await
        );
    }

    #[test]
    fn idle_invariant_keeps_app_timeout_below_quic_timeout() {
        let limits = ZakuraLocalLimits::from_config(&Config::default());

        validate_idle_invariant(&limits).expect("default Zakura limits satisfy idle invariant");
        assert!(
            (limits.initial_limits().idle_timeout_millis as u128)
                < limits.quic_idle_timeout.as_millis()
        );
    }

    fn redial_test_peer_id() -> ZakuraPeerId {
        ZakuraPeerId::new(vec![9u8; 32]).expect("32-byte node id is valid")
    }

    fn count_dial(
        calls: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
        result: DialResult,
    ) -> impl FnMut() -> Pin<Box<dyn Future<Output = DialResult> + Send>> {
        let calls = calls.clone();
        move || {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move { result }) as Pin<Box<dyn Future<Output = DialResult> + Send>>
        }
    }

    fn dial_count(calls: &std::sync::Arc<std::sync::atomic::AtomicUsize>) -> usize {
        calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// A `connect_once` dial that keeps failing gives up after `max_attempts`.
    #[tokio::test]
    async fn dial_supervisor_connect_once_gives_up_after_max_attempts() {
        let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let policy =
            RedialPolicy::connect_once(Duration::from_millis(1), Duration::from_millis(1), 3);

        tokio::time::timeout(
            Duration::from_secs(5),
            run_dial_supervisor(
                redial_test_peer_id(),
                registered,
                policy,
                count_dial(&calls, DialResult::Failed),
            ),
        )
        .await
        .expect("connect_once must stop after exhausting its attempts");

        assert_eq!(dial_count(&calls), 3);
    }

    /// A `connect_once` dial that connects healthily stops without re-dialing.
    #[tokio::test]
    async fn dial_supervisor_connect_once_stops_after_healthy_connection() {
        let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let policy =
            RedialPolicy::connect_once(Duration::from_millis(1), Duration::from_millis(1), 3);

        tokio::time::timeout(
            Duration::from_secs(5),
            run_dial_supervisor(
                redial_test_peer_id(),
                registered,
                policy,
                count_dial(&calls, DialResult::Healthy),
            ),
        )
        .await
        .expect("connect_once returns once it has connected");

        assert_eq!(dial_count(&calls), 1);
    }

    /// A peer already registered (e.g. it dialed us first) is not re-dialed.
    #[tokio::test]
    async fn dial_supervisor_skips_already_registered_peer() {
        let peer_id = redial_test_peer_id();
        let (_tx, registered) = tokio::sync::watch::channel(vec![peer_id.clone()]);
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let policy =
            RedialPolicy::connect_once(Duration::from_millis(1), Duration::from_millis(1), 3);

        tokio::time::timeout(
            Duration::from_secs(5),
            run_dial_supervisor(
                peer_id,
                registered,
                policy,
                count_dial(&calls, DialResult::Failed),
            ),
        )
        .await
        .expect("an already-connected connect_once peer returns immediately");

        assert_eq!(dial_count(&calls), 0);
    }

    /// A `maintain` peer keeps re-dialing after each connection drops.
    #[tokio::test]
    async fn dial_supervisor_maintain_redials_after_drop() {
        let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let policy = RedialPolicy::maintain(Duration::from_millis(1), Duration::from_millis(1));

        // Every attempt "connects then drops" after a brief serve, so maintain
        // must dial again; the small sleep also yields between attempts.
        let dial_calls = calls.clone();
        let dial = move || {
            dial_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(2)).await;
                DialResult::Healthy
            }) as Pin<Box<dyn Future<Output = DialResult> + Send>>
        };

        let supervisor = tokio::spawn(run_dial_supervisor(
            redial_test_peer_id(),
            registered,
            policy,
            dial,
        ));

        let deadline = Instant::now() + Duration::from_secs(5);
        while dial_count(&calls) < 3 {
            assert!(
                Instant::now() < deadline,
                "maintain never re-dialed after the connection dropped",
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        supervisor.abort();
    }
}
