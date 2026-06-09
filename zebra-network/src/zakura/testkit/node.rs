//! In-process Zakura test node built from the production handler.

use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use iroh::{endpoint::TransportConfig, protocol::Router, NodeAddr};
use tokio::{sync::Mutex, task::JoinHandle};
use zebra_jsonl_trace::JsonlTracer;

use super::{InboundRecorder, LocalEndpointFactory, WaitError};
use crate::{
    zakura::{
        run_native_discovery_dialer, InboundSink, ZakuraDiscoveryConfig, ZakuraDiscoveryHandle,
        ZakuraDiscoveryLocalConfig, ZakuraEndpoint, ZakuraHandshakeConfig, ZakuraLocalLimits,
        ZakuraNodeRecord, ZakuraPeerId, ZakuraProtocolHandler, ZakuraServiceId,
        ZakuraSupervisorHandle, ZakuraTestServiceResponse, ZakuraTrace,
        DEFAULT_DISCOVERY_RECORD_TTL, P2P_V2_ALPN,
    },
    BoxError, Config,
};

/// A running in-process Zakura node for integration tests.
#[derive(Debug)]
pub struct ZakuraTestNode {
    seed: u64,
    endpoint: ZakuraEndpoint,
    limits: ZakuraLocalLimits,
    recorder: InboundRecorder,
    dial_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    _tracer: JsonlTracer,
}

impl ZakuraTestNode {
    /// Create a node builder using `seed` as the deterministic identity.
    pub fn builder(seed: u64) -> ZakuraTestNodeBuilder {
        ZakuraTestNodeBuilder::new(seed)
    }

    /// Deterministic seed used by this node.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Current Iroh node address.
    pub async fn node_addr(&self) -> NodeAddr {
        self.endpoint.node_addr().await
    }

    /// Clone the production endpoint backing this test node.
    pub(crate) fn endpoint(&self) -> ZakuraEndpoint {
        self.endpoint.clone()
    }

    /// Active supervisor handle.
    pub fn supervisor(&self) -> ZakuraSupervisorHandle {
        self.endpoint.supervisor()
    }

    /// Passive native discovery state.
    pub fn discovery(&self) -> ZakuraDiscoveryHandle {
        self.endpoint
            .discovery()
            .expect("ZakuraTestNode always enables discovery")
    }

    /// Local limits used by this node.
    pub fn limits(&self) -> &ZakuraLocalLimits {
        &self.limits
    }

    /// Bounded inbound recorder.
    pub fn recorder(&self) -> InboundRecorder {
        self.recorder.clone()
    }

    /// Spawn the real discovery dial loop for this test node.
    pub fn spawn_discovery_dialer(&self) -> JoinHandle<()> {
        tokio::spawn(run_native_discovery_dialer(
            self.endpoint(),
            self.discovery(),
            self.limits.clone(),
        ))
    }

    /// Insert a trusted static discovery candidate for `peer`.
    pub async fn insert_static_discovery_candidate(
        &self,
        peer: &ZakuraTestNode,
    ) -> Result<iroh::NodeId, BoxError> {
        let node_addr = peer.node_addr().await;
        let node_id = node_addr.node_id;
        self.discovery().insert_static_candidate(node_addr).await?;
        Ok(node_id)
    }

    /// Import a trusted static signed record for `peer` using its loopback endpoint address.
    pub async fn import_static_loopback_record(
        &self,
        peer: &ZakuraTestNode,
        sequence: u64,
    ) -> Result<iroh::NodeId, BoxError> {
        let node_addr = peer.node_addr().await;
        let mut body = peer.discovery().current_self_record().body.clone();
        body.direct_addrs = node_addr.direct_addresses().copied().collect();
        body.sequence = sequence;
        body.expires_at_unix_secs =
            current_unix_secs().saturating_add(DEFAULT_DISCOVERY_RECORD_TTL.as_secs());
        let record = ZakuraNodeRecord::sign(body, &LocalEndpointFactory::secret_key(peer.seed()))?;
        let node_id = record.body.node_id;
        self.discovery().import_static_record(record).await?;
        Ok(node_id)
    }

    /// Route a test-only echo/status request through service-aware Zakura selection.
    pub async fn request_test_echo_status(
        &self,
        service: &ZakuraServiceId,
        payload: Vec<u8>,
    ) -> Result<ZakuraTestServiceResponse, BoxError> {
        self.endpoint
            .request_test_echo_status(service, payload)
            .await
    }

    /// Start a native dial to `peer` and wait until this node registers it.
    pub async fn connect_native(
        &self,
        peer: &ZakuraTestNode,
        timeout: Duration,
    ) -> Result<(), BoxError> {
        let peer_addr = peer.node_addr().await;
        self.endpoint.add_node_addr(peer_addr.clone())?;
        let mut handle = self.endpoint.spawn_native_dial(peer_addr.clone());
        let peer_id = peer_addr.node_id.as_bytes().to_vec();
        let mut peer_set_rx = self.supervisor().subscribe();

        let result = tokio::time::timeout(timeout, async {
            tokio::select! {
                registered = wait_for_peer_registration(&mut peer_set_rx, peer_id.as_slice()) => {
                    registered
                }
                joined = &mut handle => {
                    joined
                        .map_err(|error| -> BoxError { format!("native Zakura dial task failed: {error}").into() })?;
                    Err("native Zakura dial task ended before serving the connection".into())
                }
            }
        })
        .await;

        match result {
            Ok(Ok(())) => {
                self.dial_tasks.lock().await.push(handle);
                Ok(())
            }
            Ok(Err(error)) => {
                handle.abort();
                Err(error)
            }
            Err(_) => {
                handle.abort();
                Err(Box::new(WaitError::new(
                    "native Zakura peer registration",
                    timeout,
                )))
            }
        }
    }

    /// Shut the node down and abort outstanding dial tasks.
    pub async fn shutdown(&self) {
        self.endpoint.shutdown().await;
        let mut tasks = self.dial_tasks.lock().await;
        for task in tasks.drain(..) {
            task.abort();
        }
    }
}

async fn wait_for_peer_registration(
    peer_set_rx: &mut tokio::sync::watch::Receiver<Vec<ZakuraPeerId>>,
    peer_id: &[u8],
) -> Result<(), BoxError> {
    loop {
        if peer_set_rx
            .borrow()
            .iter()
            .any(|id| id.as_bytes() == peer_id)
        {
            return Ok(());
        }

        peer_set_rx.changed().await.map_err(|_| -> BoxError {
            "Zakura peer-set watcher closed before registration".into()
        })?;
    }
}

/// Builder for [`ZakuraTestNode`].
pub struct ZakuraTestNodeBuilder {
    seed: u64,
    limits: ZakuraLocalLimits,
    transport_config: Option<TransportConfig>,
    legacy_upgrade: bool,
    tracer: JsonlTracer,
    discovery_config: ZakuraDiscoveryConfig,
    discovery_direct_addrs: Option<Vec<SocketAddr>>,
    extra_advertised_services: Vec<ZakuraServiceId>,
    loopback_port: u16,
    inbound_sink: Option<Arc<dyn InboundSink>>,
    inbound_sink_factory:
        Option<Box<dyn FnOnce(ZakuraSupervisorHandle) -> Arc<dyn InboundSink> + Send>>,
}

impl fmt::Debug for ZakuraTestNodeBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZakuraTestNodeBuilder")
            .field("seed", &self.seed)
            .field("limits", &self.limits)
            .field("transport_config", &self.transport_config.is_some())
            .field("legacy_upgrade", &self.legacy_upgrade)
            .field("tracer", &self.tracer)
            .field("discovery_config", &self.discovery_config)
            .field("discovery_direct_addrs", &self.discovery_direct_addrs)
            .field("extra_advertised_services", &self.extra_advertised_services)
            .field("loopback_port", &self.loopback_port)
            .field(
                "inbound_sink",
                &(self.inbound_sink.is_some() || self.inbound_sink_factory.is_some()),
            )
            .finish()
    }
}

impl ZakuraTestNodeBuilder {
    /// Create a node builder.
    pub fn new(seed: u64) -> Self {
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 16;
        limits.max_pending_handshakes = 8;
        limits.max_open_streams = 16;
        limits.max_inbound_queue_depth = 64;
        Self {
            seed,
            limits,
            transport_config: None,
            legacy_upgrade: false,
            tracer: JsonlTracer::noop(),
            discovery_config: ZakuraDiscoveryConfig::default(),
            discovery_direct_addrs: None,
            extra_advertised_services: Vec::new(),
            loopback_port: 0,
            inbound_sink: None,
            inbound_sink_factory: None,
        }
    }

    /// Override local limits.
    pub fn limits(mut self, limits: ZakuraLocalLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Mutate the transport configuration used by the endpoint factory.
    pub fn transport(mut self, configure: impl FnOnce(&mut TransportConfig)) -> Self {
        let mut transport = self.limits.transport_config();
        configure(&mut transport);
        self.transport_config = Some(transport);
        self
    }

    /// Reserve the legacy-upgrade test hook. Native-only remains the default.
    pub fn enable_legacy_upgrade(mut self, enable: bool) -> Self {
        self.legacy_upgrade = enable;
        self
    }

    /// Reserve the JSONL tracer hook used by the trace-introspection plan.
    pub fn tracer(mut self, tracer: JsonlTracer) -> Self {
        self.tracer = tracer;
        self
    }

    /// Override passive discovery configuration.
    pub fn discovery_config(mut self, discovery_config: ZakuraDiscoveryConfig) -> Self {
        self.discovery_config = discovery_config;
        self
    }

    /// Override locally advertised discovery direct addresses.
    pub fn discovery_direct_addrs(mut self, direct_addrs: Vec<SocketAddr>) -> Self {
        self.discovery_direct_addrs = Some(direct_addrs);
        self
    }

    /// Add a custom service id to this node's signed discovery self-record.
    pub fn add_advertised_service(mut self, service: ZakuraServiceId) -> Self {
        self.extra_advertised_services.push(service);
        self
    }

    /// Bind the node's Iroh endpoint to a specific loopback port.
    pub fn loopback_port(mut self, port: u16) -> Self {
        self.loopback_port = port;
        self
    }

    /// Install a custom inbound sink instead of the default recorder.
    pub fn inbound_sink(mut self, inbound_sink: Arc<dyn InboundSink>) -> Self {
        self.inbound_sink = Some(inbound_sink);
        self
    }

    /// Install a custom inbound sink that needs this node's supervisor.
    pub fn inbound_sink_from_supervisor(
        mut self,
        factory: impl FnOnce(ZakuraSupervisorHandle) -> Arc<dyn InboundSink> + Send + 'static,
    ) -> Self {
        self.inbound_sink_factory = Some(Box::new(factory));
        self
    }

    /// Spawn the node.
    pub async fn spawn(self) -> Result<ZakuraTestNode, BoxError> {
        if self.legacy_upgrade {
            return Err(
                "ZakuraTestNode legacy-upgrade mode is reserved until connect_via_upgrade is implemented"
                    .into(),
            );
        }

        let transport = self
            .transport_config
            .unwrap_or_else(|| self.limits.transport_config());
        let endpoint = LocalEndpointFactory::with_transport_config(transport)
            .endpoint_on_loopback_port(self.seed, self.loopback_port)
            .await?;
        let supervisor = ZakuraSupervisorHandle::new(self.limits.max_connections);
        let handshake_config = ZakuraHandshakeConfig::for_network(&Config::default().network);
        let mut services = vec![
            ZakuraServiceId::discovery(),
            ZakuraServiceId::legacy_gossip(),
            ZakuraServiceId::legacy_requests(),
            ZakuraServiceId::service_discovery(),
        ];
        services.extend(self.extra_advertised_services);
        let discovery = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: LocalEndpointFactory::secret_key(self.seed),
                direct_addrs: self
                    .discovery_direct_addrs
                    .unwrap_or_else(|| vec![test_discovery_addr(self.seed)]),
                services,
                zakura_protocol_min: handshake_config.zakura_protocol_min,
                zakura_protocol_max: handshake_config.zakura_protocol_max,
                network_id: handshake_config.network_id,
                chain_id: handshake_config.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig {
                max_zakura_connections: self.limits.max_connections,
                ..self.discovery_config
            },
            supervisor.subscribe(),
        )?;
        let recorder = InboundRecorder::new(usize::from(self.limits.max_inbound_queue_depth));
        let inbound_sink = if let Some(factory) = self.inbound_sink_factory {
            factory(supervisor.clone())
        } else {
            self.inbound_sink
                .unwrap_or_else(|| Arc::new(recorder.clone()))
        };
        let handler = ZakuraProtocolHandler::new_with_sink_trace_and_discovery(
            supervisor.clone(),
            handshake_config,
            self.limits.clone(),
            inbound_sink,
            ZakuraTrace::new(self.tracer.clone(), seed_label(self.seed)),
            Some(discovery),
        );
        let router = Router::builder(endpoint)
            .accept(P2P_V2_ALPN, handler.clone())
            .spawn();

        Ok(ZakuraTestNode {
            seed: self.seed,
            endpoint: ZakuraEndpoint::from_parts(router, supervisor, handler),
            limits: self.limits,
            recorder,
            dial_tasks: Arc::new(Mutex::new(Vec::new())),
            _tracer: self.tracer,
        })
    }
}

fn seed_label(seed: u64) -> String {
    format!("{seed:02}")
}

fn test_discovery_addr(seed: u64) -> SocketAddr {
    let host = u8::try_from(seed % 250 + 1).expect("test host octet is in range");
    let port = 20_000 + u16::try_from(seed % 10_000).expect("test port offset fits in u16");
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, host)), port)
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("test system clock is after the Unix epoch")
        .as_secs()
}

impl Drop for ZakuraTestNode {
    fn drop(&mut self) {
        if let Ok(mut tasks) = self.dial_tasks.try_lock() {
            for task in tasks.drain(..) {
                task.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn legacy_upgrade_builder_fails_loudly() {
        let error = ZakuraTestNode::builder(1)
            .enable_legacy_upgrade(true)
            .spawn()
            .await
            .expect_err("legacy-upgrade hook is reserved, not silently ignored");

        assert!(error.to_string().contains("connect_via_upgrade"));
    }
}
