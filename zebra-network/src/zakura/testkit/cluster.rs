//! Multi-node in-process Zakura harness.

use std::time::Duration;

use super::{await_until, TraceCapture, ZakuraTestNode};
use crate::{zakura::ZakuraPeerId, BoxError};

/// Supported deterministic topologies.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ClusterTopology {
    /// Every node dials every higher-indexed peer.
    FullMesh,
    /// Node `n` dials node `n + 1`.
    Line,
}

/// In-process collection of Zakura nodes.
#[derive(Debug, Default)]
pub struct ZakuraTestCluster {
    nodes: Vec<ZakuraTestNode>,
}

impl ZakuraTestCluster {
    /// Create an empty cluster.
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn one node and append it to the cluster.
    pub async fn spawn_node(&mut self, seed: u64) -> Result<usize, BoxError> {
        let node = ZakuraTestNode::builder(seed).spawn().await?;
        self.nodes.push(node);
        Ok(self.nodes.len() - 1)
    }

    /// Spawn one preconfigured node and append it to the cluster.
    pub async fn spawn_node_with(
        &mut self,
        builder: super::ZakuraTestNodeBuilder,
    ) -> Result<usize, BoxError> {
        let node = builder.spawn().await?;
        self.nodes.push(node);
        Ok(self.nodes.len() - 1)
    }

    /// Spawn one node with a per-node JSONL trace directory.
    pub async fn spawn_traced_node(
        &mut self,
        seed: u64,
        trace: &mut TraceCapture,
    ) -> Result<usize, BoxError> {
        let node = ZakuraTestNode::builder(seed)
            .tracer(trace.tracer_for_node(seed))
            .spawn()
            .await?;
        self.nodes.push(node);
        Ok(self.nodes.len() - 1)
    }

    /// Spawn all nodes in `seeds`.
    pub async fn spawn_nodes(
        &mut self,
        seeds: impl IntoIterator<Item = u64>,
    ) -> Result<(), BoxError> {
        for seed in seeds {
            self.spawn_node(seed).await?;
        }
        Ok(())
    }

    /// Borrow all nodes.
    pub fn nodes(&self) -> &[ZakuraTestNode] {
        &self.nodes
    }

    /// Borrow one node by index.
    pub fn node(&self, index: usize) -> &ZakuraTestNode {
        &self.nodes[index]
    }

    /// Connect nodes according to `topology`.
    pub async fn connect_topology(
        &self,
        topology: ClusterTopology,
        timeout: Duration,
    ) -> Result<(), BoxError> {
        match topology {
            ClusterTopology::FullMesh => self.connect_full_mesh(timeout).await,
            ClusterTopology::Line => {
                for pair in self.nodes.windows(2) {
                    pair[0].connect_native(&pair[1], timeout).await?;
                }
                Ok(())
            }
        }
    }

    /// Connect every pair in the cluster.
    pub async fn connect_full_mesh(&self, timeout: Duration) -> Result<(), BoxError> {
        for left in 0..self.nodes.len() {
            for right in (left + 1)..self.nodes.len() {
                self.nodes[left]
                    .connect_native(&self.nodes[right], timeout)
                    .await?;
            }
        }
        Ok(())
    }

    /// Wait until every node has registered all expected peers.
    pub async fn await_all_connected(&self, timeout: Duration) -> Result<(), BoxError> {
        let mut ids = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            ids.push(node.node_addr().await.node_id.as_bytes().to_vec());
        }

        for node in &self.nodes {
            let own_id = node.node_addr().await.node_id.as_bytes().to_vec();
            let expected_peers: Vec<Vec<u8>> =
                ids.iter().filter(|id| **id != own_id).cloned().collect();
            let registered = node.supervisor().subscribe();
            await_until("cluster peer set", timeout, || {
                expected_peers
                    .iter()
                    .all(|expected| contains_peer(&registered.borrow(), expected))
            })
            .await?;
        }
        Ok(())
    }

    /// Shut down all nodes.
    pub async fn shutdown(&self) {
        for node in &self.nodes {
            node.shutdown().await;
        }
    }
}

fn contains_peer(peers: &[ZakuraPeerId], expected: &[u8]) -> bool {
    peers.iter().any(|peer| peer.as_bytes() == expected)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc};

    use super::super::{HostilePeer, TestEchoStatusService, TEST_ECHO_STATUS_SERVICE_ID};
    use super::*;
    use crate::{
        protocol::internal::{InventoryResponse, PeerSource, Request, Response},
        zakura::{
            Frame, InboundSink, InboundSinkReject, LegacyGossipSink, ZakuraDiscoveryConfig,
            ZakuraDualStackService, ZakuraLocalLimits, ZakuraNodeRecord, ZakuraPeerId,
            ZakuraServiceId, ZAKURA_STREAM_TEST_ECHO_STATUS,
        },
        Config,
    };
    use tokio::sync::mpsc::UnboundedReceiver;
    use tower::{Service, ServiceExt};
    use zebra_chain::{
        block::{self, Block},
        serialization::ZcashDeserialize,
        transaction::{LockTime, Transaction, UnminedTx},
    };
    use zebra_test::vectors::BLOCK_TESTNET_141042_BYTES;

    #[tokio::test]
    #[ignore = "native handler mesh smoke is exercised by the zakura-integration nextest profile once dial scheduling is made deterministic"]
    async fn cluster_forms_native_two_node_mesh() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let mut cluster = ZakuraTestCluster::new();
        cluster.spawn_nodes([1, 2]).await?;

        cluster.connect_full_mesh(Duration::from_secs(5)).await?;
        cluster.await_all_connected(Duration::from_secs(5)).await?;
        cluster.shutdown().await;

        Ok(())
    }

    #[tokio::test]
    async fn traced_node_records_native_handshake_and_ratelimit_events() -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let mut capture = TraceCapture::for_test_with_keep_override(
            "traced_node_records_native_handshake_and_ratelimit_events",
            false,
        )?;
        let mut cluster = ZakuraTestCluster::new();
        let victim_idx = cluster.spawn_traced_node(1, &mut capture).await?;
        let victim = cluster.node(victim_idx);
        let hostile = HostilePeer::connect_native(victim, 2).await?;

        tokio::time::sleep(Duration::from_millis(200)).await;
        hostile.oversize_frame_declared_len(2).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        hostile.shutdown().await;
        cluster.shutdown().await;
        capture.flush().await;

        let reader = capture.reader()?;
        reader
            .node("01")
            .table("handshake")
            .assert_sequence(&["control.started", "control.succeeded"]);
        assert!(reader.node("01").table("conn").count("accepted") >= 1);
        assert!(reader.node("01").table("stream").count("accepted") >= 1);
        assert!(reader.node("01").table("ratelimit").count("frame.oversize") >= 1);

        assert!(capture.finish().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn unknown_stream_kind_is_reset_and_never_delivered() -> Result<(), BoxError> {
        // FLUP-015: a peer-controlled prelude naming an unknown kind must be
        // reset before the stream's frame reaches the inbound sink, while a
        // known kind on the same connection is still delivered. Asserted on
        // recorder state, not metrics.
        let _guard = zebra_test::init();
        let mut cluster = ZakuraTestCluster::new();
        let victim_idx = cluster.spawn_node(1).await?;
        let victim = cluster.node(victim_idx);
        let recorder = victim.recorder();
        let hostile = HostilePeer::connect_native(victim, 2).await?;

        let known_payload = b"known-kind-frame".to_vec();
        let unknown_payload = b"unknown-kind-frame".to_vec();
        // Unknown kind 9: must be reset and dropped.
        hostile.send_frame(9, unknown_payload.clone()).await?;
        // Known kind 2 (gossip): must be delivered.
        hostile.send_frame(2, known_payload.clone()).await?;

        await_until("known-kind frame delivered", Duration::from_secs(5), || {
            recorder.contains_payload(2, &known_payload)
        })
        .await?;

        // The known frame arrived; the unknown one must never have been delivered
        // under any kind label.
        let delivered = recorder.drain();
        assert!(
            delivered
                .iter()
                .any(|m| m.stream_kind == 2 && m.frame.payload == known_payload),
            "known-kind frame must be delivered"
        );
        assert!(
            !delivered.iter().any(|m| m.frame.payload == unknown_payload),
            "unknown-kind frame must be reset before delivery, got {delivered:?}"
        );

        hostile.shutdown().await;
        cluster.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_stream_version_is_reset_and_never_delivered() -> Result<(), BoxError> {
        // FLUP-015: a known kind at an unsupported version is rejected too.
        let _guard = zebra_test::init();
        let mut cluster = ZakuraTestCluster::new();
        let victim_idx = cluster.spawn_node(3).await?;
        let victim = cluster.node(victim_idx);
        let recorder = victim.recorder();
        let hostile = HostilePeer::connect_native(victim, 4).await?;

        let bad_version = b"kind-2-version-99".to_vec();
        let good = b"kind-2-version-1".to_vec();
        hostile
            .send_frame_with_version(2, 99, bad_version.clone())
            .await?;
        hostile.send_frame_with_version(2, 1, good.clone()).await?;

        await_until("version-1 frame delivered", Duration::from_secs(5), || {
            recorder.contains_payload(2, &good)
        })
        .await?;

        let delivered = recorder.drain();
        assert!(
            !delivered.iter().any(|m| m.frame.payload == bad_version),
            "unsupported-version frame must be reset before delivery, got {delivered:?}"
        );

        hostile.shutdown().await;
        cluster.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn same_kind_streams_share_aggregate_message_budget() -> Result<(), BoxError> {
        // FLUP-014: two streams of the SAME kind on ONE connection must share a
        // single per-connection message-rate budget. Flooding both must deliver
        // at most ~one budget worth within a refill window, NOT one budget per
        // stream. Asserted on recorder state.
        let _guard = zebra_test::init();

        // Small, deterministic message budget so the aggregate cap is observable
        // without sending hundreds of frames.
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 16;
        limits.max_pending_handshakes = 8;
        limits.max_open_streams = 16;
        limits.max_inbound_queue_depth = 256;
        limits.message_rate_per_second = 4;
        // Allow the stream opens themselves (open-rate is a separate limiter).
        limits.stream_open_rate_per_second = 64;
        let message_budget = limits.message_rate_per_second as usize;

        let victim = ZakuraTestNode::builder(5).limits(limits).spawn().await?;
        let recorder = victim.recorder();
        let hostile = HostilePeer::connect_native(&victim, 6).await?;

        // Flood exactly TWO same-kind streams (kind 2), each well past one budget.
        // Two streams keeps us clear of the open-stream semaphore so the only
        // limiter exercised is the shared per-kind message bucket.
        let per_stream = message_budget * 8;
        hostile.flood_stream(2, 'a', per_stream).await?;
        hostile.flood_stream(2, 'b', per_stream).await?;

        // Wait until rate limiting has clearly engaged (more frames sent than one
        // budget, so the bucket must have emptied at least once).
        await_until("rate limiting engaged", Duration::from_secs(5), || {
            recorder.len() + recorder.dropped_count() >= message_budget
        })
        .await?;
        // Brief settle to let any in-flight frames either deliver or be throttled.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Total ever delivered = retained + dropped-by-recorder (the recorder is a
        // bounded tap). A FRESH bucket per stream would let ~2 budgets through
        // immediately; the shared bucket caps the burst near one budget. Allow a
        // little headroom for sub-second refill during the settle window, but
        // stay well below the two-budgets-per-stream bug signature.
        let delivered_total = recorder.len() + recorder.dropped_count();
        assert!(
            delivered_total < message_budget * 2,
            "aggregate across two same-kind streams ({delivered_total}) must stay below two \
             independent {message_budget}-token budgets; a shared bucket caps the burst near one"
        );

        hostile.shutdown().await;
        victim.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn all_zakura_discovery_e2e_transitive_service_and_block_propagation(
    ) -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        let service = ZakuraServiceId::new(TEST_ECHO_STATUS_SERVICE_ID)
            .expect("test echo/status service id is valid");
        let discovery_config = ZakuraDiscoveryConfig {
            discovery_connection_headroom: 0,
            refresh_interval: Duration::from_millis(50),
            ..ZakuraDiscoveryConfig::default()
        };
        let block = Arc::new(Block::zcash_deserialize(
            BLOCK_TESTNET_141042_BYTES.as_slice(),
        )?);
        let transaction = UnminedTx::from(empty_v5_transaction(44));

        let (seed, _seed_rx) = legacy_recorder_node(901, discovery_config.clone()).await?;
        let node2_echo = Arc::new(TestEchoStatusService::accepting());
        let node2 = block_provider_echo_node(
            902,
            discovery_config.clone(),
            service.clone(),
            node2_echo.clone(),
            block.clone(),
            transaction,
        )
        .await?;
        let (node3, mut node3_rx) = legacy_recorder_node(903, discovery_config).await?;
        let seed_id = node_id(&seed).await;
        let node2_id = node_id(&node2).await;
        let node3_id = node_id(&node3).await;

        node2.insert_static_discovery_candidate(&seed).await?;
        node3.insert_static_discovery_candidate(&seed).await?;
        let node2_bootstrap = node2.spawn_discovery_dialer();
        let node3_bootstrap = node3.spawn_discovery_dialer();
        wait_for_registered_peer(&node2, seed_id).await?;
        wait_for_registered_peer(&node3, seed_id).await?;
        node2_bootstrap.abort();
        node3_bootstrap.abort();

        let node3_seen_by_node2 = wait_for_discovery_record(&node2, node3_id).await?;
        let node2_seen_by_node3 = wait_for_discovery_record(&node3, node2_id).await?;
        assert_eq!(node3_seen_by_node2.body.node_id, node3_id);
        assert_eq!(node2_seen_by_node3.body.node_id, node2_id);
        assert!(!registered_peers(&node2)
            .await
            .contains(&peer_id_for(node3_id)));
        assert!(!registered_peers(&node3)
            .await
            .contains(&peer_id_for(node2_id)));

        // The records above prove transitive discovery through the seed. Local
        // Iroh endpoints are loopback-only, while untrusted peer imports must
        // reject loopback addresses, so the testkit replaces the discovered
        // address with a trusted static loopback record for the same NodeId
        // before starting the real discovery dialer.
        node2
            .import_static_loopback_record(&node3, node3_seen_by_node2.body.sequence + 1)
            .await?;
        let node2_to_node3_dialer = node2.spawn_discovery_dialer();
        wait_for_registered_peer(&node2, node3_id).await?;
        wait_for_registered_peer(&node3, node2_id).await?;

        let service_response = node3
            .request_test_echo_status(&service, b"all-zakura-e2e".to_vec())
            .await?;
        assert_eq!(service_response.responder, node2_id);
        assert_eq!(service_response.payload, b"all-zakura-e2e");
        assert!(!service_response.used_fallback);
        assert_eq!(node2_echo.call_count(), 1);

        let mut node2_all_zakura =
            ZakuraDualStackService::new(LegacyDisabled, node2.supervisor(), false);
        node2_all_zakura
            .ready()
            .await?
            .call(Request::AdvertiseBlockToAll(block.hash()))
            .await?;
        match recv_request(&mut node3_rx).await? {
            Request::AdvertiseBlock(hash, Some(PeerSource::Zakura(peer_id))) => {
                assert_eq!(hash, block.hash());
                assert_eq!(peer_id, peer_id_for(node2_id));
            }
            request => panic!("unexpected node3 gossip request: {request:?}"),
        }

        let mut node3_all_zakura =
            ZakuraDualStackService::new(LegacyDisabled, node3.supervisor(), false);
        let block_response = node3_all_zakura
            .ready()
            .await?
            .call(Request::BlocksByHashFrom {
                hashes: HashSet::from([block.hash()]),
                source: PeerSource::Zakura(peer_id_for(node2_id)),
            })
            .await?;
        let Response::Blocks(blocks) = block_response else {
            panic!("unexpected block response: {block_response:?}");
        };
        assert!(matches!(
            blocks.as_slice(),
            [InventoryResponse::Available((received, None))] if received.hash() == block.hash()
        ));
        node2_to_node3_dialer.abort();
        seed.shutdown().await;
        node2.shutdown().await;
        node3.shutdown().await;
        Ok(())
    }

    #[derive(Clone, Debug)]
    struct RequestRecorder {
        tx: tokio::sync::mpsc::UnboundedSender<Request>,
    }

    impl Service<Request> for RequestRecorder {
        type Response = Response;
        type Error = BoxError;
        type Future = std::future::Ready<Result<Response, BoxError>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request) -> Self::Future {
            match self.tx.send(request) {
                Ok(()) => std::future::ready(Ok(Response::Nil)),
                Err(error) => std::future::ready(Err(Box::new(error))),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct BlockProvider {
        block: Arc<Block>,
        transaction: UnminedTx,
    }

    impl Service<Request> for BlockProvider {
        type Response = Response;
        type Error = BoxError;
        type Future = std::future::Ready<Result<Response, BoxError>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request) -> Self::Future {
            let response = match request {
                Request::AdvertiseBlock(..) | Request::AdvertiseTransactionIds(..) => Response::Nil,
                Request::FindBlocks { .. } => Response::BlockHashes(vec![self.block.hash()]),
                Request::FindHeaders { .. } => Response::BlockHeaders(vec![block::CountedHeader {
                    header: self.block.header.clone(),
                }]),
                Request::MempoolTransactionIds => {
                    Response::TransactionIds(vec![self.transaction.id])
                }
                Request::BlocksByHash(hashes) | Request::BlocksByHashFrom { hashes, .. } => {
                    Response::Blocks(
                        hashes
                            .into_iter()
                            .map(|hash| {
                                if hash == self.block.hash() {
                                    InventoryResponse::Available((self.block.clone(), None))
                                } else {
                                    InventoryResponse::Missing(hash)
                                }
                            })
                            .collect(),
                    )
                }
                request => {
                    return std::future::ready(Err(format!(
                        "unexpected all-Zakura block-provider request: {request:?}"
                    )
                    .into()));
                }
            };
            std::future::ready(Ok(response))
        }
    }

    #[derive(Clone, Debug)]
    struct LegacyDisabled;

    impl Service<Request> for LegacyDisabled {
        type Response = Response;
        type Error = BoxError;
        type Future = std::future::Ready<Result<Response, BoxError>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request) -> Self::Future {
            std::future::ready(Err(format!(
                "legacy path must be disabled in all-Zakura e2e, got {request:?}"
            )
            .into()))
        }
    }

    #[derive(Debug)]
    struct CombinedSink {
        legacy: LegacyGossipSink,
        echo: Arc<TestEchoStatusService>,
    }

    impl InboundSink for CombinedSink {
        fn deliver(
            &self,
            peer_id: ZakuraPeerId,
            stream_kind: u16,
            frame: Frame,
        ) -> Result<(), InboundSinkReject> {
            if stream_kind == ZAKURA_STREAM_TEST_ECHO_STATUS {
                self.echo.deliver(peer_id, stream_kind, frame)
            } else {
                self.legacy.deliver(peer_id, stream_kind, frame)
            }
        }

        fn request<'a>(
            &'a self,
            peer_id: ZakuraPeerId,
            stream_kind: u16,
            request_id: u64,
            max_frame_bytes: u32,
            frame: Frame,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<Frame>, InboundSinkReject>> + Send + 'a>>
        {
            if stream_kind == ZAKURA_STREAM_TEST_ECHO_STATUS {
                self.echo
                    .request(peer_id, stream_kind, request_id, max_frame_bytes, frame)
            } else {
                self.legacy
                    .request(peer_id, stream_kind, request_id, max_frame_bytes, frame)
            }
        }
    }

    async fn legacy_recorder_node(
        seed: u64,
        discovery_config: ZakuraDiscoveryConfig,
    ) -> Result<(ZakuraTestNode, UnboundedReceiver<Request>), BoxError> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let node = ZakuraTestNode::builder(seed)
            .discovery_config(discovery_config)
            .inbound_sink_from_supervisor(move |supervisor| {
                Arc::new(LegacyGossipSink::spawn(RequestRecorder { tx }, supervisor))
            })
            .spawn()
            .await?;
        Ok((node, rx))
    }

    async fn block_provider_echo_node(
        seed: u64,
        discovery_config: ZakuraDiscoveryConfig,
        service: ZakuraServiceId,
        echo: Arc<TestEchoStatusService>,
        block: Arc<Block>,
        transaction: UnminedTx,
    ) -> Result<ZakuraTestNode, BoxError> {
        let node = ZakuraTestNode::builder(seed)
            .discovery_config(discovery_config)
            .add_advertised_service(service)
            .inbound_sink_from_supervisor(move |supervisor| {
                Arc::new(CombinedSink {
                    legacy: LegacyGossipSink::spawn(
                        BlockProvider { block, transaction },
                        supervisor,
                    ),
                    echo,
                })
            })
            .spawn()
            .await?;
        Ok(node)
    }

    async fn node_id(node: &ZakuraTestNode) -> iroh::NodeId {
        node.node_addr().await.node_id
    }

    fn peer_id_for(node_id: iroh::NodeId) -> ZakuraPeerId {
        ZakuraPeerId::new(node_id.as_bytes().to_vec()).expect("iroh node ids are valid peer ids")
    }

    async fn registered_peers(node: &ZakuraTestNode) -> Vec<ZakuraPeerId> {
        node.supervisor().registered_ids().await
    }

    async fn wait_for_registered_peer(
        node: &ZakuraTestNode,
        node_id: iroh::NodeId,
    ) -> Result<(), BoxError> {
        let peer_id = peer_id_for(node_id);
        await_until("registered Zakura peer", Duration::from_secs(5), || {
            node.supervisor().subscribe().borrow().contains(&peer_id)
        })
        .await
        .map_err(|error| -> BoxError { Box::new(error) })
    }

    async fn wait_for_discovery_record(
        node: &ZakuraTestNode,
        node_id: iroh::NodeId,
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

    async fn recv_request(rx: &mut UnboundedReceiver<Request>) -> Result<Request, BoxError> {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .map_err(|_| -> BoxError { "timed out waiting for request".into() })?
            .ok_or_else(|| "request recorder closed".into())
    }

    fn empty_v5_transaction(byte: u8) -> Transaction {
        Transaction::V5 {
            network_upgrade: zebra_chain::parameters::NetworkUpgrade::Nu5,
            lock_time: LockTime::min_lock_time_timestamp(),
            expiry_height: block::Height(u32::from(byte)),
            inputs: Vec::new(),
            outputs: Vec::new(),
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        }
    }
}
