use super::*;
use super::{config::*, error::*, events::*, reactor::*, validation::*, wire::*};
use crate::zakura::{
    testkit::{TraceCapture, TraceValue},
    HeaderSyncServiceSummary, ServicePeerDirection, ServicePeerLimits,
};
use chrono::Duration;
use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use rand::rngs::OsRng;
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
};
use zebra_chain::{
    parameters::{
        testnet::{
            ConfiguredActivationHeights, ConfiguredCheckpoints, Parameters, RegtestParameters,
        },
        Network,
    },
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    work::{difficulty::CompactDifficulty, equihash::Solution},
};
use zebra_test::vectors::{
    BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES, BLOCK_MAINNET_3_BYTES, BLOCK_MAINNET_4_BYTES,
    BLOCK_MAINNET_GENESIS_BYTES, BLOCK_TESTNET_GENESIS_BYTES,
};

#[derive(Default)]
struct HeaderSyncMetricsRecorder {
    counters: Mutex<BTreeMap<String, u64>>,
}

struct RecordedCounter {
    name: String,
    recorder: &'static HeaderSyncMetricsRecorder,
}

impl CounterFn for RecordedCounter {
    fn increment(&self, value: u64) {
        let mut counters = self.recorder.counters.lock().expect("metrics mutex ok");
        let counter = counters.entry(self.name.clone()).or_default();
        *counter = counter.saturating_add(value);
    }

    fn absolute(&self, value: u64) {
        let mut counters = self.recorder.counters.lock().expect("metrics mutex ok");
        counters.insert(self.name.clone(), value);
    }
}

impl Recorder for HeaderSyncMetricsRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(RecordedCounter {
            name: key.name().to_string(),
            recorder: header_sync_metrics_recorder(),
        }))
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

fn header_sync_metrics_recorder() -> &'static HeaderSyncMetricsRecorder {
    static RECORDER: OnceLock<HeaderSyncMetricsRecorder> = OnceLock::new();
    let recorder = RECORDER.get_or_init(HeaderSyncMetricsRecorder::default);
    let _ = metrics::set_global_recorder(recorder);
    recorder
}

fn metric_value(name: &str) -> u64 {
    let recorder = header_sync_metrics_recorder();
    recorder
        .counters
        .lock()
        .expect("metrics mutex ok")
        .get(name)
        .copied()
        .unwrap_or_default()
}

fn metric_snapshot(names: &[&'static str]) -> BTreeMap<&'static str, u64> {
    names
        .iter()
        .copied()
        .map(|name| (name, metric_value(name)))
        .collect()
}

fn assert_metric_incremented(snapshot: &BTreeMap<&'static str, u64>, name: &'static str) {
    assert!(
        metric_value(name) > snapshot.get(name).copied().unwrap_or_default(),
        "expected metric {name} to increment"
    );
}

fn mainnet_block(bytes: &[u8]) -> Arc<block::Block> {
    Arc::new(bytes.zcash_deserialize_into().expect("block vector parses"))
}

fn mainnet_header(bytes: &[u8]) -> Arc<block::Header> {
    mainnet_block(bytes).header.clone()
}

/// Map a decoded stream-5 message to the narrowed shared-effect event a peer
/// routine would forward to the reactor after running its peer-local validation.
///
/// Reactor-effect tests inject this narrowed event directly (the routine-level
/// validation is covered by dedicated routine-harness tests). This is test
/// plumbing, not production demux: the routine, not this helper, owns the
/// validation that decides which narrowed event a real wire frame produces. A
/// rejected message (which the routine would turn into `PeerMisbehavior` + a
/// disconnect) is not representable here; tests that need a routine rejection
/// drive the routine harness instead.
fn narrowed_event(peer: ZakuraPeerId, msg: HeaderSyncMessage) -> HeaderSyncEvent {
    match msg {
        HeaderSyncMessage::Status(status) => HeaderSyncEvent::PeerStatusUpdated { peer, status },
        HeaderSyncMessage::Headers {
            headers,
            body_sizes,
        } => HeaderSyncEvent::PeerHeadersReceived {
            peer,
            headers,
            body_sizes,
        },
        HeaderSyncMessage::GetHeaders {
            start_height,
            count,
        } => HeaderSyncEvent::InboundGetHeadersRequested {
            peer,
            start_height,
            count,
        },
        HeaderSyncMessage::NewBlock(block) => HeaderSyncEvent::NewBlockCandidate { peer, block },
    }
}

fn headers_message(headers: Vec<Arc<block::Header>>) -> HeaderSyncMessage {
    let body_sizes = vec![0; headers.len()];
    HeaderSyncMessage::Headers {
        headers,
        body_sizes,
    }
}

fn headers_message_with_sizes(
    headers: Vec<Arc<block::Header>>,
    body_sizes: Vec<u32>,
) -> HeaderSyncMessage {
    HeaderSyncMessage::Headers {
        headers,
        body_sizes,
    }
}

async fn validate_headers_stateless_after_equihash_acceptance(
    headers: Vec<Arc<block::Header>>,
    context: HeaderSyncValidationContext<'_>,
) -> Result<(), HeaderSyncWireError> {
    validate_header_count(headers.len(), context.decode_context)?;
    validate_internal_continuity(&headers)?;
    validate_header_times(&headers, context.now, context.start_height)?;
    validate_solution_sizes(&headers, context.network)?;
    tokio::task::spawn_blocking(move || {
        for header in headers {
            let hash = block::Hash::from(header.as_ref());
            validate_difficulty_filter(hash, header.difficulty_threshold)?;
        }
        Ok(())
    })
    .await?
}

fn headers_context(count: u32, peer_cap: u32) -> HeaderSyncDecodeContext {
    HeaderSyncDecodeContext::for_headers_response(
        ExpectedHeadersResponse::new(block::Height(1), count).unwrap(),
        peer_cap,
    )
}

/// A minimal in-process driver for one peer routine's relocated peer-local
/// validation, used by the tests that moved off the reactor in chunk 03 (the
/// peer-local `Status`/`GetHeaders`/`NewBlock` gates). It encodes a constructed
/// message to a frame and runs the production [`decode_and_ingest`] against a fresh
/// per-peer [`HsLocal`], then drains the narrowed shared-effect events the routine
/// forwarded.
struct RoutineHarness {
    local: super::pipe::HsLocal,
    env: super::pipe::HsEnv,
    peer: ZakuraPeerId,
    events: mpsc::Receiver<HeaderSyncEvent>,
}

impl RoutineHarness {
    fn new(peer: ZakuraPeerId) -> Self {
        Self::with_config(peer, Network::Mainnet, ZakuraHeaderSyncConfig::default())
    }

    fn with_config(peer: ZakuraPeerId, network: Network, config: ZakuraHeaderSyncConfig) -> Self {
        let (events, events_rx) = mpsc::channel(64);
        let (lifecycle, _lifecycle_rx) = mpsc::unbounded_channel();
        let (_tip_tx, tip) = watch::channel((block::Height(0), block::Hash([0; 32])));
        let (_peers_tx, peers) = watch::channel(crate::zakura::ServicePeerSnapshot::default());
        let (_candidates_tx, candidates) =
            watch::channel(crate::zakura::ZakuraHeaderSyncCandidateState::default());
        let handle = HeaderSyncHandle {
            events,
            lifecycle,
            tip,
            peers,
            candidates,
        };
        let env = super::pipe::HsEnv::new(handle, network, config, LOCAL_MAX_MESSAGE_BYTES);
        let local = super::pipe::new_ingest_local(&env);
        Self {
            local,
            env,
            peer,
            events: events_rx,
        }
    }

    /// Record an outbound `GetHeaders` expectation so a later `Headers` response
    /// correlates (mirrors production's `RecordExpectedHeaders`).
    fn record_expected(&mut self, start_height: block::Height, count: u32) {
        self.local.record_expected(
            ExpectedHeadersResponse::new(start_height, count).expect("valid count"),
        );
    }

    /// Drive one constructed message through the routine and return the resulting
    /// [`Flow`].
    fn ingest(&mut self, msg: HeaderSyncMessage) -> crate::zakura::Flow<()> {
        let frame = msg.encode_frame().expect("message encodes");
        super::pipe::decode_and_ingest(&mut self.local, &self.env, self.peer.clone(), frame)
    }

    fn try_next_event(&mut self) -> Option<HeaderSyncEvent> {
        self.events.try_recv().ok()
    }
}

struct ReactorFixture {
    handle: HeaderSyncHandle,
    actions: mpsc::Receiver<HeaderSyncAction>,
    task: JoinHandle<()>,
    outbound_receivers: Mutex<Vec<crate::zakura::FramedRecv>>,
}

impl Drop for ReactorFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds")
}

fn node_peer() -> (ZakuraPeerId, iroh::NodeId) {
    let node_id = iroh::SecretKey::generate(OsRng).public();
    (
        ZakuraPeerId::new(node_id.as_bytes().to_vec()).expect("node id is a valid peer id"),
        node_id,
    )
}

fn advisory_header_summary(
    best_height: block::Height,
    inbound_slots_free: u16,
) -> HeaderSyncServiceSummary {
    HeaderSyncServiceSummary {
        best_height,
        best_hash: block::Hash([7; 32]),
        finalized_height: None,
        serving_headers: true,
        inbound_slots_free,
        inbound_slots_max: inbound_slots_free,
        outbound_slots_free: 1,
        outbound_slots_max: 1,
    }
}

fn regtest_network() -> Network {
    Network::new_regtest(Default::default())
}

fn checkpoint_testnet_with_hash(
    checkpoint_height: block::Height,
    checkpoint_hash: block::Hash,
) -> (Network, block::Hash) {
    let mainnet = Network::Mainnet;
    let network = Parameters::build()
        .with_network_name("HeadersyncCheckpointTest")
        .expect("custom network name is valid")
        .with_genesis_hash(mainnet.genesis_hash())
        .expect("mainnet genesis hash is valid")
        .with_activation_heights(ConfiguredActivationHeights {
            overwinter: Some(1),
            sapling: Some(2),
            blossom: Some(3),
            heartwood: Some(4),
            canopy: Some(4),
            ..Default::default()
        })
        .expect("custom activation heights are in order")
        .clear_funding_streams()
        .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(vec![
            (block::Height(0), mainnet.genesis_hash()),
            (checkpoint_height, checkpoint_hash),
        ]))
        .expect("custom checkpoints are valid")
        .to_network()
        .expect("custom testnet parameters are valid");

    (network, checkpoint_hash)
}

fn checkpoint_regtest(checkpoint_height: block::Height) -> (Network, block::Hash) {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_1_BYTES).as_ref());
    checkpoint_regtest_with_hash(checkpoint_height, checkpoint_hash)
}

fn checkpoint_regtest_with_hash(
    checkpoint_height: block::Height,
    checkpoint_hash: block::Hash,
) -> (Network, block::Hash) {
    let default_regtest = regtest_network();
    let params = RegtestParameters {
        checkpoints: Some(ConfiguredCheckpoints::HeightsAndHashes(vec![
            (block::Height(0), default_regtest.genesis_hash()),
            (checkpoint_height, checkpoint_hash),
        ])),
        ..Default::default()
    };

    (Network::new_regtest(params), checkpoint_hash)
}

fn startup_for(
    network: Network,
    anchor: (block::Height, block::Hash),
    best_header_tip: Option<(block::Height, block::Hash)>,
) -> HeaderSyncStartup {
    let mut startup = HeaderSyncStartup::new(
        network,
        anchor,
        HeaderSyncFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        best_header_tip,
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.range_state_actions_enabled = true;
    startup.inbound_new_block_acceptance_enabled = true;
    startup
}

#[test]
fn startup_new_is_passive_until_local_hooks_are_wired() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let startup = HeaderSyncStartup::new(
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

    assert!(!startup.range_state_actions_enabled);
    assert!(!startup.inbound_new_block_acceptance_enabled);
}

fn startup_with_timeout(
    network: Network,
    anchor: (block::Height, block::Hash),
    request_timeout: std::time::Duration,
) -> HeaderSyncStartup {
    let mut startup = startup_for(network, anchor, None);
    startup.request_timeout = request_timeout;
    startup
}

#[tokio::test]
async fn peer_caps_reject_full_without_status_or_misbehavior_and_free_on_remove() {
    let network = Network::Mainnet;
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
        ZakuraHeaderSyncConfig {
            peer_limits: ServicePeerLimits {
                max_inbound_peers: 1,
                ..ServicePeerLimits::default()
            },
            ..ZakuraHeaderSyncConfig::default()
        },
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.range_state_actions_enabled = false;
    let mut fixture = spawn_test_reactor(startup);
    let admitted = peer(11);
    let rejected = peer(12);

    connect_peer(&fixture, admitted.clone()).await;
    assert!(matches!(
        next_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            peer,
            msg: HeaderSyncMessage::Status(_),
        } if peer == admitted
    ));
    assert_eq!(fixture.handle.peer_snapshot().inbound_peers, 1);
    assert_eq!(fixture.handle.peer_snapshot().inbound_slots_free, 0);

    let rejected_cancel =
        connect_peer_with_direction(&fixture, rejected.clone(), ServicePeerDirection::Inbound)
            .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        rejected_cancel.cancelled(),
    )
    .await
    .expect("rejected header-sync service session is locally parked");
    assert_eq!(fixture.handle.peer_snapshot().inbound_peers, 1);

    fixture
        .handle
        .send(narrowed_event(
            rejected.clone(),
            HeaderSyncMessage::Status(HeaderSyncStatus::default()),
        ))
        .await
        .unwrap();
    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), fixture.actions.recv()).await
    {
        assert!(
            !matches!(
                action,
                HeaderSyncAction::SendMessage { ref peer, .. } if *peer == rejected
            ),
            "rejected peer must not receive header-sync scheduling state"
        );
        assert!(
            !matches!(
                action,
                HeaderSyncAction::Misbehavior { ref peer, .. } if *peer == rejected
            ),
            "locally rejected peer must not be scored as misbehaving"
        );
    }

    fixture
        .handle
        .send(HeaderSyncEvent::PeerDisconnected(admitted))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert_eq!(fixture.handle.peer_snapshot().inbound_peers, 0);
    assert_eq!(fixture.handle.peer_snapshot().inbound_slots_free, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn advisory_summary_status_mismatch_uses_status_without_misbehavior_and_backs_off() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let (peer_id, peer_node_id) = node_peer();

    fixture
        .handle
        .send(HeaderSyncEvent::AdvisoryHeaderSummary {
            peer: peer_id.clone(),
            summary: advisory_header_summary(block::Height(10), 1),
        })
        .await
        .unwrap();
    assert!(fixture
        .handle
        .candidate_state()
        .backed_off_node_ids
        .is_empty());

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;

    let mut saw_status_authoritative_request = false;
    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        match action {
            HeaderSyncAction::Misbehavior { .. } => {
                panic!("summary/Status mismatch must not score misbehavior")
            }
            HeaderSyncAction::SendMessage {
                peer,
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height,
                        count,
                    },
            } if peer == peer_id => {
                assert_eq!(start_height, block::Height(1));
                assert_eq!(count, 1);
                saw_status_authoritative_request = true;
                break;
            }
            _ => {}
        }
    }
    assert!(saw_status_authoritative_request);

    fixture
        .handle
        .send(narrowed_event(peer_id.clone(), headers_message(Vec::new())))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    assert!(
        fixture
            .handle
            .candidate_state()
            .backed_off_node_ids
            .contains(&peer_node_id),
        "repeated unconfirmed advisory usefulness enters local non-punitive backoff"
    );
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn advisory_backoff_is_pruned_on_peer_disconnected() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let (peer_id, peer_node_id) = node_peer();

    fixture
        .handle
        .send(HeaderSyncEvent::AdvisoryHeaderSummary {
            peer: peer_id.clone(),
            summary: advisory_header_summary(block::Height(10), 1),
        })
        .await
        .unwrap();

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;

    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if matches!(
            action,
            HeaderSyncAction::SendMessage {
                ref peer,
                msg: HeaderSyncMessage::GetHeaders { .. },
            } if *peer == peer_id
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(peer_id.clone(), headers_message(Vec::new())))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(fixture
        .handle
        .candidate_state()
        .backed_off_node_ids
        .contains(&peer_node_id));

    fixture
        .handle
        .send(HeaderSyncEvent::PeerDisconnected(peer_id.clone()))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    assert!(
        !fixture
            .handle
            .candidate_state()
            .backed_off_node_ids
            .contains(&peer_node_id),
        "disconnect prunes advisory backoff state"
    );
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn admission_failure_after_advisory_selection_creates_no_outstanding_range() {
    let network = regtest_network();
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
        ZakuraHeaderSyncConfig {
            peer_limits: ServicePeerLimits {
                max_inbound_peers: 0,
                ..ServicePeerLimits::default()
            },
            ..ZakuraHeaderSyncConfig::default()
        },
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.range_state_actions_enabled = true;
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(22);

    fixture
        .handle
        .send(HeaderSyncEvent::AdvisoryHeaderSummary {
            peer: peer_id.clone(),
            summary: advisory_header_summary(block::Height(10), 1),
        })
        .await
        .unwrap();
    let cancel =
        connect_peer_with_direction(&fixture, peer_id.clone(), ServicePeerDirection::Inbound).await;
    tokio::time::timeout(std::time::Duration::from_secs(1), cancel.cancelled())
        .await
        .expect("admission failure parks the service session");

    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(10),
        1,
        1,
    )
    .await;

    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), fixture.actions.recv()).await
    {
        assert!(
            !matches!(
                action,
                HeaderSyncAction::SendMessage {
                    ref peer,
                    msg: HeaderSyncMessage::GetHeaders { .. },
                } if *peer == peer_id
            ),
            "locally rejected advisory peer must not get outstanding range work"
        );
        assert!(
            !matches!(
                action,
                HeaderSyncAction::Misbehavior { ref peer, .. } if *peer == peer_id
            ),
            "admission failure is local and non-punitive"
        );
    }
}

fn spawn_test_reactor(startup: HeaderSyncStartup) -> ReactorFixture {
    let (handle, actions, task) = spawn_header_sync_reactor(startup).unwrap();
    ReactorFixture {
        handle,
        actions,
        task,
        outbound_receivers: Mutex::new(Vec::new()),
    }
}

async fn next_action(actions: &mut mpsc::Receiver<HeaderSyncAction>) -> HeaderSyncAction {
    tokio::time::timeout(std::time::Duration::from_secs(5), actions.recv())
        .await
        .expect("action arrives before timeout")
        .expect("reactor action channel stays open")
}

async fn next_non_query_action(actions: &mut mpsc::Receiver<HeaderSyncAction>) -> HeaderSyncAction {
    loop {
        let action = next_action(actions).await;
        if !matches!(
            action,
            HeaderSyncAction::QueryBestHeaderTip
                | HeaderSyncAction::QueryMissingBlockBodies { .. }
                | HeaderSyncAction::QueryHeadersByHeightRange { .. }
                | HeaderSyncAction::HeaderAdvanced { .. }
        ) {
            return action;
        }
    }
}

async fn next_query_headers_action(
    actions: &mut mpsc::Receiver<HeaderSyncAction>,
) -> HeaderSyncAction {
    loop {
        let action = next_action(actions).await;
        if matches!(action, HeaderSyncAction::QueryHeadersByHeightRange { .. }) {
            return action;
        }
    }
}

async fn next_outbound_get_headers(
    actions: &mut mpsc::Receiver<HeaderSyncAction>,
) -> (ZakuraPeerId, block::Height, u32) {
    loop {
        match next_non_query_action(actions).await {
            HeaderSyncAction::SendMessage {
                peer,
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height,
                        count,
                    },
            } => return (peer, start_height, count),
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("unexpected misbehavior from {peer:?}: {reason:?}")
            }
            _ => {}
        }
    }
}

async fn assert_no_commit_or_misbehavior(actions: &mut mpsc::Receiver<HeaderSyncAction>) {
    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), actions.recv()).await
    {
        assert!(
            !matches!(
                action,
                HeaderSyncAction::CommitHeaderRange { .. } | HeaderSyncAction::Misbehavior { .. }
            ),
            "unexpected commit or misbehavior action: {action:?}"
        );
    }
}

async fn connect_peer(fixture: &ReactorFixture, peer_id: ZakuraPeerId) {
    connect_peer_with_direction(fixture, peer_id, ServicePeerDirection::Inbound).await;
}

async fn connect_peer_with_direction(
    fixture: &ReactorFixture,
    peer_id: ZakuraPeerId,
    direction: ServicePeerDirection,
) -> CancellationToken {
    let (send, recv) = crate::zakura::framed_channel(32);
    fixture
        .outbound_receivers
        .lock()
        .expect("test outbound receiver mutex ok")
        .push(recv);
    let cancel = CancellationToken::new();
    let session =
        HeaderSyncPeerSession::from_parts_with_direction(peer_id, direction, send, cancel.clone());
    fixture
        .handle
        .send(HeaderSyncEvent::PeerConnected(session))
        .await
        .unwrap();
    cancel
}

async fn advertise_tip(
    fixture: &ReactorFixture,
    peer_id: ZakuraPeerId,
    anchor_height: block::Height,
    tip_height: block::Height,
    max_headers_per_response: u32,
    max_inflight_requests: u16,
) {
    advertise_tip_with_hash(
        fixture,
        peer_id,
        anchor_height,
        tip_height,
        block::Hash([9; 32]),
        max_headers_per_response,
        max_inflight_requests,
    )
    .await;
}

async fn advertise_tip_with_hash(
    fixture: &ReactorFixture,
    peer_id: ZakuraPeerId,
    anchor_height: block::Height,
    tip_height: block::Height,
    tip_hash: block::Hash,
    max_headers_per_response: u32,
    max_inflight_requests: u16,
) {
    fixture
        .handle
        .send(HeaderSyncEvent::PeerStatusUpdated {
            peer: peer_id,
            status: HeaderSyncStatus {
                tip_height,
                tip_hash,
                anchor_height,
                max_headers_per_response,
                max_inflight_requests,
            },
        })
        .await
        .unwrap();
}

#[test]
fn codec_round_trips_status() {
    let status = HeaderSyncStatus {
        tip_height: block::Height(10),
        tip_hash: block::Hash([9; 32]),
        anchor_height: block::Height(1),
        max_headers_per_response: DEFAULT_HS_RANGE,
        max_inflight_requests: DEFAULT_HS_MAX_INFLIGHT,
    };
    let message = HeaderSyncMessage::Status(status);

    let encoded = message.encode().unwrap();
    let decoded = HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()).unwrap();

    assert_eq!(decoded, message);
}

#[test]
fn codec_round_trips_get_headers() {
    let message = HeaderSyncMessage::GetHeaders {
        start_height: block::Height(42),
        count: DEFAULT_HS_RANGE,
    };

    let encoded = message.encode().unwrap();
    let decoded = HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()).unwrap();

    assert_eq!(decoded, message);
}

#[test]
fn codec_round_trips_headers_with_bounded_vector() {
    let headers = vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)];
    let message = headers_message_with_sizes(headers, vec![123_456]);

    let encoded = message.encode().unwrap();
    let decoded = HeaderSyncMessage::decode(&encoded, headers_context(1, 1)).unwrap();

    assert_eq!(decoded, message);
}

#[test]
fn codec_round_trips_headers_with_unknown_body_size_sentinel() {
    let headers = vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)];
    let message = headers_message_with_sizes(headers, vec![0]);

    let encoded = message.encode().unwrap();
    let decoded = HeaderSyncMessage::decode(&encoded, headers_context(1, 1)).unwrap();

    assert_eq!(decoded, message);
}

#[test]
fn codec_round_trips_new_block() {
    let message = HeaderSyncMessage::NewBlock(mainnet_block(&BLOCK_MAINNET_1_BYTES));

    let encoded = message.encode().unwrap();
    let decoded = HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()).unwrap();

    assert_eq!(decoded, message);
}

#[test]
fn codec_rejects_unknown_message_types_and_trailing_bytes() {
    assert!(matches!(
        HeaderSyncMessage::decode(&[99], HeaderSyncDecodeContext::control()),
        Err(HeaderSyncWireError::UnknownMessageType(99))
    ));

    let mut encoded = HeaderSyncMessage::GetHeaders {
        start_height: block::Height(1),
        count: 1,
    }
    .encode()
    .unwrap();
    encoded.push(0);

    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()),
        Err(HeaderSyncWireError::TrailingBytes)
    ));
}

#[test]
fn headers_codec_rejects_body_size_mismatch_truncation_and_trailing_bytes() {
    let headers = vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)];
    let message = headers_message_with_sizes(headers.clone(), vec![100]);

    assert!(matches!(
        headers_message_with_sizes(headers.clone(), vec![100, 200]).encode(),
        Err(HeaderSyncWireError::BodySizeCountMismatch {
            headers: 1,
            body_sizes: 2,
        })
    ));

    let mut truncated_mid_size = message.encode().unwrap();
    truncated_mid_size.pop();
    assert!(HeaderSyncMessage::decode(&truncated_mid_size, headers_context(1, 1)).is_err());

    let mut truncated_mid_header = vec![MSG_HS_HEADERS];
    truncated_mid_header.write_u32::<LittleEndian>(1).unwrap();
    truncated_mid_header.extend_from_slice(&[0; 8]);
    assert!(HeaderSyncMessage::decode(&truncated_mid_header, headers_context(1, 1)).is_err());

    let mut with_trailing = message.encode().unwrap();
    with_trailing.push(0);
    assert!(matches!(
        HeaderSyncMessage::decode(&with_trailing, headers_context(1, 1)),
        Err(HeaderSyncWireError::TrailingBytes)
    ));
}

#[test]
fn frame_decode_rejects_oversized_payload_length_before_allocating() {
    let mut bytes = Vec::new();
    bytes
        .write_u16::<LittleEndian>(u16::from(MSG_HS_STATUS))
        .unwrap();
    bytes.write_u16::<LittleEndian>(0).unwrap();
    bytes
        .write_u32::<LittleEndian>(MAX_HS_MESSAGE_BYTES as u32 + 1)
        .unwrap();

    assert!(Frame::decode(&bytes, MAX_HS_MESSAGE_BYTES as u32).is_err());
}

#[test]
fn decode_rejects_header_counts_over_contract_caps() {
    let mut encoded = vec![MSG_HS_HEADERS];
    encoded.write_u32::<LittleEndian>(MAX_HS_RANGE + 1).unwrap();
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, headers_context(MAX_HS_RANGE, MAX_HS_RANGE)),
        Err(HeaderSyncWireError::HeaderCountLimit { .. })
    ));

    let mut encoded = vec![MSG_HS_HEADERS];
    encoded.write_u32::<LittleEndian>(2).unwrap();
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, headers_context(1, MAX_HS_RANGE)),
        Err(HeaderSyncWireError::HeaderCountLimit { actual: 2, max: 1 })
    ));

    let mut encoded = vec![MSG_HS_HEADERS];
    encoded.write_u32::<LittleEndian>(2).unwrap();
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, headers_context(MAX_HS_RANGE, 1)),
        Err(HeaderSyncWireError::HeaderCountLimit { actual: 2, max: 1 })
    ));
}

#[test]
fn headers_codec_does_not_use_legacy_160_header_cap() {
    let header = mainnet_header(&BLOCK_MAINNET_1_BYTES);
    let headers = vec![header; 161];
    let message = headers_message(headers);

    let encoded = message.encode().unwrap();
    let decoded = HeaderSyncMessage::decode(&encoded, headers_context(161, 161)).unwrap();

    match decoded {
        HeaderSyncMessage::Headers {
            headers,
            body_sizes,
        } => {
            assert_eq!(headers.len(), 161);
            assert_eq!(body_sizes, vec![0; 161]);
        }
        _ => panic!("decoded message must be Headers"),
    }
}

#[test]
fn get_headers_rejects_invalid_counts() {
    assert!(HeaderSyncMessage::GetHeaders {
        start_height: block::Height(1),
        count: 0,
    }
    .encode()
    .is_err());

    assert!(HeaderSyncMessage::GetHeaders {
        start_height: block::Height(1),
        count: MAX_HS_RANGE + 1,
    }
    .encode()
    .is_err());
}

#[test]
fn advertised_defaults_and_clamping_match_design() {
    let config = ZakuraHeaderSyncConfig::default();
    assert_eq!(config.max_headers_per_response, DEFAULT_HS_RANGE);
    assert_eq!(config.max_inflight_requests, DEFAULT_HS_MAX_INFLIGHT);
    assert!(config.accept_new_blocks);
    assert_eq!(
        ZakuraHeaderSyncConfig {
            max_inflight_requests: u16::MAX,
            ..ZakuraHeaderSyncConfig::default()
        }
        .advertised_max_inflight_requests(),
        LOCAL_MAX_HS_INFLIGHT_PER_PEER
    );

    let status = HeaderSyncStatus {
        max_headers_per_response: MAX_HS_RANGE + 10,
        ..HeaderSyncStatus::default()
    };
    let encoded = HeaderSyncMessage::Status(status).encode().unwrap();
    let decoded = HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()).unwrap();
    match decoded {
        HeaderSyncMessage::Status(status) => {
            assert_eq!(status.max_headers_per_response, MAX_HS_RANGE);
        }
        _ => panic!("decoded message must be Status"),
    }
}

#[test]
fn header_serialized_sizes_are_exact_and_message_cap_has_headroom() {
    let mainnet = mainnet_header(&BLOCK_MAINNET_GENESIS_BYTES);
    let mut mainnet_bytes = Vec::new();
    mainnet.zcash_serialize(&mut mainnet_bytes).unwrap();
    assert_eq!(mainnet_bytes.len(), COMMON_HEADER_BYTES);

    let testnet = mainnet_header(&BLOCK_TESTNET_GENESIS_BYTES);
    let mut testnet_bytes = Vec::new();
    testnet.zcash_serialize(&mut testnet_bytes).unwrap();
    assert_eq!(testnet_bytes.len(), COMMON_HEADER_BYTES);

    let mut regtest = *mainnet;
    regtest.solution = Solution::Regtest([0; 36]);
    let mut regtest_bytes = Vec::new();
    regtest.zcash_serialize(&mut regtest_bytes).unwrap();
    assert_eq!(regtest_bytes.len(), REGTEST_HEADER_BYTES);

    let default_response_bytes = HEADER_SYNC_MESSAGE_TYPE_BYTES
        + HEADER_SYNC_COUNT_BYTES
        + (COMMON_HEADER_BYTES + HEADER_SYNC_BODY_SIZE_BYTES) * DEFAULT_HS_RANGE as usize;
    assert!(default_response_bytes < MAX_HS_MESSAGE_BYTES);
    assert!(MAX_HS_MESSAGE_BYTES < LOCAL_MAX_MESSAGE_BYTES as usize);
}

#[test]
fn request_and_serving_counts_are_clamped_by_byte_budget() {
    let count = clamp_header_sync_request_count(
        MAX_HS_RANGE,
        MAX_HS_RANGE,
        &Network::Mainnet,
        LOCAL_MAX_MESSAGE_BYTES,
    );

    assert!(count < MAX_HS_RANGE);
    let headers =
        vec![mainnet_header(&BLOCK_MAINNET_1_BYTES); usize::try_from(count).unwrap() + 100];
    let headers =
        truncate_headers_to_byte_budget(headers, &Network::Mainnet, LOCAL_MAX_MESSAGE_BYTES);
    let encoded = headers_message(headers).encode().unwrap();

    assert!(encoded.len() <= MAX_HS_MESSAGE_BYTES);
    assert!(encoded.len() + FRAME_HEADER_BYTES <= LOCAL_MAX_MESSAGE_BYTES as usize);
}

#[tokio::test(flavor = "current_thread")]
async fn reactor_starts_from_storage_frontiers_and_publishes_watch() {
    let network = regtest_network();
    let best = (block::Height(7), block::Hash([7; 32]));
    let startup = HeaderSyncStartup::new(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        HeaderSyncFrontiers {
            finalized_height: block::Height(2),
            verified_block_tip: block::Height(5),
            verified_block_hash: block::Hash([5; 32]),
        },
        Some(best),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    let fixture = spawn_test_reactor(startup);

    assert_eq!(fixture.handle.best_header_tip(), best);
    assert_eq!(*fixture.handle.subscribe_tip().borrow(), best);
}

#[tokio::test(flavor = "current_thread")]
async fn restart_rebuilds_schedule_from_durable_best_tip_and_peer_status() {
    let network = regtest_network();
    let best = (block::Height(4), block::Hash([4; 32]));
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some(best),
    ));
    let peer_id = peer(41);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id,
        block::Height(0),
        block::Height(8),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    loop {
        if let HeaderSyncAction::SendMessage {
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(start_height, block::Height(5));
            assert_eq!(count, 4);
            break;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn handle_sends_events_and_peer_connect_sends_status_first() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(1);

    connect_peer(&fixture, peer_id.clone()).await;

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::SendMessage { peer, msg } => {
            assert_eq!(peer, peer_id);
            assert!(matches!(msg, HeaderSyncMessage::Status(_)));
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn status_updates_peer_caps_and_scheduler_respects_them() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(2);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(10),
        2,
        u16::MAX,
    )
    .await;

    let mut saw_get_headers = false;
    for _ in 0..4 {
        if let HeaderSyncAction::SendMessage {
            peer,
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                },
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, peer_id);
            assert_eq!(start_height, block::Height(1));
            assert_eq!(count, 2);
            saw_get_headers = true;
            break;
        }
    }
    assert!(saw_get_headers);
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_limits_v1_to_one_outstanding_request_per_peer() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(31);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(20),
        2,
        u16::MAX,
    )
    .await;

    let mut get_headers_count = 0;
    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if matches!(
            action,
            HeaderSyncAction::SendMessage {
                peer,
                msg: HeaderSyncMessage::GetHeaders { .. },
            } if peer == peer_id
        ) {
            get_headers_count += 1;
        }
    }

    assert_eq!(get_headers_count, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_fans_out_same_forward_range_to_three_peers() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peers = [peer(3), peer(4), peer(5)];

    for peer_id in peers.clone() {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(&fixture, peer_id, block::Height(0), block::Height(5), 5, 1).await;
    }

    let mut requested = HashSet::new();
    while requested.len() < HEADER_SYNC_FANOUT {
        if let HeaderSyncAction::SendMessage {
            peer,
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                },
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(start_height, block::Height(1));
            assert_eq!(count, 5);
            requested.insert(peer);
        }
    }

    assert_eq!(requested.len(), HEADER_SYNC_FANOUT);
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_narrows_large_ranges_before_tracking_fanout() {
    let network = Network::Mainnet;
    let first_checkpoint = network
        .checkpoint_list()
        .min_height_in_range(block::Height(1)..)
        .expect("mainnet has a checkpoint above genesis");
    let best_header_hash = block::Hash([3; 32]);
    let start = next_height(first_checkpoint).expect("checkpoint height has successor");
    let unclamped_tip = block::Height(
        start
            .0
            .checked_add(MAX_HS_RANGE)
            .expect("test range fits in height"),
    );
    let clamped_count = clamp_header_sync_request_count(
        MAX_HS_RANGE,
        MAX_HS_RANGE,
        &network,
        LOCAL_MAX_MESSAGE_BYTES,
    );
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((first_checkpoint, best_header_hash)),
    ));
    let peers = [peer(37), peer(38), peer(39), peer(40)];

    for peer_id in peers.clone() {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(
            &fixture,
            peer_id,
            block::Height(0),
            unclamped_tip,
            MAX_HS_RANGE,
            1,
        )
        .await;
    }

    let mut requested = HashSet::new();
    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if let HeaderSyncAction::SendMessage {
            peer,
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                },
        } = action
        {
            assert_eq!(start_height, start);
            assert_eq!(count, clamped_count);
            assert!(
                requested.insert(peer),
                "scheduler must not duplicate a clamped chunk for one peer"
            );
        }
    }
    assert_eq!(requested.len(), HEADER_SYNC_FANOUT);

    let chunk_tip = height_after_count(start, clamped_count)
        .and_then(previous_height)
        .expect("clamped range has a tip");
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeCommitted {
            start_height: start,
            tip_height: chunk_tip,
            tip_hash: block::Hash([4; 32]),
        })
        .await
        .unwrap();

    loop {
        if let HeaderSyncAction::SendMessage {
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(
                start_height,
                next_height(chunk_tip).expect("committed chunk tip has successor")
            );
            assert_eq!(count, clamped_count);
            break;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_creates_checkpoint_forward_before_backward_ranges() {
    let (network, checkpoint_hash) = checkpoint_regtest(block::Height(3));
    let mut fixture = spawn_test_reactor(startup_for(
        network,
        (block::Height(3), checkpoint_hash),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let peer_id = peer(6);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id,
        block::Height(0),
        block::Height(8),
        DEFAULT_HS_RANGE,
        10,
    )
    .await;

    loop {
        if let HeaderSyncAction::SendMessage {
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(start_height, block::Height(4));
            assert_eq!(count, 5);
            break;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_creates_backward_checkpoint_terminating_ranges() {
    let (network, checkpoint_hash) = checkpoint_regtest(block::Height(3));
    let mut fixture = spawn_test_reactor(startup_for(
        network,
        (block::Height(3), checkpoint_hash),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let peer_id = peer(7);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id,
        block::Height(0),
        block::Height(3),
        DEFAULT_HS_RANGE,
        10,
    )
    .await;

    loop {
        if let HeaderSyncAction::SendMessage {
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(start_height, block::Height(1));
            assert_eq!(count, 3);
            break;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn incoming_headers_match_outstanding_before_commit() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let first_checkpoint = block::Height(3);
    let start = block::Height(4);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((first_checkpoint, checkpoint_hash)),
    ));
    let peer_id = peer(8);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(&fixture, peer_id.clone(), block::Height(0), start, 1, 1).await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(vec![mainnet_header(&BLOCK_MAINNET_4_BYTES)]),
        ))
        .await
        .unwrap();

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::CommitHeaderRange {
            peer,
            start_height,
            finalized,
            ..
        } => {
            assert_eq!(peer, peer_id);
            assert_eq!(start_height, start);
            assert!(!finalized);
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn headers_over_outstanding_contract_reports_response_too_long_without_flooding() {
    let network = Network::Mainnet;
    let first_checkpoint = network
        .checkpoint_list()
        .min_height_in_range(block::Height(1)..)
        .expect("mainnet has a checkpoint above genesis");
    let previous_hash = block::Hash([1; 32]);
    let start = next_height(first_checkpoint).expect("checkpoint height has successor");
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((first_checkpoint, previous_hash)),
    ));
    let peer_id = peer(61);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(start.0 + 1),
        1,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { count: 1, .. },
                ..
            }
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ]),
        ))
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { peer, reason } => {
                assert_eq!(peer, peer_id);
                assert_eq!(reason, HeaderSyncMisbehavior::ResponseTooLong);
                break;
            }
            HeaderSyncAction::ForwardNewBlock { .. } => {
                panic!("backfill Headers must never produce tip-flood forwarding")
            }
            _ => {}
        }
    }
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_headers_are_statelessly_validated_before_commit() {
    let network = Network::Mainnet;
    let first_checkpoint = network
        .checkpoint_list()
        .min_height_in_range(block::Height(1)..)
        .expect("mainnet has a checkpoint above genesis");
    let two_before_checkpoint = block::Height(
        first_checkpoint
            .0
            .checked_sub(2)
            .expect("mainnet first checkpoint has two predecessors"),
    );
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((two_before_checkpoint, block::Hash([1; 32]))),
    ));
    let peer_id = peer(32);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        first_checkpoint,
        2,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }

    let mut bad_second = *mainnet_header(&BLOCK_MAINNET_2_BYTES);
    bad_second.previous_block_hash = block::Hash([7; 32]);
    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                Arc::new(bad_second),
            ]),
        ))
        .await
        .unwrap();

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
        }
        action => panic!("unexpected action: {action:?}"),
    }
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_async_header_commit_failure_reports_peer_disconnect() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(62);

    connect_peer(&fixture, peer_id.clone()).await;
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeCommitFailed {
            peer: peer_id.clone(),
            start_height: block::Height(1),
            count: 1,
            kind: HeaderSyncCommitFailureKind::InvalidPeerRange,
        })
        .await
        .unwrap();

    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
            break;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn peer_disconnect_removes_outstanding_requests_for_that_peer() {
    let network = Network::Mainnet;
    let first_checkpoint = network
        .checkpoint_list()
        .min_height_in_range(block::Height(1)..)
        .expect("mainnet has a checkpoint above genesis");
    let previous_checkpoint_height =
        previous_height(first_checkpoint).expect("checkpoint above genesis has predecessor");
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((previous_checkpoint_height, block::Hash([1; 32]))),
    ));
    let peer_id = peer(11);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        first_checkpoint,
        1,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::PeerDisconnected(peer_id.clone()))
        .await
        .unwrap();
    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)]),
        ))
        .await
        .unwrap();

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::UnsolicitedHeaders);
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn timed_out_range_retries_with_another_peer() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_with_timeout(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        std::time::Duration::from_millis(1),
    ));
    let first_peer = peer(12);
    let second_peer = peer(13);

    connect_peer(&fixture, first_peer.clone()).await;
    advertise_tip(
        &fixture,
        first_peer,
        block::Height(0),
        block::Height(2),
        2,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    connect_peer(&fixture, second_peer.clone()).await;
    advertise_tip(
        &fixture,
        second_peer.clone(),
        block::Height(0),
        block::Height(2),
        2,
        1,
    )
    .await;

    loop {
        if let HeaderSyncAction::SendMessage { peer, msg } =
            next_non_query_action(&mut fixture.actions).await
        {
            if matches!(msg, HeaderSyncMessage::GetHeaders { .. }) {
                assert_eq!(peer, second_peer);
                break;
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn covered_hedged_outstanding_ranges_do_not_commit_twice() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let first_peer = peer(33);
    let second_peer = peer(34);

    for peer_id in [first_peer.clone(), second_peer.clone()] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(&fixture, peer_id, block::Height(0), block::Height(2), 2, 1).await;
    }

    let mut requested = HashSet::new();
    while requested.len() < 2 {
        if let HeaderSyncAction::SendMessage { peer, msg } =
            next_non_query_action(&mut fixture.actions).await
        {
            if matches!(msg, HeaderSyncMessage::GetHeaders { .. }) {
                requested.insert(peer);
            }
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeCommitted {
            start_height: block::Height(1),
            tip_height: block::Height(2),
            tip_hash: block::Hash([2; 32]),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(narrowed_event(second_peer, headers_message(Vec::new())))
        .await
        .unwrap();

    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn late_covered_response_does_not_reanchor_newer_outstanding_range() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(74);
    let committed_hash = block::Hash([1; 32]);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(2),
        1,
        1,
    )
    .await;
    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::SendMessage {
                peer,
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height: block::Height(1),
                        count: 1,
                    },
            } if peer == peer_id => break,
            _ => {}
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeCommitted {
            start_height: block::Height(1),
            tip_height: block::Height(1),
            tip_hash: committed_hash,
        })
        .await
        .unwrap();
    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::SendMessage {
                peer,
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height: block::Height(2),
                        count: 1,
                    },
            } if peer == peer_id => break,
            _ => {}
        }
    }

    while tokio::time::timeout(std::time::Duration::from_millis(10), fixture.actions.recv())
        .await
        .is_ok()
    {}

    fixture
        .handle
        .send(narrowed_event(
            peer_id,
            headers_message(vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)]),
        ))
        .await
        .unwrap();

    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), fixture.actions.recv()).await
    {
        match action {
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
            | HeaderSyncAction::HeaderReanchored { .. }
            | HeaderSyncAction::Misbehavior { .. } => {
                panic!("late covered response must not trigger a new action: {action:?}")
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_commit_failure_retries_without_peer_misbehavior() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let start = block::Height(4);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let first_peer = peer(35);
    let second_peer = peer(36);

    for peer_id in [first_peer.clone(), second_peer.clone()] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(&fixture, peer_id, block::Height(0), start, 1, 1).await;
    }

    loop {
        if let HeaderSyncAction::SendMessage { peer, msg } =
            next_non_query_action(&mut fixture.actions).await
        {
            if matches!(msg, HeaderSyncMessage::GetHeaders { .. }) && peer == first_peer {
                break;
            }
        }
    }
    fixture
        .handle
        .send(narrowed_event(
            first_peer.clone(),
            headers_message(vec![mainnet_header(&BLOCK_MAINNET_4_BYTES)]),
        ))
        .await
        .unwrap();
    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { .. } => {
                panic!("valid headers must not be scored before local commit failure")
            }
            HeaderSyncAction::CommitHeaderRange {
                peer,
                start_height,
                headers,
                ..
            } => {
                assert_eq!(peer, first_peer);
                assert_eq!(start_height, start);
                assert_eq!(headers.len(), 1);
                break;
            }
            _ => {}
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeCommitFailed {
            peer: first_peer.clone(),
            start_height: start,
            count: 1,
            kind: HeaderSyncCommitFailureKind::Local,
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { .. } => {
                panic!("local commit failure must not score peer")
            }
            HeaderSyncAction::SendMessage {
                peer,
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height,
                        count,
                    },
            } if peer == first_peer || peer == second_peer => {
                assert_eq!(start_height, start);
                assert_eq!(count, 1);
                break;
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn material_tip_advance_sends_rate_limited_unsolicited_status() {
    let network = regtest_network();
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    startup.status_refresh_interval = std::time::Duration::from_secs(60);
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(14);

    connect_peer(&fixture, peer_id.clone()).await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::Status(_),
                ..
            }
        ) {
            break;
        }
    }

    for height in [block::Height(1), block::Height(2)] {
        fixture
            .handle
            .send(HeaderSyncEvent::HeaderRangeCommitted {
                start_height: height,
                tip_height: height,
                tip_hash: block::Hash(
                    [u8::try_from(height.0).expect("test heights fit in u8"); 32],
                ),
            })
            .await
            .unwrap();
    }

    let mut status_count = 0;
    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(20), fixture.actions.recv()).await
    {
        if matches!(
            action,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::Status(_),
                ..
            }
        ) {
            status_count += 1;
        }
    }

    assert_eq!(status_count, 1);
}

#[test]
fn peer_state_suppresses_redundant_status_until_session_reset() {
    let (send, _recv) = crate::zakura::framed_channel(32);
    let session = HeaderSyncPeerSession::from_parts_with_direction(
        peer(80),
        ServicePeerDirection::Inbound,
        send,
        CancellationToken::new(),
    );
    let mut peer_state = super::state::PeerHeaderState::new(
        session,
        (block::Height(0), block::Hash([0; 32])),
        DEFAULT_HS_RANGE,
        DEFAULT_HS_MAX_INFLIGHT,
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
    );

    let status = HeaderSyncStatus {
        tip_height: block::Height(5),
        tip_hash: block::Hash([5; 32]),
        ..HeaderSyncStatus::default()
    };

    // Nothing has been sent yet, so the first status is always new.
    assert!(peer_state.status_differs_from_last_sent(status));
    peer_state.record_sent_status(status);

    // An identical status is redundant and must be suppressed.
    assert!(!peer_state.status_differs_from_last_sent(status));

    // A tip-advancing status differs and is sent.
    let advanced = HeaderSyncStatus {
        tip_height: block::Height(6),
        ..status
    };
    assert!(peer_state.status_differs_from_last_sent(advanced));

    // A same-height hash change (e.g. a reorg at the tip) also differs.
    let reorged = HeaderSyncStatus {
        tip_hash: block::Hash([9; 32]),
        ..status
    };
    assert!(peer_state.status_differs_from_last_sent(reorged));

    // Replacing the session forgets the last status, so an identical status is
    // resent — a fresh channel's remote has not received it and gates serving on it.
    peer_state.reset_sent_status();
    assert!(peer_state.status_differs_from_last_sent(status));
}

#[tokio::test(flavor = "current_thread")]
async fn reconnect_resends_initial_status_after_session_reset() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(72);

    // First connect: the peer receives its initial status.
    connect_peer(&fixture, peer_id.clone()).await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            msg: HeaderSyncMessage::Status(_),
            ..
        }
    ));

    // Reconnecting installs a fresh session at the same frontier. Even though the
    // status is byte-identical to the one already sent, the new channel's remote
    // has not received it, so it must be resent rather than suppressed.
    connect_peer(&fixture, peer_id.clone()).await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            msg: HeaderSyncMessage::Status(_),
            ..
        }
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn reconnect_clears_session_bound_outstanding_ranges() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(73);

    connect_peer(&fixture, peer_id.clone()).await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            msg: HeaderSyncMessage::Status(_),
            ..
        }
    ));
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(5),
        1,
        1,
    )
    .await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            peer,
            msg: HeaderSyncMessage::GetHeaders {
                start_height: block::Height(1),
                count: 1,
            },
        } if peer == peer_id
    ));

    connect_peer(&fixture, peer_id.clone()).await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            msg: HeaderSyncMessage::Status(_),
            ..
        }
    ));
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(5),
        1,
        1,
    )
    .await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            peer,
            msg: HeaderSyncMessage::GetHeaders {
                start_height: block::Height(1),
                count: 1,
            },
        } if peer == peer_id
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn full_block_committed_covers_outstanding_height() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(42);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: block::Height(1),
            hash: block::Hash([1; 32]),
            header: mainnet_header(&BLOCK_MAINNET_1_BYTES),
        })
        .await
        .unwrap();
    match next_action(&mut fixture.actions).await {
        HeaderSyncAction::HeaderAdvanced { height, hash } => {
            assert_eq!(height, block::Height(1));
            assert_eq!(hash, block::Hash([1; 32]));
        }
        action => panic!("full block commit must publish a header advance, got {action:?}"),
    }
    fixture
        .handle
        .send(narrowed_event(peer_id, headers_message(Vec::new())))
        .await
        .unwrap();

    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_unseen_valid_new_block_is_seen_and_forwarded_to_eligible_peers() {
    let network = Network::Mainnet;
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let hash = block.hash();
    let height = block.coinbase_height().expect("test block has height");
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let source = peer(46);
    let eligible = peer(47);
    let redundant = peer(48);

    for peer_id in [source.clone(), eligible.clone(), redundant.clone()] {
        connect_peer(&fixture, peer_id).await;
    }
    advertise_tip(
        &fixture,
        source.clone(),
        block::Height(0),
        block::Height(0),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    advertise_tip(
        &fixture,
        eligible.clone(),
        block::Height(0),
        block::Height(0),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    advertise_tip(
        &fixture,
        redundant.clone(),
        block::Height(0),
        height,
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    fixture
        .handle
        .send(narrowed_event(
            source.clone(),
            HeaderSyncMessage::NewBlock(block.clone()),
        ))
        .await
        .unwrap();

    let mut saw_pipeline_fact = false;
    let mut forwarded = Vec::new();
    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        fixture.actions.recv(),
    )
    .await
    {
        match action {
            HeaderSyncAction::NewBlockReceived {
                peer,
                height: action_height,
                hash: action_hash,
                ..
            } => {
                assert_eq!(peer, source);
                assert_eq!(action_height, height);
                assert_eq!(action_hash, hash);
                saw_pipeline_fact = true;
                fixture
                    .handle
                    .send(HeaderSyncEvent::NewBlockAccepted {
                        peer: source.clone(),
                        height,
                        hash,
                        block: block.clone(),
                    })
                    .await
                    .unwrap();
            }
            HeaderSyncAction::ForwardNewBlock {
                source: action_source,
                peer,
                height: action_height,
                hash: action_hash,
                ..
            } => {
                assert_eq!(action_source, Some(source.clone()));
                assert_eq!(action_height, height);
                assert_eq!(action_hash, hash);
                forwarded.push(peer);
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("valid NewBlock must not score {peer:?}: {reason:?}");
            }
            _ => {}
        }
    }

    assert!(saw_pipeline_fact);
    assert_eq!(forwarded, vec![eligible]);

    fixture
        .handle
        .send(narrowed_event(
            source.clone(),
            HeaderSyncMessage::NewBlock(block),
        ))
        .await
        .unwrap();

    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if matches!(
            action,
            HeaderSyncAction::ForwardNewBlock { .. }
                | HeaderSyncAction::NewBlockReceived { .. }
                | HeaderSyncAction::Misbehavior { .. }
        ) {
            panic!("duplicate NewBlock must be cheap-deduped without scoring: {action:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_duplicate_new_block_dedups_pending_acceptance_without_scoring() {
    let network = Network::Mainnet;
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let hash = block.hash();
    let height = block.coinbase_height().expect("test block has height");
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let first_peer = peer(52);
    let duplicate_peer = peer(53);
    let eligible_peer = peer(54);

    for peer_id in [
        first_peer.clone(),
        duplicate_peer.clone(),
        eligible_peer.clone(),
    ] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(
            &fixture,
            peer_id,
            block::Height(0),
            block::Height(0),
            DEFAULT_HS_RANGE,
            1,
        )
        .await;
    }

    fixture
        .handle
        .send(narrowed_event(
            first_peer.clone(),
            HeaderSyncMessage::NewBlock(block.clone()),
        ))
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::NewBlockReceived {
                peer,
                height: action_height,
                hash: action_hash,
                ..
            } => {
                assert_eq!(peer, first_peer);
                assert_eq!(action_height, height);
                assert_eq!(action_hash, hash);
                break;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("first valid NewBlock must not score {peer:?}: {reason:?}");
            }
            _ => {}
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            duplicate_peer.clone(),
            HeaderSyncMessage::NewBlock(block.clone()),
        ))
        .await
        .unwrap();

    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if matches!(
            action,
            HeaderSyncAction::NewBlockReceived { .. } | HeaderSyncAction::Misbehavior { .. }
        ) {
            panic!("pending duplicate NewBlock must not re-enter acceptance or score: {action:?}");
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::NewBlockAccepted {
            peer: first_peer,
            height,
            hash,
            block,
        })
        .await
        .unwrap();

    let mut forwarded = HashSet::new();
    while forwarded.len() < 2 {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::ForwardNewBlock {
                peer,
                height: action_height,
                hash: action_hash,
                ..
            } => {
                assert_eq!(action_height, height);
                assert_eq!(action_hash, hash);
                forwarded.insert(peer);
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("accepted duplicate flow must not score {peer:?}: {reason:?}");
            }
            _ => {}
        }
    }
    assert_eq!(forwarded, HashSet::from([duplicate_peer, eligible_peer]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_full_block_commit_prevents_later_new_block_regossip() {
    let network = Network::Mainnet;
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let height = block.coinbase_height().expect("test block has height");
    let hash = block.hash();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let source = peer(49);
    let destination = peer(50);

    for peer_id in [source.clone(), destination] {
        connect_peer(&fixture, peer_id).await;
    }
    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height,
            hash,
            header: block.header.clone(),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(narrowed_event(source, HeaderSyncMessage::NewBlock(block)))
        .await
        .unwrap();

    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if matches!(action, HeaderSyncAction::ForwardNewBlock { .. }) {
            panic!("locally committed block must not be gossiped twice: {action:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_and_malformed_new_block_report_disconnect() {
    let network = Network::Mainnet;
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let unknown_peer = peer(63);
    let invalid_peer = peer(51);
    let malformed_peer = peer(52);
    connect_peer(&fixture, invalid_peer.clone()).await;
    connect_peer(&fixture, malformed_peer.clone()).await;

    fixture
        .handle
        .send(narrowed_event(
            unknown_peer.clone(),
            HeaderSyncMessage::NewBlock(mainnet_block(&BLOCK_MAINNET_1_BYTES)),
        ))
        .await
        .unwrap();
    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, unknown_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::UnknownPeer);
            break;
        }
    }

    let mut bad_block = (*mainnet_block(&BLOCK_MAINNET_1_BYTES)).clone();
    let mut bad_header = *bad_block.header;
    bad_header.nonce[0] ^= 1;
    bad_block.header = Arc::new(bad_header);
    fixture
        .handle
        .send(narrowed_event(
            invalid_peer.clone(),
            HeaderSyncMessage::NewBlock(Arc::new(bad_block)),
        ))
        .await
        .unwrap();

    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, invalid_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidNewBlock);
            break;
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::PeerMisbehavior {
            peer: malformed_peer.clone(),
            reason: HeaderSyncMisbehavior::MalformedMessage,
        })
        .await
        .unwrap();

    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, malformed_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::MalformedMessage);
            break;
        }
    }
}

/// Status-spam classification moved into the routine. A second non-advancing
/// status inside the rate window is `StatusSpam`: the routine emits
/// `PeerMisbehavior` but, matching the chunk-02 baseline's record-only handling,
/// does NOT reject the peer — it keeps processing frames (`Flow::Continue`). The
/// first status is accepted.
#[test]
fn rapid_status_updates_report_status_spam_in_routine() {
    let mut harness = RoutineHarness::new(peer(53));
    let status = || {
        HeaderSyncMessage::Status(HeaderSyncStatus {
            tip_height: block::Height(1),
            tip_hash: block::Hash([9; 32]),
            anchor_height: block::Height(0),
            max_headers_per_response: DEFAULT_HS_RANGE,
            max_inflight_requests: 1,
        })
    };
    assert!(matches!(
        harness.ingest(status()),
        crate::zakura::Flow::Continue(())
    ));
    assert!(matches!(
        harness.try_next_event(),
        Some(HeaderSyncEvent::PeerStatusUpdated { .. })
    ));

    // Status spam is a decoded-message rate violation: record-only at the baseline,
    // so the routine continues rather than tearing down the connection.
    assert!(matches!(
        harness.ingest(status()),
        crate::zakura::Flow::Continue(())
    ));
    match harness.try_next_event() {
        Some(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
            assert_eq!(reason, HeaderSyncMisbehavior::StatusSpam);
        }
        other => panic!("expected PeerMisbehavior(StatusSpam), got {other:?}"),
    }
}

/// `NewBlock` spam classification stays reactor-side: the semantic, post-dedup
/// flood meter fires for distinct unseen blocks delivered faster than the budget.
/// The narrowed `NewBlockCandidate` event re-enters the reactor's dedup + meter
/// path exactly as a routine-forwarded candidate would.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_block_spam_reports_disconnect() {
    let network = Network::Mainnet;
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let block_peer = peer(54);
    connect_peer(&fixture, block_peer.clone()).await;

    for bytes in [
        BLOCK_MAINNET_1_BYTES.as_slice(),
        BLOCK_MAINNET_2_BYTES.as_slice(),
    ] {
        fixture
            .handle
            .send(narrowed_event(
                block_peer.clone(),
                HeaderSyncMessage::NewBlock(mainnet_block(bytes)),
            ))
            .await
            .unwrap();
    }

    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, block_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::NewBlockSpam);
            break;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rapid_advancing_status_updates_are_not_spam() {
    let network = Network::Mainnet;
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let status_peer = peer(55);
    connect_peer(&fixture, status_peer.clone()).await;

    advertise_tip(
        &fixture,
        status_peer.clone(),
        block::Height(0),
        block::Height(1),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    advertise_tip(
        &fixture,
        status_peer,
        block::Height(0),
        block::Height(2),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if let HeaderSyncAction::Misbehavior { reason, .. } = action {
            panic!("advancing status update was reported as {reason:?}");
        }
    }
}

/// Status-spam classification moved into the routine. A second non-tip-advancing
/// status at the same height (only the hash churned) inside the rate window is
/// classified `StatusSpam` by the routine, which emits `PeerMisbehavior` but keeps
/// the connection alive (record-only at the baseline, `Flow::Continue`). The first
/// status is accepted (`PeerStatusUpdated`).
#[test]
fn same_height_hash_churn_is_status_spam() {
    let mut harness = RoutineHarness::new(peer(59));

    let first = HeaderSyncMessage::Status(HeaderSyncStatus {
        tip_height: block::Height(1),
        tip_hash: block::Hash([1; 32]),
        anchor_height: block::Height(0),
        max_headers_per_response: DEFAULT_HS_RANGE,
        max_inflight_requests: 1,
    });
    assert!(matches!(
        harness.ingest(first),
        crate::zakura::Flow::Continue(())
    ));
    assert!(matches!(
        harness.try_next_event(),
        Some(HeaderSyncEvent::PeerStatusUpdated { .. })
    ));

    let churned = HeaderSyncMessage::Status(HeaderSyncStatus {
        tip_height: block::Height(1),
        tip_hash: block::Hash([2; 32]),
        anchor_height: block::Height(0),
        max_headers_per_response: DEFAULT_HS_RANGE,
        max_inflight_requests: 1,
    });
    assert!(matches!(
        harness.ingest(churned),
        crate::zakura::Flow::Continue(())
    ));
    match harness.try_next_event() {
        Some(HeaderSyncEvent::PeerMisbehavior { peer, reason }) => {
            assert_eq!(peer, harness.peer);
            assert_eq!(reason, HeaderSyncMisbehavior::StatusSpam);
        }
        other => panic!("expected PeerMisbehavior(StatusSpam), got {other:?}"),
    }
}

// ===================== chunk 03: routine-local validation =====================
//
// The following tests drive the relocated peer-local validation directly through
// the routine ingest path. They cover the gates the chunk moved off the reactor.

/// A valid `Status` updates the peer summary through the typed event path: the
/// routine forwards `PeerStatusUpdated` carrying the accepted status. No raw wire
/// message reaches the reactor.
#[test]
fn routine_valid_status_updates_summary_via_typed_event() {
    let mut harness = RoutineHarness::new(peer(70));
    let status = HeaderSyncStatus {
        tip_height: block::Height(5),
        tip_hash: block::Hash([3; 32]),
        anchor_height: block::Height(1),
        max_headers_per_response: 7,
        max_inflight_requests: 4,
    };

    assert!(matches!(
        harness.ingest(HeaderSyncMessage::Status(status)),
        crate::zakura::Flow::Continue(())
    ));
    match harness.try_next_event() {
        Some(HeaderSyncEvent::PeerStatusUpdated {
            peer,
            status: forwarded,
        }) => {
            assert_eq!(peer, harness.peer);
            assert_eq!(forwarded.tip_height, block::Height(5));
            assert_eq!(forwarded.anchor_height, block::Height(1));
        }
        other => panic!("expected PeerStatusUpdated, got {other:?}"),
    }
}

/// An impossible `Status` (`anchor_height > tip_height`) is `InvalidStatus`: the
/// routine emits `PeerMisbehavior` but, matching the chunk-02 baseline's
/// record-only handling, keeps the connection alive (`Flow::Continue`).
#[test]
fn routine_invalid_status_is_misbehavior() {
    let mut harness = RoutineHarness::new(peer(71));
    let status = HeaderSyncStatus {
        tip_height: block::Height(0),
        tip_hash: block::Hash([0; 32]),
        anchor_height: block::Height(1),
        max_headers_per_response: 1,
        max_inflight_requests: 1,
    };

    assert!(matches!(
        harness.ingest(HeaderSyncMessage::Status(status)),
        crate::zakura::Flow::Continue(())
    ));
    match harness.try_next_event() {
        Some(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidStatus);
        }
        other => panic!("expected PeerMisbehavior(InvalidStatus), got {other:?}"),
    }
}

/// Advertised caps are clamped by the routine before the summary is forwarded: an
/// over-large `max_headers_per_response` is clamped to `MAX_HS_RANGE`, and a zero
/// `max_inflight_requests` is clamped up to 1.
#[test]
fn routine_clamps_advertised_caps() {
    let mut harness = RoutineHarness::new(peer(72));
    let status = HeaderSyncStatus {
        tip_height: block::Height(1),
        tip_hash: block::Hash([9; 32]),
        anchor_height: block::Height(0),
        max_headers_per_response: MAX_HS_RANGE + 1000,
        max_inflight_requests: 0,
    };

    harness.ingest(HeaderSyncMessage::Status(status));
    match harness.try_next_event() {
        Some(HeaderSyncEvent::PeerStatusUpdated { status, .. }) => {
            assert_eq!(status.max_headers_per_response, MAX_HS_RANGE);
            assert_eq!(status.max_inflight_requests, 1);
        }
        other => panic!("expected PeerStatusUpdated, got {other:?}"),
    }
}

/// A valid inbound `GetHeaders` from a status-confirmed peer is forwarded as
/// `InboundGetHeadersRequested` (which the reactor dispatches to the backend).
#[test]
fn routine_valid_get_headers_requests_backend_lookup() {
    let mut harness = RoutineHarness::new(peer(73));
    // First confirm status so the received-status gate passes.
    harness.ingest(HeaderSyncMessage::Status(HeaderSyncStatus {
        tip_height: block::Height(1),
        tip_hash: block::Hash([9; 32]),
        anchor_height: block::Height(0),
        max_headers_per_response: DEFAULT_HS_RANGE,
        max_inflight_requests: 1,
    }));
    assert!(matches!(
        harness.try_next_event(),
        Some(HeaderSyncEvent::PeerStatusUpdated { .. })
    ));

    assert!(matches!(
        harness.ingest(HeaderSyncMessage::GetHeaders {
            start_height: block::Height(2),
            count: 3,
        }),
        crate::zakura::Flow::Continue(())
    ));
    match harness.try_next_event() {
        Some(HeaderSyncEvent::InboundGetHeadersRequested {
            start_height,
            count,
            ..
        }) => {
            assert_eq!(start_height, block::Height(2));
            assert_eq!(count, 3);
        }
        other => panic!("expected InboundGetHeadersRequested, got {other:?}"),
    }
}

/// Inbound `GetHeaders` before any status is `GetHeadersSpam` (the received-status
/// gate the routine owns); an over-cap count is `GetHeadersTooLong`. Both operate on
/// a DECODED `GetHeaders`, so they are record-only at the chunk-02 baseline: the
/// routine emits `PeerMisbehavior` but keeps the connection (`Flow::Continue`).
#[test]
fn routine_get_headers_gates_status_and_count() {
    let mut harness = RoutineHarness::new(peer(74));

    // No status yet: the received-status gate records GetHeadersSpam and continues.
    assert!(matches!(
        harness.ingest(HeaderSyncMessage::GetHeaders {
            start_height: block::Height(1),
            count: 1,
        }),
        crate::zakura::Flow::Continue(())
    ));
    match harness.try_next_event() {
        Some(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
            assert_eq!(reason, HeaderSyncMisbehavior::GetHeadersSpam);
        }
        other => panic!("expected PeerMisbehavior(GetHeadersSpam), got {other:?}"),
    }

    // Confirm status, then send an over-cap request: GetHeadersTooLong.
    harness.ingest(HeaderSyncMessage::Status(HeaderSyncStatus {
        tip_height: block::Height(1),
        tip_hash: block::Hash([9; 32]),
        anchor_height: block::Height(0),
        max_headers_per_response: DEFAULT_HS_RANGE,
        max_inflight_requests: 1,
    }));
    assert!(matches!(
        harness.try_next_event(),
        Some(HeaderSyncEvent::PeerStatusUpdated { .. })
    ));
    // A count above this node's advertised serving cap (DEFAULT_HS_RANGE) but still
    // encodable (<= MAX_HS_RANGE) exceeds the inbound count limit: GetHeadersTooLong,
    // record-only, so the routine continues.
    assert!(matches!(
        harness.ingest(HeaderSyncMessage::GetHeaders {
            start_height: block::Height(1),
            count: DEFAULT_HS_RANGE + 1,
        }),
        crate::zakura::Flow::Continue(())
    ));
    match harness.try_next_event() {
        Some(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
            assert_eq!(reason, HeaderSyncMisbehavior::GetHeadersTooLong);
        }
        other => panic!("expected PeerMisbehavior(GetHeadersTooLong), got {other:?}"),
    }
}

/// A correlated `Headers` response (matching a recorded expectation) is forwarded
/// as `PeerHeadersReceived`; a body-size/header-count mismatch is `MalformedMessage`.
#[test]
fn routine_headers_correlation_and_shape_validation() {
    let mut harness = RoutineHarness::new(peer(75));
    harness.record_expected(block::Height(1), 1);

    // A valid, correlated one-header response is forwarded as PeerHeadersReceived.
    let header = mainnet_header(&BLOCK_MAINNET_1_BYTES);
    assert!(matches!(
        harness.ingest(headers_message_with_sizes(vec![header.clone()], vec![0])),
        crate::zakura::Flow::Continue(())
    ));
    match harness.try_next_event() {
        Some(HeaderSyncEvent::PeerHeadersReceived {
            headers,
            body_sizes,
            ..
        }) => {
            assert_eq!(headers.len(), 1);
            assert_eq!(body_sizes, vec![0]);
        }
        other => panic!("expected PeerHeadersReceived, got {other:?}"),
    }
}

/// A valid `NewBlock` decode is forwarded as a `NewBlockCandidate` for global
/// acceptance; the routine does not run global dedup or stateless validation.
#[test]
fn routine_new_block_forwards_candidate() {
    let mut harness = RoutineHarness::new(peer(76));
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);

    assert!(matches!(
        harness.ingest(HeaderSyncMessage::NewBlock(block.clone())),
        crate::zakura::Flow::Continue(())
    ));
    match harness.try_next_event() {
        Some(HeaderSyncEvent::NewBlockCandidate {
            block: candidate, ..
        }) => {
            assert_eq!(candidate.hash(), block.hash());
        }
        other => panic!("expected NewBlockCandidate, got {other:?}"),
    }
}

/// A malformed stream-5 frame (an empty `Status` payload) is a protocol reject and
/// `MalformedMessage` misbehavior, so it still disconnects the peer.
#[test]
fn routine_malformed_frame_disconnects() {
    let mut harness = RoutineHarness::new(peer(77));
    let malformed = Frame {
        message_type: u16::from(MSG_HS_STATUS),
        flags: 0,
        payload: Vec::new(),
    };

    let flow = super::pipe::decode_and_ingest(
        &mut harness.local,
        &harness.env,
        harness.peer.clone(),
        malformed,
    );
    assert!(matches!(
        flow,
        crate::zakura::Flow::Reject(crate::zakura::SinkReject::Protocol(_))
    ));
    match harness.try_next_event() {
        Some(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
            assert_eq!(reason, HeaderSyncMisbehavior::MalformedMessage);
        }
        other => panic!("expected PeerMisbehavior(MalformedMessage), got {other:?}"),
    }
}

/// The inbound status/response receive metrics moved into the routine with chunk
/// 03: the routine increments `peer.status.received` and `response.received` as it
/// decodes each inbound message. (`tip.new_block.received` stays reactor-side,
/// emitted once the `NewBlockCandidate` reaches the reactor.)
#[test]
fn routine_records_inbound_receive_metrics() {
    let names = [
        "sync.header.peer.status.received",
        "sync.header.response.received",
    ];
    let before = metric_snapshot(&names);

    let mut harness = RoutineHarness::new(peer(80));
    harness.ingest(HeaderSyncMessage::Status(HeaderSyncStatus {
        tip_height: block::Height(1),
        tip_hash: block::Hash([9; 32]),
        anchor_height: block::Height(0),
        max_headers_per_response: DEFAULT_HS_RANGE,
        max_inflight_requests: 1,
    }));
    harness.record_expected(block::Height(1), 1);
    harness.ingest(headers_message_with_sizes(
        vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)],
        vec![0],
    ));

    for name in names {
        assert_metric_incremented(&before, name);
    }
}

/// Gate: routine-to-reactor events are narrowed shared effects, NOT a one-for-one
/// mirror of the four wire-message variants. There are five shared-effect events,
/// `PeerMisbehavior` has no wire counterpart (every wire variant can produce it),
/// and the invalid/spam paths produce no positive event at all.
#[test]
fn routine_events_do_not_mirror_wire_variants() {
    // Four wire variants: Status, GetHeaders, Headers, NewBlock.
    // The narrowed events a routine emits are a different set, with PeerMisbehavior
    // as a cross-cutting effect produced by ANY wire variant's validation failure.
    let mut spam = RoutineHarness::new(peer(78));
    // A Status produces PeerStatusUpdated...
    spam.ingest(HeaderSyncMessage::Status(HeaderSyncStatus {
        tip_height: block::Height(1),
        tip_hash: block::Hash([9; 32]),
        anchor_height: block::Height(0),
        max_headers_per_response: DEFAULT_HS_RANGE,
        max_inflight_requests: 1,
    }));
    assert!(matches!(
        spam.try_next_event(),
        Some(HeaderSyncEvent::PeerStatusUpdated { .. })
    ));
    // ...but a second, non-advancing Status produces PeerMisbehavior, NOT a second
    // PeerStatusUpdated — so Status does not map one-for-one onto a single event.
    spam.ingest(HeaderSyncMessage::Status(HeaderSyncStatus {
        tip_height: block::Height(1),
        tip_hash: block::Hash([9; 32]),
        anchor_height: block::Height(0),
        max_headers_per_response: DEFAULT_HS_RANGE,
        max_inflight_requests: 1,
    }));
    match spam.try_next_event() {
        Some(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
            assert_eq!(reason, HeaderSyncMisbehavior::StatusSpam);
        }
        other => panic!("expected PeerMisbehavior(StatusSpam), got {other:?}"),
    }

    // An unsolicited Headers (a different wire variant) ALSO collapses to
    // PeerMisbehavior, proving the misbehavior event is not a per-variant mirror.
    let mut unsolicited = RoutineHarness::new(peer(79));
    let header = mainnet_header(&BLOCK_MAINNET_1_BYTES);
    unsolicited.ingest(headers_message_with_sizes(vec![header], vec![0]));
    match unsolicited.try_next_event() {
        Some(HeaderSyncEvent::PeerMisbehavior { reason, .. }) => {
            assert_eq!(reason, HeaderSyncMisbehavior::UnsolicitedHeaders);
        }
        other => panic!("expected PeerMisbehavior(UnsolicitedHeaders), got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_height_hash_change_with_token_is_accepted() {
    let network = Network::Mainnet;
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let status_peer = peer(60);
    connect_peer(&fixture, status_peer.clone()).await;

    advertise_tip_with_hash(
        &fixture,
        status_peer,
        block::Height(0),
        block::Height(0),
        block::Hash([3; 32]),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if let HeaderSyncAction::Misbehavior { reason, .. } = action {
            panic!("same-height status update with a token was reported as {reason:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_block_spam_does_not_poison_seen_cache() {
    let network = Network::Mainnet;
    let first_block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let second_block = mainnet_block(&BLOCK_MAINNET_2_BYTES);
    let second_hash = second_block.hash();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let spam_peer = peer(56);
    let honest_peer = peer(57);
    let destination = peer(58);

    for peer_id in [spam_peer.clone(), honest_peer.clone(), destination] {
        connect_peer(&fixture, peer_id).await;
    }

    fixture
        .handle
        .send(narrowed_event(
            spam_peer.clone(),
            HeaderSyncMessage::NewBlock(first_block),
        ))
        .await
        .unwrap();
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::NewBlockReceived { hash, .. } if hash != second_hash
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            spam_peer.clone(),
            HeaderSyncMessage::NewBlock(second_block.clone()),
        ))
        .await
        .unwrap();
    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, spam_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::NewBlockSpam);
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            honest_peer.clone(),
            HeaderSyncMessage::NewBlock(second_block),
        ))
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::NewBlockReceived { peer, hash, .. } if hash == second_hash => {
                assert_eq!(peer, honest_peer);
                break;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("honest retry must not be deduped or scored: {peer:?} {reason:?}");
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_new_block_does_not_forward_or_poison_seen_cache() {
    let network = Network::Mainnet;
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let hash = block.hash();
    let height = block.coinbase_height().expect("test block has height");
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let source = peer(59);
    let retry_peer = peer(60);
    let destination = peer(61);

    for peer_id in [source.clone(), retry_peer.clone(), destination] {
        connect_peer(&fixture, peer_id).await;
    }

    fixture
        .handle
        .send(narrowed_event(
            source.clone(),
            HeaderSyncMessage::NewBlock(block.clone()),
        ))
        .await
        .unwrap();

    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::NewBlockReceived { peer, hash: action_hash, .. }
                if peer == source && action_hash == hash
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::NewBlockRejected {
            peer: source.clone(),
            hash,
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { peer, reason } => {
                assert_eq!(peer, source);
                assert_eq!(reason, HeaderSyncMisbehavior::InvalidNewBlock);
                break;
            }
            HeaderSyncAction::ForwardNewBlock { .. } => {
                panic!("rejected NewBlock must not be forwarded");
            }
            _ => {}
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            retry_peer.clone(),
            HeaderSyncMessage::NewBlock(block),
        ))
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::NewBlockReceived {
                peer,
                height: action_height,
                hash: action_hash,
                ..
            } => {
                assert_eq!(peer, retry_peer);
                assert_eq!(action_height, height);
                assert_eq!(action_hash, hash);
                break;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("retry after rejection must not be scored: {peer:?} {reason:?}");
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn inbound_get_headers_requires_status_and_respects_serving_cap() {
    let network = regtest_network();
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    startup.config.max_headers_per_response = 3;
    startup.config.max_inflight_requests = 2;
    let mut fixture = spawn_test_reactor(startup);
    let no_status_peer = peer(59);
    let requester = peer(60);

    connect_peer(&fixture, no_status_peer.clone()).await;
    fixture
        .handle
        .send(narrowed_event(
            no_status_peer.clone(),
            HeaderSyncMessage::GetHeaders {
                start_height: block::Height(1),
                count: 1,
            },
        ))
        .await
        .unwrap();
    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, no_status_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::GetHeadersSpam);
            break;
        }
    }

    connect_peer(&fixture, requester.clone()).await;
    advertise_tip(
        &fixture,
        requester.clone(),
        block::Height(0),
        block::Height(0),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    for start in [block::Height(1), block::Height(4)] {
        fixture
            .handle
            .send(narrowed_event(
                requester.clone(),
                HeaderSyncMessage::GetHeaders {
                    start_height: start,
                    count: 3,
                },
            ))
            .await
            .unwrap();
        match next_query_headers_action(&mut fixture.actions).await {
            HeaderSyncAction::QueryHeadersByHeightRange {
                peer,
                start: action_start,
                count,
            } => {
                assert_eq!(peer, requester);
                assert_eq!(action_start, start);
                assert_eq!(count, 3);
            }
            action => panic!("unexpected action: {action:?}"),
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            requester.clone(),
            HeaderSyncMessage::GetHeaders {
                start_height: block::Height(7),
                count: 1,
            },
        ))
        .await
        .unwrap();
    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, requester);
            assert_eq!(reason, HeaderSyncMisbehavior::GetHeadersSpam);
            break;
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeResponseFinished {
            peer: requester.clone(),
            start_height: block::Height(1),
            requested_count: 1,
            returned_count: 0,
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(narrowed_event(
            requester.clone(),
            HeaderSyncMessage::GetHeaders {
                start_height: block::Height(8),
                count: 1,
            },
        ))
        .await
        .unwrap();
    match next_query_headers_action(&mut fixture.actions).await {
        HeaderSyncAction::QueryHeadersByHeightRange { peer, start, count } => {
            assert_eq!(peer, requester);
            assert_eq!(start, block::Height(8));
            assert_eq!(count, 1);
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn inbound_get_headers_over_cap_disconnects_without_state_read() {
    let network = regtest_network();
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    startup.config.max_headers_per_response = 3;
    let mut fixture = spawn_test_reactor(startup);
    let requester = peer(61);

    connect_peer(&fixture, requester.clone()).await;
    advertise_tip(
        &fixture,
        requester.clone(),
        block::Height(0),
        block::Height(0),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    fixture
        .handle
        .send(narrowed_event(
            requester.clone(),
            HeaderSyncMessage::GetHeaders {
                start_height: block::Height(1),
                count: 4,
            },
        ))
        .await
        .unwrap();

    loop {
        match next_action(&mut fixture.actions).await {
            HeaderSyncAction::QueryHeadersByHeightRange { .. } => {
                panic!("over-cap GetHeaders must not query state");
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                assert_eq!(peer, requester);
                assert_eq!(reason, HeaderSyncMisbehavior::GetHeadersTooLong);
                break;
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_non_linking_range_traces_link_stage_and_error_kind() {
    let network = regtest_network();
    let anchor = (block::Height(0), network.genesis_hash());
    let mut capture =
        TraceCapture::for_test("rejected_non_linking_range_traces_link_stage_and_error_kind")
            .unwrap();
    let mut startup = startup_for(network, anchor, Some(anchor));
    startup.trace = ZakuraTrace::new(capture.tracer(), "01");
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(64);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        anchor.0,
        block::Height(1),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    let (served_peer, start_height, count) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(served_peer, peer_id);
    assert_eq!(start_height, block::Height(1));
    assert_eq!(count, 1);

    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(vec![mainnet_header(&BLOCK_MAINNET_2_BYTES)]),
        ))
        .await
        .unwrap();

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
        }
        action => panic!("unexpected action: {action:?}"),
    }

    capture.flush().await;
    let reader = capture.reader().unwrap();
    let header_sync = reader.table(HEADER_SYNC_TABLE.table());
    let anchor_hash = format!("{}", anchor.1);
    header_sync.assert_row(
        hs_trace::HEADER_RANGE_REJECTED,
        &[
            (hs_trace::RANGE_START, TraceValue::U64(1)),
            (hs_trace::RANGE_COUNT, TraceValue::U64(1)),
            (hs_trace::ANCHOR_HASH, TraceValue::Str(&anchor_hash)),
            (hs_trace::VALIDATION_STAGE, TraceValue::Str("link")),
            (
                hs_trace::ERROR_KIND,
                TraceValue::Str("first_header_does_not_link"),
            ),
        ],
    );

    let _ = capture.finish().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn header_sync_jsonl_trace_captures_status_range_dedup_and_disconnect() {
    let network = Network::Mainnet;
    let mut capture = TraceCapture::for_test(
        "header_sync_jsonl_trace_captures_status_range_dedup_and_disconnect",
    )
    .unwrap();
    let first_checkpoint = network
        .checkpoint_list()
        .min_height_in_range(block::Height(1)..)
        .expect("mainnet has a checkpoint above genesis");
    let checkpoint_hash = network
        .checkpoint_list()
        .hash(first_checkpoint)
        .expect("checkpoint height has a hash");
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((first_checkpoint, checkpoint_hash)),
    );
    startup.trace = ZakuraTrace::new(capture.tracer(), "01");
    let fixture = spawn_test_reactor(startup);
    let peer_id = peer(55);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        next_height(first_checkpoint).expect("checkpoint has a successor"),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: block::Height(1),
            hash: mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
            header: mainnet_header(&BLOCK_MAINNET_1_BYTES),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            HeaderSyncMessage::NewBlock(mainnet_block(&BLOCK_MAINNET_1_BYTES)),
        ))
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::PeerMisbehavior {
            peer: peer_id,
            reason: HeaderSyncMisbehavior::MalformedMessage,
        })
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    capture.flush().await;
    let reader = capture.reader().unwrap();
    let header_sync = reader.table(HEADER_SYNC_TABLE.table());

    assert!(header_sync.count(hs_trace::HEADER_STATUS_SENT) >= 1);
    assert!(header_sync.count(hs_trace::HEADER_STATUS_RECEIVED) >= 1);
    assert!(header_sync.count(hs_trace::HEADER_GET_HEADERS_SENT) >= 1);
    assert!(header_sync.count(hs_trace::HEADER_NEW_BLOCK_DEDUPED) >= 1);
    assert!(header_sync.count(hs_trace::HEADER_PEER_DISCONNECT_REQUESTED) >= 1);

    for row in header_sync.rows() {
        assert!(
            row.get("block").is_none() && row.get("headers").is_none(),
            "header-sync trace rows must not contain full payloads: {row:?}"
        );
    }

    let _ = capture.finish().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn header_sync_metrics_record_status_range_new_block_dedup_and_violation() {
    // Reactor-owned metrics. The peer-local inbound-receive counters
    // (`peer.status.received`, `response.received`) moved into the routine with
    // chunk 03 and are asserted by `routine_records_inbound_receive_metrics`
    // instead, since this reactor flow injects the narrowed events and never runs
    // the routine decode. `tip.new_block.received` stays reactor-side.
    let metrics = [
        "sync.header.peer.status.sent",
        "sync.header.request.sent",
        "sync.header.range.committed",
        "sync.header.tip.new_block.received",
        "sync.header.tip.new_block.deduped",
        "sync.header.peer.violation",
    ];
    let before = metric_snapshot(&metrics);

    let first_checkpoint = block::Height(3);
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(first_checkpoint, checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((first_checkpoint, checkpoint_hash)),
    ));
    let peer_id = peer(56);

    connect_peer(&fixture, peer_id.clone()).await;
    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::SendMessage {
            msg: HeaderSyncMessage::Status(_),
            ..
        } => {}
        action => panic!("unexpected action: {action:?}"),
    }

    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(4),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(vec![mainnet_header(&BLOCK_MAINNET_4_BYTES)]),
        ))
        .await
        .unwrap();
    let committed_hash = match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::CommitHeaderRange {
            start_height,
            headers,
            ..
        } => {
            assert_eq!(
                start_height,
                next_height(first_checkpoint).expect("checkpoint has a successor")
            );
            block::Hash::from(headers.last().expect("one header").as_ref())
        }
        action => panic!("unexpected action: {action:?}"),
    };

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeCommitted {
            start_height: next_height(first_checkpoint).expect("checkpoint has a successor"),
            tip_height: next_height(first_checkpoint).expect("checkpoint has a successor"),
            tip_hash: committed_hash,
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: block::Height(1),
            hash: mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
            header: mainnet_header(&BLOCK_MAINNET_1_BYTES),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            HeaderSyncMessage::NewBlock(mainnet_block(&BLOCK_MAINNET_1_BYTES)),
        ))
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::PeerMisbehavior {
            peer: peer_id,
            reason: HeaderSyncMisbehavior::MalformedMessage,
        })
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    for metric in metrics {
        assert_metric_incremented(&before, metric);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unsolicited_headers_are_misbehavior_but_empty_headers_retry() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_with_timeout(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        std::time::Duration::from_millis(20),
    ));
    let peer_id = peer(9);

    fixture
        .handle
        .send(narrowed_event(peer_id.clone(), headers_message(Vec::new())))
        .await
        .unwrap();
    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::UnsolicitedHeaders);
        }
        action => panic!("unexpected action: {action:?}"),
    }

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }
    fixture
        .handle
        .send(narrowed_event(peer_id.clone(), headers_message(Vec::new())))
        .await
        .unwrap();
    assert!(
        matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ),
        "empty Headers for an outstanding range should retry without disconnecting"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn committed_range_updates_best_tip_watch_and_does_not_advance_finality() {
    let network = regtest_network();
    let fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let mut tip = fixture.handle.subscribe_tip();
    let tip_hash = block::Hash([12; 32]);

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeCommitted {
            start_height: block::Height(1),
            tip_height: block::Height(1),
            tip_hash,
        })
        .await
        .unwrap();

    tip.changed().await.unwrap();
    assert_eq!(*tip.borrow(), (block::Height(1), tip_hash));
    assert_ne!(fixture.handle.best_header_tip().0, block::Height(0));
}

#[tokio::test(flavor = "current_thread")]
async fn forward_link_wedge_reanchors_to_verified_tip_without_banning() {
    let network = regtest_network();
    let verified = (block::Height(0), network.genesis_hash());
    let stranded_tip = (block::Height(3), block::Hash([3; 32]));
    let mut startup = HeaderSyncStartup::new(
        network.clone(),
        verified,
        HeaderSyncFrontiers {
            finalized_height: verified.0,
            verified_block_tip: verified.0,
            verified_block_hash: verified.1,
        },
        Some(stranded_tip),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.range_state_actions_enabled = true;
    let mut fixture = spawn_test_reactor(startup);
    let mut tip = fixture.handle.subscribe_tip();
    let peers = [peer(61), peer(62)];

    for peer_id in peers.iter().cloned() {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(
            &fixture,
            peer_id,
            verified.0,
            block::Height(4),
            DEFAULT_HS_RANGE,
            1,
        )
        .await;
    }

    for _ in 0..3 {
        let (served_peer, start_height, count) =
            next_outbound_get_headers(&mut fixture.actions).await;
        assert_eq!(start_height, block::Height(4));
        assert_eq!(count, 1);
        fixture
            .handle
            .send(narrowed_event(
                served_peer,
                headers_message(vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)]),
            ))
            .await
            .unwrap();
    }

    tip.changed().await.unwrap();
    assert_eq!(*tip.borrow(), verified);
    assert_eq!(fixture.handle.best_header_tip(), verified);

    let expected_start = verified.0.next().expect("genesis has a successor");
    let mut saw_reanchor_action = false;
    for _ in 0..8 {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::HeaderReanchored { old, new } => {
                assert_eq!(old, stranded_tip);
                assert_eq!(new, verified);
                saw_reanchor_action = true;
            }
            HeaderSyncAction::SendMessage {
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height,
                        count: _,
                    },
                ..
            } if saw_reanchor_action && start_height == expected_start => {
                assert_no_commit_or_misbehavior(&mut fixture.actions).await;
                return;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("unexpected misbehavior from {peer:?}: {reason:?}");
            }
            _ => {}
        }
    }
    panic!("after re-anchor, header sync did not emit the reanchor action and request forward from the verified tip");
}

#[tokio::test(flavor = "current_thread")]
async fn single_peer_forward_link_failures_do_not_reanchor_globally() {
    let network = regtest_network();
    let verified = (block::Height(0), network.genesis_hash());
    let stranded_tip = (block::Height(3), block::Hash([3; 32]));
    let mut startup = HeaderSyncStartup::new(
        network.clone(),
        verified,
        HeaderSyncFrontiers {
            finalized_height: verified.0,
            verified_block_tip: verified.0,
            verified_block_hash: verified.1,
        },
        Some(stranded_tip),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.range_state_actions_enabled = true;
    let mut fixture = spawn_test_reactor(startup);
    let mut tip = fixture.handle.subscribe_tip();
    let peer_id = peer(63);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id,
        verified.0,
        block::Height(4),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    for _ in 0..3 {
        let (served_peer, start_height, count) =
            next_outbound_get_headers(&mut fixture.actions).await;
        assert_eq!(start_height, block::Height(4));
        assert_eq!(count, 1);
        fixture
            .handle
            .send(narrowed_event(
                served_peer,
                headers_message(vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)]),
            ))
            .await
            .unwrap();
    }

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), tip.changed())
            .await
            .is_err(),
        "one peer alone must not lower the global header frontier"
    );
    assert_eq!(fixture.handle.best_header_tip(), stranded_tip);
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn forward_genesis_backfill_reaches_checkpoint_before_finalized_commit() {
    let headers = [
        mainnet_header(&BLOCK_MAINNET_1_BYTES),
        mainnet_header(&BLOCK_MAINNET_2_BYTES),
        mainnet_header(&BLOCK_MAINNET_3_BYTES),
    ];
    let checkpoint_hash = block::Hash::from(headers[2].as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(43);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(3),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    loop {
        if let HeaderSyncAction::SendMessage {
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(start_height, block::Height(1));
            assert_eq!(count, 3);
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(headers.to_vec()),
        ))
        .await
        .unwrap();

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::CommitHeaderRange {
            peer,
            start_height,
            headers,
            finalized,
            ..
        } => {
            assert_eq!(peer, peer_id);
            assert_eq!(start_height, block::Height(1));
            assert_eq!(headers.len(), 3);
            assert!(finalized);
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn truncated_finalized_backfill_is_rejected_before_commit() {
    let headers = [
        mainnet_header(&BLOCK_MAINNET_1_BYTES),
        mainnet_header(&BLOCK_MAINNET_2_BYTES),
        mainnet_header(&BLOCK_MAINNET_3_BYTES),
    ];
    let checkpoint_hash = block::Hash::from(headers[2].as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(44);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(3),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(headers[..2].to_vec()),
        ))
        .await
        .unwrap();

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
        }
        action => panic!("unexpected action: {action:?}"),
    }
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn backward_checkpoint_backfill_accepts_linking_run_as_finalized() {
    let headers = [
        mainnet_header(&BLOCK_MAINNET_1_BYTES),
        mainnet_header(&BLOCK_MAINNET_2_BYTES),
        mainnet_header(&BLOCK_MAINNET_3_BYTES),
    ];
    let checkpoint_hash = block::Hash::from(headers[2].as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network,
        (block::Height(3), checkpoint_hash),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let peer_id = peer(45);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(3),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(headers.to_vec()),
        ))
        .await
        .unwrap();

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::CommitHeaderRange {
            peer,
            start_height,
            headers,
            finalized,
            ..
        } => {
            assert_eq!(peer, peer_id);
            assert_eq!(start_height, block::Height(1));
            assert_eq!(headers.len(), 3);
            assert!(finalized);
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_backfill_rejects_non_contiguous_run_before_commit() {
    let (network, checkpoint_hash) = checkpoint_regtest(block::Height(3));
    let mut fixture = spawn_test_reactor(startup_for(
        network,
        (block::Height(3), checkpoint_hash),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let peer_id = peer(10);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(3),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(vec![
                mainnet_header(&BLOCK_MAINNET_GENESIS_BYTES),
                mainnet_header(&BLOCK_MAINNET_GENESIS_BYTES),
                mainnet_header(&BLOCK_MAINNET_GENESIS_BYTES),
            ]),
        ))
        .await
        .unwrap();

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn header_response_that_does_not_link_to_anchor_is_misbehavior_before_commit() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let anchor = (block::Height(0), network.genesis_hash());
    let mut fixture = spawn_test_reactor(startup_for(network, anchor, Some(anchor)));
    let peer_id = peer(46);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(4),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(vec![mainnet_header(&BLOCK_MAINNET_2_BYTES)]),
        ))
        .await
        .unwrap();

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
        }
        action => panic!("unexpected action: {action:?}"),
    }
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_backfill_rejects_checkpoint_hash_mismatch_before_commit() {
    let headers = [
        mainnet_header(&BLOCK_MAINNET_1_BYTES),
        mainnet_header(&BLOCK_MAINNET_2_BYTES),
        mainnet_header(&BLOCK_MAINNET_3_BYTES),
    ];
    let divergent_checkpoint_hash = block::Hash::from(headers[0].as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), divergent_checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network,
        (block::Height(3), divergent_checkpoint_hash),
        Some((block::Height(3), divergent_checkpoint_hash)),
    ));
    let peer_id = peer(46);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(3),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            }
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(narrowed_event(
            peer_id.clone(),
            headers_message(headers.to_vec()),
        ))
        .await
        .unwrap();

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
        }
        action => panic!("unexpected action: {action:?}"),
    }
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_validation_accepts_valid_contiguous_headers() {
    let headers = vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)];
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };

    validate_headers_stateless(headers, context).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_validation_rejects_non_contiguous_and_future_headers() {
    let mut second = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    second.previous_block_hash = block::Hash([1; 32]);
    let headers = vec![
        mainnet_header(&BLOCK_MAINNET_GENESIS_BYTES),
        Arc::new(second),
    ];
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(0),
        decode_context: headers_context(2, DEFAULT_HS_RANGE),
    };
    assert!(matches!(
        validate_headers_stateless(headers, context).await,
        Err(HeaderSyncWireError::NonContiguousHeaders)
    ));

    let mut future = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    future.time = Utc::now() + Duration::hours(3);
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };
    assert!(matches!(
        validate_headers_stateless(vec![Arc::new(future)], context).await,
        Err(HeaderSyncWireError::Time(_))
    ));
}

#[test]
fn range_link_validation_rejects_non_linking_headers() {
    let genesis = mainnet_block(&BLOCK_MAINNET_GENESIS_BYTES);
    let block1 = mainnet_header(&BLOCK_MAINNET_1_BYTES);
    let block2 = mainnet_header(&BLOCK_MAINNET_2_BYTES);

    let mut bad_first = *block1;
    bad_first.previous_block_hash = block::Hash([1; 32]);
    assert!(matches!(
        validate_header_range_links(genesis.hash(), &[Arc::new(bad_first)]),
        Err(HeaderSyncWireError::FirstHeaderDoesNotLink)
    ));

    let mut bad_second = *block2;
    bad_second.previous_block_hash = block::Hash([2; 32]);
    assert!(matches!(
        validate_header_range_links(genesis.hash(), &[block1, Arc::new(bad_second)]),
        Err(HeaderSyncWireError::NonContiguousHeaders)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_validation_rejects_bad_pow() {
    let mut bad_solution = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    bad_solution.nonce[0] ^= 1;
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };
    assert!(matches!(
        validate_headers_stateless(vec![Arc::new(bad_solution)], context).await,
        Err(HeaderSyncWireError::Equihash(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_block_stateless_validation_accepts_valid_mainnet_block() {
    validate_new_block_stateless(
        mainnet_block(&BLOCK_MAINNET_1_BYTES),
        &Network::Mainnet,
        Utc::now(),
        block::Height(1),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_block_stateless_validation_rejects_wrong_solution_size_and_bad_pow() {
    let mut wrong_solution_size = (*mainnet_block(&BLOCK_MAINNET_1_BYTES)).clone();
    let mut header = *wrong_solution_size.header;
    header.solution = Solution::Regtest([0; 36]);
    wrong_solution_size.header = Arc::new(header);

    assert!(matches!(
        validate_new_block_stateless(
            Arc::new(wrong_solution_size),
            &Network::Mainnet,
            Utc::now(),
            block::Height(1),
        )
        .await,
        Err(HeaderSyncWireError::WrongEquihashSolutionSize)
    ));

    let mut bad_pow = (*mainnet_block(&BLOCK_MAINNET_1_BYTES)).clone();
    let mut header = *bad_pow.header;
    header.nonce[0] ^= 1;
    bad_pow.header = Arc::new(header);

    assert!(matches!(
        validate_new_block_stateless(
            Arc::new(bad_pow),
            &Network::Mainnet,
            Utc::now(),
            block::Height(1),
        )
        .await,
        Err(HeaderSyncWireError::Equihash(_))
    ));
}

#[test]
fn difficulty_filter_rejects_hash_above_threshold() {
    let threshold =
        CompactDifficulty::from_bytes_in_display_order(&[0x01, 0x01, 0x00, 0x00]).unwrap();

    assert!(matches!(
        validate_difficulty_filter(block::Hash([0xff; 32]), threshold),
        Err(HeaderSyncWireError::DifficultyFilter { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_header_validation_surfaces_difficulty_filter_after_equihash_acceptance() {
    let mut header = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    header.difficulty_threshold =
        CompactDifficulty::from_bytes_in_display_order(&[0x01, 0x01, 0x00, 0x00]).unwrap();
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };

    assert!(matches!(
        validate_headers_stateless_after_equihash_acceptance(vec![Arc::new(header)], context).await,
        Err(HeaderSyncWireError::DifficultyFilter { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_validation_rejects_wrong_solution_size_for_network() {
    let mut regtest_sized = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    regtest_sized.solution = Solution::Regtest([0; 36]);
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };

    assert!(matches!(
        validate_headers_stateless(vec![Arc::new(regtest_sized)], context).await,
        Err(HeaderSyncWireError::WrongEquihashSolutionSize)
    ));
}

#[test]
fn regtest_header_validation_accepts_common_and_short_solution_sizes() {
    let regtest = Network::new_regtest(Default::default());
    let common_sized = mainnet_header(&BLOCK_MAINNET_1_BYTES);
    let mut short_sized = *common_sized;
    short_sized.solution = Solution::Regtest([0; 36]);

    validate_solution_sizes(std::slice::from_ref(&common_sized), &regtest)
        .expect("regtest accepts Zebra-mined common-size solutions");
    validate_solution_sizes(&[Arc::new(short_sized)], &regtest)
        .expect("regtest accepts short regtest solutions");
    assert!(matches!(
        validate_solution_sizes(&[Arc::new(short_sized)], &Network::Mainnet),
        Err(HeaderSyncWireError::WrongEquihashSolutionSize)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regtest_stateless_validation_skips_pow_filter() {
    let regtest = Network::new_regtest(Default::default());
    let mut header = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    header.difficulty_threshold =
        CompactDifficulty::from_bytes_in_display_order(&[0x01, 0x01, 0x00, 0x00]).unwrap();
    let context = HeaderSyncValidationContext {
        network: &regtest,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };

    validate_headers_stateless(vec![Arc::new(header)], context)
        .await
        .expect("regtest header sync leaves PoW enforcement to block verification");
}

#[tokio::test(flavor = "current_thread")]
async fn pow_validation_does_not_monopolize_the_runtime_thread() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let headers = vec![
        mainnet_header(&BLOCK_MAINNET_1_BYTES),
        mainnet_header(&BLOCK_MAINNET_2_BYTES),
        mainnet_header(&BLOCK_MAINNET_3_BYTES),
        mainnet_header(&BLOCK_MAINNET_4_BYTES),
    ];
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(4, DEFAULT_HS_RANGE),
    };

    let ticks = Arc::new(AtomicUsize::new(0));
    let ticker_ticks = ticks.clone();
    let ticker = tokio::spawn(async move {
        loop {
            ticker_ticks.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
        }
    });

    validate_headers_stateless(headers, context).await.unwrap();
    let progressed = ticks.load(Ordering::SeqCst);
    ticker.abort();

    assert!(
        progressed > 0,
        "reactor thread was blocked during PoW validation"
    );
}

#[test]
fn hostile_vectors_are_rejected_for_allocation_and_unsolicited_headers() {
    let mut encoded = vec![MSG_HS_HEADERS];
    encoded.write_u32::<LittleEndian>(u32::MAX).unwrap();
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, headers_context(MAX_HS_RANGE, MAX_HS_RANGE)),
        Err(HeaderSyncWireError::HeaderCountLimit { .. })
    ));

    let mut encoded = vec![MSG_HS_HEADERS];
    encoded.write_u32::<LittleEndian>(1).unwrap();
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()),
        Err(HeaderSyncWireError::UnsolicitedHeaders)
    ));
}

/// Misbehavior is record-only: an `InvalidStatus` (formerly an immediate
/// disconnect) is still *recorded* as a `Misbehavior` action, but the peer's
/// session is **not** cancelled. Peer scoring no longer drives disconnects.
#[tokio::test]
async fn misbehavior_is_recorded_without_disconnecting_the_peer() {
    let network = Network::Mainnet;
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
    // Keep the test deterministic: no scheduling/state actions, so the only
    // actions enqueued are the ones we drive below.
    startup.range_state_actions_enabled = false;
    let mut fixture = spawn_test_reactor(startup);

    // Connect the peer we will flag as misbehaving and keep its session
    // cancellation token so we can confirm it is never cancelled.
    let probe = peer(7);
    let probe_cancel =
        connect_peer_with_direction(&fixture, probe.clone(), ServicePeerDirection::Inbound).await;

    // `anchor_height > tip_height` is an `InvalidStatus` misbehavior the routine
    // classifies; the reactor only aggregates it (record-only). Inject the narrowed
    // misbehavior event the routine would forward.
    fixture
        .handle
        .send(HeaderSyncEvent::PeerMisbehavior {
            peer: probe.clone(),
            reason: HeaderSyncMisbehavior::InvalidStatus,
        })
        .await
        .expect("event queues");

    // The violation is recorded: a `Misbehavior` action for the probe is emitted.
    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, probe);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidStatus);
            break;
        }
    }

    // But the peer's session is never cancelled: misbehavior is record-only.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !probe_cancel.is_cancelled(),
        "misbehavior is record-only: an InvalidStatus peer must NOT be disconnected",
    );
}
