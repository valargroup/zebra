use std::{collections::HashMap, future};

use super::*;
use super::{
    config::{DEFAULT_BS_MAX_INFLIGHT, MAX_BS_RESPONSE_BYTES},
    reorder::*,
    scheduler::*,
    state::*,
};
use crate::zakura::{
    framed_channel, Peer, PeerStreamSession, Service, ServicePeerSnapshot, ServiceRegistry,
    StreamMode,
};
use zebra_chain::serialization::{ZcashDeserializeInto, ZcashSerialize};
use zebra_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES, BLOCK_MAINNET_3_BYTES};

fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds")
}

fn mainnet_block(bytes: &[u8]) -> Arc<block::Block> {
    Arc::new(bytes.zcash_deserialize_into().expect("block vector parses"))
}

fn mainnet_blocks_1_to_3() -> Vec<Arc<block::Block>> {
    vec![
        mainnet_block(&BLOCK_MAINNET_1_BYTES),
        mainnet_block(&BLOCK_MAINNET_2_BYTES),
        mainnet_block(&BLOCK_MAINNET_3_BYTES),
    ]
}

fn block_size(block: &block::Block) -> u32 {
    u32::try_from(
        block
            .zcash_serialize_to_vec()
            .expect("test block serializes")
            .len(),
    )
    .expect("test block size fits u32")
}

fn status() -> BlockSyncStatus {
    BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(42),
        tip_hash: block::Hash([7; 32]),
        max_blocks_per_response: 16,
        max_inflight_requests: 4,
        max_response_bytes: MAX_BS_RESPONSE_BYTES,
    }
}

fn round_trip(message: BlockSyncMessage) {
    let encoded = message.encode().expect("message encodes");
    let decoded = BlockSyncMessage::decode(&encoded).expect("message decodes");

    assert_eq!(decoded, message);
}

async fn next_event(events: &mut mpsc::Receiver<BlockSyncEvent>) -> BlockSyncEvent {
    tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("block-sync event should arrive")
        .expect("block-sync event channel should stay open")
}

async fn next_action(actions: &mut mpsc::Receiver<BlockSyncAction>) -> BlockSyncAction {
    tokio::time::timeout(Duration::from_secs(1), actions.recv())
        .await
        .expect("block-sync action should arrive")
        .expect("block-sync action channel should stay open")
}

async fn wait_for_getblocks(
    actions: &mut mpsc::Receiver<BlockSyncAction>,
) -> (ZakuraPeerId, block::Height, u32) {
    loop {
        match next_action(actions).await {
            BlockSyncAction::SendMessage {
                peer,
                msg:
                    BlockSyncMessage::GetBlocks {
                        start_height,
                        count,
                    },
            } => return (peer, start_height, count),
            BlockSyncAction::SendMessage { .. } => {}
            action => panic!("unexpected action before GetBlocks: {action:?}"),
        }
    }
}

async fn wait_for_connect_status(actions: &mut mpsc::Receiver<BlockSyncAction>) -> ZakuraPeerId {
    loop {
        match next_action(actions).await {
            BlockSyncAction::SendMessage {
                peer,
                msg: BlockSyncMessage::Status(_),
            } => return peer,
            BlockSyncAction::SendMessage { .. } => {}
            action => panic!("unexpected action before connect status: {action:?}"),
        }
    }
}

fn peer_state(byte: u8) -> (ZakuraPeerId, PeerBlockState) {
    let peer = peer(byte);
    let (_inbound_tx, inbound_rx) = framed_channel(4);
    let (outbound_tx, _outbound_rx) = framed_channel(4);
    let session = PeerStreamSession::new(
        peer.clone(),
        ZAKURA_STREAM_BLOCK_SYNC,
        inbound_rx,
        outbound_tx,
        CancellationToken::new(),
    );
    let mut state = PeerBlockState::new(
        BlockSyncPeerSession::new(&session, ServicePeerDirection::Outbound),
        &ZakuraBlockSyncConfig::default(),
    );
    state.received_status = true;
    state.servable_high = block::Height(100);
    (peer, state)
}

fn needed(height: u32, size: BlockSizeEstimate) -> NeededBlock {
    NeededBlock {
        height: block::Height(height),
        hash: block::Hash([height as u8; 32]),
        size,
    }
}

#[test]
fn codec_round_trips_every_message_variant() {
    round_trip(BlockSyncMessage::Status(status()));
    round_trip(BlockSyncMessage::GetBlocks {
        start_height: block::Height(10),
        count: 3,
    });
    round_trip(BlockSyncMessage::Block(mainnet_block(
        &BLOCK_MAINNET_1_BYTES,
    )));
    round_trip(BlockSyncMessage::BlocksDone {
        start_height: block::Height(10),
        returned: 3,
    });
    round_trip(BlockSyncMessage::RangeUnavailable {
        start_height: block::Height(10),
        count: 3,
    });
}

#[test]
fn codec_round_trips_block_near_max_block_bytes() {
    let block = Arc::new(zebra_chain::block::tests::generate::large_multi_transaction_block());
    let serialized_len = block
        .zcash_serialize_to_vec()
        .expect("large test block serializes")
        .len();
    let max_block_bytes =
        usize::try_from(block::MAX_BLOCK_BYTES).expect("max block size fits in usize");

    assert!(
        serialized_len <= max_block_bytes && serialized_len > max_block_bytes - 1000,
        "test block should be close to the consensus cap, got {serialized_len}"
    );
    round_trip(BlockSyncMessage::Block(block));
}

#[test]
fn codec_rejects_malformed_discriminator_and_truncated_payload() {
    assert!(matches!(
        BlockSyncMessage::decode(&[99]),
        Err(BlockSyncWireError::UnknownMessageType(99))
    ));

    assert!(matches!(
        BlockSyncMessage::decode(&[MSG_BS_GET_BLOCKS, 1, 0]),
        Err(BlockSyncWireError::Io(_))
    ));
}

#[test]
fn codec_rejects_oversized_frame_and_oversized_block() {
    let oversized_payload = vec![0; MAX_BS_MESSAGE_BYTES + 1];
    assert!(matches!(
        BlockSyncMessage::decode(&oversized_payload),
        Err(BlockSyncWireError::OversizedPayload { .. })
    ));

    let oversized_block =
        Arc::new(zebra_chain::block::tests::generate::oversized_multi_transaction_block());
    assert!(matches!(
        BlockSyncMessage::Block(oversized_block).encode(),
        Err(BlockSyncWireError::OversizedBlock { .. })
            | Err(BlockSyncWireError::OversizedPayload { .. })
    ));
}

#[test]
fn codec_rejects_count_and_returned_over_cap() {
    let over_cap = MAX_BS_BLOCKS_PER_REQUEST + 1;

    assert!(matches!(
        BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: over_cap,
        }
        .encode(),
        Err(BlockSyncWireError::BlockCountLimit { .. })
    ));

    assert!(matches!(
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(1),
            returned: over_cap,
        }
        .encode(),
        Err(BlockSyncWireError::BlockCountLimit { .. })
    ));

    assert!(matches!(
        BlockSyncMessage::RangeUnavailable {
            start_height: block::Height(1),
            count: over_cap,
        }
        .encode(),
        Err(BlockSyncWireError::BlockCountLimit { .. })
    ));
}

#[test]
fn frame_decode_rejects_mismatched_unknown_flags_and_trailing_payload() {
    let frame = Frame {
        message_type: u16::from(MSG_BS_GET_BLOCKS),
        flags: 1,
        payload: BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 1,
        }
        .encode()
        .expect("message encodes"),
    };
    assert!(matches!(
        BlockSyncMessage::decode_frame(frame),
        Err(BlockSyncWireError::UnsupportedFlags(1))
    ));

    let mut payload = BlockSyncMessage::Status(status())
        .encode()
        .expect("message encodes");
    payload.push(0);
    assert!(matches!(
        BlockSyncMessage::decode(&payload),
        Err(BlockSyncWireError::TrailingBytes)
    ));

    let frame = Frame {
        message_type: u16::from(MSG_BS_BLOCK),
        flags: 0,
        payload: BlockSyncMessage::Status(status())
            .encode()
            .expect("message encodes"),
    };
    assert!(matches!(
        BlockSyncMessage::decode_frame(frame),
        Err(BlockSyncWireError::MismatchedFrameMessageType { .. })
    ));
}

#[test]
fn status_decode_clamps_peer_capacity_advertisements() {
    let mut payload = Vec::new();
    payload.push(MSG_BS_STATUS);
    payload.extend_from_slice(&block::Height(1).0.to_le_bytes());
    payload.extend_from_slice(&block::Height(2).0.to_le_bytes());
    block::Hash([9; 32])
        .zcash_serialize(&mut payload)
        .expect("hash serializes");
    payload.extend_from_slice(&u32::MAX.to_le_bytes());
    payload.extend_from_slice(&u16::MAX.to_le_bytes());
    payload.extend_from_slice(&u32::MAX.to_le_bytes());

    let BlockSyncMessage::Status(status) =
        BlockSyncMessage::decode(&payload).expect("status decodes")
    else {
        panic!("expected status message");
    };

    assert_eq!(status.max_blocks_per_response, MAX_BS_BLOCKS_PER_REQUEST);
    assert_eq!(status.max_inflight_requests, DEFAULT_BS_MAX_INFLIGHT);
    assert_eq!(status.max_response_bytes, MAX_BS_RESPONSE_BYTES);
}

#[test]
fn scheduler_assigns_needed_ranges_with_fanout_slots_and_dedup() {
    let mut scheduler = BlockRangeScheduler::new(2);
    scheduler.refresh_needed(vec![
        needed(1, BlockSizeEstimate::Advertised(10_000)),
        needed(2, BlockSizeEstimate::Advertised(10_000)),
        needed(3, BlockSizeEstimate::Advertised(10_000)),
    ]);
    let mut budget = ByteBudget::new(1_000_000);
    let (peer1, state1) = peer_state(31);
    let (peer2, state2) = peer_state(32);
    let (peer3, state3) = peer_state(33);

    let first = scheduler
        .next_for_peer(&peer1, &state1, &mut budget)
        .expect("first peer gets the range");
    assert_eq!(first.start_height, block::Height(1));
    assert_eq!(first.count, 3);

    let second = scheduler
        .next_for_peer(&peer2, &state2, &mut budget)
        .expect("fanout allows a second peer");
    assert_eq!(second.start_height, block::Height(1));
    assert_eq!(second.count, 3);

    assert!(
        scheduler
            .next_for_peer(&peer1, &state1, &mut budget)
            .is_none(),
        "same peer must not receive overlapping duplicate assignment"
    );
    assert!(
        scheduler
            .next_for_peer(&peer3, &state3, &mut budget)
            .is_none(),
        "fanout cap must deduplicate the covered range"
    );
}

#[test]
fn scheduler_byte_budget_sizing_shrinks_or_defers_requests() {
    let mut scheduler = BlockRangeScheduler::new(1);
    scheduler.set_estimator_for_tests(750, 1);
    scheduler.refresh_needed(vec![
        needed(10, BlockSizeEstimate::Advertised(100)),
        needed(11, BlockSizeEstimate::Advertised(100)),
        needed(12, BlockSizeEstimate::Advertised(100)),
    ]);
    let (peer, mut state) = peer_state(34);
    state.max_response_bytes = 180;
    state.max_blocks_per_response = 10;
    let mut budget = ByteBudget::new(1_000);

    let request = scheduler
        .next_for_peer(&peer, &state, &mut budget)
        .expect("one block fits the peer response-byte cap");
    assert_eq!(request.start_height, block::Height(10));
    assert_eq!(request.count, 1);
    assert_eq!(budget.reserved(), 100);

    scheduler.complete(&request, &mut budget);
    assert_eq!(budget.reserved(), 0);

    let mut small_budget = ByteBudget::new(50);
    assert!(
        scheduler
            .next_for_peer(&peer, &state, &mut small_budget)
            .is_none(),
        "a first block that does not fit is deferred"
    );
    assert_eq!(small_budget.reserved(), 0);
}

#[test]
fn scheduler_releases_budget_on_completion_timeout_and_cancel() {
    let (peer, state) = peer_state(35);
    let mut scheduler = BlockRangeScheduler::new(1);
    scheduler.set_estimator_for_tests(750, 1);
    scheduler.refresh_needed(vec![needed(20, BlockSizeEstimate::Advertised(1_000))]);
    let mut budget = ByteBudget::new(10_000);
    let request = scheduler
        .next_for_peer(&peer, &state, &mut budget)
        .expect("range fits");
    assert_eq!(budget.reserved(), 1_000);
    scheduler.complete(&request, &mut budget);
    assert_eq!(budget.reserved(), 0);

    scheduler.refresh_needed(vec![needed(21, BlockSizeEstimate::Advertised(2_000))]);
    let request = scheduler
        .next_for_peer(&peer, &state, &mut budget)
        .expect("range fits");
    scheduler.timeout(request, &mut budget);
    assert_eq!(budget.reserved(), 0);

    let request = scheduler
        .next_for_peer(&peer, &state, &mut budget)
        .expect("retried range fits");
    assert_eq!(budget.reserved(), 2_000);
    assert_eq!(request.count, 1);
    scheduler.release_cancelled(&mut budget);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn scheduler_uses_ewma_for_unknown_and_confirmed_size_values() {
    let (peer, mut state) = peer_state(36);
    state.max_response_bytes = 100_000;
    let mut scheduler = BlockRangeScheduler::new(1);
    scheduler.set_estimator_for_tests(750, 1_000);
    scheduler.refresh_needed(vec![
        needed(30, BlockSizeEstimate::Unknown),
        needed(31, BlockSizeEstimate::Advertised(500)),
        needed(32, BlockSizeEstimate::Confirmed(50_000)),
    ]);
    let mut budget = ByteBudget::new(100_000);

    let request = scheduler
        .next_for_peer(&peer, &state, &mut budget)
        .expect("range fits");
    assert_eq!(request.count, 3);
    assert_eq!(request.estimated_bytes, 52_000);
}

#[test]
fn reorder_releases_only_contiguous_prefix_and_clears_budget() {
    let mut reorder = ReorderBuffer::new();
    let mut budget = ByteBudget::new(10_000);
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);

    assert_eq!(
        reorder.insert(block::Height(3), block.clone(), 300, &mut budget),
        ReorderInsertResult::Inserted
    );
    assert!(reorder
        .drain_contiguous_prefix(block::Height(0), &mut budget)
        .is_empty());
    assert_eq!(reorder.buffered_bytes(), 300);
    assert_eq!(budget.reserved(), 300);

    assert_eq!(
        reorder.insert(block::Height(1), block.clone(), 100, &mut budget),
        ReorderInsertResult::Inserted
    );
    let released = reorder.drain_contiguous_prefix(block::Height(0), &mut budget);
    assert_eq!(
        released
            .iter()
            .map(|(height, _)| *height)
            .collect::<Vec<_>>(),
        vec![block::Height(1)]
    );
    assert_eq!(reorder.buffered_bytes(), 300);
    assert_eq!(budget.reserved(), 300);

    assert_eq!(
        reorder.insert(block::Height(2), block.clone(), 200, &mut budget),
        ReorderInsertResult::Inserted
    );
    let released = reorder.drain_contiguous_prefix(block::Height(1), &mut budget);
    assert_eq!(
        released
            .iter()
            .map(|(height, _)| *height)
            .collect::<Vec<_>>(),
        vec![block::Height(2), block::Height(3)]
    );
    assert_eq!(budget.reserved(), 0);

    assert_eq!(
        reorder.insert(block::Height(2), block.clone(), 200, &mut budget),
        ReorderInsertResult::Inserted
    );
    assert_eq!(
        reorder.insert(block::Height(3), block, 300, &mut budget),
        ReorderInsertResult::Inserted
    );
    reorder.drop_from(block::Height(3), &mut budget);
    assert_eq!(reorder.buffered_bytes(), 200);
    assert_eq!(budget.reserved(), 200);
    reorder.clear(&mut budget);
    assert_eq!(reorder.buffered_bytes(), 0);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn reorder_fuzzes_arrival_order_as_parent_first() {
    let orders = [
        [1, 2, 3, 4],
        [4, 3, 2, 1],
        [2, 4, 1, 3],
        [3, 1, 4, 2],
        [2, 1, 4, 3],
    ];

    for order in orders {
        let mut reorder = ReorderBuffer::new();
        let mut budget = ByteBudget::new(10_000);
        let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
        let mut tip = block::Height(0);
        let mut released_all = Vec::new();

        for height in order {
            assert_eq!(
                reorder.insert(block::Height(height), block.clone(), 100, &mut budget),
                ReorderInsertResult::Inserted
            );
            for (released, _) in reorder.drain_contiguous_prefix(tip, &mut budget) {
                assert_eq!(released, block::Height(tip.0 + 1));
                tip = released;
                released_all.push(released);
            }
        }

        assert_eq!(
            released_all,
            vec![
                block::Height(1),
                block::Height(2),
                block::Height(3),
                block::Height(4)
            ]
        );
        assert_eq!(budget.reserved(), 0);
    }
}

#[test]
fn block_sync_stream_declares_kind_capability_version_and_frame_cap() {
    let stream = block_sync_streams()
        .first()
        .copied()
        .expect("block sync declares one stream");

    assert_eq!(stream.kind, ZAKURA_STREAM_BLOCK_SYNC);
    assert_eq!(stream.version, ZAKURA_BLOCK_SYNC_STREAM_VERSION);
    assert_eq!(stream.capability, ZAKURA_CAP_BLOCK_SYNC);
    assert_eq!(stream.mode, StreamMode::Ordered);
    assert_eq!(stream.frame_cap, MAX_BS_FRAME_BYTES);
}

#[tokio::test]
async fn service_registry_routes_block_sync_by_exact_capability_and_version() {
    let service = Arc::new(BlockSyncService::new(ZakuraBlockSyncConfig::default()));
    let registry =
        ServiceRegistry::new(vec![service]).expect("block-sync service declares unique kind");
    let peer = peer(1);

    assert_eq!(
        registry.capability_for_stream(ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_BLOCK_SYNC_STREAM_VERSION),
        Some(ZAKURA_CAP_BLOCK_SYNC)
    );
    assert!(registry
        .capability_for_stream(
            ZAKURA_STREAM_BLOCK_SYNC,
            ZAKURA_BLOCK_SYNC_STREAM_VERSION + 1
        )
        .is_none());
    assert_eq!(
        registry
            .ordered_streams_for_negotiated(ZAKURA_CAP_BLOCK_SYNC)
            .iter()
            .map(|stream| stream.kind)
            .collect::<Vec<_>>(),
        vec![ZAKURA_STREAM_BLOCK_SYNC]
    );
    assert!(registry.ordered_streams_for_negotiated(0).is_empty());
    assert!(registry.wants_ordered_stream(
        ZAKURA_STREAM_BLOCK_SYNC,
        ZAKURA_CAP_BLOCK_SYNC,
        &peer,
        ServicePeerDirection::Inbound,
    ));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn inert_reactor_parks_after_header_tip_watch_closes() {
    let _service = BlockSyncService::new(ZakuraBlockSyncConfig::default());

    let elapsed = tokio::time::timeout(Duration::from_secs(1), future::pending::<()>()).await;

    assert!(
        elapsed.is_err(),
        "paused-time timeout only elapses if the inert reactor has no always-ready branch"
    );
}

#[tokio::test]
async fn add_peer_emits_events_and_round_trips_status_over_framed_path() {
    let (service, mut events) = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
    let peer = peer(2);
    let cancel_token = CancellationToken::new();
    let (inbound_tx, inbound_rx) = framed_channel(4);
    let (outbound_tx, mut outbound_rx) = framed_channel(4);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        streams,
        cancel_token,
    ));

    let session = match next_event(&mut events).await {
        BlockSyncEvent::PeerConnected(session) => session,
        event => panic!("expected PeerConnected, got {event:?}"),
    };
    assert_eq!(session.peer_id(), &peer);
    assert_eq!(service.peer_count(), 1);

    service
        .send_action(BlockSyncAction::SendMessage {
            peer: peer.clone(),
            msg: BlockSyncMessage::Status(status()),
        })
        .await
        .expect("action queues to source");
    let frame = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("action status frame should be sent")
        .expect("outbound frame receiver stays open");
    assert_eq!(
        BlockSyncMessage::decode_frame(frame).expect("status frame decodes"),
        BlockSyncMessage::Status(status())
    );

    inbound_tx
        .send(
            BlockSyncMessage::Status(status())
                .encode_frame()
                .expect("status frame encodes"),
        )
        .await
        .expect("inbound status queues");
    assert!(matches!(
        next_event(&mut events).await,
        BlockSyncEvent::WireMessage {
            msg: BlockSyncMessage::Status(_),
            ..
        }
    ));

    service.remove_peer(&peer);
    assert_eq!(service.peer_count(), 0);
    assert!(session.cancel_token().is_cancelled());
}

#[tokio::test]
async fn lifecycle_events_bypass_full_bounded_wire_queue() {
    let mut config = ZakuraBlockSyncConfig::default();
    config.peer_limits.inbound_queue_depth = 1;
    let (events, _event_rx) = mpsc::channel(config.peer_limits.inbound_queue_depth);
    events
        .try_send(BlockSyncEvent::WireMessage {
            peer: peer(90),
            msg: BlockSyncMessage::Status(status()),
        })
        .expect("test fills bounded wire queue");
    let (lifecycle, mut lifecycle_rx) = mpsc::unbounded_channel();
    let (_peers_tx, peers) = watch::channel(ServicePeerSnapshot::new(0, 0, config.peer_limits));
    let handle = BlockSyncHandle {
        events,
        lifecycle,
        peers,
    };
    let service = BlockSyncService::new_with_handle_for_test(config, handle);
    let peer = peer(91);
    let (inbound_tx, inbound_rx) = framed_channel(4);
    let (outbound_tx, _outbound_rx) = framed_channel(4);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    drop(inbound_tx);

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), lifecycle_rx.recv())
            .await
            .expect("lifecycle event arrives")
            .expect("lifecycle channel stays open"),
        BlockSyncEvent::PeerConnected(session) if session.peer_id() == &peer
    ));

    service.remove_peer(&peer);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), lifecycle_rx.recv())
            .await
            .expect("lifecycle event arrives")
            .expect("lifecycle channel stays open"),
        BlockSyncEvent::PeerDisconnected(disconnected) if disconnected == peer
    ));
}

#[tokio::test]
async fn add_peer_decode_failure_emits_wire_decode_failed() {
    let (service, mut events) = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
    let peer = peer(3);
    let (inbound_tx, inbound_rx) = framed_channel(4);
    let (outbound_tx, _outbound_rx) = framed_channel(4);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    let cancel_token = CancellationToken::new();

    service.add_peer(Peer::new(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        streams,
        cancel_token.clone(),
    ));
    let _ = next_event(&mut events).await;

    inbound_tx
        .send(Frame {
            message_type: u16::from(MSG_BS_STATUS),
            flags: 0,
            payload: Vec::new(),
        })
        .await
        .expect("malformed inbound frame queues");

    assert!(matches!(
        next_event(&mut events).await,
        BlockSyncEvent::WireDecodeFailed { .. }
    ));
    assert!(cancel_token.is_cancelled());
}

#[tokio::test]
async fn registry_add_peer_requires_negotiated_block_sync_capability() {
    let (service, mut events) = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
    let registry = ServiceRegistry::new(vec![Arc::new(service)])
        .expect("block-sync service declares unique kind");
    let peer = peer(4);
    let (inbound_tx, inbound_rx) = framed_channel(4);
    let (outbound_tx, _outbound_rx) = framed_channel(4);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    registry.add_peer(Peer::new(peer, None, 0, streams, CancellationToken::new()));
    drop(inbound_tx);

    assert!(
        tokio::time::timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err(),
        "without cap 1<<3 the registry must not deliver kind-6 streams"
    );
}

#[tokio::test]
async fn wants_peer_rejects_when_configured_slot_cap_is_reached() {
    let config = ZakuraBlockSyncConfig {
        peer_limits: ServicePeerLimits {
            max_inbound_peers: 0,
            max_outbound_peers: 2,
            ..ServicePeerLimits::default()
        },
        ..ZakuraBlockSyncConfig::default()
    };
    let (service, mut events) = BlockSyncService::new_for_test(config);
    let inbound_peer = peer(5);

    assert!(!service.wants_peer(
        &inbound_peer,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Inbound
    ));
    assert!(service.wants_peer(
        &inbound_peer,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound
    ));

    for byte in 6..=7 {
        let peer_id = peer(byte);
        let (_inbound_tx, inbound_rx) = framed_channel(4);
        let (outbound_tx, _outbound_rx) = framed_channel(4);
        let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

        service.add_peer(Peer::new_with_direction(
            peer_id.clone(),
            None,
            ZAKURA_CAP_BLOCK_SYNC,
            ServicePeerDirection::Outbound,
            streams,
            CancellationToken::new(),
        ));

        assert!(matches!(
            next_event(&mut events).await,
            BlockSyncEvent::PeerConnected(session) if session.peer_id() == &peer_id
        ));
    }

    assert_eq!(service.peer_count(), 2);
    assert!(!service.wants_peer(
        &peer(8),
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound
    ));

    let (_inbound_tx, inbound_rx) = framed_channel(4);
    let (outbound_tx, _outbound_rx) = framed_channel(4);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer(8),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    assert_eq!(service.peer_count(), 2);
}

#[tokio::test]
async fn reactor_drives_tip_to_getblocks_to_submit_over_framed_path() {
    let config = ZakuraBlockSyncConfig::default();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer = peer(40);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: block::Hash([1; 32]),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let block_hash = block.hash();
    let block_size = u32::try_from(
        block
            .zcash_serialize_to_vec()
            .expect("block serializes")
            .len(),
    )
    .expect("test block size fits u32");
    tip_tx
        .send((block::Height(1), block_hash))
        .expect("tip watch is live");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip,
                best_header_tip,
            } => {
                assert_eq!(verified_block_tip, block::Height(0));
                assert_eq!(best_header_tip, block::Height(1));
                break;
            }
            BlockSyncAction::SendMessage { .. } => {}
            action => panic!("unexpected action before query: {action:?}"),
        }
    }

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: block_hash,
            size: BlockSizeEstimate::Advertised(block_size),
        }]))
        .await
        .expect("needed metadata queues");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SendMessage {
                peer: action_peer,
                msg:
                    BlockSyncMessage::GetBlocks {
                        start_height,
                        count,
                    },
            } => {
                assert_eq!(action_peer, peer);
                assert_eq!(start_height, block::Height(1));
                assert_eq!(count, 1);
                break;
            }
            BlockSyncAction::SendMessage { .. } => {}
            action => panic!("unexpected action before getblocks: {action:?}"),
        }
    }

    loop {
        let frame = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .expect("outbound frame arrives")
            .expect("outbound channel is live");
        if let BlockSyncMessage::GetBlocks {
            start_height,
            count,
        } = BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes")
        {
            assert_eq!(start_height, block::Height(1));
            assert_eq!(count, 1);
            break;
        }
    }

    inbound_tx
        .send(
            BlockSyncMessage::Block(block.clone())
                .encode_frame()
                .expect("block frame encodes"),
        )
        .await
        .expect("block frame queues");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block: submitted } => {
                assert_eq!(submitted.hash(), block_hash);
                break;
            }
            BlockSyncAction::SendMessage { .. } => {}
            action => panic!("unexpected action before submit: {action:?}"),
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_accepts_multi_block_range_and_submits_parent_first() {
    let config = ZakuraBlockSyncConfig::default();
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer = peer(43);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(3),
                tip_hash: blocks[2].hash(),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");
    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks
                .iter()
                .map(|block| BlockSyncBlockMeta {
                    height: block.coinbase_height().expect("test block has height"),
                    hash: block.hash(),
                    size: BlockSizeEstimate::Advertised(block_size(block)),
                })
                .collect(),
        ))
        .await
        .expect("needed metadata queues");

    let (action_peer, start_height, count) = wait_for_getblocks(&mut actions).await;
    assert_eq!(action_peer, peer);
    assert_eq!(start_height, block::Height(1));
    assert_eq!(count, 3);

    while !matches!(
        BlockSyncMessage::decode_frame(
            tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
                .await
                .expect("outbound frame arrives")
                .expect("outbound channel is live")
        )
        .expect("frame decodes"),
        BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 3,
        }
    ) {}

    for index in [1usize, 2, 0] {
        inbound_tx
            .send(
                BlockSyncMessage::Block(blocks[index].clone())
                    .encode_frame()
                    .expect("block encodes"),
            )
            .await
            .expect("block queues");
    }

    let mut submitted = Vec::new();
    while submitted.len() < 3 {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block } => submitted.push(
                block
                    .coinbase_height()
                    .expect("submitted test block has height"),
            ),
            BlockSyncAction::SendMessage { .. } => {}
            BlockSyncAction::Misbehavior { reason, .. } => {
                panic!("honest multi-block response was misclassified: {reason:?}")
            }
            action => panic!("unexpected action before all submits: {action:?}"),
        }
    }
    assert_eq!(
        submitted,
        vec![block::Height(1), block::Height(2), block::Height(3)]
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_treats_duplicate_buffered_blocks_as_benign() {
    let config = ZakuraBlockSyncConfig::default();
    let blocks = [
        mainnet_block(&BLOCK_MAINNET_1_BYTES),
        mainnet_block(&BLOCK_MAINNET_2_BYTES),
    ];
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer_id = peer(44);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, _outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    assert_eq!(wait_for_connect_status(&mut actions).await, peer_id);
    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(2),
                tip_hash: blocks[1].hash(),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    tip_tx
        .send((block::Height(2), blocks[1].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks
                .iter()
                .map(|block| BlockSyncBlockMeta {
                    height: block.coinbase_height().expect("test block has height"),
                    hash: block.hash(),
                    size: BlockSizeEstimate::Advertised(block_size(block)),
                })
                .collect(),
        ))
        .await
        .expect("needed metadata queues");

    let (action_peer, start_height, count) = wait_for_getblocks(&mut actions).await;
    assert_eq!(action_peer, peer_id);
    assert_eq!(start_height, block::Height(1));
    assert_eq!(count, 2);

    for block in [&blocks[1], &blocks[1], &blocks[0]] {
        inbound_tx
            .send(
                BlockSyncMessage::Block(block.clone())
                    .encode_frame()
                    .expect("block encodes"),
            )
            .await
            .expect("block queues");
    }

    let mut submitted = Vec::new();
    while submitted.len() < 2 {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block } => submitted.push(
                block
                    .coinbase_height()
                    .expect("submitted test block has height"),
            ),
            BlockSyncAction::SendMessage { .. } => {}
            BlockSyncAction::Misbehavior { reason, .. } => {
                panic!("duplicate buffered body was misclassified: {reason:?}")
            }
            action => panic!("unexpected action before submit: {action:?}"),
        }
    }
    assert_eq!(submitted, vec![block::Height(1), block::Height(2)]);

    let quiet = tokio::time::timeout(Duration::from_millis(100), async {
        while let Some(action) = actions.recv().await {
            if let BlockSyncAction::Misbehavior { reason, .. } = action {
                panic!("duplicate buffered body was misclassified after submit: {reason:?}");
            }
        }
    })
    .await;
    assert!(quiet.is_err());

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_accepts_rapid_status_growth_without_spam_score() {
    let config = ZakuraBlockSyncConfig::default();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    drop(tip_tx);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle);
    let peer_id = peer(46);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, _outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    assert_eq!(wait_for_connect_status(&mut actions).await, peer_id);

    for servable_high in [block::Height(1), block::Height(2)] {
        let hash_byte = u8::try_from(servable_high.0).expect("test height fits in u8");
        inbound_tx
            .send(
                BlockSyncMessage::Status(BlockSyncStatus {
                    servable_low: block::Height(1),
                    servable_high,
                    tip_hash: block::Hash([hash_byte; 32]),
                    max_blocks_per_response: 4,
                    max_inflight_requests: 1,
                    max_response_bytes: MAX_BS_RESPONSE_BYTES,
                })
                .encode_frame()
                .expect("status encodes"),
            )
            .await
            .expect("status frame queues");
    }

    let quiet = tokio::time::timeout(Duration::from_millis(100), async {
        while let Some(action) = actions.recv().await {
            if let BlockSyncAction::Misbehavior { reason, .. } = action {
                panic!("rapid status growth was misclassified: {reason:?}");
            }
        }
    })
    .await;
    assert!(quiet.is_err());

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_rejects_block_hash_mismatch_without_hard_drop_for_size_mismatch() {
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        ZakuraBlockSyncConfig::default(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(
        ZakuraBlockSyncConfig::default(),
        handle.clone(),
    );
    let peer = peer(41);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: block::Hash([1; 32]),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status queues");
    tip_tx
        .send((block::Height(1), block::Hash([9; 32])))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: block::Hash([9; 32]),
            size: BlockSizeEstimate::Advertised(1),
        }]))
        .await
        .expect("needed metadata queues");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::SendMessage {
            msg: BlockSyncMessage::GetBlocks { .. },
            ..
        }
    ) {}
    while !matches!(
        BlockSyncMessage::decode_frame(
            tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
                .await
                .expect("outbound frame arrives")
                .expect("outbound channel is live")
        )
        .expect("frame decodes"),
        BlockSyncMessage::GetBlocks { .. }
    ) {}

    inbound_tx
        .send(
            BlockSyncMessage::Block(mainnet_block(&BLOCK_MAINNET_1_BYTES))
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("block queues");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::Misbehavior { reason, .. } => {
                assert_eq!(reason, BlockSyncMisbehavior::InvalidBlock);
                break;
            }
            BlockSyncAction::SendMessage { .. } => {}
            action => panic!("unexpected action before invalid-block report: {action:?}"),
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_reports_size_mismatch_softly_and_still_submits_valid_block() {
    let mut config = ZakuraBlockSyncConfig {
        size_deviation_tolerance: 100,
        ..ZakuraBlockSyncConfig::default()
    };
    config.peer_limits.outbound_queue_depth = 8;
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer = peer(42);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer,
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: block::Hash([1; 32]),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status queues");
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    tip_tx
        .send((block::Height(1), block.hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: block.hash(),
            size: BlockSizeEstimate::Advertised(1),
        }]))
        .await
        .expect("needed metadata queues");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::SendMessage {
            msg: BlockSyncMessage::GetBlocks { .. },
            ..
        }
    ) {}
    while !matches!(
        BlockSyncMessage::decode_frame(
            tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
                .await
                .expect("outbound frame arrives")
                .expect("outbound channel is live")
        )
        .expect("frame decodes"),
        BlockSyncMessage::GetBlocks { .. }
    ) {}

    inbound_tx
        .send(
            BlockSyncMessage::Block(block.clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("block queues");

    let mut saw_size_mismatch = false;
    let mut saw_submit = false;
    while !(saw_size_mismatch && saw_submit) {
        match next_action(&mut actions).await {
            BlockSyncAction::Misbehavior { reason, .. } => {
                assert_eq!(reason, BlockSyncMisbehavior::SizeMismatch);
                saw_size_mismatch = true;
            }
            BlockSyncAction::SubmitBlock { block: submitted } => {
                assert_eq!(submitted.hash(), block.hash());
                saw_submit = true;
            }
            BlockSyncAction::SendMessage { .. } => {}
            action => panic!("unexpected action during size mismatch test: {action:?}"),
        }
    }

    reactor_task.abort();
}
