//! Fixed test vectors for the peer set.

use std::{cmp::max, collections::HashSet, iter, net::SocketAddr, sync::Arc, time::Duration};

use tokio::time::timeout;
use tower::{Service, ServiceExt};

use zebra_chain::{
    block,
    parameters::{Network, NetworkUpgrade},
};

use crate::{
    constants::DEFAULT_MAX_CONNS_PER_IP,
    peer::{ClientRequest, MinimumPeerVersion},
    peer_set::inventory_registry::InventoryStatus,
    protocol::external::{types::Version, InventoryHash},
    PeerSocketAddr, Request, Response, SharedPeerError,
};
use indexmap::IndexMap;
use tokio::sync::watch;

use super::{super::StallOutcome, PeerSetBuilder, PeerVersions};

#[test]
fn peer_set_ready_single_connection() {
    // We are going to use just one peer version in this test
    let peer_versions = PeerVersions {
        peer_versions: vec![Version::min_specified_for_upgrade(
            &Network::Mainnet,
            NetworkUpgrade::Nu6,
        )],
    };

    // Start the runtime
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    // Get peers and client handles of them
    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // We will just use the first peer handle
    let mut client_handle = handles
        .into_iter()
        .next()
        .expect("we always have at least one client");

    // Client did not received anything yet
    assert!(client_handle
        .try_to_receive_outbound_client_request()
        .is_empty());

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        // Get a ready future
        let peer_ready_future = peer_set.ready();

        // If the readiness future gains a `Drop` impl, we want it to be called here.
        #[allow(unknown_lints)]
        #[allow(clippy::drop_non_drop)]
        std::mem::drop(peer_ready_future);

        // Peer set will remain ready for requests
        let peer_ready1 = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Make sure the client did not received anything yet
        assert!(client_handle
            .try_to_receive_outbound_client_request()
            .is_empty());

        // Make a call to the peer set that returns a future
        let fut = peer_ready1.call(Request::Peers);

        // Client received the request
        assert!(matches!(
            client_handle
                .try_to_receive_outbound_client_request()
                .request(),
            Some(ClientRequest {
                request: Request::Peers,
                ..
            })
        ));

        // Drop the future
        std::mem::drop(fut);

        // Peer set will remain ready for requests
        let peer_ready2 = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Get a new future calling a different request than before
        let _fut = peer_ready2.call(Request::MempoolTransactionIds);

        // Client received the request
        assert!(matches!(
            client_handle
                .try_to_receive_outbound_client_request()
                .request(),
            Some(ClientRequest {
                request: Request::MempoolTransactionIds,
                ..
            })
        ));
    });
}

#[test]
fn peer_set_ready_multiple_connections() {
    // Use three peers with the same version
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version, peer_version],
    };

    // Start the runtime
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get peers and client handles of them
    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), 3);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(3, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(peer_ready.ready_services.len(), 3);

        // Stop some peer connections but not all
        handles[0].stop_connection_task().await;
        handles[1].stop_connection_task().await;

        // We can still make the peer set ready
        peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Stop the connection of the last peer
        handles[2].stop_connection_task().await;

        // Peer set hangs when no more connections are present
        let peer_ready = peer_set.ready();
        assert!(timeout(Duration::from_secs(10), peer_ready).await.is_err());
    });
}

#[test]
fn peer_set_find_blocks_with_sources_preserves_response_peers() {
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version, peer_version],
    };

    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    assert_eq!(handles.len(), 3);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(3, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        assert_eq!(peer_ready.ready_services.len(), 3);

        let known_blocks = vec![block::Hash([0; 32])];
        let response_fut = peer_ready.call(Request::FindBlocksWithSources {
            known_blocks: known_blocks.clone(),
            stop: None,
            max_peers: 3,
        });

        let mut expected_responses = HashSet::new();

        for (peer_index, handle) in handles.iter_mut().enumerate() {
            let ClientRequest { request, tx, .. } = handle
                .try_to_receive_outbound_client_request()
                .request()
                .expect("fanout should send a request to every selected peer");

            assert_eq!(
                request,
                Request::FindBlocks {
                    known_blocks: known_blocks.clone(),
                    stop: None,
                }
            );

            let peer_addr: PeerSocketAddr = SocketAddr::new(
                [127, 0, 0, 1].into(),
                u16::try_from(peer_index + 1).expect("test peer index fits in u16"),
            )
            .into();
            let peer_hash = block::Hash([u8::try_from(peer_index + 1).unwrap(); 32]);

            tx.send(Ok(Response::BlockHashes(vec![peer_hash])))
                .expect("peer set should still be awaiting the fanout response");

            expected_responses.insert((peer_addr, vec![peer_hash]));
        }

        let response = response_fut.await.expect("fanout request should succeed");
        let Response::BlockHashesBySource(responses) = response else {
            panic!("unexpected fanout response: {response:?}");
        };

        assert_eq!(responses.len(), expected_responses.len());

        for response in responses {
            assert!(
                expected_responses.remove(&response),
                "unexpected peer source response: {response:?}"
            );
        }

        assert!(
            expected_responses.is_empty(),
            "missing expected peer source responses: {expected_responses:?}"
        );
    });
}

#[test]
fn peer_set_find_blocks_with_sources_records_slow_peers_as_stalls() {
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    // CORRECTNESS: This test does not depend on external resources that could
    // really timeout, like real network connections.
    tokio::time::pause();

    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    assert_eq!(handles.len(), 2);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        assert_eq!(peer_ready.ready_services.len(), 2);

        let known_blocks = vec![block::Hash([0; 32])];
        let response_fut = peer_ready.call(Request::FindBlocksWithSources {
            known_blocks: known_blocks.clone(),
            stop: None,
            max_peers: 2,
        });

        let mut pending_txs = Vec::new();
        let mut responding_peer = None;
        let mut slow_peer = None;
        let responding_hash = block::Hash([0x11; 32]);

        for (peer_index, handle) in handles.iter_mut().enumerate() {
            let ClientRequest { request, tx, .. } = handle
                .try_to_receive_outbound_client_request()
                .request()
                .expect("fanout should send a request to every selected peer");

            assert_eq!(
                request,
                Request::FindBlocks {
                    known_blocks: known_blocks.clone(),
                    stop: None,
                }
            );

            let peer_addr: PeerSocketAddr = SocketAddr::new(
                [127, 0, 0, 1].into(),
                u16::try_from(peer_index + 1).expect("test peer index fits in u16"),
            )
            .into();

            if peer_index == 0 {
                tx.send(Ok(Response::BlockHashes(vec![responding_hash])))
                    .expect("peer set should still be awaiting the fanout response");
                responding_peer = Some(peer_addr);
            } else {
                pending_txs.push(tx);
                slow_peer = Some(peer_addr);
            }
        }

        let response = timeout(Duration::from_secs(10), response_fut)
            .await
            .expect("fanout should return partial responses after the response window")
            .expect("fanout request should succeed");

        let Response::BlockHashesBySource(responses) = response else {
            panic!("unexpected fanout response: {response:?}");
        };

        assert_eq!(
            responses,
            vec![(responding_peer.expect("set above"), vec![responding_hash])]
        );

        let slow_peer = slow_peer.expect("set above");
        let responding_peer = responding_peer.expect("set above");
        let mut saw_clear = false;
        let mut saw_stall = false;

        while let Ok((peer, outcome)) = peer_ready.stall_event_rx.try_recv() {
            if peer == responding_peer {
                assert_eq!(outcome, StallOutcome::Clear);
                saw_clear = true;
            } else if peer == slow_peer {
                assert_eq!(outcome, StallOutcome::Stall);
                saw_stall = true;
            }
        }

        assert!(saw_clear, "responding peer should clear prior stalls");
        assert!(saw_stall, "slow peer should record a stall");

        drop(pending_txs);
    });
}

#[test]
fn peer_set_rejects_connections_past_per_ip_limit() {
    const NUM_PEER_VERSIONS: usize = crate::constants::DEFAULT_MAX_CONNS_PER_IP + 1;

    // Use three peers with the same version
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: [peer_version; NUM_PEER_VERSIONS].into_iter().collect(),
    };

    // Start the runtime
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get peers and client handles of them
    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), NUM_PEER_VERSIONS);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(
            peer_ready.ready_services.len(),
            crate::constants::DEFAULT_MAX_CONNS_PER_IP
        );
    });
}

/// Check that a peer set with an empty inventory registry routes requests to a random ready peer.
#[test]
fn peer_set_route_inv_empty_registry() {
    let test_hash = block::Hash([0; 32]);

    // Use two peers with the same version
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    // Start the runtime
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get peers and client handles of them
    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), 2);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(peer_ready.ready_services.len(), 2);

        // Send an inventory-based request
        let sent_request = Request::BlocksByHash(iter::once(test_hash).collect());
        let _fut = peer_ready.call(sent_request.clone());

        // Check that one of the clients received the request
        let mut received_count = 0;
        for mut handle in handles {
            if let Some(ClientRequest { request, .. }) =
                handle.try_to_receive_outbound_client_request().request()
            {
                assert_eq!(sent_request, request);
                received_count += 1;
            }
        }

        assert_eq!(received_count, 1);
    });
}

#[test]
fn broadcast_all_queued_removes_banned_peers() {
    let peer_versions = PeerVersions {
        peer_versions: vec![Version::min_specified_for_upgrade(
            &Network::Mainnet,
            NetworkUpgrade::Nu6,
        )],
    };

    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    let (discovered_peers, _handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        let banned_ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        let mut bans_map: IndexMap<std::net::IpAddr, std::time::Instant> = IndexMap::new();
        bans_map.insert(banned_ip, std::time::Instant::now());

        let (bans_tx, bans_rx) = watch::channel(Arc::new(bans_map));
        let _ = bans_tx;
        peer_set.bans_receiver = bans_rx;

        let banned_addr: PeerSocketAddr = SocketAddr::new(banned_ip, 1).into();
        let mut remaining_peers = HashSet::new();
        remaining_peers.insert(banned_addr);

        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        peer_set.queued_broadcast_all = Some((Request::Peers, sender, remaining_peers));

        peer_set.broadcast_all_queued();

        if let Some((_req, _sender, remaining_peers)) = peer_set.queued_broadcast_all.take() {
            assert!(remaining_peers.is_empty());
        } else {
            assert!(receiver.try_recv().is_ok());
        }
    });
}

#[test]
fn remove_unready_peer_clears_cancel_handle_and_updates_counts() {
    let peer_versions = PeerVersions {
        peer_versions: vec![Version::min_specified_for_upgrade(
            &Network::Mainnet,
            NetworkUpgrade::Nu6,
        )],
    };

    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    let (discovered_peers, _handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        // Prepare a banned IP map (not strictly required for remove(), but keeps
        // the test's setup similar to real-world conditions).
        let banned_ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        let mut bans_map: IndexMap<std::net::IpAddr, std::time::Instant> = IndexMap::new();
        bans_map.insert(banned_ip, std::time::Instant::now());
        let (_bans_tx, bans_rx) = watch::channel(Arc::new(bans_map));
        peer_set.bans_receiver = bans_rx;

        // Create a cancel handle as if a request was in-flight to `banned_addr`.
        let banned_addr: PeerSocketAddr = SocketAddr::new(banned_ip, 1).into();
        let (tx, _rx) =
            crate::peer_set::set::oneshot::channel::<crate::peer_set::set::CancelClientWork>();
        peer_set.cancel_handles.insert(banned_addr, tx);

        // The peer is counted as 1 peer with that IP.
        assert_eq!(peer_set.num_peers_with_ip(banned_ip), 1);

        // Remove the peer (simulates a discovery::Remove or equivalent).
        peer_set.remove(&banned_addr);

        // After removal, the cancel handle should be gone and the count zero.
        assert!(!peer_set.cancel_handles.contains_key(&banned_addr));
        assert_eq!(peer_set.num_peers_with_ip(banned_ip), 0);
    });
}

/// Check that a peer set routes inventory requests to a peer that has advertised that inventory.
#[test]
fn peer_set_route_inv_advertised_registry() {
    peer_set_route_inv_advertised_registry_order(true);
    peer_set_route_inv_advertised_registry_order(false);
}

fn peer_set_route_inv_advertised_registry_order(advertised_first: bool) {
    let test_hash = block::Hash([0; 32]);
    let test_inv = InventoryHash::Block(test_hash);

    // Hard-code the fixed test address created by mock_peer_discovery
    // TODO: add peer test addresses to ClientTestHarness
    let test_peer = if advertised_first {
        "127.0.0.1:1"
    } else {
        "127.0.0.1:2"
    }
    .parse()
    .expect("unexpected invalid peer address");

    let test_change = InventoryStatus::new_available(test_inv, test_peer);

    // Use two peers with the same version
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    // Start the runtime
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get peers and client handles of them
    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), 2);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, mut peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        // Advertise some inventory
        peer_set_guard
            .inventory_sender()
            .as_mut()
            .expect("unexpected missing inv sender")
            .send(test_change)
            .expect("unexpected dropped receiver");

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(peer_ready.ready_services.len(), 2);

        // Send an inventory-based request
        let sent_request = Request::BlocksByHash(iter::once(test_hash).collect());
        let _fut = peer_ready.call(sent_request.clone());

        // Check that the client that advertised the inventory received the request
        let advertised_handle = if advertised_first {
            &mut handles[0]
        } else {
            &mut handles[1]
        };

        if let Some(ClientRequest { request, .. }) = advertised_handle
            .try_to_receive_outbound_client_request()
            .request()
        {
            assert_eq!(sent_request, request);
        } else {
            panic!("inv request not routed to advertised peer");
        }

        let other_handle = if advertised_first {
            &mut handles[1]
        } else {
            &mut handles[0]
        };

        assert!(
            other_handle
                .try_to_receive_outbound_client_request()
                .request()
                .is_none(),
            "request routed to non-advertised peer",
        );
    });
}

/// Check that a peer set routes inventory requests to peers that are not missing that inventory.
#[test]
fn peer_set_route_inv_missing_registry() {
    peer_set_route_inv_missing_registry_order(true);
    peer_set_route_inv_missing_registry_order(false);
}

fn peer_set_route_inv_missing_registry_order(missing_first: bool) {
    let test_hash = block::Hash([0; 32]);
    let test_inv = InventoryHash::Block(test_hash);

    // Hard-code the fixed test address created by mock_peer_discovery
    // TODO: add peer test addresses to ClientTestHarness
    let test_peer = if missing_first {
        "127.0.0.1:1"
    } else {
        "127.0.0.1:2"
    }
    .parse()
    .expect("unexpected invalid peer address");

    let test_change = InventoryStatus::new_missing(test_inv, test_peer);

    // Use two peers with the same version
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    // Start the runtime
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get peers and client handles of them
    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), 2);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, mut peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        // Mark some inventory as missing
        peer_set_guard
            .inventory_sender()
            .as_mut()
            .expect("unexpected missing inv sender")
            .send(test_change)
            .expect("unexpected dropped receiver");

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(peer_ready.ready_services.len(), 2);

        // Send an inventory-based request
        let sent_request = Request::BlocksByHash(iter::once(test_hash).collect());
        let _fut = peer_ready.call(sent_request.clone());

        // Check that the client missing the inventory did not receive the request
        let missing_handle = if missing_first {
            &mut handles[0]
        } else {
            &mut handles[1]
        };

        assert!(
            missing_handle
                .try_to_receive_outbound_client_request()
                .request()
                .is_none(),
            "request routed to missing peer",
        );

        // Check that the client that was not missing the inventory received the request
        let other_handle = if missing_first {
            &mut handles[1]
        } else {
            &mut handles[0]
        };

        if let Some(ClientRequest { request, .. }) = other_handle
            .try_to_receive_outbound_client_request()
            .request()
        {
            assert_eq!(sent_request, request);
        } else {
            panic!(
                "inv request should have been routed to the only peer not missing the inventory"
            );
        }
    });
}

/// Check that a peer set fails inventory requests if all peers are missing that inventory.
#[test]
fn peer_set_route_inv_all_missing_fail() {
    let test_hash = block::Hash([0; 32]);
    let test_inv = InventoryHash::Block(test_hash);

    // Hard-code the fixed test address created by mock_peer_discovery
    // TODO: add peer test addresses to ClientTestHarness
    let test_peer = "127.0.0.1:1"
        .parse()
        .expect("unexpected invalid peer address");

    let test_change = InventoryStatus::new_missing(test_inv, test_peer);

    // Use one peer
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version],
    };

    // Start the runtime
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get the peer and its client handle
    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), 1);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, mut peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        // Mark the inventory as missing for all peers
        peer_set_guard
            .inventory_sender()
            .as_mut()
            .expect("unexpected missing inv sender")
            .send(test_change)
            .expect("unexpected dropped receiver");

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(peer_ready.ready_services.len(), 1);

        // Send an inventory-based request
        let sent_request = Request::BlocksByHash(iter::once(test_hash).collect());
        let response_fut = peer_ready.call(sent_request.clone());

        // Check that the client missing the inventory did not receive the request
        let missing_handle = &mut handles[0];

        assert!(
            missing_handle
                    .try_to_receive_outbound_client_request()
                    .request().is_none(),
            "request routed to missing peer",
        );

        // Check that the response is a synthetic error
        let response = response_fut.await;
        assert_eq!(
            response
                .expect_err("peer set should return an error (not a Response)")
                .downcast_ref::<SharedPeerError>()
                .expect("peer set should return a boxed SharedPeerError")
                .inner_debug(),
            "NotFoundRegistry([Block(block::Hash(\"0000000000000000000000000000000000000000000000000000000000000000\"))])"
        );
    });
}

/// Check that source-routed single inventory requests do not fall back to random
/// peers when none of the source peers are connected.
#[test]
fn peer_set_route_source_single_unavailable_fail_without_fallback() {
    peer_set_route_source_unavailable_fail_without_fallback(false);
}

/// Check that source-routed batch inventory requests do not fall back to random
/// peers when none of the source peers are connected.
#[test]
fn peer_set_route_source_batch_unavailable_fail_without_fallback() {
    peer_set_route_source_unavailable_fail_without_fallback(true);
}

fn peer_set_route_source_unavailable_fail_without_fallback(batch: bool) {
    let block1_hash = block::Hash([1; 32]);
    let block2_hash = block::Hash([2; 32]);
    let disconnected_source = "127.0.0.1:99"
        .parse()
        .expect("unexpected invalid peer address");

    let hashes = if batch {
        [block1_hash, block2_hash].into_iter().collect()
    } else {
        iter::once(block1_hash).collect()
    };
    let preferred_peers = iter::once(disconnected_source).collect();
    let sent_request = Request::BlocksByHashFromPeers {
        hashes,
        preferred_peers,
    };

    // Use two ready peers which are not the requested source peer.
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    tokio::time::pause();

    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        assert_eq!(peer_ready.ready_services.len(), 2);

        let response_fut = peer_ready.call(sent_request);

        for handle in &mut handles {
            assert!(
                handle
                    .try_to_receive_outbound_client_request()
                    .request()
                    .is_none(),
                "source-routed request fell back to a non-source peer"
            );
        }

        let response_task = tokio::spawn(response_fut);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(50)).await;

        let response = response_task
            .await
            .expect("source unavailable response task should not panic");
        assert_eq!(
            response
                .expect_err("peer set should return an error when sources are unavailable")
                .downcast_ref::<SharedPeerError>()
                .expect("peer set should return a boxed SharedPeerError")
                .inner_debug(),
            "NoReadyPeers"
        );
    });
}

/// Check that source-routed single inventory requests do not fall back to an
/// advertised non-source peer when the source peer is connected but busy.
#[test]
fn peer_set_route_source_single_busy_ignores_advertised_non_source() {
    let test_hash = block::Hash([4; 32]);
    let test_inv = InventoryHash::Block(test_hash);
    let source_peer: PeerSocketAddr = "127.0.0.1:1"
        .parse()
        .expect("unexpected invalid peer address");
    let advertised_peer: PeerSocketAddr = "127.0.0.1:2"
        .parse()
        .expect("unexpected invalid peer address");

    let sent_request = Request::BlocksByHashFromPeers {
        hashes: iter::once(test_hash).collect(),
        preferred_peers: iter::once(source_peer).collect(),
    };

    let non_source_available = InventoryStatus::new_available(test_inv, advertised_peer);

    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    tokio::time::pause();

    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, mut peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        let inventory_sender = peer_set_guard
            .inventory_sender()
            .as_mut()
            .expect("unexpected missing inv sender");
        inventory_sender
            .send(non_source_available)
            .expect("unexpected dropped receiver");

        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");
        assert_eq!(peer_ready.ready_services.len(), 2);

        let source_service = peer_ready
            .ready_services
            .remove(&source_peer)
            .expect("source peer should start ready");
        std::mem::drop(source_service);

        let (cancel_tx, _cancel_rx) =
            crate::peer_set::set::oneshot::channel::<crate::peer_set::set::CancelClientWork>();
        peer_ready.cancel_handles.insert(source_peer, cancel_tx);

        let response_fut = peer_ready.call(sent_request);

        for handle in &mut handles {
            assert!(
                handle
                    .try_to_receive_outbound_client_request()
                    .request()
                    .is_none(),
                "source-routed request fell back to a non-source peer"
            );
        }

        let response_task = tokio::spawn(response_fut);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(50)).await;

        let response = response_task
            .await
            .expect("source busy response task should not panic");
        assert_eq!(
            response
                .expect_err("peer set should return an error when sources are busy")
                .downcast_ref::<SharedPeerError>()
                .expect("peer set should return a boxed SharedPeerError")
                .inner_debug(),
            "PreferredPeersBusy"
        );
    });
}

/// Check that source-routed single inventory requests refresh sources when the
/// only connected source peer is already missing the requested inventory.
#[test]
fn peer_set_route_source_single_mixed_missing_disconnected_is_unavailable() {
    let test_hash = block::Hash([5; 32]);
    let test_inv = InventoryHash::Block(test_hash);
    let missing_source: PeerSocketAddr = "127.0.0.1:1"
        .parse()
        .expect("unexpected invalid peer address");
    let non_source_peer: PeerSocketAddr = "127.0.0.1:2"
        .parse()
        .expect("unexpected invalid peer address");
    let disconnected_source: PeerSocketAddr = "127.0.0.1:99"
        .parse()
        .expect("unexpected invalid peer address");

    let sent_request = Request::BlocksByHashFromPeers {
        hashes: iter::once(test_hash).collect(),
        preferred_peers: [missing_source, disconnected_source].into_iter().collect(),
    };

    let source_missing = InventoryStatus::new_missing(test_inv, missing_source);
    let non_source_available = InventoryStatus::new_available(test_inv, non_source_peer);

    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    tokio::time::pause();

    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, mut peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        let inventory_sender = peer_set_guard
            .inventory_sender()
            .as_mut()
            .expect("unexpected missing inv sender");
        inventory_sender
            .send(source_missing)
            .expect("unexpected dropped receiver");
        peer_set
            .inventory_registry
            .update()
            .await
            .expect("inventory registry should process missing source");
        inventory_sender
            .send(non_source_available)
            .expect("unexpected dropped receiver");
        peer_set
            .inventory_registry
            .update()
            .await
            .expect("inventory registry should process non-source advertisement");
        assert!(
            peer_set
                .inventory_registry
                .missing_peers(test_inv)
                .any(|addr| *addr == missing_source),
            "missing source should be visible before routing"
        );

        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");
        assert_eq!(peer_ready.ready_services.len(), 2);

        let response_fut = peer_ready.call(sent_request);

        for handle in &mut handles {
            assert!(
                handle
                    .try_to_receive_outbound_client_request()
                    .request()
                    .is_none(),
                "source-routed request fell back to a non-source peer"
            );
        }

        let response_task = tokio::spawn(response_fut);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(50)).await;

        let response = response_task
            .await
            .expect("source unavailable response task should not panic");
        assert_eq!(
            response
                .expect_err("peer set should return an error when no usable sources are available")
                .downcast_ref::<SharedPeerError>()
                .expect("peer set should return a boxed SharedPeerError")
                .inner_debug(),
            "NoReadyPeers"
        );
    });
}

/// Check that source-routed batch inventory requests do not fall back to random
/// peers when the source set is empty.
#[test]
fn peer_set_route_source_batch_empty_sources_fail_without_fallback() {
    let block1_hash = block::Hash([6; 32]);
    let block2_hash = block::Hash([7; 32]);
    let sent_request = Request::BlocksByHashFromPeers {
        hashes: [block1_hash, block2_hash].into_iter().collect(),
        preferred_peers: HashSet::new(),
    };

    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    tokio::time::pause();

    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");
        assert_eq!(peer_ready.ready_services.len(), 2);

        let response_fut = peer_ready.call(sent_request);

        for handle in &mut handles {
            assert!(
                handle
                    .try_to_receive_outbound_client_request()
                    .request()
                    .is_none(),
                "source-routed request fell back to a non-source peer"
            );
        }

        let response_task = tokio::spawn(response_fut);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(50)).await;

        let response = response_task
            .await
            .expect("source unavailable response task should not panic");
        assert_eq!(
            response
                .expect_err("peer set should return an error when no sources are available")
                .downcast_ref::<SharedPeerError>()
                .expect("peer set should return a boxed SharedPeerError")
                .inner_debug(),
            "NoReadyPeers"
        );
    });
}

/// Check that source-routed single inventory requests do not fall back to random
/// peers when the source peer is connected but busy.
#[test]
fn peer_set_route_source_single_busy_fail_without_fallback() {
    peer_set_route_source_busy_fail_without_fallback(false);
}

/// Check that source-routed batch inventory requests do not fall back to random
/// peers when the source peer is connected but busy.
#[test]
fn peer_set_route_source_batch_busy_fail_without_fallback() {
    peer_set_route_source_busy_fail_without_fallback(true);
}

fn peer_set_route_source_busy_fail_without_fallback(batch: bool) {
    let block2_hash = block::Hash([2; 32]);
    let block3_hash = block::Hash([3; 32]);
    let source_peer: PeerSocketAddr = "127.0.0.1:1"
        .parse()
        .expect("unexpected invalid peer address");

    let hashes = if batch {
        [block2_hash, block3_hash].into_iter().collect()
    } else {
        iter::once(block2_hash).collect()
    };
    let sent_request = Request::BlocksByHashFromPeers {
        hashes,
        preferred_peers: iter::once(source_peer).collect(),
    };

    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();

    tokio::time::pause();

    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");
        assert_eq!(peer_ready.ready_services.len(), 2);

        let source_service = peer_ready
            .ready_services
            .remove(&source_peer)
            .expect("source peer should start ready");
        std::mem::drop(source_service);

        let (cancel_tx, _cancel_rx) =
            crate::peer_set::set::oneshot::channel::<crate::peer_set::set::CancelClientWork>();
        peer_ready.cancel_handles.insert(source_peer, cancel_tx);

        let response_fut = peer_ready.call(sent_request);

        for handle in &mut handles {
            assert!(
                handle
                    .try_to_receive_outbound_client_request()
                    .request()
                    .is_none(),
                "source-routed request fell back to a non-source peer"
            );
        }

        let response_task = tokio::spawn(response_fut);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(50)).await;

        let response = response_task
            .await
            .expect("source busy response task should not panic");
        assert_eq!(
            response
                .expect_err("peer set should return an error when sources are busy")
                .downcast_ref::<SharedPeerError>()
                .expect("peer set should return a boxed SharedPeerError")
                .inner_debug(),
            "PreferredPeersBusy"
        );
    });
}
