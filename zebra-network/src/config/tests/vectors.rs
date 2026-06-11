//! Fixed test vectors for zebra-network configuration.

use static_assertions::const_assert;
use std::collections::BTreeMap;

use zebra_chain::{
    block::Height,
    parameters::{
        fork,
        testnet::{self, ConfiguredFundingStreams},
        Magic, Network, NetworkUpgrade,
    },
    work::difficulty::{ExpandedDifficulty, U256},
};

use crate::{
    constants::{INBOUND_PEER_LIMIT_MULTIPLIER, OUTBOUND_PEER_LIMIT_MULTIPLIER},
    Config,
};

#[test]
fn parse_config_listen_addr() {
    let _init_guard = zebra_test::init();

    let fixtures = vec![
        ("listen_addr = '0.0.0.0'", "0.0.0.0:8233"),
        ("listen_addr = '0.0.0.0:9999'", "0.0.0.0:9999"),
        (
            "listen_addr = '0.0.0.0'\nnetwork = 'Testnet'",
            "0.0.0.0:18233",
        ),
        (
            "listen_addr = '0.0.0.0:8233'\nnetwork = 'Testnet'",
            "0.0.0.0:8233",
        ),
        ("listen_addr = '[::]'", "[::]:8233"),
        ("listen_addr = '[::]:9999'", "[::]:9999"),
        ("listen_addr = '[::]'\nnetwork = 'Testnet'", "[::]:18233"),
        (
            "listen_addr = '[::]:8233'\nnetwork = 'Testnet'",
            "[::]:8233",
        ),
        ("listen_addr = '[::1]:8233'", "[::1]:8233"),
        ("listen_addr = '[2001:db8::1]:8233'", "[2001:db8::1]:8233"),
    ];

    for (config, value) in fixtures {
        let config: Config = toml::from_str(config).unwrap();
        assert_eq!(config.listen_addr.to_string(), value);
    }
}

/// Make sure the peer connection limits are consistent with each other.
#[test]
fn ensure_peer_connection_limits_consistent() {
    let _init_guard = zebra_test::init();

    // This fork prioritizes fast outbound sync over inbound-serving capacity.
    const_assert!(INBOUND_PEER_LIMIT_MULTIPLIER <= OUTBOUND_PEER_LIMIT_MULTIPLIER);

    let config = Config::default();

    assert!(
        config.peerset_inbound_connection_limit() <= config.peerset_outbound_connection_limit(),
        "this fork caps inbound connections at or below the outbound limit, to prioritize sync",
    );
}

#[test]
fn testnet_params_serialization_roundtrip() {
    let _init_guard = zebra_test::init();

    let config = Config {
        network: testnet::Parameters::build()
            .with_disable_pow(true)
            .to_network()
            .expect("failed to build configured network"),
        initial_testnet_peers: [].into(),
        ..Config::default()
    };

    let serialized = toml::to_string(&config).unwrap();
    let deserialized: Config = toml::from_str(&serialized).unwrap();

    assert_eq!(config, deserialized);
}

fn forked_mainnet_network() -> Network {
    let fork_height = Height(3_400_000);
    let fork_hash = zebra_chain::block::Hash([0x11; 32]);
    let network_magic = Magic([0xab, 0xcd, 0xef, 0x01]);
    let post_fork_limit = ExpandedDifficulty::from((U256::one() << 251) - 1).to_compact();

    Network::new_forked_mainnet(
        fork::Parameters::new(
            "LocalFork",
            fork_height,
            fork_hash,
            network_magic,
            BTreeMap::from([(Height(3_400_001), NetworkUpgrade::Nu7)]),
            post_fork_limit,
            true,
        )
        .expect("test fork parameters should be valid"),
    )
}

#[test]
fn forked_mainnet_params_serialization_roundtrip() {
    let _init_guard = zebra_test::init();

    let config = Config {
        network: forked_mainnet_network(),
        initial_fork_peers: ["127.0.0.1:38233".to_string()].into(),
        ..Config::default()
    };

    let serialized = toml::to_string(&config).unwrap();
    let deserialized: Config = toml::from_str(&serialized).unwrap();

    assert_eq!(config, deserialized);
    assert!(serialized.contains("forked_mainnet"));
    assert!(serialized.contains("initial_fork_peers"));

    let Network::ForkedMainnet(params) = &deserialized.network else {
        panic!("deserialized network must be ForkedMainnet");
    };

    let post_fork_limit = ExpandedDifficulty::from((U256::one() << 251) - 1)
        .to_compact()
        .to_expanded()
        .expect("test difficulty should expand");
    assert_eq!(params.post_fork_target_difficulty_limit(), post_fork_limit);
    assert_eq!(
        params.post_fork_activation_heights(),
        &BTreeMap::from([(Height(3_400_001), NetworkUpgrade::Nu7)])
    );
}

#[test]
fn forked_mainnet_without_initial_peers_does_not_use_public_seeders() {
    let _init_guard = zebra_test::init();

    let config = Config {
        network: forked_mainnet_network(),
        ..Config::default()
    };

    assert!(
        config.initial_peer_hostnames().is_empty(),
        "forked-mainnet must not inherit public Mainnet or Testnet DNS seeders"
    );
}

#[test]
fn forked_mainnet_config_rejects_ambiguous_difficulty_length() {
    let _init_guard = zebra_test::init();

    let config = r#"
        [network.forked_mainnet]
        fork_name = "LocalFork"
        fork_height = 3400000
        fork_hash = "1111111111111111111111111111111111111111111111111111111111111111"
        network_magic = [171, 205, 239, 1]
        target_difficulty_limit = "1000"
        disable_pow_after_fork = true
    "#;

    let error = toml::from_str::<Config>(config)
        .expect_err("ambiguous difficulty strings should be rejected");

    assert!(error.to_string().contains("8-hex or expanded 64-hex"));
}

#[test]
fn forked_mainnet_config_rejects_activation_height_above_max() {
    let _init_guard = zebra_test::init();

    let config = format!(
        r#"
        [network.forked_mainnet]
        fork_name = "LocalFork"
        fork_height = 3400000
        fork_hash = "1111111111111111111111111111111111111111111111111111111111111111"
        network_magic = [171, 205, 239, 1]
        target_difficulty_limit = "037fffff"
        disable_pow_after_fork = true

        [network.forked_mainnet.post_fork_activation_heights]
        NU7 = {}
    "#,
        Height::MAX.0 + 1,
    );

    let error = toml::from_str::<Config>(&config)
        .expect_err("out-of-range activation heights should be rejected");

    assert!(error.to_string().contains("activation height is invalid"));
}

#[test]
fn default_config_uses_ipv6() {
    let _init_guard = zebra_test::init();
    let config = Config::default();

    assert_eq!(config.listen_addr.to_string(), "[::]:8233");
    assert!(config.listen_addr.is_ipv6());
}

#[test]
fn funding_streams_serialization_roundtrip() {
    let _init_guard = zebra_test::init();

    let fs = testnet::Parameters::default()
        .funding_streams()
        .iter()
        .map(ConfiguredFundingStreams::from)
        .collect();

    let config = Config {
        network: testnet::Parameters::build()
            .with_funding_streams(fs)
            .to_network()
            .expect("failed to build configured network"),
        initial_testnet_peers: [].into(),
        ..Config::default()
    };

    let serialized = toml::to_string(&config).unwrap();
    let deserialized: Config = toml::from_str(&serialized).unwrap();

    assert_eq!(config, deserialized);
}

/// Checks that a configured Testnet's temporary Orchard-disabling soft fork height
/// survives a serialization round-trip.
#[test]
fn temporary_orchard_disabling_soft_fork_height_serialization_roundtrip() {
    let _init_guard = zebra_test::init();

    let soft_fork_height = Height(2_000_000);

    let config = Config {
        network: testnet::Parameters::build()
            .with_temporary_orchard_disabling_soft_fork_height(soft_fork_height)
            .to_network()
            .expect("failed to build configured network"),
        initial_testnet_peers: [].into(),
        ..Config::default()
    };

    let serialized = toml::to_string(&config).unwrap();
    let deserialized: Config = toml::from_str(&serialized).unwrap();

    assert_eq!(config, deserialized);

    // The configured height must be preserved through the round-trip.
    let Network::Testnet(params) = &deserialized.network else {
        panic!("deserialized network must be a Testnet");
    };
    assert_eq!(
        params.temporary_orchard_disabling_soft_fork_height(),
        Some(soft_fork_height),
    );
}
