//! Fixed test vectors for zebra-network configuration.

use static_assertions::const_assert;
use zebra_chain::{
    block::Height,
    parameters::{
        testnet::{self, ConfiguredFundingStreams},
        Network, NetworkUpgrade,
    },
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

#[test]
fn private_nu6_3_marker_requires_explicit_network_magic() {
    let _init_guard = zebra_test::init();

    let config = r#"
        initial_testnet_peers = ["192.0.2.10:18233"]

        [network]
        network_name = "UnityPrivate"
        fork_height = 100
        nu6_3_activation_height = 120
        checkpoints = true
    "#;

    let error = toml::from_str::<Config>(config).expect_err("config should fail");
    assert!(
        error
            .to_string()
            .contains("requires an explicit `network_magic` override"),
        "unexpected error: {error}",
    );
}

#[test]
fn private_nu6_3_marker_requires_explicit_initial_peers() {
    let _init_guard = zebra_test::init();

    let config = r#"
        initial_testnet_peers = []

        [network]
        network_name = "UnityPrivate"
        network_magic = [250, 191, 180, 45]
        fork_height = 100
        nu6_3_activation_height = 120
        checkpoints = true
        extend_funding_stream_addresses_as_required = true
    "#;

    let error = toml::from_str::<Config>(config).expect_err("config should fail");
    assert!(
        error
            .to_string()
            .contains("requires explicit `initial_testnet_peers`"),
        "unexpected error: {error}",
    );
}

#[test]
fn private_nu6_3_marker_requires_ordered_heights() {
    let _init_guard = zebra_test::init();

    let config = r#"
        initial_testnet_peers = ["192.0.2.10:18233"]

        [network]
        network_name = "UnityPrivate"
        network_magic = [250, 191, 180, 45]
        fork_height = 200
        nu6_3_activation_height = 120
        checkpoints = true
    "#;

    let error = toml::from_str::<Config>(config).expect_err("config should fail");
    assert!(
        error
            .to_string()
            .contains("requires `nu6_3_activation_height >= fork_height`"),
        "unexpected error: {error}",
    );
}

#[test]
fn private_nu6_3_marker_requires_matching_configured_nu6_2() {
    let _init_guard = zebra_test::init();

    let config = r#"
        initial_testnet_peers = ["192.0.2.10:18233"]

        [network]
        network_name = "UnityPrivate"
        network_magic = [250, 191, 180, 45]
        fork_height = 100
        nu6_3_activation_height = 120
        checkpoints = true

        [network.activation_heights]
        "NU6.2" = 121
    "#;

    let error = toml::from_str::<Config>(config).expect_err("config should fail");
    assert!(
        error
            .to_string()
            .contains("requires `activation_heights.NU6.2` to match `nu6_3_activation_height`"),
        "unexpected error: {error}",
    );
}

#[test]
fn private_nu6_3_marker_maps_to_nu6_2_activation() {
    let _init_guard = zebra_test::init();

    let config = r#"
        initial_testnet_peers = ["192.0.2.10:18233"]

        [network]
        network_name = "UnityPrivate"
        network_magic = [250, 191, 180, 45]
        fork_height = 100
        nu6_3_activation_height = 120
        checkpoints = true
        extend_funding_stream_addresses_as_required = true
    "#;

    let config: Config = toml::from_str(config).expect("config should parse");
    let nu6_2_height = NetworkUpgrade::Nu6_2.activation_height(&config.network);

    assert_eq!(
        nu6_2_height,
        Some(Height(120)),
        "private NU6.3 marker must map to NU6.2 activation",
    );
}
