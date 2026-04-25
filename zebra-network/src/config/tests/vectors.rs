//! Fixed test vectors for zebra-network configuration.

use static_assertions::const_assert;
use zebra_chain::parameters::{
    testnet::{self, ConfiguredFundingStreams},
    EquihashParams,
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

    // Zebra should allow more inbound connections, to avoid connection exhaustion
    const_assert!(INBOUND_PEER_LIMIT_MULTIPLIER > OUTBOUND_PEER_LIMIT_MULTIPLIER);

    let config = Config::default();

    assert!(
        config.peerset_inbound_connection_limit() - config.peerset_outbound_connection_limit()
            >= 50,
        "default config should allow more inbound connections, to avoid connection exhaustion",
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
fn regtest_config_uses_regtest_equihash_params() {
    let _init_guard = zebra_test::init();

    let config: Config = toml::from_str("network = 'Regtest'").unwrap();

    assert_eq!(config.network.equihash_params(), EquihashParams::Regtest);
}

#[test]
fn configured_testnet_can_use_regtest_equihash_params() {
    let _init_guard = zebra_test::init();

    let config: Config = toml::from_str(
        r#"
        network = 'Testnet'
        initial_testnet_peers = []

        [testnet_parameters]
        network_name = 'EasyTestnet'
        checkpoints = true
        equihash_params = 'regtest'
        "#,
    )
    .unwrap();

    assert_eq!(config.network.equihash_params(), EquihashParams::Regtest);
}

#[test]
fn configured_testnet_parses_larger_daa_windows() {
    let _init_guard = zebra_test::init();

    let config: Config = toml::from_str(
        r#"
        network = 'Testnet'
        initial_testnet_peers = []

        [testnet_parameters]
        network_name = 'LocalDaaTestnet'
        checkpoints = true
        target_difficulty_limit = '0x0400000000000000000000000000000000000000000000000000000000000000'
        pow_averaging_window = 51
        pow_median_block_span = 33
        post_blossom_pow_target_spacing = 25
        pow_damping_factor = 4
        pow_max_adjust_up_percent = 16
        pow_max_adjust_down_percent = 32
        "#,
    )
    .unwrap();

    assert_eq!(config.network.pow_averaging_window(), 51);
    assert_eq!(config.network.pow_median_block_span(), 33);
    assert_eq!(config.network.post_blossom_pow_target_spacing(), 25);
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
