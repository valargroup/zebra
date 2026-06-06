//! Fixed test vectors for state contextual validation checks.

use chrono::{Duration, Utc};
use hex::FromHex;
use zebra_chain::serialization::ZcashDeserializeInto;
use zebra_chain::{
    block,
    parameters::Network,
    work::difficulty::{CompactDifficulty, ParameterDifficulty},
};

use super::super::*;

#[test]
fn test_orphan_consensus_check() {
    let _init_guard = zebra_test::init();

    let height = zebra_test::vectors::BLOCK_MAINNET_347499_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .unwrap()
        .coinbase_height()
        .unwrap();

    block_is_not_orphaned(block::Height(0), height).expect("tip is lower so it should be fine");
    block_is_not_orphaned(block::Height(347498), height)
        .expect("tip is lower so it should be fine");
    block_is_not_orphaned(block::Height(347499), height)
        .expect_err("tip is equal so it should error");
    block_is_not_orphaned(block::Height(500000), height)
        .expect_err("tip is higher so it should error");
}

#[test]
fn test_sequential_height_check() {
    let _init_guard = zebra_test::init();

    let height = zebra_test::vectors::BLOCK_MAINNET_347499_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .unwrap()
        .coinbase_height()
        .unwrap();

    height_one_more_than_parent_height(block::Height(0), height)
        .expect_err("block is much lower, should panic");
    height_one_more_than_parent_height(block::Height(347497), height)
        .expect_err("parent height is 2 less, should panic");
    height_one_more_than_parent_height(block::Height(347498), height)
        .expect("parent height is 1 less, should be good");
    height_one_more_than_parent_height(block::Height(347499), height)
        .expect_err("parent height is equal, should panic");
    height_one_more_than_parent_height(block::Height(347500), height)
        .expect_err("parent height is way more, should panic");
    height_one_more_than_parent_height(block::Height(500000), height)
        .expect_err("parent height is way more, should panic");
}

#[test]
fn regtest_allows_non_contextual_difficulty_threshold_when_pow_is_disabled() {
    let _init_guard = zebra_test::init();

    let network = Network::new_regtest(Default::default());
    let context_time = Utc::now();
    let adjusted = AdjustedDifficulty::new_from_header_time(
        context_time + Duration::seconds(1),
        block::Height(0),
        &network,
        vec![(network.target_difficulty_limit().to_compact(), context_time)],
    );
    let non_contextual_threshold = CompactDifficulty::from_hex("200f0f0f")
        .expect("hard-coded regtest difficulty threshold should parse");

    difficulty_threshold_and_time_are_valid(non_contextual_threshold, adjusted)
        .expect("pow-disabled networks should not enforce contextual threshold equality");
}

#[test]
fn pow_enabled_networks_reject_non_contextual_difficulty_threshold() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let context_time = Utc::now();
    let adjusted = AdjustedDifficulty::new_from_header_time(
        context_time + Duration::seconds(1),
        block::Height(1),
        &network,
        vec![(network.target_difficulty_limit().to_compact(), context_time)],
    );
    let non_contextual_threshold = CompactDifficulty::from_hex("200f0f0f")
        .expect("hard-coded regtest difficulty threshold should parse");

    let error = difficulty_threshold_and_time_are_valid(non_contextual_threshold, adjusted)
        .expect_err("pow-enabled networks should enforce contextual threshold equality");
    assert!(
        matches!(
            error,
            ValidateContextError::InvalidDifficultyThreshold { .. }
        ),
        "unexpected error type: {error:?}"
    );
}
