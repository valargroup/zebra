//! Tests for the chain-history-based portion of the contextual LTS / NSM
//! payout check ([`super::super::lts::expected_lts_payout`]).
//!
//! These tests exercise the resolver against a hand-built [`Chain`] whose
//! `block_info_by_height` map carries planted LTS pool snapshots. The
//! [`ZebraDb`] passed to the check is empty — the test layout keeps every
//! ancestor we look up inside `parent_chain`, so the finalized fallback is
//! never hit. End-to-end tests of `validate_and_commit` would also exercise
//! the fallback path; that's covered by the full-chain integration tests
//! and is out of scope here.
//!
//! The block-bytes side of the contextual check (the implied-claim
//! derivation and ZIP-235 deposit floor in
//! [`super::super::lts::check_claimed_lts_payout`]) is exercised both by the
//! deposit/over-claim tests below and by full-block validation tests.
//!
//! Under the continuous-payout rule, the only state lookup is the parent
//! block's LTS pool snapshot — there is no era-start lookup.

use std::{collections::HashMap, sync::Arc};

use chrono::DateTime;

use zebra_chain::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block::{merkle, Block, Hash, Header, Height},
    block_info::BlockInfo,
    fmt::HexDebug,
    parameters::{
        subsidy::{
            block_subsidy, funding_stream_values, lts_disbursement_start, lts_payout,
            FundingStreamReceiver,
        },
        testnet::ConfiguredActivationHeights,
        Network, NetworkUpgrade,
    },
    transaction::{self, LockTime, Transaction},
    transparent,
    value_balance::ValueBalance,
    work::{difficulty::ParameterDifficulty, equihash::Solution},
};

use crate::{
    service::{
        check::lts::{check_claimed_lts_payout, expected_lts_payout},
        finalized_state::FinalizedState,
        non_finalized_state::Chain,
    },
    Config, ValidateContextError,
};

/// Builds a regtest with NU7 active at height 1 — the same network used by
/// the unit tests on `lts_payout`.
fn regtest_nu7_at_1() -> Network {
    Network::new_regtest(
        ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        }
        .into(),
    )
}

/// Constructs an empty [`Chain`] suitable for injecting `BlockInfo` records
/// directly. The note-commitment trees and history tree are defaulted; we
/// don't exercise any path that reads them.
fn empty_chain(network: &Network) -> Chain {
    Chain::new(
        network,
        Height(0),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        ValueBalance::zero(),
    )
}

/// Plants a [`BlockInfo`] record at `height` carrying `lts_pool` as the LTS
/// balance. Other pool fields are left at zero — the LTS check only reads
/// `value_pools().lts_amount()`.
fn plant_lts_pool(chain: &mut Chain, height: Height, lts_pool: u64) {
    let mut value_pools = ValueBalance::<NonNegative>::zero();
    value_pools.set_lts_amount(Amount::<NonNegative>::try_from(lts_pool).unwrap());
    chain
        .block_info_by_height
        .insert(height, BlockInfo::new(value_pools, 0));
}

fn ephemeral_finalized_state(network: &Network) -> FinalizedState {
    FinalizedState::new(
        &Config::ephemeral(),
        network,
        #[cfg(feature = "elasticsearch")]
        false,
    )
}

fn block_with_coinbase_output(
    network: &Network,
    height: Height,
    output_value: Amount<NonNegative>,
) -> Block {
    let coinbase = Arc::new(Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu7,
        lock_time: LockTime::unlocked(),
        expiry_height: height,
        inputs: vec![transparent::Input::new_coinbase(height, Vec::new(), None)],
        outputs: vec![transparent::Output::new(
            output_value,
            transparent::Script::new(&[]),
        )],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    });
    let transactions = vec![coinbase];
    let merkle_root: merkle::Root = transactions.iter().cloned().collect();

    Block {
        header: Arc::new(Header {
            version: 4,
            previous_block_hash: Hash([0; 32]),
            merkle_root,
            commitment_bytes: HexDebug([0; 32]),
            time: DateTime::from_timestamp(1_700_000_000, 0)
                .expect("hard-coded timestamp is valid"),
            difficulty_threshold: network.target_difficulty_limit().to_compact(),
            nonce: HexDebug([0; 32]),
            solution: Solution::for_proposal(),
        }),
        transactions,
    }
}

/// The ZIP-234 deferred (lockbox) funding-stream contribution to the chain
/// value pool at `height`. The contextual derivation adds this to the coinbase
/// total output, so the deposit-targeting helper must account for it.
fn deferred_pool_change(network: &Network, height: Height) -> Amount<NonNegative> {
    let subsidy = block_subsidy(height, network).unwrap();
    funding_stream_values(height, network, subsidy)
        .unwrap()
        .remove(&FundingStreamReceiver::Deferred)
        .unwrap_or_else(Amount::zero)
}

/// Builds a block whose coinbase has a single transparent output of
/// `coinbase_output_value`, plus — when `fee > 0` — one transparent
/// fee-paying transaction whose `remaining_transaction_value` equals `fee`.
/// Returns the block and the `spent_utxos` map covering the fee tx's input.
fn block_with_coinbase_and_fee(
    network: &Network,
    height: Height,
    coinbase_output_value: Amount<NonNegative>,
    fee: Amount<NonNegative>,
) -> (Block, HashMap<transparent::OutPoint, transparent::Utxo>) {
    let coinbase = Arc::new(Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu7,
        lock_time: LockTime::unlocked(),
        expiry_height: height,
        inputs: vec![transparent::Input::new_coinbase(height, Vec::new(), None)],
        outputs: vec![transparent::Output::new(
            coinbase_output_value,
            transparent::Script::new(&[]),
        )],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    });

    let mut transactions = vec![coinbase];
    let mut spent_utxos = HashMap::new();

    if fee > Amount::<NonNegative>::zero() {
        // input − output == fee, so the tx's remaining value (the miner fee) is `fee`.
        let output_value = Amount::<NonNegative>::try_from(1_000).unwrap();
        let input_value = (output_value + fee).unwrap();
        let outpoint = transparent::OutPoint::from_usize(transaction::Hash([7; 32]), 0);
        spent_utxos.insert(
            outpoint,
            transparent::Utxo::new(
                transparent::Output::new(input_value, transparent::Script::new(&[])),
                Height(1),
                false,
            ),
        );
        transactions.push(Arc::new(Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu7,
            lock_time: LockTime::unlocked(),
            expiry_height: height,
            inputs: vec![transparent::Input::PrevOut {
                outpoint,
                unlock_script: transparent::Script::new(&[]),
                sequence: 0,
            }],
            outputs: vec![transparent::Output::new(
                output_value,
                transparent::Script::new(&[]),
            )],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        }));
    }

    let merkle_root: merkle::Root = transactions.iter().cloned().collect();
    let block = Block {
        header: Arc::new(Header {
            version: 4,
            previous_block_hash: Hash([0; 32]),
            merkle_root,
            commitment_bytes: HexDebug([0; 32]),
            time: DateTime::from_timestamp(1_700_000_000, 0)
                .expect("hard-coded timestamp is valid"),
            difficulty_threshold: network.target_difficulty_limit().to_compact(),
            nonce: HexDebug([0; 32]),
            solution: Solution::for_proposal(),
        }),
        transactions,
    };

    (block, spent_utxos)
}

/// Coinbase output value that makes the contextual implied claim equal
/// `target_claim` for a block at `height` carrying `fee` in miner fees.
///
/// The contextual derivation is
/// `implied_claim = output + deferred − subsidy − fee`, so
/// `output = target_claim + subsidy + fee − deferred`.
fn coinbase_output_for_claim(
    network: &Network,
    height: Height,
    fee: Amount<NonNegative>,
    target_claim: i64,
) -> Amount<NonNegative> {
    let subsidy = i64::from(block_subsidy(height, network).unwrap());
    let deferred = i64::from(deferred_pool_change(network, height));
    let fee = i64::from(fee);

    Amount::<NonNegative>::try_from(target_claim + subsidy + fee - deferred)
        .expect("test coinbase output is a valid non-negative amount")
}

/// Before `lts_disbursement_start`, the expected payout is zero — there is
/// no LTS pool snapshot to consult yet.
#[test]
fn expected_payout_zero_before_disbursement_start() {
    let network = regtest_nu7_at_1();
    let chain = empty_chain(&network);
    let finalized = ephemeral_finalized_state(&network);
    let pre_height = lts_disbursement_start(&network)
        .unwrap()
        .previous()
        .unwrap();

    assert_eq!(
        Amount::<NonNegative>::zero(),
        expected_lts_payout(&network, &chain, &finalized.db, pre_height)
            .expect("pre-disbursement payout does not need BlockInfo"),
    );
}

/// After NU7 activates but before `lts_disbursement_start`, the expected LTS
/// payout is still zero, so a positive implied claim must be rejected.
#[test]
fn claimed_payout_rejects_positive_claim_before_disbursement_start() {
    let network = regtest_nu7_at_1();
    let chain = empty_chain(&network);
    let finalized = ephemeral_finalized_state(&network);
    let pre_height = lts_disbursement_start(&network)
        .unwrap()
        .previous()
        .unwrap();

    assert!(
        pre_height >= NetworkUpgrade::Nu7.activation_height(&network).unwrap(),
        "test height must be inside the pre-disbursement NU7 window"
    );

    let subsidy = block_subsidy(pre_height, &network).unwrap();
    let excess = Amount::<NonNegative>::try_from(1u64).unwrap();
    let output_value = (subsidy + excess).unwrap();
    let block = block_with_coinbase_output(&network, pre_height, output_value);

    let err = check_claimed_lts_payout(
        &network,
        &chain,
        &finalized.db,
        pre_height,
        &block,
        &HashMap::new(),
    )
    .expect_err("positive pre-disbursement LTS claim must be rejected");

    let ValidateContextError::InvalidLtsDeposit {
        height,
        expected_minimum,
        actual,
    } = err
    else {
        panic!("unexpected error: {err:?}");
    };

    assert_eq!(pre_height, height);
    assert_eq!(Amount::<NonNegative>::zero(), expected_minimum);
    assert!(actual < Amount::<NonNegative>::zero());
}

/// Before the disbursement window, a coinbase that under-claims (deposits into
/// the LTS pool) by at least the ZIP-235 minimum is accepted, and the returned
/// pool delta equals the deposited amount.
#[test]
fn claimed_payout_accepts_under_claim_before_disbursement_start() {
    let network = regtest_nu7_at_1();
    let chain = empty_chain(&network);
    let finalized = ephemeral_finalized_state(&network);
    let pre_height = lts_disbursement_start(&network)
        .unwrap()
        .previous()
        .unwrap();

    // No fees → the minimum deposit is zero; deposit 500 zatoshi (claim = −500).
    let deposit = 500i64;
    let output = coinbase_output_for_claim(&network, pre_height, Amount::zero(), -deposit);
    let (block, spent_utxos) =
        block_with_coinbase_and_fee(&network, pre_height, output, Amount::zero());

    let delta = check_claimed_lts_payout(
        &network,
        &chain,
        &finalized.db,
        pre_height,
        &block,
        &spent_utxos,
    )
    .expect("under-claim that meets the zero minimum deposit is valid");

    // pool delta = −implied_claim = +deposit
    assert_eq!(Amount::<NegativeAllowed>::try_from(deposit).unwrap(), delta);
}

/// Inside the disbursement window, claiming more than the scheduled payout
/// (net of the minimum deposit) is rejected as an over-claim.
#[test]
fn claimed_payout_rejects_over_claim_in_disbursement_window() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();
    let parent_height = start.previous().unwrap();

    let parent_pool = 10_000_000_000u64; // expected payout = 4126
    let mut chain = empty_chain(&network);
    plant_lts_pool(&mut chain, parent_height, parent_pool);
    let finalized = ephemeral_finalized_state(&network);

    let expected = i64::from(lts_payout(
        start,
        &network,
        Amount::<NonNegative>::try_from(parent_pool).unwrap(),
    ));
    assert!(expected > 0, "test needs a positive scheduled payout");

    // Claim one zatoshi more than the scheduled payout (zero fees → zero minimum).
    let output = coinbase_output_for_claim(&network, start, Amount::zero(), expected + 1);
    let (block, spent_utxos) =
        block_with_coinbase_and_fee(&network, start, output, Amount::zero());

    let err = check_claimed_lts_payout(
        &network,
        &chain,
        &finalized.db,
        start,
        &block,
        &spent_utxos,
    )
    .expect_err("over-claiming the scheduled payout must be rejected");

    let ValidateContextError::InvalidLtsDeposit {
        height,
        expected_minimum,
        actual,
    } = err
    else {
        panic!("unexpected error: {err:?}");
    };

    assert_eq!(start, height);
    assert_eq!(Amount::<NonNegative>::zero(), expected_minimum);
    assert_eq!(Amount::<NegativeAllowed>::try_from(-1).unwrap(), actual);
}

/// With non-zero miner fees, the contextual check enforces the ZIP-235 60%
/// floor: depositing exactly the minimum is accepted; one zatoshi short is
/// rejected.
#[test]
fn claimed_payout_enforces_zip235_minimum_with_fees() {
    let network = regtest_nu7_at_1();
    let chain = empty_chain(&network);
    let finalized = ephemeral_finalized_state(&network);
    let pre_height = lts_disbursement_start(&network)
        .unwrap()
        .previous()
        .unwrap();

    let fee = Amount::<NonNegative>::try_from(1_000).unwrap();
    let minimum = 600i64; // 60% of 1000

    // Deposit exactly the minimum: claim = −minimum, so deposit == floor → accepted.
    let output = coinbase_output_for_claim(&network, pre_height, fee, -minimum);
    let (block, spent_utxos) = block_with_coinbase_and_fee(&network, pre_height, output, fee);

    let delta = check_claimed_lts_payout(
        &network,
        &chain,
        &finalized.db,
        pre_height,
        &block,
        &spent_utxos,
    )
    .expect("depositing exactly the ZIP-235 minimum is valid");
    assert_eq!(Amount::<NegativeAllowed>::try_from(minimum).unwrap(), delta);

    // Deposit one zatoshi short of the minimum → rejected.
    let output = coinbase_output_for_claim(&network, pre_height, fee, -(minimum - 1));
    let (block, spent_utxos) = block_with_coinbase_and_fee(&network, pre_height, output, fee);

    let err = check_claimed_lts_payout(
        &network,
        &chain,
        &finalized.db,
        pre_height,
        &block,
        &spent_utxos,
    )
    .expect_err("depositing below the ZIP-235 minimum must be rejected");

    let ValidateContextError::InvalidLtsDeposit {
        expected_minimum,
        actual,
        ..
    } = err
    else {
        panic!("unexpected error: {err:?}");
    };

    assert_eq!(Amount::<NonNegative>::try_from(minimum).unwrap(), expected_minimum);
    assert_eq!(
        Amount::<NegativeAllowed>::try_from(minimum - 1).unwrap(),
        actual
    );
}

/// At `lts_disbursement_start`, the expected payout is
/// `ceil(parent_pool * 4126 / 10_000_000_000)` derived from the parent pool
/// snapshot in the chain.
#[test]
fn expected_payout_matches_lts_payout_at_disbursement_start() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();

    // Pick a parent pool large enough that the expected payout is positive.
    let parent_pool_u = 10_000_000_000u64; // expected payout = 4126 exactly
    let parent_height = start.previous().unwrap();
    let mut chain = empty_chain(&network);
    plant_lts_pool(&mut chain, parent_height, parent_pool_u);
    let finalized = ephemeral_finalized_state(&network);

    let expected_amount = lts_payout(
        start,
        &network,
        Amount::<NonNegative>::try_from(parent_pool_u).unwrap(),
    );
    assert_eq!(
        Amount::<NonNegative>::try_from(4_126u64).unwrap(),
        expected_amount,
        "sanity-check the closed-form payout"
    );

    assert_eq!(
        expected_amount,
        expected_lts_payout(&network, &chain, &finalized.db, start)
            .expect("planted parent BlockInfo is available"),
    );
}

#[test]
fn expected_payout_errors_when_parent_block_info_is_missing() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();
    let chain = empty_chain(&network);
    let finalized = ephemeral_finalized_state(&network);
    let parent_height = start.previous().unwrap();

    let err = expected_lts_payout(&network, &chain, &finalized.db, start)
        .expect_err("missing parent BlockInfo should be a validation error");

    let ValidateContextError::MissingLtsBlockInfo { height } = err else {
        panic!("unexpected error: {err:?}");
    };

    assert_eq!(parent_height, height);
}

/// On the tail block where the parent pool is tiny, the ceiling rule pays out
/// at most the parent pool.
#[test]
fn expected_payout_tail_block_capped_to_parent_pool() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();

    let test_height = (start + 5).unwrap();
    let parent_height = test_height.previous().unwrap();

    // Parent pool of 7 zatoshi → ceiling rule says payout = 1, capped to 7
    // by the cap-to-parent-pool rule.
    let parent_pool_u = 7u64;
    let mut chain = empty_chain(&network);
    plant_lts_pool(&mut chain, parent_height, parent_pool_u);
    let finalized = ephemeral_finalized_state(&network);

    let expected_amount = lts_payout(
        test_height,
        &network,
        Amount::<NonNegative>::try_from(parent_pool_u).unwrap(),
    );
    assert!(
        u64::from(expected_amount) <= parent_pool_u,
        "expected payout must be capped by parent pool"
    );

    assert_eq!(
        expected_amount,
        expected_lts_payout(&network, &chain, &finalized.db, test_height)
            .expect("planted parent BlockInfo is available"),
    );
}

/// On a non-finalized fork whose parent is *not* the current best-chain
/// tip, the expected payout is computed from *that* fork's own pool
/// history — not from a sibling fork or the finalized tip. This is the
/// re-org safety property: the LTS check is per-`Chain`, not per-best-chain.
#[test]
fn expected_payout_uses_parent_chains_own_pool_history() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();
    let parent_height = start.previous().unwrap();

    let finalized = ephemeral_finalized_state(&network);

    // Fork A: parent pool = 10_000_000_000 → expected payout = 4126.
    let pool_a = 10_000_000_000u64;
    let mut chain_a = empty_chain(&network);
    plant_lts_pool(&mut chain_a, parent_height, pool_a);

    // Fork B: parent pool = 5_000_000_000 → expected payout = 2063.
    let pool_b = 5_000_000_000u64;
    let mut chain_b = empty_chain(&network);
    plant_lts_pool(&mut chain_b, parent_height, pool_b);

    let expected_a = lts_payout(
        start,
        &network,
        Amount::<NonNegative>::try_from(pool_a).unwrap(),
    );
    let expected_b = lts_payout(
        start,
        &network,
        Amount::<NonNegative>::try_from(pool_b).unwrap(),
    );
    assert_ne!(expected_a, expected_b, "test design: forks must differ");

    assert_eq!(
        expected_a,
        expected_lts_payout(&network, &chain_a, &finalized.db, start)
            .expect("planted fork A parent BlockInfo is available"),
    );
    assert_eq!(
        expected_b,
        expected_lts_payout(&network, &chain_b, &finalized.db, start)
            .expect("planted fork B parent BlockInfo is available"),
    );
}

/// Halving boundaries have no special effect on the LTS payout: with the same
/// parent-pool snapshot on both sides of a halving boundary, the contextual
/// check produces the same expected payout.
#[test]
fn expected_payout_unchanged_across_halving_boundary() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();

    let height_a = (start + 1).unwrap();
    let height_b = (start + 5_000).unwrap();
    let parent_a = height_a.previous().unwrap();
    let parent_b = height_b.previous().unwrap();
    let pool = 1_000_000_000_000u64;

    let mut chain = empty_chain(&network);
    plant_lts_pool(&mut chain, parent_a, pool);
    plant_lts_pool(&mut chain, parent_b, pool);
    let finalized = ephemeral_finalized_state(&network);

    let expected_a = expected_lts_payout(&network, &chain, &finalized.db, height_a)
        .expect("planted parent BlockInfo for height A is available");
    let expected_b = expected_lts_payout(&network, &chain, &finalized.db, height_b)
        .expect("planted parent BlockInfo for height B is available");
    assert_eq!(
        expected_a, expected_b,
        "expected payout is height-independent inside the disbursement window"
    );
}

/// Block N's expected payout uses the parent pool. Block N+1's parent pool
/// is `parent_pool + contribution - payout_N`, so block N+1's expected
/// payout reflects the inflow from block N.
#[test]
fn expected_payout_reflects_block_n_inflow_at_block_n_plus_1() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();

    let height_n = (start + 1).unwrap();
    let height_n_plus_1 = (start + 2).unwrap();
    let parent_n = height_n.previous().unwrap();
    let parent_n_plus_1 = height_n_plus_1.previous().unwrap();

    let parent_pool_n = 1_000_000_000_000u64;
    let contribution = 500_000_000_000u64;

    let payout_n = lts_payout(
        height_n,
        &network,
        Amount::<NonNegative>::try_from(parent_pool_n).unwrap(),
    );

    let parent_pool_n_plus_1 = parent_pool_n + contribution - u64::from(payout_n);
    let payout_n_plus_1 = lts_payout(
        height_n_plus_1,
        &network,
        Amount::<NonNegative>::try_from(parent_pool_n_plus_1).unwrap(),
    );
    assert!(
        payout_n_plus_1 > payout_n,
        "block N's contribution should grow block N+1's payout"
    );

    let mut chain = empty_chain(&network);
    plant_lts_pool(&mut chain, parent_n, parent_pool_n);
    plant_lts_pool(&mut chain, parent_n_plus_1, parent_pool_n_plus_1);
    let finalized = ephemeral_finalized_state(&network);

    assert_eq!(
        payout_n,
        expected_lts_payout(&network, &chain, &finalized.db, height_n)
            .expect("planted parent BlockInfo for block N is available"),
    );
    assert_eq!(
        payout_n_plus_1,
        expected_lts_payout(&network, &chain, &finalized.db, height_n_plus_1)
            .expect("planted parent BlockInfo for block N+1 is available"),
    );
}
