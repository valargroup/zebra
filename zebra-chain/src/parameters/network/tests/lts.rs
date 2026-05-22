//! Unit tests for the LTS / NSM payout helpers
//! ([`crate::parameters::subsidy::lts_disbursement_start`],
//! [`crate::parameters::subsidy::lts_payout`]).
//!
//! These tests exercise the pure payout function in isolation. The payout
//! is a continuous ZIP-234 ceiling fraction of the parent LTS pool — the
//! per-block dynamics across multiple blocks (decay, inflow propagation)
//! are also covered here at the function level, and the per-fork resolution
//! path is covered by the contextual block-validation tests in `zebra-state`.

use crate::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::{
        subsidy::{lts_disbursement_start, lts_payout},
        testnet::ConfiguredActivationHeights,
        Network, NetworkUpgrade,
    },
};

/// Builds a regtest with NU7 active at height 1 — the smallest config that
/// makes [`lts_disbursement_start`] return `Some(_)`.
fn regtest_nu7_at_1() -> Network {
    Network::new_regtest(
        ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        }
        .into(),
    )
}

/// Closed-form expected payout matching the ZIP-234 ceiling rule used by the
/// implementation. Test-side reference for the consensus formula.
fn expected_payout_for(parent_pool: u64) -> u64 {
    if parent_pool == 0 {
        return 0;
    }
    let numerator = u128::from(parent_pool) * 4_126u128;
    let payout = numerator.div_ceil(10_000_000_000u128);
    u64::try_from(payout.min(u128::from(parent_pool))).unwrap()
}

/// `lts_disbursement_start = height_for_halving(halving(NU7) + 2, network)`,
/// and is `None` on networks where NU7 isn't configured.
#[test]
fn lts_disbursement_start_requires_nu7() {
    // Mainnet does not have NU7 configured → no disbursement_start.
    assert_eq!(None, lts_disbursement_start(&Network::Mainnet));

    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).expect("regtest has NU7 configured");
    let nu7 = NetworkUpgrade::Nu7
        .activation_height(&network)
        .expect("regtest has NU7 configured");
    assert!(
        start > nu7,
        "disbursement_start ({start:?}) should be after NU7 ({nu7:?})"
    );
}

/// Before the disbursement window, the payout is zero regardless of pool size.
#[test]
fn lts_payout_zero_before_disbursement_start() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();
    let one_zec = Amount::<NonNegative>::try_from(100_000_000).unwrap();

    // One block before disbursement: payout = 0 even with a huge pool.
    let pre_height = start.previous().unwrap();
    assert_eq!(
        Amount::<NonNegative>::zero(),
        lts_payout(pre_height, &network, one_zec)
    );

    // At genesis: payout = 0.
    assert_eq!(
        Amount::<NonNegative>::zero(),
        lts_payout(Height(0), &network, one_zec)
    );
}

/// On a network without NU7 configured (so no disbursement start), the payout
/// is zero at every height regardless of parent pool.
#[test]
fn lts_payout_zero_when_nu7_unconfigured() {
    let one_zec = Amount::<NonNegative>::try_from(100_000_000).unwrap();
    let mainnet_height = Height(2_000_000);
    assert_eq!(
        Amount::<NonNegative>::zero(),
        lts_payout(mainnet_height, &Network::Mainnet, one_zec)
    );
}

/// At and within the disbursement window, an empty parent pool yields a zero
/// payout.
#[test]
fn lts_payout_zero_when_parent_pool_is_empty() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();

    assert_eq!(
        Amount::<NonNegative>::zero(),
        lts_payout(start, &network, Amount::<NonNegative>::zero())
    );

    // Same well past disbursement_start.
    assert_eq!(
        Amount::<NonNegative>::zero(),
        lts_payout(
            (start + 100).unwrap(),
            &network,
            Amount::<NonNegative>::zero()
        )
    );
}

/// Inside the disbursement window the payout equals
/// `ceil(parent_pool * 4126 / 10_000_000_000)`. Exercises a few pool sizes
/// against the closed-form helper to pin down the ZIP-234 fraction and the
/// ceiling rounding.
#[test]
fn lts_payout_matches_zip234_ceiling_fraction() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();

    // 10_000_000_000 zatoshi → exactly 4126 (no remainder).
    let exact = 10_000_000_000u64;
    assert_eq!(
        Amount::<NonNegative>::try_from(expected_payout_for(exact)).unwrap(),
        lts_payout(
            start,
            &network,
            Amount::<NonNegative>::try_from(exact).unwrap()
        )
    );
    assert_eq!(4_126, expected_payout_for(exact));

    // 1 ZEC = 100_000_000 zatoshi → ceil(100_000_000 * 4126 / 10^10) = 42.
    let one_zec = 100_000_000u64;
    assert_eq!(42, expected_payout_for(one_zec));
    assert_eq!(
        Amount::<NonNegative>::try_from(42u64).unwrap(),
        lts_payout(
            start,
            &network,
            Amount::<NonNegative>::try_from(one_zec).unwrap()
        )
    );

    // A pool whose multiplication is not divisible by 10^10 must round up.
    // parent_pool = 1234567 → numerator = 1234567 * 4126 = 5_093_823_442;
    //   ceil(5_093_823_442 / 10^10) = 1.
    let small_residual = 1_234_567u64;
    assert_eq!(1, expected_payout_for(small_residual));
    assert_eq!(
        Amount::<NonNegative>::try_from(1u64).unwrap(),
        lts_payout(
            start,
            &network,
            Amount::<NonNegative>::try_from(small_residual).unwrap()
        )
    );
}

/// A one-zatoshi pool drains in a single block under the ceiling rule —
/// no separate dust handling is needed.
#[test]
fn lts_payout_one_zatoshi_pool_drains_in_one_block() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();
    let one = Amount::<NonNegative>::try_from(1u64).unwrap();

    assert_eq!(one, lts_payout(start, &network, one));
}

/// The payout is always capped by the parent pool, so the chain never
/// underflows. Covers small pools where the ZIP-234 ceiling could otherwise
/// exceed the available amount.
#[test]
fn lts_payout_never_exceeds_parent_pool() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();

    for pool in [1u64, 7, 1_000, 1_234_567, 100_000_000, 10_000_000_000] {
        let parent_pool = Amount::<NonNegative>::try_from(pool).unwrap();
        let payout = lts_payout(start, &network, parent_pool);
        assert!(
            u64::from(payout) <= pool,
            "payout {} must not exceed parent pool {}",
            u64::from(payout),
            pool
        );
        assert_eq!(
            Amount::<NonNegative>::try_from(expected_payout_for(pool)).unwrap(),
            payout
        );
    }
}

/// Across two consecutive blocks with no inflow, the parent pool shrinks by
/// the previous payout and the next payout is recomputed from the smaller
/// pool — never larger than the prior payout.
#[test]
fn lts_payout_decays_across_two_blocks_without_inflow() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();

    let parent_pool_n = 1_000_000_000_000u64; // 10_000 ZEC
    let payout_n = u64::from(lts_payout(
        start,
        &network,
        Amount::<NonNegative>::try_from(parent_pool_n).unwrap(),
    ));
    assert_eq!(expected_payout_for(parent_pool_n), payout_n);

    // Next block's parent pool is the prior parent pool minus the prior payout
    // (no inflow this block).
    let parent_pool_n_plus_1 = parent_pool_n - payout_n;
    let payout_n_plus_1 = u64::from(lts_payout(
        start.next().unwrap(),
        &network,
        Amount::<NonNegative>::try_from(parent_pool_n_plus_1).unwrap(),
    ));
    assert_eq!(expected_payout_for(parent_pool_n_plus_1), payout_n_plus_1);

    assert!(
        payout_n_plus_1 <= payout_n,
        "no-inflow decay must be monotone: payout_n_plus_1 ({payout_n_plus_1}) > payout_n ({payout_n})"
    );
    assert!(
        payout_n_plus_1 > 0,
        "pool is far from zero — decay shouldn't bottom out"
    );
}

/// A large LTS contribution at block N enters the pool in block N and only
/// affects block N+1's payout — block N's payout still uses the parent pool.
/// Exercises the parent-pool rule that breaks the within-block circularity.
#[test]
fn lts_payout_inflow_at_block_n_affects_block_n_plus_1_only() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();

    let parent_pool = 1_000_000_000_000u64; // 10_000 ZEC at block N's parent
    let contribution = 500_000_000_000u64; // hefty inflow during block N

    // Block N's payout uses the parent pool only — the inflow doesn't appear.
    let payout_n = u64::from(lts_payout(
        start,
        &network,
        Amount::<NonNegative>::try_from(parent_pool).unwrap(),
    ));
    assert_eq!(expected_payout_for(parent_pool), payout_n);

    // After block N applies, the new pool = parent_pool + contribution - payout_n.
    let new_pool = parent_pool + contribution - payout_n;

    // Block N+1's payout reflects the new pool.
    let payout_n_plus_1 = u64::from(lts_payout(
        start.next().unwrap(),
        &network,
        Amount::<NonNegative>::try_from(new_pool).unwrap(),
    ));
    assert_eq!(expected_payout_for(new_pool), payout_n_plus_1);

    // The inflow makes the next payout strictly larger than the current one —
    // the contribution propagates forward, not into the same block.
    assert!(
        payout_n_plus_1 > payout_n,
        "inflow should grow the next block's payout: {payout_n_plus_1} ≤ {payout_n}"
    );
}

/// Halving boundaries have no special LTS-payout effect under the continuous
/// rule: with the same parent pool on both sides of the boundary, the payout
/// is identical. The block subsidy still halves at the boundary — that
/// schedule lives in `block_subsidy`, not in the LTS path.
#[test]
fn lts_payout_no_special_effect_at_halving_boundary() {
    let network = regtest_nu7_at_1();
    let start = lts_disbursement_start(&network).unwrap();

    // Pick any two heights well inside the disbursement window. The math is
    // height-independent inside the window — the rule depends only on the
    // parent pool, not on whether `height` straddles a halving boundary.
    let height_a = (start + 5).unwrap();
    let height_b = (start + 5_000).unwrap();
    let pool = Amount::<NonNegative>::try_from(1_000_000_000_000u64).unwrap();

    assert_eq!(
        lts_payout(height_a, &network, pool),
        lts_payout(height_b, &network, pool),
        "same parent pool ⇒ same payout, regardless of height inside the disbursement window"
    );
}
