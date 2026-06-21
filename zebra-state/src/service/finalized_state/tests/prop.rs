//! Randomised property tests for the finalized state.

use std::env;

use zebra_chain::{
    block::Height,
    parameters::{
        testnet::{ConfiguredActivationHeights, ParametersBuilder},
        NetworkUpgrade,
    },
    LedgerState,
};
use zebra_test::prelude::*;

use crate::{
    config::Config,
    service::{
        arbitrary::PreparedChain,
        finalized_state::{CheckpointVerifiedBlock, FinalizedState},
    },
    tests::FakeChainHelper,
};

const DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES: u32 = 1;

#[test]
fn blocks_with_v5_transactions() -> Result<()> {
    let _init_guard = zebra_test::init();
    proptest!(ProptestConfig::with_cases(env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES)),
        |((chain, count, network, _history_tree) in PreparedChain::default())| {
            let mut state = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            let mut height = Height(0);
            // use `count` to minimize test failures, so they are easier to diagnose
            for block in chain.iter().take(count) {
                let checkpoint_verified = CheckpointVerifiedBlock::from(block.block.clone());
                let (hash, _) = state.commit_finalized_direct(
                    checkpoint_verified.into(),
                    None,
                    None,
                    None,
                    "blocks_with_v5_transactions test"
                ).unwrap();
                prop_assert_eq!(Some(height), state.finalized_tip_height());
                prop_assert_eq!(hash, block.hash);
                height = Height(height.0 + 1);
            }
    });

    Ok(())
}

/// Test if committing blocks from all upgrades work correctly, to make
/// sure the contextual validation done by the finalized state works.
/// Also test if a block with the wrong commitment is correctly rejected.
#[test]
#[allow(clippy::print_stderr)]
fn all_upgrades_and_wrong_commitments_with_fake_activation_heights() -> Result<()> {
    let _init_guard = zebra_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            // These are dummy values. The particular values don't matter much,
            // as long as the nu5 one is smaller than the chains being generated
            // (MAX_PARTIAL_CHAIN_BLOCKS) to make sure that upgrade is exercised
            // in the test below. (The test will fail if that does not happen.)
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), NetworkUpgrade::Nu5, None, false);

    // Use no_shrink() because we're ignoring _count and there is nothing to actually shrink.
    proptest!(ProptestConfig::with_cases(env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES)),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy).with_valid_commitments().no_shrink())| {

            let mut state = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            let mut height = Height(0);
            let heartwood_height = NetworkUpgrade::Heartwood.activation_height(&network).unwrap();
            let heartwood_height_plus1 = (heartwood_height + 1).unwrap();
            let nu5_height = NetworkUpgrade::Nu5.activation_height(&network).unwrap();
            let nu5_height_plus1 = (nu5_height + 1).unwrap();

            let mut failure_count = 0;
            for block in chain.iter() {
                let block_hash = block.hash;
                let current_height = block.block.coinbase_height().unwrap();
                // For some specific heights, try to commit a block with
                // corrupted commitment.
                match current_height {
                    h if h == heartwood_height ||
                        h == heartwood_height_plus1 ||
                        h == nu5_height ||
                        h == nu5_height_plus1 => {
                            let block = block.block.clone().set_block_commitment([0x42; 32]);
                            let checkpoint_verified = CheckpointVerifiedBlock::from(block);
                            state.commit_finalized_direct(
                                checkpoint_verified.into(),
                                None,
                                None,
                                None,
                                "all_upgrades test"
                            ).expect_err("Must fail commitment check");
                            failure_count += 1;
                        },
                    _ => {},
                }
                let checkpoint_verified = CheckpointVerifiedBlock::from(block.block.clone());
                let (hash, _) = state.commit_finalized_direct(
                    checkpoint_verified.into(),
                    None,
                    None,
                    None,
                    "all_upgrades test"
                ).unwrap();
                prop_assert_eq!(Some(height), state.finalized_tip_height());
                prop_assert_eq!(hash, block_hash);
                height = Height(height.0 + 1);
            }
            // Make sure the failure path was triggered
            prop_assert_eq!(failure_count, 4);
    });

    Ok(())
}

/// Verified-commitment-trees fast path (`commit_finalized_direct` Checkpoint arm):
/// committing with correct fixture roots produces the same consensus state (anchor
/// sets + history root) as the legacy recompute path across all upgrade boundaries,
/// and a wrong fixture root is rejected (verify-before-commit) rather than persisted.
/// Exercises: a below-Heartwood seed, history-tree creation at Heartwood, the NU5
/// V1->V2 transition, verify-ahead against the buffered successor, the in-arrears
/// commit of the tip block (no successor), and rejection of a corrupted root.
#[test]
#[allow(clippy::needless_range_loop)] // the loops index blocks[i+1] and the fixture by height
fn vct_fast_path_matches_legacy_and_rejects_wrong_roots() -> Result<()> {
    let _init_guard = zebra_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), NetworkUpgrade::Nu5, None, false);

    proptest!(ProptestConfig::with_cases(env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES)),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;

            // Process a bounded prefix [0, last] spanning the Heartwood (history-tree
            // creation) and NU5 (V1->V2) boundaries plus a couple of V2 blocks; `last` is
            // the tip we compare at. Chains are far longer than this
            // (MAX_PARTIAL_CHAIN_BLOCKS), so this is a plain assertion, not a discard.
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last, "generated chain unexpectedly short");

            // The fast path runs below the checkpoint, seeded from an already-committed
            // tip. Seed just before Heartwood so the fast range creates the history tree
            // (Heartwood) and crosses NU5 (V1->V2).
            let seed = (heartwood - 1) as usize;

            // Legacy pass over [0, last]: record per-block roots for the fast range as
            // the fixture, and the golden consensus state at the tip.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            let mut fixture = std::collections::HashMap::new();
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, None, "vct legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(i as u32, (trees.sapling.root(), trees.orchard.root()));
                }
            }
            let golden_anchors = legacy.db.vct_anchor_digest();
            let golden_history = legacy.db.history_tree().hash();

            // Fast pass over [0, last] with the correct fixture: genesis..=seed recompute
            // (no fixture entry); seed+1..=last-1 verify-ahead against their buffered
            // successor; the tip (`last`) commits on the in-arrears check (next = None).
            // Every fast-eligible block takes the fast path, and the result equals legacy.
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            fast.enable_vct_fast_fixture(fixture.clone());
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last).then(|| (blocks[i + 1].block.clone(), None));
                fast.commit_finalized_direct(cv.into(), None, None, next, "vct fast")
                    .expect("verified fast commit succeeds");
            }
            prop_assert_eq!(fast.db.vct_anchor_digest(), golden_anchors, "fast anchors must match legacy");
            prop_assert_eq!(fast.db.history_tree().hash(), golden_history, "fast history must match legacy");
            prop_assert_eq!(fast.vct_fast_count(), (last - seed) as u64, "every fast-eligible block took the fast path");

            // Negative: corrupt the fixture Sapling root at a V2 (post-NU5) height with a
            // distinct value (the empty root; a V2 block has a non-empty Sapling tree).
            // Fast mode cannot recompute a bad root away (the frontier is frozen), so the
            // wrong root must be *rejected* by the next block's commitment (verify-before-
            // commit) — the commit at that height fails rather than persisting it.
            let bad_height = (nu5 + 1) as usize;
            let mut bad_fixture = fixture.clone();
            let bad_entry = bad_fixture.get_mut(&(bad_height as u32)).unwrap();
            prop_assert_ne!(bad_entry.0, Default::default(), "a V2 block must have a non-empty Sapling root");
            bad_entry.0 = Default::default();

            let mut bad = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            bad.enable_vct_fast_fixture(bad_fixture);
            let mut error_height = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last).then(|| (blocks[i + 1].block.clone(), None));
                if bad.commit_finalized_direct(cv.into(), None, None, next, "vct bad").is_err() {
                    error_height = Some(i);
                    break;
                }
            }
            prop_assert_eq!(error_height, Some(bad_height), "a wrong fixture root is rejected at its own commit");
    });

    Ok(())
}
