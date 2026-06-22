//! Randomised property tests for the finalized state.

use std::env;

use tempfile::TempDir;

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
        finalized_state::{commitment_aux, CheckpointVerifiedBlock, FinalizedState},
    },
    tests::FakeChainHelper,
    HashOrHeight,
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
            nu6_3: Some(48),
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
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

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
            // The dedup: each header commitment is checked once, not twice. Only the
            // first fast block runs its own commitment check; every later fast block
            // was already validated by its predecessor's look-ahead, so it is skipped.
            prop_assert_eq!(fast.vct_prevalidated_count(), (last - seed - 1) as u64, "every fast block after the first skips its redundant own commitment check");

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

            // Negative (Orchard, below NU5): no header commits to an Orchard root below
            // NU5 (V1 history leaves ignore it; no MMR below Heartwood), so the fast path
            // pins it to the empty-tree root. Corrupt a below-NU5 fixture Orchard root to
            // a non-empty value. Unlike the Sapling MMR path (one-block lag), this is a
            // direct check, so it is rejected at the block's *own* commit — closing the
            // hole where an untrusted source injects a spurious Orchard anchor.
            let bad_orchard_height = (nu5 - 1) as usize;
            prop_assert!(bad_orchard_height > seed, "the corrupted height must be in the fast range");
            let empty_orchard = zebra_chain::orchard::tree::NoteCommitmentTree::default().root();
            let wrong_orchard = zebra_chain::orchard::tree::Root::try_from([0u8; 32])
                .expect("zero is a valid pallas base field element");
            prop_assert_ne!(wrong_orchard, empty_orchard, "the wrong root must differ from the empty-tree root");

            let mut bad_orchard_fixture = fixture.clone();
            let bad_orchard_entry = bad_orchard_fixture.get_mut(&(bad_orchard_height as u32)).unwrap();
            prop_assert_eq!(bad_orchard_entry.1, empty_orchard, "a below-NU5 block has the empty Orchard root");
            bad_orchard_entry.1 = wrong_orchard;

            let mut bad_orchard = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            bad_orchard.enable_vct_fast_fixture(bad_orchard_fixture);
            let mut orchard_error_height = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last).then(|| (blocks[i + 1].block.clone(), None));
                if bad_orchard.commit_finalized_direct(cv.into(), None, None, next, "vct bad orchard").is_err() {
                    orchard_error_height = Some(i);
                    break;
                }
            }
            prop_assert_eq!(orchard_error_height, Some(bad_orchard_height), "a wrong below-NU5 orchard root is rejected at its own commit");
    });

    Ok(())
}

/// A verified-commitment-trees fast sync must never legacy-recompute a height whose
/// supplied root is missing once the note-commitment frontier is frozen: the running
/// frontier is no longer the real one, so recomputing would fold a wrong root into the
/// history MMR and silently corrupt consensus state (a peer that omits a height — see the
/// driver's gap handling — could trigger this). Instead the committer must refuse with the
/// retryable `VctSuppliedRootUnavailable` error and leave the database untouched, so the
/// block can be committed later from a fetched root. This guards the liveness/no-corruption
/// half of the peer-source fast path (the bad-root rejection half is covered by
/// `vct_fast_path_matches_legacy_and_rejects_wrong_roots`).
#[test]
#[allow(clippy::needless_range_loop)] // the loop indexes blocks[i+1] and the fixture by height
fn vct_frozen_frontier_hole_refuses_instead_of_recomputing() -> Result<()> {
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
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last, "generated chain unexpectedly short");
            let seed = (heartwood - 1) as usize;

            // Record the per-block roots for the fast range as the fixture.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            let mut fixture = std::collections::HashMap::new();
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, None, "vct hole legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(i as u32, (trees.sapling.root(), trees.orchard.root()));
                }
            }

            // Punch a hole: drop a post-NU5 height's root from the fixture, simulating a
            // peer that omitted it (or a root evicted after failing verification). Earlier
            // fast blocks freeze the frontier, so this height has no real frontier to
            // recompute against.
            let hole = (nu5 + 1) as usize;
            prop_assert!(hole > seed && hole < last, "the hole must be inside the fast range");
            fixture.remove(&(hole as u32));

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            fast.enable_vct_fast_fixture(fixture);

            let mut error_height = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last).then(|| (blocks[i + 1].block.clone(), None));
                match fast.commit_finalized_direct(cv.into(), None, None, next, "vct hole fast") {
                    Ok(_) => {}
                    Err(error) => {
                        // The refusal is the typed, retryable error — not a generic
                        // invalid-block error and not silent corruption.
                        prop_assert!(
                            format!("{error:?}").contains("VctSuppliedRootUnavailable"),
                            "a frozen-frontier hole returns the retryable VctSuppliedRootUnavailable error, got: {error:?}"
                        );
                        error_height = Some(i);
                        break;
                    }
                }
            }

            prop_assert_eq!(error_height, Some(hole), "the commit refuses at the hole height, not before or after");
            // Nothing at or past the hole was persisted: the tip is the last block before
            // the hole, so no corrupt MMR leaf was written.
            prop_assert_eq!(
                fast.db.finalized_tip_height(),
                Some(Height((hole - 1) as u32)),
                "the database tip stays just below the hole — the refused block left state untouched"
            );
    });

    Ok(())
}

/// The frozen-frontier guard must survive a restart. A fast sync interrupted before the
/// checkpoint handoff leaves the stale frozen frontier persisted (fast commits never write
/// per-height trees) with the tip still below the handoff, but the in-memory `frozen` flag
/// is rebuilt from scratch on open. If it came back `false`, the first post-restart height
/// with no supplied root would legacy-recompute against the stale on-disk frontier and
/// corrupt the history MMR — the exact hazard the in-session guard prevents
/// (`vct_frozen_frontier_hole_refuses_instead_of_recomputing`). So `FinalizedState::new`
/// re-derives the flag from the durable fast-sync marker. This reopens the database between
/// freezing and the hole, and asserts that the very first commit of the new session (no
/// prior fast block to re-arm the flag in-session) still refuses with the retryable
/// `VctSuppliedRootUnavailable`, leaves state untouched, and commits once the root arrives.
#[test]
#[allow(clippy::needless_range_loop)] // the loop indexes blocks[i+1] and the fixture by height
fn vct_frozen_frontier_survives_reopen() -> Result<()> {
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
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let handoff_height = nu5 + 3;
            let last = handoff_height as usize;
            prop_assert!(blocks.len() > last, "generated chain unexpectedly short");
            let seed = (heartwood - 1) as usize;

            // Stop the fast sync two blocks below the handoff, so the tip is inside the
            // frozen region and there is room for the hole at `stop + 1` (still below the
            // handoff, where the real frontier would have been written).
            let stop = (handoff_height - 2) as usize;
            let hole = stop + 1;
            prop_assert!(seed < stop && hole < last, "the hole must sit inside the frozen fast range");

            // Legacy golden pass over [0, last]: the per-block fixture for the fast range
            // and the real final frontiers at the handoff (needed to configure fast mode).
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            let mut fixture = std::collections::HashMap::new();
            let mut handoff_trees = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, None, "vct reopen legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(i as u32, (trees.sapling.root(), trees.orchard.root()));
                }
                if i == last {
                    handoff_trees = Some(trees);
                }
            }
            let handoff_trees = handoff_trees.expect("committed the handoff block");

            // A persistent database so the syncing handle can be dropped and reopened by
            // path, modelling a node restart. Archive storage mode (the default): fast sync
            // is the default under checkpoint sync, and a fast-synced database reopens fine
            // in archive mode, exactly as in production.
            let dir = TempDir::new().expect("temp dir");
            let config = Config {
                cache_dir: dir.path().to_path_buf(),
                ephemeral: false,
                ..Config::default()
            };

            // Session 1: a genesis-start fast sync interrupted at `stop`, two blocks below
            // the handoff. The fast commits write the fast-sync marker but no per-height
            // trees, so the on-disk frontier is frozen and the tip is below the handoff.
            {
                let mut fast = FinalizedState::new(&config, &network, #[cfg(feature = "elasticsearch")] false);
                fast.enable_vct_fast_fixture_with_handoff(
                    fixture.clone(),
                    Height(handoff_height),
                    handoff_trees.sapling.clone(),
                    handoff_trees.orchard.clone(),
                    handoff_trees.sprout.clone(),
                );
                for i in 0..=stop {
                    let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                    let next = (i < stop).then(|| (blocks[i + 1].block.clone(), None));
                    fast.commit_finalized_direct(cv.into(), None, None, next, "vct reopen fast")
                        .expect("verified fast commit succeeds");
                }
                prop_assert_eq!(fast.vct_fast_synced_below(), Some(Height(handoff_height)), "the interrupted sync left the fast-sync marker");
                prop_assert_eq!(fast.db.finalized_tip_height(), Some(Height(stop as u32)), "the tip is parked below the handoff");
                // Drop releases the database lock for the reopen below.
            }

            // Session 2 (restart): reopen the same database, then punch a hole at the next
            // height (a peer that omitted it, or a root evicted after failing verification).
            let mut reopened = FinalizedState::new(&config, &network, #[cfg(feature = "elasticsearch")] false);
            prop_assert_eq!(reopened.vct_fast_synced_below(), Some(Height(handoff_height)), "the marker is still durable after reopen");

            let mut holed = fixture.clone();
            holed.remove(&(hole as u32));
            reopened.enable_vct_fast_fixture_with_handoff(
                holed,
                Height(handoff_height),
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
            );

            // The very first commit of the new session is the hole. No fast block has run
            // since the reopen, so the only thing that can arm the guard is the flag seeded
            // from the durable marker. Before the fix it came back `false` and this would
            // legacy-recompute against the stale frontier; now it refuses.
            let cv = CheckpointVerifiedBlock::from(blocks[hole].block.clone());
            let next = Some((blocks[hole + 1].block.clone(), None));
            let error = reopened
                .commit_finalized_direct(cv.into(), None, None, next, "vct reopen hole")
                .expect_err("a frozen-frontier hole must refuse after reopen, not recompute");
            prop_assert!(
                format!("{error:?}").contains("VctSuppliedRootUnavailable"),
                "the reopened committer returns the retryable VctSuppliedRootUnavailable, got: {error:?}"
            );
            prop_assert_eq!(reopened.db.finalized_tip_height(), Some(Height(stop as u32)), "the refused block left the reopened state untouched");

            // Retryable: once a verifiable root for the hole is supplied, the same height
            // commits and the tip advances — the refusal was a stall, not a permanent wedge.
            reopened.enable_vct_fast_fixture_with_handoff(
                fixture.clone(),
                Height(handoff_height),
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
            );
            let cv = CheckpointVerifiedBlock::from(blocks[hole].block.clone());
            let next = Some((blocks[hole + 1].block.clone(), None));
            reopened
                .commit_finalized_direct(cv.into(), None, None, next, "vct reopen refill")
                .expect("the height commits once its root is fetched");
            prop_assert_eq!(reopened.db.finalized_tip_height(), Some(Height(hole as u32)), "the tip advances past the former hole once the root arrives");
    });

    Ok(())
}

/// Verified-commitment-trees checkpoint handoff (merged increments 4+5): a
/// genesis-start fast sync writes the verified final frontier at the handoff
/// height, marks the database fast-synced, guards historical per-height tree reads
/// below the handoff, and leaves the tip treestate (which post-checkpoint semantic
/// verification resumes from) byte-identical to the legacy recompute.
#[test]
#[allow(clippy::needless_range_loop)] // the loops index blocks[i+1] and the fixture by height
fn vct_fast_sync_handoff_marks_database_and_resumes() -> Result<()> {
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
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES)),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last, "generated chain unexpectedly short");
            let handoff = Height(last as u32);

            // The fast range is seeded just below Heartwood, so it is authenticated by
            // the ZIP-221 MMR (the synthetic chain's pre-Heartwood `FinalSaplingRoot`
            // headers are not consistent with the computed trees, so the Sapling-era
            // direct-header path can't be exercised here — that rides with the real
            // synced node). The handoff is at the tip.
            let seed = (heartwood - 1) as usize;

            // Legacy pass over [0, last]: the per-block fixture for the fast range, the
            // golden consensus state, and the real final frontiers at the handoff.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            let mut fixture = std::collections::HashMap::new();
            let mut handoff_trees = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, None, "vct legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(i as u32, (trees.sapling.root(), trees.orchard.root()));
                }
                if i == last {
                    handoff_trees = Some(trees);
                }
            }
            let golden_anchors = legacy.db.vct_anchor_digest();
            let golden_history = legacy.db.history_tree().hash();
            let golden_tip = legacy.db.note_commitment_trees_for_tip();
            let handoff_trees = handoff_trees.expect("committed the handoff block");

            // Fast genesis-start pass over [0, last], supplying the verified frontiers
            // for the handoff at `last`.
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            fast.enable_vct_fast_fixture_with_handoff(
                fixture.clone(),
                handoff,
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
            );
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last).then(|| (blocks[i + 1].block.clone(), None));
                fast.commit_finalized_direct(cv.into(), None, None, next, "vct fast handoff")
                    .expect("verified fast commit succeeds");
            }

            // The database is marked fast-synced at the handoff height.
            prop_assert_eq!(fast.vct_fast_synced_below(), Some(handoff), "fast-sync marker is set to the handoff height");

            // Consensus state (anchor sets + history root) matches the legacy recompute.
            prop_assert_eq!(fast.db.vct_anchor_digest(), golden_anchors, "fast anchors must match legacy");
            prop_assert_eq!(fast.db.history_tree().hash(), golden_history, "fast history must match legacy");

            // The handoff wrote the real frontier at the checkpoint, so the tip
            // treestate that semantic verification resumes from matches legacy.
            let fast_tip = fast.db.note_commitment_trees_for_tip();
            prop_assert_eq!(fast_tip.sapling.root(), golden_tip.sapling.root(), "tip sapling frontier must match legacy");
            prop_assert_eq!(fast_tip.orchard.root(), golden_tip.orchard.root(), "tip orchard frontier must match legacy");
            prop_assert_eq!(fast_tip.sprout.root(), golden_tip.sprout.root(), "tip sprout frontier must match legacy");

            // Historical per-height tree reads below the handoff are unavailable
            // (guarded, no panic), while the handoff height itself is present.
            prop_assert!(fast.db.sapling_tree_by_height(&Height(last as u32 - 1)).is_none(), "below-handoff sapling tree read is guarded");
            prop_assert!(fast.db.orchard_tree_by_height(&Height(last as u32 - 1)).is_none(), "below-handoff orchard tree read is guarded");
            prop_assert!(fast.db.sapling_tree_by_height(&handoff).is_some(), "handoff sapling tree is present");
            prop_assert!(fast.db.orchard_tree_by_height(&handoff).is_some(), "handoff orchard tree is present");

            // The `z_gettreestate` RPC gate predicate matches the read guard: a
            // below-handoff height is unavailable (typed archive-mode error), while the
            // handoff height itself is available.
            prop_assert!(fast.db.fast_synced_tree_unavailable(HashOrHeight::Height(Height(last as u32 - 1))), "RPC gate: below-handoff treestate is unavailable");
            prop_assert!(!fast.db.fast_synced_tree_unavailable(HashOrHeight::Height(handoff)), "RPC gate: handoff treestate is available");

            // Negative: a peer can supply a wrong root exactly at the handoff height,
            // where there is no buffered checkpoint successor to authenticate it. The
            // final embedded frontier still binds the expected root, so the committer
            // must reject and retry instead of panicking or writing a bad handoff.
            let mut bad_handoff_fixture = fixture.clone();
            let bad_handoff_entry = bad_handoff_fixture
                .get_mut(&(last as u32))
                .expect("fixture contains the handoff root");
            prop_assert_ne!(bad_handoff_entry.0, Default::default(), "a post-NU5 handoff block must have a non-empty Sapling root");
            bad_handoff_entry.0 = Default::default();

            let mut bad_handoff = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            bad_handoff.enable_vct_fast_fixture_with_handoff(
                bad_handoff_fixture,
                handoff,
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
            );

            let mut error_height = None;
            let mut handoff_error = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last).then(|| (blocks[i + 1].block.clone(), None));
                match bad_handoff.commit_finalized_direct(cv.into(), None, None, next, "vct bad handoff") {
                    Ok(_) => {}
                    Err(error) => {
                        error_height = Some(i);
                        handoff_error = Some(error);
                        break;
                    }
                }
            }
            prop_assert_eq!(error_height, Some(last), "the bad handoff root is rejected at the handoff height");
            let handoff_error = handoff_error.expect("the bad handoff root failed");
            prop_assert!(
                format!("{handoff_error:?}").contains("VctSuppliedRootUnavailable"),
                "a bad handoff root returns the retryable VctSuppliedRootUnavailable error, got: {handoff_error:?}"
            );
            prop_assert_eq!(
                bad_handoff.db.finalized_tip_height(),
                Some(Height(last as u32 - 1)),
                "the refused handoff block left state untouched"
            );
    });

    Ok(())
}

/// Standalone test isolating the verify-before-commit **dedup**: each header
/// commitment is checked once, not twice.
///
/// - **Skip:** the first fast block runs its own commitment check; the next one
///   is skipped, because the first block's look-ahead already validated it.
/// - **Stale-cache guard:** a cache entry with the right height but the *wrong*
///   hash must not trigger a skip — the guard forces the own check to run, so a
///   stale or mismatched entry can never let an unverified block through.
#[test]
fn vct_dedup_skips_redundant_check_and_guards_stale_cache() -> Result<()> {
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
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0 as usize;

            // Seed just before Heartwood so the fast range creates the history tree,
            // then operate on four consecutive fast blocks. The dedup is era-agnostic;
            // the cross-boundary coverage lives in the proptest above.
            let seed = heartwood - 1;
            let last = seed + 4;
            prop_assert!(blocks.len() > last, "generated chain unexpectedly short");

            // Legacy pass to record the correct per-block roots as the fixture.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            let mut fixture = std::collections::HashMap::new();
            for (i, prepared) in blocks.iter().take(last + 1).enumerate() {
                let cv = CheckpointVerifiedBlock::from(prepared.block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, None, "vct dedup legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(i as u32, (trees.sapling.root(), trees.orchard.root()));
                }
            }

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            fast.enable_vct_fast_fixture(fixture);

            // Commit block `i` with its real successor as the one-block look-ahead.
            let commit = |fast: &mut FinalizedState, i: usize| {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last).then(|| (blocks[i + 1].block.clone(), None));
                fast.commit_finalized_direct(cv.into(), None, None, next, "vct dedup fast")
                    .expect("verified fast commit succeeds");
            };

            // genesis..=seed take the recompute path (no fixture entries), so the dedup
            // never engages here.
            for i in 0..=seed {
                commit(&mut fast, i);
            }
            prop_assert_eq!(fast.vct_prevalidated_count(), 0, "no fast blocks committed yet");

            // First fast block: no cached predecessor, so it runs its own check.
            commit(&mut fast, seed + 1);
            prop_assert_eq!(fast.vct_prevalidated_count(), 0, "the first fast block runs its own commitment check");

            // Second fast block: its predecessor's look-ahead already validated it,
            // so the own check is skipped — the dedup engages.
            commit(&mut fast, seed + 2);
            prop_assert_eq!(fast.vct_prevalidated_count(), 1, "the second fast block skips its redundant own commitment check");

            // Stale-cache guard: overwrite the cache with the correct height but the
            // hash of a *different* block. The next commit must NOT skip.
            let stale_hash = blocks[seed + 1].hash;
            prop_assert_ne!(stale_hash, blocks[seed + 3].hash, "stale hash must differ from the real block");
            fast.vct_prevalidated_next = Some((Height((seed + 3) as u32), stale_hash));
            commit(&mut fast, seed + 3);
            prop_assert_eq!(fast.vct_prevalidated_count(), 1, "a stale cache entry (wrong hash) must not cause a false skip");
    });

    Ok(())
}

/// Increment-3 contract proof: a roots/frontier payload **produced from a database**
/// (the serving read path) can replace the fixture and drives the fast path to
/// byte-identical consensus state.
///
/// Builds an archive/legacy state over a generated valid-commitment chain (crossing
/// Heartwood and NU5), produces the per-block roots and final frontier from that DB
/// via [`commitment_aux::produce_block_roots`] / [`commitment_aux::produce_final_frontiers`],
/// then drives a fresh fast-sync state that consumes the produced payload through a
/// [`commitment_aux::VecRootSource`]. Asserts the fast anchors + history-tree hash are
/// byte-identical to the legacy build, and that the produced final frontier agrees with
/// the legacy tip frontier and the produced root at the handoff height.
///
/// This is coverage the existing equivalence test lacks: there the roots are captured
/// from the committer's inline-returned trees, here they come from the **DB read path**
/// a serving node runs. No networking and no DB-format change.
#[test]
#[allow(clippy::needless_range_loop)] // the loops index blocks[i+1] (the look-ahead) and by height
fn vct_db_produced_payload_round_trips_to_byte_identical_state() -> Result<()> {
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
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last, "generated chain unexpectedly short");
            // Seed below Heartwood so the fast range creates the history tree and
            // crosses the NU5 V1->V2 boundary, matching the equivalence test.
            let seed = (heartwood - 1) as usize;

            // Legacy/archive pass: a real DB with per-height trees, plus the golden state.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            for block in blocks.iter().take(last + 1) {
                let cv = CheckpointVerifiedBlock::from(block.block.clone());
                legacy
                    .commit_finalized_direct(cv.into(), None, None, None, "vct round-trip legacy")
                    .unwrap();
            }
            let golden_anchors = legacy.db.vct_anchor_digest();
            let golden_history = legacy.db.history_tree().hash();

            // Produce the payload from the legacy DB's per-height trees (the serving read path).
            let last_height = Height(last as u32);
            let produced_roots = commitment_aux::produce_block_roots(
                &legacy.db,
                Height((seed + 1) as u32)..=last_height,
            );
            let produced_frontiers = commitment_aux::produce_final_frontiers(&legacy.db, last_height)
                .expect("legacy DB has the tip frontier");

            // The produced final frontier agrees with the legacy tip frontier and with the
            // produced root at the handoff height (the two producer outputs are consistent).
            let handoff = produced_roots.last().expect("produced a non-empty range");
            prop_assert_eq!(produced_frontiers.sapling.root(), handoff.sapling_root, "produced sapling frontier matches the produced root at handoff");
            prop_assert_eq!(produced_frontiers.orchard.root(), handoff.orchard_root, "produced orchard frontier matches the produced root at handoff");
            prop_assert_eq!(produced_frontiers.sapling.root(), legacy.db.sapling_tree_by_height(&last_height).unwrap().root(), "produced sapling frontier matches legacy tip");
            prop_assert_eq!(produced_frontiers.sprout.root(), legacy.db.sprout_tree_for_tip().root(), "produced sprout frontier matches legacy tip");

            // Consume the DB-produced roots in a fresh fast-sync state.
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            fast.enable_vct_fast_source(Box::new(commitment_aux::VecRootSource::from_payload(produced_roots, None)));
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last).then(|| (blocks[i + 1].block.clone(), None));
                fast.commit_finalized_direct(cv.into(), None, None, next, "vct round-trip fast")
                    .expect("verified fast commit from DB-produced roots succeeds");
            }

            prop_assert_eq!(fast.db.vct_anchor_digest(), golden_anchors, "fast anchors from DB-produced roots match legacy");
            prop_assert_eq!(fast.db.history_tree().hash(), golden_history, "fast history from DB-produced roots match legacy");
    });

    Ok(())
}

/// Verified-commitment-trees consumer half of the `tree_aux` peer source (increment 6a):
/// a [`commitment_aux::PeerSource`] **filled incrementally** by its writer handle (as the
/// driver fills it when root ranges arrive from peers) drives the fast path to
/// byte-identical consensus state. Same harness as the DB-produced round-trip, but the
/// produced roots are inserted into the shared cache in two chunks via
/// [`commitment_aux::PeerSourceWriter`] — proving the fillable, driver-facing source is a
/// drop-in for the fixture. (The network transport that fills it is the rest of 6a.)
#[test]
#[allow(clippy::needless_range_loop)] // the loops index blocks[i+1] (the look-ahead) and by height
fn vct_peer_source_filled_incrementally_drives_byte_identical_state() -> Result<()> {
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
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last, "generated chain unexpectedly short");
            let seed = (heartwood - 1) as usize;

            // Legacy/archive pass: a real DB with per-height trees, plus the golden state.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            for block in blocks.iter().take(last + 1) {
                let cv = CheckpointVerifiedBlock::from(block.block.clone());
                legacy
                    .commit_finalized_direct(cv.into(), None, None, None, "vct peer-source legacy")
                    .unwrap();
            }
            let golden_anchors = legacy.db.vct_anchor_digest();
            let golden_history = legacy.db.history_tree().hash();

            // Produce the payload from the legacy DB (the serving read path).
            let produced_roots = commitment_aux::produce_block_roots(
                &legacy.db,
                Height((seed + 1) as u32)..=Height(last as u32),
            );

            // Fill the peer source incrementally via its writer, in two chunks, as the
            // driver would when successive root ranges arrive from a peer.
            let (peer_source, writer) = commitment_aux::PeerSource::new(None);
            let split = produced_roots.len() / 2;
            writer.insert_roots(produced_roots[..split].iter().cloned());
            writer.insert_roots(produced_roots[split..].iter().cloned());

            // Consume the peer-source-supplied roots in a fresh fast-sync state.
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false);
            fast.enable_vct_fast_source(Box::new(peer_source));
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last).then(|| (blocks[i + 1].block.clone(), None));
                fast.commit_finalized_direct(cv.into(), None, None, next, "vct peer-source fast")
                    .expect("verified fast commit from peer-source roots succeeds");
            }

            prop_assert_eq!(fast.db.vct_anchor_digest(), golden_anchors, "fast anchors from peer-source roots match legacy");
            prop_assert_eq!(fast.db.history_tree().hash(), golden_history, "fast history from peer-source roots match legacy");
    });

    Ok(())
}
