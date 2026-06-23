//! Tests for the history-tree rebuild format upgrade.
//!
//! The Ironwood `zcash_history` bump grew `MAX_ENTRY_SIZE`, which made history-tree column-family
//! entries written by older Zebra versions unreadable (a bincode `UnexpectedEof` panic). The
//! [`rebuild_history_tree`](crate::service::finalized_state::disk_format::upgrade::rebuild_history_tree)
//! upgrade repairs this by rebuilding the tip tree from blocks and the per-height note commitment
//! tree roots and rewriting it in the current format.
//!
//! This test can't open a real pre-Ironwood on-disk database here (that runs on a droplet with a
//! snapshot), so it verifies the two load-bearing properties locally:
//!
//! 1. The rebuild reproduces the *exact same* chain-history MMR root that was stored — this is the
//!    consensus-correctness guarantee.
//! 2. The rebuild detects that a database already written in the current format does not need
//!    repair, so it never touches a healthy database.

use std::env;

use crossbeam_channel::bounded;

use zebra_chain::{
    block::Height,
    parameters::{
        testnet::{ConfiguredActivationHeights, Parameters as TestnetParameters},
        Network, NetworkUpgrade,
    },
    LedgerState,
};
use zebra_test::prelude::*;

use crate::{
    config::Config,
    service::{
        arbitrary::PreparedChain,
        finalized_state::{
            disk_format::upgrade::{rebuild_history_tree, DiskFormatUpgrade},
            CheckpointVerifiedBlock, FinalizedState,
        },
    },
    SemanticallyVerifiedBlock,
};

/// Number of proptest cases. Each case syncs an on-disk database, so the default is low.
const DEFAULT_REBUILD_PROPTEST_CASES: u32 = 1;

fn proptest_cases() -> u32 {
    env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_REBUILD_PROPTEST_CASES)
}

/// A configured testnet with low activation heights, so a short generated chain reaches Heartwood
/// (height 5) and stores a non-empty history tree.
fn rebuild_test_network() -> Network {
    TestnetParameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(2),
            sapling: Some(3),
            blossom: Some(4),
            heartwood: Some(5),
            canopy: Some(6),
            nu5: Some(7),
            nu6: Some(8),
            nu6_1: Some(9),
            nu6_2: Some(10),
            nu6_3: Some(11),
            nu7: Some(12),
        })
        .expect("configured activation heights are valid")
        .extend_funding_streams()
        .to_network()
        .expect("configured network is valid")
}

/// Syncs a fresh ephemeral finalized state by committing `blocks` in order, returning the live
/// state so its database can be queried.
///
/// Format upgrades are skipped so the background format-change thread (which runs concurrently with
/// commits and would race the genesis-roots format check on a partially-synced database) is never
/// spawned. This isolates the test to the history-tree rebuild logic, which it drives directly.
fn sync_to(network: &Network, blocks: &[SemanticallyVerifiedBlock]) -> FinalizedState {
    let mut state = FinalizedState::new_with_debug(
        &Config::ephemeral(),
        network,
        true,
        #[cfg(feature = "elasticsearch")]
        false,
        false,
    );

    for block in blocks {
        let checkpoint_verified = CheckpointVerifiedBlock::from(block.block.clone());
        state
            .commit_finalized_direct(
                checkpoint_verified.into(),
                None,
                "rebuild history tree test",
            )
            .expect("committing a generated block to a fresh state succeeds");
    }

    state
}

#[test]
fn rebuild_reproduces_stored_history_root() -> Result<()> {
    let _init_guard = zebra_test::init();

    let network = rebuild_test_network();
    // Generate a chain from genesis with valid commitments, so the post-Heartwood blocks carry
    // valid chain-history-root commitments and can be committed to a finalized state.
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), NetworkUpgrade::Nu5, Some(2), true);

    proptest!(
        ProptestConfig::with_cases(proptest_cases()),
        |((chain, _count, network, _history_tree) in PreparedChain::default()
            .with_ledger_strategy(ledger_strategy)
            .with_valid_commitments()
            .no_shrink())| {
            let synced: Vec<SemanticallyVerifiedBlock> = chain.iter().cloned().collect();
            // Require enough blocks to be a few past the Heartwood activation height (5), so a
            // non-empty history tree with more than one entry is stored.
            prop_assume!(synced.len() > 8);

            let state = sync_to(&network, &synced);
            let db = &state.db;

            // The stored tip history root is the ground truth a fresh sync produced.
            let stored_root = db.history_tree().hash();
            prop_assert!(
                stored_root.is_some(),
                "a Heartwood-onward chain should store a non-empty history tree",
            );

            // A freshly synced database is already in the current format, so the upgrade must detect
            // that no rebuild is needed.
            prop_assert!(
                !rebuild_history_tree::needs_rebuild(db),
                "a freshly synced database should already be in the current history tree format",
            );

            // Running the upgrade rebuilds the tip tree from blocks and note commitment roots. The
            // rebuilt tree must produce the identical MMR root, which is the consensus-correctness
            // guarantee. (The run is exercised here even though no rewrite is strictly needed,
            // confirming it never reads or corrupts a current-format entry.)
            let (_never_cancel_handle, never_cancel_receiver) = bounded(1);
            rebuild_history_tree::Upgrade
                .run(
                    db.finalized_tip_height()
                        .expect("synced database has a finalized tip"),
                    db,
                    &never_cancel_receiver,
                )
                .expect("history tree rebuild upgrade should not be cancelled");

            let rebuilt_root = db.history_tree().hash();

            prop_assert_eq!(
                rebuilt_root,
                stored_root,
                "rebuilt history tree root must match the originally stored root",
            );

            // The upgrade's own validity check must pass after running.
            prop_assert!(
                rebuild_history_tree::quick_check(db).is_ok(),
                "history tree should be readable in the current format after the rebuild",
            );
        }
    );

    Ok(())
}

/// An empty database has no history tree entry, so the upgrade detects nothing to rebuild and the
/// validity check passes.
#[test]
fn rebuild_is_noop_on_empty_database() -> Result<()> {
    let _init_guard = zebra_test::init();

    let network = rebuild_test_network();

    let state = sync_to(&network, &[]);
    let db = &state.db;

    assert!(
        !rebuild_history_tree::needs_rebuild(db),
        "an empty database has no history tree entry to rebuild",
    );

    let (_never_cancel_handle, never_cancel_receiver) = bounded(1);
    // An empty database has no finalized tip; the upgrade framework skips empty databases before
    // calling `run`, but `run` is exercised directly here using the genesis height as a stand-in to
    // confirm it does nothing when there is no entry.
    rebuild_history_tree::Upgrade
        .run(Height(0), db, &never_cancel_receiver)
        .expect("history tree rebuild upgrade should not be cancelled");

    assert!(
        rebuild_history_tree::quick_check(db).is_ok(),
        "an empty database passes the history tree validity check",
    );

    Ok(())
}
