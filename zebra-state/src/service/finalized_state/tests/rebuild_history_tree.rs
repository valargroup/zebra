//! Tests for the history-tree rebuild format upgrade.
//!
//! The Ironwood `zcash_history` bump grew `MAX_ENTRY_SIZE`, which made history-tree column-family
//! entries written by older Zebra versions unreadable (a bincode `UnexpectedEof` panic). The
//! [`rebuild_history_tree`](crate::service::finalized_state::disk_format::upgrade::rebuild_history_tree)
//! upgrade repairs this by rebuilding the tip tree from blocks and the per-height note commitment
//! tree roots and rewriting it in the current format.
//!
//! This test can't open a real pre-Ironwood on-disk database here (that runs on a droplet with a
//! snapshot), so it reproduces the failure mode locally and verifies the load-bearing properties:
//!
//! 1. The rebuild reproduces the *exact same* chain-history MMR root that was stored — this is the
//!    consensus-correctness guarantee. This is checked both by calling the rebuild directly
//!    (bypassing the up-front "needs rebuild?" check), and by corrupting the stored entry into an
//!    unreadable old-format blob and driving the real synchronous repair path end to end.
//! 2. The rebuild detects that a database already written in the current format does not need
//!    repair, so it never touches a healthy database (including an empty one).
//! 3. A database that needs a rebuild but is missing the historical data the rebuild reads (a
//!    database pruned before the Ironwood bump) fails with a clear, explained error rather than an
//!    opaque panic.

use std::env;

use bincode::Options as _;

use zebra_chain::{
    block::Height,
    parameters::{
        testnet::{ConfiguredActivationHeights, Parameters as TestnetParameters},
        Network, NetworkUpgrade,
    },
    primitives::zcash_history::MAX_ENTRY_SIZE,
    LedgerState,
};
use zebra_test::prelude::*;

use crate::{
    config::Config,
    service::{
        arbitrary::PreparedChain,
        finalized_state::{
            disk_format::{
                chain::{
                    encode_history_tree_parts_at_old_width,
                    reencode_old_format_history_tree_parts, HistoryTreeParts,
                },
                upgrade::rebuild_history_tree,
                FromDisk, IntoDisk, RawBytes,
            },
            CheckpointVerifiedBlock, DiskWriteBatch, FinalizedState,
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

/// Overwrites the stored tip history-tree entry with an unreadable, old-format-style blob.
///
/// Reproduces the on-disk state of a pre-Ironwood database: the entry exists but can no longer be
/// deserialized in the current `Entry` format. We do this by truncating the real current-format
/// bytes, which makes the bincode reader hit end-of-input partway through an `Entry` — exactly the
/// `UnexpectedEof` failure the larger Ironwood `MAX_ENTRY_SIZE` produces when it reads a smaller
/// stored entry.
fn corrupt_tip_history_tree_to_old_format(db: &crate::service::finalized_state::ZebraDb) {
    let raw_entry = db
        .raw_history_tree_value_cf()
        .zs_get(&())
        .expect("a synced post-Heartwood database has a stored tip history tree entry");

    let mut truncated = raw_entry.raw_bytes().clone();
    assert!(
        !truncated.is_empty(),
        "the stored history tree entry should have a non-empty serialization to truncate",
    );
    // Drop the final byte so the reader runs out of input mid-entry.
    truncated.pop();

    let mut batch = DiskWriteBatch::new();
    let _ = db
        .raw_history_tree_value_cf()
        .with_batch_for_writing(&mut batch)
        .zs_insert(&(), &RawBytes::new_raw_bytes(truncated));
    db.write_batch(batch)
        .expect("writing a synthetic old-format history tree entry succeeds");

    assert!(
        rebuild_history_tree::needs_rebuild(db),
        "the corrupted entry must be detected as needing a rebuild",
    );
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

            let tip_height = db
                .finalized_tip_height()
                .expect("synced database has a finalized tip");

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

            // Property 1, directly: rebuilding from blocks and note commitment roots (exercising the
            // activation-height selection and the push loop) reproduces the identical MMR root. This
            // bypasses the up-front "needs rebuild?" check so the rebuild always runs.
            let rebuilt = rebuild_history_tree::rebuild_tip_history_tree(db, &network, tip_height)
                .expect("rebuild from a fully synced database should not be missing any data")
                .expect("a Heartwood-onward tip should rebuild a non-empty history tree");
            prop_assert_eq!(
                rebuilt.hash(),
                stored_root,
                "the directly rebuilt history tree root must match the originally stored root",
            );

            // Property 1, end to end: corrupt the stored entry into an unreadable old-format blob,
            // then run the real synchronous repair path. It must rewrite the entry so it reads back
            // in the current format, with the same root, and pass the upgrade's validity check.
            corrupt_tip_history_tree_to_old_format(db);

            rebuild_history_tree::rebuild_tip_history_tree_if_needed(db, tip_height)
                .expect("repairing a fully synced database should not be missing any data");

            prop_assert!(
                !rebuild_history_tree::needs_rebuild(db),
                "the entry must be readable in the current format after the repair",
            );
            prop_assert!(
                rebuild_history_tree::quick_check(db).is_ok(),
                "history tree should pass its validity check after the repair",
            );
            prop_assert_eq!(
                db.history_tree().hash(),
                stored_root,
                "the repaired history tree root must match the originally stored root",
            );
        }
    );

    Ok(())
}

/// An empty database has no history tree entry, so the upgrade detects nothing to rebuild and the
/// repair is a no-op.
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

    // An empty database has no finalized tip; the open path skips the synchronous repair when there
    // is no tip, but the repair is exercised directly here using the genesis height as a stand-in to
    // confirm it does nothing when there is no entry.
    rebuild_history_tree::rebuild_tip_history_tree_if_needed(db, Height(0))
        .expect("an empty database needs no rebuild");

    assert!(
        rebuild_history_tree::quick_check(db).is_ok(),
        "an empty database passes the history tree validity check",
    );

    Ok(())
}

/// A database that needs a rebuild but is missing the historical blocks the rebuild reads (a
/// database pruned before the Ironwood bump) fails with a clear, explained error rather than an
/// opaque `.expect()` panic.
#[test]
fn rebuild_fails_clearly_on_pruned_old_format_database() -> Result<()> {
    let _init_guard = zebra_test::init();

    let network = rebuild_test_network();
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), NetworkUpgrade::Nu5, Some(2), true);

    proptest!(
        ProptestConfig::with_cases(proptest_cases()),
        |((chain, _count, network, _history_tree) in PreparedChain::default()
            .with_ledger_strategy(ledger_strategy)
            .with_valid_commitments()
            .no_shrink())| {
            let synced: Vec<SemanticallyVerifiedBlock> = chain.iter().cloned().collect();
            prop_assume!(synced.len() > 8);

            let state = sync_to(&network, &synced);
            let db = &state.db;

            let tip_height = db
                .finalized_tip_height()
                .expect("synced database has a finalized tip");

            // Make the tip entry need a rebuild, then delete a block the rebuild requires. The
            // history tree resets at the current network upgrade's activation height, so deleting a
            // block at or after that height removes data the rebuild reads. Heartwood activates at
            // height 5 and the post-NU5 tip's history window starts at the most recent upgrade
            // activation, so the block just below the tip is always within the rebuild range.
            corrupt_tip_history_tree_to_old_format(db);

            let missing_height = Height(tip_height.0 - 1);
            let mut batch = DiskWriteBatch::new();
            batch.delete_block_header(db, missing_height);
            db.write_batch(batch)
                .expect("deleting a block header to simulate a pruned database succeeds");

            let result = rebuild_history_tree::rebuild_tip_history_tree_if_needed(db, tip_height);

            prop_assert!(
                matches!(
                    result,
                    Err(rebuild_history_tree::RebuildError::MissingData { .. })
                ),
                "a pruned old-format database must fail the rebuild with a clear MissingData error, \
                 got: {result:?}",
            );
        }
    );

    Ok(())
}


/// A configured testnet whose history tree stays in the *pre-Ironwood* (`V1`/`V2`) entry format for
/// any reachable tip: Heartwood (and the Orchard-era upgrades) activate early, but Nu6.3/Nu7 — the
/// first Ironwood (`V3`) upgrades — are pushed far out of reach of a short generated chain. A tip in
/// this network therefore stores `peaks` whose meaningful bytes fit in the old 253-byte width, which
/// is exactly the on-disk shape a pre-Ironwood Zebra produced and the shape the in-place re-encode
/// repairs.
fn pre_ironwood_test_network() -> Network {
    TestnetParameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(2),
            sapling: Some(3),
            blossom: Some(4),
            heartwood: Some(5),
            canopy: Some(6),
            nu5: Some(7),
            // Keep every Ironwood-capable upgrade out of reach so the tip tree holds only V1/V2
            // entries, which are the entries the pruned-database in-place re-encode is for.
            nu6: Some(10_000),
            nu6_1: Some(10_001),
            nu6_2: Some(10_002),
            nu6_3: Some(10_003),
            nu7: Some(10_004),
        })
        .expect("configured activation heights are valid")
        .extend_funding_streams()
        .to_network()
        .expect("configured network is valid")
}

/// Regression test for the in-place re-encode (PR #242): re-encoding a genuine pre-Ironwood
/// history-tree blob into the current `Entry` width is the *exact inverse* of the width change.
///
/// This is the round-trip the reviewers asked for before un-drafting. It builds a real
/// current-format [`HistoryTreeParts`] from a synced regtest tip, re-emits its peaks at the old
/// 253-byte width to synthesize a *genuine* old-format blob (not a truncated current one — see
/// [`encode_history_tree_parts_at_old_width`], which refuses to drop any non-zero byte), confirms
/// that blob is unreadable in the current format (i.e. it reproduces the Ironwood `UnexpectedEof`
/// failure), then runs [`reencode_old_format_history_tree_parts`] and asserts the output is
/// byte-for-byte identical to the original current-format bytes — and hence yields the same MMR root.
#[test]
fn reencode_round_trip_restores_current_format_bytes() -> Result<()> {
    let _init_guard = zebra_test::init();

    let network = pre_ironwood_test_network();
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), NetworkUpgrade::Nu5, Some(2), true);

    proptest!(
        ProptestConfig::with_cases(proptest_cases()),
        |((chain, _count, network, _history_tree) in PreparedChain::default()
            .with_ledger_strategy(ledger_strategy)
            .with_valid_commitments()
            .no_shrink())| {
            let synced: Vec<SemanticallyVerifiedBlock> = chain.iter().cloned().collect();
            // A few blocks past Heartwood (height 5), so the tip stores a non-empty history tree
            // with more than one peak, while staying well below the Ironwood activation heights.
            prop_assume!(synced.len() > 8);

            let state = sync_to(&network, &synced);
            let db = &state.db;

            let stored_root = db.history_tree().hash();
            prop_assert!(
                stored_root.is_some(),
                "a Heartwood-onward chain should store a non-empty history tree",
            );

            // The genuine current-format on-disk bytes for the tip tree: this is the ground truth
            // the re-encode must reproduce exactly.
            let current_bytes = db
                .raw_history_tree_value_cf()
                .zs_get(&())
                .expect("a synced post-Heartwood database has a stored tip history tree entry")
                .raw_bytes()
                .clone();

            // Reconstruct the typed parts from those bytes. `as_bytes()` round-trips them unchanged,
            // confirming `current_bytes` is the canonical current-format serialization.
            let parts = HistoryTreeParts::from_bytes(&current_bytes);
            prop_assert_eq!(
                &parts.as_bytes(),
                &current_bytes,
                "the stored bytes must be the canonical current-format serialization",
            );

            // Synthesize a genuine pre-Ironwood blob by re-emitting the same peaks at the old width.
            // `None` here would mean the tip tree holds V3 data that does not fit the old width,
            // which `pre_ironwood_test_network` is constructed to avoid.
            let old_blob = encode_history_tree_parts_at_old_width(&parts)
                .expect("a pre-Ironwood tip tree narrows to the old width without losing any data");

            // The synthetic blob must really be the smaller, old layout: shorter than the current
            // bytes (each of N peaks is 73 bytes narrower) and — crucially — unreadable in the
            // current format, which is exactly the `Io(UnexpectedEof)` failure the Ironwood
            // `MAX_ENTRY_SIZE` bump introduced for old databases.
            prop_assert!(
                old_blob.len() < current_bytes.len(),
                "the old-format blob must be smaller than the current-format blob",
            );
            prop_assert!(
                bincode::DefaultOptions::new()
                    .deserialize::<HistoryTreeParts>(&old_blob)
                    .is_err(),
                "the synthesized old-format blob must be unreadable in the current format, \
                 reproducing the failure the re-encode repairs",
            );

            // The fix under test: re-encode the old blob into the current width.
            let reencoded = reencode_old_format_history_tree_parts(&old_blob)
                .expect("a genuine old-format blob must re-encode into the current format");

            // (a) It deserializes as a current `HistoryTreeParts` ...
            prop_assert!(
                bincode::DefaultOptions::new()
                    .deserialize::<HistoryTreeParts>(&reencoded)
                    .is_ok(),
                "the re-encoded blob must be readable in the current format",
            );
            // (b) ... and equals the original current-format bytes exactly. This proves the
            // re-encode is the precise inverse of the width change: same peaks, same `size`, same
            // `current_height`, only the per-entry zero padding restored.
            prop_assert_eq!(
                &reencoded,
                &current_bytes,
                "re-encoding the old blob must reproduce the original current-format bytes exactly",
            );

            // And, as the consensus-level corollary, the MMR root is unchanged.
            let reencoded_root = HistoryTreeParts::from_bytes(&reencoded)
                .with_network(&network)
                .expect("the re-encoded parts must rebuild a valid history tree")
                .hash();
            prop_assert_eq!(
                Some(reencoded_root),
                stored_root,
                "the re-encoded history tree must have the same MMR root as the stored one",
            );
        }
    );

    Ok(())
}

/// End-to-end regression test for the pruned-database fallback (PR #242): a database with a genuine
/// old-format tip entry whose rebuild source blocks have been pruned is repaired *in place* by the
/// re-encode fallback, leaving the history root unchanged.
///
/// This is the success counterpart to [`rebuild_fails_clearly_on_pruned_old_format_database`]. That
/// test predates the fallback and asserts the from-blocks rebuild fails with `MissingData` when the
/// source blocks are gone; here the same pruned-and-old-format database instead *succeeds*, because
/// the `RebuildError::MissingData` arm now re-encodes the still-present old-format blob without
/// reading any block.
#[test]
fn pruned_old_format_database_repairs_in_place_via_reencode_fallback() -> Result<()> {
    let _init_guard = zebra_test::init();

    let network = pre_ironwood_test_network();
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), NetworkUpgrade::Nu5, Some(2), true);

    proptest!(
        ProptestConfig::with_cases(proptest_cases()),
        |((chain, _count, network, _history_tree) in PreparedChain::default()
            .with_ledger_strategy(ledger_strategy)
            .with_valid_commitments()
            .no_shrink())| {
            let synced: Vec<SemanticallyVerifiedBlock> = chain.iter().cloned().collect();
            prop_assume!(synced.len() > 8);

            let state = sync_to(&network, &synced);
            let db = &state.db;

            let tip_height = db
                .finalized_tip_height()
                .expect("synced database has a finalized tip");
            let stored_root = db.history_tree().hash();
            prop_assert!(
                stored_root.is_some(),
                "a Heartwood-onward chain should store a non-empty history tree",
            );

            // Overwrite the tip entry with a genuine old-format blob (the exact bytes a pre-Ironwood
            // Zebra wrote), so the entry needs a rebuild *and* is re-encodable in place.
            let parts = HistoryTreeParts::from_bytes(
                db.raw_history_tree_value_cf()
                    .zs_get(&())
                    .expect("a synced post-Heartwood database has a stored tip history tree entry")
                    .raw_bytes(),
            );
            let old_blob = encode_history_tree_parts_at_old_width(&parts)
                .expect("a pre-Ironwood tip tree narrows to the old width without losing any data");

            let mut batch = DiskWriteBatch::new();
            let _ = db
                .raw_history_tree_value_cf()
                .with_batch_for_writing(&mut batch)
                .zs_insert(&(), &RawBytes::new_raw_bytes(old_blob));
            db.write_batch(batch)
                .expect("writing a genuine old-format history tree entry succeeds");

            prop_assert!(
                rebuild_history_tree::needs_rebuild(db),
                "the old-format entry must be detected as needing a rebuild",
            );

            // Prune a block the from-blocks rebuild reads, so `rebuild_tip_history_tree` reports
            // `MissingData` and the in-place re-encode fallback is exercised. (The history tree
            // window starts at the current upgrade's activation height, so the block just below the
            // tip is always within the rebuild range — the same block the failure test deletes.)
            let missing_height = Height(tip_height.0 - 1);
            let mut batch = DiskWriteBatch::new();
            batch.delete_block_header(db, missing_height);
            db.write_batch(batch)
                .expect("deleting a block header to simulate a pruned database succeeds");

            // The repair path must now SUCCEED via the fallback, even though the from-blocks rebuild
            // cannot run.
            rebuild_history_tree::rebuild_tip_history_tree_if_needed(db, tip_height).expect(
                "a pruned old-format database must be repaired in place by the re-encode fallback",
            );

            prop_assert!(
                !rebuild_history_tree::needs_rebuild(db),
                "the entry must be readable in the current format after the in-place re-encode",
            );
            prop_assert!(
                rebuild_history_tree::quick_check(db).is_ok(),
                "the history tree must pass its validity check after the in-place re-encode",
            );
            prop_assert_eq!(
                db.history_tree().hash(),
                stored_root,
                "the in-place re-encoded history tree root must match the originally stored root",
            );
        }
    );

    Ok(())
}

/// Pins the pre-Ironwood entry width (`OLD_MAX_ENTRY_SIZE = 253` in
/// [`crate::service::finalized_state::disk_format::chain`]) to the current width minus the Ironwood
/// (`V3`) node-data delta.
///
/// `chain.rs` also enforces this at compile time with a `const` assertion that references the private
/// `OLD_MAX_ENTRY_SIZE`; this runtime test documents the same relationship against the public
/// `zcash_history::MAX_ENTRY_SIZE`, so a future width change trips a named, greppable regression test
/// here in addition to the compile-time guard.
#[test]
fn old_max_entry_size_tracks_current_width_minus_v3_delta() {
    // The literal pre-Ironwood width. It is `pub(crate)`-private to `chain.rs`, so it is duplicated
    // here; the compile-time `const _` assertion in `chain.rs` is what actually ties the two
    // together, and this test guards the arithmetic against the public current width.
    const OLD_MAX_ENTRY_SIZE: usize = 253;
    // V3 added two 32-byte Ironwood tree roots and a 9-byte compact tx count over the V2 layout.
    const V3_NODE_DATA_DELTA: usize = 32 + 32 + 9;

    assert_eq!(
        MAX_ENTRY_SIZE, 326,
        "the current Ironwood-capable entry width is expected to be 326 bytes",
    );
    assert_eq!(V3_NODE_DATA_DELTA, 73, "the V3 node-data delta is 73 bytes");
    assert_eq!(
        MAX_ENTRY_SIZE - OLD_MAX_ENTRY_SIZE,
        V3_NODE_DATA_DELTA,
        "OLD_MAX_ENTRY_SIZE must be the current MAX_ENTRY_SIZE minus the V3 node-data delta",
    );
}
