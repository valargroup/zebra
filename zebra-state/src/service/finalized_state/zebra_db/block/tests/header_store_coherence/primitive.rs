//! Contract tests for `set_canonical_suffix`, the single owner of zakura
//! header-store chain membership (REORG_PLAN Pillar 1).
//!
//! The coherence harness exercises the primitive through the production
//! writers (range, seed, release); these tests pin the primitive's own
//! contract directly: preconditions reject without staging anything,
//! deletion is total-above-fork across all five column families (including
//! rows stranded above hand-made gaps), an empty replacement truncates, and
//! a pure append stages no deletions (so the post-reorg audit hook does not
//! fire for it).

use zebra_chain::block::Height;

use super::super::super::CanonicalHeaderRow;
use super::super::super::{ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT, ZAKURA_HEADER_HASH_BY_HEIGHT};
use super::super::common::{
    commit_header_range, state_with_genesis_config, write_full_block_header_and_transactions,
};
use super::{
    audit::{audit_store, dump_store},
    fabricate::{fabricate_body, FabHeader, Universe, BRANCH_A, FORK_HEIGHT},
};
use crate::{
    error::{CommitHeaderRangeError, StoreIncoherentError},
    service::finalized_state::{
        disk_db::{DiskWriteBatch, WriteDisk},
        disk_format::block::TransactionLocation,
        RawBytes, ZebraDb,
    },
    Config,
};

fn trunk_state(universe: &Universe) -> ZebraDb {
    let state = state_with_genesis_config(
        &universe.network,
        universe.genesis.clone(),
        Config::ephemeral(),
    );
    let trunk_headers: Vec<_> = universe.trunk[..FORK_HEIGHT as usize]
        .iter()
        .map(|fab| fab.header.clone())
        .collect();
    commit_header_range(&state, universe.genesis.hash(), &trunk_headers);
    state
}

fn rows_of(fabs: &[FabHeader]) -> Vec<CanonicalHeaderRow> {
    fabs.iter()
        .map(|fab| CanonicalHeaderRow {
            header: fab.header.clone(),
            advertised_body_size: None,
            roots: None,
        })
        .collect()
}

fn assert_clean(state: &ZebraDb) {
    let violations = audit_store(state);
    assert!(
        violations.is_empty(),
        "unexpected violations: {violations:?}"
    );
}

/// New rows that do not link to the fork row are rejected before anything is
/// staged.
#[test]
fn rejects_unlinked_new_rows_without_staging() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);
    let original = dump_store(&state);

    // Branch-A rows link at the fork height (50), not at 30.
    let rows = rows_of(&universe.branches[BRANCH_A].headers[..3]);
    let mut batch = DiskWriteBatch::new();
    let error = batch
        .set_canonical_suffix(&state, (Height(30), universe.trunk_at(30).hash), &rows)
        .expect_err("rows do not link to trunk@30");
    assert!(
        matches!(
            error,
            CommitHeaderRangeError::UnlinkedRange { height, .. } if height == Height(31)
        ),
        "expected UnlinkedRange at 31, got {error:?}"
    );
    assert_eq!(batch.zakura_suffix_replaced_rows(), 0);

    state.write_batch(batch).expect("empty batch writes");
    assert_eq!(dump_store(&state), original, "a rejection stages nothing");
}

/// A fork hash that is not the stored row at the fork height is rejected:
/// the caller's fork decision must describe the store being mutated.
#[test]
fn rejects_stale_fork_row() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);
    let original = dump_store(&state);

    // These rows link to branch-A's fourth row, which is not stored at 30.
    let foreign_fork = &universe.branches[BRANCH_A].headers[3];
    let rows = rows_of(&universe.branches[BRANCH_A].headers[4..6]);
    let mut batch = DiskWriteBatch::new();
    let error = batch
        .set_canonical_suffix(&state, (Height(30), foreign_fork.hash), &rows)
        .expect_err("the stored row at 30 is the trunk's");
    assert!(
        matches!(
            error,
            CommitHeaderRangeError::StoreIncoherent(StoreIncoherentError::BijectionMismatch {
                height,
                ..
            }) if height == Height(30)
        ),
        "expected BijectionMismatch at the fork row, got {error:?}"
    );
    assert_eq!(batch.zakura_suffix_replaced_rows(), 0);
    assert_eq!(dump_store(&state), original);
}

/// A fork below the finalized tip is rejected: committed heights are
/// immutable through the primitive.
#[test]
fn rejects_fork_below_finalized_tip() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);

    // Commit the body of trunk height 1: the finalized tip moves to 1.
    write_full_block_header_and_transactions(&state, fabricate_body(&universe.trunk[0]));
    assert_eq!(state.finalized_tip_height(), Some(Height(1)));

    let rows = rows_of(&universe.trunk[..2]);
    let mut batch = DiskWriteBatch::new();
    let error = batch
        .set_canonical_suffix(&state, (Height(0), universe.genesis.hash()), &rows)
        .expect_err("the fork is below the finalized tip");
    assert!(
        matches!(
            error,
            CommitHeaderRangeError::ImmutableConflict { height } if height == Height(1)
        ),
        "expected ImmutableConflict, got {error:?}"
    );
    assert_eq!(batch.zakura_suffix_replaced_rows(), 0);
}

/// The committed-body safety interlock: a body row above the fork aborts the
/// replacement before anything is staged, even when the finalized-tip bound
/// cannot see it.
#[test]
fn body_above_fork_aborts_loudly() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);

    // Hand-plant a body marker above the finalized tip — a state the
    // orchestration must never produce.
    let tx_by_loc = state.db.cf_handle("tx_by_loc").unwrap();
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(
        &tx_by_loc,
        TransactionLocation::min_for_height(Height(45)),
        RawBytes::new_raw_bytes(vec![0u8]),
    );
    state.db.write(batch).expect("raw insert writes");

    let rows = rows_of(&universe.trunk[30..35]);
    let mut batch = DiskWriteBatch::new();
    let error = batch
        .set_canonical_suffix(&state, (Height(30), universe.trunk_at(30).hash), &rows)
        .expect_err("a body above the fork must abort the replacement");
    assert!(
        matches!(
            error,
            CommitHeaderRangeError::ConflictingFullBlockHeader { height } if height == Height(45)
        ),
        "expected ConflictingFullBlockHeader at 45, got {error:?}"
    );
    assert_eq!(batch.zakura_suffix_replaced_rows(), 0);
}

/// Deletion is total-above-fork: rows stranded above a hand-made gap and
/// stray rows in single column families are deleted along with the linked
/// suffix, in all five column families.
#[test]
fn truncation_is_total_above_fork_including_strays() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);

    // A gap at 40 strands rows 41..=50; a stray body-size row sits at 60,
    // above the zakura tip.
    let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
    let body_size_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
        .unwrap();
    let mut batch = DiskWriteBatch::new();
    batch.zs_delete(&hash_cf, Height(40));
    batch.zs_insert(
        &body_size_cf,
        Height(60),
        super::super::super::test_body_size(7),
    );
    state.db.write(batch).expect("raw writes succeed");

    let rows = rows_of(&universe.trunk[30..35]);
    let mut batch = DiskWriteBatch::new();
    batch
        .set_canonical_suffix(&state, (Height(30), universe.trunk_at(30).hash), &rows)
        .expect("the replacement is well-formed");
    assert!(batch.zakura_suffix_replaced_rows() > 0);
    state.write_batch(batch).expect("replacement batch writes");

    assert_eq!(
        state.best_header_tip(),
        Some((Height(35), universe.trunk_at(35).hash))
    );
    let dump = dump_store(&state);
    assert_eq!(dump.hashes.keys().next_back(), Some(&Height(35)));
    assert!(!dump.body_sizes.contains_key(&Height(60)));
    assert_clean(&state);
}

/// An empty replacement truncates the store to the fork point.
#[test]
fn empty_replacement_truncates_to_fork() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);

    let mut batch = DiskWriteBatch::new();
    batch
        .set_canonical_suffix(&state, (Height(40), universe.trunk_at(40).hash), &[])
        .expect("an empty replacement is a truncation");
    assert!(batch.zakura_suffix_replaced_rows() > 0);
    state.write_batch(batch).expect("truncation batch writes");

    assert_eq!(
        state.best_header_tip(),
        Some((Height(40), universe.trunk_at(40).hash))
    );
    assert_clean(&state);
}

/// A pure append deletes nothing, so it does not count as a reorg (the
/// post-reorg audit hook stays quiet for the steady-state extension path).
#[test]
fn pure_append_replaces_nothing() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);

    // Branch A forks at the trunk tip, so its rows extend the stored chain.
    let rows = rows_of(&universe.branches[BRANCH_A].headers[..3]);
    let mut batch = DiskWriteBatch::new();
    batch
        .set_canonical_suffix(
            &state,
            (Height(FORK_HEIGHT), universe.trunk_at(FORK_HEIGHT).hash),
            &rows,
        )
        .expect("an extension at the tip is well-formed");
    assert_eq!(
        batch.zakura_suffix_replaced_rows(),
        0,
        "a pure append is not a reorg"
    );
    state.write_batch(batch).expect("append batch writes");

    assert_eq!(
        state.best_header_tip(),
        Some((
            Height(FORK_HEIGHT + 3),
            universe.branches[BRANCH_A].headers[2].hash
        ))
    );
    assert_clean(&state);
}
