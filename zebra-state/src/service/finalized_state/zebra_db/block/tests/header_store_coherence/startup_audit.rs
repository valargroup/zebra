//! Startup-audit self-repair: a hand-corrupted zakura header store heals when
//! the database is (re)opened (REORG_PLAN Pillar 3, startup half).
//!
//! The write path refuses to create incoherent stores (the Phase-1.5 guards)
//! and the consensus readers refuse to consume them (Pillar 2), so these tests
//! corrupt the column families directly — the same hand-made corruption
//! technique as the audit's and read-path tests — and assert that:
//!
//! - the startup audit finds the violation and truncates the store to the
//!   last coherent height in all five column families, leaving a store the
//!   full coherence audit passes and header sync can re-download onto;
//! - the repair happens on the real startup path (`ZebraDb::new` at reopen)
//!   and persists: a second reopen finds a coherent store and changes nothing;
//! - repair is minimal where truncation is not needed (an orphaned
//!   reverse-index row, stale rows at committed heights);
//! - a store that failed the Pillar-2 linkage-verified reads passes them
//!   after the heal; and
//! - the `state.zakura.header_store.incoherent` metric is emitted exactly
//!   when a repair runs.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use zebra_chain::block::Height;

use super::super::super::startup_audit::ZakuraStoreViolation;
use super::super::super::{
    test_body_size, ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT, ZAKURA_HEADER_BY_HEIGHT,
    ZAKURA_HEADER_HASH_BY_HEIGHT, ZAKURA_HEADER_HEIGHT_BY_HASH,
};
use super::super::common::{
    commit_header_range, persistent_config, persistent_state, state_with_genesis_config,
    write_full_block_header_and_transactions,
};
use super::{
    audit::{audit_store, dump_store, StoreDump},
    fabricate::{fabricate_body, Universe, BRANCH_A, FORK_HEIGHT},
};
use crate::{
    error::{CommitHeaderRangeError, StoreIncoherentError},
    service::finalized_state::{
        disk_db::{DiskWriteBatch, WriteDisk},
        disk_format::shielded::CommitmentRootsByHeight,
        ZebraDb, COMMITMENT_ROOTS_BY_HEIGHT,
    },
    Config,
};

/// A store holding genesis plus the trunk up to the fork height, built through
/// the production write path, on the given (persistent or ephemeral) config.
fn trunk_state(universe: &Universe, config: Config) -> ZebraDb {
    let state = state_with_genesis_config(&universe.network, universe.genesis.clone(), config);
    let trunk_headers: Vec<_> = universe.trunk[..FORK_HEIGHT as usize]
        .iter()
        .map(|fab| fab.header.clone())
        .collect();
    commit_header_range(&state, universe.genesis.hash(), &trunk_headers);
    state
}

/// Closes `state` and reopens the same database, running the startup audit.
fn reopen(state: ZebraDb, config: &Config, universe: &Universe) -> ZebraDb {
    let mut state = state;
    state.shutdown(true);
    drop(state);
    persistent_state(config, &universe.network)
}

/// The expected store after truncating everything above `last` (the original
/// coherent dump with all five column families filtered to `..= last`).
fn truncated(dump: &StoreDump, last: Height) -> StoreDump {
    StoreDump {
        headers: dump
            .headers
            .iter()
            .filter(|(height, _)| **height <= last)
            .map(|(height, header)| (*height, header.clone()))
            .collect(),
        hashes: dump
            .hashes
            .iter()
            .filter(|(height, _)| **height <= last)
            .map(|(height, hash)| (*height, *hash))
            .collect(),
        heights_by_hash: dump
            .heights_by_hash
            .iter()
            .filter(|(_, points_at)| *points_at <= last)
            .copied()
            .collect(),
        body_sizes: dump
            .body_sizes
            .iter()
            .filter(|(height, _)| **height <= last)
            .map(|(height, size)| (*height, *size))
            .collect(),
        roots: dump
            .roots
            .iter()
            .filter(|(height, _)| **height <= last)
            .map(|(height, roots)| (*height, *roots))
            .collect(),
    }
}

fn assert_clean(state: &ZebraDb) {
    let violations = audit_store(state);
    assert!(
        violations.is_empty(),
        "expected a coherent store after repair: {violations:?}"
    );
}

/// A coherent store is untouched: the direct audit reports nothing, and a
/// reopen through the real startup path changes no rows.
#[test]
fn startup_audit_is_noop_on_coherent_store() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let tempdir = tempfile::tempdir().expect("test tempdir is available");
    let config = persistent_config(tempdir.path());
    let state = trunk_state(&universe, config.clone());
    let original = dump_store(&state);

    let repair = state
        .audit_and_repair_zakura_header_store()
        .expect("audit reads and writes succeed");
    assert!(
        repair.is_none(),
        "no repair expected on a coherent store: {repair:?}"
    );
    assert_eq!(dump_store(&state), original);

    let state = reopen(state, &config, &universe);
    assert_eq!(
        dump_store(&state),
        original,
        "a reopen must not change a coherent store"
    );
}

/// Broken linkage mid-frontier (a self-consistent foreign row spliced into the
/// height index — the incident-shaped poison): the audit truncates to the row
/// below the splice, the heal persists across a further reopen, and header
/// sync re-downloads the truncated suffix back to the original store.
#[test]
fn startup_heals_broken_linkage_then_resync_converges() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let tempdir = tempfile::tempdir().expect("test tempdir is available");
    let config = persistent_config(tempdir.path());
    let state = trunk_state(&universe, config.clone());
    let original = dump_store(&state);

    // Replace the header and hash rows at height 20 with branch-A's fourth
    // row: internally consistent, but it does not link to trunk@19.
    let foreign = &universe.branches[BRANCH_A].headers[3];
    let header_cf = state.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
    let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&header_cf, Height(20), foreign.header.clone());
    batch.zs_insert(&hash_cf, Height(20), foreign.hash);
    state.db.write(batch).expect("raw insert writes");

    // The corruption is visible to the Pillar-2 reads before the heal.
    let error = state
        .recent_header_context(Height(30))
        .expect_err("the walk crosses the corrupted row");
    assert!(matches!(error, StoreIncoherentError::BrokenLinkage { .. }));

    let repair = state
        .audit_and_repair_zakura_header_store()
        .expect("audit reads and writes succeed")
        .expect("the corrupted store is repaired");
    assert_eq!(
        repair.last_coherent,
        Some((Height(19), universe.trunk_at(19).hash))
    );
    assert!(
        repair.violations.iter().any(|violation| matches!(
            violation,
            ZakuraStoreViolation::BrokenLinkage { height, actual_below, .. }
                if *height == Height(20) && *actual_below == universe.trunk_at(19).hash
        )),
        "expected BrokenLinkage at the splice: {:?}",
        repair.violations
    );

    // Everything above the last coherent height is gone, in all five CFs.
    assert_eq!(dump_store(&state), truncated(&original, Height(19)));
    assert_clean(&state);
    state
        .recent_header_context(Height(19))
        .expect("the healed store walks cleanly");

    // The heal persists: a reopen through the startup path changes nothing.
    let state = reopen(state, &config, &universe);
    assert_eq!(dump_store(&state), truncated(&original, Height(19)));

    // Header sync re-downloads the truncated suffix and converges back onto
    // the original chain.
    let redelivered: Vec<_> = universe.trunk[19..FORK_HEIGHT as usize]
        .iter()
        .map(|fab| fab.header.clone())
        .collect();
    commit_header_range(&state, universe.trunk_at(19).hash, &redelivered);
    assert_eq!(dump_store(&state), original);
    assert_clean(&state);
}

/// A gap mid-frontier strands every row above it: reopening alone (no direct
/// audit call) heals the store, proving the `ZebraDb::new` hook fires.
#[test]
fn startup_heals_gap_and_stranded_rows_on_reopen() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let tempdir = tempfile::tempdir().expect("test tempdir is available");
    let config = persistent_config(tempdir.path());
    let state = trunk_state(&universe, config.clone());
    let original = dump_store(&state);

    let header_cf = state.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
    let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
    let mut batch = DiskWriteBatch::new();
    batch.zs_delete(&header_cf, Height(15));
    batch.zs_delete(&hash_cf, Height(15));
    state.db.write(batch).expect("raw delete writes");

    let state = reopen(state, &config, &universe);

    // Rows 16..=50 were stranded above the gap; the audit truncated to 14.
    assert_eq!(dump_store(&state), truncated(&original, Height(14)));
    assert_clean(&state);
    assert_eq!(
        state.best_header_tip(),
        Some((Height(14), universe.trunk_at(14).hash))
    );
}

/// A missing reverse-index row breaks the hash↔height bijection: the audit
/// truncates to the row below it.
#[test]
fn startup_heals_missing_reverse_index_row() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe, Config::ephemeral());
    let original = dump_store(&state);

    let height_by_hash_cf = state.db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
    let mut batch = DiskWriteBatch::new();
    batch.zs_delete(&height_by_hash_cf, universe.trunk_at(10).hash);
    state.db.write(batch).expect("raw delete writes");

    let repair = state
        .audit_and_repair_zakura_header_store()
        .expect("audit reads and writes succeed")
        .expect("the corrupted store is repaired");
    assert_eq!(
        repair.last_coherent,
        Some((Height(9), universe.trunk_at(9).hash))
    );
    assert!(
        repair.violations.iter().any(|violation| matches!(
            violation,
            ZakuraStoreViolation::WrongHeightByHash { height, indexed: None, .. }
                if *height == Height(10)
        )),
        "expected WrongHeightByHash at the deleted entry: {:?}",
        repair.violations
    );
    assert_eq!(dump_store(&state), truncated(&original, Height(9)));
    assert_clean(&state);
}

/// An orphaned reverse-index row (a stranded `height_by_hash` entry from an
/// overwrite that never cleaned up the displaced hash) is removed on its own:
/// the linked chain is coherent, so nothing is truncated.
#[test]
fn startup_removes_orphan_reverse_row_without_truncating() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe, Config::ephemeral());
    let original = dump_store(&state);

    let height_by_hash_cf = state.db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
    let stray_hash = universe.branches[BRANCH_A].headers[0].hash;
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&height_by_hash_cf, stray_hash, Height(12));
    state.db.write(batch).expect("raw insert writes");

    let repair = state
        .audit_and_repair_zakura_header_store()
        .expect("audit reads and writes succeed")
        .expect("the corrupted store is repaired");
    assert_eq!(
        repair.last_coherent,
        Some((Height(FORK_HEIGHT), universe.trunk_at(FORK_HEIGHT).hash)),
        "an orphan reverse row must not shorten the coherent chain"
    );
    assert_eq!(repair.deleted_rows, 1);
    assert!(
        repair.violations.iter().any(|violation| matches!(
            violation,
            ZakuraStoreViolation::OrphanHeightByHash { hash, points_at }
                if *hash == stray_hash && *points_at == Height(12)
        )),
        "expected OrphanHeightByHash for the stray entry: {:?}",
        repair.violations
    );
    assert_eq!(dump_store(&state), original);
    assert_clean(&state);
}

/// Stale zakura rows at a committed height (the pre-guard re-delivery shape:
/// rows the release trim never touches again) are removed without disturbing
/// the frontier above.
#[test]
fn startup_removes_stale_rows_at_committed_heights() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe, Config::ephemeral());
    let original = dump_store(&state);

    // Genesis is the only committed block; plant zakura rows at its height.
    let header_cf = state.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
    let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
    let height_by_hash_cf = state.db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
    let genesis_hash = universe.genesis.hash();
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&header_cf, Height(0), universe.genesis.header.clone());
    batch.zs_insert(&hash_cf, Height(0), genesis_hash);
    batch.zs_insert(&height_by_hash_cf, genesis_hash, Height(0));
    state.db.write(batch).expect("raw insert writes");

    let repair = state
        .audit_and_repair_zakura_header_store()
        .expect("audit reads and writes succeed")
        .expect("the corrupted store is repaired");
    assert_eq!(
        repair.last_coherent,
        Some((Height(FORK_HEIGHT), universe.trunk_at(FORK_HEIGHT).hash)),
        "stale committed-height rows must not shorten the frontier"
    );
    assert_eq!(repair.deleted_rows, 3);
    assert!(
        repair.violations.iter().all(|violation| matches!(
            violation,
            ZakuraStoreViolation::StaleRowAtCommittedHeight { height, .. }
                if *height == Height(0)
        )),
        "expected only StaleRowAtCommittedHeight at genesis: {:?}",
        repair.violations
    );
    assert_eq!(dump_store(&state), original);
    assert_clean(&state);
}

/// The reads.rs anchor-corruption shape (a hash row overwritten with a foreign
/// hash) makes the Pillar-2 range writer reject with `StoreIncoherent`; after
/// the heal the same delivery commits. This pins the Pillar-2 interaction: any
/// store those reads refuse is healed by this audit.
#[test]
fn startup_heal_unblocks_linkage_verified_reads() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe, Config::ephemeral());
    let original = dump_store(&state);

    // The hash row at height 30 now names a different block, while the header
    // row and `height_by_hash` still claim the trunk.
    let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
    let stray_hash = universe.branches[BRANCH_A].headers[0].hash;
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&hash_cf, Height(30), stray_hash);
    state.db.write(batch).expect("raw insert writes");

    // A re-delivered range anchored at trunk@30 is rejected as a local
    // storage fault before the heal.
    let anchor = universe.trunk_at(30).hash;
    let headers: Vec<_> = universe.trunk[30..35]
        .iter()
        .map(|fab| fab.header.clone())
        .collect();
    let body_sizes = vec![0; headers.len()];
    let mut batch = DiskWriteBatch::new();
    let error = batch
        .prepare_header_range_batch(&state, anchor, &headers, &body_sizes)
        .expect_err("the anchor round-trip fails on the corrupted index");
    assert!(matches!(
        error,
        CommitHeaderRangeError::StoreIncoherent(StoreIncoherentError::BijectionMismatch { .. })
    ));

    let repair = state
        .audit_and_repair_zakura_header_store()
        .expect("audit reads and writes succeed")
        .expect("the corrupted store is repaired");
    assert_eq!(
        repair.last_coherent,
        Some((Height(29), universe.trunk_at(29).hash))
    );
    assert!(
        repair.violations.iter().any(|violation| matches!(
            violation,
            ZakuraStoreViolation::HeaderHashMismatch { height, indexed, .. }
                if *height == Height(30) && *indexed == stray_hash
        )),
        "expected HeaderHashMismatch at the overwritten hash row: {:?}",
        repair.violations
    );
    assert_eq!(dump_store(&state), truncated(&original, Height(29)));
    assert_clean(&state);

    // The same fork of history now commits, anchored at the healed tip.
    let redelivered: Vec<_> = universe.trunk[29..FORK_HEIGHT as usize]
        .iter()
        .map(|fab| fab.header.clone())
        .collect();
    commit_header_range(&state, universe.trunk_at(29).hash, &redelivered);
    assert_eq!(dump_store(&state), original);
    assert_clean(&state);
}

/// A minimal local metrics recorder capturing counter increments by name.
#[derive(Clone, Default)]
struct CounterCapture(Arc<Mutex<HashMap<String, u64>>>);

impl CounterCapture {
    fn get(&self, name: &str) -> u64 {
        self.0
            .lock()
            .expect("counter capture lock is never poisoned")
            .get(name)
            .copied()
            .unwrap_or(0)
    }
}

struct CaptureHandle {
    name: String,
    store: Arc<Mutex<HashMap<String, u64>>>,
}

impl metrics::CounterFn for CaptureHandle {
    fn increment(&self, value: u64) {
        *self
            .store
            .lock()
            .expect("counter capture lock is never poisoned")
            .entry(self.name.clone())
            .or_insert(0) += value;
    }

    fn absolute(&self, value: u64) {
        self.store
            .lock()
            .expect("counter capture lock is never poisoned")
            .insert(self.name.clone(), value);
    }
}

impl metrics::Recorder for CounterCapture {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::from_arc(Arc::new(CaptureHandle {
            name: key.name().to_string(),
            store: self.0.clone(),
        }))
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// The `state.zakura.header_store.incoherent` metric fires exactly when a
/// startup repair runs: once for the healing reopen, and not at all for the
/// clean reopen after it.
#[test]
fn startup_repair_emits_incoherent_metric() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let tempdir = tempfile::tempdir().expect("test tempdir is available");
    let config = persistent_config(tempdir.path());
    let state = trunk_state(&universe, config.clone());

    let header_cf = state.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
    let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
    let mut batch = DiskWriteBatch::new();
    batch.zs_delete(&header_cf, Height(15));
    batch.zs_delete(&hash_cf, Height(15));
    state.db.write(batch).expect("raw delete writes");

    const METRIC: &str = "state.zakura.header_store.incoherent";

    let healing = CounterCapture::default();
    let state = metrics::with_local_recorder(&healing, || reopen(state, &config, &universe));
    assert_eq!(
        healing.get(METRIC),
        1,
        "the healing reopen emits the metric"
    );
    assert_clean(&state);

    let clean = CounterCapture::default();
    let state = metrics::with_local_recorder(&clean, || reopen(state, &config, &universe));
    assert_eq!(clean.get(METRIC), 0, "a clean reopen emits nothing");
    assert_clean(&state);
}

/// Stale zakura rows at *pruned* heights are classified
/// `StaleRowAtCommittedHeight` and deleted, while everything else — the
/// zakura frontier and the roots rows at committed heights — is untouched.
///
/// This is the test the audit's committed-height predicate was chosen for:
/// the predicate is the consensus `hash_by_height` row, which pruning
/// retains, not body presence (pruning removes bodies). A body-presence
/// predicate would misclassify these rows as frontier rows and let the
/// pre-#491 bug-2 shape (stale re-delivered rows below the body tip)
/// survive on pruned stores.
#[test]
fn startup_removes_stale_rows_at_pruned_heights() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let tempdir = tempfile::tempdir().expect("test tempdir is available");
    let config = persistent_config(tempdir.path());
    let state = trunk_state(&universe, config.clone());

    // Commit bodies 1..=10 (releasing the zakura rows there), then prune
    // raw transactions below height 8 through the production prune batch.
    for fab in &universe.trunk[..10] {
        write_full_block_header_and_transactions(&state, fabricate_body(fab));
    }
    let mut batch = DiskWriteBatch::new();
    batch.prepare_prune_batch(&state, Height(1), Height(8));
    state.write_batch(batch).expect("prune batch writes");

    // The pruned-store shape: committed heights whose bodies are gone.
    assert!(state.contains_height(Height(5)));
    assert!(!state.contains_body_at_height(Height(5)));
    assert!(state.contains_body_at_height(Height(8)));

    // A verified roots row at a pruned height (pruning retains these): the
    // audit must leave it untouched — committed heights are out of scope.
    let roots_cf = state.db.cf_handle(COMMITMENT_ROOTS_BY_HEIGHT).unwrap();
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(
        &roots_cf,
        Height(5),
        CommitmentRootsByHeight {
            sapling: zebra_chain::sapling::tree::NoteCommitmentTree::default().root(),
            orchard: zebra_chain::orchard::tree::NoteCommitmentTree::default().root(),
            auth_data_root: zebra_chain::block::merkle::AuthDataRoot::from([7u8; 32]),
            ironwood: zebra_chain::ironwood::tree::NoteCommitmentTree::default().root(),
            sapling_tx: 0,
            orchard_tx: 0,
            ironwood_tx: 0,
        },
    );
    state.db.write(batch).expect("raw insert writes");

    let clean = dump_store(&state);
    assert!(
        clean.roots.contains_key(&Height(5)),
        "the roots row at a pruned height is present and out of audit scope"
    );

    // Hand-plant stale zakura rows at pruned heights: a full row triple at
    // height 5 and a body-size hint at height 3 (the pre-guard re-delivery
    // shape: `contains_height` true, `contains_body_at_height` false).
    let header_cf = state.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
    let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
    let reverse_cf = state.db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
    let body_size_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
        .unwrap();
    let stale = &universe.trunk[4];
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&header_cf, Height(5), stale.header.clone());
    batch.zs_insert(&hash_cf, Height(5), stale.hash);
    batch.zs_insert(&reverse_cf, stale.hash, Height(5));
    batch.zs_insert(&body_size_cf, Height(3), test_body_size(77));
    state.db.write(batch).expect("raw insert writes");

    let repair = state
        .audit_and_repair_zakura_header_store()
        .expect("audit reads and writes succeed")
        .expect("the stale rows are repaired");
    assert_eq!(repair.deleted_rows, 4);
    for (cf, height) in [
        (ZAKURA_HEADER_BY_HEIGHT, Height(5)),
        (ZAKURA_HEADER_HASH_BY_HEIGHT, Height(5)),
        (ZAKURA_HEADER_HEIGHT_BY_HASH, Height(5)),
        (ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT, Height(3)),
    ] {
        assert!(
            repair.violations.iter().any(|violation| matches!(
                violation,
                ZakuraStoreViolation::StaleRowAtCommittedHeight {
                    cf: found_cf,
                    height: found_height,
                } if *found_cf == cf && *found_height == height
            )),
            "expected StaleRowAtCommittedHeight for {cf} at {height:?}: {:?}",
            repair.violations
        );
    }
    assert_eq!(
        repair.last_coherent,
        Some((Height(FORK_HEIGHT), universe.trunk_at(FORK_HEIGHT).hash)),
        "stale rows at pruned heights must not shorten the coherent frontier"
    );

    // The repair restores exactly the pre-corruption store: the frontier
    // and the out-of-scope roots rows at committed heights survive.
    assert_eq!(dump_store(&state), clean);
    assert_clean(&state);

    // The same heal runs on the real startup path (reopen).
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&hash_cf, Height(6), universe.trunk[5].hash);
    state.db.write(batch).expect("raw insert writes");
    let state = reopen(state, &config, &universe);
    assert_eq!(
        dump_store(&state),
        clean,
        "the startup repair removes the stale pruned-height row"
    );
    assert_clean(&state);
}
