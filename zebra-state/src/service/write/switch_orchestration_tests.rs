//! End-to-end tests for the Pillar-1a branch-switch orchestration: a
//! reorging `CommitHeaderRange` driven through the real worker function
//! ([`super::commit_header_range`]), against a real header store and
//! non-finalized state (INTEGRATION_TEST_PLAN.md T1).
//!
//! Fixture design (per the spike recorded in the plan): one fabricated
//! universe on [`pre_heartwood_test_network`], where non-finalized commits
//! run in the pre-Heartwood regime and accept fabricated V4-coinbase bodies.
//! Branches fork above a shared non-finalized prefix so rolled-back blocks
//! can recommit through the full validation path (their parent stays in the
//! non-finalized state).
//!
//! ```text
//! genesis ── trunk (1..=50) ── prefix (51..=53) ─┬─ A      (54..=79, fast)
//!   header rows: trunk + prefix + one branch     ├─ B      (54..=83, slow: lower work than A)
//!   nf bodies:   prefix + a few A blocks         ├─ W      (54..=83, fast: higher work than A)
//!                                                └─ A_ext  (A + 6 fast: higher work than W)
//! ```

use std::sync::Arc;

use tokio::sync::{oneshot, watch};
use zebra_chain::{
    block::{self, Block, Height},
    parameters::Network,
};

use crate::{
    arbitrary::Prepare,
    error::CommitHeaderRangeError,
    service::{
        finalized_state::{
            zebra_db::block::tests::{
                common::{mainnet_block, persistent_config, CounterCapture},
                header_store_coherence::{
                    audit::{audit_store, dump_store},
                    fabricate::{
                        extend_context, fabricate_headers, fabricate_v4_body,
                        pre_heartwood_test_network, total_work, DifficultyContext, FabHeader,
                        Spacing,
                    },
                },
            },
            DiskWriteBatch, FinalizedState, WriteDisk, ZebraDb,
        },
        non_finalized_state::NonFinalizedState,
        ChainTipSender,
    },
    Config, HeaderRangeCommitOutcome,
};

/// The first height where the branches conflict with each other.
const REORG_HEIGHT: Height = Height(54);

/// The repair metric emitted by the store audits.
const INCOHERENT_METRIC: &str = "state.zakura.header_store.incoherent";

/// The counter emitted when the orchestration rolls back a stranded suffix.
const ROLLBACK_METRIC: &str = "sync.header.fork_recovery.body_suffix_invalidated";

/// The fixed branch universe every switch test runs over.
struct SwitchUniverse {
    network: Network,
    genesis: Arc<Block>,
    /// Heights 1..=50, fast spacing.
    trunk: Vec<FabHeader>,
    /// Heights 51..=53: the shared prefix, committed both as header rows and
    /// as non-finalized bodies.
    prefix: Vec<FabHeader>,
    /// 26 fast headers off the prefix tip (54..=79).
    branch_a: Vec<FabHeader>,
    /// 30 slow headers off the prefix tip (54..=83) — longer than `A` but
    /// lower total work.
    branch_b: Vec<FabHeader>,
    /// 30 fast headers off the prefix tip (54..=83) — strictly more work
    /// than `A` (same per-header thresholds, four more headers).
    branch_w: Vec<FabHeader>,
    /// `A` plus a 6-header fast continuation (54..=85) — strictly more work
    /// than `W`.
    branch_a_ext: Vec<FabHeader>,
}

impl SwitchUniverse {
    fn new() -> Self {
        let genesis = mainnet_block(0);
        let network = pre_heartwood_test_network(genesis.hash());
        let genesis_context: DifficultyContext =
            vec![(genesis.header.difficulty_threshold, genesis.header.time)];

        let trunk = fabricate_headers(
            &network,
            (Height(0), genesis.hash()),
            genesis_context.clone(),
            &[Spacing::Fast; 50],
            0x10,
        );
        let trunk_tip = trunk.last().expect("trunk is non-empty");
        let trunk_context = extend_context(genesis_context, &trunk);

        let prefix = fabricate_headers(
            &network,
            (trunk_tip.height, trunk_tip.hash),
            trunk_context.clone(),
            &[Spacing::Fast; 3],
            0x20,
        );
        let prefix_tip = prefix.last().expect("prefix is non-empty");
        let fork_parent = (prefix_tip.height, prefix_tip.hash);
        let fork_context = extend_context(trunk_context, &prefix);

        let branch_a = fabricate_headers(
            &network,
            fork_parent,
            fork_context.clone(),
            &[Spacing::Fast; 26],
            0x40,
        );
        let branch_b = fabricate_headers(
            &network,
            fork_parent,
            fork_context.clone(),
            &[Spacing::Slow; 30],
            0x80,
        );
        let branch_w = fabricate_headers(
            &network,
            fork_parent,
            fork_context.clone(),
            &[Spacing::Fast; 30],
            0xC0,
        );

        let a_tip = branch_a.last().expect("branch A is non-empty");
        let a_context = extend_context(fork_context, &branch_a);
        let mut branch_a_ext = branch_a.clone();
        branch_a_ext.extend(fabricate_headers(
            &network,
            (a_tip.height, a_tip.hash),
            a_context,
            &[Spacing::Fast; 6],
            0x60,
        ));

        let universe = SwitchUniverse {
            network,
            genesis,
            trunk,
            prefix,
            branch_a,
            branch_b,
            branch_w,
            branch_a_ext,
        };

        // The work orderings every test relies on.
        assert!(
            total_work(&universe.branch_a) > total_work(&universe.branch_b),
            "branch A must out-work the longer slow branch B"
        );
        assert!(
            total_work(&universe.branch_w) > total_work(&universe.branch_a),
            "branch W must out-work branch A"
        );
        assert!(
            total_work(&universe.branch_a_ext) > total_work(&universe.branch_w),
            "extended branch A must out-work branch W"
        );

        universe
    }

    fn prefix_tip(&self) -> &FabHeader {
        self.prefix.last().expect("prefix is non-empty")
    }
}

/// The channel plumbing around the states, mirroring the write worker's.
struct Plumbing {
    chain_tip_sender: ChainTipSender,
    _latest_chain_tip: crate::LatestChainTip,
    _chain_tip_change: crate::ChainTipChange,
    nf_sender: watch::Sender<NonFinalizedState>,
    nf_receiver: watch::Receiver<NonFinalizedState>,
}

impl Plumbing {
    fn new(network: &Network) -> Self {
        let (chain_tip_sender, latest_chain_tip, chain_tip_change) =
            ChainTipSender::new(None, network);
        let (nf_sender, nf_receiver) = watch::channel(NonFinalizedState::new(network));
        Plumbing {
            chain_tip_sender,
            _latest_chain_tip: latest_chain_tip,
            _chain_tip_change: chain_tip_change,
            nf_sender,
            nf_receiver,
        }
    }
}

/// Commits `headers` as header rows through the production range writer,
/// panicking on rejection (fixture setup only).
fn commit_rows(db: &ZebraDb, anchor: block::Hash, headers: &[FabHeader]) {
    let mut batch = DiskWriteBatch::new();
    let arcs: Vec<_> = headers.iter().map(|fab| fab.header.clone()).collect();
    let body_sizes: Vec<_> = headers.iter().map(|fab| fab.body_size).collect();
    batch
        .prepare_header_range_batch(db, anchor, &arcs, &body_sizes)
        .expect("fixture header range is valid");
    db.write_batch(batch).expect("fixture header range writes");
}

/// Builds the standard fixture: genesis finalized with treestates; header
/// rows for trunk + prefix + branch A; non-finalized bodies for the prefix
/// (when `include_prefix_bodies`) plus the first `a_bodies` branch-A blocks.
fn fixture(
    universe: &SwitchUniverse,
    config: Config,
    include_prefix_bodies: bool,
    a_bodies: usize,
) -> (FinalizedState, NonFinalizedState, Plumbing) {
    let mut finalized_state = FinalizedState::new(
        &config,
        &universe.network,
        #[cfg(feature = "elasticsearch")]
        false,
    );
    finalized_state
        .commit_finalized_direct(universe.genesis.clone().into(), None, None, "switch tests")
        .expect("genesis commits with treestates");

    commit_rows(
        &finalized_state.db,
        universe.genesis.hash(),
        &universe.trunk,
    );
    let trunk_tip = universe.trunk.last().expect("trunk is non-empty");
    commit_rows(&finalized_state.db, trunk_tip.hash, &universe.prefix);
    commit_rows(
        &finalized_state.db,
        universe.prefix_tip().hash,
        &universe.branch_a,
    );

    let mut non_finalized_state = NonFinalizedState::new(&universe.network);
    let mut bodies: Vec<&FabHeader> = Vec::new();
    if include_prefix_bodies {
        bodies.extend(&universe.prefix);
    }
    bodies.extend(&universe.branch_a[..a_bodies]);
    for (index, fab) in bodies.iter().enumerate() {
        let prepared = fabricate_v4_body(fab).prepare();
        if index == 0 {
            non_finalized_state
                .commit_new_chain(prepared, &finalized_state)
                .expect("fixture chain root commits");
        } else {
            non_finalized_state
                .commit_block(prepared, &finalized_state)
                .expect("fixture chain block commits");
        }
    }

    let plumbing = Plumbing::new(&universe.network);
    (finalized_state, non_finalized_state, plumbing)
}

/// Drives a header range through the real switch orchestration and returns
/// its response.
#[allow(clippy::unwrap_in_result)]
fn deliver(
    finalized_state: &FinalizedState,
    non_finalized_state: &mut NonFinalizedState,
    plumbing: &mut Plumbing,
    anchor: block::Hash,
    headers: &[FabHeader],
) -> Result<HeaderRangeCommitOutcome, CommitHeaderRangeError> {
    let (rsp_tx, mut rsp_rx) = oneshot::channel();
    super::commit_header_range(
        finalized_state,
        non_finalized_state,
        &mut plumbing.chain_tip_sender,
        &plumbing.nf_sender,
        None,
        anchor,
        headers.iter().map(|fab| fab.header.clone()).collect(),
        headers.iter().map(|fab| fab.body_size).collect(),
        headers.iter().map(|fab| fab.roots.clone()).collect(),
        rsp_tx,
    );
    rsp_rx
        .try_recv()
        .expect("the orchestration always sends a response")
}

/// Asserts the stored header chain above the prefix is exactly `branch`.
fn assert_store_holds_branch(db: &ZebraDb, universe: &SwitchUniverse, branch: &[FabHeader]) {
    for fab in universe.trunk.iter().chain(&universe.prefix).chain(branch) {
        assert_eq!(
            db.zakura_header_hash(fab.height),
            Some(fab.hash),
            "stored hash row at {:?} matches the expected branch",
            fab.height,
        );
    }
    let branch_tip = branch.last().expect("branches are non-empty");
    let above = branch_tip
        .height
        .next()
        .expect("test heights stay in range");
    assert_eq!(
        db.zakura_header_hash(above),
        None,
        "no stray row above the branch tip",
    );
    assert_eq!(
        db.best_header_tip(),
        Some((branch_tip.height, branch_tip.hash)),
        "the header tip is the branch tip",
    );
}

/// T1.1: a conflicting higher-work range delivered through the worker
/// switches the header store, rolls the stranded body suffix back to the
/// fork, publishes the truncated chain, and reports the reorg in the
/// response — with the post-reorg audit running clean.
#[test]
fn reorging_commit_switches_store_and_rolls_back_stranded_bodies() {
    let _init_guard = zebra_test::init();
    let universe = SwitchUniverse::new();
    let (finalized_state, mut non_finalized_state, mut plumbing) =
        fixture(&universe, Config::ephemeral(), true, 3);

    let capture = CounterCapture::default();
    let outcome = metrics::with_local_recorder(&capture, || {
        deliver(
            &finalized_state,
            &mut non_finalized_state,
            &mut plumbing,
            universe.prefix_tip().hash,
            &universe.branch_w,
        )
    })
    .expect("the higher-work conflicting range commits");

    // The response reports the switch.
    let w_tip = universe.branch_w.last().expect("branch W is non-empty");
    assert_eq!(outcome.tip_hash, w_tip.hash);
    assert_eq!(outcome.reorged_at, Some(REORG_HEIGHT));
    assert_eq!(outcome.reorged_to_hash, Some(universe.branch_w[0].hash));

    // The header store switched to W and stayed coherent.
    assert_store_holds_branch(&finalized_state.db, &universe, &universe.branch_w);
    assert!(audit_store(&finalized_state.db).is_empty());

    // The stranded body suffix was rolled back to the fork, keeping the
    // shared prefix, and the truncated chain was published.
    let best_chain = non_finalized_state
        .best_chain()
        .expect("the shared prefix survives");
    assert!(best_chain.contains_block_hash(universe.prefix_tip().hash));
    assert!(!best_chain.contains_block_hash(universe.branch_a[0].hash));
    assert!(plumbing
        .nf_receiver
        .borrow()
        .best_chain()
        .is_some_and(|chain| !chain.contains_block_hash(universe.branch_a[0].hash)));

    // Exactly one rollback ran, and the post-reorg audit found nothing.
    assert_eq!(capture.get(ROLLBACK_METRIC), 1);
    assert_eq!(capture.get(INCOHERENT_METRIC), 0);
}

/// T1.1 (repair variant): the post-reorg audit hook runs on the reorging
/// commit and repairs a hand-planted stray row, emitting the repair metric.
#[test]
fn reorging_commit_audit_hook_repairs_planted_stray_row() {
    let _init_guard = zebra_test::init();
    let universe = SwitchUniverse::new();
    let (finalized_state, mut non_finalized_state, mut plumbing) =
        fixture(&universe, Config::ephemeral(), true, 3);

    // An orphan reverse-index row pointing *below* the fork: invisible to
    // the delivery path (which never scans the reverse index) and outside
    // the suffix the reorg batch replaces, so only the audit hook can
    // repair it. (A stray above the new tip would be swept by the
    // suffix-replacement primitive itself before the audit looks.)
    let height_by_hash = finalized_state
        .db
        .db()
        .cf_handle("zakura_header_height_by_hash")
        .expect("column family exists");
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&height_by_hash, block::Hash([0xEE; 32]), Height(20));
    finalized_state
        .db
        .db()
        .write(batch)
        .expect("stray row writes");
    assert!(
        !audit_store(&finalized_state.db).is_empty(),
        "the planted row is a store violation"
    );

    let capture = CounterCapture::default();
    metrics::with_local_recorder(&capture, || {
        deliver(
            &finalized_state,
            &mut non_finalized_state,
            &mut plumbing,
            universe.prefix_tip().hash,
            &universe.branch_w,
        )
    })
    .expect("the higher-work conflicting range commits");

    assert_eq!(
        capture.get(INCOHERENT_METRIC),
        1,
        "the post-reorg audit repaired the stray row"
    );
    assert!(audit_store(&finalized_state.db).is_empty());
    assert_store_holds_branch(&finalized_state.db, &universe, &universe.branch_w);
}

/// T1.2: a crash between the body rollback and the header rewrite leaves
/// the crash-table intermediate — header store unchanged and coherent, nf
/// truncated — the startup audit passes it without repair, and re-running
/// the same commit converges to the switched state (recovery is the
/// ordinary flow).
#[test]
fn kill_between_rollback_and_header_rewrite_recovers_through_ordinary_flow() {
    let _init_guard = zebra_test::init();
    let universe = SwitchUniverse::new();
    let tempdir = tempfile::tempdir().expect("test tempdir is available");
    let config = persistent_config(tempdir.path());
    let (finalized_state, mut non_finalized_state, mut plumbing) =
        fixture(&universe, config.clone(), true, 3);

    let store_before = dump_store(&finalized_state.db);

    // Step 1 (evaluate): prepare the batch, deciding the fork point.
    let arcs: Vec<_> = universe
        .branch_w
        .iter()
        .map(|fab| fab.header.clone())
        .collect();
    let body_sizes: Vec<_> = universe.branch_w.iter().map(|fab| fab.body_size).collect();
    let roots: Vec<_> = universe
        .branch_w
        .iter()
        .map(|fab| fab.roots.clone())
        .collect();
    let mut batch = DiskWriteBatch::new();
    let outcome = batch
        .prepare_header_range_batch_with_roots(
            &finalized_state.db,
            universe.prefix_tip().hash,
            &arcs,
            &body_sizes,
            &roots,
        )
        .expect("the range evaluates to a reorging batch");
    assert_eq!(outcome.reorged_at, Some(REORG_HEIGHT));

    // Step 2 (body rollback), then crash before step 3: the prepared batch
    // is dropped unwritten.
    super::invalidate_stranded_body_suffix(
        &mut non_finalized_state,
        &mut plumbing.chain_tip_sender,
        &plumbing.nf_sender,
        None,
        REORG_HEIGHT,
        universe.branch_w[0].hash,
    );
    drop(batch);

    // The crash intermediate: the header store is unchanged (still A) and
    // coherent; only the nf suffix is gone.
    assert_eq!(dump_store(&finalized_state.db), store_before);
    assert!(audit_store(&finalized_state.db).is_empty());
    assert_store_holds_branch(&finalized_state.db, &universe, &universe.branch_a);
    let best_chain = non_finalized_state
        .best_chain()
        .expect("the shared prefix survives");
    assert!(!best_chain.contains_block_hash(universe.branch_a[0].hash));

    // Restart: the startup audit passes the intermediate without repair.
    drop(finalized_state);
    let capture = CounterCapture::default();
    let finalized_state = metrics::with_local_recorder(&capture, || {
        FinalizedState::new(
            &config,
            &universe.network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
    });
    assert_eq!(
        capture.get(INCOHERENT_METRIC),
        0,
        "the crash intermediate is coherent: no startup repair"
    );
    assert_eq!(dump_store(&finalized_state.db), store_before);

    // Recovery is the ordinary flow: the same commit re-runs end-to-end and
    // converges to the switched state.
    let outcome = deliver(
        &finalized_state,
        &mut non_finalized_state,
        &mut plumbing,
        universe.prefix_tip().hash,
        &universe.branch_w,
    )
    .expect("the re-run commits");
    assert_eq!(outcome.reorged_at, Some(REORG_HEIGHT));
    assert_store_holds_branch(&finalized_state.db, &universe, &universe.branch_w);
    assert!(audit_store(&finalized_state.db).is_empty());
}

/// T1.3: a reorg at the non-finalized root empties the chain set without
/// panicking, publishes the emptied state, and still switches the store.
#[test]
fn whole_suffix_switch_empties_non_finalized_state_without_panicking() {
    let _init_guard = zebra_test::init();
    let universe = SwitchUniverse::new();
    // No prefix bodies: the nf chain roots at the reorged height itself.
    let (finalized_state, mut non_finalized_state, mut plumbing) =
        fixture(&universe, Config::ephemeral(), false, 3);

    let outcome = deliver(
        &finalized_state,
        &mut non_finalized_state,
        &mut plumbing,
        universe.prefix_tip().hash,
        &universe.branch_w,
    )
    .expect("the higher-work conflicting range commits");

    assert_eq!(outcome.reorged_at, Some(REORG_HEIGHT));
    assert!(
        non_finalized_state.best_chain().is_none(),
        "rolling back the nf root empties the chain set"
    );
    assert!(
        plumbing.nf_receiver.borrow().best_chain().is_none(),
        "the emptied state was published"
    );
    assert_store_holds_branch(&finalized_state.db, &universe, &universe.branch_w);
    assert!(audit_store(&finalized_state.db).is_empty());
}

/// T1.4: a lower-work conflict is rejected with no side effects; the
/// higher-work extension switches; the original branch switches back when
/// it out-works the winner; and its rolled-back bodies recommit through the
/// full validation path — rollback is not a ban, end to end.
#[test]
fn losing_then_winning_branch_switches_back_and_recommits() {
    let _init_guard = zebra_test::init();
    let universe = SwitchUniverse::new();
    let (finalized_state, mut non_finalized_state, mut plumbing) =
        fixture(&universe, Config::ephemeral(), true, 2);

    let store_before = dump_store(&finalized_state.db);
    let nf_before = non_finalized_state.clone();

    // A lower-work conflicting range is rejected with no side effects.
    let error = deliver(
        &finalized_state,
        &mut non_finalized_state,
        &mut plumbing,
        universe.prefix_tip().hash,
        &universe.branch_b,
    )
    .expect_err("the lower-work range is rejected");
    assert!(
        matches!(error, CommitHeaderRangeError::LowerWorkConflict { .. }),
        "unexpected rejection: {error:?}"
    );
    assert_eq!(
        dump_store(&finalized_state.db),
        store_before,
        "a rejected candidate leaves the store untouched"
    );
    assert!(
        non_finalized_state.eq_internal_state(&nf_before),
        "a rejected candidate leaves the nf state untouched"
    );

    // The higher-work branch W wins: bodies roll back to the prefix.
    let outcome = deliver(
        &finalized_state,
        &mut non_finalized_state,
        &mut plumbing,
        universe.prefix_tip().hash,
        &universe.branch_w,
    )
    .expect("the higher-work range commits");
    assert_eq!(outcome.reorged_at, Some(REORG_HEIGHT));
    assert_store_holds_branch(&finalized_state.db, &universe, &universe.branch_w);

    // Branch A, extended past W's work, switches back.
    let outcome = deliver(
        &finalized_state,
        &mut non_finalized_state,
        &mut plumbing,
        universe.prefix_tip().hash,
        &universe.branch_a_ext,
    )
    .expect("the extended original branch switches back");
    assert_eq!(outcome.reorged_at, Some(REORG_HEIGHT));
    assert_eq!(outcome.reorged_to_hash, Some(universe.branch_a[0].hash));
    assert_store_holds_branch(&finalized_state.db, &universe, &universe.branch_a_ext);
    assert!(audit_store(&finalized_state.db).is_empty());

    // The rolled-back A bodies recommit through the full validation path
    // (contextual checks included): rollback is not a ban.
    for fab in &universe.branch_a[..2] {
        let prepared = fabricate_v4_body(fab).prepare();
        super::validate_and_commit_non_finalized(
            &finalized_state.db,
            &mut non_finalized_state,
            prepared,
        )
        .expect("a rolled-back block recommits when its branch wins again");
    }
    let best_chain = non_finalized_state
        .best_chain()
        .expect("chain is non-empty");
    assert!(best_chain.contains_block_hash(universe.branch_a[0].hash));
    assert!(best_chain.contains_block_hash(universe.branch_a[1].hash));
}
