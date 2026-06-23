//! Verified-commitment-trees fast-sync experiment state (POC harness).
//!
//! This module holds the embedded-frontier plumbing and run counters for the
//! verified-commitment-trees fast path. On networks with an embedded handoff frontier,
//! the default source is the peer `tree_aux` source; `checkpoint_sync = false` opts out
//! to legacy recompute.
//!
//! [`super`] (`finalized_state.rs`) holds only the commit-path hook (the checkpoint
//! handoff write and the fast-sync marker); everything about *where the data comes
//! from* lives here, behind a small method API so the commit path never touches the
//! experiment's internals.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use thiserror::Error;
#[cfg(test)]
use zebra_chain::parallel::tree::NoteCommitmentTrees;
use zebra_chain::{
    block, orchard,
    parameters::{Network, NetworkUpgrade},
    sapling, sprout,
};

use super::commitment_aux::{CommitmentRootSource, FinalFrontiers, PeerSource, PeerSourceHandle};

/// Embedded verified final note-commitment frontiers for Mainnet.
const MAINNET_FINAL_FRONTIERS: &[u8] = include_bytes!("vct/mainnet-frontier.bin");

/// Errors validating serialized VCT final-frontier bytes.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FinalFrontiersValidationError {
    /// The bytes could not be parsed as [`FinalFrontiers`].
    #[error("invalid VCT final frontier bytes: {error}")]
    InvalidBytes {
        /// The parser error message.
        error: String,
    },

    /// The serialized frontier height does not match the expected checkpoint handoff height.
    #[error("embedded VCT final frontier height must match the network's max checkpoint height")]
    HeightMismatch {
        /// Height encoded in the serialized frontier.
        actual: block::Height,
        /// Expected checkpoint handoff height.
        expected: block::Height,
    },
}

/// POC state for the verified-commitment-trees experiment
/// (`docs/design/verified-commitment-trees.md`). Shared across
/// [`super::FinalizedState`] clones via `Arc` so the counters are shared.
///
/// A checkpoint-trusting sync (`checkpoint_sync = true`) uses the peer `tree_aux` source by
/// default on networks with embedded final frontiers; `checkpoint_sync = false` opts out to
/// the legacy per-block recompute (no VCT state).
#[derive(Debug)]
pub(crate) struct VctState {
    /// Fast mode: skip the per-block frontier recompute and fold the source's roots
    /// into the anchor set + history tree.
    fast: bool,
    /// Where the verified per-block roots and handoff frontiers come from. The
    /// committer reads roots/handoff/frontiers through this seam only.
    source: Box<dyn CommitmentRootSource>,
    /// Whether roots from this VCT state must be confirmed against a buffered successor
    /// before they are committed.
    requires_verified_successor: bool,
    /// Count of blocks that took the fast (skip-recompute) path, for the run summary.
    fast_count: AtomicU64,
    /// Count of fast blocks whose own commitment check was skipped because the
    /// previous block's look-ahead already validated it (the dedup). Lets tests
    /// assert the dedup actually engages, so it can't be silently regressed.
    prevalidated_count: AtomicU64,
    /// Per-state `tree_aux` driver handle, present only for the peer source.
    peer_source_handle: Option<PeerSourceHandle>,
}

/// Which commitment-root source the committer uses, resolved from the (already read)
/// configuration signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceMode {
    /// Legacy recompute committer (no VCT state).
    Legacy,
    /// Fetch per-block roots from peers — the default where embedded frontiers exist.
    Peer,
}

/// Resolve the source mode as a pure function, so the peer-source default is
/// unit-testable without touching embedded-frontier files. The fast verified path
/// (peer source) is the default whenever the node syncs under checkpoint trust and
/// the network has an embedded handoff frontier. `checkpoint_sync = false` is the
/// only mode that fully reconstructs the note-commitment trees per block, so it
/// selects the legacy recompute; a network with no embedded frontier also falls back
/// to legacy. Storage mode (Archive vs. Pruned) is orthogonal and not an input here.
fn select_source_mode(checkpoint_sync: bool, has_embedded_frontiers: bool) -> SourceMode {
    if !checkpoint_sync || !has_embedded_frontiers {
        SourceMode::Legacy
    } else {
        SourceMode::Peer
    }
}

impl VctState {
    /// Build the committer state from `checkpoint_sync` (the mirror of
    /// `consensus.checkpoint_sync`). On networks with an embedded handoff frontier (Mainnet)
    /// a checkpoint-trusting sync defaults to the peer (`tree_aux`) fast source;
    /// `checkpoint_sync = false` (or a network without an embedded frontier) returns
    /// `None` for a zero-overhead legacy committer that recomputes the trees per block.
    pub(super) fn from_config(checkpoint_sync: bool, network: &Network) -> Option<Arc<Self>> {
        // Parse the embedded handoff frontier once (None on networks without one, e.g.
        // Testnet). The decision below only needs its presence; the peer arm reuses the
        // parsed value.
        let embedded = embedded_final_frontiers(network);

        match select_source_mode(checkpoint_sync, embedded.is_some()) {
            // Default: the peer (`tree_aux`) source on any network with embedded final
            // frontiers (Mainnet). Per-block roots arrive from peers into a shared cache
            // filled by the driver; the committer reads them per height and folds them in,
            // skipping the recompute. A height the peer cannot supply — or any node with no
            // serving peers — stays in legacy mode, bit-identical to a legacy committer by
            // construction (the precompute overlap is preserved for those blocks; see
            // `vct_fast_will_apply`).
            SourceMode::Peer => {
                let parsed = embedded?;
                tracing::info!(
                    handoff_height = parsed.height.0,
                    "VCT: peer (tree_aux) source enabled by default — roots fetched from peers"
                );
                let (source, peer_source_handle) = PeerSource::new(Some(parsed));
                Some(Arc::new(VctState {
                    fast: true,
                    source: Box::new(source),
                    requires_verified_successor: true,
                    fast_count: AtomicU64::new(0),
                    prevalidated_count: AtomicU64::new(0),
                    peer_source_handle: Some(peer_source_handle),
                }))
            }

            // Legacy committer: `checkpoint_sync = false` (full per-block recompute), or a
            // network with no embedded frontiers. No VCT state, zero overhead.
            SourceMode::Legacy => None,
        }
    }

    /// `true` when the fast (skip-recompute) path is active.
    pub(super) fn is_fast(&self) -> bool {
        self.fast
    }

    /// The per-state peer-source driver handle, if this committer uses the `tree_aux`
    /// peer source.
    pub(super) fn peer_source_handle(&self) -> Option<PeerSourceHandle> {
        self.peer_source_handle.clone()
    }

    /// Request a targeted peer-root refetch for `height`.
    pub(super) fn request_peer_root_refetch(&self, height: block::Height) -> bool {
        let Some(handle) = &self.peer_source_handle else {
            return false;
        };

        handle.request_refetch(height);
        true
    }

    /// The supplied roots for `height`, when fast mode has a source entry for it
    /// (the signal that this block takes the fast path).
    pub(super) fn fast_root(
        &self,
        height: block::Height,
    ) -> Option<(sapling::tree::Root, orchard::tree::Root)> {
        if !self.fast {
            return None;
        }

        if self
            .source
            .handoff_height()
            .is_some_and(|handoff| height > handoff)
        {
            return None;
        }

        self.source.fast_root(height)
    }

    /// `true` when committing `height` on the fast path needs a buffered successor before
    /// it can safely persist this block's supplied roots.
    ///
    /// Only untrusted peer-supplied roots at or above Heartwood require this. The
    /// checkpoint handoff is exempt because its embedded final frontiers are verified
    /// against this block's roots before the real tip treestate is written; trusted
    /// local fixtures can commit their tip root on the in-arrears check.
    pub(super) fn fast_root_needs_successor(
        &self,
        height: block::Height,
        network: &Network,
    ) -> bool {
        self.fast
            && self.fast_root(height).is_some()
            && self.requires_verified_successor
            && self
                .source
                .final_frontiers()
                .is_none_or(|frontiers| frontiers.height != height)
            && Some(height) >= NetworkUpgrade::Heartwood.activation_height(network)
    }

    /// Discard the supplied root for `height` after it failed verification, so a re-fetch
    /// can replace it. See
    /// [`CommitmentRootSource::invalidate`](super::commitment_aux::CommitmentRootSource::invalidate).
    pub(super) fn invalidate_fast_root(&self, height: block::Height) {
        self.source.invalidate(height);
    }

    /// Discard peer-supplied roots that are no longer needed after `height` has committed.
    pub(super) fn evict_committed_roots_through(&self, height: block::Height) {
        self.source.evict_committed_through(height);
    }

    /// The checkpoint handoff height: the boundary below which the fast path skips
    /// per-height note-commitment trees. `None` unless final frontiers are loaded.
    pub(super) fn fast_sync_handoff_height(&self) -> Option<block::Height> {
        self.source.handoff_height()
    }

    /// The verified `(sapling, orchard, sprout)` frontiers to write as the tip
    /// treestate, when `height` is the checkpoint handoff height.
    #[allow(clippy::type_complexity)]
    pub(super) fn final_frontiers_for_handoff(
        &self,
        height: block::Height,
    ) -> Option<(
        Arc<sapling::tree::NoteCommitmentTree>,
        Arc<orchard::tree::NoteCommitmentTree>,
        Arc<sprout::tree::NoteCommitmentTree>,
    )> {
        self.source
            .final_frontiers()
            .filter(|f| f.height == height)
            .map(|f| (f.sapling.clone(), f.orchard.clone(), f.sprout.clone()))
    }

    /// Record that a block took the fast (skip-recompute) path.
    pub(super) fn record_fast_block(&self) {
        self.fast_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a fast block whose own commitment check was skipped by the dedup.
    pub(super) fn record_prevalidated(&self) {
        self.prevalidated_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of blocks that took the fast path so far.
    pub(super) fn fast_count(&self) -> u64 {
        self.fast_count.load(Ordering::Relaxed)
    }

    /// Number of fast blocks whose own commitment check the dedup skipped.
    #[cfg(test)]
    pub(super) fn prevalidated_count(&self) -> u64 {
        self.prevalidated_count.load(Ordering::Relaxed)
    }

    /// Test-only: build fast-mode state from an arbitrary commitment-root source
    /// (e.g. a payload produced from a database), so the producer→consumer round-trip
    /// can be exercised without networking.
    #[cfg(test)]
    pub(super) fn test_with_source(
        source: Box<dyn CommitmentRootSource>,
        requires_verified_successor: bool,
    ) -> Arc<Self> {
        Arc::new(VctState {
            fast: true,
            source,
            requires_verified_successor,
            fast_count: AtomicU64::new(0),
            prevalidated_count: AtomicU64::new(0),
            peer_source_handle: None,
        })
    }
}

/// The verified final frontiers embedded for `network`, if supported.
///
/// Mainnet uses the constant embedded in the binary. Regtest has no fixed checkpoint —
/// its checkpoint list is derived at runtime from the mined chain — so there is no
/// committed frontier to embed; for deterministic e2e/integration testing of the fast
/// path on Regtest, the frontier is instead loaded from the file named by the
/// `VCT_REGTEST_FRONTIER` env var. This is scoped to **Regtest only** and validated
/// against the configured Regtest checkpoint height, so Mainnet always uses the
/// embedded constant and never reads the env. Other testnets have no frontier.
fn embedded_final_frontiers(network: &Network) -> Option<FinalFrontiers> {
    match network {
        Network::Mainnet => Some(parse_embedded_final_frontiers(
            MAINNET_FINAL_FRONTIERS,
            network.checkpoint_list().max_height(),
        )),
        Network::Testnet(params) if params.is_regtest() => {
            let path = std::env::var_os("VCT_REGTEST_FRONTIER")?;
            Some(load_frontier_file(
                path.as_ref(),
                network.checkpoint_list().max_height(),
            ))
        }
        Network::Testnet(_) => None,
    }
}

/// Load and validate a final-frontier fixture file (the Regtest path; see
/// [`embedded_final_frontiers`]). Separated from the env read so it is unit-testable
/// without mutating process environment variables.
fn load_frontier_file(path: &std::ffi::OsStr, expected_height: block::Height) -> FinalFrontiers {
    let bytes =
        std::fs::read(path).expect("VCT_REGTEST_FRONTIER must name a readable final-frontier file");
    parse_embedded_final_frontiers(&bytes, expected_height)
}

/// Parse embedded final frontiers and verify they match the checkpoint list.
fn parse_embedded_final_frontiers(bytes: &[u8], expected_height: block::Height) -> FinalFrontiers {
    parse_final_frontiers_bytes(bytes, expected_height).unwrap_or_else(|error| panic!("{error}"))
}

fn parse_final_frontiers_bytes(
    bytes: &[u8],
    expected_height: block::Height,
) -> Result<FinalFrontiers, FinalFrontiersValidationError> {
    let parsed = FinalFrontiers::from_bytes(bytes).map_err(|error| {
        FinalFrontiersValidationError::InvalidBytes {
            error: error.to_string(),
        }
    })?;

    if parsed.height != expected_height {
        return Err(FinalFrontiersValidationError::HeightMismatch {
            actual: parsed.height,
            expected: expected_height,
        });
    }

    Ok(parsed)
}

/// Validate serialized VCT final-frontier bytes against an expected checkpoint handoff height.
pub fn validate_final_frontiers_bytes(
    bytes: &[u8],
    expected_height: block::Height,
) -> Result<(), FinalFrontiersValidationError> {
    parse_final_frontiers_bytes(bytes, expected_height).map(|_| ())
}

/// Test/developer helper for producing embedded final-frontier bytes from a
/// legacy-computed tip treestate.
#[cfg(test)]
fn final_frontiers_bytes(height: block::Height, trees: &NoteCommitmentTrees) -> Vec<u8> {
    FinalFrontiers {
        height,
        sapling: trees.sapling.clone(),
        orchard: trees.orchard.clone(),
        sprout: trees.sprout.clone(),
    }
    .to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED_MAINNET_FINAL_SAPLING_ROOT: [u8; 32] = [
        5, 88, 219, 64, 134, 21, 57, 124, 234, 59, 83, 8, 7, 143, 19, 29, 247, 58, 105, 80, 119,
        139, 242, 243, 206, 137, 211, 94, 151, 126, 154, 13,
    ];
    const EXPECTED_MAINNET_FINAL_ORCHARD_ROOT: [u8; 32] = [
        177, 173, 139, 203, 63, 186, 47, 172, 148, 107, 150, 204, 211, 212, 33, 155, 172, 108, 132,
        148, 70, 210, 120, 97, 219, 160, 58, 242, 198, 124, 44, 3,
    ];
    const EXPECTED_MAINNET_FINAL_SPROUT_ROOT: [u8; 32] = [
        77, 239, 224, 205, 90, 67, 51, 216, 15, 139, 120, 78, 55, 17, 177, 22, 246, 34, 206, 184,
        49, 7, 97, 172, 28, 178, 69, 208, 13, 101, 55, 169,
    ];

    #[test]
    fn source_mode_precedence() {
        use SourceMode::*;
        // Args are (checkpoint_sync, has_embedded_frontiers).

        // The default: a checkpoint-trusting sync uses the peer source wherever embedded
        // frontiers exist (Mainnet). Storage mode (Archive/Pruned) is not an input, so this
        // covers both Archive and Pruned.
        assert_eq!(select_source_mode(true, true), Peer);
        // `checkpoint_sync = false` is the only mode that fully recomputes the trees: legacy,
        // never peer, regardless of embedded frontiers.
        assert_eq!(select_source_mode(false, true), Legacy);
        assert_eq!(select_source_mode(false, false), Legacy);
        // No embedded frontiers (e.g. Testnet): legacy, never peer, even under checkpoint sync.
        assert_eq!(select_source_mode(true, false), Legacy);
    }

    #[test]
    fn successor_policy_is_vct_state_data() {
        let network = Network::Mainnet;
        let height = NetworkUpgrade::Heartwood
            .activation_height(&network)
            .expect("mainnet has a Heartwood activation height");
        let root_map =
            || std::iter::once((height.0, (Default::default(), Default::default()))).collect();

        let trusted = VctState::test_with_source(
            Box::new(super::super::commitment_aux::FixtureSource::new(
                root_map(),
                None,
            )),
            false,
        );
        assert!(
            !trusted.fast_root_needs_successor(height, &network),
            "trusted fixture roots can commit without a buffered successor"
        );

        let untrusted = VctState::test_with_source(
            Box::new(super::super::commitment_aux::FixtureSource::new(
                root_map(),
                None,
            )),
            true,
        );
        assert!(
            untrusted.fast_root_needs_successor(height, &network),
            "untrusted roots defer until a buffered successor verifies them"
        );
    }

    #[test]
    fn fast_root_is_bounded_by_handoff_height() {
        let handoff = block::Height(10);
        let after_handoff = (handoff + 1).expect("test height is valid");
        let roots = std::collections::HashMap::from([
            (handoff.0, (Default::default(), Default::default())),
            (after_handoff.0, (Default::default(), Default::default())),
        ]);
        let frontiers = FinalFrontiers {
            height: handoff,
            sapling: Arc::new(sapling::tree::NoteCommitmentTree::default()),
            orchard: Arc::new(orchard::tree::NoteCommitmentTree::default()),
            sprout: Arc::new(sprout::tree::NoteCommitmentTree::default()),
        };

        let bounded = VctState::test_with_source(
            Box::new(super::super::commitment_aux::FixtureSource::new(
                roots.clone(),
                Some(frontiers),
            )),
            false,
        );
        assert!(
            bounded.fast_root(handoff).is_some(),
            "the handoff root remains fast-path eligible"
        );
        assert!(
            bounded.fast_root(after_handoff).is_none(),
            "roots above the handoff are ignored"
        );

        let unbounded = VctState::test_with_source(
            Box::new(super::super::commitment_aux::FixtureSource::new(
                roots, None,
            )),
            false,
        );
        assert!(
            unbounded.fast_root(after_handoff).is_some(),
            "sources without a handoff keep the existing fixture behavior"
        );
    }

    #[test]
    fn embedded_mainnet_final_frontiers_parse() {
        let frontiers = embedded_final_frontiers(&Network::Mainnet)
            .expect("mainnet has embedded final frontiers");

        assert_eq!(
            frontiers.height,
            Network::Mainnet.checkpoint_list().max_height(),
            "embedded frontier is tied to the last mainnet checkpoint"
        );
        assert_eq!(
            <[u8; 32]>::from(frontiers.sapling.root()),
            EXPECTED_MAINNET_FINAL_SAPLING_ROOT,
            "embedded mainnet final Sapling frontier root is pinned"
        );
        assert_eq!(
            <[u8; 32]>::from(frontiers.orchard.root()),
            EXPECTED_MAINNET_FINAL_ORCHARD_ROOT,
            "embedded mainnet final Orchard frontier root is pinned"
        );
        assert_eq!(
            <[u8; 32]>::from(frontiers.sprout.root()),
            EXPECTED_MAINNET_FINAL_SPROUT_ROOT,
            "embedded mainnet final Sprout frontier root is pinned"
        );
    }

    #[test]
    fn final_frontiers_capture_helper_serializes_tip_trees() {
        let height = block::Height(3_358_006);
        let trees = NoteCommitmentTrees::default();

        let parsed = FinalFrontiers::from_bytes(&final_frontiers_bytes(height, &trees))
            .expect("captured final frontiers should parse");

        assert_eq!(parsed.height, height, "captured height round-trips");
        assert_eq!(
            parsed.sapling.root(),
            trees.sapling.root(),
            "captured sapling frontier round-trips"
        );
        assert_eq!(
            parsed.orchard.root(),
            trees.orchard.root(),
            "captured orchard frontier round-trips"
        );
        assert_eq!(
            parsed.sprout.root(),
            trees.sprout.root(),
            "captured sprout frontier round-trips"
        );
    }

    #[test]
    #[should_panic(expected = "embedded VCT final frontier height must match")]
    fn embedded_final_frontiers_reject_checkpoint_height_mismatch() {
        let frontiers = FinalFrontiers {
            height: block::Height(1),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
        };

        let _ = parse_embedded_final_frontiers(&frontiers.to_bytes(), block::Height(2));
    }

    #[test]
    fn final_frontiers_parser_rejects_short_height() {
        let error =
            FinalFrontiers::from_bytes(&[0, 1, 2]).expect_err("short height should be rejected");

        assert_eq!(
            error.to_string(),
            "missing final frontier height: expected 4 bytes, got 3"
        );
    }

    #[test]
    fn final_frontiers_parser_rejects_missing_tree_length() {
        let bytes = block::Height(1).0.to_le_bytes();

        let error =
            FinalFrontiers::from_bytes(&bytes).expect_err("missing length should be rejected");

        assert_eq!(
            error.to_string(),
            "missing sapling frontier length prefix at byte 4: expected 4 bytes, got 0"
        );
    }

    #[test]
    fn final_frontiers_parser_rejects_truncated_tree_blob() {
        let mut bytes = block::Height(1).0.to_le_bytes().to_vec();
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&[0, 1]);

        let error =
            FinalFrontiers::from_bytes(&bytes).expect_err("truncated blob should be rejected");

        assert_eq!(
            error.to_string(),
            "truncated sapling frontier blob at byte 8: length prefix says 3 bytes, but only 2 remain"
        );
    }

    #[test]
    fn final_frontiers_parser_rejects_trailing_bytes() {
        let bytes = FinalFrontiers {
            height: block::Height(1),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
        }
        .to_bytes()
        .into_iter()
        .chain([0])
        .collect::<Vec<_>>();

        let error =
            FinalFrontiers::from_bytes(&bytes).expect_err("trailing bytes should be rejected");

        assert_eq!(
            error.to_string(),
            format!(
                "unexpected trailing final frontier bytes at byte {}: 1 bytes",
                bytes.len() - 1
            )
        );
    }

    #[test]
    #[should_panic(expected = "invalid VCT final frontier bytes: truncated sapling frontier blob")]
    fn embedded_final_frontiers_reject_malformed_bytes_with_context() {
        let mut bytes = block::Height(1).0.to_le_bytes().to_vec();
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&[0, 1]);

        let _ = parse_embedded_final_frontiers(&bytes, block::Height(1));
    }

    #[test]
    fn embedded_final_frontiers_are_network_specific() {
        assert!(
            embedded_final_frontiers(&Network::new_default_testnet()).is_none(),
            "testnet has no embedded final frontier until VCT fast sync supports it"
        );
    }

    /// The Regtest frontier-file loader (the `VCT_REGTEST_FRONTIER` path) round-trips a
    /// captured frontier and ties it to the expected checkpoint height — exercising the
    /// producer (`to_bytes`) → loader (`load_frontier_file`) seam without env vars.
    #[test]
    fn load_frontier_file_round_trips_a_captured_frontier() {
        let height = block::Height(123);
        let bytes = FinalFrontiers {
            height,
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
        }
        .to_bytes();

        let path =
            std::env::temp_dir().join(format!("vct-frontier-load-test-{}.bin", std::process::id()));
        std::fs::write(&path, &bytes).expect("write temp frontier file");

        let loaded = load_frontier_file(path.as_os_str(), height);
        assert_eq!(loaded.height, height, "loaded frontier height matches");
        assert_eq!(
            loaded.sapling.root(),
            sapling::tree::NoteCommitmentTree::default().root(),
            "loaded sapling frontier round-trips"
        );

        let _ = std::fs::remove_file(&path);
    }

    /// A frontier whose height does not match the checkpoint height is rejected, so a
    /// stale/wrong Regtest fixture cannot silently mis-seed the handoff.
    #[test]
    #[should_panic(expected = "embedded VCT final frontier height must match")]
    fn load_frontier_file_rejects_height_mismatch() {
        let bytes = FinalFrontiers {
            height: block::Height(5),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
        }
        .to_bytes();
        let path = std::env::temp_dir().join(format!(
            "vct-frontier-mismatch-test-{}.bin",
            std::process::id()
        ));
        std::fs::write(&path, &bytes).expect("write temp frontier file");

        let _ = load_frontier_file(path.as_os_str(), block::Height(6));
    }
}
