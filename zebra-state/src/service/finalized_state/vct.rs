//! Verified-commitment-trees fast-sync experiment state (POC harness).
//!
//! This module holds the fixture/embedded-frontier *plumbing* for the
//! verified-commitment-trees fast path: loading the per-block roots fixture
//! (`VCT_FIXTURE`), loading the final frontiers embedded in the binary, capturing
//! per-block roots during a legacy sync, and the run counters. On networks with an
//! embedded handoff frontier, the default source is the peer `tree_aux` source; the
//! `VCT_*` environment variables select local fixture/capture modes or opt out to
//! legacy recompute.
//!
//! [`super`] (`finalized_state.rs`) holds only the commit-path hook (the checkpoint
//! handoff write and the fast-sync marker); everything about *where the data comes
//! from* lives here, behind a small method API so the commit path never touches the
//! experiment's internals.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

#[cfg(test)]
use zebra_chain::parallel::tree::NoteCommitmentTrees;
use zebra_chain::{block, orchard, parameters::Network, sapling, sprout};

use super::{
    commitment_aux::{install_peer_source, CommitmentRootSource, FinalFrontiers, FixtureSource},
    FromDisk, IntoDisk,
};

/// Byte length of one fixture record: height (u32 LE) + sapling root + orchard root.
const VCT_RECORD_LEN: usize = 4 + 32 + 32;

/// Embedded verified final note-commitment frontiers for Mainnet.
const MAINNET_FINAL_FRONTIERS: &[u8] = include_bytes!("vct/mainnet-frontier.bin");

/// POC state for the verified-commitment-trees experiment
/// (`docs/design/verified-commitment-trees-poc.md`). Shared across
/// [`super::FinalizedState`] clones via `Arc` so the capture sink and counters are
/// shared.
///
/// A checkpoint-trusting sync (`checkpoint_sync = true`) uses the peer `tree_aux` source by
/// default on networks with embedded final frontiers; `checkpoint_sync = false` opts out to
/// the legacy per-block recompute (no VCT state). `VCT_FAST` selects the local fixture source
/// and `VCT_CAPTURE` records fixtures — both test-only overrides.
#[derive(Debug)]
pub(crate) struct VctState {
    /// Fast mode: skip the per-block frontier recompute and fold the source's roots
    /// into the anchor set + history tree.
    fast: bool,
    /// Where the verified per-block roots and handoff frontiers come from. Today a
    /// fixture/embedded [`FixtureSource`]; a transport-backed peer source later. The
    /// committer reads roots/handoff/frontiers through this seam only.
    source: Box<dyn CommitmentRootSource>,
    /// Capture sink: append `(height, sapling_root, orchard_root)` per committed
    /// checkpoint block, to record a fixture during a legacy sync.
    capture: Option<Mutex<std::io::BufWriter<File>>>,
    /// Count of blocks that took the fast (skip-recompute) path, for the run summary.
    fast_count: AtomicU64,
    /// Count of fast blocks whose own commitment check was skipped because the
    /// previous block's look-ahead already validated it (the dedup). Lets tests
    /// assert the dedup actually engages, so it can't be silently regressed.
    prevalidated_count: AtomicU64,
    /// Capture the handoff frontier: when the (legacy-path) committer commits the block
    /// at the target height, dump the tip treestate frontier to the file. Used to
    /// generate a Regtest frontier fixture for `VCT_REGTEST_FRONTIER`. `(path, target)`.
    capture_frontier: Option<(std::path::PathBuf, block::Height)>,
}

/// Read the `VCT_CAPTURE_FRONTIER` (output path) + `VCT_CAPTURE_FRONTIER_HEIGHT` (the
/// checkpoint height to capture at) env vars, if set. Test/harness plumbing only.
fn capture_frontier_from_env() -> Option<(std::path::PathBuf, block::Height)> {
    let path = std::env::var_os("VCT_CAPTURE_FRONTIER")?;
    let height: u32 = std::env::var("VCT_CAPTURE_FRONTIER_HEIGHT")
        .expect("VCT_CAPTURE_FRONTIER requires VCT_CAPTURE_FRONTIER_HEIGHT")
        .parse()
        .expect("VCT_CAPTURE_FRONTIER_HEIGHT must be a u32 height");
    Some((std::path::PathBuf::from(path), block::Height(height)))
}

/// Which commitment-root source the committer uses, resolved from the (already read)
/// configuration signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceMode {
    /// Legacy recompute committer (no VCT state).
    Legacy,
    /// Replay a local fixture (explicit `VCT_FAST` + `VCT_FIXTURE`).
    Fixture,
    /// Legacy commit that records per-block roots (explicit `VCT_CAPTURE`).
    Capture,
    /// Fetch per-block roots from peers — the default where embedded frontiers exist.
    Peer,
}

/// Resolve the source precedence as a pure function, so the order — and in particular the
/// peer-source default — is unit-testable without touching process environment variables
/// or the embedded-frontier files. The test-only fixture override wins, then the test-only
/// capture override, then the config-driven decision: the fast verified path (peer source) is
/// the default whenever the node syncs under checkpoint trust and the network has an embedded
/// handoff frontier. `checkpoint_sync = false` is the only mode that fully reconstructs the
/// note-commitment trees per block, so it selects the legacy recompute; a network with no
/// embedded frontier also falls back to legacy (there is nothing to hand off to). Storage mode
/// (Archive vs. Pruned) is orthogonal and not an input here.
fn select_source_mode(
    checkpoint_sync: bool,
    fixture: bool,
    capture: bool,
    has_embedded_frontiers: bool,
) -> SourceMode {
    if fixture {
        SourceMode::Fixture
    } else if capture {
        SourceMode::Capture
    } else if !checkpoint_sync || !has_embedded_frontiers {
        SourceMode::Legacy
    } else {
        SourceMode::Peer
    }
}

impl VctState {
    /// Build the committer state from `checkpoint_sync` (the mirror of
    /// `consensus.checkpoint_sync`) plus the test-only `VCT_FAST` / `VCT_FIXTURE` /
    /// `VCT_CAPTURE` environment overrides. On networks with an embedded handoff frontier
    /// (Mainnet) a checkpoint-trusting sync defaults to the peer (`tree_aux`) fast source;
    /// `checkpoint_sync = false` (or a network without an embedded frontier) returns `None`
    /// for a zero-overhead legacy committer that recomputes the trees per block.
    #[allow(clippy::unwrap_in_result)] // misconfiguration / unreadable fixture should fail loudly
    pub(super) fn from_config(checkpoint_sync: bool, network: &Network) -> Option<Arc<Self>> {
        // Test-only override: replay a local fixture instead of fetching roots from peers,
        // without standing up the network driver.
        let fixture = std::env::var_os("VCT_FAST").is_some();

        let capture = std::env::var_os("VCT_CAPTURE").map(|path| {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .expect("VCT_CAPTURE path must be writable");
            Mutex::new(std::io::BufWriter::new(file))
        });

        // Frontier-capture (harness fixture generation): needs the legacy recompute to
        // have the real tip treestate, so it implies capture mode like `VCT_CAPTURE`.
        let capture_frontier = capture_frontier_from_env();
        // Parse the embedded handoff frontier once (None on networks without one, e.g.
        // Testnet). The decision below only needs its presence; the fixture/peer arms reuse
        // the parsed value.
        let embedded = embedded_final_frontiers(network);

        match select_source_mode(
            checkpoint_sync,
            fixture,
            capture.is_some() || capture_frontier.is_some(),
            embedded.is_some(),
        ) {
            // Fixture fast mode (explicit): replay per-block roots recorded in a local
            // fixture (`VCT_FAST` + `VCT_FIXTURE`) instead of fetching from peers.
            SourceMode::Fixture => {
                let path = std::env::var_os("VCT_FIXTURE")
                    .expect("VCT_FAST fixture override requires VCT_FIXTURE");
                let mut bytes = Vec::new();
                File::open(&path)
                    .expect("VCT_FIXTURE must exist")
                    .read_to_end(&mut bytes)
                    .expect("VCT_FIXTURE read failed");
                assert_eq!(
                    bytes.len() % VCT_RECORD_LEN,
                    0,
                    "corrupt VCT fixture: length not a multiple of {VCT_RECORD_LEN}"
                );
                let mut roots = HashMap::new();
                for rec in bytes.chunks_exact(VCT_RECORD_LEN) {
                    let height = u32::from_le_bytes(rec[0..4].try_into().expect("4 bytes"));
                    let sap = <sapling::tree::Root as FromDisk>::from_bytes(&rec[4..36]);
                    let orch = <orchard::tree::Root as FromDisk>::from_bytes(&rec[36..68]);
                    roots.insert(height, (sap, orch));
                }
                let parsed = embedded.unwrap_or_else(|| {
                    panic!("VCT fast mode requires embedded final frontiers for {network}")
                });
                tracing::info!(
                    handoff_height = parsed.height.0,
                    fixture_roots = roots.len(),
                    "VCT: loaded fixture + embedded final frontiers, fast (skip-recompute) mode enabled"
                );
                Some(Arc::new(VctState {
                    fast: true,
                    source: Box::new(FixtureSource::new(roots, Some(parsed))),
                    capture,
                    fast_count: AtomicU64::new(0),
                    prevalidated_count: AtomicU64::new(0),
                    capture_frontier: None,
                }))
            }

            // Capture mode (explicit): a legacy sync that records each committed block's
            // roots to a fixture. Recording requires the legacy recompute, so it overrides
            // the peer-source default (which would skip the recompute and capture nothing).
            SourceMode::Capture => {
                tracing::info!(
                    "VCT: capture mode enabled (recording per-block roots, legacy commit)"
                );
                Some(Arc::new(VctState {
                    fast: false,
                    source: Box::new(FixtureSource::new(HashMap::new(), None)),
                    capture,
                    fast_count: AtomicU64::new(0),
                    prevalidated_count: AtomicU64::new(0),
                    capture_frontier,
                }))
            }

            // Default: the peer (`tree_aux`) source on any network with embedded final
            // frontiers (Mainnet). Per-block roots arrive from peers into a shared cache
            // filled by the driver; the committer reads them per height and folds them in,
            // skipping the recompute. A height the peer cannot supply — or any node with no
            // serving peers — stays in legacy mode, bit-identical to a legacy committer by
            // construction (the precompute overlap is preserved for those blocks; see
            // `vct_fast_will_apply`).
            SourceMode::Peer => {
                let parsed = embedded
                    .expect("peer mode is only selected when embedded frontiers are present");
                tracing::info!(
                    handoff_height = parsed.height.0,
                    "VCT: peer (tree_aux) source enabled by default — roots fetched from peers"
                );
                Some(Arc::new(VctState {
                    fast: true,
                    source: Box::new(install_peer_source(Some(parsed))),
                    capture: None,
                    fast_count: AtomicU64::new(0),
                    prevalidated_count: AtomicU64::new(0),
                    capture_frontier: None,
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

    /// The supplied roots for `height`, when fast mode has a fixture entry for it
    /// (the signal that this block takes the fast path).
    pub(super) fn fast_root(
        &self,
        height: block::Height,
    ) -> Option<(sapling::tree::Root, orchard::tree::Root)> {
        if !self.fast {
            return None;
        }
        self.source.fast_root(height)
    }

    /// Discard the supplied root for `height` after it failed verification, so a re-fetch
    /// can replace it (no-op for a trusted local fixture). See
    /// [`CommitmentRootSource::invalidate`](super::commitment_aux::CommitmentRootSource::invalidate).
    pub(super) fn invalidate_fast_root(&self, height: block::Height) {
        self.source.invalidate(height);
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

    /// Append a captured per-block roots record for `height` (no-op outside capture mode).
    pub(super) fn capture_per_height_roots(
        &self,
        height: u32,
        sapling_root: &sapling::tree::Root,
        orchard_root: &orchard::tree::Root,
    ) {
        if let Some(sink) = &self.capture {
            let mut buf = [0u8; VCT_RECORD_LEN];
            buf[0..4].copy_from_slice(&height.to_le_bytes());
            buf[4..36].copy_from_slice(IntoDisk::as_bytes(sapling_root).as_ref());
            buf[36..68].copy_from_slice(IntoDisk::as_bytes(orchard_root).as_ref());
            let mut sink = sink.lock().expect("VCT capture mutex poisoned");
            sink.write_all(&buf).expect("VCT capture write failed");
        }
    }

    /// When `height` is the configured capture target, dump the tip treestate frontier
    /// to the `VCT_CAPTURE_FRONTIER` file (no-op otherwise). Generates the Regtest frontier
    /// fixture that `VCT_REGTEST_FRONTIER` loads. Called on the legacy commit path, where
    /// the supplied trees are the real tip treestate at `height`.
    pub(super) fn capture_frontier_at(
        &self,
        height: block::Height,
        sapling: &Arc<sapling::tree::NoteCommitmentTree>,
        orchard: &Arc<orchard::tree::NoteCommitmentTree>,
        sprout: &Arc<sprout::tree::NoteCommitmentTree>,
    ) {
        let Some((path, target)) = &self.capture_frontier else {
            return;
        };
        if height != *target {
            return;
        }
        let bytes = FinalFrontiers {
            height,
            sapling: sapling.clone(),
            orchard: orchard.clone(),
            sprout: sprout.clone(),
        }
        .to_bytes();
        std::fs::write(path, bytes).expect("VCT_CAPTURE_FRONTIER write failed");
        tracing::info!(
            height = height.0,
            "VCT: captured final frontier fixture to file"
        );
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

    /// Flush the capture sink so the fixture file is complete before exit.
    pub(super) fn flush_capture(&self) {
        if let Some(sink) = &self.capture {
            let _ = sink.lock().expect("VCT capture mutex poisoned").flush();
        }
    }

    /// Test-only: build fast-mode state from an arbitrary commitment-root source
    /// (e.g. a payload produced from a database), so the producer→consumer round-trip
    /// can be exercised without the `VCT_FIXTURE` file or networking.
    #[cfg(test)]
    pub(super) fn test_with_source(source: Box<dyn CommitmentRootSource>) -> Arc<Self> {
        Arc::new(VctState {
            fast: true,
            source,
            capture: None,
            fast_count: AtomicU64::new(0),
            prevalidated_count: AtomicU64::new(0),
            capture_frontier: None,
        })
    }

    /// Test-only: in-memory fixture (instead of the `VCT_FIXTURE` file), no handoff.
    #[cfg(test)]
    pub(super) fn test_fixture(
        roots: HashMap<u32, (sapling::tree::Root, orchard::tree::Root)>,
    ) -> Arc<Self> {
        Self::test_with_source(Box::new(FixtureSource::new(roots, None)))
    }

    /// Test-only: like [`Self::test_fixture`], but also supplies the final frontiers
    /// at `handoff_height`, so the checkpoint handoff (verify + write the real
    /// treestate + set the fast-sync marker) can be unit tested.
    #[cfg(test)]
    pub(super) fn test_fixture_with_handoff(
        roots: HashMap<u32, (sapling::tree::Root, orchard::tree::Root)>,
        handoff_height: block::Height,
        sapling: Arc<sapling::tree::NoteCommitmentTree>,
        orchard: Arc<orchard::tree::NoteCommitmentTree>,
        sprout: Arc<sprout::tree::NoteCommitmentTree>,
    ) -> Arc<Self> {
        let frontiers = FinalFrontiers {
            height: handoff_height,
            sapling,
            orchard,
            sprout,
        };
        Self::test_with_source(Box::new(FixtureSource::new(roots, Some(frontiers))))
    }
}

/// The verified final frontiers embedded for `network`, if supported.
///
/// Mainnet uses the constant embedded in the binary. Regtest has no fixed checkpoint —
/// its checkpoint list is derived at runtime from the mined chain — so there is no
/// committed frontier to embed; for deterministic e2e/integration testing of the fast
/// path on Regtest, the frontier is instead loaded from the file named by the
/// `VCT_REGTEST_FRONTIER` env var (the harness generates it from a synced node's tip
/// treestate via [`VctState::capture_frontier_at`]). This is scoped to **Regtest only**
/// and validated against the configured Regtest checkpoint height, so Mainnet always uses
/// the embedded constant and never reads the env. Other testnets have no frontier.
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
    let parsed = FinalFrontiers::from_bytes(bytes);
    assert_eq!(
        parsed.height, expected_height,
        "embedded VCT final frontier height must match the network's max checkpoint height"
    );
    parsed
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

    #[test]
    fn source_mode_precedence() {
        use SourceMode::*;
        // Args are (checkpoint_sync, fixture, capture, has_embedded_frontiers).

        // The default: a checkpoint-trusting sync uses the peer source wherever embedded
        // frontiers exist (Mainnet). Storage mode (Archive/Pruned) is not an input, so this
        // covers both Archive and Pruned.
        assert_eq!(select_source_mode(true, false, false, true), Peer);
        // `checkpoint_sync = false` is the only mode that fully recomputes the trees: legacy,
        // never peer, regardless of embedded frontiers.
        assert_eq!(select_source_mode(false, false, false, true), Legacy);
        assert_eq!(select_source_mode(false, false, false, false), Legacy);
        // No embedded frontiers (e.g. Testnet): legacy, never peer, even under checkpoint sync.
        assert_eq!(select_source_mode(true, false, false, false), Legacy);
        // Capture (test override) wins over the peer default (recording needs the recompute).
        assert_eq!(select_source_mode(true, false, true, true), Capture);
        // Fixture (test override) takes precedence over capture and the peer default…
        assert_eq!(select_source_mode(true, true, false, true), Fixture);
        assert_eq!(select_source_mode(true, true, true, true), Fixture);
        // …and applies even when checkpoint_sync is off, since it is an explicit request.
        assert_eq!(select_source_mode(false, true, false, true), Fixture);
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
        let _sapling_root = frontiers.sapling.root();
        let _orchard_root = frontiers.orchard.root();
        let _sprout_root = frontiers.sprout.root();
    }

    #[test]
    fn final_frontiers_capture_helper_serializes_tip_trees() {
        let height = block::Height(3_358_006);
        let trees = NoteCommitmentTrees::default();

        let parsed = FinalFrontiers::from_bytes(&final_frontiers_bytes(height, &trees));

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
