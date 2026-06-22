//! Verified-commitment-trees fast-sync experiment state (POC harness).
//!
//! This module holds the fixture/embedded-frontier *plumbing* for the
//! verified-commitment-trees fast path: loading the per-block roots fixture
//! (`VCT_FIXTURE`), loading the final frontiers embedded in the binary, capturing
//! per-block roots during a legacy sync, and the run counters. It is gated behind
//! `Config::enable_verified_commitment_trees` / the `VCT_*` environment variables and
//! is **experiment scaffolding, not a shippable feature** — the local files stand in
//! for the eventual `tree_aux` peer source.
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
/// This is gated behind `Config::enable_verified_commitment_trees` (fast mode) and
/// the `VCT_FIXTURE` / `VCT_CAPTURE` environment variables. It is an experiment that
/// trusts a recorded fixture and is NOT a shippable feature.
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
/// or the embedded-frontier files. Explicit fixture wins, then explicit capture, then the
/// peer-source default (unless opted out, or the network has no embedded frontiers).
fn select_source_mode(
    fast_flag: bool,
    capture: bool,
    legacy_opt_out: bool,
    has_embedded_frontiers: bool,
) -> SourceMode {
    if fast_flag {
        SourceMode::Fixture
    } else if capture {
        SourceMode::Capture
    } else if legacy_opt_out || !has_embedded_frontiers {
        SourceMode::Legacy
    } else {
        SourceMode::Peer
    }
}

impl VctState {
    /// Build the POC state from the config flag and the `VCT_FIXTURE` / `VCT_CAPTURE` /
    /// `VCT_LEGACY` environment variables. On networks with an embedded handoff frontier
    /// (Mainnet) the default is the peer (`tree_aux`) source; `VCT_LEGACY` (or a network
    /// without an embedded frontier) returns `None` for a zero-overhead legacy committer.
    #[allow(clippy::unwrap_in_result)] // misconfiguration / unreadable fixture should fail loudly
    pub(super) fn from_config(fast_flag: bool, network: &Network) -> Option<Arc<Self>> {
        // The config flag is `serde(skip)`, so for the POC harness also honor an
        // env override to enable fast mode without TOML/zebrad plumbing.
        let fast_flag = fast_flag || std::env::var_os("VCT_FAST").is_some();

        let capture = std::env::var_os("VCT_CAPTURE").map(|path| {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .expect("VCT_CAPTURE path must be writable");
            Mutex::new(std::io::BufWriter::new(file))
        });

        let legacy_opt_out = std::env::var_os("VCT_LEGACY").is_some();
        // Parse the embedded handoff frontier once (None on networks without one, e.g.
        // Testnet). The decision below only needs its presence; the fixture/peer arms reuse
        // the parsed value.
        let embedded = embedded_final_frontiers(network);

        match select_source_mode(
            fast_flag,
            capture.is_some(),
            legacy_opt_out,
            embedded.is_some(),
        ) {
            // Fixture fast mode (explicit): replay per-block roots recorded in a local
            // fixture (`VCT_FAST` + `VCT_FIXTURE`) instead of fetching from peers.
            SourceMode::Fixture => {
                let path = std::env::var_os("VCT_FIXTURE")
                    .expect("enable_verified_commitment_trees requires VCT_FIXTURE");
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
                }))
            }

            // Legacy committer: `VCT_LEGACY` opt-out, or a network with no embedded
            // frontiers. No VCT state, zero overhead.
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
    pub(super) fn capture(
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
fn embedded_final_frontiers(network: &Network) -> Option<FinalFrontiers> {
    match network {
        Network::Mainnet => Some(parse_embedded_final_frontiers(
            MAINNET_FINAL_FRONTIERS,
            network.checkpoint_list().max_height(),
        )),
        Network::Testnet(_) => None,
    }
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

        // The flipped default: peer source wherever embedded frontiers exist (Mainnet).
        assert_eq!(select_source_mode(false, false, false, true), Peer);
        // No embedded frontiers (e.g. Testnet): legacy, never peer.
        assert_eq!(select_source_mode(false, false, false, false), Legacy);
        // `VCT_LEGACY` opt-out forces legacy even where the peer default would apply.
        assert_eq!(select_source_mode(false, false, true, true), Legacy);
        // Capture overrides the peer default (recording needs the legacy recompute).
        assert_eq!(select_source_mode(false, true, false, true), Capture);
        // Fixture fast mode takes precedence over capture and the peer default…
        assert_eq!(select_source_mode(true, false, false, true), Fixture);
        assert_eq!(select_source_mode(true, true, false, true), Fixture);
        // …and over the legacy opt-out, since it is an explicit request.
        assert_eq!(select_source_mode(true, false, true, true), Fixture);
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
}
