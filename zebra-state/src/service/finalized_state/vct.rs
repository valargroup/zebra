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

use super::{FromDisk, IntoDisk};

/// Byte length of one fixture record: height (u32 LE) + sapling root + orchard root.
const VCT_RECORD_LEN: usize = 4 + 32 + 32;

/// Embedded verified final note-commitment frontiers for Mainnet.
const MAINNET_FINAL_FRONTIERS: &[u8] = include_bytes!("vct/mainnet-frontier.bin");

/// The verified final note-commitment frontiers at the checkpoint handoff height,
/// embedded in the binary.
///
/// Fast mode skips the per-block frontier recompute below the checkpoint, so the
/// running sapling/orchard frontiers are never advanced. To let post-checkpoint
/// semantic verification resume, the real frontiers at the checkpoint are supplied
/// here, verified (`frontier.root() == the verified root at the checkpoint`), and
/// written as the tip treestate at the handoff. Subtree tips are not carried: the
/// resuming chain recomputes them from the frontier position.
#[derive(Clone, Debug)]
struct FinalFrontiers {
    height: block::Height,
    sapling: Arc<sapling::tree::NoteCommitmentTree>,
    orchard: Arc<orchard::tree::NoteCommitmentTree>,
    sprout: Arc<sprout::tree::NoteCommitmentTree>,
}

impl FinalFrontiers {
    /// Serialize to the embedded byte format: height (u32 LE), then sapling, orchard,
    /// and sprout trees, each as `u32`-LE-length-prefixed `IntoDisk` bytes.
    #[cfg(test)]
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.height.0.to_le_bytes());
        let blobs: [Vec<u8>; 3] = [
            IntoDisk::as_bytes(&*self.sapling),
            IntoDisk::as_bytes(&*self.orchard),
            IntoDisk::as_bytes(&*self.sprout),
        ];
        for blob in blobs {
            let len = u32::try_from(blob.len()).expect("note commitment tree fits in u32 bytes");
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&blob);
        }
        out
    }

    /// Parse the embedded byte format.
    fn from_bytes(bytes: &[u8]) -> Self {
        let height = block::Height(u32::from_le_bytes(
            bytes[0..4].try_into().expect("4 bytes for height"),
        ));

        // Read three `u32`-length-prefixed blobs starting after the height.
        let mut cursor = 4;
        let mut next_blob = |bytes: &[u8]| -> Vec<u8> {
            let len = u32::from_le_bytes(
                bytes[cursor..cursor + 4]
                    .try_into()
                    .expect("4 bytes for length"),
            ) as usize;
            cursor += 4;
            let blob = bytes[cursor..cursor + len].to_vec();
            cursor += len;
            blob
        };
        let sapling = next_blob(bytes);
        let orchard = next_blob(bytes);
        let sprout = next_blob(bytes);

        FinalFrontiers {
            height,
            sapling: Arc::new(<sapling::tree::NoteCommitmentTree as FromDisk>::from_bytes(
                sapling,
            )),
            orchard: Arc::new(<orchard::tree::NoteCommitmentTree as FromDisk>::from_bytes(
                orchard,
            )),
            sprout: Arc::new(<sprout::tree::NoteCommitmentTree as FromDisk>::from_bytes(
                sprout,
            )),
        }
    }
}

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
    /// Fast mode: skip the per-block frontier recompute and fold `roots` into the
    /// anchor set + history tree.
    fast: bool,
    /// Fixture roots by height (fast mode only).
    roots: HashMap<u32, (sapling::tree::Root, orchard::tree::Root)>,
    /// Capture sink: append `(height, sapling_root, orchard_root)` per committed
    /// checkpoint block, to record a fixture during a legacy sync.
    capture: Option<Mutex<std::io::BufWriter<File>>>,
    /// Count of blocks that took the fast (skip-recompute) path, for the run summary.
    fast_count: AtomicU64,
    /// Count of fast blocks whose own commitment check was skipped because the
    /// previous block's look-ahead already validated it (the dedup). Lets tests
    /// assert the dedup actually engages, so it can't be silently regressed.
    prevalidated_count: AtomicU64,
    /// Verified final frontiers at the checkpoint handoff height (fast mode), loaded
    /// from embedded network data. When present, the fast path marks the database
    /// as fast-synced (boundary = `height`) and writes the real treestate at the
    /// handoff so semantic verification can resume.
    frontiers: Option<FinalFrontiers>,
}

impl VctState {
    /// Build the POC state from the config flag and the `VCT_FIXTURE` /
    /// `VCT_CAPTURE` environment variables. Returns `None` when neither
    /// capture nor fast mode is requested (the default), so there is zero overhead.
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

        let mut roots = HashMap::new();
        let mut fast = false;
        let mut frontiers = None;
        if fast_flag {
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
            for rec in bytes.chunks_exact(VCT_RECORD_LEN) {
                let height = u32::from_le_bytes(rec[0..4].try_into().expect("4 bytes"));
                let sap = <sapling::tree::Root as FromDisk>::from_bytes(&rec[4..36]);
                let orch = <orchard::tree::Root as FromDisk>::from_bytes(&rec[36..68]);
                roots.insert(height, (sap, orch));
            }
            fast = true;

            let parsed = embedded_final_frontiers(network).unwrap_or_else(|| {
                panic!("VCT fast mode requires embedded final frontiers for {network}")
            });
            tracing::info!(
                handoff_height = parsed.height.0,
                "VCT: loaded embedded final frontiers, checkpoint handoff enabled"
            );
            frontiers = Some(parsed);

            tracing::info!(
                fixture_roots = roots.len(),
                "VCT: loaded fixture, fast (skip-recompute) mode enabled"
            );
        }

        if capture.is_none() && !fast {
            return None;
        }
        if capture.is_some() {
            tracing::info!("VCT: capture mode enabled (recording per-block roots)");
        }
        Some(Arc::new(VctState {
            fast,
            roots,
            capture,
            fast_count: AtomicU64::new(0),
            prevalidated_count: AtomicU64::new(0),
            frontiers,
        }))
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
        self.roots.get(&height.0).copied()
    }

    /// The checkpoint handoff height: the boundary below which the fast path skips
    /// per-height note-commitment trees. `None` unless final frontiers are loaded.
    pub(super) fn fast_sync_handoff_height(&self) -> Option<block::Height> {
        self.frontiers.as_ref().map(|f| f.height)
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
        self.frontiers
            .as_ref()
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

    /// Test-only: in-memory fixture (instead of the `VCT_FIXTURE` file), no handoff.
    #[cfg(test)]
    pub(super) fn test_fixture(
        roots: HashMap<u32, (sapling::tree::Root, orchard::tree::Root)>,
    ) -> Arc<Self> {
        Arc::new(VctState {
            fast: true,
            roots,
            capture: None,
            fast_count: AtomicU64::new(0),
            prevalidated_count: AtomicU64::new(0),
            frontiers: None,
        })
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
        Arc::new(VctState {
            fast: true,
            roots,
            capture: None,
            fast_count: AtomicU64::new(0),
            prevalidated_count: AtomicU64::new(0),
            frontiers: Some(FinalFrontiers {
                height: handoff_height,
                sapling,
                orchard,
                sprout,
            }),
        })
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

    /// The embedded frontier serialization round-trips: the parsed frontiers
    /// carry the same height and tree roots as the originals. (The handoff write
    /// path is covered end-to-end by `vct_fast_sync_handoff_marks_database_and_resumes`
    /// and the real-mainnet e2e; this isolates the byte format.)
    #[test]
    fn final_frontiers_bytes_round_trips() {
        let frontiers = FinalFrontiers {
            height: block::Height(1_687_200),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
        };

        let parsed = FinalFrontiers::from_bytes(&frontiers.to_bytes());

        assert_eq!(parsed.height, frontiers.height, "height round-trips");
        assert_eq!(
            parsed.sapling.root(),
            frontiers.sapling.root(),
            "sapling frontier round-trips"
        );
        assert_eq!(
            parsed.orchard.root(),
            frontiers.orchard.root(),
            "orchard frontier round-trips"
        );
        assert_eq!(
            parsed.sprout.root(),
            frontiers.sprout.root(),
            "sprout frontier round-trips"
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
