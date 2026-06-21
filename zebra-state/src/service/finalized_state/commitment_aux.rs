//! Commitment-root source seam and payload types for the verified-commitment-trees
//! fast path (`docs/design/verified-commitment-trees.md` §5, increment 3).
//!
//! The fast path consumes per-block Sapling/Orchard roots and a final frontier at the
//! checkpoint handoff. *Where* that data comes from is abstracted behind
//! [`CommitmentRootSource`], so the committer reads through one seam regardless of
//! source. Today the only source is the fixture/embedded scaffolding ([`FixtureSource`],
//! loaded in [`super::vct`]); the destination is a transport-backed `PeerSource` over
//! `tree_aux` (increment 6a). This is *not* a new mode — it is the same fast verified
//! path, with its source factored out.
//!
//! It also provides the **producer** half ([`produce_block_roots`] /
//! [`produce_final_frontiers`]): deriving the same payload from an existing database's
//! per-height trees. That is the read path a serving node runs, and it lets a
//! DB-produced payload be fed back through the fast path in-process (the round-trip
//! that proves producer and consumer agree, with no networking).

use std::{collections::HashMap, sync::Arc};

use zebra_chain::{block, orchard, sapling, sprout};

#[cfg(test)]
use super::IntoDisk;
use super::{FromDisk, ZebraDb};

/// Per-block verified commitment roots — the essential fast-path payload
/// (design §5.1). One entry per height; the root is the treestate root as of
/// end-of-block-`height`.
// Produced/consumed by the round-trip test this increment; becomes the wire payload
// over `tree_aux` in increment 6a.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(super) struct BlockCommitmentRoots {
    pub(super) height: block::Height,
    pub(super) sapling_root: sapling::tree::Root,
    pub(super) orchard_root: orchard::tree::Root,
}

/// The verified final note-commitment frontiers at the checkpoint handoff height
/// (design §5.2).
///
/// Fast mode skips the per-block frontier recompute below the checkpoint, so the
/// running Sapling/Orchard frontiers are never advanced. To let post-checkpoint
/// semantic verification resume, the real frontiers at the checkpoint are supplied
/// here, verified (`frontier.root() == the verified root at the checkpoint`), and
/// written as the tip treestate at the handoff. Subtree tips are not carried: the
/// resuming chain recomputes them from the frontier position.
#[derive(Clone, Debug)]
pub(super) struct FinalFrontiers {
    pub(super) height: block::Height,
    pub(super) sapling: Arc<sapling::tree::NoteCommitmentTree>,
    pub(super) orchard: Arc<orchard::tree::NoteCommitmentTree>,
    pub(super) sprout: Arc<sprout::tree::NoteCommitmentTree>,
}

impl FinalFrontiers {
    /// Serialize to the embedded byte format: height (u32 LE), then sapling, orchard,
    /// and sprout trees, each as `u32`-LE-length-prefixed `IntoDisk` bytes.
    #[cfg(test)]
    pub(super) fn to_bytes(&self) -> Vec<u8> {
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

    /// Parse the embedded byte format written by [`Self::to_bytes`].
    pub(super) fn from_bytes(bytes: &[u8]) -> Self {
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

/// Where the fast path's verified per-block roots and handoff frontiers come from.
///
/// One enduring seam, two enduring data paths: the standard/legacy path rebuilds
/// trees locally and never consults a source; the fast verified path reads roots
/// from *some* source and verifies them against the headers. [`FixtureSource`] is
/// today's scaffolding behind this trait; a transport-backed `PeerSource` over
/// `tree_aux` replaces it later (increment 6a). The trait carries no trust: every
/// supplied root is verified against the checkpoint-committed headers before commit.
pub(super) trait CommitmentRootSource: std::fmt::Debug + Send + Sync {
    /// The supplied roots for `height`, if this source has them.
    fn fast_root(
        &self,
        height: block::Height,
    ) -> Option<(sapling::tree::Root, orchard::tree::Root)>;

    /// The checkpoint handoff height (below which the fast path skips per-height
    /// trees), if this source supplies a final frontier.
    fn handoff_height(&self) -> Option<block::Height>;

    /// The verified final frontiers at the handoff height, if supplied.
    fn final_frontiers(&self) -> Option<&FinalFrontiers>;
}

/// The shared in-memory representation behind the concrete sources: a height→roots
/// map plus the optional handoff frontiers.
#[derive(Debug, Default)]
struct RootMap {
    roots: HashMap<u32, (sapling::tree::Root, orchard::tree::Root)>,
    frontiers: Option<FinalFrontiers>,
}

impl RootMap {
    fn fast_root(
        &self,
        height: block::Height,
    ) -> Option<(sapling::tree::Root, orchard::tree::Root)> {
        self.roots.get(&height.0).copied()
    }

    fn handoff_height(&self) -> Option<block::Height> {
        self.frontiers.as_ref().map(|f| f.height)
    }

    fn final_frontiers(&self) -> Option<&FinalFrontiers> {
        self.frontiers.as_ref()
    }
}

/// Today's scaffolding source: roots loaded from the `VCT_FIXTURE` file and frontiers
/// embedded in the binary (see [`super::vct`]). Stands in for the eventual peer source.
#[derive(Debug)]
pub(super) struct FixtureSource(RootMap);

impl FixtureSource {
    pub(super) fn new(
        roots: HashMap<u32, (sapling::tree::Root, orchard::tree::Root)>,
        frontiers: Option<FinalFrontiers>,
    ) -> Self {
        FixtureSource(RootMap { roots, frontiers })
    }
}

impl CommitmentRootSource for FixtureSource {
    fn fast_root(
        &self,
        height: block::Height,
    ) -> Option<(sapling::tree::Root, orchard::tree::Root)> {
        self.0.fast_root(height)
    }
    fn handoff_height(&self) -> Option<block::Height> {
        self.0.handoff_height()
    }
    fn final_frontiers(&self) -> Option<&FinalFrontiers> {
        self.0.final_frontiers()
    }
}

/// An in-memory source built from a produced payload (a `Vec<BlockCommitmentRoots>`
/// plus optional handoff frontiers). Adapts producer output back into a
/// [`CommitmentRootSource`] for the in-process round-trip and, later, the peer driver.
// Exercised by the round-trip test this increment; the peer driver uses it in 6a.
#[allow(dead_code)]
#[derive(Debug)]
pub(super) struct VecRootSource(RootMap);

#[allow(dead_code)]
impl VecRootSource {
    pub(super) fn from_payload(
        roots: Vec<BlockCommitmentRoots>,
        frontiers: Option<FinalFrontiers>,
    ) -> Self {
        let roots = roots
            .into_iter()
            .map(|r| (r.height.0, (r.sapling_root, r.orchard_root)))
            .collect();
        VecRootSource(RootMap { roots, frontiers })
    }
}

impl CommitmentRootSource for VecRootSource {
    fn fast_root(
        &self,
        height: block::Height,
    ) -> Option<(sapling::tree::Root, orchard::tree::Root)> {
        self.0.fast_root(height)
    }
    fn handoff_height(&self) -> Option<block::Height> {
        self.0.handoff_height()
    }
    fn final_frontiers(&self) -> Option<&FinalFrontiers> {
        self.0.final_frontiers()
    }
}

/// Produce the per-block roots payload for `range` from `db`'s per-height trees.
///
/// This is the serving read path (the future `TreeAuxStatePort::read_block_roots`),
/// minus the network: it derives each root from the stored per-height tree, exactly
/// the value the fast path folds into the anchor set. Requires per-height trees, so
/// `db` must be an archive/legacy database; panics on a height whose tree is absent.
// Exercised by the round-trip test this increment; becomes the serving read path in 6a.
#[allow(dead_code)]
pub(super) fn produce_block_roots(
    db: &ZebraDb,
    range: std::ops::RangeInclusive<block::Height>,
) -> Vec<BlockCommitmentRoots> {
    let (start, end) = (range.start().0, range.end().0);
    (start..=end)
        .map(|h| {
            let height = block::Height(h);
            BlockCommitmentRoots {
                height,
                sapling_root: db
                    .sapling_tree_by_height(&height)
                    .expect("archive database has a per-height Sapling tree below the tip")
                    .root(),
                orchard_root: db
                    .orchard_tree_by_height(&height)
                    .expect("archive database has a per-height Orchard tree below the tip")
                    .root(),
            }
        })
        .collect()
}

/// Produce the final frontiers at `height` from `db`'s per-height trees (the future
/// `TreeAuxStatePort::read_final_frontiers`). Sprout is frozen far below any modern
/// checkpoint, so the tip Sprout tree is the frontier at `height`. Returns `None`
/// if `height` is above the database tip.
// Exercised by the round-trip test this increment; becomes the serving read path in 6a.
#[allow(dead_code)]
pub(super) fn produce_final_frontiers(
    db: &ZebraDb,
    height: block::Height,
) -> Option<FinalFrontiers> {
    Some(FinalFrontiers {
        height,
        sapling: db.sapling_tree_by_height(&height)?,
        orchard: db.orchard_tree_by_height(&height)?,
        sprout: db.sprout_tree_for_tip(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The final-frontier serialization round-trips: parsed frontiers carry the same
    /// height and tree roots as the originals.
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

    /// `VecRootSource` from a produced payload looks up roots by height and exposes
    /// the handoff frontier — the consumer view of producer output.
    #[test]
    fn vec_root_source_round_trips_payload() {
        let roots = vec![
            BlockCommitmentRoots {
                height: block::Height(10),
                sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
                orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
            },
            BlockCommitmentRoots {
                height: block::Height(11),
                sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
                orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
            },
        ];
        let frontiers = FinalFrontiers {
            height: block::Height(11),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
        };

        let source = VecRootSource::from_payload(roots, Some(frontiers));

        assert!(
            source.fast_root(block::Height(10)).is_some(),
            "produced root is looked up by height"
        );
        assert!(
            source.fast_root(block::Height(99)).is_none(),
            "absent height has no root"
        );
        assert_eq!(
            source.handoff_height(),
            Some(block::Height(11)),
            "handoff height comes from the supplied frontiers"
        );
    }
}
