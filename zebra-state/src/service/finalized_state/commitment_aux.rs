//! Commitment-root source seam and payload types for the verified-commitment-trees
//! fast path (`docs/design/verified-commitment-trees.md` §5, increment 3).
//!
//! The fast path consumes per-block Sapling/Orchard roots and a final frontier at the
//! checkpoint handoff. *Where* that data comes from is abstracted behind
//! [`CommitmentRootSource`], so the committer reads through one seam regardless of
//! source. The production source is the transport-backed [`PeerSource`] over `tree_aux`;
//! tests use a crate-local fixture source over the same `RootMap` shape.
//!
//! It also provides the **producer** half ([`produce_block_roots`] /
//! [`produce_final_frontiers`]): deriving the same payload from an existing database's
//! per-height trees. That is the read path a serving node runs, and tests can feed the
//! DB-produced payload back through the fast path in-process to prove producer and
//! consumer agreement without networking.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, RwLock},
};

use thiserror::Error;
use zebra_chain::{
    block::{self, merkle::AuthDataRoot},
    orchard, sapling, sprout,
};

use super::{FromDisk, IntoDisk, ZebraDb};

/// Per-block verified commitment roots — the essential fast-path payload (design §5.1),
/// the wire payload carried over `tree_aux` (increment 6a). Defined in `zebra-chain` so
/// `zebra-network` and `zebra-state` share it without a dependency cycle.
pub(super) use zebra_chain::parallel::commitment_aux::BlockCommitmentRoots;

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

/// Errors producing [`FinalFrontiers`] from a finalized database.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FinalFrontiersGenerationError {
    /// The database has no Sapling tree at the requested height.
    #[error("missing Sapling final frontier tree at height {height:?}")]
    MissingSaplingTree {
        /// The requested final frontier height.
        height: block::Height,
    },

    /// The database has no Orchard tree at the requested height.
    #[error("missing Orchard final frontier tree at height {height:?}")]
    MissingOrchardTree {
        /// The requested final frontier height.
        height: block::Height,
    },
}

/// Errors parsing [`FinalFrontiers`] from the embedded/frontier-file byte format.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum FinalFrontiersParseError {
    /// The input ended before the 4-byte height field.
    MissingHeight {
        /// The total number of bytes in the input.
        actual_len: usize,
    },
    /// The input ended before a tree blob's 4-byte length prefix.
    MissingLength {
        /// The tree whose length prefix was being read.
        tree: &'static str,
        /// Byte offset where the length prefix starts.
        offset: usize,
        /// Bytes remaining from `offset`.
        remaining: usize,
    },
    /// A tree blob's length prefix points past the end of the input.
    TruncatedBlob {
        /// The tree whose blob was being read.
        tree: &'static str,
        /// Byte offset where the blob starts.
        offset: usize,
        /// Blob length from the prefix.
        expected_len: usize,
        /// Bytes remaining from `offset`.
        remaining: usize,
    },
    /// A tree blob's length prefix overflows `usize` arithmetic.
    LengthOverflow {
        /// The tree whose blob was being read.
        tree: &'static str,
        /// Byte offset where the blob starts.
        offset: usize,
        /// Blob length from the prefix.
        len: usize,
    },
    /// The parser consumed all expected fields, but extra bytes remained.
    TrailingBytes {
        /// Byte offset where the trailing data starts.
        offset: usize,
        /// Number of trailing bytes.
        trailing_len: usize,
    },
}

impl fmt::Display for FinalFrontiersParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FinalFrontiersParseError::MissingHeight { actual_len } => write!(
                f,
                "missing final frontier height: expected 4 bytes, got {actual_len}"
            ),
            FinalFrontiersParseError::MissingLength {
                tree,
                offset,
                remaining,
            } => write!(
                f,
                "missing {tree} frontier length prefix at byte {offset}: expected 4 bytes, got {remaining}"
            ),
            FinalFrontiersParseError::TruncatedBlob {
                tree,
                offset,
                expected_len,
                remaining,
            } => write!(
                f,
                "truncated {tree} frontier blob at byte {offset}: length prefix says {expected_len} bytes, but only {remaining} remain"
            ),
            FinalFrontiersParseError::LengthOverflow { tree, offset, len } => write!(
                f,
                "{tree} frontier blob length overflows at byte {offset}: {len} bytes"
            ),
            FinalFrontiersParseError::TrailingBytes {
                offset,
                trailing_len,
            } => write!(
                f,
                "unexpected trailing final frontier bytes at byte {offset}: {trailing_len} bytes"
            ),
        }
    }
}

impl std::error::Error for FinalFrontiersParseError {}

impl FinalFrontiers {
    /// Serialize to the embedded byte format: height (u32 LE), then sapling, orchard,
    /// and sprout trees, each as `u32`-LE-length-prefixed `IntoDisk` bytes. Used to
    /// create embedded or test final-frontier fixtures.
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
    pub(super) fn from_bytes(bytes: &[u8]) -> Result<Self, FinalFrontiersParseError> {
        let height_bytes = bytes
            .get(0..4)
            .ok_or(FinalFrontiersParseError::MissingHeight {
                actual_len: bytes.len(),
            })?;
        let height_bytes: [u8; 4] =
            height_bytes
                .try_into()
                .map_err(|_| FinalFrontiersParseError::MissingHeight {
                    actual_len: bytes.len(),
                })?;
        let height = block::Height(u32::from_le_bytes(height_bytes));

        // Read three `u32`-length-prefixed blobs starting after the height.
        let mut cursor: usize = 4;
        let mut next_blob = |tree: &'static str| -> Result<Vec<u8>, FinalFrontiersParseError> {
            let len_end =
                cursor
                    .checked_add(4)
                    .ok_or(FinalFrontiersParseError::LengthOverflow {
                        tree,
                        offset: cursor,
                        len: 4,
                    })?;
            let len_bytes =
                bytes
                    .get(cursor..len_end)
                    .ok_or(FinalFrontiersParseError::MissingLength {
                        tree,
                        offset: cursor,
                        remaining: bytes.len().saturating_sub(cursor),
                    })?;
            let len_bytes: [u8; 4] =
                len_bytes
                    .try_into()
                    .map_err(|_| FinalFrontiersParseError::MissingLength {
                        tree,
                        offset: cursor,
                        remaining: bytes.len().saturating_sub(cursor),
                    })?;
            // Zebra's supported platforms have at least 32-bit `usize`, so every
            // u32 length prefix fits in memory indexes.
            let len = u32::from_le_bytes(len_bytes) as usize;
            cursor = len_end;
            let blob_end =
                cursor
                    .checked_add(len)
                    .ok_or(FinalFrontiersParseError::LengthOverflow {
                        tree,
                        offset: cursor,
                        len,
                    })?;
            let blob =
                bytes
                    .get(cursor..blob_end)
                    .ok_or(FinalFrontiersParseError::TruncatedBlob {
                        tree,
                        offset: cursor,
                        expected_len: len,
                        remaining: bytes.len().saturating_sub(cursor),
                    })?;
            cursor = blob_end;
            Ok(blob.to_vec())
        };
        let sapling = next_blob("sapling")?;
        let orchard = next_blob("orchard")?;
        let sprout = next_blob("sprout")?;

        if cursor != bytes.len() {
            return Err(FinalFrontiersParseError::TrailingBytes {
                offset: cursor,
                trailing_len: bytes.len() - cursor,
            });
        }

        Ok(FinalFrontiers {
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
        })
    }
}

/// Where the fast path's verified per-block roots and handoff frontiers come from.
///
/// One enduring seam, two enduring data paths: the standard/legacy path rebuilds
/// trees locally and never consults a source; the fast verified path reads roots
/// from *some* source and verifies them against the headers. The production source is
/// [`PeerSource`]; tests may install a trusted local source to isolate committer
/// behavior. The trait carries no trust policy by itself: the owning VCT state decides
/// whether supplied roots must be confirmed by a buffered successor before commit.
pub(super) trait CommitmentRootSource: std::fmt::Debug + Send + Sync {
    /// The supplied roots for `height`, if this source has them.
    fn vct_root(&self, height: block::Height)
        -> Option<(sapling::tree::Root, orchard::tree::Root)>;

    /// The checkpoint handoff height (below which the vct path skips per-height
    /// trees), if this source supplies a final frontier.
    fn vct_last_checkpoint_height(&self) -> Option<block::Height>;

    /// The verified final frontiers at the handoff height, if supplied.
    fn final_frontiers(&self) -> Option<&FinalFrontiers>;

    /// Discard the supplied root for `height` so a later [`fast_root`](Self::fast_root)
    /// returns `None` for it.
    ///
    /// Called by the committer when a supplied root fails verification: dropping the bad
    /// root un-poisons the cache so a re-fetch from a different peer can replace it, rather
    /// than the committer re-reading the same rejected root forever. The default is a no-op
    /// for test-only local sources; the peer source overrides it.
    fn invalidate(&self, _height: block::Height) {}

    /// Discard roots for heights that have already been committed.
    ///
    /// Called after the database write succeeds, so retry paths still keep roots needed
    /// for an uncommitted block. The default is a no-op for test-only local sources; the
    /// peer source uses this to keep its live fetch-ahead cache bounded during sync.
    fn evict_committed_through(&self, _height: block::Height) {}
}

/// The shared in-memory representation behind the concrete sources: a height→roots
/// map plus the optional handoff frontiers.
#[cfg(test)]
#[derive(Debug, Default)]
struct RootMap {
    roots: HashMap<u32, (sapling::tree::Root, orchard::tree::Root)>,
    frontiers: Option<FinalFrontiers>,
}

#[cfg(test)]
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

/// Test-only local source over a height-keyed roots map.
#[cfg(test)]
#[derive(Debug)]
pub(super) struct FixtureSource(RootMap);

#[cfg(test)]
impl FixtureSource {
    pub(super) fn new(
        roots: HashMap<u32, (sapling::tree::Root, orchard::tree::Root)>,
        frontiers: Option<FinalFrontiers>,
    ) -> Self {
        FixtureSource(RootMap { roots, frontiers })
    }
}

#[cfg(test)]
impl CommitmentRootSource for FixtureSource {
    fn vct_root(
        &self,
        height: block::Height,
    ) -> Option<(sapling::tree::Root, orchard::tree::Root)> {
        self.0.fast_root(height)
    }
    fn vct_last_checkpoint_height(&self) -> Option<block::Height> {
        self.0.handoff_height()
    }
    fn final_frontiers(&self) -> Option<&FinalFrontiers> {
        self.0.final_frontiers()
    }
}

/// A [`CommitmentRootSource`] backed by provisional header-ahead roots in `db`.
///
/// Header sync persists peer-supplied roots into `db` ahead of body commit; the committer
/// reads them per height through the [`CommitmentRootSource`] seam. The handoff frontier is
/// embedded in the binary (design §5.2), held immutably here and never fetched over the
/// network. The in-memory `cache` is test-only scaffolding for the non-`db` source.
#[derive(Debug)]
pub(super) struct PeerSource {
    db: Option<ZebraDb>,
    cache: Arc<RwLock<PeerRootsCache>>,
    frontiers: Option<FinalFrontiers>,
}

/// Shared peer-source cache state.
#[derive(Debug, Default)]
struct PeerRootsCache {
    roots: HashMap<u32, (sapling::tree::Root, orchard::tree::Root)>,
    committed_through: Option<u32>,
}

impl PeerSource {
    /// Create an empty in-memory peer source and a writer sharing its cache. `frontiers`
    /// is the embedded handoff frontier (`None` for the bare benchmark, with no checkpoint
    /// handoff). The writer lets a test fill roots before and after the source is moved
    /// into the committer.
    #[cfg(any(test, feature = "proptest-impl"))]
    #[allow(dead_code)]
    pub(super) fn new(frontiers: Option<FinalFrontiers>) -> (Self, PeerSourceWriter) {
        let cache = Arc::new(RwLock::new(PeerRootsCache::default()));
        let writer = PeerSourceWriter {
            cache: Arc::clone(&cache),
        };
        (
            PeerSource {
                db: None,
                cache,
                frontiers,
            },
            writer,
        )
    }

    /// Create a source backed by provisional header-ahead roots in `db`.
    pub(super) fn new_with_db(db: ZebraDb, frontiers: Option<FinalFrontiers>) -> Self {
        PeerSource {
            db: Some(db),
            cache: Arc::new(RwLock::new(PeerRootsCache::default())),
            frontiers,
        }
    }
}

/// Test-only writer sharing a [`PeerSource`]'s in-memory cache, so a proptest can fill
/// roots before and after the source is moved into the committer.
#[cfg(any(test, feature = "proptest-impl"))]
#[derive(Clone, Debug)]
pub(super) struct PeerSourceWriter {
    cache: Arc<RwLock<PeerRootsCache>>,
}

#[cfg(any(test, feature = "proptest-impl"))]
impl PeerSourceWriter {
    /// Insert roots into the shared in-memory cache. Last write wins per uncommitted
    /// height; roots at already-committed heights are ignored.
    #[allow(dead_code)]
    pub(super) fn insert_roots(&self, roots: impl IntoIterator<Item = BlockCommitmentRoots>) {
        let mut cache = self.cache.write().expect("peer source roots lock poisoned");
        for r in roots {
            if cache
                .committed_through
                .is_some_and(|height| r.height.0 <= height)
            {
                continue;
            }

            cache
                .roots
                .insert(r.height.0, (r.sapling_root, r.orchard_root));
        }
    }
}

impl CommitmentRootSource for PeerSource {
    fn vct_root(
        &self,
        height: block::Height,
    ) -> Option<(sapling::tree::Root, orchard::tree::Root)> {
        if let Some(db) = &self.db {
            return db
                .zakura_header_commitment_roots_by_height_range(height..=height)
                .into_iter()
                .next()
                .map(|roots| (roots.sapling_root, roots.orchard_root));
        }

        self.cache
            .read()
            .expect("peer source roots lock poisoned")
            .roots
            .get(&height.0)
            .copied()
    }
    fn vct_last_checkpoint_height(&self) -> Option<block::Height> {
        self.frontiers.as_ref().map(|f| f.height)
    }
    fn final_frontiers(&self) -> Option<&FinalFrontiers> {
        self.frontiers.as_ref()
    }
    fn invalidate(&self, height: block::Height) {
        // Drop the rejected root so the next read misses; header sync can then deliver a
        // verifiable replacement for this height from another peer.
        if let Some(db) = &self.db {
            if let Err(error) = db.delete_zakura_header_commitment_roots([height]) {
                tracing::debug!(?error, ?height, "failed to delete rejected VCT root");
            }
            return;
        }

        self.cache
            .write()
            .expect("peer source roots lock poisoned")
            .roots
            .remove(&height.0);
    }

    fn evict_committed_through(&self, height: block::Height) {
        let mut cache = self.cache.write().expect("peer source roots lock poisoned");
        let start = cache
            .committed_through
            .map_or(0, |height| height.saturating_add(1));

        if start <= height.0 {
            for cached_height in start..=height.0 {
                cache.roots.remove(&cached_height);
            }
            cache.committed_through = Some(height.0);
        }
    }
}

/// Produce the per-block roots payload for `range` from `db`'s per-height trees.
///
/// This is the serving read path (the future `TreeAuxStatePort::read_block_roots`),
/// minus the network: it derives each root from the stored per-height tree, exactly
/// the value the fast path folds into the anchor set. It requires per-height trees, so
/// the caller restricts it to a non-fast-synced (archive/pre-index) database within the
/// tip, where the trees are present. As defense-in-depth on this peer-triggered read, a
/// height whose tree is unexpectedly absent stops the scan and serves the contiguous
/// prefix collected so far rather than panicking; the wire client validates contiguity
/// and treats a short batch as partial progress.
// The `ReadRequest::BlockRoots` serving read path; also exercised by the round-trip test.
pub(crate) fn produce_block_roots(
    db: &ZebraDb,
    range: std::ops::RangeInclusive<block::Height>,
) -> Vec<BlockCommitmentRoots> {
    let (start, end) = (range.start().0, range.end().0);
    let mut roots = Vec::new();
    for h in start..=end {
        let height = block::Height(h);
        let (Some(sapling), Some(orchard)) = (
            db.sapling_tree_by_height(&height),
            db.orchard_tree_by_height(&height),
        ) else {
            break;
        };
        roots.push(BlockCommitmentRoots {
            height,
            sapling_root: sapling.root(),
            orchard_root: orchard.root(),
            // Below the upgrade height the serving index does not exist, so derive the
            // auth-data root from the locally stored block (this archival node holds the
            // body for these heights). Zero only if the body is somehow absent, in which
            // case the recipient simply re-fetches from a node that has it.
            auth_data_root: db
                .block(height.into())
                .map(|block| block.auth_data_root())
                .unwrap_or_else(|| AuthDataRoot::from([0u8; 32])),
        });
    }
    roots
}

/// Serve the per-block roots for `range`, stitching the two sources at the upgrade height `U`.
///
/// The `commitment_roots_by_height` serving index only covers heights at and above `U` (the lowest
/// height this binary committed). Heights below `U` predate the index, so they are derived from the
/// per-height trees instead, and the two runs are concatenated. This is what lets a node that
/// upgraded mid-chain serve a request that straddles `U` as one gap-free batch, rather than the
/// short index-only prefix that would stall the client's minimum-progress check.
///
/// Both sources stop at the first absent height, so the result is always a contiguous run from
/// `range.start()`; a tree gap below `U` is served as the prefix collected so far without reaching
/// into the index. A database that never recorded `U` — a pre-index archive node — derives the
/// whole range from the trees, the original archive fallback.
pub(crate) fn serve_block_roots(
    db: &ZebraDb,
    range: std::ops::RangeInclusive<block::Height>,
) -> Vec<BlockCommitmentRoots> {
    let Some(upgrade) = db.vct_upgrade_height() else {
        return produce_block_roots(db, range);
    };

    let (start, end) = (*range.start(), *range.end());

    // Wholly at/above `U`: the index covers it. (`U == 0` for a node that fast-synced from
    // genesis takes this path for every request, never touching the absent per-height trees.)
    if start >= upgrade {
        return db.commitment_roots_by_height_range(range);
    }

    // Below `U`: derive the per-height-tree run up to `U - 1` (`start < upgrade` so `upgrade >= 1`).
    let trees_end = block::Height(end.0.min(upgrade.0 - 1));
    let mut roots = produce_block_roots(db, start..=trees_end);

    // Continue into the index only if the tree run is contiguous up to `U - 1`; a short run means a
    // gap below `U`, so serve it alone and let the client retry the remainder.
    if roots.last().map(|root| root.height) == Some(trees_end) && end >= upgrade {
        roots.extend(db.commitment_roots_by_height_range(upgrade..=end));
    }

    roots
}

/// Produce the final frontiers at `height` from `db`'s per-height trees.
///
/// Sprout is frozen far below any modern checkpoint, so the tip Sprout tree is the frontier at
/// `height`.
pub(super) fn produce_final_frontiers(
    db: &ZebraDb,
    height: block::Height,
) -> Result<FinalFrontiers, FinalFrontiersGenerationError> {
    let sapling = db
        .sapling_tree_by_height(&height)
        .ok_or(FinalFrontiersGenerationError::MissingSaplingTree { height })?;
    let orchard = db
        .orchard_tree_by_height(&height)
        .ok_or(FinalFrontiersGenerationError::MissingOrchardTree { height })?;

    Ok(FinalFrontiers {
        height,
        sapling,
        orchard,
        sprout: db.sprout_tree_for_tip(),
    })
}

/// Produce serialized final-frontier bytes for the checkpoint handoff at `height`.
///
/// These bytes use the same format as the embedded `mainnet-frontier.bin` file consumed by
/// [`super::vct`].
pub fn produce_final_frontiers_bytes(
    db: &ZebraDb,
    height: block::Height,
) -> Result<Vec<u8>, FinalFrontiersGenerationError> {
    Ok(produce_final_frontiers(db, height)?.to_bytes())
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

        let parsed =
            FinalFrontiers::from_bytes(&frontiers.to_bytes()).expect("frontiers should parse");

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

    /// The test fixture source looks up produced roots by height and exposes
    /// the handoff frontier — the consumer view of producer output.
    #[test]
    fn fixture_source_round_trips_payload() {
        let roots = vec![
            BlockCommitmentRoots {
                height: block::Height(10),
                sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
                orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
                auth_data_root: AuthDataRoot::from([0u8; 32]),
            },
            BlockCommitmentRoots {
                height: block::Height(11),
                sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
                orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
                auth_data_root: AuthDataRoot::from([0u8; 32]),
            },
        ];
        let roots = roots
            .into_iter()
            .map(|root| (root.height.0, (root.sapling_root, root.orchard_root)))
            .collect();
        let frontiers = FinalFrontiers {
            height: block::Height(11),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
        };

        let source = FixtureSource::new(roots, Some(frontiers));

        assert!(
            source.vct_root(block::Height(10)).is_some(),
            "produced root is looked up by height"
        );
        assert!(
            source.vct_root(block::Height(99)).is_none(),
            "absent height has no root"
        );
        assert_eq!(
            source.vct_last_checkpoint_height(),
            Some(block::Height(11)),
            "handoff height comes from the supplied frontiers"
        );
    }

    /// `invalidate` drops a peer-supplied root so a later read misses it, letting the
    /// driver re-fetch a verifiable replacement from another peer. This un-poisons the
    /// cache after a bad root is rejected by the committer, so one malicious peer cannot
    /// wedge the same rejected root in place forever.
    #[test]
    fn peer_source_invalidate_evicts_a_root() {
        let (source, writer) = PeerSource::new(None);
        writer.insert_roots([BlockCommitmentRoots {
            height: block::Height(42),
            sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
            orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
            auth_data_root: AuthDataRoot::from([0u8; 32]),
        }]);

        assert!(
            source.vct_root(block::Height(42)).is_some(),
            "the inserted root is present before eviction"
        );

        source.invalidate(block::Height(42));

        assert!(
            source.vct_root(block::Height(42)).is_none(),
            "an evicted root is gone, so the next read misses and a re-fetch can replace it"
        );
    }
}
