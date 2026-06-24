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
use tokio::sync::broadcast;
use zebra_chain::{block, orchard, sapling, sprout};

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
    fn vct_root(
        &self,
        height: block::Height,
    ) -> Option<(sapling::tree::Root, orchard::tree::Root)>;

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

/// A fillable [`CommitmentRootSource`] backed by a shared, height-keyed roots cache.
///
/// The `tree_aux` driver (increment 6a) writes verified roots into the cache *ahead of*
/// the committer via [`PeerSourceWriter`], after staging the initial range to completion
/// or when serving targeted refetches. The committer reads them per height through the
/// [`CommitmentRootSource`] seam. The handoff frontier is embedded in the binary
/// (design §5.2), so it is held immutably here and never fetched over the network —
/// only roots come from peers.
#[derive(Debug)]
pub(super) struct PeerSource {
    cache: Arc<RwLock<PeerRootsCache>>,
    frontiers: Option<FinalFrontiers>,
}

/// Write handle for a [`PeerSource`]: the driver fills the shared cache after verified root
/// ranges are complete, or for targeted single-height refetches. Cloneable so the driver
/// and source share one cache.
#[derive(Clone, Debug)]
pub(crate) struct PeerSourceWriter {
    cache: Arc<RwLock<PeerRootsCache>>,
}

/// Per-state driver handle for a [`PeerSource`].
///
/// The `tree_aux` driver writes verified roots through [`Self::insert_roots`] and subscribes
/// to targeted refetch requests from the committer through [`Self::subscribe_refetch`].
#[derive(Clone, Debug)]
pub(crate) struct PeerSourceHandle {
    writer: PeerSourceWriter,
    refetch_sender: broadcast::Sender<block::Height>,
}

/// Shared peer-source cache state.
#[derive(Debug, Default)]
struct PeerRootsCache {
    roots: HashMap<u32, (sapling::tree::Root, orchard::tree::Root)>,
    committed_through: Option<u32>,
}

impl PeerSource {
    /// Create an empty peer source and its driver handle. `frontiers` is the embedded
    /// handoff frontier (`None` for the bare benchmark, with no checkpoint handoff).
    pub(super) fn new(frontiers: Option<FinalFrontiers>) -> (Self, PeerSourceHandle) {
        let cache = Arc::new(RwLock::new(PeerRootsCache::default()));
        let writer = PeerSourceWriter {
            cache: Arc::clone(&cache),
        };
        (
            PeerSource {
                cache: Arc::clone(&cache),
                frontiers,
            },
            PeerSourceHandle {
                writer,
                refetch_sender: broadcast::channel(64).0,
            },
        )
    }
}

impl PeerSourceWriter {
    /// Insert verified roots fetched for a range into the shared cache.
    ///
    /// Last write wins per uncommitted height; roots at already-committed heights are
    /// ignored so stale refetches cannot grow the cache below the finalized tip.
    pub(crate) fn insert_roots(&self, roots: impl IntoIterator<Item = BlockCommitmentRoots>) {
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

    /// Remove peer-supplied roots at `heights` from the shared cache.
    fn invalidate_roots(&self, heights: impl IntoIterator<Item = block::Height>) {
        let mut cache = self.cache.write().expect("peer source roots lock poisoned");
        for height in heights {
            cache.roots.remove(&height.0);
        }
    }

    /// The highest finalized height whose peer roots have been evicted from the cache.
    fn committed_through(&self) -> Option<block::Height> {
        self.cache
            .read()
            .expect("peer source roots lock poisoned")
            .committed_through
            .map(block::Height)
    }
}

impl PeerSourceHandle {
    /// Insert verified roots fetched for a range into the shared cache.
    pub(crate) fn insert_roots(&self, roots: impl IntoIterator<Item = BlockCommitmentRoots>) {
        self.writer.insert_roots(roots);
    }

    /// Remove peer-supplied roots at `heights` from the shared cache.
    pub(crate) fn invalidate_roots(&self, heights: impl IntoIterator<Item = block::Height>) {
        self.writer.invalidate_roots(heights);
    }

    /// The highest finalized height whose peer roots have been evicted from the cache.
    pub(crate) fn committed_through(&self) -> Option<block::Height> {
        self.writer.committed_through()
    }

    /// Subscribe to targeted peer-root refetch requests.
    pub(crate) fn subscribe_refetch(&self) -> broadcast::Receiver<block::Height> {
        self.refetch_sender.subscribe()
    }

    /// Request a targeted peer-root refetch for `height`.
    pub(crate) fn request_refetch(&self, height: block::Height) {
        if self.refetch_sender.send(height).is_err() {
            metrics::counter!("state.vct.root.refetch.no_receiver.count").increment(1);
            tracing::debug!(
                ?height,
                "VCT: requested peer root refetch but no tree_aux driver is subscribed"
            );
        }
    }
}

impl CommitmentRootSource for PeerSource {
    fn vct_root(
        &self,
        height: block::Height,
    ) -> Option<(sapling::tree::Root, orchard::tree::Root)> {
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
        // Drop the rejected root so the next read misses and the driver can re-fetch a
        // (verifiable) replacement for this height from another peer.
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
        });
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
            },
            BlockCommitmentRoots {
                height: block::Height(11),
                sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
                orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
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

    /// Bulk invalidation drops every still-cached root named by the driver after it
    /// identifies a bad supplier, so a poisoned fetch window is retried from another peer
    /// instead of failing one height at a time.
    #[test]
    fn peer_source_bulk_invalidate_evicts_multiple_roots() {
        let (source, writer) = PeerSource::new(None);
        let empty_sapling_root = sapling::tree::NoteCommitmentTree::default().root();
        let empty_orchard_root = orchard::tree::NoteCommitmentTree::default().root();

        writer.insert_roots((40..=44).map(|height| BlockCommitmentRoots {
            height: block::Height(height),
            sapling_root: empty_sapling_root,
            orchard_root: empty_orchard_root,
        }));

        writer.invalidate_roots([block::Height(41), block::Height(43)]);

        assert!(
            source.vct_root(block::Height(40)).is_some(),
            "roots outside the invalidation set stay cached"
        );
        assert!(
            source.vct_root(block::Height(41)).is_none(),
            "the first invalidated height is evicted"
        );
        assert!(
            source.vct_root(block::Height(43)).is_none(),
            "the second invalidated height is evicted"
        );
        assert!(
            source.vct_root(block::Height(44)).is_some(),
            "higher roots outside the invalidation set stay cached"
        );
    }

    /// Each peer source owns its cache and refetch signal. This is the property the
    /// per-state handle replaces the old process-global `OnceLock` publishing with.
    #[test]
    fn peer_source_handles_are_isolated_per_state() {
        let (source_a, handle_a) = PeerSource::new(None);
        let (source_b, handle_b) = PeerSource::new(None);
        let empty_sapling_root = sapling::tree::NoteCommitmentTree::default().root();
        let empty_orchard_root = orchard::tree::NoteCommitmentTree::default().root();

        handle_a.insert_roots([BlockCommitmentRoots {
            height: block::Height(42),
            sapling_root: empty_sapling_root,
            orchard_root: empty_orchard_root,
        }]);

        assert!(
            source_a.vct_root(block::Height(42)).is_some(),
            "the first peer source sees roots inserted through its own handle"
        );
        assert!(
            source_b.vct_root(block::Height(42)).is_none(),
            "a second peer source in the same process has an independent cache"
        );

        let mut refetch_a = handle_a.subscribe_refetch();
        let mut refetch_b = handle_b.subscribe_refetch();
        handle_a.request_refetch(block::Height(42));

        assert_eq!(
            refetch_a.try_recv(),
            Ok(block::Height(42)),
            "the first handle receives its own refetch request"
        );
        assert!(
            matches!(
                refetch_b.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "the second handle does not receive another state's refetch request"
        );
    }

    /// After a block commits, the peer source drops all roots through that height while
    /// retaining fetch-ahead roots the committer still needs.
    #[test]
    fn peer_source_evicts_committed_roots_only() {
        let (source, writer) = PeerSource::new(None);
        let empty_sapling_root = sapling::tree::NoteCommitmentTree::default().root();
        let empty_orchard_root = orchard::tree::NoteCommitmentTree::default().root();

        assert_eq!(
            writer.committed_through(),
            None,
            "a fresh peer source has no committed watermark"
        );

        writer.insert_roots((40..=44).map(|height| BlockCommitmentRoots {
            height: block::Height(height),
            sapling_root: empty_sapling_root,
            orchard_root: empty_orchard_root,
        }));

        source.evict_committed_through(block::Height(42));

        assert_eq!(
            writer.committed_through(),
            Some(block::Height(42)),
            "the writer exposes the cache eviction watermark to the fetch driver"
        );

        assert!(
            source.vct_root(block::Height(40)).is_none(),
            "roots below the committed height are evicted"
        );
        assert!(
            source.vct_root(block::Height(42)).is_none(),
            "the committed height's root is evicted"
        );
        assert!(
            source.vct_root(block::Height(43)).is_some(),
            "fetch-ahead roots remain cached"
        );

        writer.insert_roots((41..=43).map(|height| BlockCommitmentRoots {
            height: block::Height(height),
            sapling_root: empty_sapling_root,
            orchard_root: empty_orchard_root,
        }));

        assert!(
            source.vct_root(block::Height(41)).is_none(),
            "late inserts at already-committed heights are ignored"
        );
        assert!(
            source.vct_root(block::Height(43)).is_some(),
            "late inserts above the committed height are still cached"
        );

        source.evict_committed_through(block::Height(41));

        assert_eq!(
            writer.committed_through(),
            Some(block::Height(42)),
            "committed watermark never regresses"
        );
    }
}
