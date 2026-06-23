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

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
};

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

impl FinalFrontiers {
    /// Serialize to the embedded byte format: height (u32 LE), then sapling, orchard,
    /// and sprout trees, each as `u32`-LE-length-prefixed `IntoDisk` bytes. Used to
    /// capture a Regtest handoff frontier at test/dev time (see `vct::capture_frontier_at`).
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

    /// Discard the supplied root for `height` so a later [`fast_root`](Self::fast_root)
    /// returns `None` for it.
    ///
    /// Called by the committer when a supplied root fails verification: dropping the bad
    /// root un-poisons the cache so a re-fetch from a different peer can replace it, rather
    /// than the committer re-reading the same rejected root forever. The default is a no-op
    /// (a local fixture is trusted and not re-fetched); only the peer source overrides it.
    fn invalidate(&self, _height: block::Height) {}

    /// Discard roots for heights that have already been committed.
    ///
    /// Called after the database write succeeds, so retry paths still keep roots needed
    /// for an uncommitted block. The default is a no-op for finite local fixtures; the
    /// peer source uses this to keep its live fetch-ahead cache bounded during sync.
    fn evict_committed_through(&self, _height: block::Height) {}

    /// Whether the committer must confirm each supplied root against a *buffered successor*
    /// before committing it (the one-block-lag verification, design §6).
    ///
    /// A block's roots are only committed by the next block's header, so a root committed
    /// without a successor to confirm it is unverified at commit time and only checked one
    /// block later — by which point it is irreversibly on disk. For an **untrusted** source
    /// (peers), a single wrong tip root would then wedge the sync with no recovery, so the
    /// committer must instead *defer* such a block until its successor is buffered. Returns
    /// `true` for the peer source; the default `false` is for a trusted local fixture, which
    /// is not adversarial and may commit its tip root on the in-arrears check (design §2.4).
    fn requires_verified_successor(&self) -> bool {
        false
    }
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

/// A fillable [`CommitmentRootSource`] backed by a shared, height-keyed roots cache.
///
/// The `tree_aux` driver (increment 6a) writes verified roots into the cache *ahead of*
/// the committer via [`PeerSourceWriter`], as ranges arrive from peers; the committer
/// reads them per height through the [`CommitmentRootSource`] seam. The handoff frontier
/// is embedded in the binary (design §5.2), so it is held immutably here and never
/// fetched over the network — only roots come from peers.
#[derive(Debug)]
pub(super) struct PeerSource {
    cache: Arc<RwLock<PeerRootsCache>>,
    frontiers: Option<FinalFrontiers>,
}

/// Write handle for a [`PeerSource`]: the driver fills the shared cache as verified root
/// ranges arrive. Cloneable so the driver and source share one cache.
#[derive(Clone, Debug)]
pub(crate) struct PeerSourceWriter {
    cache: Arc<RwLock<PeerRootsCache>>,
}

/// Shared peer-source cache state.
#[derive(Debug, Default)]
struct PeerRootsCache {
    roots: HashMap<u32, (sapling::tree::Root, orchard::tree::Root)>,
    committed_through: Option<u32>,
}

impl PeerSource {
    /// Create an empty peer source and its write handle. `frontiers` is the embedded
    /// handoff frontier (`None` for the bare benchmark, with no checkpoint handoff).
    pub(super) fn new(frontiers: Option<FinalFrontiers>) -> (Self, PeerSourceWriter) {
        let cache = Arc::new(RwLock::new(PeerRootsCache::default()));
        (
            PeerSource {
                cache: Arc::clone(&cache),
                frontiers,
            },
            PeerSourceWriter { cache },
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
}

/// Process-global handle to the live peer-source writer, published once when the
/// committer is built in peer (`tree_aux`) mode. The `tree_aux` driver in `zebrad`
/// fetches it to fill the committer's root cache as ranges arrive from peers.
///
/// A process global (rather than threading a writer return value back out through the
/// state-service init) keeps the experimental verified-commitment-trees wiring off the
/// production state-init signatures, matching the env-driven style of [`super::vct`].
static PEER_ROOTS_WRITER: OnceLock<PeerSourceWriter> = OnceLock::new();

/// Process-global signal used by the finalized committer to request a targeted root refetch.
static PEER_ROOT_REFETCH: OnceLock<broadcast::Sender<block::Height>> = OnceLock::new();

/// Build a [`PeerSource`] over the embedded handoff `frontiers` and publish its writer
/// globally so the `tree_aux` driver can fill it. Returns the source for the committer.
///
/// First writer wins: a second committer build (e.g. a test re-init in the same process)
/// reuses the originally published cache handle, so the driver and committer never split.
pub(super) fn install_peer_source(frontiers: Option<FinalFrontiers>) -> PeerSource {
    let (source, writer) = PeerSource::new(frontiers);
    let _ = PEER_ROOTS_WRITER.set(writer);
    let _ = PEER_ROOT_REFETCH.set(broadcast::channel(64).0);
    source
}

/// The live peer-source writer, if the committer was built in peer (`tree_aux`) mode.
/// Used by the `tree_aux` driver to write fetched root ranges into the committer's cache.
pub(crate) fn peer_roots_writer() -> Option<PeerSourceWriter> {
    PEER_ROOTS_WRITER.get().cloned()
}

/// Subscribe to targeted peer-root refetch requests.
pub(crate) fn peer_root_refetch_receiver() -> Option<broadcast::Receiver<block::Height>> {
    PEER_ROOT_REFETCH.get().map(|sender| sender.subscribe())
}

/// Request a targeted peer-root refetch for `height`.
pub(crate) fn request_peer_root_refetch(height: block::Height) {
    if let Some(sender) = PEER_ROOT_REFETCH.get() {
        if sender.send(height).is_err() {
            metrics::counter!("state.vct.root.refetch.no_receiver.count").increment(1);
            tracing::debug!(
                ?height,
                "VCT: requested peer root refetch but no tree_aux driver is subscribed"
            );
        }
    } else {
        metrics::counter!("state.vct.root.refetch.no_sender.count").increment(1);
        tracing::debug!(
            ?height,
            "VCT: requested peer root refetch before the peer-source signal was installed"
        );
    }
}

impl CommitmentRootSource for PeerSource {
    fn fast_root(
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
    fn handoff_height(&self) -> Option<block::Height> {
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

    fn requires_verified_successor(&self) -> bool {
        // Peer-supplied roots are untrusted: never commit one without a buffered successor
        // to confirm it, so a wrong tip root is rejected before it is persisted rather than
        // detected one block too late (see the trait method).
        true
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
            source.fast_root(block::Height(42)).is_some(),
            "the inserted root is present before eviction"
        );

        source.invalidate(block::Height(42));

        assert!(
            source.fast_root(block::Height(42)).is_none(),
            "an evicted root is gone, so the next read misses and a re-fetch can replace it"
        );
    }

    /// After a block commits, the peer source drops all roots through that height while
    /// retaining fetch-ahead roots the committer still needs.
    #[test]
    fn peer_source_evicts_committed_roots_only() {
        let (source, writer) = PeerSource::new(None);
        let empty_sapling_root = sapling::tree::NoteCommitmentTree::default().root();
        let empty_orchard_root = orchard::tree::NoteCommitmentTree::default().root();

        writer.insert_roots((40..=44).map(|height| BlockCommitmentRoots {
            height: block::Height(height),
            sapling_root: empty_sapling_root,
            orchard_root: empty_orchard_root,
        }));

        source.evict_committed_through(block::Height(42));

        assert!(
            source.fast_root(block::Height(40)).is_none(),
            "roots below the committed height are evicted"
        );
        assert!(
            source.fast_root(block::Height(42)).is_none(),
            "the committed height's root is evicted"
        );
        assert!(
            source.fast_root(block::Height(43)).is_some(),
            "fetch-ahead roots remain cached"
        );

        writer.insert_roots((41..=43).map(|height| BlockCommitmentRoots {
            height: block::Height(height),
            sapling_root: empty_sapling_root,
            orchard_root: empty_orchard_root,
        }));

        assert!(
            source.fast_root(block::Height(41)).is_none(),
            "late inserts at already-committed heights are ignored"
        );
        assert!(
            source.fast_root(block::Height(43)).is_some(),
            "late inserts above the committed height are still cached"
        );
    }

    /// Only the untrusted peer source requires a buffered successor before committing a
    /// supplied root; the trusted local fixture does not. This is the trust boundary the
    /// committer's deferral guard keys on, so it must not silently flip for either source.
    #[test]
    fn only_the_peer_source_requires_a_verified_successor() {
        let (peer, _writer) = PeerSource::new(None);
        assert!(
            peer.requires_verified_successor(),
            "the untrusted peer source must confirm each root against a successor"
        );

        let fixture = FixtureSource::new(HashMap::new(), None);
        assert!(
            !fixture.requires_verified_successor(),
            "the trusted local fixture commits its tip root on the in-arrears check"
        );
    }
}
