//! Shared Zakura sync frontier contract.

use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::watch;
use zebra_chain::block;

use super::{
    BlockApplyResult, BlockApplyToken, BlockSyncBlockMeta, HeaderSyncCommitFailureKind,
    ZakuraPeerId,
};

/// A height/hash pair at one chain frontier.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Frontier {
    /// Frontier height.
    pub height: block::Height,
    /// Frontier block hash.
    pub hash: block::Hash,
}

impl Frontier {
    /// Construct a frontier from its height and hash.
    pub fn new(height: block::Height, hash: block::Hash) -> Self {
        Self { height, hash }
    }
}

/// Shared Zakura chain facts owned by the sync exchange.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ChainFrontier {
    /// Highest finalized block.
    pub finalized: Frontier,
    /// Highest verified block body.
    pub verified_body: Frontier,
    /// Highest committed header target.
    pub best_header: Frontier,
}

/// Cause for a shared frontier update.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FrontierChange {
    /// Initial or whole-state snapshot.
    Snapshot,
    /// Verified body frontier advanced.
    VerifiedGrow,
    /// Verified body frontier was reset, possibly lower.
    VerifiedReset,
    /// Best header target advanced.
    HeaderAdvanced,
    /// Best header target was reanchored, possibly lower.
    HeaderReanchored,
}

/// Latest shared frontier plus the transition cause.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct FrontierUpdate {
    /// Current shared frontier after the change.
    pub frontier: ChainFrontier,
    /// Cause of the current value.
    pub change: FrontierChange,
}

/// Result of a header range commit through the sync exchange.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderRangeCommit {
    /// First committed header height.
    pub start_height: block::Height,
    /// New durable best header frontier.
    pub tip: Frontier,
}

/// Result of a submitted block body through the sync exchange.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BlockBodySubmit {
    /// Submission token echoed from the reactor.
    pub token: BlockApplyToken,
    /// Submitted block height.
    pub height: block::Height,
    /// Submitted block hash.
    pub hash: block::Hash,
    /// Verifier result.
    pub result: BlockApplyResult,
    /// Locally observed shared frontier after the apply attempt.
    pub local_frontier: Option<ChainFrontier>,
}

/// Header-sync view of shared Zakura state.
pub trait HeaderSyncStatePortImpl: Send + Sync + 'static {
    /// Return the currently cached shared frontier.
    fn current_frontier(&self) -> ChainFrontier;

    /// Subscribe to latest-value shared frontier updates.
    fn subscribe_frontier(&self) -> watch::Receiver<FrontierUpdate>;

    /// Commit a contiguous header range.
    fn commit_header_range(
        &self,
        peer: ZakuraPeerId,
        anchor: block::Hash,
        start_height: block::Height,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
        finalized: bool,
    ) -> BoxFuture<'static, Result<HeaderRangeCommit, HeaderSyncCommitFailureKind>>;

    /// Publish a locally accepted best-header advance.
    fn publish_best_header(&self, tip: Frontier) -> BoxFuture<'static, ()>;

    /// Publish a best-header reanchor.
    fn publish_header_reanchor(&self, old: Frontier, new: Frontier) -> BoxFuture<'static, ()>;
}

/// Block-sync view of shared Zakura state.
pub trait BlockSyncStatePortImpl: Send + Sync + 'static {
    /// Return the currently cached shared frontier.
    fn current_frontier(&self) -> ChainFrontier;

    /// Subscribe to latest-value shared frontier updates.
    fn subscribe_frontier(&self) -> watch::Receiver<FrontierUpdate>;

    /// Query committed headers that still need block bodies.
    fn query_missing_bodies(
        &self,
        verified_body: block::Height,
        best_header: block::Height,
    ) -> BoxFuture<'static, Result<Vec<BlockSyncBlockMeta>, zebra_chain::BoxError>>;

    /// Submit a downloaded body to the verifier/state pipeline.
    fn submit_block_body(
        &self,
        token: BlockApplyToken,
        block: Arc<block::Block>,
    ) -> BoxFuture<'static, BlockBodySubmit>;

    /// Read committed blocks for serving stream-6 peers.
    fn read_committed_blocks(
        &self,
        start: block::Height,
        count: u32,
    ) -> BoxFuture<
        'static,
        Result<Vec<(block::Height, Arc<block::Block>, usize)>, zebra_chain::BoxError>,
    >;

    /// Publish body-sync progress that did not come from a direct submit path.
    fn publish_body_progress(&self, update: FrontierUpdate) -> BoxFuture<'static, ()>;
}

/// Cloneable header-sync state port.
#[derive(Clone)]
pub struct HeaderSyncStatePort {
    inner: Arc<dyn HeaderSyncStatePortImpl>,
}

impl HeaderSyncStatePort {
    /// Wrap an implementation in a cloneable port.
    pub fn new(inner: Arc<dyn HeaderSyncStatePortImpl>) -> Self {
        Self { inner }
    }

    /// Return the currently cached shared frontier.
    pub fn current_frontier(&self) -> ChainFrontier {
        self.inner.current_frontier()
    }

    /// Subscribe to latest-value shared frontier updates.
    pub fn subscribe_frontier(&self) -> watch::Receiver<FrontierUpdate> {
        self.inner.subscribe_frontier()
    }

    /// Commit a contiguous header range.
    pub fn commit_header_range(
        &self,
        peer: ZakuraPeerId,
        anchor: block::Hash,
        start_height: block::Height,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
        finalized: bool,
    ) -> BoxFuture<'static, Result<HeaderRangeCommit, HeaderSyncCommitFailureKind>> {
        self.inner
            .commit_header_range(peer, anchor, start_height, headers, body_sizes, finalized)
    }

    /// Publish a locally accepted best-header advance.
    pub fn publish_best_header(&self, tip: Frontier) -> BoxFuture<'static, ()> {
        self.inner.publish_best_header(tip)
    }

    /// Publish a best-header reanchor.
    pub fn publish_header_reanchor(&self, old: Frontier, new: Frontier) -> BoxFuture<'static, ()> {
        self.inner.publish_header_reanchor(old, new)
    }
}

impl std::fmt::Debug for HeaderSyncStatePort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("HeaderSyncStatePort")
    }
}

/// Cloneable block-sync state port.
#[derive(Clone)]
pub struct BlockSyncStatePort {
    inner: Arc<dyn BlockSyncStatePortImpl>,
}

impl BlockSyncStatePort {
    /// Wrap an implementation in a cloneable port.
    pub fn new(inner: Arc<dyn BlockSyncStatePortImpl>) -> Self {
        Self { inner }
    }

    /// Return the currently cached shared frontier.
    pub fn current_frontier(&self) -> ChainFrontier {
        self.inner.current_frontier()
    }

    /// Subscribe to latest-value shared frontier updates.
    pub fn subscribe_frontier(&self) -> watch::Receiver<FrontierUpdate> {
        self.inner.subscribe_frontier()
    }

    /// Query committed headers that still need block bodies.
    pub fn query_missing_bodies(
        &self,
        verified_body: block::Height,
        best_header: block::Height,
    ) -> BoxFuture<'static, Result<Vec<BlockSyncBlockMeta>, zebra_chain::BoxError>> {
        self.inner.query_missing_bodies(verified_body, best_header)
    }

    /// Submit a downloaded body to the verifier/state pipeline.
    pub fn submit_block_body(
        &self,
        token: BlockApplyToken,
        block: Arc<block::Block>,
    ) -> BoxFuture<'static, BlockBodySubmit> {
        self.inner.submit_block_body(token, block)
    }

    /// Read committed blocks for serving stream-6 peers.
    pub fn read_committed_blocks(
        &self,
        start: block::Height,
        count: u32,
    ) -> BoxFuture<
        'static,
        Result<Vec<(block::Height, Arc<block::Block>, usize)>, zebra_chain::BoxError>,
    > {
        self.inner.read_committed_blocks(start, count)
    }

    /// Publish body-sync progress that did not come from a direct submit path.
    pub fn publish_body_progress(&self, update: FrontierUpdate) -> BoxFuture<'static, ()> {
        self.inner.publish_body_progress(update)
    }
}

impl std::fmt::Debug for BlockSyncStatePort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BlockSyncStatePort")
    }
}

/// Build a [`ChainFrontier`] from startup pieces that do not expose finalized hashes.
pub fn chain_frontier_from_parts(
    finalized_height: block::Height,
    verified_body: Frontier,
    best_header: Frontier,
) -> ChainFrontier {
    let finalized_hash = if finalized_height == verified_body.height {
        verified_body.hash
    } else {
        block::Hash([0; 32])
    };

    ChainFrontier {
        finalized: Frontier::new(finalized_height, finalized_hash),
        verified_body,
        best_header,
    }
}

/// Apply one requested frontier update to the current exchange frontier.
///
/// Returns `None` when the requested update is stale or redundant.
pub fn apply_frontier_update(
    current: FrontierUpdate,
    requested: FrontierUpdate,
) -> Option<FrontierUpdate> {
    let mut frontier = current.frontier;

    match requested.change {
        FrontierChange::Snapshot => {
            if requested.frontier == current.frontier {
                return None;
            }
            frontier = requested.frontier;
        }
        FrontierChange::VerifiedGrow => {
            if requested.frontier.verified_body.height < current.frontier.verified_body.height {
                return None;
            }
            frontier.finalized =
                higher_frontier(current.frontier.finalized, requested.frontier.finalized);
            frontier.verified_body = requested.frontier.verified_body;
        }
        FrontierChange::VerifiedReset => {
            frontier.finalized = requested.frontier.finalized;
            frontier.verified_body = requested.frontier.verified_body;
        }
        FrontierChange::HeaderAdvanced => {
            if requested.frontier.best_header.height <= current.frontier.best_header.height {
                return None;
            }
            frontier.best_header = requested.frontier.best_header;
        }
        FrontierChange::HeaderReanchored => {
            if requested.frontier.best_header == current.frontier.best_header {
                return None;
            }
            frontier.best_header = requested.frontier.best_header;
        }
    }

    Some(FrontierUpdate {
        frontier,
        change: requested.change,
    })
}

fn higher_frontier(left: Frontier, right: Frontier) -> Frontier {
    if right.height > left.height {
        right
    } else {
        left
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frontier(height: u32, seed: u8) -> Frontier {
        Frontier::new(block::Height(height), block::Hash([seed; 32]))
    }

    fn update(
        finalized: Frontier,
        verified_body: Frontier,
        best_header: Frontier,
        change: FrontierChange,
    ) -> FrontierUpdate {
        FrontierUpdate {
            frontier: ChainFrontier {
                finalized,
                verified_body,
                best_header,
            },
            change,
        }
    }

    #[test]
    fn grow_advances_verified_body() {
        let current = update(
            frontier(1, 1),
            frontier(2, 2),
            frontier(5, 5),
            FrontierChange::Snapshot,
        );
        let requested = update(
            frontier(3, 3),
            frontier(4, 4),
            frontier(9, 9),
            FrontierChange::VerifiedGrow,
        );

        let updated = apply_frontier_update(current, requested).expect("grow is accepted");

        assert_eq!(updated.frontier.verified_body, frontier(4, 4));
        assert_eq!(updated.frontier.best_header, frontier(5, 5));
    }

    #[test]
    fn stale_lower_grow_is_ignored() {
        let current = update(
            frontier(1, 1),
            frontier(8, 8),
            frontier(9, 9),
            FrontierChange::Snapshot,
        );
        let requested = update(
            frontier(1, 1),
            frontier(7, 7),
            frontier(9, 9),
            FrontierChange::VerifiedGrow,
        );

        assert!(apply_frontier_update(current, requested).is_none());
    }

    #[test]
    fn reset_may_lower_verified_body() {
        let current = update(
            frontier(5, 5),
            frontier(8, 8),
            frontier(10, 10),
            FrontierChange::Snapshot,
        );
        let requested = update(
            frontier(4, 4),
            frontier(6, 6),
            frontier(2, 2),
            FrontierChange::VerifiedReset,
        );

        let updated = apply_frontier_update(current, requested).expect("reset is accepted");

        assert_eq!(updated.frontier.verified_body, frontier(6, 6));
        assert_eq!(updated.frontier.best_header, frontier(10, 10));
    }

    #[test]
    fn header_reanchor_lowers_only_best_header() {
        let current = update(
            frontier(5, 5),
            frontier(8, 8),
            frontier(12, 12),
            FrontierChange::Snapshot,
        );
        let requested = update(
            frontier(1, 1),
            frontier(2, 2),
            frontier(9, 9),
            FrontierChange::HeaderReanchored,
        );

        let updated = apply_frontier_update(current, requested).expect("reanchor is accepted");

        assert_eq!(updated.frontier.finalized, frontier(5, 5));
        assert_eq!(updated.frontier.verified_body, frontier(8, 8));
        assert_eq!(updated.frontier.best_header, frontier(9, 9));
    }

    #[test]
    fn header_advance_cannot_change_verified_body() {
        let current = update(
            frontier(5, 5),
            frontier(8, 8),
            frontier(12, 12),
            FrontierChange::Snapshot,
        );
        let requested = update(
            frontier(1, 1),
            frontier(2, 2),
            frontier(13, 13),
            FrontierChange::HeaderAdvanced,
        );

        let updated = apply_frontier_update(current, requested).expect("advance is accepted");

        assert_eq!(updated.frontier.finalized, frontier(5, 5));
        assert_eq!(updated.frontier.verified_body, frontier(8, 8));
        assert_eq!(updated.frontier.best_header, frontier(13, 13));
    }
}
