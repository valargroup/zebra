//! The apply-side seam between the block-sync Sequencer and the commit pump.
//!
//! The Sequencer drains its contiguous reorder prefix into [`ApplyItem`]s and
//! pushes them onto the `applyQ` (`mpsc::Sender<ApplyItem>`). A `Committer` task
//! living in node wiring (it names the consensus verifier, which `zebra-network`
//! cannot) drains the queue and fires `Request::Commit` for each item. The two
//! halves communicate only through this channel and a single reverse edge,
//! [`CommitterReset`], which the Committer raises when consensus rejects a body or
//! a commit times out.
//!
//! These types live here, in the lower crate, so the channel that carries them can
//! cross the `zebra-network` → node-wiring boundary; they are re-exported from the
//! crate root for the node-side Committer to name.

use super::*;

/// One contiguous, hash-verified block body ready to commit.
///
/// Items are produced by the Sequencer in strictly ascending, gap-free height
/// order starting at `durable_tip + 1`, so the Committer needs no completion
/// feedback to preserve ordering — contiguity is guaranteed by the producer.
#[derive(Clone, Debug)]
pub struct ApplyItem {
    /// Body height; ascending and gap-free across the queue.
    pub height: block::Height,
    /// Committed header hash this body already matched on receipt.
    pub hash: block::Hash,
    /// Parent-first block body to verify and commit.
    pub block: Arc<block::Block>,
    /// Actual serialized body size, already settled by the download side. Carried
    /// so a rejection can be attributed and the held byte reservation reconciled.
    pub bytes: u64,
    /// Peer that delivered this body, for misbehavior attribution on commit
    /// failure.
    pub source_peer: ZakuraPeerId,
    /// Reset generation stamped by the Sequencer. The Committer discards items
    /// whose epoch is stale (`<= last_reset_epoch`) so a rejection's successors
    /// already in flight do not commit out from under a reset.
    pub epoch: u64,
}

/// Why the Committer is asking the Sequencer to roll back from a height.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CommitRejection {
    /// Consensus reported the body invalid (or returned an unexpected hash). The
    /// Sequencer scores the delivering peer for misbehavior.
    Invalid,
    /// The commit did not complete before the local apply timeout. This is a local
    /// stall, not a peer fault, so the peer is not scored.
    TimedOut,
}

/// The single reverse edge from the Committer back to the Sequencer.
///
/// Raised only when a commit fails (near-never on the checkpoint path). It carries
/// the failed height, the epoch it was stamped with (so a stale reset is ignored),
/// the delivering peer, and whether the failure should score misbehavior. The
/// Sequencer rolls its floor back below `height`, drops the rejected body and its
/// successors, releases their held bytes, and — for [`CommitRejection::Invalid`] —
/// emits one `Misbehavior` action against `source_peer`.
#[derive(Clone, Debug)]
pub struct CommitterReset {
    /// Lowest height to roll back to (this body and every successor are dropped).
    pub height: block::Height,
    /// Epoch the failed item was stamped with; a stale reset (older epoch) is a
    /// no-op.
    pub epoch: u64,
    /// Peer that delivered the failed body, for misbehavior attribution.
    pub source_peer: ZakuraPeerId,
    /// Whether the failure should score the delivering peer.
    pub rejection: CommitRejection,
}
