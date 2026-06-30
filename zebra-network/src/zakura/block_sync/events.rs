use super::{request::*, state::*, *};

/// Committed header metadata used by block sync to schedule and validate a body.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BlockSyncBlockMeta {
    /// Header-known block height whose body is missing.
    pub height: block::Height,
    /// Committed header hash expected from the downloaded body.
    pub hash: block::Hash,
    /// Advisory or confirmed body-size estimate for scheduling.
    pub size: BlockSizeEstimate,
}

/// Facts accepted by the block-sync scaffold and later reactor.
///
/// The inbound data flow is inverted: a peer's stream-6 frames are decoded and
/// the download logic runs in the per-peer pipe-routine
/// ([`PeerRoutine`](super::peer_routine)). Inbound messages no longer flow
/// through the reactor as a `WireMessage`; the routine forwards only shared
/// concerns to the reactor over [`RoutineToReactor`].
#[derive(Clone, Debug)]
pub enum BlockSyncEvent {
    /// A peer became available for stream-6 block sync.
    PeerConnected(BlockSyncPeerSession),
    /// A peer disconnected; all of its outstanding work is dropped.
    PeerDisconnected(ZakuraPeerId),
    /// Header sync advanced the committed header target.
    HeaderTipChanged {
        /// Current best header height.
        height: block::Height,
        /// Current best header hash.
        hash: block::Hash,
    },
    /// Locally observed finalized or verified-body frontiers changed.
    StateFrontiersChanged(BlockSyncFrontiers),
    /// State grew the verified body chain tip.
    ChainTipGrow(BlockSyncFrontiers),
    /// State reset the verified body chain tip after a rollback, best-chain switch,
    /// activation boundary, or coalesced multi-block tip update.
    ChainTipReset(BlockSyncFrontiers),
    /// Driver returned the current body-missing, header-known heights with committed hashes.
    NeededBlocks(Vec<BlockSyncBlockMeta>),
}

/// Result of applying a block-sync body through the verifier driver.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockApplyResult {
    /// The block was verified and committed.
    Committed,
    /// The verifier reported the block was already committed.
    Duplicate,
    /// The verifier rejected the block.
    Rejected,
    /// The verifier did not answer before the driver timeout.
    TimedOut,
}

/// Monotonic per-commit sequence id used by the node-side `Committer` to
/// correlate the commit-state trace rows for one body. (Formerly the verifier
/// submission token the in-task apply pipeline echoed back; that pipeline is gone,
/// but the lightweight id is still useful for tracing.)
pub type BlockApplyToken = u64;

/// Verification path used for a submitted block body.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockApplyClass {
    /// Body will be committed through the checkpoint verifier path.
    Checkpoint,
    /// Body will be committed through the full semantic verifier path.
    Full,
}

/// Actions emitted by the future block-sync reactor for the service seam.
#[derive(Clone, Debug)]
pub enum BlockSyncAction {
    /// Ask node wiring to read `missing_block_bodies`, header hashes, and size hints.
    QueryNeededBlocks {
        /// Current verified body tip.
        verified_block_tip: block::Height,
        /// Current best header target.
        best_header_tip: block::Height,
    },
    /// Report peer misbehavior to the supervisor.
    Misbehavior {
        /// Misbehaving peer.
        peer: ZakuraPeerId,
        /// Reason for reporting.
        reason: BlockSyncMisbehavior,
    },
}

/// Block-sync peer-accounting violations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockSyncMisbehavior {
    /// A stream-6 payload was malformed before semantic handling.
    MalformedMessage,
    /// A peer sent blocks that were not requested.
    UnsolicitedBlock,
    /// A peer requested more blocks than this node advertised it can serve.
    GetBlocksTooLong,
    /// A peer exceeded this node's inbound `GetBlocks` serving budget.
    GetBlocksSpam,
    /// A peer supplied a body whose hash or size does not match committed metadata.
    InvalidBlock,
    /// A peer supplied a body outside the tolerated scheduling-size deviation.
    SizeMismatch,
    /// Peer status is internally impossible.
    InvalidStatus,
    /// A response terminator arrived without an outstanding range.
    UnsolicitedDone,
    /// A peer reported a requested range unavailable.
    RangeUnavailable,
    /// A peer sent too many status frames.
    StatusSpam,
}

/// The shared routine→reactor channel (per-peer routines inverted data flow).
///
/// Each per-peer pipe-routine ([`PeerRoutine`](super::peer_routine)) decodes its
/// own frames and runs the download logic locally; it forwards only the concerns
/// that need reactor-global state (status advertisement, the producer,
/// misbehavior aggregation) over this channel. The sender is `try_send`/bounded
/// so a busy reactor never backpressures a routine's decode loop into stalling
/// its transport (the only blocking routine send is the Sequencer `AcceptBody`).
#[derive(Clone, Debug)]
pub(super) enum RoutineToReactor {
    /// A routine received a `Status` and updated its own servable/caps + the
    /// registry. The reactor advertises our `Status` reply and republishes the
    /// candidate set. `send_reply` is the routine's rate-meter decision (the
    /// previous `unsolicited.try_take`) for whether a reply is due this time.
    StatusReceived {
        /// Peer whose status was applied.
        peer: ZakuraPeerId,
        /// Whether the rate meter allows sending a `Status` reply now.
        send_reply: bool,
    },
    /// A routine drained its pending work; the producer should re-query (it
    /// self-gates on low-water, so the ping is idempotent/cheap).
    RequeryNeeded,
    /// A routine scored a peer offense that needs the reactor-side
    /// disconnect/scoring action (serving-side malformed frames report via this
    /// path; download-side offenses score directly through the `actions`
    /// channel + the shared registry count).
    Misbehavior {
        /// Misbehaving peer.
        peer: ZakuraPeerId,
        /// Reason for reporting.
        reason: BlockSyncMisbehavior,
    },
}
