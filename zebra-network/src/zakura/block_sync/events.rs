use super::{wire::*, *};

/// Facts accepted by the block-sync scaffold and later reactor.
#[derive(Clone, Debug)]
pub enum BlockSyncEvent {
    /// A peer became available for stream-6 block sync.
    PeerConnected(BlockSyncPeerSession),
    /// A peer disconnected; all of its outstanding work is dropped.
    PeerDisconnected(ZakuraPeerId),
    /// Inbound stream-6 message from `peer`.
    WireMessage {
        /// Serving peer.
        peer: ZakuraPeerId,
        /// Decoded stream-6 message.
        msg: BlockSyncMessage,
    },
    /// Stream-6 frame decoding failed after handler admission.
    WireDecodeFailed {
        /// Peer that sent the malformed frame.
        peer: ZakuraPeerId,
        /// Decode/validation error.
        error: Arc<BlockSyncWireError>,
    },
}

/// Actions emitted by the future block-sync reactor for the service seam.
#[derive(Clone, Debug)]
pub enum BlockSyncAction {
    /// Queue a typed stream-6 message to a peer.
    SendMessage {
        /// Destination peer.
        peer: ZakuraPeerId,
        /// Message that should be written to the peer's stream.
        msg: BlockSyncMessage,
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
}
