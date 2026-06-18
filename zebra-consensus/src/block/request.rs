//! Block verifier request type.

use std::sync::Arc;

use zebra_chain::block::Block;
use zebra_state::CheckpointVerifiedBlock;

#[derive(Debug, Clone, PartialEq, Eq)]
/// A request to the chain or block verifier
pub enum Request {
    /// Performs semantic validation, then asks the state to perform contextual validation and commit the block
    Commit(Arc<Block>),

    /// Like [`Request::Commit`], but the (CPU-heavy) checkpoint-verifier
    /// precomputation — the per-transaction txids and the auth data root — has
    /// already been done by the caller, off the single-threaded checkpoint
    /// verifier.
    ///
    /// Only valid below the checkpoint height; the verifier still performs all
    /// validity checks (proof of work, Merkle root, height). Used by the syncer,
    /// which can build these blocks concurrently across many download tasks.
    CommitCheckpointPrecomputed(CheckpointVerifiedBlock),

    /// Performs semantic validation but skips checking proof of work,
    /// then asks the state to perform contextual validation.
    /// Does not commit the block to the state.
    CheckProposal(Arc<Block>),
}

impl Request {
    /// Returns inner block
    pub fn block(&self) -> Arc<Block> {
        match self {
            Request::Commit(block) => Arc::clone(block),
            Request::CommitCheckpointPrecomputed(block) => Arc::clone(&block.block),
            Request::CheckProposal(block) => Arc::clone(block),
        }
    }

    /// Returns `true` if the request is a proposal
    pub fn is_proposal(&self) -> bool {
        match self {
            Request::Commit(_) | Request::CommitCheckpointPrecomputed(_) => false,
            Request::CheckProposal(_) => true,
        }
    }
}
