//! Block verifier request type.

use std::sync::Arc;

use zebra_chain::block::{self, Block};
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
    /// Creates a commit request for the downloaded block.
    ///
    /// For checkpoint-height blocks, precompute the checkpoint-verified block
    /// off the verifier's single-threaded buffer worker. Callers should do this
    /// before reserving verifier readiness, so the CPU-heavy work does not hold a
    /// verifier slot.
    pub async fn create_commit_request(
        block: Arc<Block>,
        block_height: block::Height,
        max_checkpoint_height: block::Height,
    ) -> Self {
        if block_height <= max_checkpoint_height {
            let checkpoint_block = tokio::task::spawn_blocking(move || {
                let hash = block.hash();
                CheckpointVerifiedBlock::with_hash(block, hash)
            })
            .await
            .expect("checkpoint block precomputation should not panic");

            Request::CommitCheckpointPrecomputed(checkpoint_block)
        } else {
            Request::Commit(block)
        }
    }

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
