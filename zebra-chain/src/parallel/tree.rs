//! Parallel note commitment tree update methods.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use thiserror::Error;

use crate::{
    block::{self, Block},
    ironwood, orchard,
    parallel::batch_frontier::PARALLEL_HASH_THRESHOLD,
    sapling, sprout,
    subtree::{NoteCommitmentSubtree, NoteCommitmentSubtreeIndex},
};

/// An argument wrapper struct for note commitment trees.
///
/// The default instance represents the trees and subtrees that correspond to the genesis block.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NoteCommitmentTrees {
    /// The sprout note commitment tree.
    pub sprout: Arc<sprout::tree::NoteCommitmentTree>,

    /// The sapling note commitment tree.
    pub sapling: Arc<sapling::tree::NoteCommitmentTree>,

    /// The sapling note commitment subtree.
    pub sapling_subtree: Option<NoteCommitmentSubtree<sapling_crypto::Node>>,

    /// The orchard note commitment tree.
    pub orchard: Arc<orchard::tree::NoteCommitmentTree>,

    /// The orchard note commitment subtree.
    pub orchard_subtree: Option<NoteCommitmentSubtree<orchard::tree::Node>>,

    /// The Ironwood note commitment tree.
    pub ironwood: Arc<ironwood::tree::NoteCommitmentTree>,

    /// The Ironwood note commitment subtree.
    pub ironwood_subtree: Option<NoteCommitmentSubtree<ironwood::tree::Node>>,
}

/// Note commitment tree errors.
#[derive(Error, Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum NoteCommitmentTreeError {
    /// A sprout tree error
    #[error("sprout error: {0}")]
    Sprout(#[from] sprout::tree::NoteCommitmentTreeError),

    /// A sapling tree error
    #[error("sapling error: {0}")]
    Sapling(#[from] sapling::tree::NoteCommitmentTreeError),

    /// A orchard tree error
    #[error("orchard error: {0}")]
    Orchard(#[from] orchard::tree::NoteCommitmentTreeError),

    /// An Ironwood tree error.
    #[error("ironwood error: {0}")]
    Ironwood(ironwood::tree::NoteCommitmentTreeError),
}

impl NoteCommitmentTrees {
    /// Updates the note commitment trees using the transactions in `block`,
    /// then re-calculates the cached tree roots, using parallel `rayon` threads.
    ///
    /// If any of the tree updates cause an error,
    /// it will be returned at the end of the parallel batches.
    #[allow(clippy::unwrap_in_result)]
    pub fn update_trees_parallel(
        &mut self,
        block: &Arc<Block>,
    ) -> Result<(), NoteCommitmentTreeError> {
        self.update_trees_parallel_with(block, None)
    }

    /// Like [`update_trees_parallel`](Self::update_trees_parallel), but applies a
    /// [`BlockNotePrecompute`] computed ahead of time off the committer when one is
    /// supplied and still matches the current tree sizes.
    ///
    /// The Sapling/Orchard per-leaf Merkle hashing is the dominant cost of
    /// committing a shielded block; precomputing it concurrently (keyed only on the
    /// note position) lets the committer do just the cheap apply the precomputed subtree roots. A `None` or
    /// size-mismatched precompute transparently falls back to hashing inline, so the
    /// result is always identical to the plain update.
    #[allow(clippy::unwrap_in_result)]
    pub fn update_trees_parallel_with(
        &mut self,
        block: &Arc<Block>,
        precompute: Option<BlockNotePrecompute>,
    ) -> Result<(), NoteCommitmentTreeError> {
        let block = block.clone();
        let height = block
            .coinbase_height()
            .expect("height was already validated");

        // Prepare arguments for parallel threads
        let NoteCommitmentTrees {
            sprout,
            sapling,
            orchard,
            ironwood,
            ..
        } = self.clone();

        let sprout_note_commitments: Vec<_> = block.sprout_note_commitments().cloned().collect();
        let sapling_note_commitments: Vec<_> = block.sapling_note_commitments().cloned().collect();
        let orchard_note_commitments: Vec<_> = block.orchard_note_commitments().cloned().collect();
        let ironwood_note_commitments: Vec<_> =
            block.ironwood_note_commitments().cloned().collect();

        // Only use the precompute if it was computed for this exact block. A
        // precompute is otherwise keyed only by starting tree size, so without this
        // check one accidentally paired with a different block of the same starting
        // size would apply the wrong leaves and silently produce a wrong root. A
        // mismatch (or `None`) falls back to inline hashing, which is correct, just
        // slower — so this can only cost speed, never correctness.
        let (sapling_precompute, orchard_precompute) = match precompute {
            Some(p) if p.block_hash == block.hash() => (p.sapling, p.orchard),
            _ => (None, None),
        };

        let mut sprout_result = None;
        let mut sapling_result = None;
        let mut orchard_result = None;
        let mut ironwood_result = None;

        rayon::in_place_scope_fifo(|scope| {
            if !sprout_note_commitments.is_empty() {
                scope.spawn_fifo(|_scope| {
                    sprout_result = Some(Self::update_sprout_note_commitment_tree(
                        sprout,
                        sprout_note_commitments,
                    ));
                });
            }

            if !sapling_note_commitments.is_empty() {
                scope.spawn_fifo(|_scope| {
                    sapling_result = Some(Self::update_sapling_note_commitment_tree_with(
                        sapling,
                        sapling_note_commitments,
                        sapling_precompute,
                    ));
                });
            }

            if !orchard_note_commitments.is_empty() {
                scope.spawn_fifo(|_scope| {
                    orchard_result = Some(Self::update_orchard_note_commitment_tree_with(
                        orchard,
                        orchard_note_commitments,
                        orchard_precompute,
                    ));
                });
            }

            if !ironwood_note_commitments.is_empty() {
                scope.spawn_fifo(|_scope| {
                    ironwood_result = Some(Self::update_ironwood_note_commitment_tree(
                        ironwood,
                        ironwood_note_commitments,
                    ));
                });
            }
        });

        if let Some(sprout_result) = sprout_result {
            self.sprout = sprout_result?;
        }

        if let Some(sapling_result) = sapling_result {
            let (sapling, subtree_root) = sapling_result?;
            self.sapling = sapling;
            self.sapling_subtree =
                subtree_root.map(|(idx, node)| NoteCommitmentSubtree::new(idx, height, node));
        };

        if let Some(orchard_result) = orchard_result {
            let (orchard, subtree_root) = orchard_result?;
            self.orchard = orchard;
            self.orchard_subtree =
                subtree_root.map(|(idx, node)| NoteCommitmentSubtree::new(idx, height, node));
        };

        if let Some(ironwood_result) = ironwood_result {
            let (ironwood, subtree_root) = ironwood_result?;
            self.ironwood = ironwood;
            self.ironwood_subtree =
                subtree_root.map(|(idx, node)| NoteCommitmentSubtree::new(idx, height, node));
        };

        Ok(())
    }

    /// Update the sprout note commitment tree.
    /// This method modifies the tree inside the `Arc`, if the `Arc` only has one reference.
    fn update_sprout_note_commitment_tree(
        mut sprout: Arc<sprout::tree::NoteCommitmentTree>,
        sprout_note_commitments: Vec<sprout::NoteCommitment>,
    ) -> Result<Arc<sprout::tree::NoteCommitmentTree>, NoteCommitmentTreeError> {
        let sprout_nct = Arc::make_mut(&mut sprout);

        for sprout_note_commitment in sprout_note_commitments {
            sprout_nct.append(sprout_note_commitment)?;
        }

        // Re-calculate and cache the tree root.
        let _ = sprout_nct.root();

        Ok(sprout)
    }

    /// Update the sapling note commitment tree.
    /// This method modifies the tree inside the `Arc`, if the `Arc` only has one reference.
    #[allow(clippy::unwrap_in_result)]
    pub fn update_sapling_note_commitment_tree(
        mut sapling: Arc<sapling::tree::NoteCommitmentTree>,
        sapling_note_commitments: Vec<sapling::tree::NoteCommitmentUpdate>,
    ) -> Result<
        (
            Arc<sapling::tree::NoteCommitmentTree>,
            Option<(NoteCommitmentSubtreeIndex, sapling_crypto::Node)>,
        ),
        NoteCommitmentTreeError,
    > {
        let sapling_nct = Arc::make_mut(&mut sapling);

        // It is impossible for blocks to contain more than one level 16 sapling root:
        // > [NU5 onward] nSpendsSapling, nOutputsSapling, and nActionsOrchard MUST all be less than 2^16.
        // <https://zips.z.cash/protocol/protocol.pdf#txnconsensus>
        //
        // Before NU5, this limit holds due to the minimum size of Sapling outputs (948 bytes)
        // and the maximum size of a block:
        // > The size of a block MUST be less than or equal to 2000000 bytes.
        // <https://zips.z.cash/protocol/protocol.pdf#blockheader>
        // <https://zips.z.cash/protocol/protocol.pdf#txnencoding>
        //
        // The note commitments are appended as a single parallel batch, which
        // returns the (at most one) subtree completed within this block, matching
        // the per-leaf append exactly (see `crate::parallel::batch_frontier`).
        let subtree_root = sapling_nct.append_batch(&sapling_note_commitments)?;

        // Re-calculate and cache the tree root.
        let _ = sapling_nct.root();

        Ok((sapling, subtree_root))
    }

    /// Update the orchard note commitment tree.
    /// This method modifies the tree inside the `Arc`, if the `Arc` only has one reference.
    #[allow(clippy::unwrap_in_result)]
    pub fn update_orchard_note_commitment_tree(
        mut orchard: Arc<orchard::tree::NoteCommitmentTree>,
        orchard_note_commitments: Vec<orchard::tree::NoteCommitmentUpdate>,
    ) -> Result<
        (
            Arc<orchard::tree::NoteCommitmentTree>,
            Option<(NoteCommitmentSubtreeIndex, orchard::tree::Node)>,
        ),
        NoteCommitmentTreeError,
    > {
        let orchard_nct = Arc::make_mut(&mut orchard);

        // It is impossible for blocks to contain more than one level 16 orchard root:
        // > [NU5 onward] nSpendsSapling, nOutputsSapling, and nActionsOrchard MUST all be less than 2^16.
        // <https://zips.z.cash/protocol/protocol.pdf#txnconsensus>
        //
        // The note commitments are appended as a single parallel batch, which
        // returns the (at most one) subtree completed within this block, matching
        // the per-leaf append exactly (see `crate::parallel::batch_frontier`).
        let subtree_root = orchard_nct.append_batch(&orchard_note_commitments)?;

        // Re-calculate and cache the tree root.
        let _ = orchard_nct.root();

        Ok((orchard, subtree_root))
    }

    /// Update the Ironwood note commitment tree.
    /// This method modifies the tree inside the `Arc`, if the `Arc` only has one reference.
    #[allow(clippy::unwrap_in_result)]
    pub fn update_ironwood_note_commitment_tree(
        mut ironwood: Arc<ironwood::tree::NoteCommitmentTree>,
        ironwood_note_commitments: Vec<ironwood::tree::NoteCommitmentUpdate>,
    ) -> Result<
        (
            Arc<ironwood::tree::NoteCommitmentTree>,
            Option<(NoteCommitmentSubtreeIndex, ironwood::tree::Node)>,
        ),
        NoteCommitmentTreeError,
    > {
        let ironwood_nct = Arc::make_mut(&mut ironwood);

        // It is impossible for blocks to contain more than one level 16 Ironwood root:
        // > [NU6.3 onward] nActionsIronwood MUST be less than 2^16.
        // <https://zips.z.cash/protocol/protocol.pdf#txnconsensus>
        //
        // The note commitments are appended as a single parallel batch, which
        // returns the (at most one) subtree completed within this block, matching
        // the per-leaf append exactly (see `crate::parallel::batch_frontier`).
        let subtree_root = ironwood_nct
            .append_batch(&ironwood_note_commitments)
            .map_err(NoteCommitmentTreeError::Ironwood)?;

        // Re-calculate and cache the tree root.
        let _ = ironwood_nct.root();

        Ok((ironwood, subtree_root))
    }

    /// Like [`update_sapling_note_commitment_tree`](Self::update_sapling_note_commitment_tree),
    /// but applies `precompute` (off-committer parallel hashing) when present and its
    /// `start_size` still matches the tree; otherwise hashes inline. Identical result.
    #[allow(clippy::unwrap_in_result)]
    pub(crate) fn update_sapling_note_commitment_tree_with(
        mut sapling: Arc<sapling::tree::NoteCommitmentTree>,
        sapling_note_commitments: Vec<sapling::tree::NoteCommitmentUpdate>,
        precompute: Option<sapling::tree::PrecomputedAppendBatch>,
    ) -> Result<
        (
            Arc<sapling::tree::NoteCommitmentTree>,
            Option<(NoteCommitmentSubtreeIndex, sapling_crypto::Node)>,
        ),
        NoteCommitmentTreeError,
    > {
        let sapling_nct = Arc::make_mut(&mut sapling);

        let subtree_root = match precompute {
            Some(pre) if pre.start_size() == sapling_nct.count() => {
                sapling_nct.apply_precomputed_append(pre)?
            }
            _ => sapling_nct.append_batch(&sapling_note_commitments)?,
        };

        // Re-calculate and cache the tree root.
        let _ = sapling_nct.root();

        Ok((sapling, subtree_root))
    }

    /// Like [`update_orchard_note_commitment_tree`](Self::update_orchard_note_commitment_tree),
    /// but applies `precompute` when present and size-matched; otherwise inline. Identical result.
    #[allow(clippy::unwrap_in_result)]
    pub(crate) fn update_orchard_note_commitment_tree_with(
        mut orchard: Arc<orchard::tree::NoteCommitmentTree>,
        orchard_note_commitments: Vec<orchard::tree::NoteCommitmentUpdate>,
        precompute: Option<orchard::tree::PrecomputedAppendBatch>,
    ) -> Result<
        (
            Arc<orchard::tree::NoteCommitmentTree>,
            Option<(NoteCommitmentSubtreeIndex, orchard::tree::Node)>,
        ),
        NoteCommitmentTreeError,
    > {
        let orchard_nct = Arc::make_mut(&mut orchard);

        let subtree_root = match precompute {
            Some(pre) if pre.start_size() == orchard_nct.count() => {
                orchard_nct.apply_precomputed_append(pre)?
            }
            _ => orchard_nct.append_batch(&orchard_note_commitments)?,
        };

        // Re-calculate and cache the tree root.
        let _ = orchard_nct.root();

        Ok((orchard, subtree_root))
    }
}

/// The off-committer precomputed parallel-append work for one block's Sapling and
/// Orchard note commitments, produced by [`BlockNotePrecompute::compute`] and applied
/// via [`NoteCommitmentTrees::update_trees_parallel_with`].
#[derive(Clone, Debug)]
pub struct BlockNotePrecompute {
    /// The hash of the block this precompute was computed for. The committer
    /// applies the precompute only to this exact block, so a precompute that was
    /// accidentally paired with a different block (even one with the same starting
    /// tree size) is rejected instead of applying the wrong leaves. See
    /// [`NoteCommitmentTrees::update_trees_parallel_with`].
    pub(crate) block_hash: block::Hash,
    /// Precomputed Sapling append, if the block has Sapling outputs.
    pub(crate) sapling: Option<sapling::tree::PrecomputedAppendBatch>,
    /// Precomputed Orchard append, if the block has Orchard actions.
    pub(crate) orchard: Option<orchard::tree::PrecomputedAppendBatch>,
}

impl BlockNotePrecompute {
    /// Precomputes the Sapling and Orchard per-leaf Merkle hashing for `block`,
    /// given the tree sizes (cumulative note counts) the block will commit at.
    ///
    /// Runs off the committer, concurrently across blocks. The committer then only
    /// applies the precomputed subtree roots. `sapling_start` / `orchard_start` are the respective tree `count`s
    /// immediately before this block; the committer re-checks them and falls back to
    /// inline hashing on any mismatch. Pools with no notes (or a precompute error)
    /// are left `None`, also falling back to inline.
    ///
    /// The Sapling and Orchard precomputes run concurrently via [`rayon::join`],
    /// mirroring the per-pool parallelism of [`NoteCommitmentTrees::update_trees_parallel`]:
    /// each pool's hashing is already internally parallel, and the join lets the two
    /// pools overlap. For small blocks (both pools below [`PARALLEL_HASH_THRESHOLD`])
    /// they are computed sequentially, since there is too little hashing to repay the
    /// cross-pool join.
    ///
    /// # Cancellation
    ///
    /// This is started speculatively for the *next* block while the *current* block
    /// is still committing, so a failed or invalid current block leaves the work
    /// unwanted (the committer drops the receiver). `cancel` lets the writer abort it:
    /// the flag is checked once up front and again at the start of each pool's hashing,
    /// so a cancel that lands before a pool starts skips that pool's work. (Once a
    /// pool's hashing is under way it runs to completion — the bound is best-effort,
    /// not interrupt-in-the-middle.) A cancelled call returns an empty precompute,
    /// which the committer treats like any other miss and hashes inline.
    pub fn compute(
        sapling_start: u64,
        orchard_start: u64,
        block: &Block,
        cancel: &AtomicBool,
    ) -> Self {
        let block_hash = block.hash();

        if cancel.load(Ordering::Relaxed) {
            return Self {
                block_hash,
                sapling: None,
                orchard: None,
            };
        }

        let sapling_notes: Vec<_> = block.sapling_note_commitments().cloned().collect();
        let orchard_notes: Vec<_> = block.orchard_note_commitments().cloned().collect();

        let sapling_fn = || {
            if cancel.load(Ordering::Relaxed) || sapling_notes.is_empty() {
                return None;
            }
            sapling::tree::NoteCommitmentTree::precompute_append(sapling_start, &sapling_notes).ok()
        };
        let orchard_fn = || {
            if cancel.load(Ordering::Relaxed) || orchard_notes.is_empty() {
                return None;
            }
            orchard::tree::NoteCommitmentTree::precompute_append(orchard_start, &orchard_notes).ok()
        };

        let overlap_pools = sapling_notes.len() >= PARALLEL_HASH_THRESHOLD
            || orchard_notes.len() >= PARALLEL_HASH_THRESHOLD;
        let (sapling, orchard) = if overlap_pools {
            rayon::join(sapling_fn, orchard_fn)
        } else {
            (sapling_fn(), orchard_fn())
        };

        Self {
            block_hash,
            sapling,
            orchard,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialization::ZcashDeserialize;

    /// A precompute started speculatively for the next block is cancellable: when
    /// the writer trips the flag (because the current block's commit failed and the
    /// child will be discarded), `compute` returns an empty precompute instead of
    /// hashing the block. Uses a real NU5 block with Sapling notes; the flag check
    /// is identical for the Orchard pool.
    #[test]
    fn block_note_precompute_respects_cancellation() {
        let _init_guard = zebra_test::init();

        let block =
            Block::zcash_deserialize(zebra_test::vectors::BLOCK_MAINNET_1687106_BYTES.as_slice())
                .expect("hard-coded NU5 block vector deserializes");

        // Precondition: the block exercises the Sapling pool.
        assert!(
            block.sapling_note_commitments().next().is_some(),
            "test block must have Sapling notes"
        );

        // Not cancelled: the Sapling pool is precomputed.
        let live = BlockNotePrecompute::compute(0, 0, &block, &AtomicBool::new(false));
        assert!(
            live.sapling.is_some(),
            "a live precompute hashes the populated pool"
        );

        // Cancelled before it runs: no hashing, an empty precompute the committer
        // treats as a miss (hashing inline instead).
        let cancelled = BlockNotePrecompute::compute(0, 0, &block, &AtomicBool::new(true));
        assert!(
            cancelled.sapling.is_none() && cancelled.orchard.is_none(),
            "a cancelled precompute does no work"
        );
    }

    /// A precompute is bound to the block it was computed for: applying one built for
    /// a *different* block — even with the same starting tree size, which the
    /// size-only guard would have accepted — must be rejected and fall back to inline
    /// hashing, so it can never silently graft the wrong block's leaves.
    #[test]
    fn precompute_is_bound_to_its_block() {
        let _init_guard = zebra_test::init();

        // Two distinct blocks that both add Sapling notes.
        let candidates: [&[u8]; 6] = [
            zebra_test::vectors::BLOCK_MAINNET_1687106_BYTES.as_slice(),
            zebra_test::vectors::BLOCK_MAINNET_1687107_BYTES.as_slice(),
            zebra_test::vectors::BLOCK_MAINNET_1687108_BYTES.as_slice(),
            zebra_test::vectors::BLOCK_MAINNET_1687113_BYTES.as_slice(),
            zebra_test::vectors::BLOCK_MAINNET_1687118_BYTES.as_slice(),
            zebra_test::vectors::BLOCK_MAINNET_1687121_BYTES.as_slice(),
        ];
        let sapling_blocks: Vec<Block> = candidates
            .iter()
            .map(|bytes| Block::zcash_deserialize(*bytes).expect("block vector deserializes"))
            .filter(|block| block.sapling_note_commitments().next().is_some())
            .collect();
        assert!(
            sapling_blocks.len() >= 2,
            "need two distinct Sapling blocks for this test"
        );

        let block_a = Arc::new(sapling_blocks[0].clone());
        let block_b = sapling_blocks[1].clone();
        assert_ne!(block_a.hash(), block_b.hash(), "blocks must differ");

        // The correct trees for committing block A onto the genesis trees.
        let mut correct = NoteCommitmentTrees::default();
        correct
            .update_trees_parallel(&block_a)
            .expect("appending block A's notes succeeds");

        // A precompute built for block B at the same starting tree size (0) as A: its
        // `start_size` matches A's tree, so the size-only guard would have applied B's
        // leaves. The block-hash binding must reject it instead.
        let pre_b = BlockNotePrecompute::compute(0, 0, &block_b, &AtomicBool::new(false));
        assert!(
            pre_b.sapling.is_some(),
            "block B exercises the Sapling pool"
        );

        let mut mismatched = NoteCommitmentTrees::default();
        mismatched
            .update_trees_parallel_with(&block_a, Some(pre_b))
            .expect("update succeeds");
        assert_eq!(
            mismatched.sapling.root(),
            correct.sapling.root(),
            "a precompute for a different block must be rejected, not grafted"
        );

        // The correctly-bound precompute for A is still applied and matches.
        let pre_a = BlockNotePrecompute::compute(0, 0, &block_a, &AtomicBool::new(false));
        let mut matched = NoteCommitmentTrees::default();
        matched
            .update_trees_parallel_with(&block_a, Some(pre_a))
            .expect("update succeeds");
        assert_eq!(
            matched.sapling.root(),
            correct.sapling.root(),
            "a precompute bound to this block is applied"
        );
    }
}
