//! Parallel batch append for incremental Merkle [`Frontier`]s.
//!
//! Note commitment trees (Sprout, Sapling, Orchard) are all
//! [`incrementalmerkletree::frontier::Frontier<H, 32>`], differing only in the
//! per-pool [`Hashable::combine`] hash (SHA-256 / Pedersen / Sinsemilla). The
//! standard [`Frontier::append`] adds one leaf at a time, performing the Merkle
//! merge hashes sequentially. For a block with many shielded outputs this is the
//! dominant cost of committing the block, and it runs on a single thread.
//!
//! [`parallel_append`] produces a [`Frontier`] **byte-identical** to appending
//! each leaf sequentially, but computes the internal Merkle hashes with a
//! parallel divide-and-conquer reduction across the rayon thread pool.
//!
//! # Correctness
//!
//! This is consensus-critical: the frontier *is* the note commitment tree
//! commitment, so the result must match the sequential append exactly. The
//! implementation is validated by differential property tests against the
//! sequential [`Frontier::append`] (identical frontier parts and identical root)
//! in the `tests` module below.

use incrementalmerkletree::{
    frontier::{Frontier, FrontierError},
    Hashable, Level, Position,
};
use rayon::prelude::*;
use std::{error::Error, fmt};

/// A pure binary-counter forest of a contiguous run of leaves, indexed by level:
/// `slots[L] == Some(root)` iff bit `L` of the run length is set, in which case
/// `root` is the root of the complete `2^L`-leaf subtree covering that aligned
/// block. Higher set bits (older subtrees) are further left in leaf order.
type LevelSlots<H> = Vec<Option<H>>;

/// Errors from batch frontier updates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchFrontierError {
    /// A frontier reconstruction error.
    Frontier(FrontierError),

    /// The batch would complete more than one tracked subtree.
    BatchSpansMultipleSubtrees,
}

impl fmt::Display for BatchFrontierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchFrontierError::Frontier(error) => {
                write!(f, "frontier reconstruction error: {error:?}")
            }
            BatchFrontierError::BatchSpansMultipleSubtrees => {
                write!(f, "batch spans more than one tracked subtree boundary")
            }
        }
    }
}

impl Error for BatchFrontierError {}

impl From<FrontierError> for BatchFrontierError {
    fn from(error: FrontierError) -> Self {
        BatchFrontierError::Frontier(error)
    }
}

/// Adds one complete subtree to the frontier's binary-counter forest.
///
/// Callers merge subtrees from left to right, so an occupied slot is always the
/// older left child and the new carry is always the newer right child.
fn merge_complete_subtree<H: Hashable + Clone>(slots: &mut LevelSlots<H>, level: usize, node: H) {
    let mut idx = level;
    let mut carry = node;
    loop {
        match slots[idx].take() {
            None => {
                slots[idx] = Some(carry);
                break;
            }
            Some(existing) => {
                // Combining two level-`idx` nodes yields a level-`idx+1` node;
                // the crate's `combine` takes the *children's* level.
                carry = H::combine(Level::from(idx as u8), &existing, &carry);
                idx += 1;
            }
        }
    }
}

/// Computes the root of a perfect subtree of exactly `2^k` `leaves`, using a
/// parallel divide-and-conquer reduction. The combine hashes within and across
/// the two halves are independent, so this scales across the rayon pool.
fn perfect_subtree_root<H: Hashable + Clone + Send + Sync>(leaves: &[H]) -> H {
    debug_assert!(leaves.len().is_power_of_two());
    if leaves.len() == 1 {
        return leaves[0].clone();
    }
    let half = leaves.len() / 2;
    // children level = log2(len) - 1 = log2(half)
    let child_level = Level::from(half.trailing_zeros() as u8);
    let (left, right) = leaves.split_at(half);
    let (l, r) = rayon::join(
        || perfect_subtree_root(left),
        || perfect_subtree_root(right),
    );
    H::combine(child_level, &l, &r)
}

/// Appends `new_leaves` (in order) to `frontier`, returning the updated frontier.
///
/// The result is identical to calling [`Frontier::append`] for each leaf in turn,
/// but the Merkle merge hashes are computed in parallel.
///
/// # Method
///
/// A frontier of size `S` stores the last leaf raw plus `ommers` that are exactly
/// the pure forest of the first `S - 1` leaves. We:
/// 1. rebuild that forest from `ommers` (bit `L` of `position` set ⇒ one ommer);
/// 2. merge the old tip leaf, giving the pure forest of all `S` leaves;
/// 3. append every new leaf except the last as **globally position-aligned
///    dyadic blocks** — each block's root computed in parallel — merging them
///    in ascending position order (aligned blocks compose with no cross-boundary
///    re-pairing, which is what makes the parallel reduction exact);
/// 4. the new last leaf becomes the frontier's raw leaf, and the resulting forest
///    becomes its ommers.
///
/// Returns [`FrontierError`] if appending would overflow the tree's `DEPTH` capacity.
pub fn parallel_append<H, const DEPTH: u8>(
    frontier: Frontier<H, DEPTH>,
    new_leaves: Vec<H>,
) -> Result<Frontier<H, DEPTH>, FrontierError>
where
    H: Hashable + Clone + Send + Sync,
{
    if new_leaves.is_empty() {
        return Ok(frontier);
    }

    let old_tree_size = frontier.tree_size();
    let Some(new_tree_size) = old_tree_size.checked_add(new_leaves.len() as u64) else {
        return Err(FrontierError::MaxDepthExceeded {
            depth: DEPTH.saturating_add(1),
        });
    };
    let max_tree_size = 1u64
        .checked_shl(u32::from(DEPTH))
        .expect("Zcash note commitment tree depth fits in u64");
    if new_tree_size > max_tree_size {
        return Err(FrontierError::MaxDepthExceeded {
            depth: DEPTH.saturating_add(1),
        });
    }

    // Pre-size one slot for each valid Merkle level.
    let empty_slots = || vec![None; usize::from(DEPTH)];

    let (mut slots, mut old_size): (LevelSlots<H>, u64) = match frontier.value() {
        None => (empty_slots(), 0),
        Some(f) => {
            let (position, leaf, ommers) = (f.position(), f.leaf().clone(), f.ommers().to_vec());
            let pos = u64::from(position); // = S - 1
                                           // ommers (low→high) sit at the set bits of `pos`.
            let mut slots = empty_slots();
            let mut ommers = ommers.into_iter();
            for level in 0..u64::BITS {
                if pos & (1 << level) != 0 {
                    slots[level as usize] = Some(ommers.next().expect("ommer per set bit"));
                }
            }
            // Merge the old tip leaf (position `pos`) to get the pure forest of S leaves.
            merge_complete_subtree(&mut slots, 0, leaf);
            // The next free position is one past the current tip.
            (slots, pos + 1)
        }
    };

    // The new last leaf stays raw; everything before it joins the forest.
    let last = new_leaves.len() - 1;
    let body = &new_leaves[..last];

    // Format: Partition `body` (all new leaves except the last) into maximal power-of-two-sized blocks,
    // aligned to powers-of-two with respect to the global leaf index (the current size).
    //
    // Each block has width 2^L, starts at an index divisible by 2^L, and doesn't extend past `end`.
    // This ensures each block can be processed independently as a perfect subtree.
    //
    // Example: with a tree size of 6 and 7 new leaves (body indices 6–12):
    //   Indices [6–7]   -> 2-leaf block (level=1, width=2, 6 is divisible by 2)
    //   Indices [8–11]  -> 4-leaf block (level=2, width=4, 8 is divisible by 4)
    //   Index  [12]     -> 1-leaf block (level=0, width=1, 12 is divisible by 1)
    //
    // Partitioning is done left to right, respecting block boundaries and maximal size.
    let mut complete_subtree_blocks: Vec<(usize, &[H])> = Vec::new();
    {
        // global_pos and body_offset always move together. They track the same
        // position in two different coordinate systems.
        // cursor in the global leaf index space
        let mut global_pos = old_size;
        // cursor in the body-slice space
        let mut body_offset = 0usize;
        let body_end = old_size + body.len() as u64;

        while global_pos < body_end {
            // "How large a power-of-two block can start here, given alignment?"
            // Find the largest level `L` such that:
            //   1. global_pos % 2^L == 0 (align_level)
            //   2. global_pos + 2^L <= body_end (block fits within remaining body leaves)
            // When global_pos == 0 any power-of-two size is aligned, so treat
            // the alignment level as infinite (u64::BITS > any fit_level).
            let align_level = if global_pos == 0 {
                u64::BITS
            } else {
                global_pos.trailing_zeros()
            };
            // "How many leaves are left to be assigned to blocks?"
            let leaves_left = body_end - global_pos;

            // "How large a block fits in what's left?"
            // floor(log2(leaves_left))
            let fit_level = u64::BITS - 1 - leaves_left.leading_zeros();

            let level = align_level.min(fit_level) as usize;
            let block_width = 1usize << level; // 2^level
            complete_subtree_blocks.push((level, &body[body_offset..body_offset + block_width]));
            body_offset += block_width;
            global_pos += block_width as u64;
        }
    }

    // Compute all block roots concurrently (and each reduction is itself
    // internally parallel). `par_iter().collect()` preserves order, so merging
    // below stays in ascending position order, keeping the carry order exact.
    let roots: Vec<(usize, H)> = complete_subtree_blocks
        .into_par_iter()
        .map(|(level, leaves)| (level, perfect_subtree_root(leaves)))
        .collect();
    for (level, root) in roots {
        merge_complete_subtree(&mut slots, level, root);
    }
    old_size += body.len() as u64;

    // The final frontier: raw last leaf at `position = size`, ommers = forest
    // (compacted low→high, matching the set bits of `position`).
    let position = Position::from(old_size);
    let leaf = new_leaves[last].clone();
    let ommers: Vec<H> = slots.into_iter().flatten().collect();

    Frontier::from_parts(position, leaf, ommers)
}

/// Appends `nodes` to `frontier` and returns the completed subtree's
/// `(index_value, root)` if the batch crosses a
/// [`TRACKED_SUBTREE_HEIGHT`](crate::subtree::TRACKED_SUBTREE_HEIGHT) boundary.
///
/// This is the shared implementation for [`crate::sapling::tree::NoteCommitmentTree::append_batch`]
/// and [`crate::orchard::tree::NoteCommitmentTree::append_batch`]. Callers convert their
/// commitment type to `H` before calling and wrap the returned index value in
/// `NoteCommitmentSubtreeIndex`.
///
/// # Batch Size
///
/// `nodes` must contain the commitments from a single block. The consensus block-size
/// cap bounds a block to far fewer than `2^TRACKED_SUBTREE_HEIGHT` (65,536) outputs or
/// actions, so a batch can cross **at most one** subtree boundary.
///
/// Returns [`BatchFrontierError`] if appending would overflow the tree's capacity,
/// or if the batch spans more than one tracked subtree boundary.
pub fn append_batch_with_subtree<H, const DEPTH: u8>(
    frontier: Frontier<H, DEPTH>,
    nodes: Vec<H>,
) -> Result<(Frontier<H, DEPTH>, Option<(u64, H)>), BatchFrontierError>
where
    H: Hashable + Clone + Send + Sync,
{
    use crate::subtree::TRACKED_SUBTREE_HEIGHT;

    if nodes.is_empty() {
        return Ok((frontier, None));
    }

    // nodes.len() fits in u64: consensus rules cap a block at 2^16 actions
    let old_size = frontier.tree_size();
    let new_size = old_size + nodes.len() as u64;

    // A consensus block crosses at most one tracked-subtree boundary. If a
    // caller passes a larger batch, return an error instead of dropping later
    // completed subtrees.
    let subtree_size = 1u64 << TRACKED_SUBTREE_HEIGHT;
    // Round old_size up to the next subtree boundary.
    let boundary = (old_size / subtree_size)
        .checked_add(1)
        .and_then(|n| n.checked_mul(subtree_size));
    if boundary
        .and_then(|b| b.checked_add(subtree_size))
        .is_some_and(|second_boundary| second_boundary <= new_size)
    {
        return Err(BatchFrontierError::BatchSpansMultipleSubtrees);
    }

    if boundary.is_some_and(|b| b <= new_size) {
        let boundary = boundary.expect("checked above");
        let head_len = (boundary - old_size) as usize;
        let mut head = nodes;
        let tail = head.split_off(head_len);

        let f1 = parallel_append(frontier, head)?;

        // index = (boundary / subtree_size) - 1; fits in u16 by tree depth.
        let index_value = (boundary >> TRACKED_SUBTREE_HEIGHT) - 1;
        let root = f1
            .value()
            .expect("just appended at least one leaf")
            .root(Some(Level::from(TRACKED_SUBTREE_HEIGHT)));

        let f2 = parallel_append(f1, tail)?;
        Ok((f2, Some((index_value, root))))
    } else {
        let f = parallel_append(frontier, nodes)?;
        Ok((f, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const DEPTH: u8 = 32;

    /// A self-contained test node whose `combine` is **order-sensitive** (so a
    /// left/right swap changes the result) and **level-sensitive** (so a wrong
    /// `combine` level argument changes the result). This lets the differential
    /// tests catch ordering and level bugs in the parallel append.
    ///
    /// Uses a hand-rolled FNV-style mix rather than `DefaultHasher` so the output
    /// is stable across Rust releases and proptest regression seeds stay valid.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    struct TestNode(u64);

    /// Stable, order- and level-sensitive mix of three u64 values.
    /// Based on FNV-1a with domain separation by argument position.
    fn mix3(level: u64, a: u64, b: u64) -> u64 {
        const FNV_PRIME: u64 = 0x00000100000001B3;
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        let mut h = FNV_OFFSET;
        h ^= level;
        h = h.wrapping_mul(FNV_PRIME);
        h ^= a;
        h = h.wrapping_mul(FNV_PRIME);
        h ^= b;
        h = h.wrapping_mul(FNV_PRIME);
        h
    }

    impl Hashable for TestNode {
        fn empty_leaf() -> Self {
            Self(0)
        }

        fn combine(level: Level, a: &Self, b: &Self) -> Self {
            Self(mix3(u8::from(level) as u64, a.0, b.0))
        }
    }

    /// Append `leaves` to `start` one at a time using the sequential crate API.
    fn sequential_append<const DEPTH: u8>(
        start: Frontier<TestNode, DEPTH>,
        leaves: &[TestNode],
    ) -> Frontier<TestNode, DEPTH> {
        let mut f = start;
        for leaf in leaves {
            assert!(f.append(*leaf), "test trees never overflow");
        }
        f
    }

    fn build_frontier<const DEPTH: u8>(prefix: &[TestNode]) -> Frontier<TestNode, DEPTH> {
        let mut f = Frontier::<TestNode, DEPTH>::empty();
        for leaf in prefix {
            assert!(f.append(*leaf));
        }
        f
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        /// The parallel batch append must produce a byte-identical frontier (and
        /// therefore an identical root) to the sequential append, for any
        /// starting tree size and any batch size.
        #[test]
        fn parallel_matches_sequential(
            prefix_len in 0usize..300,
            batch in proptest::collection::vec(any::<u64>().prop_map(TestNode), 0..300),
        ) {
            let prefix: Vec<TestNode> = (0..prefix_len as u64).map(TestNode).collect();
            let start = build_frontier::<DEPTH>(&prefix);

            let seq = sequential_append::<DEPTH>(start.clone(), &batch);
            let par = parallel_append(start, batch.clone()).expect("no overflow in tests");

            prop_assert_eq!(seq.root(), par.root(), "root mismatch");
            prop_assert_eq!(
                seq.value().map(|f| f.clone().into_parts()),
                par.value().map(|f| f.clone().into_parts()),
                "frontier parts mismatch"
            );
        }
    }

    /// Spot-check small exhaustive sizes for off-by-one boundary bugs.
    #[test]
    fn exhaustive_small() {
        for prefix_len in 0u64..40 {
            let prefix: Vec<TestNode> = (0..prefix_len).map(TestNode).collect();
            let start = build_frontier::<DEPTH>(&prefix);
            for batch_len in 0u64..40 {
                let batch: Vec<TestNode> = (1000..1000 + batch_len).map(TestNode).collect();
                let seq = sequential_append::<DEPTH>(start.clone(), &batch);
                let par = parallel_append(start.clone(), batch).expect("no overflow");
                assert_eq!(
                    seq.root(),
                    par.root(),
                    "root mismatch p={prefix_len} b={batch_len}"
                );
                assert_eq!(
                    seq.value().map(|f| f.clone().into_parts()),
                    par.value().map(|f| f.clone().into_parts()),
                    "parts mismatch p={prefix_len} b={batch_len}"
                );
            }
        }
    }

    /// A full batch append should either succeed completely or report overflow.
    #[test]
    fn overflow_is_reported() {
        const SMALL_DEPTH: u8 = 3;

        let prefix: Vec<TestNode> = (0..7).map(TestNode).collect();
        let start = build_frontier::<SMALL_DEPTH>(&prefix);
        let exact_capacity_batch = [TestNode(100)];

        let seq = sequential_append::<SMALL_DEPTH>(start.clone(), &exact_capacity_batch);
        let par = parallel_append(start.clone(), exact_capacity_batch.to_vec())
            .expect("one remaining leaf fits");

        assert_eq!(seq.root(), par.root(), "root mismatch at exact capacity");
        assert_eq!(
            seq.value().map(|f| f.clone().into_parts()),
            par.value().map(|f| f.clone().into_parts()),
            "parts mismatch at exact capacity"
        );

        let empty_append = parallel_append(par.clone(), Vec::new()).expect("empty append succeeds");
        assert_eq!(
            par.value().map(|f| f.clone().into_parts()),
            empty_append.value().map(|f| f.clone().into_parts()),
            "empty append changed a full frontier"
        );

        let full_tree_overflow = parallel_append(par, vec![TestNode(101)]);
        assert!(
            full_tree_overflow.is_err(),
            "appending to a full tree overflows"
        );

        let partial_batch_overflow = parallel_append(start, vec![TestNode(100), TestNode(101)]);
        assert!(
            partial_batch_overflow.is_err(),
            "batch crossing tree capacity overflows"
        );
    }

    /// Batches that would complete more than one tracked subtree are rejected,
    /// because the return type can only report one completed subtree.
    #[test]
    fn append_batch_errors_on_multiple_subtree_boundaries() {
        use crate::subtree::TRACKED_SUBTREE_HEIGHT;

        let start = Frontier::<TestNode, DEPTH>::empty();
        let subtree_size = 1usize << TRACKED_SUBTREE_HEIGHT;
        let batch = vec![TestNode(0); subtree_size * 2];

        let result = append_batch_with_subtree(start, batch);

        assert_eq!(result, Err(BatchFrontierError::BatchSpansMultipleSubtrees));
    }

    /// Deterministic positions around powers of two exercise carry propagation and
    /// globally aligned dyadic block decomposition beyond the small exhaustive range.
    #[test]
    fn matches_sequential_at_alignment_boundaries() {
        let interesting_prefix_lengths = [
            0usize, 1, 2, 3, 7, 8, 9, 15, 16, 17, 255, 256, 257, 65_535, 65_536, 65_537,
        ];
        let interesting_batch_lengths = [0usize, 1, 2, 3, 4, 5, 31, 32, 33];
        let max_prefix_len = *interesting_prefix_lengths
            .last()
            .expect("interesting prefixes are non-empty");

        let mut frontier = Frontier::<TestNode, DEPTH>::empty();
        let mut snapshots = Vec::new();

        for prefix_len in 0..=max_prefix_len {
            if interesting_prefix_lengths.contains(&prefix_len) {
                snapshots.push((prefix_len, frontier.clone()));
            }

            if prefix_len < max_prefix_len {
                assert!(frontier.append(TestNode(
                    u64::try_from(prefix_len).expect("test prefix length fits in u64")
                )));
            }
        }

        for (prefix_len, start) in snapshots {
            for batch_len in interesting_batch_lengths {
                let prefix_len = u64::try_from(prefix_len).expect("test prefix length fits in u64");
                let batch: Vec<TestNode> = (0..batch_len)
                    .map(|leaf| {
                        TestNode(
                            1_000_000
                                + prefix_len
                                + u64::try_from(leaf).expect("test batch length fits in u64"),
                        )
                    })
                    .collect();

                let seq = sequential_append::<DEPTH>(start.clone(), &batch);
                let par = parallel_append(start.clone(), batch).expect("no overflow");

                assert_eq!(
                    seq.root(),
                    par.root(),
                    "root mismatch p={prefix_len} b={batch_len}"
                );
                assert_eq!(
                    seq.value().map(|f| f.clone().into_parts()),
                    par.value().map(|f| f.clone().into_parts()),
                    "parts mismatch p={prefix_len} b={batch_len}"
                );
            }
        }
    }
}
