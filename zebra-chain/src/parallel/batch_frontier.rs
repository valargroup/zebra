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

/// A pure binary-counter forest of a contiguous run of leaves, indexed by level:
/// `forest[L] == Some(root)` iff bit `L` of the run length is set, in which case
/// `root` is the root of the complete `2^L`-leaf subtree covering that aligned
/// block. Higher set bits (older subtrees) are further left in leaf order.
///
/// This is *pure*: unlike the crate's `CommitmentTree`, it has no lazy level-0
/// staging, which makes the algebra below unambiguous.
type Forest<H> = Vec<Option<H>>;

/// Injects a complete subtree `node` at level `level` into the binary-counter
/// `forest`, propagating carries upward.
///
/// `node` (and anything it carries into) must be **strictly newer** (further
/// right in leaf order) than everything already in `forest`, so the existing slot
/// value is always the left (older) argument of [`Hashable::combine`]. This holds
/// because we only ever inject the old tip leaf and then the new leaves, in
/// ascending position order.
fn inject<H: Hashable + Clone>(forest: &mut Forest<H>, level: usize, node: H) {
    let mut idx = level;
    let mut carry = node;
    loop {
        if idx >= forest.len() {
            forest.resize(idx + 1, None);
        }
        match forest[idx].take() {
            None => {
                forest[idx] = Some(carry);
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
/// 2. inject the old tip leaf, giving the pure forest of all `S` leaves;
/// 3. append every new leaf except the last as **globally position-aligned
///    dyadic blocks** — each block's root computed in parallel — injecting them
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

    // Rebuild the pure forest of the existing tree, and the next free position.
    let (mut forest, mut size): (Forest<H>, u64) = match frontier.value() {
        None => (Vec::new(), 0),
        Some(f) => {
            let (position, leaf, ommers) = (f.position(), f.leaf().clone(), f.ommers().to_vec());
            let pos = u64::from(position); // = S - 1
                                           // ommers (low→high) sit at the set bits of `pos`.
            let mut forest: Forest<H> = Vec::new();
            let mut ommers = ommers.into_iter();
            for level in 0..u64::BITS {
                if pos & (1 << level) != 0 {
                    if level as usize >= forest.len() {
                        forest.resize(level as usize + 1, None);
                    }
                    forest[level as usize] = Some(ommers.next().expect("ommer per set bit"));
                }
            }
            // Inject the old tip leaf (position `pos`) to get the pure forest of S leaves.
            inject(&mut forest, 0, leaf);
            (forest, pos + 1)
        }
    };

    // The new last leaf stays raw; everything before it joins the forest as
    // globally-aligned dyadic blocks.
    let last = new_leaves.len() - 1;
    let body = &new_leaves[..last];

    // Decompose [size, size + body.len()) into maximal position-aligned dyadic
    // blocks, then compute each block's root in parallel.
    let mut blocks: Vec<(usize, &[H])> = Vec::new();
    {
        let mut pos = size;
        let end = size + body.len() as u64;
        let mut offset = 0usize;
        while pos < end {
            // Largest level L with pos % 2^L == 0 and pos + 2^L <= end.
            let align = if pos == 0 {
                u64::BITS
            } else {
                pos.trailing_zeros()
            };
            let remaining = end - pos;
            let span = u64::BITS - 1 - remaining.leading_zeros(); // floor(log2(remaining))
            let level = align.min(span) as usize;
            let width = 1usize << level;
            blocks.push((level, &body[offset..offset + width]));
            offset += width;
            pos += width as u64;
        }
    }

    // Compute all block roots concurrently (and each reduction is itself
    // internally parallel). `par_iter().collect()` preserves order, so injection
    // below stays in ascending position order, keeping the carry order exact.
    let roots: Vec<(usize, H)> = blocks
        .into_par_iter()
        .map(|(level, leaves)| (level, perfect_subtree_root(leaves)))
        .collect();
    for (level, root) in roots {
        inject(&mut forest, level, root);
    }
    size += body.len() as u64;

    // The final frontier: raw last leaf at `position = size`, ommers = forest
    // (compacted low→high, matching the set bits of `position`).
    let position = Position::from(size);
    let leaf = new_leaves[last].clone();
    let ommers: Vec<H> = forest.into_iter().flatten().collect();

    Frontier::from_parts(position, leaf, ommers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::hash::{Hash, Hasher};

    const DEPTH: u8 = 32;

    /// A self-contained test node whose `combine` is **order-sensitive** (so a
    /// left/right swap changes the result) and **level-sensitive** (so a wrong
    /// `combine` level argument changes the result). This lets the differential
    /// tests catch ordering and level bugs in the parallel append.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    struct TestNode(u64);

    impl Hashable for TestNode {
        fn empty_leaf() -> Self {
            Self(0)
        }

        fn combine(level: Level, a: &Self, b: &Self) -> Self {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            u8::from(level).hash(&mut hasher);
            a.0.hash(&mut hasher);
            b.0.hash(&mut hasher);
            // Keep it non-zero-ish and order/level sensitive.
            Self(hasher.finish())
        }
    }

    /// Append `leaves` to `start` one at a time using the sequential crate API.
    fn sequential_append(
        start: Frontier<TestNode, DEPTH>,
        leaves: &[TestNode],
    ) -> Frontier<TestNode, DEPTH> {
        let mut f = start;
        for leaf in leaves {
            assert!(f.append(*leaf), "test trees never overflow");
        }
        f
    }

    fn build_frontier(prefix: &[TestNode]) -> Frontier<TestNode, DEPTH> {
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
            let start = build_frontier(&prefix);

            let seq = sequential_append(start.clone(), &batch);
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
            let start = build_frontier(&prefix);
            for batch_len in 0u64..40 {
                let batch: Vec<TestNode> = (1000..1000 + batch_len).map(TestNode).collect();
                let seq = sequential_append(start.clone(), &batch);
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
