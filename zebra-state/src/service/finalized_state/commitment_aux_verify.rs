//! Read-only verification of supplied per-block note-commitment roots against the
//! checkpoint-committed block headers, via the ZIP-221 ChainHistory MMR.
//!
//! This is the "verify" half of the verified-commitment-trees design
//! (`docs/design/verified-commitment-trees.md` §6): given a sequence of per-block
//! Sapling/Orchard roots (from a fixture today, an untrusted peer later), confirm
//! they reconstruct a history tree consistent with the header commitments. It does
//! not commit anything and does not change the commit path; it is the logic a later
//! verify-before-commit step will wrap.
//!
//! It reuses the existing consensus check
//! ([`block_commitment_is_valid_for_chain_history`](crate::service::check::block_commitment_is_valid_for_chain_history))
//! and [`HistoryTree::push`], which build the V1/V2 leaf from the block body and the
//! supplied roots — so there is no new crypto here.

use std::sync::Arc;

use zebra_chain::{
    block::{Block, Height},
    history_tree::HistoryTree,
    orchard,
    parameters::Network,
    sapling,
};

use zebra_chain::block::{Commitment, CommitmentError};

use crate::{service::check, ValidateContextError};

/// Verifies a supplied Sapling root for a *pre-Heartwood* block directly against the
/// block header (design §6.1).
///
/// The ZIP-221 history MMR does not exist below Heartwood, so
/// [`block_commitment_is_valid_for_chain_history`](check::block_commitment_is_valid_for_chain_history)
/// is a no-op there and cannot authenticate the supplied roots. This fills that gap:
///
/// - Sapling..Heartwood: the header's `FinalSaplingRoot` commits the Sapling root
///   directly, so the supplied root must equal it.
/// - Pre-Sapling: the Sapling tree is empty, so the supplied root must be the
///   empty-tree root.
///
/// Heartwood and later (`ChainHistoryRoot` / `ChainHistoryBlockTxAuthCommitment` /
/// the activation-reserved block) are authenticated by the MMR path and accepted
/// here. Orchard does not activate until NU5 and is not committed by any header
/// below NU5, so it is not checked here.
pub(crate) fn verify_supplied_sapling_root_below_heartwood(
    network: &Network,
    block: &Block,
    sapling_root: &sapling::tree::Root,
) -> Result<(), ValidateContextError> {
    let expected = match block.commitment(network)? {
        Commitment::FinalSaplingRoot(header_root) => header_root,
        Commitment::PreSaplingReserved(_) => sapling::tree::NoteCommitmentTree::default().root(),
        // Heartwood activation and later are authenticated by the MMR path.
        _ => return Ok(()),
    };

    if sapling_root != &expected {
        return Err(ValidateContextError::InvalidBlockCommitment(
            CommitmentError::InvalidFinalSaplingRoot {
                expected: <[u8; 32]>::from(expected),
                actual: <[u8; 32]>::from(*sapling_root),
            },
        ));
    }

    Ok(())
}

/// Verifies that `items` (blocks in ascending height order, each with its supplied
/// Sapling/Orchard roots) reconstruct a ZIP-221 history MMR consistent with the
/// block header commitments, starting from `tree` (the parent block's history tree).
///
/// Returns the final history tree on success, or `(height, error)` for the first
/// block whose header commitment rejects the roots folded in so far.
///
/// # Lag
///
/// A block's commitment commits to the history tree as of its *parent*, so the root
/// supplied for height `H` is only confirmed when height `H + 1` is processed. Over a
/// contiguous range `[start..=end]` this therefore confirms the roots at
/// `[start..=end - 1]`; pass the block at `end + 1` to confirm the root at `end`.
#[allow(dead_code)] // POC scaffold: exercised by tests; wired into the commit path in a later increment.
pub(crate) fn verify_commitment_roots<I>(
    network: &Network,
    mut tree: HistoryTree,
    items: I,
) -> Result<HistoryTree, (Height, ValidateContextError)>
where
    I: IntoIterator<Item = (Arc<Block>, sapling::tree::Root, orchard::tree::Root)>,
{
    for (block, sapling_root, orchard_root) in items {
        let height = block
            .coinbase_height()
            .expect("checkpoint-verified blocks have a coinbase height");

        // Validate this block's header commitment against the current (parent) tree,
        // i.e. against every root already folded in. `None` lets the check compute
        // `block.auth_data_root()` itself; it is only used on the NU5+ path.
        check::block_commitment_is_valid_for_chain_history(block.clone(), network, &tree, None)
            .map_err(|error| (height, error))?;

        // Fold this block's supplied roots into the running MMR (builds the leaf from
        // the block body tx-counts + the roots).
        tree.push(network, block, &sapling_root, &orchard_root)
            .map_err(Arc::new)
            .map_err(ValidateContextError::from)
            .map_err(|error| (height, error))?;
    }

    Ok(tree)
}

#[cfg(test)]
mod tests {
    use super::*;

    use zebra_chain::{
        block::Block,
        parameters::{Network::Mainnet, NetworkUpgrade},
        serialization::ZcashDeserializeInto,
    };

    /// Build an empty [`HistoryTree`] (the genesis block is pre-Heartwood).
    fn empty_history_tree() -> HistoryTree {
        let genesis = Arc::new(
            zebra_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
                .zcash_deserialize_into::<Block>()
                .expect("genesis deserializes"),
        );
        HistoryTree::from_block(&Mainnet, genesis, &Default::default(), &Default::default())
            .expect("empty history tree for a pre-Heartwood block")
    }

    /// The verifier confirms real Sapling roots over the Heartwood activation and its
    /// next block (the V1 `ChainHistoryRoot` path), and rejects a wrong root at the
    /// *next* block (the one-block lag).
    #[test]
    fn verifies_real_roots_and_rejects_a_wrong_root_at_next_height() {
        let (blocks, sapling_roots) = Mainnet.block_sapling_roots_map();
        let activation = NetworkUpgrade::Heartwood
            .activation_height(&Mainnet)
            .expect("mainnet has Heartwood")
            .0;

        let block_at = |height: u32| -> Arc<Block> {
            Arc::new(
                blocks
                    .get(&height)
                    .expect("test vector block exists")
                    .zcash_deserialize_into::<Block>()
                    .expect("block deserializes"),
            )
        };
        let root_at = |height: u32| -> sapling::tree::Root {
            sapling::tree::Root::try_from(**sapling_roots.get(&height).expect("root vector exists"))
                .expect("valid root")
        };

        let act_block = block_at(activation);
        let next_block = block_at(activation + 1);
        let act_root = root_at(activation);
        let next_root = root_at(activation + 1);

        // Positive: the real roots reconstruct a tree the next block's header commits to.
        let ok_items = vec![
            (act_block.clone(), act_root, orchard::tree::Root::default()),
            (
                next_block.clone(),
                next_root,
                orchard::tree::Root::default(),
            ),
        ];
        verify_commitment_roots(&Mainnet, empty_history_tree(), ok_items)
            .expect("real roots verify against the headers");

        // Negative + lag: a wrong root at the activation height (here, the next
        // block's root, which is a valid but different root) is only caught when the
        // following block's commitment is checked.
        assert_ne!(act_root, next_root, "test needs two distinct roots");
        let bad_items = vec![
            (act_block, next_root, orchard::tree::Root::default()),
            (next_block, next_root, orchard::tree::Root::default()),
        ];
        let (fail_height, _error) =
            verify_commitment_roots(&Mainnet, empty_history_tree(), bad_items)
                .expect_err("a wrong root must be rejected");
        assert_eq!(
            fail_height.0,
            activation + 1,
            "a wrong root at H is detected at H+1 (the lag)"
        );
    }

    /// Real NU5/V2-range verification over the POC range (1,707,211..=1,717,210),
    /// exercising the actual [`verify_commitment_roots`] on production data.
    ///
    /// Gated by env vars so it stays out of normal CI. Requires two read-only forks
    /// of the RUNBOOK 1.707M master snapshot:
    /// - `VCT_SEED_DB`: an *unsynced* `cp -al` fork (its tip history tree at height
    ///   1,707,210 is the seed — mid-NU5-epoch, so no activation boundary to handle).
    /// - `VCT_ARCHIVE_DB`: an archive fork synced to >= 1,717,211 (provides the blocks
    ///   and per-height roots).
    ///
    /// Run:
    /// ```text
    /// VCT_SEED_DB=<unsynced-fork> VCT_ARCHIVE_DB=<synced-fork> \
    ///   cargo test -p zebra-state --lib commitment_aux_verify -- --ignored --nocapture
    /// ```
    #[ignore]
    #[test]
    #[allow(clippy::print_stderr)] // intentional progress output for a manual run
    fn verifies_real_nu5_range_over_synced_forks() {
        use std::path::PathBuf;

        use crate::{
            constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
            service::finalized_state::{ZebraDb, STATE_COLUMN_FAMILIES_IN_CODE},
            Config,
        };

        let (Some(seed_dir), Some(archive_dir)) = (
            std::env::var_os("VCT_SEED_DB"),
            std::env::var_os("VCT_ARCHIVE_DB"),
        ) else {
            eprintln!("skipping: set VCT_SEED_DB (unsynced fork) and VCT_ARCHIVE_DB (synced fork)");
            return;
        };

        let open = |dir: PathBuf| -> ZebraDb {
            let config = Config {
                cache_dir: dir,
                ephemeral: false,
                ..Default::default()
            };
            ZebraDb::new(
                &config,
                STATE_DATABASE_KIND,
                &state_database_format_version_in_code(),
                &Mainnet,
                true, // skip format upgrades
                STATE_COLUMN_FAMILIES_IN_CODE
                    .iter()
                    .map(ToString::to_string),
                true, // read-only
            )
        };

        let seed_db = open(PathBuf::from(seed_dir));
        let archive_db = open(PathBuf::from(archive_dir));

        let start = 1_707_211u32;
        let end = 1_717_210u32;

        // Seed: the history tree at 1,707,210 (the unsynced fork's tip).
        let seed = (*seed_db.history_tree()).clone();
        assert_eq!(
            seed_db.finalized_tip_height().map(|h| h.0),
            Some(start - 1),
            "VCT_SEED_DB must be the unsynced 1,707,210 master fork"
        );
        assert!(
            archive_db.finalized_tip_height().map(|h| h.0).unwrap_or(0) > end,
            "VCT_ARCHIVE_DB must be synced to at least {}",
            end + 1
        );

        // Build (block, sapling_root, orchard_root) for [start..=end+1]; the +1 block
        // confirms the in-range root at `end` via the one-block lag.
        let item_at = |h: u32| -> (Arc<Block>, sapling::tree::Root, orchard::tree::Root) {
            let block = archive_db
                .block(Height(h).into())
                .expect("archive fork has the block");
            let sapling_root = archive_db
                .sapling_tree_by_height(&Height(h))
                .expect("archive fork has the per-height Sapling tree")
                .root();
            let orchard_root = archive_db
                .orchard_tree_by_height(&Height(h))
                .expect("archive fork has the per-height Orchard tree")
                .root();
            (block, sapling_root, orchard_root)
        };
        let items: Vec<_> = (start..=end + 1).map(item_at).collect();

        // Positive: every supplied root in the range is confirmed by the V2 headers.
        verify_commitment_roots(&Mainnet, seed.clone(), items.clone())
            .expect("real NU5 roots verify against the headers");
        eprintln!("VCT NU5 positive: {} blocks verified", items.len());

        // Negative + lag: corrupt one root mid-range with a distinct valid root (the
        // range's first root, certainly different after thousands of sandblast blocks);
        // expect rejection at H+1.
        let bad_offset = 5_000usize;
        let bad_height = start + bad_offset as u32;
        let wrong_root = items[0].1;
        let mut bad_items = items;
        assert_ne!(
            bad_items[bad_offset].1, wrong_root,
            "need a distinct wrong root"
        );
        bad_items[bad_offset].1 = wrong_root;
        let (fail_height, _error) = verify_commitment_roots(&Mainnet, seed, bad_items)
            .expect_err("a wrong NU5 root must be rejected");
        assert_eq!(
            fail_height.0,
            bad_height + 1,
            "a wrong root at H is detected at H+1 (the lag)"
        );
        eprintln!(
            "VCT NU5 negative: wrong root at {bad_height} rejected at {}",
            fail_height.0
        );
    }
}
