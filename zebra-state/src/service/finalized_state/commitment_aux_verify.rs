//! Read-only verification of supplied per-block note-commitment roots against the
//! checkpoint-committed block headers, via the ZIP-221 ChainHistory MMR.
//!
//! This is the "verify" half of the verified-commitment-trees design
//! (`docs/design/verified-commitment-trees.md` §6): given a sequence of per-block
//! Sapling/Orchard roots (from a fixture today, an untrusted peer later), confirm
//! they reconstruct a history tree consistent with the header commitments. The
//! commit path uses this module before persisting supplied roots.
//!
//! It reuses the existing consensus check
//! ([`block_commitment_is_valid_for_chain_history`](crate::service::check::block_commitment_is_valid_for_chain_history))
//! and [`HistoryTree::push`], which build the V1/V2 leaf from the block body and the
//! supplied roots — so there is no new crypto here.

use std::sync::Arc;

use zebra_chain::{
    block::{merkle::AuthDataRoot, Block, Header, Height},
    history_tree::HistoryTree,
    orchard,
    parameters::{Network, NetworkUpgrade},
    sapling,
};

use zebra_chain::block::{Commitment, CommitmentError};

use crate::{service::check, ValidateContextError};

/// One block-sized step in supplied commitment-root verification.
#[derive(Clone, Debug)]
pub(crate) struct CommitmentRootVerification {
    pub(crate) header: Arc<Header>,
    pub(crate) height: Height,
    /// The full block, needed only to fold this block's supplied roots into the MMR
    /// (`tree.push` builds the leaf from the body). `None` for a header-only successor
    /// confirmation item, whose body may not be downloaded yet.
    pub(crate) block: Option<Arc<Block>>,
    pub(crate) roots: Option<(sapling::tree::Root, orchard::tree::Root)>,
    /// This block's own ZIP-244 auth-data root — the co-input to its NU5+ header
    /// commitment. Resolved at construction because the body may be absent later.
    pub(crate) auth_data_root: AuthDataRoot,
    pub(crate) skip_parent_check: bool,
}

impl CommitmentRootVerification {
    pub(crate) fn with_roots(
        block: Arc<Block>,
        sapling_root: sapling::tree::Root,
        orchard_root: orchard::tree::Root,
        precomputed_auth_data_root: Option<AuthDataRoot>,
        skip_parent_check: bool,
    ) -> Self {
        let height = block
            .coinbase_height()
            .expect("checkpoint-verified blocks have a coinbase height");
        let auth_data_root = precomputed_auth_data_root.unwrap_or_else(|| block.auth_data_root());
        CommitmentRootVerification {
            header: block.header.clone(),
            height,
            block: Some(block),
            roots: Some((sapling_root, orchard_root)),
            auth_data_root,
            skip_parent_check,
        }
    }

    /// A confirmation-only item built from a block's already-committed *successor*: its
    /// header authenticates the predecessor's folded roots, and `auth_data_root` is the
    /// successor's own ZIP-244 auth-data root (carried over header sync), so no successor
    /// body is needed.
    pub(crate) fn header_only(
        header: Arc<Header>,
        height: Height,
        auth_data_root: AuthDataRoot,
    ) -> Self {
        CommitmentRootVerification {
            header,
            height,
            block: None,
            roots: None,
            auth_data_root,
            skip_parent_check: false,
        }
    }
}

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
/// here. The Orchard root below NU5 is pinned separately by
/// [`verify_supplied_orchard_root_below_nu5`].
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

/// Verifies a supplied Orchard root for a *pre-NU5* block (design §6.1).
///
/// The Orchard tree does not activate until NU5, and no header below NU5 commits to an
/// Orchard root: the ZIP-221 V1 history leaf (Heartwood..Canopy) *ignores* the Orchard
/// root entirely (`zcash_history.rs`, `V1::block_to_history_node`), and below Heartwood
/// there is no MMR at all. So the MMR path that authenticates Orchard roots from NU5
/// onward cannot vouch for any root below NU5 — yet the fast path folds the supplied
/// Orchard root into the anchor set for every block. Without this check an untrusted
/// source could inject an arbitrary Orchard anchor below NU5 that the legacy recompute
/// path never produces, breaking the §11 trust boundary and consensus equivalence.
///
/// Below NU5 the Orchard tree is always the empty default, so the supplied root must
/// equal the empty-tree root. At and above NU5 activation the MMR path authenticates
/// the root, so this accepts.
pub(crate) fn verify_supplied_orchard_root_below_nu5(
    network: &Network,
    height: Height,
    orchard_root: &orchard::tree::Root,
) -> Result<(), ValidateContextError> {
    // At/above NU5 the ZIP-221 V2 MMR commits to the Orchard root, so it is
    // authenticated there, not here.
    if let Some(nu5_height) = NetworkUpgrade::Nu5.activation_height(network) {
        if height >= nu5_height {
            return Ok(());
        }
    }

    let expected = orchard::tree::NoteCommitmentTree::default().root();
    if orchard_root != &expected {
        return Err(ValidateContextError::InvalidBlockCommitment(
            CommitmentError::InvalidPreNu5OrchardRoot {
                expected: <[u8; 32]>::from(expected),
                actual: <[u8; 32]>::from(*orchard_root),
            },
        ));
    }

    Ok(())
}

/// Verifies that `items` (blocks in ascending height order, with supplied
/// Sapling/Orchard roots when they should be folded in) reconstruct a ZIP-221
/// history MMR consistent with the block header commitments, starting from `tree`
/// (the parent block's history tree).
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
pub(crate) fn verify_commitment_roots<I>(
    network: &Network,
    mut tree: HistoryTree,
    items: I,
) -> Result<HistoryTree, (Height, ValidateContextError)>
where
    I: IntoIterator<Item = CommitmentRootVerification>,
{
    for item in items {
        let CommitmentRootVerification {
            header,
            height,
            block,
            roots,
            auth_data_root,
            skip_parent_check,
        } = item;

        // Validate this block's header commitment against the current (parent) tree,
        // i.e. against every root already folded in. Driven by the header (+ height +
        // auth-data root) so a header-only successor item can confirm the predecessor
        // without its own body.
        if !skip_parent_check {
            check::header_commitment_is_valid_for_chain_history(
                &header,
                height,
                network,
                &tree,
                auth_data_root,
            )
            .map_err(|error| (height, error))?;
        }

        let Some((sapling_root, orchard_root)) = roots else {
            continue;
        };

        // An item that folds roots into the MMR always carries its block body.
        let block = block.expect("a root-folding verification item carries its block body");

        verify_supplied_sapling_root_below_heartwood(network, &block, &sapling_root)
            .map_err(|error| (height, error))?;
        verify_supplied_orchard_root_below_nu5(network, height, &orchard_root)
            .map_err(|error| (height, error))?;

        // Fold this block's supplied roots into the running MMR (builds the leaf
        // from the block body tx-counts + the roots).
        tree.push(
            network,
            block,
            &sapling_root,
            &orchard_root,
            &Default::default(),
        )
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
        parameters::{
            testnet::{ConfiguredActivationHeights, RegtestParameters},
            Network::Mainnet,
            NetworkUpgrade,
        },
        serialization::ZcashDeserializeInto,
    };

    /// Build an empty [`HistoryTree`] (the genesis block is pre-Heartwood).
    fn empty_history_tree() -> HistoryTree {
        let genesis = Arc::new(
            zebra_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
                .zcash_deserialize_into::<Block>()
                .expect("genesis deserializes"),
        );
        HistoryTree::from_block(
            &Mainnet,
            genesis,
            &Default::default(),
            &Default::default(),
            &Default::default(),
        )
        .expect("empty history tree for a pre-Heartwood block")
    }

    /// A distinct, valid Orchard root that is *not* the empty-tree root, for the
    /// negative cases. Zero is a valid Pallas base field element, and the empty
    /// Orchard tree root is an uncommitted-leaf hash, so the two differ.
    fn non_empty_orchard_root() -> orchard::tree::Root {
        let empty = orchard::tree::NoteCommitmentTree::default().root();
        let wrong = orchard::tree::Root::try_from([0u8; 32])
            .expect("zero is a valid pallas base field element");
        assert_ne!(
            wrong, empty,
            "the negative cases need a root distinct from the empty-tree root"
        );
        wrong
    }

    fn verification_item(
        block: Arc<Block>,
        sapling_root: sapling::tree::Root,
        orchard_root: orchard::tree::Root,
    ) -> CommitmentRootVerification {
        CommitmentRootVerification::with_roots(block, sapling_root, orchard_root, None, false)
    }

    /// Below NU5 the supplied Orchard root must equal the empty-tree root (no header
    /// commits to it there), and any other root is rejected. At/above NU5 the MMR
    /// authenticates it, so this check accepts unconditionally.
    #[test]
    fn pins_orchard_root_to_empty_below_nu5_and_defers_above() {
        let nu5 = NetworkUpgrade::Nu5
            .activation_height(&Mainnet)
            .expect("mainnet has NU5");
        let empty = orchard::tree::NoteCommitmentTree::default().root();
        let wrong = non_empty_orchard_root();

        // Below NU5: the empty root is accepted, a non-empty root is rejected.
        let pre_nu5 = Height(nu5.0 - 1);
        verify_supplied_orchard_root_below_nu5(&Mainnet, pre_nu5, &empty)
            .expect("the empty-tree root is accepted below NU5");
        let error = verify_supplied_orchard_root_below_nu5(&Mainnet, pre_nu5, &wrong)
            .expect_err("a non-empty orchard root must be rejected below NU5");
        assert!(
            matches!(
                error,
                ValidateContextError::InvalidBlockCommitment(
                    CommitmentError::InvalidPreNu5OrchardRoot { .. }
                )
            ),
            "rejection uses the dedicated pre-NU5 orchard error, got: {error:?}"
        );

        // Pre-Sapling/Heartwood (well below NU5) is also pinned to empty.
        verify_supplied_orchard_root_below_nu5(&Mainnet, Height(1), &empty)
            .expect("the empty-tree root is accepted at low heights");
        verify_supplied_orchard_root_below_nu5(&Mainnet, Height(1), &wrong)
            .expect_err("a non-empty orchard root must be rejected at low heights");

        // At and above NU5 the MMR path authenticates the root, so even a non-empty
        // root is accepted here (it is checked elsewhere).
        verify_supplied_orchard_root_below_nu5(&Mainnet, nu5, &wrong)
            .expect("at NU5 the root is authenticated by the MMR, not pinned here");
        verify_supplied_orchard_root_below_nu5(&Mainnet, Height(nu5.0 + 1), &wrong)
            .expect("above NU5 the root is authenticated by the MMR, not pinned here");
    }

    #[test]
    fn pins_orchard_root_to_empty_when_nu5_is_unconfigured() {
        let network = zebra_chain::parameters::Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu5: None,
                ..Default::default()
            },
            ..Default::default()
        });
        let empty = orchard::tree::NoteCommitmentTree::default().root();
        let wrong = non_empty_orchard_root();

        verify_supplied_orchard_root_below_nu5(&network, Height(1), &empty)
            .expect("the empty-tree root is accepted when NU5 is unconfigured");
        let error = verify_supplied_orchard_root_below_nu5(&network, Height(1), &wrong)
            .expect_err("a non-empty orchard root must be rejected when NU5 is unconfigured");
        assert!(
            matches!(
                error,
                ValidateContextError::InvalidBlockCommitment(
                    CommitmentError::InvalidPreNu5OrchardRoot { .. }
                )
            ),
            "rejection uses the dedicated pre-NU5 orchard error, got: {error:?}"
        );
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
        let empty_orchard_root = orchard::tree::NoteCommitmentTree::default().root();

        // Positive: the real roots reconstruct a tree the next block's header commits to.
        let ok_items = vec![
            verification_item(act_block.clone(), act_root, empty_orchard_root),
            verification_item(next_block.clone(), next_root, empty_orchard_root),
        ];
        verify_commitment_roots(&Mainnet, empty_history_tree(), ok_items)
            .expect("real roots verify against the headers");

        // Negative + lag: a wrong root at the activation height (here, the next
        // block's root, which is a valid but different root) is only caught when the
        // following block's commitment is checked.
        assert_ne!(act_root, next_root, "test needs two distinct roots");
        let bad_items = vec![
            verification_item(act_block, next_root, empty_orchard_root),
            verification_item(next_block, next_root, empty_orchard_root),
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

    /// The successor that confirms a block's folded roots can be a *header-only* item —
    /// its header plus ZIP-244 auth-data root, with no body. This is the body-free path
    /// the committer uses to authenticate a block against its already-committed successor
    /// header before the successor's body has been downloaded. The folded root is still
    /// confirmed, and a wrong folded root is still rejected — purely from the successor
    /// header.
    #[test]
    fn header_only_successor_confirms_and_rejects_wrong_root() {
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
        let empty_orchard_root = orchard::tree::NoteCommitmentTree::default().root();
        let next_height = Height(activation + 1);
        let next_auth = next_block.auth_data_root();

        // Positive: `with_roots(act)` folds the real root, and a *header-only* successor
        // item (no body) confirms it.
        let ok_items = vec![
            CommitmentRootVerification::with_roots(
                act_block.clone(),
                act_root,
                empty_orchard_root,
                None,
                false,
            ),
            CommitmentRootVerification::header_only(
                next_block.header.clone(),
                next_height,
                next_auth,
            ),
        ];
        verify_commitment_roots(&Mainnet, empty_history_tree(), ok_items)
            .expect("a header-only successor confirms the real folded root");

        // Negative: a wrong folded root is rejected by the header-only successor alone.
        assert_ne!(act_root, next_root, "test needs two distinct roots");
        let bad_items = vec![
            CommitmentRootVerification::with_roots(
                act_block,
                next_root,
                empty_orchard_root,
                None,
                false,
            ),
            CommitmentRootVerification::header_only(
                next_block.header.clone(),
                next_height,
                next_auth,
            ),
        ];
        let (fail_height, _error) =
            verify_commitment_roots(&Mainnet, empty_history_tree(), bad_items)
                .expect_err("a wrong folded root must be rejected by the header-only successor");
        assert_eq!(
            fail_height.0,
            activation + 1,
            "the wrong root at H is detected at the header-only successor H+1"
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
        let item_at = |h: u32| -> CommitmentRootVerification {
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
            verification_item(block, sapling_root, orchard_root)
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
        let wrong_root = items[0].roots.expect("test verification item has roots").0;
        let mut bad_items = items;
        assert_ne!(
            bad_items[bad_offset]
                .roots
                .expect("test verification item has roots")
                .0,
            wrong_root,
            "need a distinct wrong root"
        );
        bad_items[bad_offset]
            .roots
            .as_mut()
            .expect("test verification item has roots")
            .0 = wrong_root;
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
