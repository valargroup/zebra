//! Rebuild the finalized tip history tree in the current on-disk format.
//!
//! # Why this upgrade exists
//!
//! The history-tree node [`Entry`](zebra_chain::primitives::zcash_history::Entry) is a fixed-size
//! buffer whose length is `zcash_history::MAX_ENTRY_SIZE`. Adding Ironwood (`V3`) to the
//! `zcash_history` dependency grew `MAX_ENTRY_SIZE` (V3 node data carries the extra Ironwood tree
//! roots and tx count), so the buffer went from 253 to 326 bytes.
//!
//! [`HistoryTreeParts`](crate::service::finalized_state::disk_format::chain::HistoryTreeParts)
//! bincode-serializes the tip tree's `peaks: BTreeMap<u32, Entry>`. Databases written before the
//! Ironwood `MAX_ENTRY_SIZE` bump stored each `Entry` at the *smaller* size. The new code reads the
//! *larger* fixed array per entry, overrunning the bincode stream and panicking with
//! `Io(UnexpectedEof)` the first time anything deserializes the history-tree column family (for
//! example in `history_trees_full_tip` during `format_validity_checks_detailed`).
//!
//! Because bincode is not self-describing and uses varint encoding here, there is no clean way to
//! detect-and-read the old layout in place. Instead, this upgrade *rebuilds* the single tip tree
//! from data that is still readable — the finalized blocks and the per-height Sapling/Orchard/
//! Ironwood note commitment tree roots — and writes it back, which re-serializes it in the current
//! `Entry` format. The MMR root is a pure function of that node data, so the rebuilt tree is
//! byte-for-byte equivalent in consensus terms (same `peaks`, same `size`, same root) to the tree a
//! fresh sync would produce.
//!
//! [`crate::service::finalized_state::disk_format::upgrade::DbFormatChange::apply_format_upgrade`]
//! runs this upgrade's [`run`](Upgrade::run) before any validity check reads the column family, so
//! the unreadable entry is replaced before it is ever deserialized.

use std::sync::Arc;

use bincode::Options as _;
use crossbeam_channel::Receiver;
use semver::Version;

use zebra_chain::{
    block::{Block, Height},
    history_tree::HistoryTree,
    ironwood, orchard,
    parameters::{Network, NetworkUpgrade},
    sapling,
};

use crate::service::finalized_state::{
    disk_format::chain::HistoryTreeParts, DiskWriteBatch, ZebraDb,
};

use super::{CancelFormatChange, DiskFormatUpgrade};

/// Implements [`DiskFormatUpgrade`] for rebuilding the tip history tree in the current `Entry`
/// format.
pub struct Upgrade;

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        // Comes after the Ironwood activation-tree upgrade (28.0.0), which is the upgrade that
        // bumped `zcash_history` and grew `MAX_ENTRY_SIZE`, making older history-tree entries
        // unreadable.
        Version::new(28, 1, 0)
    }

    fn description(&self) -> &'static str {
        "rebuild tip history tree in current entry format"
    }

    #[allow(clippy::unwrap_in_result)]
    fn run(
        &self,
        initial_tip_height: Height,
        db: &ZebraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        // Return early if the upgrade is cancelled.
        if cancel_receiver.try_recv().is_ok() {
            return Err(CancelFormatChange);
        }

        // Nothing to rebuild if the tip tree is already readable in the current format. This is the
        // case for databases that were created or last written by code with the current
        // `MAX_ENTRY_SIZE`, including pruned databases that may be missing the historical blocks the
        // rebuild would need.
        if !needs_rebuild(db) {
            return Ok(());
        }

        let network = db.network();

        let Some(history_tree) = rebuild_tip_history_tree(db, &network, initial_tip_height) else {
            // Pre-Heartwood tips have no history tree, so there is nothing to rebuild. (Any stale
            // entry would be deleted rather than rewritten, but pre-Heartwood databases never wrote
            // one.)
            return Ok(());
        };

        // Return before writing if the upgrade is cancelled.
        if cancel_receiver.try_recv().is_ok() {
            return Err(CancelFormatChange);
        }

        // Writing the tree back to the database re-serializes it in the current `Entry` format,
        // overwriting the unreadable old-format entry under the same `()` key.
        let mut batch = DiskWriteBatch::new();
        batch.update_history_tree(db, &history_tree);
        db.write_batch(batch)
            .expect("rewriting the tip history tree in the current format should always succeed");

        Ok(())
    }

    #[allow(clippy::unwrap_in_result)]
    fn validate(
        &self,
        db: &ZebraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        Ok(quick_check(db))
    }
}

/// Returns `true` if the tip history tree entry exists but cannot be deserialized in the current
/// format, and therefore needs to be rebuilt.
///
/// Reads the entry as raw bytes and attempts a non-panicking deserialization in the current format.
/// An entry that fails this check was written with a smaller `Entry` buffer by an older Zebra
/// version, which is exactly the case this upgrade repairs.
pub(crate) fn needs_rebuild(db: &ZebraDb) -> bool {
    let Some(raw_entry) = db.raw_history_tree_value_cf().zs_get(&()) else {
        // No tip tree stored (empty/pre-Heartwood database), so there is nothing to rebuild.
        return false;
    };

    bincode::DefaultOptions::new()
        .deserialize::<HistoryTreeParts>(raw_entry.raw_bytes())
        .is_err()
}

/// Rebuilds the finalized tip history tree from finalized blocks and the per-height note commitment
/// tree roots.
///
/// Returns `None` if the tip is pre-Heartwood, where no history tree exists.
///
/// The history tree resets at every network upgrade boundary, so the tip tree only contains blocks
/// from the current network upgrade's activation height up to the tip. Rebuilding from that
/// activation height reproduces the identical tree.
#[allow(clippy::unwrap_in_result)]
fn rebuild_tip_history_tree(
    db: &ZebraDb,
    network: &Network,
    tip_height: Height,
) -> Option<HistoryTree> {
    let network_upgrade = NetworkUpgrade::current(network, tip_height);

    if network_upgrade < NetworkUpgrade::Heartwood {
        return None;
    }

    let start_height = network_upgrade
        .activation_height(network)
        .expect("network upgrades at or after Heartwood have an activation height");

    let (block, sapling_root, orchard_root, ironwood_root) =
        history_rebuild_inputs_at_height(db, start_height);
    let mut history_tree =
        HistoryTree::from_block(network, block, &sapling_root, &orchard_root, &ironwood_root)
            .expect("rebuilding the tip history tree from a finalized block should always succeed");

    for height in ((start_height.0 + 1)..=tip_height.0).map(Height) {
        let (block, sapling_root, orchard_root, ironwood_root) =
            history_rebuild_inputs_at_height(db, height);

        history_tree
            .push(network, block, &sapling_root, &orchard_root, &ironwood_root)
            .expect("pushing a finalized block onto the tip history tree should always succeed");
    }

    Some(history_tree)
}

/// Loads the block and the Sapling, Orchard, and Ironwood note commitment tree roots at `height`,
/// which are the inputs needed to add a block to the history tree.
///
/// This reads only column families that are unaffected by the `Entry` format change, so it works on
/// a database whose history-tree column family is in the old format.
fn history_rebuild_inputs_at_height(
    db: &ZebraDb,
    height: Height,
) -> (
    Arc<Block>,
    sapling::tree::Root,
    orchard::tree::Root,
    ironwood::tree::Root,
) {
    let block = db
        .block(height.into())
        .expect("history tree rebuild requires every finalized block up to the tip");
    let sapling_root = db
        .sapling_tree_by_height(&height)
        .expect("history tree rebuild requires the Sapling tree at every height up to the tip")
        .root();
    let orchard_root = db
        .orchard_tree_by_height(&height)
        .expect("history tree rebuild requires the Orchard tree at every height up to the tip")
        .root();
    // Ironwood trees are only stored from the Ironwood activation height onwards, and are
    // de-duplicated, so search backwards for the most recent one. Before Ironwood activation the
    // root is the empty-tree root, which the pre-Ironwood history tree versions ignore.
    let ironwood_root = match db.ironwood_tree_by_height_range(..=height).last() {
        Some((_height, tree)) => tree.root(),
        None => Default::default(),
    };

    (block, sapling_root, orchard_root, ironwood_root)
}

/// Quickly checks that the tip history tree can be read in the current format.
///
/// After this upgrade runs, the entry (if any) must deserialize cleanly in the current `Entry`
/// format. An entry that still fails means the rebuild did not complete.
pub fn quick_check(db: &ZebraDb) -> Result<(), String> {
    if needs_rebuild(db) {
        let err = Err(
            "tip history tree could not be read in the current format after the history tree \
             rebuild upgrade"
                .to_string(),
        );
        error!(?err);
        return err;
    }

    Ok(())
}
