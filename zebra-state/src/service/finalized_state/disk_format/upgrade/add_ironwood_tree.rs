//! Backfill the empty Ironwood note commitment tree for existing databases.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;
use zebra_chain::{block::Height, ironwood};

use crate::service::finalized_state::{DiskWriteBatch, ZebraDb};

use super::{CancelFormatChange, DiskFormatUpgrade};

/// Implements [`DiskFormatUpgrade`] for adding Ironwood tree data.
pub struct Upgrade;

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        Version::new(28, 0, 0)
    }

    fn description(&self) -> &'static str {
        "add ironwood value pool and indexes upgrade"
    }

    #[allow(clippy::unwrap_in_result)]
    fn run(
        &self,
        _initial_tip_height: Height,
        db: &ZebraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        if has_ironwood_tree_at_or_before(db, Height::MIN) {
            return Ok(());
        }

        let ironwood_tree = ironwood::tree::NoteCommitmentTree::default();
        let mut batch = DiskWriteBatch::new();
        batch.create_ironwood_tree(db, &Height::MIN, &ironwood_tree);

        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        db.write_batch(batch)
            .expect("backfilling Ironwood tree data should always succeed");

        Ok(())
    }

    fn validate(
        &self,
        db: &ZebraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        let Some(tip_height) = db.finalized_tip_height() else {
            return Ok(Ok(()));
        };

        if !has_ironwood_tree_at_or_before(db, Height::MIN) {
            return Ok(Err(
                "missing Ironwood note commitment tree for the first finalized height".to_string(),
            ));
        }

        let Some((_height, ironwood_tree)) = db.ironwood_tree_by_height_range(..=tip_height).last()
        else {
            return Ok(Err(format!(
                "missing Ironwood note commitment tree for finalized tip {tip_height:?}"
            )));
        };

        if !db.contains_ironwood_anchor(&ironwood_tree.root()) {
            return Ok(Err(format!(
                "missing Ironwood anchor for finalized tip {tip_height:?}"
            )));
        }

        Ok(Ok(()))
    }
}

fn has_ironwood_tree_at_or_before(db: &ZebraDb, height: Height) -> bool {
    db.ironwood_tree_by_height_range(..=height).next().is_some()
}
