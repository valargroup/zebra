//! Backfill Ironwood state needed by existing databases.

use crossbeam_channel::Receiver;
use semver::Version;
use zebra_chain::{block::Height, ironwood, parameters::NetworkUpgrade};

use crate::service::finalized_state::{DiskWriteBatch, ZebraDb};

use super::{rebuild_history_tree, CancelFormatChange, DiskFormatUpgrade};

/// Implements [`DiskFormatUpgrade`] for adding Ironwood upgrade state.
pub struct Upgrade;

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        Version::new(28, 0, 0)
    }

    fn description(&self) -> &'static str {
        "add Ironwood value pool, indexes, activation tree, and repaired history tree"
    }

    #[allow(clippy::unwrap_in_result)]
    fn run(
        &self,
        initial_tip_height: Height,
        db: &ZebraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        if let Some(activation_height) = NetworkUpgrade::Nu6_3.activation_height(&db.network()) {
            if initial_tip_height >= activation_height
                && db
                    .ironwood_tree_by_height_range(..=activation_height)
                    .next()
                    .is_none()
            {
                let mut batch = DiskWriteBatch::new();
                let ironwood_tree = ironwood::tree::NoteCommitmentTree::default();
                batch.create_ironwood_tree(db, &activation_height, &ironwood_tree);
                db.write_batch(batch)
                    .expect("backfilling the Ironwood activation tree should always succeed");
            }
        }

        // Return early if the upgrade is cancelled.
        if cancel_receiver.try_recv().is_ok() {
            return Err(CancelFormatChange);
        }

        // The tip tree is rebuilt synchronously while the database is opened (see
        // `rebuild_tip_history_tree_if_needed`), so by the time this runs in the background upgrade
        // thread there is normally nothing to do. This call is kept for idempotency and to handle
        // the case where the synchronous rebuild was skipped.
        if let Err(err @ rebuild_history_tree::RebuildError::MissingData { .. }) =
            rebuild_history_tree::rebuild_tip_history_tree_if_needed(db, initial_tip_height)
        {
            // A pruned old-format database can't be rebuilt. Surface it as a loud, explained panic
            // rather than marking the database as upgraded with an unreadable entry. (The
            // synchronous open path returns this same error before this point in production.)
            panic!("{err}");
        }

        Ok(())
    }

    #[allow(clippy::unwrap_in_result)]
    fn validate(
        &self,
        db: &ZebraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        Ok(rebuild_history_tree::quick_check(db))
    }
}
