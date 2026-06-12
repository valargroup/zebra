//! Rebuild the history tree using the current encoded entry format.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;
use zebra_chain::{block::Height, history_tree::HistoryTree};

use crate::service::finalized_state::{DiskWriteBatch, ZebraDb};

use super::{CancelFormatChange, DiskFormatUpgrade};

/// Implements [`DiskFormatUpgrade`] for rebuilding history tree entries.
pub struct RebuildHistoryTree;

impl DiskFormatUpgrade for RebuildHistoryTree {
    fn version(&self) -> Version {
        Version::new(29, 0, 0)
    }

    fn description(&self) -> &'static str {
        "rebuild history tree entries for Ironwood metadata"
    }

    #[allow(clippy::unwrap_in_result)]
    fn run(
        &self,
        _initial_tip_height: Height,
        db: &ZebraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        loop {
            check_cancelled(cancel_receiver)?;

            let Some(tip @ (tip_height, _)) = db.tip() else {
                return Ok(());
            };

            let history_tree =
                db.rebuild_history_tree_to_height(tip_height, || check_cancelled(cancel_receiver))?;

            check_cancelled(cancel_receiver)?;

            let mut batch = DiskWriteBatch::new();
            batch.update_history_tree(db, &history_tree);

            let wrote_tree = db
                .write_batch_if_finalized_tip(batch, tip)
                .expect("rewriting history tree data should always succeed");

            if wrote_tree {
                return Ok(());
            }
        }
    }

    fn validate(
        &self,
        db: &ZebraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        loop {
            check_cancelled(cancel_receiver)?;

            let Some(tip @ (tip_height, _)) = db.tip() else {
                return Ok(Ok(()));
            };

            let expected_history_tree =
                db.rebuild_history_tree_to_height(tip_height, || check_cancelled(cancel_receiver))?;
            let history_tree = db.history_tree_from_disk();

            if db.tip() != Some(tip) {
                continue;
            }

            let expected_hash = expected_history_tree.hash();
            let actual_hash = history_tree.hash();

            if actual_hash != expected_hash {
                return Ok(Err(format!(
                    "history tree hash mismatch at finalized tip {tip_height:?}: \
                     expected {expected_hash:?}, found {actual_hash:?}"
                )));
            }

            let expected_height = history_tree_height(&expected_history_tree);
            let actual_height = history_tree_height(&history_tree);

            if actual_height != expected_height {
                return Ok(Err(format!(
                    "history tree height mismatch at finalized tip {tip_height:?}: \
                     expected {expected_height:?}, found {actual_height:?}"
                )));
            }

            if db.tip() == Some(tip) {
                return Ok(Ok(()));
            }
        }
    }
}

fn check_cancelled(
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    match cancel_receiver.try_recv() {
        Err(TryRecvError::Empty) => Ok(()),
        _ => Err(CancelFormatChange),
    }
}

fn history_tree_height(history_tree: &HistoryTree) -> Option<Height> {
    history_tree.as_ref().map(|tree| tree.current_height())
}
