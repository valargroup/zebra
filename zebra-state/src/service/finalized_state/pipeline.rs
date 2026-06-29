//! In-memory state for the run-ahead finalized-commit pipeline.
//!
//! When [`Config::finalized_block_pipeline_depth`](crate::config::Config::finalized_block_pipeline_depth)
//! is greater than zero, the finalized committer assembles a block's
//! [`DiskWriteBatch`](super::DiskWriteBatch) on the assembler thread and hands it
//! to a dedicated disk-writer thread, so the next block's assembly overlaps this
//! block's flush.
//!
//! Assembling block `X + 1` while block `X` is still being flushed means `X + 1`'s
//! reads can target state that `X` created but has not yet written to disk. This
//! module holds that not-yet-durable state so those reads can be served from
//! memory before falling back to RocksDB:
//!
//! - **Threaded tip state** — the history tree, note-commitment trees, value pool,
//!   `vct_upgrade_height` marker, and the (height, hash) tip cursor. These advance
//!   every block and are otherwise read fresh from disk per block, so they must be
//!   carried forward in memory while the assembler is ahead of the writer.
//! - **A read-through overlay** — the transparent outputs created, and the absolute
//!   address balances updated, by not-yet-flushed blocks. Entries are retired once
//!   the writer has flushed up to (or past) the height that wrote them, so the
//!   overlay is bounded by the pipeline depth.
//!
//! This is only used below the last checkpoint, the reorg-free region, so the
//! overlay never has to roll back: a crash mid-pipeline simply resumes from the
//! durable finalized tip and re-downloads the gap.
//!
//! The committer (in [`write`](crate::service::write)) [`seed`]s the pipeline from
//! disk, [`record_block`]s each assembled block's contribution before handing its
//! batch to the disk-writer thread, and [`retire_through`]s the overlay as the
//! writer makes blocks durable.
//!
//! [`seed`]: FinalizedPipeline::seed
//! [`record_block`]: FinalizedPipeline::record_block
//! [`retire_through`]: FinalizedPipeline::retire_through

use std::{collections::HashMap, sync::Arc};

use zebra_chain::{
    amount::NonNegative,
    block::{self, Height},
    history_tree::HistoryTree,
    transparent,
    value_balance::ValueBalance,
};

use crate::service::finalized_state::{
    disk_format::transparent::{AddressBalanceLocation, AddressBalanceLocationUpdates},
    disk_format::OutputLocation,
    ZebraDb,
};

use super::NoteCommitmentTrees;

/// The pipeline-relevant outputs of [`DiskWriteBatch::prepare_block_batch`], returned
/// so the run-ahead committer can thread them forward in memory.
///
/// [`DiskWriteBatch::prepare_block_batch`]: super::DiskWriteBatch::prepare_block_batch
pub(crate) struct BlockBatchOutputs {
    /// The chain value pool after this block.
    pub value_pool: ValueBalance<NonNegative>,
    /// The absolute address balances this block updated, captured only when the
    /// pipeline is running ahead (`None` on the synchronous path).
    pub address_balances: Option<AddressBalanceLocationUpdates>,
    /// Whether this block wrote the set-once `vct_upgrade_height` marker.
    pub wrote_vct_upgrade_marker: bool,
}

/// The pipeline contribution captured by [`ZebraDb::assemble_block_batch`] for a
/// single block: its value pool, created outputs, and updated address balances.
/// Combined with the block's tip/history/note trees by the committer to form a
/// [`BlockPipelineContribution`].
///
/// [`ZebraDb::assemble_block_batch`]: super::ZebraDb::assemble_block_batch
pub(crate) struct PipelineBatchContribution {
    /// The chain value pool after this block.
    pub value_pool: ValueBalance<NonNegative>,
    /// Whether this block wrote the set-once `vct_upgrade_height` marker.
    pub wrote_vct_upgrade_marker: bool,
    /// The transparent outputs this block created: `(outpoint, location, utxo)`.
    pub created_outputs: Vec<(transparent::OutPoint, OutputLocation, transparent::Utxo)>,
    /// The absolute address balances this block updated.
    pub updated_balances: Vec<(transparent::Address, AddressBalanceLocation)>,
}

/// A transparent output created by a not-yet-flushed block.
#[derive(Clone, Debug)]
struct OverlayUtxo {
    /// The on-disk location this output will occupy once flushed.
    out_loc: OutputLocation,
    /// The unspent output.
    utxo: transparent::Utxo,
    /// The height of the block that created it, for retirement.
    height: Height,
}

/// An absolute address balance written by a not-yet-flushed block.
#[derive(Clone, Debug)]
struct OverlayBalance {
    /// The absolute balance/location after the latest not-yet-flushed update.
    balance: AddressBalanceLocation,
    /// The height of the latest block that updated it, for retirement.
    height: Height,
}

/// Everything block assembly captures about a committed-but-not-yet-flushed block,
/// so the next block's assembly can read its effects from memory.
#[derive(Clone, Debug)]
pub(crate) struct BlockPipelineContribution {
    /// The committed block's height and hash (the new in-memory tip cursor).
    pub tip: (Height, block::Hash),
    /// The history tree after this block.
    pub history_tree: Arc<HistoryTree>,
    /// The note-commitment trees after this block.
    pub note_commitment_trees: NoteCommitmentTrees,
    /// The value pool after this block.
    pub value_pool: ValueBalance<NonNegative>,
    /// Whether this block wrote the set-once `vct_upgrade_height` marker.
    pub wrote_vct_upgrade_marker: bool,
    /// The transparent outputs this block created: `(outpoint, location, utxo)`.
    pub created_outputs: Vec<(transparent::OutPoint, OutputLocation, transparent::Utxo)>,
    /// The absolute address balances this block updated.
    pub updated_balances: Vec<(transparent::Address, AddressBalanceLocation)>,
}

/// The in-memory tip state and read-through overlay for the run-ahead committer.
///
/// Seeded lazily from disk on the first block, then advanced in memory by
/// [`record_block`](Self::record_block) and trimmed by
/// [`retire_through`](Self::retire_through) as the writer makes blocks durable.
#[derive(Debug)]
pub(crate) struct FinalizedPipeline {
    /// `true` once the threaded tip state has been seeded from disk.
    seeded: bool,
    /// The in-memory finalized tip `(height, hash)`, ahead of the durable tip.
    tip: Option<(Height, block::Hash)>,
    /// The history tree after the latest assembled block.
    history_tree: Arc<HistoryTree>,
    /// The note-commitment trees after the latest assembled block.
    note_commitment_trees: NoteCommitmentTrees,
    /// The value pool after the latest assembled block.
    value_pool: ValueBalance<NonNegative>,
    /// Whether a not-yet-flushed block has written the `vct_upgrade_height` marker.
    vct_upgrade_marker_set: bool,
    /// Transparent outputs created by not-yet-flushed blocks, keyed by outpoint.
    utxos: HashMap<transparent::OutPoint, OverlayUtxo>,
    /// Absolute address balances updated by not-yet-flushed blocks.
    address_balances: HashMap<transparent::Address, OverlayBalance>,
}

impl FinalizedPipeline {
    /// Create an empty, unseeded pipeline. The threaded tip state is read from
    /// `db` on the first [`seed`](Self::seed).
    pub(crate) fn new() -> Self {
        Self {
            seeded: false,
            tip: None,
            history_tree: Arc::new(HistoryTree::default()),
            note_commitment_trees: NoteCommitmentTrees::default(),
            value_pool: ValueBalance::zero(),
            vct_upgrade_marker_set: false,
            utxos: HashMap::new(),
            address_balances: HashMap::new(),
        }
    }

    /// Seed the threaded tip state from the durable database, if not already seeded.
    ///
    /// Called before assembling the first block of a pipeline run, so the in-memory
    /// tip state starts exactly equal to disk.
    pub(crate) fn seed(&mut self, db: &ZebraDb) {
        if self.seeded {
            return;
        }

        self.tip = db
            .finalized_tip_height()
            .map(|height| (height, db.finalized_tip_hash()));
        self.history_tree = db.history_tree();
        self.note_commitment_trees = db.note_commitment_trees_for_tip();
        self.value_pool = db.finalized_value_pool();
        self.vct_upgrade_marker_set = db.vct_upgrade_height().is_some();
        self.seeded = true;
    }

    /// The in-memory tip `(height, hash)`, used for the commit ordering asserts.
    pub(crate) fn tip(&self) -> Option<(Height, block::Hash)> {
        self.tip
    }

    /// The note-commitment trees after the latest assembled block (the parent
    /// trees for the next block).
    pub(crate) fn note_commitment_trees(&self) -> NoteCommitmentTrees {
        self.note_commitment_trees.clone()
    }

    /// The history tree after the latest assembled block.
    pub(crate) fn history_tree(&self) -> Arc<HistoryTree> {
        self.history_tree.clone()
    }

    /// The value pool after the latest assembled block (the starting pool for the
    /// next block).
    pub(crate) fn value_pool(&self) -> ValueBalance<NonNegative> {
        self.value_pool
    }

    /// Whether a not-yet-flushed block has already written the set-once
    /// `vct_upgrade_height` marker, so the next block must not write it again.
    pub(crate) fn vct_upgrade_marker_set(&self) -> bool {
        self.vct_upgrade_marker_set
    }

    /// Overlay lookup for a spent output: returns its location and value if it was
    /// created by a not-yet-flushed block.
    pub(crate) fn spent_utxo_override(
        &self,
        outpoint: &transparent::OutPoint,
    ) -> Option<(OutputLocation, transparent::Utxo)> {
        self.utxos
            .get(outpoint)
            .map(|entry| (entry.out_loc, entry.utxo.clone()))
    }

    /// Overlay lookup for an address balance: returns the absolute balance if a
    /// not-yet-flushed block updated it.
    pub(crate) fn address_balance_override(
        &self,
        address: &transparent::Address,
    ) -> Option<AddressBalanceLocation> {
        self.address_balances
            .get(address)
            .map(|entry| entry.balance)
    }

    /// Record a freshly-assembled block's effects, advancing the in-memory tip
    /// state and overlay so the next block's assembly observes them.
    pub(crate) fn record_block(&mut self, contribution: BlockPipelineContribution) {
        let BlockPipelineContribution {
            tip,
            history_tree,
            note_commitment_trees,
            value_pool,
            wrote_vct_upgrade_marker,
            created_outputs,
            updated_balances,
        } = contribution;

        let (height, _hash) = tip;

        self.tip = Some(tip);
        self.history_tree = history_tree;
        self.note_commitment_trees = note_commitment_trees;
        self.value_pool = value_pool;
        self.vct_upgrade_marker_set |= wrote_vct_upgrade_marker;

        for (outpoint, out_loc, utxo) in created_outputs {
            self.utxos.insert(
                outpoint,
                OverlayUtxo {
                    out_loc,
                    utxo,
                    height,
                },
            );
        }

        for (address, balance) in updated_balances {
            self.address_balances
                .insert(address, OverlayBalance { balance, height });
        }
    }

    /// Retire overlay entries the writer has made durable, after it flushes
    /// `flushed_height`.
    ///
    /// Once a block is on disk, its created outputs and updated balances are served
    /// from RocksDB, so the overlay entries written at or below the durable height
    /// are redundant and can be dropped, keeping the overlay bounded by depth.
    pub(crate) fn retire_through(&mut self, flushed_height: Height) {
        self.utxos.retain(|_, entry| entry.height > flushed_height);
        self.address_balances
            .retain(|_, entry| entry.height > flushed_height);
    }
}
