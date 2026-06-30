//! Provides high-level access to database whole-chain:
//! - history trees
//! - chain value pools
//!
//! This module makes sure that:
//! - all disk writes happen inside a RocksDB transaction, and
//! - format-specific invariants are maintained.
//!
//! # Correctness
//!
//! [`crate::constants::state_database_format_version_in_code()`] must be incremented
//! each time the database format (column, serialization, etc) changes.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use zebra_chain::{
    amount::NonNegative,
    block::Height,
    block_info::BlockInfo,
    history_tree::HistoryTree,
    parameters::Network,
    serialization::{CompactSizeMessage, ZcashSerialize as _},
    transparent,
    value_balance::ValueBalance,
};

use crate::{
    request::FinalizedBlock,
    service::finalized_state::{
        disk_db::{DiskWriteBatch, ReadDisk},
        disk_format::transparent::AddressBalanceLocationUpdates,
        disk_format::{chain::HistoryTreeParts, OutputLocation, RawBytes},
        pipeline::ReconcileBlock,
        zebra_db::{metrics::value_pool_metrics, ZebraDb},
        TypedColumnFamily,
    },
    BoxError, HashOrHeight, ValidateContextError,
};

/// The name of the History Tree column family.
///
/// This constant should be used so the compiler can detect typos.
pub const HISTORY_TREE: &str = "history_tree";

/// The type for reading history trees from the database.
///
/// This constant should be used so the compiler can detect incorrectly typed accesses to the
/// column family.
pub type HistoryTreePartsCf<'cf> = TypedColumnFamily<'cf, (), HistoryTreeParts>;

/// The legacy (1.3.0 and earlier) type for reading history trees from the database.
/// This type should not be used in new code.
pub type LegacyHistoryTreePartsCf<'cf> = TypedColumnFamily<'cf, Height, HistoryTreeParts>;

/// A generic raw key type for reading history trees from the database, regardless of the database version.
/// This type should not be used in new code.
pub type RawHistoryTreePartsCf<'cf> = TypedColumnFamily<'cf, RawBytes, HistoryTreeParts>;

/// The name of the tip-only chain value pools column family.
///
/// This constant should be used so the compiler can detect typos.
pub const CHAIN_VALUE_POOLS: &str = "tip_chain_value_pool";

/// The type for reading value pools from the database.
///
/// This constant should be used so the compiler can detect incorrectly typed accesses to the
/// column family.
pub type ChainValuePoolsCf<'cf> = TypedColumnFamily<'cf, (), ValueBalance<NonNegative>>;

/// The name of the block info column family.
///
/// This constant should be used so the compiler can detect typos.
pub const BLOCK_INFO: &str = "block_info";

/// The type for reading value pools from the database.
///
/// This constant should be used so the compiler can detect incorrectly typed accesses to the
/// column family.
pub type BlockInfoCf<'cf> = TypedColumnFamily<'cf, Height, BlockInfo>;

impl ZebraDb {
    // Column family convenience methods

    /// Returns a typed handle to the `history_tree` column family.
    pub(crate) fn history_tree_cf(&self) -> HistoryTreePartsCf<'_> {
        HistoryTreePartsCf::new(&self.db, HISTORY_TREE)
            .expect("column family was created when database was created")
    }

    /// Returns a legacy typed handle to the `history_tree` column family.
    /// This should not be used in new code.
    pub(crate) fn legacy_history_tree_cf(&self) -> LegacyHistoryTreePartsCf<'_> {
        LegacyHistoryTreePartsCf::new(&self.db, HISTORY_TREE)
            .expect("column family was created when database was created")
    }

    /// Returns a generic raw key typed handle to the `history_tree` column family.
    /// This should not be used in new code.
    pub(crate) fn raw_history_tree_cf(&self) -> RawHistoryTreePartsCf<'_> {
        RawHistoryTreePartsCf::new(&self.db, HISTORY_TREE)
            .expect("column family was created when database was created")
    }

    /// Returns a typed handle to the chain value pools column family.
    pub(crate) fn chain_value_pools_cf(&self) -> ChainValuePoolsCf<'_> {
        ChainValuePoolsCf::new(&self.db, CHAIN_VALUE_POOLS)
            .expect("column family was created when database was created")
    }

    /// Returns a typed handle to the block data column family.
    pub(crate) fn block_info_cf(&self) -> BlockInfoCf<'_> {
        BlockInfoCf::new(&self.db, BLOCK_INFO)
            .expect("column family was created when database was created")
    }

    // History tree methods

    /// Returns the ZIP-221 history tree of the finalized tip.
    ///
    /// If history trees have not been activated yet (pre-Heartwood), or the state is empty,
    /// returns an empty history tree.
    pub fn history_tree(&self) -> Arc<HistoryTree> {
        let history_tree_cf = self.history_tree_cf();

        // # Backwards Compatibility
        //
        // This code can read the column family format in 1.2.0 and earlier (tip height key),
        // and after PR #7392 is merged (empty key). The height-based code can be removed when
        // versions 1.2.0 and earlier are no longer supported.
        //
        // # Concurrency
        //
        // There is only one entry in this column family, which is atomically updated by a block
        // write batch (database transaction). If we used a height as the key in this column family,
        // any updates between reading the tip height and reading the tree could cause panics.
        //
        // So we use the empty key `()`. Since the key has a constant value, we will always read
        // the latest tree.
        let mut history_tree_parts = history_tree_cf.zs_get(&());

        if history_tree_parts.is_none() {
            let legacy_history_tree_cf = self.legacy_history_tree_cf();

            // In Zebra 1.4.0 and later, we only update the history tip tree when it has changed (for every block after heartwood).
            // But we write with a `()` key, not a height key.
            // So we need to look for the most recent update height if the `()` key has never been written.
            history_tree_parts = legacy_history_tree_cf
                .zs_last_key_value()
                .map(|(_height_key, tree_value)| tree_value);
        }

        let history_tree = history_tree_parts.map(|parts| {
            parts.with_network(&self.db.network()).expect(
                "deserialization format should match the serialization format used by IntoDisk",
            )
        });
        Arc::new(HistoryTree::from(history_tree))
    }

    /// Returns `Ok(())` if the stored tip history tree decodes with the current
    /// `HistoryTreeParts` format.
    ///
    /// This is a non-panicking compatibility probe used during DB open before
    /// background format checks can call [`Self::history_tree`]. It reads raw
    /// bytes and performs the same current-key then legacy-key fallback as
    /// [`Self::history_tree`].
    pub(crate) fn check_tip_history_tree_decodes(&self) -> Result<(), String> {
        let history_tree_cf = self
            .db
            .cf_handle(HISTORY_TREE)
            .expect("column family was created when database was created");

        let raw_parts: Option<RawBytes> = self.db.zs_get(&history_tree_cf, &());
        let raw_parts = raw_parts.or_else(|| {
            self.db
                .zs_last_key_value::<_, RawBytes, RawBytes>(&history_tree_cf)
                .map(|(_height_key, tree_value)| tree_value)
        });

        let Some(raw_parts) = raw_parts else {
            return Ok(());
        };

        let parts = HistoryTreeParts::from_bytes_result(raw_parts.raw_bytes())
            .map_err(|error| format!("stored history tree does not deserialize: {error}"))?;

        parts
            .with_network(&self.db.network())
            .map_err(|error| format!("stored history tree is invalid for this network: {error}"))?;

        Ok(())
    }

    /// Returns all the history tip trees.
    /// We only store the history tree for the tip, so this method is only used in tests and
    /// upgrades.
    pub(crate) fn history_trees_full_tip(&self) -> BTreeMap<RawBytes, Arc<HistoryTree>> {
        let raw_history_tree_cf = self.raw_history_tree_cf();

        raw_history_tree_cf
            .zs_forward_range_iter(..)
            .map(|(raw_key, history_tree_parts)| {
                let history_tree = history_tree_parts.with_network(&self.db.network()).expect(
                    "deserialization format should match the serialization format used by IntoDisk",
                );
                (raw_key, Arc::new(HistoryTree::from(history_tree)))
            })
            .collect()
    }

    // Value pool methods

    /// Returns the stored `ValueBalance` for the best chain at the finalized tip height.
    pub fn finalized_value_pool(&self) -> ValueBalance<NonNegative> {
        let chain_value_pools_cf = self.chain_value_pools_cf();

        chain_value_pools_cf
            .zs_get(&())
            .unwrap_or_else(ValueBalance::zero)
    }

    /// Verification helper (offline tools): the count of `block_info` entries and
    /// the lowest/highest heights missing a `BlockInfo` in `[lo, hi]`.
    pub fn block_info_coverage(&self, lo: Height, hi: Height) -> (u64, Option<Height>, u64) {
        let cf = self.block_info_cf();
        let mut count = 0u64;
        let mut first_missing = None;
        let mut missing = 0u64;
        for height in lo.0..=hi.0 {
            if cf.zs_get(&Height(height)).is_some() {
                count += 1;
            } else {
                if first_missing.is_none() {
                    first_missing = Some(Height(height));
                }
                missing += 1;
            }
        }
        (count, first_missing, missing)
    }

    /// Returns the stored `BlockInfo` for the given block.
    pub fn block_info(&self, hash_or_height: HashOrHeight) -> Option<BlockInfo> {
        let height = hash_or_height.height_or_else(|hash| self.height(hash))?;

        let block_info_cf = self.block_info_cf();

        block_info_cf.zs_get(&height)
    }

    /// Per-checkpoint transparent reconcile: write, in one atomic batch, the
    /// `utxo_by_out_loc` deletes for every deferred spend in `records` and the
    /// recomputed chain value pool (the tip pool plus per-height `BlockInfo`).
    /// Returns the value pool after the last block in `records`.
    ///
    /// `start_value_pool` is the pool before the first block in `records`,
    /// `resolved` maps each spent outpoint to its on-disk location and UTXO.
    ///
    /// This reproduces, in a batched pass, exactly what the per-block
    /// [`prepare_spent_transparent_outputs_batch`](DiskWriteBatch::prepare_spent_transparent_outputs_batch)
    /// (with the address index off) and
    /// [`prepare_chain_value_pools_batch`](DiskWriteBatch::prepare_chain_value_pools_batch)
    /// would have written inline, so the resulting state is byte-identical.
    #[allow(clippy::unwrap_in_result)]
    pub(crate) fn commit_checkpoint_reconcile(
        &self,
        network: &Network,
        start_value_pool: ValueBalance<NonNegative>,
        records: &[ReconcileBlock],
        resolved: &HashMap<transparent::OutPoint, (OutputLocation, transparent::Utxo)>,
    ) -> Result<ValueBalance<NonNegative>, BoxError> {
        let mut batch = DiskWriteBatch::new();

        // The deletes: every resolved spend, keyed by output location. The address
        // index is off in the deferred range, so `skip_index = true` makes this
        // delete only the `utxo_by_out_loc` entries (no address-link deletes), and
        // the empty `address_balances` is never consulted.
        let spent_utxos_by_out_loc: BTreeMap<OutputLocation, transparent::Utxo> = resolved
            .values()
            .map(|(out_loc, utxo)| (*out_loc, utxo.clone()))
            .collect();
        batch.prepare_spent_transparent_outputs_batch(
            &self.db,
            network,
            &spent_utxos_by_out_loc,
            &AddressBalanceLocationUpdates::Insert(HashMap::new()),
            true,
        );

        // Recompute the value pool sequentially in block order, writing each block's
        // running pool into `BlockInfo` (exactly as the inline per-block path does),
        // and the final pool into the single-key tip CF once at the end.
        let mut value_pool = start_value_pool;
        for record in records {
            let spent_by_block: HashMap<transparent::OutPoint, transparent::Utxo> = record
                .spent
                .iter()
                .map(|outpoint| {
                    let (_out_loc, utxo) = resolved
                        .get(outpoint)
                        .expect("every deferred spend was resolved above");
                    (*outpoint, utxo.clone())
                })
                .collect();

            let block_value_pool_change = record
                .block
                .chain_value_pool_change(&spent_by_block, record.deferred_pool_change)?;
            value_pool = value_pool.add_chain_value_pool_change(block_value_pool_change)?;
            value_pool_metrics(&value_pool);

            // Block size, summed per-transaction (byte-identical to serializing the
            // whole block), as in `prepare_chain_value_pools_batch`.
            let block_size = {
                let transactions = &record.block.transactions;
                let transactions_size: usize = transactions
                    .iter()
                    .map(|transaction| transaction.zcash_serialized_size())
                    .sum();
                let tx_count_size = CompactSizeMessage::try_from(transactions.len())
                    .expect("block must have a valid transaction count")
                    .zcash_serialized_size();
                record.block.header.zcash_serialized_size() + tx_count_size + transactions_size
            };

            let _ = self
                .block_info_cf()
                .with_batch_for_writing(&mut batch)
                .zs_insert(
                    &record.height,
                    &BlockInfo::new(value_pool, block_size as u32),
                );
        }

        let _ = self
            .chain_value_pools_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&(), &value_pool);

        self.write_batch(batch)?;

        Ok(value_pool)
    }
}

impl DiskWriteBatch {
    // History tree methods

    /// Updates the history tree for the tip, if it is not empty.
    ///
    /// The batch must be written to the database by the caller.
    pub fn update_history_tree(&mut self, db: &ZebraDb, tree: &HistoryTree) {
        let history_tree_cf = db.history_tree_cf().with_batch_for_writing(self);

        if let Some(tree) = tree.as_ref() {
            // The batch is modified by this method and written by the caller.
            let _ = history_tree_cf.zs_insert(&(), &HistoryTreeParts::from(tree));
        }
    }

    /// Legacy method: Deletes the range of history trees at the given [`Height`]s.
    /// Doesn't delete the upper bound.
    ///
    /// From state format 25.3.0 onwards, the history trees are indexed by an empty key,
    /// so this method does nothing.
    ///
    /// The batch must be written to the database by the caller.
    pub fn delete_range_history_tree(
        &mut self,
        db: &ZebraDb,
        from: &Height,
        until_strictly_before: &Height,
    ) {
        let history_tree_cf = db.legacy_history_tree_cf().with_batch_for_writing(self);

        // The batch is modified by this method and written by the caller.
        //
        // TODO: convert zs_delete_range() to take std::ops::RangeBounds
        let _ = history_tree_cf.zs_delete_range(from, until_strictly_before);
    }

    // Value pool methods

    /// Prepares a database batch containing the chain value pool update from `finalized.block`, and
    /// returns it without actually writing anything.
    ///
    /// The batch is modified by this method and written by the caller. The caller should not write
    /// the batch if this method returns an error.
    ///
    /// The parameter `utxos_spent_by_block` must contain the [`transparent::Utxo`]s of every input
    /// in this block, including UTXOs created by earlier transactions in this block.
    ///
    /// Note that the chain value pool has the opposite sign to the transaction value pool. See the
    /// [`chain_value_pool_change`] and [`add_chain_value_pool_change`] methods for more details.
    ///
    /// # Errors
    ///
    /// - Propagates any errors from updating value pools
    ///
    /// [`chain_value_pool_change`]: zebra_chain::block::Block::chain_value_pool_change
    /// [`add_chain_value_pool_change`]: ValueBalance::add_chain_value_pool_change
    #[allow(clippy::unwrap_in_result)]
    /// Returns the chain value pool after applying this block, so the run-ahead
    /// committer can thread it forward in memory to the next block's assembly.
    pub fn prepare_chain_value_pools_batch(
        &mut self,
        db: &ZebraDb,
        finalized: &FinalizedBlock,
        utxos_spent_by_block: HashMap<transparent::OutPoint, transparent::Utxo>,
        value_pool: ValueBalance<NonNegative>,
    ) -> Result<ValueBalance<NonNegative>, ValidateContextError> {
        // Per-checkpoint reconcile / ceiling probe: the value-pool change needs the spent
        // values. When deferring (`defers_transparent_spends`), the spent reads are skipped
        // here and recomputed in the batched checkpoint reconcile, so leave the pool
        // unchanged (it is threaded forward and corrected at the next checkpoint). The
        // benchmark probe (`bench_skip_transparent_reads`) skips it with no reconcile,
        // leaving the pool permanently stale (measurement only, never shipped).
        if db.defers_transparent_spends() || super::bench_skip_transparent_reads() {
            return Ok(value_pool);
        }

        let block_value_pool_change = finalized
            .block
            .chain_value_pool_change(
                &utxos_spent_by_block,
                finalized.deferred_pool_balance_change,
            )
            .map_err(|value_balance_error| {
                ValidateContextError::CalculateBlockChainValueChange {
                    value_balance_error,
                    height: finalized.height,
                    block_hash: finalized.hash,
                    transaction_count: finalized.transaction_hashes.len(),
                    spent_utxo_count: utxos_spent_by_block.len(),
                }
            })?;

        let new_value_pool = value_pool
            .add_chain_value_pool_change(block_value_pool_change)
            .map_err(|value_balance_error| ValidateContextError::AddValuePool {
                value_balance_error,
                chain_value_pools: Box::new(value_pool),
                block_value_pool_change: Box::new(block_value_pool_change),
                height: Some(finalized.height),
            })?;

        // Update value pool metrics for observability (ZIP-209 compliance monitoring)
        value_pool_metrics(&new_value_pool);

        let _ = db
            .chain_value_pools_cf()
            .with_batch_for_writing(self)
            .zs_insert(&(), &new_value_pool);

        // Get the block size to store with the BlockInfo.
        //
        // `Block::zcash_serialized_size` walks the entire block's serialization
        // on a single thread, which is a significant per-block cost on heavy
        // shielded blocks (it re-traverses every transaction).
        // Sum the independent per-transaction sizes. This is byte-count-identical
        // to serializing the block:
        // size = header + CompactSize(tx_count) + sum(transaction sizes).
        // Only fan out to rayon once the block has enough transactions to
        // amortize the fork-join cost; small blocks sum sequentially (see
        // PARALLEL_BLOCK_TX_THRESHOLD).
        let block_size = {
            let transactions = &finalized.block.transactions;
            let transactions_size: usize =
                if transactions.len() >= super::PARALLEL_BLOCK_TX_THRESHOLD {
                    use rayon::prelude::*;
                    transactions
                        .par_iter()
                        .map(|transaction| transaction.zcash_serialized_size())
                        .sum()
                } else {
                    transactions
                        .iter()
                        .map(|transaction| transaction.zcash_serialized_size())
                        .sum()
                };
            let tx_count_size = CompactSizeMessage::try_from(transactions.len())
                .expect("block must have a valid transaction count")
                .zcash_serialized_size();

            finalized.block.header.zcash_serialized_size() + tx_count_size + transactions_size
        };

        let _ = db.block_info_cf().with_batch_for_writing(self).zs_insert(
            &finalized.height,
            &BlockInfo::new(new_value_pool, block_size as u32),
        );

        Ok(new_value_pool)
    }
}
