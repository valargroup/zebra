//! Offline pruning support for finalized raw transaction data.

use semver::Version;
use zebra_chain::{block, parameters::Network};

use crate::{
    config::{database_format_version_on_disk, PruningConfig, StorageMode},
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    service::finalized_state::{
        disk_db::DiskWriteBatch, zebra_db::ZebraDb, STATE_COLUMN_FAMILIES_IN_CODE,
    },
    BoxError, Config,
};

/// Options for pruning finalized raw transaction data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PruneFinalizedStateOptions {
    /// Number of recent finalized blocks below the tip whose raw transaction data is retained.
    pub tx_retention: u32,
}

/// Summary of a finalized-state pruning operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PruneFinalizedStateSummary {
    /// Finalized tip at the time the pruning plan was created.
    pub tip: (block::Height, block::Hash),

    /// Requested raw transaction retention window.
    pub tx_retention: u32,

    /// Lowest retained height before this pruning operation.
    pub previous_lowest_retained_height: Option<block::Height>,

    /// Lowest retained height after this pruning operation.
    pub new_lowest_retained_height: Option<block::Height>,

    /// Half-open height range whose raw transaction data is pruned.
    pub pruned_height_range: Option<(block::Height, block::Height)>,

    /// Number of block heights pruned by this operation.
    pub pruned_height_count: u32,
}

/// Errors returned by finalized-state pruning.
#[derive(Debug, thiserror::Error)]
pub enum PruneFinalizedStateError {
    /// The configured pruning retention is invalid.
    #[error("invalid pruning configuration")]
    InvalidConfig(#[source] BoxError),

    /// The on-disk state database format does not match the running code.
    #[error(
        "state database format mismatch: on disk {on_disk:?}, running code {in_code}; \
         use a Zebra binary with the same state format"
    )]
    FormatMismatch {
        /// Version read from disk.
        on_disk: Option<Version>,
        /// Version implemented by the running code.
        in_code: Version,
    },

    /// The on-disk state database format version file could not be read.
    #[error("could not read the state database format version file")]
    UnreadableFormatVersion(#[source] BoxError),

    /// The state database has no finalized tip.
    #[error("state database has no finalized tip")]
    EmptyDatabase,

    /// The database has already pruned data needed by the requested retention window.
    #[error(
        "database has already been pruned beyond the requested retention: \
         lowest retained height is {current:?}, requested retention needs {requested:?}"
    )]
    AlreadyPrunedBeyondRetention {
        /// Current lowest retained height in the database.
        current: block::Height,
        /// Lowest retained height required by the requested retention, or `None` if no pruning
        /// should have happened yet.
        requested: Option<block::Height>,
    },

    /// RocksDB failed while writing the pruning batch.
    #[error("failed to write pruning batch")]
    RocksDb(#[from] rocksdb::Error),
}

/// Preview finalized-state pruning without mutating the database.
pub fn preview_prune_finalized_state(
    config: Config,
    network: &Network,
    options: PruneFinalizedStateOptions,
) -> Result<PruneFinalizedStateSummary, PruneFinalizedStateError> {
    let config = pruning_config(config, network, options.tx_retention)?;
    check_format_version(&config, network)?;

    let db = open_pruning_db(&config, network, true);
    pruning_summary(&db, &options)
}

/// Prune finalized raw transaction data below the configured retention window.
pub fn prune_finalized_state(
    config: Config,
    network: &Network,
    options: PruneFinalizedStateOptions,
) -> Result<PruneFinalizedStateSummary, PruneFinalizedStateError> {
    let config = pruning_config(config, network, options.tx_retention)?;
    check_format_version(&config, network)?;

    let db = open_pruning_db(&config, network, false);
    let summary = pruning_summary(&db, &options)?;

    if let Some((prune_from, prune_until)) = summary.pruned_height_range {
        let mut batch = DiskWriteBatch::new();
        batch.prepare_prune_batch(&db, prune_from, prune_until);
        db.write_batch(batch)?;
    }

    Ok(summary)
}

fn pruning_config(
    mut config: Config,
    network: &Network,
    tx_retention: u32,
) -> Result<Config, PruneFinalizedStateError> {
    config.storage_mode = StorageMode::Pruned(PruningConfig { tx_retention });
    config
        .validate_storage_mode(network)
        .map_err(PruneFinalizedStateError::InvalidConfig)?;
    Ok(config)
}

fn check_format_version(
    config: &Config,
    network: &Network,
) -> Result<(), PruneFinalizedStateError> {
    let in_code = state_database_format_version_in_code();
    let on_disk =
        database_format_version_on_disk(config, STATE_DATABASE_KIND, in_code.major, network)
            .map_err(PruneFinalizedStateError::UnreadableFormatVersion)?;

    if on_disk.as_ref() != Some(&in_code) {
        return Err(PruneFinalizedStateError::FormatMismatch { on_disk, in_code });
    }

    Ok(())
}

fn open_pruning_db(config: &Config, network: &Network, read_only: bool) -> ZebraDb {
    ZebraDb::new(
        config,
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        network,
        // This utility requires the current format and should not run upgrades while pruning.
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        read_only,
    )
}

fn pruning_summary(
    db: &ZebraDb,
    options: &PruneFinalizedStateOptions,
) -> Result<PruneFinalizedStateSummary, PruneFinalizedStateError> {
    let tip = db.tip().ok_or(PruneFinalizedStateError::EmptyDatabase)?;
    let previous_lowest_retained_height = db.lowest_retained_height();
    let requested_lowest_retained_height =
        lowest_retained_height_for_retention(tip.0, options.tx_retention);

    if let Some(current) = previous_lowest_retained_height {
        if requested_lowest_retained_height.is_none_or(|requested| current > requested) {
            return Err(PruneFinalizedStateError::AlreadyPrunedBeyondRetention {
                current,
                requested: requested_lowest_retained_height,
            });
        }
    }

    let pruned_height_range = prune_height_range_for_retention(tip.0, options.tx_retention);
    let new_lowest_retained_height =
        pruned_height_range.map_or(previous_lowest_retained_height, |(_, until)| Some(until));
    let pruned_height_count = pruned_height_range.map_or(0, |(from, until)| until.0 - from.0);

    Ok(PruneFinalizedStateSummary {
        tip,
        tx_retention: options.tx_retention,
        previous_lowest_retained_height,
        new_lowest_retained_height,
        pruned_height_range,
        pruned_height_count,
    })
}

fn lowest_retained_height_for_retention(
    tip: block::Height,
    retention: u32,
) -> Option<block::Height> {
    let max_prunable = tip.0.checked_sub(retention)?;

    if max_prunable == 0 {
        return None;
    }

    Some(block::Height(max_prunable + 1))
}

/// Returns the half-open range `[from, until)` of block heights the offline tool
/// prunes for `retention`, or `None` if the retention window covers the whole
/// chain.
///
/// Unlike online pruning (which, when first enabled on an existing archive
/// database, starts at the current retention boundary), the offline tool always
/// covers the full range below the boundary, starting just above genesis. This
/// reclaims any historical raw transaction data left intact when pruning was
/// first enabled online, which the online path and a marker-resuming offline pass
/// would otherwise leave stranded forever. Re-deleting already-pruned heights is
/// an idempotent no-op (a single range tombstone over absent keys).
fn prune_height_range_for_retention(
    tip: block::Height,
    retention: u32,
) -> Option<(block::Height, block::Height)> {
    let prune_until = lowest_retained_height_for_retention(tip, retention)?;
    // Genesis (height 0) is never pruned, so the lowest prunable height is 1.
    let prune_from = block::Height(1);

    if prune_from >= prune_until {
        return None;
    }

    Some((prune_from, prune_until))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use zebra_chain::{
        block::Block, parameters::Network::Mainnet, serialization::ZcashDeserializeInto,
    };

    use crate::service::finalized_state::FinalizedState;

    /// The number of leading blocks committed by the database-backed prune tests.
    const TEST_BLOCKS: u32 = 9;

    /// Opens a fresh finalized state and commits blocks `0..=TEST_BLOCKS`.
    fn new_state_with_blocks() -> FinalizedState {
        let mut state = FinalizedState::new(
            &Config::ephemeral(),
            &Mainnet,
            #[cfg(feature = "elasticsearch")]
            false,
        );

        let blocks = Mainnet.blockchain_map();
        for height in 0..=TEST_BLOCKS {
            let block: Arc<Block> = blocks
                .get(&height)
                .expect("block height has test data")
                .zcash_deserialize_into()
                .expect("test data deserializes");

            state
                .commit_finalized_direct(block.into(), None, "offline prune tests")
                .expect("test block is valid");
        }

        state
    }

    /// Returns the coinbase transaction hash of the mainnet block at `height`.
    fn coinbase_tx_hash(height: u32) -> zebra_chain::transaction::Hash {
        let block: Arc<Block> = Mainnet
            .blockchain_map()
            .get(&height)
            .expect("block height has test data")
            .zcash_deserialize_into()
            .expect("test data deserializes");

        block.transactions[0].hash()
    }

    #[test]
    fn retention_range_keeps_genesis_and_recent_window() {
        assert_eq!(
            lowest_retained_height_for_retention(block::Height(100), 5000),
            None
        );
        assert_eq!(
            lowest_retained_height_for_retention(block::Height(5000), 5000),
            None
        );
        assert_eq!(
            lowest_retained_height_for_retention(block::Height(5001), 5000),
            Some(block::Height(2))
        );

        // The offline tool always covers the full range below the boundary,
        // starting just above genesis, regardless of any existing marker.
        assert_eq!(
            prune_height_range_for_retention(block::Height(5001), 5000),
            Some((block::Height(1), block::Height(2)))
        );
        assert_eq!(
            prune_height_range_for_retention(block::Height(10_000), 5000),
            Some((block::Height(1), block::Height(5001)))
        );

        // Retention covering the whole chain leaves nothing to prune.
        assert_eq!(
            prune_height_range_for_retention(block::Height(5000), 5000),
            None
        );
    }

    #[test]
    fn pruning_summary_plans_full_offline_range() {
        let _init_guard = zebra_test::init();
        let state = new_state_with_blocks();

        let summary = pruning_summary(&state.db, &PruneFinalizedStateOptions { tx_retention: 5 })
            .expect("summary should be available");

        assert_eq!(summary.tip.0, block::Height(TEST_BLOCKS));
        assert_eq!(summary.previous_lowest_retained_height, None);
        assert_eq!(
            summary.new_lowest_retained_height,
            Some(block::Height(5)),
            "height 5 is the first retained non-genesis height"
        );
        assert_eq!(
            summary.pruned_height_range,
            Some((block::Height(1), block::Height(5))),
            "offline pruning plans the entire eligible range in one batch"
        );
        assert_eq!(summary.pruned_height_count, 4);
    }

    #[test]
    fn pruning_summary_reclaims_full_range_below_existing_marker() {
        let _init_guard = zebra_test::init();
        let state = new_state_with_blocks();

        // Simulate online pruning having advanced the marker to height 3 while
        // leaving height 1 intact below it — the state left behind when pruning is
        // first enabled on an existing archive database.
        let mut batch = DiskWriteBatch::new();
        batch.prepare_prune_batch(&state.db, block::Height(2), block::Height(3));
        state.db.write_batch(batch).expect("prune batch writes");
        assert_eq!(state.db.lowest_retained_height(), Some(block::Height(3)));
        assert!(
            state.db.transaction(coinbase_tx_hash(1)).is_some(),
            "height 1 was left intact below the online pruning marker"
        );

        let summary = pruning_summary(&state.db, &PruneFinalizedStateOptions { tx_retention: 5 })
            .expect("summary should plan the full range below the boundary");

        assert_eq!(
            summary.previous_lowest_retained_height,
            Some(block::Height(3))
        );
        assert_eq!(
            summary.pruned_height_range,
            Some((block::Height(1), block::Height(5))),
            "offline pruning reclaims the full range below the boundary, including \
             heights left intact below the marker"
        );
        assert_eq!(summary.pruned_height_count, 4);
    }

    #[test]
    fn pruning_summary_refuses_already_pruned_beyond_requested_retention() {
        let _init_guard = zebra_test::init();
        let state = new_state_with_blocks();

        let mut batch = DiskWriteBatch::new();
        batch.prepare_prune_batch(&state.db, block::Height(1), block::Height(5));
        state.db.write_batch(batch).expect("prune batch writes");

        let error = pruning_summary(&state.db, &PruneFinalizedStateOptions { tx_retention: 6 })
            .expect_err("missing transaction data cannot be restored");

        assert!(matches!(
            error,
            PruneFinalizedStateError::AlreadyPrunedBeyondRetention {
                current: block::Height(5),
                requested: Some(block::Height(4)),
            }
        ));
    }

    #[test]
    fn pruning_config_rejects_retention_below_floor() {
        let error = pruning_config(Config::ephemeral(), &Mainnet, 1)
            .expect_err("retention below MIN_PRUNING_RETENTION is invalid");

        assert!(matches!(error, PruneFinalizedStateError::InvalidConfig(_)));
    }
}
