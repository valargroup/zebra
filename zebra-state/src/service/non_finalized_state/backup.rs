use std::{
    collections::{BTreeMap, HashMap},
    fs::DirEntry,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use hex::ToHex;
use zebra_chain::{
    amount::{Amount, DeferredPoolBalanceChange},
    block::{self, Block, Height},
    parameters::{Network, NetworkUpgrade},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};

use crate::{
    ContextuallyVerifiedBlock, IntoDisk, NonFinalizedState, SemanticallyVerifiedBlock,
    WatchReceiver, ZebraDb,
};

#[cfg(not(test))]
use crate::service::write::validate_and_commit_non_finalized;

/// The minimum duration that Zebra will wait between updates to the non-finalized state backup cache.
pub(crate) const MIN_DURATION_BETWEEN_BACKUP_UPDATES: Duration = Duration::from_secs(5);

/// The file name for cached consensus parameters used to validate backup blocks.
const BACKUP_ACTIVATION_HEIGHTS_FILE_NAME: &str = "activation-heights";

/// Accepts an optional path to the non-finalized state backup directory and a handle to the database.
///
/// Looks for blocks above the finalized tip height in the backup directory (if a path was provided) and
/// attempts to commit them to the non-finalized state.
///
/// Returns the resulting non-finalized state.
pub(super) fn restore_backup(
    mut non_finalized_state: NonFinalizedState,
    backup_dir_path: &Path,
    finalized_state: &ZebraDb,
) -> NonFinalizedState {
    let mut store: BTreeMap<Height, Vec<SemanticallyVerifiedBlock>> = BTreeMap::new();

    for block in read_non_finalized_blocks_from_backup(backup_dir_path, finalized_state) {
        store.entry(block.height).or_default().push(block);
    }

    if let Some(rewind_height) =
        backup_rewind_height_for_changed_activation(backup_dir_path, finalized_state, &store)
    {
        tracing::warn!(
            ?backup_dir_path,
            ?rewind_height,
            "rewinding non-finalized backup cache because it reaches a network \
             upgrade activation height with unknown or changed activation parameters"
        );

        store.retain(|height, _blocks| *height < rewind_height);
    }

    for (height, blocks) in store {
        for block in blocks {
            #[cfg(test)]
            let commit_result = if non_finalized_state
                .any_chain_contains(&block.block.header.previous_block_hash)
            {
                non_finalized_state.commit_block(block, finalized_state)
            } else {
                non_finalized_state.commit_new_chain(block, finalized_state)
            };

            #[cfg(not(test))]
            let commit_result =
                validate_and_commit_non_finalized(finalized_state, &mut non_finalized_state, block);

            // Re-computes the block hash in case the hash from the filename is wrong.
            if let Err(commit_error) = commit_result {
                tracing::warn!(
                    ?commit_error,
                    ?height,
                    "failed to commit non-finalized block from backup directory"
                );
            }
        }
    }

    non_finalized_state
}

/// Updates the non-finalized state backup cache by writing any blocks that are in the
/// non-finalized state but missing in the backup cache, and deleting any backup files
/// that are no longer present in the non-finalized state.
///
/// `backup_blocks` should be the current contents of the backup directory, obtained by
/// calling [`list_backup_dir_entries`] before the non-finalized state was updated.
///
/// This function performs blocking I/O and should be called from a blocking context,
/// or wrapped in [`tokio::task::spawn_blocking`].
pub(super) fn update_non_finalized_state_backup(
    backup_dir_path: &Path,
    non_finalized_state: &NonFinalizedState,
    mut backup_blocks: HashMap<block::Hash, PathBuf>,
) {
    write_backup_activation_heights(backup_dir_path, &non_finalized_state.network);

    for block in non_finalized_state
        .chain_iter()
        .flat_map(|chain| chain.blocks.values())
        // Remove blocks from `backup_blocks` that are present in the non-finalized state
        .filter(|block| backup_blocks.remove(&block.hash).is_none())
    {
        // This loop will typically iterate only once, but may write multiple blocks if it misses
        // some non-finalized state changes while waiting for I/O ops.
        write_backup_block(backup_dir_path, block);
    }

    // Remove any backup blocks that are not present in the non-finalized state
    for (_, outdated_backup_block_path) in backup_blocks {
        if let Err(delete_error) = std::fs::remove_file(outdated_backup_block_path) {
            tracing::warn!(?delete_error, "failed to delete backup block file");
        }
    }
}

/// Updates the non-finalized state backup cache whenever the non-finalized state changes,
/// deleting any outdated backup files and writing any blocks that are in the non-finalized
/// state but missing in the backup cache.
pub(super) async fn run_backup_task(
    mut non_finalized_state_receiver: WatchReceiver<NonFinalizedState>,
    backup_dir_path: PathBuf,
) {
    let err = loop {
        let rate_limit = tokio::time::sleep(MIN_DURATION_BETWEEN_BACKUP_UPDATES);
        let backup_blocks: HashMap<block::Hash, PathBuf> = {
            let backup_dir_path = backup_dir_path.clone();
            tokio::task::spawn_blocking(move || list_backup_dir_entries(&backup_dir_path))
                .await
                .expect("failed to join blocking task when reading in backup task")
                .collect()
        };

        if let (Err(err), _) = tokio::join!(non_finalized_state_receiver.changed(), rate_limit) {
            break err;
        };

        let latest_non_finalized_state = non_finalized_state_receiver.cloned_watch_data();

        let backup_dir_path = backup_dir_path.clone();
        tokio::task::spawn_blocking(move || {
            update_non_finalized_state_backup(
                &backup_dir_path,
                &latest_non_finalized_state,
                backup_blocks,
            );
        })
        .await
        .expect("failed to join blocking task when writing in backup task");
    };

    tracing::warn!(
        ?err,
        "got recv error waiting on non-finalized state change, is Zebra shutting down?"
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NonFinalizedBlockBackup {
    block: Arc<Block>,
    deferred_pool_balance_change: Amount,
}

impl From<&ContextuallyVerifiedBlock> for NonFinalizedBlockBackup {
    fn from(cv_block: &ContextuallyVerifiedBlock) -> Self {
        Self {
            block: cv_block.block.clone(),
            deferred_pool_balance_change: cv_block.chain_value_pool_change.deferred_amount(),
        }
    }
}

impl NonFinalizedBlockBackup {
    /// Encodes a [`NonFinalizedBlockBackup`] as a vector of bytes.
    fn as_bytes(&self) -> Vec<u8> {
        let block_bytes = self
            .block
            .zcash_serialize_to_vec()
            .expect("verified block header version should be valid");

        let deferred_pool_balance_change_bytes =
            self.deferred_pool_balance_change.as_bytes().to_vec();

        [deferred_pool_balance_change_bytes, block_bytes].concat()
    }

    /// Constructs a new [`NonFinalizedBlockBackup`] from a vector of bytes.
    #[allow(clippy::unwrap_in_result)]
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, io::Error> {
        let (deferred_pool_balance_change_bytes, block_bytes) = bytes
            .split_at_checked(size_of::<Amount>())
            .ok_or(io::Error::new(
                ErrorKind::InvalidInput,
                "input is too short",
            ))?;

        Ok(Self {
            block: Arc::new(
                block_bytes
                    .zcash_deserialize_into()
                    .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?,
            ),
            deferred_pool_balance_change: Amount::from_bytes(
                deferred_pool_balance_change_bytes
                    .try_into()
                    .expect("slice from `split_at_checked()` should fit in [u8; 8]"),
            )
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?,
        })
    }
}

/// Writes a block to a file in the provided non-finalized state backup cache directory path.
fn write_backup_block(backup_dir_path: &Path, block: &ContextuallyVerifiedBlock) {
    let backup_block_file_name: String = block.hash.encode_hex();
    let backup_block_file_path = backup_dir_path.join(backup_block_file_name);
    let non_finalized_block_backup: NonFinalizedBlockBackup = block.into();

    if let Err(err) = std::fs::write(
        backup_block_file_path,
        non_finalized_block_backup.as_bytes(),
    ) {
        tracing::warn!(?err, "failed to write non-finalized state backup block");
    }
}

/// Returns the first height where backup blocks might have been validated using
/// a different activation schedule from the current [`Network`].
fn backup_rewind_height_for_changed_activation(
    backup_dir_path: &Path,
    finalized_state: &ZebraDb,
    backup_blocks: &BTreeMap<Height, Vec<SemanticallyVerifiedBlock>>,
) -> Option<Height> {
    let network = finalized_state.network();

    if read_backup_activation_heights(backup_dir_path).as_deref()
        == Some(serialized_activation_heights(&network).as_str())
    {
        return None;
    }

    let Some(first_backup_height) = backup_blocks.keys().next().copied() else {
        return None;
    };
    let Some(last_backup_height) = backup_blocks.keys().next_back().copied() else {
        return None;
    };

    network
        .full_activation_list()
        .into_iter()
        .find_map(|(activation_height, _network_upgrade)| {
            (first_backup_height <= activation_height && activation_height <= last_backup_height)
                .then_some(activation_height)
        })
}

/// Writes the activation schedule used to validate backup blocks.
fn write_backup_activation_heights(backup_dir_path: &Path, network: &Network) {
    let activation_heights_file_path = backup_dir_path.join(BACKUP_ACTIVATION_HEIGHTS_FILE_NAME);

    if let Err(err) = std::fs::write(
        activation_heights_file_path,
        serialized_activation_heights(network),
    ) {
        tracing::warn!(
            ?err,
            "failed to write non-finalized state backup activation heights"
        );
    }
}

/// Reads the activation schedule that was used to validate backup blocks.
fn read_backup_activation_heights(backup_dir_path: &Path) -> Option<String> {
    let activation_heights_file_path = backup_dir_path.join(BACKUP_ACTIVATION_HEIGHTS_FILE_NAME);

    match std::fs::read_to_string(activation_heights_file_path) {
        Ok(activation_heights) => Some(activation_heights),
        Err(err) if err.kind() == ErrorKind::NotFound => None,
        Err(err) => {
            tracing::warn!(
                ?err,
                "failed to read non-finalized state backup activation heights"
            );

            None
        }
    }
}

/// Serializes activation heights in a stable text format.
fn serialized_activation_heights(network: &Network) -> String {
    network
        .full_activation_list()
        .into_iter()
        .map(|(height, network_upgrade)| {
            format!(
                "{}:{}\n",
                height.0,
                serialized_network_upgrade(network_upgrade)
            )
        })
        .collect()
}

/// Returns a stable text name for a [`NetworkUpgrade`].
fn serialized_network_upgrade(network_upgrade: NetworkUpgrade) -> &'static str {
    match network_upgrade {
        NetworkUpgrade::Genesis => "Genesis",
        NetworkUpgrade::BeforeOverwinter => "BeforeOverwinter",
        NetworkUpgrade::Overwinter => "Overwinter",
        NetworkUpgrade::Sapling => "Sapling",
        NetworkUpgrade::Blossom => "Blossom",
        NetworkUpgrade::Heartwood => "Heartwood",
        NetworkUpgrade::Canopy => "Canopy",
        NetworkUpgrade::Nu5 => "Nu5",
        NetworkUpgrade::Nu6 => "Nu6",
        NetworkUpgrade::Nu6_1 => "Nu6_1",
        NetworkUpgrade::Nu6_2 => "Nu6_2",
        NetworkUpgrade::Nu7 => "Nu7",

        #[cfg(zcash_unstable = "zfuture")]
        NetworkUpgrade::ZFuture => "ZFuture",
    }
}

/// Reads blocks from the provided non-finalized state backup directory path.
///
/// Returns any blocks that are valid and not present in the finalized state.
fn read_non_finalized_blocks_from_backup<'a>(
    backup_dir_path: &Path,
    finalized_state: &'a ZebraDb,
) -> impl Iterator<Item = SemanticallyVerifiedBlock> + 'a {
    list_backup_dir_entries(backup_dir_path)
        // It's okay to leave the file here, the backup task will delete it as long as
        // the block is not added to the non-finalized state.
        .filter(|&(block_hash, _)| !finalized_state.contains_hash(block_hash))
        .filter_map(|(block_hash, file_path)| match std::fs::read(file_path) {
            Ok(block_bytes) => Some((block_hash, block_bytes)),
            Err(err) => {
                tracing::warn!(?err, "failed to open non-finalized state backup block file");
                None
            }
        })
        .filter_map(|(expected_block_hash, backup_block_file_contents)| {
            match NonFinalizedBlockBackup::from_bytes(backup_block_file_contents) {
                Ok(NonFinalizedBlockBackup {
                    block,
                    deferred_pool_balance_change,
                }) if block.coinbase_height().is_some() => {
                    let block = SemanticallyVerifiedBlock::from(block)
                        .with_deferred_pool_balance_change(Some(DeferredPoolBalanceChange::new(
                            deferred_pool_balance_change,
                        )));
                    if block.hash != expected_block_hash {
                        tracing::warn!(
                            block_hash = ?block.hash,
                            ?expected_block_hash,
                            "wrong block hash in file name"
                        );
                    }
                    Some(block)
                }
                Ok(block) => {
                    tracing::warn!(
                        ?block,
                        "invalid non-finalized backup block, missing coinbase height"
                    );
                    None
                }
                Err(err) => {
                    tracing::warn!(
                        ?err,
                        "failed to deserialize non-finalized backup data into block"
                    );
                    None
                }
            }
        })
}

/// Accepts a backup directory path, opens the directory, converts its entries
/// filenames to block hashes, and deletes any entries with invalid file names.
///
/// # Panics
///
/// If the provided path cannot be opened as a directory.
/// See [`read_backup_dir`] for more details.
pub(super) fn list_backup_dir_entries(
    backup_dir_path: &Path,
) -> impl Iterator<Item = (block::Hash, PathBuf)> {
    read_backup_dir(backup_dir_path).filter_map(process_backup_dir_entry)
}

/// Accepts a backup directory path and opens the directory.
///
/// Returns an iterator over all [`DirEntry`]s in the directory that are successfully read.
///
/// # Panics
///
/// If the provided path cannot be opened as a directory.
fn read_backup_dir(backup_dir_path: &Path) -> impl Iterator<Item = DirEntry> {
    std::fs::read_dir(backup_dir_path)
        .expect("failed to read non-finalized state backup directory")
        .filter_map(|entry| match entry {
            Ok(entry) => Some(entry),
            Err(io_err) => {
                tracing::warn!(
                    ?io_err,
                    "failed to read DirEntry in non-finalized state backup dir"
                );

                None
            }
        })
}

/// Accepts a [`DirEntry`] from the non-finalized state backup directory and
/// parses the filename into a block hash.
///
/// Returns the block hash and the file path if successful, or
/// returns None and deletes the file at the entry path otherwise.
fn process_backup_dir_entry(entry: DirEntry) -> Option<(block::Hash, PathBuf)> {
    let delete_file = || {
        if let Err(delete_error) = std::fs::remove_file(entry.path()) {
            tracing::warn!(?delete_error, "failed to delete backup block file");
        }
    };

    let block_file_name = match entry.file_name().into_string() {
        Ok(block_hash) => block_hash,
        Err(err) => {
            tracing::warn!(
                ?err,
                "failed to convert OsString to String, attempting to delete file"
            );

            delete_file();
            return None;
        }
    };

    if block_file_name == BACKUP_ACTIVATION_HEIGHTS_FILE_NAME {
        return None;
    }

    let block_hash: block::Hash = match block_file_name.parse() {
        Ok(block_hash) => block_hash,
        Err(err) => {
            tracing::warn!(
                ?err,
                "failed to parse hex-encoded block hash from file name, attempting to delete file"
            );

            delete_file();
            return None;
        }
    };

    Some((block_hash, entry.path()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        arbitrary::Prepare, service::finalized_state::FinalizedState, tests::FakeChainHelper,
        Config,
    };

    #[test]
    fn restore_backup_rewinds_stale_cache_at_activation_height() {
        let network = Network::Mainnet;
        let finalized_state = FinalizedState::new(
            &Config::ephemeral(),
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        );
        let backup_dir = tempfile::tempdir().expect("temporary directory is created");

        let heartwood_activation = NetworkUpgrade::Heartwood
            .activation_height(&network)
            .expect("Heartwood activates on mainnet");
        let before_heartwood = (heartwood_activation - 1).expect("activation is above genesis");

        let blocks = network.block_map();
        let prev_block = Arc::new(
            blocks
                .get(&before_heartwood.0)
                .expect("test vector exists")
                .zcash_deserialize_into::<Block>()
                .expect("block is structurally valid"),
        );
        let stale_activation_block = prev_block.make_fake_child();
        let stale_block_1 = stale_activation_block.make_fake_child();
        let stale_block_2 = stale_block_1.make_fake_child();

        let mut stale_non_finalized_state = NonFinalizedState::new(&network);
        stale_non_finalized_state
            .commit_new_chain(prev_block.clone().prepare(), &finalized_state.db)
            .expect("pre-activation block should commit to the test state");

        update_non_finalized_state_backup(
            backup_dir.path(),
            &stale_non_finalized_state,
            HashMap::new(),
        );
        write_stale_backup_block(backup_dir.path(), stale_activation_block);
        write_stale_backup_block(backup_dir.path(), stale_block_1);
        write_stale_backup_block(backup_dir.path(), stale_block_2);

        std::fs::remove_file(backup_dir.path().join(BACKUP_ACTIVATION_HEIGHTS_FILE_NAME))
            .expect("simulating an old backup cache should remove activation metadata");

        let restored_non_finalized_state = restore_backup(
            NonFinalizedState::new(&network),
            backup_dir.path(),
            &finalized_state.db,
        );

        assert_eq!(
            restored_non_finalized_state.best_chain_len(),
            Some(1),
            "restore should keep only pre-activation backup blocks"
        );
        assert_eq!(
            restored_non_finalized_state.best_tip(),
            Some((before_heartwood, prev_block.hash())),
            "restore should rewind the non-finalized backup to before activation"
        );
    }

    fn write_stale_backup_block(backup_dir_path: &Path, block: Arc<Block>) {
        let backup_block_file_name: String = block.hash().encode_hex();
        let backup_block_file_path = backup_dir_path.join(backup_block_file_name);
        let backup_block = NonFinalizedBlockBackup {
            block,
            deferred_pool_balance_change: Amount::zero(),
        };

        std::fs::write(backup_block_file_path, backup_block.as_bytes())
            .expect("test should write stale backup block");
    }

    #[test]
    fn backup_activation_metadata_rewinds_crossed_activation() {
        let network = Network::Mainnet;
        let finalized_state = FinalizedState::new(
            &Config::ephemeral(),
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        );
        let backup_dir = tempfile::tempdir().expect("temporary directory is created");

        let heartwood_activation = NetworkUpgrade::Heartwood
            .activation_height(&network)
            .expect("Heartwood activates on mainnet");
        let before_heartwood = (heartwood_activation - 1).expect("activation is above genesis");
        let after_heartwood = (heartwood_activation + 1).expect("activation is below max height");

        let mut backup_blocks = BTreeMap::new();
        backup_blocks.insert(before_heartwood, Vec::new());
        backup_blocks.insert(after_heartwood, Vec::new());

        assert_eq!(
            backup_rewind_height_for_changed_activation(
                backup_dir.path(),
                &finalized_state.db,
                &backup_blocks
            ),
            Some(heartwood_activation),
            "missing metadata should reject backup blocks that cross an activation height"
        );

        write_backup_activation_heights(backup_dir.path(), &network);

        assert_eq!(
            backup_rewind_height_for_changed_activation(
                backup_dir.path(),
                &finalized_state.db,
                &backup_blocks
            ),
            None,
            "matching metadata should allow backup blocks that cross an activation height"
        );
    }

    #[test]
    fn backup_activation_metadata_allows_single_upgrade_range() {
        let network = Network::Mainnet;
        let finalized_state = FinalizedState::new(
            &Config::ephemeral(),
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        );
        let backup_dir = tempfile::tempdir().expect("temporary directory is created");

        let heartwood_activation = NetworkUpgrade::Heartwood
            .activation_height(&network)
            .expect("Heartwood activates on mainnet");
        let before_heartwood = (heartwood_activation - 2).expect("activation is above genesis");
        let last_before_heartwood =
            (heartwood_activation - 1).expect("activation is above genesis");

        let mut backup_blocks = BTreeMap::new();
        backup_blocks.insert(before_heartwood, Vec::new());
        backup_blocks.insert(last_before_heartwood, Vec::new());

        assert_eq!(
            backup_rewind_height_for_changed_activation(
                backup_dir.path(),
                &finalized_state.db,
                &backup_blocks
            ),
            None,
            "missing metadata should allow backup blocks that do not cross an activation height"
        );
    }
}
