//! Provides high-level access to database [`Block`]s and [`Transaction`]s.
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
    collections::{BTreeMap, HashMap, HashSet},
    ops::RangeBounds,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use itertools::Itertools;

use zebra_chain::{
    amount::NonNegative,
    block::{self, Block, Height},
    orchard,
    parallel::{commitment_aux::BlockCommitmentRoots, tree::NoteCommitmentTrees},
    parameters::{Network, GENESIS_PREVIOUS_BLOCK_HASH},
    sapling,
    serialization::{CompactSizeMessage, TrustedPreallocate, ZcashSerialize as _},
    transaction::{self, Transaction},
    transparent,
    value_balance::ValueBalance,
    work::difficulty::PartialCumulativeWork,
};

use crate::{
    constants::{
        MAX_BLOCK_REORG_HEIGHT, MAX_HEADER_SYNC_HEIGHT_RANGE, MAX_PRUNE_HEIGHTS_PER_COMMIT,
    },
    error::{CommitCheckpointVerifiedError, CommitHeaderRangeError},
    request::{AuthenticatedCheckpointHash, FinalizedBlock},
    service::check,
    service::finalized_state::{
        disk_db::{DiskDb, DiskWriteBatch, ReadDisk, WriteDisk},
        disk_format::{
            block::TransactionLocation,
            shielded::CommitmentRootsByHeight,
            transparent::{AddressBalanceLocationUpdates, OutputLocation},
        },
        zebra_db::{metrics::block_precommit_metrics, ZebraDb},
        FromDisk, IntoDisk, RawBytes, PRUNING_METADATA, VCT_SYNC_METADATA, VCT_UPGRADE_METADATA,
        ZAKURA_HEADER_COMMITMENT_ROOTS_BY_HEIGHT,
    },
    HashOrHeight,
};

#[cfg(feature = "indexer")]
use crate::request::Spend;

use super::super::pipeline::{BlockBatchOutputs, FinalizedPipeline, PipelineBatchContribution};

#[cfg(test)]
mod tests;

const ZAKURA_HEADER_HASH_BY_HEIGHT: &str = "zakura_header_hash_by_height";
const ZAKURA_HEADER_HEIGHT_BY_HASH: &str = "zakura_header_height_by_hash";
const ZAKURA_HEADER_BY_HEIGHT: &str = "zakura_header_by_height";
pub const ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT: &str = "zakura_header_body_size_by_height";

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct AdvertisedBodySize(u32);

impl AdvertisedBodySize {
    fn new(size: u32) -> Option<Self> {
        (size != 0).then_some(Self(size))
    }

    fn get(self) -> u32 {
        self.0
    }
}

impl IntoDisk for AdvertisedBodySize {
    type Bytes = [u8; 4];

    fn as_bytes(&self) -> Self::Bytes {
        self.0.to_be_bytes()
    }
}

impl FromDisk for AdvertisedBodySize {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes
            .as_ref()
            .try_into()
            .expect("advertised body sizes are stored as u32");
        Self(u32::from_be_bytes(bytes))
    }
}

impl ZebraDb {
    // Read block methods

    /// Returns true if the database is empty.
    //
    // TODO: move this method to the tip section
    pub fn is_empty(&self) -> bool {
        self.tip().is_none()
    }

    /// Returns the tip height and hash, if there is one.
    //
    // TODO: rename to finalized_tip()
    //       move this method to the tip section
    #[allow(clippy::unwrap_in_result)]
    pub fn tip(&self) -> Option<(block::Height, block::Hash)> {
        // The finalized tip is the highest full block committed to the consensus
        // `hash_by_height` column family. That CF is written only when a full
        // block body is committed (header-only Zakura frontier heights live in
        // the separate `zakura_header_*` CFs), and it is retained through
        // pruning. Reading it here — instead of the body tip via `tx_by_loc` —
        // keeps `tip()` correct in pruned storage mode, where checkpoint blocks
        // below the prune horizon have their `tx_by_loc` rows skipped/removed but
        // their `hash_by_height` row retained. In non-pruned mode the two are
        // identical, since every full block writes both rows.
        let hash_by_height = self.db.cf_handle("hash_by_height").unwrap();
        self.db.zs_last_key_value(&hash_by_height)
    }

    /// Returns `true` if `height` is present in the finalized state.
    #[allow(clippy::unwrap_in_result)]
    pub fn contains_height(&self, height: block::Height) -> bool {
        let hash_by_height = self.db.cf_handle("hash_by_height").unwrap();

        self.db.zs_contains(&hash_by_height, &height)
    }

    /// Returns `true` if a full block body is present and serveable at `height`.
    ///
    /// Valid Zcash blocks always have at least a coinbase transaction, so the
    /// presence of the first transaction location is the body-availability
    /// marker. A `false` result means the body is not serveable: either no body
    /// has been committed yet (a header-only frontier height above the body tip),
    /// or the body was committed and later pruned (pruning removes `tx_by_loc`
    /// rows but retains the header). This is a body predicate, not a
    /// "header-only" marker.
    #[allow(clippy::unwrap_in_result)]
    pub fn contains_body_at_height(&self, height: block::Height) -> bool {
        let tx_by_loc = self.db.cf_handle("tx_by_loc").unwrap();
        let first_tx = TransactionLocation::min_for_height(height);

        self.db.zs_contains(&tx_by_loc, &first_tx)
    }

    /// Returns the advisory body-size hint for a header-only height, if known.
    ///
    /// `None` means the peer supplied the `0` unknown sentinel or no hint has been
    /// stored. This value is not consensus data.
    #[allow(clippy::unwrap_in_result)]
    pub fn advertised_body_size(&self, height: block::Height) -> Option<u32> {
        let body_size_by_height = self
            .db
            .cf_handle(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
            .unwrap();

        self.db
            .zs_get(&body_size_by_height, &height)
            .map(AdvertisedBodySize::get)
    }

    /// Returns the finalized hash for a given `block::Height` if it is present.
    #[allow(clippy::unwrap_in_result)]
    pub fn hash(&self, height: block::Height) -> Option<block::Hash> {
        let hash_by_height = self.db.cf_handle("hash_by_height").unwrap();
        self.db.zs_get(&hash_by_height, &height)
    }

    /// Returns the hash for `height` only if a full block body is present.
    pub fn body_hash(&self, height: block::Height) -> Option<block::Hash> {
        self.contains_body_at_height(height)
            .then(|| self.hash(height))
            .flatten()
    }

    /// Returns `true` if `hash` is present in the finalized state.
    #[allow(clippy::unwrap_in_result)]
    pub fn contains_hash(&self, hash: block::Hash) -> bool {
        self.height(hash)
            .is_some_and(|height| self.contains_body_at_height(height))
    }

    /// Returns the height of the given block if it exists.
    #[allow(clippy::unwrap_in_result)]
    pub fn height(&self, hash: block::Hash) -> Option<block::Height> {
        let height_by_hash = self.db.cf_handle("height_by_hash").unwrap();
        self.db.zs_get(&height_by_hash, &hash)
    }

    /// Returns the previous block hash for the given block hash in the finalized state.
    #[allow(dead_code)]
    pub fn prev_block_hash_for_hash(&self, hash: block::Hash) -> Option<block::Hash> {
        let height = self.height(hash)?;
        let prev_height = height.previous().ok()?;

        self.hash(prev_height)
    }

    /// Returns the previous block height for the given block hash in the finalized state.
    #[allow(dead_code)]
    pub fn prev_block_height_for_hash(&self, hash: block::Hash) -> Option<block::Height> {
        let height = self.height(hash)?;

        height.previous().ok()
    }

    /// Returns the [`block::Header`] with [`block::Hash`] or
    /// [`Height`], if it exists in the finalized chain.
    //
    // TODO: move this method to the start of the section
    #[allow(clippy::unwrap_in_result)]
    pub fn block_header(&self, hash_or_height: HashOrHeight) -> Option<Arc<block::Header>> {
        // Block Header
        let block_header_by_height = self.db.cf_handle("block_header_by_height").unwrap();

        let height = hash_or_height.height_or_else(|hash| self.height(hash))?;
        let header = self.db.zs_get(&block_header_by_height, &height)?;

        Some(header)
    }

    /// Returns the raw [`block::Header`] with [`block::Hash`] or [`Height`], if
    /// it exists in the finalized chain.
    #[allow(clippy::unwrap_in_result)]
    fn raw_block_header(&self, hash_or_height: HashOrHeight) -> Option<RawBytes> {
        // Block Header
        let block_header_by_height = self.db.cf_handle("block_header_by_height").unwrap();

        let height = hash_or_height.height_or_else(|hash| self.height(hash))?;
        let header: RawBytes = self.db.zs_get(&block_header_by_height, &height)?;

        Some(header)
    }

    /// Returns the [`Block`] with [`block::Hash`] or
    /// [`Height`], if it exists in the finalized chain and its raw transaction
    /// data is available.
    ///
    /// Returns `None` if the block does not exist, or if its transaction bodies
    /// are missing because they have been pruned.
    //
    // TODO: move this method to the start of the section
    #[allow(clippy::unwrap_in_result)]
    pub fn block(&self, hash_or_height: HashOrHeight) -> Option<Arc<Block>> {
        let (raw_header, raw_txs) = self.raw_block(hash_or_height)?;

        let header = Arc::<block::Header>::from_bytes(raw_header.raw_bytes());
        let transactions = raw_txs
            .iter()
            .map(|raw_tx| Arc::<Transaction>::from_bytes(raw_tx.raw_bytes()))
            .collect();

        Some(Arc::new(Block {
            header,
            transactions,
        }))
    }

    /// Returns the [`Block`] with [`block::Hash`] or [`Height`], if it exists
    /// in the finalized chain, and its serialized size, if its raw transaction
    /// data is available.
    ///
    /// Returns `None` if the block does not exist, or if its transaction bodies
    /// are missing because they have been pruned.
    #[allow(clippy::unwrap_in_result)]
    pub fn block_and_size(&self, hash_or_height: HashOrHeight) -> Option<(Arc<Block>, usize)> {
        let (raw_header, raw_txs) = self.raw_block(hash_or_height)?;

        let header = Arc::<block::Header>::from_bytes(raw_header.raw_bytes());
        let txs: Vec<_> = raw_txs
            .iter()
            .map(|raw_tx| Arc::<Transaction>::from_bytes(raw_tx.raw_bytes()))
            .collect();

        // Compute the size of the block from the size of header and size of
        // transactions. This requires summing them all and also adding the
        // size of the CompactSize-encoded transaction count.
        // See https://developer.bitcoin.org/reference/block_chain.html#serialized-blocks
        let tx_count = CompactSizeMessage::try_from(txs.len())
            .expect("must work for a previously serialized block");
        let tx_raw = tx_count
            .zcash_serialize_to_vec()
            .expect("must work for a previously serialized block");
        let size = raw_header.raw_bytes().len()
            + raw_txs
                .iter()
                .map(|raw_tx| raw_tx.raw_bytes().len())
                .sum::<usize>()
            + tx_raw.len();

        let block = Block {
            header,
            transactions: txs,
        };
        Some((Arc::new(block), size))
    }

    /// Returns the raw [`Block`] with [`block::Hash`] or
    /// [`Height`], if it exists in the finalized chain and its raw transaction
    /// data is available.
    ///
    /// Returns `None` if the block does not exist, or if its transaction bodies
    /// are missing because they have been pruned.
    #[allow(clippy::unwrap_in_result)]
    fn raw_block(&self, hash_or_height: HashOrHeight) -> Option<(RawBytes, Vec<RawBytes>)> {
        // Block
        let height = hash_or_height.height_or_else(|hash| self.height(hash))?;
        if !self.contains_body_at_height(height) {
            return None;
        }

        let header = self.raw_block_header(height.into())?;

        // Transactions

        let transactions: Vec<RawBytes> = self
            .raw_transactions_by_height(height)
            .map(|(_, tx)| tx)
            .collect();

        if self.raw_block_transactions_may_be_pruned(height) {
            let transaction_hashes = self.transaction_hashes_for_block(height.into())?;
            if transactions.len() != transaction_hashes.len() {
                return None;
            }
        }

        Some((header, transactions))
    }

    /// Returns `true` if `height` is in the range where raw transactions may
    /// have been pruned from `tx_by_loc`.
    fn raw_block_transactions_may_be_pruned(&self, height: Height) -> bool {
        height.0 != 0
            && self
                .lowest_retained_height()
                .is_some_and(|lowest| height < lowest)
    }

    /// Returns the Sapling [`note commitment tree`](sapling::tree::NoteCommitmentTree) specified by
    /// a hash or height, if it exists in the finalized state.
    #[allow(clippy::unwrap_in_result)]
    pub fn sapling_tree_by_hash_or_height(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Option<Arc<sapling::tree::NoteCommitmentTree>> {
        let height = hash_or_height.height_or_else(|hash| self.height(hash))?;

        self.sapling_tree_by_height(&height)
    }

    /// Returns the Orchard [`note commitment tree`](orchard::tree::NoteCommitmentTree) specified by
    /// a hash or height, if it exists in the finalized state.
    #[allow(clippy::unwrap_in_result)]
    pub fn orchard_tree_by_hash_or_height(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Option<Arc<orchard::tree::NoteCommitmentTree>> {
        let height = hash_or_height.height_or_else(|hash| self.height(hash))?;

        self.orchard_tree_by_height(&height)
    }

    // Read tip block methods

    /// Returns the hash of the current finalized tip block.
    pub fn finalized_tip_hash(&self) -> block::Hash {
        self.tip()
            .map(|(_, hash)| hash)
            // if the state is empty, return the genesis previous block hash
            .unwrap_or(GENESIS_PREVIOUS_BLOCK_HASH)
    }

    /// Returns the height of the current finalized tip block.
    pub fn finalized_tip_height(&self) -> Option<block::Height> {
        self.tip().map(|(height, _)| height)
    }

    /// Returns the tip block, if there is one.
    pub fn tip_block(&self) -> Option<Arc<Block>> {
        let (height, _hash) = self.tip()?;
        self.block(height.into())
    }

    /// Returns the highest header stored on disk.
    #[allow(clippy::unwrap_in_result)]
    pub fn best_header_tip(&self) -> Option<(block::Height, block::Hash)> {
        let body_tip = self.tip();
        let zakura_header_tip = self.zakura_header_tip();

        match (body_tip, zakura_header_tip) {
            (Some(body_tip), Some(header_tip)) if body_tip.0 >= header_tip.0 => Some(body_tip),
            (Some(_), Some(header_tip)) => Some(header_tip),
            (Some(body_tip), None) => Some(body_tip),
            (None, Some(header_tip)) => Some(header_tip),
            (None, None) => None,
        }
    }

    /// Returns the hash at `height` from a committed body, falling back to a persisted Zakura
    /// header-only row. `None` if neither is present.
    fn header_hash_at(&self, height: block::Height) -> Option<block::Hash> {
        self.hash(height)
            .or_else(|| self.zakura_header_hash(height))
    }

    /// Returns the highest checkpoint height whose header hash is positively re-established against
    /// the hardcoded checkpoint list. Every height at or below this is checkpoint-authenticated.
    ///
    /// Used to bound checkpoint-class body scheduling (airtight ordering): the Zakura checkpoint
    /// fast-commit path has no fallback, so a checkpoint-class body must never be requested before
    /// its height is authenticated here.
    pub fn authenticated_header_tip(&self) -> Option<block::Height> {
        let checkpoints = self.network().checkpoint_list();
        let persisted_tip = self.best_header_tip().map(|(height, _)| height)?;

        // Walk checkpoints down from the highest at/below the persisted tip until one is positively
        // authenticated. Normally the first matches; we do not trust persistence to have checked it
        // (see `authenticated_checkpoint_hash`). The body tip's enclosing checkpoint is always
        // authenticated because it was committed from a verified block, so this terminates.
        let mut candidate = checkpoints.max_height_in_range(..=persisted_tip);
        while let Some(c) = candidate {
            if self.header_hash_at(c) == checkpoints.hash(c) {
                return Some(c);
            }
            candidate = c
                .previous()
                .ok()
                .and_then(|below| checkpoints.max_height_in_range(..=below));
        }

        None
    }

    /// Returns the Zakura-authenticated header hash at `height`, if `height` is a header-only
    /// frontier height (above the committed body tip) whose enclosing checkpoint has been reached
    /// and authenticated; otherwise `None`.
    ///
    /// The hash is taken **strictly from the Zakura header store**, never from committed body state,
    /// so the token means exactly "the checkpoint-authenticated Zakura header hash for `height`".
    /// Authentication is established positively at the gate: for the next checkpoint `C >= height`,
    /// the persisted Zakura header hash at `C` must equal the hardcoded `checkpoint_list.hash(C)`.
    ///
    /// Continuity is sound because `height` and `C` are both in the un-trimmed frontier above the
    /// body tip (the Zakura store releases a row only once its body commits), so every height in
    /// `[height, C]` is also in the frontier; header sync persists only anchor-linked contiguous
    /// ranges, so continuity from `height` up to the pinned `C` authenticates `height`'s hash. An
    /// already-committed height returns `None` here (its Zakura row has been released), so the fast
    /// path never re-commits or authenticates from body state.
    ///
    /// This is the only constructor of [`AuthenticatedCheckpointHash`].
    pub fn authenticated_checkpoint_hash(
        &self,
        height: block::Height,
    ) -> Option<AuthenticatedCheckpointHash> {
        let checkpoints = self.network().checkpoint_list();

        // The next checkpoint at or above `height`. `None` => above the last checkpoint, i.e. not a
        // checkpoint-anchored height (the caller must not fast-commit it).
        let next_checkpoint = checkpoints.min_height_in_range(height..)?;

        // Construct strictly from the Zakura header frontier: `height` must be a header-only frontier
        // height. (`None` for an already-committed height, so we never authenticate from body
        // state.)
        let header_hash = self.zakura_header_hash(height)?;

        // Positively re-check the checkpoint anchor against the Zakura frontier. If the frontier has
        // not reached `C`, or the persisted hash there does not match the hardcoded checkpoint,
        // `height` is not yet authenticated.
        if self.zakura_header_hash(next_checkpoint) != checkpoints.hash(next_checkpoint) {
            return None;
        }

        Some(AuthenticatedCheckpointHash::new(header_hash))
    }

    /// Returns a contiguous ascending header range from full blocks and Zakura header rows.
    pub fn headers_by_height_range(
        &self,
        start: block::Height,
        count: u32,
    ) -> Vec<(block::Height, block::Hash, Arc<block::Header>)> {
        let capped_count = count.min(MAX_HEADER_SYNC_HEIGHT_RANGE);

        let mut headers = Vec::with_capacity(
            usize::try_from(capped_count).expect("capped header count fits in usize"),
        );
        let mut height = start;

        for _ in 0..capped_count {
            let Some((hash, header)) = self.header_by_height(height) else {
                break;
            };

            headers.push((height, hash, header));

            let Ok(next_height) = height.next() else {
                break;
            };
            height = next_height;
        }

        headers
    }

    /// Returns recent header difficulty/time context in reverse height order,
    /// starting at `height`.
    pub fn recent_header_context(
        &self,
        height: block::Height,
    ) -> Vec<(
        zebra_chain::work::difficulty::CompactDifficulty,
        DateTime<Utc>,
    )> {
        let mut context = Vec::with_capacity(check::difficulty::POW_ADJUSTMENT_BLOCK_SPAN);
        let mut current_height = Some(height);

        while let Some(height) = current_height {
            let Some((_hash, header)) = self.header_by_height(height) else {
                break;
            };

            context.push((header.difficulty_threshold, header.time));
            if context.len() == check::difficulty::POW_ADJUSTMENT_BLOCK_SPAN {
                break;
            }

            current_height = height.previous().ok();
        }

        context
    }

    /// Returns header-known, body-missing heights.
    pub fn missing_block_bodies(
        &self,
        verified_block_tip: Option<block::Height>,
        best_header_tip: Option<block::Height>,
        from: block::Height,
        limit: u32,
    ) -> Vec<block::Height> {
        let Some(best_header_tip) = best_header_tip else {
            return Vec::new();
        };

        let start = verified_block_tip
            .and_then(|tip| tip.next().ok())
            .map_or(from, |first_missing| first_missing.max(from));

        if start > best_header_tip {
            return Vec::new();
        }

        let count = limit.min(best_header_tip.0.saturating_sub(start.0).saturating_add(1));

        // Airtight ordering: never schedule a checkpoint-class body (`height <= checkpoint_max`)
        // before header sync has checkpoint-authenticated that height. The Zakura checkpoint
        // fast-commit path has no fallback, so requesting such a body early would later trip a hard
        // invariant. Full-class heights (above the last checkpoint) are unaffected.
        let checkpoint_max = self.network().checkpoint_list().max_height();
        let authenticated_header_tip = self.authenticated_header_tip();

        self.headers_by_height_range(start, count)
            .into_iter()
            .map(|(height, _, _)| height)
            .filter(|height| !self.contains_body_at_height(*height))
            .filter(|height| {
                *height > checkpoint_max
                    || authenticated_header_tip.is_some_and(|tip| *height <= tip)
            })
            .take(limit as usize)
            .collect()
    }

    #[allow(clippy::unwrap_in_result)]
    fn zakura_header_tip(&self) -> Option<(block::Height, block::Hash)> {
        let hash_by_height = self.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
        self.db.zs_last_key_value(&hash_by_height)
    }

    #[allow(clippy::unwrap_in_result)]
    fn zakura_header_hash(&self, height: block::Height) -> Option<block::Hash> {
        let hash_by_height = self.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
        self.db.zs_get(&hash_by_height, &height)
    }

    #[allow(clippy::unwrap_in_result)]
    fn zakura_header_height(&self, hash: block::Hash) -> Option<block::Height> {
        let height_by_hash = self.db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
        self.db.zs_get(&height_by_hash, &hash)
    }

    #[allow(clippy::unwrap_in_result)]
    fn zakura_header(&self, height: block::Height) -> Option<Arc<block::Header>> {
        let header_by_height = self.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
        self.db.zs_get(&header_by_height, &height)
    }

    /// Returns provisional Zakura header-ahead roots for the contiguous prefix of `range`.
    pub fn zakura_header_commitment_roots_by_height_range(
        &self,
        range: std::ops::RangeInclusive<Height>,
    ) -> Vec<BlockCommitmentRoots> {
        let cf = self
            .db
            .cf_handle(ZAKURA_HEADER_COMMITMENT_ROOTS_BY_HEIGHT)
            .unwrap();
        let mut roots = Vec::new();
        for height in (range.start().0..=range.end().0).map(Height) {
            let Some(value) = self
                .db
                .zs_get::<_, _, CommitmentRootsByHeight>(&cf, &height)
            else {
                break;
            };
            roots.push(BlockCommitmentRoots {
                height,
                sapling_root: value.sapling,
                orchard_root: value.orchard,
            });
        }
        roots
    }

    /// Persist provisional header-ahead roots supplied by Zakura header sync.
    pub fn insert_zakura_header_commitment_roots(
        &self,
        roots: impl IntoIterator<Item = BlockCommitmentRoots>,
    ) -> Result<(), rocksdb::Error> {
        let cf = self
            .db
            .cf_handle(ZAKURA_HEADER_COMMITMENT_ROOTS_BY_HEIGHT)
            .unwrap();
        let mut batch = DiskWriteBatch::new();
        for roots in roots {
            batch.zs_insert(
                &cf,
                roots.height,
                CommitmentRootsByHeight {
                    sapling: roots.sapling_root,
                    orchard: roots.orchard_root,
                },
            );
        }
        self.write_batch(batch)
    }

    /// Delete provisional header-ahead roots by height.
    pub fn delete_zakura_header_commitment_roots(
        &self,
        heights: impl IntoIterator<Item = Height>,
    ) -> Result<(), rocksdb::Error> {
        let cf = self
            .db
            .cf_handle(ZAKURA_HEADER_COMMITMENT_ROOTS_BY_HEIGHT)
            .unwrap();
        let mut batch = DiskWriteBatch::new();
        for height in heights {
            batch.zs_delete(&cf, height);
        }
        self.write_batch(batch)
    }

    // The header readers below resolve from the consensus header column families
    // (`hash_by_height` / `height_by_hash` / `block_header_by_height`) *ungated*
    // by body availability, then fall back to the provisional Zakura frontier.
    // Reading the consensus header rows directly keeps a height's header readable
    // even when its body is absent because it was pruned (those rows are retained
    // by pruning, which only deletes `tx_by_loc`).

    fn header_hash(&self, height: block::Height) -> Option<block::Hash> {
        self.hash(height)
            .or_else(|| self.zakura_header_hash(height))
    }

    fn header_height(&self, hash: block::Hash) -> Option<block::Height> {
        self.height(hash)
            .or_else(|| self.zakura_header_height(hash))
    }

    fn header_by_height(&self, height: block::Height) -> Option<(block::Hash, Arc<block::Header>)> {
        if let Some(hash) = self.hash(height) {
            return self
                .block_header(height.into())
                .map(|header| (hash, header));
        }

        let hash = self.zakura_header_hash(height)?;
        let header = self.zakura_header(height)?;

        Some((hash, header))
    }

    // Read transaction methods

    /// Returns the [`Transaction`] with [`transaction::Hash`], and its [`Height`],
    /// if a transaction with that hash exists in the finalized chain.
    #[allow(clippy::unwrap_in_result)]
    pub fn transaction(
        &self,
        hash: transaction::Hash,
    ) -> Option<(Arc<Transaction>, Height, DateTime<Utc>)> {
        let tx_by_loc = self.db.cf_handle("tx_by_loc").unwrap();

        let transaction_location = self.transaction_location(hash)?;

        let block_time = self
            .block_header(transaction_location.height.into())
            .map(|header| header.time);

        self.db
            .zs_get(&tx_by_loc, &transaction_location)
            .and_then(|tx| block_time.map(|time| (tx, transaction_location.height, time)))
    }

    /// Returns an iterator of all [`Transaction`]s for a provided block height in finalized state.
    #[allow(clippy::unwrap_in_result)]
    pub fn transactions_by_height(
        &self,
        height: Height,
    ) -> impl Iterator<Item = (TransactionLocation, Transaction)> + '_ {
        self.transactions_by_location_range(
            TransactionLocation::min_for_height(height)
                ..=TransactionLocation::max_for_height(height),
        )
    }

    /// Returns an iterator of all raw [`Transaction`]s for a provided block
    /// height in finalized state.
    #[allow(clippy::unwrap_in_result)]
    fn raw_transactions_by_height(
        &self,
        height: Height,
    ) -> impl Iterator<Item = (TransactionLocation, RawBytes)> + '_ {
        self.raw_transactions_by_location_range(
            TransactionLocation::min_for_height(height)
                ..=TransactionLocation::max_for_height(height),
        )
    }

    /// Returns an iterator of all [`Transaction`]s in the provided range
    /// of [`TransactionLocation`]s in finalized state.
    #[allow(clippy::unwrap_in_result)]
    pub fn transactions_by_location_range<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (TransactionLocation, Transaction)> + '_
    where
        R: RangeBounds<TransactionLocation>,
    {
        let tx_by_loc = self.db.cf_handle("tx_by_loc").unwrap();
        self.db.zs_forward_range_iter(tx_by_loc, range)
    }

    /// Returns an iterator of all raw [`Transaction`]s in the provided range
    /// of [`TransactionLocation`]s in finalized state.
    #[allow(clippy::unwrap_in_result)]
    pub fn raw_transactions_by_location_range<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (TransactionLocation, RawBytes)> + '_
    where
        R: RangeBounds<TransactionLocation>,
    {
        let tx_by_loc = self.db.cf_handle("tx_by_loc").unwrap();
        self.db.zs_forward_range_iter(tx_by_loc, range)
    }

    /// Returns `true` if raw transaction bytes exist in the half-open height range
    /// `[from, until)`.
    pub(in super::super) fn raw_transactions_exist_in_range(
        &self,
        from: Height,
        until: Height,
    ) -> bool {
        if from >= until {
            return false;
        }

        self.raw_transactions_by_location_range(
            TransactionLocation::min_for_height(from)..TransactionLocation::min_for_height(until),
        )
        .next()
        .is_some()
    }

    /// Returns the [`TransactionLocation`] for [`transaction::Hash`],
    /// if it exists in the finalized chain.
    #[allow(clippy::unwrap_in_result)]
    pub fn transaction_location(&self, hash: transaction::Hash) -> Option<TransactionLocation> {
        let tx_loc_by_hash = self.db.cf_handle("tx_loc_by_hash").unwrap();
        self.db.zs_get(&tx_loc_by_hash, &hash)
    }

    /// Returns the [`transaction::Hash`] for [`TransactionLocation`],
    /// if it exists in the finalized chain.
    #[allow(clippy::unwrap_in_result)]
    #[allow(dead_code)]
    pub fn transaction_hash(&self, location: TransactionLocation) -> Option<transaction::Hash> {
        let hash_by_tx_loc = self.db.cf_handle("hash_by_tx_loc").unwrap();
        self.db.zs_get(&hash_by_tx_loc, &location)
    }

    /// Returns the [`transaction::Hash`] of the transaction that spent or revealed the given
    /// [`transparent::OutPoint`] or nullifier, if it is spent or revealed in the finalized state.
    #[cfg(feature = "indexer")]
    pub fn spending_transaction_hash(&self, spend: &Spend) -> Option<transaction::Hash> {
        let tx_loc = match spend {
            Spend::OutPoint(outpoint) => self.spending_tx_loc(outpoint)?,
            Spend::Sprout(nullifier) => self.sprout_revealing_tx_loc(nullifier)?,
            Spend::Sapling(nullifier) => self.sapling_revealing_tx_loc(nullifier)?,
            Spend::Orchard(nullifier) => self.orchard_revealing_tx_loc(nullifier)?,
        };

        self.transaction_hash(tx_loc)
    }

    /// Returns the [`transaction::Hash`]es in the block with `hash_or_height`,
    /// if it exists in this chain.
    ///
    /// Hashes are returned in block order.
    ///
    /// Returns `None` if the block is not found.
    #[allow(clippy::unwrap_in_result)]
    pub fn transaction_hashes_for_block(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Option<Arc<[transaction::Hash]>> {
        // Block
        let height = hash_or_height.height_or_else(|hash| self.height(hash))?;
        if !self.contains_body_at_height(height) {
            return None;
        }

        // Transaction hashes
        let hash_by_tx_loc = self.db.cf_handle("hash_by_tx_loc").unwrap();

        // Manually fetch the entire block's transaction hashes
        let mut transaction_hashes = Vec::new();

        for tx_index in 0..=Transaction::max_allocation() {
            let tx_loc = TransactionLocation::from_u64(height, tx_index);

            if let Some(tx_hash) = self.db.zs_get(&hash_by_tx_loc, &tx_loc) {
                transaction_hashes.push(tx_hash);
            } else {
                break;
            }
        }

        Some(transaction_hashes.into())
    }

    // Pruning methods

    /// Returns the next block height managed by online pruning, if the database is
    /// in pruned storage mode.
    ///
    /// Raw transactions in non-genesis blocks below this height may have been
    /// pruned. When pruning is first enabled on an existing archive database,
    /// regular online pruning may advance this marker before older archive raw
    /// transactions are deleted. Checkpoint sync drains that archive backlog in
    /// bounded chunks before it skips raw transaction writes before its retention
    /// start.
    /// Returns `None` if the database has never pruned any data (it is
    /// effectively an archive database).
    pub fn lowest_retained_height(&self) -> Option<Height> {
        let pruning_metadata = self.db.cf_handle(PRUNING_METADATA)?;
        self.db.zs_get(&pruning_metadata, &())
    }

    /// Returns `true` if the database has pruned historical data, and therefore
    /// cannot be reopened in [`StorageMode::Archive`](crate::StorageMode::Archive).
    pub fn is_pruned(&self) -> bool {
        self.lowest_retained_height().is_some()
    }

    // Verified-commitment-trees fast-sync methods

    /// Returns the checkpoint handoff height `H` of a verified-commitment-trees fast-synced
    /// database: the upper (exclusive) bound of the band `[U, H)` in which per-height
    /// note-commitment trees are absent. `U` is [`vct_upgrade_height`](Self::vct_upgrade_height).
    ///
    /// The fast path skips per-height trees only below the handoff; at and above `H`, semantic sync
    /// writes them again. (Trees below the upgrade height `U` are also present — written before this
    /// binary ran.) Returns `None` if the database was synced normally (per-height trees for every
    /// height below the tip). Use [`vct_tree_absent`](Self::vct_tree_absent) to test a single
    /// height rather than comparing against this bound directly.
    pub fn vct_synced_below(&self) -> Option<Height> {
        let vct_sync_metadata = self.db.cf_handle(VCT_SYNC_METADATA)?;
        self.db.zs_get(&vct_sync_metadata, &())
    }

    /// Returns `true` if the database was built by the verified-commitment-trees
    /// path, and therefore lacks per-height note-commitment trees below the
    /// handoff height. The missing history is surfaced at the RPC boundary (§9);
    /// it does not prevent reopening in any storage mode.
    pub fn is_vct_synced(&self) -> bool {
        self.vct_synced_below().is_some()
    }

    /// Returns the verified-commitment-trees upgrade height `U`: the lowest height this binary
    /// committed, equal to the lowest height in the `commitment_roots_by_height` serving index.
    ///
    /// Written once on the first committed block and never moved (see
    /// [`VCT_UPGRADE_METADATA`](crate::service::finalized_state::VCT_UPGRADE_METADATA)). Heights
    /// below `U` predate this binary, so they hold per-height trees but no index entry; heights at
    /// or above `U` hold an index entry. Returns `None` for a database written before this marker
    /// existed (a pre-index archive database), where every height is served from the trees.
    pub fn vct_upgrade_height(&self) -> Option<Height> {
        let vct_upgrade_metadata = self.db.cf_handle(VCT_UPGRADE_METADATA)?;
        self.db.zs_get(&vct_upgrade_metadata, &())
    }

    /// Returns `true` if the per-height note-commitment tree at `height` was never written because
    /// this is a vct-synced database, i.e. `height` falls in the absent band `[U, H)`.
    ///
    /// `U` is the upgrade height ([`vct_upgrade_height`](Self::vct_upgrade_height)) and `H` is the
    /// checkpoint handoff ([`vct_synced_below`](Self::vct_synced_below)). The fast path skips
    /// per-height trees only at and after the upgrade and only below the checkpoint: heights below
    /// `U` keep their pre-upgrade trees, and heights at or above `H` get trees again from semantic
    /// sync. Returns `false` for a normally-synced database (`H` is `None`). When `H` is set, `U`
    /// is too (both are written by the commit path), but `U` defaults to genesis if ever absent,
    /// which preserves the original "absent below `H`" behaviour.
    pub fn vct_tree_absent(&self, height: Height) -> bool {
        let Some(handoff) = self.vct_synced_below() else {
            return false;
        };
        let upgrade = self.vct_upgrade_height().unwrap_or(Height(0));
        upgrade <= height && height < handoff
    }

    /// Returns `true` if `hash_or_height` resolves to a non-tip historical height
    /// whose per-height note-commitment tree is unavailable because this is a
    /// vct-synced database (the tree within the `[U, H)` absent band was never
    /// written). Read-request handlers use this to return an archive-mode error
    /// instead of a misleading "not found".
    pub fn vct_historical_tree_unavailable(&self, hash_or_height: HashOrHeight) -> bool {
        hash_or_height
            .height_or_else(|hash| self.height(hash))
            .is_some_and(|height| self.vct_tree_absent(height))
    }

    /// Returns the half-open range of block heights `[from, until)` whose raw
    /// transaction data should be pruned when committing a block at `new_tip`,
    /// given the configured `retention` window. Returns `None` if there is
    /// nothing to prune in this commit.
    ///
    /// The genesis block (height 0) is never pruned. Regular online pruning
    /// starts at the current retention boundary rather than draining all
    /// historical raw transactions from height 1. Checkpoint-sync archive backlog
    /// draining is handled by [`Self::checkpoint_raw_transaction_prune_range`].
    /// Per-commit work is still bounded by [`MAX_PRUNE_HEIGHTS_PER_COMMIT`].
    ///
    /// # Correctness
    ///
    /// `retention` is always at least
    /// [`MIN_PRUNING_RETENTION`](crate::constants::MIN_PRUNING_RETENTION), which is strictly
    /// greater than [`MAX_BLOCK_REORG_HEIGHT`].
    /// Since the returned range only ever covers heights at or below
    /// `new_tip - retention`, pruning can never delete data that a reorg or
    /// rollback could read.
    pub(in super::super) fn prune_height_range(
        new_tip: Height,
        retention: u32,
        lowest_retained: Option<Height>,
    ) -> Option<(Height, Height)> {
        let lowest_retained = lowest_retained.map(|height| height.0);
        let (from, until) = prune_height_range_inner(new_tip.0, retention, lowest_retained)?;
        Some((Height(from), Height(until)))
    }

    /// Returns a bounded raw transaction prune range for checkpoint sync after
    /// pruning is enabled on an archive database before the checkpoint retention
    /// start.
    ///
    /// Checkpoint sync can skip raw transaction writes before the start, but a
    /// single pruning marker cannot represent skipped heights above still-retained
    /// archive data. While the caller's cached archive-backlog flag is set,
    /// checkpoint commits keep raw transaction bytes and drain the backlog in
    /// bounded chunks.
    pub(in super::super) fn checkpoint_raw_transaction_prune_range(
        &self,
        skipped_until: Height,
    ) -> Option<(Height, Height)> {
        let prune_from = self.lowest_retained_height().unwrap_or(Height(1));

        if prune_from >= skipped_until {
            return None;
        }

        let prune_until = Height(
            prune_from
                .0
                .saturating_add(MAX_PRUNE_HEIGHTS_PER_COMMIT)
                .min(skipped_until.0),
        );

        Some((prune_from, prune_until))
    }

    // Write block methods

    /// Commit `finalized` to the finalized state: assemble its [`DiskWriteBatch`]
    /// and immediately flush it to disk.
    ///
    /// This is the synchronous (tip-mode) entry point. The run-ahead committer
    /// instead calls [`assemble_block_batch`](Self::assemble_block_batch) on the
    /// assembler thread and [`flush_block_batch`](Self::flush_block_batch) on a
    /// separate disk-writer thread, so the next block's assembly overlaps this
    /// block's flush.
    ///
    /// Production commits go through [`FinalizedState::assemble_finalized_direct`]
    /// and [`FinalizedState::flush_finalized_direct`], which call the two halves
    /// below directly; this combined wrapper is retained as a synchronous one-shot
    /// for tests, so it is unused in lib-only builds.
    ///
    /// # Errors
    ///
    /// - Propagates any errors from writing to the DB
    /// - Propagates any errors from computing the block's chain value balance change or
    ///   from applying the change to the chain value balance
    #[allow(dead_code)]
    #[allow(clippy::unwrap_in_result)]
    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn write_block(
        &mut self,
        finalized: FinalizedBlock,
        prev_note_commitment_trees: Option<NoteCommitmentTrees>,
        network: &Network,
        source: &str,
        retention: RetentionPlan,
        vct_anchor_roots: Option<(sapling::tree::Root, orchard::tree::Root)>,
        vct_sync_below: Option<Height>,
    ) -> Result<block::Hash, CommitCheckpointVerifiedError> {
        let (batch, hash, _contribution, commit_trace) = self.assemble_block_batch(
            finalized,
            prev_note_commitment_trees,
            network,
            retention,
            vct_anchor_roots,
            vct_sync_below,
            None,
        )?;
        self.flush_block_batch(batch, commit_trace, source);
        Ok(hash)
    }

    /// Assemble the [`DiskWriteBatch`] for `finalized` without writing it to disk.
    ///
    /// This is the read/compute half of a block commit: it reads the spent UTXOs,
    /// changed-address balances, and current value pool, applies the transparent
    /// and shielded batch preparation, and returns the fully-built batch plus the
    /// committed block hash. It performs no writes, so it can run on the assembler
    /// thread (or the look-ahead) while a previously-assembled batch is being
    /// flushed by the disk-writer thread.
    ///
    /// The reads it performs (`read_spent_utxo`, [`address_balance_location`], the
    /// value pool) depend on the parent block. In run-ahead (sync) mode the parent
    /// may not yet be durable on disk, so those reads must be served from the
    /// in-memory pipeline overlay before falling back to disk; see the caller.
    ///
    /// [`address_balance_location`]: ZebraDb::address_balance_location
    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn assemble_block_batch(
        &self,
        finalized: FinalizedBlock,
        prev_note_commitment_trees: Option<NoteCommitmentTrees>,
        network: &Network,
        retention: RetentionPlan,
        // When `Some`, skip per-height tree writes and fold these roots into
        // the anchor set.
        vct_anchor_roots: Option<(sapling::tree::Root, orchard::tree::Root)>,
        // When `Some(height)`, mark the database as vct-synced.
        vct_sync_below: Option<Height>,
        // The run-ahead pipeline's in-memory tip state and overlay. When `Some`,
        // the parent-block reads (spent UTXOs, address balances, value pool, and
        // the `vct_upgrade_height` marker) are served from it before falling back
        // to disk, and this block's contribution is captured and returned.
        overlay: Option<&FinalizedPipeline>,
    ) -> Result<
        (
            DiskWriteBatch,
            block::Hash,
            Option<PipelineBatchContribution>,
            super::super::commit_pressure::PreparedCommitTrace,
        ),
        CommitCheckpointVerifiedError,
    > {
        let write_start = std::time::Instant::now();
        let tx_hash_indexes: HashMap<transaction::Hash, usize> = finalized
            .transaction_hashes
            .iter()
            .enumerate()
            .map(|(index, hash)| (*hash, index))
            .collect();

        // Get a list of the new UTXOs in the format we need for database updates.
        //
        // TODO: index new_outputs by TransactionLocation,
        //       simplify the spent_utxos location lookup code,
        //       and remove the extra new_outputs_by_out_loc argument
        let new_outputs_by_out_loc: BTreeMap<OutputLocation, transparent::Utxo> = finalized
            .new_outputs
            .iter()
            .map(|(outpoint, ordered_utxo)| {
                (
                    lookup_out_loc(finalized.height, outpoint, &tx_hash_indexes),
                    ordered_utxo.utxo.clone(),
                )
            })
            .collect();

        // Get a list of the spent UTXOs, before we delete any from the database.
        //
        // Per-checkpoint transparent reconcile (`defer_transparent_reconcile`): in the
        // deferred range, record this block's spent outpoints into the reconcile window
        // and pass NO outpoints to the read loop, so the per-block spent-UTXO resolution,
        // the `utxo_by_out_loc` deletes, and the transparent value-pool debit are all
        // skipped here. Unlike the probe below, this is correct: the checkpoint reconcile
        // resolves, deletes, and debits in one batched pass. The recorded outpoints (and
        // the block) are threaded into the pipeline contribution below; deferral only
        // takes effect on the run-ahead pipeline path, which the bench always uses.
        //
        // Benchmark-only ceiling probe (`ZEBRA_BENCH_SKIP_TRANSPARENT_READS=1`): drop the
        // spent outpoints with NO reconcile, so the spend work is skipped entirely. This
        // measures the throughput ceiling of deferring that work off the commit critical
        // path. It produces an INCORRECT value pool and UTXO set, so it is never a shipped
        // path — only a measurement of the upper bound.
        let defer_spends = self.defers_transparent_spends();
        // Deferral records into the run-ahead pipeline's reconcile window, so it is
        // only correct when an overlay is present. Without one there is nowhere to
        // record the spends and the checkpoint reconcile would never run, silently
        // corrupting the value pool / UTXO set. The bench always runs pipelined.
        assert!(
            !defer_spends || overlay.is_some(),
            "defer_transparent_reconcile requires the run-ahead pipeline \
             (finalized_block_pipeline_depth > 0)"
        );
        let mut deferred_spent: Vec<transparent::OutPoint> = Vec::new();
        let outpoints: Vec<transparent::OutPoint> = if defer_spends {
            deferred_spent = finalized
                .block
                .transactions
                .iter()
                .flat_map(|tx| tx.inputs().iter())
                .flat_map(|input| input.outpoint())
                .collect();
            Vec::new()
        } else if super::bench_skip_transparent_reads() {
            Vec::new()
        } else {
            finalized
                .block
                .transactions
                .iter()
                .flat_map(|tx| tx.inputs().iter())
                .flat_map(|input| input.outpoint())
                .collect()
        };

        // Serialize the raw transaction bytes for `tx_by_loc` concurrently with the
        // spent-UTXO reads. Serialization is CPU-bound while the reads wait on disk,
        // so overlapping them keeps the raw-tx serialization off the committer's
        // serial critical path. The bytes are handed to `prepare_block_batch`; if
        // `None` it serializes inline (e.g. the semantic path).
        let store_raw_txs = retention.stores_raw_transactions();
        let db: &ZebraDb = self;
        // Resolve a spent output from the run-ahead overlay first (it may have been
        // created by a not-yet-flushed block), then from disk / this block's own
        // new outputs. With `overlay = None` this is exactly the disk read.
        let read_one_spent =
            |outpoint: transparent::OutPoint| -> (transparent::OutPoint, OutputLocation, transparent::Utxo) {
                if let Some(overlay) = overlay {
                    if let Some((out_loc, utxo)) = overlay.spent_utxo_override(&outpoint) {
                        return (outpoint, out_loc, utxo);
                    }
                }
                read_spent_utxo(
                    db,
                    finalized.height,
                    outpoint,
                    &tx_hash_indexes,
                    &finalized.new_outputs,
                )
            };
        let spent_reads_start = std::time::Instant::now();
        let (spent_utxos, precomputed_raw_txs): (
            Vec<(transparent::OutPoint, OutputLocation, transparent::Utxo)>,
            Option<Vec<RawBytes>>,
        ) = rayon::join(
            || {
                if outpoints.len() >= super::PARALLEL_BLOCK_READ_THRESHOLD {
                    use rayon::prelude::*;
                    outpoints.into_par_iter().map(&read_one_spent).collect()
                } else {
                    outpoints.into_iter().map(&read_one_spent).collect()
                }
            },
            || {
                if store_raw_txs {
                    use rayon::prelude::*;
                    Some(
                        finalized
                            .block
                            .transactions
                            .par_iter()
                            .map(|transaction| RawBytes::new_raw_bytes(transaction.as_bytes()))
                            .collect(),
                    )
                } else {
                    None
                }
            },
        );
        let reads_dur = spent_reads_start.elapsed();
        #[cfg(feature = "commit-metrics")]
        metrics::histogram!("zebra.state.write.spent_utxo_reads.duration_seconds")
            .record(reads_dur.as_secs_f64());

        // Spend-distance instrumentation (trace-only): how many blocks back was each
        // spent UTXO created? The cumulative buckets give the hit rate an in-memory
        // UTXO cache of a given size would achieve, since the run-ahead overlay already
        // serves only the pipeline-depth window. Built only when stage timing is on.
        if zebra_chain::stage_timing::enabled() {
            let spend_h = finalized.height.0;
            let (mut le4k, mut le16k, mut le65k, mut le262k, mut le1m) =
                (0u64, 0u64, 0u64, 0u64, 0u64);
            for (_op, out_loc, _utxo) in &spent_utxos {
                let d = spend_h.saturating_sub(out_loc.height().0);
                if d <= 4_096 {
                    le4k += 1;
                }
                if d <= 16_384 {
                    le16k += 1;
                }
                if d <= 65_536 {
                    le65k += 1;
                }
                if d <= 262_144 {
                    le262k += 1;
                }
                if d <= 1_048_576 {
                    le1m += 1;
                }
            }
            zebra_chain::stage_timing::record_val(spend_h, "spent_total", spent_utxos.len() as u64);
            zebra_chain::stage_timing::record_val(spend_h, "spent_le4k", le4k);
            zebra_chain::stage_timing::record_val(spend_h, "spent_le16k", le16k);
            zebra_chain::stage_timing::record_val(spend_h, "spent_le65k", le65k);
            zebra_chain::stage_timing::record_val(spend_h, "spent_le262k", le262k);
            zebra_chain::stage_timing::record_val(spend_h, "spent_le1m", le1m);
        }

        let spent_utxos_by_outpoint: HashMap<transparent::OutPoint, transparent::Utxo> =
            spent_utxos
                .iter()
                .map(|(outpoint, _output_loc, utxo)| (*outpoint, utxo.clone()))
                .collect();

        // TODO: Add `OutputLocation`s to the values in `spent_utxos_by_outpoint` to avoid creating a second hashmap with the same keys
        #[cfg(feature = "indexer")]
        let out_loc_by_outpoint: HashMap<transparent::OutPoint, OutputLocation> = spent_utxos
            .iter()
            .map(|(outpoint, out_loc, _utxo)| (*outpoint, *out_loc))
            .collect();
        let spent_utxos_by_out_loc: BTreeMap<OutputLocation, transparent::Utxo> = spent_utxos
            .into_iter()
            .map(|(_outpoint, out_loc, utxo)| (out_loc, utxo))
            .collect();

        // Like the spent-UTXO reads above, the per-address balance lookups are
        // cache-served but serial. Fan them across the rayon pool once a block
        // touches enough addresses to amortize the fork-join cost.
        fn read_addr_locs<T: Send, F: Fn(&transparent::Address) -> Option<T> + Sync>(
            changed_addresses: HashSet<transparent::Address>,
            f: F,
        ) -> HashMap<transparent::Address, T> {
            if changed_addresses.len() >= super::PARALLEL_BLOCK_READ_THRESHOLD {
                use rayon::prelude::*;
                changed_addresses
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into_par_iter()
                    .filter_map(|address| Some((address, f(&address)?)))
                    .collect()
            } else {
                changed_addresses
                    .into_iter()
                    .filter_map(|address| Some((address, f(&address)?)))
                    .collect()
            }
        }

        // # Performance
        //
        // It's better to update entries in RocksDB with insertions over merge operations when there is no risk that
        // insertions may overwrite values that are updated concurrently in database format upgrades as inserted values
        // are quicker to read and require less background compaction.
        //
        // Reading entries that have been updated with merge ops often requires reading the latest fully-merged value,
        // reading all of the pending merge operands (potentially hundreds), and applying pending merge operands to the
        // fully-merged value such that it's much faster to read entries that have been updated with insertions than it
        // is to read entries that have been updated with merge operations.
        //
        // When the address index is skipped (fast-sync), none of the per-address
        // balance reads happen and `address_balances` is left empty; the gated
        // transparent index passes below then write no address entries.
        let address_reads_start = std::time::Instant::now();
        let address_balances: AddressBalanceLocationUpdates = if self.config().skip_address_index()
        {
            AddressBalanceLocationUpdates::Insert(HashMap::new())
        } else {
            // Transparent addresses with changed balances/UTXOs in this block.
            let changed_addresses: HashSet<transparent::Address> = spent_utxos_by_out_loc
                .values()
                .chain(
                    finalized
                        .new_outputs
                        .values()
                        .map(|ordered_utxo| &ordered_utxo.utxo),
                )
                .filter_map(|utxo| utxo.output.address(network))
                .unique()
                .collect();
            // Resolve an address balance from the run-ahead overlay first (it may
            // have been updated by a not-yet-flushed block), then from disk.
            let lookup_balance = |addr: &transparent::Address| {
                if let Some(overlay) = overlay {
                    if let Some(balance) = overlay.address_balance_override(addr) {
                        return Some(balance);
                    }
                }
                self.address_balance_location(addr)
            };
            if self.finished_format_upgrades() {
                AddressBalanceLocationUpdates::Insert(read_addr_locs(changed_addresses, |addr| {
                    lookup_balance(addr)
                }))
            } else {
                AddressBalanceLocationUpdates::Merge(read_addr_locs(changed_addresses, |addr| {
                    Some(lookup_balance(addr)?.into_new_change())
                }))
            }
        };
        let address_reads_dur = address_reads_start.elapsed();
        #[cfg(feature = "commit-metrics")]
        metrics::histogram!("zebra.state.write.address_reads.duration_seconds")
            .record(address_reads_dur.as_secs_f64());

        // The value pool and `vct_upgrade_height` marker are threaded forward in
        // memory by the run-ahead pipeline; with `overlay = None` they come from
        // disk exactly as before. `capture_pipeline_outputs` is set only when
        // running ahead, so the synchronous path avoids the balance clone.
        let value_pool = overlay
            .map(|overlay| overlay.value_pool())
            .unwrap_or_else(|| self.finalized_value_pool());
        let pipeline_vct_marker_set = overlay
            .map(|overlay| overlay.vct_upgrade_marker_set())
            .unwrap_or(false);
        let capture_pipeline_outputs = overlay.is_some();

        let assembly_start = std::time::Instant::now();
        let mut batch = DiskWriteBatch::new();

        // In case of errors, propagate and do not write the batch.
        #[cfg(feature = "commit-metrics")]
        let batch_prep_start = std::time::Instant::now();
        let block_outputs = batch.prepare_block_batch(
            self,
            network,
            &finalized,
            new_outputs_by_out_loc,
            spent_utxos_by_outpoint,
            spent_utxos_by_out_loc,
            #[cfg(feature = "indexer")]
            out_loc_by_outpoint,
            address_balances,
            value_pool,
            prev_note_commitment_trees,
            store_raw_txs,
            precomputed_raw_txs,
            vct_anchor_roots,
            vct_sync_below,
            pipeline_vct_marker_set,
            capture_pipeline_outputs,
        )?;

        // In pruned storage mode, delete raw transaction history that has fallen
        // outside the retention window, and/or advance the pruning marker. This
        // goes in the same atomic batch as the tip advance, so pruning and the
        // tip advance are always consistent, and it reuses the single-writer
        // block commit path. In archive mode the plan is always `Store`, so this
        // is a no-op.
        retention.prepare_prune(&mut batch, self, &finalized);
        let batch_assembly_dur = assembly_start.elapsed();
        #[cfg(feature = "commit-metrics")]
        {
            metrics::histogram!("zebra.state.write.batch_prep.duration_seconds")
                .record(batch_prep_start.elapsed().as_secs_f64());
            metrics::histogram!("zebra.state.write.batch_bytes")
                .record(batch.size_in_bytes() as f64);
        }

        // When running ahead, capture this block's overlay contribution: the
        // outputs it created (so the next block can spend them from memory) and
        // the absolute address balances it updated.
        let contribution = overlay.map(|_| {
            let created_outputs = finalized
                .new_outputs
                .iter()
                .map(|(outpoint, ordered_utxo)| {
                    (
                        *outpoint,
                        lookup_out_loc(finalized.height, outpoint, &tx_hash_indexes),
                        ordered_utxo.utxo.clone(),
                    )
                })
                .collect();
            let updated_balances = match block_outputs.address_balances {
                Some(AddressBalanceLocationUpdates::Insert(balances)) => {
                    balances.into_iter().collect()
                }
                // The pipeline only runs once format upgrades are finished (the
                // `Insert` path), so `Merge`/`None` contribute no balances.
                _ => Vec::new(),
            };

            PipelineBatchContribution {
                value_pool: block_outputs.value_pool,
                wrote_vct_upgrade_marker: block_outputs.wrote_vct_upgrade_marker,
                created_outputs,
                updated_balances,
                // Per-checkpoint reconcile: when deferring, hand the spent outpoints
                // + block + deferred-pool change to the pipeline window so the
                // checkpoint reconcile can resolve, delete, and re-debit them. Empty
                // / `None` when not deferring (no behavior change).
                deferred_spent,
                deferred_block: defer_spends.then(|| finalized.block.clone()),
                deferred_pool_change: finalized.deferred_pool_balance_change,
            }
        });

        let commit_trace = super::super::commit_pressure::PreparedCommitTrace {
            height: finalized.height.0,
            block: finalized.block.clone(),
            tx_count: finalized.transaction_hashes.len(),
            output_count: finalized.new_outputs.len(),
            batch_keys: batch.len(),
            batch_bytes: batch.size_in_bytes(),
            reads: reads_dur,
            address_reads: address_reads_dur,
            batch_assembly: batch_assembly_dur,
            // The VCT fold runs before `assemble_block_batch`; the caller
            // (`assemble_finalized_direct`) sets this after it returns.
            fold: std::time::Duration::ZERO,
            // Self-time of the read/compute half (excludes the queue-wait + flush
            // that `commit_total` also captures).
            assemble_self: write_start.elapsed(),
            write_start,
        };

        Ok((batch, finalized.hash, contribution, commit_trace))
    }

    /// Flush a previously [`assemble_block_batch`](Self::assemble_block_batch)d
    /// batch to disk.
    ///
    /// This is the write half of a block commit. In run-ahead (sync) mode it runs
    /// on the dedicated disk-writer thread so it overlaps the next block's
    /// assembly; in tip mode it runs inline. A rocksdb write failure is fatal, as
    /// before.
    pub(crate) fn flush_block_batch(
        &self,
        batch: DiskWriteBatch,
        commit_trace: super::super::commit_pressure::PreparedCommitTrace,
        source: &str,
    ) {
        // Track batch commit latency for observability
        let batch_start = std::time::Instant::now();
        self.db
            .write(batch)
            .expect("unexpected rocksdb error while writing block");
        let batch_commit = batch_start.elapsed();
        metrics::histogram!("zebra.state.rocksdb.batch_commit.duration_seconds")
            .record(batch_commit.as_secs_f64());
        let total_dur = commit_trace.write_start.elapsed();
        // Optional per-slow-commit pressure row (inert unless ZEBRA_COMMIT_PRESSURE_TRACE
        // is set): pairs this commit's latency with RocksDB L0/compaction/flush state.
        // Timed separately because, when enabled, it re-serializes the block and samples
        // RocksDB stats on the disk-writer's critical path (a Heisenberg cost to subtract).
        let record_start = std::time::Instant::now();
        super::super::commit_pressure::record_commit(&self.db, commit_trace, batch_commit);
        metrics::histogram!("zebra.state.write.commit_trace_record.duration_seconds")
            .record(record_start.elapsed().as_secs_f64());

        metrics::histogram!("zebra.state.write.total.duration_seconds")
            .record(total_dur.as_secs_f64());

        tracing::trace!(?source, "committed block from");
    }

    /// Writes the given batch to the database.
    pub fn write_batch(&self, batch: DiskWriteBatch) -> Result<(), rocksdb::Error> {
        self.db.write(batch)
    }

    /// Flushes pending writes to SST files.
    pub fn flush(&self) -> Result<(), rocksdb::Error> {
        self.db.flush()
    }

    /// Compact raw transaction data in the half-open height range
    /// `[prune_from, prune_until_strictly_before)`.
    pub fn compact_raw_transaction_range(
        &self,
        prune_from: Height,
        prune_until_strictly_before: Height,
    ) {
        let tx_by_loc = self.db.cf_handle("tx_by_loc").unwrap();
        let range_start = TransactionLocation::min_for_height(prune_from);
        let range_end = TransactionLocation::min_for_height(prune_until_strictly_before);

        self.db.zs_compact_range(&tx_by_loc, range_start, range_end);
    }

    /// Seed or reconcile the Zakura header store from a committed full block.
    pub(crate) fn seed_zakura_header_from_committed_block(
        &self,
        height: block::Height,
        block: &Arc<block::Block>,
    ) -> Result<(), CommitHeaderRangeError> {
        let mut batch = DiskWriteBatch::new();
        batch.prepare_zakura_header_from_committed_block(&self.db, height, block)?;
        self.db
            .write(batch)
            .map_err(|error| CommitHeaderRangeError::StorageWriteError {
                error: error.to_string(),
            })
    }

    /// Seeds the Zakura header store with frontier header rows for a contiguous run
    /// of blocks whose bodies have **not** been committed (heights strictly above the
    /// body tip), writing in chunked batches.
    ///
    /// Offline-bench only: this stands in for what header sync would persist, so the
    /// header-authenticated checkpoint fast path (`authenticated_checkpoint_hash`) can
    /// run against a snapshot with no live header sync. Each row is independent and the
    /// insert is idempotent, so callers may seed in any order.
    pub fn seed_zakura_headers_from_blocks(
        &self,
        blocks: impl IntoIterator<Item = (block::Height, Arc<block::Block>)>,
    ) -> Result<(), CommitHeaderRangeError> {
        let write = |batch: DiskWriteBatch| {
            self.db
                .write(batch)
                .map_err(|error| CommitHeaderRangeError::StorageWriteError {
                    error: error.to_string(),
                })
        };

        let mut batch = DiskWriteBatch::new();
        let mut pending = 0usize;
        for (height, block) in blocks {
            batch.prepare_zakura_header_from_committed_block(&self.db, height, &block)?;
            pending += 1;
            if pending >= 2000 {
                write(std::mem::take(&mut batch))?;
                pending = 0;
            }
        }
        if pending > 0 {
            write(batch)?;
        }
        Ok(())
    }
}

/// Read a spent transparent UTXO and its output location before deleting it from the database.
///
/// Some UTXOs are created and spent in the same block, so they are in
/// `tx_hash_indexes` and `new_outputs` rather than the database.
fn read_spent_utxo(
    db: &ZebraDb,
    height: Height,
    outpoint: transparent::OutPoint,
    tx_hash_indexes: &HashMap<transaction::Hash, usize>,
    new_outputs: &HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
) -> (transparent::OutPoint, OutputLocation, transparent::Utxo) {
    let db_out_loc = db.output_location(&outpoint);
    let out_loc = db_out_loc.unwrap_or_else(|| lookup_out_loc(height, &outpoint, tx_hash_indexes));
    let utxo = db_out_loc
        .and_then(|loc| db.utxo_by_location(loc))
        .map(|ordered_utxo| ordered_utxo.utxo)
        .or_else(|| {
            new_outputs
                .get(&outpoint)
                .map(|ordered_utxo| ordered_utxo.utxo.clone())
        })
        .expect("already checked UTXO was in state or block");

    (outpoint, out_loc, utxo)
}

/// Lookup the output location for an outpoint.
///
/// `tx_hash_indexes` must contain `outpoint.hash` and that transaction's index in its block.
fn lookup_out_loc(
    height: Height,
    outpoint: &transparent::OutPoint,
    tx_hash_indexes: &HashMap<transaction::Hash, usize>,
) -> OutputLocation {
    let tx_index = tx_hash_indexes
        .get(&outpoint.hash)
        .expect("already checked UTXO was in state or block");

    let tx_loc = TransactionLocation::from_usize(height, *tx_index);

    OutputLocation::from_outpoint(tx_loc, outpoint)
}

/// Computes the half-open range of block heights `[from, until)` to prune when a
/// block is committed at `new_tip`, given the `retention` window and the
/// `lowest_retained` pruning progress marker (`None` if nothing has been pruned
/// yet). Returns `None` if there is nothing to prune.
///
/// See [`ZebraDb::prune_height_range`] for the correctness invariant.
fn prune_height_range_inner(
    new_tip: u32,
    retention: u32,
    lowest_retained: Option<u32>,
) -> Option<(u32, u32)> {
    // Highest height eligible for pruning: keep `retention` blocks below the tip.
    let max_prunable = new_tip.checked_sub(retention)?;

    // Never prune the genesis block (height 0); it is special-cased throughout.
    if max_prunable == 0 {
        return None;
    }

    // Resume pruning from the existing progress marker. If pruning is first enabled
    // on an existing archive database, leave older history intact and start at the
    // current retention boundary.
    let prune_from = lowest_retained.unwrap_or(max_prunable);
    if prune_from > max_prunable {
        // Nothing new to prune yet.
        return None;
    }

    // Bound the per-commit work when draining a backlog. `prune_until` is the
    // exclusive upper bound on the pruned heights.
    let prune_until = (max_prunable + 1).min(prune_from + MAX_PRUNE_HEIGHTS_PER_COMMIT);

    Some((prune_from, prune_until))
}

/// Returns true when pruning progress should be logged for operators.
fn should_log_prune_progress(
    already_pruned: bool,
    new_tip: Height,
    prune_from: Height,
    prune_until: Height,
) -> bool {
    !already_pruned
        || prune_until.0 - prune_from.0 >= MAX_PRUNE_HEIGHTS_PER_COMMIT
        || new_tip.0 % MAX_PRUNE_HEIGHTS_PER_COMMIT == 0
}

/// The resolved raw-transaction retention decision for committing one finalized
/// block.
///
/// Computed once per block by
/// [`FinalizedState::retention_plan`](super::super::FinalizedState::retention_plan)
/// and applied by [`ZebraDb::write_block`] without re-derivation.
///
/// Only [`RetentionPlan::Store`] occurs in archive mode; the other variants are
/// only produced in pruned storage mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum RetentionPlan {
    /// Store this block's raw transactions and prune nothing in this commit.
    ///
    /// Archive mode, or pruned mode within the retention window with nothing due.
    Store,

    /// Store this block's raw transactions, and delete the half-open raw
    /// transaction height range `[from, until)` that has aged out of the
    /// retention window (ordinary online pruning near the tip).
    Prune { from: Height, until: Height },

    /// Drain a bounded chunk `[from, until)` of pre-existing archive raw
    /// transaction backlog while checkpoint sync is before the retention start.
    ///
    /// `final_chunk` is set on the last chunk, which reaches the checkpoint skip
    /// boundary: this block's own raw transactions are skipped and the
    /// archive-backlog flag is cleared after the commit succeeds.
    DrainBacklog {
        from: Height,
        until: Height,
        final_chunk: bool,
    },

    /// Skip this checkpoint block's raw transactions (it is before the retention
    /// start with no archive backlog left to drain).
    ///
    /// Advances the pruning marker to `lowest_retained` when `write_marker` is
    /// set (i.e. the marker is not already at or ahead of it).
    Skip {
        lowest_retained: Height,
        write_marker: bool,
    },
}

impl RetentionPlan {
    /// Returns `true` when this block's raw transaction bytes should be written
    /// to `tx_by_loc`.
    pub(in super::super) fn stores_raw_transactions(self) -> bool {
        match self {
            RetentionPlan::Store | RetentionPlan::Prune { .. } => true,
            RetentionPlan::DrainBacklog { final_chunk, .. } => !final_chunk,
            RetentionPlan::Skip { .. } => false,
        }
    }

    /// Returns `true` when the archive-backlog flag should be cleared after the
    /// commit succeeds, because the backlog has been fully drained.
    pub(in super::super) fn clears_archive_backlog(self) -> bool {
        matches!(
            self,
            RetentionPlan::DrainBacklog {
                final_chunk: true,
                ..
            }
        )
    }

    /// Adds this plan's raw transaction deletes and/or pruning marker to `batch`,
    /// and logs pruning progress for operators.
    ///
    /// The block header and transaction data must already have been added to
    /// `batch` (using [`Self::stores_raw_transactions`] to decide whether raw
    /// transactions were written), so that pruning and the tip advance commit
    /// together in one atomic batch.
    pub(in super::super) fn prepare_prune(
        self,
        batch: &mut DiskWriteBatch,
        zebra_db: &ZebraDb,
        finalized: &FinalizedBlock,
    ) {
        let already_pruned = zebra_db.lowest_retained_height().is_some();
        let retention = zebra_db
            .config()
            .pruning_config()
            .map(|pruning| pruning.tx_retention);

        match self {
            RetentionPlan::Store => {}

            RetentionPlan::Prune { from, until } => {
                if should_log_prune_progress(already_pruned, finalized.height, from, until) {
                    tracing::info!(
                        prune_from = ?from,
                        prune_until = ?until,
                        tip = ?finalized.height,
                        ?retention,
                        "pruning raw transaction history outside the retention window",
                    );
                }

                batch.prepare_prune_batch(zebra_db, from, until);
            }

            RetentionPlan::DrainBacklog { from, until, .. } => {
                if should_log_prune_progress(already_pruned, finalized.height, from, until) {
                    tracing::info!(
                        prune_from = ?from,
                        prune_until = ?until,
                        tip = ?finalized.height,
                        ?retention,
                        "pruning archive raw transaction history before checkpoint skipping",
                    );
                }

                batch.prepare_prune_batch(zebra_db, from, until);
            }

            RetentionPlan::Skip {
                lowest_retained,
                write_marker,
            } => {
                if write_marker {
                    batch.prepare_pruning_marker_batch(zebra_db, lowest_retained);
                }

                debug_assert!(
                    ZebraDb::prune_height_range(
                        finalized.height,
                        retention.expect("skipping raw transactions only happens in pruned mode"),
                        zebra_db.lowest_retained_height().max(Some(lowest_retained)),
                    )
                    .is_none(),
                    "checkpoint raw transaction skipping should keep the pruning marker ahead of online pruning"
                );
            }
        }
    }
}

#[cfg(test)]
fn inferred_header_range_roots(
    zebra_db: &ZebraDb,
    anchor: block::Hash,
    count: usize,
) -> Result<Vec<BlockCommitmentRoots>, CommitHeaderRangeError> {
    let anchor_height = zebra_db
        .header_height(anchor)
        .or_else(|| (anchor == zebra_db.network().genesis_hash()).then_some(block::Height(0)))
        .unwrap_or(block::Height(0));

    (0..count)
        .map(|index| {
            let offset =
                u32::try_from(index + 1).map_err(|_| CommitHeaderRangeError::HeightOverflow)?;
            let height = (anchor_height + i64::from(offset))
                .ok_or(CommitHeaderRangeError::HeightOverflow)?;
            Ok(BlockCommitmentRoots {
                height,
                sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
                orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
            })
        })
        .collect()
}

impl DiskWriteBatch {
    // Write block methods

    /// Prepare a database batch containing `finalized.block`,
    /// and return it (without actually writing anything).
    ///
    /// If this method returns an error, it will be propagated,
    /// and the batch should not be written to the database.
    ///
    /// # Errors
    ///
    /// - Propagates any errors from computing the block's chain value balance change or
    ///   from applying the change to the chain value balance
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_block_batch(
        &mut self,
        zebra_db: &ZebraDb,
        network: &Network,
        finalized: &FinalizedBlock,
        new_outputs_by_out_loc: BTreeMap<OutputLocation, transparent::Utxo>,
        spent_utxos_by_outpoint: HashMap<transparent::OutPoint, transparent::Utxo>,
        spent_utxos_by_out_loc: BTreeMap<OutputLocation, transparent::Utxo>,
        #[cfg(feature = "indexer")] out_loc_by_outpoint: HashMap<
            transparent::OutPoint,
            OutputLocation,
        >,
        address_balances: AddressBalanceLocationUpdates,
        value_pool: ValueBalance<NonNegative>,
        prev_note_commitment_trees: Option<NoteCommitmentTrees>,
        store_raw_transactions: bool,
        precomputed_raw_txs: Option<Vec<RawBytes>>,
        vct_anchor_roots: Option<(sapling::tree::Root, orchard::tree::Root)>,
        vct_sync_below: Option<Height>,
        // When `true`, a not-yet-flushed pipeline block already wrote the set-once
        // `vct_upgrade_height` marker, so this block must not write it again.
        pipeline_vct_marker_set: bool,
        // When `true`, capture and return this block's post-update value pool and
        // absolute address balances for the run-ahead pipeline overlay.
        capture_pipeline_outputs: bool,
    ) -> Result<BlockBatchOutputs, CommitCheckpointVerifiedError> {
        // Per-category key-count attribution (env-gated; no-op when stage timing
        // is off). Each step's `batch.len()` delta is how many keys/deletes that
        // category contributes to the commit batch (the compaction input).
        let kh = finalized.height.0;
        let mut kp = self.len();

        // Commit block, transaction, and note commitment tree data.
        self.prepare_block_header_and_transaction_data_batch(
            zebra_db,
            finalized,
            store_raw_transactions,
            precomputed_raw_txs,
        )?;
        zebra_chain::stage_timing::record_val(kh, "keys_header_tx", (self.len() - kp) as u64);
        kp = self.len();

        let zakura_header_commitment_roots_by_height = zebra_db
            .db
            .cf_handle(ZAKURA_HEADER_COMMITMENT_ROOTS_BY_HEIGHT)
            .unwrap();
        self.zs_delete(&zakura_header_commitment_roots_by_height, finalized.height);
        zebra_chain::stage_timing::record_val(kh, "keys_vct_evict", (self.len() - kp) as u64);
        kp = self.len();

        // The consensus rules are silent on shielded transactions in the genesis block,
        // because there aren't any in the mainnet or testnet genesis blocks.
        // So this means the genesis anchor is the same as the empty anchor,
        // which is already present from height 1 to the first shielded transaction.
        //
        // In Zebra we include the nullifiers and note commitments in the genesis block because it simplifies our code.
        self.prepare_shielded_transaction_batch(zebra_db, finalized);
        zebra_chain::stage_timing::record_val(kh, "keys_shielded", (self.len() - kp) as u64);
        kp = self.len();

        // The `vct_upgrade_height` marker is written once, by the first block this
        // binary commits. In the run-ahead pipeline an earlier not-yet-flushed block
        // may have already set it in memory (`pipeline_vct_marker_set`), in which case
        // its disk write is still pending, so the disk read would wrongly look absent;
        // suppress the duplicate write here and report which block actually wrote it.
        let wrote_vct_upgrade_marker =
            !pipeline_vct_marker_set && zebra_db.vct_upgrade_height().is_none();
        self.prepare_trees_batch(
            zebra_db,
            finalized,
            prev_note_commitment_trees,
            vct_anchor_roots,
            vct_sync_below,
            wrote_vct_upgrade_marker,
        );
        zebra_chain::stage_timing::record_val(kh, "keys_trees", (self.len() - kp) as u64);
        kp = self.len();

        // # Consensus
        //
        // > A transaction MUST NOT spend an output of the genesis block coinbase transaction.
        // > (There is one such zero-valued output, on each of Testnet and Mainnet.)
        //
        // https://zips.z.cash/protocol/protocol.pdf#txnconsensus
        //
        // So we ignore the genesis UTXO, transparent address index, and value pool updates
        // for the genesis block. This also ignores genesis shielded value pool updates, but there
        // aren't any of those on mainnet or testnet.
        let mut captured_address_balances = None;
        if !finalized.height.is_min() {
            // Commit transaction indexes
            captured_address_balances = self.prepare_transparent_transaction_batch(
                zebra_db,
                network,
                finalized,
                &new_outputs_by_out_loc,
                &spent_utxos_by_outpoint,
                &spent_utxos_by_out_loc,
                #[cfg(feature = "indexer")]
                &out_loc_by_outpoint,
                address_balances,
                capture_pipeline_outputs,
            );
        }
        zebra_chain::stage_timing::record_val(kh, "keys_transparent", (self.len() - kp) as u64);
        kp = self.len();

        // Commit UTXOs and value pools. This runs for every height, including
        // genesis (which writes the initial pool and block info), matching the
        // original commit path.
        let new_value_pool = self.prepare_chain_value_pools_batch(
            zebra_db,
            finalized,
            spent_utxos_by_outpoint,
            value_pool,
        )?;
        zebra_chain::stage_timing::record_val(kh, "keys_valuepool", (self.len() - kp) as u64);

        // The block has passed contextual validation, so update the metrics
        block_precommit_metrics(&finalized.block, finalized.hash, finalized.height);

        Ok(BlockBatchOutputs {
            value_pool: new_value_pool,
            address_balances: captured_address_balances,
            wrote_vct_upgrade_marker,
        })
    }

    /// Adds deletes for pruned raw transaction data to this batch, for the
    /// half-open height range `[prune_from, prune_until_strictly_before)`, and
    /// records the new pruning progress marker.
    ///
    /// This prunes only the raw transaction bytes in `tx_by_loc`, which is the
    /// largest historical column family. Everything else is retained, including:
    ///
    /// - consensus-critical state (the UTXO set, nullifiers, anchors, note
    ///   commitment trees, history tree, and value pools), and
    /// - the transaction location indexes `tx_loc_by_hash` and `hash_by_tx_loc`.
    ///
    /// # Correctness
    ///
    /// `tx_loc_by_hash` must be retained even though it is historical lookup data:
    /// spending a UTXO resolves its outpoint to an [`OutputLocation`] via
    /// [`ZebraDb::transaction_location`], which reads `tx_loc_by_hash`. A UTXO
    /// created in an old block can be spent at any later height, so pruning that
    /// index would break validation of those spends. Only the raw transaction
    /// bytes (needed for historical RPC queries, not for validating future blocks)
    /// are safe to prune.
    ///
    /// The range to prune must satisfy the retention invariant documented on
    /// [`ZebraDb::prune_height_range`].
    pub fn prepare_prune_batch(
        &mut self,
        zebra_db: &ZebraDb,
        prune_from: Height,
        prune_until_strictly_before: Height,
    ) {
        let db = &zebra_db.db;

        let tx_by_loc = db.cf_handle("tx_by_loc").unwrap();
        let pruning_metadata = db.cf_handle(PRUNING_METADATA).unwrap();

        // Range-delete the height-keyed raw transaction column family with a
        // single tombstone over the pruned height span, which is cheap for RocksDB
        // to compact (unlike a tombstone per key).
        let range_start = TransactionLocation::min_for_height(prune_from);
        let range_end = TransactionLocation::min_for_height(prune_until_strictly_before);
        self.zs_delete_range(&tx_by_loc, range_start, range_end);

        // Record pruning progress: raw transactions below `prune_until_strictly_before`
        // (except genesis) are now pruned. Writing this entry also marks the
        // database as pruned, which is a one-way state.
        self.zs_insert(&pruning_metadata, (), prune_until_strictly_before);
    }

    /// Adds a write for the pruning progress marker to this batch.
    pub fn prepare_pruning_marker_batch(
        &mut self,
        zebra_db: &ZebraDb,
        lowest_retained_height: Height,
    ) {
        let pruning_metadata = zebra_db.db.cf_handle(PRUNING_METADATA).unwrap();

        // Writing this entry also marks the database as pruned, which is a
        // one-way state.
        self.zs_insert(&pruning_metadata, (), lowest_retained_height);
    }

    /// Prepare a database batch containing the block header and transaction data
    /// from `finalized.block`, and return it (without actually writing anything).
    #[allow(clippy::unwrap_in_result)]
    pub fn prepare_block_header_and_transaction_data_batch(
        &mut self,
        zebra_db: &ZebraDb,
        finalized: &FinalizedBlock,
        store_raw_transactions: bool,
        precomputed_raw_txs: Option<Vec<RawBytes>>,
    ) -> Result<(), CommitCheckpointVerifiedError> {
        let db = &zebra_db.db;

        // Blocks
        let block_header_by_height = db.cf_handle("block_header_by_height").unwrap();
        let hash_by_height = db.cf_handle("hash_by_height").unwrap();
        let height_by_hash = db.cf_handle("height_by_hash").unwrap();

        // Transactions
        let tx_by_loc = db.cf_handle("tx_by_loc").unwrap();
        let hash_by_tx_loc = db.cf_handle("hash_by_tx_loc").unwrap();
        let tx_loc_by_hash = db.cf_handle("tx_loc_by_hash").unwrap();

        let FinalizedBlock {
            block,
            hash,
            height,
            transaction_hashes,
            ..
        } = finalized;

        // Commit block header data. Full block verification is authoritative:
        // it may replace conflicting header-only provisional rows at this
        // height and truncate their header-only descendants.
        let existing_body_header: Option<Arc<block::Header>> =
            db.zs_get(&block_header_by_height, height);
        if existing_body_header.is_some_and(|existing_header| existing_header != block.header) {
            return Err(
                CommitHeaderRangeError::ConflictingFullBlockHeader { height: *height }.into(),
            );
        }

        // Release the provisional Zakura header row for this height: once the
        // body is committed the authoritative header lives in
        // `block_header_by_height`, so the Zakura header store only ever holds
        // heights with no committed body (the frontier above the body tip).
        // This is unconditional so it also cleans up rows left by a prior run
        // that had `enable_zakura_header_seed_from_committed_blocks` enabled.
        self.prepare_zakura_header_release_from_committed_block(db, *height, block)?;

        // Index the block header, hash, and height. This also restores the
        // verified full block row after any provisional cleanup above.
        self.zs_insert(&block_header_by_height, height, &block.header);
        self.zs_insert(&hash_by_height, height, hash);
        self.zs_insert(&height_by_hash, hash, height);

        // Serialize the raw transaction bytes up front: on heavy shielded blocks
        // this serialization dominates the per-block write cost, and each
        // transaction serializes independently. The result is byte-identical to
        // inserting the transactions directly, because `RawBytes` is stored
        // verbatim. The serialized bytes are inserted in height/index order below.
        //
        // Only fan out to rayon once the block has enough transactions to amortize
        // the fork-join cost; small blocks serialize sequentially (see
        // PARALLEL_BLOCK_TX_THRESHOLD).
        let raw_transactions: Vec<RawBytes> = if !store_raw_transactions {
            Vec::new()
        } else if let Some(precomputed) = precomputed_raw_txs {
            // Serialized off the committer's critical path (overlapped with the
            // spent-UTXO reads in `write_block`); use those bytes directly.
            precomputed
        } else if block.transactions.len() >= super::PARALLEL_BLOCK_TX_THRESHOLD {
            use rayon::prelude::*;
            block
                .transactions
                .par_iter()
                .map(|transaction| RawBytes::new_raw_bytes(transaction.as_bytes()))
                .collect()
        } else {
            block
                .transactions
                .iter()
                .map(|transaction| RawBytes::new_raw_bytes(transaction.as_bytes()))
                .collect()
        };

        for (transaction_index, transaction_hash) in transaction_hashes.iter().enumerate() {
            let transaction_location = TransactionLocation::from_usize(*height, transaction_index);

            // Commit each transaction's raw bytes only when the storage policy
            // keeps historical transaction data for this height (then
            // `raw_transactions` holds the pre-serialized bytes in order).
            if let Some(raw_transaction) = raw_transactions.get(transaction_index) {
                self.zs_insert(&tx_by_loc, transaction_location, raw_transaction);
            }

            // Index each transaction hash and location
            self.zs_insert(&hash_by_tx_loc, transaction_location, transaction_hash);
            self.zs_insert(&tx_loc_by_hash, transaction_hash, transaction_location);
        }

        Ok(())
    }

    /// Prepare a database batch that seeds the Zakura header store from a
    /// committed full block.
    ///
    /// Full block verification is authoritative for the stored body. If a
    /// provisional Zakura header at this height differs, replace it with the
    /// block-derived header and drop stale provisional descendants.
    #[allow(clippy::unwrap_in_result)]
    pub fn prepare_zakura_header_from_committed_block(
        &mut self,
        db: &DiskDb,
        height: block::Height,
        block: &Arc<block::Block>,
    ) -> Result<(), CommitHeaderRangeError> {
        let zakura_header_by_height = db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
        let zakura_hash_by_height = db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
        let zakura_height_by_hash = db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
        let zakura_body_size_by_height = db.cf_handle(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT).unwrap();
        let tx_by_loc = db.cf_handle("tx_by_loc").unwrap();

        let hash = block.hash();
        let existing_zakura_header: Option<Arc<block::Header>> =
            db.zs_get(&zakura_header_by_height, &height);

        if existing_zakura_header.as_ref() == Some(&block.header)
            && db.zs_get::<_, _, block::Hash>(&zakura_hash_by_height, &height) == Some(hash)
        {
            return Ok(());
        }

        if existing_zakura_header.is_some_and(|existing_header| existing_header != block.header) {
            let best_header_tip: Option<(block::Height, block::Hash)> =
                db.zs_last_key_value(&zakura_hash_by_height);

            if let Some((best_header_tip, _)) = best_header_tip {
                for old_height in height.0..=best_header_tip.0 {
                    let old_height = block::Height(old_height);

                    if old_height != height
                        && db.zs_contains(
                            &tx_by_loc,
                            &TransactionLocation::min_for_height(old_height),
                        )
                    {
                        return Err(CommitHeaderRangeError::ConflictingFullBlockHeader {
                            height: old_height,
                        });
                    }

                    if let Some(old_hash) =
                        db.zs_get::<_, _, block::Hash>(&zakura_hash_by_height, &old_height)
                    {
                        self.zs_delete(&zakura_height_by_hash, old_hash);
                    }

                    self.zs_delete(&zakura_hash_by_height, old_height);
                    self.zs_delete(&zakura_header_by_height, old_height);
                    self.zs_delete(&zakura_body_size_by_height, old_height);
                }
            }
        } else if let Some(old_hash) =
            db.zs_get::<_, _, block::Hash>(&zakura_hash_by_height, &height)
        {
            if old_hash != hash {
                self.zs_delete(&zakura_height_by_hash, old_hash);
            }
        }

        self.zs_insert(&zakura_header_by_height, height, &block.header);
        self.zs_insert(&zakura_hash_by_height, height, hash);
        self.zs_insert(&zakura_height_by_hash, hash, height);

        Ok(())
    }

    /// Prepare a database batch that releases the Zakura header store entry for a
    /// committed full block.
    ///
    /// Once a full block body is committed at `height`, its authoritative header
    /// lives in `block_header_by_height`, so the provisional Zakura header row at
    /// this height is dropped. This maintains the frontier-overlay invariant: the
    /// Zakura header store only ever holds heights with no committed body (the
    /// frontier strictly above the body tip), so it never overlaps pruned history
    /// and is self-trimming as bodies arrive.
    ///
    /// If the committed block's header conflicts with a provisional header at this
    /// height, the stale provisional descendants above it are truncated as well,
    /// refusing to touch any height that already has a committed body.
    #[allow(clippy::unwrap_in_result)]
    pub fn prepare_zakura_header_release_from_committed_block(
        &mut self,
        db: &DiskDb,
        height: block::Height,
        block: &Arc<block::Block>,
    ) -> Result<(), CommitHeaderRangeError> {
        let zakura_header_by_height = db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
        let zakura_hash_by_height = db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
        let zakura_height_by_hash = db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
        let zakura_body_size_by_height = db.cf_handle(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT).unwrap();
        let tx_by_loc = db.cf_handle("tx_by_loc").unwrap();

        let existing_zakura_header: Option<Arc<block::Header>> =
            db.zs_get(&zakura_header_by_height, &height);
        let existing_zakura_hash: Option<block::Hash> = db.zs_get(&zakura_hash_by_height, &height);

        // Nothing to release: this height never carried a provisional header.
        if existing_zakura_header.is_none() && existing_zakura_hash.is_none() {
            return Ok(());
        }

        // A committed block whose header conflicts with the provisional chain at
        // this height invalidates the provisional descendants built on top of it.
        // Drop them, but never overwrite a height that already has a committed body.
        if existing_zakura_header.is_some_and(|existing_header| existing_header != block.header) {
            let zakura_tip: Option<(block::Height, block::Hash)> =
                db.zs_last_key_value(&zakura_hash_by_height);

            if let Some((zakura_tip, _)) = zakura_tip {
                for descendant in (height.0 + 1)..=zakura_tip.0 {
                    let descendant = block::Height(descendant);

                    if db.zs_contains(&tx_by_loc, &TransactionLocation::min_for_height(descendant))
                    {
                        return Err(CommitHeaderRangeError::ConflictingFullBlockHeader {
                            height: descendant,
                        });
                    }

                    if let Some(old_hash) =
                        db.zs_get::<_, _, block::Hash>(&zakura_hash_by_height, &descendant)
                    {
                        self.zs_delete(&zakura_height_by_hash, old_hash);
                    }

                    self.zs_delete(&zakura_hash_by_height, descendant);
                    self.zs_delete(&zakura_header_by_height, descendant);
                    self.zs_delete(&zakura_body_size_by_height, descendant);
                }
            }
        }

        // Release the provisional row at this height.
        if let Some(old_hash) = existing_zakura_hash {
            self.zs_delete(&zakura_height_by_hash, old_hash);
        }
        self.zs_delete(&zakura_hash_by_height, height);
        self.zs_delete(&zakura_header_by_height, height);
        self.zs_delete(&zakura_body_size_by_height, height);

        Ok(())
    }

    /// Prepare a database batch containing a contextually validated header range.
    #[cfg(test)]
    pub fn prepare_header_range_batch(
        &mut self,
        zebra_db: &ZebraDb,
        anchor: block::Hash,
        headers: &[Arc<block::Header>],
        body_sizes: &[u32],
    ) -> Result<block::Hash, CommitHeaderRangeError> {
        let roots = inferred_header_range_roots(zebra_db, anchor, headers.len())?;
        self.prepare_header_range_batch_with_roots(zebra_db, anchor, headers, body_sizes, &roots)
    }

    /// Prepare a database batch containing a contextually validated header range
    /// and one provisional tree-aux root per header.
    pub fn prepare_header_range_batch_with_roots(
        &mut self,
        zebra_db: &ZebraDb,
        anchor: block::Hash,
        headers: &[Arc<block::Header>],
        body_sizes: &[u32],
        tree_aux_roots: &[BlockCommitmentRoots],
    ) -> Result<block::Hash, CommitHeaderRangeError> {
        if headers.is_empty() {
            return Err(CommitHeaderRangeError::EmptyRange);
        }

        if headers.len() != body_sizes.len() {
            return Err(CommitHeaderRangeError::BodySizeCountMismatch {
                headers: headers.len(),
                body_sizes: body_sizes.len(),
            });
        }

        if headers.len() != tree_aux_roots.len() {
            return Err(CommitHeaderRangeError::TreeAuxRootCountMismatch {
                headers: headers.len(),
                roots: tree_aux_roots.len(),
            });
        }

        if headers.len() > MAX_HEADER_SYNC_HEIGHT_RANGE as usize {
            return Err(CommitHeaderRangeError::RangeTooLong {
                actual: headers.len(),
            });
        }

        let header_by_height = zebra_db.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
        let hash_by_height = zebra_db.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
        let height_by_hash = zebra_db.db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
        let body_size_by_height = zebra_db
            .db
            .cf_handle(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
            .unwrap();
        let roots_by_height = zebra_db
            .db
            .cf_handle(ZAKURA_HEADER_COMMITMENT_ROOTS_BY_HEIGHT)
            .unwrap();

        let anchor_height = zebra_db
            .header_height(anchor)
            .or_else(|| (anchor == zebra_db.network().genesis_hash()).then_some(block::Height(0)))
            .ok_or(CommitHeaderRangeError::UnknownAnchor { anchor })?;

        if anchor != zebra_db.network().genesis_hash()
            && zebra_db.header_hash(anchor_height) != Some(anchor)
        {
            return Err(CommitHeaderRangeError::UnknownAnchor { anchor });
        }

        let finalized_height = zebra_db.finalized_tip_height();
        let best_header_tip = zebra_db.best_header_tip().map(|(height, _)| height);
        let checkpoints = zebra_db.network().checkpoint_list();

        let mut recent_headers = zebra_db.recent_header_context(anchor_height);
        if recent_headers.is_empty() {
            if anchor == zebra_db.network().genesis_hash() && anchor_height == block::Height(0) {
                return Err(CommitHeaderRangeError::MissingGenesisAnchor { anchor });
            }
            return Err(CommitHeaderRangeError::UnknownAnchor { anchor });
        }

        let mut first_conflicting_height = None;
        let mut validated_headers = Vec::with_capacity(headers.len());

        for (index, header) in headers.iter().enumerate() {
            let offset =
                u32::try_from(index + 1).map_err(|_| CommitHeaderRangeError::HeightOverflow)?;
            let height = (anchor_height + i64::from(offset))
                .ok_or(CommitHeaderRangeError::HeightOverflow)?;
            let hash = block::Hash::from(&**header);
            let body_size = body_sizes[index];
            if let Some(roots) = tree_aux_roots.get(index) {
                if roots.height != height {
                    return Err(CommitHeaderRangeError::TreeAuxRootHeightMismatch {
                        expected_height: height,
                        root_height: roots.height,
                    });
                }
            }

            if let Some(expected) = checkpoints.hash(height) {
                if expected != hash {
                    return Err(CommitHeaderRangeError::CheckpointConflict {
                        height,
                        expected,
                        actual: hash,
                    });
                }
            }

            if let Some((_existing_hash, existing_header)) = zebra_db.header_by_height(height) {
                if existing_header != *header {
                    if finalized_height.is_some_and(|finalized_height| height <= finalized_height) {
                        return Err(CommitHeaderRangeError::ImmutableConflict { height });
                    }

                    if zebra_db.contains_body_at_height(height) {
                        return Err(CommitHeaderRangeError::ConflictingFullBlockHeader { height });
                    }

                    if let Some(best_header_tip) = best_header_tip {
                        if best_header_tip.0.saturating_sub(height.0) >= MAX_BLOCK_REORG_HEIGHT {
                            return Err(CommitHeaderRangeError::ReorgTooDeep {
                                height,
                                best_header_tip,
                            });
                        }
                    }

                    first_conflicting_height.get_or_insert(height);
                }
            }

            check::header_is_valid_for_recent_chain(
                header,
                height
                    .previous()
                    .map_err(|_| CommitHeaderRangeError::HeightOverflow)?,
                &zebra_db.network(),
                recent_headers.iter().copied(),
            )?;

            recent_headers.insert(0, (header.difficulty_threshold, header.time));
            recent_headers.truncate(check::difficulty::POW_ADJUSTMENT_BLOCK_SPAN);

            validated_headers.push((height, hash, header, body_size));
        }

        // Before overwriting a conflicting header suffix, require the new range to
        // carry strictly more cumulative work than the chain it would replace. The
        // per-header checks above only validate each header's own difficulty
        // threshold and contextual difficulty; without this most-work gate, a
        // lower-work fork — for example a low-difficulty header flood built with
        // manipulated timestamps past the last checkpoint — could replace a longer,
        // higher-work header chain purely because it conflicts within the reorg
        // window, steering body-gap discovery off the real chain. Heights below
        // `first_conflicting_height` are shared by both chains, so comparing the
        // conflicting suffixes is equivalent to comparing total chain work.
        if let (Some(first_conflicting_height), Some(best_header_tip)) =
            (first_conflicting_height, best_header_tip)
        {
            let mut existing_work = PartialCumulativeWork::zero();
            for height in first_conflicting_height.0..=best_header_tip.0 {
                if let Some((_hash, existing_header)) =
                    zebra_db.header_by_height(block::Height(height))
                {
                    // A stored header passed difficulty validation when committed, so
                    // its threshold always converts to work; skip defensively if not.
                    if let Some(work) = existing_header.difficulty_threshold.to_work() {
                        existing_work += work;
                    }
                }
            }

            let mut new_work = PartialCumulativeWork::zero();
            for (height, _hash, header, _body_size) in &validated_headers {
                if *height >= first_conflicting_height {
                    if let Some(work) = header.difficulty_threshold.to_work() {
                        new_work += work;
                    }
                }
            }

            if new_work <= existing_work {
                return Err(CommitHeaderRangeError::LowerWorkConflict {
                    height: first_conflicting_height,
                    existing_work: existing_work.as_u128(),
                    new_work: new_work.as_u128(),
                });
            }
        }

        if let (Some(first_conflicting_height), Some(best_header_tip)) =
            (first_conflicting_height, best_header_tip)
        {
            for height in first_conflicting_height.0..=best_header_tip.0 {
                let height = block::Height(height);

                if zebra_db.contains_body_at_height(height) {
                    return Err(CommitHeaderRangeError::ConflictingFullBlockHeader { height });
                }

                if let Some(old_hash) = zebra_db.zakura_header_hash(height) {
                    self.zs_delete(&height_by_hash, old_hash);
                }

                self.zs_delete(&hash_by_height, height);
                self.zs_delete(&header_by_height, height);
                self.zs_delete(&body_size_by_height, height);
                self.zs_delete(&roots_by_height, height);
            }
        }

        for (index, (height, hash, header, body_size)) in validated_headers.into_iter().enumerate()
        {
            let same_header = zebra_db.zakura_header_hash(height) == Some(hash);
            let advertised_body_size = match (
                same_header,
                zebra_db.advertised_body_size(height),
                AdvertisedBodySize::new(body_size).map(AdvertisedBodySize::get),
            ) {
                (true, existing, Some(new)) => Some(existing.unwrap_or(0).max(new)),
                (true, existing, None) => existing,
                (false, _existing, new) => new,
            };

            self.zs_insert(&header_by_height, height, header);
            self.zs_insert(&hash_by_height, height, hash);
            self.zs_insert(&height_by_hash, hash, height);
            if let Some(body_size) = advertised_body_size.and_then(AdvertisedBodySize::new) {
                self.zs_insert(&body_size_by_height, height, body_size);
            } else {
                self.zs_delete(&body_size_by_height, height);
            }

            if let Some(roots) = tree_aux_roots.get(index) {
                self.zs_insert(
                    &roots_by_height,
                    height,
                    CommitmentRootsByHeight {
                        sapling: roots.sapling_root,
                        orchard: roots.orchard_root,
                    },
                );
            }
        }

        Ok(block::Hash::from(
            &**headers.last().expect("headers is non-empty"),
        ))
    }

    /// Deletes the block header at `height`.
    ///
    /// This is only used by rollback tests to prove modern rollback targets do not need pre-upgrade
    /// blocks for note commitment tree replay.
    #[cfg(test)]
    pub fn delete_block_header(&mut self, zebra_db: &ZebraDb, height: Height) {
        let block_header_by_height = zebra_db.db.cf_handle("block_header_by_height").unwrap();
        self.zs_delete(&block_header_by_height, height);
    }
}
