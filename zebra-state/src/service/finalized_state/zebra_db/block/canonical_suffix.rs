//! The single owner of zakura header-store chain membership:
//! [`DiskWriteBatch::set_canonical_suffix`] (REORG_PLAN Pillar 1).
//!
//! The zakura header store is five column families acting as a replicated
//! view of the canonical header chain above the finalized tip. Historically
//! it was mutated by four independent writers, each with its own bounded
//! delete loop; every proven corruption class (unlinked anchors, re-inserts
//! below the body tip, unlinked seeds, stranded suffixes) came from one of
//! those writers preserving the store invariants only under assumptions
//! about the others.
//!
//! This module replaces the per-writer delete/insert logic with one
//! primitive: *replace the suffix above a fork point*. Deletion is
//! total-above-fork by construction — it iterates the actual on-disk rows of
//! every column family, never a cached or computed tip — so stranding is
//! impossible. Linkage is a hard precondition, not a validation step callers
//! can forget. Committed heights are refused for deletes and inserts alike.
//!
//! Callers stay responsible for *policy*: contextual validation, checkpoint
//! conflicts, the cumulative-work gate, and reorg-depth limits all happen
//! before the primitive is invoked. The primitive owns *mechanism*: which
//! rows exist after a suffix replacement, in all five column families, in
//! one atomic batch.

use std::sync::Arc;

use zebra_chain::block::{self, Height};

use super::{
    AdvertisedBodySize, ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT, ZAKURA_HEADER_BY_HEIGHT,
    ZAKURA_HEADER_HASH_BY_HEIGHT, ZAKURA_HEADER_HEIGHT_BY_HASH,
};
use crate::{
    error::{CommitHeaderRangeError, StoreIncoherentError},
    service::finalized_state::{
        disk_db::{DiskWriteBatch, WriteDisk},
        disk_format::shielded::CommitmentRootsByHeight,
        zebra_db::ZebraDb,
        COMMITMENT_ROOTS_BY_HEIGHT,
    },
};

/// One header row of a new canonical suffix.
///
/// The row's height is implied by its position: `fork_point + 1 + index`.
/// Its hash is computed from the header inside the primitive, so a caller
/// cannot stage a hash that disagrees with the stored header.
#[derive(Clone, Debug)]
pub(crate) struct CanonicalHeaderRow {
    /// The header to store.
    pub header: Arc<block::Header>,

    /// The advisory body-size hint, if known. Not consensus data; `None`
    /// stores no row (and removes any stale hint left at this height).
    pub advertised_body_size: Option<u32>,

    /// Provisional commitment roots for this height, if the caller has them.
    /// `None` stores no row (the seed path has no roots for its block).
    pub roots: Option<CommitmentRootsByHeight>,
}

impl DiskWriteBatch {
    /// Replaces the zakura header-store suffix above `fork_point` with
    /// `new_rows`, in this batch.
    ///
    /// This is the **only** way any path may change which headers the zakura
    /// store considers canonical above a height. In one atomic batch it:
    ///
    /// 1. deletes every row strictly above `fork_point` in all five header
    ///    column families — iterating each family's actual on-disk rows, so
    ///    stranded rows above gaps are removed too — including every
    ///    `height_by_hash` entry pointing above the fork (displaced and
    ///    already-orphaned entries alike), then
    /// 2. writes `new_rows` at consecutive heights starting at
    ///    `fork_point + 1`.
    ///
    /// # Preconditions (hard errors, nothing staged on failure)
    ///
    /// - `fork_point` must be at or above the finalized tip
    ///   ([`CommitHeaderRangeError::ImmutableConflict`]): committed heights
    ///   are immutable through this primitive, which also guarantees no
    ///   insert lands at a committed height (the re-delivery bug shape).
    /// - When `new_rows` is non-empty, the stored header row at
    ///   `fork_point.0` must be `fork_point.1`
    ///   ([`StoreIncoherentError::BijectionMismatch`]): the caller's fork
    ///   decision must describe the store being mutated.
    /// - `new_rows[0]` must link to `fork_point.1` and every row to its
    ///   predecessor ([`CommitHeaderRangeError::UnlinkedRange`]): linkage is
    ///   structural here, not a caller obligation.
    /// - No height above `fork_point` may hold a committed full block body
    ///   ([`CommitHeaderRangeError::ConflictingFullBlockHeader`]). Bodies
    ///   above the finalized tip cannot exist, so this is a safety interlock:
    ///   reaching it means the caller's orchestration is buggy, and the
    ///   switch aborts loudly instead of deleting a body's header row.
    ///
    /// `new_rows` may be empty: that truncates the store to `fork_point`
    /// (the body-commit path drops conflicting provisional descendants this
    /// way). An empty replacement skips the fork-row check, because the
    /// caller may be staging a new full-block row at the fork height in this
    /// same batch, which reads through `zebra_db` cannot see yet.
    #[allow(clippy::unwrap_in_result)]
    pub(crate) fn set_canonical_suffix(
        &mut self,
        zebra_db: &ZebraDb,
        fork_point: (Height, block::Hash),
        new_rows: &[CanonicalHeaderRow],
    ) -> Result<(), CommitHeaderRangeError> {
        let (fork_height, fork_hash) = fork_point;

        // Committed heights are immutable: the finalized tip is the highest
        // committed block, so requiring the fork at or above it keeps every
        // delete *and* insert strictly above committed history (valid on
        // pruned stores too, where bodies are absent but heights committed).
        if let Some(finalized_tip_height) = zebra_db.finalized_tip_height() {
            if fork_height < finalized_tip_height {
                return Err(CommitHeaderRangeError::ImmutableConflict {
                    height: fork_height
                        .next()
                        .map_err(|_| CommitHeaderRangeError::HeightOverflow)?,
                });
            }
        }

        // The fork row named by the caller must be the row on disk: a
        // mismatch means the fork decision describes a different store state
        // than the one being mutated.
        if !new_rows.is_empty() {
            let stored = zebra_db
                .header_hash(fork_height)
                .or_else(|| (fork_hash == zebra_db.network().genesis_hash()).then_some(fork_hash));
            if stored != Some(fork_hash) {
                return Err(StoreIncoherentError::BijectionMismatch {
                    hash: fork_hash,
                    height: fork_height,
                    stored,
                }
                .into());
            }
        }

        // Structural linkage: the new suffix must chain from the fork row.
        let mut expected_parent = fork_hash;
        let mut hashes = Vec::with_capacity(new_rows.len());
        for (index, row) in new_rows.iter().enumerate() {
            let offset =
                u32::try_from(index + 1).map_err(|_| CommitHeaderRangeError::HeightOverflow)?;
            let height =
                (fork_height + i64::from(offset)).ok_or(CommitHeaderRangeError::HeightOverflow)?;

            if row.header.previous_block_hash != expected_parent {
                return Err(CommitHeaderRangeError::UnlinkedRange {
                    height,
                    expected_parent,
                    actual_parent: row.header.previous_block_hash,
                });
            }

            let hash = block::Hash::from(&*row.header);
            expected_parent = hash;
            hashes.push((height, hash));
        }

        let header_cf = zebra_db.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
        let hash_cf = zebra_db.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
        let height_by_hash_cf = zebra_db.db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
        let body_size_cf = zebra_db
            .db
            .cf_handle(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
            .unwrap();
        let roots_cf = zebra_db.db.cf_handle(COMMITMENT_ROOTS_BY_HEIGHT).unwrap();

        let Ok(delete_from) = fork_height.next() else {
            // The fork is at the maximum height: nothing can exist above it,
            // and the linkage loop already rejected any new rows.
            return Ok(());
        };

        // Total-above-fork deletion: walk the actual on-disk rows of every
        // column family from the fork upward. No cached or computed tip
        // bounds the loops, so rows stranded above gaps are deleted too.
        //
        // The committed-body interlock runs on the stored hash rows *and*
        // the insert heights before anything is staged.
        let stored_hash_rows: Vec<(Height, block::Hash)> = zebra_db
            .db
            .zs_forward_range_iter(&hash_cf, delete_from..)
            .collect();
        for &(height, _) in &stored_hash_rows {
            if zebra_db.contains_body_at_height(height) {
                return Err(CommitHeaderRangeError::ConflictingFullBlockHeader { height });
            }
        }
        for &(height, _) in &hashes {
            if zebra_db.contains_body_at_height(height) {
                return Err(CommitHeaderRangeError::ConflictingFullBlockHeader { height });
            }
        }

        let mut deleted_rows = 0;
        for (height, _displaced_hash) in stored_hash_rows {
            self.zs_delete(&hash_cf, height);
            deleted_rows += 1;
        }

        // The reverse index is hash-keyed, so deriving its deletions from the
        // hash rows would miss entries already orphaned by earlier damage (a
        // deleted hash row leaves its reverse entry behind). Scan the whole
        // reverse index — it is frontier-sized — and delete every entry
        // pointing above the fork, displaced and orphaned alike.
        for (hash, points_at) in zebra_db
            .db
            .zs_forward_range_iter::<_, block::Hash, Height, _>(&height_by_hash_cf, ..)
        {
            if points_at > fork_height {
                self.zs_delete(&height_by_hash_cf, hash);
                deleted_rows += 1;
            }
        }
        for (height, _) in zebra_db
            .db
            .zs_forward_range_iter::<_, Height, Arc<block::Header>, _>(&header_cf, delete_from..)
        {
            self.zs_delete(&header_cf, height);
            deleted_rows += 1;
        }
        for (height, _) in zebra_db
            .db
            .zs_forward_range_iter::<_, Height, AdvertisedBodySize, _>(&body_size_cf, delete_from..)
        {
            self.zs_delete(&body_size_cf, height);
            deleted_rows += 1;
        }
        for (height, _) in zebra_db
            .db
            .zs_forward_range_iter::<_, Height, CommitmentRootsByHeight, _>(
                &roots_cf,
                delete_from..,
            )
        {
            self.zs_delete(&roots_cf, height);
            deleted_rows += 1;
        }

        // A batch that deletes rows performs a header reorg: the write site
        // audits the store after committing it (the Pillar-3 hook).
        self.note_zakura_suffix_replacement(deleted_rows);

        // Write the new suffix.
        for (row, &(height, hash)) in new_rows.iter().zip(&hashes) {
            self.zs_insert(&header_cf, height, &row.header);
            self.zs_insert(&hash_cf, height, hash);
            self.zs_insert(&height_by_hash_cf, hash, height);
            if let Some(body_size) = row.advertised_body_size.and_then(AdvertisedBodySize::new) {
                self.zs_insert(&body_size_cf, height, body_size);
            }
            if let Some(roots) = &row.roots {
                self.zs_insert(&roots_cf, height, *roots);
            }
        }

        Ok(())
    }
}
