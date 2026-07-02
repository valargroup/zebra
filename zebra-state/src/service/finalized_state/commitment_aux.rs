//! Payload types and the producer/serving half of the verified-commitment-trees
//! fast path (`docs/design/verified-commitment-trees.md`).
//!
//! The fast path consumes per-block Sapling/Orchard roots and a final frontier at the
//! last checkpoint. This module provides the **producer** half
//! ([`produce_block_roots`] / [`produce_final_frontiers`]): deriving that payload from
//! an existing database's per-height trees.
//!
//! [`serve_block_roots`] is the serving entry point: it stitches the
//! `commitment_roots_by_height` index with the per-height trees at the upgrade height,
//! so upgraded and fast-synced nodes keep serving one gap-free `tree_aux` payload.

use std::{fmt, sync::Arc};

use thiserror::Error;
use zebra_chain::{
    block::{self, merkle::AuthDataRoot},
    orchard, sapling, sprout,
};

use super::{FromDisk, IntoDisk, ZebraDb};

/// Per-block verified commitment roots
pub(super) use zebra_chain::parallel::commitment_aux::BlockCommitmentRoots;

/// The verified final note-commitment frontiers at the last checkpoint height.
///
/// Verified-commitment-tree (VCT) mode skips the per-block frontier recompute below the checkpoint, so the
/// running Sapling/Orchard frontiers are never advanced. To let post-checkpoint
/// semantic verification resume, the real frontiers at the checkpoint are supplied
/// here, verified (`frontier.root() == the verified root at the checkpoint`), and
/// written as the tip treestate at the last checkpoint. Subtree tips are not carried: the
/// resuming chain recomputes them from the frontier position.
#[derive(Clone, Debug)]
pub(super) struct FinalFrontiers {
    pub(super) height: block::Height,
    pub(super) sapling: Arc<sapling::tree::NoteCommitmentTree>,
    pub(super) orchard: Arc<orchard::tree::NoteCommitmentTree>,
    pub(super) sprout: Arc<sprout::tree::NoteCommitmentTree>,
}

/// Errors producing [`FinalFrontiers`] from a finalized database.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FinalFrontiersGenerationError {
    /// The database has no Sapling tree at the requested height.
    #[error("missing Sapling final frontier tree at height {height:?}")]
    MissingSaplingTree {
        /// The requested final frontier height.
        height: block::Height,
    },

    /// The database has no Orchard tree at the requested height.
    #[error("missing Orchard final frontier tree at height {height:?}")]
    MissingOrchardTree {
        /// The requested final frontier height.
        height: block::Height,
    },
}

/// Errors parsing [`FinalFrontiers`] from the embedded/frontier-file byte format.
// The non-test consumer is the VCT embedded-frontier loader, which lands with the
// committer fast path in a follow-up increment; the round-trip test exercises it here.
#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum FinalFrontiersParseError {
    /// The input ended before the 4-byte height field.
    MissingHeight {
        /// The total number of bytes in the input.
        actual_len: usize,
    },
    /// The input ended before a tree blob's 4-byte length prefix.
    MissingLength {
        /// The tree whose length prefix was being read.
        tree: &'static str,
        /// Byte offset where the length prefix starts.
        offset: usize,
        /// Bytes remaining from `offset`.
        remaining: usize,
    },
    /// A tree blob's length prefix points past the end of the input.
    TruncatedBlob {
        /// The tree whose blob was being read.
        tree: &'static str,
        /// Byte offset where the blob starts.
        offset: usize,
        /// Blob length from the prefix.
        expected_len: usize,
        /// Bytes remaining from `offset`.
        remaining: usize,
    },
    /// A tree blob's length prefix overflows `usize` arithmetic.
    LengthOverflow {
        /// The tree whose blob was being read.
        tree: &'static str,
        /// Byte offset where the blob starts.
        offset: usize,
        /// Blob length from the prefix.
        len: usize,
    },
    /// The parser consumed all expected fields, but extra bytes remained.
    TrailingBytes {
        /// Byte offset where the trailing data starts.
        offset: usize,
        /// Number of trailing bytes.
        trailing_len: usize,
    },
}

impl fmt::Display for FinalFrontiersParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FinalFrontiersParseError::MissingHeight { actual_len } => write!(
                f,
                "missing final frontier height: expected 4 bytes, got {actual_len}"
            ),
            FinalFrontiersParseError::MissingLength {
                tree,
                offset,
                remaining,
            } => write!(
                f,
                "missing {tree} frontier length prefix at byte {offset}: expected 4 bytes, got {remaining}"
            ),
            FinalFrontiersParseError::TruncatedBlob {
                tree,
                offset,
                expected_len,
                remaining,
            } => write!(
                f,
                "truncated {tree} frontier blob at byte {offset}: length prefix says {expected_len} bytes, but only {remaining} remain"
            ),
            FinalFrontiersParseError::LengthOverflow { tree, offset, len } => write!(
                f,
                "{tree} frontier blob length overflows at byte {offset}: {len} bytes"
            ),
            FinalFrontiersParseError::TrailingBytes {
                offset,
                trailing_len,
            } => write!(
                f,
                "unexpected trailing final frontier bytes at byte {offset}: {trailing_len} bytes"
            ),
        }
    }
}

impl std::error::Error for FinalFrontiersParseError {}

impl FinalFrontiers {
    /// Serialize to the embedded byte format: height (u32 LE), then sapling, orchard,
    /// and sprout trees, each as `u32`-LE-length-prefixed `IntoDisk` bytes. Used to
    /// create embedded or test final-frontier fixtures.
    pub(super) fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.height.0.to_le_bytes());
        let blobs: [Vec<u8>; 3] = [
            IntoDisk::as_bytes(&*self.sapling),
            IntoDisk::as_bytes(&*self.orchard),
            IntoDisk::as_bytes(&*self.sprout),
        ];
        for blob in blobs {
            let len = u32::try_from(blob.len()).expect("note commitment tree fits in u32 bytes");
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&blob);
        }
        out
    }

    /// Parse the embedded byte format written by [`Self::to_bytes`].
    // The non-test consumer is the VCT embedded-frontier loader, which lands with the
    // committer fast path in a follow-up increment; the round-trip test exercises it here.
    #[allow(dead_code)]
    pub(super) fn from_bytes(bytes: &[u8]) -> Result<Self, FinalFrontiersParseError> {
        let height_bytes = bytes
            .get(0..4)
            .ok_or(FinalFrontiersParseError::MissingHeight {
                actual_len: bytes.len(),
            })?;
        let height_bytes: [u8; 4] =
            height_bytes
                .try_into()
                .map_err(|_| FinalFrontiersParseError::MissingHeight {
                    actual_len: bytes.len(),
                })?;
        let height = block::Height(u32::from_le_bytes(height_bytes));

        // Read three `u32`-length-prefixed blobs starting after the height.
        let mut cursor: usize = 4;
        let mut next_blob = |tree: &'static str| -> Result<Vec<u8>, FinalFrontiersParseError> {
            let len_end =
                cursor
                    .checked_add(4)
                    .ok_or(FinalFrontiersParseError::LengthOverflow {
                        tree,
                        offset: cursor,
                        len: 4,
                    })?;
            let len_bytes =
                bytes
                    .get(cursor..len_end)
                    .ok_or(FinalFrontiersParseError::MissingLength {
                        tree,
                        offset: cursor,
                        remaining: bytes.len().saturating_sub(cursor),
                    })?;
            let len_bytes: [u8; 4] =
                len_bytes
                    .try_into()
                    .map_err(|_| FinalFrontiersParseError::MissingLength {
                        tree,
                        offset: cursor,
                        remaining: bytes.len().saturating_sub(cursor),
                    })?;
            // Zebra's supported platforms have at least 32-bit `usize`, so every
            // u32 length prefix fits in memory indexes.
            let len = u32::from_le_bytes(len_bytes) as usize;
            cursor = len_end;
            let blob_end =
                cursor
                    .checked_add(len)
                    .ok_or(FinalFrontiersParseError::LengthOverflow {
                        tree,
                        offset: cursor,
                        len,
                    })?;
            let blob =
                bytes
                    .get(cursor..blob_end)
                    .ok_or(FinalFrontiersParseError::TruncatedBlob {
                        tree,
                        offset: cursor,
                        expected_len: len,
                        remaining: bytes.len().saturating_sub(cursor),
                    })?;
            cursor = blob_end;
            Ok(blob.to_vec())
        };
        let sapling = next_blob("sapling")?;
        let orchard = next_blob("orchard")?;
        let sprout = next_blob("sprout")?;

        if cursor != bytes.len() {
            return Err(FinalFrontiersParseError::TrailingBytes {
                offset: cursor,
                trailing_len: bytes.len() - cursor,
            });
        }

        Ok(FinalFrontiers {
            height,
            sapling: Arc::new(<sapling::tree::NoteCommitmentTree as FromDisk>::from_bytes(
                sapling,
            )),
            orchard: Arc::new(<orchard::tree::NoteCommitmentTree as FromDisk>::from_bytes(
                orchard,
            )),
            sprout: Arc::new(<sprout::tree::NoteCommitmentTree as FromDisk>::from_bytes(
                sprout,
            )),
        })
    }
}

/// Produce the per-block roots payload for `range` from `db`'s per-height trees.
///
/// Derives each root from the per-height note commitment tree.
pub(crate) fn produce_block_roots(
    db: &ZebraDb,
    range: std::ops::RangeInclusive<block::Height>,
) -> Vec<BlockCommitmentRoots> {
    let (start, end) = (range.start().0, range.end().0);
    let mut roots = Vec::new();
    for h in start..=end {
        let height = block::Height(h);
        let (Some(sapling), Some(orchard)) = (
            db.sapling_tree_by_height(&height),
            db.orchard_tree_by_height(&height),
        ) else {
            break;
        };
        // Below the upgrade height the serving index does not exist, so derive the
        // auth-data root and the shielded tx-counts from the locally stored block (this
        // archival node holds the body for these heights). Zero only if the body is somehow
        // absent, in which case the recipient simply re-fetches from a node that has it.
        let block = db.block(height.into());
        let (sapling_tx, orchard_tx, ironwood_tx, auth_data_root) = block
            .as_ref()
            .map(|block| {
                (
                    block.sapling_transactions_count(),
                    block.orchard_transactions_count(),
                    block.ironwood_transactions_count(),
                    block.auth_data_root(),
                )
            })
            .unwrap_or((0, 0, 0, AuthDataRoot::from([0u8; 32])));
        roots.push(BlockCommitmentRoots {
            height,
            sapling_root: sapling.root(),
            orchard_root: orchard.root(),
            // The Ironwood tree does not exist below Nu7, so its root is the empty-tree root
            // for every currently-servable height (no per-height Ironwood tree store yet).
            ironwood_root: zebra_chain::ironwood::tree::NoteCommitmentTree::default().root(),
            sapling_tx,
            orchard_tx,
            ironwood_tx,
            auth_data_root,
        });
    }
    roots
}

/// Serve the per-block roots for `range`, stitching the two sources at the upgrade height `U`.
///
/// The `commitment_roots_by_height` serving index only covers heights at and above `U` (the lowest
/// height this binary committed). Heights below `U` predate the index, so they are derived from the
/// per-height trees instead, and the two runs are concatenated. This is what lets a node that
/// upgraded mid-chain serve a request that straddles `U` as one gap-free batch, rather than the
/// short index-only prefix that would stall the client's minimum-progress check.
///
/// Both sources stop at the first absent height, so the result is always a contiguous run from
/// `range.start()`; a tree gap below `U` is served as the prefix collected so far without reaching
/// into the index. A database that never recorded `U` — a pre-index archive node — derives the
/// whole range from the trees, the original archive fallback.
pub(crate) fn serve_block_roots(
    db: &ZebraDb,
    range: std::ops::RangeInclusive<block::Height>,
) -> Vec<BlockCommitmentRoots> {
    // Below the VCT upgrade height, we use the per-height trees to derive the roots.
    let Some(upgrade) = db.vct_upgrade_height() else {
        return produce_block_roots(db, range);
    };

    let (start, end) = (*range.start(), *range.end());

    // Wholly at/above `U`: the VCT-specific index covers it. (`U == 0` for a node that fast-synced from
    // genesis takes this path for every request, never touching the absent per-height trees.)
    if start >= upgrade {
        return db.commitment_roots_by_height_range(range);
    }

    // Below `U`: derive the per-height-tree run up to `U - 1` (`start < upgrade` so `upgrade >= 1`).
    let trees_end = block::Height(end.0.min(upgrade.0 - 1));
    let mut roots = produce_block_roots(db, start..=trees_end);

    // Continue into the index only if the tree run is contiguous up to `U - 1`; a short run means a
    // gap below `U`, so serve it alone and let the client retry the remainder.
    if roots.last().map(|root| root.height) == Some(trees_end) && end >= upgrade {
        roots.extend(db.commitment_roots_by_height_range(upgrade..=end));
    }

    roots
}

/// Produce the final frontiers at `height` from `db`'s per-height trees.
///
/// Sprout is frozen far below any modern checkpoint, so the tip Sprout tree is the frontier at
/// `height`.
pub(super) fn produce_final_frontiers(
    db: &ZebraDb,
    height: block::Height,
) -> Result<FinalFrontiers, FinalFrontiersGenerationError> {
    let sapling = db
        .sapling_tree_by_height(&height)
        .ok_or(FinalFrontiersGenerationError::MissingSaplingTree { height })?;
    let orchard = db
        .orchard_tree_by_height(&height)
        .ok_or(FinalFrontiersGenerationError::MissingOrchardTree { height })?;

    Ok(FinalFrontiers {
        height,
        sapling,
        orchard,
        sprout: db.sprout_tree_for_tip(),
    })
}

/// Produce serialized final-frontier bytes for the checkpoint handoff at `height`.
///
/// These bytes use the same format as the embedded `mainnet-frontier.bin` file consumed by
/// the VCT frontier loader (landing with the committer fast path in a follow-up increment).
pub fn produce_final_frontiers_bytes(
    db: &ZebraDb,
    height: block::Height,
) -> Result<Vec<u8>, FinalFrontiersGenerationError> {
    Ok(produce_final_frontiers(db, height)?.to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The final-frontier serialization round-trips: parsed frontiers carry the same
    /// height and tree roots as the originals.
    #[test]
    fn final_frontiers_bytes_round_trips() {
        let frontiers = FinalFrontiers {
            height: block::Height(1_687_200),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            sprout: Arc::new(Default::default()),
        };

        let parsed =
            FinalFrontiers::from_bytes(&frontiers.to_bytes()).expect("frontiers should parse");

        assert_eq!(parsed.height, frontiers.height, "height round-trips");
        assert_eq!(
            parsed.sapling.root(),
            frontiers.sapling.root(),
            "sapling frontier round-trips"
        );
        assert_eq!(
            parsed.orchard.root(),
            frontiers.orchard.root(),
            "orchard frontier round-trips"
        );
        assert_eq!(
            parsed.sprout.root(),
            frontiers.sprout.root(),
            "sprout frontier round-trips"
        );
    }
}
