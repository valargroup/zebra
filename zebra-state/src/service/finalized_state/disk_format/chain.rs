//! Chain data serialization formats for finalized data.
//!
//! # Correctness
//!
//! [`crate::constants::state_database_format_version_in_code()`] must be incremented
//! each time the database format (column, serialization, etc) changes.

use std::collections::BTreeMap;

use bincode::Options;
use serde_big_array::BigArray;

use zebra_chain::{
    amount::NonNegative,
    block::Height,
    block_info::BlockInfo,
    history_tree::{HistoryTreeError, NonEmptyHistoryTree},
    parameters::{Network, NetworkKind},
    primitives::zcash_history,
    value_balance::ValueBalance,
};

use crate::service::finalized_state::disk_format::{FromDisk, IntoDisk};

impl IntoDisk for ValueBalance<NonNegative> {
    type Bytes = [u8; 48];

    fn as_bytes(&self) -> Self::Bytes {
        self.to_bytes()
    }
}

impl FromDisk for ValueBalance<NonNegative> {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        ValueBalance::from_bytes(bytes.as_ref()).expect("ValueBalance should be parsable")
    }
}

// The following implementations for history trees use `serde` and
// `bincode`. `serde` serializations depend on the inner structure of the type.
// They should not be used in new code. (This is an issue for any derived serialization format.)
//
// We explicitly use `bincode::DefaultOptions`  to disallow trailing bytes; see
// https://docs.rs/bincode/1.3.3/bincode/config/index.html#options-struct-vs-bincode-functions

#[derive(serde::Serialize, serde::Deserialize)]
pub struct HistoryTreeParts {
    network_kind: NetworkKind,
    size: u32,
    peaks: BTreeMap<u32, zcash_history::Entry>,
    current_height: Height,
}

/// Width in bytes of a history-tree `Entry` as serialized by pre-Ironwood Zebra versions, i.e.
/// `zcash_history::MAX_ENTRY_SIZE` before the Ironwood (`V3`) node-data fields were added.
///
/// The current width is `326` (V3 node data `MAX_NODE_DATA_SIZE = 317`, plus the 9-byte entry
/// header). V3 added two 32-byte Ironwood roots and a 9-byte compact tx count over V2, i.e. `73`
/// bytes, so the pre-Ironwood width was `326 - 73 = 253`. This is the *buffer* width, which is the
/// same for any pre-Ironwood tip regardless of whether its entries hold V1 (Heartwood/Canopy) or V2
/// (Nu5..Nu6.2) node data, because the buffer is always sized to the maximum node data of the code
/// that wrote it.
const OLD_MAX_ENTRY_SIZE: usize = 253;

/// The Ironwood (`V3`) node data added two 32-byte tree roots and a 9-byte compact tx count over the
/// pre-Ironwood (`V2`) layout, i.e. `73` bytes, which is exactly how much the entry buffer grew. If
/// the current width or this delta ever changes, [`OLD_MAX_ENTRY_SIZE`] must be updated in lockstep,
/// so this is pinned at compile time. (The 9-byte entry header is common to both widths.)
const _: () = {
    const V3_NODE_DATA_DELTA: usize = 32 + 32 + 9;
    assert!(
        zcash_history::MAX_ENTRY_SIZE - OLD_MAX_ENTRY_SIZE == V3_NODE_DATA_DELTA,
        "OLD_MAX_ENTRY_SIZE must be the current MAX_ENTRY_SIZE minus the V3 node-data delta",
    );
};

/// A mirror of a single pre-Ironwood history-tree `Entry`: a fixed `OLD_MAX_ENTRY_SIZE`-byte buffer,
/// serialized by `bincode` exactly as the real `zcash_history::Entry` was at the old width (a raw,
/// length-prefix-free fixed array, via `serde_big_array::BigArray`).
///
/// Also derives `Serialize` under `cfg(test)` so tests can synthesize a genuine old-format blob
/// (the exact bytes a pre-Ironwood Zebra wrote) by re-emitting current-format peaks at this
/// narrower width.
#[derive(serde::Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct OldEntry {
    #[serde(with = "BigArray")]
    inner: [u8; OLD_MAX_ENTRY_SIZE],
}

/// A mirror of [`HistoryTreeParts`] with the *old* (pre-Ironwood) entry width, used only to read an
/// entry written by an older Zebra version.
///
/// Every field has the same type and order as [`HistoryTreeParts`] except `peaks`, whose values are
/// [`OldEntry`] (253-byte buffers) instead of `zcash_history::Entry` (326-byte buffers). Because
/// `bincode::DefaultOptions` is not self-describing and encodes a fixed `[u8; N]` as exactly `N`
/// raw bytes, the *only* on-disk difference between an old and a current `HistoryTreeParts` blob is
/// this per-entry width; deserializing with the old width therefore reads a pre-Ironwood blob with
/// no trailing bytes (`DefaultOptions` disallows trailing bytes, so any mismatch is rejected).
#[derive(serde::Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct OldHistoryTreeParts {
    network_kind: NetworkKind,
    size: u32,
    peaks: BTreeMap<u32, OldEntry>,
    current_height: Height,
}

/// Re-encodes a pre-Ironwood tip history-tree blob into the current `Entry` format, returning the
/// new `bincode` bytes (what [`HistoryTreeParts::as_bytes`] would produce), or `None` if `raw` is
/// not a readable old-format blob.
///
/// This is the in-place repair used when the historical blocks a from-blocks rebuild needs are
/// missing (a database pruned before the Ironwood bump). For a pre-Ironwood chain every peak is V1
/// or V2 node data, which is consensus-fixed; widening each entry's buffer copies that data verbatim
/// and only changes the trailing zero padding, so the re-encoded `peaks` (and hence the MMR root)
/// are identical to what a from-blocks rebuild or a fresh sync produces.
pub(crate) fn reencode_old_format_history_tree_parts(raw: &[u8]) -> Option<Vec<u8>> {
    let old: OldHistoryTreeParts = bincode::DefaultOptions::new().deserialize(raw).ok()?;

    let mut peaks = BTreeMap::new();
    for (idx, old_entry) in old.peaks {
        // Copy the consensus-fixed prefix into a current-width, zero-padded `Entry`. The current
        // reader parses the meaningful prefix and ignores the extra padding, so this is byte-for-byte
        // equivalent in consensus terms to the original entry.
        let entry = zcash_history::Entry::from_smaller_format_bytes(&old_entry.inner)?;
        peaks.insert(idx, entry);
    }

    let parts = HistoryTreeParts {
        network_kind: old.network_kind,
        size: old.size,
        peaks,
        current_height: old.current_height,
    };

    Some(parts.as_bytes())
}

/// Test-only inverse of [`reencode_old_format_history_tree_parts`]: re-emits a current-format
/// [`HistoryTreeParts`] as the *narrower* pre-Ironwood (`OLD_MAX_ENTRY_SIZE`) blob, producing the
/// exact bytes an older Zebra version would have written for the same tip tree.
///
/// Each current `Entry` is a 326-byte zero-padded buffer; an entry holding pre-Ironwood (V1/V2) node
/// data uses at most `OLD_MAX_ENTRY_SIZE` of those bytes, the rest being zero padding. This narrows
/// each peak to its first `OLD_MAX_ENTRY_SIZE` bytes and serializes via the `Old*` mirror types, so
/// the result is byte-for-byte what the old code wrote (not a truncated current blob).
///
/// Returns `None` if any peak has a non-zero byte at or beyond `OLD_MAX_ENTRY_SIZE` — i.e. it holds
/// V3 (Ironwood) node data that does not fit the old width. That makes the synthesis self-checking:
/// it only ever produces a *faithful* old blob, never a lossily-truncated one.
#[cfg(test)]
pub(crate) fn encode_history_tree_parts_at_old_width(parts: &HistoryTreeParts) -> Option<Vec<u8>> {
    let mut peaks = BTreeMap::new();
    for (idx, entry) in &parts.peaks {
        // Serialize the current entry to its raw fixed-width bytes (BigArray => exactly
        // `MAX_ENTRY_SIZE` bytes, no length prefix), then keep only the pre-Ironwood prefix.
        let wide = bincode::DefaultOptions::new()
            .serialize(entry)
            .expect("serializing a history tree entry to a vec does not fail");

        // Refuse to narrow an entry that actually uses the extra V3 bytes: those would be lost.
        if wide[OLD_MAX_ENTRY_SIZE..].iter().any(|&b| b != 0) {
            return None;
        }

        let mut inner = [0u8; OLD_MAX_ENTRY_SIZE];
        inner.copy_from_slice(&wide[..OLD_MAX_ENTRY_SIZE]);
        peaks.insert(*idx, OldEntry { inner });
    }

    let old = OldHistoryTreeParts {
        network_kind: parts.network_kind,
        size: parts.size,
        peaks,
        current_height: parts.current_height,
    };

    Some(
        bincode::DefaultOptions::new()
            .serialize(&old)
            .expect("serializing the old-format history tree parts to a vec does not fail"),
    )
}

impl HistoryTreeParts {
    /// Converts [`HistoryTreeParts`] to a [`NonEmptyHistoryTree`].
    pub(crate) fn with_network(
        self,
        network: &Network,
    ) -> Result<NonEmptyHistoryTree, HistoryTreeError> {
        assert_eq!(
            self.network_kind,
            network.kind(),
            "history tree network kind should match current network"
        );

        NonEmptyHistoryTree::from_cache(network, self.size, self.peaks, self.current_height)
    }
}

impl From<&NonEmptyHistoryTree> for HistoryTreeParts {
    fn from(history_tree: &NonEmptyHistoryTree) -> Self {
        HistoryTreeParts {
            network_kind: history_tree.network().kind(),
            size: history_tree.size(),
            peaks: history_tree.peaks().clone(),
            current_height: history_tree.current_height(),
        }
    }
}

impl IntoDisk for HistoryTreeParts {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        bincode::DefaultOptions::new()
            .serialize(self)
            .expect("serialization to vec doesn't fail")
    }
}

impl FromDisk for HistoryTreeParts {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        bincode::DefaultOptions::new()
            .deserialize(bytes.as_ref())
            .expect("deserialization format should match the serialization format used by IntoDisk")
    }
}

impl IntoDisk for BlockInfo {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        self.value_pools()
            .as_bytes()
            .iter()
            .copied()
            .chain(self.size().to_le_bytes().iter().copied())
            .collect()
    }
}

impl FromDisk for BlockInfo {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        const LEGACY_VALUE_BALANCE_LEN: usize = 40;
        const VALUE_BALANCE_LEN: usize = 48;
        const BLOCK_SIZE_LEN: usize = 4;
        const LEGACY_BLOCK_INFO_LEN: usize = LEGACY_VALUE_BALANCE_LEN + BLOCK_SIZE_LEN;
        const BLOCK_INFO_LEN: usize = VALUE_BALANCE_LEN + BLOCK_SIZE_LEN;

        let bytes = bytes.as_ref();

        // We want to be forward-compatible, so this must work even if the
        // size of the buffer is larger than expected.
        match bytes.len() {
            BLOCK_INFO_LEN.. => {
                let value_pools =
                    ValueBalance::<NonNegative>::from_bytes(&bytes[..VALUE_BALANCE_LEN])
                        .expect("must work for 48 bytes");
                let size = u32::from_le_bytes(
                    bytes[VALUE_BALANCE_LEN..VALUE_BALANCE_LEN + BLOCK_SIZE_LEN]
                        .try_into()
                        .expect("must be 4 bytes"),
                );
                BlockInfo::new(value_pools, size)
            }
            LEGACY_BLOCK_INFO_LEN.. => {
                let value_pools =
                    ValueBalance::<NonNegative>::from_bytes(&bytes[..LEGACY_VALUE_BALANCE_LEN])
                        .expect("must work for 40 bytes");
                let size = u32::from_le_bytes(
                    bytes[LEGACY_VALUE_BALANCE_LEN..LEGACY_VALUE_BALANCE_LEN + BLOCK_SIZE_LEN]
                        .try_into()
                        .expect("must be 4 bytes"),
                );
                BlockInfo::new(value_pools, size)
            }
            _ => panic!("invalid format"),
        }
    }
}
