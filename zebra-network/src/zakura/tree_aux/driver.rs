//! Client-side `tree_aux` root fetching.
//!
//! [`fetch_roots`] pulls verified per-block commitment roots for a height range from
//! connected peers and hands each contiguous batch to a sink. The node stages those
//! batches until the requested range succeeds, then publishes the complete range to
//! `PeerSource`, so the fast committer reads peer-fetched roots through the same seam
//! it uses for the fixture. Run *ahead of* body download (header-sync-aligned), so a
//! range's coverage is known before it is committed.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use zebra_chain::{block, parallel::commitment_aux::BlockCommitmentRoots};

use super::{TreeAuxMessage, MAX_TA_ROOTS_PER_REQUEST, ZAKURA_STREAM_TREE_AUX};
use crate::{
    zakura::{ZakuraPeerHandle, ZakuraSupervisorHandle},
    BoxError,
};

static NEXT_TREE_AUX_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_TREE_AUX_PEER_OFFSET: AtomicU64 = AtomicU64::new(0);

/// Per-peer timeout for one bounded `tree_aux` root request.
///
/// A stalled peer must not block the client from trying other connected peers for the same
/// sub-range, especially when a frozen-frontier refetch is needed to unblock the committer.
const TREE_AUX_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Fetch verified per-block commitment roots for `[start, end]` from connected peers,
/// delivering each received contiguous batch to `sink`.
///
/// Requests are bounded by [`MAX_TA_ROOTS_PER_REQUEST`]; the fetch advances by what each
/// peer returns and stops once `end` is covered. Returns an error if no peer is connected
/// or every available peer reports an unavailable/malformed sub-range. Before the VCT
/// frontier is frozen, callers can leave that range on the legacy path; after freeze, the
/// committer parks the block and asks the driver to retry the missing root.
///
/// No trust is placed in the peer here: the committer re-verifies every delivered root
/// against its own checkpoint-committed headers before folding it in.
pub async fn fetch_roots<F>(
    supervisor: &ZakuraSupervisorHandle,
    start: block::Height,
    end: block::Height,
    mut sink: F,
) -> Result<(), BoxError>
where
    F: FnMut(Vec<BlockCommitmentRoots>),
{
    let mut next = start.0;
    while next <= end.0 {
        let count = (end.0 - next + 1).min(MAX_TA_ROOTS_PER_REQUEST);
        let mut handles = supervisor.outbound_peer_handles().await;
        if handles.is_empty() {
            return Err("no connected tree_aux peer".into());
        }

        let offset = rotated_peer_offset(
            NEXT_TREE_AUX_PEER_OFFSET.fetch_add(1, Ordering::Relaxed),
            handles.len(),
        );
        handles.rotate_left(offset);

        let mut last_error = None;
        let mut fetched_roots = None;
        for handle in handles {
            match request_roots_from_peer(&handle, next, count).await {
                Ok(roots) => {
                    fetched_roots = Some(roots);
                    break;
                }
                Err(error) => {
                    tracing::debug!(
                        peer_id = ?handle.peer_id(),
                        ?error,
                        height = next,
                        count,
                        "tree_aux peer could not serve roots, trying another peer"
                    );
                    last_error = Some(error);
                }
            }
        }

        let roots = fetched_roots.ok_or_else(|| -> BoxError {
            last_error.unwrap_or_else(|| "no connected tree_aux peer could serve roots".into())
        })?;
        let last = roots
            .last()
            .expect("validated tree_aux roots are non-empty")
            .height
            .0;
        sink(roots);
        next = last
            .checked_add(1)
            .ok_or_else(|| -> BoxError { "tree_aux root height overflow".into() })?;
    }

    Ok(())
}

/// Return the left-rotation offset for one root request over `peer_count` peers.
///
/// The caller passes a monotonically increasing request index. Taking it modulo the
/// current peer count spreads retries across the available outbound peers without
/// requiring persistent per-peer state.
fn rotated_peer_offset(request_index: u64, peer_count: usize) -> usize {
    let peer_count = u64::try_from(peer_count).expect("peer handle count fits in u64 for rotation");
    let offset = request_index % peer_count;
    usize::try_from(offset).expect("peer rotation offset is less than the peer handle count")
}

/// Request one bounded root batch from `handle` and verify the batch shape before returning it.
///
/// This only validates transport framing and height contiguity. Root contents are still
/// untrusted until the state committer verifies them against checkpoint-committed headers.
async fn request_roots_from_peer(
    handle: &ZakuraPeerHandle,
    next: u32,
    count: u32,
) -> Result<Vec<BlockCommitmentRoots>, BoxError> {
    let request = TreeAuxMessage::GetRoots {
        start_height: block::Height(next),
        count,
    }
    .encode_frame()
    .map_err(|error| -> BoxError { format!("encoding GetRoots failed: {error}").into() })?;

    let request_id = NEXT_TREE_AUX_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let frames = tokio::time::timeout(
        TREE_AUX_REQUEST_TIMEOUT,
        handle.request(
            ZAKURA_STREAM_TREE_AUX,
            request_id,
            request.message_type,
            request.flags,
            request.payload,
        ),
    )
    .await
    .map_err(|_| -> BoxError {
        format!("tree_aux peer request timed out after {TREE_AUX_REQUEST_TIMEOUT:?}").into()
    })??;

    let frame = frames
        .into_iter()
        .next()
        .ok_or_else(|| -> BoxError { "empty tree_aux response".into() })?;

    match TreeAuxMessage::decode_frame(frame).map_err(|error| -> BoxError {
        format!("decoding tree_aux response failed: {error}").into()
    })? {
        TreeAuxMessage::Roots { roots } => {
            validate_contiguous_roots(&roots, next, count)?;
            Ok(roots)
        }
        TreeAuxMessage::RangeUnavailable { .. } => {
            Err(format!("peer cannot serve tree_aux roots at height {next}").into())
        }
        other => Err(format!("unexpected tree_aux response to GetRoots: {other:?}").into()),
    }
}

/// Validate that a peer returned a non-empty, in-order prefix of the requested range.
///
/// Short batches are allowed so a peer can make partial progress, but each returned
/// height must be exactly `next..next + roots.len()`. Gaps are rejected here instead of
/// being inserted into the peer root cache and surfacing later as frozen-frontier misses.
fn validate_contiguous_roots(
    roots: &[BlockCommitmentRoots],
    next: u32,
    count: u32,
) -> Result<(), BoxError> {
    if roots.is_empty() {
        return Err("peer returned an empty Roots batch".into());
    }

    let root_count =
        u32::try_from(roots.len()).expect("tree_aux root batch length fits in u32 after decoding");
    if root_count > count {
        return Err(format!("peer returned {root_count} roots for a {count}-root request").into());
    }

    for (index, root) in roots.iter().enumerate() {
        let index =
            u32::try_from(index).expect("tree_aux root batch index fits in u32 after decoding");
        let expected = next
            .checked_add(index)
            .ok_or_else(|| -> BoxError { "tree_aux expected root height overflow".into() })?;
        if root.height.0 != expected {
            return Err(format!(
                "peer returned non-contiguous tree_aux roots: expected {:?}, got {:?}",
                block::Height(expected),
                root.height
            )
            .into());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use zebra_chain::{orchard, sapling};

    use super::*;

    fn root_at(height: u32) -> BlockCommitmentRoots {
        BlockCommitmentRoots {
            height: block::Height(height),
            sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
            orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
        }
    }

    #[test]
    fn rotated_peer_offset_wraps_by_peer_count() {
        assert_eq!(rotated_peer_offset(0, 3), 0);
        assert_eq!(rotated_peer_offset(1, 3), 1);
        assert_eq!(rotated_peer_offset(2, 3), 2);
        assert_eq!(rotated_peer_offset(3, 3), 0);
        assert_eq!(rotated_peer_offset(4, 3), 1);
    }

    #[test]
    fn validate_contiguous_roots_accepts_short_prefix() -> Result<(), BoxError> {
        let roots = vec![root_at(10), root_at(11)];

        validate_contiguous_roots(&roots, 10, 4)?;

        Ok(())
    }

    #[test]
    fn validate_contiguous_roots_rejects_empty_batch() {
        let roots = [];

        assert!(
            validate_contiguous_roots(&roots, 10, 4).is_err(),
            "empty tree_aux root batches do not advance the fetch cursor"
        );
    }

    #[test]
    fn validate_contiguous_roots_rejects_too_many_roots() {
        let roots = vec![root_at(10), root_at(11), root_at(12)];

        assert!(
            validate_contiguous_roots(&roots, 10, 2).is_err(),
            "peers must not return more roots than requested"
        );
    }

    #[test]
    fn validate_contiguous_roots_rejects_gaps() {
        let roots = vec![root_at(10), root_at(12)];

        assert!(
            validate_contiguous_roots(&roots, 10, 4).is_err(),
            "gapped tree_aux root batches must be retried with another peer"
        );
    }
}
