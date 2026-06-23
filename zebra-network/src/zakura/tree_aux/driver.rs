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

use futures::{
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};
use zebra_chain::{block, parallel::commitment_aux::BlockCommitmentRoots};

use super::{TreeAuxMessage, MAX_TA_ROOTS_PER_REQUEST, ZAKURA_STREAM_TREE_AUX};
use crate::{
    zakura::{ZakuraPeerHandle, ZakuraPeerId, ZakuraSupervisorHandle},
    BoxError,
};

static NEXT_TREE_AUX_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_TREE_AUX_PEER_OFFSET: AtomicU64 = AtomicU64::new(0);

/// Per-peer timeout for one bounded `tree_aux` root request.
///
/// A stalled peer must not block the client from trying other connected peers for the same
/// sub-range, especially when a frozen-frontier refetch is needed to unblock the committer.
const TREE_AUX_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Minimum fraction of a requested batch a peer must return to make useful progress.
///
/// This keeps honest partial responses and final small ranges valid, while rejecting lazy
/// one-root replies to large requests that would amplify the fetch into thousands of round trips.
const TREE_AUX_MIN_PROGRESS_DENOMINATOR: u32 = 4;

/// Number of peers queried concurrently for one bounded root request.
const TREE_AUX_HEDGE_PEERS: usize = 3;

/// Delay before adding another peer to a still-unanswered hedged request.
const TREE_AUX_HEDGE_DELAY: Duration = Duration::from_secs(2);

/// One contiguous root batch and the peer that supplied it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerRootBatch {
    /// Authenticated peer that supplied `roots`.
    pub peer_id: ZakuraPeerId,
    /// Contiguous roots returned by `peer_id`.
    pub roots: Vec<BlockCommitmentRoots>,
}

/// Caller preference for selecting a peer for one `tree_aux` request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PeerPreference {
    /// Try this peer in the normal rotated order.
    Normal,
    /// Keep this peer eligible, but try normal peers first.
    Demoted,
    /// Do not query this peer for this request.
    Excluded,
}

/// Result of one peer request attempt, reported to the caller's local peer policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerFetchEvent {
    /// Peer that was queried.
    pub peer_id: ZakuraPeerId,
    /// First requested height.
    pub height: block::Height,
    /// Requested root count.
    pub count: u32,
    /// Transport/request outcome for this peer.
    pub status: PeerFetchStatus,
}

/// Outcome for a peer queried by `tree_aux`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerFetchStatus {
    /// The peer supplied a well-shaped root batch.
    Success,
    /// The peer could not serve this request, but its content has not failed state verification.
    SoftFailure {
        /// Human-readable request error.
        error: String,
    },
}

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
    fetch_roots_with_peer(
        supervisor,
        start,
        end,
        |_| PeerPreference::Normal,
        |_| {},
        |batch| sink(batch.roots),
    )
    .await
}

/// Fetch roots like [`fetch_roots`], but preserve the supplying peer for each batch.
///
/// `peer_preference` lets callers apply local peer policy, for example excluding a peer whose
/// previously supplied root failed state verification, or demoting peers that repeatedly time out
/// or withhold roots. If every connected peer is excluded or unusable, the range remains
/// un-fetched and the caller retries later.
pub async fn fetch_roots_with_peer<F, S, E>(
    supervisor: &ZakuraSupervisorHandle,
    start: block::Height,
    end: block::Height,
    mut peer_preference: S,
    mut peer_event: E,
    mut sink: F,
) -> Result<(), BoxError>
where
    F: FnMut(PeerRootBatch),
    S: FnMut(&ZakuraPeerId) -> PeerPreference,
    E: FnMut(PeerFetchEvent),
{
    let mut next = start.0;
    while next <= end.0 {
        let count = (end.0 - next + 1).min(MAX_TA_ROOTS_PER_REQUEST);
        let handles = ordered_peer_handles(
            supervisor.outbound_peer_handles().await,
            NEXT_TREE_AUX_PEER_OFFSET.fetch_add(1, Ordering::Relaxed),
            &mut peer_preference,
        );
        if handles.is_empty() {
            return Err("no selectable connected tree_aux peer".into());
        }

        let (batch, last_error) =
            request_roots_from_hedged_peers(handles, next, count, &mut peer_event).await;

        let batch = batch.ok_or_else(|| -> BoxError {
            last_error.unwrap_or_else(|| "no connected tree_aux peer could serve roots".into())
        })?;
        let last = batch
            .roots
            .last()
            .expect("validated tree_aux roots are non-empty")
            .height
            .0;
        sink(batch);
        next = last
            .checked_add(1)
            .ok_or_else(|| -> BoxError { "tree_aux root height overflow".into() })?;
    }

    Ok(())
}

/// Request one batch from the first bounded group of preferred peers.
async fn request_roots_from_hedged_peers<E>(
    handles: Vec<ZakuraPeerHandle>,
    next: u32,
    count: u32,
    peer_event: &mut E,
) -> (Option<PeerRootBatch>, Option<BoxError>)
where
    E: FnMut(PeerFetchEvent),
{
    let mut last_error = None;
    let mut handles = handles.into_iter().take(TREE_AUX_HEDGE_PEERS);
    let mut requests = FuturesUnordered::new();
    let mut can_launch_more = true;

    if let Some(handle) = handles.next() {
        requests.push(request_roots_from_owned_peer(handle, next, count).boxed());
    }

    loop {
        if requests.is_empty() {
            return (None, last_error);
        }

        let result = if can_launch_more {
            let hedge_delay = tokio::time::sleep(TREE_AUX_HEDGE_DELAY);
            tokio::pin!(hedge_delay);

            tokio::select! {
                result = requests.next() => result,
                () = &mut hedge_delay => {
                    match handles.next() {
                        Some(handle) => {
                            requests.push(request_roots_from_owned_peer(handle, next, count).boxed());
                        }
                        None => can_launch_more = false,
                    }
                    continue;
                }
            }
        } else {
            requests.next().await
        };

        let Some((peer_id, result)) = result else {
            return (None, last_error);
        };

        if let Some(batch) =
            handle_peer_request_result(peer_id, result, next, count, peer_event, &mut last_error)
        {
            // Each Zakura request owns its own request/response stream; dropping `requests`
            // cancels only the losing hedged waits, not a shared per-peer response slot.
            return (Some(batch), last_error);
        }

        if requests.is_empty() && can_launch_more {
            match handles.next() {
                Some(handle) => {
                    requests.push(request_roots_from_owned_peer(handle, next, count).boxed());
                }
                None => can_launch_more = false,
            }
        }
    }
}

async fn request_roots_from_owned_peer(
    handle: ZakuraPeerHandle,
    next: u32,
    count: u32,
) -> (ZakuraPeerId, Result<Vec<BlockCommitmentRoots>, BoxError>) {
    let peer_id = handle.peer_id().clone();
    let result = request_roots_from_peer(&handle, next, count).await;
    (peer_id, result)
}

fn handle_peer_request_result<E>(
    peer_id: ZakuraPeerId,
    result: Result<Vec<BlockCommitmentRoots>, BoxError>,
    next: u32,
    count: u32,
    peer_event: &mut E,
    last_error: &mut Option<BoxError>,
) -> Option<PeerRootBatch>
where
    E: FnMut(PeerFetchEvent),
{
    match result {
        Ok(roots) => {
            peer_event(PeerFetchEvent {
                peer_id: peer_id.clone(),
                height: block::Height(next),
                count,
                status: PeerFetchStatus::Success,
            });
            Some(PeerRootBatch { peer_id, roots })
        }
        Err(error) => {
            peer_event(PeerFetchEvent {
                peer_id: peer_id.clone(),
                height: block::Height(next),
                count,
                status: PeerFetchStatus::SoftFailure {
                    error: error.to_string(),
                },
            });
            tracing::debug!(
                ?peer_id,
                ?error,
                height = next,
                count,
                "tree_aux peer could not serve roots, trying another peer"
            );
            *last_error = Some(error);
            None
        }
    }
}

/// Order peers by caller preference, preserving fallback to demoted peers.
fn ordered_peer_handles<S>(
    handles: Vec<ZakuraPeerHandle>,
    request_index: u64,
    peer_preference: &mut S,
) -> Vec<ZakuraPeerHandle>
where
    S: FnMut(&ZakuraPeerId) -> PeerPreference,
{
    ordered_peers(
        handles,
        request_index,
        |handle| handle.peer_id(),
        peer_preference,
    )
}

fn ordered_peers<T, S, I>(
    peers: Vec<T>,
    request_index: u64,
    peer_id: I,
    peer_preference: &mut S,
) -> Vec<T>
where
    S: FnMut(&ZakuraPeerId) -> PeerPreference,
    I: Fn(&T) -> &ZakuraPeerId,
{
    let mut normal = Vec::new();
    let mut demoted = Vec::new();

    for peer in peers {
        match peer_preference(peer_id(&peer)) {
            PeerPreference::Normal => normal.push(peer),
            PeerPreference::Demoted => demoted.push(peer),
            PeerPreference::Excluded => {}
        }
    }

    rotate_peer_group(&mut normal, request_index);
    rotate_peer_group(&mut demoted, request_index);
    normal.extend(demoted);
    normal
}

fn rotate_peer_group<T>(peers: &mut [T], request_index: u64) {
    if peers.is_empty() {
        return;
    }

    peers.rotate_left(rotated_peer_offset(request_index, peers.len()));
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
/// Short batches are allowed so a peer can make bounded partial progress, but each returned
/// height must be exactly `next..next + roots.len()` and a large request must return a useful
/// fraction of the range. Gaps are rejected here instead of being inserted into the peer root
/// cache and surfacing later as frozen-frontier misses.
fn validate_contiguous_roots(
    roots: &[BlockCommitmentRoots],
    next: u32,
    count: u32,
) -> Result<(), BoxError> {
    if roots.is_empty() {
        return Err("peer returned an empty Roots batch".into());
    }

    // Compare in `usize`: a peer's batch length is bounded by `MAX_TA_ROOTS_PER_REQUEST`, so
    // widening `count` (u32 -> usize is lossless on Zebra's >= 32-bit platforms) avoids a
    // fallible length cast in this `Result`-returning validator.
    let root_count = roots.len();
    let count_usize = count as usize;
    if root_count > count_usize {
        return Err(format!("peer returned {root_count} roots for a {count}-root request").into());
    }

    // Until peer Status narrows requests to each peer's servable range, this can reject an
    // honest peer whose range ends early. Treating that as a soft failure bounds slow-prefix
    // amplification without affecting small tail requests.
    let minimum_progress = count.div_ceil(TREE_AUX_MIN_PROGRESS_DENOMINATOR) as usize;
    if root_count < minimum_progress {
        return Err(format!(
            "peer returned {root_count} roots for a {count}-root request, below the {minimum_progress}-root minimum progress threshold"
        )
        .into());
    }

    // Walk a running expected height instead of an index cast, so each returned height must be
    // exactly `next, next + 1, …`.
    let mut expected = next;
    for root in roots {
        if root.height.0 != expected {
            return Err(format!(
                "peer returned non-contiguous tree_aux roots: expected {:?}, got {:?}",
                block::Height(expected),
                root.height
            )
            .into());
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| -> BoxError { "tree_aux expected root height overflow".into() })?;
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

    fn peer_id(seed: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![seed; 32]).expect("test peer id is within bounds")
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
    fn ordered_peers_try_normal_before_demoted() {
        let normal_a = peer_id(1);
        let demoted = peer_id(2);
        let normal_b = peer_id(3);
        let peers = vec![normal_a.clone(), demoted.clone(), normal_b.clone()];

        let ordered = ordered_peers(peers, 0, |peer_id| peer_id, &mut |peer_id| {
            if peer_id == &demoted {
                PeerPreference::Demoted
            } else {
                PeerPreference::Normal
            }
        });

        assert_eq!(
            ordered,
            vec![normal_a, normal_b, demoted],
            "soft-demoted peers are kept as fallback after normal peers"
        );
    }

    #[test]
    fn ordered_peers_keep_demoted_eligible_when_all_are_demoted() {
        let peer_a = peer_id(1);
        let peer_b = peer_id(2);
        let peers = vec![peer_a.clone(), peer_b.clone()];

        let ordered = ordered_peers(peers, 1, |peer_id| peer_id, &mut |_peer_id| {
            PeerPreference::Demoted
        });

        assert_eq!(
            ordered,
            vec![peer_b, peer_a],
            "all-demoted peer sets remain selectable and keep rotating"
        );
    }

    #[test]
    fn ordered_peers_drop_excluded_peers() {
        let normal = peer_id(1);
        let excluded = peer_id(2);
        let demoted = peer_id(3);
        let peers = vec![normal.clone(), excluded.clone(), demoted.clone()];

        let ordered = ordered_peers(peers, 0, |peer_id| peer_id, &mut |peer_id| {
            if peer_id == &excluded {
                PeerPreference::Excluded
            } else if peer_id == &demoted {
                PeerPreference::Demoted
            } else {
                PeerPreference::Normal
            }
        });

        assert_eq!(
            ordered,
            vec![normal, demoted],
            "excluded peers are not queried even when demoted peers remain eligible"
        );
    }

    #[test]
    fn validate_contiguous_roots_accepts_short_prefix() -> Result<(), BoxError> {
        let roots = vec![root_at(10), root_at(11)];

        validate_contiguous_roots(&roots, 10, 4)?;

        Ok(())
    }

    #[test]
    fn validate_contiguous_roots_accepts_small_tail_progress() -> Result<(), BoxError> {
        let roots = vec![root_at(10)];

        validate_contiguous_roots(&roots, 10, 1)?;

        Ok(())
    }

    #[test]
    fn validate_contiguous_roots_rejects_tiny_large_request_prefix() {
        let roots = vec![root_at(10)];

        assert!(
            validate_contiguous_roots(&roots, 10, MAX_TA_ROOTS_PER_REQUEST).is_err(),
            "one-root replies to large tree_aux requests do not make enough progress"
        );
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
