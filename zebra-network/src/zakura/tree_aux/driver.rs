//! Client-side `tree_aux` root fetching.
//!
//! [`fetch_roots`] pulls verified per-block commitment roots for a height range from
//! connected peers and hands each contiguous batch to a sink. The node wires that sink
//! to a `PeerSource`, so the fast committer reads peer-fetched roots through the same
//! seam it uses for the fixture. Run *ahead of* body download (header-sync-aligned), so
//! a range's coverage is known before it is committed.

use std::sync::atomic::{AtomicU64, Ordering};

use zebra_chain::{block, parallel::commitment_aux::BlockCommitmentRoots};

use super::{TreeAuxMessage, MAX_TA_ROOTS_PER_REQUEST, ZAKURA_STREAM_TREE_AUX};
use crate::{zakura::ZakuraSupervisorHandle, BoxError};

static NEXT_TREE_AUX_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Fetch verified per-block commitment roots for `[start, end]` from connected peers,
/// delivering each received contiguous batch to `sink`.
///
/// Requests are bounded by [`MAX_TA_ROOTS_PER_REQUEST`]; the fetch advances by what each
/// peer returns and stops once `end` is covered. Returns an error if no peer is connected
/// or a sub-range is unavailable/malformed — the caller treats that range as un-fetched
/// and falls back to legacy verification for it (never wrong, possibly slow).
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

        let handle = supervisor
            .outbound_peer_handles()
            .await
            .into_iter()
            .next()
            .ok_or_else(|| -> BoxError { "no connected tree_aux peer".into() })?;

        let request = TreeAuxMessage::GetRoots {
            start_height: block::Height(next),
            count,
        }
        .encode_frame()
        .map_err(|error| -> BoxError { format!("encoding GetRoots failed: {error}").into() })?;

        let request_id = NEXT_TREE_AUX_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let frames = handle
            .request(
                ZAKURA_STREAM_TREE_AUX,
                request_id,
                request.message_type,
                request.flags,
                request.payload,
            )
            .await?;

        let frame = frames
            .into_iter()
            .next()
            .ok_or_else(|| -> BoxError { "empty tree_aux response".into() })?;

        match TreeAuxMessage::decode_frame(frame).map_err(|error| -> BoxError {
            format!("decoding tree_aux response failed: {error}").into()
        })? {
            TreeAuxMessage::Roots { roots } => {
                let last = roots
                    .last()
                    .map(|root| root.height.0)
                    .ok_or_else(|| -> BoxError { "peer returned an empty Roots batch".into() })?;
                if last < next {
                    return Err("peer returned out-of-range tree_aux roots".into());
                }
                sink(roots);
                next = last
                    .checked_add(1)
                    .ok_or_else(|| -> BoxError { "tree_aux root height overflow".into() })?;
            }
            TreeAuxMessage::RangeUnavailable { .. } => {
                return Err(format!("peer cannot serve tree_aux roots at height {next}").into());
            }
            other => {
                return Err(format!("unexpected tree_aux response to GetRoots: {other:?}").into());
            }
        }
    }

    Ok(())
}
