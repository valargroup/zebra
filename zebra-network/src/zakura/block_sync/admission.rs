use std::time::{Duration, Instant};

use zebra_chain::block;

use super::{config::ZakuraBlockSyncConfig, state::next_height};

/// Delivery rate assumed when sizing an above-floor deadline for a peer whose
/// measured BtlBw is still near zero, so the patience window is bounded rather than
/// unbounded. A worst-case `MAX_BLOCK_BYTES` body at this rate transfers in ~8 s, so
/// with the `request_timeout` base the above-floor deadline tops out near 16 s — the
/// "a block every ~16 s is fine" tolerance the directive sets for speculative work.
const ABOVE_FLOOR_DEADLINE_MIN_BYTES_PER_SEC: u64 = 256 * 1024;

/// Estimated resident-memory multiple of a buffered block body's serialized size.
///
/// Buffered bodies are held decoded (`Arc<Block>`, `sequencer::ApplyingBlock`), whose
/// in-memory footprint is several times their wire/serialized size. The look-ahead byte
/// budget must bound that *resident* cost, not the wire bytes, or a small-block backlog
/// blows past the intended memory ceiling — the ZCA-742 OOM, where ~569k decoded blocks
/// held under a wire-byte cap reached ~26 GiB RSS.
///
// TODO(ZCA-742): replace this flat factor with a precise per-block heap-size estimate
// (a structural walk, or a `GetSize`-style measure on `Block`), so the budget tracks real
// memory exactly. The factor is a deliberately conservative calibration from the measured
// ~3.3–4x wire→resident ratio; it is an approximation, not a true per-block size.
pub(super) const DESERIALIZED_MEM_FACTOR: u64 = 4;

/// Pure inputs for deciding whether a block request may consume budget.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct AdmissionSnapshot {
    pub(super) download_floor: block::Height,
    /// The verified (commit) tip. The single contiguous block above it is always fundable
    /// (liveness), so commit can drain the pipeline; everything else is memory-gated.
    pub(super) verified_block_tip: block::Height,
    pub(super) reorder_buffered_bytes: u64,
    pub(super) reorder_buffered_blocks: u64,
    pub(super) applying_buffered_bytes: u64,
    pub(super) applying_buffered_blocks: u64,
    pub(super) sequencer_input_queued_bytes: u64,
    pub(super) reserved_above_floor_bytes: u64,
    pub(super) reserved_above_floor_blocks: u64,
    pub(super) budget_available: u64,
}

/// Whether a request is rescuing the current floor or speculating above it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum RequestPriority {
    Floor,
    AboveFloor,
}

/// Admission result for one candidate request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct AdmissionDecision {
    pub(super) priority: RequestPriority,
    pub(super) max_request_bytes: u64,
}

/// Return the highest start height that can be rescued by a floor request.
pub(super) fn floor_rescue_high(download_floor: block::Height) -> block::Height {
    next_height(download_floor).unwrap_or(download_floor)
}

pub(super) fn request_priority(
    download_floor: block::Height,
    start_height: block::Height,
) -> RequestPriority {
    // The next height above the floor can still unblock the current floor.
    if start_height <= floor_rescue_high(download_floor) {
        RequestPriority::Floor
    } else {
        RequestPriority::AboveFloor
    }
}

/// The per-request network deadline (the one sanctioned timer), set by priority:
///
/// - **Floor**: a short fixed leash. On expiry the lowest missing height is rescued
///   to a faster carrier (returned to the queue + the peer retry-avoided), so the
///   contiguous floor never waits on a slow peer — and the peer is *not* disconnected.
/// - **Above-floor**: the base `request_timeout` plus the size-expected transfer time
///   (`estimated_bytes / BtlBw`), so a legitimately slow large-body fetch runs to
///   completion. These deadlines never gate the floor, so they can afford to be
///   patient; `btlbw_bytes_per_sec` is the peer's measured rate (`None` cold-start),
///   floored at [`ABOVE_FLOOR_DEADLINE_MIN_BYTES_PER_SEC`].
pub(super) fn request_deadline(
    priority: RequestPriority,
    queued_at: Instant,
    request_timeout: Duration,
    floor_rescue_timeout: Duration,
    estimated_bytes: u64,
    btlbw_bytes_per_sec: Option<u64>,
) -> Instant {
    match priority {
        RequestPriority::Floor => queued_at + floor_rescue_timeout,
        RequestPriority::AboveFloor => {
            let rate = btlbw_bytes_per_sec
                .unwrap_or(0)
                .max(ABOVE_FLOOR_DEADLINE_MIN_BYTES_PER_SEC);
            // One body per request, so `estimated_bytes / rate` is at most
            // `MAX_BLOCK_BYTES / rate` (~8 s): finite and non-negative.
            let transfer = Duration::from_secs_f64(estimated_bytes as f64 / rate as f64);
            queued_at + request_timeout + transfer
        }
    }
}

/// The block that lets commit advance next (`verified_tip + 1`). Always fundable, so the
/// pipeline can drain even when the look-ahead budget is full.
fn commit_frontier(snapshot: &AdmissionSnapshot) -> block::Height {
    next_height(snapshot.verified_block_tip).unwrap_or(snapshot.verified_block_tip)
}

/// Estimated resident memory of the decoded bodies already held in the pipeline
/// (`held wire bytes * DESERIALIZED_MEM_FACTOR`).
fn held_memory_bytes(snapshot: &AdmissionSnapshot) -> u64 {
    snapshot
        .reorder_buffered_bytes
        .saturating_add(snapshot.applying_buffered_bytes)
        .saturating_add(snapshot.sequencer_input_queued_bytes)
        .saturating_add(snapshot.reserved_above_floor_bytes)
        .saturating_mul(DESERIALIZED_MEM_FACTOR)
}

fn held_blocks(snapshot: &AdmissionSnapshot) -> u64 {
    snapshot
        .reorder_buffered_blocks
        .saturating_add(snapshot.applying_buffered_blocks)
        .saturating_add(snapshot.reserved_above_floor_blocks)
}

/// Whether the resident-memory look-ahead budget (or the block cap) is already full.
fn lookahead_over_budget(config: &ZakuraBlockSyncConfig, snapshot: &AdmissionSnapshot) -> bool {
    held_memory_bytes(snapshot) >= config.effective_max_reorder_lookahead_bytes()
        || held_blocks(snapshot) >= u64::from(config.max_reorder_lookahead_blocks)
}

/// Whether a floor-priority take at `start_height` is allowed past the resident-memory
/// look-ahead backpressure.
///
/// The commit-frontier block is always allowed (liveness — the committer must be able to
/// fetch the block it needs to advance, which also reaches the floor-reservation funding
/// path); every other height is allowed only while the resident-memory budget and block
/// cap have headroom. This is deliberately **independent of the in-flight byte budget**, so
/// an exhausted in-flight budget still lets the commit-frontier floor reach its funding
/// path — unlike [`admission_decision`], whose `max_request_bytes` collapses to zero when
/// the in-flight budget is spent. Anchoring the floor exemption to the commit frontier
/// (rather than the download floor, which advances on every download) is what stops
/// `body_download_floor` from escalating unboundedly ahead of commit (ZCA-742).
pub(super) fn floor_take_allowed(
    config: &ZakuraBlockSyncConfig,
    snapshot: AdmissionSnapshot,
    start_height: block::Height,
) -> bool {
    start_height <= commit_frontier(&snapshot) || !lookahead_over_budget(config, &snapshot)
}

/// Returns the admission decision for a candidate block response starting at `start_height`.
///
/// The single contiguous block just above the *verified* (commit) frontier is always
/// fundable, so the committer can advance and drain the pipeline even when the look-ahead
/// budget is full. Every other request — floor-priority included — is admitted only while
/// the configured look-ahead limits still have capacity, measured against the *resident*
/// memory of the buffered decoded bodies (`held_bytes * DESERIALIZED_MEM_FACTOR`) rather
/// than their wire bytes.
///
/// Gating the floor lane (with only the commit-frontier exempt) is what bounds the applying
/// queue: the download floor advances on every download, so a floor exemption tied to it
/// escalates unboundedly ahead of commit. Anchoring the exemption to the commit frontier
/// caps the pipeline to the look-ahead budget regardless of how far headers/downloads run
/// ahead (ZCA-742).
///
/// Returns `None` when no bytes can be admitted, or when a non-frontier request would exceed
/// the look-ahead limits.
pub(super) fn admission_decision(
    config: &ZakuraBlockSyncConfig,
    snapshot: AdmissionSnapshot,
    start_height: block::Height,
    response_byte_cap: u64,
) -> Option<AdmissionDecision> {
    let priority = request_priority(snapshot.download_floor, start_height);

    let max_request_bytes = if start_height <= commit_frontier(&snapshot) {
        snapshot.budget_available.min(response_byte_cap)
    } else {
        if lookahead_over_budget(config, &snapshot) {
            return None;
        }
        // Remaining memory headroom, expressed back in wire bytes for the response cap so a
        // single response can't push resident memory past the budget.
        let remaining_wire_bytes = config
            .effective_max_reorder_lookahead_bytes()
            .saturating_sub(held_memory_bytes(&snapshot))
            / DESERIALIZED_MEM_FACTOR;
        snapshot
            .budget_available
            .min(remaining_wire_bytes)
            .min(response_byte_cap)
    };

    (max_request_bytes > 0).then_some(AdmissionDecision {
        priority,
        max_request_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(8);
    const RESCUE: Duration = Duration::from_secs(2);

    #[test]
    fn floor_request_uses_the_short_rescue_leash() {
        let now = Instant::now();
        let deadline = request_deadline(
            RequestPriority::Floor,
            now,
            TIMEOUT,
            RESCUE,
            2_000_000,
            None,
        );
        // The floor is rescued on the fixed leash regardless of size or measured rate.
        assert_eq!(deadline, now + RESCUE);
    }

    #[test]
    fn above_floor_deadline_grows_with_body_size() {
        let now = Instant::now();
        // No measured rate: the min-rate floor (256 KiB/s) sizes the transfer term, so a
        // 256 KiB body adds ~1 s and a 2 MiB body adds ~8 s on top of the base timeout.
        let small = request_deadline(
            RequestPriority::AboveFloor,
            now,
            TIMEOUT,
            RESCUE,
            256 * 1024,
            None,
        );
        let large = request_deadline(
            RequestPriority::AboveFloor,
            now,
            TIMEOUT,
            RESCUE,
            2 * 1024 * 1024,
            None,
        );
        assert_eq!(small, now + TIMEOUT + Duration::from_secs(1));
        assert_eq!(large, now + TIMEOUT + Duration::from_secs(8));
        assert!(large > small);
    }

    #[test]
    fn above_floor_deadline_shrinks_as_measured_rate_rises() {
        let now = Instant::now();
        // A fast peer transfers the body quickly, so its above-floor deadline collapses
        // toward the base timeout — the size term is negligible at high BtlBw.
        let fast = request_deadline(
            RequestPriority::AboveFloor,
            now,
            TIMEOUT,
            RESCUE,
            2 * 1024 * 1024,
            Some(64 * 1024 * 1024),
        );
        assert!(fast > now + TIMEOUT);
        assert!(fast < now + TIMEOUT + Duration::from_millis(100));
    }
}
