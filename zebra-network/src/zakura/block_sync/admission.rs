use zebra_chain::block;

use super::{config::ZakuraBlockSyncConfig, state::next_height};

/// Pure inputs for deciding whether a block request may consume budget.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct AdmissionSnapshot {
    pub(super) download_floor: block::Height,
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

// Return the highest height that can be rescued by a floor-rescue request.
pub(super) fn max_floor_rescue_start_height(download_floor: block::Height) -> block::Height {
    next_height(download_floor).unwrap_or(download_floor)
}


pub(super) fn request_priority(
    download_floor: block::Height,
    start_height: block::Height,
) -> RequestPriority {
    // This is <= because down
    if start_height <= max_floor_rescue_start_height(download_floor) {
        RequestPriority::Floor
    } else {
        RequestPriority::AboveFloor
    }
}

/// Returns the admission decision for a candidate block response starting at `start_height`.
///
/// Floor-rescue requests may use any available response budget up to `response_byte_cap`.
/// Speculative requests above the floor are admitted only while the configured reorder
/// lookahead byte and block limits still have capacity.
///
/// Returns `None` when no bytes can be admitted, or when an above-floor request would
/// exceed the lookahead limits.
pub(super) fn admission_decision(
    config: &ZakuraBlockSyncConfig,
    snapshot: AdmissionSnapshot,
    start_height: block::Height,
    response_byte_cap: u64,
) -> Option<AdmissionDecision> {
    let priority = request_priority(snapshot.download_floor, start_height);
    let max_request_bytes = match priority {
        RequestPriority::Floor => snapshot.budget_available.min(response_byte_cap),
        RequestPriority::AboveFloor => {
            let held_bytes = snapshot
                .reorder_buffered_bytes
                .saturating_add(snapshot.applying_buffered_bytes)
                .saturating_add(snapshot.sequencer_input_queued_bytes)
                .saturating_add(snapshot.reserved_above_floor_bytes);
            let held_blocks = snapshot
                .reorder_buffered_blocks
                .saturating_add(snapshot.applying_buffered_blocks)
                .saturating_add(snapshot.reserved_above_floor_blocks);
            if held_bytes >= config.effective_max_reorder_lookahead_bytes()
                || held_blocks >= u64::from(config.max_reorder_lookahead_blocks)
            {
                return None;
            }

            let remaining_lookahead_bytes = config
                .effective_max_reorder_lookahead_bytes()
                .saturating_sub(held_bytes);
            snapshot
                .budget_available
                .min(remaining_lookahead_bytes)
                .min(response_byte_cap)
        }
    };

    (max_request_bytes > 0).then_some(AdmissionDecision {
        priority,
        max_request_bytes,
    })
}
