//! Root-stall tracking for the checkpoint write loop's verified-commitment-trees
//! (vct) fast path.

use std::time::{Duration, Instant};

use tracing::info;
use zebra_chain::block::Height;

use crate::service::queued_blocks::QueuedCheckpointVerified;

/// Delay between retryable VCT root-miss commit attempts. Nothing actively re-requests a
/// missing root, so this only polls for a re-delivery of the same header range (for example
/// another fanout peer's response); the slow poll keeps a persistent hole cheap to wait on.
const VCT_ROOT_RETRY_WAIT: Duration = Duration::from_millis(500);

/// How long a single checkpoint height may stay stuck on a retryable VCT root stall before
/// the committer escalates to an error-level log and a `state.vct.root.stalled.height` gauge.
/// Transient waits (a fanout re-delivery still in flight) clear well within this; staying
/// stuck past it means no verifiable root is available for a height the frozen frontier
/// requires, and — by design — the committer will not recompute against the stale frontier,
/// so the node cannot advance. Surfacing that loudly is the operator's only signal.
const VCT_ROOT_STALL_WARN_AFTER: Duration = Duration::from_secs(30);

/// Root-stall tracking for the checkpoint write loop's verified-commitment-trees
/// (vct) fast path. Bundles the state the loop needs to retry/escalate a stuck
/// height, so their invariants (single log per stall) live next to the data they
/// guard.
#[derive(Default)]
pub(super) struct VctWriteManager {
    /// A block parked for retry (a missing root) instead of going through the
    /// invalid-block reset path.
    retry: Option<QueuedCheckpointVerified>,
    /// `(height, first-seen)` of the height currently stuck retrying, if any.
    stall: Option<(Height, Instant)>,
    /// Whether the current stall has already been escalated to an
    /// error-level log and gauge.
    stall_logged: bool,
}

impl VctWriteManager {
    /// Takes the parked retry block ready to commit, if any.
    pub(super) fn take_ready(&mut self) -> Option<QueuedCheckpointVerified> {
        self.retry.take()
    }

    /// A successful commit clears any vct root stall: logs recovery and
    /// resets the stalled-height gauge if the stall had been escalated.
    pub(super) fn on_commit_success(&mut self) {
        if self.stall.is_some() {
            if self.stall_logged {
                info!(
                    stalled_height = ?self.stall.map(|(h, _)| h),
                    "VCT: checkpoint commit recovered; the stalled height now has a verifiable supplied root"
                );
                metrics::gauge!("state.vct.root.stalled.height").set(0.0);
            }
            self.stall = None;
            self.stall_logged = false;
        }
    }

    /// Tracks and, past the warn threshold, escalates a retryable vct root
    /// stall at `height`, parks `block` for retry, and returns how long the
    /// caller should park before retrying.
    pub(super) fn on_retryable_error(
        &mut self,
        height: Height,
        block: QueuedCheckpointVerified,
    ) -> Duration {
        metrics::counter!("state.vct.root.retry.count").increment(1);

        // Escalate a stall that persists on the same height past the warn
        // threshold: a transient wait resolves in a few polls and stays
        // quiet, but a height stuck longer means no root the frozen frontier
        // requires is available — roots are not individually re-requested, so
        // the node will not advance (it will not, by design, recompute against
        // the stale frontier). Surface it loudly.
        match self.stall {
            Some((stuck, _)) if stuck == height => {}
            _ => {
                self.stall = Some((height, Instant::now()));
                self.stall_logged = false;
            }
        }
        if !self.stall_logged
            && self
                .stall
                .is_some_and(|(_, since)| since.elapsed() >= VCT_ROOT_STALL_WARN_AFTER)
        {
            tracing::error!(
                ?height,
                stalled_for = ?VCT_ROOT_STALL_WARN_AFTER,
                "VCT: checkpoint commit stalled with no verifiable supplied root; \
                 roots are not re-requested, so the node cannot advance without a \
                 re-delivery of this header range (it will not recompute against \
                 the frozen frontier)"
            );
            metrics::gauge!("state.vct.root.stalled.height").set(f64::from(height.0));
            self.stall_logged = true;
        } else {
            tracing::warn!(
                ?height,
                block_height = ?block.0.height,
                block_hash = ?block.0.hash,
                "VCT: supplied root not yet verifiable; retrying checkpoint commit in place"
            );
        }

        self.retry = Some(block);

        VCT_ROOT_RETRY_WAIT
    }
}

#[cfg(test)]
mod tests;
