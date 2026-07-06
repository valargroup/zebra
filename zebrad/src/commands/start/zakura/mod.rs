use std::time::Duration;

use serde_json::{Map, Number, Value};
use zebra_chain::block;
use zebra_network::zakura::{
    commit_state_trace as cs_trace, zakura_trace_peer_label, BlockApplyResult, ZakuraPeerId,
    ZakuraTrace, COMMIT_STATE_TABLE,
};

/// Gates the Zakura block-sync bulk-apply pipeline so the legacy `ChainSync`
/// fallback can drain it before driving commits through the same verifier and
/// state pipeline.
///
/// Two sync engines submitting bulk commits concurrently race in the applying
/// queue (ordering races can poison the parent-error map and trip the
/// same-hash commit lockout), so the fallback must be a commit barrier: once
/// yielded, the block-sync driver starts no new applies, and the watchdog
/// waits for in-flight applies to finish before resuming legacy sync. The
/// Zakura reactors stay alive throughout — serving, statuses, and header
/// commits are unaffected; only bulk body applies are gated.
#[derive(Debug)]
pub(crate) struct ZakuraApplyGate {
    yielded: std::sync::atomic::AtomicBool,
    in_flight: std::sync::atomic::AtomicUsize,
    drained: tokio::sync::Notify,
}

/// Tracks one in-flight Zakura block apply; dropping it releases the slot and
/// wakes a pending [`ZakuraApplyGate::yield_and_drain`].
#[derive(Debug)]
pub(crate) struct ZakuraApplyPermit(std::sync::Arc<ZakuraApplyGate>);

impl ZakuraApplyGate {
    pub(crate) fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            yielded: std::sync::atomic::AtomicBool::new(false),
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            drained: tokio::sync::Notify::new(),
        })
    }

    /// Whether the pipeline has been yielded to legacy sync.
    pub(crate) fn is_yielded(&self) -> bool {
        self.yielded.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns a permit for one block apply, or `None` once the pipeline has
    /// been yielded to legacy sync.
    pub(crate) fn begin_apply(self: &std::sync::Arc<Self>) -> Option<ZakuraApplyPermit> {
        if self.is_yielded() {
            return None;
        }
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Re-check after reserving so a yield that lands between the check and
        // the increment still sees an accurate in-flight count.
        if self.is_yielded() {
            self.release();
            return None;
        }
        Some(ZakuraApplyPermit(self.clone()))
    }

    fn release(&self) {
        if self
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            == 1
        {
            self.drained.notify_waiters();
        }
    }

    /// Yields the apply pipeline to legacy sync and waits until in-flight
    /// applies drain, bounded by `timeout` (each apply already has its own
    /// driver timeout, so the bound is a backstop, not the primary limit).
    pub(crate) async fn yield_and_drain(&self, timeout: Duration) {
        self.yielded
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let drained = self.drained.notified();
            let in_flight = self.in_flight.load(std::sync::atomic::Ordering::SeqCst);
            if in_flight == 0 {
                return;
            }
            if tokio::time::timeout_at(deadline, drained).await.is_err() {
                tracing::warn!(
                    in_flight,
                    "timed out draining Zakura block applies before legacy fallback; \
                     remaining applies resolve through their own driver timeouts"
                );
                return;
            }
        }
    }
}

impl Drop for ZakuraApplyPermit {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub(crate) mod block_sync_driver;
pub(crate) mod frontier;
pub(crate) mod header_sync_driver;
pub(crate) mod throughput_probe;

pub(crate) use block_sync_driver::drive_block_sync_actions;
#[cfg(test)]
pub(crate) use block_sync_driver::{
    apply_block_sync_body, block_apply_class, block_sync_missing_body_window,
    block_sync_needed_blocks_from_state, coalesce_ready_needed_block_queries,
    coalesce_stale_needed_block_queries, commit_block_sync_body, query_block_sync_needed_blocks,
    BlockApplyClass, ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_INTERVAL,
    ZAKURA_BLOCK_SYNC_MISSING_BODY_WINDOW,
};
pub(crate) use frontier::{query_block_sync_frontiers, verified_block_tip_from_state};
#[cfg(test)]
pub(crate) use header_sync_driver::{
    block_roots_cover_range, block_sync_chain_tip_event, body_sizes_for_served_header_range,
    chain_tip_mirror_frontier_change, header_range_commit_failure_kind,
    notify_block_sync_header_tip, reconcile_stranded_body_suffix,
    root_covered_query_best_header_tip, tree_aux_roots_for_served_header_range,
};
pub(crate) use header_sync_driver::{
    drive_zakura_header_sync_actions, mirror_zakura_full_block_commits,
    zakura_header_sync_driver_startup, ZakuraHeaderSyncDriverHandles,
};
pub(crate) use throughput_probe::{BlocksyncThroughputProbe, BlocksyncThroughputSummary};

pub(crate) const ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn emit_commit_state(
    trace: &ZakuraTrace,
    event: &'static str,
    source: &'static str,
    build: impl FnOnce(&mut Map<String, Value>),
) {
    trace.emit_with(COMMIT_STATE_TABLE, |row| {
        row.insert(
            cs_trace::EVENT.to_string(),
            Value::String(event.to_string()),
        );
        insert_cs_str(row, cs_trace::SOURCE, source);
        build(row);
    });
}

pub(crate) fn insert_cs_height(
    row: &mut Map<String, Value>,
    key: &'static str,
    height: block::Height,
) {
    insert_cs_u64(row, key, u64::from(height.0));
}

pub(crate) fn insert_cs_hash(row: &mut Map<String, Value>, key: &'static str, hash: block::Hash) {
    row.insert(key.to_string(), Value::String(format!("{hash}")));
}

pub(crate) fn insert_cs_peer(row: &mut Map<String, Value>, key: &'static str, peer: &ZakuraPeerId) {
    row.insert(
        key.to_string(),
        Value::String(zakura_trace_peer_label(peer)),
    );
}

pub(crate) fn insert_cs_u64(row: &mut Map<String, Value>, key: &'static str, value: u64) {
    row.insert(key.to_string(), Value::Number(Number::from(value)));
}

pub(crate) fn insert_cs_bool(row: &mut Map<String, Value>, key: &'static str, value: bool) {
    row.insert(key.to_string(), Value::Bool(value));
}

pub(crate) fn insert_cs_str(row: &mut Map<String, Value>, key: &'static str, value: &str) {
    row.insert(key.to_string(), Value::String(value.to_string()));
}

pub(crate) fn insert_cs_frontiers(
    row: &mut Map<String, Value>,
    frontiers: &zebra_network::zakura::BlockSyncFrontiers,
) {
    insert_cs_height(row, cs_trace::FINALIZED_HEIGHT, frontiers.finalized_height);
    insert_cs_height(
        row,
        cs_trace::VERIFIED_BLOCK_TIP,
        frontiers.verified_block_tip,
    );
    insert_cs_hash(
        row,
        cs_trace::VERIFIED_BLOCK_HASH,
        frontiers.verified_block_hash,
    );
}

pub(crate) fn block_apply_result_label(result: BlockApplyResult) -> &'static str {
    match result {
        BlockApplyResult::Committed => "committed",
        BlockApplyResult::Duplicate => "duplicate",
        BlockApplyResult::Rejected => "rejected",
        BlockApplyResult::TimedOut => "timed_out",
    }
}

pub(crate) fn block_verify_error_is_duplicate<Error>(error: &Error) -> bool
where
    Error: std::fmt::Debug + Send + Sync + 'static,
{
    let error = error as &dyn std::any::Any;

    error
        .downcast_ref::<zebra_consensus::RouterError>()
        .is_some_and(zebra_consensus::RouterError::is_duplicate_request)
        || error
            .downcast_ref::<zebra_consensus::VerifyBlockError>()
            .is_some_and(zebra_consensus::VerifyBlockError::is_duplicate_request)
        || error
            .downcast_ref::<zebra_consensus::BoxError>()
            .is_some_and(|error| {
                error
                    .downcast_ref::<zebra_consensus::RouterError>()
                    .is_some_and(zebra_consensus::RouterError::is_duplicate_request)
                    || error
                        .downcast_ref::<zebra_consensus::VerifyBlockError>()
                        .is_some_and(zebra_consensus::VerifyBlockError::is_duplicate_request)
            })
}
