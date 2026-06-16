use std::time::Duration;

pub(crate) mod block_sync_driver;
pub(crate) mod frontier;
pub(crate) mod header_sync_driver;

pub(crate) use block_sync_driver::drive_block_sync_actions;
#[cfg(test)]
pub(crate) use block_sync_driver::{
    apply_block_sync_body, block_sync_misbehavior_is_hard, block_sync_missing_body_window,
    block_sync_needed_blocks_from_state, commit_block_sync_body, BlockApplyClass,
    ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_INTERVAL,
};
pub(crate) use frontier::{query_block_sync_frontiers, verified_block_tip_from_state};
#[cfg(test)]
pub(crate) use header_sync_driver::{
    block_sync_chain_tip_event, body_sizes_for_served_header_range,
    header_range_commit_failure_kind, notify_block_sync_header_tip,
};
pub(crate) use header_sync_driver::{
    drive_zakura_header_sync_actions, mirror_zakura_full_block_commits,
    zakura_header_sync_driver_startup, ZakuraHeaderSyncDriverHandles,
};

pub(crate) const ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT: Duration = Duration::from_secs(30);

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
