use std::{sync::Arc, time::Duration, time::Instant};

use tokio::sync::oneshot;
use zebra_chain::{block::Height, serialization::ZcashDeserializeInto};

use super::{VctWriteManager, VCT_ROOT_RETRY_WAIT};
use crate::{
    request::CheckpointVerifiedBlock, service::queued_blocks::QueuedCheckpointVerified,
    tests::FakeChainHelper,
};

/// Builds a distinct [`QueuedCheckpointVerified`] with a discarded response channel, so
/// tests can tell blocks apart by hash without caring about the response side.
fn queued_block(seed: u128) -> QueuedCheckpointVerified {
    let genesis = zebra_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into::<Arc<zebra_chain::block::Block>>()
        .expect("genesis block deserializes");
    let block = genesis.make_fake_child().set_work(seed);
    let (rsp_tx, _rsp_rx) = oneshot::channel();
    (CheckpointVerifiedBlock::from(block), rsp_tx)
}

#[test]
fn take_ready_returns_none_when_empty() {
    let mut manager = VctWriteManager::default();
    assert!(manager.take_ready().is_none());
}

#[test]
fn on_commit_success_is_a_no_op_without_a_stall() {
    let mut manager = VctWriteManager::default();
    // Must not panic, and must leave the (already-clear) stall state alone.
    manager.on_commit_success();
    assert!(manager.stall.is_none());
    assert!(!manager.stall_logged);
}

#[test]
fn on_commit_success_clears_an_escalated_stall() {
    let mut manager = VctWriteManager::default();
    let height = Height(1);

    // Force the stall past the warn threshold so it gets escalated (logged).
    manager.stall = Some((height, Instant::now() - Duration::from_secs(31)));
    manager.on_retryable_error(height, queued_block(1));
    assert!(manager.stall_logged, "the stall should have been escalated");

    manager.on_commit_success();

    assert!(manager.stall.is_none());
    assert!(!manager.stall_logged);
}

#[test]
fn on_retryable_error_keeps_the_same_stall_start_for_a_repeated_height() {
    let mut manager = VctWriteManager::default();
    let height = Height(5);

    manager.on_retryable_error(height, queued_block(1));
    let first_seen = manager.stall.expect("a stall is now tracked").1;

    manager.on_retryable_error(height, queued_block(2));
    let still_first_seen = manager.stall.expect("the stall is still tracked").1;

    assert_eq!(
        first_seen, still_first_seen,
        "retrying the same height must not reset the stall's start time"
    );
}

#[test]
fn on_retryable_error_resets_the_stall_for_a_different_height() {
    let mut manager = VctWriteManager::default();

    manager.on_retryable_error(Height(1), queued_block(1));
    manager.stall_logged = true; // simulate an already-escalated stall

    manager.on_retryable_error(Height(2), queued_block(2));

    assert_eq!(manager.stall.map(|(h, _)| h), Some(Height(2)));
    assert!(
        !manager.stall_logged,
        "a new height starts a fresh, unescalated stall"
    );
}

#[test]
fn on_retryable_error_escalates_past_the_warn_threshold() {
    let mut manager = VctWriteManager::default();
    let height = Height(7);

    // Below the threshold: not escalated yet.
    manager.on_retryable_error(height, queued_block(1));
    assert!(!manager.stall_logged);

    // Backdate the stall past the warn threshold and retry the same height.
    manager.stall = Some((height, Instant::now() - Duration::from_secs(31)));
    manager.on_retryable_error(height, queued_block(2));
    assert!(manager.stall_logged);
}

#[test]
fn on_retryable_error_parks_the_block_for_retry() {
    let mut manager = VctWriteManager::default();
    let block = queued_block(1);
    let hash = block.0.hash;

    manager.on_retryable_error(Height(1), block);

    let ready = manager
        .take_ready()
        .expect("the block was parked for retry");
    assert_eq!(ready.0.hash, hash);
}

#[test]
fn on_retryable_error_returns_the_root_retry_wait() {
    let mut manager = VctWriteManager::default();

    let missing_root_wait = manager.on_retryable_error(Height(1), queued_block(1));
    assert_eq!(missing_root_wait, VCT_ROOT_RETRY_WAIT);
}
