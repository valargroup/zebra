//! Prepare a base fork by rolling its finalized tip back to `target`.
//!
//! Used to manufacture a base at `start-1` from a higher snapshot when no
//! lower snapshot is available, so the `apply` window can be replayed on top.

use std::path::Path;

use color_eyre::eyre::{eyre, Result};
use zebra_chain::{block::Height, parameters::Network};
use zebra_state::{rollback_finalized_state, RollbackFinalizedStateOptions};

use crate::config::state_config;

/// Rolls the finalized state at `base` back to `target` height.
pub fn run(base: &Path, target: u32, network: Network) -> Result<()> {
    let config = state_config(base.to_path_buf(), true);
    tracing::info!(base = %base.display(), target, "rolling back base fork");

    let summary = rollback_finalized_state(
        config,
        &network,
        RollbackFinalizedStateOptions {
            target_height: Height(target),
            keep_rolled_back_blocks: false,
            max_checkpoint_height: None,
        },
    )
    .map_err(|e| eyre!("rollback failed: {e}"))?;

    tracing::info!(
        old_tip = summary.old_tip.0 .0,
        new_tip = summary.new_tip.0 .0,
        removed = summary.rolled_back_count,
        "rollback complete"
    );
    Ok(())
}
