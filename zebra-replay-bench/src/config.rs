//! State config construction shared by the subcommands.

use std::path::PathBuf;

use zebra_state::{Config, PruningConfig, StorageMode};

/// Builds a [`zebra_state::Config`] pointing at `cache_dir`.
///
/// `cache_dir` is the snapshot/fork root that contains `state/vN/<network>`. The
/// benchmark runs the production fast-sync configuration: [`StorageMode::Pruned`]
/// with checkpoint sync, so it measures exactly what a real pruned validator does
/// (including skipping the transparent address index). The per-run fork is a
/// throwaway copy, so online pruning never touches the base snapshot.
///
/// `force_legacy` clears `vct_fast_sync`, which forces the committer onto the full
/// per-block note-commitment recompute path — the write-assembler + disk-writer
/// work this benchmark exists to measure. Left on (the default), Mainnet would
/// select the VCT peer-source fast path and skip the recompute.
pub fn state_config(cache_dir: PathBuf, force_legacy: bool) -> Config {
    let mut config = Config {
        cache_dir,
        ephemeral: false,
        checkpoint_sync: true,
        vct_fast_sync: !force_legacy,
        storage_mode: StorageMode::Pruned(PruningConfig::default()),
        ..Config::default()
    };
    // Run-ahead finalized-commit pipeline depth (PR #309). 0 = synchronous (default).
    config.finalized_block_pipeline_depth = std::env::var("ZRB_PIPELINE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // Per-checkpoint transparent reconcile prototype: defer the per-block spent-UTXO
    // resolution off the commit path and reconcile it in a batch at each checkpoint.
    // Requires the run-ahead pipeline (ZRB_PIPELINE_DEPTH > 0) for correctness.
    config.defer_transparent_reconcile = std::env::var("ZRB_DEFER_TRANSPARENT")
        .map(|v| v == "1")
        .unwrap_or(false);
    // Blocks between deferred reconciles (0 = per checkpoint). Mainnet checkpoints are
    // ~30-40 blocks apart here, too frequent to amortize the reconcile; a larger fixed
    // interval batches a bigger window.
    config.defer_reconcile_interval = std::env::var("ZRB_RECONCILE_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // Force the v1 inline reconcile (on the assembler thread) instead of the default
    // v2 worker thread; mainly to A/B the off-critical-path parallelism win.
    config.defer_reconcile_inline = std::env::var("ZRB_RECONCILE_INLINE")
        .map(|v| v == "1")
        .unwrap_or(false);
    // Deterministic stop for byte-match verification: when set, the committer flushes
    // and exits the process at exactly this height (and the per-checkpoint reconcile
    // fires for the final window via the stop-height hook), so two runs land on an
    // identical tip. Leave unset for throughput runs.
    config.debug_stop_at_height = std::env::var("ZRB_STOP_AT_HEIGHT")
        .ok()
        .and_then(|s| s.parse().ok());
    config
}
