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
    config
}
