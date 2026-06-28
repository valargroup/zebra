//! State config construction shared by the subcommands.

use std::path::PathBuf;

use zebra_state::{Config, StorageMode};

/// Builds a [`zebra_state::Config`] pointing at `cache_dir`.
///
/// `cache_dir` is the snapshot/fork root that contains `state/vN/<network>`. The
/// benchmark only ever opens archive snapshots (full block bodies + per-height
/// trees), so storage mode is fixed to [`StorageMode::Archive`].
///
/// `force_legacy` disables VCT fast sync, which forces the committer onto
/// the full per-block note-commitment recompute path — the write-assembler +
/// disk-writer work this benchmark exists to measure. Without it, Mainnet would
/// select the VCT peer-source fast path and skip the recompute.
pub fn state_config(cache_dir: PathBuf, force_legacy: bool) -> Config {
    Config {
        cache_dir,
        ephemeral: false,
        checkpoint_sync: true,
        vct_fast_sync: !force_legacy,
        storage_mode: StorageMode::Archive,
        ..Config::default()
    }
}
