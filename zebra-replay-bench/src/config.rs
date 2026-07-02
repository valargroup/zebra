//! State config construction shared by the subcommands.

use std::path::PathBuf;

use zebra_state::{Config, StorageMode};

/// Builds a [`zebra_state::Config`] pointing at `cache_dir`.
///
/// `cache_dir` is the snapshot/fork root that contains `state/vN/<network>`. The
/// benchmark only ever opens archive snapshots (full block bodies + per-height
/// trees), so storage mode is fixed to [`StorageMode::Archive`].
pub fn state_config(cache_dir: PathBuf) -> Config {
    Config {
        cache_dir,
        ephemeral: false,
        storage_mode: StorageMode::Archive,
        ..Config::default()
    }
}
