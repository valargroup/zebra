//! Configuration for semantic verification which is run in parallel.

use serde::{Deserialize, Serialize};

/// Configuration for parallel semantic verification:
/// <https://zebra.zfnd.org/dev/rfcs/0002-parallel-verification.html#definitions>
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(
    deny_unknown_fields,
    default,
    from = "InnerConfig",
    into = "InnerConfig"
)]
pub struct Config {
    /// Should Zebra make sure that it follows the consensus chain while syncing?
    /// This is a developer-only option.
    ///
    /// # Security
    ///
    /// Disabling this option leaves your node vulnerable to some kinds of chain-based attacks.
    /// Zebra regularly updates its checkpoints to ensure nodes are following the best chain.
    ///
    /// # Details
    ///
    /// This option is `true` by default, because it prevents some kinds of chain attacks.
    ///
    /// Disabling this option makes Zebra start full validation earlier.
    /// It is slower and less secure.
    /// To keep checkpoint sync enabled but force-disable the initial VCT fast-sync rollout, use
    /// [`disable_vct_fast_sync`](Self::disable_vct_fast_sync) instead.
    ///
    /// Zebra requires some checkpoints to simplify validation of legacy network upgrades.
    /// Required checkpoints are always active, even when this option is `false`.
    ///
    /// # Deprecation
    ///
    /// For security reasons, this option might be deprecated or ignored in a future Zebra
    /// release.
    pub checkpoint_sync: bool,

    /// Force-disable the verified-commitment-trees fast sync path during its initial rollout.
    ///
    /// This keeps [`checkpoint_sync`](Self::checkpoint_sync) enabled while forcing the legacy
    /// per-block Sapling/Orchard tree recompute in both Archive and Pruned storage modes. Set to
    /// `false` by default: checkpoint sync uses VCT fast sync on networks with embedded handoff
    /// frontiers.
    pub disable_vct_fast_sync: bool,
}

impl From<InnerConfig> for Config {
    fn from(
        InnerConfig {
            checkpoint_sync,
            disable_vct_fast_sync,
            ..
        }: InnerConfig,
    ) -> Self {
        Self {
            checkpoint_sync,
            disable_vct_fast_sync,
        }
    }
}

impl From<Config> for InnerConfig {
    fn from(
        Config {
            checkpoint_sync,
            disable_vct_fast_sync,
        }: Config,
    ) -> Self {
        Self {
            checkpoint_sync,
            disable_vct_fast_sync,
            _debug_skip_parameter_preload: false,
        }
    }
}

/// Inner consensus configuration for backwards compatibility with older `zebrad.toml` files,
/// which contain fields that have been removed.
///
/// Rust API callers should use [`Config`].
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct InnerConfig {
    /// See [`Config`] for more details.
    pub checkpoint_sync: bool,

    /// See [`Config`] for more details.
    pub disable_vct_fast_sync: bool,

    #[serde(skip_serializing, rename = "debug_skip_parameter_preload")]
    /// Unused config field for backwards compatibility.
    pub _debug_skip_parameter_preload: bool,
}

// we like our default configs to be explicit
#[allow(unknown_lints)]
#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        Self {
            checkpoint_sync: true,
            disable_vct_fast_sync: false,
        }
    }
}

impl Default for InnerConfig {
    fn default() -> Self {
        Self {
            checkpoint_sync: Config::default().checkpoint_sync,
            disable_vct_fast_sync: Config::default().disable_vct_fast_sync,
            _debug_skip_parameter_preload: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disable_vct_fast_sync_defaults_false_and_converts_through_inner_config() {
        assert!(!Config::default().disable_vct_fast_sync);

        let force_disabled = Config::from(InnerConfig {
            checkpoint_sync: true,
            disable_vct_fast_sync: true,
            _debug_skip_parameter_preload: false,
        });

        assert!(force_disabled.checkpoint_sync);
        assert!(force_disabled.disable_vct_fast_sync);

        let inner = InnerConfig::from(force_disabled);
        assert!(inner.disable_vct_fast_sync);
    }
}
