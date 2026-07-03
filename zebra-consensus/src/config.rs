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
    /// To keep checkpoint sync enabled but opt out of the initial VCT fast-sync rollout, set
    /// [`vct_fast_sync`](Self::vct_fast_sync) to `false`.
    ///
    /// Zebra requires some checkpoints to simplify validation of legacy network upgrades.
    /// Required checkpoints are always active, even when this option is `false`.
    ///
    /// # Deprecation
    ///
    /// For security reasons, this option might be deprecated or ignored in a future Zebra
    /// release.
    pub checkpoint_sync: bool,

    /// Use the verified-commitment-trees fast sync path during its initial rollout.
    ///
    /// `true` by default: checkpoint sync folds in verified Sapling/Orchard/Ironwood roots and
    /// skips the per-block tree recompute on networks with embedded handoff frontiers. Set to
    /// `false` to keep [`checkpoint_sync`](Self::checkpoint_sync) enabled while forcing the legacy
    /// per-block recompute in both Archive and Pruned storage modes.
    pub vct_fast_sync: bool,
}

impl Config {
    /// Validate relationships between consensus configuration options.
    pub fn validate(&self) -> Result<(), &'static str> {
        if !self.checkpoint_sync && self.vct_fast_sync {
            return Err("consensus.vct_fast_sync = true requires consensus.checkpoint_sync = true");
        }

        Ok(())
    }
}

impl From<InnerConfig> for Config {
    fn from(
        InnerConfig {
            checkpoint_sync,
            vct_fast_sync,
            ..
        }: InnerConfig,
    ) -> Self {
        Self {
            checkpoint_sync,
            vct_fast_sync,
        }
    }
}

impl From<Config> for InnerConfig {
    fn from(
        Config {
            checkpoint_sync,
            vct_fast_sync,
        }: Config,
    ) -> Self {
        Self {
            checkpoint_sync,
            vct_fast_sync,
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
    pub vct_fast_sync: bool,

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
            vct_fast_sync: true,
        }
    }
}

impl Default for InnerConfig {
    fn default() -> Self {
        Self {
            checkpoint_sync: Config::default().checkpoint_sync,
            vct_fast_sync: Config::default().vct_fast_sync,
            _debug_skip_parameter_preload: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vct_fast_sync_defaults_true_and_converts_through_inner_config() {
        assert!(Config::default().vct_fast_sync);

        let force_disabled = Config::from(InnerConfig {
            checkpoint_sync: true,
            vct_fast_sync: false,
            _debug_skip_parameter_preload: false,
        });

        assert!(force_disabled.checkpoint_sync);
        assert!(!force_disabled.vct_fast_sync);

        let inner = InnerConfig::from(force_disabled);
        assert!(!inner.vct_fast_sync);
    }

    #[test]
    fn vct_fast_sync_requires_checkpoint_sync() {
        let valid_default = Config {
            checkpoint_sync: true,
            vct_fast_sync: true,
        };
        assert!(valid_default.validate().is_ok());

        let valid_legacy_recompute = Config {
            checkpoint_sync: true,
            vct_fast_sync: false,
        };
        assert!(valid_legacy_recompute.validate().is_ok());

        let valid_full_verification = Config {
            checkpoint_sync: false,
            vct_fast_sync: false,
        };
        assert!(valid_full_verification.validate().is_ok());

        let invalid = Config {
            checkpoint_sync: false,
            vct_fast_sync: true,
        };
        assert_eq!(
            invalid.validate(),
            Err("consensus.vct_fast_sync = true requires consensus.checkpoint_sync = true")
        );
    }
}
