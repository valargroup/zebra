//! Forked Mainnet consensus parameters.

use std::{collections::BTreeMap, fmt, sync::Arc};

use thiserror::Error;

use crate::{
    block::{self, Height},
    parameters::{
        checkpoint::list::CheckpointList, constants::magics, network::magic::Magic, Network,
        NetworkUpgrade,
    },
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, ParameterDifficulty as _},
};

/// Maximum length for a fork name.
pub const MAX_FORK_NAME_LENGTH: usize = 30;

/// Reserved names that should not be allowed for Mainnet forks.
pub const RESERVED_FORK_NAMES: [&str; 4] = ["Mainnet", "Testnet", "Regtest", "ForkedMainnet"];

/// Errors returned while constructing forked Mainnet parameters.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ParametersError {
    /// The configured fork name is reserved.
    #[error("fork name should not use a reserved network name: {fork_name}")]
    ReservedForkName {
        /// The invalid fork name.
        fork_name: String,
    },

    /// The configured fork name is too long.
    #[error("fork name must be {max_length} characters or less: {fork_name}")]
    ForkNameTooLong {
        /// The invalid fork name.
        fork_name: String,
        /// The maximum allowed length.
        max_length: usize,
    },

    /// The configured fork name contains invalid characters.
    #[error("fork name must only contain alphanumeric characters or underscores")]
    InvalidForkNameCharacter,

    /// The configured network magic is reserved for an existing network.
    #[error("fork network magic should be distinct from reserved network magics")]
    ReservedNetworkMagic,

    /// The configured fork height is below Zebra's mandatory Mainnet checkpoint.
    #[error("fork height {fork_height:?} must be at or above the mandatory Mainnet checkpoint height {mandatory_checkpoint_height:?}")]
    ForkHeightBeforeMandatoryCheckpoint {
        /// The invalid fork height.
        fork_height: Height,
        /// The mandatory Mainnet checkpoint height.
        mandatory_checkpoint_height: Height,
    },

    /// The configured fork height is above Zebra's maximum valid height.
    #[error("fork height {fork_height:?} must be less than or equal to Height::MAX")]
    ForkHeightAboveMax {
        /// The invalid fork height.
        fork_height: Height,
    },

    /// The configured post-fork activation height is above Zebra's maximum valid height.
    #[error("post-fork activation height {activation_height:?} must be less than or equal to Height::MAX")]
    PostForkActivationAboveMax {
        /// The invalid activation height.
        activation_height: Height,
    },

    /// A post-fork activation was configured at or before the fork height.
    #[error("post-fork activation height {activation_height:?} must be greater than fork height {fork_height:?}")]
    PostForkActivationAtOrBeforeFork {
        /// The configured fork height.
        fork_height: Height,
        /// The invalid activation height.
        activation_height: Height,
    },

    /// A post-fork activation tried to activate Genesis.
    #[error("Genesis cannot be configured as a post-fork activation")]
    PostForkGenesisActivation,

    /// A post-fork activation tried to reactivate a network upgrade active at the fork.
    #[error("post-fork activation {network_upgrade} must be after the Mainnet upgrade active at the fork: {current_mainnet_upgrade}")]
    PostForkActivationNotAfterCurrentMainnet {
        /// The currently active Mainnet network upgrade at the fork height.
        current_mainnet_upgrade: NetworkUpgrade,
        /// The invalid post-fork network upgrade.
        network_upgrade: NetworkUpgrade,
    },

    /// Post-fork activations were configured out of network upgrade order.
    #[error("post-fork activation heights must be in network upgrade order")]
    OutOfOrderPostForkActivations,

    /// The configured target difficulty limit is invalid.
    #[error("post-fork target difficulty limit must be a valid compact difficulty")]
    InvalidTargetDifficultyLimit,

    /// The configured fork hash conflicts with an existing Mainnet checkpoint at the fork height.
    #[error("fork hash conflicts with an existing Mainnet checkpoint at the fork height")]
    ForkHashCheckpointMismatch,

    /// The checkpoint list could not be constructed.
    #[error("checkpoint list must be valid")]
    InvalidCheckpoints,
}

/// Consensus parameters for a Mainnet fork.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Parameters {
    /// The name used to distinguish this fork in display output and cache names.
    network_name: String,
    /// The human-readable fork name.
    fork_name: String,
    /// The Mainnet height where the fork anchors.
    fork_height: Height,
    /// The expected Mainnet block hash at `fork_height`.
    fork_hash: block::Hash,
    /// The network magic used by forked peers.
    network_magic: Magic,
    /// Network upgrade activations that apply strictly after `fork_height`.
    post_fork_activation_heights: BTreeMap<Height, NetworkUpgrade>,
    /// The easiest target difficulty allowed strictly after `fork_height`.
    post_fork_target_difficulty_limit: ExpandedDifficulty,
    /// Whether proof-of-work validation should be disabled strictly after `fork_height`.
    disable_pow_after_fork: bool,
    /// Mainnet checkpoints up to `fork_height`, including the configured fork anchor.
    checkpoints: Arc<CheckpointList>,
}

impl Parameters {
    /// Creates validated [`Parameters`] for a Mainnet fork.
    pub fn new(
        fork_name: impl fmt::Display,
        fork_height: Height,
        fork_hash: block::Hash,
        network_magic: Magic,
        post_fork_activation_heights: BTreeMap<Height, NetworkUpgrade>,
        post_fork_target_difficulty_limit: CompactDifficulty,
        disable_pow_after_fork: bool,
    ) -> Result<Self, ParametersError> {
        let fork_name = fork_name.to_string();
        validate_fork_name(&fork_name)?;
        validate_network_magic(network_magic)?;
        validate_fork_height(fork_height)?;
        validate_post_fork_activation_heights(fork_height, &post_fork_activation_heights)?;

        // Reject difficulty limits whose work value overflows `u128`. The
        // non-finalized chain accumulates block work as a `u128`, and the
        // disable-pow path skips the work validation that normally guarantees a
        // valid work value, so an overflowing limit would panic `Chain::push`
        // on the first post-fork block instead of being rejected here.
        if post_fork_target_difficulty_limit.to_work().is_none() {
            return Err(ParametersError::InvalidTargetDifficultyLimit);
        }

        let post_fork_target_difficulty_limit = post_fork_target_difficulty_limit
            .to_expanded()
            .ok_or(ParametersError::InvalidTargetDifficultyLimit)?;

        let checkpoints = mainnet_checkpoints_through_fork(fork_height, fork_hash)?;
        let network_name = format!("ForkedMainnet_{fork_name}");

        Ok(Self {
            network_name,
            fork_name,
            fork_height,
            fork_hash,
            network_magic,
            post_fork_activation_heights,
            post_fork_target_difficulty_limit,
            disable_pow_after_fork,
            checkpoints,
        })
    }

    /// Returns the network display name.
    pub fn network_name(&self) -> &str {
        &self.network_name
    }

    /// Returns the configured fork name.
    pub fn fork_name(&self) -> &str {
        &self.fork_name
    }

    /// Returns the Mainnet height where the fork anchors.
    pub fn fork_height(&self) -> Height {
        self.fork_height
    }

    /// Returns the expected Mainnet block hash at the fork height.
    pub fn fork_hash(&self) -> block::Hash {
        self.fork_hash
    }

    /// Returns true if `height` is strictly after the fork anchor.
    pub fn is_post_fork_height(&self, height: Height) -> bool {
        height > self.fork_height
    }

    /// Returns the network magic used by forked peers.
    pub fn network_magic(&self) -> Magic {
        self.network_magic
    }

    /// Returns the post-fork network upgrade activation heights.
    pub fn post_fork_activation_heights(&self) -> &BTreeMap<Height, NetworkUpgrade> {
        &self.post_fork_activation_heights
    }

    /// Returns Mainnet activation heights up to the fork point plus configured post-fork activations.
    pub fn activation_heights(&self) -> BTreeMap<Height, NetworkUpgrade> {
        let mut activation_heights: BTreeMap<_, _> = Network::Mainnet
            .activation_list()
            .into_iter()
            .filter(|(height, _)| *height <= self.fork_height)
            .collect();

        activation_heights.extend(
            self.post_fork_activation_heights
                .iter()
                .map(|(&height, &network_upgrade)| (height, network_upgrade)),
        );

        activation_heights
    }

    /// Returns the post-fork target difficulty limit.
    pub fn post_fork_target_difficulty_limit(&self) -> ExpandedDifficulty {
        self.post_fork_target_difficulty_limit
    }

    /// Returns the target difficulty limit for `height`.
    pub fn target_difficulty_limit_at_height(&self, height: Height) -> ExpandedDifficulty {
        if self.is_post_fork_height(height) {
            self.post_fork_target_difficulty_limit
        } else {
            Network::Mainnet.target_difficulty_limit()
        }
    }

    /// Returns true if proof-of-work validation should be disabled after the fork.
    pub fn disable_pow_after_fork(&self) -> bool {
        self.disable_pow_after_fork
    }

    /// Returns true if proof-of-work validation should be disabled at `height`.
    pub fn disable_pow_at_height(&self, height: Height) -> bool {
        self.is_post_fork_height(height) && self.disable_pow_after_fork
    }

    /// Returns the checkpoint list for this fork.
    pub fn checkpoints(&self) -> Arc<CheckpointList> {
        self.checkpoints.clone()
    }
}

fn validate_fork_name(fork_name: &str) -> Result<(), ParametersError> {
    if RESERVED_FORK_NAMES.contains(&fork_name) {
        return Err(ParametersError::ReservedForkName {
            fork_name: fork_name.to_string(),
        });
    }

    if fork_name.len() > MAX_FORK_NAME_LENGTH {
        return Err(ParametersError::ForkNameTooLong {
            fork_name: fork_name.to_string(),
            max_length: MAX_FORK_NAME_LENGTH,
        });
    }

    if !fork_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(ParametersError::InvalidForkNameCharacter);
    }

    Ok(())
}

fn validate_network_magic(network_magic: Magic) -> Result<(), ParametersError> {
    if [magics::MAINNET, magics::TESTNET, magics::REGTEST]
        .into_iter()
        .any(|reserved_magic| reserved_magic == network_magic)
    {
        Err(ParametersError::ReservedNetworkMagic)
    } else {
        Ok(())
    }
}

fn validate_fork_height(fork_height: Height) -> Result<(), ParametersError> {
    let mandatory_checkpoint_height = Network::Mainnet.mandatory_checkpoint_height();

    if fork_height > Height::MAX {
        Err(ParametersError::ForkHeightAboveMax { fork_height })
    } else if fork_height < mandatory_checkpoint_height {
        Err(ParametersError::ForkHeightBeforeMandatoryCheckpoint {
            fork_height,
            mandatory_checkpoint_height,
        })
    } else {
        Ok(())
    }
}

fn validate_post_fork_activation_heights(
    fork_height: Height,
    post_fork_activation_heights: &BTreeMap<Height, NetworkUpgrade>,
) -> Result<(), ParametersError> {
    let current_mainnet_upgrade = NetworkUpgrade::current(&Network::Mainnet, fork_height);
    let mut previous_network_upgrade = current_mainnet_upgrade;

    for (&activation_height, &network_upgrade) in post_fork_activation_heights {
        if activation_height > Height::MAX {
            return Err(ParametersError::PostForkActivationAboveMax { activation_height });
        }

        if activation_height <= fork_height {
            return Err(ParametersError::PostForkActivationAtOrBeforeFork {
                fork_height,
                activation_height,
            });
        }

        if network_upgrade == NetworkUpgrade::Genesis {
            return Err(ParametersError::PostForkGenesisActivation);
        }

        if network_upgrade <= current_mainnet_upgrade {
            return Err(ParametersError::PostForkActivationNotAfterCurrentMainnet {
                current_mainnet_upgrade,
                network_upgrade,
            });
        }

        if network_upgrade <= previous_network_upgrade {
            return Err(ParametersError::OutOfOrderPostForkActivations);
        }

        previous_network_upgrade = network_upgrade;
    }

    Ok(())
}

fn mainnet_checkpoints_through_fork(
    fork_height: Height,
    fork_hash: block::Hash,
) -> Result<Arc<CheckpointList>, ParametersError> {
    let mut checkpoints: BTreeMap<_, _> = Network::Mainnet
        .checkpoint_list()
        .iter_cloned()
        .filter(|(height, _)| *height <= fork_height)
        .collect();

    if checkpoints
        .insert(fork_height, fork_hash)
        .is_some_and(|checkpoint_hash| checkpoint_hash != fork_hash)
    {
        return Err(ParametersError::ForkHashCheckpointMismatch);
    }

    CheckpointList::from_list(checkpoints)
        .map(Arc::new)
        .map_err(|_| ParametersError::InvalidCheckpoints)
}
