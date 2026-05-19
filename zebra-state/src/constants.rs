//! Constants that impact state behaviour.

use lazy_static::lazy_static;
use regex::Regex;
use semver::Version;

use zebra_chain::{
    block,
    parameters::{Network, NetworkUpgrade},
};

// For doc comment links
#[allow(unused_imports)]
use crate::{
    config::{self, Config},
    constants,
};

pub use zebra_chain::transparent::MIN_TRANSPARENT_COINBASE_MATURITY;

/// The maximum chain reorganisation height before the NU7 network upgrade.
///
/// Aligned with [`MIN_TRANSPARENT_COINBASE_MATURITY`] − 1 so that a matured
/// coinbase output (and any transaction spending it) cannot be reverted by a
/// non-finalized reorg.
pub const PRE_NU7_MAX_BLOCK_REORG_HEIGHT: u32 = MIN_TRANSPARENT_COINBASE_MATURITY - 1;

/// The maximum chain reorganisation height from the NU7 network upgrade onward.
///
/// Scaled so that the wall-clock timespan of the rollback window matches a
/// reference of 100 blocks at 300 s per block (30 000 s), which at the
/// post-NU7 25 s target spacing equals `100 * (300 / 25) = 1200` blocks.
pub const POST_NU7_MAX_BLOCK_REORG_HEIGHT: u32 = 1200;

/// The maximum chain reorganisation height across all regimes.
///
/// This is the upper bound used to size static memory budgets such as
/// [`crate::service::write::PARENT_ERROR_MAP_LIMIT`], [`MAX_NON_FINALIZED_CHAIN_FORKS`],
/// and [`MAX_INVALIDATED_BLOCKS`]. It must always be at least as large as the
/// active reorg limit returned by [`max_block_reorg_height`].
///
/// Callers making finalization decisions or bounding lookback by the *active*
/// reorg limit at a given height must use [`max_block_reorg_height`] instead.
///
/// This threshold determines the maximum length of the best non-finalized chain.
/// Larger reorganisations are outside Zebra's rollback window because older
/// blocks have already been finalized.
//
// TODO: change to HeightDiff
pub const MAX_BLOCK_REORG_HEIGHT: u32 = POST_NU7_MAX_BLOCK_REORG_HEIGHT;

/// Returns the active maximum chain reorganisation height for `network` at
/// `tip_height`.
///
/// Selects [`PRE_NU7_MAX_BLOCK_REORG_HEIGHT`] for heights below NU7 activation,
/// and [`POST_NU7_MAX_BLOCK_REORG_HEIGHT`] from NU7 onward.
pub fn max_block_reorg_height(network: &Network, tip_height: block::Height) -> u32 {
    if NetworkUpgrade::current(network, tip_height) < NetworkUpgrade::Nu7 {
        PRE_NU7_MAX_BLOCK_REORG_HEIGHT
    } else {
        POST_NU7_MAX_BLOCK_REORG_HEIGHT
    }
}

/// The directory name used to distinguish the state database from Zebra's other databases or flat files.
pub const STATE_DATABASE_KIND: &str = "state";

/// The database format major version, incremented each time the on-disk database format has a
/// breaking data format change.
///
/// Breaking changes include:
/// - deleting a column family, or
/// - changing a column family's data format in an incompatible way.
///
/// Breaking changes become minor version changes if:
/// - we previously added compatibility code, and
/// - it's available in all supported Zebra versions.
///
/// Instead of using this constant directly, use [`constants::state_database_format_version_in_code()`]
/// or [`config::database_format_version_on_disk()`] to get the full semantic format version.
const DATABASE_FORMAT_VERSION: u64 = 27;

/// The database format minor version, incremented each time the on-disk database format has a
/// significant data format change.
///
/// Significant changes include:
/// - adding new column families,
/// - changing the format of a column family in a compatible way, or
/// - breaking changes with compatibility code in all supported Zebra versions.
const DATABASE_FORMAT_MINOR_VERSION: u64 = 1;

/// The database format patch version, incremented each time the on-disk database format has a
/// significant format compatibility fix.
const DATABASE_FORMAT_PATCH_VERSION: u64 = 0;

/// Returns the full semantic version of the currently running state database format code.
///
/// This is the version implemented by the Zebra code that's currently running,
/// the version on disk can be different.
pub fn state_database_format_version_in_code() -> Version {
    Version {
        major: DATABASE_FORMAT_VERSION,
        minor: DATABASE_FORMAT_MINOR_VERSION,
        patch: DATABASE_FORMAT_PATCH_VERSION,
        pre: semver::Prerelease::EMPTY,
        #[cfg(feature = "indexer")]
        build: semver::BuildMetadata::new("indexer").expect("hard-coded value should be valid"),
        #[cfg(not(feature = "indexer"))]
        build: semver::BuildMetadata::EMPTY,
    }
}

/// The name of the file containing the database version.
///
/// Note: This file has historically omitted the major database version.
///
/// Use [`Config::version_file_path()`] to get the path to this file.
pub(crate) const DATABASE_FORMAT_VERSION_FILE_NAME: &str = "version";

/// The maximum number of blocks to check for NU5 transactions,
/// before we assume we are on a pre-NU5 legacy chain.
///
/// Zebra usually only has to check back a few blocks on mainnet, but on testnet it can be a long
/// time between v5 transactions.
pub const MAX_LEGACY_CHAIN_BLOCKS: usize = 100_000;

/// The maximum number of non-finalized chain forks Zebra will track.
/// When this limit is reached, we drop the chain with the lowest work.
///
/// When the network is under heavy transaction load, there are around 5 active forks in the last
/// 1200 blocks. (1 fork per 240 blocks.) When block propagation is efficient, there is around
/// 1 fork per 1200 blocks.
///
/// This limits non-finalized chain memory to around:
/// `10 forks * 1200 blocks * 2 MB per block = 24 GB`
pub const MAX_NON_FINALIZED_CHAIN_FORKS: usize = 10;

/// The maximum number of block hashes allowed in `getblocks` responses in the Zcash network protocol.
pub const MAX_FIND_BLOCK_HASHES_RESULTS: u32 = 500;

/// The maximum number of block headers allowed in `getheaders` responses in the Zcash network protocol.
pub const MAX_FIND_BLOCK_HEADERS_RESULTS: u32 = 160;

/// The maximum number of invalidated block records.
///
/// This limits the memory use to around:
/// `100 entries * up to 1200 blocks * 2 MB per block = 240 GB`
pub const MAX_INVALIDATED_BLOCKS: usize = 100;

lazy_static! {
    /// Regex that matches the RocksDB error when its lock file is already open.
    pub static ref LOCK_FILE_ERROR: Regex = Regex::new("(lock file).*(temporarily unavailable)|(in use)|(being used by another process)|(Database likely already open)").expect("regex is valid");
}

#[cfg(test)]
mod tests {
    use super::*;
    use zebra_chain::parameters::{
        testnet::{self, ConfiguredActivationHeights},
        NetworkUpgrade,
    };

    /// Build a testnet [`Network`] with NU7 configured at `nu7_height`.
    fn testnet_with_nu7_at(nu7_height: u32) -> Network {
        testnet::Parameters::build()
            .with_activation_heights(ConfiguredActivationHeights {
                blossom: Some(1),
                nu7: Some(nu7_height),
                ..Default::default()
            })
            .expect("activation heights are valid")
            .clear_funding_streams()
            .to_network()
            .expect("configured testnet is valid")
    }

    #[test]
    fn max_block_reorg_height_returns_pre_nu7_below_activation() {
        let network = testnet_with_nu7_at(10);

        assert_eq!(
            max_block_reorg_height(&network, block::Height(0)),
            PRE_NU7_MAX_BLOCK_REORG_HEIGHT
        );
        assert_eq!(
            max_block_reorg_height(&network, block::Height(9)),
            PRE_NU7_MAX_BLOCK_REORG_HEIGHT
        );
    }

    #[test]
    fn max_block_reorg_height_returns_post_nu7_at_and_above_activation() {
        let network = testnet_with_nu7_at(10);

        assert_eq!(
            max_block_reorg_height(&network, block::Height(10)),
            POST_NU7_MAX_BLOCK_REORG_HEIGHT
        );
        assert_eq!(
            max_block_reorg_height(&network, block::Height(11)),
            POST_NU7_MAX_BLOCK_REORG_HEIGHT
        );
        assert_eq!(
            max_block_reorg_height(&network, block::Height(1_000_000)),
            POST_NU7_MAX_BLOCK_REORG_HEIGHT
        );
    }

    #[test]
    fn max_block_reorg_height_never_exceeds_upper_bound() {
        let network = testnet_with_nu7_at(10);
        for height in [0, 9, 10, 11, 1000] {
            let active = max_block_reorg_height(&network, block::Height(height));
            assert!(
                active <= MAX_BLOCK_REORG_HEIGHT,
                "active reorg limit must never exceed the MAX_BLOCK_REORG_HEIGHT upper bound"
            );
        }
    }

    #[test]
    fn nu7_is_post_nu7_in_helper_ordering() {
        // Sanity check: the ordering used by `max_block_reorg_height` matches
        // the explicit Nu7 variant.
        assert!(NetworkUpgrade::Nu5 < NetworkUpgrade::Nu7);
        assert!(NetworkUpgrade::Nu6 < NetworkUpgrade::Nu7);
        assert!(NetworkUpgrade::Nu6_1 < NetworkUpgrade::Nu7);
    }
}
