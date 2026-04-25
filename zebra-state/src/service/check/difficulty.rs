//! Block difficulty adjustment calculations for contextual validation.
//!
//! This module supports the following consensus rule calculations:
//!  * `ThresholdBits` from the Zcash Specification,
//!  * the Testnet minimum difficulty adjustment from ZIPs 205 and 208, and
//!  * `median-time-past`.

use std::cmp::{max, min};

use chrono::{DateTime, Duration, Utc};

use zebra_chain::{
    block::{self, Block},
    parameters::{Network, NetworkUpgrade},
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, ParameterDifficulty as _, U256},
};

/// The maximum number of seconds between the `median-time-past` of a block,
/// and the block's `time` field.
///
/// Part of the block header consensus rules in the Zcash specification.
pub const BLOCK_MAX_TIME_SINCE_MEDIAN: u32 = 90 * 60;

/// Returns the overall block span used for adjusting Zcash block difficulty.
///
/// `PoWAveragingWindow + PoWMedianBlockSpan` in the Zcash specification.
pub(crate) fn pow_adjustment_block_span(network: &Network) -> usize {
    network
        .pow_averaging_window()
        .saturating_add(network.pow_median_block_span())
}

/// Contains the context needed to calculate the adjusted difficulty for a block.
pub(crate) struct AdjustedDifficulty {
    /// The `header.time` field from the candidate block
    candidate_time: DateTime<Utc>,
    /// The coinbase height from the candidate block
    ///
    /// If we only have the header, this field is calculated from the previous
    /// block height.
    candidate_height: block::Height,
    /// The configured network
    network: Network,
    /// The `header.difficulty_threshold`s from the previous
    /// `PoWAveragingWindow + PoWMedianBlockSpan` blocks, in reverse height
    /// order.
    relevant_difficulty_thresholds: Vec<CompactDifficulty>,
    /// The `header.time`s from the previous
    /// `PoWAveragingWindow + PoWMedianBlockSpan` blocks, in reverse height
    /// order.
    relevant_times: Vec<DateTime<Utc>>,
}

impl AdjustedDifficulty {
    /// Initialise and return a new `AdjustedDifficulty` using a `candidate_block`,
    /// `network`, and a `context`.
    ///
    /// The `context` contains the previous
    /// `PoWAveragingWindow + PoWMedianBlockSpan` `difficulty_threshold`s and
    /// `time`s from the relevant chain for `candidate_block`, in reverse height
    /// order, starting with the previous block.
    ///
    /// Note that the `time`s might not be in reverse chronological order, because
    /// block times are supplied by miners.
    ///
    /// # Panics
    ///
    /// This function may panic in the following cases:
    /// - The `candidate_block` has no coinbase height (should never happen for valid blocks).
    /// - The `candidate_block` is the genesis block, so `previous_block_height` cannot be computed.
    /// - `AdjustedDifficulty::new_from_header_time` panics.
    pub fn new_from_block<C>(
        candidate_block: &Block,
        network: &Network,
        context: C,
    ) -> AdjustedDifficulty
    where
        C: IntoIterator<Item = (CompactDifficulty, DateTime<Utc>)>,
    {
        let candidate_block_height = candidate_block
            .coinbase_height()
            .expect("semantically valid blocks have a coinbase height");
        let previous_block_height = (candidate_block_height - 1)
            .expect("contextual validation is never run on the genesis block");

        AdjustedDifficulty::new_from_header_time(
            candidate_block.header.time,
            previous_block_height,
            network,
            context,
        )
    }

    /// Initialise and return a new [`AdjustedDifficulty`] using a
    /// `candidate_header_time`, `previous_block_height`, `network`, and a `context`.
    ///
    /// Designed for use when validating block headers, where the full block has not
    /// been downloaded yet.
    ///
    /// See [`Self::new_from_block`] for detailed information about the `context`.
    ///
    /// # Panics
    ///
    /// This function may panic in the following cases:
    /// - The next block height is invalid.
    /// - The context iterator is empty, because at least one difficulty threshold and block time are required.
    pub fn new_from_header_time<C>(
        candidate_header_time: DateTime<Utc>,
        previous_block_height: block::Height,
        network: &Network,
        context: C,
    ) -> AdjustedDifficulty
    where
        C: IntoIterator<Item = (CompactDifficulty, DateTime<Utc>)>,
    {
        let candidate_height = (previous_block_height + 1).expect("next block height is valid");
        let adjustment_block_span = pow_adjustment_block_span(network);

        let (relevant_difficulty_thresholds, relevant_times) = context
            .into_iter()
            .take(adjustment_block_span)
            .unzip::<_, _, Vec<_>, Vec<_>>();

        assert!(
            !relevant_difficulty_thresholds.is_empty() && !relevant_times.is_empty(),
            "context must provide at least one difficulty threshold and block time"
        );

        AdjustedDifficulty {
            candidate_time: candidate_header_time,
            candidate_height,
            network: network.clone(),
            relevant_difficulty_thresholds,
            relevant_times,
        }
    }

    /// Returns the candidate block's height.
    pub fn candidate_height(&self) -> block::Height {
        self.candidate_height
    }

    /// Returns the candidate block's time field.
    pub fn candidate_time(&self) -> DateTime<Utc> {
        self.candidate_time
    }

    /// Returns the configured network.
    pub fn network(&self) -> Network {
        self.network.clone()
    }

    /// Calculate the expected `difficulty_threshold` for a candidate block, based
    /// on the `candidate_time`, `candidate_height`, `network`, and the
    /// `difficulty_threshold`s and `time`s from the previous
    /// `PoWAveragingWindow + PoWMedianBlockSpan` blocks in the relevant chain.
    ///
    /// Implements `ThresholdBits` from the Zcash specification, and the Testnet
    /// minimum difficulty adjustment from ZIPs 205 and 208.
    pub fn expected_difficulty_threshold(&self) -> CompactDifficulty {
        if NetworkUpgrade::is_testnet_min_difficulty_block(
            &self.network,
            self.candidate_height,
            self.candidate_time,
            *self
                .relevant_times
                .first()
                .expect("context must provide at least one block time"),
        ) {
            assert!(
                self.network.is_a_test_network(),
                "invalid network: the minimum difficulty rule only applies on test networks"
            );
            self.network.target_difficulty_limit().to_compact()
        } else {
            self.threshold_bits()
        }
    }

    /// Calculate the `difficulty_threshold` for a candidate block, based on the
    /// `candidate_height`, `network`, and the relevant `difficulty_threshold`s and
    /// `time`s.
    ///
    /// See [`Self::expected_difficulty_threshold`] for details.
    ///
    /// Implements `ThresholdBits` from the Zcash specification. (Which excludes the
    /// Testnet minimum difficulty adjustment.)
    fn threshold_bits(&self) -> CompactDifficulty {
        let averaging_window_timespan = NetworkUpgrade::averaging_window_timespan_for_height(
            &self.network,
            self.candidate_height,
        );

        let threshold = (self.mean_target_difficulty() / averaging_window_timespan.num_seconds())
            * self.median_timespan_bounded().num_seconds();
        let threshold = min(self.network.target_difficulty_limit(), threshold);

        threshold.to_compact()
    }

    /// Calculate the arithmetic mean of the averaging window thresholds: the
    /// expanded `difficulty_threshold`s from the previous `PoWAveragingWindow`
    /// blocks in the relevant chain.
    ///
    /// Implements `MeanTarget` from the Zcash specification.
    fn mean_target_difficulty(&self) -> ExpandedDifficulty {
        // In Zebra, contextual validation starts after Canopy activation, so we
        // can assume that the relevant chain contains at least `pow_averaging_window`
        // blocks. Therefore, the `PoWLimit` case of `MeanTarget()` from the Zcash
        // specification is unreachable.

        let averaging_window = self.network.pow_averaging_window();
        let averaging_window_thresholds =
            if self.relevant_difficulty_thresholds.len() >= averaging_window {
                &self.relevant_difficulty_thresholds.as_slice()[0..averaging_window]
            } else {
                return self.network.target_difficulty_limit();
            };

        // Configured testnet parameters reject `PoWLimit` values that could overflow
        // this sum for their configured averaging window.
        let total: ExpandedDifficulty = averaging_window_thresholds
            .iter()
            .map(|compact| {
                compact
                    .to_expanded()
                    .expect("difficulty thresholds in previously verified blocks are valid")
            })
            .sum();

        let divisor: U256 = averaging_window.into();
        total / divisor
    }

    /// Calculate the bounded median timespan. The median timespan is the
    /// difference of medians of the timespan times, which are the `time`s from
    /// the previous `PoWAveragingWindow + PoWMedianBlockSpan` blocks in the
    /// relevant chain.
    ///
    /// Uses the candidate block's `height' and `network` to calculate the
    /// `AveragingWindowTimespan` for that block.
    ///
    /// The median timespan is damped by the `PoWDampingFactor`, and bounded by
    /// `PoWMaxAdjustDown` and `PoWMaxAdjustUp`.
    ///
    /// Implements `ActualTimespanBounded` from the Zcash specification.
    ///
    /// Note: This calculation only uses `PoWMedianBlockSpan` times at the
    /// start and end of the timespan times. timespan times `[11..=16]` are ignored.
    fn median_timespan_bounded(&self) -> Duration {
        let averaging_window_timespan = NetworkUpgrade::averaging_window_timespan_for_height(
            &self.network,
            self.candidate_height,
        );
        // This value is exact, but we need to truncate its nanoseconds component
        let damped_variance = (self.median_timespan() - averaging_window_timespan)
            / self.network.pow_damping_factor();
        // num_seconds truncates negative values towards zero, matching the Zcash specification
        let damped_variance = Duration::seconds(damped_variance.num_seconds());

        // `ActualTimespanDamped` in the Zcash specification
        let median_timespan_damped = averaging_window_timespan + damped_variance;

        // `MinActualTimespan` and `MaxActualTimespan` in the Zcash spec
        let min_median_timespan =
            averaging_window_timespan * (100 - self.network.pow_max_adjust_up_percent()) / 100;
        let max_median_timespan =
            averaging_window_timespan * (100 + self.network.pow_max_adjust_down_percent()) / 100;

        // `ActualTimespanBounded` in the Zcash specification
        max(
            min_median_timespan,
            min(max_median_timespan, median_timespan_damped),
        )
    }

    /// Calculate the median timespan. The median timespan is the difference of
    /// medians of the timespan times, which are the `time`s from the previous
    /// `PoWAveragingWindow + PoWMedianBlockSpan` blocks in the relevant chain.
    ///
    /// Implements `ActualTimespan` from the Zcash specification.
    ///
    /// See [`Self::median_timespan_bounded`] for details.
    fn median_timespan(&self) -> Duration {
        let newer_median = self.median_time_past();
        let averaging_window = self.network.pow_averaging_window();
        let median_block_span = self.network.pow_median_block_span();

        // MedianTime(height : N) := median([ nTime(𝑖) for 𝑖 from max(0, height − PoWMedianBlockSpan) up to max(0, height − 1) ])
        let older_median = if self.relevant_times.len() > averaging_window {
            let older_times: Vec<_> = self
                .relevant_times
                .iter()
                .skip(averaging_window)
                .cloned()
                .take(median_block_span)
                .collect();

            AdjustedDifficulty::median_time(older_times)
        } else {
            *self
                .relevant_times
                .last()
                .expect("context must provide at least one block time")
        };

        // `ActualTimespan` in the Zcash specification
        newer_median - older_median
    }

    /// Calculate the median of the `time`s from the previous
    /// `PoWMedianBlockSpan` blocks in the relevant chain.
    ///
    /// Implements `median-time-past` and `MedianTime(candidate_height)` from the
    /// Zcash specification. (These functions are identical, but they are
    /// specified in slightly different ways.)
    pub fn median_time_past(&self) -> DateTime<Utc> {
        let median_times: Vec<DateTime<Utc>> = self
            .relevant_times
            .iter()
            .take(self.network.pow_median_block_span())
            .cloned()
            .collect();

        AdjustedDifficulty::median_time(median_times)
    }

    /// Calculate the median of the `median_block_span_times`: the `time`s from a
    /// Vec of `PoWMedianBlockSpan` or fewer blocks in the relevant chain.
    ///
    /// Implements `MedianTime` from the Zcash specification.
    ///
    /// # Panics
    ///
    /// If provided an empty Vec
    pub(crate) fn median_time(mut median_block_span_times: Vec<DateTime<Utc>>) -> DateTime<Utc> {
        median_block_span_times.sort_unstable();

        // > median(𝑆) := sorted(𝑆)_{ceiling((length(𝑆)+1)/2)}
        // <https://zips.z.cash/protocol/protocol.pdf>, section 7.7.3, Difficulty Adjustment (p. 132)
        let median_idx = median_block_span_times.len() / 2;
        median_block_span_times[median_idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use zebra_chain::{
        parameters::testnet,
        work::difficulty::{CompactDifficulty, U256},
    };

    #[test]
    fn configured_testnet_uses_larger_difficulty_context() {
        let network = testnet::Parameters::build()
            .with_network_name("LargeDaaContext")
            .expect("network name is valid")
            .with_target_difficulty_limit(U256::MAX / U256::from(52u64))
            .expect("difficulty limit is valid")
            .with_pow_averaging_window(51)
            .with_pow_median_block_span(33)
            .to_network()
            .expect("configured testnet parameters are valid");

        let context = (0..100).map(|seconds| {
            (
                CompactDifficulty::default(),
                DateTime::from_timestamp(seconds, 0).expect("timestamp is valid"),
            )
        });

        let adjusted = AdjustedDifficulty::new_from_header_time(
            DateTime::from_timestamp(200, 0).expect("timestamp is valid"),
            block::Height(100),
            &network,
            context,
        );

        assert_eq!(pow_adjustment_block_span(&network), 84);
        assert_eq!(adjusted.relevant_times.len(), 84);
        assert_eq!(
            adjusted.median_time_past(),
            DateTime::from_timestamp(16, 0).expect("timestamp is valid")
        );
    }
}
