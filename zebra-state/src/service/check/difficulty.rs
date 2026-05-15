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

    use std::{
        fs::{self, File},
        io::{BufWriter, Write},
        path::PathBuf,
    };

    use zebra_chain::{
        parameters::{testnet, NetworkUpgrade},
        work::difficulty::{CompactDifficulty, ParameterDifficulty, U256},
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

    #[test]
    #[ignore = "simulation output for target-spacing activation analysis"]
    fn simulate_three_x_target_spacing_reduction() {
        const ACTIVATION_HEIGHT: u32 = 1_000;
        const PRE_ACTIVATION_TARGET_SPACING_SECONDS: i64 = 75;
        const POST_ACTIVATION_TARGET_SPACING_SECONDS: u32 = 25;
        const BLOCKS_AFTER_ACTIVATION: u32 = 160;

        let network = testnet::Parameters::build()
            .with_network_name("TargetSpacingSim")
            .expect("network name is valid")
            .with_activation_heights(testnet::ConfiguredActivationHeights {
                before_overwinter: Some(1),
                overwinter: Some(2),
                sapling: Some(3),
                blossom: Some(ACTIVATION_HEIGHT),
                heartwood: Some(ACTIVATION_HEIGHT + 1),
                canopy: Some(ACTIVATION_HEIGHT + 2),
                ..Default::default()
            })
            .expect("activation heights are valid")
            .with_pre_blossom_pow_target_spacing(PRE_ACTIVATION_TARGET_SPACING_SECONDS)
            .with_post_blossom_pow_target_spacing(POST_ACTIVATION_TARGET_SPACING_SECONDS)
            .with_testnet_min_difficulty_start_height(block::Height(u32::MAX))
            .clear_funding_streams()
            .to_network()
            .expect("configured testnet parameters are valid");

        let stable_compact = (network.target_difficulty_limit() / U256::from(1_000_u64))
            .to_compact()
            .to_expanded()
            .expect("compact-expanded stable difficulty is valid")
            .to_compact();
        let stable_relative_difficulty = stable_compact.relative_to_network(&network);
        let activation_time =
            DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp is valid");

        let mut chain: Vec<(block::Height, CompactDifficulty, DateTime<Utc>)> = (1..=28)
            .rev()
            .map(|blocks_before_activation| {
                let height = block::Height(ACTIVATION_HEIGHT - blocks_before_activation);
                let seconds_before_activation =
                    i64::from(blocks_before_activation) * PRE_ACTIVATION_TARGET_SPACING_SECONDS;
                (
                    height,
                    stable_compact,
                    activation_time - Duration::seconds(seconds_before_activation),
                )
            })
            .collect();

        println!(
            "height,blocks_after_activation,target_spacing_seconds,expected_spacing_seconds,relative_difficulty,difficulty_ratio,difficulty_threshold"
        );

        let mut simulated_time_seconds = chain
            .last()
            .expect("simulation chain has a previous block")
            .2
            // This timestamp is near 1.7e9 seconds, which fits within the
            // exactly representable integer range of f64.
            .timestamp() as f64;

        for height in ACTIVATION_HEIGHT..ACTIVATION_HEIGHT + BLOCKS_AFTER_ACTIVATION {
            let previous_height = block::Height(height - 1);
            let next_height = block::Height(height);
            let context = chain
                .iter()
                .rev()
                .map(|(_, difficulty, time)| (*difficulty, *time));
            let candidate_time = chain
                .last()
                .expect("simulation chain has a previous block")
                .2
                + Duration::seconds(1);
            let next_difficulty = AdjustedDifficulty::new_from_header_time(
                candidate_time,
                previous_height,
                &network,
                context,
            )
            .expected_difficulty_threshold();

            let relative_difficulty = next_difficulty.relative_to_network(&network);
            let difficulty_ratio = relative_difficulty / stable_relative_difficulty;
            let expected_spacing_seconds =
                // The target spacing is small enough to convert to f64 exactly.
                PRE_ACTIVATION_TARGET_SPACING_SECONDS as f64 * difficulty_ratio;
            simulated_time_seconds += expected_spacing_seconds;
            // The simulated timestamp remains near 1.7e9 seconds, which is well
            // within the exactly representable integer range of f64.
            let next_time = DateTime::from_timestamp(simulated_time_seconds.round() as i64, 0)
                .expect("timestamp is valid");
            let target_spacing =
                NetworkUpgrade::target_spacing_for_height(&network, next_height).num_seconds();

            println!(
                "{height},{},{target_spacing},{expected_spacing_seconds:.3},{relative_difficulty:.9},{difficulty_ratio:.9},{next_difficulty}",
                height - ACTIVATION_HEIGHT,
            );

            chain.push((next_height, next_difficulty, next_time));
        }
    }

    #[derive(Clone, Copy, Debug)]
    struct DaaSimulationParameters {
        pow_averaging_window: usize,
        pow_median_block_span: usize,
        pow_damping_factor: i32,
        pow_max_adjust_up_percent: i32,
        pow_max_adjust_down_percent: i32,
    }

    impl Default for DaaSimulationParameters {
        fn default() -> Self {
            Self {
                pow_averaging_window: 17,
                pow_median_block_span: 11,
                pow_damping_factor: 4,
                pow_max_adjust_up_percent: 16,
                pow_max_adjust_down_percent: 32,
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    struct HashRateShockScenario {
        name: &'static str,
        target_spacing_seconds: i64,
        hash_rate_percent_after_shock: f64,
        blocks_after_shock: u32,
        recovery_tolerance_percent: f64,
        daa: DaaSimulationParameters,
    }

    impl HashRateShockScenario {
        fn with_daa(
            self,
            daa: DaaSimulationParameters,
            name: &'static str,
        ) -> HashRateShockScenario {
            HashRateShockScenario { name, daa, ..self }
        }

        fn hash_rate_factor_after_shock(self) -> f64 {
            self.hash_rate_percent_after_shock / 100.0
        }
    }

    #[derive(Clone, Copy, Debug)]
    struct HashRateShockRecovery {
        blocks_after_shock: u32,
        elapsed_minutes: f64,
        expected_spacing_seconds: f64,
        spacing_error_percent: f64,
    }

    #[test]
    #[ignore = "simulation output for named hash-rate shock benchmark cases"]
    fn benchmark_hash_rate_shock_daa_configurations() {
        const BLOCKS_AFTER_SHOCK: u32 = 720;
        const RECOVERY_TOLERANCE_PERCENT: f64 = 20.0;

        let default_75_percent_drop = HashRateShockScenario {
            name: "25s_75_percent_drop_default",
            target_spacing_seconds: 25,
            hash_rate_percent_after_shock: 25.0,
            blocks_after_shock: BLOCKS_AFTER_SHOCK,
            recovery_tolerance_percent: RECOVERY_TOLERANCE_PERCENT,
            daa: DaaSimulationParameters::default(),
        };
        let default_75s_75_percent_drop = HashRateShockScenario {
            name: "75s_75_percent_drop_default",
            target_spacing_seconds: 75,
            hash_rate_percent_after_shock: 25.0,
            blocks_after_shock: BLOCKS_AFTER_SHOCK,
            recovery_tolerance_percent: RECOVERY_TOLERANCE_PERCENT,
            daa: DaaSimulationParameters::default(),
        };
        let default_4x_increase = HashRateShockScenario {
            name: "25s_4x_increase_default",
            target_spacing_seconds: 25,
            hash_rate_percent_after_shock: 400.0,
            blocks_after_shock: BLOCKS_AFTER_SHOCK,
            recovery_tolerance_percent: RECOVERY_TOLERANCE_PERCENT,
            daa: DaaSimulationParameters::default(),
        };
        let triple_average_window = DaaSimulationParameters {
            pow_averaging_window: DaaSimulationParameters::default().pow_averaging_window * 3,
            ..DaaSimulationParameters::default()
        };
        let max_adjust_down_9_percent = DaaSimulationParameters {
            pow_max_adjust_down_percent: 9,
            ..DaaSimulationParameters::default()
        };

        let benchmark_scenarios = [
            default_75_percent_drop,
            default_75s_75_percent_drop,
            default_4x_increase,
            default_75_percent_drop.with_daa(
                triple_average_window,
                "25s_75_percent_drop_3x_average_window",
            ),
            default_4x_increase
                .with_daa(triple_average_window, "25s_4x_increase_3x_average_window"),
            default_75_percent_drop.with_daa(
                max_adjust_down_9_percent,
                "25s_75_percent_drop_max_down_9_percent",
            ),
        ];

        let output_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("target")
            .join("hash-rate-shock-sim");
        fs::create_dir_all(&output_dir).expect("simulation output directory can be created");
        let output_dir = output_dir
            .canonicalize()
            .expect("simulation output directory has a canonical path");

        println!(
            "scenario,file,recovered_after_blocks,elapsed_minutes,expected_spacing_seconds,spacing_error_percent"
        );

        for scenario in benchmark_scenarios {
            let output_path = output_dir.join(format!("{}.csv", scenario.name));
            let output_file =
                File::create(&output_path).expect("simulation output file can be created");
            let mut writer = BufWriter::new(output_file);
            write_hash_rate_shock_csv_header(&mut writer);

            let recovery = simulate_hash_rate_shock_recovery(scenario, Some(&mut writer));
            writer.flush().expect("simulation output can be flushed");

            if let Some(recovery) = recovery {
                println!(
                    "{},{},{},{:.3},{:.3},{:.3}",
                    scenario.name,
                    output_path.display(),
                    recovery.blocks_after_shock,
                    recovery.elapsed_minutes,
                    recovery.expected_spacing_seconds,
                    recovery.spacing_error_percent,
                );
            } else {
                println!(
                    "{},{},not_recovered,not_recovered,not_recovered,not_recovered",
                    scenario.name,
                    output_path.display(),
                );
            }
        }
    }

    #[test]
    #[ignore = "simulation output for hash-rate shock parameter analysis"]
    fn sweep_hash_rate_shock_adjustment_limits() {
        const BLOCKS_AFTER_SHOCK: u32 = 720;
        const RECOVERY_TOLERANCE_PERCENT: f64 = 20.0;

        let sweep_scenarios = [
            HashRateShockScenario {
                name: "25s_75_percent_drop",
                target_spacing_seconds: 25,
                hash_rate_percent_after_shock: 25.0,
                blocks_after_shock: BLOCKS_AFTER_SHOCK,
                recovery_tolerance_percent: RECOVERY_TOLERANCE_PERCENT,
                daa: DaaSimulationParameters::default(),
            },
            HashRateShockScenario {
                name: "25s_4x_increase",
                target_spacing_seconds: 25,
                hash_rate_percent_after_shock: 400.0,
                blocks_after_shock: BLOCKS_AFTER_SHOCK,
                recovery_tolerance_percent: RECOVERY_TOLERANCE_PERCENT,
                daa: DaaSimulationParameters::default(),
            },
        ];

        println!(
            "scenario,pow_max_adjust_down_percent,pow_max_adjust_up_percent,recovered_after_blocks,elapsed_minutes,expected_spacing_seconds,spacing_error_percent"
        );

        for scenario in sweep_scenarios {
            for max_adjust_percent in 1..=64 {
                let mut daa = scenario.daa;

                if scenario.hash_rate_percent_after_shock < 100.0 {
                    daa.pow_max_adjust_down_percent = max_adjust_percent;
                } else {
                    daa.pow_max_adjust_up_percent = max_adjust_percent;
                }

                let scenario = scenario.with_daa(daa, scenario.name);
                let recovery = simulate_hash_rate_shock_recovery(scenario, None);

                if let Some(recovery) = recovery {
                    println!(
                        "{},{},{},{},{:.3},{:.3},{:.3}",
                        scenario.name,
                        scenario.daa.pow_max_adjust_down_percent,
                        scenario.daa.pow_max_adjust_up_percent,
                        recovery.blocks_after_shock,
                        recovery.elapsed_minutes,
                        recovery.expected_spacing_seconds,
                        recovery.spacing_error_percent,
                    );
                } else {
                    println!(
                        "{},{},{},not_recovered,not_recovered,not_recovered,not_recovered",
                        scenario.name,
                        scenario.daa.pow_max_adjust_down_percent,
                        scenario.daa.pow_max_adjust_up_percent,
                    );
                }
            }
        }
    }

    fn simulate_hash_rate_shock_recovery(
        scenario: HashRateShockScenario,
        mut row_writer: Option<&mut dyn Write>,
    ) -> Option<HashRateShockRecovery> {
        const SHOCK_HEIGHT: u32 = 1_000;
        const BLOSSOM_ACTIVATION_HEIGHT: u32 = 4;

        assert!(
            scenario.target_spacing_seconds > 0,
            "target spacing must be positive"
        );
        assert!(
            scenario.hash_rate_percent_after_shock > 0.0,
            "hash-rate percent must be positive"
        );
        assert!(
            scenario.recovery_tolerance_percent >= 0.0,
            "recovery tolerance must not be negative"
        );

        let target_difficulty_limit = U256::MAX
            / U256::from(
                u64::try_from(scenario.daa.pow_averaging_window.saturating_add(1))
                    .expect("configured averaging window fits in u64"),
            );

        let network = testnet::Parameters::build()
            .with_network_name("HashRateShockSim")
            .expect("network name is valid")
            .with_target_difficulty_limit(target_difficulty_limit)
            .expect("difficulty limit is valid")
            .with_activation_heights(testnet::ConfiguredActivationHeights {
                before_overwinter: Some(1),
                overwinter: Some(2),
                sapling: Some(3),
                blossom: Some(BLOSSOM_ACTIVATION_HEIGHT),
                heartwood: Some(BLOSSOM_ACTIVATION_HEIGHT + 1),
                canopy: Some(BLOSSOM_ACTIVATION_HEIGHT + 2),
                ..Default::default()
            })
            .expect("activation heights are valid")
            .with_pre_blossom_pow_target_spacing(scenario.target_spacing_seconds)
            .with_post_blossom_pow_target_spacing(
                u32::try_from(scenario.target_spacing_seconds).expect("target spacing fits in u32"),
            )
            .with_pow_averaging_window(scenario.daa.pow_averaging_window)
            .with_pow_median_block_span(scenario.daa.pow_median_block_span)
            .with_pow_damping_factor(scenario.daa.pow_damping_factor)
            .with_pow_max_adjust_up_percent(scenario.daa.pow_max_adjust_up_percent)
            .with_pow_max_adjust_down_percent(scenario.daa.pow_max_adjust_down_percent)
            .with_testnet_min_difficulty_start_height(block::Height(u32::MAX))
            .clear_funding_streams()
            .to_network()
            .expect("configured testnet parameters are valid");

        let stable_compact = (network.target_difficulty_limit() / U256::from(1_000_u64))
            .to_compact()
            .to_expanded()
            .expect("compact-expanded stable difficulty is valid")
            .to_compact();
        let stable_relative_difficulty = stable_compact.relative_to_network(&network);
        let shock_time = DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp is valid");
        let context_block_count = u32::try_from(pow_adjustment_block_span(&network))
            .expect("difficulty adjustment block span fits in u32");

        let previous_context_blocks = 1..=context_block_count;
        let mut chain: Vec<(block::Height, CompactDifficulty, DateTime<Utc>)> =
            previous_context_blocks
                .rev()
                .map(|blocks_before_shock| {
                    let height = block::Height(SHOCK_HEIGHT - blocks_before_shock);
                    let seconds_before_shock =
                        i64::from(blocks_before_shock) * scenario.target_spacing_seconds;
                    (
                        height,
                        stable_compact,
                        shock_time - Duration::seconds(seconds_before_shock),
                    )
                })
                .collect();

        // The last pre-shock block is anchored at `shock_time -
        // target_spacing_seconds` (see the chain construction above:
        // `blocks_before_shock == 1` => `shock_time - target_spacing`).
        // Post-shock block times are `shock_time + elapsed_since_shock_seconds`.
        // Starting the accumulator at 0 would place the first post-shock block
        // a full `target_spacing` too late, injecting one extra target-spacing
        // of slowness into the first difficulty-averaging window (the boundary
        // "seam"). Seed it one target-spacing back so the first post-shock
        // interval is exactly one (hash-rate-adjusted) spacing, not two.
        let mut elapsed_since_shock_seconds = -(scenario.target_spacing_seconds as f64);
        let mut recovery = None;

        for height in SHOCK_HEIGHT..SHOCK_HEIGHT + scenario.blocks_after_shock {
            let previous_height = block::Height(height - 1);
            let next_height = block::Height(height);
            let context = chain
                .iter()
                .rev()
                .map(|(_, difficulty, time)| (*difficulty, *time));
            let previous_relative_difficulty = chain
                .last()
                .expect("simulation chain has a previous block")
                .1
                .relative_to_network(&network);
            let candidate_time = chain
                .last()
                .expect("simulation chain has a previous block")
                .2
                + Duration::seconds(1);
            let next_difficulty = AdjustedDifficulty::new_from_header_time(
                candidate_time,
                previous_height,
                &network,
                context,
            )
            .expected_difficulty_threshold();

            let relative_difficulty = next_difficulty.relative_to_network(&network);
            let difficulty_ratio = relative_difficulty / stable_relative_difficulty;
            // The target spacing is small enough to convert to f64 exactly.
            let target_spacing_seconds = scenario.target_spacing_seconds as f64;
            let expected_spacing_seconds =
                target_spacing_seconds * difficulty_ratio / scenario.hash_rate_factor_after_shock();
            elapsed_since_shock_seconds += expected_spacing_seconds;
            // The simulated timestamp remains near 1.7e9 seconds, which is well
            // within the exactly representable integer range of f64.
            let next_time = DateTime::from_timestamp(
                shock_time.timestamp() + elapsed_since_shock_seconds.round() as i64,
                0,
            )
            .expect("timestamp is valid");
            let target_spacing =
                NetworkUpgrade::target_spacing_for_height(&network, next_height).num_seconds();
            let elapsed_since_shock_minutes = elapsed_since_shock_seconds / 60.0;
            let spacing_error_percent = ((expected_spacing_seconds - target_spacing_seconds).abs()
                / target_spacing_seconds)
                * 100.0;
            let difficulty_change_percent = ((relative_difficulty - previous_relative_difficulty)
                / previous_relative_difficulty)
                * 100.0;
            let difficulty_change_from_stable_percent =
                ((relative_difficulty - stable_relative_difficulty) / stable_relative_difficulty)
                    * 100.0;
            let difficulty_work = next_difficulty
                .to_work()
                .expect("simulated difficulty thresholds are valid work")
                .as_u128();
            // Work bits are approximate and only used as a human-readable display metric.
            let difficulty_work_bits = (difficulty_work as f64).log2();

            if let Some(writer) = row_writer.as_deref_mut() {
                writeln!(
                    writer,
                    "{},{height},{},{target_spacing},{:.3},{},{},{},{},{},{expected_spacing_seconds:.3},{elapsed_since_shock_seconds:.3},{elapsed_since_shock_minutes:.3},{spacing_error_percent:.3},{relative_difficulty:.9},{difficulty_ratio:.9},{difficulty_change_percent:.9},{difficulty_change_from_stable_percent:.9},{difficulty_work_bits:.9},{next_difficulty}",
                    scenario.name,
                    height - SHOCK_HEIGHT,
                    scenario.hash_rate_percent_after_shock,
                    scenario.daa.pow_averaging_window,
                    scenario.daa.pow_median_block_span,
                    scenario.daa.pow_damping_factor,
                    scenario.daa.pow_max_adjust_up_percent,
                    scenario.daa.pow_max_adjust_down_percent,
                )
                .expect("simulation output row can be written");
            }

            if recovery.is_none() && spacing_error_percent <= scenario.recovery_tolerance_percent {
                recovery = Some(HashRateShockRecovery {
                    blocks_after_shock: height - SHOCK_HEIGHT,
                    elapsed_minutes: elapsed_since_shock_minutes,
                    expected_spacing_seconds,
                    spacing_error_percent,
                });
            }

            chain.push((next_height, next_difficulty, next_time));
        }

        recovery
    }

    fn write_hash_rate_shock_csv_header(writer: &mut dyn Write) {
        writeln!(
            writer,
            "scenario,height,blocks_after_shock,target_spacing_seconds,hash_rate_percent,pow_averaging_window,pow_median_block_span,pow_damping_factor,pow_max_adjust_up_percent,pow_max_adjust_down_percent,expected_spacing_seconds,elapsed_since_shock_seconds,elapsed_since_shock_minutes,spacing_error_percent,relative_difficulty,difficulty_ratio,difficulty_change_percent,difficulty_change_from_stable_percent,difficulty_work_bits,difficulty_threshold"
        )
        .expect("simulation output header can be written");
    }
}
