use color_eyre::eyre::{bail, eyre, Result};

use zebra_chain::{block, parameters::Network};

use crate::mode::RunMode;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RangePlan {
    pub first_height: u32,
    pub requested_last: u32,
    pub measured_checkpoint: block::Height,
    pub load_checkpoint: block::Height,
}

pub fn parse_network(network: &str) -> Result<Network> {
    match network.to_ascii_lowercase().as_str() {
        "mainnet" => Ok(Network::Mainnet),
        other => bail!("only --network mainnet is supported for now (got {other:?})"),
    }
}

pub fn plan_checkpoint_range(
    network: &Network,
    state_tip: Option<block::Height>,
    blocks: u32,
    mode: RunMode,
    with_roots: bool,
) -> Result<RangePlan> {
    if blocks == 0 {
        bail!("--blocks must be greater than 0");
    }

    let (checkpoint_list, _) =
        zebra_consensus::router::init_checkpoint_list(zebra_consensus::Config::default(), network);
    let first_height = state_tip.map_or(0, |height| height.0.saturating_add(1));
    let requested_last = first_height.saturating_add(blocks).saturating_sub(1);
    let measured_checkpoint = checkpoint_list
        .max_height_in_range(block::Height(first_height)..=block::Height(requested_last))
        .ok_or_else(|| {
            eyre!(
                "blocks {first_height}..={requested_last} reach no checkpoint above the anchor; \
                 increase --blocks"
            )
        })?;

    let load_checkpoint = if matches!(mode, RunMode::ApplyQueue) && with_roots {
        measured_checkpoint
            .next()
            .ok()
            .and_then(|next| checkpoint_list.min_height_in_range(next..))
            .unwrap_or(measured_checkpoint)
    } else {
        measured_checkpoint
    };

    Ok(RangePlan {
        first_height,
        requested_last,
        measured_checkpoint,
        load_checkpoint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_queue_roots_plans_one_checkpoint_of_lookahead() {
        let plan = plan_checkpoint_range(
            &Network::Mainnet,
            Some(block::Height(1_707_210)),
            800,
            RunMode::ApplyQueue,
            true,
        )
        .unwrap();

        assert_eq!(plan.first_height, 1_707_211);
        assert_eq!(plan.requested_last, 1_708_010);
        assert_eq!(plan.measured_checkpoint, block::Height(1_707_981));
        assert_eq!(plan.load_checkpoint, block::Height(1_708_054));
    }

    #[test]
    fn apply_queue_roots_plans_large_sandblasting_range() {
        let plan = plan_checkpoint_range(
            &Network::Mainnet,
            Some(block::Height(1_707_210)),
            200_000,
            RunMode::ApplyQueue,
            true,
        )
        .unwrap();

        assert_eq!(plan.measured_checkpoint, block::Height(1_907_065));
        assert_eq!(plan.load_checkpoint, block::Height(1_907_465));
    }

    #[test]
    fn direct_mode_does_not_add_lookahead() {
        let plan = plan_checkpoint_range(
            &Network::Mainnet,
            Some(block::Height(1_707_210)),
            800,
            RunMode::DirectVerifier,
            true,
        )
        .unwrap();

        assert_eq!(plan.measured_checkpoint, block::Height(1_707_981));
        assert_eq!(plan.load_checkpoint, plan.measured_checkpoint);
    }
}
