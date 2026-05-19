//! Tests for types and functions for the `getblocktemplate` RPC.

use zcash_keys::address::Address;
use zcash_transparent::address::TransparentAddress;

use zebra_chain::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block::Height,
    parameters::{
        subsidy::block_subsidy,
        testnet::{self, ConfiguredActivationHeights, ConfiguredFundingStreams},
        Network,
    },
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::Transaction,
};

use super::{check_block_template_supported, standard_coinbase_outputs};

#[test]
fn block_template_before_canopy_returns_error() -> Result<(), Box<dyn std::error::Error>> {
    let network = Network::new_regtest(
        ConfiguredActivationHeights {
            overwinter: Some(5),
            nu7: Some(5),
            ..Default::default()
        }
        .into(),
    );

    let error = check_block_template_supported(&network, Height(4))
        .expect_err("pre-Canopy getblocktemplate should be rejected");

    assert!(
        error.message().contains("from Canopy activation onward"),
        "unexpected error message: {error:?}"
    );
    assert!(check_block_template_supported(&network, Height(5)).is_ok());

    Ok(())
}

/// Tests that a minimal coinbase transaction can be generated.
#[test]
fn minimal_coinbase() -> Result<(), Box<dyn std::error::Error>> {
    let regtest = testnet::Parameters::build()
        .with_slow_start_interval(Height::MIN)
        .with_activation_heights(ConfiguredActivationHeights {
            nu6: Some(1),
            ..Default::default()
        })?
        .with_funding_streams(vec![ConfiguredFundingStreams {
            height_range: Some(Height(1)..Height(10)),
            recipients: None,
        }])
        .to_network()?;

    let outputs = standard_coinbase_outputs(
        &regtest,
        Height(1),
        &Address::from(TransparentAddress::PublicKeyHash([0x42; 20])),
        Amount::zero(),
        #[cfg(all(zcash_unstable = "nu7", feature = "tx_v6"))]
        Amount::zero(),
        #[cfg(zcash_unstable = "nsm")]
        Amount::zero(),
    );

    // It should be possible to generate a coinbase tx from these params.
    Transaction::new_v5_coinbase(&regtest, Height(1), outputs, vec![])
        .zcash_serialize_to_vec()?
        // Deserialization contains checks for elementary consensus rules, which must pass.
        .zcash_deserialize_into::<Transaction>()?;

    Ok(())
}

/// `standard_coinbase_outputs` routes the LTS payout into the miner's primary
/// transparent output. Increasing `lts_payout` by `delta` must increase the
/// first output's amount by exactly `delta`, with all other outputs (funding
/// streams, lockbox disbursements, miner reward at zero LTS) unchanged.
///
/// This pins down the contract the contextual verifier relies on: the claim
/// the miner makes via `coinbase_outputs` matches the per-block rate computed
/// from the pool snapshot.
#[cfg(zcash_unstable = "nsm")]
#[test]
fn standard_coinbase_outputs_route_lts_payout_into_miner_reward(
) -> Result<(), Box<dyn std::error::Error>> {
    let regtest = testnet::Parameters::build()
        .with_slow_start_interval(Height::MIN)
        .with_activation_heights(ConfiguredActivationHeights {
            nu6: Some(1),
            ..Default::default()
        })?
        .with_funding_streams(vec![ConfiguredFundingStreams {
            height_range: Some(Height(1)..Height(10)),
            recipients: None,
        }])
        .to_network()?;

    let miner_address = Address::from(TransparentAddress::PublicKeyHash([0x42; 20]));
    let miner_fee = Amount::zero();
    let lts_payout = Amount::try_from(123_456_u64)?;

    // Baseline: no LTS payout.
    let baseline = standard_coinbase_outputs(
        &regtest,
        Height(1),
        &miner_address,
        miner_fee,
        Amount::zero(),
        Amount::zero(),
    );
    // With a non-zero LTS payout.
    let with_lts = standard_coinbase_outputs(
        &regtest,
        Height(1),
        &miner_address,
        miner_fee,
        Amount::zero(),
        lts_payout,
    );

    assert_eq!(
        baseline.len(),
        with_lts.len(),
        "LTS payout must not change the number of outputs"
    );

    // The miner reward is always the first output (see standard_coinbase_outputs).
    let (baseline_miner_amount, baseline_miner_script) = &baseline[0];
    let (lts_miner_amount, lts_miner_script) = &with_lts[0];
    assert_eq!(
        baseline_miner_script, lts_miner_script,
        "miner reward script must not depend on lts_payout"
    );
    let delta = (*lts_miner_amount - *baseline_miner_amount)?;
    assert_eq!(
        lts_payout, delta,
        "miner reward must grow by exactly the lts_payout"
    );

    // All other outputs (funding streams, lockboxes) must be byte-identical
    // between the two calls — only the miner reward changes.
    assert_eq!(&baseline[1..], &with_lts[1..]);

    Ok(())
}

/// Fee-bearing generated coinbases must round-trip through the same
/// value-balance equation used by the semantic and contextual verifiers.
#[cfg(all(zcash_unstable = "nsm", zcash_unstable = "zip235", feature = "tx_v6"))]
#[test]
fn generated_coinbase_implies_only_lts_payout_with_zip233() -> Result<(), Box<dyn std::error::Error>>
{
    let regtest = testnet::Parameters::build()
        .with_slow_start_interval(Height::MIN)
        .with_activation_heights(ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        })?
        .to_network()?;

    let height = Height(2);
    let miner_address = Address::from(TransparentAddress::PublicKeyHash([0x42; 20]));
    let miner_fee = Amount::try_from(10_000_u64)?;
    let lts_payout = Amount::try_from(123_u64)?;
    let zip233_amount = zebra_chain::transaction::builder::zip235_minimum_zip233_amount(miner_fee);
    let outputs = standard_coinbase_outputs(
        &regtest,
        height,
        &miner_address,
        miner_fee,
        zip233_amount,
        lts_payout,
    );
    let coinbase_tx = Transaction::new_v6_coinbase(
        &regtest,
        height,
        outputs,
        Vec::new(),
        Some(zip233_amount),
        miner_fee,
    );
    let coinbase_tx = coinbase_tx
        .zcash_serialize_to_vec()?
        .zcash_deserialize_into::<Transaction>()?;

    let transparent_value: Amount<NegativeAllowed> = coinbase_tx
        .outputs()
        .iter()
        .map(|output| output.value())
        .sum::<Result<Amount<NonNegative>, _>>()?
        .constrain()?;
    let zip233_amount: Amount<NegativeAllowed> = coinbase_tx.zip233_amount().constrain()?;
    let total_output_value = (transparent_value + zip233_amount)?;

    let expected_block_subsidy: Amount<NegativeAllowed> =
        block_subsidy(height, &regtest)?.constrain()?;
    let miner_fee: Amount<NegativeAllowed> = miner_fee.constrain()?;
    let total_input_value = (expected_block_subsidy + miner_fee)?;

    let implied_lts_claim = (total_output_value - total_input_value)?.constrain::<NonNegative>()?;

    assert_eq!(
        coinbase_tx.zip233_amount(),
        zip233_amount,
        "generated coinbase must set the default ZIP-235 minimum"
    );
    assert_eq!(
        lts_payout, implied_lts_claim,
        "coinbase output plus ZIP-233 must imply exactly the LTS payout"
    );

    Ok(())
}
