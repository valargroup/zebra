//! Tests for types and functions for the `getblocktemplate` RPC.

use zcash_keys::address::Address;
use zcash_transparent::address::TransparentAddress;

use zebra_chain::{
    amount::Amount,
    block::Height,
    parameters::{
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
    );

    // It should be possible to generate a coinbase tx from these params.
    Transaction::new_v5_coinbase(&regtest, Height(1), outputs, vec![])
        .zcash_serialize_to_vec()?
        // Deserialization contains checks for elementary consensus rules, which must pass.
        .zcash_deserialize_into::<Transaction>()?;

    Ok(())
}
