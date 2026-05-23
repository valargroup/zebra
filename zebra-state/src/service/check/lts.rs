//! Contextual check for ZIP-234/235 (NSM) Long-Term Support pool payouts.
//!
//! The semantic verifier (`miner_fees_are_valid`) only checks that the
//! coinbase value equation is arithmetically well-formed. This module
//! re-derives the signed implied claim from the block bytes and checks it
//! against the expected per-block payout computed from the parent block's LTS
//! pool snapshot (ZIP-234 smooth-issuance ceiling rule) and the ZIP-235
//! minimum deposit.

use std::collections::HashMap;

use zebra_chain::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block::{Block, Height},
    parameters::{
        subsidy::{block_subsidy, funding_stream_values, lts_disbursement_start, lts_payout},
        Network, NetworkUpgrade,
    },
    transparent,
    value_balance::ValueBalanceError,
};

use crate::{
    service::{finalized_state::ZebraDb, non_finalized_state::Chain},
    HashOrHeight, ValidateContextError,
};

/// Validate the LTS claim implied by the coinbase of `block` matches the
/// expected per-block payout for `height` given the chain's pool history.
///
/// `parent_chain` is the candidate non-finalized chain the new block is being
/// appended to (it may be empty if the block extends the finalized tip
/// directly). The parent-block LTS pool snapshot is resolved by looking at
/// `parent_chain` first, then falling back to `finalized_state` — so each
/// fork's claim is validated against its own pool history.
///
/// `spent_utxos` must contain the UTXOs spent by every transparent input in
/// `block` (including outputs created by earlier transactions in the same
/// block); those are used to compute the per-block miner fees needed to
/// re-derive the implied claim.
///
/// On success returns the LTS pool delta this block contributes (the signed
/// coinbase under-/over-claim, i.e. `-implied_claim`), which the caller sets
/// on the block's `chain_value_pool_change`. This avoids re-deriving the
/// value balance in [`block_lts_pool_delta`] on the non-finalized path.
/// Returns `InvalidLtsDeposit` if the miner over-claims relative to the
/// scheduled payout and required deposit.
#[allow(clippy::unwrap_in_result)]
pub(crate) fn check_claimed_lts_payout(
    network: &Network,
    parent_chain: &Chain,
    finalized_state: &ZebraDb,
    height: Height,
    block: &Block,
    spent_utxos: &HashMap<transparent::OutPoint, transparent::Utxo>,
) -> Result<Amount<NegativeAllowed>, ValidateContextError> {
    // The LTS contextual check only applies once NSM activates at NU7. Before
    // NU7, the semantic verifier already enforces strict transparent
    // conservation (NU6 onward) or the historical pre-NU6 inequality, so the
    // implied-claim derivation against pre-NSM blocks would fight that math.
    let Some(nsm_activation_height) = NetworkUpgrade::Nu7.activation_height(network) else {
        return Ok(Amount::zero());
    };
    if height < nsm_activation_height {
        return Ok(Amount::zero());
    }

    let expected = expected_lts_payout(network, parent_chain, finalized_state, height)?;
    let LtsValueBalance {
        implied_claim,
        block_miner_fees,
    } = derive_lts_value_balance(block, network, height, spent_utxos).map_err(
        |value_balance_error| ValidateContextError::CalculateBlockChainValueChange {
            value_balance_error,
            height,
            block_hash: block.hash(),
            transaction_count: block.transactions.len(),
            spent_utxo_count: spent_utxos.len(),
        },
    )?;

    let expected_claim: Amount<NegativeAllowed> =
        expected.constrain().map_err(|value_balance_error| {
            ValidateContextError::CalculateBlockChainValueChange {
                value_balance_error: ValueBalanceError::Lts(value_balance_error),
                height,
                block_hash: block.hash(),
                transaction_count: block.transactions.len(),
                spent_utxo_count: spent_utxos.len(),
            }
        })?;
    let implied_deposit = (expected_claim - implied_claim).map_err(|value_balance_error| {
        ValidateContextError::CalculateBlockChainValueChange {
            value_balance_error: ValueBalanceError::Lts(value_balance_error),
            height,
            block_hash: block.hash(),
            transaction_count: block.transactions.len(),
            spent_utxo_count: spent_utxos.len(),
        }
    })?;

    let minimum_deposit =
        zebra_chain::transaction::builder::zip235_minimum_zip233_amount(block_miner_fees);

    if implied_deposit < minimum_deposit {
        return Err(ValidateContextError::InvalidLtsDeposit {
            height,
            expected_minimum: minimum_deposit,
            actual: implied_deposit,
        });
    }

    Ok(-implied_claim)
}

/// Returns the LTS pool delta this block contributes to the chain pool.
///
/// In the no-v6 NSM model, the LTS pool delta is the negative signed implied
/// coinbase claim. A coinbase under-claim increases the pool; an over-claim
/// decreases it.
///
/// Used by the finalized commit path to set the `lts_amount` leg of the
/// per-block `chain_value_pool_change`. The non-finalized path reuses the
/// delta returned by [`check_claimed_lts_payout`] instead of calling this.
pub(crate) fn block_lts_pool_delta(
    block: &Block,
    network: &Network,
    height: Height,
    spent_utxos: &HashMap<transparent::OutPoint, transparent::Utxo>,
) -> Result<Amount<NegativeAllowed>, ValueBalanceError> {
    let Some(nsm_activation_height) = NetworkUpgrade::Nu7.activation_height(network) else {
        return Ok(Amount::zero());
    };
    if height < nsm_activation_height {
        return Ok(Amount::zero());
    }

    let implied_claim =
        derive_lts_value_balance(block, network, height, spent_utxos)?.implied_claim;

    Ok(-implied_claim)
}

/// Computes the expected LTS payout for `height` from chain history.
///
/// `parent_chain` is the candidate non-finalized chain the new block is being
/// appended to (it may be empty if the block extends the finalized tip
/// directly). The parent-block LTS pool snapshot is resolved by looking at
/// `parent_chain` first, then falling back to `finalized_state` — so each
/// fork's expected payout reflects its own pool history.
#[allow(clippy::unwrap_in_result)]
pub(crate) fn expected_lts_payout(
    network: &Network,
    parent_chain: &Chain,
    finalized_state: &ZebraDb,
    height: Height,
) -> Result<Amount<NonNegative>, ValidateContextError> {
    let Some(start) = lts_disbursement_start(network) else {
        return Ok(Amount::zero());
    };
    if height < start {
        return Ok(Amount::zero());
    }

    // The parent pool snapshot is the LTS pool *after* the block at
    // `height - 1`. height ≥ disbursement_start ≥ NU7 activation > 0, so the
    // parent height is always available.
    let parent_height = height
        .previous()
        .expect("height ≥ disbursement_start > 0, parent height exists");
    let parent_pool = resolve_lts_pool_at(parent_chain, finalized_state, parent_height)?;

    Ok(lts_payout(height, network, parent_pool))
}

struct LtsValueBalance {
    implied_claim: Amount<NegativeAllowed>,
    block_miner_fees: Amount<NonNegative>,
}

/// Re-derive the LTS value-balance terms from the coinbase value-balance equation.
/// Mirrors the formula in `miner_fees_are_valid` in `zebra-consensus`,
/// without that crate's tower-service plumbing.
///
/// `implied_claim = total_coinbase_output − (expected_subsidy + block_miner_fees)`
///
/// where `total_coinbase_output` is the coinbase's transparent + shielded +
/// deferred contribution to the chain value pool, and `block_miner_fees` is
/// the sum of per-tx miner fees over non-coinbase transactions.
#[allow(clippy::unwrap_in_result)]
fn derive_lts_value_balance(
    block: &Block,
    network: &Network,
    height: Height,
    spent_utxos: &HashMap<transparent::OutPoint, transparent::Utxo>,
) -> Result<LtsValueBalance, ValueBalanceError> {
    use zebra_chain::amount::Error as AmountError;
    use zebra_chain::parameters::subsidy::FundingStreamReceiver;

    let coinbase_tx = block
        .transactions
        .first()
        .expect("verified block has a coinbase transaction");

    // Coinbase total output (transparent − sapling − orchard + deferred).
    let transparent_value_balance: Amount<NegativeAllowed> = coinbase_tx
        .outputs()
        .iter()
        .map(|output| output.value())
        .sum::<Result<Amount<NonNegative>, AmountError>>()
        .map_err(ValueBalanceError::Transparent)?
        .constrain()
        .map_err(ValueBalanceError::Transparent)?;
    let sapling_value_balance = coinbase_tx.sapling_value_balance().sapling_amount();
    let orchard_value_balance = coinbase_tx.orchard_value_balance().orchard_amount();

    // Expected block subsidy and deferred-pool contribution for this height.
    // Both are pure functions of `(height, network)`; they only fail on
    // misconfigured networks (NU activation heights missing or out of range)
    // — none of which can apply here, since we've already passed the
    // semantic verifier. Mirror the `subsidy_is_valid` derivation.
    let expected_block_subsidy = block_subsidy(height, network)
        .expect("contextual LTS check: block_subsidy is valid for verified-height block");
    let mut funding_streams = funding_stream_values(height, network, expected_block_subsidy)
        .expect("contextual LTS check: funding stream values are valid for verified-height block");
    let deferred_pool_balance_change_nn = funding_streams
        .remove(&FundingStreamReceiver::Deferred)
        .unwrap_or_default();
    let deferred_pool_balance_change: Amount<NegativeAllowed> = deferred_pool_balance_change_nn
        .constrain()
        .map_err(ValueBalanceError::Transparent)?;

    let total_output_value =
        (transparent_value_balance - sapling_value_balance - orchard_value_balance
            + deferred_pool_balance_change)
            .map_err(ValueBalanceError::Transparent)?;

    // Block miner fees match the formula in `zebra-consensus`'s transaction
    // verifier (`Response::miner_fee()`). Coinbase contributes nothing to fees.
    let mut block_miner_fees: Amount<NonNegative> = Amount::zero();
    for tx in block.transactions.iter().skip(1) {
        let vb = tx.value_balance(spent_utxos)?;
        let rtv = vb
            .remaining_transaction_value()
            .map_err(ValueBalanceError::Transparent)?;
        block_miner_fees = (block_miner_fees + rtv).map_err(ValueBalanceError::Transparent)?;
    }

    let total_input_value: Amount<NegativeAllowed> = (expected_block_subsidy + block_miner_fees)
        .map_err(ValueBalanceError::Transparent)?
        .constrain()
        .map_err(ValueBalanceError::Transparent)?;

    let implied_claim =
        (total_output_value - total_input_value).map_err(ValueBalanceError::Transparent)?;

    Ok(LtsValueBalance {
        implied_claim,
        block_miner_fees,
    })
}

/// Resolve the LTS pool balance *after* the block at `height` by consulting
/// the non-finalized chain first, then the finalized state.
///
/// Returns an error if neither the chain nor the finalized state has block info
/// at `height`. This is a contextual invariant: every height ≤ tip has a
/// `BlockInfo` record (after the v27 disk-format upgrade), and we only call
/// this with heights that are ancestors of the candidate block.
fn resolve_lts_pool_at(
    parent_chain: &Chain,
    finalized_state: &ZebraDb,
    height: Height,
) -> Result<Amount<NonNegative>, ValidateContextError> {
    let info = parent_chain
        .block_info(HashOrHeight::Height(height))
        .or_else(|| finalized_state.block_info(HashOrHeight::Height(height)))
        .ok_or(ValidateContextError::MissingLtsBlockInfo { height })?;

    Ok(info.value_pools().lts_amount())
}
