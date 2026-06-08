//! Transaction ID computation. Contains code for generating the Transaction ID
//! from the transaction.

#[cfg(zcash_unstable = "nu7")]
use blake2b_simd::{Hash as Blake2bHash, Params};
#[cfg(zcash_unstable = "nu7")]
use group::ff::PrimeField;

use super::{Hash, Transaction};
use crate::serialization::{sha256d, ZcashSerialize};

#[cfg(zcash_unstable = "nu7")]
use crate::{
    block, orchard::ShieldedData, parameters::TX_V6_VERSION_GROUP_ID, sapling,
    transaction::LockTime, transparent,
};

#[cfg(zcash_unstable = "nu7")]
const ZCASH_TX_PERSONALIZATION_PREFIX: &[u8; 12] = b"ZcashTxHash_";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_HEADERS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdHeadersHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_TRANSPARENT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdTranspaHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_SAPLING_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSaplingHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_PREVOUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdPrevoutHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_SEQUENCE_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSequencHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_OUTPUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOutputsHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_SAPLING_SPENDS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSSpendsHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_SAPLING_SPENDS_COMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSSpendCHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_SAPLING_SPENDS_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSSpendNHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_SAPLING_OUTPUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutputHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_SAPLING_OUTPUTS_COMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutC__Hash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_SAPLING_OUTPUTS_MEMOS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutM__Hash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_SAPLING_OUTPUTS_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutN__Hash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_ORCHARD_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrchardHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_ORCHARD_ACTIONS_COMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrcActCHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_ORCHARD_ACTIONS_MEMOS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrcActMHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_ORCHARD_ACTIONS_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrcActNHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_IRONWOOD_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdIronwd_Hash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_IRONWOOD_ACTIONS_COMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdIrnActCHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_IRONWOOD_ACTIONS_MEMOS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdIrnActMHash";
#[cfg(zcash_unstable = "nu7")]
const ZCASH_IRONWOOD_ACTIONS_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdIrnActNHash";

#[cfg(zcash_unstable = "nu7")]
struct OrchardStyleBundlePersonalization {
    txid_bundle: &'static [u8; 16],
    txid_actions_compact: &'static [u8; 16],
    txid_actions_memos: &'static [u8; 16],
    txid_actions_noncompact: &'static [u8; 16],
}

#[cfg(zcash_unstable = "nu7")]
const IRONWOOD_BUNDLE_PERSONALIZATION: OrchardStyleBundlePersonalization =
    OrchardStyleBundlePersonalization {
        txid_bundle: ZCASH_IRONWOOD_HASH_PERSONALIZATION,
        txid_actions_compact: ZCASH_IRONWOOD_ACTIONS_COMPACT_HASH_PERSONALIZATION,
        txid_actions_memos: ZCASH_IRONWOOD_ACTIONS_MEMOS_HASH_PERSONALIZATION,
        txid_actions_noncompact: ZCASH_IRONWOOD_ACTIONS_NONCOMPACT_HASH_PERSONALIZATION,
    };

#[cfg(zcash_unstable = "nu7")]
const ORCHARD_BUNDLE_PERSONALIZATION: OrchardStyleBundlePersonalization =
    OrchardStyleBundlePersonalization {
        txid_bundle: ZCASH_ORCHARD_HASH_PERSONALIZATION,
        txid_actions_compact: ZCASH_ORCHARD_ACTIONS_COMPACT_HASH_PERSONALIZATION,
        txid_actions_memos: ZCASH_ORCHARD_ACTIONS_MEMOS_HASH_PERSONALIZATION,
        txid_actions_noncompact: ZCASH_ORCHARD_ACTIONS_NONCOMPACT_HASH_PERSONALIZATION,
    };

#[cfg(zcash_unstable = "nu7")]
fn hasher(personal: &[u8; 16]) -> blake2b_simd::State {
    Params::new().hash_length(32).personal(personal).to_state()
}

/// A Transaction ID builder. It computes the transaction ID by hashing
/// different parts of the transaction, depending on the transaction version.
/// For V5 transactions, it follows [ZIP-244] and [ZIP-225].
///
/// [ZIP-244]: https://zips.z.cash/zip-0244
/// [ZIP-225]: https://zips.z.cash/zip-0225
pub(super) struct TxIdBuilder<'a> {
    trans: &'a Transaction,
}

impl<'a> TxIdBuilder<'a> {
    /// Return a new TxIdBuilder for the given transaction.
    pub fn new(trans: &'a Transaction) -> Self {
        TxIdBuilder { trans }
    }

    /// Compute the Transaction ID for the previously specified transaction.
    pub(super) fn txid(self) -> Option<Hash> {
        match self.trans {
            Transaction::V1 { .. }
            | Transaction::V2 { .. }
            | Transaction::V3 { .. }
            | Transaction::V4 { .. } => self.txid_v1_to_v4(),
            Transaction::V5 { .. } => self.txid_v5(),
            #[cfg(zcash_unstable = "nu7")]
            Transaction::V6 { .. } => self.txid_v6(),
        }
    }

    /// Compute the Transaction ID for transactions V1 to V4.
    /// In these cases it's simply the hash of the serialized transaction.
    fn txid_v1_to_v4(self) -> Option<Hash> {
        let mut hash_writer = sha256d::Writer::default();
        self.trans.zcash_serialize(&mut hash_writer).ok()?;
        Some(Hash(hash_writer.finish()))
    }

    /// Compute the Transaction ID for a V5 transaction in the given network upgrade.
    /// In this case it's the hash of a tree of hashes of specific parts of the
    /// transaction, as specified in ZIP-244 and ZIP-225.
    fn txid_v5(self) -> Option<Hash> {
        let nu = self.trans.network_upgrade()?;

        // We compute v5 txid (from ZIP-244) using librustzcash.
        Some(Hash(*self.trans.to_librustzcash(nu).ok()?.txid().as_ref()))
    }

    /// Compute the Transaction ID for a V6 transaction.
    #[cfg(zcash_unstable = "nu7")]
    fn txid_v6(self) -> Option<Hash> {
        let Transaction::V6 {
            network_upgrade,
            lock_time,
            expiry_height,
            inputs,
            outputs,
            sapling_shielded_data,
            orchard_shielded_data,
            ironwood_shielded_data,
        } = self.trans
        else {
            unreachable!("txid_v6() is only called for v6 transactions");
        };

        let branch_id = u32::from(network_upgrade.branch_id()?);
        let header_digest = hash_v6_header_txid_data(branch_id, *lock_time, *expiry_height);
        let transparent_digest = hash_transparent_txid_data(inputs, outputs);
        let sapling_digest = hash_sapling_txid_data(sapling_shielded_data.as_ref());
        let orchard_digest = hash_orchard_txid_data(orchard_shielded_data.as_ref());
        let ironwood_digest = hash_ironwood_txid_data(ironwood_shielded_data.as_ref());

        Some(Hash(
            v6_txid_hash(
                branch_id,
                header_digest,
                transparent_digest,
                sapling_digest,
                orchard_digest,
                ironwood_digest,
            )
            .as_bytes()
            .try_into()
            .expect("Blake2b hash is 32 bytes"),
        ))
    }
}

#[cfg(zcash_unstable = "nu7")]
fn hash_v6_header_txid_data(
    branch_id: u32,
    lock_time: LockTime,
    expiry_height: block::Height,
) -> Blake2bHash {
    let mut h = hasher(ZCASH_HEADERS_HASH_PERSONALIZATION);

    h.update(&(1_u32 << 31 | 6).to_le_bytes());
    h.update(&TX_V6_VERSION_GROUP_ID.to_le_bytes());
    h.update(&branch_id.to_le_bytes());
    h.update(&lock_time_to_u32(lock_time).to_le_bytes());
    h.update(&expiry_height.0.to_le_bytes());

    h.finalize()
}

#[cfg(zcash_unstable = "nu7")]
fn lock_time_to_u32(lock_time: LockTime) -> u32 {
    let mut bytes = Vec::new();
    lock_time
        .zcash_serialize(&mut bytes)
        .expect("lock_time should serialize");

    u32::from_le_bytes(bytes.try_into().expect("lock_time serializes as 4 bytes"))
}

#[cfg(zcash_unstable = "nu7")]
fn update_hash_with_serialized<T: ZcashSerialize>(h: &mut blake2b_simd::State, value: &T) {
    let mut bytes = Vec::new();
    value
        .zcash_serialize(&mut bytes)
        .expect("hash inputs must serialize");
    h.update(&bytes);
}

#[cfg(zcash_unstable = "nu7")]
fn hash_transparent_txid_data(
    inputs: &[transparent::Input],
    outputs: &[transparent::Output],
) -> Blake2bHash {
    let mut h = hasher(ZCASH_TRANSPARENT_HASH_PERSONALIZATION);

    if !(inputs.is_empty() && outputs.is_empty()) {
        h.update(hash_transparent_prevouts(inputs).as_bytes());
        h.update(hash_transparent_sequences(inputs).as_bytes());
        h.update(hash_transparent_outputs(outputs).as_bytes());
    }

    h.finalize()
}

#[cfg(zcash_unstable = "nu7")]
fn hash_transparent_prevouts(inputs: &[transparent::Input]) -> Blake2bHash {
    let mut h = hasher(ZCASH_PREVOUTS_HASH_PERSONALIZATION);

    for input in inputs {
        match input {
            transparent::Input::PrevOut { outpoint, .. } => {
                update_hash_with_serialized(&mut h, outpoint);
            }
            transparent::Input::Coinbase { .. } => {
                h.update(&[0; 32]);
                h.update(&u32::MAX.to_le_bytes());
            }
        }
    }

    h.finalize()
}

#[cfg(zcash_unstable = "nu7")]
fn hash_transparent_sequences(inputs: &[transparent::Input]) -> Blake2bHash {
    let mut h = hasher(ZCASH_SEQUENCE_HASH_PERSONALIZATION);

    for input in inputs {
        h.update(&input.sequence().to_le_bytes());
    }

    h.finalize()
}

#[cfg(zcash_unstable = "nu7")]
fn hash_transparent_outputs(outputs: &[transparent::Output]) -> Blake2bHash {
    let mut h = hasher(ZCASH_OUTPUTS_HASH_PERSONALIZATION);

    for output in outputs {
        update_hash_with_serialized(&mut h, output);
    }

    h.finalize()
}

#[cfg(zcash_unstable = "nu7")]
fn hash_sapling_txid_data(
    sapling_shielded_data: Option<&sapling::ShieldedData<sapling::SharedAnchor>>,
) -> Blake2bHash {
    let mut h = hasher(ZCASH_SAPLING_HASH_PERSONALIZATION);

    if let Some(sapling_shielded_data) = sapling_shielded_data {
        h.update(hash_sapling_spends(sapling_shielded_data).as_bytes());
        h.update(hash_sapling_outputs(sapling_shielded_data).as_bytes());
        h.update(&sapling_shielded_data.value_balance.to_bytes());
    }

    h.finalize()
}

#[cfg(zcash_unstable = "nu7")]
fn hash_sapling_spends(
    sapling_shielded_data: &sapling::ShieldedData<sapling::SharedAnchor>,
) -> Blake2bHash {
    let mut h = hasher(ZCASH_SAPLING_SPENDS_HASH_PERSONALIZATION);

    if sapling_shielded_data.spends().next().is_some() {
        let mut ch = hasher(ZCASH_SAPLING_SPENDS_COMPACT_HASH_PERSONALIZATION);
        let mut nh = hasher(ZCASH_SAPLING_SPENDS_NONCOMPACT_HASH_PERSONALIZATION);
        let shared_anchor = sapling_shielded_data
            .shared_anchor()
            .expect("Sapling shared anchor is present when there are spends");
        let shared_anchor_bytes: [u8; 32] = shared_anchor.into();

        for spend in sapling_shielded_data.spends() {
            let nullifier_bytes: [u8; 32] = spend.nullifier.into();
            let rk_bytes: [u8; 32] = spend.rk.clone().into();

            ch.update(&nullifier_bytes);

            nh.update(&spend.cv.0.to_bytes());
            nh.update(&shared_anchor_bytes);
            nh.update(&rk_bytes);
        }

        h.update(ch.finalize().as_bytes());
        h.update(nh.finalize().as_bytes());
    }

    h.finalize()
}

#[cfg(zcash_unstable = "nu7")]
fn hash_sapling_outputs(
    sapling_shielded_data: &sapling::ShieldedData<sapling::SharedAnchor>,
) -> Blake2bHash {
    let mut h = hasher(ZCASH_SAPLING_OUTPUTS_HASH_PERSONALIZATION);

    if sapling_shielded_data.outputs().next().is_some() {
        let mut ch = hasher(ZCASH_SAPLING_OUTPUTS_COMPACT_HASH_PERSONALIZATION);
        let mut mh = hasher(ZCASH_SAPLING_OUTPUTS_MEMOS_HASH_PERSONALIZATION);
        let mut nh = hasher(ZCASH_SAPLING_OUTPUTS_NONCOMPACT_HASH_PERSONALIZATION);

        for output in sapling_shielded_data.outputs() {
            let ephemeral_key_bytes: [u8; 32] = (&output.ephemeral_key).into();

            ch.update(&output.cm_u.to_bytes());
            ch.update(&ephemeral_key_bytes);
            ch.update(&output.enc_ciphertext.0[..52]);

            mh.update(&output.enc_ciphertext.0[52..564]);

            nh.update(&output.cv.0.to_bytes());
            nh.update(&output.enc_ciphertext.0[564..]);
            nh.update(&output.out_ciphertext.0);
        }

        h.update(ch.finalize().as_bytes());
        h.update(mh.finalize().as_bytes());
        h.update(nh.finalize().as_bytes());
    }

    h.finalize()
}

#[cfg(zcash_unstable = "nu7")]
fn hash_orchard_txid_data(orchard_shielded_data: Option<&ShieldedData>) -> Blake2bHash {
    hash_orchard_style_txid_data(orchard_shielded_data, &ORCHARD_BUNDLE_PERSONALIZATION)
}

#[cfg(zcash_unstable = "nu7")]
fn hash_ironwood_txid_data(ironwood_shielded_data: Option<&ShieldedData>) -> Blake2bHash {
    hash_orchard_style_txid_data(ironwood_shielded_data, &IRONWOOD_BUNDLE_PERSONALIZATION)
}

#[cfg(zcash_unstable = "nu7")]
fn hash_orchard_style_txid_data(
    shielded_data: Option<&ShieldedData>,
    personal: &OrchardStyleBundlePersonalization,
) -> Blake2bHash {
    let mut h = hasher(personal.txid_bundle);

    let Some(shielded_data) = shielded_data else {
        return h.finalize();
    };

    let mut ch = hasher(personal.txid_actions_compact);
    let mut mh = hasher(personal.txid_actions_memos);
    let mut nh = hasher(personal.txid_actions_noncompact);

    for action in shielded_data.actions() {
        let nullifier_bytes: [u8; 32] = action.nullifier.into();
        let ephemeral_key_bytes: [u8; 32] = (&action.ephemeral_key).into();
        let cv_bytes: [u8; 32] = action.cv.into();
        let rk_bytes: [u8; 32] = action.rk.into();

        ch.update(&nullifier_bytes);
        ch.update(&action.cm_x.to_repr());
        ch.update(&ephemeral_key_bytes);
        ch.update(&action.enc_ciphertext.0[..52]);

        mh.update(&action.enc_ciphertext.0[52..564]);

        nh.update(&cv_bytes);
        nh.update(&rk_bytes);
        nh.update(&action.enc_ciphertext.0[564..]);
        nh.update(&action.out_ciphertext.0);
    }

    h.update(ch.finalize().as_bytes());
    h.update(mh.finalize().as_bytes());
    h.update(nh.finalize().as_bytes());
    h.update(&[shielded_data.flags.bits()]);
    h.update(&shielded_data.value_balance.to_bytes());
    h.update(&<[u8; 32]>::from(&shielded_data.shared_anchor));

    h.finalize()
}

#[cfg(zcash_unstable = "nu7")]
fn v6_txid_hash(
    branch_id: u32,
    header_digest: Blake2bHash,
    transparent_digest: Blake2bHash,
    sapling_digest: Blake2bHash,
    orchard_digest: Blake2bHash,
    ironwood_digest: Blake2bHash,
) -> Blake2bHash {
    let mut personal = [0; 16];
    personal[..12].copy_from_slice(ZCASH_TX_PERSONALIZATION_PREFIX);
    personal[12..].copy_from_slice(&branch_id.to_le_bytes());

    let mut h = hasher(&personal);
    h.update(header_digest.as_bytes());
    h.update(transparent_digest.as_bytes());
    h.update(sapling_digest.as_bytes());
    h.update(orchard_digest.as_bytes());
    h.update(ironwood_digest.as_bytes());

    h.finalize()
}
