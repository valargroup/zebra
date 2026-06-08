//! Contains code that interfaces with the zcash_note_encryption crate from
//! librustzcash.

use crate::{
    block::Height,
    parameters::{Network, NetworkUpgrade},
    transaction::Transaction,
};

/// Returns true if all Sapling, Orchard, or Ironwood outputs, if any, decrypt successfully with
/// an all-zeroes outgoing viewing key.
pub fn decrypts_successfully(tx: &Transaction, network: &Network, height: Height) -> bool {
    let nu = NetworkUpgrade::current(network, height);

    #[cfg(zcash_unstable = "nu7")]
    if let Transaction::V6 {
        network_upgrade,
        lock_time,
        expiry_height,
        inputs,
        outputs,
        sapling_shielded_data,
        orchard_shielded_data,
        ironwood_shielded_data,
    } = tx
    {
        // Output recovery only checks note encryption fields, so V6 bundles can be projected
        // through the V5 librustzcash types until librustzcash has native Ironwood V6 support.
        let v5_compatible_transaction = Transaction::V5 {
            network_upgrade: *network_upgrade,
            lock_time: *lock_time,
            expiry_height: *expiry_height,
            inputs: inputs.clone(),
            outputs: outputs.clone(),
            sapling_shielded_data: sapling_shielded_data.clone(),
            orchard_shielded_data: orchard_shielded_data.clone(),
        };

        if !decrypts_librustzcash_outputs(&v5_compatible_transaction, nu) {
            return false;
        }

        if ironwood_shielded_data.is_some() {
            let ironwood_as_orchard_transaction = Transaction::V5 {
                network_upgrade: *network_upgrade,
                lock_time: *lock_time,
                expiry_height: *expiry_height,
                inputs: Vec::new(),
                outputs: Vec::new(),
                sapling_shielded_data: None,
                orchard_shielded_data: ironwood_shielded_data.clone(),
            };

            return decrypts_librustzcash_outputs(&ironwood_as_orchard_transaction, nu);
        }

        return true;
    }

    decrypts_librustzcash_outputs(tx, nu)
}

fn decrypts_librustzcash_outputs(tx: &Transaction, nu: NetworkUpgrade) -> bool {
    let Ok(tx) = tx.to_librustzcash(nu) else {
        return false;
    };

    let null_sapling_ovk = sapling_crypto::keys::OutgoingViewingKey([0u8; 32]);

    // Note that, since this function is used to validate coinbase transactions, we can ignore
    // the "grace period" mentioned in ZIP-212.
    let zip_212_enforcement = if nu >= NetworkUpgrade::Canopy {
        sapling_crypto::note_encryption::Zip212Enforcement::On
    } else {
        sapling_crypto::note_encryption::Zip212Enforcement::Off
    };

    if let Some(bundle) = tx.sapling_bundle() {
        for output in bundle.shielded_outputs().iter() {
            let recovery = sapling_crypto::note_encryption::try_sapling_output_recovery(
                &null_sapling_ovk,
                output,
                zip_212_enforcement,
            );
            if recovery.is_none() {
                return false;
            }
        }
    }

    if let Some(bundle) = tx.orchard_bundle() {
        for act in bundle.actions() {
            if zcash_note_encryption::try_output_recovery_with_ovk(
                &orchard::note_encryption::OrchardDomain::for_action(act),
                &orchard::keys::OutgoingViewingKey::from([0u8; 32]),
                act,
                act.cv_net(),
                &act.encrypted_note().out_ciphertext,
            )
            .is_none()
            {
                return false;
            }
        }
    }

    true
}
