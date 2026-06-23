//! Property tests for the Orchard consensus flag byte codec.
//!
//! Zebra delegates the flag (de)serialization to the `orchard` crate's
//! [`orchard::Flags::to_byte`] / [`orchard::Flags::from_byte`] consensus codec. These tests
//! exercise that public path through the bundle formats Zebra uses on the wire: V5 transactions
//! use [`orchard::BundleFormat::PreNu6_3`] and V6 transactions use [`orchard::BundleFormat::Nu6_3`].

use proptest::prelude::*;

use crate::orchard::{self, BundleFormat};

/// The bits that are reserved (must be zero) under each bundle format.
fn reserved_bits(format: BundleFormat) -> u8 {
    match format {
        // Pre-NU6.3 reserves bits 2..=7; only the spend and output bits are meaningful.
        BundleFormat::PreNu6_3 => !0b0000_0011,
        // NU6.3 reinterprets bit 2 as `enableCrossAddress`; only bits 3..=7 are reserved.
        BundleFormat::Nu6_3 => !0b0000_0111,
        // `BundleFormat` is `#[non_exhaustive]`; only the two variants Zebra serializes exist today.
        _ => unreachable!("zebra only serializes the PreNu6_3 and Nu6_3 bundle formats"),
    }
}

proptest! {
    /// Every byte either round-trips through the canonical flag codec, or is rejected because it
    /// sets a reserved bit. A canonically-decoded byte must re-encode to itself.
    #[test]
    fn flag_byte_roundtrip(byte in any::<u8>(), format in prop_oneof![
        Just(BundleFormat::PreNu6_3),
        Just(BundleFormat::Nu6_3),
    ]) {
        let reserved = reserved_bits(format);

        match orchard::Flags::from_byte(byte, format) {
            Some(flags) => {
                // A byte is only accepted when no reserved bit is set.
                prop_assert_eq!(byte & reserved, 0);
                // Decoding then re-encoding must reproduce the original byte exactly.
                prop_assert_eq!(flags.to_byte(format), Some(byte));
            }
            None => {
                // Rejection happens exactly when a reserved bit is set.
                prop_assert_ne!(byte & reserved, 0);
            }
        }
    }
}

/// A flag set with cross-address transfers disabled cannot be encoded in the pre-NU6.3 format,
/// where bit 2 is reserved and cross-address transfers are implicitly enabled.
#[test]
fn cross_address_disabled_rejected_pre_nu6_3() {
    assert_eq!(
        orchard::Flags::CROSS_ADDRESS_DISABLED.to_byte(BundleFormat::PreNu6_3),
        None,
        "cross-address-disabled flags are not encodable under the pre-NU6.3 format"
    );

    // The same flag set is encodable under NU6.3, where bit 2 is `enableCrossAddress`.
    assert_eq!(
        orchard::Flags::CROSS_ADDRESS_DISABLED.to_byte(BundleFormat::Nu6_3),
        Some(0b0000_0011),
        "cross-address-disabled flags encode to spends+outputs with bit 2 clear under NU6.3"
    );
}
