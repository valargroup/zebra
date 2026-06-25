use std::io::{Cursor, ErrorKind};

use crate::orchard::{self, shielded_data::FlagFormat};

/// Make sure only valid flags deserialize.
#[test]
fn flag_roundtrip_bytes() {
    for flags in u8::MIN..=u8::MAX {
        let mut serialized = Cursor::new(vec![flags]);
        let maybe_deserialized =
            orchard::Flags::zcash_deserialize_with_format(&mut serialized, FlagFormat::Nu6_3);

        let nu6_3_allowed = (orchard::Flags::ENABLE_SPENDS
            | orchard::Flags::ENABLE_OUTPUTS
            | orchard::Flags::ENABLE_CROSS_ADDRESS)
            .bits();
        let invalid_bits_mask = !nu6_3_allowed;
        match orchard::Flags::from_bits(flags) {
            Some(valid_flags) => {
                assert_eq!(maybe_deserialized.ok(), Some(valid_flags));
                assert_eq!(flags & invalid_bits_mask, 0);
            }
            None => {
                assert_eq!(
                    maybe_deserialized.unwrap_err().to_string(),
                    "parse error: invalid reserved orchard flags"
                );
                assert_ne!(flags & invalid_bits_mask, 0);
            }
        }
    }
}

#[test]
fn nu6_3_flags_allow_cross_address_bit() {
    let bits = (orchard::Flags::ENABLE_SPENDS
        | orchard::Flags::ENABLE_OUTPUTS
        | orchard::Flags::ENABLE_CROSS_ADDRESS)
        .bits();
    let mut serialized = Cursor::new(vec![bits]);

    let flags = orchard::Flags::zcash_deserialize_with_format(&mut serialized, FlagFormat::Nu6_3)
        .expect("NU6.3 flag format allows enableCrossAddress");

    assert!(flags.contains(orchard::Flags::ENABLE_SPENDS));
    assert!(flags.contains(orchard::Flags::ENABLE_OUTPUTS));
    assert!(flags.contains(orchard::Flags::ENABLE_CROSS_ADDRESS));
}

#[test]
fn nu6_3_flags_allow_cross_address_bit_on_serialize() {
    let flags = orchard::Flags::ENABLE_SPENDS
        | orchard::Flags::ENABLE_OUTPUTS
        | orchard::Flags::ENABLE_CROSS_ADDRESS;

    let mut serialized = Vec::new();

    flags
        .zcash_serialize_with_format(&mut serialized, FlagFormat::Nu6_3)
        .expect("NU6.3 flag format allows enableCrossAddress");

    assert_eq!(serialized, vec![flags.bits()]);
}

#[test]
fn pre_nu6_3_flags_reject_cross_address_bit() {
    let mut serialized = Cursor::new(vec![orchard::Flags::ENABLE_CROSS_ADDRESS.bits()]);

    let error =
        orchard::Flags::zcash_deserialize_with_format(&mut serialized, FlagFormat::PreNu6_3)
            .expect_err("pre-NU6.3 flag format reserves enableCrossAddress");

    assert_eq!(
        error.to_string(),
        "parse error: invalid reserved orchard flags"
    );
}

#[test]
fn pre_nu6_3_flags_reject_cross_address_bit_on_serialize() {
    let flags = orchard::Flags::ENABLE_CROSS_ADDRESS;
    let mut serialized = Vec::new();

    let error = flags
        .zcash_serialize_with_format(&mut serialized, FlagFormat::PreNu6_3)
        .expect_err("pre-NU6.3 flag format reserves enableCrossAddress");

    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(serialized.is_empty());
}
