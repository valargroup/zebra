//! Note and value commitments.

use std::io;

use hex::{FromHex, FromHexError, ToHex};

use crate::serialization::{SerializationError, ZcashDeserialize, ZcashSerialize};

#[cfg(test)]
mod test_vectors;

/// The randomness used in the Pedersen Hash for note commitment.
///
/// Equivalent to `sapling_crypto::note::CommitmentRandomness`,
/// but we can't use it directly as it is not public.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CommitmentRandomness(jubjub::Fr);

/// A raw Sapling value commitment encoding.
///
/// This type stores the 32 bytes from the transaction without proving they are a
/// canonical, non-small-order Jubjub point. Since the note-commitment tree uses
/// `cm_u` rather than `cv`, Zebra keeps the raw bytes and skips decompression
/// during deserialization.
///
/// # Consensus
///
/// Deserialization only checks the byte length; the semantic verifier and
/// mempool must check that it is a canonical, non-small-order point. They do so
/// by converting it to [`ValueCommitment`] (via
/// [`Transaction::sapling_point_encodings_are_valid`]) and also convert the
/// transaction through librustzcash, whose `read_value_commitment` rejects a
/// small-order `cv`.
///
/// [`Transaction::sapling_point_encodings_are_valid`]: crate::transaction::Transaction::sapling_point_encodings_are_valid
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ValueCommitmentBytes(pub(crate) [u8; 32]);

impl ValueCommitmentBytes {
    /// Return the stored 32-byte (little-endian) compressed encoding.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Returns true if this raw encoding can be converted into a consensus-valid
    /// Sapling value commitment.
    pub fn is_valid_not_small_order(&self) -> bool {
        ValueCommitment::try_from(*self).is_ok()
    }

    /// Return the hash bytes in big-endian byte-order suitable for printing out byte by byte.
    ///
    /// Zebra displays commitment value in big-endian byte-order,
    /// following the convention set by zcashd.
    pub fn bytes_in_display_order(&self) -> [u8; 32] {
        let mut reversed_bytes = self.0;
        reversed_bytes.reverse();
        reversed_bytes
    }
}

impl ToHex for &ValueCommitmentBytes {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        self.bytes_in_display_order().encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        self.bytes_in_display_order().encode_hex_upper()
    }
}

impl FromHex for ValueCommitmentBytes {
    type Error = FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        // Parse hex string to 32 bytes
        let mut bytes = <[u8; 32]>::from_hex(hex)?;
        // Convert from big-endian (display) to little-endian (internal)
        bytes.reverse();

        Ok(Self(bytes))
    }
}

#[cfg(any(test, feature = "proptest-impl"))]
impl From<jubjub::ExtendedPoint> for ValueCommitmentBytes {
    /// Convert a Jubjub point into raw value commitment bytes.
    fn from(extended_point: jubjub::ExtendedPoint) -> Self {
        ValueCommitmentBytes(jubjub::AffinePoint::from(extended_point).to_bytes())
    }
}

impl ZcashDeserialize for sapling_crypto::value::ValueCommitment {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf)?;

        let value_commitment: Option<sapling_crypto::value::ValueCommitment> =
            sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&buf).into_option();

        value_commitment.ok_or(SerializationError::Parse("invalid ValueCommitment bytes"))
    }
}

impl ZcashDeserialize for ValueCommitmentBytes {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        // Store the raw bytes without decompressing the Jubjub point; the point
        // and its not-small-order check are recovered lazily in
        // `ValueCommitment::try_from`.
        let mut bytes = [0u8; 32];
        reader.read_exact(&mut bytes)?;
        Ok(Self(bytes))
    }
}

impl ZcashSerialize for ValueCommitmentBytes {
    fn zcash_serialize<W: io::Write>(&self, mut writer: W) -> Result<(), io::Error> {
        writer.write_all(&self.0)?;
        Ok(())
    }
}

/// A validated Sapling value commitment.
///
/// Values of this type are canonical, non-small-order Jubjub points, so they
/// satisfy the Sapling spend/output consensus rule.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ValueCommitment(pub(crate) [u8; 32]);

impl ValueCommitment {
    /// Return the stored 32-byte (little-endian) compressed encoding.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Decompresses and returns the underlying `sapling_crypto` value
    /// commitment.
    pub fn commitment(&self) -> sapling_crypto::value::ValueCommitment {
        sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&self.0)
            .into_option()
            .expect("value commitment was validated when ValueCommitment was constructed")
    }

    /// Return the hash bytes in big-endian byte-order suitable for printing out byte by byte.
    ///
    /// Zebra displays commitment value in big-endian byte-order,
    /// following the convention set by zcashd.
    pub fn bytes_in_display_order(&self) -> [u8; 32] {
        let mut reversed_bytes = self.0;
        reversed_bytes.reverse();
        reversed_bytes
    }
}

impl TryFrom<ValueCommitmentBytes> for ValueCommitment {
    type Error = &'static str;

    /// Validate raw value commitment bytes as a canonical, non-small-order
    /// Jubjub point.
    ///
    /// To stay in consensus with the rest of the network, this must accept
    /// exactly the `cv` encodings librustzcash accepts, so it calls the same
    /// `from_bytes_not_small_order` that `read_value_commitment` uses. Do not
    /// swap in a different decoder. Equivalence is pinned by
    /// `sapling_point_checks_match_librustzcash_predicates`.
    fn try_from(raw: ValueCommitmentBytes) -> Result<Self, Self::Error> {
        if bool::from(
            sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&raw.0).is_some(),
        ) {
            Ok(Self(raw.0))
        } else {
            Err("value commitment is not a canonical, non-small-order Jubjub point")
        }
    }
}

impl TryFrom<[u8; 32]> for ValueCommitment {
    type Error = &'static str;

    fn try_from(bytes: [u8; 32]) -> Result<Self, Self::Error> {
        Self::try_from(ValueCommitmentBytes(bytes))
    }
}

impl ToHex for &ValueCommitment {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        self.bytes_in_display_order().encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        self.bytes_in_display_order().encode_hex_upper()
    }
}

impl FromHex for ValueCommitment {
    type Error = FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        ValueCommitmentBytes::from_hex(hex)?
            .try_into()
            .map_err(|_| FromHexError::InvalidStringLength)
    }
}

#[cfg(any(test, feature = "proptest-impl"))]
impl From<jubjub::ExtendedPoint> for ValueCommitment {
    /// Convert a Jubjub point into a ValueCommitment.
    ///
    /// # Panics
    ///
    /// Panics if the given point does not correspond to a valid ValueCommitment.
    fn from(extended_point: jubjub::ExtendedPoint) -> Self {
        ValueCommitmentBytes::from(extended_point)
            .try_into()
            .expect("extended point must be a valid value commitment")
    }
}

impl ZcashDeserialize for ValueCommitment {
    fn zcash_deserialize<R: io::Read>(reader: R) -> Result<Self, SerializationError> {
        ValueCommitmentBytes::zcash_deserialize(reader).and_then(|raw| {
            raw.try_into()
                .map_err(|_| SerializationError::Parse("invalid ValueCommitment bytes"))
        })
    }
}

impl ZcashSerialize for ValueCommitment {
    fn zcash_serialize<W: io::Write>(&self, mut writer: W) -> Result<(), io::Error> {
        writer.write_all(&self.0)?;
        Ok(())
    }
}

impl ZcashDeserialize for sapling_crypto::note::ExtractedNoteCommitment {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf)?;

        let extracted_note_commitment: Option<sapling_crypto::note::ExtractedNoteCommitment> =
            sapling_crypto::note::ExtractedNoteCommitment::from_bytes(&buf).into_option();

        extracted_note_commitment.ok_or(SerializationError::Parse(
            "invalid ExtractedNoteCommitment bytes",
        ))
    }
}
