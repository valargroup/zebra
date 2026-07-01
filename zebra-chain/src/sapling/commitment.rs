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

/// A Sapling value commitment, stored as the 32-byte compressed encoding from
/// the transaction.
///
/// The commitment is a Jubjub curve point. Recovering the point from its
/// encoding requires a field square root (point decompression), which is
/// expensive, and the note-commitment tree uses the note commitment `cm_u`, not
/// `cv`, so the point is decompressed lazily via [`ValueCommitment::commitment`]
/// rather than eagerly at deserialization. This keeps the dominant per-block CPU
/// cost of checkpoint sync (Jubjub point decompression) off the hot path.
///
/// # Consensus
///
/// `ZcashDeserialize` and `FromHex` only check the byte length and preserve the
/// encoding exactly; they do not prove that the bytes are a canonical,
/// non-small-order Jubjub point. Call [`ValueCommitment::commitment`] or
/// [`ValueCommitment::is_valid_not_small_order`] when the caller needs a
/// consensus-valid point.
///
/// The not-small-order check that this type used to perform at deserialization
/// is deferred, but still enforced for every untrusted transaction. The
/// checkpoint verifier trusts block hashes and does not need it. The semantic
/// verifier and the mempool call
/// [`crate::transaction::Transaction::sapling_point_encodings_are_valid`] and
/// convert every transaction via `to_librustzcash` (`CachedFfiTransaction::new`);
/// librustzcash also enforces the rule at *read*: `zcash_primitives`'
/// `read_value_commitment` uses `ValueCommitment::from_bytes_not_small_order`, so
/// a small-order `cv` makes the conversion fail and the transaction is rejected.
/// Validated by `sapling_small_order_cv_epk_deferred_but_caught_by_librustzcash`
/// in `transaction/tests/vectors.rs`.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ValueCommitment(pub(crate) [u8; 32]);

impl ValueCommitment {
    /// Decompresses and returns the underlying `sapling_crypto` value
    /// commitment, or `None` if the stored bytes are not a canonical,
    /// non-small-order Jubjub point.
    ///
    /// This performs the point decompression that deserialization defers, so it
    /// is fallible by design: the encoding is validated only where the point is
    /// used, and callers must handle an invalid commitment rather than assume it
    /// is valid. Consensus validation of the encoding happens on the semantic
    /// path via [`crate::transaction::Transaction::sapling_point_encodings_are_valid`]
    /// and `to_librustzcash`; the checkpoint verifier trusts block hashes and
    /// never calls this.
    pub fn commitment(&self) -> Option<sapling_crypto::value::ValueCommitment> {
        sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&self.0).into_option()
    }

    /// Return the stored 32-byte (little-endian) compressed encoding.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Returns true if the stored encoding is a canonical, non-small-order
    /// Jubjub point, i.e. a valid value commitment per the consensus rules.
    ///
    /// This performs the point decompression that deserialization defers; it is
    /// called by the semantic verifier (not the checkpoint verifier) to enforce
    /// the not-small-order rule on untrusted transactions.
    ///
    /// # Consensus equivalence
    ///
    /// This MUST accept exactly the encodings that librustzcash accepts for a
    /// `cv` on the verification path. If it diverged, Zebra and the rest of the
    /// network would disagree on transaction validity — a chain split, not a
    /// local bug. `zcash_primitives`'s `read_value_commitment` rejects a `cv`
    /// unless `sapling_crypto::value::ValueCommitment::from_bytes_not_small_order`
    /// returns a point, so this calls that exact function. Do not reimplement it
    /// in terms of a different decoder. The equivalence is pinned by
    /// `sapling_point_checks_match_librustzcash_predicates` in
    /// `transaction/tests/vectors.rs`.
    pub fn is_valid_not_small_order(&self) -> bool {
        bool::from(
            sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&self.0).is_some(),
        )
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
        // Parse hex string to 32 bytes
        let mut bytes = <[u8; 32]>::from_hex(hex)?;
        // Convert from big-endian (display) to little-endian (internal)
        bytes.reverse();

        Self::zcash_deserialize(io::Cursor::new(&bytes))
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
        ValueCommitment(jubjub::AffinePoint::from(extended_point).to_bytes())
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

impl ZcashDeserialize for ValueCommitment {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        // Store the encoding bytes without decompressing the Jubjub point. The
        // point (and its non-small-order check) is recovered lazily in
        // `ValueCommitment::commitment`, only where the point is actually needed.
        let mut bytes = [0u8; 32];
        reader.read_exact(&mut bytes)?;
        Ok(Self(bytes))
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
