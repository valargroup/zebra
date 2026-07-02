//! Ironwood shielded pool types.
//!
//! Ironwood reuses the Orchard action proof system and encoded action shape, but
//! has distinct note commitment, nullifier, and value-pool state. The wrapper
//! types in this module keep Ironwood distinct from Orchard at type boundaries
//! while reusing Orchard's wire-format and proof-verification machinery.

#![warn(missing_docs)]

use std::ops::{Deref, DerefMut};

use crate::orchard;

#[cfg(any(test, feature = "proptest-impl"))]
mod arbitrary;

pub use crate::orchard::{
    tree, Action, Address, AuthorizedAction, CommitmentRandomness, Diversifier, EncryptedNote,
    Flags, Note, NoteCommitment, ValueCommitment, WrappedNoteKey,
};

/// An Ironwood nullifier.
///
/// Ironwood uses the Orchard nullifier construction, but Ironwood nullifiers
/// live in their own nullifier set. This wrapper prevents accidental Orchard and
/// Ironwood nullifier interchange.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct Nullifier(orchard::Nullifier);

impl From<orchard::Nullifier> for Nullifier {
    fn from(nullifier: orchard::Nullifier) -> Self {
        Self(nullifier)
    }
}

impl TryFrom<[u8; 32]> for Nullifier {
    type Error = <orchard::Nullifier as TryFrom<[u8; 32]>>::Error;

    fn try_from(bytes: [u8; 32]) -> Result<Self, Self::Error> {
        orchard::Nullifier::try_from(bytes).map(Self)
    }
}

impl From<Nullifier> for orchard::Nullifier {
    fn from(nullifier: Nullifier) -> Self {
        nullifier.0
    }
}

impl From<Nullifier> for [u8; 32] {
    fn from(nullifier: Nullifier) -> Self {
        nullifier.0.into()
    }
}

/// Ironwood shielded data.
///
/// Ironwood reuses the v6 Orchard-protocol bundle shape and serialization, but
/// commits into the Ironwood pool.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ShieldedData(orchard::ShieldedDataV6);

impl ShieldedData {
    /// Wraps a v6 Orchard-protocol bundle as Ironwood shielded data.
    pub fn new(shielded_data: orchard::ShieldedDataV6) -> Self {
        Self(shielded_data)
    }

    /// Returns the inner Orchard shielded bundle.
    pub fn data(&self) -> &orchard::ShieldedData {
        self.0.data()
    }

    /// Returns the inner Orchard shielded bundle mutably.
    pub fn data_mut(&mut self) -> &mut orchard::ShieldedData {
        self.0.data_mut()
    }

    /// Consumes this wrapper and returns the inner v6 Orchard-protocol bundle.
    pub fn into_inner(self) -> orchard::ShieldedDataV6 {
        self.0
    }

    /// Iterate over the Ironwood actions in this bundle.
    pub fn actions(&self) -> impl Iterator<Item = &Action> {
        self.data().actions()
    }

    /// Iterate over the Ironwood nullifiers in this bundle.
    pub fn nullifiers(&self) -> impl Iterator<Item = Nullifier> + '_ {
        self.data().nullifiers().copied().map(Nullifier::from)
    }

    /// Iterate over the Ironwood note commitments in this bundle.
    pub fn note_commitments(&self) -> impl Iterator<Item = &halo2::pasta::pallas::Base> {
        self.data().note_commitments()
    }
}

impl Deref for ShieldedData {
    type Target = orchard::ShieldedData;

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl DerefMut for ShieldedData {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data_mut()
    }
}
