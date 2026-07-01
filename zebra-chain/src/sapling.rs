//! Sapling-related functionality.
//!
//! These data structures enforce the *structural validity* of Sapling-related
//! consensus-critical objects.
//!
//! **Consensus rule**:
//!
//! Validated Sapling point types enforce
//! [ZIP-216](https://zips.z.cash/zip-0216) canonical Jubjub point encodings.
//! Some transaction fields store raw bytes and defer point validation to the
//! semantic verifier so checkpoint sync can avoid unnecessary decompression.

mod commitment;
mod note;

#[cfg(any(test, feature = "proptest-impl"))]
mod arbitrary;
#[cfg(test)]
mod tests;

pub mod keys;
pub mod output;
pub mod shielded_data;
pub mod spend;
pub mod tree;

pub use commitment::{CommitmentRandomness, ValueCommitment, ValueCommitmentBytes};
pub use keys::Diversifier;
pub use note::{EncryptedNote, Note, Nullifier, WrappedNoteKey};
pub use output::{Output, OutputInTransactionV4, OutputPrefixInTransactionV5};
pub use shielded_data::{
    AnchorVariant, FieldNotPresent, PerSpendAnchor, SharedAnchor, ShieldedData, TransferData,
};
pub use spend::{Spend, SpendPrefixInTransactionV5};
