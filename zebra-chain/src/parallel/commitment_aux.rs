//! Cross-client commitment-auxiliary payload types for the verified-commitment-trees
//! fast path (`docs/design/verified-commitment-trees.md` §5).
//!
//! These travel over the Zakura `tree_aux` stream (increment 6) and are also produced
//! and consumed locally by `zebra-state`. They live here in `zebra-chain` so both
//! `zebra-network` and `zebra-state` can use them without a dependency cycle.
//!
//! The final-frontier handoff payload (§5.2) is *not* here: it is embedded in the
//! binary, not carried on the wire, so `tree_aux` is a roots-only stream.

use std::io;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    block, orchard, sapling,
    serialization::{SerializationError, ZcashDeserialize, ZcashSerialize},
};

/// Per-block verified commitment roots — the essential fast-path payload (design §5.1).
///
/// One entry per height; each root is the note-commitment treestate root as of
/// end-of-block-`height`. `orchard_root` is the empty/default root below NU5.
///
/// This payload carries no trust: a recipient re-verifies every root against its own
/// checkpoint-committed block headers (design §6) before the fast path folds it in, so
/// a forwarding/serving node is exactly as trustworthy as an originating one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockCommitmentRoots {
    /// The block height these roots are for.
    pub height: block::Height,
    /// The Sapling note-commitment tree root as of the end of this block.
    pub sapling_root: sapling::tree::Root,
    /// The Orchard note-commitment tree root as of the end of this block (empty below NU5).
    pub orchard_root: orchard::tree::Root,
}

impl ZcashSerialize for BlockCommitmentRoots {
    fn zcash_serialize<W: io::Write>(&self, mut writer: W) -> Result<(), io::Error> {
        writer.write_u32::<LittleEndian>(self.height.0)?;
        self.sapling_root.zcash_serialize(&mut writer)?;
        self.orchard_root.zcash_serialize(&mut writer)?;
        Ok(())
    }
}

impl ZcashDeserialize for BlockCommitmentRoots {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        // The height is an unvalidated `u32` here; an out-of-range or wrong height simply
        // fails to match any local header during verification (design §6), so it is
        // harmless. The Sapling/Orchard root parsers reject malformed root bytes.
        let height = block::Height(reader.read_u32::<LittleEndian>()?);
        let sapling_root = sapling::tree::Root::zcash_deserialize(&mut reader)?;
        let orchard_root = orchard::tree::Root::zcash_deserialize(&mut reader)?;
        Ok(BlockCommitmentRoots {
            height,
            sapling_root,
            orchard_root,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialization::ZcashDeserializeInto;

    #[test]
    fn block_commitment_roots_round_trip() {
        let roots = BlockCommitmentRoots {
            height: block::Height(1_687_200),
            sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
            orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
        };

        let bytes = roots
            .zcash_serialize_to_vec()
            .expect("serialization to a vec does not fail");
        let parsed: BlockCommitmentRoots = bytes
            .zcash_deserialize_into()
            .expect("round-trips back to the original");

        assert_eq!(
            parsed, roots,
            "BlockCommitmentRoots round-trips on the wire"
        );
    }
}
