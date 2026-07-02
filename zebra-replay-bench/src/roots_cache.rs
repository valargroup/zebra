//! Sidecar holding per-height commitment roots and the final successor block for
//! later VCT fast-sync replay branches.
//!
//! The committer's `PeerSource` reads per-height roots from the base DB's
//! `zakura_header_commitment_roots_by_height` column family (where header sync
//! normally writes them). This sidecar carries roots derived from the source
//! snapshot plus the block one past the window's end, which later fast-sync
//! replay branches use as the verified successor of the last block.
//!
//! Layout (little-endian):
//! ```text
//!   magic     [u8; 4]  = b"ZRBV"
//!   start     u32      height of the first root
//!   count     u32      number of roots
//!   roots     count × BlockCommitmentRoots (zcash-serialized)
//!   succ_len  u32      byte length of the successor block
//!   succ      [u8]     zcash-serialized block at height start+count (end+1)
//! ```

use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
};

use zebra_chain::{
    block::Block, parallel::commitment_aux::BlockCommitmentRoots, serialization::ZcashSerialize,
};

const MAGIC: [u8; 4] = *b"ZRBV";

/// Writes the sidecar to `path`.
pub(crate) fn write(
    path: &Path,
    start: u32,
    roots: &[BlockCommitmentRoots],
    successor: &Block,
) -> io::Result<()> {
    let mut f = BufWriter::new(File::create(path)?);
    f.write_all(&MAGIC)?;
    f.write_all(&start.to_le_bytes())?;
    let count = u32::try_from(roots.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many roots"))?;
    f.write_all(&count.to_le_bytes())?;
    for r in roots {
        r.zcash_serialize(&mut f)?;
    }
    let succ = successor
        .zcash_serialize_to_vec()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let succ_len = u32::try_from(succ.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "successor too large"))?;
    f.write_all(&succ_len.to_le_bytes())?;
    f.write_all(&succ)?;
    f.flush()?;
    Ok(())
}
