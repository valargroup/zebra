//! Sidecar holding the per-height anchor roots (and the final successor block)
//! needed to drive the VCT fast path offline.
//!
//! The committer's `PeerSource` reads per-height roots from the base DB's
//! `zakura_header_commitment_roots_by_height` column family (where header sync
//! normally writes them). This sidecar carries those roots — derived from the
//! source snapshot's committed root index — plus the block one past the window's
//! end, which the fast path needs as the "verified successor" of the last block.
//!
//! Layout (little-endian):
//! ```text
//!   magic     [u8; 4]  = b"ZRBV"
//!   start     u32      height of the first root
//!   count     u32      number of roots
//!   roots     count × BlockCommitmentRoots (zcash-serialized, 68 bytes each)
//!   succ_len  u32      byte length of the successor block
//!   succ      [u8]     zcash-serialized block at height start+count (end+1)
//! ```

use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
};

use zebra_chain::{
    block::Block,
    parallel::commitment_aux::BlockCommitmentRoots,
    serialization::{ZcashDeserialize, ZcashSerialize},
};

const MAGIC: [u8; 4] = *b"ZRBV";

/// The anchor roots for a window plus the successor block of its last height.
pub struct RootsSidecar {
    /// Height of the first root.
    pub start: u32,
    /// Per-height roots for `start..start+roots.len()`.
    pub roots: Vec<BlockCommitmentRoots>,
    /// The block at `start+roots.len()` (window end + 1), the verified successor
    /// the fast path checks the last committed block against.
    pub successor: Block,
}

impl RootsSidecar {
    /// Writes the sidecar to `path`.
    pub fn write(
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

    /// Reads the sidecar from `path`.
    pub fn read(path: &Path) -> io::Result<Self> {
        let mut f = BufReader::new(File::open(path)?);
        let mut magic = [0u8; 4];
        f.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a zebra-replay-bench roots sidecar (bad magic)",
            ));
        }
        let mut four = [0u8; 4];
        f.read_exact(&mut four)?;
        let start = u32::from_le_bytes(four);
        f.read_exact(&mut four)?;
        let count = u32::from_le_bytes(four);

        let mut roots = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let r = BlockCommitmentRoots::zcash_deserialize(&mut f)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            roots.push(r);
        }

        f.read_exact(&mut four)?;
        let succ_len = u32::from_le_bytes(four) as usize;
        let mut succ = vec![0u8; succ_len];
        f.read_exact(&mut succ)?;
        let successor = Block::zcash_deserialize(&succ[..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Self {
            start,
            roots,
            successor,
        })
    }
}
