//! Flat on-disk cache of serialized blocks shared between the `index` and
//! `apply` phases.
//!
//! Layout (little-endian):
//! ```text
//!   magic        [u8; 4]   = b"ZRB1"
//!   network      u8        0 = Mainnet, 1 = Testnet
//!   start_height u32       height of the first block
//!   count        u32       number of blocks
//!   tip_hash     [u8; 32]  hash of the last block (for post-apply verification)
//!   records      count × (len: u32, bytes: [u8; len])   raw zcash-serialized blocks
//! ```
//! Records are stored sequentially in height order, so `apply` streams them with
//! one buffered read pass and no random seeks.

use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
};

use zebra_chain::{block::Height, parameters::Network};

const MAGIC: [u8; 4] = *b"ZRB1";

fn network_byte(network: &Network) -> u8 {
    match network {
        Network::Mainnet => 0,
        _ => 1,
    }
}

/// Streaming writer for the block cache.
pub struct CacheWriter {
    file: BufWriter<File>,
    count: u32,
}

impl CacheWriter {
    /// Creates a new cache file and writes a provisional header. The header is
    /// rewritten with the final count and tip hash by [`CacheWriter::finish`].
    pub fn create(path: &Path, network: &Network, start_height: Height) -> io::Result<Self> {
        let mut file = BufWriter::new(File::create(path)?);
        file.write_all(&MAGIC)?;
        file.write_all(&[network_byte(network)])?;
        file.write_all(&start_height.0.to_le_bytes())?;
        // Provisional count + tip hash, backfilled in `finish`.
        file.write_all(&0u32.to_le_bytes())?;
        file.write_all(&[0u8; 32])?;
        Ok(Self { file, count: 0 })
    }

    /// Appends one raw-serialized block.
    pub fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        // A block over u32::MAX bytes is impossible on any real network, but
        // surface it as an error rather than panicking from the write path.
        let len = u32::try_from(bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "block exceeds u32 length"))?;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(bytes)?;
        self.count += 1;
        Ok(())
    }

    /// Backfills the header count and tip hash, then flushes.
    pub fn finish(mut self, tip_hash: [u8; 32]) -> io::Result<u32> {
        self.file.flush()?;
        let mut file = self.file.into_inner()?;
        use std::io::Seek;
        // count sits after magic(4) + network(1) + start_height(4) = offset 9.
        file.seek(io::SeekFrom::Start(9))?;
        file.write_all(&self.count.to_le_bytes())?;
        file.write_all(&tip_hash)?;
        file.flush()?;
        Ok(self.count)
    }
}

/// Header metadata read back from a cache file.
#[derive(Clone, Copy, Debug)]
pub struct CacheHeader {
    /// 0 = Mainnet, 1 = Testnet.
    pub network: u8,
    /// Height of the first block in the cache.
    pub start_height: u32,
    /// Number of blocks in the cache.
    pub count: u32,
    /// Hash of the last block, for post-apply verification.
    pub tip_hash: [u8; 32],
}

/// Streaming reader that yields raw block bytes in height order.
pub struct CacheReader {
    file: BufReader<File>,
    header: CacheHeader,
    yielded: u32,
}

impl CacheReader {
    /// Opens a cache file and parses its header.
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = BufReader::new(File::open(path)?);

        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a zebra-replay-bench cache file (bad magic)",
            ));
        }
        let mut one = [0u8; 1];
        file.read_exact(&mut one)?;
        let network = one[0];
        let mut four = [0u8; 4];
        file.read_exact(&mut four)?;
        let start_height = u32::from_le_bytes(four);
        file.read_exact(&mut four)?;
        let count = u32::from_le_bytes(four);
        let mut tip_hash = [0u8; 32];
        file.read_exact(&mut tip_hash)?;

        Ok(Self {
            file,
            header: CacheHeader {
                network,
                start_height,
                count,
                tip_hash,
            },
            yielded: 0,
        })
    }

    /// The parsed header.
    pub fn header(&self) -> CacheHeader {
        self.header
    }

    /// Reads the next block's raw bytes, or `None` at end of cache.
    pub fn next_block(&mut self) -> io::Result<Option<Vec<u8>>> {
        if self.yielded >= self.header.count {
            return Ok(None);
        }
        let mut four = [0u8; 4];
        self.file.read_exact(&mut four)?;
        let len = u32::from_le_bytes(four) as usize;
        let mut buf = vec![0u8; len];
        self.file.read_exact(&mut buf)?;
        self.yielded += 1;
        Ok(Some(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_round_trips_blocks_and_header() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.zrb");
        let blocks: Vec<Vec<u8>> = vec![vec![1, 2, 3], vec![], vec![9; 1000], vec![42, 42]];
        let tip = [7u8; 32];

        let mut w = CacheWriter::create(&path, &Network::Mainnet, Height(100)).expect("create");
        for b in &blocks {
            w.append(b).expect("append");
        }
        let count = w.finish(tip).expect("finish");
        assert_eq!(count, blocks.len() as u32);

        let mut r = CacheReader::open(&path).expect("open");
        let h = r.header();
        assert_eq!(h.network, 0);
        assert_eq!(h.start_height, 100);
        assert_eq!(h.count, blocks.len() as u32);
        assert_eq!(h.tip_hash, tip);

        let mut read_back = Vec::new();
        while let Some(b) = r.next_block().expect("next") {
            read_back.push(b);
        }
        assert_eq!(read_back, blocks);
    }
}
