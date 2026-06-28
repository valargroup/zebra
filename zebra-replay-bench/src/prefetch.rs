//! Bounded look-ahead block prefetch.
//!
//! A producer thread streams the block cache, deserializes each block, and builds
//! its [`CheckpointVerifiedBlock`] (which runs the verifier-side `prepare_block_data`:
//! per-tx txid + ZIP-244 auth digest, the auth-data root, and the new-outputs map),
//! pushing the results into a bounded channel.
//!
//! This keeps block read + parse + prep **off** the timed commit thread — so both
//! `apply` and `apply_worker` measure the committer only — while bounding memory to
//! the channel capacity. The producer blocks when the channel is full, so the bench
//! scales to arbitrarily large windows without holding the whole window in RAM. It
//! mirrors production, where the verifier prepares blocks upstream of the committer.

use std::{
    sync::{
        mpsc::{sync_channel, Receiver},
        Arc,
    },
    thread::{self, JoinHandle},
};

use color_eyre::eyre::{eyre, Result};
use zebra_chain::{
    block::{merkle::AuthDataRoot, Block},
    serialization::ZcashDeserialize,
};
use zebra_state::CheckpointVerifiedBlock;

use crate::cache::CacheReader;

/// Default number of prepared blocks the producer may run ahead of the committer.
/// Big enough to keep the commit thread fed across jitter, small enough that
/// memory stays flat regardless of window size.
pub const DEFAULT_PREFETCH_CAPACITY: usize = 64;

/// Prefetch depth, overridable with `ZRB_PREFETCH_CAP` for tuning the
/// memory-vs-contention tradeoff: a deeper buffer lets the producer race ahead and
/// front-load its (multi-core) prep, leaving the committer more cores later, at the
/// cost of holding more prepared blocks in memory.
pub fn capacity() -> usize {
    std::env::var("ZRB_PREFETCH_CAP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_PREFETCH_CAPACITY)
}

/// One prepared block ready to commit: the verified block plus the per-block prep
/// the committer's `next_checkpoint` needs (the block handle and its auth-data
/// root), with its height and serialized byte length.
pub struct Prepared {
    pub cv: CheckpointVerifiedBlock,
    pub block: Arc<Block>,
    pub auth: AuthDataRoot,
    pub len: usize,
    pub height: u32,
}

/// Spawns the prefetch producer over `reader`, returning its join handle and a
/// bounded receiver of prepared blocks in height order. The producer blocks once
/// `capacity` items are buffered, so memory stays bounded. Errors are delivered
/// in-band as `Err` items; the producer stops after the first error or when the
/// consumer drops the receiver.
pub fn spawn(reader: CacheReader, capacity: usize) -> (JoinHandle<()>, Receiver<Result<Prepared>>) {
    let (tx, rx) = sync_channel(capacity);
    let mut reader = reader;
    let mut height = reader.header().start_height;

    let handle = thread::spawn(move || loop {
        let bytes = match reader.next_block() {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return,
            Err(e) => {
                let _ = tx.send(Err(eyre!("reading block {height}: {e}")));
                return;
            }
        };
        let len = bytes.len();
        let block = match Block::zcash_deserialize(&bytes[..]) {
            Ok(block) => Arc::new(block),
            Err(e) => {
                let _ = tx.send(Err(eyre!("deserializing block {height}: {e}")));
                return;
            }
        };
        let auth = block.auth_data_root();
        let cv = CheckpointVerifiedBlock::from(block.clone());

        if tx
            .send(Ok(Prepared {
                cv,
                block,
                auth,
                len,
                height,
            }))
            .is_err()
        {
            // Consumer dropped the receiver: stop producing.
            return;
        }
        height += 1;
    });

    (handle, rx)
}
