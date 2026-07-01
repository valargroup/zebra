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
    time::Instant,
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

/// One raw block ready to feed into an upstream verifier or sequencer.
///
/// This path avoids verifier-side precomputation when the consumer will submit the
/// raw block to the real verifier anyway.
pub struct RawBlock {
    pub block: Arc<Block>,
    pub len: usize,
    pub height: u32,
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

/// Spawns a raw-block prefetch producer over `reader`.
///
/// Use this for paths that feed raw blocks into the real verifier. Unlike
/// [`spawn`], this does not build [`CheckpointVerifiedBlock`] values that would
/// be discarded before the verifier rebuilds them.
pub fn spawn_raw(
    reader: CacheReader,
    capacity: usize,
) -> (JoinHandle<()>, Receiver<Result<RawBlock>>) {
    let (tx, rx) = sync_channel(capacity);
    let mut reader = reader;
    let mut height = reader.header().start_height;

    let handle = thread::spawn(move || loop {
        // Split the single-threaded producer's per-block cost: disk read vs shielded
        // deserialize (point decompression) vs send backpressure (consumer slow).
        let read_start = Instant::now();
        let bytes = match reader.next_block() {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return,
            Err(e) => {
                let _ = tx.send(Err(eyre!("reading block {height}: {e}")));
                return;
            }
        };
        let read_us = read_start.elapsed().as_micros() as u64;
        let len = bytes.len();
        let deser_start = Instant::now();
        let block = match Block::zcash_deserialize(&bytes[..]) {
            Ok(block) => Arc::new(block),
            Err(e) => {
                let _ = tx.send(Err(eyre!("deserializing block {height}: {e}")));
                return;
            }
        };
        zebra_chain::stage_timing::record_val(height, "prefetch_read_us", read_us);
        zebra_chain::stage_timing::record_val(
            height,
            "prefetch_deser_us",
            deser_start.elapsed().as_micros() as u64,
        );

        let send_start = Instant::now();
        let sent = tx.send(Ok(RawBlock { block, len, height }));
        zebra_chain::stage_timing::record_val(
            height,
            "prefetch_send_us",
            send_start.elapsed().as_micros() as u64,
        );
        if sent.is_err() {
            // Consumer dropped the receiver: stop producing.
            return;
        }
        height += 1;
    });

    (handle, rx)
}

/// Like [`spawn_raw`], but deserializes on `workers` threads in parallel.
///
/// Block deserialization (shielded point decompression) is the single-threaded
/// producer's dominant cost, so this splits it: one reader thread streams raw bytes
/// off the cache (cheap, mostly page-cached) into an MPMC work queue, and `workers`
/// threads deserialize concurrently. Output arrives in *roughly* — not strictly —
/// height order (workers finish out of order); the Zakura sequencer reorders, so the
/// caller may feed bodies as received. `workers <= 1` falls back to [`spawn_raw`].
pub fn spawn_raw_parallel(
    reader: CacheReader,
    capacity: usize,
    workers: usize,
) -> (Vec<JoinHandle<()>>, Receiver<Result<RawBlock>>) {
    let workers = workers.max(1);
    if workers == 1 {
        let (handle, rx) = spawn_raw(reader, capacity);
        return (vec![handle], rx);
    }

    let start_height = reader.header().start_height;
    let (out_tx, out_rx) = sync_channel(capacity);
    let (work_tx, work_rx) = crossbeam_channel::bounded::<(u32, Vec<u8>)>(capacity);
    // Workers emit (height, result) out of order; the reorder thread serializes them.
    // The worker->reorder channel is intentionally unbounded so the reorder thread can
    // always accept the (out-of-order) block it is waiting for. Read-ahead is instead
    // bounded by `permit`: at most `capacity` blocks may be read but not yet emitted.
    // Without this bound, a backpressured consumer (e.g. a slow sandblast region) lets
    // the reader race ahead and deserialize the whole window into RAM (each block's
    // Orchard Halo2 proof is large), exhausting memory.
    let (result_tx, result_rx) = std::sync::mpsc::channel::<(u32, Result<RawBlock>)>();
    let (permit_tx, permit_rx) = crossbeam_channel::bounded::<()>(capacity);
    for _ in 0..capacity {
        let _ = permit_tx.send(());
    }
    let mut handles = Vec::with_capacity(workers + 2);

    // Reader thread: sequential disk read -> work queue (errors go to the reorder).
    let mut reader = reader;
    let mut height = start_height;
    let reader_result = result_tx.clone();
    handles.push(thread::spawn(move || loop {
        // Acquire a read-ahead permit (released by the reorder thread once a block is
        // emitted downstream). Closed channel => consumer is gone, so stop reading.
        if permit_rx.recv().is_err() {
            return;
        }
        let read_start = Instant::now();
        let bytes = match reader.next_block() {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return,
            Err(e) => {
                let _ = reader_result.send((height, Err(eyre!("reading block {height}: {e}"))));
                return;
            }
        };
        zebra_chain::stage_timing::record_val(
            height,
            "prefetch_read_us",
            read_start.elapsed().as_micros() as u64,
        );
        if work_tx.send((height, bytes)).is_err() {
            return;
        }
        height += 1;
    }));

    // Deserialize workers: pull bytes, deserialize (the expensive part), emit (height, result).
    for _ in 0..workers {
        let work_rx = work_rx.clone();
        let result_tx = result_tx.clone();
        handles.push(thread::spawn(move || {
            while let Ok((height, bytes)) = work_rx.recv() {
                let len = bytes.len();
                let deser_start = Instant::now();
                let item = match Block::zcash_deserialize(&bytes[..]) {
                    Ok(block) => Ok(RawBlock {
                        block: Arc::new(block),
                        len,
                        height,
                    }),
                    Err(e) => Err(eyre!("deserializing block {height}: {e}")),
                };
                let is_err = item.is_err();
                zebra_chain::stage_timing::record_val(
                    height,
                    "prefetch_deser_us",
                    deser_start.elapsed().as_micros() as u64,
                );
                if result_tx.send((height, item)).is_err() || is_err {
                    return;
                }
            }
        }));
    }
    drop(result_tx); // result_rx closes once the reader + all workers finish

    // Reorder thread: emit blocks in strict contiguous height order, so the consumer's
    // committed-tip backpressure is deadlock-safe (the next block the committer needs is
    // always produced before any block beyond it). Pending is bounded by the read-ahead
    // permit capacity, so it can't grow unbounded.
    handles.push(thread::spawn(move || {
        let mut pending: std::collections::BTreeMap<u32, Result<RawBlock>> =
            std::collections::BTreeMap::new();
        let mut next = start_height;
        while let Ok((height, item)) = result_rx.recv() {
            pending.insert(height, item);
            while let Some(item) = pending.remove(&next) {
                let stop = item.is_err();
                if out_tx.send(item).is_err() {
                    return;
                }
                // Release one read-ahead permit now that this block has left the buffer.
                let _ = permit_tx.send(());
                next += 1;
                if stop {
                    return;
                }
            }
        }
        // Producers done (normal completion drains fully above; this only runs if a gap
        // remains, e.g. after an error elsewhere — emit what's left so the consumer ends).
        while let Some((_, item)) = pending.pop_first() {
            if out_tx.send(item).is_err() {
                return;
            }
        }
    }));

    (handles, out_rx)
}
