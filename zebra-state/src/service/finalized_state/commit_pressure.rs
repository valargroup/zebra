//! Optional per-commit RocksDB pressure trace (off by default).
//!
//! When `ZEBRA_COMMIT_PRESSURE_TRACE=<path>` is set, every block commit whose
//! `batch_commit` write takes at least `ZEBRA_COMMIT_PRESSURE_MS` (default 50 ms)
//! appends one JSON row to `<path>`, pairing the commit latency with the RocksDB
//! internal pressure sampled right after the write (L0 files, pending-compaction
//! bytes, running compactions/flushes, memtable + SST bytes). This lets an offline
//! analysis line up commit-latency spikes with compaction/flush pressure.
//!
//! The hot path is a single `OnceLock` load when the env var is unset (the
//! production default), so this stays inert unless explicitly enabled — e.g. by
//! the offline replay bench, which sets the env var for a run.

use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

use zebra_chain::{block::Block, serialization::ZcashSerialize};

use super::disk_db::DiskDb;

struct CommitPressureTrace {
    file: Mutex<BufWriter<File>>,
    threshold: Duration,
    /// Also emit a baseline row every `every` commits (0 = only slow commits), so the
    /// trace carries a continuous compaction-pressure curve, not just the spikes.
    every: u64,
    count: AtomicU64,
    start: Instant,
}

static TRACE: OnceLock<Option<CommitPressureTrace>> = OnceLock::new();

fn trace() -> Option<&'static CommitPressureTrace> {
    TRACE
        .get_or_init(|| {
            let path = std::env::var_os("ZEBRA_COMMIT_PRESSURE_TRACE")?;
            let threshold_ms = std::env::var("ZEBRA_COMMIT_PRESSURE_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(50);
            let every = std::env::var("ZEBRA_COMMIT_PRESSURE_EVERY")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            match File::create(&path) {
                Ok(file) => {
                    tracing::info!(?path, threshold_ms, every, "commit-pressure trace enabled");
                    Some(CommitPressureTrace {
                        file: Mutex::new(BufWriter::new(file)),
                        threshold: Duration::from_millis(threshold_ms),
                        every,
                        count: AtomicU64::new(0),
                        start: Instant::now(),
                    })
                }
                Err(error) => {
                    tracing::warn!(?path, %error, "could not open commit-pressure trace");
                    None
                }
            }
        })
        .as_ref()
}

/// Per-block timings of the committer's instrumented phases, for the trace row.
pub(crate) struct PreparedCommitTrace {
    pub(crate) height: u32,
    pub(crate) block: Arc<Block>,
    pub(crate) tx_count: usize,
    pub(crate) output_count: usize,
    pub(crate) batch_keys: usize,
    pub(crate) batch_bytes: usize,
    pub(crate) reads: Duration,
    pub(crate) address_reads: Duration,
    pub(crate) batch_assembly: Duration,
    /// VCT commitment-root verification (`verify_commitment_roots`) — CPU, runs
    /// before `write_start`, so it is *not* part of `commit_total`. Set by the
    /// caller after assembly. `ZERO` for legacy (non-VCT-fast) commits.
    pub(crate) fold: Duration,
    /// Self-time of `assemble_block_batch` (`write_start` → trace build): the
    /// read/compute half's CPU, excluding the pipeline queue-wait and disk flush.
    pub(crate) assemble_self: Duration,
    pub(crate) write_start: Instant,
}

/// Records one commit-pressure row, when the trace is enabled and the commit was
/// at least the configured threshold (or on the periodic baseline tick).
///
/// A cheap no-op (one `OnceLock` load) when `ZEBRA_COMMIT_PRESSURE_TRACE` is unset.
pub(super) fn record_commit(db: &DiskDb, prepared: PreparedCommitTrace, batch_commit: Duration) {
    let Some(trace) = trace() else { return };
    let n = trace.count.fetch_add(1, Ordering::Relaxed);
    let slow = batch_commit >= trace.threshold;
    let periodic = trace.every > 0 && n % trace.every == 0;
    if !slow && !periodic {
        return;
    }

    // Sample RocksDB pressure right after the write, and the block size (re-serialized
    // here; only on the slow-commit / periodic-sample path, so it stays off the hot path).
    let p = db.pressure_snapshot();
    let block_bytes = prepared.block.zcash_serialized_size();
    // Full per-block decomposition. `commit_total` spans `write_start` (assemble
    // start) → here (after the disk flush), so it = assemble_self + queue_wait +
    // batch_commit. `fold` is the VCT commitment-root verify, which runs *before*
    // `write_start` and is therefore additional CPU not counted in `commit_total`.
    let commit_total = prepared.write_start.elapsed();
    let assemble_self = prepared.assemble_self;
    let queue_wait = commit_total
        .saturating_sub(assemble_self)
        .saturating_sub(batch_commit);
    let line = format!(
        concat!(
            r#"{{"ts_us":{},"height":{},"slow":{},"fold_ms":{:.3},"reads_ms":{:.3},"address_reads_ms":{:.3},"#,
            r#""batch_assembly_ms":{:.3},"assemble_self_ms":{:.3},"queue_wait_ms":{:.3},"batch_commit_ms":{:.3},"commit_total_ms":{:.3},"#,
            r#""block_bytes":{},"tx_count":{},"output_count":{},"batch_keys":{},"batch_bytes":{},"#,
            r#""l0_files":{},"pending_compaction_bytes":{},"running_compactions":{},"#,
            r#""running_flushes":{},"memtable_bytes":{},"total_sst_bytes":{},"live_data_bytes":{}}}"#
        ),
        trace.start.elapsed().as_micros(),
        prepared.height,
        slow,
        prepared.fold.as_secs_f64() * 1000.0,
        prepared.reads.as_secs_f64() * 1000.0,
        prepared.address_reads.as_secs_f64() * 1000.0,
        prepared.batch_assembly.as_secs_f64() * 1000.0,
        assemble_self.as_secs_f64() * 1000.0,
        queue_wait.as_secs_f64() * 1000.0,
        batch_commit.as_secs_f64() * 1000.0,
        commit_total.as_secs_f64() * 1000.0,
        block_bytes,
        prepared.tx_count,
        prepared.output_count,
        prepared.batch_keys,
        prepared.batch_bytes,
        p.l0_files,
        p.pending_compaction_bytes,
        p.running_compactions,
        p.running_flushes,
        p.memtable_bytes,
        p.total_sst_bytes,
        p.live_data_bytes,
    );

    if let Ok(mut file) = trace.file.lock() {
        // Flush each row so a killed/partial run is still inspectable.
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}
