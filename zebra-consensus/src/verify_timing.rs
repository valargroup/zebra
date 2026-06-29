//! Optional per-block checkpoint-verify timing trace (off by default).
//!
//! When `ZEBRA_VERIFY_TIMING_TRACE=<path>` is set, the checkpoint verifier appends a
//! JSON row per block (or every `ZEBRA_VERIFY_TIMING_EVERY` blocks) with the verify
//! sub-phase timings — proof-of-work (difficulty + equihash), precompute (per-tx txids
//! + auth digest), and Merkle root — keyed by height, so an offline analysis can build
//! a by-height verify breakdown and join it with the committer's per-block trace.
//!
//! The hot path is a single `OnceLock` load when the env var is unset (the production
//! default), so this stays inert unless explicitly enabled (e.g. by the replay bench).

use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

struct VerifyTimingTrace {
    file: Mutex<BufWriter<File>>,
    every: u64,
    count: AtomicU64,
    start: Instant,
}

static TRACE: OnceLock<Option<VerifyTimingTrace>> = OnceLock::new();

fn trace() -> Option<&'static VerifyTimingTrace> {
    TRACE
        .get_or_init(|| {
            let path = std::env::var_os("ZEBRA_VERIFY_TIMING_TRACE")?;
            let every = std::env::var("ZEBRA_VERIFY_TIMING_EVERY")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(1)
                .max(1);
            match File::create(&path) {
                Ok(file) => {
                    tracing::info!(?path, every, "verify-timing trace enabled");
                    Some(VerifyTimingTrace {
                        file: Mutex::new(BufWriter::new(file)),
                        every,
                        count: AtomicU64::new(0),
                        start: Instant::now(),
                    })
                }
                Err(error) => {
                    tracing::warn!(?path, %error, "could not open verify-timing trace");
                    None
                }
            }
        })
        .as_ref()
}

/// Records one verify-timing row (every `every` blocks). Cheap no-op (one `OnceLock`
/// load) when `ZEBRA_VERIFY_TIMING_TRACE` is unset.
pub(crate) fn record(height: u32, pow: Duration, precompute: Duration, merkle: Duration) {
    let Some(trace) = trace() else { return };
    let n = trace.count.fetch_add(1, Ordering::Relaxed);
    if n % trace.every != 0 {
        return;
    }
    let line = format!(
        r#"{{"ts_us":{},"height":{},"pow_ms":{:.3},"precompute_ms":{:.3},"merkle_ms":{:.3}}}"#,
        trace.start.elapsed().as_micros(),
        height,
        pow.as_secs_f64() * 1000.0,
        precompute.as_secs_f64() * 1000.0,
        merkle.as_secs_f64() * 1000.0,
    );
    if let Ok(mut file) = trace.file.lock() {
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}
