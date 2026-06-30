//! Optional per-height pipeline-stage timing trace for offline profiling.
//!
//! When `ZEBRA_STAGE_TIMING_TRACE=<path>` is set, [`record`] appends one JSON row
//! per call, pairing a block `height` with a named pipeline `stage` and a wall-clock
//! timestamp (microseconds since the Unix epoch). Because the timestamp is wall-clock
//! and the facility is process-global, callers in different crates and threads (the
//! apply driver, the consensus verifier, the state service, the write worker) all
//! land on the same timeline, so an offline join by `height` localizes where a block
//! spends its time between stages.
//!
//! This lives in `zebra-chain` only because it is the common crate every stage
//! depends on; it has no consensus role. The hot path is a single `OnceLock` load
//! when the env var is unset (the default), so it stays inert in production.

use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

static TRACE: OnceLock<Option<Mutex<BufWriter<File>>>> = OnceLock::new();

fn writer() -> Option<&'static Mutex<BufWriter<File>>> {
    TRACE
        .get_or_init(|| {
            let path = std::env::var_os("ZEBRA_STAGE_TIMING_TRACE")?;
            match File::create(&path) {
                Ok(file) => Some(Mutex::new(BufWriter::new(file))),
                Err(_) => None,
            }
        })
        .as_ref()
}

/// Whether stage timing is active (`ZEBRA_STAGE_TIMING_TRACE` is set). Lets callers
/// skip building trace-only aggregates on the hot path when tracing is off.
pub fn enabled() -> bool {
    writer().is_some()
}

/// Records that block `height` reached pipeline `stage`, with a wall-clock
/// (epoch-microsecond) timestamp, when `ZEBRA_STAGE_TIMING_TRACE` is set.
///
/// A cheap no-op (one `OnceLock` load) when the env var is unset. The row is small
/// and carries no re-serialized block data, so the per-call cost is a timestamp read
/// plus a buffered write — negligible relative to the stages it measures.
pub fn record(height: u32, stage: &str) {
    write_row(height, stage, None);
}

/// Like [`record`], but also tags the row with a numeric `value` (e.g. an in-flight
/// queue depth at this stage), under the JSON key `"val"`.
pub fn record_val(height: u32, stage: &str, value: u64) {
    write_row(height, stage, Some(value));
}

fn write_row(height: u32, stage: &str, value: Option<u64>) {
    let Some(writer) = writer() else { return };
    let ts_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    if let Ok(mut file) = writer.lock() {
        // Flush each row so a killed/partial run is still inspectable; the write is a
        // tiny line (no block re-serialization), so this stays off the critical path.
        let _ = match value {
            Some(v) => writeln!(
                file,
                r#"{{"height":{height},"stage":"{stage}","ts_us":{ts_us},"val":{v}}}"#
            ),
            None => writeln!(
                file,
                r#"{{"height":{height},"stage":"{stage}","ts_us":{ts_us}}}"#
            ),
        };
        let _ = file.flush();
    }
}
