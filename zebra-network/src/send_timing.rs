//! Non-blocking send-path timing traces for diagnosing network slowdowns.
//!
//! Writes JSONL records to `send_timing.jsonl` on its own channel, separate
//! from P2P message tracing. Records capture timing at three points in the
//! send pipeline:
//!
//! - **encode**: serialization time in `Codec::encode()`
//! - **sink_send**: time for the Sink `send()` call in `PeerTx` (encode + TCP flush)
//! - **send_message**: total time in `Connection::send_message()`
//!
//! The difference between `send_message` and `encode` gives you TCP flush /
//! backpressure time.

use std::path::PathBuf;

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use zebra_jsonl_trace::{JsonlTraceConfig, JsonlTracer, JsonlWriteEvent};

const TABLE: &str = "send_timing";
const FILE_NAME: &str = "send_timing.jsonl";

/// A pre-rendered JSONL record for one send-path timing event.
#[derive(Serialize)]
struct SendTimingRecord {
    /// ISO8601 wall-clock timestamp.
    ts: String,
    /// Pipeline phase: "encode", "sink_send", or "send_message".
    phase: &'static str,
    /// Wire command name (e.g. "block", "tx", "inv").
    command: &'static str,
    /// Peer address label.
    peer: String,
    /// Connection ID (0 when not available, e.g. in the codec).
    conn: u64,
    /// Elapsed microseconds for this phase.
    elapsed_us: u128,
    /// Serialized body size in bytes (only set for "encode" phase).
    #[serde(skip_serializing_if = "Option::is_none")]
    body_bytes: Option<usize>,
}

/// A non-blocking handle for emitting send-timing trace records.
///
/// Clone is cheap — it's an `Arc`-wrapped channel sender.
#[derive(Clone)]
pub(crate) struct SendTimingTracer {
    tracer: Option<JsonlTracer>,
}

impl SendTimingTracer {
    /// Create a no-op tracer. All trace calls return immediately.
    pub fn noop() -> Self {
        Self { tracer: None }
    }

    fn new(tracer: JsonlTracer) -> Self {
        Self {
            tracer: tracer.is_enabled().then_some(tracer),
        }
    }

    /// Record a timing event. Never blocks; silently drops if the channel is full.
    pub fn record(
        &self,
        phase: &'static str,
        command: &'static str,
        peer: &str,
        conn: u64,
        elapsed: std::time::Duration,
        body_bytes: Option<usize>,
    ) {
        let Some(tracer) = &self.tracer else {
            return;
        };

        let record = SendTimingRecord {
            ts: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            phase,
            command,
            peer: peer.to_string(),
            conn,
            elapsed_us: elapsed.as_micros(),
            body_bytes,
        };

        let Ok(line) = serde_json::to_vec(&record) else {
            return;
        };

        let _ = tracer.try_send(JsonlWriteEvent {
            table: TABLE,
            file_name: FILE_NAME,
            line,
        });
    }
}

impl std::fmt::Debug for SendTimingTracer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendTimingTracer").finish()
    }
}

/// Initialize the send-timing tracer from the same trace directory environment
/// variables as P2P tracing. Spawns its own background writer task.
pub(crate) fn init_send_timing() -> SendTimingTracer {
    match trace_dir_from_env() {
        Some(trace_dir) => {
            SendTimingTracer::new(JsonlTracer::spawn_with_config(trace_dir, trace_config()))
        }
        None => SendTimingTracer::noop(),
    }
}

fn trace_config() -> JsonlTraceConfig {
    JsonlTraceConfig::default()
}

fn trace_dir_from_env() -> Option<PathBuf> {
    let trace_dir = std::env::var_os("ZEBRA_P2P_TRACE_DIR")
        .or_else(|| std::env::var_os("ZEBRA_TRACE_DIR"))
        .map(PathBuf::from);

    match trace_dir {
        Some(path) if !path.as_os_str().is_empty() => Some(path),
        _ => None,
    }
}
