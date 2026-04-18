//! Per-connection session summary traces.
//!
//! Emits one JSONL record to `peer_session.jsonl` when a peer connection
//! closes. Each record aggregates counters kept by `SessionCounters` over the
//! lifetime of the connection so downstream analysis can answer "who served
//! whom how much" without re-scanning the full message log.

use std::{path::PathBuf, sync::Arc, time::Instant};

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use zebra_jsonl_trace::{JsonlTraceConfig, JsonlTracer, JsonlWriteEvent};

use crate::protocol::external::Message;

const TABLE: &str = "peer_session";
const FILE_NAME: &str = "peer_session.jsonl";
const SCHEMA: &str = "zebra.session.v1";

/// A pre-rendered JSONL record for one connection's lifetime summary.
#[derive(Serialize)]
struct SessionRecord {
    schema: &'static str,
    ts: String,
    node_id: &'static str,
    event: &'static str,
    peer: String,
    conn: u64,
    direction: &'static str,
    duration_s: f64,
    blocks_served: u64,
    blocks_served_bytes: u64,
    blocks_received: u64,
    blocks_received_bytes: u64,
    txs_served: u64,
    txs_served_bytes: u64,
    txs_received: u64,
    txs_received_bytes: u64,
    inv_sent: u64,
    inv_received: u64,
    getdata_sent: u64,
    getdata_received: u64,
    notfound_sent: u64,
    notfound_received: u64,
    close_reason: String,
}

/// A non-blocking handle for emitting session-summary records.
///
/// Clone is cheap — it's an `Arc`-wrapped channel sender.
#[derive(Clone)]
pub(crate) struct SessionTracer {
    tracer: Option<JsonlTracer>,
}

impl SessionTracer {
    /// Create a no-op tracer.
    pub fn noop() -> Self {
        Self { tracer: None }
    }

    fn new(tracer: JsonlTracer) -> Self {
        Self {
            tracer: tracer.is_enabled().then_some(tracer),
        }
    }

    fn is_enabled(&self) -> bool {
        self.tracer.is_some()
    }

    fn emit(&self, record: SessionRecord) {
        let Some(tracer) = &self.tracer else {
            return;
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

impl std::fmt::Debug for SessionTracer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionTracer").finish()
    }
}

/// Per-connection counters for a single peer session.
///
/// Incremented by the connection task as messages flow. Exactly one
/// [`SessionCounters::emit`] call is made when the connection closes.
#[derive(Debug)]
pub(crate) struct SessionCounters {
    tracer: SessionTracer,
    peer: Arc<str>,
    conn: u64,
    direction: &'static str,
    started_at: Instant,
    emitted: bool,

    blocks_served: u64,
    blocks_served_bytes: u64,
    blocks_received: u64,
    blocks_received_bytes: u64,
    txs_served: u64,
    txs_served_bytes: u64,
    txs_received: u64,
    txs_received_bytes: u64,
    inv_sent: u64,
    inv_received: u64,
    getdata_sent: u64,
    getdata_received: u64,
    notfound_sent: u64,
    notfound_received: u64,
}

impl SessionCounters {
    pub fn new(tracer: SessionTracer, peer: Arc<str>, conn: u64, direction: &'static str) -> Self {
        Self {
            tracer,
            peer,
            conn,
            direction,
            started_at: Instant::now(),
            emitted: false,

            blocks_served: 0,
            blocks_served_bytes: 0,
            blocks_received: 0,
            blocks_received_bytes: 0,
            txs_served: 0,
            txs_served_bytes: 0,
            txs_received: 0,
            txs_received_bytes: 0,
            inv_sent: 0,
            inv_received: 0,
            getdata_sent: 0,
            getdata_received: 0,
            notfound_sent: 0,
            notfound_received: 0,
        }
    }

    /// Returns true once `emit` has been called (so callers can avoid
    /// double-emitting when both `shutdown_async` and `drop` run).
    pub fn has_emitted(&self) -> bool {
        self.emitted
    }

    /// Record an outbound message on this connection.
    pub fn record_sent(&mut self, msg: &Message) {
        if !self.tracer.is_enabled() {
            return;
        }
        match msg {
            Message::Block(block) => {
                self.blocks_served = self.blocks_served.saturating_add(1);
                self.blocks_served_bytes = self
                    .blocks_served_bytes
                    .saturating_add(block_size(block.as_ref()));
            }
            Message::Tx(tx) => {
                self.txs_served = self.txs_served.saturating_add(1);
                self.txs_served_bytes = self.txs_served_bytes.saturating_add(tx.size as u64);
            }
            Message::Inv(_) => {
                self.inv_sent = self.inv_sent.saturating_add(1);
            }
            Message::GetData(_) => {
                self.getdata_sent = self.getdata_sent.saturating_add(1);
            }
            Message::NotFound(_) => {
                self.notfound_sent = self.notfound_sent.saturating_add(1);
            }
            _ => {}
        }
    }

    /// Record an inbound message on this connection.
    pub fn record_received(&mut self, msg: &Message) {
        if !self.tracer.is_enabled() {
            return;
        }
        match msg {
            Message::Block(block) => {
                self.blocks_received = self.blocks_received.saturating_add(1);
                self.blocks_received_bytes = self
                    .blocks_received_bytes
                    .saturating_add(block_size(block.as_ref()));
            }
            Message::Tx(tx) => {
                self.txs_received = self.txs_received.saturating_add(1);
                self.txs_received_bytes = self.txs_received_bytes.saturating_add(tx.size as u64);
            }
            Message::Inv(_) => {
                self.inv_received = self.inv_received.saturating_add(1);
            }
            Message::GetData(_) => {
                self.getdata_received = self.getdata_received.saturating_add(1);
            }
            Message::NotFound(_) => {
                self.notfound_received = self.notfound_received.saturating_add(1);
            }
            _ => {}
        }
    }

    /// Emit the session summary record. Safe to call multiple times; only the
    /// first call emits.
    pub fn emit(&mut self, close_reason: impl Into<String>) {
        if self.emitted || !self.tracer.is_enabled() {
            self.emitted = true;
            return;
        }
        self.emitted = true;

        let record = SessionRecord {
            schema: SCHEMA,
            ts: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            node_id: zebra_jsonl_trace::node_id(),
            event: "session_end",
            peer: self.peer.to_string(),
            conn: self.conn,
            direction: self.direction,
            duration_s: self.started_at.elapsed().as_secs_f64(),
            blocks_served: self.blocks_served,
            blocks_served_bytes: self.blocks_served_bytes,
            blocks_received: self.blocks_received,
            blocks_received_bytes: self.blocks_received_bytes,
            txs_served: self.txs_served,
            txs_served_bytes: self.txs_served_bytes,
            txs_received: self.txs_received,
            txs_received_bytes: self.txs_received_bytes,
            inv_sent: self.inv_sent,
            inv_received: self.inv_received,
            getdata_sent: self.getdata_sent,
            getdata_received: self.getdata_received,
            notfound_sent: self.notfound_sent,
            notfound_received: self.notfound_received,
            close_reason: close_reason.into(),
        };

        self.tracer.emit(record);
    }
}

fn block_size(block: &zebra_chain::block::Block) -> u64 {
    use zebra_chain::serialization::ZcashSerialize;
    block.zcash_serialized_size() as u64
}

/// Initialize the session tracer from the P2P trace directory env vars.
pub(crate) fn init_session_tracing() -> SessionTracer {
    match trace_dir_from_env() {
        Some(trace_dir) => {
            SessionTracer::new(JsonlTracer::spawn_with_config(trace_dir, trace_config()))
        }
        None => SessionTracer::noop(),
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
