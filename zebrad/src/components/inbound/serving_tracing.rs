//! JSONL tracing for application-level serving latency.
//!
//! Wraps the [`BlocksByHash`] and [`TransactionsById`] handlers in
//! [`super::Inbound`] so every serve-response cycle emits one record to
//! `serving_event.jsonl`. Latency here is "request received → response ready
//! to hand back to the network layer" — it does **not** include TCP send
//! time, which is covered by `send_timing.jsonl` instead.

use std::{path::PathBuf, sync::OnceLock, time::Duration};

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use zebra_jsonl_trace::{JsonlTraceConfig, JsonlTraceSendError, JsonlTracer, JsonlWriteEvent};

const TABLE: &str = "serving_event";
const FILE_NAME: &str = "serving_event.jsonl";
const SCHEMA: &str = "zebra.serving.v1";

#[derive(Serialize)]
struct ServingRecord {
    schema: &'static str,
    ts: String,
    node_id: &'static str,
    event: &'static str,
    requested: usize,
    served: usize,
    not_found: usize,
    total_bytes: usize,
    latency_ms: u128,
}

#[derive(Clone)]
struct ServingTracer {
    tracer: JsonlTracer,
}

impl ServingTracer {
    fn from_env() -> Self {
        let trace_dir = std::env::var_os("ZEBRA_P2P_TRACE_DIR")
            .or_else(|| std::env::var_os("ZEBRA_TRACE_DIR"))
            .filter(|path| !path.is_empty())
            .map(PathBuf::from);
        let tracer = trace_dir.map_or_else(JsonlTracer::noop, |dir| {
            JsonlTracer::spawn_with_config(dir, JsonlTraceConfig::default())
        });
        Self { tracer }
    }

    fn emit(&self, record: ServingRecord) {
        if !self.tracer.is_enabled() {
            return;
        }
        let Ok(line) = serde_json::to_vec(&record) else {
            return;
        };
        match self.tracer.try_send(JsonlWriteEvent {
            table: TABLE,
            file_name: FILE_NAME,
            line,
        }) {
            Ok(())
            | Err(JsonlTraceSendError::Disabled(_))
            | Err(JsonlTraceSendError::Closed(_))
            | Err(JsonlTraceSendError::Full(_)) => {}
        }
    }
}

fn tracer() -> &'static ServingTracer {
    static SERVING_TRACER: OnceLock<ServingTracer> = OnceLock::new();
    SERVING_TRACER.get_or_init(ServingTracer::from_env)
}

/// Record one completed block-serving request.
pub(crate) fn record_served_blocks(
    requested: usize,
    served: usize,
    not_found: usize,
    total_bytes: usize,
    latency: Duration,
) {
    tracer().emit(ServingRecord {
        schema: SCHEMA,
        ts: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        node_id: zebra_jsonl_trace::node_id(),
        event: "served_blocks",
        requested,
        served,
        not_found,
        total_bytes,
        latency_ms: latency.as_millis(),
    });
}

/// Record one completed tx-serving request.
pub(crate) fn record_served_txs(
    requested: usize,
    served: usize,
    not_found: usize,
    total_bytes: usize,
    latency: Duration,
) {
    tracer().emit(ServingRecord {
        schema: SCHEMA,
        ts: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        node_id: zebra_jsonl_trace::node_id(),
        event: "served_txs",
        requested,
        served,
        not_found,
        total_bytes,
        latency_ms: latency.as_millis(),
    });
}
