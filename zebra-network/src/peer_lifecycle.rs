//! Peer lifecycle JSONL traces (connect, disconnect, handshake, errors).
//!
//! Writes `peer_lifecycle.jsonl` to the same trace directory used by the other
//! P2P JSONL tracers (`ZEBRA_P2P_TRACE_DIR` / `ZEBRA_TRACE_DIR`). A process-global
//! tracer is used so callsites across the network stack can emit records
//! without threading a handle through every struct.

#[cfg(feature = "p2p-tracing")]
use std::{path::PathBuf, sync::OnceLock};

#[cfg(feature = "p2p-tracing")]
use chrono::{SecondsFormat, Utc};
#[cfg(feature = "p2p-tracing")]
use serde::Serialize;
#[cfg(feature = "p2p-tracing")]
use zebra_jsonl_trace::{JsonlTraceConfig, JsonlTracer, JsonlWriteEvent};

#[cfg(feature = "p2p-tracing")]
const TABLE: &str = "peer_lifecycle";
#[cfg(feature = "p2p-tracing")]
const FILE_NAME: &str = "peer_lifecycle.jsonl";

#[cfg(feature = "p2p-tracing")]
static TRACER: OnceLock<Option<JsonlTracer>> = OnceLock::new();

#[cfg(feature = "p2p-tracing")]
#[derive(Serialize)]
struct Record<'a> {
    ts: String,
    node_id: &'static str,
    event: &'static str,
    direction: &'static str,
    peer: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_version: Option<u32>,
}

/// Initialize the peer-lifecycle tracer. Safe to call multiple times; only the
/// first call takes effect.
pub(crate) fn init() {
    #[cfg(feature = "p2p-tracing")]
    {
        let _ = TRACER.set(build_tracer());
    }
}

#[cfg(feature = "p2p-tracing")]
fn build_tracer() -> Option<JsonlTracer> {
    let trace_dir = std::env::var_os("ZEBRA_P2P_TRACE_DIR")
        .or_else(|| std::env::var_os("ZEBRA_TRACE_DIR"))
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())?;
    let tracer = JsonlTracer::spawn_with_config(trace_dir, JsonlTraceConfig::default());
    tracer.is_enabled().then_some(tracer)
}

#[cfg(feature = "p2p-tracing")]
fn emit(
    event: &'static str,
    direction: &'static str,
    peer: &str,
    reason: Option<String>,
    peer_version: Option<u32>,
) {
    let Some(Some(tracer)) = TRACER.get() else {
        return;
    };
    let record = Record {
        ts: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        node_id: zebra_jsonl_trace::node_id(),
        event,
        direction,
        peer,
        reason,
        peer_version,
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

pub(crate) fn dial_attempt(peer: impl std::fmt::Display) {
    #[cfg(feature = "p2p-tracing")]
    emit("dial_attempt", "outbound", &peer.to_string(), None, None);
    #[cfg(not(feature = "p2p-tracing"))]
    {
        let _ = peer;
    }
}

pub(crate) fn dial_ok(peer: impl std::fmt::Display) {
    #[cfg(feature = "p2p-tracing")]
    emit("dial_ok", "outbound", &peer.to_string(), None, None);
    #[cfg(not(feature = "p2p-tracing"))]
    {
        let _ = peer;
    }
}

pub(crate) fn dial_failed(peer: impl std::fmt::Display, error: &dyn std::fmt::Display) {
    #[cfg(feature = "p2p-tracing")]
    emit(
        "dial_failed",
        "outbound",
        &peer.to_string(),
        Some(error.to_string()),
        None,
    );
    #[cfg(not(feature = "p2p-tracing"))]
    {
        let _ = (peer, error);
    }
}

pub(crate) fn inbound_accept(peer: impl std::fmt::Display) {
    #[cfg(feature = "p2p-tracing")]
    emit("inbound_accept", "inbound", &peer.to_string(), None, None);
    #[cfg(not(feature = "p2p-tracing"))]
    {
        let _ = peer;
    }
}

pub(crate) fn inbound_failed(peer: impl std::fmt::Display, error: &dyn std::fmt::Display) {
    #[cfg(feature = "p2p-tracing")]
    emit(
        "inbound_failed",
        "inbound",
        &peer.to_string(),
        Some(error.to_string()),
        None,
    );
    #[cfg(not(feature = "p2p-tracing"))]
    {
        let _ = (peer, error);
    }
}

pub(crate) fn handshake_ok(
    peer: impl std::fmt::Display,
    direction: &'static str,
    peer_version: u32,
) {
    #[cfg(feature = "p2p-tracing")]
    emit(
        "handshake_ok",
        direction,
        &peer.to_string(),
        None,
        Some(peer_version),
    );
    #[cfg(not(feature = "p2p-tracing"))]
    {
        let _ = (peer, direction, peer_version);
    }
}

pub(crate) fn handshake_failed(
    peer: impl std::fmt::Display,
    direction: &'static str,
    error: &dyn std::fmt::Display,
) {
    #[cfg(feature = "p2p-tracing")]
    emit(
        "handshake_failed",
        direction,
        &peer.to_string(),
        Some(error.to_string()),
        None,
    );
    #[cfg(not(feature = "p2p-tracing"))]
    {
        let _ = (peer, direction, error);
    }
}

pub(crate) fn disconnect(peer: impl std::fmt::Display, reason: impl std::fmt::Display) {
    #[cfg(feature = "p2p-tracing")]
    emit(
        "disconnect",
        "unknown",
        &peer.to_string(),
        Some(reason.to_string()),
        None,
    );
    #[cfg(not(feature = "p2p-tracing"))]
    {
        let _ = (peer, reason);
    }
}
