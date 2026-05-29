//! JSONL tracing for block download and verification timing.
//!
//! Emits one record per block verification attempt to
//! `block_verify_event.jsonl`, capturing download duration, verification
//! duration, source (sync vs gossip), and result.

use std::{path::PathBuf, sync::OnceLock, time::Duration};

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use zebra_chain::block;
use zebra_jsonl_trace::{JsonlTraceConfig, JsonlTraceSendError, JsonlTracer, JsonlWriteEvent};

const TABLE: &str = "block_verify_event";
const FILE_NAME: &str = "block_verify_event.jsonl";
const SCHEMA: &str = "zebra.block_verify.v1";

/// The source of the block being verified.
#[derive(Copy, Clone, Debug)]
pub(crate) enum BlockSource {
    /// Downloaded during chain sync.
    Sync,
    /// Received via gossip from a peer.
    Gossip,
}

/// The outcome of a block verification attempt.
#[derive(Copy, Clone, Debug)]
pub(crate) enum VerifyResult {
    Success,
    Failure,
}

#[derive(Serialize)]
struct BlockVerifyRecord {
    schema: &'static str,
    ts: String,
    node_id: &'static str,
    event: &'static str,
    source: &'static str,
    height: Option<u32>,
    hash: String,
    block_time: String,
    block_time_unix_s: i64,
    download_ms: u64,
    verify_ms: u64,
    total_ms: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_class: Option<String>,
}

#[derive(Clone)]
struct BlockVerifyTracer {
    tracer: JsonlTracer,
}

impl BlockVerifyTracer {
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

    fn emit(&self, record: BlockVerifyRecord) {
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

fn tracer() -> &'static BlockVerifyTracer {
    static BLOCK_VERIFY_TRACER: OnceLock<BlockVerifyTracer> = OnceLock::new();
    BLOCK_VERIFY_TRACER.get_or_init(BlockVerifyTracer::from_env)
}

fn source_str(source: BlockSource) -> &'static str {
    match source {
        BlockSource::Sync => "sync",
        BlockSource::Gossip => "gossip",
    }
}

fn result_str(result: VerifyResult) -> &'static str {
    match result {
        VerifyResult::Success => "success",
        VerifyResult::Failure => "failure",
    }
}

/// Record one block verification attempt.
pub(crate) fn record_block_verify(
    source: BlockSource,
    height: Option<block::Height>,
    hash: block::Hash,
    block_time: chrono::DateTime<Utc>,
    download_duration: Duration,
    verify_duration: Duration,
    result: VerifyResult,
    error_class: Option<String>,
) {
    let total = download_duration + verify_duration;
    tracer().emit(BlockVerifyRecord {
        schema: SCHEMA,
        ts: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        node_id: zebra_jsonl_trace::node_id(),
        event: "block_verify",
        source: source_str(source),
        height: height.map(|h| h.0),
        hash: hash.to_string(),
        block_time: block_time.to_rfc3339_opts(SecondsFormat::Secs, true),
        block_time_unix_s: block_time.timestamp(),
        download_ms: download_duration.as_millis() as u64,
        verify_ms: verify_duration.as_millis() as u64,
        total_ms: total.as_millis() as u64,
        result: result_str(result),
        error_class,
    });
}
