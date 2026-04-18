//! Periodic node-level activity snapshots.
//!
//! Emits one JSONL record to `node_heartbeat.jsonl` on every tick of a
//! background task. Each record aggregates cross-connection counters into
//! fixed-interval buckets so downstream analysis can answer "how idle is
//! this node over time" without joining millions of wire-message events.

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use tokio::time::{self, MissedTickBehavior};
use zebra_chain::chain_tip::ChainTip;
use zebra_jsonl_trace::{JsonlTraceConfig, JsonlTracer, JsonlWriteEvent};

use crate::protocol::external::Message;

const TABLE: &str = "node_heartbeat";
const FILE_NAME: &str = "node_heartbeat.jsonl";
const SCHEMA: &str = "zebra.heartbeat.v1";

/// Default heartbeat interval.
pub(crate) const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Shared node-wide state for the heartbeat trace.
///
/// `connected_peers` is a gauge (up/down as sessions begin/end). Every other
/// field is an interval counter that is swapped to zero on each tick.
#[derive(Default, Debug)]
pub(crate) struct HeartbeatState {
    connected_peers: AtomicUsize,
    blocks_served: AtomicU64,
    blocks_served_bytes: AtomicU64,
    blocks_received: AtomicU64,
    blocks_received_bytes: AtomicU64,
    txs_served: AtomicU64,
    txs_served_bytes: AtomicU64,
    txs_received: AtomicU64,
    txs_received_bytes: AtomicU64,
    inv_sent: AtomicU64,
    inv_received: AtomicU64,
    getdata_sent: AtomicU64,
    getdata_received: AtomicU64,
    notfound_sent: AtomicU64,
    notfound_received: AtomicU64,
}

/// A pre-rendered JSONL record for one heartbeat interval.
#[derive(Serialize)]
struct HeartbeatRecord {
    schema: &'static str,
    ts: String,
    node_id: &'static str,
    event: &'static str,
    interval_s: f64,
    connected_peers: usize,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    tip_height: Option<u32>,
}

/// A non-blocking handle passed into every connection so it can record
/// per-message events into the shared node-wide counters.
#[derive(Clone)]
pub(crate) struct HeartbeatTracer {
    inner: Option<HeartbeatInner>,
}

#[derive(Clone)]
struct HeartbeatInner {
    state: Arc<HeartbeatState>,
}

impl HeartbeatTracer {
    /// Create a no-op tracer. All record calls return immediately.
    pub fn noop() -> Self {
        Self { inner: None }
    }

    fn new(enabled: bool, state: Arc<HeartbeatState>) -> Self {
        Self {
            inner: enabled.then_some(HeartbeatInner { state }),
        }
    }

    fn state(&self) -> Option<&HeartbeatState> {
        self.inner.as_ref().map(|inner| inner.state.as_ref())
    }

    /// Increment the connected-peers gauge when a session starts.
    pub fn on_session_start(&self) {
        let Some(state) = self.state() else { return };
        state.connected_peers.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the connected-peers gauge when a session ends.
    pub fn on_session_end(&self) {
        let Some(state) = self.state() else { return };
        state.connected_peers.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record an outbound message on any connection.
    pub fn record_sent(&self, msg: &Message) {
        let Some(state) = self.state() else { return };
        match msg {
            Message::Block(block) => {
                state.blocks_served.fetch_add(1, Ordering::Relaxed);
                state
                    .blocks_served_bytes
                    .fetch_add(block_size(block.as_ref()), Ordering::Relaxed);
            }
            Message::Tx(tx) => {
                state.txs_served.fetch_add(1, Ordering::Relaxed);
                state
                    .txs_served_bytes
                    .fetch_add(tx.size as u64, Ordering::Relaxed);
            }
            Message::Inv(_) => {
                state.inv_sent.fetch_add(1, Ordering::Relaxed);
            }
            Message::GetData(_) => {
                state.getdata_sent.fetch_add(1, Ordering::Relaxed);
            }
            Message::NotFound(_) => {
                state.notfound_sent.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Record an inbound message on any connection.
    pub fn record_received(&self, msg: &Message) {
        let Some(state) = self.state() else { return };
        match msg {
            Message::Block(block) => {
                state.blocks_received.fetch_add(1, Ordering::Relaxed);
                state
                    .blocks_received_bytes
                    .fetch_add(block_size(block.as_ref()), Ordering::Relaxed);
            }
            Message::Tx(tx) => {
                state.txs_received.fetch_add(1, Ordering::Relaxed);
                state
                    .txs_received_bytes
                    .fetch_add(tx.size as u64, Ordering::Relaxed);
            }
            Message::Inv(_) => {
                state.inv_received.fetch_add(1, Ordering::Relaxed);
            }
            Message::GetData(_) => {
                state.getdata_received.fetch_add(1, Ordering::Relaxed);
            }
            Message::NotFound(_) => {
                state.notfound_received.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

impl std::fmt::Debug for HeartbeatTracer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeartbeatTracer").finish()
    }
}

fn block_size(block: &zebra_chain::block::Block) -> u64 {
    use zebra_chain::serialization::ZcashSerialize;
    block.zcash_serialized_size() as u64
}

/// Initialize the heartbeat tracer from the P2P trace directory env vars.
/// If tracing is enabled, also spawns the background heartbeat-emission task.
pub(crate) fn init_heartbeat<C>(latest_chain_tip: C) -> HeartbeatTracer
where
    C: ChainTip + Clone + Send + 'static,
{
    let Some(trace_dir) = trace_dir_from_env() else {
        return HeartbeatTracer::noop();
    };

    let jsonl = JsonlTracer::spawn_with_config(trace_dir, trace_config());
    if !jsonl.is_enabled() {
        return HeartbeatTracer::noop();
    }

    let state = Arc::new(HeartbeatState::default());
    let tracer = HeartbeatTracer::new(true, Arc::clone(&state));

    tokio::spawn(run_heartbeat_loop(
        jsonl,
        state,
        DEFAULT_HEARTBEAT_INTERVAL,
        latest_chain_tip,
    ));

    tracer
}

async fn run_heartbeat_loop<C>(
    tracer: JsonlTracer,
    state: Arc<HeartbeatState>,
    interval_duration: Duration,
    latest_chain_tip: C,
) where
    C: ChainTip,
{
    let mut ticker = time::interval(interval_duration);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Skip the first immediate tick so the first record covers a full interval.
    ticker.tick().await;
    let mut last_tick = time::Instant::now();

    loop {
        let now = ticker.tick().await;
        let interval_s = now.duration_since(last_tick).as_secs_f64();
        last_tick = now;

        let record = HeartbeatRecord {
            schema: SCHEMA,
            ts: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            node_id: zebra_jsonl_trace::node_id(),
            event: "heartbeat",
            interval_s,
            connected_peers: state.connected_peers.load(Ordering::Relaxed),
            blocks_served: state.blocks_served.swap(0, Ordering::Relaxed),
            blocks_served_bytes: state.blocks_served_bytes.swap(0, Ordering::Relaxed),
            blocks_received: state.blocks_received.swap(0, Ordering::Relaxed),
            blocks_received_bytes: state.blocks_received_bytes.swap(0, Ordering::Relaxed),
            txs_served: state.txs_served.swap(0, Ordering::Relaxed),
            txs_served_bytes: state.txs_served_bytes.swap(0, Ordering::Relaxed),
            txs_received: state.txs_received.swap(0, Ordering::Relaxed),
            txs_received_bytes: state.txs_received_bytes.swap(0, Ordering::Relaxed),
            inv_sent: state.inv_sent.swap(0, Ordering::Relaxed),
            inv_received: state.inv_received.swap(0, Ordering::Relaxed),
            getdata_sent: state.getdata_sent.swap(0, Ordering::Relaxed),
            getdata_received: state.getdata_received.swap(0, Ordering::Relaxed),
            notfound_sent: state.notfound_sent.swap(0, Ordering::Relaxed),
            notfound_received: state.notfound_received.swap(0, Ordering::Relaxed),
            tip_height: latest_chain_tip.best_tip_height().map(|h| h.0),
        };

        let Ok(line) = serde_json::to_vec(&record) else {
            continue;
        };

        if tracer
            .try_send(JsonlWriteEvent {
                table: TABLE,
                file_name: FILE_NAME,
                line,
            })
            .is_err()
        {
            // Queue is full or the writer has closed; drop the record.
            continue;
        }
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
