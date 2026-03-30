//! Non-blocking P2P message tracing for network experiments.
//!
//! Appends JSONL records to per-table files in a trace directory.
//! Uses a bounded channel with `try_reserve` - if the channel is full, trace
//! events are silently dropped. The connection task never blocks on disk I/O.

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc::{
        self,
        error::{TryRecvError, TrySendError},
    },
    time::{self, Instant},
};
use zebra_chain::{block, transaction};

use crate::protocol::external::{InventoryHash, Message, Nonce};

#[cfg(test)]
mod tests;

/// Channel capacity. The writer batches aggressively and adapts under queue
/// pressure, so we keep the queue smaller to bound memory usage.
const TRACE_CHANNEL_CAPACITY: usize = 16_384;

/// Max number of events to drain into a single write batch.
const TRACE_MAX_BATCH_EVENTS: usize = 256;

/// How long the writer waits for more events after receiving the first event in a batch.
const TRACE_BATCH_LINGER: Duration = Duration::from_millis(5);

/// Flush table buffers to the underlying file once they reach this size.
const TRACE_BUFFER_FLUSH_BYTES: usize = 256 * 1024;

/// Force a buffered file flush at least this often.
const TRACE_FILE_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Max number of hashes to include in a payload summary.
const MAX_SUMMARY_HASHES: usize = 5;

/// Remaining queue capacity thresholds for adaptive sampling.
const TRACE_SAMPLE_RATE_LOW_PRESSURE: u64 = 2;
const TRACE_SAMPLE_RATE_MEDIUM_PRESSURE: u64 = 8;
const TRACE_SAMPLE_RATE_HIGH_PRESSURE: u64 = 32;

/// Global connection ID counter.
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

const TRACE_DIR_ENV: &str = "ZEBRA_P2P_TRACE_DIR";
const LEGACY_TRACE_FILE_ENV: &str = "ZEBRA_P2P_TRACE_FILE";
const TRACE_DIR_NAME: &str = "traces";

/// Returns a unique, monotonically increasing connection ID.
pub(crate) fn next_connection_id() -> u64 {
    NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed)
}

/// Logical output tables.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum TraceTable {
    PeerMessage,
    TraceDropped,
}

impl TraceTable {
    const ALL: [Self; 2] = [Self::PeerMessage, Self::TraceDropped];

    fn file_name(self) -> &'static str {
        match self {
            Self::PeerMessage => "peer_message.jsonl",
            Self::TraceDropped => "trace_dropped.jsonl",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::PeerMessage => "peer_message",
            Self::TraceDropped => "trace_dropped",
        }
    }
}

enum TraceEvent {
    PeerMessage(PeerMessageEvent),
    TraceDropped(TraceDroppedEvent),
}

impl TraceEvent {
    fn table(&self) -> TraceTable {
        match self {
            Self::PeerMessage(_) => TraceTable::PeerMessage,
            Self::TraceDropped(_) => TraceTable::TraceDropped,
        }
    }
}

struct PeerMessageEvent {
    ts_unix_ms: i64,
    dir: &'static str,
    msg: &'static str,
    peer: Arc<str>,
    conn: u64,
    mid: TraceMessageId,
    summary: Option<CompactPayloadSummary>,
}

struct TraceDroppedEvent {
    ts_unix_ms: i64,
    table: TraceTable,
    queue_full_dropped: u64,
    sampled_dropped: u64,
}

enum TraceDropReason {
    QueueFull,
    Sampled,
}

#[derive(Clone)]
struct TraceRuntime {
    tx: mpsc::Sender<TraceEvent>,
    queue_full_drops: Arc<AtomicU64>,
    sampled_drops: Arc<AtomicU64>,
    sample_counter: Arc<AtomicU64>,
}

impl TraceRuntime {
    fn new(tx: mpsc::Sender<TraceEvent>) -> Self {
        Self {
            tx,
            queue_full_drops: Arc::new(AtomicU64::new(0)),
            sampled_drops: Arc::new(AtomicU64::new(0)),
            sample_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    fn record_drop(&self, reason: TraceDropReason) {
        match reason {
            TraceDropReason::QueueFull => {
                self.queue_full_drops.fetch_add(1, Ordering::Relaxed);
            }
            TraceDropReason::Sampled => {
                self.sampled_drops.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn adaptive_sample_rate(&self) -> u64 {
        let remaining = self.tx.capacity();

        if remaining <= TRACE_CHANNEL_CAPACITY / 32 {
            TRACE_SAMPLE_RATE_HIGH_PRESSURE
        } else if remaining <= TRACE_CHANNEL_CAPACITY / 16 {
            TRACE_SAMPLE_RATE_MEDIUM_PRESSURE
        } else if remaining <= TRACE_CHANNEL_CAPACITY / 8 {
            TRACE_SAMPLE_RATE_LOW_PRESSURE
        } else {
            1
        }
    }

    fn should_sample_drop(&self) -> bool {
        let sample_rate = self.adaptive_sample_rate();
        sample_rate > 1
            && !self
                .sample_counter
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(sample_rate)
    }

    fn try_emit_drop_record(&self, ts_unix_ms: i64) {
        let queue_full_dropped = self.queue_full_drops.swap(0, Ordering::Relaxed);
        let sampled_dropped = self.sampled_drops.swap(0, Ordering::Relaxed);

        if queue_full_dropped == 0 && sampled_dropped == 0 {
            return;
        }

        let event = TraceEvent::TraceDropped(TraceDroppedEvent {
            ts_unix_ms,
            table: TraceTable::PeerMessage,
            queue_full_dropped,
            sampled_dropped,
        });

        match self.tx.try_send(event) {
            Ok(()) | Err(TrySendError::Closed(_)) => {}
            Err(TrySendError::Full(TraceEvent::TraceDropped(event))) => {
                self.queue_full_drops
                    .fetch_add(event.queue_full_dropped, Ordering::Relaxed);
                self.sampled_drops
                    .fetch_add(event.sampled_dropped, Ordering::Relaxed);
            }
            Err(TrySendError::Full(_)) => unreachable!("trace drop events only requeue themselves"),
        }
    }
}

/// A handle for emitting trace events. Clone is cheap for active tracers.
#[derive(Clone)]
pub(crate) struct P2pTracer {
    inner: TraceState,
}

#[derive(Clone)]
enum TraceState {
    Disabled,
    Enabled(TraceRuntime),
}

impl P2pTracer {
    /// Create a new tracer backed by the given channel sender.
    fn new(tx: mpsc::Sender<TraceEvent>) -> Self {
        Self {
            inner: TraceState::Enabled(TraceRuntime::new(tx)),
        }
    }

    /// Create a no-op tracer. All trace calls return immediately.
    pub(crate) fn noop() -> Self {
        Self {
            inner: TraceState::Disabled,
        }
    }

    /// Emit a trace record. Never blocks. Drops the record if the channel is full.
    #[cfg(test)]
    fn trace(&self, event: TraceEvent) {
        let TraceState::Enabled(runtime) = &self.inner else {
            return;
        };

        let _ = runtime.tx.try_send(event);
    }

    /// Convenience: build and emit a trace record from a message.
    pub(crate) fn trace_msg(
        &self,
        direction: &'static str,
        msg: &Message,
        peer_addr: &Arc<str>,
        connection_id: u64,
        seq: &AtomicU64,
    ) {
        let TraceState::Enabled(runtime) = &self.inner else {
            return;
        };

        if runtime.should_sample_drop() {
            runtime.record_drop(TraceDropReason::Sampled);
            return;
        }

        let permit = match runtime.tx.try_reserve() {
            Ok(permit) => permit,
            Err(_) => {
                runtime.record_drop(TraceDropReason::QueueFull);
                return;
            }
        };

        let ts_unix_ms = Utc::now().timestamp_millis();
        let (msg_type, summary) = summarize_message(msg);
        let mid = message_id(msg, connection_id, seq);

        permit.send(TraceEvent::PeerMessage(PeerMessageEvent {
            ts_unix_ms,
            dir: direction,
            msg: msg_type,
            peer: Arc::clone(peer_addr),
            conn: connection_id,
            mid,
            summary,
        }));

        runtime.try_emit_drop_record(ts_unix_ms);
    }
}

impl std::fmt::Debug for P2pTracer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P2pTracer").finish()
    }
}

/// A single trace event, serialized as one JSONL line.
#[derive(Serialize)]
pub(crate) struct P2pTraceRecord {
    /// ISO8601 wall-clock timestamp.
    pub ts: String,
    /// "send" or "recv"
    pub dir: &'static str,
    /// Wire message type (e.g. "inv", "block", "tx").
    pub msg: &'static str,
    /// Peer address label.
    pub peer: String,
    /// Monotonic connection ID.
    pub conn: u64,
    /// Message identifier for correlation.
    ///
    /// Content-addressed messages use stable IDs where possible. All other
    /// messages fall back to connection-local sequencing.
    pub mid: String,
    /// Lightweight payload summary (never full payloads).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<PayloadSummary>,
}

/// Structured record describing dropped trace events under backpressure.
#[derive(Serialize)]
pub(crate) struct TraceDroppedRecord {
    /// ISO8601 wall-clock timestamp.
    pub ts: String,
    /// The table that dropped events.
    pub table: &'static str,
    /// Events dropped because the channel was full.
    #[serde(skip_serializing_if = "is_zero")]
    pub queue_full_dropped: u64,
    /// Events dropped by adaptive sampling while the queue was under pressure.
    #[serde(skip_serializing_if = "is_zero")]
    pub sampled_dropped: u64,
}

/// Lightweight summary of message payload.
#[derive(Serialize)]
pub(crate) struct PayloadSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hashes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<u64>,
}

struct CompactPayloadSummary {
    count: Option<usize>,
    hashes: Vec<TraceHash>,
    height: Option<u32>,
    nonce: Option<u64>,
}

enum TraceHash {
    Error,
    Block(block::Hash),
    Tx(transaction::Hash),
    Wtx(transaction::WtxId),
    Text(Box<str>),
}

enum TraceMessageId {
    Nonce {
        prefix: &'static str,
        nonce: u64,
    },
    Hash {
        prefix: &'static str,
        hash: TraceHash,
    },
    HashList {
        prefix: &'static str,
        first: Option<TraceHash>,
        count: usize,
    },
    Addr {
        conn: u64,
        seq: u64,
        count: usize,
    },
    ConnectionSeq {
        prefix: &'static str,
        conn: u64,
        seq: u64,
    },
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// Extract a lightweight summary from a message without cloning large data.
fn summarize_message(msg: &Message) -> (&'static str, Option<CompactPayloadSummary>) {
    match msg {
        Message::Version(v) => (
            "version",
            Some(CompactPayloadSummary {
                height: Some(v.start_height.0),
                nonce: Some(v.nonce.0),
                count: None,
                hashes: Vec::new(),
            }),
        ),
        Message::Verack => ("verack", None),
        Message::Ping(Nonce(n)) => (
            "ping",
            Some(CompactPayloadSummary {
                nonce: Some(*n),
                count: None,
                hashes: Vec::new(),
                height: None,
            }),
        ),
        Message::Pong(Nonce(n)) => (
            "pong",
            Some(CompactPayloadSummary {
                nonce: Some(*n),
                count: None,
                hashes: Vec::new(),
                height: None,
            }),
        ),
        Message::Reject { message, ccode, .. } => (
            "reject",
            Some(CompactPayloadSummary {
                count: None,
                hashes: vec![TraceHash::Text(
                    format!("{message}:{ccode:?}").into_boxed_str(),
                )],
                height: None,
                nonce: None,
            }),
        ),
        Message::GetAddr => ("getaddr", None),
        Message::Addr(addrs) => (
            "addr",
            Some(CompactPayloadSummary {
                count: Some(addrs.len()),
                hashes: Vec::new(),
                height: None,
                nonce: None,
            }),
        ),
        Message::GetBlocks {
            known_blocks,
            stop: _,
        } => (
            "getblocks",
            Some(CompactPayloadSummary {
                count: Some(known_blocks.len()),
                hashes: first_n_block_hashes(known_blocks, MAX_SUMMARY_HASHES),
                height: None,
                nonce: None,
            }),
        ),
        Message::Inv(items) => (
            "inv",
            Some(CompactPayloadSummary {
                count: Some(items.len()),
                hashes: first_n_inv_hashes(items, MAX_SUMMARY_HASHES),
                height: None,
                nonce: None,
            }),
        ),
        Message::GetHeaders { known_blocks, .. } => (
            "getheaders",
            Some(CompactPayloadSummary {
                count: Some(known_blocks.len()),
                hashes: first_n_block_hashes(known_blocks, MAX_SUMMARY_HASHES),
                height: None,
                nonce: None,
            }),
        ),
        Message::Headers(headers) => (
            "headers",
            Some(CompactPayloadSummary {
                count: Some(headers.len()),
                hashes: headers
                    .iter()
                    .take(MAX_SUMMARY_HASHES)
                    .map(|h| TraceHash::Block(h.header.hash()))
                    .collect(),
                height: None,
                nonce: None,
            }),
        ),
        Message::GetData(items) => (
            "getdata",
            Some(CompactPayloadSummary {
                count: Some(items.len()),
                hashes: first_n_inv_hashes(items, MAX_SUMMARY_HASHES),
                height: None,
                nonce: None,
            }),
        ),
        Message::Block(block) => (
            "block",
            Some(CompactPayloadSummary {
                count: None,
                hashes: vec![TraceHash::Block(block.hash())],
                height: block.coinbase_height().map(|h| h.0),
                nonce: None,
            }),
        ),
        Message::Tx(tx) => (
            "tx",
            Some(CompactPayloadSummary {
                count: None,
                hashes: vec![TraceHash::Tx(tx.id.mined_id())],
                height: None,
                nonce: None,
            }),
        ),
        Message::NotFound(items) => (
            "notfound",
            Some(CompactPayloadSummary {
                count: Some(items.len()),
                hashes: first_n_inv_hashes(items, MAX_SUMMARY_HASHES),
                height: None,
                nonce: None,
            }),
        ),
        Message::Mempool => ("mempool", None),
        Message::FilterLoad { .. } => ("filterload", None),
        Message::FilterAdd { .. } => ("filteradd", None),
        Message::FilterClear => ("filterclear", None),
    }
}

/// Generate a message identifier for correlation.
fn message_id(msg: &Message, conn: u64, seq: &AtomicU64) -> TraceMessageId {
    match msg {
        Message::Ping(Nonce(n)) => TraceMessageId::Nonce {
            prefix: "ping",
            nonce: *n,
        },
        Message::Pong(Nonce(n)) => TraceMessageId::Nonce {
            prefix: "pong",
            nonce: *n,
        },
        Message::Block(block) => TraceMessageId::Hash {
            prefix: "block",
            hash: TraceHash::Block(block.hash()),
        },
        Message::Tx(tx) => TraceMessageId::Hash {
            prefix: "tx",
            hash: TraceHash::Tx(tx.id.mined_id()),
        },
        Message::Inv(items) => inv_id("inv", items),
        Message::GetData(items) => inv_id("getdata", items),
        Message::NotFound(items) => inv_id("notfound", items),
        Message::GetBlocks { known_blocks, .. } => block_hash_list_id("getblocks", known_blocks),
        Message::GetHeaders { known_blocks, .. } => block_hash_list_id("getheaders", known_blocks),
        Message::Headers(headers) => TraceMessageId::HashList {
            prefix: "headers",
            first: headers.first().map(|h| TraceHash::Block(h.header.hash())),
            count: headers.len(),
        },
        Message::Addr(addrs) => TraceMessageId::Addr {
            conn,
            seq: seq_next(seq),
            count: addrs.len(),
        },
        // Parameterless or rarely-correlated messages use connection+sequence.
        _ => TraceMessageId::ConnectionSeq {
            prefix: msg.command(),
            conn,
            seq: seq_next(seq),
        },
    }
}

fn seq_next(seq: &AtomicU64) -> u64 {
    seq.fetch_add(1, Ordering::Relaxed)
}

fn inv_id(prefix: &'static str, items: &[InventoryHash]) -> TraceMessageId {
    TraceMessageId::HashList {
        prefix,
        first: items.first().map(trace_hash_from_inventory),
        count: items.len(),
    }
}

fn block_hash_list_id(prefix: &'static str, hashes: &[block::Hash]) -> TraceMessageId {
    TraceMessageId::HashList {
        prefix,
        first: hashes.first().map(|hash| TraceHash::Block(*hash)),
        count: hashes.len(),
    }
}

fn trace_hash_from_inventory(hash: &InventoryHash) -> TraceHash {
    match hash {
        InventoryHash::Block(hash) | InventoryHash::FilteredBlock(hash) => TraceHash::Block(*hash),
        InventoryHash::Tx(hash) => TraceHash::Tx(*hash),
        InventoryHash::Wtx(wtx_id) => TraceHash::Wtx(*wtx_id),
        InventoryHash::Error => TraceHash::Error,
    }
}

fn first_n_inv_hashes(items: &[InventoryHash], n: usize) -> Vec<TraceHash> {
    items
        .iter()
        .take(n)
        .map(trace_hash_from_inventory)
        .collect()
}

fn first_n_block_hashes(hashes: &[block::Hash], n: usize) -> Vec<TraceHash> {
    hashes
        .iter()
        .take(n)
        .map(|hash| TraceHash::Block(*hash))
        .collect()
}

fn render_message_id(message_id: TraceMessageId) -> String {
    match message_id {
        TraceMessageId::Nonce { prefix, nonce } => format!("{prefix}:{nonce}"),
        TraceMessageId::Hash { prefix, hash } => format!("{prefix}:{}", render_trace_hash(hash)),
        TraceMessageId::HashList {
            prefix,
            first,
            count,
        } => {
            let first = first.map(render_trace_hash).unwrap_or_default();
            format!("{prefix}:{first}+{count}")
        }
        TraceMessageId::Addr { conn, seq, count } => format!("addr:{conn}:{seq}:{count}"),
        TraceMessageId::ConnectionSeq { prefix, conn, seq } => {
            format!("{prefix}:{conn}:{seq}")
        }
    }
}

fn render_trace_hash(hash: TraceHash) -> String {
    match hash {
        TraceHash::Error => "error".to_string(),
        TraceHash::Block(hash) => hash.to_string(),
        TraceHash::Tx(hash) => hash.to_string(),
        TraceHash::Wtx(wtx_id) => wtx_id.id.to_string(),
        TraceHash::Text(text) => text.into_string(),
    }
}

fn render_summary(summary: CompactPayloadSummary) -> PayloadSummary {
    PayloadSummary {
        count: summary.count,
        hashes: summary.hashes.into_iter().map(render_trace_hash).collect(),
        height: summary.height,
        nonce: summary.nonce,
    }
}

fn format_trace_timestamp(ts_unix_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ts_unix_ms)
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn render_peer_message_record(event: PeerMessageEvent) -> P2pTraceRecord {
    P2pTraceRecord {
        ts: format_trace_timestamp(event.ts_unix_ms),
        dir: event.dir,
        msg: event.msg,
        peer: event.peer.to_string(),
        conn: event.conn,
        mid: render_message_id(event.mid),
        summary: event.summary.map(render_summary),
    }
}

fn render_trace_dropped_record(event: TraceDroppedEvent) -> TraceDroppedRecord {
    TraceDroppedRecord {
        ts: format_trace_timestamp(event.ts_unix_ms),
        table: event.table.name(),
        queue_full_dropped: event.queue_full_dropped,
        sampled_dropped: event.sampled_dropped,
    }
}

struct TableWriter {
    file: BufWriter<tokio::fs::File>,
    encode_buf: Vec<u8>,
}

impl TableWriter {
    fn new(file: tokio::fs::File) -> Self {
        Self {
            file: BufWriter::new(file),
            encode_buf: Vec::with_capacity(TRACE_BUFFER_FLUSH_BYTES),
        }
    }

    fn append_json<T: Serialize>(&mut self, record: &T) -> Result<(), serde_json::Error> {
        serde_json::to_writer(&mut self.encode_buf, record)?;
        self.encode_buf.push(b'\n');
        Ok(())
    }

    async fn flush_buffer(&mut self, flush_file: bool) -> std::io::Result<()> {
        if !self.encode_buf.is_empty() {
            self.file.write_all(&self.encode_buf).await?;
            self.encode_buf.clear();
        }

        if flush_file {
            self.file.flush().await?;
        }

        Ok(())
    }
}

struct TraceWriter {
    trace_dir: PathBuf,
    peer_message: Option<TableWriter>,
    trace_dropped: Option<TableWriter>,
    last_file_flush: Instant,
}

impl TraceWriter {
    async fn new(trace_dir: PathBuf) -> std::io::Result<Self> {
        tokio::fs::create_dir_all(&trace_dir).await?;

        Ok(Self {
            peer_message: Some(TableWriter::new(
                tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(trace_dir.join(TraceTable::PeerMessage.file_name()))
                    .await?,
            )),
            trace_dropped: Some(TableWriter::new(
                tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(trace_dir.join(TraceTable::TraceDropped.file_name()))
                    .await?,
            )),
            trace_dir,
            last_file_flush: Instant::now(),
        })
    }

    fn has_open_files(&self) -> bool {
        self.peer_message.is_some() || self.trace_dropped.is_some()
    }

    async fn write_batch(&mut self, batch: Vec<TraceEvent>, force_flush: bool) {
        let mut failed_tables = Vec::new();

        for event in batch {
            let table = event.table();
            let Some(table_writer) = self.table_writer_mut(table) else {
                continue;
            };

            let append_result = match event {
                TraceEvent::PeerMessage(event) => {
                    let record = render_peer_message_record(event);
                    table_writer.append_json(&record)
                }
                TraceEvent::TraceDropped(event) => {
                    let record = render_trace_dropped_record(event);
                    table_writer.append_json(&record)
                }
            };

            if let Err(error) = append_result {
                warn!(
                    ?error,
                    table = table.name(),
                    "failed to serialize trace event"
                );
            }
        }

        let flush_file = force_flush || self.last_file_flush.elapsed() >= TRACE_FILE_FLUSH_INTERVAL;
        for table in TraceTable::ALL {
            let should_flush_buffer = self.table_writer(table).is_some_and(|table_writer| {
                table_writer.encode_buf.len() >= TRACE_BUFFER_FLUSH_BYTES
            });

            if !should_flush_buffer && !flush_file && !force_flush {
                continue;
            }

            let Some(table_writer) = self.table_writer_mut(table) else {
                continue;
            };

            if let Err(error) = table_writer.flush_buffer(flush_file || force_flush).await {
                warn!(
                    ?error,
                    table = table.name(),
                    trace_dir = ?self.trace_dir,
                    "disabling trace table after write failure"
                );
                failed_tables.push(table);
            }
        }

        if flush_file || force_flush {
            self.last_file_flush = Instant::now();
        }

        for table in failed_tables {
            self.disable_table(table);
        }
    }

    async fn flush_all(&mut self) {
        self.write_batch(Vec::new(), true).await;
    }

    fn disable_table(&mut self, table: TraceTable) {
        match table {
            TraceTable::PeerMessage => {
                self.peer_message = None;
            }
            TraceTable::TraceDropped => {
                self.trace_dropped = None;
            }
        }
    }

    fn table_writer(&self, table: TraceTable) -> Option<&TableWriter> {
        match table {
            TraceTable::PeerMessage => self.peer_message.as_ref(),
            TraceTable::TraceDropped => self.trace_dropped.as_ref(),
        }
    }

    fn table_writer_mut(&mut self, table: TraceTable) -> Option<&mut TableWriter> {
        match table {
            TraceTable::PeerMessage => self.peer_message.as_mut(),
            TraceTable::TraceDropped => self.trace_dropped.as_mut(),
        }
    }
}

/// Dedicated task that drains the trace channel and writes JSONL records.
/// Batches writes for performance. Runs until the channel is closed or all
/// tables have been disabled.
async fn run_trace_writer(mut rx: mpsc::Receiver<TraceEvent>, mut writer: TraceWriter) {
    loop {
        let mut batch = Vec::with_capacity(TRACE_MAX_BATCH_EVENTS);
        let mut receiver_closed = false;

        match rx.recv().await {
            Some(event) => batch.push(event),
            None => receiver_closed = true,
        }

        if receiver_closed {
            writer.flush_all().await;
            break;
        }

        let deadline = Instant::now() + TRACE_BATCH_LINGER;
        let sleep = time::sleep_until(deadline);
        tokio::pin!(sleep);

        while batch.len() < TRACE_MAX_BATCH_EVENTS {
            match rx.try_recv() {
                Ok(event) => {
                    batch.push(event);
                    continue;
                }
                Err(TryRecvError::Disconnected) => {
                    receiver_closed = true;
                    break;
                }
                Err(TryRecvError::Empty) => {}
            }

            tokio::select! {
                maybe_event = rx.recv() => {
                    match maybe_event {
                        Some(event) => batch.push(event),
                        None => {
                            receiver_closed = true;
                            break;
                        }
                    }
                }
                _ = &mut sleep => break,
            }
        }

        writer.write_batch(batch, receiver_closed).await;

        if !writer.has_open_files() {
            warn!(trace_dir = ?writer.trace_dir, "all trace tables have been disabled");
            break;
        }

        if receiver_closed {
            writer.flush_all().await;
            break;
        }
    }
}

fn trace_dir_from_env() -> Option<PathBuf> {
    let trace_dir = std::env::var_os(TRACE_DIR_ENV).map(PathBuf::from);

    match trace_dir {
        Some(path) if !path.as_os_str().is_empty() => Some(path),
        _ => legacy_trace_dir_from_env(),
    }
}

fn legacy_trace_dir_from_env() -> Option<PathBuf> {
    let path = std::env::var_os(LEGACY_TRACE_FILE_ENV).map(PathBuf::from)?;
    if path.as_os_str().is_empty() {
        return None;
    }

    let parent_dir = path.parent().unwrap_or_else(|| Path::new("."));
    Some(parent_dir.join(TRACE_DIR_NAME))
}

/// Create the trace channel and spawn the writer task.
/// Reads the trace directory from `ZEBRA_P2P_TRACE_DIR`.
///
/// For compatibility, `ZEBRA_P2P_TRACE_FILE` also enables tracing, but now
/// writes per-table files to a sibling `traces/` directory.
pub(crate) async fn init_tracing() -> P2pTracer {
    match trace_dir_from_env() {
        Some(trace_dir) => match TraceWriter::new(trace_dir.clone()).await {
            Ok(writer) => {
                let (tx, rx) = mpsc::channel(TRACE_CHANNEL_CAPACITY);
                tokio::spawn(run_trace_writer(rx, writer));
                info!(trace_dir = ?trace_dir, "P2P message tracing enabled");
                P2pTracer::new(tx)
            }
            Err(error) => {
                warn!(
                    ?error,
                    trace_dir = ?trace_dir,
                    "failed to initialize P2P trace directory, disabling tracing"
                );
                P2pTracer::noop()
            }
        },
        None => P2pTracer::noop(),
    }
}
