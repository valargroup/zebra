//! Tests for the P2P message tracing infrastructure.

use std::{sync::atomic::AtomicU64, sync::Arc};

use super::*;

#[test]
fn trace_record_serializes_to_json() {
    let record = P2pTraceRecord {
        ts: "2026-03-29T12:00:00.000Z".to_string(),
        dir: "send",
        msg: "inv",
        peer: "192.168.1.50:8233".to_string(),
        conn: 42,
        mid: "inv:abc123+3".to_string(),
        summary: Some(PayloadSummary {
            count: Some(3),
            hashes: vec!["abc123".to_string(), "def456".to_string()],
            height: None,
            nonce: None,
        }),
    };

    let json = serde_json::to_string(&record).expect("should serialize");
    assert!(json.contains("\"dir\":\"send\""));
    assert!(json.contains("\"msg\":\"inv\""));
    assert!(json.contains("\"conn\":42"));
    let _: serde_json::Value = serde_json::from_str(&json).expect("should be valid JSON");
}

#[test]
fn trace_record_skips_none_summary() {
    let record = P2pTraceRecord {
        ts: "2026-03-29T12:00:00.000Z".to_string(),
        dir: "recv",
        msg: "verack",
        peer: "10.0.0.1:8233".to_string(),
        conn: 1,
        mid: "verack:1:0".to_string(),
        summary: None,
    };

    let json = serde_json::to_string(&record).expect("should serialize");
    assert!(!json.contains("summary"));
}

#[test]
fn dropped_record_skips_zero_fields() {
    let record = TraceDroppedRecord {
        ts: "2026-03-29T12:00:00.000Z".to_string(),
        table: "peer_message",
        queue_full_dropped: 0,
        sampled_dropped: 4,
    };

    let json = serde_json::to_string(&record).expect("should serialize");
    assert!(!json.contains("queue_full_dropped"));
    assert!(json.contains("\"sampled_dropped\":4"));
}

#[test]
fn payload_summary_skips_empty_fields() {
    let summary = PayloadSummary {
        count: Some(5),
        hashes: Vec::new(),
        height: None,
        nonce: None,
    };

    let json = serde_json::to_string(&summary).expect("should serialize");
    assert!(json.contains("\"count\":5"));
    assert!(!json.contains("hashes"));
    assert!(!json.contains("height"));
    assert!(!json.contains("nonce"));
}

#[test]
fn noop_tracer_does_not_panic() {
    let tracer = P2pTracer::noop();
    tracer.trace(TraceEvent::TraceDropped(TraceDroppedEvent {
        ts_unix_ms: 0,
        table: TraceTable::PeerMessage,
        queue_full_dropped: 1,
        sampled_dropped: 2,
    }));
}

#[test]
fn full_channel_drops_without_blocking() {
    let (tx, _rx) = mpsc::channel(1);
    let tracer = P2pTracer::new(tx);

    tracer.trace(TraceEvent::PeerMessage(PeerMessageEvent {
        ts_unix_ms: 0,
        dir: "send",
        msg: "ping",
        peer: Arc::from("127.0.0.1:8233"),
        conn: 0,
        mid: TraceMessageId::Nonce {
            prefix: "ping",
            nonce: 1,
        },
        summary: None,
    }));

    tracer.trace(TraceEvent::PeerMessage(PeerMessageEvent {
        ts_unix_ms: 1,
        dir: "send",
        msg: "pong",
        peer: Arc::from("127.0.0.1:8233"),
        conn: 0,
        mid: TraceMessageId::Nonce {
            prefix: "pong",
            nonce: 1,
        },
        summary: None,
    }));
}

#[test]
fn noop_tracer_skips_message_id_work() {
    let tracer = P2pTracer::noop();
    let seq = AtomicU64::new(0);

    tracer.trace_msg(
        "send",
        &Message::GetAddr,
        &Arc::from("127.0.0.1:8233"),
        7,
        &seq,
    );

    assert_eq!(seq.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
fn message_id_is_deterministic() {
    let seq = AtomicU64::new(0);

    let ping = render_message_id(message_id(&Message::Ping(Nonce(12345)), 1, &seq));
    let pong = render_message_id(message_id(&Message::Pong(Nonce(12345)), 1, &seq));

    assert_eq!(ping, "ping:12345");
    assert_eq!(pong, "pong:12345");
}

#[test]
fn summarize_verack_has_no_summary() {
    let (cmd, summary) = summarize_message(&Message::Verack);
    assert_eq!(cmd, "verack");
    assert!(summary.is_none());
}

#[test]
fn summarize_mempool_has_no_summary() {
    let (cmd, summary) = summarize_message(&Message::Mempool);
    assert_eq!(cmd, "mempool");
    assert!(summary.is_none());
}

#[test]
fn summarize_ping_has_nonce() {
    let (cmd, summary) = summarize_message(&Message::Ping(Nonce(99)));
    assert_eq!(cmd, "ping");
    let summary = summary.expect("should have summary");
    assert_eq!(summary.nonce, Some(99));
}

#[test]
fn connection_id_is_monotonic() {
    let id1 = next_connection_id();
    let id2 = next_connection_id();
    assert!(id2 > id1);
}

#[tokio::test]
async fn writer_task_produces_per_table_jsonl() {
    let dir = tempfile::tempdir().expect("tempdir");
    let trace_dir = dir.path().join("traces");

    let (tx, rx) = mpsc::channel(16);
    let writer = TraceWriter::new(trace_dir.clone())
        .await
        .expect("trace writer");
    let handle = tokio::spawn(run_trace_writer(rx, writer));

    for i in 0..3 {
        tx.send(TraceEvent::PeerMessage(PeerMessageEvent {
            ts_unix_ms: 1_743_249_600_000 + i as i64,
            dir: "send",
            msg: "ping",
            peer: Arc::from("127.0.0.1:8233"),
            conn: 1,
            mid: TraceMessageId::Nonce {
                prefix: "ping",
                nonce: i as u64,
            },
            summary: Some(CompactPayloadSummary {
                nonce: Some(i as u64),
                count: None,
                hashes: Vec::new(),
                height: None,
            }),
        }))
        .await
        .expect("send should succeed");
    }

    tx.send(TraceEvent::TraceDropped(TraceDroppedEvent {
        ts_unix_ms: 1_743_249_700_000,
        table: TraceTable::PeerMessage,
        queue_full_dropped: 2,
        sampled_dropped: 5,
    }))
    .await
    .expect("send should succeed");

    drop(tx);
    handle.await.expect("writer task should complete");

    let peer_message_path = trace_dir.join("peer_message.jsonl");
    let peer_message_contents = tokio::fs::read_to_string(&peer_message_path)
        .await
        .expect("read peer_message");
    let peer_message_lines: Vec<&str> = peer_message_contents.lines().collect();
    assert_eq!(peer_message_lines.len(), 3);
    for line in &peer_message_lines {
        let _: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
    }

    let trace_dropped_path = trace_dir.join("trace_dropped.jsonl");
    let trace_dropped_contents = tokio::fs::read_to_string(&trace_dropped_path)
        .await
        .expect("read trace_dropped");
    let trace_dropped_lines: Vec<&str> = trace_dropped_contents.lines().collect();
    assert_eq!(trace_dropped_lines.len(), 1);
    assert!(trace_dropped_lines[0].contains("\"sampled_dropped\":5"));
    assert!(trace_dropped_lines[0].contains("\"queue_full_dropped\":2"));
}
