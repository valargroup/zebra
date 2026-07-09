# Zakura P2P Connections / Transport — Specification

## Overview

The transport carries every Zakura service over **iroh QUIC**. A connection is opened by a
**dial** (outbound) or an **accept** (inbound), authenticated by the peer's iroh node id,
negotiated by a **control handshake**, and then **registered** in a single per-peer
registry before any service sees it. One `ZakuraProtocolHandler` owns the endpoint; one
`ZakuraSupervisor` owns the registry; one `ServiceRegistry` fans a registered peer out to
the services it negotiated.

The transport's job is to turn an unauthenticated, attacker-reachable QUIC socket into a
small set of authenticated, deduplicated, rate-limited, bounded peer sessions. It balances
three goals: **liveness** (a genuinely dead or wedged peer is reaped promptly and its slot
reused), **stability** (a healthy long-lived peer is never churned by a redial race or a
late teardown), and **bounded cost** (connections, streams, handshakes, frames, and queues
are all capped, so no peer can exhaust local resources).

The subtle invariant that ties liveness and stability together is the **generation guard**
(Zakura WS-1): a per-peer registry entry is keyed by node id _and_ a monotonic connection
generation, so the teardown of a superseded connection can never delete its live
successor.

## Glossary

Plain term (code identifier):

- **Endpoint** (`ZakuraEndpoint` / `ZakuraProtocolHandler`) — the single iroh QUIC endpoint
  and its ALPN protocol handler.
- **Peer identity** (`ZakuraPeerId`) — the authenticated iroh node id (Ed25519 public key),
  bounded to `MAX_IROH_NODE_ID_BYTES` (128).
- **Connection generation** (`ZakuraConnId`, a `u64`) — a monotonically increasing id
  stamped on every accepted/dialed connection from one `next_conn_id` counter.
- **Registry / supervisor** (`ZakuraSupervisor`, `active_by_peer`) — the one map from peer
  identity to its current live connection entry (`ZakuraPeerConnectionEntry`).
- **Transcript hash** (`transcript_hash`, `TRANSCRIPT_HASH_BYTES` = 32) — a deterministic
  per-connection value (derived from the initiator's node id) that both ends of a
  simultaneous-open race compute identically; the duplicate-resolution seed.
- **Control handshake** (`ZakuraControlHello` / `ZakuraControlAck`) — the version /
  identity / capability / limits exchange on the first bidirectional stream.
- **Negotiated limits** (`ZakuraConnectionLimits`, from `ZakuraLocalLimits::clamp`) — the
  per-connection frame/message/stream/idle ceilings, each the min of local and peer.
- **Frame** (`Frame { message_type, flags, payload }`) — the length-prefixed unit on a
  stream; header `FRAME_HEADER_BYTES` = 8.
- **Stream kind / mode** (`StreamPrelude.stream_kind`, `StreamMode::{Ordered, RequestResponse}`)
  — which service a stream belongs to, and whether it is long-lived or per-request.
- **Freshness reaper** (`freshness_reaper`, `freshness_tx`) — the app-level idle reaper,
  bumped on any inbound frame.
- **Close cause** (`CloseCause`) — first-writer-wins attribution of why a connection closed.

## Connection lifecycle

**MUST:**

- Every connection MUST take a distinct generation `conn_id = next_conn_id.fetch_add(1)`
  before anything else, on both the accept and dial paths.
- Every connection MUST hold a **global admission** permit (`admission`, a
  `Semaphore(max_connections)`) for its whole life. Accept without a permit closes with
  `ZAKURA_CLOSE_RESOURCE`; dial without one fails `ResourceLimit("admission")`.
- Every connection MUST hold a **pending-handshake** permit
  (`Semaphore(max_pending_handshakes)`) for the duration of the control handshake only, so
  the number of concurrent un-negotiated handshakes is bounded.
- The peer identity MUST be the QUIC-authenticated `connection.remote_node_id()`, read
  before the handshake, and the handshake MUST bind the claimed id to it (see
  [Security](#security-identity-and-authentication)).
- A connection becomes **active** only when `register_and_serve` calls
  `supervisor.register(conn_id, peer_id, remote_ip, transcript_hash, …)` and it returns
  `Registered`. Registration MUST precede service fan-out; a rejected registration MUST
  close the connection without ever handing it to a service.
- Teardown MUST cancel the connection's `disconnect_token`, drain stream workers (bounded
  by `STREAM_WORKER_DRAIN_TIMEOUT`, 1 s) then abort them, and call — in order —
  `registry.remove_peer(peer_id, conn_id, …)` and `supervisor.deregister(peer_id, conn_id)`,
  **both carrying this connection's generation** (see [generation guard](#registry-deduplication-and-the-generation-guard)).

**Handshake sequence.**

- The control handshake runs on the **first bidirectional QUIC stream**. The initiator
  `open_bi` and sends `ZakuraControlHello`; the responder `accept_bi` (under
  `control_timeout`), validates, and replies `ZakuraControlAck`.
- `ZakuraControlHello` carries: magic, `control_version` (`CONTROL_VERSION` = 1), selected
  protocol, `handshake_path` (`Native` for a direct dial, `Upgraded` for a legacy TCP→QUIC
  hand-off), `role`, `network_id`, `chain_id`, the sender's `iroh_node_id`, a random 32-byte
  `peer_nonce`, the upgrade nonces + transcript (all-zero on `Native`), `capabilities`, and
  `initial_limits` (`ZakuraLimits`).
- `ZakuraControlAck` echoes the nonces and returns `accepted_capabilities`,
  `accepted_channels`, and `accepted_limits`. The initiator MUST validate the nonce
  round-trip and the returned limits before serving.
- The negotiated per-connection limits are `ZakuraLocalLimits::clamp(&peer_limits)` — each
  ceiling is the min of the local hard limit and the peer's advertised value.

**SHOULD:**

- The endpoint SHOULD bind loopback-only when `listen_addr` is unset, so the experimental
  `p2p-v2/1` ALPN surface is not exposed on all interfaces for a dial-only node.
- On the inbound path the remote IP SHOULD be recovered from the connection's confirmed
  direct path (iroh's router consumes the address before `accept`); a relay-only path
  yields no attributable IP and falls back to the global cap alone.

## Registry, deduplication, and the generation guard

The registry (`ZakuraSupervisorState`) holds one entry per peer
(`active_by_peer: HashMap<ZakuraPeerId, ZakuraPeerConnectionEntry>`), a per-IP count
(`active_by_ip: HashMap<IpAddr, usize>`), and a tiny authenticated-peer transcript map. An
entry is `{ conn_id, outbound_handle, disconnect_token, registered_at, remote_ip }`.

**MUST — single live connection per peer.**

- At most one connection per peer identity MUST be `active_by_peer` at a time. A second
  connection to the same peer MUST be resolved deterministically, never left as a silent
  duplicate.
- Resolution uses the **transcript-hash tiebreak**: in `register_authenticated`, if an
  incumbent hash exists and `incumbent_hash <= new_hash` the incumbent wins (`Duplicate`),
  otherwise the newcomer replaces it (`Upgraded`). Ties keep the incumbent. Because the
  transcript hash is derived from the **initiator's node id**
  (`native_connection_transcript_hash`), both ends of a simultaneous open compute the same
  winner and converge.
- The IP accounting invariant `sum(active_by_ip) == count(active_by_peer with Some(ip))`
  MUST hold (checked by `debug_assert_accounting`). Duplicate eviction MUST cancel exactly
  the **incumbent's** `disconnect_token` (never the newcomer's) and adjust `active_by_ip`
  symmetrically.

**MUST — the generation guard (Zakura WS-1).**

A registry entry is addressed by `(peer_id, conn_id)`, not `peer_id` alone. When a
duplicate replaces an incumbent (the `Upgraded` path) or a peer restarts and redials, the
_old_ connection's serve loop is still unwinding and will call
`deregister(peer_id, old_conn_id)` and `remove_peer(peer_id, old_conn_id)`. Without the
guard, that late teardown would delete the **new** connection's entry and tear down its
healthy service sessions.

- `deregister` and `remove_peer` MUST be **no-ops unless the caller's `conn_id` equals the
  currently-registered generation**: `if entry.conn_id != conn_id { return }`.
- Every layer MUST thread the generation: `Peer.conn_id` carries it into each service
  session; `Service::remove_peer(&self, peer, conn_id)` and
  `ServiceRegistry::remove_peer(peer, conn_id, negotiated)` compare it (e.g. block-sync
  removes only if `record.conn_id == conn_id`; discovery keys its admitted-peer entry by
  `conn_id`).
- A superseded connection's teardown MUST NOT decrement the peer's IP count, cancel the
  successor's token, or deregister the successor's transcript.

**SHOULD — reclaim stale slots fast without flapping.**

- On the `Upgraded` path the replaced incumbent's token SHOULD be cancelled immediately so
  its slot frees in milliseconds.
- On the `Duplicate` path (incumbent wins) the newcomer is closed neutrally
  (`b"duplicate"`) and relies on its redial; but if the incumbent has been registered for
  at least `ZAKURA_DUPLICATE_EVICT_MIN_AGE` (300 s) and its token is not already cancelled,
  the incumbent SHOULD be cancelled so a restarted peer reclaims the slot in milliseconds
  rather than waiting the ~150 s QUIC idle timeout. A younger incumbent is kept, so a redial
  race does not flap a fresh healthy connection.

## Framing and streams

**MUST:**

- A `Frame` is an 8-byte header (`message_type: u16`, `flags: u16`, `payload_len: u32`, all
  LE) followed by `payload_len` bytes. Encode MUST reject a payload larger than
  `max_frame_bytes - FRAME_HEADER_BYTES`; decode MUST reject an oversize length **before
  allocating** the payload and MUST reject trailing bytes.
- The streaming reader (`read_frame`) MUST enforce `frame_len <= max_frame_bytes` before
  allocation. The receiver's cap is the _authoritative_ one:
  `inbound_frame_cap_for_stream_kind` clamps to `max_message_bytes + header` so a peer that
  negotiated a large `max_frame_bytes` still cannot force a large allocation.
- Every stream opens with a `StreamPrelude` (magic `ZKST`, `stream_kind`, `stream_version`,
  optional `request_id`, `max_frame_bytes`). The prelude read MUST be bounded by
  `prelude_timeout` (3 s). An `Ordered` stream MUST NOT carry a `request_id`; a
  `RequestResponse` stream MUST carry one — a violation closes the connection with
  `ZAKURA_CLOSE_BAD_PRELUDE`.
- A stream whose `(stream_kind, stream_version)` is not a registered pair MUST be reset with
  `ZAKURA_CLOSE_UNKNOWN_STREAM` (this is how a superseded protocol version — e.g.
  header-sync v1 after the v5 break — is refused).
- All Zakura streams MUST be **bidirectional**: the QUIC transport config sets
  `max_concurrent_uni_streams = 0` and disables datagrams.

**Stream ↔ service mapping.**

- Each `Stream { kind, version, frame_cap, capability, mode }` MUST map to exactly one
  service and exactly one **single-bit** capability. The registry build MUST reject a
  non-single-bit capability (`InvalidCapability`) and two services claiming one kind
  (`DuplicateKind`).
- **Ordered** streams are one long-lived stream per protocol per peer; **RequestResponse**
  streams are one short-lived stream per request. The `read_frame` reader is **not
  cancellation-safe** once the first header byte is consumed, so a persistent ordered
  stream MUST run its reader in a dedicated task rather than as a `select!` branch, so an
  outbound-write branch can never drop a mid-read future and desync the stream.

**SHOULD:** the dialer (initiator) SHOULD proactively open all the ordered streams it
demands; the responder SHOULD additionally open only block-sync (the sole symmetric
service). See [service_discovery.md](service_discovery.md) for the symmetric-collision
tiebreak.

## Timeouts, keepalive, and backpressure

**MUST — no unbounded wait, no idle survivor.**

- The endpoint MUST fail to build unless `keep_alive_interval < quic_idle_timeout` and the
  advertised initial idle timeout `< quic_idle_timeout` (`validate_idle_invariant`), so
  keepalive (10 s) always fires well within the idle timeout (150 s).
- The negotiated idle timeout MUST be clamped to `quic_idle_timeout - 1ms` and floored at
  1 ms, and to at most `LOCAL_MAX_IDLE_TIMEOUT_MILLIS` (10 min) from a peer.
- An in-progress frame read MUST be bounded by `read_timeout = idle_timeout`. The
  **first-byte** wait is `None` for persistent ordered streams (legitimately quiet between
  frames — inter-frame silence MUST NOT cancel the connection) and `Some(idle_timeout)` for
  request streams.
- The **freshness reaper** MUST close a connection (`b"idle"`, cause `idle_timeout`) after
  `idle_timeout` with no inbound frame on any stream; every stream worker bumps
  `freshness_tx` on any inbound frame. Genuinely dead connections are additionally caught by
  the QUIC idle timeout.

**MUST — bounded queues, natural backpressure.**

- Every inbound and outbound path MUST be a bounded `mpsc` channel. The per-connection
  inbound depth is split across its streams
  (`per_stream_inbound_queue_depth = (depth / count).max(1)`).
- When a service's inbound queue is full the stream worker MUST **await** the send rather
  than drop — this stalls QUIC flow control (the 32 MiB receive windows) and applies
  backpressure to the peer.
- Outbound work MUST be non-blocking from the service's view: `try_send_frame` returns
  `Full`/`Closed` and the caller sheds load; `outbound_peer_handles()` MUST only return
  handles with free capacity, so a peer that stopped reading is skipped for new work rather
  than blocking others.
- When a peer stops reading, a `Stopped` write MUST close only that stream; any other write
  error resets the stream and cancels the connection.

## Rate limiting and DoS resistance

**MUST — every admission surface is bounded:**

- **Global connections** — `Semaphore(max_connections)`, default 256, on both accept and
  dial.
- **Per-IP connections** — `max_connections_per_ip`, default 3 (Zakura WS-4;
  `DEFAULT_ZAKURA_MAX_CONNS_PER_IP`, coerced back to 3 if configured 0), enforced in
  `register` and pre-checked by the dialer via `can_accept_remote_ip_with_in_flight`. A
  same-`(peer_id, ip)` re-registration is exempt (a duplicate redial, not a new host).
  Relay-only inbound (no attributable IP) falls back to the global cap only.
- **Pending handshakes** — `Semaphore(max_pending_handshakes)`, default 32.
- **Stream-open rate** — a per-connection `TokenBucket(stream_open_rate_per_second)`,
  default 32/s. The token MUST be charged **before** parsing the prelude, so
  protocol-invalid stream churn (bad prelude, unknown kind, unnegotiated capability) still
  spends budget. Over-rate resets that stream with `ZAKURA_CLOSE_RATE_LIMIT` (connection
  kept).
- **Message rate** — one shared `TokenBucket(message_rate_per_second)` per stream-kind per
  connection (default 2048/s), so N same-kind streams draw one budget. An oversize message
  MUST disconnect (`ZAKURA_CLOSE_OVERSIZE`); a throttled **ordered** frame MUST disconnect
  (`ZAKURA_CLOSE_RATE_LIMIT`, because dropping a solicited ordered frame is a permanent
  gap); a throttled **request** frame is intentionally not rejected.

**MUST — bounded blast radius and allocation:**

- Each peer's work MUST run under a panic-containing supervised task
  (`spawn_supervised_pipe` / `spawn_supervised_peer_task`) so a panicking or hostile peer is
  contained to itself; the build MUST refuse `panic = "abort"` where containment cannot
  work.
- Frame, message, control (16 KiB), and retained request/response
  (`LEGACY_RESPONSE_MAX_AGGREGATE_BYTES`) sizes MUST all be capped, enforced before
  allocation.

## Security, identity, and authentication

**MUST:**

- The peer identity MUST be the QUIC/TLS-authenticated iroh node id
  (`connection.remote_node_id()`). `ZakuraControlHello::validate` MUST reject a hello whose
  claimed `iroh_node_id` does not equal the authenticated id (`IdentityMismatch`).
- The handshake MUST enforce magic, `control_version`, selected protocol,
  `handshake_path`, `role`, `network_id`, and `chain_id`; on the `Native` path all
  upgrade-nonce and transcript fields MUST be zero.
- The endpoint MUST disable iroh relays and side-channel discovery
  (`direct_endpoint_builder`), so every peer has a direct source IP for per-IP admission.
- The node's own secret key MUST be stable across restarts (configured or persisted) so its
  node id is consistent.
- Every wire decoder MUST use hard caps and reject trailing bytes
  (`MAX_PRELUDE_PAYLOAD_BYTES` 4 KiB, `MAX_CONTROL_PAYLOAD_BYTES` 16 KiB,
  `MAX_IROH_NODE_ID_BYTES` 128, `MAX_IROH_DIRECT_ADDRESSES` 8, etc.).

**SHOULD:** the legacy TCP→QUIC upgrade path SHOULD bind a Blake2b transcript over both
legacy version messages plus the upgrade init/accept, so the hand-off cannot be spliced;
the `Native` path skips the transcript (there is no prior TCP leg to bind).

## Edge cases and bounds

**A superseded connection tears down its successor.** The classic use-after-replace: peer
restarts, its new connection registers, then the old serve loop unwinds and deregisters.
The **generation guard** makes the old teardown a no-op because
`old_conn_id != current_conn_id`. Test coverage:
`upgraded_winner_registers_second_loser_cleanup_is_generation_guarded` proves the winner
stays registered with correct IP accounting after the loser's late deregister.

**A simultaneous open races both directions.** Both ends dial each other. The transcript
hash is derived from the initiator's node id, so both compute the same winner; the loser is
closed neutrally and does not churn. Block-sync additionally opens symmetrically and
resolves its stream collision by the node-id tiebreak (see service_discovery.md), keeping
the connection.

**A restarted peer's slot is pinned for ~150 s.** A peer that restarts reconnects from a
fresh ephemeral path; the incumbent entry would otherwise survive until the QUIC idle
timeout. `ZAKURA_DUPLICATE_EVICT_MIN_AGE` (300 s) lets the registry cancel a _stale_
incumbent immediately while keeping a _young_ one, trading flap-resistance against reclaim
latency.

**A peer stops reading and holds our outbound full.** QUIC flow control (32 MiB windows)
plus the bounded app queue apply backpressure; `outbound_peer_handles()` skips the full
peer for new work; the freshness reaper and QUIC idle timeout eventually reap it. No other
peer is blocked.

**A peer opens invalid streams in a loop.** The stream-open token is charged before parsing,
so churn is rate-limited regardless of validity; unknown/unnegotiated/oversize streams are
reset with the appropriate close code, and a mode/request-id violation cancels the whole
connection.

**A peer advertises huge limits.** Negotiated limits are the min of local and peer, floored
where needed; the receiver's frame cap is clamped to `max_message_bytes + header`
independent of the negotiated `max_frame_bytes`, so a large advertised frame size cannot
buy a large allocation.

**Numeric safety (MUST).** All rate refills, byte budgets, and limit clamps MUST saturate or
be checked; token buckets use nanosecond-precision saturating refill; every `.max(1)`
floor prevents a zero limit from wedging a stream.

**Observability (SHOULD).** The transport SHOULD emit connection counters
(`zakura.p2p.conn.{accepted,active,closed.*,rejected.*,duplicate.*}`), queue depth
(`zakura.p2p.queue.depth`), and a first-writer-wins `CloseCause` on every close, so a trace
can attribute why any connection ended.

## Defaults

| Knob | Default | Meaning |
| --- | --- | --- |
| `IROH_VERSION` | `0.92.0` | pinned iroh (QUIC) version |
| `P2P_V2_ALPN` | `p2p-v2/1` | single whole-connection ALPN |
| `DEFAULT_ZAKURA_LISTEN_ADDR` | `0.0.0.0:8234` | native endpoint bind (loopback-only if `None`) |
| `max_connections` | 256 | global connection cap (inbound + outbound) |
| `max_connections_per_ip` | 3 | per-source-IP cap (WS-4; 0 ⇒ 3) |
| `max_pending_handshakes` | 32 | concurrent control handshakes |
| `stream_open_rate_per_second` | 32 | per-connection stream-open token rate |
| `message_rate_per_second` | 2048 | per-stream-kind message token rate |
| `DEFAULT_ZAKURA_QUIC_IDLE_TIMEOUT` | 150 s | QUIC idle + app idle-reaper bound |
| `DEFAULT_ZAKURA_KEEP_ALIVE_INTERVAL` | 10 s | QUIC keepalive |
| `DEFAULT_ZAKURA_PRELUDE_TIMEOUT` | 3 s | stream prelude read deadline |
| `DEFAULT_ZAKURA_CONTROL_TIMEOUT` | 10 s | control read/write + dial connect deadline |
| `DEFAULT_ZAKURA_*_WINDOW` | 32 MiB | QUIC stream/connection send/receive windows |
| `ZAKURA_DUPLICATE_EVICT_MIN_AGE` | 300 s | min incumbent age before stale-eviction on redial |
| `STREAM_WORKER_DRAIN_TIMEOUT` | 1 s | worker drain before abort on teardown |
| `OUTBOUND_STREAM_WRITE_TIMEOUT` | 10 s | ordered-frame write deadline |
| `OUTBOUND_REQUEST_RESPONSE_TIMEOUT` | 30 s | request/response round-trip deadline |
| `FRAME_HEADER_BYTES` | 8 | frame header (type + flags + len) |
| `MAX_CONTROL_PAYLOAD_BYTES` | 16 KiB | control payload hard cap |
| `LOCAL_MAX_CONTROL_FRAME_BYTES` / `LOCAL_MAX_MESSAGE_BYTES` | 1 MiB / 4 MiB | control frame / message ceilings |
| `LOCAL_MAX_OPEN_STREAMS` / `LOCAL_MAX_INBOUND_QUEUE_DEPTH` | 1024 / 4096 | per-connection stream / queue ceilings |
| `LOCAL_MAX_IDLE_TIMEOUT_MILLIS` | 600000 | largest peer-advertisable idle timeout |
| `DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF` / `_MAX_BACKOFF` | 1 s / 30 s | supervised dial backoff (see peer_discovery.md) |
| close codes | `NEUTRAL=0 RESOURCE=1 BAD_PRELUDE=2 RATE_LIMIT=3 OVERSIZE=4 UNKNOWN_STREAM=5` | bounded QUIC reset codes |
