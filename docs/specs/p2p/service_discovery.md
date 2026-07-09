# Zakura P2P Service Discovery — Specification

## Overview

Service discovery is how two connected peers learn **which services each other speaks**, and
how a service (block-sync, header-sync, discovery, legacy gossip) **acquires the peers** that
offer it. It spans two moments: the **control handshake**, where the pair negotiate a
capability mask once for the connection's life, and **stream fan-out**, where each negotiated
service is handed a tagged QUIC stream and decides whether to admit the peer.

The design keeps **three namespaces distinct** — conflating them is the classic bug:

1. **Stream kind** (`u16`, in every `StreamPrelude`) — _routes_ a stream to its owning
   service.
2. **Capability bit** (`u64`, negotiated in the handshake) — _authorises_ which stream kinds
   may be opened.
3. **Service id** (`ZakuraServiceId`, in signed discovery records) — _ranks who to dial_;
   never routes or authorises anything on a live connection.

Capability advertisement is **static per node, symmetric, and negotiated by intersection**: a
node advertises exactly the services it runs, and the accepted set is the bitwise AND of the
two sides. There is no dynamic capability probing on a live connection — what a connection can
carry is fixed at handshake time.

## Glossary

Plain term (code identifier):

- **Capability mask** (`capabilities`, `accepted_capabilities`) — the `u64` bitset a node
  advertises / the pair agree on.
- **Supported capabilities** (`registry.supported_capabilities()`) — the OR of every
  registered service's single capability bit; a node cannot advertise more.
- **Stream kind / version** (`StreamPrelude.stream_kind`, `.stream_version`) — the routing
  tag and protocol version on a stream.
- **Service registry** (`ServiceRegistry`, `Service` trait) — the transport-side router that
  maps a negotiated stream to its owning service and calls `add_peer` / `remove_peer`.
- **Peer session** (`Peer`, carrying `id`, `conn_id`, `direction`, and a
  `HashMap<u16, ServiceStream>`) — the per-connection handle a service claims streams from
  via `take_stream(kind)`.
- **Demand hooks** (`Service::wants_peer`, `wants_ordered_stream`) — a service's advisory
  "do I have room / want this?" check before a stream is opened.
- **Service id** (`ZakuraServiceId`, e.g. `zakura.block_sync.v1`) — the discovery-namespace
  advertisement of a service, used only for candidate selection.
- **Symmetric service** — block-sync, the only ordered stream both peers open (so it can
  collide and needs a tiebreak).

## Capability negotiation

**MUST:**

- A node's advertised `capabilities` MUST equal `registry.supported_capabilities()` — the OR
  of the single capability bit of each locally registered service. A node MUST NOT advertise
  a capability whose service is not registered.
- The **initiator** sends its `capabilities` in `ZakuraControlHello`. The **responder**
  computes `accepted_capabilities = hello.capabilities & local_supported`, returns it in
  `ZakuraControlAck`, and the initiator MUST validate it. Negotiation is a pure
  **intersection** — optional capabilities present on only one side are silently dropped.
- `required_capabilities` defaults to 0. A peer MUST reject the connection with
  `MissingRequiredCapability` only if a _required_ bit is absent — an unmatched _optional_
  capability MUST NOT fail the handshake.
- Each declared `Stream.capability` MUST be exactly one non-zero bit
  (`is_power_of_two`, else `InvalidCapability`), and each stream kind MUST be claimed by
  exactly one service (`DuplicateKind`). These are enforced at registry build.

**Authorisation to open a stream (MUST).** A peer may open stream kind _k_ iff the owning
service's capability bit is fully within `accepted_capabilities`. On the inbound side a stream
whose `stream.capability` is not a subset of `accepted_capabilities` MUST be reset with
`ZAKURA_CLOSE_UNKNOWN_STREAM`. So "may I open a block-sync stream?" is exactly
`accepted_capabilities & ZAKURA_CAP_BLOCK_SYNC == ZAKURA_CAP_BLOCK_SYNC`.

**Discovery-record advertisement is independent (MUST).** A signed discovery record separately
advertises `services: Vec<ZakuraServiceId>` (default: discovery, block_sync, header_sync,
legacy_gossip, legacy_requests, service_discovery). This is read by a _remote_ node to decide
the peer is worth dialing for a given service, **before** any handshake. It MUST NOT be treated
as authorisation to open a stream — the handshake capability mask is the only such authority.
Note `service_discovery` and `legacy_requests` have service ids but no dedicated capability bit
(legacy requests ride `ZAKURA_CAP_LEGACY_GOSSIP`).

## Stream identification and routing

Every stream (other than the control handshake) opens with a `StreamPrelude` carrying
`stream_kind` and `stream_version`. Inbound stream admission (`admit_bi_stream`) MUST, in
order:

1. Acquire a stream-concurrency permit and charge one stream-open token **before** parsing —
   so protocol-invalid churn still costs budget.
2. Read and validate the prelude magic (`ZKST`).
3. Look up `(stream_kind, stream_version)` in the registry; an unknown kind or unsupported
   version MUST reset with `ZAKURA_CLOSE_UNKNOWN_STREAM`. (This is how header-sync v1 is
   refused after the v5 break.)
4. Enforce the capability gate (above).
5. Enforce mode/request-id consistency: `RequestResponse` MUST carry a `request_id`, `Ordered`
   MUST NOT — a violation cancels the whole connection (`ZAKURA_CLOSE_BAD_PRELUDE`).
6. Route: an accepted request stream dispatches to the owning service's
   `RequestResponseService`; an accepted ordered stream is handed to the service via
   `ServiceRegistry::add_escalated_peer` with just that stream.

The canonical kind → service → capability table lives in the [README](README.md#stream-kinds).
Per-kind receiver frame caps (`app_frame_cap_for_stream_kind` / `inbound_frame_cap_for_stream_kind`)
clamp header-sync and block-sync to their message ceilings regardless of the negotiated frame
size.

## How a service acquires peers

Three layers cooperate; keep them separate.

**(a) Transport fan-out (the primary mechanism).** When a connection finishes handshake,
`serve_connection` fans the peer out to every service whose capability is in
`accepted_capabilities`:

- It computes the demanded ordered streams
  (`registry.ordered_streams_for_negotiated(accepted_capabilities)`).
- The **initiator** opens all demanded ordered streams; the **responder** additionally opens
  only block-sync (the sole symmetric service).
- Each opened stream routes to its owning service via `add_escalated_peer`, which calls
  `Service::add_peer(Peer)` once per service, handing only that service's streams. The `Peer`
  carries `conn_id` (the generation, see [connections.md](connections.md)) into the session.
- Before opening, the demand hooks `wants_peer` / `wants_ordered_stream` give a service an
  advisory chance to decline (e.g. no free slots); `add_peer` is the **authoritative**
  admission.

**(b) Per-service admission.** Each service decides independently:

- **block-sync** — admits iff the peer is not parked and a direction slot is free
  (`peer_slots_free`); `add_peer` re-checks `can_admit_peer`, takes stream 6, and spawns the
  per-peer routine. A block-sync peer is _usable for serving_ only after it has sent an
  `MSG_BS_STATUS` (`has_received_status`).
- **header-sync** — admits iff a direction slot is free; takes stream 5 and spawns its sink.
- **discovery** — admits with a local-room check, takes stream 4, then runs the async
  self-record/peer-sample exchange.

Per-service admission is capped by `ServicePeerLimits` (`max_inbound_peers` /
`max_outbound_peers`, default 256 each), counted independently per direction, returning
`ServiceAdmissionDecision::{Admit, RejectFull, RejectNotUseful, RejectBackoff, RejectUnsupported}`.

**(c) Discovery-driven candidate selection (who to dial).** Separately from admission, a sync
reactor learns _candidate_ peers to dial from discovery's service-aware API
(`service_candidates` / `header_sync_candidates` / `block_sync_candidates`), which returns
connected peers advertising the service (ranked by live-summary preference) plus dialable
records filtered by that `ZakuraServiceId`. The transport then dials, and fan-out in (a)
delivers the stream. Discovery shares connected-peer state via `watch` channels.

## The symmetric block-sync collision

**MUST.** Block-sync is the only service both peers open (stream kind 6), so a
simultaneous double-open is expected and MUST NOT tear down the connection. It is resolved by
the node-id tiebreak `i_open_collision_winner(local, remote) = local_id < remote_id`: the
winner keeps its own stream and parks the peer's duplicate; the loser adopts the peer's
stream. Because the comparison is mirror-stable, both ends agree.

Any **other** peer-opened ordered stream arriving at an initiator (a service the initiator did
not expect the peer to open) MUST close the connection (`unexpected_stream`); a **duplicate**
accepted kind (a second stream of a kind already wired) MUST also close the connection. A
demand re-check (`wants_ordered_stream`) that finds no demand parks that one stream locally
(cancelling only its token) while keeping the connection.

## Edge cases and bounds

**Capability lies.** A node cannot advertise a capability it does not run — the advertised mask
is derived from the registry, not from config, so `supported_capabilities` is exactly the set
of registered services. A peer that opens a stream for an un-negotiated capability is reset,
not trusted.

**Version skew.** A stream is admitted only for a declared `(kind, version)` pair. A breaking
protocol change bumps the stream version (header-sync 4→5, block-sync current 2), and old
versions are refused with `ZAKURA_CLOSE_UNKNOWN_STREAM` rather than mis-parsed.

**Two streams, one capability.** Legacy gossip (kind 2, Ordered) and legacy requests (kind 3,
RequestResponse) share `ZAKURA_CAP_LEGACY_GOSSIP`. The single-bit-per-stream invariant still
holds (each stream declares the same one bit); the capability gate authorises both, and the
registry routes each kind to its handler.

**Stale service teardown after a reconnect.** A service's `remove_peer` carries the connection
generation `conn_id`; a superseded connection's late `remove_peer` MUST be a no-op unless it
matches the currently-admitted generation, so it cannot evict the peer's live successor session
(the generation guard, [connections.md](connections.md)).

**Discovery advertises a service the peer no longer runs.** A record's `services` list is a
hint; if a dialed peer's negotiated capability mask lacks the service, no stream of that kind
is opened and the service simply does not acquire the peer — the record is not authoritative.

**Block-sync admits but the peer never sends status.** A block-sync peer counts against the
service cap on admission but is not _usable for serving_ until it sends `MSG_BS_STATUS`; a peer
that connects and stays silent is handled by block-sync's own liveness/park policy (see
`congestion_control.md`), not by service discovery.

**Numeric / bounds safety (MUST).** Every wire field in the handshake and preludes is bounded
and trailing-byte-rejecting; capability and channel masks are plain `u64` intersections;
`ZakuraServiceId` is bounded ASCII (`MAX_ZAKURA_SERVICE_ID_BYTES` = 64, ≤ 32 per record).

**Observability (SHOULD).** Stream admission and per-service `add_peer` / `remove_peer` SHOULD
be traced with the stream-kind label and direction, so a trace shows which services each peer
was admitted to and why any admission was rejected.

## Defaults

| Knob | Default | Meaning |
| --- | --- | --- |
| capability bits | `LEGACY_GOSSIP=1<<0 HEADER_SYNC=1<<1 DISCOVERY=1<<2 BLOCK_SYNC=1<<3` | per-service authorisation bits |
| `required_capabilities` | 0 | no capability is mandatory by default |
| stream versions | discovery 1 · header-sync 5 · block-sync 2 · legacy 1 | current `(kind, version)` pairs |
| `ServicePeerLimits.max_inbound_peers` / `max_outbound_peers` | 256 / 256 | per-service, per-direction admission cap |
| `DEFAULT_SERVICE_INBOUND_QUEUE_DEPTH` / `_OUTBOUND_` | 128 / 128 | reserved per-service queue depths (not yet enforced) |
| `DEFAULT_SERVICE_MAX_PENDING_ESCALATIONS` | 32 | reserved lazy-escalation bound (not yet enforced) |
| advertised service ids | discovery, block_sync, header_sync, legacy_gossip, legacy_requests, service_discovery | default signed-record `services` set |
| `MAX_ZAKURA_SERVICE_ID_BYTES` / `MAX_SERVICES_PER_RECORD` | 64 / 32 | service-id string / count bounds |
| symmetric service | block-sync (kind 6) | only ordered stream both peers open |
