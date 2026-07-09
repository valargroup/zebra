# Zakura P2P Stack — Specification

## Overview

Zakura is Zebra's **second-generation peer-to-peer transport (P2P v2)**. It runs over
**iroh QUIC** alongside the legacy Zcash TCP gossip network, and carries the native
header-sync, block-sync, discovery, and legacy-gossip-compatibility services. This
directory specifies the parts of the stack that are common to every service — how peers
**connect**, how they **find each other**, and how they learn **which services** each
other speak.

Three properties frame the whole design:

- **Bounded** — every queue, allocation, loop, and wait over attacker-influenced data has
  an explicit cap or deadline. A peer cannot make this node allocate, spin, or block
  without limit.
- **Authenticated and scoped** — a peer is its iroh node id (an Ed25519 public key). Every
  connection is scoped to a network and a chain (genesis hash), so mainnet, testnet, and
  private dev cohorts never cross-connect.
- **Direct-only** — iroh relays and side-channel discovery are disabled
  (`direct_endpoint_builder`); Zakura dials direct QUIC paths so that every peer has a real
  source IP to key per-IP admission on.

The three specs:

- **[connections.md](connections.md)** — the transport: connection lifecycle, the per-peer
  registry and duplicate resolution (the **generation guard**), framing, streams,
  timeouts, backpressure, rate limiting, and identity.
- **[peer_discovery.md](peer_discovery.md)** — how a node learns peer addresses (bootstrap,
  gossip, self-records), the signed-record discovery protocol, candidate selection,
  dialing, redial/backoff, and target connection counts.
- **[service_discovery.md](service_discovery.md)** — how connected peers negotiate
  capabilities, how a stream is tagged to a service, and how each service (block-sync,
  header-sync, discovery, legacy gossip) acquires and admits peers.

## Architecture

```text
                         ┌───────────────────────────────────────────┐
                         │            iroh QUIC endpoint               │
                         │   ALPN p2p-v2/1 · relays & discovery off     │
                         └───────────────────────────────────────────┘
   dial (outbound) ─────────────►│                    │◄───────── accept (inbound)
                                  ▼                    ▼
                         control handshake (hello / ack): version · path · role ·
                         network_id · chain_id · node id · nonces · capabilities · limits
                                  │
                                  ▼
                    ┌─────────────────────────────┐   register(conn_id, peer_id, ip, …)
                    │   ZakuraSupervisor registry  │──► per-peer entry keyed by node id,
                    │   dedup · per-IP cap · gen    │   guarded by monotonic conn generation
                    └─────────────────────────────┘
                                  │  fan out ordered / request streams by capability
             ┌────────────────────┼────────────────────┬────────────────────┐
             ▼                    ▼                    ▼                    ▼
        legacy gossip        discovery            header-sync           block-sync
        (kinds 2,3)          (kind 4)             (kind 5)              (kind 6)
             │                    │
             │                    └── learns peer addresses, ranks dial candidates ──┐
             │                                                                        ▼
             └────────────────────────────────────────────────► outbound dialer / redial
```

## Shared vocabulary

Three **independent namespaces** identify "a service"; the spec keeps them distinct:

1. **Stream kind** (`u16`) — tags one QUIC stream to the service that owns it. Carried in
   every stream's `StreamPrelude`. Routes bytes.
2. **Capability bit** (`u64` bitmask) — negotiated once in the control handshake; gates
   _which stream kinds may be opened_ on a connection. Authorises.
3. **Service id** (`ZakuraServiceId`, bounded ASCII, e.g. `zakura.block_sync.v1`) —
   advertised in signed discovery records; used only to decide _who to dial_. Never routes
   bytes.

A single **ALPN** (`P2P_V2_ALPN = b"p2p-v2/1"`) gates the whole connection — there is no
per-service ALPN in production.

### Stream kinds

| Kind | Constant | Mode | Service | Version | Capability bit |
| --- | --- | --- | --- | --- | --- |
| 0 | — (`control`) | — | control handshake (trace label only) | — | — |
| 1 | — (`request`) | — | request/response (trace label only) | — | — |
| 2 | `ZAKURA_STREAM_GOSSIP` | Ordered | legacy gossip | 1 | `ZAKURA_CAP_LEGACY_GOSSIP` |
| 3 | `ZAKURA_STREAM_LEGACY_REQUESTS` | RequestResponse | legacy inventory req/resp | 1 | `ZAKURA_CAP_LEGACY_GOSSIP` |
| 4 | `ZAKURA_STREAM_DISCOVERY` | Ordered | native discovery | 1 | `ZAKURA_CAP_DISCOVERY` |
| 5 | `ZAKURA_STREAM_HEADER_SYNC` | Ordered | native header sync | 5 | `ZAKURA_CAP_HEADER_SYNC` |
| 6 | `ZAKURA_STREAM_BLOCK_SYNC` | Ordered | native block sync | 2 | `ZAKURA_CAP_BLOCK_SYNC` |

Kinds 0 and 1 are trace/metric labels only, not registered streams. Kinds 2 and 3 share
one capability bit. Comments that say "stream-6" / "kind-6" always mean block-sync.

### Capability bits

| Constant | Value | Gates stream kind(s) |
| --- | --- | --- |
| `ZAKURA_CAP_LEGACY_GOSSIP` | `1 << 0` | 2, 3 |
| `ZAKURA_CAP_HEADER_SYNC` | `1 << 1` | 5 |
| `ZAKURA_CAP_DISCOVERY` | `1 << 2` | 4 |
| `ZAKURA_CAP_BLOCK_SYNC` | `1 << 3` | 6 |

A node advertises exactly the OR of the capability bits of the services it actually runs
(`registry.supported_capabilities()`); it cannot claim a capability whose service is not
registered.

### Network and chain scoping

Every handshake and every signed discovery record carries `network_id` (`ZakuraNetworkId`:
Mainnet=1, Testnet=2, Regtest=3, Configured=4) and `chain_id` (the network's genesis hash).
A mismatch is rejected (`WrongNetwork` / `WrongChain`). A private dev cohort
(`ZakuraConfig::dev_network` tag) advertises `Configured` and a `chain_id` derived from the
genesis and the tag, so cohorts and the public network reject each other **without changing
consensus** (same genesis, magic, and activation heights). See
[`private-zakura-network.md`](../../../book/src/dev/private-zakura-network.md).

### Wire magics

`P2P_V2_ALPN = b"p2p-v2/1"` · `PRELUDE_MAGIC = b"ZAKURA1\0"` (legacy-upgrade prelude) ·
`CONTROL_HELLO_MAGIC = b"ZAKCTRL\0"` · `CONTROL_ACK_MAGIC = b"ZAKACK\0\0"` ·
`STREAM_PRELUDE_MAGIC = b"ZKST"` · signed-record domain
`ZAKURA_NODE_RECORD_SIG_DOMAIN = b"zakura-node-record-v1"`.

All wire codecs are little-endian, hard-capped, and reject trailing bytes.

## Status

Pinned iroh version: `IROH_VERSION = "0.92.0"`. This spec reflects the transport as of the
generation-guard connection-registration work (Zakura WS-1) and the native per-IP
connection default (Zakura WS-4, default 3). Behavior gated behind future work streams
(e.g. WS-5 block-sync parking, see `docs/plans/zakura-ws5.md`) is out of scope and called
out where relevant.
