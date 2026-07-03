# Zakura P2P Peer Discovery — Specification

## Overview

Peer discovery is how a node learns **which peers exist and how to reach them**, and then
decides **which to dial**. A peer is its iroh node id (an Ed25519 public key); reaching it
means a `NodeAddr` = node id + a bounded set of direct socket addresses. Discovery keeps an
in-memory **address book** of signed peer records, fills it from four sources (operator
bootstrap, gossip responses, connected peers' self-records, and a persisted cache), and
runs a dialer that turns eligible book entries into connections up to a target count.

Discovery runs as its own ordered service (stream kind 4) on every connection: two peers
exchange self-records and peer samples over a long-lived stream. It balances **reach**
(keep enough diverse peers connected to sync and gossip), **safety** (never dial or gossip
an address that isn't provably a routable public host owned by a key that signed for it),
and **bounded cost** (every record, sample, dial, and retry is capped, and expensive
signature checks never run under the shared lock).

Discovery is **advisory to dialing, authoritative to nothing else**: it decides who is
_worth_ dialing; the transport ([connections.md](connections.md)) decides whether a dial is
_admitted_, and each service ([service_discovery.md](service_discovery.md)) decides whether
a connected peer is _useful_.

## Glossary

Plain term (code identifier):

- **Address book** (`ZakuraDiscoveryBook`, `ZakuraDiscoveryInner` behind
  `Arc<Mutex<…>>`) — the in-memory store of learned peers.
- **Signed record** (`ZakuraNodeRecord` = `ZakuraNodeRecordBody` + Ed25519 signature) — a
  peer's self-description: node id, direct addrs, advertised services, protocol range,
  `network_id`, `chain_id`, monotonic `sequence`, expiry.
- **Book entry** (`ZakuraDiscoveryEntry`) — a stored record plus dial bookkeeping:
  `source`, `last_seen`, `last_dial_attempt`, `last_success`,
  `last_short_lived_exchange`, `failure_count`.
- **Static candidate** (`ZakuraStaticDiscoveryCandidate`) — an operator bootstrap peer
  (trusted unsigned dial hint), stored separately and never gossiped.
- **Dial authority** (`entry_has_confirmed_dial_authority`) — a record is dialable only if
  it is a static candidate or self-authored (the peer signed for its own address).
- **Live service summary** (`ServiceSummaryEnvelope`) — a short-TTL advisory hint about a
  connected peer's per-service state, used only to rank dial/admission preference.
- **Dial slot limit** (`discovery_dial_slot_limit`) — how many new dials the candidate
  dialer may start right now.
- **Redial policy** (`RedialPolicy`) — the supervised reconnect/backoff for bootstrap and
  upgrade dials, distinct from the book's per-candidate dial backoff.

## Sourcing peers

**MUST — four sources, one book, strict separation:**

1. **Operator bootstrap** — `DEFAULT_ZAKURA_BOOTSTRAP_PEERS` (`node_id@ip:port` seeds on
   port 8234) parsed into **static candidates**. Static candidates MUST be stored apart from
   signed records and MUST NOT appear in any peer sample returned to another node.
2. **Gossip** — a `Peers { records }` response, imported by `import_peer_records`.
3. **Connected-peer self-records** — a `Hello { record }` on the discovery stream, imported
   by `import_connected_peer_record`.
4. **Persisted cache** — records loaded from disk are re-validated on import like any
   untrusted record (cache-persistence wiring is staged; the type carries no filesystem
   behavior yet).

- There are **no DNS seeds** in the native path (DNS seeding belongs to the legacy address
  book, not Zakura).
- A node's **own** record MUST NOT be stored in the book (`SelfRecord`).

**Record import validation (MUST).** `validate_record_body_for_import` MUST reject a record
whose `network_id` or `chain_id` does not match this node (`WrongNetwork` / `WrongChain`),
whose protocol range does not overlap (`IncompatibleProtocol`), whose expiry is outside
`[now - skew, now + max_record_ttl + skew]` (`Expired` / `FarFutureExpiry`), or whose
Ed25519 signature does not verify against its own `node_id` (`InvalidSignature`). The
signature domain is `zakura-node-record-v1`.

## The discovery protocol

The service runs one long-lived ordered stream (kind 4) per peer. Messages
(`DiscoveryMessage`) are LE, length-prefixed, one leading `u8` tag, trailing-byte-rejecting,
and hard-capped at `MAX_DISCOVERY_MESSAGE_BYTES` (16 KiB):

| Tag | Constant | Variant | Purpose |
| --- | --- | --- | --- |
| 1 | `MSG_DISCOVERY_HELLO` | `Hello { record }` | sender's signed self-record |
| 2 | `MSG_DISCOVERY_GET_PEERS` | `GetPeers { limit, wanted_services, exclude_node_ids }` | request a bounded peer sample |
| 3 | `MSG_DISCOVERY_PEERS` | `Peers { records }` | signed peer records (gossip response) |
| 4 | `MSG_DISCOVERY_GET_SERVICES` | `GetServices(…)` | request live service summaries |
| 5 | `MSG_DISCOVERY_SERVICES` | `Services(…)` | live per-service advisory summaries |

**Per-peer exchange (SHOULD).** On admission a peer SHOULD send `Hello` (self-record), then
`GetPeers` (excluding self, connected peers, and recently-known records, bounded by
`MAX_DISCOVERY_EXCLUDED_NODE_IDS` = 256), then `GetServices`, and settle within
`DISCOVERY_EXCHANGE_SETTLE_TIMEOUT` (2 s).

**Live summaries are advisory only (MUST).** A `Services` summary
(`HeaderSyncServiceSummary` / `BlockSyncServiceSummary` / `DiscoveryServiceSummary`, tagged
`SUMMARY_TAG_*_V1`) MUST be accepted only from the authenticated peer it describes, MUST be
TTL-clamped to `DEFAULT_LIVE_SERVICE_SUMMARY_TTL` (30 s), and MUST only influence dial /
admission _preference_ — never authorise a dial or bypass a validation.

## Dialing and candidate selection

Two dialer paths, both gated by transport admission capacity
(`endpoint.has_native_admission_capacity()`) and the per-IP cap:

- **Bootstrap dialer** — one supervised task per configured bootstrap peer, using
  `RedialPolicy::maintain` (redial forever). On a Zakura-only node this is the only healing
  path.
- **Candidate dialer** (`run_native_discovery_dialer`) — the loop that dials discovered
  peers. It wakes on endpoint shutdown, a worker finishing, a supervisor registration
  change, or every `ZAKURA_DISCOVERY_DIAL_INTERVAL` (1 s).

**MUST — bounded, deduplicated, per-IP-safe dialing:**

- The dialer MUST NOT start a dial unless the transport admission semaphore has a free
  permit and per-IP capacity remains, counting **in-flight** dials toward the per-IP cap
  (`can_accept_discovery_dial_ip` + `in_flight_by_ip`) so concurrent dials cannot overshoot
  `max_connections_per_ip`.
- A node id already **in-flight** MUST NOT be dialed again concurrently
  (`in_flight: HashSet<NodeId>`).
- The number of new dials MUST be bounded by `discovery_dial_slot_limit`:

  ```text
  soft_cap                   = max_zakura_connections − connection_headroom
  available_connection_slots = soft_cap − connected_count
  available_dial_slots       = max_concurrent_dials − in_flight_count
  limit                      = min(available_connection_slots, available_dial_slots)   // saturating
  ```

  If `limit == 0`, dialing stops. So the dialer targets a **soft cap** below the global
  connection ceiling, reserving `connection_headroom` slots (raised to
  `max(4, bootstrap_count)` in production so bootstrap dials are never starved by discovered
  peers), and never runs more than `max_concurrent_discovery_dials` (4) dials at once.

**Dial success is registration, not dial completion (MUST).** A dial "succeeds" only when
the peer appears in the supervisor's registration watch, not when the dial future returns.
Outcomes (`DiscoveryDialResult`): `Registered` (stayed registered ≥
`ZAKURA_REDIAL_HEALTHY_CONNECTION`, 60 s ⇒ `mark_dial_success`), `ShortLivedRegistered`
(registered then dropped early ⇒ `mark_short_lived_exchange`), `Failed`
(⇒ `mark_dial_failure`), `LocalResourceLimit` (our own cap ⇒ no penalty on the peer).

## Redial and backoff

Two distinct backoffs — do not conflate them:

**Supervised connection-level redial (`RedialPolicy`)** — for bootstrap and legacy-upgrade
dials.

- Exponential doubling capped at `max_backoff`: `backoff = (backoff × 2).min(max_backoff)`,
  from `DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF` (1 s) to `_MAX_BACKOFF` (30 s).
- A connection alive ≥ `ZAKURA_REDIAL_HEALTHY_CONNECTION` (60 s) counts as healthy and
  resets the backoff to initial, so a long-lived peer is not penalised on its next redial.
- **Anti-churn (MUST):** before each dial, a peer already in the supervisor registration set
  MUST be skipped (it may have dialed us first), so both directions do not churn duplicates.
  A `maintain` policy never self-exits; teardown MUST be driven by the endpoint shutdown
  token.

**Book-side per-candidate dial backoff** — decides candidate _eligibility_.

- `dial_backoff_secs(failure_count) = base × 2^min(failure_count−1, 10)`, capped at `max`,
  from `DEFAULT_DISCOVERY_DIAL_BACKOFF_BASE` (60 s) to `_MAX` (1 h); `failure_count == 0`
  ⇒ no backoff.
- A candidate is suppressed while `now < last_dial_attempt + backoff(failure_count)`
  (`entry_in_dial_backoff`), and for one base interval (60 s) after a short-lived exchange
  (`entry_in_short_lived_exchange_backoff`), so a peer that connects, does one exchange, and
  drops is not re-dialed immediately.

**No hard blacklist (MUST).** A dial failure only increments `failure_count`; a bad peer is
pruned by book eviction, never permanently banned. `HIGH_DIAL_FAILURE_COUNT` (3) marks a
candidate as a preferred eviction target.

## Target connection counts

Discovered-peer dialing targets `soft_cap = max_zakura_connections − connection_headroom`.
Production wires `max_zakura_connections` to the transport's `max_connections` (256) and
`connection_headroom` to `max(4, bootstrap_count)`. So a node keeps dialing discovered
peers until its connected count reaches the soft cap, then stops; separately,
per-direction admission into the discovery _service_ is capped by `ServicePeerLimits`
(`max_inbound_peers` / `max_outbound_peers`, default 256 each). Inbound and outbound are
counted and capped independently.

## Candidate lifecycle and scoring

**Lifecycle.** unknown → **stored** (`import_*`, monotonic `sequence` gate: `< stored`
ignored, `==` metadata-refresh with first-party source preferred over gossip, `>` replaced)
→ **dialable** (passes every eligibility gate below) → **dialing** (`mark_dial_attempt`)
→ **healthy** (`mark_dial_success`, resets `failure_count`) or **short-lived**
(60 s cooldown) or **failed** (`failure_count++`, exponential backoff) → **evicted**.

**Eligibility (MUST all hold).** A book entry is a dial candidate only if it is: not already
connected, not in-flight, not local, not expired, not in dial-backoff, not in
short-lived-exchange backoff, has **confirmed dial authority** (static or self-authored),
advertises a wanted service, and has at least one usable direct address.

**Scoring** (`dial_candidate_sort_key`, top-k selected in O(n) via `select_nth_unstable`,
not a full sort): prefer signed records over static, then most-recent `last_success`, then
fewest `failure_count`, then most-recently-seen, then a random tie-break (deterministic
node-id tie-break for static).

**Eviction** (`evict_to_limits`, when `discovered_len > max_records`, never evicts a static
candidate): expired records first (soonest expiry), then never-successful high-failure
records (`last_success.is_none() && failure_count ≥ 3`), then the least-recently
successful/seen.

## Edge cases and bounds

**A signed record proves key ownership, not address ownership.** An attacker can sign a
record pointing at a victim's address. Gossiped (untrusted) records MUST advertise only
**globally routable** addresses: `is_discovery_dialable_addr` rejects port 0, unspecified,
loopback, multicast, broadcast, link-local, RFC 1918 private, RFC 6598 CGNAT (100.64/10),
and RFC 4193 unique-local. Static/operator records are looser (loopback allowed for
regtest). Combined with "dial authority = static or self-authored," a peer can only get you
to dial _its own_ routable address.

**Expensive verification under a global lock is a DoS.** Ed25519 verification of imported
records MUST run **outside** the shared discovery mutex; only the cheap book mutation holds
the lock. The same rationale drives reservoir sampling in `sample_peers` and top-k
selection (not full sort) in `dial_candidates`, so a large record set or a flood of records
cannot pin the lock.

**A summary or self-record impersonates another peer.** A `Hello`, `Services`, or live
summary that does not match the authenticated peer it describes MUST be rejected
(`MismatchedConnectedPeerRecord` / `MismatchedNodeId`).

**A malicious legacy responder leaks maintained dials.** The legacy→Zakura upgrade dials a
peer-supplied node address under `RedialPolicy::maintain`. If the peer never registers
within the wait window and did not register from another connection, the dial MUST be
cancelled and its entry dropped, so repeated failed upgrades with distinct node ids cannot
leak unbounded maintained dials.

**A pure-discovery peer holds a connection open.** If the discovery exchange completed and
no other service owns the peer, the connection SHOULD be dropped after the exchange (and the
peer put in the 60 s short-lived cooldown), rather than held idle.

**Self-record sequence monotonicity is best-effort.** The self-record `sequence` is seeded
from a wall-clock nanosecond value; a backward clock step can regress it until cache
persistence lands. Peers apply the monotonic `sequence` gate on import, so a regressed
sequence is ignored rather than accepted as fresh.

**Bounded everything (MUST).** Per record: `MAX_DIRECT_ADDRS_PER_RECORD` (8),
`MAX_SERVICES_PER_RECORD` (32), `MAX_NODE_RECORD_BODY_BYTES` (16 KiB). Per response:
`MAX_DISCOVERY_RECORDS_PER_RESPONSE` (32), `MAX_SERVICE_SUMMARIES_PER_RESPONSE` (32),
`MAX_SERVICE_SUMMARY_BYTES` (1 KiB). Book: `max_records` (`DEFAULT_MAX_DISCOVERY_BOOK_RECORDS`,
10 000). Sample exclusions: `MAX_DISCOVERY_EXCLUDED_NODE_IDS` (256).

**Numeric safety (MUST).** Heights, sequences, and expiries decoded from records MUST be
range-checked (`InvalidHeight`, `NumericOverflow`); backoff shifts are capped (shift ≤ 10)
so the doubling cannot overflow.

**Observability (SHOULD).** The dialer SHOULD emit
`zakura.p2p.discovery.dial.{started,succeeded,failed,short_lived_registered,local_resource_limit,worker_failed}`
so a trace can distinguish a healthy dial mix from a failing or churning one.

## Defaults

| Knob | Default | Meaning |
| --- | --- | --- |
| `DEFAULT_ZAKURA_BOOTSTRAP_PEERS` | 9 mainnet seeds (`node_id@ip:8234`) | operator static candidates |
| `record_ttl` (`DEFAULT_DISCOVERY_RECORD_TTL`) | 24 h | self-record advertised lifetime |
| `refresh_interval` (`DEFAULT_DISCOVERY_REFRESH_INTERVAL`) | 10 min | self-record re-publish cadence |
| `peer_sample_limit` (`DEFAULT_DISCOVERY_PEER_SAMPLE_LIMIT`) | 32 | max records returned per `GetPeers` |
| `dial_backoff_base` / `_max` | 60 s / 1 h | book per-candidate dial backoff |
| `max_concurrent_discovery_dials` | 4 | concurrent discovered-peer dials |
| `connection_headroom` (`DEFAULT_DISCOVERY_CONNECTION_HEADROOM`) | 4 (prod `max(4, bootstrap)`) | slots reserved below the soft cap |
| `max_zakura_connections` | 256 (prod override of the 32 default) | soft-cap basis for dialing |
| `max_record_ttl` | 24 h | reject records expiring beyond this |
| `clock_skew_tolerance` | 5 min | expiry tolerance on import |
| `peer_limits` (`ServicePeerLimits`) | in = out = 256 | discovery-service per-direction admission |
| `ZAKURA_DISCOVERY_DIAL_INTERVAL` | 1 s | candidate-dialer wake cadence |
| `ZAKURA_REDIAL_HEALTHY_CONNECTION` | 60 s | uptime that resets redial backoff |
| `DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF` / `_MAX_BACKOFF` | 1 s / 30 s | supervised dial backoff |
| `DISCOVERY_EXCHANGE_SETTLE_TIMEOUT` | 2 s | per-peer exchange settle wait |
| `DEFAULT_LIVE_SERVICE_SUMMARY_TTL` | 30 s | live summary freshness bound |
| `HIGH_DIAL_FAILURE_COUNT` | 3 | failure count that flags eviction |
| `MAX_DISCOVERY_MESSAGE_BYTES` | 16 KiB | discovery frame hard cap |
| `max_records` (`DEFAULT_MAX_DISCOVERY_BOOK_RECORDS`) | 10 000 | address-book size cap |
