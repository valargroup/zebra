# Verified commitment trees — fast checkpoint sync

> Reconstruction note: the previous copy of this document was kept as an untracked
> working file and was lost when a shared worktree was cleaned. This version is
> rebuilt from the increment-6a plan, the increment roadmap, the startup-wiring
> work, and the serving-availability discussion. It is now tracked so it cannot be
> lost again. Sections that predate this rebuild should be reconciled against any
> older copy a contributor still holds.

## 1. Goal

Let a node sync the chain up to the last checkpoint without recomputing the Sapling
and Orchard note-commitment frontiers per block — the dominant CPU cost of checkpoint
sync (~70% of per-block commit time) — by consuming **per-block commitment roots** that
are verified against the node's own checkpoint-committed headers, and a **final frontier**
at the checkpoint so semantic verification resumes correctly afterwards.

This is one fast verified path with its data source factored out behind a seam, not a new
consensus mode: every supplied root is verified before commit, and a node that cannot
obtain roots falls back to the legacy recompute, bit-identical to today.

## 2. Design decisions

### 2.1 Per-block roots are the wire payload; the frontier is embedded

The fast path consumes two things: the per-block Sapling/Orchard roots, and the verified
note-commitment frontiers at the checkpoint handoff height.

- **Roots travel over the network.** `BlockCommitmentRoots { height, sapling_root,
  orchard_root }` lives in `zebra-chain` (`parallel/commitment_aux.rs`) so `zebra-network`
  and `zebra-state` share it without a dependency cycle, with `ZcashSerialize` /
  `ZcashDeserialize`.
- **The final frontier is embedded in the binary**, not on the wire
  (`zebra-state/src/service/finalized_state/vct/mainnet-frontier.bin`, via `include_bytes!`),
  refreshed per release like a checkpoint. So the `tree_aux` stream carries **only roots** —
  there is no `GetFinalFrontiers` / `FinalFrontiers` message and no frontier-serving path.

### 2.2 Header-sync alignment

Commitment roots are header-adjacent verified metadata, not body data: tiny, verified
against the header chain, servable only by a node holding the validated headers, and needed
buffered ahead of the committer. So `tree_aux` is a **separate Zakura stream** (its own
capability bit) **templated and timed on `header_sync`, not `block_sync`** — driven ahead of
body download. Because header sync runs ahead of block sync, a range's coverage is known
before it is committed, which keeps the legacy fallback sound by construction. The one
coupling to bodies: verifying a root via the ZIP-221 MMR leaf needs the block's tx-counts
(from the body), so roots are **consumed** at commit time with bodies even though they are
**fetched** early with headers.

### 2.3 Fetch window starts at the verified tip, not genesis

The driver fetches from `verified_tip + 1`, not genesis. Heights at or below the verified
tip are already committed and their roots are never looked up; a node that starts above
genesis (a snapshot) would otherwise spend the whole fetch on already-committed heights and
never cache the window roots before the committer reaches them, forcing every block back to
legacy recompute. (See §6 on the serving consequence of this.)

### 2.4 Peer source is the committer default

On networks with an embedded handoff frontier (Mainnet), the committer defaults to the peer
(`tree_aux`) source. Explicit `VCT_FAST` + `VCT_FIXTURE` (fixture replay) and `VCT_CAPTURE`
(legacy commit that records roots) take precedence; `VCT_LEGACY`, or a network with no
embedded frontier, yields a zero-overhead legacy committer. The precedence is resolved by a
pure `select_source_mode` so it is unit-testable without process env or the embedded files.

## 3. Architecture

### 3.1 The source seam

`CommitmentRootSource` (`zebra-state/.../commitment_aux.rs`) abstracts where the fast path's
roots and handoff frontier come from: `fast_root(height)`, `handoff_height()`,
`final_frontiers()`. The committer (`VctState.source`) reads through this one seam regardless
of source. Implementations: `FixtureSource` (file/embedded), `VecRootSource` (in-process
round-trip), and `PeerSource` (the fillable, transport-backed cache). Producers
`produce_block_roots(db, range)` / `produce_final_frontiers(db, height)` derive the same
payload from a database's per-height trees — the serving read path, minus the network.

### 3.2 The `tree_aux` Zakura stream

A separate request/response stream (kind 7, capability `1 << 4`, version 1), templated on
`header_sync`. Messages: `Status`, `GetRoots { start_height, count }`,
`Roots { roots }`, `RangeUnavailable { start_height, count }`. DoS bounds
`MAX_TA_ROOTS_PER_REQUEST` and `MAX_TA_MESSAGE_BYTES`. `TreeAuxService` serves inbound
`GetRoots` from local state via a `TreeAuxStatePort` (async, over `ReadRequest::BlockRoots`);
the client driver (`fetch_roots`) issues bounded `GetRoots` requests and advances by the last
height returned.

### 3.3 Driver + `PeerSource` (ahead of bodies)

The `tree_aux` driver fetches root ranges from a peer into a shared `PeerSource` cache that
the committer pulls per height via `fast_root`. `PeerSource::final_frontiers()` /
`handoff_height()` return the embedded frontier, so only roots come from the network. The
driver and committer share one cache: the committer's `PeerSource` publishes its
`PeerSourceWriter` through a process-global handle (`tree_aux_roots_writer`) that the driver
fills as ranges arrive.

### 3.4 Failure policy — safe by construction

Verify-before-commit already rejects a root that fails its header check. Because roots are
fetched and coverage-checked ahead of bodies, a range the peer cannot supply is known before
that range is committed, so the committer stays in legacy mode for that segment (real frontier
maintained) — no frozen-frontier splice. A bad, slow, or withholding peer degrades to legacy
speed, never wrong state, never a hard stall. Downscoring and re-request-from-another-peer are
the deferred adversarial refinement (§5, 6b).

## 4. Serving availability (open design concern)

As nodes fast-sync, fewer nodes can *serve* roots: `produce_block_roots` derives roots from
per-height note-commitment trees, and a fast-synced node deliberately never built those below
its handoff. Only archive/produced nodes can serve today. This is a value-at-scale concern,
not a safety one — a client that finds no serving peer degrades to legacy speed, it does not
stall.

Two mechanisms address it, in order of cost:

- **Roots-index CF (lightweight, preferred).** A fast node already verified every root it
  folded in. Persisting those into a compact column family (~68 bytes/block, ~200 MB for all
  of Mainnet) lets it serve them to others without per-height trees, at near-zero extra cost.
  A background task can backfill missing lower ranges by fetching **roots** (not bodies) from
  existing servers, so even a snapshot-started node becomes a full-range roots server cheaply.
  This is the targeted fix for the §2.3 / §4 availability gap.
- **Indexing-follower resync (heavyweight, opt-in).** Rebuild the per-height trees off the
  consensus critical path (re-downloading bodies if pruned), turning a fast node into a full
  archive node. This pays back the cost fast-sync avoided, so it is the archive/RPC path
  (increments 7–8), not a default.

Protocol hygiene that reduces the failure surface meanwhile: `Status` advertises each peer's
servable `[low, high]` range so clients only request from peers that can serve; plus
multi-peer fanout and re-request-from-another-peer (6b). Wiring `Status`-range advertisement is
the cheapest and most impactful of these.

## 5. Increment roadmap

- **Increments 0–5 (done):** the fast path proven end-to-end from a local source — the source
  seam, verify-before-commit against headers, frontier-recompute skip, and the verified
  checkpoint handoff with persistent fast-synced databases.
- **Increment 6a — Peer source: fetch + serve (happy-path POC).** The `tree_aux` stream
  (roots-only), the `TreeAuxStatePort` serving side, and the driver + `PeerSource`, plugged
  into the source seam. First point at which real nodes obtain roots over the network. Minimal
  POC first: prove a two-node fast-sync reaches byte-identical consensus state before
  hardening. Out of scope here, deferred as follow-ups:
  - **Header-sync-progress coupling.** The POC driver does a single bounded fetch of the
    `verified_tip + 1` → checkpoint range once a peer connects (retry on error). Driving the
    fetch incrementally off live header-sync progress is deferred; safety does not depend on
    it (an unarrived root stays legacy).
  - **Multi-peer fanout robustness** (parallelism, peer diversity, straggler hedging).
  - **Adversarial peer policy** (6b, below).
  - The fast-node roots-index serving CF (§4) and RLE wire encoding.
- **Increment 6b — Adversarial peer policy.** Wire `tree_aux` verification failures into
  Zakura's peer-reputation / reject machinery: downscore the offending peer and re-request the
  range from a different peer (bounded retries, peer diversity), with local recompute as the
  last-resort backstop. The security-critical increment, with its own hostile-peer test matrix
  (wrong roots / truncated range / mismatched MMR / withholding / eclipse → reject, re-request,
  recover).
- **Increment 7 — Indexing follower lane (archive only).** Stand up the split indexing follower
  lane: relocate `tx_by_loc` + address indexes and the per-height trees + subtree CFs onto an
  async follower, so archive mode regains historical RPC without re-adding the frontier
  recompute to the consensus path. The deliberate "refactor at the end," once every boundary is
  known.
- **Increment 8 — Archive mode via the follower.** Run the full per-block recompute off the
  critical path to restore `z_gettreestate` / `GetSubtreeRoots`, while the consensus lane uses
  verified roots.
- **Increment 9 — Spec / ZIP.** Publish the cross-client payload schema and verification
  algorithm so other clients (zcashd, zaino, …) can serve and verify identically.

## 6. Delivered by the startup wiring

The startup wiring (PR against `perf-note-commit-tree`) connects the increment-6a pieces in a
running node:

- async `TreeAuxStatePort` over `ReadRequest::BlockRoots`, threaded through
  `init_with_zakura_header_sync` → `spawn_zakura_endpoint_with_header_sync_driver` →
  `service_registry`, registering `TreeAuxService` under the Zakura sync path;
- the `PeerSource` shared between the driver and the committer via the process-global writer
  handle (`tree_aux_roots_writer`);
- `StateTreeAuxPort` (the serve adapter) and the driver spawned alongside the header-sync
  driver;
- **peer source as the committer default** on Mainnet, with the per-next-block precompute gate
  (`vct_fast_will_apply`) so legacy-fallback blocks keep their note-precompute overlap.

### Verification status

Unit and two-node transport equivalence are covered by tests. The real end-to-end two-node
`zebrad` fast-sync (a fresh node reaching byte-identical consensus state from a peer) is
manual-only: it crosses crate boundaries that cannot be wired into CI without a dependency
cycle.

## 7. Observability

Live commit-path counters distinguish the fast and legacy paths:

- `state.vct.fast.block.count` — block folded supplied roots, skipped the recompute.
- `state.vct.legacy.block.count` — block recomputed the frontier (VCT off, or roots
  unavailable for this height and it fell back).
- `state.vct.prevalidated.block.count` — the dedup sub-case where the previous fast block's
  look-ahead already validated this header.

The fast-vs-legacy ratio is the signal an integration test asserts to prove roots actually
came over the wire rather than a silent legacy sync.

## 8. Testing strategy

- **Unit:** wire-codec round-trip for every `TreeAuxMessage` variant + DoS-bound rejection;
  the `select_source_mode` precedence; the `StateTreeAuxPort` serve mapping (passthrough,
  error/wrong-response → empty range); the in-process producer → `PeerSource` → committer
  byte-identical equivalence.
- **Two-node equivalence (headline POC):** a server node serves roots over `tree_aux`; a fresh
  client with the peer source fast-syncs `verified_tip + 1` → checkpoint from the peer, hands
  off, and reaches a byte-identical consensus state (anchors + history root) to a legacy node.
  Assert `state.vct.fast.block.count > 0`. Recommended home: extend the Zakura regtest e2e
  (needs a Regtest embedded frontier fixture), with a Mainnet cached-state lane for real-data
  coverage.
- **Safe-fallback smoke:** with the server withholding a range, the client stays in legacy mode
  for that segment and still commits correct state (no halt, no wrong state).
