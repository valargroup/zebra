# Verified commitment trees — fast checkpoint sync

> **Status & default decision.** The fast verified path is the **default** whenever a node
> syncs under checkpoint trust (`consensus.checkpoint_sync = true`) on a network with an
> embedded handoff frontier (Mainnet) — for both the Archive and Pruned storage modes. This
> default-on posture is an **explicit, deliberate decision**, not an experimental default that
> slipped in: it is justified by the verify-before-commit safety contract (§6, §11), the
> fail-closed-on-frozen-frontier policy (§8), the byte-identical-to-legacy equivalence proven by
> automated tests (§14), and the adversarial peer policy (§11, §12 increment 6b) being in place.
> The committer never lets an unverified or unobtainable root influence consensus state, so a
> bad/missing root degrades to a bounded refetch/refusal rather than wrong state.
>
> The escape hatch is first-class, not a workaround: `consensus.disable_vct_fast_sync = true`
> keeps checkpoint sync enabled while fully reconstructing the note-commitment trees per block
> (the byte-identical legacy committer), so any operator can opt out without giving up checkpoint
> sync. See §4.4 for the mode matrix. (The implementation remains a recent addition; treat the
> kill switch as the supported rollback if a node ever needs the legacy committer.)
>
> **Document history.** An earlier copy of this design was kept as an untracked working
> file and was lost when a shared worktree was cleaned. This version is rebuilt from the
> PR #189 commit history and *reconciled against the merged code* — the section numbers
> here (§5.1, §5.2, §5.4, §6.1, §9, §11, …) are the ones the source comments cite, so a
> `design §N` reference in the code resolves to the section of the same number below.

## Overview (start here)

**What it is.** Below the last checkpoint, Zebra normally rebuilds the Sapling and Orchard
note-commitment trees for every block just to learn each block's treestate root — the single
biggest CPU cost of checkpoint sync. Verified commitment trees (VCT) instead **fetch the
per-block roots from peers**, **verify each one against the headers the node already trusts**,
fold them straight into the anchor set and history tree, and **skip the rebuild**. At the
checkpoint handoff an **embedded final frontier** (verified against that block's proven root) is
written so normal per-block verification resumes above the checkpoint. Result: same consensus
state as the legacy committer, far less work — and no new cryptography.

**The one invariant that makes it safe:** *no root influences consensus state until it has been
authenticated against a header commitment.* Everything else (the transport, the cache, the peer
policy) is plumbing around that invariant. A root that cannot be obtained or verified is refused,
never guessed — inside the post-fold "frozen" window the committer **fails closed** rather than
recomputing against a now-stale frontier (§8).

**Data flow (fetch + commit path):**

```text
header sync (runs ahead of bodies)
   │ validated headers
   ▼
tree_aux driver (zebrad) ──GetRoots──▶ peers ──Roots──▶ verify batch shape; hedge slow peers;
   │                                                     demote soft-failers / exclude liars (§8.1)
   │ stage whole window, publish only on full success, bounded ahead of commit (§4.2–4.3)
   ▼
PeerSource cache (zebra-state)  ◀──invalidate / evict-committed── finalized committer
   │ fast_root(height)
   ▼
finalized committer: verify-before-commit (§6) ──fold roots, skip recompute──▶ DB
   │ at the handoff height: verify + write the embedded final frontier ──▶ resume legacy recompute
```

**Serving path (how a node answers other nodes' fetches):**

```text
peer GetRoots ─▶ TreeAuxService (zebra-network) ─▶ StateTreeAuxPort (zebrad)
   ─▶ ReadRequest::BlockRoots ─▶ commitment_roots_by_height index (fast nodes) or per-height trees (archive)
```

**Lifecycle of one fast sync.** (1) Node starts under `consensus.checkpoint_sync = true` on
Mainnet → the committer is built in peer mode (§4.4). (2) The driver fetches roots for
`[verified_tip+1, handoff]` in bounded windows, ahead of the committer (§4.3). (3) Each
checkpoint block: look up its root; verify it (own header now, successor header next block, plus
the direct below-Heartwood/below-NU5 checks); fold it in; freeze the frontier (§6, §7). (4) At
the handoff height, verify and write the embedded frontier and unfreeze. (5) Above the handoff,
ordinary semantic verification resumes from the real frontier. A bad/missing root anywhere in
the frozen window parks the block and refetches; it never writes wrong state.

**Glossary.**

| Term | Meaning |
| --- | --- |
| **Checkpoint sync** | `consensus.checkpoint_sync = true`: trust the embedded checkpoint list for headers/PoW up to the max checkpoint. Precondition for VCT. |
| **Handoff height** | The network's max checkpoint height; the boundary where the fast path ends and the embedded final frontier is written. |
| **Fast root** | A peer-supplied `(sapling_root, orchard_root)` for one height, folded in after verification instead of being recomputed. |
| **Final frontier** | The real Sapling/Orchard/Sprout note-commitment trees at the handoff height, embedded in the binary (§5.2) and written as the tip treestate at handoff. |
| **Frozen frontier** | The window `tip < handoff` during a fast sync where the on-disk frontier is intentionally stale (roots folded, trees not advanced). Legacy recompute here would corrupt state, so the committer fails closed (§8). |
| **Verify-before-commit** | Authenticating each root against the node's header commitments (ZIP-221 MMR one-block-lag + direct sub-Heartwood/sub-NU5 checks) before it affects state (§6). |
| **Fail closed** | In the frozen window, refuse the commit (retryable) rather than recompute or guess (§8). |
| **Provenance / cooldown / demotion** | Driver-side peer policy (§8.1): which peer supplied each root, hard-failure cooldown + escalating disconnect for liars, soft-failure back-of-rotation demotion for slow/withholding peers. |
| **Hedge** | Tied request strategy (§5.4): query one peer, add another only if it is slow, to bound stalls without tripling load. |
| **Kill switch** | `consensus.disable_vct_fast_sync = true`: keep checkpoint sync but force the legacy committer (§4.4). |

For where each piece lives in the tree, see the file map (§15).

## 1. Goal

Let a node sync the chain up to the last checkpoint **without recomputing the Sapling and
Orchard note-commitment frontiers per block** — the dominant CPU cost of checkpoint sync
(the per-block `update_trees_parallel` recompute, ~70% of per-block commit time).

Instead of rebuilding the trees, the committer consumes:

1. **per-block commitment roots** (the Sapling and Orchard treestate roots as of the end of
   each block), each **verified against the node's own checkpoint-committed block headers**
   before it is allowed to influence consensus state; and
2. a **final note-commitment frontier** at the checkpoint handoff height, so post-checkpoint
   semantic verification resumes from a correct frontier.

This is **one fast verified path with its data source factored out behind a seam**, not a
new consensus mode. Every supplied root is verified before commit; a node that cannot obtain
or verify a root falls back to the legacy recompute, bit-identical to today.

## 2. Scope and non-goals

- **In scope:** the consensus-critical commit path (verify-before-commit, the frozen-frontier
  failure policy, the checkpoint handoff), the `tree_aux` peer transport that delivers roots,
  the serving read path, and the persistent fast-synced database format.
- **Not a consensus change.** There are exactly two enduring code paths: the standard local
  tree rebuild (legacy) and the fast verified path. Which one runs is config-driven by
  `consensus.checkpoint_sync` plus the rollout force-disable knob
  (`consensus.disable_vct_fast_sync`; §4.4); the `state.storage_mode` axis (Archive vs. Pruned)
  is orthogonal — it controls raw-tx/index pruning, not the tree path, so both storage modes
  use the fast path under checkpoint sync unless force-disabled. The network `PeerSource` and
  crate-local test fixtures are *sources* behind one seam (§5.3) — not modes.
- **No new cryptography.** Verification reuses the existing consensus checks
  (`block_commitment_is_valid_for_chain_history`, `HistoryTree::push`); see §6.
- **Out of scope for the fast lane:** historical tree/subtree RPCs (`z_gettreestate`,
  `GetSubtreeRoots`) below the handoff. A fast-synced node deliberately never built the
  per-height trees those need; they return a typed archive-mode error below the handoff and
  are restored only by the archive follower (§12, increments 7–8).

## 3. Background: the cost being eliminated

On checkpoint sync, header and PoW validity are already attested by the checkpoint list, so
the committer's remaining per-block work is dominated by advancing the Sapling and Orchard
note-commitment trees (`update_trees_parallel`) to recompute each block's treestate root.
The roots themselves are small and, from Heartwood onward, are **already committed to by the
block headers** via the ZIP-221 ChainHistory MMR: a block's header commitment binds the
history tree as of its parent, and each history-tree leaf is built from the block body plus
that block's Sapling/Orchard roots.

That is the lever: if a node is *handed* the per-block roots, it can fold them straight into
the anchor set and history MMR and **confirm them against the headers it already trusts**,
skipping the frontier recompute entirely — without weakening any consensus check.

## 4. Design decisions

### 4.1 Roots travel on the wire; the frontier is embedded

The fast path needs two things, and they are sourced differently:

- **Per-block roots travel over the network.** `BlockCommitmentRoots { height, sapling_root,
  orchard_root }` (§5.1) is the only `tree_aux` wire payload.
- **The final frontier is embedded in the binary** (§5.2), refreshed per release like a
  checkpoint, *not* sent on the wire. So `tree_aux` is a **roots-only** stream: there is no
  `GetFinalFrontiers`/`FinalFrontiers` message and no frontier-serving path to attack or
  keep available.

### 4.2 Header-sync alignment

Commitment roots are header-adjacent verified metadata, not body data: tiny, verified against
the header chain, servable only by a node holding the validated headers, and needed *buffered
ahead of* the committer. So `tree_aux` is a **separate Zakura stream** (its own capability
bit) **templated and timed on `header_sync`, not `block_sync`** — driven ahead of body
download. The driver stages fetched batches and only publishes them to the committer cache after
the whole verified-tip-to-handoff range succeeds, so a range's coverage is known before any of
its roots can trigger the fast path.

The one coupling to bodies: verifying a root via the ZIP-221 MMR leaf needs the block's
tx-counts (from the body), so roots are **consumed** at commit time with bodies even though
they are **fetched** early with headers.

### 4.3 The fetch window starts at the verified tip, not genesis

The driver fetches from `verified_tip + 1` (`max(finalized_tip, best_tip) + 1`), not genesis.
The committer only ever looks up a fast root for a block it is about to commit — the range
`[verified_tip + 1, checkpoint]`. Heights at or below the verified tip are already committed
and their roots are never queried. A node that starts above genesis (a snapshot) would
otherwise spend the whole fetch streaming already-committed roots and never cache the window
the committer needs before it reaches it, forcing every block back to legacy recompute.
A genesis-empty node still yields `Height(1)`, so "fetch from genesis" falls out exactly when
the node really is at genesis. (Implemented in `root_fetch_start`; regression covered by
`root_fetch_start_is_one_above_the_verified_tip`.)

The driver also bounds how far peer delivery may run ahead of finalized commits. It fetches
only up to `committed_through + TREE_AUX_FETCH_AHEAD_ROOTS`, where `committed_through` is the
peer-source eviction watermark advanced after successful database writes. If the next fetch
cursor is beyond that cap, the driver waits for commit progress while still servicing targeted
root refetch requests. This keeps the live `PeerSource` cache bounded by the fetch-ahead
window plus transient retry/refetch data, rather than by the whole checkpoint range.

### 4.4 Mode selection: fast under checkpoint sync

The fast-vs-legacy choice is driven by user-facing config, not by env vars. The axes are
`consensus.checkpoint_sync` (full checkpoint trust), `consensus.disable_vct_fast_sync` (initial
rollout force-disable for VCT fast sync), and `state.storage_mode` (Archive vs. Pruned, an
orthogonal pruning axis). The resulting modes:

| Mode | Config | Tree behavior |
| --- | --- | --- |
| **Archive** (default) | `consensus.checkpoint_sync = true`, `consensus.disable_vct_fast_sync = false`, `storage_mode = archive` | Fast — verified roots folded in, recompute skipped. Unpruned (raw tx + indexes kept). No per-height tree history below the handoff *for now* (§7, §10). |
| **Pruning** | `consensus.checkpoint_sync = true`, `consensus.disable_vct_fast_sync = false`, `storage_mode.pruned` | Fast — same as Archive, **plus** raw-tx/index pruning outside the retention window. |
| **Force-disabled VCT** | `consensus.checkpoint_sync = true`, `consensus.disable_vct_fast_sync = true` (any storage mode) | Legacy — keeps checkpoint sync enabled but fully reconstructs the Sapling/Orchard trees per block. |
| **Checkpoint sync disabled** | `consensus.checkpoint_sync = false` (any storage mode) | Legacy — fully reconstructs the Sapling/Orchard trees per block, using only mandatory checkpoints. |

Gating fast on `checkpoint_sync` is also a correctness precondition: the embedded handoff
frontier is pinned to the network's **full** max checkpoint height (§5.2), which only applies
when `checkpoint_sync = true` (with it `false`, the effective max checkpoint drops to the
Canopy mandatory checkpoint, so there is no valid handoff to resume from). zebrad mirrors
`consensus.checkpoint_sync` into the state config at startup
(`state_config.checkpoint_sync`), so the state makes the decision without depending on
`zebra-consensus`.

Precedence is resolved by a pure, unit-tested `select_source_mode` (no process env, no embedded
files in the decision — `consensus.checkpoint_sync`, `consensus.disable_vct_fast_sync`, and the
embedded-frontier presence are passed in as plain inputs):

1. `consensus.checkpoint_sync = false`, `consensus.disable_vct_fast_sync = true`, or a network
   with **no embedded frontier** → **legacy** (no VCT state, zero overhead);
2. else → **peer** (the default under checkpoint sync where embedded frontiers exist).

The earlier file-backed checkpoint/fixture root source (`VCT_FAST`/`VCT_FIXTURE`) and capture
mode (`VCT_CAPTURE`) were transient integration scaffolding before peer delivery existed and
have been removed. `VCT_REGTEST_FRONTIER` remains as a Regtest final-frontier test hook.
`consensus.disable_vct_fast_sync = true` is the supported user-facing way to force the legacy
committer without disabling checkpoint sync (the deliberate opt-out for the default-on path; see
the status note at the top of this document).

## 5. Payload, wire, and the source seam

### 5.1 Per-block commitment roots (the wire payload)

`zebra_chain::parallel::commitment_aux::BlockCommitmentRoots` holds `{ height, sapling_root,
orchard_root }` with `ZcashSerialize`/`ZcashDeserialize`. It lives in `zebra-chain` so
`zebra-network` and `zebra-state` share one type without a dependency cycle. `orchard_root` is
the empty/default root below NU5. The deserializer treats `height` as an unvalidated `u32`: a
wrong or out-of-range height simply fails to match any local header during verification (§6),
so it is harmless; malformed root bytes are rejected by the root parsers.

The payload carries **no trust**: a recipient re-verifies every root against its own
checkpoint-committed headers (§6) before folding it in, so a forwarding/serving node is
exactly as trustworthy as an originating one.

### 5.2 The final frontier handoff (embedded)

Fast mode never advances the running Sapling/Orchard frontiers below the checkpoint, so the
real frontiers at the checkpoint must be supplied for the resume. `FinalFrontiers { height,
sapling, orchard, sprout }` is embedded in the binary
(`zebra-state/src/service/finalized_state/vct/mainnet-frontier.bin`, via `include_bytes!`),
tied to the network's max checkpoint height (validated on load:
`embedded VCT final frontier height must match the network's max checkpoint height`). When the
Mainnet checkpoint list advances, this file is regenerated alongside the checkpoint artifacts
by the maintenance tool described in §16.

- **Sprout** is frozen far below any modern checkpoint, so the tip Sprout tree is its frontier.
- **Subtree tips are not carried**: the resuming chain recomputes them from the frontier
  position.
- **Regtest** has no fixed checkpoint (its list is derived at runtime), so there is no constant
  to embed; for deterministic e2e testing the frontier is loaded from the file named by
  `VCT_REGTEST_FRONTIER` and validated against the Regtest checkpoint height. This is scoped to
  Regtest only — Mainnet always uses the embedded constant and never reads the env.

### 5.3 The `CommitmentRootSource` seam

`CommitmentRootSource` (`zebra-state/.../finalized_state/commitment_aux.rs`) abstracts *where*
the fast path's roots and handoff frontier come from. The committer (`VctState.source`) reads
through this one seam regardless of source:

```rust
fn fast_root(&self, height) -> Option<(sapling::Root, orchard::Root)>;
fn handoff_height(&self) -> Option<block::Height>;
fn final_frontiers(&self) -> Option<&FinalFrontiers>;
fn invalidate(&self, height);   // drop a rejected root so a re-fetch can replace it
```

Implementations:

- `PeerSource` — a fillable, transport-backed cache (the production default). Its
  `PeerSourceWriter` is filled by the `tree_aux` driver only after the initial requested range
  has been fully fetched; the committer reads it per height. The handoff frontier is held
  immutably from the embedded constant, so only roots come from the network. `invalidate` evicts
  a rejected root from the cache so the next read misses and a re-fetch from another peer can
  replace it (the key to not letting one malicious peer wedge a bad root in place — §8, §11).
- `FixtureSource` — a crate-local `#[cfg(test)]` source over the same height→roots map, used only
  to isolate committer behavior and DB-produced payload round trips without networking.

The **producer** half (`produce_block_roots(db, range)` / `produce_final_frontiers(db,
height)`) derives the same payload from a database's per-height trees — the serving read path
(§9), minus the network. The producer→`PeerSource`→committer round-trip proving producer and
consumer agree is `vct_db_produced_payload_round_trips`.

Peer mode creates a per-state `TreeAuxRootsWriter` alongside the committer's `PeerSource`.
`zebra_state::init` returns that handle to `zebrad`, which passes it to the `tree_aux` driver.
The driver stages each bounded fetch window locally and publishes it atomically after that
window succeeds; the same handle also exposes the committed-root eviction watermark and carries
targeted refetch subscriptions, so each state instance pairs its committer, root cache, and
driver without process-global state. The state cache deliberately stores no peer identity:
root provenance and peer-exclusion policy live in the `zebrad` driver (§8.1), preserving the
`zebra-state` / `zebra-network` crate boundary.

### 5.4 The `tree_aux` Zakura stream

A separate request/response stream:

| Property | Value |
| --- | --- |
| Stream kind | 7 (`ZAKURA_STREAM_TREE_AUX`) |
| Capability bit | `1 << 4` (`ZAKURA_CAP_TREE_AUX`) |
| Version | 1 |
| Mode | `RequestResponse` (one-shot; no ordered-stream reactor or scheduler) |
| Templated on | `header_sync` |

Messages (`TreeAuxMessage`): `Status { servable_low, servable_high }`, `GetRoots {
start_height, count }`, `Roots { roots }`, `RangeUnavailable { start_height, count }`.

DoS bounds: `MAX_TA_ROOTS_PER_REQUEST = 4000` and `MAX_TA_MESSAGE_BYTES = 1 MiB`, enforced on
both encode and decode; the decoder also rejects unknown message types, unsupported frame
flags, and trailing bytes, and **never preallocates from the untrusted count** (it grows the
vec as roots are read). Client-side, the `tree_aux` driver also caps speculative fetch-ahead
to a fixed number of these request batches beyond the committer's eviction watermark, so cache
memory is bounded even when peers serve roots faster than blocks commit to disk.

- **Server** (`TreeAuxService`, a `RequestResponseService`): decodes a `GetRoots`, reads roots
  from local state through `TreeAuxStatePort` (§9), and returns `Roots` — or `RangeUnavailable`
  when it holds nothing. The response is additionally bounded by the negotiated frame/message
  caps (`effective_response_payload_bytes`), so it never overruns a smaller peer cap.
- **Client** (`fetch_roots` / `fetch_roots_with_peer`): issues bounded `GetRoots` requests and
  advances by the last height each peer returns. For each sub-range it uses a **tied (deferred)
  hedge**: it sends to one preferred peer and launches another only if the previous one has not
  answered within `TREE_AUX_HEDGE_DELAY` (currently 2s), up to `TREE_AUX_HEDGE_PEERS` (3) in
  flight; it accepts the first valid response and drops (cancels) the losing requests. A fast
  failure advances to the next peer immediately rather than waiting out the delay. So the happy
  path costs a single request — hedging only spends extra requests against a genuinely slow or
  withholding peer — while a slow peer no longer imposes a full per-peer timeout before an honest
  hedged peer can answer. If all peers in the bounded hedge fail, the sub-range returns an error
  and the caller retries later with rotation/demotion state instead of walking the whole peer set
  in one attempt. Short contiguous responses are
  accepted only when they meet a minimum-progress threshold (currently at least one quarter of
  the requested count, rounded up), so a peer cannot turn a 4000-root request into thousands of
  one-root round trips. Any unavailable/malformed/out-of-range/low-progress sub-range returns
  an error; the caller treats that range as un-fetched and leaves it on the legacy path. The
  provenance-preserving helper returns the supplier `ZakuraPeerId` for each accepted batch,
  accepts a peer-selection policy, and reports per-peer request outcomes so the `zebrad` policy
  can avoid hard-failed peers while only demoting soft-failed peers.

The outbound path in `zebra-network` is stream-kind-aware: a `tree_aux` `GetRoots` is read with
the generic stream frame budget rather than being validated as a legacy request message (which
previously rejected it as an "unsupported legacy request message type").

## 6. Verification — verify-before-commit

Before a supplied root influences consensus state, the committer confirms it against the
node's own checkpoint-committed headers. The logic lives in
`finalized_state/commitment_aux_verify.rs` and reuses the existing consensus check
`block_commitment_is_valid_for_chain_history` plus `HistoryTree::push` — **no new crypto**.

A block's header commitment binds the history tree *as of its parent*, so the root supplied
for height `H` is folded into a candidate history tree and confirmed when `H+1`'s commitment
is checked against that candidate. A wrong root makes that check fail and the block is
**rejected, not recomputed** (§8). The standalone `verify_commitment_roots` returns the first
offending height; over `[start..=end]` it confirms `[start..=end-1]`, and `end+1` confirms
`end`.

### 6.1 Direct header checks below Heartwood and NU5

The ZIP-221 MMR does not authenticate everything, so two gaps are closed by direct comparison
(no one-block lag — a wrong root is rejected at the block's own commit):

- **Sapling below Heartwood** (`verify_supplied_sapling_root_below_heartwood`): there is no MMR
  yet, so the header's `FinalSaplingRoot` is compared directly; pre-Sapling the root must be
  the empty-tree root. At/above Heartwood the MMR path authenticates it.
- **Orchard below NU5** (`verify_supplied_orchard_root_below_nu5`): the V1 history leaf
  (Heartwood..Canopy) *ignores* the Orchard root and there is no MMR below Heartwood, so no
  header commits to an Orchard root below NU5 — yet the fast path folds the supplied Orchard
  root into the anchor set for every block. The Orchard tree is provably empty there (no
  Orchard actions are allowed), so the supplied root is pinned to the empty-tree root. Without
  this, an untrusted source could inject an Orchard anchor the legacy recompute never produces,
  breaking the §11 trust boundary and consensus equivalence. This was a real hole, masked only
  while the source was a trusted fixture; the in-flight peer source would have armed it
  (fix in commit #190).

### 6.2 The one-block lag and the dedup

A block's own commitment check `C(X, T_{X-1})` is the *identical* computation the previous
fast block already ran as its look-ahead one commit earlier. The committer caches the
look-ahead result as `(next_height, next_hash)` and skips a block's own check when the prior
look-ahead validated exactly it. The guard is hash identity and heights are monotonic, so a
stale or cloned cache entry can never cause a false skip. Steady state drops from two
commitment checks per block to one (legacy parity) while still attesting every root before it
is persisted. A non-handoff fast block with no buffered successor is deferred by the write
worker until the successor arrives; the checkpoint handoff is the only no-successor fast commit
because the embedded final frontier independently authenticates that height's roots. The cache
is cleared on handoff and on legacy blocks. The dedup is observable
(`state.vct.prevalidated.block.count`) so it cannot silently regress.

### 6.3 The auth-data-root cache lock

The NU5+ commitment check trusts a precomputed `AuthDataRoot` carried on
`CheckpointVerifiedBlock` (so the single-threaded committer does not recompute it). Every
cached value is computed from the block by the constructors, so it is correct *by
construction* — but the public API previously let it be desynced after construction
(`pub auth_data_root`, `DerefMut`, both re-exported). A holder could swap the block while
keeping a stale root, and a header matching the stale root would finalize a block without
proving the header binds the block's actual authorizing data. The (block, auth-data-root) pair
is now locked together: `auth_data_root` is `pub(crate)`, `CheckpointVerifiedBlock` drops
`DerefMut`, the one legitimately-post-set field goes through
`set_deferred_pool_balance_change`, and the semantic verifier builds blocks through
`from_semantic_data` (auth-data root left unset). Compile-time enforced (fix in commit #192).

## 7. The fast commit path and checkpoint handoff

The commit-path hook lives in `finalized_state.rs`; everything about *where data comes from*
lives in the `vct` and `commitment_aux` submodules, so the commit path holds only the handoff
logic. For a checkpoint-verified block at `height`:

1. **Fast-root lookup.** `vct.fast_root(height)` returns the supplied roots, or `None`.
2. **If supplied (fast path):**
   - run the own-commitment check unless the dedup (§6.2) already validated it;
   - apply the direct below-Heartwood/below-NU5 checks (§6.1);
   - build a candidate history tree with the roots folded in (`HistoryTree::push`);
   - **verify-before-commit:** either check the buffered successor's commitment against the
     candidate (the one-block-lag confirmation) and cache `(height+1, next_hash)` as
     pre-validated, or, at the checkpoint handoff only, verify the embedded final frontiers
     against this height's roots; a failure means *this* height's root is bad → reject and
     evict (§8);
   - fold the roots into the anchor set, skip the frontier recompute, and **freeze** the
     note-commitment frontier (`vct_frontier_frozen = true`) for non-handoff fast blocks.
3. **Checkpoint handoff** (when `height` is the handoff height): verify the embedded frontier
   against this block's verified root (`frontier.root() == verified root`; collision resistance
   makes the root a binding commitment to the frontier), write it as the real tip treestate via
   the normal write path, and **unfreeze** — heights at/above the handoff resume legacy
   recompute from a correct frontier.
4. **If not supplied:** §8.

The write worker enforces the successor side of this contract before calling the committer: if
a queued checkpoint block would take the fast path, is not the handoff height, and has no
buffered successor yet, it is parked locally and retried when another checkpoint block arrives.
It is not reported through the invalid-block reset path, because no verification failure has
occurred — the needed `H+1` witness is merely not buffered yet.

**Persistent fast-synced databases.** A persistent fast sync marks the database with a
`fast_sync_metadata` column family recording the handoff height (DB format minor bump to
**27.3.0**, consolidated with the roots serving index and history-tree repair). This is a sibling
to `pruning_metadata`, not a reuse — pruning drops tx bytes and keeps trees, fast-sync drops the
per-height trees; a DB can be both. Because fast sync deletes nothing, a **completed** fast-synced
DB (tip at/above the handoff) **reopens in any storage mode** — a reopen loses no servable data,
and `consensus.disable_vct_fast_sync = true` or `consensus.checkpoint_sync = false` simply resumes
the legacy recompute from the real tip frontier.

The one reopen that *is* refused is an **interrupted** fast sync (frozen frontier, tip below the
handoff) reopened with the fast path disabled (legacy mode —
`consensus.disable_vct_fast_sync = true`, `consensus.checkpoint_sync = false`, or no embedded
frontier). The on-disk frontier is stale and no source can supply the verified roots, so the
fail-closed policy (§8) would refuse every below-handoff block forever. The open guard refuses
with a clear recovery path (finish the fast sync under `consensus.checkpoint_sync = true` and
`consensus.disable_vct_fast_sync = false`, or re-sync from genesis) instead of stalling silently.
Guards: per-height tree reads return `None` below the handoff (before the backward search, so no
stale tree and no panic); `z_gettreestate` returns a typed archive-mode error below the handoff;
genesis-root and subtree format-validity checks skip fast-synced DBs.

## 8. Failure policy — fail closed on a frozen frontier

While the frontier is frozen (a fast sync has folded roots but the handoff has not yet written
the real frontier), the on-disk frontier is **stale**. A legacy recompute in that window would
extend the stale frontier and fold a *wrong* root into the MMR — corrupting consensus state.
So the committer **fails closed** rather than falling back to recompute (commit #211):

- A supplied root that fails *any* verification step is **evicted** from its source (so a
  re-fetch from another peer can replace it) and the commit is **refused** with the typed,
  **retryable** `VctSuppliedRootUnavailable { height }` error — not retried against the same
  rejected root forever, and not recomputed locally.
- A frozen-frontier height with **no** valid supplied root (never fetched, or just evicted)
  refuses with the same retryable error and leaves the database untouched. The block commits
  once a verifiable root is fetched.
- A non-handoff fast block with a valid supplied root but **no buffered successor** is not a
  root failure: the write worker defers it locally until `H+1` is available to authenticate
  the candidate history tree. If a direct committer caller bypasses that deferral, the
  committer still fails closed before writing.
- The frozen flag is **seeded from the durable fast-sync marker on open**, not just tracked
  in-session: a fast sync interrupted by a restart (frozen frontier persisted, tip below the
  handoff) still refuses on the first post-restart height with a missing root. The frozen
  region is exactly `tip < handoff` (the handoff height itself carries the real frontier).

Outside the frozen window (legacy), a missing root is
simply the ordinary legacy recompute — bit-identical to today. Inside the frozen window, a
missing root parks the current checkpoint block, requests a targeted `tree_aux` refetch from
peers, and retries the same commit once the cache is refilled — **without resetting the block
queue**. A peer-supplied root that has no buffered successor to confirm it against the header
chain (the one-block lag) is likewise **deferred, not committed on faith**: an untrusted tip
root is rejected before it is persisted, rather than one block too late (when it would be
irreversibly on disk and could wedge the sync). Test-only trusted local sources are exempt and
commit a tip root on the in-arrears check. This is the safety contract: **a bad, slow, or
withholding peer cannot publish an incomplete initial prefix; after freeze, a later bad or
missing refetch never writes wrong state and does not reset the block queue for root
availability.** A height that stays stuck on a retryable stall past a threshold escalates
to an error-level log and the `state.vct.root.stalled.height` gauge, so a genuinely unservable
root surfaces loudly instead of a silent stall. Because the initial driver only publishes after
full-range success (§4.2), the common case is that the frozen window is never entered without
its roots in hand. Counters:
`state.vct.root.rejected.count` (evicted after failing verification),
`state.vct.root.unavailable.count` (frozen-frontier hole refused),
`state.vct.root.await_successor.count` (deferred for a missing successor),
`state.vct.root.retry.count` (park-and-retry attempts), and the
`state.vct.root.stalled.height` gauge (raised once a height is stuck past the warn threshold).

### 8.1 Adversarial peer policy (increment 6b)

The committer only reports root failures by height, and `zebra-state` intentionally has no
dependency on `zebra-network` peer types. Peer attribution therefore lives in the `zebrad`
`tree_aux` driver:

1. `zebra-network::zakura::fetch_roots_with_peer` returns each contiguous root batch with the
   authenticated `ZakuraPeerId` that supplied it.
2. The `zebrad` driver stages both roots and `(height, peer_id)` provenance for the requested
   range. After the full range succeeds, it records provenance **before** publishing roots to
   `TreeAuxRootsWriter`, so the committer cannot observe a root before the driver knows its
   supplier.
3. The driver keeps a bounded side table keyed by height (`TREE_AUX_FETCH_AHEAD_ROOTS +
   MAX_TA_ROOTS_PER_REQUEST`) and prunes entries at or below the committed-root eviction
   watermark.
4. If the committer rejects height `H` and requests a targeted refetch, the driver looks up the
   last supplier of `H`. If there is no provenance (the root was never supplied, or the request
   is stale), no peer is blamed.
5. Independently, peer request failures that are not state-verified bad content — timeouts,
   `RangeUnavailable`, malformed frames, or badly shaped batches — are recorded as **soft**
   failures. They move the peer behind normal peers for a short demotion window but do not evict
   cached roots, increment hard-failure counters, or disconnect the peer. A successful later
   response clears that soft state, and all-demoted peer sets remain selectable as fallback.
6. If a supplier is found, every still-cached height from that supplier is bulk-evicted through
   `TreeAuxRootsWriter::invalidate_roots`, and the supplier enters a bounded hard-failure
   cooldown. Refetches exclude peers in that cooldown via the `fetch_roots_with_peer` selection
   policy; hard exclusion overrides any soft-demotion state.
7. The driver keeps a per-peer offense record beyond the cooldown. A first offense is
   cooldown-only; repeated offenses in the decay window escalate to a whole-peer disconnect.

The current policy uses a 5-minute `tree_aux` cooldown, disconnects on the third hard failure in
one streak, and decays the streak after 30 minutes without another hard failure. A cooldown must
expire before an honest scheduling path can select the peer again, so three offenses represent
"lied, cooled down, came back and lied again, cooled down, came back and lied a third time."
Soft failures use a 1-minute back-of-rotation demotion and decay after 5 minutes without another
soft failure; they are liveness hints, not evidence that the peer supplied invalid root content.
This improves steady-state selection after a slow or withholding peer is observed, while hedged
requests cap cold-peer-set stalls when a good peer is in the first hedge group.

This closes the honest-peer-available liveness loop: a well-shaped but lying peer can cause one
retryable refusal, then its cached window is dropped and the same height is refetched from a
different peer. The initial response is intentionally scoped: bulk eviction clears the poisoned
window and the cooldown avoids reselection, but the peer can keep serving other Zakura streams.
If the same peer returns after cooldown and lies repeatedly before the offense record decays, the
escalated disconnect is whole-peer, not stream-local. Root verification happens after fetch in
state, outside Zakura block-sync's existing consensus-rejection scoring path, so the driver uses
a local `tree_aux` cooldown for selection and drops the connection only for persistent offenders.
This policy is kept separate from block-sync scoring because the rejected datum is not a block
body delivered through block sync; it is post-fetch root metadata verified by the state committer.

This policy still cannot guarantee liveness under a true eclipse where every selectable peer
lies, withholds, or is excluded. In that case the node remains fail-closed: no wrong state is
written, the root stays retryable, and the stall metrics/logs surface the unservable height.
Hedging is bounded to the first few preferred peers for each sub-range, so a cold rotation can
still miss an honest peer outside the first hedge group on one attempt, but it no longer walks
the whole peer set one 30-second timeout at a time.

## 9. The serving read path (`BlockRoots` / `TreeAuxStatePort`)

A node serves roots from local state via `ReadRequest::BlockRoots { start_height, count }` →
`ReadResponse::BlockRoots(Vec<BlockCommitmentRoots>)`, derived from per-height trees by
`produce_block_roots`. The handler:

- clamps the range to the finalized tip;
- serves from the compact `commitment_roots_by_height` index on fast-synced nodes, so nodes that
  lack historical per-height trees below the handoff can still serve root ranges;
- returns an empty vec for out-of-range/empty requests.

`zebra-network`'s `TreeAuxService` reads through `TreeAuxStatePort`, an async trait the node
implements over this request (`StateTreeAuxPort` in `zebrad`) — so `zebra-network` keeps no
dependency on `zebra-state`. The port maps read errors and wrong responses to an empty
(unavailable) serve, never wrong data.

## 10. Serving availability (open design concern)

Fast-synced nodes serve roots from `commitment_roots_by_height`, while older archive-produced
nodes can still derive roots from per-height trees. This keeps the root-serving fleet available
as more nodes fast-sync. A client that finds no serving peer degrades to legacy speed before
freeze or waits on targeted root refetches in the frozen window; it does not corrupt state. Two
mechanisms address it, in order of cost:

- **Roots-index CF (lightweight, preferred).** A fast node already verified every root it
  folded in. Persisting them into a compact column family (~68 bytes/block, ~200 MB for all of
  Mainnet) lets it serve them without per-height trees, at near-zero extra cost. A background
  task can backfill missing lower ranges by fetching *roots* (not bodies), so even a
  snapshot-started node becomes a full-range roots server cheaply. This is the targeted fix for
  the §4.3 / §10 availability gap.
- **Indexing-follower resync (heavyweight, opt-in).** Rebuild the per-height trees off the
  consensus critical path (re-downloading bodies if pruned), turning a fast node into a full
  archive node. This pays back the cost fast-sync avoided, so it is the archive/RPC path
  (increments 7–8), not a default.

Protocol hygiene that reduces the failure surface meanwhile: `Status` advertises each peer's
servable `[low, high]` range so clients only request from peers that can serve; plus multi-peer
fanout and re-request-from-another-peer (§12, 6b). Wiring `Status`-range advertisement is the
cheapest and most impactful.

## 11. Trust boundary and security

The trust boundary is sharp: **every peer-provided root must be authenticated against a header
commitment before it influences the anchor set or the history MMR.** Consequences:

- The wire payload (§5.1) and the source seam (§5.3) carry no trust; a serving/forwarding node
  is exactly as trustworthy as an originating one.
- The below-NU5 Orchard pin and below-Heartwood Sapling check (§6.1) close the only ranges the
  MMR cannot vouch for. Skipping either would let an untrusted source inject an anchor the
  legacy recompute never produces — a consensus-equivalence break, not just a slowdown.
- The frozen-frontier fail-closed policy (§8) means a hostile root never corrupts state: it is
  evicted and refused. The driver-side peer policy (§8.1) maps rejected heights back to their
  suppliers, bulk-evicts the supplier's cached roots, excludes the supplier from `tree_aux`, and
  disconnects repeat offenders. This prevents one lying-but-well-formed peer from grinding the
  sync height by height when honest peers are available.
- DoS bounds on the `tree_aux` codec (§5.4), the no-preallocate-from-count decode, and the
  fetch-ahead cap protect the serving and client paths from unbounded memory growth.
- The auth-data-root cache lock (§6.3) closes a cross-crate API hole that could otherwise
  finalize a block without binding its authorizing data.

## 12. Increment roadmap

- **Increments 0–5 (done):** the fast path proven end-to-end from a local test source — the
  source seam, verify-before-commit against headers, the frontier-recompute skip, and the
  verified checkpoint handoff with persistent fast-synced databases.
- **Increment 6a — peer source: fetch + serve (happy-path POC, this PR).** The `tree_aux`
  stream (roots-only), the `TreeAuxStatePort` serving side, the driver + `PeerSource`, and the
  peer-source default on Mainnet — the first point at which real nodes obtain roots over the
  network. The initial peer fetch is bounded by finalized commit progress, so peer delivery
  cannot fill the cache with the entire checkpoint range. Deferred follow-ups: tighter
  integration with live header-sync progress; multi-peer fanout/straggler hedging; and an RLE
  wire encoding.
- **Increment 6b — adversarial peer policy (done).** Verification failures stay consensus-local
  in state, but the `zebrad` driver records height→peer provenance for roots it publishes. A
  rejected height bulk-evicts all cached roots from that supplier, puts the supplier in a
  `tree_aux` hard-failure cooldown, and re-requests from another selectable peer. Repeated
  offenses in the decay window escalate to disconnecting the active Zakura peer. The §8 refusal
  remains the backstop for withholding and eclipse cases.
- **Increment 7 — indexing follower lane (archive only).** Relocate `tx_by_loc` + address
  indexes and the per-height trees + subtree CFs onto an async follower, so archive mode regains
  historical RPC without re-adding the frontier recompute to the consensus path.
- **Increment 8 — archive mode via the follower.** Run the full per-block recompute off the
  critical path to restore `z_gettreestate` / `GetSubtreeRoots`, while the consensus lane uses
  verified roots.
- **Increment 9 — spec / ZIP.** Publish the cross-client payload schema and verification
  algorithm so other clients (zcashd, zaino, …) can serve and verify identically.

### Supporting fix: Zakura header-store rollback

Independent of the fast path but on the same branch, `rollback_finalized_state` now also rolls
back the Zakura header store (`delete_zakura_headers_above`). The header store races ahead of
the body chain and is keyed independently; leaving it untouched on a rollback kept a
`BestHeaderTip` above the new body tip, which stalled body sync (the contiguous floor body was
never requestable) until the 5-minute timeout fell back to legacy ChainSync. (Commits #198,
#202.)

## 13. Observability

Live commit-path counters distinguish the fast and legacy paths and the failure modes:

| Metric | Meaning |
| --- | --- |
| `state.vct.fast.block.count` | block folded supplied roots, skipped the recompute |
| `state.vct.legacy.block.count` | block recomputed the frontier (`consensus.disable_vct_fast_sync = true`, `consensus.checkpoint_sync = false`, or fell back outside the frozen window) |
| `state.vct.prevalidated.block.count` | dedup sub-case: the previous fast block's look-ahead already validated this header |
| `state.vct.root.rejected.count` | supplied root failed verification and was evicted for re-fetch |
| `state.vct.root.unavailable.count` | frozen-frontier height with no valid root; commit refused (retryable) |
| `tree_aux.peer.hard_failure.count` | driver attributed a rejected root to a supplier and cooled it down |
| `tree_aux.peer.disconnect.count` | repeat root-supplier failures escalated to a whole-peer disconnect |
| `tree_aux.peer.cooldown.active` | number of peers currently excluded by the driver-side hard-failure cooldown |

The fast-vs-legacy ratio is the signal an integration test asserts to prove roots actually came
over the wire rather than a silent legacy sync.

## 14. Testing strategy

- **Unit:** the `BlockCommitmentRoots` and every `TreeAuxMessage` wire round-trip + DoS-bound /
  trailing-byte rejection; `select_source_mode` precedence (`consensus.disable_vct_fast_sync =
  true` or `consensus.checkpoint_sync = false` ⇒ legacy regardless of storage mode or embedded
  frontier; checkpoint sync + enabled VCT + embedded frontier ⇒ peer);
  a completed fast-synced DB reopens in archive
  mode (`reopening_fast_synced_database_in_archive_mode_succeeds`) while an interrupted one
  reopened with the fast path off is refused
  (`reopening_interrupted_fast_sync_without_a_root_source_panics`); the below-NU5 Orchard pin and
  below-Heartwood Sapling check; the `verify_commitment_roots` lag (wrong root rejected at H+1);
  the dedup (second consecutive fast block skips its check; a stale cache entry does not cause a
  false skip); the `StateTreeAuxPort` serve mapping (passthrough; error/wrong-response → empty
  range); `PeerSource::invalidate` and bulk `TreeAuxRootsWriter::invalidate_roots` eviction; the
  driver-owned provenance/failure table (committed pruning, capacity bounds, unknown-height
  no-blame, cooldown expiry with offense retention, offense decay, disconnect escalation, and
  all-heights-from-supplier rejection); and the in-process producer → `PeerSource` → committer
  byte-identical equivalence.
- **Frozen-frontier proptests:** a frozen-frontier hole returns the retryable
  `VctSuppliedRootUnavailable` and leaves the DB untouched; a reopened committer (frozen marker
  persisted) still refuses on the first post-restart missing root.
- **Two-node transport:** `two_nodes_exchange_roots_over_tree_aux` (cap negotiation + fetch),
  `client_driver_fetches_a_root_range_over_tree_aux`,
  `client_driver_reports_root_batch_provenance`, `client_driver_skips_excluded_tree_aux_peer`,
  `client_driver_errors_when_all_tree_aux_peers_are_excluded`, and
  `tree_aux_serves_real_state_roots_over_the_wire` (a real `populated_state` finalized DB serves
  through the production `StateTreeAuxPort` → `TreeAuxService` over the real loopback transport;
  an above-tip range errors so the committer keeps it legacy).
- **Adversarial peer policy integration:**
  `tree_aux_policy_disconnects_rejected_supplier_and_refetches_from_another_peer` drives the
  production `handle_refetch_request` path with real loopback Zakura peers, a real
  `TreeAuxRootsWriter`, driver-side provenance, repeat-offender whole-peer disconnect,
  replacement-peer refetch, and bulk invalidation of the rejected supplier's cached roots.
- **Driver resource bounds:** pure window-math tests cover initial fetch-ahead, handoff
  clamping, saturation, and waiting until the committed-root watermark opens more cache room;
  peer-source tests assert the watermark starts empty, advances on committed-root eviction, and
  never regresses.
- **Real-data manual runs (`#[ignore]`, env-gated):** `verifies_real_nu5_range_over_synced_forks`
  verifies the real NU5/V2 range against synced archive forks (corrupted root rejected at H+1).
- **Headline end-to-end (manual, follow-up):** a fresh node fast-syncing
  `verified_tip + 1` → checkpoint from a peer and reaching byte-identical consensus state, with
  `state.vct.fast.block.count > 0`. The full two-process Regtest docker e2e is unblocked by the
  `VCT_REGTEST_FRONTIER` override but crosses crate boundaries that cannot be wired into CI
  without a dependency cycle, so it stays manual.

## 15. File map

| Area | File |
| --- | --- |
| Wire payload (`BlockCommitmentRoots`) | `zebra-chain/src/parallel/commitment_aux.rs` |
| Source seam, `PeerSource`, producers, bulk root invalidation | `zebra-state/src/service/finalized_state/commitment_aux.rs` |
| Verify-before-commit logic | `zebra-state/src/service/finalized_state/commitment_aux_verify.rs` |
| Embedded frontier plumbing, `select_source_mode`, counters | `zebra-state/src/service/finalized_state/vct.rs` |
| `checkpoint_sync` mirror field (mode input) | `zebra-state/src/config.rs`; set in `zebrad/src/commands/start.rs` |
| Embedded Mainnet frontier | `zebra-state/src/service/finalized_state/vct/mainnet-frontier.bin` |
| Commit-path hook, handoff, frozen-frontier policy | `zebra-state/src/service/finalized_state.rs` |
| `BlockRoots` serving read | `zebra-state/src/service.rs` |
| `tree_aux` wire codec | `zebra-network/src/zakura/tree_aux/wire.rs` |
| `tree_aux` serving service + `TreeAuxStatePort` | `zebra-network/src/zakura/tree_aux/service.rs` |
| `tree_aux` client driver (`fetch_roots`, `fetch_roots_with_peer`) | `zebra-network/src/zakura/tree_aux/driver.rs` |
| Serving port, peer-source driver wiring, driver-side provenance/cooldown policy | `zebrad/src/commands/start/zakura/tree_aux_driver.rs` |

## 16. Frontier regeneration tool

The embedded Mainnet frontier is a release artifact coupled to the last Mainnet checkpoint.
Whenever the checkpoint list's max height changes, the matching
`zebra-state/src/service/finalized_state/vct/mainnet-frontier.bin` must be regenerated from a
synced Zebra state at that same height.

This belongs in the checkpoint-maintenance flow rather than in node runtime configuration. The
`zebra-checkpoints` utility runs against a synced node and produces the `HEIGHT HASH`
checkpoint artifact consumed by `.github/workflows/checkpoint-update.yml`. It also has an
explicit Mainnet frontier-artifact output:

```text
zebra-checkpoints \
  --addr 127.0.0.1:8232 \
  --last-checkpoint <old-height> \
  --mainnet-frontier-output /tmp/mainnet-frontier.bin \
  --state-cache-dir <synced-zebra-state-cache-dir> \
  --frontier-height auto
```

The checkpoint stdout format stays unchanged. The frontier is written only when
`--mainnet-frontier-output` is supplied, and status details go to stderr so the existing
checkpoint log scraper remains stable. `--frontier-height auto` means "use the final Mainnet
checkpoint height generated by this run"; an explicit height is useful for local validation and
debugging. `--state-cache-dir` is required whenever `--mainnet-frontier-output` is supplied.
With `--frontier-height auto`, the utility fails if the run did not emit any checkpoint above
genesis, because there is no updated handoff height to pair with the frontier artifact.

The frontier generator must read Zebra's finalized state, not reconstruct trees from RPC block
data. Checkpoint generation only needs block hashes and sizes, but frontier generation needs the
exact Sapling, Orchard, and Sprout note-commitment trees. The utility therefore opens Zebra
state read-only and calls `zebra-state` helpers that:

- opens the finalized DB read-only from the supplied state cache directory;
- reads the Sapling and Orchard trees at the requested height;
- reads the tip Sprout tree (Sprout is frozen far below modern checkpoints);
- serializes `FinalFrontiers { height, sapling, orchard, sprout }` using the same byte format
  parsed by node startup: `height` as `u32` little-endian, followed by length-prefixed
  `IntoDisk` blobs for Sapling, Orchard, and Sprout;
- immediately validates the generated bytes by parsing them through the same height-checking
  path used for the embedded frontier (`produce_final_frontiers_bytes` followed by
  `validate_final_frontiers_bytes`).

The GCP checkpoint-generation workflow copies `/tmp/mainnet-frontier.bin` out of the Mainnet
checkpoint-generation container and uploads it as a separate artifact named
`generate-checkpoints-mainnet-frontier`. `checkpoint-update.yml` replaces the embedded frontier
only when it appends new Mainnet checkpoints, and fails closed if Mainnet checkpoints advance
but the frontier artifact is missing, empty, or has an embedded height that does not match the
updated checkpoint max height.

Local testing proves byte compatibility with the node loader:

- build a small legacy `FinalizedState` over a generated valid chain;
- produce frontier bytes from that DB at a chosen height;
- write the bytes to a temporary file;
- load the file through the same loader/parser path used by `VCT_REGTEST_FRONTIER` and the
  embedded Mainnet frontier;
- assert the parsed height matches, the parsed Sapling/Orchard/Sprout roots match the DB, and
  parsing with a different expected height fails.

That test is the compatibility contract: if the local tool writes bytes that pass this path, the
node will parse the artifact in the same way at startup.

The focused local checks are:

```text
cargo test -p zebra-state final_frontier
cargo test -p zebra-utils --features zebra-checkpoints
cargo test -p zebrad --features zebra-checkpoints checkpoints
```
