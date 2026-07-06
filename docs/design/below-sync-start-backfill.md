# Below-sync-start forward backfill (Zakura header sync)

## Overview

A node whose Zakura header sync starts at a **trusted sync start** — a configured checkpoint
above genesis — has no headers or commitment roots for the region below it. This design
replaces the old, disabled *backward* checkpoint backfill with a **forward backfill**: a second
forward cursor that sweeps genesis → sync start using the exact same code path as the main
forward frontier, verifying peer-supplied commitment roots along the way and filling the
`commitment_roots_by_height` serving index.

The mechanism ships **dormant** behind a compile-time default
(`BELOW_SYNC_START_BACKFILL_ENABLED = false`, plumbed per-startup as
`HeaderSyncStartup::backfill_enabled`) until the node wiring consumes backfilled data. Its
targeted consumer is the roots-index serving backfill described in
`verified-commitment-trees.md` §10: with it armed, even a snapshot-started node becomes a
full-range roots server without fetching bodies.

Alongside the rework, the overloaded term **"anchor"** is renamed across the header-sync layer
(see [Terminology rename](#terminology-rename)).

## 1. Why the backward design was disabled

The original v1 backfill scheduled one *backward* checkpoint bracket below the sync start,
linked on the previous checkpoint's hash. Under verified commitment trees (VCT,
`verified-commitment-trees.md` §6.4), every root-carrying range must be folded into a history
tree positioned at the range's parent to authenticate its roots against header commitments.
A backward range folds onto the *previous checkpoint's* tree — which the reactor never tracks
and cannot cheaply obtain (there is no per-height finalized history tree, and checkpoints carry
hashes, not tree states). So backward ranges could only ever be checkpoint-hash-authenticated,
committed with no verified roots, and nothing consumed them. The scheduling was disabled
outright rather than left emitting unverifiable requests.

The insight behind the rework: **the only tree seed the reactor can always know is genesis**
(the empty tree — the natural pre-Heartwood value). A backfill that runs *forward from genesis*
needs no checkpoint-bracket tree tracking at all: it builds its own tree exactly the way the
main forward frontier does.

## 2. Design

### 2.1 A second forward cursor

`HeaderSyncCore` gains a backfill frontier mirroring the forward one:

| Field | Meaning |
| --- | --- |
| `backfill_tip` / `backfill_hash` / `backfill_parent_hash` | highest backfilled header below the sync start (its own root may still be unconfirmed) |
| `backfill_history_tree` | ZIP-221 history tree positioned at the parent of the next backfill range; seeded empty at genesis |
| `backfill_sync_start_root_confirmed` | true once the stitch (§2.3) has confirmed and persisted the sync-start height's own root |

`refresh_backfill_range` (state.rs) schedules one range at a time while
`backfill_tip < sync_start`:

- **Checkpoint-bounded brackets.** Each range ends at the next checkpoint at/above the frontier,
  capped at the sync start (itself a checkpoint, so the cap never produces a non-checkpoint
  end). Ranges are `finalized: true` — whole delivery is required and the delivered end must
  hash-match the checkpoint list — and `want_tree_aux_roots: true` (the whole region is below
  the VCT handoff boundary).
- **One-block overlap.** Root-carrying ranges redeliver the frontier header, linked on
  `backfill_parent_hash`, so the previous range's tip root is confirmed by its successor —
  identical to the forward overlap rule.
- **Priority.** `RangePriority::Backfill` ranges are assigned strictly after forward work
  (the scheduler drains the forward queue first), so backfill never competes with tip sync.

### 2.2 Verification: the same gate as forward

Delivery validation reuses the forward pipeline end to end. The verified-roots gate keys on
`want_tree_aux_roots` alone; `validate_header_aux_commitments_for_range` selects the tree by
priority (forward frontier tree vs backfill tree) and both feed the same
`verify_supplied_roots_from_parts` fold (`zebra-chain/src/parallel/commitment_aux_verify.rs`) —
ZIP-221 one-block-lag commitment checks plus the direct below-Heartwood/NU5/`Nu6_3` pins, no
new crypto. Commits carry `verified_roots: Some(..)` and persist only the header-authenticated
confirmed prefix, exactly like forward ranges. The trust boundary of
`verified-commitment-trees.md` §11 is unchanged: nothing unauthenticated is ever written.

`HeaderSyncAction::CommitHeaderRange` gains a `backfill: bool` flag. On the committed event the
reactor resolves the originating cursor from its pending commit and advances **only** the
backfill frontier: backfill commits never move `best_header_tip`, never publish the shared
sync-exchange frontier (the zebrad driver skips `publish_header_frontier`), and never produce
body gaps.

### 2.3 The sync-start stitch

A root at height `H` is only confirmed by the header at `H + 1`. Inside the backfill region that
is handled by the bracket overlap, but the **sync-start height's own root** has no confirming
successor there — and the forward path never persists it either (its first range starts at
`sync_start + 1`, and `CommitHeaderRange` persists confirmed prefixes, never the height below a
range start). Left alone, the roots serving index would have a permanent one-height hole exactly
at the sync start, truncating every all-or-nothing `BlockRoots` serve that crosses it.

So once the backfill frontier reaches the sync start, a final **stitch range**
`[sync_start ..= sync_start + 1]` re-fetches the sync-start header plus its successor:

- `finalized: false` (its end is not a checkpoint), so instead the reactor authenticates any
  delivered backfill header at the sync-start height **in-span against the trusted hash**;
  state re-checks every checkpoint height at commit as defense in depth.
- The fold confirms exactly the sync-start root (confirmed prefix of a two-header range).
- The redelivered `sync_start + 1` header re-commits idempotently (state accepts identical
  re-commits and never lets a header range overwrite committed bodies or finalized headers).
- The commit sets `backfill_sync_start_root_confirmed`; the frontier itself stays at the sync
  start, and the backfill is complete.

### 2.4 Coverage exemption and the backfill staleness rule

The scheduler's covered-interval machinery tracks the **forward** frontier's committed spans.
The stitch deliberately overlaps forward-covered heights (the forward path starts at the sync
start), so backfill ranges are **exempt from covered checks everywhere** — `ensure`, `retry`,
`prune_covered`, and `cancel_covered_outstanding` all skip `RangePriority::Backfill`.

That exemption needs a replacement staleness rule. Without one, a fanout duplicate of an
already-committed bracket would loop forever: delivered against the advanced backfill tree it
can only mismatch, be retried, be reassigned, and mismatch again. Backfill's rule is keyed to
its own frontier: when a bracket commit advances `backfill_tip`, queued, assigned, and in-flight
backfill ranges whose end is at or below the new frontier are retired
(`RangeScheduler::retire_stale_backfill`, `cancel_stale_backfill_outstanding`), and late
deliveries for the cancelled requests are tolerated through the existing late-covered-response
credit. A delivery that still reaches the tree-mismatch arm with a stale range is dropped rather
than retried.

### 2.5 Tree mismatch is a bug, not a race

For the forward frontier, a tree behind its range parent means a non-Zakura path committed
ahead, and a guarded `QueryBestHeaderHistoryTree` rebuild recovers. Nothing analogous moves the
backfill region: the rebuild path is keyed to `best_header_tip`, and full-block commits below
the sync start do not occur in the intended deployment. A backfill tree mismatch therefore
indicates an internal bug. The reactor logs at `warn`, increments
`sync.header.backfill.tree_mismatch`, clears the assignment, and retries (or drops a stale
range) — it deliberately does **not** dispatch the forward rebuild, which would install the
wrong tree. The v1 recovery for a persistently wedged backfill is a restart, which reseeds the
cursor at genesis.

### 2.6 Startup and restarts

v1 keeps no durable backfill cursor: every startup reseeds at genesis. This is safe (identical
re-commits are idempotent; roots re-verify from the empty tree) and acceptable for a dormant
feature, but wasteful once armed. The follow-up is a startup read analogous to
`ReadRequest::BestHeaderHistoryTree` but genesis-based: fold the durable below-sync-start
confirmed roots from the empty tree up to the first gap (capped below the sync start) and return
the frontier `(height, hash)` plus tree. `HeaderSyncCore::new` isolates backfill seeding at a
single insertion point for exactly this.

## 3. Terminology rename

"Anchor" previously meant three things in this layer: the trusted sync start, the per-range link
target, and — worst — it collided with Zcash note-commitment *anchors*, the very anchor sets VCT
folds roots into. Renamed across `zebra-network/src/zakura/` and the zebrad Zakura drivers:

| Old | New |
| --- | --- |
| `anchor` (sync-start sense: core/startup/config accessor) | `trusted_sync_start` |
| `RangeRequest.anchor_hash`, block-sync request `anchor_hash` | `link_hash` |
| `validate_anchor` | `validate_trusted_sync_start` |
| `StaleAnchorFailures` / `stale_anchor` / `HEADER_SYNC_STALE_ANCHOR_*` | `StaleLinkFailures` / `stale_link` / `HEADER_SYNC_STALE_LINK_*` |
| `reanchor_*`, `HeaderReanchored`, `HeaderFrontierReanchor` | `rebase_*`, `HeaderRebased`, `HeaderFrontierRebase` |
| metric `sync.header.history_tree.reanchor` | `sync.header.history_tree.rebase` |
| peer status `anchor_height` (advertised serving floor) | `sync_start_height` (wire encoding unchanged) |

Deliberately kept (compatibility; flagged as follow-up): the `ZakuraHeaderSyncConfig`
`anchor_height` / `anchor_hash` config keys, and the zebra-state API names
(`Request::CommitHeaderRange { anchor }`, `CommitHeaderRangeError::{UnknownAnchor,
MissingGenesisAnchor}`). The driver maps `link_hash` into the state request's `anchor` field at
the crate boundary with a comment.

## 4. Pre-arming checklist (known gaps, acceptable while dormant)

- **Genesis link context.** The first backfill commit is linked on the genesis hash and requires
  the genesis header in state (`MissingGenesisAnchor` otherwise — a silent local retry loop). A
  sync-start-configured node may hold neither; seed the genesis header at driver startup when
  arming.
- **Peers without below-sync-start history.** Assignment filters only by advertised tip, so
  peers that themselves started at a sync start return empty responses and cycle through the
  retry backoff. Enhancement: prefer peers whose advertised `sync_start_height` is at/below the
  range parent for backfill ranges.
- **Archival nodes with bodies below the sync start.** Backfill would re-insert zakura header
  rows at body-holding heights. Either document "don't arm backfill on archival nodes" or add a
  state-side skip of identical header-row inserts at body-holding heights.
- **Bracket size vs the root-carrying byte budget.** Finalized ranges are never narrowed, so
  every checkpoint bracket must fit one root-carrying response
  (`clamp_header_sync_request_count(.., want_tree_aux_roots)`). Verify mainnet checkpoint
  spacing (≤ 400) fits before arming; otherwise brackets need a multi-range interior.
- **Startup resume read** (§2.6) to avoid re-fetching from genesis on every restart.

## 5. Testing

Reactor (`zebra-network/src/zakura/header_sync/tests.rs`, fixtures opt in via
`backfill_enabled`):

- `below_sync_start_backfill_is_disabled_by_default` — default-off regression.
- `backfill_schedules_checkpoint_bounded_range_from_genesis` — bracket shape + truncated
  finalized delivery rejected.
- `backfill_checkpoint_end_hash_mismatch_is_misbehavior` — divergent checkpoint fixture.
- `backfill_verifies_roots_against_backfill_tree` — commit carries `backfill: true` with the
  confirmed prefix; a wrong root is `InvalidRange` with no commit.
- `backfill_commit_advances_backfill_frontier_and_stitch_completes` — full lifecycle: bracket
  commit → stitch request → stitch commit confirms exactly the sync-start root → no further
  backfill requests; asserts no `HeaderAdvanced`/`BodyGaps` on backfill commits.
- `forward_ranges_are_assigned_before_backfill` — the forward range's full fanout is assigned
  before the backfill bracket gets its first peer.
- `late_backfill_fanout_delivery_is_dropped_without_rebuild` — no `QueryBestHeaderHistoryTree`,
  no misbehavior, no duplicate commit.

State (`zebra-state`): `header_range_commit_accepts_intermediate_anchor_and_identical_recommit`
— mid-backfill bracket anchors resolve through the header index, identical re-commits (the
stitch shape) are accepted, and the header tip never regresses. Existing tests already pin the
confirmed-prefix/empty/full-length root-vector shapes.

Driver (`zebrad`): `header_sync_driver_backfill_commit_does_not_publish_header_frontier` — a
`backfill: true` commit leaves the exchange frontier untouched; the identical forward commit
publishes it.

## 6. Related documents

- `verified-commitment-trees.md` — the VCT design this builds on; §6.4 describes the shared
  verification gate, §10 the serving-availability consumer, §12 increment 6f.
