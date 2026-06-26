# glue_refactor — post-checkpoint-sync follow-ups

Things we *know* we need to fix, but that are **not reachable on the checkpoint
sync path** and so were deliberately left for the full-sync / post-checkpoint
phase (Phase 5 in `design_doc.md` / `scoped_design_doc.md`). Each item is safe
to defer because the checkpoint path commits to the **finalized** state, where
`verified_block_tip` is monotonic over the (permanent) finalized tip — the
property that makes the apply seam self-healing below the checkpoint boundary.
Post-checkpoint the best chain is **non-finalized and reorg-able**, so that
property no longer holds and these gaps become real.

Status: open. Owner: TBD when the full-sync phase starts.

---

## 1. `Duplicate` commit result does not refresh the frontier

**Where:** `zebrad/src/commands/start/zakura/committer.rs`
(`on_commit_done` collapses `Committed | Duplicate` → advance `committed_marker`,
nothing else); contrast the deleted executor tail in `block_sync_driver.rs`
(pre-cutover `a3e1d21e8`, ~line 669) which, on a `Duplicate`, fell through to
`query_block_sync_frontiers` + `publish_body_frontier` (the checkpoint
early-return was gated on `Committed` only).

**The gap:** a duplicate commit does **not** mutate state, so it fires no new
`ChainTipChange`. The `Committer` holds no state handle (by design — the A3
zero-read invariant), so the only thing that releases the Sequencer's held
`applying` bytes / advances its floor for that height is the durable-tip watcher
(`drive_block_sync_durable_frontier`), which is edge-triggered on
`ChainTipChange`. If a `Duplicate` is the *last* activity and no further tip
change ever fires, that height's held byte reservation can sit unreleased.

**Why it's safe to defer (checkpoint path):**
- A checkpoint `Duplicate` means the height is **finalized** (permanent).
  `verified_block_tip_from_state` returns `max(finalized, best_chain)`
  (`frontier.rs:17`), so any watcher fire after the height is durable reads
  `≥ height` and `release_applied_through` frees it. The release tracks
  durability correctly.
- For the height to be *held* in `applying` with `verified_tip < height`, the
  watcher must not yet have read the finalizing tip-change — which is **latched**
  in the `ChainTipChange` watch and therefore guaranteed to be processed. Once
  read, `verified_tip ≥ height` and finalized-monotonicity means it never rolls
  back, so the height can never re-enter `applying`
  (`accept_buffered_body` rejects `height <= floor` as `Redundant`).
- `MissingBlockBodies` only returns heights with no committed body, so block-sync
  does not re-request an already-finalized height in the first place.

**Why it becomes real post-checkpoint:** non-finalized blocks are reorg-able, so
`verified_block_tip` is **not** monotonic, and two commit paths (block-sync +
legacy syncer) can race the same non-finalized block → genuine `Duplicate`s. The
worst case is a bounded, per-height held-byte leak (not a hard wedge — reorgs/
grows still fire `ChainTipChange`), but it should be closed.

**Proposed fix (event-driven, preserves the Committer's zero-read invariant):**
on a `Duplicate` (rare), have the `Committer` poke an `Arc<Notify>` that the
durable-frontier watcher also selects on; the *watcher* (which already owns
`ReadState`) re-reads and reports the frontier. That is "refresh after a commit
that didn't move the tip" expressed as an **event**, not the level-driven poll
the cutover deleted. ~15 lines across `committer.rs`, `block_sync_driver.rs`, and
the `start.rs` wiring, plus a test (a `Duplicate`-only verifier + a controllable
watcher asserting the held bytes release without a state-mutating tip change).

---

## 2. `FrontierAdvance` has no apply-epoch / generation guard

**Where:** `zebra-network/src/zakura/block_sync/sequencer_task.rs`
`handle_frontier_advance` — the only guard is monotonic
(`if frontiers.verified_block_tip < self.sequencer.verified_tip() return`).
Contrast `handle_commit_rejected`, which is epoch-guarded
(`applying_epoch(h) == reset.epoch`).

**The gap:** an in-flight `query_block_sync_frontiers` can complete *after* a
concurrent destructive reset (`reset_to` rolled `verified_tip` back and bumped
`apply_epoch`) and report a higher tip from before the reset. The monotonic guard
accepts it (higher than the just-reset tip) and re-advances over a repopulated
ledger.

**Why it's safe to defer (checkpoint path):** state reads reflect committed
(finalized) state, and finalized never rolls back, so a read can't report a tip
that a checkpoint reset invalidated — the accepted advance is always truthful.

**Why it becomes real post-checkpoint:** with reorg-able best-chain tips,
`reset_to` can legitimately roll `verified_tip` back below a previously-read tip;
an in-flight stale read could then re-advance past the reset. Add a generation
guard to `FrontierAdvance` (carry the `apply_epoch` the read was issued under, or
re-validate against the current epoch) for symmetry with the reject path.

---

## 3. Full-sync / fork commit path (Phase 5 — the parent of 1 & 2)

**Where:** the same applyQ + `Committer` will handle Full-class blocks
(per-block `Request::Commit`, no batching). Non-finalized/fork handling lives in
zebra-state (`validate_and_commit_non_finalized`, `commit_block`/
`commit_new_chain`).

**What's missing:** the behavioral difference is that the best tip moves
**non-monotonically** on reorg (`set_best_non_finalized_tip` →
`ChainTipChange::Reset`). The Sequencer's `handle_frontier_reset` and the
download side must roll `sent_tip` / needed-set to the new tip, and Serving must
key off the durable tip (never the Downloader's `sent_tip`) so we never serve an
uncommitted block. Items 1 and 2 are specific instances of this path's
non-monotonicity; close them together with the full-sync work, with reorg
property tests (verified_block_tip non-monotonic, double-commit `Duplicate`,
stale-read-after-reset).

---

## 4. State-internal timers (Phase 6 — out of scope here, tracked)

Not part of block-sync, but on the same "delete the backstops" thread and they
sit under the commit path:
- the write thread `park_timeout(10 ms)` self-poll (`zebra-state/.../write.rs`)
  → `blocking_recv()` on the write channel;
- the VCT root waits (`VCT_ROOT_RETRY_WAIT` 500 ms / `VCT_AWAIT_SUCCESSOR_WAIT`
  20 ms) → a roots-available `Notify` from the header-sync persist path.

These are zebra-state internals; event-driving them is a separate change behind
its own review.

---

## Convergence checklist for the full-sync phase

- [ ] Item 1: `Duplicate` → Notify-poke the durable watcher; held bytes release
      without a state-mutating tip change (test).
- [ ] Item 2: `FrontierAdvance` epoch guard; stale-read-after-reset rejected (test).
- [ ] Item 3: reorg property tests (non-monotonic tip, fork reset rolls
      `sent_tip`/needed-set, serving keyed off durable tip).
- [ ] Item 4: tracked separately (state-internal).
