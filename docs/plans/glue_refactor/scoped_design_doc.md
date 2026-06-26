# Block-sync apply-side seam — scoped, reviewable plan

> **Relationship to `design_doc.md`.** `design_doc.md` is the full event-driven rebuild — the
> superior architecture, kept as the north star / future direction. It rebuilds the battle-tested
> networking/reactor/download layer, which is too large and risky for review right now. **This
> scoped plan ships the part that matters** — the simple apply pipeline and killing the 200 ms
> frontier poll — **without touching the download/reactor machinery**. It keeps the full plan's
> apply-side simplicity and its correctness anchors; it drops the download-half rebuild.

## Context

The perf bottleneck is the 200 ms `CHECKPOINT_FRONTIER_REFRESH` poll: after a checkpoint commit the
sequencer polls `refresh_checkpoint_frontier()` to learn the durable frontier, and that frontier
advance is the only thing that calls `release_applied_through` to free the byte budget the download
side runs against. So the budget — which governs how far ahead block-sync fetches — recycles once
per poll tick instead of continuously, leaving the finalized writer idle with periodic stalls.

The full plan fixes this by rebuilding both halves event-driven. **Reviewers cannot absorb a
rebuild of the reactor/peer-routine/work-queue networking layer.** So we scope to the apply side:
keep the existing download machinery filling the pipeline exactly as it does today, and replace
only the sequencer's *commit tail* with the simple `applyQ` + `Committer`, swapping the 200 ms poll
for a durable-tip watch that feeds the *existing* frontier-advance path.

## The cut

Cut the pipeline at the contiguous-drain point. The existing machinery keeps **filling**; only the
sequencer's **commit tail** is replaced.

```
  [ KEPT, unchanged ............................ ]   [ NEW ]        [ NEW ]
  peers → peer routines → reorder → contiguous drain → applyQ  →  Committer  → Request::Commit → state
                                       │                                                           │
                                       │ (byte-budget RESERVATION, work queue, floor: KEPT)        │
                                       ▼                                                           ▼
            sequencer.release_applied_through ◄── FrontierAdvance ◄── durable-tip watch (set_finalized_tip)
              (KEPT mechanism, NEW trigger: watch instead of 200 ms poll)
```

## What stays UNCHANGED (the reviewer-sensitive surface)

- **Untouched files:** `reactor.rs`, `peer_routine.rs`, `work_queue.rs`, `peer_registry.rs`,
  `reorder.rs`, `admission.rs`, and all serving / needed-blocks-producer code.
- **Untouched behavior:** peer download, peer scoring, the reactor select loop, the work queue, the
  byte-budget **reservation** path, the download floor, `FrontierReset`, `FundFloorReservation`,
  the `SequencerView` the reactor reads.
- **The download-side backstops REMAIN** (floor shed tick, floor watchdog, retry-avoid, outbound
  poll, metrics tick). They are the price of a reviewable change; the full plan removes them. This
  plan removes **only** the 200 ms frontier poll.
- **Kept verbatim in the sequencer:** `advance_verified_tip`, `release_applied_through`, floor
  tracking, `handle_frontier_advance`. Only their *trigger* changes (poll → watch) and the *block
  storage* moves out to `applyQ` (the sequencer keeps a bytes-only ledger for release accounting).

## What is REPLACED (small, contained — sequencer commit tail only)

**DELETE from `sequencer_task.rs`:**
- `submit_pending_blocks`, `can_submit_class`, the apply-window counters
  (`checkpoint_in_flight` / `full_in_flight`), `in_flight_applies` (`FuturesUnordered`),
  `observe_apply_completion`, `process_apply_completion`,
- `CheckpointFrontierRefresh`, `process_checkpoint_refresh`, the 200 ms refresh `select!` arm and
  its constants,
- the executor seam `BlockApplyExecutor::{apply, refresh_checkpoint_frontier}`.

**ADD:**
- the contiguous drain pushes `ApplyItem`s into `applyQ` (instead of into the submit path);
- a bytes-only ledger `held_until_durable: BTreeMap<Height, u64>` populated on push and drained by
  the existing `release_applied_through` on `FrontierAdvance` (the `applying` buffer is repurposed
  from "submitted, awaiting completion" to "pushed to applyQ, awaiting durable" — it now holds only
  bytes, since the block `Arc` lives in `applyQ`/the `Committer`);
- the new `Committer` task draining `applyQ`;
- a tiny durable-tip watcher that feeds `FrontierAdvance` (see below).

## The seam: `ApplyItem` + `applyQ`

```rust
/// One contiguous, hash-verified block ready to commit (ascending, gap-free, from durable_tip+1).
pub(crate) struct ApplyItem {
    pub height: block::Height,
    pub hash: block::Hash,          // already checked == committed header hash at receipt (download side)
    pub block: Arc<block::Block>,
    pub bytes: u64,                 // actual serialized size (already settled by the download side)
    pub source_peer: ZakuraPeerId,  // misbehavior attribution on commit failure
    pub epoch: u64,                 // reset generation; Committer discards stale epochs
}
// applyQ = mpsc::channel::<ApplyItem>(APPLYQ_CAP)  — generous COUNT cap; the byte budget is the
//   real bound (every item was reserved by the download side), so the producer never blocks on send.
```

## The Committer (the apply-side simplicity, kept from the full plan)

A pump: fire `Request::Commit` for each item **without awaiting** into a `FuturesUnordered`, drain
completions. The checkpoint verifier batches a contiguous range internally, so a serial "await each
Ok" loop **deadlocks** — fire the whole range concurrently. In-flight depth is bounded by the byte
budget (see the budget-floor invariant). **The Committer does not touch the byte budget** — the
sequencer releases it on the durable watch; the Committer only commits and, on failure, resets.

```rust
struct Committer {
    apply_rx:  mpsc::Receiver<ApplyItem>,
    verifier:  BlockVerifierService,             // zebra_consensus::Request::Commit(Arc<Block>)
    max_checkpoint_height: block::Height,
    actions:   mpsc::Sender<BlockSyncAction>,    // Misbehavior
    reset_tx:  mpsc::Sender<CommitterReset>,     // -> sequencer (rejection only; near-never on checkpoint path)
    commit_timeout: Duration,                    // ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT = 30s
    in_flight: FuturesUnordered<BoxFuture<'static, CommitOutcome>>,
    committed_marker: block::Height,
    last_reset_epoch: u64,
    shutdown:  CancellationToken,
}

async fn run(mut self) {
    loop {
        if self.shutdown.is_cancelled() && self.in_flight.is_empty() { break; }
        tokio::select! {                          // plain select; single consumer, no shared state
            _ = self.shutdown.cancelled() => {}
            item = self.apply_rx.recv(), if !self.shutdown.is_cancelled()
                => self.on_item(item),            // fire commit, push to in_flight; NO await
            Some(done) = self.in_flight.next(), if !self.in_flight.is_empty()
                => self.on_commit_done(done),
        }
    }
}
```

```rust
fn on_item(&mut self, item: Option<ApplyItem>);
//   None => producer gone (drain). Some(item): discard if item.epoch <= last_reset_epoch; else push
//   timeout(commit_timeout, verifier.oneshot(Request::Commit(item.block))) into in_flight (FIRE).
fn on_commit_done(&mut self, done: CommitOutcome);
//   Ok(hash==meta.hash) => committed_marker = max(..); trace. Budget release is the SEQUENCER's job
//     (durable watch); nothing else here. Ok(other)|Err => on_commit_error(meta).
fn on_commit_error(&mut self, meta);
//   ONE Misbehavior{source_peer, InvalidBlock} + ONE CommitterReset{height, epoch:++ , source_peer};
//   set last_reset_epoch. Sibling Errs in the failed range are coalesced by the epoch guard.
```

(For the full apply-half rationale — why concurrent, why epoch coalescing, the write-thread
`invalid_block_reset` re-entry — see `design_doc.md` §Apply half. The Committer here is identical
except it does not own the byte release.)

## Frontier feedback: durable watch → existing `FrontierAdvance` (chosen approach)

Replace the deleted 200 ms `refresh_checkpoint_frontier` poll with a **tiny watcher task** that
subscribes to the state's `set_finalized_tip` signal (`LatestChainTip` / `ChainTipChange`,
`zebra-state/src/service/chain_tip.rs`) and sends `SequencerControlInput::FrontierAdvance` to the
sequencer — **exactly the message the poll produced**. Then:

- the sequencer's `handle_frontier_advance` → `advance_verified_tip` → `release_applied_through`
  runs **unchanged**, releasing the `held_until_durable` bytes ≤ durable and advancing the floor;
- the reactor, work queue, and floor consume the same `SequencerView` / frontier they do today.

This is the one event-driven change, and it is the one that fixes the bottleneck: the byte budget
now recycles continuously (per durable advance) instead of once per 200 ms poll. It is also
memory-safe — release happens on **durable**, covering the block's residency in `applyQ` and the
state write queue. The watcher is ~30 lines and lives in the driver; no reactor code changes.

## Byte lifecycle (reserve the header-provided size — NOT worst-case)

The `ByteBudget` is the single memory bound; every block holds a reservation from the moment we ask
for it until it is durable. The reservation is sized from the **header-provided body size**, with
worst-case only as a last-resort fallback. Getting this right is what lets the budget hold the *true*
number of in-flight blocks, and it is the one part of the byte path this plan is careful to keep
correct (the rest is unchanged from today).

1. **Reserve — at request-send, sized from the header.** Before a `GetBlocks` goes out, the download
   side `try_reserve(size)`s the body size. `size` is the `BlockSizeEstimate` on the work item
   (`work_queue.rs::estimate_bytes_with`, `request.rs`), which is one of:
   - **`Confirmed(n)`** — the exact serialized body size from committed block metadata. Header-sync
     persists per-block `body_sizes` alongside each committed header; once committed this size is
     authoritative. **This is the size provided by the header, and the normal case.**
   - **`Advertised(n)`** — a size a header-sync peer reported but not yet confirmed; used as-is for
     scheduling and verified on receipt.
   - **`Unknown`** — *only here* do we reserve the per-block worst case `block::MAX_BLOCK_BYTES`,
     clamped to `[floor, MAX_BLOCK_BYTES]`.

   So the normal path reserves the **real body size**, not worst-case. This matters: reserving
   `MAX_BLOCK_BYTES` (≈2 MB) for every block when real bodies average a small fraction of that would
   over-account the budget by ~an order of magnitude — the budget would "fill" logically while RAM is
   nearly empty, so we'd fetch far fewer blocks ahead than memory allows and throttle throughput for
   no memory benefit. We have the size from the header, so we use it; `Unknown`/`MAX_BLOCK_BYTES` is a
   fallback for the rare unsized height, not the default.

   The header size is safe to reserve against because the body must hash to the committed header
   hash: exactly one body does, and its serialized size is fixed. A wrong `Advertised` size cannot
   smuggle in a larger body — see settle.

2. **Settle — on receipt, to the actual serialized size.** When the body arrives the download side
   reconciles the reservation to the bytes actually held: release slack if the estimate over-stated,
   `ByteBudget::charge` the overshoot if it under-stated (charge exists precisely because an admitted
   body cannot be rejected at this point). Two guards bound the error: a body deviating beyond
   `config.size_deviation_tolerance` from its estimate is `SizeMismatch` misbehavior
   (`tolerated_bytes`), and a body that does not hash to the committed header hash is rejected
   outright. So a peer cannot inflate memory by lying about size — at worst one tolerated-deviation
   body before it is scored.

3. **Hold — through reorder + applyQ + the state write queue.** The (now exact) reservation stays
   held while the body sits out-of-order in the reorder buffer, waits in `applyQ`, and sits in the
   state's *unbounded* finalized write queue after `Request::Commit` returns Ok but before it is
   durable (state owns the `Arc`; the block is still resident in RAM). This residency is exactly why
   release is gated on durability, not on commit-Ok.

4. **Release — on the durable `FrontierAdvance`.** When `set_finalized_tip` crosses the height the
   durable-tip watcher emits `FrontierAdvance`; the sequencer's `release_applied_through` releases the
   `held_until_durable` bytes ≤ the new tip and `notify_waiters()` wakes the download side to fetch
   more. Releasing on durable makes the budget a true end-to-end RAM bound no matter where the block
   sits.

Net: header-sized reservations mean the budget tracks real bytes, fill depth reflects actual memory
(not an inflated estimate), and `settle`/`charge` + the deviation/hash guards absorb the rare hint
error. Fill depth ≈ today (one budget ahead); release timing ≈ today (durable), now event-driven
instead of polled. The download side cannot tell the difference except the budget frees more smoothly.

## Concurrent submission + the hard budget-floor invariant

Because the checkpoint verifier resolves a block's `Request::Commit` only after the **entire range
to the next checkpoint** is submitted, the Committer fires the whole range concurrently. The
in-flight depth is bounded by the byte budget, which imposes a hard invariant enforced by
`config.validate()`:

> **`max_inflight_block_bytes` ≥ the largest checkpoint range's total bytes + headroom**
> (`MAX_CHECKPOINT_BYTE_COUNT`). A budget smaller than one checkpoint range cannot hold a full range
> in flight → the verifier never completes the batch → permanent deadlock.

This is the invariant the old ~401 apply-window silently encoded; it is now explicit.

## What we deliberately did NOT change (state this in the PR)

- No change to peer download, the reactor select loop, the work queue, peer scoring, or serving.
- The download-side backstops remain (shed tick, floor watchdog, retry-avoid, outbound poll). Only
  the 200 ms frontier poll and the apply-window/submit machinery are removed.
- No `Assembler` / `PeerRoutine` / `NeededProducer` / `NeededSet` rewrite (those are the full plan).

## What this deletes (net simplification)

The seam is a **replacement, not an addition**: ~1,200 LOC of production code deleted (plus ~3,000
LOC of coupled tests) against ~300 added — **≈ −900 LOC net in production**. The added code is
dominated by the `Committer`, which mostly *reuses* the existing commit functions. Three buckets
carry the deletion:

1. **The type-erased executor seam (~226 LOC).** `BlockApplyExecutor` trait + `BlockApplyExecutorPort`
   + `BlockApplyRequest` / `BlockApplyOutput` / `BlockApplyToken` / `BlockApplyLimits`
   (`events.rs:96-227`) + `ZebradBlockApplyExecutor` and its trait impl (`block_sync_driver.rs:32-132`)
   + `install_block_apply_executor`. This abstraction exists *only* because zebra-network cannot name
   `zebra_consensus` / `zebra_state`; a plain `mpsc<ApplyItem>` channel to the zebrad-side `Committer`
   erases it. (`BlockApplyResult` + `BlockApplyClass` survive — the Committer reuses them.)
2. **The 200 ms checkpoint-frontier refresh machinery (~290 LOC).** `CheckpointFrontierRefresh` +
   `process_checkpoint_refresh` + `observe_apply_completion` + the 200 ms `select!` arm
   (`sequencer_task.rs:199-244, 545-553, 750-769`) + `refresh_block_sync_frontiers_for_checkpoint_window`
   + `query_checkpoint_refresh_frontiers` + the refresh trace helpers (`block_sync_driver.rs:846-1092`).
   All replaced by the one ~40-60 LOC durable-tip watcher shim.
3. **The apply-window (~430 LOC).** `submit_pending_blocks` / `can_submit_class` /
   `checkpoint_in_flight` / `full_in_flight` / `in_flight_applies` + the apply-completion handling
   (`sequencer_task.rs:712-769, 1072-1184`) + the entire submit/token side of the `Sequencer`
   (`SubmitItem`, `prepare_submit`, `submittable_heights`, `unsubmit`, `record_submitted_apply`, the
   `submitted_applies` map, token minting — `sequencer.rs:306-432`). Replaced by the contiguous drain
   pushing `ApplyItem` onto applyQ, concurrency bounded by the Committer's `FuturesUnordered` under
   the budget-floor invariant.

Plus ~3,000 LOC of tests exercising the deleted machinery (mock executors, apply-window cap tests,
completion-feedback tests); some are rewritten onto the Committer rather than dropped.

**Relocated, not deleted (be honest in the PR):** the reject/timeout floor-rollback + `Misbehavior`
logic in `handle_apply_finished` (`sequencer_task.rs:1018-1051`, ~34 LOC) returns as a small
`SequencerControlInput::CommitRejected` from the Committer (reusing the existing
`release_applying_blocks_from` / `reset_floor_below`); the commit-progress/throughput observability
is re-fed by the Committer rather than removed.

**New code (~300 LOC):** `ApplyItem` (~12), applyQ wiring (~15), the drain push replacing
`submit_pending_blocks` (~15), the `held_until_durable` ledger tweak (~10), the `Committer`
(~120-160, mostly reusing `commit_block_sync_body_with_stall_trace` / `block_commit_result`), the
`CommitRejected` feedback input (~30), and the durable-tip watcher shim (~40-60).

## Test-property catalog (scoped)

Apply-half (identical to `design_doc.md` A1–A9): batch-resolve no-deadlock, byte-release-exactly-once
(now asserted on the **sequencer's** release at the durable watch), zero reads on the commit hot
path, reset attribution, stale-epoch discard, checkpoint→full switch, budget-floor invariant
enforced, in-flight bounded by budget, shutdown drain.

Scoped-specific:
- **S1 fill-parity** — under the same recorded trace, the download fetches ≈ the same depth ahead as
  the baseline sequencer (compare reorder bytes / in-flight bytes vs baseline) — A/B against `main`.
- **S2 frontier-from-watch** — a `set_finalized_tip` change produces exactly one `FrontierAdvance`,
  and the resulting `release_applied_through` frees exactly the `held_until_durable` bytes ≤ durable
  — mock `LatestChainTip` + `ByteBudget::audit` (seam: chain-tip watch).
- **S3 no apply-window** — submission depth is governed solely by the byte budget; the
  `checkpoint_in_flight`/`full_in_flight` fields no longer exist — compile + behavior test.
- **S4 reactor untouched** — CI diff-scope gate: the PR does not modify `reactor.rs`,
  `peer_routine.rs`, `work_queue.rs`, or `peer_registry.rs`.

## Phasing

1. **Seam.** Add `ApplyItem`, `applyQ`, the `held_until_durable` ledger, and the durable-watch
   `FrontierAdvance` shim. Behind a flag; nothing consumes it yet.
2. **Committer.** Implement the plain-select Committer; A1–A9 green against a test producer
   (incl. the batching mock verifier).
3. **Swap the commit tail.** Sequencer drains → applyQ; delete submit/apply-window/in_flight; delete
   the 200 ms poll; wire the durable-watch `FrontierAdvance`. S1–S4 + A* green.
4. **Cutover.** Remove the executor `apply`/`refresh_checkpoint_frontier` seam; workspace gates;
   real-stack bench (`cargo xtask zakura-commit-bench -- run`) vs baseline.

## Critical files (small surface)

- **New:** `committer.rs`, `apply_item.rs`; a durable-tip→`FrontierAdvance` watcher in
  `zebrad/src/commands/start/zakura/block_sync_driver.rs`.
- **Changed:** `sequencer_task.rs` (commit tail only), `sequencer.rs` (repurpose `applying` to a
  bytes-only `held_until_durable` ledger; keep tip/floor/release), `events.rs` (drop the executor
  seam; add `ApplyItem`/`CommitterReset`), `block_sync_driver.rs` (watch shim; drop the refresh
  path), `config.rs` (budget-floor invariant in `validate()`).
- **UNCHANGED:** `reactor.rs`, `peer_routine.rs`, `work_queue.rs`, `peer_registry.rs`, `reorder.rs`,
  `admission.rs`, serving.

## Verification

Apply-half + scoped property catalog above; the **S4** diff-scope CI gate proving the networking
layer is untouched; `cargo fmt`/`clippy -D warnings`/`cargo test --workspace`; real-stack bench vs
baseline confirming continuous durable-tip advance (no 200 ms saw-tooth), writer near-saturated, and
RSS bounded by `max_inflight_block_bytes`.

---

# Implementation status & file-by-file execution plan

> Authored after landing Phases 1–2. Build env note: `librocksdb-sys` needs
> `CXXFLAGS="-include cstdint"` on GCC 15 (the rocksdb 8.10 vendored headers miss `<cstdint>`).
> Set it for any `cargo` invocation that touches `zebra-state`/`zebrad`.

## DONE (compiling + tested)

**Phase 1 — apply seam (zebra-network).**
- `block_sync/apply_item.rs` (new): `pub struct ApplyItem { height, hash, block: Arc<Block>, bytes, source_peer, epoch }`, `pub enum CommitRejection { Invalid, TimedOut }`, `pub struct CommitterReset { height, epoch, source_peer, rejection }`. Re-exported in `block_sync/mod.rs` and out via `zakura::*`.
- `sequencer_task.rs`: added `SequencerControlInput::CommitRejected(CommitterReset)` + `handle_commit_rejected`. **Current guard is the interim "is-live" form** (`height > verified_tip && applying_hash(height).is_some()`); Phase 3 replaces it with the per-height epoch guard once the held ledger stores epochs (see below). It reuses `release_applying_blocks_from` / `reset_floor_below` / `drop_reorder_from` / `work.reset_above` and emits `Misbehavior` only for `Invalid`.
- `state.rs`: `BlockSyncHandle::report_commit_rejected(reset)` → sends `SequencerControlInput::CommitRejected` over `routine_wiring.sequencer_control`.

**Phase 2 — Committer (zebrad).**
- `commands/start/zakura/committer.rs` (new): `Committer<BlockVerifier>`, a plain-`select!` pump (`apply_rx` recv / `in_flight` `FuturesUnordered` / shutdown) that **fires `Request::Commit` without awaiting** and reuses `commit_block_sync_body_with_stall_trace`. Rejections raise one `CommitterReset` through a `CommitRejectSink` trait (impl'd for `BlockSyncHandle`; tests inject a recorder). `run()` returns the committed marker. `last_reset_epoch` starts at 0 and item epochs are **1-based**, so initial items are never discarded.
- Widened `commit_block_sync_body_with_stall_trace` + `block_apply_class_label` to `pub(super)` in `block_sync_driver.rs`; added `mod committer` + the missing `block_roots_cover_range`/`root_covered_query_best_header_tip` test re-exports in `zakura/mod.rs`.
- **6 A-tests pass** (`cargo test -p zebrad --lib …committer`): A1 concurrent-fire/no-deadlock (a `Barrier(n)` batching verifier — a serial committer hangs it), A3 zero-reads (structural: the Committer holds no `ReadState`), A4 reset attribution, A5 stale-epoch discard, A6 class switch, A9 shutdown drain, + happy path.

## Phase 3 — the cutover (NOT started; tree-breaking until complete)

### Held-ledger design (the one subtlety that drives several files)
The block `Arc` moves to the applyQ/Committer, so the Sequencer's `applying` map becomes a
**bytes + metadata** ledger, not a block store. The reset logic still calls `applying_hash`,
`applying_previous_block_hash`, and `has_buffered_at_or_above`, so the ledger entry must capture, **at
drain time** (when the block is still in hand): `{ bytes, hash, previous_block_hash, epoch }`.
`source_peer` is no longer needed in the ledger — reject attribution now travels on the `ApplyItem`
and comes back in the `CommitterReset`.

### Epoch model (finalize the Phase-1 interim)
- Sequencer owns `apply_epoch: u64` (start at **1**), stamped onto every `ApplyItem` **and** recorded
  in the held-ledger entry. Bump it on every rollback: a processed `CommitRejected` and a destructive
  `handle_frontier_reset`.
- `handle_commit_rejected(reset)`: process iff `held_epoch(height) == reset.epoch` and
  `height > verified_tip`; then drop `[height..]`, roll the floor, release bytes, score `Invalid`.
  Per-height-epoch (not a global `apply_epoch == reset.epoch`) is required so multiple distinct
  in-range failures each roll back **lowest-wins** while a stale completion from a superseded
  generation (height re-pushed at a newer epoch) is ignored. (Checkpoint ranges never `Err` — the
  verifier re-queues and waits, `checkpoint.rs:1032-1071` — so multi-failure is only the full path or
  verifier-drop on shutdown; correctness still required.)

### File-by-file

1. **`sequencer.rs`** — repurpose `applying` → `held_until_durable: BTreeMap<Height, HeldEntry{bytes,hash,prev_hash,epoch}>`. **Delete** the submit/token half: `submitted_applies`, `next_apply_token`, `SubmitItem`, `prepare_submit`, `submittable_heights`, `unsubmit`, `record_submitted_apply`, `decrement_submitted_apply`, `clear_submitted_applies_*`, `has_submitted_apply`, `submitted_has_only_other_hashes`, `applying_token_hash`, `submitted_apply_limit`, `submitted_applying_*`, `unsubmitted_applying_count`, `lowest_submitted_height`. **Rename** `drain_ready_into_applying` → `drain_ready_into_held` returning the drained `(height,hash,prev_hash,bytes)` tuples for the task to push to applyQ (carrying the block out before it is stored). Keep `accept_buffered_body`, `advance_verified_tip`, `release_applied_through`, `release_applying_blocks_from`, `reset_floor_below`, `drop_reorder_from`, `reset_to`, floor/tip, reorder accessors, `applying_hash`/`applying_previous_block_hash`/`has_buffered_at_or_above` (now reading the ledger).
2. **`sequencer_task.rs`** — add fields `apply_tx: mpsc::Sender<ApplyItem>` and `apply_epoch: u64`. Rewrite `release_contiguous_blocks` to drain held → stamp `apply_epoch` → `apply_tx.try_send(ApplyItem)` (never blocks; the byte budget bounds it). **Delete**: `in_flight_applies`, `checkpoint_in_flight`/`full_in_flight`, `apply_executor`(+rx), `install_apply_executor_if_ready`, `should_watch/replace_apply_executor`, `submit_pending_blocks`, `can_submit_class`, `increment/decrement_in_flight_apply_count`, `observe_apply_completion`, `process_apply_completion`, `poll_ready_apply_completion`, `handle_apply_finished`, `CheckpointFrontierRefresh`+`process_checkpoint_refresh`+`refresh_due`+the 200 ms select arm + the two refresh constants, `SubmittedBlockApply`, `ReadySource::{ApplyCompletion,ApplyExecutor,CheckpointRefresh}` (collapse to `{Control,Body}`), the `#[cfg(test)]` `drive_apply_completion`/`drive_checkpoint_refresh_advance`. Update `SequencerView`/`initial_view`/`publish_view` to drop submit/in-flight fields and audit `reserved + reorder + held + body_input`. Refine `handle_commit_rejected` to the per-height epoch guard; bump `apply_epoch` on it and in `handle_frontier_reset`.
3. **`reactor.rs` `spawn_block_sync_reactor`** — `let (apply_tx, apply_rx) = mpsc::channel::<ApplyItem>(APPLYQ_CAP)` (generous count cap; budget is the real bound). Pass `apply_tx` to `SequencerTask::new`. Remove the `apply_executor` watch + the `#[cfg(test)]` `ImmediateTestBlockApplyExecutor` install. Stash `apply_rx` for the handle (see 4).
4. **`state.rs` `BlockSyncHandle`** — drop the `apply_executor` field; add `apply_queue_rx: Arc<std::sync::Mutex<Option<mpsc::Receiver<ApplyItem>>>>` + `pub fn take_apply_queue(&self) -> Option<Receiver<ApplyItem>>` (Clone handle can't hold a `Receiver` directly → take-once Mutex). Add `pub fn report_durable_frontier(&self, frontiers: BlockSyncFrontiers)` → `SequencerControlInput::FrontierAdvance { frontiers, release_applied: true }`. Delete `install_block_apply_executor` + `replace_block_apply_executor_for_test`.
5. **`events.rs`** — delete `BlockApplyExecutor`, `BlockApplyExecutorPort`, `BlockApplyRequest`, `BlockApplyOutput`, `BlockApplyLimits`, `BlockApplyToken`; drop `BlockSyncEvent::TestApplyDone` and `BlockSyncAction::ApplySubmitted`. **Keep** `BlockApplyResult` + `BlockApplyClass` (the Committer reuses them). Update the `mod.rs` `pub use events::{…}`.
6. **`block_sync_driver.rs`** — delete `ZebradBlockApplyExecutor`(+impl), `apply_block_sync_body_to_output`, `apply_block_sync_body`, `refresh_block_sync_frontiers_for_checkpoint_window`, `query_checkpoint_refresh_frontiers`, the executor install/limits in `drive_block_sync_actions`. **Keep** `commit_block_sync_body_with_stall_trace`, `block_commit_result`, `block_apply_class`, `block_sync_needed_blocks_from_state` & the query path. **Add** `drive_block_sync_durable_frontier(chain_tip_change, read_state, block_sync, trace, shutdown)`: on each `ChainTipChange::wait_for_tip_change`, read `FinalizedTip`+`Tip`, derive `BlockSyncFrontiers` (relocate `verified_block_tip_from_state` use), call `block_sync.report_durable_frontier(..)`.
   - **Durable-watcher rationale:** `mirror_zakura_full_block_commits` already turns `ChainTipChange` → endpoint frontier → reactor `handle_state_frontiers_changed` → `FrontierAdvance`. The dedicated watcher injects `FrontierAdvance` **directly** so the checkpoint frontier advance is guaranteed independent of the mirror/endpoint path (the original reason the 200 ms poll existed); it is idempotent with the mirror path via the sequencer's `verified_block_tip < verified_tip()` stale guard.
7. **`commands/start.rs`** — after `spawn_block_sync_reactor`: `let apply_rx = block_sync.take_apply_queue().expect(..)`, build `Committer::new(apply_rx, block_verifier.clone(), Arc::new(block_sync.clone()), max_checkpoint_height, trace, throughput_probe)`, `tokio::spawn(committer.run(shutdown))`; `tokio::spawn(drive_block_sync_durable_frontier(chain_tip_change.clone(), read_state.clone(), block_sync.clone(), trace, shutdown))`. Remove the `install_block_apply_executor` / `ZebradBlockApplyExecutor` production wiring. Fix the 5 `ZebradBlockApplyExecutor::new` **test** sites (≈2612–2848) — port to a `Committer` + applyQ harness or delete.
8. **`config.rs` (Phase 4)** — `validate()`: reject `max_inflight_block_bytes < MAX_CHECKPOINT_BYTE_COUNT + headroom`; define `MAX_CHECKPOINT_BYTE_COUNT` (one checkpoint range's worst-case bytes). This is the budget-floor invariant the old ~401 apply-window encoded (A7).

### Test churn (the bulk — ~2,000 LOC across two crates)
`zebra-network/.../tests.rs` references to retire/rewrite onto applyQ+Committer: `ApplySubmitted` ×59, `apply_executor` ×34, `BlockApplyExecutor` ×30, `BlockApplyOutput` ×20, `TestApplyDone` ×10, `submittable_heights` ×8, `refresh_checkpoint`/`BlockApplyRequest` ×4, `drive_apply_completion` ×3. Strategy: white-box apply-window/executor/completion tests either move to the Committer (zebrad) or assert the new applyQ-push + held-ledger behavior; the durable-advance tests assert `report_durable_frontier` → `release_applied_through` (S2). Add the **S4** diff-scope CI gate (`reactor.rs`/`peer_routine.rs`/`work_queue.rs`/`peer_registry.rs` untouched).

---

## Phase 3 & 4 — LANDED (cutover complete, green on both crates)

Phases 3 (cutover) and the Phase-4 cleanup landed on `evan/perf-plus-download-fixes`. Net diff
**≈ −5,550 lines** (1,333 added / 6,883 deleted), matching the projected "replacement, not addition."

**Production (all per the file-by-file plan above):**
- `sequencer.rs` — `applying` repurposed to the `ApplyingEntry { bytes, hash, prev_hash, epoch }`
  held ledger; the whole submit/token half deleted; `drain_ready_into_applying(epoch) ->
  Vec<DrainedBlock>` carries the block `Arc` out; `advance_verified_tip` now also returns the
  `committed_blocks`/`committed_bytes` it made durable (for throughput); `reset_to(new_tip)` lost
  the `keep_submitted` arg.
- `sequencer_task.rs` — drains onto the **unbounded** applyQ (`apply_tx.send`, never blocks/drops;
  the byte budget is the real bound); apply-window/executor-watch/200ms-refresh/commit-progress all
  deleted; `apply_epoch` (1-based) stamped on every drain, bumped on a processed `CommitRejected`
  and a destructive `reset_to`; `handle_commit_rejected` guard is now per-height
  (`applying_epoch(h) == reset.epoch`); committed throughput recorded on the durable advance via
  `ThroughputMeter::record_n`. 2-source (`Control`/`Body`) fair rotation + the kept shed backstop.
- `committer.rs` — applyQ is `mpsc::UnboundedReceiver`; reset coalescing is **height-aware**
  (`last_reset_height`): within one generation only a strictly-lower failure raises a reset, so
  several invalid blocks in one contiguous full-path range roll back lowest-wins (the Sequencer's
  per-height guard dedups the rest). `CommitMeta` trimmed to `{height, source_peer, epoch}`.
- `events.rs`/`mod.rs` — executor seam types + `TestApplyDone`/`ApplySubmitted` deleted; kept
  `BlockApplyResult`/`BlockApplyClass`/`BlockApplyToken`.
- `reactor.rs` — applyQ channel created + `apply_tx` threaded to `SequencerTask::new`; executor
  watch + `ImmediateTestBlockApplyExecutor` gone; the 4 dropped `SequencerView` fields removed from
  the state-trace + gauges.
- `state.rs` — `apply_queue_rx: Arc<Mutex<Option<UnboundedReceiver<ApplyItem>>>>` + `take_apply_queue`
  + `report_durable_frontier`; `ThroughputMeter::record_n`; executor install/replace deleted.
- `block_sync_driver.rs` — `ZebradBlockApplyExecutor` + apply/refresh fns deleted; `drive_block_sync_actions`
  slimmed to the state-read seam; new **`drive_block_sync_durable_frontier`** watcher (subscribes
  `ChainTipChange`, `query_block_sync_frontiers` → `report_durable_frontier`).
- `start.rs` — spawns the `Committer` + the durable watcher; old executor wiring removed.

**Test churn (~6,300 deleted / rewired):** the executor test harness + ~38 apply-window/executor/
completion/refresh tests deleted across `tests.rs` (zebra-network) and `start.rs` (zebrad); ~22
download/serving/reorder tests **rewired onto the applyQ** (`take_apply_queue` + a synthetic
`report_durable_frontier` driver, replacing `ApplySubmitted`/`TestApplyDone`); the two testkit mock
drivers (`cluster.rs`, `mock_blocksync.rs`) rewired the same way. Apply-side correctness now lives
in the `Committer` A-tests (zebrad) + the `Sequencer` unit tests.

**Gates:** `cargo fmt` clean; `cargo clippy --all-targets` clean (0 warnings) on both crates;
**549 zebra-network `zakura::` tests pass / 0 fail**; **61 zebrad `commands::start` tests pass / 0
fail** (incl. the 6 Committer A-tests). Build env: `CXXFLAGS="-include cstdint"` (GCC 15 vs rocksdb).

**A7 budget-floor `config.validate()` invariant — LANDED (bug review, 2026-06-26).**
The earlier "~4 GB worst-case is too strict" note was **wrong** (it assumed a far larger gap).
`MAX_CHECKPOINT_HEIGHT_GAP = 400`, so a full worst-case range is `401 × MAX_BLOCK_BYTES (2 MB) ≈
802 MB` — comfortably under the 6 GiB default. The invariant is therefore shippable and was added:
new `pub const BS_CHECKPOINT_RANGE_BYTE_FLOOR` (config.rs) + a `validate()` check rejecting
`max_inflight_block_bytes < one full checkpoint range` (a smaller budget can never hold a
partially-submitted range → the verifier never resolves → nothing durable → no release → deadlock),
with test `config_validate_requires_a_full_checkpoint_range_budget`. (Note: `validate()` is not yet
*called* on the production config-load path — a separate pre-existing gap; small-budget reactor
tests construct configs directly and bypass it, so they are unaffected.)

**Deliberately deferred (documented, not blocking):**
- **E2 real-stack bench** (`cargo xtask zakura-commit-bench -- run`) — needs a live stack; run
  separately to confirm the continuous-durable-advance throughput win vs the 200ms saw-tooth.
- **S4 diff-scope CI gate** — `reactor.rs` was minimally touched (the unavoidable spawn-wiring:
  applyQ channel + `apply_tx`), so the literal "reactor untouched" gate from the scoped plan does
  not apply; `peer_routine.rs`/`work_queue.rs`/`peer_registry.rs` are untouched.

**Post-checkpoint-sync follow-ups** — known gaps that are unreachable on the checkpoint path
(finalized-tip monotonicity makes the apply seam self-healing) but become real on the reorg-able
full-sync path. Tracked in [`post_checkpoint_sync_followups.md`](./post_checkpoint_sync_followups.md):
the `Duplicate`-result frontier refresh (this review's P2), the missing `FrontierAdvance` epoch
guard, the full-sync/fork reorg path (Phase 5), and the state-internal timers (Phase 6).
