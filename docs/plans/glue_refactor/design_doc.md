# Block-sync: bulletproof event-driven rebuild (no backstops)

> **This is the full event-driven north star.** It rebuilds the reactor/peer-routine/work-queue
> networking layer, which is currently too large to land through review. The **scoped, shippable**
> variant — apply-side seam only, download/reactor untouched, 200 ms poll → durable watch — lives in
> [`scoped_design_doc.md`](./scoped_design_doc.md). Keep this doc as the target architecture.

## Context

Speeding up Zakura block-sync has caused a long tail of compounding bugs. The root cause is
architectural: the pipeline is shot through with **backstop timers** — a 200 ms frontier-refresh
poll, a 500 ms floor-starvation shed tick, a 1 s central floor watchdog, a 50 ms retry-avoid
backoff, a 10 ms outbound-capacity poll, an 8 s metrics tick, and even a 10 ms `park_timeout`
self-poll inside the state write thread. Every one of these exists to *re-derive internal state
when the real event path didn't fire*. That is precisely what makes them dangerous: a backstop
**masks** the missing/buggy event path, so the bug never surfaces in testing and errors compound
in production. They are not safety nets; they are blindfolds.

This plan rebuilds block-sync to be **entirely event-driven**. The mental model is the Go program:

```go
applyQ := make(chan Block, N)         // producer pushes contiguous blocks
// consumer:
for block := range applyQ {           // blocks on the next genuine event
    batch = append(batch, block)
    if len(batch) == CHECKPOINT_RANGE { applyCheckpoint(batch); batch = batch[:0] }
}
```

The bounded channel *is* the backpressure; the producer already knows what it sent, so **no
completion feedback loop is needed at all**. For checkpoint sync it really is that simple.

### Can it be entirely event-driven? Yes, with exactly one irreducible exception.

Every wait becomes one of: a channel `recv`, a single future completion, a `tokio::sync::Notify`
wakeup, or a `watch` change. The **one** unavoidable timer is the **per-request network
deadline**: a QUIC connection can stay alive while a peer silently ignores one block request
forever, and only a deadline can detect that. This is not a backstop — it is the sole detector of
an external failure, and it *is itself an event* ("this peer broke its contract → re-request
elsewhere"), one `sleep` per outstanding request that fires once and is destroyed. Everything else
is deleted.

### Decisions (confirmed with the user)
1. **Byte-bounded** memory throughout (not block-count). One `ByteBudget` is the single
   end-to-end bound.
2. **Rework the download half too** — it holds most of the backstops. Lowest-first scheduling,
   byte-bounded reorder buffer, per-request deadline, **floor budget reserved as an invariant
   (no shed)**.
3. **Spec depth:** the checkpoint apply pipeline and the reworked download path are specified
   here at full function level. Header-sync, roots, serving, and the full-sync/fork path are
   specified at interface level (they largely exist; we specify how the new pieces plug in).
4. **Concurrency model:** independent event-driven routines + fine-grained state (mutex the
   specific shared state, queue+routine where order matters), NOT one single-owner select loop.

### Three global reconciliations (override the per-half design passes — read these first)
- **Release budget on the durable `set_finalized_tip` watch, not on `Request::Commit` Ok.**
  State holds its own `Arc` in an *unbounded* write queue until durable; releasing on Ok
  under-accounts memory by the write-queue depth and re-creates the OOM. Releasing on durable
  makes the `ByteBudget` a true total-memory bound no matter where a block sits (reorder / applyQ
  / write queue). The **Assembler** does the release (it owns `held_until_durable` and subscribes
  to the durable watch for GC). This is event-driven (a watch change), never a poll.
- **Many small event-driven routines + fine-grained state, NOT one round-robin owner.** Rather than
  a single task multiplexing every event source (which needs a hand-rolled anti-starvation
  round-robin), use **N independent routines, each blocked on its OWN event source**, and let the
  tokio scheduler provide fairness — a flood on one routine's source cannot starve another routine.
  Shared state uses the narrowest mechanism (see §Concurrency model): lock-free where possible (the
  `ByteBudget` is already atomic + `Notify`); a brief `parking_lot::Mutex` for simple shared maps
  (`NeededSet`), **never held across an `.await`**; and a **queue + single owning routine** where
  mutation must be serialized/ordered (the reorder→applyQ assembly). After any state mutation, wake
  the affected routines immediately via `Notify` (event-driven), never a poll. This supersedes both
  the design pass's single-owner task and an earlier round-robin draft.
- **Concurrent checkpoint submission, NOT serial — and a hard budget-floor invariant.** The
  checkpoint verifier (`checkpoint.rs` `process_checkpoint_range`) resolves a block's `Request::Commit`
  oneshot **only after the entire contiguous range to the next checkpoint has been submitted**
  (it batch-verifies the range, then sends `Ok` on every block's channel). Therefore the Committer
  must **fire every block up to the next checkpoint without awaiting**, holding the futures in a
  `FuturesUnordered` and draining completions — a serial "await each Ok" loop **deadlocks**. The
  in-flight depth is bounded by the `ByteBudget` (every in-flight commit is a reserved+held block),
  so no separate apply-window is needed — BUT this imposes a hard invariant:
  **`max_inflight_block_bytes` ≥ the largest checkpoint range's total bytes + headroom**
  (`MAX_CHECKPOINT_BYTE_COUNT`), enforced by `config.validate()`. A budget smaller than one
  checkpoint range cannot hold a full range in flight, so the verifier never completes the batch →
  permanent deadlock. This is the invariant the old ~401 apply-window silently encoded.

---

## Architecture

Routines (independent tasks) and the channels/watches/shared-state between them. The "download
half" is not one task — it is N `PeerRoutine`s + an `Assembler` + a `NeededProducer` sharing a
`NeededSet` (Mutex) and the lock-free `ByteBudget` (see §Concurrency model).

```
  HeaderSync (exists) --watch (best_header_tip)--> NeededProducer --extend--> [NeededSet: Mutex]
  Peers <--frames--> PeerRoutine×N  --take/return--> [NeededSet]  --reserve/release--> (ByteBudget: lock-free)
  PeerRoutine×N  --received: mpsc--> Assembler
  State write thread --watch (durable_tip = set_finalized_tip)--> Assembler (release+GC+floor) + Serving + HeaderSync-bound
  Assembler  --applyQ: mpsc<ApplyItem> (ascending, gap-free)--> Committer
  Assembler  <--mpsc<CommitterReset>-- Committer               (rejection only)
  Committer  --Request::Commit(Arc<Block>)--> BlockVerifier/State
  Committer  --mpsc<BlockSyncAction::Misbehavior>--> supervisor
  work_available: Notify   pinged by NeededProducer/Assembler/PeerRoutine on NeededSet growth
```

**The single byte budget** (reuse `ByteBudget`, `zebra-network/src/zakura/transport/guard.rs:37`).
Lifecycle of one block's reservation:
`reserve(size)` at request-send (a PeerRoutine), where `size` is the **header-provided body size**
(`BlockSizeEstimate::Confirmed` from committed metadata, or `Advertised` from header-sync — NOT a
blanket worst-case; `MAX_BLOCK_BYTES` is used only for the rare `Unknown` height) →
`settle(reserved, actual)` on receipt (deviation beyond tolerance is `SizeMismatch`; a body must
hash to the committed header hash) → **held** through reorder + applyQ + the state write queue →
`release(actual)` by the Assembler when the durable watch crosses that height. Exhaustion is the backpressure: a full budget stops
the PeerRoutines from reserving → they stop requesting → peers backpressure; the durable-driven
`release` wakes `subscribe_capacity()` and the PeerRoutines resume. Because release is gated on
durability, the budget bounds *all* in-memory blocks end to end.

**Floor-reserve invariant (replaces the shed entirely):** split the budget conceptually into a
`floor_reserve` slice usable **only** by the floor and a small near-floor band, plus a
`general` pool for everything above. Above-floor admission draws only from
`available().saturating_sub(floor_reserve)`. Combined with lowest-first scheduling, the floor is
**always** either in-flight (with a live deadline) or pending-and-fundable. No eviction, no shed,
ever.

---

## Concurrency model

No single owner task, no hand-rolled round-robin. Instead: **one routine (task) per event source**,
each blocked only on its own source, with shared state held behind the narrowest synchronization.

| Shared state | Mechanism | Who touches it |
|---|---|---|
| `ByteBudget` | **lock-free** (atomics + `Notify`, `guard.rs`) | every routine reserves/releases |
| `NeededSet` (pending / in-flight heights) | `Arc<parking_lot::Mutex<..>>`, brief synchronous holds | peer routines (take/return), Assembler (gc/return) |
| reorder + `applyQ` + `sent_tip` + `epoch` + `held_until_durable` | **single owning routine** (Assembler), fed by queues + the durable watch — no lock | Assembler only |
| per-peer outbound + outstanding requests + their deadlines | owned by each **PeerRoutine** — no sharing | that peer's routine |

Rules (these are the bug-prevention invariants):
1. **Never hold a lock across an `.await`.** Every `NeededSet` lock is a brief synchronous
   BTreeMap operation; all `.await`s are either lock-free (`ByteBudget`, channels) or on owned
   state (the Assembler's own).
2. **Wake, don't poll.** After any state mutation that could unblock another routine, fire a
   `Notify`: `budget.release()` already calls `notify_waiters()`; add a `work_available: Notify`
   pinged when `NeededSet` grows or heights return. A routine re-checks state only when notified.
3. **Serialize via a routine, not a lock, when order matters.** The reorder→applyQ production must
   be strictly ordered (single producer, contiguous), so it is owned by one routine (the
   Assembler) fed by a `received` queue — not a mutex that two peer routines could interleave.
4. **Plain `select!` (random fairness), never `biased;`.** Within a single routine the arms are
   few and not mutually-starving (e.g. a peer's inbound frames are bounded by its own outstanding
   requests, and its deadline only matters when frames are *not* arriving). Cross-routine fairness
   is the tokio scheduler's job.

This is the existing peer-routine + work-queue shape (`peer_routine.rs` + `work_queue.rs` +
`reactor.rs`), **cleaned of its backstops** — not a from-scratch single-owner rewrite.

---

## The seam: `ApplyItem` + `applyQ`

```rust
/// One contiguous, hash-verified block ready to commit.
/// Items are strictly ascending and gap-free, starting at durable_tip + 1.
pub(crate) struct ApplyItem {
    pub height: block::Height,
    pub hash: block::Hash,          // already checked == committed header hash at receipt
    pub block: Arc<block::Block>,
    pub bytes: u64,                 // actual serialized size (settled at receipt)
    pub source_peer: ZakuraPeerId,  // for misbehavior attribution on commit failure
    pub epoch: u64,                 // reset generation; Committer discards stale epochs
}
// applyQ = tokio::sync::mpsc::channel::<ApplyItem>(APPLYQ_CAP)
//   APPLYQ_CAP is a generous COUNT cap only; the ByteBudget is the real bound, so the
//   producer exhausts budget before applyQ fills and never blocks on send.
```

The Assembler is the sole producer; the Committer the sole consumer. Contiguity and hash-validity
are guaranteed by the producer, so the consumer needs no feedback channel back — the only
back-edge is the rare `CommitterReset` on an invalid block (near-impossible on the checkpoint
path; see §Apply).

---

## Download half — full function-level spec

Three kinds of routine + shared state, per the concurrency model above. No single owner, no
round-robin. This is `peer_routine.rs` + `work_queue.rs` cleaned of backstops.

### Shared state (constructed once, cloned into the routines)

```rust
budget:        ByteBudget,                              // lock-free (guard.rs); floor-reserve split
needed:        Arc<Mutex<NeededSet>>,                   // parking_lot; brief synchronous holds only
work_available: Arc<Notify>,                            // pinged when `needed` grows / heights return
floor:         Arc<AtomicU64>,                          // durable_tip + 1, written by Assembler
header_tip:    watch::Receiver<(block::Height, block::Hash)>,  // target (HeaderSync)
durable_tip:   watch::Receiver<ChainTipData>,           // state set_finalized_tip
received_tx:   mpsc::Sender<Received>,                  // PeerRoutine -> Assembler (the serializer)
apply_tx:      mpsc::Sender<ApplyItem>,                 // Assembler -> Committer (the seam)
reset_rx:      mpsc::Receiver<CommitterReset>,          // Committer -> Assembler (invalid block)
actions:       mpsc::Sender<BlockSyncAction>,           // Misbehavior
read_state:    ReadStateService,                        // fetch (hash,size) for needed heights

/// Work-set: replaces WorkQueue. The per-height byte reservation lives in the InFlight entry
/// (in-flight) or travels with the body (received); no separate ledger.
struct NeededSet { pending: BTreeMap<block::Height, BlockSyncBlockMeta>,
                   in_flight: BTreeMap<block::Height, InFlight> }
struct InFlight { peer: ZakuraPeerId, req_id: ReqId, bytes_reserved: u64 }
/// A settled body handed from a PeerRoutine to the Assembler.
struct Received { height: block::Height, hash: block::Hash, raw: Arc<[u8]>, bytes: u64,
                  source_peer: ZakuraPeerId }
struct CommitterReset { height: block::Height, epoch: u64, source_peer: ZakuraPeerId }
```

### Routine 1 — `PeerRoutine` (one task per connected peer)

Owns its own peer state; shares only `budget`, `needed`, `work_available`, `floor`, `received_tx`.
The **per-request deadline (the one timer) is peer-local** — no central scan.

```rust
struct PeerRoutine {
    peer: ZakuraPeerId,
    session: BlockSyncPeerSession,
    inbound: FramedRecv,
    outbound: FramedSend,                 // + reserve_owned() accessor (see reuse-enabler)
    servable_range: Option<(block::Height, block::Height)>,
    outstanding: BTreeMap<ReqId, Outstanding>,   // heights + per-request deadline
    deadlines: DeadlineSet,               // earliest-deadline sleep over `outstanding`
    avoid: BTreeSet<block::Height>,       // heights this peer failed (event-cleared, NOT timed)
    pending_send: Option<GetBlocks>,      // a request parked on outbound capacity
    /* + clones of the shared state */
}

async fn run(mut self) {
    loop {
        tokio::select! {                  // plain select; arms are not mutually-starving
            frame = self.inbound.recv()                       => self.on_frame(frame)?,
            _ = self.deadlines.sleep()                        => self.on_deadline(),
            _ = self.work_available.notified()                => self.try_request(),
            _ = self.budget.subscribe_capacity().notified()   => self.try_request(),
            _ = self.outbound_ready(), if self.pending_send.is_some() => self.flush_send(),
            _ = self.shutdown.cancelled()                     => break,
        }
    }
}
```

```rust
fn on_frame(&mut self, frame: Option<Frame>) -> Result<(), Disconnect>;
//   None => disconnect (returns its outstanding heights to `needed`, pings work_available, exits).
//   Some(Block) => on_block_received.
fn on_block_received(&mut self, height, hash, raw: Arc<[u8]>);
//   1) verify hash == outstanding hash (else Misbehavior::InvalidBlock, drop, do NOT count);
//   2) budget.settle(reserved, actual) — reservation now travels with the body;
//   3) received_tx.try_send(Received{..}) — hand to the Assembler (serialized production);
//   4) remove from outstanding + deadlines; budget is NOT released here (Assembler holds it).
fn on_deadline(&mut self);
//   The one timer fired: for each expired ReqId, lock `needed`, return un-received heights to
//   pending, add them to `self.avoid`, drop the outstanding+deadline, ping work_available. Repeated
//   expiries reduce the outbound window / score Misbehavior / disconnect (existing policy).
fn try_request(&mut self);
//   Lock `needed`; floor-reserve-aware take of the lowest contiguous run within `servable_range`,
//   skipping `self.avoid`: a run at the floor may use full budget.try_reserve; a run above the
//   floor only `available().saturating_sub(FLOOR_RESERVE)`. Move heights pending->in_flight with
//   the reservation; build GetBlocks; send (or park in `pending_send` on outbound-full); register
//   per-request deadline. Unlock before any await.
fn flush_send(&mut self);
//   Outbound capacity freed (reserve_owned permit): send `pending_send`.
```

### Routine 2 — `Assembler` (single task; owns all serialized progress state)

The only writer of reorder, applyQ, `sent_tip`, `epoch`, `held_until_durable`, and `floor`. Fed by
the `received` queue and the durable watch — **no lock on its own state** (single owner).

```rust
struct Assembler {
    reorder: ReorderBuffer,                          // reuse reorder.rs
    sent_tip: block::Height,                         // highest contiguous height pushed to applyQ
    epoch: u64,                                       // stamped onto ApplyItems
    held_until_durable: BTreeMap<block::Height, u64>, // bytes to release on durable advance
    received_rx: mpsc::Receiver<Received>,
    durable_tip: watch::Receiver<ChainTipData>,
    reset_rx: mpsc::Receiver<CommitterReset>,
    apply_tx: mpsc::Sender<ApplyItem>,
    /* + budget, needed, work_available, floor clones */
}

async fn run(mut self) {
    loop {
        tokio::select! {
            Some(r)  = self.received_rx.recv() => self.on_received(r),
            Ok(())   = self.durable_tip.changed() => self.on_durable_advance(),
            Some(rs) = self.reset_rx.recv()    => self.on_reset(rs),
            _ = self.shutdown.cancelled()      => break,
        }
    }
}
```

```rust
fn on_received(&mut self, r: Received);
//   reorder.insert_body(RawFramePayload, bytes); record bytes in held_until_durable; then
//   drain_contiguous_prefix(sent_tip): for each, decode + apply_tx.try_send(ApplyItem{epoch});
//   advance sent_tip. try_send never blocks (budget bounds applyQ; see seam).
fn on_durable_advance(&mut self);
//   d = durable_tip.borrow().height. Release Σ held_until_durable bytes <= d via budget.release
//   (THE byte release; wakes peer routines' subscribe_capacity); drop those entries;
//   reorder.drop_through(d); floor.store(d+1); lock `needed` gc_below(d+1); ping work_available.
fn on_reset(&mut self, reset: CommitterReset);
//   If reset.epoch == self.epoch: epoch += 1; sent_tip = reset.height-1; reorder.drop_from(height)
//   (release those held bytes); lock `needed` return in_flight/pending >= height to pending and
//   reconcile their reservations; ping work_available. New ApplyItems carry the bumped epoch.
```

### Routine 3 — `NeededProducer` (single task)

```rust
async fn run(mut self) {
    loop { tokio::select! {
        Ok(()) = self.header_tip.changed() => self.refresh_needed().await,
        _ = self.shutdown.cancelled() => break,
    }}
}
fn refresh_needed(&mut self) -> impl Future;
//   Read state for floor..=header_tip (MissingBlockBodies + HeadersByHeightRange + BlockSizeHints,
//   reusing the existing driver path); lock `needed`, extend pending with (hash,size); ping
//   work_available. The only place a state READ happens on the download side, and it is gated on
//   the header-tip watch event — not a poll.
```

### Shared helper types

```rust
// NeededSet (methods called under the Mutex; all synchronous)
fn extend(&mut self, metas: impl Iterator<Item = BlockSyncBlockMeta>);
fn take_lowest_run(&mut self, servable: (block::Height, block::Height), budget_for_run: u64,
                   max_count: usize, avoid: &BTreeSet<block::Height>)
    -> Option<(ReqId, Vec<(block::Height, block::Hash)>, u64 /*reserved*/)>; // one contiguous run
fn return_to_pending(&mut self, heights: impl IntoIterator<Item = block::Height>) -> u64; // bytes freed
fn gc_below(&mut self, floor: block::Height);
fn min_pending(&self) -> Option<block::Height>;

// DeadlineSet — peer-local, NO scan. Min-deadline sleep rebuilt only on add/remove.
fn insert(&mut self, req: ReqId, deadline: Instant);
fn remove(&mut self, req: ReqId);
fn sleep(&self) -> impl Future<Output = ()>;            // sleep_until(earliest) or pending()
fn take_expired(&mut self, now: Instant) -> Vec<ReqId>;
```

### One small reuse-enabler
`FramedSend` (`zebra-network/src/zakura/transport/io.rs`) exposes `send`/`try_send`/`capacity`. Add
a one-line `reserve_owned()` passthrough so a PeerRoutine awaits an outbound permit (event) instead
of the 10 ms `OUTBOUND_FULL_POLL_INTERVAL` poll. `reserve_owned` is already used at
`peer_routine.rs:404`.

### Floor liveness without a watchdog (proof sketch)
Floor = `floor.load()` = durable+1. It is rescued by an event in every stall mode, no scan:
(a) pending + budget free → a `work_available`/`budget capacity` Notify wakes a PeerRoutine →
`try_request` funds it from floor_reserve; (b) pending + budget full → the Assembler's durable
release frees floor_reserve and notifies; (c) in-flight, peer delivers → receipt; (d) in-flight,
peer black-holes → that peer's **deadline** fires → returns it to pending + pings work_available;
(e) the floor's peer disconnects → `on_frame(None)` returns it to pending; (f) no servable peer →
a peer-connect / header-tip event re-drives `try_request`. The floor_reserve invariant guarantees
(a)/(b) can always fund it.

---

## Apply half — full function-level spec (checkpoint pipeline)

A pump: for each `ApplyItem` from `applyQ`, **fire `Request::Commit` immediately** (do not await)
and push the future into a `FuturesUnordered`; concurrently drain completions. The checkpoint
verifier does the batching *internally* — the Go sketch's `applyCheckpoint(batch)` is realized by
the verifier's range batching, so the Committer is just the `for item := range applyQ { commit }`
pump. In-flight depth is bounded by the `ByteBudget` (every in-flight commit is a reserved+held
block), subject to the budget-floor invariant above (budget ≥ one checkpoint range). This is
structurally Plan A's `in_flight_applies` minus the frontier poll, the apply-window cap, and the
completion-feedback-to-producer.

### Types

```rust
struct Committer {
    apply_rx:  mpsc::Receiver<ApplyItem>,
    verifier:  BlockVerifierService,            // zebra_consensus::Request::Commit(Arc<Block>)
    max_checkpoint_height: block::Height,
    actions:   mpsc::Sender<BlockSyncAction>,   // Misbehavior
    reset_tx:  mpsc::Sender<CommitterReset>,    // -> Assembler (rejection only)
    commit_timeout: Duration,                   // ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT = 30s
    in_flight: FuturesUnordered<BoxFuture<'static, CommitOutcome>>, // fired, not-yet-resolved
    committed_marker: block::Height,            // highest committed (trace + contiguity assert)
    last_reset_epoch: u64,
    shutdown:  CancellationToken,
}
struct CommitOutcome { item_meta: (block::Height, block::Hash, ZakuraPeerId, u64),
                       result: Result<block::Hash, CommitError> }
```

### The run loop (single routine, plain select — NOT serial-await)

```rust
async fn run(mut self) {
    loop {
        if self.shutdown.is_cancelled() && self.in_flight.is_empty() { break; }
        tokio::select! {
            _ = self.shutdown.cancelled() => {}
            item = self.apply_rx.recv(), if !self.shutdown.is_cancelled()
                => self.on_item(item),                     // fire commit, push to in_flight; NO await
            Some(done) = self.in_flight.next(), if !self.in_flight.is_empty()
                => self.on_commit_done(done),              // handle Ok/Err
        }
    }
}
```
The Committer is a single consumer with no shared state, so a plain `select!` over its two genuine
event sources (a new item; a commit completing) suffices — random fairness, neither starves under
load. Zero polls: no timer arm, no durable-tip arm; the only `sleep` anywhere is `commit_timeout`
wrapping each commit future (a genuine external-operation timeout). Firing without awaiting is what
lets a full checkpoint range sit in flight so the verifier can resolve the batch. (If completion
handling ever grows, split it into a `commit_done` queue + a completion routine — the same
queue+routine pattern as the Assembler.)

### Methods (every signature + one-line doc)

```rust
fn on_item(&mut self, item: Option<ApplyItem>);
//   None => producer gone (begin drain). Some(item): discard if item.epoch <= last_reset_epoch;
//   else build the commit future and push to in_flight (FIRE, do not await):
//     timeout(commit_timeout, verifier.oneshot(Request::Commit(item.block)))
//       .map(|r| CommitOutcome{ item_meta: (height,hash,source_peer,bytes), result: r })
//   The Arc moves into the future; it is dropped when the future resolves (state holds its own).
fn on_commit_done(&mut self, done: CommitOutcome);
//   Ok(hash == meta.hash) => committed_marker = max(marker, height); trace progress. Budget is
//                            released later by the Assembler on the durable watch (NOT here).
//   Ok(other) | Err       => on_commit_error(meta).
fn on_commit_error(&mut self, meta: (block::Height, block::Hash, ZakuraPeerId, u64));
//   Emit ONE BlockSyncAction::Misbehavior{peer: source_peer, reason: InvalidBlock};
//   bump+send ONE CommitterReset{height, epoch: ++epoch, source_peer}; set last_reset_epoch.
//   In-flight futures for the failed range will resolve Err and are coalesced by the epoch guard
//   (already-reset => their CommitError is dropped, not re-reset). A write-thread
//   invalid_block_reset re-enters as an Err on a subsequent commit and funnels through this path.
fn classify(&self, height: block::Height) -> BlockApplyClass;
//   height <= max_checkpoint_height -> Checkpoint else Full (reuse block_apply_class, driver:502).
//   Checkpoint and Full both fire per-block Request::Commit; classify only gates the Full-path
//   behavioral differences (no reorg handling needed below the checkpoint boundary) and trace.
```

### Why this does not re-create unbounded write-queue growth
Firing concurrently lets the verifier resolve a range; resolved blocks go to the state write
thread's *unbounded* queue until durable. That queue is nonetheless bounded **because the byte
budget is released only on the durable watch** (the global reconciliation above): an in-flight or
write-queued block still counts against the budget, so the PeerRoutines stop feeding new bodies once
~the budget's worth is outstanding, regardless of where they sit. Total in-memory blocks ≤ budget,
end to end. The depth of "fired but not durable" is therefore bounded by the budget, which the
config invariant pins at ≥ one checkpoint range.

### Byte release (restated — the key memory invariant)
The Committer never touches the budget. The reservation travels with the `ApplyItem`; the
**Assembler** releases it in `on_durable_advance` when `set_finalized_tip` crosses the height.
Over-holding between `Ok` and durable is conservatively safe and *is* the correct backpressure
(download throttles to the write thread's true durability rate).

---

## Interface-level wiring

- **Header-sync (exists, unchanged):** `zebra-network/src/zakura/header_sync/` publishes the
  `watch::Receiver<(Height,Hash)>` best-header tip the download routines target, and persists
  optional `tree_aux_roots` ahead of bodies. Bound: header-sync may run arbitrarily ahead; the
  PeerRoutines only request within `floor..=header_tip`, and the byte budget bounds how much they
  pull.
- **Durable-tip watch (the event that replaces the 200 ms poll):** the state write thread's
  `chain_tip_sender.set_finalized_tip(tip)` (`write.rs:586`) fires `LatestChainTip`/`ChainTipChange`
  (`chain_tip.rs`). Consumers: Assembler (GC + byte release + floor advance), Serving (durable read
  boundary), header-sync bound. The Committer does **not** consume it.
- **Roots:** for a checkpoint block, the `tree_aux` roots the VCT fast path needs are already
  persisted by header-sync (running ahead) in CF `zakura_header_commitment_roots_by_height`
  (`commitment_aux.rs`, `PeerSource`). If a root is missing at commit, zebra-state currently
  *internally* waits (`VCT_ROOT_RETRY_WAIT` 500 ms / `VCT_AWAIT_SUCCESSOR_WAIT` 20 ms,
  `write.rs:56`). These are **state-subsystem internals, out of scope** for this rebuild — flagged
  as a follow-on to event-drive (a roots-available `Notify` from the header-sync persist path).
- **Serving (exists, simplified):** peers' GetBlocks are answered from `ReadRequest::BlocksByHeightRange`
  (finalized DB + best non-finalized chain). Serving keys off the **durable** tip watch, never the
  Assembler's `sent_tip`, so we never serve an uncommitted block.
- **Full-sync / fork path (interface level):** the SAME applyQ + Committer handle Full-class blocks
  — per-block `Request::Commit`, no batching; non-finalized/fork handling lives in zebra-state
  (`validate_and_commit_non_finalized`, `commit_block`/`commit_new_chain`). The one behavioral
  difference: the best tip can move **non-monotonically** on reorg (`set_best_non_finalized_tip`),
  surfaced as `ChainTipChange::Reset`; Serving and the Assembler handle it via the reset path
  (the Assembler rolls `sent_tip`/needed-set to the new tip). Specified in full as a later phase.
- **Checkpoint→full transition:** the consensus router routes `Request::Commit` for height ≤
  `max_checkpoint_height` to the checkpoint verifier (range-batched) and above it to the full
  verifier (per-block); the Committer fires uniformly and does not special-case the boundary in its
  hot path. The final checkpoint range resolves once all its blocks are in flight (same batch
  semantics); full blocks resolve independently per-block. The state side drops
  `finalized_block_write_sender` post-checkpoint (existing behavior). `classify()` is used only for
  trace and for gating the Full-path reorg behavior, not for routing.

---

## Backstops deleted → replacement event

| Deleted backstop | Today | Replaced by (genuine event) |
|---|---|---|
| `CHECKPOINT_FRONTIER_REFRESH_INTERVAL` 200 ms poll + `refresh_checkpoint_frontier` | sequencer_task.rs:34 | `set_finalized_tip` durable watch → Assembler release/GC |
| `FLOOR_STARVATION_SHED_INTERVAL` 500 ms shed | sequencer_task.rs:32 | floor-reserve invariant: floor always fundable, shed unreachable |
| `floor_watchdog_ticks` 1 s central scan | reactor.rs:313 | per-PeerRoutine `DeadlineSet` fire (peer-local, no scan) |
| `RETRY_AVOID_BACKOFF` 50 ms | peer_routine.rs:68 | per-PeerRoutine `avoid` set, cleared by a `work_available` event |
| `OUTBOUND_FULL_POLL_INTERVAL` 10 ms | peer_routine.rs:70 | `FramedSend::reserve_owned()` permit |
| `metrics_ticks` 8 s | reactor.rs:306 | trace-on-event at each handler tail |
| write thread `park_timeout(10 ms)` self-poll | write.rs:~432 | `blocking_recv()` on the write channel (state-internal; flag) |
| apply-window + completion feedback loop | sequencer_task.rs | deleted — bounded budget + Assembler owns contiguity |

The only survivors: **one `sleep_until(earliest)` per PeerRoutine** (covering that peer's
outstanding requests, rebuilt only on add/remove, never scanned) and the **`commit_timeout`** on
each `Request::Commit`. Both are external-operation deadlines — events, not backstops.

---

## Test-property catalog

Each as `name — assertion — deterministic method (seam to mock)`. All timing tests use
`tokio::time::pause()`; peers are `framed_channel` mocks; the verifier and state are mock services.

**Download half**
- D1 budget conservation — `budget.reserved() == Σ(in_flight reserved) + Σ(reorder bytes) + Σ(held_until_durable)` at every step — proptest of interleaved receive/durable/reset ledger + `budget.audit()` (seam: ByteBudget).
- D2 floor liveness under black-hole — floor commits within `request_timeout + ε` when its peer accepts-then-stalls — paused clock + mock peer that accepts then never sends; assert a re-request to a different peer (seam: two mock peers).
- D3 memory bound — total held bytes never exceed `max_inflight_block_bytes` under any delivery order — proptest shuffled delivery (seam: ByteBudget max).
- D4 applyQ contiguity — every emitted `ApplyItem` sequence is strictly ascending, gap-free, starts at durable+1 — assert on the applyQ receiver under shuffled receipt (seam: applyQ).
- D5 cross-routine no-starvation — flood one PeerRoutine's inbound; assert the Assembler still drains `received`, the Durable handler still releases budget, and another peer's deadline still fires — multi-task harness on a multi-thread runtime (seam: separate tasks).
- D6 floor-reserve isolation — above-floor `try_request` never reduces `available` below `floor_reserve` — drive budget to the boundary across peer routines, assert the floor is still reservable (seam: ByteBudget split + NeededSet take).
- D7 peer-disconnect reassignment — a disconnected peer's in-flight heights return to `needed.pending` and re-issue from another peer — drop a mock peer mid-flight (seam: FramedRecv close).
- D8 dedup idempotency — a duplicate body delivery releases its slack and does not double-count — deliver a height twice (seam: reorder + budget).
- D9 re-request peer-diversity — after a timeout, the same height is not re-requested from the failed peer — assert `avoid` honored across two peer routines (seam: two PeerRoutines).
- D10 no hidden poll — structural gate: the ONLY time primitives in download+apply are (1) the per-PeerRoutine `DeadlineSet` sleep and (2) the `commit_timeout`; grep for `interval`/`MissedTickBehavior`/`sleep`/the deleted constants finds nothing else — test/CI gate.
- D11 no lock across await — `clippy::await_holding_lock` (or a test) proves the `NeededSet` mutex guard is never held across an `.await`; the Assembler holds no lock on its own state — lint gate.

**Apply half**
- A1 batch-resolve no-deadlock — a mock verifier that withholds every `Ok` until the full contiguous range to the next checkpoint has been submitted still drives all blocks to committed; the Committer must have fired the whole range concurrently (a serial-await implementation hangs this test) — batching mock verifier (seam: BlockVerifier).
- A2 byte release exactly once — each committed height is released exactly once, on the durable watch, never on Ok — instrument release + a controllable mock durable watch (seam: durable watch + ByteBudget).
- A3 zero reads on hot path — committing N blocks performs zero `ReadState` calls and zero durable polls — panicking mock ReadState + instrumented `LatestChainTip` (seam: ReadState mock). *This makes reintroducing the frontier poll a test failure.*
- A4 reset attribution — an invalid block emits exactly one `Misbehavior{source_peer}` and one `CommitterReset{height, epoch+1}`; sibling Errs in the same failed range are coalesced by the epoch guard, not re-reset — mock verifier returns Err for a range (seam: BlockVerifier).
- A5 stale-epoch discard — items with `epoch <= last_reset_epoch` after a reset are dropped, not committed — feed stale items post-reset (seam: applyQ).
- A6 checkpoint→full switch — class switches exactly at `max_checkpoint_height` — drive heights across the boundary (seam: max_checkpoint_height).
- A7 budget-floor invariant enforced — `config.validate()` rejects `max_inflight_block_bytes < MAX_CHECKPOINT_BYTE_COUNT + headroom`; and with a budget exactly at the floor the pipeline commits a full range without deadlock — config unit test + the A1 harness at the boundary (seam: config + budget).
- A8 in-flight bounded by budget — the number of concurrently-fired commits never exceeds what the budget admits; no unbounded `FuturesUnordered` growth — concurrency probe + ByteBudget cap (seam: BlockVerifier + ByteBudget).
- A9 shutdown drain — on shutdown, in-flight commits resolve and the loop exits; no `ApplyItem` is dropped silently and the budget reconciles toward release on durable — cancel mid-flight (seam: shutdown token).

**End-to-end**
- E1 throughput-no-stall — under steady delivery, the durable tip advances continuously (no 200 ms/500 ms saw-tooth) — assert inter-commit gaps bounded by commit latency only (seam: full harness, paused clock).
- E2 real-stack — `cargo xtask zakura-commit-bench -- run` shows higher committed blk/s and no writer idle vs the backstop baseline.

---

## Phasing (build green and testable at each step)

1. **Seam + budget contract.** Add `ApplyItem`, `applyQ`, the `held_until_durable` release-on-durable
   path, and the `ByteBudget` floor-reserve split. Behind a feature flag; nothing consumes it yet.
2. **Committer (checkpoint).** Implement the plain-select Committer (`apply_rx` + `in_flight`
   `FuturesUnordered` + shutdown); wire to the existing `Request::Commit`. Drive it from a test
   producer, including the batching mock verifier (A1–A9 green).
3. **Download routines + shared state.** Implement the `Mutex<NeededSet>` + lock-free budget
   floor-reserve, the `Assembler` (received-queue + durable watch + reset → applyQ), the reworked
   `PeerRoutine` (peer-local `DeadlineSet`, `reserve_owned`, event-cleared `avoid`), and the
   `NeededProducer`. D1–D11 green. Delete the shed tick, central watchdog, retry-avoid, outbound
   poll, and metrics tick as each replacement lands.
4. **Cutover.** Replace the `SequencerTask` + old wiring with the routines + Committer; delete the
   frontier poll and `CheckpointFrontierRefresh`. E1/E2 + the workspace gates.
5. **Full-sync path.** Promote the interface-level full/fork handling to a full spec + impl
   (per-block commits, `ChainTipChange::Reset` handling). Separate follow-on.
6. **State-internal follow-on (optional).** Event-drive the write thread `park_timeout` and the VCT
   root waits with `Notify`s (out of scope here; tracked).

## Critical files
- `zebra-network/src/zakura/block_sync/` — new `assembler.rs`, `committer.rs`, `apply_item.rs`,
  `needed_set.rs`; rework `peer_routine.rs` (peer-local deadlines, `reserve_owned`, event `avoid`);
  reuse `reorder.rs`; retire `sequencer_task.rs`, `sequencer.rs`, `work_queue.rs`, `peer_registry.rs`;
  trim `reactor.rs` to the `NeededProducer` + serving.
- `zebra-network/src/zakura/transport/guard.rs` — `ByteBudget` (+ floor-reserve), `io.rs`
  (`reserve_owned()` accessor).
- `zebrad/src/commands/start/zakura/block_sync_driver.rs` — reuse `Request::Commit` /
  `block_apply_class`; delete the refresh path.
- `zebra-state/src/service/chain_tip.rs` (durable watch source), `write.rs` (durability fire;
  `park_timeout`/VCT waits flagged).

## Verification
Run the property catalog (`cargo test -p zebra-network`, `-p zebrad`), the structural no-poll gates
(D10/A3), the real-stack bench (`cargo xtask zakura-commit-bench -- run`), and the workspace gates
(`cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace`). End-to-end: confirm the durable tip advances smoothly with the writer
near-saturated and RSS bounded by `max_inflight_block_bytes` under a withheld-floor / black-hole-peer
injection.

---

> Status: living design draft. Known open areas still to refine before it is "perfect":
> the full-sync/fork path (interface-level only here), the exact `floor_reserve` sizing and the
> `MAX_CHECKPOINT_BYTE_COUNT` headroom for the budget-floor invariant, the state-internal VCT/root
> waits (still poll-based inside zebra-state), and the precise `NeededProducer` ⇄ serving split when
> `reactor.rs` is trimmed.
