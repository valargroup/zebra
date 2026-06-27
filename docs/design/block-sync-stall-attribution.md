# Block-sync stall attribution & pipeline thinning

## Overview (start here)

**The problem.** Block-sync stalls are real but **unattributable**. The operator sees
throughput collapse and cannot tell which stage is the limiter: download head-of-line (a low
height never fetched), the reorder buffer holding bodies behind a hole, the apply window
throttling submission, the checkpoint verifier parking commits on its *own* contiguity gate, or
the state DB write itself. Every stall looks the same from outside, so every investigation
starts from zero.

**The root cause is structural.** The download→verify→commit path is not one pipeline. It is
**two stacked reorder/contiguity/windowing layers**, and the second one is largely redundant
under Zakura yet lives in a different crate from the tracing backbone, so it is a blind spot:

```text
Zakura download  (peer_routine + work_queue + DownloadWindow + byte budget)
  │   issuance gates only on the global byte budget + per-peer window — never on floor distance
  ▼
ReorderBuffer ─▶ Sequencer.applying  ── windowed submit ──┐     LAYER 1: contiguity + window
  │   orders the contiguous prefix above verified_tip      │     (zebra-network, instrumented)
  ▼                                                         │
ZebradBlockApplyExecutor.apply                              │
  ▼                                                         │
CheckpointVerifier.call_precomputed                         │
  │                                                         │
  ▼                                                         │
queued: BTreeMap ─▶ process_checkpoint_range ──────────────┘     LAYER 2: contiguity + window
  │   re-orders blocks Zakura already delivered in order;         (zebra-consensus, REDUNDANT,
  │   a single missing height parks EVERY commit task             essentially UNINSTRUMENTED)
  ▼
per-block oneshot ─▶ spawned commit task ─▶ state Buffer ─▶ committer
```

**The one idea that makes stalls debuggable:** _the pipeline always has exactly one limiting
stage, and that stage is derivable from signals we already collect._ Today those signals exist
but are scattered across two crates and never reduced to a verdict. The fix is a single derived
signal — `sync.pipeline.limiter` — computed in one place, plus closing the one blind spot (the
checkpoint commit await) so the verdict is complete.

**Two phases, in order:**

- **Phase A — Attribution (lands first, zero behavior change).** One derived
  `sync.pipeline.limiter` verdict naming the bottleneck stage, computed in the reactor from
  existing signals plus three small commit-side counters; and two histograms inside the
  checkpoint verifier that finally time the commit await. This is what turns a multi-hour
  guessing game into a one-glance read, and it is what makes the Phase B cut *evidence-driven*
  rather than speculative.

- **Phase B — Thinning (follow-on, gated on Phase A data).** Remove the redundant layer-2
  reorder/window/parking now that Phase A can prove how much it costs. Conservative first (keep
  the checkpoint anchor), then aggressive (per-block commit relying on header PoW) behind a hard
  safety invariant and equivalence tests.

**Why this order.** Attribution is cheap, safe, and immediately useful on its own. It also de-
risks the refactor: we only thin layer 2 if the `CommitBound`/`VerifierWaiting` data shows it is
actually on the critical path. We refactor what the data indicts, not what we assume.

**Scope decisions (confirmed):** attribution is the primary win this round; the checkpoint layer
is fair game to thin; control-flow refactors are acceptable with tests.

---

## Glossary

| Term | Meaning |
| --- | --- |
| **Layer 1** | The Zakura `Sequencer` reorder + `applying` window (`zebra-network/src/zakura/block_sync/`). Orders the contiguous prefix above the verified tip and windows submission to the apply executor. |
| **Layer 2** | The checkpoint verifier's `queued` BTreeMap + `process_checkpoint_range` (`zebra-consensus/src/checkpoint.rs`). A second reorder + per-checkpoint-range window over blocks layer 1 already ordered. |
| **The floor** | `body_download_floor` / `verified_block_tip`: the highest committed height. The download side treats it as garbage-collection only, never a fetch throttle. |
| **HOL (head-of-line)** | A single low, missing height blocking progress of everything queued above it. |
| **`WaitingForBlocks`** | Layer 2's early return when the contiguous run from the last checkpoint to the next checkpoint hash has a gap (`checkpoint.rs:996`). It parks every spawned commit task. |
| **The limiter** | The single stage currently bounding throughput. The verdict `sync.pipeline.limiter` names it. |
| **cs_trace** | The `COMMIT_STATE_TABLE` structured-trace backbone in `zebra-network/src/zakura/trace.rs` (#271). |

---

## 1. How the pipeline actually flows (and where it can wedge)

The two files originally in scope (`zebra-consensus/src/checkpoint.rs`,
`zebra-network/src/zakura/block_sync/sequencer.rs`) are **not parallel implementations** of the
same idea — they are stacked, with the checkpoint verifier acting as the apply backend for the
Zakura sequencer. The production wiring is
`ZebradBlockApplyExecutor::apply` → `apply_block_sync_body_to_output` →
`CheckpointVerifier::call_precomputed`
(`zebrad/src/commands/start/zakura/block_sync_driver.rs:93`, `:180`).

### 1.1 Download (layer 0)

One task per peer (`peer_routine.rs`) owns its transport read and runs the want-work fill loop
inline. Issuance gates on exactly two things — the global in-flight **byte budget** and the
peer's adaptive **outbound window** (`pipe.rs:14-19`) — and deliberately **never** on how far
ahead of the floor it is fetching. So download issuance does not HOL-block by design; a starved
low height is handled by floor-rescue prioritization, the floor watchdog
(`reactor.rs` `run_floor_watchdog`), and speculative-tail shedding (`shed_top_until_available`,
`sequencer_task.rs:257`), not by stalling issuance.

**Wedge points here:** the byte budget cannot fund even one worst-case request
(`download_blocked_on_budget`), or no peer can serve the lowest needed height
(`floor_gap.servable_peers == 0`), or the request is simply still on the wire
(`floor_gap.state == "outstanding"`). The reactor already computes all three.

### 1.2 Layer 1 — Sequencer reorder + apply window

Matched bodies are forwarded over a **bounded** channel (`sequencer_input`, capacity ≈
`MAX_CHECKPOINT_HEIGHT_GAP + 1`) to the `SequencerTask`. A routine reserves the channel permit
*before* decoding each block frame, so when the channel is full the routine stops reading its
own transport — backpressure isolated to one peer. The task orders bodies into the contiguous
prefix above `verified_tip`, drains them into `applying`, and submits applies in a window capped
by `can_submit_class` against `BlockApplyLimits`. When the verifier/committer is slow,
`in_flight_applies` stays full, `submittable_heights` returns empty, and submission is
**throttled** — recorded as `submit.throttled` / `CommitProgress::record_throttle`
(`sequencer_task.rs:1086-1090`). That throttle propagates all the way back to per-peer transport
reads.

**Wedge points here:** submission throttled (commit can't keep up), or bodies pooling in the
reorder buffer behind a low hole (`reorder_buffered_bytes > applying_buffered_bytes`).

### 1.3 Layer 2 — the checkpoint verifier (the blind spot)

`call_precomputed` queues each block into `queued: BTreeMap<Height, Vec<QueuedBlock>>`, spawns a
commit task **immediately** (`checkpoint.rs:843`), but that task **blocks on `req_block.rx.await`**
(`:846`) until `process_checkpoint_range` finds a fully contiguous chain from the last verified
checkpoint to the next checkpoint hash and fires the block's oneshot (`:1088`). A single missing
height returns `WaitingForBlocks` early (`:996`) and **no oneshot fires**, so every spawned
commit task stays parked while `queued` may hold a long contiguous-but-incomplete run. Only then
does each block reach `state_service.oneshot(CommitCheckpointVerifiedBlock)` (`:853`).

Under Zakura this is **mostly redundant**: layer 1 already delivers blocks contiguous and in
order, so layer 2's reorder/contiguity walk rarely does useful work — but when it parks, it
parks invisibly. There is **no metric timing the `rx.await` park or the commit oneshot**; the
`checkpoint.*` metrics only count queue occupancy. This is the single biggest attribution gap.

**Wedge points here:** `WaitingForBlocks` parks all commits (VerifierWaiting), or the state DB
write itself is slow (CommitDb). Today both are dark.

---

## 2. The attribution model: `sync.pipeline.limiter`

The pipeline has exactly one limiting stage at any instant. We classify it into six verdicts and
emit the verdict as both an enum-coded gauge `sync.pipeline.limiter` and a `limiter` string field
(plus a `reason` sub-tag) on the existing `BLOCK_SYNC_STATE` trace row.

| Verdict | Meaning | Primary fix lever |
| --- | --- | --- |
| `None` | Synced / idle (`body_lag == 0`). | — |
| `DownloadGap` | A low height is not fetched. `reason ∈ {budget, no_peer, on_wire}`. | budget size, peer set, request timeout / watchdog |
| `Reorder` | Bytes arriving but pooling behind a low hole. | shed tuning, lookahead caps |
| `SubmitThrottled` | A ready contiguous body could not submit; an apply-class limit is saturated. | apply limits, commit speed |
| `CommitBound` | Applies submitted and in flight, frontier frozen, download fed. Split into VerifierWaiting vs CommitDb by the §4 histograms. | thin layer 2 (Phase B) / DB |
| `Saturated` | `body_lag > 0` and nothing is starved: bandwidth-bound, all stages flowing. | more peers / bandwidth |

### 2.1 Classification rules (first match wins)

Computed once per periodic tick in `reactor.rs::trace_sync_state`, where the download/peer/
floor-gap fields already live. Let:

- `lag = self.body_lag()`
- `dlb = download_blocked_on_budget` (already computed, `reactor.rs:~1569`)
- `fg = self.floor_gap_diagnostics(now)` (`reactor.rs:~1816`) → `fg.servable_peers`, `fg.state`
- `view = self.last_view` (the `SequencerView` snapshot)
- `throttled_delta = view.submit_throttled_total − prev_total` (reactor diffs per tick)
- `in_flight = view.checkpoint_in_flight + view.full_in_flight`
- `download_starved = dlb || fg.servable_peers == 0 || fg.state == "outstanding"`

```text
1.  lag == 0                                                              → None
2.  throttled_delta > 0 && committed_blocks_per_sec > 0                   → SubmitThrottled
3.  applying_len > 0 && in_flight > 0 && committed_blocks_per_sec == 0
      && commit_frontier_stall_seconds >= STALL_S && !download_starved    → CommitBound
4.  dlb                                                                    → DownloadGap (budget)
5.  fg.servable_peers == 0                                                 → DownloadGap (no_peer)
6.  fg.state == "outstanding"                                             → DownloadGap (on_wire)
7.  reorder_len > 0 && reorder_buffered_bytes > applying_buffered_bytes
      && received_blocks_per_sec > 0                                       → Reorder
8.  else                                                                   → Saturated
```

**Why this order.** Commit-side backpressure is tested first: a slow committer fills every
upstream buffer, making the download side *look* busy, so it would otherwise be misread as
`Saturated`. Note rule 2 requires a **non-idle committer** (`committed_blocks_per_sec > 0`): under
batched commit the apply window fills during batch *accumulation* while the committer is idle and
bursts only on flush, so a full window with an idle committer is a batching artifact, not commit
pressure — it falls through to surface the real upstream limiter rather than reporting
`SubmitThrottled`. (Observed: a 2-peer, supply-bound run read 56% `SubmitThrottled` before this
guard while `committed_blocks_per_sec` was 0 at the median and the committer bursted to thousands —
the throttle was the 800-deep apply window accumulating batches, not the committer falling behind.)
Download starvation (budget → no_peer → on_wire) is next, most-specific first.
`Reorder` is distinguished from `DownloadGap` by "bytes are arriving but pooling" rather than
"nothing is arriving." `Saturated` is the residual: lag remains but no stage is starved, i.e.
the node is bandwidth-bound and healthy.

### 2.2 Why the reactor, not the sequencer task

The classifier is inherently a join of reactor-side signals (registry slot saturation, the
`floor_gap` walk, peer status — all reactor-only) and task-side signals (submit-throttle, in-
flight applies). The reactor's missing inputs are three small counters; the task's missing inputs
are the *entire* peer/registry subsystem. So the join belongs in the reactor, and the three
counters are plumbed up via `SequencerView` (which already carries ~22 fields). The
`BLOCK_SYNC_STATE` row is built only in the reactor, so any other home would require plumbing the
verdict back anyway.

---

## 3. Phase A — what changes

### 3.1 Three commit-side signals into `SequencerView`

File: `zebra-network/src/zakura/block_sync/sequencer_task.rs`
(`SequencerView` ~360-387, `publish_view` ~1462-1499).

- `submit_throttled_total: u64` — monotonic; today lives only in `CommitProgress`
  (`:110`, `:1087`) and the `block_commit_progress` row, never surfaced. The reactor diffs it per
  tick. **Primary discriminator for `SubmitThrottled`.**
- `checkpoint_in_flight: u64`, `full_in_flight: u64` — today `SequencerTask` fields (`:479-480`);
  needed to confirm the apply pipeline is actually loaded vs idle when separating `CommitBound`
  from `Saturated`.

Everything else the classifier reads already exists: budget (`state.budget`),
`peers_wanting_slots` / `download_blocked_on_budget`, `floor_gap_diagnostics`,
`reorder_len` / `applying_len` / `commit_frontier_stall_seconds` / `committed_blocks_per_sec` /
buffered bytes, `received_*_per_sec`, `body_lag`.

### 3.2 The verdict in the reactor

File: `zebra-network/src/zakura/block_sync/reactor.rs` — compute in `trace_sync_state`
(~1443-1602), mirror the gauge in `publish_metrics` (~1867). Add `LIMITER` / `LIMITER_REASON`
field constants in `zebra-network/src/zakura/trace.rs` alongside the existing `COMMIT_STATE_TABLE`
constants.

### 3.3 Close the checkpoint blind spot

File: `zebra-consensus/src/checkpoint.rs`. The await/DB split is only observable **inside** the
spawned commit task (`:843-864`) — the apply executor and any `BlockApplyResult` field see only
the sum, because the future resolves only after both finish. So time it here, with plain
`metrics::` histograms (the crate already depends on `metrics`, e.g. `:816`, `:1084`). **Do not**
make `zebra-consensus` depend on `zebra-network`'s cs_trace.

- bracket `req_block.rx.await` (`:844-849`) → `checkpoint.commit.await_seconds`
  (the HOL park — the previously-dark cost; high ⇒ **VerifierWaiting**).
- bracket `state_service.oneshot(CommitCheckpointVerifiedBlock)` (`:853-856`) →
  `checkpoint.commit.db_seconds` (high ⇒ **CommitDb**).
- gauge of spawned-but-unfired commit tasks → `checkpoint.commit.inflight`.
- at the `WaitingForBlocks` early return (`:996-998`) → `checkpoint.waiting_for_blocks.count`
  plus a since/last-advance freshness gauge.

The apply executor's existing `COMMIT_START`/`COMMIT_FINISH` cs_trace span
(`block_sync_driver.rs:599-637`) already times the *combined* apply cost; leave it as the top-
line number and let these histograms supply the split. A `CommitBound` verdict is then read as
VerifierWaiting vs CommitDb by which histogram dominates — and after Phase B the await-park
vanishes, so `CommitBound ≡ CommitDb` and the split self-resolves.

### 3.4 Deliverable

A single dashboard/log read names the limiter; `CommitBound` is disambiguated by the two
histograms. No control-flow change, no behavior risk.

---

## 4. Phase B — thinning layer 2

Done as a separate change once Phase A confirms how much time blocks actually spend parked in
layer 2. Two steps, conservative first.

### 4.1 The safety invariant (must hold before any thinning)

Layer 2's backward-anchor-to-checkpoint-hash is **load-bearing in the original model**: below a
checkpoint, PoW is skipped, so a block at height `h` (prev_cp < h < next_cp) is trusted *only*
because it lies on the unique hash-path between two trusted checkpoint hashes.
`process_checkpoint_range` proves this by requiring the full contiguous range up to `next_cp`,
anchoring to the checkpoint-list hash, and matching `previous_block_hash` backward
(`checkpoint.rs:992-1031`). Committing `h` before `next_cp` is seen would trust an
unauthenticated, PoW-free forward chain — an attacker forks right after `prev_cp` and you commit
garbage.

**Under Zakura the same trust is established earlier.** Every body that reaches `call_precomputed`
matches an expected hash from Zakura header sync, which PoW-validates and checkpoint-anchors every
header (`zebra-network/src/zakura/header_sync/validation.rs:7-15`, `:270-307`). So layer 2's re-
anchor is redundant **iff** every body reaching `call_precomputed` carries a header-verified hash.

> **Invariant B.** _No body may reach `call_precomputed` without a hash that Zakura header sync
> has already PoW-verified and checkpoint-anchored._ Before B2 ships, audit that no path — the
> unmatched-body fallthrough (`peer_routine.rs:1150-1184`), regtest, the throughput probe — can
> route an unverified body there.

Note that Zakura header sync pins the checkpoint list only at the configured start anchor and
then extends by PoW + contiguity; it does not re-assert every intermediate checkpoint hash. So
it provides PoW-equivalent security, not full per-checkpoint-pin security. Keeping the cheap
checkpoint-height pin (`process_height` `:922-929`) as a **returned error, not a panic** recovers
the original guarantee at near-zero cost (and a panic there would be a remote DoS).

### 4.2 B1 — remove the redundant reorder/window/spawn, keep the anchor (no consensus change)

Exploit layer 1's in-order contiguous delivery: replace the `queued` BTreeMap +
`target_checkpoint_height` contiguity walk + per-block `tokio::spawn` + `oneshot`
(`checkpoint.rs:782`, `:809`, `:843`, `:970`) with a single in-order pass that still verifies each
range up to its checkpoint hash and commits inline in height order. Deletes the second reorder
layer and the spawn-per-block, kills the "`WaitingForBlocks` parks everything" HOL for out-of-
order arrivals, but still anchors at the checkpoint boundary. Trust model unchanged.

### 4.3 B2 — per-block commit, rely on header PoW (gated on B1 + audit + equivalence tests)

Commit each contiguous block immediately and advance `verifier_progress` per-block **after** the
state ack (preserving read-your-writes and the `reset_sender` reset-to-tip path at `:882-897`),
keeping the checkpoint-height pin as a returned error. This is where the await-park
(VerifierWaiting) disappears entirely.

What this touches and why it is delicate:

- **`verifier_progress` representation.** `update_progress` (`:542-580`) only records progress at
  checkpoint heights; per-block advance to arbitrary heights needs the `InitialTip(height)`-style
  representation, and `check_height` / `target_checkpoint_height` must be reworked or retired or
  duplicate-rejection (`AlreadyVerified`) breaks.
- **Verifier↔state sync (`:818-841`).** Advance progress only on the commit *ack*, never on
  `tx.send`; otherwise a failed commit desyncs the verifier and the reset-to-tip path no longer
  converges.
- **Tree-root ordering.** The state service builds note-commitment and history trees incrementally
  and requires contiguous height order. This is the load-bearing equivalence to test (see the
  batched-commit read-your-writes / history-tree work — history tree is the one the pre-Heartwood
  equivalence test misses).

---

## 5. Verification

**Phase A**

- `cargo build -p zebra-network -p zebra-consensus`, then
  `cargo clippy --workspace --all-targets -- -D warnings`.
- Unit: a table-driven reactor classifier test feeding synthetic `SequencerView` / `floor_gap`
  states asserts each of the eight rows yields the expected verdict (lives alongside
  `block_sync/tests.rs`).
- Live (existing bench harness, `deploy/runner/`): confirm `sync.pipeline.limiter` and the
  `checkpoint.commit.{await,db}_seconds` histograms populate, and the verdict tracks reality —
  throttle peers → `DownloadGap`; pause the committer → `CommitBound` with `await_seconds` high.
  Cross-check against `commit_frontier_stall.seconds` and `submit.throttled`, which must agree.

**Phase B (mandatory before B2 ships)**

- **Commitment/history-tree root equivalence:** sync a multi-checkpoint range through the original
  batched path and the thinned path; assert byte-identical finalized tip hash and
  sapling/orchard + history-tree roots at every checkpoint height.
- **Ordering:** assert the thinned path never commits `N+1` before `N` is durable.
- **Failure/reset:** inject a `CommitCheckpointVerifiedBlock` failure mid-range; assert
  `verifier_progress` resets to the real state tip and re-sync converges with no skipped/double
  height.
- **Checkpoint-pin rejection:** feed a contiguous, PoW-valid, non-checkpointed fork; assert
  rejection at the checkpoint height as an error, not a panic.
- **Path audit (Invariant B):** assert no body lacking a header-verified hash match can reach
  `call_precomputed`.

---

## 6. File index

| File | Role in this design |
| --- | --- |
| `zebra-network/src/zakura/block_sync/sequencer_task.rs` | A1: `SequencerView` + `publish_view` add the three commit-side counters. |
| `zebra-network/src/zakura/block_sync/reactor.rs` | A2: classifier in `trace_sync_state`, gauge in `publish_metrics`. |
| `zebra-network/src/zakura/trace.rs` | A2: `LIMITER` / `LIMITER_REASON` field constants. |
| `zebra-consensus/src/checkpoint.rs` | A3: commit-await/db histograms (`:843-864`), waiting freshness (`:996`); Phase B thinning (`:782`, `:809`, `:970`). |
| `zebrad/src/commands/start/zakura/block_sync_driver.rs` | Combined-cost cs_trace span (reference; no change in A). |
| `zebra-network/src/zakura/header_sync/validation.rs` | Invariant B: the PoW + checkpoint anchor Phase B depends on (audit only). |
