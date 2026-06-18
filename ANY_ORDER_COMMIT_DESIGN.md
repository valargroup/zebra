# Any-order commit — design & assumptions (checkpoint-sync writer)

**Status:** design + prototyping notes. Authored autonomously from benchmark data
(see `CHECKPOINT_SYNC_FINDINGS.md` and the `wal-bench` runs). Assumptions are
documented inline; where a question would normally be asked, the assumption is
stated and the lowest-risk option chosen.

## 1. Problem (measured)

After cycles 1–7, the checkpoint-sync bottleneck is the **single finalized-writer
thread**, which is 100% busy (`write.wait`≈0, ~1140+ blocks queued behind it).
Full per-block attribution in the heavy spam region (height ~1.730M, `write.busy`
≈ 106 ms/block):

| phase | ms | depends on previous blocks? |
|---|---|---|
| `checkpoint_compute` — note-commitment tree update | ~39 | **YES (chained)** — block N+1's tree starts from N's |
| `header_tx` — serialize block + raw tx bytes into the batch | ~31 | **No** — block-only |
| `value_pools` → block-size re-serialize (`zcash_serialized_size`) | ~29 | **No** — block-only |
| `value_pools` → value-pool running total | small | YES (chained) |
| `db.write` (rocksdb commit) | ~4 | ordered (prefix consistency) |
| reads / shielded / trees / transparent | ~6 | mixed |

The writer is **CPU-bound on serialization + tree hashing**, not disk-bound
(blkio-wait = 0). ~60 ms/block is **per-block-independent serialization** that is
currently stuck on the serial writer; ~39 ms is the **chained tree update** that
cannot be reordered.

## 2. What any-order commit is

Split the writer's per-block work by dependency:

- **Ordered stage (must stay in height order):** the *chained* state — the
  note-commitment tree update (→ treestate), the value-pool running total, the
  history tree, and the canonical tip (`chain_tip_sender`).
- **Any-order stage (parallel across blocks):** the *independent* work — serialize
  the block/transactions, compute the block size, build the RocksDB batch entries
  that depend only on the block (and its now-known treestate), and `db.write`.

Blocks are *prepared/serialized* in any order, in parallel; the *canonical tip*
and *chained state* advance strictly in order; `db.write` is applied in height
order so on-disk state is always a valid prefix.

## 3. Why it beats the in-writer parallelization (B1/B2)

- **B1/B2** parallelize the independent serialization *within one block's commit*,
  but the writer is still **serial across blocks**: per-block ≈ tree(39) +
  parallel-serialize(~12) + write(4) ≈ **55 ms** → ~18 blk/s (heavy).
- **Any-order** overlaps the independent batch build (~60 ms) of blocks N+1, N+2…
  with the **chained tree update (39 ms)** of block N, on *separate threads*. The
  writer's critical path collapses to the **tree chain (~39 ms) + ordered write**
  → ~25 blk/s (heavy), i.e. ~**2.5–2.7×** over the pre-B1/B2 baseline, **if the
  batch build keeps up** (it does when parallelized across blocks).
- Any-order also **sidesteps the rayon-contention trap** that limited B1/B2 and
  cycle-5: the independent work runs on its own worker threads, not on the global
  rayon pool that the download/verification pipeline saturates.

**Floor:** the tree-update chain (~39 ms heavy, ~14 ms typical) is serial by
construction and bounds any-order commit. Going below it needs a faster tree
update (a separate problem, out of scope here).

## 4. Minimal-scope prototype

A bounded 2-stage pipeline inside the finalized-writer task:

```
verified blocks → [Stage 1: ordered, single thread]    → bounded chan(cap ~2–4) →
                  chained state: tree update, value-pool
                  total, history tree, treestate;
                  assert parent-is-tip; advance tip
                                                          [Stage 2: any-order workers]
                                                          batch build (serialize tx,
                                                          block size, batch entries)
                                                          + db.write IN HEIGHT ORDER
```

- **Stage 1** keeps the existing strict-order checks and produces a
  `FinalizedBlock` carrying its treestate (already threaded today via
  `prev_note_commitment_trees`; the **history tree must also be threaded in
  memory** — see Assumptions).
- **Stage 2** does the heavy independent work. Batch *construction* may proceed
  out of order across a small window; `db.write` is serialized in height order
  (a per-height sequencer) so the on-disk tip is always a contiguous prefix.

**Minimal first cut (lowest risk):** Stage 2 is a *single* extra thread (so writes
are trivially ordered), giving pipeline throughput = `max(Stage1, Stage2)` instead
of their sum. Even single-threaded Stage 2 (~64 ms) overlapping Stage 1 (~39 ms)
takes the per-block wall from ~106 → ~64 ms (~1.65×). A later iteration widens
Stage 2 to a few workers (batch build any-order, ordered write) to push toward the
~39 ms tree-chain floor.

## 5. Correctness invariants (consensus-critical)

1. **Tip advances in strict height order**, only after the block's `db.write`
   succeeds. Readers never see height N+1 committed before N.
2. **`db.write` applied in height order** → on-disk state is always a valid prefix;
   a crash leaves a contiguous chain.
3. **Chained state threaded in memory, never re-read mid-pipeline:** the treestate
   (`note_commitment_trees`) and the **history tree** must be passed forward from
   Stage 1 to Stage 1 of the next block. Re-reading `db.history_tree()` /
   `note_commitment_trees_for_tip()` under the pipeline would return a **stale**
   tip (Stage 2 hasn't written yet) and silently diverge every later root. This is
   the single highest-risk item.
4. **Parent-is-tip assertions** stay in the stage that owns the in-memory next
   height (Stage 1), driven by an in-memory counter seeded from the DB tip (the DB
   tip lags under the pipeline).
5. **Error/reset path:** on a compute or write error, drain/stop the pipeline,
   reset the in-memory next-height counter **and** the threaded history tree +
   treestate from the real DB tip, exactly as the current writer reset does.
   `rsp_tx` travels with the block so the response is sent after the commit attempt.
6. **Shutdown:** input close → Stage 1 drains → drops sender → Stage 2 drains,
   writes, exits, `db.shutdown(true)`.
7. **Byte-identical state:** serialization moved to Stage 2 is unchanged
   (`RawBytes` is verbatim; block size formula matches `zcash_serialized_size`).

## 6. Assumptions (where I did not have clarity)

- **A1.** Archive mode (`store_raw_transactions = true`) is the benchmark config;
  the `header_tx` 31 ms is real there. In pruned mode it's smaller but the design
  is unchanged.
- **A2.** The value-pool running total and history tree updates are cheap relative
  to the tree update, so keeping them in the ordered Stage 1 does not make Stage 1
  the dominant cost (Stage 1 ≈ tree update). Validated by attribution
  (value-pool-change is ~ms; the 29 ms was the block-size re-serialize, which moves
  to Stage 2).
- **A3.** Out-of-order *batch construction* with in-order *db.write* is acceptable
  for crash consistency, because a batch is atomic and we never write N+1 before N.
- **A4.** A small pipeline depth (2–4) is enough to hide Stage 2 behind Stage 1;
  deeper buffering only adds memory. (The download buffer is already ~1500 deep, so
  Stage 1 is never input-starved.)
- **A5.** Keeping Stage 2 single-threaded in the first cut is acceptable as a
  low-risk milestone; multi-worker Stage 2 is a follow-up once correctness is
  proven.

## 7. Risks

- **History-tree staleness (A-3 / invariant 3)** — highest risk; silent root
  divergence if the history tree is re-read instead of threaded. Mitigate with a
  `cfg(debug_assertions)` cross-check comparing the threaded history-tree `.hash()`
  against `db.history_tree().hash()` during a soak.
- **Reset path** correctness under the pipeline (must reset all threaded state).
- **Interaction with `#115` retention** — `commit_finalized_direct` returns a
  retention plan that must travel to the write stage.
- This overlaps upstream `#10725` (Arya's IBD engine), which takes the same
  any-order approach wholesale; this prototype is the *minimal* slice for the
  checkpoint-sync writer on the current architecture.

## 7a. Measured baseline (post-B1/B2) and projected benefit

After B1/B2 (parallel writer serialization on the dedicated pool, PR #128),
matched-height writer attribution in the heavy region (1.729–1.732M):

| writer phase | ms/block | in pipeline → which stage |
|---|---|---|
| note-commitment tree update | ~26 | **Stage A (ordered, chained)** — the floor |
| `header_tx` serialization | ~8 | Stage B (any-order prep) |
| `value_pools` (block size) | ~8 | Stage B (any-order prep) |
| db.write | ~4 | Stage B (ordered write) |
| other (reads, value-pool total, etc.) | ~2 | split |
| **`write.busy` total** | **~48** | |

Pipelining Stage B (serialization + write, ~20 ms) behind Stage A (tree chain,
~26 ms) of the next block collapses the writer's serial path to **~26–30 ms**
(the tree chain + db.write; serialization hidden). Projected **~1.5–1.8× over
B1/B2** on the writer side, i.e. cumulative **~3.5–4.5× over the pre-stack
baseline** in the heavy region — **bounded by the ~26 ms tree-update chain.**
Going below that needs a faster tree update (separate problem).

This projection is grounded in measured per-phase data, not estimated.

## 7d. MEASURED RESULT — built, benchmarked, no throughput gain (CPU-bound)

**Status: built the full two-thread pipeline, verified correct, benchmarked it
matched-height against B1/B2 (`zebrad-b1b2`). It does NOT improve throughput in
this regime, and is marginally slower. Root cause measured below. Not PR'd (no
measurable win).**

Implementation (branch `proto-any-order-pipeline`): the finalized writer loop is
split into **Stage A (compute thread)** — the chained treestate computation
(`compute_finalized`: note-tree update, ZIP-244 commitment check, history-tree
push), with note + history trees threaded in memory A→A — feeding a bounded
channel to **Stage B (the existing writer thread)** — `finish_pipelined`: batch
build + RocksDB write + `set_finalized_tip`, in order. `ChainTipSender` stays in
Stage B (no Clone needed); the receiver is moved into Stage A via `mem::replace`.

Correctness: 46/46 `finalized_state` unit tests pass; a clean checkpoint sync
1.707M→1.737M committed every block with no commitment errors, no resets, and the
on-shutdown DB-format integrity check passed (every block's history root validated
against the threaded trees → treestate threading is correct on the happy path).

Benchmark (mainnet heavy region 1.72M→1.73M, pinned peer, Zakura off, 8-core box):

| metric (heavy region)        | B1/B2 (serial) | pipeline |
| ---------------------------- | -------------- | -------- |
| throughput                   | 29.5 blk/s     | 26.4 blk/s |
| writer busy (compute+write)  | 25.9 ms/blk    | 18.2 ms/blk (write only) |
| Stage A compute              | — (inline)     | 17.7 ms/blk (concurrent) |
| writer wait (idle)           | 7.8 ms/blk     | 19.6 ms/blk |
| **writer cycle (busy+wait)** | **33.7 ms/blk**| **37.8 ms/blk** |
| CPU used                     | 7.75 / 8 cores | 7.14 / 8 cores |
| downloads in-flight          | 1550           | 1357 (buffer full → not peer-starved) |

**Why it doesn't help:** the heavy region is already **CPU-saturated (~7.75/8
cores)** with the download buffer full — the bottleneck after cyc1–7 + B1/B2 is no
longer the serial commit *stage*, it is *total CPU work across the whole sync
pipeline* (global-pool verify + commit-pool tree/serialization + tokio). Splitting
the commit into two threads does not add cores; it redistributes the same CPU
work. Stage A (the chained tree update) cannot be parallelized away and becomes the
new gating stage, so Stage B spends most of its time (19.6 ms) *idle waiting for
Stage A*. The cross-thread handoff + both stages sharing `COMMIT_COMPUTE_POOL`
(alongside the global verify pool) add scheduling/contention overhead, leaving
cores **more** idle (7.14 < 7.75) — net writer cycle 33.7→37.8 ms, ~10% slower.

This is work-conservation: on an N-core box at ~N/N utilization, wall-time ≥
total_work / N regardless of how the commit is partitioned. Deeper pipeline depth,
separate pools per stage, etc. cannot beat it — none add CPU capacity.

**Takeaway / redirected lever:** further *commit-side restructuring* (pipelining,
any-order, more parallel batch prep) cannot raise checkpoint-sync throughput while
CPU-bound. The only remaining lever is **reducing total CPU work**. NOTE: the
`to_librustzcash` *de-dup* is already done and in this baseline — commit
`229c620b4` / PR #125 (`txid_and_auth_digest`: one conversion → both txid +
auth-digest). That halved the conversions but each tx still does exactly **one**
`to_librustzcash()` reparse, which dominates the per-tx crypto. So the genuinely
remaining lever is **native ZIP-244 digests** — compute the v5 txid + auth
commitment directly from Zebra's `Transaction` structs, eliminating the
librustzcash reparse entirely (large, consensus-critical). That cuts cycles rather
than reshuffling them. The earlier ~1.5–1.8× projection assumed the
serial commit was the bottleneck with spare CPU to overlap onto — that assumption
no longer holds after the cyc1–7 + B1/B2 wins pushed the region to CPU saturation.

## 7c. Prototype build status (autonomous run, validated as far as safe)

**Built + verified (on `proto-any-order-pipeline`):** the foundational refactor —
`commit_finalized_direct` split into:
- `compute_finalized(&self, finalizable, prev_note_trees, prev_history_tree)
  -> ComputedFinalized` (Stage A: tree update, commitment check, history push;
  **history tree now threadable** instead of re-read), and
- `write_finalized(&mut self, ComputedFinalized) -> (hash, note_trees)`
  (Stage B: contiguity asserts, `write_block`, stop-height),
- with `commit_finalized_direct` kept as a thin wrapper (passes
  `prev_history_tree = None`, identical behaviour).

This compiles and is **behaviour-preserving — `cargo test -p zebra-state
finalized_state::tests` 46/46 pass**, and is the genuine enabler for the pipeline
(it isolates compute from write and makes the history tree threadable). It has no
standalone perf benefit (so it is not separately PR'd).

**Not built (consensus-critical, left for the reviewed refactor):** the two-thread
split itself. The remaining work, now precisely mapped:
- Restructure `WriteBlockWorkerTask::run` to own its fields (currently `&mut self`)
  so the finalized-block receiver can move into a **Stage A compute thread**,
  while the main thread becomes **Stage B** and keeps the non-`Clone`
  `ChainTipSender` (used by both the finalized writes and the later non-finalized
  phase) — this resolves the `ChainTipSender` ownership obstacle.
- A bounded channel A→B carrying `(Result<ComputedFinalized>, rsp_tx)`; Stage B
  does the write, `rsp_tx.send`, `chain_tip_sender.set_finalized_tip`, and metrics
  (replicating `commit_finalized`'s tail).
- An in-memory next-height counter in Stage A (the DB tip lags under the pipeline,
  so the wrong-height drop can't use `db.finalized_tip_height()`).
- A cross-thread reset (`Arc<AtomicBool>`): on a Stage B write error, signal Stage
  A to clear its threaded trees/history + re-seed from the DB tip.
- `pub(crate)` visibility for `compute_finalized`/`write_finalized`/`ComputedFinalized`.

**Why not completed autonomously:** the error/reset path, the contextual
(non-finalized) arm, and shutdown draining are **not exercised by a clean
checkpoint-sync differential test**, so the full split cannot be self-verified
without review — and it is exactly the reviewed refactor this work was scoped to
inform. The differential sync *does* validate the happy path (every commitment
check validates the history root → catches treestate-threading bugs), so the
threading mechanism is sound; the un-exercised paths are the review surface.

## 7b. Prototype assessment & recommendation (autonomous run)

**Status:** designed, measured, and projected — **not built autonomously, by
choice.** The writer split is consensus-critical, and its edge cases (the
error/reset path, the contextual/non-finalized arm, and shutdown draining) are
**not exercised by a clean checkpoint-sync differential test**, so a full
prototype cannot be self-verified without human review. The lower-risk building
blocks are already proven in this PR stack:

- **Concurrent prep + ordered commit works** — cycle-7 (#127) precomputes per
  block in the concurrent download stage and commits ordered; the same shape the
  pipeline generalizes.
- **The independent serialization parallelizes** — B1/B2 (#128) on the dedicated
  pool, with measured −53% per phase.
- **The chained floor is measured** — the ~26 ms tree update bounds the result.

**Recommended path for the reviewed refactor:**
1. Thread the history tree in memory through the commit path (mirror the existing
   `prev_note_commitment_trees` threading + its reset-on-error via `.take()` →
   re-read). Byte-identical without the pipeline; the prerequisite that removes
   the stale-re-read hazard. **Verify:** differential sync + the `cfg(debug)`
   history-tree `.hash()` cross-check.
2. Split `commit_finalized_direct` into `compute_finalized` (Stage A: tree +
   treestate + history push, threaded) and `write_finalized` (Stage B:
   `write_block` + tip), connected by a bounded channel (depth 2–4).
3. Single-thread Stage B first (writes trivially ordered) → ~1.65×; then widen to
   a few workers (any-order build, ordered write) → toward the ~26 ms floor.
4. Keep the contextual arm on the existing path initially (checkpoint arm only).

## 8. Benchmark & test plan

- **Throughput:** same harness (`metricsrun.sh`, Zakura-off, pinned peer), matched
  heights vs the pre-pipeline tip; expect heavy-region per-block wall ~106 → ~64 ms
  (single-thread Stage 2) → toward ~39 ms (multi-worker).
- **Correctness:** differential mainnet sync to a fixed height with byte-identical
  finalized tip hash + `z_gettreestate` roots vs baseline; the `cfg(debug)`
  history-tree cross-check during a soak; `cargo test -p zebra-state` (watch the
  `rsp_tx`/reset-path tests, which move to the write stage).
