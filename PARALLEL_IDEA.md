# Parallelizing checkpoint-zone block commit — context & next steps

Branch `sync-checkpoint-commit-parallel` (off `fix-sync-head-of-line-priority`). This documents the
investigation, what shipped, and the remaining ideas, so the next session can pick up cold.

## TL;DR

Checkpoint-zone sync (height < max mainnet checkpoint 3,358,006) was capped at **~17–22 blk/s by a
single serial CPU thread** — the finalized block-writer — while 7 of 8 cores sat idle. The dominant cost
is **note-commitment-tree (Sapling/Orchard) Merkle hashing on the writer**, *not* DB/IO (which is <8% of
commit time; UTXO reads + db.write total <3 ms/block).

Two changes landed on this branch:

- **Part 1 — overlap** (`zebra-state/.../finalized_state.rs`): run `update_trees_parallel` ‖
  `block_commitment_is_valid_for_chain_history` in a `rayon::in_place_scope_fifo`. Hides the ~9 ms
  commitment check under the tree update.
- **Part 3 — parallel batch tree append** (`zebra-chain/src/parallel/batch_frontier.rs`): a generic
  `parallel_append<H, DEPTH>` that appends a block's new note commitments to the incremental Merkle
  frontier using a parallel reduction (globally-aligned dyadic blocks, each block root via `rayon`).
  Wired into Sapling **and** Orchard via `NoteCommitmentTree::append_batch` (one generic algorithm serves
  both pools + Sprout). Byte-identical to sequential append — proven by differential proptests +
  known-answer vector tests.

**Result:** peer-independent `update_trees` ~30 → ~16.5 ms/block (~1.8×); throughput ~17–22 → ~42 blk/s;
CPU ~1.1–1.7 → ~3.3/8 (peak ~4.4). See `CHECKPOINT_SYNC_FINDINGS.md` §8–§12 for the full data.

**Part 2 — pipeline (PARKED):** split the writer into compute + db-write stages so block N's write
overlaps block N+1's compute. Parked because db.write measured only ~1.9 ms; payoff was ~18% for the
riskiest change (threading the history tree forward in memory). See "Revisit Part 2" below — it's more
attractive now.

## UPDATE (2026-06-18): Part 2 / Opportunity A was BUILT + BENCHMARKED — no gain, CPU-bound. Pivot to reducing CPU work.

The pipeline idea below (Opportunity A — the writer compute/write split, a.k.a. "any-order commit")
was fully built, verified correct, and benchmarked matched-height. **It does not improve throughput in
the current regime and is ~10% slower.** This supersedes the optimistic ~60–80 blk/s projections in the
"pipeline idea" sections below — those assumed the writer was the bottleneck *with spare CPU to overlap
onto*, which is no longer true after the cyc1–7 + B1/B2 wins. Full write-up:
`ANY_ORDER_COMMIT_DESIGN.md` §7d; branch `proto-any-order-pipeline`.

**What was built:** Stage A (new compute thread) runs `compute_finalized` (chained tree update +
ZIP-244 commitment check + history-tree push), threading note + history trees in memory A→A, feeding a
bounded channel → Stage B (existing writer thread) runs `finish_pipelined` (batch build + RocksDB write +
ordered `set_finalized_tip`). `ChainTipSender` stays in Stage B; the receiver is moved into Stage A via
`mem::replace`; write-error reset via an `AtomicBool`. Verified: 46/46 `finalized_state` tests; clean
differential sync 1.707M→1.737M (every block's history root validated → threading is correct).

**Measured (heavy region 1.72M→1.73M, pinned peer, Zakura off, 8-core box):**

| metric                       | B1/B2 (serial) | pipeline |
| ---------------------------- | -------------- | -------- |
| throughput                   | 29.5 blk/s     | 26.4 blk/s |
| writer busy                  | 25.9 ms (compute+write) | 18.2 ms (write only) |
| Stage A compute              | — (inline)     | 17.7 ms (concurrent) |
| writer wait (idle)           | 7.8 ms         | 19.6 ms |
| **writer cycle (busy+wait)** | **33.7 ms/blk**| **37.8 ms/blk** |
| CPU                          | 7.75 / 8       | 7.14 / 8 |
| downloads in-flight          | 1550           | 1357 (buffer full → not peer-starved) |

**Why:** the heavy region is already **CPU-saturated (~7.75/8 cores)** with downloads fully buffered.
Splitting commit across two threads adds no cores — it redistributes the same work. Stage A (the
un-parallelizable tree chain) becomes the gating stage, so the writer idles waiting on it (7.8→19.6 ms),
and the cross-thread handoff + shared `COMMIT_COMPUTE_POOL` contention leave cores *more* idle (7.14 <
7.75). Work-conservation: at ~N/N cores, wall ≥ total_work / N regardless of partition. Deeper buffers /
separate pools can't beat it — none add CPU. **The bottleneck is no longer the serial commit *stage*; it
is total CPU work across the whole sync pipeline.** The pipeline is shelved (committed for reference), to
be revisited only if total CPU work drops enough to un-saturate the box (then a commit stage with spare
cores could overlap — see §"bigger box" below).

### Reducing total CPU work — recommendations (the only lever while CPU-bound)

Note on what checkpoint sync spends CPU on: it **skips** script/proof/signature verification (the
checkpoint vouches for block hashes), so the per-block cost is dominated by (a) the per-tx
`to_librustzcash()` reparse for the txid (+ auth digest) and (b) note-commitment tree append hashing
(Pedersen/Sinsemilla). Equihash and DB I/O are negligible. Prioritized:

1. **Profile first (cheap, de-risks everything).** Get a real CPU flamegraph (`perf` / `cargo
   flamegraph`) of the heavy region to confirm exactly how the ~7.75 cores split across the
   `to_librustzcash` reparse, tree hashing, and serialization (global vs commit pool). The any-order
   commit was built on histogram *inference*; an hour of profiling prevents another build-then-discard.
   This is the highest-value next step.

2. **Biggest *potential* win — can txid computation be skipped in checkpoint sync at all?** Eliminating
   work beats speeding it up. In the checkpoint range the block hash is trusted, so the header (tx merkle
   root + `hashBlockCommitments`) is already validated by the hash match — per-block txids aren't needed
   for *consensus*. They appear to be computed only to populate the **tx-location index** (hash→height)
   that backs `getrawtransaction`. If that index can be gated behind config (a validator that doesn't
   serve historical raw-tx lookups) or backfilled post-sync, checkpoint sync could **skip the dominant
   per-tx `to_librustzcash` conversion entirely.** Needs code confirmation of everything that consumes
   txids on the checkpoint path + a product decision on a no-tx-index mode. Investigation, not a build.

3. **Safe, unconditional win — native ZIP-244 digests.** If txids *are* required, compute the v5 txid +
   auth commitment directly from Zebra's `Transaction` structs instead of converting to the librustzcash
   type and back (the reparse is pure overhead — Zebra already has the parsed tx). ZIP-244 is a
   well-specified BLAKE2b hash tree; the existing `txid_and_auth_digest_matches_separate` prop test is the
   harness to prove the native path is byte-identical. Consensus-critical but bounded and testable; helps
   regardless of core count. NOTE: the *de-dup* (one conversion → both txid + auth) is already shipped
   (#125, commit `229c620b4`) and in the baseline — this is the *next* step beyond it (remove the reparse,
   not just halve it).

4. **Note-commitment tree hashing — mostly upstream.** The chained append (Pedersen/Sinsemilla, scaling
   with shielded outputs) is the other big consumer and is largely irreducible in Zebra (the trees are
   required state, per-block). Real reduction means faster hash impls (SIMD) in the upstream
   `sapling`/`orchard` crates — flag upstream; only worth Zebra-side effort if the profile shows redundant
   frontier work.

5. **Context — the 8-core box.** Saturation is partly the bench environment. On production-class hardware
   (16–32 cores) the existing cross-block parallelism scales further and the calculus shifts — including
   reviving the shelved any-order commit, which only pays off once there's spare CPU. Worth one
   heavy-region measurement on a bigger box to find where the wall is in production.

**Net:** profile (1) → confirm whether txids are skippable in checkpoint sync (2, biggest) → otherwise
native digests (3). Items below this line are the original (pre-measurement) investigation, kept for
context.

## Why only ~3.3/8 cores? (the central open question)

Two separate effects:

1. **Structural ceiling (not removable by more rayon):** the commit path is a *serial chain* — blocks
   commit in strict height order on one writer thread, and each block's note-commitment tree starts from
   the previous block's tree (treestate is *chained*). So only **one block's tree update runs at a time**,
   and each block has unavoidable serial sub-steps (`root()` ~2.5 ms/tree, `write_block` ~5 ms at ~1
   core). By Amdahl, the time-average is dragged below 8 even if the burst saturated all cores.

2. **Recoverable loss (~4× headroom) — DIAGNOSED (2026-06-17): global rayon contention, not the
   algorithm.** In-node, the parallel tree-update burst ran at only **~1.6 cores effective** (and in the
   heavy-Sapling region `update_trees` ~137 ms for ~1,850 leaves ≈ *sequential* cost). An isolated
   release-mode probe of `parallel_append` against **real Sapling and Orchard hashing** (batch sizes
   128–2048 × `RAYON_NUM_THREADS=1,2,4,8`) settled the cause:
   - At 8 threads it reaches **~6.7–7.4 effective cores** for 1024–2048 leaves — **both Sapling and
     Orchard**. The reduction algorithm scales well when it **owns** the workers.
   - 1-thread parallel ≈ sequential → **no task-overhead regression**.
   - Local rayon pool ≈ global pool *in isolation* (as expected with no other load).
   ⇒ The in-node shortfall is **global rayon pool contention / scheduling interference**, not the
   reduction. It's aggravated because `update_trees_parallel` already nests Sapling+Orchard tasks *plus*
   `parallel_append`'s internal rayon work, all competing on the **global** pool with the
   download/verify/checkpoint pipeline. **Decision: prioritize pool isolation, not algorithm tuning.**

## Next steps, prioritized

> **⚠️ SUPERSEDED (2026-06-18) — read the "UPDATE" section at the top of this file first.**
> The items in this section are the 2026-06-17 plan. Their status now:
> - **§1 Dedicated tree-update rayon pool — SHIPPED** (`COMMIT_COMPUTE_POOL`, PR #122).
> - **§2 Parallelize the commitment check (auth-data root) — SHIPPED** (#121 par_iter,
>   #124 hoisted into the concurrent download tasks, #125 de-dup'd the conversion).
> - **§3 The pipeline idea (Part 2 / Opportunity A) — BUILT + benchmarked, NO GAIN
>   (CPU-bound), parked as draft PR #129.** See the top "UPDATE" section.
> - **§4 ceiling / §5 beyond-checkpoint** still stand as written.
>
> Net: the region is now CPU-saturated, so the real next lever is **reducing total
> CPU work** (profile → txid-skip investigation → native ZIP-244 digests) — see the
> "Reducing total CPU work" subsection in the top UPDATE. The text below is kept for
> historical context.

### 1. Dedicated tree-update rayon pool (highest value — diagnosis DONE)
The isolation probe (above) confirmed the algorithm scales ~7× when it owns workers, so **do NOT tune
`parallel_append`** — the loss is global-pool contention. Implementation path:
- Create a **dedicated `rayon::ThreadPool`** for treestate computation and run `update_trees_parallel`
  (and `parallel_append`) inside it via `pool.install(...)`, so tree-update workers are isolated from the
  download/verify/checkpoint work on the global pool. Size it to leave cores for verification (tune; e.g.
  start ~half the cores or `nproc-2`), and measure.
- Compose naturally with the **Part-2 writer pipeline** (a dedicated compute stage is the obvious owner
  of the dedicated pool).
- **Final confirmation still owed:** a full-node dedicated-pool **A/B** (the isolation probe proves the
  ceiling exists; the A/B proves it's realized in-node, where verification contends). Measure
  `update_trees` ms/block and CPU effective-cores during commit bursts, with `commit-metrics`.
Target: `update_trees` toward ~`sequential/7` in the burst (e.g. heavy-Sapling ~137 → ~20–30 ms; light
Orchard ~16.5 → ~5 ms), lifting commit-bound throughput in the heavy-spam regions.

### 2. Parallelize the commitment check (the next wall)
`block_commitment_is_valid_for_chain_history` is ~8.7 ms, currently *hidden* under the 16.5 ms tree update
by Part 1's overlap. If step 1 drops the tree update below ~8.7 ms, this becomes the bottleneck. It's the
ZIP-244 auth-data root — a Merkle tree over per-transaction auth digests — which is parallelizable
(`block.commitment(network)` → `AuthDataRoot`). Parallelize the per-tx digesting.

### 3. The pipeline idea (Part 2) — full context

See the dedicated section **"The pipeline idea — full design & context"** below. Short version: now more
attractive than when first parked, because (a) the writer is no longer the bottleneck (Parts 1+3) and
(b) the instrumented run shows steady state is a **bursty alternation** between network-feed and
CPU-commit that overlapping would close.

### 4. Realistic ceiling
Because the treestate chain forbids two blocks' tree updates running simultaneously, you cannot cleanly
fill all 8 cores at the commit stage. With steps 1–3, a realistic target is **~5–6 cores average /
~60–80 blk/s** (peaks near 8 during bursts), not a flat 8.

### 5. Beyond checkpoint sync (different, larger target)
All of the above only helps the checkpoint zone (below height 3.36M). Above the checkpoints the
**semantic verifier** does full validation (signature + proof verification), a far larger cost and a
separate optimization frontier. If "time to fully sync from genesis" is the real goal, that path is next.

## The pipeline idea — full design & context

There are **two distinct pipelining opportunities**. They compose.

### Why the pipeline matters now (the measured evidence)

The instrumented single run (`metricsrun.sh`, default legacy+Zakura networking, pinned peer, full 5s
`/metrics` + `res-*.csv` resource sampling; analyzer `analyze_bottleneck.sh`) showed that healthy steady
state is **not one bottleneck** — it is a **bursty alternation**:

- **commit bursts:** CPU spikes to **peak 8.4–8.7 / 8 cores** (the Part-3 parallel append fully
  saturating cores) while `net_rx ≈ 0`.
- **download/feed bursts:** `net_rx` spikes to **72–126 MB/s** while CPU drops to **<2 cores**.
- Over a 99-sample run: **25/99 intervals had net_rx≈0** (committing) and **12/99 had CPU<2 cores**
  (waiting on feed). Mean CPU ~3.7–4.1/8, mean net ~17 MB/s. Disk idle throughout (blkio-wait 0,
  iowait ~1%). Verdict oscillates between "download/peer-bandwidth-bound" and "CPU/commit-bound" →
  classifier lands on **MIXED**, which *is* the finding: download and commit **do not overlap**, so
  neither saturates and the average sits near 50% CPU at ~30–40 blk/s.

So the remaining steady-state inefficiency is the **serial alternation**, and the lever is to **overlap
the feed with the commit**. (Worst case is still peer availability — a separate, network-side problem.)

### Opportunity A — writer-internal pipeline (the original Part 2)

Split the single block-writer thread (`zebra-state/src/service/write.rs`, `WriteBlockWorkerTask::run`,
the finalized loop) into **two ordered stages joined by a small bounded FIFO channel** (capacity ~2–4):

- **Stage A — compute (new `std::thread`):** receives `QueuedCheckpointVerified`; runs the checkpoint
  arm's CPU work — `update_trees_parallel` ‖ `block_commitment_is_valid_for_chain_history` (Part 1's
  rayon scope), then `history_tree.push` — and builds the full `Treestate`/`FinalizedBlock`. Refactor:
  extract a pure `compute_checkpoint_treestate(...)` from `commit_finalized_direct`'s Checkpoint arm
  (`zebra-state/src/service/finalized_state.rs`), with no `&mut self` and no DB write.
- **Stage B — write (existing writer thread):** receives compute results **in order**; runs the
  contiguity assertions against the *real on-disk tip*, calls `db.write_block(...)`, updates
  `chain_tip_sender`, metrics, and the `debug_stop_at_height` check.

Effect: block N's `db.write` (~1.7 ms commit, ~4.5 ms total `write_block` incl. UTXO/address reads +
batch prep) overlaps block N+1's ~16.5 ms compute → the write-side serial time is hidden. On its own this
is **modest (~+10–15%)** because `write_block` is small after Parts 1+3 — but it is the structural
prerequisite for keeping the committer continuously busy, and it fills the ~1-core serial valleys.

#### Critical correctness requirements (consensus-critical)
- **Thread the history tree forward in memory (highest risk).** Today `commit_finalized_direct` re-reads
  `self.db.history_tree()` every block. Under a pipeline, Stage A computes block N+1 *before* Stage B has
  written block N, so that DB read would return a **stale** tip → every later history root diverges
  silently. Stage A must keep `prev_history_tree: Arc<HistoryTree>` (seed once from `db.history_tree()`
  at startup **and after every reset**), exactly as `prev_note_commitment_trees` is already threaded.
- **`prev_note_commitment_trees` is already threaded** between blocks (returned + passed back). Pass the
  *parent's* trees to `write_block` through the channel; an off-by-one corrupts subtree/anchor writes.
- **Strict height order:** both stages single-threaded + a FIFO channel ⇒ order preserved. Move the
  out-of-order pre-filter (currently using `db.finalized_tip_height()`) into Stage A driven by an
  **in-memory next-height counter** (seeded from the DB tip; the DB tip lags under the pipeline).
- **Keep the assertions in Stage B**, against the real on-disk tip (parent-is-tip, height == tip+1) —
  this preserves the byte-level contiguity guarantee unchanged.
- **#115 (pruned-storage retention) interaction:** `commit_finalized_direct` now returns a 5-tuple
  including `self.retention_plan(height, …)`. The Stage A/B split must carry `retention` through to the
  write stage. (This is the same refactor that caused the rebase conflict — keep it in mind.)

#### Error / reset semantics
- Stage A forwards a `Result`/enum payload; **Stage B is the sole owner** of `invalid_block_reset_sender`
  and the per-block `rsp_tx`. On either a compute error (from Stage A) or a write error, Stage B runs the
  identical reset block (`write.rs` finalized loop), then re-seeds Stage A's next-height counter **and
  history tree** from the DB tip. `rsp_tx` travels with the block to Stage B so the response is still sent
  after the commit attempt.
- Shutdown: input channel close → Stage A drains → drops its sender → Stage B drains, exits, runs
  `db.shutdown(true)`. Stage A must also exit if Stage B's channel closes.

### Opportunity B — feed ↔ commit overlap (the bigger win the evidence points to)

The macro alternation above means the **download/verify feed** and the **commit** are not running
concurrently at steady rate, even though a lookahead buffer exists (`sync_downloads_in_flight` ~1500–2400).
Investigate *why the buffer doesn't keep the committer continuously fed*:
- Is checkpoint verification batched such that commit and download phase-separate?
- Does the buffer drain (commit burst) faster than it refills from the connected peers, then refill
  (download burst) while commit idles?
- Is it amplified by a thin/single-peer feed (one peer can't sustain the commit rate; see the peer
  sections)?
If feed and commit overlapped continuously, sustained throughput would approach
`min(feed_rate, commit_rate)` instead of the alternating ~50%-duty average. This is likely the larger
lever than Opportunity A and should be scoped from the `metrics-*.prom` time series (overlay
`net_rx` vs CPU vs `in_flight` vs `state_finalized_block_height` on one axis).

### Expected ceiling and how to validate
- A + B together, with the Part-1 step (saturate the tree-update burst), realistically target
  **~5–6 cores average / ~60–80 blk/s** (peaks near 8). The treestate chain still forbids two blocks'
  tree updates at once, so a flat 8 is not achievable at the commit stage.
- **Validate:** differential mainnet tip-hash + `z_gettreestate` vs baseline at a fixed height (must be
  byte-identical); a temporary `cfg(debug_assertions)` cross-check comparing the threaded history-tree
  `.hash()` vs `db.history_tree().hash()` during a soak; `cargo test -p zebra-state` (watch the
  `rsp_tx`/reset-path tests, which move to Stage B); and the `commit-metrics` histograms — after the
  pipeline, `write_block`/`rocksdb_batch_commit` should leave the critical path (add a "Stage B stall"
  gauge to confirm the compute stage is the limiter).
- Full prior design + risk write-up: `/root/.claude/plans/distributed-wobbling-book.md` (Part 2 section).

## How to measure (reuse the instrumentation)

The per-block commit-phase histograms are gated behind the **`commit-metrics`** cargo feature (off by
default, zero overhead in production). Build with it for perf work:

```bash
cargo build --release -p zebrad --features commit-metrics
```

Exposed histograms (Prometheus `/metrics`, names sanitized to `_`):
- `zebra_state_write_checkpoint_compute_duration_seconds` — WALL of the checkpoint compute phase
- `zebra_state_write_update_trees_duration_seconds` — note-commitment tree update
- `zebra_state_write_commitment_check_duration_seconds` — chain-history commitment check
- (existing) `zebra_state_rocksdb_batch_commit_duration_seconds` — db.write only

Bench harness: `/root/wal-bench/` (`prbench_res.sh LABEL BIN 400 5` for throughput + CPU/IO sampling;
`RUNBOOK.md` for the hard-link-fork method from the 1.7M snapshot). Compute mean ms/block as
histogram `_sum / _count`. Peer noise makes absolute blk/s variable — the histogram phase times are
peer-independent and are the robust metric.

## Correctness notes (consensus-critical)

`parallel_append` is validated by differential proptests in `batch_frontier.rs` (2000 random prefix×batch
cases + exhaustive 40×40 sweep) asserting identical root *and* frontier parts vs sequential
`Frontier::append`; the test node's `combine` is order- and level-sensitive. The full `zebra-chain --lib`
suite (known-answer tree-root + subtree vectors) passes. The end-to-end guarantee is the differential
mainnet sync: every checkpoint block's commitment check validates the history root (which incorporates our
Sapling/Orchard roots) against the canonical block, so syncing cleanly to a high height *is* the proof.
Pre-existing failing tests (fail identically on the clean base, unrelated): zebra-chain
`..._nu7_...` (date-dependent), zebra-state `chain_tip_sender_is_updated`.
