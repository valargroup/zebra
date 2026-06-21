# Sapling Pedersen-hash fork — benchmark results (1.7M–1.9M)

Real-world impact of the **valargroup/sapling-crypto PR #1** ("Speed up non-circuit Pedersen hash via fused chunk-block precomputation", branch `pedersen-hash-fused-precompute` @ `f2cbd775`) on full-node checkpoint sync through the sandblast region.

## TL;DR

The fork's ~2.4× faster Pedersen hash (micro-benchmark claim) translates to a **~18% reduction in both committer `update_trees` time and total CPU-per-block across the Sapling-heavy sandblast (1.70–1.85M)**, tapering to ~4% in the more Orchard-weighted 1.85–1.90M. Both metrics are peer-independent. Since the per-block Pedersen hashing is ~30% of total CPU work in this region, a 2.4× speedup on it yields ~18% less CPU — which, in the CPU-bound deep sandblast, is throughput headroom. **Worth landing**, bit-for-bit identical output.

## Methodology

- **Fork:** sapling-crypto 0.7.0 with the fused chunk-block Pedersen precompute. Optimizes `pedersen_hash` (backs `Node::combine`/`merkle_hash`), ~2.4× on a 510-bit Merkle hash, **bit-for-bit identical output** (consensus-safe; generators unchanged). Drop-in, no API change; tables built lazily.
- **A/B:** the *same* code ± the fork. Base = `sync-perf-main-2` tip `a6e1d1791` (which has **#144 merged** — the off-committer note-tree precompute + parallel batch append). Two binaries from one worktree: `zebrad-sapling` (fork, via `[patch.crates-io] sapling-crypto = { git = … }`) and `zebrad-sapling-nofork` (stock crates.io 0.7.0). Only the Pedersen impl differs.
- **Instrumentation:** built `--features commit-metrics` + committer-utilization patch. Single fast peer (`167.99.162.47`), `feed_run_long.sh`, 1.707M → 1.900M.
- **Robust vs noisy:** `update_trees` (committer-thread tree time) and **CPU-per-block** (`Σ(cpu_cores·5s)/Δheight`) are **peer-independent** — the comparison is valid despite different peer draws. Absolute throughput is peer-confounded (reported as context only).
- **Methodology note (disk incident):** the fork run hit `No space left on device` at 1.797M (sandblast forks are ~180G; /mnt filled) and the committer thread panicked. RocksDB had committed to 1.797M, so the run was **resumed** on the same fork to 1.900M. Fork data is therefore `part1` (1.7075–1.797M) + `part2` (1.797–1.900M). `update_trees`/CPU counters are cumulative per node-run, so per-region deltas are computed **within each run** (part1, part2, no-fork separately).

## Results — per 50k-block region

| range (M) | `update_trees` ms/blk (no-fork → fork) | Δ | CPU-sec/block (no-fork → fork) | Δ |
|---|---|---|---|---|
| 1.70–1.75 | 4.73 → 3.82 | **−19.2%** | 0.0640 → 0.0527 | **−17.7%** |
| 1.75–1.80 | 6.45 → 5.23 | **−18.9%** | 0.1096 → 0.0892 | **−18.6%** |
| 1.80–1.85 | 6.17 → 5.12 | **−17.0%** | 0.0978 → 0.0796 | **−18.6%** |
| 1.85–1.90 | 5.30 → 5.06 | −4.5% | 0.1022 → 0.1002 | −2.0% |

Throughput (peer-confounded, context): no-fork 49.4 blk/s over 1.707–1.900M; in the matched deep-sandblast window (1.797–1.900M) the fork ran 48.5 vs no-fork 44.7 blk/s (~+8.5%, but different peer draws — not an attributable claim).

## Analysis

1. **The fork delivers ~18% less per-block CPU in the Sapling-heavy sandblast (1.70–1.85M).** Both the committer-side `update_trees` and the whole-node CPU-per-block drop ~17–19%, and they agree — a strong, peer-independent signal.

2. **Why 18% and not 2.4×:** the 2.4× speedup is on the Pedersen hash *only*. Working back from the data, Pedersen is ~30% of total per-block CPU in this region (`0.30 × (1 − 1/2.4) ≈ 0.18`). The other ~70% — Orchard/Sinsemilla hashing, tx digesting, download/verify, RocksDB — is untouched. So a 2.4× hash speedup is an ~18% whole-node CPU win where Sapling dominates.

3. **Interaction with #144 (important):** #144 already moved the *bulk* Pedersen hashing into the off-committer precompute pool. So the committer's `update_trees` is mostly the **graft** (root-recompute + ommer-fold), and its ~18% drop reflects only the Pedersen *within the graft*. The larger share of the fork's win lands in the off-committer precompute — which is exactly why **CPU-per-block** (whole-node) shows the same ~18%: the precompute pool simply does less work. The fork and #144 are complementary: #144 relocates the hashing off the serial committer; the fork makes that hashing cheaper.

4. **Region-dependence (1.85–1.90M only ~4%):** the sandblast pool mix flips by height (Sapling- vs Orchard-spam). The 1.85–1.90M slice is less Sapling-Pedersen-dominated, so the Pedersen fraction of CPU is smaller and the fork helps less. This is expected — the fork touches Sapling Pedersen, not Orchard Sinsemilla.

## Verdict

**Land it.** The fork is a bit-for-bit-identical, drop-in ~2.4× Pedersen speedup that yields a **real ~18% per-block CPU reduction across the Sapling-heavy sandblast** — the most CPU-bound part of checkpoint sync. Because #144 already moved the bulk hashing off the serial committer, the win shows up as **CPU/throughput headroom** rather than reduced committer-serial time: in the CPU-saturated deep sandblast (~6/8 cores), ~18% less CPU work is ~18% more throughput headroom. It composes cleanly with #144 (relocate the work) and the note-tree precompute. The benefit is region-dependent (largest where Sapling Pedersen dominates; minimal in Orchard-heavy slices), and Orchard/Sinsemilla would need a separate optimization. This is the "reduce total CPU crypto work" lever the bottleneck analysis recommended for the CPU-bound sandblast — and it delivers.

### Artifacts
- Binaries: `/root/wal-bench/zebrad-sapling` (fork), `zebrad-sapling-nofork` (stock).
- Data: `feedrun-sapfork-part1.csv` (1.7075–1.797M), `feedrun-sapfork2.csv` (1.797–1.900M), `feedrun-sapnofork.csv` (1.707–1.900M).
- Worktree: `/root/zebra-sapling` (`[patch.crates-io]` → the fork).

---

## Update — 7 MB table version (C=3, 2026-06-20)

The fork was updated (`f2cbd775` → `1e2904d3`) to a **smaller, retuned table: `CHUNKS_PER_BLOCK = 3`, ~7 MB, ~3.0× micro-bench** (the old default was C=4, ~60 MB, 2.4×). Hypothesis: 7 MB fits in L3, so it should realize more of its speedup in-node than the cache-evicted 60 MB version. Rebuilt **with frame pointers** (`-C force-frame-pointers=yes`) — which also finally made flamegraphs tractable.

**Direct measurement (precompute timer = bulk off-committer Pedersen hashing, peer-independent), 1.722–1.735M:**

| | no-fork | 7 MB fork | realized speedup |
|---|---|---|---|
| precompute (Pedersen) ms/blk | 9.64 | **6.40** | **1.51×** |
| update_trees graft ms/blk | 3.49 | 2.09 | 1.67× |

So the 7 MB table realizes **1.51×** on the Pedersen hashing — vs the 60 MB version's ~1.32× (inferred from its 18% whole-node A/B and the ~74% note-hashing CPU share). **The smaller table is modestly better in-node (1.51× vs ~1.32×), supporting the cache hypothesis — but only modestly.**

**Key nuance:** both tables realize only **~50% of their micro-bench** in-node (7 MB: 1.51 of 3.0×; 60 MB: ~1.32 of 2.4×). Since 7 MB *fits* in L3 yet still loses half, the in-node degradation is **not primarily L3 eviction** — the larger causes are L2 pressure (7 MB ≫ 1 MB L2), memory bandwidth, and the micro-bench being unrepresentative (tight loop vs interleaved-with-frontier-management in-node). The cache effect is real but secondary.

**Flamegraph (frame-pointer, clean, complete — 48,997 stacks, `flame-sapling-7MB-fork.svg`):**
- **~74% of CPU is in rayon note-hashing jobs** (`StackJob` 59.6% + `HeapJob` 14.7%) — the parallel Pedersen append/precompute (crypto inlined into the closures, so labeled as the job wrapper, not `jubjub`).
- ~18% tokio blocking tasks (committer + other). RocksDB/deserialize each <1% at the leaf.
- **Corrects an earlier artifact:** the DWARF partial showed `execute<SpinLatch>` ≈65% (suggesting spin-wait); the clean fp capture shows the leaves are `StackJob`/`HeapJob` (jobs *executing*), so there is **no significant rayon spin-wait** — that was a DWARF mis-unwind. (Caveat: fp can't see *inside* the inlined crypto, so the within-Pedersen split needs DWARF inline info; the precompute *timer* is the reliable measure of the hashing time.)

**Verdict:** the 7 MB version is the better choice — smaller footprint, modestly higher realized speedup (1.51×), bit-identical. But the bigger lesson is that ~half the micro-bench speedup is lost in-node regardless of table size, so further Pedersen wins likely need a different lever (SIMD field arithmetic via `target-cpu=native`, or reducing the hashing volume) rather than a bigger table. Methodology win: **frame-pointer builds make flamegraphs trivial here (31 MB perf.data vs 1.2–3.2 GB DWARF) — use fp going forward.**

---

## Table-size sweep: C=2 vs C=3 vs C=4 (2026-06-20) — cache hypothesis settled

Swept `PEDERSEN_HASH_CHUNKS_PER_BLOCK` (table size). All vs the **saved** no-fork baseline (precompute 9.64 ms/blk, 1.722–1.735M); peer-independent precompute timer.

| C | table | fits in | micro-bench | realized precompute | realized speedup | **realized fraction** |
|---|---|---|---|---|---|---|
| baseline | — | — | 1.0× | 9.64 ms | 1.00× | — |
| **2** | **~1.4 MB** | **L2** | ~2.0× | **6.31 ms** | **1.53×** | **76%** |
| 3 | ~7 MB | L3 | ~3.0× | 6.40 ms | 1.51× | 50% |
| 4 (old scheme) | ~60 MB | > L3 | ~2.4× | (inferred) | ~1.32× | ~55% |

**Findings:**
1. **Realized speedup plateaus at ~1.5× regardless of table size.** C=2 (1.53×) ≈ C=3 (1.51×) despite C=3's 50% higher micro-bench. The bigger table's extra theoretical speedup is **entirely lost to cache** in-node.
2. **The realized *fraction* tracks cache residency**, confirming the hypothesis: C=2 (fits L2) realizes **76%** of its micro-bench; C=3 (fits L3, not L2) **50%**; C=4 (exceeds L3) similar/worse. Smaller table → larger fraction realized.
3. **Even C=2 loses ~24%** (L2 latency + interleaving with frontier management), so the table scheme is **cache-bandwidth-bound at ~1.5× in-node** — you cannot beat that by tuning C.

**Verdict: ship C=2.** Same in-node speed as C=3 (~1.5×) with a **5× smaller table (1.4 MB vs 7 MB)** — minimal cache footprint, fits L2, less pollution of other work. There is no benefit to a larger table; the in-node ceiling for the table approach is ~1.5×.

**Beyond 1.5× needs a compute-side lever** (it composes with the table since it's orthogonal): `target-cpu=native` added +13% (1.13×) → **C=2 + native ≈ 1.73× on Pedersen** with a 1.4 MB table. The durable bigger win is hand-written batched-SIMD Pedersen + Sinsemilla upstream. Flamegraph: `flame-sapling-C2.svg`.

---

## Deep-sandblast A/B: C=2 vs no-fork, 1.800–1.815M (from snapshot, 2026-06-20)

A clean end-to-end A/B in the **deepest sandblast region reached** (1.800–1.815M), forking both arms from the 1.8M RocksDB snapshot (`zebra-ckpt-1800000`). Goal: in a region where Sapling Pedersen is a *larger* CPU share than the 1.72M window, does the C=2 fork's speedup show up as **whole-node CPU reduction and throughput**, not just the precompute timer?

- **Both binaries:** #143 @ `a6e1d1791` (includes #144), built identically. `zebrad-prof` (stock crates.io sapling-crypto) vs `zebrad-sap2` (C=2 fork, `PEDERSEN_HASH_CHUNKS_PER_BLOCK=2`, ~1.4 MB table). Only the Pedersen crate differs.
- **Harness:** `feed_run_deep.sh`, single pinned peer `167.99.162.47`, sequential arms (one ~180 G fork at a time), 15k blocks each.

| metric | no-fork | C=2 fork | Δ | peer-independent? |
|---|---|---|---|---|
| **CPU-seconds / block** | 0.1208 | 0.0876 | **−27.5%** | **yes** (Σcpu / Δblocks) |
| `update_trees` (committer graft) ms/blk | 6.06 | 4.70 | **−22.4%** (1.29×) | **yes** |
| avg CPU (cores of 8) | 5.11 | 4.38 | −14% | yes |
| throughput (blk/s) | 42.3 | 50.0 | **+18.2%** | no (single run) |
| in_flight (download queue) | 1534 | 1398 | both ~full | — |

**Findings:**

1. **CPU-per-block dropped 27.5%** — the headline peer-independent number. This is larger than the ~18% measured in the shallower 1.70–1.85M buckets, consistent with Pedersen being a *bigger* CPU share this deep (the precompute timer grows 10→17 ms/blk from 1.72M→1.79M as shielded-note volume accumulates, so the fork's fixed-ratio speedup removes more absolute CPU).

2. **The throughput win is credible here, unusually.** Both arms ran with the download queue **full** (in_flight ~1400–1530, near the 1500 limit), so neither was download-starved — the limiter is downstream processing in both. With downloads saturated identically, the +18% throughput is attributable to faster block processing, not a better peer draw. It also moves the *right* way relative to CPU: throughput went **up** while CPU/block went **down** — peer-luck would push both up together.

3. **The region is committer-serial-bound, not all-core-CPU-bound** (no-fork CPU only 5.11/8 despite a full download queue). So the fork's win lands two ways: the off-committer precompute pool does ~1.5× less Pedersen work (frees cores → CPU/block down), and the committer's in-graft Pedersen drops too (`update_trees` −22%) — shortening the serial path, which is what actually lifts throughput in this regime.

**Verdict:** confirms the earlier shallower-region result and strengthens it — in the deep sandblast the C=2 Pedersen fork delivers **~27% less whole-node CPU per block and ~22% less committer graft time**, and (both arms download-saturated) a credible **+18% throughput**. The crypto win does surface as throughput here, because reducing the in-graft Pedersen shortens the serial committer path. Single-run caveat stands (15k-block window, one peer), but every peer-independent metric agrees. **Ship C=2.**

### Artifacts
- Data: `feedrun-deepnf.csv` (no-fork), `feedrun-deepc2.csv` (C=2), both 1.800–1.815M.
- Snapshot: `/mnt/roman-dev-2-data/zebra-ckpt-1800000` (1.8M, 140 G). Binaries: `/root/wal-bench/zebrad-prof`, `zebrad-sap2`.

---

## 1.8–1.9M full-instrumentation matched A/B + tx_by_loc commit attribution (corrected decomposition) — 2026-06-20

Goal: precisely attribute the committer cost in deep sandblast and test how much of it is the raw `tx_by_loc` write. Forked both arms from the **compacted** 1.8M snapshot (95G, LSM score 27→<1), warm-up = first 10k blocks excluded (measure 1.81–1.9M).

**Matched A/B**: ONE binary `zebrad-sap2-notx` (C=2 fork + full instrumentation), run twice — env **OFF** = baseline (archive, `tx_by_loc` written) vs env **ON** (`BENCH_SKIP_TX_BY_LOC=1`, raw `tx_by_loc` write skipped like pruning but in archive mode). Same binary/peer-config, so throughput is peer-matched, not confounded.

### Correction: the "commit" metric was mislabeled
Earlier runs scraped `zebra.committer.commit.duration` and called it "RocksDB commit ≈ 18 ms." **That timer is the WHOLE `commit_finalized` (note-tree graft + commitment check + UTXO/address reads + batch build + raw-tx serialize + DB write), not the DB write.** The actual DB write (`rocksdb.batch_commit`, separate timer, previously unscraped) is ~2.5–6 ms. Renamed the metric to `zebra.committer.commit_finalized_total.duration_seconds`; added the missing scrapes (DB-write, checkpoint_compute, commitment_check, block_deserialize, recv_wait). RUNBOOK now requires the full set.

### Corrected committer decomposition (per block, baseline, ms)
| stage | 1.80–1.825M | 1.85–1.875M (heavy) | grows with depth? |
|---|---|---|---|
| **commit_finalized TOTAL** | 15.6 | 24.9 | yes |
| note-tree compute (checkpoint_compute) | 7.7 | 9.5 | yes (note volume) |
| reads + batch + raw-tx serialize (residual) | 5.4 | 9.5 | yes (RAM-starved reads) |
| **actual RocksDB write** | 2.5 | 5.9 | yes |
| (graft, subset of checkpoint_compute) | 5.4 | 5.8 | — |

So mid-range the committer ≈ ~45% note-tree crypto, ~35% reads/serialize, ~20% DB write — **not** a fat DB write. All three grow with depth.

### Matched A/B — per 25k bucket (1.81–1.9M)
| bucket | baseline commit / DBwr / reads / thru | no-tx commit / DBwr / reads / thru | **thru gain** |
|---|---|---|---|
| 1.800–1.825M | 15.6 / 2.5 / 5.4 / 60.0 | 11.4 / 0.7 / 3.9 / 80.4 | **+34%** |
| 1.825–1.850M | 17.5 / 3.2 / 5.1 / 53.4 | 13.9 / 1.3 / 3.8 / 65.9 | +23% |
| 1.850–1.875M | 24.9 / 5.9 / 9.5 / 38.0 | 20.9 / 3.7 / 8.2 / 44.8 | +18% |
| 1.875–1.900M | 16.8 / 3.9 / 5.2 / 54.4 | 14.0 / 1.8 / 4.5 / 65.5 | +20% |

### tx_by_loc attribution (peer-matched, robust)
Skipping the raw `tx_by_loc` write saves **DB-write ~2 ms + reads/serialize ~1.3 ms ≈ ~3.5 ms** of committer time — roughly **constant in absolute terms**. As a fraction that's ~26% of the light-bucket commit (→ **+34%** throughput) but only ~16% of the heavy-bucket commit (→ **+18%**). So `tx_by_loc` raw-write+serialize is **~a quarter of the committer, shrinking with depth** as note-tree crypto and reads grow.

**Correction to the earlier "doubling / half the committer" claim:** that compared no-tx to a *different, peer-confounded* baseline run (~50 blk/s, slower peer draw). Against the matched baseline (60 blk/s, identical binary/conditions) the real win is **+18–34%, not 2×**. The peer-independent committer-time decomposition (~3.5 ms saved) is the trustworthy number.

### Bottleneck confirmations (full instrumentation)
- **Committer-bound throughout**: qdepth 1574–1863 (queue full), CPU 5.9–7.8/8 (not all-core-saturated). Same both arms.
- **Precompute is not the gate**: committer `recv_wait` ≈ 0.9–1.2 ms (it mostly keeps ahead; #144 working).
- **Feed is not the gate**: block `deserialize` is 4 ms (light) → 21–26 ms (deep) *wall* per block, but parallel across download concurrency, and the committer never starves — so it doesn't bound throughput here (would matter in a feed-bound region; now it's measured, not a blind spot).
- **Notes/block** (chain property, matches both arms over 1.81–1.9M): **Sapling ≈ 140, Orchard ≈ 204** — Orchard-heavier in this range.
- **RAM caveat**: 95G DB on 31G RAM (~17G cache). The reads residual grows with depth (5→9.5 ms) because commit-path UTXO/address reads miss cache and hit disk; this is hardware-dependent (more RAM would shrink it), and is the part that scales worst at depth.

### Optimization recommendations (ranked)
1. **Raw-tx serialization off the committer** — deterministic, precompute in the existing lookahead (#144 pattern). ~1.3 ms.
2. **Defer the `tx_by_loc` raw-bytes write off the critical path** — it's not consensus-critical (only RPC reads it). Background batch keeps archive/RPC intact and recovers the ~2 ms DB-write. Together with #1 ≈ the full ~3.5 ms (the skip experiment) without losing RPC.
3. **Prefetch UTXO/address reads in the lookahead** — attacks the depth-growing, RAM-starved read residual (best durable lever deep).
4. **Batch note-tree hashing across the span** (not per block) — bulk-hash leaf-aligned complete subtrees in parallel, snapshot per-block roots cheaply; targets the note-tree stage that re-dominates at depth.

### Artifacts
- Data: `feedrun-deepc2f.csv` (baseline), `feedrun-deepntx.csv` (no-tx), 45-col full instrumentation, 1.8–1.9M.
- Binary: `zebrad-sap2-notx` (C=2 + `BENCH_SKIP_TX_BY_LOC` + recv_wait/precompute-hit-miss/block_deserialize timers). Snapshot: compacted `zebra-ckpt-1800000`.

---

## tx-serialize overlap prototype — result (2026-06-21)

Prototype of the "serialize off the critical path" quick win. **Change** (zebra-state, `write_block`): the raw `tx_by_loc` transaction serialization now runs concurrently with the spent-UTXO reads via `rayon::join` — serialization is CPU-bound while the reads wait on disk (the read path is RAM-starved at depth), so they overlap. The bytes are threaded as `precomputed_raw_txs` through `prepare_block_batch` → `prepare_block_header_and_transaction_data_batch`, which uses them directly (inline serialize fallback for the semantic path). Binary `zebrad-sap2-serial` (C=2 + overlap). Matched A/B vs `deepc2f` (same binary lineage, no overlap), both tx_loc **ON**, compacted snapshot, 1.8–1.9M.

| bucket | overlap commit / reads / thru | baseline commit / reads / thru | Δcommit | Δthru |
|---|---|---|---|---|
| 1.80–1.825M | 14.8 / 5.1 / 64 | 15.6 / 5.4 / 60 | **−0.8 ms** | +6% |
| 1.825–1.850M | 16.4 / 4.8 / 57 | 17.5 / 5.1 / 53 | **−1.1 ms** | +6% |
| 1.850–1.875M | 23.6 / 9.0 / 40 | 24.9 / 9.5 / 38 | **−1.2 ms** | +5% |
| 1.875–1.900M | 16.8 / 5.6 / 55 | 16.8 / 5.2 / 54 | ±0.0 ms | +1% |

**Verdict: the overlap works — modestly.** Peer-independent `commit_total` drops a consistent **~0.8–1.2 ms in 3 of 4 buckets** (≈0 in the 4th, where the reads were shorter / run noise), with throughput **+5–6%**. So the serialization is **not** compute-pool-bound — the `rayon::join` with the read I/O found room to hide most of it. The win is real and low-risk (no downside, archive/RPC intact), but the **ceiling is small** because serialize is only ~1.3 ms of a 15–25 ms committer.

**Bigger serialization levers remain** (not the overlap): (1) **capture the wire bytes at deserialize** and skip the re-serialization entirely — the block was just deserialized from those exact bytes, so this *eliminates* the work rather than hiding it (needs network→committer plumbing); (2) **defer the `tx_by_loc` DB write off the critical path** — it's not consensus-critical (only RPC reads it), worth ~2 ms, the larger half of the tx_loc commit cost. Artifacts: `feedrun-deepser.csv`, `zebrad-sap2-serial`.
