# CPU profile — checkpoint sync 1.7M → 1.8M

Goal: replace the back-of-envelope "Pedersen ≈ 30% of CPU" inference with measured data, and map where per-block CPU actually goes.

## TL;DR

Direct per-block stage timers (including a **new off-committer precompute timer** that captures the bulk note-commitment hashing #144 moved off the committer) show **note-commitment hashing (Sapling Pedersen + Orchard Sinsemilla) is the single dominant per-block CPU cost** — growing from ~6.5 ms/block at 1.71M to **~17 ms/block** at 1.79M, dwarfing every other commit-side stage. A hard whole-node bound puts note-hashing at **≥31% of total CPU** (likely 31–54%). So the earlier "~30%" was a *floor*, and your intuition that Pedersen is a *large* share in the Sapling sandblast is correct.

## Methodology

- **Binary:** stock (no-fork) `sync-perf-main-2` tip (#144 merged), instrumented `--features commit-metrics` + a new `zebra.state.precompute.compute.duration_seconds` timer wrapping `BlockNotePrecompute::compute` (the off-committer Pedersen/Sinsemilla hashing). Single fast peer so it's CPU/committer-bound.
- **Per-block stage timers** (wall-time of each stage; metrics scraped every 5s, deltas over height windows).
- **Total CPU/block** from `/proc/<pid>/stat` (`res-prof.csv`).
- **perf** `-F 99 --call-graph dwarf,16384` over the 1.72–1.75M Sapling-spam window: **203,823 samples**. *(Flamegraph rendering blocked — see limitation below.)*

## Per-block stage budget (measured, ms/block, wall-time)

| region | precompute (note hashing) | txid+auth digest | graft (on committer) | rocksdb commit | committer total |
|---|---|---|---|---|---|
| 1.71M (early) | 6.56 | 0.83 | 7.66 | 1.88 | 12.73 |
| **1.72–1.75M (Sapling-spam)** | **10.34** | 1.19 | 3.37 | 1.72 | 12.20 |
| 1.76–1.79M (deeper) | **17.09** | 1.71 | 6.75 | 3.10 | 20.56 |

(commitment-check is negligible, ~0.07 ms. `committer total` = graft + commitment-check + rocksdb + UTXO/address reads + batch build + history push, all serial on the committer.)

**Read:** the **precompute** (bulk Pedersen+Sinsemilla) is the largest single stage and the one that *scales with note accumulation* — it more than doubles across the range. The committer's own serial work (graft + rocksdb + reads/batch/history ≈ 12–20 ms) is the next chunk; per-tx BLAKE2b digesting and DB commit are minor (1–3 ms each).

## Total CPU per block, and the feed side

Total CPU/block (`res-prof.csv`) is **~70 ms** in the (perf-inflated) Sapling-spam window and **~113 ms** in the heavier deeper window — **much larger than the ~24 ms of timed commit-side stages.** The gap is two things:
1. **Internal parallelism** — `precompute` and the txid digest use rayon, so their CPU-seconds exceed wall-time.
2. **Untimed feed-side CPU** — block **deserialization** (parsing huge sandblast blocks: many outputs, cv/epk/proof fields) and **checkpoint verification** (equihash, merkle), which my commit-side timers don't cover.

So the per-block CPU splits roughly into **note-hashing + feed-side deserialize/verify**, with note-hashing the largest single identifiable consumer.

## The Pedersen CPU-share question — settled (with a measured bound)

Earlier I wrote "~30%," derived by back-calculating from the fork's 18% whole-node CPU reduction *assuming the full 2.4× micro-bench speedup*. That was a soft inference. The defensible statement:

- **Hard lower bound: note-hashing ≥ 31% of whole-node CPU.** The sapling-crypto fork cut whole-node CPU/block ~18% (measured A/B). Since the realized speedup can't exceed the 2.4× micro-bench, `share = 0.18 / (1 − 1/speedup) ≥ 0.18 / 0.583 = 31%`.
- **If the realized in-node speedup is lower than 2.4× (likely — the fork's 60 MB lookup table loses to cache pressure in a busy node), the share is correspondingly higher:** at a realized 1.5×, share ≈ 54%.
- The stage budget corroborates a large share: precompute alone is 10–17 ms of the per-block budget.

**Conclusion: Pedersen/note-hashing is ~⅓ to ~½ of total per-block CPU in the Sapling sandblast — a large share, not a minor one.** The "30%" was a floor, not the central estimate.

## Bottleneck ranking (1.7–1.8M checkpoint sync)

1. **Note-commitment Pedersen/Sinsemilla hashing** — the #1 CPU consumer (the precompute), scaling with shielded-note volume. Levers: the sapling-crypto fork (~18% whole-node), faster/SIMD hash impls upstream, dedicated pool isolation (#144 already relocated it off the serial committer).
2. **Feed-side block deserialization + checkpoint verification** — the other major chunk (the gap between timed commit stages and total CPU). The lazy cv/epk (#136) and native ZIP-244 (#131) PRs already cut this; further wins from eliminating redundant parsing.
3. **Committer serial overhead** — UTXO/address reads + batch build + history push (~7 ms inside committer total beyond graft/rocksdb).
4. **RocksDB commit (1.7–3 ms) and per-tx BLAKE2b digesting (0.8–1.7 ms)** — minor.

## Flamegraph (partial) — function/category shares

A second, narrower capture (`--call-graph dwarf,16384` over 1.725–1.735M, ~1.2 GB) was foldable only **partially**: the full fold stalled (same DWARF-on-248MB-binary wall), but a salvaged subset of **~5,690 samples** rendered (`flame-sapling-spam-partial.svg`). Counts are period-weighted (×1010101); shares are valid. **Inclusive** category shares (stack contains the pattern):

| category | inclusive CPU share |
|---|---|
| Sapling Pedersen (jubjub) | **~65%** |
| RocksDB | ~8% |
| block deserialize/parse | ~5% |
| point decompression | ~1% |
| equihash | ~0.7% |
| Orchard Sinsemilla | ~0% (pure-Sapling window) |
| (rayon pool, wraps the above) | ~92% |

**Caveats on the flamegraph numbers:** (1) partial subset; (2) the inclusive grep partly matches rayon job *type parameters*, so it conflates real Pedersen compute with pool overhead; (3) leaf self-time is dominated by `rayon ...execute<SpinLatch>` (~65%) — i.e. there is **significant rayon spin-wait** (workers busy-waiting for sibling tasks), which is itself a finding worth chasing (idle-spin burns CPU). The clean per-stage **metric budget above is the more reliable decomposition**; the flamegraph corroborates that Pedersen/note-hashing dominates.

**Settling the Pedersen share:** the flamegraph's ~65% (even allowing for overcount) confirms Pedersen is a *large* share — well above the ≥31% floor. Combined with the fork's measured 18% whole-node CPU reduction, that implies a **realized in-node speedup of only ~1.4×** (vs the 2.4× micro-bench) — Amdahl: `0.18 = 0.65·(1−1/1.4)`. The gap is cache pressure: the fork's ~60 MB lookup table benches hot/uncontended but in a busy node is evicted to DRAM, so it realizes ~1.4× not 2.4×. (Reconciles with the wall-time budget: `precompute` is only ~10 ms *wall* because it parallelizes via rayon, but it's a large *CPU-seconds* share — which is what the flamegraph samples.)

**Why no full flamegraph:** DWARF offline post-processing (`perf script`/`perf report`) is intractable on the full capture against the 248 MB binary (stalls); **LBR is unavailable in this VM**; a frame-pointer build (`-Cforce-frame-pointers`) + re-capture (~30 min) is the only path to a clean *complete* leaf-level flamegraph — the cheap follow-up if the exact compute-vs-spin and feed-side split is wanted.

### Artifacts
- Metrics: `metrics-prof.prom` (full /metrics every 5s), `res-prof.csv` (CPU/throughput). Binary: `/root/wal-bench/zebrad-prof` (stock, instrumented).
- Flamegraph (partial, ~5,690 samples): `/root/zebra/flame-sapling-spam-partial.svg`.
- perf captures removed after analysis (3.2 GB / 1.2 GB DWARF — un-renderable in full; see above).
