# Optimization experiments — checkpoint sync (1.7M sandblast)

Baseline (reused, not re-run each time): **stock no-fork** `sync-perf-main-2`+#144, `metrics-prof.prom`.
Over the 1.722–1.735M Sapling-spam window: **precompute 9.64 ms/blk, graft 3.49 ms/blk**.
All comparisons are the peer-independent precompute timer (= off-committer bulk Pedersen hashing).

## Trick #1 — `target-cpu=native` (free SIMD) — DONE ✅

Rebuilt stock no-fork with `RUSTFLAGS=-C target-cpu=native` (CPU has AVX2; no AVX-512) + frame pointers.

| metric (1.722–1.735M) | baseline (x86-64) | native (AVX2) | speedup |
|---|---|---|---|
| precompute (Pedersen) ms/blk | 9.64 | **8.53** | **1.13× (−11.5%)** |
| graft ms/blk | 3.49 | 3.32 | 1.05× |

**Result: a free ~13% on the Pedersen hashing, from a single recompile flag.** The gain is in the jubjub field arithmetic inside the rayon hash jobs (flamegraph: hash jobs 84% inclusive; BLAKE2b only 1.6%, memcpy 0.4% — so it's auto-vectorized field math, not BLAKE2b/memcpy).

**Why it matters:** unlike the lookup-table fork (compute→memory tradeoff, lost half in-node to cache), this is a **compute-side** gain (more arithmetic/cycle on the AVX2 units) and it **translated fully in-node** — confirming SIMD is the better hashing lever. It should **compose with the fork** (native + 7MB ≈ 1.13 × 1.51 ≈ ~1.7×). Auto-vectorization is limited (carry chains don't vectorize), so the bigger prize is **hand-written batched-SIMD Pedersen upstream**. Flamegraph: `flame-native-avx2.svg`.

## Trick #2 — perf-stat cache counters — BLOCKED ⛔

This VM exposes **no hardware PMU counters** (`LLC-load-misses`, `cache-misses`, even `cycles`/`instructions` → `<not supported>`), so direct L3-miss measurement is infeasible here.
**Substitute (done):** the 7MB-vs-60MB table A/B is the behavioral test of the cache hypothesis — the 7MB table (fits L3) realizes 1.51× vs the 60MB's ~1.32×, *modestly* better. Both lose ~half their micro-bench, so the in-node degradation is only **partly** L3 eviction (also L2 pressure / bandwidth / micro-bench optimism). See SAPLING_HASH_RESULTS.md.

## Trick #3 — rayon pool oversubscription (`RAYON_NUM_THREADS`) — DONE (neutral) ◻️

Premise was: two all-core rayon pools (`COMMIT_COMPUTE_POOL` + global verify pool) = 2× oversubscription on 8 cores → possible scheduling/spin-wait overhead. Tested `RAYON_NUM_THREADS=4` (halve the global pool) on stock `zebrad-prof`.

| metric (1.722–1.735M) | baseline (default) | RAYON_NUM_THREADS=4 |
|---|---|---|
| precompute ms/blk | 9.64 | 9.45 (≈unchanged) |
| graft ms/blk | 3.49 | 3.41 (≈unchanged) |
| throughput blk/s | 49.6 | 53.0 (peer-noise) |

**Result: neutral.** No measurable effect on the crypto. Two reasons:
1. **The spin-wait premise was a DWARF artifact.** The clean frame-pointer flamegraph (trick #1/7MB) showed the rayon leaves are `StackJob`/`HeapJob` *executing the hashing*, not `execute<SpinLatch>` — i.e. **no significant spin-wait** to recover. The earlier "65% SpinLatch" was a DWARF mis-unwind, now disproven.
2. **`RAYON_NUM_THREADS` only resizes the global pool**, not `COMMIT_COMPUTE_POOL` (hardcoded to `available_parallelism`), where the bulk Pedersen hashing actually runs. So this knob can't test compute-pool oversubscription. The precompute timer being unchanged confirms it didn't touch the hashing.

The throughput +7% is within single-run pinned-peer noise (~15% run-to-run), not attributable.

**Follow-up (code):** a real oversubscription test needs an **env-gate on `COMMIT_COMPUTE_POOL` size** (e.g. `nproc-2` or a fraction) so it can be sized to leave cores for the verify pool. Low priority given the spin-wait premise is debunked, but it's the only way to actually measure pool contention.

## Summary

| trick | outcome |
|---|---|
| #1 target-cpu=native | ✅ free **1.13×** on Pedersen (compute-side, translates in-node) |
| #2 cache counters | ⛔ blocked (no PMU); 7MB-vs-60MB A/B is the substitute (table effect modest) |
| #3 rayon oversubscription | ◻️ neutral; spin-wait premise debunked; real test needs a pool-size env-gate |

**Takeaway:** the only free win here is `target-cpu=native` (~13%), and it confirms the meta-lesson — **compute-side (SIMD) levers translate in-node, memory-side (table) levers don't.** Ship `native` + the 7MB fork together (compose to ~1.7× on hashing). The durable next step is hand-written batched-SIMD Pedersen + Sinsemilla upstream; for wall-clock sync time, more cores.
