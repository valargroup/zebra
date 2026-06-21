# Hedged head-of-line download — benchmark results

**Branch:** `proto-hedged-hol-download` (binary `/root/wal-bench/zebrad-hedge`, built `--features commit-metrics`).
**PR:** #151 (`hedge-hol-rebased` → `proto-note-tree-precompute`).
**Method:** single binary, env-toggled `SYNC_HOL_HEDGE_FANOUT=0` (baseline) vs `=4` (hedged), **random DNS peers** (the stall only manifests with diverse/churning peers — a pinned peer never reproduces it). Interleaved off/on/off/on/off/on so temporal peer drift hits both arms equally. 7.5-min fork windows from the 1,707,210 snapshot. `checkpoint_verify=1500`, `download=150`. Harness: `hedge_ab.sh`.

## Per-run data (N=3 per arm)

| run | Δblocks (7.5 min) | stall intervals (blk/s<2 & in_flight>1000) | reg_miss | all_missing | route_hedge win | steady blk/s |
|---|---|---|---|---|---|---|
| OFF-1 | 10,899 | 18/84 | 97,676 | 380,894 | — | 27.9 |
| OFF-2 | 10,539 | 21/83 | 93,517 | 364,439 | — | 25.4 |
| OFF-3 | 22,438 | 9/84 | 50,060 | 195,060 | — | 68.9 |
| **ON-1** | 18,316 | 7/81 | 43,328 | 62,469 | 17,990 | 45.4 |
| **ON-2** | 19,434 | 12/84 | 44,729 | 57,630 | 18,295 | 50.8 |
| **ON-3** | 28,213 | 3/84 | 0 | 7 | 0 (inert) | 64.7 |

## Medians (OFF → ON)

| metric | OFF | ON | Δ |
|---|---|---|---|
| stall intervals | 18 | 7 | **−61%** |
| reg_miss | 93,517 | 43,328 | **−54%** |
| **all_missing** (stale-marker fails) | 364,439 | 57,630 | **−84%** |
| Δblocks per 7.5-min window | 10,899 | 19,434 | **+78%** |
| steady-state blk/s | 27.9 | 50.8 | +82% |

## Verdict — the hedge works, and is well-behaved

**It does exactly what it was designed to do, confirmed across N=3:**

1. **Active when peers thrash.** On the two bad draws (ON-1, ON-2), the baseline equivalent would have accumulated ~360k `all_missing` synthetic failures; the hedge fired (`dispatch` ~140k per-peer, **~18k wins**), bypassing the stale "missing" inventory markers and delivering the head block from a real ready peer. Result: `all_missing` −84%, `reg_miss` −54%, stalls cut, ~+78% more blocks committed in the window.

2. **Inert when peers are clean.** ON-3 drew a healthy peer set with **0 registry-misses** — the hedge stayed at 0 dispatches and matched the best baseline draw (OFF-3: 68.9 vs ON-3: 64.7 blk/s). No overhead, no regression when there's nothing to fix.

**This contradicts the handoff's "honest risk"** that #105 might already absorb the stall: on bad draws the baseline still thrashed hard (364k `all_missing`, 18–21 stall intervals), and the hedge sharply reduced it. #105 (let markers age out during the 2s backoff) and the hedge (bypass the markers entirely on retry) are complementary — the hedge attacks the residual cases #105 doesn't resolve within budget.

## Mechanism evidence (`route_hedge` counters, bad-draw arms)

- `dispatch` ~136k–147k per-peer requests, `win` ~18k, `exhausted` ~117k–127k. So ~12–13% of per-peer hedge requests delivered the block; the rest exhausted and fell back to the unchanged #105 backoff. Even at that win rate, `all_missing` collapsed −84% and throughput rose — because each win resolves a head-of-line block that would otherwise have stalled the strictly-ordered commit for a full 2s backoff cycle.

## Honest caveats

- **Throughput is peer-draw-dependent.** The +78% Δblocks / +82% steady-state are real within these runs but confounded by which peers each window drew (the ON arm happened to also escape cold-start faster on average). The robust, mechanism-level claims are the **`all_missing` −84%** and the **18k hedge wins** — these directly measure the stale-marker bypass and are not throughput-noise.
- N=3 per arm. More runs would tighten the medians, but the direction is consistent across every pair (each ON arm has far lower `all_missing` than every OFF arm except the clean ON-3, which had none to begin with).

## DoS posture (unchanged from the design)

Scoped to the single head-of-line hash in `registry_miss_retry`; small fanout (4) clamped to ready peers; `select_random_ready_peers` (random, load-ignoring, broadcast stance); losers cancelled on first win; no new retry budget; counts as one request against `download_concurrency_limit`.

## Recommendation

Ship-worthy as a prototype. The lever is validated: it converts stale-marker `all_missing` failures into deliveries and reduces head-of-line stalls, with zero overhead on clean draws. Next tuning (per handoff §7): cut the 2s backoff for hedged retries (the fanout already addresses the root cause, so the wait is mostly wasted), and/or latency-aware peer selection to raise the floor.
