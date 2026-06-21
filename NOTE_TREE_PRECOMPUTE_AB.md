# #144 note-tree-precompute — A/B verdict (2026-06-19)

Re-validation of the note-commitment-tree precompute (#144) over the 1.707M→1.730M checkpoint
range, with both the feed and the committer thoroughly instrumented. Resolves the inconsistent
prior reads.

## Setup

- Binary: `/root/wal-bench/zebrad-treepre-instr` — `proto-note-tree-precompute` (#144) +
  feed-verifier instrumentation (`checkpoint.rs`) + committer-utilization instrumentation
  (`write.rs`, accounting for the look-ahead VecDeque) + `commit-metrics` feature +
  `NOTE_PRECOMPUTE_DISABLE` toggle. Worktree: `/root/zebra-treepre-instr`.
- Baseline: `feedrun-feed2.csv` (`zebrad-feed2` = `sync-perf-main-2` + identical instrumentation,
  no #144). Valid baseline: only the #144 diff differs; instrumentation is identical.
- Harness: `feed_run.sh`, hard-link fork of the 1.7M snapshot, scrape every 5s. Windows compared
  by **height** (not elapsed) so the two runs cover the same blocks.
- Robust vs noisy: committer-thread metrics (commit/update_trees ms, util) and within-run ratios
  (poll_empty) are peer-independent. Absolute throughput / download rate / in_flight are
  peer-draw-dependent — single-run deltas are NOT attributable (handoff: N≥3 for abs blk/s).

## The bottleneck moves within 1.7–1.73M (this is why prior answers flip-flopped)

| sub-region | gate | baseline committer util | baseline CPU | starved? |
|---|---|---|---|---|
| HEAVY 1.708–1.718M | **serial committer** | 99% | **2.97/8** (5 idle) | no (0%) |
| LIGHT 1.721–1.729M | **download/feed** | 78% | 2.94/8 | yes (22%) |

- Heavy region is **serial-committer-bound, NOT CPU-bound** — committer pegged at 99% while ~5 of 8
  cores sit idle, 1423-block backlog, never starved. update_trees = 75% of the 19.76 ms commit.
  This overturns the stale "CPU-saturated 7.75/8" any-order finding (older stack).
- Light region flips to **download-bound**: `in_flight` collapses far below the 1500 cap and the
  committer starves 22% of the time, with CPU still idle. The serial verifier (equihash+merkle
  ≈ 0.5 ms/block, ~2000 blk/s capacity) is never the gate — the limit is bursty peer **delivery**
  of large sandblast blocks, not verification CPU.

## #144 result (same height windows)

HEAVY 1.7085–1.718M:

| metric | baseline | #144 | robust? |
|---|---|---|---|
| committer util | 99% | 86% | ✅ |
| commit ms/blk | 19.76 | 16.25 | ✅ |
| update_trees ms/blk (on committer) | 14.73 | 9.33 | ✅ |
| poll_empty (committer starved) | 0% | 16.4% | ✅ (within-run) |
| throughput blk/s | 50.1 | 52.8 | ⚠️ peer-noisy |
| download blk/s | 52.7 | 56.0 | ⚠️ |
| CPU /8 | 2.97 | 3.38 | — |

LIGHT 1.721–1.729M:

| metric | baseline | #144 | robust? |
|---|---|---|---|
| update_trees ms/blk (graft) | 7.24 | 2.94 | ✅ |
| committer util | 78% | 49% | ✅ |
| poll_empty | 22.8% | 45.9% | ✅ |
| throughput blk/s | 72.4 | 55.5 | ❌ not attributable (peer noise) |
| CPU /8 | 2.88 | 2.49 | — |

## Conclusions

1. **#144 does its job (robust):** it pulls tree hashing off the committer. update_trees on the
   committer drops 14.73→9.33 ms (heavy) and 7.24→2.94 ms (light, clean graft); committer util
   falls 99→86% (heavy) and 78→49% (light). Byte-identical, validated.
2. **Throughput barely moves, and the gate moves to DOWNLOAD — not verification CPU.** Smoking gun:
   relieving the committer pushed heavy poll_empty 0%→16.4% (committer now *starves for input*),
   download 56 ≈ throughput 53, CPU stayed ~3/8. The precompute pool does NOT CPU-saturate; the
   verifier is trivial. The work didn't pile into verification — it exposed the **download ceiling
   (~53–67 blk/s)** that always sat just behind the committer.
3. **The light-region throughput drop (72→55) is NOT attributable to #144** — single run, download-
   bound region, download itself fell 66.8→60.5 (peer draw). The committer metrics carry the verdict.
4. **Heavy update_trees only fell to 9.33 ms (not the ~3 ms graft seen in light).** Likely the
   bursty feed in the committer-bound region often has no next block ready to precompute → inline
   fallback; the 1-block look-ahead under-pipelines exactly when the committer is the gate.

## Does #144 make sense? Recommendation

- **Keep it** — correct, validated, and it genuinely reduces committer load. But in this region its
  throughput ROI is **gated by download** (~55 blk/s), so on its own it buys ~5% here.
- **To realize #144's gain, raise download throughput first or in tandem** (the real next lever for
  1.7–1.73M): `in_flight` collapses below cap, bursty peer delivery — more concurrent body fetch /
  better peer selection / pipelined fetch. This is independently corroborated by COMMIT_OPTIMIZE
  ("download ~60 blk/s next gate").
- **Re-value #144 in the DEEP sandblast (1.8–1.9M)** where the committer tree update is 11–39 ms
  (committer ≫ download), so committer relief has headroom before hitting the download ceiling.
  Use N≥3 for any throughput claim.
- Optional #144 tuning: a deeper look-ahead (precompute K blocks ahead on the idle cores; the
  precompute is keyed only on note counts, so blocks are independent) would close the heavy-region
  9.33→~3 ms gap — but only matters once download is no longer the co-gate.

## Update — pinned-peer A/B (167.99.162.47), same binary toggled, 2026-06-19

Ran a clean same-binary, same-peer A/B (feed_run_pin.sh, peer 167.99.162.47) to remove swarm noise:
`feedrun-pin-on.csv` (#144) vs `feedrun-pin-off.csv` (NOTE_PRECOMPUTE_DISABLE=1).

| window | arm | thr | util | commit ms | utree ms | empty | download |
|---|---|---|---|---|---|---|---|
| HEAVY | OFF | 48.5 | 96% | 19.86 | 14.96 | 6% | 52.5 |
| HEAVY | ON  | 54.1 | 85% | 15.79 | 9.07 | 17% | 59.8 |
| LIGHT | OFF | 43.6 | 48% | 10.96 | 7.50 | 54% | 44.4 |
| LIGHT | ON  | 53.7 | 48% | 8.93 | 2.96 | 47% | 52.7 |
| FULL  | OFF | 48.5 | 74% | 15.22 | 11.09 | 34% | 49.9 |
| FULL  | ON  | 57.5 | 70% | 12.23 | 6.16 | 31% | 57.5 |

- **#144 committer relief reproduced (robust):** update_trees 14.96→9.07 (heavy), 7.50→2.96 (light);
  commit 19.86→15.79; util 96→85%. Three runs agree.
- **Throughput STILL confounded — even pinned.** In every window throughput ≈ download rate, and the
  single pinned peer's delivery rate VARIED between runs (OFF dl 49.9 vs ON dl 57.5 full-range,
  ~15%). Pinning removes peer-SELECTION noise, NOT the one peer's own rate variance. The LIGHT region
  is the tell: committer only 48% utilized in BOTH arms (download-bound), yet ON is +23% — that gain
  cannot be committer relief, it's the feed. So abs throughput needs N≥3 even pinned.
- **Cleanest #144 metric = committer CAPACITY (1000/commit_ms), download-independent:**
  heavy 50.4→63.3 (+26%), light 91→112 (+23%), full 66→82 (+25%). #144 buys ~25% committer capacity;
  it converts to throughput only where download has headroom (heavy +12% real; rest is feed variance).
- **Precompute-wait hypothesis (user):** data says feed, not precompute-stall — light region grafts
  cleanly (utree 2.96≈full hit) yet committer 48% idle / empty 47% = waiting on FEED. Heavy
  utree 9.07 ⇒ ~48% precompute HIT rate (half fall back to inline) because bursty/drained feed leaves
  no next block to pre-start. NOT YET directly instrumented.
- **NEXT (recommended):** (1) add precompute hit/miss counter + rx.recv() wait timer, rebuild, and
  re-measure in DEEP sandblast 1.8-1.9M (committer tree 17-39ms ≫ download) where #144's ~25%
  capacity has headroom to show in throughput AND the counters settle the precompute-wait question;
  (2) N≥3 per arm for any abs-throughput claim in this region.
Binaries: zebrad-treepre-instr (+ NOTE_PRECOMPUTE_DISABLE). Harness: feed_run_pin.sh.
