# Checkpoint-zone sync from the 1.7M snapshot — findings & plan

> ## ⚠️ STATUS (2026-06-18): this document is HISTORICAL — read this banner first
>
> Everything below is the investigation up to **2026-06-17**. Since then the work
> shipped and the bottleneck **moved**, so several "not yet built" levers here
> (esp. §419 and §9–§14) are now **done or disproven**. Do not re-investigate them.
>
> **Shipped / built since (PR stack on the fork, `valargroup/zebra`):**
> - §419 lever 1 (parallelize the tree update + isolate it from the verify pool) →
>   **shipped**: dedicated `COMMIT_COMPUTE_POOL` (#122) + parallel batch tree append.
> - ZIP-244 auth-data-root / commitment check parallelized + hoisted off the serial
>   committer into the concurrent download tasks → **shipped** (#121, #124, #127),
>   and the `to_librustzcash` txid/auth conversion **de-duplicated** (#125).
> - Parallel writer-batch serialization (raw tx bytes + block size) → **shipped** (#128).
> - §419 levers 2 & 3 (pipeline the writer / compute treestate in a pre-stage =
>   "any-order commit") → **BUILT + benchmarked, NO GAIN, ~10% slower** → parked as
>   draft **PR #129 (DO NOT MERGE)**.
>
> **Current bottleneck (the key change):** the heavy region (1.72–1.73M) is now
> **CPU-saturated (~7.75/8 cores, downloads fully buffered)**. The limiter is
> **total CPU work across the whole sync pipeline**, *not* the serial commit stage.
> So commit-side restructuring (incl. the pipeline) can't help while CPU-bound;
> the only lever is **reducing total CPU work**.
>
> **Next levers (see PARALLEL_IDEA.md → "Reducing total CPU work"):** (1) profile the
> heavy region; (2) investigate whether per-tx txid computation can be skipped in
> checkpoint sync (biggest potential win — eliminates the `to_librustzcash` reparse);
> (3) else native ZIP-244 digests (skip the reparse). Note the de-dup is already done
> (#125); native digests are the step beyond it.
>
> **Authoritative current sources:** `ANY_ORDER_COMMIT_DESIGN.md` §7d (measured
> any-order result + why CPU-bound) and `PARALLEL_IDEA.md` top ("UPDATE 2026-06-18").

**Date:** 2026-06-16 (updated 2026-06-17 — see §6 for the shipped head-of-line fix)
**Baseline:** `ironwood-main` @ `94ae42f48` (release); as of 2026-06-17 advanced to `3a5035904`
(PR #102 retry-instead-of-restart squash-merged upstream). Active stack:
`ironwood-main` → #104 `sync-continuous-refill` (`c4672eed0`) → #105 `fix-sync-head-of-line-priority`
(`3a385b862`), both MERGEABLE.
**Snapshot:** `/mnt/roman-dev-2-data/zebra-ckpt-master` — mainnet height **1,707,210** (below the max
mainnet checkpoint 3,358,006, so forward sync exercises the **checkpoint verifier**)
**Harness:** `/root/wal-bench/prbench.sh LABEL BIN 420 5` — 7-min fork-runs scraping height,
`sync_downloads_in_flight`, `sync.missing.block.*`, and restart events.
**Bench config (all runs):** `checkpoint_verify_concurrency_limit=1500`, `download_concurrency_limit=150`
(pinned explicitly so the unmeasured PR2 default bump doesn't confound the code comparison).

---

## Key findings

- **Sync is not resource-bound.** Steady state from the snapshot: CPU ~17–28% (equihash bursts to
  ~6.7/8 cores then idles), network duty cycle ~33–53%, state commit ~4% of wall, disk block-I/O wait
  **0.00s even after `drop_caches`**. Every resource is mostly idle on average.
- **The dominant cost is cold-start restart-thrash.** For the first 1–3 min the node can't fetch the
  head-of-line block, hits `NotFoundRegistry`, does `cancel_all` + 10s restart, and **discards the
  in-flight pipeline** — repeating. This burns **100–200s of a 300s run**. This, not steady-state
  pipelining, is the real lever.
- **PR A2 fixes the thrash.** Keep the pipeline and retry the head block (with backoff) instead of
  restarting. Result: **`restart_waits` 6–17 → 0 in every run**, ~8.4k blocks vs baseline median ~6.2k.
- **The validated win is PR A2 — *not* PR C.** With advertisers absent (`busy=0`) or saturated
  (funnel), PR C's routing never produced a clean isolated win; the prC numbers are attributable to
  PR A2's retry. PR C's registration may not even be engaging.
- **Phase-1 (`AdvertisersBusy`) was tested and REVERTED — it's a regression.** With plentiful peers it
  funnels all downloads onto the 3 advertisers and stalls (see matrix). Reverted; only its typed
  `NotFoundClass` accessor (replacing brittle `Debug`-string matching) is kept.
- **The residual worst case is peer scarcity, not a code bug.** The prF1 176s freeze was peer-bound:
  **8–9 outbound peers, 89% handshake failure, crawler added 0 peers in 176s.**
- **Correction — it is NOT "all peers genuinely lack the block."** During the freeze there were
  **162 synthetic `NotFoundRegistry` misses vs 1 real peer `NotFoundResponse`** — the head block was
  almost never actually *asked* of a peer. `NotFoundRegistry` fires both when peers are marked-missing
  *and* when no peer is free; with `in_flight≈1997` look-ahead blocks saturating 9 peers, it's
  "no free peer," compounded by the registration gap. **Local self-saturation, not genuine absence.**
- **Config bump (2000/150) was never measured** — every run used 1500/150. A *deeper* look-ahead
  plausibly *worsens* the worst case (more look-ahead saturating peers, starving the head block).
- **CORRECTION (2026-06-17) — the stall is marker-staleness, not saturation.** The working
  `pool.route_inv.*` counters (§6) show `no_ready=0` across every instrumented run and `all_missing`
  12–19×: peers were *never* saturated; every synthetic miss was "ready peers exist but ALL marked
  missing the hash." The "local self-saturation" reading above is superseded — the real mechanism is
  inventory-marker staleness, and the worst-case lever is the inventory registry / head-of-line
  priority, **not** peer acquisition or buffer depth. See §6.

---

## Data

### Round 3 matrix — Δblocks from 1,707,210 (7-min runs; peer-noisy, 5–10× run-to-run)
| binary | Δblocks | restart_waits | registry_miss | note |
|---|---|---|---|---|
| baseline | 6185 / 8480 / 4998 / 6483 / 7894 | 6–17 | n/a | restart-thrash |
| **PR A2** | 8649 / 8461 / **222** | **0 / 0 / 0** | 1 / 19 / **203** | thrash gone; `222`=peer-bound edge |
| A2 + PR C | 8331 / 8568 / 8352 | 0 / 0 / 0 | 45 / 1 / 2 | consistent, but win is A2's |
| **phase-1** (reverted) | prF1 3191 · prF2 **494** · prF3 **385** | — | 162 / 0 / 0 | funnel: busy 0 / **806** / **440** |

- **PR A2 → restart_waits 0** is the robust signal (holds across all runs regardless of peer luck).
- **Phase-1 funnel:** prF2 (25 peers) and prF3 (22 peers) collapse to ~385–494 blocks (~20× worse)
  because `has_advertiser` is true for ~every hash, so requests 4…150 all `Busy`-defer onto 3
  advertisers instead of guessing a ready peer that has the block.

### prF1 stall signature (CSV)
- Frozen **176s** at height 1,710,401; `in_flight=1997`, `reserve=0`, `busy=0`.
- `registry_miss` climbs **every 2s** (the backoff cadence) → head block retried locally, never served.
- **162 synthetic registry-misses : 1 real peer refusal** → block was barely ever put on the wire.
- Never escaped (killed by the 420s wall cap). Two distinct blocks stalled across the run, not one.

---

## Status of each change

| Change | State | Verdict |
|---|---|---|
| **PR A2** (retry-instead-of-restart) | built, in main tree | **Proven. Ship.** |
| **F1** (split Response/Registry retry counters) | built | Ship (A2 correctness) |
| **Q4** (`SYNC_RESTART_DELAY` 30→45s) | built | Ship (fixes real `ensure_timeouts_consistent` failure) |
| **typed `NotFoundClass`** accessor | built | Ship (standalone robustness win) |
| **PR C** (register multi-block invs as advertised, `handshake.rs`) | built | **Defer** — unproven, registry-bloat tradeoff |
| **config** (`checkpoint_verify=2000`, `download=150` defaults) | default only | **Defer** — never measured, may hurt worst case |
| **phase-1** (`AdvertisersBusy`) | reverted | Dropped — funnel regression |
| **PR B** (evict non-serving peers) | not built | Contraindicated — worst case isn't lie/prune |
| **pre-fetch producer** (eager `EXTEND_PREFETCH_WATERMARK`) | **built but stale & unbenched** (`/mnt/roman-dev-2-data/zebra-pr3`, missing F1/Q4) | Prioritized — needs rebase + build + bench |
| **continuous-refill** (`tokio::select!` `sync_round` rewrite) | **PR #104, rebased onto `ironwood-main`, MERGEABLE** | Unparked — it is the base the head-of-line fix needed (the `select!` loop hosts the non-blocking retry arm) |
| **head-of-line priority** (`sync.rs` gate + non-blocking backoff + `pool.route_inv.*` counters) | **built + benched + shipped; PR #105 on #104** | **Ship. 0/13 stalls, no regression.** Mitigation, not cure — see §6 |

---

## Plan

### 1. Ship now — the measured restart-thrash fix (`zebrad`-focused)
- **PR A2 + F1 + Q4 + typed `NotFoundClass`.**
- Scope the claim honestly: *eliminates cold-start head-of-line restart-thrash* (`restart_waits → 0`).
  It does **not** fix the peer-bound worst case — that's separate, deferred work.

### 2. Defer (hold, don't delete) — until isolated
- **PR C** (`handshake.rs` multi-block inv registration) — hold for a clean A2-vs-A2+C bench; its
  routing benefit is unconfirmed (`busy=0`) and it carries a registry-bloat tradeoff.
- **Config tweaks** (2000 / 150 defaults) — leave defaults unchanged in the shipped PR; bench
  separately later, and only in the *smaller-buffer / head-of-line-priority* direction the stall
  evidence points to, not bigger-for-throughput.

### 3. Next investigation — one cheap debug run (decides PR C + worst-case lever) — ✅ ANSWERED (§6)
- Re-run with `route_inv` arm counters (**"no ready peer" vs "all marked missing"**) + a
  `register_inventory_status` registration counter.
- Settles: does PR C actually populate advertisers (`busy=0` lead)? Is the stall saturation, marking,
  or genuine absence? → tells us whether the worst-case lever is **head-of-line priority**, **peer
  acquisition**, or a **PR C fix**.
- **RESOLVED (2026-06-17):** the `pool.route_inv.*` counters were built (replacing the broken
  `zcash.net.*` ones — see PR C fate §) and run. Verdict: **`no_ready=0`, `all_missing` 12–19× → the
  stall is marker-staleness, and the lever is head-of-line priority / the inventory registry.** Not
  saturation, not genuine absence, not peer acquisition. Full analysis and the shipped fix in §6.

### 4. Prioritized build — pre-fetch producer
- Rebase the `zebra-pr3` eager-prefetch onto the current stack → build → bench (N≥3).
- Targets the steady-state sawtooth (overlap the next FindBlocks hash-fetch with downloads so the
  buffer stays fed). *Caveat:* steady state is already healthy; this is a steady-state lever, and a
  fuller buffer may interact with the saturation worst case — measure both throughput **and**
  worst-case recovery.

### 5. Built + benched — continuous-refill (parked as draft PR #104)
Built on top of the pre-fetch producer: replaced the sequential `try_to_sync_once` (drain → extend →
dispatch) with a single `tokio::select!` `sync_round` overlapping draining completed downloads, one
in-flight tip extension (`build_extend`, a self-contained refactor of `discover_extend_hashes`), and
dispatch. Branch `sync-continuous-refill`, draft PR #104 against `fix-sync-restart-thrash`.
(Implementation note: the optional-extension `select!` arm must use `OptionFuture`, not a guarded
`.expect()` — `select!` evaluates a branch expression even when its `if` precondition is false, which
panicked the first benched build within ~40s. Fixed; unit tests didn't catch the interleaving.)

**Result — throughput increased and became less variable, but the head-of-line stall persists.**
- **Post-first-commit rate** (Δblocks ÷ (wall − escape); factors out cold-start peering), N=3 healthy
  draws each, identical config (`checkpoint_verify=1500`, `download=150`):
  - pre-fetch only:        **22.8 / 20.9 / 17.2** blk/s  (median 20.9, wide — one weak draw)
  - + continuous-refill:   **22.2 / 22.8 / 22.2** blk/s  (median 22.2, tight)
- Throughput is **on par to slightly higher and notably less variable**; mean in-flight rises
  **~1705 → ~1915 (+12%)** — the buffer is reliably fuller across FindBlocks round-trips (the intended
  mechanism). 0 restarts, 0 restart_waits, no panics on the full runs.
- **But it does NOT remove the head-of-line / peer-scarcity stall.** A later batch drew a thin peer
  window: two runs froze at the cold-start head-of-line block (fin stuck at 66, in_flight pinned 499,
  registry-miss climbing on the 2s backoff to ~207) with **all local resources idle** — CPU ~0.01
  cores, net <0.25 MB/s, disk read 0, blkio-wait 0. Same signature as prF1 / prA2c / exp1. Refill
  neither causes nor fixes it: it optimizes buffer depth, which is not the bottleneck when no peer will
  serve the frontier block.

**Bottleneck characterization (from the resource-sampled runs — `prbench_res.sh`):**
- *Healthy steady state:* **verify/commit-bound at ~22 blk/s.** Downloads run a full buffer (~1500–2000)
  ahead of finalize and idle ~half the intervals against the lookahead cap; finalize is the steady
  metronome (0 multi-second stalls; per-interval Δfin never zero). **Not** network/CPU/disk bound
  (net <0.25 MB/s, blkio-wait 0, disk read 0) — most likely the serial state-writer (per-input UTXO
  reads + ordered commit), ~45 ms/block.
- *Thin-peer draw:* **peer-availability-bound** (head-of-line block unservable), everything local idle.

**Verdict: parked (draft PR #104).** The steady-state win is real but small and on the non-bottleneck
(download) side; the true levers are verify/commit serialization (healthy) and head-of-line / peer
acquisition (worst case), both untouched by refill. Keep the draft for if/when downstream work makes a
fuller pipeline matter.

---

## 6. Head-of-line-priority fix — BUILT, BENCHED, SHIPPED (PR #105) — 2026-06-17

Built on `sync-continuous-refill` (#104). Stack after rebase: `ironwood-main` (`3a5035904`, #102
squash-merged) → #104 (`c4672eed0`) → #105 `fix-sync-head-of-line-priority` (`3a385b862`).

**What it does** (confined to `sync.rs` + read-only counters in `set.rs` — no DoS-sensitive routing
change, per the locked decision):
- **Part A — diagnostics (`route_inv`):** four `pool.route_inv.*` counters (advertiser / maybe /
  notfound.no_ready / notfound.all_missing). Uses the `pool.*` prefix that scrapes correctly — this
  **fixes the exp1 counter-export bug** (the old `zcash.net.*` names never exported; see PR C fate §).
  Verified live: all series appear and increment.
- **Part B — fix:** while a required block is registry-missing (`registry_miss_retry` *map* non-empty),
  (B1) pause new speculative dispatch, and (B2) move the 2s backoff out of the inline blocking `sleep`
  into a non-blocking `biased` `select!` timer arm so the loop keeps draining/extending during the wait.

**KEY FINDING — the stall is inventory-marker staleness, NOT saturation.** This answers §3 and corrects
the original "self-saturation" root cause. Across all instrumented runs:

| run | no_ready | all_missing |
|---|---|---|
| hol1 / hol2 / hol3 | 0 / 0 / 0 | 12 / 12 / 3 |
| h7_2 (worst) | 0 | 19 |
| h7_5 / h7_6 | 0 | 2 / 1 |

`no_ready` was **0 everywhere** — peers were never saturated; ready peers always existed. Every
synthetic miss was `all_missing`: ready peers exist but ALL are marked-missing the hash, so `route_inv`
synthesizes `NotFoundRegistry` without hitting the wire and we wait out the ~53s/106s registry rotation.

**Implication: B2 (non-blocking backoff) is the actual fix; B1 (the saturation gate) targets a
condition (`no_ready`) that never fires** — cheap/defensive, may marginally cut marker creation, but
not what recovers the stall. base (continuous-refill, no HOL) still has the inline blocking `sleep`, so
on a miss it freezes the loop and re-hits the same marked peers ~200× (base3: registry_miss→201)
without progress; hol keeps the loop running so by retry the marker has aged / a different peer is ready.

**Benchmark** (420s fork-runs, thin-peer regime; `zebrad-hol` vs `zebrad-refill`):
- **base 1/6 stalled** (base3: ~300s freeze, 0.5 blk/s, registry_miss→201; base4/5 partial).
- **hol 0/13 stalled** (3 unconstrained + 3 A/B + 7 consecutive). Recovers from all 12–19 `all_missing`
  events per run by waiting them out.
- **No throughput regression:** both ~20–22 blk/s on healthy draws (hol 20.0–21.6; A/B tbase≈thol ~21.6).
- *Caveats:* stall is peer-draw-dependent, so no controlled same-peers A/B was achievable; base lacks
  Part A counters, so its stall mechanism is inferred (near-certainly the same `all_missing`).

**Resource characterization (steady state): commit-bound, not download/peer-bound.** `in_flight` pins
at the 1500 cap with `reserve` at 996 (both buffers full) while the instantaneous rate decays 33→16
blk/s over a run — the signature of a rising per-block state-commit cost (RocksDB growth +
note-commitment trees), i.e. the serial finalized writer + disk, not verification CPU. Confirms the §5
"verify/commit-bound at ~22 blk/s" reading.

**Is #105 the right solution, or is there a better one? (analysis 2026-06-17)** — #105 is correct and
the safe thing to ship, but it is a **mitigation** (waits out marker staleness), not a cure. The counter
data points to more-direct alternatives:
1. **Registry-marker fix (most root-cause):** in `InventoryRegistry` — a targeted `clear_missing` for
   the starved head-of-line hash, faster marker expiry, or make `NotFoundRegistry` non-terminal for the
   critical block. Eliminates `all_missing` at the source. *Downside:* DoS-sensitive peer-set code (the
   marker exists to avoid hammering peers that lack a block); needs its own bench + DoS review. This is
   the documented follow-up direction (cf. PR C fate §).
2. **Shrink the lookahead buffer (cheapest, data-supported):** since steady state is commit-bound with
   `in_flight` pinned at 1500, dropping `checkpoint_verify_concurrency_limit` to ~300–500 costs ~zero
   throughput (the consumer can't go faster) and cuts the speculative-request volume that creates the
   missing-markers. Composes with #105 and could let us drop the B1 gate. Static knob vs. the dynamic gate.
3. **#105 as shipped:** proven, low-risk, confined to `sync.rs`, no DoS-sensitive change.

**Recommendation:** ship #105; if going further, test buffer-shrink (#2) first (free on throughput,
attacks the cause); treat the registry-marker fix (#1) as a deferred, separately-reviewed follow-up
only if the residual `all_missing` micro-stalls ever become user-visible (currently they don't —
0 stalls, full throughput). Harness: `/root/wal-bench/{prbench_thin,run_hol7,analyze_hol}.sh`.

---

## Reference
- `route_inv` — `zebra-network/src/peer_set/set.rs:991`; synthetic `NotFoundRegistry` at `:1058`.
- Advertiser registration (PR C) — `zebra-network/src/peer/handshake.rs:~1230`.
- Inventory rotation governor — `INVENTORY_ROTATION_INTERVAL=53s` (`constants.rs:145`).
- Retry state machine — `zebrad/src/components/sync.rs` `handle_block_response_with_missing_retry`.
- Pre-fetch producer — `zebra-pr3` worktree: `EXTEND_PREFETCH_WATERMARK`, `discover_extend_hashes`.

## PR C fate (inventory-routing experiment)

**Status — two separate decisions, only one of which is made:**
- **DECIDED (deployment):** PR C is *excluded from the production PR* (#102) and parked on the local
  branch `experiment-inventory-routing`. The production thrashing fix (PR A2 + F1 + Q4 + typed accessor)
  is the measured, robust win and ships on its own.
- **NOT DECIDED (keep vs drop):** whether PR C is ultimately worth keeping is **unresolved**. The
  decisive fate-check was inconclusive (see below), so we have neither confirmed a benefit nor proven
  it inert. It stays parked, blocked on one cheap verification step before a keep/drop call can be made.

**What we know about PR C (register FindBlocks-reply blocks as advertised inventory):**
- **Registration works at least sometimes.** With phase-1 layered on top (prF2/prF3, 22-25 peers), the
  `busy` path engaged heavily (806/440) — which can only happen if `route_inv` saw `has_advertiser =
  true`, i.e. PR C had registered the FindBlocks responders as advertisers.
- **Standalone benefit is unconfirmed.** Without phase-1, PR C only adds "route to a *ready* advertiser
  in Step 1, else guess" — no funnel, but the prC matrix win (~8.4k, 0 restart-waits) is attributable to
  PR A2's pipeline-preserving retry, not PR C's routing. We never isolated a clean A2-vs-A2+PR-C delta.
- **The counter-instrumented fate-check (exp1) is a measurement bug, now CONFIRMED — not evidence
  about PR C.** A cross-check of the exp1 CSV settles it: the sync-side `registry_miss` climbed to
  **203**, while the network-side `route_inv.registry_miss` stayed **0** for the whole run. Every one of
  those 203 misses is synthesized *inside* `route_inv` at exactly the line that increments the network
  counter, so it must have fired ~203 times — reading 0 proves the new `zcash.net.*` counters are not
  exporting under the scraped names (a metric-name/registration bug in the diagnostics), **not** that PR
  C is inert. So exp1 tells us nothing about PR C either way; the earlier prF2/prF3 evidence (busy
  engagement) still says registration does fire.

**Tradeoff that keeps it out of production:** PR C registers block hashes from multi-item invs, which the
original code deliberately skips to avoid inventory-registry bloat ("a query reply… the whole network has
it"). It also touches a DoS-sensitive peer-set routing path. Without a confirmed throughput benefit, that
tradeoff isn't justified in the production change.

**To resolve PR C's fate (still OPEN), on `experiment-inventory-routing`:**
1. Fix the diagnostic counters — they are **confirmed broken** (export under a different name than
   `zcash_net_route_inv_*`/`zcash_net_inv_queried_block_registered`, or aren't registering). One 60s
   live `curl /metrics` reveals the real names; correct the harness scrape.
2. Then run A2 (production) vs A2+PR C on a **rich-peer** draw (variance matters — a thin draw stalls
   and is uninformative; use N≥3 and only trust good-peer draws), watching `route_inv.advertiser` vs
   `route_inv.registry_miss` and `inv.queried.block.registered`. PR C is worth keeping only if
   `advertiser` climbs and `registry_miss` drops materially. If `registered ≈ 0` on a good draw, PR C
   is genuinely inert (a registration-gap to fix or abandon).

**Bottom line: PR C's keep/drop fate is NOT decided.** It is excluded from PR #102 and parked; the one
experiment meant to decide it was invalidated by a counter bug, so neither benefit nor inertness is
established.

Phase-1 (`AdvertisersBusy`) is **not** revived for this: §7 showed its exclusive gate funnels concurrency
and regresses throughput ~20×. Any future routing work should use the phase-2 parking-queue design
(prefer the advertiser without blocking other requests), not the exclusive gate.

## PR 3 fate (eager hash prefetch) — DROP (confirmed no-op)

**Decision: DROP.** PR 3 (split `extend_tips` into `discover_extend_hashes` + a thin wrapper, and top
up the hash reserve whenever it falls below `EXTEND_PREFETCH_WATERMARK=500` instead of only when it
hits zero) has **no observable effect** in this configuration. Changes kept local & uncommitted on
`sync-prefetch-producer`, not proposed for merge.

**Throughput A/B (prefetch vs `fix-sync-restart-thrash` baseline, post-escape rate, healthy draws):**
prefetch median **20.7 blk/s** (17.0 / 20.7 / 22.5) vs baseline **20.8 blk/s** — no difference.

**Mechanistic trace (free, from the existing 5s CSVs — pref2 vs fixb2, same config, both healthy):**
- `sync.reserve.depth` = **0 at 100% of samples** for *both* — the lookahead (1500) ≫ a FindBlocks
  batch (~500), so each extend batch is fully dispatched into `in_flight` the same iteration and the
  reserve (overflow) never accumulates, with or without prefetch. Prefetch fires every iteration; it
  just has nothing to accumulate.
- `sync_downloads_in_flight` = **identical** (pref avg 1697, range 1499–1951; baseline avg 1712, range
  1499–1952). It oscillates between the lookahead floor and the overflow allowance and **never
  approaches 0** in either.

**Root cause:** `in_flight` is **bound by the lookahead cap (1500), not by hash availability**. Prefetch
makes more hashes available, but the buffer is already pinned at the cap, so nothing changes. The
sawtooth-to-0 PR 3 was designed to fix is a **cold-start / pre-A2** phenomenon; it does not occur in
healthy steady state with lookahead=1500 — the deep buffer already absorbs the FindBlocks round-trip.
(5s sampling is adequate here: a 1500-deep buffer draining at ~20 blk/s cannot reach 0 within 5s, so a
sub-sample dip to zero is physically impossible.)

**Corollary:** the heavier continuous-refill `select!` event-loop is **also not worth building** — the
bottleneck was never hash-feeding. Post-escape steady state is verify-bound (equihash), not
network/hash-bound.

## Full benchmark matrix (all runs, post-escape metrics; cold-start removed)

Post-rate = (final_height − escape_height) / (run_end − escape_time). Draw flagged STALLED on
`registry_miss ≥ 50` (peer-scarcity, network-bound). 7-min fork-runs from height 1,707,210.

| run | config | escape | Δblocks | post-rate (blk/s) | restart_waits | reg_miss | draw |
|---|---|---|---|---|---|---|---|
| base1 | baseline | 110s | 6185 | 20.0 | 11 | – | healthy |
| base2 | baseline | 20s | 8480 | 21.2 | 6 | – | healthy |
| base3 | baseline | 181s | 4998 | 20.7 | 17 | – | healthy |
| base4 | baseline | 25s | 6483 | 16.4 | 16 | – | healthy |
| base5 | baseline | 35s | 7894 | 20.3 | 9 | – | healthy |
| base6 | baseline | 40s | 8506 | 22.3 | 0 | – | healthy |
| base7 | baseline | 15s | 5942 | 14.6 | 19 | – | healthy |
| base8 | baseline | 35s | 8448 | 21.7 | 6 | – | healthy |
| prA2 | PR-A2 | 15s | 8649 | 21.3 | 0 | 1 | healthy |
| prA2b | PR-A2 | 40s | 8461 | 22.0 | 0 | 19 | healthy |
| prA2c | PR-A2 | 51s | 222 | 0.6 | 0 | 203 | STALLED |
| prC1 | A2+C | 35s | 8331 | 21.5 | 0 | 45 | healthy |
| prC2 | A2+C | 36s | 8568 | 22.1 | 0 | 1 | healthy |
| prC3 | A2+C | 35s | 8352 | 21.6 | 0 | 2 | healthy |
| prE1 | A2+C+rot20 | 40s | 8414 | 22.2 | 0 | 0 | healthy |
| prE2 | A2+C+rot20 | 30s | 8599 | 22.0 | 0 | 1 | healthy |
| prE3 | A2+C+rot20 | 35s | 8489 | 21.9 | 0 | 26 | healthy |
| prF1 | A2+C+F1+phase1 | 35s | 3191 | 8.0 | 0 | 162 | STALLED (registry) |
| prF2 | A2+C+F1+phase1 | 45s | 494 | 1.2 | 0 | 0 | STALLED (busy funnel, busy=806) |
| prF3 | A2+C+F1+phase1 | 45s | 494 | 1.2 | 0 | 0 | STALLED (busy funnel, busy=440) |
| prG1 | A2+F1+Q4 (final) | 35s | 8602 | 22.1 | 0 | – | healthy |
| prG2 | A2+F1+Q4 (final) | 35s | 8476 | 21.8 | 0 | 2 | healthy |
| exp1 | A2+C+PRc+counters | 35s | 222 | 0.4 | 0 | 203 | STALLED |
| pref1 | A2+F1+Q4+prefetch | 55s | 8271 | 22.5 | 0 | – | healthy |
| pref2 | A2+F1+Q4+prefetch | 35s | 8006 | 20.7 | 0 | – | healthy |
| pref3 | A2+F1+Q4+prefetch | 40s | 6510 | 17.0 | 0 | – | healthy |
| fixb1 | A2+F1+Q4 baseline | 101s | 2007 | 6.2 | 0 | 150 | STALLED |
| fixb2 | A2+F1+Q4 baseline | 35s | 8075 | 20.8 | 0 | – | healthy |

(`fixb3` died on startup, transient; `smoke` was an early thin-draw never-escape.)

**Reading the matrix:**
- **The validated win (A2 / A2+F1+Q4):** `restart_waits = 0` on every run vs baseline's 6-19 — the
  thrash-elimination is the robust, binary-attributable result. Post-rate ~21-22 blk/s, comparable to
  baseline's healthy draws but without the cold-start restart thrash.
- **STALLED draws are peer-scarcity, not binary-attributable:** they appear under four different binaries
  (PR-A2, A2+C, A2+C+PRc, prod-baseline) at `reg_miss` 150-203 / ~0.4-6.2 blk/s, while the *same*
  binaries run clean on good draws. The stall is "no connected peer serves the head block" — network-
  bound, not fixable by sync-logic changes (PR B eviction is contraindicated: can't evict from a thin
  peer set).
- **phase-1 (prF2/prF3)** is the one binary-attributable regression: the `AdvertisersBusy` exclusive
  gate funnels concurrency (busy=806/440) → ~1.2 blk/s even on non-thin draws. Reverted.
- **prefetch (pref*)** ≈ baseline (fixb2) — no effect, per the no-op analysis above.

---

## §8 — The 20 blk/s ceiling is note-commitment-tree updates on the serial writer (2026-06-17)

**Question:** on `fix-sync-head-of-line-priority`, healthy steady state sits at ~20 blk/s. What is
the constraint, and why can't it go higher?

**Method:** fresh resource-sampled run (`prbench_res.sh`) from the 1.7M snapshot confirms it is a
**single serial thread**, not any hardware resource. Then an instrumented build (`zebrad-hol-instr`,
7 new phase histograms, see `/root/wal-bench/writer-phase-instrumentation.patch`) split the per-block
serial commit cost. Scrape: `/root/wal-bench/phase_scrape.sh`.

### Macro: not resource-bound (res_holinstr.csv / res_holres1.csv, steady state)
| Resource | Measured | Verdict |
|---|---|---|
| CPU | **1.1–1.7 / 8 cores** | not aggregate-CPU-bound (7 cores idle) |
| Block-I/O wait | **0.00 s** | not disk-bound |
| Physical disk reads | **0.0 MB/s** (page-cache served) | not read-bound |
| Disk writes | 25 MB/s | trivial |
| Net RX / TX | 9.2 / 4.1 MB/s | not bandwidth-bound |
| `sync_downloads_in_flight` | ~1600–2000 (buffer full) | downloads far ahead; writer is the metronome |

### Micro: per-block serial-writer breakdown (N=5297 blocks, instrumented run = 17.2 blk/s; sum=56.2 ms/block reconciles exactly)
| Phase (serial finalized-writer thread) | ms/block | % serial |
|---|---|---|
| **`update_trees_parallel`** (Sapling/Orchard note-commitment Merkle trees) | **40.9** | **72.7%** |
| `block_commitment_is_valid_for_chain_history` (ZIP-244 chain-history check) | 10.8 | 19.1% |
| `write_block` total (ALL RocksDB work) | 4.5 | 8.0% |
| · db.write (rocksdb commit — the only previously-timed part) | 2.5 | — |
| · prepare_block_batch | 1.0 | — |
| · address-balance reads | 0.45 | — |
| · per-input UTXO/output_location reads | 0.40 | — |
| `history_tree.push` (sapling/orchard root) | 0.1 | 0.2% |

**~92% of serial commit time is CPU crypto** (tree update + commitment check). All RocksDB I/O —
including the per-input UTXO reads the RUNBOOK had fingered — is **<4.5 ms (8%)**. This **overturns
the prior working hypothesis** (serial state-writer DB / UTXO reads).

### Root cause (architectural)
`commit_finalized_direct` Checkpoint arm (`finalized_state.rs:366`): *"Checkpoint-verified blocks
don't have an associated treestate"* — so `update_trees_parallel` + the commitment check run **inline
on the single finalized-writer thread**, with zero overlap (block N+1 cannot start until N's full
~56 ms completes). In the semantic/non-finalized path the same `update_trees_parallel` runs during
contextual validation (`chain.rs:1482`), off the commit critical step. The checkpoint verifier (1500
concurrency) validates blocks in parallel but **skips treestate**, dumping the most expensive op onto
one thread. `update_trees_parallel` already parallelizes *across* the 3 trees (rayon, 4 tasks), so the
~41 ms is after cross-tree parallelism → **whichever pool the spam is in at that height dominates** and
is sequential. (Correction: the dominant pool **varies by range**, not "always Sapling" — see §13.)

### Levers to break past ~20 blk/s (not yet built)
1. **Parallelize *within* a tree update**: leaf commitment hashing (Pedersen/Sinsemilla) for all of a
   block's outputs across the 7 idle cores before the sequential frontier merge — the likely big win.
2. **Pipeline the writer** (tree-update stage ahead of db-commit stage): overlaps only the ~2.5 ms
   commit — small.
3. **Compute treestate ahead of the writer in a dedicated sequential pre-stage** fed by the parallel
   checkpoint verifier — hides nothing on its own (it IS the bottleneck) unless combined with (1).

Artifacts: instrumented binary `/root/wal-bench/zebrad-hol-instr`; phase scrapes
`/root/wal-bench/phase_holinstr_final.txt`; patch `writer-phase-instrumentation.patch`.

---

## §9 — Part 1 implemented: overlap commitment-check with tree update (2026-06-17)

Worktree `/root/zebra-hol-pr`, branch `sync-checkpoint-commit-parallel` (off `fix-sync-head-of-line-priority`).
In `commit_finalized_direct`'s Checkpoint arm, `update_trees_parallel` and
`block_commitment_is_valid_for_chain_history` now run concurrently via `rayon::in_place_scope_fifo`
(tree update on the in-place thread, commitment check spawned), joining before `history_tree.push`.
The commitment check reads only the parent history tree, so it is independent (confirmed in `check.rs`).

**Measured (zebrad-part1, 5,604 blocks at steady state, within-run so peer-independent):**
- `checkpoint_compute` WALL = **30.5 ms/block** ≈ `update_trees` component alone (30.4 ms) → the
  commitment check (8.4 ms) is **fully hidden** by the overlap.
- Sequential sum would be 30.4 + 8.4 + 0.1 = 38.9 ms → actual 30.5 ms = **~8.4 ms/block saved (~21% of
  the compute phase)**.
- Throughput 26.4 blk/s; CPU still ~1.97/8 cores; db.write now only ~1.7 ms/block.

**Implication for the plan:** db.write is tiny (~1.7 ms), so Part 2 (pipeline write off the writer)
now buys at most ~write_block (~4.5 ms) of overlap — modest. The remaining serial wall is
`update_trees` (~30 ms = ~31 blk/s ceiling), so **Part 3 (parallel batch Sapling append) is the only
real lever past ~30 blk/s.** (Note: the ~30 ms here vs ~41 ms in the §8 baseline run is cross-run
variance — different machine/cache state; the §8 vs §9 numbers are not directly comparable, which is
why the Part 1 proof uses the within-run sequential-sum-vs-wall comparison instead.)

---

## §10 — Part 3 premise CONFIRMED: Sapling append is parallelizable (2026-06-17)

Micro-benchmark (`zebra-chain parallel::tree::part3_premise_bench`, release, ~1.7M-leaf tree):
| N (leaves/block) | append loop | per leaf | root() | append % |
|---|---|---|---|---|
| 256 | 18.3 ms | 71.5 µs | 2.5 ms | 88% |
| 512 | 36.6 ms | 71.5 µs | 2.5 ms | 93% |
| 1024 | 73.3 ms | 71.6 µs | 2.5 ms | 97% |

- Per-leaf append cost (this micro-bench, Sapling) is a **flat ~71.5 µs** = one Sapling Pedersen
  `combine`. `root()` is a fixed **~2.5 ms** sequential floor (one combine per spine level).
  (Per §13, in-node per-leaf costs measured ~74 µs Sapling / ~190 µs Orchard-Sinsemilla.)
- NOTE (corrected in §13): the leaf *count* per block was later measured directly — it is **not** a
  fixed ~385, and the dominant pool **varies by range** (Orchard ~87/block at 1.709M; Sapling ~255/block,
  peaks ~1.6k, at 1.724M). Do not treat the timing-derived "~385 sapling" estimate as authoritative.
- **Append dominates (88–97%) and is the parallelizable part.** Parallelizing the per-leaf combines
  across 7 cores: ~27.5 ms → ~4 ms, + 2.5 ms root ≈ **~6.5 ms/block** (tree-update side), ~4–5×.

**Design (Part 3, in progress):** parallel batch frontier append — decompose the block's new leaves into
aligned perfect subtrees, compute their roots via rayon parallel reduction (independent `H::combine` per
level), then fold into the frontier's ommers (sequential, O(log N), ~2.5 ms). Consensus-critical:
must reconstruct `NonEmptyFrontier (position, leaf, ommers)` byte-identically. Safety net: differential
proptests vs the serial `append` asserting identical `into_parts()`, `root()`, and
`completed_subtree_index_and_root` events over random tree sizes × batch sizes, before any production wiring.

---

## §11 — Part 3 implemented: parallel batch note-commitment-tree append (2026-06-17)

`zebra-chain/src/parallel/batch_frontier.rs`: generic `parallel_append<H, DEPTH>` for any
`incrementalmerkletree::Frontier` (so Sapling, Orchard, Sprout share one implementation). Algorithm:
rebuild the pure binary-counter forest from the frontier's ommers, inject the old tip leaf, then append
the new leaves (except the last, kept raw) as globally **position-aligned dyadic blocks** — each block's
root computed by a `rayon::join` parallel reduction — injected in ascending order (aligned blocks compose
with no cross-boundary re-pairing, which is what makes the parallel result exact).

Wired in via `NoteCommitmentTree::append_batch` on Sapling and Orchard, called from
`update_{sapling,orchard}_note_commitment_tree`. Subtree (2^16) completion tracking preserved by
splitting the batch at the at-most-one subtree boundary per block.

**Correctness (consensus-critical):**
- Differential proptests vs sequential `Frontier::append`: 2000 random (prefix × batch) cases + exhaustive
  40×40 sweep — identical root AND identical frontier parts. Test node `combine` is order- and
  level-sensitive to catch swaps/level bugs. (First implementation, a half-split divide-and-conquer, was
  caught wrong by the proptest at `prefix=0,batch=7` — ragged-boundary re-pairing — and replaced.)
- Full `zebra-chain --lib` suite: 259 passed, 1 failed = only the pre-existing date-dependent NU7 test
  (fails identically on clean base). Known-answer note-commitment-tree root vectors + subtree tests pass.

Next: build + benchmark (expect tree-update phase ~30 ms → single digits, CPU > 2 cores).

## §12 — Part 3 benchmark results (2026-06-17)

Bench from the 1.7M snapshot, peer-independent phase times (instrumented) + throughput/CPU.

| Metric | Baseline (§8) | Part 1 (§9) | Part 3 seq-blocks | Part 3b par-blocks |
|---|---|---|---|---|
| `update_trees` ms/blk (peer-independent) | ~30 | ~30 | 18.4 | **16.5** |
| `checkpoint_compute` WALL ms/blk | ~52 (sum) | 30.5 | 18.6 | **16.9** |
| throughput blk/s | 17–22 | 26 | 32 | **42** |
| mean CPU /8 | 1.1–1.7 | 2.0 | 2.7 | **3.3 (peak 4.4)** |

Part 3 = parallel batch note-commitment append (Sapling+Orchard). Part 3b adds `par_iter` across the
dyadic blocks (compute all block roots concurrently, each reduction internally parallel too).

**Robust claim:** `update_trees` (peer-independent) ~30→16.5 ms (~1.8×); CPU and throughput ~doubled.
Not the theoretical ~5×: each block's ~27 ms of Pedersen work is a brief burst contending with the
verification pipeline on the shared rayon pool, plus the ~2.5 ms sequential `root()` per tree and the
sequential dyadic-block injection. Diminishing returns past here.

**Overall stack (Part 1 + Part 3b):** checkpoint-zone steady state went from a single-core ~17–22 blk/s
to ~42 blk/s using ~3.3/8 cores, with byte-identical tree roots (proptests + known-answer vectors).
Part 2 (pipeline) remains parked; with db.write at ~1.9 ms it's still low-value.

---

## §13 — CORRECTION: per-block output composition varies by range (2026-06-17)

Earlier sections assumed "Sapling dominates." Direct measurement (commitment-tree size delta via
`z_gettreestate`, self-validated against the orchard nullifier counter + `getblock` shielded arrays —
three independent methods) shows the **spam pool flips by height range**:

| range | sapling outputs/block | orchard outputs/block | note |
|---|---|---|---|
| 1,709,000–1,710,999 | 0.7 | **86.7** | Orchard sandblasting |
| 1,724,000–1,725,000 | **254.6** | 0 | Sapling sandblasting (peaks ~1,649/block) |

- **Method:** parse the serialized `finalState` commitment tree (`left`/`right`/`parents` ⇒ leaf count)
  at the two heights; the delta is the exact outputs added. Orchard delta matched the independent
  nullifier counter and `getblock` exactly, validating the parser, so the Sapling delta is trustworthy.
- **Per-leaf cost by pool (in-node):** Sapling/Pedersen ~74 µs/leaf; Orchard/Sinsemilla ~190 µs/leaf
  (~2.5× heavier). So a Sapling-spam block (~255 leaves) and an Orchard-spam block (~87 leaves) land at
  similar `update_trees` cost (~17–19 ms) via different mixes.
- **Implication:** the parallel batch append (Part 3) is generic over the pool, so it covers both. But any
  per-leaf/per-block cost model must use the actual pool mix of the range under test, and the leaf count
  is highly variable (0 → ~1,650/block) and bursty. The timing-derived "~385 sapling/block" in §10 is
  superseded by these direct counts.

---

## §14 — Parallelism shortfall DIAGNOSED: global rayon contention (2026-06-17)

Isolated release-mode probe of `parallel_append` against **real Sapling and Orchard hashing** (batch
128–2048 × `RAYON_NUM_THREADS=1,2,4,8`; probe removed afterward, tree clean):
- **8 threads → ~6.7–7.4 effective cores** for 1024–2048 leaves, **both pools**. The reduction scales.
- 1-thread parallel ≈ sequential → no task-overhead regression.
- Local pool ≈ global pool *in isolation* (no other load).

In-node, the same code runs at only ~1.6 effective cores (heavy-Sapling `update_trees` ~137 ms for
~1,850 leaves ≈ sequential). ⇒ The bottleneck is **global rayon pool contention/scheduling
interference** — `update_trees_parallel` nests Sapling+Orchard tasks plus `parallel_append`'s internal
rayon work, all on the **global** pool, contending with the download/verify/checkpoint pipeline.

**Decision: prioritize a dedicated tree-update rayon pool (pool isolation), NOT `parallel_append`
algorithm tuning.** Final confirmation owed: full-node dedicated-pool A/B (isolation proves the ceiling;
A/B proves it's realized in-node). See `PARALLEL_IDEA.md` next-step #1.
