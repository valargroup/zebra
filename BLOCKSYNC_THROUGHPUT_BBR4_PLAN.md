# Block-sync throughput — committer-bbr-4 findings & plan

**Trace:** `art/debug/blocksync_refactor/manual/committer-bbr-4-20260629T235455Z` (us-0,
24 min, 1,781,982 → 1,856,727, **51.7 blk/s**). First live trace of the byte-cwnd +
WS-A + glue_refactor Committer stack.

## TL;DR — the commit pipeline is **not** the bottleneck; it is starved

The instinct was "batch the checkpoint commits like the 1000-range header path." The
trace says that would do nothing:

- The CheckpointVerifier is **fast**: it verifies ~45-block ranges in ~64 ms (86 % of
  ranges), i.e. **~700 blk/s of capacity**, yet it is fed at 51.
- It is **idle 88 % of the time**, with feed-gaps up to **35.9 s** waiting for the next
  contiguous range.
- The Committer (`zebrad/.../committer.rs`) already fires the whole contiguous range
  concurrently into a `FuturesUnordered` — required, not optional, because the verifier
  resolves a block's commit only once its whole range is submitted. Per-block
  `commit_finish elapsed` (p50 1.5 s, p99 28 s) is **range-fill residence latency**, a
  symptom of the slow feed, not a verify/commit cost. `commit_stalled` is diagnostic-only
  (`committer.rs:118` — "never gates a commit").

So the throughput ceiling is the **contiguous-floor advance rate = total download
bandwidth ≈ 51 blk/s, dominated by a single carrier doing 47 blk/s.** Everything
downstream (verifier, durable writer at 411 blk/s) has headroom.

## What the trace shows

| Signal | Value | Read |
| --- | --- | --- |
| Carrier `5ebbf236` share | **89.2 %** of bodies, 41.4 MB/s sustained | one carrier *is* the pipe |
| Carrier idle (gaps >0.5 s) | **237 s = 16 % of its span** | we do not keep it maxed |
| — cwnd-full | 108 s (46 % of idle) | window too shallow for the carrier's tail latency |
| — bubble (slots+work+budget free) | 73 s (31 % of idle) | **pure request-path waste** |
| — commit-backpressure | 55 s (23 % of idle) | reorder/applying near the lookahead cap |
| byte-cwnd | pinned at the **4 MB floor** (p50 = p90) | BDP collapses → cwnd never grows |
| reorder buffer | p50 **1606**, max 4053 (≈3.6 GB held) | every peer delivers ~1600 *above* the floor |
| re-requests | **8.0 %** (was 24.5 %) | WS-A win |
| sequencer panics | **4×** at `sequencer_task.rs:308` | both-inputs-close race — **FIXED** |
| reaper closes | 53 % of churn (176, p50 life 37 s) | slow peers still go silent >32 s |

The carrier and every other peer deliver blocks **~1600 heights above the current floor**
(p50). Nobody serves the floor tightly — the whole fleet races ahead filling the reorder
buffer while the lowest missing block lags. The floor still advances at the aggregate
download rate (the 1600 offset is latency + memory, not a throughput cap), but the offset
is large enough to (a) hold 3.6 GB and (b) brush the lookahead cap, which is what produces
the 55 s of commit-backpressure idle.

## Levers, ranked

### 1. Recover the carrier's 16 % idle (biggest controllable win: 47 → ~55 blk/s)

**1a. Fill bubble (73 s, pure waste).** When the apply queue has room (72 % of the run),
the carrier still idles with free slots + budget + queued work 22 % of the time. This is a
fill-loop wakeup gap — `try_fill` ends a pass below the peer's cap and nothing re-arms it
until the next event. Fix: re-arm the fill loop on the same event that frees a slot (a body
completing / budget releasing), event-driven, no poll. **Instrument first:** emit a
fill-stop-reason on every `try_fill` break (`no_status` / `cwnd_saturated` / `no_work` /
`budget` / `lookahead_cap`) so the bubble is a measured %, not an inference.

**1b. byte-cwnd floor-pin (108 s).** `bbr_cwnd_bytes` sits at the 4 MB floor for ≥90 % of
deliveries because BDP ≈ 0: the size-residual RTprop (`rtprop = max(0, elapsed −
bytes/btlbw)`) attributes ~all of `elapsed` to transfer, so RTprop → 0 → BDP =
BtlBw·RTprop → 0 → `cwnd = max(min_cwnd, BDP·gain) = min_cwnd`. A 4 MB (~6-block) window is
too shallow to stay busy through the carrier's own tail-latency spikes (`request_elapsed`
p99 5.7 s): one slow body drains the window and the carrier idles even though more blocks
are requestable. Options (A/B both): (i) make RTprop meaningful (floor it at the measured
min-elapsed instead of letting the residual zero it), so BDP·gain reflects real
in-flight-ness; (ii) raise `bbr_min_cwnd_bytes` so the floor itself absorbs a few seconds
of tail. The carrier sustains 41 MB/s across ~6 concurrent, so it serves in parallel — a
deeper window should convert most of the 108 s into throughput. **Add `bbr_rtprop_ms` /
`bbr_btlbw_bytes_per_sec` back onto `block_body_received`** to confirm the RTprop≈0
diagnosis directly.

### 2. Carrier diversity (the structural ceiling)

One carrier at 89 % means its 47 blk/s ≈ the entire commit rate. Beyond lever 1, more
throughput needs a *second* fast carrier. Largely peer-quality (not fully in our control),
but we can ensure the machinery uses a 2nd fast peer when one exists: verify WS-A's
floor-rescue + above-floor speculation actually spread load (here `floor_bypass` = 165,
tiny) and that the reaper isn't culling a would-be 2nd carrier (53 % of closes are the 32 s
reaper).

### 3. Floor tightness / memory (latency + smoothness, not throughput)

reorder p50 1606 / 3.6 GB held / bursty commits (`committed_blocks_per_sec` p50 0, max
14397) all stem from the fleet racing ~1600 ahead of the floor. Concentrating requests
nearer the floor (tighter lookahead, or floor-first dispatch) shrinks RAM, smooths commits,
and lifts the 55 s of backpressure idle. **Instrument:** re-enable the `FLOOR_GAP_*` fields
(oldest-outstanding floor-request age + which peer holds it) — they are defined in
`trace.rs` but not emitted in this binary, and they are exactly what's needed to see *why*
the floor lags.

### 4. Sequencer panic — **DONE**

`sequencer_task.rs:308` panicked 4× (`all branches are disabled and there is no else
branch`): the run-loop's top-of-loop "both inputs closed → break" guard is checked before
`process_one_ready`, which can itself close both inputs in one drain pass and return false,
dropping into a `select!` whose two guarded arms are both disabled. Fixed with `else =>
break,`. Process survived each panic (single session, monotonic vtip), but it was an
unclean task death and a latent abort risk.

## What NOT to do

- **Don't** add commit concurrency, raise the in-flight commit cap, or batch the apply
  submissions — the verifier is already ~700 blk/s and starved; the cap is diagnostic-only.
- **Don't** chase the reaper churn for throughput — the carrier never churns; it's noise.

## Instrumentation to land alongside (cheap, event-driven)

1. `try_fill` fill-stop-reason counter (lever 1a).
2. `bbr_rtprop_ms` + `bbr_btlbw_bytes_per_sec` on `block_body_received` (lever 1b).
3. Re-emit `FLOOR_GAP_OLDEST_OUTSTANDING_MS` + `FLOOR_GAP_OUTSTANDING_PEERS` (lever 3).

## Next live A/B

Re-deploy with the panic fix + fill-stop instrumentation, capture `slow-peer-floor-2`.
Targets: carrier idle 16 % → <5 %, bubble → ~0, reorder p50 down, throughput 51 → 60+,
zero sequencer panics. Scripts: `analyze.py` / `commit.py` / `gaps.py` in the run dir.
