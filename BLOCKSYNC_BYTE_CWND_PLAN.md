# Block-sync throughput: byte-denominated BBR from header size hints

> Branch: `evan/perf-plus-download-fixes`. Motivated by the `bbr-committer-0` us-0 live
> trace (2026-06-29). Companion to `blocksync-bbr-fuzzer-impl` and the
> `bbr-committer-0-result` analysis.

## Goal & constraints (from the user)

- **Higher throughput while keeping HoL blocking fixed**, and a **stable apply queue
  (equilibrium), not the current sawtooth.**
- **Drive congestion control from the real per-block size hints carried in the header**
  (`BlockSizeEstimate::Advertised`). **Never assume `max_response_bytes` (32 MB) or
  `MAX_BLOCK_BYTES` (2 MB) worst-case.**
- **Keep one block per request** for now (`DEFAULT_BS_BLOCKS_PER_RESPONSE = 1` stays).
  No multi-block ranges in this change.

## Evidence (bbr-committer-0, 54 min, 1.707M snapshot, 35.9 blk/s)

- `bbr_cwnd` is pinned at `min_cwnd = 4` for **99.6%** of 119k deliveries; only 0.4% ever
  exceed 8. Per-peer `outstanding@send` = 4 everywhere (shallow — HoL is gone), but peers
  are under-pipelined. The fast peer carries **75%** of bodies at cwnd 4; 7 slow peers do
  1–3 blk/s with **4–6 s refeed bubbles** (idle bandwidth).
- `range_count == 1` for **100%** of requests (single-block, as intended).
- Apply queue sawtooths: `applying ≡ floor−vtip` p50 1226 / max 4209; `sequencer_input`
  saturated (==401) 18% of snapshots; `commit_finish` per-block latency p50 1.2 s with
  **27–53 s tails** (`commit_stalled` watchdog fires at 30 s, 495×). Those tails stall the
  contiguous verified-tip advance → the sawtooth.

## Root cause (code-anchored)

The controller counts **blocks**, and a blocks-denominated BDP is physically meaningless
for heterogeneous block sizes over a single-block-per-request pipe:

1. **BtlBw is blocks/sec.** `BbrState.btlbw_blocks_per_sec` (`state.rs:471`);
   `record_delivery` (`state.rs:532`) observes `delivered_delta / interval` in *blocks*.
2. **RTprop is `min(request_elapsed)`.** For a peer that ever serves a tiny block, that
   min ≈ 1 ms. So `bdp_blocks = BtlBw_blocks × RTprop` (`state.rs:662`) ≈ `27 × 0.001 ≈ 0`
   → `cwnd_target = max(min_cwnd, round(bdp×gain))` (`state.rs:670`) → **`min_cwnd = 4`
   binds essentially always.**
3. **The delay-gradient compounds it.** `update_delay_cap` (`state.rs:569`) ratchets the
   ceiling ×0.9 whenever `smoothed_elapsed > rtprop × delay_gradient(1.5)`. A 445 KB block's
   *honest* transfer time (~60 ms) trips that against a ~1 ms RTprop, so the ceiling is
   driven to `min_cwnd` on legitimate big-block deliveries — queueing is *inferred* from
   what is really size-dependent transfer time.
4. **The byte path already hard-codes the worst case.** `CwndUnit::Bytes` exists but is
   inert/broken: `nominal_request_bytes = config.max_response_bytes` = **32 MB**
   (`state.rs:750`), and the Bytes arm computes `cwnd_bytes = cwnd_slots × 32 MB`
   (`state.rs:851`). That is the "assuming 32 MB" the user called out, and it is why
   `Blocks` is the default.

The real hints are already present and unused by the window: header sync supplies
`BlockSizeEstimate::Advertised(u32)` (`request.rs:18`); `estimate_bytes_with`
(`work_queue.rs:71`) turns it into a clamped `estimated_bytes` per work item; and
`outstanding_reserved_bytes()` (`state.rs:861`) already sums the *real* reserved bytes of
in-flight requests. **Only the cwnd target ignores them.**

## Design: byte-denominated BBR, one block per request

Make the controller reason in **bytes**, sourced from the header size hints, so the window
is a true bandwidth-delay product and the in-flight *request count* falls out as
`cwnd_bytes / advertised_block_size` — deep where blocks are small, shallow where they are
large, all with single-block requests.

### 1. Byte-denominated estimators (`state.rs BbrState`)

- **BtlBw in bytes/sec.** Rename/retype `btlbw_blocks_per_sec → btlbw_bytes_per_sec`.
  Thread `delivered_bytes` (the request's `serialized_bytes`, already on
  `block_body_received`) into `record_delivery(now, elapsed, delivered_bytes, inflight,
  snapshot)`. Sample `rate_bytes = delivered_bytes_delta / interval`. The max-filter then
  tracks real link throughput.
- **RTprop = the fixed-latency component, not min(elapsed).** `elapsed ≈ RTprop +
  bytes / BtlBw`. Estimate `RTprop` from the *size-residualized* round trip:
  `rtprop_sample = max(0, elapsed − delivered_bytes / btlbw_bytes_per_sec)`, windowed-min.
  (v1 acceptable fallback: keep `min(elapsed)` but only from deliveries whose advertised
  size is below a small percentile, so the min reflects near-zero transfer; the residual
  form is preferred and not much more code.)
- **byte-BDP.** `bdp_bytes = btlbw_bytes_per_sec × rtprop_secs`;
  `cwnd_bytes_target = max(min_cwnd_bytes, round(bdp_bytes × cwnd_gain))`.
- **Size-aware delay-gradient.** Compare the round trip against its *expected* size-aware
  value: queue is building only when `smoothed_elapsed > (rtprop + bytes/BtlBw) ×
  delay_gradient`. This stops big blocks from looking like congestion. Ratchet `delay_cap`
  in **bytes**.
- `min_cwnd_bytes` replaces the blocks `min_cwnd` (a small floor, e.g. a few × the typical
  advertised body so a brand-new peer can fetch ≥1–2 blocks before its first sample).

### 2. Window/admission uses real hints (`state.rs available_slots*`, `peer_routine.rs try_fill`)

- Make `CwndUnit::Bytes` the **active** unit (config default flips to `Bytes`). Keep the
  `Blocks` arm compiling for A/B and tests.
- Delete `nominal_request_bytes = max_response_bytes`. The Bytes arm becomes:
  `available_bytes = cwnd_bytes_target.saturating_sub(outstanding_reserved_bytes())`,
  clamped by the live memory `budget.available()` and the peer's advertised
  `max_response_bytes` (the latter only as a transport ceiling, never as the per-block
  size).
- **Admit the next single-block request iff** `outstanding_reserved_bytes +
  next_advertised_bytes ≤ cwnd_bytes_target`, where `next_advertised_bytes =
  estimate_bytes_with(work.peek_floor_hint())` — the *actual* header hint of the specific
  next height, `Unknown → DEFAULT_BS_SIZE_FLOOR_BYTES`-biased rolling average for the
  peer's region (**not** `MAX_BLOCK_BYTES`). `take_in_range_budgeted` already takes ≥1 and
  bounds the take by real `estimated_bytes`, so no change there.
- Floor bypass stays, expressed as a byte bonus (one expected-body's worth) so the floor
  height is never starved by a tight byte window.

### 3. Tracing

`block_body_received`: add `bbr_cwnd_bytes`, `bbr_btlbw_bytes_per_sec`, `bbr_rtprop_ms`,
`bbr_inflight_bytes`; keep `bbr_cwnd` as the derived in-flight *request* count
(`cwnd_bytes / avg_advertised`) so the existing analysis scripts keep working.
`SlotDiagnostics.effective_window` becomes the aggregate byte window.

## Why HoL stays fixed (single block per request)

`cwnd_bytes = BtlBw × RTprop` is the **zero-standing-queue** operating point: it is exactly
the bytes "on the wire" during one RTprop, so there is no backlog for the floor block to
wait behind. When blocks are big the byte window admits only 1–2 in flight (shallow, low
floor latency); when blocks are small it admits many (cheap, each quick) — the *request*
depth self-adjusts to keep floor latency ≈ one block's transfer, which is the floor-HoL
metric we already track. The delay-gradient (now size-aware) still ratchets the window down
if a real standing queue forms, so an over-estimate self-corrects.

## Event-driven discipline (no control loops with waits)

Every mechanism here is driven by an **event** — a body received, a request
completed/timed-out, a header-tip/frontier change, a durable-write notification — never by a
periodic wakeup, poll, tick, or watchdog. Control loops with waits are tech debt unless
truly unavoidable; the one sanctioned timer is a per-request network deadline, which is
itself an event. Audit of each timing-sensitive piece:

- **cwnd recompute** — happens inside `record_delivery`, i.e. on each body-received event.
  There is no periodic "recompute the window" tick, and the byte rewrite must not add one
  (sample rate, observe RTprop, set `cwnd_bytes_target` all in the delivery handler). ✓
- **Admission / `try_fill`** — runs on routine-loop turns woken by request completions /
  `Notify`, not on an interval. The byte-admission check is evaluated when an event frees a
  slot. ✓
- **ProbeRTT cadence** — `probe_rtt_interval` is a *deadline value compared at delivery
  events* (`now − last_probe_rtt_at ≥ interval`, inside `advance_phase`), **not** a
  `tokio::time::interval` that wakes the task. It self-sustains because `min_cwnd_bytes`
  keeps ≥1 request in flight, so completions keep arriving to drive the phase machine — a
  drained peer cannot wedge in ProbeRtt. Note: byte RTprop is size-*residualized* (no longer
  `min(elapsed)`), which **reduces the need for ProbeRTT** at all; if the fuzzer shows no
  measurable benefit, delete it rather than keep a phase whose only job is to refresh a
  measurement.
- **Request timeout (the one sanctioned timer)** — a per-request network deadline is an
  event; the `record_timeout()` β-dip fires on it. Keep. The no-progress liveness backstop
  stays only as a true last resort, expressed as a per-request deadline, not a periodic scan.
- **Memory-ceiling backpressure** — already event-driven: a full byte-budget reservation
  makes `try_reserve` fail at admission time (an event the routine sees on its next
  completion-woken turn). No depth-polling loop.

## Equilibrium / sawtooth (needed for the "stable apply queue" ask)

**Do NOT throttle download to the commit rate.** No control loop couples `cwnd_bytes` to
apply-queue depth — that is a backstop that masks the real problem and conflicts with the
event-driven, no-backstops design stance. The committer always runs flat-out; the
downloader always runs as fast as byte-cwnd allows; **the apply queue's only bound is the
memory byte budget** (the existing reservation ceiling). Whatever depth falls out of those
two rates running freely is the depth — we shape it by making both sides fast and steady,
not by capping the fast one.

The trace is unambiguous that the sawtooth is **commit-side, not a download/backpressure
oscillation**: `download_blocked_on_budget` = **0%** and `sequencer_input == 401` only
**18%** of snapshots, yet `cwnd` was pinned at 4 (36 blk/s). Download never out-ran the
queue — commit drained the 3.24 GB in-flight backlog in **bursts** (`commit_finish` p50
1.2 s with **27–53 s tails**; the 30 s `commit_stalled` watchdog fired **495×**), and each
30 s+ stall of the contiguous verified-tip advance is one tooth of the sawtooth. An AQM that
slowed download would point the wrong tool at the wrong side — the side that's already too
slow.

So equilibrium = make both sides fast and steady, queue ceilinged only by RAM:

4. **Steady download (the core of this plan).** Byte-cwnd raises the floor of the download
   rate from 36 blk/s and removes the cwnd-collapse oscillation, so the queue is fed
   smoothly instead of in the under-pipelined trickle that the bursty drain currently sits
   on top of.
5. **Smooth the commit drain — and do it event-driven.** The 27–53 s `commit_finish` tails /
   495× `commit_stalled` (30 s) events are the `blocksync-apply-window-5s-refresh-cap`
   pathology: the contiguous verified-tip advance is gated on the **5 s
   `CHECKPOINT_FRONTIER_REFRESH_INTERVAL` poll**, and the 30 s `commit_stalled` **watchdog**
   is the backstop that merely *surfaces* the stall. Both are the control-loop-with-wait tech
   debt this stance condemns. The fix is the `glue_refactor` scoped direction: **release
   `vtip` on a durable-write watch / contiguous-committed-prefix notification**, advancing the
   verified tip the instant the next contiguous block is durable (per-completion event), so
   no periodic frontier poll survives. Demote `commit_stalled` to a pure diagnostic emit that
   attributes `commit_stall_reason` from the gating event — not a control input. **If, after
   this, commit genuinely cannot keep up with a healthy download rate, that is the root
   problem to attack next — by making commit faster, never by throttling download down to
   it.**
6. **Queue bounded by memory only.** The apply queue grows until the byte-budget reservation
   ceiling, which already backpressures download naturally (no new control law). Tune that
   ceiling for the resident-memory target; do not add a depth-proportional cwnd throttle.

## Validation

- **Fuzzer first** (`testkit/blocksync_fuzz/`). Two gaps to close so the harness can prove
  this: (a) synthetic peers must serve with **byte-accurate** latency
  (`elapsed = base_rtt + bytes / peer_bandwidth`) and advertise per-block size hints into
  the corpus; (b) add a **big-block** profile and a **commit-stall** profile. Scenarios:
  - `mixed_block_sizes`: alternating small/large regions — assert request depth tracks
    `cwnd_bytes / size` (deep small, shallow large) and floor latency ≈ one block transfer.
  - `high_bw_fast_peer`: a fast peer with real headroom — assert `cwnd_bytes` grows to the
    byte-BDP and per-peer throughput rises vs the blocks-unit baseline (A/B on one seed).
  - `one_slow_peer_hol`: floor latency p99 stays bounded (no regression).
  - `commit_stall`: with a slow/bursty commit profile, download stays fast and the apply
    queue grows only to the memory ceiling (no download throttle); assert `vtip` resumes
    promptly once the stall clears and the queue drains — i.e. the tooth is a commit burst,
    not a download oscillation.
- **Unit tests**: BtlBw-bytes math; RTprop residual; size-aware delay gate (big block does
  *not* ratchet); Bytes admission with `Advertised` vs `Unknown` hints (never 2 MB/32 MB).
- **Live A/B re-run** on us-0 (same snapshot/scripts as `bbr-committer-0`). Targets:
  throughput up from 36 blk/s **without** re-introducing deep per-peer queues; floor-HoL
  p99 unchanged; re-request % ≤ current 33%; apply-queue sawtooth amplitude down; peer
  lifetimes / reaper counts unchanged. Capture as `byte-cwnd-1`.

## Config knobs (`config.rs`)

- `bbr_cwnd_unit` default → `Bytes`.
- New: `bbr_min_cwnd_bytes`. Reuse `bbr_cwnd_gain_percent`, `bbr_delay_gradient_percent`,
  RTprop/delivery windows. **No AQM knobs** — the apply queue is bounded by the existing
  memory byte-budget reservation ceiling, not by a cwnd throttle.
- Retire `nominal_request_bytes`. `max_response_bytes` stays only as the transport response
  ceiling. `DEFAULT_BS_BLOCKS_PER_RESPONSE = 1` unchanged.

## Risks

- **RTprop identifiability.** If a peer never serves a small block, the size-residual RTprop
  leans on the regression; cap it below a sane max. The residual estimator largely removes
  the original reason for ProbeRTT (refreshing a `min(elapsed)`); keep ProbeRTT only if the
  fuzzer shows it helps, and only in its event-evaluated form (see *Event-driven
  discipline*) — never as a wakeup tick.
- **Hint trust.** `Advertised` is untrusted; it only sizes a *reservation/window*, never a
  security bound — over/under-estimates self-correct via the delay gate and the real
  `serialized_bytes` measured on receipt. Keep the `[floor, MAX_BLOCK_BYTES]` clamp.
- **Bytes-mode permissiveness.** Must verify the new Bytes arm cannot exceed the advertised
  per-peer request-count hard cap (the `available_slots` hard-cap guard from review fix F2
  stays in front of the byte math).

## Build order

1. Byte estimators + size-aware delay gate (inert behind `bbr_cwnd_unit`, traced first).
2. Bytes admission from real hints; flip default to `Bytes`; fuzzer `mixed_block_sizes` +
   `high_bw_fast_peer`.
3. Commit-drain smoothing: attribute the 27–53 s tails (`commit_stall_reason`), fix the
   dominant stall; fuzzer `commit_stall` (queue bounded by memory, no download throttle).
4. Live A/B `byte-cwnd-1`; iterate gains/floors from the trace.
