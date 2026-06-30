# Block-sync: slow-peer survival + floor-HoL elimination

> Branch: `evan/perf-plus-download-fixes`. Motivated by the `bbr-committer-2` us-0 live
> trace (2026-06-29, binary `113013a6`, 55.1 blk/s). Companion to and **builds on**
> `BLOCKSYNC_BYTE_CWND_PLAN.md` (byte-denominated BBR). See memory `bbr-committer-2-result`.

## Status (2026-06-29)

- **WS-C — DONE before this work landed.** `bbr_cwnd_unit` already defaults to `Bytes`
  (`config.rs`), with byte BtlBw, size-residual RTprop, size-aware delay gate, byte
  admission, and `bbr_min_cwnd_bytes` all live (commits `0e20656b8`..`bb3e02943`).
  `nominal_request_bytes` is retired. The trace ran the *blocks* binary; the byte
  controller has since landed, so the prerequisite is satisfied.
- **WS-A — LANDED.** A1 extends the RTprop floor-preference to the normal take path
  (`peer_registry.rs` `floor_has_preferred_unsaturated_server` gains an `include_equal`
  arg: bypass `<=`, normal strict `<`; call site `peer_routine.rs try_fill`). A2 replaces
  the flat 8 s deadline with the pure `admission::request_deadline` (Floor → short
  `effective_floor_rescue_timeout`, default 2 s; AboveFloor → `request_timeout +
  estimated_bytes / max(BtlBw, 256 KiB/s)`). Unit tests in `admission.rs` +
  `peer_registry.rs`; fuzzer `fuzz_one_slow_peer_hol` strengthened (slow byte-accurate
  peer + 2 carriers, single-block; asserts convergence + zero reaper disconnects).
- **WS-B — EMERGENT, no code.** `note_block_progress` already resets the 32 s liveness
  deadline on *every* received body (incl. above-floor), so a ~16 s-cadence peer survives;
  the reaper only fired in the trace because the *blocks*-unit over-pipelining stalled
  delivery entirely, which WS-C removes (depth → ≈1). WS-B2 ("don't read an honest serve
  as congestion") falls out of A2's size-aware deadline. Validated by the fuzzer
  `protocol_rejects == 0` assertion.
- **WS-D — DEFERRED (separate track).** Commit-side `vtip` durable-write watch; it is the
  `glue_refactor` scoped work, larger and independent. Not part of this download-side change.

The remainder of this document is the original design.

## The ask (user directive, verbatim intent)

> "provided they are sending us a block every say 16s we should not kick them though. we
> should ensure we're not giving them more requests, but every little bit of HoL we want to
> avoid here including the 400 missing"

Three concrete goals:
1. **Don't kick a peer that's still delivering** (~one block / 16 s is fine).
2. **Don't over-feed slow peers** — shallow request depth, no deep per-peer queue.
3. **Eliminate every bit of head-of-line blocking, including the ~400-block apply backlog**
   (`floor − vtip`, and the 401 `sequencer_input` cap) that sits behind slow peers + the
   commit-side `vtip` lag.

## Evidence (bbr-committer-2, 17.3 min, 1.762M snapshot, 55.1 blk/s)

- **2 carriers serve 93%** of bodies (`5ebbf236` 84.6%, `1f93f117` 8.5%), **1 connection
  each, 0 closes** — rock solid. **All churn is on 6 marginal peers** (7.3% of bodies,
  50–94 reconnects each).
- Churn mechanism = **slow-peer over-pipelining → HoL → reaper → reconnect**: slow peers
  (serve 0.7–2.5 s/block) get cwnd-probed to 8–13, go HoL (`out@send` 20–50 → `elapsed`
  2–10 s), stop delivering, trip the 32 s liveness reaper (`block_sync_no_block_progress`
  ×67, 415 blocks abandoned), reconnect, repeat. This also drives the **24.5% re-request
  rate**.
- The harm is the **floor / apply backlog**, not the churn itself: `floor − vtip` p50 107 /
  max 4103; `sequencer_input == 401` 2.9%; the carrier's one **12.5 s stall** is a
  commit-side `vtip`-advance gate. Killing the slow peers gains nothing — they add ~4 blk/s
  of real bandwidth. The fix is to make them **useful-but-harmless**, never on the floor
  critical path.

## Design principle

**The floor rides the fast carriers; slow peers do speculative above-floor lookahead that
lands in the reorder buffer and never gates the contiguous verified tip.** A slow peer is
kept alive while it makes any progress, holds at most ~1 request, and any floor-critical
height it happens to hold is rescued to a carrier within a couple of carrier-serve-times —
without disconnecting it.

## Current machinery (code anchors — most of this already exists)

- **Per-request deadline is a flat 8 s for every request** regardless of priority or block
  size: `deadline = queued_at + self.config.request_timeout`
  (`peer_routine.rs:842`; `DEFAULT_BS_REQUEST_TIMEOUT = 8 s`, `config.rs:78`).
- **On expiry, the floor is already rescued** (just slowly/coarsely): `expire_due_timeouts`
  (`peer_routine.rs:951`) returns unreceived heights to the `WorkQueue`, applies a
  `retry_avoid` bias against this peer, and dips the cwnd (`record_timeout`). A faster peer
  then contests the height.
- **`RequestPriority::Floor` vs `AboveFloor`** exists (`admission.rs`) but is a *budget*
  priority only (floor gets full budget; above-floor is bounded by reorder lookahead). It is
  **not** peer-speed-aware.
- **RTprop-aware floor preference exists but only fires in the bypass region**:
  `registry.floor_has_preferred_unsaturated_server(height, self, rtprop)`
  (`peer_registry.rs:434`) is consulted only when `in_bypass` (cwnd saturated,
  `peer_routine.rs:627`). A slow peer with spare *normal* slots (cwnd 5, unsaturated) freely
  grabs floor-adjacent heights and sits on them.
- **Peer reaper** = single per-peer liveness timer: `check_liveness` (`state.rs:1056`) returns
  `Disconnect` once `block_liveness_deadline` passes (`= request_timeout × 4 = 32 s`,
  `effective_liveness_timeout`, `config.rs:383`) while `outstanding` is non-empty; the timer
  resets on any delivery (`note_block_progress`, `state.rs:1041`).
- **byte-cwnd seam landed but is inert**: `bbr_cwnd_unit` has a `Bytes` arm and
  `bbr_min_cwnd_bytes` (commit `0e20656b8`), but the **default is still `Blocks`**, so the
  trace ran blocks-denominated → cwnd floor-pinned at 5.

## The fix — four workstreams (A/B are the new emphasis; C/D land from the byte-cwnd plan)

### WS-C (prerequisite): shallow slow-peer queues — finish byte-cwnd

Land `BLOCKSYNC_BYTE_CWND_PLAN.md` §1–2: `BtlBw` in bytes/sec, RTprop size-residual,
size-aware delay gate, admission from the real `Advertised` header hints, and **flip
`bbr_cwnd_unit` default → `Bytes`**. A slow peer's byte-BDP (low `BtlBw` × small RTprop) is
**< one body**, so its in-flight depth self-limits to ≈ 1; the carriers deepen (cwnd ≈ 8–10
at 50 ms RTT for small blocks). With depth ≈ 1 a slow peer **physically cannot build the
deep HoL queue** the trace shows (cwnd 8–13, `out@send` 20–50). This is what makes "don't
over-feed them" true and makes WS-A/WS-B's per-request deadlines act on a single request
instead of a backlog. **Without WS-C, A and B don't bite** — do this first.

### WS-A: the floor never waits on a slow peer (download-side "400 missing")

**A1 — Speed-stratified floor assignment (extend RTprop preference to the normal path).**
In `try_fill` (`peer_routine.rs` ~627–639), gate the Floor arm on the RTprop preference
**even when `!in_bypass`**: a peer declines a floor-adjacent height when
`registry.floor_has_preferred_unsaturated_server(download_floor, &self.peer, rtprop)` is true
(a received-status peer with available slots and `≤` RTprop exists), and falls through to
above-floor speculative work. Net: slow peers stop taking the floor whenever a faster carrier
can serve it; the contiguous floor concentrates on the carriers.
- **Liveness guard (must not wedge):** the existing predicate requires `≤ self_score`, so the
  lowest-RTprop server of a height still takes it; only a *strictly faster* available peer
  causes a defer. Keep the "all servers saturated → `false` → floor still moves" fallthrough.
  If only slow peers can serve a height, the slowest-acceptable still takes it.

**A2 — Priority/size-aware per-request deadline** (replace the flat `queued_at +
request_timeout` at `peer_routine.rs:842`):
- **Floor priority → short rescue deadline.** `floor_deadline = queued_at +
  floor_rescue_timeout`, where `floor_rescue_timeout` ≈ a few × the median carrier serve time
  (new small config knob, e.g. 1.5–3 s), **not** 8 s. On expiry the existing
  `expire_due_timeouts` path returns the height + `retry_avoid`-biases the slow peer → a
  carrier with spare byte-cwnd re-fetches it in ~tens of ms. **The slow peer is NOT
  disconnected** — it just loses that one height. (This is a hedged/short-leash floor request,
  the tight-HoL version of the rescue that already exists at 8 s granularity.)
- **Above-floor priority → size/speed-aware, generous deadline.**
  `deadline = queued_at + base_rtt + estimated_bytes / max(peer_btlbw, floor_bw) + slack`.
  Above-floor timeouts don't gate the floor, so they can be patient: a legitimately slow
  big-block fetch (e.g. 2 MB at 200 KB/s ≈ 10–16 s) is **not** abandoned at 8 s. This is the
  direct "block every 16 s → don't abandon it" lever for speculative work.

### WS-B: keep slow-but-progressing peers (don't kick at a 16 s cadence)

**B1 — Progress-relative reaper.** With WS-A, slow peers only hold above-floor speculative
requests, so reaping them gains nothing — keep them. Make `check_liveness` /
`note_block_progress` (`state.rs:1041–1062`) arm the deadline relative to the **peer's own
delivery cadence** rather than the global `request_timeout × 4`: a peer is alive while it
delivered *any* block within `k × max(measured_serve_interval, request_timeout)`. A 16 s-cadence
peer survives; only true silence (no delivery for a long multiple) reaps. Keep
`request_timeout × 4` as the floor for a brand-new peer with no cadence sample yet.

**B2 — Don't read an honest slow serve as congestion.** The size-aware above-floor deadline
(A2) means only a true overrun *past the size-expected time* counts as a timeout and dips the
cwnd (`record_timeout`). An honestly-slow peer serving within its size-expected time no longer
gets ratcheted toward `min_cwnd` and starved further.

**B3 — Target:** `block_sync_no_block_progress` reaps → ≈ 0; the 50–94 reconnects/slow-peer
collapse toward ≈ 1 (validate in the re-run).

### WS-D: commit-side of the "400 missing" — smooth the `vtip` advance

Land `BLOCKSYNC_BYTE_CWND_PLAN.md` §5 / the `glue_refactor` scoped direction: **release the
verified tip on a durable-write watch**, replacing the 5 s `CHECKPOINT_FRONTIER_REFRESH_INTERVAL`
poll and the 30 s `commit_stalled` watchdog, advancing `vtip` per durable contiguous block.
This collapses the `floor − vtip` backlog (max 4103) and the carrier's 12.5 s stall +
`sequencer_input == 401`, so the floor that WS-A keeps moving actually drains and the queue
sits at the **memory ceiling only**. Event-driven, no backstops; demote `commit_stalled` to a
diagnostic emit. **Do not throttle download to the commit rate** — fix commit, never cap
download.

## Build / validation order

1. **WS-C** byte-cwnd (prerequisite). Fuzzer: `mixed_block_sizes`, `high_bw_fast_peer`,
   `one_slow_peer_hol` (needs byte-accurate peer serve model: `elapsed = base_rtt +
   bytes / peer_bw`).
2. **WS-A** floor stratification + priority-aware deadlines. Fuzzer: `one_slow_peer_hol`
   asserts floor-HoL p99 ≈ one carrier serve and the slow peer never holds the floor height
   past `floor_rescue_timeout`; a 2-carrier + 6-slow mix keeps the floor advancing.
3. **WS-B** progress-relative reaper. Fuzzer: a steady 16 s-cadence peer is never reaped; a
   silent peer still is.
4. **WS-D** commit-drain watch (can land in parallel — it's the glue_refactor scoped work).
5. **Live A/B re-run** us-0, same snapshot + `analyze.py`/`bbr_extra.py`/`churn.py` scripts →
   label `slow-peer-floor-1`. **Targets:** throughput up from 55 blk/s; floor-HoL p99 down;
   re-request % < 24.5%; `block_sync_no_block_progress` reaps → ≈ 0; slow-peer reconnects → ≈ 1;
   apply-backlog sawtooth amplitude down; the 2 carriers stay stable (1 conn, 0 closes).

## Config knobs (new / changed)

- `bbr_cwnd_unit` default → `Bytes`; `bbr_min_cwnd_bytes` (WS-C).
- **New** `floor_rescue_timeout` (small, e.g. 2 s) — Floor-priority per-request rescue
  deadline (WS-A2).
- Above-floor deadline becomes size/speed-aware (`base + estimated_bytes / btlbw + slack`),
  derived from `request_timeout` as the base (WS-A2).
- Liveness reaper → cadence/progress-relative (WS-B1); keep `request_timeout × 4` as the
  new-peer floor.
- **No AQM / no download-throttle knobs** (unchanged stance).

## Non-goals / guardrails

- **Don't disconnect slow peers as a perf fix** — the churn is a symptom; keep them for their
  bandwidth.
- **Don't throttle download to commit** — make commit faster (WS-D), never cap the downloader.
- **Keep single-block-per-request** (`DEFAULT_BS_BLOCKS_PER_RESPONSE = 1`).
- **Floor-preference must never wedge** the floor when only slow peers can serve a height (the
  `≤ self_score` predicate + the saturated-fallthrough preserve liveness).
- **Event-driven only**: every deadline here is a per-request network deadline (the one
  sanctioned timer); no new polls/ticks/watchdogs (`prefers-fully-event-driven-no-backstops`).

## Separate, untouched

`best_header_tip` anomaly (header lead 325,665, impossible for mainnet) is a header-sync
reporting bug — does not affect block-sync ordering; out of scope here.
