# Block-Sync BBR Congestion Control — Specification

## Overview

The controller runs **per peer** and is **byte-denominated**: it measures how fast a
peer delivers block bodies (bytes/second) and how quickly it answers (round-trip
time), and from those it sizes a per-peer **in-flight window** — the amount of
outstanding body bytes we allow that peer at any moment. One request fetches one
block body, so the in-flight _request count_ is simply `window ÷ body size`.

It pursues three goals:

1. **Responsiveness** — when we must re-request ("rescue") a block from a different
   peer because its current carrier is slow, that peer can serve it almost
   immediately. This only holds if the peer's queue is shallow: a freshly issued
   request sits near the front instead of behind a long backlog.
2. **Throughput** — every peer's link stays full.
3. **Bounded memory** — total outstanding data is capped no matter how peers behave.

The key insight is that responsiveness and throughput are **not** in tension at the
right window size. The smallest window that still keeps a link full is exactly **one
BDP** (byte delivery rate × base round-trip). At that point the pipe is saturated
(full throughput) and almost nothing is queued (full responsiveness); any larger
window adds only queue — latency — for zero extra throughput.

So the controller:

- **maximizes throughput** by growing the window toward `BDP × gain`, probing just
  above the measured pipe so it finds and fills available bandwidth;
- **maximizes responsiveness** by never growing it further — periodically draining
  the queue (ProbeRtt) to re-confirm the true base round-trip, and trimming the
  window the instant a standing queue starts to form (the delay gradient); and
- **bounds memory** with a global in-flight byte budget and per-peer caps, kept
  independent of the window logic.

## Glossary

The names below are the plain-language terms used throughout this document; the
BBR/code identifier follows in parentheses.

- **In-flight window** (`cwnd`, "congestion window") — the maximum body bytes a single
  peer may have outstanding to us at once. A larger window means more parallel
  requests to that peer.
- **Base round-trip** (`RTprop`) — the _minimum_ recent round-trip time to a peer:
  how long send-then-receive takes when nothing is queued. It is a latency, **not** a
  window size, but it sizes the window (see BDP). "RT" = round trip.
- **BDR — byte delivery rate** (`BtlBw`, "bottleneck bandwidth") — the _maximum_
  recent rate, in bytes/second, at which a peer delivers bodies: the speed of the
  bottleneck link to that peer.
- **BDP — bandwidth-delay product** = `BDR × base round-trip` — the amount of
  in-flight data that exactly fills the pipe to a peer. Hold this much outstanding and
  the link is busy with no standing queue; hold less and it idles, hold more and the
  excess only sits in a queue. This is the window's natural target.
- **Gain** — a multiplier above 1 applied to the BDP so the window probes slightly
  past the measured capacity, letting it discover newly available bandwidth.
- **Measurement horizon** — the recent time span (10 s) over which we take the min
  (base round-trip) and max (BDR). The `*_window` config knobs set these.
- **ProbeBw / ProbeRtt** — the two operating modes. ProbeBw is steady state (window ≈
  BDP). ProbeRtt briefly drains the queue to re-measure an honest base round-trip.
- **Delay gradient** — the signal that a queue is forming: the recent round-trip has
  risen above the base round-trip by more than a set ratio. It trims the window down.
- **Floor** — the lowest block height we still need; the chain cannot commit past it.
  A "floor request" fetches it, and if its carrier is slow the height is _rescued_ —
  re-requested from a faster peer.

## Measured signals (per peer)

- **Base round-trip** — windowed _min_ of the raw request round-trip
  (`bbr_rtprop_window`, 10 s). The propagation floor; never collapses to zero.
- **BDR** — windowed _max_ of the per-response delivery rate in bytes/s
  (`bbr_delivery_rate_window`, 10 s).
- **BDP** = `BDR × base round-trip` (bytes). Window target =
  `max(min window, BDP × gain)` (`gain = 200%`).
- **Delay gradient** — a smoothed round-trip compared against a size-aware healthy
  baseline (`base round-trip + bytes/BDR`), used to detect a building queue.

In-flight admission compares a peer's **reserved body bytes** against its window; the
in-flight _request count_ falls out as `window ÷ body size`.

## Control law — MUST

- A peer's outstanding reserved bytes MUST NOT exceed its window (plus the bounded
  floor bypass below).
- **ProbeBw** (steady state): the window tracks `BDP × gain`, clamped above by the
  delay-gradient ceiling and below by the minimum window.
- **ProbeRtt**: every `bbr_probe_rtt_interval` (10 s) the window MUST drain to the
  minimum and hold for `bbr_probe_rtt_duration` (200 ms) so one uncontended request
  yields a fresh base round-trip. Without this, a sustained queue inflates the
  base-round-trip min and the window never collapses for a genuinely slow peer.

## Control law — SHOULD

- On a real request timeout the window SHOULD take one multiplicative dip (`×0.85`,
  bounded by the minimum) and pull the delay ceiling down to the dipped value. A
  timeout is merely congestion evidence, it should not trigger backoff.
- When the smoothed round-trip exceeds `base round-trip × delay_gradient` (150%) the
  queue is building, so the window ceiling SHOULD ratchet down (`×0.9`); when there is
  headroom it SHOULD relax up (~12% per response, saturating) so a cleared queue
  re-probes for bandwidth.

---

## Edge cases and security bounds

### Problem: a fast peer's base round-trip can collapse toward zero and void the BDP

On a fast link, subtracting a body's transmission time from its round-trip leaves
almost nothing, which would zero `BDR × base round-trip` and pin the window at its
floor.

- The BDP MUST be sized from the **raw** round-trip minimum, never a
  transmission-stripped residual (the residual is used only by the delay gate, so a
  big body's honest transfer time is not mistaken for a queue).
- The window MUST be floored at `bbr_min_cwnd_bytes` (4 MiB, the primary tuning
  lever) so a near-zero BDP still keeps the pipe primed.

### Problem: a burst of buffered bodies inflates the BDR

Many bodies arriving in one tick would read as an impossibly high rate.

- The delivery-rate interval MUST be floored at the previous base round-trip, so a
  single-tick burst cannot inflate the BDR max.

### Problem: a slow peer holding the contiguous floor stalls the whole sync

The lowest missing height gates commit; one slow carrier must not pin it. This is the
responsiveness goal made concrete — a shallow per-peer window is what lets a rescue
land fast.

- A **floor** request MUST carry a short fixed leash (`floor_rescue_timeout`, 2 s);
  on expiry its height MUST be returned to the queue and the peer retry-avoided —
  rescued to a faster carrier, **not** disconnected (record-only).
- The floor MAY borrow up to `floor_bypass_slots` (2) representative bodies beyond a
  saturated window so it is fetched even when every peer is at its window; the borrow
  MUST stay within the advertised request-count cap and reserve real budget.
- **Above-floor** speculation SHOULD use a patient, size-aware deadline
  (`request_timeout + estimated_bytes ÷ BDR`) and MUST NOT gate the floor.

### Problem: unbounded memory under attacker-controlled bodies or stalls

- Total in-flight + reorder + applying bytes MUST be bounded by the global
  `max_inflight_block_bytes` budget (6 GiB). Concurrent reservations MUST NOT
  over-commit it.
- Every per-request size estimate MUST be clamped to `[floor, MAX_BLOCK_BYTES]`;
  untrusted header size hints MUST NOT exceed the per-block worst case.
- The advertised request-count cap (≤ `MAX_BS_INFLIGHT_REQUESTS = 32 768`) MUST bind
  even when byte headroom remains, so a peer serving tiny bodies cannot be issued an
  unbounded request count.
- The reorder look-ahead and the serving-request heap MUST be bounded.

### Problem: an unbounded wait wedges a peer

- Every outbound request MUST have a network deadline — the **only** sanctioned timer
  (and itself an event). The above-floor deadline assumes a minimum delivery rate when
  a peer's measured BDR is still near zero, so the deadline is finite (~16 s worst
  case), never unbounded.

### Numeric safety — MUST

- Arithmetic over external/untrusted values MUST saturate or be checked — never wrap
  or panic.
- Rates and BDP products MUST be clamped to finite, non-negative values before they
  size a window.

---

## Defaults

| Knob | Default | Meaning |
| --- | --- | --- |
| `bbr_cwnd_unit` | `bytes` | window budgets header-hinted body bytes |
| `bbr_cwnd_gain_percent` | 200 | window target = 2 × BDP |
| `bbr_min_cwnd_bytes` | 4 MiB | window floor / cold-start (primary lever) |
| `bbr_min_cwnd` | 4 | window floor in blocks (A/B baseline unit) |
| `bbr_rtprop_window` | 10 s | base-round-trip measurement horizon |
| `bbr_delivery_rate_window` | 10 s | BDR measurement horizon |
| `bbr_probe_rtt_interval` | 10 s | ProbeRtt cadence |
| `bbr_probe_rtt_duration` | 200 ms | drained hold to refresh base round-trip |
| `bbr_delay_gradient_percent` | 150 | queue-building round-trip ratio |
| `floor_rescue_timeout` | 2 s | floor leash before rescue |
| `floor_bypass_slots` | 2 | floor borrow beyond the window |
| `max_inflight_block_bytes` | 6 GiB | global in-flight byte ceiling |
| timeout dip / delay-cap down / EWMA α | ×0.85 / ×0.9 / 0.25 | (constants) |
