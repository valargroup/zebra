# Hedged head-of-line block download (prototype)

Branch: `proto-hedged-hol-download` (off `sync-perf-main-2`)
Worktree: `/root/zebra-hedge-hol` · Build target: `/root/cargo-target-hedge`

## Problem

Checkpoint sync commits blocks in strict height order, so throughput is hostage to the tail latency
of the single next-needed (head-of-line) block. The measured stall (host bench data) is not peer
saturation but **inventory-marker staleness**: ready peers exist but are all marked "missing" the head
block's hash (`pool.route_inv.notfound.all_missing`), so `route_inv` returns a synthetic
`NotFoundRegistry` without trying any of them. That triggers the #105 head-of-line backoff (2s) and the
registry-miss counter climbs (~162 synthetic misses : 1 real refusal) while ~2000 blocks sit buffered
behind the head.

The existing tower `Hedge` layer (`sync.rs`, `AlwaysHedge`) does not help here: it hedges the *same*
service stack, so the duplicate also flows through `route_inv` and hits the same `all_missing`
short-circuit. It also keys on a latency percentile, but `route_inv` fails fast rather than hanging.

## Change

When a required block registry-misses, re-dispatch its backoff retry as a **fan-out to a few random
ready peers, ignoring inventory markers**, and take the first peer that actually delivers it. This
bypasses the stale markers (the peers usually do have the block). Scoped to the head-of-line block
only, with a small fanout — DoS-bounded.

Per the chosen strategy ("reactive at registry-miss"), the 2s backoff and all #105 gating are
unchanged; only *how* the backed-off block is re-fetched changes.

### Files

- `zebra-network/src/protocol/internal/request.rs` — new `Request::HedgedBlocksByHash { hashes, fanout }`
  variant (a peer-set routing directive; rewritten to `BlocksByHash` per peer, so connections/wire are
  untouched). Added to the `Display`, `command`, `is_inventory_download`, `block_hash_inventory` arms.
- `zebra-network/src/peer_set/set.rs` — `call()` arm + `route_hedge()`. Reuses the existing
  `select_random_ready_peers` (random, load-ignoring — same security stance as broadcast) and resolves
  with the first `Response::Blocks` containing an available block; otherwise returns the same
  `NotFoundRegistry` as `route_inv`, so the sync-layer retry/backoff handling is unchanged. Loser
  per-peer calls are cancelled when the future set drops on first success. New metrics:
  `pool.route_hedge.{dispatch,win,exhausted,no_ready}.count`.
- `zebrad/src/components/sync/downloads.rs` — `download_and_verify_hedged(hash, fanout)`; the existing
  `download_and_verify` and it now share a private `queue_download(hash, request)` (only the request
  variant differs; response parsing, hash-binding check, and cancellation are identical).
- `zebrad/src/components/sync.rs` — `hol_hedge_fanout` field, read once from env `SYNC_HOL_HEDGE_FANOUT`
  (default `0` = off). At the registry-miss timer re-dispatch, if `> 0`, call the hedged variant.
- `zebra-network/src/peer/connection.rs`, `zebrad/src/components/inbound.rs` — defensive match arms
  (the variant never reaches these paths; handled identically to `BlocksByHash`).

### Pre-existing fix (unrelated, required to compile tests)

`zebrad/src/components/sync/tests/vectors.rs` called `Downloads::new` with 6 args on `sync-perf-main-2`
while `new` requires 7 (a `Network` param). This broke the entire zebrad lib-test target on the base
branch. Added the missing `Network::Mainnet` arg + import so tests compile. Not part of the feature.

## A/B gating

One binary, env-toggled:

- `SYNC_HOL_HEDGE_FANOUT=0` → baseline (identical to shipped #105 behavior; hedged variant never built).
- `SYNC_HOL_HEDGE_FANOUT=4` → reactive hedged retry.

## Tests

- `cargo test -p zebra-network --lib peer_set::set::tests` — includes
  `peer_set_route_hedge_bypasses_missing_markers`: two peers both marked missing the hash; the hedge
  still dispatches the rewritten `BlocksByHash` to both, where `route_inv` dispatches to neither (see
  the sibling `peer_set_route_inv_all_missing_fail`). Passes.
- `cargo fmt --all -- --check`, `cargo clippy -p zebra-network -p zebrad --all-targets -D warnings` —
  clean (pre-existing zebra-rpc `ValueCommitment` clone warnings unrelated).

## Benchmark (validation) — random DNS peers, NOT a pinned peer

The stall only manifests with diverse/churning peers, so use the default DNS peer set (the pinned-peer
A/B used for the tree work is the wrong harness here). Reuse the host fork harness (`RUNBOOK.md`),
config: `debug_stop_at_height=1760000`, `checkpoint_verify_concurrency_limit=1500`,
`download_concurrency_limit=150`. N ≥ 6 per arm (mirrors #105's 1/6 vs 0/13 method). Build the release
binary with `CARGO_TARGET_DIR=/root/cargo-target-hedge cargo build -p zebrad --release` (optionally
`--features commit-metrics`).

```bash
SYNC_HOL_HEDGE_FANOUT=0 /root/wal-bench/prbench.sh hedge-off /root/cargo-target-hedge/release/zebrad 420 5
SYNC_HOL_HEDGE_FANOUT=4 /root/wal-bench/prbench.sh hedge-on  /root/cargo-target-hedge/release/zebrad 420 5
```
(Confirm `prbench.sh` forwards the env to the spawned `zebrad`; if not, export it inside the script.)

Compare across arms:
- Stalled-run count (intervals with throughput ≈ 0 while `sync_downloads_in_flight > 1000`).
- `sync.missing.block.registry.{miss,retry}.count` totals — expect a sharp drop on the ON arm.
- `pool.route_hedge.win.count` vs `pool.route_inv.notfound.all_missing.count` — the hedge should
  convert `all_missing` failures into wins.
- Post-escape steady-state blk/s — expect no regression in healthy intervals (hedge is inert when no
  block registry-misses).

## Honest risk

#105 already cut stalls to ~0/13 by giving inventory markers time to refresh during the backoff, so the
marginal *stall-count* win may be small. The signal to target is the reduction in accumulated
registry-miss/retry cycles and tail-latency events on the residual cases (blocks that stay `all_missing`
across several backoffs, or never refresh within budget). Report registry-miss totals and route_hedge
win rate, not just binary stall count; be prepared to conclude "inert / no measurable win" if the
current code already absorbs the stall.
