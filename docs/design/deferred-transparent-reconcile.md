# Deferred transparent reconcile (prototype)

Status: **prototype, default-off, opt-in.** Byte-match-verified with lifecycle guards
(height gate, handoff drain barrier, format-check + clean-shutdown). Benchmark-only: safe on
a **disposable** below-checkpoint node; crash recovery + RPC guards are still missing (see
"Production-readiness gaps (remaining)").

## Problem

In the checkpoint-trusted fast-sync range, the finalized committer's dominant cost in
high transparent-churn regions (the 2022 "sandblast" consolidation spam) is **spent-UTXO
resolution**: for each spent transparent input the committer reads `tx_loc_by_hash`
(txid → location) and `utxo_by_out_loc` (location → value), then deletes the entry and
debits the value pool. In the sandblast range this is ~439 cold, random reads per block,
all on the per-block commit critical path.

These reads are not cacheable: ~97% of spends in that range reference UTXOs created
>4096 blocks earlier, so there is no temporal locality. A ceiling probe (skip the spend
work entirely) measured **sandblast throughput 124 → 383 blk/s (+209%)** — i.e. the spend
resolution *is* the entire sandblast penalty; with it removed a sandblast block runs at
the normal (light-region) rate.

## Design

The reads can't be removed (consensus needs the spent value + location), but they can be
**deferred off the per-block path and batched on a separate worker**:

- **Per block** (in the deferred range): the committer does not resolve spent UTXOs. It
  records the spent outpoints + the block into an in-memory window, writes the UTXO
  *creates* + headers + nullifiers + the VCT root fold inline, and skips the spent-side of
  the value pool. (The auxiliary address index is already skipped in pruned mode.)
- **Every N blocks** (`defer_reconcile_interval`, not per checkpoint — mainnet checkpoints
  are only ~30–40 blocks apart, too frequent to amortize): a **reconcile** resolves the
  window's spent outpoints from disk (sorted/deduped), recomputes the value-pool delta, and
  writes the deletes + value pool (`chain_value_pools` tip + per-height `BlockInfo`) in one
  atomic batch.
- **v2** runs the reconcile on a dedicated **worker thread** with a capacity-1 handoff
  channel, so the (CPU-bound) resolution + value-pool recompute + write overlap with
  continued block assembly. The worker owns the running value pool exclusively (the
  assembler defers it), processes windows FIFO (so the value-pool chaining stays ordered),
  and writes only keys disjoint from the assembler's (deletes + value pool vs. creates), so
  there are no shared-state races or key conflicts.

### Correctness invariants

1. **Older spends are durable at reconcile time.** A UTXO is deleted only when its
   spender's window reconciles; each UTXO is spent once, so it is never deleted earlier, and
   by reconcile time all the window's blocks are flushed.
2. **Lazy deletes are safe.** Between reconciles `utxo_by_out_loc` is a transient *superset*;
   the only reader is the next reconcile, which knows the entries are spent. A stale entry
   would only be re-read by a double-spend, impossible in a checkpoint-valid chain.
3. **Deferring the value pool is safe in the checkpoint range.** It is consensus state but
   only *checked* above `max_checkpoint_height` (the semantic verifier). The per-block
   value-pool change is additive, so the interval delta is the sum of per-block deltas.

## Results

Benchmarked offline via `zebra-replay-bench` on a mainnet 1.85M–1.9M pruned snapshot
(includes the sandblast region), run-ahead pipeline depth 8, interval 2000.

- **Byte-match: identical.** The deferred run (both the v1 inline path and the v2 worker)
  produces a **byte-identical value pool, UTXO set (`utxo_by_out_loc`, ~20.0M entries), and
  per-height `BlockInfo`** as the non-deferred run, verified with the `cf-dump` digests at a
  deterministic checkpoint stop. The block-hash checkpoint gate also passes.
- **Throughput (sandblast region):**
  | variant | sandblast | overall |
  |---|---|---|
  | non-deferred baseline | 122 | 213 blk/s |
  | v1 per-checkpoint (~38-block) | −51% | −43% |
  | v1 interval=2000 inline | +2% | +3% |
  | **v2 interval=2000 worker** | **+73%** | **+43%** |

  v2 recovers ~34% of the +209% ceiling. The remaining gap is that the single-threaded
  worker is now the bottleneck (it re-traverses every transaction of each block for the
  value-pool recompute), so sandblast stays below the light-region rate.

## Gating

Default off. Opt-in, reachable by a real node for benchmarking:

- `zebra-state` `Config` (settable under `[state]` in `zebrad.toml`):
  `defer_transparent_reconcile` (off), `defer_reconcile_interval` (0 = per checkpoint).
  `defer_reconcile_inline` (force the v1 inline path) stays `#[serde(skip)]` (bench A/B only).
- `Config::defers_transparent_spends_at(network, height)` requires
  `defer_transparent_reconcile && skip_address_index()` (pruned + checkpoint-sync) **and**
  `height <= max_checkpoint_height`. `Config::defer_reconcile_configured()` is the
  height-independent lifecycle predicate (worker spawn / drain triggers).
- `zebra-replay-bench`: `ZRB_DEFER_TRANSPARENT`, `ZRB_RECONCILE_INTERVAL`,
  `ZRB_RECONCILE_INLINE`, `ZRB_STOP_AT_HEIGHT`.

## Lifecycle guards (implemented)

The reconcile *logic* is byte-match-verified; these lifecycle guards make it safe to
benchmark on a real below-checkpoint node:

1. **Reachability.** `defer_transparent_reconcile` + `defer_reconcile_interval` are serde
   fields on the state config, so a node can opt in from `[state]`. Default off.
2. **Height-bound gate.** `defers_transparent_spends_at` returns false above
   `max_checkpoint_height`, so every above-checkpoint block commits inline (non-deferred).
   Unit-tested (`defers_transparent_spends_only_in_checkpoint_range`).
3. **Handoff drain barrier (consensus-critical).** Before the first block above
   `max_checkpoint_height` commits, the finalized committer flushes every in-flight block to
   disk, drains the pending reconcile window (`flush_and_join` the worker), and refreshes the
   pipeline's threaded value pool from the now-current disk pool — so the semantic verifier
   never reads a stale/superset UTXO set or lagging value pool. The common handoff (the
   checkpoint→non-finalized channel close) drains the same way.
4. **Format check + clean shutdown.** The background `check_new_blocks` skips the new-blocks
   validation while deferral is configured and the tip is in the deferred range (the
   `BlockInfo`/value-pool lag there is expected, and the address index is off). The reconcile
   worker is `flush_and_join`'d at the handoff and again via a `Drop` safety net, so a clean
   stop finishes any queued reconcile before teardown.

## Production-readiness gaps (remaining)

1. **No crash recovery.** The reconciled (durable transparent) tip trails the committed tip.
   A crash mid-window leaves `utxo_by_out_loc` a superset and `BlockInfo`/value pool lagging;
   restart would need to re-derive the pending window. Not implemented — a node enabling this
   must be treated as **disposable / re-snapshottable**.
2. **No RPC guards.** Mid-window the UTXO set is a superset and the value pool lags, so
   address/utxo/value RPCs would return wrong results in the deferred range. Not guarded.

Until crash recovery + RPC guards are implemented, this remains a benchmarking feature for a
disposable below-checkpoint node, not a general-purpose production mode.

## Benchmarking on a real below-checkpoint node

1. Snapshot a pruned + checkpoint-sync node whose tip is well below `max_checkpoint_height`
   (mainnet ~3.36M) — the node must be **disposable** (re-snapshot per run; no crash
   recovery).
2. In `zebrad.toml` `[state]`: set `defer_transparent_reconcile = true` and
   `defer_reconcile_interval = 2000` (pruned storage + checkpoint sync are required for the
   address index to be off, which the gate needs).
3. Set `debug_stop_at_height` to a height still below `max_checkpoint_height` so the run
   stops inside the deferred range (the handoff barrier is exercised by code/tests, not by
   the bench range).
4. Avoid address/utxo/value RPCs against the node while it is in the deferred range.
