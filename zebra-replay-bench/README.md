# zebra-replay-bench

Offline benchmark for the Zebra finalized-state **commit pipeline**, isolated
from the networking stack. It replays real mainnet blocks through the exact
production committer (`FinalizedState::commit_finalized_direct`) — no peers, no
header/body sync, no head-of-line noise — so the write-assembler + disk-writer
work can be measured and A/B compared on a stable baseline.

It deliberately runs the **legacy recompute path** (`vct_fast_sync = false`), the
per-block note-commitment-tree + history-tree work that dominates commit cost.

## Two phases, one flat cache

1. **`index`** opens a snapshot state DB **read-only**, reads a height window into
   a flat, hash-chain-validated cache file (raw `zcash`-serialized blocks).
2. **`apply`** opens a writable base fork whose tip is `start-1`, streams the
   cache, and commits each block in order, timing every commit. It verifies the
   final tip hash equals the source snapshot's, then prints throughput.

Splitting into phases keeps RocksDB read noise out of the timed loop and lets the
window warm in page cache first.

## Usage

```bash
# What height is a snapshot at?
zebra-replay-bench info --src /path/to/snapshot

# Phase 1: extract a 30k window into a cache (source opened read-only)
zebra-replay-bench index \
  --src /path/to/high-snapshot \
  --cache /tmp/win.zrb --start 1800001 --end 1830000

# Phase 2: replay onto a fork whose tip == 1800000
zebra-replay-bench apply --base /path/to/fork-at-1800000 --cache /tmp/win.zrb
```

`--src` / `--base` are snapshot roots that contain `state/vN/<network>`.

The base fork's tip must be exactly `start-1`; the committer asserts the parent
linkage. The simplest setup is a **forward** replay with no rollback: use a base
snapshot whose tip is already `start-1` and a higher (near-tip) snapshot with
block bodies through `end` as the index source.

> A `rollback` subcommand exists to manufacture a base at `start-1` from a higher
> snapshot, but rolling back a large span builds one giant in-memory delete batch
> and will OOM. Prefer two snapshots (a near-tip source + a base already at
> `start-1`) over rolling one back.

## Per-phase metrics

Build with the `commit-metrics` feature to emit and dump the `zebra_state.*`
commit histograms (`update_trees`, `commitment_check`, `batch_prep`,
`rocksdb.batch_commit`, `commit_finalized_total`, ...) at the end of `apply`:

```bash
cargo build --release -p zebra-replay-bench --features commit-metrics
```

## Make targets

```bash
make perf-build-replay-bench   # build the bench binary (commit-metrics)
make perf-replay-index         # one-time: dump the window to a block cache
make perf-replay               # legacy replay through the committer
# VCT fast path:
make perf-replay-index && deploy/runner/replay_run.sh index-roots
make perf-replay REPLAY_VCT_SIDECAR=/path/to/win.vct
```

Window/snapshot paths come from `deploy/runner/cohort.env` (the `REPLAY_*` vars);
the script falls back to sensible defaults when they're unset.
