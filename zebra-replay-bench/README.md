# zebra-replay-bench

Offline benchmark for Zebra's finalized-state commit pipeline, isolated from the
networking stack. It replays real mainnet blocks through production validation
and commit layers with no peers, no body sync, and no head-of-line network noise,
so write, verifier, and block-sync overhead can be compared on a stable baseline.

This `ironwood-main` split runs the current full recompute path. The CLI keeps
`--vct-sidecar` placeholders for later VCT fast-sync benchmark branches, but those
flags are rejected here because the header-root fast-path APIs are not present on
this base branch.

## Two Phases

1. `index` opens a snapshot state DB, reads a height window, validates the hash
   chain, and writes a flat cache of raw `zcash`-serialized blocks.
2. Replay commands open a writable base fork whose finalized tip is `start - 1`,
   stream the cache, and verify the committed tip against the source window.

Splitting the phases keeps RocksDB snapshot reads out of the timed loop and lets
the window warm in page cache first.

## Replay Rungs

`apply` calls `FinalizedState::commit_finalized_direct()` directly and measures
the commit path.

`apply-worker` drives the same blocks through `zebra-state`'s production write
worker, adding the ordered channel feed, worker park/poll loop, precompute
overlap, and chain-tip channel updates.

`apply-verifier` drives blocks through `zebra-consensus::CheckpointVerifier`,
which commits to a real `StateService`. It adds proof-of-work, Equihash,
Merkle-root verification, and checkpoint-range batching.

`apply-sequencer` drives blocks through the real Zakura block-sync `Sequencer`
via the `internal-bench` helper, then submits them to the checkpoint verifier and
state service. It adds body reorder, ordered submit, backpressure, and optional
Zakura JSONL traces.

The verifier and sequencer rungs feed, count, and gate at the last checkpoint at
or below the cache end height. The committed hash at that checkpoint must match
the embedded checkpoint hash.

## Usage

```bash
# What height is a snapshot at?
zebra-replay-bench info --src /path/to/snapshot

# Phase 1: extract a 30k window into a cache.
zebra-replay-bench index \
  --src /path/to/high-snapshot \
  --cache /tmp/win.zrb --start 1800001 --end 1830000

# Phase 2: replay onto a fork whose tip is exactly 1800000.
zebra-replay-bench apply --base /path/to/fork-at-1800000 --cache /tmp/win.zrb

# One rung up: replay through the real zebra-state write worker.
zebra-replay-bench apply-worker --base /path/to/fork-at-1800000 --cache /tmp/win.zrb

# Two rungs up: replay through the checkpoint verifier.
zebra-replay-bench apply-verifier --base /path/to/fork-at-1800000 --cache /tmp/win.zrb

# Three rungs up: replay through the Zakura block-sync sequencer.
zebra-replay-bench apply-sequencer --base /path/to/fork-at-1800000 --cache /tmp/win.zrb
```

`apply-sequencer` commits in pruned storage mode by default, matching the
production mainnet profile. The base snapshot must already be pruned because
pruning is one-way. Pass `--archive` to commit in archive mode instead.

```bash
zebra-replay-bench apply-sequencer \
  --base /path/to/pruned-fork-at-1800000 \
  --cache /tmp/win.zrb \
  --trace-dir /tmp/zakura-traces
```

`--trace-dir` writes structured Zakura JSONL trace tables for body lifecycle and
sequencer state snapshots, then flushes the writer at the end of the run.

## Snapshots

`--src` and `--base` are snapshot roots that contain `state/vN/<network>`.

The base fork's finalized tip must be exactly `start - 1`; the committer asserts
the parent linkage. The simplest setup is forward replay with no rollback: use a
base snapshot whose tip is already `start - 1` and a higher snapshot with block
bodies through `end` as the index source.

A `rollback` subcommand exists to manufacture a base at `start - 1` from a higher
snapshot, but rolling back a large span builds one giant in-memory delete batch
and can OOM. Prefer two snapshots over rolling one back.

## Prefetch

Replay commands stream the cache through a bounded `prefetch` producer. The
producer reads, deserializes, and builds each `CheckpointVerifiedBlock` off the
timed commit thread, then sends prepared blocks through a bounded channel.

`ZRB_PREFETCH_CAP` controls the buffer depth. The default is 64, which keeps
memory flat and prevents dumping a large backlog into the write worker.

## Reserved Sidecar Tooling

`index-roots` writes a sidecar containing per-height commitment roots plus the
successor block after the window. This is kept so later VCT fast-sync benchmark
branches can share the same cache format.

On this split branch, replaying with `--vct-sidecar` exits with an explicit
unsupported-mode error.

## Metrics

Build with `commit-metrics` to emit and dump `zebra_state.*` commit histograms at
the end of replay commands:

```bash
cargo build --release -p zebra-replay-bench --features commit-metrics
```

## Make Targets

```bash
make perf-build-replay-bench   # build the replay binary with commit metrics
make perf-replay-index         # one-time: dump the window to a block cache
make perf-replay               # replay through the direct committer
make perf-replay-worker        # replay through the write worker
make perf-replay-verifier      # replay through the checkpoint verifier
make perf-replay-sequencer     # replay through the block-sync sequencer
```

Window and snapshot paths come from `deploy/runner/cohort.env` (`REPLAY_*`
variables). The script falls back to defaults when they are unset.
