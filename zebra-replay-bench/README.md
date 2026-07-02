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

# One altitude up: replay through the real zebra-state write worker
zebra-replay-bench apply-worker --base /path/to/fork-at-1800000 --cache /tmp/win.zrb

# Two altitudes up: replay through the real zebra-consensus checkpoint verifier
zebra-replay-bench apply-verifier --base /path/to/fork-at-1800000 --cache /tmp/win.zrb

# Three altitudes up: replay through the real Zakura block-sync Sequencer (VCT-only)
zebra-replay-bench apply-sequencer --base /path/to/fork-at-1800000 --cache /tmp/win.zrb --vct-sidecar /tmp/win.vct
```

## Third altitude: `apply-verifier`

`apply-verifier` drives blocks through the real `zebra-consensus::CheckpointVerifier`,
which internally commits to a real `zebra-state` `StateService` (→ write worker →
committer). It adds the per-block work the lower rungs skip: proof-of-work
(difficulty + equihash) and Merkle-root validity, plus checkpoint-range batching.
Unlike `apply`/`apply-worker` (sync), it runs on a multi-thread tokio runtime
because the verifier is a Tower service. Comparing its throughput to `apply-worker`
isolates what verification adds on top of the commit pipeline.

Checkpoint batching + the VCT successor boundary: the verifier only releases a
block once its whole checkpoint range is contiguous, and the worker's VCT fast path
can't commit a block until its successor is buffered. The final checkpoint's
successor is in the dropped tail (its range can't complete past the window), so the
bench **feeds** up to the last checkpoint `<= end` (to deliver the successors the
worker needs) but **counts/gates** to the second-to-last checkpoint, checking that
block's committed hash against the embedded checkpoint hash. As with the other
rungs, blocks are read/parsed off-thread by the bounded prefetch and fed with a
bounded in-flight window (`ZRB_PREFETCH_CAP`, at least one checkpoint gap so ranges
always complete); a periodic progress log makes any stall observable.

## Fourth altitude: `apply-sequencer` (VCT-only, POC)

`apply-sequencer` drives blocks through the **real Zakura block-sync `Sequencer`**
(`zebra-network`), one rung above the verifier. The sequencer is the body reorder +
ordered-submit pipeline: bodies are fed into its reorder queue, it drains the
contiguous prefix into `applying` and emits `SubmitBlock`s, and a thin driver here
commits each through the same real `CheckpointVerifier` → `StateService` as
`apply-verifier`, reporting the commit back so the sequencer frontier advances and
releases the next blocks. Comparing to `apply-verifier` isolates the sequencer's
reorder/ordering/backpressure overhead.

It uses the real `SequencerTask` via a feature-gated helper
(`zebra_network::zakura::spawn_bench_sequencer`, `internal-bench`). **VCT-only**
(the Zakura fast-sync path) and, for this POC, bodies are fed **in height order**
(random / out-of-order multi-peer arrival — the reorder buffer's stress case — is a
future knob). Same checkpoint-batching boundary as `apply-verifier`: feed to the
last checkpoint, commit/gate to the second-to-last.

Two optional flags:

- Storage mode is **Pruned by default** (the `--base` snapshot must already be pruned;
  pruning is one-way), matching the production mainnet config. Pass `--archive` to commit
  in Archive mode instead (full raw-tx + indexes, ~2× the bytes written; needs an archive
  base).
- `--trace-dir <dir>` writes the **structured Zakura JSONL trace tables** (the
  `block_sync` body lifecycle: `block_body_accepted` / `block_body_submitted` with
  queue-elapsed and apply tokens) — the same tables `perf-run-mainnet` emits via
  `[network.zakura] trace_dir`, through the real `ZakuraTrace`/`JsonlTracer`. The
  writer is flushed at end-of-run. Without it the tracer is `noop()` (zero overhead).
  Via the harness: `REPLAY_TRACE_DIR=<dir> make perf-replay-sequencer` (add
  `REPLAY_ARCHIVE=1` for archive mode).

## Two altitudes: `apply` vs `apply-worker`

`apply` calls the committer (`commit_finalized_direct`) directly in a tight loop.
`apply-worker` drives the same blocks through the **production write worker**
(`BlockWriteSender::spawn` / `WriteBlockWorkerTask::run`), the next rung up the
abstraction ladder. The worker adds exactly what the node wraps around each
commit: the in-order channel feed, the one-block VCT successor look-ahead, the
park/poll loop, and the chain-tip channel updates. Both share the same cache,
sidecar, config, snapshots, and correctness gate (final tip hash), so their
throughput numbers are directly comparable.

Commit-only isolation: both commands stream the window through a bounded
[`prefetch`] producer that reads, deserializes, and builds each
`CheckpointVerifiedBlock` (the verifier-side `prepare_block_data`) off the timed
commit thread, into a bounded channel (`ZRB_PREFETCH_CAP`, default 64). So the
timed window measures the committer only, memory stays flat regardless of window
size, and `apply-worker` feeds the worker with a bounded in-flight window (no 30K
backlog, no RocksDB write-stall).

VCT (`--vct-sidecar`) termination: the worker builds its `next_checkpoint` from
the look-ahead, so every committed height needs its successor buffered. The bench
feeds one extra trailing block (the sidecar's `successor`, height `end+1`) so the
last counted block commits; the worker then parks on `end+1` (whose successor is
never fed) and cannot be drained to exit. The run verifies against a cloned DB
handle and returns without joining — the parked worker thread is reaped at process
exit. The legacy path has no such dependency and shuts down cleanly.

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
make perf-replay-worker        # same window, through the write worker
make perf-replay-verifier      # same window, through the checkpoint verifier
make perf-replay-sequencer     # same window, through the block-sync Sequencer (VCT)
# VCT fast path:
make perf-replay-index && deploy/runner/replay_run.sh index-roots
make perf-replay REPLAY_VCT_SIDECAR=/path/to/win.vct
```

Window/snapshot paths come from `deploy/runner/cohort.env` (the `REPLAY_*` vars);
the script falls back to sensible defaults when they're unset.
