# zakura-commit-bench

A standalone benchmark for cached-block replay below Zakura P2P.

`direct-verifier` is the lower-level diagnostic mode: cached blocks go straight
into the real checkpoint verifier (`zebra_consensus::CheckpointVerifier`) and
real state.

`apply-queue` is the production-faithful mode: cached blocks and roots are
preloaded into a hydrated snapshot, then synthetic stream-6 peers answer real
`GetBlocks` with cached block frames. The node side runs the real block-sync
reactor, per-peer routine receive path, WorkQueue, ByteBudget, Sequencer, applyQ,
`zebrad` Committer, consensus `Request::Commit`, and durable frontier feedback.

For meaningful post-NU5 runs, use `--with-roots`. The benchmark fetches roots
from `z_gettreestate`, then preloads them through the production
`CommitHeaderRange` state path before replaying bodies.

## The two ways to run

- **From genesis (cheap, pre-Sapling):** a fresh ephemeral DB, commit `0..N`.
  Fast to set up, but the blocks are tiny and the note-commitment trees are
  empty, so it does **not** exercise the real bottleneck.
- **From a snapshot (the real bottleneck):** hydrate a post-NU5 state snapshot
  and commit a range *above* its tip — sandblasting-era blocks with large trees,
  where the VCT fast path (header-carried roots) actually matters. The
  `CheckpointVerifier` commits from genesis on an empty DB, so a snapshot is the
  only way to reach a high range without replaying ~1.7M blocks.

## 0. (Post-NU5) Download a state snapshot

```bash
cargo xtask zakura-commit-bench -- snapshot   # → ~/.zakura/snapshots/<name>, prints --state-dir
```

Downloads (resumable; `aria2c` if present, else `curl -C -`), checksum-verifies,
and extracts the default pruned mainnet snapshot at height 1,707,210. Then fetch
the range above the tip with roots and run hydrated:

```bash
cargo xtask zakura-commit-bench -- fetch --rpc-url http://143.244.184.176:8232 \
  --start 1707211 --end 1708000 \
  --cache-dir target/zakura-commit-bench/blocks \
  --with-roots
cargo xtask zakura-commit-bench -- validate-cache \
  --start 1707211 --end 1708000 \
  --cache-dir target/zakura-commit-bench/blocks \
  --with-roots
cargo xtask zakura-commit-bench -- run \
  --mode apply-queue \
  --state-dir ~/.zakura/snapshots/<name> --blocks 800 \
  --cache-dir target/zakura-commit-bench/blocks \
  --with-roots \
  --disk-peers 1 \
  --trace-dir target/zakura-commit-bench/traces
```

`--with-roots` requires one `*.roots.json` file per replayed block and currently
requires `--state-dir`, because the header-root preload anchors above the
snapshot finalized tip. The summary's `VCT fast path: N hit / M miss` line
verifies whether the roots engaged.

## 1. Fetch real blocks (once, cached)

Point it at any synced node's JSON-RPC (`getblock <height> 0`). Recent probes
from this checkout found these unauthenticated synced endpoints:

- `http://143.244.184.176:8232` (`us-west-0`, fastest local probe, ~85 ms)
- `http://159.65.183.89:8232` (`us-east-0`, ~125 ms)
- `http://165.22.54.66:8232` (`asia-0`, ~425 ms)

Avoid `http://104.131.184.123:8232` for high-range fetches; it answered quickly
but was pruned and far behind the current tip during the probe.

```bash
cargo xtask zakura-commit-bench -- fetch \
  --rpc-url http://143.244.184.176:8232 \
  --start 0 --end 19999 \
  --cache-dir target/zakura-commit-bench/blocks
# zcashd auth: add --rpc-user <user> --rpc-password <pass>
```

Blocks are validated and cached one file per height; roots are cached as
`*.roots.json` when `--with-roots` is set. Use `--with-roots` for the hydrated
post-NU5 range, not for the genesis smoke. Re-runs skip what's cached.

## 2. Validate the cache

Validate cached artifacts before a run:

```bash
cargo xtask zakura-commit-bench -- validate-cache \
  --start 0 \
  --blocks 401 \
  --cache-dir target/zakura-commit-bench/blocks
```

This checks that every expected file exists, deserializes as a Zcash block, has
the expected coinbase height, and links to the previous cached block inside the
validated range. It prints total bytes and first/last hashes.

For hydrated runs, include `--with-roots` so the validator also checks every
`*.roots.json` sidecar exists and contains 32-byte Sapling/Orchard root hex.

## 3. Run the benchmark

```bash
cargo xtask zakura-commit-bench -- run \
  --mode direct-verifier \
  --blocks 20000 \
  --concurrency 401 \
  --frontier-read coalesced \
  --trace-dir target/zakura-commit-bench/traces
```

`direct-verifier` prints throughput (blk/s, MiB/s), commit-latency percentiles,
and frontier-read latency. `apply-queue` prints end-to-end throughput for the
block-sync/applyQ/Committer path. With `--trace-dir`, both modes emit
`block_sync.jsonl`; `apply-queue` also includes the production block-sync,
sequencer, committer, and commit-state trace rows.

Shortest useful smoke test, assuming the local `0..=400` cache exists:

```bash
cargo xtask zakura-commit-bench -- run \
  --mode direct-verifier \
  --blocks 401 \
  --cache-dir target/zakura-commit-bench/blocks \
  --trace-dir target/zakura-commit-bench/traces-smoke
```

Production-faithful hydrated replay:

```bash
cargo xtask zakura-commit-bench -- run \
  --mode apply-queue \
  --state-dir ~/.zakura/snapshots/<name> \
  --blocks 800 \
  --cache-dir target/zakura-commit-bench/blocks \
  --with-roots \
  --disk-peers 1
```

### `--frontier-read` is the A/B knob for the coalesce change

The post-commit durable frontier read is what held the 401 apply slot. This flag
places it exactly where each design does, so you can A/B on the real stack:

| value        | models                                  | slot held for      |
| ------------ | --------------------------------------- | ------------------ |
| `per-block`  | pre-coalesce baseline                   | verify+commit+read |
| `coalesced`  | the shipped coalesce (read off-path 5s) | verify+commit      |
| `none`       | optimistic-verified-tip ideal           | verify+commit      |

```bash
# A/B the change directly:
cargo xtask zakura-commit-bench -- run --frontier-read per-block
cargo xtask zakura-commit-bench -- run --frontier-read coalesced
```

## 4. Profiling

Reuses Zebra's existing profiling infrastructure (no pprof needed):

**CPU (samply / perf)** — builds with the `profiling` Cargo profile +
`-C force-frame-pointers=yes` (captures Rust *and* rocksdb C++ frames) and runs
under `samply` if installed:

```bash
cargo xtask zakura-commit-bench --profile -- run --blocks 20000
```

**Heap (jemalloc)** — mirrors zebrad's `jemalloc-profiling`; writes heap dumps
under `/tmp/zakura-bench-jeprof/`:

```bash
cargo xtask zakura-commit-bench --jemalloc -- run --blocks 20000
jeprof --show_bytes ./target/release/zakura-commit-bench /tmp/zakura-bench-jeprof/bench.*.heap
```

**Per-phase commit timing** — build with `--features commit-metrics` to emit the
`zebra.state.write.*` histograms (needs a metrics recorder to scrape).

## Notes

- `--concurrency` must be ≥ the checkpoint spacing (≤ 400 on mainnet) or the
  verifier can't complete a checkpoint range; default 401 matches production.
- State is ephemeral (fresh rocksdb per run) for reproducible cold-cache numbers.
  `--with-roots` is the exception: it requires `--state-dir` so header roots can
  be preloaded above the snapshot tip.
- Mainnet only for now (the checkpoint list is per-network).
- The harness's core (real blocks → real checkpoint verifier → real finalized
  state) is covered offline by the `drives_real_blocks_through_real_checkpoint_verifier`
  test using bundled block vectors.
