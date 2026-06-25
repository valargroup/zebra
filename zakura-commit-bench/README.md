# zakura-commit-bench

A standalone benchmark for the block-commit stack **below** Zakura block-sync:
the **real** checkpoint verifier (`zebra_consensus::CheckpointVerifier` with the
embedded mainnet checkpoint list) committing **real** mainnet blocks into a
**real** ephemeral finalized state (`zebra_state::init`), driven at the
production apply concurrency (`MAX_CHECKPOINT_HEIGHT_GAP + 1 = 401`).

No networking, no reactor, no mocks on the execution path — so checkpoint
verification + state commit can be measured and profiled in isolation. This is
the layer the "Coalesce Checkpoint Frontier Refreshes" work targets.

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
the range above the tip *with roots* and run hydrated:

```bash
cargo xtask zakura-commit-bench -- fetch --rpc-url http://165.22.54.66:8232 \
  --start 1707211 --end 1708000 --with-roots
cargo xtask zakura-commit-bench -- run \
  --state-dir ~/.zakura/snapshots/<name> --blocks 800 --with-roots \
  --trace-dir target/zakura-commit-bench/traces
```

The summary's `VCT fast path: N hit / M miss` line **verifies** the roots
engaged (it should be ~all hits); `0 hit` with `--with-roots` means the roots are
wrong or out of range.

## 1. Fetch real blocks (once, cached)

Point it at any synced node's JSON-RPC (`getblock <height> 0`):

```bash
cargo xtask zakura-commit-bench -- fetch \
  --rpc-url http://127.0.0.1:8232 \
  --start 0 --end 19999 \
  --cache-dir target/zakura-commit-bench/blocks
# zcashd auth: add --rpc-user <user> --rpc-password <pass>
```

Blocks are validated and cached one file per height; re-runs skip what's cached.

## 2. Run the benchmark

```bash
cargo xtask zakura-commit-bench -- run \
  --blocks 20000 \
  --concurrency 401 \
  --frontier-read coalesced \
  --trace-dir target/zakura-commit-bench/traces
```

Prints throughput (blk/s, MiB/s), commit-latency percentiles, and frontier-read
latency. With `--trace-dir`, emits `block_sync.jsonl` `block_commit_progress`
rollups the existing analysis harness can read.

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

## 3. Profiling

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
- Mainnet only for now (the checkpoint list is per-network).
- The harness's core (real blocks → real checkpoint verifier → real finalized
  state) is covered offline by the `drives_real_blocks_through_real_checkpoint_verifier`
  test using bundled block vectors.
