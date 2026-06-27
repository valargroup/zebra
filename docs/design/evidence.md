# Committer throughput triage — evidence

This document points reviewers at the captured trace evidence behind the
[block-sync stall attribution](./block-sync-stall-attribution.md) design and the
committer-throughput findings. It pairs the Stage-A `sync.pipeline.limiter`
attribution (which names the limiting stage) with a real captured genesis-sync
run so the verdict can be checked against raw structured traces.

## Trace archive

A full structured-trace capture of a public-mainnet genesis sync is archived
out-of-band (it is large JSONL, not committed to the repo):

```
/root/genesis-sync-traces-20260626.tar.zst
SHA256: e4174726892ec9d88c7f96048d03c24d2f55faadc61fef56fc1ca173d0c08205
```

Contents:

```
genesis-sync-traces/*.jsonl   # the structured Zakura JSONL trace tables (trace_dir output)
genesis-sync.log              # the text log for the run
genesis-sync.toml             # the exact config used (public Zakura v2, 7 peers, from genesis)
```

Extract:

```sh
tar --zstd -xf genesis-sync-traces-20260626.tar.zst
```

The traces are produced by setting `[network.zakura] trace_dir = "..."`, which
spawns the JSONL tracer that writes the production trace schema
(`block_sync`, `header_sync`, `commit_state` tables).

## Useful searches

```sh
# block-sync state snapshots (carry the limiter verdict + floor-gap state)
rg '"event":"block_sync_state"' genesis-sync-traces/block_sync.jsonl

# the submit-throttle / commit-progress signals (commit pipeline backpressure)
rg '"event":"block_body_submit_throttled"|"event":"block_commit_progress"' genesis-sync-traces/block_sync.jsonl

# header sync running far ahead of bodies
rg '"event":"header_frontier_advanced"|"event":"header_missing_bodies_reported"' genesis-sync-traces/header_sync.jsonl

# the commit span itself (start/finish/stall)
rg '"event":"commit_start"|"event":"commit_finish"|"event":"commit_stalled"' genesis-sync-traces/commit_state.jsonl

# the limiter verdict + floor-gap state directly
rg '"floor_gap_state":"outstanding"|"limiter":"download_gap"|"limiter":"submit_throttled"' genesis-sync-traces/block_sync.jsonl
```

## Key evidence to point reviewers at

The capture shows the download/header side running well ahead of the commit
side, with the apply/commit pipeline backed up — i.e. the limiter is the
committer/apply stage, not download:

- **Headers far ahead of `verified_block_tip`** — header sync has raced ahead while
  the verified/committed tip lags, so the node is not header-bound.
- **Huge `header_missing_bodies_reported` ranges** — large spans of headers whose
  bodies are not yet committed: the gap is downstream of header sync.
- **`checkpoint_in_flight = 800`** — the apply pipeline pinned at its concurrency cap;
  the committer cannot drain it faster.
- **Large `unsubmitted_applying_count`** — many bodies downloaded and ordered but not
  yet submitted to apply, because the submit window is saturated (`SubmitThrottled`).
- **Near-frontier `floor_gap_state`** — the download floor sits right at the frontier;
  download is not starving the head-of-line, the backlog is on the commit side.

## How this ties to the attribution

This is the raw-trace counterpart to the Stage-A summary verdict. On the
deterministic 1.8M cohort the same picture is the `sync.pipeline.limiter`
distribution **~78% `SubmitThrottled`** with the commit-await/db split showing
`db_seconds` p50 (~286ms, single-writer queue) ≫ `await_seconds` p50 (~63ms,
layer-2 park) — i.e. the commit/apply cadence at the single writer is the binding
constraint, while download and the layer-2 checkpoint reorder are not. See the
design doc §2 (the verdict model) and §4 (why layer-2 thinning is a
simplicity/debuggability change, not a throughput lever) for the full reasoning.
