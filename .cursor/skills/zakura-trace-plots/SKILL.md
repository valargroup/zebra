---
name: zakura-trace-plots
description: Generate metrics-aware plots and summaries from Zebra Zakura perf trace directories. Use when the user asks to plot or analyze Zakura traces, block_sync.jsonl, commit_state.jsonl, feedrun CSVs, applying/reorder/stalls, HoL stalls, throughput, or commit metrics from a trace_dir.
---

# Zakura Trace Plots

## Quick Start

When the user gives a Zakura `trace_dir`, generate plots with:

```bash
python3 .cursor/skills/zakura-trace-plots/scripts/plot_zakura_traces.py TRACE_DIR --out-dir perf-artifacts
```

If the matching feed-run CSV is not auto-detected, pass it explicitly:

```bash
python3 .cursor/skills/zakura-trace-plots/scripts/plot_zakura_traces.py TRACE_DIR --csv /root/wal-bench/feedrun-r1.csv --out-dir perf-artifacts
```

## What To Plot

Default outputs:

- `*-time-apply-reorder-stalls-bps.svg`
- `*-height-apply-reorder-stalls-bps.svg`
- `*-summary.txt`

Use the time plot for stall diagnosis. Height plots collapse zero-progress stalls onto one x-position.

When the user is analyzing the verify -> commit pipeline and provides per-block
timing traces such as `seq50k-commit-timing.jsonl` and
`seq50k-verify-timing.jsonl`, also generate the by-height timing plot:

```bash
python3 .cursor/skills/zakura-trace-plots/scripts/plot_commit_verify_timing.py \
  COMMIT_TIMING.jsonl VERIFY_TIMING.jsonl --out-dir perf-artifacts
```

This graph is useful for separating verifier cost from committer cost. It plots
commit phases (`spent_utxo_reads`, `address_reads`, `batch_assembly`,
`batch_commit`), commit total latency, verifier phases (`pow`, `precompute`,
`merkle`), and rolling implied throughput from each timing stream.

## Metrics Awareness

Use `block_sync.jsonl` as the source of truth for:

- `applying`
- `reorder`
- `unsubmitted_applying_count`
- `submitted_applies`
- byte counters like `applying_buffered_bytes` and `retained_pipeline_wire_bytes`
- floor-gap states such as `outstanding`, `queued`, and `in_flight_without_outstanding`

Use the CSV only for sampled node metrics:

- `blk_s`, recomputed from finalized height deltas while skipping startup `0 -> snapshot` jumps
- commit phase counters like `sur`, `ar`, `bp`, `bc`
- `cpu_cores`, peers, and other sampler-only columns

Do not trust the CSV `reorder` column unless verified against `block_sync.jsonl`; in prior runs it was effectively zero while trace reorder was thousands.

## Interpreting Output

HoL/body-floor stall signature:

- `applying` collapses near zero
- `reorder` grows
- `blk_s` drops near zero
- trace floor-gap state is usually `outstanding`

Commit/memory-pressure signature:

- `applying` and submitted applies stay high
- `reorder` stays low
- `spent_utxo_reads` or other commit phases spike
- host memory pressure or retained pipeline bytes rise

When summarizing, report the generated file paths and the strongest signature, not every metric.
