# zebra-replay-bench Results

This split PR adds the replay tooling to `ironwood-main`; it does not include a
fresh benchmark run on the final split branch.

The supported rungs on this branch are:

- `apply`: direct finalized-state commit replay.
- `apply-worker`: replay through the production write worker.
- `apply-verifier`: replay through the checkpoint verifier and state service.
- `apply-sequencer`: replay through the Zakura block-sync sequencer, checkpoint
  verifier, and state service.

VCT sidecar replay is intentionally not measured here. The sidecar writer remains
available for later fast-sync benchmark branches, but `--vct-sidecar` replay
exits with an unsupported-mode error on this base because the header-root
fast-path APIs are not present on `ironwood-main`.

## Recording A Run

After building with commit metrics:

```bash
make perf-build-replay-bench
make perf-replay-index
make perf-replay
make perf-replay-worker
make perf-replay-verifier
make perf-replay-sequencer
```

Record at least:

- branch and commit under test,
- snapshot source and base paths,
- replay window,
- storage mode for `apply-sequencer`,
- `ZRB_PREFETCH_CAP` if overridden,
- wall time, throughput, latency percentiles, and peak RSS,
- any emitted `zebra_state.*` commit-metric summaries.
