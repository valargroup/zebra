# Changelog: Parameters

A focused ledger of deliberate changes to **tunable parameters** in this fork —
constants, config defaults, timeouts, window/limit sizes, and congestion-control
coefficients.

This complements `CHANGELOG.md`. The changelog records user-visible behavior in
prose; this file is a compact table of every parameter value we have re-tuned, so
reviewers and operators can see — at a glance — what changed, where it lives, and
why.

## How to use this file

When a PR changes a tunable parameter, add a row to the table below **in the same
PR**. A "tunable parameter" is any value chosen for behavior or performance rather
than correctness — a constant, a `Config` default, a timeout, a window or limit,
or a backoff/growth coefficient.

Keep entries **newest-first**. Each row records:

- **Parameter** — the constant or config field name.
- **Location** — the file where it is defined (crate-relative path).
- **Old → New** — the previous value and the new value.
- **PR** — a link to the pull request that made the change.
- **Why** — a one-line rationale.

## Parameters

| Parameter | Location | Old → New | PR | Why |
| --- | --- | --- | --- | --- |
| `Config::finalized_block_pipeline_depth` | `zebra-state/src/config.rs` | _(new)_ → `0` | _(this PR)_ | Run-ahead finalized-commit pipeline depth (blocks the assembler may build ahead of the durable disk write). Defaults to `0` = synchronous (original behavior); `> 0` overlaps the next block's assembly with the current block's flush, bounded so the in-memory overlay stays small. |
| `OUTBOUND_WINDOW_FLOOR_TIMEOUTS_BEFORE_DISCONNECT` | `zebra-network/src/zakura/block_sync/state.rs` | `3` → `2 * OUTBOUND_WINDOW_REDUCTION_EPOCH_TIMEOUTS` (`32`) | [#303](https://github.com/valargroup/zebra/pull/303) | Tolerate two full reduction epochs (~256s at the 8s request timeout) of floor-pinned timeouts before disconnecting a block-sync peer, instead of ~24s, so briefly-congested peers are not churned. Any successful response resets the streak. |
