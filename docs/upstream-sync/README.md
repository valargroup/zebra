# Upstream Triage

This directory tracks upstream Zebra pull requests that the Valar fork has
evaluated for `ironwood-main`.

The automation is intentionally conservative:

- it is manual-only in v1;
- it processes exactly one upstream PR per run;
- it triages before attempting any import;
- Codex runs without GitHub write permissions;
- only important production bug fixes should open draft PRs;
- closed generated PRs are treated as intentional human skips on later runs; and
- reverting the implementation PR removes the automation.

Most upstream PRs should be skipped. Features, test-only fixes, docs, CI,
formatting, release metadata, and routine refactors do not meet the import bar
unless they carry an important production bug fix for this fork.

Non-import triage decisions are recorded on the `upstream-sync/state` branch in
`.github/upstream-sync/triage-ledger.jsonl`. This avoids churn on
`ironwood-main` just to remember skipped upstream PRs.

## Statuses

- `pending`: discovered but not evaluated.
- `candidate`: selected for a manual workflow run.
- `imported`: behavior was brought into the fork. Record the downstream PR or
  commit.
- `skipped`: intentionally not relevant to this fork.
- `already_present`: already covered by the fork.
- `superseded`: already covered by a fork-specific change. Record evidence.
- `blocked`: relevant, but needs human conflict resolution or a broader design
  decision.

## Pilot

The first missing upstream PR is defined by upstream history, not by the lowest
PR number or easiest patch. At the time this automation was added, that first
candidate was upstream PR 10676.
