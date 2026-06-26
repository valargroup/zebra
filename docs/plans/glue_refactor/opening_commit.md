# Review Split Plan

## Summary

Build a four-PR stack on `origin/perf-note-commit-tree`, using patch and hunk
selection rather than preserving the current commit history. First quarantine the
dirty worktree with an explicit stash or safety branch, then reconstruct review
branches so each PR has one review theme and the final branch matches current
behavior plus the upcoming sequencer refactor.

Stack:

1. `review/headersync-roots`
2. `review/blocksync-pre-apply`
3. `review/zakura-commit-bench-support`
4. `review/blocksync-sequencer-apply-refactor`

## Key Changes

### PR 1: remaining headersync roots

- Opened as draft PR #282, `review/headersync-roots` targeting
  `perf-note-commit-tree`.
- Preserve current policy: non-empty ranged header commits require complete,
  height-aligned `tree_aux_roots`.
- Include header-sync wire/request plumbing, root validation, root persistence in
  `CommitHeaderRange`, root-serving/readback paths, best-header/root-coverage
  alignment, and header-sync driver wiring.
- Include focused state/header-sync tests.
- Match the original branch's `zebra-network/src/zakura/header_sync/*` code.
  Also include two small local header-serving fixes from the original branch:
  the `QueryBestHeaderTip` trace timer used by the root-coverage error path, and
  the `body_sizes_for_served_header_range()` guard for served heights below the
  requested start height.
- Communicate clearly to reviewers that this PR is the roots/header-serving
  correctness slice. It intentionally moves only the header-sync module, the
  state API/storage needed to commit/read roots, the header-sync driver glue, and
  the small body-size alignment guard above.
- Do not include generated `.snap.new` artifacts.
- Exclude blocksync scheduling/apply changes except unavoidable interface
  notifications from header sync to block sync.
- Exclude the body-size hint policy/persistence changes from
  `fix(network): pack block sync ranges by size hint`; those belong with
  blocksync range packing in PR 2.

### PR 2: blocksync before apply-queue/refactor

- Build on top of PR #282 / `review/headersync-roots`, not directly on
  `origin/perf-note-commit-tree`. Suggested branch name:
  `review/blocksync-pre-apply`; target the PR at `review/headersync-roots` while
  the stack is open.
- Include download-side and scheduler work: range packing by size hint, request
  bookkeeping reductions, bitmap received tracking, raw body retention where it is
  still independent, congestion/admission/work-queue/budget fixes, peer
  routine/reactor glue, and transport budget accounting.
- Include state/read behavior that directly supports range packing by size hint,
  including advertised body-size persistence policy and `BlockSizeHints` test/doc
  updates left out of PR 1.
- Keep the existing external apply/commit driver shape as much as possible.
- Explicitly exclude `BlockApplyExecutor`, `BlockApplyExecutorPort`,
  `ZebradBlockApplyExecutor`, local `FuturesUnordered` apply completions in
  `SequencerTask`, removal of `SubmitBlock`/`BlockApplyFinished`, checkpoint
  frontier refresh coalescing, and apply-throughput rollup glue.
- Also exclude `zebra-network/src/zakura/block_sync/apply_item.rs` and
  `zebrad/src/commands/start/zakura/committer.rs`; those are PR 4 apply-queue
  refactor artifacts.
- Do not re-touch `zebra-network/src/zakura/header_sync/*` or
  `zebrad/src/commands/start/zakura/header_sync_driver.rs`; PR #282 now matches
  the original branch for those paths.

#### PR 2 candidate commits / hunks

Use patch-equivalent reconstruction, not history preservation. These commits are
good source material, but several need hunk filtering:

- `9a0237916 fix(network): pack block sync ranges by size hint`
  - Include block-sync work queue/peer-routine/test changes.
  - Include `zebra-state` body-size hint policy/persistence and
    `BlockSizeHints` doc/test updates.
  - Exclude the `body_sizes_for_served_header_range()` pre-start guard and its
    test if it appears in the source patch; PR #282 already owns it.
- `35bef6fc8 fix: tune params for high throughput`
  - Include config/handler/test tuning if still desired.
- `c67fe36a0 perf(block-sync): reduce request bookkeeping allocations`
  - Include request/state/reactor/peer-routine/test allocation reductions.
- `a1819e2df perf(block-sync): track received range blocks with bitmap`
  - Include bitmap received tracking in request/state/tests.
- `39ad3edab perf(block-sync): retain raw bodies in reorder backlog`
  - Include raw-body retention only while it still fits the existing
    external apply/commit driver shape. If a hunk depends on the later
    sequencer-owned apply path, defer that hunk to PR 4.
- `516973f69 fix(network): tune combined perf block sync defaults`
  - Include only config/default/test changes that still make sense before the
    apply refactor.
- `87bfeb9fb fix: always pop from the highest hight in the buffer when resueing`
  - Include reorder/backlog selection fixes if they do not require PR 4 apply
    ownership. Keep the typo in commit subject out of reviewer-facing text.
- `3d8d7c00d fix: congestion control`
  - Include admission, peer-registry, work-queue, budget, transport guard, and
    peer-routine/reactor scheduling changes.
  - Hunk-select carefully around `sequencer_task.rs`; only keep pre-apply
    scheduling/congestion behavior, not apply orchestration.

Treat these as mostly PR 4 source material unless a tiny independent bugfix is
needed to keep PR 2 compiling:

- `6ab1d1e3d fix!: sequencer`
- `c89b017b7 perf(network): coalesce checkpoint frontier refreshes and skip per-block frontier reads`
- `b085d9d88 fix(network): bound Zakura apply-window drain (#255)` and
  `4a15bdb1d fix: revert ...`
- `4ad73bad7 chore: additional traces and metrics around applying`
- `45c9e9dd9 feat: phase 1 of refactorc`

#### PR 2 reviewer framing

Explain that PR 2 is the download/scheduling throughput slice after headers are
root-covered:

- It changes how missing bodies are grouped, admitted, tracked, and budgeted.
- It keeps block application outside the block-sync reactor/sequencer for now.
- It deliberately leaves the later apply queue/committer ownership change for PR
  4 so reviewers can evaluate scheduling pressure separately from commit
  orchestration.
- It may touch `zebrad` block-sync driver glue only to preserve the current
  `SubmitBlock`/`BlockApplyFinished` shape while exposing the new scheduling
  reads/events.

#### PR 2 reconstruction checklist

1. Start from a clean worktree at `review/headersync-roots` after PR #282's
   latest force-pushed head. Do not build PR 2 from the current dirty
   `evan/perf-plus-download-fixes` checkout.
2. Create `review/blocksync-pre-apply` from `review/headersync-roots`.
3. Apply candidate commits/hunks from `evan/perf-plus-download-fixes` by theme.
   Prefer explicit path/hunk selection over `git cherry-pick` when a commit also
   contains apply/refactor work.
4. Expected PR 2 paths are mostly:
   - `zebra-network/src/zakura/block_sync/{admission,config,peer_registry,peer_routine,reactor,reorder,request,sequencer,sequencer_task,service,state,tests,wire,work_queue}.rs`
   - `zebra-network/src/zakura/handler.rs`
   - `zebra-network/src/zakura/transport/guard.rs`
   - `zebra-network/src/zakura/trace.rs`
   - `zebra-state/src/request.rs`
   - `zebra-state/src/service/finalized_state/zebra_db/block.rs`
   - `zebra-state/src/service/tests.rs`
   - `zebrad/src/commands/start.rs`
   - `zebrad/src/commands/start/zakura/block_sync_driver.rs`
5. Suspicious in PR 2 unless deliberately justified:
   - `zebra-network/src/zakura/header_sync/*`
   - `zebrad/src/commands/start/zakura/header_sync_driver.rs`
   - `zebra-network/src/zakura/block_sync/apply_item.rs`
   - `zebrad/src/commands/start/zakura/committer.rs`
   - `zakura-commit-bench/**`
   - packaging/xtask/Docker changes
   - broad `zebrad/src/commands/start.rs` test rewrites that remove the current
     external apply/commit driver shape.
6. After assembling, compare against the original branch:
   - PR 2 should explain every remaining diff in block-sync/state/zebrad driver
     paths as either intentionally included now or deferred to PR 4.
   - The remaining original-branch diff after PR 2 should be dominated by
     commit-bench/tooling and sequencer-owned apply/refactor work.

### PR 3: commit-bench and support tooling

- Add `zakura-commit-bench`, workspace/Cargo wiring, packaging/xtask/Docker
  context changes, and related benchmark docs/config.
- Classify quarantined `.dockerignore` and `zakura-commit-bench/src/run.rs`
  edits here if still desired.
- Keep unrelated local edits out unless they directly support this tool.

### PR 4: sequencer-owned apply/refactor

- Move apply orchestration behind the dependency-neutral blocksync executor
  interface.
- Install `ZebradBlockApplyExecutor` from zebrad, move apply completion handling
  into `SequencerTask`, coalesce checkpoint frontier refreshes, emit apply/commit
  progress traces, and slim the blocksync action driver.
- Include the later simplification/refactor work that replaces today's
  apply-queue-to-commit glue.
- Revisit local dirty `sequencer_task` refresh interval edits here.

## Dirty Worktree Handling

- Before branch surgery, save all current uncommitted and untracked files
  separately.
- After the main stack is reconstructed:
  - `.dockerignore` and commit-bench `run.rs` changes go to PR 3 if intentional.
  - `zebra-network/src/zakura/block_sync/*` dirty edits go to PR 4 if they match
    the refactor.
  - `zebra-state/src/config.rs` RocksDB tuning should become a separate
    storage/perf PR unless needed by this stack.
  - `zebrad/src/components/mempool/downloads.rs` should become a separate bugfix
    PR, not part of this review stack.
  - Untracked planning docs stay out of review branches unless deliberately added
    as design docs.

## Test Plan

- Every PR: `cargo fmt --all -- --check`, `cargo check` or `cargo build` for
  touched crates, and `git diff --check`.
- PR 1: focused `zebra-network` header-sync tests plus `zebra-state` tests around
  `CommitHeaderRange`, root count/height validation, header root
  storage/readback, and best-header root coverage. Current draft evidence:
  `git diff --check`, `cargo fmt --all -- --check`, and
  `cargo test -p zebra-network zakura::header_sync --lib` pass. Local
  `zebra-state`/`zebrad` focused tests are blocked before reaching Zebra tests by
  bundled `librocksdb-sys` failing to compile RocksDB C++ headers that reference
  `uint64_t` without `<cstdint>`.
- PR 2: focused `cargo test -p zebra-network zakura::block_sync --lib`,
  especially scheduling, admission, byte-budget, work-queue, peer-routine,
  transport-guard, and reactor tests that do not depend on sequencer-owned
  apply. Also run `cargo check -p zebra-network -p zebrad` if the local
  RocksDB/librocksdb-sys toolchain allows it; if it fails before Zebra code with
  the known `<cstdint>`/`uint64_t` RocksDB header issue, document that blocker in
  the PR and rely on CI for the broader compile signal.
- PR 3: `cargo check -p zakura-commit-bench -p xtask -p zebrad`; run bench CLI
  help/smoke tests without requiring network downloads.
- PR 4: focused blocksync sequencer/apply tests plus
  `cargo check -p zebra-network -p zebrad`; broad workspace tests are best-effort
  and any known failures should be documented in PR notes.

## Assumptions

- Base is `origin/perf-note-commit-tree`.
- Git history does not need preservation; patch-equivalent branch reconstruction
  is preferred.
- Roots stay required for ranged header commits in this split.
- Intermediate broad test failures are acceptable if each PR compiles and has
  focused evidence for its changed behavior.
- Final stacked endstate should match the current branch's intended behavior plus
  the new sequencer/apply refactor, with unrelated local edits split away.
