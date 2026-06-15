# Pruned Storage Mode — Handoff

> Working doc for whoever continues this line of work. **Not meant to be committed**
> to the PR — delete before opening upstream. The durable design lives in
> `/root/.claude/plans/state-commit-api-serene-snowflake.md`.

## Status: Phase 1 implemented and green

Branch: `worktree-pruned-storage-mode` (git worktree at
`/root/zebra/.claude/worktrees/pruned-storage-mode`). Nothing committed yet.

What works today:
- Opt-in `state.storage_mode = "pruned"` deletes historical **raw transaction
  bytes** (`tx_by_loc`) outside a `tx_retention` window. Default stays `archive`.
- Consensus-critical state and the transaction *location* indexes are retained.
- One-way enforcement: a pruned DB refuses to open in archive mode.
- Tests pass; `cargo build --workspace` passes; clippy/fmt clean for `zebra-state`.

## Storage mode semantics

Current design has two storage modes:

- `archive`: the node is expected to retain and serve all historical raw
  transaction data.
- `pruned`: the node may delete historical raw transaction data below the
  configured retention window.

The transition is intentionally one-way once data has actually been pruned:

- `archive -> archive`: allowed.
- `archive -> pruned`: allowed.
- `pruned -> pruned`: allowed.
- `pruned -> archive`: rejected after pruning has deleted data.

The reason is semantic, not just mechanical: once raw transaction data is missing,
the database can no longer honestly satisfy the archive-mode contract. Reopening
that same DB as archive would make RPC/history behavior look archive-capable even
though older raw transaction bytes may be gone.

This one-way marker is `pruning_metadata[()] = lowest_retained_height`. If the
marker is absent, the DB is treated as not-yet-pruned and can still open as
archive. This covers both regular archive DBs and pruned-configured DBs that have
not reached the retention boundary yet. For compatibility with older DBs, a
missing `pruning_metadata` column family also means "not pruned".

Alternative design worth discussing: a Cosmos-like mode switch could allow
`pruned -> archive` by changing the meaning to "stop pruning from this height
forward." In that model, the DB would remain non-archive for historical heights
below the old pruning boundary, but it would retain all data from the switch-back
height onward. That would need a different user-facing contract than today's
binary `archive`/`pruned` modes, probably including an explicit "archive from
height N" marker and clearer RPC errors for requests below that boundary.

## The one gotcha that matters most

**`tx_loc_by_hash` is consensus-load-bearing — do NOT prune it.**

Spending any UTXO resolves the outpoint through:
`outpoint → transaction_location() [reads tx_loc_by_hash] → OutputLocation → utxo_by_out_loc`.
A UTXO created in an old block can be spent at *any* later height, so deleting
that index by height window breaks validation of those spends. A test caught this
(the UTXO-by-outpoint lookup returned `None` after pruning). The fix was to prune
**only `tx_by_loc`** (the raw bytes — and where ~all the disk savings are). The
indexes `tx_loc_by_hash` and `hash_by_tx_loc` are kept.

If Phase 2 ever wants to prune location indexes, it must only prune entries for
transactions whose outputs are all spent — i.e. track spentness, not height.

## Other gotchas / invariants

1. **Retention floor is load-bearing.** `MIN_PRUNING_RETENTION = 5000` >
   `MAX_BLOCK_REORG_HEIGHT` (1000). The invariant: we only ever delete at
   `tip - retention`, and a rollback only touches the last ≤1000 finalized
   blocks, so pruning can never delete data a rollback reads. Enforced at startup
   in `Config::validate_storage_mode()` (called from
   `FinalizedState::new_with_debug`, before opening RocksDB). Don't lower the
   floor without re-checking the rollback path (`zebra_db/rollback.rs`).

2. **`balance_by_transparent_addr` is cumulative, not per-height.** Never
   height-range-prune it — it would corrupt current balances. (Relevant to a
   future Phase 2 only; Phase 1 doesn't touch any address CF.)

3. **Single-writer invariant.** Pruning rides inside the existing per-block
   `DiskWriteBatch` in `ZebraDb::write_block`, committed atomically with the tip
   advance. Do NOT add a second writer / background prune task — the block write
   task (`service/write.rs`) is deliberately single-writer.

4. **Reads during batch construction see committed data.** `prepare_prune_batch`
   may read the DB (e.g. to enumerate tx hashes) — that's fine because the batch
   isn't written yet. Just don't assume the batch's own deletes are visible to
   reads.

5. **No DB version dir change.** The minor bump 27.0.0 → 27.1.0 stays in the same
   `state/v27/...` directory (path uses major only), so it's an in-place upgrade,
   no re-sync. A `no_migration::NoMigration` entry for `27.1.0` was added to
   `format_upgrades()` — required so the upgrade framework reconciles the version
   cleanly. Bump minor again (and add another `NoMigration`) if you add more CFs.

6. **New CF is created on upgraded writable databases**, archive included (empty
   there). It's created lazily via `create_missing_column_families(true)`. The
   marker (`lowest_retained_height` under unit key `()`) is only *written* once
   pruning actually deletes something — so an archive DB, or a pruned-configured
   DB that hasn't hit the retention height yet, has no marker and can still be
   opened as archive. Old/read-only DB opens may not have the CF yet; treat a
   missing `pruning_metadata` CF the same as a missing marker: not pruned.

7. **Backlog drain is bounded.** `MAX_PRUNE_HEIGHTS_PER_COMMIT = 100`. In steady
   state each commit makes exactly 1 height prunable. The cap only matters when
   switching an existing archive DB to pruned (it drains 100 heights/commit). The
   range math is the pure fn `prune_height_range_inner` (unit-tested).

## File map (what changed)

| File | Change |
| --- | --- |
| `zebra-state/src/config.rs` | `StorageMode`, `PruningConfig`, `storage_mode` field, `pruning_config()`, `validate_storage_mode()` |
| `zebra-state/src/constants.rs` | `MIN_PRUNING_RETENTION`, `MAX_PRUNE_HEIGHTS_PER_COMMIT`, minor version → 1 |
| `zebra-state/src/lib.rs` | re-export `StorageMode`, `PruningConfig` |
| `zebra-state/src/service/finalized_state.rs` | add `pruning_metadata` CF + `PRUNING_METADATA` const; one-way open check + validation in `new_with_debug` |
| `zebra-state/src/service/finalized_state/zebra_db/block.rs` | `lowest_retained_height()`, `is_pruned()`, `prune_height_range()`, `prune_height_range_inner()`, `DiskWriteBatch::prepare_prune_batch()`, prune hook in `write_block()` |
| `zebra-state/src/service/finalized_state/disk_format/upgrade.rs` | `NoMigration` entry for 27.1.0 |
| `.../disk_format/tests/snapshots/*.snap` | added `pruning_metadata` to CF-list + 7 empty-CF snapshots |
| `.../zebra_db/block/tests/prune.rs` (new) + `tests.rs` | the pruning tests |
| `CHANGELOG.md` | `### Added` entry |
| `zebrad/tests/common/configs/v5.0.0-rc.3.toml` (new) | stored config for `last_config_is_stored` (see note below) |

## Known pre-existing failures (NOT caused by this work)

- `zebra-state` lib: `service::tests::chain_tip_sender_is_updated` — a
  non-finalized tip-notification proptest. **Fails on the base branch too.**
- `zebrad` acceptance `last_config_is_stored` was already stale on this fork
  (the fork changed sync defaults — `download_concurrency_limit = 100` etc. — with
  no matching stored config). The new `v5.0.0-rc.3.toml` was generated by `zebrad
  generate` with the cache_dir → `cache_dir` substitution the test applies, which
  should make it pass. Re-verify if you touch any config defaults.

## What's left

### Must do before upstream PR
- **Human discussion / issue first.** Per `CLAUDE.md`, a user-visible feature
  needs maintainer alignment and an AI-use disclosure in the PR body. A pruned
  validator is in-scope, but don't open a cold PR.
- **Run the full gate**: `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace` (note the two pre-existing failures above).
- **Wire `validate_storage_mode` earlier if desired.** It currently runs inside
  `new_with_debug` (panics on bad config). Consider surfacing it as a `Result` at
  zebrad startup for a cleaner error than a panic.

### Verification gaps worth closing
- **Full future-block-validation-after-prune test.** Current tests prove the
  UTXO-by-outpoint path survives (the key consensus read), and that consensus CFs
  are untouched. A stronger proptest would: sync past retention in pruned mode,
  then commit *new* blocks that spend pre-prune-window UTXOs and assert they
  validate. Best added alongside the `PreparedChain` harness in
  `service/finalized_state/tests/`. Note: `MIN_PRUNING_RETENTION = 5000` makes a
  real end-to-end retention test slow — either feature-gate a smaller floor for
  tests, or drive `prepare_prune_batch` directly as the existing tests do.
- **Legacy-chain-check interaction.** `check::legacy_chain` can scan back up to
  `MAX_LEGACY_CHAIN_BLOCKS = 100_000` reading transactions to detect a pre-NU5
  chain. On a real mainnet/testnet node past NU5 it returns within a few blocks of
  the tip, so retention (≥5000) covers it. But confirm it never reads raw txs
  below the retained window on a pruned node. Low risk, worth a check.
- **`block()` / `getblock(verbose)` for pruned heights.** Returns partial/None
  gracefully today. Confirm no *consensus* path calls `block()` for heights below
  the window (reorg is bounded to 1000 << retention, so it shouldn't).

### Phase 2 (designed, not built)
- Prune transparent address *location* indexes
  (`tx_loc_by_transparent_addr_loc`, `utxo_loc_by_transparent_addr_loc`,
  `tx_loc_by_spent_out_loc`) by height. Keep `balance_by_transparent_addr` whole
  (gotcha #2). This degrades `getaddresstxids` for old ranges — return a clear
  "range includes pruned heights" error rather than a silently-truncated list.

### RPC polish (intentionally descoped in Phase 1)
- `get_raw_transaction` already degrades gracefully (returns the existing
  not-found error) for pruned txs. A "below retention window" message would need
  a new `ReadRequest` exposing `lowest_retained_height` to the RPC layer. Note a
  per-txid "pruned vs never-existed" distinction is impossible once the data is
  gone — only a node-level "this node is pruned, lowest retained height = N" hint
  is achievable.

### Alignment with the future commit-API abstraction
The design doc envisions a private `CommitPlan { consensus_writes, history_writes,
index_writes, prune_deletes, progress_marker, durability }` + `CommitSequencer`.
Phase 1 is a faithful subset: `prepare_prune_batch` → `prune_deletes`,
`pruning_metadata` → a progress marker, `write_block` → proto-sequencer. Two
forward-compat requirements already honored:
1. `prepare_prune_batch` / `prune_height_range` are **range-capable** (take a
   `[from, until)` height range), so batched commits that advance the tip by N
   just prune N heights — no rewrite.
2. The prune step is its own named function, so it lifts cleanly into
   `CommitPlan.prune_deletes`.

## Quick verification commands

```bash
cd /root/zebra/.claude/worktrees/pruned-storage-mode
cargo test -p zebra-state --lib prune          # the pruning tests (6)
cargo test -p zebra-state --lib                 # full state lib (1 pre-existing fail)
cargo clippy -p zebra-state --all-targets -- -D warnings
cargo fmt -p zebra-state -- --check
cargo build --workspace
```
