# Zakura reorg stack — engineering handoff

Date: 2026-07-07. Branch context: `zakura/writeguards-canary-v3` (e7bdbb4fc),
which is `ironwood-main` (already containing the merged PRs below) + the two
open PRs (#495, #496). `REORG_PLAN.md` (committed alongside this file) is the
original first-principles design; this document is the state of the world, the
live incident, the test evidence, and where to pick up.

## 1. The problem in one paragraph

The zakura header store is a height-indexed set of five RocksDB column
families written by multiple writers (header-range sync, the full-block "seed"
path, the release path) with historically no enforced invariants. Two fleet
wedge classes traced back to this: stale/unlinked rows poisoning the
difficulty-validation window (`InvalidDifficultyThreshold` from every peer),
and store/frontier divergence wedging sync. `REORG_PLAN.md` §1–§3 defines the
invariants (bijection, linkage, tip integrity), a model-checked test harness,
and a pillar-by-pillar fix plan. The plan has been executed through Pillar 1a;
the remaining work is listed in §6 below.

## 2. What has landed (merged into `ironwood-main`)

- **#490 — coherence harness** (`test(state): add zakura header-store
  coherence harness with corruption repros`). Oracle + op alphabet + audit +
  scripted scenarios + discovery proptest at
  `zebra-state/src/service/finalized_state/zebra_db/block/tests/header_store_coherence/`
  (see its README). Proved three corruption bug classes deterministically.
  Harness facts that constrain new tests (whole-second header times, DAA
  drift needs 26–30 headers, bodies commit sequentially) are recorded in
  `REORG_PLAN.md` §1b.
- **#491 — write guards** (`fix(state): validate linkage in zakura
  header-store writers`). Closed the three proven bug classes: anchor +
  intra-range linkage checks in the range writer (`UnlinkedRange`, classified
  local/non-scoring), no re-insertion over committed heights
  (`contains_height`, pruning-safe), and the seed path refuses
  non-parent-linked writes as silent no-ops. **That last guard's deferred
  heal ("header-range sync will later deliver the missing rows") is the root
  of the testnet-4 incident — see §4.**
- **#493 — verified reads** (`fix(state): verify zakura header-store
  invariants in consensus reads`). `recent_header_context` self-verifies its
  walk (hashing, linkage, bijection round-trips); failures surface as
  `StoreIncoherentError` (local, non-scoring) instead of silently validating
  against corrupt context.
- **#497 — startup audit + repair** (re-land of closed #494). Full-frontier
  audit at `ZebraDb` open; incoherent suffixes / stale committed-height rows
  deleted in one atomic batch; `state.zakura.header_store.incoherent`
  counter. **Repair is config-gated and default-off:**
  `state.repair_zakura_header_store_on_startup = false` (full-frontier scans
  are expensive mid-sync). Enable it on any node whose store may hold
  pre-#491 rows.
- **#500 — NewBlock best-chain gate** (`fix(zakura): only advance and gossip
  NewBlocks that land on the best chain`). An accepted NewBlock that
  committed to a side chain (state `Depth` = None) no longer advances the
  header frontier or gets forwarded; read failures fail safe to the
  side-chain outcome. Trace label on this lineage:
  `accepted_non_best_chain`.

## 3. Outstanding PRs (the open stack)

- **#495 — canonical-suffix primitive** (`zakura-canonical-suffix` →
  `ironwood-main`). All header-store mutations routed through one
  suffix-replacement primitive (`DiskWriteBatch::set_canonical_suffix`) with
  structural preconditions; the seed path now *replaces* a conflicting suffix
  when its parent links (shallow tip races converge). **It intentionally
  keeps the parent-mismatch seed refusal** (`zebra_db/block.rs:1923`), so the
  §4 divergence class is narrowed, not closed.
- **#496 — switch orchestration** (`zakura/writeguards-canary-v3` → #495).
  Branch switches sequenced inside the state write worker: evaluate → body
  rollback (`NonFinalizedState::rollback_block`, rollback-not-ban) → header
  rewrite → post-reorg audit. `CommitHeaderRange` returns
  `HeaderRangeCommitOutcome { tip_hash, reorged_at, reorged_to_hash }`.
  **Note:** the zebrad driver currently only logs `reorged_at` — the reactor
  is never told a commit reorged the store (see §6, PR C).
- **#498 — fleet test restore** (targets `zakura/writeguards-canary-v3`).
  ⚠️ Written against the *old*, fleet-based canary-v3; that branch was re-cut
  onto the main lineage, so #498 needs rebasing/rework before it is useful.
- **#499 — pruning coherence op** (targets #495). Adds `Op::Prune` +
  pruned-store startup-audit coverage to the harness.

## 4. The 2026-07-07 testnet-4 incident (live, root-caused)

**Symptom:** the soaking `zakura-testnet-4` node (zakura-only, legacy P2P
off) froze at height 4150157 at ~07:20Z and stayed frozen (fleet 1,100+
blocks ahead) — no panics, no store incoherence, no chain split (its tip hash
is canonical). Header tip and body tip both frozen; thousands of peer
statuses advertising higher tips received; zero `GetHeaders` sent after
07:20.

**Causal chain** (each link verified from
`/mnt/zakura-testnet-4-data/zakura-traces/{header_sync,commit_state}.jsonl`
and the zebrad log):

1. A tip race at 4150150–4150154 (duplicate hashes at several heights, five
   block-sync reorg resets). The nf/body layer handled the reorg correctly.
2. The winning branch's first block landed as a *side-chain* commit, so it
   was never seeded into the header store; every subsequent best-tip seed
   then hit the #491 parent-mismatch refusal (silent, `debug!`-only —
   invisible at the release log level). The store's header chain froze below
   4150150 while the body chain advanced to 4150157.
3. The reactor's forward-range anchor is its in-memory `best_header_hash`,
   advanced from **body** commits via the chain-tip mirror — not from the
   store. Every recovery range was therefore anchored at a hash the store
   never had (`commit_state.jsonl` shows the failing anchors:
   `001ddc35`@4150150, `0002251b`@4150151, `000b126a`@4150152,
   `00045f80`@4150157; every pre-race commit succeeded). The store rejects
   these in µs with `CommitHeaderRangeError::UnknownAnchor` — the designed
   heal is anchored at exactly the hash whose absence it must heal.
4. `UnknownAnchor` is classified `InvalidPeerRange` in
   `header_range_commit_failure_kind` — but the *local reactor* chose the
   anchor; the peer merely answered. Honest peers were scored (record-only).
5. Scheduler assignment leak: non-`Local` commit failures never
   `clear_assignment`. After `HEADER_SYNC_FANOUT` (3) distinct peers failed
   the 4150158 range, it became permanently unassignable, and
   `ensure_forward` refuses to queue a replacement range at the same start
   height. Forward header sync halted. (Earlier identical failures at
   4150151–4150153 self-healed because gossiped full-block commits covered
   the heights and pruned the leaked ranges; block gossip ingest went quiet
   at 07:17, removing that mask.)

**The cure exists — in a dropped branch.** The old fleet lineage
(`zakura/fallback-keeps-zakura-alive`, still on the remote at 6f8b4ad88)
solved this exact mechanism after a previous live occurrence (its comments
cite "4148376 — a mirror-advanced frontier over a store whose suffix rows
diverged"):

- `93480397c` — walk back on repeated header context mismatches instead of
  scoring peers (`begin_fork_recovery_walk_back` → `QueryReanchorTarget` →
  re-anchor from the **store's** hash; also removes the synced-node
  `best <= verified` reanchor gate).
- `6f8b4ad88` — classify `UnknownAnchor` commit failures as
  `ContextMismatch` (non-scoring, feeds the walk-back quorum).

The re-cut canary-v3 (this branch) is based on `ironwood-main` and does NOT
contain these. **Validated 2026-07-07 on this exact tip:** every link of the
wedge chain is present (see §5); the reorg suite passes unchanged, proving
the wedge machinery survived the re-cut.

**Immediate operational state:** t4 is still wedged (deliberately left as
evidence; everything needed is also on disk). A plain restart heals it — the
store is *behind*, not corrupt; startup re-anchors from the durable store
tip. The rollback binary `/root/zebrad.canary-v3` (sha256 731b7610…) is the
old fleet build that contains the cure. Watcher:
`/root/zakura-exp/writeguards-soak-alerts.log`; metrics on `127.0.0.1:9999`;
RPC on `:18232`; reference node `zakura-testnet-1` (167.99.103.111).

## 5. The reorg test suite (committed with this handoff)

Scenario tests across the three layers a reorg exercises, written to pin
current behavior — comments mark which behaviors are **documented gaps**.

Reactor (`zebra-network/src/zakura/header_sync/tests.rs`, banner
`=== Header-sync reorg suite ===`):

| Test | Pins | Status at this tip |
| --- | --- | --- |
| `equal_height_reorg_commit_heals_via_next_full_block_commit` | equal-height reorg commit leaves `best_header_hash` stale (publishes only on strictly higher heights); heals via next `FullBlockCommitted` | passes — gap present |
| `equal_height_best_chain_new_block_heals_via_next_higher_accept` | same gap via the NewBlock path | passes — gap present |
| `synced_tip_fork_records_misbehavior_without_reanchor_until_body_heal` | a synced node (best == verified) cannot stale-anchor reanchor on a tip fork; honest peers get record-only `InvalidRange`; body path heals | passes — gap present (`reactor.rs:1237` gate) |
| `post_reanchor_recovery_range_spans_dead_branch_despite_covered_watermark` | recovery correctness: the covered watermark does not block the post-reanchor spanning range | passes — correct behavior |
| `fanout_exhausting_invalid_range_commit_failures_wedge_forward_sync_until_body_heal` | **the t4 wedge shape**: 3 distinct-peer `InvalidPeerRange` commit failures permanently wedge the range; only a body commit heals | passes — bug present |

Driver (`zebrad/src/commands/start.rs`,
`mod zakura_header_sync_driver_tests`, banner `=== Header-sync reorg suite:
driver-level scenarios ===`): 8 tests driving the real
`drive_zakura_header_sync_actions` loop against a real endpoint + reactor
with mock state/verifier — commit success → tip/frontier publication;
`LowerWorkConflict` (losing fork) stays local/non-scoring vs `ReorgTooDeep`
scoring, end to end; full `CommitHeaderRangeError` classification table
(passes as written ⇒ **`UnknownAnchor` is still `InvalidPeerRange` here** —
flip this assertion when the §6 PR A lands); NewBlock gate defensive
branches (Depth read failure → `accepted_non_best_chain`, duplicate /
rejected / hash-mismatch outcomes); `HeaderReanchored` frontier publication;
root-covered `QueryBestHeaderTip`; degraded serving liveness.

Store level (pre-existing, this lineage): the coherence harness (44 tests)
plus #496's `switch_orchestration_tests.rs`.

When porting fixes, flip the gap-pinning tests into the fixes' regression
tests. Caution: the synced-tip-fork test must be *adapted* (not just re-run)
on a walk-back lineage — with the cure present it loops (serving retries
forever waiting for misbehavior that correctly never comes) rather than
failing an assertion; its walk-back variant should answer
`QueryReanchorTarget` and assert the re-anchored recovery range.

```bash
cargo test -p zebra-network --lib -- zakura::header_sync::tests
cargo test -p zebrad --lib -- zakura_header_sync_driver_tests
cargo test -p zebra-state --lib -- header_store_coherence
```

All green at this tip, with fmt and `clippy -D warnings` clean.

## 6. Where to pick up — ordered

1. **Land #495 then #496** (review in flight). They are correct and needed
   (single-writer primitive; crash-safe switch sequencing) — just understand
   they do not close the §4 class.
2. **PR A — reclassify + walk back (the t4 fix).** Port from
   `zakura/fallback-keeps-zakura-alive`: `93480397c` (walk-back machinery,
   removes the synced-node reanchor gate) and `6f8b4ad88`
   (`UnknownAnchor` → `ContextMismatch`). Adapt to this lineage's driver
   (which already receives `HeaderRangeCommitOutcome`). Flip the
   classification-table assertion and adapt the synced-tip-fork test as its
   regression test.
3. **PR B — scheduler assignment hygiene.** Clear (or per-peer-expire)
   assignments on `InvalidPeerRange` commit failures, or let `ensure_forward`
   replace a queued unassignable same-start range. This bug exists on every
   lineage including the old fleet one. Flip the fanout-exhaustion test.
4. **PR C — surface `reorged_at` to the reactor.** The driver already gets
   `outcome.reorged_at` (`header_sync_driver.rs:678`) and drops it; emit a
   reanchor/hash-update event when `Some` so equal-height reorg commits stop
   leaving a stale `best_header_hash`. Flip the two equal-height tests'
   heal expectations.
5. **Rebase/rework #498, land #499** once the stack settles.
6. **Soak.** Restart t4 (or roll back to 731b7610 while A–C are in review);
   after A–C, soak the patched stack through a min-difficulty burst and a
   tip race. Watcher grep notes for this lineage: "header range commit
   reorged the stored header chain" is INFO (REORG-COMMIT fires); seed
   refusals log at `debug!` only — bump the level or build with debug
   logging if you need `SEED-REFUSAL` visibility; the NewBlock gate label is
   `accepted_non_best_chain`.

## 7. Evidence index

- t4 traces: `/mnt/zakura-testnet-4-data/zakura-traces/` — `commit_state.jsonl`
  (driver-side, stops at the wedge moment 07:20Z; failing anchors in its
  tail), `header_sync.jsonl` (reactor-side; `header_range_rejected` rows,
  status stream showing peers at 4151123 with zero requests). Note: files
  append across restarts; `ts` is µs since process start — segment by file
  order, not timestamp.
- t4 log: `/mnt/zakura-testnet-4-data/logs/zebrad.log` (gossip downloads stop
  at 4150157 @ 07:17:15Z; sync-progress stall warnings after).
- Watcher + alert history: `/root/zakura-exp/writeguards-soak-alerts.log`
  (lag alerts from 07:45Z).
- Old fleet cure: branch `zakura/fallback-keeps-zakura-alive` (6f8b4ad88),
  binary `/root/zebrad.canary-v3` (731b7610…) on t4.
- Build recipe for bench binaries: `roman-zakura-3`,
  `/mnt/roman-zakura-3-data/zebra-bench/` (`fix-build.sh` pattern:
  `prometheus,commit-metrics --locked`, bins under `bins/<sha9>/`).
