# Zakura reorg corruption — repro & fix plan

Status: ALL PILLARS CODE-COMPLETE pending review (2026-07-07); full-stack canary-v3
soaking on t4 — see §1b (Phase-1 results), §1c (hotfix outcomes), §1d (Phase-2 +
canary), §1e (Pillars 3/1/1a + canary v2/v3 + monitoring). Companion to
`INVESTIGATION.md` (esp. §11c–§15). PRs: #490 (Phase-1 suite), #491 (Phase-1.5 write
guards), #493 (Phase-2 linkage-verified reads, stacked on #491), #494 (Pillar-3
startup audit + self-repair, stacked on #493), #495 (Pillar-1 suffix-replacement
primitive + after-reorg-batch audit hook, stacked on #494), #496 (Pillar-1a switch
orchestration — targets the FLEET branch `zakura/fallback-keeps-zakura-alive`; see
§1e for why). Remaining: Phase 5 (Layer 2/3 confirmation + fleet soak per §3a).

## Problem statement

After a chain reorganization, a node's zakura header store can become internally
incoherent: rows describing an abandoned branch survive in the height-indexed column
families alongside rows of the new canonical branch. The corrupted store then causes the
node to reject valid headers from honest peers and (before commit-failure
classification) to score and disconnect them, wedging the node below the network tip.
The corruption is on disk, so the worst flavor survives restarts.

### Observed incidents (testnet-4 fleet, 2026-07-06)

All incidents occurred on zakura-only nodes during natural min-difficulty burst/fork
activity on the canonical test network:

| Time (UTC) | Node | Height | Symptom | Persistence |
| --- | --- | --- | --- | --- |
| ~13:54 | t4 | 4148005 | `InvalidDifficultyThreshold` (expected `0x2000a370`, stored context yielded `0x2000a397`) on every honest header range; 59 consecutive commit-fail → peer-disconnect cycles | On disk — survived restarts |
| ~15:22 | t4 | 4148376 | `UnknownAnchor`: the frontier's anchor hash failed the store's hash↔height roundtrip (~line 2013) | Frontier/store divergence — cleared by restart |
| ~18:54 | t3 | 4148991 | Same `InvalidDifficultyThreshold` signature; self-healed via the ContextMismatch walk-back trigger after 16 classified failures | On disk until walk-back re-commit |

Common shape: a reorg-window commit leaves the store with rows from more than one
branch; the failure then surfaces later through whichever reader first touches a stale
row. §0 shows both symptoms are the same store-invariant violation seen through two
different readers.

### Scope of existing mitigations

Shipped changes (fork-recovery walk-back, PR #476; side-chain NewBlock depth gate,
PR #478; ContextMismatch commit-failure classification, `6f8b4ad88`) reduce the blast
radius: local context errors no longer score peers, and the walk-back can re-commit past
a poisoned window. They do **not** prevent the store from becoming incoherent, and the
write-side mechanism that strands rows has not been proven. This plan targets the
corruption itself: prove the mechanism with a deterministic reproduction, then
restructure the write path so the store invariants hold by construction.

---

## 0. Analysis: the header store and its invariants

All code references are to `zebra-state/src/service/finalized_state/zebra_db/block.rs`.

The zakura header store is **five height-indexed column families acting as a replicated
view of the current canonical header chain** — where "canonical" throughout this document
means the **best valid cumulative-work chain** (never height/length; the most-work gate
at ~line 2107 is the codified form of this rule):

- `ZAKURA_HEADER_BY_HEIGHT`
- `ZAKURA_HEADER_HASH_BY_HEIGHT`
- `ZAKURA_HEADER_HEIGHT_BY_HASH`
- `ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT`
- `COMMITMENT_ROOTS_BY_HEIGHT`

They are mutated by **four independent writers**:

1. the header-range batch (`prepare_header_range_batch_with_roots`, ~line 1967),
2. the body-commit path (writes verified serving-index rows),
3. the invalidate / reconsider paths (~lines 1846–1859, 1927–1945),
4. finalization (~line 1581).

The invariants that make reads sound are **assumed everywhere and enforced nowhere**:

- **I1 (bijection):** `hash_by_height` ↔ `height_by_hash` are mutually inverse.
- **I2 (linkage):** rows chain by `previous_block_hash` from the finalized tip up to the
  last row in the height index.
- **I3 (tip):** `best_header_tip()` (~line 504: `max(body tip, last hash_by_height key)`)
  is the tip of that linked chain — i.e. there are no orphan rows above or gaps below it.

Every reader trusts these blindly:

- `recent_header_context` (~line 548) walks the **height index** downward and feeds
  whatever rows it finds into difficulty validation. One stale row inside the 28-height
  `POW_ADJUSTMENT_BLOCK_SPAN` window ⇒ `InvalidDifficultyThreshold` rejections of honest
  headers (incidents 1 and 3 in the table above).
- The anchor check (~line 2013) does a hash↔height roundtrip. One non-bijective pair ⇒
  `UnknownAnchor` (incident 2).

**Both observed symptoms are the same invariant violation surfaced through two different
readers.**

The mutation paths preserve the invariants only under assumptions about each other:

- The reorg delete loop (~line 2144) fires only when a conflict is detected **inside the
  incoming range's span**, and is bounded by `first_conflicting_height..=best_header_tip`.
  If `first_conflicting_height > best_header_tip` (possible when stale rows sit above a
  gap, so `best_header_tip` under-reports), the loop is **empty**: nothing is deleted and
  the most-work gate (~line 2107) passes trivially with `existing_work = 0`.
- The insert loop (~line 2162) overwrites `hash_by_height` rows **without deleting the
  displaced hash's `height_by_hash` entry** unless the delete loop already did — a direct
  I1 violation whenever the delete loop didn't cover that height.
- Invalidation deletes rows mid-range, which can create gaps that strand rows above them
  (I2/I3 violation) for every later bounded delete loop.

Diagnosis: **a multi-writer mutable chain view with no owner and no enforced
invariants.** The specific stranding writer is not yet proven — pinning it is a repro
deliverable (Phase 1), not a precondition of this plan.

---

## 1. Repro design

### Principle: reproduce the invariant violation, not the wedge

The wedge is a delayed, reader-dependent symptom: it appears only when the DAA window or
an anchor lookup slides over a poisoned row — minutes or thousands of blocks after the
corrupting write. The violation itself is checkable **immediately after every write**.
So the repro is built around an **audit function**, and "repro" means: *find any op
sequence that fails the audit.*

### The audit (shared by all layers, and later by Pillar 3)

After every mutation, over the window `finalized_tip ..= last height-index row`:

- **A1:** for every height row `(h → hash)`, `height_by_hash[hash] == h`, and every
  `height_by_hash` entry points back at a matching height row (bijection, both ways).
- **A2:** for consecutive rows, `header[h].previous_block_hash == hash_by_height[h-1]`
  (linkage), anchored at the finalized tip hash.
- **A3:** no rows in any of the five CFs above the last linked height; no gaps below it;
  `best_header_tip()` equals the last linked height (tip integrity).
- **A4 (oracle comparison, Layer 1 only):** the linked chain equals the model's expected
  canonical chain (best valid cumulative-work, per §0).

### Layer 1 — deterministic model-based harness (the discovery layer)

Pure `zebra-state`, no network, no tokio. New test module, e.g.
`zebra-state/src/service/finalized_state/zebra_db/block/tests/header_store_coherence.rs`.

- **Oracle:** an in-memory block-tree of 2–3 branches over a shared trunk, with the
  canonical chain selected by best valid cumulative work (branches are constructed so
  work order and height order sometimes disagree, to catch any length-based selection) —
  trivially correct by construction.
- **Header fabrication:** no mining. Fabricate timestamps and compute each header's
  expected `difficulty_threshold` with the same `check::difficulty` functions the
  validator uses, so real difficulty logic runs on every branch (min-difficulty gaps
  included). Custom testnet parameters; PoW checks stay on but thresholds are trivial.
- **Op alphabet** (mirrors the real writers):
  - `CommitHeaderRange { anchor, branch, offset, len }` — including partial, split,
    overlapping, and stale-branch ranges;
  - `CommitBody { branch, next }` — the body path's serving-index writes;
  - `Invalidate { hash }` / `Reconsider { hash }`;
  - `Finalize { up_to }`;
  - `Reopen` — close and reopen the DB (persistence / restart-survival check).
- **Drivers:**
  1. **Scripted scenarios** (~10) transcribing the production event shapes: walk-back
     re-commit delivered in split ranges; body commit racing a header-range reorg;
     invalidate-then-range; reorg to a *lower* height; double reorg at the same fork
     point; reorg crossing the 28-block DAA window edge; restart between walk-back and
     re-commit; `Reopen` between body invalidation and header rewrite (the Pillar 1a
     crash-point table); **lower-work-then-higher-work fork** (see below).
  1a. **Scenario: `LowerWorkConflict` is non-terminal.** Branch B first arrives as a
     lower-cumulative-work suffix than the current canonical branch A: the commit must
     be rejected with `LowerWorkConflict`, the store must be byte-identical to before
     the attempt (audit + oracle: A still canonical, no partial rows from B), and the
     rejection must be classified as a *normal chain-selection outcome* — not an error
     that terminates the branch or the source. Later, an extended version of B arrives
     (in production: from a different peer) whose suffix now carries strictly more work
     than A: the switch must succeed and B become canonical. Variants: B extended in one
     range vs. split ranges; B's second delivery re-including the previously rejected
     prefix; a `Reopen` between the two deliveries. This pins the property that losing
     a work comparison *once* is not a lasting verdict on the branch — the store must
     accept the same fork point again the moment the work balance flips.
  2. **Property test** (proptest) over random op sequences, shrinking on audit failure —
     this *searches* the sequence space for the trigger instead of guessing, and reduces
     any hit to a minimal seed-pinned counterexample.

**Exit criterion:** at least one deterministic, shrunk, seed-pinned failing test
(expected: several). Each becomes a permanent regression test for Phase 2.

### Layer 2 — reactor testkit scenarios

Existing zakura testkit: drive the header-sync reactor against a scripted peer serving
both branches (fork mid-sync, walk-back, range re-delivery), then run the audit on the
resulting store. Covers reactor↔state interaction the pure-state layer can't: walk-back
timing, range splitting, frontier/store divergence.

Includes the reactor-level half of scenario 1a: peer P1 serves the lower-work branch B
and is rejected with `LowerWorkConflict`; peer P2 later serves the extended, higher-work
B. Assertions: **P1 is never scored or disconnected** for the rejection (an honest peer
on a losing fork is not misbehaving), the reactor keeps requesting from P1 afterwards,
and the switch to B via P2 completes. This is the regression test for the policy that
work-comparison losses are non-terminal and non-scoring at the peer layer.

### Layer 3 — local controlled-mining net (confirmation & soak)

Demoted from discovery to confirmation. 3–6 zebrad instances on one host:

- custom testnet (own genesis, upgrades at height 1, trivial `target_difficulty_limit`,
  PoW **on** so DAA logic runs), zakura-only P2P on loopback with explicit
  `zakura.bootstrap_peers` and per-node iroh keys;
- two miner roles driven by `getblocktemplate`/`submitblock` (harness pattern already in
  `zebrad/tests/common/regtest.rs`); controlled template timestamps let us fabricate
  min-difficulty bursts deterministically;
- a partition controller (stop process / firewall the QUIC port) to manufacture reorgs of
  chosen depth: isolate B, mine N on A and N+1 on B, heal, assert convergence + audit.

Used to confirm minimal Layer-1 sequences behave identically with real reactors, gossip
and restarts — and for post-fix soak.

### What this repro deliberately does NOT target

Timing races (§12 crossing-dial wedge), WAN gossip lag, and load-driven commit stalls are
different bug classes; they stay with fleet soaks. This plan is scoped to reorg-driven
store corruption.

---

## 1b. Phase 1 results (2026-07-06)

The Layer-1 harness landed in PR #490 at
`zebra-state/src/service/finalized_state/zebra_db/block/tests/header_store_coherence/`
(module README has the full walkthrough; shared helpers promoted to
`block/tests/common.rs`). Exit criterion exceeded: instead of one shrunk failing
sequence, **three distinct write-path bug classes** were proven, each pinned by a
deterministic `corruption_repro_*` test (passes today, demonstrating the violation)
plus an `#[ignore]`d `*_upholds_invariants` twin (the true invariant, to be un-ignored
with the fix — at which point the repro fails loudly and the pair is re-triaged).

### Proven bug classes

1. **Unlinked-anchor commit** (`corruption_repro_unlinked_anchor_commit`) — I2.
   `prepare_header_range_batch_with_roots` never checks
   `headers[0].previous_block_hash == anchor`, nor any intra-range linkage. A range of
   fork-branch headers anchored at the same-height hash of a *different* branch passes
   contextual difficulty validation and commits a suffix that does not link to the row
   below it. **Reachable from a single untrusted peer response** — nothing upstream of
   the store re-checks anchor linkage. Repro: commit trunk, then `A[1..]` anchored at
   `trunk@51`.
2. **Re-delivery over committed bodies** (`corruption_repro_redelivery_over_bodies`) —
   I3 frontier-overlay. The insert loop (~line 2162) gates only its *roots* write on
   `contains_body_at_height` (line 2190); the header/hash/height/body-size writes are
   unconditional. A range re-delivered over heights whose bodies were committed
   meanwhile re-inserts zakura rows below the body tip, and the release trim already
   ran at body-commit time, so nothing ever removes them. The roots gate's own comment
   describes exactly this delivery shape — the gate was added for roots but not for the
   header rows themselves.
3. **Unlinked seed** (`corruption_repro_seed_above_gap`,
   `corruption_repro_seed_fork_switch`) — I2/I3. Found by the discovery proptest and
   shrunk to a **single op**. `prepare_zakura_header_from_committed_block` (~line 1801)
   writes its rows with no linkage or anchor precondition. Seeds fire only at
   non-finalized best-*tip* commits (`write.rs`
   `should_seed_zakura_header_from_non_finalized_commit`), so any nf best-tip jump —
   a fork switch between nf chains, or a restart restoring the nf backup — seeds a
   height whose parent row is missing or belongs to another branch: a gap or broken
   link on disk.

### What this changes about the §0 diagnosis

- **No races or crash timing needed.** All three are single-writer logic holes
  reachable in one or two ops. The §0 candidate mechanism (bounded delete loop made
  empty by an under-reporting `best_header_tip`) did **not** reproduce as such: the
  delete/truncate loops themselves proved fairly tight (all-five-CF deletes). The real
  holes are more direct — missing linkage validation on two writers and a missing body
  gate on the third.
- **Incident mapping (intuition, not yet trace-confirmed):** bugs 1 and 3 create
  broken-link/stranded rows that poison `recent_header_context` → the
  `InvalidDifficultyThreshold` wedges (incidents 1, 3); gap/bijection states from
  bug 3 are consistent with the `UnknownAnchor` roundtrip failure (incident 2). The
  seed path is the leading candidate for the live fleet corruptor because it fires on
  every nf best-chain switch — i.e. on exactly the fork events that preceded every
  incident. Confirming which class produced each incident from fleet traces is
  worthwhile but not blocking: all three must be fixed regardless.
- **Coverage:** a 2048-case discovery sweep with the three known shapes masked
  (`Harness::new_avoiding_known_corruptions`) found no fourth class at that depth.
  The masking hooks are in `ops.rs`; re-run discovery after each fix lands and after
  widening the op alphabet.

### Harness facts to remember (they constrain Layers 2–3 too)

- The testnet minimum-difficulty rule is **inactive below height 299,188**
  (`TESTNET_MINIMUM_DIFFICULTY_START_HEIGHT`), so low-height fabrication cannot use
  min-difficulty gaps for work divergence. Work divergence comes from DAA drift
  (~2%/block, damped and median-lagged ~6 blocks) — branches need 26–30 headers for
  work order and height order to disagree. Layer 3's controlled-mining net will face
  the same constraint.
- Header times serialize as whole seconds; sub-second fabricated spacing desyncs the
  difficulty context on the store round-trip.
- Bodies commit strictly sequentially at `body_tip + 1`; seeds fire only at nf
  best-tip commits — both facts define which op sequences are production-realistic
  (encoded in the oracle's `predict_*` gates).

### Fix-phase test mechanics

When the write-path fixes land: un-ignore the four `*_upholds_invariants` twins and
`prop_random_sequences_uphold_invariants` (the permanent sweep), delete the
`corruption_repro_*` tests, and remove the `avoid_known_corruptions` plumbing from
`ops.rs`/`prop.rs`. The committed seed file under `zebra-state/proptest-regressions/`
stays. **Executed in Phase 1.5 exactly as specified — see §1c.**

---

## 1c. Phase 1.5 results (2026-07-06)

PR #491 (`fix(state): validate linkage in zakura header-store writers`) ships the three
narrow guards from the §2 Phase-1 note, all in
`zebra-state/src/service/finalized_state/zebra_db/block.rs`:

1. **Bug 1 — range linkage validation.** `prepare_header_range_batch_with_roots` verifies
   `headers[0].previous_block_hash == anchor` and full intra-range contiguity inside the
   validation loop, before anything is staged; failures reject with the new
   `CommitHeaderRangeError::UnlinkedRange { height, expected_parent, actual_parent }`.
2. **Bug 2 — committed-height insert gate.** The insert loop skips any height where
   `contains_height` is true, generalizing the old roots-only gate to every zakura row
   write. **Deliberate predicate choice:** `contains_body_at_height` probes `tx_by_loc`,
   which pruning removes/skips, so a body-presence gate would re-insert zakura rows at
   pruned heights (rows the release trim never touches again — the bug-2 shape, invisible
   to the harness because it has no pruning op). `contains_height` probes the consensus
   `hash_by_height` row, which every committed block writes and pruning retains.
   Conflicting rows at those heights were already unreachable (`ImmutableConflict` fires
   first, since pruned/body heights are ≤ the finalized tip), so the gate only ever skips
   rows that match the stored chain.
3. **Bug 3(a) — seed parent-linkage refusal.** `prepare_zakura_header_from_committed_block`
   compares the seed's parent hash against the merged header view at `height - 1`
   (full-block row first, then zakura row) and skips the write as a silent no-op
   (`debug!` log, `Ok(())`) when it does not link. Decision taken: option (a) from the
   §2 note; option (b) — routing seeds through the Pillar-1 primitive — remains the
   Phase-3 end state. The only production caller (`write.rs` ~line 707) ignores the `Ok`
   result apart from a trace log, so no caller changes were needed. New scenario
   `s11_refused_seed_converges_via_range_delivery` proves the recovery story: refused
   seed = byte-identical store, later linked range delivery converges onto the new branch.

**Error classification (zebrad):** `UnlinkedRange` is explicitly `Local`/non-scoring in
`header_range_commit_failure_kind`. The reactor already validates every response's
linkage against the requested anchor at the wire (`validate_header_range_links`, scored
there as `FirstHeaderDoesNotLink`/`NonContiguousHeaders`) and commits with that same
anchor — so the store-level check failing means local anchor/response pairing broke, not
peer misbehavior. Misclassifying it as peer-scoring would recreate the
disconnect-honest-peers failure mode. Pinned by a unit test in `start.rs`.

**Test mechanics executed per §1b:** four twins un-ignored (now permanent gates, with
tightened assertions: the unlinked-anchor twin asserts the `UnlinkedRange` variant, the
seed twins assert the tip did not move), `corruption_repro_*` deleted, masking plumbing
removed from `ops.rs`/`prop.rs`, `prop_random_sequences_uphold_invariants` un-ignored.
The harness now models refused seeds: a successful seed call must mutate the store iff
`seed_is_parent_linked` (side-effect-freedom checked via full store dumps). One
pre-existing test re-shaped: `write.rs`'s `side_chain_commit_does_not_seed_zakura_headers`
encoded the bug-3 corruption (seed above a gap in an empty store) and now pre-writes the
fake parent's zakura hash row instead (a naked consensus `hash_by_height` row aborts the
process: a finalized tip implies note-commitment trees exist).

**Gates:** suite 22/22 green; random sweep clean at `PROPTEST_CASES=4096` against the
final code with no masking; `cargo test -p zebra-state` (323 lib + 2 integration), fmt,
`clippy -D warnings` (zebra-state + zebrad), and workspace check all clean.

**Residuals for the remaining phases:**

- These guards prevent *new* corruption; stores already poisoned by pre-#491 binaries
  stay poisoned until Pillar 3's audit + truncate-and-resync repair ships. Fleet deploy
  of #491 alone would still show the wedge signatures on already-corrupt nodes.
- A refused seed leaves the header store lagging the nf chain until range sync converges
  it — the known cost of option (a); Pillar 1's fork-point rewrite removes the lag.
- The pruning blind spot found during review (bug-2 predicate) suggests Pillar 3's
  runtime audit must define its expected-rows window correctly on pruned nodes, and the
  harness op alphabet could grow a pruning op when the audit ships.

---

## 1d. Phase 2 results + Phase-1.5 canary (2026-07-06)

**Phase 2 (Pillar 2) shipped as PR #493** (`fix(state): verify zakura header-store
invariants in consensus reads`, branch `zakura-linkage-verified-reads`, stacked on #491):

- New `StoreIncoherentError` (`HeaderHashMismatch` / `BrokenLinkage` / `Gap` /
  `BijectionMismatch`), exported from `zebra-state`. `recent_header_context` verifies at
  every step of its walk that the stored header is the block its hash row names, links to
  the row below, and that no row is missing below a stored one; the anchor round-trip
  reports index bijection violations as `StoreIncoherent` instead of `UnknownAnchor`.
- **Design addition over the §2 sketch:** parent-link verification alone consumes the
  window's bottom-edge row unverified, so the walk also hashes each consumed header
  (`HeaderHashMismatch`) — fully self-verifying at ~28 header hashes per walk.
- `CommitHeaderRangeError::StoreIncoherent` → `Local`/non-scoring in zebrad. On the fleet
  branch it should later route to `ContextMismatch` (walk-back trigger) — noted in the PR.
- New `reads.rs` module in the coherence suite pins all four fault shapes end to end
  (hand-injected corruption; walk error + writer rejection + side-effect freedom).
  `header_range_commit_rejects_non_current_anchor_hash` re-pinned to the new variant.

**Phase-1.5 canary (running):** `ironwood-main`+#491 would strip the 16 fleet mitigation
commits (`zakura/fallback-keeps-zakura-alive`, tip `6f8b4ad88` = the deployed binary
`2b67b90b…`), so the canary is **fleet baseline + cherry-picked #491** = branch
`zakura/writeguards-canary` (`c89e76a33`, tests dropped in the pick; classification merged
cleanly with the ContextMismatch arms). Built on roman-zakura-3 with the standard fleet
flags (`--features prometheus,commit-metrics --locked`), deployed to t4 only
(`/usr/local/bin/zebrad`, backup at `/root/zebrad.pre-writeguards.bak`, sha256
`f656f23f…`). Snapshot onboard (`dod1_onboard.sh`): **SUCCESS, hash-equal with the fleet
at 4149440** (~142 s). t4's tracing filter gained
`zebra_state::service::finalized_state::zebra_db=debug` so the seed-refusal skips are
observable (config backup `zebrad.toml.pre-writeguards-soak.bak`). A 5-min soak monitor
watches wedge signatures (`InvalidDifficultyThreshold`, `UnknownAnchor`, panics),
seed-refusal counts (the live bug-3 mechanism confirmation), fleet lag, and restarts,
per §3a. Rest of the fleet stays on `2b67b90b…` as the reference chain.

---

## 1e. Pillar 3 (startup half), Pillar 1, Pillar 1a results + canary v2/v3 (2026-07-07)

**Pillar 3, startup half — PR #494** (`zakura-startup-audit-repair`, stacked on #493,
`fix(state): audit and self-repair the zakura header store at startup`).
`audit_and_repair_zakura_header_store()` in `zebra_db/block/startup_audit.rs`, called
from `ZebraDb::new` (skipped for read-only instances). Ports A1–A3 into the node: a
linkage walk from the finalized tip (hash/header row agreement, header hashes to its
indexed hash, parent link, reverse-index round-trip) finds the last coherent height;
stranded rows above it, orphaned reverse-index entries, and stale rows at committed
heights are deleted in one atomic batch; header sync re-downloads the suffix. Design
decisions vs. the §2 sketch:

- **Whole-CF scan, not a height-capped window** — a `MAX_BLOCK_REORG_HEIGHT`-bounded
  window can't see stale committed-height rows or deep stranded suffixes. Cost stays
  trivial because the zakura CFs only ever hold the frontier: confirmed live on t4's
  real store — the audit walked exactly 1000 rows (the frontier is release-trimmed to
  `MAX_BLOCK_REORG_HEIGHT`, so this section's "cheap" claim holds as stated).
- **§1c residual handled:** the committed-height predicate is `height ≤ finalized tip`
  (≡ `contains_height`; pruning-safe). Verified roots at/below the tip are never
  scanned. Residual accepted: stale *provisional* roots at pruned committed heights
  are indistinguishable from verified rows and left in place (documented in the PR).
- On violation: `warn!` + `state.zakura.header_store.incoherent` (the §3a primary
  metric); a clean pass logs `info!` so the soak can observe the audit running.
- 8 fault-injection tests (broken linkage + resync-converges, gap/stranded via
  reopen-only, missing/orphaned reverse row, stale committed rows, Pillar-2
  unblocking, metric via a local recorder, coherent no-op); the harness `Reopen` op
  now exercises the audit as a no-op suite-wide.

**Pillar 1 — PR #495** (`zakura-canonical-suffix`, stacked on #494, `fix(state): own
zakura header-store mutations with a single suffix-replacement primitive`).
`DiskWriteBatch::set_canonical_suffix(fork_point, new_rows)` with the §2 contract:
total-above-fork deletion by scanning the actual on-disk rows of all five CFs (never
a computed tip), structural linkage precondition, fork ≥ finalized tip (closes the
bug-2 insert shape), committed-body interlock. All three writers routed through it;
the Phase-1 oracle is the semantics spec (`apply_header_range`: replacement from the
first conflict, and a non-conflicting redelivery must NOT truncate above the range
end; `apply_seed`: truncate-from-height on any non-identical seed). A contract test
caught a real hole during development: deriving reverse-index deletions from the hash
rows misses entries already orphaned by prior damage — the primitive scans the
(frontier-sized) reverse CF in full. **The after-every-reorg-batch audit hook landed
here** (not in a later phase): batches count the rows their replacements delete, and
the range/seed/body write sites run the audit after any batch with a nonzero count —
pure appends never pay it. Gates: coherence suite 42/42 against the rerouted writers,
1024-case release proptest sweep, fmt/clippy/zebrad clean.

**Pillar 1a — PR #496, on the FLEET branch.** Key architectural discovery: the
machinery 1a orchestrates (the PR #476 walk-back, `HeaderRangeCommitOutcome`,
`reorged_at`, driver-side invalidation) exists **only on the fleet lineage**
(`zakura/fallback-keeps-zakura-alive`); `ironwood-main` has none of the 16 fleet
commits. So 1a is implemented there (head `zakura/writeguards-canary-v3`, which
carries #491/#493/#494/#495 as cherry-picks that dissolve when the fleet branch
merges main-ward), and this plan's assumption that all phases stack on main breaks at
this phase. Three defects fixed, each one predicted by §2:

1. **Inverted sequencing.** The fleet committed the header rewrite to disk first and
   invalidated the stranded body suffix afterwards, from the driver, as a separate
   request — the §2 crash-table ordering violated. The orchestration now lives inside
   the state write worker (`commit_header_range`): evaluate → body rollback → header
   rewrite → post-reorg audit, straight-line code, no round-trip. The commit outcome
   carries `reorged_to_hash` so the worker recognizes the stranded suffix without
   re-reading state.
2. **`InvalidateBlock` is a ban, not a rollback** — `BlockPreviouslyInvalidated`
   refuses re-delivery, exactly the §2 hazard (branch unrecoverable if the winner
   evaporates). The not-a-ban test caught it live. New
   `NonFinalizedState::rollback_block` (shared chain surgery, no invalidated-list
   insert); operator-facing `Request::InvalidateBlock` semantics unchanged.
3. **Latent panic:** invalidating the non-finalized root empties the chain set and
   `update_latest_chain_channels` `expect`s it non-empty — reachable by the old
   driver flow for forks right above the finalized tip. Guarded (emptied state still
   published on the watch channel; tip watch stays stale until the new branch's
   bodies commit, same as before minus the crash).

The driver's post-commit `invalidate_reorged_body_suffix` call is gone; the helper
survives only for the startup/periodic body-suffix *reconciliation* sweep.
`StoreIncoherent` routes to `ContextMismatch` on the fleet lineage per the §1d note
(classification test re-pinned). Tests: 4 orchestration tests in `write.rs`
(rollback-from-fork + publish, not-a-ban, root-rollback-no-panic, no-op cases);
305 zebra-state + 43 zebrad header-sync tests green.

**Canary v2/v3 + monitoring (t4):**

- **v2** (01:03Z, binary `66cff3a0…`): fleet baseline + #491 + #494. First real-store
  audit run: PASS, `frontier_rows=1000`. **v3** (02:08Z, binary `731b7610…`): the
  full stack incl. 1a; audit PASS on every boot since. Binary backups chain:
  `/root/zebrad.pre-writeguards.bak` → `.writeguards-canary-v1.bak` → `-v2.bak`.
- t4's `[metrics]` endpoint is now actually enabled at `127.0.0.1:9999` (the §3a
  note was aspirational — the section was empty before 2026-07-07).
- **Watcher v3** (`writeguards-soak` unit, now `Restart=on-failure`): v1 grepped the
  journal but zebrad logs to a file, so its wedge/seed counters could never fire;
  v3 tails `/mnt/zakura-testnet-4-data/logs/zebrad.log` by byte offset, primes
  cursors on first run, and alerts on wedge signatures, panics, restarts, RPC-down,
  fleet lag, and chain splits (equal height, different best hash vs t1). Soak-proof
  events: `AUDIT-PASS`, `STORE-REPAIR`, `BODY-ROLLBACK` (the 1a sequencing firing),
  `REORG-COMMIT` (fork exposure — a soak window must show nonzero exposure before it
  counts), `SEED-REFUSAL`; heartbeats carry the incoherent counter and reorg-exposure
  metrics. Log: `/root/zakura-exp/writeguards-soak-alerts.log`.
- **Push alerts:** an hourly cloud probe checks the public RPCs (t4 vs t1) and
  Slack-DMs on failure only (RPC down after retry, lag > 20, chain split) — routine
  `trig_017pysyK77AXNqckRmQetAN8`. It cannot see the on-host log, so a repair
  recurrence that doesn't wedge the node still needs the log read once per soak
  verdict.

---

## 2. Solution design

Not a patch to delete-loop bounds — a restructure of ownership so the invariants hold by
construction. Three pillars, independently landable, each gated on the Layer-1 suite.

**Phase-1 note — narrow hotfixes are now on the table.** ✅ **Shipped as Phase 1.5
(PR #491, see §1c)** — all three hotfixes below landed, with bug 3 resolved as
option (a). The original analysis is kept for the record:

- *Bug 1 hotfix:* validate `headers[0].previous_block_hash == anchor` and intra-range
  linkage at the top of `prepare_header_range_batch_with_roots`. Highest urgency — the
  only class reachable by a single hostile peer response. A few lines.
- *Bug 2 hotfix:* skip (or gate) the header/hash/height/body-size inserts at
  body-backed heights, mirroring the existing roots gate at line 2190.
- *Bug 3 needs a decision, not just a check:* the seed writer must handle a
  non-parent-linked seed as what it is — a branch switch. Options: (a) refuse and let
  range sync converge (simplest; risks the header store lagging nf), or (b) treat it as
  a fork-point rewrite through the Pillar-1 primitive. Leaning (a) as the hotfix and
  (b) as the end state.

Recommended: ship hotfixes 1+3(a) (and 2 if trivial) as a small consensus-adjacent PR
gated on the flipped Phase-1 twins, *then* proceed with the pillars. This de-risks the
fleet while the restructure is reviewed.

### Pillar 1 — one owner, one mutation primitive

✅ **Shipped as PR #495 (see §1e)**, including the Phase-1 contract additions and the
Pillar-3 after-reorg-batch audit hook. The original design is kept for the record:

A single internal API is the **only** way any path replaces the **header-store suffix**
above a height:

```text
set_canonical_suffix(fork_point: Height, new_rows: &[HeaderRow]) -> WriteBatch ops
```

In one atomic `WriteBatch` it:

1. iterates the height index from `fork_point + 1` to the **actual last row on disk**
   (RocksDB last-key — never a cached or computed tip),
2. deletes every row in all five header CFs, **including the `height_by_hash` entry of
   each displaced hash**,
3. writes the new suffix rows.

**Scope: headers only.** `set_canonical_suffix` owns the five header CFs and nothing
else. It must **refuse** (hard precondition, as today's `ConflictingFullBlockHeader`
check at ~line 2147 does) if any height in `fork_point+1..` has a committed full block
body. Full blocks move exclusively through the explicit invalidation / reconsider /
reset semantics (`Request::Invalidate…` etc.), which carry their own consensus-side
effects (non-finalized chain updates, tip events, UTXO/tree state). A header writer
never deletes a body, accidentally or otherwise. Because of this refusal, the primitive
alone cannot execute a reorg across body-backed heights — that is the job of the switch
orchestration below.

Header-range reorgs, the header rows written by the body path, and the header-row side
of invalidation all route through it. Stranding becomes impossible because deletion is
total-above-fork by construction, not bounded by conflict detection. Existing guards
(immutable-below-finalized, committed-body refusal, cumulative-work gate, reorg depth
limit) remain as *preconditions checked before* calling the primitive.

**Phase-1 additions to the primitive's contract** (each closes a proven bug class):

- `set_canonical_suffix` **verifies linkage structurally**: `new_rows[0]` must link to
  the on-disk hash at `fork_point`, and every row to its predecessor. Bug 1 exists
  because no writer checks this today; in the primitive it is a hard precondition, not
  a validation step callers can forget.
- The **seed path routes through it too** — bug 3 showed the seed writer is a de facto
  branch-switch writer (nf best-tip jumps). A non-parent-linked seed is a fork-point
  rewrite and must carry a fork point, not a bare (height, block).
- The committed-body refusal covers **inserts as well as deletes** — bug 2 was an
  unconditional insert below the body tip, not a bad delete.

### Pillar 1a — switch orchestration: evaluate the candidate before mutating anything

✅ **Shipped as PR #496 (see §1e), on the fleet branch** — implemented inside the state
write worker rather than the driver; steps 1–2 were already the range writer's
validation, step 3 gained the required rollback-not-ban variant
(`NonFinalizedState::rollback_block`), and step 5 rides the existing chain-tip mirror
(a dedicated reanchor event is noted as follow-up). The original design is kept for
the record:

A named orchestration step in the state/driver layer sequences every branch switch. No
store mutation happens until the candidate branch has fully won on its merits:

1. **Candidate assembly & in-memory validation.** Materialize the competing branch
   (headers from walk-back re-delivery, gossip, or range responses) from its last common
   ancestor with the current canonical chain. Validate it entirely in memory against
   context derived from that ancestor — linkage, checkpoint conflicts, per-header
   difficulty via the same `check::difficulty` functions — reading the store but writing
   nothing. Validation failure ⇒ reject the candidate; the store is untouched.
2. **Work comparison & fork-point decision.** Compute cumulative work of the validated
   candidate suffix vs. the current canonical suffix above the common ancestor. Require
   **strictly greater** work to switch (ties keep the incumbent). The fork point is the
   last common ancestor — decided here, once, and passed explicitly to the mutation
   steps rather than re-derived by each of them. A losing comparison
   (`LowerWorkConflict`) is a **normal chain-selection outcome, not a fault**: it must
   not score the serving peer and must not blacklist the branch — the same fork point
   stays switchable the moment a later, extended candidate carries more work
   (test scenario 1a in §1).
3. **Body invalidation.** If any height in `fork_point+1..` has a committed full block,
   issue the explicit invalidation requests for the old-branch blocks above the fork,
   with all their consensus-side effects (non-finalized chain update, tip Reset event,
   UTXO/tree rollback). Skipped when the switch is header-only.
4. **Header rewrite.** Call `set_canonical_suffix(fork_point, candidate_rows)`. Its
   committed-body refusal is now a *safety interlock*, not a code path: reaching step 4
   with a body still present above the fork means the orchestrator is buggy, and the
   switch aborts loudly instead of corrupting the store.
5. **Driver/frontier notification.** Reanchor the header-sync frontier and publish the
   new tip so body sync targets the new branch.

#### Crash safety of the two-step switch

Steps 3 (body invalidation) and 4 (header rewrite) are separate atomic batches by
design — they carry different consensus semantics — so the node can die between them.
The order is chosen so that **every reachable intermediate state is coherent**, and the
Pillar 3 startup audit defines the expected recovery for each crash point:

| Crash point | On-disk state at restart | Startup audit verdict | Expected recovery |
| --- | --- | --- | --- |
| Before step 3 (steps 1–2 are read-only) | Unchanged: branch A canonical, all rows intact | Pass | None needed; the candidate is re-discovered from peers and the switch re-runs from step 1 |
| Between steps 3 and 4 | Trunk truncated at the fork point: A's bodies and header rows above the fork removed, chain linked up to the fork, no B rows yet | Pass (a shorter linked chain violates no invariant) | Node restarts as "behind": header sync reanchors at the fork point and re-fetches whichever branch *currently* wins on work — B if still best, or A again if B evaporated |
| Mid-step 4 | Impossible as a partial state: `set_canonical_suffix` is one atomic `WriteBatch` — restart sees either the pre-step-4 or post-step-4 store | Pass | As the row above or below |
| Between steps 4 and 5 | B is the canonical header suffix; only the in-memory frontier/tip notification was lost | Pass | Frontier and tip are rebuilt from the store at startup; body sync targets B naturally |
| Any other torn state | By definition a bug in the orchestrator or primitive | **Fail** | Pillar 3 repair: truncate header CFs to the last coherent height, metric + `warn!`, re-sync |

Two requirements fall out of the "between steps 3 and 4" row:

- **Step-3 invalidation must be rollback, not ban.** The reorg switch removes A's blocks
  above the fork so they can be *replaced*; it must not record them as permanently
  invalid. Otherwise a crash before step 4 — followed by branch B disappearing from the
  network — would leave the node unable to re-accept the still-canonical branch A. If
  the existing `Invalidate` semantics carry a ban list, the orchestrator uses (or adds)
  a rollback variant, or pairs the crash-recovery path with `Reconsider`.
- **The switch must be idempotently re-runnable.** Recovery is never a special code
  path: restart puts the node in a state where the ordinary sync + orchestration flow
  (steps 1–5) converges to the network's best chain, regardless of where the previous
  attempt died. The Layer 1 `Reopen` op and a dedicated kill-between-batches scenario
  test exactly this.

### Pillar 2 — consensus reads follow hash links, not the height index

`recent_header_context` verifies linkage as it walks: at each step, check
`header.previous_block_hash == hash_by_height[h-1]` before consuming the row below. On
mismatch, return an explicit `StoreIncoherent` error instead of silently feeding garbage
into difficulty validation. Same treatment for anchor resolution.

Effect: a stale row can no longer poison validation — it can only trigger repair.
Corollary (already policy since the ContextMismatch work, kept as a hard rule): **local
store incoherence must never score or disconnect peers.**

### Pillar 3 — audit + self-repair at runtime

✅ **Shipped**: the startup half as PR #494, the after-every-reorg-batch hook with
PR #495 (see §1e). The original design is kept for the record:

The Phase-1 audit function ships in the node:

- runs at startup and after every reorg batch, bounded to the
  `finalized_tip..header_tip` window (≤ `MAX_BLOCK_REORG_HEIGHT`, so cheap);
- on violation: `warn!` + metric (`state.zakura.header_store.incoherent`), truncate the
  header CFs to the last coherent height, let sync re-download.

Headers are re-fetchable; correctness beats preserved rows. This converts any *residual*
bug in this class from a permanent on-disk wedge into a self-healing, observable
transient — the property the fleet actually needs.

### Interaction with existing mitigations

The fork-recovery walk-back (PR #476) currently re-commits header suffixes through
`prepare_header_range_batch_with_roots`, which interleaves validation, work comparison,
and mutation in one pass; under this design it feeds candidates into the Pillar 1a
orchestration instead, replacing the bounded delete-loop semantics rather than amending
them. The
ContextMismatch classification (local context errors never score peers) is retained and
generalized by Pillar 2's `StoreIncoherent` error. If Phase 1 shows the walk-back
re-commit itself strands rows, the fix lands in the primitive, and the corresponding
Phase-1 counterexample becomes its regression test.

---

## 3. Sequencing & estimates

| Phase | Work | Est. | Gate |
| --- | --- | --- | --- |
| 1 ✅ | Layer-1 harness: oracle, op alphabet, audit, scripted scenarios, proptest | done (PR #490) | **Exceeded**: 3 deterministic bug classes proven (§1b), each with a repro + ignored-twin pair |
| 1.5 ✅ | Narrow hotfixes: range anchor/linkage check (bug 1), seed parent-linkage refusal (bug 3a), committed-height insert gate (bug 2) | done (PR #491) | **Met** (§1c): all four twins green, repros removed, sweep clean at 4096 cases without masking |
| 2 ✅ | Pillar 2: linkage-verified reads, `StoreIncoherent` error | done (PR #493) | **Met** (§1d): `reads.rs` suite green, all four fault shapes pinned, poison de-fanged |
| 4-first ✅ | Pillar 3 pulled ahead of Pillars 1/1a (sequencing decision 2026-07-06: gates fleet-wide #491 rollout, reuses the Phase-1 A1–A3 audit, independent of the restructure, gives the soak its primary §3a metric). Startup-audit half, stacked on #493. | done (PR #494) | **Met** (§1e): 8 fault-injection tests incl. reopen-persistence heal; metric asserted via a local recorder; audit PASS on t4's real store (frontier=1000 rows) |
| 3 ✅ | Pillars 1 + 1a: `set_canonical_suffix` (with the §2 Phase-1 contract additions), switch orchestration, route all writers incl. the seed path through them | done (PR #495 main stack; PR #496 **fleet branch** — see §1e for why 1a can't stack on main) | **Met** (§1e): coherence suite 42/42 against rerouted writers; 1024-case sweep sustained; body-rollback sequencing + rollback-not-ban + root-rollback-no-panic pinned by tests. Found & fixed: `InvalidateBlock` ban, nf-root panic, inverted fleet sequencing |
| 4 ✅ | Pillar 3's after-reorg-batch hook (folded into the Pillar-1 PR rather than a separate phase) | done (PR #495) | **Met**: reorg batches self-report replaced rows; audit runs post-write only on real reorgs |
| 5 | Layer 2 + Layer 3 confirmation, then fleet soak | ~1 day + soak | Local-net reorg matrix clean; fleet soak clean (soak of canary-v3 running since 2026-07-07 02:08Z — needs ≥12 h with nonzero REORG-COMMIT exposure, then §3a kill-soak + mass restarts) |

Each phase is a separate reviewable change; Phase 1 landed as tests only (safe by
construction). Phases 1.5–4 are consensus-adjacent and get the senior-review protocol.
Phase 1.5 may fold into Phase 3 if review bandwidth prefers one write-path change —
but bug 1's single-peer reachability argues for shipping it first.

---

## 3a. Final validation on testnet-4 (the real fleet)

Layer 3 confirms the fix in a controlled net; the last gate is the canonical zakura
testnet, where forks arrive naturally from min-difficulty bursts (several per hour during
burst windows — the environment that produced every production wedge).

### Fleet & ops context

- Nodes: t1 `167.99.103.111`, t2 `167.99.110.145`, t3 `138.68.229.254`,
  eu `164.92.209.78`, as `206.189.148.0`, t4 `138.197.75.218` (zakura-only).
- t4: data at `/mnt/zakura-testnet-4-data`, config `/etc/zakura/zebrad.toml`,
  RPC `:18232`, metrics `127.0.0.1:9999`. Trace jsonl files (`header_sync.jsonl`,
  `block_sync.jsonl`, `conn.jsonl`, `commit_state.jsonl`) under `/mnt/*/zakura-traces/`.
- Snapshot onboarding: `/root/zakura-exp/dod1_onboard.sh` on t4 (snapshot reset → sync →
  hash-equal verdict vs a reference node); a clean run is ~15k blocks in ~2–3 min.
- Build host: `178.128.71.19` (repo under `/mnt/roman-zakura-3-data/zebra-bench`,
  `PATH=/root/.cargo/bin`).

### Rollout order

1. **Canary (t4 only, ≥12 h):** deploy the full-fix binary to t4 alone. t4 is zakura-only
   — no legacy stack to mask header-sync bugs — so it fails loudest. The rest of the
   fleet stays on the previous binary and doubles as the reference chain.
2. **Fleet-wide (all 6 nodes, ≥48 h):** after a clean canary window that includes at
   least one natural min-difficulty burst.

### What to run during the soak

- **Natural fork exposure:** just uptime through burst windows; today's baseline was
  ~3 wedge-class events per day, so a multi-day soak has real exposure.
- **Snapshot onboarding runs** (`dod1_onboard.sh`) — each re-crosses every historical
  fork point on chain; repeat a few times per day.
- **Mass restarts** (2–3 rounds, staggered and simultaneous): restart-during-reorg and
  restart-with-corrupted-store are the historically dangerous paths; Pillar 3's startup
  audit must be observed running (and finding nothing) on every boot.
- **Kill soak:** repeated `kill -9` + restart cycles on one node to exercise the startup
  audit against genuinely torn state.

### Pass/fail signals

Pass — over the whole soak, on every node:

- `state.zakura.header_store.incoherent` == 0 after the canary's first clean startup
  (a nonzero *heal* early on is acceptable once — it means the audit repaired a store
  poisoned by the old binary — but must never recur after repair);
- zero `InvalidDifficultyThreshold` / `UnknownAnchor` header-commit failures for honest
  peers (watch `sync.header.stale_anchor.context_mismatch` — should stay near zero, vs.
  spiking during today's wedges);
- no header-sync park-rate growth vs. the pre-deploy baseline (capture per-node park
  counts before deploy; compare deltas, not absolutes);
- all nodes converge to identical tip hashes within normal propagation delay after every
  observed fork event (compare `getbestblockhash` across the fleet);
- clean `dod1_onboard.sh` verdicts on every run.

Fail — any of: a recurring incoherence heal (the audit repairing the *new* binary's own
writes), any peer scored/disconnected for a local context error, any node wedged below
the fleet tip for >10 min outside a burst.

### Evidence to keep

Per fork event: the trace jsonl window around it, the audit metric snapshot, and the
fleet tip-hash comparison. These go into the PR as test evidence (repo policy requires
it) and into `INVESTIGATION.md` as the closing entry for this bug class.

---

## 4. Definition of done

1. A deterministic test suite that provably violates today's store (the reliable repro),
   kept as regression gates.
2. Post-fix: the same suite green; property test sustained over large iteration counts.
3. Local-net reorg matrix (depths 1..N, DAA-window crossings, restarts mid-reorg) clean
   with audits enabled.
4. Testnet-4 fleet soak per §3a: canary then fleet-wide, through natural fork events,
   mass restarts, and onboarding runs, with all pass signals holding.
