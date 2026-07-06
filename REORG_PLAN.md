# Zakura reorg corruption — repro & fix plan

Status: DESIGN (2026-07-06). Companion to `INVESTIGATION.md` (esp. §11c–§15).

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

## 2. Solution design

Not a patch to delete-loop bounds — a restructure of ownership so the invariants hold by
construction. Three pillars, independently landable, each gated on the Layer-1 suite.

### Pillar 1 — one owner, one mutation primitive

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

### Pillar 1a — switch orchestration: evaluate the candidate before mutating anything

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
| 1 | Layer-1 harness: oracle, op alphabet, audit, scripted scenarios, proptest | ~1 day | ≥1 shrunk failing test; corrupting writer(s) identified |
| 2 | Pillar 2: linkage-verified reads, `StoreIncoherent` error | ~0.5 day | Layer-1 suite green on read behavior; poison de-fanged |
| 3 | Pillars 1 + 1a: `set_canonical_suffix`, switch orchestration, route all writers through them | 1.5–2 days | All Phase-1 counterexamples pass; full suite green incl. body-backed switch scenarios |
| 4 | Pillar 3: runtime audit + truncate-and-resync repair | ~1 day | Fault-injection test: corrupt store heals on startup |
| 5 | Layer 2 + Layer 3 confirmation, then fleet soak | ~1 day + soak | Local-net reorg matrix clean; fleet soak clean |

Each phase is a separate reviewable change; Phase 1 lands as tests only (safe by
construction). Phases 2–4 are consensus-adjacent and get the senior-review protocol.

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
