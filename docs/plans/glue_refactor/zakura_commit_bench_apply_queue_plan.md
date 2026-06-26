# Zakura commit bench: offline apply-queue replay plan

## Goal

Make `zakura-commit-bench` isolate only the P2P/download transport while keeping the rest of the
block application path as close to production as possible.

The benchmark should pre-download real block bytes to disk, then replay them through the same
apply-side machinery used by Zakura block sync:

```text
cached block bytes
  -> decoded downloaded bodies
  -> block-sync sequencer/reorder/applying path
  -> applyQ
  -> zebrad Committer
  -> zebra_consensus::Request::Commit
  -> real zebra_state finalized commit
  -> durable frontier feedback back into the sequencer
```

The intended claim after this work is:

> Network transport and peer download latency are removed. Everything from "a body was received" to
> durable state commit uses the production block-sync apply path.

That is stronger than the current benchmark's claim, which is only:

> Network and block-sync queueing are removed. Consensus verification and state commit are real.

## Current state

`zakura-commit-bench fetch` is already the right artifact primitive:

- downloads raw serialized blocks with `getblock <height> 0`;
- validates each block deserializes and has the expected coinbase height;
- stores stable `*.bin` files under the cache directory.

`zakura-commit-bench run` currently:

- loads those cached blocks from disk;
- initializes real `zebra_state`;
- initializes a real `zebra_consensus::CheckpointVerifier`;
- submits blocks directly to the verifier with a local `FuturesUnordered` window.

So the current benchmark exercises real block bytes, real checkpoint verification, and real finalized
state writes. It does not yet exercise:

- `spawn_block_sync_reactor`;
- `SequencerTask`;
- the reorder buffer;
- `ApplyItem`;
- applyQ;
- the node-side `Committer`;
- `commit_block_sync_body_with_stall_trace`;
- durable frontier feedback into the sequencer;
- commit rejection/reset flow from the committer back to the sequencer.

## Target benchmark mode

Add an offline apply-queue replay mode to `zakura-commit-bench`. The mode should use the existing
on-disk cache for block bytes, but inject decoded blocks at the same logical boundary where peer
routines hand downloaded, hash-matched bodies into block sync.

The benchmark should keep the existing direct verifier mode for comparison, but the apply-queue mode
should become the default for "how fast can the full apply path process cached blocks?"

Proposed CLI shape:

```bash
cargo xtask zakura-commit-bench -- run \
  --mode apply-queue \
  --cache-dir target/zakura-commit-bench/blocks \
  --blocks 20000 \
  --trace-dir target/zakura-commit-bench/traces
```

Keep a direct mode for isolating verifier/state cost:

```bash
cargo xtask zakura-commit-bench -- run \
  --mode direct-verifier \
  --cache-dir target/zakura-commit-bench/blocks \
  --blocks 20000
```

## Implementation plan

### 1. Define the injection seam

Find the smallest production-safe API that lets the benchmark feed cached bodies into the existing
block-sync apply path without opening unrelated reactor/download internals.

The preferred seam is a benchmark/test-only body injector attached to `BlockSyncHandle` or a small
public replay adapter in `zebra-network::zakura::block_sync`. It should create the same internal
input the peer routine creates after it has received and matched a body.

The injected value needs the same information production has at that point:

- height;
- expected block hash;
- decoded `Arc<block::Block>`;
- serialized byte length;
- synthetic source peer id for attribution;
- received timestamp.

The injector should not invent a separate queueing model. It should feed the existing bounded
sequencer body input so body backlog, reorder draining, applying ledger accounting, and applyQ
behavior stay production-shaped.

### 2. Wire the real applyQ and Committer in the benchmark

In apply-queue mode, initialize the production components instead of the benchmark's local
`FuturesUnordered` loop:

- initialize state as the benchmark already does;
- initialize the real checkpoint/full verifier service used by block sync;
- spawn block sync with frontiers anchored at the benchmark state tip;
- take `BlockSyncHandle::take_apply_queue()`;
- construct the real `zebrad` `Committer` over that queue;
- run the committer concurrently with the replay driver;
- feed cached blocks through the injection seam in contiguous height order.

The benchmark should still support hydrated snapshots. For snapshot runs, the injection starts at
`finalized_tip + 1`, matching the existing direct-verifier behavior.

### 3. Drive durable frontier feedback

The production apply path releases bytes only after the durable frontier crosses a held height. The
offline replay must preserve that rule.

Use the same durable-tip feedback path as block sync:

- subscribe to the state chain-tip/finalized-tip change signal;
- translate advances into `SequencerControlInput::FrontierAdvance`;
- make sure the sequencer releases applied bytes only on durable advance;
- avoid per-block state reads on the commit hot path.

This is the key correctness condition for memory/backpressure fidelity. If the benchmark releases
bytes when the committer future resolves, it will overstate throughput and understate memory
pressure.

### 4. Preserve checkpoint batching behavior

The committer must fire commits without awaiting each one serially. The checkpoint verifier only
resolves a block after enough contiguous blocks have been submitted to complete the checkpoint range.

The benchmark should assert this property with at least one smoke range that spans a checkpoint
boundary. A serial apply loop is a benchmark bug because it can deadlock or measure an impossible
production flow.

### 5. Re-port optional root fast path through header commit state

The old `--with-roots` path fed roots through the removed `tree_aux` peer-source writer. Do not
restore that path.

For production-faithful fast-path benchmarking, roots should reach the committer the same way they do
now in production:

- fetch or synthesize the header-sync commitment-root metadata needed by `CommitHeaderRange`;
- persist roots into `zakura_header_commitment_roots_by_height`;
- verify `state.vct.fast_path.hit` increases during replay.

This can be a second phase. The first apply-queue replay should work without roots and should report
that VCT fast-path coverage is unavailable unless header metadata was preloaded.

### 6. Keep artifacts reproducible

Use these artifact roots:

```text
target/zakura-commit-bench/blocks              # repo-local default cache
target/zakura-commit-bench/traces              # repo-local default traces
/home/evan/src/valar/art/debug/benchmark/glue # shared runbooks, copied traces, snapshots
```

The shared artifact directory should contain run instructions, endpoint notes, copied summaries, and
large snapshot references. Source changes should stay in the repo.

## Validation gates

### Smoke

- Fetch or reuse cached blocks for at least one full checkpoint range.
- Run apply-queue mode for 401 blocks from genesis.
- Confirm all blocks commit and no reset/misbehavior is emitted.
- Confirm the trace contains applyQ/committer rows, not only benchmark rollups.

### Direct comparison

Run both modes over the same cached block range:

- `--mode direct-verifier`;
- `--mode apply-queue`.

Expected result: apply-queue mode may be slightly slower due to production queueing and tracing, but
it should commit the same heights and land at the same finalized tip.

### Snapshot range

- Hydrate from a Zebra snapshot.
- Fetch a range above the snapshot tip.
- Run apply-queue mode across at least one checkpoint boundary.
- Confirm the final state tip equals the last committed checkpoint boundary.

### Budget fidelity

During apply-queue replay, confirm:

- bytes are held while blocks are in reorder/applying/applyQ/state commit;
- bytes are released only after durable frontier advance;
- body input and applying lengths are visible in `SequencerView`;
- no unbounded memory growth occurs when the verifier/state writer is slower than injection.

### Failure path

Add a controlled invalid-body test or benchmark smoke mode:

- corrupt one cached body after it has a valid height/hash expectation;
- confirm the committer reports `CommitterReset`;
- confirm the sequencer rolls back the failed height and successors;
- confirm an invalid-body misbehavior action is emitted against the synthetic source peer;
- confirm stale sibling failures are coalesced by epoch.

## Risks and decisions

- The cleanest injection seam may require a small `zebra-network` API. Keep it narrow and clearly
  marked as replay/test/benchmark support.
- If `zebrad`'s `Committer` remains `pub(crate)`, the benchmark cannot construct it directly from a
  separate crate. Prefer extracting the minimum reusable committer wrapper or exposing a narrow
  benchmark-facing constructor rather than duplicating the committer in `zakura-commit-bench`.
- Avoid depending on unstable private reactor internals from the benchmark crate. A narrow adapter is
  easier to review than a benchmark that reaches through several private modules.
- Do not make the benchmark responsible for peer scheduling. The replay is intentionally post-download:
  it should model "bodies arrived from disk instantly or at a configured rate", then measure the
  production apply path.

## Completion criteria

This work is done when:

- `zakura-commit-bench run --mode apply-queue` replays cached blocks through `SequencerTask`,
  applyQ, real `Committer`, real verifier, and real state;
- direct-verifier mode remains available as a lower-level comparison;
- the README and artifact runbook explain both modes and when to use each;
- smoke and crate tests pass;
- traces make it clear whether a run measured direct verifier submission or full apply-queue replay.
