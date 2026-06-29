# zebra-replay-bench — commit-pipeline results

Offline A/B of the Zebra checkpoint-sync pipeline at four abstraction levels: the
**direct committer** (`apply` → `FinalizedState::commit_finalized_direct`), the
**write worker** one rung up (`apply-worker` → `BlockWriteSender::spawn` /
`WriteBlockWorkerTask::run`), the **checkpoint verifier** above that
(`apply-verifier` → `zebra-consensus::CheckpointVerifier`, committing to a real
`StateService`), and the **block-sync Sequencer** above that (`apply-sequencer` →
`zebra-network`'s Zakura `Sequencer`, VCT-only). Goal: isolate each layer's cost
and find the next optimization lever.

## Provenance

- **Base:** branched from `feat/pre-release-main`, rebased onto `087428377`
  (#295). Measurements were taken on the equivalent code at `c787831c0` (the #305
  commit that introduced the bench); the only base delta since is the
  behavior-preserving `disable_vct_fast_sync` → `vct_fast_sync` config rename, so
  the numbers are unchanged.
- **Changes measured:** the `apply-worker` extension (`apply_worker.rs` +
  subcommand), the bounded `prefetch.rs` producer, the `apply.rs` commit-only
  refactor, and the zebra-state worker-type exports (`BlockWriteSender`,
  `QueuedCheckpointVerified`).

## Environment

- 8 cores (DO Premium Intel), 31 GiB RAM, Linux.
- Snapshots under `/mnt/roman-dev-2-data`: base = `zebra-ckpt-1800000-warm`
  (archive 27.3.0, tip 1,802,000); block + root source = `zebra-cache`
  (tip 3,376,789).
- Window: heights **1,802,001–1,832,000** (30,000 blocks, ~19.1 GiB of bodies);
  VCT roots sidecar derived from the same source over the same window.
- Forward replay, no rollback. Each run executes on a fresh hard-link fork of the
  base; the final tip-hash gate (byte-identical to the source snapshot tip
  1,832,000) passes on every run.

## Methodology — commit-only isolation via a bounded prefetch

The committer's input — a `CheckpointVerifiedBlock` — is built by `prepare_block_data`
(`zebra-state/src/request.rs`): per-tx txid + ZIP-244 auth digest, the auth-data
Merkle root, and the new-outputs (UTXO) map. That is **verifier-side prep** that
runs upstream of the committer in production, so it must not be in the timed
window of either bench.

Both benches stream the window through a shared **bounded prefetch** (`prefetch.rs`):
a producer thread reads the cache, deserializes, and builds each
`CheckpointVerifiedBlock` ahead of the committer, into a bounded channel
(capacity 64). The timed consumer only commits. This:

- keeps block read + parse + prep **off** the timed commit thread (commit-only),
- bounds memory to the channel depth (~0.9 GiB here) regardless of window size, so
  the bench scales to 100K+ without holding the window in RAM, and
- mirrors production (verifier prepares blocks upstream of the writer).

`apply` (direct) keeps a one-block look-ahead for the VCT successor and times each
`commit_finalized_direct`. `apply-worker` feeds the prefetched blocks into the
real write worker with a **bounded in-flight window** (≤64 sent-but-not-committed),
which also prevents dumping a 30K backlog into the worker's unbounded channel.

## Results — VCT, 30K, commit-only

| run | wall | throughput | p50 | p90 | p99 | max | peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direct (`apply --vct-sidecar`) | 235.0 s | **127.7 blk/s** (81.2 MiB/s) | 4.48 ms | 14.09 ms | 77.70 ms | 611 ms | 0.84 GiB |
| worker (`apply-worker --vct-sidecar`) | 242.3 s | **123.8 blk/s** (78.8 MiB/s) | 4.47 ms | 15.69 ms | 85.52 ms | 504 ms | 0.95 GiB |

Both committed all 30,000 blocks to the identical tip hash.

**Direct and worker converge** (127.7 vs 123.8 blk/s, identical p50). Once
`prepare_block_data` is pipelined off the commit thread on both sides, the write
worker has **no** commit-only throughput advantage over the direct committer in
VCT mode — it is marginally slower from the channel/oneshot plumbing. The worker's
value (look-ahead note-commitment precompute) is switched off in VCT because the
recompute it would overlap is eliminated; the residual is just coordination cost.

### How the measurement converged (direct, VCT, what's in the timed wall)

| what the timed wall includes | throughput | peak RSS |
| --- | ---: | ---: |
| block read + parse + prep + commit (original `apply`) | 99.6 blk/s | — |
| prep + commit (window pre-loaded, CV built in the loop) | 121.2 blk/s | — |
| commit only (all CVs pre-built up front, no contention) | 148.0 blk/s | 20.7 GiB |
| commit only (bounded prefetch: prep concurrent, flat memory) | 127.7 blk/s | 0.84 GiB |

Excluding parse and prep raised direct from 99.6 → 148.0 (a pure committer with no
other work running). The bounded prefetch gives back some of that (148 → 127.7)
because the producer's `prepare_block_data` (rayon, multi-core) now runs
**concurrently** and contends for cores with the committer — but it is the
scalable, production-like design (prep always runs alongside commit in a real
node) and it bounds memory to ~0.84 GiB, so it is the figure used for the A/B.

## Prefetch depth: contention, not lookahead

Sweeping the prefetch depth (`ZRB_PREFETCH_CAP`) on VCT direct shows deeper
buffering does **not** help — it is flat within run-to-run noise:

| prefetch depth | throughput | peak RSS |
| ---: | ---: | ---: |
| 64 (default) | 127.7 blk/s | 0.84 GiB |
| 2048 | 131.0 blk/s | 2.84 GiB |
| 16384 | 120.2 blk/s | 8.2 GiB |
| all pre-built (no concurrent producer) | 148.0 blk/s | 20.7 GiB |

Depth 64 already keeps the committer fed (no buffer-starvation stalls for a bigger
lookahead to remove). The only configuration that reaches 148 is pre-building the
whole window so **no** producer runs during timing — i.e. the committer gets all 8
cores. The ~15% gap is CPU contention from running `prepare_block_data` (rayon,
multi-core) concurrently with the commit, which is inherent to a streaming
pipeline and realistic for production. So depth 64 (flat ~0.9 GiB) is the right
operating point; the lever to approach 148 would be capping the producer's core
usage, not deepening the buffer.

## Worker in-flight bound also removed a write-stall

With the unbounded feed (all 30K pushed up front), the worker's max commit latency
was 37,279 ms — a RocksDB L0 write-stall from the backlog. The bounded in-flight
window (≤64) removes it: max latency is now 504 ms, in line with the direct bench.

## Third rung: checkpoint verifier (`apply-verifier`)

`apply-verifier` drives blocks through the real `zebra-consensus::CheckpointVerifier`,
which internally commits to a real `StateService` (→ write worker → committer) on a
multi-thread tokio runtime. It adds the per-block work the lower rungs skip:
proof-of-work (difficulty + equihash) and Merkle-root validity, plus
checkpoint-range batching.

The boundary: the verifier only releases a block once its whole checkpoint range is
contiguous, and the worker's VCT fast path can't commit a block until its successor
is buffered (the one-block-lag root authentication). The final checkpoint's successor
is in the dropped tail, which the verifier never releases — so the bench **feeds** up
to the last checkpoint `<= end` (so the last range delivers the successors the worker
needs) but **counts/gates** to the second-to-last checkpoint, whose committed hash is
checked against the embedded checkpoint hash.

| run (30K) | committed to | throughput | p50 | peak RSS |
| --- | ---: | ---: | ---: | ---: |
| legacy | ckpt 1,831,990 | **49.9 blk/s** | 13.99 ms | 2.35 GiB |
| VCT | ckpt 1,831,959 | **133.0 blk/s** | 4.04 ms | ~2.4 GiB |

The legacy row was measured before the second-to-last-checkpoint gate (legacy needs
no successor, so it commits the full window and its rate is unaffected by that fix).

**Verification overlaps the commit for free, in both modes.** Legacy verifier ≈ the
legacy commit-alone rate (~50 blk/s): the rayon-parallel equihash + Merkle work
overlaps the single-threaded note-tree recompute that bottlenecks legacy. VCT
verifier (133 blk/s) is actually _slightly above_ the VCT worker (123.8) and direct
(127.7) — with the recompute gone the committer is cheap, yet the concurrent
verify→commit pipeline still hides verification behind it. So at this window,
checkpoint verification is **not** a throughput bottleneck on top of the commit.

> Diagnosis note: an earlier draft reported VCT here as "stall-bound / slower than
> legacy." That was wrong — a one-block bench bug (the final checkpoint block had no
> successor delivered, so it hung forever while everything else committed at ~500
> blk/s). Adding a periodic progress log (fed/done/front-height) pinned it to exactly
> that block; the second-to-last-checkpoint gate fixes it. Lesson: instrument before
> concluding.

## Fourth rung: block-sync Sequencer (`apply-sequencer`, VCT POC)

`apply-sequencer` drives blocks through the **real Zakura block-sync `Sequencer`**
(`zebra-network`, via a feature-gated `spawn_bench_sequencer` helper): bodies are fed
into its reorder queue, it drains the contiguous prefix into `applying` and emits
ordered `SubmitBlock`s, and a thin driver commits each through the same real
`CheckpointVerifier` → `StateService`, reporting the commit back so the frontier
advances. VCT-only; for this POC bodies are fed **in height order** (random / multi-peer
out-of-order arrival — the reorder buffer's real stress case — is a future knob).

**VCT, 30K** (committed 29,959 blocks to checkpoint 1,831,959, hash gated):
**123.5 blk/s**, p50 4.71 ms, peak RSS 5.7 GiB. That lands right on the worker (123.8)
and ~7% under the verifier (133.0): the sequencer adds the body-reorder + submit/apply
control plumbing on top of verify+commit, and on an **in-order** feed the reorder does
no real work (`reorder_len` stays 0), so the gap is the channel hops + apply-finished
round-trips, not reordering. The higher RSS is the `applying` buffer filling under the
bench's unbounded byte budget (it drains by end-of-window); a finite budget would cap it.

The interesting measurement is still ahead: feeding bodies **out of order** (the future
knob) is what actually exercises the ReorderBuffer and the sequencer's backpressure —
this POC establishes the in-order baseline and that the real `SequencerTask` drives
cleanly offline.

## Open / next

- **Out-of-order / multi-peer feed for `apply-sequencer`:** the POC feeds in height
  order, so the reorder buffer is idle. Shuffling within a window (and simulating
  per-peer arrival) is what measures the sequencer's actual job.

- **Larger windows (100K+)** are unblocked for the committer/worker/verifier rungs:
  memory is flat (verifier peak ~2.4 GiB), so cache size, not RAM, is the limit.
  (The sequencer rung needs a finite byte budget first — see its RSS note.)
- **Profiling the committer floor:** with all three rungs converging in VCT
  (~124–133 blk/s) and verification shown to overlap for free, the committer remains
  the floor. The `prepare`/`update_trees`/`batch_prep`/`rocksdb.batch_commit`
  histograms (`--features commit-metrics`) are the next lever.
