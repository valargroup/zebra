# Committer / sync throughput optimization

Where the checkpoint-sync throughput bottleneck actually is, the three highest-impact
improvements, and one architectural recommendation. Grounded in instrumented runs over the
sandblast region (~1.7M), not inference.

## The measured bottleneck (steady-state, blocks 1.715M–1.728M)

The finalized **committer is the binding constraint** — confirmed by direct utilization +
queue-depth instrumentation, not guessed from per-phase profiling:

| signal | value | reads as |
| --- | --- | --- |
| committer utilization | **89% busy** | the committer is the gate, not idle |
| committer input queue depth | **937 blocks** backed up | upstream delivers faster than it commits |
| poll-empty fraction | 13% | rarely starved for input |
| commit time / block | 12.98 ms (~77 blk/s capacity) | — |
| update_trees (within commit) | 8.98 ms = **69% of the commit** | the dominant slice |
| equihash / merkle (serial verifier) | 0.42 / 0.03 ms | feed/verifier ruled out |
| download rate | ~60 blk/s | the *next* gate, just behind |
| throughput | 68.3 blk/s | committer draining its buffer |

Key facts:
- The single-threaded committer does, per block in order: note-commitment tree update +
  write-batch build + RocksDB write + history-tree push. Tree update is **69%** of it.
- The "feed" (download → verify) is **not** the bottleneck here: the serial verifier
  (equihash + merkle) is ~0.5 ms, and blocks are backed up 937-deep at the committer's input.
- The committer's capacity (~77 blk/s) is only slightly above the **download rate (~60 blk/s)**,
  so once the committer is sped up, the gate shifts to download bandwidth. The two are close,
  which is why the bottleneck kept appearing to move between runs (it depends on how fast blocks
  are being delivered, which varies with peers/conditions).

Earlier confusion (recorded for honesty): a first A/B of improvement #1 showed flat throughput,
because that run happened to be in a download-limited regime (committer had slack). Per-phase
profiling tells you where time goes *within* a stage; only utilization/queue-depth instrumentation
(or a controlled A/B in the right regime) identifies the binding stage. The numbers above are from
that instrumentation.

## Top 3 highest-impact improvements (ranked)

### 1. Note-commitment tree precompute off the committer — highest ROI, already built (PR #144)
Move the tree's per-leaf Merkle hashing (Pedersen/Sinsemilla) off the serial committer: precompute
it ahead of time, keyed only on the cumulative note count, concurrently across many blocks on the
idle cores; the committer then only "grafts" the precomputed subtree roots (O(log N)).
- Cuts `update_trees` ~9 ms → ~4 ms, i.e. removes ~69% of the committer's per-block cost; committer
  capacity ~77 → ~120 blk/s.
- Validated byte-identical to the inline append (differential proptests); env toggle for A/B.
- Status: implemented and PR'd against `sync-perf-main-2` (draft). Attacks the proven gate directly.

### 2. Shrink the committer's *remaining* work: multi-block RocksDB commit + overlap the DB write
After #1, the committer's cost is dominated by the write path (batch build + RocksDB write +
history push, ~4 ms). Commit several blocks per RocksDB write batch (amortize per-commit overhead,
which grows with DB size), and overlap block N's disk write with block N+1's prepare.
- Pushes the committer toward the rocksdb-write floor; compounds with #1.
- Note (from a separate investigation): RocksDB had **zero write stalls** and the WAL is async, so
  the win here is fewer/larger writes and less memtable-insert overhead, *not* WAL removal.

### 3. Raise the download ceiling for large sandblast blocks (~60 blk/s — the next gate)
Once the committer is no longer the gate, download bandwidth (~60 blk/s) is the steady-state limit.
`in_flight` sits ~1026 (below the 1500 cap) yet completes only ~60/s → ~17 s effective per-block
latency: latency/concurrency-bound, not capped. More concurrent block-body requests, better peer
selection, and pipelined body fetch raise the durable ceiling.
- Medium-high ROI because it is the *steady-state* limiter after #1 and #2.

## Architectural recommendation: parallel-prepare / thin-serial-commit

The structural ceiling is that the finalized committer is a single serial thread doing
tree-update + batch-build + RocksDB-write + history-push per block, in order. Re-architect the
finalized commit into two stages:

- **Prepare (parallel, many blocks ahead, off the critical path):** everything that depends only on
  the block and its position, not on the live DB write — tree hashing (#1 does this), write-batch
  build, serialization, address/UTXO index prep.
- **Commit (serial, minimal):** only the strictly-ordered work — the atomic RocksDB write and tip
  advance.

This is the correct version of the idea behind the parked "any-order commit pipeline" prototype
(PR #129). #129 split at the wrong seam (it overlapped the *tree compute* with the write) and was
measured when the box was CPU-saturated (~7.75/8), so it showed no gain. After the crypto wins the
box runs at ~3/8 (5 idle cores), and #1 makes the tree compute nearly free — so the right seam is
**prepare ‖ serial-write**, not tree-compute ‖ write.

With prepare fully parallelized and commit reduced to the RocksDB write + multi-block batching, the
serial committer shrinks several-fold and the system-wide bottleneck moves cleanly to **download
bandwidth** — the honest physical floor for chain sync (you cannot validate faster than you fetch).

**Direction:** #144 vs #129 is not a real choice — #144 is the better mechanism (it *reduces* the
dominant cost rather than redistributing it, and it makes #129's specific overlap moot). Land #144,
then pipeline the *write* (not the tree), then attack downloads. One-liner for the team: *#144
removes the bottleneck; #129 only rearranged it. Land #144, then pipeline the write, not the tree.*

## Suggested sequencing

1. Merge #144 → re-measure; the committer gate should narrow and shift toward downloads.
2. Add multi-block commit batching + write/prepare overlap (improvement #2).
3. Decide between further committer work vs download parallelism based on which is then closer.
