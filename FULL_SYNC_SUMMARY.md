# Full mainnet sync analysis (genesis → tip)

A full Zcash mainnet sync from genesis to the chain tip, profiled per phase with the
`commit-metrics` instrumentation, on the optimized binary and an 8-core box. This document
breaks the sync down by height range and lists the major bottlenecks.

## Binary and methodology

- **Binary:** `zebrad-readpar` — the proto optimization stack: native ZIP-244 digests, dropped
  v5-deserialize reparse, lazy Sapling cv/epk point decompression, parallel block writer, the
  #138 serialization gate, and the #140 committer read parallelization.
- **Run:** genesis → tip in a fresh state dir. Reached the max checkpoint (3,358,006) and continued
  through semantic verification to the tip (~3.382M). One disk-full interruption around 1.79M was
  resumed in place (RocksDB recovered); the per-block phase metrics below are committer-thread
  timers and are independent of that interruption and of peer/download luck. Throughput (blk/s) is
  peer-dependent and is reported only as a secondary signal.
- **Phase columns (ms/block):** `prep` = UTXO/address reads before the batch; `tree` = note-commitment
  tree update; `batch` = write-batch build; `rocks` = RocksDB commit; `wbt` = total DB-write
  (prep+batch+rocks+tip). `tree` runs concurrently with the write, so it is reported separately.

## Timing

| segment | blocks | wall time | avg blk/s |
| --- | --- | --- | --- |
| genesis → 1.79M (checkpoint) | 1.79M | 3.37 h | ~148 |
| 1.79M → tip (incl. resume stalls + semantic tail) | ~1.59M | 4.20 h | 105 |
| of which: semantic tail (> max checkpoint 3.358M) | ~24.6K | 0.64 h | **11** |

The semantic tail (above the last checkpoint) is full validation — proofs and signatures — at
~11 blk/s, CPU ~1.6/8. Every optimization in this work targets the checkpoint region below 3.358M;
the tail is a different, fundamentally slower regime.

## Per-100K breakdown (genesis → 3.2M)

| range | blk/s | cpu/8 | prep | tree | batch | rocks | wbt | tx/blk | dominant |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 100k | 90 | 2.8 | 2.64 | 0.04 | 3.32 | 4.40 | 10.38 | 8.7 | rocksdb |
| 200k | 66 | 3.1 | 3.75 | 0.05 | 4.50 | 6.04 | 14.30 | 14.3 | rocksdb |
| 300k | 72 | 3.3 | 3.52 | 0.04 | 4.89 | 4.65 | 13.07 | 11.1 | batch_prep |
| 400k | 239 | 3.4 | 1.05 | 0.64 | 0.71 | 1.28 | 3.05 | 5.9 | rocksdb |
| 500k | 210 | 3.3 | 1.15 | 1.08 | 0.77 | 1.19 | 3.11 | 8.0 | rocksdb |
| 600k | 254 | 3.6 | 0.93 | 0.95 | 0.63 | 0.96 | 2.52 | 5.1 | rocksdb |
| 700k | 358 | 3.3 | 0.52 | 0.94 | 0.29 | 0.54 | 1.35 | 4.1 | tree |
| 800k | 279 | 3.2 | 0.75 | 1.31 | 0.35 | 0.66 | 1.77 | 4.9 | tree |
| 900k | 259 | 3.1 | 0.71 | 1.21 | 0.39 | 0.83 | 1.94 | 5.2 | tree |
| 1000k | 243 | 3.1 | 0.80 | 1.30 | 0.43 | 0.91 | 2.15 | 4.7 | tree |
| 1100k | 203 | 3.3 | 1.13 | 1.34 | 0.61 | 1.19 | 2.94 | 5.6 | tree |
| 1200k | 216 | 3.1 | 0.99 | 1.07 | 0.56 | 1.22 | 2.78 | 5.1 | rocksdb |
| 1300k | 214 | 3.3 | 1.18 | 1.05 | 0.51 | 1.17 | 2.87 | 4.3 | prep_reads |
| 1400k | 209 | 3.0 | 0.81 | 2.02 | 0.41 | 0.85 | 2.09 | 5.7 | tree |
| 1500k | 229 | 3.2 | 0.84 | 1.41 | 0.56 | 0.95 | 2.36 | 4.1 | tree |
| 1600k | 257 | 3.2 | 0.67 | 1.33 | 0.40 | 0.74 | 1.81 | 5.2 | tree |
| **1800k** | **38** | 5.3 | 2.18 | **17.60** | 1.59 | 3.96 | 7.74 | 4.2 | **tree** |
| **1900k** | **55** | 5.2 | 1.08 | **12.67** | 0.87 | 2.55 | 4.51 | 4.3 | **tree** |
| **2000k** | **64** | 4.2 | 0.97 | **11.34** | 0.67 | 1.87 | 3.52 | 5.4 | **tree** |
| 2100k | 100 | 2.9 | 0.63 | 7.43 | 0.38 | 0.84 | 1.86 | 4.0 | tree |
| 2200k | 158 | 2.6 | 0.78 | 3.66 | 0.41 | 0.87 | 2.07 | 3.5 | tree |
| 2300k | 360 | 3.0 | 0.35 | 1.35 | 0.18 | 0.32 | 0.86 | 2.7 | tree |
| 2400k | 282 | 2.8 | 0.46 | 1.73 | 0.22 | 0.40 | 1.09 | 2.9 | tree |
| 2500k | 143 | 2.6 | 0.70 | 4.49 | 0.36 | 0.79 | 1.86 | 3.8 | tree |
| 2600k | 149 | 2.7 | 0.73 | 3.94 | 0.36 | 0.85 | 1.95 | 3.2 | tree |
| 2700k | 305 | 2.8 | 0.32 | 1.94 | 0.16 | 0.26 | 0.74 | 2.1 | tree |
| 2800k | 351 | 2.9 | 0.21 | 1.73 | 0.12 | 0.21 | 0.55 | 2.0 | tree |
| 2900k | 301 | 2.6 | 0.22 | 2.18 | 0.12 | 0.20 | 0.56 | 2.0 | tree |
| 3000k | 217 | 2.5 | 0.46 | 2.99 | 0.18 | 0.28 | 0.93 | 3.0 | tree |
| 3100k | 133 | 2.5 | 0.78 | 5.00 | 0.54 | 0.57 | 1.90 | 8.9 | tree |
| 3200k | 169 | 2.5 | 0.44 | 4.29 | 0.23 | 0.41 | 1.10 | 5.3 | tree |

(At 5K granularity the sandblast peak is sharper still: tree update hits ~39 ms/block around 1.875M.)

## The four regimes

1. **Transparent band (~100–330K):** the slowest pre-sandblast stretch, 66–90 blk/s, `wbt` 10–14 ms.
   Many transparent inputs/outputs per block. Dominated by **rocksdb commit + batch_prep**; `prep_reads`
   here is already cut to 2.6–3.7 ms by #140 (was ~25 ms / 58% of wall before it).
2. **Post-Sapling low-tx (~400–650K):** fast, 210–254 blk/s, everything small; rocksdb the largest slice.
3. **Shielded era (~700K–1.6M and ~2.3M onward):** 130–360 blk/s; **note-commitment tree update** is the
   dominant phase as Sapling/Orchard notes accumulate (~1–5 ms).
4. **Sandblast region (~1.7M–2.2M):** the slowest part of the whole chain, 38–158 blk/s. The spam created
   huge numbers of shielded outputs, so the **note-commitment tree update explodes to 11–18 ms/block**
   (peaking ~39 ms at 5K granularity). CPU rises to ~5/8 here as the parallel tree append engages — yet it
   is still the bottleneck because the note volume overwhelms it.

A constant across every range: **CPU sits at ~2.5–3.5/8** (rising to ~5/8 only in sandblast). The committer
is a single serial critical path, leaving ~3–5 cores idle nearly everywhere — which is why moving work off
that thread (rather than parallelizing within it) is the recurring lever.

## Major bottlenecks (ranked)

1. **Note-commitment tree update — the #1 cost.** Dominant for the entire shielded half of the chain
   (~700K → tip) and catastrophic in sandblast (11–18 ms/block, ~39 ms peak). Already internally
   parallelized; the lever is to move its per-leaf Pedersen/Sinsemilla hashing off the serial committer.
   *(Optimization implemented — see below.)*
2. **RocksDB commit — the transparent-band ceiling.** 4.4–6 ms/block at 100–300K and the largest slice in
   the low-tx span; grows with DB size. Evidence (live `rocksdb.LOG`): zero write stalls and the WAL is
   async, so the cost is memtable insertion, not I/O. PR #90's WAL-skip targets a near-absent cost here;
   the real levers are multi-block batch commits and/or pipelining the commit. *(Indexed for later.)*
3. **Serial committer / idle cores — structural.** CPU ~3/8 everywhere; one thread gates throughput while
   most cores idle. Underlies both #1 and #2.
4. **prep_reads — transparent-input UTXO/address reads.** Was 58% of wall (~25 ms) at 340K; now 2.6–3.7 ms
   after #140 (parallel + de-duplicated reads). Largely resolved.
5. **Semantic verification tail (> max checkpoint 3.358M).** ~11 blk/s, full proof/signature validation.
   Out of scope for checkpoint-sync optimization; inherently slow.

## Improvements validated and shipped this work

- **#138** — par_iter size gate (don't fork-join tiny blocks): batch_prep −8 to −13%.
- **#140** — parallelize + de-duplicate the committer's UTXO/address reads: **prep_reads −55 to −68%**,
  write_block_total −25 to −37% across the transparent band; this flattened regime 1's `prep_reads`.
- **Note-commitment tree precompute (implemented, A/B pending)** — splits the tree append into an
  off-committer `precompute_subtree_roots` (the heavy hashing, keyed only on note count) and a cheap
  on-committer `graft`, driven by a 1-block look-ahead so the hashing overlaps the previous commit on idle
  cores. Byte-identical to the sequential append (differential proptests), with a size-match fallback so it
  can only affect speed, never correctness. Targets bottleneck #1.

## Remaining levers

- **Tree precompute** (above) — pending throughput A/B in the sandblast (1.8–2.2M) and shielded ranges.
- **Multi-block RocksDB commit batching** — bottleneck #2, the transparent-band and low-tx ceiling.
- **Commit pipelining** — overlap block N's commit with block N+1's prep/reads on the idle cores.
