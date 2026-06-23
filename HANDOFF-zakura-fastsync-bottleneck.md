# Zakura v2 Fast-Sync Bottleneck Investigation — Engineering Handoff

_Date: 2026-06-23. Author: perf investigation session._

## 1. Objective & setup

Profile a single Zebra node doing a **Zakura v2 fast-sync (verified-commitment-trees)**
from genesis on Mainnet, identify the per-region bottleneck, and find why throughput is
far below the link capacity.

- **Binary:** `perf-note-commit-tree` tip (`fc0516fbd`, includes `#199` non-blocking
  block-sync reactor, `#215`/`#216` VCT changes) + a private instrumentation commit.
  Built in an isolated worktree: `/root/zebra-vct-build` (branch
  `perf/vct-bottleneck-instr-tip`). Binary: `/root/zebra-vct-build/target/release/zebrad`.
- **Node config:** `/mnt/roman-dev-2-data/vct-test/cfg-genesis-instr.toml`
  - `v2_p2p=true, legacy_p2p=false` (Zakura block-sync only)
  - `storage_mode = "pruned"` (tx_retention 10000) — **forced** (see §5.3)
  - `[network.zakura.block_sync] max_inflight_block_bytes = 4294967296` (4 GiB; was 1 GiB)
  - cache_dir `/mnt/roman-dev-2-data/vct-test/run-genesis`, metrics `127.0.0.1:9990`,
    trace_dir `/mnt/roman-dev-2-data/vct-test/trace-genesis`
- **Peers:** 7 remote DigitalOcean Zakura nodes (`:8234`), bootstrap list in `env.sh`.
- **Samplers:** `scripts/results-table.sh` (per-100k table → `logs/results-table.md`),
  `scripts/bottleneck-sampler.sh` (detailed per-window → `logs/bottleneck.log`).

## 2. Environment / infra (answers to the direct questions)

- **Our node datacenter:** DigitalOcean **Santa Clara, California (SFO)**, AS14061,
  external IP `157.245.239.19`.
- **Link bandwidth capacity (measured):** `curl` of a 100 MB file from CacheFly =
  **838 MB/s ≈ 6.7 Gbps**. (DO virtual NIC reports no fixed speed.)
- **Bandwidth actually utilized by the sync:** **~10–16.7 MB/s peak (~80–130 Mbps)** —
  i.e. **~1–2% of the link.** The sync is nowhere near link- or config-limited.
- **Peer RTTs are wildly heterogeneous** (this matters, see §5.5):
  `143.244.184.176 = 1.24 ms` (same DC, Santa Clara), `159.65.183.89 = 74 ms`,
  `64.227.44.93 = 139 ms`, `161.35.156.226 = 149 ms`, `139.59.64.115 = 222 ms`.

## 3. Instrumentation added (private commit on the worktree branch)

Always-on monotonic `*.nanos` counters (cheap; `Instant::now()` pair per phase) that
decompose the serial committer's wall-clock, plus root-fetch timing. Exposed on the
Prometheus endpoint (dots → underscores).

| counter | location | meaning |
|---|---|---|
| `vct.commit.wait_input.nanos` | write.rs | committer idle (starved on upstream) |
| `vct.commit.verify.nanos` | finalized_state.rs | ZIP-221 commitment / auth-data check |
| `vct.commit.cpu_tree.nanos` | finalized_state.rs | note-commitment tree update (≈0 on fast path) |
| `vct.commit.write_block.nanos` | finalized_state.rs | whole DB-write stage |
| `vct.commit.db_commit.nanos` | block.rs | **pure RocksDB write** (I/O) |
| `vct.commit.wb.reads.nanos` | block.rs | spent-UTXO + address-balance **reads** |
| `vct.commit.wb.rawtx.nanos` | block.rs | raw-tx archive serialization |
| `vct.commit.wb.transparent.nanos` | block.rs | UTXO set + transparent address indexes |
| `vct.commit.wb.shielded.nanos` / `wb.trees.nanos` | block.rs | nullifiers / tree batch |
| `vct.root.fetch.nanos` / `vct.root.fetched.count` | zakura/tree_aux/driver.rs | root download |

`serialize = write_block - db_commit`; `serialize ≈ reads + transparent + rawtx + …`.

## 4. Metrics & data sources used

- **Prometheus (`:9990`)**: `zcash_chain_verified_block_height` (commit tip/rate),
  `sync_header_best_tip_height`, `sync_block_applying`, `sync_block_outstanding`,
  `state_vct_fast_block_count` / `state_vct_legacy_block_count`, `zakura_p2p_conn_active`
  / `zakura_p2p_conn_closed_neutral`, `checkpoint_verified_block_count`,
  `checkpoint_waiting_count`, plus all `vct.commit.*` / `vct.root.*` above.
- **Zakura trace (`trace-genesis/block_sync.jsonl`)**: `block_sync_state` rows
  (`received_bytes_per_sec`, `received_blocks_per_sec`, `committed_blocks_per_sec`,
  `download_blocked_on_budget`, `budget_reserved`, `outstanding`, `reorder`,
  `reorder_buffered_bytes`, `floor_gap_*`, `applying`, `request_slot_available`,
  `peers_wanting_slots`); per-event `block_body_received` / `block_get_blocks_sent`
  (`peer`, `request_elapsed_ms`, `serialized_bytes`, `height`).
- **System**: `/proc/<pid>/status` VmRSS, `top` (CPU cores busy), `free`, `dmesg` (OOM),
  `ping` (peer RTT), `curl` (link bandwidth).

## 5. Findings

### 5.1 Below the checkpoint, the committer is `reads`-bound, not "serialization"
With the VCT fast path, `cpu_tree ≈ 0` (note-tree recompute skipped — the feature works).
The committer cost decomposes (warm sample ~316k): **reads 47% (spent-UTXO + address-balance
lookups), transparent/UTXO-index 28%, db_commit (separate) ~39% of write_block, rawtx only
2%, trees ~0%.** So "CPU_SERIALIZE" is really **DB reads + transparent-index building**, not
transaction serialization. Pruned vs archive barely matters for commit cost (`rawtx`=2%).

### 5.2 OOM from the in-flight byte budget (fixed knob is wrong)
Default `max_inflight_block_bytes = 8 GiB` let download race ~1.1M blocks ahead in the
tiny-block region; with the **decode amplification of tiny blocks (~2.3× bytes→RSS)** the
process hit **20.5 GB RSS and was OOM-killed** at height ~107,750 (32 GB host). Capping to
1 GiB fixed it. **Recommendation:** the ceiling should be **RAM-derived and denominated in
estimated decoded memory, not serialized bytes** — the bytes→RSS ratio swings ~5× between
tiny blocks (high per-block overhead) and big blocks (byte-proportional). See the
EIP-1559-style adaptive-ceiling discussion in the session notes.

### 5.3 A VCT fast-synced DB can only be reopened in **pruned** mode
The fast path never writes the historical per-height note-commitment trees, so the reopen
guard **refuses archive mode** (`...cannot be opened in archive storage mode...`). Resuming
the run required `storage_mode = "pruned"`. Also note: **the tip binary (db format 27.5.0)
cannot open the older 27.3.0 full archive** (`srv-cache`) — it panics deserializing the
history tree (`chain.rs:94`, `UnexpectedEof`). That blocked using a local archive peer.

### 5.4 Fast-sync requires an **archive** peer to source roots
The committer (frozen frontier) refuses to recompute and **stalls** if no peer can serve
`tree_aux` roots (`VctSuppliedRootUnavailable`, retryable — but the retry/queue-reset loop
eventually **crashed** the node: `buffer's worker closed unexpectedly`). Peers serving
bodies ≠ peers serving roots: a fast-synced/pruned peer has bodies but **no per-height trees**,
so it returns empty roots. **A cluster of all fast-synced nodes cannot bootstrap each other's
roots.** (Hit when the cluster was redeployed; recovered when archive root-serving nodes
came back.) Two robustness bugs to file: (a) root-unavailable retry loop should not be able
to crash the node; (b) Zakura does not re-dial bootstrap peers after `conn_active` hits 0.

### 5.5 The real throughput bottleneck: big blocks × heterogeneous-RTT peers × in-order commit
- Below ~1.6M (tiny ~2 KB blocks): **committer-bound** (`CPU_SERIALIZE`, ~300–400 blk/s);
  download trivial, peer latency invisible.
- The **"sandblast" spam era (~1.69–1.79M)** balloons block size **~1000×** (2 KB → ~2 MB,
  confirmed: height 1.75M = 1.95 MB). Download becomes the bottleneck and rate collapses to
  ~10–30 blk/s with the **committer 97% idle** and **~1 of 8 CPU cores busy**.
- Requests **are** fanned out across all 7 peers (1205/683/352/336/196/118/110 over a
  sample), but **received bodies concentrate ~98% on the one same-DC (1.24 ms) peer.** Reason:
  blocks commit in strict height order; big blocks from **high-RTT peers (74–222 ms)** are
  slow, so whenever a floor-region block is assigned to a slow peer the **contiguous floor
  stalls**, while the fast peer races ahead delivering out-of-order (buffered) blocks that
  can't yet commit.
- **None of it is config:** raising `max_inflight_block_bytes` 1→4 GiB gave **no** steady-state
  throughput gain (it just buffers more behind the same floor); QUIC windows are generous
  (3 MiB stream / 16 MiB conn); `message_rate` = 2048 msg/s = 2048 blocks/s; request slots
  ~3000+ free. The limiter is **head-of-line on the in-order floor when a slow peer holds it**.

## 6. Recommendations (priority order)

1. **Head-of-line hedging for the floor** (`fanout > 1` / hedged-HOL, cf. PR #151): request
   the next-needed (floor) block from multiple peers / the fastest peer so a single slow peer
   can't gate the contiguous tip. This is the direct fix for §5.5 and should recover most of
   the lost throughput in heavy regions. `fanout=1` today = hedging off.
2. **Deprioritize / cap very-high-RTT peers** for floor-critical requests (the 150–220 ms
   peers drag the floor; the 1.24 ms same-DC peer alone sustains ~16 MB/s).
3. **RAM-derived, memory-denominated in-flight ceiling** (replace the fixed `max_inflight_block_bytes`
   constant) — fixes both the OOM (§5.2) and the throttle, and auto-adapts to block size.
4. **Committer `reads` optimization** (§5.1) for the below-checkpoint region: the spent-UTXO +
   address-balance reads dominate; the transparent **address indexes** are RPC-only and could
   be skipped/deferred for a fast-sync validator.
5. **Robustness:** root-unavailable retry must not crash the node; Zakura should re-dial
   bootstrap peers after total peer loss.

## 7. Current state (live)

- Tip ~1.83M (~54%), pruned, 7 peers, RSS ~5 GB, 100% fast path, recovering post-sandblast.
- Samplers running; `results-table.md` accumulating per-100k rows (representative below):

```
| 900k-1000k  | 295 blk/s | CPU_SERIALIZE  | Blossom;  1% idle; reads 54% |
| 1000k-1100k | 276 blk/s | CPU_SERIALIZE  | Heartwood; 3% idle; reads 53% |
| 1200k-1400k | ~260 blk/s| CPU_SERIALIZE  | Canopy; committer-bound |
| 1600k-1700k | 328 blk/s | (VERIFIER_APPLY*)| sandblast onset; *actually download-bound |
```
*The sampler's `VERIFIER_APPLY` verdict in the sandblast region is mislabeled — it's
download/floor-bound (committer idle + bodies buffered non-contiguously). Add a block-size /
floor-latency check to the classifier to label it `BLOCK_DOWNLOAD (big blocks)`.

---

## Appendix A — Measured values (raw data points)

All from the live run unless noted. Pruned mode except where "archive" is called out.

### A.1 Committer phase decomposition (the `CPU_SERIALIZE` breakdown)
Warm 25 s sample at ~height 316k (1,346 blocks, ~54 blk/s):
```
write_block   = 23.66 s   (per-block 17.6 ms)
  db_commit   =  9.12 s   (39% of write_block — pure RocksDB write)
  serialize   = 14.54 s   (61%)
    reads       = 6.77 s  (47% of serialize — spent-UTXO + address-balance lookups)
    transparent = 4.06 s  (28% — UTXO set + transparent address indexes)
    rawtx       = 0.33 s  ( 2% — raw-tx archive serialization)
    trees       = 0.01 s  shielded = 0.01 s  (~0%)
verify (ZIP-221) ~1 s     cpu_tree = 0.0 (fast path skips note-tree)
```
3-min sample at 399k–418k (19,109 blocks, 180 s = 106.1 blk/s):
```
wait_input = 96.9 s (54% idle)   verify = 3.4 s   cpu_tree = 0.0
serialize  = 47.3 s   db_commit = 28.0 s   write_block = 75.2 s
  reads = 20.9 s (44%)   transparent = 10.5 s (22%)   rawtx = 8.5 s (18%)
```

### A.2 Per-100k window results (blk/s = full-window average)
```
| window      | blk/s | verdict        | committer detail / notes                    |
|-------------|-------|----------------|---------------------------------------------|
| 0–5k        | 99.8  | CPU_SERIALIZE  | serialize 24.1s ≈ db_commit 18.7s; cpu_tree 0 |
| 100k        | 66–153| CPU_SERIALIZE  | (archive) wait≈0.1–0.3s                      |
| 200k        | 74.4  | CPU_SERIALIZE  | (archive) serialize 36.0s, db_commit 24.9s; write_block 15.6 ms/blk |
| 300k        | 54.2  | CPU_SERIALIZE  | (archive) serialize 47.3s, db_commit 31.8s   |
| 400k        | 95.7  | (pruned)       | wait 29.9s; serialize 14.0s, db_commit 8.2s; write_block 4.3 ms/blk; applying 68,399 |
| 900k–1000k  | 295   | CPU_SERIALIZE  | Blossom;   1% idle; reads 54% of serialize   |
| 1000k–1100k | 276   | CPU_SERIALIZE  | Heartwood; 3% idle; reads 53%                |
| 1200k–1300k | 280   | CPU_SERIALIZE  | Canopy;    2% idle; reads 57%                |
| 1300k–1400k | 241   | CPU_SERIALIZE  | Canopy;    1% idle; reads 57%                |
| 1400k–1500k | 402   | CPU_SERIALIZE  | Canopy;    9% idle; reads 57%                |
| 1500k–1600k | 366   | CPU_SERIALIZE  | Canopy;    3% idle; reads 54%                |
| 1600k–1700k | 328   | (download-bound)| 44% idle; applying 19,742; sandblast onset  |
| sandblast   | 10–30 | download-bound | NU5 ~1.69–1.79M; see A.5                      |
```
Archive→pruned committer speedup: write_block **15.6 ms/blk (300k, archive) → 4.3 ms/blk
(400k, pruned)** ≈ 4×. (Confounded with the restart; not isolated.)

### A.3 OOM and memory
- OOM-kill at **height 107,750**: anon-RSS **20,554,588 kB (~20.5 GB)**, total-vm 23.16 GB,
  on a **32 GB** host. `budget_reserved = 8,589,524,391 (8 GiB cap)`, `body_lag = 1,117,650`,
  `applying = 189,374`. RSS/budget ratio ≈ 2.3× (tiny-block decode amplification).
- After 1 GiB cap, RSS vs height (budget pinned ~1.07 GB throughout):
  `4.6 GB @82k → 6.16 @122k → 6.51 @170k → 6.89 @218k → 7.55 @266k`.
  Model: **RSS ≈ 2 × cap(GiB) + base(height)**, base ≈ 2.5–4 GB and growing with chain depth.

### A.4 In-flight budget A/B (1 GiB vs 4 GiB) — no throughput gain
```
1 GiB cap:  ~13 blk/s,  ~10–11 MB/s,  RSS 4.2 GB
4 GiB cap:  ~10 blk/s,  ~8.6 MB/s,    RSS 5.1 GB   (transient 31 blk/s = buffer drain only)
```
Both `download_blocked_on_budget = 1`. At 4 GiB: `budget_reserved ~4.29 GB`,
`outstanding 52–201`, `request_slot_available 2867–4493`, `peers_wanting_slots 3`.

### A.5 Sandblast / NU5 download collapse (~height 1.78M)
- Rate 14 blk/s; **CPU 91% = ~1 of 8 cores busy**; committer **97% idle**
  (write_block 1.7 ms/blk, verify 0.21 ms/blk — committer has huge headroom).
- Download: received 9–13 blk/s, **8.6–16.7 MB/s**; per-block `request_elapsed` ~3,755 ms
  at peak (later 2–35 ms / ~171 ms avg); avg block 615 KB (last 2000).
- **Block size by height** (confirms the ~1000× spike): `1.5M=2,110 B; 1.6M=2,203 B;
  1.69M=4,100 B; 1.75M=1,955,300 B; 1.81M=2,886 B`.

### A.6 Per-peer request vs receipt distribution (the head-of-line finding)
```
requests sent (last 3000):  9aa6=1205  995563=683  9e3ab1=352  67870c=336  cbb2f3=196  00de73=118  8d5202=110
bodies received (last 2000): 9aa6=1958 (98%)  995563=22  9e3ab1=18  cbb2f3=2  (rest 0)
```
Requests fan out across all 7; **receipts concentrate ~98% on the one same-DC peer** because
the in-order floor stalls on slow peers holding big blocks.

### A.7 Peer RTT (heterogeneous — the root of A.6)
```
143.244.184.176 =   1.24 ms   (DigitalOcean Santa Clara — same DC, does ~98% of receipts)
159.65.183.89   =  74.20 ms
64.227.44.93    = 139.00 ms
161.35.156.226  = 149.01 ms
139.59.64.115   = 221.83 ms
```

### A.8 Bandwidth
- **Link capacity (measured):** CacheFly 100 MB download = **838.1 MB/s = 6,705 Mbps (~6.7 Gbps)**.
- **Sync utilization:** ~10–16.7 MB/s (~80–130 Mbps) = **~1–2% of link.**

### A.9 Relevant config / transport constants (none caps at ~10 MB/s)
```
max_inflight_block_bytes = 1 GiB (later 4 GiB)   max_inflight_requests = 2048 (used ~60–200)
message_rate_per_second  = 2048 (= 2048 blocks/s; 1 block = 1 frame)
QUIC stream_receive_window = 3 MiB    receive_window = 16 MiB    send_window = 16 MiB
fanout = 1 (head-of-line hedging OFF)   max_blocks_per_response = 1   max_response_bytes = 32 MiB
MAX_BS_FRAME_BYTES ≈ MAX_BS_MESSAGE_BYTES (< 4 MiB, so a block ≤ ~2 MB fits in one frame)
```

### A.10 DB format
- `srv-cache` (full archive) on disk = **27.3.0**; tip binary = **27.5.0**. The tip binary
  **panics** opening 27.3.0 (`chain.rs:94`: history-tree `deserialization ... UnexpectedEof`).
  The run DB (`run-genesis`, pruned) was migrated 27.3.0→27.5.0 successfully on resume.

---

## 8. Continue here — reproduce · debug · optimize  (start here to keep working)

### 8.1 Bring the run back up / resume
```bash
cd /mnt/roman-dev-2-data/vct-test
# resumes from the pruned run-genesis DB (current tip ~1.8M); pruned is mandatory (§5.3)
nohup env VCT_DIGEST=1 /root/zebra-vct-build/target/release/zebrad \
     -c cfg-genesis-instr.toml start > logs/genesis-instr.out 2>&1 &
# REQUIRES at least one ARCHIVE peer that serves tree_aux roots ahead of our tip (§5.4),
# else the committer stalls on VctSuppliedRootUnavailable and can crash.
# samplers (one per-100k table row each):
nohup env START=$NEXT_100K bash scripts/results-table.sh    > logs/results-table.out 2>&1 &
```

### 8.2 Live debugging cheat-sheet
```bash
# rate / tip
curl -s 127.0.0.1:9990/metrics | awk '$1=="zcash_chain_verified_block_height"{print $2}'
# committer decomposition: diff the vct_commit_*_nanos counters over a window, /1e9 -> seconds
#   wait_input (idle) vs verify / cpu_tree / db_commit / wb_reads / wb_transparent / wb_rawtx
# download / floor state
grep '"event":"block_sync_state"' trace-genesis/block_sync.jsonl | tail -1   # received_*_per_sec,
#   download_blocked_on_budget, budget_reserved, floor_gap_state, applying, outstanding, reorder
# HOL check (per-peer receipts vs requests)
grep '"event":"block_body_received"'  trace-genesis/block_sync.jsonl | tail -3000 | grep -oE '"peer":"peer:[a-f0-9]+"' | sort | uniq -c | sort -rn
grep '"event":"block_get_blocks_sent"' trace-genesis/block_sync.jsonl | tail -3000 | grep -oE '"peer":"peer:[a-f0-9]+"' | sort | uniq -c | sort -rn
# block size at a height
grep '"event":"block_body_received"' trace-genesis/block_sync.jsonl | grep '"height":H' | grep -oE '"serialized_bytes":[0-9]+'
```

**Bottleneck decision tree (how verdicts were assigned):**
- committer idle% = `Δwait_input / Δwall`.
- **idle < ~35% → committer-bound:** pick the largest of `verify` / `cpu_tree` / `serialize`
  (=`reads`+`transparent`+`rawtx`) / `db_commit`.
- **idle > ~35% → upstream-bound:** if `applying` is large AND avg block size is large →
  **download/floor-bound** (confirm with per-peer receipts skewed to one peer +
  `floor_gap_state` + high `request_elapsed_ms`); if header lead small → header download;
  if `state_vct_legacy_block_count` climbing → root-supply miss (§5.4).
- CPU check: `top -p <pid>` — `~1 of 8 cores busy` while idle ⇒ NOT compute-bound (it's I/O/network).

### 8.3 Optimization roadmap (code pointers + how to validate)

1. **Head-of-line hedging — a LIVE CONFIG KNOB, do this first.** `fanout` defaults to **1**
   (hedging off): `DEFAULT_BS_FANOUT` `block_sync/config.rs:42`, field `config.rs:138`,
   behavior `peer_routine.rs:~1207`, scheduling in `work_queue.rs` / `sequencer.rs` /
   `reactor.rs`. **Validate with no code change:** add to the node config
   `[network.zakura.block_sync]\n fanout = 3`, restart in the sandblast region (~1.7M), and
   watch (a) per-peer receipts flatten and (b) the contiguous floor advance faster. This is
   the highest-value / lowest-effort next experiment and directly targets §5.5.
2. **Prefer low-RTT peers for floor-critical heights.** `peer_registry.rs` (servable ranges /
   scoring), `reactor.rs:1783 floor_gap_diagnostics` + `registry.floor_gap_servable`. The
   floor block should not be assigned to a 150–220 ms peer when the 1.24 ms peer can serve it.
3. **RAM-derived, memory-denominated in-flight ceiling** (replace the fixed byte budget; fixes
   the OOM §5.2 and the throttle). Worst-case reservation `BS_PER_BLOCK_WORST_CASE_BYTES =
   MAX_BLOCK_BYTES` `config.rs:28`; `max_inflight_block_bytes` `config.rs:125`; `ByteBudget`
   `transport/guard.rs:37`. Cap **estimated decoded memory** at X% of system RAM, not bytes.
4. **Committer `reads` (below-checkpoint regime, §5.1).** `zebra_db/block.rs:940–993`
   (`read_addr_locs`, `address_balance_location`, `PARALLEL_BLOCK_READ_THRESHOLD`). The
   transparent **address indexes** (`prepare_transparent_transaction_batch`,
   `balance_by_transparent_addr` / `tx_loc_by_transparent_addr_loc`) are **RPC-only** — skip or
   defer them for a pure fast-sync validator to cut both the address-balance reads and the
   index writes.
5. **Robustness bugs to file.** (a) the `VctSuppliedRootUnavailable` retry/queue-reset loop can
   crash the node (`buffer's worker closed unexpectedly`) — make it back off, not die;
   (b) Zakura does not re-dial configured bootstrap peers after `conn_active` hits 0.

### 8.4 What is NOT the bottleneck (don't re-investigate)
Byte budget (1↔4 GiB no effect, §A.4); request concurrency (≤200 of 2048 used); QUIC windows
(3/16 MiB, generous); `message_rate` (2048 = 2048 blocks/s); CPU (1 of 8 cores in heavy region);
the committer in the heavy region (97% idle); the link (6.7 Gbps, ~2% used). The binding
constraint is the **in-order floor gated by slow peers on big blocks** (§5.5) → start at 8.3.1.
