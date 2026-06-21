# Checkpoint-sync benchmark RUNBOOK — fast runs from the 1.7M snapshot

How to run repeatable checkpoint-verifier sync benchmarks **without redownloading the chain**.
Core trick: a **hard-link fork** (`cp -al`) of a pre-synced 1.7M state — each run gets a private,
writable copy in seconds with ~0 bytes copied.

---

## Fixed assets

| Asset | Path |
|---|---|
| Master snapshot — mainnet height **1,707,210**, ~35 GiB | `/mnt/roman-dev-2-data/zebra-ckpt-master` |
| Baseline binary — `ironwood-main` @ `94ae42f48` (release) | `/mnt/roman-dev-2-data/cargo-target-ironwood/release/zebrad` |
| Scratch disk for forks — `/dev/sda`, ~492 GiB | `/mnt/roman-dev-2-data/` |
| Harness scripts + results | `/root/wal-bench/` |

**Why height 1.707M:** it is **below the max mainnet checkpoint (3,358,006)**, so syncing forward
exercises the **checkpoint verifier** (not the semantic/full verifier). Starting from the snapshot
means no genesis-to-here resync.

**Building a fresh baseline binary** (root fs is tight — target the big disk):
```bash
git worktree add --detach /mnt/roman-dev-2-data/zebra-ironwood-main <sha>
cd /mnt/roman-dev-2-data/zebra-ironwood-main
CARGO_TARGET_DIR=/mnt/roman-dev-2-data/cargo-target-ironwood \
  cargo build --release --locked -p zebrad     # ~7 min warm, ~30 min cold
```

---

## The four core moves

### 1. Hard-link fork — the "no copy, no redownload" trick
```bash
FORK=/mnt/roman-dev-2-data/walbench-fork-$LABEL
rm -rf "$FORK"
cp -al /mnt/roman-dev-2-data/zebra-ckpt-master "$FORK"   # hard links: ~seconds, ~0 bytes
find "$FORK" -name LOCK -delete                          # drop stale RocksDB lock
```
`cp -al` makes directory entries pointing at the **same inodes** — no 35 GiB copy. Safe because
RocksDB SSTs/MANIFEST are immutable and new data goes to **new** files; the fork only *diverges*
from the master by appending. Never open the master itself read-write while forks exist.

### 2. Config — fork dir + metrics + deterministic stop
```toml
[network]
network = "Mainnet"
cache_dir = "<FORK>"
[metrics]
endpoint_addr = "127.0.0.1:9999"
[state]
cache_dir = "<FORK>"
debug_stop_at_height = 1760000     # set HIGH; cap on wall-clock instead (see pitfalls)
[sync]
checkpoint_verify_concurrency_limit = 1500
download_concurrency_limit = 150
full_verify_concurrency_limit = 20
[tracing]
filter = "info"                    # add ,zebrad::components::sync=debug for FindBlocks timing
```

### 3. Run + scrape — log to tmpfs, sample on a timer
```bash
"$BIN" -c "$CFG" start >/dev/shm/node-$LABEL.log 2>&1 &   # tmpfs, NOT the fork/disk
PID=$!; sleep 3
kill -0 $PID || { echo "died on startup"; tail -8 /dev/shm/node-$LABEL.log; exit 1; }
# loop every Ns until wall cap or process exit:
#   curl -s 127.0.0.1:9999/metrics   (parse gauges/counters below)
#   read /proc/$PID/io   /proc/$PID/stat   /sys/class/net/eth0/statistics/rx_bytes
```

### 4. Cleanup — reclaim the divergent SSTs (keep the CSV)
```bash
kill $PID; sleep 3; kill -9 $PID 2>/dev/null
rm -rf "$FORK"
```

---

## REQUIRED instrumentation — every bottleneck run must emit AND scrape all of these

To attribute a bottleneck you must be able to separate **network**, **feed/verifier CPU**,
**precompute CPU**, and **committer** — and *within* the committer, separate the actual DB write
from note-tree crypto from read/serialize overhead. A run that scrapes only `commit.duration` will
mis-attribute, because that timer is the **whole** committer, not RocksDB (this exact mistake was
made: ~18 ms "rocksdb commit" was really ~4 ms DB write + ~5 ms note-tree + ~7 ms reads/serialize).

Build with `--features commit-metrics` (gates the state/committer timers). `recv_wait`,
`precompute.started/absent`, and `block_deserialize` are added timers in this fork. The reference
scraper that captures all of these is **`feed_run_compact.sh`** (45-column CSV).

### Network — is the feed starved?
| metric | meaning | CSV |
|---|---|---|
| `sync_downloads_in_flight` | download queue depth (full ⇒ not download-starved) | in_flight |
| `sync_downloaded_block_count` | download rate (Δ/Δt) | downloaded |
| `/sys/class/net/eth0/statistics/rx_bytes` | network RX MB/s | net_rx |

### Feed / verifier CPU
| metric | meaning | CSV |
|---|---|---|
| `zebra.feed.block_deserialize.duration_seconds_{sum,count}` | **block parse** (dominant feed CPU on sandblast) | des_sum/cnt |
| `zebra.feed.equihash_pow.duration_seconds_{sum,count}` | PoW (Equihash) | eq_sum/cnt |
| `zebra.feed.merkle_root.duration_seconds_{sum,count}` | Merkle-root recompute | mk_sum/cnt |

### Precompute CPU (off-committer note hashing) + coupling
| metric | meaning | CSV |
|---|---|---|
| `zebra.state.precompute.compute.duration_seconds_{sum,count}` | bulk Sapling/Orchard hashing (parallel) | prec_sum/cnt |
| `zebra.committer.precompute.recv_wait.duration_seconds_{sum,count}` | committer wait on precompute (≫0 ⇒ precompute is the gate) | recvwait_sum/cnt |
| `zebra.committer.precompute.started` / `.absent` | lookahead hit / miss (committer hashed inline) | pre_started/absent |
| `zebra.state.notes.sapling.per_block` / `.orchard.per_block` | notes appended/block (drives hashing cost) | nsap/nor |

### Committer — total AND its decomposition (do not stop at the total)
| metric | meaning | CSV |
|---|---|---|
| `zebra.committer.commit_finalized_total.duration_seconds_{sum,count}` | **TOTAL** committer per block (not just DB!) — renamed from the ambiguous `commit.duration` | commit_sum/cnt |
| `zebra.state.rocksdb.batch_commit.duration_seconds_{sum,count}` | **actual DB write** only (`db.write(batch)`) | rdbw_sum/cnt |
| `zebra.state.write.checkpoint_compute.duration_seconds_{sum,count}` | serial tree-update + commitment check | ckpt_sum/cnt |
| `zebra.state.write.update_trees.duration_seconds_{sum,count}` | note-tree **graft** (root recompute + fold) | ut_sum/cnt |
| `zebra.state.write.commitment_check.duration_seconds_{sum,count}` | ZIP-244 history-commitment check | cmck_sum/cnt |
| `zebra.committer.input_queue_depth` | committer backlog (high ⇒ committer-bound) | qdepth |
| `zebra.committer.poll_ready` / `poll_empty` | committer busy vs starved (empty% ≈ 0 ⇒ committer is the gate, never feed) | poll_ready/empty |
| `zebra.state.write.block_tx_count_{sum,count}` | tx/block (normalizer) | btc_sum/cnt |

**Reads/serialize residual** = commit_total − checkpoint_compute − rdbw. This is UTXO/address reads +
batch build + raw-tx serialization (the `tx_by_loc` write path). Not separately timed; derive it.

### Host (always, via /proc and /sys)
| metric | meaning |
|---|---|
| CPU cores | `/proc/$PID/stat` f14+f15 (utime+stime), CLK_TCK=100 — whole-node; idle headroom ⇒ not CPU-bound |
| Block-I/O wait | `/proc/$PID/stat` f42 (delayacct_blkio_ticks) — writer blocked on disk |
| Read volume | `/proc/$PID/io` `read_bytes` (physical) vs `rchar` (logical) — cache-miss pressure |
| Write health | `num_files_at_level{level="0"}`, `zebra_state_rocksdb_is_write_stopped` — compaction stall |

**Attribution rule of thumb:** committer-bound iff qdepth high AND poll_empty ≈ 0 AND in_flight full.
Then read the committer decomposition (rdbw / checkpoint_compute / reads-residual) to name the stage.
If poll_empty is high, the feed is the gate — look at deserialize + download. If whole-node CPU is
pegged, it's all-core CPU-bound (precompute + verify); if CPU is idle with high qdepth, it's the
serial committer (DB write / reads), not CPU.

---

## Pitfalls (learned the hard way)

- **One node per fork.** A second `zebrad` on the same fork dir aborts on the RocksDB `LOCK`; if it
  wins the lock, the real run's CSV silently stays empty. Verify `pgrep -f cfg-$LABEL` = exactly one.
- **Launcher must exit 0.** Backgrounded runs got reaped when the launching shell exited non-zero
  (e.g. a leading `pkill` that found nothing → exit 1). Run the harness script **directly as a
  tracked background task**, or ensure the launcher returns 0.
- **Log to `/dev/shm`,** not the fork — keeps `/proc/$PID/io write_bytes` = RocksDB only and avoids
  disk contention with the DB.
- **`debug_stop_at_height` is a poor timer.** Set it high and stop on a wall-clock cap, so a run
  can't hang if it stalls before the stop height.
- **Warm vs cold cache.** A fresh `cp -al` fork is page-cache-warm. For a cold-read test:
  `sync; echo 3 > /proc/sys/vm/drop_caches` (OS cache). RocksDB's in-process block cache only clears
  on a **node restart** — needed for a fully cold read path.
- **Network noise.** Forward sync is over the live P2P network. Per-block-normalized metrics
  (ms/block, KB/block, %-of-wall, cores) are robust to it; **absolute blocks/sec is not** — use
  N≥3 medians for any throughput claim, and record git SHA + machine + wall-clock window.
- **Disk headroom.** Each fork's divergence + WAL grows on `/dev/sda`; `rm -rf` the fork between
  sequential runs so they share the headroom.

---

## Existing harness scripts in this directory

| Script | Purpose |
|---|---|
| `forkrun.sh LABEL BIN STOP [int] [maxsec]` | throughput + RocksDB commit/WAL metrics |
| `longrun.sh` | 20-min run: throughput / CPU / net / commit, raw `commit_sum`+`commit_count` |
| `diag-bottleneck.sh` | CPU-cores vs network-MB/s split (is it CPU- or bandwidth-bound?) |
| `readio-probe.sh` | attaches to a live node: `rchar`/`read_bytes`/blkio-wait/iowait vs net |
| `pipeline-probe.sh` | 1 Hz `in_flight` sawtooth + `FindBlocks`/`extra_hashes` log → dead-time attribution |

Results land as `*.csv` here; analysis findings are in
`/root/zebra/CHECKPOINT_SYNC_FINDINGS.md`.
