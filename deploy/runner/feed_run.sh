#!/usr/bin/env bash
# Bottleneck-instrumentation bench run.
#
# Hard-link-forks a snapshot, syncs forward to STOP, and samples the five-category
# bottleneck metrics every 5s into a CSV. All host-specific paths come from
# cohort.env (the BENCH_* vars) or the environment — nothing is hard-coded here.
#
#   network   : zcash.net.{in,out}.bytes.total, peers.connected
#   download  : sync.downloads.in_flight, sync.downloaded.block.count
#   verifier  : zebra.feed.equihash_pow / merkle_root (checkpoint verifier CPU)
#   commit cpu: update_trees (note tree) + commitment_check + batch_prep
#   commit db : prep_reads (UTXO/addr reads) + rocksdb.batch_commit (write I/O)
#   committer : commit busy + input_queue_depth + poll_ready/poll_empty
#
# The clone breaks hardlinks on RocksDB mutable metadata (CURRENT/MANIFEST/LOG/
# *.log/OPTIONS/LOCK/IDENTITY) and the format `version` file, so per-run RocksDB
# open churn and any NoMigration version bump can never write through to the
# pristine source snapshot.
#
# Usage: feed_run.sh LABEL BIN [stop] [met] [maxsec]
set -uo pipefail

RUNNER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Pull host-specific paths/defaults from the top config.
# shellcheck source=/dev/null
[ -f "$RUNNER_DIR/cohort.env" ] && source "$RUNNER_DIR/cohort.env"

LABEL="${1:?usage: feed_run.sh LABEL BIN [stop] [met] [maxsec]}"
BIN="${2:?need binary path}"
STOP="${3:-${BENCH_STOP_DEFAULT:-1830000}}"
MET="${4:-${BENCH_MET_DEFAULT:-19998}}"
MAXSEC="${5:-${BENCH_MAXSEC_DEFAULT:-3600}}"

# Host-specific paths (configurable via cohort.env / env).
WORK="${BENCH_WORK_DIR:?set BENCH_WORK_DIR in cohort.env}"
MASTER_WARM="${BENCH_MASTER:?set BENCH_MASTER in cohort.env}"
MASTER_COLD="${BENCH_MASTER_COLD:-$MASTER_WARM}"
DBREL="${BENCH_DB_REL:-state/v27/mainnet}"
FORK_DIR="${BENCH_FORK_DIR:-$WORK}"
LOG_DIR="${BENCH_LOG_DIR:-$WORK}"
CONFIG_SRC="${CONFIG_SRC:-${BENCH_CONFIG_SRC:-$RUNNER_DIR/zebra-bench-config.toml}}"

# Prefer the warm master (already upgraded with a repaired history tree) so a run
# starts in seconds; fall back to the cold snapshot (rebuild on open) if absent.
MASTER=${MASTER:-$([ -d "$MASTER_WARM/$DBREL" ] && echo "$MASTER_WARM" || echo "$MASTER_COLD")}
if [ "$MASTER" = "$MASTER_COLD" ] && [ "$MASTER_COLD" != "$MASTER_WARM" ]; then
  echo "[$LABEL] WARNING: using COLD snapshot; expect a one-time history-tree rebuild on open."
fi
FORK="$FORK_DIR/feedrun-fork-$LABEL"
CFG="$WORK/cfg-feedrun-$LABEL.toml"
LOG="$LOG_DIR/feedrun-$LABEL.log"
CSV="$WORK/feedrun-$LABEL.csv"
mkdir -p "$WORK" "$FORK_DIR" "$LOG_DIR"

if [ ! -d "$MASTER/$DBREL" ]; then echo "FATAL: master missing at $MASTER/$DBREL"; exit 1; fi
if [ ! -x "$BIN" ]; then echo "FATAL: binary not executable: $BIN"; exit 1; fi

echo "[$LABEL] safe-cloning $MASTER -> $FORK"
rm -rf "$FORK"
mkdir -p "$FORK/$(dirname "$DBREL")"
cp -al "$MASTER/$DBREL" "$FORK/$DBREL"
# Break links on every file the clone may rewrite, so writes can't reach the source.
( cd "$FORK/$DBREL"
  for f in CURRENT IDENTITY LOG LOCK OPTIONS-* MANIFEST-* *.log version; do
    [ -e "$f" ] || continue
    cp -p "$f" "$f.unlink" && mv -f "$f.unlink" "$f"
  done )

# Build the per-run config from the canonical source-of-truth template by
# substituting the per-run tokens. Override the template with CONFIG_SRC=...
# This is the ONLY config the node loads.
LISTEN=$((8200 + MET % 100))
if [ ! -f "$CONFIG_SRC" ]; then echo "FATAL: config template not found: $CONFIG_SRC"; exit 1; fi
sed -e "s#@@FORK@@#$FORK#g" \
    -e "s#@@LISTEN@@#$LISTEN#g" \
    -e "s#@@MET@@#$MET#g" \
    -e "s#@@STOP@@#$STOP#g" \
    "$CONFIG_SRC" > "$CFG"
# Fail loudly if any token went unsubstituted (typo / missing var).
if grep -q '@@' "$CFG"; then echo "FATAL: unsubstituted @@TOKEN@@ in $CFG:"; grep -n '@@' "$CFG"; exit 1; fi
echo "[$LABEL] config: $CONFIG_SRC -> $CFG (cache_dir=$FORK, met=$MET, listen=$LISTEN, stop=$STOP)"

: > "$LOG"
"$BIN" -c "$CFG" start >>"$LOG" 2>&1 &
PID=$!; sleep 3
if ! kill -0 "$PID" 2>/dev/null; then echo "[$LABEL] died on startup"; tail -15 "$LOG"; exit 1; fi
echo "[$LABEL] started pid=$PID stop=$STOP met=$MET fork=$FORK"

HZ=$(getconf CLK_TCK)
pstat(){ awk '{s=$0;sub(/^[0-9]+ \([^)]*\) /,"",s);split(s,a," ");print a[12]+a[13]}' "/proc/$PID/stat" 2>/dev/null; }
# Bare-name scrape (metric with no labels): first matching `name value`.
mv(){ awk -v n="$1" '$1==n{print $2; exit}' <<<"$2"; }
# Label-summing scrape: sum $2 over `name value` and every `name{...} value` line.
msum(){ awk -v n="$1" '$1==n || index($1,n"{")==1 {s+=$2} END{printf "%.6f", s+0}' <<<"$2"; }

echo "epoch,elapsed,height,blk_s,cpu_cores,\
net_in_bytes,net_out_bytes,peers,\
in_flight,downloaded,dld_sum,dld_cnt,vfy_sum,vfy_cnt,obt_sum,obt_cnt,ext_sum,ext_cnt,\
eq_sum,eq_cnt,mk_sum,mk_cnt,\
ut_sum,ut_cnt,cc_sum,cc_cnt,bp_sum,bp_cnt,\
pr_sum,pr_cnt,dc_sum,dc_cnt,\
commit_sum,commit_cnt,wbt_sum,wbt_cnt,qdepth,poll_ready,poll_empty,\
ckc_sum,ckc_cnt,ttr_sum,ttr_cnt,hpu_sum,hpu_cnt,rtc_sum,rtc_cnt,\
prp_sum,prp_cnt,wbi_sum,wbi_cnt,pst_sum,pst_cnt,\
vct_fast,vct_legacy" > "$CSV"

read pcpu < <(pstat); prevh=0; START=$(date +%s)
while kill -0 "$PID" 2>/dev/null; do
  el=$(( $(date +%s)-START )); [ "$el" -ge "$MAXSEC" ] && { echo "[$LABEL] wall cap"; break; }
  sleep 5
  m=$(curl -s --max-time 4 "http://127.0.0.1:$MET/metrics" 2>/dev/null)
  read ncpu < <(pstat)
  h=$(mv state_finalized_block_height "$m"); h=${h:-0}
  # network (per-peer counters summed across addr labels; peers is a bare gauge)
  nin=$(msum zcash_net_in_bytes_total "$m"); nout=$(msum zcash_net_out_bytes_total "$m")
  peers=$(mv zcash_net_peers "$m")
  # download + sync cadence (these histograms carry a {result=...} label -> msum)
  inf=$(mv sync_downloads_in_flight "$m"); dl=$(mv sync_downloaded_block_count "$m")
  dld_s=$(msum sync_block_download_duration_seconds_sum "$m"); dld_c=$(msum sync_block_download_duration_seconds_count "$m")
  vfy_s=$(msum sync_block_verify_duration_seconds_sum "$m");   vfy_c=$(msum sync_block_verify_duration_seconds_count "$m")
  obt_s=$(msum sync_obtain_response_hash_count_sum "$m");      obt_c=$(msum sync_obtain_response_hash_count_count "$m")
  ext_s=$(msum sync_extend_response_hash_count_sum "$m");      ext_c=$(msum sync_extend_response_hash_count_count "$m")
  # verifier
  eq_s=$(mv zebra_feed_equihash_pow_duration_seconds_sum "$m");  eq_c=$(mv zebra_feed_equihash_pow_duration_seconds_count "$m")
  mk_s=$(mv zebra_feed_merkle_root_duration_seconds_sum "$m");   mk_c=$(mv zebra_feed_merkle_root_duration_seconds_count "$m")
  # commit cpu
  ut_s=$(mv zebra_state_write_update_trees_duration_seconds_sum "$m");      ut_c=$(mv zebra_state_write_update_trees_duration_seconds_count "$m")
  cc_s=$(mv zebra_state_write_commitment_check_duration_seconds_sum "$m");  cc_c=$(mv zebra_state_write_commitment_check_duration_seconds_count "$m")
  bp_s=$(mv zebra_state_write_batch_prep_duration_seconds_sum "$m");        bp_c=$(mv zebra_state_write_batch_prep_duration_seconds_count "$m")
  # commit db
  pr_s=$(mv zebra_state_write_prep_reads_duration_seconds_sum "$m");        pr_c=$(mv zebra_state_write_prep_reads_duration_seconds_count "$m")
  dc_s=$(mv zebra_state_rocksdb_batch_commit_duration_seconds_sum "$m");    dc_c=$(mv zebra_state_rocksdb_batch_commit_duration_seconds_count "$m")
  # committer (binding-stage)
  cm_s=$(mv zebra_committer_commit_duration_seconds_sum "$m");  cm_c=$(mv zebra_committer_commit_duration_seconds_count "$m")
  wbt_s=$(mv zebra_state_write_write_block_total_duration_seconds_sum "$m"); wbt_c=$(mv zebra_state_write_write_block_total_duration_seconds_count "$m")
  qd=$(mv zebra_committer_input_queue_depth "$m"); prdy=$(mv zebra_committer_poll_ready "$m"); pe=$(mv zebra_committer_poll_empty "$m")
  # commit sub-phases (explain committer-busy minus note_tree/write_block)
  ckc_s=$(mv zebra_state_write_checkpoint_compute_duration_seconds_sum "$m"); ckc_c=$(mv zebra_state_write_checkpoint_compute_duration_seconds_count "$m")
  ttr_s=$(mv zebra_state_commit_tip_trees_read_duration_seconds_sum "$m");    ttr_c=$(mv zebra_state_commit_tip_trees_read_duration_seconds_count "$m")
  hpu_s=$(mv zebra_state_commit_history_push_duration_seconds_sum "$m");      hpu_c=$(mv zebra_state_commit_history_push_duration_seconds_count "$m")
  rtc_s=$(mv zebra_state_commit_result_trees_clone_duration_seconds_sum "$m"); rtc_c=$(mv zebra_state_commit_result_trees_clone_duration_seconds_count "$m")
  prp_s=$(mv zebra_state_commit_prep_duration_seconds_sum "$m");               prp_c=$(mv zebra_state_commit_prep_duration_seconds_count "$m")
  wbi_s=$(mv zebra_state_commit_write_block_install_duration_seconds_sum "$m"); wbi_c=$(mv zebra_state_commit_write_block_install_duration_seconds_count "$m")
  pst_s=$(mv zebra_state_commit_post_duration_seconds_sum "$m");               pst_c=$(mv zebra_state_commit_post_duration_seconds_count "$m")
  vf=$(mv state_vct_fast_block_count "$m"); vl=$(mv state_vct_legacy_block_count "$m")

  cores=$(awk -v d=$((ncpu-pcpu)) -v hz=$HZ 'BEGIN{printf "%.2f",d/hz/5}')
  bps=$(awk -v dh=$((h-prevh)) 'BEGIN{printf "%.1f",dh/5}')
  echo "$(date +%s),$el,$h,$bps,$cores,\
${nin:-0},${nout:-0},${peers:-0},\
${inf:-0},${dl:-0},${dld_s:-0},${dld_c:-0},${vfy_s:-0},${vfy_c:-0},${obt_s:-0},${obt_c:-0},${ext_s:-0},${ext_c:-0},\
${eq_s:-0},${eq_c:-0},${mk_s:-0},${mk_c:-0},\
${ut_s:-0},${ut_c:-0},${cc_s:-0},${cc_c:-0},${bp_s:-0},${bp_c:-0},\
${pr_s:-0},${pr_c:-0},${dc_s:-0},${dc_c:-0},\
${cm_s:-0},${cm_c:-0},${wbt_s:-0},${wbt_c:-0},${qd:-0},${prdy:-0},${pe:-0},\
${ckc_s:-0},${ckc_c:-0},${ttr_s:-0},${ttr_c:-0},${hpu_s:-0},${hpu_c:-0},${rtc_s:-0},${rtc_c:-0},\
${prp_s:-0},${prp_c:-0},${wbi_s:-0},${wbi_c:-0},${pst_s:-0},${pst_c:-0},\
${vf:-0},${vl:-0}" >> "$CSV"
  pcpu=$ncpu; prevh=$h
done
fin=$(mv state_finalized_block_height "$(curl -s http://127.0.0.1:$MET/metrics 2>/dev/null)")
echo "[$LABEL] DONE height=${fin:-?}  csv=$CSV  log=$LOG"
echo "[$LABEL] cleanup: rm -rf $FORK   (keeps source $MASTER pristine)"
