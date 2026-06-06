#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Usage: do-genesis-sync-benchmark.sh ZEBRAD_BIN TARGET_HEIGHT

Runs zebrad from an empty state cache and stops after TARGET_HEIGHT is reached.

Optional environment:
  BENCH_ROOT                    Default: /mnt/zebra/runs
  CURRENT_RUN_LINK              Default: /mnt/zebra/current-run
  VARIANT                       Default: genesis
  NETWORK                       Default: Mainnet
  NETWORK_LISTEN_ADDR           Default: 127.0.0.1:0
  NETWORK_CACHE_DIR             Default: /mnt/zebra/network-cache
  RPC_PORT                      Default: 18232
  METRICS_PORT                  Optional. Default: disabled
  PEERSET_INITIAL_TARGET_SIZE   Default: 100
  DOWNLOAD_CONCURRENCY          Default: 100
  CHECKPOINT_CONCURRENCY        Default: 1000
  FULL_VERIFY_CONCURRENCY       Default: 20
  PARALLEL_CPU_THREADS          Default: 0
  POLL_INTERVAL                 Default: 5
  ZEBRAD_FILTERS                Default: info
  MAX_ELAPSED_SECONDS           Optional. Stop after this many seconds.
  MAX_STALL_SECONDS             Optional. Stop if RPC height does not advance.
  SOURCE_REF                    Optional. Source ref that produced ZEBRAD_BIN.
  SOURCE_SHA                    Optional. Source commit that produced ZEBRAD_BIN.
  WORKFLOW_REF                  Optional. Workflow ref that provided this harness.
  WORKFLOW_SHA                  Optional. Workflow commit that provided this harness.
USAGE
}

if [[ $# -ne 2 ]]; then
  usage
  exit 2
fi

ZEBRAD_BIN=$1
TARGET_HEIGHT=$2

if [[ ! -x "$ZEBRAD_BIN" ]]; then
  echo "zebrad binary is missing or not executable: $ZEBRAD_BIN" >&2
  exit 1
fi

if [[ ! "$TARGET_HEIGHT" =~ ^[0-9]+$ ]] || (( TARGET_HEIGHT <= 0 )); then
  echo "TARGET_HEIGHT must be a positive integer: $TARGET_HEIGHT" >&2
  exit 1
fi

BENCH_ROOT=${BENCH_ROOT:-/mnt/zebra/runs}
CURRENT_RUN_LINK=${CURRENT_RUN_LINK:-/mnt/zebra/current-run}
VARIANT=${VARIANT:-genesis}
NETWORK=${NETWORK:-Mainnet}
NETWORK_LISTEN_ADDR=${NETWORK_LISTEN_ADDR:-127.0.0.1:0}
NETWORK_CACHE_DIR=${NETWORK_CACHE_DIR:-/mnt/zebra/network-cache}
RPC_PORT=${RPC_PORT:-18232}
METRICS_PORT=${METRICS_PORT:-}
PEERSET_INITIAL_TARGET_SIZE=${PEERSET_INITIAL_TARGET_SIZE:-100}
DOWNLOAD_CONCURRENCY=${DOWNLOAD_CONCURRENCY:-100}
CHECKPOINT_CONCURRENCY=${CHECKPOINT_CONCURRENCY:-1000}
FULL_VERIFY_CONCURRENCY=${FULL_VERIFY_CONCURRENCY:-20}
PARALLEL_CPU_THREADS=${PARALLEL_CPU_THREADS:-0}
POLL_INTERVAL=${POLL_INTERVAL:-5}
ZEBRAD_FILTERS=${ZEBRAD_FILTERS:-info}
MAX_ELAPSED_SECONDS=${MAX_ELAPSED_SECONDS:-0}
MAX_STALL_SECONDS=${MAX_STALL_SECONDS:-0}
SOURCE_REF=${SOURCE_REF:-}
SOURCE_SHA=${SOURCE_SHA:-}
WORKFLOW_REF=${WORKFLOW_REF:-}
WORKFLOW_SHA=${WORKFLOW_SHA:-}

VARIANT=$(printf '%s' "$VARIANT" | tr -c 'A-Za-z0-9._-' '-')
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${VARIANT}-genesis-to-${TARGET_HEIGHT}"
RUN_DIR="$BENCH_ROOT/$RUN_ID"
STATE_DIR="$RUN_DIR/state"
LOG_DIR="$RUN_DIR/logs"
CONFIG="$RUN_DIR/zebrad.toml"
SUMMARY="$RUN_DIR/summary.env"
SUMMARY_MD="$RUN_DIR/summary.md"
SAMPLES="$RUN_DIR/height-samples.csv"

mkdir -p "$RUN_DIR" "$STATE_DIR" "$LOG_DIR" "$NETWORK_CACHE_DIR" "$(dirname "$CURRENT_RUN_LINK")"
ln -sfn "$RUN_DIR" "$CURRENT_RUN_LINK"

child_pids() {
  local pid=$1
  local child

  for child in $(pgrep -P "$pid" 2>/dev/null || true); do
    echo "$child"
    child_pids "$child"
  done
}

terminate_process_tree() {
  local pid=${1:-}
  local child

  [[ -z "$pid" ]] && return 0

  for child in $(child_pids "$pid" | tac); do
    kill "$child" 2>/dev/null || true
  done
  kill "$pid" 2>/dev/null || true
}

cleanup() {
  set +e
  if [[ -n "${ZEBRAD_PID:-}" ]] && kill -0 "$ZEBRAD_PID" 2>/dev/null; then
    terminate_process_tree "$ZEBRAD_PID"
    wait "$ZEBRAD_PID" 2>/dev/null
  fi

  for monitor_pid in ${MONITOR_PIDS:-}; do
    kill "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

{
  echo "[consensus]"
  echo "checkpoint_sync = true"
  echo
  if [[ -n "$METRICS_PORT" ]]; then
    echo "[metrics]"
    echo "endpoint_addr = '127.0.0.1:${METRICS_PORT}'"
    echo
  fi
  echo "[network]"
  echo "cache_dir = '$NETWORK_CACHE_DIR'"
  echo "listen_addr = '$NETWORK_LISTEN_ADDR'"
  echo "network = '$NETWORK'"
  echo "peerset_initial_target_size = $PEERSET_INITIAL_TARGET_SIZE"
  echo
  echo "[rpc]"
  echo "listen_addr = '127.0.0.1:${RPC_PORT}'"
  echo "enable_cookie_auth = false"
  echo "cookie_dir = '$RUN_DIR'"
  echo
  echo "[state]"
  echo "cache_dir = '$STATE_DIR'"
  echo "debug_stop_at_height = $TARGET_HEIGHT"
  echo "delete_old_database = false"
  echo "should_backup_non_finalized_state = false"
  echo
  echo "[sync]"
  echo "download_concurrency_limit = $DOWNLOAD_CONCURRENCY"
  echo "checkpoint_verify_concurrency_limit = $CHECKPOINT_CONCURRENCY"
  echo "full_verify_concurrency_limit = $FULL_VERIFY_CONCURRENCY"
  echo "parallel_cpu_threads = $PARALLEL_CPU_THREADS"
} >"$CONFIG"

{
  echo "run_id=$RUN_ID"
  echo "variant=$VARIANT"
  echo "source_ref=$SOURCE_REF"
  echo "source_sha=$SOURCE_SHA"
  echo "workflow_ref=$WORKFLOW_REF"
  echo "workflow_sha=$WORKFLOW_SHA"
  echo "zebrad_bin=$ZEBRAD_BIN"
  echo "zebrad_version=$("$ZEBRAD_BIN" --version | tr -d '\r')"
  echo "network=$NETWORK"
  echo "start_height=0"
  echo "target_height=$TARGET_HEIGHT"
  echo "rpc_port=$RPC_PORT"
  echo "peerset_initial_target_size=$PEERSET_INITIAL_TARGET_SIZE"
  echo "download_concurrency=$DOWNLOAD_CONCURRENCY"
  echo "checkpoint_concurrency=$CHECKPOINT_CONCURRENCY"
  echo "full_verify_concurrency=$FULL_VERIFY_CONCURRENCY"
  echo "parallel_cpu_threads=$PARALLEL_CPU_THREADS"
  echo "poll_interval=$POLL_INTERVAL"
  echo "max_elapsed_seconds=$MAX_ELAPSED_SECONDS"
  echo "max_stall_seconds=$MAX_STALL_SECONDS"
  echo "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$SUMMARY"

echo "unix_time,iso_time,elapsed_seconds,height,delta_from_start,interval_blocks,interval_seconds,interval_bps,avg_bps,restart_waits,source_peers_busy,download_errors,connection_timeouts,disk_used_gb,disk_avail_gb,rss_kb" >"$SAMPLES"

if command -v iostat >/dev/null 2>&1; then
  iostat -xm "$POLL_INTERVAL" >"$LOG_DIR/iostat.log" 2>&1 &
  MONITOR_PIDS="${MONITOR_PIDS:-} $!"
fi

START_EPOCH=$(date +%s)

(
  set +e
  /usr/bin/time -v "$ZEBRAD_BIN" -c "$CONFIG" --filters "$ZEBRAD_FILTERS" start
  echo "$?" >"$RUN_DIR/zebrad.exit"
) >"$LOG_DIR/zebrad.log" 2>&1 &
ZEBRAD_PID=$!
echo "$ZEBRAD_PID" >"$RUN_DIR/zebrad-wrapper.pid"

ZEBRAD_MONITOR_PID=$ZEBRAD_PID
for _ in {1..40}; do
  ZEBRAD_CHILD_PID=$(pgrep -P "$ZEBRAD_PID" -x zebrad 2>/dev/null | head -1 || true)
  if [[ -z "$ZEBRAD_CHILD_PID" ]]; then
    for child in $(pgrep -P "$ZEBRAD_PID" 2>/dev/null || true); do
      ZEBRAD_CHILD_PID=$(pgrep -P "$child" -x zebrad 2>/dev/null | head -1 || true)
      [[ -n "$ZEBRAD_CHILD_PID" ]] && break
    done
  fi

  if [[ -n "$ZEBRAD_CHILD_PID" ]]; then
    ZEBRAD_MONITOR_PID=$ZEBRAD_CHILD_PID
    break
  fi
  sleep 0.25
done

if command -v pidstat >/dev/null 2>&1; then
  pidstat -dur -p "$ZEBRAD_MONITOR_PID" "$POLL_INTERVAL" >"$LOG_DIR/pidstat.log" 2>&1 &
  MONITOR_PIDS="${MONITOR_PIDS:-} $!"
fi

rpc_height() {
  curl -fsS --max-time 5 \
    -H 'content-type: application/json' \
    --data-binary '{"jsonrpc":"2.0","id":"bench","method":"getblockcount","params":[]}' \
    "http://127.0.0.1:${RPC_PORT}/" |
    jq -r '.result // empty'
}

disk_field() {
  local field=$1
  df -BG "$STATE_DIR" | awk -v f="$field" 'NR == 2 { gsub("G", "", $f); print $f }'
}

rss_kb() {
  local pid=$1
  if [[ -n "$pid" ]] && [[ -r "/proc/${pid}/status" ]]; then
    awk '/VmRSS:/ { print $2 }' "/proc/${pid}/status"
  fi
}

count_log() {
  local pattern=$1
  grep -Ec "$pattern" "$LOG_DIR/zebrad.log" 2>/dev/null || true
}

STOP_REASON=process_exit
LAST_HEIGHT=0
LAST_SAMPLE_HEIGHT=""
LAST_SAMPLE_EPOCH=$START_EPOCH
LAST_ADVANCE_EPOCH=$START_EPOCH
FIRST_RPC_SECONDS=""
FIRST_BLOCK_SECONDS=""
FIRST_BLOCK_HEIGHT=""

while kill -0 "$ZEBRAD_PID" 2>/dev/null; do
  NOW=$(date +%s)
  NOW_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  ELAPSED=$((NOW - START_EPOCH))
  HEIGHT=$(rpc_height 2>/dev/null || true)

  if [[ "$HEIGHT" =~ ^[0-9]+$ ]]; then
    if [[ -z "$FIRST_RPC_SECONDS" ]]; then
      FIRST_RPC_SECONDS=$ELAPSED
    fi
    if (( HEIGHT > 0 )) && [[ -z "$FIRST_BLOCK_SECONDS" ]]; then
      FIRST_BLOCK_SECONDS=$ELAPSED
      FIRST_BLOCK_HEIGHT=$HEIGHT
    fi

    if [[ "$LAST_SAMPLE_HEIGHT" =~ ^[0-9]+$ ]]; then
      INTERVAL_BLOCKS=$((HEIGHT - LAST_SAMPLE_HEIGHT))
      INTERVAL_SECONDS=$((NOW - LAST_SAMPLE_EPOCH))
    else
      INTERVAL_BLOCKS=0
      INTERVAL_SECONDS=0
    fi

    if (( INTERVAL_SECONDS > 0 )); then
      INTERVAL_BPS=$(awk -v b="$INTERVAL_BLOCKS" -v s="$INTERVAL_SECONDS" 'BEGIN { printf "%.6f", b / s }')
    else
      INTERVAL_BPS=0
    fi

    if (( ELAPSED > 0 )); then
      AVG_BPS=$(awk -v h="$HEIGHT" -v e="$ELAPSED" 'BEGIN { printf "%.6f", h / e }')
    else
      AVG_BPS=0
    fi

    if (( HEIGHT > LAST_HEIGHT )); then
      LAST_HEIGHT=$HEIGHT
      LAST_ADVANCE_EPOCH=$NOW
    fi

    RESTART_WAITS=$(count_log 'waiting to restart sync')
    SOURCE_PEERS_BUSY=$(count_log 'source peers busy')
    DOWNLOAD_ERRORS=$(count_log 'block download.*(error|fail)|failed.*block|download.*timeout|DownloadFailed')
    CONNECTION_TIMEOUTS=$(count_log 'connection timed out|timed out|timeout')
    DISK_USED=$(disk_field 3 || true)
    DISK_AVAIL=$(disk_field 4 || true)
    RSS=$(rss_kb "$ZEBRAD_MONITOR_PID" || true)

    echo "${NOW},${NOW_ISO},${ELAPSED},${HEIGHT},${HEIGHT},${INTERVAL_BLOCKS},${INTERVAL_SECONDS},${INTERVAL_BPS},${AVG_BPS},${RESTART_WAITS},${SOURCE_PEERS_BUSY},${DOWNLOAD_ERRORS},${CONNECTION_TIMEOUTS},${DISK_USED:-0},${DISK_AVAIL:-0},${RSS:-0}" >>"$SAMPLES"

    LAST_SAMPLE_HEIGHT=$HEIGHT
    LAST_SAMPLE_EPOCH=$NOW

    if (( HEIGHT >= TARGET_HEIGHT )); then
      STOP_REASON=target_height
      terminate_process_tree "$ZEBRAD_PID"
      break
    fi
  else
    echo "${NOW},${NOW_ISO},${ELAPSED},,,,,,,,,,,,," >>"$SAMPLES"
  fi

  if (( MAX_ELAPSED_SECONDS > 0 && ELAPSED >= MAX_ELAPSED_SECONDS )); then
    STOP_REASON=max_elapsed_seconds
    echo "stopping after ${ELAPSED}s because MAX_ELAPSED_SECONDS=$MAX_ELAPSED_SECONDS" >>"$LOG_DIR/harness.log"
    terminate_process_tree "$ZEBRAD_PID"
    break
  fi

  if (( MAX_STALL_SECONDS > 0 && NOW - LAST_ADVANCE_EPOCH >= MAX_STALL_SECONDS )); then
    STOP_REASON=max_stall_seconds
    echo "stopping after $((NOW - LAST_ADVANCE_EPOCH))s without RPC height progress; last_height=$LAST_HEIGHT" >>"$LOG_DIR/harness.log"
    terminate_process_tree "$ZEBRAD_PID"
    break
  fi

  sleep "$POLL_INTERVAL"
done

set +e
wait "$ZEBRAD_PID"
ZEBRAD_STATUS=$?
set -e
ZEBRAD_PID=""

if [[ -s "$RUN_DIR/zebrad.exit" ]]; then
  ZEBRAD_STATUS=$(cat "$RUN_DIR/zebrad.exit")
fi

END_EPOCH=$(date +%s)
ELAPSED=$((END_EPOCH - START_EPOCH))
TIP_HEIGHT=$("$ZEBRAD_BIN" tip-height --cache-dir "$STATE_DIR" --network "$NETWORK" 2>/dev/null | tr -d '\r' || true)
if [[ "$TIP_HEIGHT" =~ ^[0-9]+$ ]]; then
  END_HEIGHT=$TIP_HEIGHT
else
  END_HEIGHT=$LAST_HEIGHT
fi

if [[ "$END_HEIGHT" =~ ^[0-9]+$ ]]; then
  BLOCKS_SYNCED=$END_HEIGHT
else
  BLOCKS_SYNCED=0
fi

if (( ELAPSED > 0 )); then
  AVG_BPS=$(awk -v b="$BLOCKS_SYNCED" -v e="$ELAPSED" 'BEGIN { printf "%.6f", b / e }')
else
  AVG_BPS=0
fi

RESTART_WAITS=$(count_log 'waiting to restart sync')
SOURCE_PEERS_BUSY=$(count_log 'source peers busy')
DOWNLOAD_ERRORS=$(count_log 'block download.*(error|fail)|failed.*block|download.*timeout|DownloadFailed')
CONNECTION_TIMEOUTS=$(count_log 'connection timed out|timed out|timeout')
TARGET_REACHED=false
if [[ "$END_HEIGHT" =~ ^[0-9]+$ ]] && (( END_HEIGHT >= TARGET_HEIGHT )); then
  TARGET_REACHED=true
fi

{
  echo "finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "zebrad_status=$ZEBRAD_STATUS"
  echo "stop_reason=$STOP_REASON"
  echo "target_reached=$TARGET_REACHED"
  echo "elapsed_seconds=$ELAPSED"
  echo "end_height=$END_HEIGHT"
  echo "blocks_synced=$BLOCKS_SYNCED"
  echo "avg_blocks_per_second=$AVG_BPS"
  echo "first_rpc_seconds=${FIRST_RPC_SECONDS:-}"
  echo "first_block_seconds=${FIRST_BLOCK_SECONDS:-}"
  echo "first_block_height=${FIRST_BLOCK_HEIGHT:-}"
  echo "restart_waits=$RESTART_WAITS"
  echo "source_peers_busy=$SOURCE_PEERS_BUSY"
  echo "download_errors=$DOWNLOAD_ERRORS"
  echo "connection_timeouts=$CONNECTION_TIMEOUTS"
  echo "run_dir=$RUN_DIR"
} >>"$SUMMARY"

{
  echo "## Genesis Sync Benchmark"
  echo
  echo "| Metric | Value |"
  echo "| --- | ---: |"
  echo "| Variant | \`$VARIANT\` |"
  echo "| Source ref | \`$SOURCE_REF\` |"
  echo "| Source SHA | \`$SOURCE_SHA\` |"
  echo "| Target height | $TARGET_HEIGHT |"
  echo "| End height | $END_HEIGHT |"
  echo "| Target reached | $TARGET_REACHED |"
  echo "| Elapsed seconds | $ELAPSED |"
  echo "| Average blocks/s | $AVG_BPS |"
  echo "| Restart waits | $RESTART_WAITS |"
  echo "| Source peers busy deferrals | $SOURCE_PEERS_BUSY |"
  echo "| Download errors | $DOWNLOAD_ERRORS |"
  echo "| Timeout log lines | $CONNECTION_TIMEOUTS |"
  echo
  echo "### Zebrad"
  echo
  echo "\`\`\`text"
  "$ZEBRAD_BIN" --version
  echo "\`\`\`"
  echo
  echo "### Recent Height Samples"
  echo
  echo "\`\`\`csv"
  tail -n 20 "$SAMPLES"
  echo "\`\`\`"
  echo
  echo "### Recent Zebra Log"
  echo
  echo "\`\`\`text"
  tail -n 80 "$LOG_DIR/zebrad.log"
  echo "\`\`\`"
} >"$SUMMARY_MD"

cat "$SUMMARY"

if [[ "$TARGET_REACHED" != true ]]; then
  exit 1
fi
