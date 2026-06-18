#!/usr/bin/env bash
#
# checkpoint-sync-bench.sh — repeatable checkpoint-zone sync benchmark.
#
# Downloads a pre-synced ~1.7M mainnet state snapshot once, hard-link-forks it per
# run (cp -al), runs a prebuilt release zebrad forward through the checkpoint zone
# pinned to a single peer, and prints: time taken, blocks covered, blocks/s.
#
# No build: the zebrad binary comes from a published GitHub release tarball.
# Designed to run on the roman-zakura-3 self-hosted runner, but it is self-contained
# and can be run by hand on any Linux box with enough disk.
#
# Inputs (environment variables; the workflow sets these from inputs/vars):
#   RELEASE_TAG            release tag to benchmark            (e.g. v5.0.0-test.7)
#   BASELINE_TAG          optional: also run this tag, print A/B comparison
#   STOP_HEIGHT           debug_stop_at_height                (default 1737210, +30k)
#   WALL_CAP              hard wall-clock cap, seconds         (default 3600)
#   FEED_PEER             single pinned peer ip:port           (default 167.99.162.47:8233)
#   CKPT_LIMIT            checkpoint_verify_concurrency_limit  (default 1500)
#   DL_LIMIT              download_concurrency_limit           (default 150)
#   SNAPSHOT_URL          primary snapshot .tar.zst URL
#   SNAPSHOT_SHA256       expected sha256 of the .tar.zst
#   START_HEIGHT          snapshot tip height                  (default 1707210)
#   BENCH_HOME            persistent cache root                (default /opt/zebra-bench)
#   GH_REPO               releases repo                        (default valargroup/zebra)
#   OUT_DIR               artifact output dir                  (default ./bench-out)
#   METRICS_PORT          Prometheus port (auto-bumps if busy) (default 19999)
#   LISTEN_PORT           P2P listen port  (auto-bumps if busy)(default 18233)
#
# Ports default high and auto-skip busy ones so the bench can coexist with another
# zebrad already running on the host (which typically holds 8233 / 9999).
set -euo pipefail

# ---- inputs / defaults -------------------------------------------------------
RELEASE_TAG="${RELEASE_TAG:?RELEASE_TAG is required}"
BASELINE_TAG="${BASELINE_TAG:-}"
STOP_HEIGHT="${STOP_HEIGHT:-1737210}"
WALL_CAP="${WALL_CAP:-3600}"
FEED_PEER="${FEED_PEER:-167.99.162.47:8233}"
CKPT_LIMIT="${CKPT_LIMIT:-1500}"
DL_LIMIT="${DL_LIMIT:-150}"
START_HEIGHT="${START_HEIGHT:-1707210}"
SNAPSHOT_URL="${SNAPSHOT_URL:-https://zebra.valargroup.org/mainnet/historical/zebra-mainnet-20260616T032721Z-1707210.tar.zst}"
SNAPSHOT_SHA256="${SNAPSHOT_SHA256:-19ac5d24eaa4e912cc8bbd4e7f5f2aaa2b6c132854e75d93678316016f0f2769}"
SNAPSHOT_MIRROR="${SNAPSHOT_MIRROR:-https://zebra-snapshots.nyc3.cdn.digitaloceanspaces.com/mainnet/historical/zebra-mainnet-20260616T032721Z-1707210.tar.zst}"
BENCH_HOME="${BENCH_HOME:-/opt/zebra-bench}"
GH_REPO="${GH_REPO:-valargroup/zebra}"
OUT_DIR="${OUT_DIR:-$PWD/bench-out}"

SNAP_FILE="$(basename "$SNAPSHOT_URL")"
MASTER="$BENCH_HOME/master-${START_HEIGHT}"
SAMPLE_INTERVAL=5
ZEBRAD_BIN=""

log()  { printf '[bench %(%H:%M:%S)T] %s\n' -1 "$*" >&2; }
die()  { log "FATAL: $*"; exit 1; }

# Always tear down a launched node + its fork, even on FATAL/interrupt, so a failed
# run never leaves an orphan zebrad thrashing the box or a fork eating disk.
CUR_PID=""; CUR_FORK=""
cleanup() {
  [[ -n "$CUR_PID" ]] && kill -9 "$CUR_PID" 2>/dev/null
  [[ -n "$CUR_FORK" ]] && rm -rf "$CUR_FORK" 2>/dev/null
  return 0
}
trap cleanup EXIT INT TERM

# pick a free TCP port starting at $1 (avoids colliding with another node on the host)
pick_free_port() {
  local p="$1"
  if command -v ss >/dev/null 2>&1; then
    while ss -ltnH "sport = :$p" 2>/dev/null | grep -q LISTEN; do p=$((p+1)); done
  fi
  echo "$p"
}
METRICS_PORT="$(pick_free_port "${METRICS_PORT:-19999}")"
LISTEN_PORT="$(pick_free_port "${LISTEN_PORT:-18233}")"

mkdir -p "$OUT_DIR"

# ---- 0. dependencies + disk --------------------------------------------------
ensure_deps() {
  local missing=()
  for t in zstd tar jq curl awk sha256sum; do command -v "$t" >/dev/null 2>&1 || missing+=("$t"); done
  if ((${#missing[@]})); then
    log "installing missing tools: ${missing[*]}"
    if command -v apt-get >/dev/null 2>&1; then
      sudo apt-get update -qq && sudo apt-get install -y -qq "${missing[@]}" \
        || die "could not install: ${missing[*]} (install them on the runner)"
    else
      die "missing tools and no apt-get: ${missing[*]}"
    fi
  fi
}

ensure_bench_home() {
  if [[ ! -d "$BENCH_HOME" ]]; then
    sudo mkdir -p "$BENCH_HOME" && sudo chown "$(id -u):$(id -g)" "$BENCH_HOME" \
      || die "cannot create $BENCH_HOME"
  fi
  [[ -w "$BENCH_HOME" ]] || die "$BENCH_HOME not writable"
  mkdir -p "$BENCH_HOME/snapshots" "$BENCH_HOME/bins" "$BENCH_HOME/forks"
  local avail_gib
  avail_gib=$(df -B1G --output=avail "$BENCH_HOME" | tail -1 | tr -dc '0-9')
  log "free space at $BENCH_HOME: ${avail_gib}GiB"
  # streaming extract needs room for the ~40GiB extracted master + per-run fork divergence
  (( avail_gib >= 45 )) || die "need >=45GiB free at $BENCH_HOME, have ${avail_gib}GiB"
}

# ---- 1. snapshot (stream download+extract once, cached) ----------------------
# Streams the .tar.zst straight through zstd|tar so the compressed tarball is never
# stored on disk (the box has only ~one disk and can't hold tarball + extracted state).
# sha256 is computed over the compressed stream via tee and checked after extraction.
ensure_snapshot() {
  if [[ -f "$MASTER/state/v27/mainnet/version" ]]; then
    log "snapshot master present: $MASTER (db v$(cat "$MASTER/state/v27/mainnet/version"))"
    return
  fi
  local tmp="$MASTER.tmp.$$" sumf; sumf="$BENCH_HOME/snapshots/.sha.$$"
  rm -rf "$tmp"; mkdir -p "$tmp"
  log "streaming snapshot download+extract (~30GiB compressed, no tarball kept) ..."
  local ok=0 url
  for url in "$SNAPSHOT_URL" "$SNAPSHOT_MIRROR"; do
    [[ -n "$url" ]] || continue
    log "source: $url"
    if curl -fL --retry 3 --retry-delay 5 --connect-timeout 30 "$url" \
         | tee >(sha256sum | awk '{print $1}' > "$sumf") \
         | zstd -dc --long=31 | tar -x -C "$tmp"; then
      ok=1; break
    fi
    log "source failed; cleaning and trying next"; rm -rf "$tmp"; mkdir -p "$tmp"
  done
  (( ok )) || { rm -rf "$tmp" "$sumf"; die "snapshot download failed from all sources"; }
  local got; got="$(cat "$sumf" 2>/dev/null || true)"; rm -f "$sumf"
  if [[ -n "$SNAPSHOT_SHA256" && "$got" != "$SNAPSHOT_SHA256" ]]; then
    rm -rf "$tmp"; die "snapshot checksum mismatch: got '$got' want '$SNAPSHOT_SHA256'"
  fi
  log "snapshot checksum OK ($got)"
  # the archive may contain the state/ tree at top level or under one parent dir; normalize
  if [[ -d "$tmp/state" ]]; then
    mv "$tmp" "$MASTER"
  else
    local inner
    inner="$(find "$tmp" -maxdepth 2 -type d -name state -printf '%h\n' | head -1)"
    [[ -n "$inner" ]] || die "could not locate state/ in extracted snapshot"
    mv "$inner" "$MASTER"; rm -rf "$tmp"
  fi
  [[ -f "$MASTER/state/v27/mainnet/version" ]] || die "extracted snapshot missing state/v27/mainnet/version"
  log "snapshot ready: db v$(cat "$MASTER/state/v27/mainnet/version")"
}

# ---- 2. release binary (download once per tag, cached) -----------------------
# sets ZEBRAD_BIN to the zebrad binary path for $1=tag (returns via global, not
# stdout, so no subcommand chatter can ever pollute the path)
ensure_binary() {
  local tag="$1" bindir="$BENCH_HOME/bins/$1" zebrad
  zebrad="$bindir/zebrad"
  if [[ -x "$zebrad" ]]; then ZEBRAD_BIN="$zebrad"; return; fi
  mkdir -p "$bindir"
  log "fetching release $tag from $GH_REPO ..." >&2
  local dl="$bindir/dl"; rm -rf "$dl"; mkdir -p "$dl"
  gh release download "$tag" -R "$GH_REPO" \
    -p 'zebrad-*-linux-x86_64.tar.gz' -p 'SHA256SUMS.txt' -D "$dl" \
    || die "gh release download failed for $tag"
  local tgz; tgz="$(find "$dl" -name 'zebrad-*-linux-x86_64.tar.gz' | head -1)"
  [[ -n "$tgz" ]] || die "no linux-x86_64 tarball asset on release $tag"
  if [[ -f "$dl/SHA256SUMS.txt" ]]; then
    # NB: keep all output on stderr — this function's stdout is captured as the binary path
    ( cd "$dl" && grep "$(basename "$tgz")" SHA256SUMS.txt | sha256sum -c - ) >&2 \
      || die "release tarball checksum mismatch for $tag"
  fi
  tar -xzf "$tgz" -C "$dl"
  local found; found="$(find "$dl" -type f -name zebrad | head -1)"
  [[ -n "$found" ]] || die "zebrad binary not found in tarball for $tag"
  mv "$found" "$zebrad"; chmod +x "$zebrad"; rm -rf "$dl"
  log "zebrad $tag: $("$zebrad" --version 2>/dev/null | head -1)" >&2
  ZEBRAD_BIN="$zebrad"
}

# ---- height scraping ---------------------------------------------------------
# Prometheus first, trying several metric names across zebrad versions (the
# checkpoint verifier exports checkpoint_verified_height; newer builds also export
# a finalized-height gauge). Falls back to a *specific* committed/finalized/verified
# log line — never a bare Height(N), which also appears for network-tip/target heights.
HEIGHT_METRICS="state_finalized_block_height state_checkpoint_finalized_block_height checkpoint_finalized_block_height checkpoint_verified_height"
scrape_height() {
  local logf="$1" page m v
  page="$(curl -fsS --max-time 4 "127.0.0.1:${METRICS_PORT}/metrics" 2>/dev/null || true)"
  if [[ -n "$page" ]]; then
    for m in $HEIGHT_METRICS; do
      v="$(awk -v n="$m" '$1==n {printf "%d", $2; exit}' <<<"$page")"
      [[ -n "$v" && "$v" -gt 0 ]] && { echo "$v"; return; }
    done
  fi
  v="$(grep -aoiE '(finaliz|committed|checkpoint)[a-z_ ]*[Hh]eight[^0-9]*([0-9]{6,})' "$logf" 2>/dev/null \
        | grep -oE '[0-9]{6,}' | sort -n | tail -1)" || true
  [[ -n "$v" ]] && echo "$v"
}

# ---- 3-7. one benchmark run for a given tag ----------------------------------
# usage: run_one TAG OUTPREFIX ; sets RESULT_* globals
run_one() {
  local tag="$1" prefix="$2"
  ensure_binary "$tag"; local zebrad="$ZEBRAD_BIN"
  local run_id="${prefix}-$$-$(date +%s)"
  local fork="$BENCH_HOME/forks/$run_id"
  local logf="/dev/shm/zebra-bench-$run_id.log"
  local csv="$OUT_DIR/samples-$prefix.csv"
  local cfg="$fork.config.toml"

  log "fork: cp -al master -> $fork"
  rm -rf "$fork"; cp -al "$MASTER" "$fork"; CUR_FORK="$fork"
  find "$fork" -name LOCK -delete 2>/dev/null || true

  # $1 = include the Zakura v2 P2P toggles (present only on v5.0.0+ "Zakura" releases)
  write_config() {
    {
      echo '[network]'
      echo 'network = "Mainnet"'
      echo "cache_dir = \"$fork\""
      echo "listen_addr = \"127.0.0.1:$LISTEN_PORT\""
      echo "initial_mainnet_peers = [\"$FEED_PEER\"]"
      echo 'peerset_initial_target_size = 1'
      if [[ "$1" == "with_zakura" ]]; then
        echo 'legacy_p2p = true'
        echo 'v2_p2p = false'   # ZAKURA off: legacy single-peer feed only
      fi
      echo ''
      echo '[state]'
      echo "cache_dir = \"$fork\""
      echo "debug_stop_at_height = $STOP_HEIGHT"
      echo ''
      echo '[sync]'
      echo "checkpoint_verify_concurrency_limit = $CKPT_LIMIT"
      echo "download_concurrency_limit = $DL_LIMIT"
      echo 'full_verify_concurrency_limit = 20'
      echo ''
      echo '[metrics]'
      echo "endpoint_addr = \"127.0.0.1:$METRICS_PORT\""
      echo ''
      echo '[tracing]'
      echo 'filter = "info"'
    } > "$cfg"
  }

  local pid t0 mode="with_zakura"
  log "starting zebrad ($tag), stop_height=$STOP_HEIGHT, peer=$FEED_PEER, cap=${WALL_CAP}s, metrics=:$METRICS_PORT, listen=:$LISTEN_PORT"
  write_config "$mode"
  "$zebrad" -c "$cfg" start >"$logf" 2>&1 &
  pid=$!; CUR_PID="$pid"; t0=$(date +%s); sleep 3
  if ! kill -0 "$pid" 2>/dev/null; then
    # version-skew fallback: older tags lack v2_p2p/legacy_p2p -> deny_unknown_fields.
    if grep -qiE 'unknown field|v2_p2p|legacy_p2p|deny_unknown|error parsing config|failed to parse' "$logf"; then
      log "config rejected (likely pre-Zakura tag); retrying without v2_p2p/legacy_p2p"
      write_config "no_zakura"
      "$zebrad" -c "$cfg" start >"$logf" 2>&1 &
      pid=$!; CUR_PID="$pid"; t0=$(date +%s); sleep 3
    fi
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    log "zebrad died on startup; last log lines:"; tail -20 "$logf" >&2
    die "startup failure for $tag"
  fi

  echo "epoch,elapsed,height" > "$csv"
  local t_escape="" end_height="$START_HEIGHT" h now elapsed
  while :; do
    now=$(date +%s); elapsed=$((now - t0))
    h="$(scrape_height "$logf")" || true
    if [[ -n "$h" ]]; then
      echo "$now,$elapsed,$h" >> "$csv"
      end_height="$h"
      [[ -z "$t_escape" && "$h" -gt "$START_HEIGHT" ]] && { t_escape=$now; log "escaped cold-start at +${elapsed}s, height $h"; }
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid" 2>/dev/null || true
      log "zebrad exited (clean stop) at +${elapsed}s, height ${end_height}"
      break
    fi
    if (( elapsed >= WALL_CAP )); then
      log "wall cap ${WALL_CAP}s reached; stopping zebrad"
      kill "$pid" 2>/dev/null || true; sleep 5; kill -9 "$pid" 2>/dev/null || true
      break
    fi
    sleep "$SAMPLE_INTERVAL"
  done
  local t_end; t_end=$(date +%s)

  # final authoritative height (log scan catches the very last commit)
  h="$(scrape_height "$logf")" || true
  [[ -n "$h" && "$h" -gt "$end_height" ]] && end_height="$h"

  # quick error scan (ignore peer/network noise)
  local errs
  errs="$(grep -iE 'panic|ERROR committing|resetting state queue' "$logf" 2>/dev/null \
            | grep -viE 'zebra_network|peer' | head -3 || true)"
  cp "$logf" "$OUT_DIR/node-$prefix.log" 2>/dev/null || true

  local blocks=$((end_height - START_HEIGHT))
  local total=$((t_end - t0))
  local post=$total
  [[ -n "$t_escape" ]] && post=$((t_end - t_escape))
  (( total > 0 )) || total=1
  (( post  > 0 )) || post=1
  local bps  pbps stalled="no"
  bps="$(awk -v b="$blocks" -v t="$total" 'BEGIN{printf "%.2f", b/t}')"
  pbps="$(awk -v b="$blocks" -v t="$post"  'BEGIN{printf "%.2f", b/t}')"
  (( end_height < STOP_HEIGHT )) && stalled="yes (capped before stop_height)"

  rm -rf "$fork" "$cfg"   # reclaim divergent SSTs; keep csv/log artifacts
  CUR_PID=""; CUR_FORK=""

  RESULT_TAG="$tag"; RESULT_START="$START_HEIGHT"; RESULT_END="$end_height"
  RESULT_BLOCKS="$blocks"; RESULT_TIME="$total"; RESULT_POST="$post"
  RESULT_BPS="$bps"; RESULT_PBPS="$pbps"; RESULT_STALLED="$stalled"; RESULT_ERRS="$errs"
}

print_one() {
  local title="$1"
  cat <<EOF

=== checkpoint-sync benchmark ${title} ===
release:        $RESULT_TAG
feed peer:      $FEED_PEER  (single-peer)
start height:   $RESULT_START
end height:     $RESULT_END
blocks covered: $RESULT_BLOCKS
time taken:     ${RESULT_TIME} s
blocks/s:       $RESULT_BPS        (post-first-commit: $RESULT_PBPS blocks/s over ${RESULT_POST}s)
reached stop:   $( [[ "$RESULT_STALLED" == no ]] && echo yes || echo "$RESULT_STALLED" )
EOF
  [[ -n "$RESULT_ERRS" ]] && printf 'WARNING — log errors:\n%s\n' "$RESULT_ERRS"
}

summary_row() { # markdown row -> step summary
  printf '| %s | %s | %s | %s | %s | %s |\n' \
    "$1" "$RESULT_END" "$RESULT_BLOCKS" "${RESULT_TIME}s" "$RESULT_BPS" "$RESULT_PBPS"
}

# ---- main --------------------------------------------------------------------
ensure_deps
ensure_bench_home
ensure_snapshot

SUMMARY="${GITHUB_STEP_SUMMARY:-$OUT_DIR/summary.md}"
{
  echo "## Checkpoint-sync benchmark"
  echo ""
  echo "- snapshot start height: **$START_HEIGHT**, stop height: **$STOP_HEIGHT**, peer: \`$FEED_PEER\`"
  echo "- sync knobs: checkpoint_verify=$CKPT_LIMIT, download=$DL_LIMIT"
  echo ""
  echo "| tag | end height | blocks covered | time taken | blocks/s | post-commit blk/s |"
  echo "|-----|-----------:|---------------:|-----------:|---------:|------------------:|"
} >> "$SUMMARY"

if [[ -n "$BASELINE_TAG" ]]; then
  log "A/B mode: baseline=$BASELINE_TAG vs release=$RELEASE_TAG"
  run_one "$BASELINE_TAG" "baseline"; print_one "(baseline)"; summary_row "$BASELINE_TAG (baseline)" >> "$SUMMARY"
  B_BPS="$RESULT_BPS"; B_TIME="$RESULT_TIME"
  run_one "$RELEASE_TAG" "release";  print_one "(release)";  summary_row "$RELEASE_TAG (release)"  >> "$SUMMARY"
  R_BPS="$RESULT_BPS"
  SPEEDUP="$(awk -v r="$R_BPS" -v b="$B_BPS" 'BEGIN{ if (b>0) printf "%.2f", r/b; else print "n/a" }')"
  {
    echo ""
    echo "**Speedup:** ${B_BPS} → ${R_BPS} blocks/s = **${SPEEDUP}×**"
  } >> "$SUMMARY"
  printf '\n=== A/B: %s -> %s = %s× faster ===\n' "$B_BPS" "$R_BPS" "$SPEEDUP"
else
  run_one "$RELEASE_TAG" "release"; print_one ""; summary_row "$RELEASE_TAG" >> "$SUMMARY"
fi

log "done. artifacts in $OUT_DIR"
