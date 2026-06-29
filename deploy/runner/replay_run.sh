#!/usr/bin/env bash
# Offline commit-pipeline replay bench (zebra-replay-bench).
#
# Replays real mainnet blocks through the state committer with NO networking, to
# benchmark the write-assembler + disk-writer in isolation. Forward model (no
# rollback): the base snapshot's tip must equal REPLAY_START-1, and blocks are
# replayed onto a fork of it.
#
#   replay_run.sh index             # fork the block source, dump the window to a cache
#   replay_run.sh index-roots       # fork the source, derive per-height roots -> VCT sidecar
#   replay_run.sh run <label> BIN   # fork the base, apply the cache, time it
#
# `index` (and `index-roots` for VCT) are one-time setup. `run` is the repeatable
# A/B step — it re-forks the base each time so commits never touch the source.
# Set REPLAY_VCT_SIDECAR to a sidecar path to drive the VCT fast path in `run`.
#
# All host paths come from cohort.env (REPLAY_* / BENCH_* vars) or the environment.
#
# NOTE: rolling a single snapshot back to manufacture a lower base is intentionally
# NOT used here — rollback of a large span builds one giant in-memory delete batch
# and OOMs. Use two snapshots (a near-tip source + a base at START-1) instead. The
# binary still exposes a `rollback` subcommand for small spans if ever needed.
set -uo pipefail

RUNNER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
[ -f "$RUNNER_DIR/cohort.env" ] && source "$RUNNER_DIR/cohort.env"

DBREL="${BENCH_DB_REL:-state/v27/mainnet}"
SRC="${REPLAY_SRC:-/mnt/roman-dev-2-data/zebra-cache}"
# Base = the cleanly-pruned snapshot at tip 1849957 (START-1). It was produced by
# replaying the older 1800000 pruned base forward past 1849957 so the checkpoint
# raw-tx archive backlog is already drained: forking it for a `run` does NOT
# trigger a startup DrainBacklog (the earlier 1800000-warm-pruned base did, which
# polluted the first ~10K blocks of every run with delete/compaction churn).
BASE_SRC="${REPLAY_BASE_SRC:-/mnt/roman-dev-2-data/zebra-ckpt-1850000-warm-pruned}"
CACHE="${REPLAY_CACHE:-/mnt/roman-dev-2-data/win-1850k.zrb}"
SIDECAR="${REPLAY_SIDECAR:-/mnt/roman-dev-2-data/win-1850k.vct}"
START="${REPLAY_START:-1849958}"
END="${REPLAY_END:-1899957}"
FORK_DIR="${BENCH_FORK_DIR:-/mnt/roman-dev-2-data}"
BIN_DEFAULT="${REPLAY_BIN:-/root/wal-bench/zebra-replay-bench}"

die()  { echo "FATAL: $*" >&2; exit 1; }
note() { echo "[replay] $*" >&2; }

# Hard-link fork SRC -> DST, breaking links on RocksDB mutable metadata so writes
# to the fork can never reach the source snapshot (same trick as feed_run.sh).
clone_fork() {
  local src="$1" dst="$2"
  [ -d "$src/$DBREL" ] || die "source DB missing: $src/$DBREL"
  rm -rf "$dst"
  mkdir -p "$dst/$(dirname "$DBREL")"
  cp -al "$src/$DBREL" "$dst/$DBREL"
  ( cd "$dst/$DBREL"
    for f in CURRENT IDENTITY LOG LOCK OPTIONS-* MANIFEST-* *.log version; do
      [ -e "$f" ] || continue
      cp -p "$f" "$f.unlink" && mv -f "$f.unlink" "$f"
    done )
}

cmd_index() {
  local bin="${1:-$BIN_DEFAULT}"
  [ -x "$bin" ] || die "binary not executable: $bin"
  local src="$FORK_DIR/replay-src-fork"
  note "fork $SRC -> $src; index $START..$END -> $CACHE"
  clone_fork "$SRC" "$src"
  "$bin" index --src "$src" --cache "$CACHE" --start "$START" --end "$END"
  rm -rf "$src"
  note "index done: $CACHE"
}

cmd_index_roots() {
  local bin="${1:-$BIN_DEFAULT}"
  [ -x "$bin" ] || die "binary not executable: $bin"
  local src="$FORK_DIR/replay-src-fork"
  note "fork $SRC -> $src; derive roots $START..$END -> $SIDECAR"
  clone_fork "$SRC" "$src"
  "$bin" index-roots --src "$src" --sidecar "$SIDECAR" --start "$START" --end "$END"
  rm -rf "$src"
  note "roots sidecar done: $SIDECAR"
}

cmd_run() {
  local label="${1:?usage: replay_run.sh run <label> [bin]}"; shift || true
  local bin="${1:-$BIN_DEFAULT}"
  [ -x "$bin" ] || die "binary not executable: $bin (build with 'make perf-build-replay-bench')"
  [ -f "$CACHE" ] || die "cache missing: $CACHE (run 'replay_run.sh index' first)"
  local fork="$FORK_DIR/replay-run-$label"
  note "fork base $BASE_SRC -> $fork; apply $CACHE (expects base tip $((START - 1)))"
  clone_fork "$BASE_SRC" "$fork"
  if [ -n "${REPLAY_VCT_SIDECAR:-}" ]; then
    [ -f "$REPLAY_VCT_SIDECAR" ] || die "VCT sidecar missing: $REPLAY_VCT_SIDECAR (run 'replay_run.sh index-roots')"
    note "VCT mode: --vct-sidecar $REPLAY_VCT_SIDECAR"
    "$bin" apply --base "$fork" --cache "$CACHE" --vct-sidecar "$REPLAY_VCT_SIDECAR"
  else
    "$bin" apply --base "$fork" --cache "$CACHE"
  fi
  note "cleanup: rm -rf $fork"
  rm -rf "$fork"
}

# Like cmd_run, but replays through the real zebra-state write worker
# (apply-worker), one altitude above the direct committer.
cmd_run_worker() {
  local label="${1:?usage: replay_run.sh run-worker <label> [bin]}"; shift || true
  local bin="${1:-$BIN_DEFAULT}"
  [ -x "$bin" ] || die "binary not executable: $bin (build with 'make perf-build-replay-bench')"
  [ -f "$CACHE" ] || die "cache missing: $CACHE (run 'replay_run.sh index' first)"
  local fork="$FORK_DIR/replay-worker-$label"
  note "fork base $BASE_SRC -> $fork; apply-worker $CACHE (expects base tip $((START - 1)))"
  clone_fork "$BASE_SRC" "$fork"
  if [ -n "${REPLAY_VCT_SIDECAR:-}" ]; then
    [ -f "$REPLAY_VCT_SIDECAR" ] || die "VCT sidecar missing: $REPLAY_VCT_SIDECAR (run 'replay_run.sh index-roots')"
    note "VCT mode: --vct-sidecar $REPLAY_VCT_SIDECAR"
    "$bin" apply-worker --base "$fork" --cache "$CACHE" --vct-sidecar "$REPLAY_VCT_SIDECAR"
  else
    "$bin" apply-worker --base "$fork" --cache "$CACHE"
  fi
  note "cleanup: rm -rf $fork"
  rm -rf "$fork"
}

# Like cmd_run_worker, but replays through the real zebra-consensus checkpoint
# verifier (which commits to a real StateService), one altitude above the worker.
cmd_run_verifier() {
  local label="${1:?usage: replay_run.sh run-verifier <label> [bin]}"; shift || true
  local bin="${1:-$BIN_DEFAULT}"
  [ -x "$bin" ] || die "binary not executable: $bin (build with 'make perf-build-replay-bench')"
  [ -f "$CACHE" ] || die "cache missing: $CACHE (run 'replay_run.sh index' first)"
  local fork="$FORK_DIR/replay-verifier-$label"
  note "fork base $BASE_SRC -> $fork; apply-verifier $CACHE (expects base tip $((START - 1)))"
  clone_fork "$BASE_SRC" "$fork"
  if [ -n "${REPLAY_VCT_SIDECAR:-}" ]; then
    [ -f "$REPLAY_VCT_SIDECAR" ] || die "VCT sidecar missing: $REPLAY_VCT_SIDECAR (run 'replay_run.sh index-roots')"
    note "VCT mode: --vct-sidecar $REPLAY_VCT_SIDECAR"
    "$bin" apply-verifier --base "$fork" --cache "$CACHE" --vct-sidecar "$REPLAY_VCT_SIDECAR"
  else
    "$bin" apply-verifier --base "$fork" --cache "$CACHE"
  fi
  note "cleanup: rm -rf $fork"
  rm -rf "$fork"
}

# Like cmd_run_verifier, but replays through the real Zakura block-sync Sequencer
# (reorder + ordered submit to the verifier->state). VCT-only.
cmd_run_sequencer() {
  local label="${1:?usage: replay_run.sh run-sequencer <label> [bin]}"; shift || true
  local bin="${1:-$BIN_DEFAULT}"
  [ -x "$bin" ] || die "binary not executable: $bin (build with 'make perf-build-replay-bench')"
  [ -f "$CACHE" ] || die "cache missing: $CACHE (run 'replay_run.sh index' first)"
  [ -n "${REPLAY_VCT_SIDECAR:-}" ] || die "apply-sequencer is VCT-only; set REPLAY_VCT_SIDECAR (run 'replay_run.sh index-roots')"
  [ -f "$REPLAY_VCT_SIDECAR" ] || die "VCT sidecar missing: $REPLAY_VCT_SIDECAR"
  local fork="$FORK_DIR/replay-sequencer-$label"
  note "fork base $BASE_SRC -> $fork; apply-sequencer $CACHE (VCT; expects base tip $((START - 1)))"
  clone_fork "$BASE_SRC" "$fork"
  local args=(apply-sequencer --base "$fork" --cache "$CACHE" --vct-sidecar "$REPLAY_VCT_SIDECAR")
  # Storage mode: Pruned by default (BASE_SRC must already be a pruned snapshot; pruning
  # is one-way). REPLAY_ARCHIVE=1 opts back into Archive (needs an archive base).
  if [ -n "${REPLAY_ARCHIVE:-}" ]; then
    note "storage mode: archive"
    args+=(--archive)
  else
    note "storage mode: pruned (default)"
  fi
  # Clamp the committed window to a sub-range of the cache (stops at the last
  # checkpoint <= REPLAY_STOP_HEIGHT). Lets a smaller window reuse a larger cache.
  if [ -n "${REPLAY_STOP_HEIGHT:-}" ]; then
    note "stop height: $REPLAY_STOP_HEIGHT"
    args+=(--stop-height "$REPLAY_STOP_HEIGHT")
  fi
  # Structured Zakura JSONL traces, like perf-run-mainnet's [network.zakura] trace_dir.
  if [ -n "${REPLAY_TRACE_DIR:-}" ]; then
    note "Zakura JSONL traces -> $REPLAY_TRACE_DIR"
    args+=(--trace-dir "$REPLAY_TRACE_DIR")
  fi
  "$bin" "${args[@]}"
  note "cleanup: rm -rf $fork"
  rm -rf "$fork"
}

case "${1:-}" in
  index)         shift; cmd_index "$@" ;;
  index-roots)   shift; cmd_index_roots "$@" ;;
  run)           shift; cmd_run "$@" ;;
  run-worker)    shift; cmd_run_worker "$@" ;;
  run-verifier)  shift; cmd_run_verifier "$@" ;;
  run-sequencer) shift; cmd_run_sequencer "$@" ;;
  *) echo "usage: replay_run.sh {index|index-roots|run <label> [bin]|run-worker <label> [bin]|run-verifier <label> [bin]|run-sequencer <label> [bin]}" >&2; exit 2 ;;
esac
