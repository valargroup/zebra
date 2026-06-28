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
BASE_SRC="${REPLAY_BASE_SRC:-/mnt/roman-dev-2-data/zebra-ckpt-1800000-warm}"
CACHE="${REPLAY_CACHE:-/mnt/roman-dev-2-data/win-fwd.zrb}"
SIDECAR="${REPLAY_SIDECAR:-/mnt/roman-dev-2-data/win-fwd.vct}"
START="${REPLAY_START:-1802001}"
END="${REPLAY_END:-1832000}"
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

case "${1:-}" in
  index)       shift; cmd_index "$@" ;;
  index-roots) shift; cmd_index_roots "$@" ;;
  run)         shift; cmd_run "$@" ;;
  *) echo "usage: replay_run.sh {index|index-roots|run <label> [bin]}" >&2; exit 2 ;;
esac
