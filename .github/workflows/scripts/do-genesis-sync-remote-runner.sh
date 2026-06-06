#!/usr/bin/env bash
set -euo pipefail

TARGET_BLOCKS=${TARGET_BLOCKS:?TARGET_BLOCKS is required}
BENCH_VARIANT=${BENCH_VARIANT:?BENCH_VARIANT is required}
MAX_ELAPSED_SECONDS=${MAX_ELAPSED_SECONDS:-14400}
MAX_STALL_SECONDS=${MAX_STALL_SECONDS:-900}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  build-essential \
  ca-certificates \
  clang \
  cmake \
  curl \
  git \
  jq \
  libssl-dev \
  lsof \
  pkg-config \
  procps \
  protobuf-compiler \
  sysstat \
  tar \
  xz-utils

if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
    sh -s -- -y --profile minimal
fi

# shellcheck source=/dev/null
source "$HOME/.cargo/env"

cd /mnt/zebra/src
cargo build --release --locked --bin zebrad

VARIANT="$BENCH_VARIANT" \
  MAX_ELAPSED_SECONDS="$MAX_ELAPSED_SECONDS" \
  MAX_STALL_SECONDS="$MAX_STALL_SECONDS" \
  SOURCE_REF="${SOURCE_REF:-}" \
  SOURCE_SHA="${SOURCE_SHA:-}" \
  WORKFLOW_REF="${WORKFLOW_REF:-}" \
  WORKFLOW_SHA="${WORKFLOW_SHA:-}" \
  bash "$SCRIPT_DIR/do-genesis-sync-benchmark.sh" \
    /mnt/zebra/src/target/release/zebrad \
    "$TARGET_BLOCKS"
