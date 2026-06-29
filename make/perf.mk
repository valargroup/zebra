# Perf harness wrapper — thin Make targets over deploy/runner/perf.sh, the
# deterministic isolated-cohort benchmark driver (see deploy/runner/runbook.md).
#
# The serving cohort + bench live in deploy/runner/ (local tooling); these
# targets are stable entry points over it. Override the vars below on the
# command line, e.g.:
#   make perf-run     PERF_LABEL=r2 PERF_STOP=1825000
#   make perf-run     PERF_LABEL=r2 PERF_BIN_NAME=my-build
#   make perf-run     my-build
#   make perf-analyze PERF_LABEL=r2 PERF_LO=1806000 PERF_HI=1824000

.PHONY: \
	perf-build-local \
	perf-build-stage-bin \
	perf-build-replay-bench \
	perf-replay-index \
	perf-replay \
	perf-replay-worker \
	perf-replay-verifier \
	perf-replay-sequencer \
	perf-run \
	perf-run-mainnet \
	perf-analyze \
	perf-logs \
	perf-dashboard \
	perf-verify-isolation \
	perf-seed-serving \
	perf-peers \
	perf-freeze-serving \
	perf-status

PERF_SH ?= $(CURDIR)/deploy/runner/perf.sh
FEED_RUN ?= $(CURDIR)/deploy/runner/feed_run.sh
PERF_MAINNET_CONFIG ?= $(CURDIR)/deploy/runner/zebra-mainnet-bench-config.toml

PERF_GOAL_ARG := $(word 2,$(MAKECMDGOALS))
ifneq ($(filter perf-build-stage-bin perf-run perf-run-mainnet,$(firstword $(MAKECMDGOALS))),)
ifneq ($(PERF_GOAL_ARG),)
PERF_BIN_NAME ?= $(PERF_GOAL_ARG)
.PHONY: $(PERF_GOAL_ARG)
$(PERF_GOAL_ARG):
	@:
endif
endif

# perf-run / perf-analyze parameters (override on the command line).
PERF_LABEL         ?= r1
PERF_MAINNET_LABEL ?= r1-mainnet
PERF_STOP          ?= 1900000
PERF_LO            ?= 1810000
PERF_HI            ?= 1895000
PERF_BIN_NAME      ?=

# Build the instrumented (commit-metrics) local bench binary -> $BENCH_BIN.
perf-build-local:
	"$(PERF_SH)" build-local

# Build the instrumented bench binary and stage it as perf-artifacts/<name>.
perf-build-stage-bin:
	@if [ -n "$(PERF_BIN_NAME)" ]; then :; else echo "usage: make perf-build-stage-bin <name> (or PERF_BIN_NAME=<name>)" >&2; exit 2; fi
	"$(PERF_SH)" stage-bin "$(PERF_BIN_NAME)"

# Fork the snapshot and run an isolated bench against the cohort, emitting a CSV.
perf-run:
	PERF_BIN_NAME="$(PERF_BIN_NAME)" "$(PERF_SH)" run $(PERF_LABEL) $(PERF_STOP)

# Fork the snapshot and run against public Mainnet Zakura bootstrap peers.
perf-run-mainnet:
	bin="$$(PERF_BIN_NAME="$(PERF_BIN_NAME)" "$(PERF_SH)" bench-bin)" && CONFIG_SRC="$(PERF_MAINNET_CONFIG)" "$(FEED_RUN)" $(PERF_MAINNET_LABEL) "$$bin" $(PERF_STOP)

# ─── Offline commit-pipeline replay bench (zebra-replay-bench) ────────────────
# Replays real mainnet blocks through the state committer with NO networking, to
# benchmark the committer (write-assembler + disk-writer) in isolation. Forward
# model: the base snapshot's tip must equal REPLAY_START-1 (no rollback). Paths
# come from deploy/runner/cohort.env (REPLAY_* vars). `perf-replay-index` is a
# one-time setup; `perf-replay` is the repeatable A/B step. Add --vct-sidecar via
# REPLAY_VCT_SIDECAR to exercise the VCT fast path.
#   make perf-build-replay-bench   # build the bench binary (commit-metrics)
#   make perf-replay-index         # one-time: dump the window to a block cache
#   make perf-replay               # replay the cache through the committer
#   make perf-replay-worker        # same window, through the write worker
#   make perf-replay-verifier      # same window, through the checkpoint verifier
#   make perf-replay-sequencer     # same window, through the block-sync Sequencer (VCT)

PERF_REPLAY_RUN   ?= $(CURDIR)/deploy/runner/replay_run.sh
REPLAY_BIN        ?= $(CURDIR)/target/release/zebra-replay-bench
PERF_REPLAY_LABEL ?= r1-replay

# Build the replay bench binary (commit-metrics) into the default target dir.
perf-build-replay-bench:
	cargo build --release -p zebra-replay-bench --features commit-metrics --locked

# One-time: dump the configured window from the block source into the cache.
perf-replay-index:
	REPLAY_BIN="$(REPLAY_BIN)" "$(PERF_REPLAY_RUN)" index "$(REPLAY_BIN)"

# Repeatable: fork the base snapshot, apply the cache, report throughput.
perf-replay:
	REPLAY_BIN="$(REPLAY_BIN)" "$(PERF_REPLAY_RUN)" run $(PERF_REPLAY_LABEL) "$(REPLAY_BIN)"

# Repeatable: same window, but replayed through the real zebra-state write worker
# (one altitude above the direct committer). Set REPLAY_VCT_SIDECAR for VCT mode.
perf-replay-worker:
	REPLAY_BIN="$(REPLAY_BIN)" "$(PERF_REPLAY_RUN)" run-worker $(PERF_REPLAY_LABEL) "$(REPLAY_BIN)"

# Repeatable: same window, through the real zebra-consensus checkpoint verifier
# (commits to a real StateService; one altitude above the worker). Adds PoW/
# equihash + Merkle verification. Set REPLAY_VCT_SIDECAR for VCT mode.
perf-replay-verifier:
	REPLAY_BIN="$(REPLAY_BIN)" "$(PERF_REPLAY_RUN)" run-verifier $(PERF_REPLAY_LABEL) "$(REPLAY_BIN)"

# Repeatable: same window, through the real Zakura block-sync Sequencer (reorder +
# ordered submit to the verifier->state; one altitude above the verifier). VCT-only:
# requires REPLAY_VCT_SIDECAR. Commits in Pruned storage mode by default (BASE_SRC must
# be a pruned snapshot), matching the production mainnet config. Optional knobs:
#   REPLAY_ARCHIVE=1          commit in Archive storage mode instead (needs an archive base)
#   REPLAY_STOP_HEIGHT=<h>    stop at the last checkpoint <= h (bench a sub-range of the cache)
#   REPLAY_TRACE_DIR=<dir>    write structured Zakura JSONL traces (block_sync.jsonl),
#                             the same tables perf-run-mainnet emits via trace_dir; plot
#                             with .cursor/skills/zakura-trace-plots
perf-replay-sequencer:
	REPLAY_BIN="$(REPLAY_BIN)" "$(PERF_REPLAY_RUN)" run-sequencer $(PERF_REPLAY_LABEL) "$(REPLAY_BIN)"

# Steady-state bottleneck attribution over the CSV window [PERF_LO, PERF_HI].
perf-analyze:
	"$(PERF_SH)" analyze $(PERF_LABEL) $(PERF_LO) $(PERF_HI)

# Follow the running bench node's log (byte-budget drift spam filtered).
# Pass RAW=1 to include everything, LINES=N for a different backlog size.
perf-logs:
	"$(PERF_SH)" logs $(PERF_LABEL)

# Live metrics dashboard (auto-detects the running bench node's metrics port).
perf-dashboard:
	"$(PERF_SH)" dashboard

# Confirm the bench node sees only the two cohort peers and no rejects.
perf-verify-isolation:
	"$(PERF_SH)" verify-isolation

# ─── Serving-cohort lifecycle (one-time / occasional) ─────────────────────────

# Deploy both serving nodes and sync them from public mainnet to SEED_HEIGHT.
perf-seed-serving:
	"$(PERF_SH)" seed-serving

# Capture each serving node's node_id@ip:8234 into deploy/runner/cohort.env.
perf-peers:
	"$(PERF_SH)" peers

# Redeploy the serving nodes cohort-isolated (legacy off) to serve a static range.
perf-freeze-serving:
	"$(PERF_SH)" freeze-serving

# Report each serving node's service state and version.
perf-status:
	"$(PERF_SH)" status
