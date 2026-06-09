.PHONY: \
	compat-zebrad-start-supervised-managed \
	compat-zebrad-start-supervised \
	compat-zebrad-start-unsupervised \
	compat-zcashd-start-standalone \
	compat-zebrad-status \
	compat-zcashd-status \
	compat-status-sync

ZEBRAD_BIN ?= $(CURDIR)/target/release/zebrad
ZCASHD_BIN ?= /root/unity/zcash/src/zcashd
ZCASH_CLI_BIN ?= /root/unity/zcash/src/zcash-cli

# TODO: make more general
NETWORK ?= Testnet
ZEBRA_STATE_CACHE_DIR ?= /mnt/data/zebra-state-testnet
ZEBRA_RPC_LISTEN_ADDR ?= 127.0.0.1:28232
ZCASHD_COMPAT_COOKIE_DIR ?= $(HOME)/.cache/zebra/zcashd-compat-rpc
ZCASHD_DATADIR ?= /mnt/data/zcashd-profile-b/.zcashd
ZCASHD_CONF ?= $(CURDIR)/deploy/profile-b/zcash.testnet.zebra-compat.conf
ZCASHD_EXTRA_ARGS ?= -printtoconsole

ZEBRA_RPC_URL ?= http://$(ZEBRA_RPC_LISTEN_ADDR)
ZEBRA_COOKIE_FILE ?= $(ZCASHD_COMPAT_COOKIE_DIR)/.cookie
ZEBRA_ZCASHD_EXTRA_ARGS ?= ["-conf=$(ZCASHD_CONF)","-printtoconsole"]
HEIGHT_MAX_DRIFT ?= 50

compat-zebrad-start-supervised-managed:
	@echo "Starting zebrad in zcashd-compat mode with managed zcashd download..."
	ZEBRA_NETWORK__NETWORK="$(NETWORK)" \
	ZEBRA_STATE__CACHE_DIR="$(ZEBRA_STATE_CACHE_DIR)" \
	ZEBRA_ZCASHD_COMPAT__LISTEN_ADDR="$(ZEBRA_RPC_LISTEN_ADDR)" \
	ZEBRA_ZCASHD_COMPAT__COOKIE_DIR="$(ZCASHD_COMPAT_COOKIE_DIR)" \
	ZEBRA_ZCASHD_COMPAT__ZCASHD_SOURCE=managed \
	ZEBRA_ZCASHD_COMPAT__ZCASHD_DATADIR="$(ZCASHD_DATADIR)" \
	ZEBRA_ZCASHD_COMPAT__ZCASHD_EXTRA_ARGS='$(ZEBRA_ZCASHD_EXTRA_ARGS)' \
	"$(ZEBRAD_BIN)" start --zcashd-compat

compat-zebrad-start-supervised:
	@echo "Starting zebrad in zcashd-compat mode with supervision enabled..."
	ZEBRA_NETWORK__NETWORK="$(NETWORK)" \
	ZEBRA_STATE__CACHE_DIR="$(ZEBRA_STATE_CACHE_DIR)" \
	ZEBRA_ZCASHD_COMPAT__LISTEN_ADDR="$(ZEBRA_RPC_LISTEN_ADDR)" \
	ZEBRA_ZCASHD_COMPAT__COOKIE_DIR="$(ZCASHD_COMPAT_COOKIE_DIR)" \
	ZEBRA_ZCASHD_COMPAT__ZCASHD_SOURCE=path \
	ZEBRA_ZCASHD_COMPAT__ZCASHD_PATH="$(ZCASHD_BIN)" \
	ZEBRA_ZCASHD_COMPAT__ZCASHD_DATADIR="$(ZCASHD_DATADIR)" \
	ZEBRA_ZCASHD_COMPAT__ZCASHD_EXTRA_ARGS='$(ZEBRA_ZCASHD_EXTRA_ARGS)' \
	"$(ZEBRAD_BIN)" start --zcashd-compat

compat-zebrad-start-unsupervised:
	@echo "Starting zebrad in zcashd-compat mode with supervision disabled..."
	ZEBRA_NETWORK__NETWORK="$(NETWORK)" \
	ZEBRA_STATE__CACHE_DIR="$(ZEBRA_STATE_CACHE_DIR)" \
	ZEBRA_ZCASHD_COMPAT__LISTEN_ADDR="$(ZEBRA_RPC_LISTEN_ADDR)" \
	ZEBRA_ZCASHD_COMPAT__COOKIE_DIR="$(ZCASHD_COMPAT_COOKIE_DIR)" \
	ZEBRA_ZCASHD_COMPAT__MANAGE_ZCASHD=false \
	ZEBRA_ZCASHD_COMPAT__ZCASHD_SOURCE=path \
	ZEBRA_ZCASHD_COMPAT__ZCASHD_PATH="$(ZCASHD_BIN)" \
	ZEBRA_ZCASHD_COMPAT__ZCASHD_DATADIR="$(ZCASHD_DATADIR)" \
	ZEBRA_ZCASHD_COMPAT__ZCASHD_EXTRA_ARGS='$(ZEBRA_ZCASHD_EXTRA_ARGS)' \
	"$(ZEBRAD_BIN)" start --zcashd-compat

compat-zcashd-start-standalone:
	@echo "Starting zcashd -zebra-compat as a standalone process..."
	"$(ZCASHD_BIN)" \
		-zebra-compat \
		-zebra-compat-url="$(ZEBRA_RPC_URL)" \
		-zebra-compat-cookiefile="$(ZEBRA_COOKIE_FILE)" \
		-datadir="$(ZCASHD_DATADIR)" \
		-conf="$(ZCASHD_CONF)" \
		$(ZCASHD_EXTRA_ARGS)

compat-zebrad-status:
	@echo "Checking zebrad process..."
	@if pgrep -f "zebrad start --zcashd-compat" >/dev/null; then \
		echo "zebrad process: OK"; \
	else \
		echo "zebrad process: NOT RUNNING"; \
		exit 1; \
	fi
	@echo "Checking Zebra RPC getblockcount..."
	@if [ ! -f "$(ZEBRA_COOKIE_FILE)" ]; then \
		echo "Zebra cookie file missing: $(ZEBRA_COOKIE_FILE)"; \
		exit 1; \
	fi
	@zebra_height="$$(curl -sS --fail --user "$$(cat "$(ZEBRA_COOKIE_FILE)")" \
		-H 'Content-Type: application/json' \
		--data '{"jsonrpc":"1.0","id":"make","method":"getblockcount","params":[]}' \
		"$(ZEBRA_RPC_URL)" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"])')"; \
		echo "zebrad RPC height: $$zebra_height"

compat-zcashd-status:
	@echo "Checking zcashd process..."
	@if pgrep -f "zcashd.*-zebra-compat" >/dev/null; then \
		echo "zcashd process: OK"; \
	else \
		echo "zcashd process: NOT RUNNING"; \
		exit 1; \
	fi
	@echo "Checking zcashd zebra-compat status..."
	@"$(ZCASH_CLI_BIN)" -conf="$(ZCASHD_CONF)" -datadir="$(ZCASHD_DATADIR)" getzebracompatinfo >/dev/null
	@zcashd_height="$$( "$(ZCASH_CLI_BIN)" -conf="$(ZCASHD_CONF)" -datadir="$(ZCASHD_DATADIR)" getblockcount )"; \
		echo "zcashd height: $$zcashd_height"

compat-status-sync:
	@$(MAKE) compat-zebrad-status
	@$(MAKE) compat-zcashd-status
	@zebra_height="$$(curl -sS --fail --user "$$(cat "$(ZEBRA_COOKIE_FILE)")" \
		-H 'Content-Type: application/json' \
		--data '{"jsonrpc":"1.0","id":"make","method":"getblockcount","params":[]}' \
		"$(ZEBRA_RPC_URL)" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"])')"; \
		zcashd_height="$$( "$(ZCASH_CLI_BIN)" -conf="$(ZCASHD_CONF)" -datadir="$(ZCASHD_DATADIR)" getblockcount )"; \
		drift=$$(( zebra_height - zcashd_height )); \
		if [ $$drift -lt 0 ]; then drift=$$(( -drift )); fi; \
		echo "zebrad height: $$zebra_height"; \
		echo "zcashd height: $$zcashd_height"; \
		echo "height drift: $$drift (max allowed: $(HEIGHT_MAX_DRIFT))"; \
		if [ $$drift -gt "$(HEIGHT_MAX_DRIFT)" ]; then \
			echo "ERROR: height drift exceeded threshold"; \
			exit 1; \
		fi
