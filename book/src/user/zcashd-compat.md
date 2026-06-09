# zcashd-compat Mode (`zebrad start --zcashd-compat`)

zcashd-compat mode runs Zebra as the consensus source and optionally supervises a
`zcashd -zebra-compat` child process that uses Zebra's RPC endpoint for chain data,
mempool data, and transaction forwarding.

## What zcashd-compat mode does

When you start Zebra with:

```console
zebrad start --zcashd-compat
```

Zebra:

- enables zcashd-compat mode (`[zcashd_compat].enabled = true`);
- ensures an RPC listen address is configured (defaults to `127.0.0.1:8232`);
- forces cookie auth on (`rpc.enable_cookie_auth = true`);
- raises `rpc.max_response_body_size` if needed for large zcashd-compat batches;
- optionally spawns and supervises `zcashd -zebra-compat`.

If zcashd-compat supervision is enabled, Zebra starts `zcashd` with:

```text
-zebra-compat
-zebra-compat-url=<rpc_url>
-zebra-compat-cookiefile=<rpc.cookie_dir>/.cookie
-datadir=<zcashd_compat.zcashd_datadir or state.cache_dir/zcashd-compat-zcashd>
[-testnet | -regtest]
```

## Configuration

zcashd-compat mode adds a `[zcashd_compat]` section:

```toml
[zcashd_compat]
enabled = false
manage_zcashd = true
zcashd_source = "managed"                            # "managed" or "path"
zcashd_path = "/path/to/local/zcashd"               # optional explicit override
zcashd_datadir = "/path/to/zcashd/datadir"      # optional
zcashd_extra_args = ["-printtoconsole"]          # optional
rpc_url = "http://127.0.0.1:8232"               # optional, defaults from rpc.listen_addr
startup_delay = "1s"
restart_backoff = "2s"
max_restarts = 10
shutdown_grace_period = "10s"
```

When overriding `zcashd_extra_args` via environment variables, pass a JSON array string:

```console
ZEBRA_ZCASHD_COMPAT__ZCASHD_EXTRA_ARGS='["-conf=/path/to/zcash.conf","-printtoconsole"]'
```

If `manage_zcashd = false`, Zebra still applies zcashd-compat RPC guardrails, but
does not spawn `zcashd`.

If `manage_zcashd = true`, Zebra resolves `zcashd` as follows:

1. If `zcashd_path` is set, Zebra uses that local executable directly.
2. Otherwise, `zcashd_source = "managed"` uses Zebra's embedded release manifest
   to fetch a compatible `zcashd` archive, verify its SHA256, cache it, and run it.
3. `zcashd_source = "path"` requires `zcashd_path` to be set.

Managed downloads are cached under:

```text
<state.cache_dir>/zcashd-compat/bin/<release_tag>/<target>/zcashd
```

If managed artifacts are unavailable for the local platform, set `zcashd_path`
to a local binary instead.

## Quick regtest loop

1. Configure:

```toml
[network]
network = "Regtest"
```

1. Start Zebra in zcashd-compat mode:

```console
zebrad start --zcashd-compat
```

1. Confirm the startup log banner shows the zcashd-compat RPC URL and cookie file.

2. Use `zcash-cli getzebracompatinfo` to verify Zebra identity and readiness.

3. Generate blocks via Zebra RPC (`generate`) and verify `zcashd` follows.

## Notes

- `zcashd -zebra-compat` talks to Zebra over RPC, not over peer-to-peer connections.
- On shutdown, Zebra sends SIGTERM to the supervised `zcashd`, then SIGKILL
  after `shutdown_grace_period` if needed.
- Regtest interoperability can depend on matching assumptions between Zebra and
  `zcashd` builds. If regtest semantics diverge, use testnet for initial
  interoperability validation.

## Process lifecycle behavior

When zcashd-compat supervision is enabled (`zcashd_compat.enabled = true` and
`zcashd_compat.manage_zcashd = true`):

- If `zcashd` exits unexpectedly, Zebra's zcashd-compat supervisor restarts it using
  `restart_backoff`, up to `max_restarts`.
- If the zcashd-compat supervisor later exits or returns a runtime error (for
  example, spawn failures or restart-limit exhaustion while running), Zebra logs
  a warning and keeps running without zcashd supervision.
- Startup-time zcashd-compat config validation is unchanged. For example, if
  `zcashd_compat.manage_zcashd = true` and explicit `zcashd_path` cannot be resolved,
  Zebra startup fails with an error.
- Managed download failures (missing target artifact, hash mismatch, transport
  failures) fail closed before Zebra supervises `zcashd`.
- If `zebrad` is shut down normally, it asks the zcashd-compat supervisor to stop
  `zcashd` gracefully: SIGTERM first, then SIGKILL after
  `shutdown_grace_period` if needed.
- If `zebrad` is terminated ungracefully (for example `kill -9`), normal
  shutdown handlers do not run, so `zcashd` can remain running until it is
  stopped externally.
