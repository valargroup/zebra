# unity-node (POC)

`unity-node` is a Rust launcher that runs:
- `zebrad` as the primary consensus process
- `zallet` as the preferred wallet-service (optional in manifest)
- `zcashd` as an optional fallback wallet provider (`connect=127.0.0.1:<zebra_p2p>`)

## Commands

- `unity-node start --network regtest --state-dir /var/lib/unity-node --manifest unity-node/manifest.toml`
- `unity-node start --network mainnet-like --state-dir /var/lib/unity-node --manifest unity-node/manifest.toml --regtest-producer external --canonical-regtest true`
- `unity-node status --network regtest --state-dir /var/lib/unity-node --manifest unity-node/manifest.toml`
- `unity-node stop --network regtest --state-dir /var/lib/unity-node`
- `unity-node start --network regtest --regtest-producer external`
- `unity-node start --network regtest --canonical-regtest true --follower-lag-tolerance 20`

## Behavior

- Verifies pinned binary hashes from `manifest.toml` before launch.
- Renders `zebra.toml`, `zcash.conf`, and `zallet.toml` under `<state-dir>/<network>/`.
- Startup order: `zebrad`, then optional `zallet`, then optional `zcashd` fallback.
- Writes `run/wallet-routing.json` with the current P0 wallet routing assignment.
- `zcashd` startup is controlled by wallet routing fallback needs.
- Fallback metadata includes all zcashd-supported wallet methods grouped by `P0`/`P1`/`P2`.
- `zallet` bootstrap prerequisites are enforced before startup:
  - schema-compatible `zallet.toml` in `<state-dir>/<network>/zallet/zallet.toml`,
  - an age identity file at `<state-dir>/<network>/zallet/encryption-identity.txt`.
- `unity-node` attempts to generate the identity file with `rage-keygen` / `age-keygen` when missing.
- Readiness gates include zallet JSON-RPC (`getwalletinfo`) when zallet is enabled.
- `--network mainnet-like` runs a private, height-0 profile on Zebra's Regtest engine with:
  - compressed mainnet-like activation sequence (`Overwinter` through `NU6.2` at low private heights),
  - `disable_pow = true`,
  - isolated ports (`19235` P2P, `9232` Zebra RPC, `19233` zcashd RPC).
- Regtest producer modes:
  - `--regtest-producer external` (default): runs an external side process to produce blocks.
    - If `--external-producer-cmd` is set, executes that command via `sh -c`.
    - Otherwise runs the built-in producer harness (`producer-harness`) which mines via `zcashd` `generate`, fetches raw block hex via `getblock`, then submits to Zebra using `submitblock`.
  - `--regtest-producer internal`: enables Zebra internal miner.
- Readiness gates:
  - Zebra RPC `getblockchaininfo` reachable via cookie auth
  - zcashd `getpeerinfo` reports exactly one peer
  - In canonical regtest mode (`--canonical-regtest true`), the single peer must be Zebra loopback P2P
- Canonical private verification (enabled when `--canonical-regtest true` on `regtest` or `mainnet-like`):
  - Runs a 60-poll verification window (~120s) after startup
  - Requires Zebra height growth during the window
  - Fails if zcashd remains ahead of Zebra for sustained polls
  - Enforces final follower lag <= `--follower-lag-tolerance`
  - Re-checks canonical single-peer isolation during the window
- Uses PID files for lifecycle:
  - `<state-dir>/<network>/run/zebrad.pid`
  - `<state-dir>/<network>/run/zallet.pid`
  - `<state-dir>/<network>/run/zcashd.pid`
  - `<state-dir>/<network>/run/producer.pid`

## Mainnet-like runbook

Use this for a private height-0 network where `zebrad` is primary and `zcashd` follows.

1. Start:
   - `unity-node start --network mainnet-like --state-dir /var/lib/unity-node --manifest unity-node/manifest.toml --timeout-secs 180 --canonical-regtest true --follower-lag-tolerance 20 --regtest-producer external --producer-interval-secs 2`
2. Check health:
   - `unity-node status --network mainnet-like --state-dir /var/lib/unity-node --manifest unity-node/manifest.toml`
   - Expected: `zebrad rpc: ok`, `zcashd rpc: ok`, `zcashd peers: 1`.
3. Wallet smoke:
   - `zcash-cli -conf=/var/lib/unity-node/mainnet-like/zcash.conf getbalance`
   - `zcash-cli -conf=/var/lib/unity-node/mainnet-like/zcash.conf getnewaddress`
   - `zcash-cli -conf=/var/lib/unity-node/mainnet-like/zcash.conf sendtoaddress <addr> 1.0`
4. Stop:
   - `unity-node stop --network mainnet-like --state-dir /var/lib/unity-node`

## Troubleshooting

- `submitblock ... FoundersRewardNotFound` on height 1:
  - Activation heights are not canopy-at-1 in mainnet-like mode; regenerate config with current launcher and restart from clean state.
- `missing lockbox disbursements for NU6.1 activation block`:
  - NU6.1 activation is too early for `zcashd` generated blocks; use the launcher's default deferred NU6.1/NU6.2 heights.
- Canonical divergence (`zcashd ahead of zebra`):
  - Check `/var/lib/unity-node/mainnet-like/logs/producer.log` and `zebrad.log` for `submitblock rejected`.
  - Restart with clean state directory and confirm peer isolation (`zcashd peers: 1`, addr `127.0.0.1:19235`).
- Zallet fails with `Failed to fetch blockchain info`:
  - Verify Zebra RPC is healthy and cookie auth is enabled.
  - Confirm zallet config points indexer validator settings to Zebra (`127.0.0.1:9232` in mainnet-like) and the cookie path under `cookies/zebra/.cookie`.
- Zallet fails with missing encryption identity:
  - Ensure `rage-keygen` is installed or manually create `<state-dir>/<network>/zallet/encryption-identity.txt`.
