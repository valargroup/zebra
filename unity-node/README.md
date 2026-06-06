# unity-node (POC)

`unity-node` is a Rust launcher that runs:
- `zebrad` as the primary consensus process
- `zcashd` as a wallet follower (`connect=127.0.0.1:<zebra_p2p>`)

## Commands

- `unity-node start --network regtest --state-dir /var/lib/unity-node --manifest unity-node/manifest.toml`
- `unity-node status --network regtest --state-dir /var/lib/unity-node --manifest unity-node/manifest.toml`
- `unity-node stop --network regtest --state-dir /var/lib/unity-node`
- `unity-node start --network regtest --regtest-producer external`
- `unity-node start --network regtest --canonical-regtest true --follower-lag-tolerance 20`

## Behavior

- Verifies pinned binary hashes from `manifest.toml` before launch.
- Renders `zebra.toml` and `zcash.conf` under `<state-dir>/<network>/`.
- Startup order: `zebrad` first, then `zcashd`.
- Regtest producer modes:
  - `--regtest-producer external` (default): runs an external side process to produce blocks.
    - If `--external-producer-cmd` is set, executes that command via `sh -c`.
    - Otherwise runs the built-in producer harness (`producer-harness`) which mines via `zcashd` `generate`, fetches raw block hex via `getblock`, then submits to Zebra using `submitblock`.
  - `--regtest-producer internal`: enables Zebra internal miner.
- Readiness gates:
  - Zebra RPC `getblockchaininfo` reachable via cookie auth
  - zcashd `getpeerinfo` reports exactly one peer
  - In canonical regtest mode (`--canonical-regtest true`), the single peer must be Zebra loopback P2P
- Canonical regtest verification (enabled when `--canonical-regtest true`):
  - Runs a 60-poll verification window (~120s) after startup
  - Requires Zebra height growth during the window
  - Fails if zcashd remains ahead of Zebra for sustained polls
  - Enforces final follower lag <= `--follower-lag-tolerance`
  - Re-checks canonical single-peer isolation during the window
- Uses PID files for lifecycle:
  - `<state-dir>/<network>/run/zebrad.pid`
  - `<state-dir>/<network>/run/zcashd.pid`
  - `<state-dir>/<network>/run/producer.pid`
