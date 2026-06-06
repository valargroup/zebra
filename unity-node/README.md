# unity-node (POC)

`unity-node` is a Rust launcher that runs:
- `zebrad` as the primary consensus process
- `zcashd` as a wallet follower (`connect=127.0.0.1:<zebra_p2p>`)

## Commands

- `unity-node start --network regtest --state-dir /var/lib/unity-node --manifest unity-node/manifest.toml`
- `unity-node status --network regtest --state-dir /var/lib/unity-node --manifest unity-node/manifest.toml`
- `unity-node stop --network regtest --state-dir /var/lib/unity-node`
- `unity-node start --network regtest --regtest-producer external --canonical-regtest true`

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
  - zcashd `getpeerinfo` reports exactly one peer (and in canonical regtest mode, that peer must be Zebra loopback P2P)
- Canonical regtest verification:
  - Zebra height must increase.
  - zcashd must not remain ahead of Zebra for sustained polling windows.
  - zcashd lag behind Zebra must stay under `--follower-lag-tolerance`.
- Uses PID files for lifecycle:
  - `<state-dir>/<network>/run/zebrad.pid`
  - `<state-dir>/<network>/run/zcashd.pid`
  - `<state-dir>/<network>/run/producer.pid`
