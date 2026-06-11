# Forked Mainnet

Forked Mainnet lets developers start a local fork from an existing Mainnet state
at a chosen anchor height. Blocks up to the anchor are read from the normal
Mainnet finalized database, while post-fork blocks stay in a fork-specific
non-finalized cache.

Use forked-mainnet when tests need real Mainnet chain history and note
commitment or UTXO state. Use [Regtest](./regtest.md) or
[Custom Testnets](./custom-testnets.md) when a test can start from a fresh chain.

## Safety Model

Forked Mainnet intentionally reuses the Mainnet finalized state. To keep this
safe:

- Zebra never finalizes fork-only blocks when running a forked-mainnet config.
- Fork-only blocks are stored in
  `<state.cache_dir>/non_finalized_state/forkedmainnet_<fork_name>`.
- Forked nodes use a distinct network magic and do not use public Mainnet or
  Testnet seed peers by default.
- To return to public Mainnet, stop Zebra, delete the fork's non-finalized cache,
  and restart with a normal Mainnet config.

Do not run a forked-mainnet config against a Mainnet database whose finalized tip
does not match the configured fork anchor.

## Preparing A Fork Config

First sync a normal Mainnet Zebra node to the height where the fork should
start:

```console
zebrad -c mainnet.toml start
```

Zebra exposes a JSON-RPC endpoint (there is no `zcash-cli`); query it with `curl`.
The helper below posts a method and params to the RPC port from your Mainnet
config (`rpc.listen_addr`, `8232` by default):

```console
zrpc() { curl -s --data-binary "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":$2}" \
  -H 'content-type:application/json' 127.0.0.1:8232; }
```

The fork anchor must be the **finalized** database tip, not the best chain tip.
Zebra keeps roughly the last 100 blocks in non-finalized state, so after you stop
the node the finalized tip is up to ~100 blocks below the height reported by
`getblockchaininfo`. You do not have to compute this exactly: Zebra validates the
anchor on startup and, if it does not match, the error message reports the exact
finalized height and hash to use. A convenient first pass is to use the best tip:

```console
ANCHOR_HEIGHT=$(zrpc getblockcount '[]' | sed -E 's/.*"result":([0-9]+).*/\1/')
ANCHOR_HASH=$(zrpc getblockhash "[$ANCHOR_HEIGHT]" | sed -E 's/.*"result":"([0-9a-f]+)".*/\1/')
```

Stop Zebra before preparing or starting the fork. Then generate a fork config
(if the anchor is wrong, Zebra's startup error tells you the finalized height and
hash to put here):

```console
zebrad fork-mainnet prepare \
  --height "$ANCHOR_HEIGHT" \
  --hash "$ANCHOR_HASH" \
  --name LocalFork \
  --network-magic a1b2c3d4 \
  --activation NU7=3400100 \
  --disable-pow \
  --output-file forked-mainnet.toml
```

The fork name is used in logs and cache directory names. The network magic must
be unique for the fork; peers with a different magic are rejected during
handshake.

By default, `fork-mainnet prepare` enables `--easy-difficulty=true`, which writes
an easy post-fork DAA starting limit (the Testnet PoW limit, `2007ffff`). You can
override it with `--target-difficulty-limit`, using compact 8-hex or expanded
64-hex format:

```console
zebrad fork-mainnet prepare \
  --height "$ANCHOR_HEIGHT" \
  --hash "$ANCHOR_HASH" \
  --name LocalFork \
  --network-magic a1b2c3d4 \
  --target-difficulty-limit 2007ffff \
  --output-file forked-mainnet.toml
```

The limit must be easy enough that its work value fits in a `u128` (its target
must be at least `2^128`); `fork-mainnet prepare` rejects limits that are too
hard, such as `037fffff`. The Mainnet, Testnet, and Regtest PoW limits are all
valid choices.

Post-fork activation heights must be greater than the fork height and no greater
than Zebra's maximum valid block height.

## Starting The Fork

`fork-mainnet prepare` writes a minimal config. To mine local blocks you must
also enable the RPC server, set a miner address, and force-enable the mempool
(a fork node has no peers, so it never reaches "synced to tip" on its own). Add
these to the generated `forked-mainnet.toml`:

```toml
[rpc]
listen_addr = "127.0.0.1:28232"
enable_cookie_auth = false   # omit, or configure a cookie, for non-local nodes

[mining]
# Any valid transparent address for this network; coinbase outputs are never
# finalized on a fork, so a throwaway address is fine.
miner_address = "t1Hsc1LR8yKnbbe3twRp88p6vFfC5t7DLbs"

[mempool]
# Force the mempool active so `generate` works without peers (height <= tip).
debug_enable_at_height = 0
```

Then start Zebra:

```console
zebrad -c forked-mainnet.toml start
```

The Mainnet finalized database must already contain the configured anchor block.
Zebra validates the anchor height and hash on startup and refuses to start if the
finalized tip is not exactly the anchor.

If `--disable-pow` was used when preparing the config, post-fork blocks do not
need valid proof of work, and the `generate` RPC can create local post-fork
blocks. Using the `zrpc` helper from above (pointed at the fork's RPC port):

```console
zrpc() { curl -s --data-binary "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":$2}" \
  -H 'content-type:application/json' 127.0.0.1:28232; }

zrpc generate '[1]'                  # mine one block
zrpc getblockchaininfo '[]'          # inspect tip height and active upgrades
```

Block templates are assembled over the full inherited chain state, so each
`generate` call can take tens of seconds at a high anchor height.

> **Note on not-yet-released upgrades.** Scheduling a post-fork activation for an
> upgrade whose consensus branch id is not yet finalized upstream (currently NU7)
> activates the upgrade at the configured height, but **producing** blocks at or
> after that height requires a build with NU7 support
> (`--features tx_v6` and `RUSTFLAGS='--cfg zcash_unstable="nu7"'`). Without it,
> the activation block is rejected with `WrongTransactionConsensusBranchId`.

For multi-node fork tests, generate each node's config with the same anchor,
activation heights, difficulty settings, and network magic. Add only the fork
nodes as explicit peers:

```console
zebrad fork-mainnet prepare \
  --height "$ANCHOR_HEIGHT" \
  --hash "$ANCHOR_HASH" \
  --name LocalFork \
  --network-magic a1b2c3d4 \
  --initial-peer 127.0.0.1:38233 \
  --output-file forked-mainnet-node-2.toml
```

## Resetting Fork Blocks

To discard fork blocks and return to public Mainnet:

1. Stop Zebra.
2. Reset the fork's non-finalized cache.
3. Restart Zebra with a normal Mainnet config.

```console
zebrad -c forked-mainnet.toml reset-non-finalized-state --dry-run
```

```console
zebrad -c forked-mainnet.toml reset-non-finalized-state --force
```

The reset command only deletes the selected network's non-finalized backup
cache. For forked-mainnet, it also checks a fork marker so the command does not
silently delete a different fork's cache.

To reset Mainnet's ordinary non-finalized cache directly, preview the target and
then pass both `--force` and `--confirm-mainnet`:

```console
zebrad reset-non-finalized-state \
  --network mainnet \
  --cache-dir ~/.cache/zebra \
  --dry-run
```

```console
zebrad reset-non-finalized-state \
  --network mainnet \
  --cache-dir ~/.cache/zebra \
  --force \
  --confirm-mainnet
```

## Limitations

- Forked Mainnet is for local developer testing, not public network splits.
- Fork blocks are intentionally never finalized. Very long forks can consume
  memory or disk until reset.
- The fork is not Regtest. It preserves Mainnet consensus history and state up
  to the anchor, then applies forked-mainnet post-fork settings.
- Public Mainnet peers are not useful on a fork. Add only peers that were
  intentionally started with matching fork settings.
