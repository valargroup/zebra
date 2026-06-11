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

Record both the anchor height and block hash:

```console
ANCHOR_HEIGHT=3400000
ANCHOR_HASH=$(zcash-cli getblockhash "$ANCHOR_HEIGHT")
```

Stop Zebra before preparing or starting the fork. Then generate a fork config:

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
an easy post-fork DAA starting limit. You can override it with
`--target-difficulty-limit`, using compact 8-hex or expanded 64-hex format:

```console
zebrad fork-mainnet prepare \
  --height "$ANCHOR_HEIGHT" \
  --hash "$ANCHOR_HASH" \
  --name LocalFork \
  --network-magic a1b2c3d4 \
  --target-difficulty-limit 037fffff \
  --output-file forked-mainnet.toml
```

Post-fork activation heights must be greater than the fork height and no greater
than Zebra's maximum valid block height.

## Starting The Fork

Start Zebra with the generated config:

```console
zebrad -c forked-mainnet.toml start
```

The Mainnet finalized database must already contain the configured anchor block.
Zebra will validate the anchor hash on startup.

If `--disable-pow` was used when preparing the config, post-fork blocks do not
need valid proof of work. The `generate` RPC can then create local post-fork
blocks:

```console
zcash-cli generate 1
```

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
