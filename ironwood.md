# Ironwood

High-level changes:

- New Ironwood pool and value balance.
- V6 transaction format supports Sapling, Orchard, Ironwood, and
  transparent, where Sapling and transparent are the same as in v5.
- Ironwood has a separate note commitment tree and nullifier set from
  Orchard, even though it reuses the Orchard ZKP and action proof system.
- Ironwood transaction hashes use Orchard-style bundle/action hashing with
  Ironwood-specific personalization strings.
- From NU7 activation onward, transactions must not have a negative Orchard
  value balance, so new value cannot enter the Orchard pool.
- V6 transaction IDs are computed through the patched librustzcash txid path,
  which includes Ironwood-specific bundle/action hashing in the ZIP-244-style
  transaction ID tree.
