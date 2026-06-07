# Ironwood

High-level changes:

- New Ironwood pool and value balance.
- V6 transaction format supports Sapling, Orchard, Ironwood, and
  transparent, where Sapling and transparent are the same as in v5.
- Ironwood has a separate note commitment tree and nullifier set from
  Orchard, even though it reuses the Orchard ZKP and action proof system.
- From NU7 activation onward, transactions must not have a positive Orchard
  value balance, so new value cannot enter the Orchard pool.
- V6 transaction IDs use a local ZIP-244-style tree while librustzcash does
  not support Ironwood fields. The local implementation reuses librustzcash's
  v5 Sapling and Orchard transaction ID subtree digests.

TODO:

- Route v6 transaction ID computation through librustzcash once librustzcash
  supports Ironwood.
