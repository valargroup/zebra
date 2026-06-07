# Ironwood

High-level changes:

- New Ironwood pool and value balance.
- V6 transaction format only supports Orchard, Ironwood, and transparent,
  where transparent is the same as in v5.
- V6 transaction IDs use a local ZIP-244-style tree while librustzcash does
  not support Ironwood fields.

TODO:

- Route v6 transaction ID computation through librustzcash once librustzcash
  supports Ironwood.
