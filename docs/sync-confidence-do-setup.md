# Sync-confidence on DigitalOcean — one-time setup

The `Sync confidence` workflows run on ephemeral DigitalOcean droplets in `nyc3`
and store cached chain state in DO Spaces. Configure these once (and update the
state objects after a DB format-version bump by re-running the snapshots workflow).

## Secrets (Settings -> Secrets and variables -> Actions -> Secrets)
- `DIGITALOCEAN_ACCESS_TOKEN` - a DO API token with read/write scope.
- `SPACES_ACCESS_KEY` / `SPACES_SECRET_KEY` - a Spaces access keypair.
- `DO_SSH_PRIVATE_KEY` - private key of a dedicated CI SSH keypair (PEM).

## Variables (... -> Variables)
- `SPACES_BUCKET` - the Space (bucket) name, created in `nyc3`.
- `DO_SSH_KEY_FINGERPRINT` - fingerprint of the public key, after registering it
  in DO (`doctl compute ssh-key import ci-key --public-key-file ci-key.pub`, then
  `doctl compute ssh-key list`).

## First run
1. Confirm 3,405,000 <= current mainnet head (else lower the post-nu62 heights in
   `zebrad/tests/acceptance.rs` + regenerate).
2. Run **Sync confidence snapshots** (Actions -> Run workflow) to seed the two
   state tarballs in Spaces. The post-nu62 generate job syncs genesis->3.4M and is
   long; size its droplet for the full finalized state (see `droplet_size`).
3. Once snapshots exist, **Sync confidence** runs on merge to `ironwood-main` and
   via manual dispatch.

## Cost note
Droplets are deleted after each run (`if: always()` + a 1h orphan sweep). If a run
is force-killed, check `doctl compute droplet list --tag-name sync-confidence-ci`.
