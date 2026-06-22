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

## Seed the snapshots (one-time, and after a DB format-version bump)
1. On a host with ~750 GB-1 TB of free disk and the Zebra build deps, run
   `.github/workflows/scripts/make-sync-confidence-snapshots.sh` (point `SNAP_URL`
   at a recent full mainnet snapshot, `REPO` at this checkout, and configure
   `s3cmd` for the destination Space). It rewinds the snapshot to each window
   start, prunes, and uploads `pre-nu62`/`post-nu62` tarballs to the Space.
2. Make the GHCR image public: GitHub -> the org's **Packages** -> `zebra-tests`
   -> Package settings -> Change visibility -> **Public** (created on the first
   `Sync confidence` run; droplets pull it anonymously).

## Running
Once the snapshots exist and the package is public, **Sync confidence** runs on
merge to `ironwood-main` and via manual dispatch; each window restores its pruned
tarball and syncs its 5k-block range.

## Cost note
Droplets are deleted after each run (`if: always()` + a 1h orphan sweep). If a run
is force-killed, check `doctl compute droplet list --tag-name sync-confidence-ci`.
