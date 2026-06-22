#!/usr/bin/env bash
# Runs ON the ephemeral DigitalOcean droplet. Pulls the prebuilt test image,
# (consume) restores cached state from Spaces, runs the nextest profile, and
# (generate) uploads the resulting state back to Spaces.
#
# Config is provided via /root/run.env (sourced by the caller before exec):
#   MODE            consume | generate
#   IMAGE_REF       ghcr.io/valargroup/zebra-tests:sha-xxxx
#   NEXTEST_PROFILE e.g. sync-range-pre-nu62
#   TEST_VARIABLES  comma-separated KEY=VALUE passed into the container
#   STATE_KEY       pre-nu62 | post-nu62
#   STATE_VERSION   LOCAL_STATE_VERSION (Spaces object path component)
#   FEATURES        must equal the image build arg (default-release-binaries)
#   SPACES_BUCKET, SPACES_REGION, SPACES_ACCESS_KEY, SPACES_SECRET_KEY
set -euo pipefail

STATE_DIR=/mnt/zebra-state
OBJECT="s3://${SPACES_BUCKET}/sync-confidence/state/v${STATE_VERSION}/mainnet/${STATE_KEY}.tar.zst"

# Configure s3cmd for Spaces (S3-compatible).
cat > "${HOME}/.s3cfg" <<CFG
[default]
access_key = ${SPACES_ACCESS_KEY}
secret_key = ${SPACES_SECRET_KEY}
host_base = ${SPACES_REGION}.digitaloceanspaces.com
host_bucket = %(bucket)s.${SPACES_REGION}.digitaloceanspaces.com
use_https = True
CFG

mkdir -p "${STATE_DIR}"
docker pull "${IMAGE_REF}"

if [[ "${MODE}" == "consume" ]]; then
  echo "Downloading cached state ${OBJECT}"
  s3cmd get "${OBJECT}" /tmp/state.tar.zst
  tar --use-compress-program=zstd -xf /tmp/state.tar.zst -C "${STATE_DIR}"
  rm -f /tmp/state.tar.zst
fi

# The container runs as UID 10001 (zebra); make the bind mount writable by it.
chown -R 10001:10001 "${STATE_DIR}"

# Turn comma-separated TEST_VARIABLES into repeated -e flags.
ENV_FLAGS=()
IFS=',' read -ra KVS <<< "${TEST_VARIABLES}"
for kv in "${KVS[@]}"; do ENV_FLAGS+=( -e "${kv}" ); done

set +e
docker run --rm \
  -e NEXTEST_PROFILE="${NEXTEST_PROFILE}" \
  -e FEATURES="${FEATURES}" \
  -e ZEBRA_STATE__CACHE_DIR=/state \
  "${ENV_FLAGS[@]}" \
  -v "${STATE_DIR}:/state" \
  "${IMAGE_REF}"
RESULT=$?
set -e
echo "nextest exit code: ${RESULT}"

if [[ "${MODE}" == "generate" && "${RESULT}" -eq 0 ]]; then
  echo "Uploading state to ${OBJECT}"
  tar --use-compress-program='zstd -T0' -cf /tmp/state.tar.zst -C "${STATE_DIR}" .
  s3cmd put /tmp/state.tar.zst "${OBJECT}"
fi

exit "${RESULT}"
