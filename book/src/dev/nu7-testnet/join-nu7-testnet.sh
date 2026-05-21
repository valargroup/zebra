#!/usr/bin/env bash
set -Eeuo pipefail

BUNDLE_URL="${NU7_JOIN_BUNDLE_URL:-}"
KEEP_BUNDLE=0
PASSTHROUGH_ARGS=()

usage() {
    cat <<USAGE
Usage: ${0##*/} --bundle-url URL_OR_PATH [join-script args...]

Downloads or copies the latest generated NU7 join bundle and executes its join-nu7-testnet.sh.
Set NU7_JOIN_BUNDLE_URL instead of passing --bundle-url if preferred.
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --bundle-url)
            BUNDLE_URL="${2:-}"
            if [ -z "$BUNDLE_URL" ]; then
                echo "missing value for --bundle-url" >&2
                exit 2
            fi
            shift 2
            ;;
        --keep-bundle)
            KEEP_BUNDLE=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            PASSTHROUGH_ARGS+=("$1")
            shift
            ;;
    esac
done

if [ -z "$BUNDLE_URL" ]; then
    echo "missing join bundle URL or path; pass --bundle-url or set NU7_JOIN_BUNDLE_URL" >&2
    exit 2
fi

for cmd in curl tar mktemp find head; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "required command not found: $cmd" >&2
        exit 1
    fi
done

WORK_DIR="$(mktemp -d)"
if [ "$KEEP_BUNDLE" -eq 0 ]; then
    trap 'rm -rf "$WORK_DIR"' EXIT
else
    echo "keeping downloaded bundle in $WORK_DIR"
fi

ARCHIVE="$WORK_DIR/join-bundle.tar.gz"
if [ -f "$BUNDLE_URL" ]; then
    cp "$BUNDLE_URL" "$ARCHIVE"
else
    curl -fL "$BUNDLE_URL" -o "$ARCHIVE"
fi
tar -xzf "$ARCHIVE" -C "$WORK_DIR"

JOIN_SCRIPT="$(find "$WORK_DIR" -maxdepth 3 -type f -name join-nu7-testnet.sh | head -n 1)"
if [ -z "$JOIN_SCRIPT" ]; then
    echo "downloaded bundle does not contain join-nu7-testnet.sh" >&2
    exit 1
fi

bash "$JOIN_SCRIPT" "${PASSTHROUGH_ARGS[@]}"
