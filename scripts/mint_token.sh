#!/usr/bin/env bash
# Prints a JWT for the query API: scripts/mint_token.sh <reader|analyst> [ttl_seconds] [subject]
set -euo pipefail
cd "$(dirname "$0")/.."
docker build -q -t ocds-pytools tools/pytools >/dev/null
MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W 2>/dev/null || pwd)/deploy/pki:/pki:ro" -v "$(pwd -W 2>/dev/null || pwd)/scripts:/scripts:ro" ocds-pytools python -I /scripts/mint_token.py "$@"
