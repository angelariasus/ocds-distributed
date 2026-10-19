#!/usr/bin/env bash
# Generates deploy/pki (CA, service certificates, JWT and ledger signing keys). Requires Docker only.
set -euo pipefail
cd "$(dirname "$0")/.."
docker build -q -t ocds-pytools tools/pytools >/dev/null
MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W 2>/dev/null || pwd):/work" -w /work ocds-pytools python -I scripts/pki.py deploy/pki "$@"
