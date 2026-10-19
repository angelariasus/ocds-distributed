#!/usr/bin/env bash
# Asks the coordinator replicas' current Raft leader to start a planning round now.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
for n in coordinator1 coordinator2 coordinator3; do
  code=$(docker run --rm --network ocds_default curlimages/curl:8.10.1 -s -o /dev/null -w '%{http_code}' -X POST "http://$n:2112/plan" 2>/dev/null || true)
  if [ "$code" = "202" ]; then
    echo "round requested from leader $n"
    exit 0
  fi
done
echo "no leader answered (is a majority of the coordinators up?)" >&2
exit 1
