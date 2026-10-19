#!/usr/bin/env bash
# Regenerates gRPC stubs for every service from proto/. Requires Docker only.
set -euo pipefail
cd "$(dirname "$0")/.."

docker build -q -t ocds-protoc tools/protoc >/dev/null

run() { MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W 2>/dev/null || pwd):/work" ocds-protoc "$@"; }

gen_go() { # service proto
  local mod="github.com/angelariasus/ocds-distributed/services/$1/gen"
  run protoc -I proto \
    --go_out="services/$1/gen" --go_opt="module=$mod" \
    --go-grpc_out="services/$1/gen" --go-grpc_opt="module=$mod" \
    "$2"
}

gen_go limiter proto/limiter/v1/limiter.proto
gen_go catalog proto/catalog/v1/catalog.proto

run python -m grpc_tools.protoc -I proto \
  --python_out=services/extractor/src --pyi_out=services/extractor/src --grpc_python_out=services/extractor/src \
  proto/limiter/v1/limiter.proto proto/catalog/v1/catalog.proto

for pkg in limiter limiter/v1 catalog catalog/v1; do
  [ -f "services/extractor/src/$pkg/__init__.py" ] || : > "services/extractor/src/$pkg/__init__.py"
done
echo "stubs regenerated"
