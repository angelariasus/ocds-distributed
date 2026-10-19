#!/usr/bin/env bash
# Regenerates gRPC stubs for every service from proto/. Requires Docker only.
set -euo pipefail
cd "$(dirname "$0")/.."

docker build -q -t ocds-protoc tools/protoc >/dev/null

run() { MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W 2>/dev/null || pwd):/work" ocds-protoc "$@"; }

run protoc -I proto \
  --go_out=services/limiter/gen --go_opt=module=github.com/angelariasus/ocds-distributed/services/limiter/gen \
  --go-grpc_out=services/limiter/gen --go-grpc_opt=module=github.com/angelariasus/ocds-distributed/services/limiter/gen \
  proto/limiter/v1/limiter.proto

run python -m grpc_tools.protoc -I proto \
  --python_out=services/extractor/src --pyi_out=services/extractor/src --grpc_python_out=services/extractor/src \
  proto/limiter/v1/limiter.proto

for pkg in services/extractor/src/limiter services/extractor/src/limiter/v1; do
  [ -f "$pkg/__init__.py" ] || : > "$pkg/__init__.py"
done
echo "stubs regenerated"
