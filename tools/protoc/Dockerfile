FROM golang:1.26-alpine AS go-plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10 \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir grpcio-tools==1.78.0
COPY --from=go-plugins /go/bin/protoc-gen-go /go/bin/protoc-gen-go-grpc /usr/local/bin/
WORKDIR /work
