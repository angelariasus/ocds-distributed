FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ledger ./cmd/ledger \
 && mkdir /data /auditor

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ledger /ledger
COPY --from=build --chown=nonroot:nonroot /data /data
COPY --from=build --chown=nonroot:nonroot /auditor /auditor
VOLUME ["/data", "/auditor"]
EXPOSE 8100
ENTRYPOINT ["/ledger"]
