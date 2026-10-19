FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/catalog ./cmd/catalog \
 && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/catalog /catalog
# A fresh named volume inherits this directory's ownership, so the nonroot user can write the log.
COPY --from=build --chown=nonroot:nonroot /data /data
VOLUME /data
EXPOSE 7100 2114
ENTRYPOINT ["/catalog"]
