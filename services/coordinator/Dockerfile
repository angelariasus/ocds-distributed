FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/coordinator ./cmd/coordinator \
 && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/coordinator /coordinator
COPY --from=build --chown=nonroot:nonroot /data /data
VOLUME /data
EXPOSE 2112 7300
ENTRYPOINT ["/coordinator"]
