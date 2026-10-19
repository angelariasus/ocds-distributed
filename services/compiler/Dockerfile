FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/compiler ./cmd/compiler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/compiler /compiler
EXPOSE 2115
ENTRYPOINT ["/compiler"]
