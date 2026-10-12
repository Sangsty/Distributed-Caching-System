# One image with all three binaries; docker-compose picks which one to run.
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/node ./cmd/coordinator ./cmd/cachectl

FROM alpine:3.20
RUN adduser -D -u 10001 cachekit
COPY --from=build /out/node /out/coordinator /out/cachectl /usr/local/bin/
USER cachekit
