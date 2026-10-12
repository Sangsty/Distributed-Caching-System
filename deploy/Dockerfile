# One image with all binaries; docker-compose picks which one to run.
#
# The demo app needs one third-party module (the pgx PostgreSQL driver). `go mod
# tidy` fetches it and writes go.sum, so the build works even if you have not
# run it on your machine. GOTOOLCHAIN=auto lets Go download a newer toolchain
# if the resolved go.mod asks for one.
FROM golang:1.25-alpine AS build
ENV GOTOOLCHAIN=auto
WORKDIR /src
COPY . .
RUN go mod tidy && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/node ./cmd/coordinator ./cmd/cachectl ./cmd/demo

FROM alpine:3.20
RUN adduser -D -u 10001 cachekit
COPY --from=build /out/node /out/coordinator /out/cachectl /out/demo /usr/local/bin/
USER cachekit
