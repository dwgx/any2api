# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY src/go.mod src/go.sum ./
RUN go mod download
COPY src/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/any2api ./cmd/any2api

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && adduser -D -H -u 10001 any2api \
    && mkdir -p /etc/any2api /data \
    && chown -R any2api:any2api /etc/any2api /data
COPY --from=build /out/any2api /usr/local/bin/any2api
COPY --chown=any2api:any2api src/config.example.yaml /etc/any2api/config.example.yaml
USER any2api
WORKDIR /data
EXPOSE 8787
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8787/healthz >/dev/null || exit 1
ENTRYPOINT ["any2api", "-config", "/etc/any2api/config.yaml"]
