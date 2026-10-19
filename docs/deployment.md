# Deployment

## Local

```bash
cd src
go build -o any2api ./cmd/any2api
cp config.example.yaml ../config.yaml
# edit config.yaml
../any2api -config ../config.yaml
```

Default listener: `127.0.0.1:8787`.

## Docker Compose

```bash
cp src/config.example.yaml config.yaml
# edit config.yaml
docker compose up -d --build
curl -s http://127.0.0.1:8787/healthz
```

The compose file mounts `config.yaml` read-only and stores runtime data in the `any2api-data` volume.

## Reverse proxy and TLS

Keep the service bound to loopback. Terminate TLS at a reviewed reverse proxy (Caddy, nginx, or a tunnel) and forward to `127.0.0.1:8787`.

Requirements for streaming:

- disable response buffering (`flush_interval -1` on Caddy; `proxy_buffering off` and a long read timeout on nginx);
- forward `X-Forwarded-For` and `X-Forwarded-Proto` only if you configure `public.trusted_proxies` with the proxy's own address;
- do not expose `/admin` through the public proxy.

## Public service

`public.enabled` is `false` by default. Before enabling it:

1. complete the legal/privacy review for your jurisdiction;
2. configure access keys, trusted proxies, limits, and budget;
3. test in an isolated instance (no production credentials);
4. prepare a kill switch: set `public.enabled: false` and restart.

## Health and shutdown

`GET /healthz` is always available. The service handles `SIGTERM`/`SIGINT` with a bounded shutdown and closes the store explicitly.

## Hardening checklist

- [ ] service user is unprivileged;
- [ ] `config.yaml` is `0600` and not committed;
- [ ] admin key is set only if needed; admin routes are not publicly proxied;
- [ ] TLS terminates at the proxy;
- [ ] rate limits and budgets are configured;
- [ ] logs do not contain credentials or raw personal data;
- [ ] a rollback/kill-switch procedure is documented and tested.
