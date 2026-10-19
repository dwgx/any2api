# Service API and operations

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/chat/completions` | OpenAI Chat entry |
| POST | `/v1/messages` | Anthropic Messages entry |
| POST | `/v1/responses` | OpenAI Responses entry |
| GET | `/v1/responses/{id}` | Managed background task snapshot |
| POST | `/v1/responses/{id}/cancel` | Cancel a managed task |
| POST | `/v1beta/models/{model}:generateContent` | Gemini entry |
| POST | `/v1beta/models/{model}:streamGenerateContent` | Gemini streaming entry |
| POST | `/v1/messages/count_tokens` | Token counting |
| GET | `/v1/models` | Model discovery |
| GET | `/healthz` | Health check |

The service also exposes an optional UI and admin surface when configured.

## Authentication

- Upstream credentials are configured server-side; clients do not send them.
- The optional public surface supports session cookies and access keys.
- `admin_key` enables admin routes; when unset they are not registered.
- Keep the service on loopback and terminate TLS at a reviewed reverse proxy.

## Streaming

- Upstream SSE is relayed incrementally; terminal frames are validated per protocol.
- Committed streams are never replayed or failed over.
- Client cancellation is recorded separately from upstream failure.
- Error frames are forwarded as error signals; the request is not counted as delivered.

## Admin surface

Optional endpoints (disabled without `admin_key`):

- `GET /admin/diagnostics`
- `GET /admin/metrics`
- `GET /admin/requests`
- `GET /admin/requests/{id}`
- `GET /admin/audit`
- `GET /admin/alerts`
- `POST /admin/cooldowns/reset`

Do not expose these through a public reverse proxy.

## Public surface

When `public.enabled: true`:

- sessions are issued and validated;
- `access_mode: gated` requires an access key;
- per-IP rate limits and a global concurrency gate apply;
- cost class and daily budget controls apply;
- `trusted_proxies` controls whether `X-Forwarded-For` is honored.

`public.enabled` defaults to `false`.

## Operational notes

- `/healthz` is always available.
- The service handles `SIGTERM`/`SIGINT` with a bounded shutdown.
- Logs and admin data are redacted; credentials are never written to responses or logs.
- Upstream terms, data retention, and legal requirements are the deployer's responsibility.
