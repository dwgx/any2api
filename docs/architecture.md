# Architecture

Any2API is a Go service with a translation core and a thin HTTP shell.

```
client
  |  OpenAI Chat / Anthropic Messages / Gemini / Responses
  v
entry handler
  |  parse request, choose upstream chain, apply model aliases
  v
conversion layer (only when entry != upstream protocol)
  |  intermediate representation (IR) + per-format renderers
  v
upstream HTTP/SSE
  |  failover on pre-commit failures, cooldowns, retry-after
  v
response conversion / same-format passthrough
  v
client
```

## Entry formats

| Entry | Endpoint |
|---|---|
| OpenAI Chat | `POST /v1/chat/completions` |
| Anthropic Messages | `POST /v1/messages` |
| Gemini | `POST /v1beta/models/{model}:generateContent` and `:streamGenerateContent` |
| OpenAI Responses | `POST /v1/responses` (including `background: true` managed tasks) |

## Upstream protocols

`openai_chat`, `anthropic`, `gemini`, `responses`, `interactions`, and optional web-compatible adapters. The configured `protocol` selects request/response conversion and the upstream path.

## Conversion rules

- Same protocol: bytes are forwarded as-is (subject to optional response hygiene).
- Cross protocol: the entry parser produces an IR; the upstream renderer produces the upstream body; the upstream response is parsed back into IR and rendered for the entry format.
- Streaming uses an SSE frame reader and a streaming pipe; terminal frames are validated per protocol.
- Tool names are normalized for upstream compatibility and restored in responses.
- Usage and timing observations are diagnostic; they do not change the response body unless a hygiene option is explicitly enabled.

## Reliability

- Ordered upstream chain; pre-commit failures can fail over, committed streams never replay.
- Cooldowns for repeated failures, `retry-after` handling, first-byte and idle guards.
- Client cancellation is recorded separately from upstream failure.
- Admin diagnostics are optional and disabled without `admin_key`.

## Security boundary

- The service does not collect upstream credentials from clients; credentials are configured server-side.
- The public surface is opt-in (`public.enabled`), with sessions, access keys, per-IP rate limits, concurrency limits, and budget controls.
- Keep the admin surface on loopback. Use a reviewed reverse proxy for any external exposure.
