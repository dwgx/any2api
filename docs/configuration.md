# Configuration

The full example is [`src/config.example.yaml`](../src/config.example.yaml). Copy it to `config.yaml`; `config.yaml` is ignored by git.

## Minimal shape

```yaml
listen: "127.0.0.1:8787"

upstreams:
  - name: primary
    protocol: openai_chat
    base_url: "https://api.example.com/v1"
    api_key: "${UPSTREAM_API_KEY}"

models:
  - id: example-model
    upstream_model: upstream-model-name
    upstreams: ["primary"]
```

## Credentials

`api_key` accepts `${VAR}` references. An undefined or empty variable fails at startup; the value is never printed.

Use environment variables or a local file with restrictive permissions. Never commit `config.yaml`, `.env`, or generated databases.

## Public surface

```yaml
public:
  enabled: false          # keep false unless reviewed
  access_mode: gated      # open | gated
  access_keys: ["${A2A_ACCESS_KEY}"]
  trusted_proxies: ["127.0.0.1/32", "::1"]
  max_inflight: 6
  queue_size: 32
  queue_timeout_seconds: 60
  max_cost_class: medium
  daily_budget_units: 250
```

`public.enabled: false` keeps the new surface off: no sessions, access-key gate, public rate limits, or trusted-proxy handling. Enabling it is an operational and legal decision.

## Admin surface

```yaml
admin_key: "${A2A_ADMIN_KEY}"
```

If unset, admin routes are not registered (404). Keep the service on loopback and the admin key private.

## Persistence

```yaml
store:
  enabled: true
  path: "data/any2api.db"
```

Optional. If enabled, the directory must be writable by the service user.

## Response hygiene

```yaml
response_hygiene:
  scrub_same_format_extras: false
```

Default `false` preserves same-format bytes exactly. When `true`, known private timing keys are removed from non-streaming same-format success bodies; other bytes are preserved. Cross-format responses are filtered separately.

## Environment

The service reads only the config file and the environment references it contains. It does not require a database unless `store.enabled` is true.
