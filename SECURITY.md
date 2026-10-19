# Security Policy

## No secrets in this repository

This repository contains no production accounts, cookies, API keys, session tokens, or deployment credentials. Test fixtures use obviously fake values.

If you deploy Any2API:

- Put credentials in environment variables or a local `config.yaml` that is not committed.
- Restrict the config file to the service user (`chmod 600`).
- Keep the service bound to loopback (`127.0.0.1`) unless a reviewed reverse proxy and TLS terminate in front.
- Leave `public.enabled: false` until you have reviewed access control, rate limits, data retention, and the legal requirements for your jurisdiction.
- Set `admin_key` only when you need the admin surface; keep the admin port private.
- Rotate credentials after any suspected exposure.

## Reporting a vulnerability

Do not open a public issue for a security problem. Contact the repository owner privately with:

- affected version/commit,
- reproduction steps,
- impact assessment,
- whether credentials or user data may be involved.

Do not include real credentials or personal data in the report.

## Scope

This project is a protocol translator. It does not grant rights to use any upstream service. You are responsible for complying with upstream terms, applicable law, and data-protection obligations.
