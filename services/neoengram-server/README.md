# NeoEngram Server

`neoengram-server` is the network composition root. The public listener uses Fusen 0.9.0. When Agent enrollment is enabled, the same process also starts a separate Hyper listener for the Agent control and data-plane API. Both listeners share the SQLite authority and control catalog.

The Agent listener is contract-first: [`neoengram-agent-api.yaml`](../../docs/openapi/neoengram-agent-api.yaml) is an independent OpenAPI 3.1 contract. Every operation is an action-style POST, and all Agent, session, Job, batch, page, and object identities are carried in the JSON body:

- `POST /agent/enrollment/bootstrap`
- `POST /agent/enrollment/status/query`
- `POST /agent/session/open`
- `POST /agent/session/heartbeat/report`
- `POST /agent/session/message/list/query`
- `POST /agent/job/report/create`
- `POST /agent/job/metadata/batch/stage`
- `POST /agent/job/metadata/page/stage`
- `POST /agent/job/index/page/query`
- `POST /agent/job/object/missing/query`
- `POST /agent/job/object/upload`
- `POST /agent/session/close`

The development transport is Agent-initiated HTTP/1 short polling. Approved Ed25519 keys sign each request; no session bearer token is issued. Session identity and generation fence stale boots, assignments are redelivered until Accepted, and decisions are redelivered until the Finalized acknowledgement. Production mTLS, S3 tickets, PostgreSQL, and HA are outside this development profile.

## Development startup

Create a private 32-byte keyring. The server rejects symlinks, files owned by another Unix user, and files with any group or other permissions.

```sh
umask 077
key="$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '=')"
jq -n --arg key "$key" \
  '{version: 1, active_key_id: "development-key", keys: {"development-key": $key}}' \
  > /tmp/neoengram-enrollment-keyring.json
chmod 600 /tmp/neoengram-enrollment-keyring.json
```

Start both loopback listeners:

```sh
cargo run -p neoengram-server -- \
  --authority-dir /tmp/neoengram-authority \
  --bind 127.0.0.1:8080 \
  --development \
  --development-token local-development-token \
  --development-tenants tenant-local \
  --agent-enrollment-enabled \
  --agent-bind 127.0.0.1:8081 \
  --agent-enrollment-keyring-file /tmp/neoengram-enrollment-keyring.json
```

Verify the public listener:

```sh
curl -i http://127.0.0.1:8080/health/live
curl -i http://127.0.0.1:8080/health/ready
```

Public business requests require `NeoEngram-API-Version: 1` and a Bearer token. In production, use OIDC/JWKS plus a deny-by-default RBAC file; development authentication is loopback-only. TLS terminates at the ingress or reverse proxy. Route `/agent/*` to the Agent listener, not the public Fusen listener.

SQLite is a single-process authority. Do not run more than one server replica against the same authority directory.
